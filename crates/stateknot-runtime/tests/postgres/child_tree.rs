// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn parent_graph(
    child: &CompiledGraph,
    agent: &AgentDescriptor,
    name: &str,
    limits: ChildRunTopologyLimits,
) -> CompiledGraph {
    CompiledGraph::compile(
        capability(name),
        child.input_schema().clone(),
        child.state_schema().clone(),
        child.update_schema().clone(),
        child.output_schema().clone(),
        child.reducer().clone(),
        child.entry_nodes().clone(),
        child.nodes().iter().cloned(),
        child.limits(),
    )
    .unwrap()
    .with_child_runs(
        GraphChildRunPolicy::new(
            limits,
            child.nodes().iter().map(|node| {
                ChildRunDeclaration::new(
                    node.node_id().clone(),
                    ChildRunSlot::new("analysis").unwrap(),
                    agent,
                    child,
                )
                .unwrap()
            }),
        )
        .unwrap(),
    )
    .unwrap()
}

pub(super) async fn root_admission(
    store: &PostgresStore,
    fixture: &DriverFixture,
    tenant: TenantId,
) -> StoredAgentAdmission {
    let request = durable_admission_request(
        fixture,
        tenant,
        AgentRunIds::generate(),
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    store
        .register_graph_definition(
            request.intent().provenance().tenant_id().clone(),
            fixture.graph.clone(),
        )
        .await
        .unwrap();
    let provenance = request.intent().provenance();
    let append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(
            provenance.tenant_id().clone(),
            provenance.run_id(),
            request.admission_event_id(),
            payload(AgentAdmission::JOURNAL_EVENT_KIND),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = CheckpointWrite::initial(
        provenance.tenant_id().clone(),
        provenance.run_id(),
        request.initial_checkpoint_id(),
        fixture.graph.reference(),
        request.initial_state().clone(),
        fixture.graph.entry_nodes().clone(),
    )
    .unwrap();
    Box::pin(store.admit_agent_run(
        request.intent().clone(),
        append,
        checkpoint,
        fixture.registry.schemas(),
    ))
    .await
    .unwrap()
    .stored()
    .clone()
}

pub(super) async fn spawn_below(
    store: &PostgresStore,
    parent: &StoredAgentAdmission,
    child: &DriverFixture,
) -> Result<stateknot_store_postgres::ChildRunRecord, StoreError> {
    let parent_id = parent.admission().intent().provenance();
    let request = durable_admission_request(
        child,
        parent_id.tenant_id().clone(),
        AgentRunIds::generate(),
        child.graph.output_schema().clone(),
        child.graph.input_schema().clone(),
    );
    store
        .register_graph_definition(parent_id.tenant_id().clone(), child.graph.clone())
        .await?;
    let intent = ChildRunAdmissionIntent::new(
        parent.admission(),
        PreparationFixture::key(parent.checkpoint()),
        request.intent().clone(),
        child.graph.clone(),
        request.initial_state().clone(),
    )
    .unwrap();
    let key = intent.key();
    let fence = store
        .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
        .await?
        .lease()
        .fence()
        .clone();
    let run = store.load_run(key.tenant_id(), key.parent_run_id()).await?;
    let start = store
        .start_node_attempt(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                EventId::generate(),
                run.journal_head().unwrap().clone(),
                fence.clone(),
            ),
            key.parent().clone(),
            AttemptId::generate(),
        )
        .await?;
    let attempt = match start {
        NodeAttemptCommitOutcome::Committed { attempt, .. } => attempt,
        other => panic!("fresh nested start expected: {other:?}"),
    };
    let append = JournalAppend::new(
        JournalExpectation::exact(attempt.start().journal_head().clone()),
        JournalEventIntent::worker(
            key.tenant_id().clone(),
            key.parent_run_id(),
            EventId::generate(),
            fence,
            payload("child-run-admitted"),
        )
        .unwrap(),
    )
    .unwrap();
    let (child_append, checkpoint) = child_write(&intent);
    Ok(Box::pin(store.admit_child_run(
        intent,
        &attempt.start().head(),
        append,
        child_append,
        checkpoint,
        BudgetUsage::zero(),
        child.registry.schemas(),
    ))
    .await?
    .record()
    .clone())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn nested_ownership_enforces_ancestor_limits_cancellation_and_once_only_subtree_cost() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for (active_limit, cancel) in [(1, false), (2, false), (2, true)] {
        let tenant = tenant("owned-nested-tree");
        let leaf = driver_fixture();
        let leaf_request = durable_admission_request(
            &leaf,
            tenant.clone(),
            AgentRunIds::generate(),
            leaf.graph.output_schema().clone(),
            leaf.graph.input_schema().clone(),
        );
        let middle = declared_parent(&leaf, leaf_request.intent().descriptor());
        let middle_request = durable_admission_request(
            &middle,
            tenant.clone(),
            AgentRunIds::generate(),
            middle.graph.output_schema().clone(),
            middle.graph.input_schema().clone(),
        );
        let graph = parent_graph(
            &middle.graph,
            middle_request.intent().descriptor(),
            "nested-root",
            ChildRunTopologyLimits::new(2, 8, active_limit).unwrap(),
        );
        let root = DriverFixture {
            graph,
            registry: middle.registry.clone(),
            first_calls: Arc::new(AtomicUsize::new(0)),
            second_calls: Arc::new(AtomicUsize::new(0)),
        };
        let root = Box::pin(root_admission(&store, &root, tenant.clone())).await;
        let owned_middle = Box::pin(spawn_below(&store, &root, &middle)).await.unwrap();
        let root_id = root.admission().intent().provenance().run_id();
        let middle_id = owned_middle
            .child()
            .admission()
            .intent()
            .provenance()
            .run_id();
        if cancel {
            let run = store.load_run(&tenant, root_id).await.unwrap();
            let request = RunCancellationRequest::new(
                Failure::new(
                    FailureId::generate(),
                    FailureCategory::Cancelled,
                    FailureCode::new("run.cancelled").unwrap(),
                    FailureOrigin::new("test.child").unwrap(),
                    FailureMessage::new("Cancelled by test.").unwrap(),
                    RetryAdvice::Never,
                )
                .unwrap(),
                store.observe_database_clock().await.unwrap(),
            )
            .unwrap();
            let append = JournalAppend::new(
                JournalExpectation::exact(run.journal_head().unwrap().clone()),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    root_id,
                    EventId::generate(),
                    payload("test-cancellation"),
                )
                .unwrap(),
            )
            .unwrap();
            store
                .append_control_plane(
                    append,
                    RunProjection::transition(
                        run.lifecycle().revision(),
                        RunTransition::RequestCancellation { request },
                    ),
                )
                .await
                .unwrap();
        }
        let result = Box::pin(spawn_below(&store, owned_middle.child(), &leaf)).await;
        if cancel {
            assert!(
                matches!(result, Err(StoreError::ChildRunRejected)),
                "{result:?}"
            );
        } else if active_limit == 1 {
            assert!(
                matches!(result, Err(StoreError::ChildRunTopologyExceeded)),
                "{result:?}"
            );
        } else {
            let grandchild = result.unwrap();
            assert_eq!(grandchild.ancestors(), &[root_id, middle_id]);
            let leaf_id = grandchild
                .child()
                .admission()
                .intent()
                .provenance()
                .run_id();
            assert!(matches!(
                fail(&store, &tenant, middle_id, BudgetUsage::zero()).await,
                Err(StoreError::UnsettledChildRuns)
            ));
            let leaf_usage = BudgetUsage::builder()
                .input_tokens(TokenCount::new(3))
                .build()
                .unwrap();
            fail(&store, &tenant, leaf_id, leaf_usage).await.unwrap();
            Box::pin(store.settle_child_run(
                grandchild.intent().key(),
                settlement_append(&store, grandchild.intent().key()).await,
            ))
            .await
            .unwrap();
            let direct = BudgetUsage::builder()
                .input_tokens(TokenCount::new(2))
                .build()
                .unwrap();
            let middle_usage = store
                .include_child_usage(&tenant, middle_id, direct)
                .await
                .unwrap();
            assert_eq!(middle_usage.input_tokens(), TokenCount::new(5));
            fail(&store, &tenant, middle_id, middle_usage)
                .await
                .unwrap();
            Box::pin(store.settle_child_run(
                owned_middle.intent().key(),
                settlement_append(&store, owned_middle.intent().key()).await,
            ))
            .await
            .unwrap();
            let total = store
                .include_child_usage(&tenant, root_id, BudgetUsage::zero())
                .await
                .unwrap();
            assert_eq!(total.input_tokens(), TokenCount::new(5));
            fail(&store, &tenant, root_id, total).await.unwrap();
            assert!(
                store
                    .pending_child_settlements_after(&tenant, Some(grandchild.intent().key()))
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }
    store.close().await;
}
