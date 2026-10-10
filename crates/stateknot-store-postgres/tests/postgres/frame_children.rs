// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Isolated children keep their real frame owner and the original Run account.
use super::frame_transactions::{admit, caller_graph, graphs, initial_usage};
use super::*;
use stateknot_core::{
    ChildRunAdmissionIntent, ChildRunDeclaration, ChildRunKey, ChildRunSlot,
    ChildRunTopologyLimits, GraphChildRunPolicy, GraphFrameCheckpoint, GraphFrameEntryPlan,
    NodeAttemptStartHead, RunFence,
};
use stateknot_store_postgres::{ChildRunCommitOutcome, GraphFrameEntryCommitOutcome};

struct Fixture {
    admission: StoredAgentAdmission,
    intent: ChildRunAdmissionIntent,
    node: NodeAttemptStartHead,
    fence: RunFence,
    head: JournalHead,
    direct: BudgetUsage,
    frame: GraphFrameCheckpoint,
}

fn child_descriptor(graph: &CompiledGraph) -> AgentDescriptor {
    let template = agent_fixture_definition();
    AgentDescriptor::new(
        template.metadata().clone(),
        graph.input_schema().clone(),
        graph.output_schema().clone(),
        template.model().clone(),
        template.instructions().clone(),
        template.tools().clone(),
        template.execution().clone(),
        template.budget_limits().clone(),
    )
    .unwrap()
}

fn declared(graph: CompiledGraph, child: &CompiledGraph, agent: &AgentDescriptor) -> CompiledGraph {
    graph
        .with_child_runs(
            GraphChildRunPolicy::new(
                ChildRunTopologyLimits::new(2, 8, 4).unwrap(),
                [ChildRunDeclaration::new(
                    NodeId::new("finish").unwrap(),
                    ChildRunSlot::new("analysis").unwrap(),
                    agent,
                    child,
                )
                .unwrap()],
            )
            .unwrap(),
        )
        .unwrap()
}

async fn started(store: &PostgresStore, name: &str) -> Fixture {
    Box::pin(started_with_sibling(store, name, false)).await
}

#[allow(clippy::too_many_lines)]
async fn started_with_sibling(store: &PostgresStore, name: &str, sibling: bool) -> Fixture {
    let tenant = tenant(name);
    let run = RunId::generate();
    let (_, template, _) = graphs();
    let isolated = CompiledGraph::compile(
        CapabilityIdentity::new(
            template.identity().owner().clone(),
            CapabilityReference::new("isolated-child".parse().unwrap(), Version::new(1, 0, 0)),
        ),
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        template.entry_nodes().clone(),
        template.nodes().iter().cloned(),
        template.limits(),
    )
    .unwrap();
    let agent = child_descriptor(&isolated);
    let template = if sibling {
        CompiledGraph::compile(
            template.identity().clone(),
            template.input_schema().clone(),
            template.state_schema().clone(),
            template.update_schema().clone(),
            template.output_schema().clone(),
            template.reducer().clone(),
            ReadyNodes::try_new([
                NodeId::new("finish").unwrap(),
                NodeId::new("sibling").unwrap(),
            ])
            .unwrap(),
            [
                GraphNode::new(
                    NodeId::new("finish").unwrap(),
                    None,
                    GraphRoutes::empty(),
                    None,
                    true,
                )
                .unwrap(),
                GraphNode::new(
                    NodeId::new("sibling").unwrap(),
                    None,
                    GraphRoutes::empty(),
                    None,
                    true,
                )
                .unwrap(),
            ],
            GraphExecutionLimits::new(Superstep::new(8).unwrap(), 2).unwrap(),
        )
        .unwrap()
    } else {
        template
    };
    let leaf = declared(template, &isolated, &agent);
    let root = declared(
        caller_graph("scoped-child-root", &leaf, 8),
        &isolated,
        &agent,
    );
    store
        .register_graph_definition(tenant.clone(), isolated.clone())
        .await
        .unwrap();
    let admission = Box::pin(admit(store, &tenant, run, &root, &leaf)).await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let call = &root.frame_calls().unwrap().calls()[0];
    let plan = GraphFrameEntryPlan::for_root(
        call,
        &root,
        admission.checkpoint(),
        &leaf,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence.clone(),
    )
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(entry) = Box::pin(store.enter_graph_frame(
        plan,
        EventId::generate(),
        admission.event().head(),
        initial_usage(&admission),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap() else {
        panic!("fresh frame entry");
    };
    let planner =
        ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
            .unwrap();
    let dispatch = planner
        .finish(entry.event().head(), entry.event().recorded_at())
        .unwrap();
    let start = Box::pin(store.start_recovered_graph_frame_node_attempt(
        worker_append(
            tenant.clone(),
            run,
            EventId::generate(),
            JournalExpectation::exact(entry.event().head()),
            fence.clone(),
            42_000,
        ),
        &dispatch,
        &NodeId::new("finish").unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let node = start.attempt().start().head();
    let head = start.attempt().start().journal_head().clone();
    let parent = admission.admission().intent();
    let request = AgentRequest::new(
        isolated.input_schema().clone(),
        BoundedJson::try_from(json!({"count":1})).unwrap(),
        parent
            .request()
            .budget_limits()
            .clone()
            .with_event_bytes(ByteCount::new(1_048_576))
            .with_checkpoint_bytes(ByteCount::new(1_048_576))
            .with_graph_steps(stateknot_core::ExecutionCount::new(8))
            .with_input_bytes(ByteCount::new(512))
            .with_output_bytes(ByteCount::new(512)),
    );
    let child = AgentAdmissionIntent::new(
        AgentResultProvenance::for_agent(
            tenant.clone(),
            RunId::generate(),
            ThreadId::generate(),
            InvocationId::generate(),
            &agent,
        ),
        agent,
        request,
        parent.budget_layers().iter().cloned(),
        isolated.reference(),
        parent.authority().clone(),
    )
    .unwrap();
    let intent = ChildRunAdmissionIntent::for_frame(
        admission.admission(),
        entry.entry().checkpoint(),
        ChildRunKey::new(
            node.activation().clone(),
            ChildRunSlot::new("analysis").unwrap(),
        )
        .unwrap(),
        child,
        isolated.clone(),
        CheckpointState::new(
            isolated.state_schema().clone(),
            BoundedJson::try_from(json!({"count":1})).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    // This zero-I/O fixture has exactly three real events and two checkpoints.
    // Build the complete observation from those facts, not the structural floor.
    let events = store
        .load_journal_page(
            &tenant,
            run,
            None,
            stateknot_store_postgres::JournalPageSize::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(events.events().len(), 3);
    assert_eq!(events.events().last().unwrap().head(), head);
    let event_bytes: usize = events
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len())
        .sum();
    let checkpoint_bytes = serde_json_canonicalizer::to_vec(admission.checkpoint())
        .unwrap()
        .len()
        + serde_json_canonicalizer::to_vec(entry.entry().checkpoint().checkpoint())
            .unwrap()
            .len()
        + serde_json_canonicalizer::to_vec(&entry.entry().checkpoint().head())
            .unwrap()
            .len();
    let direct = BudgetUsage::builder()
        .graph_depth(stateknot_core::ExecutionCount::new(2))
        .graph_steps(stateknot_core::ExecutionCount::new(2))
        .concurrent_branches(stateknot_core::ExecutionCount::new(1))
        .fan_out(stateknot_core::ExecutionCount::new(1))
        .event_bytes(ByteCount::new(event_bytes as u64))
        .checkpoint_bytes(ByteCount::new(checkpoint_bytes as u64))
        .input_bytes(ByteCount::new(
            serde_json_canonicalizer::to_vec(parent.request().input())
                .unwrap()
                .len() as u64,
        ))
        .build()
        .unwrap();
    Fixture {
        admission,
        intent,
        node,
        fence,
        head,
        direct,
        frame: entry.entry().checkpoint().clone(),
    }
}

fn join_append(value: &Fixture, head: &JournalHead) -> JournalAppend {
    let activation = value.intent.key().parent();
    JournalAppend::new(
        JournalExpectation::exact(head.clone()),
        JournalEventIntent::worker(
            activation.tenant_id().clone(),
            activation.run_id(),
            EventId::generate(),
            value.fence.clone(),
            payload("child-join-registered"),
        )
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn scoped_join_suspends_leaf_without_counting_its_authenticated_framework_caller() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "scoped-child-join")).await;
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let request = stateknot_core::ChildRunJoinRequest::new([value.intent.key().clone()]).unwrap();
    let append = join_append(&value, &spawned.record().spawn().head());
    let registered =
        Box::pin(store.register_child_join(request.clone(), &value.node, append.clone()))
            .await
            .unwrap();
    assert!(matches!(
        registered,
        stateknot_store_postgres::ChildJoinCommitOutcome::Committed(_)
    ));
    let run = store
        .load_run(
            value.intent.key().tenant_id(),
            value.intent.key().parent_run_id(),
        )
        .await
        .unwrap();
    assert!(run.lease().is_none());
    assert_eq!(
        run.journal_head(),
        Some(&registered.record().registration().head())
    );
    let active = store
        .load_active_graph_frame(
            value.intent.key().tenant_id(),
            value.intent.key().parent_run_id(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.checkpoint(), &value.frame);
    let retry = Box::pin(store.register_child_join(request, &value.node, append))
        .await
        .unwrap();
    assert!(matches!(
        retry,
        stateknot_store_postgres::ChildJoinCommitOutcome::Idempotent(_)
    ));
    assert_eq!(
        retry.record().registration(),
        registered.record().registration()
    );
    assert!(matches!(
        store
            .claim_lease(
                value.intent.key().tenant_id(),
                value.intent.key().parent_run_id(),
                AttemptId::generate()
            )
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    store.close().await;
}

#[tokio::test]
async fn scoped_join_rejects_an_actual_unfinished_ordinary_sibling() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = Box::pin(started_with_sibling(
        &store,
        "scoped-child-join-exclusive",
        true,
    ))
    .await;
    let dispatch = ReadyNodeRecoveryPlanner::for_frame(value.frame.clone(), value.fence.clone())
        .unwrap()
        .finish(
            value.head.clone(),
            store.observe_database_clock().await.unwrap(),
        )
        .unwrap();
    let sibling = Box::pin(store.start_recovered_graph_frame_node_attempt(
        worker_append(
            value.fence.tenant_id().clone(),
            value.fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(value.head.clone()),
            value.fence.clone(),
            42_001,
        ),
        &dispatch,
        &NodeId::new("sibling").unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    value.head = sibling.event().head();
    // One additional real zero-I/O physical start and its actual event bytes.
    value.direct = value
        .direct
        .checked_accumulate(
            &BudgetUsage::builder()
                .graph_steps(stateknot_core::ExecutionCount::new(1))
                .event_bytes(ByteCount::new(
                    serde_json_canonicalizer::to_vec(sibling.event())
                        .unwrap()
                        .len() as u64,
                ))
                .build()
                .unwrap(),
        )
        .unwrap();
    let spawned = Box::pin(spawn(&store, &value)).await.unwrap();
    let request = stateknot_core::ChildRunJoinRequest::new([value.intent.key().clone()]).unwrap();
    assert!(matches!(
        Box::pin(store.register_child_join(
            request,
            &value.node,
            join_append(&value, &spawned.record().spawn().head()),
        ))
        .await,
        Err(StoreError::ChildJoinRejected)
    ));
    let run = store
        .load_run(value.fence.tenant_id(), value.fence.run_id())
        .await
        .unwrap();
    assert_eq!(run.journal_head(), Some(&spawned.record().spawn().head()));
    assert_eq!(run.lease().unwrap().fence(), &value.fence);
    store.close().await;
}

#[tokio::test]
async fn scoped_child_ack_recovers_before_a_released_original_fence() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "scoped-child-ack")).await;
    let first = Box::pin(spawn(&store, &value)).await.unwrap();
    store.release_lease(&value.fence).await.unwrap();
    let successor = store
        .claim_lease(
            value.fence.tenant_id(),
            value.fence.run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    assert!(successor.lease().fence().epoch() > value.fence.epoch());
    let retry = Box::pin(spawn(&store, &value)).await.unwrap();
    assert!(matches!(retry, ChildRunCommitOutcome::Idempotent(_)));
    assert_eq!(retry.record().spawn(), first.record().spawn());
    assert_eq!(
        retry.record().child().admission(),
        first.record().child().admission()
    );
    let run = store
        .load_run(value.fence.tenant_id(), value.fence.run_id())
        .await
        .unwrap();
    assert_eq!(run.journal_head(), Some(&first.record().spawn().head()));
    assert_eq!(run.lease().unwrap().fence(), successor.lease().fence());
    store.close().await;
}

#[tokio::test]
async fn scoped_child_rejects_a_self_consistent_sibling_namespace_before_writing() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = Box::pin(started(&store, "scoped-child-crossed-namespace")).await;
    let sibling = GraphFrameCheckpoint::new(
        stateknot_core::GraphFrameIdentity::new(
            value.frame.frame().origin().clone(),
            NodeId::new("forged.slot").unwrap(),
            value.frame.checkpoint().graph().clone(),
        )
        .unwrap(),
        value.frame.checkpoint().clone(),
    )
    .unwrap();
    value.intent = ChildRunAdmissionIntent::for_frame(
        value.admission.admission(),
        &sibling,
        ChildRunKey::new(
            sibling
                .activation(value.node.activation().node_id().clone())
                .unwrap(),
            value.intent.key().slot().clone(),
        )
        .unwrap(),
        value.intent.child().clone(),
        value.intent.child_graph().clone(),
        value.intent.initial_state().clone(),
    )
    .unwrap();
    assert!(matches!(
        Box::pin(spawn(&store, &value)).await,
        Err(StoreError::StaleCheckpointHead)
    ));
    let run = store
        .load_run(value.fence.tenant_id(), value.fence.run_id())
        .await
        .unwrap();
    assert_eq!(run.journal_head(), Some(&value.head));
    assert_eq!(run.lease().unwrap().fence(), &value.fence);
    assert!(
        store
            .load_child_budget_account(value.fence.tenant_id(), value.fence.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store
            .load_run(
                value.intent.child().provenance().tenant_id(),
                value.intent.child().provenance().run_id()
            )
            .await,
        Err(StoreError::RunNotFound)
    ));
    store.close().await;
}

fn payload(kind: &str) -> JournalPayload {
    JournalPayload::new(
        checkpoint_schema("scoped-child-event"),
        kind.parse().unwrap(),
        BoundedJson::try_from(json!({"test":true})).unwrap(),
    )
    .unwrap()
}

async fn spawn(
    store: &PostgresStore,
    value: &Fixture,
) -> Result<ChildRunCommitOutcome, StoreError> {
    let key = value.intent.key();
    let child = value.intent.child().provenance();
    let parent_append = JournalAppend::new(
        JournalExpectation::exact(value.head.clone()),
        JournalEventIntent::worker(
            key.tenant_id().clone(),
            key.parent_run_id(),
            EventId::generate(),
            value.fence.clone(),
            payload("child-run-admitted"),
        )
        .unwrap(),
    )
    .unwrap();
    let child_append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(
            child.tenant_id().clone(),
            child.run_id(),
            EventId::generate(),
            payload(AgentAdmission::JOURNAL_EVENT_KIND),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = CheckpointWrite::initial(
        child.tenant_id().clone(),
        child.run_id(),
        CheckpointId::generate(),
        value.intent.child().graph().clone(),
        value.intent.initial_state().clone(),
        value.intent.child_graph().entry_nodes().clone(),
    )
    .unwrap();
    Box::pin(store.admit_child_run(
        value.intent.clone(),
        &value.node,
        parent_append,
        child_append,
        checkpoint,
        value.direct.clone(),
        &AcceptGraphSchemas,
    ))
    .await
}

#[tokio::test]
async fn scoped_child_admission_keeps_root_account_and_original_frame_owner() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Box::pin(started(&store, "scoped-child-admission")).await;
    let committed = Box::pin(spawn(&store, &value)).await.unwrap();
    assert!(matches!(committed, ChildRunCommitOutcome::Committed(_)));
    let record = committed.record();
    assert_eq!(record.intent(), &value.intent);
    assert_eq!(record.ancestors(), &[value.intent.key().parent_run_id()]);
    let restored = store.load_child_run(value.intent.key()).await.unwrap();
    assert_eq!(restored.spawn(), record.spawn());
    let account = store
        .load_child_budget_account(
            value.intent.key().tenant_id(),
            value.intent.key().parent_run_id(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    assert_eq!(account.children()[0].key(), value.intent.key());
    let root = store
        .load_current_checkpoint(
            value.intent.key().tenant_id(),
            value.intent.key().parent_run_id(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root, *value.admission.checkpoint());
    store.close().await;
}
