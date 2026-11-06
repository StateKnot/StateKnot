// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! A grandchild remains subject to the original frame owner's narrower topology.
use super::frame_barriers::pool;
use super::frame_transactions::{admit, caller_graph, graphs, initial_usage};
use super::*;
use stateknot_core::{
    ChildRunAdmissionIntent, ChildRunBudgetAccount, ChildRunDeclaration, ChildRunKey,
    ChildRunPolicyError, ChildRunSlot, ChildRunTopologyLimits, CostLimits, ExecutionCount,
    GraphChildRunPolicy, GraphFrameCheckpoint, GraphFrameEntryPlan, NodeAttemptStartHead, RunFence,
    TokenCount,
};
use stateknot_store_postgres::{ChildRunCommitOutcome, GraphFrameEntryCommitOutcome};

fn payload(kind: &str) -> JournalPayload {
    JournalPayload::new(
        checkpoint_schema("scoped-child-topology-event"),
        kind.parse().unwrap(),
        BoundedJson::try_from(json!({"test":true})).unwrap(),
    )
    .unwrap()
}

struct Parent {
    admission: StoredAgentAdmission,
    frame: Option<GraphFrameCheckpoint>,
    node: NodeAttemptStartHead,
    fence: RunFence,
}

fn single_node_graph(name: &str) -> CompiledGraph {
    let (_, template, _) = graphs();
    CompiledGraph::compile(
        CapabilityIdentity::new(
            template.identity().owner().clone(),
            CapabilityReference::new(name.parse().unwrap(), Version::new(1, 0, 0)),
        ),
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap(),
        [GraphNode::new(
            NodeId::new("finish").unwrap(),
            None,
            GraphRoutes::empty(),
            None,
            true,
        )
        .unwrap()],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap()
}

fn descriptor(graph: &CompiledGraph) -> AgentDescriptor {
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

fn declared(
    graph: CompiledGraph,
    child: &CompiledGraph,
    slots: &[&str],
    limits: ChildRunTopologyLimits,
) -> CompiledGraph {
    let agent = descriptor(child);
    graph
        .with_child_runs(
            GraphChildRunPolicy::new(
                limits,
                slots.iter().map(|slot| {
                    ChildRunDeclaration::new(
                        NodeId::new("finish").unwrap(),
                        ChildRunSlot::new(*slot).unwrap(),
                        &agent,
                        child,
                    )
                    .unwrap()
                }),
            )
            .unwrap(),
        )
        .unwrap()
}

async fn frame_parent(
    store: &PostgresStore,
    tenant: &TenantId,
    child: &CompiledGraph,
    limits: ChildRunTopologyLimits,
) -> Parent {
    let leaf = declared(
        single_node_graph("topology-owner"),
        child,
        &["branch"],
        limits,
    );
    let root = caller_graph("topology-root", &leaf, 8)
        .with_child_runs(
            GraphChildRunPolicy::new(
                ChildRunTopologyLimits::new(3, 8, 4).unwrap(),
                [ChildRunDeclaration::new(
                    NodeId::new("finish").unwrap(),
                    ChildRunSlot::new("branch").unwrap(),
                    &descriptor(child),
                    child,
                )
                .unwrap()],
            )
            .unwrap(),
        )
        .unwrap();
    let admission = Box::pin(admit(store, tenant, RunId::generate(), &root, &leaf)).await;
    let fence = store
        .claim_lease(
            tenant,
            admission.admission().intent().provenance().run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &root.frame_calls().unwrap().calls()[0],
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
        panic!("the original frame must be real");
    };
    let planner =
        ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
            .unwrap();
    let plan = planner
        .finish(entry.event().head(), entry.event().recorded_at())
        .unwrap();
    let start = Box::pin(store.start_recovered_graph_frame_node_attempt(
        worker_append(
            tenant.clone(),
            fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(entry.event().head()),
            fence.clone(),
            45_100,
        ),
        &plan,
        &NodeId::new("finish").unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    Parent {
        admission,
        frame: Some(entry.entry().checkpoint().clone()),
        node: start.attempt().start().head(),
        fence,
    }
}

async fn root_parent(store: &PostgresStore, admission: StoredAgentAdmission) -> Parent {
    let identity = admission.admission().intent().provenance();
    let fence = store
        .claim_lease(
            identity.tenant_id(),
            identity.run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let activation =
        NodeActivation::for_ready_root(admission.checkpoint(), NodeId::new("finish").unwrap())
            .unwrap();
    let start = Box::pin(store.start_node_attempt(
        worker_append(
            identity.tenant_id().clone(),
            identity.run_id(),
            EventId::generate(),
            JournalExpectation::exact(admission.event().head()),
            fence.clone(),
            45_101,
        ),
        activation,
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    Parent {
        admission,
        frame: None,
        node: start.attempt().start().head(),
        fence,
    }
}

fn child_intent(
    parent: &Parent,
    child: &CompiledGraph,
    slot: &str,
    grandchild: bool,
) -> ChildRunAdmissionIntent {
    let original = parent.admission.admission().intent();
    let agent = descriptor(child);
    // These fixtures perform no provider I/O. Narrow every cumulative reserve
    // so the second grandchild is permitted by budget and the per-Run child cap.
    let limits = original
        .request()
        .budget_limits()
        .clone()
        .with_graph_steps(ExecutionCount::new(if grandchild { 2 } else { 8 }))
        .with_graph_depth(ExecutionCount::new(2))
        .with_model_attempts(ExecutionCount::ZERO)
        .with_model_turns(ExecutionCount::ZERO)
        .with_input_tokens(TokenCount::ZERO)
        .with_cached_input_tokens(TokenCount::ZERO)
        .with_reasoning_tokens(TokenCount::ZERO)
        .with_output_tokens(TokenCount::ZERO)
        .with_tool_calls(ExecutionCount::ZERO)
        .with_write_calls(ExecutionCount::ZERO)
        .with_remote_agent_delegations(ExecutionCount::ZERO)
        .with_retries(ExecutionCount::ZERO)
        .with_concurrent_branches(ExecutionCount::new(1))
        .with_fan_out(ExecutionCount::new(1))
        .with_input_bytes(ByteCount::new(if grandchild { 128 } else { 512 }))
        .with_output_bytes(ByteCount::new(if grandchild { 128 } else { 512 }))
        .with_event_bytes(ByteCount::new(if grandchild { 131_072 } else { 1_048_576 }))
        .with_checkpoint_bytes(ByteCount::new(if grandchild { 131_072 } else { 1_048_576 }))
        .with_artifact_bytes(ByteCount::ZERO)
        .with_costs(CostLimits::try_new([]).unwrap());
    let intent = AgentAdmissionIntent::new(
        AgentResultProvenance::for_agent(
            original.provenance().tenant_id().clone(),
            RunId::generate(),
            ThreadId::generate(),
            InvocationId::generate(),
            &agent,
        ),
        agent,
        AgentRequest::new(
            child.input_schema().clone(),
            BoundedJson::try_from(json!({"count":1})).unwrap(),
            limits,
        ),
        original.budget_layers().iter().cloned(),
        child.reference(),
        original.authority().clone(),
    )
    .unwrap();
    let key = ChildRunKey::new(
        parent.node.activation().clone(),
        ChildRunSlot::new(slot).unwrap(),
    )
    .unwrap();
    let state = CheckpointState::new(
        child.state_schema().clone(),
        BoundedJson::try_from(json!({"count":1})).unwrap(),
    )
    .unwrap();
    match &parent.frame {
        Some(frame) => ChildRunAdmissionIntent::for_frame(
            parent.admission.admission(),
            frame,
            key,
            intent,
            child.clone(),
            state,
        ),
        None => ChildRunAdmissionIntent::new(
            parent.admission.admission(),
            key,
            intent,
            child.clone(),
            state,
        ),
    }
    .unwrap()
}

async fn direct_usage(store: &PostgresStore, parent: &Parent) -> (JournalHead, BudgetUsage) {
    let page = store
        .load_journal_page(
            parent.fence.tenant_id(),
            parent.fence.run_id(),
            None,
            JournalPageSize::new(32).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    let head = page.events().last().unwrap().head();
    let bytes: usize = page
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len())
        .sum();
    let mut checkpoints = serde_json_canonicalizer::to_vec(parent.admission.checkpoint())
        .unwrap()
        .len();
    let graph_depth = if let Some(frame) = &parent.frame {
        checkpoints += serde_json_canonicalizer::to_vec(frame.checkpoint())
            .unwrap()
            .len()
            + serde_json_canonicalizer::to_vec(&frame.head())
                .unwrap()
                .len();
        2
    } else {
        1
    };
    let usage = BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(graph_depth))
        .graph_steps(ExecutionCount::new(graph_depth))
        .concurrent_branches(ExecutionCount::new(1))
        .fan_out(ExecutionCount::new(1))
        .event_bytes(ByteCount::new(bytes as u64))
        .checkpoint_bytes(ByteCount::new(checkpoints as u64))
        .input_bytes(ByteCount::new(
            serde_json_canonicalizer::to_vec(
                parent.admission.admission().intent().request().input(),
            )
            .unwrap()
            .len() as u64,
        ))
        .build()
        .unwrap();
    (head, usage)
}

async fn assert_capacity(store: &PostgresStore, parent: &Parent, intent: &ChildRunAdmissionIntent) {
    let (head, usage) = Box::pin(direct_usage(store, parent)).await;
    let root_graph = store
        .load_graph_definition(
            parent.fence.tenant_id(),
            parent.admission.admission().intent().graph(),
        )
        .await
        .unwrap();
    let at = store
        .observe_live_lease(&parent.fence)
        .await
        .unwrap()
        .observed_at();
    let account = store
        .load_child_budget_account(parent.fence.tenant_id(), parent.fence.run_id())
        .await
        .unwrap()
        .map_or_else(
            || {
                ChildRunBudgetAccount::new(
                    parent.admission.admission(),
                    root_graph.graph(),
                    head.clone(),
                    usage.clone(),
                )
            },
            |account| account.observe_direct(head.clone(), usage.clone()),
        )
        .unwrap();
    if let Some(frame) = &parent.frame {
        let graph = store
            .load_graph_definition(parent.fence.tenant_id(), frame.checkpoint().graph())
            .await
            .unwrap();
        intent
            .validate_for_frame(parent.admission.admission(), frame, &AcceptGraphSchemas, at)
            .unwrap();
        account
            .reserve_for_frame(intent, frame, graph.graph(), at)
            .unwrap();
    } else {
        intent
            .validate_for(
                parent.admission.admission(),
                parent.admission.checkpoint(),
                &AcceptGraphSchemas,
                at,
            )
            .unwrap();
        account.reserve(intent, root_graph.graph(), at).unwrap();
    }
}

async fn spawn(
    store: &PostgresStore,
    parent: &Parent,
    intent: &ChildRunAdmissionIntent,
) -> Result<ChildRunCommitOutcome, StoreError> {
    let (head, usage) = Box::pin(direct_usage(store, parent)).await;
    let child = intent.child().provenance();
    let append = JournalAppend::new(
        JournalExpectation::exact(head),
        JournalEventIntent::worker(
            parent.fence.tenant_id().clone(),
            parent.fence.run_id(),
            EventId::generate(),
            parent.fence.clone(),
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
        intent.child().graph().clone(),
        intent.initial_state().clone(),
        intent.child_graph().entry_nodes().clone(),
    )
    .unwrap();
    Box::pin(store.admit_child_run(
        intent.clone(),
        &parent.node,
        append,
        child_append,
        checkpoint,
        usage,
        &AcceptGraphSchemas,
    ))
    .await
}

async fn snapshot(db: &PgPool, tenant: &TenantId) -> Vec<(String, Digest)> {
    let tables = query_scalar::<_, String>("SELECT table_name FROM information_schema.columns WHERE table_schema='stateknot' AND column_name='tenant_id' ORDER BY table_name").fetch_all(db).await.unwrap();
    let mut result = Vec::new();
    for table in tables {
        assert!(
            table
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        );
        let sql = format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM stateknot.{table} t WHERE tenant_id=$1"
        );
        let rows = query_scalar::<_, String>(&sql)
            .bind(tenant.as_str())
            .fetch_one(db)
            .await
            .unwrap();
        result.push((table, Digest::sha256(rows.as_bytes())));
    }
    result
}

async fn rejects_grandchild(
    store: &PostgresStore,
    owner_limits: ChildRunTopologyLimits,
    allow_first: bool,
    name: &str,
) {
    let tenant = tenant(name);
    let final_graph = single_node_graph("topology-grandchild");
    let child_graph = declared(
        single_node_graph("topology-child"),
        &final_graph,
        &["first", "second"],
        ChildRunTopologyLimits::new(1, 8, 4).unwrap(),
    );
    for graph in [&final_graph, &child_graph] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let original = Box::pin(frame_parent(store, &tenant, &child_graph, owner_limits)).await;
    let child_intent = child_intent(&original, &child_graph, "branch", false);
    Box::pin(assert_capacity(store, &original, &child_intent)).await;
    let child = Box::pin(spawn(store, &original, &child_intent))
        .await
        .unwrap();
    assert!(matches!(child, ChildRunCommitOutcome::Committed(_)));
    let parent = Box::pin(root_parent(store, child.record().child().clone())).await;
    if allow_first {
        let intent = self::child_intent(&parent, &final_graph, "first", true);
        Box::pin(assert_capacity(store, &parent, &intent)).await;
        let first = Box::pin(spawn(store, &parent, &intent)).await.unwrap();
        assert!(matches!(first, ChildRunCommitOutcome::Committed(_)));
        assert_eq!(
            first.record().ancestors(),
            &[original.fence.run_id(), parent.fence.run_id()]
        );
    }
    let intent = self::child_intent(
        &parent,
        &final_graph,
        if allow_first { "second" } else { "first" },
        true,
    );
    let db = pool().await;
    let before = snapshot(&db, &tenant).await;
    Box::pin(assert_capacity(store, &parent, &intent)).await;
    let result = Box::pin(spawn(store, &parent, &intent)).await;
    assert!(
        matches!(result, Err(StoreError::ChildRunTopologyExceeded)),
        "unexpected result {result:?}"
    );
    assert_eq!(snapshot(&db, &tenant).await, before);
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, original.fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *original.admission.checkpoint()
    );
    assert_eq!(
        store
            .load_active_graph_frame(&tenant, original.fence.run_id())
            .await
            .unwrap()
            .unwrap()
            .checkpoint(),
        original.frame.as_ref().unwrap()
    );
    assert_eq!(
        store
            .load_child_budget_account(&tenant, original.fence.run_id())
            .await
            .unwrap()
            .unwrap()
            .children()
            .len(),
        1
    );
    assert_eq!(
        store
            .load_child_budget_account(&tenant, parent.fence.run_id())
            .await
            .unwrap()
            .map(|account| account.children().len()),
        allow_first.then_some(1)
    );
    assert!(matches!(
        store
            .load_run(&tenant, intent.child().provenance().run_id())
            .await,
        Err(StoreError::RunNotFound)
    ));
    db.close().await;
}

#[tokio::test]
async fn original_frame_depth_ceiling_rejects_a_declared_target_with_too_much_depth() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-child-original-depth");
    let final_graph = single_node_graph("topology-grandchild");
    let child_graph = declared(
        single_node_graph("topology-child"),
        &final_graph,
        &["first", "second"],
        ChildRunTopologyLimits::new(1, 8, 4).unwrap(),
    );
    for graph in [&final_graph, &child_graph] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let original = Box::pin(frame_parent(
        &store,
        &tenant,
        &child_graph,
        ChildRunTopologyLimits::new(1, 8, 4).unwrap(),
    ))
    .await;
    let intent = child_intent(&original, &child_graph, "branch", false);
    let owner = store
        .load_graph_definition(
            &tenant,
            original.frame.as_ref().unwrap().checkpoint().graph(),
        )
        .await
        .unwrap();
    assert!(matches!(
        intent.validate_declaration(owner.graph()),
        Err(ChildRunPolicyError::DepthNotNarrowed)
    ));
    let db = pool().await;
    let before = snapshot(&db, &tenant).await;
    assert!(matches!(
        Box::pin(spawn(&store, &original, &intent)).await,
        Err(StoreError::ChildRunRejected)
    ));
    assert_eq!(snapshot(&db, &tenant).await, before);
    assert!(
        store
            .load_child_budget_account(&tenant, original.fence.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store
            .load_run(&tenant, intent.child().provenance().run_id())
            .await,
        Err(StoreError::RunNotFound)
    ));
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn original_frame_active_ceiling_refuses_the_next_valid_root_grandchild() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    Box::pin(rejects_grandchild(
        &store,
        ChildRunTopologyLimits::new(2, 8, 2).unwrap(),
        true,
        "frame-child-original-active",
    ))
    .await;
    store.close().await;
}

#[tokio::test]
async fn original_frame_room_allows_two_grandchildren_in_the_same_per_run_account() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-child-original-room");
    let final_graph = single_node_graph("topology-grandchild");
    let child_graph = declared(
        single_node_graph("topology-child"),
        &final_graph,
        &["first", "second"],
        ChildRunTopologyLimits::new(1, 8, 4).unwrap(),
    );
    for graph in [&final_graph, &child_graph] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let original = Box::pin(frame_parent(
        &store,
        &tenant,
        &child_graph,
        ChildRunTopologyLimits::new(2, 8, 4).unwrap(),
    ))
    .await;
    let intent = child_intent(&original, &child_graph, "branch", false);
    Box::pin(assert_capacity(&store, &original, &intent)).await;
    let child = Box::pin(spawn(&store, &original, &intent)).await.unwrap();
    assert!(matches!(child, ChildRunCommitOutcome::Committed(_)));
    let parent = Box::pin(root_parent(&store, child.record().child().clone())).await;
    for slot in ["first", "second"] {
        let intent = child_intent(&parent, &final_graph, slot, true);
        Box::pin(assert_capacity(&store, &parent, &intent)).await;
        let committed = Box::pin(spawn(&store, &parent, &intent)).await.unwrap();
        assert!(matches!(committed, ChildRunCommitOutcome::Committed(_)));
        assert_eq!(
            committed.record().ancestors(),
            &[original.fence.run_id(), parent.fence.run_id()]
        );
    }
    assert_eq!(
        store
            .load_child_budget_account(&tenant, original.fence.run_id())
            .await
            .unwrap()
            .unwrap()
            .children()
            .len(),
        1
    );
    assert_eq!(
        store
            .load_child_budget_account(&tenant, parent.fence.run_id())
            .await
            .unwrap()
            .unwrap()
            .children()
            .len(),
        2
    );
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, parent.fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *parent.admission.checkpoint()
    );
    store.close().await;
}
