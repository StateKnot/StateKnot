// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real scoped barriers, successor dispatch and complete acknowledgment recovery.
use super::frame_transactions::{admit, caller_graph, graphs, initial_usage};
use super::*;
use stateknot_core::{
    GraphBarrierDisposition, GraphFrameBarrierPlan, GraphFrameCall, GraphFrameCallPolicy,
    GraphFrameCheckpoint, GraphFrameEntryPlan, GraphNode, GraphRoute, GraphRoutes,
    NodeTerminalOutput, PendingNodeResult, ReadyNodeRecoveryPlanner, RouteId, RunFence,
};
use stateknot_store_postgres::{
    GraphFrameBarrierCommitOutcome, GraphFrameEntryCommitOutcome, StoredGraphFrameBarrier,
    StoredGraphFrameEntry,
};

async fn enter(
    store: &PostgresStore,
    name: &str,
    two_nodes: bool,
) -> (
    StoredAgentAdmission,
    StoredGraphFrameEntry,
    CompiledGraph,
    RunFence,
) {
    let (_, child, _) = graphs();
    let child = if two_nodes {
        CompiledGraph::compile(
            child.identity().clone(),
            child.input_schema().clone(),
            child.state_schema().clone(),
            child.update_schema().clone(),
            child.output_schema().clone(),
            child.reducer().clone(),
            ReadyNodes::try_new([NodeId::new("first").unwrap()]).unwrap(),
            [
                GraphNode::new(
                    NodeId::new("first").unwrap(),
                    Some(ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap()),
                    GraphRoutes::default(),
                    None,
                    false,
                )
                .unwrap(),
                GraphNode::new(
                    NodeId::new("finish").unwrap(),
                    None,
                    GraphRoutes::default(),
                    None,
                    true,
                )
                .unwrap(),
            ],
            child.limits(),
        )
        .unwrap()
    } else {
        child
    };
    Box::pin(enter_graph(store, name, child, &[])).await
}

async fn enter_graph(
    store: &PostgresStore,
    name: &str,
    child: CompiledGraph,
    dependencies: &[CompiledGraph],
) -> (
    StoredAgentAdmission,
    StoredGraphFrameEntry,
    CompiledGraph,
    RunFence,
) {
    let parent = caller_graph("barrier-parent", &child, 4096);
    let tenant = tenant(name);
    let run = RunId::generate();
    for dependency in dependencies {
        store
            .register_graph_definition(tenant.clone(), dependency.clone())
            .await
            .unwrap();
    }
    let admission = Box::pin(admit(store, &tenant, run, &parent, &child)).await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let call = GraphFrameCall::new(
        NodeId::new("call").unwrap(),
        NodeId::new("slot.a").unwrap(),
        &child,
        RouteId::new("return").unwrap(),
    )
    .unwrap();
    let plan = GraphFrameEntryPlan::for_root(
        &call,
        &parent,
        admission.checkpoint(),
        &child,
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
        panic!("fresh entry");
    };
    (admission, entry, child, fence)
}

async fn succeed(
    store: &PostgresStore,
    base: &GraphFrameCheckpoint,
    fence: &RunFence,
    control: NodeControl,
) -> PendingNodeResult {
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let observed = run.journal_head().unwrap();
    let planner = ReadyNodeRecoveryPlanner::for_frame(base.clone(), fence.clone())
        .unwrap()
        .finish(observed.clone(), observed.recorded_at())
        .unwrap();
    let start = Box::pin(store.start_recovered_graph_frame_node_attempt(
        worker_append(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(observed.clone()),
            fence.clone(),
            23_000,
        ),
        &planner,
        base.checkpoint().ready_nodes().iter().next().unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let intent = PendingNodeResultIntent::new(
        start.attempt().start().activation().clone(),
        NodeStateChange::Unchanged,
        control,
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    let append = worker_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        JournalExpectation::exact(start.event().head()),
        fence.clone(),
        23_001,
    );
    Box::pin(store.succeed_node_attempt(
        append,
        &start.attempt().start().head(),
        intent.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    store
        .load_pending_node_result(intent.activation())
        .await
        .unwrap()
}
fn terminal(graph: &CompiledGraph) -> NodeControl {
    NodeControl::Terminal {
        output: NodeTerminalOutput::new(
            graph.output_schema().clone(),
            BoundedJson::try_from(json!({"count":7,"source":"terminal"})).unwrap(),
        )
        .unwrap(),
    }
}
fn plan(
    graph: &CompiledGraph,
    base: &GraphFrameCheckpoint,
    result: &PendingNodeResult,
) -> GraphFrameBarrierPlan {
    graph
        .plan_frame_barrier(
            base,
            std::slice::from_ref(result),
            CheckpointId::generate(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone(),
            },
        )
        .unwrap()
}
async fn commit(
    store: &PostgresStore,
    plan: GraphFrameBarrierPlan,
    fence: &RunFence,
    usage: BudgetUsage,
    graph: &CompiledGraph,
) -> StoredGraphFrameBarrier {
    let observed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let GraphFrameBarrierCommitOutcome::Committed(record) =
        Box::pin(store.commit_graph_frame_barrier(
            plan,
            EventId::generate(),
            fence.clone(),
            observed,
            usage,
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone(),
            },
        ))
        .await
        .unwrap()
    else {
        panic!("fresh barrier");
    };
    record
}

struct NoSchemas;
impl GraphSchemaValidator for NoSchemas {
    fn validate(
        &self,
        _: &SchemaReference,
        _: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        panic!("idempotent retry must authenticate before schema callbacks")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn scoped_barrier_race_consumes_once_and_whole_retry_survives_takeover() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, graph, fence) =
        Box::pin(enter(&store, "frame-barrier-race", false)).await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        terminal(&graph),
    ))
    .await;
    let plan = Arc::new(plan(&graph, entry.entry().checkpoint(), &result));
    let observed = result.journal_head().clone();
    let usage = entry.direct_usage_after().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let plan = Arc::clone(&plan);
        let fence = fence.clone();
        let observed = observed.clone();
        let usage = usage.clone();
        let reducer = IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        };
        tasks.spawn(async move {
            Box::pin(store.commit_graph_frame_barrier(
                (*plan).clone(),
                EventId::generate(),
                fence,
                observed,
                usage,
                &AcceptGraphSchemas,
                &reducer,
            ))
            .await
        });
    }
    let mut committed = 0;
    let mut idempotent = 0;
    let mut record = None;
    while let Some(outcome) = tasks.join_next().await {
        match outcome.unwrap().unwrap() {
            GraphFrameBarrierCommitOutcome::Committed(value) => {
                committed += 1;
                record = Some(value);
            }
            GraphFrameBarrierCommitOutcome::Idempotent(_) => idempotent += 1,
            _ => panic!("unexpected outcome"),
        }
    }
    assert_eq!((committed, idempotent), (1, 23));
    let record = record.unwrap();
    assert!(matches!(
        record.disposition(),
        GraphBarrierDisposition::Terminal { .. }
    ));
    assert!(record.checkpoint().checkpoint().ready_nodes().is_empty());
    assert_eq!(record.checkpoint().checkpoint().superstep().get(), 1);
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    assert_eq!(
        store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Active
    );
    store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap();
    let retry = Box::pin(store.commit_graph_frame_barrier(
        (*plan).clone(),
        EventId::generate(),
        fence.clone(),
        admission.event().head(),
        BudgetUsage::zero(),
        &NoSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap();
    let GraphFrameBarrierCommitOutcome::Idempotent(retry) = retry else {
        panic!("whole retry");
    };
    assert_eq!(retry.digest(), record.digest());
    assert_eq!(
        store
            .load_graph_frame_barrier(
                fence.tenant_id(),
                fence.run_id(),
                entry.entry().checkpoint().frame().namespace(),
                entry.entry().checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .unwrap()
            .unwrap()
            .digest(),
        record.digest()
    );
    assert_eq!(
        store
            .load_graph_frame_checkpoint(
                fence.tenant_id(),
                fence.run_id(),
                record.checkpoint().frame().namespace(),
                record.checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .unwrap(),
        *record.checkpoint()
    );
    assert_eq!(
        store
            .load_pending_node_result(result.intent().activation())
            .await
            .unwrap(),
        result
    );
    store.close().await;
}

#[tokio::test]
async fn scoped_successor_dispatch_authenticates_all_barriers_and_keeps_root_suspended() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, graph, fence) =
        Box::pin(enter(&store, "frame-barrier-continuation", true)).await;
    let first = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        NodeControl::Continue,
    ))
    .await;
    let first_plan = plan(&graph, entry.entry().checkpoint(), &first);
    let first_barrier = Box::pin(commit(
        &store,
        first_plan,
        &fence,
        entry.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    assert!(matches!(
        first_barrier.disposition(),
        GraphBarrierDisposition::Continue
    ));
    let final_result = Box::pin(succeed(
        &store,
        first_barrier.checkpoint(),
        &fence,
        terminal(&graph),
    ))
    .await;
    let final_plan = plan(&graph, first_barrier.checkpoint(), &final_result);
    let final_barrier = Box::pin(commit(
        &store,
        final_plan,
        &fence,
        first_barrier.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    assert_eq!(final_barrier.checkpoint().checkpoint().superstep().get(), 2);
    assert_eq!(
        store
            .load_graph_frame_checkpoint(
                fence.tenant_id(),
                fence.run_id(),
                final_barrier.checkpoint().frame().namespace(),
                final_barrier.checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .unwrap(),
        *final_barrier.checkpoint()
    );
    assert_eq!(
        store
            .load_pending_node_result(final_result.intent().activation())
            .await
            .unwrap(),
        final_result
    );
    assert_eq!(
        store
            .load_pending_node_result(first.intent().activation())
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    store.close().await;
}

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap()
}
async fn snapshot(pool: &PgPool, fence: &RunFence) -> (Vec<i64>, String, String) {
    let mut counts = Vec::new();
    for table in [
        "run_events",
        "run_checkpoints",
        "pending_node_results",
        "pending_node_result_consumptions",
        "graph_frame_barriers",
        "node_attempts",
        "node_attempt_completions",
    ] {
        counts.push(
            query_scalar(&format!(
                "SELECT count(*) FROM stateknot.{table} WHERE tenant_id=$1 AND run_id=$2"
            ))
            .bind(fence.tenant_id().as_str())
            .bind(*fence.run_id().as_uuid())
            .fetch_one(pool)
            .await
            .unwrap(),
        );
    }
    let run = query_scalar(
        "SELECT to_jsonb(r)::text FROM stateknot.runs r WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    let head=query_scalar("SELECT jsonb_agg(to_jsonb(h) ORDER BY graph_namespace)::text FROM stateknot.graph_frame_heads h WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(pool).await.unwrap();
    (counts, run, head)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_barrier_write_faults_roll_back_event_state_consumption_witness_and_heads() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, fence) = Box::pin(enter(&store, "frame-barrier-faults", false)).await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        terminal(&graph),
    ))
    .await;
    let plan = plan(&graph, entry.entry().checkpoint(), &result);
    let pool = pool().await;
    let before = snapshot(&pool, &fence).await;
    query("CREATE FUNCTION stateknot.test_frame_barrier_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected frame barrier write fault'; END $$").execute(&pool).await.unwrap();
    for (table, action) in [
        ("run_events", "INSERT"),
        ("run_checkpoints", "INSERT"),
        ("pending_node_result_consumptions", "INSERT"),
        ("graph_frame_barriers", "INSERT"),
        ("graph_frame_heads", "UPDATE"),
        ("runs", "UPDATE"),
    ] {
        query(&format!("CREATE TRIGGER test_frame_barrier_fault BEFORE {action} ON stateknot.{table} FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_barrier_fault()"))
            .execute(&pool).await.unwrap();
        let outcome = Box::pin(store.commit_graph_frame_barrier(
            plan.clone(),
            EventId::generate(),
            fence.clone(),
            result.journal_head().clone(),
            entry.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone(),
            },
        ))
        .await;
        query(&format!(
            "DROP TRIGGER test_frame_barrier_fault ON stateknot.{table}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        assert!(outcome.is_err(), "fault must hit {table}");
        assert_eq!(
            snapshot(&pool, &fence).await,
            before,
            "partial fact after {table}"
        );
        assert!(
            store
                .load_graph_frame_barrier(
                    fence.tenant_id(),
                    fence.run_id(),
                    entry.entry().checkpoint().frame().namespace(),
                    entry.entry().checkpoint().checkpoint().checkpoint_id()
                )
                .await
                .unwrap()
                .is_none()
        );
    }
    query("DROP FUNCTION stateknot.test_frame_barrier_fault()")
        .execute(&pool)
        .await
        .unwrap();
    let record = Box::pin(commit(
        &store,
        plan,
        &fence,
        entry.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    assert_eq!(record.barrier().result_heads().len(), 1);
    store.verify_schema().await.unwrap();
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_barrier_stale_fence_and_incomplete_shared_usage_create_no_facts() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, fence) = Box::pin(enter(&store, "frame-barrier-authority", false)).await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        terminal(&graph),
    ))
    .await;
    let plan = plan(&graph, entry.entry().checkpoint(), &result);
    let next = store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let observed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let pool = pool().await;
    let before = snapshot(&pool, &fence).await;
    let reducer = IntegrationGraphReducer {
        reference: graph.reducer().clone(),
    };
    assert!(matches!(
        Box::pin(store.commit_graph_frame_barrier(
            plan.clone(),
            EventId::generate(),
            fence.clone(),
            observed.clone(),
            entry.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
            &reducer
        ))
        .await,
        Err(StoreError::StaleFence)
    ));
    assert_eq!(snapshot(&pool, &fence).await, before);
    assert!(matches!(
        Box::pin(store.commit_graph_frame_barrier(
            plan.clone(),
            EventId::generate(),
            next.clone(),
            observed,
            BudgetUsage::zero(),
            &AcceptGraphSchemas,
            &reducer
        ))
        .await,
        Err(StoreError::IncompleteChildAccounting)
    ));
    assert_eq!(snapshot(&pool, &fence).await, before);
    Box::pin(commit(
        &store,
        plan,
        &next,
        entry.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_barrier_rechecks_lease_after_slow_deferred_constraints() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let (_, entry, graph, fence) = Box::pin(enter(&store, "frame-barrier-late-fence", false)).await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        terminal(&graph),
    ))
    .await;
    let plan = plan(&graph, entry.entry().checkpoint(), &result);
    let pool = pool().await;
    query("CREATE FUNCTION stateknot.test_frame_barrier_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.1); RETURN NEW; END $$").execute(&pool).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER test_frame_barrier_delay AFTER INSERT ON stateknot.graph_frame_barriers DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_barrier_delay()")
        .execute(&pool).await.unwrap();
    let fence = store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let before = snapshot(&pool, &fence).await;
    let outcome = Box::pin(store.commit_graph_frame_barrier(
        plan,
        EventId::generate(),
        fence.clone(),
        result.journal_head().clone(),
        entry.direct_usage_after().unwrap(),
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await;
    query("DROP TRIGGER test_frame_barrier_delay ON stateknot.graph_frame_barriers")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_frame_barrier_delay()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        matches!(outcome, Err(StoreError::LeaseExpired)),
        "late barrier authority: {outcome:?}"
    );
    assert_eq!(snapshot(&pool, &fence).await, before);
    store.verify_schema().await.unwrap();
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn nested_entry_after_scoped_barrier_authenticates_parent_successor_and_usage_floor() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, leaf, _) = graphs();
    let finish = ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap();
    let mut middle = CompiledGraph::compile(
        CapabilityIdentity::new(
            leaf.identity().owner().clone(),
            CapabilityReference::new("barrier-middle".parse().unwrap(), Version::new(1, 0, 0)),
        ),
        leaf.input_schema().clone(),
        leaf.state_schema().clone(),
        leaf.update_schema().clone(),
        leaf.output_schema().clone(),
        leaf.reducer().clone(),
        ReadyNodes::try_new([NodeId::new("first").unwrap()]).unwrap(),
        [
            GraphNode::new(
                NodeId::new("first").unwrap(),
                Some(ReadyNodes::try_new([NodeId::new("call").unwrap()]).unwrap()),
                GraphRoutes::default(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("call").unwrap(),
                None,
                GraphRoutes::try_new([
                    GraphRoute::new(RouteId::new("return").unwrap(), finish).unwrap()
                ])
                .unwrap(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("finish").unwrap(),
                None,
                GraphRoutes::default(),
                None,
                true,
            )
            .unwrap(),
        ],
        leaf.limits(),
    )
    .unwrap();
    let call = GraphFrameCall::new(
        NodeId::new("call").unwrap(),
        NodeId::new("slot.a").unwrap(),
        &leaf,
        RouteId::new("return").unwrap(),
    )
    .unwrap();
    middle = middle
        .with_frame_calls(GraphFrameCallPolicy::new(7, 4096, [call.clone()]).unwrap())
        .unwrap();
    // Admission resolves the whole declared closure; the target is registered
    // before admitting the actual Root/middle definitions.
    let (admission, entry, middle, fence) = Box::pin(enter_graph(
        &store,
        "frame-barrier-parent-continuation",
        middle,
        std::slice::from_ref(&leaf),
    ))
    .await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        NodeControl::Continue,
    ))
    .await;
    let barrier = Box::pin(commit(
        &store,
        plan(&middle, entry.entry().checkpoint(), &result),
        &fence,
        entry.direct_usage_after().unwrap(),
        &middle,
    ))
    .await;
    let candidate = GraphFrameEntryPlan::for_frame(
        &call,
        &middle,
        barrier.checkpoint(),
        &leaf,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence.clone(),
    )
    .unwrap();
    assert!(matches!(
        Box::pin(store.enter_graph_frame(
            candidate.clone(),
            EventId::generate(),
            barrier.event().head(),
            entry.direct_usage_after().unwrap(),
            &AcceptGraphSchemas
        ))
        .await,
        Err(StoreError::IncompleteChildAccounting)
    ));
    let GraphFrameEntryCommitOutcome::Committed(nested) = Box::pin(store.enter_graph_frame(
        candidate,
        EventId::generate(),
        barrier.event().head(),
        barrier.direct_usage_after().unwrap(),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap() else {
        panic!("nested continuation must commit");
    };
    assert_eq!(
        nested
            .entry()
            .checkpoint()
            .frame()
            .origin()
            .base_checkpoint(),
        &barrier.checkpoint().checkpoint().head()
    );
    assert_eq!(
        store
            .load_graph_frame_entry(
                fence.tenant_id(),
                fence.run_id(),
                nested.entry().checkpoint().frame().namespace()
            )
            .await
            .unwrap()
            .digest(),
        nested.digest()
    );
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    store.close().await;
}

#[tokio::test]
async fn reserved_scoped_barrier_event_cannot_commit_through_ordinary_append() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-barrier-reserved", false)).await;
    let (schema, _) = PostgresStore::graph_frame_barrier_event_schema().unwrap();
    let data = JournalPayload::new(
        schema,
        "graph-frame-barrier-committed".parse().unwrap(),
        BoundedJson::try_from(json!({"reserved":true})).unwrap(),
    )
    .unwrap();
    let pool = pool().await;
    let before = snapshot(&pool, &fence).await;
    let intent = JournalEventIntent::worker(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        fence.clone(),
        data.clone(),
    )
    .unwrap();
    let append =
        JournalAppend::new(JournalExpectation::exact(entry.event().head()), intent).unwrap();
    assert!(matches!(
        store
            .append_worker(append, RunProjection::unchanged())
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    assert_eq!(snapshot(&pool, &fence).await, before);
    let intent = JournalEventIntent::control_plane(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        data,
    )
    .unwrap();
    let append =
        JournalAppend::new(JournalExpectation::exact(entry.event().head()), intent).unwrap();
    assert!(matches!(
        store
            .append_control_plane(append, RunProjection::unchanged())
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    assert_eq!(snapshot(&pool, &fence).await, before);
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn rewritten_scoped_barrier_bytes_are_rejected_by_whole_and_checkpoint_recovery() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, fence) = Box::pin(enter(&store, "frame-barrier-corrupt", false)).await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        terminal(&graph),
    ))
    .await;
    let record = Box::pin(commit(
        &store,
        plan(&graph, entry.entry().checkpoint(), &result),
        &fence,
        entry.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    let pool = pool().await;
    let original: Vec<u8> = query_scalar(
        "SELECT barrier_bytes FROM stateknot.graph_frame_barriers WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut wire: serde_json::Value = serde_json::from_slice(&original).unwrap();
    wire["budget"]["direct_usage"]["graph_steps"] = json!("0");
    let damaged = serde_json_canonicalizer::to_vec(&wire).unwrap();
    assert_ne!(damaged, original);
    query(
        "ALTER TABLE stateknot.graph_frame_barriers DISABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&pool)
    .await
    .unwrap();
    query("UPDATE stateknot.graph_frame_barriers SET barrier_bytes=$3 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(damaged).execute(&pool).await.unwrap();
    query(
        "ALTER TABLE stateknot.graph_frame_barriers ENABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        store
            .load_graph_frame_barrier(
                fence.tenant_id(),
                fence.run_id(),
                record.checkpoint().frame().namespace(),
                entry.entry().checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .is_err()
    );
    assert!(
        store
            .load_graph_frame_checkpoint(
                fence.tenant_id(),
                fence.run_id(),
                record.checkpoint().frame().namespace(),
                record.checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .is_err()
    );
    query(
        "ALTER TABLE stateknot.graph_frame_barriers DISABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&pool)
    .await
    .unwrap();
    query("UPDATE stateknot.graph_frame_barriers SET barrier_bytes=$3 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(original).execute(&pool).await.unwrap();
    query(
        "ALTER TABLE stateknot.graph_frame_barriers ENABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        store
            .load_graph_frame_checkpoint(
                fence.tenant_id(),
                fence.run_id(),
                record.checkpoint().frame().namespace(),
                record.checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .unwrap(),
        *record.checkpoint()
    );
    store.verify_schema().await.unwrap();
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn source_schema_28_upgrade_preserves_populated_scoped_history_before_real_barrier_commit() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let original = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_frame_barrier_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url_with_name(&original, "postgres"))
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let url = database_url_with_name(&original, &name);
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(&url, options.clone()).await.unwrap();
    let (_, entry, graph, fence) = Box::pin(enter(&store, "frame-barrier-upgrade", false)).await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        terminal(&graph),
    ))
    .await;
    let candidate = plan(&graph, entry.entry().checkpoint(), &result);
    store.close().await;
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    // Reconstruct the actual source-28 guard bodies and migration inventory.
    // This proves a populated source-schema upgrade, not an old binary.
    sqlx_core::raw_sql::raw_sql(include_str!("../fixtures/revert_graph_frame_barriers.sql"))
        .execute(&fixture)
        .await
        .unwrap();
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        28
    );
    assert!(matches!(
        PostgresStore::connect(&url, options.clone()).await,
        Err(StoreError::IncompatibleSchema)
    ));
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let upgraded = PostgresStore::connect(&url, options).await.unwrap();
    assert_eq!(
        upgraded
            .load_graph_frame_entry(
                fence.tenant_id(),
                fence.run_id(),
                entry.entry().checkpoint().frame().namespace()
            )
            .await
            .unwrap()
            .digest(),
        entry.digest()
    );
    assert_eq!(
        upgraded
            .load_pending_node_result(result.intent().activation())
            .await
            .unwrap(),
        result
    );
    Box::pin(commit(
        &upgraded,
        candidate,
        &fence,
        entry.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    upgraded.verify_schema().await.unwrap();
    upgraded.close().await;
    fixture.close().await;
    query(&format!("DROP DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
