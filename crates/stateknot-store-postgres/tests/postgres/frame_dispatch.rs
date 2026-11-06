// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Actual scoped starts reuse the original Run and bounded physical history.
use super::frame_transactions::{admit, caller_graph, graphs, initial_usage};
use super::*;
use stateknot_core::{GraphFrameEntryPlan, ReadyNodeRecoveryPlan, RunFence};
use stateknot_store_postgres::{GraphFrameEntryCommitOutcome, StoredGraphFrameEntry};

async fn enter(
    store: &PostgresStore,
    name: &str,
) -> (StoredAgentAdmission, StoredGraphFrameEntry, RunFence) {
    let tenant = tenant(name);
    let run = RunId::generate();
    let (parent, child, call) = graphs();
    let admission = Box::pin(admit(store, &tenant, run, &parent, &child)).await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
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
        panic!("fresh entry must commit");
    };
    (admission, entry, fence)
}

async fn plan(
    store: &PostgresStore,
    entry: &StoredGraphFrameEntry,
    fence: &RunFence,
    previous: Option<&stateknot_core::NodeAttempt>,
) -> ReadyNodeRecoveryPlan {
    let head = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let mut planner =
        ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
            .unwrap();
    if let Some(previous) = previous {
        planner.observe_attempt(previous).unwrap();
    }
    planner.finish(head.clone(), head.recorded_at()).unwrap()
}

fn append(plan: &ReadyNodeRecoveryPlan, event: EventId) -> JournalAppend {
    worker_append(
        plan.fence().tenant_id().clone(),
        plan.fence().run_id(),
        event,
        JournalExpectation::exact(plan.journal_head().clone()),
        plan.fence().clone(),
        22_000,
    )
}

async fn snapshot(pool: &PgPool, fence: &RunFence) -> Vec<i64> {
    let mut counts = Vec::new();
    for table in [
        "run_events",
        "run_attempt_claims",
        "node_attempts",
        "run_checkpoints",
        "graph_frame_entries",
        "pending_node_results",
        "node_attempt_completions",
        "pending_node_result_model_bindings",
        "pending_node_result_tool_bindings",
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
    counts
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn scoped_start_race_commits_one_launch_and_acknowledgment_loss_remains_in_flight() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, fence) = Box::pin(enter(&store, "frame-dispatch-race")).await;
    let root_bytes = serde_json_canonicalizer::to_vec(admission.checkpoint()).unwrap();
    let plan = Arc::new(Box::pin(plan(&store, &entry, &fence, None)).await);
    let node = NodeId::new("finish").unwrap();
    let attempt_id = AttemptId::generate();
    let event_id = EventId::generate();
    assert!(matches!(
        store
            .start_node_attempt(
                append(&plan, event_id),
                plan.nodes()[0].activation().clone(),
                attempt_id
            )
            .await,
        Err(StoreError::InvalidNodeAttemptActivation)
    ));
    assert!(matches!(
        store
            .start_recovered_node_attempt(append(&plan, event_id), &plan, &node, attempt_id)
            .await,
        Err(StoreError::InvalidReadyNodeDispatchPlan)
    ));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let plan = Arc::clone(&plan);
        let node = node.clone();
        tasks.spawn(async move {
            for _ in 0..64 {
                match Box::pin(store.start_recovered_graph_frame_node_attempt(
                    append(&plan, event_id),
                    &plan,
                    &node,
                    attempt_id,
                ))
                .await
                {
                    result @ Ok(_) => return result,
                    Err(error) if error.is_retryable() => tokio::task::yield_now().await,
                    error @ Err(_) => return error,
                }
            }
            panic!("identical scoped starts must converge within the bound")
        });
    }
    let mut committed = 0;
    let mut idempotent = 0;
    let mut winner = None;
    while let Some(result) = tasks.join_next().await {
        let result = result.unwrap().unwrap();
        if let Some(winner) = &winner {
            assert_eq!(result.attempt().start().head(), *winner);
        } else {
            winner = Some(result.attempt().start().head());
        }
        match result {
            NodeAttemptCommitOutcome::Committed { .. } => committed += 1,
            NodeAttemptCommitOutcome::Idempotent { .. } => idempotent += 1,
            _ => panic!("unexpected outcome"),
        }
    }
    assert_eq!((committed, idempotent), (1, 23));
    let durable = store
        .load_node_attempt(fence.tenant_id(), &fence.run_id(), attempt_id)
        .await
        .unwrap();
    assert_eq!(durable.start().head(), winner.unwrap());
    let current = Box::pin(self::plan(&store, &entry, &fence, Some(&durable))).await;
    assert_eq!(current.nodes()[0].kind(), RecoveryNodeKind::InFlight);
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(&current, EventId::generate()),
            &current,
            &node,
            AttemptId::generate()
        ))
        .await,
        Err(StoreError::ReadyNodeNotDispatchable)
    ));
    // A stale pure first-attempt plan cannot bypass the complete same-fence history.
    let optimistic = Box::pin(self::plan(&store, &entry, &fence, None)).await;
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(&optimistic, EventId::generate()),
            &optimistic,
            &node,
            AttemptId::generate()
        ))
        .await,
        Err(StoreError::InvalidNodeAttemptTransition)
    ));
    let root = store
        .load_current_checkpoint(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(serde_json_canonicalizer::to_vec(&root).unwrap(), root_bytes);
    assert_eq!(
        store
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
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_start_requires_successor_fence_and_retains_historical_start_after_takeover() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, fence) = Box::pin(enter(&store, "frame-dispatch-takeover")).await;
    let first_plan = Box::pin(plan(&store, &entry, &fence, None)).await;
    let node = NodeId::new("finish").unwrap();
    let first_id = AttemptId::generate();
    let event = EventId::generate();
    let first = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(&first_plan, event),
        &first_plan,
        &node,
        first_id,
    ))
    .await
    .unwrap();
    let successor = store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let stale = Box::pin(plan(&store, &entry, &fence, None)).await;
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(&stale, EventId::generate()),
            &stale,
            &node,
            AttemptId::generate()
        ))
        .await,
        Err(StoreError::StaleFence)
    ));
    let recovered = Box::pin(plan(&store, &entry, &successor, Some(first.attempt()))).await;
    assert_eq!(
        recovered.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::SupersededAttempt)
    );
    let next_id = AttemptId::generate();
    let next = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(&recovered, EventId::generate()),
        &recovered,
        &node,
        next_id,
    ))
    .await
    .unwrap();
    assert!(matches!(next, NodeAttemptCommitOutcome::Committed { .. }));
    assert_eq!(next.attempt().start().fence(), &successor);
    assert_eq!(
        store
            .load_node_attempt(fence.tenant_id(), &fence.run_id(), first_id)
            .await
            .unwrap()
            .start()
            .head(),
        first.attempt().start().head()
    );
    // An acknowledged historical fact is readable after its fence is replaced.
    let lost_ack = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(&first_plan, event),
        &first_plan,
        &node,
        first_id,
    ))
    .await
    .unwrap();
    assert!(matches!(
        lost_ack,
        NodeAttemptCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(
        lost_ack.attempt().start().head(),
        first.attempt().start().head()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn suspended_frame_cannot_start_while_a_deeper_leaf_owns_the_stack() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-dispatch-suspended");
    let run = RunId::generate();
    let (_, leaf, _) = graphs();
    let middle = caller_graph("dispatch-middle", &leaf, 4096);
    let root = caller_graph("dispatch-root", &middle, 4096);
    store
        .register_graph_definition(tenant.clone(), leaf.clone())
        .await
        .unwrap();
    let admission = Box::pin(admit(&store, &tenant, run, &root, &middle)).await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let first = GraphFrameEntryPlan::for_root(
        &root.frame_calls().unwrap().calls()[0],
        &root,
        admission.checkpoint(),
        &middle,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence.clone(),
    )
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(first) = Box::pin(store.enter_graph_frame(
        first,
        EventId::generate(),
        admission.event().head(),
        initial_usage(&admission),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap() else {
        panic!("fresh entry");
    };
    let prior_plan = Box::pin(plan(&store, &first, &fence, None)).await;
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(&prior_plan, EventId::generate()),
            &prior_plan,
            prior_plan.nodes()[0].activation().node_id(),
            AttemptId::generate(),
        ))
        .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    let next = GraphFrameEntryPlan::for_frame(
        &middle.frame_calls().unwrap().calls()[0],
        &middle,
        first.entry().checkpoint(),
        &leaf,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence.clone(),
    )
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(next) = Box::pin(store.enter_graph_frame(
        next,
        EventId::generate(),
        first.event().head(),
        first.direct_usage_after().unwrap(),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap() else {
        panic!("nested entry");
    };
    let crossed_append = worker_append(
        tenant.clone(),
        run,
        EventId::generate(),
        JournalExpectation::exact(next.event().head()),
        fence.clone(),
        22_000,
    );
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            crossed_append,
            &prior_plan,
            prior_plan.nodes()[0].activation().node_id(),
            AttemptId::generate()
        ))
        .await,
        Err(StoreError::GraphFrameConflict)
    ));
    let leaf_plan = Box::pin(plan(&store, &next, &fence, None)).await;
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(&leaf_plan, EventId::generate()),
            &leaf_plan,
            &NodeId::new("finish").unwrap(),
            AttemptId::generate()
        ))
        .await
        .unwrap(),
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    // Both framework starts remain whole-record authenticated even after the
    // active leaf has moved beyond their base checkpoints.
    for entry in [&first, &next] {
        assert_eq!(
            store
                .load_node_attempt(&tenant, &run, entry.entry().start().attempt_id())
                .await
                .unwrap()
                .start(),
            entry.entry().start()
        );
    }
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_start_write_faults_roll_back_every_component_and_preserve_root() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, fence) = Box::pin(enter(&store, "frame-dispatch-write-faults")).await;
    let plan = Box::pin(plan(&store, &entry, &fence, None)).await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let before = snapshot(&pool, &fence).await;
    for (table, operation) in [
        ("run_events", "INSERT"),
        ("run_attempt_claims", "INSERT"),
        ("node_attempts", "INSERT"),
        ("runs", "UPDATE"),
    ] {
        query("CREATE FUNCTION stateknot.test_frame_dispatch_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected frame start fault'; END $$").execute(&pool).await.unwrap();
        query(&format!("CREATE TRIGGER test_frame_dispatch_fault BEFORE {operation} ON stateknot.{table} FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_dispatch_fault()"))
            .execute(&pool).await.unwrap();
        let event = EventId::generate();
        let attempt = AttemptId::generate();
        let result = Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(&plan, event),
            &plan,
            &NodeId::new("finish").unwrap(),
            attempt,
        ))
        .await;
        // Restore the exact schema even if the assertion below fails.
        query(&format!(
            "DROP TRIGGER test_frame_dispatch_fault ON stateknot.{table}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        query("DROP FUNCTION stateknot.test_frame_dispatch_fault()")
            .execute(&pool)
            .await
            .unwrap();
        assert!(result.is_err(), "fault at {table} committed");
        assert_eq!(
            snapshot(&pool, &fence).await,
            before,
            "partial facts at {table}"
        );
        assert!(matches!(
            store
                .load_node_attempt(fence.tenant_id(), &fence.run_id(), attempt)
                .await,
            Err(StoreError::NodeAttemptNotFound)
        ));
        assert_eq!(
            store
                .load_run(fence.tenant_id(), fence.run_id())
                .await
                .unwrap()
                .journal_head(),
            Some(&entry.event().head())
        );
    }
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    store.verify_schema().await.unwrap();
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_start_rechecks_database_clock_after_slow_deferred_constraints() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let (_, entry, first_fence) = Box::pin(enter(&store, "frame-dispatch-deferred-expiry")).await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    query("CREATE FUNCTION stateknot.test_frame_dispatch_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.1); RETURN NEW; END $$").execute(&pool).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER test_frame_dispatch_delay AFTER INSERT ON stateknot.node_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_dispatch_delay()")
        .execute(&pool).await.unwrap();
    let fence = store
        .supersede_lease(
            first_fence.tenant_id(),
            first_fence.run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let plan = Box::pin(plan(&store, &entry, &fence, None)).await;
    let before = snapshot(&pool, &fence).await;
    let attempt = AttemptId::generate();
    let result = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(&plan, EventId::generate()),
        &plan,
        &NodeId::new("finish").unwrap(),
        attempt,
    ))
    .await;
    query("DROP TRIGGER test_frame_dispatch_delay ON stateknot.node_attempts")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_frame_dispatch_delay()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        matches!(result, Err(StoreError::LeaseExpired)),
        "late authority check: {result:?}"
    );
    assert_eq!(snapshot(&pool, &fence).await, before);
    assert_eq!(
        store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .journal_head(),
        Some(&entry.event().head())
    );
    assert!(matches!(
        store
            .load_node_attempt(fence.tenant_id(), &fence.run_id(), attempt)
            .await,
        Err(StoreError::NodeAttemptNotFound)
    ));
    store.verify_schema().await.unwrap();
    pool.close().await;
    store.close().await;
}

fn completion_append(
    started: &NodeAttemptCommitOutcome,
    event: EventId,
    index: u64,
) -> JournalAppend {
    let start = started.attempt().start();
    worker_append(
        start.activation().tenant_id().clone(),
        start.activation().run_id(),
        event,
        JournalExpectation::exact(started.event().head()),
        start.fence().clone(),
        index,
    )
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_success_commits_exact_attempt_result_and_reloads_after_takeover() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, fence) = Box::pin(enter(&store, "frame-node-success")).await;
    let plan = Box::pin(plan(&store, &entry, &fence, None)).await;
    let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(&plan, EventId::generate()),
        &plan,
        &NodeId::new("finish").unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let intent = pending_result_intent(
        started.attempt().start().activation().clone(),
        NodeInvocationBindings::empty(),
    );
    let event = EventId::generate();
    let success_append = completion_append(&started, event, 22_001);
    let succeeded = Box::pin(store.succeed_node_attempt(
        success_append.clone(),
        &started.attempt().start().head(),
        intent.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        succeeded,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    assert_eq!(succeeded.attempt().status(), NodeAttemptStatus::Succeeded);
    let result = store
        .load_pending_node_result(intent.activation())
        .await
        .unwrap();
    assert_eq!(result.intent(), &intent);
    let completion = succeeded.attempt().completion().unwrap();
    assert_eq!(completion.journal_head(), result.journal_head());
    let mut planner =
        ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
            .unwrap();
    planner.observe_attempt(succeeded.attempt()).unwrap();
    planner.observe_result(&result).unwrap();
    let recovered = planner
        .finish(succeeded.event().head(), succeeded.event().recorded_at())
        .unwrap();
    assert_eq!(recovered.nodes()[0].kind(), RecoveryNodeKind::Completed);
    store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap();
    let retry = Box::pin(store.succeed_node_attempt(
        success_append,
        &started.attempt().start().head(),
        intent.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(
        retry
            .attempt()
            .completion()
            .map(stateknot_core::NodeAttemptCompletion::digest),
        succeeded
            .attempt()
            .completion()
            .map(stateknot_core::NodeAttemptCompletion::digest)
    );
    assert_eq!(
        store
            .load_node_attempt(
                fence.tenant_id(),
                &fence.run_id(),
                started.attempt().start().attempt_id()
            )
            .await
            .unwrap()
            .completion()
            .map(stateknot_core::NodeAttemptCompletion::digest),
        succeeded
            .attempt()
            .completion()
            .map(stateknot_core::NodeAttemptCompletion::digest)
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
#[allow(clippy::too_many_lines)]
async fn scoped_terminal_failure_is_durable_and_blocks_automatic_retry() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, fence) = Box::pin(enter(&store, "frame-node-failure")).await;
    let plan = Box::pin(plan(&store, &entry, &fence, None)).await;
    let node = NodeId::new("finish").unwrap();
    let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(&plan, EventId::generate()),
        &plan,
        &node,
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let event = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new("frame.node_failed").unwrap(),
        FailureOrigin::new("graph.frame").unwrap(),
        FailureMessage::new("The frame node failed safely.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(event);
    let failure_append = completion_append(&started, event, 22_002);
    let failed = Box::pin(store.fail_node_attempt(
        failure_append.clone(),
        &started.attempt().start().head(),
        failure.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert_eq!(failed.attempt().status(), NodeAttemptStatus::Failed);
    let recovered = Box::pin(self::plan(&store, &entry, &fence, Some(failed.attempt()))).await;
    assert_eq!(recovered.nodes()[0].kind(), RecoveryNodeKind::Failed);
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            append(&recovered, EventId::generate()),
            &recovered,
            &node,
            AttemptId::generate()
        ))
        .await,
        Err(StoreError::ReadyNodeNotDispatchable)
    ));
    let retry = Box::pin(store.fail_node_attempt(
        failure_append,
        &started.attempt().start().head(),
        failure,
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(
        store
            .load_node_attempt(
                fence.tenant_id(),
                &fence.run_id(),
                failed.attempt().start().attempt_id()
            )
            .await
            .unwrap()
            .completion()
            .map(stateknot_core::NodeAttemptCompletion::digest),
        failed
            .attempt()
            .completion()
            .map(stateknot_core::NodeAttemptCompletion::digest)
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn ordinary_node_completion_cannot_release_framework_owned_caller() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, fence) = Box::pin(enter(&store, "frame-caller-completion-guard")).await;
    let start = entry.entry().start();
    let event = EventId::generate();
    let append = worker_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        event,
        JournalExpectation::exact(entry.event().head()),
        fence.clone(),
        22_003,
    );
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new("frame.caller_failed").unwrap(),
        FailureOrigin::new("graph.frame").unwrap(),
        FailureMessage::new("The frame caller cannot finish independently.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(event);
    assert!(matches!(
        Box::pin(store.fail_node_attempt(
            append.clone(),
            &start.head(),
            failure,
            BudgetUsage::zero()
        ))
        .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    let intent = pending_result_intent(start.activation().clone(), NodeInvocationBindings::empty());
    assert!(matches!(
        Box::pin(store.succeed_node_attempt(append, &start.head(), intent, BudgetUsage::zero()))
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    assert_eq!(
        store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .journal_head(),
        Some(&entry.event().head())
    );
    assert_eq!(
        store
            .load_node_attempt(fence.tenant_id(), &fence.run_id(), start.attempt_id())
            .await
            .unwrap()
            .status(),
        NodeAttemptStatus::Executing
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_success_write_faults_roll_back_result_completion_and_journal() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, fence) = Box::pin(enter(&store, "frame-success-write-faults")).await;
    let plan = Box::pin(plan(&store, &entry, &fence, None)).await;
    let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append(&plan, EventId::generate()),
        &plan,
        &NodeId::new("finish").unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let intent = pending_result_intent(
        started.attempt().start().activation().clone(),
        NodeInvocationBindings::empty(),
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let before = snapshot(&pool, &fence).await;
    for (table, operation) in [
        ("run_events", "INSERT"),
        ("pending_node_results", "INSERT"),
        ("node_attempt_completions", "INSERT"),
        ("runs", "UPDATE"),
    ] {
        query("CREATE FUNCTION stateknot.test_frame_success_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected frame success fault'; END $$").execute(&pool).await.unwrap();
        query(&format!("CREATE TRIGGER test_frame_success_fault BEFORE {operation} ON stateknot.{table} FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_success_fault()"))
            .execute(&pool).await.unwrap();
        let result = Box::pin(store.succeed_node_attempt(
            completion_append(&started, EventId::generate(), 22_004),
            &started.attempt().start().head(),
            intent.clone(),
            BudgetUsage::zero(),
        ))
        .await;
        query(&format!(
            "DROP TRIGGER test_frame_success_fault ON stateknot.{table}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        query("DROP FUNCTION stateknot.test_frame_success_fault()")
            .execute(&pool)
            .await
            .unwrap();
        assert!(result.is_err(), "fault at {table} committed");
        assert_eq!(
            snapshot(&pool, &fence).await,
            before,
            "partial completion facts at {table}"
        );
        assert_eq!(
            store
                .load_node_attempt(
                    fence.tenant_id(),
                    &fence.run_id(),
                    started.attempt().start().attempt_id()
                )
                .await
                .unwrap()
                .status(),
            NodeAttemptStatus::Executing
        );
        assert_eq!(
            store
                .load_run(fence.tenant_id(), fence.run_id())
                .await
                .unwrap()
                .journal_head(),
            Some(&started.event().head())
        );
    }
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    store.verify_schema().await.unwrap();
    pool.close().await;
    store.close().await;
}
