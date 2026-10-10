// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Authenticated active stacks for real framework recovery and closure.
use super::frame_barriers::{commit, enter, enter_graph, plan, pool, snapshot, succeed, terminal};
use super::*;
use stateknot_core::{GraphFrameCheckpoint, NodeWait, NodeWaits, ReadyNodeRecoveryPlan, RunFence};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn active_snapshot_tracks_physical_takeover_terminal_proof_and_whole_return() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, graph, old) =
        Box::pin(enter(&store, "frame-active-takeover", false)).await;
    let initial = Box::pin(store.load_active_graph_frame(old.tenant_id(), old.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(initial.entry().digest(), entry.digest());
    assert_eq!(initial.checkpoint(), entry.entry().checkpoint());
    assert_eq!(initial.caller(), entry.entry().start());
    assert_eq!(initial.caller_binding_digest(), entry.digest());
    assert_eq!(initial.open_frames().len(), 1);
    let compact = &initial.open_frames()[0];
    assert_eq!(compact.checkpoint(), &initial.checkpoint().head());
    assert_eq!(compact.entry_digest(), initial.entry().digest());
    assert_eq!(compact.caller(), &initial.caller().head());
    assert_eq!(
        compact.caller_binding_digest(),
        initial.caller_binding_digest()
    );
    assert_eq!(
        initial.minimum_direct_usage(),
        &entry.direct_usage_after().unwrap()
    );
    let fence = store
        .supersede_lease(old.tenant_id(), old.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let binding = Box::pin(store.rebind_graph_frame_caller(
        entry.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        fence.clone(),
        entry.event().head(),
        entry.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    let usage = super::frame_callers::charged(&entry.direct_usage_after().unwrap(), &binding);
    let taken = Box::pin(store.load_active_graph_frame(old.tenant_id(), old.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(taken.entry().digest(), entry.digest());
    assert_eq!(taken.checkpoint(), initial.checkpoint());
    assert_eq!(taken.caller(), binding.attempt().start());
    assert_eq!(taken.caller().fence(), &fence);
    assert_eq!(taken.minimum_direct_usage(), &usage);
    let result = Box::pin(succeed(
        &store,
        taken.checkpoint(),
        &fence,
        terminal(&graph),
    ))
    .await;
    let final_plan = plan(&graph, taken.checkpoint(), &result);
    let barrier = Box::pin(commit(&store, final_plan.clone(), &fence, usage, &graph)).await;
    let saved = Box::pin(store.load_active_graph_frame(old.tenant_id(), old.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.checkpoint(), barrier.checkpoint());
    assert_eq!(
        saved.minimum_direct_usage(),
        &barrier.direct_usage_after().unwrap()
    );
    let returned = Box::pin(store.return_graph_frame(
        final_plan,
        EventId::generate(),
        fence,
        barrier.event().head(),
        barrier.direct_usage_after().unwrap(),
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap();
    assert!(matches!(
        returned,
        stateknot_store_postgres::GraphFrameReturnCommitOutcome::Committed(_)
    ));
    assert!(
        Box::pin(store.load_active_graph_frame(old.tenant_id(), old.run_id()))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .load_current_checkpoint(old.tenant_id(), old.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    store.close().await;
}

#[tokio::test]
async fn active_snapshot_rejects_a_root_alias_and_invented_lifetime_progress() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-active-alias", false)).await;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    let namespace = entry.entry().checkpoint().frame().namespace().as_str();
    let digest = entry.entry().checkpoint().frame().digest();
    query("ALTER TABLE stateknot.graph_frame_stacks DISABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    query("UPDATE stateknot.graph_frame_stacks SET active_namespace='',active_frame_identity_digest=NULL WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
    query("ALTER TABLE stateknot.graph_frame_stacks ENABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id())).await,
        Err(StoreError::CorruptData { .. })
    ));
    query("ALTER TABLE stateknot.graph_frame_stacks DISABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    query("UPDATE stateknot.graph_frame_stacks SET active_namespace=$3,active_frame_identity_digest=$4,lifetime_starts=2 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(namespace).bind(digest.as_bytes()).execute(&db).await.unwrap();
    query("ALTER TABLE stateknot.graph_frame_stacks ENABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id())).await,
        Err(StoreError::CorruptData { .. })
    ));
    query("ALTER TABLE stateknot.graph_frame_stacks DISABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    query("UPDATE stateknot.graph_frame_stacks SET lifetime_starts=1 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
    query("ALTER TABLE stateknot.graph_frame_stacks ENABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    assert_eq!(snapshot(&db, &fence).await, before);
    let actual = Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual.checkpoint(), entry.entry().checkpoint());
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_wait_cancellation_authenticates_abandonment_and_keeps_the_stack_for_close() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let waits = NodeWaits::try_new([NodeWait::timer(
        TimerId::generate(),
        RunTimerKind::Sleep,
        Timestamp::from_unix_micros(2_000_000_000_000_000).unwrap(),
    )])
    .unwrap();
    let (_, entry, graph, fence, plan, observed, revision) = Box::pin(
        super::frame_waits::prepared(&store, "frame-active-cancel", waits),
    )
    .await;
    let saved = Box::pin(super::frame_waits::suspend(
        &store,
        plan,
        &fence,
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &graph,
    ))
    .await;
    let active = Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.checkpoint(), saved.checkpoint());
    assert_eq!(active.run().lifecycle().status(), RunStatus::Waiting);
    let append = control_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        JournalExpectation::exact(saved.event().head()),
        25_050,
    );
    let cancellation = RunTransition::RequestCancellation {
        request: cancellation_request(saved.event().recorded_at()),
    };
    let cancelled = store
        .append_control_plane_abandon_waits(
            append.clone(),
            active.run().lifecycle().revision(),
            cancellation.clone(),
        )
        .await
        .unwrap();
    let exact = store
        .append_control_plane_abandon_waits(
            append,
            active.run().lifecycle().revision(),
            cancellation,
        )
        .await
        .unwrap();
    assert!(matches!(
        cancelled,
        WaitAbandonmentCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        exact,
        WaitAbandonmentCommitOutcome::Idempotent { .. }
    ));
    let cancelled = Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled.checkpoint(), saved.checkpoint());
    assert_eq!(
        cancelled.run().lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(cancelled.run().unresolved_wait_count(), 0);
    assert!(cancelled.run().lease().is_none());
    let db = pool().await;
    let original: String = query_scalar(
        "SELECT reason_kind FROM stateknot.wait_abandonments WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    query("ALTER TABLE stateknot.wait_abandonments DISABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    query("UPDATE stateknot.wait_abandonments SET reason_kind=$3 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .bind("run_failure")
        .execute(&db)
        .await
        .unwrap();
    query("ALTER TABLE stateknot.wait_abandonments ENABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    assert!(matches!(
        Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id())).await,
        Err(StoreError::CorruptData { .. })
    ));
    query("ALTER TABLE stateknot.wait_abandonments DISABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    query("UPDATE stateknot.wait_abandonments SET reason_kind=$3 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .bind(original)
        .execute(&db)
        .await
        .unwrap();
    query("ALTER TABLE stateknot.wait_abandonments ENABLE TRIGGER USER")
        .execute(&db)
        .await
        .unwrap();
    Box::pin(store.load_active_graph_frame(fence.tenant_id(), fence.run_id()))
        .await
        .unwrap();
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn claimed_frame_planning_recovers_completed_work_and_rejects_journal_progress() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, fence) = Box::pin(enter(&store, "frame-claimed-ready", false)).await;
    let base = entry.entry().checkpoint();
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(entry.event().head()),
        Digest::sha256(b"claimed frame ready recovery"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    let fresh = Box::pin(recovery.plan_graph_frame_ready_nodes(&base.head()))
        .await
        .unwrap();
    assert_eq!(fresh.frame(), Some(base.frame()));
    assert_eq!(fresh.checkpoint(), base.checkpoint());
    assert_eq!(fresh.fence(), &fence);
    assert_eq!(fresh.nodes().len(), 1);
    assert_eq!(fresh.nodes()[0].kind(), RecoveryNodeKind::Dispatchable);
    assert_eq!(
        fresh.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::FirstAttempt)
    );
    assert_eq!(
        fresh.nodes()[0].activation().graph_namespace(),
        base.frame().namespace()
    );
    let empty = Box::pin(store.load_graph_frame_pending_node_result_page(
        &base.head(),
        None,
        PendingNodeResultPageSize::new(1).unwrap(),
    ))
    .await
    .unwrap();
    assert!(empty.records().is_empty());
    assert!(!empty.has_more());
    assert!(matches!(
        store
            .load_unconsumed_pending_node_result_page(
                &base.checkpoint().head(),
                None,
                PendingNodeResultPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::StaleCheckpointHead)
    ));
    let node = fresh.nodes()[0].activation().node_id();
    let result = Box::pin(succeed_claimed(&store, &fresh, node, terminal(&graph))).await;
    assert!(matches!(
        Box::pin(recovery.plan_graph_frame_ready_nodes(&base.head())).await,
        Err(StoreError::StaleClaimedRunRecoveryObservation)
    ));
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(result.journal_head().clone()),
        Digest::sha256(b"claimed frame completed recovery"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    let completed = Box::pin(recovery.plan_graph_frame_ready_nodes(&base.head()))
        .await
        .unwrap();
    assert_eq!(completed.frame(), Some(base.frame()));
    assert_eq!(completed.nodes().len(), 1);
    assert_eq!(completed.nodes()[0].kind(), RecoveryNodeKind::Completed);
    assert!(completed.nodes()[0].dispatch_reason().is_none());
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            claimed_append(&completed),
            &completed,
            node,
            AttemptId::generate(),
        ))
        .await,
        Err(StoreError::ReadyNodeNotDispatchable)
    ));
    assert_eq!(
        store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .journal_head(),
        Some(result.journal_head())
    );
    let page = Box::pin(store.load_graph_frame_pending_node_result_page(
        &base.head(),
        None,
        PendingNodeResultPageSize::new(1).unwrap(),
    ))
    .await
    .unwrap();
    assert_eq!(page.records(), std::slice::from_ref(&result));
    assert!(!page.has_more());
    assert_eq!(page.snapshot_journal_head(), result.journal_head());
    let prepared = plan(&graph, base, &result);
    let barrier = Box::pin(commit(
        &store,
        prepared,
        &fence,
        entry.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    assert!(matches!(
        Box::pin(store.load_graph_frame_pending_node_result_page(
            &base.head(),
            None,
            PendingNodeResultPageSize::new(1).unwrap(),
        ))
        .await,
        Err(StoreError::StaleCheckpointHead)
    ));
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(barrier.event().head()),
        Digest::sha256(b"claimed frame terminal recovery"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    let terminal = Box::pin(recovery.plan_graph_frame_ready_nodes(&barrier.checkpoint().head()))
        .await
        .unwrap();
    assert_eq!(terminal.frame(), Some(base.frame()));
    assert!(terminal.nodes().is_empty());
    assert_eq!(terminal.checkpoint(), barrier.checkpoint().checkpoint());
    assert!(
        !store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.close().await;
}

pub(super) async fn claimed_plan(
    store: &PostgresStore,
    base: &GraphFrameCheckpoint,
    fence: &RunFence,
) -> ReadyNodeRecoveryPlan {
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        Digest::sha256(b"claimed frame history observation"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    Box::pin(recovery.plan_graph_frame_ready_nodes(&base.head()))
        .await
        .unwrap()
}

fn claimed_append(plan: &ReadyNodeRecoveryPlan) -> JournalAppend {
    worker_append(
        plan.fence().tenant_id().clone(),
        plan.fence().run_id(),
        EventId::generate(),
        JournalExpectation::exact(plan.journal_head().clone()),
        plan.fence().clone(),
        23_500,
    )
}

pub(super) async fn succeed_claimed(
    store: &PostgresStore,
    plan: &ReadyNodeRecoveryPlan,
    node: &NodeId,
    control: NodeControl,
) -> stateknot_core::PendingNodeResult {
    let start = Box::pin(store.start_recovered_graph_frame_node_attempt(
        claimed_append(plan),
        plan,
        node,
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    assert!(matches!(start, NodeAttemptCommitOutcome::Committed { .. }));
    let intent = PendingNodeResultIntent::new(
        start.attempt().start().activation().clone(),
        NodeStateChange::Unchanged,
        control,
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    let append = worker_append(
        plan.fence().tenant_id().clone(),
        plan.fence().run_id(),
        EventId::generate(),
        JournalExpectation::exact(start.event().head()),
        plan.fence().clone(),
        23_501,
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

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn claimed_frame_pages_preserve_scope_order_and_exact_journal_snapshot() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, child, _) = super::frame_transactions::graphs();
    let nodes = ReadyNodes::try_new(["a", "b", "c"].map(|n| NodeId::new(n).unwrap())).unwrap();
    let graph = CompiledGraph::compile(
        child.identity().clone(),
        child.input_schema().clone(),
        child.state_schema().clone(),
        child.update_schema().clone(),
        child.output_schema().clone(),
        child.reducer().clone(),
        nodes.clone(),
        nodes.iter().map(|node| {
            GraphNode::new(node.clone(), None, GraphRoutes::default(), None, true).unwrap()
        }),
        GraphExecutionLimits::new(child.limits().maximum_supersteps(), 3).unwrap(),
    )
    .unwrap();
    let (_, entry, graph, fence) =
        Box::pin(enter_graph(&store, "frame-claimed-page", graph, &[])).await;
    let base = entry.entry().checkpoint();
    let mut results = Vec::new();
    for name in ["a", "b"] {
        let plan = Box::pin(claimed_plan(&store, base, &fence)).await;
        results.push(
            Box::pin(succeed_claimed(
                &store,
                &plan,
                &NodeId::new(name).unwrap(),
                terminal(&graph),
            ))
            .await,
        );
    }
    let size = PendingNodeResultPageSize::new(1).unwrap();
    let first = Box::pin(store.load_graph_frame_pending_node_result_page(&base.head(), None, size))
        .await
        .unwrap();
    assert_eq!(first.records(), &results[..1]);
    assert!(first.has_more());
    let cursor = first.next_cursor().unwrap();
    let second = Box::pin(store.load_graph_frame_pending_node_result_page(
        &base.head(),
        Some(&cursor),
        size,
    ))
    .await
    .unwrap();
    assert_eq!(second.records(), &results[1..]);
    assert!(!second.has_more());
    let exhausted = Box::pin(store.load_graph_frame_pending_node_result_page(
        &base.head(),
        second.next_cursor().as_ref(),
        size,
    ))
    .await
    .unwrap();
    assert!(exhausted.records().is_empty());
    assert!(!exhausted.has_more());
    assert!(exhausted.next_cursor().is_none());
    assert_eq!(
        first.snapshot_journal_head(),
        second.snapshot_journal_head()
    );
    let (_, other, _, _) = Box::pin(enter(&store, "frame-claimed-cross-page", false)).await;
    assert!(matches!(
        Box::pin(store.load_graph_frame_pending_node_result_page(
            &other.entry().checkpoint().head(),
            Some(&cursor),
            size
        ))
        .await,
        Err(StoreError::InvalidPendingNodeResultCursor)
    ));
    let plan = Box::pin(claimed_plan(&store, base, &fence)).await;
    assert_eq!(
        plan.nodes()
            .iter()
            .map(stateknot_core::RecoveryNode::kind)
            .collect::<Vec<_>>(),
        [
            RecoveryNodeKind::Completed,
            RecoveryNodeKind::Completed,
            RecoveryNodeKind::Dispatchable
        ]
    );
    results.push(
        Box::pin(succeed_claimed(
            &store,
            &plan,
            &NodeId::new("c").unwrap(),
            terminal(&graph),
        ))
        .await,
    );
    assert!(matches!(
        Box::pin(store.load_graph_frame_pending_node_result_page(
            &base.head(),
            Some(&cursor),
            size
        ))
        .await,
        Err(StoreError::StalePendingNodeResultSnapshot)
    ));
    let recovered = Box::pin(claimed_plan(&store, base, &fence)).await;
    assert!(recovered.is_barrier_ready());
    assert_eq!(
        recovered.completed_result_heads().unwrap().as_ref(),
        results
            .iter()
            .map(stateknot_core::PendingNodeResult::head)
            .collect::<Vec<_>>()
    );
    store.close().await;
}

#[tokio::test]
async fn claimed_frame_history_distinguishes_in_flight_and_successor_takeover() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-claimed-takeover", false)).await;
    let base = entry.entry().checkpoint();
    let initial = Box::pin(claimed_plan(&store, base, &fence)).await;
    let node = initial.nodes()[0].activation().node_id();
    let first = Box::pin(store.start_recovered_graph_frame_node_attempt(
        claimed_append(&initial),
        &initial,
        node,
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let running = Box::pin(claimed_plan(&store, base, &fence)).await;
    assert_eq!(running.nodes()[0].kind(), RecoveryNodeKind::InFlight);
    assert_eq!(
        running.nodes()[0].in_flight_attempt(),
        Some(&first.attempt().start().head())
    );
    assert!(matches!(
        Box::pin(store.start_recovered_graph_frame_node_attempt(
            claimed_append(&running),
            &running,
            node,
            AttemptId::generate()
        ))
        .await,
        Err(StoreError::ReadyNodeNotDispatchable)
    ));
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(first.event().head()),
        Digest::sha256(b"claimed old frame owner"),
    )
    .unwrap();
    let old = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    let successor = store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    assert!(matches!(
        Box::pin(old.plan_graph_frame_ready_nodes(&base.head())).await,
        Err(StoreError::StaleFence)
    ));
    let takeover = Box::pin(claimed_plan(&store, base, &successor)).await;
    assert_eq!(
        takeover.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::SupersededAttempt)
    );
    let second = Box::pin(store.start_recovered_graph_frame_node_attempt(
        claimed_append(&takeover),
        &takeover,
        node,
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    assert!(matches!(second, NodeAttemptCommitOutcome::Committed { .. }));
    assert_eq!(second.attempt().start().fence(), &successor);
    assert_eq!(
        store
            .load_node_attempt(
                fence.tenant_id(),
                &fence.run_id(),
                first.attempt().start().attempt_id()
            )
            .await
            .unwrap()
            .start()
            .head(),
        first.attempt().start().head()
    );
    assert!(
        !store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn claimed_frame_corruption_quarantine_requires_the_exact_live_owner() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    for (name, disposition) in [
        ("frame-claimed-corrupt-current", 0),
        ("frame-claimed-corrupt-stale", 1),
        ("frame-claimed-corrupt-expired", 2),
    ] {
        let (_, entry, _, fence) = Box::pin(enter(&store, name, false)).await;
        let context = CorruptionQuarantineContext::new(
            fence.tenant_id().clone(),
            fence.run_id(),
            QuarantineId::generate(),
            JournalExpectation::exact(entry.event().head()),
            Digest::sha256(b"claimed frame corrupt lifetime"),
        )
        .unwrap();
        let recovery = store
            .begin_claimed_run_recovery(fence.clone(), context)
            .await
            .unwrap();
        let successor = if disposition == 1 {
            Some(
                store
                    .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
                    .await
                    .unwrap()
                    .lease()
                    .fence()
                    .clone(),
            )
        } else {
            None
        };
        if disposition == 2 {
            query("UPDATE stateknot.runs SET lease_acquired_at=clock_timestamp()-interval '2 seconds', \
                lease_renewed_at=clock_timestamp()-interval '1 second', \
                lease_expires_at=clock_timestamp()-interval '1 microsecond' WHERE tenant_id=$1 AND run_id=$2")
                .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
        }
        query("ALTER TABLE stateknot.graph_frame_stacks DISABLE TRIGGER USER")
            .execute(&db)
            .await
            .unwrap();
        query("UPDATE stateknot.graph_frame_stacks SET lifetime_starts=2 WHERE tenant_id=$1 AND run_id=$2")
            .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
        query("ALTER TABLE stateknot.graph_frame_stacks ENABLE TRIGGER USER")
            .execute(&db)
            .await
            .unwrap();
        let rejected =
            Box::pin(recovery.plan_graph_frame_ready_nodes(&entry.entry().checkpoint().head()))
                .await;
        match disposition {
            0 => assert!(matches!(rejected, Err(StoreError::RunQuarantined))),
            1 => assert!(matches!(rejected, Err(StoreError::StaleFence))),
            2 => assert!(matches!(rejected, Err(StoreError::LeaseExpired))),
            _ => unreachable!(),
        }
        let run = store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap();
        assert_eq!(run.is_quarantined(), disposition == 0);
        if disposition == 0 {
            assert!(run.lease().is_none());
            assert_eq!(
                store
                    .load_run_quarantine(fence.tenant_id(), fence.run_id())
                    .await
                    .unwrap()
                    .request()
                    .expected_fence(),
                Some(&fence)
            );
        } else {
            assert!(matches!(
                store
                    .load_run_quarantine(fence.tenant_id(), fence.run_id())
                    .await,
                Err(StoreError::RunQuarantineNotFound)
            ));
            if let Some(successor) = successor {
                assert_eq!(run.lease().unwrap().fence(), &successor);
            }
        }
        query("ALTER TABLE stateknot.graph_frame_stacks DISABLE TRIGGER USER")
            .execute(&db)
            .await
            .unwrap();
        query("UPDATE stateknot.graph_frame_stacks SET lifetime_starts=1 WHERE tenant_id=$1 AND run_id=$2")
            .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
        query("ALTER TABLE stateknot.graph_frame_stacks ENABLE TRIGGER USER")
            .execute(&db)
            .await
            .unwrap();
    }
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn claimed_frame_result_namespace_corruption_is_never_an_empty_page() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, fence) =
        Box::pin(enter(&store, "frame-claimed-corrupt-namespace", false)).await;
    let base = entry.entry().checkpoint();
    let plan = Box::pin(claimed_plan(&store, base, &fence)).await;
    let result = Box::pin(succeed_claimed(
        &store,
        &plan,
        plan.nodes()[0].activation().node_id(),
        terminal(&graph),
    ))
    .await;
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(result.journal_head().clone()),
        Digest::sha256(b"claimed frame result namespace contradiction"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    let db = pool().await;
    relabel_result_namespace(&db, result.intent().activation(), "").await;
    let page = Box::pin(store.load_graph_frame_pending_node_result_page(
        &base.head(),
        None,
        PendingNodeResultPageSize::new(1).unwrap(),
    ))
    .await;
    let recovered = Box::pin(recovery.plan_graph_frame_ready_nodes(&base.head())).await;
    relabel_result_namespace(
        &db,
        result.intent().activation(),
        base.frame().namespace().as_str(),
    )
    .await;
    assert!(matches!(page, Err(StoreError::CorruptData { .. })));
    assert!(matches!(recovered, Err(StoreError::RunQuarantined)));
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(run.is_quarantined());
    assert!(run.lease().is_none());
    assert_eq!(
        store
            .load_run_quarantine(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .request()
            .expected_fence(),
        Some(&fence)
    );
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn claimed_frame_result_namespace_corruption_cannot_hide_a_cursor_row() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, fence) = Box::pin(enter(
        &store,
        "frame-claimed-corrupt-cursor-namespace",
        false,
    ))
    .await;
    let base = entry.entry().checkpoint();
    let plan = Box::pin(claimed_plan(&store, base, &fence)).await;
    let result = Box::pin(succeed_claimed(
        &store,
        &plan,
        plan.nodes()[0].activation().node_id(),
        terminal(&graph),
    ))
    .await;
    let size = PendingNodeResultPageSize::new(1).unwrap();
    let initial =
        Box::pin(store.load_graph_frame_pending_node_result_page(&base.head(), None, size))
            .await
            .unwrap();
    assert_eq!(initial.records(), std::slice::from_ref(&result));
    let cursor = initial.next_cursor().unwrap();
    let db = pool().await;
    relabel_result_namespace(&db, result.intent().activation(), "").await;
    let rejected = Box::pin(store.load_graph_frame_pending_node_result_page(
        &base.head(),
        Some(&cursor),
        size,
    ))
    .await;
    relabel_result_namespace(
        &db,
        result.intent().activation(),
        base.frame().namespace().as_str(),
    )
    .await;
    let restored = Box::pin(store.load_graph_frame_pending_node_result_page(
        &base.head(),
        Some(&cursor),
        size,
    ))
    .await
    .unwrap();
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
    assert!(matches!(rejected, Err(StoreError::CorruptData { .. })));
    assert!(restored.records().is_empty());
    assert_eq!(
        restored.snapshot_journal_head(),
        initial.snapshot_journal_head()
    );
}

async fn relabel_result_namespace(db: &PgPool, activation: &NodeActivation, namespace: &str) {
    // Administrative fault injection changes only the SQL projection. LOCAL
    // restores enforcement on commit and leaves the trigger catalog intact.
    let mut transaction = db.begin().await.unwrap();
    query("SET LOCAL session_replication_role = replica")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let updated = query(
        "UPDATE stateknot.pending_node_results SET graph_namespace=$1 \
         WHERE tenant_id=$2 AND run_id=$3 AND base_checkpoint_id=$4 AND node_id=$5",
    )
    .bind(namespace)
    .bind(activation.base_checkpoint().tenant_id().as_str())
    .bind(*activation.base_checkpoint().run_id().as_uuid())
    .bind(*activation.base_checkpoint().checkpoint_id().as_uuid())
    .bind(activation.node_id().as_str())
    .execute(&mut *transaction)
    .await
    .unwrap();
    assert_eq!(updated.rows_affected(), 1);
    transaction.commit().await.unwrap();
}
