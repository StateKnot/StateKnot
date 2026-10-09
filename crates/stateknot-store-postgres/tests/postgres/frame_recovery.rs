// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Authenticated active stacks for real framework recovery and closure.
use super::frame_barriers::{commit, enter, plan, pool, snapshot, succeed, terminal};
use super::*;
use stateknot_core::{NodeWait, NodeWaits};

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
