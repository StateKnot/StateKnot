// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Public recovery-session boundaries for the registered scoped Join preflight.
use super::*;
use stateknot_core::GraphFrameCheckpointHead;
use stateknot_store_postgres::{
    ClaimedRunRecovery, LeaseClaimOutcome, RunQuarantineCause, RunQuarantineCommitOutcome,
    RunQuarantineComponent, RunQuarantineRequest,
};

async fn bounds_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap()
}

async fn actual_leaf(store: &PostgresStore, scenario: &Scenario) -> GraphFrameCheckpointHead {
    let key = &scenario.key;
    let active = Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        active.checkpoint().frame().namespace(),
        key.parent().graph_namespace()
    );
    assert_eq!(
        &active.checkpoint().checkpoint().head(),
        key.parent().base_checkpoint()
    );
    active.checkpoint().head()
}

async fn assert_live_fence(store: &PostgresStore, fence: &RunFence, journal: &JournalHead) {
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(!current.is_quarantined());
    assert_eq!(
        current.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(current.journal_head(), Some(journal));
    assert_eq!(current.last_fencing_epoch(), Some(fence.epoch()));
    assert_eq!(current.lease().unwrap().fence(), fence);
    store.observe_live_lease(fence).await.unwrap();
}

async fn claim_successor(
    store: &PostgresStore,
    previous: &RunFence,
    journal: &JournalHead,
) -> RunFence {
    let before = store
        .load_run(previous.tenant_id(), previous.run_id())
        .await
        .unwrap();
    assert!(before.lease().is_none());
    assert_eq!(before.last_fencing_epoch(), Some(previous.epoch()));
    assert_eq!(before.journal_head(), Some(journal));
    let claimed = store
        .claim_lease(
            previous.tenant_id(),
            previous.run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    assert!(matches!(claimed, LeaseClaimOutcome::Claimed(_)));
    let next = claimed.lease().fence().clone();
    assert!(next.epoch() > previous.epoch());
    assert_ne!(next.attempt_id(), previous.attempt_id());
    assert_live_fence(store, &next, journal).await;
    next
}

async fn session<'a>(
    store: &'a PostgresStore,
    fence: &RunFence,
    journal: &JournalHead,
) -> ClaimedRunRecovery<'a> {
    assert_live_fence(store, fence, journal).await;
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(journal.clone()),
        Digest::sha256(b"registered scoped Join public preflight bounds"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    assert_eq!(recovery.fence(), fence);
    assert_eq!(recovery.initial_run().journal_head(), Some(journal));
    assert_eq!(recovery.initial_run().lease().unwrap().fence(), fence);
    assert_eq!(
        recovery.initial_run().lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(
        recovery.quarantine_context().expectation(),
        &JournalExpectation::exact(journal.clone())
    );
    recovery
}

#[tokio::test]
async fn registered_join_preflight_repeats_without_writes_and_rejects_another_real_leaf() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let pool = bounds_pool().await;
    // Both leaves come from real public F1 Start, spawn, unpublished Join,
    // cancellation, child terminal and settlement commits in separate scopes.
    let scenario = Box::pin(prepare(&store)).await;
    let other = Box::pin(prepare(&store)).await;
    assert_ne!(scenario.key.tenant_id(), other.key.tenant_id());
    assert_ne!(scenario.key.parent_run_id(), other.key.parent_run_id());
    let leaf = Box::pin(actual_leaf(&store, &scenario)).await;
    let other_leaf = Box::pin(actual_leaf(&store, &other)).await;
    let f2 = claim_successor(&store, &scenario.fence, &scenario.observed).await;
    let before = snapshot(&pool, scenario.key.tenant_id()).await;
    let other_before = snapshot(&pool, other.key.tenant_id()).await;
    let recovery = session(&store, &f2, &scenario.observed).await;
    assert_eq!(snapshot(&pool, scenario.key.tenant_id()).await, before);
    for _ in 0..2 {
        recovery
            .validate_registered_join_frame_cancellation(&leaf)
            .await
            .unwrap();
        assert_live_fence(&store, &f2, &scenario.observed).await;
        assert_eq!(snapshot(&pool, scenario.key.tenant_id()).await, before);
        assert_eq!(snapshot(&pool, other.key.tenant_id()).await, other_before);
    }
    let crossed = recovery
        .validate_registered_join_frame_cancellation(&other_leaf)
        .await;
    assert!(
        matches!(&crossed, Err(StoreError::InvalidClaimedRunRecoveryContext)),
        "another real leaf must fail the exact session scope: {crossed:?}"
    );
    assert_live_fence(&store, &f2, &scenario.observed).await;
    assert_eq!(snapshot(&pool, scenario.key.tenant_id()).await, before);
    assert_eq!(snapshot(&pool, other.key.tenant_id()).await, other_before);
    pool.close().await;
    store.close().await;
}

async fn advance_journal(store: &PostgresStore, scenario: &Scenario, f2: &RunFence) -> JournalHead {
    assert_live_fence(store, f2, &scenario.observed).await;
    store
        .append_control_plane(
            control(
                scenario.key.tenant_id(),
                scenario.key.parent_run_id(),
                scenario.observed.clone(),
                "test-registered-join-preflight-journal-advance",
            ),
            RunProjection::unchanged(),
        )
        .await
        .unwrap();
    let advanced = store
        .load_run(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(advanced.lifecycle()).unwrap(),
        scenario.lifecycle
    );
    let journal = advanced.journal_head().unwrap().clone();
    assert_ne!(journal, scenario.observed);
    assert_live_fence(store, f2, &journal).await;
    journal
}

async fn operator_quarantine(store: &PostgresStore, f3: &RunFence, journal: &JournalHead) {
    assert_live_fence(store, f3, journal).await;
    let request = RunQuarantineRequest::new(
        f3.tenant_id().clone(),
        f3.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(journal.clone()),
        RunQuarantineCause::OperatorPolicy,
        RunQuarantineComponent::new("registered-join-preflight-bounds").unwrap(),
        Digest::sha256(b"public operator quarantine after F3 session"),
    )
    .unwrap();
    assert!(matches!(
        store.quarantine_run(request.clone()).await.unwrap(),
        RunQuarantineCommitOutcome::Committed(_)
    ));
    let retained = store
        .load_run_quarantine(f3.tenant_id(), f3.run_id())
        .await
        .unwrap();
    assert_eq!(retained.request(), &request);
    let current = store.load_run(f3.tenant_id(), f3.run_id()).await.unwrap();
    assert!(current.is_quarantined());
    assert!(current.lease().is_none());
    assert_eq!(current.last_fencing_epoch(), Some(f3.epoch()));
    assert_eq!(current.journal_head(), Some(journal));
}

#[tokio::test]
async fn registered_join_preflight_rejects_real_journal_progress_successor_fence_and_quarantine() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let pool = bounds_pool().await;
    let scenario = Box::pin(prepare(&store)).await;
    let tenant = scenario.key.tenant_id();
    let leaf = Box::pin(actual_leaf(&store, &scenario)).await;
    let f2 = claim_successor(&store, &scenario.fence, &scenario.observed).await;
    let original = session(&store, &f2, &scenario.observed).await;
    let journal = advance_journal(&store, &scenario, &f2).await;
    let after_advance = snapshot(&pool, tenant).await;
    let stale_observation = original
        .validate_registered_join_frame_cancellation(&leaf)
        .await;
    assert!(
        matches!(
            &stale_observation,
            Err(StoreError::StaleClaimedRunRecoveryObservation)
        ),
        "real journal progress must invalidate the old session: {stale_observation:?}"
    );
    assert_live_fence(&store, &f2, &journal).await;
    assert_eq!(snapshot(&pool, tenant).await, after_advance);

    let refreshed = session(&store, &f2, &journal).await;
    assert_eq!(snapshot(&pool, tenant).await, after_advance);
    assert!(matches!(
        store.release_lease(&f2).await.unwrap(),
        LeaseReleaseOutcome::Released
    ));
    let f3 = claim_successor(&store, &f2, &journal).await;
    let after_takeover = snapshot(&pool, tenant).await;
    let stale_fence = refreshed
        .validate_registered_join_frame_cancellation(&leaf)
        .await;
    assert!(
        matches!(&stale_fence, Err(StoreError::StaleFence)),
        "real F3 ownership must reject the old F2 session: {stale_fence:?}"
    );
    assert_live_fence(&store, &f3, &journal).await;
    assert_eq!(snapshot(&pool, tenant).await, after_takeover);

    let successor = session(&store, &f3, &journal).await;
    successor
        .validate_registered_join_frame_cancellation(&leaf)
        .await
        .unwrap();
    assert_eq!(snapshot(&pool, tenant).await, after_takeover);
    operator_quarantine(&store, &f3, &journal).await;
    let after_quarantine = snapshot(&pool, tenant).await;
    let quarantined = successor
        .validate_registered_join_frame_cancellation(&leaf)
        .await;
    assert!(
        matches!(&quarantined, Err(StoreError::RunQuarantined)),
        "public operator quarantine must reject the F3 session: {quarantined:?}"
    );
    assert_eq!(snapshot(&pool, tenant).await, after_quarantine);
    pool.close().await;
    store.close().await;
}
