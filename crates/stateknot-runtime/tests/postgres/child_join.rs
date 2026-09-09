// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_core::{ChildRunJoinRequest, NodeInvocationBindings, PendingNodeResultIntent};
use stateknot_store_postgres::ChildJoinCommitOutcome;

#[path = "child_join_qualification.rs"]
mod qualification;

#[path = "child_join_driver.rs"]
mod driver;

async fn setup_join(store: &PostgresStore, name: &str) -> (Started, ChildRunJoinRequest, RunId) {
    let mut value = started(store, name).await;
    let spawned = spawn(store, &value).await.unwrap();
    let child = spawned
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    value.head = spawned.record().spawn().head();
    let request = ChildRunJoinRequest::new([value.intent.key().clone()]).unwrap();
    (value, request, child)
}
async fn publish_append(store: &PostgresStore, request: &ChildRunJoinRequest) -> JournalAppend {
    let a = request.activation();
    let run = store.load_run(a.tenant_id(), a.run_id()).await.unwrap();
    JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            a.tenant_id().clone(),
            a.run_id(),
            EventId::generate(),
            payload("child-join-published"),
        )
        .unwrap(),
    )
    .unwrap()
}
async fn settle(store: &PostgresStore, value: &Started, child: RunId) {
    fail(
        store,
        value.intent.key().tenant_id(),
        child,
        BudgetUsage::zero(),
    )
    .await
    .unwrap();
    Box::pin(store.settle_child_run(
        value.intent.key(),
        settlement_append(store, value.intent.key()).await,
    ))
    .await
    .unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_join_register_publish_recover_and_consume_exact_result() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (value, request, child) = setup_join(&store, "join-complete").await;
    let a = request.activation();
    let append = parent_append(&value, "child-join-registered");
    let registered = store
        .register_child_join(request.clone(), &value.node, append.clone())
        .await
        .unwrap();
    assert!(registered.record().head().is_none());
    assert!(
        store
            .load_run(a.tenant_id(), a.run_id())
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    assert!(matches!(
        store
            .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    assert!(
        store
            .pending_child_joins_after(a.tenant_id(), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store
            .publish_child_join(&request, publish_append(&store, &request).await)
            .await,
        Err(StoreError::ChildRunSettlementUnavailable)
    ));
    assert!(matches!(
        store
            .register_child_join(request.clone(), &value.node, append)
            .await
            .unwrap(),
        ChildJoinCommitOutcome::Idempotent(_)
    ));
    settle(&store, &value, child).await;
    assert_eq!(
        store
            .pending_child_joins_after(a.tenant_id(), None)
            .await
            .unwrap(),
        vec![request.clone()]
    );
    let append = publish_append(&store, &request).await;
    let published = store
        .publish_child_join(&request, append.clone())
        .await
        .unwrap();
    let head = published.record().head().unwrap().clone();
    assert!(matches!(
        store.publish_child_join(&request, append).await.unwrap(),
        ChildJoinCommitOutcome::Idempotent(_)
    ));
    assert!(
        store
            .pending_child_joins_after(a.tenant_id(), Some(&request))
            .await
            .unwrap()
            .is_empty()
    );
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let run = store.load_run(a.tenant_id(), a.run_id()).await.unwrap();
    let started = store
        .start_node_attempt(
            worker_append(
                a.tenant_id().clone(),
                a.run_id(),
                EventId::generate(),
                run.journal_head().unwrap().clone(),
                fence.clone(),
            ),
            a.clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let NodeAttemptCommitOutcome::Committed { attempt, .. } = started else {
        panic!("fresh recovery");
    };
    let plain = PendingNodeResultIntent::new(
        a.clone(),
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    let append = worker_append(
        a.tenant_id().clone(),
        a.run_id(),
        EventId::generate(),
        attempt.start().journal_head().clone(),
        fence,
    );
    assert!(matches!(
        store
            .succeed_node_attempt(
                append.clone(),
                &attempt.start().head(),
                plain.clone(),
                BudgetUsage::zero()
            )
            .await,
        Err(StoreError::ChildJoinRejected)
    ));
    let result = plain.with_child_join(head.clone()).unwrap();
    let completed = store
        .succeed_node_attempt(
            append.clone(),
            &attempt.start().head(),
            result.clone(),
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    assert!(matches!(
        completed,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    store
        .succeed_node_attempt(append, &attempt.start().head(), result, BudgetUsage::zero())
        .await
        .unwrap();
    let read = store.load_child_join(a).await.unwrap().unwrap();
    assert_eq!(read.head(), Some(&head));
    assert!(read.consumed().is_some());
    store.load_child_run(value.intent.key()).await.unwrap();
    assert_eq!(
        store
            .load_child_budget_account(a.tenant_id(), a.run_id())
            .await
            .unwrap()
            .unwrap()
            .children()
            .len(),
        1
    );
    // A recreated registry/driver must reuse the committed joined result,
    // advance the real non-initial checkpoint, and never execute Step_A again.
    let rebuilt = declared_parent(&driver_fixture(), value.intent.child().descriptor());
    let run = store.load_run(a.tenant_id(), a.run_id()).await.unwrap();
    let resumed = DurableGraphDriver::new(
        store.clone(),
        rebuilt.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .drive(
        run.lease().unwrap().fence().clone(),
        CancellationSignal::never(),
    )
    .await
    .unwrap();
    assert!(matches!(
        resumed.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    assert_eq!(rebuilt.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(rebuilt.second_calls.load(Ordering::SeqCst), 1);
    assert!(
        store
            .load_current_checkpoint(a.tenant_id(), a.run_id())
            .await
            .unwrap()
            .unwrap()
            .superstep()
            .get()
            > 0
    );
    store.close().await;
}

#[tokio::test]
async fn child_join_terminal_before_registration_and_duplicate_publication() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (mut value, request, child) = setup_join(&store, "join-already-terminal").await;
    settle(&store, &value, child).await;
    value.head = store
        .load_run(
            request.activation().tenant_id(),
            request.activation().run_id(),
        )
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    store
        .register_child_join(
            request.clone(),
            &value.node,
            parent_append(&value, "child-join-registered"),
        )
        .await
        .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    let append = publish_append(&store, &request).await;
    for _ in 0..12 {
        let store = store.clone();
        let request = request.clone();
        let append = append.clone();
        tasks.spawn(async move { store.publish_child_join(&request, append).await });
    }
    let mut fresh = 0;
    while let Some(result) = tasks.join_next().await {
        if matches!(
            result.unwrap().unwrap(),
            ChildJoinCommitOutcome::Committed(_)
        ) {
            fresh += 1;
        }
    }
    assert_eq!(fresh, 1);
    store.close().await;
}

async fn inject_failure(
    pool: &sqlx_postgres::PgPool,
    table: &str,
    operation: &str,
    condition: &str,
) {
    query("CREATE FUNCTION stateknot.test_join_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected Join transaction failure'; END $$").execute(pool).await.unwrap();
    query(&format!("CREATE TRIGGER test_join_fault AFTER {operation} ON stateknot.{table} FOR EACH ROW WHEN ({condition}) EXECUTE FUNCTION stateknot.test_join_fault()")).execute(pool).await.unwrap();
}
async fn clear_failure(pool: &sqlx_postgres::PgPool, table: &str) {
    query(&format!(
        "DROP TRIGGER test_join_fault ON stateknot.{table}"
    ))
    .execute(pool)
    .await
    .unwrap();
    query("DROP FUNCTION stateknot.test_join_fault()")
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_join_final_writes_roll_back_registration_wakeup_and_consumption() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (value, request, child) = setup_join(&store, "join-faults").await;
    let a = request.activation();
    let pool = sql_pool().await;
    inject_failure(&pool,"runs","UPDATE",&format!("NEW.run_id='{}'::uuid AND OLD.lease_attempt_id IS NOT NULL AND NEW.lease_attempt_id IS NULL",a.run_id())).await;
    let append = parent_append(&value, "child-join-registered");
    let failed = store
        .register_child_join(request.clone(), &value.node, append.clone())
        .await;
    clear_failure(&pool, "runs").await;
    assert!(
        matches!(failed, Err(StoreError::Database { .. })),
        "{failed:?}"
    );
    assert!(store.load_child_join(a).await.unwrap().is_none());
    let run = store.load_run(a.tenant_id(), a.run_id()).await.unwrap();
    assert_eq!(run.journal_head(), Some(&value.head));
    assert_eq!(run.lease().unwrap().fence(), &value.fence);
    store
        .register_child_join(request.clone(), &value.node, append)
        .await
        .unwrap();
    settle(&store, &value, child).await;
    let before = store.load_run(a.tenant_id(), a.run_id()).await.unwrap();
    inject_failure(&pool,"runs","UPDATE",&format!("NEW.run_id='{}'::uuid AND NEW.scheduler_ready_at IS DISTINCT FROM OLD.scheduler_ready_at",a.run_id())).await;
    let append = publish_append(&store, &request).await;
    let failed = store.publish_child_join(&request, append.clone()).await;
    clear_failure(&pool, "runs").await;
    assert!(
        matches!(failed, Err(StoreError::Database { .. })),
        "{failed:?}"
    );
    assert!(
        store
            .load_child_join(a)
            .await
            .unwrap()
            .unwrap()
            .head()
            .is_none()
    );
    assert_eq!(
        store
            .load_run(a.tenant_id(), a.run_id())
            .await
            .unwrap()
            .journal_head(),
        before.journal_head()
    );
    assert_eq!(
        store
            .pending_child_joins_after(a.tenant_id(), None)
            .await
            .unwrap(),
        vec![request.clone()]
    );
    let published = store.publish_child_join(&request, append).await.unwrap();
    let fence = store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let run = store.load_run(a.tenant_id(), a.run_id()).await.unwrap();
    let start = store
        .start_node_attempt(
            worker_append(
                a.tenant_id().clone(),
                a.run_id(),
                EventId::generate(),
                run.journal_head().unwrap().clone(),
                fence.clone(),
            ),
            a.clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let NodeAttemptCommitOutcome::Committed { attempt, .. } = start else {
        panic!("new physical start");
    };
    let intent = PendingNodeResultIntent::new(
        a.clone(),
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap()
    .with_child_join(published.record().head().unwrap().clone())
    .unwrap();
    let append = worker_append(
        a.tenant_id().clone(),
        a.run_id(),
        EventId::generate(),
        attempt.start().journal_head().clone(),
        fence,
    );
    inject_failure(
        &pool,
        "node_attempt_completions",
        "INSERT",
        &format!("NEW.run_id='{}'::uuid", a.run_id()),
    )
    .await;
    let failed = store
        .succeed_node_attempt(
            append.clone(),
            &attempt.start().head(),
            intent.clone(),
            BudgetUsage::zero(),
        )
        .await;
    clear_failure(&pool, "node_attempt_completions").await;
    assert!(
        matches!(failed, Err(StoreError::Database { .. })),
        "{failed:?}"
    );
    assert!(
        store
            .load_child_join(a)
            .await
            .unwrap()
            .unwrap()
            .consumed()
            .is_none()
    );
    assert_eq!(
        store
            .load_run(a.tenant_id(), a.run_id())
            .await
            .unwrap()
            .journal_head(),
        Some(attempt.start().journal_head())
    );
    store
        .succeed_node_attempt(append, &attempt.start().head(), intent, BudgetUsage::zero())
        .await
        .unwrap();
    assert!(
        store
            .load_child_join(a)
            .await
            .unwrap()
            .unwrap()
            .consumed()
            .is_some()
    );
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn child_join_cancel_during_wait_drains_without_fake_consumption() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (value, request, child) = setup_join(&store, "join-cancel").await;
    let a = request.activation();
    store
        .register_child_join(
            request.clone(),
            &value.node,
            parent_append(&value, "child-join-registered"),
        )
        .await
        .unwrap();
    cancellation::cancel_run(&store, a.tenant_id(), a.run_id()).await;
    assert!(
        store
            .pending_child_joins_after(a.tenant_id(), None)
            .await
            .unwrap()
            .is_empty()
    );
    settle(&store, &value, child).await;
    assert!(matches!(
        store
            .publish_child_join(&request, publish_append(&store, &request).await)
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    store
        .claim_lease(a.tenant_id(), a.run_id(), AttemptId::generate())
        .await
        .unwrap();
    Box::pin(cancellation::confirm(
        &store,
        a.tenant_id(),
        a.run_id(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    let joined = store.load_child_join(a).await.unwrap().unwrap();
    assert!(joined.head().is_none());
    assert!(joined.consumed().is_none());
    assert_eq!(
        store
            .load_run(a.tenant_id(), a.run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Cancelled
    );
    store.close().await;
}

#[tokio::test]
async fn child_join_legacy_writer_and_sealed_spawn_cannot_bypass_guards() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (value, request, _) = setup_join(&store, "join-guards").await;
    let a = request.activation();
    let pool = sql_pool().await;
    store
        .register_child_join(
            request.clone(),
            &value.node,
            parent_append(&value, "child-join-registered"),
        )
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    query("SET LOCAL stateknot.child_runtime_version='1'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let old =
        query("UPDATE stateknot.runs SET updated_at=updated_at WHERE tenant_id=$1 AND run_id=$2")
            .bind(a.tenant_id().as_str())
            .bind(*a.run_id().as_uuid())
            .execute(&mut *tx)
            .await
            .unwrap_err();
    assert_eq!(
        old.as_database_error().unwrap().code().as_deref(),
        Some("SKC01")
    );
    tx.rollback().await.unwrap();
    let sealed=query("INSERT INTO stateknot.child_run_ownership SELECT * FROM stateknot.child_run_ownership WHERE tenant_id=$1 AND parent_run_id=$2")
        .bind(a.tenant_id().as_str()).bind(*a.run_id().as_uuid()).execute(&pool).await.unwrap_err();
    assert_eq!(
        sealed.as_database_error().unwrap().code().as_deref(),
        Some("SKC07")
    );
    let altered=query("UPDATE stateknot.child_run_joins SET ready_at=clock_timestamp() WHERE tenant_id=$1 AND parent_run_id=$2")
        .bind(a.tenant_id().as_str()).bind(*a.run_id().as_uuid()).execute(&pool).await.unwrap_err();
    assert_eq!(
        altered.as_database_error().unwrap().code().as_deref(),
        Some("SKC05")
    );
    let other = ChildRunJoinRequest::new([ChildRunKey::new(
        a.clone(),
        ChildRunSlot::new("secondary").unwrap(),
    )
    .unwrap()])
    .unwrap();
    assert!(matches!(
        store
            .register_child_join(
                other,
                &value.node,
                parent_append(&value, "child-join-registered")
            )
            .await,
        Err(StoreError::ChildJoinRejected)
    ));
    pool.close().await;
    store.close().await;
}
