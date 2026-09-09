// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_runtime::{
    DurableRunFailureCloser, register_standard_run_failure_close_event_schema,
};
use stateknot_store_postgres::RunFailureCloseOutcome;

#[path = "failure_close_upgrade.rs"]
mod upgrade;

struct Shutdown;
impl stateknot_core::CancellationObserver for Shutdown {
    fn is_cancelled(&self) -> bool {
        true
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

fn closer(store: &PostgresStore) -> DurableRunFailureCloser {
    let mut schemas = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    register_standard_run_failure_close_event_schema(&mut schemas).unwrap();
    DurableRunFailureCloser::new(
        store.clone(),
        schemas.build().unwrap(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
}
fn direct_usage() -> BudgetUsage {
    BudgetUsage::builder()
        .input_tokens(TokenCount::new(7))
        .build()
        .unwrap()
}
async fn finish_node(store: &PostgresStore, value: &mut Started) {
    let key = value.intent.key();
    value.head = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let append = parent_append(value, "test-node-failed");
    let failure = test_failure("test.parent.failed", "Original private failure detail.")
        .with_caused_by_event(append.intent().event_id());
    store
        .fail_node_attempt(append, &value.node, failure, direct_usage())
        .await
        .unwrap();
    value.head = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
}
async fn request(
    store: &PostgresStore,
    value: &Started,
    usage: BudgetUsage,
) -> Result<RunFailureCloseOutcome, StoreError> {
    store
        .request_run_failure_close(
            value.intent.key().parent().base_checkpoint(),
            test_failure(
                "test.original.failed",
                "Original failure must survive restart.",
            ),
            usage,
            parent_append(value, "run-failure-close-requested"),
        )
        .await
}
async fn tick(store: &PostgresStore, tenant: &TenantId) -> stateknot_runtime::RunFailureCloseTick {
    closer(store)
        .tick(tenant.clone(), None, CancellationSignal::never())
        .await
        .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failure_close_final_write_rollback_guards_and_terminal_accounting_are_enforced() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "failure-close-write-guards").await;
    let key = value.intent.key().clone();
    spawn(&store, &value).await.unwrap();
    finish_node(&store, &mut value).await;
    let pool = sql_pool().await;
    query("CREATE FUNCTION stateknot.test_close_write_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected close lease release failure'; END $$").execute(&pool).await.unwrap();
    query(&format!("CREATE TRIGGER zz_test_close_write_failure BEFORE UPDATE ON stateknot.runs FOR EACH ROW WHEN (NEW.run_id='{}'::uuid AND OLD.lease_attempt_id IS NOT NULL AND NEW.lease_attempt_id IS NULL) EXECUTE FUNCTION stateknot.test_close_write_failure()",key.parent_run_id())).execute(&pool).await.unwrap();
    let rejected = request(&store, &value, direct_usage()).await;
    query("DROP TRIGGER zz_test_close_write_failure ON stateknot.runs")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_close_write_failure()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(rejected, Err(StoreError::Database { .. })));
    assert!(
        store
            .load_run_failure_close(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .pending_child_cancellations_after(key.tenant_id(), None)
            .await
            .unwrap()
            .is_empty()
    );
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(run.journal_head(), Some(&value.head));
    assert_eq!(run.lease().unwrap().fence(), &value.fence);
    let close = request(&store, &value, direct_usage()).await.unwrap();
    let original = close.record().failure().clone();
    let mut tx = pool.begin().await.unwrap();
    for sql in [
        "SET LOCAL stateknot.child_runtime_version='1'",
        "SET LOCAL stateknot.child_join_version='1'",
    ] {
        query(sql).execute(&mut *tx).await.unwrap();
    }
    let old =
        query("UPDATE stateknot.runs SET updated_at=updated_at WHERE tenant_id=$1 AND run_id=$2")
            .bind(key.tenant_id().as_str())
            .bind(*key.parent_run_id().as_uuid())
            .execute(&mut *tx)
            .await
            .unwrap_err();
    assert_eq!(
        old.as_database_error().unwrap().code().as_deref(),
        Some("SKC08")
    );
    tx.rollback().await.unwrap();
    let (schema, document) = stateknot_runtime::standard_agent_deadline_event_schema().unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    schemas.register(schema.clone(), document).unwrap();
    assert!(matches!(
        store
            .request_agent_deadline_cancellation(
                key.tenant_id(),
                key.parent_run_id(),
                EventId::generate(),
                FailureId::generate(),
                &schema,
                &schemas.build().unwrap()
            )
            .await
            .unwrap(),
        stateknot_store_postgres::AgentDeadlineCancellationOutcome::FailureClosing
    ));
    assert!(
        store
            .due_agent_deadlines_after(key.tenant_id(), None)
            .await
            .unwrap()
            .is_empty()
    );
    let current = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let event = EventId::generate();
    let cancelled = RunCancellationRequest::new(
        Failure::new(
            FailureId::generate(),
            FailureCategory::Cancelled,
            FailureCode::new("test.cancel").unwrap(),
            FailureOrigin::new("test").unwrap(),
            FailureMessage::new("Later cancellation").unwrap(),
            RetryAdvice::Never,
        )
        .unwrap()
        .with_caused_by_event(event),
        store.observe_database_clock().await.unwrap(),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(current.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            key.tenant_id().clone(),
            key.parent_run_id(),
            event,
            payload("test-later-cancel"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_control_plane(
                append,
                RunProjection::transition(
                    current.lifecycle().revision(),
                    RunTransition::RequestCancellation { request: cancelled }
                )
            )
            .await,
        Err(StoreError::RunFailureClosing)
    ));
    assert!(
        store
            .start_node_attempt(
                parent_append(&value, "test-new-node"),
                key.parent().clone(),
                AttemptId::generate()
            )
            .await
            .is_err()
    );
    let child = value.intent.child().provenance().run_id();
    let child_usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(11))
        .build()
        .unwrap();
    fail(&store, key.tenant_id(), child, child_usage.clone())
        .await
        .unwrap();
    cancellation::reconciler(&store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    // Every generic terminal writer must retain both the exact failure and frozen direct usage.
    let current = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    for (failure, usage, expected_accounting) in [
        (original.clone(), child_usage.clone(), true),
        (
            test_failure("wrong.failure", "Do not replace the original."),
            direct_usage().checked_accumulate(&child_usage).unwrap(),
            false,
        ),
    ] {
        let append = JournalAppend::new(
            JournalExpectation::exact(current.journal_head().unwrap().clone()),
            JournalEventIntent::control_plane(
                key.tenant_id().clone(),
                key.parent_run_id(),
                EventId::generate(),
                payload("test-wrong-final"),
            )
            .unwrap(),
        )
        .unwrap();
        let failure = RunFailure::new(
            failure,
            store.observe_database_clock().await.unwrap(),
            usage,
        )
        .unwrap();
        let result = store
            .append_control_plane(
                append,
                RunProjection::transition(
                    current.lifecycle().revision(),
                    RunTransition::Fail { failure },
                ),
            )
            .await;
        if expected_accounting {
            assert!(
                matches!(result, Err(StoreError::IncompleteChildAccounting)),
                "{result:?}"
            );
        } else {
            assert!(
                matches!(result, Err(StoreError::RunFailureClosing)),
                "{result:?}"
            );
        }
    }
    tick(&store, key.tenant_id()).await.items()[0]
        .result()
        .unwrap();
    assert_eq!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .lifecycle()
            .terminal_failure()
            .unwrap()
            .id(),
        original.id()
    );
    // Even a compatibility-enabled low-level writer cannot resurrect the saved Active snapshot.
    let mut tx = pool.begin().await.unwrap();
    for sql in [
        "SET LOCAL stateknot.child_runtime_version='1'",
        "SET LOCAL stateknot.child_join_version='1'",
        "SET LOCAL stateknot.failure_close_version='1'",
    ] {
        query(sql).execute(&mut *tx).await.unwrap();
    }
    let resurrection=query("UPDATE stateknot.runs SET lifecycle_status='active',lifecycle_bytes=$3,lifecycle_revision=$4::numeric,changed_at=admitted_at WHERE tenant_id=$1 AND run_id=$2")
        .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid())
        .bind(serde_json_canonicalizer::to_vec(close.record().lifecycle()).unwrap())
        .bind(close.record().lifecycle().revision().get().to_string()).execute(&mut *tx).await.unwrap_err();
    assert_eq!(
        resurrection.as_database_error().unwrap().code().as_deref(),
        Some("SKC09")
    );
    tx.rollback().await.unwrap();
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn failure_close_checks_fresh_lease_after_child_queue_capture() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let mut value = started(&store, "failure-close-final-expiry").await;
    let key = value.intent.key().clone();
    spawn(&store, &value).await.unwrap();
    finish_node(&store, &mut value).await;
    let pool = sql_pool().await;
    query("CREATE FUNCTION stateknot.test_close_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.1); RETURN NEW; END $$").execute(&pool).await.unwrap();
    query(&format!("CREATE TRIGGER zz_test_close_delay AFTER INSERT ON stateknot.run_failure_closes FOR EACH ROW WHEN (NEW.run_id='{}'::uuid) EXECUTE FUNCTION stateknot.test_close_delay()",key.parent_run_id())).execute(&pool).await.unwrap();
    let result = request(&store, &value, direct_usage()).await;
    query("DROP TRIGGER zz_test_close_delay ON stateknot.run_failure_closes")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_close_delay()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        matches!(result, Err(StoreError::LeaseExpired)),
        "{result:?}"
    );
    assert!(
        store
            .load_run_failure_close(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .pending_child_cancellations_after(key.tenant_id(), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .journal_head(),
        Some(&value.head)
    );
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failure_close_pages_continue_past_blocked_work_and_preserve_tenant_shutdown_and_quarantine()
 {
    use stateknot_store_postgres::{
        RunQuarantineCause, RunQuarantineComponent, RunQuarantineRequest,
    };
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("failure-close-pages");
    let mut blocked = None;
    for index in 0..17 {
        let fixture = Box::pin(setup_in_tenant(&store, tenant.clone())).await;
        let mut value = Box::pin(started_fixture(&store, fixture)).await;
        if index == 0 {
            spawn(&store, &value).await.unwrap();
            blocked = Some(value.intent.key().parent_run_id());
        }
        finish_node(&store, &mut value).await;
        let close = request(&store, &value, direct_usage()).await.unwrap();
        if index == 1 {
            store
                .quarantine_run(
                    RunQuarantineRequest::new(
                        tenant.clone(),
                        value.intent.key().parent_run_id(),
                        QuarantineId::generate(),
                        JournalExpectation::exact(close.record().registration().head()),
                        RunQuarantineCause::OperatorPolicy,
                        RunQuarantineComponent::new("failure-close-test").unwrap(),
                        Digest::sha256("operator quarantine evidence"),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
    }
    let worker = closer(&store);
    let stop = CancellationSignal::new(Shutdown);
    assert!(
        worker
            .tick(tenant.clone(), None, stop)
            .await
            .unwrap()
            .is_cancelled()
    );
    assert_eq!(
        store
            .pending_run_failure_closes_after(&tenant, None)
            .await
            .unwrap()
            .len(),
        16
    );
    let first = worker
        .tick(tenant.clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(first.items().len(), 16);
    assert!(matches!(
        first.items()[0].result(),
        Err(StoreError::UnsettledChildRuns)
    ));
    assert!(matches!(
        first.items()[1].result(),
        Err(StoreError::RunQuarantined)
    ));
    assert_eq!(
        first
            .items()
            .iter()
            .filter(|item| item.result().is_ok())
            .count(),
        14
    );
    assert!(
        worker
            .tick(
                TenantId::new("wrong-tenant").unwrap(),
                Some(first.cursor().clone()),
                CancellationSignal::never()
            )
            .await
            .is_err()
    );
    let second = worker
        .tick(
            tenant.clone(),
            Some(first.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(second.items().len(), 1);
    second.items()[0].result().unwrap();
    let third = worker
        .tick(
            tenant.clone(),
            Some(second.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(third.items().len(), 2);
    assert_eq!(third.items()[0].candidate().run_id(), blocked.unwrap());
    assert!(
        store
            .pending_run_failure_closes_after(
                &TenantId::new("another-tenant").unwrap(),
                Some(first.items()[0].candidate())
            )
            .await
            .is_err()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failure_close_drains_children_preserves_original_and_charges_once_after_reconstruction() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "failure-close-restart").await;
    let key = value.intent.key().clone();
    let child = spawn(&store, &value)
        .await
        .unwrap()
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    finish_node(&store, &mut value).await;
    let committed = request(&store, &value, direct_usage()).await.unwrap();
    let original = serde_json::to_value(committed.record().failure()).unwrap();
    let source = committed.record().registration().head();
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Active);
    assert!(run.lease().is_none());
    assert_eq!(
        run.checkpoint().unwrap().checkpoint_id(),
        value.fixture.parent.checkpoint().checkpoint_id()
    );
    assert!(matches!(
        store
            .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    let work = store.load_child_cancellation(&key).await.unwrap();
    assert!(work.is_parent_failure_close());
    assert_eq!(work.parent_head(), &source);
    assert!(matches!(
        tick(&store, key.tenant_id()).await.items()[0].result(),
        Err(StoreError::UnsettledChildRuns)
    ));
    let replay = request(&store, &value, BudgetUsage::zero()).await.unwrap();
    assert!(matches!(replay, RunFailureCloseOutcome::Existing(_)));
    assert_eq!(
        serde_json::to_value(replay.record().failure()).unwrap(),
        original
    );
    assert_eq!(replay.record().direct_usage(), &direct_usage());
    // Reconstruct coordinators and connections; no in-memory notification is required.
    store.close().await;
    let store = test_store().await.unwrap();
    let delivered = cancellation::reconciler(&store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert!(delivered.items().iter().any(|item| item.result().is_ok()));
    let child_run = store.load_run(key.tenant_id(), child).await.unwrap();
    let reason = child_run
        .lifecycle()
        .cancellation_request()
        .unwrap()
        .failure();
    assert_eq!(reason.code().as_str(), "child.parent_failed");
    assert!(
        !serde_json::to_string(reason)
            .unwrap()
            .contains("Original failure")
    );
    let usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(11))
        .build()
        .unwrap();
    cancellation::confirm(&store, key.tenant_id(), child, usage.clone())
        .await
        .unwrap();
    cancellation::reconciler(&store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    let completed = tick(&store, key.tenant_id()).await;
    let outcome = completed.items()[0].result().unwrap();
    assert!(outcome.record().completed_at().is_some());
    let final_run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(final_run.lifecycle().status(), RunStatus::Failed);
    assert_eq!(
        serde_json::to_value(final_run.lifecycle().terminal_failure().unwrap()).unwrap(),
        original
    );
    assert_eq!(
        final_run.lifecycle().terminal_usage().unwrap(),
        &direct_usage().checked_accumulate(&usage).unwrap()
    );
    assert!(tick(&store, key.tenant_id()).await.items().is_empty());
    assert_eq!(
        store
            .load_child_cancellation(&key)
            .await
            .unwrap()
            .parent_head(),
        &source
    );
    let replay = request(&store, &value, direct_usage()).await.unwrap();
    assert!(replay.record().completed_at().is_some());
    assert_eq!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .journal_head(),
        final_run.journal_head()
    );
    store.close().await;
}

#[tokio::test]
async fn failure_close_rejects_unfinished_or_unpriced_direct_evidence_without_stranding_lease() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "failure-close-direct-gate").await;
    let key = value.intent.key().clone();
    assert!(matches!(
        request(&store, &value, direct_usage()).await,
        Err(StoreError::InvalidRunFailureClose)
    ));
    finish_node(&store, &mut value).await;
    let unknown = BudgetUsage::builder()
        .unpriced_cost_events(ExecutionCount::new(1))
        .build()
        .unwrap();
    assert!(matches!(
        request(&store, &value, unknown).await,
        Err(StoreError::InvalidRunFailureClose)
    ));
    assert!(
        store
            .load_run_failure_close(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .is_none()
    );
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(run.journal_head(), Some(&value.head));
    assert_eq!(run.lease().unwrap().fence(), &value.fence);
    request(&store, &value, direct_usage()).await.unwrap();
    tick(&store, key.tenant_id()).await.items()[0]
        .result()
        .unwrap();
    store.close().await;
}

#[tokio::test]
async fn failure_close_unknown_child_cost_blocks_only_its_own_closure() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "failure-close-unknown-child").await;
    let key = value.intent.key().clone();
    let child = spawn(&store, &value)
        .await
        .unwrap()
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    let unknown = BudgetUsage::builder()
        .unpriced_cost_events(ExecutionCount::new(1))
        .build()
        .unwrap();
    fail(&store, key.tenant_id(), child, unknown).await.unwrap();
    finish_node(&store, &mut value).await;
    request(&store, &value, direct_usage()).await.unwrap();
    cancellation::reconciler(&store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        tick(&store, key.tenant_id()).await.items()[0].result(),
        Err(StoreError::UnsettledChildRuns)
    ));
    assert!(
        store
            .load_run_failure_close(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap()
            .completed_at()
            .is_none()
    );
    assert!(
        store
            .load_child_run(&key)
            .await
            .unwrap()
            .settlement()
            .is_none()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failure_closing_child_retains_its_own_failure_before_parent_settlement() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "failure-close-child-owns-reason").await;
    let key = value.intent.key().clone();
    let owned = spawn(&store, &value).await.unwrap().record().clone();
    let child = owned.child().admission().intent().provenance().run_id();
    let fence = store
        .claim_lease(key.tenant_id(), child, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let append = JournalAppend::new(
        JournalExpectation::exact(owned.child().event().head()),
        JournalEventIntent::worker(
            key.tenant_id().clone(),
            child,
            EventId::generate(),
            fence,
            payload("run-failure-close-requested"),
        )
        .unwrap(),
    )
    .unwrap();
    let child_failure = test_failure(
        "child.original.failure",
        "The child has its own earlier failure.",
    );
    store
        .request_run_failure_close(
            &owned.child().checkpoint().head(),
            child_failure.clone(),
            direct_usage(),
            append,
        )
        .await
        .unwrap();
    finish_node(&store, &mut value).await;
    let parent = request(&store, &value, direct_usage()).await.unwrap();
    cancellation::reconciler(&store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert!(
        store
            .load_child_cancellation(&key)
            .await
            .unwrap()
            .receipt()
            .is_none()
    );
    // Child closes first; the parent cannot reinterpret an Active failure close as cancellation acknowledgement.
    let first = tick(&store, key.tenant_id()).await;
    assert!(
        first
            .items()
            .iter()
            .any(|item| item.candidate().run_id() == child && item.result().is_ok())
    );
    assert!(
        store
            .load_child_cancellation(&key)
            .await
            .unwrap()
            .receipt()
            .is_none()
    );
    let child_run = store.load_run(key.tenant_id(), child).await.unwrap();
    assert_eq!(child_run.lifecycle().status(), RunStatus::Failed);
    assert_eq!(
        child_run.lifecycle().terminal_failure().unwrap().id(),
        child_failure.id()
    );
    cancellation::reconciler(&store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    tick(&store, key.tenant_id()).await.items()[0]
        .result()
        .unwrap();
    let parent_run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(
        parent_run.lifecycle().terminal_failure().unwrap().id(),
        parent.record().failure().id()
    );
    assert_eq!(
        parent_run.lifecycle().terminal_usage().unwrap(),
        &direct_usage().checked_accumulate(&direct_usage()).unwrap()
    );
    assert_eq!(
        store
            .load_child_cancellation(&key)
            .await
            .unwrap()
            .receipt()
            .unwrap()
            .outcome(),
        stateknot_store_postgres::ChildCancellationOutcome::Terminal
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failure_close_graph_handoff_recovers_without_reconsulting_evidence() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "failure-close-driver").await;
    let key = value.intent.key().clone();
    spawn(&store, &value).await.unwrap();
    finish_node(&store, &mut value).await;
    let driver = DurableGraphDriver::new(
        store.clone(),
        value.fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let (outcome, _) = driver
        .drive(value.fence.clone(), CancellationSignal::never())
        .await
        .unwrap()
        .into_parts();
    let GraphDriveOutcome::Blocked(blocked) = outcome else {
        panic!("failed node must block");
    };
    let admission = value.fixture.parent.admission().intent();
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        value.fixture.driver.registry.clone(),
        Arc::new(StaticLifecycleEvidence {
            terminal: GraphTerminalEvidence::new(
                admission.descriptor().clone(),
                admission.request().clone(),
                admission.budget().clone(),
                AgentArtifacts::empty(),
                direct_usage(),
            ),
            failure: Some(GraphFailureEvidence::new(
                test_failure(
                    "parent.original.failure",
                    "Failure evidence is frozen once.",
                ),
                direct_usage(),
            )),
        }),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let committed = lifecycle.resolve_blocked((*blocked).clone()).await.unwrap();
    assert!(matches!(
        committed,
        GraphBarrierLifecycleOutcome::FailureClosing(_)
    ));
    let restored = DurableGraphLifecycle::new(
        store.clone(),
        value.fixture.driver.registry.clone(),
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        restored.resolve_blocked(*blocked).await.unwrap(),
        GraphBarrierLifecycleOutcome::FailureClosing(_)
    ));
    assert_eq!(value.fixture.driver.first_calls.load(Ordering::SeqCst), 0);
    assert!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    store.close().await;
}
