// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_runtime::{
    DurableRunFailureCloser, register_standard_run_failure_close_event_schema,
};
use stateknot_store_postgres::RunFailureCloseOutcome;

#[path = "failure_close_upgrade.rs"]
mod upgrade;

#[path = "failure_close_process.rs"]
mod process;

#[path = "failure_close_commit_loss.rs"]
mod commit_loss;

#[path = "failure_close_completion_commit_loss.rs"]
mod completion_commit_loss;

#[path = "role_profile.rs"]
mod role_profile;

#[path = "agent_maintenance.rs"]
mod maintenance;

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

struct PanickingFailureEvidence {
    factory_panic: bool,
    fence: RunFence,
    stage: AtomicUsize,
    blocked_context: AtomicBool,
}

impl GraphLifecycleEvidenceProvider for PanickingFailureEvidence {
    fn terminal_evidence(
        &self,
        _: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Err(GraphLifecycleEvidenceError::Unavailable) })
    }

    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        self.blocked_context.store(
            context.fence() == &self.fence
                && context.blockers().in_flight() == 0
                && context.blockers().failed() == 1,
            Ordering::SeqCst,
        );
        self.stage.store(1, Ordering::SeqCst);
        assert!(
            !self.factory_panic,
            "controlled failure evidence factory panic"
        );
        Box::pin(async {
            self.stage.store(2, Ordering::SeqCst);
            panic!("controlled failure evidence poll panic");
        })
    }

    fn cancellation_evidence(
        &self,
        _: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Err(GraphLifecycleEvidenceError::Unavailable) })
    }
}

#[derive(Debug)]
struct FailurePanicObservation {
    exit: String,
    stage: usize,
    blocked_context: bool,
    loop_released: bool,
    before: Value,
    after: Value,
    replacement_fenced: bool,
    dispatches: (usize, usize),
}

fn failure_panic_child_usage() -> BudgetUsage {
    BudgetUsage::builder()
        .unpriced_cost_events(ExecutionCount::new(1))
        .build()
        .unwrap()
}

async fn failure_panic_snapshot(store: &PostgresStore, value: &Started) -> Value {
    let key = value.intent.key();
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let child = store.load_child_run(key).await.unwrap();
    let child_run = store
        .load_run(
            key.tenant_id(),
            child.child().admission().intent().provenance().run_id(),
        )
        .await
        .unwrap();
    let attempts = store
        .load_node_attempt_history_page(
            key.parent(),
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    let original_direct = attempts.records().len() == 1
        && !attempts.has_more()
        && attempts.records()[0]
            .completion()
            .is_some_and(|completion| {
                completion.status() == stateknot_core::NodeAttemptStatus::Failed
                    && completion.usage() == &direct_usage()
            });
    let checkpoint = store
        .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let no_failure_close = store
        .load_run_failure_close(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .is_none();
    json!({
        "parent_lifecycle": parent.lifecycle(),
        "parent_active": parent.lifecycle().status() == RunStatus::Active,
        "parent_head": parent.journal_head(),
        "checkpoint": checkpoint,
        "node_attempts": attempts.records(),
        "original_failed_direct": original_direct,
        "child_lifecycle": child_run.lifecycle(),
        "child_failed": child_run.lifecycle().status() == RunStatus::Failed,
        "child_usage": child_run.lifecycle().terminal_usage(),
        "child_settled": child.settlement().is_some(),
        "no_failure_close": no_failure_close,
    })
}

async fn failure_panic_exit(agent_loop: DurableAgentLoop, fence: RunFence) -> String {
    let mut task =
        tokio::spawn(async move { agent_loop.run(fence, CancellationSignal::never()).await });
    match tokio::time::timeout(Duration::from_secs(10), &mut task).await {
        Ok(Ok(Err(AgentLoopError::Lifecycle { source }))) => {
            if let stateknot_runtime::GraphLifecycleError::Evidence(
                GraphLifecycleEvidenceError::Unavailable,
            ) = *source
            {
                "closed_unavailable".to_owned()
            } else {
                format!("other_lifecycle_error: {source:?}")
            }
        }
        Ok(Err(error)) if error.is_panic() => "join_panic".to_owned(),
        Ok(other) => format!("other_exit: {other:?}"),
        Err(_) => {
            task.abort();
            let retired = task.await;
            format!("host_timeout_after_abort_and_join: {retired:?}")
        }
    }
}

async fn retire_failure_panic_claim(store: &PostgresStore, fence: &RunFence) -> bool {
    // This runs only after recording whether the public Loop itself released.
    store.release_lease(fence).await.unwrap();
    let replacement = store
        .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let stale_rejected = matches!(
        store.release_lease(fence).await,
        Err(StoreError::StaleFence)
    );
    let parent = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let replacement_preserved = parent
        .lease()
        .is_some_and(|lease| lease.fence() == &replacement);
    store.release_lease(&replacement).await.unwrap();
    replacement.epoch() > fence.epoch() && stale_rejected && replacement_preserved
}

async fn failure_panic_fixture(
    store: &PostgresStore,
    factory_panic: bool,
) -> FailurePanicObservation {
    let name = if factory_panic {
        "failure-evidence-factory-panic"
    } else {
        "failure-evidence-poll-panic"
    };
    let mut value = Box::pin(started(store, name)).await;
    let child = Box::pin(spawn(store, &value))
        .await
        .unwrap()
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    Box::pin(fail(
        store,
        value.fence.tenant_id(),
        child,
        failure_panic_child_usage(),
    ))
    .await
    .unwrap();
    Box::pin(finish_node(store, &mut value)).await;
    let before = Box::pin(failure_panic_snapshot(store, &value)).await;
    let evidence = Arc::new(PanickingFailureEvidence {
        factory_panic,
        fence: value.fence.clone(),
        stage: AtomicUsize::new(0),
        blocked_context: AtomicBool::new(false),
    });
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        value.fixture.driver.registry.clone(),
        evidence.clone(),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let exit = Box::pin(failure_panic_exit(agent_loop, value.fence.clone())).await;
    let parent = store
        .load_run(value.fence.tenant_id(), value.fence.run_id())
        .await
        .unwrap();
    let loop_released = parent.lease().is_none();
    let after = Box::pin(failure_panic_snapshot(store, &value)).await;
    let replacement_fenced = Box::pin(retire_failure_panic_claim(store, &value.fence)).await;
    FailurePanicObservation {
        exit,
        stage: evidence.stage.load(Ordering::SeqCst),
        blocked_context: evidence.blocked_context.load(Ordering::SeqCst),
        loop_released,
        before,
        after,
        replacement_fenced,
        dispatches: (
            value.fixture.driver.first_calls.load(Ordering::SeqCst),
            value.fixture.driver.second_calls.load(Ordering::SeqCst),
        ),
    }
}

#[tokio::test]
async fn failure_evidence_factory_and_poll_panic_are_closed_and_preserve_unpriced_child_usage() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    // Both independent fixtures finish and every task/lease is retired before verdict.
    let observations = [
        Box::pin(failure_panic_fixture(&store, true)).await,
        Box::pin(failure_panic_fixture(&store, false)).await,
    ];
    store.close().await;
    for (factory_panic, observation) in [true, false].into_iter().zip(&observations) {
        eprintln!(
            "factory_panic={factory_panic}: exit={}, stage={}, loop_released={}",
            observation.exit, observation.stage, observation.loop_released,
        );
    }
    for (factory_panic, observation) in [true, false].into_iter().zip(observations) {
        assert_eq!(observation.stage, if factory_panic { 1 } else { 2 });
        assert!(observation.blocked_context);
        assert_eq!(observation.exit, "closed_unavailable", "{observation:?}");
        assert!(observation.loop_released);
        assert_eq!(observation.before, observation.after);
        for field in [
            "parent_active",
            "original_failed_direct",
            "child_failed",
            "no_failure_close",
        ] {
            assert_eq!(observation.after[field], json!(true), "{field}");
        }
        assert_eq!(
            observation.after["child_usage"],
            json!(failure_panic_child_usage())
        );
        assert_eq!(observation.after["child_settled"], json!(false));
        assert!(observation.replacement_fenced);
        assert_eq!(observation.dispatches, (0, 0));
    }
}

// A real provider Unknown is distinct from a synthetic unpriced terminal usage.
use sqlx_postgres::{PgPool, PgPoolOptions};
use stateknot_core::{JournalEvent, ToolInvocation};
use std::collections::BTreeMap;

#[derive(Clone)]
struct CloseToolReceipt {
    tenant: TenantId,
    run: RunId,
    thread: ThreadId,
    origin: EventId,
    input: ToolInput,
    result: ToolResult,
}

#[derive(Default)]
struct CloseToolBackend {
    sends: AtomicUsize,
    probes: AtomicUsize,
    denied: AtomicUsize,
    receipt: std::sync::Mutex<Option<CloseToolReceipt>>,
    consent: std::sync::Mutex<Option<(ToolInvocation, &'static str)>>,
}

#[derive(Clone)]
struct CloseUnknownTool {
    descriptor: ToolDescriptor,
    unknown: bool,
    backend: Arc<CloseToolBackend>,
}

impl ErasedTool for CloseUnknownTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn supports_reconciliation(&self) -> bool {
        true
    }

    fn call(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        self.backend.sends.fetch_add(1, Ordering::SeqCst);
        let result = ToolResult::for_invocation(
            &context,
            &self.descriptor,
            BoundedJson::try_from_value(json!({
                "receipt": context.invocation_id().to_string(), "operation": "test.write"
            }))
            .unwrap(),
            ToolArtifacts::empty(),
        );
        *self.backend.receipt.lock().unwrap() = Some(CloseToolReceipt {
            tenant: context.tenant_id().clone(),
            run: context.run_id(),
            thread: context.thread_id(),
            origin: context.origin_event_id().unwrap(),
            input,
            result: result.clone(),
        });
        let outcome = if self.unknown {
            let failure = Failure::new(
                FailureId::generate(),
                FailureCategory::AmbiguousExternalOutcome,
                FailureCode::new("test.close.effect_unknown").unwrap(),
                FailureOrigin::new("test.close.tool").unwrap(),
                FailureMessage::new("The write acknowledgement was lost.").unwrap(),
                RetryAdvice::ReconcileFirst,
            )
            .unwrap();
            Err(ToolError::new(
                failure,
                ToolErrorPhase::Execution,
                ToolExternalEffect::Unknown,
                ToolErrorProvenance::for_invocation(&context, &self.descriptor),
            )
            .unwrap())
        } else {
            Ok(result)
        };
        Box::pin(async move { outcome })
    }

    fn reconcile(
        &self,
        context: ToolReconciliationContext,
        input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolReconciliationObservation, ToolReconciliationProbeError>> {
        self.backend.probes.fetch_add(1, Ordering::SeqCst);
        let receipt = self.backend.receipt.lock().unwrap().clone().unwrap();
        let consent = self.backend.consent.lock().unwrap().clone();
        let allowed = consent.as_ref().is_some_and(|(original, operation)| {
            *operation == "read-original-write-receipt"
                && matches!(original.state(), ToolInvocationState::Unknown { .. })
                && original.intent().descriptor() == &self.descriptor
                && original.intent().input() == &input
                && input == receipt.input
                && original.intent().tenant_id() == context.tenant_id()
                && original.intent().run_id() == context.run_id()
                && original.intent().invocation_id() == context.invocation_id()
                && original.attempt_id() == Some(context.attempt_id())
                && original
                    .previous()
                    .map(|head| head.journal_head().event_id())
                    == context.origin_event_id()
                && context.tenant_id() == &receipt.tenant
                && context.run_id() == receipt.run
                && context.thread_id() == receipt.thread
                && context.origin_event_id() == Some(receipt.origin)
                && context.tool() == self.descriptor.metadata().identity()
                && receipt.result.provenance().invocation_id() == context.invocation_id()
                && receipt.result.provenance().attempt_id() == context.attempt_id()
                && receipt.result.output_schema() == self.descriptor.output_schema()
        });
        let outcome = if allowed {
            Ok(ToolReconciliationObservation::Result(receipt.result))
        } else {
            self.backend.denied.fetch_add(1, Ordering::SeqCst);
            Err(ToolReconciliationProbeError::new(test_failure(
                "test.close.reconciliation_denied",
                "The original operation has no current consent.",
            ))
            .unwrap())
        };
        Box::pin(async move { outcome })
    }
}

fn close_tool(unknown: bool) -> CloseUnknownTool {
    let template = tool_descriptor();
    let (output, _) = schema("failure-close-real-tool-output");
    CloseUnknownTool {
        descriptor: ToolDescriptor::new(
            template.metadata().clone(),
            template.input_schema().clone(),
            output,
            template.semantics().clone(),
            template.resources().clone(),
            template.invocation().clone(),
            template.limits().clone(),
        )
        .unwrap(),
        unknown,
        backend: Arc::new(CloseToolBackend::default()),
    }
}

fn close_tool_executor(
    store: &PostgresStore,
    tool: &CloseUnknownTool,
) -> DurableInvocationExecutor {
    let (_, output) = schema("failure-close-real-tool-output");
    let mut tools = ToolProviderRegistryBuilder::new();
    tools.register(Arc::new(tool.clone())).unwrap();
    DurableInvocationExecutor::new(
        store.clone(),
        invocation_schema_registry_with(tool.descriptor.output_schema().clone(), output),
        ModelProviderRegistryBuilder::new().build(),
        tools.build(),
        Arc::new(StaticInvocationBudget {
            resolved: invocation_budget(),
        }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap()
}

async fn close_tool_start(
    store: &PostgresStore,
    fence: &RunFence,
    tool: &CloseUnknownTool,
) -> ToolInvocation {
    let checkpoint = store
        .load_current_checkpoint(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .unwrap();
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(current.lifecycle().status(), RunStatus::Active);
    let intent = ToolInvocationIntent::new(
        invocation_activation(&checkpoint),
        InvocationId::generate(),
        tool.descriptor.clone(),
        tool_input(&tool.descriptor),
        tool.descriptor.limits().clone(),
    )
    .unwrap();
    let prepared = store
        .prepare_tool_invocation(
            worker_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                current.journal_head().unwrap().clone(),
                fence.clone(),
            ),
            intent,
        )
        .await
        .unwrap();
    let handoff = ToolAttemptHandoff::new(
        fence.clone(),
        prepared.invocation().clone(),
        AttemptId::generate(),
        InvocationAttemptEventIds::generate(),
        CancellationSignal::never(),
        None,
    )
    .unwrap();
    let outcome = close_tool_executor(store, tool)
        .execute_tool(handoff)
        .await
        .unwrap();
    let ToolAttemptOutcome::Dispatched { invocation, .. } = outcome else {
        panic!("a fresh business call must dispatch: {outcome:?}");
    };
    invocation
}

async fn close_unknown_snapshot(pool: &PgPool, tenant: &TenantId) -> BTreeMap<String, String> {
    let mut tx = pool.begin().await.unwrap();
    query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .execute(&mut *tx)
        .await
        .unwrap();
    let tables = query_scalar::<_, String>("SELECT quote_ident(c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='stateknot' AND c.relkind IN ('r','p') AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='tenant_id' AND NOT a.attisdropped) ORDER BY c.relname").fetch_all(&mut *tx).await.unwrap();
    let mut snapshot = BTreeMap::new();
    for table in tables {
        let rows = query_scalar::<_, String>(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM stateknot.{table} t WHERE tenant_id=$1"))
            .bind(tenant.as_str()).fetch_one(&mut *tx).await.unwrap();
        snapshot.insert(table, rows);
    }
    let migrations = query_scalar::<_, String>(
        "SELECT jsonb_agg(to_jsonb(m) ORDER BY version)::text FROM _sqlx_migrations m",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    snapshot.insert("_sqlx_migrations".to_owned(), migrations);
    tx.commit().await.unwrap();
    snapshot
}

async fn close_unknown_rebuild(store: &mut PostgresStore) {
    store.close().await;
    *store = test_store().await.unwrap();
}

async fn close_unknown_claim(store: &PostgresStore, tenant: &TenantId, run: RunId) -> RunFence {
    store
        .claim_lease(tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone()
}

async fn close_unknown_tool_usage(
    store: &PostgresStore,
    tenant: &TenantId,
    run: RunId,
    id: InvocationId,
    events: &[JournalEvent],
) -> BudgetUsage {
    let current = store.load_tool_invocation(tenant, run, id).await.unwrap();
    let ToolInvocationState::Committed { result } = current.state() else {
        panic!("Unknown is never a complete priced DIRECT observation");
    };
    let mut head = current.head();
    let mut statuses = Vec::new();
    for _ in 0..4 {
        let (event, record) = store
            .load_tool_invocation_revision(tenant, run, id, head.revision())
            .await
            .unwrap();
        assert_eq!(event.head(), record.journal_head().clone());
        assert!(events.contains(&event));
        statuses.push(record.status());
        if let Some(previous) = record.previous() {
            head = previous.clone();
        } else {
            break;
        }
    }
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == ToolInvocationStatus::Prepared)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == ToolInvocationStatus::Executing)
            .count(),
        1
    );
    let accounting = PricedInvocationAccounting {
        unpriced: false,
        reference: AgentInvocationAccountingReference::new(
            capability("failure-close-tool-price"),
            Digest::sha256(b"failure-close original Tool known USD micros v1"),
        ),
    };
    let AgentInvocationCharge::Known(costs) = accounting.tool_charge(&current) else {
        panic!("the selected receipt has an explicit known-price policy");
    };
    BudgetUsage::builder()
        .tool_calls(ExecutionCount::new(1))
        .write_calls(ExecutionCount::new(u64::from(
            current.intent().descriptor().semantics().risk() != ToolRisk::ReadOnly,
        )))
        .input_bytes(ByteCount::new(
            u64::try_from(current.intent().input().value().stats().compact_bytes()).unwrap(),
        ))
        .output_bytes(ByteCount::new(
            u64::try_from(result.output().stats().compact_bytes()).unwrap(),
        ))
        .artifact_bytes(result.artifacts().total_bytes())
        .known_costs(costs)
        .build()
        .unwrap()
}

fn close_unknown_event_bytes(events: &[JournalEvent]) -> u64 {
    events
        .iter()
        .map(|event| u64::try_from(serde_json_canonicalizer::to_vec(event).unwrap().len()).unwrap())
        .sum()
}

async fn close_unknown_usage(
    store: &PostgresStore,
    pool: &PgPool,
    key: &ChildRunKey,
    run: RunId,
    tool: Option<InvocationId>,
    node: Option<&NodeAttemptStartHead>,
) -> BudgetUsage {
    let tenant = key.tenant_id();
    let admitted = store.load_agent_admission(tenant, run).await.unwrap();
    let current = store.load_run(tenant, run).await.unwrap();
    let page = store
        .load_journal_page(tenant, run, None, JournalPageSize::new(256).unwrap())
        .await
        .unwrap();
    assert!(!page.has_more());
    assert_eq!(
        page.events().last().map(JournalEvent::head).as_ref(),
        current.journal_head()
    );
    assert_eq!(
        current.checkpoint().map(|pointer| (
            pointer.checkpoint_id(),
            pointer.superstep(),
            pointer.digest()
        )),
        Some((
            admitted.checkpoint().checkpoint_id(),
            admitted.checkpoint().superstep(),
            admitted.checkpoint().digest()
        ))
    );
    for (table, expected) in [
        ("model_invocations", 0),
        ("graph_frame_entries", 0),
        ("run_checkpoints", 1),
        ("tool_invocations", i64::from(tool.is_some())),
        ("node_attempts", i64::from(node.is_some())),
    ] {
        let count = query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM stateknot.{table} WHERE tenant_id=$1 AND run_id=$2"
        ))
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(count, expected, "closed fixture ledger: {table}");
    }
    let event_bytes = close_unknown_event_bytes(page.events());
    let mut usage = BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(1))
        .concurrent_branches(ExecutionCount::new(u64::from(node.is_some())))
        .fan_out(ExecutionCount::new(u64::from(node.is_some())))
        .input_bytes(ByteCount::new(
            u64::try_from(
                admitted
                    .admission()
                    .intent()
                    .request()
                    .input()
                    .stats()
                    .compact_bytes(),
            )
            .unwrap(),
        ))
        .event_bytes(ByteCount::new(event_bytes))
        .checkpoint_bytes(ByteCount::new(
            u64::try_from(
                serde_json_canonicalizer::to_vec(admitted.checkpoint())
                    .unwrap()
                    .len(),
            )
            .unwrap(),
        ))
        .build()
        .unwrap();
    if let Some(node) = node {
        let attempt = store
            .load_node_attempt(tenant, &run, node.attempt_id())
            .await
            .unwrap();
        assert_eq!(attempt.start().head(), *node);
        let completion = attempt.completion().unwrap();
        assert_eq!(
            completion.status(),
            stateknot_core::NodeAttemptStatus::Failed
        );
        assert_eq!(completion.usage(), &direct_usage());
        // This ordinary Start has no committed graph step; its failure's seven
        // input tokens exclude the separately read Tool invocation and child.
        usage = usage.checked_accumulate(completion.usage()).unwrap();
    }
    if let Some(id) = tool {
        let tool_usage = Box::pin(close_unknown_tool_usage(
            store,
            tenant,
            run,
            id,
            page.events(),
        ))
        .await;
        usage = usage.checked_accumulate(&tool_usage).unwrap();
    }
    usage
}

struct CloseUnknownEvidence {
    store: PostgresStore,
    pool: PgPool,
    key: ChildRunKey,
    tool: Option<InvocationId>,
    node: Option<NodeAttemptStartHead>,
    failure: Option<Failure>,
    calls: Arc<AtomicUsize>,
}

impl GraphLifecycleEvidenceProvider for CloseUnknownEvidence {
    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        UnavailableLifecycleEvidence.terminal_evidence(context)
    }

    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let (Some(node), Some(failure)) = (self.node.as_ref(), self.failure.as_ref()) else {
                return Err(GraphLifecycleEvidenceError::Unavailable);
            };
            let run = self
                .store
                .load_run(
                    context.provenance().tenant_id(),
                    context.provenance().run_id(),
                )
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            if context.leaf().is_some()
                || context.provenance().tenant_id() != self.key.tenant_id()
                || context.provenance().run_id() != self.key.parent_run_id()
                || run.journal_head() != Some(context.journal_head())
                || run.checkpoint().map(|pointer| {
                    (
                        pointer.checkpoint_id(),
                        pointer.superstep(),
                        pointer.digest(),
                    )
                }) != Some((
                    context.checkpoint().checkpoint_id(),
                    context.checkpoint().superstep(),
                    context.checkpoint().digest(),
                ))
                || run.lease().map(stateknot_core::RunLease::fence) != Some(context.fence())
                || context.blockers().failed() != 1
                || context.blockers().in_flight() != 0
            {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            }
            let usage = Box::pin(close_unknown_usage(
                &self.store,
                &self.pool,
                &self.key,
                context.provenance().run_id(),
                self.tool,
                Some(node),
            ))
            .await;
            Ok(GraphFailureEvidence::new(failure.clone(), usage))
        })
    }

    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let run = self
                .store
                .load_run(
                    context.provenance().tenant_id(),
                    context.provenance().run_id(),
                )
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            let child = self
                .store
                .load_child_run(&self.key)
                .await
                .map_err(|_| GraphLifecycleEvidenceError::Unavailable)?;
            if context.leaf().is_some()
                || context.provenance().tenant_id() != self.key.tenant_id()
                || context.provenance().run_id()
                    != child.child().admission().intent().provenance().run_id()
                || run.journal_head() != Some(context.journal_head())
                || run.checkpoint().map(|pointer| {
                    (
                        pointer.checkpoint_id(),
                        pointer.superstep(),
                        pointer.digest(),
                    )
                }) != Some((
                    context.checkpoint().checkpoint_id(),
                    context.checkpoint().superstep(),
                    context.checkpoint().digest(),
                ))
                || run.lease().map(stateknot_core::RunLease::fence) != Some(context.fence())
            {
                return Err(GraphLifecycleEvidenceError::Corrupt);
            }
            let usage = Box::pin(close_unknown_usage(
                &self.store,
                &self.pool,
                &self.key,
                context.provenance().run_id(),
                self.tool,
                None,
            ))
            .await;
            Ok(GraphCancellationEvidence::new(usage))
        })
    }
}

async fn close_unknown_child(
    store: &PostgresStore,
    pool: &PgPool,
    value: &Started,
    fence: &RunFence,
    tool: InvocationId,
    checks: &mut Vec<(&'static str, bool)>,
) -> JournalEvent {
    let calls = Arc::new(AtomicUsize::new(0));
    let evidence = Arc::new(CloseUnknownEvidence {
        store: store.clone(),
        pool: pool.clone(),
        key: value.intent.key().clone(),
        tool: Some(tool),
        node: None,
        failure: None,
        calls: Arc::clone(&calls),
    });
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        value.fixture.child_driver.registry.clone(),
        evidence,
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let result = agent_loop
        .run(fence.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let AgentLoopOutcome::CancellationConfirmed(outcome) = result.outcome() else {
        panic!("the real public Loop must confirm child cancellation: {result:?}");
    };
    let event = outcome.event().clone();
    checks.push((
        "child public Loop used complete evidence",
        calls.load(Ordering::SeqCst) == 1,
    ));
    checks.push((
        "child cancellation did not dispatch ordinary nodes",
        value
            .fixture
            .child_driver
            .first_calls
            .load(Ordering::SeqCst)
            == 0
            && value
                .fixture
                .child_driver
                .second_calls
                .load(Ordering::SeqCst)
                == 0,
    ));
    event
}

async fn close_unknown_reconcile(
    store: &PostgresStore,
    pool: &PgPool,
    fence: &RunFence,
    original: &ToolInvocation,
    tool: &CloseUnknownTool,
    checks: &mut Vec<(&'static str, bool)>,
) {
    let executor = close_tool_executor(store, tool);
    let before = close_unknown_snapshot(pool, fence.tenant_id()).await;
    let denied = executor
        .reconcile_tool(
            ToolReconciliationAttemptHandoff::new(
                fence.clone(),
                original.clone(),
                EventId::generate(),
                CancellationSignal::never(),
            )
            .unwrap(),
        )
        .await;
    checks.push((
        "unconsented original-operation probe is exactly denied",
        matches!(denied,
        Err(stateknot_runtime::ToolReconciliationAttemptExecutionError::Probe(ref error))
            if error.failure().code().as_str() == "test.close.reconciliation_denied"),
    ));
    checks.push((
        "denied probe is a full-tenant no-write",
        before == close_unknown_snapshot(pool, fence.tenant_id()).await,
    ));
    *tool.backend.consent.lock().unwrap() = Some((original.clone(), "read-original-write-receipt"));
    let resolved = executor
        .reconcile_tool(
            ToolReconciliationAttemptHandoff::new(
                fence.clone(),
                original.clone(),
                EventId::generate(),
                CancellationSignal::never(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    checks.push((
        "authorized original-operation probe resolved",
        matches!(
            resolved,
            ToolReconciliationAttemptOutcome::Resolved {
                kind: ToolReconciliationKind::Result,
                ..
            }
        ),
    ));
    let current = resolved.invocation().clone();
    let (ack, owned) = store
        .load_tool_invocation_revision(
            fence.tenant_id(),
            fence.run_id(),
            current.intent().invocation_id(),
            current.revision(),
        )
        .await
        .unwrap();
    let receipt = tool.backend.receipt.lock().unwrap().clone().unwrap();
    checks.push(("complete resolved Tool record is the original receipt", json!(owned) == json!(current)
        && matches!(current.state(), ToolInvocationState::Committed { result } if result == &receipt.result)));
    let before = close_unknown_snapshot(pool, fence.tenant_id()).await;
    let replay = executor
        .commit_tool_reconciliation(
            ToolReconciliationHandoff::result(
                fence.clone(),
                original.clone(),
                ack.event_id(),
                receipt.result,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let replay_exact = matches!(replay,
        ToolReconciliationOutcome::Idempotent { event, invocation, .. } if event == ack && json!(invocation) == json!(current));
    checks.push((
        "reconciliation replay preserves complete original ACK",
        replay_exact,
    ));
    checks.push((
        "reconciliation replay is a full-tenant no-write",
        before == close_unknown_snapshot(pool, fence.tenant_id()).await,
    ));
    checks.push((
        "business write is never redispatched",
        tool.backend.sends.load(Ordering::SeqCst) == 1,
    ));
    checks.push((
        "explicit denial and authorization both reached the provider",
        tool.backend.probes.load(Ordering::SeqCst) == 2
            && tool.backend.denied.load(Ordering::SeqCst) == 1,
    ));
}

async fn close_unknown_setup(
    store: &PostgresStore,
    name: &str,
    tracked: &mut Vec<(TenantId, RunId)>,
) -> (Started, RunId) {
    let value = Box::pin(started(store, name)).await;
    let key = value.intent.key();
    tracked.push((key.tenant_id().clone(), key.parent_run_id()));
    let child = Box::pin(spawn(store, &value))
        .await
        .unwrap()
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    tracked.push((key.tenant_id().clone(), child));
    (value, child)
}

async fn close_unknown_node_failure(store: &PostgresStore, value: &mut Started) -> Failure {
    Box::pin(finish_node(store, value)).await;
    let attempt = store
        .load_node_attempt(
            value.fence.tenant_id(),
            &value.fence.run_id(),
            value.node.attempt_id(),
        )
        .await
        .unwrap();
    let stateknot_core::NodeAttemptOutcome::Failed { failure } =
        attempt.completion().unwrap().outcome()
    else {
        panic!("the original completed node must retain its failure");
    };
    failure.clone()
}

async fn close_unknown_request(
    store: &PostgresStore,
    value: &Started,
    failure: &Failure,
    usage: BudgetUsage,
) -> Result<RunFailureCloseOutcome, StoreError> {
    let current = store
        .load_run(value.fence.tenant_id(), value.fence.run_id())
        .await?;
    let append = JournalAppend::new(
        JournalExpectation::exact(current.journal_head().unwrap().clone()),
        JournalEventIntent::worker(
            value.fence.tenant_id().clone(),
            value.fence.run_id(),
            EventId::generate(),
            value.fence.clone(),
            payload("run-failure-close-requested"),
        )
        .unwrap(),
    )
    .unwrap();
    store
        .request_run_failure_close(
            value.intent.key().parent().base_checkpoint(),
            failure.clone(),
            usage,
            append,
        )
        .await
}

async fn close_unknown_reject_parent(
    store: &PostgresStore,
    pool: &PgPool,
    value: &Started,
    failure: &Failure,
    checks: &mut Vec<(&'static str, bool)>,
) {
    let before = close_unknown_snapshot(pool, value.fence.tenant_id()).await;
    // Negative candidate only: this known floor is never attested as COMPLETE
    // DIRECT while the real invocation remains Unknown.
    let result = close_unknown_request(store, value, failure, direct_usage()).await;
    checks.push((
        "A exact real Tool blocker",
        matches!(result, Err(StoreError::CheckpointBlockedByToolInvocation)),
    ));
    checks.push((
        "A rejected registration is a full-tenant no-write",
        before == close_unknown_snapshot(pool, value.fence.tenant_id()).await,
    ));
    let run = store
        .load_run(value.fence.tenant_id(), value.fence.run_id())
        .await
        .unwrap();
    checks.push((
        "A remains Active with its original failure",
        run.lifecycle().status() == RunStatus::Active
            && store
                .load_run_failure_close(value.fence.tenant_id(), value.fence.run_id())
                .await
                .unwrap()
                .is_none(),
    ));
}

async fn close_unknown_parent(
    store: &PostgresStore,
    pool: &PgPool,
    value: &Started,
    failure: &Failure,
    tool: Option<InvocationId>,
    checks: &mut Vec<(&'static str, bool)>,
) -> stateknot_store_postgres::RunFailureCloseRecord {
    let calls = Arc::new(AtomicUsize::new(0));
    let evidence = Arc::new(CloseUnknownEvidence {
        store: store.clone(),
        pool: pool.clone(),
        key: value.intent.key().clone(),
        tool,
        node: Some(value.node.clone()),
        failure: Some(failure.clone()),
        calls: Arc::clone(&calls),
    });
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        value.fixture.driver.registry.clone(),
        evidence,
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let result = agent_loop
        .run(value.fence.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let AgentLoopOutcome::FailureClosing(outcome) = result.outcome() else {
        panic!("the public parent Loop must seal its original failure: {result:?}");
    };
    let record = outcome.record().clone();
    checks.push((
        "parent public Loop selected its original blocked failure",
        calls.load(Ordering::SeqCst) == 1
            && json!(record.failure()) == json!(failure)
            && value.fixture.driver.first_calls.load(Ordering::SeqCst) == 0
            && value.fixture.driver.second_calls.load(Ordering::SeqCst) == 0,
    ));
    record
}

async fn close_unknown_unsettled(
    store: &PostgresStore,
    pool: &PgPool,
    key: &ChildRunKey,
    checks: &mut Vec<(&'static str, bool)>,
) {
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    let report = tick(store, key.tenant_id()).await;
    checks.push((
        "B exact unsettled child blocker",
        report.items().len() == 1
            && matches!(
                report.items()[0].result(),
                Err(StoreError::UnsettledChildRuns)
            ),
    ));
    checks.push((
        "B rejected completion is a full-tenant no-write",
        before == close_unknown_snapshot(pool, key.tenant_id()).await,
    ));
    let child = store.load_child_run(key).await.unwrap();
    let request = store
        .load_run_failure_close(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    checks.push((
        "B retains pending parent decision and unsettled child",
        request.completed_at().is_none()
            && child.settlement().is_none()
            && child.child().run().lifecycle().terminal_usage().is_none(),
    ));
}

async fn close_unknown_parent_replay(
    store: &mut PostgresStore,
    pool: &PgPool,
    value: &Started,
    original: &stateknot_store_postgres::RunFailureCloseRecord,
    terminal_ack: &JournalEvent,
    checks: &mut Vec<(&'static str, bool)>,
) {
    let key = value.intent.key();
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    close_unknown_rebuild(store).await;
    checks.push((
        "completed Store reconstruction is a full-tenant no-write",
        before == close_unknown_snapshot(pool, key.tenant_id()).await,
    ));
    let replay = close_unknown_request(
        store,
        value,
        original.failure(),
        original.direct_usage().clone(),
    )
    .await
    .unwrap();
    let current = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(current.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            key.tenant_id().clone(),
            key.parent_run_id(),
            EventId::generate(),
            payload("run-failure-close-completed"),
        )
        .unwrap(),
    )
    .unwrap();
    let completed = store.complete_run_failure_close(append).await.unwrap();
    checks.push((
        "cold original registration and completion replay return Existing",
        matches!(replay, RunFailureCloseOutcome::Existing(_))
            && matches!(completed, RunFailureCloseOutcome::Existing(_))
            && completed.record().registration() == original.registration(),
    ));
    let page = store
        .load_journal_page(
            key.tenant_id(),
            key.parent_run_id(),
            None,
            JournalPageSize::new(256).unwrap(),
        )
        .await
        .unwrap();
    checks.push((
        "cold replay preserves complete terminal ACK and every tenant fact",
        page.events().last() == Some(terminal_ack)
            && before == close_unknown_snapshot(pool, key.tenant_id()).await,
    ));
}

async fn close_unknown_terminal_account(
    store: &PostgresStore,
    key: &ChildRunKey,
    original: &stateknot_store_postgres::RunFailureCloseRecord,
    account: &stateknot_core::ChildRunBudgetAccount,
    terminal_ack: &JournalEvent,
    checks: &mut Vec<(&'static str, bool)>,
) -> stateknot_core::ChildRunBudgetAccount {
    let expected_account = account
        .clone()
        .observe_direct(terminal_ack.head(), original.direct_usage().clone())
        .unwrap();
    let actual_account = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    checks.push((
        "parent terminal advances exact DIRECT without settling child again",
        actual_account == expected_account
            && actual_account.children() == account.children()
            && actual_account.delegated_usage().unwrap() == account.delegated_usage().unwrap(),
    ));
    actual_account
}

async fn close_unknown_finish(
    store: &mut PostgresStore,
    pool: &PgPool,
    value: &Started,
    original: &stateknot_store_postgres::RunFailureCloseRecord,
    child_ack: &JournalEvent,
    checks: &mut Vec<(&'static str, bool)>,
) -> Value {
    let key = value.intent.key();
    let child = store.load_child_run(key).await.unwrap();
    let child_usage = child
        .child()
        .run()
        .lifecycle()
        .terminal_usage()
        .unwrap()
        .clone();
    let account = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    checks.push((
        "whole original child account is settled once",
        child.settlement().is_some()
            && account.children().len() == 1
            && account
                .entry(key)
                .and_then(|entry| entry.settlement())
                .is_some_and(|settlement| settlement.usage() == &child_usage)
            && account.delegated_usage().unwrap() == child_usage.cumulative_only(),
    ));
    checks.push((
        "original child terminal ACK is retained",
        child.child().run().journal_head() == Some(&child_ack.head()),
    ));
    let finished = tick(store, key.tenant_id()).await;
    let finished = finished.items()[0].result().unwrap().record();
    checks.push((
        "original parent failure-close registration ACK retained",
        finished.registration() == original.registration()
            && json!(finished.failure()) == json!(original.failure())
            && finished.direct_usage() == original.direct_usage()
            && finished.completed_at().is_some(),
    ));
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let page = store
        .load_journal_page(
            key.tenant_id(),
            key.parent_run_id(),
            None,
            JournalPageSize::new(256).unwrap(),
        )
        .await
        .unwrap();
    let terminal_ack = page.events().last().unwrap().clone();
    let expected_usage = original
        .direct_usage()
        .checked_accumulate(&account.delegated_usage().unwrap())
        .unwrap();
    checks.push((
        "parent terminal preserves exact original failure and complete once usage",
        run.lifecycle().status() == RunStatus::Failed
            && json!(run.lifecycle().terminal_failure()) == json!(Some(original.failure()))
            && run.lifecycle().terminal_usage() == Some(&expected_usage)
            && run.lease().is_none()
            && run.journal_head() == Some(&terminal_ack.head())
            && !page.has_more(),
    ));
    let actual_account = Box::pin(close_unknown_terminal_account(
        store,
        key,
        original,
        &account,
        &terminal_ack,
        checks,
    ))
    .await;
    let before_cold = close_unknown_snapshot(pool, key.tenant_id()).await;
    Box::pin(close_unknown_parent_replay(
        store,
        pool,
        value,
        original,
        &terminal_ack,
        checks,
    ))
    .await;
    checks.push((
        "cold replay preserves the complete terminal account and whole tenant",
        store
            .load_child_budget_account(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap()
            == actual_account
            && before_cold == close_unknown_snapshot(pool, key.tenant_id()).await,
    ));
    json!({"parent":key.parent_run_id(), "child":child.child().admission().intent().provenance().run_id(),
        "original_registration_ack":original.registration(), "child_terminal_ack":child_ack,
        "parent_terminal_ack":terminal_ack, "direct":original.direct_usage(), "child_usage":child_usage,
        "parent_usage":expected_usage, "whole_child_account":actual_account})
}

async fn close_unknown_cold(
    store: &mut PostgresStore,
    pool: &PgPool,
    key: &ChildRunKey,
    run: RunId,
    original: &ToolInvocation,
    old_fence: &RunFence,
    checks: &mut Vec<(&'static str, bool)>,
) -> RunFence {
    store.release_lease(old_fence).await.unwrap();
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    close_unknown_rebuild(store).await;
    let current = store
        .load_tool_invocation(key.tenant_id(), run, original.intent().invocation_id())
        .await
        .unwrap();
    checks.push((
        "Unknown and original request survive Store reconstruction without writes",
        json!(current) == json!(original)
            && before == close_unknown_snapshot(pool, key.tenant_id()).await,
    ));
    close_unknown_claim(store, key.tenant_id(), run).await
}

async fn close_unknown_a(
    store: &mut PostgresStore,
    pool: &PgPool,
    tracked: &mut Vec<(TenantId, RunId)>,
    checks: &mut Vec<(&'static str, bool)>,
) -> Value {
    let (mut value, child) = Box::pin(close_unknown_setup(
        store,
        "close-real-unknown-parent",
        tracked,
    ))
    .await;
    let key = value.intent.key().clone();
    let child_fence = close_unknown_claim(store, key.tenant_id(), child).await;
    let child_tool = close_tool(false);
    let child_invocation = Box::pin(close_tool_start(store, &child_fence, &child_tool)).await;
    cancellation::cancel_run(store, key.tenant_id(), child).await;
    let child_ack = Box::pin(close_unknown_child(
        store,
        pool,
        &value,
        &child_fence,
        child_invocation.intent().invocation_id(),
        checks,
    ))
    .await;
    let settled = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert!(settled.items().iter().all(|item| item.result().is_ok()));
    assert!(
        store
            .load_child_run(&key)
            .await
            .unwrap()
            .settlement()
            .is_some()
    );
    let tool = close_tool(true);
    let original_tool = Box::pin(close_tool_start(store, &value.fence, &tool)).await;
    checks.push(("A business call reached real Unknown", matches!(original_tool.state(), ToolInvocationState::Unknown { error }
        if error.external_effect() == ToolExternalEffect::Unknown && error.failure().retry_advice() == RetryAdvice::ReconcileFirst)));
    let failure = Box::pin(close_unknown_node_failure(store, &mut value)).await;
    Box::pin(close_unknown_reject_parent(
        store, pool, &value, &failure, checks,
    ))
    .await;
    value.fence = Box::pin(close_unknown_cold(
        store,
        pool,
        &key,
        key.parent_run_id(),
        &original_tool,
        &value.fence,
        checks,
    ))
    .await;
    Box::pin(close_unknown_reject_parent(
        store, pool, &value, &failure, checks,
    ))
    .await;
    Box::pin(close_unknown_reconcile(
        store,
        pool,
        &value.fence,
        &original_tool,
        &tool,
        checks,
    ))
    .await;
    let original = Box::pin(close_unknown_parent(
        store,
        pool,
        &value,
        &failure,
        Some(original_tool.intent().invocation_id()),
        checks,
    ))
    .await;
    checks.push((
        "A known child also sent its own business call once",
        child_tool.backend.sends.load(Ordering::SeqCst) == 1,
    ));
    Box::pin(close_unknown_finish(
        store, pool, &value, &original, &child_ack, checks,
    ))
    .await
}

async fn close_unknown_b(
    store: &mut PostgresStore,
    pool: &PgPool,
    tracked: &mut Vec<(TenantId, RunId)>,
    checks: &mut Vec<(&'static str, bool)>,
) -> Value {
    let (mut value, child) = Box::pin(close_unknown_setup(
        store,
        "close-real-unknown-child",
        tracked,
    ))
    .await;
    let key = value.intent.key().clone();
    let child_fence = close_unknown_claim(store, key.tenant_id(), child).await;
    let tool = close_tool(true);
    let original_tool = Box::pin(close_tool_start(store, &child_fence, &tool)).await;
    checks.push(("B business call reached real Unknown", matches!(original_tool.state(), ToolInvocationState::Unknown { error }
        if error.external_effect() == ToolExternalEffect::Unknown && error.failure().retry_advice() == RetryAdvice::ReconcileFirst)));
    let failure = Box::pin(close_unknown_node_failure(store, &mut value)).await;
    let original = Box::pin(close_unknown_parent(
        store, pool, &value, &failure, None, checks,
    ))
    .await;
    let delivered = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert!(delivered.items().iter().all(|item| item.result().is_ok()));
    let requested = store.load_run(key.tenant_id(), child).await.unwrap();
    assert_eq!(
        requested.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    let cancellation = store.load_child_cancellation(&key).await.unwrap();
    checks.push((
        "B original parent failure queued and delivered child cancellation",
        cancellation.is_parent_failure_close()
            && cancellation.parent_head() == &original.registration().head()
            && cancellation.receipt().is_some(),
    ));
    Box::pin(close_unknown_unsettled(store, pool, &key, checks)).await;
    let recovered = Box::pin(close_unknown_cold(
        store,
        pool,
        &key,
        child,
        &original_tool,
        &child_fence,
        checks,
    ))
    .await;
    Box::pin(close_unknown_unsettled(store, pool, &key, checks)).await;
    Box::pin(close_unknown_reconcile(
        store,
        pool,
        &recovered,
        &original_tool,
        &tool,
        checks,
    ))
    .await;
    let child_ack = Box::pin(close_unknown_child(
        store,
        pool,
        &value,
        &recovered,
        original_tool.intent().invocation_id(),
        checks,
    ))
    .await;
    let settled = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert!(settled.items().iter().all(|item| item.result().is_ok()));
    Box::pin(close_unknown_finish(
        store, pool, &value, &original, &child_ack, checks,
    ))
    .await
}

#[tokio::test]
async fn failure_close_real_tool_unknown_preserves_parent_and_child_account_until_authorized_reconciliation()
 {
    use futures_util::FutureExt;

    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(mut store) = test_store().await else {
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let mut tracked = Vec::new();
    let mut checks = Vec::new();
    let verdict = std::panic::AssertUnwindSafe(async {
        let a = Box::pin(close_unknown_a(
            &mut store,
            &pool,
            &mut tracked,
            &mut checks,
        ))
        .await;
        let b = Box::pin(close_unknown_b(
            &mut store,
            &pool,
            &mut tracked,
            &mut checks,
        ))
        .await;
        [a, b]
    })
    .catch_unwind()
    .await;
    let mut released = true;
    for (tenant, run) in tracked {
        if let Ok(current) = store.load_run(&tenant, run).await {
            if let Some(lease) = current.lease() {
                released &= store.release_lease(lease.fence()).await.is_ok();
            }
        } else {
            released = false;
        }
    }
    pool.close().await;
    store.close().await;
    let observations = match verdict {
        Ok(rows) => rows,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert!(released, "all owned claims must retire before verdict");
    for (label, passed) in &checks {
        assert!(*passed, "{label}: {observations:?}");
    }
    eprintln!(
        "STATEKNOT_FAILURE_CLOSE_REAL_TOOL_UNKNOWN_EVIDENCE={}",
        json!({
            "profiles":observations, "all_checks":checks, "Store_Runtime_reconstructed":true,
            "OS_process_replacement":false, "original_lease_and_budgets":true
        })
    );
}
