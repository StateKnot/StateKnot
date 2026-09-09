// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_runtime::{
    ChildReconciliationCommit, ChildReconciliationKind, DurableChildReconciler,
    DurableChildReconcilerOptions, register_standard_child_reconciliation_event_schema,
};
use stateknot_store_postgres::{ChildCancellationDelivery, ChildCancellationOutcome};

#[path = "child_cancellation_upgrade.rs"]
mod upgrade;

pub(super) fn reconciler(store: &PostgresStore) -> DurableChildReconciler {
    let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    register_standard_child_reconciliation_event_schema(&mut builder).unwrap();
    DurableChildReconciler::new(
        store.clone(),
        builder.build().unwrap(),
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
}

fn cancellation_request(at: Timestamp, event: EventId) -> RunCancellationRequest {
    RunCancellationRequest::new(
        Failure::new(
            FailureId::generate(),
            FailureCategory::Cancelled,
            FailureCode::new("test.child.cancel").unwrap(),
            FailureOrigin::new("test.child").unwrap(),
            FailureMessage::new("Parent-specific diagnostic.").unwrap(),
            RetryAdvice::Never,
        )
        .unwrap()
        .with_caused_by_event(event),
        at,
    )
    .unwrap()
}

pub(super) async fn cancel_run(
    store: &PostgresStore,
    tenant: &TenantId,
    run: RunId,
) -> JournalHead {
    let current = store.load_run(tenant, run).await.unwrap();
    let event = EventId::generate();
    let request = cancellation_request(store.observe_database_clock().await.unwrap(), event);
    let append = JournalAppend::new(
        JournalExpectation::exact(current.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            tenant.clone(),
            run,
            event,
            payload("test-parent-cancelled"),
        )
        .unwrap(),
    )
    .unwrap();
    let transition = RunTransition::RequestCancellation { request };
    store
        .append_control_plane(
            append,
            RunProjection::transition(current.lifecycle().revision(), transition),
        )
        .await
        .unwrap()
        .event()
        .head()
}

async fn delivery(
    store: &PostgresStore,
    key: &ChildRunKey,
) -> (JournalAppend, RunCancellationRequest) {
    let child = store.load_child_run(key).await.unwrap();
    let run = child.child().run();
    let event = EventId::generate();
    let request = cancellation_request(store.observe_database_clock().await.unwrap(), event);
    let append = JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            key.tenant_id().clone(),
            run.lifecycle().provenance().run_id(),
            event,
            payload("child-run-cancellation-requested"),
        )
        .unwrap(),
    )
    .unwrap();
    (append, request)
}

pub(super) async fn confirm(
    store: &PostgresStore,
    tenant: &TenantId,
    run: RunId,
    usage: BudgetUsage,
) -> Result<(), StoreError> {
    let current = store.load_run(tenant, run).await?;
    let append = JournalAppend::new(
        JournalExpectation::exact(current.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            tenant.clone(),
            run,
            EventId::generate(),
            payload("test-cancel-confirmed"),
        )
        .unwrap(),
    )
    .unwrap();
    store
        .append_control_plane(
            append,
            RunProjection::transition(
                current.lifecycle().revision(),
                RunTransition::ConfirmCancellation {
                    completed_at: store.observe_database_clock().await?,
                    usage,
                },
            ),
        )
        .await?;
    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cancellation_delivery_is_atomic_race_safe_and_never_fabricates_terminal_usage() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = started(&store, "cancel-owned-race").await;
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
    let source = cancel_run(&store, key.tenant_id(), key.parent_run_id()).await;
    let first = store.load_child_cancellation(&key).await.unwrap();
    assert_eq!(first.parent_head(), &source);
    assert!(first.receipt().is_none());
    assert_eq!(
        store
            .pending_child_cancellations_after(key.tenant_id(), None)
            .await
            .unwrap(),
        vec![key.clone()]
    );
    assert!(matches!(
        confirm(
            &store,
            key.tenant_id(),
            key.parent_run_id(),
            BudgetUsage::zero()
        )
        .await,
        Err(StoreError::UnsettledChildRuns)
    ));
    let pool = sqlx_postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    query("CREATE FUNCTION stateknot.test_cancel_receipt_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test receipt failure'; END $$").execute(&pool).await.unwrap();
    query("CREATE TRIGGER test_cancel_receipt_failure BEFORE INSERT ON stateknot.child_run_cancellation_receipts FOR EACH ROW EXECUTE FUNCTION stateknot.test_cancel_receipt_failure()").execute(&pool).await.unwrap();
    let (append, request) = delivery(&store, &key).await;
    assert!(
        Box::pin(store.deliver_child_cancellation(&key, append, request))
            .await
            .is_err()
    );
    query("DROP TRIGGER test_cancel_receipt_failure ON stateknot.child_run_cancellation_receipts")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_cancel_receipt_failure()")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        store
            .load_run(key.tenant_id(), child)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Active
    );
    assert!(
        store
            .load_child_cancellation(&key)
            .await
            .unwrap()
            .receipt()
            .is_none()
    );
    let mut tasks = Vec::new();
    for _ in 0..24 {
        let (append, request) = delivery(&store, &key).await;
        let store = store.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            Box::pin(store.deliver_child_cancellation(&key, append, request))
                .await
                .unwrap()
        }));
    }
    let mut commits = 0;
    let mut heads = Vec::new();
    for task in tasks {
        let outcome = task.await.unwrap();
        if matches!(outcome, ChildCancellationDelivery::Committed(_)) {
            commits += 1;
        }
        heads.push(outcome.record().receipt().unwrap().head().clone());
    }
    assert_eq!(commits, 1);
    assert!(heads.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(
        store
            .load_run(key.tenant_id(), child)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::CancellationRequested
    );
    assert!(
        store
            .pending_child_cancellations_after(key.tenant_id(), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .pending_child_cancellations_after(key.tenant_id(), Some(&key))
            .await
            .unwrap()
            .is_empty()
    );
    let usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(7))
        .build()
        .unwrap();
    confirm(&store, key.tenant_id(), child, usage.clone())
        .await
        .unwrap();
    // A worker recreated after delivery recovers the immutable cancellation receipt.
    let loaded = store.load_child_cancellation(&key).await.unwrap();
    assert_eq!(loaded.receipt().unwrap().head(), &heads[0]);
    assert_eq!(
        loaded.receipt().unwrap().lifecycle().status(),
        RunStatus::CancellationRequested
    );
    let tick = reconciler(&store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(tick.items().len(), 1);
    assert_eq!(tick.items()[0].kind(), ChildReconciliationKind::Settlement);
    assert_eq!(
        tick.items()[0].result().unwrap(),
        ChildReconciliationCommit::Settlement
    );
    confirm(&store, key.tenant_id(), key.parent_run_id(), usage)
        .await
        .unwrap();
    assert_eq!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Cancelled
    );
    assert!(
        reconciler(&store)
            .tick(
                key.tenant_id().clone(),
                Some(tick.cursor().clone()),
                CancellationSignal::never()
            )
            .await
            .unwrap()
            .items()
            .is_empty()
    );
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn existing_child_request_and_terminal_outcome_are_not_replaced() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for terminal in [false, true] {
        let value = started(&store, "cancel-existing-outcome").await;
        let key = value.intent.key();
        let child = spawn(&store, &value)
            .await
            .unwrap()
            .record()
            .child()
            .admission()
            .intent()
            .provenance()
            .run_id();
        if terminal {
            fail(&store, key.tenant_id(), child, BudgetUsage::zero())
                .await
                .unwrap();
        } else {
            cancel_run(&store, key.tenant_id(), child).await;
        }
        let before = store.load_run(key.tenant_id(), child).await.unwrap();
        cancel_run(&store, key.tenant_id(), key.parent_run_id()).await;
        let tick = reconciler(&store)
            .tick(key.tenant_id().clone(), None, CancellationSignal::never())
            .await
            .unwrap();
        assert_eq!(
            tick.items()[0].result().unwrap(),
            ChildReconciliationCommit::Cancellation(if terminal {
                ChildCancellationOutcome::Terminal
            } else {
                ChildCancellationOutcome::AlreadyRequested
            })
        );
        let after = store.load_run(key.tenant_id(), child).await.unwrap();
        assert_eq!(before.journal_head(), after.journal_head());
        assert_eq!(
            serde_json::to_value(before.lifecycle()).unwrap(),
            serde_json::to_value(after.lifecycle()).unwrap()
        );
        assert!(
            store
                .load_child_cancellation(key)
                .await
                .unwrap()
                .receipt()
                .is_some()
        );
    }
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn cancellation_atomically_abandons_real_child_waits_and_makes_cleanup_schedulable() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("cancel-waiting-child");
    let (child_fixture, timer, _) = wait_fixture();
    let request = durable_admission_request(
        &child_fixture,
        tenant.clone(),
        AgentRunIds::generate(),
        child_fixture.graph.output_schema().clone(),
        child_fixture.graph.input_schema().clone(),
    );
    let parent_fixture = declared_parent(&child_fixture, request.intent().descriptor());
    let parent = Box::pin(tree::root_admission(
        &store,
        &parent_fixture,
        tenant.clone(),
    ))
    .await;
    let child = Box::pin(tree::spawn_below(&store, &parent, &child_fixture))
        .await
        .unwrap();
    let child_id = child.child().admission().intent().provenance().run_id();
    let lease = store
        .claim_lease(&tenant, child_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        child_fixture.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let report = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = report.into_parts().0 else {
        panic!("wait handoff expected");
    };
    DurableGraphLifecycle::new(
        store.clone(),
        child_fixture.registry,
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
    .commit_barrier(*handoff)
    .await
    .unwrap();
    assert_eq!(
        store
            .load_run(&tenant, child_id)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Waiting
    );
    cancel_run(
        &store,
        &tenant,
        parent.admission().intent().provenance().run_id(),
    )
    .await;
    let tick = reconciler(&store)
        .tick(tenant.clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(
        tick.items()[0].result().unwrap(),
        ChildReconciliationCommit::Cancellation(ChildCancellationOutcome::Requested)
    );
    let child_run = store.load_run(&tenant, child_id).await.unwrap();
    assert_eq!(
        child_run.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert!(child_run.lifecycle().waits().is_none());
    assert_eq!(
        store
            .load_timer_abandonment(&tenant, child_id, timer)
            .await
            .unwrap()
            .reason(),
        stateknot_store_postgres::WaitAbandonmentReason::RunCancellation
    );
    assert!(
        store
            .load_child_cancellation(child.intent().key())
            .await
            .unwrap()
            .receipt()
            .is_some()
    );
    assert!(
        store
            .claim_lease(&tenant, child_id, AttemptId::generate())
            .await
            .is_ok()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn nested_delivery_queues_one_level_per_commit_and_settles_leaf_to_root_once() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("cancel-nested-tree");
    let leaf = driver_fixture();
    let leaf_request = durable_admission_request(
        &leaf,
        tenant.clone(),
        AgentRunIds::generate(),
        leaf.graph.output_schema().clone(),
        leaf.graph.input_schema().clone(),
    );
    let middle = declared_parent(&leaf, leaf_request.intent().descriptor());
    let middle_request = durable_admission_request(
        &middle,
        tenant.clone(),
        AgentRunIds::generate(),
        middle.graph.output_schema().clone(),
        middle.graph.input_schema().clone(),
    );
    let root = DriverFixture {
        graph: tree::parent_graph(
            &middle.graph,
            middle_request.intent().descriptor(),
            "cancel-tree-root",
            ChildRunTopologyLimits::new(2, 8, 4).unwrap(),
        ),
        registry: middle.registry.clone(),
        first_calls: Arc::new(AtomicUsize::new(0)),
        second_calls: Arc::new(AtomicUsize::new(0)),
    };
    let root = Box::pin(tree::root_admission(&store, &root, tenant.clone())).await;
    let owned_middle = Box::pin(tree::spawn_below(&store, &root, &middle))
        .await
        .unwrap();
    let owned_leaf = Box::pin(tree::spawn_below(&store, owned_middle.child(), &leaf))
        .await
        .unwrap();
    let root_id = root.admission().intent().provenance().run_id();
    let middle_id = owned_middle
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    let leaf_id = owned_leaf
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    cancel_run(&store, &tenant, root_id).await;
    assert_eq!(
        store
            .pending_child_cancellations_after(&tenant, None)
            .await
            .unwrap(),
        vec![owned_middle.intent().key().clone()]
    );
    let first = reconciler(&store)
        .tick(tenant.clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(first.items().len(), 1);
    assert!(first.items()[0].result().is_ok());
    assert_eq!(
        store
            .load_run(&tenant, leaf_id)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Active
    );
    let second = reconciler(&store)
        .tick(
            tenant.clone(),
            Some(first.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(second.items().len(), 1);
    assert!(second.items()[0].result().is_ok());
    assert_eq!(
        store
            .load_run(&tenant, leaf_id)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::CancellationRequested
    );
    assert!(matches!(
        confirm(&store, &tenant, middle_id, BudgetUsage::zero()).await,
        Err(StoreError::UnsettledChildRuns)
    ));
    let leaf_usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(3))
        .build()
        .unwrap();
    confirm(&store, &tenant, leaf_id, leaf_usage).await.unwrap();
    let third = reconciler(&store)
        .tick(
            tenant.clone(),
            Some(second.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(
        third.items()[0].result().unwrap(),
        ChildReconciliationCommit::Settlement
    );
    let total = store
        .include_child_usage(
            &tenant,
            middle_id,
            BudgetUsage::builder()
                .input_tokens(TokenCount::new(2))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    confirm(&store, &tenant, middle_id, total.clone())
        .await
        .unwrap();
    let fourth = reconciler(&store)
        .tick(
            tenant.clone(),
            Some(third.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(
        fourth.items()[0].result().unwrap(),
        ChildReconciliationCommit::Settlement
    );
    confirm(&store, &tenant, root_id, total).await.unwrap();
    assert_eq!(
        store
            .load_run(&tenant, root_id)
            .await
            .unwrap()
            .lifecycle()
            .terminal_usage()
            .unwrap()
            .input_tokens()
            .get(),
        5
    );
    store
        .load_child_cancellation(owned_leaf.intent().key())
        .await
        .unwrap();
    store
        .load_child_cancellation(owned_middle.intent().key())
        .await
        .unwrap();
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn draining_parent_cannot_reclaim_capacity_until_last_child_settles() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = started(&store, "cancel-drain-claims").await;
    let key = value.intent.key();
    let child = spawn(&store, &value)
        .await
        .unwrap()
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    cancel_run(&store, key.tenant_id(), key.parent_run_id()).await;
    let previous = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let expiry = Timestamp::from_unix_micros(
        previous.lease().unwrap().expires_at().unix_micros() + 30_000_000,
    )
    .unwrap();
    store.renew_lease(&value.fence, expiry).await.unwrap();
    assert!(
        store
            .claim_lease(
                key.tenant_id(),
                key.parent_run_id(),
                value.fence.attempt_id()
            )
            .await
            .is_ok()
    );
    assert!(matches!(
        store
            .supersede_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    let pool = sql_pool().await;
    let mut tx = pool.begin().await.unwrap();
    query("SET LOCAL stateknot.child_runtime_version = '1'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let error =
        query("UPDATE stateknot.runs SET lease_attempt_id=$3 WHERE tenant_id=$1 AND run_id=$2")
            .bind(key.tenant_id().as_str())
            .bind(*key.parent_run_id().as_uuid())
            .bind(*AttemptId::generate().as_uuid())
            .execute(&mut *tx)
            .await
            .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("SKC06")
    );
    tx.rollback().await.unwrap();
    pool.close().await;
    store.release_lease(&value.fence).await.unwrap();
    for terminal in [false, true] {
        if terminal {
            fail(&store, key.tenant_id(), child, BudgetUsage::zero())
                .await
                .unwrap();
        }
        assert!(matches!(
            store
                .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
                .await,
            Err(StoreError::RunNotRunnable)
        ));
        let page = store
            .load_runnable_run_page(
                key.tenant_id(),
                None,
                stateknot_store_postgres::RunnableRunPageSize::new(16).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            page.records()
                .iter()
                .all(|row| row.run().lifecycle().provenance().run_id() != key.parent_run_id())
        );
    }
    let tick = reconciler(&store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(tick.items().len(), 2);
    assert!(tick.items().iter().all(|item| item.result().is_ok()));
    let page = store
        .load_runnable_run_page(
            key.tenant_id(),
            None,
            stateknot_store_postgres::RunnableRunPageSize::new(16).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.records().len(), 1);
    assert_eq!(
        page.records()[0].run().lifecycle().provenance().run_id(),
        key.parent_run_id()
    );
    let lease = store
        .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
        .await
        .unwrap();
    assert_eq!(
        lease.lease().fence().epoch().get(),
        value.fence.epoch().get() + 1
    );
    confirm(
        &store,
        key.tenant_id(),
        key.parent_run_id(),
        BudgetUsage::zero(),
    )
    .await
    .unwrap();
    store.close().await;
}

#[tokio::test]
async fn racing_parent_cancel_and_spawn_never_leaves_an_unowned_or_unqueued_child() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for _ in 0..8 {
        let value = started(&store, "cancel-spawn-race").await;
        let key = value.intent.key();
        let current = store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap();
        let event = EventId::generate();
        let request = cancellation_request(store.observe_database_clock().await.unwrap(), event);
        let append = JournalAppend::new(
            JournalExpectation::exact(current.journal_head().unwrap().clone()),
            JournalEventIntent::control_plane(
                key.tenant_id().clone(),
                key.parent_run_id(),
                event,
                payload("test-parent-cancelled"),
            )
            .unwrap(),
        )
        .unwrap();
        let (spawned, cancelled) = tokio::join!(
            spawn(&store, &value),
            store.append_control_plane(
                append,
                RunProjection::transition(
                    current.lifecycle().revision(),
                    RunTransition::RequestCancellation { request }
                )
            )
        );
        if let Err(error) = cancelled {
            assert!(matches!(error, StoreError::StaleJournalHead), "{error:?}");
            assert!(spawned.is_ok());
            cancel_run(&store, key.tenant_id(), key.parent_run_id()).await;
        }
        match spawned {
            Ok(_) => {
                let pending = store
                    .pending_child_cancellations_after(key.tenant_id(), None)
                    .await
                    .unwrap();
                assert_eq!(pending, vec![key.clone()]);
                assert!(
                    store
                        .load_child_cancellation(key)
                        .await
                        .unwrap()
                        .receipt()
                        .is_none()
                );
            }
            Err(error) => {
                assert!(
                    matches!(
                        error,
                        StoreError::ChildRunRejected
                            | StoreError::StaleJournalHead
                            | StoreError::RunNotRunnable
                    ),
                    "{error:?}"
                );
                assert!(
                    store
                        .list_child_run_identities(key.tenant_id(), key.parent_run_id())
                        .await
                        .unwrap()
                        .is_empty()
                );
                assert!(matches!(
                    store
                        .load_run(key.tenant_id(), value.intent.child().provenance().run_id())
                        .await,
                    Err(StoreError::RunNotFound)
                ));
                assert!(
                    store
                        .pending_child_cancellations_after(key.tenant_id(), None)
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
        }
    }
    store.close().await;
}

struct AlreadyShutDown;
impl stateknot_core::CancellationObserver for AlreadyShutDown {
    fn is_cancelled(&self) -> bool {
        true
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn bounded_cursor_passes_unpriced_prefix_and_survives_recreation_and_shutdown() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("cancel-bounded-pages");
    let leaf = driver_fixture();
    let request = durable_admission_request(
        &leaf,
        tenant.clone(),
        AgentRunIds::generate(),
        leaf.graph.output_schema().clone(),
        leaf.graph.input_schema().clone(),
    );
    let parent = declared_parent(&leaf, request.intent().descriptor());
    let mut keys = Vec::new();
    for index in 0..18 {
        let root = Box::pin(tree::root_admission(&store, &parent, tenant.clone())).await;
        let owned = Box::pin(tree::spawn_below(&store, &root, &leaf))
            .await
            .unwrap();
        let usage = if index < 16 {
            BudgetUsage::builder()
                .unpriced_cost_events(ExecutionCount::new(1))
                .build()
                .unwrap()
        } else {
            BudgetUsage::zero()
        };
        fail(
            &store,
            &tenant,
            owned.child().admission().intent().provenance().run_id(),
            usage,
        )
        .await
        .unwrap();
        cancel_run(
            &store,
            &tenant,
            root.admission().intent().provenance().run_id(),
        )
        .await;
        keys.push(owned.intent().key().clone());
    }
    let stopped = reconciler(&store)
        .tick(
            tenant.clone(),
            None,
            CancellationSignal::new(AlreadyShutDown),
        )
        .await
        .unwrap();
    assert!(stopped.is_cancelled());
    assert!(stopped.items().is_empty());
    assert!(
        store
            .load_child_cancellation(&keys[0])
            .await
            .unwrap()
            .receipt()
            .is_none()
    );
    let first = reconciler(&store)
        .tick(
            tenant.clone(),
            Some(stopped.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(first.items().len(), 32);
    assert_eq!(
        first
            .items()
            .iter()
            .filter(|item| item.result().is_err())
            .count(),
        16
    );
    assert!(first.items()[..16].iter().all(|item| matches!(
        item.result(),
        Ok(ChildReconciliationCommit::Cancellation(
            ChildCancellationOutcome::Terminal
        ))
    )));
    assert!(matches!(
        reconciler(&store)
            .tick(
                TenantId::new("foreign-cancel-cursor").unwrap(),
                Some(first.cursor().clone()),
                CancellationSignal::never()
            )
            .await,
        Err(StoreError::ChildRunRejected)
    ));
    let second = reconciler(&store)
        .tick(
            tenant.clone(),
            Some(first.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(second.items().len(), 4);
    assert!(second.items().iter().all(|item| item.result().is_ok()));
    for key in &keys[16..] {
        assert!(
            store
                .load_child_run(key)
                .await
                .unwrap()
                .settlement()
                .is_some()
        );
    }
    let third = reconciler(&store)
        .tick(
            tenant.clone(),
            Some(second.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(third.items().len(), 16);
    assert!(
        third.items().iter().all(
            |item| item.kind() == ChildReconciliationKind::Settlement && item.result().is_err()
        )
    );
    assert!(
        store
            .pending_child_cancellations_after(&tenant, None)
            .await
            .unwrap()
            .is_empty()
    );
    store.close().await;
}
