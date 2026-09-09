// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_core::ChildRunJoinRequest;
use stateknot_runtime::{
    DurableAgentDeadlineReconciler, register_standard_agent_deadline_event_schema,
};
use stateknot_store_postgres::AgentDeadlineCancellationOutcome;

#[path = "agent_deadline_upgrade.rs"]
mod upgrade;

fn deadline_worker(store: &PostgresStore) -> DurableAgentDeadlineReconciler {
    let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    register_standard_agent_deadline_event_schema(&mut builder).unwrap();
    DurableAgentDeadlineReconciler::new(
        store.clone(),
        builder.build().unwrap(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
}
fn deadline_request(
    base: &DurableAgentAdmissionRequest,
    deadline: Timestamp,
) -> DurableAgentAdmissionRequest {
    let i = base.intent();
    let p = i.provenance();
    DurableAgentAdmissionRequest::new(
        p.tenant_id().clone(),
        AgentRunIds::new(
            p.run_id(),
            p.thread_id(),
            p.invocation_id(),
            base.admission_event_id(),
            base.initial_checkpoint_id(),
        ),
        i.descriptor().clone(),
        AgentRequest::new(
            i.request().input_schema().clone(),
            i.request().input().clone(),
            i.request().budget_limits().clone().with_deadline(deadline),
        ),
        i.budget_layers().to_vec(),
        i.graph().clone(),
        i.authority().clone(),
        base.initial_state().clone(),
    )
    .unwrap()
}
async fn future_deadline(store: &PostgresStore, seconds: i64) -> Timestamp {
    Timestamp::from_unix_micros(
        store.observe_database_clock().await.unwrap().unix_micros() + seconds * 1_000_000,
    )
    .unwrap()
}
async fn await_due(store: &PostgresStore, deadline: Timestamp) {
    let now = store.observe_database_clock().await.unwrap();
    let delay = deadline
        .unix_micros()
        .saturating_sub(now.unix_micros())
        .max(0);
    assert!(delay <= 30_000_000, "test deadline must be bounded");
    tokio::time::sleep(
        Duration::from_micros(u64::try_from(delay).unwrap()) + Duration::from_millis(5),
    )
    .await;
    assert!(store.observe_database_clock().await.unwrap() >= deadline);
}
async fn admit_deadline(
    store: &PostgresStore,
    fixture: &DriverFixture,
    tenant: TenantId,
    deadline: Timestamp,
) -> StoredAgentAdmission {
    let base = durable_admission_request(
        fixture,
        tenant,
        AgentRunIds::generate(),
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let request = deadline_request(&base, deadline);
    let p = request.intent().provenance();
    store
        .register_graph_definition(p.tenant_id().clone(), fixture.graph.clone())
        .await
        .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(
            p.tenant_id().clone(),
            p.run_id(),
            request.admission_event_id(),
            payload(AgentAdmission::JOURNAL_EVENT_KIND),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = CheckpointWrite::initial(
        p.tenant_id().clone(),
        p.run_id(),
        request.initial_checkpoint_id(),
        fixture.graph.reference(),
        request.initial_state().clone(),
        fixture.graph.entry_nodes().clone(),
    )
    .unwrap();
    Box::pin(store.admit_agent_run(
        request.intent().clone(),
        append,
        checkpoint,
        fixture.registry.schemas(),
    ))
    .await
    .unwrap()
    .stored()
    .clone()
}
async fn request_expiry(
    store: &PostgresStore,
    tenant: &TenantId,
    run: RunId,
) -> Result<AgentDeadlineCancellationOutcome, StoreError> {
    let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    let schema = register_standard_agent_deadline_event_schema(&mut builder).unwrap();
    store
        .request_agent_deadline_cancellation(
            tenant,
            run,
            EventId::generate(),
            FailureId::generate(),
            &schema,
            &builder.build().unwrap(),
        )
        .await
}
fn cleanup_loop(
    store: &PostgresStore,
    registry: ExecutableGraphRegistry,
    admission: &AgentAdmission,
    usage: BudgetUsage,
) -> DurableAgentLoop {
    let i = admission.intent();
    DurableAgentLoop::new(
        store.clone(),
        registry,
        Arc::new(StaticLifecycleEvidence {
            terminal: GraphTerminalEvidence::new(
                i.descriptor().clone(),
                i.request().clone(),
                i.budget().clone(),
                AgentArtifacts::empty(),
                usage,
            ),
            failure: None,
        }),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn deadline_uses_database_clock_converges_racers_and_waits_for_real_cleanup_evidence() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant = tenant("deadline-root");
    let deadline = future_deadline(&store, 2).await;
    let admission = admit_deadline(&store, &fixture, tenant.clone(), deadline).await;
    let run = admission.admission().intent().provenance().run_id();
    assert!(
        store
            .due_agent_deadlines_after(&tenant, None)
            .await
            .unwrap()
            .is_empty()
    );
    let before = request_expiry(&store, &tenant, run).await.unwrap();
    assert!(
        matches!(before,AgentDeadlineCancellationOutcome::NotDue { deadline:d, observed_at } if d==deadline && observed_at<d)
    );
    assert_eq!(
        store.load_run(&tenant, run).await.unwrap().journal_head(),
        admission.run().journal_head()
    );
    await_due(&store, deadline).await;
    let (first, second) = tokio::join!(
        request_expiry(&store, &tenant, run),
        request_expiry(&store, &tenant, run)
    );
    let results = [first.unwrap(), second.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, AgentDeadlineCancellationOutcome::Requested(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, AgentDeadlineCancellationOutcome::AlreadyRequested(_)))
            .count(),
        1
    );
    let requested = store.load_run(&tenant, run).await.unwrap();
    let reason = requested.lifecycle().cancellation_request().unwrap();
    assert_eq!(reason.failure().code().as_str(), "agent.deadline.expired");
    assert!(reason.requested_at() >= deadline);
    assert!(requested.lifecycle().terminal_usage().is_none());
    assert!(
        store
            .due_agent_deadlines_after(&tenant, None)
            .await
            .unwrap()
            .is_empty()
    );
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap();
    let unavailable = DurableAgentLoop::new(
        store.clone(),
        fixture.registry.clone(),
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(
        unavailable
            .run(lease.lease().fence().clone(), CancellationSignal::never())
            .await
            .is_err()
    );
    let still = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(still.lifecycle().status(), RunStatus::CancellationRequested);
    assert!(still.lifecycle().terminal_usage().is_none());
    assert!(still.lease().is_none());
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap();
    let done = cleanup_loop(
        &store,
        fixture.registry.clone(),
        admission.admission(),
        BudgetUsage::zero(),
    )
    .run(lease.lease().fence().clone(), CancellationSignal::never())
    .await
    .unwrap();
    assert!(matches!(
        done.outcome(),
        AgentLoopOutcome::CancellationConfirmed(_)
    ));
    let recovered = request_expiry(&store, &tenant, run).await.unwrap();
    assert!(
        matches!(recovered,AgentDeadlineCancellationOutcome::AlreadyRequested(r) if r.failure().id()==reason.failure().id())
    );
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn deadline_abandons_real_waits_atomically_without_firing_the_timer() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (fixture, timer, _) = wait_fixture();
    let tenant = tenant("deadline-wait");
    let deadline = future_deadline(&store, 2).await;
    let admission = admit_deadline(&store, &fixture, tenant.clone(), deadline).await;
    let run = admission.admission().intent().provenance().run_id();
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap();
    let loop_ = cleanup_loop(
        &store,
        fixture.registry.clone(),
        admission.admission(),
        BudgetUsage::zero(),
    );
    assert!(matches!(
        loop_
            .run(lease.lease().fence().clone(), CancellationSignal::never())
            .await
            .unwrap()
            .outcome(),
        AgentLoopOutcome::Waiting(_)
    ));
    let waiting = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(waiting.unresolved_wait_count(), 1);
    await_due(&store, deadline).await;
    let report = deadline_worker(&store)
        .tick(tenant.clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(report.items().len(), 1);
    assert!(matches!(
        report.items()[0].result().unwrap(),
        AgentDeadlineCancellationOutcome::Requested(_)
    ));
    let requested = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(requested.unresolved_wait_count(), 0);
    assert_eq!(
        requested.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(
        requested.checkpoint().unwrap(),
        waiting.checkpoint().unwrap()
    );
    assert!(matches!(
        store.load_durable_timer_record(&tenant, run, timer).await,
        Err(StoreError::WaitWasAbandoned)
    ));
    let abandonment = store
        .load_timer_abandonment(&tenant, run, timer)
        .await
        .unwrap();
    assert_eq!(
        abandonment.reason(),
        stateknot_store_postgres::WaitAbandonmentReason::RunCancellation
    );
    let pool = sql_pool().await;
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT count(*) FROM stateknot.timer_firings WHERE tenant_id=$1 AND run_id=$2"
        )
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT count(*) FROM stateknot.wait_abandonments WHERE tenant_id=$1 AND run_id=$2"
        )
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap();
    assert!(matches!(
        loop_
            .run(lease.lease().fence().clone(), CancellationSignal::never())
            .await
            .unwrap()
            .outcome(),
        AgentLoopOutcome::CancellationConfirmed(_)
    ));
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    pool.close().await;
    store.close().await;
}

async fn started_deadline(store: &PostgresStore, name: &str, deadline: Timestamp) -> Started {
    let child_driver = driver_fixture();
    let child = durable_admission_request(
        &child_driver,
        tenant(name),
        AgentRunIds::generate(),
        child_driver.graph.output_schema().clone(),
        child_driver.graph.input_schema().clone(),
    );
    let driver = declared_parent(&child_driver, child.intent().descriptor());
    let parent = admit_deadline(
        store,
        &driver,
        child.intent().provenance().tenant_id().clone(),
        deadline,
    )
    .await;
    let facade = DurableAgentAdmission::new(store.clone(), driver.registry.clone()).unwrap();
    let fixture = PreparationFixture {
        driver,
        child_driver,
        facade,
        parent,
    };
    let child = deadline_request(&fixture.child(), deadline);
    store
        .register_graph_definition(
            child.intent().provenance().tenant_id().clone(),
            fixture.child_driver.graph.clone(),
        )
        .await
        .unwrap();
    let intent = fixture
        .prepare(&child, store.observe_database_clock().await.unwrap())
        .unwrap();
    let key = intent.key();
    let fence = store
        .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let started = store
        .start_node_attempt(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                EventId::generate(),
                fixture.parent.event().head(),
                fence.clone(),
            ),
            key.parent().clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let NodeAttemptCommitOutcome::Committed { attempt, .. } = started else {
        panic!("fresh start")
    };
    Started {
        fixture,
        intent,
        node: attempt.start().head(),
        fence,
        head: attempt.start().journal_head().clone(),
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn deadline_suspended_join_queues_children_rollback_then_drains_before_parent_closure() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let deadline = future_deadline(&store, 3).await;
    let mut value = Box::pin(started_deadline(&store, "deadline-join", deadline)).await;
    let child = spawn(&store, &value).await.unwrap();
    let child_id = child
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    value.head = child.record().spawn().head();
    let key = value.intent.key().clone();
    let request = ChildRunJoinRequest::new([key.clone()]).unwrap();
    store
        .register_child_join(
            request,
            &value.node,
            parent_append(&value, "child-join-registered"),
        )
        .await
        .unwrap();
    await_due(&store, deadline).await;
    let pool = sql_pool().await;
    query("CREATE FUNCTION stateknot.test_deadline_queue_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test queue failure'; END $$").execute(&pool).await.unwrap();
    query("CREATE TRIGGER test_deadline_queue_failure BEFORE INSERT ON stateknot.child_run_cancellations FOR EACH ROW EXECUTE FUNCTION stateknot.test_deadline_queue_failure()").execute(&pool).await.unwrap();
    let before = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let failed = request_expiry(&store, key.tenant_id(), key.parent_run_id()).await;
    query("DROP TRIGGER test_deadline_queue_failure ON stateknot.child_run_cancellations")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_deadline_queue_failure()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(failed.is_err());
    let after = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(before.journal_head(), after.journal_head());
    assert_eq!(after.lifecycle().status(), RunStatus::Active);
    assert!(after.lease().is_none());
    assert!(matches!(
        request_expiry(&store, key.tenant_id(), key.parent_run_id())
            .await
            .unwrap(),
        AgentDeadlineCancellationOutcome::Requested(_)
    ));
    assert!(matches!(
        cancellation::confirm(
            &store,
            key.tenant_id(),
            key.parent_run_id(),
            BudgetUsage::zero()
        )
        .await,
        Err(StoreError::UnsettledChildRuns)
    ));
    assert!(matches!(
        store
            .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    let restart = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store.options().clone(),
    )
    .await
    .unwrap();
    let report = cancellation::reconciler(&restart)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(report.items().len(), 1);
    report.items()[0].result().unwrap();
    let child_run = store.load_run(key.tenant_id(), child_id).await.unwrap();
    assert_eq!(
        child_run.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    let child_reason = child_run
        .lifecycle()
        .cancellation_request()
        .unwrap()
        .failure()
        .id();
    assert!(
        matches!(request_expiry(&store,key.tenant_id(),child_id).await.unwrap(),AgentDeadlineCancellationOutcome::AlreadyRequested(r) if r.failure().id()==child_reason)
    );
    let child_usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(7))
        .build()
        .unwrap();
    cancellation::confirm(&store, key.tenant_id(), child_id, child_usage)
        .await
        .unwrap();
    assert!(matches!(
        store
            .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    let report = cancellation::reconciler(&restart)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    report.items()[0].result().unwrap();
    let lease = store
        .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
        .await
        .unwrap();
    let done = cleanup_loop(
        &store,
        value.fixture.driver.registry.clone(),
        value.fixture.parent.admission(),
        BudgetUsage::zero(),
    )
    .run(lease.lease().fence().clone(), CancellationSignal::never())
    .await
    .unwrap();
    assert!(matches!(
        done.outcome(),
        AgentLoopOutcome::CancellationConfirmed(_)
    ));
    assert_eq!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .lifecycle()
            .terminal_usage()
            .unwrap()
            .input_tokens()
            .get(),
        7
    );
    assert!(
        store
            .load_child_join(key.parent())
            .await
            .unwrap()
            .unwrap()
            .consumed()
            .is_none()
    );
    assert_eq!(value.fixture.driver.first_calls.load(Ordering::SeqCst), 0);
    pool.close().await;
    restart.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn deadline_sweep_pages_past_errors_resets_after_tail_and_restarts_without_losing_work() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant = tenant("deadline-pages");
    let deadline = future_deadline(&store, 8).await;
    let mut runs = Vec::new();
    for _ in 0..17 {
        runs.push(
            admit_deadline(&store, &fixture, tenant.clone(), deadline)
                .await
                .admission()
                .intent()
                .provenance()
                .run_id(),
        );
    }
    runs.sort();
    await_due(&store, deadline).await;
    let pool = sql_pool().await;
    query(&format!("CREATE FUNCTION stateknot.test_deadline_page_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id='{}'::uuid AND NEW.lifecycle_status='cancellation_requested' THEN RAISE EXCEPTION 'test first item failure'; END IF; RETURN NEW; END $$",runs[0])).execute(&pool).await.unwrap();
    query("CREATE TRIGGER test_deadline_page_failure BEFORE UPDATE ON stateknot.runs FOR EACH ROW EXECUTE FUNCTION stateknot.test_deadline_page_failure()").execute(&pool).await.unwrap();
    let worker = deadline_worker(&store);
    let first = worker
        .tick(tenant.clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    query("DROP TRIGGER test_deadline_page_failure ON stateknot.runs")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_deadline_page_failure()")
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(first.items().len(), 16);
    assert!(first.items()[0].result().is_err());
    for item in &first.items()[1..] {
        assert!(matches!(
            item.result().unwrap(),
            AgentDeadlineCancellationOutcome::Requested(_)
        ));
    }
    let other = super::tenant("deadline-other");
    assert!(matches!(
        worker
            .tick(
                other.clone(),
                Some(first.cursor().clone()),
                CancellationSignal::never()
            )
            .await,
        Err(StoreError::InvalidAgentDeadline)
    ));
    assert!(matches!(
        store
            .due_agent_deadlines_after(&other, Some(first.items()[0].candidate()))
            .await,
        Err(StoreError::InvalidAgentDeadline)
    ));
    let tail = deadline_worker(&store)
        .tick(
            tenant.clone(),
            Some(first.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(tail.items().len(), 1);
    assert_eq!(tail.items()[0].candidate().run_id(), runs[16]);
    let recovered = worker
        .tick(
            tenant.clone(),
            Some(tail.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(recovered.items().len(), 1);
    assert_eq!(recovered.items()[0].candidate().run_id(), runs[0]);
    recovered.items()[0].result().unwrap();
    assert!(
        deadline_worker(&store)
            .tick(tenant, None, CancellationSignal::never())
            .await
            .unwrap()
            .items()
            .is_empty()
    );
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn deadline_never_overwrites_terminal_or_operator_cancellation() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant = tenant("deadline-first-reason");
    let deadline = future_deadline(&store, 2).await;
    let failed = admit_deadline(&store, &fixture, tenant.clone(), deadline)
        .await
        .admission()
        .intent()
        .provenance()
        .run_id();
    let cancelled = admit_deadline(&store, &fixture, tenant.clone(), deadline)
        .await
        .admission()
        .intent()
        .provenance()
        .run_id();
    fail(&store, &tenant, failed, BudgetUsage::zero())
        .await
        .unwrap();
    cancellation::cancel_run(&store, &tenant, cancelled).await;
    let before = store.load_run(&tenant, cancelled).await.unwrap();
    await_due(&store, deadline).await;
    assert!(matches!(
        request_expiry(&store, &tenant, failed).await.unwrap(),
        AgentDeadlineCancellationOutcome::Terminal(RunStatus::Failed)
    ));
    assert!(
        matches!(request_expiry(&store,&tenant,cancelled).await.unwrap(),AgentDeadlineCancellationOutcome::AlreadyRequested(r) if r.failure().id()==before.lifecycle().cancellation_request().unwrap().failure().id())
    );
    assert!(
        store
            .due_agent_deadlines_after(&tenant, None)
            .await
            .unwrap()
            .is_empty()
    );
    store.close().await;
}

struct Shutdown;
impl stateknot_core::CancellationObserver for Shutdown {
    fn is_cancelled(&self) -> bool {
        true
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[tokio::test]
async fn deadline_shutdown_preserves_work_and_quarantine_is_visible_without_blocking_other_runs() {
    use stateknot_store_postgres::{
        RunQuarantineCause, RunQuarantineComponent, RunQuarantineRequest,
    };
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant = tenant("deadline-quarantine");
    let deadline = future_deadline(&store, 2).await;
    let quarantined = admit_deadline(&store, &fixture, tenant.clone(), deadline).await;
    let healthy = admit_deadline(&store, &fixture, tenant.clone(), deadline).await;
    let run = quarantined.admission().intent().provenance().run_id();
    store
        .quarantine_run(
            RunQuarantineRequest::new(
                tenant.clone(),
                run,
                QuarantineId::generate(),
                JournalExpectation::exact(quarantined.event().head()),
                RunQuarantineCause::OperatorPolicy,
                RunQuarantineComponent::new("deadline-test").unwrap(),
                Digest::sha256("operator evidence"),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    await_due(&store, deadline).await;
    let worker = deadline_worker(&store);
    let stopped = worker
        .tick(tenant.clone(), None, CancellationSignal::new(Shutdown))
        .await
        .unwrap();
    assert!(stopped.is_cancelled());
    assert!(stopped.items().is_empty());
    assert_eq!(
        store
            .load_run(&tenant, healthy.admission().intent().provenance().run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Active
    );
    let report = worker
        .tick(
            tenant.clone(),
            Some(stopped.cursor().clone()),
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(report.items().len(), 2);
    assert!(matches!(
        report
            .items()
            .iter()
            .find(|i| i.candidate().run_id() == run)
            .unwrap()
            .result(),
        Err(StoreError::RunQuarantined)
    ));
    assert!(matches!(
        report
            .items()
            .iter()
            .find(|i| i.candidate().run_id() != run)
            .unwrap()
            .result()
            .unwrap(),
        AgentDeadlineCancellationOutcome::Requested(_)
    ));
    assert_eq!(
        store.load_run(&tenant, run).await.unwrap().journal_head(),
        quarantined.run().journal_head()
    );
    store.close().await;
}

#[tokio::test]
async fn deadline_observes_expiry_after_waiting_for_the_lifecycle_lock() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant = tenant("deadline-lock-clock");
    let deadline = future_deadline(&store, 2).await;
    let admission = admit_deadline(&store, &fixture, tenant.clone(), deadline).await;
    let run = admission.admission().intent().provenance().run_id();
    let pool = sql_pool().await;
    let mut tx = pool.begin().await.unwrap();
    query("SELECT run_id FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2 FOR UPDATE")
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let other = store.clone();
    let scope = tenant.clone();
    let task = tokio::spawn(async move { request_expiry(&other, &scope, run).await });
    // Separate observer connection: the fixture pool's only connection owns the lock.
    let observer = sql_pool().await;
    tokio::time::timeout(Duration::from_secs(1),async {
        loop {
            let waiting=query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%stateknot.runs%' AND query LIKE '%FOR UPDATE%')")
                .fetch_one(&observer).await.unwrap();
            if waiting { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert!(store.observe_database_clock().await.unwrap() < deadline);
    await_due(&store, deadline).await;
    tx.commit().await.unwrap();
    assert!(matches!(
        task.await.unwrap().unwrap(),
        AgentDeadlineCancellationOutcome::Requested(_)
    ));
    assert!(
        store
            .load_run(&tenant, run)
            .await
            .unwrap()
            .lifecycle()
            .cancellation_request()
            .unwrap()
            .requested_at()
            >= deadline
    );
    observer.close().await;
    pool.close().await;
    store.close().await;
}
