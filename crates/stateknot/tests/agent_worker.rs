// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real database qualification of the concrete owned execution role.
#![allow(clippy::wildcard_imports, clippy::too_many_lines)]

use serde_json::{Value, json};
use stateknot::{agent_http::*, agent_worker::*, core::*, postgres::*, runtime::*};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::Notify,
    time::{sleep, timeout},
};

#[path = "agent_http/fixture.rs"]
#[allow(dead_code)] // Shares the real admission/registry fixture, not HTTP test helpers.
mod fixture;
use fixture::Fixture;

use stateknot::agent_maintenance::{
    AgentMaintenance, AgentMaintenanceBinding, AgentMaintenanceJob,
    AgentMaintenanceMutationOptions, AgentMaintenanceOptions, AgentMaintenanceReadiness,
    AgentMaintenanceReadinessError,
};

impl AgentMaintenanceReadiness for Host {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentMaintenanceReadinessError>> {
        Box::pin(async {
            AgentWorkerReadiness::check(self)
                .await
                .map_err(|_| AgentMaintenanceReadinessError)
        })
    }
}

#[tokio::test]
async fn postgres_worker_and_maintenance_have_independent_joined_lifecycles() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    register_standard_agent_deadline_event_schema(&mut schemas).unwrap();
    register_standard_child_reconciliation_event_schema(&mut schemas).unwrap();
    register_standard_child_join_event_schema(&mut schemas).unwrap();
    register_standard_run_failure_close_event_schema(&mut schemas).unwrap();
    let maintenance = AgentMaintenanceBinding::new(
        f.store.clone(),
        schemas.build().unwrap(),
        vec![f.caller.tenant_id().clone()],
        AgentMaintenanceMutationOptions::default(),
    )
    .unwrap();
    let mut role = AgentMaintenance::start(
        maintenance,
        Arc::new(Host::default()),
        AgentMaintenanceOptions::default()
            .with_pacing(Duration::from_millis(10), Duration::from_millis(20))
            .unwrap(),
    )
    .await
    .unwrap();
    let mut worker = AgentWorker::start(
        binding(&f, evidence(&f)),
        Arc::new(Host::default()),
        options(),
    )
    .await
    .unwrap();
    let first = admit(&f).await;
    succeeded(&f, first).await;
    until(|| {
        AgentMaintenanceJob::ALL
            .iter()
            .all(|job| role.health().report().job(*job).completed_ticks > 0)
    })
    .await;
    assert_eq!(role.shutdown().await.unwrap().failure, None);
    let second = admit(&f).await;
    succeeded(&f, second).await;
    assert_ne!(first, second);
    assert_eq!(worker.shutdown().await.unwrap().failure, None);
    assert_eq!(worker.health().active_ticks(), 0);
    assert_eq!(role.health().active_ticks(), 0);
    assert!(f.store.observe_database_clock().await.is_ok());
    f.store.close().await;
    println!(
        "\nSTATEKNOT_WORKER_MAINTENANCE_EVIDENCE={{\"concurrent_roles\":true,\"independent_shutdown\":true,\"execution_after_maintenance_stop\":true}}"
    );
}

#[derive(Default)]
struct Host {
    mode: AtomicUsize,
    calls: AtomicUsize,
}
impl AgentWorkerReadiness for Host {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentWorkerReadinessError>> {
        Box::pin(async {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode.load(Ordering::SeqCst) {
                0 => Ok(()),
                1 => Err(AgentWorkerReadinessError),
                2 => std::future::pending().await,
                _ => panic!("fixture host readiness panic"),
            }
        })
    }
}

// Fixture-specific accounting: recover the one mock model node's actual durable
// completion. Never invent a model turn to satisfy AgentResult validation.
struct Evidence {
    store: PostgresStore,
    mode: AtomicUsize,
    entered: AtomicUsize,
}
impl GraphLifecycleEvidenceProvider for Evidence {
    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::SeqCst);
            match self.mode.load(Ordering::SeqCst) {
                1 => std::future::pending::<()>().await,
                2 => panic!("fixture lifecycle evidence panic"),
                3 => return Err(GraphLifecycleEvidenceError::TemporarilyUnavailable),
                _ => {}
            }
            let stored = self
                .store
                .load_agent_admission(
                    context.provenance().tenant_id(),
                    context.provenance().run_id(),
                )
                .await
                .unwrap();
            let intent = stored.admission().intent();
            assert_eq!(&stored.checkpoint().head(), context.checkpoint());
            let activation =
                NodeActivation::for_ready_root(stored.checkpoint(), NodeId::new("finish").unwrap())
                    .unwrap();
            let history = self
                .store
                .load_node_attempt_history_page(
                    &activation,
                    None,
                    NodeAttemptHistoryPageSize::new(2).unwrap(),
                )
                .await
                .unwrap();
            assert!(!history.has_more());
            assert_eq!(history.records().len(), 1);
            let completion = history.records()[0].completion().unwrap();
            assert_eq!(completion.status(), NodeAttemptStatus::Succeeded);
            Ok(GraphTerminalEvidence::new(
                intent.descriptor().clone(),
                intent.request().clone(),
                intent.budget().clone(),
                AgentArtifacts::empty(),
                completion.usage().clone(),
            ))
        })
    }
    fn failure_evidence(
        &self,
        _: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Err(GraphLifecycleEvidenceError::Unavailable) })
    }
    fn cancellation_evidence(
        &self,
        _: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Ok(GraphCancellationEvidence::new(BudgetUsage::zero())) })
    }
}
fn evidence(f: &Fixture) -> Arc<Evidence> {
    Arc::new(Evidence {
        store: f.store.clone(),
        mode: AtomicUsize::new(0),
        entered: AtomicUsize::new(0),
    })
}
fn options() -> AgentWorkerOptions {
    AgentWorkerOptions::default()
        .with_execution_limits(2, Duration::from_secs(20), Duration::from_secs(2))
        .unwrap()
        .with_pacing(
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(100),
        )
        .unwrap()
        .with_readiness_limits(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(10),
        )
        .unwrap()
}
fn binding(f: &Fixture, evidence: Arc<Evidence>) -> AgentWorkerBinding {
    AgentWorkerBinding::tenant(
        f.store.clone(),
        f.executable.clone(),
        evidence,
        f.caller.tenant_id().clone(),
        AgentWorkerExecutionOptions::default(),
    )
    .unwrap()
}
async fn short_lease_binding(f: &Fixture, evidence: Arc<Evidence>) -> AgentWorkerBinding {
    let store = PostgresStore::connect(
        &std::env::var("STATEKNOT_TEST_DATABASE_URL").unwrap(),
        PostgresStoreOptions::default()
            .with_transport_security(PostgresTransportSecurity::Disabled)
            .with_lease_timing(Duration::from_secs(3), Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let execution = AgentWorkerExecutionOptions {
        driver: DurableGraphDriverOptions::new(
            GraphReplayLimits::default(),
            128,
            Duration::from_millis(500),
            Duration::from_secs(30),
            3,
            Duration::from_millis(25),
        )
        .unwrap(),
        ..AgentWorkerExecutionOptions::default()
    };
    AgentWorkerBinding::tenant(
        store,
        f.executable.clone(),
        evidence,
        f.caller.tenant_id().clone(),
        execution,
    )
    .unwrap()
}
async fn until(mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(10), async {
        while !predicate() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
async fn admit(f: &Fixture) -> RunId {
    let input = f.submission();
    f.service
        .submit(
            f.caller.clone(),
            &input.submission_key,
            &input.agent,
            input.request,
        )
        .await
        .unwrap()
        .snapshot()
        .provenance()
        .run_id()
}
async fn succeeded(f: &Fixture, run: RunId) {
    timeout(Duration::from_secs(12), async {
        loop {
            let snapshot = f.service.load(f.caller.clone(), run).await.unwrap();
            if snapshot.status() == RunStatus::Succeeded {
                break;
            }
            assert_eq!(snapshot.status(), RunStatus::Active);
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn postgres_worker_actual_store_failure_stops_role_and_rejects_startup() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let host = Arc::new(Host::default());
    let limits = options()
        .with_pacing(
            Duration::from_millis(200),
            Duration::from_millis(200),
            Duration::from_millis(200),
        )
        .unwrap()
        .with_readiness_limits(
            Duration::from_secs(60),
            Duration::from_secs(1),
            Duration::from_secs(120),
        )
        .unwrap();
    let mut worker = AgentWorker::start(binding(&f, evidence(&f)), host.clone(), limits)
        .await
        .unwrap();
    let health = worker.health();
    until(|| health.report().completed_ticks > 0 && health.active_ticks() == 0).await;
    f.store.close().await;
    let report = timeout(Duration::from_secs(5), worker.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.failure, Some(AgentWorkerFailure::Scheduler));
    assert_eq!(health.active_ticks(), 0);
    let before = host.calls.load(Ordering::SeqCst);
    assert!(matches!(
        AgentWorker::start(binding(&f, evidence(&f)), host.clone(), options()).await,
        Err(AgentWorkerError::Unavailable)
    ));
    assert_eq!(host.calls.load(Ordering::SeqCst), before); // Actual binding checked first.
}

#[tokio::test]
async fn postgres_worker_fixture_terminal_contract() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let run = admit(&f).await;
    let scheduler = DurableTenantScheduler::new(
        f.store.clone(),
        f.executable.clone(),
        evidence(&f),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
        DurableTenantSchedulerOptions::default(),
    )
    .unwrap();
    let tick = scheduler
        .tick(f.caller.tenant_id().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(
        matches!(tick.outcome(), TenantSchedulerOutcome::Executed { result, .. } if matches!(result.outcome(), AgentLoopOutcome::Succeeded(_))),
        "{tick:?}"
    );
    succeeded(&f, run).await;
}

#[tokio::test]
async fn postgres_worker_readiness_execution_concurrency_and_joined_shutdown() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let host = Arc::new(Host::default());
    let e = evidence(&f);
    host.mode.store(1, Ordering::SeqCst);
    let first = admit(&f).await;
    assert!(matches!(
        AgentWorker::start(binding(&f, e.clone()), host.clone(), options()).await,
        Err(AgentWorkerError::Unavailable)
    ));
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);
    host.mode.store(0, Ordering::SeqCst);
    let mut worker = AgentWorker::start(binding(&f, e), host.clone(), options())
        .await
        .unwrap();
    let health = worker.health();
    succeeded(&f, first).await;
    host.mode.store(1, Ordering::SeqCst);
    until(|| health.status() == AgentWorkerStatus::Unavailable && health.active_ticks() == 0).await;
    let second = admit(&f).await;
    sleep(Duration::from_millis(120)).await;
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 1);
    host.mode.store(0, Ordering::SeqCst);
    succeeded(&f, second).await;
    f.node_block.store(true, Ordering::SeqCst);
    for _ in 0..5 {
        admit(&f).await;
    }
    until(|| health.active_nodes() == 2).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(health.active_nodes(), 2);
    assert_eq!(health.active_ticks(), 2);
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 4);
    assert!(
        timeout(Duration::from_millis(20), worker.wait())
            .await
            .is_err()
    );
    let report = worker.shutdown().await.unwrap();
    assert_eq!(report.failure, None);
    assert_eq!(report.forced_ticks, 0);
    assert!(report.executed_quanta >= 2);
    assert!(report.readiness_failures > 0);
    assert_eq!(health.active_ticks(), 0);
    assert_eq!(health.active_nodes(), 0);
    assert_eq!(health.status(), AgentWorkerStatus::Stopped);
    assert_eq!(worker.wait().await, Err(AgentWorkerError::Stopped));
    println!(
        "\nSTATEKNOT_WORKER_EXECUTION_EVIDENCE=startup_denied;readiness_pause_resume;real_execution;fixed_concurrency;cancel_safe_wait;joined_nodes"
    );
}

#[tokio::test]
async fn postgres_worker_forced_shutdown_recovers_in_fresh_process_without_node_repeat() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let e = evidence(&f);
    e.mode.store(1, Ordering::SeqCst);
    let run = admit(&f).await;
    let mut worker = AgentWorker::start(
        short_lease_binding(&f, e.clone()).await,
        Arc::new(Host::default()),
        options()
            .with_execution_limits(1, Duration::from_secs(20), Duration::from_millis(50))
            .unwrap(),
    )
    .await
    .unwrap();
    let health = worker.health();
    until(|| e.entered.load(Ordering::SeqCst) == 1).await;
    let report = worker.shutdown().await.unwrap();
    assert_eq!(report.forced_ticks, 1);
    assert_eq!(report.failure, None);
    assert_eq!(health.active_ticks(), 0);
    assert_eq!(health.active_nodes(), 0);
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.service
            .load(f.caller.clone(), run)
            .await
            .unwrap()
            .status(),
        RunStatus::Active
    );
    let output = child(&f, "recover", &run.to_string()).await;
    assert!(output.contains("STATEKNOT_WORKER_CHILD_RECOVERED=node_calls:0"));
    succeeded(&f, run).await;
    println!(
        "\nSTATEKNOT_WORKER_RECOVERY_EVIDENCE=forced_join;lease_expiry;fresh_process;terminal_replay;no_node_repeat"
    );
}

#[tokio::test]
async fn postgres_worker_deadline_and_panics_fail_stop() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    for mode in [1, 2] {
        let e = evidence(&f);
        e.mode.store(mode, Ordering::SeqCst);
        admit(&f).await;
        let mut worker = AgentWorker::start(
            binding(&f, e),
            Arc::new(Host::default()),
            options()
                .with_execution_limits(
                    1,
                    Duration::from_secs(if mode == 1 { 2 } else { 20 }),
                    Duration::from_millis(50),
                )
                .unwrap(),
        )
        .await
        .unwrap();
        let health = worker.health();
        let report = timeout(Duration::from_secs(5), worker.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            report.failure,
            Some(if mode == 1 {
                AgentWorkerFailure::TickDeadline
            } else {
                AgentWorkerFailure::Task
            })
        );
        assert_eq!(health.active_nodes(), 0);
        assert_eq!(health.active_ticks(), 0);
    }
    let host = Arc::new(Host::default());
    let mut worker = AgentWorker::start(binding(&f, evidence(&f)), host.clone(), options())
        .await
        .unwrap();
    host.mode.store(3, Ordering::SeqCst);
    assert_eq!(
        timeout(Duration::from_secs(5), worker.wait())
            .await
            .unwrap()
            .unwrap()
            .failure,
        Some(AgentWorkerFailure::Task)
    );
    println!(
        "\nSTATEKNOT_WORKER_FAILSTOP_EVIDENCE=tick_deadline;lifecycle_panic;probe_panic;joined_cleanup"
    );
}

#[tokio::test]
async fn postgres_worker_drop_reclaims_nested_nodes() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.node_block.store(true, Ordering::SeqCst);
    admit(&f).await;
    let worker = AgentWorker::start(
        binding(&f, evidence(&f)),
        Arc::new(Host::default()),
        options(),
    )
    .await
    .unwrap();
    let health = worker.health();
    until(|| health.active_nodes() == 1).await;
    drop(worker);
    until(|| {
        health.status() == AgentWorkerStatus::Stopped
            && health.active_ticks() == 0
            && health.active_nodes() == 0
    })
    .await;
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn postgres_worker_startup_cancellation_and_probe_timeout_claim_no_work() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    admit(&f).await;
    let host = Arc::new(Host::default());
    host.mode.store(2, Ordering::SeqCst);
    assert!(
        timeout(
            Duration::from_millis(50),
            AgentWorker::start(binding(&f, evidence(&f)), host.clone(), options())
        )
        .await
        .is_err()
    );
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);
    let limits = options()
        .with_readiness_limits(
            Duration::from_millis(50),
            Duration::from_secs(3),
            Duration::from_secs(4),
        )
        .unwrap();
    assert!(matches!(
        AgentWorker::start(binding(&f, evidence(&f)), host.clone(), limits.clone()).await,
        Err(AgentWorkerError::Unavailable)
    ));
    host.mode.store(0, Ordering::SeqCst);
    let mut worker = AgentWorker::start(binding(&f, evidence(&f)), host.clone(), limits)
        .await
        .unwrap();
    let health = worker.health();
    host.mode.store(2, Ordering::SeqCst);
    until(|| health.status() == AgentWorkerStatus::Unavailable).await;
    let calls = host.calls.load(Ordering::SeqCst);
    sleep(Duration::from_millis(150)).await;
    assert!(host.calls.load(Ordering::SeqCst) - calls <= 2); // Single-flight + delay, no busy loop.
    assert!(worker.shutdown().await.unwrap().failure.is_none());
}

#[tokio::test]
async fn postgres_worker_run_failure_backs_off_without_crashing_role() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let e = evidence(&f);
    e.mode.store(3, Ordering::SeqCst);
    let run = admit(&f).await;
    let limits = options()
        .with_execution_limits(1, Duration::from_secs(20), Duration::from_secs(2))
        .unwrap();
    let mut worker = AgentWorker::start(binding(&f, e.clone()), Arc::new(Host::default()), limits)
        .await
        .unwrap();
    let health = worker.health();
    until(|| health.report().run_failures > 0).await;
    let calls = e.entered.load(Ordering::SeqCst);
    sleep(Duration::from_millis(30)).await;
    assert_eq!(e.entered.load(Ordering::SeqCst), calls);
    e.mode.store(0, Ordering::SeqCst);
    succeeded(&f, run).await;
    assert_eq!(worker.shutdown().await.unwrap().failure, None);
    assert_eq!(f.node_calls.load(Ordering::SeqCst), 1);
}

fn fair_policy(f: &Fixture, shard: &str) -> WeightedFairnessPolicy {
    WeightedFairnessPolicy::new(
        SchedulerShardId::new(shard).unwrap(),
        [
            TenantFairnessWeight::new(f.caller.tenant_id().clone(), 2).unwrap(),
            TenantFairnessWeight::new(
                TenantId::new(format!("idle-{}", f.caller.tenant_id())).unwrap(),
                1,
            )
            .unwrap(),
        ],
    )
    .unwrap()
}
async fn fair_worker(f: &Fixture, policy: WeightedFairnessPolicy) -> u64 {
    let binding = AgentWorkerBinding::fair(
        f.store.clone(),
        f.executable.clone(),
        evidence(f),
        policy,
        AgentWorkerExecutionOptions::default(),
    )
    .await
    .unwrap();
    let mut worker = AgentWorker::start(
        binding,
        Arc::new(Host::default()),
        options()
            .with_execution_limits(1, Duration::from_secs(20), Duration::from_secs(2))
            .unwrap(),
    )
    .await
    .unwrap();
    let health = worker.health();
    until(|| health.report().completed_ticks >= 6).await;
    let report = worker.shutdown().await.unwrap();
    assert_eq!(report.failure, None);
    assert_eq!(report.started_ticks, report.completed_ticks);
    report.started_ticks
}

#[tokio::test]
async fn postgres_worker_fair_cursor_survives_fresh_process() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let shard = format!("worker-{}", EventId::generate());
    let policy = fair_policy(&f, &shard);
    let first = fair_worker(&f, policy.clone()).await;
    let output = child(&f, "fair", &shard).await;
    let second: u64 = output
        .lines()
        .find_map(|line| line.strip_prefix("STATEKNOT_WORKER_CHILD_SLOTS="))
        .unwrap()
        .parse()
        .unwrap();
    let reservation = f
        .store
        .reserve_scheduler_fairness_slot(
            policy.shard_id(),
            policy.digest(),
            SchedulerReservationId::generate(),
        )
        .await
        .unwrap();
    assert_eq!(reservation.sequence(), first + second);
    assert_eq!(
        u64::from(reservation.slot()),
        (first + second) % u64::from(policy.cycle_length())
    );
    println!(
        "\nSTATEKNOT_WORKER_FAIR_EVIDENCE=owned_fair_role;persisted_policy;fresh_process;continuous_slots"
    );
}

async fn child(f: &Fixture, mode: &str, value: &str) -> String {
    let output = timeout(
        Duration::from_secs(25),
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "postgres_worker_child",
                "--ignored",
                "--nocapture",
            ])
            .env(
                "STATEKNOT_WORKER_CHILD_TENANT",
                f.caller.tenant_id().as_str(),
            )
            .env("STATEKNOT_WORKER_CHILD_MODE", mode)
            .env("STATEKNOT_WORKER_CHILD_VALUE", value)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "child stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test]
#[ignore = "fresh-process fixture invoked by parent qualification"]
async fn postgres_worker_child() {
    let tenant = TenantId::new(std::env::var("STATEKNOT_WORKER_CHILD_TENANT").unwrap()).unwrap();
    let f = Fixture::for_tenant(tenant).await.unwrap();
    let value = std::env::var("STATEKNOT_WORKER_CHILD_VALUE").unwrap();
    match std::env::var("STATEKNOT_WORKER_CHILD_MODE")
        .unwrap()
        .as_str()
    {
        "recover" => {
            let mut worker = AgentWorker::start(
                binding(&f, evidence(&f)),
                Arc::new(Host::default()),
                options(),
            )
            .await
            .unwrap();
            succeeded(&f, value.parse().unwrap()).await;
            assert_eq!(worker.shutdown().await.unwrap().failure, None);
            assert_eq!(f.node_calls.load(Ordering::SeqCst), 0);
            println!("\nSTATEKNOT_WORKER_CHILD_RECOVERED=node_calls:0");
        }
        "fair" => {
            let slots = fair_worker(&f, fair_policy(&f, &value)).await;
            println!("\nSTATEKNOT_WORKER_CHILD_SLOTS={slots}");
        }
        _ => panic!("unexpected child mode"),
    }
}
