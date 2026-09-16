// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use stateknot_runtime::{
    agent_maintenance::*, register_standard_agent_deadline_event_schema,
    register_standard_child_join_event_schema, register_standard_child_reconciliation_event_schema,
};
use std::sync::atomic::AtomicUsize;
use tokio::time::{sleep, timeout};

#[derive(Default)]
struct Host {
    mode: AtomicUsize,
}
impl AgentMaintenanceReadiness for Host {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentMaintenanceReadinessError>> {
        Box::pin(async {
            match self.mode.load(Ordering::SeqCst) {
                0 => Ok(()),
                1 => Err(AgentMaintenanceReadinessError),
                2 => std::future::pending().await,
                _ => panic!("fixture host panic"),
            }
        })
    }
}
fn schemas() -> JsonSchemaRegistry {
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    register_standard_agent_deadline_event_schema(&mut schemas).unwrap();
    register_standard_child_reconciliation_event_schema(&mut schemas).unwrap();
    register_standard_child_join_event_schema(&mut schemas).unwrap();
    register_standard_run_failure_close_event_schema(&mut schemas).unwrap();
    schemas.build().unwrap()
}
fn binding(store: &PostgresStore, tenants: Vec<TenantId>) -> AgentMaintenanceBinding {
    AgentMaintenanceBinding::new(
        store.clone(),
        schemas(),
        tenants,
        AgentMaintenanceMutationOptions::default(),
    )
    .unwrap()
}
fn options() -> AgentMaintenanceOptions {
    AgentMaintenanceOptions::default()
        .with_pacing(Duration::from_millis(10), Duration::from_millis(20))
        .unwrap()
        .with_readiness_limits(
            Duration::from_secs(1),
            Duration::from_secs(5),
            Duration::from_secs(10),
        )
        .unwrap()
}
async fn status(store: &PostgresStore, tenant: &TenantId, run: RunId, expected: RunStatus) {
    timeout(Duration::from_secs(30), async {
        loop {
            if store
                .load_run(tenant, run)
                .await
                .unwrap()
                .lifecycle()
                .status()
                == expected
            {
                return;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("maintenance must converge durable status");
}
async fn wait_health(health: &AgentMaintenanceHealth, expected: AgentMaintenanceStatus) {
    timeout(Duration::from_secs(20), async {
        while health.status() != expected {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn owned_maintenance_drives_all_jobs_and_excludes_unauthorized_tenants() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let deadline_tenant = tenant("maintenance-deadline");
    let excluded = tenant("maintenance-excluded");
    let due = deadlines::future_deadline(&store, 3).await;
    let root = deadlines::admit_deadline(&store, &fixture, deadline_tenant.clone(), due).await;
    let excluded_root = deadlines::admit_deadline(&store, &fixture, excluded.clone(), due).await;
    let mut closing = started(&store, "maintenance-close").await;
    let key = closing.intent.key().clone();
    let child = spawn(&store, &closing)
        .await
        .unwrap()
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    finish_node(&store, &mut closing).await;
    let original = request(&store, &closing, direct_usage())
        .await
        .unwrap()
        .record()
        .failure()
        .clone();
    let mut joining = started(&store, "maintenance-join").await;
    let joined = spawn(&store, &joining).await.unwrap();
    joining.head = joined.record().spawn().head();
    let join_child = joined
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    let join = stateknot_core::ChildRunJoinRequest::new([joining.intent.key().clone()]).unwrap();
    store
        .register_child_join(
            join.clone(),
            &joining.node,
            parent_append(&joining, "child-join-registered"),
        )
        .await
        .unwrap();
    fail(
        &store,
        join.activation().tenant_id(),
        join_child,
        BudgetUsage::zero(),
    )
    .await
    .unwrap();
    deadlines::await_due(&store, due).await;
    let tenants = vec![
        deadline_tenant.clone(),
        key.tenant_id().clone(),
        join.activation().tenant_id().clone(),
    ];
    let mut role = AgentMaintenance::start(
        binding(&store, tenants.clone()),
        Arc::new(Host::default()),
        options(),
    )
    .await
    .unwrap();
    // Two replicas share durable serialization, not a second in-memory queue.
    let mut replica = AgentMaintenance::start(
        binding(&store, tenants),
        Arc::new(Host::default()),
        options(),
    )
    .await
    .unwrap();
    status(
        &store,
        &deadline_tenant,
        root.admission().intent().provenance().run_id(),
        RunStatus::CancellationRequested,
    )
    .await;
    status(
        &store,
        key.tenant_id(),
        child,
        RunStatus::CancellationRequested,
    )
    .await;
    // Execution/cleanup remains separate; maintenance must not invent completion.
    assert_ne!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Failed
    );
    cancellation::confirm(&store, key.tenant_id(), child, BudgetUsage::zero())
        .await
        .unwrap();
    status(
        &store,
        key.tenant_id(),
        key.parent_run_id(),
        RunStatus::Failed,
    )
    .await;
    let closed = store
        .load_run_failure_close(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(closed.failure()).unwrap(),
        serde_json::to_value(&original).unwrap()
    );
    timeout(Duration::from_secs(30), async {
        loop {
            if store
                .load_child_join(join.activation())
                .await
                .unwrap()
                .unwrap()
                .head()
                .is_some()
            {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store
            .load_run(
                &excluded,
                excluded_root.admission().intent().provenance().run_id()
            )
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Active
    );
    let report = role.shutdown().await.unwrap();
    let other = replica.shutdown().await.unwrap();
    assert_eq!(report.failure, None);
    assert_eq!(other.failure, None);
    for job in AgentMaintenanceJob::ALL {
        assert!(report.job(job).items + other.job(job).items > 0);
    }
    assert_eq!(role.health().active_ticks(), 0);
    assert_eq!(role.health().status(), AgentMaintenanceStatus::Stopped);
    assert!(
        store.observe_database_clock().await.is_ok(),
        "shared pool stays open"
    );
    println!(
        "\nSTATEKNOT_MAINTENANCE_EFFECTS_EVIDENCE={{\"deadline\":true,\"child_cancel\":true,\"child_settle\":true,\"join\":true,\"failure_close\":true,\"replicas\":true,\"tenant_allowlist\":true}}"
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn owned_maintenance_advances_past_a_full_failed_page_and_restart_revisits_it() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant = tenant("maintenance-cursor");
    let due = deadlines::future_deadline(&store, 10).await;
    let mut runs = Vec::new();
    for _ in 0..17 {
        runs.push(
            deadlines::admit_deadline(&store, &fixture, tenant.clone(), due)
                .await
                .admission()
                .intent()
                .provenance()
                .run_id(),
        );
    }
    runs.sort();
    deadlines::await_due(&store, due).await;
    let pool = sql_pool().await;
    query(&format!("CREATE FUNCTION stateknot.test_maintenance_page_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.tenant_id='{}' AND NEW.run_id<>'{}'::uuid AND NEW.lifecycle_status='cancellation_requested' THEN RAISE EXCEPTION 'fixture failed page'; END IF; RETURN NEW; END $$",tenant.as_str(),runs[16])).execute(&pool).await.unwrap();
    query("CREATE TRIGGER test_maintenance_page_failure BEFORE UPDATE ON stateknot.runs FOR EACH ROW EXECUTE FUNCTION stateknot.test_maintenance_page_failure()").execute(&pool).await.unwrap();
    let mut role = AgentMaintenance::start(
        binding(&store, vec![tenant.clone()]),
        Arc::new(Host::default()),
        options(),
    )
    .await
    .unwrap();
    let result = timeout(
        Duration::from_secs(30),
        status(&store, &tenant, runs[16], RunStatus::CancellationRequested),
    )
    .await;
    let report = role.shutdown().await.unwrap();
    query("DROP TRIGGER test_maintenance_page_failure ON stateknot.runs")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_maintenance_page_failure()")
        .execute(&pool)
        .await
        .unwrap();
    result.unwrap();
    assert!(report.job(AgentMaintenanceJob::Deadline).item_failures >= 16);
    assert_eq!(report.failure, None);
    for job in AgentMaintenanceJob::ALL {
        assert!(report.job(job).completed_ticks > 0);
    }
    for run in &runs[..16] {
        assert_eq!(
            store
                .load_run(&tenant, *run)
                .await
                .unwrap()
                .lifecycle()
                .status(),
            RunStatus::Active
        );
    }
    let restart = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store.options().clone(),
    )
    .await
    .unwrap();
    let mut role = AgentMaintenance::start(
        binding(&restart, vec![tenant.clone()]),
        Arc::new(Host::default()),
        options(),
    )
    .await
    .unwrap();
    for run in runs {
        status(&store, &tenant, run, RunStatus::CancellationRequested).await;
    }
    assert_eq!(role.shutdown().await.unwrap().failure, None);
    println!(
        "\nSTATEKNOT_MAINTENANCE_CURSOR_EVIDENCE={{\"failed_page\":16,\"tail_reached\":true,\"restart_revisited\":true}}"
    );
    restart.close().await;
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn owned_maintenance_readiness_wait_drop_and_startup_fail_closed() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("maintenance-readiness");
    let host = Arc::new(Host::default());
    assert!(matches!(
        AgentMaintenanceBinding::new(
            store.clone(),
            schemas(),
            vec![],
            AgentMaintenanceMutationOptions::default()
        ),
        Err(AgentMaintenanceError::InvalidTenants)
    ));
    assert!(matches!(
        AgentMaintenanceBinding::new(
            store.clone(),
            schemas(),
            vec![tenant.clone(); 2],
            AgentMaintenanceMutationOptions::default()
        ),
        Err(AgentMaintenanceError::InvalidTenants)
    ));
    let mut incomplete = JsonSchemaRegistryBuilder::with_default_limits();
    register_standard_agent_deadline_event_schema(&mut incomplete).unwrap();
    assert!(matches!(
        AgentMaintenanceBinding::new(
            store.clone(),
            incomplete.build().unwrap(),
            vec![tenant.clone()],
            AgentMaintenanceMutationOptions::default()
        ),
        Err(AgentMaintenanceError::SchemaUnavailable)
    ));
    for mode in 1..=3 {
        host.mode.store(mode, Ordering::SeqCst);
        assert!(matches!(
            AgentMaintenance::start(
                binding(&store, vec![tenant.clone()]),
                host.clone(),
                options()
            )
            .await,
            Err(AgentMaintenanceError::Unavailable)
        ));
    }
    host.mode.store(0, Ordering::SeqCst);
    let mut role = AgentMaintenance::start(
        binding(&store, vec![tenant.clone()]),
        host.clone(),
        options(),
    )
    .await
    .unwrap();
    let health = role.health();
    assert!(
        timeout(Duration::from_millis(20), role.wait())
            .await
            .is_err(),
        "cancelled wait must not lose handle"
    );
    host.mode.store(1, Ordering::SeqCst);
    wait_health(&health, AgentMaintenanceStatus::Unavailable).await;
    sleep(Duration::from_millis(100)).await;
    let before = health.report().started_ticks;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(health.report().started_ticks, before);
    host.mode.store(0, Ordering::SeqCst);
    wait_health(&health, AgentMaintenanceStatus::Ready).await;
    timeout(Duration::from_secs(10), async {
        while health.report().started_ticks == before {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    role.begin_shutdown();
    assert_eq!(health.status(), AgentMaintenanceStatus::Draining);
    let report = role.wait().await.unwrap();
    assert_eq!(report.failure, None);
    assert!(report.readiness_failures > 0);
    assert_eq!(role.wait().await, Err(AgentMaintenanceError::Stopped));
    let role = AgentMaintenance::start(binding(&store, vec![tenant]), host, options())
        .await
        .unwrap();
    let health = role.health();
    drop(role);
    wait_health(&health, AgentMaintenanceStatus::Stopped).await;
    timeout(Duration::from_secs(5), async {
        while health.active_ticks() != 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(store.observe_database_clock().await.is_ok());
    store.close().await;
}

#[tokio::test]
async fn owned_maintenance_periodic_panic_and_store_error_fail_stop() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("maintenance-failstop");
    let host = Arc::new(Host::default());
    let mut role = AgentMaintenance::start(
        binding(&store, vec![tenant.clone()]),
        host.clone(),
        options(),
    )
    .await
    .unwrap();
    host.mode.store(3, Ordering::SeqCst);
    let report = timeout(Duration::from_secs(15), role.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.failure, Some(AgentMaintenanceFailure::Task));
    assert_eq!(role.health().active_ticks(), 0);
    let slow_probe = options()
        .with_readiness_limits(
            Duration::from_secs(60),
            Duration::from_secs(5),
            Duration::from_secs(90),
        )
        .unwrap();
    let mut role = AgentMaintenance::start(
        binding(&store, vec![tenant]),
        Arc::new(Host::default()),
        slow_probe,
    )
    .await
    .unwrap();
    store.close().await;
    let report = timeout(Duration::from_secs(15), role.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.failure, Some(AgentMaintenanceFailure::Store));
    assert_eq!(role.health().active_ticks(), 0);
}

#[tokio::test]
async fn owned_maintenance_tick_deadline_cancels_locked_database_work() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let bounded = options()
        .with_deadlines(Duration::from_millis(200), Duration::from_secs(2))
        .unwrap()
        .with_readiness_limits(
            Duration::from_secs(60),
            Duration::from_secs(5),
            Duration::from_secs(90),
        )
        .unwrap();
    let mut role = AgentMaintenance::start(
        binding(&store, vec![tenant("maintenance-deadline-lock")]),
        Arc::new(Host::default()),
        bounded,
    )
    .await
    .unwrap();
    let pool = sql_pool().await;
    let mut lock = pool.begin().await.unwrap();
    query("LOCK TABLE stateknot.runs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let result = timeout(Duration::from_secs(10), role.wait()).await;
    lock.rollback().await.unwrap();
    let report = result.unwrap().unwrap();
    assert_eq!(report.failure, Some(AgentMaintenanceFailure::TickDeadline));
    assert_eq!(role.health().active_ticks(), 0);
    println!(
        "\nSTATEKNOT_MAINTENANCE_LIFECYCLE_EVIDENCE={{\"real_database_tick_deadline\":true,\"joined\":true}}"
    );
    pool.close().await;
    store.close().await;
}

const CHILD_TEST: &str =
    "child_admission::ownership::failure_close::maintenance::owned_maintenance_process_worker";
#[tokio::test]
async fn owned_maintenance_process_worker() {
    if std::env::var_os(process_harness::INPUT_ENV).is_none() {
        return;
    }
    process_harness::watch_parent();
    let input: Value =
        serde_json::from_str(&std::env::var(process_harness::INPUT_ENV).unwrap()).unwrap();
    let store = test_store().await.unwrap();
    let tenant: TenantId = input["tenant"].as_str().unwrap().parse().unwrap();
    let child: RunId = input["child"].as_str().unwrap().parse().unwrap();
    let _role = AgentMaintenance::start(
        binding(&store, vec![tenant.clone()]),
        Arc::new(Host::default()),
        options(),
    )
    .await
    .unwrap();
    status(&store, &tenant, child, RunStatus::CancellationRequested).await;
    process_harness::ready_and_park(json!({"cancelled":true})).await;
}

#[tokio::test]
async fn owned_maintenance_sigkill_then_fresh_owner_finishes_without_duplicate_effects() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "maintenance-process").await;
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
    let original = request(&store, &value, direct_usage())
        .await
        .unwrap()
        .record()
        .failure()
        .clone();
    let mut process = process_harness::TestProcess::spawn(
        CHILD_TEST,
        &json!({"tenant":key.tenant_id().as_str(),"child":child.to_string()}),
    );
    assert_eq!(process.ready().await.unwrap()["cancelled"], true);
    let before = store
        .load_run(key.tenant_id(), child)
        .await
        .unwrap()
        .lifecycle()
        .cancellation_request()
        .unwrap()
        .clone();
    process.kill_and_reap().await;
    let restart = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store.options().clone(),
    )
    .await
    .unwrap();
    let mut role = AgentMaintenance::start(
        binding(&restart, vec![key.tenant_id().clone()]),
        Arc::new(Host::default()),
        options(),
    )
    .await
    .unwrap();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(
        serde_json::to_value(
            store
                .load_run(key.tenant_id(), child)
                .await
                .unwrap()
                .lifecycle()
                .cancellation_request()
        )
        .unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    cancellation::confirm(&store, key.tenant_id(), child, BudgetUsage::zero())
        .await
        .unwrap();
    status(
        &store,
        key.tenant_id(),
        key.parent_run_id(),
        RunStatus::Failed,
    )
    .await;
    assert_eq!(
        serde_json::to_value(
            store
                .load_run_failure_close(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap()
                .unwrap()
                .failure()
        )
        .unwrap(),
        serde_json::to_value(&original).unwrap()
    );
    assert_eq!(role.shutdown().await.unwrap().failure, None);
    println!(
        "\nSTATEKNOT_MAINTENANCE_RECOVERY_EVIDENCE={{\"forced_process_loss\":true,\"fresh_owner\":true,\"cancellation_identity_preserved\":true,\"original_failure_preserved\":true}}"
    );
    restart.close().await;
    store.close().await;
}
