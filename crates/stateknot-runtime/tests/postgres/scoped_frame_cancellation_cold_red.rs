// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Future-contract tracer: existing owned maintenance must finish a cold closure.
use super::*;
use stateknot_core::ChildRunBudgetAccount;
use stateknot_runtime::{
    agent_maintenance::*, register_standard_agent_cancellation_event_schema,
    register_standard_agent_deadline_event_schema, register_standard_child_join_event_schema,
    register_standard_child_reconciliation_event_schema,
    register_standard_run_failure_close_event_schema,
};

const CASE: &str = "scoped_join_close_process_faults::cold_continuation::closed_scoped_join_cancellation_is_finished_by_fresh_process_owned_maintenance";

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ColdInput {
    tenant: TenantId,
    run: RunId,
}

async fn connect_existing() -> PostgresStore {
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, 8)
        .with_acquire_timeout(Duration::from_secs(30))
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
        .with_lease_timing(Duration::from_secs(30), Duration::from_secs(5 * 60));
    // The producer already migrated. A fresh process must only connect.
    PostgresStore::connect(&std::env::var(DATABASE_URL_ENV).unwrap(), options)
        .await
        .unwrap()
}

fn maintenance_schemas() -> JsonSchemaRegistry {
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    register_standard_agent_deadline_event_schema(&mut schemas).unwrap();
    register_standard_child_reconciliation_event_schema(&mut schemas).unwrap();
    register_standard_child_join_event_schema(&mut schemas).unwrap();
    register_standard_run_failure_close_event_schema(&mut schemas).unwrap();
    register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
    for (reference, document) in [
        PostgresStore::graph_frame_closure_event_schema().unwrap(),
        PostgresStore::graph_frame_join_stop_event_schema().unwrap(),
        PostgresStore::graph_frame_published_join_stop_event_schema().unwrap(),
    ] {
        schemas.register(reference, document).unwrap();
    }
    schemas.build().unwrap()
}

struct ActualDependencies {
    store: PostgresStore,
    input: ColdInput,
}

impl AgentMaintenanceReadiness for ActualDependencies {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentMaintenanceReadinessError>> {
        Box::pin(async move {
            self.store
                .observe_database_clock()
                .await
                .map_err(|_| AgentMaintenanceReadinessError)?;
            let proof = Box::pin(
                self.store
                    .load_graph_frame_closure(&self.input.tenant, self.input.run),
            )
            .await
            .map_err(|_| AgentMaintenanceReadinessError)?;
            if proof.is_none() {
                return Err(AgentMaintenanceReadinessError);
            }
            Ok(())
        })
    }
}

async fn worker(input: ColdInput) {
    let resume = parent_control();
    let store = connect_existing().await;
    let before = store.load_run(&input.tenant, input.run).await.unwrap();
    assert_eq!(
        before.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert!(before.lease().is_none());
    assert!(!before.is_quarantined());
    let binding = AgentMaintenanceBinding::new(
        store.clone(),
        maintenance_schemas(),
        vec![input.tenant.clone()],
        AgentMaintenanceMutationOptions::default(),
    )
    .unwrap();
    let host = Arc::new(ActualDependencies {
        store: store.clone(),
        input: ColdInput {
            tenant: input.tenant.clone(),
            run: input.run,
        },
    });
    let mut role = AgentMaintenance::start(binding, host, AgentMaintenanceOptions::default())
        .await
        .unwrap();
    let health = role.health();
    // This is the existing maintenance fixture's status-convergence bound.
    // Timeout is an observation: stop/join and report it before the final expect.
    let observed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let run = store.load_run(&input.tenant, input.run).await.unwrap();
            let report = health.report();
            let visited = AgentMaintenanceJob::ALL
                .into_iter()
                .all(|job| report.job(job).completed_ticks > 0);
            if (run.lifecycle().status() == RunStatus::Cancelled && visited)
                || report.failure.is_some()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let report = role.shutdown().await.unwrap();
    let after = store.load_run(&input.tenant, input.run).await.unwrap();
    let converged = observed.is_ok()
        && after.lifecycle().status() == RunStatus::Cancelled
        && AgentMaintenanceJob::ALL
            .into_iter()
            .all(|job| report.job(job).completed_ticks > 0);
    let jobs: Vec<Value> = AgentMaintenanceJob::ALL
        .into_iter()
        .map(|job| {
            json!({
                "job":format!("{job:?}"), "completed_ticks":report.job(job).completed_ticks,
                "items":report.job(job).items, "item_failures":report.job(job).item_failures,
            })
        })
        .collect();
    let observation = json!({
        "pid":std::process::id(), "status":after.lifecycle().status(),
        "converged_within_existing_30s":converged,
        "started_ticks":report.started_ticks, "readiness_failures":report.readiness_failures,
        "forced_ticks":report.forced_ticks, "failure":format!("{:?}",report.failure),
        "jobs":jobs, "health":format!("{:?}",health.status()),
        "active_ticks":health.active_ticks(),
    });
    store.close().await;
    // The real role has joined; keep only this owned test process alive so the
    // controller receives the complete actual report and observes successful exit.
    publish_ready(&observation);
    resume.await.unwrap();
}

fn table_rows(snapshot: &Snapshot, table: &str) -> Vec<Value> {
    serde_json::from_str(&snapshot[table]).unwrap()
}

fn same_parent_columns_except(
    before: &Snapshot,
    after: &Snapshot,
    table: &str,
    key: &str,
    parent: RunId,
    allowed: &[&str],
) {
    let parent = parent.to_string();
    let project = |facts: &Snapshot| {
        let mut rows = table_rows(facts, table);
        let mut matches = 0;
        for row in &mut rows {
            if row[key].as_str() == Some(parent.as_str()) {
                matches += 1;
                for field in allowed {
                    assert!(row.as_object_mut().unwrap().remove(*field).is_some());
                }
            }
        }
        assert_eq!(matches, 1, "one actual target row in {table}");
        rows.sort_by_key(|row| serde_json::to_string(row).unwrap());
        rows
    };
    assert_eq!(
        project(after),
        project(before),
        "immutable columns/other rows: {table}"
    );
}

fn terminal_snapshot_delta(before: &Snapshot, after: &Snapshot, parent: RunId) {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    for (table, rows) in before {
        if !["runs", "run_events", "child_run_budget_accounts"].contains(&table.as_str()) {
            assert_eq!(
                after.get(table),
                Some(rows),
                "unchanged whole tenant table: {table}"
            );
        }
    }
    same_parent_columns_except(
        before,
        after,
        "runs",
        "run_id",
        parent,
        &[
            "journal_sequence",
            "journal_event_id",
            "journal_recorded_at",
            "journal_digest",
            "lifecycle_bytes",
            "lifecycle_revision",
            "lifecycle_status",
            "changed_at",
            "scheduler_ready_at",
            "scheduler_not_before",
            "lease_attempt_id",
            "lease_acquired_at",
            "lease_renewed_at",
            "lease_expires_at",
            "wait_set_digest",
            "unresolved_wait_count",
            "next_timer_due_at",
            "next_interrupt_expiry_at",
            "updated_at",
        ],
    );
    same_parent_columns_except(
        before,
        after,
        "child_run_budget_accounts",
        "parent_run_id",
        parent,
        &[
            "account_digest",
            "account_bytes",
            "journal_sequence",
            "journal_event_id",
            "journal_recorded_at",
            "journal_digest",
        ],
    );
    let parent_text = parent.to_string();
    let runs = table_rows(after, "runs");
    let run = runs
        .iter()
        .find(|row| row["run_id"].as_str() == Some(parent_text.as_str()))
        .unwrap();
    assert_eq!(run["updated_at"], run["journal_recorded_at"]);
    assert_eq!(run["unresolved_wait_count"], 0);
    for field in [
        "scheduler_ready_at",
        "scheduler_not_before",
        "lease_attempt_id",
        "lease_acquired_at",
        "lease_renewed_at",
        "lease_expires_at",
        "wait_set_digest",
        "next_timer_due_at",
        "next_interrupt_expiry_at",
    ] {
        assert!(run[field].is_null(), "terminal row retained {field}");
    }
    let original = table_rows(before, "run_events");
    let current = table_rows(after, "run_events");
    assert_eq!(current.len(), original.len() + 1);
    for row in &original {
        assert!(
            current.contains(row),
            "an original tenant event was changed or lost"
        );
    }
    let added: Vec<_> = current
        .iter()
        .filter(|row| !original.contains(row))
        .collect();
    assert_eq!(added.len(), 1);
    let parent = parent.to_string();
    assert_eq!(added[0]["run_id"].as_str(), Some(parent.as_str()));
}

async fn check_retained_owning_facts(store: &PostgresStore, scenario: &Scenario) {
    let key = &scenario.key;
    let history = Box::pin(store.load_node_attempt_history_page(
        key.parent(),
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].start().head(), scenario.owner);
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    assert!(history.records()[0].completion().is_none());
    let join = Box::pin(store.load_child_join(key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(join.request(), &scenario.join_request);
    assert_eq!(join.registration(), &scenario.join_registration);
    assert!(join.binding().is_none());
    assert!(join.head().is_none());
    assert!(join.consumed().is_none());
    assert_eq!(
        Box::pin(store.load_child_run(key))
            .await
            .unwrap()
            .settlement()
            .unwrap()
            .usage(),
        &scenario.child_usage
    );
    assert!(
        Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap(),
        scenario.root_checkpoint
    );
}

#[allow(clippy::too_many_lines)]
async fn check_actual_terminal(
    store: &PostgresStore,
    scenario: &Scenario,
    closure: &StoredGraphFrameClosure,
    original_account: &ChildRunBudgetAccount,
) {
    let key = &scenario.key;
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert!(run.lease().is_none());
    assert!(!run.is_quarantined());
    assert_eq!(run.last_fencing_epoch(), Some(scenario.fence.epoch()));
    assert!(run.scheduler_ready_at().is_none());
    assert!(run.scheduler_not_before().is_none());
    assert!(run.lifecycle().result().is_none());
    assert_eq!(
        run.lifecycle().revision().get(),
        scenario.revision.get() + 1
    );
    let original: stateknot_core::RunLifecycle =
        serde_json::from_value(scenario.lifecycle.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(run.lifecycle().cancellation_request()).unwrap(),
        serde_json::to_value(original.cancellation_request()).unwrap()
    );
    assert_eq!(
        serde_json::to_value(run.lifecycle().terminal_failure()).unwrap(),
        serde_json::to_value(
            original
                .cancellation_request()
                .map(stateknot_core::RunCancellationRequest::failure)
        )
        .unwrap()
    );
    let total = scenario
        .direct
        .checked_accumulate(&scenario.child_usage.cumulative_only())
        .unwrap();
    assert_eq!(run.lifecycle().terminal_usage(), Some(&total));
    let page = store
        .load_journal_page(
            key.tenant_id(),
            key.parent_run_id(),
            Some(&closure.event().head()),
            JournalPageSize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    assert_eq!(page.events().len(), 1);
    let event = &page.events()[0];
    assert_eq!(run.journal_head(), Some(&event.head()));
    assert_eq!(event.sequence().get(), closure.event().sequence().get() + 1);
    assert_eq!(event.previous_digest(), Some(closure.event().digest()));
    assert_eq!(
        event.source(),
        &stateknot_core::JournalEventSource::ControlPlane
    );
    assert_eq!(
        event.payload().kind().as_str(),
        "agent-cancellation-confirmed"
    );
    assert_eq!(
        event.payload().schema(),
        &stateknot_runtime::standard_agent_cancellation_event_schema()
            .unwrap()
            .0
    );
    maintenance_schemas()
        .validate_bounded(event.payload().schema(), event.payload().data())
        .unwrap();
    let graph = scenario
        .root_checkpoint
        .graph()
        .definition_digest()
        .to_string();
    assert_eq!(
        event.payload().data().as_value(),
        &json!({
            "operation":"agent_cancellation_confirmed", "graph_digest":graph.strip_prefix("sha256:").unwrap(),
            "checkpoint_id":scenario.root_checkpoint.checkpoint_id().to_string(),
            "superstep":scenario.root_checkpoint.superstep().get().to_string(), "failure_id":closure.failure().id().to_string(),
        })
    );
    assert!(run.lifecycle().changed_at() >= closure.event().recorded_at());
    assert!(run.lifecycle().changed_at() <= event.recorded_at());
    let account = Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        account,
        original_account
            .observe_direct(event.head(), scenario.direct.clone())
            .unwrap()
    );
    assert_eq!(
        Box::pin(store.include_child_usage(
            key.tenant_id(),
            key.parent_run_id(),
            scenario.direct.clone()
        ))
        .await
        .unwrap(),
        total
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn closed_scoped_join_cancellation_is_finished_by_fresh_process_owned_maintenance() {
    if let Some(input) = std::env::var_os(INPUT_ENV) {
        Box::pin(worker(
            serde_json::from_str(input.to_str().unwrap()).unwrap(),
        ))
        .await;
        return;
    }
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let scenario = Box::pin(prepare(&store)).await;
    let key = &scenario.key;
    let committed = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        scenario.event_id,
        scenario.observed.clone(),
        scenario.revision,
        scenario.direct.clone(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        committed,
        GraphFrameClosureCommitOutcome::Committed(_)
    ));
    let proof = Box::pin(owning_close(&store, &scenario)).await;
    let original_proof = fingerprint(&proof);
    let original_account =
        Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let before = snapshot(&pool, key.tenant_id()).await;
    let original_bytes = v2_bytes(&pool, &scenario).await;
    drop(proof);
    drop(committed);
    pool.close().await;
    store.close().await;
    drop(store);

    // No handoff, fence, checkpoint, closure, usage or old EventId crosses the
    // process boundary. The actual existing maintenance role owns its discovery.
    let input = ColdInput {
        tenant: key.tenant_id().clone(),
        run: key.parent_run_id(),
    };
    let mut process = TestProcess::spawn(CASE, &serde_json::to_value(input).unwrap());
    let observation = process
        .ready()
        .await
        .expect("fresh maintenance must return its actual joined report");
    process.resume();
    process.wait_success().await;
    println!("STATEKNOT_COLD_FRAME_MAINTENANCE_OBSERVATION={observation}");
    assert_ne!(
        observation["pid"].as_u64(),
        Some(u64::from(std::process::id()))
    );
    assert_eq!(observation["health"], "Stopped");
    assert_eq!(observation["active_ticks"], 0);
    assert_eq!(observation["forced_ticks"], 0);
    assert_eq!(observation["failure"], "None");
    assert_eq!(observation["readiness_failures"], 0);
    let jobs = observation["jobs"].as_array().unwrap();
    assert_eq!(jobs.len(), AgentMaintenanceJob::ALL.len());
    assert!(!jobs.is_empty());
    assert!(observation["started_ticks"].as_u64().unwrap() >= u64::try_from(jobs.len()).unwrap());
    for job in jobs {
        assert!(
            job["completed_ticks"].as_u64().unwrap() > 0,
            "actual job did not complete a tick: {job}"
        );
        assert_eq!(job["item_failures"], 0, "actual per-item failures: {job}");
    }
    let fresh = connect_existing().await;
    let proof = Box::pin(fresh.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fingerprint(&proof), original_proof);
    Box::pin(check_retained_owning_facts(&fresh, &scenario)).await;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    assert_eq!(v2_bytes(&pool, &scenario).await, original_bytes);
    let current = fresh
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(
        observation["status"],
        serde_json::to_value(current.lifecycle().status()).unwrap()
    );
    let after = snapshot(&pool, key.tenant_id()).await;
    if current.lifecycle().status() == RunStatus::Cancelled {
        assert_eq!(observation["converged_within_existing_30s"], true);
        Box::pin(check_actual_terminal(
            &fresh,
            &scenario,
            &proof,
            &original_account,
        ))
        .await;
        terminal_snapshot_delta(&before, &after, key.parent_run_id());
    } else {
        assert_eq!(
            after, before,
            "an unsuccessful cold continuation must not change any tenant row"
        );
    }
    assert_eq!(snapshot(&pool, key.tenant_id()).await, after);
    pool.close().await;
    fresh.close().await;
    assert_eq!(
        current.lifecycle().status(),
        RunStatus::Cancelled,
        "a fresh process running owned maintenance must finish the real whole-frame cancellation; actual joined observation={observation}"
    );
}
