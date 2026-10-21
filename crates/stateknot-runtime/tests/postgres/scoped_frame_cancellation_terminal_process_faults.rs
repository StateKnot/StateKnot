// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cut only the final `ConfirmCancellation` transaction, after closure is durable.
//! Test children receive tenant/run IDs, never a prior handoff, candidate or fence.
use super::*;
use stateknot_core::{ChildRunBudgetAccount, RunLifecycle};
use stateknot_runtime::{DurableFrameCancellationFinalizer, FrameCancellationOutcome};
use stateknot_store_postgres::{AppendOutcome, StoredRun};

const CASE: &str = "scoped_join_close_process_faults::terminal_confirmation::closed_scoped_join_terminal_commit_loss_survives_real_kill_and_id_only_cold_retry";

#[derive(serde::Deserialize)]
#[serde(tag = "role", rename_all = "snake_case", deny_unknown_fields)]
enum WorkerInput {
    Writer {
        tenant: TenantId,
        run: RunId,
        proxy_port: u16,
    },
    Cold {
        tenant: TenantId,
        run: RunId,
    },
}

struct ClosedFixture {
    scenario: Box<Scenario>,
    closure: Box<StoredGraphFrameClosure>,
    account: Box<ChildRunBudgetAccount>,
    snapshot: Snapshot,
    closure_bytes: Vec<u8>,
}

struct KilledCut {
    snapshot: Snapshot,
    signal: i32,
    writer_pid: u64,
}

fn maintenance_schemas() -> JsonSchemaRegistry {
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    stateknot_runtime::register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
    schemas.build().unwrap()
}

fn cancelled(outcome: FrameCancellationOutcome) -> (JournalEvent, &'static str) {
    let FrameCancellationOutcome::Cancelled(append) = outcome else {
        panic!("a real cancellation closure must not become NotApplicable");
    };
    let kind = match append.as_ref() {
        AppendOutcome::Committed(_) => "committed",
        AppendOutcome::Idempotent(_) => "idempotent",
        _ => panic!("unexpected append outcome"),
    };
    (append.event().clone(), kind)
}

async fn worker(input: &Value) {
    let resume = parent_control();
    let parsed: WorkerInput = serde_json::from_value(input.clone()).unwrap();
    let (tenant, run, writer) = match parsed {
        WorkerInput::Writer {
            tenant,
            run,
            proxy_port,
        } => {
            assert_eq!(input["proxy_port"].as_u64(), Some(u64::from(proxy_port)));
            (tenant, run, true)
        }
        WorkerInput::Cold { tenant, run } => (tenant, run, false),
    };
    // The inherited connection helper performs no migration. Only the writer
    // uses the one-session proxy; a cold worker opens its own direct connection.
    let store = worker_store(input).await;
    let finalizer = DurableFrameCancellationFinalizer::new(
        store.clone(),
        maintenance_schemas(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    if writer {
        publish_ready(
            &json!({"phase":"writer_before_confirmation", "case":CASE, "pid":std::process::id()}),
        );
        resume.await.unwrap();
        let result = finalizer.resume(&tenant, run).await;
        panic!("terminal confirmation returned before the required process kill: {result:?}");
    }
    let (first, first_kind) = cancelled(finalizer.resume(&tenant, run).await.unwrap());
    let (second, second_kind) = cancelled(finalizer.resume(&tenant, run).await.unwrap());
    assert_eq!(second_kind, "idempotent");
    assert_eq!(
        second, first,
        "the real terminal ACK must retain every event byte"
    );
    drop(finalizer);
    store.close().await;
    publish_ready(&json!({
        "phase":"cold_terminal_ack_verified", "case":CASE, "pid":std::process::id(),
        "first_append":first_kind, "second_append":second_kind,
        "first_event":first, "second_event":second,
    }));
    resume.await.unwrap();
}

async fn prepare_closed(store: &PostgresStore, pool: &PgPool) -> ClosedFixture {
    let scenario = Box::new(Box::pin(prepare(store)).await);
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
    let closure = Box::new(Box::pin(owning_close(store, &scenario)).await);
    assert_eq!(fingerprint(&closure), fingerprint(committed.record()));
    let account = Box::new(
        Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap(),
    );
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::CancellationRequested);
    assert_eq!(run.journal_head(), Some(&closure.event().head()));
    let closure_bytes = v2_bytes(pool, &scenario).await;
    Box::pin(check_retained_owning_facts(store, &scenario)).await;
    let snapshot = snapshot(pool, key.tenant_id()).await;
    ClosedFixture {
        scenario,
        closure,
        account,
        snapshot,
        closure_bytes,
    }
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

async fn actual_terminal_event(
    store: &PostgresStore,
    scenario: &Scenario,
    closure: &StoredGraphFrameClosure,
) -> JournalEvent {
    let key = &scenario.key;
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
    event.clone()
}

fn check_terminal_projection(
    run: &StoredRun,
    closed: &ClosedFixture,
    event: &JournalEvent,
) -> BudgetUsage {
    let scenario = &closed.scenario;
    assert_eq!(run.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(run.journal_head(), Some(&event.head()));
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
    let original: RunLifecycle = serde_json::from_value(scenario.lifecycle.clone()).unwrap();
    let total = scenario
        .direct
        .checked_accumulate(&scenario.child_usage.cumulative_only())
        .unwrap();
    assert_ne!(
        total, scenario.direct,
        "the settled child contributes exactly once"
    );
    assert_eq!(run.lifecycle().terminal_usage(), Some(&total));
    let expected = original
        .apply(RunTransition::ConfirmCancellation {
            completed_at: run.lifecycle().changed_at(),
            usage: total.clone(),
        })
        .unwrap();
    assert_eq!(
        serde_json::to_value(run.lifecycle()).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert!(run.lifecycle().changed_at() >= closed.closure.event().recorded_at());
    assert!(
        run.lifecycle().changed_at() < event.recorded_at(),
        "real lifecycle time must differ from later event persistence time"
    );
    total
}

async fn check_terminal_account(
    store: &PostgresStore,
    closed: &ClosedFixture,
    event: &JournalEvent,
    total: &BudgetUsage,
) {
    let scenario = &closed.scenario;
    let key = &scenario.key;
    let account = Box::pin(store.load_child_budget_account(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        account,
        closed
            .account
            .observe_direct(event.head(), scenario.direct.clone())
            .unwrap()
    );
    let actual_total = Box::pin(store.include_child_usage(
        key.tenant_id(),
        key.parent_run_id(),
        scenario.direct.clone(),
    ))
    .await
    .unwrap();
    assert_eq!(&actual_total, total);
}

async fn check_actual_terminal(store: &PostgresStore, closed: &ClosedFixture) -> JournalEvent {
    let scenario = &closed.scenario;
    let key = &scenario.key;
    let event = Box::pin(actual_terminal_event(store, scenario, &closed.closure)).await;
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let total = check_terminal_projection(&run, closed, &event);
    Box::pin(check_terminal_account(store, closed, &event, &total)).await;
    Box::pin(check_retained_owning_facts(store, scenario)).await;
    event
}

async fn inspect_cut(
    store: &PostgresStore,
    pool: &PgPool,
    closed: &ClosedFixture,
    cut: Cut,
) -> Snapshot {
    let scenario = &closed.scenario;
    let key = &scenario.key;
    let observed = snapshot(pool, key.tenant_id()).await;
    let retained_closure =
        Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
    Box::pin(check_retained_owning_facts(store, scenario)).await;
    assert_eq!(fingerprint(&retained_closure), fingerprint(&closed.closure));
    assert_eq!(v2_bytes(pool, scenario).await, closed.closure_bytes);
    match cut {
        Cut::BeforeCommit => {
            assert_eq!(
                observed, closed.snapshot,
                "COMMIT was not forwarded: no tenant write may be visible"
            );
            let run = store
                .load_run(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap();
            assert_eq!(run.lifecycle().status(), RunStatus::CancellationRequested);
            assert_eq!(run.journal_head(), Some(&closed.closure.event().head()));
            let page = store
                .load_journal_page(
                    key.tenant_id(),
                    key.parent_run_id(),
                    Some(&closed.closure.event().head()),
                    JournalPageSize::new(1).unwrap(),
                )
                .await
                .unwrap();
            assert!(page.events().is_empty());
            assert!(!page.has_more());
        }
        Cut::CommitResponse => {
            // This independent, non-proxy reader proves durable cancellation
            // before the live writer is killed with its real ACK still withheld.
            let _terminal = Box::pin(check_actual_terminal(store, closed)).await;
            terminal_snapshot_delta(&closed.snapshot, &observed, key.parent_run_id());
        }
    }
    assert_eq!(
        snapshot(pool, key.tenant_id()).await,
        observed,
        "read-only cut checks changed tenant facts"
    );
    observed
}

fn exit_signal(status: std::process::ExitStatus) -> Option<i32> {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    }
    #[cfg(not(unix))]
    {
        let _ = status;
        None
    }
}

async fn kill_at_cut(
    store: &PostgresStore,
    pool: &PgPool,
    connection: &PgConnectOptions,
    closed: &ClosedFixture,
    cut: Cut,
) -> KilledCut {
    let key = &closed.scenario.key;
    let mut proxy = CommitProxy::start(
        connection,
        cut,
        TransactionTarget::GraphFrameCancellationConfirmation,
    )
    .await;
    let mut writer = TestProcess::spawn(
        CASE,
        &json!({
            "role":"writer", "tenant":key.tenant_id(), "run":key.parent_run_id(), "proxy_port":proxy.port(),
        }),
    );
    let ready = writer.ready().await.unwrap();
    assert_eq!(ready["phase"], "writer_before_confirmation");
    assert_eq!(ready["case"], CASE);
    let writer_pid = ready["pid"].as_u64().unwrap();
    assert_ne!(writer_pid, u64::from(std::process::id()));
    println!("\nSTATEKNOT_FRAME_CANCELLATION_WRITER_OBSERVATION={ready}");
    writer.resume();
    proxy.wait_cut(cut).await;
    let backend_pid = proxy.backend_pid();
    let observed = Box::pin(inspect_cut(store, pool, closed, cut)).await;
    assert_eq!(proxy.messages_while_ack_held(), 0);
    let killed = writer.kill_and_reap().await;
    let signal = exit_signal(killed).expect("terminal confirmation cuts require real Unix SIGKILL");
    assert_eq!(signal, 9);
    proxy.stop().await;
    wait_backend_disconnect(pool, backend_pid).await;
    assert_eq!(
        snapshot(pool, key.tenant_id()).await,
        observed,
        "kill/rollback must preserve the actual cut state"
    );
    KilledCut {
        snapshot: observed,
        signal,
        writer_pid,
    }
}

async fn cold_retry(
    store: &PostgresStore,
    pool: &PgPool,
    closed: &ClosedFixture,
    cut: Cut,
    killed: &KilledCut,
) -> (Value, JournalEvent) {
    let key = &closed.scenario.key;
    // These are the only business facts transferred into the new process.
    let mut process = TestProcess::spawn(
        CASE,
        &json!({"role":"cold", "tenant":key.tenant_id(), "run":key.parent_run_id()}),
    );
    let observation = process.ready().await.unwrap();
    assert_eq!(observation["phase"], "cold_terminal_ack_verified");
    assert_eq!(observation["case"], CASE);
    let pid = observation["pid"].as_u64().unwrap();
    assert_ne!(pid, u64::from(std::process::id()));
    assert_ne!(pid, killed.writer_pid);
    assert_eq!(
        observation["first_append"],
        if cut == Cut::BeforeCommit {
            "committed"
        } else {
            "idempotent"
        }
    );
    assert_eq!(observation["second_append"], "idempotent");
    println!("\nSTATEKNOT_FRAME_CANCELLATION_COLD_OBSERVATION={observation}");
    let event = Box::pin(check_actual_terminal(store, closed)).await;
    assert_eq!(
        observation["first_event"],
        serde_json::to_value(&event).unwrap()
    );
    assert_eq!(observation["second_event"], observation["first_event"]);
    let after = snapshot(pool, key.tenant_id()).await;
    terminal_snapshot_delta(&closed.snapshot, &after, key.parent_run_id());
    if cut == Cut::CommitResponse {
        assert_eq!(
            after, killed.snapshot,
            "cold exact ACK must not mutate any tenant fact"
        );
    }
    let retained_closure =
        Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .unwrap();
    Box::pin(check_retained_owning_facts(store, &closed.scenario)).await;
    assert_eq!(fingerprint(&retained_closure), fingerprint(&closed.closure));
    assert_eq!(v2_bytes(pool, &closed.scenario).await, closed.closure_bytes);
    process.resume();
    process.wait_success().await;
    assert_eq!(
        snapshot(pool, key.tenant_id()).await,
        after,
        "successful same-selector child exit changed tenant facts"
    );
    (observation, event)
}

async fn run_cut(store: &PostgresStore, pool: &PgPool, connection: &PgConnectOptions, cut: Cut) {
    let closed = Box::pin(prepare_closed(store, pool)).await;
    let killed = Box::pin(kill_at_cut(store, pool, connection, &closed, cut)).await;
    let (cold, event) = Box::pin(cold_retry(store, pool, &closed, cut, &killed)).await;
    let key = &closed.scenario.key;
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(pool)
        .await
        .unwrap();
    println!(
        "\nSTATEKNOT_COLD_FRAME_CANCELLATION_COMMIT_LOSS_EVIDENCE={}",
        json!({
            "profile":"frame-cancellation-terminal-commit-loss-v1",
            "cut":if cut == Cut::BeforeCommit {"commit_not_forwarded"} else {"commit_response_withheld"},
            "transaction_target":"projected-run-head-terminal-confirmation",
            "closure_committed_before_proxy":true, "closure_version":2,
            "commit_forwarded":cut == Cut::CommitResponse, "commit_response_forwarded":false,
            "writer_killed_and_reaped":true, "signal":killed.signal, "writer_pid":killed.writer_pid,
            "cold_pid":cold["pid"], "cold_process_success":true, "cold_business_inputs":"tenant-and-run-only",
            "cold_first_append":cold["first_append"], "second_actual_ACK":"idempotent",
            "actual_terminal_event":event, "completed_at":run.lifecycle().changed_at(),
            "terminal_usage":run.lifecycle().terminal_usage(), "closure_event_id":closed.closure.event().event_id(),
            "closure_bytes_sha256":Digest::sha256(&closed.closure_bytes),
            "whole_tenant_exact_delta":true, "unique_terminal_event":true, "actual_ACK_event_identical":true,
            "owner_status":"Executing", "owner_completion":false,
            "Join_published_or_consumed":false, "original_fence_owning_account_exact":true,
            "invariants":"passed", "postgres":version, "termination":"SIGKILL",
            "os":std::env::consts::OS, "arch":std::env::consts::ARCH,
        })
    );
}

#[tokio::test]
async fn closed_scoped_join_terminal_commit_loss_survives_real_kill_and_id_only_cold_retry() {
    if let Ok(input) = std::env::var(INPUT_ENV) {
        Box::pin(worker(&serde_json::from_str(&input).unwrap())).await;
        return;
    }
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let connection = std::env::var(DATABASE_URL_ENV)
        .ok()
        .map(|url| url.parse::<PgConnectOptions>().unwrap());
    if let Some(connection) = &connection {
        let _ = loopback_target(connection);
    }
    let Some(store) = test_store().await else {
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    for cut in [Cut::BeforeCommit, Cut::CommitResponse] {
        Box::pin(run_cut(&store, &pool, connection.as_ref().unwrap(), cut)).await;
    }
    pool.close().await;
    store.close().await;
}
