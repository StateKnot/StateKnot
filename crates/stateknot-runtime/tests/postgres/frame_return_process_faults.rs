// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real Return COMMIT cuts, owned process death and cold higher-fence recovery.
use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, parent_control, publish_ready},
};
use sqlx_core::{connection::ConnectOptions, query_scalar::query_scalar};
use sqlx_postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use stateknot_core::{GraphFrameBarrierPlan, GraphFrameCheckpointHead, JournalHead};
use stateknot_store_postgres::{GraphFrameReturnCommitOutcome, StoredGraphFrameReturn};
use std::collections::BTreeMap;

const WORKER: &str = "nested_driver::frame_return_process_faults::return_commit_loss_worker";
type Snapshot = BTreeMap<&'static str, Value>;

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    terminal: GraphFrameCheckpointHead,
    base_checkpoint_id: CheckpointId,
    fence: RunFence,
    observed: JournalHead,
    direct_usage: BudgetUsage,
    event_id: EventId,
}

async fn worker_store(input: &Value) -> PostgresStore {
    let mut connection: PgConnectOptions =
        std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let cut = input["role"].as_str() == Some("cut");
    if cut {
        connection = connection
            .host("127.0.0.1")
            .port(u16::try_from(input["proxy_port"].as_u64().unwrap()).unwrap());
    }
    // One connection belongs to the test proxy. Cold Driver recovery retains the
    // existing fixture's eight-connection pool and all original timing options.
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, if cut { 1 } else { 8 })
        .with_acquire_timeout(Duration::from_secs(30))
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
        .with_lease_timing(Duration::from_secs(30), Duration::from_secs(5 * 60));
    PostgresStore::connect(connection.to_url_lossy().as_str(), options)
        .await
        .unwrap()
}

async fn terminal_plan(
    store: &PostgresStore,
    fixture: &NestedFixture,
    scenario: &Scenario,
) -> GraphFrameBarrierPlan {
    let tenant = scenario.fence.tenant_id();
    let run = scenario.fence.run_id();
    let namespace = scenario.terminal.frame().namespace();
    let stored = store
        .load_graph_frame_barrier(tenant, run, namespace, scenario.base_checkpoint_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.checkpoint().head(), scenario.terminal);
    assert!(matches!(
        stored.disposition(),
        GraphBarrierDisposition::Terminal { .. }
    ));
    let base = store
        .load_graph_frame_checkpoint(tenant, run, namespace, scenario.base_checkpoint_id)
        .await
        .unwrap();
    let mut results = Vec::new();
    for head in stored.barrier().result_heads() {
        let result = store
            .load_pending_node_result(head.activation())
            .await
            .unwrap();
        assert_eq!(result.head(), *head);
        results.push(result);
    }
    let executable = fixture
        .driver
        .registry
        .resolve(scenario.terminal.frame().target())
        .unwrap();
    let plan = executable
        .graph()
        .plan_frame_barrier(
            &base,
            &results,
            scenario.terminal.checkpoint().checkpoint_id(),
            executable.schemas(),
            executable.reducer(),
        )
        .unwrap();
    assert_eq!(plan.barrier(), stored.barrier());
    assert_eq!(plan.disposition(), &stored.disposition());
    assert!(
        stored
            .checkpoint()
            .checkpoint()
            .matches_write(plan.barrier().successor())
    );
    plan
}

async fn return_attempt(
    store: &PostgresStore,
    fixture: &NestedFixture,
    scenario: &Scenario,
) -> (bool, StoredGraphFrameReturn) {
    let plan = Box::pin(terminal_plan(store, fixture, scenario)).await;
    let parent = fixture
        .driver
        .registry
        .resolve(scenario.terminal.frame().origin().base_checkpoint().graph())
        .unwrap();
    match Box::pin(store.return_graph_frame(
        plan,
        scenario.event_id,
        scenario.fence.clone(),
        scenario.observed.clone(),
        scenario.direct_usage.clone(),
        fixture.driver.registry.schemas(),
        parent.reducer(),
    ))
    .await
    .unwrap()
    {
        GraphFrameReturnCommitOutcome::Committed(record) => (false, record),
        GraphFrameReturnCommitOutcome::Idempotent(record) => (true, record),
        _ => panic!("unsupported whole Return outcome"),
    }
}

#[tokio::test]
async fn return_commit_loss_worker() {
    let Ok(input) = std::env::var(INPUT_ENV) else {
        return;
    };
    let resume = parent_control();
    let input: Value = serde_json::from_str(&input).unwrap();
    let scenario: Scenario = serde_json::from_value(input["scenario"].clone()).unwrap();
    let store = worker_store(&input).await;
    let fixture = nested_fixture();
    if input["role"].as_str() == Some("cut") {
        let _ = Box::pin(return_attempt(&store, &fixture, &scenario)).await;
        panic!("Return transaction returned before the required process kill");
    }
    assert_eq!(input["role"].as_str(), Some("resume"));
    let already_committed = input["already_committed"].as_bool().unwrap();
    let replayed = if already_committed {
        let (idempotent, record) = Box::pin(return_attempt(&store, &fixture, &scenario)).await;
        assert!(
            idempotent,
            "old-fence whole Return must recover its original record"
        );
        assert_eq!(record.completion().start().fence(), &scenario.fence);
        Some(json!({"digest": record.digest(), "event": record.event().head()}))
    } else {
        None
    };
    publish_ready(&json!({"phase":"cold_before_driver", "replayed":replayed}));
    resume.await.unwrap();
    let fence: RunFence = serde_json::from_value(input["replacement_fence"].clone()).unwrap();
    assert!(fence.epoch() > scenario.fence.epoch());
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.driver.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap()
    .with_direct_usage_evidence(Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    }));
    let outcome = driver
        .drive(fence, CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        outcome.outcome(),
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    let expected_returns = u32::from(!already_committed);
    assert_eq!(outcome.report().frame_returns_committed(), expected_returns);
    assert_eq!(outcome.report().frame_callers_rebound(), expected_returns);
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 1);
    assert!(
        store
            .load_active_graph_frame(scenario.fence.tenant_id(), scenario.fence.run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .load_run(scenario.fence.tenant_id(), scenario.fence.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.close().await;
}

async fn prepare(store: &PostgresStore) -> Scenario {
    let fixture = nested_fixture();
    let ids = AgentRunIds::generate();
    let tenant_id = tenant("frame-return-process-fault");
    admit_nested_fixture(store, &fixture, &tenant_id, ids).await;
    let evidence = Arc::new(AdmissionDirectEvidence {
        store: store.clone(),
    });
    for quantum in [3, 2, 3, 2] {
        let driver = DurableGraphDriver::new(
            store.clone(),
            fixture.driver.registry.clone(),
            nested_quantum(quantum),
        )
        .unwrap()
        .with_direct_usage_evidence(evidence.clone());
        let lease = store
            .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
            .await
            .unwrap();
        let outcome = driver
            .drive(lease.lease().fence().clone(), CancellationSignal::never())
            .await
            .unwrap();
        assert!(matches!(
            outcome.outcome(),
            GraphDriveOutcome::Yielded { .. }
        ));
        assert_eq!(outcome.report().durable_events(), quantum);
        assert_eq!(outcome.report().frame_returns_committed(), 0);
    }
    let active = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert!(active.checkpoint().checkpoint().ready_nodes().is_empty());
    assert_eq!(fixture.leaf_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.root_calls.load(Ordering::SeqCst), 0);
    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap();
    let fence = lease.lease().fence().clone();
    let usage = evidence
        .usage_at(
            &tenant_id,
            ids.run_id(),
            active.run().journal_head().unwrap(),
        )
        .await
        .unwrap();
    store
        .rebind_graph_frame_caller(
            active.checkpoint().frame(),
            AttemptId::generate(),
            EventId::generate(),
            fence.clone(),
            active.run().journal_head().unwrap().clone(),
            usage,
        )
        .await
        .unwrap();
    let active = store
        .load_active_graph_frame(&tenant_id, ids.run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.caller().fence(), &fence);
    let observed = active.run().journal_head().unwrap().clone();
    let direct_usage = evidence
        .usage_at(&tenant_id, ids.run_id(), &observed)
        .await
        .unwrap();
    Scenario {
        base_checkpoint_id: active
            .checkpoint()
            .checkpoint()
            .parent()
            .unwrap()
            .checkpoint_id(),
        terminal: active.checkpoint().head(),
        fence,
        observed,
        direct_usage,
        event_id: EventId::generate(),
    }
}

async fn snapshot(pool: &PgPool, scenario: &Scenario) -> Snapshot {
    let mut rows = BTreeMap::new();
    for table in [
        "runs",
        "run_events",
        "run_attempt_claims",
        "node_attempts",
        "node_attempt_completions",
        "pending_node_results",
        "pending_node_result_consumptions",
        "graph_frame_entries",
        "graph_frame_barriers",
        "graph_frame_caller_bindings",
        "graph_frame_returns",
        "graph_frame_heads",
        "graph_frame_stacks",
        "run_checkpoints",
    ] {
        let bytes: String = query_scalar(&format!(
            "SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text),'[]'::jsonb)::text FROM stateknot.{table} x WHERE tenant_id=$1 AND run_id=$2"
        )).bind(scenario.fence.tenant_id().as_str()).bind(*scenario.fence.run_id().as_uuid())
            .fetch_one(pool).await.unwrap();
        rows.insert(table, serde_json::from_str(&bytes).unwrap());
    }
    rows
}

fn assert_atomic_return(before: &Snapshot, after: &Snapshot) {
    for table in [
        "run_events",
        "graph_frame_returns",
        "pending_node_results",
        "node_attempt_completions",
    ] {
        let original = before[table].as_array().unwrap();
        let committed = after[table].as_array().unwrap();
        assert_eq!(committed.len(), original.len() + 1, "{table}");
        assert!(
            original.iter().all(|row| committed.contains(row)),
            "{table} old rows changed"
        );
    }
    for table in [
        "run_attempt_claims",
        "node_attempts",
        "pending_node_result_consumptions",
        "graph_frame_entries",
        "graph_frame_barriers",
        "graph_frame_caller_bindings",
        "graph_frame_heads",
        "run_checkpoints",
    ] {
        assert_eq!(after[table], before[table], "{table}");
    }
    let mut prior = before["graph_frame_stacks"][0].clone();
    assert!(!prior["active_namespace"].as_str().unwrap().is_empty());
    prior["active_namespace"] = json!("");
    prior["active_frame_identity_digest"] = Value::Null;
    assert_eq!(after["graph_frame_stacks"], json!([prior]));
    for column in [
        "checkpoint_id",
        "checkpoint_superstep",
        "checkpoint_digest",
        "lifecycle_bytes",
    ] {
        assert_eq!(
            after["runs"][0][column], before["runs"][0][column],
            "{column}"
        );
    }
}

async fn owning_return(store: &PostgresStore, scenario: &Scenario) -> StoredGraphFrameReturn {
    let record = store
        .load_graph_frame_return(
            scenario.fence.tenant_id(),
            scenario.fence.run_id(),
            scenario.terminal.frame().namespace(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.terminal_checkpoint(), &scenario.terminal);
    assert_eq!(
        record.result().intent().activation(),
        scenario.terminal.frame().origin()
    );
    let result = store
        .load_pending_node_result(scenario.terminal.frame().origin())
        .await
        .unwrap();
    assert_eq!(&result, record.result());
    let attempt = store
        .load_node_attempt(
            scenario.fence.tenant_id(),
            &scenario.fence.run_id(),
            record.completion().start().attempt_id(),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json_canonicalizer::to_vec(attempt.completion().unwrap()).unwrap(),
        serde_json_canonicalizer::to_vec(record.completion()).unwrap()
    );
    record
}

fn immutable_return_bundle(rows: &Snapshot, record: &StoredGraphFrameReturn) -> Vec<Value> {
    let sequence = json!(record.event().sequence().get());
    let mut bundle = Vec::new();
    for (table, column) in [
        ("run_events", "sequence"),
        ("graph_frame_returns", "journal_sequence"),
        ("pending_node_results", "journal_sequence"),
        ("node_attempt_completions", "journal_sequence"),
    ] {
        let found: Vec<_> = rows[table]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row[column] == sequence)
            .collect();
        assert_eq!(found.len(), 1, "{table}");
        bundle.push(Value::clone(found[0]));
    }
    bundle
}

async fn wait_backend_disconnect(pool: &PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let gone: bool =
                query_scalar("SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
                    .bind(pid)
                    .fetch_one(pool)
                    .await
                    .unwrap();
            if gone {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("killed writer backend did not disconnect");
}

async fn interrupt_return(
    store: &PostgresStore,
    pool: &PgPool,
    connection: &PgConnectOptions,
    scenario: &Scenario,
    cut: Cut,
) -> Option<(Digest, Vec<Value>)> {
    let before = snapshot(pool, scenario).await;
    let mut proxy = CommitProxy::start(connection, cut, TransactionTarget::GraphFrameReturn).await;
    let mut interrupted = TestProcess::spawn(
        WORKER,
        &json!({"role":"cut", "scenario":scenario, "proxy_port":proxy.port()}),
    );
    proxy.wait_cut(cut).await;
    let backend = proxy.backend_pid();
    let at_cut = snapshot(pool, scenario).await;
    let original = if cut == Cut::CommitResponse {
        assert_atomic_return(&before, &at_cut);
        let record = owning_return(store, scenario).await;
        assert_eq!(record.event().event_id(), scenario.event_id);
        assert_eq!(record.completion().start().fence(), &scenario.fence);
        let current = store
            .load_run(scenario.fence.tenant_id(), scenario.fence.run_id())
            .await
            .unwrap();
        assert_eq!(current.journal_head(), Some(&record.event().head()));
        Some((record.digest(), immutable_return_bundle(&at_cut, &record)))
    } else {
        assert_eq!(at_cut, before, "unforwarded Return COMMIT leaked facts");
        None
    };
    interrupted.kill_and_reap().await;
    proxy.stop().await;
    wait_backend_disconnect(pool, backend).await;
    let after_kill = snapshot(pool, scenario).await;
    assert_eq!(
        &after_kill,
        if original.is_some() { &at_cut } else { &before }
    );
    original
}

async fn cold_recovery(
    store: &PostgresStore,
    pool: &PgPool,
    scenario: &Scenario,
    original: Option<&(Digest, Vec<Value>)>,
) -> (StoredGraphFrameReturn, RunFence) {
    let replacement = store
        .supersede_lease(
            scenario.fence.tenant_id(),
            scenario.fence.run_id(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let replacement_fence = replacement.lease().fence().clone();
    assert!(replacement_fence.epoch() > scenario.fence.epoch());
    let before_recovery = snapshot(pool, scenario).await;
    assert!(matches!(
        store
            .renew_lease(&scenario.fence, replacement.lease().expires_at())
            .await,
        Err(StoreError::StaleFence)
    ));
    assert_eq!(snapshot(pool, scenario).await, before_recovery);
    let mut recovery = TestProcess::spawn(
        WORKER,
        &json!({"role":"resume", "scenario":scenario,
        "already_committed":original.is_some(), "replacement_fence":replacement_fence}),
    );
    let ready = recovery.ready().await.unwrap();
    assert_eq!(ready["phase"], "cold_before_driver");
    if let Some((digest, _)) = original {
        assert_eq!(ready["replayed"]["digest"], json!(digest));
    } else {
        assert!(ready["replayed"].is_null());
    }
    assert_eq!(
        snapshot(pool, scenario).await,
        before_recovery,
        "original ACK recovery created new facts"
    );
    recovery.resume();
    recovery.wait_success().await;
    (owning_return(store, scenario).await, replacement_fence)
}

async fn run_cut(store: &PostgresStore, pool: &PgPool, connection: &PgConnectOptions, cut: Cut) {
    let scenario = prepare(store).await;
    let original = Box::pin(interrupt_return(store, pool, connection, &scenario, cut)).await;
    let (record, replacement) = cold_recovery(store, pool, &scenario, original.as_ref()).await;
    let final_rows = snapshot(pool, &scenario).await;
    assert_eq!(
        final_rows["graph_frame_returns"].as_array().unwrap().len(),
        1
    );
    if let Some((digest, bytes)) = original {
        assert_eq!(record.digest(), digest);
        assert_eq!(immutable_return_bundle(&final_rows, &record), bytes);
        assert_eq!(record.completion().start().fence(), &scenario.fence);
    } else {
        assert_eq!(record.completion().start().fence(), &replacement);
    }
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(pool)
        .await
        .unwrap();
    println!(
        "\nSTATEKNOT_FRAME_RETURN_COMMIT_LOSS_EVIDENCE={}",
        json!({
            "profile":"frame-return-commit-loss-v1", "cut":if cut == Cut::BeforeCommit { "commit_not_forwarded" } else { "commit_response_withheld" },
            "commit_forwarded":cut == Cut::CommitResponse, "commit_response_forwarded":false,
            "writer_killed_and_reaped":true, "cold_higher_fence_driver_verified":true,
            "original_return_recovered":cut == Cut::CommitResponse, "leaf_application_redispatches":0,
            "parent_application_calls":1, "invariants":"passed", "postgres":version,
            "termination":if cfg!(unix) { "SIGKILL" } else { "Child::kill" }, "os":std::env::consts::OS,
            "arch":std::env::consts::ARCH,
        })
    );
}

#[tokio::test]
async fn frame_return_commit_loss_survives_real_kill_and_cold_higher_fence_driver() {
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
