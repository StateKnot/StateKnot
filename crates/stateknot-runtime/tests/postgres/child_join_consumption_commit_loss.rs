// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real-process COMMIT-loss qualification for atomic Join-result consumption.

use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, ready_and_park, watch_parent},
};
use sqlx_core::{connection::ConnectOptions, query_scalar::query_scalar};
use sqlx_postgres::PgConnectOptions;
use stateknot_core::{ChildRunJoinHead, NodeAttemptStartHead};
use stateknot_store_postgres::JournalPage;

const WORKER: &str = "child_admission::ownership::join::consumption_commit_loss::child_join_consumption_commit_loss_worker";

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    expected: NodeAttemptStartHead,
    join_head: ChildRunJoinHead,
}

async fn worker_store(input: &Value) -> PostgresStore {
    let url = std::env::var(DATABASE_URL_ENV).expect("COMMIT-loss worker requires PostgreSQL");
    let mut connection: PgConnectOptions = url.parse().unwrap();
    if let Some(port) = input["proxy_port"].as_u64() {
        connection = connection
            .host("127.0.0.1")
            .port(u16::try_from(port).unwrap());
    }
    PostgresStore::connect(
        connection.to_url_lossy().as_str(),
        PostgresStoreOptions::default()
            .with_transport_security(PostgresTransportSecurity::Disabled)
            .with_pool_size(1, 1)
            .with_acquire_timeout(Duration::from_secs(30))
            .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20)),
    )
    .await
    .expect("single-session qualification store must connect")
}

async fn attempt(store: &PostgresStore, scenario: &Scenario) -> Value {
    let activation = scenario.expected.activation();
    let run = store
        .load_run(activation.tenant_id(), activation.run_id())
        .await
        .unwrap();
    let append = worker_append(
        activation.tenant_id().clone(),
        activation.run_id(),
        EventId::generate(),
        run.journal_head().unwrap().clone(),
        scenario.expected.fence().clone(),
    );
    let intent = PendingNodeResultIntent::new(
        activation.clone(),
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap()
    .with_child_join(scenario.join_head.clone())
    .unwrap();
    let result = store
        .succeed_node_attempt(append, &scenario.expected, intent, BudgetUsage::zero())
        .await
        .unwrap();
    json!({
        "outcome": if matches!(result, NodeAttemptCommitOutcome::Committed { .. }) {
            "committed"
        } else {
            "idempotent"
        },
        "event": result.event().head()
    })
}

#[tokio::test]
async fn child_join_consumption_commit_loss_worker() {
    let Ok(input) = std::env::var(INPUT_ENV) else {
        return;
    };
    watch_parent();
    let input: Value = serde_json::from_str(&input).unwrap();
    let scenario: Scenario = serde_json::from_value(input["scenario"].clone()).unwrap();
    let store = worker_store(&input).await;
    match input["role"].as_str().unwrap() {
        "cut" => {
            let _ = attempt(&store, &scenario).await;
            panic!("instrumented transaction returned before process termination");
        }
        "recover" => {
            let result = attempt(&store, &scenario).await;
            ready_and_park(json!({"result": result})).await;
        }
        _ => panic!("unknown COMMIT-loss worker role"),
    }
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    run: Value,
    join: Value,
    attempt: Value,
    result: Value,
    journal: JournalPage,
}

async fn snapshot(store: &PostgresStore, scenario: &Scenario) -> Snapshot {
    let activation = scenario.expected.activation();
    let run = store
        .load_run(activation.tenant_id(), activation.run_id())
        .await
        .unwrap();
    let join = store.load_child_join(activation).await.unwrap().unwrap();
    let attempt = store
        .load_node_attempt(
            activation.tenant_id(),
            &activation.run_id(),
            scenario.expected.attempt_id(),
        )
        .await
        .unwrap();
    let result = match store.load_pending_node_result(activation).await {
        Ok(result) => json!(result),
        Err(StoreError::PendingNodeResultNotFound) => Value::Null,
        Err(error) => panic!("pending result integrity failed: {error:?}"),
    };
    let journal = store
        .load_journal_page(
            activation.tenant_id(),
            activation.run_id(),
            None,
            JournalPageSize::new(100).unwrap(),
        )
        .await
        .unwrap();
    assert!(!journal.has_more());
    Snapshot {
        run: json!({
            "lifecycle": run.lifecycle(),
            "journal_head": run.journal_head(),
            "lease": run.lease(),
            "checkpoint": run.checkpoint().map(|checkpoint| json!({
                "id": checkpoint.checkpoint_id(),
                "superstep": checkpoint.superstep().get(),
                "digest": checkpoint.digest()
            }))
        }),
        join: json!({
            "registration": join.registration().head(),
            "head": join.head(),
            "consumed": join.consumed()
        }),
        attempt: json!(attempt),
        result,
        journal,
    }
}

async fn wait_for_backend_disconnect(pool: &sqlx_postgres::PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let gone: bool =
                query_scalar("SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
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
    .expect("owned disconnected backend must release its transaction");
}

fn verify_committed(before: &Snapshot, after: &Snapshot, scenario: &Scenario) {
    assert_eq!(
        after.journal.events().len(),
        before.journal.events().len() + 1
    );
    assert_ne!(after.run["journal_head"], before.run["journal_head"]);
    assert_eq!(after.run["checkpoint"], before.run["checkpoint"]);
    assert_eq!(after.join["head"], before.join["head"]);
    assert!(!after.join["consumed"].is_null());
    assert!(!after.attempt["completion"].is_null());
    assert!(!after.result.is_null());
    assert_eq!(
        after.result["intent"]["child_join"],
        json!(scenario.join_head)
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_join_consumption_commit_loss_is_recoverable() {
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
    let connection = connection.unwrap();
    let pool = sql_pool().await;
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    for cut in [Cut::BeforeCommit, Cut::CommitResponse] {
        let (value, request, child) = setup_join(&store, "join-consumption-commit-loss").await;
        let activation = request.activation();
        store
            .register_child_join(
                request.clone(),
                &value.node,
                parent_append(&value, "child-join-registered"),
            )
            .await
            .unwrap();
        settle(&store, &value, child).await;
        let published = store
            .publish_child_join(&request, publish_append(&store, &request).await)
            .await
            .unwrap();
        let join_head = published.record().head().unwrap().clone();
        let fence = store
            .claim_lease(
                activation.tenant_id(),
                activation.run_id(),
                AttemptId::generate(),
            )
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        let run = store
            .load_run(activation.tenant_id(), activation.run_id())
            .await
            .unwrap();
        let started = store
            .start_node_attempt(
                worker_append(
                    activation.tenant_id().clone(),
                    activation.run_id(),
                    EventId::generate(),
                    run.journal_head().unwrap().clone(),
                    fence,
                ),
                activation.clone(),
                AttemptId::generate(),
            )
            .await
            .unwrap();
        let NodeAttemptCommitOutcome::Committed { attempt, .. } = started else {
            panic!("fresh node attempt required");
        };
        let scenario = Scenario {
            expected: attempt.start().head(),
            join_head,
        };
        let before = snapshot(&store, &scenario).await;
        assert!(before.join["consumed"].is_null());
        assert!(before.result.is_null());
        let mut proxy =
            CommitProxy::start(&connection, cut, TransactionTarget::ChildJoinConsumption).await;
        let mut interrupted = TestProcess::spawn(
            WORKER,
            &json!({"role": "cut", "scenario": scenario, "proxy_port": proxy.port()}),
        );
        proxy.wait_cut(cut).await;
        let pid = proxy.backend_pid();
        let at_cut = snapshot(&store, &scenario).await;
        if cut == Cut::BeforeCommit {
            assert_eq!(at_cut, before, "unforwarded COMMIT leaked Join consumption");
        } else {
            verify_committed(&before, &at_cut, &scenario);
        }
        interrupted.kill_and_reap().await;
        proxy.stop().await;
        wait_for_backend_disconnect(&pool, pid).await;
        if cut == Cut::BeforeCommit {
            assert_eq!(snapshot(&store, &scenario).await, before);
        }
        let mut recovery =
            TestProcess::spawn(WORKER, &json!({"role": "recover", "scenario": scenario}));
        let recovered = recovery.ready().await.unwrap();
        assert_eq!(
            recovered["result"]["outcome"],
            if cut == Cut::BeforeCommit {
                "committed"
            } else {
                "idempotent"
            }
        );
        recovery.kill_and_reap().await;
        let after = snapshot(&store, &scenario).await;
        verify_committed(&before, &after, &scenario);
        if cut == Cut::CommitResponse {
            assert_eq!(after, at_cut, "recovery changed committed Join evidence");
            assert_eq!(recovered["result"]["event"], after.run["journal_head"]);
        }
        let rebuilt = declared_parent(&driver_fixture(), value.intent.child().descriptor());
        let run = store
            .load_run(activation.tenant_id(), activation.run_id())
            .await
            .unwrap();
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
                .load_current_checkpoint(activation.tenant_id(), activation.run_id())
                .await
                .unwrap()
                .unwrap()
                .superstep()
                .get()
                > 0
        );
        println!(
            "\nSTATEKNOT_CHILD_JOIN_CONSUMPTION_COMMIT_LOSS_EVIDENCE={}",
            json!({
                "profile": "child-join-consumption-commit-loss-v1",
                "cut": if cut == Cut::BeforeCommit { "commit_not_forwarded" } else { "commit_response_withheld" },
                "commit_forwarded": cut == Cut::CommitResponse,
                "commit_response_forwarded": false,
                "interrupted_writer_killed": true,
                "cold_recovery_verified": true,
                "original_event_recovered": cut == Cut::CommitResponse,
                "downstream_noninitial_checkpoint_verified": true,
                "invariants": "passed",
                "termination": if cfg!(unix) { "SIGKILL" } else { "Child::kill" },
                "postgres": version,
                "os": std::env::consts::OS,
                "arch": std::env::consts::ARCH
            })
        );
    }
    pool.close().await;
    store.close().await;
}
