// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Ambiguous-COMMIT qualification for the final parent failure-close transaction.

use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, ready_and_park, watch_parent},
};
use sqlx_core::{connection::ConnectOptions, query_scalar::query_scalar};
use sqlx_postgres::PgConnectOptions;
use stateknot_store_postgres::{JournalPage, StoredRun};

const WORKER: &str = "child_admission::ownership::failure_close::completion_commit_loss::failure_close_completion_commit_loss_worker";

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    key: ChildRunKey,
    child: RunId,
}

fn child_usage() -> BudgetUsage {
    BudgetUsage::builder()
        .input_tokens(TokenCount::new(11))
        .build()
        .unwrap()
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
    let run = store
        .load_run(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap();
    let candidate = EventId::generate();
    let append = JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            scenario.key.tenant_id().clone(),
            scenario.key.parent_run_id(),
            candidate,
            payload("run-failure-close-completed"),
        )
        .unwrap(),
    )
    .unwrap();
    let outcome = store.complete_run_failure_close(append).await.unwrap();
    let durable = store
        .load_run(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap();
    json!({
        "outcome": if matches!(outcome, RunFailureCloseOutcome::Committed(_)) {
            "committed"
        } else {
            "existing"
        },
        "candidate_event": candidate,
        "durable_event": durable.journal_head().unwrap().event_id(),
        "original_failure": outcome.record().failure(),
        "direct_usage": outcome.record().direct_usage(),
        "registration": outcome.record().registration().head(),
        "completed_at": outcome.record().completed_at()
    })
}

#[tokio::test]
async fn failure_close_completion_commit_loss_worker() {
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
            panic!("instrumented finalization returned before process termination");
        }
        "recover" => {
            let result = attempt(&store, &scenario).await;
            ready_and_park(json!({"result": result})).await;
        }
        _ => panic!("unknown failure-close completion COMMIT-loss worker role"),
    }
}

async fn journal(store: &PostgresStore, tenant: &TenantId, run: RunId) -> JournalPage {
    let page = store
        .load_journal_page(tenant, run, None, JournalPageSize::new(100).unwrap())
        .await
        .unwrap();
    assert!(!page.has_more());
    page
}

fn event_count(page: &JournalPage, kind: &str) -> usize {
    page.events()
        .iter()
        .filter(|event| event.payload().kind().as_str() == kind)
        .count()
}

fn run_value(run: &StoredRun) -> Value {
    json!({
        "lifecycle": run.lifecycle(),
        "journal_head": run.journal_head(),
        "lease": run.lease(),
        "scheduler_ready_at": run.scheduler_ready_at(),
        "scheduler_not_before": run.scheduler_not_before(),
        "wait_set_digest": run.wait_set_digest(),
        "unresolved_wait_count": run.unresolved_wait_count(),
        "checkpoint": run.checkpoint().map(|checkpoint| json!({
            "id": checkpoint.checkpoint_id(),
            "superstep": checkpoint.superstep(),
            "digest": checkpoint.digest()
        }))
    })
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    parent: Value,
    child: Value,
    close: Value,
    ownership: Value,
    cancellation: Value,
    account: Value,
    pending: Value,
    sql_projection: Value,
    parent_journal: JournalPage,
    child_journal: JournalPage,
}

// Independent nonlocking MVCC reads are intentional while the interrupted
// finalizer holds the parent row lock and its COMMIT is still unresolved.
async fn snapshot(
    store: &PostgresStore,
    pool: &sqlx_postgres::PgPool,
    scenario: &Scenario,
) -> Snapshot {
    let tenant = scenario.key.tenant_id();
    let parent = store
        .load_run(tenant, scenario.key.parent_run_id())
        .await
        .unwrap();
    let child = store.load_run(tenant, scenario.child).await.unwrap();
    let close: Value = query_scalar(
        "SELECT to_jsonb(close) FROM stateknot.run_failure_closes AS close WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.as_str())
    .bind(*scenario.key.parent_run_id().as_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    let ownership = store.load_child_run(&scenario.key).await.unwrap();
    let cancellation = store.load_child_cancellation(&scenario.key).await.unwrap();
    let close_rows: i64 = query_scalar(
        "SELECT count(*) FROM stateknot.run_failure_closes WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.as_str())
    .bind(*scenario.key.parent_run_id().as_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    let completed_rows: i64 = query_scalar(
        "SELECT count(*) FROM stateknot.run_failure_closes WHERE tenant_id=$1 AND run_id=$2 AND completed_at IS NOT NULL",
    )
    .bind(tenant.as_str())
    .bind(*scenario.key.parent_run_id().as_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    Snapshot {
        parent: run_value(&parent),
        child: run_value(&child),
        close,
        ownership: json!({
            "intent": ownership.intent(),
            "spawn": ownership.spawn(),
            "ancestors": ownership.ancestors(),
            "settlement": ownership.settlement()
        }),
        cancellation: json!({
            "parent_head": cancellation.parent_head(),
            "receipt": cancellation.receipt().map(|receipt| json!({
                "head": receipt.head(),
                "lifecycle": receipt.lifecycle(),
                "outcome": format!("{:?}", receipt.outcome())
            }))
        }),
        account: serde_json::to_value(
            store
                .load_child_budget_account(tenant, scenario.key.parent_run_id())
                .await
                .unwrap(),
        )
        .unwrap(),
        pending: json!({
            "failure_closes": store.pending_run_failure_closes_after(tenant, None).await.unwrap().len(),
            "cancellations": store.pending_child_cancellations_after(tenant, None).await.unwrap(),
            "settlements": store.pending_child_settlements_after(tenant, None).await.unwrap()
        }),
        sql_projection: json!({"close_rows": close_rows, "completed_rows": completed_rows}),
        parent_journal: journal(store, tenant, scenario.key.parent_run_id()).await,
        child_journal: journal(store, tenant, scenario.child).await,
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
    .expect("owned disconnected backend must release the finalization transaction");
}

async fn prepare(store: &PostgresStore, suffix: &str) -> Scenario {
    let mut value = Box::pin(started(store, suffix)).await;
    let key = value.intent.key().clone();
    let committed = Box::pin(spawn(store, &value)).await.unwrap();
    assert!(matches!(committed, ChildRunCommitOutcome::Committed(_)));
    let child = committed
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    Box::pin(finish_node(store, &mut value)).await;
    assert!(matches!(
        Box::pin(request(store, &value, direct_usage()))
            .await
            .unwrap(),
        RunFailureCloseOutcome::Committed(_)
    ));
    let delivered = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(delivered.items().len(), 1);
    delivered.items()[0].result().unwrap();
    cancellation::confirm(store, key.tenant_id(), child, child_usage())
        .await
        .unwrap();
    let settled = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(settled.items().len(), 1);
    settled.items()[0].result().unwrap();
    assert!(
        store
            .load_child_run(&key)
            .await
            .unwrap()
            .settlement()
            .is_some()
    );
    assert_eq!(
        store
            .pending_run_failure_closes_after(key.tenant_id(), None)
            .await
            .unwrap()
            .len(),
        1
    );
    Scenario { key, child }
}

async fn verify_committed(store: &PostgresStore, scenario: &Scenario, value: &Snapshot) {
    let parent = store
        .load_run(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap();
    let close = store
        .load_run_failure_close(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent.lifecycle().status(), RunStatus::Failed);
    assert!(parent.lease().is_none());
    assert_eq!(close.completed_at(), Some(parent.lifecycle().changed_at()));
    assert_eq!(close.direct_usage(), &direct_usage());
    assert_eq!(
        serde_json::to_value(parent.lifecycle().terminal_failure().unwrap()).unwrap(),
        serde_json::to_value(close.failure()).unwrap()
    );
    assert_eq!(
        parent.lifecycle().terminal_usage().unwrap(),
        &direct_usage().checked_accumulate(&child_usage()).unwrap()
    );
    assert_eq!(
        value.sql_projection,
        json!({"close_rows": 1, "completed_rows": 1})
    );
    assert_eq!(value.pending["failure_closes"], 0);
    assert_eq!(value.pending["cancellations"], json!([]));
    assert_eq!(value.pending["settlements"], json!([]));
    assert_eq!(
        event_count(&value.parent_journal, "run-failure-close-requested"),
        1
    );
    assert_eq!(event_count(&value.parent_journal, "child-run-settled"), 1);
    assert_eq!(
        event_count(&value.parent_journal, "run-failure-close-completed"),
        1
    );
    assert_eq!(
        value.parent_journal.events().last().unwrap().event_id(),
        parent.journal_head().unwrap().event_id()
    );
    assert!(
        store
            .load_child_run(&scenario.key)
            .await
            .unwrap()
            .settlement()
            .is_some()
    );
    assert_eq!(
        store
            .load_child_budget_account(scenario.key.tenant_id(), scenario.key.parent_run_id())
            .await
            .unwrap()
            .unwrap()
            .delegated_usage()
            .unwrap(),
        child_usage()
    );
    assert!(
        tick(store, scenario.key.tenant_id())
            .await
            .items()
            .is_empty()
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failure_close_completion_commit_loss_is_recoverable() {
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
        let scenario = Box::pin(prepare(
            &store,
            if cut == Cut::BeforeCommit {
                "failure-close-completion-not-forwarded"
            } else {
                "failure-close-completion-response-withheld"
            },
        ))
        .await;
        let before = Box::pin(snapshot(&store, &pool, &scenario)).await;
        let original = store
            .load_run_failure_close(scenario.key.tenant_id(), scenario.key.parent_run_id())
            .await
            .unwrap()
            .unwrap();
        let original_failure = serde_json::to_value(original.failure()).unwrap();
        let original_registration = serde_json::to_value(original.registration().head()).unwrap();
        assert_eq!(
            before.sql_projection,
            json!({"close_rows": 1, "completed_rows": 0})
        );
        assert_eq!(before.pending["failure_closes"], 1);
        assert_eq!(
            event_count(&before.parent_journal, "run-failure-close-completed"),
            0
        );
        let mut proxy =
            CommitProxy::start(&connection, cut, TransactionTarget::FailureCloseCompletion).await;
        let mut interrupted = TestProcess::spawn(
            WORKER,
            &json!({"role": "cut", "scenario": scenario, "proxy_port": proxy.port()}),
        );
        proxy.wait_cut(cut).await;
        let pid = proxy.backend_pid();
        let at_cut = Box::pin(snapshot(&store, &pool, &scenario)).await;
        if cut == Cut::BeforeCommit {
            assert_eq!(
                at_cut, before,
                "unforwarded COMMIT leaked parent terminal evidence"
            );
        } else {
            assert_ne!(at_cut, before, "forwarded COMMIT did not become visible");
            Box::pin(verify_committed(&store, &scenario, &at_cut)).await;
        }
        interrupted.kill_and_reap().await;
        proxy.stop().await;
        wait_for_backend_disconnect(&pool, pid).await;
        if cut == Cut::BeforeCommit {
            assert_eq!(
                Box::pin(snapshot(&store, &pool, &scenario)).await,
                before,
                "disconnect did not roll back terminal event, run projection and close marker"
            );
        }
        let mut recovery =
            TestProcess::spawn(WORKER, &json!({"role": "recover", "scenario": scenario}));
        let recovered = recovery.ready().await.unwrap();
        assert_eq!(
            recovered["result"]["outcome"],
            if cut == Cut::BeforeCommit {
                "committed"
            } else {
                "existing"
            }
        );
        assert_eq!(recovered["result"]["direct_usage"], json!(direct_usage()));
        assert_eq!(recovered["result"]["original_failure"], original_failure);
        assert_eq!(recovered["result"]["registration"], original_registration);
        assert!(recovered["result"]["completed_at"].is_string());
        assert_eq!(
            recovered["result"]["candidate_event"] == recovered["result"]["durable_event"],
            cut == Cut::BeforeCommit
        );
        recovery.kill_and_reap().await;
        let after = Box::pin(snapshot(&store, &pool, &scenario)).await;
        Box::pin(verify_committed(&store, &scenario, &after)).await;
        if cut == Cut::CommitResponse {
            assert_eq!(after, at_cut, "replay changed immutable terminal evidence");
        }
        let replay = Box::pin(attempt(&store, &scenario)).await;
        assert_eq!(replay["outcome"], "existing");
        assert_ne!(replay["candidate_event"], replay["durable_event"]);
        assert_eq!(Box::pin(snapshot(&store, &pool, &scenario)).await, after);
        println!(
            "\nSTATEKNOT_PARENT_FINALIZATION_COMMIT_LOSS_EVIDENCE={}",
            json!({
                "profile": "parent-finalization-commit-loss-v1",
                "cut": if cut == Cut::BeforeCommit { "commit_not_forwarded" } else { "commit_response_withheld" },
                "commit_forwarded": cut == Cut::CommitResponse,
                "commit_response_forwarded": false,
                "interrupted_writer_killed": true,
                "cold_recovery_verified": true,
                "original_failure_and_event_verified": true,
                "terminal_accounting_verified": true,
                "close_record_and_journal_atomicity_verified": true,
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
