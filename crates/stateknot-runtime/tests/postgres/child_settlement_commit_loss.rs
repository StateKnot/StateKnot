// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Ambiguous-COMMIT qualification for immutable child settlement and parent accounting.

use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, ready_and_park, watch_parent},
};
use sqlx_core::{connection::ConnectOptions, query_as::query_as, query_scalar::query_scalar};
use sqlx_postgres::PgConnectOptions;
use stateknot_store_postgres::{JournalPage, StoredRun};

const WORKER: &str =
    "child_admission::ownership::child_settlement_commit_loss::child_settlement_commit_loss_worker";

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    key: ChildRunKey,
    child: RunId,
    terminal: JournalHead,
    usage: BudgetUsage,
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
    let append = settlement_append(store, &scenario.key).await;
    let candidate = append.intent().event_id();
    let outcome = Box::pin(store.settle_child_run(&scenario.key, append))
        .await
        .unwrap();
    let (kind, event, record) = match outcome {
        ChildRunSettlementOutcome::Committed { event, record } => ("committed", event, record),
        ChildRunSettlementOutcome::Idempotent { event, record } => ("idempotent", event, record),
        other => panic!("unexpected child settlement outcome: {other:?}"),
    };
    let settlement = record.settlement().unwrap();
    json!({
        "outcome": kind,
        "candidate_event": candidate,
        "durable_event": event.event_id(),
        "terminal": settlement.terminal(),
        "usage": settlement.usage()
    })
}

#[tokio::test]
async fn child_settlement_commit_loss_worker() {
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
            panic!("instrumented settlement returned before process termination");
        }
        "recover" => {
            let result = attempt(&store, &scenario).await;
            ready_and_park(json!({"result": result})).await;
        }
        _ => panic!("unknown child-settlement COMMIT-loss worker role"),
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
    ownership: Value,
    account: Value,
    pending_settlements: Value,
    sql_projection: Value,
    parent_journal: JournalPage,
    child_journal: JournalPage,
}

async fn snapshot(
    store: &PostgresStore,
    pool: &sqlx_postgres::PgPool,
    scenario: &Scenario,
) -> Snapshot {
    let parent = store
        .load_run(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap();
    let child = store
        .load_run(scenario.key.tenant_id(), scenario.child)
        .await
        .unwrap();
    let owned = store.load_child_run(&scenario.key).await.unwrap();
    let projection: (bool, bool, i64) = query_as(
        "SELECT owner.settled, owner.terminal_pending_at IS NOT NULL, \
         (SELECT count(*) FROM stateknot.child_run_settlements AS settlement \
         WHERE settlement.tenant_id=owner.tenant_id \
         AND settlement.parent_run_id=owner.parent_run_id \
         AND settlement.key_digest=owner.key_digest) \
         FROM stateknot.child_run_ownership AS owner \
         WHERE owner.tenant_id=$1 AND owner.parent_run_id=$2 AND owner.key_digest=$3",
    )
    .bind(scenario.key.tenant_id().as_str())
    .bind(*scenario.key.parent_run_id().as_uuid())
    .bind(scenario.key.digest().as_bytes())
    .fetch_one(pool)
    .await
    .unwrap();
    Snapshot {
        parent: run_value(&parent),
        child: run_value(&child),
        ownership: json!({
            "intent": owned.intent(),
            "spawn": owned.spawn(),
            "ancestors": owned.ancestors(),
            "settlement": owned.settlement()
        }),
        account: serde_json::to_value(
            store
                .load_child_budget_account(scenario.key.tenant_id(), scenario.key.parent_run_id())
                .await
                .unwrap(),
        )
        .unwrap(),
        pending_settlements: serde_json::to_value(
            store
                .pending_child_settlements_after(scenario.key.tenant_id(), None)
                .await
                .unwrap(),
        )
        .unwrap(),
        sql_projection: json!(projection),
        parent_journal: journal(
            store,
            scenario.key.tenant_id(),
            scenario.key.parent_run_id(),
        )
        .await,
        child_journal: journal(store, scenario.key.tenant_id(), scenario.child).await,
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

async fn prepare(store: &PostgresStore, suffix: &str) -> Scenario {
    let started = Box::pin(started(store, suffix)).await;
    let outcome = Box::pin(spawn(store, &started)).await.unwrap();
    assert!(matches!(outcome, ChildRunCommitOutcome::Committed(_)));
    let key = started.intent.key().clone();
    let child = outcome
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    let usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(7))
        .tool_calls(ExecutionCount::new(2))
        .build()
        .unwrap();
    fail(store, key.tenant_id(), child, usage.clone())
        .await
        .unwrap();
    let terminal = store
        .load_run(key.tenant_id(), child)
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let audit = JournalAppend::new(
        JournalExpectation::exact(terminal.clone()),
        JournalEventIntent::control_plane(
            key.tenant_id().clone(),
            child,
            EventId::generate(),
            payload("post-terminal-audit"),
        )
        .unwrap(),
    )
    .unwrap();
    store
        .append_control_plane(audit, RunProjection::unchanged())
        .await
        .unwrap();
    assert_eq!(
        store
            .pending_child_settlements(key.tenant_id())
            .await
            .unwrap(),
        vec![key.clone()]
    );
    Scenario {
        key,
        child,
        terminal,
        usage,
    }
}

async fn verify_committed(store: &PostgresStore, scenario: &Scenario, value: &Snapshot) {
    let record = store.load_child_run(&scenario.key).await.unwrap();
    let settlement = record.settlement().unwrap();
    assert_eq!(settlement.terminal(), &scenario.terminal);
    assert_eq!(settlement.usage(), &scenario.usage);
    let account = store
        .load_child_budget_account(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    assert_eq!(account.children()[0].settlement(), Some(settlement));
    assert_eq!(value.sql_projection, json!([true, false, 1]));
    assert_eq!(value.pending_settlements, json!([]));
    assert_eq!(event_count(&value.parent_journal, "child-run-settled"), 1);
    assert_eq!(event_count(&value.child_journal, "test-failed"), 1);
    assert_eq!(event_count(&value.child_journal, "post-terminal-audit"), 1);
}

async fn verify_downstream(store: &PostgresStore, scenario: &Scenario) {
    assert!(matches!(
        fail(
            store,
            scenario.key.tenant_id(),
            scenario.key.parent_run_id(),
            BudgetUsage::zero(),
        )
        .await,
        Err(StoreError::IncompleteChildAccounting)
    ));
    let total = store
        .include_child_usage(
            scenario.key.tenant_id(),
            scenario.key.parent_run_id(),
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    assert_eq!(total, scenario.usage);
    fail(
        store,
        scenario.key.tenant_id(),
        scenario.key.parent_run_id(),
        total,
    )
    .await
    .unwrap();
    assert_eq!(
        store
            .load_run(scenario.key.tenant_id(), scenario.key.parent_run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Failed
    );
    assert_eq!(
        store
            .load_child_run(&scenario.key)
            .await
            .unwrap()
            .settlement()
            .unwrap()
            .usage(),
        &scenario.usage
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_settlement_commit_loss_is_recoverable() {
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
                "child-settlement-commit-not-forwarded"
            } else {
                "child-settlement-commit-response-withheld"
            },
        ))
        .await;
        let before = Box::pin(snapshot(&store, &pool, &scenario)).await;
        assert_eq!(before.sql_projection, json!([false, true, 0]));
        assert_eq!(before.pending_settlements, json!([scenario.key]));
        assert_eq!(event_count(&before.parent_journal, "child-run-settled"), 0);
        assert_eq!(
            store
                .load_run(scenario.key.tenant_id(), scenario.child)
                .await
                .unwrap()
                .lifecycle()
                .status(),
            RunStatus::Failed
        );
        let mut proxy =
            CommitProxy::start(&connection, cut, TransactionTarget::ChildSettlement).await;
        let mut interrupted = TestProcess::spawn(
            WORKER,
            &json!({
                "role": "cut",
                "scenario": scenario,
                "proxy_port": proxy.port()
            }),
        );
        proxy.wait_cut(cut).await;
        let pid = proxy.backend_pid();
        let at_cut = Box::pin(snapshot(&store, &pool, &scenario)).await;
        if cut == Cut::BeforeCommit {
            assert_eq!(at_cut, before, "unforwarded COMMIT leaked settlement");
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
                "disconnect did not roll back settlement and account"
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
                "idempotent"
            }
        );
        assert_eq!(recovered["result"]["terminal"], json!(scenario.terminal));
        assert_eq!(recovered["result"]["usage"], json!(scenario.usage));
        if cut == Cut::BeforeCommit {
            assert_eq!(
                recovered["result"]["candidate_event"],
                recovered["result"]["durable_event"]
            );
        } else {
            assert_ne!(
                recovered["result"]["candidate_event"],
                recovered["result"]["durable_event"]
            );
        }
        recovery.kill_and_reap().await;
        let after = Box::pin(snapshot(&store, &pool, &scenario)).await;
        Box::pin(verify_committed(&store, &scenario, &after)).await;
        if cut == Cut::CommitResponse {
            assert_eq!(
                after, at_cut,
                "idempotent recovery changed settlement or accounting evidence"
            );
        }
        Box::pin(verify_downstream(&store, &scenario)).await;
        println!(
            "\nSTATEKNOT_CHILD_SETTLEMENT_COMMIT_LOSS_EVIDENCE={}",
            json!({
                "profile": "child-settlement-commit-loss-v1",
                "cut": if cut == Cut::BeforeCommit {
                    "commit_not_forwarded"
                } else {
                    "commit_response_withheld"
                },
                "commit_forwarded": cut == Cut::CommitResponse,
                "commit_response_forwarded": false,
                "interrupted_writer_killed": true,
                "cold_recovery_verified": true,
                "original_event_recovery_verified": true,
                "terminal_anchor_verified": true,
                "accounting_and_notification_verified": true,
                "parent_finalization_verified": true,
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
