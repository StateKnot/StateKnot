// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Ambiguous-COMMIT qualification for atomic child admission and reservation.

use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, ready_and_park, watch_parent},
};
use sqlx_core::{connection::ConnectOptions, query_scalar::query_scalar};
use sqlx_postgres::PgConnectOptions;
use stateknot_core::{ChildRunAdmissionIntent, JournalHead, NodeAttemptStartHead};
use stateknot_store_postgres::{ChildRunCommitOutcome, JournalPage};

const WORKER: &str =
    "child_admission::ownership::child_admission_commit_loss::child_admission_commit_loss_worker";

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    intent: ChildRunAdmissionIntent,
    node: NodeAttemptStartHead,
    parent_head: JournalHead,
}

impl Scenario {
    fn key(&self) -> &ChildRunKey {
        self.intent.key()
    }

    fn child(&self) -> RunId {
        self.intent.child().provenance().run_id()
    }
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

fn admission_parent_append(scenario: &Scenario) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(scenario.parent_head.clone()),
        JournalEventIntent::worker(
            scenario.key().tenant_id().clone(),
            scenario.key().parent_run_id(),
            EventId::generate(),
            scenario.node.fence().clone(),
            payload("child-run-admitted"),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn attempt(store: &PostgresStore, scenario: &Scenario) -> Value {
    let parent_append = admission_parent_append(scenario);
    let (child_append, child_checkpoint) = child_write(&scenario.intent);
    let candidate_parent_event = parent_append.intent().event_id();
    let candidate_child_event = child_append.intent().event_id();
    let candidate_checkpoint = child_checkpoint.checkpoint_id();
    let fixture = driver_fixture();
    let outcome = Box::pin(store.admit_child_run(
        scenario.intent.clone(),
        &scenario.node,
        parent_append,
        child_append,
        child_checkpoint,
        BudgetUsage::zero(),
        fixture.registry.schemas(),
    ))
    .await
    .unwrap();
    let record = outcome.record();
    json!({
        "outcome": if matches!(outcome, ChildRunCommitOutcome::Committed(_)) {
            "committed"
        } else {
            "idempotent"
        },
        "candidate": {
            "parent_event": candidate_parent_event,
            "child_event": candidate_child_event,
            "checkpoint": candidate_checkpoint
        },
        "durable": {
            "parent_event": record.spawn().head().event_id(),
            "child_event": record.child().event().head().event_id(),
            "checkpoint": record.child().checkpoint().checkpoint_id(),
            "child_run": record.child().admission().intent().provenance().run_id(),
            "admission_digest": record.child().admission().digest()
        }
    })
}

#[tokio::test]
async fn child_admission_commit_loss_worker() {
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
        _ => panic!("unknown child-admission COMMIT-loss worker role"),
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

#[derive(Debug, PartialEq)]
struct Snapshot {
    parent: Value,
    parent_checkpoint: Value,
    child: Value,
    ownership: Value,
    account: Value,
    child_identities: Value,
    pending_settlements: Value,
    parent_journal: JournalPage,
    child_journal: Option<JournalPage>,
}

async fn snapshot(store: &PostgresStore, scenario: &Scenario) -> Snapshot {
    let key = scenario.key();
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let parent_checkpoint = store
        .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let child = match store
        .load_agent_admission(key.tenant_id(), scenario.child())
        .await
    {
        Ok(child) => json!({
            "admission": child.admission(),
            "run": {
                "lifecycle": child.run().lifecycle(),
                "journal_head": child.run().journal_head(),
                "lease": child.run().lease(),
                "scheduler_ready_at": child.run().scheduler_ready_at(),
                "scheduler_not_before": child.run().scheduler_not_before(),
                "wait_set_digest": child.run().wait_set_digest()
            },
            "event": child.event(),
            "checkpoint": child.checkpoint()
        }),
        Err(StoreError::AgentAdmissionNotFound) => Value::Null,
        Err(error) => panic!("unexpected child admission read failure: {error:?}"),
    };
    let ownership = match store.load_child_run(key).await {
        Ok(record) => json!({
            "intent": record.intent(),
            "spawn": record.spawn(),
            "ancestors": record.ancestors(),
            "child_admission_digest": record.child().admission().digest(),
            "child_checkpoint": record.child().checkpoint(),
            "settlement": record.settlement()
        }),
        Err(StoreError::ChildRunNotFound) => Value::Null,
        Err(error) => panic!("unexpected child ownership read failure: {error:?}"),
    };
    let child_journal = if child.is_null() {
        None
    } else {
        Some(journal(store, key.tenant_id(), scenario.child()).await)
    };
    Snapshot {
        parent: json!({
            "lifecycle": parent.lifecycle(),
            "journal_head": parent.journal_head(),
            "lease": parent.lease(),
            "scheduler_ready_at": parent.scheduler_ready_at(),
            "scheduler_not_before": parent.scheduler_not_before(),
            "wait_set_digest": parent.wait_set_digest()
        }),
        parent_checkpoint: serde_json::to_value(parent_checkpoint).unwrap(),
        child,
        ownership,
        account: serde_json::to_value(
            store
                .load_child_budget_account(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap(),
        )
        .unwrap(),
        child_identities: serde_json::to_value(
            store
                .list_child_run_identities(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap(),
        )
        .unwrap(),
        pending_settlements: serde_json::to_value(
            store
                .pending_child_settlements_after(key.tenant_id(), None)
                .await
                .unwrap(),
        )
        .unwrap(),
        parent_journal: journal(store, key.tenant_id(), key.parent_run_id()).await,
        child_journal,
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

async fn verify_committed(store: &PostgresStore, scenario: &Scenario, snapshot: &Snapshot) {
    let key = scenario.key();
    assert_eq!(
        event_count(&snapshot.parent_journal, "child-run-admitted"),
        1
    );
    assert_eq!(
        event_count(
            snapshot.child_journal.as_ref().unwrap(),
            AgentAdmission::JOURNAL_EVENT_KIND
        ),
        1
    );
    assert_ne!(snapshot.child, Value::Null);
    assert_ne!(snapshot.ownership, Value::Null);
    assert_ne!(snapshot.account, Value::Null);
    assert_eq!(snapshot.pending_settlements, json!([]));
    let identities = store
        .list_child_run_identities(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(identities.len(), 1);
    assert_eq!(identities[0].1, scenario.child());
    let account = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
}

async fn verify_downstream(store: &PostgresStore, scenario: &Scenario) {
    let claimed = store
        .claim_lease(
            scenario.key().tenant_id(),
            scenario.child(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    store.release_lease(claimed.lease().fence()).await.unwrap();
    assert!(matches!(
        fail(
            store,
            scenario.key().tenant_id(),
            scenario.key().parent_run_id(),
            BudgetUsage::zero()
        )
        .await,
        Err(StoreError::UnsettledChildRuns)
    ));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_admission_commit_loss_is_recoverable() {
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
        let started = Box::pin(started(
            &store,
            if cut == Cut::BeforeCommit {
                "child-admission-commit-not-forwarded"
            } else {
                "child-admission-commit-response-withheld"
            },
        ))
        .await;
        let scenario = Scenario {
            intent: started.intent.clone(),
            node: started.node.clone(),
            parent_head: started.head.clone(),
        };
        let before = Box::pin(snapshot(&store, &scenario)).await;
        let mut proxy =
            CommitProxy::start(&connection, cut, TransactionTarget::ChildAdmission).await;
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
        let at_cut = Box::pin(snapshot(&store, &scenario)).await;
        if cut == Cut::BeforeCommit {
            assert_eq!(at_cut, before, "unforwarded COMMIT leaked admission state");
        } else {
            assert_ne!(at_cut, before, "forwarded COMMIT did not become visible");
            Box::pin(verify_committed(&store, &scenario, &at_cut)).await;
        }
        interrupted.kill_and_reap().await;
        proxy.stop().await;
        wait_for_backend_disconnect(&pool, pid).await;
        if cut == Cut::BeforeCommit {
            assert_eq!(
                Box::pin(snapshot(&store, &scenario)).await,
                before,
                "disconnect did not roll back the unforwarded admission"
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
        if cut == Cut::CommitResponse {
            assert_ne!(
                recovered["result"]["candidate"]["parent_event"],
                recovered["result"]["durable"]["parent_event"]
            );
            assert_ne!(
                recovered["result"]["candidate"]["child_event"],
                recovered["result"]["durable"]["child_event"]
            );
            assert_ne!(
                recovered["result"]["candidate"]["checkpoint"],
                recovered["result"]["durable"]["checkpoint"]
            );
        }
        recovery.kill_and_reap().await;
        let after = Box::pin(snapshot(&store, &scenario)).await;
        Box::pin(verify_committed(&store, &scenario, &after)).await;
        if cut == Cut::CommitResponse {
            assert_eq!(
                after, at_cut,
                "idempotent recovery replaced committed admission evidence"
            );
        }
        Box::pin(verify_downstream(&store, &scenario)).await;
        println!(
            "\nSTATEKNOT_CHILD_ADMISSION_COMMIT_LOSS_EVIDENCE={}",
            json!({
                "profile": "child-admission-commit-loss-v1",
                "cut": if cut == Cut::BeforeCommit {
                    "commit_not_forwarded"
                } else {
                    "commit_response_withheld"
                },
                "commit_forwarded": cut == Cut::CommitResponse,
                "commit_response_forwarded": false,
                "interrupted_writer_killed": true,
                "cold_recovery_verified": true,
                "original_identity_recovery_verified": true,
                "downstream_progress_verified": true,
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
