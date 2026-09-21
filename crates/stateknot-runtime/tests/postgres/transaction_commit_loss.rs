// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! COMMIT-loss qualification for Join registration/publication and deadline cancellation.

use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, ready_and_park, watch_parent},
};
use sqlx_core::{connection::ConnectOptions, query_scalar::query_scalar};
use sqlx_postgres::PgConnectOptions;
use stateknot_core::{ChildRunJoinRequest, JournalHead, NodeAttemptStartHead};
use stateknot_store_postgres::{
    AgentDeadlineCancellationOutcome, ChildJoinCommitOutcome, ChildJoinRecord, JournalPage,
};

const WORKER: &str =
    "child_admission::ownership::transaction_commit_loss::join_deadline_commit_loss_worker";

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    JoinRegistration,
    JoinPublication,
    DeadlineCancellation,
}

impl Operation {
    const ALL: [Self; 3] = [
        Self::JoinRegistration,
        Self::JoinPublication,
        Self::DeadlineCancellation,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::JoinRegistration => "join_registration",
            Self::JoinPublication => "join_publication",
            Self::DeadlineCancellation => "deadline_cancellation",
        }
    }

    const fn target(self) -> TransactionTarget {
        match self {
            Self::JoinRegistration => TransactionTarget::ChildJoinRegistration,
            Self::JoinPublication => TransactionTarget::ChildJoinPublication,
            Self::DeadlineCancellation => TransactionTarget::AgentDeadlineCancellation,
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    operation: Operation,
    key: ChildRunKey,
    node: NodeAttemptStartHead,
    precommit_head: JournalHead,
}

struct Prepared {
    scenario: Scenario,
    started: Started,
    child: RunId,
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

fn registration_append(scenario: &Scenario) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(scenario.precommit_head.clone()),
        JournalEventIntent::worker(
            scenario.key.tenant_id().clone(),
            scenario.key.parent_run_id(),
            EventId::generate(),
            scenario.node.fence().clone(),
            payload("child-join-registered"),
        )
        .unwrap(),
    )
    .unwrap()
}

fn publication_append(scenario: &Scenario) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(scenario.precommit_head.clone()),
        JournalEventIntent::control_plane(
            scenario.key.tenant_id().clone(),
            scenario.key.parent_run_id(),
            EventId::generate(),
            payload("child-join-published"),
        )
        .unwrap(),
    )
    .unwrap()
}

async fn attempt(store: &PostgresStore, scenario: &Scenario) -> Value {
    let request = ChildRunJoinRequest::new([scenario.key.clone()]).unwrap();
    match scenario.operation {
        Operation::JoinRegistration => {
            let outcome = store
                .register_child_join(request, &scenario.node, registration_append(scenario))
                .await
                .unwrap();
            json!({
                "outcome": if matches!(outcome, ChildJoinCommitOutcome::Committed(_)) {
                    "committed"
                } else {
                    "idempotent"
                },
                "journal": outcome.record().registration().head()
            })
        }
        Operation::JoinPublication => {
            let outcome = store
                .publish_child_join(&request, publication_append(scenario))
                .await
                .unwrap();
            json!({
                "outcome": if matches!(outcome, ChildJoinCommitOutcome::Committed(_)) {
                    "committed"
                } else {
                    "idempotent"
                },
                "journal": outcome.record().head()
            })
        }
        Operation::DeadlineCancellation => {
            let outcome = deadlines::request_expiry(
                store,
                scenario.key.tenant_id(),
                scenario.key.parent_run_id(),
            )
            .await
            .unwrap();
            match outcome {
                AgentDeadlineCancellationOutcome::Requested(head) => {
                    json!({"outcome": "requested", "journal": head})
                }
                AgentDeadlineCancellationOutcome::AlreadyRequested(request) => json!({
                    "outcome": "already_requested",
                    "failure": request.failure()
                }),
                other => panic!("deadline recovery did not converge: {other:?}"),
            }
        }
    }
}

#[tokio::test]
async fn join_deadline_commit_loss_worker() {
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
            ready_and_park(json!({"operation": scenario.operation.name(), "result": result})).await;
        }
        _ => panic!("unknown COMMIT-loss worker role"),
    }
}

fn join_value(record: Option<&ChildJoinRecord>) -> Value {
    record.map_or(Value::Null, |record| {
        json!({
            "request": record.request(),
            "registration": record.registration().head(),
            "binding": record.binding(),
            "head": record.head(),
            "consumed": record.consumed()
        })
    })
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
    child: Value,
    ownership: Value,
    join: Value,
    cancellation: Value,
    account: Value,
    attempts: Value,
    pending_joins: Value,
    pending_cancellations: Value,
    pending_settlements: Value,
    parent_journal: JournalPage,
    child_journal: JournalPage,
}

async fn snapshot(store: &PostgresStore, scenario: &Scenario) -> Snapshot {
    let key = &scenario.key;
    let owned = store.load_child_run(key).await.unwrap();
    let child_id = owned.child().admission().intent().provenance().run_id();
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let child = store.load_run(key.tenant_id(), child_id).await.unwrap();
    let join = store.load_child_join(key.parent()).await.unwrap();
    let cancellation = if parent.lifecycle().cancellation_request().is_some() {
        let record = store.load_child_cancellation(key).await.unwrap();
        json!({
            "parent_lifecycle": record.parent_lifecycle(),
            "parent_head": record.parent_head(),
            "queued_at": record.queued_at(),
            "receipt": record.receipt().map(|receipt| json!({
                "outcome": format!("{:?}", receipt.outcome()),
                "lifecycle": receipt.lifecycle(),
                "head": receipt.head(),
                "delivered_at": receipt.delivered_at()
            }))
        })
    } else {
        Value::Null
    };
    let history = store
        .load_node_attempt_history_page(
            key.parent(),
            None,
            NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    assert!(!history.has_more());
    Snapshot {
        parent: json!({
            "lifecycle": parent.lifecycle(),
            "cancellation_code": parent
                .lifecycle()
                .cancellation_request()
                .map(|request| request.failure().code().as_str()),
            "journal_head": parent.journal_head(),
            "lease": parent.lease(),
            "scheduler_ready_at": parent.scheduler_ready_at(),
            "scheduler_not_before": parent.scheduler_not_before(),
            "wait_set_digest": parent.wait_set_digest()
        }),
        child: json!({
            "lifecycle": child.lifecycle(),
            "journal_head": child.journal_head(),
            "lease": child.lease()
        }),
        ownership: json!({
            "intent": owned.intent(),
            "spawn": owned.spawn().head(),
            "ancestors": owned.ancestors(),
            "settlement": owned.settlement()
        }),
        join: join_value(join.as_ref()),
        cancellation,
        account: serde_json::to_value(
            store
                .load_child_budget_account(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap(),
        )
        .unwrap(),
        attempts: serde_json::to_value(history.records()).unwrap(),
        pending_joins: serde_json::to_value(
            store
                .pending_child_joins_after(key.tenant_id(), None)
                .await
                .unwrap(),
        )
        .unwrap(),
        pending_cancellations: serde_json::to_value(
            store
                .pending_child_cancellations_after(key.tenant_id(), None)
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
        child_journal: journal(store, key.tenant_id(), child_id).await,
    }
}

async fn prepare(store: &PostgresStore, operation: Operation) -> Prepared {
    match operation {
        Operation::JoinRegistration | Operation::JoinPublication => {
            let (started, request, child) =
                join::setup_join(store, &format!("{}-commit-loss", operation.name())).await;
            if matches!(operation, Operation::JoinPublication) {
                store
                    .register_child_join(
                        request,
                        &started.node,
                        parent_append(&started, "child-join-registered"),
                    )
                    .await
                    .unwrap();
                join::settle(store, &started, child).await;
            }
            let parent = store
                .load_run(
                    started.intent.key().tenant_id(),
                    started.intent.key().parent_run_id(),
                )
                .await
                .unwrap();
            Prepared {
                scenario: Scenario {
                    operation,
                    key: started.intent.key().clone(),
                    node: started.node.clone(),
                    precommit_head: parent.journal_head().unwrap().clone(),
                },
                started,
                child,
            }
        }
        Operation::DeadlineCancellation => {
            let deadline = deadlines::future_deadline(store, 2).await;
            let mut started = Box::pin(deadlines::started_deadline(
                store,
                &format!("{}-commit-loss", operation.name()),
                deadline,
            ))
            .await;
            let admitted = spawn(store, &started).await.unwrap();
            let child = admitted
                .record()
                .child()
                .admission()
                .intent()
                .provenance()
                .run_id();
            started.head = admitted.record().spawn().head();
            let request = ChildRunJoinRequest::new([started.intent.key().clone()]).unwrap();
            store
                .register_child_join(
                    request,
                    &started.node,
                    parent_append(&started, "child-join-registered"),
                )
                .await
                .unwrap();
            deadlines::await_due(store, deadline).await;
            let parent = store
                .load_run(
                    started.intent.key().tenant_id(),
                    started.intent.key().parent_run_id(),
                )
                .await
                .unwrap();
            Prepared {
                scenario: Scenario {
                    operation,
                    key: started.intent.key().clone(),
                    node: started.node.clone(),
                    precommit_head: parent.journal_head().unwrap().clone(),
                },
                started,
                child,
            }
        }
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

fn verify_committed(snapshot: &Snapshot, operation: Operation, key: &ChildRunKey) {
    match operation {
        Operation::JoinRegistration => {
            assert_eq!(
                event_count(&snapshot.parent_journal, "child-join-registered"),
                1
            );
            assert_ne!(snapshot.join, Value::Null);
            assert!(snapshot.parent["lease"].is_null());
        }
        Operation::JoinPublication => {
            assert_eq!(
                event_count(&snapshot.parent_journal, "child-join-published"),
                1
            );
            assert!(!snapshot.join["head"].is_null());
            assert_eq!(snapshot.pending_joins, json!([]));
        }
        Operation::DeadlineCancellation => {
            assert_eq!(
                event_count(
                    &snapshot.parent_journal,
                    "agent-deadline-cancellation-requested"
                ),
                1
            );
            assert_eq!(
                snapshot.parent["cancellation_code"],
                "agent.deadline.expired"
            );
            assert_eq!(snapshot.pending_cancellations, json!([key]));
            assert_ne!(snapshot.cancellation, Value::Null);
        }
    }
}

async fn verify_downstream(store: &PostgresStore, prepared: &Prepared) {
    let scenario = &prepared.scenario;
    let request = ChildRunJoinRequest::new([scenario.key.clone()]).unwrap();
    match scenario.operation {
        Operation::JoinRegistration => {
            join::settle(store, &prepared.started, prepared.child).await;
            let published = store
                .publish_child_join(&request, join::publish_append(store, &request).await)
                .await
                .unwrap();
            assert!(matches!(published, ChildJoinCommitOutcome::Committed(_)));
        }
        Operation::JoinPublication => {
            store
                .claim_lease(
                    scenario.key.tenant_id(),
                    scenario.key.parent_run_id(),
                    AttemptId::generate(),
                )
                .await
                .unwrap();
        }
        Operation::DeadlineCancellation => {
            let report = cancellation::reconciler(store)
                .tick(
                    scenario.key.tenant_id().clone(),
                    None,
                    CancellationSignal::never(),
                )
                .await
                .unwrap();
            assert_eq!(report.items().len(), 1);
            report.items()[0].result().unwrap();
            assert!(
                store
                    .pending_child_cancellations_after(scenario.key.tenant_id(), None)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn join_and_deadline_commit_loss_are_recoverable() {
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
    for operation in Operation::ALL {
        for cut in [Cut::BeforeCommit, Cut::CommitResponse] {
            let prepared = Box::pin(prepare(&store, operation)).await;
            let before = Box::pin(snapshot(&store, &prepared.scenario)).await;
            let mut proxy = CommitProxy::start(&connection, cut, operation.target()).await;
            let mut interrupted = TestProcess::spawn(
                WORKER,
                &json!({
                    "role": "cut",
                    "scenario": prepared.scenario,
                    "proxy_port": proxy.port()
                }),
            );
            proxy.wait_cut(cut).await;
            let pid = proxy.backend_pid();
            let at_cut = Box::pin(snapshot(&store, &prepared.scenario)).await;
            if cut == Cut::BeforeCommit {
                assert_eq!(at_cut, before, "unforwarded COMMIT leaked durable state");
            } else {
                assert_ne!(at_cut, before, "forwarded COMMIT did not become visible");
                verify_committed(&at_cut, operation, &prepared.scenario.key);
            }
            interrupted.kill_and_reap().await;
            proxy.stop().await;
            wait_for_backend_disconnect(&pool, pid).await;
            if cut == Cut::BeforeCommit {
                assert_eq!(
                    Box::pin(snapshot(&store, &prepared.scenario)).await,
                    before,
                    "disconnect did not roll back the unforwarded COMMIT"
                );
            }
            let mut recovery = TestProcess::spawn(
                WORKER,
                &json!({"role": "recover", "scenario": prepared.scenario}),
            );
            let recovered = recovery.ready().await.unwrap();
            assert_eq!(recovered["operation"], operation.name());
            let expected = match (operation, cut) {
                (Operation::DeadlineCancellation, Cut::BeforeCommit) => "requested",
                (Operation::DeadlineCancellation, Cut::CommitResponse) => "already_requested",
                (_, Cut::BeforeCommit) => "committed",
                (_, Cut::CommitResponse) => "idempotent",
            };
            assert_eq!(recovered["result"]["outcome"], expected);
            recovery.kill_and_reap().await;
            let after = Box::pin(snapshot(&store, &prepared.scenario)).await;
            verify_committed(&after, operation, &prepared.scenario.key);
            if cut == Cut::CommitResponse {
                assert_eq!(
                    after, at_cut,
                    "idempotent recovery changed committed evidence"
                );
            }
            Box::pin(verify_downstream(&store, &prepared)).await;
            println!(
                "\nSTATEKNOT_JOIN_DEADLINE_COMMIT_LOSS_EVIDENCE={}",
                json!({
                    "profile": "join-deadline-commit-loss-v1",
                    "operation": operation.name(),
                    "cut": if cut == Cut::BeforeCommit {
                        "commit_not_forwarded"
                    } else {
                        "commit_response_withheld"
                    },
                    "commit_forwarded": cut == Cut::CommitResponse,
                    "commit_response_forwarded": false,
                    "interrupted_writer_killed": true,
                    "cold_recovery_verified": true,
                    "downstream_progress_verified": true,
                    "invariants": "passed",
                    "termination": if cfg!(unix) { "SIGKILL" } else { "Child::kill" },
                    "postgres": version,
                    "os": std::env::consts::OS,
                    "arch": std::env::consts::ARCH
                })
            );
        }
    }
    pool.close().await;
    store.close().await;
}
