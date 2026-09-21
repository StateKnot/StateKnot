// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real-process qualification for deadline cancellation of a suspended child Join.

use super::*;
use crate::process_harness::{
    INPUT_ENV, TestProcess, parent_control, ready_and_park, watch_parent,
};
use stateknot_core::NodeAttemptStatus;
use stateknot_runtime::{ChildReconciliationCommit, ChildReconciliationKind};
use stateknot_store_postgres::{
    AgentDeadlineCancellationOutcome, ChildCancellationOutcome, ChildJoinRecord, ChildRunRecord,
    JournalPage,
};

const WORKER: &str = "child_admission::ownership::deadlines::process::deadline_join_process_worker";
const STALE_WORKER: &str =
    "child_admission::ownership::deadlines::process::deadline_join_stale_worker";

fn child_usage() -> BudgetUsage {
    BudgetUsage::builder()
        .input_tokens(TokenCount::new(7))
        .build()
        .unwrap()
}

async fn rebuilt_fixture(
    store: &PostgresStore,
    key: &ChildRunKey,
) -> (PreparationFixture, ChildRunAdmissionIntent) {
    let owned = store.load_child_run(key).await.unwrap();
    let intent = owned.intent().clone();
    let child_driver = driver_fixture();
    assert_eq!(child_driver.graph, *intent.child_graph());
    let driver = declared_parent(&child_driver, intent.child().descriptor());
    assert_eq!(
        driver.graph.reference(),
        key.parent().base_checkpoint().graph().clone()
    );
    let parent = store
        .load_agent_admission(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let facade = DurableAgentAdmission::new(store.clone(), driver.registry.clone()).unwrap();
    (
        PreparationFixture {
            driver,
            child_driver,
            facade,
            parent,
        },
        intent,
    )
}

async fn physical_attempt(
    store: &PostgresStore,
    activation: &NodeActivation,
) -> stateknot_core::NodeAttempt {
    let page = store
        .load_node_attempt_history_page(
            activation,
            None,
            NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    page.records()
        .iter()
        .find(|attempt| attempt.status() == NodeAttemptStatus::Executing)
        .cloned()
        .expect("suspended parent attempt must exist")
}

async fn register_join(store: &PostgresStore, key: &ChildRunKey) {
    let request = ChildRunJoinRequest::new([key.clone()]).unwrap();
    let attempt = physical_attempt(store, key.parent()).await;
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::worker(
            key.tenant_id().clone(),
            key.parent_run_id(),
            EventId::generate(),
            attempt.start().fence().clone(),
            payload("child-join-registered"),
        )
        .unwrap(),
    )
    .unwrap();
    store
        .register_child_join(request, &attempt.start().head(), append)
        .await
        .unwrap();
}

async fn run_child_cleanup(store: &PostgresStore, key: &ChildRunKey) {
    let (fixture, intent) = Box::pin(rebuilt_fixture(store, key)).await;
    let owned = store.load_child_run(key).await.unwrap();
    let child = intent.child().provenance().run_id();
    let lease = store
        .claim_lease(key.tenant_id(), child, AttemptId::generate())
        .await
        .unwrap();
    let result = cleanup_loop(
        store,
        fixture.child_driver.registry,
        owned.child().admission(),
        child_usage(),
    )
    .run(lease.lease().fence().clone(), CancellationSignal::never())
    .await
    .unwrap();
    assert!(matches!(
        result.outcome(),
        AgentLoopOutcome::CancellationConfirmed(_)
    ));
}

async fn run_parent_cleanup(store: &PostgresStore, key: &ChildRunKey) {
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let fence = run.lease().unwrap().fence().clone();
    let (fixture, _) = Box::pin(rebuilt_fixture(store, key)).await;
    let result = cleanup_loop(
        store,
        fixture.driver.registry,
        fixture.parent.admission(),
        BudgetUsage::zero(),
    )
    .run(fence, CancellationSignal::never())
    .await
    .unwrap();
    assert!(matches!(
        result.outcome(),
        AgentLoopOutcome::CancellationConfirmed(_)
    ));
}

#[tokio::test]
async fn deadline_join_process_worker() {
    let Ok(input) = std::env::var(INPUT_ENV) else {
        return;
    };
    watch_parent();
    let input: Value = serde_json::from_str(&input).unwrap();
    let store = test_store()
        .await
        .expect("process qualification requires PostgreSQL");
    let phase = input["phase"].as_str().unwrap();
    let key = if phase == "admission" {
        let deadline = future_deadline(&store, 10).await;
        let name = format!("deadline-process-{}", uuid::Uuid::now_v7());
        let value = Box::pin(started_deadline(&store, &name, deadline)).await;
        let committed = spawn(&store, &value).await.unwrap();
        committed.record().intent().key().clone()
    } else {
        serde_json::from_value(input["key"].clone()).unwrap()
    };
    match phase {
        "admission" => {}
        "join" => register_join(&store, &key).await,
        "deadline" => {
            let report = deadline_worker(&store)
                .tick(key.tenant_id().clone(), None, CancellationSignal::never())
                .await
                .unwrap();
            let owned = store.load_child_run(&key).await.unwrap();
            let child = owned.child().admission().intent().provenance().run_id();
            assert_eq!(report.items().len(), 2);
            let mut runs = report
                .items()
                .iter()
                .map(|item| {
                    assert!(matches!(
                        item.result().unwrap(),
                        AgentDeadlineCancellationOutcome::Requested(_)
                    ));
                    item.candidate().run_id().to_string()
                })
                .collect::<Vec<_>>();
            runs.sort();
            let mut expected = vec![key.parent_run_id().to_string(), child.to_string()];
            expected.sort();
            assert_eq!(runs, expected);
        }
        "child_cancel" | "settlement" => {
            let report = cancellation::reconciler(&store)
                .tick(key.tenant_id().clone(), None, CancellationSignal::never())
                .await
                .unwrap();
            assert_eq!(report.items().len(), 1);
            let expected = if phase == "child_cancel" {
                ChildReconciliationKind::Cancellation
            } else {
                ChildReconciliationKind::Settlement
            };
            assert_eq!(report.items()[0].kind(), expected);
            let result = report.items()[0].result().unwrap();
            if phase == "child_cancel" {
                assert_eq!(
                    result,
                    ChildReconciliationCommit::Cancellation(
                        ChildCancellationOutcome::AlreadyRequested
                    )
                );
            } else {
                assert_eq!(result, ChildReconciliationCommit::Settlement);
            }
        }
        "child_terminal" => Box::pin(run_child_cleanup(&store, &key)).await,
        "takeover" => {
            store
                .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
                .await
                .unwrap();
        }
        "parent_terminal" => Box::pin(run_parent_cleanup(&store, &key)).await,
        "replay" => {
            assert!(
                deadline_worker(&store)
                    .tick(key.tenant_id().clone(), None, CancellationSignal::never())
                    .await
                    .unwrap()
                    .items()
                    .is_empty()
            );
            assert!(
                cancellation::reconciler(&store)
                    .tick(key.tenant_id().clone(), None, CancellationSignal::never())
                    .await
                    .unwrap()
                    .items()
                    .is_empty()
            );
        }
        _ => panic!("unknown deadline Join qualification phase"),
    }
    ready_and_park(json!({"phase": phase, "key": key})).await;
    panic!("worker escaped its kill point");
}

#[tokio::test]
async fn deadline_join_stale_worker() {
    let Ok(input) = std::env::var(INPUT_ENV) else {
        return;
    };
    let resume = parent_control();
    let input: Value = serde_json::from_str(&input).unwrap();
    let key: ChildRunKey = serde_json::from_value(input["key"].clone()).unwrap();
    let store = test_store()
        .await
        .expect("process qualification requires PostgreSQL");
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let stale = run.lease().unwrap().fence().clone();
    crate::process_harness::publish_ready(&json!({
        "key": key,
        "epoch": stale.epoch().get().to_string()
    }));
    resume.await.unwrap();
    let current = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert!(current.lease().unwrap().fence().epoch() > stale.epoch());
    let result = store
        .renew_lease(&stale, current.lease().unwrap().expires_at())
        .await;
    assert!(matches!(result, Err(StoreError::StaleFence)));
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
    checkpoint: Value,
    parent_journal: JournalPage,
    child_journal: JournalPage,
}

fn ownership_value(record: &ChildRunRecord) -> Value {
    json!({
        "intent": record.intent(),
        "spawn": record.spawn().head(),
        "ancestors": record.ancestors(),
        "settlement": record.settlement()
    })
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

#[allow(clippy::too_many_lines)]
async fn snapshot(store: &PostgresStore, key: &ChildRunKey, stage: usize) -> Snapshot {
    let owned = store.load_child_run(key).await.unwrap();
    let child_id = owned.child().admission().intent().provenance().run_id();
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let child = store.load_run(key.tenant_id(), child_id).await.unwrap();
    let join = store.load_child_join(key.parent()).await.unwrap();
    assert_eq!(join.is_some(), stage >= 1);
    if let Some(join) = &join {
        assert!(join.head().is_none());
        assert!(join.consumed().is_none());
    }
    assert_eq!(owned.settlement().is_some(), stage >= 5);
    assert_eq!(
        parent.lifecycle().status(),
        if stage >= 7 {
            RunStatus::Cancelled
        } else if stage >= 2 {
            RunStatus::CancellationRequested
        } else {
            RunStatus::Active
        }
    );
    assert_eq!(
        child.lifecycle().status(),
        if stage >= 4 {
            RunStatus::Cancelled
        } else if stage >= 2 {
            RunStatus::CancellationRequested
        } else {
            RunStatus::Active
        }
    );
    assert_eq!(parent.lease().is_some(), matches!(stage, 0 | 6));
    assert!(child.lease().is_none());
    let cancellation = if stage >= 2 {
        let record = store.load_child_cancellation(key).await.unwrap();
        assert!(!record.is_parent_failure_close());
        assert_eq!(record.receipt().is_some(), stage >= 3);
        if let Some(receipt) = record.receipt() {
            assert_eq!(
                receipt.outcome(),
                ChildCancellationOutcome::AlreadyRequested
            );
            assert_eq!(
                receipt
                    .lifecycle()
                    .cancellation_request()
                    .unwrap()
                    .failure()
                    .code()
                    .as_str(),
                "agent.deadline.expired"
            );
        }
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
    assert_eq!(
        store
            .pending_child_cancellations_after(key.tenant_id(), None)
            .await
            .unwrap(),
        if stage == 2 {
            vec![key.clone()]
        } else {
            vec![]
        }
    );
    assert_eq!(
        store
            .pending_child_settlements_after(key.tenant_id(), None)
            .await
            .unwrap(),
        if stage == 4 {
            vec![key.clone()]
        } else {
            vec![]
        }
    );
    if stage >= 2 {
        assert!(
            store
                .due_agent_deadlines_after(key.tenant_id(), None)
                .await
                .unwrap()
                .is_empty()
        );
    }
    let history = store
        .load_node_attempt_history_page(
            key.parent(),
            None,
            NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    assert!(!history.has_more());
    let parent_journal = journal(store, key.tenant_id(), key.parent_run_id()).await;
    let child_journal = journal(store, key.tenant_id(), child_id).await;
    if stage >= 7 {
        assert_eq!(
            event_count(&parent_journal, "agent-deadline-cancellation-requested"),
            1
        );
        assert_eq!(event_count(&parent_journal, "child-run-settled"), 1);
        assert_eq!(
            event_count(&parent_journal, "agent-cancellation-confirmed"),
            1
        );
        assert_eq!(
            event_count(&child_journal, "agent-deadline-cancellation-requested"),
            1
        );
        assert_eq!(
            event_count(&child_journal, "child-run-cancellation-requested"),
            0
        );
        assert_eq!(
            event_count(&child_journal, "agent-cancellation-confirmed"),
            1
        );
        assert_eq!(parent.lifecycle().terminal_usage().unwrap(), &child_usage());
    }
    Snapshot {
        parent: json!({"lifecycle": parent.lifecycle(), "lease": parent.lease()}),
        child: json!({"lifecycle": child.lifecycle(), "lease": child.lease()}),
        ownership: ownership_value(&owned),
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
        checkpoint: serde_json::to_value(
            store
                .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap(),
        )
        .unwrap(),
        parent_journal,
        child_journal,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn deadline_join_survives_process_loss_and_rejects_retained_stale_worker() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let pool = sql_pool().await;
    let database_version: String = query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    let mut key = Value::Null;
    let mut stale: Option<TestProcess> = None;
    let mut completed: Option<Snapshot> = None;
    for (stage, phase) in [
        "admission",
        "join",
        "deadline",
        "child_cancel",
        "child_terminal",
        "settlement",
        "takeover",
        "parent_terminal",
        "replay",
    ]
    .into_iter()
    .enumerate()
    {
        if phase == "deadline" {
            let owned_key: ChildRunKey = serde_json::from_value(key.clone()).unwrap();
            let admission = store
                .load_agent_admission(owned_key.tenant_id(), owned_key.parent_run_id())
                .await
                .unwrap();
            await_due(&store, admission.admission().intent().budget().deadline()).await;
        }
        let mut worker = TestProcess::spawn(WORKER, &json!({"phase": phase, "key": key}));
        let ready = worker
            .ready()
            .await
            .unwrap_or_else(|error| panic!("{phase}: {error}"));
        assert_eq!(ready["phase"], phase);
        if stage != 0 {
            assert_eq!(ready["key"], key);
        }
        key = ready["key"].clone();
        let owned_key: ChildRunKey = serde_json::from_value(key.clone()).unwrap();
        if phase == "admission" {
            let mut process = TestProcess::spawn(STALE_WORKER, &json!({"key": key}));
            let stale_ready = process.ready().await.unwrap();
            assert_eq!(stale_ready["key"], key);
            stale = Some(process);
        }
        let before = Box::pin(snapshot(&store, &owned_key, stage)).await;
        if phase == "takeover" {
            let stale = stale.as_mut().unwrap();
            stale.resume();
            stale.wait_success().await;
        }
        let status = worker.kill_and_reap().await;
        let after = Box::pin(snapshot(&store, &owned_key, stage)).await;
        assert_eq!(before, after, "durable evidence changed after {phase} kill");
        if stage >= 7 {
            if let Some(completed) = &completed {
                assert_eq!(&after, completed, "replay changed terminal evidence");
            } else {
                completed = Some(after);
            }
        }
        println!(
            "\nSTATEKNOT_DEADLINE_JOIN_PROCESS_EVIDENCE={}",
            json!({
                "profile": "deadline-cancel-join-committed-boundaries-v1",
                "phase": phase,
                "forced_exit": !status.success(),
                "invariants": "passed",
                "termination": if cfg!(unix) { "SIGKILL" } else { "Child::kill" },
                "postgres": database_version,
                "os": std::env::consts::OS,
                "arch": std::env::consts::ARCH
            })
        );
    }
    store.close().await;
}
