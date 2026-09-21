// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real-process qualification for the successful isolated-child Join path.

use super::*;
use crate::process_harness::{
    INPUT_ENV, TestProcess, parent_control, ready_and_park, watch_parent,
};
use stateknot_core::{AgentAdmissionIntent, ChildRunJoinRequest, NodeAttemptStatus};
use stateknot_runtime::{DurableChildJoinPublisher, DurableChildReconcilerOptions};
use stateknot_store_postgres::{ChildJoinRecord, ChildRunRecord, JournalPage};

const WORKER: &str = "child_admission::ownership::join::process::child_join_process_worker";
const STALE_WORKER: &str = "child_admission::ownership::join::process::child_join_stale_worker";

fn lifecycle_evidence(intent: &AgentAdmissionIntent) -> StaticLifecycleEvidence {
    StaticLifecycleEvidence {
        terminal: GraphTerminalEvidence::new(
            intent.descriptor().clone(),
            intent.request().clone(),
            intent.budget().clone(),
            AgentArtifacts::empty(),
            BudgetUsage::builder()
                .model_attempts(ExecutionCount::new(1))
                .model_turns(ExecutionCount::new(1))
                .input_bytes(ByteCount::new(1_024))
                .output_bytes(ByteCount::new(1_024))
                .build()
                .unwrap(),
        ),
        failure: None,
    }
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
    status: NodeAttemptStatus,
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
        .find(|attempt| attempt.status() == status)
        .cloned()
        .expect("qualification attempt must exist")
}

async fn register_join(store: &PostgresStore, key: &ChildRunKey) {
    let request = ChildRunJoinRequest::new([key.clone()]).unwrap();
    let attempt = physical_attempt(store, key.parent(), NodeAttemptStatus::Executing).await;
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

async fn run_child(store: &PostgresStore, key: &ChildRunKey) {
    let (fixture, intent) = Box::pin(rebuilt_fixture(store, key)).await;
    let child = intent.child().provenance().run_id();
    let lease = store
        .claim_lease(key.tenant_id(), child, AttemptId::generate())
        .await
        .unwrap();
    let loop_ = DurableAgentLoop::new(
        store.clone(),
        fixture.child_driver.registry,
        Arc::new(lifecycle_evidence(intent.child())),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let result = loop_
        .run(lease.lease().fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(result.outcome(), AgentLoopOutcome::Succeeded(_)));
}

async fn publish_join(store: &PostgresStore, key: &ChildRunKey) {
    let (fixture, _) = Box::pin(rebuilt_fixture(store, key)).await;
    let report = DurableChildJoinPublisher::new(
        store.clone(),
        fixture.driver.registry.schemas().clone(),
        DurableChildReconcilerOptions::default(),
    )
    .unwrap()
    .tick(key.tenant_id().clone(), None, CancellationSignal::never())
    .await
    .unwrap();
    assert_eq!(report.items().len(), 1);
    report.items()[0].result().unwrap();
}

async fn resume_parent(store: &PostgresStore, key: &ChildRunKey) {
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let fence = run.lease().unwrap().fence().clone();
    let (fixture, intent) = Box::pin(rebuilt_fixture(store, key)).await;
    let launched = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let deployment = super::driver::registry(store, &fixture, &intent, &launched, &resumed, false);
    let loop_ = DurableAgentLoop::new(
        store.clone(),
        deployment,
        Arc::new(lifecycle_evidence(fixture.parent.admission().intent())),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let result = loop_.run(fence, CancellationSignal::never()).await.unwrap();
    assert!(matches!(result.outcome(), AgentLoopOutcome::Succeeded(_)));
    assert_eq!(launched.load(Ordering::SeqCst), 0);
    assert_eq!(resumed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.driver.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.driver.second_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn child_join_process_worker() {
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
        let value = started(&store, "child-join-process").await;
        let committed = spawn(&store, &value).await.unwrap();
        committed.record().intent().key().clone()
    } else {
        serde_json::from_value(input["key"].clone()).unwrap()
    };
    match phase {
        "admission" => {}
        "join" => register_join(&store, &key).await,
        "child_terminal" => Box::pin(run_child(&store, &key)).await,
        "settlement" => {
            Box::pin(store.settle_child_run(&key, settlement_append(&store, &key).await))
                .await
                .unwrap();
        }
        "publication" => Box::pin(publish_join(&store, &key)).await,
        "takeover" => {
            store
                .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
                .await
                .unwrap();
        }
        "parent_resume" => Box::pin(resume_parent(&store, &key)).await,
        "replay" => {
            let request = ChildRunJoinRequest::new([key.clone()]).unwrap();
            assert!(
                store
                    .pending_child_settlements_after(key.tenant_id(), None)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(
                store
                    .pending_child_joins_after(key.tenant_id(), Some(&request))
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        _ => panic!("unknown process qualification phase"),
    }
    ready_and_park(json!({"phase": phase, "key": key})).await;
    panic!("worker escaped its kill point");
}

#[tokio::test]
async fn child_join_stale_worker() {
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
        .start_node_attempt(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                EventId::generate(),
                current.journal_head().unwrap().clone(),
                stale,
            ),
            key.parent().clone(),
            AttemptId::generate(),
        )
        .await;
    assert!(matches!(result, Err(StoreError::StaleFence)));
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    parent: Value,
    child: Value,
    ownership: Value,
    join: Value,
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
    assert_eq!(owned.settlement().is_some(), stage >= 3);
    if let Some(join) = &join {
        assert_eq!(join.head().is_some(), stage >= 4);
        assert_eq!(join.consumed().is_some(), stage >= 6);
    }
    assert_eq!(
        child.lifecycle().status(),
        if stage >= 2 {
            RunStatus::Succeeded
        } else {
            RunStatus::Active
        }
    );
    assert_eq!(
        parent.lifecycle().status(),
        if stage >= 6 {
            RunStatus::Succeeded
        } else {
            RunStatus::Active
        }
    );
    assert_eq!(parent.lease().is_some(), matches!(stage, 0 | 5));
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
        parent: json!({"lifecycle": parent.lifecycle(), "lease": parent.lease()}),
        child: json!({"lifecycle": child.lifecycle(), "lease": child.lease()}),
        ownership: ownership_value(&owned),
        join: join_value(join.as_ref()),
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
        parent_journal: journal(store, key.tenant_id(), key.parent_run_id()).await,
        child_journal: journal(store, key.tenant_id(), child_id).await,
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_join_survives_process_loss_and_rejects_retained_stale_worker() {
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
        "child_terminal",
        "settlement",
        "publication",
        "takeover",
        "parent_resume",
        "replay",
    ]
    .into_iter()
    .enumerate()
    {
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
        if stage >= 6 {
            if let Some(completed) = &completed {
                assert_eq!(&after, completed, "replay changed terminal evidence");
            } else {
                completed = Some(after);
            }
        }
        println!(
            "\nSTATEKNOT_CHILD_JOIN_PROCESS_EVIDENCE={}",
            json!({
                "profile": "child-join-committed-boundaries-v1",
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
