// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Ambiguous-COMMIT qualification for child cancellation delivery and wait cleanup.

use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, ready_and_park, watch_parent},
};
use sqlx_core::{connection::ConnectOptions, query_scalar::query_scalar};
use sqlx_postgres::PgConnectOptions;
use stateknot_store_postgres::{
    ChildCancellationDelivery, ChildCancellationOutcome, JournalPage, WaitAbandonmentReason,
};

const WORKER: &str = "child_admission::ownership::child_cancellation_commit_loss::child_cancellation_delivery_commit_loss_worker";

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    key: ChildRunKey,
    child: RunId,
    timer: TimerId,
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
    let record = store.load_child_cancellation(&scenario.key).await.unwrap();
    let child = store
        .load_run(scenario.key.tenant_id(), scenario.child)
        .await
        .unwrap();
    let event_id = EventId::generate();
    let failure_id = FailureId::generate();
    let failure = Failure::new(
        failure_id,
        FailureCategory::Cancelled,
        FailureCode::new("child.parent_cancelled").unwrap(),
        FailureOrigin::new("stateknot.runtime.child_reconciler").unwrap(),
        FailureMessage::new("The owning parent requested cancellation.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(event_id);
    let request = RunCancellationRequest::new(
        failure,
        store
            .observe_database_clock()
            .await
            .unwrap()
            .max(record.queued_at()),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(child.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            scenario.key.tenant_id().clone(),
            scenario.child,
            event_id,
            payload("child-run-cancellation-requested"),
        )
        .unwrap(),
    )
    .unwrap();
    let outcome = Box::pin(store.deliver_child_cancellation(&scenario.key, append, request))
        .await
        .unwrap();
    let receipt = outcome.record().receipt().unwrap();
    json!({
        "outcome": if matches!(outcome, ChildCancellationDelivery::Committed(_)) {
            "committed"
        } else {
            "idempotent"
        },
        "candidate": {"event": event_id, "failure": failure_id},
        "durable": {
            "event": receipt.head().event_id(),
            "failure": receipt.lifecycle().cancellation_request().unwrap().failure().id(),
            "outcome": format!("{:?}", receipt.outcome()).to_ascii_lowercase()
        }
    })
}

#[tokio::test]
async fn child_cancellation_delivery_commit_loss_worker() {
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
        _ => panic!("unknown child-cancellation COMMIT-loss worker role"),
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

async fn cancellation_value(store: &PostgresStore, scenario: &Scenario) -> Value {
    let record = store.load_child_cancellation(&scenario.key).await.unwrap();
    json!({
        "key": record.key(),
        "child": record.child_run_id(),
        "parent_lifecycle": record.parent_lifecycle(),
        "parent_head": record.parent_head(),
        "queued_at": record.queued_at(),
        "receipt": record.receipt().map(|receipt| json!({
            "outcome": format!("{:?}", receipt.outcome()).to_ascii_lowercase(),
            "lifecycle": receipt.lifecycle(),
            "head": receipt.head(),
            "delivered_at": receipt.delivered_at()
        }))
    })
}

async fn abandonment_value(store: &PostgresStore, scenario: &Scenario) -> Value {
    match store
        .load_timer_abandonment(scenario.key.tenant_id(), scenario.child, scenario.timer)
        .await
    {
        Ok(abandonment) => json!({
            "wait": abandonment.wait(),
            "reason": abandonment.reason(),
            "journal": abandonment.journal(),
            "digest": abandonment.digest()
        }),
        Err(StoreError::WaitAbandonmentNotFound) => Value::Null,
        Err(error) => panic!("unexpected timer abandonment read failure: {error:?}"),
    }
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    parent: Value,
    child: Value,
    ownership: Value,
    cancellation: Value,
    abandonment: Value,
    account: Value,
    pending_cancellations: Value,
    pending_settlements: Value,
    child_journal: JournalPage,
}

fn checkpoint_value(run: &stateknot_store_postgres::StoredRun) -> Value {
    run.checkpoint().map_or(Value::Null, |checkpoint| {
        json!({
            "id": checkpoint.checkpoint_id(),
            "superstep": checkpoint.superstep(),
            "digest": checkpoint.digest()
        })
    })
}

async fn snapshot(store: &PostgresStore, scenario: &Scenario) -> Snapshot {
    let parent = store
        .load_run(scenario.key.tenant_id(), scenario.key.parent_run_id())
        .await
        .unwrap();
    let child = store
        .load_run(scenario.key.tenant_id(), scenario.child)
        .await
        .unwrap();
    let owned = store.load_child_run(&scenario.key).await.unwrap();
    Snapshot {
        parent: json!({
            "lifecycle": parent.lifecycle(),
            "journal_head": parent.journal_head(),
            "lease": parent.lease(),
            "scheduler_ready_at": parent.scheduler_ready_at(),
            "scheduler_not_before": parent.scheduler_not_before(),
            "wait_set_digest": parent.wait_set_digest(),
            "unresolved_wait_count": parent.unresolved_wait_count(),
            "checkpoint": checkpoint_value(&parent)
        }),
        child: json!({
            "lifecycle": child.lifecycle(),
            "journal_head": child.journal_head(),
            "lease": child.lease(),
            "scheduler_ready_at": child.scheduler_ready_at(),
            "scheduler_not_before": child.scheduler_not_before(),
            "wait_set_digest": child.wait_set_digest(),
            "unresolved_wait_count": child.unresolved_wait_count(),
            "checkpoint": checkpoint_value(&child)
        }),
        ownership: json!({
            "intent": owned.intent(),
            "spawn": owned.spawn(),
            "ancestors": owned.ancestors(),
            "settlement": owned.settlement()
        }),
        cancellation: Box::pin(cancellation_value(store, scenario)).await,
        abandonment: Box::pin(abandonment_value(store, scenario)).await,
        account: serde_json::to_value(
            store
                .load_child_budget_account(scenario.key.tenant_id(), scenario.key.parent_run_id())
                .await
                .unwrap(),
        )
        .unwrap(),
        pending_cancellations: serde_json::to_value(
            store
                .pending_child_cancellations_after(scenario.key.tenant_id(), None)
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
    let tenant = tenant(suffix);
    let (child_fixture, timer, _) = wait_fixture();
    let request = durable_admission_request(
        &child_fixture,
        tenant.clone(),
        AgentRunIds::generate(),
        child_fixture.graph.output_schema().clone(),
        child_fixture.graph.input_schema().clone(),
    );
    let parent_fixture = declared_parent(&child_fixture, request.intent().descriptor());
    let parent = Box::pin(tree::root_admission(store, &parent_fixture, tenant.clone())).await;
    let owned = Box::pin(tree::spawn_below(store, &parent, &child_fixture))
        .await
        .unwrap();
    let child = owned.child().admission().intent().provenance().run_id();
    let lease = store
        .claim_lease(&tenant, child, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        child_fixture.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let report = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = report.into_parts().0 else {
        panic!("wait handoff expected");
    };
    DurableGraphLifecycle::new(
        store.clone(),
        child_fixture.registry,
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap()
    .commit_barrier(*handoff)
    .await
    .unwrap();
    assert_eq!(
        store
            .load_run(&tenant, child)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Waiting
    );
    cancellation::cancel_run(
        store,
        &tenant,
        parent.admission().intent().provenance().run_id(),
    )
    .await;
    Scenario {
        key: owned.intent().key().clone(),
        child,
        timer,
    }
}

async fn verify_committed(store: &PostgresStore, scenario: &Scenario, value: &Snapshot) {
    let record = store.load_child_cancellation(&scenario.key).await.unwrap();
    let receipt = record.receipt().unwrap();
    assert_eq!(receipt.outcome(), ChildCancellationOutcome::Requested);
    assert_eq!(
        receipt.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    let child = store
        .load_run(scenario.key.tenant_id(), scenario.child)
        .await
        .unwrap();
    assert_eq!(child.lifecycle().status(), RunStatus::CancellationRequested);
    assert!(child.lifecycle().waits().is_none());
    assert_eq!(child.unresolved_wait_count(), 0);
    assert!(child.wait_set_digest().is_none());
    assert!(child.scheduler_ready_at().is_some());
    let abandonment = store
        .load_timer_abandonment(scenario.key.tenant_id(), scenario.child, scenario.timer)
        .await
        .unwrap();
    assert_eq!(abandonment.reason(), WaitAbandonmentReason::RunCancellation);
    assert_eq!(abandonment.journal(), receipt.head());
    assert_eq!(
        event_count(&value.child_journal, "child-run-cancellation-requested"),
        1
    );
    assert_eq!(value.pending_cancellations, json!([]));
    assert_eq!(value.pending_settlements, json!([]));
}

async fn verify_downstream(store: &PostgresStore, scenario: &Scenario) {
    let claimed = store
        .claim_lease(
            scenario.key.tenant_id(),
            scenario.child,
            AttemptId::generate(),
        )
        .await
        .unwrap();
    store.release_lease(claimed.lease().fence()).await.unwrap();
    cancellation::confirm(
        store,
        scenario.key.tenant_id(),
        scenario.child,
        BudgetUsage::zero(),
    )
    .await
    .unwrap();
    let tick = cancellation::reconciler(store)
        .tick(
            scenario.key.tenant_id().clone(),
            None,
            CancellationSignal::never(),
        )
        .await
        .unwrap();
    assert_eq!(tick.items().len(), 1);
    assert_eq!(
        tick.items()[0].result().unwrap(),
        stateknot_runtime::ChildReconciliationCommit::Settlement
    );
    cancellation::confirm(
        store,
        scenario.key.tenant_id(),
        scenario.key.parent_run_id(),
        BudgetUsage::zero(),
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
        RunStatus::Cancelled
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_cancellation_delivery_commit_loss_is_recoverable() {
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
                "child-cancel-commit-not-forwarded"
            } else {
                "child-cancel-commit-response-withheld"
            },
        ))
        .await;
        let before = Box::pin(snapshot(&store, &scenario)).await;
        assert_eq!(before.abandonment, Value::Null);
        let waiting = store
            .load_run(scenario.key.tenant_id(), scenario.child)
            .await
            .unwrap();
        assert_eq!(waiting.lifecycle().status(), RunStatus::Waiting);
        assert_eq!(waiting.unresolved_wait_count(), 1);
        assert!(waiting.wait_set_digest().is_some());
        assert!(waiting.scheduler_ready_at().is_none());
        let mut proxy = CommitProxy::start(
            &connection,
            cut,
            TransactionTarget::ChildCancellationDelivery,
        )
        .await;
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
            assert_eq!(
                at_cut, before,
                "unforwarded COMMIT leaked cancellation state"
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
                Box::pin(snapshot(&store, &scenario)).await,
                before,
                "disconnect did not roll back cancellation and wait cleanup"
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
        assert_eq!(recovered["result"]["durable"]["outcome"], "requested");
        if cut == Cut::BeforeCommit {
            assert_eq!(
                recovered["result"]["candidate"]["event"],
                recovered["result"]["durable"]["event"]
            );
            assert_eq!(
                recovered["result"]["candidate"]["failure"],
                recovered["result"]["durable"]["failure"]
            );
        } else {
            assert_ne!(
                recovered["result"]["candidate"]["event"],
                recovered["result"]["durable"]["event"]
            );
            assert_ne!(
                recovered["result"]["candidate"]["failure"],
                recovered["result"]["durable"]["failure"]
            );
        }
        recovery.kill_and_reap().await;
        let after = Box::pin(snapshot(&store, &scenario)).await;
        Box::pin(verify_committed(&store, &scenario, &after)).await;
        if cut == Cut::CommitResponse {
            assert_eq!(
                after, at_cut,
                "idempotent recovery replaced cancellation or abandonment evidence"
            );
        }
        Box::pin(verify_downstream(&store, &scenario)).await;
        println!(
            "\nSTATEKNOT_CHILD_CANCELLATION_COMMIT_LOSS_EVIDENCE={}",
            json!({
                "profile": "child-cancellation-delivery-commit-loss-v1",
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
                "wait_abandonment_verified": true,
                "downstream_settlement_verified": true,
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
