// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, parent_control, publish_ready, ready_and_park},
};
use sqlx_core::connection::ConnectOptions;
use sqlx_postgres::PgConnectOptions;

const WORKER: &str = "child_admission::ownership::failure_close::commit_loss::commit_loss_worker";

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    key: ChildRunKey,
    fence: RunFence,
    failure: Failure,
}

async fn worker_store(input: &Value) -> PostgresStore {
    let url = std::env::var(DATABASE_URL_ENV).expect("commit-loss worker requires PostgreSQL");
    let mut connection: PgConnectOptions = url.parse().unwrap();
    if let Some(port) = input["proxy_port"].as_u64() {
        connection = connection
            .host("127.0.0.1")
            .port(u16::try_from(port).unwrap());
    }
    // Do not migrate through the proxy. The controller has already migrated and
    // prepared this isolated test database before the instrumented connection.
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

async fn attempt(
    store: &PostgresStore,
    scenario: &Scenario,
    replace_candidate: bool,
) -> Result<RunFailureCloseOutcome, StoreError> {
    let key = &scenario.key;
    let run = store.load_run(key.tenant_id(), key.parent_run_id()).await?;
    let append = JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::worker(
            key.tenant_id().clone(),
            key.parent_run_id(),
            EventId::generate(),
            scenario.fence.clone(),
            payload("run-failure-close-requested"),
        )
        .unwrap(),
    )
    .unwrap();
    store
        .request_run_failure_close(
            key.parent().base_checkpoint(),
            if replace_candidate {
                test_failure(
                    "test.replacement.rejected",
                    "Do not replace the committed decision.",
                )
            } else {
                scenario.failure.clone()
            },
            if replace_candidate {
                BudgetUsage::zero()
            } else {
                direct_usage()
            },
            append,
        )
        .await
}

#[tokio::test]
async fn commit_loss_worker() {
    let Ok(input) = std::env::var(INPUT_ENV) else {
        return;
    };
    let resume = parent_control();
    let input: Value = serde_json::from_str(&input).unwrap();
    let scenario: Scenario = serde_json::from_value(input["scenario"].clone()).unwrap();
    let store = worker_store(&input).await;
    match input["role"].as_str().unwrap() {
        "cut" => {
            // The COMMIT call cannot return: the proxy holds either its request
            // or both completion frames until this actual process is SIGKILLed.
            let _ = attempt(&store, &scenario, false).await;
            panic!("instrumented call returned before the process was killed");
        }
        "stale" => {
            let run = store
                .load_run(scenario.key.tenant_id(), scenario.key.parent_run_id())
                .await
                .unwrap();
            assert_eq!(run.lease().unwrap().fence(), &scenario.fence);
            publish_ready(&json!({"retained_fence": scenario.fence}));
            resume.await.unwrap();
            // Read the new journal head, but retain the old in-process fence.
            // Rejection must be StaleFence, not an incidental journal CAS error.
            assert!(matches!(
                attempt(&store, &scenario, false).await,
                Err(StoreError::StaleFence)
            ));
            store.close().await;
        }
        "recover" => {
            let existing = input["existing"].as_bool().unwrap();
            let result = attempt(&store, &scenario, existing).await.unwrap();
            assert_eq!(
                matches!(result, RunFailureCloseOutcome::Existing(_)),
                existing
            );
            assert_eq!(
                serde_json::to_value(result.record().failure()).unwrap(),
                serde_json::to_value(&scenario.failure).unwrap()
            );
            assert_eq!(result.record().direct_usage(), &direct_usage());
            ready_and_park(
                json!({"recovered": true, "source": result.record().registration().head()}),
            )
            .await;
        }
        _ => panic!("unknown commit-loss worker role"),
    }
}

// Nonlocking MVCC observations are intentional: while COMMIT is held, the
// interrupted writer owns the Run lock. A FOR UPDATE read would not be a barrier.
async fn unsealed_snapshot(store: &PostgresStore, scenario: &Scenario) -> Value {
    let key = &scenario.key;
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let lease = run.lease().unwrap();
    let journal = store
        .load_journal_page(
            key.tenant_id(),
            key.parent_run_id(),
            None,
            JournalPageSize::new(100).unwrap(),
        )
        .await
        .unwrap();
    assert!(!journal.has_more());
    assert!(
        store
            .pending_child_cancellations_after(key.tenant_id(), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .pending_run_failure_closes_after(key.tenant_id(), None)
            .await
            .unwrap()
            .is_empty()
    );
    json!({
        "lifecycle": run.lifecycle(), "fence": lease.fence(),
        "expires_at": lease.expires_at(), "head": run.journal_head(),
        "checkpoint": {"id": run.checkpoint().unwrap().checkpoint_id(), "digest": run.checkpoint().unwrap().digest()},
        "journal": journal.events(),
        "account": store.load_child_budget_account(key.tenant_id(), key.parent_run_id()).await.unwrap()
    })
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
    .expect("owned disconnected backend must terminate and release its transaction");
}

async fn take_over(store: &PostgresStore, scenario: &mut Scenario) {
    let mut stale = TestProcess::spawn(WORKER, &json!({"role": "stale", "scenario": scenario}));
    let ready = stale.ready().await.unwrap();
    assert_eq!(
        ready["retained_fence"],
        serde_json::to_value(&scenario.fence).unwrap()
    );
    let key = &scenario.key;
    let expires_at = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .lease()
        .unwrap()
        .expires_at();
    tokio::time::timeout(Duration::from_secs(45), async {
        while store.observe_database_clock().await.unwrap() < expires_at {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("real database-clock lease expiry must be observed");
    let successor = store
        .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
        .await
        .unwrap();
    let new_fence = successor.lease().fence().clone();
    assert!(new_fence.epoch() > scenario.fence.epoch());
    assert_ne!(new_fence.attempt_id(), scenario.fence.attempt_id());
    let before = Box::pin(unsealed_snapshot(store, scenario)).await;
    stale.resume();
    stale.wait_success().await;
    assert_eq!(before, Box::pin(unsealed_snapshot(store, scenario)).await);
    scenario.fence = new_fence;
}

async fn drain_and_verify(store: &PostgresStore, scenario: &Scenario) {
    let key = &scenario.key;
    let child = store.load_child_run(key).await.unwrap();
    let child_id = child.child().run().lifecycle().provenance().run_id();
    let source = store
        .load_run_failure_close(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(source.failure()).unwrap(),
        serde_json::to_value(&scenario.failure).unwrap()
    );
    assert!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    assert!(matches!(
        tick(store, key.tenant_id()).await.items()[0].result(),
        Err(StoreError::UnsettledChildRuns)
    ));
    let delivery = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(delivery.items().len(), 1);
    delivery.items()[0].result().unwrap();
    let usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(11))
        .build()
        .unwrap();
    cancellation::confirm(store, key.tenant_id(), child_id, usage.clone())
        .await
        .unwrap();
    let settlement = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(settlement.items().len(), 1);
    settlement.items()[0].result().unwrap();
    tick(store, key.tenant_id()).await.items()[0]
        .result()
        .unwrap();
    let final_run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(final_run.lifecycle().status(), RunStatus::Failed);
    assert_eq!(
        serde_json::to_value(final_run.lifecycle().terminal_failure().unwrap()).unwrap(),
        serde_json::to_value(&scenario.failure).unwrap()
    );
    assert_eq!(
        final_run.lifecycle().terminal_usage().unwrap(),
        &direct_usage().checked_accumulate(&usage).unwrap()
    );
    let page = store
        .load_journal_page(
            key.tenant_id(),
            key.parent_run_id(),
            None,
            JournalPageSize::new(100).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    for kind in [
        "run-failure-close-requested",
        "child-run-settled",
        "run-failure-close-completed",
    ] {
        assert_eq!(
            page.events()
                .iter()
                .filter(|event| event.payload().kind().as_str() == kind)
                .count(),
            1
        );
    }
    let replay = attempt(store, scenario, true).await.unwrap();
    assert!(matches!(replay, RunFailureCloseOutcome::Existing(_)));
    assert_eq!(
        replay.record().registration().head(),
        source.registration().head()
    );
    assert!(replay.record().completed_at().is_some());
    assert!(tick(store, key.tenant_id()).await.items().is_empty());
    assert_eq!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .journal_head(),
        final_run.journal_head()
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failure_close_commit_loss_and_fence_takeover_are_recoverable() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let connection = std::env::var(DATABASE_URL_ENV)
        .ok()
        .map(|url| url.parse::<PgConnectOptions>().unwrap());
    if let Some(connection) = &connection {
        // Reject remote targets before migrating or preparing any fixture data.
        let _ = loopback_target(connection);
    }
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(20)).await else {
        return;
    };
    let connection = connection.unwrap();
    let pool = sql_pool().await;
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    for cut in [Cut::BeforeCommit, Cut::CommitResponse] {
        let mut value = started(&store, "failure-close-commit-loss").await;
        spawn(&store, &value).await.unwrap();
        finish_node(&store, &mut value).await;
        let mut scenario = Scenario {
            key: value.intent.key().clone(),
            fence: value.fence.clone(),
            failure: test_failure(
                "test.commit.original",
                "Original complete failure across commit loss.",
            ),
        };
        let before = Box::pin(unsealed_snapshot(&store, &scenario)).await;
        let mut proxy = CommitProxy::start(&connection, cut).await;
        let mut worker = TestProcess::spawn(
            WORKER,
            &json!({"role": "cut", "scenario": scenario, "proxy_port": proxy.port()}),
        );
        proxy.wait_cut(cut).await;
        let pid = proxy.backend_pid();
        let mut committed_head = None;
        if cut == Cut::BeforeCommit {
            assert_eq!(before, Box::pin(unsealed_snapshot(&store, &scenario)).await);
        } else {
            let close = store
                .load_run_failure_close(scenario.key.tenant_id(), scenario.key.parent_run_id())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(close.failure()).unwrap(),
                serde_json::to_value(&scenario.failure).unwrap()
            );
            assert_eq!(close.direct_usage(), &direct_usage());
            committed_head = Some(close.registration().head());
        }
        worker.kill_and_reap().await;
        proxy.stop().await;
        wait_for_backend_disconnect(&pool, pid).await;
        if cut == Cut::BeforeCommit {
            assert_eq!(before, Box::pin(unsealed_snapshot(&store, &scenario)).await);
            assert!(
                store
                    .load_run_failure_close(scenario.key.tenant_id(), scenario.key.parent_run_id())
                    .await
                    .unwrap()
                    .is_none()
            );
            Box::pin(take_over(&store, &mut scenario)).await;
        }
        let mut recovery = TestProcess::spawn(
            WORKER,
            &json!({"role": "recover", "scenario": scenario, "existing": cut == Cut::CommitResponse}),
        );
        let recovered = recovery.ready().await.unwrap();
        assert_eq!(recovered["recovered"], true);
        if let Some(head) = committed_head {
            assert_eq!(recovered["source"], serde_json::to_value(head).unwrap());
        }
        recovery.kill_and_reap().await;
        Box::pin(drain_and_verify(&store, &scenario)).await;
        println!(
            "\nSTATEKNOT_COMMIT_LOSS_EVIDENCE={}",
            json!({
                "profile": "failure-close-commit-loss-v1",
                "cut": if cut == Cut::BeforeCommit { "commit_not_forwarded" } else { "commit_response_withheld" },
                "commit_forwarded": cut == Cut::CommitResponse,
                "commit_response_forwarded": false,
                "interrupted_writer_killed": true, "cold_recovery_verified": true,
                "expired_fence_rejection_verified": cut == Cut::BeforeCommit,
                "termination": if cfg!(unix) { "SIGKILL" } else { "Child::kill" },
                "invariants": "passed", "postgres": version,
                "os": std::env::consts::OS, "arch": std::env::consts::ARCH
            })
        );
    }
    pool.close().await;
    store.close().await;
}
