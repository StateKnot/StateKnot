// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! OS-kill qualification at committed boundaries, not a database failover drill.

use super::*;
use crate::process_harness::{INPUT_ENV, TestProcess, ready_and_park, watch_parent};
use stateknot_store_postgres::JournalPage;

const WORKER: &str =
    "child_admission::ownership::failure_close::process::failure_close_process_worker";

fn child_usage() -> BudgetUsage {
    BudgetUsage::builder()
        .input_tokens(TokenCount::new(11))
        .build()
        .unwrap()
}

// This entry point is selected with --exact by the parent; ordinary discovery
// does no work here. Missing real PostgreSQL is fatal once subprocess mode is set.
#[tokio::test]
async fn failure_close_process_worker() {
    let Ok(input) = std::env::var(INPUT_ENV) else {
        return;
    };
    watch_parent();
    let input: Value = serde_json::from_str(&input).unwrap();
    let store = test_store()
        .await
        .expect("process qualification requires PostgreSQL");
    let key: ChildRunKey = if input["phase"] == "source" {
        let mut value = started(&store, "failure-close-os-kill").await;
        spawn(&store, &value).await.unwrap();
        finish_node(&store, &mut value).await;
        let result = request(&store, &value, direct_usage()).await.unwrap();
        assert!(matches!(result, RunFailureCloseOutcome::Committed(_)));
        value.intent.key().clone()
    } else {
        serde_json::from_value(input["key"].clone()).unwrap()
    };
    let tenant = key.tenant_id();
    match input["phase"].as_str().unwrap() {
        "source" => {}
        "delivery" | "settlement" => {
            let result = cancellation::reconciler(&store)
                .tick(tenant.clone(), None, CancellationSignal::never())
                .await
                .unwrap();
            assert_eq!(result.items().len(), 1);
            let expected = if input["phase"] == "delivery" {
                stateknot_runtime::ChildReconciliationKind::Cancellation
            } else {
                stateknot_runtime::ChildReconciliationKind::Settlement
            };
            assert_eq!(result.items()[0].kind(), expected);
            result.items()[0].result().unwrap();
        }
        "child_terminal" => {
            let child = store.load_child_run(&key).await.unwrap();
            cancellation::confirm(
                &store,
                tenant,
                child.child().run().lifecycle().provenance().run_id(),
                child_usage(),
            )
            .await
            .unwrap();
        }
        "close" => {
            let result = tick(&store, tenant).await;
            assert_eq!(result.items().len(), 1);
            assert!(
                result.items()[0]
                    .result()
                    .unwrap()
                    .record()
                    .completed_at()
                    .is_some()
            );
        }
        "replay" => {
            assert!(tick(&store, tenant).await.items().is_empty());
            assert!(
                cancellation::reconciler(&store)
                    .tick(tenant.clone(), None, CancellationSignal::never())
                    .await
                    .unwrap()
                    .items()
                    .is_empty()
            );
        }
        _ => panic!("unknown process qualification phase"),
    }
    ready_and_park(json!({"phase": input["phase"], "key": key})).await;
    // Intentionally unreachable on success; the parent must kill this process.
    panic!("worker escaped its kill point");
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    parent: Value,
    child: Value,
    source: JournalHead,
    completed_at: Option<Timestamp>,
    failure: Value,
    settlement: Value,
    account: Value,
    receipt: Value,
    parent_journal: JournalPage,
    child_journal: JournalPage,
}

async fn journal(store: &PostgresStore, tenant: &TenantId, run: RunId) -> JournalPage {
    let page = store
        .load_journal_page(tenant, run, None, JournalPageSize::new(100).unwrap())
        .await
        .unwrap();
    assert!(
        !page.has_more(),
        "fixture must fit one fully verified journal page"
    );
    page
}

#[allow(clippy::too_many_lines)]
async fn snapshot(store: &PostgresStore, key: &ChildRunKey, stage: usize) -> Snapshot {
    let tenant = key.tenant_id();
    let parent = store.load_run(tenant, key.parent_run_id()).await.unwrap();
    let child = store.load_child_run(key).await.unwrap();
    let child_run = child.child().run();
    let child_id = child_run.lifecycle().provenance().run_id();
    let close = store
        .load_run_failure_close(tenant, key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let cancellation = store.load_child_cancellation(key).await.unwrap();
    assert!(parent.lease().is_none());
    assert!(child_run.lease().is_none());
    assert_eq!(close.direct_usage(), &direct_usage());
    let checkpoint = parent.checkpoint().unwrap();
    assert_eq!(
        checkpoint.checkpoint_id(),
        key.parent().base_checkpoint().checkpoint_id()
    );
    assert_eq!(checkpoint.digest(), key.parent().base_checkpoint().digest());
    assert_eq!(cancellation.parent_head(), &close.registration().head());
    assert!(cancellation.is_parent_failure_close());
    assert_eq!(cancellation.receipt().is_some(), stage >= 1);
    assert_eq!(child.settlement().is_some(), stage >= 3);
    assert_eq!(close.completed_at().is_some(), stage >= 4);
    assert_eq!(
        store
            .pending_child_cancellations_after(tenant, None)
            .await
            .unwrap(),
        if stage == 0 {
            vec![key.clone()]
        } else {
            vec![]
        }
    );
    assert_eq!(
        store
            .pending_child_settlements_after(tenant, None)
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
            .pending_run_failure_closes_after(tenant, None)
            .await
            .unwrap()
            .len(),
        usize::from(stage < 4)
    );
    assert_eq!(
        parent.lifecycle().status(),
        if stage >= 4 {
            RunStatus::Failed
        } else {
            RunStatus::Active
        }
    );
    assert_eq!(
        child_run.lifecycle().status(),
        match stage {
            0 => RunStatus::Active,
            1 => RunStatus::CancellationRequested,
            _ => RunStatus::Cancelled,
        }
    );
    if stage >= 1 {
        let reason = child_run
            .lifecycle()
            .cancellation_request()
            .unwrap()
            .failure();
        assert_eq!(reason.code().as_str(), "child.parent_failed");
        assert!(
            !serde_json::to_string(reason)
                .unwrap()
                .contains("Original failure")
        );
    }
    if stage >= 2 {
        assert_eq!(
            child_run.lifecycle().terminal_usage().unwrap(),
            &child_usage()
        );
    }
    if stage >= 4 {
        assert_eq!(
            serde_json::to_value(parent.lifecycle().terminal_failure().unwrap()).unwrap(),
            serde_json::to_value(close.failure()).unwrap()
        );
        assert_eq!(
            parent.lifecycle().terminal_usage().unwrap(),
            &direct_usage().checked_accumulate(&child_usage()).unwrap()
        );
    }
    assert!(matches!(
        store
            .claim_lease(tenant, key.parent_run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    Snapshot {
        parent: serde_json::to_value(parent.lifecycle()).unwrap(),
        child: serde_json::to_value(child_run.lifecycle()).unwrap(),
        source: close.registration().head(),
        completed_at: close.completed_at(),
        failure: serde_json::to_value(close.failure()).unwrap(),
        settlement: serde_json::to_value(child.settlement()).unwrap(),
        account: serde_json::to_value(
            store
                .load_child_budget_account(tenant, key.parent_run_id())
                .await
                .unwrap(),
        )
        .unwrap(),
        receipt: serde_json::to_value(
            cancellation
                .receipt()
                .map(stateknot_store_postgres::ChildCancellationReceipt::head),
        )
        .unwrap(),
        parent_journal: journal(store, tenant, key.parent_run_id()).await,
        child_journal: journal(store, tenant, child_id).await,
    }
}

fn event_count(page: &JournalPage, kind: &str) -> usize {
    page.events()
        .iter()
        .filter(|event| event.payload().kind().as_str() == kind)
        .count()
}

#[tokio::test]
async fn failure_close_survives_os_kill_at_each_committed_drain_boundary() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut key = Value::Null;
    let pool = sql_pool().await;
    let database_version: String = query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .unwrap();
    pool.close().await;
    let mut original: Option<Snapshot> = None;
    let mut completed: Option<Snapshot> = None;
    for (stage, phase) in [
        "source",
        "delivery",
        "child_terminal",
        "settlement",
        "close",
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
        // Independent database reads prove the rendezvous actually follows commit.
        let before = Box::pin(snapshot(&store, &owned_key, stage)).await;
        let status = worker.kill_and_reap().await;
        let after = Box::pin(snapshot(&store, &owned_key, stage)).await;
        assert_eq!(
            before, after,
            "durable evidence changed after kill at {phase}"
        );
        if let Some(original) = &original {
            assert_eq!(after.failure, original.failure);
            assert_eq!(after.source, original.source);
        } else {
            original = Some(before);
        }
        if stage >= 4 {
            assert_eq!(
                event_count(&after.parent_journal, "run-failure-close-requested"),
                1
            );
            assert_eq!(event_count(&after.parent_journal, "child-run-settled"), 1);
            assert_eq!(
                event_count(&after.parent_journal, "run-failure-close-completed"),
                1
            );
            assert_eq!(
                event_count(&after.child_journal, "child-run-cancellation-requested"),
                1
            );
            assert_eq!(
                event_count(&after.child_journal, "test-cancel-confirmed"),
                1
            );
            if let Some(completed) = &completed {
                assert_eq!(
                    &after, completed,
                    "restart replay appended or charged again"
                );
            } else {
                completed = Some(after);
            }
        }
        // Machine-readable, non-sensitive evidence in mandatory PostgreSQL CI logs.
        println!(
            "\nSTATEKNOT_PROCESS_KILL_EVIDENCE={}",
            json!({
                "profile": "failure-close-committed-boundaries-v1",
            "phase": phase, "forced_exit": !status.success(), "invariants": "passed",
            "termination": if cfg!(unix) { "SIGKILL" } else { "Child::kill" },
            "postgres": database_version, "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH
            })
        );
    }
    store.close().await;
}
