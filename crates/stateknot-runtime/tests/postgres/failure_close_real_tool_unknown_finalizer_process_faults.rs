// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real Unknown is resolved before the killed finalizer; the provider is not restarted.

#[path = "failure_close_real_tool_unknown_process_loss.rs"]
mod unresolved_unknown_faults;

use super::completion_commit_loss::{wait_for_backend_disconnect, worker_store};
use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, ready_and_park, watch_parent},
};
use futures_util::FutureExt;
use sqlx_postgres::PgConnectOptions;
use stateknot_core::ChildRunBudgetAccount;
use stateknot_store_postgres::RunFailureCloseRecord;

const WORKER: &str = "child_admission::ownership::failure_close::real_unknown_finalizer_faults::real_unknown_finalizer_worker";

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    tenant: TenantId,
    run: RunId,
}

struct Pending {
    value: Started,
    child: RunId,
    fence: RunFence,
    tool: CloseUnknownTool,
    invocation: ToolInvocation,
    original: RunFailureCloseRecord,
}

struct Prepared {
    identity: Identity,
    key: ChildRunKey,
    original: RunFailureCloseRecord,
    account: ChildRunBudgetAccount,
    child_ack: JournalEvent,
    invocation: ToolInvocation,
    tool: CloseUnknownTool,
}

fn full_digest<T: serde::Serialize>(value: &T) -> Digest {
    Digest::sha256(serde_json_canonicalizer::to_vec(value).unwrap())
}

fn record_value(record: &RunFailureCloseRecord) -> Value {
    json!({"failure": record.failure(), "direct": record.direct_usage(),
        "checkpoint": record.checkpoint(), "lifecycle": record.lifecycle(),
        "registration": record.registration(), "completed_at": record.completed_at()})
}

async fn terminal_ack(store: &PostgresStore, identity: &Identity) -> JournalEvent {
    let page = store
        .load_journal_page(
            &identity.tenant,
            identity.run,
            None,
            JournalPageSize::new(256).unwrap(),
        )
        .await
        .unwrap();
    assert!(!page.has_more());
    assert_eq!(
        page.events()
            .iter()
            .filter(|event| event.payload().kind().as_str() == "run-failure-close-completed")
            .count(),
        1
    );
    page.events().last().unwrap().clone()
}

async fn cold_observation(store: &PostgresStore, identity: &Identity) -> Value {
    let close = store
        .load_run_failure_close(&identity.tenant, identity.run)
        .await
        .unwrap()
        .unwrap();
    let run = store
        .load_run(&identity.tenant, identity.run)
        .await
        .unwrap();
    let account = store
        .load_child_budget_account(&identity.tenant, identity.run)
        .await
        .unwrap()
        .unwrap();
    let event = terminal_ack(store, identity).await;
    assert_eq!(run.lifecycle().status(), RunStatus::Failed);
    assert!(run.lease().is_none());
    assert_eq!(run.journal_head(), Some(&event.head()));
    assert_eq!(
        json!(run.lifecycle().terminal_failure()),
        json!(Some(close.failure()))
    );
    assert_eq!(close.completed_at(), Some(run.lifecycle().changed_at()));
    assert!(close.direct_usage().known_costs().is_empty());
    assert_eq!(account.direct_head(), &event.head());
    assert_eq!(account.direct_usage(), close.direct_usage());
    assert_eq!(account.children().len(), 1);
    assert!(account.children()[0].settlement().is_some());
    let delegated = account.delegated_usage().unwrap();
    assert_eq!(
        json!(delegated.known_costs()),
        json!([{"currency":"USD","micro_units":"11"}])
    );
    assert_eq!(
        run.lifecycle().terminal_usage(),
        Some(&close.direct_usage().checked_accumulate(&delegated).unwrap())
    );
    json!({"record": record_value(&close), "terminal_ack": full_digest(&event),
        "whole_account": full_digest(&account)})
}

// Called only with --exact and an authenticated harness input. No providers are
// registered in this process, so recovery cannot perform another business send.
#[tokio::test]
async fn real_unknown_finalizer_worker() {
    let Ok(input) = std::env::var(INPUT_ENV) else {
        return;
    };
    watch_parent();
    let input: Value = serde_json::from_str(&input).unwrap();
    let identity: Identity = serde_json::from_value(input["identity"].clone()).unwrap();
    let store = worker_store(&input).await;
    let report = tick(&store, &identity.tenant).await;
    assert!(!report.is_cancelled());
    for item in report.items() {
        assert!(item.result().unwrap().record().completed_at().is_some());
    }
    assert_ne!(
        input["role"], "cut",
        "finalizer returned before the controller killed the actual COMMIT cut"
    );
    assert_eq!(input["role"], "recover");
    let pool = sql_pool().await;
    let before = close_unknown_snapshot(&pool, &identity.tenant).await;
    let observation = cold_observation(&store, &identity).await;
    assert!(tick(&store, &identity.tenant).await.items().is_empty());
    assert_eq!(cold_observation(&store, &identity).await, observation);
    assert_eq!(
        close_unknown_snapshot(&pool, &identity.tenant).await,
        before
    );
    ready_and_park(
        json!({"items": report.items().len(), "observation": observation,
        "second_tick_and_complete_reads_tenant_unchanged": true}),
    )
    .await;
    panic!("recovery worker escaped its controller-owned kill point");
}

async fn prepare_pending(
    store: &PostgresStore,
    pool: &PgPool,
    name: &str,
    tracked: &mut Vec<(TenantId, RunId)>,
    checks: &mut Vec<(&'static str, bool)>,
) -> Pending {
    let (mut value, child) = Box::pin(close_unknown_setup(store, name, tracked)).await;
    let key = value.intent.key().clone();
    let fence = close_unknown_claim(store, key.tenant_id(), child).await;
    let tool = close_tool(true);
    let invocation = Box::pin(close_tool_start(store, &fence, &tool)).await;
    assert!(
        matches!(invocation.state(), ToolInvocationState::Unknown { error }
        if error.external_effect() == ToolExternalEffect::Unknown
            && error.failure().retry_advice() == RetryAdvice::ReconcileFirst)
    );
    let failure = Box::pin(close_unknown_node_failure(store, &mut value)).await;
    let original = Box::pin(close_unknown_parent(
        store, pool, &value, &failure, None, checks,
    ))
    .await;
    let delivered = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(delivered.items().len(), 1);
    assert!(delivered.items()[0].result().is_ok());
    assert_eq!(
        store
            .load_run(key.tenant_id(), child)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::CancellationRequested
    );
    let receipt = store.load_child_cancellation(&key).await.unwrap();
    assert!(receipt.is_parent_failure_close());
    assert_eq!(receipt.parent_head(), &original.registration().head());
    assert!(receipt.receipt().is_some());
    Pending {
        value,
        child,
        fence,
        tool,
        invocation,
        original,
    }
}

async fn prepare_settled(
    store: &mut PostgresStore,
    pool: &PgPool,
    pending: Pending,
    checks: &mut Vec<(&'static str, bool)>,
) -> Prepared {
    let key = pending.value.intent.key().clone();
    Box::pin(close_unknown_unsettled(store, pool, &key, checks)).await;
    let recovered = Box::pin(close_unknown_cold(
        store,
        pool,
        &key,
        pending.child,
        &pending.invocation,
        &pending.fence,
        checks,
    ))
    .await;
    Box::pin(close_unknown_unsettled(store, pool, &key, checks)).await;
    Box::pin(close_unknown_reconcile(
        store,
        pool,
        &recovered,
        &pending.invocation,
        &pending.tool,
        checks,
    ))
    .await;
    let child_ack = Box::pin(close_unknown_child(
        store,
        pool,
        &pending.value,
        &recovered,
        pending.invocation.intent().invocation_id(),
        checks,
    ))
    .await;
    let settled = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(settled.items().len(), 1);
    assert!(settled.items()[0].result().is_ok());
    let (account, invocation) = Box::pin(settled_observation(
        store,
        &key,
        pending.invocation.intent().invocation_id(),
    ))
    .await;
    assert!(pending.original.direct_usage().known_costs().is_empty());
    Prepared {
        identity: Identity {
            tenant: key.tenant_id().clone(),
            run: key.parent_run_id(),
        },
        key,
        original: pending.original,
        account,
        child_ack,
        invocation,
        tool: pending.tool,
    }
}

async fn settled_observation(
    store: &PostgresStore,
    key: &ChildRunKey,
    invocation: InvocationId,
) -> (ChildRunBudgetAccount, ToolInvocation) {
    let child = store.load_child_run(key).await.unwrap();
    let account = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let usage = child.child().run().lifecycle().terminal_usage().unwrap();
    assert_eq!(child.settlement().unwrap().usage(), usage);
    assert_eq!(account.children().len(), 1);
    assert_eq!(
        account.entry(key).unwrap().settlement().unwrap().usage(),
        usage
    );
    assert_eq!(account.delegated_usage().unwrap(), usage.cumulative_only());
    assert_eq!(
        json!(usage.known_costs()),
        json!([{"currency":"USD","micro_units":"11"}])
    );
    let child_id = child.child().admission().intent().provenance().run_id();
    let current = store
        .load_tool_invocation(key.tenant_id(), child_id, invocation)
        .await
        .unwrap();
    assert!(matches!(
        current.state(),
        ToolInvocationState::Committed { .. }
    ));
    (account, current)
}

type TenantSnapshot = BTreeMap<String, String>;

fn rows(snapshot: &TenantSnapshot, table: &str) -> Vec<Value> {
    serde_json::from_str(snapshot.get(table).unwrap()).unwrap()
}

fn only_parent_columns(
    before: &TenantSnapshot,
    after: &TenantSnapshot,
    table: &str,
    id: &str,
    parent: RunId,
    mutable: &[&str],
) {
    let normalize = |snapshot: &TenantSnapshot| {
        let mut values = rows(snapshot, table);
        for value in &mut values {
            if value[id] == json!(parent) {
                for key in mutable {
                    value.as_object_mut().unwrap().remove(*key);
                }
            }
        }
        values.sort_by_cached_key(Value::to_string);
        values
    };
    assert_eq!(
        normalize(before),
        normalize(after),
        "unexpected {table} mutation"
    );
}

fn terminal_delta(before: &TenantSnapshot, after: &TenantSnapshot, parent: RunId) {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    for (table, value) in before {
        if ![
            "run_events",
            "runs",
            "run_failure_closes",
            "child_run_budget_accounts",
        ]
        .contains(&table.as_str())
        {
            assert_eq!(after.get(table), Some(value), "terminal changed {table}");
        }
    }
    only_parent_columns(
        before,
        after,
        "runs",
        "run_id",
        parent,
        &[
            "journal_sequence",
            "journal_event_id",
            "journal_recorded_at",
            "journal_digest",
            "lifecycle_bytes",
            "lifecycle_revision",
            "lifecycle_status",
            "changed_at",
            "scheduler_ready_at",
            "scheduler_not_before",
            "lease_attempt_id",
            "lease_acquired_at",
            "lease_renewed_at",
            "lease_expires_at",
            "wait_set_digest",
            "unresolved_wait_count",
            "next_timer_due_at",
            "next_interrupt_expiry_at",
            "updated_at",
        ],
    );
    only_parent_columns(
        before,
        after,
        "child_run_budget_accounts",
        "parent_run_id",
        parent,
        &[
            "account_bytes",
            "account_digest",
            "journal_sequence",
            "journal_event_id",
            "journal_recorded_at",
            "journal_digest",
        ],
    );
    only_parent_columns(
        before,
        after,
        "run_failure_closes",
        "run_id",
        parent,
        &["completed_at"],
    );
    let old = rows(before, "run_events");
    let new = rows(after, "run_events");
    assert_eq!(new.len(), old.len() + 1);
    assert!(old.iter().all(|row| new.contains(row)));
    let added: Vec<_> = new.iter().filter(|row| !old.contains(row)).collect();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0]["run_id"], json!(parent));
}

async fn verify_terminal(store: &PostgresStore, prepared: &Prepared) -> JournalEvent {
    let key = &prepared.key;
    let event = terminal_ack(store, &prepared.identity).await;
    let close = store
        .load_run_failure_close(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let mut expected_record = record_value(&prepared.original);
    expected_record["completed_at"] = json!(close.completed_at());
    assert_eq!(record_value(&close), expected_record);
    assert!(close.completed_at().is_some());
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Failed);
    assert_eq!(run.journal_head(), Some(&event.head()));
    assert!(run.lease().is_none());
    assert_eq!(
        json!(run.lifecycle().terminal_failure()),
        json!(Some(prepared.original.failure()))
    );
    assert_eq!(close.completed_at(), Some(run.lifecycle().changed_at()));
    let expected = prepared
        .account
        .observe_direct(event.head(), prepared.original.direct_usage().clone())
        .unwrap();
    let actual = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.children(), prepared.account.children());
    assert_eq!(
        actual.delegated_usage().unwrap(),
        prepared.account.delegated_usage().unwrap()
    );
    assert!(actual.direct_usage().known_costs().is_empty());
    let total = prepared
        .original
        .direct_usage()
        .checked_accumulate(&prepared.account.delegated_usage().unwrap())
        .unwrap();
    assert_eq!(run.lifecycle().terminal_usage(), Some(&total));
    let child = store.load_child_run(key).await.unwrap();
    assert_eq!(
        child.child().run().journal_head(),
        Some(&prepared.child_ack.head())
    );
    assert_eq!(
        child.settlement(),
        prepared.account.entry(key).unwrap().settlement()
    );
    let child_id = child.child().admission().intent().provenance().run_id();
    let invocation = store
        .load_tool_invocation(
            key.tenant_id(),
            child_id,
            prepared.invocation.intent().invocation_id(),
        )
        .await
        .unwrap();
    assert_eq!(json!(invocation), json!(prepared.invocation));
    assert_eq!(prepared.tool.backend.sends.load(Ordering::SeqCst), 1);
    event
}

async fn cut_writer(
    store: &PostgresStore,
    pool: &PgPool,
    connection: &PgConnectOptions,
    prepared: &Prepared,
    cut: Cut,
    before: &TenantSnapshot,
) -> (TenantSnapshot, Option<JournalEvent>) {
    let mut proxy =
        CommitProxy::start(connection, cut, TransactionTarget::FailureCloseCompletion).await;
    let mut writer = TestProcess::spawn(
        WORKER,
        &json!({"role":"cut", "identity":prepared.identity, "proxy_port":proxy.port()}),
    );
    proxy.wait_cut(cut).await;
    let pid = proxy.backend_pid();
    // Raw MVCC reads cannot block on the writer's uncommitted parent row lock.
    let at_cut = close_unknown_snapshot(pool, &prepared.identity.tenant).await;
    if cut == Cut::BeforeCommit {
        assert_eq!(&at_cut, before, "unforwarded COMMIT leaked a tenant write");
    } else {
        terminal_delta(before, &at_cut, prepared.identity.run);
    }
    let status = writer.kill_and_reap().await;
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }
    proxy.stop().await;
    wait_for_backend_disconnect(pool, pid).await;
    assert_eq!(
        close_unknown_snapshot(pool, &prepared.identity.tenant).await,
        at_cut
    );
    let committed = if cut == Cut::CommitResponse {
        Some(Box::pin(verify_terminal(store, prepared)).await)
    } else {
        assert_eq!(
            store
                .load_child_budget_account(prepared.key.tenant_id(), prepared.key.parent_run_id())
                .await
                .unwrap(),
            Some(prepared.account.clone())
        );
        assert_eq!(
            record_value(
                &store
                    .load_run_failure_close(prepared.key.tenant_id(), prepared.key.parent_run_id())
                    .await
                    .unwrap()
                    .unwrap()
            ),
            record_value(&prepared.original)
        );
        None
    };
    (at_cut, committed)
}

async fn recover_and_verify(
    store: &PostgresStore,
    pool: &PgPool,
    prepared: &Prepared,
    cut: Cut,
    before: &TenantSnapshot,
    at_cut: &TenantSnapshot,
    committed: Option<&JournalEvent>,
) -> Value {
    let mut cold = TestProcess::spawn(
        WORKER,
        &json!({"role":"recover", "identity":prepared.identity}),
    );
    let ready = cold.ready().await.unwrap();
    assert_eq!(ready["items"], json!(usize::from(cut == Cut::BeforeCommit)));
    assert_eq!(
        ready["second_tick_and_complete_reads_tenant_unchanged"],
        true
    );
    let event = Box::pin(verify_terminal(store, prepared)).await;
    if let Some(original) = committed {
        assert_eq!(&event, original);
    }
    let observed = cold_observation(store, &prepared.identity).await;
    assert_eq!(ready["observation"], observed);
    let after = close_unknown_snapshot(pool, &prepared.identity.tenant).await;
    terminal_delta(before, &after, prepared.identity.run);
    if cut == Cut::CommitResponse {
        assert_eq!(&after, at_cut);
    }
    cold.kill_and_reap().await;
    assert_eq!(
        close_unknown_snapshot(pool, &prepared.identity.tenant).await,
        after
    );
    assert!(
        tick(store, &prepared.identity.tenant)
            .await
            .items()
            .is_empty()
    );
    assert_eq!(Box::pin(verify_terminal(store, prepared)).await, event);
    assert_eq!(cold_observation(store, &prepared.identity).await, observed);
    assert_eq!(
        close_unknown_snapshot(pool, &prepared.identity.tenant).await,
        after
    );
    json!({"cut":if cut == Cut::BeforeCommit {"commit_not_forwarded"} else {"commit_response_withheld"},
        "writer_killed_and_reaped":true, "cold_identity_only":true,
        "complete_original_terminal_ack":full_digest(&event), "whole_account_verified":true,
        "replay_scope":"public tick plus complete owning reads; no Store Existing replay",
        "business_sends":prepared.tool.backend.sends.load(Ordering::SeqCst)})
}

async fn run_cut(
    store: &mut PostgresStore,
    pool: &PgPool,
    connection: &PgConnectOptions,
    cut: Cut,
    tracked: &mut Vec<(TenantId, RunId)>,
) -> Value {
    let mut checks = Vec::new();
    let name = if cut == Cut::BeforeCommit {
        "unknown-finalizer-before"
    } else {
        "unknown-finalizer-after"
    };
    let pending = Box::pin(prepare_pending(store, pool, name, tracked, &mut checks)).await;
    let prepared = Box::pin(prepare_settled(store, pool, pending, &mut checks)).await;
    for (label, passed) in &checks {
        assert!(*passed, "{label}");
    }
    let before = close_unknown_snapshot(pool, &prepared.identity.tenant).await;
    let (at_cut, committed) =
        Box::pin(cut_writer(store, pool, connection, &prepared, cut, &before)).await;
    let evidence = Box::pin(recover_and_verify(
        store,
        pool,
        &prepared,
        cut,
        &before,
        &at_cut,
        committed.as_ref(),
    ))
    .await;
    json!({"checks":checks, "evidence":evidence,
        "pre_terminal_whole_account":prepared.account,
        "frozen_direct":prepared.original.direct_usage(),
        "original_registration_ack":prepared.original.registration(),
        "original_child_terminal_ack":prepared.child_ack,
        "at_cut_complete_tenant":full_digest(&at_cut),
        "before_complete_tenant":full_digest(&before)})
}

#[tokio::test]
async fn real_tool_unknown_finalizer_commit_loss_survives_kill_and_cold_owning_reads() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(mut store) = test_store().await else {
        return;
    };
    let connection: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let _ = loopback_target(&connection);
    let pool = sql_pool().await;
    let mut tracked = Vec::new();
    let verdict = std::panic::AssertUnwindSafe(async {
        let mut observations = Vec::new();
        for cut in [Cut::BeforeCommit, Cut::CommitResponse] {
            observations
                .push(Box::pin(run_cut(&mut store, &pool, &connection, cut, &mut tracked)).await);
        }
        observations
    })
    .catch_unwind()
    .await;
    let mut released = true;
    for (tenant, run) in tracked {
        match store.load_run(&tenant, run).await {
            Ok(current) => {
                if let Some(lease) = current.lease() {
                    released &= store.release_lease(lease.fence()).await.is_ok();
                }
            }
            Err(_) => released = false,
        }
    }
    pool.close().await;
    store.close().await;
    let observations = match verdict {
        Ok(observations) => observations,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert!(released, "owned fixture leases must retire before verdict");
    eprintln!(
        "STATEKNOT_REAL_UNKNOWN_FINALIZER_FAULT_EVIDENCE={}",
        json!({
            "profile":"real-unknown-finalizer-commit-loss-v1", "cuts":observations,
            "provider_reconciliation_precedes_killed_writer":true,
            "provider_backend_process_durability":false, "original_limits_preserved":true,
            "os":std::env::consts::OS, "arch":std::env::consts::ARCH,
        })
    );
}
