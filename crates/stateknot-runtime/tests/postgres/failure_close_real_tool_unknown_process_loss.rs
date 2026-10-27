// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Framework Unknown survives writer loss. The controller retains the actual
//! fixture receipt; this is not independent external-provider durability.

use super::*;
use stateknot_core::ChildRunBudgetSettlement;
use stateknot_store_postgres::StoredRun;

const WORKER: &str = "child_admission::ownership::failure_close::real_unknown_finalizer_faults::unresolved_unknown_faults::unresolved_unknown_process_worker";

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct PendingIdentity {
    key: ChildRunKey,
    invocation: InvocationId,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CapturedReceipt {
    tenant: TenantId,
    run: RunId,
    thread: ThreadId,
    origin: EventId,
    input: ToolInput,
    result: ToolResult,
}

struct InterruptedUnknown {
    identity: PendingIdentity,
    original: ToolInvocation,
    close: RunFailureCloseRecord,
    account: ChildRunBudgetAccount,
    facts: Value,
    ready: Value,
}

fn receipt_value(receipt: &CloseToolReceipt) -> Value {
    json!({"tenant":receipt.tenant, "run":receipt.run, "thread":receipt.thread,
        "origin":receipt.origin, "input":receipt.input, "result":receipt.result})
}

// Lease and scheduling writes are checked separately against exact SQL columns.
// No account, lifecycle, invocation, checkpoint or journal field is excluded.
fn stable_run(run: &StoredRun) -> Value {
    json!({"lifecycle":run.lifecycle(), "head":run.journal_head(),
        "checkpoint":run.checkpoint().map(|p| json!({"id":p.checkpoint_id(),
            "step":p.superstep(), "digest":p.digest()})),
        "wait_digest":run.wait_set_digest(), "wait_count":run.unresolved_wait_count(),
        "timer":run.next_timer_due_at(), "interrupt":run.next_interrupt_expiry_at(),
        "quarantined":run.is_quarantined()})
}

async fn full_journal(store: &PostgresStore, tenant: &TenantId, run: RunId) -> Vec<JournalEvent> {
    let page = store
        .load_journal_page(tenant, run, None, JournalPageSize::new(256).unwrap())
        .await
        .unwrap();
    assert!(!page.has_more());
    page.events().to_vec()
}

async fn tool_history(store: &PostgresStore, original: &ToolInvocation) -> Value {
    let mut head = original.head();
    let mut revisions = Vec::new();
    for _ in 0..4 {
        let (event, record) = store
            .load_tool_invocation_revision(
                original.intent().tenant_id(),
                original.intent().run_id(),
                original.intent().invocation_id(),
                head.revision(),
            )
            .await
            .unwrap();
        assert_eq!(event.head(), record.journal_head().clone());
        let previous = record.previous().cloned();
        revisions.push(json!({"event":event, "invocation":record}));
        let Some(previous) = previous else { break };
        head = previous;
    }
    assert_eq!(revisions.len(), 3);
    assert_eq!(revisions[0]["invocation"], json!(original));
    json!(revisions)
}

async fn pending_accounts(store: &PostgresStore, key: &ChildRunKey, child: RunId) -> Value {
    let parent = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let child = store
        .load_child_budget_account(key.tenant_id(), child)
        .await
        .unwrap();
    assert_eq!(parent.children().len(), 1);
    assert!(parent.entry(key).unwrap().settlement().is_none());
    assert!(child.is_none());
    json!({"whole_parent_account":parent, "whole_child_account":child})
}

fn cancellation_value(record: &stateknot_store_postgres::ChildCancellationRecord) -> Value {
    let receipt = record.receipt().unwrap();
    json!({"key":record.key(), "child":record.child_run_id(),
        "parent_lifecycle":record.parent_lifecycle(), "parent_head":record.parent_head(),
        "queued_at":record.queued_at(), "receipt":{"head":receipt.head(),
            "lifecycle":receipt.lifecycle(), "delivered_at":receipt.delivered_at(),
            "outcome":format!("{:?}",receipt.outcome())}})
}

async fn pending_facts(store: &PostgresStore, identity: &PendingIdentity) -> Value {
    let key = &identity.key;
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let child = store.load_child_run(key).await.unwrap();
    let run = child.child().run();
    let child_id = run.lifecycle().provenance().run_id();
    let original = store
        .load_tool_invocation(key.tenant_id(), child_id, identity.invocation)
        .await
        .unwrap();
    assert!(
        matches!(original.state(), ToolInvocationState::Unknown { error }
        if error.external_effect() == ToolExternalEffect::Unknown
            && error.failure().retry_advice() == RetryAdvice::ReconcileFirst)
    );
    let close = store
        .load_run_failure_close(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let cancellation = cancellation_value(&store.load_child_cancellation(key).await.unwrap());
    assert_eq!(parent.lifecycle().status(), RunStatus::Active);
    assert_eq!(run.lifecycle().status(), RunStatus::CancellationRequested);
    assert!(parent.lease().is_none());
    assert!(parent.lifecycle().terminal_usage().is_none());
    assert!(run.lifecycle().terminal_usage().is_none());
    assert!(close.completed_at().is_none());
    assert!(child.settlement().is_none());
    assert_eq!(
        cancellation["parent_head"],
        json!(close.registration().head())
    );
    let parent_journal = full_journal(store, key.tenant_id(), key.parent_run_id()).await;
    let child_journal = full_journal(store, key.tenant_id(), child_id).await;
    assert!(
        child_journal
            .iter()
            .any(|event| json!(event.head()) == cancellation["receipt"]["head"])
    );
    json!({"parent":stable_run(&parent), "parent_lease":parent.lease(),
        "parent_epoch":parent.last_fencing_epoch(),
        "parent_ready":parent.scheduler_ready_at(), "parent_not_before":parent.scheduler_not_before(),
        "child":stable_run(run), "tool_history":tool_history(store,&original).await,
        "close":record_value(&close), "accounts":pending_accounts(store,key,child_id).await,
        "ownership":{"intent":child.intent(), "spawn":child.spawn(),
            "ancestors":child.ancestors(), "settlement":child.settlement()},
        "cancellation":cancellation, "parent_journal":parent_journal, "child_journal":child_journal})
}

fn lease_delta(before: &TenantSnapshot, after: &TenantSnapshot, child: RunId, columns: &[&str]) {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    for (table, rows) in before {
        if table != "runs" {
            assert_eq!(after.get(table), Some(rows), "lease changed {table}");
        }
    }
    only_parent_columns(before, after, "runs", "run_id", child, columns);
}

async fn wait_expiry(store: &PostgresStore, expires: Timestamp) {
    tokio::time::timeout(Duration::from_secs(45), async {
        while store.observe_database_clock().await.unwrap() < expires {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("observe the original 30-second lease's natural database-clock expiry");
}

async fn produce_unknown(store: &PostgresStore, pool: &PgPool) {
    let mut tracked = Vec::new();
    let mut checks = Vec::new();
    let pending = Box::pin(prepare_pending(
        store,
        pool,
        "unresolved-unknown-kill",
        &mut tracked,
        &mut checks,
    ))
    .await;
    assert!(checks.iter().all(|(_, passed)| *passed));
    let identity = PendingIdentity {
        key: pending.value.intent.key().clone(),
        invocation: pending.invocation.intent().invocation_id(),
    };
    let facts = Box::pin(pending_facts(store, &identity)).await;
    let receipt = pending
        .tool
        .backend
        .receipt
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    assert!(pending.tool.backend.consent.lock().unwrap().is_none());
    assert_eq!(pending.tool.backend.sends.load(Ordering::SeqCst), 1);
    assert_eq!(pending.tool.backend.probes.load(Ordering::SeqCst), 0);
    let data = json!({"identity":identity, "actual_receipt":receipt_value(&receipt),
        "writer_business_sends":1, "writer_probes":0, "facts_digest":full_digest(&facts)});
    assert!(serde_json::to_vec(&data).unwrap().len() < 32 * 1024);
    ready_and_park(data).await;
}

async fn claim_after_expiry(
    store: &PostgresStore,
    pool: &PgPool,
    identity: &PendingIdentity,
    facts: &Value,
) -> RunFence {
    let key = &identity.key;
    let child = store.load_child_run(key).await.unwrap();
    let child_id = child.child().run().lifecycle().provenance().run_id();
    let old = child.child().run().lease().unwrap().clone();
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    wait_expiry(store, old.expires_at()).await;
    assert_eq!(close_unknown_snapshot(pool, key.tenant_id()).await, before);
    let claimed = store
        .claim_lease(key.tenant_id(), child_id, AttemptId::generate())
        .await
        .unwrap();
    let fence = claimed.lease().fence().clone();
    assert!(fence.epoch() > old.fence().epoch());
    assert_ne!(fence.attempt_id(), old.fence().attempt_id());
    let at_claim = close_unknown_snapshot(pool, key.tenant_id()).await;
    lease_delta(
        &before,
        &at_claim,
        child_id,
        &[
            "fencing_epoch",
            "lease_attempt_id",
            "lease_acquired_at",
            "lease_renewed_at",
            "lease_expires_at",
            "scheduler_not_before",
            "updated_at",
        ],
    );
    assert_eq!(&Box::pin(pending_facts(store, identity)).await, facts);
    fence
}

async fn drive_pending_child(
    store: &PostgresStore,
    pool: &PgPool,
    identity: &PendingIdentity,
    fence: &RunFence,
    facts: &Value,
) {
    let child = store
        .load_run(identity.key.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let before = close_unknown_snapshot(pool, identity.key.tenant_id()).await;
    let fixture = driver_fixture();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let outcome = driver
        .drive(fence.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::CancellationRequested(handoff) = outcome.outcome() else {
        panic!("Unknown child cancellation must remain a lifecycle handoff: {outcome:?}");
    };
    assert_eq!(handoff.lease().fence(), fence);
    assert_eq!(handoff.journal_head(), child.journal_head().unwrap());
    assert_eq!(outcome.report().durable_events(), 0);
    assert_eq!(outcome.report().node_attempts_started(), 0);
    assert_eq!(outcome.report().node_attempts_completed(), 0);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    let after = close_unknown_snapshot(pool, identity.key.tenant_id()).await;
    lease_delta(
        &before,
        &after,
        fence.run_id(),
        &["lease_renewed_at", "lease_expires_at", "updated_at"],
    );
    assert_eq!(&Box::pin(pending_facts(store, identity)).await, facts);
}

async fn release_and_assert_blocked(
    store: &PostgresStore,
    pool: &PgPool,
    identity: &PendingIdentity,
    fence: &RunFence,
    facts: &Value,
) {
    let key = &identity.key;
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    store.release_lease(fence).await.unwrap();
    let released = close_unknown_snapshot(pool, key.tenant_id()).await;
    lease_delta(
        &before,
        &released,
        fence.run_id(),
        &[
            "lease_attempt_id",
            "lease_acquired_at",
            "lease_renewed_at",
            "lease_expires_at",
            "scheduler_ready_at",
            "scheduler_not_before",
            "updated_at",
        ],
    );
    assert!(
        store
            .load_run(key.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    let mut checks = Vec::new();
    Box::pin(close_unknown_unsettled(store, pool, key, &mut checks)).await;
    assert!(checks.iter().all(|(_, passed)| *passed));
    assert_eq!(&Box::pin(pending_facts(store, identity)).await, facts);
    assert_eq!(
        close_unknown_snapshot(pool, key.tenant_id()).await,
        released
    );
}

async fn cold_driver(store: &PostgresStore, pool: &PgPool, identity: &PendingIdentity) {
    let facts = Box::pin(pending_facts(store, identity)).await;
    let fence = Box::pin(claim_after_expiry(store, pool, identity, &facts)).await;
    Box::pin(drive_pending_child(store, pool, identity, &fence, &facts)).await;
    Box::pin(release_and_assert_blocked(
        store, pool, identity, &fence, &facts,
    ))
    .await;
    ready_and_park(
        json!({"facts_digest":full_digest(&facts), "cold_fence":fence,
        "driver_durable_events":0, "application_calls":0, "parent_still_blocked":true,
        "claim_driver_release_delta_verified":true, "read_and_rejection_window_unchanged":true}),
    )
    .await;
}

#[tokio::test]
async fn unresolved_unknown_process_worker() {
    let Ok(input) = std::env::var(INPUT_ENV) else {
        return;
    };
    watch_parent();
    let input: Value = serde_json::from_str(&input).unwrap();
    let store = test_store()
        .await
        .expect("Unknown process-loss requires PostgreSQL");
    assert_eq!(store.options().lease_duration(), Duration::from_secs(30));
    let pool = sql_pool().await;
    match input["role"].as_str().unwrap() {
        "writer" => Box::pin(produce_unknown(&store, &pool)).await,
        "cold" => {
            let identity = serde_json::from_value(input["identity"].clone()).unwrap();
            Box::pin(cold_driver(&store, &pool, &identity)).await;
        }
        _ => panic!("unknown unresolved-Unknown process role"),
    }
    panic!("owned worker escaped its controller kill point");
}

fn restore_receipt(captured: CapturedReceipt, original: &ToolInvocation) -> CloseUnknownTool {
    let tool = close_tool(true);
    assert_eq!(original.intent().descriptor(), &tool.descriptor);
    assert_eq!(original.intent().tenant_id(), &captured.tenant);
    assert_eq!(original.intent().run_id(), captured.run);
    assert_eq!(original.intent().input(), &captured.input);
    assert_eq!(
        original.intent().invocation_id(),
        captured.result.provenance().invocation_id()
    );
    assert_eq!(
        original.attempt_id(),
        Some(captured.result.provenance().attempt_id())
    );
    assert_eq!(
        original.previous().unwrap().journal_head().event_id(),
        captured.origin
    );
    assert_eq!(
        captured.result.output_schema(),
        tool.descriptor.output_schema()
    );
    *tool.backend.receipt.lock().unwrap() = Some(CloseToolReceipt {
        tenant: captured.tenant,
        run: captured.run,
        thread: captured.thread,
        origin: captured.origin,
        input: captured.input,
        result: captured.result,
    });
    assert!(tool.backend.consent.lock().unwrap().is_none());
    assert_eq!(tool.backend.sends.load(Ordering::SeqCst), 0);
    tool
}

async fn reconcile_original(
    store: &PostgresStore,
    pool: &PgPool,
    fence: &RunFence,
    original: &ToolInvocation,
    tool: &CloseUnknownTool,
) -> JournalEvent {
    let executor = close_tool_executor(store, tool);
    let attempt = || {
        ToolReconciliationAttemptHandoff::new(
            fence.clone(),
            original.clone(),
            EventId::generate(),
            CancellationSignal::never(),
        )
        .unwrap()
    };
    let before = close_unknown_snapshot(pool, fence.tenant_id()).await;
    let denied = executor.reconcile_tool(attempt()).await;
    assert!(matches!(denied,
        Err(stateknot_runtime::ToolReconciliationAttemptExecutionError::Probe(error))
        if error.failure().code().as_str() == "test.close.reconciliation_denied"));
    assert_eq!(
        close_unknown_snapshot(pool, fence.tenant_id()).await,
        before
    );
    *tool.backend.consent.lock().unwrap() = Some((original.clone(), "read-original-write-receipt"));
    let resolved = executor.reconcile_tool(attempt()).await.unwrap();
    assert!(matches!(
        resolved,
        ToolReconciliationAttemptOutcome::Resolved {
            kind: ToolReconciliationKind::Result,
            ..
        }
    ));
    let current = resolved.invocation().clone();
    let (ack, owned) = store
        .load_tool_invocation_revision(
            fence.tenant_id(),
            fence.run_id(),
            current.intent().invocation_id(),
            current.revision(),
        )
        .await
        .unwrap();
    assert_eq!(json!(owned), json!(current));
    let receipt = tool.backend.receipt.lock().unwrap().clone().unwrap();
    assert!(
        matches!(current.state(), ToolInvocationState::Committed { result } if result == &receipt.result)
    );
    let before = close_unknown_snapshot(pool, fence.tenant_id()).await;
    let replay = executor
        .commit_tool_reconciliation(
            ToolReconciliationHandoff::result(
                fence.clone(),
                original.clone(),
                ack.event_id(),
                receipt.result,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        matches!(replay, ToolReconciliationOutcome::Idempotent { event, invocation, .. }
        if event == ack && json!(invocation) == json!(current))
    );
    assert_eq!(
        close_unknown_snapshot(pool, fence.tenant_id()).await,
        before
    );
    assert_eq!(tool.backend.sends.load(Ordering::SeqCst), 0);
    assert_eq!(tool.backend.probes.load(Ordering::SeqCst), 2);
    assert_eq!(tool.backend.denied.load(Ordering::SeqCst), 1);
    ack
}

async fn cancel_original_child(
    store: &PostgresStore,
    pool: &PgPool,
    identity: &PendingIdentity,
    fence: &RunFence,
) -> JournalEvent {
    let fixture = driver_fixture();
    let calls = Arc::new(AtomicUsize::new(0));
    let evidence = Arc::new(CloseUnknownEvidence {
        store: store.clone(),
        pool: pool.clone(),
        key: identity.key.clone(),
        tool: Some(identity.invocation),
        node: None,
        failure: None,
        calls: Arc::clone(&calls),
    });
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        fixture.registry,
        evidence,
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let result = agent_loop
        .run(fence.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let AgentLoopOutcome::CancellationConfirmed(outcome) = result.outcome() else {
        panic!("authorized original receipt must permit complete child cancellation: {result:?}");
    };
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    outcome.event().clone()
}

async fn settle_and_finish(
    store: &PostgresStore,
    pool: &PgPool,
    identity: &PendingIdentity,
    original: &RunFailureCloseRecord,
    before: &ChildRunBudgetAccount,
    child_ack: &JournalEvent,
) -> Value {
    let key = &identity.key;
    let delivery = cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(delivery.items().len(), 1);
    delivery.items()[0].result().unwrap();
    let child = store.load_child_run(key).await.unwrap();
    let evidence = ChildRunBudgetSettlement::new(
        child.child().admission(),
        child.child().run().lifecycle(),
        child_ack.head(),
    )
    .unwrap();
    let expected = before.settle(key, evidence.clone()).unwrap();
    let settled = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(settled, expected);
    assert_eq!(child.settlement(), Some(&evidence));
    assert_eq!(child.child().run().journal_head(), Some(&child_ack.head()));
    assert_eq!(
        settled.delegated_usage().unwrap(),
        evidence.usage().cumulative_only()
    );
    assert_eq!(
        json!(evidence.usage().known_costs()),
        json!([{"currency":"USD","micro_units":"11"}])
    );
    let before_terminal = close_unknown_snapshot(pool, key.tenant_id()).await;
    let report = tick(store, key.tenant_id()).await;
    assert_eq!(report.items().len(), 1);
    assert!(
        report.items()[0]
            .result()
            .unwrap()
            .record()
            .completed_at()
            .is_some()
    );
    let after_terminal = close_unknown_snapshot(pool, key.tenant_id()).await;
    terminal_delta(&before_terminal, &after_terminal, key.parent_run_id());
    let parent_identity = Identity {
        tenant: key.tenant_id().clone(),
        run: key.parent_run_id(),
    };
    let ack = terminal_ack(store, &parent_identity).await;
    let account = settled
        .observe_direct(ack.head(), original.direct_usage().clone())
        .unwrap();
    assert_eq!(
        store
            .load_child_budget_account(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap(),
        Some(account.clone())
    );
    let close = store
        .load_run_failure_close(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let mut expected_record = record_value(original);
    expected_record["completed_at"] = json!(close.completed_at());
    assert_eq!(record_value(&close), expected_record);
    let observation = cold_observation(store, &parent_identity).await;
    assert!(
        store
            .load_child_budget_account(
                key.tenant_id(),
                child.child().run().lifecycle().provenance().run_id()
            )
            .await
            .unwrap()
            .is_none()
    );
    json!({"parent_terminal_ack":ack, "child_terminal_ack":child_ack,
        "original_registration_ack":original.registration(), "whole_parent_account":account,
        "whole_child_account":Value::Null, "observation":observation})
}

async fn verify_completed_replay(
    store: &PostgresStore,
    pool: &PgPool,
    identity: &PendingIdentity,
    completion: &Value,
    tool_ack: &JournalEvent,
) {
    let key = &identity.key;
    let child = store.load_child_run(key).await.unwrap();
    let child_id = child.child().run().lifecycle().provenance().run_id();
    let original_tool = store
        .load_tool_invocation(key.tenant_id(), child_id, identity.invocation)
        .await
        .unwrap();
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    assert!(tick(store, key.tenant_id()).await.items().is_empty());
    let (ack, current) = store
        .load_tool_invocation_revision(
            key.tenant_id(),
            child_id,
            identity.invocation,
            original_tool.revision(),
        )
        .await
        .unwrap();
    assert_eq!(&ack, tool_ack);
    assert_eq!(json!(current), json!(original_tool));
    let child_ack: JournalEvent =
        serde_json::from_value(completion["child_terminal_ack"].clone()).unwrap();
    assert_eq!(
        full_journal(store, key.tenant_id(), child_id).await.last(),
        Some(&child_ack)
    );
    let parent_identity = Identity {
        tenant: key.tenant_id().clone(),
        run: key.parent_run_id(),
    };
    assert_eq!(
        json!(terminal_ack(store, &parent_identity).await),
        completion["parent_terminal_ack"]
    );
    assert_eq!(
        cold_observation(store, &parent_identity).await,
        completion["observation"]
    );
    assert_eq!(
        json!(
            store
                .load_child_budget_account(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap()
                .unwrap()
        ),
        completion["whole_parent_account"]
    );
    assert_eq!(close_unknown_snapshot(pool, key.tenant_id()).await, before);
}

async fn interrupt_writer(
    store: &PostgresStore,
    pool: &PgPool,
    tracked: &mut Vec<(TenantId, RunId)>,
) -> InterruptedUnknown {
    let mut writer = TestProcess::spawn(WORKER, &json!({"role":"writer"}));
    let ready = writer.ready().await.unwrap();
    let identity: PendingIdentity = serde_json::from_value(ready["identity"].clone()).unwrap();
    let key = &identity.key;
    let child = store.load_child_run(key).await.unwrap();
    let child_id = child.child().run().lifecycle().provenance().run_id();
    tracked.extend([
        (key.tenant_id().clone(), key.parent_run_id()),
        (key.tenant_id().clone(), child_id),
    ]);
    let original = store
        .load_tool_invocation(key.tenant_id(), child_id, identity.invocation)
        .await
        .unwrap();
    let close = store
        .load_run_failure_close(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let account = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let facts = Box::pin(pending_facts(store, &identity)).await;
    assert_eq!(ready["facts_digest"], json!(full_digest(&facts)));
    assert_eq!(ready["writer_business_sends"], 1);
    assert_eq!(ready["writer_probes"], 0);
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    writer.kill_and_reap().await;
    assert_eq!(close_unknown_snapshot(pool, key.tenant_id()).await, before);
    assert_eq!(Box::pin(pending_facts(store, &identity)).await, facts);
    InterruptedUnknown {
        identity,
        original,
        close,
        account,
        facts,
        ready,
    }
}

async fn observe_cold_driver(
    store: &PostgresStore,
    pool: &PgPool,
    interrupted: &InterruptedUnknown,
) -> Value {
    let identity = &interrupted.identity;
    let key = &identity.key;
    let ready = &interrupted.ready;
    let facts = &interrupted.facts;
    let mut cold = TestProcess::spawn(WORKER, &json!({"role":"cold", "identity":identity}));
    let observed = cold.ready().await.unwrap();
    assert_eq!(observed["facts_digest"], ready["facts_digest"]);
    assert_eq!(observed["driver_durable_events"], 0);
    assert_eq!(observed["application_calls"], 0);
    assert_eq!(observed["parent_still_blocked"], true);
    assert_eq!(observed["claim_driver_release_delta_verified"], true);
    assert_eq!(observed["read_and_rejection_window_unchanged"], true);
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    cold.kill_and_reap().await;
    assert_eq!(close_unknown_snapshot(pool, key.tenant_id()).await, before);
    assert_eq!(Box::pin(pending_facts(store, identity)).await, *facts);
    observed
}

async fn claim_controller(
    store: &PostgresStore,
    pool: &PgPool,
    interrupted: &InterruptedUnknown,
    observed: &Value,
) -> RunFence {
    let key = &interrupted.identity.key;
    let child = store.load_child_run(key).await.unwrap();
    let child_id = child.child().run().lifecycle().provenance().run_id();
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    let fence = close_unknown_claim(store, key.tenant_id(), child_id).await;
    let at_claim = close_unknown_snapshot(pool, key.tenant_id()).await;
    lease_delta(
        &before,
        &at_claim,
        child_id,
        &[
            "fencing_epoch",
            "lease_attempt_id",
            "lease_acquired_at",
            "lease_renewed_at",
            "lease_expires_at",
            "scheduler_not_before",
            "updated_at",
        ],
    );
    let cold_fence: RunFence = serde_json::from_value(observed["cold_fence"].clone()).unwrap();
    assert!(fence.epoch() > cold_fence.epoch());
    assert_eq!(
        Box::pin(pending_facts(store, &interrupted.identity)).await,
        interrupted.facts
    );
    fence
}

async fn scenario(
    store: &PostgresStore,
    pool: &PgPool,
    tracked: &mut Vec<(TenantId, RunId)>,
) -> Value {
    let interrupted = Box::pin(interrupt_writer(store, pool, tracked)).await;
    let observed = Box::pin(observe_cold_driver(store, pool, &interrupted)).await;
    let fence = Box::pin(claim_controller(store, pool, &interrupted, &observed)).await;
    // Only now reconstruct a fixture provider from the captured actual receipt.
    // Its counters start at zero; they never impersonate the killed writer.
    let captured = serde_json::from_value(interrupted.ready["actual_receipt"].clone()).unwrap();
    let tool = restore_receipt(captured, &interrupted.original);
    let tool_ack = Box::pin(reconcile_original(
        store,
        pool,
        &fence,
        &interrupted.original,
        &tool,
    ))
    .await;
    let child_ack = Box::pin(cancel_original_child(
        store,
        pool,
        &interrupted.identity,
        &fence,
    ))
    .await;
    let completed = Box::pin(settle_and_finish(
        store,
        pool,
        &interrupted.identity,
        &interrupted.close,
        &interrupted.account,
        &child_ack,
    ))
    .await;
    Box::pin(verify_completed_replay(
        store,
        pool,
        &interrupted.identity,
        &completed,
        &tool_ack,
    ))
    .await;
    assert_eq!(tool.backend.sends.load(Ordering::SeqCst), 0);
    json!({"identity":interrupted.identity, "writer_business_sends":1, "recovery_business_sends":0,
        "original_unknown_history":tool_history(store,&interrupted.original).await,
        "reconciliation_ack":tool_ack, "completion":completed,
        "cold_driver":observed, "writer_killed_and_reaped":true,
        "cold_driver_observer_killed_and_reaped":true,
        "unknown_survived_writer_loss":true, "actual_receipt_retained_by_controller":true,
        "external_provider_backend_durability":false, "unknown_transaction_commit_fault":false})
}

#[tokio::test]
async fn unresolved_tool_unknown_survives_writer_kill_until_explicit_reconciliation() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    assert_eq!(store.options().lease_duration(), Duration::from_secs(30));
    let pool = sql_pool().await;
    let mut tracked = Vec::new();
    let verdict = std::panic::AssertUnwindSafe(Box::pin(scenario(&store, &pool, &mut tracked)))
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
    let observation = match verdict {
        Ok(value) => value,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert!(released, "owned fixture leases must retire before verdict");
    eprintln!(
        "STATEKNOT_UNRESOLVED_UNKNOWN_WRITER_LOSS_EVIDENCE={}",
        json!({
        "profile":"unresolved-unknown-writer-loss-v1", "observation":observation,
        "os":std::env::consts::OS, "arch":std::env::consts::ARCH,
        "lease30_ready90_default_stack":true, "qualification_awarded":false})
    );
}
