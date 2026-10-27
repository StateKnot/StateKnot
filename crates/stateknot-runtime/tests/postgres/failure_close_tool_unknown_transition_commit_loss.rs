// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real Tool outcome COMMIT cuts. Before stays Executing; it is never fabricated
//! into Unknown. The controller retains fixture facts, not an external backend.

use super::*;
use crate::process_harness::publish_ready;
use stateknot_core::ToolInvocationRevision;
use stateknot_runtime::{
    ToolAttemptExecutionError, ToolAttemptStartOutcome, ToolReconciliationAttemptHandoffError,
};

const OUTCOME_WORKER: &str = "child_admission::ownership::failure_close::real_unknown_finalizer_faults::unresolved_unknown_faults::outcome_commit_faults::tool_unknown_outcome_commit_worker";

#[derive(Clone)]
struct CapturingTool {
    inner: CloseUnknownTool,
    error: Arc<std::sync::Mutex<Option<ToolError>>>,
}

impl ErasedTool for CapturingTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.inner.descriptor
    }

    fn supports_reconciliation(&self) -> bool {
        self.inner.supports_reconciliation()
    }

    fn reconcile(
        &self,
        context: ToolReconciliationContext,
        input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolReconciliationObservation, ToolReconciliationProbeError>> {
        self.inner.reconcile(context, input)
    }

    fn call(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            let outcome = self.inner.call(context, input).await;
            *self.error.lock().unwrap() = outcome.as_ref().err().cloned();
            outcome
        })
    }
}

fn outcome_executor(store: &PostgresStore, tool: &CapturingTool) -> DurableInvocationExecutor {
    let mut providers = ToolProviderRegistryBuilder::new();
    providers.register(Arc::new(tool.clone())).unwrap();
    let (_, output) = schema("failure-close-real-tool-output");
    DurableInvocationExecutor::new(
        store.clone(),
        invocation_schema_registry_with(tool.inner.descriptor.output_schema().clone(), output),
        ModelProviderRegistryBuilder::new().build(),
        providers.build(),
        Arc::new(StaticInvocationBudget {
            resolved: invocation_budget(),
        }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap()
}

// No provider instance is registered. Recovery must win before registry resolve.
fn cold_executor(store: &PostgresStore, original: &ToolInvocation) -> DurableInvocationExecutor {
    let (_, output) = schema("failure-close-real-tool-output");
    DurableInvocationExecutor::new(
        store.clone(),
        invocation_schema_registry_with(
            original.intent().descriptor().output_schema().clone(),
            output,
        ),
        ModelProviderRegistryBuilder::new().build(),
        ToolProviderRegistryBuilder::new().build(),
        Arc::new(StaticInvocationBudget {
            resolved: invocation_budget(),
        }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap()
}

async fn prepare_outcome(
    store: &PostgresStore,
    fence: &RunFence,
    tool: &CapturingTool,
) -> (ToolInvocation, stateknot_runtime::ToolTerminalCommitHandoff) {
    let checkpoint = store
        .load_current_checkpoint(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .unwrap();
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let intent = ToolInvocationIntent::new(
        invocation_activation(&checkpoint),
        InvocationId::generate(),
        tool.inner.descriptor.clone(),
        tool_input(&tool.inner.descriptor),
        tool.inner.descriptor.limits().clone(),
    )
    .unwrap();
    let prepared = store
        .prepare_tool_invocation(
            worker_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                EventId::generate(),
                current.journal_head().unwrap().clone(),
                fence.clone(),
            ),
            intent,
        )
        .await
        .unwrap();
    let executor = outcome_executor(store, tool);
    let start = executor
        .start_tool_attempt(
            ToolAttemptHandoff::new(
                fence.clone(),
                prepared.invocation().clone(),
                AttemptId::generate(),
                InvocationAttemptEventIds::generate(),
                CancellationSignal::never(),
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let ToolAttemptStartOutcome::Started(dispatch) = start else {
        panic!("fresh fixture must grant the one real provider dispatch");
    };
    let terminal = executor.dispatch_started_tool(*dispatch).await;
    let executing = store
        .load_tool_invocation(
            fence.tenant_id(),
            fence.run_id(),
            prepared.invocation().intent().invocation_id(),
        )
        .await
        .unwrap();
    assert!(matches!(
        executing.state(),
        ToolInvocationState::Executing { .. }
    ));
    assert_eq!(executing.revision().get(), 1);
    (executing, terminal)
}

async fn write_outcome(input: &Value) {
    let store = test_store()
        .await
        .expect("outcome writer requires PostgreSQL");
    assert_eq!(store.options().lease_duration(), Duration::from_secs(30));
    let mut tracked = Vec::new();
    let (value, child) = Box::pin(close_unknown_setup(
        &store,
        "tool-outcome-commit",
        &mut tracked,
    ))
    .await;
    let fence = close_unknown_claim(&store, value.intent.key().tenant_id(), child).await;
    let tool = CapturingTool {
        inner: close_tool(true),
        error: Arc::default(),
    };
    let (executing, terminal) = Box::pin(prepare_outcome(&store, &fence, &tool)).await;
    let error = tool.error.lock().unwrap().clone().unwrap();
    assert_eq!(error.external_effect(), ToolExternalEffect::Unknown);
    assert_eq!(error.failure().retry_advice(), RetryAdvice::ReconcileFirst);
    let receipt = tool.inner.backend.receipt.lock().unwrap().clone().unwrap();
    assert_eq!(tool.inner.backend.sends.load(Ordering::SeqCst), 1);
    assert_eq!(tool.inner.backend.probes.load(Ordering::SeqCst), 0);
    assert!(tool.inner.backend.consent.lock().unwrap().is_none());
    let identity = PendingIdentity {
        key: value.intent.key().clone(),
        invocation: executing.intent().invocation_id(),
    };
    let ready = json!({"identity":identity, "executing":executing,
        "actual_error":error, "actual_receipt":receipt_value(&receipt),
        "writer_business_sends":1, "writer_probes":0, "fence":fence});
    assert!(serde_json::to_vec(&ready).unwrap().len() < 32 * 1024);
    publish_ready(&ready);
    // A distinct one-session Store never performs prepare/start/provider I/O.
    // The controller owns a one-shot phase gate before the semantic UPDATE.
    let proxied = worker_store(input).await;
    let _ = outcome_executor(&proxied, &tool)
        .commit_tool_terminal(terminal)
        .await;
    panic!("outcome commit returned before the controller killed its exact cut");
}

async fn original_prepared(
    store: &PostgresStore,
    identity: &PendingIdentity,
    child: RunId,
) -> ToolInvocation {
    let (event, prepared) = store
        .load_tool_invocation_revision(
            identity.key.tenant_id(),
            child,
            identity.invocation,
            ToolInvocationRevision::INITIAL,
        )
        .await
        .unwrap();
    assert_eq!(event.head(), prepared.journal_head().clone());
    assert!(matches!(prepared.state(), ToolInvocationState::Prepared));
    prepared
}

async fn complete_history(store: &PostgresStore, original: &ToolInvocation) -> Value {
    let mut history = Vec::new();
    for revision in 0..=original.revision().get() {
        let (event, record) = store
            .load_tool_invocation_revision(
                original.intent().tenant_id(),
                original.intent().run_id(),
                original.intent().invocation_id(),
                ToolInvocationRevision::new(revision).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(event.head(), record.journal_head().clone());
        history.push(json!({"ack":event, "record":record}));
    }
    assert_eq!(history.last().unwrap()["record"], json!(original));
    json!(history)
}

async fn owning_facts(store: &PostgresStore, identity: &PendingIdentity) -> Value {
    let key = &identity.key;
    let child = store.load_child_run(key).await.unwrap();
    let child_id = child.child().run().lifecycle().provenance().run_id();
    let parent = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    let original = store
        .load_tool_invocation(key.tenant_id(), child_id, identity.invocation)
        .await
        .unwrap();
    assert_eq!(parent.lifecycle().status(), RunStatus::Active);
    assert_eq!(child.child().run().lifecycle().status(), RunStatus::Active);
    assert!(parent.lifecycle().terminal_usage().is_none());
    assert!(child.child().run().lifecycle().terminal_usage().is_none());
    assert!(child.settlement().is_none());
    assert!(
        store
            .load_run_failure_close(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .is_none()
    );
    json!({"parent":stable_run(&parent), "parent_lease":parent.lease(),
        "child":stable_run(child.child().run()),
        "accounts":pending_accounts(store,key,child_id).await,
        "ownership":{"intent":child.intent(), "spawn":child.spawn(), "ancestors":child.ancestors(),
            "settlement":child.settlement()},
        "history":complete_history(store,&original).await,
        "parent_journal":full_journal(store,key.tenant_id(),key.parent_run_id()).await,
        "child_journal":full_journal(store,key.tenant_id(),child_id).await})
}

async fn recover_without_provider(
    store: &PostgresStore,
    pool: &PgPool,
    identity: &PendingIdentity,
    fence: &RunFence,
    original: &ToolInvocation,
) {
    let prepared = original_prepared(store, identity, fence.run_id()).await;
    assert_eq!(prepared.intent(), original.intent());
    let executor = cold_executor(store, original);
    let request = |attempt| {
        ToolAttemptHandoff::new(
            fence.clone(),
            prepared.clone(),
            attempt,
            InvocationAttemptEventIds::generate(),
            CancellationSignal::never(),
            None,
        )
        .unwrap()
    };
    let before = close_unknown_snapshot(pool, fence.tenant_id()).await;
    let recovered = executor
        .execute_tool(request(original.attempt_id().unwrap()))
        .await
        .unwrap();
    assert!(
        matches!(recovered, ToolAttemptOutcome::Recovered { invocation }
        if json!(invocation) == json!(original))
    );
    assert!(matches!(
        executor.execute_tool(request(AttemptId::generate())).await,
        Err(ToolAttemptExecutionError::InvocationAdvanced)
    ));
    let reconcile = ToolReconciliationAttemptHandoff::new(
        fence.clone(),
        original.clone(),
        EventId::generate(),
        CancellationSignal::never(),
    );
    match original.state() {
        ToolInvocationState::Executing { .. } => assert!(matches!(
            reconcile,
            Err(ToolReconciliationAttemptHandoffError::InvocationNotUnknown)
        )),
        ToolInvocationState::Unknown { .. } => assert!(reconcile.is_ok()),
        _ => panic!("cut must preserve Executing or original Unknown"),
    }
    assert_eq!(
        close_unknown_snapshot(pool, fence.tenant_id()).await,
        before
    );
}

async fn observe_cold_outcome(store: &PostgresStore, pool: &PgPool, identity: &PendingIdentity) {
    let child = store.load_child_run(&identity.key).await.unwrap();
    let run = child.child().run();
    let child_id = run.lifecycle().provenance().run_id();
    let old = run.lease().unwrap().clone();
    let facts = Box::pin(owning_facts(store, identity)).await;
    let before = close_unknown_snapshot(pool, identity.key.tenant_id()).await;
    wait_expiry(store, old.expires_at()).await;
    assert_eq!(
        close_unknown_snapshot(pool, identity.key.tenant_id()).await,
        before
    );
    let fence = close_unknown_claim(store, identity.key.tenant_id(), child_id).await;
    assert!(fence.epoch() > old.fence().epoch());
    assert_ne!(fence.attempt_id(), old.fence().attempt_id());
    let claimed = close_unknown_snapshot(pool, identity.key.tenant_id()).await;
    lease_delta(
        &before,
        &claimed,
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
    assert_eq!(Box::pin(owning_facts(store, identity)).await, facts);
    let original = store
        .load_tool_invocation(identity.key.tenant_id(), child_id, identity.invocation)
        .await
        .unwrap();
    Box::pin(recover_without_provider(
        store, pool, identity, &fence, &original,
    ))
    .await;
    let no_write = close_unknown_snapshot(pool, identity.key.tenant_id()).await;
    assert!(matches!(
        store.release_lease(old.fence()).await,
        Err(StoreError::StaleFence)
    ));
    assert_eq!(
        close_unknown_snapshot(pool, identity.key.tenant_id()).await,
        no_write
    );
    store.release_lease(&fence).await.unwrap();
    let released = close_unknown_snapshot(pool, identity.key.tenant_id()).await;
    lease_delta(
        &claimed,
        &released,
        child_id,
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
    assert_eq!(Box::pin(owning_facts(store, identity)).await, facts);
    assert_eq!(
        close_unknown_snapshot(pool, identity.key.tenant_id()).await,
        released
    );
    ready_and_park(
        json!({"cold_fence":fence, "facts_digest":full_digest(&facts),
        "no_provider_registry":true, "recovered_original_attempt":true,
        "different_attempt_rejected":true,"stale_release_rejected":true,
        "reconciliation_constructor_checked":true, "natural_expiry_and_exact_lease_delta":true,
        "owning_read_and_refusal_no_write":true}),
    )
    .await;
}

#[tokio::test]
async fn tool_unknown_outcome_commit_worker() {
    let Ok(raw) = std::env::var(INPUT_ENV) else {
        return;
    };
    watch_parent();
    let input: Value = serde_json::from_str(&raw).unwrap();
    match input["role"].as_str().unwrap() {
        "writer" => Box::pin(write_outcome(&input)).await,
        "cold" => {
            let identity: PendingIdentity =
                serde_json::from_value(input["identity"].clone()).unwrap();
            let store = test_store()
                .await
                .expect("cold outcome worker requires PostgreSQL");
            assert_eq!(store.options().lease_duration(), Duration::from_secs(30));
            let pool = sql_pool().await;
            Box::pin(observe_cold_outcome(&store, &pool, &identity)).await;
        }
        _ => panic!("unknown outcome process role"),
    }
    panic!("owned outcome worker escaped its kill point");
}

fn unchanged_except_columns(
    before: &TenantSnapshot,
    after: &TenantSnapshot,
    table: &str,
    id_column: &str,
    id: &Value,
    mutable: &[&str],
) {
    let normalized = |snapshot: &TenantSnapshot| {
        let mut values = rows(snapshot, table);
        let mut matches = 0;
        for row in &mut values {
            if &row[id_column] == id {
                matches += 1;
                for field in mutable {
                    row.as_object_mut().unwrap().remove(*field);
                }
            }
        }
        assert_eq!(matches, 1, "one exact owning row in {table}");
        values.sort_by_cached_key(Value::to_string);
        values
    };
    assert_eq!(
        normalized(before),
        normalized(after),
        "unrelated {table} changed"
    );
}

fn one_added_row(before: &TenantSnapshot, after: &TenantSnapshot, table: &str) -> Value {
    let old = rows(before, table);
    let new = rows(after, table);
    assert_eq!(new.len(), old.len() + 1, "one new {table} row");
    for row in &old {
        assert!(new.contains(row), "existing {table} row changed");
    }
    let additions: Vec<_> = new.into_iter().filter(|row| !old.contains(row)).collect();
    assert_eq!(additions.len(), 1);
    additions.into_iter().next().unwrap()
}

fn outcome_delta(before: &TenantSnapshot, after: &TenantSnapshot, original: &ToolInvocation) {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    for (table, value) in before {
        if ![
            "runs",
            "run_events",
            "tool_invocations",
            "tool_invocation_revisions",
        ]
        .contains(&table.as_str())
        {
            assert_eq!(after.get(table), Some(value), "outcome changed {table}");
        }
    }
    let child = original.intent().run_id();
    unchanged_except_columns(
        before,
        after,
        "runs",
        "run_id",
        &json!(child),
        &[
            "journal_sequence",
            "journal_event_id",
            "journal_recorded_at",
            "journal_digest",
            "updated_at",
        ],
    );
    unchanged_except_columns(
        before,
        after,
        "tool_invocations",
        "invocation_id",
        &json!(original.intent().invocation_id()),
        &[
            "current_revision",
            "current_status",
            "current_attempt_id",
            "current_record_digest",
            "updated_at",
        ],
    );
    let event = one_added_row(before, after, "run_events");
    assert_eq!(event["run_id"], json!(child));
    assert_eq!(event["event_id"], json!(original.journal_head().event_id()));
    let revision = one_added_row(before, after, "tool_invocation_revisions");
    assert_eq!(revision["run_id"], json!(child));
    assert_eq!(
        revision["invocation_id"],
        json!(original.intent().invocation_id())
    );
    assert_eq!(revision["revision"], json!(2));
    assert_eq!(revision["status"], "unknown");
    assert_eq!(revision["journal_event_id"], event["event_id"]);
}

async fn reconcile_after_committed_cut(
    store: &PostgresStore,
    pool: &PgPool,
    key: &ChildRunKey,
    child: RunId,
    original: &ToolInvocation,
    observed: &Value,
    ready: &Value,
) -> Value {
    let fence = close_unknown_claim(store, key.tenant_id(), child).await;
    let prior: RunFence = serde_json::from_value(observed["cold_fence"].clone()).unwrap();
    assert!(fence.epoch() > prior.epoch());
    let captured: CapturedReceipt =
        serde_json::from_value(ready["actual_receipt"].clone()).unwrap();
    let tool = restore_receipt(captured, original);
    let ack = Box::pin(reconcile_original(store, pool, &fence, original, &tool)).await;
    let resolved = store
        .load_tool_invocation(key.tenant_id(), child, original.intent().invocation_id())
        .await
        .unwrap();
    let settled = store.load_child_run(key).await.unwrap();
    assert!(settled.settlement().is_none());
    assert!(settled.child().run().lifecycle().terminal_usage().is_none());
    store.release_lease(&fence).await.unwrap();
    json!({"ack":ack,"record":resolved,"recovery_sends":0,"probes":2,"denied":1})
}

async fn prove_cold_outcome(
    store: &PostgresStore,
    pool: &PgPool,
    identity: &PendingIdentity,
    facts: &Value,
) -> Value {
    let key = &identity.key;
    let mut cold = TestProcess::spawn(OUTCOME_WORKER, &json!({"role":"cold","identity":identity}));
    let observed = cold.ready().await.unwrap();
    assert_eq!(observed["facts_digest"], json!(full_digest(facts)));
    for flag in [
        "no_provider_registry",
        "recovered_original_attempt",
        "different_attempt_rejected",
        "stale_release_rejected",
        "reconciliation_constructor_checked",
        "natural_expiry_and_exact_lease_delta",
        "owning_read_and_refusal_no_write",
    ] {
        assert_eq!(observed[flag], true, "{flag}");
    }
    let cold_before = close_unknown_snapshot(pool, key.tenant_id()).await;
    cold.kill_and_reap().await;
    assert_eq!(
        close_unknown_snapshot(pool, key.tenant_id()).await,
        cold_before
    );
    assert_eq!(&Box::pin(owning_facts(store, identity)).await, facts);
    observed
}

async fn run_outcome_cut(
    store: &PostgresStore,
    pool: &PgPool,
    connection: &PgConnectOptions,
    cut: Cut,
    tracked: &mut Vec<(TenantId, RunId)>,
) -> Value {
    let mut proxy = CommitProxy::start_paused_tool_outcome(connection, cut).await;
    let mut writer = TestProcess::spawn(
        OUTCOME_WORKER,
        &json!({"role":"writer","proxy_port":proxy.port()}),
    );
    let ready = writer.ready().await.unwrap();
    let identity: PendingIdentity = serde_json::from_value(ready["identity"].clone()).unwrap();
    let executing: ToolInvocation = serde_json::from_value(ready["executing"].clone()).unwrap();
    let key = &identity.key;
    let child = executing.intent().run_id();
    tracked.extend([
        (key.tenant_id().clone(), key.parent_run_id()),
        (key.tenant_id().clone(), child),
    ]);
    assert_eq!(ready["writer_business_sends"], 1);
    assert_eq!(ready["writer_probes"], 0);
    let before = close_unknown_snapshot(pool, key.tenant_id()).await;
    let before_facts = Box::pin(owning_facts(store, &identity)).await;
    assert_eq!(
        json!(
            store
                .load_tool_invocation(key.tenant_id(), child, identity.invocation)
                .await
                .unwrap()
        ),
        json!(executing)
    );
    assert_eq!(close_unknown_snapshot(pool, key.tenant_id()).await, before);
    proxy.arm_tool_outcome();
    proxy.wait_cut(cut).await;
    let pid = proxy.backend_pid();
    let at_cut = close_unknown_snapshot(pool, key.tenant_id()).await;
    if cut == Cut::BeforeCommit {
        assert_eq!(at_cut, before);
    }
    writer.kill_and_reap().await;
    proxy.stop().await;
    wait_for_backend_disconnect(pool, pid).await;
    assert_eq!(close_unknown_snapshot(pool, key.tenant_id()).await, at_cut);
    let original = store
        .load_tool_invocation(key.tenant_id(), child, identity.invocation)
        .await
        .unwrap();
    if cut == Cut::BeforeCommit {
        assert_eq!(json!(original), json!(executing));
        assert_eq!(Box::pin(owning_facts(store, &identity)).await, before_facts);
    } else {
        assert_eq!(original.revision().get(), 2);
        assert_eq!(original.previous(), Some(&executing.head()));
        assert!(
            matches!(original.state(),ToolInvocationState::Unknown { error }
            if json!(error) == ready["actual_error"])
        );
        outcome_delta(&before, &at_cut, &original);
    }
    let facts = Box::pin(owning_facts(store, &identity)).await;
    let history = complete_history(store, &original).await;
    let observed = Box::pin(prove_cold_outcome(store, pool, &identity, &facts)).await;
    let reconciliation = if cut == Cut::CommitResponse {
        Box::pin(reconcile_after_committed_cut(
            store, pool, key, child, &original, &observed, &ready,
        ))
        .await
    } else {
        Value::Null
    };
    let final_snapshot = close_unknown_snapshot(pool, key.tenant_id()).await;
    assert_eq!(complete_history(store, &original).await, history);
    // For After, original history is still addressable after authorized resolution.
    assert_eq!(
        close_unknown_snapshot(pool, key.tenant_id()).await,
        final_snapshot
    );
    json!({"cut":format!("{cut:?}"),"identity":identity,"original_record":original,
        "original_history":history,"cold":observed,"reconciliation":reconciliation,
        "actual_error":ready["actual_error"],"actual_receipt":ready["actual_receipt"],
        "whole_accounts_at_cut":facts["accounts"],
        "before_tenant_digest":full_digest(&before),"at_cut_tenant_digest":full_digest(&at_cut),
        "writer_business_sends":1,"cold_provider_count":0,"recovery_business_sends":0,
        "writer_killed_and_reaped":true,"cold_killed_and_reaped":true,
        "actual_receipt_and_error_retained_by_controller":true,"external_provider_backend_durability":false,
        "child_terminal_or_settlement_claimed":false,"automatic_executing_resolution_claimed":false})
}

#[tokio::test]
async fn tool_unknown_transition_commit_cuts_preserve_executing_or_original_unknown() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    assert_eq!(store.options().lease_duration(), Duration::from_secs(30));
    let connection: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let _ = loopback_target(&connection);
    let pool = sql_pool().await;
    let mut tracked = Vec::new();
    let verdict = std::panic::AssertUnwindSafe(async {
        let mut cuts = Vec::new();
        for cut in [Cut::BeforeCommit, Cut::CommitResponse] {
            cuts.push(
                Box::pin(run_outcome_cut(
                    &store,
                    &pool,
                    &connection,
                    cut,
                    &mut tracked,
                ))
                .await,
            );
        }
        cuts
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
    let cuts = match verdict {
        Ok(cuts) => cuts,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert!(released, "owned fixture leases must retire before verdict");
    eprintln!(
        "STATEKNOT_TOOL_UNKNOWN_TRANSITION_COMMIT_EVIDENCE={}",
        json!({
        "profile":"tool-unknown-transition-commit-v1","cuts":cuts,
        "lease30_ready90_default_stack":true,"os":std::env::consts::OS,"arch":std::env::consts::ARCH,
        "qualification_awarded":false})
    );
}
