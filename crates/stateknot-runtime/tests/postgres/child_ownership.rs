// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Transactional ownership tests, deliberately independent of an automatic Join driver.

#[path = "child_tree.rs"]
mod tree;

use super::*;
use sqlx_core::{query::query, query_scalar::query_scalar};
use stateknot_core::{AgentAdmission, JournalHead, NodeAttemptStartHead, RunFailure};
use stateknot_store_postgres::{ChildRunCommitOutcome, ChildRunSettlementOutcome};

struct Started {
    fixture: PreparationFixture,
    intent: ChildRunAdmissionIntent,
    node: NodeAttemptStartHead,
    fence: RunFence,
    head: JournalHead,
}

fn payload(kind: &str) -> JournalPayload {
    JournalPayload::new(
        test_payload().schema().clone(),
        JournalEventKind::new(kind).unwrap(),
        BoundedJson::try_from_value(json!({"test": true})).unwrap(),
    )
    .unwrap()
}

fn parent_append(started: &Started, kind: &str) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(started.head.clone()),
        JournalEventIntent::worker(
            started.intent.key().tenant_id().clone(),
            started.intent.key().parent_run_id(),
            EventId::generate(),
            started.fence.clone(),
            payload(kind),
        )
        .unwrap(),
    )
    .unwrap()
}

fn child_write(intent: &ChildRunAdmissionIntent) -> (JournalAppend, CheckpointWrite) {
    let child = intent.child().provenance();
    let append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(
            child.tenant_id().clone(),
            child.run_id(),
            EventId::generate(),
            payload(AgentAdmission::JOURNAL_EVENT_KIND),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = CheckpointWrite::initial(
        child.tenant_id().clone(),
        child.run_id(),
        CheckpointId::generate(),
        intent.child().graph().clone(),
        intent.initial_state().clone(),
        intent.child_graph().entry_nodes().clone(),
    )
    .unwrap();
    (append, checkpoint)
}

async fn started(store: &PostgresStore, name: &str) -> Started {
    Box::pin(started_inner(store, name)).await
}

async fn started_inner(store: &PostgresStore, name: &str) -> Started {
    let fixture = Box::pin(setup(store, name)).await;
    store
        .register_graph_definition(
            fixture
                .parent
                .admission()
                .intent()
                .provenance()
                .tenant_id()
                .clone(),
            fixture.child_driver.graph.clone(),
        )
        .await
        .unwrap();
    let child = fixture.child();
    let intent = fixture
        .prepare(&child, store.observe_database_clock().await.unwrap())
        .unwrap();
    let key = intent.key();
    let fence = store
        .claim_lease(key.tenant_id(), key.parent_run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let outcome = store
        .start_node_attempt(
            worker_append(
                key.tenant_id().clone(),
                key.parent_run_id(),
                EventId::generate(),
                fixture.parent.run().journal_head().unwrap().clone(),
                fence.clone(),
            ),
            key.parent().clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let attempt = match outcome {
        NodeAttemptCommitOutcome::Committed { attempt, .. } => attempt,
        other => panic!("fresh node start expected: {other:?}"),
    };
    Started {
        fixture,
        intent,
        head: attempt.start().journal_head().clone(),
        node: attempt.start().head(),
        fence,
    }
}

async fn spawn(
    store: &PostgresStore,
    value: &Started,
) -> Result<ChildRunCommitOutcome, StoreError> {
    let (append, checkpoint) = child_write(&value.intent);
    Box::pin(store.admit_child_run(
        value.intent.clone(),
        &value.node,
        parent_append(value, "child-run-admitted"),
        append,
        checkpoint,
        BudgetUsage::zero(),
        value.fixture.driver.registry.schemas(),
    ))
    .await
}

async fn fail(
    store: &PostgresStore,
    tenant: &TenantId,
    run_id: RunId,
    usage: BudgetUsage,
) -> Result<(), StoreError> {
    let run = store.load_run(tenant, run_id).await?;
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../stateknot-core/tests/fixtures/core-failure-v1.json"
    ))
    .unwrap();
    let failure: Failure = serde_json::from_value(fixture["failures"]["valid"][0].clone()).unwrap();
    let failure = RunFailure::new(failure, store.observe_database_clock().await?, usage).unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            tenant.clone(),
            run_id,
            EventId::generate(),
            payload("test-failed"),
        )
        .unwrap(),
    )
    .unwrap();
    store
        .append_control_plane(
            append,
            RunProjection::transition(run.lifecycle().revision(), RunTransition::Fail { failure }),
        )
        .await?;
    Ok(())
}

async fn settlement_append(store: &PostgresStore, key: &ChildRunKey) -> JournalAppend {
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            key.tenant_id().clone(),
            key.parent_run_id(),
            EventId::generate(),
            payload("child-run-settled"),
        )
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn atomic_spawn_retry_terminal_reconciliation_and_parent_accounting() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "owned-child-accounting").await;
    let committed = spawn(&store, &value).await.unwrap();
    assert!(matches!(committed, ChildRunCommitOutcome::Committed(_)));
    let record = committed.record();
    let child = record.child().admission().intent().provenance().run_id();
    assert_ne!(child, value.intent.key().parent_run_id());
    assert_ne!(
        record.child().checkpoint().checkpoint_id(),
        value.fixture.parent.checkpoint().checkpoint_id()
    );
    assert_eq!(record.ancestors(), &[value.intent.key().parent_run_id()]);
    let key = value.intent.key().clone();
    let account = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account.children().len(), 1);
    assert!(matches!(
        fail(
            &store,
            key.tenant_id(),
            key.parent_run_id(),
            BudgetUsage::zero()
        )
        .await,
        Err(StoreError::UnsettledChildRuns)
    ));
    // Retry with new candidate run/thread/invocation/checkpoint/event IDs returns the first identities.
    let retry = value.fixture.child();
    value.intent = value
        .fixture
        .prepare(&retry, store.observe_database_clock().await.unwrap())
        .unwrap();
    let recovered = spawn(&store, &value).await.unwrap();
    assert!(matches!(recovered, ChildRunCommitOutcome::Idempotent(_)));
    assert_eq!(
        recovered
            .record()
            .child()
            .admission()
            .intent()
            .provenance()
            .run_id(),
        child
    );
    assert!(matches!(
        store
            .load_run(key.tenant_id(), retry.intent().provenance().run_id())
            .await,
        Err(StoreError::RunNotFound)
    ));
    let usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(7))
        .tool_calls(ExecutionCount::new(2))
        .build()
        .unwrap();
    fail(&store, key.tenant_id(), child, usage.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .pending_child_settlements(key.tenant_id())
            .await
            .unwrap(),
        vec![key.clone()]
    );
    // Exact child terminal evidence remains valid after an unrelated later audit event.
    let child_run = store.load_run(key.tenant_id(), child).await.unwrap();
    let terminal = child_run.journal_head().unwrap().clone();
    let audit = JournalAppend::new(
        JournalExpectation::exact(terminal.clone()),
        JournalEventIntent::control_plane(
            key.tenant_id().clone(),
            child,
            EventId::generate(),
            payload("post-terminal-audit"),
        )
        .unwrap(),
    )
    .unwrap();
    store
        .append_control_plane(audit, RunProjection::unchanged())
        .await
        .unwrap();
    let append = settlement_append(&store, &key).await;
    let settled = Box::pin(store.settle_child_run(&key, append.clone()))
        .await
        .unwrap();
    let evidence = match settled {
        ChildRunSettlementOutcome::Committed { record, .. } => record.settlement().unwrap().clone(),
        other => panic!("fresh settlement expected: {other:?}"),
    };
    assert_eq!(evidence.terminal(), &terminal);
    assert_eq!(evidence.usage(), &usage);
    assert!(matches!(
        Box::pin(store.settle_child_run(&key, append))
            .await
            .unwrap(),
        ChildRunSettlementOutcome::Idempotent { .. }
    ));
    assert!(
        store
            .pending_child_settlements(key.tenant_id())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        fail(
            &store,
            key.tenant_id(),
            key.parent_run_id(),
            BudgetUsage::zero()
        )
        .await,
        Err(StoreError::IncompleteChildAccounting)
    ));
    let total = store
        .include_child_usage(key.tenant_id(), key.parent_run_id(), BudgetUsage::zero())
        .await
        .unwrap();
    assert_eq!(total, usage);
    fail(&store, key.tenant_id(), key.parent_run_id(), total)
        .await
        .unwrap();
    assert_eq!(
        store.load_child_run(&key).await.unwrap().settlement(),
        Some(&evidence)
    );
    store.close().await;
}

#[tokio::test]
async fn concurrent_logical_spawn_has_one_owner_and_no_orphan_candidates() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = Arc::new(started(&store, "owned-child-spawn-race").await);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let value = Arc::clone(&value);
        tasks.spawn(async move { spawn(&store, &value).await });
    }
    let mut committed = 0;
    let mut child = None;
    while let Some(result) = tasks.join_next().await {
        let outcome = result.unwrap().unwrap();
        committed += usize::from(matches!(outcome, ChildRunCommitOutcome::Committed(_)));
        let id = outcome
            .record()
            .child()
            .admission()
            .intent()
            .provenance()
            .run_id();
        assert_eq!(*child.get_or_insert(id), id);
    }
    assert_eq!(committed, 1);
    assert_eq!(
        store
            .list_child_run_identities(
                value.intent.key().tenant_id(),
                value.intent.key().parent_run_id()
            )
            .await
            .unwrap()
            .len(),
        1
    );
    store.close().await;
}

#[tokio::test]
async fn stale_spawn_and_existing_root_attachment_leave_parent_unchanged() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = started(&store, "owned-child-stale").await;
    let key = value.intent.key();
    // Existing independent admission is not adoptable, even with byte-identical intent.
    let (append, checkpoint) = child_write(&value.intent);
    Box::pin(store.admit_agent_run(
        value.intent.child().clone(),
        append,
        checkpoint,
        value.fixture.driver.registry.schemas(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        spawn(&store, &value).await,
        Err(StoreError::ChildRunConflict)
    ));
    assert_eq!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .journal_head(),
        Some(&value.head)
    );
    assert!(
        store
            .load_child_budget_account(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .list_child_run_identities(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .is_empty()
    );
    store.release_lease(&value.fence).await.unwrap();
    let rejected = spawn(&store, &value).await;
    assert!(
        matches!(rejected, Err(StoreError::NoActiveLease)),
        "{rejected:?}"
    );
    store.close().await;
}

async fn sql_pool() -> sqlx_postgres::PgPool {
    sqlx_postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn spawn_final_write_failure_rolls_back_every_fact_and_old_workers_fail_closed() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = started(&store, "owned-child-fault").await;
    let key = value.intent.key();
    let pool = sql_pool().await;
    let old =
        query("UPDATE stateknot.runs SET updated_at=updated_at WHERE tenant_id=$1 AND run_id=$2")
            .bind(key.tenant_id().as_str())
            .bind(*key.parent_run_id().as_uuid())
            .execute(&pool)
            .await
            .unwrap_err();
    assert_eq!(
        old.as_database_error().unwrap().code().as_deref(),
        Some("SKC01")
    );
    for trigger in ["runs_child_mutation_guard", "runs_child_terminal_capture"] {
        query(&format!(
            "ALTER TABLE stateknot.runs DISABLE TRIGGER {trigger}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        assert!(matches!(
            store.verify_schema().await,
            Err(StoreError::IncompleteSchema)
        ));
        query(&format!(
            "ALTER TABLE stateknot.runs ENABLE TRIGGER {trigger}"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    query("CREATE FUNCTION stateknot.test_child_final_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected child final write failure'; END $$")
        .execute(&pool).await.unwrap();
    query(&format!("CREATE TRIGGER test_child_final_failure BEFORE UPDATE ON stateknot.runs FOR EACH ROW WHEN (NEW.run_id = '{}'::uuid AND NEW.journal_sequence > OLD.journal_sequence) EXECUTE FUNCTION stateknot.test_child_final_failure()", key.parent_run_id()))
        .execute(&pool).await.unwrap();
    let failure = spawn(&store, &value).await;
    query("DROP TRIGGER test_child_final_failure ON stateknot.runs")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_child_final_failure()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        matches!(failure, Err(StoreError::Database { .. })),
        "{failure:?}"
    );
    for table in ["child_run_ownership", "child_run_budget_accounts"] {
        let count = query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM stateknot.{table} WHERE tenant_id=$1"
        ))
        .bind(key.tenant_id().as_str())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 0);
    }
    assert!(matches!(
        store
            .load_run(key.tenant_id(), value.intent.child().provenance().run_id())
            .await,
        Err(StoreError::RunNotFound)
    ));
    assert_eq!(
        store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .journal_head(),
        Some(&value.head)
    );
    let record = spawn(&store, &value).await.unwrap();
    // Compatibility-enabled raw writes still cannot skip child lifecycle guards.
    for mutation in [
        "checkpoint_id = '01912345-6789-7abc-8def-0123456789a1'::uuid",
        "lifecycle_status = 'succeeded'",
        "lifecycle_status = 'failed'",
        "lifecycle_status = 'cancelled'",
    ] {
        let mut tx = pool.begin().await.unwrap();
        query("SET LOCAL stateknot.child_runtime_version = '1'")
            .execute(&mut *tx)
            .await
            .unwrap();
        let error = query(&format!(
            "UPDATE stateknot.runs SET {mutation} WHERE tenant_id=$1 AND run_id=$2"
        ))
        .bind(key.tenant_id().as_str())
        .bind(*key.parent_run_id().as_uuid())
        .execute(&mut *tx)
        .await
        .unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("SKC02")
        );
        tx.rollback().await.unwrap();
    }
    // Immutable evidence is protected even from accidental maintenance UPDATEs.
    let mutation =
        query("UPDATE stateknot.child_run_ownership SET child_slot='other' WHERE tenant_id=$1")
            .bind(key.tenant_id().as_str())
            .execute(&pool)
            .await
            .unwrap_err();
    assert_eq!(
        mutation.as_database_error().unwrap().code().as_deref(),
        Some("SKC05")
    );
    let child = record
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    fail(&store, key.tenant_id(), child, BudgetUsage::zero())
        .await
        .unwrap();
    // A failed parent settlement commit must preserve both reservation and notification.
    query("CREATE FUNCTION stateknot.test_child_settlement_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected settlement failure'; END $$").execute(&pool).await.unwrap();
    query(&format!("CREATE TRIGGER test_child_settlement_failure BEFORE UPDATE ON stateknot.runs FOR EACH ROW WHEN (NEW.run_id = '{}'::uuid AND NEW.journal_sequence > OLD.journal_sequence) EXECUTE FUNCTION stateknot.test_child_settlement_failure()", key.parent_run_id())).execute(&pool).await.unwrap();
    let append = settlement_append(&store, key).await;
    let failed = Box::pin(store.settle_child_run(key, append.clone())).await;
    query("DROP TRIGGER test_child_settlement_failure ON stateknot.runs")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_child_settlement_failure()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(failed, Err(StoreError::Database { .. })));
    assert!(
        store
            .load_child_run(key)
            .await
            .unwrap()
            .settlement()
            .is_none()
    );
    assert_eq!(
        store
            .pending_child_settlements(key.tenant_id())
            .await
            .unwrap(),
        vec![key.clone()]
    );
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let key = key.clone();
        let append = append.clone();
        tasks.spawn(async move { Box::pin(store.settle_child_run(&key, append)).await });
    }
    let mut winners = 0;
    while let Some(result) = tasks.join_next().await {
        winners += usize::from(matches!(
            result.unwrap().unwrap(),
            ChildRunSettlementOutcome::Committed { .. }
        ));
    }
    assert_eq!(winners, 1);
    pool.close().await;
    store.close().await;
}

struct ChildBudgetModel {
    descriptor: ModelDescriptor,
    seen: Arc<AtomicUsize>,
}
impl Model for ChildBudgetModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }
    fn invoke(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxFuture<'_, Result<ModelResponse, ModelError>> {
        self.seen.store(
            usize::try_from(context.budget().input_tokens().get()).unwrap(),
            Ordering::SeqCst,
        );
        let response = model_response_for(&self.descriptor, &request, context.attempt_id());
        Box::pin(async move { Ok(response) })
    }
    fn stream(
        &self,
        _: ModelContext,
        _: ModelRequest,
    ) -> BoxStream<'_, Result<ModelEvent, ModelError>> {
        Box::pin(EmptyModelStream)
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn external_dispatch_excludes_live_children_and_deducts_settled_usage() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "owned-child-direct-budget").await;
    let child = spawn(&store, &value)
        .await
        .unwrap()
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    let key = value.intent.key().clone();
    value.head = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let model = ModelInvocationIntent::new(
        key.parent().clone(),
        InvocationId::generate(),
        model_descriptor(),
        model_request(),
    )
    .unwrap();
    assert!(matches!(
        store
            .prepare_model_invocation(parent_append(&value, "test-model-prepared"), model.clone())
            .await,
        Err(StoreError::UnsettledChildRuns)
    ));
    let tool = ToolInvocationIntent::new(
        key.parent().clone(),
        InvocationId::generate(),
        tool_descriptor(),
        tool_input(&tool_descriptor()),
        tool_descriptor().limits().clone(),
    )
    .unwrap();
    assert!(matches!(
        store
            .prepare_tool_invocation(parent_append(&value, "test-tool-prepared"), tool)
            .await,
        Err(StoreError::UnsettledChildRuns)
    ));
    fail(
        &store,
        key.tenant_id(),
        child,
        BudgetUsage::builder()
            .input_tokens(TokenCount::new(7))
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    let append = settlement_append(&store, &key).await;
    Box::pin(store.settle_child_run(&key, append))
        .await
        .unwrap();
    value.head = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let prepared = store
        .prepare_model_invocation(parent_append(&value, "test-model-prepared"), model)
        .await
        .unwrap();
    value.head = prepared.invocation().journal_head().clone();
    let transition = stateknot_core::ModelInvocationTransition::StartAttempt {
        attempt_id: AttemptId::generate(),
    };
    assert!(matches!(
        store
            .advance_model_invocation(
                parent_append(&value, "test-model-start"),
                &prepared.invocation().head(),
                transition.clone()
            )
            .await,
        Err(StoreError::ChildBudgetObservationRequired)
    ));
    assert!(matches!(
        store
            .advance_model_invocation_with_child_budget(
                parent_append(&value, "test-model-start"),
                &prepared.invocation().head(),
                transition,
                Some(Digest::sha256("stale"))
            )
            .await,
        Err(StoreError::ChildBudgetObservationRequired)
    ));
    let seen = Arc::new(AtomicUsize::new(0));
    let mut models = ModelProviderRegistryBuilder::new();
    models
        .register(Arc::new(ChildBudgetModel {
            descriptor: model_descriptor(),
            seen: Arc::clone(&seen),
        }))
        .unwrap();
    let budget = invocation_budget();
    let expected = budget.input_tokens().get() - 7;
    let executor = DurableInvocationExecutor::new(
        store.clone(),
        invocation_schema_registry(),
        models.build(),
        ToolProviderRegistryBuilder::new().build(),
        Arc::new(StaticInvocationBudget { resolved: budget }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let handoff = ModelAttemptHandoff::new(
        value.fence.clone(),
        prepared.invocation().clone(),
        AttemptId::generate(),
        InvocationAttemptEventIds::generate(),
        CancellationSignal::never(),
        None,
    )
    .unwrap();
    assert!(matches!(
        executor.execute_model(handoff.clone()).await.unwrap(),
        ModelAttemptOutcome::Dispatched { .. }
    ));
    assert_eq!(
        seen.load(Ordering::SeqCst),
        usize::try_from(expected).unwrap()
    );
    seen.store(0, Ordering::SeqCst);
    assert!(matches!(
        executor.execute_model(handoff).await.unwrap(),
        ModelAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(seen.load(Ordering::SeqCst), 0);
    store.close().await;
}

#[tokio::test]
async fn prepared_parent_invocation_prevents_child_budget_reservation() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "owned-child-prepared-parent").await;
    let model = ModelInvocationIntent::new(
        value.intent.key().parent().clone(),
        InvocationId::generate(),
        model_descriptor(),
        model_request(),
    )
    .unwrap();
    let prepared = store
        .prepare_model_invocation(parent_append(&value, "test-model-prepared"), model)
        .await
        .unwrap();
    value.head = prepared.invocation().journal_head().clone();
    let outcome = spawn(&store, &value).await;
    assert!(
        matches!(outcome, Err(StoreError::CheckpointBlockedByModelInvocation)),
        "{outcome:?}"
    );
    assert!(
        store
            .load_child_budget_account(
                value.intent.key().tenant_id(),
                value.intent.key().parent_run_id()
            )
            .await
            .unwrap()
            .is_none()
    );
    store.close().await;
}

#[tokio::test]
async fn unknown_cost_remains_discoverable_while_known_overrun_settles_without_erasure() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for unknown in [false, true] {
        let value = started(&store, "owned-child-cost-evidence").await;
        let key = value.intent.key();
        let child = spawn(&store, &value)
            .await
            .unwrap()
            .record()
            .child()
            .admission()
            .intent()
            .provenance()
            .run_id();
        let usage = if unknown {
            BudgetUsage::builder()
                .unpriced_cost_events(ExecutionCount::new(1))
                .build()
                .unwrap()
        } else {
            BudgetUsage::builder()
                .input_tokens(TokenCount::new(
                    value.intent.child().budget().input_tokens().get() + 1,
                ))
                .build()
                .unwrap()
        };
        fail(&store, key.tenant_id(), child, usage.clone())
            .await
            .unwrap();
        let append = settlement_append(&store, key).await;
        let result = Box::pin(store.settle_child_run(key, append)).await;
        if unknown {
            assert!(matches!(
                result,
                Err(StoreError::ChildRunSettlementUnavailable)
            ));
            assert_eq!(
                store
                    .pending_child_settlements(key.tenant_id())
                    .await
                    .unwrap(),
                vec![key.clone()]
            );
        } else {
            result.unwrap();
            let account = store
                .load_child_budget_account(key.tenant_id(), key.parent_run_id())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(account.delegated_usage().unwrap(), usage);
            assert!(
                account
                    .remaining(store.observe_database_clock().await.unwrap())
                    .is_err()
            );
        }
    }
    store.close().await;
}

#[tokio::test]
async fn sibling_reservation_after_head_retry_cannot_reuse_allocated_capacity() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "owned-child-sibling-budget").await;
    spawn(&store, &value).await.unwrap();
    let request = value.fixture.child();
    let key = ChildRunKey::new(
        value.intent.key().parent().clone(),
        ChildRunSlot::new("secondary").unwrap(),
    )
    .unwrap();
    value.intent = ChildRunAdmissionIntent::new(
        value.fixture.parent.admission(),
        key.clone(),
        request.intent().clone(),
        value.fixture.child_driver.graph.clone(),
        request.initial_state().clone(),
    )
    .unwrap();
    assert!(matches!(
        spawn(&store, &value).await,
        Err(StoreError::StaleJournalHead)
    ));
    value.head = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    assert!(matches!(
        spawn(&store, &value).await,
        Err(StoreError::ChildRunRejected)
    ));
    assert_eq!(
        store
            .load_child_budget_account(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap()
            .children()
            .len(),
        1
    );
    assert!(matches!(
        store
            .load_run(key.tenant_id(), request.intent().provenance().run_id())
            .await,
        Err(StoreError::RunNotFound)
    ));
    store.close().await;
}

#[tokio::test]
async fn account_corruption_blocks_both_read_and_lost_ack_recovery() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let value = started(&store, "owned-child-corruption").await;
    spawn(&store, &value).await.unwrap();
    let key = value.intent.key();
    let account = store
        .load_child_budget_account(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap()
        .unwrap();
    let pool = sql_pool().await;
    query("UPDATE stateknot.child_run_budget_accounts SET account_digest=$1 WHERE tenant_id=$2 AND parent_run_id=$3")
        .bind(Digest::sha256("corruption").as_bytes()).bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).execute(&pool).await.unwrap();
    assert!(matches!(
        store.load_child_run(key).await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        spawn(&store, &value).await,
        Err(StoreError::CorruptData { .. })
    ));
    query("UPDATE stateknot.child_run_budget_accounts SET account_digest=$1 WHERE tenant_id=$2 AND parent_run_id=$3")
        .bind(account.digest().as_bytes()).bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).execute(&pool).await.unwrap();
    assert!(matches!(
        spawn(&store, &value).await.unwrap(),
        ChildRunCommitOutcome::Idempotent(_)
    ));
    let removed = query("DELETE FROM stateknot.child_run_budget_accounts WHERE tenant_id=$1")
        .bind(key.tenant_id().as_str())
        .execute(&pool)
        .await;
    assert!(removed.is_err());
    pool.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_terminal_racing_settlement_is_consistent_without_ancestor_lock_inversion() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for _ in 0..4 {
        let value = started(&store, "owned-child-terminal-race").await;
        let key = value.intent.key().clone();
        let child = spawn(&store, &value)
            .await
            .unwrap()
            .record()
            .child()
            .admission()
            .intent()
            .provenance()
            .run_id();
        let append = settlement_append(&store, &key).await;
        let gate = Arc::new(tokio::sync::Barrier::new(9));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let store = store.clone();
            let key = key.clone();
            let append = append.clone();
            let gate = Arc::clone(&gate);
            tasks.spawn(async move {
                gate.wait().await;
                Box::pin(store.settle_child_run(&key, append)).await
            });
        }
        gate.wait().await;
        fail(&store, key.tenant_id(), child, BudgetUsage::zero())
            .await
            .unwrap();
        let mut committed = 0;
        while let Some(result) = tasks.join_next().await {
            match result.unwrap() {
                Ok(ChildRunSettlementOutcome::Committed { .. }) => committed += 1,
                Ok(ChildRunSettlementOutcome::Idempotent { .. })
                | Err(StoreError::ChildRunSettlementUnavailable) => {}
                other => panic!("terminal race must not report corruption/deadlock: {other:?}"),
            }
        }
        assert!(committed <= 1);
        Box::pin(store.settle_child_run(&key, append))
            .await
            .unwrap();
        assert!(
            store
                .load_child_run(&key)
                .await
                .unwrap()
                .settlement()
                .is_some()
        );
    }
    store.close().await;
}
