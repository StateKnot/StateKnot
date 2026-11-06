// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real ordinary Graph creation, whole Root recovery and original cost rules.
use super::*;
use sqlx_core::{query::query, query_scalar::query_scalar};
use sqlx_postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use stateknot_core::{
    AgentResult, AgentResultValidationError, AgentTerminalValidationInputs, JsonLimits,
    ModelInvocation, ModelInvocationState, NodeInvocationBinding,
};
use stateknot_runtime::GraphLifecycleError;

struct CountUnavailable(Arc<AtomicUsize>);
impl GraphLifecycleEvidenceProvider for CountUnavailable {
    fn terminal_evidence(
        &self,
        _: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(GraphLifecycleEvidenceError::TemporarilyUnavailable) })
    }
    fn failure_evidence(
        &self,
        _: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Err(GraphLifecycleEvidenceError::TemporarilyUnavailable) })
    }
    fn cancellation_evidence(
        &self,
        _: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Err(GraphLifecycleEvidenceError::TemporarilyUnavailable) })
    }
}

async fn ordinary_pool() -> PgPool {
    let options: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    commit_proxy::loopback_target(&options);
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.ssl_mode(PgSslMode::Disable))
        .await
        .unwrap()
}

async fn separate_initial_inputs(
    store: &PostgresStore,
    graph: &CompiledGraph,
    name: &str,
) -> (
    stateknot_store_postgres::StoredRun,
    JournalAppend,
    CheckpointWrite,
) {
    let tenant = tenant(name);
    let run = RunId::generate();
    store
        .register_graph_definition(tenant.clone(), graph.clone())
        .await
        .unwrap();
    let admitted = store
        .admit_run(provenance(tenant.clone(), run))
        .await
        .unwrap();
    store
        .append_control_plane(
            control_append(tenant.clone(), run, EventId::generate()),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
        )
        .await
        .unwrap();
    let started = store.load_run(&tenant, run).await.unwrap();
    assert!(started.checkpoint().is_none());
    let write = CheckpointWrite::initial(
        tenant.clone(),
        run,
        CheckpointId::generate(),
        graph.reference(),
        CheckpointState::new(
            graph.state_schema().clone(),
            BoundedJson::try_from_value(json!({"step": "initial"})).unwrap(),
        )
        .unwrap(),
        graph.entry_nodes().clone(),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(started.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(tenant, run, EventId::generate(), test_payload())
            .unwrap(),
    )
    .unwrap();
    (started, append, write)
}

async fn initial_snapshot(db: &PgPool, run: &stateknot_store_postgres::StoredRun) -> String {
    let owner = run.lifecycle().provenance();
    query_scalar("SELECT jsonb_build_object('run',(SELECT to_jsonb(r) FROM stateknot.runs r WHERE tenant_id=$1 AND run_id=$2),'events',(SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY sequence),'[]'::jsonb) FROM stateknot.run_events e WHERE tenant_id=$1 AND run_id=$2),'checkpoints',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY superstep),'[]'::jsonb) FROM stateknot.run_checkpoints c WHERE tenant_id=$1 AND run_id=$2),'results',(SELECT count(*) FROM stateknot.pending_node_results WHERE tenant_id=$1 AND run_id=$2),'attempts',(SELECT count(*) FROM stateknot.node_attempts WHERE tenant_id=$1 AND run_id=$2),'bundles',(SELECT count(*) FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2),'consumptions',(SELECT coalesce(jsonb_agg(to_jsonb(c) ORDER BY to_jsonb(c)::text),'[]'::jsonb) FROM stateknot.pending_node_result_consumptions c WHERE tenant_id=$1 AND run_id=$2),'waits',(SELECT coalesce(jsonb_agg(to_jsonb(w) ORDER BY to_jsonb(w)::text),'[]'::jsonb) FROM stateknot.run_wait_registrations w WHERE tenant_id=$1 AND run_id=$2))::text")
        .bind(owner.tenant_id().as_str()).bind(*owner.run_id().as_uuid()).fetch_one(db).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_initial_checkpoint_rejects_crossed_fresh_components_and_rolls_back() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = ordinary_pool().await;
    let fixture = driver_fixture();
    for (name, table, fault) in [
        (
            "projection",
            "run_events",
            "NEW.projection_digest:=decode(repeat('00',32),'hex');".to_owned(),
        ),
        (
            "owning-id",
            "run_events",
            format!("NEW.event_id:='{}'::uuid;", EventId::generate()),
        ),
        (
            "predecessor",
            "run_checkpoints",
            format!(
                "UPDATE stateknot.runs SET journal_event_id='{}'::uuid WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;",
                EventId::generate()
            ),
        ),
        (
            "false-initial",
            "run_checkpoints",
            "NEW.superstep:=1;".to_owned(),
        ),
        (
            "canonical",
            "run_checkpoints",
            "NEW.checkpoint_bytes:=convert_to(' ','UTF8')||NEW.checkpoint_bytes;".to_owned(),
        ),
    ] {
        Box::pin(check_initial_fault(
            &store,
            &db,
            &fixture.graph,
            name,
            table,
            &fault,
        ))
        .await;
    }
    Box::pin(check_crossed_initial_input(&store, &db, &fixture.graph)).await;
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

async fn check_initial_fault(
    store: &PostgresStore,
    db: &PgPool,
    graph: &CompiledGraph,
    name: &str,
    table: &str,
    fault: &str,
) {
    let (started, append, write) = Box::pin(separate_initial_inputs(
        store,
        graph,
        &format!("ordinary-initial-{name}"),
    ))
    .await;
    let before = initial_snapshot(db, &started).await;
    query(&format!("CREATE FUNCTION stateknot.test_ordinary_initial_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {fault} RETURN NEW; END $$")).execute(db).await.unwrap();
    query(&format!("CREATE TRIGGER test_ordinary_initial_fault BEFORE INSERT ON stateknot.{table} FOR EACH ROW EXECUTE FUNCTION stateknot.test_ordinary_initial_fault()")).execute(db).await.unwrap();
    let outcome = store
        .append_control_plane_checkpoint(append.clone(), RunProjection::unchanged(), write.clone())
        .await;
    // Restore the catalog before asserting a red result, and compare all fresh
    // components including the Run pointer/lifecycle, not just returned errors.
    query(&format!(
        "DROP TRIGGER test_ordinary_initial_fault ON stateknot.{table}"
    ))
    .execute(db)
    .await
    .unwrap();
    query("DROP FUNCTION stateknot.test_ordinary_initial_fault()")
        .execute(db)
        .await
        .unwrap();
    let after = initial_snapshot(db, &started).await;
    assert!(
        matches!(
            outcome,
            Err(StoreError::CorruptData { .. } | StoreError::Database { .. })
        ),
        "{name}: {outcome:?}"
    );
    assert_eq!(before, after, "{name}: complete fresh rollback");
    let saved = store
        .append_control_plane_checkpoint(append, RunProjection::unchanged(), write)
        .await
        .unwrap();
    assert_eq!(
        store
            .load_checkpoint(
                saved.checkpoint().tenant_id(),
                saved.checkpoint().run_id(),
                saved.checkpoint().checkpoint_id()
            )
            .await
            .unwrap(),
        *saved.checkpoint()
    );
}

async fn check_crossed_initial_input(store: &PostgresStore, db: &PgPool, graph: &CompiledGraph) {
    let (started, append, _) = Box::pin(separate_initial_inputs(
        store,
        graph,
        "ordinary-initial-owner",
    ))
    .await;
    let (other, _, crossed) = Box::pin(separate_initial_inputs(
        store,
        graph,
        "ordinary-initial-other",
    ))
    .await;
    let before = (
        initial_snapshot(db, &started).await,
        initial_snapshot(db, &other).await,
    );
    let outcome = store
        .append_control_plane_checkpoint(append, RunProjection::unchanged(), crossed)
        .await;
    let after = (
        initial_snapshot(db, &started).await,
        initial_snapshot(db, &other).await,
    );
    assert!(matches!(outcome, Err(StoreError::CheckpointCommitConflict)));
    assert_eq!(before, after);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_start_and_initial_checkpoint_keep_positive_creation_and_whole_recovery() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let (started, append, write) = Box::pin(separate_initial_inputs(
        &store,
        &fixture.graph,
        "ordinary-separate-start",
    ))
    .await;
    let tenant = started.lifecycle().provenance().tenant_id().clone();
    let run = started.lifecycle().provenance().run_id();
    let initial = store
        .append_control_plane_checkpoint(append.clone(), RunProjection::unchanged(), write.clone())
        .await
        .unwrap();
    let retry = store
        .append_control_plane_checkpoint(append, RunProjection::unchanged(), write)
        .await
        .unwrap();
    assert!(matches!(retry, CheckpointCommitOutcome::Idempotent { .. }));
    assert_eq!(retry.checkpoint(), initial.checkpoint());
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let (outcome, _) = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap()
        .into_parts();
    assert!(matches!(
        outcome,
        GraphDriveOutcome::LifecycleBarrierReady(_)
    ));
    let whole = store
        .load_root_graph_barrier(&initial.checkpoint().head())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        whole.event().payload().data().as_value()["source"],
        json!("trusted_graph")
    );
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 1);
    assert!(!store.load_run(&tenant, run).await.unwrap().is_quarantined());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_legacy_creation_is_operational_before_any_node_dispatch() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let before = Box::pin(unavailable_legacy_run(&store, &fixture.graph)).await;
    let tenant = before.lifecycle().provenance().tenant_id();
    let run = before.lifecycle().provenance().run_id();
    let lease = before.lease().unwrap();
    let db = ordinary_pool().await;
    let before_snapshot = initial_snapshot(&db, &before).await;
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let outcome = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await;
    assert!(
        matches!(
            &outcome,
            Err(GraphDriverError::Store { source }) if matches!(source.as_ref(), StoreError::GraphReplayHistoricalDispositionUnavailable)
        ),
        "{outcome:?}"
    );
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    let after = store.load_run(tenant, run).await.unwrap();
    assert_eq!(before.journal_head(), after.journal_head());
    assert_eq!(before.checkpoint(), after.checkpoint());
    assert!(!after.is_quarantined());
    assert_eq!(initial_snapshot(&db, &after).await, before_snapshot);
    db.close().await;
    store.close().await;
}

async fn unavailable_legacy_run(
    store: &PostgresStore,
    graph: &CompiledGraph,
) -> stateknot_store_postgres::StoredRun {
    let tenant = tenant("ordinary-unavailable-creation");
    let run = RunId::generate();
    store
        .register_graph_definition(tenant.clone(), graph.clone())
        .await
        .unwrap();
    let admitted = store
        .admit_run(provenance(tenant.clone(), run))
        .await
        .unwrap();
    // This legal generic journal event predates Start. Its unchanged projection
    // cannot be substituted for the original source's required Start preimage.
    store
        .append_control_plane(
            control_append(tenant.clone(), run, EventId::generate()),
            RunProjection::unchanged(),
        )
        .await
        .unwrap();
    let pending = store.load_run(&tenant, run).await.unwrap();
    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(pending.journal_head().unwrap().clone()),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    run,
                    EventId::generate(),
                    test_payload(),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
        )
        .await
        .unwrap();
    let started = store.load_run(&tenant, run).await.unwrap();
    let write = CheckpointWrite::initial(
        tenant.clone(),
        run,
        CheckpointId::generate(),
        graph.reference(),
        CheckpointState::new(
            graph.state_schema().clone(),
            BoundedJson::try_from_value(json!({"step":"initial"})).unwrap(),
        )
        .unwrap(),
        graph.entry_nodes().clone(),
    )
    .unwrap();
    store
        .append_control_plane_checkpoint(
            JournalAppend::new(
                JournalExpectation::exact(started.journal_head().unwrap().clone()),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    run,
                    EventId::generate(),
                    test_payload(),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::unchanged(),
            write,
        )
        .await
        .unwrap();
    store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap();
    store.load_run(&tenant, run).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creating_source_preflight_preserves_exact_session_and_never_writes() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = ordinary_pool().await;
    let fixture = driver_fixture();
    let tenant = tenant("ordinary-source-snapshot");
    let run = RunId::generate();
    let initial = start_run(&store, &fixture.graph, tenant.clone(), run).await;
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let before_run = store.load_run(&tenant, run).await.unwrap();
    let journal = before_run.journal_head().unwrap().clone();
    let context = CorruptionQuarantineContext::new(
        tenant.clone(),
        run,
        QuarantineId::generate(),
        JournalExpectation::exact(journal.clone()),
        Digest::sha256(b"ordinary source snapshot"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(lease.fence().clone(), context)
        .await
        .unwrap();
    let before = initial_snapshot(&db, &before_run).await;
    recovery
        .validate_graph_creating_source(&initial.checkpoint().head(), &journal)
        .await
        .unwrap();
    assert_eq!(initial_snapshot(&db, &before_run).await, before);

    let other = start_run(&store, &fixture.graph, tenant.clone(), RunId::generate()).await;
    assert!(matches!(
        recovery
            .validate_graph_creating_source(&other.checkpoint().head(), &journal)
            .await,
        Err(StoreError::StaleClaimedRunRecoveryObservation)
    ));
    assert!(matches!(
        recovery
            .validate_graph_creating_source(&initial.checkpoint().head(), &other.event().head())
            .await,
        Err(StoreError::StaleClaimedRunRecoveryObservation)
    ));
    assert_eq!(initial_snapshot(&db, &before_run).await, before);

    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(journal.clone()),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    run,
                    EventId::generate(),
                    test_payload(),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::unchanged(),
        )
        .await
        .unwrap();
    let advanced = store.load_run(&tenant, run).await.unwrap();
    let after_advance = initial_snapshot(&db, &advanced).await;
    // A new journal observation cannot be substituted into an older session,
    // even though the lease and initial Root checkpoint remain unchanged.
    assert!(matches!(
        recovery
            .validate_graph_creating_source(
                &initial.checkpoint().head(),
                advanced.journal_head().unwrap()
            )
            .await,
        Err(StoreError::StaleClaimedRunRecoveryObservation)
    ));
    assert!(matches!(
        recovery
            .validate_graph_creating_source(&initial.checkpoint().head(), &journal)
            .await,
        Err(StoreError::StaleClaimedRunRecoveryObservation)
    ));
    assert_eq!(initial_snapshot(&db, &advanced).await, after_advance);

    check_source_successor_fence(&store, &db, &initial.checkpoint().head(), lease.fence()).await;
    assert!(!store.load_run(&tenant, run).await.unwrap().is_quarantined());
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    db.close().await;
    store.close().await;
}

async fn check_source_successor_fence(
    store: &PostgresStore,
    db: &PgPool,
    head: &stateknot_core::CheckpointHead,
    old_fence: &RunFence,
) {
    store.release_lease(old_fence).await.unwrap();
    let successor = store
        .claim_lease(head.tenant_id(), head.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let current = store
        .load_run(head.tenant_id(), head.run_id())
        .await
        .unwrap();
    let before = initial_snapshot(db, &current).await;
    assert!(matches!(
        store
            .validate_worker_graph_creating_source(old_fence, head, current.journal_head().unwrap())
            .await,
        Err(StoreError::StaleFence)
    ));
    store
        .validate_worker_graph_creating_source(
            successor.fence(),
            head,
            current.journal_head().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(initial_snapshot(db, &current).await, before);
}

fn usage_with_cost(evidence: &GraphTerminalEvidence, costs: Value, unpriced: u64) -> BudgetUsage {
    let mut value = serde_json::to_value(evidence.usage()).unwrap();
    value["known_costs"] = costs;
    value["unpriced_cost_events"] = json!(unpriced.to_string());
    serde_json::from_value(value).unwrap()
}

fn evidence_with_usage(
    evidence: &GraphTerminalEvidence,
    usage: BudgetUsage,
) -> GraphTerminalEvidence {
    GraphTerminalEvidence::new(
        evidence.descriptor().clone(),
        evidence.request().clone(),
        evidence.budget().clone(),
        evidence.artifacts().clone(),
        usage,
    )
}

async fn terminal_handoff(
    store: &PostgresStore,
    fixture: &DriverFixture,
    evidence: &GraphTerminalEvidence,
    name: &str,
) -> (stateknot_runtime::GraphLifecycleBarrierHandoff, Checkpoint) {
    let tenant = tenant(name);
    let run = RunId::generate();
    let initial = start_run_with_provenance(
        store,
        &fixture.graph,
        AgentResultProvenance::for_agent(
            tenant.clone(),
            run,
            ThreadId::generate(),
            InvocationId::generate(),
            evidence.descriptor(),
        ),
        json!({"step": "initial"}),
    )
    .await
    .checkpoint()
    .clone();
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let options = if store.options().lease_duration() < Duration::from_secs(30) {
        DurableGraphDriverOptions::new(
            GraphReplayLimits::default(),
            1_024,
            store.options().lease_duration() / 4,
            Duration::from_secs(15 * 60),
            3,
            Duration::from_millis(25),
        )
        .unwrap()
    } else {
        DurableGraphDriverOptions::default()
    };
    let driver = DurableGraphDriver::new(store.clone(), fixture.registry.clone(), options).unwrap();
    let (outcome, _) = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap()
        .into_parts();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = outcome else {
        panic!("actual ordinary Graph must produce its complete terminal plan")
    };
    (*handoff, initial)
}

fn original_result(
    run: &stateknot_store_postgres::StoredRun,
    handoff: &stateknot_runtime::GraphLifecycleBarrierHandoff,
    evidence: &GraphTerminalEvidence,
) -> AgentResult {
    let GraphBarrierDisposition::Terminal { output } = handoff.plan().disposition() else {
        panic!("expected actual terminal disposition")
    };
    AgentResult::new(
        run.lifecycle().provenance().clone(),
        run.journal_head().unwrap().recorded_at(),
        output.schema().clone(),
        output.data().clone(),
        evidence.artifacts().clone(),
        evidence.usage().clone(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trusted_terminal_keeps_maximum_request_and_original_inputs_after_lost_ack() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let template = terminal_evidence(&fixture.graph);
    let maximum = JsonLimits::MAXIMUM.max_bytes();
    let input = BoundedJson::try_from_value_with_limits(
        json!({"a":"x".repeat(JsonLimits::MAXIMUM.max_string_bytes()),
            "b":"y".repeat(maximum-JsonLimits::MAXIMUM.max_string_bytes()-15)}),
        JsonLimits::MAXIMUM,
    )
    .unwrap();
    assert_eq!(
        serde_json_canonicalizer::to_vec(&input).unwrap().len(),
        maximum
    );
    let request = AgentRequest::new(
        template.request().input_schema().clone(),
        input,
        template.request().budget_limits().clone(),
    );
    let mut usage = serde_json::to_value(template.usage()).unwrap();
    usage["input_bytes"] = json!(maximum.to_string());
    let evidence = GraphTerminalEvidence::new(
        template.descriptor().clone(),
        request,
        template.budget().clone(),
        template.artifacts().clone(),
        serde_json::from_value(usage).unwrap(),
    );
    let inputs = AgentTerminalValidationInputs::for_descriptor(
        evidence.descriptor(),
        evidence.request().clone(),
        evidence.budget().clone(),
    );
    let (handoff, _) = Box::pin(terminal_handoff(
        &store,
        &fixture,
        &evidence,
        "ordinary-max-request",
    ))
    .await;
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(StaticLifecycleEvidence {
            terminal: evidence,
            failure: None,
        }),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        lifecycle.commit_barrier(handoff.clone()).await.unwrap(),
        GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));
    let saved = store
        .load_root_graph_barrier(handoff.plan().barrier().base_checkpoint())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.terminal_validation_inputs(), Some(&inputs));
    let db = ordinary_pool().await;
    let bytes: i32 = query_scalar("SELECT octet_length(bundle_bytes) FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2 AND disposition_kind='terminal'")
        .bind(saved.event().tenant_id().as_str()).bind(*saved.event().run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert!(usize::try_from(bytes).unwrap() > maximum);
    // The real evidence provider is now unavailable. Recovery must authenticate
    // the whole committed inputs rather than fabricate a smaller fresh request.
    let calls = Arc::new(AtomicUsize::new(0));
    let recovered = DurableGraphLifecycle::new(
        store.clone(),
        driver_fixture().registry,
        Arc::new(CountUnavailable(calls.clone())),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        recovered.commit_barrier(handoff).await.unwrap(),
        GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Idempotent { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

fn large_json(bytes: usize) -> BoundedJson {
    let first = JsonLimits::MAXIMUM.max_string_bytes();
    let value = BoundedJson::try_from_value_with_limits(
        json!({"a":"x".repeat(first),"b":"y".repeat(bytes-first-15)}),
        JsonLimits::MAXIMUM,
    )
    .unwrap();
    assert_eq!(value.stats().compact_bytes(), bytes);
    value
}

struct MaximumModel {
    descriptor: ModelDescriptor,
    calls: Arc<AtomicUsize>,
}
impl Model for MaximumModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }
    fn invoke(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxFuture<'_, Result<ModelResponse, ModelError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
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

// This real test provider charges one USD micro-unit for each complete response.
fn maximum_model_usage(invocation: &ModelInvocation) -> BudgetUsage {
    let ModelInvocationState::Committed { response } = invocation.state() else {
        panic!("real maximum fixture provider must commit its response")
    };
    BudgetUsage::builder()
        .model_attempts(ExecutionCount::new(1))
        .model_turns(ExecutionCount::new(1))
        .input_tokens(response.usage().input_tokens())
        .cached_input_tokens(
            response
                .usage()
                .cached_input_tokens()
                .unwrap_or(TokenCount::ZERO),
        )
        .output_tokens(response.usage().output_tokens())
        .reasoning_tokens(
            response
                .usage()
                .reasoning_tokens()
                .unwrap_or(TokenCount::ZERO),
        )
        .known_costs(serde_json::from_value(json!([{"currency":"USD","micro_units":"1"}])).unwrap())
        .build()
        .unwrap()
}

struct LargeTerminal {
    store: PostgresStore,
    executor: DurableInvocationExecutor,
    invocation_id: InvocationId,
    graph: GraphReference,
    node: NodeId,
    output: NodeTerminalOutput,
}
impl GraphNodeExecutor for LargeTerminal {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }
    fn node_id(&self) -> &NodeId {
        &self.node
    }
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            let start = context.attempt();
            let prepared = self
                .store
                .prepare_model_invocation(
                    worker_append(
                        start.fence().tenant_id().clone(),
                        start.fence().run_id(),
                        EventId::generate(),
                        start.journal_head().clone(),
                        start.fence().clone(),
                    ),
                    ModelInvocationIntent::new(
                        start.activation().clone(),
                        self.invocation_id,
                        model_descriptor(),
                        model_request(),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            let outcome = self
                .executor
                .execute_model(
                    ModelAttemptHandoff::new(
                        start.fence().clone(),
                        prepared.invocation().clone(),
                        AttemptId::generate(),
                        InvocationAttemptEventIds::generate(),
                        context.cancellation().clone(),
                        None,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            let ModelAttemptOutcome::Dispatched {
                terminal: ModelAttemptTerminalKind::Response,
                invocation,
            } = outcome
            else {
                panic!("maximum fixture must execute one real model provider call")
            };
            let bindings = NodeInvocationBindings::try_new(
                start.activation(),
                [NodeInvocationBinding::from_model(&invocation).unwrap()],
            )
            .unwrap();
            let mut usage = serde_json::to_value(maximum_model_usage(&invocation)).unwrap();
            usage["graph_steps"] = json!("1");
            usage["output_bytes"] = json!(self.output.data().stats().compact_bytes().to_string());
            Ok(GraphNodeExecution::new(
                NodeStateChange::Unchanged,
                NodeControl::Terminal {
                    output: self.output.clone(),
                },
                bindings,
                serde_json::from_value(usage).unwrap(),
            ))
        })
    }
}

fn maximum_fixture(
    store: &PostgresStore,
) -> (
    CompiledGraph,
    ExecutableGraphRegistry,
    Arc<AtomicUsize>,
    InvocationId,
) {
    let input = schema("ordinary-maximum-input");
    let state = schema("ordinary-maximum-state");
    let update = schema("ordinary-maximum-update");
    let output = schema("ordinary-maximum-output");
    let reducer = GraphReducerReference::new(
        capability("ordinary-maximum-reducer"),
        Digest::sha256(b"ordinary maximum reducer"),
    );
    let node = NodeId::new("finish").unwrap();
    let graph = CompiledGraph::compile(
        capability("ordinary-maximum-graph"),
        input.0.clone(),
        state.0.clone(),
        update.0.clone(),
        output.0.clone(),
        reducer.clone(),
        ReadyNodes::try_new([node.clone()]).unwrap(),
        [GraphNode::new(node.clone(), None, GraphRoutes::empty(), None, true).unwrap()],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    for (reference, document) in [input, state, update, output] {
        schemas.register(reference, document).unwrap();
    }
    register_standard_graph_driver_event_schema(&mut schemas).unwrap();
    register_standard_graph_lifecycle_event_schema(&mut schemas).unwrap();
    register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas.build().unwrap());
    registry.register_graph(graph.clone()).unwrap();
    registry
        .register_reducer(Arc::new(TestReducer { reference: reducer }))
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut models = ModelProviderRegistryBuilder::new();
    models
        .register(Arc::new(MaximumModel {
            descriptor: model_descriptor(),
            calls: calls.clone(),
        }))
        .unwrap();
    let executor = DurableInvocationExecutor::new(
        store.clone(),
        invocation_schema_registry(),
        models.build(),
        ToolProviderRegistryBuilder::new().build(),
        Arc::new(StaticInvocationBudget {
            resolved: invocation_budget(),
        }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let invocation_id = InvocationId::generate();
    registry
        .register_node(Arc::new(LargeTerminal {
            store: store.clone(),
            executor,
            invocation_id,
            graph: graph.reference(),
            node,
            output: NodeTerminalOutput::new(
                graph.output_schema().clone(),
                large_json(JsonLimits::MAXIMUM.max_bytes() - 16 * 1024),
            )
            .unwrap(),
        }))
        .unwrap();
    (graph, registry.build().unwrap(), calls, invocation_id)
}

async fn maximum_handoff(
    store: &PostgresStore,
    graph: &CompiledGraph,
    registry: &ExecutableGraphRegistry,
) -> stateknot_runtime::GraphLifecycleBarrierHandoff {
    let template = terminal_evidence(graph);
    let tenant = tenant("ordinary-maximum-components");
    let run = RunId::generate();
    let owner = AgentResultProvenance::for_agent(
        tenant.clone(),
        run,
        ThreadId::generate(),
        InvocationId::generate(),
        template.descriptor(),
    );
    store
        .register_graph_definition(tenant.clone(), graph.clone())
        .await
        .unwrap();
    let admitted = store.admit_run(owner).await.unwrap();
    let write = CheckpointWrite::initial(
        tenant.clone(),
        run,
        CheckpointId::generate(),
        graph.reference(),
        CheckpointState::new(
            graph.state_schema().clone(),
            large_json(JsonLimits::MAXIMUM.max_bytes()),
        )
        .unwrap(),
        graph.entry_nodes().clone(),
    )
    .unwrap();
    store
        .append_control_plane_checkpoint(
            control_append(tenant.clone(), run, EventId::generate()),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
            write,
        )
        .await
        .unwrap();
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap()
        .into_parts()
        .0
    else {
        panic!("actual large terminal plan")
    };
    *handoff
}

async fn maximum_evidence(
    store: &PostgresStore,
    db: &PgPool,
    graph: &CompiledGraph,
    handoff: &stateknot_runtime::GraphLifecycleBarrierHandoff,
    invocation_id: InvocationId,
) -> GraphTerminalEvidence {
    let template = terminal_evidence(graph);
    let owner = handoff.plan().barrier().base_checkpoint();
    let journal = store
        .load_journal_page(
            owner.tenant_id(),
            owner.run_id(),
            None,
            JournalPageSize::new(128).unwrap(),
        )
        .await
        .unwrap();
    assert!(!journal.has_more());
    let event_bytes: u64 = journal
        .events()
        .iter()
        .map(|e| u64::try_from(serde_json_canonicalizer::to_vec(e).unwrap().len()).unwrap())
        .sum();
    let checkpoint_bytes: i64 = query_scalar("SELECT sum(octet_length(checkpoint_bytes))::bigint FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2").bind(owner.tenant_id().as_str()).bind(*owner.run_id().as_uuid()).fetch_one(db).await.unwrap();
    let maximum = u64::try_from(JsonLimits::MAXIMUM.max_bytes()).unwrap();
    let invocation = store
        .load_model_invocation(owner.tenant_id(), owner.run_id(), invocation_id)
        .await
        .unwrap();
    let mut usage = serde_json::to_value(maximum_model_usage(&invocation)).unwrap();
    for (name, count) in [
        ("graph_depth", 1),
        ("graph_steps", 1),
        ("input_bytes", maximum),
        ("output_bytes", maximum - 16 * 1024),
        ("event_bytes", event_bytes),
        ("checkpoint_bytes", u64::try_from(checkpoint_bytes).unwrap()),
    ] {
        usage[name] = json!(count.to_string());
    }
    let usage = serde_json::from_value(usage).unwrap();
    GraphTerminalEvidence::new(
        template.descriptor().clone(),
        AgentRequest::new(
            template.request().input_schema().clone(),
            large_json(JsonLimits::MAXIMUM.max_bytes()),
            template.request().budget_limits().clone(),
        ),
        template.budget().clone(),
        template.artifacts().clone(),
        usage,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trusted_root_keeps_maximum_input_state_and_large_terminal_together() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = ordinary_pool().await;
    let (graph, registry, model_calls, invocation_id) = maximum_fixture(&store);
    let handoff = Box::pin(maximum_handoff(&store, &graph, &registry)).await;
    assert_eq!(model_calls.load(Ordering::SeqCst), 1);
    let evidence = Box::pin(maximum_evidence(
        &store,
        &db,
        &graph,
        &handoff,
        invocation_id,
    ))
    .await;
    let owner = handoff.plan().barrier().base_checkpoint().clone();
    let before = store
        .load_run(owner.tenant_id(), owner.run_id())
        .await
        .unwrap();
    let expected = original_result(&before, &handoff, &evidence);
    expected
        .validate_for(
            before.lifecycle().provenance(),
            evidence.request(),
            evidence.descriptor(),
            evidence.budget(),
        )
        .unwrap();
    let inputs = AgentTerminalValidationInputs::for_descriptor(
        evidence.descriptor(),
        evidence.request().clone(),
        evidence.budget().clone(),
    );
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        registry.clone(),
        Arc::new(StaticLifecycleEvidence {
            terminal: evidence,
            failure: None,
        }),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        lifecycle.commit_barrier(handoff.clone()).await.unwrap(),
        GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));
    let whole = store
        .load_root_graph_barrier(&owner)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(whole.committed_lifecycle().result(), Some(&expected));
    assert_eq!(whole.terminal_validation_inputs(), Some(&inputs));
    assert_eq!(
        whole.checkpoint().state().data().stats().compact_bytes(),
        JsonLimits::MAXIMUM.max_bytes()
    );
    let bytes: i32 = query_scalar("SELECT octet_length(bundle_bytes) FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2").bind(owner.tenant_id().as_str()).bind(*owner.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    // The compact bundle stores the full validation request and terminal output;
    // the full state remains in its separately anchored immutable checkpoint.
    let payload_bytes = inputs.request().input().stats().compact_bytes()
        + expected.output().stats().compact_bytes();
    assert!(usize::try_from(bytes).unwrap() > payload_bytes);
    eprintln!(
        "complete maximum components: inline payload={payload_bytes}, bundle={bytes}, separately anchored state={}",
        whole.checkpoint().state().data().stats().compact_bytes()
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let ack = DurableGraphLifecycle::new(
        store.clone(),
        registry,
        Arc::new(CountUnavailable(calls.clone())),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        ack.commit_barrier(handoff).await.unwrap(),
        GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Idempotent { .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(model_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .load_run(owner.tenant_id(), owner.run_id())
            .await
            .unwrap()
            .lifecycle()
            .result(),
        Some(&expected)
    );
    db.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trusted_root_terminal_keeps_original_known_and_unpriced_cost_predicate() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for (name, costs, unpriced) in [
        (
            "ordinary-cost-at-limit",
            json!([{"currency":"USD","micro_units":"1000000"}]),
            0,
        ),
        (
            "ordinary-cost-exceeded",
            json!([{"currency":"USD","micro_units":"1000001"}]),
            0,
        ),
        (
            "ordinary-cost-currency",
            json!([{"currency":"EUR","micro_units":"1"}]),
            0,
        ),
        ("ordinary-cost-unpriced", json!([]), 1),
        (
            "ordinary-cost-error-order",
            json!([{"currency":"USD","micro_units":"1000001"}]),
            1,
        ),
    ] {
        Box::pin(check_cost_case(&store, name, costs, unpriced)).await;
    }
    store.verify_schema().await.unwrap();
    store.close().await;
}

pub(crate) async fn qualify_trusted_graph_with_store(store: &PostgresStore) {
    Box::pin(check_cost_case(
        store,
        "role-ordinary-root-v2",
        json!([]),
        0,
    ))
    .await;
}

async fn check_cost_case(store: &PostgresStore, name: &str, costs: Value, unpriced: u64) {
    let fixture = driver_fixture();
    let template = terminal_evidence(&fixture.graph);
    let evidence = evidence_with_usage(&template, usage_with_cost(&template, costs, unpriced));
    let (handoff, initial) = Box::pin(terminal_handoff(store, &fixture, &evidence, name)).await;
    let tenant = initial.tenant_id();
    let run = initial.run_id();
    let before = store.load_run(tenant, run).await.unwrap();
    let result = original_result(&before, &handoff, &evidence);
    let original = result.validate_for(
        before.lifecycle().provenance(),
        evidence.request(),
        evidence.descriptor(),
        evidence.budget(),
    );
    let snapshot = AgentTerminalValidationInputs::for_descriptor(
        evidence.descriptor(),
        evidence.request().clone(),
        evidence.budget().clone(),
    );
    assert_eq!(
        original,
        result.validate_for_terminal_inputs(before.lifecycle().provenance(), &snapshot)
    );
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(StaticLifecycleEvidence {
            terminal: evidence,
            failure: None,
        }),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let outcome = lifecycle.commit_barrier(handoff.clone()).await;
    let after = store.load_run(tenant, run).await.unwrap();
    match original {
        Ok(()) => {
            assert!(matches!(
                outcome.unwrap(),
                GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
            ));
            assert_eq!(after.lifecycle().result(), Some(&result));
            let whole = store
                .load_root_graph_barrier(handoff.plan().barrier().base_checkpoint())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(whole.terminal_validation_inputs(), Some(&snapshot));
            assert_eq!(
                whole.event().payload().data().as_value()["version"],
                json!(2)
            );
            assert_eq!(
                whole.event().payload().data().as_value()["source"],
                json!("trusted_graph")
            );
        }
        Err(expected) => {
            let GraphLifecycleError::AgentResultValidation { source } = outcome.unwrap_err() else {
                panic!("ordinary terminal must retain the original validation classification")
            };
            assert_eq!(*source, expected);
            assert!(matches!(
                expected,
                AgentResultValidationError::Budget { .. }
            ));
            assert_eq!(
                serde_json_canonicalizer::to_vec(after.lifecycle()).unwrap(),
                serde_json_canonicalizer::to_vec(before.lifecycle()).unwrap()
            );
            assert_eq!(after.journal_head(), before.journal_head());
            assert_eq!(after.checkpoint(), before.checkpoint());
            assert!(after.lease().is_some());
            assert!(
                store
                    .load_root_graph_barrier(handoff.plan().barrier().base_checkpoint())
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
    assert!(!after.is_quarantined());
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trusted_root_rejects_fresh_source_substitution_without_partial_commit() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = ordinary_pool().await;
    let fixture = driver_fixture();
    for (name, fault) in [
        (
            "start-preimage",
            r#"NEW.bundle_bytes:=convert_to(regexp_replace(convert_from(NEW.bundle_bytes,'UTF8'),$r$"start_projection_digest":"sha256:[0-9a-f]{64}$r$,$r$"start_projection_digest":"sha256:$r$||repeat('0',64)),'UTF8');"#,
        ),
        (
            "creation-time",
            r#"NEW.bundle_bytes:=convert_to(regexp_replace(convert_from(NEW.bundle_bytes,'UTF8'),$r$"started_at":"[^"]+"$r$,$r$"started_at":"1970-01-01T00:00:00Z"$r$),'UTF8');"#,
        ),
        (
            "format-downgrade",
            r#"NEW.bundle_bytes:=convert_to(replace(convert_from(NEW.bundle_bytes,'UTF8'),'"version":2','"version":1'),'UTF8');"#,
        ),
        (
            "agent-witness",
            "UPDATE stateknot.runs SET agent_deadline_at=clock_timestamp()+interval '1 hour' WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;",
        ),
    ] {
        Box::pin(check_source_fault(&store, &db, &fixture, name, fault)).await;
    }
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

async fn check_source_fault(
    store: &PostgresStore,
    db: &PgPool,
    fixture: &DriverFixture,
    name: &str,
    fault: &str,
) {
    let prepared = Box::pin(prepare_trusted_terminal(
        store,
        fixture,
        &format!("ordinary-source-fault-{name}"),
    ))
    .await;
    let base = prepared.handoff.plan().barrier().base_checkpoint();
    let before = initial_snapshot(db, &prepared.run).await;
    query(&format!("CREATE FUNCTION stateknot.test_ordinary_source_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN {fault} RETURN NEW; END $$")).execute(db).await.unwrap();
    query("CREATE TRIGGER test_ordinary_source_fault BEFORE INSERT ON stateknot.root_graph_barriers FOR EACH ROW EXECUTE FUNCTION stateknot.test_ordinary_source_fault()").execute(db).await.unwrap();
    let executable = fixture
        .registry
        .resolve(&fixture.graph.reference())
        .unwrap();
    let outcome = store
        .append_worker_graph_barrier(
            prepared.append.clone(),
            prepared.intent.clone(),
            fixture.registry.schemas(),
            executable.reducer(),
        )
        .await;
    query("DROP TRIGGER test_ordinary_source_fault ON stateknot.root_graph_barriers")
        .execute(db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_ordinary_source_fault()")
        .execute(db)
        .await
        .unwrap();
    let after = initial_snapshot(db, &prepared.run).await;
    assert!(
        matches!(
            outcome,
            Err(StoreError::CorruptData { .. }
                | StoreError::Database { .. }
                | StoreError::RootGraphBarrierCompoundRequired)
        ),
        "{name}: {outcome:?}"
    );
    assert_eq!(before, after, "{name}: full fresh transaction rollback");
    assert!(
        !store
            .load_run(base.tenant_id(), base.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    assert!(matches!(
        store
            .append_worker_graph_barrier(
                prepared.append,
                prepared.intent,
                fixture.registry.schemas(),
                executable.reducer()
            )
            .await
            .unwrap(),
        stateknot_store_postgres::RootGraphBarrierCommitOutcome::Committed(_)
    ));
}

struct PreparedTerminal {
    handoff: stateknot_runtime::GraphLifecycleBarrierHandoff,
    run: stateknot_store_postgres::StoredRun,
    append: JournalAppend,
    intent: stateknot_store_postgres::RootGraphBarrierIntent,
}

async fn prepare_trusted_terminal(
    store: &PostgresStore,
    fixture: &DriverFixture,
    name: &str,
) -> PreparedTerminal {
    let evidence = terminal_evidence(&fixture.graph);
    let (handoff, _) = Box::pin(terminal_handoff(store, fixture, &evidence, name)).await;
    let base = handoff.plan().barrier().base_checkpoint();
    let before_run = store
        .load_run(base.tenant_id(), base.run_id())
        .await
        .unwrap();
    let event = EventId::generate();
    let intent = store
        .prepare_worker_graph_barrier(
            handoff.plan().clone(),
            before_run.lifecycle().clone(),
            event,
            Some(original_result(&before_run, &handoff, &evidence)),
            Some(AgentTerminalValidationInputs::for_descriptor(
                evidence.descriptor(),
                evidence.request().clone(),
                evidence.budget().clone(),
            )),
        )
        .await
        .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(before_run.journal_head().unwrap().clone()),
        JournalEventIntent::worker(
            base.tenant_id().clone(),
            base.run_id(),
            event,
            handoff.lease().fence().clone(),
            intent.payload().unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    PreparedTerminal {
        handoff,
        run: before_run,
        append,
        intent,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trusted_root_final_clock_rejects_slow_deferred_terminal_and_rolls_back() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let db = ordinary_pool().await;
    let fixture = driver_fixture();
    let prepared = Box::pin(prepare_trusted_terminal(
        &store,
        &fixture,
        "ordinary-deferred-terminal",
    ))
    .await;
    let before = initial_snapshot(&db, &prepared.run).await;
    query("CREATE FUNCTION stateknot.test_ordinary_deferred_lease() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(3); RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER test_ordinary_deferred_lease AFTER INSERT ON stateknot.root_graph_barriers DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.test_ordinary_deferred_lease()").execute(&db).await.unwrap();
    let executable = fixture
        .registry
        .resolve(&fixture.graph.reference())
        .unwrap();
    let outcome = store
        .append_worker_graph_barrier(
            prepared.append,
            prepared.intent,
            fixture.registry.schemas(),
            executable.reducer(),
        )
        .await;
    query("DROP TRIGGER test_ordinary_deferred_lease ON stateknot.root_graph_barriers")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_ordinary_deferred_lease()")
        .execute(&db)
        .await
        .unwrap();
    let after = initial_snapshot(&db, &prepared.run).await;
    assert!(
        matches!(outcome, Err(StoreError::LeaseExpired)),
        "{outcome:?}"
    );
    assert_eq!(
        before, after,
        "original lifecycle/lease/head and all fresh components roll back"
    );
    assert!(
        !store
            .load_run(
                prepared.handoff.lease().fence().tenant_id(),
                prepared.handoff.lease().fence().run_id()
            )
            .await
            .unwrap()
            .is_quarantined()
    );
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}
