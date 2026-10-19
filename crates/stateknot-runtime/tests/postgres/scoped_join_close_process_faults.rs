// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Public Store-only v2 closure recovery through an actual lost COMMIT response.
//! Runtime owns the existing process/protocol harness; no Driver is exercised.
use super::*;
use crate::{
    commit_proxy::{CommitProxy, Cut, TransactionTarget, loopback_target},
    process_harness::{INPUT_ENV, TestProcess, parent_control, publish_ready},
};
use sqlx_core::{connection::ConnectOptions, query_scalar::query_scalar};
use sqlx_postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use stateknot_core::{
    AgentAdmission, AgentAdmissionIntent, ChildRunAdmissionIntent, ChildRunDeclaration,
    ChildRunJoinRequest, ChildRunKey, ChildRunSlot, ChildRunTopologyLimits, GraphChildRunPolicy,
    GraphFrameCall, GraphFrameCallPolicy, GraphFrameEntryPlan, GraphRoute, JournalEvent,
    JournalHead, NodeAttemptStartHead, NodeAttemptStatus, ReadyNodeRecoveryPlanner, RouteId,
    RunRevision,
};
use stateknot_store_postgres::{
    ChildCancellationDelivery, ChildCancellationOutcome, ChildJoinCommitOutcome,
    ChildRunCommitOutcome, ChildRunSettlementOutcome, GraphFrameClosureCommitOutcome,
    GraphFrameEntryCommitOutcome, StoredAgentAdmission, StoredGraphFrameClosure,
};
use std::collections::BTreeMap;

const SELECTED: &str = "scoped_join_close_process_faults::registered_scoped_join_close_lost_commit_ack_survives_real_kill_and_cold_retry";
type Snapshot = BTreeMap<String, String>;

#[derive(serde::Deserialize, serde::Serialize)]
struct Scenario {
    key: ChildRunKey,
    owner: NodeAttemptStartHead,
    fence: RunFence,
    root_checkpoint: Checkpoint,
    join_request: ChildRunJoinRequest,
    join_registration: JournalEvent,
    observed: JournalHead,
    revision: RunRevision,
    lifecycle: Value,
    direct: BudgetUsage,
    child_usage: BudgetUsage,
    event_id: EventId,
}

fn payload(kind: &str) -> JournalPayload {
    JournalPayload::new(
        test_payload().schema().clone(),
        JournalEventKind::new(kind).unwrap(),
        BoundedJson::try_from_value(json!({"test": true})).unwrap(),
    )
    .unwrap()
}

fn control(tenant: &TenantId, run: RunId, head: JournalHead, kind: &str) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(head),
        JournalEventIntent::control_plane(tenant.clone(), run, EventId::generate(), payload(kind))
            .unwrap(),
    )
    .unwrap()
}

fn worker_append_kind(
    tenant: &TenantId,
    run: RunId,
    head: JournalHead,
    fence: &RunFence,
    kind: &str,
) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(head),
        JournalEventIntent::worker(
            tenant.clone(),
            run,
            EventId::generate(),
            fence.clone(),
            payload(kind),
        )
        .unwrap(),
    )
    .unwrap()
}

fn cancellation(at: Timestamp, event: EventId) -> RunCancellationRequest {
    RunCancellationRequest::new(
        Failure::new(
            FailureId::generate(),
            FailureCategory::Cancelled,
            FailureCode::new("test.scoped_join.cancel").unwrap(),
            FailureOrigin::new("stateknot.runtime.integration").unwrap(),
            FailureMessage::new("Cancel the real registered scoped Join.").unwrap(),
            RetryAdvice::Never,
        )
        .unwrap()
        .with_caused_by_event(event),
        at,
    )
    .unwrap()
}

fn declared(
    graph: CompiledGraph,
    node: &str,
    isolated: &CompiledGraph,
    agent: &AgentDescriptor,
) -> CompiledGraph {
    graph
        .with_child_runs(
            GraphChildRunPolicy::new(
                ChildRunTopologyLimits::new(2, 8, 4).unwrap(),
                [ChildRunDeclaration::new(
                    NodeId::new(node).unwrap(),
                    ChildRunSlot::new("analysis").unwrap(),
                    agent,
                    isolated,
                )
                .unwrap()],
            )
            .unwrap(),
        )
        .unwrap()
}

// Reuse Runtime's immutable graph/schema and admission-data builders. They do
// not dispatch an executor. All durable preparation below calls public Store APIs.
fn graphs() -> (DriverFixture, CompiledGraph, CompiledGraph, AgentDescriptor) {
    let mut fixture = driver_fixture();
    let template = &fixture.graph;
    let scoped = |name| {
        CompiledGraph::compile(
            capability(name),
            template.state_schema().clone(),
            template.state_schema().clone(),
            template.update_schema().clone(),
            template.output_schema().clone(),
            template.reducer().clone(),
            template.entry_nodes().clone(),
            template.nodes().iter().cloned(),
            template.limits(),
        )
        .unwrap()
    };
    let isolated = scoped("join-stop-process-child");
    let agent = terminal_evidence(&isolated).descriptor().clone();
    let leaf = declared(
        scoped("join-stop-process-leaf"),
        "Step_A",
        &isolated,
        &agent,
    );
    let call = GraphFrameCall::new(
        NodeId::new("call").unwrap(),
        NodeId::new("slot.a").unwrap(),
        &leaf,
        RouteId::new("return").unwrap(),
    )
    .unwrap();
    let root = CompiledGraph::compile(
        capability("join-stop-process-root"),
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.output_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        ReadyNodes::try_new([NodeId::new("call").unwrap()]).unwrap(),
        [
            GraphNode::new(
                NodeId::new("call").unwrap(),
                None,
                GraphRoutes::try_new([GraphRoute::new(
                    RouteId::new("return").unwrap(),
                    ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap(),
                )
                .unwrap()])
                .unwrap(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("finish").unwrap(),
                None,
                GraphRoutes::empty(),
                None,
                true,
            )
            .unwrap(),
        ],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap()
    .with_frame_calls(GraphFrameCallPolicy::new(7, 8, [call]).unwrap())
    .unwrap();
    fixture.graph = declared(root, "finish", &isolated, &agent);
    (fixture, leaf, isolated, agent)
}

fn initial_usage(admission: &StoredAgentAdmission) -> BudgetUsage {
    BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(1))
        .event_bytes(ByteCount::new(
            serde_json_canonicalizer::to_vec(admission.event())
                .unwrap()
                .len() as u64,
        ))
        .checkpoint_bytes(ByteCount::new(
            serde_json_canonicalizer::to_vec(admission.checkpoint())
                .unwrap()
                .len() as u64,
        ))
        .build()
        .unwrap()
}

async fn complete_journal(
    store: &PostgresStore,
    tenant: &TenantId,
    run: RunId,
    count: usize,
) -> usize {
    let journal = store
        .load_journal_page(tenant, run, None, JournalPageSize::new(8).unwrap())
        .await
        .unwrap();
    assert!(!journal.has_more());
    assert_eq!(journal.events().len(), count);
    journal
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len())
        .sum()
}

async fn prepare(store: &PostgresStore) -> Scenario {
    let definitions = graphs();
    let tenant = tenant("scoped-join-close-process");
    let ids = AgentRunIds::generate();
    Box::pin(prepare_with_identity(store, definitions, tenant, ids)).await
}

#[allow(clippy::too_many_lines)]
async fn prepare_with_identity(
    store: &PostgresStore,
    definitions: (DriverFixture, CompiledGraph, CompiledGraph, AgentDescriptor),
    tenant: TenantId,
    ids: AgentRunIds,
) -> Scenario {
    let (fixture, leaf, isolated, agent) = definitions;
    let run = ids.run_id();
    for graph in [&fixture.graph, &leaf, &isolated] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let request = durable_admission_request(
        &fixture,
        tenant.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let intent = request.intent();
    let append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(
            tenant.clone(),
            run,
            request.admission_event_id(),
            JournalPayload::new(
                test_payload().schema().clone(),
                JournalEventKind::new(AgentAdmission::JOURNAL_EVENT_KIND).unwrap(),
                BoundedJson::try_from_value(json!({"intent_digest":intent.intent_digest()}))
                    .unwrap(),
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint = CheckpointWrite::initial(
        tenant.clone(),
        run,
        request.initial_checkpoint_id(),
        fixture.graph.reference(),
        request.initial_state().clone(),
        fixture.graph.entry_nodes().clone(),
    )
    .unwrap();
    let admitted = Box::pin(store.admit_agent_run(
        intent.clone(),
        append,
        checkpoint,
        fixture.registry.schemas(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        admitted,
        AgentAdmissionCommitOutcome::Committed(_)
    ));
    let admission = admitted.stored();
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &fixture.graph.frame_calls().unwrap().calls()[0],
        &fixture.graph,
        admission.checkpoint(),
        &leaf,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence.clone(),
    )
    .unwrap();
    let entered = Box::pin(store.enter_graph_frame(
        plan,
        EventId::generate(),
        admission.event().head(),
        initial_usage(admission),
        fixture.registry.schemas(),
    ))
    .await
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(entry) = entered else {
        panic!("fresh frame entry")
    };
    let dispatch =
        ReadyNodeRecoveryPlanner::for_frame(entry.entry().checkpoint().clone(), fence.clone())
            .unwrap()
            .finish(entry.event().head(), entry.event().recorded_at())
            .unwrap();
    let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
        worker_append(
            tenant.clone(),
            run,
            EventId::generate(),
            entry.event().head(),
            fence.clone(),
        ),
        &dispatch,
        &NodeId::new("Step_A").unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let owner = started.attempt().start().head();
    let parent = admission.admission().intent();
    let child = AgentAdmissionIntent::new(
        AgentResultProvenance::for_agent(
            tenant.clone(),
            RunId::generate(),
            ThreadId::generate(),
            InvocationId::generate(),
            &agent,
        ),
        agent,
        AgentRequest::new(
            isolated.input_schema().clone(),
            BoundedJson::try_from_value(json!({"step":"child"})).unwrap(),
            parent
                .request()
                .budget_limits()
                .clone()
                .with_event_bytes(ByteCount::new(1_048_576))
                .with_checkpoint_bytes(ByteCount::new(1_048_576))
                .with_graph_steps(ExecutionCount::new(8))
                .with_input_bytes(ByteCount::new(512))
                .with_output_bytes(ByteCount::new(512)),
        ),
        parent.budget_layers().iter().cloned(),
        isolated.reference(),
        parent.authority().clone(),
    )
    .unwrap();
    let child_intent = ChildRunAdmissionIntent::for_frame(
        admission.admission(),
        entry.entry().checkpoint(),
        ChildRunKey::new(
            owner.activation().clone(),
            ChildRunSlot::new("analysis").unwrap(),
        )
        .unwrap(),
        child,
        isolated.clone(),
        CheckpointState::new(
            isolated.state_schema().clone(),
            BoundedJson::try_from_value(json!({"step":"child"})).unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let checkpoint_bytes = serde_json_canonicalizer::to_vec(admission.checkpoint())
        .unwrap()
        .len()
        + serde_json_canonicalizer::to_vec(entry.entry().checkpoint().checkpoint())
            .unwrap()
            .len()
        + serde_json_canonicalizer::to_vec(&entry.entry().checkpoint().head())
            .unwrap()
            .len();
    let direct_at = |event_bytes| {
        BudgetUsage::builder()
            .graph_depth(ExecutionCount::new(2))
            .graph_steps(ExecutionCount::new(2))
            .concurrent_branches(ExecutionCount::new(1))
            .fan_out(ExecutionCount::new(1))
            .event_bytes(ByteCount::new(event_bytes as u64))
            .checkpoint_bytes(ByteCount::new(checkpoint_bytes as u64))
            .input_bytes(ByteCount::new(
                serde_json_canonicalizer::to_vec(parent.request().input())
                    .unwrap()
                    .len() as u64,
            ))
            .build()
            .unwrap()
    };
    let key = child_intent.key().clone();
    let child_ids = child_intent.child().provenance();
    let spawned = Box::pin(
        store.admit_child_run(
            child_intent.clone(),
            &owner,
            worker_append_kind(
                &tenant,
                run,
                started.attempt().start().journal_head().clone(),
                &fence,
                "child-run-admitted",
            ),
            JournalAppend::new(
                JournalExpectation::empty(),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    child_ids.run_id(),
                    EventId::generate(),
                    payload(AgentAdmission::JOURNAL_EVENT_KIND),
                )
                .unwrap(),
            )
            .unwrap(),
            CheckpointWrite::initial(
                tenant.clone(),
                child_ids.run_id(),
                CheckpointId::generate(),
                child_intent.child().graph().clone(),
                child_intent.initial_state().clone(),
                isolated.entry_nodes().clone(),
            )
            .unwrap(),
            direct_at(complete_journal(store, &tenant, run, 3).await),
            fixture.registry.schemas(),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(spawned, ChildRunCommitOutcome::Committed(_)));
    let joined = Box::pin(store.register_child_join(
        ChildRunJoinRequest::new([key.clone()]).unwrap(),
        &owner,
        worker_append_kind(
            &tenant,
            run,
            spawned.record().spawn().head(),
            &fence,
            "child-join-registered",
        ),
    ))
    .await
    .unwrap();
    assert!(matches!(joined, ChildJoinCommitOutcome::Committed(_)));
    assert!(joined.record().head().is_none());
    let current = store.load_run(&tenant, run).await.unwrap();
    assert!(current.lease().is_none());
    let cancel_id = EventId::generate();
    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(current.journal_head().unwrap().clone()),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    run,
                    cancel_id,
                    payload("test-parent-cancelled"),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                current.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: cancellation(store.observe_database_clock().await.unwrap(), cancel_id),
                },
            ),
        )
        .await
        .unwrap();
    let child = spawned.record().child();
    let delivery_id = EventId::generate();
    let delivered = Box::pin(
        store.deliver_child_cancellation(
            &key,
            JournalAppend::new(
                JournalExpectation::exact(child.run().journal_head().unwrap().clone()),
                JournalEventIntent::control_plane(
                    tenant.clone(),
                    child_ids.run_id(),
                    delivery_id,
                    payload("child-run-cancellation-requested"),
                )
                .unwrap(),
            )
            .unwrap(),
            cancellation(store.observe_database_clock().await.unwrap(), delivery_id),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(delivered, ChildCancellationDelivery::Committed(_)));
    let receipt = delivered.record().receipt().unwrap();
    assert_eq!(receipt.outcome(), ChildCancellationOutcome::Requested);
    let child_usage = BudgetUsage::builder()
        .checkpoint_bytes(initial_usage(child).checkpoint_bytes())
        .event_bytes(ByteCount::new(
            complete_journal(store, &tenant, child_ids.run_id(), 2).await as u64,
        ))
        .input_bytes(ByteCount::new(
            serde_json_canonicalizer::to_vec(child_intent.child().request().input())
                .unwrap()
                .len() as u64,
        ))
        .build()
        .unwrap();
    store
        .append_control_plane(
            control(
                &tenant,
                child_ids.run_id(),
                receipt.head().clone(),
                "test-child-cancellation-confirmed",
            ),
            RunProjection::transition(
                receipt.lifecycle().revision(),
                RunTransition::ConfirmCancellation {
                    completed_at: store.observe_database_clock().await.unwrap(),
                    usage: child_usage,
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .load_run(&tenant, child_ids.run_id())
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Cancelled
    );
    let current = store.load_run(&tenant, run).await.unwrap();
    let settled = Box::pin(store.settle_child_run(
        &key,
        control(
            &tenant,
            run,
            current.journal_head().unwrap().clone(),
            "child-run-settled",
        ),
    ))
    .await
    .unwrap();
    let ChildRunSettlementOutcome::Committed { record, .. } = settled else {
        panic!("fresh real settlement")
    };
    let child_usage = record.settlement().unwrap().usage().clone();
    // Admission, frame entry, ordinary Start, spawn, Join, cancellation, settlement.
    // The two physical starts and actual bytes are complete for this zero-I/O fixture.
    let direct = direct_at(complete_journal(store, &tenant, run, 7).await);
    assert_ne!(direct, BudgetUsage::zero());
    assert_eq!(direct.unpriced_cost_events(), ExecutionCount::ZERO);
    let current = store.load_run(&tenant, run).await.unwrap();
    assert_eq!(
        current.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert!(current.lease().is_none());
    let active = Box::pin(store.load_active_graph_frame(&tenant, run))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.checkpoint(), entry.entry().checkpoint());
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, run)
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    let history = Box::pin(store.load_node_attempt_history_page(
        key.parent(),
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].start().head(), owner);
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    assert!(history.records()[0].completion().is_none());
    let retained_join = Box::pin(store.load_child_join(key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained_join.request(), joined.record().request());
    assert_eq!(retained_join.registration(), joined.record().registration());
    assert!(retained_join.binding().is_none());
    assert!(retained_join.head().is_none());
    assert!(retained_join.consumed().is_none());
    assert_eq!(
        Box::pin(store.include_child_usage(&tenant, run, direct.clone()))
            .await
            .unwrap(),
        direct
            .checked_accumulate(&child_usage.cumulative_only())
            .unwrap(),
    );
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    Scenario {
        key,
        owner,
        fence,
        root_checkpoint: admission.checkpoint().clone(),
        join_request: joined.record().request().clone(),
        join_registration: joined.record().registration().clone(),
        observed: current.journal_head().unwrap().clone(),
        revision: current.lifecycle().revision(),
        lifecycle: serde_json::to_value(current.lifecycle()).unwrap(),
        direct,
        child_usage,
        event_id: EventId::generate(),
    }
}

async fn snapshot(pool: &PgPool, tenant: &TenantId) -> Snapshot {
    let tables = query_scalar::<_, String>(
        "SELECT quote_ident(c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='stateknot' AND c.relkind IN ('r','p') AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='tenant_id' AND NOT a.attisdropped) ORDER BY c.relname",
    ).fetch_all(pool).await.unwrap();
    let mut result = BTreeMap::new();
    for table in tables {
        let rows = query_scalar::<_, String>(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM stateknot.{table} t WHERE tenant_id=$1"
        )).bind(tenant.as_str()).fetch_one(pool).await.unwrap();
        result.insert(table, rows);
    }
    result
}

fn retained_original_facts(before: &Snapshot, after: &Snapshot) {
    for table in [
        "agent_admissions",
        "run_checkpoints",
        "run_attempt_claims",
        "node_attempts",
        "node_attempt_completions",
        "pending_node_results",
        "child_run_budget_accounts",
        "child_run_ownership",
        "child_run_terminals",
        "child_run_settlements",
        "child_run_joins",
        "child_run_join_bindings",
        "child_run_join_consumptions",
        "child_run_cancellations",
        "child_run_cancellation_receipts",
    ] {
        assert!(before.contains_key(table));
        assert_eq!(
            after.get(table),
            before.get(table),
            "retained tenant facts: {table}"
        );
    }
}

fn fingerprint(record: &StoredGraphFrameClosure) -> Value {
    json!({"digest":record.digest(), "event":record.event(), "direct":record.direct_usage(),
        "failure":record.failure(), "completions":record.completions()})
}

async fn owning_close(store: &PostgresStore, scenario: &Scenario) -> StoredGraphFrameClosure {
    let key = &scenario.key;
    let record = Box::pin(store.load_graph_frame_closure(key.tenant_id(), key.parent_run_id()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.event().event_id(), scenario.event_id);
    assert_eq!(record.direct_usage(), &scenario.direct);
    assert_eq!(record.completions().len(), 1);
    assert_ne!(record.completions()[0].start(), &scenario.owner);
    assert_eq!(record.completions()[0].start().fence(), &scenario.fence);
    let history = Box::pin(store.load_node_attempt_history_page(
        key.parent(),
        None,
        NodeAttemptHistoryPageSize::new(2).unwrap(),
    ))
    .await
    .unwrap();
    assert!(!history.has_more());
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].start().head(), scenario.owner);
    assert!(history.records()[0].completion().is_none());
    assert_eq!(history.records()[0].status(), NodeAttemptStatus::Executing);
    let join = Box::pin(store.load_child_join(key.parent()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(join.request(), &scenario.join_request);
    assert_eq!(join.registration(), &scenario.join_registration);
    assert!(join.binding().is_none());
    assert!(join.head().is_none());
    assert!(join.consumed().is_none());
    assert!(
        Box::pin(store.load_active_graph_frame(key.tenant_id(), key.parent_run_id()))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await
            .unwrap()
            .unwrap(),
        scenario.root_checkpoint
    );
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(run.journal_head(), Some(&record.event().head()));
    assert_eq!(
        serde_json::to_value(run.lifecycle()).unwrap(),
        scenario.lifecycle
    );
    assert!(run.lease().is_none());
    assert!(!run.is_quarantined());
    let child = Box::pin(store.load_child_run(key)).await.unwrap();
    assert_eq!(child.settlement().unwrap().usage(), &scenario.child_usage);
    assert_eq!(
        Box::pin(store.include_child_usage(
            key.tenant_id(),
            key.parent_run_id(),
            scenario.direct.clone()
        ))
        .await
        .unwrap(),
        scenario
            .direct
            .checked_accumulate(&scenario.child_usage.cumulative_only())
            .unwrap(),
    );
    record
}

async fn v2_bytes(pool: &PgPool, scenario: &Scenario) -> Vec<u8> {
    let bytes: Vec<u8> = query_scalar(
        "SELECT closure_bytes FROM stateknot.graph_frame_closures WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(scenario.key.tenant_id().as_str())
    .bind(*scenario.key.parent_run_id().as_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    let wire: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(wire["version"], 2);
    assert_eq!(wire["intent"]["version"], 2);
    assert!(wire["intent"]["stopped_join"].is_object());
    bytes
}

async fn worker_store(input: &Value) -> PostgresStore {
    let mut connection: PgConnectOptions =
        std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    let cut = input["role"].as_str() == Some("writer");
    if cut {
        connection = connection
            .host("127.0.0.1")
            .port(u16::try_from(input["proxy_port"].as_u64().unwrap()).unwrap());
    }
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, if cut { 1 } else { 8 })
        .with_acquire_timeout(Duration::from_secs(30))
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
        .with_lease_timing(Duration::from_secs(30), Duration::from_secs(5 * 60));
    PostgresStore::connect(connection.to_url_lossy().as_str(), options)
        .await
        .unwrap()
}

async fn worker(input: &Value) {
    let resume = parent_control();
    let scenario: Scenario = serde_json::from_value(input["scenario"].clone()).unwrap();
    let store = worker_store(input).await;
    let key = &scenario.key;
    if input["role"].as_str() == Some("writer") {
        publish_ready(&json!({"phase":"writer_before_close"}));
        resume.await.unwrap();
        let _ = Box::pin(store.close_graph_frames(
            key.tenant_id(),
            key.parent_run_id(),
            scenario.event_id,
            scenario.observed.clone(),
            scenario.revision,
            scenario.direct.clone(),
        ))
        .await;
        panic!("closure returned before the required lost-ACK process kill");
    }
    assert_eq!(input["role"].as_str(), Some("cold"));
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let before = snapshot(&pool, key.tenant_id()).await;
    let loaded = Box::pin(owning_close(&store, &scenario)).await;
    assert_eq!(fingerprint(&loaded), input["original"]);
    assert_ne!(scenario.observed, loaded.event().head());
    assert_eq!(
        Digest::sha256(v2_bytes(&pool, &scenario).await),
        serde_json::from_value::<Digest>(input["wire_digest"].clone()).unwrap()
    );
    assert_eq!(
        snapshot(&pool, key.tenant_id()).await,
        before,
        "cold owning reads changed tenant facts"
    );
    let retry_id = EventId::generate();
    assert_ne!(retry_id, scenario.event_id);
    assert_ne!(scenario.revision, RunRevision::ZERO);
    let retry = Box::pin(store.close_graph_frames(
        key.tenant_id(),
        key.parent_run_id(),
        retry_id,
        scenario.observed.clone(),
        RunRevision::ZERO,
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, GraphFrameClosureCommitOutcome::Existing(_)));
    assert_eq!(fingerprint(retry.record()), input["original"]);
    assert_eq!(
        snapshot(&pool, key.tenant_id()).await,
        before,
        "cold retry changed tenant facts"
    );
    let reloaded = Box::pin(owning_close(&store, &scenario)).await;
    assert_eq!(fingerprint(&reloaded), input["original"]);
    publish_ready(&json!({"phase":"cold_store_verified", "original":fingerprint(&reloaded)}));
    resume.await.unwrap();
    pool.close().await;
    store.close().await;
}

async fn wait_backend_disconnect(pool: &PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let gone: bool =
                query_scalar("SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
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
    .expect("killed writer backend did not disconnect");
}

async fn emit_evidence(
    pool: &PgPool,
    original: &StoredGraphFrameClosure,
    original_bytes: &[u8],
    killed: &std::process::ExitStatus,
) {
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(pool)
        .await
        .unwrap();
    println!(
        "\nSTATEKNOT_SCOPED_JOIN_CLOSE_COMMIT_LOSS_EVIDENCE={}",
        json!({
            "profile":"scoped-join-close-commit-loss-v1", "cut":"commit_response_withheld",
            "commit_forwarded":true, "commit_response_forwarded":false,
            "writer_killed_and_reaped":true, "writer_exit":killed.to_string(),
            "cold_process_success":true, "closure_version":2,
            "original_closure_recovered":true, "whole_tenant_unchanged_after_commit":true,
            "owner_status":"Executing", "owner_completion":false, "join_consumed":false,
            "qualification":"public-store-only", "driver_exercised":false,
            "closure_digest":original.digest(), "closure_bytes_digest":Digest::sha256(original_bytes),
            "invariants":"passed", "postgres":version,
            "termination":if cfg!(unix) { "SIGKILL" } else { "Child::kill" },
            "os":std::env::consts::OS, "arch":std::env::consts::ARCH,
        })
    );
}

async fn run_controller(store: &PostgresStore, connection: &PgConnectOptions) {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let scenario = Box::pin(prepare(store)).await;
    let key = &scenario.key;
    let before = snapshot(&pool, key.tenant_id()).await;
    assert_eq!(
        serde_json::from_str::<Value>(&before["graph_frame_closures"]).unwrap(),
        json!([])
    );
    let mut proxy = CommitProxy::start(
        connection,
        Cut::CommitResponse,
        TransactionTarget::GraphFrameClosure,
    )
    .await;
    let mut writer = TestProcess::spawn(
        SELECTED,
        &json!({"role":"writer", "scenario":scenario, "proxy_port":proxy.port()}),
    );
    assert_eq!(
        writer.ready().await.unwrap()["phase"],
        "writer_before_close"
    );
    writer.resume();
    // This rendezvous requires the actual backend COMMIT CommandComplete and
    // idle ReadyForQuery; both remain withheld from the writer until it is killed.
    proxy.wait_cut(Cut::CommitResponse).await;
    let backend_pid = proxy.backend_pid();
    let at_cut = snapshot(&pool, key.tenant_id()).await;
    let original = Box::pin(owning_close(store, &scenario)).await;
    let original_fingerprint = fingerprint(&original);
    let original_bytes = v2_bytes(&pool, &scenario).await;
    assert_eq!(
        snapshot(&pool, key.tenant_id()).await,
        at_cut,
        "owning reads at the held ACK changed tenant facts"
    );
    retained_original_facts(&before, &at_cut);
    assert_eq!(
        serde_json::from_str::<Value>(&at_cut["graph_frame_closures"])
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        serde_json::from_str::<Value>(&at_cut["run_events"])
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        serde_json::from_str::<Value>(&before["run_events"])
            .unwrap()
            .as_array()
            .unwrap()
            .len()
            + 1,
    );
    let killed = writer.kill_and_reap().await;
    proxy.stop().await;
    wait_backend_disconnect(&pool, backend_pid).await;
    assert_eq!(
        snapshot(&pool, key.tenant_id()).await,
        at_cut,
        "kill changed the already committed tenant facts"
    );
    let mut cold = TestProcess::spawn(
        SELECTED,
        &json!({
            "role":"cold", "scenario":scenario, "original":original_fingerprint,
            "wire_digest":Digest::sha256(&original_bytes),
        }),
    );
    let ready = cold.ready().await.unwrap();
    assert_eq!(ready["phase"], "cold_store_verified");
    assert_eq!(ready["original"], original_fingerprint);
    assert_eq!(
        snapshot(&pool, key.tenant_id()).await,
        at_cut,
        "separate cold process created tenant facts"
    );
    cold.resume();
    cold.wait_success().await;
    assert_eq!(snapshot(&pool, key.tenant_id()).await, at_cut);
    assert_eq!(v2_bytes(&pool, &scenario).await, original_bytes);
    assert_eq!(
        fingerprint(&Box::pin(owning_close(store, &scenario)).await),
        original_fingerprint
    );
    assert_eq!(snapshot(&pool, key.tenant_id()).await, at_cut);
    emit_evidence(&pool, &original, &original_bytes, &killed).await;
    pool.close().await;
}

#[tokio::test]
async fn registered_scoped_join_close_lost_commit_ack_survives_real_kill_and_cold_retry() {
    if let Ok(input) = std::env::var(INPUT_ENV) {
        let input = serde_json::from_str(&input).unwrap();
        Box::pin(worker(&input)).await;
        return;
    }
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
    Box::pin(run_controller(&store, connection.as_ref().unwrap())).await;
    store.close().await;
}

#[path = "scoped_join_driver_f2_red.rs"]
mod f2_driver;

#[path = "scoped_join_driver_f2_bounds.rs"]
mod f2_bounds;

#[path = "scoped_frame_cancellation_cold_red.rs"]
mod cold_continuation;

#[path = "scoped_frame_cancellation_child_quarantine.rs"]
mod child_quarantine;

#[path = "scoped_frame_cancellation_finalizer_acceptance.rs"]
mod finalizer_acceptance;

#[path = "scoped_frame_cancellation_terminal_process_faults.rs"]
mod terminal_confirmation;
