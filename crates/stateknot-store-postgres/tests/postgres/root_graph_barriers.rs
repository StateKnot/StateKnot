// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Whole Root dispositions, original input recovery and durable corruption isolation.
use super::*;
use stateknot_core::{
    AgentArtifacts, AgentRequest, AgentResult, ExecutionCount, GraphBarrierDisposition,
    GraphBarrierPlan, JsonLimits, NodeTerminalOutput, NodeWait, NodeWaits, PendingNodeResult,
    RunFence, TokenCount,
};
use stateknot_store_postgres::{
    RootGraphBarrierCommitOutcome, RootGraphBarrierIntent, StoredRootGraphBarrier,
};

struct Unavailable;
impl GraphSchemaValidator for Unavailable {
    fn validate(
        &self,
        _: &SchemaReference,
        _: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        Err(GraphSchemaValidationError::Unavailable)
    }
}
fn graph() -> CompiledGraph {
    let (_, template, _) = frame_transactions::graphs();
    let descriptor = agent_fixture_definition();
    CompiledGraph::compile(
        template.identity().clone(),
        descriptor.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        descriptor.output_schema().clone(),
        template.reducer().clone(),
        ReadyNodes::try_new([NodeId::new("work").unwrap()]).unwrap(),
        [
            GraphNode::new(
                NodeId::new("work").unwrap(),
                Some(ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap()),
                GraphRoutes::default(),
                Some(ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap()),
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("finish").unwrap(),
                None,
                GraphRoutes::default(),
                None,
                true,
            )
            .unwrap(),
        ],
        template.limits(),
    )
    .unwrap()
}
async fn admitted(
    store: &PostgresStore,
    name: &str,
) -> (StoredAgentAdmission, CompiledGraph, RunFence) {
    let graph = graph();
    let tenant = tenant(name);
    let run = RunId::generate();
    let admission = Box::pin(frame_transactions::admit(
        store, &tenant, run, &graph, &graph,
    ))
    .await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    (admission, graph, fence)
}
#[allow(clippy::too_many_lines)]
async fn succeed(
    store: &PostgresStore,
    base: &Checkpoint,
    fence: &RunFence,
    control: NodeControl,
    with_model: bool,
) -> (PendingNodeResult, BudgetUsage) {
    let observed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let planner = ReadyNodeRecoveryPlanner::new(base.clone(), fence.clone())
        .unwrap()
        .finish(observed.clone(), observed.recorded_at())
        .unwrap();
    let start = Box::pin(store.start_recovered_node_attempt(
        worker_append(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(observed),
            fence.clone(),
            30_000,
        ),
        &planner,
        base.ready_nodes().iter().next().unwrap(),
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    let activation = start.attempt().start().activation().clone();
    let mut head = start.event().head();
    let mut bindings = NodeInvocationBindings::empty();
    let mut usage = BudgetUsage::zero();
    if with_model {
        let intent =
            model_invocation_intent_for_activation(activation.clone(), InvocationId::generate());
        let prepared = store
            .prepare_model_invocation(
                worker_append(
                    fence.tenant_id().clone(),
                    fence.run_id(),
                    EventId::generate(),
                    JournalExpectation::exact(head),
                    fence.clone(),
                    30_001,
                ),
                intent.clone(),
            )
            .await
            .unwrap();
        let attempt = AttemptId::generate();
        let executing = store
            .advance_model_invocation(
                worker_append(
                    fence.tenant_id().clone(),
                    fence.run_id(),
                    EventId::generate(),
                    JournalExpectation::exact(prepared.event().head()),
                    fence.clone(),
                    30_002,
                ),
                &prepared.invocation().head(),
                ModelInvocationTransition::StartAttempt {
                    attempt_id: attempt,
                },
            )
            .await
            .unwrap();
        let response = model_response(&intent, attempt);
        // The fixture provider charges one explicit USD micro-unit for this response.
        // This is trusted test evidence, rather than an inferred zero charge.
        let costs = serde_json::from_value(json!([{"currency":"USD","micro_units":"1"}])).unwrap();
        usage = BudgetUsage::builder()
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
            .known_costs(costs)
            .build()
            .unwrap();
        let committed = store
            .advance_model_invocation(
                worker_append(
                    fence.tenant_id().clone(),
                    fence.run_id(),
                    EventId::generate(),
                    JournalExpectation::exact(executing.event().head()),
                    fence.clone(),
                    30_003,
                ),
                &executing.invocation().head(),
                ModelInvocationTransition::RecordResponse { response },
            )
            .await
            .unwrap();
        bindings = NodeInvocationBindings::try_new(
            &activation,
            [NodeInvocationBinding::from_model(committed.invocation()).unwrap()],
        )
        .unwrap();
        head = committed.event().head();
    }
    let intent =
        PendingNodeResultIntent::new(activation, NodeStateChange::Unchanged, control, bindings)
            .unwrap();
    Box::pin(store.succeed_node_attempt(
        worker_append(
            fence.tenant_id().clone(),
            fence.run_id(),
            EventId::generate(),
            JournalExpectation::exact(head),
            fence.clone(),
            30_004,
        ),
        &start.attempt().start().head(),
        intent.clone(),
        usage.clone(),
    ))
    .await
    .unwrap();
    let result = store
        .load_pending_node_result(intent.activation())
        .await
        .unwrap();
    (result, usage)
}
fn planned(
    graph: &CompiledGraph,
    base: &Checkpoint,
    result: &PendingNodeResult,
) -> GraphBarrierPlan {
    graph
        .plan_barrier(
            base,
            std::slice::from_ref(result),
            CheckpointId::generate(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone(),
            },
        )
        .unwrap()
}
async fn prepared(
    store: &PostgresStore,
    graph: &CompiledGraph,
    plan: GraphBarrierPlan,
    fence: &RunFence,
    terminal: Option<AgentResult>,
) -> (JournalAppend, RootGraphBarrierIntent) {
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    let event = EventId::generate();
    let intent =
        RootGraphBarrierIntent::new(plan, run.lifecycle().clone(), event, terminal).unwrap();
    let append = JournalAppend::new(
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        JournalEventIntent::worker(
            fence.tenant_id().clone(),
            fence.run_id(),
            event,
            fence.clone(),
            intent.payload().unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        intent.plan().barrier().base_checkpoint().graph(),
        &graph.reference()
    );
    (append, intent)
}
async fn commit(
    store: &PostgresStore,
    graph: &CompiledGraph,
    append: JournalAppend,
    intent: RootGraphBarrierIntent,
) -> StoredRootGraphBarrier {
    match Box::pin(store.append_worker_graph_barrier(
        append,
        intent,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap()
    {
        RootGraphBarrierCommitOutcome::Committed(record) => record,
        _ => panic!("fresh whole Root barrier"),
    }
}
fn interrupt(id: InterruptId) -> NodeWait {
    NodeWait::interrupt(
        id,
        RunInterruptKind::Approval,
        payload(30_010),
        Digest::sha256(b"Root barrier action"),
        None,
        ScopeSet::empty(),
        None,
    )
}
async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap()
}
async fn snapshot(db: &PgPool, fence: &RunFence) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    for table in [
        "runs",
        "run_events",
        "run_checkpoints",
        "root_graph_barriers",
        "pending_node_results",
        "pending_node_result_consumptions",
        "run_wait_registrations",
        "node_attempts",
        "node_attempt_completions",
    ] {
        let raw:String=query_scalar(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text),'[]'::jsonb)::text FROM stateknot.{table} x WHERE tenant_id=$1 AND run_id=$2")).bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(db).await.unwrap();
        rows.push(serde_json::from_str(&raw).unwrap());
    }
    rows
}

#[tokio::test]
async fn root_continue_retains_original_whole_inputs_and_recovers_before_callbacks() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-continue")).await;
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Continue,
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let (append, intent) = Box::pin(prepared(&store, &graph, plan.clone(), &fence, None)).await;
    let saved = Box::pin(commit(&store, &graph, append.clone(), intent.clone())).await;
    assert_eq!(saved.barrier(), plan.barrier());
    assert_eq!(saved.disposition(), plan.disposition());
    assert_eq!(saved.source_journal(), append.expectation().head().unwrap());
    assert_eq!(
        serde_json_canonicalizer::to_vec(saved.original_lifecycle()).unwrap(),
        serde_json_canonicalizer::to_vec(saved.committed_lifecycle()).unwrap()
    );
    assert!(saved.waits().is_empty());
    assert_ne!(saved.digest(), saved.original_projection_digest());
    let retry = Box::pin(store.append_worker_graph_barrier(
        append.clone(),
        intent.clone(),
        &Unavailable,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap();
    assert!(matches!(
        retry,
        RootGraphBarrierCommitOutcome::Idempotent(_)
    ));
    store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap();
    assert!(
        store
            .recover_worker_graph_barrier(&append, &intent)
            .await
            .unwrap()
            .is_some()
    );
    let loaded = store
        .load_root_graph_barrier(&admission.checkpoint().head())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.digest(), saved.digest());
    let changed = JournalAppend::new(
        JournalExpectation::exact(saved.event().head()),
        append.intent().clone(),
    )
    .unwrap();
    assert!(matches!(
        store.recover_worker_graph_barrier(&changed, &intent).await,
        Err(StoreError::ProjectionIntentConflict)
    ));
    store.verify_schema().await.unwrap();
    store.close().await;
}

#[tokio::test]
async fn root_wait_retains_all_sixty_four_registrations_and_original_revision() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-wait")).await;
    let waits =
        NodeWaits::try_new((0..NodeWaits::MAX_LEN).map(|_| interrupt(InterruptId::generate())))
            .unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait {
            waits: waits.clone(),
        },
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    assert!(matches!(
        plan.disposition(),
        GraphBarrierDisposition::Wait { .. }
    ));
    let (append, intent) = Box::pin(prepared(&store, &graph, plan.clone(), &fence, None)).await;
    let saved = Box::pin(commit(&store, &graph, append.clone(), intent.clone())).await;
    assert_eq!(saved.waits().len(), NodeWaits::MAX_LEN);
    assert_eq!(saved.disposition(), plan.disposition());
    assert_eq!(saved.committed_lifecycle().status(), RunStatus::Waiting);
    assert_eq!(
        saved.committed_lifecycle().revision().get(),
        saved.original_lifecycle().revision().get() + 1
    );
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(current.unresolved_wait_count(), 64);
    assert!(current.lease().is_none());
    assert_eq!(
        store
            .load_root_graph_barrier(&admission.checkpoint().head())
            .await
            .unwrap()
            .unwrap()
            .digest(),
        saved.digest()
    );
    assert!(matches!(
        Box::pin(store.append_worker_graph_barrier(
            append,
            intent,
            &Unavailable,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await
        .unwrap(),
        RootGraphBarrierCommitOutcome::Idempotent(_)
    ));
    store.verify_schema().await.unwrap();
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn root_terminal_preserves_true_model_evidence_and_full_agent_result() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-terminal")).await;
    let (first, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Continue,
        false,
    ))
    .await;
    let first_plan = planned(&graph, admission.checkpoint(), &first);
    let (append, intent) = Box::pin(prepared(&store, &graph, first_plan, &fence, None)).await;
    let original_continue_append = append.clone();
    let original_continue_intent = intent.clone();
    let continued = Box::pin(commit(&store, &graph, append, intent)).await;
    let output = NodeTerminalOutput::new(
        graph.output_schema().clone(),
        BoundedJson::try_from(json!({"summary":"Actual Root output"})).unwrap(),
    )
    .unwrap();
    let (last, model_usage) = Box::pin(succeed(
        &store,
        continued.checkpoint(),
        &fence,
        NodeControl::Terminal {
            output: output.clone(),
        },
        true,
    ))
    .await;
    let db = pool().await;
    let result = Box::pin(terminal_result(
        &store,
        &admission,
        &last,
        &output,
        &model_usage,
    ))
    .await;
    let plan = planned(&graph, continued.checkpoint(), &last);
    let (append, intent) = Box::pin(prepared(
        &store,
        &graph,
        plan.clone(),
        &fence,
        Some(result.clone()),
    ))
    .await;
    let saved = Box::pin(commit(&store, &graph, append.clone(), intent.clone())).await;
    assert_eq!(saved.disposition(), plan.disposition());
    assert_eq!(saved.committed_lifecycle().result(), Some(&result));
    assert_eq!(result.usage().model_turns().get(), 1);
    assert!(!result.usage().known_costs().is_empty());
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(current.lifecycle().result(), Some(&result));
    assert!(current.lease().is_none());
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *saved.checkpoint()
    );
    assert_eq!(
        store
            .load_root_graph_barrier(&continued.checkpoint().head())
            .await
            .unwrap()
            .unwrap()
            .committed_lifecycle()
            .result(),
        Some(&result)
    );
    assert!(matches!(
        Box::pin(store.append_worker_graph_barrier(
            append,
            intent,
            &Unavailable,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await
        .unwrap(),
        RootGraphBarrierCommitOutcome::Idempotent(_)
    ));
    assert!(matches!(
        Box::pin(store.append_worker_graph_barrier(
            original_continue_append,
            original_continue_intent,
            &Unavailable,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await
        .unwrap(),
        RootGraphBarrierCommitOutcome::Idempotent(_)
    ));
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn root_wait_checks_original_live_fence_after_slow_deferred_guards() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-expiry")).await;
    let waits = NodeWaits::try_new([interrupt(InterruptId::generate())]).unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let next = store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let (append, intent) = Box::pin(prepared(&store, &graph, plan, &next, None)).await;
    let db = pool().await;
    let before = snapshot(&db, &next).await;
    query("CREATE FUNCTION stateknot.test_root_barrier_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.1); RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER test_root_barrier_delay AFTER INSERT ON stateknot.root_graph_barriers DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.test_root_barrier_delay()").execute(&db).await.unwrap();
    let result = Box::pin(store.append_worker_graph_barrier(
        append,
        intent,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await;
    query("DROP TRIGGER test_root_barrier_delay ON stateknot.root_graph_barriers")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_root_barrier_delay()")
        .execute(&db)
        .await
        .unwrap();
    let after = snapshot(&db, &next).await;
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
    assert!(
        matches!(result, Err(StoreError::LeaseExpired)),
        "{result:?}"
    );
    assert_eq!(before, after);
}

async fn terminal_result(
    store: &PostgresStore,
    admission: &StoredAgentAdmission,
    last: &PendingNodeResult,
    output: &NodeTerminalOutput,
    model_usage: &BudgetUsage,
) -> AgentResult {
    let source_owner = admission.admission().intent().provenance();
    let db = pool().await;
    let journal = store
        .load_journal_page(
            source_owner.tenant_id(),
            source_owner.run_id(),
            None,
            JournalPageSize::new(128).unwrap(),
        )
        .await
        .unwrap();
    assert!(!journal.has_more());
    let event_bytes: u64 = journal
        .events()
        .iter()
        .map(|event| serde_json_canonicalizer::to_vec(event).unwrap().len() as u64)
        .sum();
    let checkpoint_bytes:i64=query_scalar("SELECT sum(octet_length(checkpoint_bytes))::bigint FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2").bind(source_owner.tenant_id().as_str()).bind(*source_owner.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    let source = admission.admission().intent();
    let usage = BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(1))
        .graph_steps(ExecutionCount::new(2))
        .model_attempts(model_usage.model_attempts())
        .model_turns(model_usage.model_turns())
        .input_tokens(model_usage.input_tokens())
        .cached_input_tokens(model_usage.cached_input_tokens())
        .output_tokens(model_usage.output_tokens())
        .reasoning_tokens(model_usage.reasoning_tokens())
        .known_costs(model_usage.known_costs().clone())
        .input_bytes(ByteCount::new(
            source.request().input().stats().compact_bytes() as u64,
        ))
        .output_bytes(ByteCount::new(output.data().stats().compact_bytes() as u64))
        .event_bytes(ByteCount::new(event_bytes))
        .checkpoint_bytes(ByteCount::new(u64::try_from(checkpoint_bytes).unwrap()))
        .build()
        .unwrap();
    let result = AgentResult::new(
        source.provenance().clone(),
        last.journal_head().recorded_at(),
        output.schema().clone(),
        output.data().clone(),
        AgentArtifacts::empty(),
        usage,
    )
    .unwrap();
    result
        .validate_for(
            source.provenance(),
            source.request(),
            source.descriptor(),
            source.budget(),
        )
        .unwrap();
    db.close().await;
    result
}
async fn replace_bundle(db: &PgPool, base: &CheckpointHead, bytes: &[u8]) {
    let mut tx = db.begin().await.unwrap();
    query("SET LOCAL session_replication_role=replica")
        .execute(&mut *tx)
        .await
        .unwrap();
    let changed=query("UPDATE stateknot.root_graph_barriers SET bundle_bytes=$4 WHERE tenant_id=$1 AND run_id=$2 AND base_checkpoint_id=$3").bind(base.tenant_id().as_str()).bind(*base.run_id().as_uuid()).bind(*base.checkpoint_id().as_uuid()).bind(bytes).execute(&mut *tx).await.unwrap().rows_affected();
    tx.commit().await.unwrap();
    assert_eq!(changed, 1);
}
const BUNDLE_INSERT_COLUMNS: &str = "tenant_id,run_id,base_checkpoint_id,base_superstep,base_checkpoint_digest,successor_checkpoint_id,successor_superstep,successor_checkpoint_digest,result_count,result_heads_digest,barrier_intent_digest,disposition_kind,disposition_digest,original_projection_digest,lifecycle_before_revision,lifecycle_after_revision,lifecycle_before_bytes,lifecycle_after_bytes,intent_digest,compound_digest,bundle_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest";
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn root_missing_bundle_rejects_public_batch_cursor_and_claimed_owner_reads() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-missing")).await;
    let (first, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Continue,
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &first);
    let (append, intent) = Box::pin(prepared(&store, &graph, plan, &fence, None)).await;
    let saved = Box::pin(commit(&store, &graph, append, intent)).await;
    let db = pool().await;
    query("CREATE TEMP TABLE saved_root_bundle AS SELECT * FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.unwrap();
    let mut tx = db.begin().await.unwrap();
    query("SET LOCAL session_replication_role=replica")
        .execute(&mut *tx)
        .await
        .unwrap();
    let deleted =
        query("DELETE FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2")
            .bind(fence.tenant_id().as_str())
            .bind(*fence.run_id().as_uuid())
            .execute(&mut *tx)
            .await
            .unwrap()
            .rows_affected();
    tx.commit().await.unwrap();
    assert_eq!(deleted, 1);
    let direct = store
        .load_root_graph_barrier(&admission.checkpoint().head())
        .await;
    let current = store
        .load_current_checkpoint(fence.tenant_id(), fence.run_id())
        .await;
    let run = store.load_run(fence.tenant_id(), fence.run_id()).await;
    let admission_read = store
        .load_agent_admission(fence.tenant_id(), fence.run_id())
        .await;
    let lineage = store
        .load_checkpoint_lineage_page(
            fence.tenant_id(),
            fence.run_id(),
            None,
            CheckpointLineagePageSize::new(2).unwrap(),
        )
        .await;
    let cursor = store
        .load_journal_page(
            fence.tenant_id(),
            fence.run_id(),
            Some(&saved.event().head()),
            JournalPageSize::new(1).unwrap(),
        )
        .await;
    let empty = store
        .load_unconsumed_pending_node_result_page(
            &saved.checkpoint().head(),
            None,
            PendingNodeResultPageSize::new(1).unwrap(),
        )
        .await;
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(saved.event().head()),
        Digest::sha256(b"Root missing bundle actual fixture"),
    )
    .unwrap();
    let claimed = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await;
    // The committed fault is always repaired before checking any rejected result.
    let mut tx = db.begin().await.unwrap();
    query("SET LOCAL session_replication_role=replica")
        .execute(&mut *tx)
        .await
        .unwrap();
    query(&format!("INSERT INTO stateknot.root_graph_barriers ({BUNDLE_INSERT_COLUMNS}) SELECT {BUNDLE_INSERT_COLUMNS} FROM saved_root_bundle")).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    query("DROP TABLE saved_root_bundle")
        .execute(&db)
        .await
        .unwrap();
    let quarantined = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
    assert!(matches!(direct, Err(StoreError::CorruptData { .. })));
    assert!(matches!(current, Err(StoreError::CorruptData { .. })));
    assert!(matches!(run, Err(StoreError::CorruptData { .. })));
    assert!(matches!(
        admission_read,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(lineage, Err(StoreError::CorruptData { .. })));
    assert!(matches!(cursor, Err(StoreError::CorruptData { .. })));
    assert!(matches!(empty, Err(StoreError::CorruptData { .. })));
    assert!(matches!(claimed, Err(StoreError::RunQuarantined)));
    assert!(quarantined.is_quarantined());
    assert!(quarantined.lease().is_none());
    assert_eq!(quarantined.journal_head(), Some(&saved.event().head()));
}

#[tokio::test]
async fn root_durable_wire_contradictions_are_corrupt_and_sixty_fifth_reference_is_rejected() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-wire")).await;
    let waits = NodeWaits::try_new((0..64).map(|_| interrupt(InterruptId::generate()))).unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let (append, intent) = Box::pin(prepared(&store, &graph, plan, &fence, None)).await;
    let saved = Box::pin(commit(&store, &graph, append, intent)).await;
    let db = pool().await;
    let original: Vec<u8> = query_scalar(
        "SELECT bundle_bytes FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    let original_wire: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let mut cases = Vec::new();
    let mut crossed = original_wire.clone();
    crossed["intent"]["base"]["checkpoint_id"] = json!(CheckpointId::generate());
    cases.push(serde_json_canonicalizer::to_vec(&crossed).unwrap());
    let mut inactive = original_wire.clone();
    inactive["intent"]["lifecycle_before"] =
        serde_json::to_value(saved.committed_lifecycle()).unwrap();
    cases.push(serde_json_canonicalizer::to_vec(&inactive).unwrap());
    let mut refs = original_wire.clone();
    let first = refs["intent"]["disposition"]["registrations"][0].clone();
    refs["intent"]["disposition"]["registrations"]
        .as_array_mut()
        .unwrap()
        .push(first);
    cases.push(serde_json_canonicalizer::to_vec(&refs).unwrap());
    let mut unknown = original_wire.clone();
    unknown["intent"]["unexpected"] = json!(true);
    cases.push(serde_json_canonicalizer::to_vec(&unknown).unwrap());
    let mut noncanonical = original.clone();
    noncanonical.push(b' ');
    cases.push(noncanonical);
    let mut duplicate = original.clone();
    duplicate.pop();
    duplicate.extend_from_slice(b",\"version\":1}");
    cases.push(duplicate);
    for changed in cases {
        replace_bundle(&db, &admission.checkpoint().head(), &changed).await;
        let direct = store
            .load_root_graph_barrier(&admission.checkpoint().head())
            .await;
        let run = store.load_run(fence.tenant_id(), fence.run_id()).await;
        replace_bundle(&db, &admission.checkpoint().head(), &original).await;
        assert!(
            matches!(direct, Err(StoreError::CorruptData { .. })),
            "{direct:?}"
        );
        assert!(
            matches!(run, Err(StoreError::CorruptData { .. })),
            "{run:?}"
        );
    }
    assert_eq!(
        store
            .load_root_graph_barrier(&admission.checkpoint().head())
            .await
            .unwrap()
            .unwrap()
            .digest(),
        saved.digest()
    );
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn root_timer_resume_keeps_original_wait_bundle_and_complete_replay() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-resume")).await;
    let timer_id = TimerId::generate();
    let interrupt_id = InterruptId::generate();
    let waits = NodeWaits::try_new([
        interrupt(interrupt_id),
        NodeWait::timer(
            timer_id,
            RunTimerKind::Sleep,
            future_timer_due(&store).await,
        ),
    ])
    .unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let (append, intent) = Box::pin(prepared(&store, &graph, plan, &fence, None)).await;
    let saved = Box::pin(commit(&store, &graph, append.clone(), intent.clone())).await;
    let request = store
        .load_interrupt_request(fence.tenant_id(), fence.run_id(), interrupt_id)
        .await
        .unwrap();
    let resolution_id = EventId::generate();
    let resolver = InterruptResolver::new(
        PrincipalIdentity::new(
            "https://issuer.example.com/root-barrier".parse().unwrap(),
            "approver".parse().unwrap(),
        ),
        ScopeSet::empty(),
    );
    let resolution = store
        .resolve_interrupt(
            control_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                resolution_id,
                JournalExpectation::exact(saved.event().head()),
                30_039,
            ),
            saved.committed_lifecycle().revision(),
            InterruptResolutionIntent::new(&request, resolution_id, payload(30_038), resolver)
                .unwrap(),
        )
        .await
        .unwrap();
    let partially_resolved = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(partially_resolved.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(partially_resolved.unresolved_wait_count(), 1);
    assert!(matches!(
        Box::pin(store.append_worker_graph_barrier(
            append.clone(),
            intent.clone(),
            &Unavailable,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await
        .unwrap(),
        RootGraphBarrierCommitOutcome::Idempotent(_)
    ));
    let timer = store
        .load_durable_timer(fence.tenant_id(), fence.run_id(), timer_id)
        .await
        .unwrap();
    wait_until_database_time(&store, timer.marker().due_at()).await;
    let event = EventId::generate();
    store
        .fire_timer(
            control_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                event,
                JournalExpectation::exact(resolution.event().head()),
                30_040,
            ),
            partially_resolved.lifecycle().revision(),
            TimerFiringIntent::new(&timer, event).unwrap(),
        )
        .await
        .unwrap();
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(current.lifecycle().status(), RunStatus::Active);
    let next = store
        .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(current.journal_head().unwrap().clone()),
        Digest::sha256(b"Root wait actual resolved timer"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(next, context)
        .await
        .unwrap();
    let report = Box::pin(recovery.validate_noninitial_replay(
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
        GraphReplayLimits::default(),
    ))
    .await
    .unwrap();
    assert_eq!(report.barriers_replayed(), 1);
    let loaded = store
        .load_root_graph_barrier(&admission.checkpoint().head())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded.digest(), saved.digest());
    assert_eq!(loaded.committed_lifecycle().status(), RunStatus::Waiting);
    assert!(matches!(
        Box::pin(store.append_worker_graph_barrier(
            append,
            intent,
            &Unavailable,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await
        .unwrap(),
        RootGraphBarrierCommitOutcome::Idempotent(_)
    ));
    assert!(
        !store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .is_quarantined()
    );
    store.verify_schema().await.unwrap();
    store.close().await;
}

#[tokio::test]
async fn root_wait_large_payloads_use_only_original_authenticated_registration_references() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-large-wait")).await;
    let large = JournalPayload::new(
        checkpoint_schema("large-wait-evidence"),
        "root-wait-evidence".parse().unwrap(),
        BoundedJson::try_from_value_with_limits(
            json!({"a":"x".repeat(1024*1024-1),"b":"y".repeat(1024*1024-4096)}),
            JsonLimits::MAXIMUM,
        )
        .unwrap(),
    )
    .unwrap();
    let waits = NodeWaits::try_new((0..64).map(|index| {
        NodeWait::interrupt(
            InterruptId::generate(),
            RunInterruptKind::Approval,
            if index < 3 {
                large.clone()
            } else {
                payload(30_045)
            },
            Digest::sha256(b"large Root wait evidence"),
            None,
            ScopeSet::empty(),
            None,
        )
    }))
    .unwrap();
    assert!(
        serde_json_canonicalizer::to_vec(&waits).unwrap().len()
            > 2 * JsonLimits::MAXIMUM.max_bytes()
    );
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let (append, intent) = Box::pin(prepared(&store, &graph, plan.clone(), &fence, None)).await;
    assert!(
        serde_json_canonicalizer::to_vec(append.intent().payload())
            .unwrap()
            .len()
            < 2048
    );
    let saved = Box::pin(commit(&store, &graph, append, intent)).await;
    let db = pool().await;
    let size:i32=query_scalar("SELECT octet_length(bundle_bytes) FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert!(size < 64 * 1024);
    assert_eq!(saved.disposition(), plan.disposition());
    assert_eq!(saved.waits().len(), 64);
    assert_eq!(
        store
            .load_root_graph_barrier(&admission.checkpoint().head())
            .await
            .unwrap()
            .unwrap()
            .disposition(),
        plan.disposition()
    );
    assert_eq!(
        store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unresolved_wait_count(),
        64
    );
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn root_writer_rejects_a_trigger_substituted_source_journal_before_commit() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-source-trigger")).await;
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Continue,
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let (append, intent) = Box::pin(prepared(&store, &graph, plan, &fence, None)).await;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    query("CREATE TABLE public.test_root_original_bundle (tenant_id text,run_id uuid,bundle_bytes bytea)").execute(&db).await.unwrap();
    query(r"CREATE FUNCTION stateknot.test_root_source_substitution() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
      INSERT INTO public.test_root_original_bundle VALUES(NEW.tenant_id,NEW.run_id,NEW.bundle_bytes);
      NEW.bundle_bytes:=convert_to(jsonb_set(convert_from(NEW.bundle_bytes,'UTF8')::jsonb,'{source_journal,digest}',to_jsonb('sha256:'||repeat('0',64)))::text,'UTF8');
      RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE TRIGGER test_root_source_substitution BEFORE INSERT ON stateknot.root_graph_barriers FOR EACH ROW EXECUTE FUNCTION stateknot.test_root_source_substitution()").execute(&db).await.unwrap();
    let outcome = Box::pin(store.append_worker_graph_barrier(
        append,
        intent,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await;
    query("DROP TRIGGER test_root_source_substitution ON stateknot.root_graph_barriers")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_root_source_substitution()")
        .execute(&db)
        .await
        .unwrap();
    let original:Option<Vec<u8>>=query_scalar("SELECT bundle_bytes FROM public.test_root_original_bundle WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_optional(&db).await.unwrap();
    // A red writer may have committed damaged bytes. Restore its complete original
    // bytes before asserting, so the shared native fixture remains usable.
    if let Some(bytes) = original.as_ref() {
        replace_bundle(&db, &admission.checkpoint().head(), bytes).await;
    }
    query("DROP TABLE public.test_root_original_bundle")
        .execute(&db)
        .await
        .unwrap();
    let after = snapshot(&db, &fence).await;
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
    assert!(
        matches!(
            outcome,
            Err(StoreError::Database { .. }
                | StoreError::CorruptData { .. }
                | StoreError::RootGraphBarrierCompoundRequired)
        ),
        "{outcome:?}"
    );
    assert_eq!(before, after);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn root_writer_rejects_substituted_whole_wire_fields_without_partial_commits() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let zero = json!(Digest::sha256(b"Root wire substitution"));
    for (kind, path, replacement) in [
        (
            "continue",
            "{intent,base,graph,definition_digest}",
            zero.clone(),
        ),
        (
            "continue",
            "{intent,base,journal_head,digest}",
            zero.clone(),
        ),
        ("continue", "{checkpoint,digest}", zero.clone()),
        ("continue", "{intent,successor_intent_digest}", zero.clone()),
        ("continue", "{intent,event_id}", json!(EventId::generate())),
        (
            "continue",
            "{intent,original_projection_digest}",
            zero.clone(),
        ),
        ("wait", "{intent,disposition,digest}", zero.clone()),
        ("terminal", "{intent,terminal_result_digest}", zero.clone()),
        ("terminal", "{intent,disposition,output_digest}", zero),
    ] {
        let (admission, graph, fence) =
            Box::pin(admitted(&store, "root-bundle-wire-trigger")).await;
        let mut base = admission.checkpoint().clone();
        if kind == "terminal" {
            let (first, _) =
                Box::pin(succeed(&store, &base, &fence, NodeControl::Continue, false)).await;
            let plan = planned(&graph, &base, &first);
            let (append, intent) = Box::pin(prepared(&store, &graph, plan, &fence, None)).await;
            base = Box::pin(commit(&store, &graph, append, intent))
                .await
                .checkpoint()
                .clone();
        }
        let output = NodeTerminalOutput::new(
            graph.output_schema().clone(),
            BoundedJson::try_from(json!({"summary":"Fault matrix real output"})).unwrap(),
        )
        .unwrap();
        let control = match kind {
            "wait" => NodeControl::Wait {
                waits: NodeWaits::try_new([interrupt(InterruptId::generate())]).unwrap(),
            },
            "terminal" => NodeControl::Terminal {
                output: output.clone(),
            },
            _ => NodeControl::Continue,
        };
        let (result, usage) =
            Box::pin(succeed(&store, &base, &fence, control, kind == "terminal")).await;
        let terminal = if kind == "terminal" {
            Some(
                Box::pin(terminal_result(
                    &store, &admission, &result, &output, &usage,
                ))
                .await,
            )
        } else {
            None
        };
        let plan = planned(&graph, &base, &result);
        let (append, intent) = Box::pin(prepared(&store, &graph, plan, &fence, terminal)).await;
        let db = pool().await;
        let before = snapshot(&db, &fence).await;
        install_canonical_mutation(&db).await;
        query("CREATE TABLE public.test_root_original_bundle (tenant_id text,run_id uuid,bundle_bytes bytea)").execute(&db).await.unwrap();
        let literal = serde_json::to_string(&replacement)
            .unwrap()
            .replace('\'', "''");
        query(&format!("CREATE FUNCTION stateknot.test_root_wire_substitution() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.test_root_original_bundle VALUES(NEW.tenant_id,NEW.run_id,NEW.bundle_bytes); IF convert_from(NEW.bundle_bytes,'UTF8')<>public.test_root_canonical(convert_from(NEW.bundle_bytes,'UTF8')::jsonb) THEN RAISE EXCEPTION 'fault fixture canonical mismatch'; END IF; NEW.bundle_bytes:=convert_to(public.test_root_canonical(jsonb_set(convert_from(NEW.bundle_bytes,'UTF8')::jsonb,'{path}','{literal}'::jsonb)),'UTF8'); RETURN NEW; END $$")).execute(&db).await.unwrap();
        query("CREATE TRIGGER test_root_wire_substitution BEFORE INSERT ON stateknot.root_graph_barriers FOR EACH ROW EXECUTE FUNCTION stateknot.test_root_wire_substitution()").execute(&db).await.unwrap();
        let outcome = Box::pin(store.append_worker_graph_barrier(
            append,
            intent,
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone(),
            },
        ))
        .await;
        query("DROP TRIGGER test_root_wire_substitution ON stateknot.root_graph_barriers")
            .execute(&db)
            .await
            .unwrap();
        query("DROP FUNCTION stateknot.test_root_wire_substitution()")
            .execute(&db)
            .await
            .unwrap();
        let original:Option<Vec<u8>>=query_scalar("SELECT bundle_bytes FROM public.test_root_original_bundle WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_optional(&db).await.unwrap();
        if let Some(bytes) = original.as_ref() {
            replace_bundle(&db, &base.head(), bytes).await;
        }
        query("DROP TABLE public.test_root_original_bundle")
            .execute(&db)
            .await
            .unwrap();
        query("DROP FUNCTION public.test_root_canonical(jsonb)")
            .execute(&db)
            .await
            .unwrap();
        let after = snapshot(&db, &fence).await;
        store.verify_schema().await.unwrap();
        db.close().await;
        assert!(
            matches!(
                outcome,
                Err(StoreError::Database { .. }
                    | StoreError::CorruptData { .. }
                    | StoreError::RootGraphBarrierCompoundRequired)
            ),
            "unexpected {kind} {path}: {outcome:?}"
        );
        assert_eq!(before, after, "partial {kind} {path}");
    }
    store.close().await;
}

#[tokio::test]
async fn root_late_wait_component_is_rejected_after_actual_timer_resume() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-late-wait")).await;
    let timer_id = TimerId::generate();
    let waits = NodeWaits::try_new([NodeWait::timer(
        timer_id,
        RunTimerKind::Sleep,
        future_timer_due(&store).await,
    )])
    .unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let (append, intent) = Box::pin(prepared(&store, &graph, plan, &fence, None)).await;
    let saved = Box::pin(commit(&store, &graph, append, intent)).await;
    let timer = store
        .load_durable_timer(fence.tenant_id(), fence.run_id(), timer_id)
        .await
        .unwrap();
    wait_until_database_time(&store, timer.marker().due_at()).await;
    let event = EventId::generate();
    store
        .fire_timer(
            control_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                event,
                JournalExpectation::exact(saved.event().head()),
                30_050,
            ),
            saved.committed_lifecycle().revision(),
            TimerFiringIntent::new(&timer, event).unwrap(),
        )
        .await
        .unwrap();
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    let columns:Vec<String>=query_scalar("SELECT attname::text FROM pg_attribute WHERE attrelid='stateknot.run_wait_registrations'::regclass AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum").fetch_all(&db).await.unwrap();
    let values: Vec<String> = columns
        .iter()
        .map(|name| match name.as_str() {
            "wait_id" => "$3::uuid".to_owned(),
            "status" => "'outstanding'".to_owned(),
            "resolution_digest"
            | "firing_digest"
            | "abandonment_digest"
            | "terminal_sequence"
            | "terminal_event_id"
            | "terminal_recorded_at"
            | "terminal_event_digest" => "NULL".to_owned(),
            _ => name.clone(),
        })
        .collect();
    let mut tx = db.begin().await.unwrap();
    let insertion=query(&format!("INSERT INTO stateknot.run_wait_registrations ({}) SELECT {} FROM stateknot.run_wait_registrations WHERE tenant_id=$1 AND run_id=$2 AND wait_id=$4",columns.join(","),values.join(","))).bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(*TimerId::generate().as_uuid()).bind(*timer_id.as_uuid()).execute(&mut *tx).await;
    assert!(
        insertion.is_ok(),
        "late row must reach its deferred Root guard: {insertion:?}"
    );
    let rejected = query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut *tx)
        .await;
    tx.rollback().await.unwrap();
    let after = snapshot(&db, &fence).await;
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
    assert!(
        matches!(rejected,Err(ref error) if error.as_database_error().and_then(sqlx_core::error::DatabaseError::code).as_deref()==Some("SKG02")),
        "{rejected:?}"
    );
    assert_eq!(before, after);
}

async fn install_canonical_mutation(db: &PgPool) {
    query(r#"CREATE FUNCTION public.test_root_canonical(data jsonb) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
    DECLARE value text;
    BEGIN
      CASE jsonb_typeof(data)
      WHEN 'object' THEN SELECT '{'||coalesce(string_agg(to_json(key)::text||':'||public.test_root_canonical(item),',' ORDER BY key COLLATE "C"),'')||'}' INTO value FROM jsonb_each(data) x(key,item);
      WHEN 'array' THEN SELECT '['||coalesce(string_agg(public.test_root_canonical(item),',' ORDER BY ordinal),'')||']' INTO value FROM jsonb_array_elements(data) WITH ORDINALITY x(item,ordinal);
      ELSE value:=data::text;
      END CASE;
      RETURN value;
    END $$"#).execute(db).await.unwrap();
}
async fn admitted_large_output(
    store: &PostgresStore,
) -> (StoredAgentAdmission, CompiledGraph, RunFence) {
    let graph = graph();
    let tenant = tenant("root-bundle-large-terminal");
    let run = RunId::generate();
    store
        .register_graph_definition(tenant.clone(), graph.clone())
        .await
        .unwrap();
    let (template, _, _) = agent_admission_fixture(tenant.clone(), run);
    let request = AgentRequest::new(
        template.request().input_schema().clone(),
        template.request().input().clone(),
        template
            .request()
            .budget_limits()
            .clone()
            .with_output_bytes(ByteCount::new(JsonLimits::MAXIMUM.max_bytes() as u64)),
    );
    let intent = AgentAdmissionIntent::new(
        template.provenance().clone(),
        template.descriptor().clone(),
        request,
        template.budget_layers().iter().cloned(),
        graph.reference(),
        template.authority().clone(),
    )
    .unwrap();
    let audit = JournalPayload::new(
        checkpoint_schema("large-terminal-admission"),
        AgentAdmission::JOURNAL_EVENT_KIND.parse().unwrap(),
        BoundedJson::try_from(json!({"intent_digest":intent.intent_digest()})).unwrap(),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(tenant.clone(), run, EventId::generate(), audit).unwrap(),
    )
    .unwrap();
    let write = CheckpointWrite::initial(
        tenant.clone(),
        run,
        CheckpointId::generate(),
        graph.reference(),
        CheckpointState::new(
            graph.state_schema().clone(),
            BoundedJson::try_from(json!({"count":7,"source":"parent"})).unwrap(),
        )
        .unwrap(),
        graph.entry_nodes().clone(),
    )
    .unwrap();
    let AgentAdmissionCommitOutcome::Committed(admission) =
        Box::pin(store.admit_agent_run(intent, append, write, &AcceptGraphSchemas))
            .await
            .unwrap()
    else {
        panic!("fresh large admission");
    };
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    (admission, graph, fence)
}
#[tokio::test]
async fn root_terminal_keeps_one_large_result_beyond_the_journal_payload_ceiling() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted_large_output(&store)).await;
    let (first, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Continue,
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &first);
    let (append, intent) = Box::pin(prepared(&store, &graph, plan, &fence, None)).await;
    let continued = Box::pin(commit(&store, &graph, append, intent)).await;
    let data = BoundedJson::try_from_value_with_limits(
        json!({"a":"x".repeat(1024*1024-1),"b":"y".repeat(1024*1024-4096)}),
        JsonLimits::MAXIMUM,
    )
    .unwrap();
    let output = NodeTerminalOutput::new(graph.output_schema().clone(), data).unwrap();
    let (last, usage) = Box::pin(succeed(
        &store,
        continued.checkpoint(),
        &fence,
        NodeControl::Terminal {
            output: output.clone(),
        },
        true,
    ))
    .await;
    let result = Box::pin(terminal_result(&store, &admission, &last, &output, &usage)).await;
    let plan = planned(&graph, continued.checkpoint(), &last);
    let (append, intent) =
        Box::pin(prepared(&store, &graph, plan, &fence, Some(result.clone()))).await;
    let saved = Box::pin(commit(&store, &graph, append.clone(), intent.clone())).await;
    let db = pool().await;
    let (life_size,bundle_size):(i32,i32)=query_as("SELECT octet_length(lifecycle_after_bytes),octet_length(bundle_bytes) FROM stateknot.root_graph_barriers WHERE tenant_id=$1 AND run_id=$2 AND disposition_kind='terminal'").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    let life_size = usize::try_from(life_size).unwrap();
    let bundle_size = usize::try_from(bundle_size).unwrap();
    assert!(life_size <= JsonLimits::MAXIMUM.max_bytes());
    assert!(life_size > JsonLimits::MAXIMUM.max_bytes() - 4096);
    assert!(bundle_size > JsonLimits::MAXIMUM.max_bytes());
    assert!(
        serde_json_canonicalizer::to_vec(append.intent().payload())
            .unwrap()
            .len()
            < 2048
    );
    let stored = store
        .load_root_graph_barrier(&continued.checkpoint().head())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.committed_lifecycle().result(), Some(&result));
    assert_eq!(stored.digest(), saved.digest());
    assert!(matches!(
        Box::pin(store.append_worker_graph_barrier(
            append,
            intent,
            &Unavailable,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await
        .unwrap(),
        RootGraphBarrierCommitOutcome::Idempotent(_)
    ));
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

async fn future_timer_due(store: &PostgresStore) -> Timestamp {
    let now = store.observe_database_clock().await.unwrap();
    Timestamp::from_unix_micros(now.unix_micros() + 5_000_000).unwrap()
}
async fn wait_until_database_time(store: &PostgresStore, due: Timestamp) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if store.observe_database_clock().await.unwrap() >= due {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("database clock reaches the unchanged timer deadline");
}

#[tokio::test]
async fn root_wait_ack_survives_cancellation_and_actual_abandonment() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-abandon")).await;
    let interrupt_id = InterruptId::generate();
    let waits = NodeWaits::try_new([interrupt(interrupt_id)]).unwrap();
    let (result, _) = Box::pin(succeed(
        &store,
        admission.checkpoint(),
        &fence,
        NodeControl::Wait { waits },
        false,
    ))
    .await;
    let plan = planned(&graph, admission.checkpoint(), &result);
    let (append, intent) = Box::pin(prepared(&store, &graph, plan, &fence, None)).await;
    let saved = Box::pin(commit(&store, &graph, append.clone(), intent.clone())).await;
    let event_id = EventId::generate();
    let abandoned = store
        .append_control_plane_abandon_waits(
            control_append(
                fence.tenant_id().clone(),
                fence.run_id(),
                event_id,
                JournalExpectation::exact(saved.event().head()),
                30_060,
            ),
            saved.committed_lifecycle().revision(),
            RunTransition::RequestCancellation {
                request: cancellation_request(saved.committed_lifecycle().changed_at()),
            },
        )
        .await
        .unwrap();
    assert_eq!(abandoned.abandonments().len(), 1);
    let current = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(
        current.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(current.unresolved_wait_count(), 0);
    let original = store
        .load_root_graph_barrier(&admission.checkpoint().head())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.digest(), saved.digest());
    assert_eq!(original.committed_lifecycle().status(), RunStatus::Waiting);
    assert!(matches!(
        Box::pin(store.append_worker_graph_barrier(
            append,
            intent,
            &Unavailable,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await
        .unwrap(),
        RootGraphBarrierCommitOutcome::Idempotent(_)
    ));
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    let columns: Vec<String> = query_scalar("SELECT attname::text FROM pg_attribute WHERE attrelid='stateknot.run_wait_registrations'::regclass AND attnum>0 AND NOT attisdropped AND attgenerated='' ORDER BY attnum").fetch_all(&db).await.unwrap();
    let values: Vec<String> = columns
        .iter()
        .map(|name| match name.as_str() {
            "wait_id" => "$3::uuid".to_owned(),
            "status" => "'outstanding'".to_owned(),
            "resolution_digest"
            | "firing_digest"
            | "abandonment_digest"
            | "terminal_sequence"
            | "terminal_event_id"
            | "terminal_recorded_at"
            | "terminal_event_digest" => "NULL".to_owned(),
            _ => name.clone(),
        })
        .collect();
    let mut tx = db.begin().await.unwrap();
    query(&format!("INSERT INTO stateknot.run_wait_registrations ({}) SELECT {} FROM stateknot.run_wait_registrations WHERE tenant_id=$1 AND run_id=$2 AND wait_id=$4", columns.join(","), values.join(","))).bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(*InterruptId::generate().as_uuid()).bind(*interrupt_id.as_uuid()).execute(&mut *tx).await.unwrap();
    let rejected = query("SET CONSTRAINTS ALL IMMEDIATE")
        .execute(&mut *tx)
        .await;
    tx.rollback().await.unwrap();
    let after = snapshot(&db, &fence).await;
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
    assert!(
        matches!(rejected, Err(ref error) if error.as_database_error().and_then(sqlx_core::error::DatabaseError::code).as_deref() == Some("SKG02")),
        "{rejected:?}"
    );
    assert_eq!(before, after);
}

#[tokio::test]
async fn root_fresh_commit_rejects_substituted_run_epoch_and_queue_age() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for (kind, change) in [
        (
            "continue",
            "NEW.scheduler_ready_at:=NEW.scheduler_ready_at-interval '1 second';",
        ),
        ("wait", "NEW.fencing_epoch:=NEW.fencing_epoch+1;"),
        ("terminal", "NEW.fencing_epoch:=NEW.fencing_epoch+1;"),
    ] {
        let (admission, graph, fence) = Box::pin(admitted(&store, "root-bundle-final-run")).await;
        let mut base = admission.checkpoint().clone();
        if kind == "terminal" {
            let (first, _) =
                Box::pin(succeed(&store, &base, &fence, NodeControl::Continue, false)).await;
            let (append, intent) = Box::pin(prepared(
                &store,
                &graph,
                planned(&graph, &base, &first),
                &fence,
                None,
            ))
            .await;
            base = Box::pin(commit(&store, &graph, append, intent))
                .await
                .checkpoint()
                .clone();
        }
        let output = NodeTerminalOutput::new(
            graph.output_schema().clone(),
            BoundedJson::try_from(json!({"summary":"final actual projection"})).unwrap(),
        )
        .unwrap();
        let control = match kind {
            "wait" => NodeControl::Wait {
                waits: NodeWaits::try_new([interrupt(InterruptId::generate())]).unwrap(),
            },
            "terminal" => NodeControl::Terminal {
                output: output.clone(),
            },
            _ => NodeControl::Continue,
        };
        let (result, usage) =
            Box::pin(succeed(&store, &base, &fence, control, kind == "terminal")).await;
        let terminal = if kind == "terminal" {
            Some(
                Box::pin(terminal_result(
                    &store, &admission, &result, &output, &usage,
                ))
                .await,
            )
        } else {
            None
        };
        let (append, intent) = Box::pin(prepared(
            &store,
            &graph,
            planned(&graph, &base, &result),
            &fence,
            terminal,
        ))
        .await;
        let db = pool().await;
        let before = snapshot(&db, &fence).await;
        query(&format!("CREATE FUNCTION stateknot.test_root_run_substitution() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT 1 FROM stateknot.root_graph_barriers b WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.journal_sequence=NEW.journal_sequence) THEN {change} END IF; RETURN NEW; END $$")).execute(&db).await.unwrap();
        query("CREATE TRIGGER test_root_run_substitution BEFORE UPDATE ON stateknot.runs FOR EACH ROW EXECUTE FUNCTION stateknot.test_root_run_substitution()").execute(&db).await.unwrap();
        let outcome = Box::pin(store.append_worker_graph_barrier(
            append,
            intent,
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone(),
            },
        ))
        .await;
        query("DROP TRIGGER test_root_run_substitution ON stateknot.runs")
            .execute(&db)
            .await
            .unwrap();
        query("DROP FUNCTION stateknot.test_root_run_substitution()")
            .execute(&db)
            .await
            .unwrap();
        let after = snapshot(&db, &fence).await;
        store.verify_schema().await.unwrap();
        db.close().await;
        assert!(
            matches!(
                outcome,
                Err(StoreError::CorruptData { .. }
                    | StoreError::RootGraphBarrierCompoundRequired
                    | StoreError::Database { .. })
            ),
            "{kind} {outcome:?}"
        );
        assert_eq!(before, after, "{kind}");
    }
    store.close().await;
}
