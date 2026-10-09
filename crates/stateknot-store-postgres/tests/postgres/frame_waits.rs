// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Whole leaf suspensions, authenticated resolution and scoped continuation.
use super::frame_barriers::{commit, enter_graph, plan, pool, snapshot, succeed, terminal};
use super::*;
use serde_json::Value;
use stateknot_core::{GraphFrameBarrierPlan, NodeWait, NodeWaits, RunFence, RunRevision};
use stateknot_store_postgres::{GraphFrameBarrierCommitOutcome, StoredGraphFrameBarrier};

fn principal() -> PrincipalIdentity {
    PrincipalIdentity::new(
        "https://issuer.example.com/frame-waits".parse().unwrap(),
        "approver".parse().unwrap(),
    )
}
fn scopes() -> ScopeSet {
    ScopeSet::try_new(["run.resolve".parse::<Scope>().unwrap()]).unwrap()
}
fn interrupt(id: InterruptId) -> NodeWait {
    NodeWait::interrupt(
        id,
        RunInterruptKind::Approval,
        payload(25_000),
        Digest::sha256(b"scoped action"),
        Some(principal()),
        scopes(),
        None,
    )
}
async fn prepared(
    store: &PostgresStore,
    name: &str,
    waits: NodeWaits,
) -> (
    StoredAgentAdmission,
    stateknot_store_postgres::StoredGraphFrameEntry,
    CompiledGraph,
    RunFence,
    GraphFrameBarrierPlan,
    JournalHead,
    RunRevision,
) {
    let (_, child, _) = super::frame_transactions::graphs();
    let graph = CompiledGraph::compile(
        child.identity().clone(),
        child.input_schema().clone(),
        child.state_schema().clone(),
        child.update_schema().clone(),
        child.output_schema().clone(),
        child.reducer().clone(),
        ReadyNodes::try_new([NodeId::new("first").unwrap()]).unwrap(),
        [
            GraphNode::new(
                NodeId::new("first").unwrap(),
                None,
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
        child.limits(),
    )
    .unwrap();
    let (admission, entry, graph, fence) = Box::pin(enter_graph(store, name, graph, &[])).await;
    let result = Box::pin(succeed(
        store,
        entry.entry().checkpoint(),
        &fence,
        NodeControl::Wait { waits },
    ))
    .await;
    let prepared = plan(&graph, entry.entry().checkpoint(), &result);
    let run = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    (
        admission,
        entry,
        graph,
        fence,
        prepared,
        result.journal_head().clone(),
        run.lifecycle().revision(),
    )
}
#[allow(clippy::too_many_arguments)]
async fn suspend(
    store: &PostgresStore,
    plan: GraphFrameBarrierPlan,
    fence: &RunFence,
    observed: JournalHead,
    usage: BudgetUsage,
    revision: RunRevision,
    graph: &CompiledGraph,
) -> StoredGraphFrameBarrier {
    let GraphFrameBarrierCommitOutcome::Committed(record) =
        Box::pin(store.commit_graph_frame_wait(
            plan,
            EventId::generate(),
            fence.clone(),
            observed,
            usage,
            revision,
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone(),
            },
        ))
        .await
        .unwrap()
    else {
        panic!("fresh whole suspension");
    };
    record
}
struct NoSchemas;
impl GraphSchemaValidator for NoSchemas {
    fn validate(
        &self,
        _: &SchemaReference,
        _: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        panic!("whole suspension retry must precede callbacks")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn whole_leaf_wait_race_restarts_resolves_both_conditions_and_resumes_exact_scope() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    let now: i64 = query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(&db)
        .await
        .unwrap();
    let due = Timestamp::from_unix_micros(now + 30_000_000).unwrap();
    let interrupt_id = InterruptId::generate();
    let timer_id = TimerId::generate();
    let waits = NodeWaits::try_new([
        interrupt(interrupt_id),
        NodeWait::timer(timer_id, RunTimerKind::Sleep, due),
    ])
    .unwrap();
    let (admission, entry, graph, fence, prepared, observed, revision) =
        Box::pin(prepared(&store, "frame-wait-race", waits)).await;
    let usage = entry.direct_usage_after().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let (store, plan, fence, observed, usage, graph) = (
            store.clone(),
            prepared.clone(),
            fence.clone(),
            observed.clone(),
            usage.clone(),
            graph.clone(),
        );
        tasks.spawn(async move {
            let event = EventId::generate();
            for retry in 0..8 {
                let outcome = Box::pin(store.commit_graph_frame_wait(
                    plan.clone(),
                    event,
                    fence.clone(),
                    observed.clone(),
                    usage.clone(),
                    revision,
                    &AcceptGraphSchemas,
                    &IntegrationGraphReducer {
                        reference: graph.reducer().clone(),
                    },
                ))
                .await;
                if retry < 7 && outcome.as_ref().is_err_and(StoreError::is_retryable) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                return outcome;
            }
            unreachable!("bounded retry returns final outcome")
        });
    }
    let (mut committed, mut idempotent, mut saved) = (0, 0, None);
    while let Some(result) = tasks.join_next().await {
        match result.unwrap().unwrap() {
            GraphFrameBarrierCommitOutcome::Committed(record) => {
                committed += 1;
                saved = Some(record);
            }
            GraphFrameBarrierCommitOutcome::Idempotent(_) => idempotent += 1,
            _ => panic!("whole suspension outcome"),
        }
    }
    assert_eq!((committed, idempotent), (1, 23));
    let saved = saved.unwrap();
    assert_eq!(saved.wait_revision(), Some(revision));
    assert_eq!(saved.waits().unwrap().unwrap().len(), 2);
    let waiting = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(waiting.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(waiting.unresolved_wait_count(), 2);
    assert!(waiting.lease().is_none());
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    let fresh = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store.options().clone(),
    )
    .await
    .unwrap();
    let restored = Box::pin(fresh.load_graph_frame_checkpoint(
        fence.tenant_id(),
        fence.run_id(),
        saved.checkpoint().frame().namespace(),
        saved.checkpoint().checkpoint().checkpoint_id(),
    ))
    .await
    .unwrap();
    assert_eq!(restored, *saved.checkpoint());
    assert!(
        fresh
            .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
            .await
            .is_err()
    );
    let request = fresh
        .load_interrupt_request(fence.tenant_id(), fence.run_id(), interrupt_id)
        .await
        .unwrap();
    assert!(
        InterruptResolutionIntent::new(
            &request,
            EventId::generate(),
            payload(25_001),
            InterruptResolver::new(principal(), ScopeSet::default())
        )
        .is_err()
    );
    let resolution_id = EventId::generate();
    let intent = InterruptResolutionIntent::new(
        &request,
        resolution_id,
        payload(25_002),
        InterruptResolver::new(principal(), scopes()),
    )
    .unwrap();
    let resolution_append = control_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        resolution_id,
        JournalExpectation::exact(saved.event().head()),
        25_003,
    );
    let resolved = fresh
        .resolve_interrupt(
            resolution_append.clone(),
            waiting.lifecycle().revision(),
            intent.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        fresh
            .resolve_interrupt(resolution_append, waiting.lifecycle().revision(), intent)
            .await
            .unwrap()
            .record(),
        resolved.record()
    );
    let remaining = fresh
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(remaining.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(remaining.unresolved_wait_count(), 1);
    let timer = fresh
        .load_durable_timer(fence.tenant_id(), fence.run_id(), timer_id)
        .await
        .unwrap();
    let firing_id = EventId::generate();
    let firing = TimerFiringIntent::new(&timer, firing_id).unwrap();
    let append = control_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        firing_id,
        JournalExpectation::exact(resolved.event().head()),
        25_004,
    );
    let now: i64 = query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000000)::bigint")
        .fetch_one(&db)
        .await
        .unwrap();
    if now < due.unix_micros() {
        assert!(matches!(
            fresh
                .fire_timer(
                    append.clone(),
                    remaining.lifecycle().revision(),
                    firing.clone()
                )
                .await,
            Err(StoreError::InvalidTimerFiring)
        ));
        tokio::time::sleep(Duration::from_micros(
            u64::try_from(due.unix_micros() - now + 100_000).unwrap(),
        ))
        .await;
    }
    let fired = fresh
        .fire_timer(
            append.clone(),
            remaining.lifecycle().revision(),
            firing.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        fresh
            .fire_timer(append, remaining.lifecycle().revision(), firing)
            .await
            .unwrap()
            .record(),
        fired.record()
    );
    let resumed = fresh
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(resumed.lifecycle().status(), RunStatus::Active);
    assert_eq!(resumed.unresolved_wait_count(), 0);
    let newer = fresh
        .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    // A valid registration projection and terminal journal are insufficient:
    // full terminal bytes must authenticate before scoped node dispatch.
    let recovery = stateknot_core::ReadyNodeRecoveryPlanner::for_frame(
        saved.checkpoint().clone(),
        newer.clone(),
    )
    .unwrap()
    .finish(fired.event().head(), fired.event().recorded_at())
    .unwrap();
    for (table, column, field, replacement) in [
        (
            "interrupt_resolutions",
            "resolution_bytes",
            vec!["intent", "resolver", "principal", "subject"],
            json!("forged-approver"),
        ),
        (
            "timer_firings",
            "firing_bytes",
            vec!["intent", "firing_event_id"],
            json!(EventId::generate()),
        ),
    ] {
        let original: Vec<u8> = query_scalar(&format!(
            "SELECT {column} FROM stateknot.{table} WHERE tenant_id=$1 AND run_id=$2"
        ))
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .fetch_one(&db)
        .await
        .unwrap();
        let mut wire: Value = serde_json::from_slice(&original).unwrap();
        let mut slot = &mut wire;
        for key in field {
            slot = &mut slot[key];
        }
        assert!(
            !slot.is_null(),
            "terminal corruption must replace a real field"
        );
        *slot = replacement;
        let changed = serde_json_canonicalizer::to_vec(&wire).unwrap();
        query(&format!(
            "ALTER TABLE stateknot.{table} DISABLE TRIGGER USER"
        ))
        .execute(&db)
        .await
        .unwrap();
        query(&format!(
            "UPDATE stateknot.{table} SET {column}=$3 WHERE tenant_id=$1 AND run_id=$2"
        ))
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .bind(changed)
        .execute(&db)
        .await
        .unwrap();
        query(&format!(
            "ALTER TABLE stateknot.{table} ENABLE TRIGGER USER"
        ))
        .execute(&db)
        .await
        .unwrap();
        let before = waits_snapshot(&db, &newer).await;
        assert!(matches!(
            Box::pin(fresh.load_graph_frame_checkpoint(
                fence.tenant_id(),
                fence.run_id(),
                saved.checkpoint().frame().namespace(),
                saved.checkpoint().checkpoint().checkpoint_id(),
            ))
            .await,
            Err(StoreError::CorruptData { .. })
        ));
        assert!(matches!(
            Box::pin(fresh.start_recovered_graph_frame_node_attempt(
                worker_append(
                    fence.tenant_id().clone(),
                    fence.run_id(),
                    EventId::generate(),
                    JournalExpectation::exact(fired.event().head()),
                    newer.clone(),
                    25_005
                ),
                &recovery,
                &NodeId::new("finish").unwrap(),
                AttemptId::generate(),
            ))
            .await,
            Err(StoreError::CorruptData { .. })
        ));
        assert_eq!(waits_snapshot(&db, &newer).await, before);
        query(&format!(
            "ALTER TABLE stateknot.{table} DISABLE TRIGGER USER"
        ))
        .execute(&db)
        .await
        .unwrap();
        query(&format!(
            "UPDATE stateknot.{table} SET {column}=$3 WHERE tenant_id=$1 AND run_id=$2"
        ))
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .bind(original)
        .execute(&db)
        .await
        .unwrap();
        query(&format!(
            "ALTER TABLE stateknot.{table} ENABLE TRIGGER USER"
        ))
        .execute(&db)
        .await
        .unwrap();
        Box::pin(fresh.load_graph_frame_checkpoint(
            fence.tenant_id(),
            fence.run_id(),
            saved.checkpoint().frame().namespace(),
            saved.checkpoint().checkpoint().checkpoint_id(),
        ))
        .await
        .unwrap();
    }
    let retry = Box::pin(fresh.commit_graph_frame_wait(
        prepared,
        EventId::generate(),
        fence.clone(),
        observed,
        BudgetUsage::zero(),
        revision,
        &NoSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap();
    assert!(matches!(
        retry,
        GraphFrameBarrierCommitOutcome::Idempotent(_)
    ));
    let result = Box::pin(succeed(
        &fresh,
        saved.checkpoint(),
        &newer,
        terminal(&graph),
    ))
    .await;
    assert_eq!(
        result.intent().activation().graph_namespace(),
        saved.checkpoint().frame().namespace()
    );
    let final_plan = plan(&graph, saved.checkpoint(), &result);
    let finished = Box::pin(commit(
        &fresh,
        final_plan.clone(),
        &newer,
        saved.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    assert_eq!(finished.checkpoint().checkpoint().superstep().get(), 2);
    let binding = Box::pin(fresh.rebind_graph_frame_caller(
        finished.checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        newer.clone(),
        finished.event().head(),
        finished.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    let usage = super::frame_callers::charged(&finished.direct_usage_after().unwrap(), &binding);
    let returned = Box::pin(fresh.return_graph_frame(
        final_plan,
        EventId::generate(),
        newer,
        binding.event().head(),
        usage,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap();
    assert!(matches!(
        returned,
        stateknot_store_postgres::GraphFrameReturnCommitOutcome::Committed(_)
    ));
    let active: String = query_scalar("SELECT active_namespace FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert!(active.is_empty());
    assert_eq!(
        fresh
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    assert_eq!(
        fresh
            .load_interrupt_request(fence.tenant_id(), fence.run_id(), interrupt_id)
            .await
            .unwrap(),
        request
    );
    fresh.close().await;
    store.close().await;
}

async fn waits_snapshot(
    db: &PgPool,
    fence: &RunFence,
) -> ((Vec<i64>, String, String), Option<String>) {
    (snapshot(db,fence).await,query_scalar("SELECT jsonb_agg(to_jsonb(w) ORDER BY wait_id)::text FROM stateknot.run_wait_registrations w WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(db).await.unwrap())
}
#[tokio::test]
async fn every_scoped_wait_component_failure_rolls_back_the_whole_suspension() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let waits = NodeWaits::try_new([interrupt(InterruptId::generate())]).unwrap();
    let (_, entry, graph, fence, plan, observed, revision) =
        Box::pin(prepared(&store, "frame-wait-faults", waits)).await;
    let db = pool().await;
    let before = waits_snapshot(&db, &fence).await;
    query("CREATE FUNCTION stateknot.test_frame_wait_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected scoped wait component fault'; END $$").execute(&db).await.unwrap();
    for (table, action) in [
        ("run_events", "INSERT"),
        ("run_checkpoints", "INSERT"),
        ("pending_node_result_consumptions", "INSERT"),
        ("graph_frame_barriers", "INSERT"),
        ("graph_frame_heads", "UPDATE"),
        ("run_wait_registrations", "INSERT"),
        ("runs", "UPDATE"),
    ] {
        query(&format!("CREATE TRIGGER test_frame_wait_fault BEFORE {action} ON stateknot.{table} FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_wait_fault()" )).execute(&db).await.unwrap();
        let outcome = Box::pin(store.commit_graph_frame_wait(
            plan.clone(),
            EventId::generate(),
            fence.clone(),
            observed.clone(),
            entry.direct_usage_after().unwrap(),
            revision,
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone(),
            },
        ))
        .await;
        query(&format!(
            "DROP TRIGGER test_frame_wait_fault ON stateknot.{table}"
        ))
        .execute(&db)
        .await
        .unwrap();
        assert!(outcome.is_err(), "fault must hit {table}");
        assert_eq!(
            waits_snapshot(&db, &fence).await,
            before,
            "partial wait fact after {table}"
        );
    }
    query("DROP FUNCTION stateknot.test_frame_wait_fault()")
        .execute(&db)
        .await
        .unwrap();
    Box::pin(suspend(
        &store,
        plan,
        &fence,
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &graph,
    ))
    .await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_wait_policy_substitution_fails_closed_in_all_whole_readers() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let id = InterruptId::generate();
    let waits = NodeWaits::try_new([interrupt(id)]).unwrap();
    let (_, entry, graph, fence, prepared, observed, revision) =
        Box::pin(prepared(&store, "frame-wait-corruption", waits)).await;
    let saved = Box::pin(suspend(
        &store,
        prepared,
        &fence,
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &graph,
    ))
    .await;
    let db = pool().await;
    // Neither a raw lifecycle reset nor a legacy wait can authorize the leaf.
    assert!(
        query(
            "UPDATE stateknot.runs SET lifecycle_status='active' WHERE tenant_id=$1 AND run_id=$2"
        )
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .execute(&db)
        .await
        .is_err()
    );
    let original: Vec<u8> = query_scalar(
        "SELECT barrier_bytes FROM stateknot.graph_frame_barriers WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    let mut wire: Value = serde_json::from_slice(&original).unwrap();
    wire["disposition"]["waits"][0]["action_digest"] =
        json!(Digest::sha256(b"substituted scoped action").to_string());
    let changed = serde_json_canonicalizer::to_vec(&wire).unwrap();
    query(
        "ALTER TABLE stateknot.graph_frame_barriers DISABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    query("UPDATE stateknot.graph_frame_barriers SET barrier_bytes=$3 WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(&changed).execute(&db).await.unwrap();
    query(
        "ALTER TABLE stateknot.graph_frame_barriers ENABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    assert!(
        Box::pin(store.load_graph_frame_barrier(
            fence.tenant_id(),
            fence.run_id(),
            saved.checkpoint().frame().namespace(),
            entry.entry().checkpoint().checkpoint().checkpoint_id()
        ))
        .await
        .is_err()
    );
    assert!(
        Box::pin(store.load_graph_frame_checkpoint(
            fence.tenant_id(),
            fence.run_id(),
            saved.checkpoint().frame().namespace(),
            saved.checkpoint().checkpoint().checkpoint_id()
        ))
        .await
        .is_err()
    );
    assert!(
        store
            .load_interrupt_request(fence.tenant_id(), fence.run_id(), id)
            .await
            .is_err()
    );
    query(
        "ALTER TABLE stateknot.graph_frame_barriers DISABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    query("UPDATE stateknot.graph_frame_barriers SET barrier_bytes=$3 WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(&original).execute(&db).await.unwrap();
    query(
        "ALTER TABLE stateknot.graph_frame_barriers ENABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    assert_eq!(
        Box::pin(store.load_graph_frame_barrier(
            fence.tenant_id(),
            fence.run_id(),
            saved.checkpoint().frame().namespace(),
            entry.entry().checkpoint().checkpoint().checkpoint_id()
        ))
        .await
        .unwrap()
        .unwrap()
        .digest(),
        saved.digest()
    );
    store
        .load_interrupt_request(fence.tenant_id(), fence.run_id(), id)
        .await
        .unwrap();
    // Malformed suspension bytes must not prevent fail-stop isolation. The
    // audit lives outside the potentially damaged journal and preserves every
    // lifecycle, wait, checkpoint and journal fact.
    let before = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    query(
        "ALTER TABLE stateknot.graph_frame_barriers DISABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    query("UPDATE stateknot.graph_frame_barriers SET barrier_bytes=$3 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(b"not-json".as_slice()).execute(&db).await.unwrap();
    query(
        "ALTER TABLE stateknot.graph_frame_barriers ENABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    let context = CorruptionQuarantineContext::new(
        fence.tenant_id().clone(),
        fence.run_id(),
        QuarantineId::generate(),
        JournalExpectation::exact(saved.event().head()),
        Digest::sha256(b"redacted scoped recovery failure"),
    )
    .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            store
                .with_corruption_quarantine(
                    context.clone(),
                    Box::pin(store.load_graph_frame_checkpoint(
                        fence.tenant_id(),
                        fence.run_id(),
                        saved.checkpoint().frame().namespace(),
                        saved.checkpoint().checkpoint().checkpoint_id()
                    )),
                )
                .await,
            Err(StoreError::RunQuarantined)
        ));
    }
    let quarantine = store
        .load_run_quarantine(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(
        quarantine.request().expectation(),
        &JournalExpectation::exact(saved.event().head())
    );
    query(
        "ALTER TABLE stateknot.graph_frame_barriers DISABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    query("UPDATE stateknot.graph_frame_barriers SET barrier_bytes=$3 WHERE tenant_id=$1 AND run_id=$2")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(original).execute(&db).await.unwrap();
    query(
        "ALTER TABLE stateknot.graph_frame_barriers ENABLE TRIGGER graph_frame_barriers_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    let isolated = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert!(isolated.is_quarantined());
    assert_eq!(
        serde_json_canonicalizer::to_vec(isolated.lifecycle()).unwrap(),
        serde_json_canonicalizer::to_vec(before.lifecycle()).unwrap(),
    );
    assert_eq!(isolated.checkpoint(), before.checkpoint());
    assert_eq!(isolated.journal_head(), before.journal_head());
    assert_eq!(
        isolated.unresolved_wait_count(),
        before.unresolved_wait_count()
    );
    assert!(isolated.lease().is_none());
    assert!(matches!(
        store
            .claim_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
            .await,
        Err(StoreError::RunQuarantined)
    ));
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn scoped_wait_rechecks_the_original_lease_after_release_and_slow_deferred_guards() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let waits = NodeWaits::try_new([interrupt(InterruptId::generate())]).unwrap();
    let (_, entry, graph, fence, prepared, observed, revision) =
        Box::pin(prepared(&store, "frame-wait-late-lease", waits)).await;
    let db = pool().await;
    query("CREATE FUNCTION stateknot.test_frame_wait_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.1); RETURN NEW; END $$").execute(&db).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER test_frame_wait_delay AFTER INSERT ON stateknot.graph_frame_barriers DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_wait_delay()").execute(&db).await.unwrap();
    let next = store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let before = waits_snapshot(&db, &next).await;
    let outcome = Box::pin(store.commit_graph_frame_wait(
        prepared,
        EventId::generate(),
        next.clone(),
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await;
    query("DROP TRIGGER test_frame_wait_delay ON stateknot.graph_frame_barriers")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_frame_wait_delay()")
        .execute(&db)
        .await
        .unwrap();
    assert!(matches!(outcome, Err(StoreError::LeaseExpired)));
    assert_eq!(waits_snapshot(&db, &next).await, before);
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn source_schema_31_upgrade_preserves_nonempty_scoped_facts_and_commits_whole_wait() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let original = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_frame_wait_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url_with_name(&original, "postgres"))
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let url = database_url_with_name(&original, &name);
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(&url, options.clone()).await.unwrap();
    let waits = NodeWaits::try_new([interrupt(InterruptId::generate())]).unwrap();
    let (admission, entry, graph, fence, plan, observed, revision) =
        Box::pin(prepared(&store, "frame-wait-upgrade", waits)).await;
    // Retain a separate actual version-1 terminal barrier across the upgrade.
    let (_, prior_entry, _, prior_fence, _, prior_barrier) = Box::pin(
        super::frame_returns::finished(&store, "frame-wait-upgrade-v1"),
    )
    .await;
    store.close().await;
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let checksums: Vec<(i64, Vec<u8>)> = query_as(
        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=31 ORDER BY version",
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    let facts = waits_snapshot(&fixture, &fence).await;
    let prior_facts = snapshot(&fixture, &prior_fence).await;
    // This reconstructs a populated source-schema fixture; it is not a retained
    // historical executable or N-1/N-2 compatibility qualification.
    sqlx_core::raw_sql::raw_sql(include_str!("../fixtures/revert_graph_frame_waits.sql"))
        .execute(&fixture)
        .await
        .unwrap();
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        31
    );
    assert!(matches!(
        PostgresStore::connect(&url, options.clone()).await,
        Err(StoreError::IncompatibleSchema)
    ));
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let upgraded = PostgresStore::connect(&url, options).await.unwrap();
    assert_eq!(checksums, query_as::<_, (i64, Vec<u8>)>(
        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=31 ORDER BY version",
    ).fetch_all(&fixture).await.unwrap());
    assert_eq!(facts, waits_snapshot(&fixture, &fence).await);
    assert_eq!(prior_facts, snapshot(&fixture, &prior_fence).await);
    assert_eq!(
        Box::pin(upgraded.load_graph_frame_entry(
            fence.tenant_id(),
            fence.run_id(),
            entry.entry().checkpoint().frame().namespace()
        ))
        .await
        .unwrap()
        .digest(),
        entry.digest()
    );
    assert_eq!(
        Box::pin(
            upgraded.load_graph_frame_barrier(
                prior_fence.tenant_id(),
                prior_fence.run_id(),
                prior_barrier.checkpoint().frame().namespace(),
                prior_entry
                    .entry()
                    .checkpoint()
                    .checkpoint()
                    .checkpoint_id(),
            )
        )
        .await
        .unwrap()
        .unwrap()
        .digest(),
        prior_barrier.digest()
    );
    assert_eq!(
        upgraded
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    let saved = Box::pin(suspend(
        &upgraded,
        plan,
        &fence,
        observed,
        entry.direct_usage_after().unwrap(),
        revision,
        &graph,
    ))
    .await;
    assert_eq!(saved.wait_revision(), Some(revision));
    let run = upgraded
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(run.unresolved_wait_count(), 1);
    assert!(run.lease().is_none());
    // Downgrading after a version-2 suspension must not erase persisted facts.
    let mut tx = fixture.begin().await.unwrap();
    assert!(
        sqlx_core::raw_sql::raw_sql(include_str!("../fixtures/revert_graph_frame_waits.sql"))
            .execute(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    upgraded.verify_schema().await.unwrap();
    upgraded.close().await;
    fixture.close().await;
    query(&format!("DROP DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
