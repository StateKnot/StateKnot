// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real once-only child returns, parent continuation and whole recovery.
use super::frame_barriers::{commit, enter, plan, pool, succeed, terminal};
use super::frame_transactions::caller_graph;
use super::*;
use stateknot_core::{GraphFrameBarrierPlan, NodeAttemptStatus, PendingNodeResult, RunFence};
use stateknot_store_postgres::{GraphFrameReturnCommitOutcome, StoredGraphFrameReturn};

pub(super) async fn finished(
    store: &PostgresStore,
    name: &str,
) -> (
    StoredAgentAdmission,
    stateknot_store_postgres::StoredGraphFrameEntry,
    CompiledGraph,
    RunFence,
    GraphFrameBarrierPlan,
    stateknot_store_postgres::StoredGraphFrameBarrier,
) {
    let (admission, entry, graph, fence) = Box::pin(enter(store, name, false)).await;
    let result = Box::pin(succeed(
        store,
        entry.entry().checkpoint(),
        &fence,
        terminal(&graph),
    ))
    .await;
    let prepared = plan(&graph, entry.entry().checkpoint(), &result);
    let barrier = Box::pin(commit(
        store,
        prepared.clone(),
        &fence,
        entry.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    (admission, entry, graph, fence, prepared, barrier)
}
async fn returned(
    store: &PostgresStore,
    plan: GraphFrameBarrierPlan,
    fence: &RunFence,
    usage: BudgetUsage,
    graph: &CompiledGraph,
) -> StoredGraphFrameReturn {
    let observed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let GraphFrameReturnCommitOutcome::Committed(record) = Box::pin(store.return_graph_frame(
        plan,
        EventId::generate(),
        fence.clone(),
        observed,
        usage,
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap() else {
        panic!("fresh return");
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
        panic!("whole retry precedes callbacks");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn whole_return_race_recovers_once_and_parent_barrier_consumes_its_result() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, graph, fence, prepared, barrier) =
        Box::pin(finished(&store, "frame-return-race")).await;
    let observed = barrier.event().head();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let prepared = prepared.clone();
        let fence = fence.clone();
        let observed = observed.clone();
        let usage = barrier.direct_usage_after().unwrap();
        let reducer = IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        };
        tasks.spawn(async move {
            let event_id = EventId::generate();
            // Contention may hit the production lock deadline. Retry only
            // conservatively classified transaction errors, retaining every
            // original identity, observation and accounting input.
            for retry in 0..8 {
                let outcome = Box::pin(store.return_graph_frame(
                    prepared.clone(),
                    event_id,
                    fence.clone(),
                    observed.clone(),
                    usage.clone(),
                    &AcceptGraphSchemas,
                    &reducer,
                ))
                .await;
                if retry < 7 && outcome.as_ref().is_err_and(StoreError::is_retryable) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                return outcome;
            }
            unreachable!("bounded retry returns its final outcome")
        });
    }
    let (mut committed, mut idempotent) = (0, 0);
    let mut record = None;
    while let Some(outcome) = tasks.join_next().await {
        match outcome.unwrap().unwrap() {
            GraphFrameReturnCommitOutcome::Committed(r) => {
                committed += 1;
                record = Some(r);
            }
            GraphFrameReturnCommitOutcome::Idempotent(_) => idempotent += 1,
            _ => panic!("return outcome"),
        }
    }
    assert_eq!((committed, idempotent), (1, 23));
    let record = record.unwrap();
    let origin = entry.entry().start().activation();
    assert_eq!(record.result().intent().activation(), origin);
    assert_eq!(record.completion().start(), &entry.entry().start().head());
    let db = pool().await;
    let stack:(String,Option<Vec<u8>>,i32)=query_as("SELECT active_namespace,active_frame_identity_digest,lifetime_starts FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert_eq!(stack, (String::new(), None, 1));
    let fresh = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store.options().clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        Box::pin(fresh.load_graph_frame_return(
            fence.tenant_id(),
            fence.run_id(),
            entry.entry().checkpoint().frame().namespace()
        ))
        .await
        .unwrap()
        .unwrap()
        .digest(),
        record.digest()
    );
    assert_eq!(
        Box::pin(fresh.load_node_attempt(
            fence.tenant_id(),
            &fence.run_id(),
            entry.entry().start().attempt_id()
        ))
        .await
        .unwrap()
        .status(),
        NodeAttemptStatus::Succeeded
    );
    let result: PendingNodeResult = Box::pin(fresh.load_pending_node_result(origin))
        .await
        .unwrap();
    assert_eq!(result, *record.result());
    assert_eq!(
        Box::pin(fresh.load_graph_frame_entry(
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
        Box::pin(fresh.load_graph_frame_checkpoint(
            fence.tenant_id(),
            fence.run_id(),
            entry.entry().checkpoint().frame().namespace(),
            barrier.checkpoint().checkpoint().checkpoint_id()
        ))
        .await
        .unwrap(),
        *barrier.checkpoint()
    );
    let parent = caller_graph("barrier-parent", &graph, 4096);
    let root_plan = parent
        .plan_barrier(
            admission.checkpoint(),
            std::slice::from_ref(&result),
            CheckpointId::generate(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: parent.reducer().clone(),
            },
        )
        .unwrap();
    let append = worker_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        JournalExpectation::exact(record.event().head()),
        fence.clone(),
        24_001,
    );
    let advanced = Box::pin(fresh.append_worker_barrier(
        append,
        RunProjection::unchanged(),
        root_plan.into_parts().0,
    ))
    .await
    .unwrap();
    assert_eq!(advanced.checkpoint().superstep().get(), 1);
    assert_eq!(
        advanced.checkpoint().ready_nodes(),
        &ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap()
    );
    let _next =
        Box::pin(fresh.supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate()))
            .await
            .unwrap();
    let retry = Box::pin(fresh.return_graph_frame(
        prepared,
        EventId::generate(),
        fence.clone(),
        admission.event().head(),
        BudgetUsage::zero(),
        &NoSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await
    .unwrap();
    let GraphFrameReturnCommitOutcome::Idempotent(retry) = retry else {
        panic!("returned child cannot repeat");
    };
    assert_eq!(retry.digest(), record.digest());
    let rebound = Box::pin(fresh.rebind_graph_frame_caller(
        entry.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        fence.clone(),
        admission.event().head(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        rebound,
        NodeAttemptCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(rebound.attempt().status(), NodeAttemptStatus::Succeeded);
    fresh.close().await;
    db.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn nested_return_resumes_scoped_parent_without_resetting_shared_usage() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, outer, middle, fence, child_return) =
        Box::pin(nested_child_return(&store)).await;
    Box::pin(nested_parent_return(
        &store,
        admission,
        outer,
        middle,
        fence,
        child_return,
    ))
    .await;
}

async fn nested_child_return(
    store: &PostgresStore,
) -> (
    StoredAgentAdmission,
    stateknot_store_postgres::StoredGraphFrameEntry,
    CompiledGraph,
    RunFence,
    StoredGraphFrameReturn,
) {
    let (_, leaf, _) = super::frame_transactions::graphs();
    let middle = caller_graph("return-middle", &leaf, 4096);
    let (admission, outer, middle, fence) = Box::pin(super::frame_barriers::enter_graph(
        store,
        "frame-return-nested",
        middle,
        &[leaf.clone()],
    ))
    .await;
    let call = &middle.frame_calls().unwrap().calls()[0];
    let entry_plan = stateknot_core::GraphFrameEntryPlan::for_frame(
        call,
        &middle,
        outer.entry().checkpoint(),
        &leaf,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence.clone(),
    )
    .unwrap();
    let stateknot_store_postgres::GraphFrameEntryCommitOutcome::Committed(inner) =
        Box::pin(store.enter_graph_frame(
            entry_plan,
            EventId::generate(),
            outer.event().head(),
            outer.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
        ))
        .await
        .unwrap()
    else {
        panic!("inner entry");
    };
    let result = Box::pin(succeed(
        store,
        inner.entry().checkpoint(),
        &fence,
        terminal(&leaf),
    ))
    .await;
    let child_plan = plan(&leaf, inner.entry().checkpoint(), &result);
    let child_barrier = Box::pin(commit(
        store,
        child_plan.clone(),
        &fence,
        inner.direct_usage_after().unwrap(),
        &leaf,
    ))
    .await;
    let child_return = Box::pin(returned(
        store,
        child_plan,
        &fence,
        child_barrier.direct_usage_after().unwrap(),
        &leaf,
    ))
    .await;
    (admission, outer, middle, fence, child_return)
}

#[allow(clippy::too_many_lines)]
async fn nested_parent_return(
    store: &PostgresStore,
    admission: StoredAgentAdmission,
    outer: stateknot_store_postgres::StoredGraphFrameEntry,
    middle: CompiledGraph,
    fence: RunFence,
    child_return: StoredGraphFrameReturn,
) {
    let parent_result =
        Box::pin(store.load_pending_node_result(child_return.result().intent().activation()))
            .await
            .unwrap();
    assert_eq!(parent_result, *child_return.result());
    let parent_plan = plan(&middle, outer.entry().checkpoint(), &parent_result);
    let observed = child_return.event().head();
    assert!(matches!(
        Box::pin(store.commit_graph_frame_barrier(
            parent_plan.clone(),
            EventId::generate(),
            fence.clone(),
            observed,
            outer.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: middle.reducer().clone()
            }
        ))
        .await,
        Err(StoreError::IncompleteChildAccounting)
    ));
    let continued = Box::pin(commit(
        store,
        parent_plan,
        &fence,
        child_return.direct_usage_after().unwrap(),
        &middle,
    ))
    .await;
    let finished = Box::pin(succeed(
        store,
        continued.checkpoint(),
        &fence,
        terminal(&middle),
    ))
    .await;
    let final_plan = plan(&middle, continued.checkpoint(), &finished);
    let final_barrier = Box::pin(commit(
        store,
        final_plan.clone(),
        &fence,
        continued.direct_usage_after().unwrap(),
        &middle,
    ))
    .await;
    let root_return = Box::pin(returned(
        store,
        final_plan,
        &fence,
        final_barrier.direct_usage_after().unwrap(),
        &middle,
    ))
    .await;
    assert_eq!(
        root_return.result().intent().activation(),
        outer.entry().start().activation()
    );
    assert_eq!(
        root_return
            .direct_usage_after()
            .unwrap()
            .graph_steps()
            .get(),
        7
    );
    assert_eq!(
        Box::pin(store.load_graph_frame_checkpoint(
            fence.tenant_id(),
            fence.run_id(),
            outer.entry().checkpoint().frame().namespace(),
            final_barrier.checkpoint().checkpoint().checkpoint_id()
        ))
        .await
        .unwrap(),
        *final_barrier.checkpoint()
    );
    assert_eq!(
        Box::pin(store.load_graph_frame_return(
            fence.tenant_id(),
            fence.run_id(),
            child_return.terminal_checkpoint().frame().namespace()
        ))
        .await
        .unwrap()
        .unwrap()
        .digest(),
        child_return.digest()
    );
    assert_eq!(
        store
            .load_current_checkpoint(fence.tenant_id(), fence.run_id())
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    let db = pool().await;
    let stack:(String,i32)=query_as("SELECT active_namespace,lifetime_starts FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert_eq!(stack, (String::new(), 2));
    db.close().await;
}

async fn snapshot(db: &PgPool, fence: &RunFence) -> Vec<String> {
    let mut rows = Vec::new();
    for table in [
        "runs",
        "run_events",
        "run_attempt_claims",
        "node_attempts",
        "node_attempt_completions",
        "pending_node_results",
        "pending_node_result_consumptions",
        "graph_frame_entries",
        "graph_frame_barriers",
        "graph_frame_caller_bindings",
        "graph_frame_returns",
        "graph_frame_heads",
        "graph_frame_stacks",
        "run_checkpoints",
    ] {
        rows.push(query_scalar::<_,String>(&format!("SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text),'[]'::jsonb)::text FROM stateknot.{table} x WHERE tenant_id=$1 AND run_id=$2")).bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(db).await.unwrap());
    }
    rows
}
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn return_write_faults_roll_back_every_component_and_exact_stack() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, _, graph, fence, prepared, barrier) =
        Box::pin(finished(&store, "frame-return-fault")).await;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    query("CREATE FUNCTION stateknot.test_frame_return_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected frame return write failure'; END $$").execute(&db).await.unwrap();
    for (table, operation) in [
        ("run_events", "INSERT"),
        ("graph_frame_returns", "INSERT"),
        ("graph_frame_stacks", "UPDATE"),
        ("pending_node_results", "INSERT"),
        ("node_attempt_completions", "INSERT"),
        ("runs", "UPDATE"),
    ] {
        query(&format!("CREATE TRIGGER test_frame_return_fault BEFORE {operation} ON stateknot.{table} FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_return_fault()")).execute(&db).await.unwrap();
        let result = Box::pin(store.return_graph_frame(
            prepared.clone(),
            EventId::generate(),
            fence.clone(),
            barrier.event().head(),
            barrier.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone(),
            },
        ))
        .await;
        assert!(result.is_err(), "{table}");
        query(&format!(
            "DROP TRIGGER test_frame_return_fault ON stateknot.{table}"
        ))
        .execute(&db)
        .await
        .unwrap();
        assert_eq!(snapshot(&db, &fence).await, before, "{table}");
    }
    query("DROP FUNCTION stateknot.test_frame_return_fault()")
        .execute(&db)
        .await
        .unwrap();
    Box::pin(returned(
        &store,
        prepared,
        &fence,
        barrier.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    store.verify_schema().await.unwrap();
    db.close().await;
}
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn checksum_valid_return_substitution_is_rejected_by_all_whole_readers() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, fence, prepared, barrier) =
        Box::pin(finished(&store, "frame-return-corrupt")).await;
    let record = Box::pin(returned(
        &store,
        prepared,
        &fence,
        barrier.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    let db = pool().await;
    let original: Vec<u8> = query_scalar(
        "SELECT return_bytes FROM stateknot.graph_frame_returns WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(&db)
    .await
    .unwrap();
    let mut wire: serde_json::Value = serde_json::from_slice(&original).unwrap();
    wire["intent"]["budget"]["direct_usage"] = serde_json::to_value(BudgetUsage::zero()).unwrap();
    let tampered = serde_json_canonicalizer::to_vec(&wire).unwrap();
    query(
        "ALTER TABLE stateknot.graph_frame_returns DISABLE TRIGGER graph_frame_returns_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    query(
        "UPDATE stateknot.graph_frame_returns SET return_bytes=$3 WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .bind(tampered)
    .execute(&db)
    .await
    .unwrap();
    query("ALTER TABLE stateknot.graph_frame_returns ENABLE TRIGGER graph_frame_returns_immutable")
        .execute(&db)
        .await
        .unwrap();
    assert!(
        Box::pin(store.load_graph_frame_return(
            fence.tenant_id(),
            fence.run_id(),
            entry.entry().checkpoint().frame().namespace()
        ))
        .await
        .is_err()
    );
    assert!(
        Box::pin(store.load_node_attempt(
            fence.tenant_id(),
            &fence.run_id(),
            entry.entry().start().attempt_id()
        ))
        .await
        .is_err()
    );
    assert!(
        Box::pin(store.load_pending_node_result(entry.entry().start().activation()))
            .await
            .is_err()
    );
    query(
        "ALTER TABLE stateknot.graph_frame_returns DISABLE TRIGGER graph_frame_returns_immutable",
    )
    .execute(&db)
    .await
    .unwrap();
    query(
        "UPDATE stateknot.graph_frame_returns SET return_bytes=$3 WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .bind(original)
    .execute(&db)
    .await
    .unwrap();
    query("ALTER TABLE stateknot.graph_frame_returns ENABLE TRIGGER graph_frame_returns_immutable")
        .execute(&db)
        .await
        .unwrap();
    assert_eq!(
        Box::pin(store.load_graph_frame_return(
            fence.tenant_id(),
            fence.run_id(),
            entry.entry().checkpoint().frame().namespace()
        ))
        .await
        .unwrap()
        .unwrap()
        .digest(),
        record.digest()
    );
    store.verify_schema().await.unwrap();
    db.close().await;
}

// Each phase releases its large future before the next recovered frame is
// settled. The production replay still uses the normal process/thread stack.
async fn finish_and_return(
    store: &PostgresStore,
    entry: &stateknot_store_postgres::StoredGraphFrameEntry,
    graph: &CompiledGraph,
    fence: &RunFence,
    usage: BudgetUsage,
    child: Option<&StoredGraphFrameReturn>,
) -> StoredGraphFrameReturn {
    let (base, usage) = if let Some(child) = child {
        let continued = Box::pin(commit(
            store,
            plan(graph, entry.entry().checkpoint(), child.result()),
            fence,
            usage,
            graph,
        ))
        .await;
        (
            continued.checkpoint().clone(),
            continued.direct_usage_after().unwrap(),
        )
    } else {
        (entry.entry().checkpoint().clone(), usage)
    };
    let result = Box::pin(succeed(store, &base, fence, terminal(graph))).await;
    let prepared = plan(graph, &base, &result);
    let barrier = Box::pin(commit(store, prepared.clone(), fence, usage, graph)).await;
    Box::pin(returned(
        store,
        prepared,
        fence,
        barrier.direct_usage_after().unwrap(),
        graph,
    ))
    .await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn seven_level_return_cascade_preserves_every_proof_and_lifetime_counter() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-seven-returns");
    let run = RunId::generate();
    let (_, leaf, _) = super::frame_transactions::graphs();
    let mut definitions = vec![leaf];
    for level in (0..7).rev() {
        definitions.push(caller_graph(
            &format!("return-level-{level}"),
            definitions.last().unwrap(),
            4096,
        ));
    }
    definitions.reverse();
    for definition in &definitions {
        store
            .register_graph_definition(tenant.clone(), definition.clone())
            .await
            .unwrap();
    }
    let admission = Box::pin(super::frame_transactions::admit(
        &store,
        &tenant,
        run,
        &definitions[0],
        &definitions[1],
    ))
    .await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let mut observed = admission.event().head();
    let mut usage = super::frame_transactions::initial_usage(&admission);
    let mut entries: Vec<stateknot_store_postgres::StoredGraphFrameEntry> = Vec::new();
    for level in 0..7 {
        let parent = &definitions[level];
        let target = &definitions[level + 1];
        let call = &parent.frame_calls().unwrap().calls()[0];
        let plan = if let Some(previous) = entries.last() {
            stateknot_core::GraphFrameEntryPlan::for_frame(
                call,
                parent,
                previous.entry().checkpoint(),
                target,
                CheckpointId::generate(),
                AttemptId::generate(),
                fence.clone(),
            )
        } else {
            stateknot_core::GraphFrameEntryPlan::for_root(
                call,
                parent,
                admission.checkpoint(),
                target,
                CheckpointId::generate(),
                AttemptId::generate(),
                fence.clone(),
            )
        }
        .unwrap();
        let stateknot_store_postgres::GraphFrameEntryCommitOutcome::Committed(entry) =
            Box::pin(store.enter_graph_frame(
                plan,
                EventId::generate(),
                observed,
                usage,
                &AcceptGraphSchemas,
            ))
            .await
            .unwrap()
        else {
            panic!("fresh level");
        };
        observed = entry.event().head();
        usage = entry.direct_usage_after().unwrap();
        entries.push(entry);
    }
    let mut previous = None;
    let mut returns = Vec::new();
    for level in (0..7).rev() {
        let live = store.observe_live_lease(&fence).await.unwrap();
        let expires = Timestamp::from_unix_micros(
            live.observed_at()
                .unix_micros()
                .checked_add(30_000_000)
                .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            store.renew_lease(&fence, expires).await.unwrap(),
            LeaseRenewalOutcome::Renewed(_)
        ));
        let record = Box::pin(finish_and_return(
            &store,
            &entries[level],
            &definitions[level + 1],
            &fence,
            usage,
            previous.as_ref(),
        ))
        .await;
        usage = record.direct_usage_after().unwrap();
        assert_eq!(usage.graph_depth().get(), 8);
        previous = Some(record.clone());
        returns.push(record);
    }
    assert_eq!(usage.graph_steps().get(), 27);
    let db = pool().await;
    let stack: (String, Option<Vec<u8>>, i32) = query_as("SELECT active_namespace,active_frame_identity_digest,lifetime_starts FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2").bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&db).await.unwrap();
    assert_eq!(stack, (String::new(), None, 7));
    for record in returns {
        let loaded = Box::pin(store.load_graph_frame_return(
            &tenant,
            run,
            record.terminal_checkpoint().frame().namespace(),
        ))
        .await
        .unwrap()
        .unwrap();
        assert_eq!(loaded.digest(), record.digest());
        assert_eq!(
            Box::pin(store.load_pending_node_result(record.result().intent().activation()))
                .await
                .unwrap(),
            *record.result()
        );
    }
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, run)
            .await
            .unwrap()
            .unwrap(),
        *admission.checkpoint()
    );
    db.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn return_requires_current_framework_start_after_fence_takeover() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, old, prepared, barrier) =
        Box::pin(finished(&store, "frame-return-takeover")).await;
    let next =
        Box::pin(store.supersede_lease(old.tenant_id(), old.run_id(), AttemptId::generate()))
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
    let observed = store
        .load_run(next.tenant_id(), next.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    assert!(matches!(
        Box::pin(store.return_graph_frame(
            prepared.clone(),
            EventId::generate(),
            old.clone(),
            observed.clone(),
            barrier.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await,
        Err(StoreError::StaleFence)
    ));
    // A newer Run fence alone cannot complete the old physical caller.
    assert!(matches!(
        Box::pin(store.return_graph_frame(
            prepared.clone(),
            EventId::generate(),
            next.clone(),
            observed.clone(),
            barrier.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await,
        Err(StoreError::StaleFence)
    ));
    let binding = Box::pin(store.rebind_graph_frame_caller(
        entry.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        observed,
        barrier.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    let usage = super::frame_callers::charged(&barrier.direct_usage_after().unwrap(), &binding);
    let record = Box::pin(returned(&store, prepared, &next, usage, &graph)).await;
    assert_eq!(
        record.completion().start(),
        &binding.attempt().start().head()
    );
    assert_eq!(record.result().fence(), &next);
    assert_eq!(
        Box::pin(store.load_node_attempt(
            old.tenant_id(),
            &old.run_id(),
            entry.entry().start().attempt_id()
        ))
        .await
        .unwrap()
        .status(),
        NodeAttemptStatus::Executing
    );
    assert_eq!(
        Box::pin(store.load_node_attempt(
            next.tenant_id(),
            &next.run_id(),
            binding.attempt().start().attempt_id()
        ))
        .await
        .unwrap()
        .status(),
        NodeAttemptStatus::Succeeded
    );
    let retry = Box::pin(store.rebind_graph_frame_caller(
        entry.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next,
        entry.event().head(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert_eq!(retry.attempt().status(), NodeAttemptStatus::Succeeded);
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
}

#[tokio::test]
async fn deferred_return_delay_cannot_commit_after_live_lease_expires() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_options(test_options(Duration::from_secs(2))).await else {
        return;
    };
    let (_, _, graph, old, prepared, barrier) =
        Box::pin(finished(&store, "frame-return-expiry")).await;
    let fence = old;
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    query("CREATE FUNCTION stateknot.test_frame_return_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.1); RETURN NULL; END $$").execute(&db).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER z_test_frame_return_delay AFTER INSERT ON stateknot.graph_frame_returns DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_return_delay()").execute(&db).await.unwrap();
    let outcome = Box::pin(store.return_graph_frame(
        prepared,
        EventId::generate(),
        fence.clone(),
        barrier.event().head(),
        barrier.direct_usage_after().unwrap(),
        &AcceptGraphSchemas,
        &IntegrationGraphReducer {
            reference: graph.reducer().clone(),
        },
    ))
    .await;
    query("DROP TRIGGER z_test_frame_return_delay ON stateknot.graph_frame_returns")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_frame_return_delay()")
        .execute(&db)
        .await
        .unwrap();
    assert!(
        matches!(outcome, Err(StoreError::LeaseExpired)),
        "{outcome:?}"
    );
    assert_eq!(snapshot(&db, &fence).await, before);
    store.verify_schema().await.unwrap();
    db.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn reserved_return_event_and_raw_stack_pop_cannot_bypass_whole_settlement() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, _, _, fence, _, barrier) = Box::pin(finished(&store, "frame-return-reserved")).await;
    let (schema, _) = PostgresStore::graph_frame_return_event_schema().unwrap();
    let payload = JournalPayload::new(
        schema,
        "graph-frame-returned".parse().unwrap(),
        BoundedJson::try_from(json!({"reserved":true})).unwrap(),
    )
    .unwrap();
    let db = pool().await;
    let before = snapshot(&db, &fence).await;
    let worker = JournalEventIntent::worker(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        fence.clone(),
        payload.clone(),
    )
    .unwrap();
    let append =
        JournalAppend::new(JournalExpectation::exact(barrier.event().head()), worker).unwrap();
    assert!(matches!(
        store
            .append_worker(append, RunProjection::unchanged())
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    let control = JournalEventIntent::control_plane(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        payload,
    )
    .unwrap();
    let append =
        JournalAppend::new(JournalExpectation::exact(barrier.event().head()), control).unwrap();
    assert!(matches!(
        store
            .append_control_plane(append, RunProjection::unchanged())
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    assert!(query("UPDATE stateknot.graph_frame_stacks SET active_namespace='',active_frame_identity_digest=NULL WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.is_err());
    assert!(query("UPDATE stateknot.graph_frame_stacks SET lifetime_starts=lifetime_starts+1 WHERE tenant_id=$1 AND run_id=$2").bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).execute(&db).await.is_err());
    assert_eq!(snapshot(&db, &fence).await, before);
    db.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn source_schema_30_upgrade_retains_terminal_and_rebound_caller_before_return() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let original = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_frame_return_upgrade_{}",
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
    let (_, entry, graph, old, prepared, barrier) =
        Box::pin(finished(&store, "frame-return-upgrade")).await;
    let fence =
        Box::pin(store.supersede_lease(old.tenant_id(), old.run_id(), AttemptId::generate()))
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
    let observed = store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    let binding = Box::pin(store.rebind_graph_frame_caller(
        entry.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        fence.clone(),
        observed,
        barrier.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    let usage = super::frame_callers::charged(&barrier.direct_usage_after().unwrap(), &binding);
    store.close().await;
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    // Populated source-schema reconstruction preserves actual committed facts.
    // It does not substitute for retained historical N-1/N-2 binary tests.
    let before: Vec<(i64, Vec<u8>)> = query_as(
        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=30 ORDER BY version",
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    let facts = snapshot(&fixture, &fence).await;
    sqlx_core::raw_sql::raw_sql(include_str!("../fixtures/revert_graph_frame_returns.sql"))
        .execute(&fixture)
        .await
        .unwrap();
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        30
    );
    assert!(matches!(
        PostgresStore::connect(&url, options.clone()).await,
        Err(StoreError::IncompatibleSchema)
    ));
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let upgraded = PostgresStore::connect(&url, options).await.unwrap();
    let after: Vec<(i64, Vec<u8>)> = query_as(
        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=30 ORDER BY version",
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    assert_eq!(before, after);
    assert_eq!(facts, snapshot(&fixture, &fence).await);
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
        Box::pin(upgraded.load_graph_frame_barrier(
            fence.tenant_id(),
            fence.run_id(),
            entry.entry().checkpoint().frame().namespace(),
            entry.entry().checkpoint().checkpoint().checkpoint_id()
        ))
        .await
        .unwrap()
        .unwrap()
        .digest(),
        barrier.digest()
    );
    assert_eq!(
        Box::pin(upgraded.load_node_attempt(
            fence.tenant_id(),
            &fence.run_id(),
            binding.attempt().start().attempt_id()
        ))
        .await
        .unwrap()
        .start(),
        binding.attempt().start()
    );
    let record = Box::pin(returned(&upgraded, prepared, &fence, usage, &graph)).await;
    assert_eq!(
        record.completion().start(),
        &binding.attempt().start().head()
    );
    upgraded.verify_schema().await.unwrap();
    upgraded.close().await;
    fixture.close().await;
    query(&format!("DROP DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
