// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real framework caller takeover without duplicating logical child frames.
use super::frame_barriers::{commit, enter, enter_graph, plan, pool, succeed, terminal};
use super::frame_transactions::{caller_graph, graphs};
use super::*;
use stateknot_core::{ExecutionCount, GraphFrameEntryPlan, RunFence};
use stateknot_store_postgres::GraphFrameEntryCommitOutcome;

async fn observed(store: &PostgresStore, fence: &RunFence) -> JournalHead {
    store
        .load_run(fence.tenant_id(), fence.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone()
}
async fn takeover(store: &PostgresStore, fence: &RunFence) -> RunFence {
    store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone()
}
fn charged(usage: &BudgetUsage, result: &NodeAttemptCommitOutcome) -> BudgetUsage {
    usage
        .checked_accumulate(
            &BudgetUsage::builder()
                .graph_steps(ExecutionCount::new(1))
                .retries(ExecutionCount::new(1))
                .event_bytes(ByteCount::new(
                    serde_json_canonicalizer::to_vec(result.event())
                        .unwrap()
                        .len() as u64,
                ))
                .build()
                .unwrap(),
        )
        .unwrap()
}
async fn snapshot(pool: &PgPool, fence: &RunFence) -> Vec<String> {
    let mut snapshot = Vec::new();
    for table in [
        "runs",
        "run_events",
        "run_attempt_claims",
        "node_attempts",
        "node_attempt_completions",
        "pending_node_results",
        "run_checkpoints",
        "graph_frame_entries",
        "graph_frame_caller_bindings",
        "graph_frame_heads",
        "graph_frame_stacks",
    ] {
        snapshot.push(query_scalar::<_,String>(&format!("SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY to_jsonb(x)::text),'[]'::jsonb)::text FROM stateknot.{table} x WHERE tenant_id=$1 AND run_id=$2"))
            .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).fetch_one(pool).await.unwrap());
    }
    snapshot
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn framework_caller_epoch_race_recovers_once_without_reentering_child() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (admission, entry, _, old) = Box::pin(enter(&store, "frame-caller-race", false)).await;
    let frame = entry.entry().checkpoint().frame().clone();
    let next = takeover(&store, &old).await;
    let head = observed(&store, &next).await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let frame = frame.clone();
        let fence = next.clone();
        let head = head.clone();
        let usage = entry.direct_usage_after().unwrap();
        tasks.spawn(async move {
            Box::pin(store.rebind_graph_frame_caller(
                &frame,
                AttemptId::generate(),
                EventId::generate(),
                fence,
                head,
                usage,
            ))
            .await
        });
    }
    let mut committed = 0;
    let mut idempotent = 0;
    let mut actual = None;
    while let Some(result) = tasks.join_next().await {
        let result = result.unwrap().unwrap();
        match &result {
            NodeAttemptCommitOutcome::Committed { .. } => committed += 1,
            NodeAttemptCommitOutcome::Idempotent { .. } => idempotent += 1,
            _ => panic!("caller start outcome"),
        }
        if let Some(start) = &actual {
            assert_eq!(result.attempt().start(), start);
        } else {
            actual = Some(result.attempt().start().clone());
        }
    }
    assert_eq!((committed, idempotent), (1, 23));
    let start = actual.unwrap();
    assert_eq!(start.activation(), entry.entry().start().activation());
    assert_eq!(start.fence(), &next);
    let fresh = PostgresStore::connect(
        &std::env::var(DATABASE_URL_ENV).unwrap(),
        store.options().clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        Box::pin(fresh.load_node_attempt(next.tenant_id(), &next.run_id(), start.attempt_id()))
            .await
            .unwrap()
            .start(),
        &start
    );
    // No ordinary completion may convert framework authority into application work.
    let intent = PendingNodeResultIntent::new(
        start.activation().clone(),
        NodeStateChange::Unchanged,
        NodeControl::Route {
            route_id: "return".parse().unwrap(),
        },
        stateknot_core::NodeInvocationBindings::empty(),
    )
    .unwrap();
    let head = observed(&store, &next).await;
    let append = worker_append(
        next.tenant_id().clone(),
        next.run_id(),
        EventId::generate(),
        JournalExpectation::exact(head),
        next.clone(),
        0,
    );
    assert!(matches!(
        Box::pin(store.succeed_node_attempt(append, &start.head(), intent, BudgetUsage::zero()))
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    let _future = takeover(&store, &next).await;
    let retry = Box::pin(store.rebind_graph_frame_caller(
        &frame,
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        entry.event().head(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(retry.attempt().start(), &start);
    let original = Box::pin(store.rebind_graph_frame_caller(
        &frame,
        AttemptId::generate(),
        EventId::generate(),
        old.clone(),
        entry.event().head(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        original,
        NodeAttemptCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(original.attempt().start(), entry.entry().start());
    assert_eq!(
        store
            .load_current_checkpoint(next.tenant_id(), next.run_id())
            .await
            .unwrap()
            .as_ref(),
        Some(admission.checkpoint())
    );
    assert_eq!(
        store
            .load_graph_frame_checkpoint(
                next.tenant_id(),
                next.run_id(),
                frame.namespace(),
                entry.entry().checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .unwrap(),
        *entry.entry().checkpoint()
    );
    let db = pool().await;
    let counts:(i64,i64,i64)=query_as("SELECT (SELECT count(*) FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2),(SELECT count(*) FROM stateknot.graph_frame_caller_bindings WHERE tenant_id=$1 AND run_id=$2),(SELECT count(*) FROM stateknot.node_attempts WHERE tenant_id=$1 AND run_id=$2)").bind(next.tenant_id().as_str()).bind(*next.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    assert_eq!(counts, (1, 1, 2));
    db.close().await;
    fresh.close().await;
    store.close().await;
}

#[tokio::test]
async fn framework_takeover_at_noninitial_checkpoint_charges_shared_budget_before_barrier() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, old) = Box::pin(enter(&store, "frame-caller-continuation", true)).await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &old,
        NodeControl::Continue,
    ))
    .await;
    let first = Box::pin(commit(
        &store,
        plan(&graph, entry.entry().checkpoint(), &result),
        &old,
        entry.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    let next = takeover(&store, &old).await;
    let head = observed(&store, &next).await;
    let binding = Box::pin(store.rebind_graph_frame_caller(
        first.checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        head,
        first.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    let usage = charged(&first.direct_usage_after().unwrap(), &binding);
    let result = Box::pin(succeed(&store, first.checkpoint(), &next, terminal(&graph))).await;
    let candidate = plan(&graph, first.checkpoint(), &result);
    assert!(matches!(
        Box::pin(store.commit_graph_frame_barrier(
            candidate.clone(),
            EventId::generate(),
            next.clone(),
            result.journal_head().clone(),
            first.direct_usage_after().unwrap(),
            &AcceptGraphSchemas,
            &IntegrationGraphReducer {
                reference: graph.reducer().clone()
            }
        ))
        .await,
        Err(StoreError::IncompleteChildAccounting)
    ));
    let final_barrier = Box::pin(commit(&store, candidate, &next, usage, &graph)).await;
    assert_eq!(final_barrier.checkpoint().checkpoint().superstep().get(), 2);
    assert_eq!(
        store
            .load_graph_frame_checkpoint(
                next.tenant_id(),
                next.run_id(),
                final_barrier.checkpoint().frame().namespace(),
                final_barrier.checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .unwrap(),
        *final_barrier.checkpoint()
    );
    assert_eq!(
        Box::pin(store.load_node_attempt(
            next.tenant_id(),
            &next.run_id(),
            binding.attempt().start().attempt_id()
        ))
        .await
        .unwrap()
        .start(),
        binding.attempt().start()
    );
    store.close().await;
}

#[tokio::test]
async fn scoped_parent_caller_can_rebind_while_its_existing_leaf_remains_active() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, leaf, _) = graphs();
    let middle = caller_graph("caller-middle", &leaf, 4096);
    let (_, entry, middle, old) = Box::pin(enter_graph(
        &store,
        "frame-caller-scoped-parent",
        middle,
        std::slice::from_ref(&leaf),
    ))
    .await;
    let call = &middle.frame_calls().unwrap().calls()[0];
    let candidate = GraphFrameEntryPlan::for_frame(
        call,
        &middle,
        entry.entry().checkpoint(),
        &leaf,
        CheckpointId::generate(),
        AttemptId::generate(),
        old.clone(),
    )
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(nested) = Box::pin(store.enter_graph_frame(
        candidate,
        EventId::generate(),
        entry.event().head(),
        entry.direct_usage_after().unwrap(),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap() else {
        panic!("nested entry")
    };
    let next = takeover(&store, &old).await;
    let head = observed(&store, &next).await;
    let binding = Box::pin(store.rebind_graph_frame_caller(
        nested.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        head,
        nested.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    assert_eq!(
        binding.attempt().start().activation().graph_namespace(),
        entry.entry().checkpoint().frame().namespace()
    );
    assert_eq!(
        binding.attempt().start().activation(),
        nested.entry().start().activation()
    );
    assert_eq!(
        Box::pin(store.load_node_attempt(
            next.tenant_id(),
            &next.run_id(),
            binding.attempt().start().attempt_id()
        ))
        .await
        .unwrap()
        .start(),
        binding.attempt().start()
    );
    assert_eq!(
        store
            .load_graph_frame_checkpoint(
                next.tenant_id(),
                next.run_id(),
                nested.entry().checkpoint().frame().namespace(),
                nested.entry().checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .unwrap(),
        *nested.entry().checkpoint()
    );
    store.close().await;
}

#[tokio::test]
async fn framework_rebinding_write_faults_roll_back_every_component_and_projection() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, old) = Box::pin(enter(&store, "frame-caller-faults", false)).await;
    let next = takeover(&store, &old).await;
    let head = observed(&store, &next).await;
    let db = pool().await;
    let before = snapshot(&db, &next).await;
    query("CREATE FUNCTION stateknot.test_frame_caller_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected caller binding fault'; END $$").execute(&db).await.unwrap();
    for (table, action) in [
        ("run_events", "INSERT"),
        ("graph_frame_caller_bindings", "INSERT"),
        ("run_attempt_claims", "INSERT"),
        ("node_attempts", "INSERT"),
        ("runs", "UPDATE"),
    ] {
        query(&format!("CREATE TRIGGER test_frame_caller_fault BEFORE {action} ON stateknot.{table} FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_caller_fault()" )).execute(&db).await.unwrap();
        let result = Box::pin(store.rebind_graph_frame_caller(
            entry.entry().checkpoint().frame(),
            AttemptId::generate(),
            EventId::generate(),
            next.clone(),
            head.clone(),
            entry.direct_usage_after().unwrap(),
        ))
        .await;
        query(&format!(
            "DROP TRIGGER test_frame_caller_fault ON stateknot.{table}"
        ))
        .execute(&db)
        .await
        .unwrap();
        assert!(result.is_err(), "{table}");
        assert_eq!(snapshot(&db, &next).await, before, "{table}");
    }
    query("DROP FUNCTION stateknot.test_frame_caller_fault()")
        .execute(&db)
        .await
        .unwrap();
    let result = Box::pin(store.rebind_graph_frame_caller(
        entry.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        head,
        entry.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    assert!(matches!(result, NodeAttemptCommitOutcome::Committed { .. }));
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn framework_rebinding_rejects_checksum_valid_witness_rewrite_then_recovers_original() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, old) = Box::pin(enter(&store, "frame-caller-corrupt", false)).await;
    let next = takeover(&store, &old).await;
    let head = observed(&store, &next).await;
    let result = Box::pin(store.rebind_graph_frame_caller(
        entry.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        head,
        entry.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    let db = pool().await;
    let original:Vec<u8>=query_scalar("SELECT binding_bytes FROM stateknot.graph_frame_caller_bindings WHERE tenant_id=$1 AND run_id=$2").bind(next.tenant_id().as_str()).bind(*next.run_id().as_uuid()).fetch_one(&db).await.unwrap();
    let mut wire: serde_json::Value = serde_json::from_slice(&original).unwrap();
    wire["intent"]["budget"]["direct_usage"]["graph_steps"] = json!("0");
    let damaged = serde_json_canonicalizer::to_vec(&wire).unwrap();
    assert_ne!(original, damaged);
    for bytes in [damaged, original] {
        query("ALTER TABLE stateknot.graph_frame_caller_bindings DISABLE TRIGGER graph_frame_caller_bindings_immutable").execute(&db).await.unwrap();
        query("UPDATE stateknot.graph_frame_caller_bindings SET binding_bytes=$3 WHERE tenant_id=$1 AND run_id=$2").bind(next.tenant_id().as_str()).bind(*next.run_id().as_uuid()).bind(bytes.clone()).execute(&db).await.unwrap();
        query("ALTER TABLE stateknot.graph_frame_caller_bindings ENABLE TRIGGER graph_frame_caller_bindings_immutable").execute(&db).await.unwrap();
        let loaded = Box::pin(store.load_node_attempt(
            next.tenant_id(),
            &next.run_id(),
            result.attempt().start().attempt_id(),
        ))
        .await;
        if bytes == serde_json_canonicalizer::to_vec(&wire).unwrap() {
            assert!(loaded.is_err());
        } else {
            assert_eq!(loaded.unwrap().start(), result.attempt().start());
        }
    }
    store.verify_schema().await.unwrap();
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn framework_rebinding_deferred_delay_cannot_commit_after_live_lease_expires() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_options(test_options(Duration::from_secs(2))).await else {
        return;
    };
    let (_, entry, _, old) = Box::pin(enter(&store, "frame-caller-expiry", false)).await;
    let next = takeover(&store, &old).await;
    let head = observed(&store, &next).await;
    let db = pool().await;
    let before = snapshot(&db, &next).await;
    query("CREATE FUNCTION stateknot.test_frame_caller_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2.1); RETURN NULL; END $$").execute(&db).await.unwrap();
    query("CREATE CONSTRAINT TRIGGER test_frame_caller_delay AFTER INSERT ON stateknot.graph_frame_caller_bindings DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_caller_delay()").execute(&db).await.unwrap();
    let result = Box::pin(store.rebind_graph_frame_caller(
        entry.entry().checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        head,
        entry.direct_usage_after().unwrap(),
    ))
    .await;
    query("DROP TRIGGER test_frame_caller_delay ON stateknot.graph_frame_caller_bindings")
        .execute(&db)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_frame_caller_delay()")
        .execute(&db)
        .await
        .unwrap();
    assert!(
        matches!(result, Err(StoreError::LeaseExpired)),
        "{result:?}"
    );
    assert_eq!(snapshot(&db, &next).await, before);
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn framework_rebinding_preserves_original_retry_budget_and_bounded_physical_history() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let db = pool().await;
    for (name, limit, expected) in [
        ("frame-caller-retry-budget", 25usize, false),
        ("frame-caller-hard-limit", 63usize, true),
    ] {
        let (_, child, _) = graphs();
        let parent = caller_graph(name, &child, 4096);
        let tenant = tenant(name);
        let run = RunId::generate();
        let admission = Box::pin(super::frame_transactions::admit_with_retry_limit(
            &store,
            &tenant,
            run,
            &parent,
            &child,
            Some(if expected { 64 } else { 25 }),
        ))
        .await;
        let mut fence = store
            .claim_lease(&tenant, run, AttemptId::generate())
            .await
            .unwrap()
            .lease()
            .fence()
            .clone();
        let candidate = GraphFrameEntryPlan::for_root(
            &parent.frame_calls().unwrap().calls()[0],
            &parent,
            admission.checkpoint(),
            &child,
            CheckpointId::generate(),
            AttemptId::generate(),
            fence.clone(),
        )
        .unwrap();
        let GraphFrameEntryCommitOutcome::Committed(entry) = Box::pin(store.enter_graph_frame(
            candidate,
            EventId::generate(),
            admission.event().head(),
            super::frame_transactions::initial_usage(&admission),
            &AcceptGraphSchemas,
        ))
        .await
        .unwrap() else {
            panic!("entry")
        };
        let mut usage = entry.direct_usage_after().unwrap();
        for _ in 0..limit {
            fence = takeover(&store, &fence).await;
            let head = observed(&store, &fence).await;
            let result = Box::pin(store.rebind_graph_frame_caller(
                entry.entry().checkpoint().frame(),
                AttemptId::generate(),
                EventId::generate(),
                fence.clone(),
                head,
                usage.clone(),
            ))
            .await
            .unwrap();
            assert!(matches!(result, NodeAttemptCommitOutcome::Committed { .. }));
            usage = charged(&usage, &result);
        }
        fence = takeover(&store, &fence).await;
        let head = observed(&store, &fence).await;
        let before = snapshot(&db, &fence).await;
        let result = Box::pin(store.rebind_graph_frame_caller(
            entry.entry().checkpoint().frame(),
            AttemptId::generate(),
            EventId::generate(),
            fence.clone(),
            head,
            usage,
        ))
        .await;
        if expected {
            assert!(matches!(result, Err(StoreError::NodeAttemptLimitExceeded)));
            assert_eq!(snapshot(&db, &fence).await, before);
        } else {
            assert!(
                matches!(result, Err(StoreError::GraphFrameLimitExceeded)),
                "{result:?}"
            );
            assert_eq!(snapshot(&db, &fence).await, before);
        }
    }
    db.close().await;
    store.close().await;
}

#[tokio::test]
async fn reserved_framework_binding_event_requires_the_whole_transaction() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, _, fence) = Box::pin(enter(&store, "frame-caller-reserved", false)).await;
    let (schema, _) = PostgresStore::graph_frame_caller_event_schema().unwrap();
    let data = JournalPayload::new(
        schema,
        "graph-frame-caller-rebound".parse().unwrap(),
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
        data.clone(),
    )
    .unwrap();
    let append =
        JournalAppend::new(JournalExpectation::exact(entry.event().head()), worker).unwrap();
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
        data,
    )
    .unwrap();
    let append =
        JournalAppend::new(JournalExpectation::exact(entry.event().head()), control).unwrap();
    assert!(matches!(
        store
            .append_control_plane(append, RunProjection::unchanged())
            .await,
        Err(StoreError::GraphFrameCompoundRequired)
    ));
    assert_eq!(snapshot(&db, &fence).await, before);
    db.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn source_schema_29_upgrade_preserves_real_barrier_history_before_rebinding() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let original = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_frame_caller_upgrade_{}",
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
    let (_, entry, graph, old) = Box::pin(enter(&store, "frame-caller-upgrade", false)).await;
    let result = Box::pin(succeed(
        &store,
        entry.entry().checkpoint(),
        &old,
        terminal(&graph),
    ))
    .await;
    let barrier = Box::pin(commit(
        &store,
        plan(&graph, entry.entry().checkpoint(), &result),
        &old,
        entry.direct_usage_after().unwrap(),
        &graph,
    ))
    .await;
    store.close().await;
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    // This is exact populated source-schema reconstruction, not historical
    // binary qualification. Existing migration checksums and facts stay intact.
    let before: Vec<(i64, Vec<u8>)> = query_as(
        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=29 ORDER BY version",
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    sqlx_core::raw_sql::raw_sql(include_str!(
        "../fixtures/revert_graph_frame_caller_bindings.sql"
    ))
    .execute(&fixture)
    .await
    .unwrap();
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM public._sqlx_migrations")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        29
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
        "SELECT version,checksum FROM public._sqlx_migrations WHERE version<=29 ORDER BY version",
    )
    .fetch_all(&fixture)
    .await
    .unwrap();
    assert_eq!(before, after);
    assert_eq!(
        upgraded
            .load_graph_frame_entry(
                old.tenant_id(),
                old.run_id(),
                entry.entry().checkpoint().frame().namespace()
            )
            .await
            .unwrap()
            .digest(),
        entry.digest()
    );
    assert_eq!(
        upgraded
            .load_graph_frame_barrier(
                old.tenant_id(),
                old.run_id(),
                barrier.checkpoint().frame().namespace(),
                entry.entry().checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .unwrap()
            .unwrap()
            .digest(),
        barrier.digest()
    );
    assert_eq!(
        upgraded
            .load_pending_node_result(result.intent().activation())
            .await
            .unwrap(),
        result
    );
    let next = takeover(&upgraded, &old).await;
    let head = observed(&upgraded, &next).await;
    let rebound = Box::pin(upgraded.rebind_graph_frame_caller(
        barrier.checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        head,
        barrier.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        rebound,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    upgraded.verify_schema().await.unwrap();
    upgraded.close().await;
    fixture.close().await;
    query(&format!("DROP DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
