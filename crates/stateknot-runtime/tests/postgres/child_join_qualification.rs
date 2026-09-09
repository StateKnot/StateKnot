// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use sqlx_postgres::PgPoolOptions;
use stateknot_core::AgentAdmissionIntent;

fn half_child(original: &AgentAdmissionIntent) -> AgentAdmissionIntent {
    let mut limits = serde_json::to_value(original.budget()).unwrap();
    for (name, amount) in limits.as_object_mut().unwrap() {
        if matches!(
            name.as_str(),
            "deadline" | "graph_depth" | "concurrent_branches" | "fan_out"
        ) {
            continue;
        }
        if name == "costs" {
            for cost in amount.as_array_mut().unwrap() {
                let units: u64 = cost["micro_units"].as_str().unwrap().parse().unwrap();
                cost["micro_units"] = json!((units / 2).to_string());
            }
        } else {
            let units: u64 = amount.as_str().unwrap().parse().unwrap();
            *amount = json!((units / 2).to_string());
        }
    }
    AgentAdmissionIntent::new(
        original.provenance().clone(),
        original.descriptor().clone(),
        AgentRequest::new(
            original.request().input_schema().clone(),
            original.request().input().clone(),
            serde_json::from_value(limits).unwrap(),
        ),
        original.budget_layers().to_vec(),
        original.graph().clone(),
        original.authority().clone(),
    )
    .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_join_seals_complete_membership_and_canonical_order_not_completion_order() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let mut value = started(&store, "join-siblings").await;
    let first_key = value.intent.key().clone();
    value.intent = ChildRunAdmissionIntent::new(
        value.fixture.parent.admission(),
        first_key.clone(),
        half_child(value.intent.child()),
        value.intent.child_graph().clone(),
        value.intent.initial_state().clone(),
    )
    .unwrap();
    let first = spawn(&store, &value).await.unwrap();
    let first_child = first
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    value.head = first.record().spawn().head();
    let second_key = ChildRunKey::new(
        first_key.parent().clone(),
        ChildRunSlot::new("secondary").unwrap(),
    )
    .unwrap();
    let child = value.fixture.child();
    value.intent = ChildRunAdmissionIntent::new(
        value.fixture.parent.admission(),
        second_key.clone(),
        half_child(child.intent()),
        value.intent.child_graph().clone(),
        value.intent.initial_state().clone(),
    )
    .unwrap();
    let second = spawn(&store, &value).await.unwrap();
    let second_child = second
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    value.head = second.record().spawn().head();
    let partial = ChildRunJoinRequest::new([first_key.clone()]).unwrap();
    assert!(matches!(
        store
            .register_child_join(
                partial,
                &value.node,
                parent_append(&value, "child-join-registered")
            )
            .await,
        Err(StoreError::ChildJoinRejected)
    ));
    let request = ChildRunJoinRequest::new([second_key.clone(), first_key.clone()]).unwrap();
    let append = parent_append(&value, "child-join-registered");
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let store = store.clone();
        let request = request.clone();
        let append = append.clone();
        let node = value.node.clone();
        tasks.spawn(async move { store.register_child_join(request, &node, append).await });
    }
    let mut fresh = 0;
    while let Some(result) = tasks.join_next().await {
        if matches!(
            result.unwrap().unwrap(),
            ChildJoinCommitOutcome::Committed(_)
        ) {
            fresh += 1;
        }
    }
    assert_eq!(fresh, 1);
    for (key, id) in [(&second_key, second_child), (&first_key, first_child)] {
        fail(&store, key.tenant_id(), id, BudgetUsage::zero())
            .await
            .unwrap();
        Box::pin(store.settle_child_run(key, settlement_append(&store, key).await))
            .await
            .unwrap();
    }
    let published = store
        .publish_child_join(&request, publish_append(&store, &request).await)
        .await
        .unwrap();
    let binding = published.record().binding().unwrap();
    assert_eq!(binding.terminals()[0].terminal().run_id(), first_child);
    assert_eq!(binding.terminals()[1].terminal().run_id(), second_child);
    assert!(
        binding.terminals()[0].terminal().recorded_at()
            >= binding.terminals()[1].terminal().recorded_at()
    );
    let pool = sql_pool().await;
    let swapped = stateknot_core::ChildRunJoinBinding::new(
        request.clone(),
        binding.terminals().iter().rev().cloned(),
    )
    .unwrap();
    let original = binding.canonical_bytes().unwrap();
    let changed = swapped.canonical_bytes().unwrap();
    query("ALTER TABLE stateknot.child_run_join_bindings DISABLE TRIGGER child_join_bindings_immutable").execute(&pool).await.unwrap();
    query("UPDATE stateknot.child_run_join_bindings SET binding_bytes=$3,binding_checksum=sha256($3) WHERE tenant_id=$1 AND parent_run_id=$2")
        .bind(request.activation().tenant_id().as_str()).bind(*request.activation().run_id().as_uuid()).bind(&changed).execute(&pool).await.unwrap();
    query("ALTER TABLE stateknot.child_run_join_bindings ENABLE TRIGGER child_join_bindings_immutable").execute(&pool).await.unwrap();
    assert!(store.load_child_join(request.activation()).await.is_err());
    query("ALTER TABLE stateknot.child_run_join_bindings DISABLE TRIGGER child_join_bindings_immutable").execute(&pool).await.unwrap();
    query("UPDATE stateknot.child_run_join_bindings SET binding_bytes=$3,binding_checksum=sha256($3) WHERE tenant_id=$1 AND parent_run_id=$2")
        .bind(request.activation().tenant_id().as_str()).bind(*request.activation().run_id().as_uuid()).bind(&original).execute(&pool).await.unwrap();
    query("ALTER TABLE stateknot.child_run_join_bindings ENABLE TRIGGER child_join_bindings_immutable").execute(&pool).await.unwrap();
    store.load_child_join(request.activation()).await.unwrap();
    pool.close().await;
    store.close().await;
}

#[tokio::test]
async fn child_join_terminal_commit_races_registration_without_lost_wakeup() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    for _ in 0..6 {
        let (value, request, child) = setup_join(&store, "join-race").await;
        let a = request.activation();
        let (registered, terminal) = tokio::join!(
            store.register_child_join(
                request.clone(),
                &value.node,
                parent_append(&value, "child-join-registered")
            ),
            fail(&store, a.tenant_id(), child, BudgetUsage::zero())
        );
        registered.unwrap();
        terminal.unwrap();
        Box::pin(store.settle_child_run(
            value.intent.key(),
            settlement_append(&store, value.intent.key()).await,
        ))
        .await
        .unwrap();
        assert!(
            store
                .pending_child_joins_after(a.tenant_id(), None)
                .await
                .unwrap()
                .contains(&request)
        );
        store
            .publish_child_join(&request, publish_append(&store, &request).await)
            .await
            .unwrap();
        assert!(
            store
                .load_child_join(a)
                .await
                .unwrap()
                .unwrap()
                .head()
                .is_some()
        );
    }
    store.close().await;
}

fn database_url_with_name(url: &str, name: &str) -> String {
    let (prefix, current) = url.rsplit_once('/').unwrap();
    let suffix = current.find('?').map_or("", |index| &current[index..]);
    format!("{prefix}/{name}{suffix}")
}

#[tokio::test]
async fn child_join_real_child_success_is_published_from_its_own_driver_and_lease() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (value, request, child) = setup_join(&store, "join-success").await;
    let a = request.activation();
    store
        .register_child_join(
            request.clone(),
            &value.node,
            parent_append(&value, "child-join-registered"),
        )
        .await
        .unwrap();
    let child_intent = value.intent.child();
    let evidence = GraphTerminalEvidence::new(
        child_intent.descriptor().clone(),
        child_intent.request().clone(),
        child_intent.budget().clone(),
        AgentArtifacts::empty(),
        BudgetUsage::builder()
            .model_attempts(ExecutionCount::new(1))
            .model_turns(ExecutionCount::new(1))
            .input_bytes(ByteCount::new(1_024))
            .output_bytes(ByteCount::new(1_024))
            .build()
            .unwrap(),
    );
    let fixture = driver_fixture();
    let executor = DurableAgentLoop::new(
        store.clone(),
        fixture.registry,
        Arc::new(StaticLifecycleEvidence {
            terminal: evidence,
            failure: None,
        }),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let lease = store
        .claim_lease(a.tenant_id(), child, AttemptId::generate())
        .await
        .unwrap();
    let outcome = executor
        .run(lease.lease().fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(outcome.outcome(), AgentLoopOutcome::Succeeded(_)));
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 1);
    Box::pin(store.settle_child_run(
        value.intent.key(),
        settlement_append(&store, value.intent.key()).await,
    ))
    .await
    .unwrap();
    let published = store
        .publish_child_join(&request, publish_append(&store, &request).await)
        .await
        .unwrap();
    let current = store.load_run(a.tenant_id(), child).await.unwrap();
    assert_eq!(
        published.record().binding().unwrap().terminals()[0].terminal(),
        current.journal_head().unwrap()
    );
    assert_eq!(current.lifecycle().status(), RunStatus::Succeeded);
    assert!(
        store
            .load_run(a.tenant_id(), a.run_id())
            .await
            .unwrap()
            .lease()
            .is_none()
    );
    store.close().await;
}

#[tokio::test]
async fn child_join_discovery_pages_past_unknown_cost_without_losing_retained_cursor() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (unknown, unknown_request, unknown_child) = setup_join(&store, "join-pagination").await;
    let tenant = unknown_request.activation().tenant_id();
    store
        .register_child_join(
            unknown_request.clone(),
            &unknown.node,
            parent_append(&unknown, "child-join-registered"),
        )
        .await
        .unwrap();
    fail(
        &store,
        tenant,
        unknown_child,
        BudgetUsage::builder()
            .unpriced_cost_events(ExecutionCount::new(1))
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        Box::pin(store.settle_child_run(
            unknown.intent.key(),
            settlement_append(&store, unknown.intent.key()).await
        ))
        .await,
        Err(StoreError::ChildRunSettlementUnavailable)
    ));
    let mut requests = Vec::new();
    for _ in 0..17 {
        let fixture = Box::pin(setup_in_tenant(&store, tenant.clone())).await;
        let mut value = Box::pin(started_fixture(&store, fixture)).await;
        let spawned = spawn(&store, &value).await.unwrap();
        let child = spawned
            .record()
            .child()
            .admission()
            .intent()
            .provenance()
            .run_id();
        value.head = spawned.record().spawn().head();
        let request = ChildRunJoinRequest::new([value.intent.key().clone()]).unwrap();
        store
            .register_child_join(
                request.clone(),
                &value.node,
                parent_append(&value, "child-join-registered"),
            )
            .await
            .unwrap();
        settle(&store, &value, child).await;
        requests.push(request);
    }
    let page = store.pending_child_joins_after(tenant, None).await.unwrap();
    assert_eq!(page.len(), 16);
    assert!(!page.contains(&unknown_request));
    let cursor = page.last().unwrap();
    store
        .publish_child_join(cursor, publish_append(&store, cursor).await)
        .await
        .unwrap();
    let tail = store
        .pending_child_joins_after(tenant, Some(cursor))
        .await
        .unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0], requests[16]);
    assert!(
        store
            .pending_child_joins_after(tenant, tail.last())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        store
            .pending_child_joins_after(&TenantId::new("another-tenant").unwrap(), Some(cursor))
            .await,
        Err(StoreError::ChildJoinRejected)
    ));
    assert!(
        store
            .load_child_join(unknown_request.activation())
            .await
            .unwrap()
            .unwrap()
            .head()
            .is_none()
    );
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn child_join_populated_v21_upgrade_preserves_cancel_receipts_and_detects_catalog_drift() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let base = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_join_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url_with_name(&base, "postgres"))
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let url = database_url_with_name(&base, &name);
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(&url, options.clone()).await.unwrap();
    let (value, request, child) = setup_join(&store, "join-v21").await;
    let a = request.activation();
    cancellation::cancel_run(&store, a.tenant_id(), a.run_id()).await;
    // Existing v21 durable delivery remains valid and is not converted into Join.
    let child_run = store.load_run(a.tenant_id(), child).await.unwrap();
    let cancel_id = EventId::generate();
    let cancel_request = RunCancellationRequest::new(
        Failure::new(
            FailureId::generate(),
            FailureCategory::Cancelled,
            FailureCode::new("test.join.cancel").unwrap(),
            FailureOrigin::new("test.join").unwrap(),
            FailureMessage::new("cancel child").unwrap(),
            RetryAdvice::Never,
        )
        .unwrap()
        .with_caused_by_event(cancel_id),
        store.observe_database_clock().await.unwrap(),
    )
    .unwrap();
    let child_append = JournalAppend::new(
        JournalExpectation::exact(child_run.journal_head().unwrap().clone()),
        JournalEventIntent::control_plane(
            a.tenant_id().clone(),
            child,
            cancel_id,
            payload("child-run-cancellation-requested"),
        )
        .unwrap(),
    )
    .unwrap();
    Box::pin(store.deliver_child_cancellation(value.intent.key(), child_append, cancel_request))
        .await
        .unwrap();
    let before = store
        .load_child_cancellation(value.intent.key())
        .await
        .unwrap();
    store.close().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    for sql in [
        include_str!("../../../stateknot-store-postgres/tests/fixtures/revert_agent_deadlines.sql"),
        include_str!("../../../stateknot-store-postgres/tests/fixtures/revert_child_joins.sql"),
    ]
    .into_iter()
    .flat_map(|sql| sql.split(';'))
    .filter(|sql| !sql.trim().is_empty())
    {
        query(sql).execute(&pool).await.unwrap();
    }
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        21
    );
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let upgraded = PostgresStore::connect(&url, options).await.unwrap();
    assert_eq!(
        upgraded
            .load_child_cancellation(value.intent.key())
            .await
            .unwrap()
            .receipt()
            .unwrap()
            .head(),
        before.receipt().unwrap().head()
    );
    assert!(upgraded.load_child_join(a).await.unwrap().is_none());
    assert_eq!(
        query_scalar::<_, i64>("SELECT count(*) FROM stateknot.child_run_joins")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    for (relation, trigger) in [
        ("runs", "runs_child_join_guard"),
        ("child_run_ownership", "child_join_spawn_guard"),
        ("pending_node_results", "pending_results_child_join_consume"),
        ("child_run_joins", "child_joins_immutable"),
    ] {
        query(&format!(
            "ALTER TABLE stateknot.{relation} DISABLE TRIGGER {trigger}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        assert!(matches!(
            upgraded.verify_schema().await,
            Err(StoreError::IncompleteSchema)
        ));
        query(&format!(
            "ALTER TABLE stateknot.{relation} ENABLE TRIGGER {trigger}"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    let definition = query_scalar::<_, String>(
        "SELECT pg_get_functiondef('stateknot.guard_child_join_run()'::regprocedure)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    query("CREATE OR REPLACE FUNCTION stateknot.guard_child_join_run() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").execute(&pool).await.unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(&definition).execute(&pool).await.unwrap();
    let index = query_scalar::<_, String>(
        "SELECT pg_get_indexdef('stateknot.child_run_joins_pending'::regclass)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    query("DROP INDEX stateknot.child_run_joins_pending")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(&index).execute(&pool).await.unwrap();
    query("ALTER TABLE stateknot.child_run_joins ALTER COLUMN request_bytes DROP NOT NULL")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query("ALTER TABLE stateknot.child_run_joins ALTER COLUMN request_bytes SET NOT NULL")
        .execute(&pool)
        .await
        .unwrap();
    upgraded.verify_schema().await.unwrap();
    pool.close().await;
    upgraded.close().await;
    query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
