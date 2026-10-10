// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Actual atomic admission and acknowledgment-loss reload on `PostgreSQL` 16/17.
use super::*;
use stateknot_core::{ExecutionCount, GraphFrameCall, GraphFrameEntryPlan};
use stateknot_store_postgres::GraphFrameEntryCommitOutcome;

struct UnavailableFrameSchemas;
impl GraphSchemaValidator for UnavailableFrameSchemas {
    fn validate(
        &self,
        _: &SchemaReference,
        _: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        Err(GraphSchemaValidationError::Unavailable)
    }
}

pub(super) fn graphs() -> (CompiledGraph, CompiledGraph, GraphFrameCall) {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../stateknot-core/tests/fixtures/core-graph-frame-call-v1.json"
    ))
    .unwrap();
    (
        serde_json::from_value(fixture["compiled"].clone()).unwrap(),
        serde_json::from_value(fixture["child"].clone()).unwrap(),
        serde_json::from_value(fixture["call"].clone()).unwrap(),
    )
}
pub(super) async fn admit(
    store: &PostgresStore,
    tenant: &TenantId,
    run: RunId,
    parent: &CompiledGraph,
    child: &CompiledGraph,
) -> StoredAgentAdmission {
    Box::pin(admit_with_retry_limit(
        store, tenant, run, parent, child, None,
    ))
    .await
}
pub(super) async fn admit_with_retry_limit(
    store: &PostgresStore,
    tenant: &TenantId,
    run: RunId,
    parent: &CompiledGraph,
    child: &CompiledGraph,
    retries: Option<u64>,
) -> StoredAgentAdmission {
    Box::pin(admit_with_budget_override(
        store, tenant, run, parent, child, retries, None,
    ))
    .await
}
#[allow(clippy::too_many_arguments)]
pub(super) async fn admit_with_budget_override(
    store: &PostgresStore,
    tenant: &TenantId,
    run: RunId,
    parent: &CompiledGraph,
    child: &CompiledGraph,
    retries: Option<u64>,
    budget_override: Option<BudgetLimits>,
) -> StoredAgentAdmission {
    store
        .register_graph_definition(tenant.clone(), parent.clone())
        .await
        .unwrap();
    store
        .register_graph_definition(tenant.clone(), child.clone())
        .await
        .unwrap();
    let (template, _, _) = agent_admission_fixture(tenant.clone(), run);
    let intent = AgentAdmissionIntent::new(
        template.provenance().clone(),
        template.descriptor().clone(),
        template.request().clone(),
        template.budget_layers().iter().map(|layer| {
            let limits = retries.map_or_else(
                || {
                    budget_override
                        .as_ref()
                        .unwrap_or_else(|| layer.limits())
                        .clone()
                },
                |retries| {
                    layer
                        .limits()
                        .clone()
                        .with_retries(ExecutionCount::new(retries))
                },
            );
            stateknot_core::AgentAdmissionBudgetLayer::new(
                layer.source().clone(),
                layer.decision_digest(),
                limits,
            )
            .unwrap()
        }),
        parent.reference(),
        template.authority().clone(),
    )
    .unwrap();
    let payload = JournalPayload::new(
        checkpoint_schema("agent-admission-event"),
        AgentAdmission::JOURNAL_EVENT_KIND.parse().unwrap(),
        BoundedJson::try_from(json!({"intent_digest":intent.intent_digest().to_string()})).unwrap(),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(tenant.clone(), run, EventId::generate(), payload)
            .unwrap(),
    )
    .unwrap();
    let state = CheckpointState::new(
        parent.state_schema().clone(),
        BoundedJson::try_from(json!({"count":7,"source":"parent"})).unwrap(),
    )
    .unwrap();
    let write = CheckpointWrite::initial(
        tenant.clone(),
        run,
        CheckpointId::generate(),
        parent.reference(),
        state,
        parent.entry_nodes().clone(),
    )
    .unwrap();
    let AgentAdmissionCommitOutcome::Committed(admission) =
        Box::pin(store.admit_agent_run(intent, append, write, &AcceptGraphSchemas))
            .await
            .unwrap()
    else {
        panic!("new admission must commit")
    };
    admission
}
pub(super) fn initial_usage(admission: &StoredAgentAdmission) -> BudgetUsage {
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
async fn counts(pool: &PgPool, tenant: &TenantId, run: RunId) -> Vec<i64> {
    let mut result = Vec::new();
    for table in [
        "run_events",
        "run_checkpoints",
        "node_attempts",
        "run_attempt_claims",
        "graph_frame_entries",
        "graph_frame_heads",
        "graph_frame_stacks",
    ] {
        result.push(
            query_scalar(&format!(
                "SELECT count(*) FROM stateknot.{table} WHERE tenant_id=$1 AND run_id=$2"
            ))
            .bind(tenant.as_str())
            .bind(*run.as_uuid())
            .fetch_one(pool)
            .await
            .unwrap(),
        );
    }
    result
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn frame_transaction_commits_one_bundle_and_recovers_after_lease_expiry() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let tenant = tenant("frame-transaction-commit");
    let run = RunId::generate();
    let (parent, child, call) = graphs();
    let admission = Box::pin(admit(&store, &tenant, run, &parent, &child)).await;
    let original_bytes = serde_json_canonicalizer::to_vec(admission.checkpoint()).unwrap();
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &call,
        &parent,
        admission.checkpoint(),
        &child,
        CheckpointId::generate(),
        AttemptId::generate(),
        lease.fence().clone(),
    )
    .unwrap();
    assert!(matches!(
        Box::pin(store.enter_graph_frame(
            plan.clone(),
            EventId::generate(),
            admission.event().head(),
            initial_usage(&admission),
            &UnavailableFrameSchemas,
        ))
        .await,
        Err(StoreError::GraphReplayDependencyUnavailable)
    ));
    assert_eq!(
        store.load_run(&tenant, run).await.unwrap().journal_head(),
        Some(&admission.event().head())
    );
    assert!(matches!(
        store
            .load_graph_frame_entry(&tenant, run, plan.frame().namespace())
            .await,
        Err(StoreError::GraphFrameNotFound)
    ));
    // Shared Run admission bytes and depth cannot be reset even for the first
    // frame, before any ancestor entry exists.
    assert!(matches!(
        Box::pin(store.enter_graph_frame(
            plan.clone(),
            EventId::generate(),
            admission.event().head(),
            BudgetUsage::zero(),
            &AcceptGraphSchemas,
        ))
        .await,
        Err(StoreError::IncompleteChildAccounting)
    ));
    let outcome = Box::pin(store.enter_graph_frame(
        plan.clone(),
        EventId::generate(),
        admission.event().head(),
        initial_usage(&admission),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(record) = outcome else {
        panic!("new frame must commit")
    };
    assert_eq!(record.ordinal(), 1);
    assert_eq!(
        record.entry().start().journal_head(),
        &record.event().head()
    );
    assert_eq!(
        record.entry().checkpoint().checkpoint().journal_head(),
        &record.event().head()
    );
    assert_eq!(
        record.entry().checkpoint().checkpoint().state(),
        admission.checkpoint().state()
    );
    assert_eq!(
        record.entry().checkpoint().checkpoint().superstep(),
        Superstep::INITIAL
    );
    assert_ne!(record.digest(), record.entry().start().digest());
    assert_ne!(record.digest(), record.entry().checkpoint().digest());
    let loaded = Box::pin(store.load_graph_frame_entry(&tenant, run, plan.frame().namespace()))
        .await
        .unwrap();
    assert_eq!(loaded.digest(), record.digest());
    assert_eq!(loaded.entry(), record.entry());
    assert_eq!(loaded.event(), record.event());
    assert_eq!(
        store
            .load_node_attempt(&tenant, &run, plan.attempt_id())
            .await
            .unwrap()
            .start(),
        record.entry().start()
    );
    let root = store
        .load_current_checkpoint(&tenant, run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json_canonicalizer::to_vec(&root).unwrap(),
        original_bytes
    );
    // Root-only public reads cannot reinterpret a child checkpoint.
    assert!(matches!(
        store
            .load_checkpoint(&tenant, run, plan.checkpoint().checkpoint_id())
            .await,
        Err(StoreError::CheckpointNotFound)
    ));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    assert_eq!(counts(&pool, &tenant, run).await, vec![2, 2, 1, 1, 1, 1, 1]);
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert!(store.observe_database_clock().await.unwrap() >= lease.expires_at());
    let retry = GraphFrameEntryPlan::for_root(
        &call,
        &parent,
        admission.checkpoint(),
        &child,
        CheckpointId::generate(),
        AttemptId::generate(),
        lease.fence().clone(),
    )
    .unwrap();
    let GraphFrameEntryCommitOutcome::Idempotent(recovered) = Box::pin(store.enter_graph_frame(
        retry,
        EventId::generate(),
        admission.event().head(),
        initial_usage(&admission),
        &UnavailableFrameSchemas,
    ))
    .await
    .unwrap() else {
        panic!("lost acknowledgment must only reload")
    };
    assert_eq!(recovered.entry(), record.entry());
    assert_eq!(recovered.digest(), record.digest());
    assert_eq!(counts(&pool, &tenant, run).await, vec![2, 2, 1, 1, 1, 1, 1]);
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn frame_transaction_rolls_back_every_component_after_late_insert_failure() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-transaction-rollback");
    let run = RunId::generate();
    let (parent, child, call) = graphs();
    let admission = Box::pin(admit(&store, &tenant, run, &parent, &child)).await;
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &call,
        &parent,
        admission.checkpoint(),
        &child,
        CheckpointId::generate(),
        AttemptId::generate(),
        lease.fence().clone(),
    )
    .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let before = counts(&pool, &tenant, run).await;
    query("CREATE FUNCTION stateknot.test_fail_frame_head() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.tenant_id LIKE 'frame-transaction-rollback-%' THEN RAISE EXCEPTION 'injected late frame head failure'; END IF; RETURN NEW; END $$").execute(&pool).await.unwrap();
    query("CREATE TRIGGER test_fail_frame_head BEFORE INSERT ON stateknot.graph_frame_heads FOR EACH ROW EXECUTE FUNCTION stateknot.test_fail_frame_head()").execute(&pool).await.unwrap();
    let failed = Box::pin(store.enter_graph_frame(
        plan.clone(),
        EventId::generate(),
        admission.event().head(),
        initial_usage(&admission),
        &AcceptGraphSchemas,
    ))
    .await;
    query("DROP TRIGGER test_fail_frame_head ON stateknot.graph_frame_heads")
        .execute(&pool)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.test_fail_frame_head()")
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        matches!(
            failed,
            Err(StoreError::Database {
                operation: "frame initial head",
                ..
            })
        ),
        "unexpected injected result: {failed:?}"
    );
    assert_eq!(counts(&pool, &tenant, run).await, before);
    assert_eq!(
        store.load_run(&tenant, run).await.unwrap().journal_head(),
        Some(&admission.event().head())
    );
    assert_eq!(
        store
            .load_current_checkpoint(&tenant, run)
            .await
            .unwrap()
            .as_ref(),
        Some(admission.checkpoint())
    );
    assert!(matches!(
        store
            .load_node_attempt(&tenant, &run, plan.attempt_id())
            .await,
        Err(StoreError::NodeAttemptNotFound)
    ));
    assert!(matches!(
        store
            .load_graph_frame_entry(&tenant, run, plan.frame().namespace())
            .await,
        Err(StoreError::GraphFrameNotFound)
    ));
    pool.close().await;
    store.close().await;
}

pub(super) fn caller_graph(name: &str, target: &CompiledGraph, starts: u16) -> CompiledGraph {
    use stateknot_core::{GraphFrameCallPolicy, RouteId};
    let (template, _, _) = graphs();
    let identity = CapabilityIdentity::new(
        template.identity().owner().clone(),
        CapabilityReference::new(name.parse().unwrap(), Version::new(1, 0, 0)),
    );
    let call = GraphFrameCall::new(
        NodeId::new("call").unwrap(),
        NodeId::new("slot.a").unwrap(),
        target,
        RouteId::new("return").unwrap(),
    )
    .unwrap();
    CompiledGraph::compile(
        identity,
        template.input_schema().clone(),
        template.state_schema().clone(),
        template.update_schema().clone(),
        template.output_schema().clone(),
        template.reducer().clone(),
        template.entry_nodes().clone(),
        template.nodes().iter().cloned(),
        template.limits(),
    )
    .unwrap()
    .with_frame_calls(GraphFrameCallPolicy::new(7, starts, [call]).unwrap())
    .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn frame_transaction_admits_seven_real_scopes_without_resetting_root_or_usage() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-seven-scopes");
    let run = RunId::generate();
    let (_, leaf, _) = graphs();
    let mut definitions = vec![leaf];
    for level in (0..7).rev() {
        let parent = caller_graph(
            &format!("entry-level-{level}"),
            definitions.last().unwrap(),
            4096,
        );
        definitions.push(parent);
    }
    definitions.reverse();
    for compiled in &definitions {
        store
            .register_graph_definition(tenant.clone(), compiled.clone())
            .await
            .unwrap();
    }
    let admission = Box::pin(admit(
        &store,
        &tenant,
        run,
        &definitions[0],
        &definitions[1],
    ))
    .await;
    let root_bytes = serde_json_canonicalizer::to_vec(admission.checkpoint()).unwrap();
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let mut observed = admission.event().head();
    let mut usage = initial_usage(&admission);
    let mut previous = None;
    for level in 0..7 {
        let parent = &definitions[level];
        let target = &definitions[level + 1];
        let call = &parent.frame_calls().unwrap().calls()[0];
        let plan = if let Some(previous) = &previous {
            GraphFrameEntryPlan::for_frame(
                call,
                parent,
                previous,
                target,
                CheckpointId::generate(),
                AttemptId::generate(),
                lease.fence().clone(),
            )
        } else {
            GraphFrameEntryPlan::for_root(
                call,
                parent,
                admission.checkpoint(),
                target,
                CheckpointId::generate(),
                AttemptId::generate(),
                lease.fence().clone(),
            )
        }
        .unwrap();
        let GraphFrameEntryCommitOutcome::Committed(record) = Box::pin(store.enter_graph_frame(
            plan.clone(),
            EventId::generate(),
            observed,
            usage,
            &AcceptGraphSchemas,
        ))
        .await
        .unwrap() else {
            panic!("each distinct scope must commit once")
        };
        assert_eq!(record.ordinal(), u16::try_from(level + 1).unwrap());
        assert_eq!(
            record
                .entry()
                .checkpoint()
                .frame()
                .namespace()
                .as_str()
                .split('/')
                .count(),
            level + 1
        );
        assert_eq!(
            record.entry().checkpoint().checkpoint().superstep(),
            Superstep::INITIAL
        );
        assert_eq!(
            record.entry().checkpoint().checkpoint().state(),
            admission.checkpoint().state()
        );
        let reloaded = store
            .load_graph_frame_entry(&tenant, run, plan.frame().namespace())
            .await
            .unwrap();
        assert_eq!(reloaded.digest(), record.digest());
        assert_eq!(reloaded.entry(), record.entry());
        let active = Box::pin(store.load_active_graph_frame(&tenant, run))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.entry().digest(), record.digest());
        assert_eq!(active.open_frames().len(), level + 1);
        assert_eq!(
            active.open_frames().last().unwrap().checkpoint(),
            &active.checkpoint().head()
        );
        assert_eq!(active.checkpoint(), record.entry().checkpoint());
        assert_eq!(active.caller(), record.entry().start());
        assert_eq!(
            active.minimum_direct_usage(),
            &record.direct_usage_after().unwrap()
        );
        assert_eq!(
            store
                .load_node_attempt(&tenant, &run, plan.attempt_id())
                .await
                .unwrap()
                .start(),
            record.entry().start()
        );
        observed = record.event().head();
        usage = record.direct_usage_after().unwrap();
        assert_eq!(usage.graph_steps().get(), u64::try_from(level + 1).unwrap());
        assert_eq!(usage.graph_depth().get(), u64::try_from(level + 2).unwrap());
        previous = Some(record.entry().checkpoint().clone());
    }
    assert_eq!(
        serde_json_canonicalizer::to_vec(
            &store
                .load_current_checkpoint(&tenant, run)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        root_bytes
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    assert_eq!(counts(&pool, &tenant, run).await, vec![8, 8, 7, 7, 7, 7, 1]);
    let starts: i32 = query_scalar(
        "SELECT lifetime_starts FROM stateknot.graph_frame_stacks WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.as_str())
    .bind(*run.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(starts, 7);
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn frame_transaction_intersects_ancestor_start_limits_and_rejects_a_usage_reset() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-inherited-starts");
    let run = RunId::generate();
    let (_, leaf, _) = graphs();
    let inner = caller_graph("bounded-inner", &leaf, 4096);
    let middle = caller_graph("bounded-middle", &inner, 4096);
    let root = caller_graph("bounded-root", &middle, 2);
    for graph in [&root, &middle, &inner, &leaf] {
        store
            .register_graph_definition(tenant.clone(), graph.clone())
            .await
            .unwrap();
    }
    let admission = Box::pin(admit(&store, &tenant, run, &root, &middle)).await;
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &root.frame_calls().unwrap().calls()[0],
        &root,
        admission.checkpoint(),
        &middle,
        CheckpointId::generate(),
        AttemptId::generate(),
        lease.fence().clone(),
    )
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(first) = Box::pin(store.enter_graph_frame(
        plan,
        EventId::generate(),
        admission.event().head(),
        initial_usage(&admission),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap() else {
        panic!("first entry")
    };
    let plan = GraphFrameEntryPlan::for_frame(
        &middle.frame_calls().unwrap().calls()[0],
        &middle,
        first.entry().checkpoint(),
        &inner,
        CheckpointId::generate(),
        AttemptId::generate(),
        lease.fence().clone(),
    )
    .unwrap();
    assert!(matches!(
        Box::pin(store.enter_graph_frame(
            plan.clone(),
            EventId::generate(),
            first.event().head(),
            BudgetUsage::zero(),
            &AcceptGraphSchemas
        ))
        .await,
        Err(StoreError::IncompleteChildAccounting)
    ));
    let GraphFrameEntryCommitOutcome::Committed(second) = Box::pin(store.enter_graph_frame(
        plan,
        EventId::generate(),
        first.event().head(),
        first.direct_usage_after().unwrap(),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap() else {
        panic!("second entry")
    };
    let plan = GraphFrameEntryPlan::for_frame(
        &inner.frame_calls().unwrap().calls()[0],
        &inner,
        second.entry().checkpoint(),
        &leaf,
        CheckpointId::generate(),
        AttemptId::generate(),
        lease.fence().clone(),
    )
    .unwrap();
    assert!(matches!(
        Box::pin(store.enter_graph_frame(
            plan,
            EventId::generate(),
            second.event().head(),
            second.direct_usage_after().unwrap(),
            &AcceptGraphSchemas
        ))
        .await,
        Err(StoreError::GraphFrameLimitExceeded)
    ));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    assert_eq!(counts(&pool, &tenant, run).await, vec![3, 3, 2, 2, 2, 2, 1]);
    assert_eq!(
        store.load_run(&tenant, run).await.unwrap().journal_head(),
        Some(&second.event().head())
    );
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn frame_transaction_rolls_back_at_each_of_eight_durable_write_boundaries() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-fault-bundle");
    let run = RunId::generate();
    let (parent, child, call) = graphs();
    let admission = Box::pin(admit(&store, &tenant, run, &parent, &child)).await;
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &call,
        &parent,
        admission.checkpoint(),
        &child,
        CheckpointId::generate(),
        AttemptId::generate(),
        lease.fence().clone(),
    )
    .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let before = counts(&pool, &tenant, run).await;
    for (table, operation) in [
        ("run_events", "INSERT"),
        ("run_attempt_claims", "INSERT"),
        ("node_attempts", "INSERT"),
        ("run_checkpoints", "INSERT"),
        ("graph_frame_entries", "INSERT"),
        ("graph_frame_heads", "INSERT"),
        ("graph_frame_stacks", "INSERT"),
        ("runs", "UPDATE"),
    ] {
        query("CREATE FUNCTION stateknot.test_frame_bundle_boundary() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.tenant_id LIKE 'frame-fault-bundle-%' THEN RAISE EXCEPTION 'injected frame component failure'; END IF; RETURN NEW; END $$")
            .execute(&pool).await.unwrap();
        query(&format!("CREATE TRIGGER test_frame_bundle_boundary BEFORE {operation} ON stateknot.{table} FOR EACH ROW EXECUTE FUNCTION stateknot.test_frame_bundle_boundary()"))
            .execute(&pool).await.unwrap();
        let outcome = Box::pin(store.enter_graph_frame(
            plan.clone(),
            EventId::generate(),
            admission.event().head(),
            initial_usage(&admission),
            &AcceptGraphSchemas,
        ))
        .await;
        query(&format!(
            "DROP TRIGGER test_frame_bundle_boundary ON stateknot.{table}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        query("DROP FUNCTION stateknot.test_frame_bundle_boundary()")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            matches!(outcome, Err(StoreError::Database { .. })),
            "{table}: {outcome:?}"
        );
        assert_eq!(counts(&pool, &tenant, run).await, before, "{table}");
        assert_eq!(
            store.load_run(&tenant, run).await.unwrap().journal_head(),
            Some(&admission.event().head()),
            "{table}"
        );
        assert_eq!(
            store
                .load_current_checkpoint(&tenant, run)
                .await
                .unwrap()
                .as_ref(),
            Some(admission.checkpoint()),
            "{table}"
        );
        assert!(
            matches!(
                store
                    .load_node_attempt(&tenant, &run, plan.attempt_id())
                    .await,
                Err(StoreError::NodeAttemptNotFound)
            ),
            "{table}"
        );
        assert!(
            matches!(
                store
                    .load_graph_frame_entry(&tenant, run, plan.frame().namespace())
                    .await,
                Err(StoreError::GraphFrameNotFound)
            ),
            "{table}"
        );
    }
    pool.close().await;
    store.close().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn frame_entry_reload_rejects_noncanonical_duplicate_or_substituted_compound_bytes() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant = tenant("frame-corrupt-compound");
    let run = RunId::generate();
    let (parent, child, call) = graphs();
    let admission = Box::pin(admit(&store, &tenant, run, &parent, &child)).await;
    let lease = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &call,
        &parent,
        admission.checkpoint(),
        &child,
        CheckpointId::generate(),
        AttemptId::generate(),
        lease.fence().clone(),
    )
    .unwrap();
    let GraphFrameEntryCommitOutcome::Committed(record) = store
        .enter_graph_frame(
            plan.clone(),
            EventId::generate(),
            admission.event().head(),
            initial_usage(&admission),
            &AcceptGraphSchemas,
        )
        .await
        .unwrap()
    else {
        panic!("new corruption fixture")
    };
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let original: Vec<u8> = query_scalar(
        "SELECT entry_bytes FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(tenant.as_str())
    .bind(*run.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    let original_value: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let mut cases = vec![
        format!(" {}", std::str::from_utf8(&original).unwrap()).into_bytes(),
        format!(
            "{{\"version\":1,{}",
            &std::str::from_utf8(&original).unwrap()[1..]
        )
        .into_bytes(),
    ];
    for (field, value) in [
        ("unexpected", json!(true)),
        ("version", json!(2)),
        ("scope", json!([])),
        ("budget", json!([])),
        (
            "scope_intent_digest",
            json!(Digest::sha256(b"changed scope intent")),
        ),
        (
            "compound_digest",
            json!(Digest::sha256(b"changed compound digest")),
        ),
    ] {
        let mut wire = original_value.clone();
        wire[field] = value;
        cases.push(serde_json_canonicalizer::to_vec(&wire).unwrap());
    }
    for bytes in cases {
        // The migration owner simulates damaged storage, never a runtime grant.
        query("ALTER TABLE stateknot.graph_frame_entries DISABLE TRIGGER graph_frame_entries_immutable")
            .execute(&pool).await.unwrap();
        assert!(matches!(
            store.verify_schema().await,
            Err(StoreError::IncompleteSchema)
        ));
        query("UPDATE stateknot.graph_frame_entries SET entry_bytes=$3 WHERE tenant_id=$1 AND run_id=$2")
            .bind(tenant.as_str()).bind(*run.as_uuid()).bind(bytes).execute(&pool).await.unwrap();
        query("ALTER TABLE stateknot.graph_frame_entries ENABLE TRIGGER graph_frame_entries_immutable")
            .execute(&pool).await.unwrap();
        let loaded = store
            .load_graph_frame_entry(&tenant, run, plan.frame().namespace())
            .await;
        let retry = store
            .enter_graph_frame(
                plan.clone(),
                EventId::generate(),
                admission.event().head(),
                initial_usage(&admission),
                &UnavailableFrameSchemas,
            )
            .await;
        query("ALTER TABLE stateknot.graph_frame_entries DISABLE TRIGGER graph_frame_entries_immutable")
            .execute(&pool).await.unwrap();
        query("UPDATE stateknot.graph_frame_entries SET entry_bytes=$3 WHERE tenant_id=$1 AND run_id=$2")
            .bind(tenant.as_str()).bind(*run.as_uuid()).bind(&original).execute(&pool).await.unwrap();
        query("ALTER TABLE stateknot.graph_frame_entries ENABLE TRIGGER graph_frame_entries_immutable")
            .execute(&pool).await.unwrap();
        assert!(
            matches!(loaded, Err(StoreError::CorruptData { .. })),
            "{loaded:?}"
        );
        assert!(
            matches!(retry, Err(StoreError::CorruptData { .. })),
            "{retry:?}"
        );
        store.verify_schema().await.unwrap();
    }
    assert_eq!(
        store
            .load_graph_frame_entry(&tenant, run, plan.frame().namespace())
            .await
            .unwrap()
            .digest(),
        record.digest()
    );
    pool.close().await;
    store.close().await;
}
