// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real standalone SQL principals: no SET ROLE impersonation of the runtime.

use super::*;
use sqlx_core::{connection::ConnectOptions, query_as::query_as, raw_sql::raw_sql};
use sqlx_postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use stateknot_core::{
    AgentAdmission, AgentAdmissionIntent, GraphFrameEntryPlan, GraphSchemaValidator,
    NodeAttemptStatus, NodeTerminalOutput, SchedulerReservationId,
};
use stateknot_store_postgres::{
    AgentAdmissionCommitOutcome, GraphFrameBarrierCommitOutcome, GraphFrameEntryCommitOutcome,
    GraphFrameReturnCommitOutcome, SchedulerFairnessPolicyRegistration,
    SchedulerFairnessRetentionPolicy,
};

const PROFILE: &str =
    include_str!("../../../stateknot-store-postgres/ops/trusted-role-profile.sql");

struct Fixture {
    admin: PgPool,
    owner: PgPool,
    runtime: PgPool,
    retention: PgPool,
    names: [String; 3],
    database: String,
    runtime_url: String,
    retention_url: String,
}

async fn pool(options: PgConnectOptions) -> PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(30))
        .connect_with(options)
        .await
        .unwrap()
}

fn options() -> PostgresStoreOptions {
    PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, 8)
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
}

impl Fixture {
    async fn create() -> Option<Self> {
        let url = match std::env::var(DATABASE_URL_ENV) {
            Ok(url) => url,
            Err(std::env::VarError::NotPresent)
                if std::env::var_os(REQUIRE_DATABASE_ENV).is_none() =>
            {
                return None;
            }
            Err(std::env::VarError::NotPresent) => panic!("mandatory PostgreSQL URL is missing"),
            Err(std::env::VarError::NotUnicode(_)) => {
                panic!("PostgreSQL URL must be valid Unicode")
            }
        };
        let connect: PgConnectOptions = url.parse().unwrap();
        // This fixture creates roles and a fresh database, never changes the
        // supplied database's schema/ACL, and refuses non-loopback targets.
        crate::commit_proxy::loopback_target(&connect);
        let connect = connect.ssl_mode(PgSslMode::Disable);
        let admin = pool(connect.clone()).await;
        assert!(
            query_scalar::<_, bool>("SELECT rolsuper FROM pg_roles WHERE rolname=current_user")
                .fetch_one(&admin)
                .await
                .unwrap(),
            "isolated fixture bootstrap requires an administrator"
        );
        let suffix = uuid::Uuid::now_v7().simple().to_string();
        let names = [
            format!("sk_owner_{suffix}"),
            format!("sk_runtime_{suffix}"),
            format!("sk_retention_{suffix}"),
        ];
        let database = format!("sk_roles_{suffix}");
        for name in &names {
            query(&format!("CREATE ROLE {name} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT PASSWORD 'profile_test_only'"))
                .execute(&admin).await.unwrap();
        }
        query(&format!("CREATE DATABASE {database} OWNER {}", names[0]))
            .execute(&admin)
            .await
            .unwrap();
        let role_options = |name: &str| {
            connect
                .clone()
                .database(&database)
                .username(name)
                .password("profile_test_only")
        };
        let owner_options = role_options(&names[0]);
        PostgresStore::migrate_database(owner_options.to_url_lossy().as_str(), options())
            .await
            .unwrap();
        let runtime_options = role_options(&names[1]);
        let retention_options = role_options(&names[2]);
        let runtime_url = runtime_options.to_url_lossy().to_string();
        let retention_url = retention_options.to_url_lossy().to_string();
        let fixture = Self {
            admin,
            owner: pool(owner_options).await,
            runtime: pool(runtime_options).await,
            retention: pool(retention_options).await,
            names,
            database,
            runtime_url,
            retention_url,
        };
        fixture.profile(true).await.unwrap();
        Some(fixture)
    }

    async fn profile(&self, apply: bool) -> Result<(), sqlx_core::error::Error> {
        let mut tx = self.owner.begin().await?;
        raw_sql("SET LOCAL search_path=pg_catalog; SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s';")
            .execute(&mut *tx).await?;
        query("SELECT set_config('stateknot.profile_runtime',$1,true),set_config('stateknot.profile_retention',$2,true),set_config('stateknot.profile_apply',$3,true)")
            .bind(&self.names[1]).bind(&self.names[2]).bind(apply.to_string()).execute(&mut *tx).await?;
        raw_sql(PROFILE).execute(&mut *tx).await?;
        tx.commit().await
    }

    async fn cleanup(self) {
        self.runtime.close().await;
        self.retention.close().await;
        self.owner.close().await;
        query(&format!("DROP DATABASE {}", self.database))
            .execute(&self.admin)
            .await
            .unwrap();
        for name in &self.names {
            query(&format!("DROP ROLE {name}"))
                .execute(&self.admin)
                .await
                .unwrap();
        }
        self.admin.close().await;
    }
}

async fn denied(pool: &PgPool, sql: &str) {
    let error = query(sql).execute(pool).await.unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("42501"),
        "{sql}: {error}"
    );
}

async fn privilege_rejections(fixture: &Fixture) {
    for (pool, name) in [
        (&fixture.runtime, &fixture.names[1]),
        (&fixture.retention, &fixture.names[2]),
    ] {
        let identity = query_as::<_, (String, String, bool)>(
            "SELECT current_user::text,session_user::text,rolsuper FROM pg_roles WHERE rolname=current_user")
            .fetch_one(pool).await.unwrap();
        assert_eq!(identity, (name.clone(), name.clone(), false));
        for sql in [
            "UPDATE public._sqlx_migrations SET checksum=checksum WHERE false",
            "UPDATE stateknot.run_events SET payload_bytes=payload_bytes WHERE false",
            "UPDATE stateknot.run_checkpoints SET checkpoint_digest=checkpoint_digest WHERE false",
            "UPDATE stateknot.tool_authorization_receipts SET receipt_bytes=receipt_bytes WHERE false",
            "UPDATE stateknot.runs SET tenant_id=tenant_id WHERE false",
            "UPDATE stateknot.graph_frame_entries SET compound_digest=compound_digest WHERE false",
            "UPDATE stateknot.graph_frame_heads SET frame_identity_digest=frame_identity_digest WHERE false",
            "UPDATE stateknot.graph_frame_stacks SET admission_digest=admission_digest WHERE false",
            "DELETE FROM stateknot.graph_frame_entries WHERE false",
            "UPDATE stateknot.graph_frame_caller_bindings SET compound_digest=compound_digest WHERE false",
            "DELETE FROM stateknot.graph_frame_caller_bindings WHERE false",
            "UPDATE stateknot.graph_frame_returns SET compound_digest=compound_digest WHERE false",
            "DELETE FROM stateknot.graph_frame_returns WHERE false",
            "DELETE FROM stateknot.run_events WHERE false",
            "DELETE FROM stateknot.tool_authorization_receipts WHERE false",
            "TRUNCATE stateknot.run_events",
            "ALTER TABLE stateknot.runs DISABLE TRIGGER ALL",
            "CREATE TABLE stateknot.unexpected (id integer)",
            "CREATE TABLE public.unexpected (id integer)",
            "CREATE TEMP TABLE unexpected (id integer)",
            "SET session_replication_role=replica",
        ] {
            denied(pool, sql).await;
        }
        denied(pool, &format!("SET ROLE {}", fixture.names[0])).await;
    }
    denied(
        &fixture.runtime,
        "DELETE FROM stateknot.scheduler_fairness_reservations WHERE false",
    )
    .await;
    denied(
        &fixture.retention,
        "SELECT * FROM stateknot.run_events LIMIT 1",
    )
    .await;
    denied(
        &fixture.retention,
        "UPDATE stateknot.scheduler_fairness_shards SET next_slot=next_slot WHERE false",
    )
    .await;
    assert!(
        PostgresStore::migrate_database(&fixture.runtime_url, options())
            .await
            .is_err()
    );
    assert!(
        PostgresStore::migrate_database(&fixture.retention_url, options())
            .await
            .is_err()
    );
}

async fn drift_is_rejected(fixture: &Fixture) {
    fixture.profile(false).await.unwrap();
    fixture.profile(true).await.unwrap();
    for sql in [
        "GRANT UPDATE ON stateknot.run_events TO PUBLIC".to_owned(),
        format!(
            "GRANT UPDATE (payload_bytes) ON stateknot.run_events TO {}",
            fixture.names[1]
        ),
        format!(
            "GRANT SELECT ON stateknot.run_events TO {} WITH GRANT OPTION",
            fixture.names[1]
        ),
        "ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO PUBLIC".to_owned(),
        format!(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA stateknot GRANT SELECT ON TABLES TO {}",
            fixture.names[1]
        ),
    ] {
        query(&sql).execute(&fixture.owner).await.unwrap();
        assert_eq!(
            fixture
                .profile(false)
                .await
                .unwrap_err()
                .as_database_error()
                .unwrap()
                .code()
                .as_deref(),
            Some("P0001")
        );
        fixture.profile(true).await.unwrap();
    }
    // NOINHERIT is not a boundary: a SET-only membership would still permit
    // privilege escalation. Both audit and apply must refuse it, not repair it.
    query(&format!(
        "GRANT {} TO {} WITH INHERIT FALSE, SET TRUE",
        fixture.names[0], fixture.names[1]
    ))
    .execute(&fixture.admin)
    .await
    .unwrap();
    assert!(fixture.profile(false).await.is_err());
    assert!(fixture.profile(true).await.is_err());
    query(&format!(
        "REVOKE {} FROM {}",
        fixture.names[0], fixture.names[1]
    ))
    .execute(&fixture.admin)
    .await
    .unwrap();
    fixture.profile(false).await.unwrap();
    // A post-grant audit failure must roll back the entire grant application.
    raw_sql(&format!("CREATE SCHEMA profile_extra; GRANT CREATE ON SCHEMA profile_extra TO {}; GRANT UPDATE ON stateknot.run_events TO PUBLIC;",fixture.names[1]))
        .execute(&fixture.owner).await.unwrap();
    assert!(fixture.profile(true).await.is_err());
    assert!(
        query_scalar::<_, bool>(
            "SELECT has_table_privilege(current_user,'stateknot.run_events','UPDATE')"
        )
        .fetch_one(&fixture.runtime)
        .await
        .unwrap()
    );
    query("DROP SCHEMA profile_extra")
        .execute(&fixture.owner)
        .await
        .unwrap();
    fixture.profile(true).await.unwrap();
    raw_sql("CREATE TABLE stateknot.future_table (id integer); CREATE FUNCTION stateknot.future_function() RETURNS integer LANGUAGE sql AS 'SELECT 1';")
        .execute(&fixture.owner).await.unwrap();
    assert!(fixture.profile(true).await.is_err());
    denied(&fixture.runtime, "SELECT * FROM stateknot.future_table").await;
    denied(&fixture.runtime, "SELECT stateknot.future_function()").await;
    raw_sql("DROP FUNCTION stateknot.future_function(); DROP TABLE stateknot.future_table;")
        .execute(&fixture.owner)
        .await
        .unwrap();
    fixture.profile(false).await.unwrap();
}

async fn runtime_failure_close(store: &PostgresStore) {
    let mut value = started(store, "role-separated-failure-close").await;
    let key = value.intent.key().clone();
    spawn(store, &value).await.unwrap();
    finish_node(store, &mut value).await;
    let saved = request(store, &value, direct_usage()).await.unwrap();
    assert!(matches!(
        request(store, &value, BudgetUsage::zero()).await.unwrap(),
        RunFailureCloseOutcome::Existing(_)
    ));
    cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    let child_usage = BudgetUsage::builder()
        .input_tokens(TokenCount::new(11))
        .build()
        .unwrap();
    cancellation::confirm(
        store,
        key.tenant_id(),
        value.intent.child().provenance().run_id(),
        child_usage.clone(),
    )
    .await
    .unwrap();
    cancellation::reconciler(store)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    tick(store, key.tenant_id()).await.items()[0]
        .result()
        .unwrap();
    let run = store
        .load_run(key.tenant_id(), key.parent_run_id())
        .await
        .unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Failed);
    assert_eq!(
        serde_json::to_value(run.lifecycle().terminal_failure().unwrap()).unwrap(),
        serde_json::to_value(saved.record().failure()).unwrap()
    );
    assert_eq!(
        run.lifecycle().terminal_usage(),
        Some(&direct_usage().checked_accumulate(&child_usage).unwrap())
    );
    assert!(tick(store, key.tenant_id()).await.items().is_empty());
}

async fn node_completion_race(store: &PostgresStore) {
    let value = started(store, "role-node-completion-race").await;
    let append = parent_append(&value, "test-node-failed");
    let failure = test_failure("test.role.race", "One durable completion.")
        .with_caused_by_event(append.intent().event_id());
    let outcomes = futures_util::future::join_all((0..24).map(|_| {
        store.fail_node_attempt(append.clone(), &value.node, failure.clone(), direct_usage())
    }))
    .await;
    let mut committed = 0;
    for outcome in outcomes {
        match outcome.unwrap() {
            NodeAttemptCommitOutcome::Committed { .. } => committed += 1,
            NodeAttemptCommitOutcome::Idempotent { .. } => {}
            _ => panic!("unexpected node completion outcome"),
        }
    }
    assert_eq!(committed, 1);
}

async fn skill_acting_window_authorization(store: &PostgresStore) {
    let value = started(store, "role-skill-acting-window").await;
    let provenance = value.fixture.parent.admission().intent().provenance();
    let tenant_id = provenance.tenant_id().clone();
    let approval = SkillActivationApproval::new(
        SkillActivationApprovalId::generate(),
        SkillActivationScope::new(
            tenant_id.clone(),
            provenance.run_id(),
            provenance.thread_id(),
        ),
        SkillAuthorizationSubject::new(
            "mcp",
            "role-profile-test",
            "skill://role-profile/SKILL.md",
            Digest::sha256(b"role profile exact Skill manifest"),
        )
        .unwrap(),
        SkillActivationSource::Direct,
        capability("role-skill-activation-policy"),
        Digest::sha256(b"role profile activation policy artifact"),
        Digest::sha256(b"role profile activation decision evidence"),
        SkillActingWindowDuration::new(DurationMillis::new(60_000).unwrap()).unwrap(),
    )
    .unwrap();
    let request = SkillActingWindowOpenRequest::new(SkillActingWindowId::generate(), approval);
    let window = store
        .open_skill_acting_window(request.clone())
        .await
        .unwrap();
    assert_eq!(
        store.open_skill_acting_window(request).await.unwrap(),
        window
    );
    store
        .assert_skill_acting_window_active(&window)
        .await
        .unwrap();
    store
        .revoke_skill_acting_window(&window, SkillActingWindowRevocationReason::Administrative)
        .await
        .unwrap();
    assert!(matches!(
        store.assert_skill_acting_window_active(&window).await,
        Err(StoreError::SkillActingWindowInactive)
    ));
}

struct RoleFrameSchemas;
impl GraphSchemaValidator for RoleFrameSchemas {
    fn validate(
        &self,
        _: &SchemaReference,
        _: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        Ok(())
    }
}

// This qualifies the compound Store transaction under the real runtime login;
// the framework call is not dispatched through an experimental graph driver.
#[allow(clippy::too_many_lines)]
async fn compound_frame_entry(fixture: &Fixture, store: &PostgresStore) {
    let wire: Value = serde_json::from_str(include_str!(
        "../../../stateknot-core/tests/fixtures/core-graph-frame-call-v1.json"
    ))
    .unwrap();
    let parent: CompiledGraph = serde_json::from_value(wire["compiled"].clone()).unwrap();
    let child: CompiledGraph = serde_json::from_value(wire["child"].clone()).unwrap();
    let tenant_id = tenant("role-frame-entry");
    for graph in [&parent, &child] {
        store
            .register_graph_definition(tenant_id.clone(), graph.clone())
            .await
            .unwrap();
    }
    let driver = driver_fixture();
    let template = durable_admission_request(
        &driver,
        tenant_id.clone(),
        AgentRunIds::generate(),
        driver.graph.output_schema().clone(),
        driver.graph.input_schema().clone(),
    );
    let template = template.intent();
    let run = template.provenance().run_id();
    let intent = AgentAdmissionIntent::new(
        template.provenance().clone(),
        template.descriptor().clone(),
        template.request().clone(),
        template.budget_layers().iter().cloned(),
        parent.reference(),
        template.authority().clone(),
    )
    .unwrap();
    let payload = JournalPayload::new(
        parent.state_schema().clone(),
        AgentAdmission::JOURNAL_EVENT_KIND.parse().unwrap(),
        BoundedJson::try_from_value(json!({"intent_digest": intent.intent_digest().to_string()}))
            .unwrap(),
    )
    .unwrap();
    let append = JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(tenant_id.clone(), run, EventId::generate(), payload)
            .unwrap(),
    )
    .unwrap();
    let write = CheckpointWrite::initial(
        tenant_id.clone(),
        run,
        CheckpointId::generate(),
        parent.reference(),
        CheckpointState::new(
            parent.state_schema().clone(),
            BoundedJson::try_from_value(json!({"count":7,"source":"parent"})).unwrap(),
        )
        .unwrap(),
        parent.entry_nodes().clone(),
    )
    .unwrap();
    let AgentAdmissionCommitOutcome::Committed(admission) =
        Box::pin(store.admit_agent_run(intent, append, write, &RoleFrameSchemas))
            .await
            .unwrap()
    else {
        panic!("new role-separated Root admission")
    };
    let lease = store
        .claim_lease(&tenant_id, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &parent.frame_calls().unwrap().calls()[0],
        &parent,
        admission.checkpoint(),
        &child,
        CheckpointId::generate(),
        AttemptId::generate(),
        lease.fence().clone(),
    )
    .unwrap();
    let usage = BudgetUsage::builder()
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
        .unwrap();
    let outcomes = futures_util::future::join_all((0..24).map(|_| {
        Box::pin(store.enter_graph_frame(
            plan.clone(),
            EventId::generate(),
            admission.event().head(),
            usage.clone(),
            &RoleFrameSchemas,
        ))
    }))
    .await;
    let mut committed = 0;
    let mut digest = None;
    for outcome in outcomes {
        let record = match outcome.unwrap() {
            GraphFrameEntryCommitOutcome::Committed(record) => {
                committed += 1;
                record
            }
            GraphFrameEntryCommitOutcome::Idempotent(record) => record,
            _ => panic!("unexpected frame entry outcome"),
        };
        assert_eq!(*digest.get_or_insert(record.digest()), record.digest());
    }
    assert_eq!(committed, 1);
    let entry = Box::pin(store.load_graph_frame_entry(&tenant_id, run, plan.frame().namespace()))
        .await
        .unwrap();
    assert_eq!(Some(entry.digest()), digest);
    assert_eq!(
        store
            .load_node_attempt(&tenant_id, &run, plan.attempt_id())
            .await
            .unwrap()
            .start(),
        entry.entry().start()
    );
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run)
            .await
            .unwrap()
            .as_ref(),
        Some(admission.checkpoint())
    );
    let counts: (i64,i64,i64) = query_as("SELECT (SELECT count(*) FROM stateknot.run_events WHERE tenant_id=$1 AND run_id=$2),(SELECT count(*) FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2),(SELECT count(*) FROM stateknot.graph_frame_entries WHERE tenant_id=$1 AND run_id=$2)")
        .bind(tenant_id.as_str()).bind(*run.as_uuid()).fetch_one(&fixture.runtime).await.unwrap();
    assert_eq!(counts, (2, 2, 1));
    Box::pin(scoped_node_completion(store, &entry, &child, lease.fence())).await;
    denied(
        &fixture.retention,
        "SELECT * FROM stateknot.graph_frame_entries LIMIT 1",
    )
    .await;
}

// Exercise the actual scoped SQL path as the standalone runtime LOGIN.
#[allow(clippy::too_many_lines)]
async fn scoped_node_completion(
    store: &PostgresStore,
    entry: &stateknot_store_postgres::StoredGraphFrameEntry,
    graph: &CompiledGraph,
    fence: &stateknot_core::RunFence,
) {
    let plan = stateknot_core::ReadyNodeRecoveryPlanner::for_frame(
        entry.entry().checkpoint().clone(),
        fence.clone(),
    )
    .unwrap()
    .finish(entry.event().head(), entry.event().recorded_at())
    .unwrap();
    let node = plan.nodes()[0].activation().node_id().clone();
    let append = worker_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        plan.journal_head().clone(),
        fence.clone(),
    );
    let started = Box::pin(store.start_recovered_graph_frame_node_attempt(
        append,
        &plan,
        &node,
        AttemptId::generate(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        started,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    let intent = stateknot_core::PendingNodeResultIntent::new(
        started.attempt().start().activation().clone(),
        NodeStateChange::Unchanged,
        NodeControl::Terminal {
            output: NodeTerminalOutput::new(
                graph.output_schema().clone(),
                entry
                    .entry()
                    .checkpoint()
                    .checkpoint()
                    .state()
                    .data()
                    .clone(),
            )
            .unwrap(),
        },
        stateknot_core::NodeInvocationBindings::empty(),
    )
    .unwrap();
    let append = worker_append(
        fence.tenant_id().clone(),
        fence.run_id(),
        EventId::generate(),
        started.event().head(),
        fence.clone(),
    );
    let done = Box::pin(store.succeed_node_attempt(
        append.clone(),
        &started.attempt().start().head(),
        intent.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(done, NodeAttemptCommitOutcome::Committed { .. }));
    let retry = Box::pin(store.succeed_node_attempt(
        append,
        &started.attempt().start().head(),
        intent.clone(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(
        store
            .load_pending_node_result(intent.activation())
            .await
            .unwrap()
            .journal_head(),
        &done.event().head()
    );
    assert_eq!(
        store
            .load_node_attempt(
                fence.tenant_id(),
                &fence.run_id(),
                started.attempt().start().attempt_id()
            )
            .await
            .unwrap()
            .status(),
        NodeAttemptStatus::Succeeded
    );
    let result = store
        .load_pending_node_result(intent.activation())
        .await
        .unwrap();
    let reducer = TestReducer {
        reference: graph.reducer().clone(),
    };
    let barrier = graph
        .plan_frame_barrier(
            entry.entry().checkpoint(),
            std::slice::from_ref(&result),
            CheckpointId::generate(),
            &RoleFrameSchemas,
            &reducer,
        )
        .unwrap();
    let GraphFrameBarrierCommitOutcome::Committed(record) =
        Box::pin(store.commit_graph_frame_barrier(
            barrier.clone(),
            EventId::generate(),
            fence.clone(),
            done.event().head(),
            entry.direct_usage_after().unwrap(),
            &RoleFrameSchemas,
            &reducer,
        ))
        .await
        .unwrap()
    else {
        panic!("standalone runtime must commit a whole barrier");
    };
    let retry = Box::pin(store.commit_graph_frame_barrier(
        barrier.clone(),
        EventId::generate(),
        fence.clone(),
        done.event().head(),
        BudgetUsage::zero(),
        &RoleFrameSchemas,
        &reducer,
    ))
    .await
    .unwrap();
    assert!(matches!(
        retry,
        GraphFrameBarrierCommitOutcome::Idempotent(_)
    ));
    assert_eq!(
        store
            .load_graph_frame_checkpoint(
                fence.tenant_id(),
                fence.run_id(),
                record.checkpoint().frame().namespace(),
                record.checkpoint().checkpoint().checkpoint_id()
            )
            .await
            .unwrap(),
        *record.checkpoint()
    );
    let next = store
        .supersede_lease(fence.tenant_id(), fence.run_id(), AttemptId::generate())
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
        record.checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        observed,
        record.direct_usage_after().unwrap(),
    ))
    .await
    .unwrap();
    assert!(matches!(
        binding,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    let retry = Box::pin(store.rebind_graph_frame_caller(
        record.checkpoint().frame(),
        AttemptId::generate(),
        EventId::generate(),
        next.clone(),
        entry.event().head(),
        BudgetUsage::zero(),
    ))
    .await
    .unwrap();
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(binding.attempt().start(), retry.attempt().start());
    assert_eq!(
        Box::pin(store.load_node_attempt(
            fence.tenant_id(),
            &fence.run_id(),
            binding.attempt().start().attempt_id()
        ))
        .await
        .unwrap()
        .start(),
        binding.attempt().start()
    );
    let usage = record
        .direct_usage_after()
        .unwrap()
        .checked_accumulate(
            &BudgetUsage::builder()
                .graph_steps(ExecutionCount::new(1))
                .retries(ExecutionCount::new(1))
                .event_bytes(ByteCount::new(
                    serde_json_canonicalizer::to_vec(binding.event())
                        .unwrap()
                        .len() as u64,
                ))
                .build()
                .unwrap(),
        )
        .unwrap();
    let GraphFrameReturnCommitOutcome::Committed(returned) = Box::pin(store.return_graph_frame(
        barrier.clone(),
        EventId::generate(),
        next.clone(),
        binding.event().head(),
        usage,
        &RoleFrameSchemas,
        &reducer,
    ))
    .await
    .unwrap() else {
        panic!("standalone runtime must settle a whole return");
    };
    assert_eq!(
        returned.completion().start(),
        &binding.attempt().start().head()
    );
    assert_eq!(
        Box::pin(store.load_graph_frame_return(
            fence.tenant_id(),
            fence.run_id(),
            record.checkpoint().frame().namespace()
        ))
        .await
        .unwrap()
        .unwrap()
        .digest(),
        returned.digest()
    );
    assert_eq!(
        Box::pin(store.load_pending_node_result(returned.result().intent().activation()))
            .await
            .unwrap(),
        *returned.result()
    );
    assert_eq!(
        Box::pin(store.load_node_attempt(
            fence.tenant_id(),
            &fence.run_id(),
            binding.attempt().start().attempt_id()
        ))
        .await
        .unwrap()
        .status(),
        NodeAttemptStatus::Succeeded
    );
    let retry = Box::pin(store.return_graph_frame(
        barrier,
        EventId::generate(),
        next,
        entry.event().head(),
        BudgetUsage::zero(),
        &RoleFrameSchemas,
        &reducer,
    ))
    .await
    .unwrap();
    let GraphFrameReturnCommitOutcome::Idempotent(retry) = retry else {
        panic!("whole return retry");
    };
    assert_eq!(retry.digest(), returned.digest());
}

async fn isolated_retention(fixture: &Fixture, runtime: &PostgresStore) {
    let retention = PostgresStore::connect(&fixture.retention_url, options())
        .await
        .unwrap();
    let shard = SchedulerShardId::new("role-profile-retention").unwrap();
    let registration =
        SchedulerFairnessPolicyRegistration::new(shard.clone(), br#"{"weights":[1]}"#, 1).unwrap();
    runtime
        .register_scheduler_fairness_policy(registration.clone())
        .await
        .unwrap();
    for _ in 0..3 {
        runtime
            .reserve_scheduler_fairness_slot(
                &shard,
                registration.policy_digest(),
                SchedulerReservationId::generate(),
            )
            .await
            .unwrap();
    }
    query("UPDATE stateknot.scheduler_fairness_reservations SET reserved_at=clock_timestamp()-interval '2 hours' WHERE sequence < 2")
        .execute(&fixture.owner).await.unwrap();
    let policy = SchedulerFairnessRetentionPolicy::new(Duration::from_secs(3600), 1).unwrap();
    assert!(matches!(
        runtime.prune_scheduler_fairness_reservations(policy).await,
        Err(StoreError::Database { .. })
    ));
    assert_eq!(
        retention
            .prune_scheduler_fairness_reservations(policy)
            .await
            .unwrap()
            .deleted(),
        1
    );
    assert_eq!(
        retention
            .prune_scheduler_fairness_reservations(policy)
            .await
            .unwrap()
            .deleted(),
        1
    );
    assert_eq!(
        retention
            .prune_scheduler_fairness_reservations(policy)
            .await
            .unwrap()
            .deleted(),
        0
    );
    assert_eq!(
        query_scalar::<_, i64>("SELECT count(*) FROM stateknot.scheduler_fairness_reservations")
            .fetch_one(&fixture.retention)
            .await
            .unwrap(),
        1
    );
    retention.close().await;
}

#[tokio::test]
async fn trusted_sql_role_profile_enforces_privileges_and_runs_durable_work() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    privilege_rejections(&fixture).await;
    drift_is_rejected(&fixture).await;
    let store = PostgresStore::connect(&fixture.runtime_url, options())
        .await
        .unwrap();
    Box::pin(runtime_failure_close(&store)).await;
    Box::pin(super::super::join::qualify_join_with_store(&store)).await;
    Box::pin(crate::qualify_agent_service_with_store(&store)).await;
    Box::pin(crate::qualify_provider_native_with_store(&store, false)).await;
    node_completion_race(&store).await;
    skill_acting_window_authorization(&store).await;
    Box::pin(compound_frame_entry(&fixture, &store)).await;
    isolated_retention(&fixture, &store).await;
    let snapshot = "SELECT jsonb_agg(jsonb_build_array(tenant_id,run_id,journal_sequence,journal_digest) ORDER BY tenant_id,run_id) FROM stateknot.runs";
    let before: Value = query_scalar(snapshot)
        .fetch_one(&fixture.owner)
        .await
        .unwrap();
    fixture.profile(true).await.unwrap();
    let after: Value = query_scalar(snapshot)
        .fetch_one(&fixture.owner)
        .await
        .unwrap();
    assert_eq!(before, after);
    fixture.profile(false).await.unwrap();
    let version: String = query_scalar("SHOW server_version")
        .fetch_one(&fixture.owner)
        .await
        .unwrap();
    let schema_version: i64 = query_scalar("SELECT max(version) FROM public._sqlx_migrations")
        .fetch_one(&fixture.owner)
        .await
        .unwrap();
    assert_eq!(schema_version, 31);
    store.close().await;
    fixture.cleanup().await;
    println!(
        "\nSTATEKNOT_ROLE_PROFILE_EVIDENCE={}",
        json!({
            "profile":"trusted-server-roles-v1","schema":schema_version,"postgres":version,
            "separate_login_connections":true,"owner_is_non_superuser":true,
            "effective_acl_audit":true,"privilege_rejections":true,"drift_rejected":true,
        "runtime_failure_close":true,"join_checkpoint_recovery":true,
        "agent_service_submission":true,"provider_native_recovery":true,
        "concurrent_submission_and_completion":24,
        "skill_acting_window_authorization":true,
        "compound_frame_entry_race":24,
        "compound_frame_entry_reload":true,
        "scoped_node_start_success_reload":true,
        "scoped_barrier_commit_retry_reload":true,
        "framework_caller_rebind_retry_reload":true,
        "whole_frame_return_retry_reload":true,
        "populated_reapply":true,
        "isolated_retention":true,"fixture_cleaned":true,
            "invariants":"passed"
        })
    );
}
