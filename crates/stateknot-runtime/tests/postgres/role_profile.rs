// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real standalone SQL principals: no SET ROLE impersonation of the runtime.

use super::*;
use sqlx_core::{connection::ConnectOptions, query_as::query_as, raw_sql::raw_sql};
use sqlx_postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use stateknot_core::SchedulerReservationId;
use stateknot_store_postgres::{
    SchedulerFairnessPolicyRegistration, SchedulerFairnessRetentionPolicy,
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
            "UPDATE stateknot.runs SET tenant_id=tenant_id WHERE false",
            "DELETE FROM stateknot.run_events WHERE false",
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
    Box::pin(crate::qualify_provider_native_with_store(&store)).await;
    node_completion_race(&store).await;
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
    store.close().await;
    fixture.cleanup().await;
    println!(
        "\nSTATEKNOT_ROLE_PROFILE_EVIDENCE={}",
        json!({
            "profile":"trusted-server-roles-v1","schema":24,"postgres":version,
            "separate_login_connections":true,"owner_is_non_superuser":true,
            "effective_acl_audit":true,"privilege_rejections":true,"drift_rejected":true,
        "runtime_failure_close":true,"join_checkpoint_recovery":true,
        "agent_service_submission":true,"provider_native_recovery":true,
        "concurrent_submission_and_completion":24,
        "populated_reapply":true,
        "isolated_retention":true,"fixture_cleaned":true,
            "invariants":"passed"
        })
    );
}
