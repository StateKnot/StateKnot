// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Interrupt the deployment migrator while its `SQLx` ledger `INSERT` or `UPDATE` is blocked.

use super::*;
use sqlx_core::{connection::ConnectOptions, raw_sql::raw_sql};
use sqlx_postgres::{PgConnectOptions, PgSslMode};
use std::path::Path;

#[derive(Clone, Copy)]
enum Phase {
    BeforeCommit,
    AfterCommit,
}

async fn populate_schema38(db: &PgPool) {
    // Restore the frozen schema35 writer's two Runs on its reconstructed
    // schema32, then apply the original migrations through 38.
    let source = Migrator::new(Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"))
        .await
        .unwrap();
    let through = |version| Migrator {
        migrations: Cow::Owned(
            source
                .iter()
                .take_while(|m| m.version <= version)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    through(32).run(db).await.unwrap();
    let mut tx = db.begin().await.unwrap();
    query("DELETE FROM public._sqlx_migrations")
        .execute(&mut *tx)
        .await
        .unwrap();
    raw_sql(include_str!(
        "../fixtures/actual_schema35_writer_source32.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    // The dump sets session-level GUCs. Keep its data, restore the test session.
    raw_sql("RESET ALL").execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    through(38).run(db).await.unwrap();
}

async fn original_facts(db: &PgPool) -> (Vec<String>, Vec<String>) {
    let completions = query_scalar(
        "SELECT (to_jsonb(c) - 'tool_charges' - 'failure_projection_digest')::text
         FROM stateknot.node_attempt_completions c ORDER BY tenant_id, run_id, attempt_id",
    )
    .fetch_all(db)
    .await
    .unwrap();
    let journal = query_scalar(
        "SELECT to_jsonb(e)::text FROM stateknot.run_events e ORDER BY tenant_id, run_id, sequence",
    )
    .fetch_all(db)
    .await
    .unwrap();
    (completions, journal)
}

async fn migration_ledger(db: &PgPool) -> Vec<String> {
    query_scalar("SELECT to_jsonb(m)::text FROM public._sqlx_migrations m ORDER BY version")
        .fetch_all(db)
        .await
        .unwrap()
}

async fn schema39_is_durable(db: &PgPool, expected: bool) {
    let objects: (bool, bool, bool, bool) = query_as(
        "SELECT to_regclass('stateknot.run_events_sequence_projection_unique') IS NOT NULL,
                to_regclass('stateknot.node_attempts_charged_activation') IS NOT NULL,
                EXISTS (SELECT FROM pg_attribute WHERE attrelid='stateknot.node_attempt_completions'::regclass
                        AND attname='tool_charges' AND NOT attisdropped),
                EXISTS (SELECT FROM pg_attribute WHERE attrelid='stateknot.node_attempt_completions'::regclass
                        AND attname='failure_projection_digest' AND attgenerated='s' AND NOT attisdropped)"
    ).fetch_one(db).await.unwrap();
    assert_eq!(objects, (expected, expected, expected, expected));
    if expected {
        let valid: bool = query_scalar(
            "SELECT bool_and(status='succeeded' AND tool_charges IS NULL AND failure_projection_digest IS NULL)
             FROM stateknot.node_attempt_completions"
        ).fetch_one(db).await.unwrap();
        assert!(
            valid,
            "the three legacy successes must not acquire a failure receipt"
        );
    }
}

async fn interrupt_deployment(db: &PgPool, url: &str, phase: Phase) {
    let (operation, statement) = match phase {
        Phase::BeforeCommit => ("INSERT", "INSERT INTO _sqlx_migrations"),
        Phase::AfterCommit => ("UPDATE OF execution_time", "UPDATE _sqlx_migrations"),
    };
    raw_sql(&format!(
        "CREATE FUNCTION public.pause_migration39() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_advisory_xact_lock(39, 1); RETURN NEW; END $$;
         CREATE TRIGGER pause_migration39 BEFORE {operation} ON public._sqlx_migrations
         FOR EACH ROW WHEN (NEW.version = 39) EXECUTE FUNCTION public.pause_migration39()"
    ))
    .execute(db)
    .await
    .unwrap();
    let mut gate = db.begin().await.unwrap();
    query("SELECT pg_advisory_xact_lock(39, 1)")
        .execute(&mut *gate)
        .await
        .unwrap();
    let blocker: i32 = query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    let interrupt = async {
        // The lock is database-local. Signal only a unique StateKnot migration
        // waiting on this controller, this lock, and the exact ledger operation.
        loop {
            let killed: Option<bool> = query_scalar(
                "WITH target AS MATERIALIZED (
                   SELECT a.pid, count(*) OVER () AS candidates
                   FROM pg_stat_activity a JOIN pg_locks l USING (pid)
                   WHERE a.datname=current_database() AND a.usename=current_user
                     AND a.application_name='stateknot' AND a.backend_type='client backend'
                     AND a.wait_event_type='Lock' AND a.wait_event='advisory'
                     AND strpos(a.query, $2)>0 AND $1=ANY(pg_blocking_pids(a.pid))
                     AND l.locktype='advisory' AND NOT l.granted AND l.classid=39
                     AND l.objid=1 AND l.objsubid=2
                 ) SELECT pg_terminate_backend(pid) FROM target WHERE candidates=1",
            )
            .bind(blocker)
            .bind(statement)
            .fetch_optional(db)
            .await
            .unwrap();
            if killed == Some(true) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(
            PostgresStore::migrate_database(url, test_options(Duration::from_secs(30))),
            interrupt
        )
    })
    .await
    .expect("schema39 must reach the chosen ledger boundary and lose its connection");
    assert!(
        matches!(result, Err(StoreError::Migration { .. })),
        "{result:?}"
    );
    gate.rollback().await.unwrap();
    raw_sql("DROP TRIGGER pause_migration39 ON public._sqlx_migrations; DROP FUNCTION public.pause_migration39()")
        .execute(db).await.unwrap();
}

async fn verify_case(db: &PgPool, url: &str, phase: Phase) {
    populate_schema38(db).await;
    let initial = migration_ledger(db).await;
    assert_eq!(initial.len(), 38);
    schema39_is_durable(db, false).await;
    let facts = original_facts(db).await;
    assert_eq!(facts.0.len(), 3);
    assert!(!facts.1.is_empty());

    interrupt_deployment(db, url, phase).await;
    let after = migration_ledger(db).await;
    let committed = matches!(phase, Phase::AfterCommit);
    assert_eq!(after.len(), if committed { 39 } else { 38 });
    assert_eq!(&after[..38], initial.as_slice());
    schema39_is_durable(db, committed).await;
    assert_eq!(original_facts(db).await, facts);
    let options = test_options(Duration::from_secs(30));
    let runtime = PostgresStore::connect(url, options.clone()).await;
    match phase {
        Phase::BeforeCommit => assert!(matches!(runtime, Err(StoreError::IncompatibleSchema))),
        Phase::AfterCommit => {
            runtime.unwrap().close().await;
            let telemetry: (bool, i64) = query_as(
                "SELECT success,execution_time FROM public._sqlx_migrations WHERE version=39",
            )
            .fetch_one(db)
            .await
            .unwrap();
            assert_eq!(telemetry, (true, -1));
        }
    }
    PostgresStore::migrate_database(url, options.clone())
        .await
        .unwrap();
    let recovered = migration_ledger(db).await;
    assert_eq!(recovered.len(), 39);
    assert_eq!(&recovered[..after.len()], after.as_slice());
    schema39_is_durable(db, true).await;
    assert_eq!(original_facts(db).await, facts);
    let expected: serde_json::Value = serde_json::from_str(include_str!(
        "../fixtures/actual_schema35_writer_source32.json"
    ))
    .unwrap();
    let store = PostgresStore::connect(url, options.clone()).await.unwrap();
    for key in ["fence", "prior_fence"] {
        let fence: stateknot_core::RunFence =
            serde_json::from_value(expected[key].clone()).unwrap();
        store
            .load_run(fence.tenant_id(), fence.run_id())
            .await
            .unwrap();
    }
    store.close().await;
    PostgresStore::migrate_database(url, options).await.unwrap();
    assert_eq!(migration_ledger(db).await, recovered);
}

async fn qualify(phase: Phase) {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let original = match std::env::var(DATABASE_URL_ENV) {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_none() => {
            return;
        }
        _ => panic!("mandatory PostgreSQL test URL is missing or invalid"),
    };
    let base: PgConnectOptions = original.parse().unwrap();
    assert!(matches!(base.get_host(), "localhost" | "127.0.0.1" | "::1"));
    let base = base.ssl_mode(PgSslMode::Disable);
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(base.clone().database("postgres"))
        .await
        .unwrap();
    let name = format!("stateknot_mig39_{}", Uuid::now_v7().simple());
    query(&format!("CREATE DATABASE {name} TEMPLATE template0"))
        .execute(&admin)
        .await
        .unwrap();
    let connect = base.database(&name);
    let local = tokio::task::LocalSet::new();
    let task = local.spawn_local(async move {
        let url = connect.to_url_lossy().to_string();
        let db = PgPoolOptions::new()
            .max_connections(3)
            .connect_with(connect)
            .await
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(180),
            Box::pin(verify_case(&db, &url, phase)),
        )
        .await
        .expect("the isolated migration, connection loss and cold retries must finish");
        db.close().await;
    });
    let outcome = local.run_until(task).await;
    // The name is generated here and CREATE succeeded, even if a test assertion
    // panics. Only this invocation's disposable database is removed.
    query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    outcome.unwrap();
}

#[tokio::test]
async fn interrupted_schema39_before_commit_restores_schema38() {
    Box::pin(qualify(Phase::BeforeCommit)).await;
}

#[tokio::test]
async fn interrupted_schema39_after_commit_recovers_without_reapplying() {
    Box::pin(qualify(Phase::AfterCommit)).await;
}
