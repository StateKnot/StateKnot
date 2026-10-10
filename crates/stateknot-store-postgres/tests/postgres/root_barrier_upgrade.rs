// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Populated source-schema reconstruction, not a retained historical executable.

use super::*;
use sqlx_core::connection::ConnectOptions;
use sqlx_postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgSslMode};
use stateknot_core::GraphFrameEntryPlan;
use stateknot_store_postgres::GraphFrameEntryCommitOutcome;

async fn table_fingerprints(pool: &PgPool) -> Vec<(String, i64, String)> {
    let names = query_scalar::<_, String>(
        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='stateknot' AND tablename<>'root_graph_barriers' ORDER BY tablename",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    let mut records = Vec::new();
    for name in names {
        // Names come from this isolated schema's catalog, not application input.
        let identifier = name.replace('"', "\"\"");
        let sql = format!(
            "SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(row_hash,'' ORDER BY row_hash),''),'UTF8')),'hex') FROM (SELECT encode(sha256(convert_to(row_to_json(t)::text,'UTF8')),'hex') row_hash FROM stateknot.\"{identifier}\" t) rows"
        );
        let (count, digest) = query_as::<_, (i64, String)>(&sql)
            .fetch_one(pool)
            .await
            .unwrap();
        records.push((name, count, digest));
    }
    records
}

async fn original_migrations(pool: &PgPool) -> Vec<(i64, Vec<u8>, bool)> {
    query_as("SELECT version,checksum,success FROM public._sqlx_migrations WHERE version<=33 ORDER BY version")
        .fetch_all(pool)
        .await
        .unwrap()
}

async fn verify_position_index(store: &PostgresStore, pool: &PgPool) {
    query("ALTER INDEX stateknot.root_graph_barriers_position_unique RENAME TO root_graph_barriers_wrong_position")
        .execute(pool).await.unwrap();
    assert!(matches!(
        store.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query("ALTER INDEX stateknot.root_graph_barriers_wrong_position RENAME TO root_graph_barriers_position_unique")
        .execute(pool).await.unwrap();
    store.verify_schema().await.unwrap();
}

async fn verify_catalog_tamper(store: &PostgresStore, pool: &PgPool) {
    for (table, trigger) in [
        ("root_graph_barriers", "root_graph_barriers_immutable"),
        ("root_graph_barriers", "root_barriers_complete"),
        ("run_events", "run_events_root_barrier_complete"),
        ("runs", "runs_root_barrier_projection"),
        ("run_wait_registrations", "root_wait_components_complete"),
        (
            "pending_node_result_consumptions",
            "root_result_components_complete",
        ),
    ] {
        query(&format!(
            "ALTER TABLE stateknot.{table} DISABLE TRIGGER {trigger}"
        ))
        .execute(pool)
        .await
        .unwrap();
        assert!(matches!(
            store.verify_schema().await,
            Err(StoreError::IncompleteSchema)
        ));
        query(&format!(
            "ALTER TABLE stateknot.{table} ENABLE TRIGGER {trigger}"
        ))
        .execute(pool)
        .await
        .unwrap();
        store.verify_schema().await.unwrap();
    }

    for function in [
        "guard_root_graph_barrier_complete",
        "guard_root_graph_barrier_event_complete",
        "guard_root_graph_barrier_projection",
        "guard_root_graph_barrier_component_complete",
    ] {
        let definition =
            query_scalar::<_, String>("SELECT pg_get_functiondef(to_regprocedure($1))")
                .bind(format!("stateknot.{function}()"))
                .fetch_one(pool)
                .await
                .unwrap();
        query(&format!("CREATE OR REPLACE FUNCTION stateknot.{function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$"))
            .execute(pool)
            .await
            .unwrap();
        assert!(matches!(
            store.verify_schema().await,
            Err(StoreError::IncompleteSchema)
        ));
        query(&definition).execute(pool).await.unwrap();
        store.verify_schema().await.unwrap();
    }

    let name = "root_graph_barriers_base_fk";
    let definition = query_scalar::<_, String>(
        "SELECT pg_get_constraintdef(oid) FROM pg_catalog.pg_constraint WHERE conrelid='stateknot.root_graph_barriers'::regclass AND conname=$1",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .unwrap();
    query(&format!(
        "ALTER TABLE stateknot.root_graph_barriers DROP CONSTRAINT {name}"
    ))
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        store.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(&format!(
        "ALTER TABLE stateknot.root_graph_barriers ADD CONSTRAINT {name} {definition}"
    ))
    .execute(pool)
    .await
    .unwrap();
    store.verify_schema().await.unwrap();

    verify_position_index(store, pool).await;

    // Keep the real generated expression but alter its catalog nullability.
    // Restore the same column in place so attnum, FKs and expression stay exact.
    query("ALTER TABLE stateknot.root_graph_barriers ALTER COLUMN bundle_checksum SET NOT NULL")
        .execute(pool)
        .await
        .unwrap();
    assert!(matches!(
        store.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query("ALTER TABLE stateknot.root_graph_barriers ALTER COLUMN bundle_checksum DROP NOT NULL")
        .execute(pool)
        .await
        .unwrap();
    store.verify_schema().await.unwrap();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn populated_source33_upgrade_preserves_facts_without_disposition_backfill() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let base: PgConnectOptions = std::env::var(DATABASE_URL_ENV).unwrap().parse().unwrap();
    assert!(
        matches!(base.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "this fixture only creates databases on a loopback test service"
    );
    let base = base.ssl_mode(PgSslMode::Disable);
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(base.clone().database("postgres"))
        .await
        .unwrap();
    let name = format!(
        "stateknot_root_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let connect = base.database(&name);
    let url = connect.to_url_lossy();
    PostgresStore::migrate_database(url.as_str(), options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(url.as_str(), options.clone())
        .await
        .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(connect)
        .await
        .unwrap();
    query("SET TimeZone='UTC'").execute(&pool).await.unwrap();

    let tenant = tenant("root-disposition-upgrade");
    let run = RunId::generate();
    let (parent, child, call) = super::frame_transactions::graphs();
    let admission = Box::pin(super::frame_transactions::admit(
        &store, &tenant, run, &parent, &child,
    ))
    .await;
    let fence = store
        .claim_lease(&tenant, run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let plan = GraphFrameEntryPlan::for_root(
        &call,
        &parent,
        admission.checkpoint(),
        &child,
        CheckpointId::generate(),
        AttemptId::generate(),
        fence,
    )
    .unwrap();
    let namespace = plan.frame().namespace().clone();
    let GraphFrameEntryCommitOutcome::Committed(entry) = Box::pin(store.enter_graph_frame(
        plan,
        EventId::generate(),
        admission.event().head(),
        super::frame_transactions::initial_usage(&admission),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap() else {
        panic!("fresh entry must commit");
    };
    let before = table_fingerprints(&pool).await;
    assert_eq!(before.len(), 52);
    assert!(
        before
            .iter()
            .any(|(table, count, _)| table == "graph_frame_entries" && *count == 1)
    );
    let migrations = original_migrations(&pool).await;
    assert_eq!(
        migrations
            .iter()
            .map(|(version, _, _)| *version)
            .collect::<Vec<_>>(),
        (1..=33).collect::<Vec<_>>()
    );
    assert!(migrations.iter().all(|(_, _, success)| *success));
    store.close().await;

    // Only migration34's empty objects are removed. All original canonical
    // facts and migration checksums stay untouched. This is source-schema
    // reconstruction and does not pretend to run a published N-1/N-2 binary.
    sqlx_core::raw_sql::raw_sql(
        "BEGIN;
         DROP TRIGGER run_events_root_barrier_complete ON stateknot.run_events;
         DROP TRIGGER runs_root_barrier_projection ON stateknot.runs;
         DROP TRIGGER root_wait_components_complete ON stateknot.run_wait_registrations;
         DROP TRIGGER root_result_components_complete ON stateknot.pending_node_result_consumptions;
         DROP TABLE stateknot.root_graph_barriers;
         DROP FUNCTION stateknot.guard_root_graph_barrier_complete();
         DROP FUNCTION stateknot.guard_root_graph_barrier_event_complete();
         DROP FUNCTION stateknot.guard_root_graph_barrier_projection();
         DROP FUNCTION stateknot.guard_root_graph_barrier_component_complete();
         DELETE FROM public._sqlx_migrations WHERE version=34;
         COMMIT;",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(original_migrations(&pool).await, migrations);
    assert_eq!(table_fingerprints(&pool).await, before);
    assert!(matches!(
        PostgresStore::connect(url.as_str(), options.clone()).await,
        Err(StoreError::IncompatibleSchema)
    ));
    PostgresStore::migrate_database(url.as_str(), options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(url.as_str(), options).await.unwrap();
    store.verify_schema().await.unwrap();
    assert_eq!(original_migrations(&pool).await, migrations);
    assert_eq!(table_fingerprints(&pool).await, before);
    assert_eq!(
        query_scalar::<_, i64>("SELECT count(*) FROM stateknot.root_graph_barriers")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );

    let restored_admission = store.load_agent_admission(&tenant, run).await.unwrap();
    assert_eq!(
        restored_admission.admission().digest(),
        admission.admission().digest()
    );
    let restored_entry = Box::pin(store.load_graph_frame_entry(&tenant, run, &namespace))
        .await
        .unwrap();
    assert_eq!(restored_entry.digest(), entry.digest());
    assert_eq!(restored_entry.event(), entry.event());
    assert_eq!(restored_entry.entry(), entry.entry());
    assert!(
        store
            .load_root_graph_barrier(&admission.checkpoint().head())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(table_fingerprints(&pool).await, before);
    verify_catalog_tamper(&store, &pool).await;
    assert_eq!(table_fingerprints(&pool).await, before);
    assert_eq!(original_migrations(&pool).await, migrations);
    store.close().await;
    pool.close().await;
    query(&format!("DROP DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
