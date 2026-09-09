// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use sqlx_postgres::PgPoolOptions;

fn database_url_with_name(url: &str, name: &str) -> String {
    let (prefix, current) = url.rsplit_once('/').unwrap();
    let query = current.find('?').map_or("", |index| &current[index..]);
    format!("{prefix}/{name}{query}")
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn populated_v20_upgrade_backfills_later_audit_witness_and_checks_immutable_guards() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let base = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_cancel_upgrade_{}",
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
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let value = started(&store, "cancel-v20-upgrade").await;
    let key = value.intent.key();
    let child = spawn(&store, &value)
        .await
        .unwrap()
        .record()
        .child()
        .admission()
        .intent()
        .provenance()
        .run_id();
    // Only this isolated fixture is downgraded. Retain v20 ownership and live writer.
    for sql in [
        "DROP TRIGGER runs_child_cancellation_claim_guard ON stateknot.runs",
        "DROP FUNCTION stateknot.guard_child_cancellation_claim()",
        "DROP TRIGGER runs_child_cancellation_capture ON stateknot.runs",
        "DROP FUNCTION stateknot.capture_child_run_cancellation()",
        "DROP TABLE stateknot.child_run_cancellation_receipts",
        "DROP TABLE stateknot.child_run_cancellations",
        "DROP FUNCTION stateknot.guard_child_cancellation_evidence()",
        "DELETE FROM _sqlx_migrations WHERE version=21",
    ] {
        query(sql).execute(&pool).await.unwrap();
    }
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        20
    );
    let cancel = cancel_run(&store, key.tenant_id(), key.parent_run_id()).await;
    let audit = JournalAppend::new(
        JournalExpectation::exact(cancel.clone()),
        JournalEventIntent::control_plane(
            key.tenant_id().clone(),
            key.parent_run_id(),
            EventId::generate(),
            payload("later-parent-audit"),
        )
        .unwrap(),
    )
    .unwrap();
    let later = store
        .append_control_plane(audit, RunProjection::unchanged())
        .await
        .unwrap()
        .event()
        .head();
    store.close().await;
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let upgraded = PostgresStore::connect(&url, options).await.unwrap();
    let witness = upgraded.load_child_cancellation(key).await.unwrap();
    assert_eq!(witness.child_run_id(), child);
    assert_eq!(witness.parent_head(), &later);
    assert!(witness.parent_head().sequence() > cancel.sequence());
    assert!(witness.receipt().is_none());
    let tick = reconciler(&upgraded)
        .tick(key.tenant_id().clone(), None, CancellationSignal::never())
        .await
        .unwrap();
    assert_eq!(tick.items().len(), 1);
    assert_eq!(
        tick.items()[0].result().unwrap(),
        ChildReconciliationCommit::Cancellation(ChildCancellationOutcome::Requested)
    );
    for sql in [
        "UPDATE stateknot.child_run_cancellations SET queued_at=queued_at - interval '1 second'",
        "UPDATE stateknot.child_run_cancellations SET delivered_at=NULL",
        "DELETE FROM stateknot.child_run_cancellation_receipts",
        "UPDATE stateknot.child_run_cancellation_receipts SET outcome='terminal'",
    ] {
        let error = query(sql).execute(&pool).await.unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("SKC05")
        );
    }
    // Same trigger name with a replaced function, or disabled trigger, fails startup.
    let original = query_scalar::<_, String>(
        "SELECT pg_get_functiondef('stateknot.guard_child_cancellation_claim()'::regprocedure)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    query("CREATE OR REPLACE FUNCTION stateknot.guard_child_cancellation_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$")
        .execute(&pool).await.unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(&original).execute(&pool).await.unwrap();
    query("ALTER TABLE stateknot.runs DISABLE TRIGGER runs_child_cancellation_capture")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query("ALTER TABLE stateknot.runs ENABLE TRIGGER runs_child_cancellation_capture")
        .execute(&pool)
        .await
        .unwrap();
    upgraded.verify_schema().await.unwrap();
    let index = query_scalar::<_, String>(
        "SELECT pg_get_indexdef('stateknot.child_run_cancellations_pending'::regclass)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    query("DROP INDEX stateknot.child_run_cancellations_pending")
        .execute(&pool)
        .await
        .unwrap();
    query("CREATE INDEX child_run_cancellations_pending ON stateknot.child_run_cancellations (child_run_id) WHERE delivered_at IS NOT NULL")
        .execute(&pool).await.unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query("DROP INDEX stateknot.child_run_cancellations_pending")
        .execute(&pool)
        .await
        .unwrap();
    query(&index).execute(&pool).await.unwrap();
    query("ALTER TABLE stateknot.child_run_cancellations DROP CONSTRAINT child_run_cancellations_clock_valid")
        .execute(&pool).await.unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query("ALTER TABLE stateknot.child_run_cancellations ADD CONSTRAINT child_run_cancellations_clock_valid CHECK (delivered_at IS NULL OR delivered_at >= queued_at)")
        .execute(&pool).await.unwrap();
    upgraded.verify_schema().await.unwrap();
    // Simulated privileged corruption must not be accepted merely because the hash matches.
    query("ALTER TABLE stateknot.child_run_cancellation_receipts DISABLE TRIGGER child_cancellation_receipts_immutable").execute(&pool).await.unwrap();
    query("UPDATE stateknot.child_run_cancellation_receipts SET outcome='terminal'")
        .execute(&pool)
        .await
        .unwrap();
    query("ALTER TABLE stateknot.child_run_cancellation_receipts ENABLE TRIGGER child_cancellation_receipts_immutable").execute(&pool).await.unwrap();
    assert!(upgraded.load_child_cancellation(key).await.is_err());
    pool.close().await;
    upgraded.close().await;
    query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
