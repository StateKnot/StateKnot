// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use sqlx_postgres::PgPoolOptions;

fn database_url(url: &str, name: &str) -> String {
    let (prefix, current) = url.rsplit_once('/').unwrap();
    let suffix = current.find('?').map_or("", |index| &current[index..]);
    format!("{prefix}/{name}{suffix}")
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn failure_close_populated_v23_upgrade_preserves_history_and_rejects_catalog_drift() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let base = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_failure_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url(&base, "postgres"))
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    let url = database_url(&base, &name);
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(&url, options.clone()).await.unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    for sql in include_str!(
        "../../../stateknot-store-postgres/tests/fixtures/revert_run_failure_closes.sql"
    )
    .split(';')
    .filter(|sql| !sql.trim().is_empty())
    {
        query(sql).execute(&pool).await.unwrap();
    }
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        23
    );
    // Retained writer exercises unchanged published v23 admission SQL, not an old executable.
    let fixture = driver_fixture();
    let tenant = tenant("failure-upgrade");
    let old = Box::pin(tree::root_admission(&store, &fixture, tenant.clone())).await;
    let run_id = old.admission().intent().provenance().run_id();
    store.close().await;
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(&url, options).await.unwrap();
    let loaded = store.load_agent_admission(&tenant, run_id).await.unwrap();
    assert_eq!(loaded.admission().digest(), old.admission().digest());
    assert_eq!(loaded.run().journal_head(), old.run().journal_head());
    assert_eq!(loaded.run().checkpoint(), old.run().checkpoint());
    assert!(
        store
            .load_run_failure_close(&tenant, run_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        query_scalar::<_, i64>("SELECT count(*) FROM stateknot.run_failure_closes")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    for (relation, trigger) in [
        ("runs", "runs_failure_close_guard"),
        ("runs", "runs_failure_close_complete"),
        ("child_run_ownership", "child_ownership_failure_close_guard"),
        ("run_failure_closes", "failure_closes_immutable"),
        ("run_failure_closes", "failure_closes_capture_children"),
    ] {
        query(&format!(
            "ALTER TABLE stateknot.{relation} DISABLE TRIGGER {trigger}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        assert!(matches!(
            store.verify_schema().await,
            Err(StoreError::IncompleteSchema)
        ));
        query(&format!(
            "ALTER TABLE stateknot.{relation} ENABLE TRIGGER {trigger}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        store.verify_schema().await.unwrap();
    }
    let original = query_scalar::<_, String>(
        "SELECT pg_get_functiondef('stateknot.guard_run_failure_close()'::regprocedure)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    query("CREATE OR REPLACE FUNCTION stateknot.guard_run_failure_close() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").execute(&pool).await.unwrap();
    assert!(matches!(
        store.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(&original).execute(&pool).await.unwrap();
    let index = query_scalar::<_, String>(
        "SELECT pg_get_indexdef('stateknot.run_failure_closes_pending'::regclass)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    query("DROP INDEX stateknot.run_failure_closes_pending")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(&index).execute(&pool).await.unwrap();
    store.verify_schema().await.unwrap();
    let fence = store
        .claim_lease(&tenant, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let append = JournalAppend::new(
        JournalExpectation::exact(old.event().head()),
        JournalEventIntent::worker(
            tenant.clone(),
            run_id,
            EventId::generate(),
            fence,
            payload("run-failure-close-requested"),
        )
        .unwrap(),
    )
    .unwrap();
    store
        .request_run_failure_close(
            &old.checkpoint().head(),
            test_failure("upgrade.failure", "Upgrade preserves the source failure."),
            BudgetUsage::zero(),
            append,
        )
        .await
        .unwrap();
    tick(&store, &tenant).await.items()[0].result().unwrap();
    assert_eq!(
        store
            .load_run(&tenant, run_id)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Failed
    );
    store.close().await;
    pool.close().await;
    query(&format!("DROP DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
