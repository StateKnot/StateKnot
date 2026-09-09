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
async fn deadline_populated_v22_upgrade_preserves_history_and_verifies_exact_projection_guards() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    let options = shared.options().clone();
    shared.close().await;
    let base = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_deadline_upgrade_{}",
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
    for fixture in [
        include_str!(
            "../../../stateknot-store-postgres/tests/fixtures/revert_run_failure_closes.sql"
        ),
        include_str!("../../../stateknot-store-postgres/tests/fixtures/revert_agent_deadlines.sql"),
    ] {
        for sql in fixture.split(';').filter(|sql| !sql.trim().is_empty()) {
            query(sql).execute(&pool).await.unwrap();
        }
    }
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        22
    );
    // The retained connection uses the unchanged v22 admission SQL. This is a
    // populated SQL-compatibility test, not execution of an old binary artifact.
    let fixture = driver_fixture();
    let tenant = tenant("deadline-v22");
    let deadline = future_deadline(&store, 2).await;
    let old = admit_deadline(&store, &fixture, tenant.clone(), deadline).await;
    let run = old.admission().intent().provenance().run_id();
    store.close().await;
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let upgraded = PostgresStore::connect(&url, options).await.unwrap();
    let loaded = upgraded.load_agent_admission(&tenant, run).await.unwrap();
    assert_eq!(loaded.admission().digest(), old.admission().digest());
    assert_eq!(loaded.run().journal_head(), old.run().journal_head());
    assert_eq!(loaded.run().checkpoint(), old.run().checkpoint());
    let later = admit_deadline(
        &upgraded,
        &fixture,
        tenant.clone(),
        "2099-01-01T00:00:00.000000Z".parse().unwrap(),
    )
    .await;
    assert!(matches!(
        request_expiry(
            &upgraded,
            &tenant,
            later.admission().intent().provenance().run_id()
        )
        .await
        .unwrap(),
        AgentDeadlineCancellationOutcome::NotDue { .. }
    ));
    for sql in [
        "UPDATE stateknot.runs SET agent_deadline_at=NULL WHERE tenant_id=$1 AND run_id=$2",
        "UPDATE stateknot.runs SET agent_deadline_at=agent_deadline_at+interval '1 second' WHERE tenant_id=$1 AND run_id=$2",
    ] {
        assert!(
            query(sql)
                .bind(tenant.as_str())
                .bind(*run.as_uuid())
                .execute(&pool)
                .await
                .is_err()
        );
    }
    for (table, trigger) in [
        ("runs", "runs_agent_deadline_guard"),
        ("agent_admissions", "agent_admissions_deadline_capture"),
    ] {
        query(&format!(
            "ALTER TABLE stateknot.{table} DISABLE TRIGGER {trigger}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        assert!(matches!(
            upgraded.verify_schema().await,
            Err(StoreError::IncompleteSchema)
        ));
        query(&format!(
            "ALTER TABLE stateknot.{table} ENABLE TRIGGER {trigger}"
        ))
        .execute(&pool)
        .await
        .unwrap();
    }
    let original = query_scalar::<_, String>(
        "SELECT pg_get_functiondef('stateknot.guard_agent_deadline()'::regprocedure)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    query("CREATE OR REPLACE FUNCTION stateknot.guard_agent_deadline() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").execute(&pool).await.unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(&original).execute(&pool).await.unwrap();
    let index = query_scalar::<_, String>(
        "SELECT pg_get_indexdef('stateknot.runs_due_agent_deadlines'::regclass)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    query("DROP INDEX stateknot.runs_due_agent_deadlines")
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(&index).execute(&pool).await.unwrap();
    upgraded.verify_schema().await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    query("SET LOCAL enable_seqscan=off")
        .execute(&mut *tx)
        .await
        .unwrap();
    let plan=query_scalar::<_,Value>("EXPLAIN (FORMAT JSON) SELECT agent_deadline_at,run_id FROM stateknot.runs WHERE tenant_id=$1 AND agent_deadline_at IS NOT NULL AND lifecycle_status IN ('pending','active','waiting') AND agent_deadline_at <= statement_timestamp() ORDER BY agent_deadline_at,run_id LIMIT 16")
        .bind(tenant.as_str()).fetch_one(&mut *tx).await.unwrap();
    assert!(plan.to_string().contains("runs_due_agent_deadlines"));
    tx.rollback().await.unwrap();
    await_due(&upgraded, deadline).await;
    let candidates = upgraded
        .due_agent_deadlines_after(&tenant, None)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].deadline(), deadline);
    assert_eq!(candidates[0].run_id(), run);
    assert!(matches!(
        request_expiry(&upgraded, &tenant, run).await.unwrap(),
        AgentDeadlineCancellationOutcome::Requested(_)
    ));
    upgraded.close().await;
    pool.close().await;
    query(&format!("DROP DATABASE {name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
