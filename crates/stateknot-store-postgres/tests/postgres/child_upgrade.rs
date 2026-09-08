// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn migration_twenty_preserves_v19_admissions_and_fences_existing_declared_graphs() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    shared.close().await;
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_v20_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url_with_name(&database_url, "postgres"))
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {name}"))
        .execute(&administration)
        .await
        .unwrap();
    let url = database_url_with_name(&database_url, &name);
    let options = test_options(Duration::from_secs(30));
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let store = PostgresStore::connect(&url, options.clone()).await.unwrap();
    let ordinary_tenant = tenant("v20-ordinary");
    let ordinary_id = RunId::generate();
    let ordinary = Box::pin(admit_atomic_agent_fixture(
        &store,
        &ordinary_tenant,
        ordinary_id,
    ))
    .await;
    let declared_tenant = tenant("v20-declared");
    let declared_id = RunId::generate();
    let (template, append, checkpoint) =
        agent_admission_fixture(declared_tenant.clone(), declared_id);
    let child_graph = checkpoint_compiled_graph();
    let child_agent = AgentDescriptor::new(
        template.descriptor().metadata().clone(),
        child_graph.input_schema().clone(),
        child_graph.output_schema().clone(),
        template.descriptor().model().clone(),
        template.descriptor().instructions().clone(),
        template.descriptor().tools().clone(),
        template.descriptor().execution().clone(),
        template.descriptor().budget_limits().clone(),
    )
    .unwrap();
    let policy = stateknot_core::GraphChildRunPolicy::new(
        stateknot_core::ChildRunTopologyLimits::new(1, 8, 4).unwrap(),
        [stateknot_core::ChildRunDeclaration::new(
            child_graph.entry_nodes().iter().next().unwrap().clone(),
            stateknot_core::ChildRunSlot::new("analysis").unwrap(),
            &child_agent,
            &child_graph,
        )
        .unwrap()],
    )
    .unwrap();
    let graph = child_graph.with_child_runs(policy).unwrap();
    let intent = AgentAdmissionIntent::new(
        template.provenance().clone(),
        template.descriptor().clone(),
        template.request().clone(),
        template.budget_layers().to_vec(),
        graph.reference(),
        template.authority().clone(),
    )
    .unwrap();
    let checkpoint = CheckpointWrite::initial(
        declared_tenant.clone(),
        declared_id,
        checkpoint.checkpoint_id(),
        graph.reference(),
        checkpoint.state().clone(),
        graph.entry_nodes().clone(),
    )
    .unwrap();
    store
        .register_graph_definition(declared_tenant.clone(), graph)
        .await
        .unwrap();
    let declared = Box::pin(store.admit_agent_run(intent, append, checkpoint, &AcceptGraphSchemas))
        .await
        .unwrap()
        .stored()
        .clone();
    store.close().await;
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    remove_child_run_ownership(&fixture).await;
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        19
    );
    fixture.close().await;
    PostgresStore::migrate_database(&url, options.clone())
        .await
        .unwrap();
    let upgraded = PostgresStore::connect(&url, options).await.unwrap();
    assert_eq!(
        upgraded
            .load_agent_admission(&ordinary_tenant, ordinary_id)
            .await
            .unwrap()
            .admission(),
        ordinary.admission()
    );
    assert_eq!(
        upgraded
            .load_agent_admission(&declared_tenant, declared_id)
            .await
            .unwrap()
            .admission(),
        declared.admission()
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    for (tenant, id, expected) in [
        (&ordinary_tenant, ordinary_id, 0_i16),
        (&declared_tenant, declared_id, 1_i16),
    ] {
        assert_eq!(
            query_scalar::<_, i16>(
                "SELECT child_runtime_version FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2"
            )
            .bind(tenant.as_str())
            .bind(*id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap(),
            expected
        );
    }
    let old =
        query("UPDATE stateknot.runs SET updated_at=updated_at WHERE tenant_id=$1 AND run_id=$2")
            .bind(declared_tenant.as_str())
            .bind(*declared_id.as_uuid())
            .execute(&pool)
            .await
            .unwrap_err();
    assert_eq!(
        old.as_database_error().unwrap().code().as_deref(),
        Some("SKC01")
    );
    // A same-named but replaced guard must not satisfy startup verification.
    let definition = query_scalar::<_, String>(
        "SELECT pg_get_functiondef('stateknot.guard_child_direct_invocation()'::regprocedure)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    query("CREATE OR REPLACE FUNCTION stateknot.guard_child_direct_invocation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$")
        .execute(&pool).await.unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(&definition).execute(&pool).await.unwrap();
    upgraded.verify_schema().await.unwrap();
    pool.close().await;
    upgraded.close().await;
    query(&format!("DROP DATABASE {name}"))
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
}
