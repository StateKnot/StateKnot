// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Scoped relational integrity; manual fixture rows do not prove frame entry,
//! compound journal authority, nested execution or historical N-1/N-2 support.
use super::*;
use stateknot_core::{GraphFrameCheckpoint, GraphFrameIdentity};

async fn anchor(store: &PostgresStore, root: &Checkpoint, index: u64) -> JournalHead {
    let head = store
        .load_run(root.tenant_id(), root.run_id())
        .await
        .unwrap()
        .journal_head()
        .unwrap()
        .clone();
    match store
        .append_control_plane(
            control_append(
                root.tenant_id().clone(),
                root.run_id(),
                EventId::generate(),
                JournalExpectation::exact(head),
                index,
            ),
            RunProjection::unchanged(),
        )
        .await
        .unwrap()
    {
        AppendOutcome::Committed(event) | AppendOutcome::Idempotent(event) => event.head(),
        _ => panic!("unsupported fixture append"),
    }
}

async fn initial_frame(
    store: &PostgresStore,
    root: &Checkpoint,
    slot: &str,
    index: u64,
) -> GraphFrameCheckpoint {
    let frame = GraphFrameIdentity::new(
        NodeActivation::for_ready_root(root, root.ready_nodes().iter().next().unwrap().clone())
            .unwrap(),
        NodeId::new(slot).unwrap(),
        root.graph().clone(),
    )
    .unwrap();
    let write = CheckpointWrite::initial(
        root.tenant_id().clone(),
        root.run_id(),
        CheckpointId::generate(),
        root.graph().clone(),
        root.state().clone(),
        root.ready_nodes().clone(),
    )
    .unwrap();
    GraphFrameCheckpoint::new(
        frame,
        Checkpoint::commit(write, anchor(store, root, index).await).unwrap(),
    )
    .unwrap()
}

async fn insert_frame_checkpoint(
    pool: &PgPool,
    frame: &GraphFrameCheckpoint,
) -> Result<(), sqlx_core::Error> {
    let checkpoint = frame.checkpoint();
    let parent = checkpoint.parent();
    let schema = checkpoint.graph().state_schema();
    query(
        "INSERT INTO stateknot.run_checkpoints (
            tenant_id,run_id,checkpoint_id,superstep,
            parent_checkpoint_id,parent_superstep,parent_digest,
            journal_sequence,journal_event_id,journal_recorded_at,journal_digest,
            graph_definition_digest,state_schema_id,state_schema_version,state_schema_digest,
            state_digest,intent_digest,checkpoint_digest,checkpoint_bytes,
            graph_namespace,frame_identity_digest,frame_checkpoint_digest,frame_checkpoint_head_bytes
         ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)",
    )
    .bind(checkpoint.tenant_id().as_str())
    .bind(*checkpoint.run_id().as_uuid())
    .bind(*checkpoint.checkpoint_id().as_uuid())
    .bind(i64::try_from(checkpoint.superstep().get()).unwrap())
    .bind(parent.map(|head| *head.checkpoint_id().as_uuid()))
    .bind(parent.map(|head| i64::try_from(head.superstep().get()).unwrap()))
    .bind(parent.map(|head| head.digest().as_bytes().to_vec()))
    .bind(i64::try_from(checkpoint.journal_head().sequence().get()).unwrap())
    .bind(*checkpoint.journal_head().event_id().as_uuid())
    .bind(chrono::DateTime::from_timestamp_micros(checkpoint.journal_head().recorded_at().unix_micros()).unwrap())
    .bind(checkpoint.journal_head().digest().as_bytes())
    .bind(checkpoint.graph().definition_digest().as_bytes())
    .bind(schema.id().as_str())
    .bind(schema.version().to_string())
    .bind(schema.digest().as_bytes())
    .bind(checkpoint.state().digest().as_bytes())
    .bind(checkpoint.intent_digest().as_bytes())
    .bind(checkpoint.digest().as_bytes())
    .bind(serde_json_canonicalizer::to_vec(checkpoint).unwrap())
    .bind(frame.frame().namespace().as_str())
    .bind(frame.frame().digest().as_bytes())
    .bind(frame.digest().as_bytes())
    .bind(serde_json_canonicalizer::to_vec(&frame.head()).unwrap())
    .execute(pool)
    .await?;
    Ok(())
}

fn constraint(error: &sqlx_core::Error) -> Option<&str> {
    error
        .as_database_error()
        .and_then(|error| error.constraint())
}

async fn upgrade_nonempty_root_fixture(url: &str, pool: &PgPool, root: &Checkpoint) {
    let bytes: Vec<u8> = query_scalar(
        "SELECT checkpoint_bytes FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2",
    )
    .bind(root.tenant_id().as_str())
    .bind(*root.run_id().as_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    for sql in include_str!("../fixtures/revert_scoped_checkpoints.sql")
        .split(';')
        .filter(|sql| !sql.trim().is_empty())
    {
        query(sql).execute(pool).await.unwrap();
    }
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(pool)
            .await
            .unwrap(),
        26
    );
    assert!(matches!(
        PostgresStore::connect(url, test_options(Duration::from_secs(30))).await,
        Err(StoreError::IncompatibleSchema)
    ));
    PostgresStore::migrate_database(url, test_options(Duration::from_secs(30)))
        .await
        .unwrap();
    let after: Vec<u8> = query_scalar(
        "SELECT checkpoint_bytes FROM stateknot.run_checkpoints WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=''",
    )
    .bind(root.tenant_id().as_str())
    .bind(*root.run_id().as_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(after, bytes);
    assert_eq!(after, serde_json_canonicalizer::to_vec(root).unwrap());
}

async fn reject_catalog_drift(url: &str, pool: &PgPool, store: &PostgresStore, root: &Checkpoint) {
    let options = test_options(Duration::from_secs(30));
    query("ALTER TABLE stateknot.tool_invocations DROP CONSTRAINT tool_invocations_scoped_checkpoint_fk")
        .execute(pool).await.unwrap();
    assert!(matches!(
        PostgresStore::connect(url, options.clone()).await,
        Err(StoreError::IncompleteSchema)
    ));
    query("ALTER TABLE stateknot.tool_invocations ADD CONSTRAINT tool_invocations_scoped_checkpoint_fk
        FOREIGN KEY (tenant_id,run_id,base_checkpoint_id,base_superstep,base_checkpoint_digest)
        REFERENCES stateknot.run_checkpoints (tenant_id,run_id,checkpoint_id,superstep,checkpoint_digest) ON DELETE RESTRICT")
        .execute(pool).await.unwrap();
    assert!(matches!(
        PostgresStore::connect(url, options.clone()).await,
        Err(StoreError::IncompleteSchema)
    ));
    query("ALTER TABLE stateknot.tool_invocations DROP CONSTRAINT tool_invocations_scoped_checkpoint_fk,
        ADD CONSTRAINT tool_invocations_scoped_checkpoint_fk
        FOREIGN KEY (tenant_id,run_id,graph_namespace,base_checkpoint_id,base_superstep,base_checkpoint_digest)
        REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest) ON DELETE RESTRICT")
        .execute(pool).await.unwrap();
    store.verify_schema().await.unwrap();
    query("ALTER TABLE stateknot.runs DROP CONSTRAINT runs_root_checkpoint_fk,
        DROP COLUMN checkpoint_graph_namespace, ADD COLUMN checkpoint_graph_namespace text NOT NULL DEFAULT '',
        ADD CONSTRAINT runs_root_checkpoint_fk FOREIGN KEY (tenant_id,run_id,checkpoint_graph_namespace,checkpoint_id,checkpoint_superstep,checkpoint_digest)
        REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest) ON DELETE RESTRICT")
        .execute(pool).await.unwrap();
    assert!(matches!(
        PostgresStore::connect(url, options.clone()).await,
        Err(StoreError::IncompleteSchema)
    ));
    query("ALTER TABLE stateknot.runs DROP CONSTRAINT runs_root_checkpoint_fk,
        DROP COLUMN checkpoint_graph_namespace, ADD COLUMN checkpoint_graph_namespace text GENERATED ALWAYS AS (''::text) STORED,
        ADD CONSTRAINT runs_root_checkpoint_fk FOREIGN KEY (tenant_id,run_id,checkpoint_graph_namespace,checkpoint_id,checkpoint_superstep,checkpoint_digest)
        REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest) ON DELETE RESTRICT")
        .execute(pool).await.unwrap();
    store.verify_schema().await.unwrap();
    query(
        "ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_frame_shape,
        ADD CONSTRAINT run_checkpoints_frame_shape CHECK (true)",
    )
    .execute(pool)
    .await
    .unwrap();
    assert!(matches!(
        PostgresStore::connect(url, options).await,
        Err(StoreError::IncompleteSchema)
    ));
    query("UPDATE stateknot.run_checkpoints SET frame_identity_digest=$3 WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=''")
        .bind(root.tenant_id().as_str()).bind(*root.run_id().as_uuid())
        .bind(Digest::sha256(b"corrupt root frame column").as_bytes()).execute(pool).await.unwrap();
    assert!(matches!(
        store
            .load_current_checkpoint(root.tenant_id(), root.run_id())
            .await,
        Err(StoreError::CorruptData { .. })
    ));
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scoped_checkpoint_keys_preserve_root_bytes_and_reject_crossed_references() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(shared) = test_store().await else {
        return;
    };
    shared.close().await;
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let name = format!(
        "stateknot_frame_scope_{}",
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
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let admitted = Box::pin(admit_atomic_agent_fixture(
        &store,
        &tenant("frame-scope"),
        RunId::generate(),
    ))
    .await;
    let root = admitted.checkpoint();
    upgrade_nonempty_root_fixture(&url, &pool, root).await;
    store.verify_schema().await.unwrap();
    let a = initial_frame(&store, root, "slot-a", 27_001).await;
    insert_frame_checkpoint(&pool, &a).await.unwrap();
    let b = initial_frame(&store, root, "slot-b", 27_002).await;
    insert_frame_checkpoint(&pool, &b).await.unwrap();
    assert_ne!(a.frame().namespace(), b.frame().namespace());
    let duplicate = initial_frame(&store, root, "slot-a", 27_003).await;
    assert_eq!(
        constraint(
            &insert_frame_checkpoint(&pool, &duplicate)
                .await
                .unwrap_err()
        ),
        Some("run_checkpoints_scoped_position_unique")
    );
    assert!(matches!(
        store
            .load_checkpoint(
                root.tenant_id(),
                root.run_id(),
                a.checkpoint().checkpoint_id()
            )
            .await,
        Err(StoreError::CheckpointNotFound)
    ));
    assert_eq!(
        store
            .load_current_checkpoint(root.tenant_id(), root.run_id())
            .await
            .unwrap()
            .as_ref(),
        Some(root)
    );
    assert!(matches!(
        store
            .load_checkpoint_lineage_page(
                root.tenant_id(),
                root.run_id(),
                Some(&a.checkpoint().head()),
                CheckpointLineagePageSize::new(1).unwrap()
            )
            .await,
        Err(StoreError::InvalidCheckpointCursor)
    ));
    for table in ["runs", "agent_admissions"] {
        let error = query(&format!("UPDATE stateknot.{table} SET checkpoint_id=$3,checkpoint_superstep=$4,checkpoint_digest=$5 WHERE tenant_id=$1 AND run_id=$2"))
            .bind(root.tenant_id().as_str()).bind(*root.run_id().as_uuid())
            .bind(*a.checkpoint().checkpoint_id().as_uuid()).bind(0_i64).bind(a.checkpoint().digest().as_bytes())
            .execute(&pool).await.unwrap_err();
        assert_eq!(
            constraint(&error),
            Some(format!("{table}_root_checkpoint_fk").as_str())
        );
    }
    let write = CheckpointWrite::successor(
        CheckpointId::generate(),
        a.checkpoint(),
        a.checkpoint().state().clone(),
        a.checkpoint().ready_nodes().clone(),
    )
    .unwrap();
    let checkpoint = Checkpoint::commit(write, anchor(&store, root, 27_004).await).unwrap();
    let crossed = GraphFrameCheckpoint::new(b.frame().clone(), checkpoint.clone()).unwrap();
    assert!(a.verify_successor(&crossed).is_err());
    assert_eq!(
        constraint(&insert_frame_checkpoint(&pool, &crossed).await.unwrap_err()),
        Some("run_checkpoints_scoped_parent_fk")
    );
    let next = GraphFrameCheckpoint::new(a.frame().clone(), checkpoint).unwrap();
    a.verify_successor(&next).unwrap();
    insert_frame_checkpoint(&pool, &next).await.unwrap();
    let error = query("UPDATE stateknot.run_checkpoints SET frame_identity_digest=$4 WHERE tenant_id=$1 AND run_id=$2 AND checkpoint_id=$3")
        .bind(root.tenant_id().as_str()).bind(*root.run_id().as_uuid()).bind(*next.checkpoint().checkpoint_id().as_uuid())
        .bind(Digest::sha256(b"substituted frame identity").as_bytes()).execute(&pool).await.unwrap_err();
    assert_eq!(
        constraint(&error),
        Some("run_checkpoints_frame_parent_identity_fk")
    );
    let error = query("UPDATE stateknot.run_checkpoints SET graph_namespace=$4 WHERE tenant_id=$1 AND run_id=$2 AND checkpoint_id=$3")
        .bind(root.tenant_id().as_str()).bind(*root.run_id().as_uuid()).bind(*a.checkpoint().checkpoint_id().as_uuid())
        .bind("not-a-full-frame-digest").execute(&pool).await.unwrap_err();
    assert_eq!(constraint(&error), Some("run_checkpoints_frame_shape"));
    let lease = store
        .claim_lease(root.tenant_id(), root.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let run = store
        .load_run(root.tenant_id(), root.run_id())
        .await
        .unwrap();
    let attempt_id = AttemptId::generate();
    store
        .start_node_attempt(
            worker_append(
                root.tenant_id().clone(),
                root.run_id(),
                EventId::generate(),
                JournalExpectation::exact(run.journal_head().unwrap().clone()),
                lease.fence().clone(),
                27_005,
            ),
            NodeActivation::for_ready_root(root, root.ready_nodes().iter().next().unwrap().clone())
                .unwrap(),
            attempt_id,
        )
        .await
        .unwrap();
    let error = query("UPDATE stateknot.node_attempts SET graph_namespace=$4 WHERE tenant_id=$1 AND run_id=$2 AND attempt_id=$3")
        .bind(root.tenant_id().as_str()).bind(*root.run_id().as_uuid()).bind(*attempt_id.as_uuid())
        .bind(a.frame().namespace().as_str()).execute(&pool).await.unwrap_err();
    assert_eq!(
        constraint(&error),
        Some("node_attempts_scoped_checkpoint_fk")
    );
    reject_catalog_drift(&url, &pool, &store, root).await;
    pool.close().await;
    store.close().await;
    query(&format!("DROP DATABASE {name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
}
