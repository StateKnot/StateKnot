// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! The public owning read authenticates a real successful noninitial scoped attempt.
use super::frame_barriers::{commit, enter, plan, succeed, terminal};
use super::*;

#[tokio::test]
async fn successful_scoped_attempt_read_keeps_complete_owning_proof() {
    let _guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (_, entry, graph, fence) = enter(&store, "proof-base-reuse", true).await;
    let first = succeed(
        &store,
        entry.entry().checkpoint(),
        &fence,
        NodeControl::Continue,
    )
    .await;
    let first_barrier = commit(
        &store,
        plan(&graph, entry.entry().checkpoint(), &first),
        &fence,
        entry.direct_usage_after().unwrap(),
        &graph,
    )
    .await;
    let result = succeed(&store, first_barrier.checkpoint(), &fence, terminal(&graph)).await;
    let pool = super::frame_barriers::pool().await;
    let attempt_id: Uuid = query_scalar("SELECT node_attempt_id FROM stateknot.pending_node_results WHERE tenant_id=$1 AND run_id=$2 AND graph_namespace=$3 AND base_checkpoint_id=$4 AND node_id=$5")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid())
        .bind(result.intent().activation().graph_namespace().as_str())
        .bind(*result.intent().activation().base_checkpoint().checkpoint_id().as_uuid())
        .bind(result.intent().activation().node_id().as_str())
        .fetch_one(&pool).await.unwrap();
    let attempt_id = AttemptId::from_uuid(attempt_id).unwrap();
    assert_eq!(
        result
            .intent()
            .activation()
            .base_checkpoint()
            .superstep()
            .get(),
        1
    );
    let attempt = store
        .load_node_attempt(fence.tenant_id(), &fence.run_id(), attempt_id)
        .await
        .unwrap();
    assert_eq!(attempt.start().activation(), result.intent().activation());
    assert_eq!(attempt.status(), NodeAttemptStatus::Succeeded);
    assert_eq!(
        attempt.completion().unwrap().outcome().result(),
        Some(&result.head())
    );
    verify_current_components_are_required(&store, &pool, &fence, attempt_id).await;
    pool.close().await;
    store.close().await;
}

// Corruption is confined to this test's actual durable records. Every trigger
// is restored before the owning read; each damaged record is then restored too.
async fn mutate_fixture(
    pool: &PgPool,
    table: &str,
    sql: &str,
    fence: &stateknot_core::RunFence,
    bytes: Option<&[u8]>,
) {
    let mut tx = pool.begin().await.unwrap();
    query(&format!(
        "ALTER TABLE stateknot.{table} DISABLE TRIGGER ALL"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    let statement = query(sql)
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid());
    if let Some(bytes) = bytes {
        assert_eq!(
            statement
                .bind(bytes)
                .execute(&mut *tx)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
    } else {
        assert_eq!(
            statement.execute(&mut *tx).await.unwrap().rows_affected(),
            1
        );
    }
    query(&format!("ALTER TABLE stateknot.{table} ENABLE TRIGGER ALL"))
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

#[allow(clippy::too_many_lines)]
async fn verify_current_components_are_required(
    store: &PostgresStore,
    pool: &PgPool,
    fence: &stateknot_core::RunFence,
    attempt: AttemptId,
) {
    for (table, column) in [
        ("pending_node_results", "result_bytes"),
        ("node_attempt_completions", "completion_bytes"),
    ] {
        let predicate = "tenant_id=$1 AND run_id=$2 AND node_id='finish'";
        let original: Vec<u8> = query_scalar(&format!(
            "SELECT {column} FROM stateknot.{table} WHERE {predicate}"
        ))
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .fetch_one(pool)
        .await
        .unwrap();
        let mut damaged = original.clone();
        damaged.push(b' ');
        let sql = format!("UPDATE stateknot.{table} SET {column}=$3 WHERE {predicate}");
        mutate_fixture(pool, table, &sql, fence, Some(&damaged)).await;
        let result = store
            .load_node_attempt(fence.tenant_id(), &fence.run_id(), attempt)
            .await;
        mutate_fixture(pool, table, &sql, fence, Some(&original)).await;
        assert!(
            matches!(result, Err(StoreError::CorruptData { .. })),
            "{table} must be read and authenticated"
        );
    }
    let anchor_predicate = "tenant_id=$1 AND run_id=$2 AND sequence=(SELECT journal_sequence FROM stateknot.node_attempt_completions WHERE tenant_id=$1 AND run_id=$2 AND node_id='finish')";
    let anchor_digest: Vec<u8> = query_scalar(&format!(
        "SELECT projection_digest FROM stateknot.run_events WHERE {anchor_predicate}"
    ))
    .bind(fence.tenant_id().as_str())
    .bind(*fence.run_id().as_uuid())
    .fetch_one(pool)
    .await
    .unwrap();
    let mut damaged_anchor = anchor_digest.clone();
    damaged_anchor[0] ^= 1;
    let anchor_sql =
        format!("UPDATE stateknot.run_events SET projection_digest=$3 WHERE {anchor_predicate}");
    mutate_fixture(
        pool,
        "run_events",
        &anchor_sql,
        fence,
        Some(&damaged_anchor),
    )
    .await;
    let result = store
        .load_node_attempt(fence.tenant_id(), &fence.run_id(), attempt)
        .await;
    mutate_fixture(pool, "run_events", &anchor_sql, fence, Some(&anchor_digest)).await;
    assert!(
        matches!(result, Err(StoreError::CorruptData { .. })),
        "actual completion anchor must be read and authenticated"
    );

    let owner_sql = "UPDATE stateknot.pending_node_results SET node_attempt_id=NULL WHERE tenant_id=$1 AND run_id=$2 AND node_id='finish'";
    mutate_fixture(pool, "pending_node_results", owner_sql, fence, None).await;
    let result = store
        .load_node_attempt(fence.tenant_id(), &fence.run_id(), attempt)
        .await;
    let mut tx = pool.begin().await.unwrap();
    query("ALTER TABLE stateknot.pending_node_results DISABLE TRIGGER ALL")
        .execute(&mut *tx)
        .await
        .unwrap();
    query("UPDATE stateknot.pending_node_results SET node_attempt_id=$3 WHERE tenant_id=$1 AND run_id=$2 AND node_id='finish'")
        .bind(fence.tenant_id().as_str()).bind(*fence.run_id().as_uuid()).bind(*attempt.as_uuid())
        .execute(&mut *tx).await.unwrap();
    query("ALTER TABLE stateknot.pending_node_results ENABLE TRIGGER ALL")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        matches!(result, Err(StoreError::CorruptData { .. })),
        "actual result owner must be read"
    );

    let insert = "INSERT INTO stateknot.pending_node_result_model_bindings (tenant_id,run_id,base_checkpoint_id,base_superstep,base_checkpoint_digest,graph_namespace,node_id,activation_input_digest,result_record_digest,result_journal_sequence,result_journal_recorded_at,result_journal_digest,invocation_id,invocation_revision,invocation_record_digest,invocation_status,invocation_journal_sequence,invocation_journal_recorded_at,invocation_journal_digest) SELECT tenant_id,run_id,base_checkpoint_id,base_superstep,base_checkpoint_digest,graph_namespace,node_id,activation_input_digest,record_digest,journal_sequence,journal_recorded_at,journal_digest,run_id,1,record_digest,'committed',base_journal_sequence,base_journal_recorded_at,base_journal_digest FROM stateknot.pending_node_results WHERE tenant_id=$1 AND run_id=$2 AND node_id='finish'";
    mutate_fixture(
        pool,
        "pending_node_result_model_bindings",
        insert,
        fence,
        None,
    )
    .await;
    let result = store
        .load_node_attempt(fence.tenant_id(), &fence.run_id(), attempt)
        .await;
    let delete = "DELETE FROM stateknot.pending_node_result_model_bindings WHERE tenant_id=$1 AND run_id=$2 AND node_id='finish' AND invocation_id=run_id";
    mutate_fixture(
        pool,
        "pending_node_result_model_bindings",
        delete,
        fence,
        None,
    )
    .await;
    assert!(
        matches!(result, Err(StoreError::CorruptData { .. })),
        "actual binding inventory must be read"
    );
    assert_eq!(
        store
            .load_node_attempt(fence.tenant_id(), &fence.run_id(), attempt)
            .await
            .unwrap()
            .status(),
        NodeAttemptStatus::Succeeded
    );
}
