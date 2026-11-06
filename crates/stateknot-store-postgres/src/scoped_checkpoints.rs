// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Exact catalog verification for scoped keys; no frame dispatch authority.
use sqlx_core::query_scalar::query_scalar;
use sqlx_postgres::PgPool;

use crate::StoreError;

pub(super) async fn verify_schema(pool: &PgPool) -> Result<(), StoreError> {
    let installed = query_scalar::<_, String>(CATALOG_QUERY)
        .fetch_one(pool)
        .await
        .map_err(|source| StoreError::database("scoped checkpoint catalog", source))?;
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("scoped_checkpoint_catalog.json"))
            .map_err(|_| StoreError::IncompleteSchema)?;
    if serde_json::from_str::<serde_json::Value>(&installed)
        .map_err(|_| StoreError::IncompleteSchema)?
        != expected
    {
        return Err(StoreError::IncompleteSchema);
    }
    Ok(())
}

const CATALOG_QUERY: &str = r"
WITH required(table_name, constraint_name) AS (VALUES
    ('run_checkpoints', 'run_checkpoints_frame_shape'),
    ('run_checkpoints', 'run_checkpoints_scoped_position_unique'),
    ('run_checkpoints', 'run_checkpoints_scoped_id_unique'),
    ('run_checkpoints', 'run_checkpoints_scoped_identity_unique'),
    ('run_checkpoints', 'run_checkpoints_scoped_anchor_unique'),
    ('run_checkpoints', 'run_checkpoints_scoped_parent_fk'),
    ('run_checkpoints', 'run_checkpoints_frame_parent_identity_unique'),
    ('run_checkpoints', 'run_checkpoints_frame_parent_identity_fk'),
    ('runs', 'runs_root_checkpoint_fk'),
    ('agent_admissions', 'agent_admissions_root_checkpoint_fk'),
    ('tool_invocations', 'tool_invocations_scoped_checkpoint_fk'),
    ('model_invocations', 'model_invocations_scoped_checkpoint_fk'),
    ('pending_node_results', 'pending_node_results_scoped_checkpoint_fk'),
    ('node_attempts', 'node_attempts_scoped_checkpoint_fk'),
    ('pending_node_result_consumptions', 'pending_node_result_consumptions_scoped_successor_fk'),
    ('child_run_ownership', 'child_run_ownership_scoped_checkpoint_fk'),
    ('child_run_joins', 'child_run_joins_scoped_checkpoint_fk'),
    ('node_attempts', 'node_attempts_scoped_origin_unique'),
    ('node_attempts', 'node_attempts_scoped_node_origin_unique'),
    ('child_run_ownership', 'child_run_ownership_scoped_attempt_fk'),
    ('child_run_joins', 'child_run_joins_scoped_attempt_fk')
), installed AS (
    SELECT required.table_name,c.* FROM required JOIN pg_constraint c
      ON c.conrelid=to_regclass('stateknot.' || required.table_name)
     AND c.conname=required.constraint_name
), columns(table_name, column_name) AS (VALUES
    ('run_checkpoints', 'graph_namespace'),
    ('run_checkpoints', 'frame_identity_digest'),
    ('run_checkpoints', 'frame_checkpoint_digest'),
    ('run_checkpoints', 'frame_checkpoint_head_bytes'),
    ('run_checkpoints', 'frame_checkpoint_head_checksum'),
    ('runs', 'checkpoint_graph_namespace'),
    ('agent_admissions', 'checkpoint_graph_namespace'),
    ('child_run_ownership', 'parent_graph_namespace')
)
SELECT jsonb_build_object(
    'columns',(SELECT jsonb_agg(jsonb_build_array(columns.table_name,a.attname,
        format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attgenerated,
        pg_get_expr(d.adbin,d.adrelid)) ORDER BY columns.table_name,a.attname)
      FROM columns JOIN pg_attribute a ON a.attrelid=to_regclass('stateknot.' || columns.table_name)
        AND a.attname=columns.column_name AND NOT a.attisdropped
      LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum),
    'constraints',(SELECT jsonb_agg(jsonb_build_array(table_name,conname,
        pg_get_constraintdef(oid),convalidated,condeferrable) ORDER BY table_name,conname) FROM installed),
    'indexes',(SELECT jsonb_agg(jsonb_build_array(c.table_name,pg_get_indexdef(i.indexrelid),
        i.indisvalid,i.indisready,i.indislive) ORDER BY c.table_name,c.conname)
      FROM installed c JOIN pg_index i ON i.indexrelid=c.conindid WHERE c.contype='u'),
    'unscoped_position_absent',NOT EXISTS (SELECT 1 FROM pg_constraint
      WHERE conrelid=to_regclass('stateknot.run_checkpoints') AND conname='run_checkpoints_superstep_unique')
)::text
";
