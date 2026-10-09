-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Source fixture downgrade is forbidden while caller bindings exist.
SELECT 1 / CASE WHEN EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings LIMIT 1) THEN 0 ELSE 1 END;
DROP TRIGGER run_events_frame_caller_complete ON stateknot.run_events;
DROP TABLE stateknot.graph_frame_caller_bindings;
DROP FUNCTION stateknot.guard_graph_frame_caller_binding_complete();
DROP FUNCTION stateknot.guard_graph_frame_caller_event_complete();
DELETE FROM _sqlx_migrations WHERE version=30;

-- Isolated Root-only source fixtures. Refuse real retained nested-frame data.
-- Schema-29 removal is restricted to isolated Root-only test fixtures.
SELECT 1 / CASE WHEN EXISTS (SELECT 1 FROM stateknot.graph_frame_barriers LIMIT 1) THEN 0 ELSE 1 END;
DROP TRIGGER run_events_frame_barrier_complete ON stateknot.run_events;
DROP TRIGGER frame_heads_advance ON stateknot.graph_frame_heads;
DROP TRIGGER barrier_consumptions_frame_complete ON stateknot.pending_node_result_consumptions;
DROP TABLE stateknot.graph_frame_barriers;
DROP FUNCTION stateknot.guard_graph_frame_barrier_complete();
DROP FUNCTION stateknot.guard_graph_frame_barrier_event_complete();
DROP FUNCTION stateknot.guard_graph_frame_head_advance();
DROP FUNCTION stateknot.guard_graph_frame_consumption_complete();
DELETE FROM _sqlx_migrations WHERE version=29;
SELECT 1 / CASE WHEN EXISTS (SELECT 1 FROM stateknot.graph_frame_entries LIMIT 1) THEN 0 ELSE 1 END;
DROP TRIGGER node_attempts_frame_scope ON stateknot.node_attempts;
DROP TRIGGER node_completions_frame_scope ON stateknot.node_attempt_completions;
DROP TRIGGER pending_results_frame_scope ON stateknot.pending_node_results;
DROP TRIGGER tool_invocations_frame_scope ON stateknot.tool_invocations;
DROP TRIGGER model_invocations_frame_scope ON stateknot.model_invocations;
DROP TRIGGER checkpoints_frame_scope ON stateknot.run_checkpoints;
DROP TRIGGER checkpoints_frame_complete ON stateknot.run_checkpoints;
DROP TRIGGER waits_frame_scope ON stateknot.run_wait_registrations;
DROP TRIGGER tool_revisions_frame_scope ON stateknot.tool_invocation_revisions;
DROP TRIGGER model_revisions_frame_scope ON stateknot.model_invocation_revisions;
DROP TRIGGER run_events_frame_entry_complete ON stateknot.run_events;
DROP TRIGGER runs_frame_root_projection ON stateknot.runs;
DROP TABLE stateknot.graph_frame_stacks;
DROP TABLE stateknot.graph_frame_heads;
DROP TABLE stateknot.graph_frame_entries;
DROP FUNCTION stateknot.guard_graph_frame_entry_immutable();
DROP FUNCTION stateknot.guard_graph_frame_execution_scope();
DROP FUNCTION stateknot.guard_graph_frame_entry_complete();
DROP FUNCTION stateknot.guard_graph_frame_revision_scope();
DROP FUNCTION stateknot.guard_graph_frame_stack_complete();
DROP FUNCTION stateknot.guard_graph_frame_entry_components();
DROP FUNCTION stateknot.guard_graph_frame_root_projection();
DROP FUNCTION stateknot.guard_graph_frame_checkpoint_complete();
DROP FUNCTION stateknot.guard_graph_frame_head_complete();
DROP FUNCTION stateknot.guard_graph_frame_legacy_wait();
DELETE FROM _sqlx_migrations WHERE version=28;

-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0
-- Test-only removal on isolated root fixtures before source-schema upgrades.
ALTER TABLE stateknot.child_run_joins DROP CONSTRAINT child_run_joins_scoped_attempt_fk;
ALTER TABLE stateknot.child_run_ownership DROP CONSTRAINT child_run_ownership_scoped_attempt_fk;
ALTER TABLE stateknot.node_attempts DROP CONSTRAINT node_attempts_scoped_node_origin_unique;
ALTER TABLE stateknot.node_attempts DROP CONSTRAINT node_attempts_scoped_origin_unique;
ALTER TABLE stateknot.child_run_joins DROP CONSTRAINT child_run_joins_scoped_checkpoint_fk;
ALTER TABLE stateknot.child_run_ownership DROP CONSTRAINT child_run_ownership_scoped_checkpoint_fk;
ALTER TABLE stateknot.child_run_ownership DROP COLUMN parent_graph_namespace;
ALTER TABLE stateknot.pending_node_result_consumptions DROP CONSTRAINT pending_node_result_consumptions_scoped_successor_fk;
ALTER TABLE stateknot.node_attempts DROP CONSTRAINT node_attempts_scoped_checkpoint_fk;
ALTER TABLE stateknot.pending_node_results DROP CONSTRAINT pending_node_results_scoped_checkpoint_fk;
ALTER TABLE stateknot.model_invocations DROP CONSTRAINT model_invocations_scoped_checkpoint_fk;
ALTER TABLE stateknot.tool_invocations DROP CONSTRAINT tool_invocations_scoped_checkpoint_fk;
ALTER TABLE stateknot.agent_admissions DROP CONSTRAINT agent_admissions_root_checkpoint_fk;
ALTER TABLE stateknot.agent_admissions DROP COLUMN checkpoint_graph_namespace;
ALTER TABLE stateknot.runs DROP CONSTRAINT runs_root_checkpoint_fk;
ALTER TABLE stateknot.runs DROP COLUMN checkpoint_graph_namespace;
ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_frame_parent_identity_fk;
ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_frame_parent_identity_unique;
ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_scoped_parent_fk;
ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_scoped_anchor_unique;
ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_scoped_identity_unique;
ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_scoped_id_unique;
ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_scoped_position_unique;
ALTER TABLE stateknot.run_checkpoints DROP CONSTRAINT run_checkpoints_frame_shape;
ALTER TABLE stateknot.run_checkpoints DROP COLUMN frame_checkpoint_head_checksum;
ALTER TABLE stateknot.run_checkpoints DROP COLUMN frame_checkpoint_head_bytes;
ALTER TABLE stateknot.run_checkpoints DROP COLUMN frame_checkpoint_digest;
ALTER TABLE stateknot.run_checkpoints DROP COLUMN frame_identity_digest;
ALTER TABLE stateknot.run_checkpoints DROP COLUMN graph_namespace;
ALTER TABLE stateknot.run_checkpoints ADD CONSTRAINT run_checkpoints_superstep_unique UNIQUE (tenant_id, run_id, superstep);
DELETE FROM _sqlx_migrations WHERE version = 27;
