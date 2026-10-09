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
