-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

WITH frame_tables(name) AS (VALUES ('graph_frame_entries'),('graph_frame_heads'),('graph_frame_stacks')),
frame_functions(name) AS (VALUES ('guard_graph_frame_entry_immutable'),('guard_graph_frame_execution_scope'),('guard_graph_frame_entry_complete'),('guard_graph_frame_revision_scope'),('guard_graph_frame_stack_complete'),('guard_graph_frame_entry_components'),('guard_graph_frame_root_projection'),('guard_graph_frame_checkpoint_complete'),('guard_graph_frame_head_complete'),('guard_graph_frame_legacy_wait')),
frame_triggers(table_name,name) AS (VALUES
 ('graph_frame_entries','graph_frame_entries_immutable'),('graph_frame_entries','frame_entries_complete'),
 ('graph_frame_stacks','frame_stacks_complete'),('runs','runs_frame_root_projection'),
 ('node_attempts','node_attempts_frame_scope'),('node_attempt_completions','node_completions_frame_scope'),('pending_node_results','pending_results_frame_scope'),
 ('tool_invocations','tool_invocations_frame_scope'),('model_invocations','model_invocations_frame_scope'),
 ('run_checkpoints','checkpoints_frame_scope'),('run_checkpoints','checkpoints_frame_complete'),
 ('graph_frame_heads','frame_heads_complete'),('run_wait_registrations','waits_frame_scope'),
 ('run_events','run_events_frame_entry_complete'),
 ('tool_invocation_revisions','tool_revisions_frame_scope'),('model_invocation_revisions','model_revisions_frame_scope'))
SELECT jsonb_build_object(
 'columns',(SELECT jsonb_agg(jsonb_build_array(t.name,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attgenerated,pg_get_expr(d.adbin,d.adrelid)) ORDER BY t.name,a.attnum)
 FROM frame_tables t JOIN pg_attribute a ON a.attrelid=to_regclass('stateknot.'||t.name) AND a.attnum>0 AND NOT a.attisdropped
 LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum),
 'constraints',(SELECT jsonb_agg(jsonb_build_array(t.name,c.conname,pg_get_constraintdef(c.oid),c.convalidated,c.condeferrable,c.condeferred) ORDER BY t.name,c.conname)
 FROM frame_tables t JOIN pg_constraint c ON c.conrelid=to_regclass('stateknot.'||t.name)),
 'indexes',(SELECT jsonb_agg(jsonb_build_array(t.name,pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indislive) ORDER BY t.name,pg_get_indexdef(i.indexrelid))
 FROM frame_tables t JOIN pg_index i ON i.indrelid=to_regclass('stateknot.'||t.name)),
 'functions',(SELECT jsonb_agg(jsonb_build_array(f.name,pg_get_functiondef(p.oid),p.prosecdef,p.provolatile,p.proconfig) ORDER BY f.name)
 FROM frame_functions f JOIN pg_proc p ON p.oid=to_regprocedure('stateknot.'||f.name||'()')),
 'triggers',(SELECT jsonb_agg(jsonb_build_array(f.table_name,f.name,pg_get_triggerdef(t.oid),t.tgenabled,t.tgdeferrable,t.tginitdeferred) ORDER BY f.table_name,f.name)
 FROM frame_triggers f JOIN pg_trigger t ON t.tgrelid=to_regclass('stateknot.'||f.table_name) AND t.tgname=f.name)
)::text;
