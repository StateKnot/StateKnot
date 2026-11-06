-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

WITH frame_tables(name) AS (VALUES ('root_graph_barriers')),
frame_functions(name) AS (VALUES ('guard_root_graph_barrier_complete'),('guard_root_graph_barrier_event_complete'),('guard_root_graph_barrier_projection'),('guard_root_graph_barrier_component_complete')),
frame_triggers(table_name,name) AS (VALUES
 ('root_graph_barriers','root_graph_barriers_immutable'),('root_graph_barriers','root_barriers_complete'),
 ('run_events','run_events_root_barrier_complete'),('runs','runs_root_barrier_projection'),
 ('run_wait_registrations','root_wait_components_complete'),('pending_node_result_consumptions','root_result_components_complete'))
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
