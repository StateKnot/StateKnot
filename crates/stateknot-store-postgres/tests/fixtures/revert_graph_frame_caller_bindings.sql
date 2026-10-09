-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Source fixture downgrade is forbidden while caller bindings exist.
SELECT 1 / CASE WHEN EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings LIMIT 1) THEN 0 ELSE 1 END;
DROP TRIGGER run_events_frame_caller_complete ON stateknot.run_events;
DROP TABLE stateknot.graph_frame_caller_bindings;
DROP FUNCTION stateknot.guard_graph_frame_caller_binding_complete();
DROP FUNCTION stateknot.guard_graph_frame_caller_event_complete();
DELETE FROM _sqlx_migrations WHERE version=30;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_execution_scope() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE data jsonb; tenant text; identity uuid; namespace text; active text;
BEGIN
 data:=to_jsonb(NEW);
 tenant:=data->>'tenant_id'; identity:=(data->>'run_id')::uuid;
 namespace:=coalesce(data->>'graph_namespace','');
 SELECT active_namespace INTO active FROM stateknot.graph_frame_stacks
  WHERE tenant_id=tenant AND run_id=identity;
 active:=coalesce(active,'');
 IF TG_TABLE_NAME='node_attempt_completions' AND EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=tenant AND e.run_id=identity
   AND e.caller_attempt_id=(data->>'attempt_id')::uuid
 ) THEN RAISE EXCEPTION 'framework caller requires whole frame return' USING ERRCODE='SKG02'; END IF;
 IF namespace<>active THEN
  IF TG_TABLE_NAME='node_attempts' AND EXISTS (
   SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=tenant AND e.run_id=identity
    AND e.caller_attempt_id=(data->>'attempt_id')::uuid
    AND e.journal_sequence=(data->>'journal_sequence')::bigint
    AND e.parent_namespace=namespace
  ) THEN RETURN NULL; END IF;
  RAISE EXCEPTION 'only the current graph frame may execute' USING ERRCODE='SKG01';
 END IF;
 RETURN NULL;
END $$;
