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

-- Source-schema fixture only; no historical executable is exercised.
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

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_checkpoint_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.graph_namespace<>'' AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e
  JOIN stateknot.run_events v ON v.tenant_id=e.tenant_id AND v.run_id=e.run_id AND v.sequence=e.journal_sequence
  WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.graph_namespace=NEW.graph_namespace
   AND e.frame_identity_digest=NEW.frame_identity_digest
   AND e.initial_checkpoint_id=NEW.checkpoint_id AND e.initial_checkpoint_digest=NEW.checkpoint_digest
   AND NEW.superstep=0 AND NEW.parent_checkpoint_id IS NULL
   AND e.journal_sequence=NEW.journal_sequence AND e.journal_event_id=NEW.journal_event_id
   AND e.journal_recorded_at=NEW.journal_recorded_at AND e.journal_digest=NEW.journal_digest
   AND v.event_kind='graph-frame-entered' AND v.projection_digest=e.compound_digest
 ) THEN RAISE EXCEPTION 'scoped checkpoint requires a whole frame transaction' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_head_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e
  JOIN stateknot.run_checkpoints c ON c.tenant_id=e.tenant_id AND c.run_id=e.run_id AND c.graph_namespace=e.graph_namespace AND c.checkpoint_id=e.initial_checkpoint_id
  WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.graph_namespace=NEW.graph_namespace
   AND e.frame_identity_digest=NEW.frame_identity_digest AND NEW.superstep=0
   AND e.initial_checkpoint_id=NEW.checkpoint_id AND e.initial_checkpoint_digest=NEW.checkpoint_digest
   AND c.frame_checkpoint_digest=NEW.frame_checkpoint_digest
 ) THEN RAISE EXCEPTION 'frame head requires its complete scoped checkpoint' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
