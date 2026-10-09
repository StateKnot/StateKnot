-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Source fixture downgrade refuses retained scoped suspension records.
SELECT 1 / CASE WHEN EXISTS (SELECT 1 FROM stateknot.graph_frame_barriers WHERE convert_from(barrier_bytes,'UTF8')::jsonb->>'version'='2') THEN 0 ELSE 1 END;
DROP TRIGGER frame_waits_complete ON stateknot.graph_frame_barriers;
DROP FUNCTION stateknot.guard_graph_frame_wait_complete();
DELETE FROM _sqlx_migrations WHERE version=32;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_legacy_wait() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks s
  WHERE s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id AND s.active_namespace<>'') THEN
  RAISE EXCEPTION 'legacy wait has no active frame ownership' USING ERRCODE='SKG01';
 END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_root_projection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks s
  WHERE s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id AND s.active_namespace<>'')
 AND (NEW.checkpoint_id IS DISTINCT FROM OLD.checkpoint_id
  OR NEW.checkpoint_superstep IS DISTINCT FROM OLD.checkpoint_superstep
  OR NEW.checkpoint_digest IS DISTINCT FROM OLD.checkpoint_digest
  OR NEW.lifecycle_status IN ('waiting','succeeded','failed','cancelled')) THEN
  RAISE EXCEPTION 'open frames must be discharged before root continuation or closure' USING ERRCODE='SKG01';
 END IF;
 RETURN NEW;
END $$;

-- Source fixture downgrade refuses every retained whole return fact.
SELECT 1 / CASE WHEN EXISTS (SELECT 1 FROM stateknot.graph_frame_returns LIMIT 1) THEN 0 ELSE 1 END;
DROP TRIGGER run_events_frame_return_complete ON stateknot.run_events;
DROP TRIGGER frame_stacks_transition ON stateknot.graph_frame_stacks;
DROP TABLE stateknot.graph_frame_returns;
DROP FUNCTION stateknot.guard_graph_frame_return_complete();
DROP FUNCTION stateknot.guard_graph_frame_return_event_complete();
DROP FUNCTION stateknot.guard_graph_frame_stack_transition();
DELETE FROM _sqlx_migrations WHERE version=31;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_stack_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE total bigint; maximum integer;
BEGIN
 SELECT count(*),max(ordinal) INTO total,maximum FROM stateknot.graph_frame_entries
  WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF total<>NEW.lifetime_starts OR maximum IS DISTINCT FROM NEW.lifetime_starts
  OR NOT EXISTS (
   SELECT 1 FROM stateknot.graph_frame_entries e
   JOIN stateknot.run_events v ON v.tenant_id=e.tenant_id AND v.run_id=e.run_id AND v.sequence=e.journal_sequence
   JOIN stateknot.graph_frame_heads h ON h.tenant_id=e.tenant_id AND h.run_id=e.run_id AND h.graph_namespace=e.graph_namespace
   WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.ordinal=NEW.lifetime_starts
    AND e.graph_namespace=NEW.active_namespace AND e.frame_identity_digest=NEW.active_frame_identity_digest
    AND v.event_kind='graph-frame-entered' AND v.schema_id='https://stknot.com/schemas/core/graph-frame-entry-event/1.0.0'
    AND v.schema_version='1.0.0' AND v.schema_digest=decode('a2f2d5679fdf7c4dc9a7fa906121e41b9571177aee0d9e28cb29f1d921fa4ea3','hex')
    AND v.projection_digest=e.compound_digest
  ) THEN RAISE EXCEPTION 'incomplete graph frame stack push' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_execution_scope() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE data jsonb; tenant text; identity uuid; namespace text; active text;
BEGIN
 data:=to_jsonb(NEW);
 tenant:=data->>'tenant_id'; identity:=(data->>'run_id')::uuid;
 namespace:=coalesce(data->>'graph_namespace','');
 SELECT active_namespace INTO active FROM stateknot.graph_frame_stacks
  WHERE tenant_id=tenant AND run_id=identity;
 active:=coalesce(active,'');
 IF TG_TABLE_NAME='node_attempt_completions' AND (
  EXISTS (SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=tenant AND e.run_id=identity
   AND e.caller_attempt_id=(data->>'attempt_id')::uuid)
  OR EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings b WHERE b.tenant_id=tenant AND b.run_id=identity
   AND b.caller_attempt_id=(data->>'attempt_id')::uuid)
 ) THEN RAISE EXCEPTION 'framework caller requires whole frame return' USING ERRCODE='SKG02'; END IF;
 IF namespace<>active THEN
  IF TG_TABLE_NAME='node_attempts' AND (
   EXISTS (SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=tenant AND e.run_id=identity
    AND e.caller_attempt_id=(data->>'attempt_id')::uuid
    AND e.journal_sequence=(data->>'journal_sequence')::bigint AND e.parent_namespace=namespace)
   OR EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings b
    JOIN stateknot.graph_frame_entries e USING (tenant_id,run_id,graph_namespace)
    WHERE b.tenant_id=tenant AND b.run_id=identity AND b.caller_attempt_id=(data->>'attempt_id')::uuid
     AND b.journal_sequence=(data->>'journal_sequence')::bigint AND b.journal_digest=decode(substr(data->>'journal_digest',3),'hex')
     AND e.parent_namespace=namespace AND b.graph_namespace=active)
  ) THEN RETURN NULL; END IF;
  RAISE EXCEPTION 'only the current graph frame may execute' USING ERRCODE='SKG01';
 END IF;
 RETURN NULL;
END $$;
