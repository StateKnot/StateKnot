-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Source reconstruction refuses retained whole closure/caller facts.
SELECT 1 / CASE WHEN EXISTS(SELECT 1 FROM stateknot.graph_frame_closures) OR EXISTS(SELECT 1 FROM stateknot.graph_frame_closed_callers) THEN 0 ELSE 1 END;
DROP TRIGGER runs_frame_closed_guard ON stateknot.runs;
DROP TRIGGER run_events_frame_closure_complete ON stateknot.run_events;
DROP TABLE stateknot.graph_frame_closed_callers;
DROP TABLE stateknot.graph_frame_closures;
DROP FUNCTION stateknot.guard_graph_frame_closure_complete();
DROP FUNCTION stateknot.guard_graph_frame_closure_event_complete();
DROP FUNCTION stateknot.guard_graph_frame_closed_run();
DELETE FROM _sqlx_migrations WHERE version=33;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_stack_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE total bigint; maximum integer;
BEGIN
 SELECT count(*),max(ordinal) INTO total,maximum FROM stateknot.graph_frame_entries WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF total<>NEW.lifetime_starts OR maximum IS DISTINCT FROM NEW.lifetime_starts
  OR EXISTS (SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id
   AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=e.tenant_id AND x.run_id=e.run_id AND x.graph_namespace=e.graph_namespace)
   AND e.graph_namespace<>NEW.active_namespace AND left(NEW.active_namespace,length(e.graph_namespace)+1)<>e.graph_namespace||'/')
  OR (NEW.active_namespace<>'' AND NOT EXISTS (
   SELECT 1 FROM stateknot.graph_frame_entries e JOIN stateknot.graph_frame_heads h USING (tenant_id,run_id,graph_namespace)
   JOIN stateknot.run_events v ON v.tenant_id=e.tenant_id AND v.run_id=e.run_id AND v.sequence=e.journal_sequence
   WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.graph_namespace=NEW.active_namespace AND e.frame_identity_digest=NEW.active_frame_identity_digest
    AND h.frame_identity_digest=e.frame_identity_digest AND v.event_kind='graph-frame-entered' AND v.projection_digest=e.compound_digest
    AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=e.tenant_id AND x.run_id=e.run_id AND x.graph_namespace=e.graph_namespace)
  )) THEN RAISE EXCEPTION 'incomplete graph frame stack' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_stack_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.tenant_id<>OLD.tenant_id OR NEW.run_id<>OLD.run_id OR NEW.admission_digest<>OLD.admission_digest THEN
  RAISE EXCEPTION 'frame stack identity is immutable' USING ERRCODE='SKG02';
 END IF;
 IF NEW.lifetime_starts=OLD.lifetime_starts+1 AND EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.ordinal=NEW.lifetime_starts
   AND e.graph_namespace=NEW.active_namespace AND e.frame_identity_digest=NEW.active_frame_identity_digest AND e.parent_namespace=OLD.active_namespace
 ) THEN RETURN NEW; END IF;
 IF NEW.lifetime_starts=OLD.lifetime_starts AND EXISTS (
  SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=OLD.tenant_id AND x.run_id=OLD.run_id AND x.graph_namespace=OLD.active_namespace
   AND x.frame_identity_digest=OLD.active_frame_identity_digest AND x.parent_namespace=NEW.active_namespace
   AND ((NEW.active_namespace='' AND NEW.active_frame_identity_digest IS NULL)
    OR EXISTS (SELECT 1 FROM stateknot.graph_frame_heads h WHERE h.tenant_id=NEW.tenant_id AND h.run_id=NEW.run_id AND h.graph_namespace=NEW.active_namespace AND h.frame_identity_digest=NEW.active_frame_identity_digest))
 ) THEN RETURN NEW; END IF;
 RAISE EXCEPTION 'frame stack transition requires whole push or return' USING ERRCODE='SKG02';
END $$;

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
