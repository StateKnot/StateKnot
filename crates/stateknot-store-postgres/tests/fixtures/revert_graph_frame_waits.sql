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
