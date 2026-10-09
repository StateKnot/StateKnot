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
