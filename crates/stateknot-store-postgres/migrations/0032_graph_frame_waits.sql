-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- A version-2 whole scoped barrier owns every registration through its exact
-- journal anchor. Existing Root waits and version-1 barriers retain their bytes.
CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_legacy_wait() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks s
  WHERE s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id AND s.active_namespace<>'') AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_stacks s
  JOIN stateknot.graph_frame_heads h USING (tenant_id,run_id)
  JOIN stateknot.graph_frame_barriers b ON b.tenant_id=s.tenant_id AND b.run_id=s.run_id AND b.graph_namespace=s.active_namespace AND b.successor_checkpoint_id=h.checkpoint_id
  JOIN stateknot.run_events v ON v.tenant_id=b.tenant_id AND v.run_id=b.run_id AND v.sequence=b.journal_sequence
  CROSS JOIN LATERAL jsonb_array_elements(convert_from(b.barrier_bytes,'UTF8')::jsonb->'disposition'->'waits') w
  WHERE s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id AND h.graph_namespace=s.active_namespace
   AND b.frame_identity_digest=s.active_frame_identity_digest AND b.successor_frame_checkpoint_digest=h.frame_checkpoint_digest
   AND b.journal_sequence=NEW.registration_sequence AND b.journal_event_id=NEW.registration_event_id
   AND b.journal_recorded_at=NEW.registered_at AND b.journal_digest=NEW.registration_event_digest
   AND v.source_kind='worker' AND v.event_kind='graph-frame-barrier-committed' AND v.projection_digest=b.compound_digest
   AND convert_from(b.barrier_bytes,'UTF8')::jsonb->>'version'='2'
   AND convert_from(b.barrier_bytes,'UTF8')::jsonb->'disposition'->>'kind'='wait'
   AND w->>'kind'=NEW.wait_kind
   AND coalesce(w->>'interrupt_id',w->>'timer_id')=NEW.wait_id::text
 ) THEN RAISE EXCEPTION 'wait requires its whole active scoped suspension' USING ERRCODE='SKG02'; END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_root_projection() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE active text; suspension stateknot.graph_frame_barriers%ROWTYPE; outstanding bigint;
BEGIN
 SELECT active_namespace INTO active FROM stateknot.graph_frame_stacks
  WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF coalesce(active,'')='' THEN RETURN NEW; END IF;
 IF NEW.checkpoint_id IS DISTINCT FROM OLD.checkpoint_id
  OR NEW.checkpoint_superstep IS DISTINCT FROM OLD.checkpoint_superstep
  OR NEW.checkpoint_digest IS DISTINCT FROM OLD.checkpoint_digest
  OR NEW.lifecycle_status IN ('succeeded','failed','cancelled') THEN
  RAISE EXCEPTION 'open frames must be discharged before root continuation or closure' USING ERRCODE='SKG01';
 END IF;
 -- Fail-stop quarantine must remain possible even when suspension bytes are
 -- damaged. It preserves lifecycle/root facts and requires its immutable audit.
 IF NEW.quarantined_at IS NOT NULL AND NEW.lifecycle_bytes=OLD.lifecycle_bytes
  AND NEW.lifecycle_revision=OLD.lifecycle_revision AND NEW.lifecycle_status=OLD.lifecycle_status
  AND EXISTS (SELECT 1 FROM stateknot.run_quarantines q WHERE q.tenant_id=NEW.tenant_id AND q.run_id=NEW.run_id AND q.quarantined_at=NEW.quarantined_at) THEN RETURN NEW; END IF;
 SELECT b.* INTO suspension FROM stateknot.graph_frame_barriers b
 JOIN stateknot.graph_frame_heads h ON h.tenant_id=b.tenant_id AND h.run_id=b.run_id AND h.graph_namespace=b.graph_namespace AND h.checkpoint_id=b.successor_checkpoint_id
 WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.graph_namespace=active
  AND convert_from(b.barrier_bytes,'UTF8')::jsonb->>'version'='2'
  AND convert_from(b.barrier_bytes,'UTF8')::jsonb->'disposition'->>'kind'='wait';
 IF NEW.lifecycle_status='waiting' THEN
  SELECT count(*) INTO outstanding FROM stateknot.run_wait_registrations
   WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND status='outstanding';
  IF suspension.journal_sequence IS NULL OR NEW.unresolved_wait_count<>outstanding OR outstanding<1
   OR EXISTS (SELECT 1 FROM stateknot.run_wait_registrations w WHERE w.tenant_id=NEW.tenant_id AND w.run_id=NEW.run_id AND w.status='outstanding' AND w.registration_sequence<>suspension.journal_sequence)
   OR (OLD.lifecycle_status<>'waiting' AND (
    NEW.journal_sequence<>suspension.journal_sequence
    OR OLD.lifecycle_revision<>(convert_from(suspension.barrier_bytes,'UTF8')::jsonb->>'wait_revision')::numeric
    OR NEW.lifecycle_revision<>OLD.lifecycle_revision+1)) THEN
   RAISE EXCEPTION 'waiting projection requires its whole scoped suspension' USING ERRCODE='SKG02';
  END IF;
 ELSIF NEW.lifecycle_status='active' AND suspension.journal_sequence IS NOT NULL THEN
  IF EXISTS (SELECT 1 FROM stateknot.run_wait_registrations w WHERE w.tenant_id=NEW.tenant_id AND w.run_id=NEW.run_id AND w.registration_sequence=suspension.journal_sequence AND w.status='outstanding')
   OR (OLD.lifecycle_status='waiting' AND NOT EXISTS (
    SELECT 1 FROM stateknot.run_wait_registrations w
    JOIN stateknot.run_events v ON v.tenant_id=w.tenant_id AND v.run_id=w.run_id AND v.sequence=w.terminal_sequence AND v.event_id=w.terminal_event_id AND v.event_digest=w.terminal_event_digest
    WHERE w.tenant_id=NEW.tenant_id AND w.run_id=NEW.run_id AND w.registration_sequence=suspension.journal_sequence
     AND w.status IN ('resolved','fired') AND w.terminal_sequence=NEW.journal_sequence
   )) THEN RAISE EXCEPTION 'scoped continuation requires complete wait discharge' USING ERRCODE='SKG02'; END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION stateknot.guard_graph_frame_wait_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE data jsonb; total bigint;
BEGIN
 data:=convert_from(NEW.barrier_bytes,'UTF8')::jsonb;
 IF data->'disposition'->>'kind'<>'wait' THEN RETURN NULL; END IF;
 SELECT count(*) INTO total FROM stateknot.run_wait_registrations
  WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND registration_sequence=NEW.journal_sequence;
 IF data->>'version'<>'2' OR data->>'wait_revision' IS NULL
  OR jsonb_typeof(data->'disposition'->'waits')<>'array'
  OR total<1 OR total>64 OR total<>jsonb_array_length(data->'disposition'->'waits')
  OR EXISTS (
   SELECT 1 FROM jsonb_array_elements(data->'disposition'->'waits') w WHERE NOT EXISTS (
    SELECT 1 FROM stateknot.run_wait_registrations r WHERE r.tenant_id=NEW.tenant_id AND r.run_id=NEW.run_id
     AND r.registration_sequence=NEW.journal_sequence AND r.registration_event_id=NEW.journal_event_id
     AND r.registered_at=NEW.journal_recorded_at AND r.registration_event_digest=NEW.journal_digest AND r.status='outstanding'
     AND r.wait_kind=w->>'kind' AND r.wait_id::text=coalesce(w->>'interrupt_id',w->>'timer_id')
   ))
  OR NOT EXISTS (
   SELECT 1 FROM stateknot.runs r JOIN stateknot.graph_frame_stacks s USING (tenant_id,run_id)
   WHERE r.tenant_id=NEW.tenant_id AND r.run_id=NEW.run_id AND r.lifecycle_status='waiting'
    AND r.lifecycle_revision=(data->>'wait_revision')::numeric+1 AND r.unresolved_wait_count=total
    AND r.journal_sequence=NEW.journal_sequence AND r.journal_digest=NEW.journal_digest
    AND r.lease_attempt_id IS NULL AND s.active_namespace=NEW.graph_namespace AND s.active_frame_identity_digest=NEW.frame_identity_digest
  ) THEN RAISE EXCEPTION 'incomplete whole scoped suspension' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER frame_waits_complete AFTER INSERT ON stateknot.graph_frame_barriers
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_wait_complete();
