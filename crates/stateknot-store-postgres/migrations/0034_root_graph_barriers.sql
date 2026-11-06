-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- No backfill: old legal journal facts retain their original bytes and meaning.
CREATE TABLE stateknot.root_graph_barriers (
 tenant_id text NOT NULL, run_id uuid NOT NULL,
 graph_namespace text GENERATED ALWAYS AS ('') STORED,
 base_checkpoint_id uuid NOT NULL, base_superstep bigint NOT NULL,
 base_checkpoint_digest bytea NOT NULL,
 successor_checkpoint_id uuid NOT NULL, successor_superstep bigint NOT NULL,
 successor_checkpoint_digest bytea NOT NULL,
 result_count integer NOT NULL, result_heads_digest bytea NOT NULL,
 barrier_intent_digest bytea NOT NULL,
 disposition_kind text NOT NULL, disposition_digest bytea NOT NULL,
 original_projection_digest bytea NOT NULL,
 lifecycle_before_revision numeric(20,0) NOT NULL, lifecycle_after_revision numeric(20,0) NOT NULL,
 lifecycle_before_bytes bytea NOT NULL, lifecycle_after_bytes bytea NOT NULL,
 intent_digest bytea NOT NULL, compound_digest bytea NOT NULL,
 bundle_bytes bytea NOT NULL, bundle_checksum bytea GENERATED ALWAYS AS (sha256(bundle_bytes)) STORED,
 journal_sequence bigint NOT NULL, journal_event_id uuid NOT NULL,
 journal_recorded_at timestamptz(6) NOT NULL, journal_digest bytea NOT NULL,
 PRIMARY KEY (tenant_id,run_id,base_checkpoint_id),
 CONSTRAINT root_graph_barriers_position_unique UNIQUE (tenant_id,run_id,base_superstep),
 CONSTRAINT root_graph_barriers_successor_unique UNIQUE (tenant_id,run_id,successor_checkpoint_id),
 CONSTRAINT root_graph_barriers_event_unique UNIQUE (tenant_id,run_id,journal_sequence),
 CONSTRAINT root_graph_barriers_shape CHECK (
  base_superstep>=0 AND successor_superstep=base_superstep+1
  AND result_count BETWEEN 1 AND 1024 AND journal_sequence>1
  AND disposition_kind IN ('continue','wait','terminal')
  AND lifecycle_before_revision BETWEEN 1 AND 18446744073709551615
  AND lifecycle_after_revision BETWEEN 1 AND 18446744073709551615
  AND ((disposition_kind='continue' AND lifecycle_after_revision=lifecycle_before_revision AND lifecycle_before_bytes=lifecycle_after_bytes)
   OR (disposition_kind<>'continue' AND lifecycle_after_revision=lifecycle_before_revision+1))
  AND octet_length(base_checkpoint_digest)=32 AND octet_length(successor_checkpoint_digest)=32
  AND octet_length(result_heads_digest)=32 AND octet_length(barrier_intent_digest)=32
  AND octet_length(disposition_digest)=32 AND octet_length(original_projection_digest)=32
  AND octet_length(intent_digest)=32 AND octet_length(compound_digest)=32 AND octet_length(journal_digest)=32
  AND octet_length(lifecycle_before_bytes) BETWEEN 1 AND 2097152
  AND octet_length(lifecycle_after_bytes) BETWEEN 1 AND 2097152
  AND octet_length(bundle_bytes) BETWEEN 1 AND 9474048
 ),
 CONSTRAINT root_graph_barriers_base_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,base_checkpoint_id,base_superstep,base_checkpoint_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest) ON DELETE RESTRICT,
 CONSTRAINT root_graph_barriers_successor_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,successor_checkpoint_id,successor_superstep,successor_checkpoint_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) ON DELETE RESTRICT,
 CONSTRAINT root_graph_barriers_event_fk FOREIGN KEY (tenant_id,run_id,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
  REFERENCES stateknot.run_events (tenant_id,run_id,sequence,event_id,recorded_at,event_digest) ON DELETE RESTRICT
);
CREATE TRIGGER root_graph_barriers_immutable BEFORE UPDATE OR DELETE ON stateknot.root_graph_barriers
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_immutable();

CREATE FUNCTION stateknot.guard_root_graph_barrier_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE data jsonb; total bigint; wait_total bigint;
BEGIN
 data:=convert_from(NEW.bundle_bytes,'UTF8')::jsonb;
 SELECT count(*) INTO total FROM stateknot.pending_node_result_consumptions p
  WHERE p.tenant_id=NEW.tenant_id AND p.run_id=NEW.run_id AND p.base_checkpoint_id=NEW.base_checkpoint_id;
 SELECT count(*) INTO wait_total FROM stateknot.run_wait_registrations w
  WHERE w.tenant_id=NEW.tenant_id AND w.run_id=NEW.run_id AND w.registration_sequence=NEW.journal_sequence;
 IF total<>NEW.result_count OR data->>'version' IS DISTINCT FROM '1' OR data->'intent'->>'version' IS DISTINCT FROM '1'
  OR data->>'intent_digest' IS DISTINCT FROM 'sha256:'||encode(NEW.intent_digest,'hex')
  OR data->>'compound_digest' IS DISTINCT FROM 'sha256:'||encode(NEW.compound_digest,'hex')
  OR data->'intent'->'disposition'->>'kind' IS DISTINCT FROM NEW.disposition_kind
  OR data->'intent'->'lifecycle_before' IS DISTINCT FROM convert_from(NEW.lifecycle_before_bytes,'UTF8')::jsonb
  OR data->'lifecycle_after' IS DISTINCT FROM convert_from(NEW.lifecycle_after_bytes,'UTF8')::jsonb
  OR data->'intent'->>'result_count' IS DISTINCT FROM NEW.result_count::text
  OR data->'intent'->>'barrier_intent_digest' IS DISTINCT FROM 'sha256:'||encode(NEW.barrier_intent_digest,'hex')
  OR data->'intent'->>'result_heads_digest' IS DISTINCT FROM 'sha256:'||encode(NEW.result_heads_digest,'hex')
  OR data->'intent'->>'original_projection_digest' IS DISTINCT FROM 'sha256:'||encode(NEW.original_projection_digest,'hex')
  OR NOT EXISTS (
   SELECT 1 FROM stateknot.run_checkpoints b
   JOIN stateknot.run_checkpoints n ON n.tenant_id=b.tenant_id AND n.run_id=b.run_id AND n.checkpoint_id=NEW.successor_checkpoint_id
   JOIN stateknot.run_events e ON e.tenant_id=b.tenant_id AND e.run_id=b.run_id AND e.sequence=NEW.journal_sequence
   JOIN stateknot.run_events p ON p.tenant_id=e.tenant_id AND p.run_id=e.run_id AND p.sequence=e.sequence-1
   JOIN stateknot.runs r ON r.tenant_id=b.tenant_id AND r.run_id=b.run_id
   WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.graph_namespace='' AND b.checkpoint_id=NEW.base_checkpoint_id
    AND b.superstep=NEW.base_superstep AND b.checkpoint_digest=NEW.base_checkpoint_digest AND b.journal_sequence<NEW.journal_sequence
    AND n.graph_namespace='' AND n.parent_checkpoint_id=b.checkpoint_id AND n.parent_superstep=b.superstep AND n.parent_digest=b.checkpoint_digest
    AND n.superstep=NEW.successor_superstep AND n.checkpoint_digest=NEW.successor_checkpoint_digest
    AND n.journal_sequence=NEW.journal_sequence AND n.journal_event_id=NEW.journal_event_id AND n.journal_digest=NEW.journal_digest
    AND e.event_kind='root-graph-barrier-committed' AND e.source_kind='worker' AND e.projection_digest=NEW.compound_digest
    AND e.previous_digest=p.event_digest AND p.recorded_at<=e.recorded_at
    AND data->'source_journal'->>'tenant_id'=p.tenant_id AND data->'source_journal'->>'run_id'=p.run_id::text
    AND data->'source_journal'->>'sequence'=p.sequence::text AND data->'source_journal'->>'event_id'=p.event_id::text
    AND data->'source_journal'->>'recorded_at'=to_char(p.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    AND data->'source_journal'->>'digest'='sha256:'||encode(p.event_digest,'hex')
    AND e.schema_id='https://stknot.com/schemas/store/root-graph-barrier-event/1.0.0' AND e.schema_version='1.0.0'
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'version'='1'
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'graph_namespace'=''
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'base_checkpoint_id'=NEW.base_checkpoint_id::text
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'base_checkpoint_digest'='sha256:'||encode(NEW.base_checkpoint_digest,'hex')
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'successor_checkpoint_id'=NEW.successor_checkpoint_id::text
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'barrier_intent_digest'='sha256:'||encode(NEW.barrier_intent_digest,'hex')
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'intent_digest'='sha256:'||encode(NEW.intent_digest,'hex')
    AND r.journal_sequence=NEW.journal_sequence AND r.journal_event_id=NEW.journal_event_id AND r.journal_recorded_at=NEW.journal_recorded_at AND r.journal_digest=NEW.journal_digest
    AND r.checkpoint_id=NEW.successor_checkpoint_id AND r.checkpoint_superstep=NEW.successor_superstep AND r.checkpoint_digest=NEW.successor_checkpoint_digest
    AND r.lifecycle_bytes=NEW.lifecycle_after_bytes AND r.lifecycle_revision=NEW.lifecycle_after_revision
    AND r.lifecycle_status=CASE NEW.disposition_kind WHEN 'continue' THEN 'active' WHEN 'wait' THEN 'waiting' ELSE 'succeeded' END
    AND ((NEW.disposition_kind='continue' AND r.lease_attempt_id=e.worker_attempt_id AND r.fencing_epoch=e.worker_epoch)
     OR (NEW.disposition_kind<>'continue' AND r.lease_attempt_id IS NULL))
  ) OR EXISTS (
   SELECT 1 FROM stateknot.pending_node_result_consumptions p WHERE p.tenant_id=NEW.tenant_id AND p.run_id=NEW.run_id AND p.base_checkpoint_id=NEW.base_checkpoint_id
    AND (p.graph_namespace<>'' OR p.base_superstep<>NEW.base_superstep OR p.base_checkpoint_digest<>NEW.base_checkpoint_digest
     OR p.successor_checkpoint_id<>NEW.successor_checkpoint_id OR p.successor_superstep<>NEW.successor_superstep OR p.successor_checkpoint_digest<>NEW.successor_checkpoint_digest
     OR p.successor_journal_sequence<>NEW.journal_sequence OR p.successor_journal_event_id<>NEW.journal_event_id OR p.successor_journal_recorded_at<>NEW.journal_recorded_at OR p.successor_journal_digest<>NEW.journal_digest)
  ) OR EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks s WHERE s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id AND s.active_namespace<>'') THEN
  RAISE EXCEPTION 'incomplete whole Root graph barrier' USING ERRCODE='SKG02';
 END IF;
 IF NEW.disposition_kind='wait' THEN
  IF jsonb_typeof(data->'intent'->'disposition'->'registrations') IS DISTINCT FROM 'array' OR wait_total NOT BETWEEN 1 AND 64
   OR wait_total<>jsonb_array_length(data->'intent'->'disposition'->'registrations') OR EXISTS (
    SELECT 1 FROM jsonb_array_elements(data->'intent'->'disposition'->'registrations') w WHERE NOT EXISTS (
     SELECT 1 FROM stateknot.run_wait_registrations r WHERE r.tenant_id=NEW.tenant_id AND r.run_id=NEW.run_id
      AND r.registration_sequence=NEW.journal_sequence AND r.registration_event_id=NEW.journal_event_id AND r.registered_at=NEW.journal_recorded_at AND r.registration_event_digest=NEW.journal_digest
      AND r.wait_kind=w->>'kind' AND r.wait_id::text=w->>'wait_id' AND 'sha256:'||encode(r.intent_digest,'hex')=w->>'intent_digest' AND r.status='outstanding'
    )) THEN RAISE EXCEPTION 'incomplete whole Root wait barrier' USING ERRCODE='SKG02'; END IF;
 ELSIF wait_total<>0 THEN RAISE EXCEPTION 'unexpected whole Root wait registrations' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER root_barriers_complete AFTER INSERT ON stateknot.root_graph_barriers
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_root_graph_barrier_complete();

CREATE FUNCTION stateknot.guard_root_graph_barrier_event_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.event_kind='root-graph-barrier-committed' AND NOT EXISTS (
  SELECT 1 FROM stateknot.root_graph_barriers b WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.journal_sequence=NEW.sequence
   AND b.journal_event_id=NEW.event_id AND b.journal_recorded_at=NEW.recorded_at AND b.journal_digest=NEW.event_digest
   AND b.compound_digest=NEW.projection_digest AND NEW.source_kind='worker'
 ) THEN RAISE EXCEPTION 'Root graph event requires its whole transaction' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER run_events_root_barrier_complete AFTER INSERT ON stateknot.run_events
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_root_graph_barrier_event_complete();

CREATE FUNCTION stateknot.guard_root_graph_barrier_projection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.journal_sequence IS NOT DISTINCT FROM OLD.journal_sequence THEN RETURN NEW; END IF;
 IF EXISTS (SELECT 1 FROM stateknot.run_events e WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.sequence=NEW.journal_sequence AND e.event_kind='root-graph-barrier-committed') AND NOT EXISTS (
  SELECT 1 FROM stateknot.root_graph_barriers b JOIN stateknot.run_events e ON e.tenant_id=b.tenant_id AND e.run_id=b.run_id AND e.sequence=b.journal_sequence
  WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.journal_sequence=NEW.journal_sequence AND b.journal_digest=NEW.journal_digest
   AND b.lifecycle_before_bytes=OLD.lifecycle_bytes AND b.lifecycle_before_revision=OLD.lifecycle_revision AND OLD.lifecycle_status='active'
   AND b.lifecycle_after_bytes=NEW.lifecycle_bytes AND b.lifecycle_after_revision=NEW.lifecycle_revision
   AND OLD.lease_attempt_id=e.worker_attempt_id AND OLD.fencing_epoch=e.worker_epoch
   AND OLD.lease_expires_at>clock_timestamp() AND OLD.agent_deadline_at>clock_timestamp()
   AND b.successor_checkpoint_id=NEW.checkpoint_id AND b.successor_checkpoint_digest=NEW.checkpoint_digest
 ) THEN RAISE EXCEPTION 'Root barrier must preserve its exact original fenced lifecycle' USING ERRCODE='SKG02'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runs_root_barrier_projection BEFORE UPDATE ON stateknot.runs
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_root_graph_barrier_projection();

-- Later insertion cannot extend or replace an already committed whole batch.
-- This guard authenticates immutable origin facts, without requiring a Run or
-- a mutable wait status to remain at its historical position.
CREATE FUNCTION stateknot.guard_root_graph_barrier_component_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source_sequence bigint; bundle stateknot.root_graph_barriers%ROWTYPE; total bigint; data jsonb;
BEGIN
 IF TG_TABLE_NAME='run_wait_registrations' THEN source_sequence:=NEW.registration_sequence;
 ELSE source_sequence:=NEW.successor_journal_sequence; END IF;
 IF NOT EXISTS (SELECT 1 FROM stateknot.run_events e WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id
  AND e.sequence=source_sequence AND e.event_kind='root-graph-barrier-committed') THEN RETURN NULL; END IF;
 SELECT b.* INTO bundle FROM stateknot.root_graph_barriers b JOIN stateknot.run_events e
  ON e.tenant_id=b.tenant_id AND e.run_id=b.run_id AND e.sequence=b.journal_sequence
 WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.journal_sequence=source_sequence
  AND e.event_kind='root-graph-barrier-committed' AND e.source_kind='worker' AND e.event_id=b.journal_event_id
  AND e.recorded_at=b.journal_recorded_at AND e.event_digest=b.journal_digest AND e.projection_digest=b.compound_digest;
 IF NOT FOUND THEN RAISE EXCEPTION 'Root component requires its original whole bundle' USING ERRCODE='SKG02'; END IF;
 IF TG_TABLE_NAME='run_wait_registrations' THEN
  data:=convert_from(bundle.bundle_bytes,'UTF8')::jsonb;
  SELECT count(*) INTO total FROM stateknot.run_wait_registrations w WHERE w.tenant_id=bundle.tenant_id
   AND w.run_id=bundle.run_id AND w.registration_sequence=bundle.journal_sequence;
  IF bundle.disposition_kind<>'wait' OR jsonb_typeof(data->'intent'->'disposition'->'registrations') IS DISTINCT FROM 'array'
   OR total NOT BETWEEN 1 AND 64 OR total<>jsonb_array_length(data->'intent'->'disposition'->'registrations')
   OR NEW.registration_event_id<>bundle.journal_event_id OR NEW.registered_at<>bundle.journal_recorded_at OR NEW.registration_event_digest<>bundle.journal_digest
   OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(data->'intent'->'disposition'->'registrations') w
    WHERE w->>'kind'=NEW.wait_kind AND w->>'wait_id'=NEW.wait_id::text AND w->>'intent_digest'='sha256:'||encode(NEW.intent_digest,'hex'))
   OR EXISTS (SELECT 1 FROM stateknot.run_wait_registrations r WHERE r.tenant_id=bundle.tenant_id AND r.run_id=bundle.run_id AND r.registration_sequence=bundle.journal_sequence
    AND (r.registration_event_id<>bundle.journal_event_id OR r.registered_at<>bundle.journal_recorded_at OR r.registration_event_digest<>bundle.journal_digest
     OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(data->'intent'->'disposition'->'registrations') w
      WHERE w->>'kind'=r.wait_kind AND w->>'wait_id'=r.wait_id::text AND w->>'intent_digest'='sha256:'||encode(r.intent_digest,'hex')))) THEN
   RAISE EXCEPTION 'Root wait cannot extend its original whole batch' USING ERRCODE='SKG02';
  END IF;
 ELSE
  SELECT count(*) INTO total FROM stateknot.pending_node_result_consumptions p WHERE p.tenant_id=bundle.tenant_id
   AND p.run_id=bundle.run_id AND p.base_checkpoint_id=bundle.base_checkpoint_id;
  IF total<>bundle.result_count OR NEW.graph_namespace<>'' OR NEW.base_checkpoint_id<>bundle.base_checkpoint_id
   OR NEW.base_superstep<>bundle.base_superstep OR NEW.base_checkpoint_digest<>bundle.base_checkpoint_digest
   OR NEW.successor_checkpoint_id<>bundle.successor_checkpoint_id OR NEW.successor_superstep<>bundle.successor_superstep OR NEW.successor_checkpoint_digest<>bundle.successor_checkpoint_digest
   OR NEW.successor_journal_event_id<>bundle.journal_event_id OR NEW.successor_journal_recorded_at<>bundle.journal_recorded_at OR NEW.successor_journal_digest<>bundle.journal_digest
   OR EXISTS (SELECT 1 FROM stateknot.pending_node_result_consumptions p WHERE p.tenant_id=bundle.tenant_id AND p.run_id=bundle.run_id AND p.base_checkpoint_id=bundle.base_checkpoint_id
    AND (p.graph_namespace<>'' OR p.base_superstep<>bundle.base_superstep OR p.base_checkpoint_digest<>bundle.base_checkpoint_digest
     OR p.successor_checkpoint_id<>bundle.successor_checkpoint_id OR p.successor_superstep<>bundle.successor_superstep OR p.successor_checkpoint_digest<>bundle.successor_checkpoint_digest
     OR p.successor_journal_sequence<>bundle.journal_sequence OR p.successor_journal_event_id<>bundle.journal_event_id
     OR p.successor_journal_recorded_at<>bundle.journal_recorded_at OR p.successor_journal_digest<>bundle.journal_digest)) THEN
   RAISE EXCEPTION 'Root result cannot extend or replace its original whole batch' USING ERRCODE='SKG02';
  END IF;
 END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER root_wait_components_complete AFTER INSERT ON stateknot.run_wait_registrations
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_root_graph_barrier_component_complete();
CREATE CONSTRAINT TRIGGER root_result_components_complete AFTER INSERT ON stateknot.pending_node_result_consumptions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_root_graph_barrier_component_complete();
