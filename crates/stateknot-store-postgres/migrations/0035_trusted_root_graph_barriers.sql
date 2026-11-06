-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- No backfill or rewriting of deployed V1 facts. CREATE OR REPLACE preserves
-- function identity, ownership, ACLs, and existing deferred trigger bindings.
-- V2 additionally carries its positive original creation and terminal inputs;
-- V1 retains its original byte ceiling and original Agent deadline authority.
ALTER TABLE stateknot.root_graph_barriers DROP CONSTRAINT root_graph_barriers_shape;
ALTER TABLE stateknot.root_graph_barriers ADD CONSTRAINT root_graph_barriers_shape CHECK (

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
  AND octet_length(bundle_bytes) BETWEEN 1 AND 14241792
  AND (octet_length(bundle_bytes)<=9474048
   OR convert_from(bundle_bytes,'UTF8')::jsonb->>'version'='2')
);

CREATE OR REPLACE FUNCTION stateknot.guard_root_graph_barrier_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE data jsonb; total bigint; wait_total bigint; creation jsonb;
BEGIN
 data:=convert_from(NEW.bundle_bytes,'UTF8')::jsonb;
 SELECT count(*) INTO total FROM stateknot.pending_node_result_consumptions p
  WHERE p.tenant_id=NEW.tenant_id AND p.run_id=NEW.run_id AND p.base_checkpoint_id=NEW.base_checkpoint_id;
 SELECT count(*) INTO wait_total FROM stateknot.run_wait_registrations w
  WHERE w.tenant_id=NEW.tenant_id AND w.run_id=NEW.run_id AND w.registration_sequence=NEW.journal_sequence;
 IF total<>NEW.result_count
  OR NOT ((data->>'version' IS NOT DISTINCT FROM '1' AND data->'intent'->>'version' IS NOT DISTINCT FROM '1'
    AND NOT (data->'intent' ? 'source') AND NOT (data->'intent' ? 'terminal_validation') AND octet_length(NEW.bundle_bytes)<=9474048)
   OR (data->>'version' IS NOT DISTINCT FROM '2' AND data->'intent'->>'version' IS NOT DISTINCT FROM '2'
    AND data->'intent'->'source'->>'kind' IS NOT DISTINCT FROM 'trusted_graph'
    AND jsonb_typeof(data->'intent'->'source'->'creation') IS NOT DISTINCT FROM 'object'
    AND ((NEW.disposition_kind='terminal' AND jsonb_typeof(data->'intent'->'terminal_validation') IS NOT DISTINCT FROM 'object')
     OR (NEW.disposition_kind<>'terminal' AND NOT (data->'intent' ? 'terminal_validation')))))
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
    AND ((data->>'version'='1' AND e.schema_id='https://stknot.com/schemas/store/root-graph-barrier-event/1.0.0' AND e.schema_version='1.0.0'
      AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'version'='1')
     OR (data->>'version'='2' AND e.schema_id='https://stknot.com/schemas/store/root-graph-barrier-event/2.0.0' AND e.schema_version='2.0.0'
      AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'version'='2'
      AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'source'='trusted_graph'))
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
 IF data->>'version'='2' THEN
  creation:=data->'intent'->'source'->'creation';
  -- Positive original creation, independent of current lifecycle position. A
  -- null deadline or missing admission alone never grants the ordinary profile.
  IF NOT EXISTS (
   SELECT 1 FROM stateknot.runs r
   JOIN stateknot.run_events f ON f.tenant_id=r.tenant_id AND f.run_id=r.run_id AND f.sequence=1
   JOIN stateknot.run_checkpoints c ON c.tenant_id=r.tenant_id AND c.run_id=r.run_id AND c.graph_namespace='' AND c.superstep=0
   JOIN stateknot.run_events e ON e.tenant_id=c.tenant_id AND e.run_id=c.run_id AND e.sequence=c.journal_sequence
   WHERE r.tenant_id=NEW.tenant_id AND r.run_id=NEW.run_id AND r.agent_deadline_at IS NULL
    AND f.previous_digest IS NULL AND f.event_kind<>'agent-admitted'
    AND creation->'provenance'=convert_from(r.lifecycle_bytes,'UTF8')::jsonb->'provenance'
    AND creation->'provenance'=data->'intent'->'lifecycle_before'->'provenance'
    AND creation->>'admitted_at'=to_char(r.admitted_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    AND creation->>'admitted_at'=data->'intent'->'lifecycle_before'->>'admitted_at'
    AND creation->>'started_at'=to_char((creation->>'started_at')::timestamptz AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    AND (creation->>'started_at')::timestamptz BETWEEN r.admitted_at AND f.recorded_at
    AND creation->'first_journal'=jsonb_build_object('tenant_id',f.tenant_id,'run_id',f.run_id::text,'sequence',f.sequence::text,
     'event_id',f.event_id::text,'recorded_at',to_char(f.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'digest','sha256:'||encode(f.event_digest,'hex'))
    AND creation->'start_journal'=creation->'first_journal'
    AND creation->>'start_projection_digest'='sha256:'||encode(f.projection_digest,'hex')
    AND f.projection_digest=sha256(convert_to('stateknot-postgres-run-projection-v1','UTF8')||decode('00','hex')||
     convert_to('{"expected_revision":"0","kind":"transition","transition":{"kind":"start","started_at":"'||(creation->>'started_at')||'"}}','UTF8'))
    AND c.parent_checkpoint_id IS NULL AND c.parent_superstep IS NULL AND c.parent_digest IS NULL
    AND convert_from(c.checkpoint_bytes,'UTF8')::jsonb->'graph'=data->'intent'->'base'->'graph'
    AND c.journal_sequence<=NEW.journal_sequence AND c.journal_sequence>=f.sequence AND c.journal_recorded_at>=f.recorded_at
    AND c.journal_event_id=e.event_id AND c.journal_recorded_at=e.recorded_at AND c.journal_digest=e.event_digest
    AND e.event_kind NOT IN ('root-graph-barrier-committed','graph-frame-entered')
    AND ((e.sequence=f.sequence AND e.event_digest=f.event_digest AND e.projection_digest=f.projection_digest)
     OR (e.sequence>f.sequence AND e.projection_digest=sha256(convert_to('stateknot-postgres-run-projection-v1','UTF8')||decode('00','hex')||convert_to('{"kind":"unchanged"}','UTF8'))))
    AND creation->'initial_journal'=jsonb_build_object('tenant_id',e.tenant_id,'run_id',e.run_id::text,'sequence',e.sequence::text,
     'event_id',e.event_id::text,'recorded_at',to_char(e.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'digest','sha256:'||encode(e.event_digest,'hex'))
    AND creation->'initial_checkpoint'=jsonb_build_object('tenant_id',c.tenant_id,'run_id',c.run_id::text,'checkpoint_id',c.checkpoint_id::text,
     'superstep',c.superstep::text,'graph',convert_from(c.checkpoint_bytes,'UTF8')::jsonb->'graph','journal_head',creation->'initial_journal','digest','sha256:'||encode(c.checkpoint_digest,'hex'))
    AND NOT EXISTS (SELECT 1 FROM stateknot.agent_admissions a WHERE a.tenant_id=r.tenant_id AND a.run_id=r.run_id)
    AND NOT EXISTS (SELECT 1 FROM stateknot.agent_submission_keys a WHERE a.tenant_id=r.tenant_id AND a.run_id=r.run_id)
    AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_entries a WHERE a.tenant_id=r.tenant_id AND a.run_id=r.run_id)
    AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks a WHERE a.tenant_id=r.tenant_id AND a.run_id=r.run_id)
    AND NOT EXISTS (SELECT 1 FROM stateknot.child_run_budget_accounts a WHERE a.tenant_id=r.tenant_id AND a.parent_run_id=r.run_id)
    AND NOT EXISTS (SELECT 1 FROM stateknot.child_run_ownership a WHERE a.tenant_id=r.tenant_id AND (a.parent_run_id=r.run_id OR a.child_run_id=r.run_id))
  ) THEN RAISE EXCEPTION 'trusted Root requires its positive original creation' USING ERRCODE='SKG02'; END IF;
 END IF;
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION stateknot.guard_root_graph_barrier_projection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.journal_sequence IS NOT DISTINCT FROM OLD.journal_sequence THEN RETURN NEW; END IF;
 IF EXISTS (SELECT 1 FROM stateknot.run_events e WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.sequence=NEW.journal_sequence AND e.event_kind='root-graph-barrier-committed') AND NOT EXISTS (
  SELECT 1 FROM stateknot.root_graph_barriers b JOIN stateknot.run_events e ON e.tenant_id=b.tenant_id AND e.run_id=b.run_id AND e.sequence=b.journal_sequence
  WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.journal_sequence=NEW.journal_sequence AND b.journal_digest=NEW.journal_digest
   AND b.lifecycle_before_bytes=OLD.lifecycle_bytes AND b.lifecycle_before_revision=OLD.lifecycle_revision AND OLD.lifecycle_status='active'
   AND b.lifecycle_after_bytes=NEW.lifecycle_bytes AND b.lifecycle_after_revision=NEW.lifecycle_revision
   AND OLD.lease_attempt_id=e.worker_attempt_id AND OLD.fencing_epoch=e.worker_epoch
   AND OLD.lease_expires_at>clock_timestamp()
   AND ((convert_from(b.bundle_bytes,'UTF8')::jsonb->>'version'='1' AND OLD.agent_deadline_at>clock_timestamp())
    OR (convert_from(b.bundle_bytes,'UTF8')::jsonb->>'version'='2' AND OLD.agent_deadline_at IS NULL
     AND convert_from(b.bundle_bytes,'UTF8')::jsonb->'intent'->'source'->>'kind'='trusted_graph'))
   AND b.successor_checkpoint_id=NEW.checkpoint_id AND b.successor_checkpoint_digest=NEW.checkpoint_digest
 ) THEN RAISE EXCEPTION 'Root barrier must preserve its exact original fenced lifecycle' USING ERRCODE='SKG02'; END IF;
 RETURN NEW;
END $$;
