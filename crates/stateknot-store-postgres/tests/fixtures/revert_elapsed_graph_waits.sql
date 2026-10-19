-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Schema37 only changes the closure guard. Restore its exact schema36 body
-- before the schema36 reconstruction below. Never relabel retained v2 proofs.
SELECT 1 / CASE WHEN EXISTS (
 SELECT 1 FROM stateknot.graph_frame_closures
 WHERE (convert_from(closure_bytes,'UTF8')::jsonb->>'version') IS DISTINCT FROM '1'
 OR (convert_from(closure_bytes,'UTF8')::jsonb->'intent'->>'version') IS DISTINCT FROM '1'
 OR convert_from(closure_bytes,'UTF8')::jsonb->'intent' ? 'stopped_join'
) THEN 0 ELSE 1 END;
CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_closure_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE whole stateknot.graph_frame_closures%ROWTYPE; wire jsonb; claim jsonb; completion jsonb;
 previous text:=''; actual_count bigint; position integer;
BEGIN
 SELECT * INTO whole FROM stateknot.graph_frame_closures WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'closed caller requires its whole closure' USING ERRCODE='SKG02'; END IF;
 wire:=convert_from(whole.closure_bytes,'UTF8')::jsonb;
 IF wire->>'version' IS DISTINCT FROM '1' OR wire->'intent'->>'version' IS DISTINCT FROM '1'
  OR jsonb_typeof(wire->'intent'->'frames') IS DISTINCT FROM 'array' OR jsonb_typeof(wire->'completions') IS DISTINCT FROM 'array'
 THEN RAISE EXCEPTION 'invalid whole closure wire' USING ERRCODE='SKG02'; END IF;
 IF jsonb_array_length(wire->'intent'->'frames')<>whole.frame_count OR jsonb_array_length(wire->'completions')<>whole.frame_count
  OR (wire->'intent'->'direct_usage'->>'unpriced_cost_events')::bigint IS DISTINCT FROM 0
  OR NOT EXISTS (
   SELECT 1 FROM stateknot.runs r JOIN stateknot.graph_frame_stacks s USING(tenant_id,run_id)
   JOIN stateknot.run_events v ON v.tenant_id=r.tenant_id AND v.run_id=r.run_id AND v.sequence=whole.journal_sequence
   JOIN stateknot.run_checkpoints p ON p.tenant_id=r.tenant_id AND p.run_id=r.run_id AND p.checkpoint_id=whole.root_checkpoint_id
   WHERE r.tenant_id=whole.tenant_id AND r.run_id=whole.run_id AND r.quarantined_at IS NULL
    AND r.lease_attempt_id IS NULL AND r.checkpoint_id=whole.root_checkpoint_id AND r.checkpoint_superstep=whole.root_superstep AND r.checkpoint_digest=whole.root_checkpoint_digest
    AND p.graph_namespace='' AND p.frame_identity_digest IS NULL AND p.superstep=whole.root_superstep AND p.checkpoint_digest=whole.root_checkpoint_digest
    AND r.journal_sequence=whole.journal_sequence AND r.journal_digest=whole.journal_digest
    AND convert_from(r.lifecycle_bytes,'UTF8')::jsonb=wire->'intent'->'lifecycle'
    AND ((r.lifecycle_status='cancellation_requested' AND wire->'intent'->'failure_close'='null'::jsonb
      AND wire->'intent'->'failure'=convert_from(r.lifecycle_bytes,'UTF8')::jsonb#>'{state,request,failure}')
     OR (r.lifecycle_status='active' AND EXISTS (SELECT 1 FROM stateknot.run_failure_closes f WHERE f.tenant_id=r.tenant_id AND f.run_id=r.run_id AND f.completed_at IS NULL
      AND convert_from(f.intent_bytes,'UTF8')::jsonb->'failure'=wire->'intent'->'failure'
      AND convert_from(f.intent_bytes,'UTF8')::jsonb->'direct_usage'=wire->'intent'->'direct_usage'
      AND f.journal_digest=decode(substr(wire->'intent'->'failure_close'->>'digest',8),'hex'))))
    AND s.active_namespace='' AND s.active_frame_identity_digest IS NULL AND s.lifetime_starts=whole.lifetime_starts AND s.admission_digest=whole.admission_digest
    AND v.source_kind='control_plane' AND v.event_kind='graph-frames-closed' AND v.projection_digest=whole.compound_digest
    AND v.event_id=whole.journal_event_id AND v.recorded_at=whole.journal_recorded_at AND v.event_digest=whole.journal_digest
  ) THEN RAISE EXCEPTION 'incomplete whole frame closure decision or projection' USING ERRCODE='SKG02'; END IF;
 SELECT count(*) INTO actual_count FROM stateknot.graph_frame_closed_callers c WHERE c.tenant_id=whole.tenant_id AND c.run_id=whole.run_id;
 IF actual_count<>whole.frame_count THEN RAISE EXCEPTION 'incomplete closed caller set' USING ERRCODE='SKG02'; END IF;
 FOR position IN 0..whole.frame_count-1 LOOP
  claim:=wire->'intent'->'frames'->position; completion:=wire->'completions'->position;
  IF completion->'start' IS DISTINCT FROM claim->'caller' OR completion->'outcome'->>'kind' IS DISTINCT FROM 'failed'
   OR completion->'outcome'->'failure' IS DISTINCT FROM wire->'intent'->'caller_failure'
   OR completion->'outcome'->'failure'->>'category' IS DISTINCT FROM 'cancelled'
   OR completion->'outcome'->'failure'->'retry_advice'->>'kind' IS DISTINCT FROM 'never'
   OR completion->'outcome'->'failure'->>'caused_by_event_id' IS DISTINCT FROM whole.journal_event_id::text
   OR completion->'usage' IS DISTINCT FROM '{"graph_depth":"0","graph_steps":"0","model_attempts":"0","model_turns":"0","input_tokens":"0","cached_input_tokens":"0","reasoning_tokens":"0","output_tokens":"0","tool_calls":"0","write_calls":"0","remote_agent_delegations":"0","retries":"0","concurrent_branches":"0","fan_out":"0","input_bytes":"0","output_bytes":"0","event_bytes":"0","checkpoint_bytes":"0","artifact_bytes":"0","known_costs":[],"unpriced_cost_events":"0"}'::jsonb
   OR NOT EXISTS (
    SELECT 1 FROM stateknot.graph_frame_closed_callers c
    JOIN stateknot.graph_frame_entries e USING(tenant_id,run_id,graph_namespace)
    JOIN stateknot.graph_frame_heads h USING(tenant_id,run_id,graph_namespace)
    JOIN stateknot.node_attempts n ON n.tenant_id=c.tenant_id AND n.run_id=c.run_id AND n.attempt_id=c.caller_attempt_id
    WHERE c.tenant_id=whole.tenant_id AND c.run_id=whole.run_id AND c.graph_namespace=claim#>>'{checkpoint,frame,namespace}'
     AND e.parent_namespace=previous AND c.entry_digest=e.compound_digest AND c.frame_identity_digest=e.frame_identity_digest
     AND c.entry_digest=decode(substr(claim->>'entry_digest',8),'hex')
     AND c.frame_identity_digest=decode(substr(claim#>>'{checkpoint,frame,digest}',8),'hex')
     AND c.checkpoint_id=h.checkpoint_id AND c.checkpoint_superstep=h.superstep AND c.checkpoint_digest=h.checkpoint_digest AND c.frame_checkpoint_digest=h.frame_checkpoint_digest
     AND c.checkpoint_id=(claim#>>'{checkpoint,checkpoint,checkpoint_id}')::uuid
     AND c.checkpoint_digest=decode(substr(claim#>>'{checkpoint,checkpoint,digest}',8),'hex') AND c.frame_checkpoint_digest=decode(substr(claim#>>'{checkpoint,digest}',8),'hex')
     AND c.caller_attempt_id=(claim->'caller'->>'attempt_id')::uuid AND c.caller_start_digest=n.start_digest
     AND c.caller_start_digest=decode(substr(claim->'caller'->>'digest',8),'hex') AND c.caller_compound_digest=decode(substr(claim->>'caller_binding_digest',8),'hex')
     AND n.graph_namespace=e.parent_namespace AND n.base_checkpoint_id=e.parent_checkpoint_id AND n.start_digest=c.caller_start_digest
     AND ((n.attempt_id=e.caller_attempt_id AND c.caller_compound_digest=e.compound_digest
       AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_caller_bindings b WHERE b.tenant_id=e.tenant_id AND b.run_id=e.run_id AND b.graph_namespace=e.graph_namespace))
      OR EXISTS(SELECT 1 FROM stateknot.graph_frame_caller_bindings b WHERE b.tenant_id=e.tenant_id AND b.run_id=e.run_id AND b.graph_namespace=e.graph_namespace
       AND b.caller_attempt_id=n.attempt_id AND b.caller_start_digest=n.start_digest AND b.compound_digest=c.caller_compound_digest
       AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_caller_bindings later WHERE later.tenant_id=b.tenant_id AND later.run_id=b.run_id AND later.graph_namespace=b.graph_namespace AND later.worker_epoch>b.worker_epoch)))
     AND convert_from(c.completion_bytes,'UTF8')::jsonb=completion AND c.completion_digest=decode(substr(completion->>'digest',8),'hex')
     AND c.journal_sequence=whole.journal_sequence AND c.journal_event_id=whole.journal_event_id AND c.journal_recorded_at=whole.journal_recorded_at AND c.journal_digest=whole.journal_digest
     AND NOT EXISTS(SELECT 1 FROM stateknot.node_attempt_completions ordinary WHERE ordinary.tenant_id=n.tenant_id AND ordinary.run_id=n.run_id AND ordinary.attempt_id=n.attempt_id)
     AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_returns returned WHERE returned.tenant_id=e.tenant_id AND returned.run_id=e.run_id AND returned.graph_namespace=e.graph_namespace)
   ) THEN RAISE EXCEPTION 'incomplete whole closed caller component' USING ERRCODE='SKG02'; END IF;
  previous:=claim#>>'{checkpoint,frame,namespace}';
 END LOOP;
 IF previous<>whole.active_namespace OR EXISTS(SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=whole.tenant_id AND e.run_id=whole.run_id
  AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=e.tenant_id AND x.run_id=e.run_id AND x.graph_namespace=e.graph_namespace)
  AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_closed_callers c WHERE c.tenant_id=e.tenant_id AND c.run_id=e.run_id AND c.graph_namespace=e.graph_namespace))
  OR EXISTS(SELECT 1 FROM stateknot.tool_invocations t WHERE t.tenant_id=whole.tenant_id AND t.run_id=whole.run_id AND t.current_status NOT IN('committed','failed'))
  OR EXISTS(SELECT 1 FROM stateknot.model_invocations m WHERE m.tenant_id=whole.tenant_id AND m.run_id=whole.run_id AND m.current_status NOT IN('committed','failed'))
  OR EXISTS(SELECT 1 FROM stateknot.child_run_ownership o WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id AND NOT o.settled)
  OR EXISTS(SELECT 1 FROM stateknot.node_attempts n WHERE n.tenant_id=whole.tenant_id AND n.run_id=whole.run_id
   AND NOT EXISTS(SELECT 1 FROM stateknot.node_attempts later WHERE later.tenant_id=n.tenant_id AND later.run_id=n.run_id AND later.activation_digest=n.activation_digest AND later.journal_sequence>n.journal_sequence)
   AND NOT EXISTS(SELECT 1 FROM stateknot.node_attempt_completions c WHERE c.tenant_id=n.tenant_id AND c.run_id=n.run_id AND c.attempt_id=n.attempt_id)
   AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=n.tenant_id AND e.run_id=n.run_id AND e.caller_attempt_id=n.attempt_id)
   AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_caller_bindings b WHERE b.tenant_id=n.tenant_id AND b.run_id=n.run_id AND b.caller_attempt_id=n.attempt_id))
 THEN RAISE EXCEPTION 'whole closure cannot erase open frames or unsettled work' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
DELETE FROM _sqlx_migrations WHERE version=37;

-- Isolated source reconstruction only. Refuse any retained original-condition
-- graph fact, including a damaged V3 bundle whose condition rows are absent.
SELECT 1 / CASE WHEN
 EXISTS(SELECT 1 FROM stateknot.graph_wait_conditions WHERE profile<>'legacy_registration')
 OR EXISTS(SELECT 1 FROM stateknot.root_graph_barriers WHERE convert_from(bundle_bytes,'UTF8')::jsonb->>'version'='3')
 OR EXISTS(SELECT 1 FROM stateknot.graph_frame_barriers WHERE convert_from(barrier_bytes,'UTF8')::jsonb->>'version'='3')
 THEN 0 ELSE 1 END;
DROP TRIGGER root_barriers_wait_inventory_complete ON stateknot.root_graph_barriers;
DROP TRIGGER frame_barriers_wait_inventory_complete ON stateknot.graph_frame_barriers;
DROP TRIGGER wait_registrations_identity ON stateknot.run_wait_registrations;
DROP TABLE stateknot.graph_wait_conditions;
DROP FUNCTION stateknot.guard_wait_registration_identity();
DROP FUNCTION stateknot.guard_graph_wait_condition_original();
DROP FUNCTION stateknot.guard_graph_wait_inventory_complete();

-- Restore every exact schema35 guard before removing the ledger row. Dropping
-- only the derived identity table would leave a false schema35 fixture.
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

CREATE OR REPLACE FUNCTION stateknot.guard_root_graph_barrier_component_complete() RETURNS trigger LANGUAGE plpgsql AS $$
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

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_wait_complete() RETURNS trigger LANGUAGE plpgsql AS $$
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

DELETE FROM _sqlx_migrations WHERE version=36;
