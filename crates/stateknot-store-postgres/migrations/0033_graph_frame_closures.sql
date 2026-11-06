-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Control-plane closure preserves the physical caller's original worker fence.
-- Ordinary completion rows retain their original worker-event foreign key.
CREATE TABLE stateknot.graph_frame_closures (
 tenant_id text NOT NULL, run_id uuid NOT NULL, admission_digest bytea NOT NULL,
 root_checkpoint_id uuid NOT NULL, root_superstep bigint NOT NULL, root_checkpoint_digest bytea NOT NULL,
 active_namespace text NOT NULL, active_frame_identity_digest bytea NOT NULL,
 lifetime_starts integer NOT NULL, frame_count integer NOT NULL,
 intent_digest bytea NOT NULL, compound_digest bytea NOT NULL,
 closure_bytes bytea NOT NULL, closure_checksum bytea GENERATED ALWAYS AS (sha256(closure_bytes)) STORED,
 journal_sequence bigint NOT NULL, journal_event_id uuid NOT NULL,
 journal_recorded_at timestamptz(6) NOT NULL, journal_digest bytea NOT NULL,
 PRIMARY KEY (tenant_id,run_id),
 CONSTRAINT graph_frame_closures_event_unique UNIQUE (tenant_id,run_id,journal_sequence),
 CONSTRAINT graph_frame_closures_shape CHECK (
  active_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$'
  AND lifetime_starts BETWEEN 1 AND 4096 AND frame_count BETWEEN 1 AND 7
  AND root_superstep>=0 AND journal_sequence>1
  AND octet_length(admission_digest)=32 AND octet_length(root_checkpoint_digest)=32
  AND octet_length(active_frame_identity_digest)=32 AND octet_length(intent_digest)=32
  AND octet_length(compound_digest)=32 AND octet_length(journal_digest)=32
  AND octet_length(closure_bytes) BETWEEN 1 AND 4194304
 ),
 CONSTRAINT graph_frame_closures_run_fk FOREIGN KEY (tenant_id,run_id)
  REFERENCES stateknot.runs (tenant_id,run_id) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_closures_root_fk FOREIGN KEY (tenant_id,run_id,root_checkpoint_id)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,checkpoint_id) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_closures_event_fk FOREIGN KEY (tenant_id,run_id,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
  REFERENCES stateknot.run_events (tenant_id,run_id,sequence,event_id,recorded_at,event_digest) ON DELETE RESTRICT
);
CREATE TABLE stateknot.graph_frame_closed_callers (
 tenant_id text NOT NULL, run_id uuid NOT NULL, graph_namespace text NOT NULL,
 frame_identity_digest bytea NOT NULL, entry_digest bytea NOT NULL,
 checkpoint_id uuid NOT NULL, checkpoint_superstep bigint NOT NULL, checkpoint_digest bytea NOT NULL,
 frame_checkpoint_digest bytea NOT NULL,
 caller_attempt_id uuid NOT NULL, caller_start_digest bytea NOT NULL, caller_compound_digest bytea NOT NULL,
 completion_digest bytea NOT NULL, completion_bytes bytea NOT NULL,
 completion_checksum bytea GENERATED ALWAYS AS (sha256(completion_bytes)) STORED,
 journal_sequence bigint NOT NULL, journal_event_id uuid NOT NULL,
 journal_recorded_at timestamptz(6) NOT NULL, journal_digest bytea NOT NULL,
 PRIMARY KEY (tenant_id,run_id,graph_namespace),
 CONSTRAINT graph_frame_closed_callers_attempt_unique UNIQUE (tenant_id,run_id,caller_attempt_id),
 CONSTRAINT graph_frame_closed_callers_shape CHECK (
  graph_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$'
  AND checkpoint_superstep>=0 AND journal_sequence>1
  AND octet_length(frame_identity_digest)=32 AND octet_length(entry_digest)=32
  AND octet_length(checkpoint_digest)=32 AND octet_length(frame_checkpoint_digest)=32
  AND octet_length(caller_start_digest)=32 AND octet_length(caller_compound_digest)=32
  AND octet_length(completion_digest)=32 AND octet_length(journal_digest)=32
  AND octet_length(completion_bytes) BETWEEN 1 AND 4194304
 ),
 CONSTRAINT graph_frame_closed_callers_closure_fk FOREIGN KEY (tenant_id,run_id,journal_sequence)
  REFERENCES stateknot.graph_frame_closures (tenant_id,run_id,journal_sequence) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_closed_callers_entry_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,frame_identity_digest)
  REFERENCES stateknot.graph_frame_entries (tenant_id,run_id,graph_namespace,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_closed_callers_checkpoint_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,checkpoint_id,checkpoint_superstep,checkpoint_digest,frame_identity_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_closed_callers_start_fk FOREIGN KEY (tenant_id,run_id,caller_attempt_id)
  REFERENCES stateknot.node_attempts (tenant_id,run_id,attempt_id) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_closed_callers_event_fk FOREIGN KEY (tenant_id,run_id,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
  REFERENCES stateknot.run_events (tenant_id,run_id,sequence,event_id,recorded_at,event_digest) ON DELETE RESTRICT
);
CREATE TRIGGER graph_frame_closures_immutable BEFORE UPDATE OR DELETE ON stateknot.graph_frame_closures
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_immutable();
CREATE TRIGGER graph_frame_closed_callers_immutable BEFORE UPDATE OR DELETE ON stateknot.graph_frame_closed_callers
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_immutable();

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_stack_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE total bigint; maximum integer;
BEGIN
 SELECT count(*),max(ordinal) INTO total,maximum FROM stateknot.graph_frame_entries WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF total<>NEW.lifetime_starts OR maximum IS DISTINCT FROM NEW.lifetime_starts
  OR EXISTS (SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id
   AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=e.tenant_id AND x.run_id=e.run_id AND x.graph_namespace=e.graph_namespace)
   AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_closed_callers c WHERE c.tenant_id=e.tenant_id AND c.run_id=e.run_id AND c.graph_namespace=e.graph_namespace)
   AND e.graph_namespace<>NEW.active_namespace AND left(NEW.active_namespace,length(e.graph_namespace)+1)<>e.graph_namespace||'/')
  OR (NEW.active_namespace<>'' AND NOT EXISTS (
   SELECT 1 FROM stateknot.graph_frame_entries e JOIN stateknot.graph_frame_heads h USING (tenant_id,run_id,graph_namespace)
   JOIN stateknot.run_events v ON v.tenant_id=e.tenant_id AND v.run_id=e.run_id AND v.sequence=e.journal_sequence
   WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.graph_namespace=NEW.active_namespace AND e.frame_identity_digest=NEW.active_frame_identity_digest
    AND h.frame_identity_digest=e.frame_identity_digest AND v.event_kind='graph-frame-entered' AND v.projection_digest=e.compound_digest
    AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=e.tenant_id AND x.run_id=e.run_id AND x.graph_namespace=e.graph_namespace)
   AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_closed_callers c WHERE c.tenant_id=e.tenant_id AND c.run_id=e.run_id AND c.graph_namespace=e.graph_namespace)
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
 IF NEW.lifetime_starts=OLD.lifetime_starts AND NEW.active_namespace='' AND NEW.active_frame_identity_digest IS NULL
  AND EXISTS (SELECT 1 FROM stateknot.graph_frame_closures c WHERE c.tenant_id=OLD.tenant_id AND c.run_id=OLD.run_id
   AND c.active_namespace=OLD.active_namespace AND c.active_frame_identity_digest=OLD.active_frame_identity_digest AND c.lifetime_starts=OLD.lifetime_starts)
 THEN RETURN NEW; END IF;
 RAISE EXCEPTION 'frame stack transition requires whole push or return' USING ERRCODE='SKG02';
END $$;

CREATE FUNCTION stateknot.guard_graph_frame_closure_complete() RETURNS trigger LANGUAGE plpgsql AS $$
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
CREATE CONSTRAINT TRIGGER frame_closures_complete AFTER INSERT ON stateknot.graph_frame_closures
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_closure_complete();
CREATE CONSTRAINT TRIGGER frame_closed_callers_complete AFTER INSERT ON stateknot.graph_frame_closed_callers
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_closure_complete();
CREATE FUNCTION stateknot.guard_graph_frame_closure_event_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.event_kind='graph-frames-closed' AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_closures c WHERE c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id
  AND c.journal_sequence=NEW.sequence AND c.journal_event_id=NEW.event_id AND c.journal_recorded_at=NEW.recorded_at AND c.journal_digest=NEW.event_digest
  AND c.compound_digest=NEW.projection_digest AND NEW.source_kind='control_plane')
 THEN RAISE EXCEPTION 'closure event requires the whole stack and callers' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER run_events_frame_closure_complete AFTER INSERT ON stateknot.run_events
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_closure_event_complete();
CREATE FUNCTION stateknot.guard_graph_frame_closed_run() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE whole stateknot.graph_frame_closures%ROWTYPE; intent jsonb; lifecycle jsonb;
BEGIN
 SELECT * INTO whole FROM stateknot.graph_frame_closures WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF NOT FOUND THEN
  IF NEW.lifecycle_status IN('succeeded','failed','cancelled') AND EXISTS(SELECT 1 FROM stateknot.graph_frame_stacks s WHERE s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id AND s.active_namespace<>'')
  THEN RAISE EXCEPTION 'terminal Run requires complete frame return or closure' USING ERRCODE='SKG02'; END IF;
  RETURN NEW;
 END IF;
 intent:=convert_from(whole.closure_bytes,'UTF8')::jsonb->'intent';lifecycle:=convert_from(NEW.lifecycle_bytes,'UTF8')::jsonb;
 IF NEW.lease_attempt_id IS NOT NULL OR NEW.checkpoint_id IS DISTINCT FROM whole.root_checkpoint_id
  OR NEW.checkpoint_superstep IS DISTINCT FROM whole.root_superstep OR NEW.checkpoint_digest IS DISTINCT FROM whole.root_checkpoint_digest
  OR (intent->'failure_close'='null'::jsonb AND NOT (
    (NEW.lifecycle_status='cancellation_requested' AND lifecycle=intent->'lifecycle')
    OR (NEW.lifecycle_status='cancelled' AND lifecycle#>'{state,cancellation,request}'=intent->'lifecycle'#>'{state,request}'))) 
  OR (intent->'failure_close'<>'null'::jsonb AND NEW.lifecycle_status NOT IN('active','failed'))
 THEN RAISE EXCEPTION 'closed frames retain their original Run decision and forbid execution' USING ERRCODE='SKG02'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER runs_frame_closed_guard BEFORE UPDATE ON stateknot.runs
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_closed_run();
