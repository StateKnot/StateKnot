-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- The terminal barrier is an existing immutable proof. A return settles it
-- once with the current caller, parent result, stack pop and one journal fact.
CREATE TABLE stateknot.graph_frame_returns (
 tenant_id text NOT NULL, run_id uuid NOT NULL, graph_namespace text NOT NULL,
 frame_identity_digest bytea NOT NULL, entry_digest bytea NOT NULL,
 terminal_base_checkpoint_id uuid NOT NULL, terminal_checkpoint_id uuid NOT NULL,
 terminal_superstep bigint NOT NULL, terminal_checkpoint_digest bytea NOT NULL,
 terminal_frame_checkpoint_digest bytea NOT NULL, terminal_barrier_digest bytea NOT NULL,
 caller_attempt_id uuid NOT NULL, caller_start_digest bytea NOT NULL, caller_compound_digest bytea NOT NULL,
 parent_namespace text NOT NULL, parent_checkpoint_id uuid NOT NULL, parent_superstep bigint NOT NULL,
 parent_checkpoint_digest bytea NOT NULL, node_id text NOT NULL, activation_input_digest bytea NOT NULL,
 result_intent_digest bytea NOT NULL, result_record_digest bytea NOT NULL, completion_digest bytea NOT NULL,
 intent_digest bytea NOT NULL, compound_digest bytea NOT NULL,
 return_bytes bytea NOT NULL, return_checksum bytea GENERATED ALWAYS AS (sha256(return_bytes)) STORED,
 journal_sequence bigint NOT NULL, journal_event_id uuid NOT NULL,
 journal_recorded_at timestamptz(6) NOT NULL, journal_digest bytea NOT NULL,
 PRIMARY KEY (tenant_id,run_id,graph_namespace),
 CONSTRAINT graph_frame_returns_caller_unique UNIQUE (tenant_id,run_id,caller_attempt_id),
 CONSTRAINT graph_frame_returns_event_unique UNIQUE (tenant_id,run_id,journal_sequence),
 CONSTRAINT graph_frame_returns_shape CHECK (
  graph_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$'
  AND (parent_namespace='' OR parent_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,5}$')
  AND terminal_superstep>0 AND parent_superstep>=0 AND journal_sequence>1
  AND octet_length(frame_identity_digest)=32 AND octet_length(entry_digest)=32
  AND octet_length(terminal_checkpoint_digest)=32 AND octet_length(terminal_frame_checkpoint_digest)=32
  AND octet_length(terminal_barrier_digest)=32 AND octet_length(caller_start_digest)=32 AND octet_length(caller_compound_digest)=32
  AND octet_length(parent_checkpoint_digest)=32 AND octet_length(activation_input_digest)=32
  AND octet_length(result_intent_digest)=32 AND octet_length(result_record_digest)=32 AND octet_length(completion_digest)=32
  AND octet_length(intent_digest)=32 AND octet_length(compound_digest)=32 AND octet_length(journal_digest)=32
  AND octet_length(return_bytes)>=1 AND octet_length(return_bytes)<=4194304
 ),
 CONSTRAINT graph_frame_returns_entry_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,frame_identity_digest)
  REFERENCES stateknot.graph_frame_entries (tenant_id,run_id,graph_namespace,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_returns_terminal_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,terminal_base_checkpoint_id)
  REFERENCES stateknot.graph_frame_barriers (tenant_id,run_id,graph_namespace,base_checkpoint_id) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_returns_checkpoint_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,terminal_checkpoint_id,terminal_superstep,terminal_checkpoint_digest,frame_identity_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_returns_start_fk FOREIGN KEY (tenant_id,run_id,caller_attempt_id)
  REFERENCES stateknot.node_attempts (tenant_id,run_id,attempt_id) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_returns_completion_fk FOREIGN KEY (tenant_id,run_id,caller_attempt_id)
  REFERENCES stateknot.node_attempt_completions (tenant_id,run_id,attempt_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
 CONSTRAINT graph_frame_returns_result_fk FOREIGN KEY (tenant_id,run_id,parent_checkpoint_id,parent_namespace,node_id)
  REFERENCES stateknot.pending_node_results (tenant_id,run_id,base_checkpoint_id,graph_namespace,node_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
 CONSTRAINT graph_frame_returns_event_fk FOREIGN KEY (tenant_id,run_id,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
  REFERENCES stateknot.run_events (tenant_id,run_id,sequence,event_id,recorded_at,event_digest) ON DELETE RESTRICT
);
CREATE TRIGGER graph_frame_returns_immutable BEFORE UPDATE OR DELETE ON stateknot.graph_frame_returns
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_immutable();

CREATE FUNCTION stateknot.guard_graph_frame_return_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e
  JOIN stateknot.graph_frame_barriers b ON b.tenant_id=e.tenant_id AND b.run_id=e.run_id AND b.graph_namespace=e.graph_namespace AND b.base_checkpoint_id=NEW.terminal_base_checkpoint_id
  JOIN stateknot.graph_frame_heads h ON h.tenant_id=e.tenant_id AND h.run_id=e.run_id AND h.graph_namespace=e.graph_namespace
  JOIN stateknot.node_attempts n ON n.tenant_id=e.tenant_id AND n.run_id=e.run_id AND n.attempt_id=NEW.caller_attempt_id
  JOIN stateknot.node_attempt_completions c ON c.tenant_id=n.tenant_id AND c.run_id=n.run_id AND c.attempt_id=n.attempt_id
  JOIN stateknot.pending_node_results p ON p.tenant_id=e.tenant_id AND p.run_id=e.run_id AND p.graph_namespace=e.parent_namespace AND p.base_checkpoint_id=e.parent_checkpoint_id AND p.node_id=n.node_id
  JOIN stateknot.graph_frame_stacks s ON s.tenant_id=e.tenant_id AND s.run_id=e.run_id
  JOIN stateknot.runs r ON r.tenant_id=e.tenant_id AND r.run_id=e.run_id
  JOIN stateknot.run_events v ON v.tenant_id=e.tenant_id AND v.run_id=e.run_id AND v.sequence=NEW.journal_sequence
  WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.graph_namespace=NEW.graph_namespace
   AND e.frame_identity_digest=NEW.frame_identity_digest AND e.compound_digest=NEW.entry_digest
   AND e.parent_namespace=NEW.parent_namespace AND e.parent_checkpoint_id=NEW.parent_checkpoint_id
   AND b.compound_digest=NEW.terminal_barrier_digest AND b.successor_checkpoint_id=NEW.terminal_checkpoint_id
   AND b.successor_superstep=NEW.terminal_superstep AND b.successor_checkpoint_digest=NEW.terminal_checkpoint_digest
   AND b.successor_frame_checkpoint_digest=NEW.terminal_frame_checkpoint_digest
   AND convert_from(b.barrier_bytes,'UTF8')::jsonb->'disposition'->>'kind'='terminal'
   AND b.journal_sequence<NEW.journal_sequence
   AND h.checkpoint_id=NEW.terminal_checkpoint_id AND h.superstep=NEW.terminal_superstep
   AND h.checkpoint_digest=NEW.terminal_checkpoint_digest AND h.frame_checkpoint_digest=NEW.terminal_frame_checkpoint_digest
   AND h.frame_identity_digest=NEW.frame_identity_digest
   AND n.start_digest=NEW.caller_start_digest AND n.graph_namespace=NEW.parent_namespace
   AND n.base_checkpoint_id=NEW.parent_checkpoint_id AND n.base_superstep=NEW.parent_superstep AND n.base_checkpoint_digest=NEW.parent_checkpoint_digest
   AND n.node_id=NEW.node_id AND n.activation_input_digest=NEW.activation_input_digest AND n.journal_sequence<NEW.journal_sequence
   AND n.fence_attempt_id=v.worker_attempt_id AND n.fence_epoch=v.worker_epoch
   AND ((n.attempt_id=e.caller_attempt_id AND NEW.caller_compound_digest=e.compound_digest
     AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings newer WHERE newer.tenant_id=e.tenant_id AND newer.run_id=e.run_id AND newer.graph_namespace=e.graph_namespace))
    OR EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings latest WHERE latest.tenant_id=e.tenant_id AND latest.run_id=e.run_id AND latest.graph_namespace=e.graph_namespace
     AND latest.caller_attempt_id=n.attempt_id AND latest.caller_start_digest=n.start_digest AND latest.compound_digest=NEW.caller_compound_digest
     AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings newer WHERE newer.tenant_id=e.tenant_id AND newer.run_id=e.run_id AND newer.graph_namespace=e.graph_namespace AND newer.worker_epoch>latest.worker_epoch)))
   AND c.status='succeeded' AND c.start_digest=n.start_digest AND c.completion_digest=NEW.completion_digest
   AND c.result_intent_digest=NEW.result_intent_digest AND c.result_record_digest=NEW.result_record_digest
   AND c.journal_sequence=NEW.journal_sequence AND c.journal_event_id=NEW.journal_event_id AND c.journal_recorded_at=NEW.journal_recorded_at AND c.journal_digest=NEW.journal_digest
   AND p.node_attempt_id=n.attempt_id AND p.intent_digest=NEW.result_intent_digest AND p.record_digest=NEW.result_record_digest AND p.control_kind='route'
   AND p.journal_sequence=NEW.journal_sequence AND p.journal_event_id=NEW.journal_event_id AND p.journal_recorded_at=NEW.journal_recorded_at AND p.journal_digest=NEW.journal_digest
   AND p.fence_attempt_id=n.fence_attempt_id AND p.fence_epoch=n.fence_epoch
   AND s.active_namespace=NEW.parent_namespace AND s.lifetime_starts>=e.ordinal
   AND ((NEW.parent_namespace='' AND s.active_frame_identity_digest IS NULL AND r.checkpoint_id=NEW.parent_checkpoint_id AND r.checkpoint_superstep=NEW.parent_superstep AND r.checkpoint_digest=NEW.parent_checkpoint_digest)
    OR EXISTS (SELECT 1 FROM stateknot.graph_frame_heads parent WHERE parent.tenant_id=e.tenant_id AND parent.run_id=e.run_id AND parent.graph_namespace=NEW.parent_namespace
     AND parent.checkpoint_id=NEW.parent_checkpoint_id AND parent.superstep=NEW.parent_superstep AND parent.checkpoint_digest=NEW.parent_checkpoint_digest AND s.active_frame_identity_digest=parent.frame_identity_digest))
   AND v.event_kind='graph-frame-returned' AND v.source_kind='worker' AND v.projection_digest=NEW.compound_digest
   AND r.lease_attempt_id=v.worker_attempt_id AND r.fencing_epoch=v.worker_epoch
   AND r.lease_expires_at>clock_timestamp() AND r.agent_deadline_at>clock_timestamp()
   AND r.journal_sequence=NEW.journal_sequence AND r.journal_digest=NEW.journal_digest
 ) THEN RAISE EXCEPTION 'incomplete whole graph frame return' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER frame_returns_complete AFTER INSERT ON stateknot.graph_frame_returns
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_return_complete();

CREATE FUNCTION stateknot.guard_graph_frame_return_event_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.event_kind='graph-frame-returned' AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=NEW.tenant_id AND x.run_id=NEW.run_id AND x.journal_sequence=NEW.sequence
   AND x.journal_event_id=NEW.event_id AND x.journal_recorded_at=NEW.recorded_at AND x.journal_digest=NEW.event_digest AND x.compound_digest=NEW.projection_digest AND NEW.source_kind='worker'
 ) THEN RAISE EXCEPTION 'frame return event requires whole return' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER run_events_frame_return_complete AFTER INSERT ON stateknot.run_events
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_return_event_complete();

-- Immutable frame facts and lifetime ordinals survive pops. The current leaf
-- must be unreturned and every open frame must be on its exact ancestor path.
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
CREATE FUNCTION stateknot.guard_graph_frame_stack_transition() RETURNS trigger LANGUAGE plpgsql AS $$
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
CREATE TRIGGER frame_stacks_transition BEFORE UPDATE ON stateknot.graph_frame_stacks FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_stack_transition();

-- Only the exact whole return admits a framework completion.
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
 ) AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=tenant AND x.run_id=identity
   AND x.caller_attempt_id=(data->>'attempt_id')::uuid AND x.parent_namespace=namespace
   AND x.completion_digest=decode(substr(data->>'completion_digest',3),'hex')
   AND x.journal_sequence=(data->>'journal_sequence')::bigint AND x.journal_event_id=(data->>'journal_event_id')::uuid
   AND x.journal_recorded_at=(data->>'journal_recorded_at')::timestamptz AND x.journal_digest=decode(substr(data->>'journal_digest',3),'hex')
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
