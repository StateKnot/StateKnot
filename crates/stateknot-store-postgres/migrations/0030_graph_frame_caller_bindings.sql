-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- A newer physical framework caller belongs to the existing logical child;
-- it never creates another child or grants application dispatch permission.
CREATE TABLE stateknot.graph_frame_caller_bindings (
 tenant_id text NOT NULL, run_id uuid NOT NULL, graph_namespace text NOT NULL,
 frame_identity_digest bytea NOT NULL,
 previous_attempt_id uuid NOT NULL, previous_start_digest bytea NOT NULL,
 caller_attempt_id uuid NOT NULL, caller_start_digest bytea NOT NULL,
 worker_attempt_id uuid NOT NULL, worker_epoch bigint NOT NULL,
 active_checkpoint_id uuid NOT NULL, active_superstep bigint NOT NULL,
 active_checkpoint_digest bytea NOT NULL, active_frame_checkpoint_digest bytea NOT NULL,
 previous_compound_digest bytea NOT NULL, compound_digest bytea NOT NULL,
 binding_bytes bytea NOT NULL, binding_checksum bytea GENERATED ALWAYS AS (sha256(binding_bytes)) STORED,
 journal_sequence bigint NOT NULL, journal_event_id uuid NOT NULL,
 journal_recorded_at timestamptz(6) NOT NULL, journal_digest bytea NOT NULL,
 PRIMARY KEY (tenant_id,run_id,graph_namespace,worker_epoch),
 CONSTRAINT graph_frame_caller_bindings_attempt_unique UNIQUE (tenant_id,run_id,caller_attempt_id),
 CONSTRAINT graph_frame_caller_bindings_event_unique UNIQUE (tenant_id,run_id,journal_sequence),
 CONSTRAINT graph_frame_caller_bindings_shape CHECK (
  graph_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$'
  AND caller_attempt_id<>previous_attempt_id AND caller_attempt_id<>worker_attempt_id
  AND worker_epoch>1 AND active_superstep>=0 AND journal_sequence>1
  AND octet_length(frame_identity_digest)=32 AND octet_length(previous_start_digest)=32
  AND octet_length(caller_start_digest)=32 AND octet_length(active_checkpoint_digest)=32
  AND octet_length(active_frame_checkpoint_digest)=32 AND octet_length(previous_compound_digest)=32
  AND octet_length(compound_digest)=32 AND octet_length(journal_digest)=32
  AND octet_length(binding_bytes)>=1 AND octet_length(binding_bytes)<=1179648
 ),
 CONSTRAINT graph_frame_caller_bindings_entry_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,frame_identity_digest)
  REFERENCES stateknot.graph_frame_entries (tenant_id,run_id,graph_namespace,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_caller_bindings_previous_fk FOREIGN KEY (tenant_id,run_id,previous_attempt_id)
  REFERENCES stateknot.node_attempts (tenant_id,run_id,attempt_id) ON DELETE RESTRICT,
 -- A binding is inserted before its start so the scoped insert predicate can
 -- admit precisely this suspended framework caller; all facts flush together.
 CONSTRAINT graph_frame_caller_bindings_start_fk FOREIGN KEY (tenant_id,run_id,caller_attempt_id)
  REFERENCES stateknot.node_attempts (tenant_id,run_id,attempt_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
 CONSTRAINT graph_frame_caller_bindings_checkpoint_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,active_checkpoint_id,active_superstep,active_checkpoint_digest,frame_identity_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_caller_bindings_event_fk FOREIGN KEY (tenant_id,run_id,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
  REFERENCES stateknot.run_events (tenant_id,run_id,sequence,event_id,recorded_at,event_digest) ON DELETE RESTRICT
);
CREATE TRIGGER graph_frame_caller_bindings_immutable BEFORE UPDATE OR DELETE ON stateknot.graph_frame_caller_bindings
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_immutable();

CREATE FUNCTION stateknot.guard_graph_frame_caller_binding_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e
  JOIN stateknot.node_attempts previous_start ON previous_start.tenant_id=e.tenant_id AND previous_start.run_id=e.run_id AND previous_start.attempt_id=NEW.previous_attempt_id
  JOIN stateknot.node_attempts current_start ON current_start.tenant_id=e.tenant_id AND current_start.run_id=e.run_id AND current_start.attempt_id=NEW.caller_attempt_id
  JOIN stateknot.run_attempt_claims claim ON claim.tenant_id=e.tenant_id AND claim.run_id=e.run_id AND claim.attempt_id=current_start.attempt_id
  JOIN stateknot.run_events v ON v.tenant_id=e.tenant_id AND v.run_id=e.run_id AND v.sequence=NEW.journal_sequence
  JOIN stateknot.graph_frame_heads h ON h.tenant_id=e.tenant_id AND h.run_id=e.run_id AND h.graph_namespace=e.graph_namespace
  JOIN stateknot.graph_frame_stacks s ON s.tenant_id=e.tenant_id AND s.run_id=e.run_id
  JOIN stateknot.runs r ON r.tenant_id=e.tenant_id AND r.run_id=e.run_id
  WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.graph_namespace=NEW.graph_namespace
   AND e.frame_identity_digest=NEW.frame_identity_digest
   AND previous_start.start_digest=NEW.previous_start_digest AND current_start.start_digest=NEW.caller_start_digest
   AND claim.claim_kind='node_attempt' AND claim.activation_digest=current_start.activation_digest
   AND claim.journal_sequence=NEW.journal_sequence AND claim.journal_event_id=NEW.journal_event_id
   AND claim.journal_recorded_at=NEW.journal_recorded_at AND claim.journal_digest=NEW.journal_digest
   AND claim.claimed_at=NEW.journal_recorded_at
   AND previous_start.graph_namespace=e.parent_namespace AND current_start.graph_namespace=e.parent_namespace
   AND current_start.base_checkpoint_id=previous_start.base_checkpoint_id AND current_start.base_superstep=previous_start.base_superstep
   AND current_start.base_checkpoint_digest=previous_start.base_checkpoint_digest AND current_start.node_id=previous_start.node_id
   AND current_start.activation_input_digest=previous_start.activation_input_digest AND current_start.activation_digest=previous_start.activation_digest
   AND previous_start.fence_epoch<current_start.fence_epoch AND current_start.fence_epoch=NEW.worker_epoch AND current_start.fence_attempt_id=NEW.worker_attempt_id
   AND r.lease_attempt_id=NEW.worker_attempt_id AND r.fencing_epoch=NEW.worker_epoch
   AND r.lease_expires_at>clock_timestamp() AND r.agent_deadline_at>clock_timestamp()
   AND r.journal_sequence=NEW.journal_sequence AND r.journal_digest=NEW.journal_digest
   AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings newer
       WHERE newer.tenant_id=e.tenant_id AND newer.run_id=e.run_id AND newer.graph_namespace=e.graph_namespace
        AND newer.worker_epoch>previous_start.fence_epoch AND newer.worker_epoch<NEW.worker_epoch)
   AND previous_start.journal_sequence<NEW.journal_sequence AND current_start.journal_sequence=NEW.journal_sequence
   AND current_start.journal_event_id=NEW.journal_event_id AND current_start.journal_recorded_at=NEW.journal_recorded_at AND current_start.journal_digest=NEW.journal_digest
   AND NOT EXISTS (SELECT 1 FROM stateknot.node_attempt_completions c WHERE c.tenant_id=e.tenant_id AND c.run_id=e.run_id AND c.attempt_id IN (previous_start.attempt_id,current_start.attempt_id))
   AND v.event_kind='graph-frame-caller-rebound' AND v.source_kind='worker'
   AND v.worker_attempt_id=NEW.worker_attempt_id AND v.worker_epoch=NEW.worker_epoch AND v.projection_digest=NEW.compound_digest
   AND h.frame_identity_digest=e.frame_identity_digest AND h.checkpoint_id=NEW.active_checkpoint_id
   AND h.superstep=NEW.active_superstep AND h.checkpoint_digest=NEW.active_checkpoint_digest AND h.frame_checkpoint_digest=NEW.active_frame_checkpoint_digest
   AND s.active_namespace=e.graph_namespace AND s.active_frame_identity_digest=e.frame_identity_digest
   AND ((previous_start.attempt_id=e.caller_attempt_id AND NEW.previous_compound_digest=e.compound_digest)
    OR EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings b
       WHERE b.tenant_id=e.tenant_id AND b.run_id=e.run_id AND b.graph_namespace=e.graph_namespace
        AND b.caller_attempt_id=previous_start.attempt_id AND b.caller_start_digest=previous_start.start_digest
        AND b.compound_digest=NEW.previous_compound_digest AND b.worker_epoch<NEW.worker_epoch))
 ) THEN RAISE EXCEPTION 'incomplete framework caller binding' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER frame_caller_bindings_complete AFTER INSERT ON stateknot.graph_frame_caller_bindings
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_caller_binding_complete();

CREATE FUNCTION stateknot.guard_graph_frame_caller_event_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.event_kind='graph-frame-caller-rebound' AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_caller_bindings b
  WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.journal_sequence=NEW.sequence
   AND b.journal_event_id=NEW.event_id AND b.journal_recorded_at=NEW.recorded_at AND b.journal_digest=NEW.event_digest
   AND b.compound_digest=NEW.projection_digest AND NEW.source_kind='worker'
 ) THEN RAISE EXCEPTION 'framework binding event requires its whole transaction' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER run_events_frame_caller_complete AFTER INSERT ON stateknot.run_events
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_caller_event_complete();

-- The only exception to active-leaf dispatch is a fully bound framework start.
-- All original and rebound callers still require a whole return to complete.
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
