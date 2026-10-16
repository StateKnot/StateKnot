-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- A scoped successor is admitted only by its whole barrier. Neither a legacy
-- checkpoint projection nor a result-consumption row grants that authority.
CREATE TABLE stateknot.graph_frame_barriers (
 tenant_id text NOT NULL, run_id uuid NOT NULL, graph_namespace text NOT NULL,
 frame_identity_digest bytea NOT NULL,
 base_checkpoint_id uuid NOT NULL, base_superstep bigint NOT NULL,
 base_checkpoint_digest bytea NOT NULL, base_frame_checkpoint_digest bytea NOT NULL,
 successor_checkpoint_id uuid NOT NULL, successor_superstep bigint NOT NULL,
 successor_checkpoint_digest bytea NOT NULL, successor_frame_checkpoint_digest bytea NOT NULL,
 result_count integer NOT NULL,
 barrier_intent_digest bytea NOT NULL, scope_intent_digest bytea NOT NULL,
 compound_digest bytea NOT NULL, barrier_bytes bytea NOT NULL,
 barrier_checksum bytea GENERATED ALWAYS AS (sha256(barrier_bytes)) STORED,
 journal_sequence bigint NOT NULL, journal_event_id uuid NOT NULL,
 journal_recorded_at timestamptz(6) NOT NULL, journal_digest bytea NOT NULL,
 PRIMARY KEY (tenant_id,run_id,graph_namespace,base_checkpoint_id),
 CONSTRAINT graph_frame_barriers_position_unique UNIQUE (tenant_id,run_id,graph_namespace,base_superstep),
 CONSTRAINT graph_frame_barriers_successor_unique UNIQUE (tenant_id,run_id,graph_namespace,successor_checkpoint_id),
 CONSTRAINT graph_frame_barriers_event_unique UNIQUE (tenant_id,run_id,journal_sequence),
 CONSTRAINT graph_frame_barriers_shape CHECK (
  graph_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$'
  AND base_superstep>=0 AND successor_superstep>base_superstep
  AND successor_superstep-base_superstep=1
  AND result_count>=1 AND result_count<=1024
  AND octet_length(frame_identity_digest)=32
  AND octet_length(base_checkpoint_digest)=32 AND octet_length(base_frame_checkpoint_digest)=32
  AND octet_length(successor_checkpoint_digest)=32 AND octet_length(successor_frame_checkpoint_digest)=32
  AND octet_length(barrier_intent_digest)=32 AND octet_length(scope_intent_digest)=32
  AND octet_length(compound_digest)=32 AND octet_length(journal_digest)=32
  AND octet_length(barrier_bytes)>=1 AND octet_length(barrier_bytes)<=4194304
  AND journal_sequence>1
 ),
 CONSTRAINT graph_frame_barriers_entry_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,frame_identity_digest)
  REFERENCES stateknot.graph_frame_entries (tenant_id,run_id,graph_namespace,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_barriers_base_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,base_checkpoint_id,base_superstep,base_checkpoint_digest,frame_identity_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_barriers_successor_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,successor_checkpoint_id,successor_superstep,successor_checkpoint_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_barriers_event_fk FOREIGN KEY (tenant_id,run_id,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
  REFERENCES stateknot.run_events (tenant_id,run_id,sequence,event_id,recorded_at,event_digest) ON DELETE RESTRICT
);
CREATE TRIGGER graph_frame_barriers_immutable BEFORE UPDATE OR DELETE ON stateknot.graph_frame_barriers
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_immutable();

CREATE FUNCTION stateknot.guard_graph_frame_barrier_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE total bigint;
BEGIN
 SELECT count(*) INTO total FROM stateknot.pending_node_result_consumptions p
 WHERE p.tenant_id=NEW.tenant_id AND p.run_id=NEW.run_id AND p.graph_namespace=NEW.graph_namespace
  AND p.base_checkpoint_id=NEW.base_checkpoint_id;
 IF total<>NEW.result_count OR EXISTS (
  SELECT 1 FROM stateknot.pending_node_result_consumptions p
  WHERE p.tenant_id=NEW.tenant_id AND p.run_id=NEW.run_id
   AND p.base_checkpoint_id=NEW.base_checkpoint_id
   AND (p.graph_namespace<>NEW.graph_namespace OR p.base_superstep<>NEW.base_superstep
    OR p.base_checkpoint_digest<>NEW.base_checkpoint_digest
    OR p.successor_checkpoint_id<>NEW.successor_checkpoint_id
    OR p.successor_superstep<>NEW.successor_superstep OR p.successor_checkpoint_digest<>NEW.successor_checkpoint_digest
    OR p.successor_journal_sequence<>NEW.journal_sequence OR p.successor_journal_event_id<>NEW.journal_event_id
    OR p.successor_journal_recorded_at<>NEW.journal_recorded_at OR p.successor_journal_digest<>NEW.journal_digest)
 ) OR NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e
  JOIN stateknot.run_checkpoints base ON base.tenant_id=e.tenant_id AND base.run_id=e.run_id
   AND base.graph_namespace=e.graph_namespace AND base.checkpoint_id=NEW.base_checkpoint_id
  JOIN stateknot.run_checkpoints next ON next.tenant_id=e.tenant_id AND next.run_id=e.run_id
   AND next.graph_namespace=e.graph_namespace AND next.checkpoint_id=NEW.successor_checkpoint_id
  JOIN stateknot.run_events v ON v.tenant_id=e.tenant_id AND v.run_id=e.run_id AND v.sequence=NEW.journal_sequence
  JOIN stateknot.graph_frame_heads h ON h.tenant_id=e.tenant_id AND h.run_id=e.run_id AND h.graph_namespace=e.graph_namespace
  JOIN stateknot.graph_frame_stacks s ON s.tenant_id=e.tenant_id AND s.run_id=e.run_id
  WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.graph_namespace=NEW.graph_namespace
   AND e.frame_identity_digest=NEW.frame_identity_digest
   AND e.journal_sequence<=base.journal_sequence AND base.journal_sequence<NEW.journal_sequence
   AND base.frame_identity_digest=NEW.frame_identity_digest AND base.frame_checkpoint_digest=NEW.base_frame_checkpoint_digest
   AND next.frame_identity_digest=NEW.frame_identity_digest AND next.frame_checkpoint_digest=NEW.successor_frame_checkpoint_digest
   AND next.parent_checkpoint_id=NEW.base_checkpoint_id AND next.parent_superstep=NEW.base_superstep
   AND next.parent_digest=NEW.base_checkpoint_digest
   AND v.event_kind='graph-frame-barrier-committed' AND v.source_kind='worker' AND v.projection_digest=NEW.compound_digest
   AND h.checkpoint_id=NEW.successor_checkpoint_id AND h.superstep=NEW.successor_superstep
   AND h.checkpoint_digest=NEW.successor_checkpoint_digest AND h.frame_checkpoint_digest=NEW.successor_frame_checkpoint_digest
   AND s.active_namespace=e.graph_namespace AND s.active_frame_identity_digest=e.frame_identity_digest
 ) THEN RAISE EXCEPTION 'incomplete compound frame barrier' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER frame_barriers_complete AFTER INSERT ON stateknot.graph_frame_barriers
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_barrier_complete();

CREATE FUNCTION stateknot.guard_graph_frame_barrier_event_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.event_kind='graph-frame-barrier-committed' AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_barriers b
  WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id
   AND b.journal_sequence=NEW.sequence AND b.journal_event_id=NEW.event_id
   AND b.journal_recorded_at=NEW.recorded_at AND b.journal_digest=NEW.event_digest
   AND b.compound_digest=NEW.projection_digest AND NEW.source_kind='worker'
 ) THEN RAISE EXCEPTION 'frame barrier event requires its whole transaction' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER run_events_frame_barrier_complete AFTER INSERT ON stateknot.run_events
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_barrier_event_complete();

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_checkpoint_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.graph_namespace<>'' AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e
  JOIN stateknot.run_events v ON v.tenant_id=e.tenant_id AND v.run_id=e.run_id AND v.sequence=e.journal_sequence
  WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.graph_namespace=NEW.graph_namespace
   AND e.frame_identity_digest=NEW.frame_identity_digest
   AND e.initial_checkpoint_id=NEW.checkpoint_id AND e.initial_checkpoint_digest=NEW.checkpoint_digest
   AND NEW.superstep=0 AND NEW.parent_checkpoint_id IS NULL
   AND e.journal_sequence=NEW.journal_sequence AND e.journal_event_id=NEW.journal_event_id
   AND e.journal_recorded_at=NEW.journal_recorded_at AND e.journal_digest=NEW.journal_digest
   AND v.event_kind='graph-frame-entered' AND v.projection_digest=e.compound_digest
 ) AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_barriers b
  JOIN stateknot.run_events v ON v.tenant_id=b.tenant_id AND v.run_id=b.run_id AND v.sequence=b.journal_sequence
  WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.graph_namespace=NEW.graph_namespace
   AND b.frame_identity_digest=NEW.frame_identity_digest
   AND b.successor_checkpoint_id=NEW.checkpoint_id AND b.successor_superstep=NEW.superstep
   AND b.successor_checkpoint_digest=NEW.checkpoint_digest AND b.successor_frame_checkpoint_digest=NEW.frame_checkpoint_digest
   AND b.base_checkpoint_id=NEW.parent_checkpoint_id AND b.base_superstep=NEW.parent_superstep
   AND b.base_checkpoint_digest=NEW.parent_digest
   AND b.journal_sequence=NEW.journal_sequence AND b.journal_event_id=NEW.journal_event_id
   AND b.journal_recorded_at=NEW.journal_recorded_at AND b.journal_digest=NEW.journal_digest
   AND v.event_kind='graph-frame-barrier-committed' AND v.projection_digest=b.compound_digest
 ) THEN RAISE EXCEPTION 'scoped checkpoint requires a whole frame transaction' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_head_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (
  SELECT 1 FROM stateknot.run_checkpoints c
  WHERE c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id AND c.graph_namespace=NEW.graph_namespace
   AND c.checkpoint_id=NEW.checkpoint_id AND c.superstep=NEW.superstep
   AND c.checkpoint_digest=NEW.checkpoint_digest AND c.frame_identity_digest=NEW.frame_identity_digest
   AND c.frame_checkpoint_digest=NEW.frame_checkpoint_digest
   AND (
    EXISTS (SELECT 1 FROM stateknot.graph_frame_entries e
     WHERE e.tenant_id=c.tenant_id AND e.run_id=c.run_id AND e.graph_namespace=c.graph_namespace
      AND e.frame_identity_digest=c.frame_identity_digest AND e.initial_checkpoint_id=c.checkpoint_id
      AND e.initial_checkpoint_digest=c.checkpoint_digest AND c.superstep=0)
    OR EXISTS (SELECT 1 FROM stateknot.graph_frame_barriers b
     WHERE b.tenant_id=c.tenant_id AND b.run_id=c.run_id AND b.graph_namespace=c.graph_namespace
      AND b.frame_identity_digest=c.frame_identity_digest AND b.successor_checkpoint_id=c.checkpoint_id
      AND b.successor_checkpoint_digest=c.checkpoint_digest AND b.successor_frame_checkpoint_digest=c.frame_checkpoint_digest)
   )
 ) THEN RAISE EXCEPTION 'frame head requires its complete scoped checkpoint' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;

CREATE FUNCTION stateknot.guard_graph_frame_head_advance() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
 IF NEW.tenant_id<>OLD.tenant_id OR NEW.run_id<>OLD.run_id OR NEW.graph_namespace<>OLD.graph_namespace
  OR NEW.frame_identity_digest<>OLD.frame_identity_digest OR NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_barriers b
  JOIN stateknot.run_events v ON v.tenant_id=b.tenant_id AND v.run_id=b.run_id AND v.sequence=b.journal_sequence
  JOIN stateknot.runs r ON r.tenant_id=b.tenant_id AND r.run_id=b.run_id
  JOIN stateknot.graph_frame_stacks s ON s.tenant_id=b.tenant_id AND s.run_id=b.run_id
  WHERE b.tenant_id=OLD.tenant_id AND b.run_id=OLD.run_id AND b.graph_namespace=OLD.graph_namespace
   AND b.frame_identity_digest=OLD.frame_identity_digest
   AND b.base_checkpoint_id=OLD.checkpoint_id AND b.base_superstep=OLD.superstep
   AND b.base_checkpoint_digest=OLD.checkpoint_digest AND b.base_frame_checkpoint_digest=OLD.frame_checkpoint_digest
   AND b.successor_checkpoint_id=NEW.checkpoint_id AND b.successor_superstep=NEW.superstep
   AND b.successor_checkpoint_digest=NEW.checkpoint_digest AND b.successor_frame_checkpoint_digest=NEW.frame_checkpoint_digest
   AND s.active_namespace=b.graph_namespace AND s.active_frame_identity_digest=b.frame_identity_digest
   AND v.source_kind='worker' AND r.lease_attempt_id=v.worker_attempt_id AND r.fencing_epoch=v.worker_epoch
   AND r.lease_expires_at>clock_timestamp() AND r.agent_deadline_at>clock_timestamp()
 ) THEN RAISE EXCEPTION 'frame head advance requires its current fenced whole barrier' USING ERRCODE='SKG02'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER frame_heads_advance BEFORE UPDATE ON stateknot.graph_frame_heads
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_head_advance();

CREATE FUNCTION stateknot.guard_graph_frame_consumption_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.graph_namespace<>'' AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_barriers b
  WHERE b.tenant_id=NEW.tenant_id AND b.run_id=NEW.run_id AND b.graph_namespace=NEW.graph_namespace
   AND b.base_checkpoint_id=NEW.base_checkpoint_id AND b.base_superstep=NEW.base_superstep
   AND b.base_checkpoint_digest=NEW.base_checkpoint_digest
   AND b.successor_checkpoint_id=NEW.successor_checkpoint_id AND b.successor_superstep=NEW.successor_superstep
   AND b.successor_checkpoint_digest=NEW.successor_checkpoint_digest
   AND b.journal_sequence=NEW.successor_journal_sequence AND b.journal_event_id=NEW.successor_journal_event_id
   AND b.journal_recorded_at=NEW.successor_journal_recorded_at AND b.journal_digest=NEW.successor_journal_digest
 ) THEN RAISE EXCEPTION 'scoped consumption requires its whole frame barrier' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER barrier_consumptions_frame_complete AFTER INSERT ON stateknot.pending_node_result_consumptions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_consumption_complete();
