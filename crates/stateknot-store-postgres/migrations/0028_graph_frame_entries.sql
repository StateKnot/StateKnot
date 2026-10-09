-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- RFC-0022 compound admission. Root checkpoint bytes and pointers stay exact.
CREATE TABLE stateknot.graph_frame_entries (
 tenant_id text NOT NULL,
 run_id uuid NOT NULL,
 graph_namespace text NOT NULL,
 frame_identity_digest bytea NOT NULL,
 ordinal integer NOT NULL,
 parent_namespace text NOT NULL,
 parent_checkpoint_id uuid NOT NULL,
 caller_attempt_id uuid NOT NULL,
 initial_checkpoint_id uuid NOT NULL,
 initial_superstep bigint GENERATED ALWAYS AS (0::bigint) STORED,
 initial_checkpoint_digest bytea NOT NULL,
 core_entry_digest bytea NOT NULL,
 compound_digest bytea NOT NULL,
 entry_bytes bytea NOT NULL,
 entry_checksum bytea GENERATED ALWAYS AS (sha256(entry_bytes)) STORED,
 journal_sequence bigint NOT NULL,
 journal_event_id uuid NOT NULL,
 journal_recorded_at timestamptz(6) NOT NULL,
 journal_digest bytea NOT NULL,
 PRIMARY KEY (tenant_id,run_id,graph_namespace),
 CONSTRAINT graph_frame_entries_ordinal_unique UNIQUE (tenant_id,run_id,ordinal),
 CONSTRAINT graph_frame_entries_event_unique UNIQUE (tenant_id,run_id,journal_sequence),
 CONSTRAINT graph_frame_entries_identity_unique UNIQUE (tenant_id,run_id,graph_namespace,frame_identity_digest),
 CONSTRAINT graph_frame_entries_caller_unique UNIQUE (tenant_id,run_id,caller_attempt_id),
 CONSTRAINT graph_frame_entries_shape CHECK (
  graph_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$'
  AND ordinal BETWEEN 1 AND 4096
  AND (parent_namespace='' OR parent_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,5}$')
  AND octet_length(frame_identity_digest)=32
  AND octet_length(initial_checkpoint_digest)=32
  AND octet_length(core_entry_digest)=32 AND octet_length(compound_digest)=32
  AND octet_length(entry_bytes) BETWEEN 1 AND 65536
  AND journal_sequence>1 AND octet_length(journal_digest)=32
 ),
 CONSTRAINT graph_frame_entries_admission_fk FOREIGN KEY (tenant_id,run_id)
  REFERENCES stateknot.agent_admissions (tenant_id,run_id) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_entries_caller_fk FOREIGN KEY (tenant_id,run_id,caller_attempt_id,parent_checkpoint_id,parent_namespace)
  REFERENCES stateknot.node_attempts (tenant_id,run_id,attempt_id,base_checkpoint_id,graph_namespace) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_entries_initial_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,initial_checkpoint_id,initial_superstep,initial_checkpoint_digest,frame_identity_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_entries_event_fk FOREIGN KEY (tenant_id,run_id,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
  REFERENCES stateknot.run_events (tenant_id,run_id,sequence,event_id,recorded_at,event_digest) ON DELETE RESTRICT
);
CREATE TABLE stateknot.graph_frame_heads (
 tenant_id text NOT NULL,run_id uuid NOT NULL,graph_namespace text NOT NULL,
 frame_identity_digest bytea NOT NULL,
 checkpoint_id uuid NOT NULL,superstep bigint NOT NULL,checkpoint_digest bytea NOT NULL,
 frame_checkpoint_digest bytea NOT NULL,
 PRIMARY KEY (tenant_id,run_id,graph_namespace),
 CONSTRAINT graph_frame_heads_shape CHECK (superstep>=0 AND octet_length(frame_checkpoint_digest)=32),
 CONSTRAINT graph_frame_heads_entry_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,frame_identity_digest)
  REFERENCES stateknot.graph_frame_entries (tenant_id,run_id,graph_namespace,frame_identity_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_heads_checkpoint_fk FOREIGN KEY (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest,frame_identity_digest)
  REFERENCES stateknot.run_checkpoints (tenant_id,run_id,graph_namespace,checkpoint_id,superstep,checkpoint_digest,frame_identity_digest) ON DELETE RESTRICT
);
CREATE TABLE stateknot.graph_frame_stacks (
 tenant_id text NOT NULL,run_id uuid NOT NULL,
 admission_digest bytea NOT NULL,
 lifetime_starts integer NOT NULL DEFAULT 0,
 active_namespace text NOT NULL DEFAULT '',active_frame_identity_digest bytea,
 PRIMARY KEY (tenant_id,run_id),
 CONSTRAINT graph_frame_stacks_shape CHECK (
  lifetime_starts BETWEEN 0 AND 4096 AND octet_length(admission_digest)=32
  AND ((active_namespace='' AND active_frame_identity_digest IS NULL)
   OR (active_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$' AND active_frame_identity_digest IS NOT NULL AND octet_length(active_frame_identity_digest)=32))
 ),
 CONSTRAINT graph_frame_stacks_admission_fk FOREIGN KEY (tenant_id,run_id)
  REFERENCES stateknot.agent_admissions (tenant_id,run_id) ON DELETE RESTRICT,
 CONSTRAINT graph_frame_stacks_active_fk FOREIGN KEY (tenant_id,run_id,active_namespace,active_frame_identity_digest)
  REFERENCES stateknot.graph_frame_entries (tenant_id,run_id,graph_namespace,frame_identity_digest) ON DELETE RESTRICT
);
CREATE FUNCTION stateknot.guard_graph_frame_entry_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'immutable compound frame entry' USING ERRCODE='SKG02';
END $$;
CREATE TRIGGER graph_frame_entries_immutable BEFORE UPDATE OR DELETE ON stateknot.graph_frame_entries
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_immutable();

-- A preconnected root writer cannot dispatch while a child owns the stack.
-- Deferral lets a framework caller start and its new child entry commit together;
-- ordinary FKs still reject crossed references immediately.
CREATE FUNCTION stateknot.guard_graph_frame_execution_scope() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE data jsonb; tenant text; identity uuid; namespace text; active text;
BEGIN
 data:=to_jsonb(NEW);
 tenant:=data->>'tenant_id'; identity:=(data->>'run_id')::uuid;
 namespace:=coalesce(data->>'graph_namespace','');
 SELECT active_namespace INTO active FROM stateknot.graph_frame_stacks
  WHERE tenant_id=tenant AND run_id=identity;
 active:=coalesce(active,'');
 IF namespace<>active THEN
  IF TG_TABLE_NAME='node_attempts' AND EXISTS (
   SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=tenant AND e.run_id=identity
    AND e.caller_attempt_id=(data->>'attempt_id')::uuid
    AND e.journal_sequence=(data->>'journal_sequence')::bigint
    AND e.parent_namespace=namespace
  ) THEN RETURN NULL; END IF;
  RAISE EXCEPTION 'only the current graph frame may execute' USING ERRCODE='SKG01';
 END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER node_attempts_frame_scope AFTER INSERT ON stateknot.node_attempts
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_execution_scope();
CREATE CONSTRAINT TRIGGER pending_results_frame_scope AFTER INSERT ON stateknot.pending_node_results
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_execution_scope();
CREATE CONSTRAINT TRIGGER tool_invocations_frame_scope AFTER INSERT ON stateknot.tool_invocations
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_execution_scope();
CREATE CONSTRAINT TRIGGER model_invocations_frame_scope AFTER INSERT ON stateknot.model_invocations
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_execution_scope();

-- The reserved kind cannot be committed as an event without its whole bundle.
CREATE FUNCTION stateknot.guard_graph_frame_entry_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.event_kind='graph-frame-entered' AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e
  JOIN stateknot.node_attempts n ON n.tenant_id=e.tenant_id AND n.run_id=e.run_id AND n.attempt_id=e.caller_attempt_id
  JOIN stateknot.graph_frame_heads h ON h.tenant_id=e.tenant_id AND h.run_id=e.run_id AND h.graph_namespace=e.graph_namespace
  WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.journal_sequence=NEW.sequence
   AND e.journal_event_id=NEW.event_id AND e.journal_recorded_at=NEW.recorded_at AND e.journal_digest=NEW.event_digest
   AND e.compound_digest=NEW.projection_digest AND n.journal_sequence=NEW.sequence
 ) THEN RAISE EXCEPTION 'incomplete compound frame entry' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER run_events_frame_entry_complete AFTER INSERT ON stateknot.run_events
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_complete();

-- Components can never admit a frame through a legacy projection or stale
-- scope, including a writer connected before the schema version changed.
CREATE CONSTRAINT TRIGGER checkpoints_frame_scope AFTER INSERT ON stateknot.run_checkpoints
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_execution_scope();
CREATE FUNCTION stateknot.guard_graph_frame_revision_scope() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE namespace text; active text;
BEGIN
 IF TG_TABLE_NAME='tool_invocation_revisions' THEN
  SELECT graph_namespace INTO namespace FROM stateknot.tool_invocations
   WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND invocation_id=NEW.invocation_id;
 ELSE
  SELECT graph_namespace INTO namespace FROM stateknot.model_invocations
   WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND invocation_id=NEW.invocation_id;
 END IF;
 SELECT active_namespace INTO active FROM stateknot.graph_frame_stacks
  WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF namespace IS DISTINCT FROM coalesce(active,'') THEN
  RAISE EXCEPTION 'only the current graph frame may revise an invocation' USING ERRCODE='SKG01';
 END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER tool_revisions_frame_scope AFTER INSERT ON stateknot.tool_invocation_revisions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_revision_scope();
CREATE CONSTRAINT TRIGGER model_revisions_frame_scope AFTER INSERT ON stateknot.model_invocation_revisions
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_revision_scope();

CREATE FUNCTION stateknot.guard_graph_frame_stack_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE total bigint; maximum integer;
BEGIN
 SELECT count(*),max(ordinal) INTO total,maximum FROM stateknot.graph_frame_entries
  WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF total<>NEW.lifetime_starts OR maximum IS DISTINCT FROM NEW.lifetime_starts
  OR NOT EXISTS (
   SELECT 1 FROM stateknot.graph_frame_entries e
   JOIN stateknot.run_events v ON v.tenant_id=e.tenant_id AND v.run_id=e.run_id AND v.sequence=e.journal_sequence
   JOIN stateknot.graph_frame_heads h ON h.tenant_id=e.tenant_id AND h.run_id=e.run_id AND h.graph_namespace=e.graph_namespace
   WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.ordinal=NEW.lifetime_starts
    AND e.graph_namespace=NEW.active_namespace AND e.frame_identity_digest=NEW.active_frame_identity_digest
    AND v.event_kind='graph-frame-entered' AND v.schema_id='https://stknot.com/schemas/core/graph-frame-entry-event/1.0.0'
    AND v.schema_version='1.0.0' AND v.schema_digest=decode('a2f2d5679fdf7c4dc9a7fa906121e41b9571177aee0d9e28cb29f1d921fa4ea3','hex')
    AND v.projection_digest=e.compound_digest
  ) THEN RAISE EXCEPTION 'incomplete graph frame stack push' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER frame_stacks_complete AFTER INSERT OR UPDATE ON stateknot.graph_frame_stacks
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_stack_complete();

CREATE FUNCTION stateknot.guard_graph_frame_entry_components() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (
  SELECT 1 FROM stateknot.run_events v
  JOIN stateknot.node_attempts n ON n.tenant_id=NEW.tenant_id AND n.run_id=NEW.run_id AND n.attempt_id=NEW.caller_attempt_id
  JOIN stateknot.run_checkpoints c ON c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id AND c.graph_namespace=NEW.graph_namespace AND c.checkpoint_id=NEW.initial_checkpoint_id
  JOIN stateknot.graph_frame_heads h ON h.tenant_id=NEW.tenant_id AND h.run_id=NEW.run_id AND h.graph_namespace=NEW.graph_namespace
  JOIN stateknot.graph_frame_stacks s ON s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id
  WHERE v.tenant_id=NEW.tenant_id AND v.run_id=NEW.run_id AND v.sequence=NEW.journal_sequence
   AND v.event_kind='graph-frame-entered' AND v.projection_digest=NEW.compound_digest
   AND n.journal_sequence=NEW.journal_sequence AND n.graph_namespace=NEW.parent_namespace
   AND c.journal_sequence=NEW.journal_sequence AND c.superstep=0 AND c.parent_checkpoint_id IS NULL
   AND c.frame_identity_digest=NEW.frame_identity_digest AND c.checkpoint_digest=NEW.initial_checkpoint_digest
   AND h.frame_identity_digest=NEW.frame_identity_digest
   AND h.checkpoint_id=c.checkpoint_id AND h.superstep=0 AND h.checkpoint_digest=c.checkpoint_digest
   AND h.frame_checkpoint_digest=c.frame_checkpoint_digest
   AND s.lifetime_starts>=NEW.ordinal
 ) THEN RAISE EXCEPTION 'incomplete graph frame admission components' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER frame_entries_complete AFTER INSERT ON stateknot.graph_frame_entries
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_components();

CREATE FUNCTION stateknot.guard_graph_frame_root_projection() RETURNS trigger LANGUAGE plpgsql AS $$
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
CREATE TRIGGER runs_frame_root_projection BEFORE UPDATE ON stateknot.runs
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_root_projection();


-- A scoped checkpoint is a component of one whole frame transaction. The
-- admission profile accepts its initial checkpoint only; future scoped barriers
-- must install and authenticate their own compound record before this guard
-- can admit a successor. An ordinary journal event is never that authority.
CREATE FUNCTION stateknot.guard_graph_frame_checkpoint_complete() RETURNS trigger LANGUAGE plpgsql AS $$
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
 ) THEN RAISE EXCEPTION 'scoped checkpoint requires a whole frame transaction' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER checkpoints_frame_complete AFTER INSERT ON stateknot.run_checkpoints
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_checkpoint_complete();

CREATE FUNCTION stateknot.guard_graph_frame_head_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_entries e
  JOIN stateknot.run_checkpoints c ON c.tenant_id=e.tenant_id AND c.run_id=e.run_id AND c.graph_namespace=e.graph_namespace AND c.checkpoint_id=e.initial_checkpoint_id
  WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.graph_namespace=NEW.graph_namespace
   AND e.frame_identity_digest=NEW.frame_identity_digest AND NEW.superstep=0
   AND e.initial_checkpoint_id=NEW.checkpoint_id AND e.initial_checkpoint_digest=NEW.checkpoint_digest
   AND c.frame_checkpoint_digest=NEW.frame_checkpoint_digest
 ) THEN RAISE EXCEPTION 'frame head requires its complete scoped checkpoint' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER frame_heads_complete AFTER INSERT OR UPDATE ON stateknot.graph_frame_heads
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_head_complete();

-- Legacy wait rows have no scoped owner. They cannot suspend or resolve an
-- active child until an authenticated scoped wait transaction is available.
CREATE FUNCTION stateknot.guard_graph_frame_legacy_wait() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks s
  WHERE s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id AND s.active_namespace<>'') THEN
  RAISE EXCEPTION 'legacy wait has no active frame ownership' USING ERRCODE='SKG01';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER waits_frame_scope BEFORE INSERT OR UPDATE ON stateknot.run_wait_registrations
 FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_legacy_wait();
