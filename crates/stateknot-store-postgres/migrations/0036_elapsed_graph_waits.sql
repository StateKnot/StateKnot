-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Original-condition inventory for whole Root/scoped V3 Wait barriers. The
-- tenant-wide primary key is the only wait identity arbiter. Historical V1/V2
-- facts retain their exact bytes; backfill records their real registration
-- anchor without inventing an original graph owner.
-- This migration runs transactionally. Block concurrent registration inserts
-- until backfill and the registration identity trigger are both installed.
LOCK TABLE stateknot.run_wait_registrations IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE stateknot.graph_wait_conditions (
 tenant_id text NOT NULL,
 wait_id uuid NOT NULL,
 run_id uuid NOT NULL,
 profile text NOT NULL,
 wait_kind text NOT NULL,
 interrupt_kind text,
 timer_kind text,
 due_at timestamp(6) with time zone,
 expires_at timestamp(6) with time zone,
 intent_digest bytea NOT NULL,
 registration_digest bytea,
 journal_sequence bigint NOT NULL,
 journal_event_id uuid NOT NULL,
 journal_recorded_at timestamp(6) with time zone NOT NULL,
 journal_digest bytea NOT NULL,
 graph_namespace text,
 base_checkpoint_id uuid,
 base_superstep bigint,
 base_checkpoint_digest bytea,
 successor_checkpoint_id uuid,
 successor_superstep bigint,
 successor_checkpoint_digest bytea,
 source_sequence bigint,
 source_event_id uuid,
 source_recorded_at timestamp(6) with time zone,
 source_digest bytea,
 owner_node_id text,
 owner_record_digest bytea,
 owner_intent_digest bytea,
 result_wait_index smallint,
 condition_ordinal smallint,
 specification_digest bytea,
 condition_bytes bytea,
 condition_checksum bytea GENERATED ALWAYS AS (sha256(condition_bytes)) STORED,
 CONSTRAINT graph_wait_conditions_event_fk FOREIGN KEY (tenant_id, run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest) REFERENCES stateknot.run_events(tenant_id, run_id, sequence, event_id, recorded_at, event_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_wait_conditions_future_only_registration CHECK ((((profile = 'graph_elapsed'::text) OR ((wait_kind = 'timer'::text) AND (due_at > journal_recorded_at)) OR ((wait_kind = 'interrupt'::text) AND ((expires_at IS NULL) OR (expires_at > journal_recorded_at)))) IS TRUE)),
 CONSTRAINT graph_wait_conditions_identity_valid CHECK ((((octet_length(tenant_id) >= 1) AND (octet_length(tenant_id) <= 128)) AND (tenant_id ~ '^[A-Za-z0-9._:-]+$'::text) AND (tenant_id <> ALL (ARRAY['.'::text, '..'::text])) AND stateknot.is_uuid_v7(run_id) AND stateknot.is_uuid_v7(wait_id) AND stateknot.is_uuid_v7(journal_event_id) AND (journal_sequence >= 1) AND (octet_length(journal_digest) = 32) AND (octet_length(intent_digest) = 32))),
 CONSTRAINT graph_wait_conditions_kind_shape CHECK (((((wait_kind = 'timer'::text) AND (timer_kind = ANY (ARRAY['sleep'::text, 'retry_backoff'::text])) AND (due_at IS NOT NULL) AND (interrupt_kind IS NULL) AND (expires_at IS NULL)) OR ((wait_kind = 'interrupt'::text) AND (interrupt_kind = ANY (ARRAY['approval'::text, 'input'::text, 'authentication'::text, 'external_signal'::text, 'reconciliation'::text])) AND (timer_kind IS NULL) AND (due_at IS NULL))) IS TRUE)),
 CONSTRAINT graph_wait_conditions_origin_ordinal_unique UNIQUE (tenant_id, run_id, journal_sequence, condition_ordinal),
 CONSTRAINT graph_wait_conditions_owner_fk FOREIGN KEY (tenant_id, run_id, base_checkpoint_id, base_superstep, base_checkpoint_digest, graph_namespace, owner_node_id, owner_record_digest) REFERENCES stateknot.pending_node_results(tenant_id, run_id, base_checkpoint_id, base_superstep, base_checkpoint_digest, graph_namespace, node_id, record_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_wait_conditions_pkey PRIMARY KEY (tenant_id, wait_id),
 CONSTRAINT graph_wait_conditions_profile_shape CHECK (((((profile = 'legacy_registration'::text) AND (registration_digest IS NOT NULL) AND (octet_length(registration_digest) = 32) AND (graph_namespace IS NULL) AND (base_checkpoint_id IS NULL) AND (base_superstep IS NULL) AND (base_checkpoint_digest IS NULL) AND (successor_checkpoint_id IS NULL) AND (successor_superstep IS NULL) AND (successor_checkpoint_digest IS NULL) AND (source_sequence IS NULL) AND (source_event_id IS NULL) AND (source_recorded_at IS NULL) AND (source_digest IS NULL) AND (owner_node_id IS NULL) AND (owner_record_digest IS NULL) AND (owner_intent_digest IS NULL) AND (result_wait_index IS NULL) AND (condition_ordinal IS NULL) AND (specification_digest IS NULL) AND (condition_bytes IS NULL)) OR ((profile = ANY (ARRAY['graph_registered'::text, 'graph_elapsed'::text])) AND (graph_namespace IS NOT NULL) AND ((graph_namespace = ''::text) OR ((graph_namespace ~ '^[0-9a-f]{64}(/[0-9a-f]{64}){0,6}$'::text) AND (octet_length(graph_namespace) <= 454))) AND (base_checkpoint_id IS NOT NULL) AND stateknot.is_uuid_v7(base_checkpoint_id) AND (base_superstep IS NOT NULL) AND (base_superstep >= 0) AND (base_checkpoint_digest IS NOT NULL) AND (octet_length(base_checkpoint_digest) = 32) AND (successor_checkpoint_id IS NOT NULL) AND stateknot.is_uuid_v7(successor_checkpoint_id) AND (successor_superstep IS NOT NULL) AND (successor_superstep = (base_superstep + 1)) AND (successor_checkpoint_digest IS NOT NULL) AND (octet_length(successor_checkpoint_digest) = 32) AND (source_sequence IS NOT NULL) AND (source_sequence = (journal_sequence - 1)) AND (source_event_id IS NOT NULL) AND stateknot.is_uuid_v7(source_event_id) AND (source_recorded_at IS NOT NULL) AND (source_recorded_at <= journal_recorded_at) AND (source_digest IS NOT NULL) AND (octet_length(source_digest) = 32) AND (owner_node_id IS NOT NULL) AND ((octet_length(owner_node_id) >= 1) AND (octet_length(owner_node_id) <= 128)) AND (owner_node_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]*$'::text) AND (owner_record_digest IS NOT NULL) AND (octet_length(owner_record_digest) = 32) AND (owner_intent_digest IS NOT NULL) AND (octet_length(owner_intent_digest) = 32) AND (result_wait_index IS NOT NULL) AND ((result_wait_index >= 0) AND (result_wait_index <= 63)) AND (condition_ordinal IS NOT NULL) AND ((condition_ordinal >= 0) AND (condition_ordinal <= 63)) AND (specification_digest IS NOT NULL) AND (octet_length(specification_digest) = 32) AND (condition_bytes IS NOT NULL) AND ((octet_length(condition_bytes) >= 1) AND (octet_length(condition_bytes) <= 32768)) AND (((profile = 'graph_registered'::text) AND (registration_digest IS NOT NULL) AND (octet_length(registration_digest) = 32)) OR ((profile = 'graph_elapsed'::text) AND (wait_kind = 'timer'::text) AND (registration_digest IS NULL) AND (due_at <= journal_recorded_at))))) IS TRUE)),
 CONSTRAINT graph_wait_conditions_registration_fk FOREIGN KEY (tenant_id, run_id, wait_id, wait_kind, registration_digest) REFERENCES stateknot.run_wait_registrations(tenant_id, run_id, wait_id, wait_kind, record_digest) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
 CONSTRAINT graph_wait_conditions_run_fk FOREIGN KEY (tenant_id, run_id) REFERENCES stateknot.runs(tenant_id, run_id) ON DELETE RESTRICT,
 CONSTRAINT graph_wait_conditions_source_fk FOREIGN KEY (tenant_id, run_id, source_sequence, source_event_id, source_recorded_at, source_digest) REFERENCES stateknot.run_events(tenant_id, run_id, sequence, event_id, recorded_at, event_digest) ON DELETE RESTRICT,
 CONSTRAINT graph_wait_conditions_successor_fk FOREIGN KEY (tenant_id, run_id, graph_namespace, successor_checkpoint_id, successor_superstep, successor_checkpoint_digest, journal_sequence, journal_event_id, journal_recorded_at, journal_digest) REFERENCES stateknot.run_checkpoints(tenant_id, run_id, graph_namespace, checkpoint_id, superstep, checkpoint_digest, journal_sequence, journal_event_id, journal_recorded_at, journal_digest) ON DELETE RESTRICT
);

CREATE INDEX graph_wait_conditions_origin_read ON stateknot.graph_wait_conditions USING btree (tenant_id, run_id, journal_sequence, condition_ordinal) WHERE (profile = ANY (ARRAY['graph_registered'::text, 'graph_elapsed'::text]));

INSERT INTO stateknot.graph_wait_conditions (
 tenant_id,wait_id,run_id,profile,wait_kind,interrupt_kind,timer_kind,due_at,expires_at,
 intent_digest,registration_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest
) SELECT tenant_id,wait_id,run_id,'legacy_registration',wait_kind,interrupt_kind,timer_kind,due_at,expires_at,
 intent_digest,record_digest,registration_sequence,registration_event_id,registered_at,registration_event_digest
FROM stateknot.run_wait_registrations;

CREATE FUNCTION stateknot.guard_graph_wait_condition_original()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE data jsonb; result jsonb; specification jsonb; source jsonb; successor jsonb;
BEGIN
 IF NEW.profile='legacy_registration' THEN RETURN NEW; END IF;
 data:=convert_from(NEW.condition_bytes,'UTF8')::jsonb;
 SELECT convert_from(p.result_bytes,'UTF8')::jsonb INTO result FROM stateknot.pending_node_results p
  WHERE p.tenant_id=NEW.tenant_id AND p.run_id=NEW.run_id AND p.graph_namespace=NEW.graph_namespace
   AND p.base_checkpoint_id=NEW.base_checkpoint_id AND p.record_digest=NEW.owner_record_digest
   AND p.intent_digest=NEW.owner_intent_digest AND p.node_id=NEW.owner_node_id AND p.control_kind='wait'
   AND p.journal_sequence<=NEW.source_sequence AND p.journal_recorded_at<=NEW.source_recorded_at;
 specification:=result->'intent'->'control'->'waits'->NEW.result_wait_index;
 SELECT jsonb_build_object('tenant_id',e.tenant_id,'run_id',e.run_id,'sequence',e.sequence::text,'event_id',e.event_id,
  'recorded_at',to_char(e.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'digest','sha256:'||encode(e.event_digest,'hex'))
 INTO source FROM stateknot.run_events e WHERE e.tenant_id=NEW.tenant_id AND e.run_id=NEW.run_id AND e.sequence=NEW.source_sequence
  AND e.event_id=NEW.source_event_id AND e.recorded_at=NEW.source_recorded_at AND e.event_digest=NEW.source_digest;
 SELECT jsonb_build_object('tenant_id',c.tenant_id,'run_id',c.run_id,'checkpoint_id',c.checkpoint_id,'superstep',c.superstep::text,
  'graph',convert_from(c.checkpoint_bytes,'UTF8')::jsonb->'graph','digest','sha256:'||encode(c.checkpoint_digest,'hex'),
  'journal_head',jsonb_build_object('tenant_id',e.tenant_id,'run_id',e.run_id,'sequence',e.sequence::text,'event_id',e.event_id,
   'recorded_at',to_char(e.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'digest','sha256:'||encode(e.event_digest,'hex')))
 INTO successor FROM stateknot.run_checkpoints c JOIN stateknot.run_events e ON e.tenant_id=c.tenant_id AND e.run_id=c.run_id AND e.sequence=c.journal_sequence
 WHERE c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id AND c.graph_namespace=NEW.graph_namespace AND c.checkpoint_id=NEW.successor_checkpoint_id
  AND c.parent_checkpoint_id=NEW.base_checkpoint_id AND c.parent_digest=NEW.base_checkpoint_digest
  AND e.source_kind='worker' AND e.previous_digest=NEW.source_digest AND e.sequence=NEW.journal_sequence
  AND convert_from(c.checkpoint_bytes,'UTF8')::jsonb->'graph'=result->'intent'->'activation'->'base_checkpoint'->'graph';
 IF result IS NULL OR specification IS NULL OR source IS NULL OR successor IS NULL
  OR data->>'version' IS DISTINCT FROM '1' OR data->>'ordinal' IS DISTINCT FROM NEW.condition_ordinal::text
  OR data->>'result_wait_index' IS DISTINCT FROM NEW.result_wait_index::text
  OR data->>'specification_digest' IS DISTINCT FROM 'sha256:'||encode(NEW.specification_digest,'hex')
  OR data->>'registration_intent_digest' IS DISTINCT FROM 'sha256:'||encode(NEW.intent_digest,'hex')
  OR data->'owner' IS DISTINCT FROM jsonb_build_object('activation',result->'intent'->'activation','intent_digest',result->'intent'->'intent_digest',
   'fence',result->'fence','journal_head',result->'journal_head','digest',result->'digest')
  OR data->'source' IS DISTINCT FROM source OR data->'successor' IS DISTINCT FROM successor
  OR specification->>'kind' IS DISTINCT FROM NEW.wait_kind
  OR coalesce(specification->>'interrupt_id',specification->>'timer_id') IS DISTINCT FROM NEW.wait_id::text
  OR (NEW.wait_kind='timer' AND (specification->>'timer_kind' IS DISTINCT FROM NEW.timer_kind
   OR specification->>'due_at' IS DISTINCT FROM to_char(NEW.due_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')))
  OR (NEW.wait_kind='interrupt' AND (specification->>'interrupt_kind' IS DISTINCT FROM NEW.interrupt_kind
   OR specification->>'expires_at' IS DISTINCT FROM CASE WHEN NEW.expires_at IS NULL THEN NULL ELSE to_char(NEW.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END))
 THEN RAISE EXCEPTION 'graph Wait condition differs from its complete original owner' USING ERRCODE='SKG02'; END IF;
 IF NEW.profile='graph_elapsed' THEN
  IF data->'observation'->>'kind' IS DISTINCT FROM 'elapsed_timer'
   OR data->'observation'->'timer'->>'timer_id' IS DISTINCT FROM NEW.wait_id::text
   OR data->'observation'->'timer'->>'kind' IS DISTINCT FROM NEW.timer_kind
   OR data->'observation'->'timer'->>'due_at' IS DISTINCT FROM specification->>'due_at'
   OR data->'observation'->'timer'->>'tenant_id' IS DISTINCT FROM NEW.tenant_id
   OR data->'observation'->'timer'->>'run_id' IS DISTINCT FROM NEW.run_id::text
   OR data->'observation'->'timer'->>'registration_event_id' IS DISTINCT FROM NEW.journal_event_id::text
   OR data->'observation'->'timer'->>'intent_digest' IS DISTINCT FROM 'sha256:'||encode(NEW.intent_digest,'hex')
  THEN RAISE EXCEPTION 'elapsed graph Timer changed its original specification' USING ERRCODE='SKG02'; END IF;
 ELSIF data->'observation'->>'kind' IS DISTINCT FROM 'registered_'||NEW.wait_kind
  OR data->'observation'->>'wait_id' IS DISTINCT FROM NEW.wait_id::text
  OR data->'observation'->>'record_digest' IS DISTINCT FROM 'sha256:'||encode(NEW.registration_digest,'hex')
 THEN RAISE EXCEPTION 'registered graph Wait changed its exact registration' USING ERRCODE='SKG02'; END IF;
 RETURN NEW;
END $function$
;

CREATE FUNCTION stateknot.guard_graph_wait_inventory_complete()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE data jsonb; inventory jsonb; total bigint; registered bigint; target_namespace text; target_sequence bigint;
BEGIN
 IF TG_TABLE_NAME='graph_wait_conditions' THEN
  IF NEW.profile='legacy_registration' THEN RETURN NULL; END IF;
  target_namespace:=NEW.graph_namespace; target_sequence:=NEW.journal_sequence;
  IF target_namespace='' THEN
   SELECT convert_from(bundle_bytes,'UTF8')::jsonb INTO data FROM stateknot.root_graph_barriers
    WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND journal_sequence=target_sequence;
  ELSE
   SELECT convert_from(barrier_bytes,'UTF8')::jsonb INTO data FROM stateknot.graph_frame_barriers
    WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND graph_namespace=target_namespace AND journal_sequence=target_sequence;
  END IF;
 ELSE
  target_namespace:=NEW.graph_namespace; target_sequence:=NEW.journal_sequence;
  IF TG_TABLE_NAME='root_graph_barriers' THEN data:=convert_from(NEW.bundle_bytes,'UTF8')::jsonb;
  ELSE data:=convert_from(NEW.barrier_bytes,'UTF8')::jsonb; END IF;
  IF data->>'version' IS DISTINCT FROM '3' THEN RETURN NULL; END IF;
 END IF;
 inventory:=data->'wait_inventory';
 SELECT count(*),count(*) FILTER (WHERE profile='graph_registered') INTO total,registered
 FROM stateknot.graph_wait_conditions WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND journal_sequence=target_sequence
  AND profile IN ('graph_registered','graph_elapsed');
 IF data IS NULL OR data->>'version' IS DISTINCT FROM '3' OR inventory IS NULL
  OR inventory->>'version' IS DISTINCT FROM '1' OR inventory->>'namespace' IS DISTINCT FROM target_namespace
  OR jsonb_typeof(inventory->'conditions') IS DISTINCT FROM 'array'
  OR total NOT BETWEEN 1 AND 64 OR total<>jsonb_array_length(inventory->'conditions')
  OR inventory->>'registered_count' IS DISTINCT FROM registered::text
  OR inventory->>'state' IS DISTINCT FROM (CASE WHEN registered=0 THEN 'ready' ELSE 'waiting' END)
  OR EXISTS (SELECT 1 FROM jsonb_array_elements(inventory->'conditions') WITH ORDINALITY ref(item,ordinal) WHERE NOT EXISTS (
   SELECT 1 FROM stateknot.graph_wait_conditions c WHERE c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id
    AND c.graph_namespace=target_namespace AND c.journal_sequence=target_sequence AND c.condition_ordinal=ref.ordinal-1
    AND ref.item->>'ordinal'=c.condition_ordinal::text AND ref.item->>'checksum'='sha256:'||encode(c.condition_checksum,'hex')
    AND c.profile IN ('graph_registered','graph_elapsed')
    AND inventory->'source'=convert_from(c.condition_bytes,'UTF8')::jsonb->'source'
    AND inventory->'successor'=convert_from(c.condition_bytes,'UTF8')::jsonb->'successor'
    AND inventory->'event'=convert_from(c.condition_bytes,'UTF8')::jsonb->'successor'->'journal_head'
    AND inventory->'base'=convert_from(c.condition_bytes,'UTF8')::jsonb->'owner'->'activation'->'base_checkpoint'
  ))
 THEN RAISE EXCEPTION 'graph Wait inventory requires its exact complete whole transaction' USING ERRCODE='SKG02'; END IF;
 IF (SELECT count(*) FROM stateknot.pending_node_results p
   JOIN stateknot.pending_node_result_consumptions used ON used.tenant_id=p.tenant_id AND used.run_id=p.run_id
    AND used.graph_namespace=p.graph_namespace AND used.base_checkpoint_id=p.base_checkpoint_id
    AND used.node_id=p.node_id AND used.result_record_digest=p.record_digest
    AND used.successor_journal_sequence=target_sequence
   CROSS JOIN LATERAL jsonb_array_elements(convert_from(p.result_bytes,'UTF8')::jsonb->'intent'->'control'->'waits') spec
   WHERE p.tenant_id=NEW.tenant_id AND p.run_id=NEW.run_id AND p.graph_namespace=target_namespace
    AND p.base_checkpoint_id::text=inventory->'base'->>'checkpoint_id' AND p.control_kind='wait')<>total
  OR EXISTS (
   WITH originals AS (
    SELECT p.node_id,p.record_digest,p.intent_digest,spec.index-1 AS result_wait_index,
     row_number() OVER (ORDER BY p.node_id COLLATE "C",spec.index)-1 AS condition_ordinal
    FROM stateknot.pending_node_results p
    JOIN stateknot.pending_node_result_consumptions used ON used.tenant_id=p.tenant_id AND used.run_id=p.run_id
     AND used.graph_namespace=p.graph_namespace AND used.base_checkpoint_id=p.base_checkpoint_id
     AND used.node_id=p.node_id AND used.result_record_digest=p.record_digest
    AND used.successor_journal_sequence=target_sequence
    CROSS JOIN LATERAL jsonb_array_elements(convert_from(p.result_bytes,'UTF8')::jsonb->'intent'->'control'->'waits') WITH ORDINALITY spec(value,index)
    WHERE p.tenant_id=NEW.tenant_id AND p.run_id=NEW.run_id AND p.graph_namespace=target_namespace
     AND p.base_checkpoint_id::text=inventory->'base'->>'checkpoint_id' AND p.control_kind='wait'
   ) SELECT 1 FROM originals o WHERE NOT EXISTS (
    SELECT 1 FROM stateknot.graph_wait_conditions c WHERE c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id
     AND c.graph_namespace=target_namespace AND c.journal_sequence=target_sequence
     AND c.owner_node_id=o.node_id AND c.owner_record_digest=o.record_digest AND c.owner_intent_digest=o.intent_digest
     AND c.result_wait_index=o.result_wait_index AND c.condition_ordinal=o.condition_ordinal
     AND c.profile IN ('graph_registered','graph_elapsed')
   )
  ) THEN RAISE EXCEPTION 'graph Wait inventory omitted or reordered an original complete condition' USING ERRCODE='SKG02'; END IF;
 IF registered<>(SELECT count(*) FROM stateknot.run_wait_registrations w
   WHERE w.tenant_id=NEW.tenant_id AND w.run_id=NEW.run_id AND w.registration_sequence=target_sequence)
  OR EXISTS (SELECT 1 FROM stateknot.graph_wait_conditions c
   WHERE c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id AND c.journal_sequence=target_sequence
    AND c.profile IN ('graph_registered','graph_elapsed') AND (
     (c.profile='graph_registered' AND NOT EXISTS (SELECT 1 FROM stateknot.run_wait_registrations w
      WHERE w.tenant_id=c.tenant_id AND w.run_id=c.run_id AND w.wait_id=c.wait_id AND w.wait_kind=c.wait_kind
       AND w.intent_digest=c.intent_digest AND w.record_digest=c.registration_digest
       AND w.registration_sequence=c.journal_sequence AND w.registration_event_id=c.journal_event_id
       AND w.registered_at=c.journal_recorded_at AND w.registration_event_digest=c.journal_digest))
     OR (c.profile='graph_elapsed' AND (EXISTS (SELECT 1 FROM stateknot.run_wait_registrations w WHERE w.tenant_id=c.tenant_id AND w.wait_id=c.wait_id)
      OR EXISTS (SELECT 1 FROM stateknot.timer_firings f WHERE f.tenant_id=c.tenant_id AND f.timer_id=c.wait_id)))
    )) THEN RAISE EXCEPTION 'graph Wait registration set differs from its complete original inventory' USING ERRCODE='SKG02'; END IF;
 IF target_namespace='' THEN
  IF data->'intent'->'disposition'->>'kind' IS DISTINCT FROM 'wait'
   OR jsonb_typeof(data->'intent'->'disposition'->'registrations') IS DISTINCT FROM 'array'
   OR jsonb_array_length(data->'intent'->'disposition'->'registrations')<>total
   OR EXISTS (SELECT 1 FROM jsonb_array_elements(data->'intent'->'disposition'->'registrations') ref WHERE NOT EXISTS (
    SELECT 1 FROM stateknot.graph_wait_conditions c WHERE c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id AND c.journal_sequence=target_sequence
     AND c.profile IN ('graph_registered','graph_elapsed') AND c.wait_kind=ref->>'kind' AND c.wait_id::text=ref->>'wait_id'
     AND 'sha256:'||encode(c.intent_digest,'hex')=ref->>'intent_digest'))
   OR EXISTS (SELECT 1 FROM stateknot.graph_wait_conditions c WHERE c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id AND c.journal_sequence=target_sequence
    AND c.profile IN ('graph_registered','graph_elapsed') AND NOT EXISTS (
     SELECT 1 FROM jsonb_array_elements(data->'intent'->'disposition'->'registrations') ref
      WHERE c.wait_kind=ref->>'kind' AND c.wait_id::text=ref->>'wait_id' AND 'sha256:'||encode(c.intent_digest,'hex')=ref->>'intent_digest'))
  THEN RAISE EXCEPTION 'Root Wait compact references omit an original condition' USING ERRCODE='SKG02'; END IF;
 ELSIF data->'disposition'->>'kind' IS DISTINCT FROM 'wait'
  OR data->'disposition'->>'condition_count' IS DISTINCT FROM total::text
  OR data->'disposition' ? 'waits'
 THEN RAISE EXCEPTION 'scoped Wait compact original count differs from inventory' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $function$
;

CREATE FUNCTION stateknot.guard_wait_registration_identity()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
DECLARE owned stateknot.graph_wait_conditions%ROWTYPE;
BEGIN
 INSERT INTO stateknot.graph_wait_conditions (
  tenant_id,wait_id,run_id,profile,wait_kind,interrupt_kind,timer_kind,due_at,expires_at,
  intent_digest,registration_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest
 ) VALUES (NEW.tenant_id,NEW.wait_id,NEW.run_id,'legacy_registration',NEW.wait_kind,NEW.interrupt_kind,NEW.timer_kind,NEW.due_at,NEW.expires_at,
  NEW.intent_digest,NEW.record_digest,NEW.registration_sequence,NEW.registration_event_id,NEW.registered_at,NEW.registration_event_digest)
 ON CONFLICT (tenant_id,wait_id) DO NOTHING;
 -- A conflicting uncommitted insert waits at the single unique index. This
 -- separate command must observe and compare the winner; absence is failure.
 SELECT * INTO owned FROM stateknot.graph_wait_conditions WHERE tenant_id=NEW.tenant_id AND wait_id=NEW.wait_id;
 IF NOT FOUND OR owned.profile NOT IN ('legacy_registration','graph_registered')
  OR owned.run_id IS DISTINCT FROM NEW.run_id OR owned.wait_kind IS DISTINCT FROM NEW.wait_kind
  OR owned.interrupt_kind IS DISTINCT FROM NEW.interrupt_kind OR owned.timer_kind IS DISTINCT FROM NEW.timer_kind
  OR owned.due_at IS DISTINCT FROM NEW.due_at OR owned.expires_at IS DISTINCT FROM NEW.expires_at
  OR owned.intent_digest IS DISTINCT FROM NEW.intent_digest OR owned.registration_digest IS DISTINCT FROM NEW.record_digest
  OR owned.journal_sequence IS DISTINCT FROM NEW.registration_sequence OR owned.journal_event_id IS DISTINCT FROM NEW.registration_event_id
  OR owned.journal_recorded_at IS DISTINCT FROM NEW.registered_at OR owned.journal_digest IS DISTINCT FROM NEW.registration_event_digest
 THEN RAISE EXCEPTION 'wait identity conflicts with its immutable original owner' USING ERRCODE='23505'; END IF;
 RETURN NEW;
END $function$
;

CREATE CONSTRAINT TRIGGER frame_barriers_wait_inventory_complete AFTER INSERT ON stateknot.graph_frame_barriers DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_wait_inventory_complete();

CREATE CONSTRAINT TRIGGER graph_wait_conditions_complete AFTER INSERT ON stateknot.graph_wait_conditions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_wait_inventory_complete();

CREATE TRIGGER graph_wait_conditions_immutable BEFORE DELETE OR UPDATE ON stateknot.graph_wait_conditions FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_frame_entry_immutable();

CREATE TRIGGER graph_wait_conditions_original BEFORE INSERT ON stateknot.graph_wait_conditions FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_wait_condition_original();

CREATE CONSTRAINT TRIGGER root_barriers_wait_inventory_complete AFTER INSERT ON stateknot.root_graph_barriers DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION stateknot.guard_graph_wait_inventory_complete();

CREATE TRIGGER wait_registrations_identity BEFORE INSERT ON stateknot.run_wait_registrations FOR EACH ROW EXECUTE FUNCTION stateknot.guard_wait_registration_identity();

ALTER TABLE stateknot.root_graph_barriers DROP CONSTRAINT root_graph_barriers_shape;
ALTER TABLE stateknot.root_graph_barriers ADD CONSTRAINT root_graph_barriers_shape CHECK (

  base_superstep>=0 AND successor_superstep=base_superstep+1
  AND result_count BETWEEN 1 AND 1024 AND journal_sequence>1
  AND disposition_kind IN ('continue','wait','terminal')
  AND lifecycle_before_revision BETWEEN 1 AND 18446744073709551615
  AND lifecycle_after_revision BETWEEN 1 AND 18446744073709551615
  AND ((disposition_kind='continue' AND lifecycle_after_revision=lifecycle_before_revision AND lifecycle_before_bytes=lifecycle_after_bytes)
   OR (disposition_kind<>'continue' AND lifecycle_after_revision=lifecycle_before_revision+1
    AND NOT (disposition_kind='wait' AND convert_from(bundle_bytes,'UTF8')::jsonb->>'version'='3'
     AND convert_from(bundle_bytes,'UTF8')::jsonb->'wait_inventory'->>'state'='ready'))
   OR (disposition_kind='wait' AND convert_from(bundle_bytes,'UTF8')::jsonb->>'version'='3'
    AND convert_from(bundle_bytes,'UTF8')::jsonb->'intent'->>'version'='3'
    AND convert_from(bundle_bytes,'UTF8')::jsonb->'wait_inventory'->>'state'='ready'
    AND convert_from(bundle_bytes,'UTF8')::jsonb->'wait_inventory'->>'registered_count'='0'
    AND lifecycle_after_revision=lifecycle_before_revision AND lifecycle_before_bytes=lifecycle_after_bytes))
  AND octet_length(base_checkpoint_digest)=32 AND octet_length(successor_checkpoint_digest)=32
  AND octet_length(result_heads_digest)=32 AND octet_length(barrier_intent_digest)=32
  AND octet_length(disposition_digest)=32 AND octet_length(original_projection_digest)=32
  AND octet_length(intent_digest)=32 AND octet_length(compound_digest)=32 AND octet_length(journal_digest)=32
  AND octet_length(lifecycle_before_bytes) BETWEEN 1 AND 2097152
  AND octet_length(lifecycle_after_bytes) BETWEEN 1 AND 2097152
  AND octet_length(bundle_bytes) BETWEEN 1 AND 14241792
  AND (octet_length(bundle_bytes)<=9474048
   OR convert_from(bundle_bytes,'UTF8')::jsonb->>'version' IN ('2','3'))
);


CREATE OR REPLACE FUNCTION stateknot.guard_root_graph_barrier_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE data jsonb; total bigint; wait_total bigint; creation jsonb; ready boolean;
BEGIN
 data:=convert_from(NEW.bundle_bytes,'UTF8')::jsonb;
 ready:=coalesce(data->>'version'='3' AND data->'wait_inventory'->>'state'='ready',false);
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
     OR (NEW.disposition_kind<>'terminal' AND NOT (data->'intent' ? 'terminal_validation'))))
   OR (data->>'version' IS NOT DISTINCT FROM '3' AND data->'intent'->>'version' IS NOT DISTINCT FROM '3'
    AND NEW.disposition_kind='wait' AND NOT (data->'intent' ? 'terminal_validation')
    AND ((data->'intent'->'source'->>'kind' IS NOT DISTINCT FROM 'trusted_graph'
      AND jsonb_typeof(data->'intent'->'source'->'creation') IS NOT DISTINCT FROM 'object'
      AND NOT (data->'intent' ? 'elapsed_agent_source'))
     OR (NOT (data->'intent' ? 'source')
      AND jsonb_typeof(data->'intent'->'elapsed_agent_source') IS NOT DISTINCT FROM 'object'))))
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
      AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'source'='trusted_graph')
     OR (data->>'version'='3' AND e.schema_id='https://stknot.com/schemas/store/root-graph-barrier-event/3.0.0' AND e.schema_version='3.0.0'
      AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'version'='3'
      AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'source'=CASE WHEN data->'intent' ? 'source' THEN 'trusted_graph' ELSE 'agent_admission' END))
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'graph_namespace'=''
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'base_checkpoint_id'=NEW.base_checkpoint_id::text
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'base_checkpoint_digest'='sha256:'||encode(NEW.base_checkpoint_digest,'hex')
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'successor_checkpoint_id'=NEW.successor_checkpoint_id::text
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'barrier_intent_digest'='sha256:'||encode(NEW.barrier_intent_digest,'hex')
    AND convert_from(e.payload_bytes,'UTF8')::jsonb->'data'->>'intent_digest'='sha256:'||encode(NEW.intent_digest,'hex')
    AND r.journal_sequence=NEW.journal_sequence AND r.journal_event_id=NEW.journal_event_id AND r.journal_recorded_at=NEW.journal_recorded_at AND r.journal_digest=NEW.journal_digest
    AND r.checkpoint_id=NEW.successor_checkpoint_id AND r.checkpoint_superstep=NEW.successor_superstep AND r.checkpoint_digest=NEW.successor_checkpoint_digest
    AND r.lifecycle_bytes=NEW.lifecycle_after_bytes AND r.lifecycle_revision=NEW.lifecycle_after_revision
    AND r.lifecycle_status=CASE WHEN NEW.disposition_kind='continue' OR ready THEN 'active' WHEN NEW.disposition_kind='wait' THEN 'waiting' ELSE 'succeeded' END
    AND (( (NEW.disposition_kind='continue' OR ready) AND r.lease_attempt_id=e.worker_attempt_id AND r.fencing_epoch=e.worker_epoch)
     OR (NEW.disposition_kind<>'continue' AND NOT ready AND r.lease_attempt_id IS NULL))
  ) OR EXISTS (
   SELECT 1 FROM stateknot.pending_node_result_consumptions p WHERE p.tenant_id=NEW.tenant_id AND p.run_id=NEW.run_id AND p.base_checkpoint_id=NEW.base_checkpoint_id
    AND (p.graph_namespace<>'' OR p.base_superstep<>NEW.base_superstep OR p.base_checkpoint_digest<>NEW.base_checkpoint_digest
     OR p.successor_checkpoint_id<>NEW.successor_checkpoint_id OR p.successor_superstep<>NEW.successor_superstep OR p.successor_checkpoint_digest<>NEW.successor_checkpoint_digest
     OR p.successor_journal_sequence<>NEW.journal_sequence OR p.successor_journal_event_id<>NEW.journal_event_id OR p.successor_journal_recorded_at<>NEW.journal_recorded_at OR p.successor_journal_digest<>NEW.journal_digest)
  ) OR EXISTS (SELECT 1 FROM stateknot.graph_frame_stacks s WHERE s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id AND s.active_namespace<>'') THEN
  RAISE EXCEPTION 'incomplete whole Root graph barrier' USING ERRCODE='SKG02';
 END IF;
 IF data->>'version'='3' THEN
  IF wait_total::text IS DISTINCT FROM data->'wait_inventory'->>'registered_count'
   OR jsonb_typeof(data->'intent'->'disposition'->'registrations') IS DISTINCT FROM 'array'
   OR jsonb_array_length(data->'intent'->'disposition'->'registrations') NOT BETWEEN 1 AND 64
   OR EXISTS (SELECT 1 FROM stateknot.run_wait_registrations w WHERE w.tenant_id=NEW.tenant_id AND w.run_id=NEW.run_id AND w.registration_sequence=NEW.journal_sequence
    AND (w.registration_event_id<>NEW.journal_event_id OR w.registered_at<>NEW.journal_recorded_at OR w.registration_event_digest<>NEW.journal_digest OR w.status<>'outstanding'))
  THEN RAISE EXCEPTION 'incomplete original-condition Root Wait' USING ERRCODE='SKG02'; END IF;
 ELSE
 IF NEW.disposition_kind='wait' THEN
  IF jsonb_typeof(data->'intent'->'disposition'->'registrations') IS DISTINCT FROM 'array' OR wait_total NOT BETWEEN 1 AND 64
   OR wait_total<>jsonb_array_length(data->'intent'->'disposition'->'registrations') OR EXISTS (
    SELECT 1 FROM jsonb_array_elements(data->'intent'->'disposition'->'registrations') w WHERE NOT EXISTS (
     SELECT 1 FROM stateknot.run_wait_registrations r WHERE r.tenant_id=NEW.tenant_id AND r.run_id=NEW.run_id
      AND r.registration_sequence=NEW.journal_sequence AND r.registration_event_id=NEW.journal_event_id AND r.registered_at=NEW.journal_recorded_at AND r.registration_event_digest=NEW.journal_digest
      AND r.wait_kind=w->>'kind' AND r.wait_id::text=w->>'wait_id' AND 'sha256:'||encode(r.intent_digest,'hex')=w->>'intent_digest' AND r.status='outstanding'
    )) THEN RAISE EXCEPTION 'incomplete whole Root wait barrier' USING ERRCODE='SKG02'; END IF;
 ELSIF wait_total<>0 THEN RAISE EXCEPTION 'unexpected whole Root wait registrations' USING ERRCODE='SKG02'; END IF;
 END IF;
 IF data->>'version'='2' OR (data->>'version'='3' AND data->'intent' ? 'source') THEN
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
 IF data->>'version'='3' AND data->'intent' ? 'elapsed_agent_source' THEN
  creation:=data->'intent'->'elapsed_agent_source';
  IF NOT EXISTS (
   SELECT 1 FROM stateknot.agent_admissions a
   JOIN stateknot.run_events f ON f.tenant_id=a.tenant_id AND f.run_id=a.run_id AND f.sequence=a.journal_sequence
   JOIN stateknot.run_checkpoints c ON c.tenant_id=a.tenant_id AND c.run_id=a.run_id AND c.graph_namespace='' AND c.checkpoint_id=a.checkpoint_id
   JOIN stateknot.runs r ON r.tenant_id=a.tenant_id AND r.run_id=a.run_id
   WHERE a.tenant_id=NEW.tenant_id AND a.run_id=NEW.run_id
    AND a.journal_sequence=1 AND f.previous_digest IS NULL AND f.event_kind='agent-admitted'
    AND f.event_id=a.journal_event_id AND f.recorded_at=a.journal_recorded_at AND f.event_digest=a.journal_digest
    AND c.superstep=0 AND c.superstep=a.checkpoint_superstep AND c.checkpoint_digest=a.checkpoint_digest
    AND c.parent_checkpoint_id IS NULL AND c.parent_superstep IS NULL AND c.parent_digest IS NULL
    AND c.journal_sequence=f.sequence AND c.journal_event_id=f.event_id AND c.journal_recorded_at=f.recorded_at AND c.journal_digest=f.event_digest
    AND convert_from(c.checkpoint_bytes,'UTF8')::jsonb->'graph'=data->'intent'->'base'->'graph'
    AND r.admitted_at=a.admitted_at AND a.admitted_at=f.recorded_at
    AND data->'intent'->'lifecycle_before'->'provenance'=convert_from(a.admission_bytes,'UTF8')::jsonb->'intent'->'provenance'
    AND data->'intent'->'lifecycle_before'->>'admitted_at'=to_char(a.admitted_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    AND r.agent_deadline_at=(convert_from(a.admission_bytes,'UTF8')::jsonb #>> '{intent,budget,deadline}')::timestamptz
    AND r.agent_deadline_at>NEW.journal_recorded_at
    AND creation->>'admission_digest'='sha256:'||encode(a.admission_digest,'hex')
    AND creation->'first_journal'=jsonb_build_object('tenant_id',f.tenant_id,'run_id',f.run_id::text,'sequence',f.sequence::text,
     'event_id',f.event_id::text,'recorded_at',to_char(f.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'digest','sha256:'||encode(f.event_digest,'hex'))
    AND creation->'initial_checkpoint'=jsonb_build_object('tenant_id',c.tenant_id,'run_id',c.run_id::text,'checkpoint_id',c.checkpoint_id::text,
     'superstep',c.superstep::text,'graph',convert_from(c.checkpoint_bytes,'UTF8')::jsonb->'graph','journal_head',creation->'first_journal','digest','sha256:'||encode(c.checkpoint_digest,'hex'))
  ) THEN RAISE EXCEPTION 'elapsed Root requires its positive original Agent admission' USING ERRCODE='SKG02'; END IF;
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
    OR (convert_from(b.bundle_bytes,'UTF8')::jsonb->>'version' IN ('2','3') AND OLD.agent_deadline_at IS NULL
     AND convert_from(b.bundle_bytes,'UTF8')::jsonb->'intent'->'source'->>'kind'='trusted_graph')
    OR (convert_from(b.bundle_bytes,'UTF8')::jsonb->>'version'='3' AND OLD.agent_deadline_at>clock_timestamp()
     AND NOT (convert_from(b.bundle_bytes,'UTF8')::jsonb->'intent' ? 'source')
     AND jsonb_typeof(convert_from(b.bundle_bytes,'UTF8')::jsonb->'intent'->'elapsed_agent_source')='object'))
   AND (NOT (convert_from(b.bundle_bytes,'UTF8')::jsonb->>'version'='3'
     AND convert_from(b.bundle_bytes,'UTF8')::jsonb->'wait_inventory'->>'state'='ready')
    OR (NEW.lifecycle_status=OLD.lifecycle_status AND NEW.lifecycle_bytes=OLD.lifecycle_bytes AND NEW.lifecycle_revision=OLD.lifecycle_revision
     AND NEW.changed_at=OLD.changed_at AND NEW.lease_attempt_id IS NOT DISTINCT FROM OLD.lease_attempt_id
     AND NEW.fencing_epoch IS NOT DISTINCT FROM OLD.fencing_epoch AND NEW.lease_acquired_at IS NOT DISTINCT FROM OLD.lease_acquired_at
     AND NEW.lease_renewed_at IS NOT DISTINCT FROM OLD.lease_renewed_at AND NEW.lease_expires_at IS NOT DISTINCT FROM OLD.lease_expires_at
     AND NEW.scheduler_ready_at IS NOT DISTINCT FROM OLD.scheduler_ready_at AND NEW.scheduler_not_before IS NOT DISTINCT FROM OLD.scheduler_not_before
     AND NEW.wait_set_digest IS NOT DISTINCT FROM OLD.wait_set_digest AND NEW.unresolved_wait_count=OLD.unresolved_wait_count
     AND NEW.next_timer_due_at IS NOT DISTINCT FROM OLD.next_timer_due_at AND NEW.next_interrupt_expiry_at IS NOT DISTINCT FROM OLD.next_interrupt_expiry_at))
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
  IF data->>'version'='3' THEN
   IF bundle.disposition_kind<>'wait'
    OR NEW.registration_event_id<>bundle.journal_event_id OR NEW.registered_at<>bundle.journal_recorded_at OR NEW.registration_event_digest<>bundle.journal_digest
    OR NOT EXISTS (SELECT 1 FROM stateknot.graph_wait_conditions c WHERE c.tenant_id=NEW.tenant_id AND c.run_id=NEW.run_id
     AND c.wait_id=NEW.wait_id AND c.profile='graph_registered' AND c.graph_namespace=''
     AND c.journal_sequence=bundle.journal_sequence AND c.intent_digest=NEW.intent_digest AND c.registration_digest=NEW.record_digest)
   THEN RAISE EXCEPTION 'Root wait cannot extend its original V3 inventory' USING ERRCODE='SKG02'; END IF;
   RETURN NULL;
  END IF;
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
 ) AND NOT EXISTS (
  SELECT 1 FROM stateknot.graph_frame_stacks s
  JOIN stateknot.graph_frame_heads h USING (tenant_id,run_id)
  JOIN stateknot.graph_frame_barriers b ON b.tenant_id=s.tenant_id AND b.run_id=s.run_id AND b.graph_namespace=s.active_namespace AND b.successor_checkpoint_id=h.checkpoint_id
  JOIN stateknot.run_events v ON v.tenant_id=b.tenant_id AND v.run_id=b.run_id AND v.sequence=b.journal_sequence
  JOIN stateknot.graph_wait_conditions c ON c.tenant_id=b.tenant_id AND c.run_id=b.run_id AND c.graph_namespace=b.graph_namespace AND c.journal_sequence=b.journal_sequence
  WHERE s.tenant_id=NEW.tenant_id AND s.run_id=NEW.run_id AND h.graph_namespace=s.active_namespace
   AND b.frame_identity_digest=s.active_frame_identity_digest AND b.successor_frame_checkpoint_digest=h.frame_checkpoint_digest
   AND b.journal_sequence=NEW.registration_sequence AND b.journal_event_id=NEW.registration_event_id
   AND b.journal_recorded_at=NEW.registered_at AND b.journal_digest=NEW.registration_event_digest
   AND v.source_kind='worker' AND v.event_kind='graph-frame-barrier-committed' AND v.projection_digest=b.compound_digest
   AND convert_from(b.barrier_bytes,'UTF8')::jsonb->>'version'='3'
   AND convert_from(b.barrier_bytes,'UTF8')::jsonb->'disposition'->>'kind'='wait'
   AND c.profile='graph_registered' AND c.wait_kind=NEW.wait_kind AND c.wait_id=NEW.wait_id
   AND c.intent_digest=NEW.intent_digest AND c.registration_digest=NEW.record_digest
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
  AND convert_from(b.barrier_bytes,'UTF8')::jsonb->>'version' IN ('2','3')
  AND convert_from(b.barrier_bytes,'UTF8')::jsonb->'disposition'->>'kind'='wait';
 IF suspension.journal_sequence=NEW.journal_sequence
  AND convert_from(suspension.barrier_bytes,'UTF8')::jsonb->>'version'='3'
  AND convert_from(suspension.barrier_bytes,'UTF8')::jsonb->'wait_inventory'->>'state'='ready' THEN
  IF OLD.lifecycle_status<>'active' OR NEW.lifecycle_status<>OLD.lifecycle_status OR NEW.lifecycle_bytes<>OLD.lifecycle_bytes
   OR NEW.lifecycle_revision<>OLD.lifecycle_revision OR NEW.changed_at<>OLD.changed_at
   OR OLD.lifecycle_revision<>(convert_from(suspension.barrier_bytes,'UTF8')::jsonb->>'wait_revision')::numeric
   OR NEW.wait_set_digest IS DISTINCT FROM OLD.wait_set_digest OR NEW.unresolved_wait_count<>OLD.unresolved_wait_count
   OR NEW.next_timer_due_at IS DISTINCT FROM OLD.next_timer_due_at OR NEW.next_interrupt_expiry_at IS DISTINCT FROM OLD.next_interrupt_expiry_at
  THEN RAISE EXCEPTION 'Ready scoped Wait must preserve its exact original lifecycle and lease' USING ERRCODE='SKG02'; END IF;
  -- Creating Ready preserves the original authority. Later same-journal
  -- claim/renew/release operations retain their ordinary lease checks while
  -- the lifecycle and complete wait projection above remain frozen.
  IF OLD.journal_sequence IS DISTINCT FROM NEW.journal_sequence AND (
   NEW.lease_attempt_id IS DISTINCT FROM OLD.lease_attempt_id OR NEW.fencing_epoch IS DISTINCT FROM OLD.fencing_epoch
   OR NEW.lease_acquired_at IS DISTINCT FROM OLD.lease_acquired_at OR NEW.lease_renewed_at IS DISTINCT FROM OLD.lease_renewed_at
   OR NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at OR OLD.lease_expires_at<=clock_timestamp()
   OR OLD.agent_deadline_at<=clock_timestamp()
   OR NEW.scheduler_ready_at IS DISTINCT FROM OLD.scheduler_ready_at OR NEW.scheduler_not_before IS DISTINCT FROM OLD.scheduler_not_before
  ) THEN RAISE EXCEPTION 'Ready scoped Wait must preserve its exact original lifecycle and lease' USING ERRCODE='SKG02'; END IF;
 END IF;
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
 IF data->>'version'='3' THEN
  SELECT count(*) INTO total FROM stateknot.run_wait_registrations
   WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND registration_sequence=NEW.journal_sequence;
  IF data->>'wait_revision' IS NULL OR total::text IS DISTINCT FROM data->'wait_inventory'->>'registered_count'
   OR data->'disposition'->>'condition_count' IS DISTINCT FROM jsonb_array_length(data->'wait_inventory'->'conditions')::text
   OR EXISTS (SELECT 1 FROM stateknot.run_wait_registrations w WHERE w.tenant_id=NEW.tenant_id AND w.run_id=NEW.run_id
    AND w.registration_sequence=NEW.journal_sequence AND (w.registration_event_id<>NEW.journal_event_id
     OR w.registered_at<>NEW.journal_recorded_at OR w.registration_event_digest<>NEW.journal_digest OR w.status<>'outstanding'))
   OR NOT EXISTS (
    SELECT 1 FROM stateknot.runs r JOIN stateknot.graph_frame_stacks s USING (tenant_id,run_id)
    JOIN stateknot.run_events e ON e.tenant_id=r.tenant_id AND e.run_id=r.run_id AND e.sequence=NEW.journal_sequence
    WHERE r.tenant_id=NEW.tenant_id AND r.run_id=NEW.run_id AND r.journal_sequence=NEW.journal_sequence AND r.journal_digest=NEW.journal_digest
     AND s.active_namespace=NEW.graph_namespace AND s.active_frame_identity_digest=NEW.frame_identity_digest
     AND ((total=0 AND data->'wait_inventory'->>'state'='ready' AND r.lifecycle_status='active'
       AND r.lifecycle_revision=(data->>'wait_revision')::numeric AND r.lease_attempt_id=e.worker_attempt_id AND r.fencing_epoch=e.worker_epoch
       AND r.unresolved_wait_count=0)
      OR (total BETWEEN 1 AND 64 AND data->'wait_inventory'->>'state'='waiting' AND r.lifecycle_status='waiting'
       AND r.lifecycle_revision=(data->>'wait_revision')::numeric+1 AND r.unresolved_wait_count=total AND r.lease_attempt_id IS NULL))
   ) THEN RAISE EXCEPTION 'incomplete original-condition scoped suspension' USING ERRCODE='SKG02'; END IF;
  RETURN NULL;
 END IF;
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
