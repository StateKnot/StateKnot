-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- No backfill: accounting settlement is not an application Join decision.
LOCK TABLE stateknot.runs IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE stateknot.child_run_joins (
    tenant_id text NOT NULL,
    parent_run_id uuid NOT NULL,
    activation_digest bytea NOT NULL CHECK (octet_length(activation_digest) = 32),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    request_bytes bytea NOT NULL CHECK (octet_length(request_bytes) BETWEEN 1 AND 4194304),
    request_checksum bytea NOT NULL CHECK (request_checksum = sha256(request_bytes)),
    base_checkpoint_id uuid NOT NULL,
    graph_namespace text NOT NULL,
    node_id text NOT NULL,
    node_attempt_id uuid NOT NULL,
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    queued_at timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
    ready_at timestamptz(6),
    consumed_at timestamptz(6),
    PRIMARY KEY (tenant_id, parent_run_id, activation_digest),
    CONSTRAINT child_run_joins_activation_unique UNIQUE (tenant_id, parent_run_id, base_checkpoint_id, graph_namespace, node_id),
    CONSTRAINT child_run_joins_request_unique UNIQUE (tenant_id, parent_run_id, request_digest),
    CONSTRAINT child_run_joins_node_fk FOREIGN KEY (tenant_id, parent_run_id, node_attempt_id)
        REFERENCES stateknot.node_attempts (tenant_id, run_id, attempt_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_joins_event_fk FOREIGN KEY (tenant_id, parent_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
        REFERENCES stateknot.run_events (tenant_id, run_id, sequence, event_id, recorded_at, event_digest) ON DELETE RESTRICT,
    CONSTRAINT child_run_joins_clock_valid CHECK ((ready_at IS NULL OR ready_at >= queued_at) AND (consumed_at IS NULL OR (ready_at IS NOT NULL AND consumed_at >= ready_at)))
);
CREATE INDEX child_run_joins_pending ON stateknot.child_run_joins (tenant_id, queued_at, parent_run_id, activation_digest) WHERE ready_at IS NULL;

CREATE TABLE stateknot.child_run_join_bindings (
    tenant_id text NOT NULL,
    parent_run_id uuid NOT NULL,
    activation_digest bytea NOT NULL,
    binding_bytes bytea NOT NULL CHECK (octet_length(binding_bytes) BETWEEN 1 AND 4194304),
    binding_checksum bytea NOT NULL CHECK (binding_checksum = sha256(binding_bytes)),
    head_bytes bytea NOT NULL CHECK (octet_length(head_bytes) BETWEEN 1 AND 65536),
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    ready_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, parent_run_id, activation_digest),
    CONSTRAINT child_run_join_bindings_request_fk FOREIGN KEY (tenant_id, parent_run_id, activation_digest)
        REFERENCES stateknot.child_run_joins (tenant_id, parent_run_id, activation_digest) ON DELETE RESTRICT,
    CONSTRAINT child_run_join_bindings_event_fk FOREIGN KEY (tenant_id, parent_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
        REFERENCES stateknot.run_events (tenant_id, run_id, sequence, event_id, recorded_at, event_digest) ON DELETE RESTRICT
);

CREATE TABLE stateknot.child_run_join_consumptions (
    tenant_id text NOT NULL,
    parent_run_id uuid NOT NULL,
    activation_digest bytea NOT NULL,
    journal_sequence bigint NOT NULL,
    consumed_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, parent_run_id, activation_digest),
    CONSTRAINT child_run_join_consumptions_binding_fk FOREIGN KEY (tenant_id, parent_run_id, activation_digest)
        REFERENCES stateknot.child_run_join_bindings (tenant_id, parent_run_id, activation_digest) ON DELETE RESTRICT,
    CONSTRAINT child_run_join_consumptions_result_fk FOREIGN KEY (tenant_id, parent_run_id, journal_sequence)
        REFERENCES stateknot.pending_node_results (tenant_id, run_id, journal_sequence) ON DELETE RESTRICT
);

-- Independent compatibility version: published migration 20's guard is unchanged.
CREATE FUNCTION stateknot.guard_child_join_run() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM stateknot.child_run_joins WHERE tenant_id=NEW.tenant_id AND parent_run_id=NEW.run_id) THEN
        IF current_setting('stateknot.child_join_version', true) IS DISTINCT FROM '1' THEN
            RAISE EXCEPTION 'compatible child Join writer required' USING ERRCODE='SKC01';
        END IF;
        IF NEW.lifecycle_status='active' AND NEW.lease_attempt_id IS NOT NULL
           AND EXISTS (SELECT 1 FROM stateknot.child_run_joins WHERE tenant_id=NEW.tenant_id AND parent_run_id=NEW.run_id AND ready_at IS NULL) THEN
            RAISE EXCEPTION 'child Join is waiting' USING ERRCODE='SKC06';
        END IF;
        IF (NEW.checkpoint_id IS DISTINCT FROM OLD.checkpoint_id OR NEW.lifecycle_status='succeeded')
           AND EXISTS (SELECT 1 FROM stateknot.child_run_joins WHERE tenant_id=NEW.tenant_id AND parent_run_id=NEW.run_id AND consumed_at IS NULL) THEN
            RAISE EXCEPTION 'child Join has not been consumed' USING ERRCODE='SKC07';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER runs_child_join_guard BEFORE UPDATE ON stateknot.runs FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_join_run();

CREATE FUNCTION stateknot.guard_child_join_spawn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM stateknot.child_run_joins WHERE tenant_id=NEW.tenant_id AND parent_run_id=NEW.parent_run_id AND activation_digest=NEW.activation_digest) THEN
        RAISE EXCEPTION 'child Join membership is sealed' USING ERRCODE='SKC07';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER child_join_spawn_guard BEFORE INSERT ON stateknot.child_run_ownership FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_join_spawn();

-- Result insertion and Join consumption are indivisible, including low-level
-- writers. The Rust boundary additionally authenticates every terminal anchor.
CREATE FUNCTION stateknot.consume_child_join_result() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    joined stateknot.child_run_joins%ROWTYPE;
    published stateknot.child_run_join_bindings%ROWTYPE;
    supplied jsonb;
BEGIN
    supplied := convert_from(NEW.result_bytes, 'UTF8')::jsonb #> '{intent,child_join}';
    SELECT * INTO joined FROM stateknot.child_run_joins WHERE tenant_id=NEW.tenant_id AND parent_run_id=NEW.run_id
        AND base_checkpoint_id=NEW.base_checkpoint_id AND graph_namespace=NEW.graph_namespace AND node_id=NEW.node_id;
    IF NOT FOUND THEN
        IF supplied IS NOT NULL AND supplied <> 'null'::jsonb THEN
            RAISE EXCEPTION 'child Join has no registration' USING ERRCODE='SKC07';
        END IF;
        RETURN NEW;
    END IF;
    SELECT * INTO published FROM stateknot.child_run_join_bindings WHERE tenant_id=joined.tenant_id
        AND parent_run_id=joined.parent_run_id AND activation_digest=joined.activation_digest;
    IF NOT FOUND OR current_setting('stateknot.child_join_version', true) IS DISTINCT FROM '1'
       OR supplied IS DISTINCT FROM convert_from(published.head_bytes, 'UTF8')::jsonb
       OR NEW.journal_sequence <= published.journal_sequence OR NEW.journal_recorded_at < published.journal_recorded_at THEN
        RAISE EXCEPTION 'child Join result binding mismatch' USING ERRCODE='SKC07';
    END IF;
    INSERT INTO stateknot.child_run_join_consumptions VALUES
        (joined.tenant_id, joined.parent_run_id, joined.activation_digest, NEW.journal_sequence, GREATEST(clock_timestamp(), published.ready_at));
    UPDATE stateknot.child_run_joins SET consumed_at=(SELECT consumed_at FROM stateknot.child_run_join_consumptions
        WHERE tenant_id=joined.tenant_id AND parent_run_id=joined.parent_run_id AND activation_digest=joined.activation_digest)
        WHERE tenant_id=joined.tenant_id AND parent_run_id=joined.parent_run_id AND activation_digest=joined.activation_digest;
    RETURN NEW;
END
$$;
CREATE TRIGGER pending_results_child_join_consume AFTER INSERT ON stateknot.pending_node_results FOR EACH ROW EXECUTE FUNCTION stateknot.consume_child_join_result();

CREATE FUNCTION stateknot.guard_child_join_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR TG_TABLE_NAME <> 'child_run_joins' THEN
        RAISE EXCEPTION 'child Join evidence is immutable' USING ERRCODE='SKC05';
    END IF;
    IF (to_jsonb(NEW) - ARRAY['ready_at','consumed_at']) IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['ready_at','consumed_at'])
       OR (OLD.ready_at IS NOT NULL AND NEW.ready_at IS DISTINCT FROM OLD.ready_at)
       OR (OLD.consumed_at IS NOT NULL AND NEW.consumed_at IS DISTINCT FROM OLD.consumed_at)
       OR NEW.ready_at IS DISTINCT FROM (SELECT ready_at FROM stateknot.child_run_join_bindings WHERE tenant_id=NEW.tenant_id AND parent_run_id=NEW.parent_run_id AND activation_digest=NEW.activation_digest)
       OR NEW.consumed_at IS DISTINCT FROM (SELECT consumed_at FROM stateknot.child_run_join_consumptions WHERE tenant_id=NEW.tenant_id AND parent_run_id=NEW.parent_run_id AND activation_digest=NEW.activation_digest) THEN
        RAISE EXCEPTION 'child Join projection mismatch' USING ERRCODE='SKC05';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER child_joins_immutable BEFORE UPDATE OR DELETE ON stateknot.child_run_joins FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_join_evidence();
CREATE TRIGGER child_join_bindings_immutable BEFORE UPDATE OR DELETE ON stateknot.child_run_join_bindings FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_join_evidence();
CREATE TRIGGER child_join_consumptions_immutable BEFORE UPDATE OR DELETE ON stateknot.child_run_join_consumptions FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_join_evidence();
