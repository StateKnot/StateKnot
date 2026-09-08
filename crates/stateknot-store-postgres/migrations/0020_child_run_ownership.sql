-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- A capability fence prevents pre-child workers from claiming or mutating
-- child-enabled runs. SET LOCAL is issued by compatible store transactions;
-- this is mixed-binary protection, not an untrusted SQL-user security boundary.
ALTER TABLE stateknot.runs ADD COLUMN child_runtime_version smallint NOT NULL DEFAULT 0;
ALTER TABLE stateknot.runs ADD CONSTRAINT runs_child_runtime_version_valid
    CHECK (child_runtime_version IN (0, 1));

UPDATE stateknot.runs AS run SET child_runtime_version = 1
FROM stateknot.agent_admissions AS admission
JOIN stateknot.graph_definitions AS graph
  ON graph.tenant_id = admission.tenant_id
 AND graph.owner_issuer = admission.graph_owner_issuer
 AND graph.owner_subject = admission.graph_owner_subject
 AND graph.graph_name = admission.graph_name
 AND graph.graph_version = admission.graph_version
 AND graph.definition_digest = admission.graph_definition_digest
WHERE run.tenant_id = admission.tenant_id AND run.run_id = admission.run_id
  AND jsonb_typeof(convert_from(graph.definition_bytes, 'UTF8')::jsonb -> 'child_runs') = 'object';

CREATE TABLE stateknot.child_run_budget_accounts (
    tenant_id text NOT NULL,
    parent_run_id uuid NOT NULL,
    account_digest bytea NOT NULL CHECK (octet_length(account_digest) = 32),
    account_bytes bytea NOT NULL CHECK (octet_length(account_bytes) BETWEEN 1 AND 8388608),
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    PRIMARY KEY (tenant_id, parent_run_id),
    CONSTRAINT child_run_budget_accounts_admission_fk FOREIGN KEY (tenant_id, parent_run_id)
        REFERENCES stateknot.agent_admissions (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_budget_accounts_event_fk FOREIGN KEY
        (tenant_id, parent_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
        REFERENCES stateknot.run_events
        (tenant_id, run_id, sequence, event_id, recorded_at, event_digest) ON DELETE RESTRICT
);

CREATE TABLE stateknot.child_run_ownership (
    tenant_id text NOT NULL,
    parent_run_id uuid NOT NULL,
    key_digest bytea NOT NULL CHECK (octet_length(key_digest) = 32),
    key_bytes bytea NOT NULL CHECK (octet_length(key_bytes) BETWEEN 1 AND 65536),
    child_slot text NOT NULL CHECK (octet_length(child_slot) BETWEEN 1 AND 128),
    activation_digest bytea NOT NULL CHECK (octet_length(activation_digest) = 32),
    parent_node_attempt_id uuid NOT NULL,
    spawn_digest bytea NOT NULL CHECK (octet_length(spawn_digest) = 32),
    child_run_id uuid NOT NULL,
    root_run_id uuid NOT NULL,
    ancestors uuid[] NOT NULL,
    intent_bytes bytea NOT NULL CHECK (octet_length(intent_bytes) BETWEEN 1 AND 16777216),
    child_admission_digest bytea NOT NULL CHECK (octet_length(child_admission_digest) = 32),
    parent_checkpoint_id uuid NOT NULL,
    parent_checkpoint_superstep bigint NOT NULL,
    parent_checkpoint_digest bytea NOT NULL,
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    settled boolean NOT NULL DEFAULT false,
    terminal_pending_at timestamptz(6),
    PRIMARY KEY (tenant_id, parent_run_id, key_digest),
    CONSTRAINT child_run_ownership_child_unique UNIQUE (tenant_id, child_run_id),
    CONSTRAINT child_run_ownership_exact_unique UNIQUE (tenant_id, parent_run_id, key_digest, child_run_id),
    CONSTRAINT child_run_ownership_slot_unique UNIQUE (tenant_id, parent_run_id, activation_digest, child_slot),
    CONSTRAINT child_run_ownership_parent_fk FOREIGN KEY (tenant_id, parent_run_id)
        REFERENCES stateknot.agent_admissions (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_ownership_node_fk FOREIGN KEY (tenant_id, parent_run_id, parent_node_attempt_id)
        REFERENCES stateknot.node_attempts (tenant_id, run_id, attempt_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_ownership_budget_fk FOREIGN KEY (tenant_id, parent_run_id)
        REFERENCES stateknot.child_run_budget_accounts (tenant_id, parent_run_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT child_run_ownership_child_fk FOREIGN KEY (tenant_id, child_run_id)
        REFERENCES stateknot.agent_admissions (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_ownership_root_fk FOREIGN KEY (tenant_id, root_run_id)
        REFERENCES stateknot.agent_admissions (tenant_id, run_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_ownership_checkpoint_fk FOREIGN KEY
        (tenant_id, parent_run_id, parent_checkpoint_id, parent_checkpoint_superstep, parent_checkpoint_digest)
        REFERENCES stateknot.run_checkpoints (tenant_id, run_id, checkpoint_id, superstep, checkpoint_digest)
        ON DELETE RESTRICT,
    CONSTRAINT child_run_ownership_event_fk FOREIGN KEY
        (tenant_id, parent_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
        REFERENCES stateknot.run_events (tenant_id, run_id, sequence, event_id, recorded_at, event_digest)
        ON DELETE RESTRICT,
    CONSTRAINT child_run_ownership_ancestry_valid CHECK (
        array_ndims(ancestors) = 1 AND array_lower(ancestors, 1) = 1
        AND cardinality(ancestors) BETWEEN 1 AND 32
        AND array_position(ancestors, NULL) IS NULL
        AND ancestors[1] = root_run_id
        AND ancestors[cardinality(ancestors)] = parent_run_id
        AND NOT (child_run_id = ANY(ancestors))
        AND (NOT settled OR terminal_pending_at IS NULL)
    )
);

-- The root's active-descendant bound caps this partial-index range at 256.
CREATE INDEX child_run_ownership_active_tree
    ON stateknot.child_run_ownership (tenant_id, root_run_id, parent_run_id, child_run_id)
    WHERE NOT settled;
CREATE INDEX child_run_ownership_terminal_pending
    ON stateknot.child_run_ownership (tenant_id, terminal_pending_at, child_run_id)
    WHERE terminal_pending_at IS NOT NULL AND NOT settled;

-- Exact terminal anchors are captured in the child's own transaction, without
-- taking any ancestor run locks. They survive later journal appends and permit
-- reconciliation after process loss without re-executing the child.
CREATE TABLE stateknot.child_run_terminals (
    tenant_id text NOT NULL,
    child_run_id uuid NOT NULL,
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    lifecycle_bytes bytea NOT NULL CHECK (octet_length(lifecycle_bytes) BETWEEN 1 AND 4194304),
    lifecycle_digest bytea NOT NULL CHECK (octet_length(lifecycle_digest) = 32),
    PRIMARY KEY (tenant_id, child_run_id),
    CONSTRAINT child_run_terminals_owner_fk FOREIGN KEY (tenant_id, child_run_id)
        REFERENCES stateknot.child_run_ownership (tenant_id, child_run_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_terminals_event_fk FOREIGN KEY
        (tenant_id, child_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
        REFERENCES stateknot.run_events (tenant_id, run_id, sequence, event_id, recorded_at, event_digest)
        ON DELETE RESTRICT
);

CREATE TABLE stateknot.child_run_settlements (
    tenant_id text NOT NULL,
    parent_run_id uuid NOT NULL,
    key_digest bytea NOT NULL,
    child_run_id uuid NOT NULL,
    settlement_bytes bytea NOT NULL CHECK (octet_length(settlement_bytes) BETWEEN 1 AND 65536),
    settlement_digest bytea NOT NULL CHECK (octet_length(settlement_digest) = 32),
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    PRIMARY KEY (tenant_id, parent_run_id, key_digest),
    CONSTRAINT child_run_settlements_child_unique UNIQUE (tenant_id, child_run_id),
    CONSTRAINT child_run_settlements_owner_fk FOREIGN KEY (tenant_id, parent_run_id, key_digest, child_run_id)
        REFERENCES stateknot.child_run_ownership (tenant_id, parent_run_id, key_digest, child_run_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_settlements_terminal_fk FOREIGN KEY (tenant_id, child_run_id)
        REFERENCES stateknot.child_run_terminals (tenant_id, child_run_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_settlements_event_fk FOREIGN KEY
        (tenant_id, parent_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
        REFERENCES stateknot.run_events (tenant_id, run_id, sequence, event_id, recorded_at, event_digest)
        ON DELETE RESTRICT
);

CREATE FUNCTION stateknot.guard_child_run_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.child_runtime_version < OLD.child_runtime_version THEN
        RAISE EXCEPTION 'child runtime capability cannot regress' USING ERRCODE = 'SKC01';
    END IF;
    IF NEW.child_runtime_version > 0
       AND current_setting('stateknot.child_runtime_version', true) IS DISTINCT FROM '1' THEN
        RAISE EXCEPTION 'compatible child runtime required' USING ERRCODE = 'SKC01';
    END IF;
    IF NEW.lifecycle_status IN ('succeeded', 'failed', 'cancelled')
       AND EXISTS (SELECT 1 FROM stateknot.child_run_ownership
                   WHERE tenant_id = NEW.tenant_id AND parent_run_id = NEW.run_id AND NOT settled) THEN
        RAISE EXCEPTION 'unsettled owned children block parent closure' USING ERRCODE = 'SKC02';
    END IF;
    IF NEW.lifecycle_status IN ('succeeded', 'failed', 'cancelled')
       AND (NEW.lifecycle_bytes IS DISTINCT FROM OLD.lifecycle_bytes)
       AND EXISTS (SELECT 1 FROM stateknot.child_run_budget_accounts
                   WHERE tenant_id = NEW.tenant_id AND parent_run_id = NEW.run_id)
       AND current_setting('stateknot.child_terminal_digest', true)
           IS DISTINCT FROM encode(sha256(NEW.lifecycle_bytes), 'hex') THEN
        RAISE EXCEPTION 'verified child-inclusive terminal accounting required' USING ERRCODE = 'SKC04';
    END IF;
    -- Dedicated child waits are not fabricated as ordinary timer/interrupt waits.
    IF NEW.checkpoint_id IS DISTINCT FROM OLD.checkpoint_id
       AND EXISTS (SELECT 1 FROM stateknot.child_run_ownership
                   WHERE tenant_id = NEW.tenant_id AND parent_run_id = NEW.run_id AND NOT settled) THEN
        RAISE EXCEPTION 'unsettled owned children block checkpoint advancement' USING ERRCODE = 'SKC02';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER runs_child_mutation_guard BEFORE UPDATE ON stateknot.runs
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_run_mutation();

CREATE FUNCTION stateknot.capture_child_run_terminal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lifecycle_status IN ('succeeded', 'failed', 'cancelled')
       AND OLD.lifecycle_status NOT IN ('succeeded', 'failed', 'cancelled')
       AND EXISTS (SELECT 1 FROM stateknot.child_run_ownership
                   WHERE tenant_id = NEW.tenant_id AND child_run_id = NEW.run_id) THEN
        INSERT INTO stateknot.child_run_terminals
            (tenant_id, child_run_id, journal_sequence, journal_event_id, journal_recorded_at,
             journal_digest, lifecycle_bytes, lifecycle_digest)
        VALUES (NEW.tenant_id, NEW.run_id, NEW.journal_sequence, NEW.journal_event_id,
                NEW.journal_recorded_at, NEW.journal_digest, NEW.lifecycle_bytes, sha256(NEW.lifecycle_bytes));
        UPDATE stateknot.child_run_ownership SET terminal_pending_at = NEW.journal_recorded_at
        WHERE tenant_id = NEW.tenant_id AND child_run_id = NEW.run_id AND NOT settled;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER runs_child_terminal_capture AFTER UPDATE ON stateknot.runs
    FOR EACH ROW EXECUTE FUNCTION stateknot.capture_child_run_terminal();

CREATE FUNCTION stateknot.mark_child_graph_admission() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM stateknot.graph_definitions AS graph
               WHERE graph.tenant_id = NEW.tenant_id AND graph.owner_issuer = NEW.graph_owner_issuer
                 AND graph.owner_subject = NEW.graph_owner_subject AND graph.graph_name = NEW.graph_name
                 AND graph.graph_version = NEW.graph_version AND graph.definition_digest = NEW.graph_definition_digest
                 AND jsonb_typeof(convert_from(graph.definition_bytes, 'UTF8')::jsonb -> 'child_runs') = 'object') THEN
        UPDATE stateknot.runs SET child_runtime_version = 1
        WHERE tenant_id = NEW.tenant_id AND run_id = NEW.run_id;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER admissions_child_capability AFTER INSERT ON stateknot.agent_admissions
    FOR EACH ROW EXECUTE FUNCTION stateknot.mark_child_graph_admission();

CREATE FUNCTION stateknot.guard_child_direct_invocation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_digest bytea;
BEGIN
    IF EXISTS (SELECT 1 FROM stateknot.child_run_ownership
               WHERE tenant_id = NEW.tenant_id AND parent_run_id = NEW.run_id AND NOT settled) THEN
        RAISE EXCEPTION 'owned child work excludes parent external dispatch' USING ERRCODE = 'SKC02';
    END IF;
    IF NEW.transition_kind = 'start_attempt' THEN
        SELECT account_digest INTO expected_digest FROM stateknot.child_run_budget_accounts
        WHERE tenant_id = NEW.tenant_id AND parent_run_id = NEW.run_id;
        IF expected_digest IS NULL AND EXISTS (SELECT 1 FROM stateknot.child_run_ownership
            WHERE tenant_id = NEW.tenant_id AND parent_run_id = NEW.run_id) THEN
            RAISE EXCEPTION 'missing child budget account' USING ERRCODE = 'SKC03';
        END IF;
        IF expected_digest IS NOT NULL AND current_setting('stateknot.child_budget_digest', true)
            IS DISTINCT FROM encode(expected_digest, 'hex') THEN
            RAISE EXCEPTION 'fresh child-adjusted invocation budget required' USING ERRCODE = 'SKC03';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER tool_revisions_child_budget_guard BEFORE INSERT ON stateknot.tool_invocation_revisions
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_direct_invocation();
CREATE TRIGGER model_revisions_child_budget_guard BEFORE INSERT ON stateknot.model_invocation_revisions
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_direct_invocation();

-- Ownership identities and terminal/settlement facts are append-only. Only the
-- notification/settled projection may advance, and only when its evidence exists.
CREATE FUNCTION stateknot.guard_child_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_TABLE_NAME <> 'child_run_ownership' THEN
        RAISE EXCEPTION 'child evidence is immutable' USING ERRCODE = 'SKC05';
    END IF;
    IF (to_jsonb(NEW) - 'settled' - 'terminal_pending_at') IS DISTINCT FROM
       (to_jsonb(OLD) - 'settled' - 'terminal_pending_at') OR (OLD.settled AND NOT NEW.settled) THEN
        RAISE EXCEPTION 'child ownership is immutable' USING ERRCODE = 'SKC05';
    END IF;
    IF NEW.settled IS DISTINCT FROM EXISTS (SELECT 1 FROM stateknot.child_run_settlements
        WHERE tenant_id = NEW.tenant_id AND parent_run_id = NEW.parent_run_id AND key_digest = NEW.key_digest)
       OR NEW.terminal_pending_at IS DISTINCT FROM
          (SELECT CASE WHEN NEW.settled THEN NULL ELSE journal_recorded_at END
           FROM stateknot.child_run_terminals WHERE tenant_id = NEW.tenant_id AND child_run_id = NEW.child_run_id) THEN
        RAISE EXCEPTION 'child evidence projection mismatch' USING ERRCODE = 'SKC05';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER ownership_child_immutable BEFORE UPDATE OR DELETE ON stateknot.child_run_ownership
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_evidence();
CREATE TRIGGER terminals_child_immutable BEFORE UPDATE OR DELETE ON stateknot.child_run_terminals
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_evidence();
CREATE TRIGGER settlements_child_immutable BEFORE UPDATE OR DELETE ON stateknot.child_run_settlements
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_evidence();
