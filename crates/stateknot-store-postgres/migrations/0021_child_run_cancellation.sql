-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- An append-only cancellation witness, captured by EVERY parent cancellation
-- writer. No child/ancestor Run locks are acquired by this trigger. The parent
-- Run lock already serializes cancellation against fresh child admission.
-- Acquire the Run lock before FK DDL locks ownership, matching writer order.
LOCK TABLE stateknot.runs IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE stateknot.child_run_cancellations (
    tenant_id text NOT NULL,
    parent_run_id uuid NOT NULL,
    key_digest bytea NOT NULL CHECK (octet_length(key_digest) = 32),
    child_run_id uuid NOT NULL,
    lifecycle_bytes bytea NOT NULL CHECK (octet_length(lifecycle_bytes) BETWEEN 1 AND 4194304),
    lifecycle_digest bytea NOT NULL CHECK (lifecycle_digest = sha256(lifecycle_bytes)),
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    queued_at timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
    delivered_at timestamptz(6),
    PRIMARY KEY (tenant_id, parent_run_id, key_digest),
    CONSTRAINT child_run_cancellations_child_unique UNIQUE (tenant_id, child_run_id),
    CONSTRAINT child_run_cancellations_exact_unique UNIQUE (tenant_id, parent_run_id, key_digest, child_run_id),
    CONSTRAINT child_run_cancellations_owner_fk FOREIGN KEY (tenant_id, parent_run_id, key_digest, child_run_id)
        REFERENCES stateknot.child_run_ownership (tenant_id, parent_run_id, key_digest, child_run_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_cancellations_event_fk FOREIGN KEY
        (tenant_id, parent_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
        REFERENCES stateknot.run_events (tenant_id, run_id, sequence, event_id, recorded_at, event_digest) ON DELETE RESTRICT,
    CONSTRAINT child_run_cancellations_clock_valid CHECK (delivered_at IS NULL OR delivered_at >= queued_at)
);
CREATE INDEX child_run_cancellations_pending
    ON stateknot.child_run_cancellations (tenant_id, queued_at, child_run_id)
    WHERE delivered_at IS NULL;

CREATE TABLE stateknot.child_run_cancellation_receipts (
    tenant_id text NOT NULL,
    parent_run_id uuid NOT NULL,
    key_digest bytea NOT NULL,
    child_run_id uuid NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('requested', 'already_requested', 'terminal')),
    lifecycle_bytes bytea NOT NULL CHECK (octet_length(lifecycle_bytes) BETWEEN 1 AND 4194304),
    lifecycle_digest bytea NOT NULL CHECK (lifecycle_digest = sha256(lifecycle_bytes)),
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    delivered_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, parent_run_id, key_digest),
    CONSTRAINT child_run_cancellation_receipts_child_unique UNIQUE (tenant_id, child_run_id),
    CONSTRAINT child_run_cancellation_receipts_source_fk FOREIGN KEY (tenant_id, parent_run_id, key_digest, child_run_id)
        REFERENCES stateknot.child_run_cancellations (tenant_id, parent_run_id, key_digest, child_run_id) ON DELETE RESTRICT,
    CONSTRAINT child_run_cancellation_receipts_event_fk FOREIGN KEY
        (tenant_id, child_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
        REFERENCES stateknot.run_events (tenant_id, run_id, sequence, event_id, recorded_at, event_digest) ON DELETE RESTRICT
);

CREATE FUNCTION stateknot.capture_child_run_cancellation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lifecycle_status = 'cancellation_requested'
       AND OLD.lifecycle_status <> 'cancellation_requested' THEN
        INSERT INTO stateknot.child_run_cancellations
            (tenant_id, parent_run_id, key_digest, child_run_id, lifecycle_bytes, lifecycle_digest,
             journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
        SELECT NEW.tenant_id, NEW.run_id, owner.key_digest, owner.child_run_id,
               NEW.lifecycle_bytes, sha256(NEW.lifecycle_bytes), NEW.journal_sequence,
               NEW.journal_event_id, NEW.journal_recorded_at, NEW.journal_digest
        FROM stateknot.child_run_ownership AS owner
        WHERE owner.tenant_id = NEW.tenant_id AND owner.parent_run_id = NEW.run_id AND NOT owner.settled;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER runs_child_cancellation_capture AFTER UPDATE ON stateknot.runs
    FOR EACH ROW EXECUTE FUNCTION stateknot.capture_child_run_cancellation();

-- Cancellation cleanup cannot finish before child accounting. Exclude new
-- parent leases until settlement, including older workers/direct claims. Keep
-- existing worker leases renewable so cooperative cleanup is not interrupted.
-- This is a cancellation drain gate, not a successful child Join or timer.
CREATE FUNCTION stateknot.guard_child_cancellation_claim() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lifecycle_status = 'cancellation_requested'
       AND NEW.lease_attempt_id IS NOT NULL
       AND NEW.lease_attempt_id IS DISTINCT FROM OLD.lease_attempt_id
       AND EXISTS (SELECT 1 FROM stateknot.child_run_ownership
                   WHERE tenant_id = NEW.tenant_id AND parent_run_id = NEW.run_id AND NOT settled) THEN
        RAISE EXCEPTION 'child cancellation is still draining' USING ERRCODE = 'SKC06';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER runs_child_cancellation_claim_guard BEFORE UPDATE ON stateknot.runs
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_cancellation_claim();

-- Migration locks Runs before backfill: the witness can be a later audit head,
-- but must still bind the unchanged cancellation request and current lifecycle.
INSERT INTO stateknot.child_run_cancellations
    (tenant_id, parent_run_id, key_digest, child_run_id, lifecycle_bytes, lifecycle_digest,
     journal_sequence, journal_event_id, journal_recorded_at, journal_digest)
SELECT run.tenant_id, run.run_id, owner.key_digest, owner.child_run_id,
       run.lifecycle_bytes, sha256(run.lifecycle_bytes), run.journal_sequence,
       run.journal_event_id, run.journal_recorded_at, run.journal_digest
FROM stateknot.runs AS run
JOIN stateknot.child_run_ownership AS owner
  ON owner.tenant_id = run.tenant_id AND owner.parent_run_id = run.run_id
WHERE run.lifecycle_status = 'cancellation_requested' AND NOT owner.settled;

CREATE FUNCTION stateknot.guard_child_cancellation_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_TABLE_NAME <> 'child_run_cancellations' THEN
        RAISE EXCEPTION 'child cancellation evidence is immutable' USING ERRCODE = 'SKC05';
    END IF;
    IF (to_jsonb(NEW) - 'delivered_at') IS DISTINCT FROM (to_jsonb(OLD) - 'delivered_at')
       OR (OLD.delivered_at IS NOT NULL AND NEW.delivered_at IS DISTINCT FROM OLD.delivered_at)
       OR NEW.delivered_at IS DISTINCT FROM
          (SELECT delivered_at FROM stateknot.child_run_cancellation_receipts
           WHERE tenant_id = NEW.tenant_id AND parent_run_id = NEW.parent_run_id AND key_digest = NEW.key_digest) THEN
        RAISE EXCEPTION 'child cancellation projection mismatch' USING ERRCODE = 'SKC05';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER child_cancellations_immutable BEFORE UPDATE OR DELETE ON stateknot.child_run_cancellations
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_cancellation_evidence();
CREATE TRIGGER child_cancellation_receipts_immutable BEFORE UPDATE OR DELETE ON stateknot.child_run_cancellation_receipts
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_child_cancellation_evidence();
