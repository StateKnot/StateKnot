-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- An immutable decision, separate from cancellation and from terminal accounting.
LOCK TABLE stateknot.runs IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE stateknot.run_failure_closes (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    intent_bytes bytea NOT NULL CHECK (octet_length(intent_bytes) BETWEEN 1 AND 4194304),
    intent_digest bytea NOT NULL CHECK (intent_digest = sha256(intent_bytes)),
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    completed_at timestamptz(6),
    PRIMARY KEY (tenant_id, run_id),
    FOREIGN KEY (tenant_id,run_id) REFERENCES stateknot.runs (tenant_id,run_id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id,run_id,journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
        REFERENCES stateknot.run_events (tenant_id,run_id,sequence,event_id,recorded_at,event_digest) ON DELETE RESTRICT,
    CHECK (completed_at IS NULL OR (isfinite(completed_at) AND completed_at >= journal_recorded_at))
);
CREATE INDEX run_failure_closes_pending ON stateknot.run_failure_closes
    (tenant_id,journal_recorded_at,run_id) WHERE completed_at IS NULL;

CREATE FUNCTION stateknot.guard_run_failure_close() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE intent jsonb;
BEGIN
    SELECT convert_from(intent_bytes,'UTF8')::jsonb INTO intent
        FROM stateknot.run_failure_closes WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
    IF NOT FOUND THEN RETURN NEW; END IF;
    IF current_setting('stateknot.failure_close_version',true) IS DISTINCT FROM '1' THEN
        RAISE EXCEPTION 'unsupported failure close writer' USING ERRCODE='SKC08';
    END IF;
    IF NEW.lease_attempt_id IS NOT NULL THEN
        RAISE EXCEPTION 'failure close forbids execution leases' USING ERRCODE='SKC06';
    END IF;
    IF NEW.checkpoint_id IS DISTINCT FROM OLD.checkpoint_id
       OR NEW.checkpoint_digest IS DISTINCT FROM OLD.checkpoint_digest
       OR NEW.checkpoint_superstep IS DISTINCT FROM OLD.checkpoint_superstep THEN
        RAISE EXCEPTION 'failure close checkpoint is sealed' USING ERRCODE='SKC09';
    END IF;
    IF OLD.lifecycle_status='failed' AND (NEW.lifecycle_status IS DISTINCT FROM OLD.lifecycle_status
       OR NEW.lifecycle_bytes IS DISTINCT FROM OLD.lifecycle_bytes
       OR NEW.lifecycle_revision IS DISTINCT FROM OLD.lifecycle_revision
       OR NEW.changed_at IS DISTINCT FROM OLD.changed_at) THEN
        RAISE EXCEPTION 'terminal failure is immutable' USING ERRCODE='SKC09';
    END IF;
    IF NEW.lifecycle_status='active' THEN
        IF convert_from(NEW.lifecycle_bytes,'UTF8')::jsonb IS DISTINCT FROM intent->'lifecycle' THEN
            RAISE EXCEPTION 'failure close lifecycle is sealed' USING ERRCODE='SKC09';
        END IF;
    ELSIF NEW.lifecycle_status='failed' THEN
        IF (convert_from(NEW.lifecycle_bytes,'UTF8')::jsonb #> '{state,failure,failure}')
            IS DISTINCT FROM intent->'failure' THEN
            RAISE EXCEPTION 'original failure cannot be replaced' USING ERRCODE='SKC09';
        END IF;
    ELSE
        RAISE EXCEPTION 'failure close owns the terminal decision' USING ERRCODE='SKC09';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER runs_failure_close_guard BEFORE UPDATE ON stateknot.runs
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_run_failure_close();

CREATE FUNCTION stateknot.capture_failure_close_children() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE parent stateknot.runs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT parent FROM stateknot.runs WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id FOR UPDATE;
    IF parent.lifecycle_status<>'active' OR parent.journal_event_id<>NEW.journal_event_id
       OR convert_from(parent.lifecycle_bytes,'UTF8')::jsonb IS DISTINCT FROM
          (convert_from(NEW.intent_bytes,'UTF8')::jsonb)->'lifecycle' THEN
        RAISE EXCEPTION 'invalid failure close boundary' USING ERRCODE='SKC09';
    END IF;
    INSERT INTO stateknot.child_run_cancellations
        (tenant_id,parent_run_id,key_digest,child_run_id,lifecycle_bytes,lifecycle_digest,
         journal_sequence,journal_event_id,journal_recorded_at,journal_digest)
    SELECT NEW.tenant_id,NEW.run_id,o.key_digest,o.child_run_id,parent.lifecycle_bytes,
           sha256(parent.lifecycle_bytes),NEW.journal_sequence,NEW.journal_event_id,
           NEW.journal_recorded_at,NEW.journal_digest
    FROM stateknot.child_run_ownership o
    WHERE o.tenant_id=NEW.tenant_id AND o.parent_run_id=NEW.run_id AND NOT o.settled;
    RETURN NEW;
END
$$;
CREATE TRIGGER failure_closes_capture_children AFTER INSERT ON stateknot.run_failure_closes
    FOR EACH ROW EXECUTE FUNCTION stateknot.capture_failure_close_children();

CREATE FUNCTION stateknot.guard_failure_close_spawn() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM stateknot.run_failure_closes WHERE tenant_id=NEW.tenant_id AND run_id=NEW.parent_run_id) THEN
        RAISE EXCEPTION 'failure close forbids new children' USING ERRCODE='SKC09';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER child_ownership_failure_close_guard BEFORE INSERT ON stateknot.child_run_ownership
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_failure_close_spawn();

CREATE FUNCTION stateknot.guard_failure_close_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'immutable failure close evidence' USING ERRCODE='SKC09'; END IF;
    IF (to_jsonb(NEW)-'completed_at') IS DISTINCT FROM (to_jsonb(OLD)-'completed_at')
       OR (OLD.completed_at IS NOT NULL AND NEW.completed_at IS DISTINCT FROM OLD.completed_at)
       OR NEW.completed_at IS DISTINCT FROM (SELECT changed_at FROM stateknot.runs
          WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND lifecycle_status='failed') THEN
        RAISE EXCEPTION 'invalid failure close completion' USING ERRCODE='SKC09';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER failure_closes_immutable BEFORE UPDATE OR DELETE ON stateknot.run_failure_closes
    FOR EACH ROW EXECUTE FUNCTION stateknot.guard_failure_close_evidence();

CREATE FUNCTION stateknot.complete_run_failure_close() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lifecycle_status='failed' AND OLD.lifecycle_status<>'failed' THEN
        UPDATE stateknot.run_failure_closes SET completed_at=NEW.changed_at
        WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id AND completed_at IS NULL;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER runs_failure_close_complete AFTER UPDATE ON stateknot.runs
    FOR EACH ROW EXECUTE FUNCTION stateknot.complete_run_failure_close();
