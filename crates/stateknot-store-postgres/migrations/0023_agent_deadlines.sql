-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- A rebuildable, immutable projection of the admitted finite budget. Keep it
-- on runs so the partial index removes cancelled/terminal work automatically,
-- including writes by already-connected v22 binaries. No timer or fake lease.
ALTER TABLE stateknot.runs ADD COLUMN agent_deadline_at timestamptz(6);
ALTER TABLE stateknot.runs ADD CONSTRAINT runs_agent_deadline_finite
    CHECK (agent_deadline_at IS NULL OR isfinite(agent_deadline_at));

CREATE FUNCTION stateknot.guard_agent_deadline() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE expected timestamptz(6);
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.agent_deadline_at IS NOT DISTINCT FROM OLD.agent_deadline_at THEN
            RETURN NEW;
        END IF;
        IF OLD.agent_deadline_at IS NOT NULL THEN
            RAISE EXCEPTION 'immutable Agent deadline' USING ERRCODE='23514';
        END IF;
    END IF;
    SELECT (convert_from(admission_bytes,'UTF8')::jsonb #>> '{intent,budget,deadline}')::timestamptz
        INTO expected FROM stateknot.agent_admissions
        WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
    IF NEW.agent_deadline_at IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'Agent deadline admission mismatch' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER runs_agent_deadline_guard
BEFORE INSERT OR UPDATE ON stateknot.runs
FOR EACH ROW EXECUTE FUNCTION stateknot.guard_agent_deadline();

CREATE FUNCTION stateknot.capture_agent_deadline() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE deadline timestamptz(6);
BEGIN
    deadline := (convert_from(NEW.admission_bytes,'UTF8')::jsonb #>> '{intent,budget,deadline}')::timestamptz;
    IF deadline IS NULL OR NOT isfinite(deadline) THEN
        RAISE EXCEPTION 'missing finite Agent deadline' USING ERRCODE='23514';
    END IF;
    UPDATE stateknot.runs SET agent_deadline_at=deadline
        WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'missing Agent deadline run' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER agent_admissions_deadline_capture
AFTER INSERT ON stateknot.agent_admissions
FOR EACH ROW EXECUTE FUNCTION stateknot.capture_agent_deadline();

-- Migration owns compatible writes while backfilling existing child/Join runs.
SET LOCAL stateknot.child_runtime_version = '1';
SET LOCAL stateknot.child_join_version = '1';
UPDATE stateknot.runs AS r SET agent_deadline_at=
    (convert_from(a.admission_bytes,'UTF8')::jsonb #>> '{intent,budget,deadline}')::timestamptz
FROM stateknot.agent_admissions AS a
WHERE r.tenant_id=a.tenant_id AND r.run_id=a.run_id;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM stateknot.agent_admissions a JOIN stateknot.runs r
        USING (tenant_id,run_id) WHERE r.agent_deadline_at IS NULL) THEN
        RAISE EXCEPTION 'missing backfilled Agent deadline' USING ERRCODE='23514';
    END IF;
END $$;

-- Include quarantined entries: the worker reports them and continues the scan,
-- rather than silently hiding expired work from operational supervision.
CREATE INDEX runs_due_agent_deadlines ON stateknot.runs
    (tenant_id,agent_deadline_at,run_id)
    WHERE agent_deadline_at IS NOT NULL
      AND lifecycle_status IN ('pending','active','waiting');
