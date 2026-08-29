-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Runnable discovery is a durable, indexed projection rather than per-run
-- polling. The value is the database-observed instant at which the run most
-- recently entered the scheduler queue. A live lease delays availability until
-- its exclusive expiry without requiring a timer update at that instant.
ALTER TABLE stateknot.runs
    ADD COLUMN scheduler_ready_at timestamptz(6);

UPDATE stateknot.runs
SET scheduler_ready_at = CASE
    WHEN lifecycle_status IN ('pending', 'active', 'cancellation_requested')
        THEN CASE
            WHEN lease_attempt_id IS NULL THEN updated_at
            ELSE changed_at
        END
    ELSE NULL
END;

ALTER TABLE stateknot.runs
    ADD CONSTRAINT runs_scheduler_ready_shape CHECK (
        (
            lifecycle_status IN ('pending', 'active', 'cancellation_requested')
            AND scheduler_ready_at IS NOT NULL
            AND scheduler_ready_at >= admitted_at
        )
        OR (
            lifecycle_status IN ('waiting', 'succeeded', 'failed', 'cancelled')
            AND scheduler_ready_at IS NULL
        )
    );

-- The scheduler fixes one database timestamp per page chain. This expression
-- orders an unleased run by queue entry and a leased run no earlier than lease
-- expiry. The partial predicate excludes waiting, terminal, and quarantined
-- populations from the hot index.
CREATE INDEX runs_scheduler_ready
    ON stateknot.runs (
        tenant_id,
        (GREATEST(
            scheduler_ready_at,
            COALESCE(lease_expires_at, scheduler_ready_at)
        )),
        run_id
    )
    WHERE quarantined_at IS NULL
      AND scheduler_ready_at IS NOT NULL
      AND lifecycle_status IN ('pending', 'active', 'cancellation_requested');
