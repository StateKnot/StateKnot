-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Queue age and execution eligibility are separate facts. scheduler_ready_at
-- preserves when a runnable run entered the tenant queue for fairness, while
-- scheduler_not_before suppresses useless claims until the earliest verified
-- node retry can start. A NULL gate means immediately eligible; a non-NULL
-- gate owns the run only after the scheduling transaction releases its lease.
ALTER TABLE stateknot.runs
    ADD COLUMN scheduler_not_before timestamptz(6),
    ADD CONSTRAINT runs_scheduler_not_before_shape CHECK (
        scheduler_not_before IS NULL
        OR (
            lifecycle_status IN ('pending', 'active', 'cancellation_requested')
            AND scheduler_ready_at IS NOT NULL
            AND scheduler_not_before >= scheduler_ready_at
            AND lease_attempt_id IS NULL
        )
    );

-- Replace the v7 index without changing its stable object name. Discovery is
-- ordered by the first instant at which queue admission, retry delay, and lease
-- ownership all permit a claim. No per-run timer or polling update is needed.
DROP INDEX stateknot.runs_scheduler_ready;

CREATE INDEX runs_scheduler_ready
    ON stateknot.runs (
        tenant_id,
        (GREATEST(
            scheduler_ready_at,
            COALESCE(scheduler_not_before, scheduler_ready_at),
            COALESCE(lease_expires_at, scheduler_ready_at)
        )),
        run_id
    )
    WHERE quarantined_at IS NULL
      AND scheduler_ready_at IS NOT NULL
      AND lifecycle_status IN ('pending', 'active', 'cancellation_requested');
