-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Recovery workers must not quarantine a successor merely because the journal
-- head did not change during lease takeover. New recovery-originated evidence
-- therefore records the exact attempt and fencing epoch that must still own an
-- unexpired lease in the quarantine transaction. Existing control-plane and
-- migration-10 records remain deliberately unfenced and retain their v1 digest.
ALTER TABLE stateknot.run_quarantines
    ADD COLUMN expected_fence_attempt_id uuid,
    ADD COLUMN expected_fence_epoch bigint,
    ADD CONSTRAINT run_quarantines_fence_shape CHECK (
        (
            expected_fence_attempt_id IS NULL
            AND expected_fence_epoch IS NULL
        )
        OR
        (
            expected_fence_attempt_id IS NOT NULL
            AND stateknot.is_uuid_v7(expected_fence_attempt_id)
            AND expected_fence_epoch > 0
        )
    );
