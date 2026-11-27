-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE stateknot.run_events
    ADD CONSTRAINT run_events_sequence_projection_unique
        UNIQUE (tenant_id, run_id, sequence, projection_digest);

-- An ordinary failed completion has exactly one event projection. Removing its
-- receipt switches the calculated projection to the legacy digest and fails
-- the same foreign key. Framework closures own a separate completion table.
ALTER TABLE stateknot.node_attempt_completions
    ADD COLUMN tool_charges bytea,
    ADD COLUMN failure_projection_digest bytea GENERATED ALWAYS AS (
        CASE WHEN status = 'failed' THEN
            CASE WHEN tool_charges IS NULL THEN completion_digest
            ELSE sha256(
                '\x73746174656b6e6f742e6e6f64652d6661696c7572652d746f6f6c2d636861726765732e763100'::bytea
                || completion_digest || tool_charges
            ) END
        END
    ) STORED,
    ADD CONSTRAINT node_attempt_tool_charges_bounded CHECK (
        tool_charges IS NULL OR
        (status = 'failed' AND octet_length(tool_charges) BETWEEN 2 AND 1048576)
    ),
    ADD CONSTRAINT node_attempt_failure_projection_fk
        FOREIGN KEY (tenant_id, run_id, journal_sequence, failure_projection_digest)
        REFERENCES stateknot.run_events (tenant_id, run_id, sequence, projection_digest)
        ON DELETE RESTRICT;

CREATE INDEX node_attempts_charged_activation
    ON stateknot.node_attempt_completions (tenant_id, run_id, activation_digest)
    WHERE tool_charges IS NOT NULL;
