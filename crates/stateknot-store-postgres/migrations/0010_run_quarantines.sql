-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Quarantine facts live outside the journal they may be reporting as corrupt.
-- Existing quarantined rows are intentionally not backfilled: manufacturing a
-- detector, evidence checksum, or exact journal observation would turn missing
-- audit evidence into a false claim.
CREATE TABLE stateknot.run_quarantines (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    quarantine_id uuid NOT NULL,
    quarantined_at timestamptz(6) NOT NULL,
    cause_kind text NOT NULL,
    component text NOT NULL,
    evidence_digest bytea NOT NULL,
    expected_journal_sequence bigint,
    expected_journal_event_id uuid,
    expected_journal_recorded_at timestamptz(6),
    expected_journal_digest bytea,
    record_digest bytea NOT NULL,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id),
    CONSTRAINT run_quarantines_identity_unique UNIQUE (
        tenant_id,
        quarantine_id
    ),
    CONSTRAINT run_quarantines_run_fk
        FOREIGN KEY (tenant_id, run_id)
        REFERENCES stateknot.runs (tenant_id, run_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_quarantines_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT run_quarantines_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(quarantine_id)
    ),
    CONSTRAINT run_quarantines_cause_valid CHECK (
        cause_kind IN (
            'integrity_failure',
            'unsupported_schema',
            'missing_artifact',
            'cross_tenant_reference',
            'projection_mismatch',
            'fencing_epoch_exhausted',
            'operator_policy'
        )
    ),
    CONSTRAINT run_quarantines_component_valid CHECK (
        octet_length(component) BETWEEN 1 AND 128
        AND component ~ '^[a-z0-9._:-]+$'
    ),
    CONSTRAINT run_quarantines_digest_lengths CHECK (
        octet_length(evidence_digest) = 32
        AND octet_length(record_digest) = 32
        AND (
            expected_journal_digest IS NULL
            OR octet_length(expected_journal_digest) = 32
        )
    ),
    CONSTRAINT run_quarantines_journal_shape CHECK (
        (
            expected_journal_sequence IS NULL
            AND expected_journal_event_id IS NULL
            AND expected_journal_recorded_at IS NULL
            AND expected_journal_digest IS NULL
        )
        OR
        (
            expected_journal_sequence > 0
            AND expected_journal_event_id IS NOT NULL
            AND stateknot.is_uuid_v7(expected_journal_event_id)
            AND expected_journal_recorded_at IS NOT NULL
            AND expected_journal_digest IS NOT NULL
            AND quarantined_at >= expected_journal_recorded_at
        )
    ),
    CONSTRAINT run_quarantines_clock_valid CHECK (
        created_at = quarantined_at
    )
);

CREATE INDEX run_quarantines_observed
    ON stateknot.run_quarantines (
        tenant_id,
        quarantined_at,
        run_id
    );
