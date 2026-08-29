-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

CREATE SCHEMA stateknot;

CREATE FUNCTION stateknot.is_uuid_v7(value uuid)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT (get_byte(uuid_send(value), 6) >> 4) = 7
       AND (get_byte(uuid_send(value), 8) & 192) = 128
$$;

CREATE TABLE stateknot.runs (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    thread_id uuid NOT NULL,
    invocation_id uuid NOT NULL,
    lifecycle_bytes bytea NOT NULL,
    lifecycle_revision numeric(20, 0) NOT NULL,
    lifecycle_status text NOT NULL,
    admitted_at timestamptz(6) NOT NULL,
    changed_at timestamptz(6) NOT NULL,
    journal_sequence bigint,
    journal_event_id uuid,
    journal_recorded_at timestamptz(6),
    journal_digest bytea,
    fencing_epoch bigint NOT NULL DEFAULT 0,
    lease_attempt_id uuid,
    lease_acquired_at timestamptz(6),
    lease_renewed_at timestamptz(6),
    lease_expires_at timestamptz(6),
    quarantined_at timestamptz(6),
    quarantine_reason text,
    created_at timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, run_id),
    CONSTRAINT runs_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT runs_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(thread_id)
        AND stateknot.is_uuid_v7(invocation_id)
    ),
    CONSTRAINT runs_lifecycle_bytes_bounded CHECK (
        octet_length(lifecycle_bytes) BETWEEN 1 AND 2097152
    ),
    CONSTRAINT runs_lifecycle_revision_valid CHECK (
        lifecycle_revision BETWEEN 0 AND 18446744073709551615
        AND scale(lifecycle_revision) = 0
    ),
    CONSTRAINT runs_lifecycle_status_valid CHECK (
        lifecycle_status IN (
            'pending',
            'active',
            'waiting',
            'cancellation_requested',
            'succeeded',
            'failed',
            'cancelled'
        )
    ),
    CONSTRAINT runs_changed_at_valid CHECK (changed_at >= admitted_at),
    CONSTRAINT runs_journal_head_shape CHECK (
        (
            journal_sequence IS NULL
            AND journal_event_id IS NULL
            AND journal_recorded_at IS NULL
            AND journal_digest IS NULL
        )
        OR
        (
            journal_sequence > 0
            AND journal_event_id IS NOT NULL
            AND journal_recorded_at IS NOT NULL
            AND journal_digest IS NOT NULL
            AND octet_length(journal_digest) = 32
            AND stateknot.is_uuid_v7(journal_event_id)
        )
    ),
    CONSTRAINT runs_fencing_epoch_valid CHECK (fencing_epoch >= 0),
    CONSTRAINT runs_lease_shape CHECK (
        (
            lease_attempt_id IS NULL
            AND lease_acquired_at IS NULL
            AND lease_renewed_at IS NULL
            AND lease_expires_at IS NULL
        )
        OR
        (
            lease_attempt_id IS NOT NULL
            AND stateknot.is_uuid_v7(lease_attempt_id)
            AND fencing_epoch > 0
            AND lease_acquired_at IS NOT NULL
            AND lease_renewed_at IS NOT NULL
            AND lease_expires_at IS NOT NULL
            AND lease_renewed_at >= lease_acquired_at
            AND lease_expires_at > lease_renewed_at
        )
    ),
    CONSTRAINT runs_quarantine_shape CHECK (
        (quarantined_at IS NULL AND quarantine_reason IS NULL)
        OR
        (
            quarantined_at IS NOT NULL
            AND quarantine_reason IS NOT NULL
            AND octet_length(quarantine_reason) BETWEEN 1 AND 1024
        )
    )
);

CREATE TABLE stateknot.run_events (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    sequence bigint NOT NULL,
    event_id uuid NOT NULL,
    recorded_at timestamptz(6) NOT NULL,
    source_kind text NOT NULL,
    worker_attempt_id uuid,
    worker_epoch bigint,
    event_kind text NOT NULL,
    schema_id text NOT NULL,
    schema_version text NOT NULL,
    schema_digest bytea NOT NULL,
    payload_bytes bytea NOT NULL,
    payload_byte_length integer GENERATED ALWAYS AS (octet_length(payload_bytes)) STORED,
    payload_digest bytea NOT NULL,
    intent_digest bytea NOT NULL,
    previous_digest bytea,
    event_digest bytea NOT NULL,
    PRIMARY KEY (tenant_id, run_id, sequence),
    CONSTRAINT run_events_run_fk
        FOREIGN KEY (tenant_id, run_id)
        REFERENCES stateknot.runs (tenant_id, run_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_events_event_id_unique UNIQUE (tenant_id, run_id, event_id),
    CONSTRAINT run_events_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT run_events_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id) AND stateknot.is_uuid_v7(event_id)
    ),
    CONSTRAINT run_events_sequence_positive CHECK (sequence > 0),
    CONSTRAINT run_events_source_shape CHECK (
        (
            source_kind = 'control_plane'
            AND worker_attempt_id IS NULL
            AND worker_epoch IS NULL
        )
        OR
        (
            source_kind = 'worker'
            AND worker_attempt_id IS NOT NULL
            AND stateknot.is_uuid_v7(worker_attempt_id)
            AND worker_epoch > 0
        )
    ),
    CONSTRAINT run_events_kind_valid CHECK (
        octet_length(event_kind) BETWEEN 1 AND 96
        AND event_kind ~ '^[a-z][a-z0-9]*(-[a-z0-9]+)*$'
    ),
    CONSTRAINT run_events_schema_id_valid CHECK (
        octet_length(schema_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT run_events_schema_version_valid CHECK (
        octet_length(schema_version) BETWEEN 5 AND 62
        AND schema_version ~ '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
    ),
    CONSTRAINT run_events_payload_bounded CHECK (
        payload_byte_length BETWEEN 1 AND 2097152
    ),
    CONSTRAINT run_events_digest_lengths CHECK (
        octet_length(schema_digest) = 32
        AND octet_length(payload_digest) = 32
        AND octet_length(intent_digest) = 32
        AND (previous_digest IS NULL OR octet_length(previous_digest) = 32)
        AND octet_length(event_digest) = 32
    ),
    CONSTRAINT run_events_chain_shape CHECK (
        (sequence = 1 AND previous_digest IS NULL)
        OR (sequence > 1 AND previous_digest IS NOT NULL)
    )
);

CREATE INDEX runs_lease_expiry
    ON stateknot.runs (lease_expires_at)
    WHERE lease_attempt_id IS NOT NULL;

CREATE INDEX runs_quarantined
    ON stateknot.runs (quarantined_at)
    WHERE quarantined_at IS NOT NULL;
