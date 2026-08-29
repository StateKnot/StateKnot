-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Migration 8 could persist a compact Waiting lifecycle but had no independent
-- interrupt/timer evidence. Preserve those bytes for audit while quarantining
-- only affected runs; inventing payloads, authority, or journal anchors would
-- make recovery unsafe.
UPDATE stateknot.runs
SET quarantined_at = clock_timestamp(),
    quarantine_reason = 'migration-9: legacy waiting lifecycle has no durable wait records'
WHERE lifecycle_status = 'waiting'
  AND quarantined_at IS NULL;

ALTER TABLE stateknot.runs
    ADD COLUMN wait_set_digest bytea,
    ADD COLUMN unresolved_wait_count smallint NOT NULL DEFAULT 0,
    ADD COLUMN next_timer_due_at timestamptz(6),
    ADD COLUMN next_interrupt_expiry_at timestamptz(6),
    ADD CONSTRAINT runs_wait_projection_shape CHECK (
        (
            lifecycle_status = 'waiting'
            AND (
                (
                    octet_length(wait_set_digest) = 32
                    AND unresolved_wait_count BETWEEN 1 AND 64
                )
                OR (
                    quarantined_at IS NOT NULL
                    AND wait_set_digest IS NULL
                    AND unresolved_wait_count = 0
                    AND next_timer_due_at IS NULL
                    AND next_interrupt_expiry_at IS NULL
                )
            )
        )
        OR (
            lifecycle_status <> 'waiting'
            AND wait_set_digest IS NULL
            AND unresolved_wait_count = 0
            AND next_timer_due_at IS NULL
            AND next_interrupt_expiry_at IS NULL
        )
    );

-- Shared wait identity and mutable current projection. Canonical registration
-- bytes and every terminal fact remain append-only in the detail tables below.
CREATE TABLE stateknot.run_wait_registrations (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    wait_id uuid NOT NULL,
    wait_kind text NOT NULL,
    interrupt_kind text,
    timer_kind text,
    registered_at timestamptz(6) NOT NULL,
    due_at timestamptz(6),
    expires_at timestamptz(6),
    action_digest bytea,
    registration_sequence bigint NOT NULL,
    registration_event_id uuid NOT NULL,
    registration_event_digest bytea NOT NULL,
    intent_digest bytea NOT NULL,
    record_digest bytea NOT NULL,
    record_bytes bytea NOT NULL,
    record_byte_length integer GENERATED ALWAYS AS (octet_length(record_bytes)) STORED,
    status text NOT NULL,
    terminal_sequence bigint,
    terminal_event_id uuid,
    terminal_recorded_at timestamptz(6),
    terminal_event_digest bytea,
    resolution_digest bytea,
    firing_digest bytea,
    abandonment_digest bytea,
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, wait_id),
    CONSTRAINT run_wait_registrations_tenant_identity_unique UNIQUE (
        tenant_id,
        wait_id
    ),
    CONSTRAINT run_wait_registrations_exact_record_unique UNIQUE (
        tenant_id,
        run_id,
        wait_id,
        wait_kind,
        record_digest
    ),
    CONSTRAINT run_wait_registrations_run_fk
        FOREIGN KEY (tenant_id, run_id)
        REFERENCES stateknot.runs (tenant_id, run_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_wait_registrations_origin_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            registration_sequence,
            registration_event_id,
            registered_at,
            registration_event_digest
        )
        REFERENCES stateknot.run_events (
            tenant_id,
            run_id,
            sequence,
            event_id,
            recorded_at,
            event_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT run_wait_registrations_terminal_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            terminal_sequence,
            terminal_event_id,
            terminal_recorded_at,
            terminal_event_digest
        )
        REFERENCES stateknot.run_events (
            tenant_id,
            run_id,
            sequence,
            event_id,
            recorded_at,
            event_digest
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT run_wait_registrations_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT run_wait_registrations_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(wait_id)
        AND stateknot.is_uuid_v7(registration_event_id)
        AND (terminal_event_id IS NULL OR stateknot.is_uuid_v7(terminal_event_id))
    ),
    CONSTRAINT run_wait_registrations_digest_lengths CHECK (
        octet_length(registration_event_digest) = 32
        AND octet_length(intent_digest) = 32
        AND octet_length(record_digest) = 32
        AND (action_digest IS NULL OR octet_length(action_digest) = 32)
        AND (terminal_event_digest IS NULL OR octet_length(terminal_event_digest) = 32)
        AND (resolution_digest IS NULL OR octet_length(resolution_digest) = 32)
        AND (firing_digest IS NULL OR octet_length(firing_digest) = 32)
        AND (abandonment_digest IS NULL OR octet_length(abandonment_digest) = 32)
    ),
    CONSTRAINT run_wait_registrations_bytes_bounded CHECK (
        record_byte_length BETWEEN 1 AND 4194304
    ),
    CONSTRAINT run_wait_registrations_kind_shape CHECK (
        (
            wait_kind = 'interrupt'
            AND interrupt_kind IN (
                'approval',
                'input',
                'authentication',
                'external_signal',
                'reconciliation'
            )
            AND timer_kind IS NULL
            AND due_at IS NULL
            AND octet_length(action_digest) = 32
            AND (expires_at IS NULL OR expires_at > registered_at)
        )
        OR (
            wait_kind = 'timer'
            AND interrupt_kind IS NULL
            AND timer_kind IN ('sleep', 'retry_backoff')
            AND due_at > registered_at
            AND expires_at IS NULL
            AND action_digest IS NULL
        )
    ),
    CONSTRAINT run_wait_registrations_terminal_shape CHECK (
        (
            status = 'outstanding'
            AND terminal_sequence IS NULL
            AND terminal_event_id IS NULL
            AND terminal_recorded_at IS NULL
            AND terminal_event_digest IS NULL
            AND resolution_digest IS NULL
            AND firing_digest IS NULL
            AND abandonment_digest IS NULL
        )
        OR (
            status = 'resolved'
            AND wait_kind = 'interrupt'
            AND terminal_sequence > registration_sequence
            AND terminal_recorded_at >= registered_at
            AND (expires_at IS NULL OR terminal_recorded_at < expires_at)
            AND terminal_event_id IS NOT NULL
            AND octet_length(terminal_event_digest) = 32
            AND octet_length(resolution_digest) = 32
            AND firing_digest IS NULL
            AND abandonment_digest IS NULL
        )
        OR (
            status = 'fired'
            AND wait_kind = 'timer'
            AND terminal_sequence > registration_sequence
            AND terminal_recorded_at >= due_at
            AND terminal_event_id IS NOT NULL
            AND octet_length(terminal_event_digest) = 32
            AND resolution_digest IS NULL
            AND octet_length(firing_digest) = 32
            AND abandonment_digest IS NULL
        )
        OR (
            status = 'abandoned'
            AND terminal_sequence > registration_sequence
            AND terminal_recorded_at >= registered_at
            AND terminal_event_id IS NOT NULL
            AND octet_length(terminal_event_digest) = 32
            AND resolution_digest IS NULL
            AND firing_digest IS NULL
            AND octet_length(abandonment_digest) = 32
        )
    ),
    CONSTRAINT run_wait_registrations_clock_valid CHECK (
        created_at = registered_at
        AND updated_at >= created_at
        AND (
            terminal_recorded_at IS NULL
            OR updated_at = terminal_recorded_at
        )
    )
);

CREATE TABLE stateknot.interrupt_resolutions (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    interrupt_id uuid NOT NULL,
    wait_kind text GENERATED ALWAYS AS ('interrupt'::text) STORED,
    request_digest bytea NOT NULL,
    resolution_sequence bigint NOT NULL,
    resolution_event_id uuid NOT NULL,
    resolved_at timestamptz(6) NOT NULL,
    resolution_event_digest bytea NOT NULL,
    intent_digest bytea NOT NULL,
    resolution_digest bytea NOT NULL,
    resolution_bytes bytea NOT NULL,
    resolution_byte_length integer GENERATED ALWAYS AS (octet_length(resolution_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, interrupt_id),
    CONSTRAINT interrupt_resolutions_exact_unique UNIQUE (
        tenant_id,
        run_id,
        interrupt_id,
        resolution_digest
    ),
    CONSTRAINT interrupt_resolutions_request_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            interrupt_id,
            wait_kind,
            request_digest
        )
        REFERENCES stateknot.run_wait_registrations (
            tenant_id,
            run_id,
            wait_id,
            wait_kind,
            record_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT interrupt_resolutions_event_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            resolution_sequence,
            resolution_event_id,
            resolved_at,
            resolution_event_digest
        )
        REFERENCES stateknot.run_events (
            tenant_id,
            run_id,
            sequence,
            event_id,
            recorded_at,
            event_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT interrupt_resolutions_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT interrupt_resolutions_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(interrupt_id)
        AND stateknot.is_uuid_v7(resolution_event_id)
    ),
    CONSTRAINT interrupt_resolutions_digest_lengths CHECK (
        octet_length(request_digest) = 32
        AND octet_length(resolution_event_digest) = 32
        AND octet_length(intent_digest) = 32
        AND octet_length(resolution_digest) = 32
    ),
    CONSTRAINT interrupt_resolutions_bytes_bounded CHECK (
        resolution_byte_length BETWEEN 1 AND 4194304
    ),
    CONSTRAINT interrupt_resolutions_clock_valid CHECK (
        created_at = resolved_at
    )
);

CREATE TABLE stateknot.timer_firings (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    timer_id uuid NOT NULL,
    wait_kind text GENERATED ALWAYS AS ('timer'::text) STORED,
    timer_digest bytea NOT NULL,
    firing_sequence bigint NOT NULL,
    firing_event_id uuid NOT NULL,
    fired_at timestamptz(6) NOT NULL,
    firing_event_digest bytea NOT NULL,
    intent_digest bytea NOT NULL,
    firing_digest bytea NOT NULL,
    firing_bytes bytea NOT NULL,
    firing_byte_length integer GENERATED ALWAYS AS (octet_length(firing_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, timer_id),
    CONSTRAINT timer_firings_exact_unique UNIQUE (
        tenant_id,
        run_id,
        timer_id,
        firing_digest
    ),
    CONSTRAINT timer_firings_timer_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            timer_id,
            wait_kind,
            timer_digest
        )
        REFERENCES stateknot.run_wait_registrations (
            tenant_id,
            run_id,
            wait_id,
            wait_kind,
            record_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT timer_firings_event_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            firing_sequence,
            firing_event_id,
            fired_at,
            firing_event_digest
        )
        REFERENCES stateknot.run_events (
            tenant_id,
            run_id,
            sequence,
            event_id,
            recorded_at,
            event_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT timer_firings_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT timer_firings_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(timer_id)
        AND stateknot.is_uuid_v7(firing_event_id)
    ),
    CONSTRAINT timer_firings_digest_lengths CHECK (
        octet_length(timer_digest) = 32
        AND octet_length(firing_event_digest) = 32
        AND octet_length(intent_digest) = 32
        AND octet_length(firing_digest) = 32
    ),
    CONSTRAINT timer_firings_bytes_bounded CHECK (
        firing_byte_length BETWEEN 1 AND 1048576
    ),
    CONSTRAINT timer_firings_clock_valid CHECK (created_at = fired_at)
);

CREATE TABLE stateknot.wait_abandonments (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    wait_id uuid NOT NULL,
    wait_kind text NOT NULL,
    registration_digest bytea NOT NULL,
    reason_kind text NOT NULL,
    abandonment_sequence bigint NOT NULL,
    abandonment_event_id uuid NOT NULL,
    abandoned_at timestamptz(6) NOT NULL,
    abandonment_event_digest bytea NOT NULL,
    abandonment_digest bytea NOT NULL,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, wait_id),
    CONSTRAINT wait_abandonments_exact_unique UNIQUE (
        tenant_id,
        run_id,
        wait_id,
        abandonment_digest
    ),
    CONSTRAINT wait_abandonments_registration_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            wait_id,
            wait_kind,
            registration_digest
        )
        REFERENCES stateknot.run_wait_registrations (
            tenant_id,
            run_id,
            wait_id,
            wait_kind,
            record_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT wait_abandonments_event_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            abandonment_sequence,
            abandonment_event_id,
            abandoned_at,
            abandonment_event_digest
        )
        REFERENCES stateknot.run_events (
            tenant_id,
            run_id,
            sequence,
            event_id,
            recorded_at,
            event_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT wait_abandonments_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT wait_abandonments_kind_valid CHECK (
        wait_kind IN ('interrupt', 'timer')
        AND reason_kind IN ('run_cancellation', 'run_failure')
    ),
    CONSTRAINT wait_abandonments_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(wait_id)
        AND stateknot.is_uuid_v7(abandonment_event_id)
    ),
    CONSTRAINT wait_abandonments_digest_lengths CHECK (
        octet_length(registration_digest) = 32
        AND octet_length(abandonment_event_digest) = 32
        AND octet_length(abandonment_digest) = 32
    ),
    CONSTRAINT wait_abandonments_clock_valid CHECK (created_at = abandoned_at)
);

ALTER TABLE stateknot.run_wait_registrations
    ADD CONSTRAINT run_wait_registrations_resolution_fk
        FOREIGN KEY (tenant_id, run_id, wait_id, resolution_digest)
        REFERENCES stateknot.interrupt_resolutions (
            tenant_id,
            run_id,
            interrupt_id,
            resolution_digest
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT run_wait_registrations_firing_fk
        FOREIGN KEY (tenant_id, run_id, wait_id, firing_digest)
        REFERENCES stateknot.timer_firings (
            tenant_id,
            run_id,
            timer_id,
            firing_digest
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT run_wait_registrations_abandonment_fk
        FOREIGN KEY (tenant_id, run_id, wait_id, abandonment_digest)
        REFERENCES stateknot.wait_abandonments (
            tenant_id,
            run_id,
            wait_id,
            abandonment_digest
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX run_wait_registrations_due
    ON stateknot.run_wait_registrations (tenant_id, due_at, run_id, wait_id)
    WHERE wait_kind = 'timer' AND status = 'outstanding';

CREATE INDEX run_wait_registrations_expiry
    ON stateknot.run_wait_registrations (
        tenant_id,
        expires_at,
        run_id,
        wait_id
    )
    WHERE wait_kind = 'interrupt'
      AND status = 'outstanding'
      AND expires_at IS NOT NULL;

CREATE INDEX run_wait_registrations_origin
    ON stateknot.run_wait_registrations (
        tenant_id,
        run_id,
        registration_sequence,
        wait_id
    );
