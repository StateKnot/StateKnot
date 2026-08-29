-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Immutable, versioned destination snapshots. Routing configuration is a
-- schema-pinned canonical envelope. It may contain vault handles but never raw
-- credentials; dispatch resolves credentials outside the database record.
CREATE TABLE stateknot.outbox_destinations (
    tenant_id text NOT NULL,
    destination_id uuid NOT NULL,
    snapshot_digest bytea NOT NULL,
    config_kind text NOT NULL,
    schema_id text NOT NULL,
    schema_version text NOT NULL,
    schema_digest bytea NOT NULL,
    config_bytes bytea NOT NULL,
    config_byte_length integer GENERATED ALWAYS AS (octet_length(config_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, destination_id, snapshot_digest),
    CONSTRAINT outbox_destinations_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT outbox_destinations_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(destination_id)
    ),
    CONSTRAINT outbox_destinations_digest_lengths CHECK (
        octet_length(snapshot_digest) = 32
        AND octet_length(schema_digest) = 32
    ),
    CONSTRAINT outbox_destinations_kind_valid CHECK (
        octet_length(config_kind) BETWEEN 1 AND 96
        AND config_kind ~ '^[a-z][a-z0-9]*(-[a-z0-9]+)*$'
    ),
    CONSTRAINT outbox_destinations_config_bytes_bounded CHECK (
        config_byte_length BETWEEN 1 AND 2097152
    )
);

-- One immutable delivery is atomically anchored to an existing journal event.
-- next_attempt_at is the only queue key: initial origin time, live attempt
-- expiry, or durable safe-after time. Terminal rows clear it.
CREATE TABLE stateknot.outbox_deliveries (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    origin_sequence bigint NOT NULL,
    origin_event_id uuid NOT NULL,
    origin_recorded_at timestamptz(6) NOT NULL,
    origin_digest bytea NOT NULL,
    destination_id uuid NOT NULL,
    destination_snapshot_digest bytea NOT NULL,
    intent_digest bytea NOT NULL,
    expires_at timestamptz(6) NOT NULL,
    delivery_digest bytea NOT NULL,
    delivery_bytes bytea NOT NULL,
    delivery_byte_length integer GENERATED ALWAYS AS (octet_length(delivery_bytes)) STORED,
    status text NOT NULL,
    attempt_count bigint NOT NULL,
    current_attempt_id uuid,
    current_epoch bigint,
    current_attempt_started_at timestamptz(6),
    current_attempt_expires_at timestamptz(6),
    next_attempt_at timestamptz(6),
    last_completion_digest bytea,
    terminal_at timestamptz(6),
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, delivery_id),
    CONSTRAINT outbox_deliveries_exact_identity_unique UNIQUE (
        tenant_id,
        run_id,
        delivery_id,
        expires_at,
        delivery_digest
    ),
    CONSTRAINT outbox_deliveries_run_fk
        FOREIGN KEY (tenant_id, run_id)
        REFERENCES stateknot.runs (tenant_id, run_id)
        ON DELETE RESTRICT,
    CONSTRAINT outbox_deliveries_origin_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            origin_sequence,
            origin_event_id,
            origin_recorded_at,
            origin_digest
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
    CONSTRAINT outbox_deliveries_destination_fk
        FOREIGN KEY (tenant_id, destination_id, destination_snapshot_digest)
        REFERENCES stateknot.outbox_destinations (
            tenant_id,
            destination_id,
            snapshot_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT outbox_deliveries_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT outbox_deliveries_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(delivery_id)
        AND stateknot.is_uuid_v7(origin_event_id)
        AND stateknot.is_uuid_v7(destination_id)
        AND (current_attempt_id IS NULL OR stateknot.is_uuid_v7(current_attempt_id))
    ),
    CONSTRAINT outbox_deliveries_digest_lengths CHECK (
        octet_length(origin_digest) = 32
        AND octet_length(destination_snapshot_digest) = 32
        AND octet_length(intent_digest) = 32
        AND octet_length(delivery_digest) = 32
        AND (last_completion_digest IS NULL OR octet_length(last_completion_digest) = 32)
    ),
    CONSTRAINT outbox_deliveries_bytes_bounded CHECK (
        delivery_byte_length BETWEEN 1 AND 4194304
    ),
    CONSTRAINT outbox_deliveries_position_valid CHECK (
        origin_sequence > 0
        AND attempt_count BETWEEN 0 AND 64
        AND (current_epoch IS NULL OR current_epoch BETWEEN 1 AND 64)
        AND (current_epoch IS NULL OR current_epoch = attempt_count)
    ),
    CONSTRAINT outbox_deliveries_clock_valid CHECK (
        expires_at > origin_recorded_at
        AND created_at = origin_recorded_at
        AND updated_at >= created_at
        AND (terminal_at IS NULL OR terminal_at >= created_at)
    ),
    CONSTRAINT outbox_deliveries_status_valid CHECK (
        status IN (
            'pending',
            'delivering',
            'retry_scheduled',
            'acknowledged',
            'dead_letter',
            'expired'
        )
    ),
    CONSTRAINT outbox_deliveries_projection_shape CHECK (
        (
            status = 'pending'
            AND attempt_count = 0
            AND current_attempt_id IS NULL
            AND current_epoch IS NULL
            AND current_attempt_started_at IS NULL
            AND current_attempt_expires_at IS NULL
            AND next_attempt_at = origin_recorded_at
            AND last_completion_digest IS NULL
            AND terminal_at IS NULL
        )
        OR (
            status = 'delivering'
            AND attempt_count > 0
            AND current_attempt_id IS NOT NULL
            AND current_epoch IS NOT NULL
            AND current_attempt_started_at IS NOT NULL
            AND current_attempt_expires_at > current_attempt_started_at
            AND next_attempt_at = current_attempt_expires_at
            AND last_completion_digest IS NULL
            AND terminal_at IS NULL
        )
        OR (
            status = 'retry_scheduled'
            AND attempt_count > 0
            AND current_attempt_id IS NOT NULL
            AND current_epoch IS NOT NULL
            AND current_attempt_started_at IS NOT NULL
            AND current_attempt_expires_at > current_attempt_started_at
            AND next_attempt_at IS NOT NULL
            AND last_completion_digest IS NOT NULL
            AND terminal_at IS NULL
        )
        OR (
            status = 'acknowledged'
            AND attempt_count > 0
            AND current_attempt_id IS NOT NULL
            AND current_epoch IS NOT NULL
            AND current_attempt_started_at IS NOT NULL
            AND current_attempt_expires_at > current_attempt_started_at
            AND next_attempt_at IS NULL
            AND last_completion_digest IS NOT NULL
            AND terminal_at IS NOT NULL
            AND updated_at = terminal_at
        )
        OR (
            status = 'dead_letter'
            AND attempt_count > 0
            AND current_attempt_id IS NOT NULL
            AND current_epoch IS NOT NULL
            AND current_attempt_started_at IS NOT NULL
            AND current_attempt_expires_at > current_attempt_started_at
            AND next_attempt_at IS NULL
            AND terminal_at IS NOT NULL
            AND updated_at = terminal_at
            AND (
                last_completion_digest IS NOT NULL
                OR (
                    attempt_count = 64
                    AND last_completion_digest IS NULL
                    AND terminal_at = current_attempt_expires_at
                    AND current_attempt_expires_at < expires_at
                )
            )
        )
        OR (
            status = 'expired'
            AND next_attempt_at IS NULL
            AND terminal_at = expires_at
            AND updated_at = expires_at
        )
    )
);

-- Outbox dispatch uses the same run-wide physical AttemptId namespace as node,
-- tool, and model execution. The old anchor uniqueness remains exact for those
-- ledgers; outbox attempts intentionally share their delivery's origin event.
ALTER TABLE stateknot.run_attempt_claims
    DROP CONSTRAINT run_attempt_claims_anchor_unique,
    DROP CONSTRAINT run_attempt_claims_ids_are_uuid_v7,
    DROP CONSTRAINT run_attempt_claims_kind_valid,
    DROP CONSTRAINT run_attempt_claims_owner_shape,
    DROP CONSTRAINT run_attempt_claims_clock_valid,
    ADD COLUMN delivery_id uuid,
    ADD COLUMN delivery_epoch bigint;

CREATE UNIQUE INDEX run_attempt_claims_non_outbox_anchor_unique
    ON stateknot.run_attempt_claims (tenant_id, run_id, journal_sequence)
    WHERE claim_kind <> 'outbox_attempt';

ALTER TABLE stateknot.run_attempt_claims
    ADD CONSTRAINT run_attempt_claims_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(attempt_id)
        AND (invocation_id IS NULL OR stateknot.is_uuid_v7(invocation_id))
        AND (delivery_id IS NULL OR stateknot.is_uuid_v7(delivery_id))
        AND stateknot.is_uuid_v7(journal_event_id)
    ),
    ADD CONSTRAINT run_attempt_claims_kind_valid CHECK (
        claim_kind IN (
            'tool_invocation',
            'model_invocation',
            'node_attempt',
            'outbox_attempt'
        )
    ),
    ADD CONSTRAINT run_attempt_claims_owner_shape CHECK (
        (
            claim_kind IN ('tool_invocation', 'model_invocation')
            AND invocation_id IS NOT NULL
            AND invocation_revision > 0
            AND activation_digest IS NULL
            AND delivery_id IS NULL
            AND delivery_epoch IS NULL
        )
        OR (
            claim_kind = 'node_attempt'
            AND invocation_id IS NULL
            AND invocation_revision IS NULL
            AND octet_length(activation_digest) = 32
            AND delivery_id IS NULL
            AND delivery_epoch IS NULL
        )
        OR (
            claim_kind = 'outbox_attempt'
            AND invocation_id IS NULL
            AND invocation_revision IS NULL
            AND activation_digest IS NULL
            AND delivery_id IS NOT NULL
            AND delivery_epoch BETWEEN 1 AND 64
        )
    ),
    ADD CONSTRAINT run_attempt_claims_clock_valid CHECK (
        (
            claim_kind <> 'outbox_attempt'
            AND claimed_at = journal_recorded_at
        )
        OR (
            claim_kind = 'outbox_attempt'
            AND claimed_at >= journal_recorded_at
        )
    ),
    ADD CONSTRAINT run_attempt_claims_outbox_exact_unique UNIQUE (
        tenant_id,
        run_id,
        claim_kind,
        delivery_id,
        delivery_epoch,
        attempt_id
    );

CREATE TABLE stateknot.outbox_attempts (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    delivery_expires_at timestamptz(6) NOT NULL,
    delivery_digest bytea NOT NULL,
    epoch bigint NOT NULL,
    attempt_id uuid NOT NULL,
    claim_kind text GENERATED ALWAYS AS ('outbox_attempt'::text) STORED,
    started_at timestamptz(6) NOT NULL,
    expires_at timestamptz(6) NOT NULL,
    start_digest bytea NOT NULL,
    start_bytes bytea NOT NULL,
    start_byte_length integer GENERATED ALWAYS AS (octet_length(start_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, delivery_id, epoch),
    CONSTRAINT outbox_attempts_attempt_unique UNIQUE (tenant_id, run_id, attempt_id),
    CONSTRAINT outbox_attempts_tenant_attempt_unique UNIQUE (tenant_id, attempt_id),
    CONSTRAINT outbox_attempts_exact_start_unique UNIQUE (
        tenant_id,
        run_id,
        delivery_id,
        epoch,
        attempt_id,
        started_at,
        expires_at,
        start_digest
    ),
    CONSTRAINT outbox_attempts_current_projection_unique UNIQUE (
        tenant_id,
        run_id,
        delivery_id,
        epoch,
        attempt_id,
        started_at,
        expires_at
    ),
    CONSTRAINT outbox_attempts_delivery_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            delivery_id,
            delivery_expires_at,
            delivery_digest
        )
        REFERENCES stateknot.outbox_deliveries (
            tenant_id,
            run_id,
            delivery_id,
            expires_at,
            delivery_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT outbox_attempts_claim_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            claim_kind,
            delivery_id,
            epoch,
            attempt_id
        )
        REFERENCES stateknot.run_attempt_claims (
            tenant_id,
            run_id,
            claim_kind,
            delivery_id,
            delivery_epoch,
            attempt_id
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT outbox_attempts_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT outbox_attempts_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(delivery_id)
        AND stateknot.is_uuid_v7(attempt_id)
    ),
    CONSTRAINT outbox_attempts_position_valid CHECK (epoch BETWEEN 1 AND 64),
    CONSTRAINT outbox_attempts_digest_lengths CHECK (
        octet_length(delivery_digest) = 32
        AND octet_length(start_digest) = 32
    ),
    CONSTRAINT outbox_attempts_bytes_bounded CHECK (
        start_byte_length BETWEEN 1 AND 1048576
    ),
    CONSTRAINT outbox_attempts_clock_valid CHECK (
        started_at >= created_at
        AND created_at = started_at
        AND expires_at > started_at
        AND expires_at <= started_at + interval '5 minutes'
        AND expires_at <= delivery_expires_at
    )
);

CREATE TABLE stateknot.outbox_attempt_completions (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    epoch bigint NOT NULL,
    attempt_id uuid NOT NULL,
    started_at timestamptz(6) NOT NULL,
    attempt_expires_at timestamptz(6) NOT NULL,
    start_digest bytea NOT NULL,
    outcome_kind text NOT NULL,
    retry_advice_kind text,
    retry_delay_millis bigint,
    completed_at timestamptz(6) NOT NULL,
    completion_digest bytea NOT NULL,
    completion_bytes bytea NOT NULL,
    completion_byte_length integer GENERATED ALWAYS AS (octet_length(completion_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, delivery_id, epoch),
    CONSTRAINT outbox_attempt_completions_digest_unique UNIQUE (
        tenant_id,
        run_id,
        delivery_id,
        epoch,
        completion_digest
    ),
    CONSTRAINT outbox_attempt_completions_start_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            delivery_id,
            epoch,
            attempt_id,
            started_at,
            attempt_expires_at,
            start_digest
        )
        REFERENCES stateknot.outbox_attempts (
            tenant_id,
            run_id,
            delivery_id,
            epoch,
            attempt_id,
            started_at,
            expires_at,
            start_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT outbox_attempt_completions_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT outbox_attempt_completions_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(delivery_id)
        AND stateknot.is_uuid_v7(attempt_id)
    ),
    CONSTRAINT outbox_attempt_completions_position_valid CHECK (
        epoch BETWEEN 1 AND 64
    ),
    CONSTRAINT outbox_attempt_completions_outcome_valid CHECK (
        outcome_kind IN ('acknowledged', 'failed')
        AND (
            (
                outcome_kind = 'acknowledged'
                AND retry_advice_kind IS NULL
                AND retry_delay_millis IS NULL
            )
            OR (
                outcome_kind = 'failed'
                AND retry_advice_kind = 'never'
                AND retry_delay_millis IS NULL
            )
            OR (
                outcome_kind = 'failed'
                AND retry_advice_kind = 'safe_after'
                AND retry_delay_millis >= 0
            )
        )
    ),
    CONSTRAINT outbox_attempt_completions_digest_lengths CHECK (
        octet_length(start_digest) = 32
        AND octet_length(completion_digest) = 32
    ),
    CONSTRAINT outbox_attempt_completions_bytes_bounded CHECK (
        completion_byte_length BETWEEN 1 AND 1048576
    ),
    CONSTRAINT outbox_attempt_completions_clock_valid CHECK (
        completed_at >= started_at
        AND completed_at < attempt_expires_at
        AND created_at = completed_at
    )
);

ALTER TABLE stateknot.outbox_deliveries
    ADD CONSTRAINT outbox_deliveries_current_attempt_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            delivery_id,
            current_epoch,
            current_attempt_id,
            current_attempt_started_at,
            current_attempt_expires_at
        )
        REFERENCES stateknot.outbox_attempts (
            tenant_id,
            run_id,
            delivery_id,
            epoch,
            attempt_id,
            started_at,
            expires_at
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT outbox_deliveries_last_completion_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            delivery_id,
            current_epoch,
            last_completion_digest
        )
        REFERENCES stateknot.outbox_attempt_completions (
            tenant_id,
            run_id,
            delivery_id,
            epoch,
            completion_digest
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX outbox_deliveries_ready
    ON stateknot.outbox_deliveries (tenant_id, next_attempt_at, delivery_id)
    WHERE status IN ('pending', 'delivering', 'retry_scheduled')
      AND attempt_count < 64;

CREATE INDEX outbox_deliveries_expiry
    ON stateknot.outbox_deliveries (tenant_id, expires_at, delivery_id)
    WHERE status IN ('pending', 'delivering', 'retry_scheduled');

CREATE INDEX outbox_deliveries_abandoned_limit
    ON stateknot.outbox_deliveries (
        tenant_id,
        current_attempt_expires_at,
        delivery_id
    )
    WHERE status = 'delivering'
      AND attempt_count = 64
      AND last_completion_digest IS NULL;

CREATE INDEX outbox_deliveries_origin
    ON stateknot.outbox_deliveries (
        tenant_id,
        run_id,
        origin_sequence,
        delivery_id
    );
