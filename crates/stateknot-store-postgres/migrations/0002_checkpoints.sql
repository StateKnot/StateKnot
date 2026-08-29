-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Bind every new journal mutation to the exact lifecycle projection requested
-- by its caller. Rows written by the pre-checkpoint migration remain NULL and
-- therefore fail closed for lost-acknowledgement projection comparison.
ALTER TABLE stateknot.run_events
    ADD COLUMN projection_digest bytea;

ALTER TABLE stateknot.run_events
    ADD CONSTRAINT run_events_projection_digest_valid CHECK (
        projection_digest IS NULL OR octet_length(projection_digest) = 32
    ),
    ADD CONSTRAINT run_events_checkpoint_anchor_unique UNIQUE (
        tenant_id,
        run_id,
        sequence,
        event_id,
        recorded_at,
        event_digest
    );

CREATE TABLE stateknot.run_checkpoints (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    checkpoint_id uuid NOT NULL,
    superstep bigint NOT NULL,
    parent_checkpoint_id uuid,
    parent_superstep bigint,
    parent_digest bytea,
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    graph_definition_digest bytea NOT NULL,
    state_schema_id text NOT NULL,
    state_schema_version text NOT NULL,
    state_schema_digest bytea NOT NULL,
    state_digest bytea NOT NULL,
    intent_digest bytea NOT NULL,
    checkpoint_digest bytea NOT NULL,
    checkpoint_bytes bytea NOT NULL,
    checkpoint_byte_length integer GENERATED ALWAYS AS (octet_length(checkpoint_bytes)) STORED,
    created_at timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, run_id, checkpoint_id),
    CONSTRAINT run_checkpoints_superstep_unique UNIQUE (tenant_id, run_id, superstep),
    CONSTRAINT run_checkpoints_exact_identity_unique UNIQUE (
        tenant_id,
        run_id,
        checkpoint_id,
        superstep,
        checkpoint_digest
    ),
    CONSTRAINT run_checkpoints_anchor_unique UNIQUE (tenant_id, run_id, journal_sequence),
    CONSTRAINT run_checkpoints_run_fk
        FOREIGN KEY (tenant_id, run_id)
        REFERENCES stateknot.runs (tenant_id, run_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_checkpoints_anchor_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            journal_sequence,
            journal_event_id,
            journal_recorded_at,
            journal_digest
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
    CONSTRAINT run_checkpoints_parent_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            parent_checkpoint_id,
            parent_superstep,
            parent_digest
        )
        REFERENCES stateknot.run_checkpoints (
            tenant_id,
            run_id,
            checkpoint_id,
            superstep,
            checkpoint_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT run_checkpoints_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT run_checkpoints_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(checkpoint_id)
        AND (parent_checkpoint_id IS NULL OR stateknot.is_uuid_v7(parent_checkpoint_id))
        AND stateknot.is_uuid_v7(journal_event_id)
    ),
    CONSTRAINT run_checkpoints_position_valid CHECK (superstep >= 0),
    CONSTRAINT run_checkpoints_parent_shape CHECK (
        (
            superstep = 0
            AND parent_checkpoint_id IS NULL
            AND parent_superstep IS NULL
            AND parent_digest IS NULL
        )
        OR
        (
            superstep > 0
            AND parent_checkpoint_id IS NOT NULL
            AND parent_superstep IS NOT NULL
            AND parent_superstep = superstep - 1
            AND parent_digest IS NOT NULL
            AND octet_length(parent_digest) = 32
        )
    ),
    CONSTRAINT run_checkpoints_journal_sequence_valid CHECK (journal_sequence > 0),
    CONSTRAINT run_checkpoints_schema_id_valid CHECK (
        octet_length(state_schema_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT run_checkpoints_schema_version_valid CHECK (
        octet_length(state_schema_version) BETWEEN 5 AND 62
        AND state_schema_version ~ '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
    ),
    CONSTRAINT run_checkpoints_digest_lengths CHECK (
        octet_length(journal_digest) = 32
        AND octet_length(graph_definition_digest) = 32
        AND octet_length(state_schema_digest) = 32
        AND octet_length(state_digest) = 32
        AND octet_length(intent_digest) = 32
        AND octet_length(checkpoint_digest) = 32
    ),
    CONSTRAINT run_checkpoints_bytes_bounded CHECK (
        checkpoint_byte_length BETWEEN 1 AND 2621440
    )
);

ALTER TABLE stateknot.runs
    ADD COLUMN checkpoint_id uuid,
    ADD COLUMN checkpoint_superstep bigint,
    ADD COLUMN checkpoint_digest bytea;

ALTER TABLE stateknot.runs
    ADD CONSTRAINT runs_checkpoint_head_shape CHECK (
        (
            checkpoint_id IS NULL
            AND checkpoint_superstep IS NULL
            AND checkpoint_digest IS NULL
        )
        OR
        (
            checkpoint_id IS NOT NULL
            AND stateknot.is_uuid_v7(checkpoint_id)
            AND checkpoint_superstep IS NOT NULL
            AND checkpoint_superstep >= 0
            AND checkpoint_digest IS NOT NULL
            AND octet_length(checkpoint_digest) = 32
        )
    ),
    ADD CONSTRAINT runs_checkpoint_head_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            checkpoint_id,
            checkpoint_superstep,
            checkpoint_digest
        )
        REFERENCES stateknot.run_checkpoints (
            tenant_id,
            run_id,
            checkpoint_id,
            superstep,
            checkpoint_digest
        )
        ON DELETE RESTRICT;

CREATE INDEX run_checkpoints_created_at
    ON stateknot.run_checkpoints (tenant_id, run_id, created_at);
