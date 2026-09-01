-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- One immutable weighted schedule per explicit fairness shard. The schedule
-- bytes are interpreted by the runtime, while this table owns the global
-- cursor shared by every scheduler replica. Policies are immutable by shard
-- identity so mixed rolling-deployment configurations fail closed.
CREATE TABLE stateknot.scheduler_fairness_shards (
    shard_id text PRIMARY KEY,
    policy_digest bytea NOT NULL,
    policy_bytes bytea NOT NULL,
    policy_byte_length integer
        GENERATED ALWAYS AS (octet_length(policy_bytes)) STORED,
    cycle_length integer NOT NULL,
    next_slot integer NOT NULL DEFAULT 0,
    next_sequence bigint NOT NULL DEFAULT 0,
    registered_at timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT scheduler_fairness_shards_id_valid CHECK (
        octet_length(shard_id) BETWEEN 1 AND 128
        AND shard_id ~ '^[A-Za-z0-9._:-]+$'
        AND shard_id NOT IN ('.', '..')
    ),
    CONSTRAINT scheduler_fairness_shards_digest_length CHECK (
        octet_length(policy_digest) = 32
    ),
    CONSTRAINT scheduler_fairness_shards_policy_bounded CHECK (
        policy_byte_length BETWEEN 1 AND 262144
    ),
    CONSTRAINT scheduler_fairness_shards_cycle_bounded CHECK (
        cycle_length BETWEEN 1 AND 4096
    ),
    CONSTRAINT scheduler_fairness_shards_cursor_valid CHECK (
        next_slot >= 0 AND next_slot < cycle_length
    ),
    CONSTRAINT scheduler_fairness_shards_sequence_valid CHECK (
        next_sequence >= 0
    )
);

-- Stable reservation identities make an ambiguous commit retry return the
-- original slot instead of advancing fairness twice. Rows are intentionally
-- audit-grade; a later retention policy may delete only reservations older
-- than the deployment's maximum retry and observability window.
CREATE TABLE stateknot.scheduler_fairness_reservations (
    shard_id text NOT NULL,
    reservation_id uuid NOT NULL,
    policy_digest bytea NOT NULL,
    sequence bigint NOT NULL,
    slot integer NOT NULL,
    reserved_at timestamptz(6) NOT NULL,
    PRIMARY KEY (shard_id, reservation_id),
    CONSTRAINT scheduler_fairness_reservations_id_unique UNIQUE (reservation_id),
    CONSTRAINT scheduler_fairness_reservations_shard_fk FOREIGN KEY (shard_id)
        REFERENCES stateknot.scheduler_fairness_shards (shard_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT scheduler_fairness_reservations_id_v7 CHECK (
        stateknot.is_uuid_v7(reservation_id)
    ),
    CONSTRAINT scheduler_fairness_reservations_digest_length CHECK (
        octet_length(policy_digest) = 32
    ),
    CONSTRAINT scheduler_fairness_reservations_sequence_valid CHECK (
        sequence >= 0
    ),
    CONSTRAINT scheduler_fairness_reservations_slot_valid CHECK (
        slot BETWEEN 0 AND 4095
    )
);

CREATE UNIQUE INDEX scheduler_fairness_reservations_sequence
    ON stateknot.scheduler_fairness_reservations (shard_id, sequence);

CREATE INDEX scheduler_fairness_reservations_retention
    ON stateknot.scheduler_fairness_reservations (reserved_at, shard_id, sequence);
