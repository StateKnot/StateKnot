-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Extend the existing run-wide physical-attempt registry without changing any
-- immutable v4/v5 migration. Tool/model claims retain their exact invocation
-- identity. Node claims instead bind the canonical logical-activation digest.
ALTER TABLE stateknot.run_attempt_claims
    DROP CONSTRAINT run_attempt_claims_ids_are_uuid_v7,
    DROP CONSTRAINT run_attempt_claims_kind_valid,
    DROP CONSTRAINT run_attempt_claims_position_valid,
    ALTER COLUMN invocation_id DROP NOT NULL,
    ALTER COLUMN invocation_revision DROP NOT NULL,
    ADD COLUMN activation_digest bytea;

ALTER TABLE stateknot.run_attempt_claims
    ADD CONSTRAINT run_attempt_claims_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(attempt_id)
        AND (invocation_id IS NULL OR stateknot.is_uuid_v7(invocation_id))
        AND stateknot.is_uuid_v7(journal_event_id)
    ),
    ADD CONSTRAINT run_attempt_claims_kind_valid CHECK (
        claim_kind IN ('tool_invocation', 'model_invocation', 'node_attempt')
    ),
    ADD CONSTRAINT run_attempt_claims_owner_shape CHECK (
        (
            claim_kind IN ('tool_invocation', 'model_invocation')
            AND invocation_id IS NOT NULL
            AND invocation_revision > 0
            AND activation_digest IS NULL
        )
        OR (
            claim_kind = 'node_attempt'
            AND invocation_id IS NULL
            AND invocation_revision IS NULL
            AND octet_length(activation_digest) = 32
        )
    ),
    ADD CONSTRAINT run_attempt_claims_node_exact_unique UNIQUE (
        tenant_id,
        run_id,
        claim_kind,
        activation_digest,
        attempt_id
    );

-- Starts are immutable and must commit before user node code runs. The
-- physical node attempt is intentionally distinct from the worker-run attempt
-- in the authorizing fence, so one worker lease may execute bounded retries.
CREATE TABLE stateknot.node_attempts (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    base_checkpoint_id uuid NOT NULL,
    base_superstep bigint NOT NULL,
    base_checkpoint_digest bytea NOT NULL,
    base_journal_sequence bigint NOT NULL,
    base_journal_event_id uuid NOT NULL,
    base_journal_recorded_at timestamptz(6) NOT NULL,
    base_journal_digest bytea NOT NULL,
    graph_namespace text NOT NULL,
    node_id text NOT NULL,
    activation_input_digest bytea NOT NULL,
    activation_digest bytea NOT NULL,
    attempt_id uuid NOT NULL,
    claim_kind text GENERATED ALWAYS AS ('node_attempt'::text) STORED,
    fence_attempt_id uuid NOT NULL,
    fence_epoch bigint NOT NULL,
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    start_digest bytea NOT NULL,
    start_bytes bytea NOT NULL,
    start_byte_length integer GENERATED ALWAYS AS (octet_length(start_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, attempt_id),
    CONSTRAINT node_attempts_start_anchor_unique UNIQUE (
        tenant_id,
        run_id,
        journal_sequence
    ),
    CONSTRAINT node_attempts_exact_start_unique UNIQUE (
        tenant_id,
        run_id,
        attempt_id,
        base_checkpoint_id,
        base_superstep,
        base_checkpoint_digest,
        graph_namespace,
        node_id,
        activation_input_digest,
        activation_digest,
        fence_attempt_id,
        fence_epoch,
        journal_sequence,
        journal_event_id,
        journal_recorded_at,
        journal_digest,
        start_digest
    ),
    CONSTRAINT node_attempts_base_checkpoint_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            base_journal_sequence,
            base_journal_event_id,
            base_journal_recorded_at,
            base_journal_digest
        )
        REFERENCES stateknot.run_checkpoints (
            tenant_id,
            run_id,
            checkpoint_id,
            superstep,
            checkpoint_digest,
            journal_sequence,
            journal_event_id,
            journal_recorded_at,
            journal_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT node_attempts_claim_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            claim_kind,
            activation_digest,
            attempt_id
        )
        REFERENCES stateknot.run_attempt_claims (
            tenant_id,
            run_id,
            claim_kind,
            activation_digest,
            attempt_id
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT node_attempts_anchor_fk
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
    CONSTRAINT node_attempts_worker_anchor_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            journal_sequence,
            fence_attempt_id,
            fence_epoch
        )
        REFERENCES stateknot.run_events (
            tenant_id,
            run_id,
            sequence,
            worker_attempt_id,
            worker_epoch
        )
        ON DELETE RESTRICT,
    CONSTRAINT node_attempts_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT node_attempts_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(base_checkpoint_id)
        AND stateknot.is_uuid_v7(base_journal_event_id)
        AND stateknot.is_uuid_v7(attempt_id)
        AND stateknot.is_uuid_v7(fence_attempt_id)
        AND stateknot.is_uuid_v7(journal_event_id)
        AND attempt_id <> fence_attempt_id
    ),
    CONSTRAINT node_attempts_position_valid CHECK (
        base_superstep >= 0
        AND base_journal_sequence > 0
        AND journal_sequence > base_journal_sequence
        AND fence_epoch > 0
    ),
    CONSTRAINT node_attempts_clock_valid CHECK (
        base_journal_recorded_at <= journal_recorded_at
        AND created_at = journal_recorded_at
    ),
    CONSTRAINT node_attempts_namespace_valid CHECK (
        octet_length(graph_namespace) <= 512
        AND (
            graph_namespace = ''
            OR (
                graph_namespace ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}(/[A-Za-z0-9][A-Za-z0-9_.-]{0,127})*$'
                AND NOT (string_to_array(graph_namespace, '/') && ARRAY['.', '..']::text[])
            )
        )
    ),
    CONSTRAINT node_attempts_node_id_valid CHECK (
        octet_length(node_id) BETWEEN 1 AND 128
        AND node_id ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'
        AND node_id NOT IN ('.', '..')
    ),
    CONSTRAINT node_attempts_digest_lengths CHECK (
        octet_length(base_checkpoint_digest) = 32
        AND octet_length(base_journal_digest) = 32
        AND octet_length(activation_input_digest) = 32
        AND octet_length(activation_digest) = 32
        AND octet_length(journal_digest) = 32
        AND octet_length(start_digest) = 32
    ),
    CONSTRAINT node_attempts_bytes_bounded CHECK (
        start_byte_length BETWEEN 1 AND 1048576
    )
);

-- Migration 5 predates the physical node-attempt contract. Existing pending
-- results remain truthful legacy records with a NULL node_attempt_id. Runtime
-- APIs introduced with v6 require this field for every new result and never
-- synthesize a start that did not durably happen.
ALTER TABLE stateknot.pending_node_results
    ADD COLUMN node_attempt_id uuid,
    ADD CONSTRAINT pending_node_results_node_attempt_id_valid CHECK (
        node_attempt_id IS NULL OR stateknot.is_uuid_v7(node_attempt_id)
    ),
    ADD CONSTRAINT pending_node_results_node_attempt_unique UNIQUE (
        tenant_id,
        run_id,
        node_attempt_id
    ),
    ADD CONSTRAINT pending_node_results_node_attempt_exact_unique UNIQUE (
        tenant_id,
        run_id,
        node_attempt_id,
        base_checkpoint_id,
        base_superstep,
        base_checkpoint_digest,
        graph_namespace,
        node_id,
        activation_input_digest,
        intent_digest,
        fence_attempt_id,
        fence_epoch,
        journal_sequence,
        journal_event_id,
        journal_recorded_at,
        journal_digest,
        record_digest
    ),
    ADD CONSTRAINT pending_node_results_node_attempt_fk
        FOREIGN KEY (tenant_id, run_id, node_attempt_id)
        REFERENCES stateknot.node_attempts (tenant_id, run_id, attempt_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

-- Completion is append-only. Success shares its event with the exact pending
-- result and ordinary composite foreign keys prove that relationship. Failure
-- stores the indexed recovery projection, while canonical bytes remain the
-- integrity authority for failure evidence and usage.
CREATE TABLE stateknot.node_attempt_completions (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    base_checkpoint_id uuid NOT NULL,
    base_superstep bigint NOT NULL,
    base_checkpoint_digest bytea NOT NULL,
    graph_namespace text NOT NULL,
    node_id text NOT NULL,
    activation_input_digest bytea NOT NULL,
    activation_digest bytea NOT NULL,
    fence_attempt_id uuid NOT NULL,
    fence_epoch bigint NOT NULL,
    start_journal_sequence bigint NOT NULL,
    start_journal_event_id uuid NOT NULL,
    start_journal_recorded_at timestamptz(6) NOT NULL,
    start_journal_digest bytea NOT NULL,
    start_digest bytea NOT NULL,
    status text NOT NULL,
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    result_intent_digest bytea,
    result_record_digest bytea,
    failure_id uuid,
    retry_kind text,
    retry_not_before timestamptz(6),
    completion_digest bytea NOT NULL,
    completion_bytes bytea NOT NULL,
    completion_byte_length integer GENERATED ALWAYS AS (octet_length(completion_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, attempt_id),
    CONSTRAINT node_attempt_completions_anchor_unique UNIQUE (
        tenant_id,
        run_id,
        journal_sequence
    ),
    CONSTRAINT node_attempt_completions_start_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            attempt_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest,
            activation_digest,
            fence_attempt_id,
            fence_epoch,
            start_journal_sequence,
            start_journal_event_id,
            start_journal_recorded_at,
            start_journal_digest,
            start_digest
        )
        REFERENCES stateknot.node_attempts (
            tenant_id,
            run_id,
            attempt_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest,
            activation_digest,
            fence_attempt_id,
            fence_epoch,
            journal_sequence,
            journal_event_id,
            journal_recorded_at,
            journal_digest,
            start_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT node_attempt_completions_anchor_fk
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
    CONSTRAINT node_attempt_completions_worker_anchor_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            journal_sequence,
            fence_attempt_id,
            fence_epoch
        )
        REFERENCES stateknot.run_events (
            tenant_id,
            run_id,
            sequence,
            worker_attempt_id,
            worker_epoch
        )
        ON DELETE RESTRICT,
    CONSTRAINT node_attempt_completions_result_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            attempt_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest,
            result_intent_digest,
            fence_attempt_id,
            fence_epoch,
            journal_sequence,
            journal_event_id,
            journal_recorded_at,
            journal_digest,
            result_record_digest
        )
        REFERENCES stateknot.pending_node_results (
            tenant_id,
            run_id,
            node_attempt_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest,
            intent_digest,
            fence_attempt_id,
            fence_epoch,
            journal_sequence,
            journal_event_id,
            journal_recorded_at,
            journal_digest,
            record_digest
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT node_attempt_completions_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(attempt_id)
        AND stateknot.is_uuid_v7(base_checkpoint_id)
        AND stateknot.is_uuid_v7(fence_attempt_id)
        AND stateknot.is_uuid_v7(start_journal_event_id)
        AND stateknot.is_uuid_v7(journal_event_id)
        AND (failure_id IS NULL OR stateknot.is_uuid_v7(failure_id))
        AND attempt_id <> fence_attempt_id
    ),
    CONSTRAINT node_attempt_completions_position_valid CHECK (
        base_superstep >= 0
        AND start_journal_sequence > 0
        AND journal_sequence > start_journal_sequence
        AND fence_epoch > 0
    ),
    CONSTRAINT node_attempt_completions_clock_valid CHECK (
        start_journal_recorded_at <= journal_recorded_at
        AND created_at = journal_recorded_at
    ),
    CONSTRAINT node_attempt_completions_status_valid CHECK (
        status IN ('succeeded', 'failed')
    ),
    CONSTRAINT node_attempt_completions_outcome_shape CHECK (
        (
            status = 'succeeded'
            AND result_intent_digest IS NOT NULL
            AND result_record_digest IS NOT NULL
            AND failure_id IS NULL
            AND retry_kind IS NULL
            AND retry_not_before IS NULL
        )
        OR (
            status = 'failed'
            AND result_intent_digest IS NULL
            AND result_record_digest IS NULL
            AND failure_id IS NOT NULL
            AND retry_kind IN ('never', 'safe_after')
            AND (
                (retry_kind = 'never' AND retry_not_before IS NULL)
                OR (retry_kind = 'safe_after' AND retry_not_before IS NOT NULL)
            )
        )
    ),
    CONSTRAINT node_attempt_completions_digest_lengths CHECK (
        octet_length(base_checkpoint_digest) = 32
        AND octet_length(activation_input_digest) = 32
        AND octet_length(activation_digest) = 32
        AND octet_length(start_journal_digest) = 32
        AND octet_length(start_digest) = 32
        AND octet_length(journal_digest) = 32
        AND (result_intent_digest IS NULL OR octet_length(result_intent_digest) = 32)
        AND (result_record_digest IS NULL OR octet_length(result_record_digest) = 32)
        AND octet_length(completion_digest) = 32
    ),
    CONSTRAINT node_attempt_completions_bytes_bounded CHECK (
        completion_byte_length BETWEEN 1 AND 16777216
    )
);

CREATE INDEX node_attempts_activation_history
    ON stateknot.node_attempts (
        tenant_id,
        run_id,
        base_checkpoint_id,
        base_superstep,
        base_checkpoint_digest,
        graph_namespace,
        node_id,
        activation_input_digest,
        journal_sequence
    );

CREATE INDEX node_attempt_completions_retry_ready
    ON stateknot.node_attempt_completions (
        tenant_id,
        retry_not_before,
        run_id,
        base_checkpoint_id,
        graph_namespace,
        node_id
    )
    WHERE status = 'failed' AND retry_kind = 'safe_after';
