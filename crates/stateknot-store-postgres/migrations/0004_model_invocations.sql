-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- One run-scoped registry prevents a physical provider/tool attempt identity
-- from being claimed by two logical invocations. It also gives future node
-- attempts one place to join without weakening existing immutable ledgers.
CREATE TABLE stateknot.run_attempt_claims (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    claim_kind text NOT NULL,
    invocation_id uuid NOT NULL,
    invocation_revision bigint NOT NULL,
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    claimed_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, attempt_id),
    CONSTRAINT run_attempt_claims_logical_revision_unique UNIQUE (
        tenant_id,
        run_id,
        claim_kind,
        invocation_id,
        invocation_revision
    ),
    CONSTRAINT run_attempt_claims_exact_claim_unique UNIQUE (
        tenant_id,
        run_id,
        claim_kind,
        invocation_id,
        invocation_revision,
        attempt_id
    ),
    CONSTRAINT run_attempt_claims_anchor_unique UNIQUE (
        tenant_id,
        run_id,
        journal_sequence
    ),
    CONSTRAINT run_attempt_claims_run_fk
        FOREIGN KEY (tenant_id, run_id)
        REFERENCES stateknot.runs (tenant_id, run_id)
        ON DELETE RESTRICT,
    CONSTRAINT run_attempt_claims_anchor_fk
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
    CONSTRAINT run_attempt_claims_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT run_attempt_claims_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(attempt_id)
        AND stateknot.is_uuid_v7(invocation_id)
        AND stateknot.is_uuid_v7(journal_event_id)
    ),
    CONSTRAINT run_attempt_claims_kind_valid CHECK (
        claim_kind IN ('tool_invocation', 'model_invocation')
    ),
    CONSTRAINT run_attempt_claims_position_valid CHECK (
        invocation_revision > 0
        AND journal_sequence > 0
    ),
    CONSTRAINT run_attempt_claims_digest_length CHECK (
        octet_length(journal_digest) = 32
    ),
    CONSTRAINT run_attempt_claims_clock_valid CHECK (
        claimed_at = journal_recorded_at
    )
);

-- Preserve every pre-v4 tool attempt before making the global claim mandatory.
INSERT INTO stateknot.run_attempt_claims (
    tenant_id,
    run_id,
    attempt_id,
    claim_kind,
    invocation_id,
    invocation_revision,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    claimed_at
)
SELECT
    tenant_id,
    run_id,
    started_attempt_id,
    'tool_invocation',
    invocation_id,
    revision,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    created_at
FROM stateknot.tool_invocation_revisions
WHERE started_attempt_id IS NOT NULL;

ALTER TABLE stateknot.tool_invocation_revisions
    ADD COLUMN attempt_claim_kind text
        GENERATED ALWAYS AS ('tool_invocation'::text) STORED,
    ADD CONSTRAINT tool_invocation_revisions_global_attempt_claim_fk
    FOREIGN KEY (
        tenant_id,
        run_id,
        attempt_claim_kind,
        invocation_id,
        revision,
        started_attempt_id
    )
    REFERENCES stateknot.run_attempt_claims (
        tenant_id,
        run_id,
        claim_kind,
        invocation_id,
        invocation_revision,
        attempt_id
    )
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;

-- The immutable intent is stored once. Model requests may legitimately carry
-- up to 64 MiB of inline content, and canonical JSON escaping can expand it;
-- 128 MiB is the explicit provider storage ceiling for the complete snapshot.
CREATE TABLE stateknot.model_invocations (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    invocation_id uuid NOT NULL,
    base_checkpoint_id uuid NOT NULL,
    base_superstep bigint NOT NULL,
    base_checkpoint_digest bytea NOT NULL,
    graph_namespace text NOT NULL,
    node_id text NOT NULL,
    activation_input_digest bytea NOT NULL,
    intent_digest bytea NOT NULL,
    intent_bytes bytea NOT NULL,
    intent_byte_length integer GENERATED ALWAYS AS (octet_length(intent_bytes)) STORED,
    current_revision bigint NOT NULL,
    current_status text NOT NULL,
    current_attempt_id uuid,
    current_record_digest bytea NOT NULL,
    created_at timestamptz(6) NOT NULL,
    updated_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, invocation_id),
    CONSTRAINT model_invocations_run_fk
        FOREIGN KEY (tenant_id, run_id)
        REFERENCES stateknot.runs (tenant_id, run_id)
        ON DELETE RESTRICT,
    CONSTRAINT model_invocations_checkpoint_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest
        )
        REFERENCES stateknot.run_checkpoints (
            tenant_id,
            run_id,
            checkpoint_id,
            superstep,
            checkpoint_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT model_invocations_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT model_invocations_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(invocation_id)
        AND stateknot.is_uuid_v7(base_checkpoint_id)
        AND (current_attempt_id IS NULL OR stateknot.is_uuid_v7(current_attempt_id))
    ),
    CONSTRAINT model_invocations_base_position_valid CHECK (base_superstep >= 0),
    CONSTRAINT model_invocations_namespace_valid CHECK (
        octet_length(graph_namespace) <= 512
        AND (
            graph_namespace = ''
            OR (
                graph_namespace ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}(/[A-Za-z0-9][A-Za-z0-9_.-]{0,127})*$'
                AND NOT (string_to_array(graph_namespace, '/') && ARRAY['.', '..']::text[])
            )
        )
    ),
    CONSTRAINT model_invocations_node_id_valid CHECK (
        octet_length(node_id) BETWEEN 1 AND 128
        AND node_id ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'
        AND node_id NOT IN ('.', '..')
    ),
    CONSTRAINT model_invocations_digest_lengths CHECK (
        octet_length(base_checkpoint_digest) = 32
        AND octet_length(activation_input_digest) = 32
        AND octet_length(intent_digest) = 32
        AND octet_length(current_record_digest) = 32
    ),
    CONSTRAINT model_invocations_intent_bytes_bounded CHECK (
        intent_byte_length BETWEEN 1 AND 134217728
    ),
    CONSTRAINT model_invocations_current_revision_valid CHECK (current_revision >= 0),
    CONSTRAINT model_invocations_current_status_valid CHECK (
        current_status IN ('prepared', 'executing', 'committed', 'failed')
    ),
    CONSTRAINT model_invocations_current_shape CHECK (
        (
            current_revision = 0
            AND current_status = 'prepared'
            AND current_attempt_id IS NULL
        )
        OR
        (
            current_revision > 0
            AND current_status <> 'prepared'
            AND current_attempt_id IS NOT NULL
        )
    ),
    CONSTRAINT model_invocations_clock_valid CHECK (updated_at >= created_at)
);

-- Revision bytes intentionally omit the immutable intent. The closed private
-- storage wire includes its digest and is rejoined with the verified intent
-- through ModelInvocation::restore before any record is returned.
CREATE TABLE stateknot.model_invocation_revisions (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    invocation_id uuid NOT NULL,
    revision bigint NOT NULL,
    previous_revision bigint,
    previous_digest bytea,
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    status text NOT NULL,
    attempt_id uuid,
    transition_kind text,
    started_attempt_id uuid,
    attempt_claim_kind text GENERATED ALWAYS AS ('model_invocation'::text) STORED,
    transition_digest bytea,
    record_digest bytea NOT NULL,
    record_bytes bytea NOT NULL,
    record_byte_length integer GENERATED ALWAYS AS (octet_length(record_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (tenant_id, run_id, invocation_id, revision),
    CONSTRAINT model_invocation_revisions_exact_identity_unique UNIQUE (
        tenant_id,
        run_id,
        invocation_id,
        revision,
        record_digest
    ),
    CONSTRAINT model_invocation_revisions_anchor_unique UNIQUE (
        tenant_id,
        run_id,
        journal_sequence
    ),
    CONSTRAINT model_invocation_revisions_started_attempt_unique UNIQUE (
        tenant_id,
        run_id,
        started_attempt_id
    ),
    CONSTRAINT model_invocation_revisions_invocation_fk
        FOREIGN KEY (tenant_id, run_id, invocation_id)
        REFERENCES stateknot.model_invocations (tenant_id, run_id, invocation_id)
        ON DELETE RESTRICT,
    CONSTRAINT model_invocation_revisions_anchor_fk
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
    CONSTRAINT model_invocation_revisions_previous_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            invocation_id,
            previous_revision,
            previous_digest
        )
        REFERENCES stateknot.model_invocation_revisions (
            tenant_id,
            run_id,
            invocation_id,
            revision,
            record_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT model_invocation_revisions_attempt_claim_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            attempt_claim_kind,
            invocation_id,
            revision,
            started_attempt_id
        )
        REFERENCES stateknot.run_attempt_claims (
            tenant_id,
            run_id,
            claim_kind,
            invocation_id,
            invocation_revision,
            attempt_id
        )
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT model_invocation_revisions_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT model_invocation_revisions_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(invocation_id)
        AND stateknot.is_uuid_v7(journal_event_id)
        AND (attempt_id IS NULL OR stateknot.is_uuid_v7(attempt_id))
        AND (started_attempt_id IS NULL OR stateknot.is_uuid_v7(started_attempt_id))
    ),
    CONSTRAINT model_invocation_revisions_position_valid CHECK (
        revision >= 0
        AND journal_sequence > 0
    ),
    CONSTRAINT model_invocation_revisions_status_valid CHECK (
        status IN ('prepared', 'executing', 'committed', 'failed')
    ),
    CONSTRAINT model_invocation_revisions_transition_kind_valid CHECK (
        transition_kind IS NULL
        OR transition_kind IN ('start_attempt', 'record_response', 'record_error')
    ),
    CONSTRAINT model_invocation_revisions_predecessor_shape CHECK (
        (
            revision = 0
            AND previous_revision IS NULL
            AND previous_digest IS NULL
            AND transition_kind IS NULL
            AND transition_digest IS NULL
        )
        OR
        (
            revision > 0
            AND previous_revision = revision - 1
            AND previous_digest IS NOT NULL
            AND octet_length(previous_digest) = 32
            AND transition_kind IS NOT NULL
            AND transition_digest IS NOT NULL
            AND octet_length(transition_digest) = 32
        )
    ),
    CONSTRAINT model_invocation_revisions_state_shape CHECK (
        (
            revision = 0
            AND status = 'prepared'
            AND attempt_id IS NULL
            AND started_attempt_id IS NULL
        )
        OR
        (
            revision > 0
            AND status <> 'prepared'
            AND attempt_id IS NOT NULL
        )
    ),
    CONSTRAINT model_invocation_revisions_transition_shape CHECK (
        (transition_kind IS NULL AND started_attempt_id IS NULL)
        OR
        (
            transition_kind = 'start_attempt'
            AND status = 'executing'
            AND started_attempt_id = attempt_id
        )
        OR
        (
            transition_kind = 'record_response'
            AND status = 'committed'
            AND started_attempt_id IS NULL
        )
        OR
        (
            transition_kind = 'record_error'
            AND status = 'failed'
            AND started_attempt_id IS NULL
        )
    ),
    CONSTRAINT model_invocation_revisions_digest_lengths CHECK (
        octet_length(journal_digest) = 32
        AND octet_length(record_digest) = 32
    ),
    CONSTRAINT model_invocation_revisions_record_bytes_bounded CHECK (
        record_byte_length BETWEEN 1 AND 134217728
    ),
    CONSTRAINT model_invocation_revisions_clock_valid CHECK (
        created_at = journal_recorded_at
    )
);

ALTER TABLE stateknot.model_invocations
    ADD CONSTRAINT model_invocations_current_record_fk
    FOREIGN KEY (
        tenant_id,
        run_id,
        invocation_id,
        current_revision,
        current_record_digest
    )
    REFERENCES stateknot.model_invocation_revisions (
        tenant_id,
        run_id,
        invocation_id,
        revision,
        record_digest
    )
    ON DELETE RESTRICT
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX model_invocations_unsettled_by_checkpoint
    ON stateknot.model_invocations (
        tenant_id,
        run_id,
        base_checkpoint_id,
        base_superstep,
        base_checkpoint_digest
    )
    WHERE current_status <> 'committed';
