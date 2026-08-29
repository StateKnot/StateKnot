-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Exact worker-source identity allows a pending result to prove in SQL that
-- its persisted fence is the same fence carried by its journal event.
ALTER TABLE stateknot.run_events
    ADD CONSTRAINT run_events_worker_anchor_unique UNIQUE (
        tenant_id,
        run_id,
        sequence,
        worker_attempt_id,
        worker_epoch
    );

-- Pending-result bindings must prove both the exact activation intent and an
-- exact committed revision without conditional triggers or application-only
-- joins. These targets remain useful to recovery and audit queries as well.
ALTER TABLE stateknot.run_checkpoints
    ADD CONSTRAINT run_checkpoints_exact_anchor_unique UNIQUE (
        tenant_id,
        run_id,
        checkpoint_id,
        superstep,
        checkpoint_digest,
        journal_sequence,
        journal_event_id,
        journal_recorded_at,
        journal_digest
    );

ALTER TABLE stateknot.tool_invocations
    ADD CONSTRAINT tool_invocations_exact_activation_unique UNIQUE (
        tenant_id,
        run_id,
        invocation_id,
        base_checkpoint_id,
        base_superstep,
        base_checkpoint_digest,
        graph_namespace,
        node_id,
        activation_input_digest
    );

ALTER TABLE stateknot.model_invocations
    ADD CONSTRAINT model_invocations_exact_activation_unique UNIQUE (
        tenant_id,
        run_id,
        invocation_id,
        base_checkpoint_id,
        base_superstep,
        base_checkpoint_digest,
        graph_namespace,
        node_id,
        activation_input_digest
    );

ALTER TABLE stateknot.tool_invocation_revisions
    ADD CONSTRAINT tool_invocation_revisions_committed_binding_unique UNIQUE (
        tenant_id,
        run_id,
        invocation_id,
        revision,
        record_digest,
        status,
        journal_sequence,
        journal_recorded_at,
        journal_digest
    );

ALTER TABLE stateknot.model_invocation_revisions
    ADD CONSTRAINT model_invocation_revisions_committed_binding_unique UNIQUE (
        tenant_id,
        run_id,
        invocation_id,
        revision,
        record_digest,
        status,
        journal_sequence,
        journal_recorded_at,
        journal_digest
    );

-- One immutable record exists for one logical activation key. input_digest is
-- intentionally not part of the primary key: reusing the logical key with a
-- different input is a conflict, not a second result.
CREATE TABLE stateknot.pending_node_results (
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
    intent_digest bytea NOT NULL,
    control_kind text NOT NULL,
    fence_attempt_id uuid NOT NULL,
    fence_epoch bigint NOT NULL,
    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,
    record_digest bytea NOT NULL,
    result_bytes bytea NOT NULL,
    result_byte_length integer GENERATED ALWAYS AS (octet_length(result_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (
        tenant_id,
        run_id,
        base_checkpoint_id,
        graph_namespace,
        node_id
    ),
    CONSTRAINT pending_node_results_anchor_unique UNIQUE (
        tenant_id,
        run_id,
        journal_sequence
    ),
    CONSTRAINT pending_node_results_exact_identity_unique UNIQUE (
        tenant_id,
        run_id,
        base_checkpoint_id,
        base_superstep,
        base_checkpoint_digest,
        graph_namespace,
        node_id,
        activation_input_digest,
        record_digest,
        journal_sequence,
        journal_recorded_at,
        journal_digest
    ),
    CONSTRAINT pending_node_results_consumption_target_unique UNIQUE (
        tenant_id,
        run_id,
        base_checkpoint_id,
        base_superstep,
        base_checkpoint_digest,
        graph_namespace,
        node_id,
        record_digest
    ),
    CONSTRAINT pending_node_results_run_fk
        FOREIGN KEY (tenant_id, run_id)
        REFERENCES stateknot.runs (tenant_id, run_id)
        ON DELETE RESTRICT,
    CONSTRAINT pending_node_results_base_checkpoint_fk
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
    CONSTRAINT pending_node_results_anchor_fk
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
    CONSTRAINT pending_node_results_worker_anchor_fk
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
    CONSTRAINT pending_node_results_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT pending_node_results_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(base_checkpoint_id)
        AND stateknot.is_uuid_v7(base_journal_event_id)
        AND stateknot.is_uuid_v7(fence_attempt_id)
        AND stateknot.is_uuid_v7(journal_event_id)
    ),
    CONSTRAINT pending_node_results_position_valid CHECK (
        base_superstep >= 0
        AND base_journal_sequence > 0
        AND journal_sequence > base_journal_sequence
        AND fence_epoch > 0
    ),
    CONSTRAINT pending_node_results_clock_valid CHECK (
        base_journal_recorded_at <= journal_recorded_at
        AND created_at = journal_recorded_at
    ),
    CONSTRAINT pending_node_results_namespace_valid CHECK (
        octet_length(graph_namespace) <= 512
        AND (
            graph_namespace = ''
            OR (
                graph_namespace ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}(/[A-Za-z0-9][A-Za-z0-9_.-]{0,127})*$'
                AND NOT (string_to_array(graph_namespace, '/') && ARRAY['.', '..']::text[])
            )
        )
    ),
    CONSTRAINT pending_node_results_node_id_valid CHECK (
        octet_length(node_id) BETWEEN 1 AND 128
        AND node_id ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$'
        AND node_id NOT IN ('.', '..')
    ),
    CONSTRAINT pending_node_results_control_kind_valid CHECK (
        control_kind IN ('continue', 'route', 'wait', 'terminal')
    ),
    CONSTRAINT pending_node_results_digest_lengths CHECK (
        octet_length(base_checkpoint_digest) = 32
        AND octet_length(base_journal_digest) = 32
        AND octet_length(activation_input_digest) = 32
        AND octet_length(intent_digest) = 32
        AND octet_length(journal_digest) = 32
        AND octet_length(record_digest) = 32
    ),
    CONSTRAINT pending_node_results_bytes_bounded CHECK (
        result_byte_length BETWEEN 1 AND 16777216
    )
);

-- Tool and model references use separate tables so ordinary composite foreign
-- keys, rather than polymorphic triggers, prove exact activation and committed
-- revision identity. Result journal fields are repeated only to make causal
-- ordering a local CHECK constraint.
CREATE TABLE stateknot.pending_node_result_tool_bindings (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    base_checkpoint_id uuid NOT NULL,
    base_superstep bigint NOT NULL,
    base_checkpoint_digest bytea NOT NULL,
    graph_namespace text NOT NULL,
    node_id text NOT NULL,
    activation_input_digest bytea NOT NULL,
    result_record_digest bytea NOT NULL,
    result_journal_sequence bigint NOT NULL,
    result_journal_recorded_at timestamptz(6) NOT NULL,
    result_journal_digest bytea NOT NULL,
    invocation_id uuid NOT NULL,
    invocation_revision bigint NOT NULL,
    invocation_record_digest bytea NOT NULL,
    invocation_status text GENERATED ALWAYS AS ('committed'::text) STORED,
    invocation_journal_sequence bigint NOT NULL,
    invocation_journal_recorded_at timestamptz(6) NOT NULL,
    invocation_journal_digest bytea NOT NULL,
    PRIMARY KEY (
        tenant_id,
        run_id,
        base_checkpoint_id,
        graph_namespace,
        node_id,
        invocation_id
    ),
    CONSTRAINT pending_node_result_tool_bindings_once UNIQUE (
        tenant_id,
        run_id,
        invocation_id,
        invocation_revision
    ),
    CONSTRAINT pending_node_result_tool_bindings_result_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest,
            result_record_digest,
            result_journal_sequence,
            result_journal_recorded_at,
            result_journal_digest
        )
        REFERENCES stateknot.pending_node_results (
            tenant_id,
            run_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest,
            record_digest,
            journal_sequence,
            journal_recorded_at,
            journal_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT pending_node_result_tool_bindings_activation_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            invocation_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest
        )
        REFERENCES stateknot.tool_invocations (
            tenant_id,
            run_id,
            invocation_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT pending_node_result_tool_bindings_revision_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            invocation_id,
            invocation_revision,
            invocation_record_digest,
            invocation_status,
            invocation_journal_sequence,
            invocation_journal_recorded_at,
            invocation_journal_digest
        )
        REFERENCES stateknot.tool_invocation_revisions (
            tenant_id,
            run_id,
            invocation_id,
            revision,
            record_digest,
            status,
            journal_sequence,
            journal_recorded_at,
            journal_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT pending_node_result_tool_bindings_causal CHECK (
        invocation_journal_sequence < result_journal_sequence
        AND invocation_journal_recorded_at <= result_journal_recorded_at
    ),
    CONSTRAINT pending_node_result_tool_bindings_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(base_checkpoint_id)
        AND stateknot.is_uuid_v7(invocation_id)
    ),
    CONSTRAINT pending_node_result_tool_bindings_positions_valid CHECK (
        base_superstep >= 0
        AND invocation_revision >= 0
        AND invocation_journal_sequence > 0
        AND result_journal_sequence > 0
    ),
    CONSTRAINT pending_node_result_tool_bindings_digest_lengths CHECK (
        octet_length(base_checkpoint_digest) = 32
        AND octet_length(activation_input_digest) = 32
        AND octet_length(result_record_digest) = 32
        AND octet_length(result_journal_digest) = 32
        AND octet_length(invocation_record_digest) = 32
        AND octet_length(invocation_journal_digest) = 32
    )
);

CREATE TABLE stateknot.pending_node_result_model_bindings (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    base_checkpoint_id uuid NOT NULL,
    base_superstep bigint NOT NULL,
    base_checkpoint_digest bytea NOT NULL,
    graph_namespace text NOT NULL,
    node_id text NOT NULL,
    activation_input_digest bytea NOT NULL,
    result_record_digest bytea NOT NULL,
    result_journal_sequence bigint NOT NULL,
    result_journal_recorded_at timestamptz(6) NOT NULL,
    result_journal_digest bytea NOT NULL,
    invocation_id uuid NOT NULL,
    invocation_revision bigint NOT NULL,
    invocation_record_digest bytea NOT NULL,
    invocation_status text GENERATED ALWAYS AS ('committed'::text) STORED,
    invocation_journal_sequence bigint NOT NULL,
    invocation_journal_recorded_at timestamptz(6) NOT NULL,
    invocation_journal_digest bytea NOT NULL,
    PRIMARY KEY (
        tenant_id,
        run_id,
        base_checkpoint_id,
        graph_namespace,
        node_id,
        invocation_id
    ),
    CONSTRAINT pending_node_result_model_bindings_once UNIQUE (
        tenant_id,
        run_id,
        invocation_id,
        invocation_revision
    ),
    CONSTRAINT pending_node_result_model_bindings_result_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest,
            result_record_digest,
            result_journal_sequence,
            result_journal_recorded_at,
            result_journal_digest
        )
        REFERENCES stateknot.pending_node_results (
            tenant_id,
            run_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest,
            record_digest,
            journal_sequence,
            journal_recorded_at,
            journal_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT pending_node_result_model_bindings_activation_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            invocation_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest
        )
        REFERENCES stateknot.model_invocations (
            tenant_id,
            run_id,
            invocation_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            activation_input_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT pending_node_result_model_bindings_revision_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            invocation_id,
            invocation_revision,
            invocation_record_digest,
            invocation_status,
            invocation_journal_sequence,
            invocation_journal_recorded_at,
            invocation_journal_digest
        )
        REFERENCES stateknot.model_invocation_revisions (
            tenant_id,
            run_id,
            invocation_id,
            revision,
            record_digest,
            status,
            journal_sequence,
            journal_recorded_at,
            journal_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT pending_node_result_model_bindings_causal CHECK (
        invocation_journal_sequence < result_journal_sequence
        AND invocation_journal_recorded_at <= result_journal_recorded_at
    ),
    CONSTRAINT pending_node_result_model_bindings_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(base_checkpoint_id)
        AND stateknot.is_uuid_v7(invocation_id)
    ),
    CONSTRAINT pending_node_result_model_bindings_positions_valid CHECK (
        base_superstep >= 0
        AND invocation_revision >= 0
        AND invocation_journal_sequence > 0
        AND result_journal_sequence > 0
    ),
    CONSTRAINT pending_node_result_model_bindings_digest_lengths CHECK (
        octet_length(base_checkpoint_digest) = 32
        AND octet_length(activation_input_digest) = 32
        AND octet_length(result_record_digest) = 32
        AND octet_length(result_journal_digest) = 32
        AND octet_length(invocation_record_digest) = 32
        AND octet_length(invocation_journal_digest) = 32
    )
);

-- Consumption is an append-only one-to-one record, not an UPDATE of the
-- immutable pending result. The successor must be the immediately following
-- superstep and its complete checkpoint/journal identity is foreign-keyed.
CREATE TABLE stateknot.pending_node_result_consumptions (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,
    base_checkpoint_id uuid NOT NULL,
    base_superstep bigint NOT NULL,
    base_checkpoint_digest bytea NOT NULL,
    graph_namespace text NOT NULL,
    node_id text NOT NULL,
    result_record_digest bytea NOT NULL,
    successor_checkpoint_id uuid NOT NULL,
    successor_superstep bigint NOT NULL,
    successor_checkpoint_digest bytea NOT NULL,
    successor_journal_sequence bigint NOT NULL,
    successor_journal_event_id uuid NOT NULL,
    successor_journal_recorded_at timestamptz(6) NOT NULL,
    successor_journal_digest bytea NOT NULL,
    created_at timestamptz(6) NOT NULL,
    PRIMARY KEY (
        tenant_id,
        run_id,
        base_checkpoint_id,
        graph_namespace,
        node_id
    ),
    CONSTRAINT pending_node_result_consumptions_result_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            result_record_digest
        )
        REFERENCES stateknot.pending_node_results (
            tenant_id,
            run_id,
            base_checkpoint_id,
            base_superstep,
            base_checkpoint_digest,
            graph_namespace,
            node_id,
            record_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT pending_node_result_consumptions_successor_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            successor_checkpoint_id,
            successor_superstep,
            successor_checkpoint_digest,
            successor_journal_sequence,
            successor_journal_event_id,
            successor_journal_recorded_at,
            successor_journal_digest
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
    CONSTRAINT pending_node_result_consumptions_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(base_checkpoint_id)
        AND stateknot.is_uuid_v7(successor_checkpoint_id)
        AND stateknot.is_uuid_v7(successor_journal_event_id)
    ),
    CONSTRAINT pending_node_result_consumptions_position_valid CHECK (
        base_superstep >= 0
        AND successor_superstep = base_superstep + 1
        AND successor_journal_sequence > 0
    ),
    CONSTRAINT pending_node_result_consumptions_clock_valid CHECK (
        created_at = successor_journal_recorded_at
    ),
    CONSTRAINT pending_node_result_consumptions_digest_lengths CHECK (
        octet_length(base_checkpoint_digest) = 32
        AND octet_length(result_record_digest) = 32
        AND octet_length(successor_checkpoint_digest) = 32
        AND octet_length(successor_journal_digest) = 32
    )
);

CREATE INDEX pending_node_results_unconsumed_scan
    ON stateknot.pending_node_results (
        tenant_id,
        run_id,
        base_checkpoint_id,
        graph_namespace,
        node_id
    );

CREATE INDEX pending_node_result_consumptions_successor
    ON stateknot.pending_node_result_consumptions (
        tenant_id,
        run_id,
        successor_checkpoint_id
    );
