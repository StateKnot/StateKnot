-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Immutable, audit-grade Agent admission snapshots. One row binds the exact
-- authenticated Agent/request/policy/budget intent to the database clock, the
-- first journal fact, the active lifecycle projection, the registered compiled
-- graph, and the superstep-zero checkpoint. The store creates every referenced
-- row in one transaction, so no scheduler can observe a half-initialized Agent
-- run and an ambiguous client retry can recover the original commit exactly.
CREATE TABLE stateknot.agent_admissions (
    tenant_id text NOT NULL,
    run_id uuid NOT NULL,

    agent_owner_issuer text NOT NULL,
    agent_owner_subject text NOT NULL,
    agent_name text NOT NULL,
    agent_version text NOT NULL,

    graph_owner_issuer text NOT NULL,
    graph_owner_subject text NOT NULL,
    graph_name text NOT NULL,
    graph_version text NOT NULL,
    graph_definition_digest bytea NOT NULL,

    policy_owner_issuer text NOT NULL,
    policy_owner_subject text NOT NULL,
    policy_name text NOT NULL,
    policy_version text NOT NULL,
    policy_digest bytea NOT NULL,

    intent_digest bytea NOT NULL,
    admission_digest bytea NOT NULL,
    admitted_at timestamptz(6) NOT NULL,

    journal_sequence bigint NOT NULL,
    journal_event_id uuid NOT NULL,
    journal_recorded_at timestamptz(6) NOT NULL,
    journal_digest bytea NOT NULL,

    checkpoint_id uuid NOT NULL,
    checkpoint_superstep bigint NOT NULL,
    checkpoint_digest bytea NOT NULL,

    admission_bytes bytea NOT NULL,
    admission_byte_length integer
        GENERATED ALWAYS AS (octet_length(admission_bytes)) STORED,
    created_at timestamptz(6) NOT NULL,

    PRIMARY KEY (tenant_id, run_id),
    CONSTRAINT agent_admissions_run_fk
        FOREIGN KEY (tenant_id, run_id)
        REFERENCES stateknot.runs (tenant_id, run_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT agent_admissions_graph_fk
        FOREIGN KEY (
            tenant_id,
            graph_owner_issuer,
            graph_owner_subject,
            graph_name,
            graph_version,
            graph_definition_digest
        )
        REFERENCES stateknot.graph_definitions (
            tenant_id,
            owner_issuer,
            owner_subject,
            graph_name,
            graph_version,
            definition_digest
        )
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT agent_admissions_event_fk
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
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT agent_admissions_checkpoint_fk
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
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT agent_admissions_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT agent_admissions_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(journal_event_id)
        AND stateknot.is_uuid_v7(checkpoint_id)
    ),
    CONSTRAINT agent_admissions_agent_identity_valid CHECK (
        octet_length(agent_owner_issuer) BETWEEN 9 AND 512
        AND octet_length(agent_owner_subject) BETWEEN 1 AND 255
        AND octet_length(agent_name) BETWEEN 1 AND 128
        AND octet_length(agent_version) BETWEEN 5 AND 62
    ),
    CONSTRAINT agent_admissions_graph_identity_valid CHECK (
        octet_length(graph_owner_issuer) BETWEEN 9 AND 512
        AND octet_length(graph_owner_subject) BETWEEN 1 AND 255
        AND octet_length(graph_name) BETWEEN 1 AND 128
        AND octet_length(graph_version) BETWEEN 5 AND 62
    ),
    CONSTRAINT agent_admissions_policy_identity_valid CHECK (
        octet_length(policy_owner_issuer) BETWEEN 9 AND 512
        AND octet_length(policy_owner_subject) BETWEEN 1 AND 255
        AND octet_length(policy_name) BETWEEN 1 AND 128
        AND octet_length(policy_version) BETWEEN 5 AND 62
    ),
    CONSTRAINT agent_admissions_digest_lengths CHECK (
        octet_length(graph_definition_digest) = 32
        AND octet_length(policy_digest) = 32
        AND octet_length(intent_digest) = 32
        AND octet_length(admission_digest) = 32
        AND octet_length(journal_digest) = 32
        AND octet_length(checkpoint_digest) = 32
    ),
    CONSTRAINT agent_admissions_initial_anchor CHECK (
        journal_sequence = 1
        AND checkpoint_superstep = 0
        AND journal_recorded_at = admitted_at
        AND created_at = admitted_at
    ),
    CONSTRAINT agent_admissions_bytes_bounded CHECK (
        admission_byte_length BETWEEN 1 AND 16777216
    )
);

CREATE INDEX agent_admissions_agent_version
    ON stateknot.agent_admissions (
        tenant_id,
        agent_owner_issuer,
        agent_owner_subject,
        agent_name,
        agent_version,
        admitted_at,
        run_id
    );

CREATE INDEX agent_admissions_graph_version
    ON stateknot.agent_admissions (
        tenant_id,
        graph_owner_issuer,
        graph_owner_subject,
        graph_name,
        graph_version,
        admitted_at,
        run_id
    );

CREATE INDEX agent_admissions_policy_version
    ON stateknot.agent_admissions (
        tenant_id,
        policy_owner_issuer,
        policy_owner_subject,
        policy_name,
        policy_version,
        admitted_at,
        run_id
    );

CREATE INDEX agent_admissions_digest_lookup
    ON stateknot.agent_admissions (tenant_id, admission_digest);

-- Scheduler discovery is the only automatic path to lease ownership. A
-- low-level run row remains addressable for trusted bootstrap/repair APIs, but
-- it is not executable work until an initial checkpoint exists. Including the
-- predicate in the index keeps the fail-closed query index-only at scale.
DROP INDEX stateknot.runs_scheduler_ready;

CREATE INDEX runs_scheduler_ready
    ON stateknot.runs (
        tenant_id,
        (GREATEST(
            scheduler_ready_at,
            COALESCE(scheduler_not_before, scheduler_ready_at),
            COALESCE(lease_expires_at, scheduler_ready_at)
        )),
        run_id
    )
    WHERE quarantined_at IS NULL
      AND scheduler_ready_at IS NOT NULL
      AND checkpoint_id IS NOT NULL
      AND lifecycle_status IN ('pending', 'active', 'cancellation_requested');
