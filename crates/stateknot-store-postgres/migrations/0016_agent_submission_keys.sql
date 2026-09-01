-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Durable ingress idempotency for Agent submissions. Raw caller keys are never
-- stored: key_digest is tenant-scoped and submission_digest binds the logical
-- Agent request, policy, graph and initial state while excluding framework-
-- generated provenance, audit-event and checkpoint identities. The row is
-- inserted in the same transaction as its
-- referenced Agent admission, so a lost acknowledgement can resolve the one
-- original run even when a retry generated a different candidate ID bundle.

ALTER TABLE stateknot.agent_admissions
    ADD CONSTRAINT agent_admissions_run_digest_unique
    UNIQUE (tenant_id, run_id, admission_digest);

CREATE TABLE stateknot.agent_submission_keys (
    tenant_id text NOT NULL,
    key_digest bytea NOT NULL,
    submission_digest bytea NOT NULL,
    run_id uuid NOT NULL,
    admission_digest bytea NOT NULL,
    created_at timestamptz(6) NOT NULL,

    PRIMARY KEY (tenant_id, key_digest),
    CONSTRAINT agent_submission_keys_run_unique
        UNIQUE (tenant_id, run_id),
    CONSTRAINT agent_submission_keys_admission_fk
        FOREIGN KEY (tenant_id, run_id, admission_digest)
        REFERENCES stateknot.agent_admissions (
            tenant_id,
            run_id,
            admission_digest
        )
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT agent_submission_keys_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT agent_submission_keys_ids_valid CHECK (
        stateknot.is_uuid_v7(run_id)
    ),
    CONSTRAINT agent_submission_keys_digest_lengths CHECK (
        octet_length(key_digest) = 32
        AND octet_length(submission_digest) = 32
        AND octet_length(admission_digest) = 32
    )
);

CREATE INDEX agent_submission_keys_created
    ON stateknot.agent_submission_keys (tenant_id, created_at, run_id);
