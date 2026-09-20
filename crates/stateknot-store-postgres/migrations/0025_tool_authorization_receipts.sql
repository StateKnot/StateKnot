-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- One receipt is immutable evidence that an exact policy authorized an exact
-- Tool operation. It deliberately proves neither provider dispatch nor an
-- external side effect. Canonical receipt bytes retain digests, never Tool
-- arguments or policy payloads. Receipt identity, rather than Tool-attempt
-- provenance, is the idempotency key: multiple provider calls for one durable
-- attempt require independently authorized receipts.
ALTER TABLE stateknot.runs
    ADD CONSTRAINT runs_exact_thread_unique UNIQUE (tenant_id, run_id, thread_id);

ALTER TABLE stateknot.tool_invocation_revisions
    ADD CONSTRAINT tool_invocation_revisions_attempt_event_unique UNIQUE (
        tenant_id,
        run_id,
        invocation_id,
        attempt_id,
        journal_event_id
    );

CREATE TABLE stateknot.tool_authorization_receipts (
    tenant_id text NOT NULL,
    receipt_id uuid NOT NULL,
    run_id uuid NOT NULL,
    thread_id uuid NOT NULL,
    invocation_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    origin_event_id uuid NOT NULL,
    operation text NOT NULL,
    descriptor_digest bytea NOT NULL,
    input_digest bytea NOT NULL,
    subject_digest bytea NOT NULL,
    policy_digest bytea NOT NULL,
    decision_digest bytea NOT NULL,
    receipt_digest bytea NOT NULL,
    has_recovery_handle boolean NOT NULL,
    authorized_at timestamptz(6) NOT NULL,
    recorded_at timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
    receipt_bytes bytea NOT NULL,
    receipt_byte_length integer GENERATED ALWAYS AS (octet_length(receipt_bytes)) STORED,
    receipt_bytes_digest bytea GENERATED ALWAYS AS (sha256(receipt_bytes)) STORED,
    PRIMARY KEY (tenant_id, receipt_id),
    CONSTRAINT tool_authorization_receipts_run_thread_fk
        FOREIGN KEY (tenant_id, run_id, thread_id)
        REFERENCES stateknot.runs (tenant_id, run_id, thread_id)
        ON DELETE RESTRICT,
    CONSTRAINT tool_authorization_receipts_attempt_event_fk
        FOREIGN KEY (
            tenant_id,
            run_id,
            invocation_id,
            attempt_id,
            origin_event_id
        )
        REFERENCES stateknot.tool_invocation_revisions (
            tenant_id,
            run_id,
            invocation_id,
            attempt_id,
            journal_event_id
        )
        ON DELETE RESTRICT,
    CONSTRAINT tool_authorization_receipts_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT tool_authorization_receipts_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(receipt_id)
        AND stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(thread_id)
        AND stateknot.is_uuid_v7(invocation_id)
        AND stateknot.is_uuid_v7(attempt_id)
        AND stateknot.is_uuid_v7(origin_event_id)
    ),
    CONSTRAINT tool_authorization_receipts_operation_valid CHECK (
        operation IN ('execute', 'reconcile')
    ),
    CONSTRAINT tool_authorization_receipts_digest_lengths CHECK (
        octet_length(descriptor_digest) = 32
        AND octet_length(input_digest) = 32
        AND octet_length(subject_digest) = 32
        AND octet_length(policy_digest) = 32
        AND octet_length(decision_digest) = 32
        AND octet_length(receipt_digest) = 32
        AND octet_length(receipt_bytes_digest) = 32
    ),
    CONSTRAINT tool_authorization_receipts_bytes_bounded CHECK (
        receipt_byte_length BETWEEN 1 AND 65536
    ),
    CONSTRAINT tool_authorization_receipts_clock_valid CHECK (
        isfinite(authorized_at)
        AND isfinite(recorded_at)
        AND authorized_at <= recorded_at
    )
);

CREATE INDEX tool_authorization_receipts_invocation_history
    ON stateknot.tool_authorization_receipts (
        tenant_id,
        run_id,
        invocation_id,
        recorded_at,
        receipt_id
    );

CREATE FUNCTION stateknot.reject_tool_authorization_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Tool authorization receipts are immutable' USING ERRCODE = '23514';
END
$$;

CREATE TRIGGER tool_authorization_receipts_immutable
BEFORE UPDATE OR DELETE ON stateknot.tool_authorization_receipts
FOR EACH ROW EXECUTE FUNCTION stateknot.reject_tool_authorization_receipt_mutation();
