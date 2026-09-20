-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Payload-redacted activation decisions and database-clock authority windows.
-- Base records and revocations are append-only; a window is active exactly
-- while database time is before expires_at and no revocation row exists.
CREATE TABLE stateknot.skill_activation_approvals (
    tenant_id text NOT NULL,
    approval_id uuid NOT NULL,
    run_id uuid NOT NULL,
    thread_id uuid NOT NULL,
    protocol text NOT NULL,
    origin text NOT NULL,
    skill_uri text NOT NULL,
    manifest_digest bytea NOT NULL,
    subject_digest bytea NOT NULL,
    source_kind text NOT NULL,
    parent_window_id uuid,
    policy_digest bytea NOT NULL,
    decision_digest bytea NOT NULL,
    requested_duration_ms bigint NOT NULL,
    approval_digest bytea NOT NULL,
    recorded_at timestamptz(6) NOT NULL,
    approval_bytes bytea NOT NULL,
    approval_byte_length integer GENERATED ALWAYS AS (octet_length(approval_bytes)) STORED,
    approval_bytes_digest bytea GENERATED ALWAYS AS (sha256(approval_bytes)) STORED,
    PRIMARY KEY (tenant_id, approval_id),
    CONSTRAINT skill_activation_approvals_run_thread_fk
        FOREIGN KEY (tenant_id, run_id, thread_id)
        REFERENCES stateknot.runs (tenant_id, run_id, thread_id)
        ON DELETE RESTRICT,
    CONSTRAINT skill_activation_approvals_identity_unique
        UNIQUE (tenant_id, approval_id, run_id, thread_id, subject_digest),
    CONSTRAINT skill_activation_approvals_tenant_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT skill_activation_approvals_ids_uuid_v7 CHECK (
        stateknot.is_uuid_v7(approval_id)
        AND stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(thread_id)
        AND (parent_window_id IS NULL OR stateknot.is_uuid_v7(parent_window_id))
    ),
    CONSTRAINT skill_activation_approvals_subject_valid CHECK (
        octet_length(protocol) BETWEEN 1 AND 32
        AND protocol ~ '^[a-z0-9.-]+$'
        AND octet_length(origin) BETWEEN 1 AND 128
        AND origin = btrim(origin)
        AND octet_length(skill_uri) BETWEEN 1 AND 4096
        AND skill_uri = btrim(skill_uri)
    ),
    CONSTRAINT skill_activation_approvals_source_valid CHECK (
        (source_kind = 'direct' AND parent_window_id IS NULL)
        OR (source_kind = 'nested' AND parent_window_id IS NOT NULL)
    ),
    CONSTRAINT skill_activation_approvals_digest_lengths CHECK (
        octet_length(manifest_digest) = 32
        AND octet_length(subject_digest) = 32
        AND octet_length(policy_digest) = 32
        AND octet_length(decision_digest) = 32
        AND octet_length(approval_digest) = 32
        AND octet_length(approval_bytes_digest) = 32
    ),
    CONSTRAINT skill_activation_approvals_duration_valid CHECK (
        requested_duration_ms BETWEEN 1000 AND 86400000
    ),
    CONSTRAINT skill_activation_approvals_bytes_bounded CHECK (
        approval_byte_length BETWEEN 1 AND 65536
    ),
    CONSTRAINT skill_activation_approvals_clock_valid CHECK (isfinite(recorded_at))
);

CREATE TABLE stateknot.skill_acting_windows (
    tenant_id text NOT NULL,
    window_id uuid NOT NULL,
    approval_id uuid NOT NULL,
    run_id uuid NOT NULL,
    thread_id uuid NOT NULL,
    subject_digest bytea NOT NULL,
    opened_at timestamptz(6) NOT NULL,
    expires_at timestamptz(6) NOT NULL,
    window_digest bytea NOT NULL,
    window_bytes bytea NOT NULL,
    window_byte_length integer GENERATED ALWAYS AS (octet_length(window_bytes)) STORED,
    window_bytes_digest bytea GENERATED ALWAYS AS (sha256(window_bytes)) STORED,
    PRIMARY KEY (tenant_id, window_id),
    CONSTRAINT skill_acting_windows_approval_unique UNIQUE (tenant_id, approval_id),
    CONSTRAINT skill_acting_windows_approval_fk
        FOREIGN KEY (tenant_id, approval_id, run_id, thread_id, subject_digest)
        REFERENCES stateknot.skill_activation_approvals (
            tenant_id, approval_id, run_id, thread_id, subject_digest
        )
        ON DELETE RESTRICT,
    CONSTRAINT skill_acting_windows_ids_uuid_v7 CHECK (
        stateknot.is_uuid_v7(window_id)
        AND stateknot.is_uuid_v7(approval_id)
        AND stateknot.is_uuid_v7(run_id)
        AND stateknot.is_uuid_v7(thread_id)
    ),
    CONSTRAINT skill_acting_windows_digest_lengths CHECK (
        octet_length(subject_digest) = 32
        AND octet_length(window_digest) = 32
        AND octet_length(window_bytes_digest) = 32
    ),
    CONSTRAINT skill_acting_windows_bytes_bounded CHECK (
        window_byte_length BETWEEN 1 AND 131072
    ),
    CONSTRAINT skill_acting_windows_clock_valid CHECK (
        isfinite(opened_at) AND isfinite(expires_at)
        AND expires_at > opened_at
        AND expires_at <= opened_at + interval '24 hours'
    )
);

ALTER TABLE stateknot.skill_activation_approvals
    ADD CONSTRAINT skill_activation_approvals_parent_window_fk
    FOREIGN KEY (tenant_id, parent_window_id)
    REFERENCES stateknot.skill_acting_windows (tenant_id, window_id)
    ON DELETE RESTRICT;

CREATE TABLE stateknot.skill_acting_window_revocations (
    tenant_id text NOT NULL,
    window_id uuid NOT NULL,
    reason text NOT NULL,
    revoked_at timestamptz(6) NOT NULL,
    revocation_digest bytea NOT NULL,
    revocation_bytes bytea NOT NULL,
    revocation_byte_length integer GENERATED ALWAYS AS (octet_length(revocation_bytes)) STORED,
    revocation_bytes_digest bytea GENERATED ALWAYS AS (sha256(revocation_bytes)) STORED,
    PRIMARY KEY (tenant_id, window_id),
    CONSTRAINT skill_acting_window_revocations_window_fk
        FOREIGN KEY (tenant_id, window_id)
        REFERENCES stateknot.skill_acting_windows (tenant_id, window_id)
        ON DELETE RESTRICT,
    CONSTRAINT skill_acting_window_revocations_window_uuid_v7 CHECK (
        stateknot.is_uuid_v7(window_id)
    ),
    CONSTRAINT skill_acting_window_revocations_reason_valid CHECK (
        reason IN ('user', 'policy', 'compromised', 'superseded', 'administrative')
    ),
    CONSTRAINT skill_acting_window_revocations_digest_lengths CHECK (
        octet_length(revocation_digest) = 32
        AND octet_length(revocation_bytes_digest) = 32
    ),
    CONSTRAINT skill_acting_window_revocations_bytes_bounded CHECK (
        revocation_byte_length BETWEEN 1 AND 65536
    ),
    CONSTRAINT skill_acting_window_revocations_clock_valid CHECK (isfinite(revoked_at))
);

CREATE INDEX skill_acting_windows_active_scope
    ON stateknot.skill_acting_windows (tenant_id, run_id, expires_at, window_id);

ALTER TABLE stateknot.tool_authorization_receipts
    ADD COLUMN authorization_window_id uuid,
    ADD CONSTRAINT tool_authorization_receipts_window_fk
        FOREIGN KEY (tenant_id, authorization_window_id)
        REFERENCES stateknot.skill_acting_windows (tenant_id, window_id)
        ON DELETE RESTRICT,
    ADD CONSTRAINT tool_authorization_receipts_window_uuid_v7 CHECK (
        authorization_window_id IS NULL OR stateknot.is_uuid_v7(authorization_window_id)
    );

CREATE INDEX tool_authorization_receipts_window_history
    ON stateknot.tool_authorization_receipts (
        tenant_id, authorization_window_id, recorded_at, receipt_id
    ) WHERE authorization_window_id IS NOT NULL;

CREATE FUNCTION stateknot.reject_skill_authorization_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Skill authorization evidence is immutable' USING ERRCODE = '23514';
END
$$;

CREATE TRIGGER skill_activation_approvals_immutable
BEFORE UPDATE OR DELETE ON stateknot.skill_activation_approvals
FOR EACH ROW EXECUTE FUNCTION stateknot.reject_skill_authorization_mutation();

CREATE TRIGGER skill_acting_windows_immutable
BEFORE UPDATE OR DELETE ON stateknot.skill_acting_windows
FOR EACH ROW EXECUTE FUNCTION stateknot.reject_skill_authorization_mutation();

CREATE TRIGGER skill_acting_window_revocations_immutable
BEFORE UPDATE OR DELETE ON stateknot.skill_acting_window_revocations
FOR EACH ROW EXECUTE FUNCTION stateknot.reject_skill_authorization_mutation();
