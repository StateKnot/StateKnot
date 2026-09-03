-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Immutable, tenant-scoped artifact metadata is stored separately from private
-- object coordinates. The public ArtifactRef remains safe to cross protocol
-- boundaries; only trusted storage code can load the locator. Registration
-- keys make an ambiguous commit retry converge on the exact original row.

CREATE TABLE stateknot.artifacts (
    tenant_id text NOT NULL,
    artifact_id uuid NOT NULL,
    registration_key_digest bytea NOT NULL,
    artifact_ref_digest bytea NOT NULL,
    artifact_ref_bytes bytea NOT NULL,
    artifact_ref_byte_length integer
        GENERATED ALWAYS AS (octet_length(artifact_ref_bytes)) STORED,
    content_byte_length bigint NOT NULL,
    content_digest bytea NOT NULL,
    provenance_run_id uuid NOT NULL,
    provenance_event_id uuid NOT NULL,
    storage_namespace text NOT NULL,
    object_key text NOT NULL,
    object_version text,
    object_etag text,
    registered_at timestamptz(6) NOT NULL,

    PRIMARY KEY (tenant_id, artifact_id),
    CONSTRAINT artifacts_registration_key_unique
        UNIQUE (tenant_id, registration_key_digest),
    CONSTRAINT artifacts_object_locator_unique
        UNIQUE (storage_namespace, object_key),
    CONSTRAINT artifacts_provenance_event_fk
        FOREIGN KEY (tenant_id, provenance_run_id, provenance_event_id)
        REFERENCES stateknot.run_events (tenant_id, run_id, event_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT artifacts_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT artifacts_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(artifact_id)
        AND stateknot.is_uuid_v7(provenance_run_id)
        AND stateknot.is_uuid_v7(provenance_event_id)
    ),
    CONSTRAINT artifacts_digest_lengths CHECK (
        octet_length(registration_key_digest) = 32
        AND octet_length(artifact_ref_digest) = 32
        AND octet_length(content_digest) = 32
    ),
    CONSTRAINT artifacts_ref_bytes_bounded CHECK (
        artifact_ref_byte_length BETWEEN 2 AND 262144
    ),
    CONSTRAINT artifacts_content_length_valid CHECK (content_byte_length >= 0),
    CONSTRAINT artifacts_storage_namespace_valid CHECK (
        octet_length(storage_namespace) BETWEEN 1 AND 128
        AND storage_namespace ~ '^[A-Za-z0-9][A-Za-z0-9._:-]*$'
        AND storage_namespace NOT IN ('.', '..')
    ),
    CONSTRAINT artifacts_object_key_valid CHECK (
        octet_length(object_key) BETWEEN 1 AND 1024
        AND object_key NOT LIKE '/%'
        AND object_key NOT LIKE '%/'
        AND object_key NOT LIKE '%//%'
        AND object_key !~ '(^|/)\.{1,2}(/|$)'
        AND object_key !~ '[[:cntrl:]]'
        AND position(chr(92) IN object_key) = 0
    ),
    CONSTRAINT artifacts_object_version_valid CHECK (
        object_version IS NULL
        OR (
            octet_length(object_version) BETWEEN 1 AND 1024
            AND object_version ~ '^[ -~]+$'
        )
    ),
    CONSTRAINT artifacts_object_etag_valid CHECK (
        object_etag IS NULL
        OR (
            octet_length(object_etag) BETWEEN 1 AND 1024
            AND object_etag ~ '^[ -~]+$'
        )
    )
);

CREATE INDEX artifacts_provenance
    ON stateknot.artifacts (tenant_id, provenance_run_id, provenance_event_id, artifact_id);

CREATE TABLE stateknot.artifact_parents (
    tenant_id text NOT NULL,
    artifact_id uuid NOT NULL,
    parent_artifact_id uuid NOT NULL,

    PRIMARY KEY (tenant_id, artifact_id, parent_artifact_id),
    CONSTRAINT artifact_parents_child_fk
        FOREIGN KEY (tenant_id, artifact_id)
        REFERENCES stateknot.artifacts (tenant_id, artifact_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT artifact_parents_parent_fk
        FOREIGN KEY (tenant_id, parent_artifact_id)
        REFERENCES stateknot.artifacts (tenant_id, artifact_id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT artifact_parents_not_self CHECK (artifact_id <> parent_artifact_id),
    CONSTRAINT artifact_parents_ids_are_uuid_v7 CHECK (
        stateknot.is_uuid_v7(artifact_id)
        AND stateknot.is_uuid_v7(parent_artifact_id)
    )
);

CREATE INDEX artifact_parents_reverse_lineage
    ON stateknot.artifact_parents (tenant_id, parent_artifact_id, artifact_id);
