-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Immutable, tenant-scoped compiled graph definitions. A graph version is a
-- permanent owner-qualified identity: changing any schema, reducer, node,
-- route, or execution limit requires a new version. The canonical bytes are
-- the authority; redundant columns provide bounded lookup and fail-closed
-- projection checks during every load.
CREATE TABLE stateknot.graph_definitions (
    tenant_id text NOT NULL,
    owner_issuer text NOT NULL,
    owner_subject text NOT NULL,
    graph_name text NOT NULL,
    graph_version text NOT NULL,
    definition_digest bytea NOT NULL,
    definition_bytes bytea NOT NULL,
    definition_byte_length integer
        GENERATED ALWAYS AS (octet_length(definition_bytes)) STORED,
    registered_at timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        tenant_id,
        owner_issuer,
        owner_subject,
        graph_name,
        graph_version
    ),
    CONSTRAINT graph_definitions_exact_reference_unique UNIQUE (
        tenant_id,
        owner_issuer,
        owner_subject,
        graph_name,
        graph_version,
        definition_digest
    ),
    CONSTRAINT graph_definitions_tenant_id_valid CHECK (
        octet_length(tenant_id) BETWEEN 1 AND 128
        AND tenant_id ~ '^[A-Za-z0-9._:-]+$'
        AND tenant_id NOT IN ('.', '..')
    ),
    CONSTRAINT graph_definitions_owner_issuer_valid CHECK (
        octet_length(owner_issuer) BETWEEN 9 AND 512
        AND octet_length(owner_issuer) = char_length(owner_issuer)
        AND owner_issuer ~ '^[Hh][Tt][Tt][Pp][Ss]://[^/?#@]+(/[^?#]*)?$'
    ),
    CONSTRAINT graph_definitions_owner_subject_valid CHECK (
        octet_length(owner_subject) BETWEEN 1 AND 255
        AND octet_length(owner_subject) = char_length(owner_subject)
        AND (owner_subject COLLATE "C") ~ '^[ -~]+$'
    ),
    CONSTRAINT graph_definitions_name_valid CHECK (
        octet_length(graph_name) BETWEEN 1 AND 128
        AND graph_name ~ '^[A-Za-z0-9_.-]+$'
    ),
    CONSTRAINT graph_definitions_version_valid CHECK (
        octet_length(graph_version) BETWEEN 5 AND 62
        AND graph_version ~ '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$'
    ),
    CONSTRAINT graph_definitions_digest_length CHECK (
        octet_length(definition_digest) = 32
    ),
    CONSTRAINT graph_definitions_bytes_bounded CHECK (
        definition_byte_length BETWEEN 1 AND 2097280
    )
);

-- Recovery normally addresses a row by its owner-qualified identity. This
-- secondary key supports integrity tooling and future checkpoint FK backfills
-- that start from the compact digest already projected in checkpoint rows.
CREATE INDEX graph_definitions_digest_lookup
    ON stateknot.graph_definitions (tenant_id, definition_digest);
