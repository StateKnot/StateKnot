<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable artifact storage and A2A task completion

> Status: implemented pre-alpha slice; the Rust API is not stable.<br>
> Metadata: PostgreSQL migration 18.<br>
> Bytes: private S3-compatible object storage, with an in-memory backend for tests.<br>
> Explicit exclusions: public download URLs, general retention/garbage collection,
> signed artifacts, cross-region replication qualification, and a stable artifact API.

StateKnot now keeps a completed A2A task's artifact bytes out of inline tool
JSON. `A2aRemoteAgent::bind_with_durable_artifacts` requires a Task response,
persists each terminal artifact part through `StateKnotArtifactStore`, and
returns only a bounded completion projection plus tenant-qualified
`ArtifactRef` values.

This is one evidence pipeline, not an eventually consistent convenience API:

```text
durable Executing event
  -> one A2A message send
  -> endpoint-bound task recovery handle
  -> direct GetTask polling (never a business-message resend)
  -> bounded terminal artifact parts
  -> unique staging object
  -> conditional create of deterministic final object
  -> complete length + SHA-256 verification
  -> immutable PostgreSQL registration anchored to the origin event
  -> authorized lookup + conditional object read + complete re-verification
```

## Storage invariants

- The final object key and registration key are deterministic for the exact
  tenant, run, logical invocation, physical attempt, origin event, tool,
  remote task/artifact identity, and part position. An exact retry converges;
  changed bytes at the same identity fail integrity checks.
- Final keys are published with destination-create semantics. No retry may
  overwrite an existing final object. Initialization performs a real
  put/copy-if-absent/read/repeated-copy/delete probe and refuses a backend that
  does not demonstrate this contract.
- PostgreSQL stores canonical `ArtifactRef` bytes and digest, content length
  and digest, the causing run/event, same-tenant direct parents, and a private
  provider-neutral object locator. The locator never appears in `ArtifactRef`,
  public errors, or `Debug` output.
- Registration performs no object I/O while holding its transaction. The
  object is fully published and verified first; migration 18 then atomically
  registers its immutable metadata and lineage.
- Resolution authorizes the exact principal and tenant-qualified artifact
  identity before any registry lookup. It uses the captured object
  version/entity tag when available, then reads the complete bounded body and
  verifies both byte length and SHA-256 before returning a byte.

## Configure the production boundary

Use a migration role to apply migration 18 before constructing the runtime
store. Use workload identity or a short-lived credential provider for object
storage; the wrapper deliberately exposes no access-key arguments.

```rust,no_run
use std::{net::IpAddr, sync::Arc};
use stateknot_artifact_store::{
    ArtifactStoreOptions, RemoteArtifactOrigin, S3CompatibleBackendBuilder,
    S3ConditionalCopy, StateKnotArtifactStore,
};

let objects = S3CompatibleBackendBuilder::from_env(
    "stateknot-private-artifacts",
    "ap-southeast-1",
)?
.with_https_endpoint("https://s3.example.internal")?
.with_conditional_copy(S3ConditionalCopy::AmazonMultipart)
.with_sha256_checksum()
.with_kms_key_id("alias/stateknot-artifacts")?
.build()?;

let origin = RemoteArtifactOrigin::https(
    "https://artifacts.partner.example",
    ["203.0.113.10".parse::<IpAddr>()?],
)?;
let options = ArtifactStoreOptions::default()
    .with_remote_origins([origin])?
    .with_limits(64 * 1024 * 1024, 64 * 1024 * 1024, 8 * 1024 * 1024, 2)?
    .with_concurrency_limit(8)?;

let artifacts = Arc::new(
    StateKnotArtifactStore::initialize(
        objects,
        Arc::new(postgres_store.clone()),
        Arc::new(artifact_read_authorizer),
        "production-artifacts-v1",
        options,
    )
    .await?,
);

let remote = A2aRemoteAgent::bind_with_durable_artifacts(
    descriptor,
    a2a_client,
    "answer",
    delivery,
    recovery,
    schemas,
    artifacts,
)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`AmazonMultipart` is the AWS S3 strategy. A compatible provider may instead
require its documented conditional destination header and failure status via
`S3ConditionalCopy::header` or `header_with_status`. Do not guess: run the
startup probe against the exact endpoint, bucket policy, encryption policy,
and identity used by the service.

`from_env` imports only AWS static/session credentials, web-identity settings,
and validated ECS/EKS container-credential settings. It deliberately ignores
environment-provided endpoints, HTTP enablement, metadata overrides, unsigned
requests, retry behavior, bucket/region selection, and encryption settings.
Configure a non-default web-identity STS service explicitly with the validated
`with_https_sts_endpoint` method.

The storage namespace identifies one stable physical backend without exposing
its endpoint or bucket. Changing the backing bucket under the same namespace
requires an integrity-preserving migration; otherwise allocate a new
namespace.

## Remote URL policy

Text, structured JSON, inline bytes, and external URL parts are supported.
External URLs have a separate egress boundary:

- production origins must be exact HTTPS origins with explicit IP pins;
- each redirect hop is resolved and checked against the allowlist;
- credentials and fragments in URLs are rejected;
- environment proxies, automatic redirects, retries, cookies, and transparent
  content decoding are disabled;
- `Content-Encoding` must be absent or `identity`;
- a declared part media type must match the response media type; and
- declared and observed lengths are checked against the request and local
  ceilings before registration.

Literal loopback HTTP exists only for tests or a managed same-host sidecar. It
is not a production cross-host transport.

## Bounds and lifecycle obligations

| Limit | Default | Hard ceiling |
| --- | ---: | ---: |
| Object operation timeout | 60 s | 10 min |
| Complete remote request timeout | 120 s | 10 min |
| Multipart part | 8 MiB | 5–64 MiB |
| Remote object | 64 MiB | 1 GiB |
| Materialized read | 64 MiB | 1 GiB |
| Redirects | 3 | 3 |
| Concurrent operations | 8 | 256 |

One process-local permit covers the complete ingest or materialized-read path,
including authorization, registry, remote download, and object operations. The
A2A request also intersects these settings with the Tool execution limits:
maximum artifact count, total artifact bytes, per-part bytes, and inline result
bytes. A direct Message, a non-terminal Task without a recoverable handle, an
unsupported part, duplicate local artifact identity, or any overrun fails
closed.

Configure provider lifecycle rules for abandoned multipart uploads and the
`stateknot/staging/v1/` prefix. Runtime cleanup is best effort and increments
`staging_cleanup_failures()` when it cannot delete staging data. Alert on every
increase.

Do **not** apply a blind age rule to `stateknot/artifacts/v1/`: final objects may
already be durably registered. A database outage after final publication can
also leave an unregistered deterministic object; an exact retry will adopt and
verify it. Until general retention and a registry-aware orphan collector ship,
operators must inventory that final prefix and reconcile suspected orphans
against PostgreSQL before deletion.

## Executable evidence and release boundary

```console
cargo test -p stateknot-artifact-store --locked

STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL='postgres://...' \
cargo test -p stateknot-artifact-store --test artifact_store --locked

STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL='postgres://...' \
cargo test -p stateknot-store-postgres --test postgres \
  artifact_registry_is_exact_tenant_scoped_and_lineage_safe --locked

cargo test -p stateknot-integrations --test a2a_client_contract \
  durable_task_handle --locked
```

The tests cover exact retry, substitution and object tampering, authorization
before lookup, URL origin/redirect/media/encoding/size rejection, multipart
ingestion, full resolution, the real PostgreSQL registry, migration 17→18, and
terminal-task recovery without resend over both HTTP+JSON and JSON-RPC.

This evidence does not yet qualify a live S3-compatible service, lifecycle
collector, backup/restore topology, or public download service. Those remain
production release gates rather than implicit claims.
