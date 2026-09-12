<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP compute Worker

`stateknot::McpComputeNode` runs a pure Graph calculation on a separate MCP
server without giving that server database or control-plane credentials.
The adapter lives in the trusted host. A Worker gets only explicitly selected
state fields; the host owns leases, cancellation, result commits and accounting.

This is an implemented pre-alpha profile, not a stable API, arbitrary-code
sandbox, effectful Worker API or production release. See
[RFC-0005](rfcs/0005-mcp-compute-worker-boundary.md) for exact semantics.

## Where it belongs

Use it for repeatable parsing, normalization and pure application calculations.
Do not put model calls, paid APIs, writes, mutable external reads, child
admission or MCP sampling in this profile. Use `DurableInvocationExecutor` and
the existing model/Tool providers for effectful or billable work.

The host configures either `UpdateAndContinue` or `Terminal`. A remote response
cannot choose a route, wait, child, lease, identity, usage record or invocation
binding. The output schema and existing Graph reducer remain local authority.
Schema validity does not prove that a compromised Worker computed correctly.

## Bind an existing MCP deployment

Prepare a `McpClient` with a fixed HTTPS endpoint, authenticated deployment
identity and resource-specific credentials, and an offline `JsonSchemaRegistry`.
The input/output schema documents advertised by the selected Tool must exactly
match the corresponding local canonical documents, including `$id` and version.
The complete Tool descriptor digest must come from an independently reviewed
release manifest, not a discovery response automatically approved at startup.

```rust
use stateknot::{
    McpComputeNode, McpComputeNodeBinding, McpComputeOutput, WorkerInputProjection,
};

let binding = McpComputeNodeBinding::new(
    &compiled_graph,
    node_id,
    worker_input_schema,
    WorkerInputProjection::new(["document", "locale"])?,
    McpComputeOutput::UpdateAndContinue,
    expected_server_revision,
    "normalize_document",
    reviewed_tool_descriptor_digest,
)?;
let executor = McpComputeNode::connect(
    authenticated_client,
    binding,
    frozen_schemas,
    std::time::Duration::from_secs(20),
).await?;
executable_builder.register_node(std::sync::Arc::new(executor))?;
```

This is a binding fragment; the complete executable Graph, first-party MCP
server and recovery setup are compiled in
[`mcp_compute.rs`](../crates/stateknot/tests/mcp_compute.rs) and its
[PostgreSQL module](../crates/stateknot/tests/mcp_compute/postgres.rs).
The fixture token/verifier and plaintext loopback listener are test-only,
not production authentication configuration.

Projection is a duplicate-free allowlist of at most 64 top-level fields. Each
selected field includes its complete bounded value; do not select a container
holding secrets. Empty selection sends `{}`. Missing fields fail before remote
dispatch. There is no whole-state wildcard or remote-selected projection.

The Tool must return a complete result with empty `content` and valid
`structuredContent`. Text, resources, MRTR, notifications, Tasks and promoted
input headers are outside this profile. `readOnlyHint` is not an effect proof.

## Deployment checklist

1. Run the Worker under an independent OS/workload identity with its own files
   and network policy. No PostgreSQL/control-plane/provider credentials, shared
   secret mounts, cloud metadata access or administrative callbacks.
2. Authenticate and authorize Agent admission in the trusted host. Review the
   field projection as a data-release policy for every tenant using the binding.
3. Use HTTPS with certificate validation. Configure MCP Bearer verification,
   exact Host/Origin allowlists, least-privilege Tool scopes and bounded admission
   on the Worker. Its credential must not authorize the control plane or another
   resource. Use [the existing MCP server](mcp-server.md), not the test verifier.
4. Apply CPU/memory/process and network egress limits externally. Driver/client
   concurrency and deadlines bound local requests, not a malicious remote CPU.
5. Pin and retain code, descriptor and schema release material. Changing Worker
   code, projection or policy requires a new Graph revision and draining old
   deployments; serverInfo alone is not authenticated code attestation.
6. Redact proxy/application logs. Alert on `worker.invalid_input`,
   `worker.invalid_output`, `worker.unavailable`, `worker.timeout` and
   `worker.cancelled`; keep source payloads out of public diagnostics.

Do not expose the trusted SQL runtime role described in
[PostgreSQL roles](postgresql-roles.md). Connecting to the same machine with an
empty environment does not provide filesystem or network isolation.

## Recovery and verification

Each execution makes at most one `tools/call` HTTP exchange. It does not perform
OAuth challenge, protocol-version or generic request replay. Observed failures
have `RetryAdvice::Never`. The node/Driver timeout or cancellation stops local
waiting but cannot prove the remote server stopped.

If the host crashes before committing the result, the pure computation may be
repeated by normal node recovery. Committed pending results are reused without
the Worker. A superseded host fence cannot commit a late response. Remote
results never supply usage; this profile authorizes no paid/provider work, and
its hosting cost is an operator responsibility, not a zero-cost claim.

Against a disposable PostgreSQL 16 or 17 instance:

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
cargo test -p stateknot --test mcp_compute --locked -- --nocapture
```

Set `STATEKNOT_TEST_DATABASE_URL` in the test environment. The required test
fails if it is missing. The owned Worker subprocess receives no inherited
database/provider credentials, runs the first-party MCP server, and is killed
and reaped before pending-result recovery. No application or production
database is an appropriate target for this test.

The general effectful Worker/control-plane API, worker-only SQL grants and
infrastructure isolation certification remain release gates. This profile does
not close them or qualify the complete durable-child feature.
