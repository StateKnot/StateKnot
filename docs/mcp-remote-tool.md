<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Strict MCP remote Tool profile

`McpRemoteTool` is the first implemented MCP boundary in StateKnot. It adapts
one remotely discovered MCP Tool to the protocol-neutral `ErasedTool` contract
without allowing remote wire metadata to redefine local risk, schema, retry,
or durability semantics.

This is deliberately not a claim of complete MCP support. The implemented
profile is an MCP **client-side remote Tool binding** for protocol version
`2026-07-28`, modern discovery, stateless Streamable HTTP, and complete JSON
responses.

## Supported profile

- exact protocol version `2026-07-28`;
- modern `server/discover` startup followed by bounded `tools/list` paging;
- stateless Streamable HTTP over verified HTTPS, or literal-loopback HTTP for
  explicitly managed local sidecars and tests;
- complete JSON responses only;
- one exact remote Tool name and one expected server name/version per binding;
- exact RFC 8785 byte equality between remotely discovered input/output
  schemas and the authoritative local registry;
- anonymous or bearer authorization, with an extension trait for attempt-
  scoped secret resolution;
- bounded request/response bodies, discovery pages, discovered Tools, startup
  time, shutdown time, and concurrent calls;
- local input validation before dispatch and local structured-output validation
  after response;
- read/write ambiguity mapped into StateKnot's durable Tool failure model.

The adapter uses the official MCP Rust SDK `3.2.0`, pinned exactly in the
workspace. StateKnot's MSRV is therefore Rust `1.88.0`.

## Freeze one binding

The local `ToolDescriptor` and schema registry are authoritative. Construct the
descriptor from reviewed configuration, not from untrusted MCP annotations.

```rust
use std::sync::Arc;
use stateknot_integrations::{
    McpHttpOptions, McpRemoteTool, McpServerIdentity, ProviderEndpoint,
    StaticMcpBearerAuthorization,
};

let adapter = McpRemoteTool::connect(
    local_descriptor,
    "lookup_issue",
    ProviderEndpoint::https("https://mcp.example.com/v1/")?,
    McpServerIdentity::new("issue-service", "2026.09.0")?,
    Arc::new(schema_registry),
    Arc::new(StaticMcpBearerAuthorization::new(api_key)),
    McpHttpOptions::default(),
)
.await?;
```

`connect` performs discovery once, verifies the exact negotiated version,
requires the Tools capability, checks the self-reported implementation
name/version, scans the bounded catalog, and compares canonical schemas. A
server upgrade or schema change requires constructing and registering a new
binding.

TLS authenticates the configured endpoint according to the platform trust
store. The MCP implementation name/version is self-reported discovery metadata;
pinning it detects drift but is not cryptographic server attestation. Deployers
that need stronger identity must add mTLS or an authenticated gateway outside
this adapter.

## Authorization and secrets

`StaticMcpBearerAuthorization` is suitable only for a controlled, single-tenant
binding. Multi-tenant deployments should implement `McpAuthorizationProvider`
and resolve a secret handle for startup and for each admitted durable attempt.

The adapter:

- resolves attempt credentials only after it owns the per-binding call gate;
- never copies the credential into MCP metadata;
- redacts authorization objects from `Debug` output;
- resets the in-memory authorization slot after each exchange;
- disables redirects and transport retries.

Secret retrieval failure occurs before dispatch. It is never converted into an
ambiguous write outcome.

## Call and failure semantics

Each admitted Tool attempt issues exactly one `tools/call`. The response must be
complete, must not declare `isError`, must carry `structuredContent`, and may
contain only text content blocks. Structured output is bounded and validated
against the pinned local output schema before a `ToolResult` is returned.

Remote Tool annotations are untrusted hints and do not override the local
descriptor. In particular, `readOnlyHint`, `destructiveHint`, and remote schema
text cannot silently change StateKnot's policy decision.

For a locally declared write, any transport/protocol loss after dispatch maps
to:

- `ToolExternalEffect::Unknown`;
- `FailureCategory::AmbiguousExternalOutcome`;
- `RetryAdvice::ReconcileFirst`.

The runtime must reconcile through a status query, an intrinsic provider key,
compensation, or a human decision before another write. The adapter never hides
an uncertain write as a safe retry.

## Durable reconciliation

`ToolReconciliationHandoff::result` and `ToolReconciliationHandoff::error`
bind authoritative evidence to the exact durable `Unknown` invocation and
physical attempt. Construction reuses the core invocation, tool identity,
attempt, output-schema, result-limit, artifact-ownership, risk/effect, and retry
safety invariants.

```rust,ignore
let handoff = ToolReconciliationHandoff::result(
    live_fence,
    unknown_invocation,
    EventId::generate(),
    authoritative_result,
)?;
let outcome = executor.commit_tool_reconciliation(handoff).await?;
```

Before a result commit, the runtime also validates its inline output against
the exact frozen local schema registry. The executor then appends a distinct
reconciliation audit event and advances the Tool ledger in one fenced
PostgreSQL transaction. It never resolves or calls the MCP adapter. Retrying the
same handoff/event is exactly idempotent. `rebind_fence` may attach retained
evidence to a newer live fence in the same run after lease takeover.

Error evidence with an authoritative known external effect resolves the ledger
to `Failed`. Evidence whose effect is still `Unknown` deliberately leaves it
unresolved. This API is a trusted worker/operations boundary; an HTTP or RPC
service must authorize evidence submission before constructing the handoff.

## Deliberately rejected today

- stateful MCP sessions and `Mcp-Session-Id`;
- SSE responses and legacy initialization fallback;
- automatic reconnect, transparent reinitialization, or HTTP retry;
- MRTR, Tasks, incomplete results, progress forwarding, and artifact/resource
  materialization;
- image, audio, embedded-resource, and resource-link result blocks;
- descriptors that require a StateKnot idempotency key, because generic MCP has
  no safe standard field into which that durable key can be injected;
- exposing StateKnot as an MCP server;
- roots, prompts, resources, sampling, elicitation, logging, or MCP Apps.

These exclusions fail at binding or result validation. They are not silently
downgraded.

## Operational checklist

1. Review and version the local `ToolDescriptor`, including risk, resource
   access, idempotency, status-query, and compensation semantics.
2. Register canonical input and output schemas locally before connecting.
3. Use one exact HTTPS endpoint and expected server identity per binding.
4. Keep production credentials in a vault-backed
   `McpAuthorizationProvider`; never embed them in descriptors or logs.
5. Treat `connect` as startup/readiness work. A discovery failure keeps the
   deployment unready.
6. Register the connected adapter in the immutable StateKnot Tool registry.
7. Monitor build failures, authorization failures, latency, response bounds,
   and ambiguous writes without logging request bodies or credentials.
8. Roll out a server/schema change as a new reviewed binding; do not mutate a
   live binding in place.

## Executable evidence

The literal-loopback contract suite proves exact modern discovery, schema and
identity pinning, attempt authorization headers, one-call behavior, remote
annotation distrust, startup schema-drift rejection, and ambiguous lost write
responses:

```console
cargo test -p stateknot-integrations --test mcp_contract --locked
cargo test -p stateknot-integrations --test mcp_durable --locked -- --test-threads=1
```

The durable suite uses a real PostgreSQL store and pauses a real loopback MCP
request to observe `Executing` before I/O completes. It then proves ambiguous
write persistence, duplicate suppression, authoritative reconciliation, and
idempotent replay with one network call. CI runs it on PostgreSQL 16 and 17.

These suites are profile evidence, not a complete official MCP client/server
pass. See the pinned, explicit [MCP conformance status](mcp-conformance.md) for
the official requirement inventory and the reason this strict profile is not
misreported as a general client.
