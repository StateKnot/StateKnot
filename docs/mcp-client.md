<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# General stateless MCP Tool client

> Status: implemented pre-alpha surface; no stable API or production support
> promise.<br>
> Wire profile: MCP `2026-07-28`, stateless Streamable HTTP, Tool client only.<br>
> Evidence: mandatory local HTTP/SSE contract plus the pinned official runner
> gate described in [MCP conformance status](mcp-conformance.md).

`McpClient` is the interoperability-oriented MCP client in
`stateknot-integrations`. It is deliberately separate from
[`McpRemoteTool`](mcp-remote-tool.md), whose reviewed identity/schema pins and
durable reconciliation rules are stronger than a dynamic catalog can provide.

Use `McpClient` to discover and call ordinary remote Tools. Use
`McpRemoteTool` when an external write participates in a durable Agent run and
must preserve admission, ambiguity, and reconciliation evidence.

## Implemented contract

- one immutable HTTPS endpoint, or literal-loopback HTTP for managed sidecars
  and tests;
- stateless `server/discover`, bounded `tools/list` pagination, and
  `tools/call`;
- required per-request `_meta`, `MCP-Protocol-Version`, `Mcp-Method`, and
  applicable `Mcp-Name` headers;
- JSON and request-scoped SSE responses, including bounded notifications before
  the matching final JSON-RPC response;
- nested `x-mcp-header` projection for string, integer, and boolean arguments,
  with the protocol Base64 sentinel for unsafe header bytes;
- invalid annotated Tools excluded individually with an auditable reason;
- multi-round Tool requests (MRTR): exact opaque `requestState`, fresh JSON-RPC
  IDs, exact response-key matching, and isolation between concurrent calls;
- request-scoped authorization resolution, hard resource ceilings, no
  redirects, and no generic HTTP retry;
- bounded 401/403 Bearer challenge capture and explicit authorization-provider
  recovery with a fresh JSON-RPC ID;
- JSON Schemas retained as untrusted bounded values without network `$ref`
  dereferencing.

The separate [`McpOAuthAuthorization`](mcp-oauth.md) provider now implements the
interactive OAuth authorization-code profile. This surface still does not
implement Resource/Prompt clients, Tasks, Roots, Sampling, client credentials,
DPoP, or a stable SDK tier. The separate [MCP Server profile](mcp-server.md)
does not broaden this Client claim. Static bearer credentials remain transport
credentials, not an OAuth flow.

## Connect, discover, and call

Use the exact public-preview crate version. Pre-release compatibility rules
require an exact pin:

```toml
stateknot-integrations = "=0.1.0-alpha.1"
```

```rust
use std::sync::Arc;

use serde_json::json;
use stateknot_integrations::{
    ApiKey, McpClient, McpClientIdentity, McpClientOptions, McpToolCall,
    ProviderEndpoint, StaticMcpBearerAuthorization,
};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let endpoint = ProviderEndpoint::https("https://mcp.example.com/mcp/")?;
let token = std::env::var("MCP_TOKEN")?;
let client = McpClient::connect(
    endpoint,
    McpClientIdentity::new("inventory-agent", "1.0.0")?,
    Arc::new(StaticMcpBearerAuthorization::new(ApiKey::new(token)?)),
    McpClientOptions::default(),
)
.await?;

let catalog = client.list_tools().await?;
for rejected in catalog.rejected_tools() {
    eprintln!("excluded MCP Tool {:?}: {}", rejected.name(), rejected.reason());
}

let lookup = catalog.find("inventory_lookup").ok_or("Tool is unavailable")?;
let response = client
    .call_tool(lookup, json!({ "sku": "STK-001" }))
    .await?;

for notification in response.notifications() {
    println!("notification method: {}", notification.method());
}

match response.into_outcome() {
    McpToolCall::Complete(result) => {
        if result.is_error() {
            eprintln!("the remote Tool completed with a Tool-level error");
        }
        println!("content blocks: {}", result.content().len());
    }
    McpToolCall::InputRequired(_) => {
        eprintln!("the Tool requires an application-mediated follow-up");
    }
    _ => return Err("unsupported future MCP Tool outcome".into()),
}
# Ok(())
# }
```

Tool descriptors, descriptions, annotations, content, structured content,
notifications, server instructions, and schemas remain untrusted. A host must
apply its own policy and schema validation before exposing them to a model,
user, filesystem, network, or durable state.

## Multi-round Tool requests

An `input_required` result becomes `McpInputRequired`. The pending value owns
the original client, Tool, arguments, and exact opaque request state. Calling
`resume` consumes it, preventing safe-API reuse of the same state.

The host must inspect every entry in `input_requests()` and route only supported
methods to the correct trusted subsystem. It must then return exactly one entry
for every requested key; missing or extra keys are rejected locally. StateKnot
does not auto-approve elicitation, invent Roots, or silently invoke Sampling.

```rust
use serde_json::{Map, Value};
use stateknot_integrations::McpToolCall;

# async fn resume(
#     pending: stateknot_integrations::McpInputRequired,
#     reviewed: Map<String, Value>,
# ) -> Result<(), stateknot_integrations::StatelessMcpClientError> {
let response = pending.resume(reviewed).await?;
match response.into_outcome() {
    McpToolCall::Complete(_) | McpToolCall::InputRequired(_) => {}
    _ => {}
}
# Ok(())
# }
```

Each new round receives a fresh JSON-RPC ID. When the server supplied
`requestState`, StateKnot echoes its exact string bytes and never exposes them
for application reconstruction. When it was absent, the retry omits it.

## Transport, authorization, and retry authority

Production endpoints require HTTPS. HTTP is available only through
`ProviderEndpoint::loopback_http`, which accepts a literal loopback IP and
rejects `localhost`, credentials in URLs, queries, and fragments. Redirects and
Reqwest retries are disabled.

`McpClientAuthorizationProvider::resolve` runs independently for every POST,
so a production credential provider can rotate short-lived tokens without
placing secrets in `_meta`, logs, or debug output. `AnonymousMcpAuthorization`
and `StaticMcpBearerAuthorization` cover unauthenticated and fixed-token
deployments. `McpOAuthAuthorization` additionally handles bounded Bearer
challenges, protected-resource and authorization-server discovery,
pre-registration/CIMD/DCR, PKCE, token refresh, issuer migration, scope
step-up, and exact callback validation. Production restart recovery requires
caller-owned encrypted credential and expiring PKCE-state stores.

The client performs no automatic retry after timeout, connection failure, or
ambiguous Tool dispatch. Protocol version negotiation receives one retry when
the server explicitly advertises `2026-07-28`. Authorization challenge recovery
receives one replay by default only after the provider explicitly authorizes
it; the replay uses a fresh JSON-RPC ID and has a hard maximum of three.
External writes that need recovery guarantees belong behind `McpRemoteTool`
and the durable invocation executor.

## Default resource ceilings

Defaults are finite and can be reduced through `ProviderHttpOptions` and
`McpClientOptions`:

| Resource | Default |
| --- | ---: |
| Logical request deadline | 30 seconds |
| Concurrent requests per client | 16 |
| Catalog pages / advertised entries | 16 / 1,024 |
| Request / complete JSON response | 16 MiB / 2 MiB |
| SSE line / event / total stream | 512 KiB / 2 MiB / 64 MiB |
| Notifications per response | 1,024 |
| Authorization replays / challenge bytes | 1 / 64 KiB |

Hard implementation ceilings prevent configuration from silently becoming
unbounded; the logical request deadline cannot be configured above 24 hours.
Cursor cycles, duplicate usable Tool names, foreign-client Tool
descriptors, unsafe integer headers, oversized payloads, mismatched response
IDs, independent server requests on SSE, and malformed result types fail
closed through `StatelessMcpClientError`.

## Unreleased response diagnostics

The unreleased HTTP client redacts both `Debug` and pretty `Debug` for exactly
these four response types:

| Type | Fields retained in Debug |
| --- | --- |
| `McpNotification` | `has_params` |
| `McpCompleteToolResult` | `content_count`, `has_structured_content`, `is_error` |
| `McpRemoteError` | original i32 `code`, `has_data` |
| `McpToolCallResponse` | fixed `outcome` label (`complete` or `input_required`), `notification_count` |

Each summary also has its fixed type name and non-exhaustive marker. It formats
no remote method/message, content, structured/raw value, data, request state or
Tool metadata. The outer response deliberately does not recursively format its
outcome or notifications.

This scope is precise: directly formatting `McpInputRequired` or bare
`McpToolCall::InputRequired` retains the existing Tool-metadata behavior. Other
MCP types receive no general payload-free guarantee. Explicit raw/content/
notification/error getters still return untrusted data, and existing Clone and
PartialEq behavior is unchanged. Opaque state remains private; an explicit
`resume` still echoes its original bytes, arguments and exact response keys.
No wire, authentication, retry, resource limit or durable effect mapping changes.
In particular, the HTTP malformed-complete write path retains `Applied + Never`;
this does not adopt the separate proposed stdio outcome mapping.

[The bounded diagnostic record](qualification/experimental-mcp-http-diagnostics-2026-10-16.md)
and [machine receipt](qualification/experimental-mcp-http-diagnostics-2026-10-16.json)
bind 928-file source `31a5ff55…` and 536 compilation inputs `1ec97f72…`.
Its six local gates pass on Rust1.88/original opt0/default stack: 99 workspace
suites, 2,099 passed/0 failed/6 original ignored, 48 documentation tests, strict
Clippy/Rustdoc and fixed nightly-2026-10-07, with formatting checked. The added
real-TCP family completes eight JSON/fragmented-SSE outcome branches, including
both Tool `isError` values, an explicit MRTR resume and JSON-RPC errors. Both
formatting styles exclude nested canaries while raw getter values/serialized
bytes, existing Clone/PartialEq and actual resume wire remain intact.

The original canary run on unchanged Debug behavior fails at its first JSON
Complete branch (0 passed/1 failed/2 filtered). The fixed candidate's initial
Clippy test-helper warning also remains; the final fixture borrows its JSON ID,
with no new lint allowance or production change. These failures are preserved
with the final evidence. Earlier bound-state PostgreSQL 16/17 full 15-test
serial-suite results belong to their exact source and do not qualify this
changed tree. This tree separately passes both complete targeted Native suites
on each of PostgreSQL 16.15 and 17.11: `mcp_durable` (6 tests, including A2A/MCP)
and `mcp_reconciliation` (10 tests, including definition/wire cases). Both fresh
default-feature binaries use opt0/debug0, debug assertions and overflow checks,
with serial execution and the default stack; all seven complete JSON evidence
markers match on each major. These 32 harness executions are not 32 database
cases. Conditional DB workspace entries are not Native evidence; full Store/
Runtime Native, full CI, fresh official conformance, deployment, stdio and R1
qualification remain separate.

## Verification and operating checklist

```console
cargo test -p stateknot-integrations --test mcp_client_contract --locked
bash conformance/mcp-client/run-2026-07-28.sh
```

Before deployment:

1. pin the StateKnot revision and the MCP wire profile;
2. allowlist an HTTPS endpoint and control DNS/egress outside the client;
3. use a rotating request-scoped provider, or the OAuth provider with encrypted
   tenant-scoped stores, where tokens expire;
4. treat discovery and Tool output as untrusted input;
5. set limits below downstream model, proxy, and storage limits;
6. classify Tool side effects before deciding whether a call may be retried;
7. use the strict durable binding for writes requiring reconciliation;
8. monitor rejected Tools, protocol failures, timeouts, and Tool-level
   `isError` results without logging credentials or sensitive payloads.

The pinned official evidence covers all 32 scored client scenarios in the
frozen `2026-07-28` requirement set, including all 25 scored OAuth scenarios:
371 scored assertions succeeded and none failed. Eleven optional or
unimplemented-method checks were skipped. Seven explicitly not-scored
extensions are reported separately and remain outside StateKnot's client,
server, extension, and SDK-tier claims.
