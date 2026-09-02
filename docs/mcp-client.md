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
implement MCP Server, Resources, Prompts, Tasks, Roots, Sampling, client
credentials, DPoP, or a stable SDK tier. Static bearer credentials remain
transport credentials, not an OAuth flow.

## Connect, discover, and call

The crate is unpublished. Consumers must currently pin an exact repository
revision or work inside this workspace.

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
373 scored assertions succeeded and none failed. Eleven optional or
unimplemented-method checks were skipped. Seven explicitly not-scored
extensions are reported separately and remain outside StateKnot's client,
server, extension, and SDK-tier claims.
