<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP OAuth client authorization

> Status: implemented pre-alpha surface; no stable API or production support
> promise.<br>
> Profile: interactive OAuth authorization-code flow for the stateless MCP
> `2026-07-28` Tool client.<br>
> Evidence: all 25 scored OAuth client scenarios in the frozen official
> `2026-07-28` requirement set pass with zero failures.

`McpOAuthAuthorization` is a challenge-driven authorization provider for
`McpClient`. It keeps OAuth metadata, registration, PKCE state, credentials,
issuer migration, scope upgrades, and browser handoff outside MCP request
metadata and outside the durable Agent state model.

This is not a generic OAuth library. It binds one OAuth manager to one exact
MCP resource and uses the pinned official MCP Rust SDK authorization engine
behind StateKnot's bounded challenge/replay boundary.

## Implemented profile

- protected-resource metadata discovery from a Bearer
  `WWW-Authenticate` challenge, followed by authorization-server metadata
  discovery;
- pre-registered clients, Client ID Metadata Documents (CIMD), and Dynamic
  Client Registration (DCR) fallback, in that priority order selected by the
  host policy;
- authorization code with PKCE S256 and RFC 8707 `resource` on authorization
  and token requests;
- `client_secret_basic`, `client_secret_post`, and public-client token endpoint
  authentication selected from server metadata;
- challenged scope, metadata scope, omitted scope, bounded step-up, and
  `offline_access` behavior;
- exact issuer migration and RFC 9207 authorization-response issuer checks;
- refresh-token reuse through caller-owned credential storage;
- one authorization challenge replay per MCP logical request by default, with
  a hard maximum of three;
- exact callback scheme, host, effective port, and path binding before code
  exchange; configured redirect URIs cannot contain a query or fragment;
- a 16 KiB hard ceiling for resource, metadata, authorization, redirect, and
  callback URLs.

Client credentials, private-key JWT, Enterprise Managed Authorization, DPoP,
DPoP nonce, Workload Identity Federation, and the post-release JSON Schema
preservation scenario are not implemented or claimed.

## Connect with durable stores

The crate is unpublished. Pin an exact StateKnot revision. The host must own the
user-agent integration and durable stores:

```rust
use std::sync::Arc;

use stateknot_integrations::{
    McpClient, McpClientIdentity, McpClientOptions, McpOAuthAuthorization,
    McpOAuthCredentialStore, McpOAuthOptions, McpOAuthRegistration,
    McpOAuthResource, McpOAuthStateStore, McpOAuthUserAgent, ProviderEndpoint,
};

async fn connect<C, S>(
    user_agent: Arc<dyn McpOAuthUserAgent>,
    credential_store: C,
    state_store: S,
) -> Result<McpClient, Box<dyn std::error::Error>>
where
    C: McpOAuthCredentialStore + 'static,
    S: McpOAuthStateStore + 'static,
{
    let endpoint = ProviderEndpoint::https("https://mcp.example.com/mcp/")?;
    let resource = McpOAuthResource::from_endpoint(&endpoint)?;
    let registration = McpOAuthRegistration::client_metadata_document(
        "https://agent.example.com/oauth/client-metadata.json",
    )?;
    let oauth_options = McpOAuthOptions::native(
        "http://127.0.0.1:49152/callback",
        "Inventory Agent",
        registration,
    )?;
    let authorization = Arc::new(
        McpOAuthAuthorization::new_with_stores(
            resource,
            oauth_options,
            user_agent,
            credential_store,
            state_store,
        )
        .await?,
    );
    let client_options =
        McpClientOptions::default().with_maximum_authorization_retries(1)?;

    Ok(McpClient::connect(
        endpoint,
        McpClientIdentity::new("inventory-agent", "1.0.0")?,
        authorization,
        client_options,
    )
    .await?)
}
```

Use `McpOAuthRegistration::pre_registered` when client credentials were
provisioned out of band. Use `automatic()` only when DCR compatibility is an
accepted deployment policy. A confidential secret is stored in `ApiKey` and
is always redacted from `Debug` output.

## User-agent boundary

Implement `McpOAuthUserAgent::authorize` in the trusted host application. It
receives a validated authorization URL and the exact redirect URI and must
return the complete callback URL.

For a native application:

1. reserve the loopback listener before opening the browser;
2. bind only the configured loopback address, port, and callback path;
3. open the supplied URL without logging it;
4. accept one bounded request and reject every other path or origin;
5. return the complete callback URL, then close the listener.

StateKnot applies an independent five-minute default timeout (configurable up
to a hard 24-hour maximum), checks the callback origin and path exactly, and
then delegates state, code, PKCE, and RFC 9207 issuer validation to the OAuth
session. Cancellation and malformed callbacks fail as permission denial;
listener or timeout failures fail as unavailable.

## Durable store contract

`new()` is intentionally in-memory. Production processes that must survive a
restart use `new_with_stores`. Store implementations are part of the trusted
computing base and must provide:

- encryption at rest and key rotation for access tokens, refresh tokens,
  client secrets, and PKCE verifier material;
- tenant, principal, MCP resource, and authorization-server issuer isolation;
- atomic read/write/delete behavior and compare-before-replace protection;
- TTL expiry and one-time consumption for abandoned authorization state;
- redacted telemetry and audit events that never contain tokens, codes,
  verifier values, callback queries, or authorization URLs.

Never use the in-memory stores as an implicit production durability promise.

## Challenge and retry authority

The first MCP request is anonymous when no usable credential exists. A bounded
401/403 Bearer challenge is copied, parsed, and handled outside the outbound
HTTP attempt timeout. On successful authorization the client creates a new
JSON-RPC ID and replays the same logical MCP method once. It does not replay a
timeout, connection failure, or ambiguous Tool result.

A 403 can trigger scope upgrade only when the challenge explicitly contains
`error="insufficient_scope"` and a scope. Scope-upgrade attempts and transport
replays have independent finite ceilings. An authorization-server issuer
change invalidates registration reuse and starts a new issuer-bound
registration flow.

## Security and operating checklist

Before production use:

1. require HTTPS for MCP, protected-resource metadata, authorization metadata,
   registration, authorization, and token endpoints; use HTTP only for an
   explicitly managed loopback flow;
2. allowlist DNS and egress outside the client and inspect the upstream MCP SDK
   version on every dependency update;
3. publish and pin the exact CIMD document when using CIMD;
4. use a system browser or a hardened managed user agent, never an embedded
   credential-collecting web view;
5. persist credentials only in an encrypted tenant-scoped store;
6. monitor challenge, discovery, registration, refresh, migration, and scope
   failures with redacted identifiers;
7. retain `maximum_authorization_retries(1)` unless a reviewed server contract
   proves another finite value is required;
8. rerun the pinned official requirement gate after any transport, OAuth SDK,
   URL-policy, storage, or callback change.

Reproduce the evidence with:

```console
cargo test -p stateknot-integrations --test mcp_client_contract --locked
bash conformance/mcp-client/run-2026-07-28.sh
```

See [MCP conformance status](mcp-conformance.md) for the exact scored and
not-scored boundary.
