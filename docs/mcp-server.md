<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP server profile

> Status: implemented pre-alpha profile; public API is not stable.<br>
> Protocol revision: `2026-07-28` only.<br>
> Transport: stateless Streamable HTTP with complete JSON responses and
> request-scoped SSE.<br>
> Explicit exclusion: the MCP Tasks extension is not implemented or claimed.

StateKnot now has a StateKnot-owned MCP server application layer for Tools,
Resources, Resource Templates, Prompts, Completion, and multi-round tool,
resource, or prompt requests (MRTR). The official Rust SDK remains a private
wire adapter; its domain types are not part of StateKnot's public API.

The server is still pre-alpha. This implementation does not make the whole
framework production-ready, stabilize the Rust API, implement an OAuth
authorization server, or add the Tasks extension.

## Production boundary

One request crosses the following boundaries in order:

```text
Host/Origin/body checks
  -> Bearer authentication
  -> replica-wide admission policy
  -> decoded method and resource lookup
  -> scope and operation authorization
  -> schema/argument validation
  -> application handler
  -> bounded and schema-validated result
```

The transport does not trust `Mcp-Method` or `Mcp-Name` as authorization facts.
They are hints until the JSON-RPC request has been decoded and checked. Anonymous
serving is rejected unless every configured host is literal loopback.

`McpServerHttpService` provides:

- exact `2026-07-28` version enforcement and stateless protocol metadata;
- explicit Host and Origin allowlists with no wildcard escape hatch;
- streaming request-body, concurrency, and request-deadline ceilings;
- Bearer authentication with redacted credentials and a fixed RFC 6750/9728
  challenge;
- a caller-owned admission interface for shared quotas and rate limits;
- cooperative shutdown and cancellation propagation;
- complete JSON plus request-scoped SSE, with legacy sessions and `initialize`
  disabled.

Production deployments must use Bearer authentication, TLS at the public edge,
and a cross-replica admission implementation. `anonymous_loopback()` exists only
for local development and hermetic conformance runs.

## Application surfaces

`McpServerApplicationBuilder` composes only configured surfaces and advertises
only their capabilities. Calling an absent surface returns method-not-found;
StateKnot does not advertise an empty placeholder capability.

### Tools

`McpServerToolRegistryBuilder` freezes the executable Tool registry at startup.
It validates portable names, JSON Schema 2020-12 documents, catalog and schema
byte ceilings, duplicates, stable ordering, and a canonical registry digest.
Validators compile offline; unresolved or network `$ref` values fail startup.

Each call is resource-bounded before policy code runs. Authorization then runs
before input-schema diagnostics, preventing a denied principal from probing a
private Tool's schema. The handler runs only after authorization and input
validation. Structured results are checked against the registered output schema
before leaving the process. Text, image, audio, embedded resource, resource
link, and future protocol content are bounded and validated.

Progress is available only when the caller supplied a progress token. Handler
cancellation is cooperative. Tool-level failures remain Tool results; transport,
policy, and handler failures remain protocol errors.

### Resources and templates

The immutable Resource catalog validates absolute URIs, structurally validates
URI templates, enforces catalog ceilings, and freezes a stable digest. Reads are
authorized before existence is disclosed. Text and binary contents have MIME,
Base64, item-count, and aggregate-byte checks. Every result carries an explicit
TTL and public/private cache scope.

### Prompts and Completion

The Prompt catalog validates names, unique bounded arguments, required fields,
scopes, ordering, and a stable digest. Authorization precedes existence and
argument diagnostics. Rendered messages accept bounded StateKnot text, image,
audio, and embedded-resource content.

Completion is optional and therefore advertised only when a provider is
configured. The provider receives a bounded authenticated Prompt or Resource
Template reference, current argument, and string-only context. Results contain
at most 100 unique values with internally consistent pagination metadata. The
provider owns target-specific completion authorization.

## Scope-aware discovery and caching

Tool, Resource, Resource Template, and Prompt discovery filters entries by the
authenticated principal's exact scopes. Private cursors bind the catalog digest,
principal subject, canonical scope set, surface, and offset; a cursor cannot be
replayed across identities or a changed catalog. Startup rejects public caching
when any catalog entry is scope-restricted.

Annotations, descriptions, schemas, server instructions, client capabilities,
completion values, and transport headers are data, never authority.

## Multi-round requests and request state

Tools, Resources, and Prompts can return `input_required` for Elicitation,
Sampling, or Roots input. StateKnot validates request count, IDs, payload size,
client responses, and opaque request-state size.

`McpServerRequestStateCodec` seals application JSON with an explicit keyring,
expiry, and associated data. The caller binds state to the authenticated
principal and exact operation through the request context. Keys must be at least
32 bytes, can rotate, never appear in `Debug`, and have a maximum 24-hour TTL.
Invalid, expired, tampered, or cross-operation state collapses to one public-safe
error.

Do not place secrets in request state. Sealing provides integrity and binding;
the payload still belongs to the application retention and privacy policy.

## Construction outline

The complete executable example remains in crate tests while the crate is
unpublished. The production construction sequence is:

```rust,ignore
let options = McpServerApplicationOptions::new(
    "inventory-mcp",
    "1.0.0",
    100,
    Duration::from_secs(60),
    McpServerCacheScope::Private,
)?;

let app = McpServerApplicationBuilder::new(options)
    .with_tools(tool_registry, tool_authorization)?
    .with_resources(resource_catalog, resource_reader, resource_authorization)?
    .with_prompts(prompt_catalog, prompt_renderer, prompt_authorization)?
    .with_completion_provider(completion_provider)?
    .build()?;

let service = McpServerHttpService::with_admission_control(
    app,
    http_options,
    McpServerAuthentication::bearer(authenticator, bearer_challenge),
    admission_control,
)?;
```

Mount `service` at one exact endpoint in Axum, Hyper, or another compatible
Tower host. Do not create registries per request. Build and validate every
definition before accepting traffic.

## Verification evidence

The frozen official runner is
`@modelcontextprotocol/conformance@0.2.0-alpha.11`, source revision
`c321dd32035556e6769d3724a8ee97d87c3faaac`, against requirement revision
`2026-07-28`.

The strict transport fixture passes all 37 scored Server scenarios exactly:
114 successful assertions, five explicit capability skips, one informational
SSE check, zero failures, and zero warnings. Three pending, unscored JSON Schema
and HTTP-header gates add 32 successful assertions with zero failures or
warnings. StateKnot's application surfaces separately pass real HTTP boundary
tests for capability discovery, pagination, authorization ordering, schema
validation, Tool dispatch, Resource reads, Prompt rendering, Completion, MRTR
binding, and result limits.

```console
cargo test -p stateknot-integrations mcp_server_ --locked
bash conformance/mcp-server/run-2026-07-28.sh
```

The official fixture intentionally mirrors the conformance inventory and uses
the production `McpServerHttpService` transport. It is acceptance evidence, not
an application template. The StateKnot-owned registry and policy layer is
covered by the separate HTTP tests above. Read the exact inventory and claim
rules in [MCP conformance status](mcp-conformance.md).

## Not claimed

- MCP Tasks, task lifecycle, task notifications, or Task/MRTR composition;
- deprecated stateful sessions or the legacy `initialize` flow;
- a bundled OAuth authorization server or identity provider;
- dynamic catalog mutation or list-changed notifications;
- MCP Apps or other extensions;
- a stable Rust API, crates.io release, or SDK-tier certification;
- production qualification of the complete StateKnot framework.
