<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# A2A 1.0 server profile

> Status: implemented pre-alpha server profile; public Rust API is not stable.<br>
> Protocol: A2A `1.0`.<br>
> Bindings: HTTP+JSON and JSON-RPC 2.0, including SSE streaming.<br>
> Page scope exclusions: Client behavior/evidence and gRPC binding. The Client
> is implemented and documented separately; gRPC is not implemented.

StateKnot exposes an A2A server without making the official SDK its domain
model. `A2aAgentCard`, messages, parts, tasks, artifacts, status updates, push
configuration, and request types are StateKnot-owned bounded contracts. The
official `a2a-rs` SDK is pinned and kept behind the wire adapter.

This is a real production HTTP boundary, but not a complete production task
backend. Applications provide authentication, authorization, replica-wide
admission, durable task projections, streams, cancellation, and reliable push
delivery through explicit traits.

## Request boundary

Every non-discovery request follows this order:

```text
shutdown / Host / Origin / canonical path / Content-Type / body ceiling
  -> process-local request admission
  -> Bearer authentication (before body parsing)
  -> A2A wire decoding and bounded contract validation
  -> authorization on the decoded operation (before task/config lookup)
  -> caller-owned replica-wide quota admission
  -> durable A2aTaskService operation
  -> bounded response or bounded SSE stream
```

The public Agent Card is served at `/.well-known/agent-card.json` with
`Cache-Control`, `ETag`, `Last-Modified`, and conditional-request support. It is
the only route that bypasses credential authentication. Its advertised
capabilities and mounted interface URLs must match the task service at startup;
a mismatch fails construction.

The boundary accepts no forwarded-host shortcut or wildcard authority. Exact
public `Host` authorities and optional browser Origins are configured
explicitly. Requests without `Origin` remain valid for server-to-server use.
Malformed or duplicate Bearer credentials fail before the body is decoded.

## Implemented operations

| Capability | HTTP+JSON | JSON-RPC | Backend requirement |
| --- | --- | --- | --- |
| Agent Card discovery | yes | shared route | immutable validated snapshot |
| Send message | yes | yes | durable message idempotency and task projection |
| Stream message | SSE | SSE | committed ordered events |
| Get/list task | yes | yes | tenant-scoped stable projections and cursors |
| Cancel task | yes | yes | durable request and race-safe lifecycle transition |
| Subscribe to task | SSE | SSE | snapshot, then committed ordered events |
| Push config CRUD | yes | yes | encrypted secrets and authorization-first lookup |
| Extended Agent Card | yes | yes | authenticated caller-scoped projection |

Unknown JSON fields are ignored as required by A2A's ProtoJSON model, while
unknown enum values, invalid lifecycle combinations, noncanonical routes,
unsupported versions, oversized values, unbounded collections, and invalid
media or URL fields fail closed. REST errors use the AIP-193 error shape and
canonical HTTP statuses; JSON-RPC errors use the A2A-defined code mappings.

## Durable service contract

`A2aTaskService` is deliberately storage-neutral. A production implementation
must satisfy these invariants:

- derive tenant and subject only from `A2aRequestContext`; never trust a body
  tenant override or an Agent Card description as authority;
- deduplicate accepted messages by a durable caller/message identity and return
  the original committed result after a lost acknowledgement;
- map opaque A2A task/context IDs to internal runs without exposing internal
  identifiers or allowing cross-tenant lookup;
- page from a stable snapshot and bind continuation tokens to tenant, filters,
  ordering, and the snapshot boundary;
- source streams from committed journal/outbox data, emit one ordered snapshot
  first for subscriptions, and retain the operation permit for the full stream;
- commit cancellation intent before reporting it and resolve completion versus
  cancellation under the durable lifecycle fence;
- encrypt push credentials at rest, redact them from logs, validate destination
  policy against SSRF and DNS-rebinding risks, and dispatch through a
  transactional at-least-once outbox with bounded retry and dead-letter policy;
- return `Unavailable` on ambiguous infrastructure state unless an authoritative
  read proves the committed outcome.

The repository's TCK fixture intentionally uses process-local memory and
loopback-only webhooks. It is compatibility test input, not an implementation
of this durability contract.

## Assemble the server

Implement the four policy/application traits, construct one immutable Agent
Card, and build the router once at process startup:

```rust,ignore
let card = A2aAgentCard::builder(
    "orders-agent",
    "Creates and tracks authorized orders.",
    "1.4.2",
)?
.capabilities(
    A2aAgentCapabilities::new()
        .streaming(true)
        .push_notifications(true)
        .extended_agent_card(true),
)
.interface(A2aAgentInterface::new(
    "https://agents.example.com/a2a/rest",
    A2aBinding::HttpJson,
)?)?
.interface(A2aAgentInterface::new(
    "https://agents.example.com/a2a/jsonrpc",
    A2aBinding::JsonRpc,
)?)?
.default_input_modes(vec!["application/json".into()])?
.default_output_modes(vec!["application/json".into()])?
.skill(A2aAgentSkill::new(
    "create-order",
    "Create order",
    "Creates an order under local authorization policy.",
    vec!["orders".into()],
)?)?
.build()?;

let options = A2aServerHttpOptions::new()
    .with_allowed_authorities(["agents.example.com"])?
    .with_allowed_origins(["https://console.example.com"])?
    .with_limits(1_048_576, 1_024, 512)?
    .with_maximum_response_body_bytes(8_388_608)?
    .with_timeouts(Duration::from_secs(30), Duration::from_secs(60))?
    .with_maximum_stream_events(16_384)?
    .with_bearer_challenge("Bearer realm=\"stateknot-a2a\"")?;

let shutdown = CancellationToken::new();
let server = A2aServer::new(
    card,
    authenticator,
    authorizer,
    shared_admission,
    durable_task_service,
    options,
    shutdown.clone(),
)?;
let listener = TcpListener::bind("127.0.0.1:8080").await?;
axum::serve(listener, server.router())
    .with_graceful_shutdown(shutdown.cancelled_owned())
    .await?;
```

TLS and trusted proxy termination belong outside this router. Configure the
allowlist with the authority actually visible to the application; do not trust
client-supplied forwarding headers unless an outer trusted layer has replaced
the request authority.

## Deployment gates

Before exposing traffic, verify all of the following:

- issuer, audience, resource, expiry, delegation, and revocation validation in
  `A2aServerAuthenticator`;
- authorization tests that prove denied task/config IDs are not disclosed;
- shared quota/admission behavior across replicas;
- database-backed idempotency, cancellation races, stream replay, retention,
  push encryption, egress policy, retry, and dead-letter recovery;
- graceful drain long enough for accepted unary operations, with resumable SSE
  clients after termination;
- external TLS, request logging with secrets removed, metrics, traces, and alert
  thresholds for overload, auth failures, stream expiry, and push backlog;
- the exact [A2A conformance gate](a2a-conformance.md) plus application-specific
  failure injection and restore tests.

## Not claimed

- A2A client behavior or durable outbound-agent invocation, which belongs to
  the separate [A2A Client profile](a2a-client.md);
- gRPC transport;
- a bundled identity provider, policy engine, database task service, or push
  dispatcher;
- stable API compatibility, crates.io publication, or a production-ready
  StateKnot release.
