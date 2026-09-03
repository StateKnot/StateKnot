<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# A2A 1.0 client and durable remote-agent profile

> Status: implemented pre-alpha profile; the Rust API is not stable.<br>
> Bindings: HTTP+JSON and JSON-RPC 2.0, including SSE.<br>
> Explicit exclusions: gRPC, autonomous public-agent trust, automatic send
> retries, automatic reconciliation, API-key/Basic/mTLS security profiles, and
> an official client-conformance claim.

This profile has two deliberately separate layers:

- `A2aClient` is a strict, stateless implementation of all eleven A2A 1.0
  client operations; and
- `A2aRemoteAgent` freezes one discovered skill as a StateKnot `ErasedTool`, so
  `DurableInvocationExecutor` and the PostgreSQL invocation ledger remain the
  only dispatch and recovery authority.

The separation matters. Read/list/stream administration can use the client
directly, while a graph-side message submission must use the durable adapter.
Calling `send_message` directly from recoverable graph code would bypass the
durable-before-dispatch record and is not a supported production composition.

## Implemented operation surface

| Operation | HTTP+JSON route below the interface | JSON-RPC method | Result |
| --- | --- | --- | --- |
| Send message | `POST message:send` | `SendMessage` | task or message |
| Stream message | `POST message:stream` | `SendStreamingMessage` | ordered SSE events |
| Get task | `GET tasks/{id}` | `GetTask` | task |
| List tasks | `GET tasks` | `ListTasks` | bounded task page |
| Cancel task | `POST tasks/{id}:cancel` | `CancelTask` | task |
| Subscribe to task | `POST tasks/{id}:subscribe` | `SubscribeToTask` | ordered SSE events |
| Create push config | `POST tasks/{id}/pushNotificationConfigs` | `CreateTaskPushNotificationConfig` | push config |
| Get push config | `GET tasks/{id}/pushNotificationConfigs/{config}` | `GetTaskPushNotificationConfig` | push config |
| List push configs | `GET tasks/{id}/pushNotificationConfigs` | `ListTaskPushNotificationConfigs` | bounded page |
| Delete push config | `DELETE tasks/{id}/pushNotificationConfigs/{config}` | `DeleteTaskPushNotificationConfig` | empty result |
| Extended Agent Card | `GET extendedAgentCard` | `GetExtendedAgentCard` | Agent Card |

Every request carries `A2A-Version: 1.0`. Negotiated extension URIs are carried
in `A2A-Extensions` in their configured order. If the selected interface has a
tenant, HTTP+JSON inserts it as the first path segment and uses the protocol
tenant field where the request model defines one; JSON-RPC carries it in
`params`.

SSE accepts only standard message/error events. The first success event must be
one Task, or the sole Message for a streaming send; only status and artifact
updates may follow a Task. Every update must retain the exact task/context
identity. Artifact appends require an already established, unsealed artifact
ID, and no event may follow a terminal or interrupted state. JSON-RPC version,
request ID, and the exclusive presence of `result` or `error` are checked for
every unary response and stream event. A legitimate `result: null` remains
distinct from a missing result. Duplicate JSON keys, invalid union wrappers,
cross-resource responses, oversized data, and premature or idle streams fail
closed.

Unary sends also enforce execution mode: unless `returnImmediately` is true, a
Task response must be terminal or interrupted. Task pages enforce the requested
filters, the inclusive `statusTimestampAfter` boundary, unique task IDs,
newest-status-first ordering, artifact projection rules, and response page
size. Standard HTTP+JSON errors expose an authoritative A2A code only when the
HTTP status, `google.rpc.Status`, and official `ErrorInfo` identity agree.

## Freeze discovery before execution

`A2aClient::discover` performs one bounded public Agent Card exchange and then
freezes the result. It:

1. permits HTTPS in production and literal loopback HTTP only through the
   explicit test/sidecar constructor;
2. rejects URL credentials, queries, fragments, redirects, and implicit
   retries;
3. validates the Agent Card with StateKnot-owned bounded contracts;
4. selects the first supported A2A 1.0 HTTP interface in server preference
   order;
5. requires that exact binding and URL to appear in the local egress allowlist;
6. checks every selected and required extension; and
7. checks that the configured anonymous or complete, single-scheme
   bearer-compatible security alternative satisfies the card.

The implemented authenticated profile covers HTTP Bearer, OAuth 2.0, and
OpenID Connect declarations carried as bearer tokens. Agent Cards requiring API
keys, Basic authentication, mutual TLS, or an AND-group with multiple schemes
are rejected rather than partially authenticated.

Use normal TLS server-identity validation together with the exact interface pin
when DNS and certificate operations are trusted. For a separately provisioned
card, `CanonicalSha256` additionally requires the RFC 8785 canonical Agent Card
digest. A card's embedded signature is parsed as data; it is not treated as a
trust anchor without an application-owned signature policy.

```rust,no_run
use std::sync::Arc;
use stateknot_integrations::{
    A2aAgentCardEndpoint, A2aAgentCardTrust, A2aBinding, A2aClient,
    A2aClientInterfacePin, A2aClientOptions, A2aClientSecurity,
};

let client = A2aClient::discover(
    A2aAgentCardEndpoint::https(
        "https://partner.example/.well-known/agent-card.json",
    )?,
    vec![A2aClientInterfacePin::https(
        "https://partner.example/a2a",
        A2aBinding::HttpJson,
    )?],
    A2aAgentCardTrust::TlsServerIdentity,
    A2aClientSecurity::bearer("oidc", Arc::new(token_provider))?,
    vec![],
    A2aClientOptions::default(),
).await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The token provider runs for every authenticated operation. Its request includes
the card digest, binding, operation, remote routing tenant, target task ID,
required card scopes, and—when called through `A2aRemoteAgent`—the local tenant,
run, invocation, and physical attempt IDs. Production providers should mint or
retrieve short-lived tokens for that exact tuple. Tokens, remote response text,
and response bodies are not retained in public errors or `Debug` output.

`GetExtendedAgentCard` is never dispatched by an anonymous client. Its result
is bounded and revalidated, but it is returned as data: it does not mutate the
immutable client, change egress/security pins, or silently rebind a Tool. To
adopt an extended card, review it and construct a new pinned client/binding.

## Bind a remote skill to durable execution

Agent Cards advertise media, security, and descriptive skill metadata, but A2A
1.0 has neither JSON Schema arguments for a skill nor a send-request field that
routes a message to a skill. Therefore:

- `skill_id` freezes discovery/security/media evidence only;
- StateKnot sends the validated tool input as one `application/json` data part;
- the receiving agent chooses behavior from the user message;
- a trusted local `A2aSchemaRegistry` is authoritative for both input and
  output; and
- output is the standard one-key `{"message": ...}` or `{"task": ...}`
  projection, validated against the pinned local output schema.

```rust,no_run
use std::sync::Arc;
use stateknot_integrations::{A2aRemoteAgent, A2aRemoteAgentDelivery};
use stateknot_runtime::ToolProviderRegistryBuilder;

let remote = A2aRemoteAgent::bind(
    descriptor,
    client,
    "answer",
    A2aRemoteAgentDelivery::AtMostOnce,
    schemas,
)?;

let mut tools = ToolProviderRegistryBuilder::new();
tools.register(Arc::new(remote))?;
let tools = tools.build();
# Ok::<(), Box<dyn std::error::Error>>(())
```

Binding fails unless the descriptor exactly matches the executable behavior:
read/write network access, no filesystem or dynamic code, cooperative
cancellation, no progress events, matching credential requirement, no status
query or compensation claim, and both local schemas present.

## Delivery and recovery contract

A2A 1.0 does not promise that a receiver deduplicates `messageId`. Choose one
deployment truth and encode the same truth in the `ToolDescriptor`:

| Delivery | Message ID | Required descriptor semantics | Recovery rule |
| --- | --- | --- | --- |
| `AtMostOnce` | `stateknot-attempt-{attempt_id}` | non-idempotent write + idempotency unsupported | never repeat an uncertain physical attempt |
| `MessageIdDeduplicated` | `stateknot-invocation-{idempotency_key}` | idempotent write + required key | safe only with operator evidence for durable remote deduplication |

`MessageIdDeduplicated` is an operator assertion, not an inference from the
Agent Card. The remote system must durably retain and deduplicate that ID for at
least the complete local invocation-ledger retention and disaster-recovery
window. Document the remote key scope, conflict behavior, retention, replica
consistency, backup behavior, and evidence test before enabling this mode.

The durable execution sequence is:

```text
PostgreSQL prepared/executing revision
  -> exact physical attempt and message ID
  -> one A2A send, with no client retry
  -> validate remote task/message and local output schema
  -> PostgreSQL committed/failed/unknown terminal revision
```

The adapter deliberately sets `returnImmediately: true`. A valid Task response
therefore commits the **message submission and returned durable handle**, not a
claim that the remote task reached a terminal A2A state. Waiting for remote
completion requires an application-owned, separately authorized `get_task` or
`subscribe_to_task` workflow. The adapter consequently advertises neither a
status-query capability nor terminal-task completion semantics.

Cancellation or deadline before possible dispatch is `NotStarted`. After
dispatch may have begun, a timeout, cancellation, lost connection, invalid
response, HTTP ambiguity, or non-authoritative remote error becomes
`Unknown + ReconcileFirst`; StateKnot does not send again. Only protocol errors
that authoritatively reject the request before application—parse error,
invalid request/params, method/operation/content/extension/version
unsupported—become `NotApplied + Never`.

Recovery of `Unknown` requires an application-owned, separately authorized
reconciliation workflow. This adapter deliberately does not claim that an A2A
task ID is available after a lost send response.

## Default resource policy

| Limit | Default | Hard ceiling |
| --- | ---: | ---: |
| Connect timeout | 10 s | transport policy |
| Discovery timeout | 15 s | 15 min |
| Unary / stream-establishment deadline | 60 s | 15 min |
| Stream idle timeout | 60 s | 15 min |
| Request body | 16 MiB | 32 MiB |
| Unary response body | 2 MiB | 2 MiB |
| SSE line / event / total | 512 KiB / 2 MiB / 64 MiB | 2 MiB / 2 MiB / 72 MiB |
| SSE events | 4,096 | 65,536 |
| Task page | 50 default | 100 (A2A 1.0) |
| Push-config page | 50 default | 256 (local ceiling) |

StateKnot's protocol contracts add their own lower structural limits, including
a 1 MiB Agent Card/data-part ceiling, 128 parts per message or artifact, 256
messages of history, and 128 task artifacts. Configure lower limits for the
actual use case; never increase them merely to accept an unbounded peer.

## Production gate checklist

- Maintain an exact destination/binding allowlist and restrict DNS, proxy, and
  sidecar egress independently of application validation.
- Terminate with verified TLS; do not use loopback HTTP across a host or trust
  boundary.
- Pin and review Agent Card changes, skill media, scopes, extensions, tenant,
  and security alternatives before rollout.
- Use an attempt-scoped secret manager or workload-identity token provider;
  never place bearer tokens in config files, URLs, metadata, logs, or traces.
- Register the adapter only through the durable invocation executor and keep
  PostgreSQL retention longer than every remote deduplication/reconciliation
  window.
- Alert on `Unknown`, reconciliation backlog, card-digest drift, invalid remote
  contracts, authorization denial/unavailability, stream limits, and deadline
  exhaustion without recording payloads or credentials.
- Fault-test accepted-request/lost-response, cancellation races, malformed JSON
  and SSE, stale cards, wrong interfaces, remote deduplication, PostgreSQL
  failover, and restore before production traffic.

## Executable evidence and claim boundary

```console
cargo test -p stateknot-integrations --test a2a_client_contract --locked

STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL='postgres://...' \
cargo test -p stateknot-integrations --test mcp_durable \
  a2a_send_is_durable_before_dispatch_and_unknown_is_not_redispatched \
  --locked -- --test-threads=1
```

The loopback suite exercises all eleven operations over both HTTP+JSON and
JSON-RPC, both SSE surfaces, tenant/header mapping, attempt-scoped
authorization, card/interface drift, strict errors, stream bounds, and lost
responses. The PostgreSQL test proves the executing revision exists before
request dispatch, the lost response commits `Unknown`, and replay does not send
a second message.

This is implementation evidence, not an official A2A client certification.
The frozen official TCK evidence currently applies to the separate
[A2A Server profile](a2a-server.md). Stable API review, live-partner
qualification, gRPC, automatic reconciliation, and production durable
server-side task/push storage remain independent release gates.
