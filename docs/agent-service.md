<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# AgentService v1

`AgentServiceV1` is the implemented, versioned embedding boundary for durable
Agent submission, integrity-verified reads, and two-phase cancellation. It is a
library service over PostgreSQL and exact executable registries; it is not an
HTTP, gRPC, or SSE server and it does not authenticate transport credentials.

StateKnot remains pre-alpha. This API has executable evidence but no stability,
published-crate, or production-support guarantee yet.

## Implemented contract

- every operation receives an `AgentServiceCaller` produced from an already
  verified credential;
- a mandatory `AgentServiceAuthorizer` decides submission, read, and cancel
  operations before deployment or durable target existence is disclosed;
- `AgentServiceRegistryBuilder` freezes at most 4,096 exact Agent revisions and
  rejects duplicate identities, schema disagreement, and deployment drift;
- submission commits through `DurableAgentRuns` under a tenant-scoped
  `AgentSubmissionKey` and never starts model or Tool execution inline;
- the same logical submission key and content recover the original Run after a
  timeout or lost acknowledgement; changed content fails closed as a conflict;
- Run and submission-key reads return fully revalidated public snapshots;
- cancellation binds both caller-retained `AgentCancellationIds`, records an
  authoritative PostgreSQL clock observation and immutable policy-decision
  digests, and returns `Committed` or `Idempotent`;
- cancelling a Waiting Run abandons its outstanding interrupts and timers in
  the same transaction; workers later confirm the terminal cancellation from
  durable evidence.

The service-control event intentionally excludes caller input, principal text,
policy payloads, secrets, and failure messages. Its public schema is
[`agent-service-control-event/1.0.0`](https://stknot.com/schemas/runtime/agent-service-control-event/1.0.0).

## Startup binding

Register every graph, reducer, node, typed input/output schema, and standard
runtime schema before building the service. The executable registry must
include both the Agent admission schema and the Agent service control schema.

```rust
use std::sync::Arc;
use stateknot_runtime::{
    AgentServiceRegistryBuilder, AgentServiceV1,
    register_standard_agent_service_control_event_schema,
};

register_standard_agent_service_control_event_schema(&mut schema_builder)?;

let executable_registry = executable_builder.build()?;
let mut deployments = AgentServiceRegistryBuilder::new();
deployments.register(Arc::new(provider_native_definition.clone()))?;

let service = AgentServiceV1::new(
    store.clone(),
    executable_registry,
    deployments.build(),
    Arc::new(authorizer),
)?;
```

`AgentServiceDeployment` is the extension point for another precompiled Agent
shape. Its descriptor and compiled Graph are snapshotted at startup; the
implementation must generate initial state matching the Graph state schema.

## Submit, read, and cancel

```rust
let caller = AgentServiceCaller::new(tenant_id, authenticated_principal);

let admitted = service
    .submit(
        caller.clone(),
        &submission_key,
        &agent_identity,
        request,
    )
    .await?;
let run_id = admitted.snapshot().provenance().run_id();

let snapshot = service.load(caller.clone(), run_id).await?;
let same_run = service.load_by_key(caller.clone(), &submission_key).await?;

// Persist these identities at ingress before the first call. Reuse the exact
// pair after timeout; generating a new pair creates a competing request.
let cancellation_ids = AgentCancellationIds::generate();
let outcome = service
    .request_cancellation(caller, run_id, cancellation_ids)
    .await?;
```

Submission returns after durable admission. A scheduler and Agent worker must
claim and execute the Run separately. Cancellation also has two durable phases:
the service records the request, then the Agent Loop confirms a terminal
cancelled outcome only when model usage and external-effect evidence permit it.

## Retry rules

| Operation | Safe recovery action |
| --- | --- |
| `submit` timed out | Retry with the same tenant, caller, Agent identity, submission key, and logical request. |
| `load` / `load_by_key` timed out | Retry the same authorized read. |
| `request_cancellation` timed out | Retry with the same `run_id` and exact `AgentCancellationIds`. |
| cancellation used new IDs | Treat a conflict as a competing request; do not hide it as idempotent success. |
| Run is `cancellation_requested` | Stop new dispatch and let Driver/Lifecycle reconciliation prove the final state. |

An embedding transport must not expose internal registry or database errors
verbatim. Map the closed `AgentServiceError` variants to a bounded public error
model, retain correlation IDs in trusted logs, and preserve the authorization-
before-not-found ordering.

## Production integration requirements

The embedding service remains responsible for:

1. TLS termination and token or mTLS verification;
2. deriving `TenantId` and `PrincipalIdentity` from verified credentials rather
   than request-body claims;
3. an `AgentServiceAuthorizer` backed by versioned policy and retained decision
   evidence;
4. durable retention of submission and cancellation identities before calling
   this facade;
5. scheduler/worker roles, graceful drain, health/readiness, metrics, tracing,
   rate limits, and overload control;
6. public error mapping, secret redaction, backup/restore, and tenant-isolation
   verification.

Do not put a network policy call inside a database transaction. If policy is
remote, first commit or load bounded decision evidence in a dedicated durable
ledger, then let the synchronous facade consume that trusted snapshot.

## Executable evidence

With a PostgreSQL 16 or 17 test database configured, require the integration
test instead of allowing an infrastructure skip:

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
cargo test -p stateknot-runtime --test postgres \
  agent_service_authorizes_submits_recovers_and_cancels_without_redispatch \
  --locked
```

The proof covers authorization-first missing-resource handling, exact
submission recovery, key-based reads, cancellation commit/retry/conflict, and
zero model/Tool dispatch from the service call itself.

## Deliberate exclusions

- no stable HTTP/gRPC/SSE wire API;
- no built-in identity provider or default allow-all authorizer;
- no in-memory fallback when PostgreSQL is unavailable;
- no implicit “submit and wait” operation;
- no terminal cancellation claim when external effects or usage remain
  unproven;
- no API stability or production-readiness claim during pre-alpha.
