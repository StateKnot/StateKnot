<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable Graph runtime

Status: implementation-backed pre-release contract. The APIs are unpublished
and may still change. This document describes the boundary that is implemented
and qualified today; it is not a production-release claim.

[简体中文](durable-graph-runtime.zh-CN.md)

## What the runtime owns

`stateknot-runtime` binds a canonical compiled graph to executable code without
performing network discovery while a run is active. One immutable deployment
snapshot contains:

- JSON Schema 2020-12 documents whose `$id`, RFC 8785 bytes, version, and
  SHA-256 digest exactly match each `SchemaReference`;
- one pure `GraphReducer` implementation for every referenced reducer revision;
- exactly one `GraphNodeExecutor` for every node of every registered graph
  digest; and
- no orphan reducer or node implementation that a registered graph cannot use.

Registry construction is a startup gate. A missing schema, reducer, or node;
identity reuse with different bytes; an unresolved `$ref`; a duplicate binding;
or an orphan implementation stops startup before work is claimed. Registry
contents are immutable after `build`, and there is no runtime schema fetch path.

For one exact live `RunFence`, `DurableGraphDriver` then:

1. reloads and recompiles the checkpoint-pinned graph definition;
2. independently replays every committed noninitial checkpoint with bounded
   result memory and the exact installed reducer and schemas;
3. obtains the deterministic ready-node recovery plan;
4. commits a physical node-attempt start before spawning node code;
5. launches only after a fresh `NodeAttemptCommitOutcome::Committed` result;
6. refreshes a near-expiry lease before launch, then renews it under a
   database-time-derived monotonic watchdog while propagating cancellation and
   applying a hard node deadline without holding a database transaction during
   node execution;
7. commits success or public-safe failure against the latest journal head;
8. commits Continue barriers and repeats; and
9. returns a typed handoff when another service owns the next lifecycle edge.

An idempotently observed start is never execution authority. It may be a lost
acknowledgement or another executor's committed start, so the Driver classifies
it as in flight. If that executor dies, lease expiry or explicit supersession
allows a higher fence to recover the unfinished attempt once.

## Startup integration

Migrations and executable registration belong to deployment startup, not the
request path. Use a DDL-authorized credential for `migrate_database`, discard
it, then connect the runtime pool with a least-privilege credential.

```rust,ignore
use std::sync::Arc;
use stateknot_runtime::{
    DurableGraphDriver, DurableGraphDriverOptions,
    ExecutableGraphRegistryBuilder, JsonSchemaRegistryBuilder,
    register_standard_graph_driver_event_schema,
    register_standard_graph_lifecycle_event_schema,
};
use stateknot_store_postgres::{PostgresStore, PostgresStoreOptions};

let options = PostgresStoreOptions::default();
PostgresStore::migrate_database(migration_url, options.clone()).await?;
let store = PostgresStore::connect(runtime_url, options).await?;

let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
register_standard_graph_driver_event_schema(&mut schemas)?;
register_standard_graph_lifecycle_event_schema(&mut schemas)?;
for (reference, document) in application_schema_documents() {
    schemas.register(reference, document)?;
}

let mut executables = ExecutableGraphRegistryBuilder::new(schemas.build()?);
for graph in compiled_graph_revisions() {
    executables.register_graph(graph)?;
}
for reducer in reducer_revisions() {
    executables.register_reducer(Arc::clone(&reducer))?;
}
for node in graph_node_executors() {
    executables.register_node(Arc::clone(&node))?;
}

let driver = DurableGraphDriver::new(
    store,
    executables.build()?,
    DurableGraphDriverOptions::default(),
)?;
```

The application helper functions in this example must return owned, immutable
release artifacts. A reducer or executor reference must name the same complete
graph/reducer identity and digest used during run admission. Do not install a
different implementation beneath an existing version.

The standard Driver audit schema has the immutable identifier
`https://stknot.com/schemas/runtime/graph-driver-event/1.0.0`. Install it through
`register_standard_graph_driver_event_schema`; do not copy its digest into
application code.

The lifecycle coordinator uses the separate immutable schema
`https://stknot.com/schemas/runtime/graph-lifecycle-event/1.0.0`. A deployment
that constructs `DurableGraphLifecycle`, `DurableAgentLoop`, or
`DurableTenantScheduler` must also install it through
`register_standard_graph_lifecycle_event_schema` before freezing the registry.

## Claim and drive

The scheduler discovers runnable work, selects one run, allocates a stable
UUIDv7 `AttemptId`, and calls `PostgresStore::claim_lease`. Only a successful or
same-ID idempotent claim supplies the exact `RunFence` passed to `drive`.
Different-owner `LeaseHeld` is ordinary contention.

```rust,ignore
let claim = store.claim_lease(&tenant_id, run_id, attempt_id).await?;
let fence = claim.lease().fence().clone();
let result = driver.drive(fence, shutdown_signal).await?;
let (outcome, report) = result.into_parts();
```

The shutdown signal is process-owned and monotonic. On shutdown during node
execution, the Driver signals cooperative cancellation, aborts the task if it
does not return, releases the exact fence, and leaves its durable unfinished
attempt for higher-fence recovery. Node code must put external model and tool
effects through their durable invocation ledgers; a raw external write inside a
node cannot inherit StateKnot's reconciliation guarantees.

## Outcome handling

Every `GraphDriveOutcome` has an explicit ownership rule:

| Outcome | Required caller action | Lease state |
|---|---|---|
| `LifecycleBarrierReady` | Immediately construct complete Wait or successful Terminal metadata and call the corresponding atomic provider commit with the exact plan, head, revision, and lease. Never reconstruct or partially persist the handoff. | Retained and must still be live at commit |
| `Blocked` with `in_flight > 0` | Supervise or relinquish ownership; do not dispatch another same-fence attempt. | Retained |
| `Blocked` with `failed` or `exhausted` | Apply the run-level failure policy using the plan's exact evidence and cumulative usage. Do not infer retry authority. | Retained |
| `Deferred` | Schedule no timer in process; the indexed database-time gate is already committed. | Released by the Driver |
| `Yielded` | Re-enter through scheduler discovery and a new claim if work remains. | Released by the Driver |
| `Cancelled` | Stop local work; a later scheduler claim performs recovery. | Released by the Driver |

`GraphLifecycleBarrierHandoff` is deliberately not serializable or detached
from its `RunLease`. It is short-lived commit input, not a queue message. If the
lifecycle service cannot finish before lease expiry, release or allow expiry and
replan beneath a new fence rather than committing stale metadata.

Application workers should normally use `DurableAgentLoop` instead of handling
these outcomes directly. It binds the Driver and `DurableGraphLifecycle` to one
store and registry, commits lifecycle handoffs, and performs bounded exact-fence
cleanup on errors. Direct Driver integration remains available for specialized
orchestration services that implement the same ownership rules. See the
[Agent Loop and tenant scheduler contract](durable-agent-loop.md).

## Resource and timing policy

Tune `DurableGraphDriverOptions` from measured workload bounds:

- `GraphReplayLimits` caps retained compact pending-result bytes per historical
  barrier. The default is 64 MiB and the hard ceiling is 512 MiB.
- `maximum_durable_events` bounds one `drive` quantum. Yielding happens only
  between durable operations; the default is 1,024.
- `lease_renewal_interval` must fit at least three times inside the provider's
  lease duration and must not exceed its maximum renewal horizon.
- `node_execution_timeout` is a hard wall-clock deadline; the default is 15
  minutes and the implementation maximum is seven days.
- mutation retries reuse the same durable event and attempt identities. They
  are bounded by `maximum_mutation_attempts` with capped exponential backoff.

After the durable start commits, the Driver takes a fresh database-time lease
observation before spawning node code. Each renewal is raced against a
conservative monotonic deadline anchored before the database request. A late
`Idempotent` response can confirm that renewal bytes committed, but cannot
revive an already expired lease; the watchdog cancels the node instead.

Set the database statement timeout below the lease safety margin. Set provider,
tool, and model request deadlines below the node deadline. Monitor replay counts,
retained replay bytes, starts, completions, barriers, renewals, and mutation
retries from `GraphDriveReport`.

## Recovery and safety invariants

- A node start is durable before code runs.
- An `Idempotent` start never launches code.
- Every completion is append-only and names its exact physical start.
- A stale fence cannot renew, complete, schedule, release, or commit a barrier.
- Noninitial replay uses the same pinned graph, reducer, schemas, journal
  anchors, result set, and checkpoint parentage as the original commit.
- Missing or contradictory durable graph evidence is quarantined under the
  current live fence before execution.
- Continue is the only lifecycle barrier the Driver commits autonomously.
- Ready siblings execute sequentially in stable plan order today. This preserves
  one unambiguous journal predecessor and recovery authority; bounded parallel
  sibling scheduling is not enabled until its ordering policy is qualified.

## Qualification evidence and remaining gates

Seventeen runtime scenarios run against both PostgreSQL 16 and 17. Six retain the
Driver-specific recovery coverage:

1. Continue-barrier commit followed by noninitial replay and a Terminal handoff;
2. same-fence in-flight recovery with no duplicate executor call;
3. lease renewal through execution longer than the original lease;
4. near-expiry claim refresh before node code is launched;
5. invalid initial checkpoint state quarantined before any executor call; and
6. one higher-fence takeover of an unfinished physical attempt.

Six existing lifecycle/scheduler scenarios verify atomic successful Terminal, Wait, and supervised
failure handoffs with exact lost-ack retries; database-time Wait registration;
Agent Loop success and evidence-unavailable cleanup; and tenant scheduler
selection, claim, execution, and idle convergence. Four additional scenarios
verify model terminal-fence recovery, ordered durable model streaming,
ambiguous write-tool timeout suppression, and 3:1 cross-tenant weighted
selection, and one verifies atomic Agent admission. The provider contributes 98 additional PostgreSQL integration tests
per database version. CI treats the external database suites as mandatory and
fails if the service is unavailable.

The later typed-Agent milestone now ships the first OpenAI Responses and
Anthropic Messages adapters, and the atomic admission milestone now ships its
runtime validation facade. The complete durable public run/result facade,
assembly of the implemented provider-native transcript inside its prebuilt
graph, parallel siblings, loops/subgraphs, protocol-specific outbox adapters,
role-separated database procedures, retention/archive, failover/restore
qualification, the 10,000 stale-race gate, and a stable public release have not
shipped. Those remain release blockers rather than hidden fallback behavior.
