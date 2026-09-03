<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Provider-native Agent graph

This document is the production integration contract for the unpublished
`ProviderNativeAgentGraph` in `stateknot-runtime`. The implementation composes
the typed Agent descriptor, durable invocation ledgers, executable Graph
registry, Graph Driver, lifecycle coordinator, and Agent Loop into one bounded
model/tool state machine. It remains pre-alpha: the API is not stable, the
crate is not published, and the repository does not yet claim production
support.

The compiled no-I/O example is the shortest honest starting point:

```console
cargo run -p stateknot-runtime --example provider_native_agent --locked
```

It constructs a real tool-capable descriptor, a digest-pinned local policy and
accounting contract, the generated checkpoint schema, every required standard
runtime schema, and an initial state. It deliberately performs no provider or
database I/O. Durable execution is qualified separately against PostgreSQL 16
and 17.

## Implemented execution subset

The v1 graph currently accepts exactly this subset:

- model-native JSON Schema output;
- at most the descriptor's finite `max_model_turns`;
- at most the descriptor's finite `max_tool_calls_per_turn`;
- sequential tool execution in provider proposal order;
- zero output-repair turns; and
- compact checkpoint references capped at 4,096 model/tool invocation IDs.

`ProviderNativeAgentGraph::compile` rejects tool-call-emulated final output,
repair turns, parallel tool dispatch, oversized call limits, and a composition
that cannot fit the durable superstep range. These cases do not silently fall
back to weaker behavior.

The generated graph has two stable executable nodes:

1. `agent.model` reconstructs the complete provider-native transcript from
   immutable invocation ledgers, prepares or recovers one model attempt, and
   either produces the final model-native output or commits a route to tools.
2. `agent.tools` resolves each proposed tool against the admitted descriptor,
   validates arguments, evaluates the pinned policy, and executes or recovers
   tools sequentially before returning to `agent.model`.

The checkpoint stores only the composition digest, stable input-message ID,
bounded invocation references, and the next phase. Provider responses, tool
results, and cumulative usage remain in their dedicated immutable ledgers.

## Freeze one deployment snapshot

The policy and accounting references are part of the composition digest.
Changing policy code, price tables, Agent/model/tool descriptors, schema
profiles, instructions, execution limits, or the input security label requires
a new graph version.

```rust,ignore
let definition = ProviderNativeAgentGraph::compile(
    typed_definition.descriptor().clone(),
    graph_identity,
    reducer_identity,
    state_schema_id,
    input_security_label,
    policy,
    accounting,
)?;

definition.register_schema(&mut schema_builder)?;
register_standard_graph_driver_event_schema(&mut schema_builder)?;
register_standard_graph_lifecycle_event_schema(&mut schema_builder)?;
register_standard_agent_cancellation_event_schema(&mut schema_builder)?;
register_standard_agent_admission_event_schema(&mut schema_builder)?;
register_standard_invocation_execution_event_schema(&mut schema_builder)?;
let schemas = schema_builder.build()?;
```

Before freezing the registry, also register the typed Agent input/output
schemas, every tool input/output schema, and every provider compatibility
profile. `register_executable` then binds the exact `PostgresStore`,
`DurableInvocationExecutor`, and immutable schema snapshot. Startup fails when
any digest-pinned dependency is absent or conflicting.

Do not mutate a live registry. Build a complete new deployment snapshot,
register its compiled graph durably, then admit new runs against that exact
graph reference. Existing runs continue to resolve their pinned version.

## Policy and accounting are execution dependencies

`AgentToolPolicy` is invoked before tool preparation. It is side-effect-free,
local, deterministic for its exact context, and returns a digest of immutable
decision evidence. A network policy engine needs its own durable decision
ledger; it must not be hidden behind this synchronous boundary.

The action digest binds the admitted Agent, admission digest, committed model
invocation, proposal position, tool identity, and canonical arguments. The
allowed tool plan retains both the action digest and policy-evidence digest.
Recovery revalidates them before any I/O.

`AgentInvocationAccounting` prices already durable terminal ledger evidence.
It is offline and deterministic. `Known(KnownCosts::empty())` is valid only for
a genuinely free invocation. Return `Unpriced` when the exact price is not
known; StateKnot preserves the usage evidence and stops before another call
when a finite monetary budget cannot be evaluated. Missing price data is never
converted to zero cost.

## Durable dispatch and recovery

Every external attempt follows the same authority sequence:

1. validate the checkpoint, transcript, descriptor, schemas, policy, budget,
   deadline, and lease/fence;
2. prepare the logical invocation and its stable event identities;
3. commit the physical attempt start before external dispatch;
4. dispatch only when the start result is newly `Committed`;
5. append the terminal provider/tool evidence; and
6. commit the node result and next checkpoint from exact ledger revisions.

An idempotently observed start never grants dispatch authority. After a lost
acknowledgement or process crash, recovery reads the committed terminal ledger
instead of calling the provider again. A higher fence may recover an unfinished
physical node attempt, but it cannot rewrite an already committed external
result.

Known failed tools remain ordered transcript outcomes. Their exact terminal
revision is bound into the node result, and the next model turn receives the
failure outcome without inventing success. An `Unknown` write outcome is never
retried as an ordinary business call. When both the immutable descriptor and
the exact installed provider enable reconciliation, the Tool node runs one
bounded probe for the original physical attempt. Authoritative evidence is
committed atomically; `Pending` becomes a durable `SafeAfter` node retry under a
later lease. Otherwise the run remains blocked for explicit manual
reconciliation.

Each Tool plan derives its reconciliation audit `EventId` deterministically
from already-persisted immutable identities. No new checkpoint field or state
schema version is required, so previously admitted graph references retain
their exact wire and digest compatibility.

## Two-phase durable cancellation

Cancellation intent and cancellation completion are different facts.

1. An authenticated control-plane service appends
   `RunTransition::RequestCancellation` with an immutable cancellation failure
   and an audit event. The repository does not yet expose a stable public HTTP
   cancellation endpoint; the embedding service owns that authorization and
   request schema.
2. The Driver polls durable run state while a node is active, signals
   cooperative cancellation, waits only for the configured grace period, and
   aborts local work when necessary. It dispatches no new activation after the
   request is observed.
3. The Driver returns an exact `GraphCancellationHandoff` containing the
   checkpoint, journal head, revision, failure ID, event ID, and live lease.
4. `DurableAgentLoop` passes that handoff to `DurableGraphLifecycle`, which
   reconstructs cumulative usage from trusted ledgers and atomically appends
   `agent_cancellation_confirmed` with `ConfirmCancellation`.

The confirmation timestamp comes from PostgreSQL's clock. The terminal commit
releases the lease. A lost acknowledgement is exactly retryable even after that
release: the coordinator reconstructs the committed timestamp and usage and
returns `Idempotent` for the identical event.

If a model is still `Executing`, a write tool is `Unknown`, or a failed model
lacks exact usage, `ProviderNativeAgentLifecycleEvidence` returns unavailable.
The run remains `cancellation_requested`; it does not become cancelled with
fabricated zero usage. The scheduler can retry after evidence is reconciled.

The public confirmation event schema is published at
`https://stknot.com/schemas/runtime/agent-cancellation-event/1.0.0`. It contains
only public-safe correlation fields; accounting and provider payloads remain in
their trusted stores.

## Operational settings

`DurableGraphDriverOptions::with_cancellation_timing` controls durable polling
and cooperative grace. Defaults are 250 ms and 5 s; polling is constrained to
10 ms–60 s and grace has a 5 min hard maximum. Choose values from measured
provider/tool behavior, keep external timeouts below the node deadline, and
preserve enough lease margin for the terminal lifecycle transaction.

At startup, the Driver observes the live lease and renews it before recovery
when remaining ownership is below half the configured lease duration. During
node execution, database-time observations anchor the monotonic watchdog.
Renewal, cancellation polling, and mutation retries are bounded and reported.

Monitor at minimum:

- model/tool starts, terminal states, recovered terminals, and Unknown age;
- checkpoint replay count and retained bytes;
- lease age, renewals, stale-fence failures, and takeover count;
- cancellation-requested age, cooperative aborts, evidence-unavailable
  confirmations, and idempotent confirmation retries;
- token, byte, event, invocation, and known/unpriced monetary usage; and
- policy denial/error rate keyed by immutable policy version, without logging
  arguments or credentials.

## Qualification evidence

The runtime integration suite exercises the provider-native path on real
PostgreSQL 16 and 17. Focused scenarios cover:

- a multi-turn model → tool → model path that recovers a committed model without
  redispatch and completes lifecycle success;
- a higher-fence stale policy race with no duplicate external dispatch;
- a known failed tool retained in transcript order with its exact terminal
  binding;
- an unknown tool outcome that returns `Pending`, is durably delayed, resolves
  under a later lease, continues the next model turn, and performs exactly one
  business call across two reconciliation probes;
- cancellation after a committed model with exact usage and lost-ack replay;
- cancellation before provider dispatch through `DurableAgentLoop`; and
- fail-closed cancellation when exact evidence is unavailable.

Run the offline and database-backed evidence with:

```console
cargo run -p stateknot-runtime --example provider_native_agent --locked
cargo test -p stateknot-runtime --test postgres provider_native --locked
```

The second command requires `STATEKNOT_TEST_DATABASE_URL`; CI additionally sets
`STATEKNOT_REQUIRE_POSTGRES_TESTS=1` so missing infrastructure fails rather than
silently skipping the suite.

## Explicit remaining gates

This milestone does not ship parallel sibling/tool execution, output repair,
loop/subgraph semantics, artifact retrieval, stable network Agent/cancellation
transport, protocol-specific outbox dispatch, MCP/A2A server composition,
broader protocol extensions, A2A live-peer reconciliation qualification,
live-provider drift cassettes, role-separated database procedures, general
retention, failover/restore qualification, or a production release.
[`AgentServiceV1`](agent-service.md) now supplies the embedding service boundary,
[`McpRemoteTool`](mcp-remote-tool.md) supplies one strict client-side Tool
profile, and [`A2aRemoteAgent`](a2a-client.md) supplies one durable outbound
Agent Tool profile. The independent [MCP Server profile](mcp-server.md) and
[A2A Server profile](a2a-server.md) expose their own application boundaries;
none widens the provider-native graph claim.
Those capabilities require their own versioned contracts and executable
evidence; none is implied by the provider-native graph.
