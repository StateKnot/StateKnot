<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable Agent Loop and tenant scheduler

Status: implementation-backed pre-release contract. The API is unpublished and
may still change. This document describes the production-shaped guarantees that
exist in the repository; it is not a production-release claim.

[简体中文](durable-agent-loop.zh-CN.md)

## Delivered boundary

The runtime now closes the lease-owned path from tenant-scoped runnable
discovery to a durable graph lifecycle boundary:

```text
tenant scheduler tick
  -> stable runnable-page snapshot
  -> exact lease claim
  -> durable Graph Driver
  -> Wait / success / failure / cancellation lifecycle coordinator
  -> one fenced PostgreSQL transaction
  -> lease released or next durable scheduling boundary
```

The implementation is split deliberately:

| Component | Responsibility |
|---|---|
| `DurableFairScheduler` | Reserves one replica-global weighted slot with an exact starvation bound, then delegates only to the selected tenant worker. |
| `DurableTenantScheduler` | Scans one tenant's fixed-cutoff queue in `(available_at, run_id)` order, claims at most one run, and invokes one bounded loop quantum. |
| `DurableAgentLoop` | Binds one store, one immutable executable registry, one Driver, and one lifecycle coordinator so they cannot accidentally use different deployment snapshots. |
| `DurableGraphDriver` | Replays and validates durable graph evidence, commits node starts before dispatch, executes nodes, renews the lease, polls durable cancellation, and advances Continue barriers. |
| `DurableGraphLifecycle` | Atomically commits Wait, successful Terminal, supervised run failure, or confirmed cancellation using the exact lease-bound handoff. |
| `GraphLifecycleEvidenceProvider` | Recovers already-durable admission, artifact, failure, and cumulative-accounting facts owned by the embedding application. It is not a fallback inference hook. |

This is a runnable **durable graph loop**. Provider-neutral durable model/tool
attempt execution, cross-tenant weighted selection, a typed Agent contract, the
first OpenAI Responses/Anthropic Messages adapters, durable admission/result
retrieval, and the prebuilt provider-native graph now exist. This is still not
the stable end-user Agent API: the one-call service, public cancellation
ingress, advanced graph semantics, protocol adapters, and production
qualification remain release work.

## Startup binding

Install all three standard Driver, lifecycle, and cancellation audit schemas
before freezing the executable registry.
Construct the scheduler only after migrations and all application schemas,
graphs, reducers, and node executors are installed from one immutable release
artifact.

```rust,ignore
use std::sync::Arc;
use stateknot_runtime::{
    DurableGraphDriverOptions, DurableGraphLifecycleOptions,
    DurableTenantScheduler, DurableTenantSchedulerOptions,
    ExecutableGraphRegistryBuilder, JsonSchemaRegistryBuilder,
    register_standard_agent_cancellation_event_schema,
    register_standard_graph_driver_event_schema,
    register_standard_graph_lifecycle_event_schema,
};

let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
register_standard_graph_driver_event_schema(&mut schemas)?;
register_standard_graph_lifecycle_event_schema(&mut schemas)?;
register_standard_agent_cancellation_event_schema(&mut schemas)?;
register_application_schemas(&mut schemas)?;

let mut executables = ExecutableGraphRegistryBuilder::new(schemas.build()?);
register_release_graphs_reducers_and_nodes(&mut executables)?;

let scheduler = DurableTenantScheduler::new(
    store,
    executables.build()?,
    Arc::new(application_lifecycle_evidence),
    DurableGraphDriverOptions::default(),
    DurableGraphLifecycleOptions::default(),
    DurableTenantSchedulerOptions::default(),
)?;
```

The standard lifecycle audit schema has the immutable identifier
`https://stknot.com/schemas/runtime/graph-lifecycle-event/1.0.0`. Register it
through `register_standard_graph_lifecycle_event_schema`; do not copy a digest
into application code.

Cancellation confirmation uses
`https://stknot.com/schemas/runtime/agent-cancellation-event/1.0.0`. Register it
through `register_standard_agent_cancellation_event_schema`; constructing the
lifecycle coordinator without it fails closed.

## Trusted lifecycle evidence

Successful `AgentResult` construction requires facts the graph barrier does not
own: the admitted `AgentDescriptor`, admitted `AgentRequest`, resolved finite
budget, final artifact references, and complete cumulative usage. Terminal
failure likewise requires a public-safe `Failure` and cumulative usage.
Cancellation confirmation requires cumulative usage recovered at the exact
requested checkpoint and must remain unavailable while an external result is
still executing or ambiguous.

The embedding service supplies these facts through
`GraphLifecycleEvidenceProvider`. A production implementation must:

- read only trusted durable admission, artifact, and accounting stores;
- be deterministic for the exact payload-free context it receives;
- use bounded reads and deadlines, with no model, tool, or other external side
  effects;
- return `TemporarilyUnavailable`, `Unavailable`, or `Corrupt` rather than
  guessing missing usage, reconstructing requests, or inserting zero values;
- keep protected diagnostics in telemetry because the public error is
  deliberately payload-redacted.

Before committing success, `DurableGraphLifecycle` validates request input and
terminal output against the frozen offline schema registry, constructs the
`AgentResult`, and revalidates its provenance, descriptor, request, budget,
artifacts, and cumulative usage relationships. Evidence failure causes the
Agent Loop to perform a bounded best-effort exact-fence release; the run stays
recoverable instead of being partially finalized.

## Atomic lifecycle edges

### Wait

Node code returns `NodeWaits`: one to 64 complete interrupt or timer
specifications without a process-generated registration timestamp. At the
Driver handoff, the lifecycle coordinator binds every registration to the same
stable lifecycle `EventId`, tenant, and run, then calls
`append_worker_wait_barrier`.

One PostgreSQL transaction commits the journal event, consumes the exact ready
result set, writes the successor checkpoint, transitions the run to Waiting,
registers the complete wait batch using database time, and clears the lease.
No partially registered Wait is visible, and application clock skew cannot
become durable registration evidence.

### Successful Terminal

After trusted evidence and schema validation, one barrier transaction consumes
the exact result set, writes the terminal checkpoint and public-safe lifecycle
event, stores the validated `AgentResult`, transitions the run to Succeeded,
and clears the lease.

### Blocked failure

Same-fence in-flight work is never declared failed and never dispatched twice.
The lifecycle coordinator releases ownership so a successor fence can use the
existing crash-takeover rules. A blocked plan with no in-flight work and at
least one failed, exhausted, or unsupported node enters durable supervision;
trusted failure evidence is then appended with the Failed transition and lease
release in one transaction.

### Confirmed cancellation

An authenticated control plane first commits `RequestCancellation`. The Driver
observes that durable state, stops new dispatch, applies cooperative
cancellation with a bounded grace period, and returns an exact
`GraphCancellationHandoff`. The Agent Loop automatically calls
`confirm_cancellation` with that handoff.

Lifecycle validates the request failure ID, revision, checkpoint, journal head,
graph, and live fence. It recovers exact cumulative usage, obtains completion
time from the database clock, and appends the public-safe confirmation event
with `ConfirmCancellation` in one transaction that clears the lease. If exact
evidence is unavailable, the run remains `cancellation_requested`; no zero
usage or fictitious terminal state is written.

## Lost acknowledgements and stale handoffs

Lifecycle handoffs are short-lived, non-serializable commit inputs. The Driver
allocates their stable `EventId` once, before handing control to the lifecycle
coordinator. Every retry reuses the exact event, revision, journal head,
checkpoint plan, and fence.

For successful Terminal, supervised failure, and confirmed cancellation, if the first transaction
committed but its acknowledgement was lost, the coordinator accepts only the
exact post-commit snapshot: revision advanced by one, expected terminal status,
journal head naming the same event, and no lease. It reconstructs the exact
committed `AgentResult`, `RunFailure`, or cancellation timestamp and usage from
the lifecycle and does not require the external evidence provider to be
available again. Any other changed
revision, event, status, head, or owner is a stale terminal handoff and fails
closed.

Wait replay has a deliberately different rule because an authorized resolver,
timer, or cancellation may advance the run before the original caller retries.
The PostgreSQL provider looks up the stable Wait event before applying fresh
run predicates and proves the original event intent, projection digest,
checkpoint anchor, result consumptions, and immutable registration set. An
exact retry therefore remains idempotent after those later transitions and
never rolls them back; any mismatch is a commit conflict rather than a guessed
success.

Storage retries are bounded, use capped exponential backoff, and never allocate
a replacement durable identity. Driver or lifecycle errors trigger bounded
best-effort exact-fence cleanup. A cleanup database failure is preserved
alongside the primary error so operators can distinguish execution failure from
ownership-cleanup failure.

## Tenant scheduler contract

One `DurableTenantScheduler::tick`:

1. scans a fixed database-time tenant snapshot in durable queue order;
2. limits decoded page size and the maximum page chain;
3. allocates one stable UUIDv7 `AttemptId` per candidate and reuses it across
   transient claim retries;
4. treats lease contention or a candidate that changed after discovery as a
   normal skip;
5. claims and executes at most one run; and
6. returns a closed outcome plus page, candidate, contention, and retry
   counters.

`Executed`, `ExecutionFailed`, `Idle`, `ScanLimitReached`, and `Cancelled` are
distinct outcomes. A run-local Agent Loop error does not crash the tenant
worker; an infrastructure scan or claim error does.

Deployments obtain bounded concurrency by running an explicitly configured
number of workers. Database fencing resolves races. `DurableTenantScheduler`
deliberately remains single-tenant; wrap it through `DurableFairScheduler` for
replica-safe smooth weighted selection and an exact reservation-count
starvation bound. See the [fair scheduling contract](cross-tenant-fair-scheduler.md).
Do not give a tenant worker credentials or queue scope for another tenant as a
fairness shortcut.

## Operations and observability

At minimum, export:

- scheduler pages and candidates scanned, contention skips, claim retries,
  scan-limit outcomes, and per-tenant queue age;
- Driver replay/result bytes, durable starts/completions, Continue barriers,
  renewals, timeouts, cancellations, and mutation retries;
- lifecycle Wait/success/failure/cancellation commits, idempotent recoveries, stale
  handoffs, evidence error class, exact-fence release, and cleanup failure;
- run status, lease age, delayed-retry age, and time spent Waiting without
  logging request, output, failure, or secret payloads.

Use a DDL-only migration credential at startup and a least-privilege trusted
runtime credential afterward. Database statement timeouts must remain below the
lease safety margin. Evidence-provider deadlines must fit inside the retained
handoff lease; failure should release ownership rather than attempting a stale
commit.

## Qualification evidence and remaining gates

Twenty-six runtime integration scenarios run against both PostgreSQL 16 and
17. They cover lifecycle success/Wait/failure/cancellation atomicity and exact
lost-ack replay, database-time Wait materialization, Agent Loop success and
evidence failure, tenant and weighted cross-tenant scheduling, durable
model/tool attempts and streaming, provider-native multi-turn recovery,
noninitial replay, same-fence suppression, lease renewal, near-expiry refresh,
initial-state quarantine, higher-fence takeover, and the public durable
run/result facade. The PostgreSQL provider suite also runs independently for
each database version. CI makes both suites mandatory.

Atomic admission, the public run/result facade, and the provider-native graph
with transcript recovery, policy evidence, exact accounting, and cancellation
confirmation are implemented. The remaining release blockers include a stable
public cancellation service boundary, artifact retrieval, parallel sibling
policy, loop/subgraph semantics,
protocol-specific outbox dispatch, role-separated database procedures, general
retention, backup/restore, failover, stale-race qualification, observability,
and release hardening.
