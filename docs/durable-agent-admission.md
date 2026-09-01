<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable Agent admission

Status: implemented pre-alpha integration contract. The crates remain
unpublished and the complete public run/result API has not shipped.

This document defines the trusted boundary that turns one authenticated,
schema-valid Agent request into scheduler-visible durable work. It covers the
core admission snapshot, the `DurableAgentAdmission` runtime facade, PostgreSQL
migration 15, exact retry behavior, and operational requirements. It does not
claim that authentication, policy evaluation, public idempotency-key routing,
the prebuilt model/tool graph, or terminal result retrieval is complete.

A [Simplified Chinese edition](durable-agent-admission.zh-CN.md) is maintained
alongside this document.

## What commits atomically

One successful `DurableAgentAdmission::admit` call commits all of these facts in
one PostgreSQL transaction:

1. the immutable Agent descriptor, typed request, ordered policy-budget layers,
   resolved finite budget, graph reference, authenticated principal, granted
   scopes, and authorization evidence;
2. the database-clock `admitted_at` observation and domain-separated admission
   digest;
3. the run lifecycle from pending to active;
4. the sequence-one `agent-admitted` control-plane event;
5. the superstep-zero checkpoint, exact initial state, and graph entry ready
   set; and
6. the journal, checkpoint, lifecycle, graph, and scheduler projections that
   make the run executable.

No scheduler query can observe the run between these writes. Migration 15 also
requires a non-null checkpoint in `runs_scheduler_ready`; the legacy low-level
`PostgresStore::admit_run` surface therefore creates addressable but
scheduler-invisible bootstrap rows. New Agent integrations must use the atomic
surface.

## Trusted ingress responsibilities

Admission deliberately starts after external authentication and policy
evaluation. Before constructing `AgentAdmissionAuthority`, the embedding
control plane must:

- authenticate the tenant and `PrincipalIdentity` through an application-owned
  mechanism;
- resolve one immutable Agent, graph, policy, schema, model, and tool deployment
  snapshot;
- evaluate the version-pinned policy, narrow the granted scopes, and retain
  schema-pinned evidence of the granted decision;
- derive every system, tenant, policy, Agent, and request budget layer without
  accepting caller-supplied resolved totals; and
- map the caller's idempotency key to one retained `AgentRunIds` value before
  the first database attempt.

`AgentAdmissionAuthority` is an audit snapshot, not a signature verifier.
`Digest` values prove byte identity; they do not authenticate a principal or
make embedded data secret.

## Assemble the immutable deployment

The standard admission event schema and every application schema must be
registered before the executable registry is frozen. The exact graph,
reducer, and node closure must already exist locally and the compiled graph
must already be registered for the tenant in PostgreSQL.

```rust,no_run
let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
register_standard_agent_admission_event_schema(&mut schemas)?;
register_standard_graph_driver_event_schema(&mut schemas)?;
register_standard_graph_lifecycle_event_schema(&mut schemas)?;
register_application_schemas(&mut schemas)?;

let mut executables = ExecutableGraphRegistryBuilder::new(schemas.build()?);
register_release_graphs_reducers_and_nodes(&mut executables)?;
let executables = executables.build()?;

store
    .register_graph_definition(tenant_id.clone(), compiled_graph)
    .await?;

let admission = DurableAgentAdmission::new(store.clone(), executables)?;
```

The release-owned public-safe event schema is published at
`https://stknot.com/schemas/runtime/agent-admission-event/1.0.0`. Register it
through the runtime helper instead of hard-coding its digest.

## Build and retain an exact request

Allocate the complete identity bundle once. Persist its association with the
authenticated external idempotency key before calling `admit`; a timeout must
never generate replacement identities.

```rust,no_run
let ids = AgentRunIds::generate();

// Persist external_idempotency_key -> ids in the trusted ingress store first.
let request = DurableAgentAdmissionRequest::new(
    tenant_id,
    ids,
    agent_descriptor,
    agent_request,
    evaluated_budget_layers,
    graph_reference,
    admission_authority,
    initial_state,
)?;

// Retain these validated bytes for an ambiguous retry.
let retry_bytes = serde_json::to_vec(&request)?;
let outcome = admission.admit(request).await?;
```

`DurableAgentAdmissionRequest` serialization revalidates all derived fields on
decode. Its `Debug` output exposes identities, schema references, counts, and
digests, but not request, instruction, initial-state, policy-evidence, or budget
payloads.

An exact retry reconstructs the same request bytes, uses the same run, thread,
invocation, event, and checkpoint IDs, and keeps the original immutable
deployment snapshot available:

```rust,no_run
let retained: DurableAgentAdmissionRequest =
    serde_json::from_slice(&retry_bytes)?;

match admission.admit(retained).await? {
    AgentAdmissionCommitOutcome::Committed(stored)
    | AgentAdmissionCommitOutcome::Idempotent(stored) => {
        enqueue_or_observe(stored.run())?;
    }
}
```

The PostgreSQL provider probes and verifies already-committed evidence before
consulting the current database clock. A lost acknowledgement can therefore
converge after the original deadline, but only for the exact retained intent,
event, checkpoint, and initial state. Reusing a run ID with changed input,
policy, budget, graph, ready set, or identities is a conflict. The original
digest-pinned executable and schemas must remain installed long enough to
validate the retry at the runtime boundary.

## Fail-closed validation order

Before opening the admission transaction, the runtime facade:

1. resolves the exact executable graph closure;
2. requires Agent input/output schemas to equal the graph input/output schemas;
3. validates Agent input, authorization evidence, and initial state against one
   frozen offline JSON Schema 2020-12 registry;
4. constructs and validates the closed public-safe standard event data; and
5. derives the initial ready set from the compiled graph instead of accepting
   it from the caller.

The store then independently loads the tenant-scoped immutable graph, validates
the superstep-zero checkpoint and entry ready set, obtains database time,
checks deadline admissibility, and writes the transaction. Schema rejection,
deadline expiry, graph drift, and injected late failures leave no run,
admission, event, checkpoint, or scheduler residue.

## Durable evidence and sensitive data

Migration 15 creates `stateknot.agent_admissions`. The row stores canonical
admission bytes plus redundant indexed Agent, graph, policy, digest, event, and
checkpoint anchors. Loads run in one repeatable-read snapshot and revalidate:

- canonical decoding and every derived digest;
- tenant/run/thread/invocation and Agent provenance;
- graph registry identity, version, bytes, and definition digest;
- sequence-one event identity, timestamp, kind, digest, and projection digest;
- superstep-zero checkpoint identity, state, ready set, graph, and digest; and
- the current lifecycle, journal/checkpoint heads, quarantine, lease, and wait
  projections.

The canonical snapshot can contain user input, trusted instructions, principal
attributes, authorization evidence, and policy limits. Treat the table as
sensitive application data: use least-privilege roles, encrypted transport,
encrypted backups, row/tenant isolation appropriate to the deployment, access
auditing, and a retention policy compatible with legal requirements. The
sequence-one event contains only operation and correlation digests; it is
public-safe metadata, not a substitute for protecting the snapshot.

## Migration and rollout

Run migration 15 with the migration role before deploying code that calls the
new facade. During a rolling deployment:

1. migrate and verify checksums;
2. deploy workers that understand admission rows and the revised scheduler
   predicate;
3. register the exact schemas, executable graphs, and PostgreSQL graph
   definitions for the release;
4. enable trusted ingress traffic; and
5. keep the previous digest-pinned deployment available until its retry and
   recovery window has closed.

Do not manually add a checkpoint to a low-level run or insert an admission row.
Do not mutate canonical bytes, redundant columns, or anchor rows to repair a
conflict. Quarantine and investigate contradictory evidence.

## Qualification evidence

The core suite verifies deterministic budget resolution, database-clock
deadline rejection, scope coverage, canonical digest recomputation, wire
tampering, size bounds, and redacted diagnostics. The PostgreSQL 16/17 suite
adds:

- exact commit and retry convergence after time-sensitive boundaries;
- changed-intent conflict with complete durable revalidation;
- 24-way same-request admission with one physical commit;
- invalid initial-state rollback with zero residue;
- migration-15 upgrade, index, constraint, and tamper checks; and
- runtime-facade rejection of Agent/Graph and authorization-schema drift before
  any database write.

The repository currently runs 98 PostgreSQL provider cases and 17 durable
runtime scenarios on each supported database version.

## Remaining public Agent boundary

This slice makes admission atomic and recoverable. It does not yet expose the
stable end-user operation that combines ingress idempotency, admission,
cross-tenant scheduling, provider-native transcript assembly, policy
middleware, terminal observation, artifact access, typed result decoding, and
cancellation. Those pieces must be composed and qualified as one public
`DurableAgent` facade before StateKnot claims a production-ready Agent API.
