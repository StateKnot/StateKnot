<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable Agent runs and results

Status: implemented pre-alpha integration contract. The crates are unpublished
and the API is not yet covered by a compatibility promise.

This document defines the public Rust boundary for admitting, resolving, and
reading one durable Agent run. It covers `DurableAgentRuns`, tenant-scoped
ingress idempotency, PostgreSQL migration 16, the safe run/result snapshot, and
production integration rules. Authentication, authorization, HTTP routing,
rate limiting, and ownership checks remain responsibilities of the embedding
control plane.

A [Simplified Chinese edition](durable-agent-runs.zh-CN.md) is maintained
alongside this document.

## Build one immutable runtime

`DurableAgentRuns` binds a `PostgresStore` to one frozen
`ExecutableGraphRegistry`. Build that registry exactly as described by the
[durable admission contract](durable-agent-admission.md): register every
digest-pinned application and standard schema, install the exact graph,
reducer, and node closure, freeze it, then register the same compiled graph for
the tenant in PostgreSQL.

```rust,no_run
let runs = DurableAgentRuns::new(store.clone(), executable_registry)?;
```

Every admission and load resolves the immutable graph again and revalidates
the Agent/Graph input and output schema binding, request input, authorization
evidence, initial state, admission event, and terminal output. A deployment
that no longer contains the exact executable or schema fails closed instead of
returning an unverified result.

## Submit with durable idempotency

Use `submit` at a user-facing ingress. `admit` remains available for internal
callers that can retain the complete request and all generated identities
across an ambiguous response.

```rust,no_run
let key = AgentSubmissionKey::generate();

let request = DurableAgentAdmissionRequest::new(
    tenant_id.clone(),
    AgentRunIds::generate(),
    agent_descriptor,
    agent_request,
    evaluated_budget_layers,
    graph_reference,
    admission_authority,
    initial_state,
)?;

let submitted = runs.submit(&key, request).await?;
let snapshot = submitted.snapshot();
return_key_and_run_id_to_caller(&key, snapshot.provenance().run_id())?;
```

The caller must supply or retain the key before the first request. A generated
key contains two independent UUIDv7 values (148 random bits in total). External
keys must contain 16–128 bytes from `[A-Za-z0-9._~-]`; clients should still use
at least 128 bits of cryptographic unpredictability. Treat the raw key as
sensitive correlation data and never use it as authentication.

The provider stores only a tenant-scoped SHA-256 digest of the raw key. The
mapping and a new admission commit in the same transaction. The mapping binds
the immutable Agent, request, ordered budget layers and resolved budget, graph,
authorization snapshot, initial state, and initial ready set. Framework-owned
run, thread, invocation, event, and checkpoint identities are deliberately not
part of this logical submission digest.

After a lost response, rebuild the same logical content and reuse the same key.
Fresh `AgentRunIds` are allowed; the original selected run is returned:

```rust,no_run
let retry = DurableAgentAdmissionRequest::new(
    tenant_id.clone(),
    AgentRunIds::generate(), // a retry may allocate a fresh candidate bundle
    retained_agent_descriptor,
    retained_agent_request,
    retained_budget_layers,
    retained_graph_reference,
    retained_admission_authority,
    retained_initial_state,
)?;

match runs.submit(&key, retry).await? {
    AgentRunAdmissionOutcome::Committed(snapshot)
    | AgentRunAdmissionOutcome::Idempotent(snapshot) => observe(snapshot)?,
}
```

The same key with changed logical content returns
`StoreError::AgentSubmissionConflict`. A durable run can own at most one
submission key; presenting another key for the same retained admission also
conflicts. Equal raw keys in different tenants are independent. The raw key is
never persisted or included in `Debug` output.

Do not regenerate policy evidence, deadlines, budget layers, Agent definitions,
or initial state during a retry. Retain those immutable inputs for at least the
complete client retry window. After deterministic local deployment and request
revalidation, the provider checks existing durable key evidence before new
database-time and initial-checkpoint admission checks. A lost acknowledgement
can therefore resolve after the original deadline without creating another
run, while a deployment that lost the exact executable still fails closed.

## Load by run or by key

Authenticate and authorize the tenant and requested resource before calling
either method:

```rust,no_run
authorize_run_read(&principal, &tenant_id, requested_run_id)?;
let by_run = runs.load(&tenant_id, requested_run_id).await?;

authorize_submission_read(&principal, &tenant_id)?;
let by_key = runs.load_by_key(&tenant_id, &key).await?;
```

Neither `TenantId`, `RunId`, nor `AgentSubmissionKey` is proof of access. Do not
expose the store or database role directly to untrusted clients. Return
not-found and conflict responses through an application-owned error policy
that does not leak cross-tenant existence.

Both paths load the admission, graph, initial event/checkpoint, current
lifecycle, wait projection, and submission mapping (when applicable) inside a
repeatable-read database snapshot. Canonical bytes and all redundant digests
are rederived before public data is returned.

## Public snapshot contract

`AgentRunSnapshot` intentionally excludes request input, authorization
evidence, graph state, leases, scheduler internals, and private diagnostics. It
contains:

- trusted Agent result provenance, including tenant, run, thread, invocation,
  and exact Agent identity;
- the immutable graph reference and admission digest;
- database-clock admission and latest lifecycle observations;
- the monotonic lifecycle revision and protocol-neutral status;
- a quarantine flag; and
- a required nullable `outcome` field.

Use `(run_id, revision)` as the polling or cache observation key. A repeated
revision represents the same lifecycle state. `outcome` is `null` for `active`,
`waiting`, and `cancellation_requested`; it is present exactly once the run is
`succeeded`, `failed`, or `cancelled`. Omitting the field is invalid, which
preserves the distinction between an older wire shape and a known nonterminal
run.

Successful outcomes contain a fully rebound `AgentResult`. Its provenance,
request, descriptor, output schema, cumulative usage, finite budget, and
digest-pinned output schema are revalidated on every read. Applications may
decode `result.output()` into their generated Rust output type only after this
facade returns it.

Failed and cancelled outcomes expose a public-safe `Failure`, the durable
completion time, and cumulative usage. Cancellation failures must use the
`Cancelled` category and `Never` retry advice; ordinary failures cannot use the
cancellation category. Failed or cancelled usage is not required to remain
inside the budget—the budget or deadline may be the reason the run terminated.

A quarantined run remains observable with `is_quarantined() == true`, but must
not be presented as runnable work. The public read boundary never silently
clears quarantine.

## Migration 16 and rollout

Migration 16 creates `stateknot.agent_submission_keys`, adds the exact
admission reference key, and installs constraints for tenant grammar, UUIDv7
run identities, 32-byte digests, one key per run, and a composite foreign key
to the selected admission. Only the tenant-scoped key digest is stored. The
created-time index supports operational inventory without weakening the
idempotency invariant.

Roll out in this order:

1. back up and test restore procedures;
2. apply migrations 15 and 16 with the migration role;
3. start the binary only after `PostgresStore::verify_schema` accepts every
   version, checksum, table, index, and constraint;
4. deploy the frozen executable/schema registry and register tenant graphs;
5. enable `submit` traffic; and
6. retain old executable snapshots, admission rows, and key mappings for the
   full retry and recovery lifetime.

Never delete a key mapping while a client may retry it: deletion changes a
safe retry into permission to create a second run. StateKnot does not currently
offer an independent key-deletion API. Any future run-retention implementation
must remove the run, admission, and key evidence as one explicitly bounded
operation after all retry guarantees have expired.

## Qualification evidence

The PostgreSQL 16/17 matrix covers a populated migration-15 to migration-16
upgrade, same-key retries with fresh identity bundles, changed-content and
second-key conflicts, 24 concurrent candidates converging on one physical
commit, raw-key non-persistence, mapping tamper detection, and injected mapping
failure rolling back the run, event, checkpoint, admission, and mapping. The
runtime suite covers active and cancellation-requested snapshots, then exposes
verified succeeded, failed, and confirmed-cancelled outcomes. It reloads keyed
runs by both run and key, revalidates output and provenance, and rejects
impossible or incomplete public wire snapshots and invalid terminal failure
kinds.

The current repository contains 106 PostgreSQL provider scenarios and 36
durable runtime PostgreSQL scenarios per supported database version. These are
implementation facts, not a production-support claim; release qualification,
HTTP/SSE service roles, published crates, general retention, and compatibility
guarantees remain before v1.
