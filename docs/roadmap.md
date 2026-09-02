<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot roadmap

> Current phase: M0 architecture contracts with implementation-backed vertical
> validation. StateKnot has no production release or stable API yet.

The roadmap is evidence-driven. Dates may be estimated in planning discussions,
but a milestone exits only when its acceptance evidence is committed or linked
from the repository.

## Current M0 tracking

- [x] Freeze the [v1 scope and explicit non-goals](v1-scope.md).
- [x] Define the three [qualification scenarios](scenarios/README.md), shared
  reference environment, loads, failure matrices, and release thresholds.
- [ ] Accept RFC-0001 for the core domain and capability model.
- [ ] Accept RFC-0002 for deterministic graph and scheduler semantics.
- [ ] Accept RFC-0003 for PostgreSQL durability, recovery, and migration.
- [ ] Accept RFC-0004 for MCP/A2A identity and security mapping.
- [x] Validate the first protocol-neutral run lifecycle, interrupt/timer wait,
  cancellation-race, terminal-outcome, schema, property, and wire contracts in
  the unpublished `stateknot-core` crate.
- [x] Validate RFC 8785 payload bytes, journal append identity/head/hash-chain,
  lease renewal, fencing epoch, stale-attempt, schema, property, and wire
  contracts in the unpublished `stateknot-core` crate.
- [x] Implement the first PostgreSQL 16/17 run/journal/lease slice with exact
  migration startup checks, atomic locked transitions, lost-ack idempotency,
  database-level worker predicates, injected rollback validation, and a
  100-appender contiguous-history test.
- [x] Implement immutable graph/state checkpoints with exact parent and journal
  anchoring, projection-bound retry identity, atomic control-plane/worker commit,
  corruption and rollback rejection, stale-worker fencing, and a 24-writer
  linear-chain test on PostgreSQL 16/17.
- [x] Implement bounded reverse checkpoint-lineage recovery with exact durable
  cursors, repeatable-read pages, batched journal-anchor verification, safe
  continuation across later barrier commits, and corruption tests on PostgreSQL
  16/17.
- [x] Implement the tool-invocation ledger with immutable intent snapshots,
  prepared/executing/committed/failed/unknown transitions, safe-retry and
  reconcile-first rules, exact checkpoint/journal anchors, atomic fenced
  PostgreSQL commits, lost-ack convergence, bounded verified history, rollback,
  corruption, exact-checkpoint advancement guards for unsettled calls, and
  24-writer race tests on PostgreSQL 16/17.
- [x] Validate the core model-invocation ledger with exact activation,
  descriptor, request, physical-attempt, response/error, journal, predecessor,
  and digest binding; explicit delayed retries; complete history replay; and
  frozen cross-version integrity fixtures.
- [x] Persist the model-invocation ledger on PostgreSQL with immutable intent
  snapshots, compact hash-linked revisions, exact journal/checkpoint/provenance
  binding, delayed retry, cancellation-race completion, lost-ack convergence,
  rollback/corruption rejection, checkpoint guards, 24-writer races, and a
  run-wide tool/model `AttemptId` registry whose v3 backfill and exact foreign
  keys are verified on PostgreSQL 16/17.
- [x] Validate immutable core pending node results with exact activation and
  worker-fence provenance, bounded schema-pinned update/terminal payloads,
  closed continue/route/wait/terminal control, committed activation-bound
  tool/model references, causal journal anchors, semantic idempotency, closed
  schemas, tamper tests, and a frozen canonical-wire digest fixture.
- [x] Validate durable core node-attempt starts and append-only completions with
  separate node/worker physical identity, exact journal causality, atomic
  pending-result success binding, public-safe failures, explicit delayed retry,
  higher-epoch recovery of unfinished work, closed schemas, tamper tests, and
  frozen success/failure wire fixtures.
- [x] Persist PostgreSQL pending node results with an immutable semantic key,
  exact worker fence and journal anchor, activation-bound committed tool/model
  foreign keys, full fail-closed recovery, lease-takeover idempotency,
  cancellation/corruption/rollback tests, and a 24-writer race on PostgreSQL
  16/17.
- [x] Bind checkpoint-barrier inputs to the exact base ready set, canonically
  ordered result heads, and successor write; expose bounded stable-snapshot
  PostgreSQL scanning whose journal-pinned cursor cannot miss concurrent
  lower-key result commits.
- [x] Atomically consume complete pending-result barriers with lock-free full
  record preflight, locked compact-set revalidation, append-only consumption
  rows, projection-bound idempotency, per-statement worker fencing, rollback
  injection, unsettled-invocation guards, and 24-writer linear-chain tests on
  PostgreSQL 16/17; reject every raw successor-checkpoint write.
- [x] Persist the PostgreSQL node-attempt ledger with durable-before-dispatch
  starts, append-only success/failure completion, attempt-owned results,
  database-time delayed retry, higher-fence crash takeover, bounded verified
  history, run-wide node/tool/model identity, migration-5 truth preservation,
  lost-ack convergence, corruption rejection, and atomic barrier integration.
- [x] Persist an indexed tenant-level runnable projection and expose
  16-record fixed-database-time keyset pages with complete run decoding,
  lease-expiry availability, release/lifecycle requeue timestamps, terminal
  removal, cross-tenant cursor rejection, migration-6 backfill, corruption
  guards, and a 24-scheduler single-winner lease race on PostgreSQL 16/17.
- [x] Persist the transactional outbox with immutable tenant destination
  snapshots, event-and-delivery atomicity, run-wide durable-before-dispatch
  attempt claims, fixed non-renewable fencing, at-least-once lost-ack recovery,
  database-time safe-after/dead-letter/expiry projection, a hard 64-attempt
  bound, bounded verified history, v7 upgrade preservation, indexed claim/reap
  paths, and corruption/rollback/24-worker tests on PostgreSQL 16/17.
- [x] Validate integrity-bound core interrupt request/resolution and durable
  timer registration/firing records with exact journal causality,
  schema-pinned bounded payloads, action digests, principal/scope subset
  authorization, exclusive interrupt expiry, inclusive timer due time, closed
  schemas, tamper tests, property tests, and frozen cross-version wire digests.
- [x] Persist interrupt/timer registration, resolution, firing, and explicit
  cancellation/failure abandonment atomically with initial or successor
  wait-barrier checkpoints; reject generic projection bypass, expose bounded
  indexed due/expiry discovery and exact audit loads, quarantine evidence-free
  legacy waits, and prove migration, lost-ack, authorization, corruption,
  rollback, fencing, and 24-request convergence on PostgreSQL 16/17.
- [x] Persist an audit-grade run quarantine outside a potentially corrupt
  journal with stable identity, closed causes, bounded non-secret component
  codes, evidence and record digests, exact journal-observation fencing,
  atomic lease removal/runnable exclusion, legacy-evidence honesty, lost-ack
  convergence, rollback/corruption rejection, a 24-request race, and a
  corruption-only recovery-read combinator that rejects stale observations on
  PostgreSQL 16/17. Migration 11 further preserves v1 evidence while adding
  optional exact attempt/epoch binding and a v2 digest; a fence-bound claimed
  recovery surface scopes checkpoint/journal/invocation/node-result reads to
  one stable quarantine intent, revalidates handoff, and proves that a
  superseded worker cannot quarantine its successor.
- [x] Derive canonical root-node activations from the exact checkpoint, stream
  completed results and complete attempt histories through a bounded recovery
  planner, classify every ready node as completed/dispatchable/deferred/
  in-flight/failed/exhausted using one live fence and database time, enforce a
  64-attempt hard ceiling in planning and mutation, bind the canonical plan to
  its final journal observation, quarantine contradictions, and commit a
  plan-selected physical start before node code. Property-order invariance,
  crash takeover, result reuse, lost-ACK, scope/limit rejection, and a
  24-request single-start race pass on PostgreSQL 16/17.
- [x] Project deferred-only recovery plans into an independent durable
  `scheduler_not_before` gate while preserving queue age and atomically
  releasing the exact live lease; reject direct early claims, expose due work
  through the tenant scheduler index without polling writes, converge lost-ACK
  retries, retain ownership when the boundary becomes due during commit, and
  prove exact v11 migration, corruption, and runtime behavior on PostgreSQL
  16/17.
- [x] Compile bounded declarative root graphs into canonical digest-pinned
  definitions, derive completion-order-invariant reducer/route/wait/terminal
  barrier intents, and persist immutable tenant-scoped graph versions through
  migration 13. Claimed recovery recompiles the checkpoint-pinned bytes,
  rejects projection or ready-set drift, and quarantines missing/corrupt graph
  evidence under the exact live fence; migration, corruption, tenant-isolation,
  idempotency/conflict, and concurrent-registration behavior pass on PostgreSQL
  16/17.
- [x] Freeze an offline digest-pinned JSON Schema 2020-12 registry and exact
  graph/reducer/node executable closure; reject missing, mismatched, duplicate,
  unresolved, and orphan deployment bindings before work is claimed.
- [x] Independently replay every committed noninitial checkpoint through its
  exact compiled graph, state schema, and reducer under bounded memory, and
  quarantine divergent or incomplete durable history beneath the live fence.
- [x] Drive the root graph recovery loop through durable-before-dispatch starts,
  same-fence duplicate suppression, pre-launch lease refresh, monotonic lease
  expiry enforcement, renewal, monotonic cancellation,
  success/failure commits, delayed wakeups, automatic Continue barriers, and
  typed lease-bound Wait/Terminal/failure handoffs. Root-to-terminal replay,
  long execution beyond the original lease, and higher-fence crash takeover
  pass on PostgreSQL 16/17.
- [x] Commit typed Wait/success/failure lifecycle handoffs through a stable
  event identity and exact retained fence; materialize complete Wait batches
  from database time, validate trusted terminal admission/accounting evidence,
  release same-fence in-flight work for higher-fence recovery, converge lost
  acknowledgements, and clear terminal ownership in the same transaction on
  PostgreSQL 16/17.
- [x] Bind Driver and lifecycle coordination into a bounded durable Agent Loop,
  release exact ownership after execution/evidence errors, and implement one
  tenant-scoped stable-snapshot scheduler tick with bounded paging, stable
  claim identities, normal contention skips, and closed run-local outcomes.
- [x] Implement provider-neutral first-party model/tool attempt execution with
  exact immutable provider registries, trusted budget admission, unary and
  durably-sunk streaming models, tool ambiguity preservation,
  durable-before-dispatch starts, lost-ACK duplicate suppression, and
  no-dispatch terminal recovery on PostgreSQL 16/17.
- [x] Implement replica-safe cross-tenant smooth weighted scheduling with an
  immutable PostgreSQL policy/cursor, globally ordered lost-ACK-safe
  reservations, exact per-cycle shares, explicit reservation-count starvation
  bounds, bounded database-time retention, property tests, and PostgreSQL 16/17
  concurrency and runtime evidence.
- [x] Implement the first production-shaped OpenAI Responses and Anthropic
  Messages unary/SSE adapters plus generated, canonical digest-pinned typed
  Agent input/output contracts, offline provider-profile binding, bounded
  codecs, real local HTTP fault tests, and compiled no-I/O examples.
- [x] Implement bounded provider-native unary model/tool transcripts with exact
  OpenAI Responses and Anthropic Messages replay order, encrypted reasoning
  continuation, durable outcome pairing, identity/digest binding, semantic
  replay validation before I/O, and corruption/cross-provider tests.
- [x] Implement immutable authenticated Agent admission intents, deterministic
  finite-budget resolution, a standard public-safe audit schema, and migration
  15's all-or-nothing PostgreSQL commit of database time, Active lifecycle,
  sequence-one event, superstep-zero checkpoint, graph/ready-set anchors, and
  scheduler projections. Exact retries, late rollback, tamper detection,
  24-request convergence, and the runtime validation facade pass on PostgreSQL
  16/17.
- [x] Implement the durable public run/result facade and migration 16's
  tenant-scoped ingress idempotency mapping. Same logical content with fresh
  candidate IDs resolves the original run; changed content or a second key for
  one run conflicts; 24 concurrent candidates converge; mapping failure rolls
  the whole admission back; and active, cancellation-requested, succeeded,
  failed, and confirmed-cancelled public snapshots are fully revalidated on
  PostgreSQL 16/17.
- [x] Complete the prebuilt provider-native multi-turn graph with sequential
  Tool execution, provider-native transcript reconstruction, digest-pinned local
  policy, deterministic exact accounting, no-redispatch terminal recovery,
  known failed-Tool continuation, durable cancellation observation, exact-usage
  confirmation, lost-ACK replay, and PostgreSQL 16/17 evidence.
- [x] Implement `AgentServiceV1` as an exact-version, authorization-first
  embedding boundary for tenant-scoped submission recovery, verified run/key
  reads, and caller-retained two-phase cancellation. Missing deployment/run/key
  existence remains hidden behind authorization, and the service itself never
  dispatches model or Tool code.
- [x] Implement the first strict MCP 2026-07-28 client-side Remote Tool profile
  with modern stateless discovery, complete JSON transport, exact local
  schema/server pins, attempt-scoped authorization, bounded catalog/body/time
  policy, annotation distrust, and reconciliation-first ambiguous writes.
- [ ] Complete artifact retrieval, parallel sibling/Tool ordering, output
  repair, loop/subgraph semantics, stable HTTP/gRPC/SSE Agent transport,
  protocol-specific outbox adapters, the combined PostgreSQL-backed MCP
  recovery proof and official conformance report, role isolation, general
  retention, failover, restore, and final stale-race gates.
- [ ] Compile the four public contract examples against the proposed APIs.
- [ ] Commit the benchmark harness and fault-injection matrix.

## M0 — Architecture contracts

Deliverables:

- three golden scenarios: an internal tool agent, a long-running approval flow,
  and cross-organization A2A collaboration;
- explicit load, latency, data-size, tenancy, and failure assumptions for each
  scenario;
- accepted RFCs for the core domain, deterministic graph execution,
  PostgreSQL durability, and MCP/A2A identity and security mapping;
- a recorded decision excluding built-in RAG ingestion and vector database
  adapters from v1;
- reference hardware and measurable performance/recovery thresholds.

Exit criteria:

- example APIs compile against proposed contracts;
- state transitions and external-side-effect guarantees are unambiguous;
- schema migration, retention, RPO/RTO, authentication, policy, and scheduler
  fairness have executable acceptance plans;
- unresolved questions capable of changing public types are closed.

The scope decision excludes built-in RAG ingestion and vector database adapters
from v1. Retrieval remains available through ordinary local or MCP tools.

## M1 — Production-shaped vertical slice

Build one complete path:

```text
HTTP/SSE -> durable typed graph -> model -> MCP tool -> checkpoint
         -> approval interrupt -> resume -> A2A agent -> artifact
```

The slice must use PostgreSQL, survive process termination at every persistence
boundary, reject stale worker writes through fencing, and reuse committed model
and tool results after recovery. Fake-model tests run on every pull request;
live-provider tests run only through controlled credentials and budgets.

Exit criteria include committed fault-injection results and the applicable MCP
and A2A conformance reports. A happy-path demo alone does not complete M1.

## M2 — Stable core and runtime

- typed content, model, tool, agent, and run contracts;
- deterministic sequential, conditional, parallel/join, loop, subgraph, and
  pause/resume semantics;
- PostgreSQL journal, checkpoints, pending writes, leases/fencing, invocation
  ledger, outbox, migration, and retention;
- OpenAI-compatible and Anthropic adapters;
- testkit, policy, budgets, cancellation, deadlines, and OpenTelemetry.

## M3 — Protocol and server readiness

- MCP client/server support for the declared version profile;
- A2A REST/JSON-RPC client/server, Agent Card, task, artifact, stream, cancel,
  subscription, and reliable push behavior;
- authenticated HTTP/SSE API, worker and scheduler roles, graceful drain,
  health/readiness, backup/restore procedures, and tenant isolation;
- published compatibility matrix and security test evidence.

## M4 — Release candidate

- all release gates in the implementation plan pass;
- cross-platform/MSRV CI, soak and failure tests, migration tests, SBOM,
  provenance, signed artifacts, and third-party notices are verified;
- at least two distinct production pilots validate the supported scope;
- API, support, deprecation, vulnerability-response, and release policies are
  documented for the candidate version.

## Explicitly deferred

Until real demand proves otherwise, v1 does not include time-travel/fork APIs,
a third model provider, a second vector database, AG-UI/MCP Apps/A2UI adapters,
AGNTCY/SLIM/AP2 integration, alternative durable runtimes, a plugin market, a
visual workflow editor, or a built-in code-execution sandbox.
