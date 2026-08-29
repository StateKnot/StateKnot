<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0002: Deterministic graph execution and scheduling

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-08-29
- Tracking issue: Not yet created
- Supersedes: None
- Superseded by: None

## Summary

StateKnot v1 executes version-pinned typed graphs as bulk-synchronous
supersteps. Every active node in one superstep reads the same immutable state
snapshot. Nodes return bounded typed updates and explicit control outcomes;
they never mutate shared state. Successful node results are persisted before a
barrier, sorted by stable logical identity, reduced deterministically, and then
committed as one new checkpoint with the journal fact that made the barrier
authoritative.

Physical attempts and leases are execution ownership, not logical graph
identity. A crashed or superseded worker can be replaced without changing the
logical node invocation. Results that committed before another parallel node
failed remain pending and are reused. Recovery schedules only missing work and
never repeats a committed model response, tool result, or node update.

The production scheduler uses indexed durable readiness and database fencing.
It does not keep one task or timer in memory for every suspended run, and it
does not treat `SKIP LOCKED` as proof that no work exists.

## Motivation

Agent graphs combine deterministic reduction with nondeterministic model,
tool, human, and remote-agent results. Process-local futures cannot prove which
external effects completed before a crash. An unordered collection of parallel
updates also cannot produce a portable checkpoint when reducers are
order-sensitive.

The contract therefore separates:

1. an immutable, digest-pinned graph definition;
2. a logical superstep and its logical node activations;
3. physical attempts protected by a database lease/fencing epoch;
4. immutable pending results for completed activations;
5. a barrier checkpoint containing the reduced state and next ready set; and
6. the append-only journal and invocation ledgers that remain the source of
   truth for nondeterministic observations.

The design is informed by, but does not copy:

- [Pregel](https://research.google/pubs/pregel-a-system-for-large-scale-graph-processing/),
  which makes iteration boundaries and message visibility explicit;
- [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence),
  which checkpoints thread state at supersteps and retains successful pending
  writes when sibling work fails;
- [Microsoft Agent Framework checkpoints](https://learn.microsoft.com/en-us/agent-framework/workflows/checkpoints),
  which capture executor state and pending messages at superstep boundaries and
  require stable executor identity for rehydration;
- [Temporal deterministic workflow constraints](https://docs.temporal.io/workflow-definition#deterministic-constraints),
  which require the same durable command sequence during replay and isolate
  nondeterministic external work; and
- [Restate's journal and epoch model](https://docs.restate.dev/references/architecture),
  which makes a replicated journal append the durable step boundary and fences
  superseded attempts.

StateKnot owns its graph types, ordering rules, state model, SQL transactions,
and compatibility guarantees.

## Goals and non-goals

### Goals

- deterministic state and routing results for the same graph version,
  checkpoint, and committed external results;
- sequential, conditional, bounded parallel/join, bounded loop, subgraph, and
  pause/resume foundations without hidden shared mutation;
- stable logical node identity independent of worker process or retry attempt;
- durable reuse of every successfully committed node result after a crash;
- checkpoint and journal integrity that fail closed on mismatch;
- bounded fan-out, state, updates, recursion, iterations, and concurrency;
- indexed multi-tenant readiness with explicit admission and fairness policy;
- graph-version pinning for all in-flight runs; and
- a Rust API that preserves typed state/update ergonomics without serializing
  arbitrary Rust values or executable code.

### Non-goals

- arbitrary time travel, historical forks, or a visual workflow editor in v1;
- a YAML/JSON executable workflow language;
- deterministic re-execution of model calls or external side effects;
- distributed shared-memory semantics between nodes;
- dynamic graph mutation by model output;
- user-supplied reducers loaded as database code;
- promising one global execution order across different runs; or
- using in-memory checkpoints or queues in production.

## Terminology and identity

### Graph reference

Every admitted run pins an owner-qualified workflow `CapabilityIdentity`, a
canonical graph-definition digest, and exact input, state, update, and output
`SchemaReference` values. Reusing a capability name/version with different
canonical definition bytes is a registry integrity failure.

The canonical graph definition contains only declarative metadata: stable node
identities, typed ports, edges, conditional route identifiers, reducer
identities/versions, entry nodes, terminal paths, subgraph references, and hard
execution limits. It contains no credentials, closures, source code, prompt
secrets, or deployment coordinates.

### Node identity

A `NodeId` is a case-sensitive, bounded ASCII identifier unique within one
graph version. It is a logical role such as `draft-plan`, not a process ID,
attempt UUID, user identifier, or display label. Renaming a node changes the
graph definition and requires a new graph version or an explicit migration.

One node may activate at most once in one graph namespace and superstep in v1.
Repeated work uses a later superstep or a nested subgraph namespace. This keeps
the logical key closed and unambiguous:

```text
(tenant_id, run_id, graph_namespace, superstep, node_id)
```

### Graph namespace

The root graph uses an empty namespace. A subgraph activation derives a bounded
namespace from the parent logical activation and its stable child slot. A
namespace is data, not a filesystem path, and cannot contain `.`/`..` segments
or unbounded model-generated text. Sharing parent state versus isolated child
state is declared in the graph definition and cannot change during a run.

### Superstep

`Superstep` is a zero-based integer bounded to PostgreSQL signed `BIGINT`.
Checkpoint position zero is the entry checkpoint before superstep zero. A
checkpoint at position `N + 1` is the state after superstep `N` and before
superstep `N + 1`. Position increments exactly once per successful barrier and
never wraps or resets.

Physical `AttemptId` and `FencingEpoch` are deliberately absent from logical
ordering. They are retained as provenance on attempt, invocation, event, and
pending-write records.

## Graph compilation

Compilation is a pure validation step over a trusted, schema-checked graph
descriptor. It rejects the graph before admission when any invariant fails.

Required checks include:

- unique node IDs, route IDs, port IDs, reducer IDs, and subgraph slots;
- at least one entry node and no edge referencing an absent endpoint;
- every node reachable from an entry and able to reach a wait or terminal path;
- exact input/update/output schema compatibility for every edge;
- one explicit reducer for every state channel that accepts multiple writes;
- a deterministic stable order for every join and reducer input;
- every cycle covered by a finite graph-step, loop-iteration, deadline, or
  budget limit that the runtime can enforce before dispatch;
- fan-out, nesting depth, ready-set size, and maximum concurrency below immutable
  framework ceilings and within the run's resolved budget;
- subgraph version/state-sharing declarations closed at compile time; and
- no reserved framework final-output, interrupt, or control identity collision.

The compiler emits canonical descriptor bytes and their SHA-256 digest. Runtime
registries load executable node/reducer implementations only after matching the
exact owner, name, version, kind, schemas, and definition digest.

## Node execution contract

Conceptually, typed nodes implement:

```rust,ignore
pub trait GraphState: Send + Sync + 'static {
    type Update: Send + 'static;

    fn reduce(&mut self, update: Self::Update) -> Result<(), StateError>;
}

pub trait Node<S: GraphState>: Send + Sync {
    fn run(
        &self,
        context: NodeContext,
        state: Arc<S>,
    ) -> BoxFuture<'_, Result<NodeOutcome<S::Update>, NodeError>>;
}
```

These traits are illustrative until the descriptor, erased adapter, and fault
tests compile together. The observable contract is fixed:

- each node receives the exact immutable state from its base checkpoint;
- a node cannot borrow mutable graph state or another node's process-local
  output;
- clock, randomness, model, tool, remote agent, and human input cross durable
  context/ledger APIs and never execute inside a reducer;
- one result contains a bounded typed update plus exactly one explicit control
  outcome: route, wait, terminal, or continue;
- node output is validated against the pinned update schema before it becomes a
  pending result; and
- cancellation or dropped futures do not erase already observed external
  effects, which remain ledger evidence.

The implementation-backed pending-result wire makes these outcomes closed. A
state contribution is either `unchanged` or one schema-pinned, RFC 8785
checksummed bounded JSON update. Control is exactly one of `continue`, one
declared `RouteId`, a non-empty `RunWaits` batch, or schema-pinned terminal
output. `RouteId` selects a conditional branch in the pinned graph definition;
it is not a caller-selected destination node, so dynamic graph mutation remains
impossible.

Every external-result binding contains the exact committed tool/model revision
head and the owning `NodeActivation`. The storage adapter reloads the complete
invocation revision and proves that activation before accepting a binding.
Bindings are bounded, duplicate-free, and canonically ordered by invocation
kind and logical invocation ID. The pending result's journal anchor must
strictly follow the base checkpoint and every bound external result.

The implementation-backed node-attempt contract makes execution ownership
durable before user node code runs. `NodeAttemptStart` binds the logical
activation to a fresh physical node `AttemptId`, the distinct worker
`RunFence`, and an exact journal anchor. Its optional append-only completion is
either a success referencing the pending result committed by the same worker
event or a public-safe failure naming that event as its direct cause. Automatic
retry requires explicit `SafeAfter` advice and a durably late enough successor
start. A start left incomplete by a crash may be replaced only under a higher
worker fencing epoch; success and non-retryable failure are absorbing. The core
history verifier enforces these rules independently of completion order.
PostgreSQL migration 6 implements the corresponding start/completion ledger,
run-wide node/tool/model/outbox attempt uniqueness, exact row-level fencing, and
attempt-owned result commit boundary.

## Parallel execution and barrier commit

All nodes active in one superstep read the same checkpoint. Completion order is
not semantic order.

Each successful logical activation first commits an immutable pending result
under its logical key. Repeating the same logical key and identical result is
idempotent. Reusing it with a different input digest, update digest, route,
wait, terminal outcome, or external-result binding is a conflict and
quarantines the run unless an operator can prove storage corruption did not
occur.

The semantic intent digest covers the exact activation, state contribution,
control outcome, and external bindings, but deliberately excludes physical
ownership. The immutable record separately binds the winning `RunFence` and
journal head. A lost acknowledgement can therefore compare semantic identity
without making a replacement worker's attempt/epoch part of graph semantics,
while the stored winner retains complete stale-writer provenance.

When all required activations for the superstep have committed a result, one
barrier transaction:

1. locks the tenant/run row and verifies the exact live fence;
2. loads the current checkpoint and the complete expected pending-result set;
3. verifies each logical input digest against the checkpoint and graph;
4. sorts results by canonical `(graph_namespace, node_id)` bytes;
5. groups state-channel updates by the compiled reducer plan;
6. applies pure reducers in the compiled stable order;
7. resolves routes and the next ready set in stable node order;
8. validates wait/terminal outcomes against the run lifecycle transition;
9. appends one stable journal event for the barrier;
10. creates the successor checkpoint, marks pending results consumed, updates
    the run checkpoint/journal/lifecycle heads, and enqueues related outbox work;
11. commits, then acknowledges the barrier.

No model, tool, remote agent, reducer plugin, or user callback runs while the
transaction or run row lock is held.

If an activation fails before committing a result, already committed sibling
results remain pending. A replacement worker reuses them and schedules only
missing or explicitly retryable failed activations. Failed physical attempts
never change stable reduction order.

## Checkpoint contract

A checkpoint is an immutable barrier snapshot, not a mutable key/value bag. It
binds:

- tenant and run;
- a stable UUIDv7 checkpoint ID;
- checkpoint position and exact parent checkpoint head;
- owner-qualified graph identity and graph-definition digest;
- exact state-schema reference;
- RFC 8785 canonical inline state bytes and their digest;
- the sorted unique next-ready node set;
- the exact journal head committed at the same boundary;
- database commit observation; and
- a domain-separated checkpoint digest that includes the parent checkpoint
  digest.

The initial checkpoint has position zero and no parent. Every successor names
the exact current checkpoint, increments position by one, retains the same graph
and state schema, and advances the journal sequence. Graph/state migration is a
separate audited operation with a distinct event and migration proof; ordinary
barrier commits cannot smuggle a version change.

V1 inline state uses `JsonLimits::MAXIMUM` and a hard two-MiB canonical-byte
ceiling. State is JSON only after validation by the pinned local schema. Rust
type names, `Any`, pickle, bincode, executable closures, raw credentials, and
provider sessions are forbidden checkpoint content. Blob-backed state remains
deferred until the artifact prepare/commit and retention boundary exists.

The latest checkpoint is usable only after its checkpoint chain validates and
its journal head is proven to be an ancestor of the current verified run head.
A snapshot never overrides a contradictory journal fact.

The current PostgreSQL implementation exposes hard-bounded reverse-lineage
pages. Each page starts from the current run pointer or an exact full-head
continuation, validates newest-to-oldest parent linkage, and fully verifies each
journal anchor. A continuation remains usable after later barrier commits
because checkpoints and their parent identities are immutable.

Pending node results are separate immutable rows anchored to the base
checkpoint. They are intentionally not copied into or used to mutate that
checkpoint. Recovery loads them beside the latest checkpoint and the journal
suffix; a successful barrier consumes them into the next immutable checkpoint.
The current PostgreSQL slice can atomically commit and fully verify one exact
activation result and scan unconsumed results in two-record decoded pages. A
complete cursor pins the base checkpoint, last result head, and observed run
journal head; any concurrent result commit makes continuation stale instead of
allowing a lower canonical key to be skipped. The core `CheckpointBarrier`
binds exact ready-set coverage, canonical result heads, and the successor write.
The store verifies full records outside the run lock, rechecks the complete
compact set under the lock, and atomically appends one consumption proof per
result with the successor event/checkpoint and run heads. Raw successor writes
are rejected.

## Recovery

Recovery under a newly claimed fence performs:

1. load and validate the tenant-scoped run, pinned graph, and current checkpoint
   head;
2. load the exact graph implementation and schema registry entry by digest;
3. verify the checkpoint lineage or a retained trusted archive boundary;
4. seed journal verification from the checkpoint journal head and verify the
   suffix to the current run head;
5. reconstruct lifecycle, usage, invocation, interrupt, and routing projections
   from authoritative facts and compare materialized rows;
6. load unconsumed pending results for the checkpoint's next superstep;
7. recompute the expected logical ready set from the graph and checkpoint;
8. verify every pending result belongs to that set and matches its input digest;
9. expose only missing runnable activations to the scheduler; and
10. quarantine instead of executing if any integrity, schema, graph, tenant, or
    projection check fails.

Recovery does not call a model or tool merely to rebuild an in-memory transcript.
Committed external results are read from their ledgers. An external write with
an unknown outcome remains blocked for reconciliation rather than becoming an
ordinary retry.

## Wait, resume, and cancellation

An interrupt or timer is a durable record plus a journal fact. The barrier that
enters `Waiting` commits a checkpoint from which execution can be reconstructed.
Resolution is authenticated, version-checked, and committed as a journal fact.
A later worker claim resumes from the same pinned graph and checkpoint, applies
the recorded resolution through the declared node path, and cannot reinterpret
the approval payload as changed tool arguments.

Cancellation follows the `RunLifecycle` race rules. The scheduler stops
dispatching new activations after a committed cancellation request. Late node
or external results are retained as evidence but cannot overwrite an absorbing
terminal outcome. Cleanup may run only through explicit, policy-authorized
ledger operations.

## Scheduler and fairness

Readiness is durable indexed data on the run/logical-activation boundary. A
candidate includes tenant, not-before time, admission class, bounded priority,
required worker capabilities, graph version, and current lease state.

Scheduler replicas may use short `FOR UPDATE SKIP LOCKED` batches. They must:

- treat skipped rows as contended, not absent;
- allocate a stable attempt ID before the claim transaction;
- claim through the RFC-0003 database clock and fencing epoch;
- enforce global, tenant, admission-class, model/provider, and tool concurrency
  limits before dispatch;
- stop admitting work when storage or downstream capacity crosses configured
  overload thresholds;
- avoid per-run polling for waiting runs; timers use an indexed due time and
  signals/outbox make resolved work visible; and
- publish queue age, claim latency, skipped/contended counts, saturation, and
  per-tenant service shares.

The exact weighted-fair scheduling algorithm and reference-load starvation
bound remain an acceptance blocker for this Draft. Priority alone is not a
fairness policy, and a large tenant cannot consume every worker slot.

## Versioning and compatibility

Admission snapshots the exact graph descriptor and implementation digest. New
deployments route new runs to the new version while compatible workers for old
versions drain existing runs. A worker that cannot load the pinned version does
not claim the run.

An explicit graph migration must define source/target graph and schema digests,
transform state and pending records deterministically, append an audit event,
create a new lineage boundary, and be reversible only within documented limits.
Changing node identity, reducer order, route meaning, or state schema under the
same graph version is forbidden.

Serialized graph, checkpoint, pending-result, and attempt records require
N-1/N-2 fixtures before a compatibility claim. Unknown optional fields are not
accepted by closed core types; format evolution uses explicit versions.

## Security and resource boundaries

- every durable key and query begins with `tenant_id`;
- authorization precedes graph/run existence disclosure;
- graph and schema URIs are identities resolved only from trusted local
  registries, never runtime fetch instructions;
- state, updates, routes, errors, and extension data are bounded before
  allocation and redacted in diagnostics;
- checkpoint contents never include raw credentials, access tokens, private
  error chains, or unredacted provider transport objects;
- workers cannot select the control-plane append path or a different graph
  version; and
- every worker checkpoint, pending result, attempt mutation, invocation change,
  and outbox enqueue repeats the exact attempt/epoch/unexpired predicate in SQL.

## Validation and rollout

Before this RFC can be accepted, executable evidence must cover:

- closed JSON Schema and canonical fixtures for graph/checkpoint scalar and
  integrity types;
- property/model tests proving insertion/completion order cannot change barrier
  state, routes, checkpoint bytes, or digests;
- sequential, conditional, parallel/join, bounded loop, subgraph, wait/resume,
  cancellation, and terminal graphs;
- kill points before and after pending-result, journal, checkpoint, lifecycle,
  and outbox writes;
- retry reuse of successful siblings with no repeated committed model/tool work;
- conflicting logical result and graph/schema drift quarantine;
- 10,000 forced lease-race trials with zero accepted stale writes;
- 100,000 suspended-run readiness without per-run resident tasks or polling;
- reference-load fairness, queue latency, recovery-time, and overload reports;
- PostgreSQL failover and point-in-time restore integrity validation; and
- N-1/N-2 graph/checkpoint migration fixtures and rollback-limit tests.

## Alternatives considered

### Execute nodes sequentially in completion order

Rejected because it makes parallel completion timing observable graph semantics
and destroys reproducibility.

### Let reducers mutate shared state behind a mutex

Rejected because mutex acquisition order becomes semantic order and process
memory cannot survive recovery.

### Re-run every node in a failed superstep

Rejected because successful siblings may already contain costly or externally
side-effecting results. Their committed logical results must be reused.

### Store arbitrary Rust values

Rejected because implementation-specific serializers, type names, and code
layouts do not form a language-neutral, schema-pinned durable contract.

### Use only checkpoints without a journal or ledgers

Rejected because a mutable snapshot cannot prove which nondeterministic
external observations or side effects committed.

## Open decisions

- the exact weighted-fair queue algorithm, policy snapshot, and starvation
  threshold at reference load;
- the minimal public typed graph builder API after erased descriptor validation;
- the bounded subgraph namespace wire grammar and shared-state declaration;
- reducer registration/versioning ergonomics and conformance harness; and
- whether a future accepted RFC adds blob-backed checkpoints after artifact
  lifecycle, encryption, and retention are implemented.

These decisions can change public types or production behavior, so this RFC
remains Draft and no stable graph API is claimed.
