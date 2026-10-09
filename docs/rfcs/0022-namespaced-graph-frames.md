<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0022: Namespaced same-Run graph frames

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-10-09
- Tracking: [Issue 149](https://github.com/StateKnot/StateKnot/issues/149), [R1 G3/G4/G5/G6 and child ownership](../r1-contract-gap-ledger.zh-CN.md)
- Supersedes: None
- Superseded by: None

## Summary

Execute a declared subgraph in an isolated durable frame of the same tenant and
Run. The declared graph-call closure is acyclic and version-pinned. A frame
pins its caller activation, child slot, graph implementation and schemas. Its own checkpoint position and node identities cannot alias the
caller. Returning a verified child output publishes one ordinary parent node
result; parent state changes only at the parent's subsequent barrier.

The proposed first profile has one active frame stack. A graph that declares
nested calls must have parallelism one. A leaf graph without nested calls may
still use existing bounded parallel nodes. This rule is checked before admission;
there is no implicit serial fallback for a parallel caller. Waiting in the leaf
persists its exact scope, releases execution ownership and resumes that scope
after process replacement. No suspended stack retains an application future.

This document, its private constructor prototype and experimental Core frame
data types do not add runtime support.
The RFC remains Draft until the semantics and acceptance gates below are
resolved and reviewed. Existing root graphs and static shared-state composition
retain their current supported source contracts.

## Motivation

[RFC-0002](0002-deterministic-graph-and-scheduler.md) already specifies a
same-Run namespace derived from a caller activation and child slot. The existing
`GraphNamespace` scalar supports bounded non-root names, but the execution path
is root-only: `NodeActivation::for_ready_root`, the ready-node recovery planner,
barrier validation and PostgreSQL attempt/result checks derive root activations.
`run_checkpoints` also has a unique `(tenant_id, run_id, superstep)` key and one
Run checkpoint pointer. Interrupt/timer lifecycle is Run-wide.

Removing a root check or changing only that unique key would leave checkpoint
scope, wait restoration, parent continuation and terminal ownership unverified.
Static shared-state composition expands ordinary root nodes and has a different
visibility contract. An isolated child Run has another identity and ownership
contract. Neither provides the proposed same-Run frame semantics.

## Goals and non-goals

- Pin a complete nested implementation closure before Run admission.
- Preserve same-Run authority, accounting, cancellation and external-effect
  ledgers without granting a child a wider policy or a fresh budget.
- Isolate child state and local checkpoint positions; persist caller continuation.
- Recover a waiting, running or returning frame under a newer fence without
  repeating committed node, model or Tool work.
- Commit frame admission and return atomically with their journal facts, and
  reject namespace, registry, schema and continuation substitution.
- Enforce finite nesting, frame starts, state, replay memory and execution work.

This profile does not add parallel frame stacks, mutable shared parent state,
model-selected executable graphs, recursive graph-call cycles, cross-owner calls, a workflow language,
independent child Runs, time travel or new production capacity guarantees.
Child-Run APIs used inside a frame still require their existing ownership/Join
contract plus the explicit frame integration gates below.

## User-facing design

A trusted compiled graph declares each call site, a stable `NodeId` child slot,
an exact child `GraphReference` and one explicit parent return route. A call site
is framework-owned and has no application executor. Registering an executor for
that call site is a conflict. The declaration and finite frame limits enter the
parent definition digest; they cannot live in an unbound registry side table.

For the isolated-snapshot profile, parent state, child input and child state
schema references must be exactly equal. The child output schema must equal the
parent update schema. The offline registry validates those values before frame
admission and return. A child terminal output becomes a parent update followed
by the declared route; a child failure follows Run-wide failure close. No
implicit adapter, schema coercion, state reset or dynamic return route is used.

The child graph owner must be the same exact principal as the caller graph
owner. The Run's resolved scopes, provider/Tool bindings and deadlines continue
to apply. The entire child implementation closure is part of the trusted caller
implementation, not a grant inferred from an arbitrary graph reference.

The current compilable identity prototype is
[`nested_frame_identity_prototype.rs`](../../crates/stateknot-core/tests/nested_frame_identity_prototype.rs):

```console
cargo test -p stateknot-core --test nested_frame_identity_prototype --locked
```

Its private `FrameIdentity` constructor uses real bounded Core identities and
checkpoint constructors. Five checks establish repeatable logical scope,
distinct slots/parents, finite full-digest namespace segments, tenant/Run/graph
binding, same-owner rejection and changed-pin conflict. These are constructor
checks, not a public API, wire release, authorization decision or SQL qualification.
The experimental Core implementation in
[`graph_frame.rs`](../../crates/stateknot-core/src/graph_frame.rs) adds
`GraphFrameIdentity`, `GraphFrameCheckpoint` and `GraphFrameCheckpointHead`.
Constructors and map-only readers validate the exact owner/state-schema pins,
full-digest ancestor path, tenant/Run/graph scope, journal order and reconstructed
checksums. `GraphFrameCheckpoint::activation` derives ready-node identity from
the full scoped checkpoint using the existing activation digest domain;
`verify_successor` requires the same frame and exact predecessor. Compact heads,
like existing checkpoint heads, cannot independently prove omitted state or
journal authenticity. Admission must additionally validate the declared call,
caller readiness and active lease inside the store transaction.

[`graph_frame_contracts.rs`](../../crates/stateknot-core/tests/graph_frame_contracts.rs)
provides nine deterministic checks and two independent 256-case checksum/chain
models, including seven levels of actual scoped ready-node derivation. The three
new readers have current-source fixtures, separate input/output schema pins and
the same closed inventory/fuzz oracles as existing readers. Existing root data
and input/output pins remain exact. These types are experimental while this RFC
is Draft; they do not expose a supported nested execution path. `GraphFrameCall` and `GraphFrameCallPolicy` now declare the exact target, fixed
return route, seven-level depth ceiling and at-most-4,096 lifetime frame starts.
`CompiledGraph::with_frame_calls` validates serial callers, owner/schema pins
and call-site controls, then binds those declarations into a new definition
digest. Static shared-state expansion refuses to discard isolated frame calls.
Both generated schema profiles for `CompiledGraph` and
`ChildRunAdmissionIntent` explicitly change; old graphs without calls retain
their exact canonical wires and definition digests. Registry checks validate
actual target pins, acyclic relative depth, finite closure bytes and application
executor conflicts. The complete registry/driver execution path still requires
transactional frame integration before acceptance.

The scoped pure recovery and barrier path is implemented experimentally.
`ReadyNodeRecoveryPlanner::for_frame` derives exact ready activations and retains
the frame in its plan; results from a sibling or root scope cannot be reused.
`GraphFrameBarrier` is a separate bounded, map-only intent with exact scoped
activation/input identity, ready-set coverage, local predecessor and its own
checksum. `CompiledGraph::plan_frame_barrier` reuses the existing pinned
schema/reducer/control checks. The legacy root barrier still rejects nested
results. Four recovery checks, five barrier checks and a 256-case independent
sum/canonical-preimage model cover these pure paths. Actual entry, return, waits,
SQL authority and fault qualification are still required.

`GraphFrameCall::prepare_root_entry` and `prepare_frame_entry` now check the
exact pinned declaration, target, parent graph/local position and serial ready
caller, then derive the logical frame and copy its state into a separate initial
checkpoint intent. Repeating preparation with another candidate checkpoint ID
preserves the logical frame. Root/scoped return preparation requires the exact
caller/frame and child barrier base plus a validated terminal plan; the child
output becomes a parent update with only the declared return route. Parent state
does not change during preparation. Eleven real-constructor call checks include
these boundaries, rejected substitutions and continued-child rejection. These
methods return existing data types and add no wire/schema profile or dispatch
authority; the complete compound store transaction remains required.

Migration 27 implements scoped relational keys without rewriting old root
canonical checkpoint bytes. Same-scope predecessor FKs additionally bind frame
identity, activation/result/invocation/ownership/Join references include their
namespace, and generated empty-namespace columns fix Run and Agent root
pointers. Existing root queries explicitly select the empty namespace and root
decoding rejects non-null frame columns. Exact catalog verification covers all
21 new constraints, eight columns and seven unique indexes; existing Join
catalog evidence changes only for its two added FKs. PostgreSQL 16/17 tests use
real constructor bytes and journal anchors to check nonempty source-schema
26→27 preservation, separate local position zero, crossed parent/attempt/root
references and catalog drift. Their manually inserted scoped rows establish
relational guards, not frame admission, compound journal authority, nested
execution or true historical N-1/N-2 compatibility.

The Rust-only `GraphFrameEntryPlan` and `GraphFrameEntry` now define a closed
compound entry binding. The pre-event intent hashes
`stateknot-graph-frame-entry-intent-v1\0 || JCS({ version, parent_graph, frame,
checkpoint_intent_digest, caller_attempt_id, fence })`. The version-1
`graph-frame-entered` payload adds this intent digest and uses the generated
output schema at `https://stknot.com/schemas/core/graph-frame-entry-event/1.0.0`,
pinned from RFC 8785 schema bytes. It carries no copied state. Materialization
requires the exact next event and predecessor digest for the observed Run head,
matching tenant/Run, worker source, schema and complete payload. The record
hashes `stateknot-graph-frame-entry-record-v1\0 || JCS({ intent_digest, event,
caller_start_digest, checkpoint })`, where `checkpoint` is the full scoped head.
Both components share the event anchor; neither legacy component digest is an
acceptable compound projection. Reload verification recomputes the complete
record and checks the exact start and scoped checkpoint independently.

Nine Core constructor checks cover independent intent/record preimages,
physical identity/fence changes, crossed/replaced observations, closed event
profiles, event predecessor substitution, independently valid changed components
and actual scoped entry. The generated event producer is additionally checked
against the production offline schema registry. These Rust-only types add no
public Serde reader or changes to existing public type schema pins. The store
must still reload admitted closure/active leaf/inherited bounds and repeat the
live fence in its complete atomic admission transaction; these constructors do
not persist an entry or grant launch authority.

Ordinary PostgreSQL append, checkpoint and node-start APIs reserve this event
kind and reject it instead of storing a legacy component projection. A native
database check covers both control-plane and worker initial-checkpoint paths,
ordinary appends and node starts, including rollback of every partial fact.
Migration 28 adds an experimental dedicated Store admission transaction. It
locks the admitted Run and reloads the actual pinned acyclic call closure,
caller checkpoint, active leaf, inherited absolute-depth/start ceilings and
settled child-account usage. It commits the event, framework claim/start,
isolated initial checkpoint, immutable entry, scoped head, lifetime counter and
Run journal head together. Root checkpoint bytes and its pointer remain exact.
The final write repeats the live fence and admission deadline with PostgreSQL's
clock. It rejects unresolved Run effects and undischarged child ownership.

The Store projection additionally authenticates the admitted Root digest,
Run-wide ordinal, inherited ceilings, parent namespace, observed journal head,
trusted complete DIRECT observation, settled delegated usage and child-account
digest. Its intent domain is `stateknot-postgres-frame-entry-scope-v1\0` over
`JCS({ core_intent_digest, scope, budget })`; its compound domain is
`stateknot-postgres-frame-entry-compound-v1\0` over
`JCS({ scope_intent_digest, core_record_digest })`. The existing closed Core
entry-event profile remains exact; the event's Store projection binds this
complete context rather than a legacy component or the Core record alone.
Private entry bytes are capped at 65,536 bytes and must be strict canonical
objects. Reload checks the whole event/start/scoped checkpoint, registered
closure and at most seven authenticated ancestor entries. It rejects damage
before recovery can report an idempotent commit.

`direct_usage` is a trusted, complete Run-wide DIRECT-only observation at the
exact observed head, including previous frame charges and excluding child-Run
subtrees. The transaction loads actual settled delegated charges and enforces
at least the real Root admission's event/checkpoint/depth usage plus every
active ancestor's entry charges. It charges one graph step per entry, actual
canonical event bytes, scoped checkpoint/head bytes, and the absolute depth
high-water mark. These are inherited Run charges; entry does not allocate a
fresh budget. Deriving all provider usage and unknown costs from durable
ledgers remains an independent R1 child-accounting gate.

Lost-acknowledgment recovery authenticates the existing immutable bundle before
fresh schema callbacks, candidate IDs, readiness or lease checks. It verifies
the same logical frame, target and initial state, returns the original evidence
and never grants another launch. An expired lease or unavailable schema
callback cannot turn that recovery into a new admission.

Deferred SQL guards require all admission components and reject incomplete
scoped checkpoint advances, substituted frame heads, legacy wait writes during
an active child, suspended-parent dispatch and Root continuation/terminal
projection. The schema-30 trusted-server role profile gives the runtime only
SELECT/INSERT on immutable entries/barriers/caller bindings and enumerated mutable head/stack columns.
Exact catalog checks cover installed columns, constraints, indexes, functions
and enabled triggers. Root-only source fixtures explicitly remove migration
30/29/28 before reconstructing older schemas and refuse retained actual frame data.
They do not establish historical-binary downgrade or compatibility.

The experimental Store also starts ordinary ready nodes within an authenticated
active frame checkpoint and atomically commits their success/result or
failure. It reuses bounded physical histories, safe-retry rules and fencing.
A fresh committed start grants launch authority; an idempotent start remains
in flight. Historical scoped starts/results authenticate their complete entry
and checkpoint without requiring the old frame to remain the active leaf.
Ordinary execution and completion APIs reject framework-owned call nodes.
The SQL completion guard protects the active namespace and suspended caller.
Deferred component guards run before the final database-clock lease/deadline
check, so a slow deferred constraint cannot grant launch after lease expiry.
The stack CHECK expression retains the same bounds in the exact catalog after
logical dump/restore.

Scoped continuation/terminal barriers commit the full result set and
consumptions, one worker event, successor, bounded immutable barrier witness
and active frame head atomically. Schema/reducer callbacks finish against the
actual admitted graph and full results before mutation locks; the locked commit
repeats active leaf, base, observed journal, unresolved effects, shared DIRECT
floor, settled children and live fence/budget. Deferred guards execute before
the last database-clock lease/deadline check. No Root pointer or Run terminal
transition is granted. Scoped waits still require a whole suspension record.

Recovery walks forward from the whole entry, authenticates every exact frame
successor, complete result owner/binding/consumption and journal predecessor,
and releases previous state/result buffers per edge. The admitted graph,
original Run graph-step budget, existing 64 MiB replay-result ceiling and 4 MiB
barrier witness bound cap this path. Ordinary scoped starts/results can now
bind to those authenticated successors; new nested calls retain the actual
parent successor and shared usage floor. Schema 29 pins the whole barrier
catalog and trusted runtime ACL inventory. Source-28 migration fixtures retain
nonempty actual scoped history but do not execute a historical binary.

Framework caller rebinding now atomically commits a reserved worker event,
a whole immutable binding, a new physical node start/claim and the Run journal
head. The existing frame identity, activation and current child checkpoint
stay exact. Recovery authenticates the original entry and every physical
binding, actual starts/claims and journal predecessors; compact checkpoint
heads receive independent full lineage verification without recursive result
ownership replay. Original and rebound framework callers cannot complete
through ordinary APIs. A committed epoch is recovered before new observation,
usage or lease checks and grants no application dispatch permission.

A fresh binding requires the actual active leaf, admitted graph closure,
monotonic complete DIRECT usage, settled child accounting, live database fence
and original deadline/budget. It charges one graph step, retry and the complete
canonical event bytes. The existing 64-attempt hard ceiling and 64 MiB aggregate
binding replay ceiling remain bounded. Later barriers and nested entries retain
the binding usage floor. Schema 30 pins the additional immutable table, reserved
event and suspended-caller guards, catalog and exact 49-table runtime ACL.
Source-29 upgrade fixtures retain a real noninitial terminal barrier and its
consumptions; they do not qualify historical binaries.

Schema 31 adds experimental whole returns. A terminal barrier/checkpoint first
commits as an immutable child proof, leaving the child active and the caller
suspended. The dedicated return transaction authenticates that proof and the
current physical framework caller, then atomically commits settlement, exact
declared-route parent result, caller completion, stack pop and one reserved
journal fact. The parent checkpoint stays immutable until its own barrier
consumes the returned result. A newer fence requires caller rebinding first;
ordinary completion still cannot finish framework calls.

Return recovery validates closed canonical bytes, all SQL projections, the
admitted graph closure, actual entry/terminal/caller histories, result owner,
completion, event predecessor and original shared budget. It precedes fresh
callbacks, observations and lease checks. Fresh planning runs the actual pinned
schema/reducer before locking; commit repeats active leaf, exact parent/child
heads, current physical start, settled child accounting, live database fence
and original deadline. Return charges one graph step and its canonical event;
its framework completion has zero application usage. Later mutations retain the
Run-wide DIRECT floor.

Replay reuses only compact proofs owned by its database transaction, at most
4,096 scope/return entries, with no process-global or full-state cache. Earlier
returns are authenticated in journal order before result ownership traversal.
Seven-level cascade tests run with the normal thread stack and the real lease
renewal API. Exact Schema 31 catalog and 50-table trusted-server ACLs retain
immutable facts and require whole proofs for stack transitions and framework
completion. Nonempty source-30 fixtures preserve terminal and rebound-caller
facts and migration checksums; they do not qualify historical binaries.

Schema 32 adds whole leaf suspension through `commit_graph_frame_wait`.
A version-2 Store barrier binds the original Run lifecycle revision and every
complete node wait condition; version-1 non-wait bytes and Core pins remain
unchanged. One transaction consumes results, saves the scoped successor and
whole witness, advances the leaf, registers all waits and projects Run waiting
while releasing its lease. It verifies the original locked lease/deadline after
all deferred guards, even though the resulting waiting projection has no lease.
Existing resolver/timer APIs authenticate the whole scoped witness and complete
terminal records. The last condition makes the saved leaf schedulable under a
new fence; historical retry/reads retain their exact scope after final return.
SQL guards reject incomplete suspensions and active projection with outstanding
conditions. Corruption can still be quarantined with its immutable audit without
parsing the damaged suspension. The 50-table ACL inventory is unchanged.
Development tests cover 24-way once-only registration, restart, authorization,
two-condition discharge, caller takeover/whole return, seven component rollbacks,
policy substitution, complete terminal substitution rejected before actual node
start, idempotent fail-stop isolation of malformed suspension bytes and slow
deferred lease expiry. A populated source-31 reconstruction preserves actual
entry/result and version-1 terminal facts plus all older migration checksums,
then commits the new whole wait; downgrade refuses retained version-2 facts.
It does not qualify retained historical executables. A separate runtime LOGIN
exercises the actual wait/resolve/retry/resume path. Final immutable-source
qualification, two-version backup/restore and all-frame closure remain required.

Admission, node, barrier, caller-binding, return and wait transactions are parts of
the Draft. All-frame closure, actual registry/driver dispatch,
complete process-loss/commit-loss fault qualification and production capacity
remain pending. These Store primitives do not enable
nested execution or make RFC-0022 Supported.

## Detailed semantics

### Logical identity and scope

The child namespace segment is the full 64-character lowercase SHA-256 encoding
of `stateknot-graph-frame-namespace-v1\0 || JCS({ origin, slot })`, where `origin`
is the complete caller `NodeActivation`. Append it to the caller namespace with
one `/`, omitting the separator for the root. No process ID, wall clock, physical
attempt or new random identifier enters that derivation. The same committed
caller facts derive the same scope after takeover. A later parent checkpoint
or a different slot derives a different scope.

Namespace is data, never a path. Keep the existing 512-byte scalar ceiling and
segment validation. Seven full-digest levels occupy 454 bytes; the eighth would
require 519 bytes and is rejected before dispatch. Persist a Run-wide frame-start
counter with an explicit admitted limit of at most 4,096. Intersect any more
restrictive declaration limits; descendants cannot widen them. Physical retries
of one existing frame do not allocate another frame or reset the counter.

The immutable frame identity additionally binds the target graph reference.
Changing that pin at the same namespace produces an identity conflict, not a
new frame. Namespace alone does not authorize entry. The store verifies the
call declaration, exact caller checkpoint/ready node, registry closure, policy,
resolved bounds and current fence before committing admission.

### Frame stack and state

Only the current leaf may execute application nodes. Every suspended ancestor
retains an immutable caller checkpoint and continuation. Nested-call graphs
have parallelism one; ordinary parallelism inside a leaf remains limited by its
graph, Run policy and existing driver limits. The active stack is root plus at
most seven nested frames. Recovery loads compact ancestor heads and only the
state needed for the active barrier/return within explicit replay limits.

A frame copies the validated caller state once at admission. Child barriers
change only child state. A verified child terminal output creates a parent
pending result with the declared update schema and route. The parent reducer
then applies that update at its own ordinary barrier. It never observes partial
child updates through shared mutation.

Local supersteps are scoped by namespace. All frames still share one ordered
Run journal, one lease/fence, one accounting boundary and one lifecycle. Frame
entry/return and node work consume a finite driver quantum; nested dispatch
cannot bypass the caller's superstep/deadline/resource gates. Frame starts have
a separate finite counter; ordinary node/model/Tool usage remains Run-wide and
unknown costs retain their existing fail-closed treatment.

### Admission, return and retries

Frame admission atomically commits the caller framework-attempt proof, immutable
frame identity, initial child checkpoint, active-leaf projection, counter update
and the exact journal fact. An acknowledgment loss reloads that full fact. A
repeated namespace with another identity, initial state, caller, target or intent
is a conflict. A takeover reuses the existing frame instead of re-admitting it.

A parent physical attempt from an expired fence cannot complete under a new
fence. Recovery commits a new framework-only parent start when required by the
existing attempt history, without re-executing application code or allocating
a child. Its completion must follow that start in journal order.

A child terminal barrier/checkpoint commits as an immutable proof while the
caller remains suspended. Return atomically settles that exact proof with the
parent framework-attempt completion and immutable parent pending result, and
moves the active-leaf projection to the caller. A crash between these stages
leaves a recoverable terminal child and grants no parent dispatch; recovery
authenticates the saved terminal proof and commits or reloads the whole return. It verifies every output/schema,
caller activation, declared route, parent base checkpoint, current frame head,
physical start and fence. Required invocation/child-Join ownership must already
be discharged. An acknowledgment loss returns the exact committed outcome;
reconciliation cannot invent another output or replay a completed child.

### Waiting, cancellation and terminal ownership

A leaf wait commits its exact scoped checkpoint, frame wait state, bounded
interrupt/timer bindings, Run wait projection and journal fact atomically.
Resolution/firing requires existing resolver authorization and an exact persisted
frame binding. Restart resumes the saved leaf and continuation; it cannot derive
root activations for a child checkpoint. No sleeping task owns the suspended Run.

Run cancellation and deadline failure apply to the entire stack and its owned
invocations/child Runs. Failure/unknown-effect close keeps committed evidence,
respects cancellation races and settles every frame before terminal projection.
Run success requires root scope and no open frame or undischarged ownership.
No legacy control-plane or Worker mutation may bypass these predicates.

## Persistence and migration

Use scoped checkpoint storage with an exact `(tenant, Run, namespace, local
superstep)` uniqueness key. Same-namespace parent foreign keys must include the
namespace. Activation-bearing node/model/Tool/result/Join rows must bind it to
their exact base checkpoint. The legacy root pointer uses an explicit empty
namespace foreign key and cannot point to a child row. Root and child queries
must be distinct scoped operations; tenant/Run filtering alone is insufficient.

Persist immutable frame identity/entry and settlement records, scoped checkpoint
bindings, current frame heads, the active-leaf projection, finite start count
and wait ownership. A child checkpoint binding hashes the complete frame
identity and checkpoint head with `stateknot-graph-frame-checkpoint-v1\0`.
Restore checks canonical bytes, redundant columns, every identity/digest,
same-scope parent chain and exact journal anchor. A database namespace column
without its authenticated frame binding is insufficient.

Admission and return contain multiple durable facts. Their journal projection
digest binds one closed compound intent, not whichever component was inserted
last. Existing checkpoint, node-attempt and pending-result anchor verifiers must
validate the complete recognized compound binding, exact event payload/schema,
actor fence and every referenced component. Do not admit a second arbitrary
projection digest or skip verification to reuse a legacy append API. Historical
root events continue to require their original exact legacy binding.

The migration must add scoped keys/foreign keys and Run frame projections
without rewriting valid old canonical records. Old root fixtures retain their
exact bytes and digests where their existing contract remains supported.
Nested declarations change the compiled definition and generated schema:
review and update affected input/output pins explicitly. A preserved old graph
pin with new call behavior is an integrity failure.

Rollout first validates the new schema and complete immutable registry closure,
then admits nested Runs. Mixed executables must not execute or rewrite an
unsupported nested descriptor or frame. Downgrade is refused while a nested Run
or retained nested history requires the new reader; migration checks must prove
that refusal on a nonempty database. Backup/restore includes frame projections,
all bindings and the shared journal in the same consistent recovery point.
Retention keeps reachable ancestors and ownership evidence until the ordinary
Run retention policy permits removal. No independent frame deletion or repair
of a damaged chain is authorized.

These requirements do not manufacture historical releases. True N-1/N-2
migration and rollback qualification remain tied to actual supported artifacts
and the R6/R7 gates. The constructor prototype has no SQL migration and provides
no database or compatibility qualification by itself.

## Security and privacy

The caller implementation is trusted and its nested closure is frozen by exact
owner/name/version, definition and schema pins before dispatch. It cannot widen
Run authority by selecting another owner, replacing a descriptor or changing
child state/output schemas. Frame identity is data, not an authorization receipt.
The same exact-tenant, grant, provider/Tool, audience and recovery authorization
checks continue at every external operation.

Every mutation checks the current unexpired Run fence and exact active leaf.
Only a framework-owned declared call/return may touch its suspended parent
attempt; application executors cannot submit that privileged transition.
Checkpoint, result, invocation and wait bindings reject crossed tenant, Run,
namespace, caller and target. Registry or chain drift is quarantined before
application dispatch; it is not converted into a retry.

Keep namespace/input/descriptor text bounded and closed. Raw duplicate and
unknown-field rejection, map-only object readers, schema validation and
canonical integrity checks apply to new records. Do not serialize live context,
credential, execution or cancellation handles. Journals retain the existing
secret-free payload policy; diagnostics report identities/digests and public
failure categories rather than child state, input or credentials. A child frame
inherits existing external-effect uncertainty rules and cannot blind-retry an
unknown write.

Run success, failure and cancellation predicates are checked inside the same
transaction as their frame/ownership changes. A same-Run frame must not create a
path around required child-Run cancellation/Join, unresolved model/Tool outcomes
or direct-usage settlement. Trusted-server SQL credentials remain a separate
existing deployment assumption; this RFC does not establish Worker-only SQL
isolation or substitute for independent R7 security review.

## Observability and operations

Emit bounded admission, leaf-wait/resume, return, abort, pin-conflict and
quarantine activity anchored to the authoritative journal. Protected inspection
may report the active scope, compact stack heads, frame count/limit, wait kinds
and blocking ownership. It must use existing tenant/resource authorization and
finite response limits. Namespace/Run/node identities are not metric labels;
use finite frame-depth and outcome classes.

Graceful drain persists any committed result and releases ownership without
resident suspended tasks. Process replacement reloads the exact leaf, registry
closure and owned invocation/Join evidence before dispatch. Alert on repeated
scope conflicts, unsupported pins, exhausted bounds, unresolved effects and
failed settlement; operators may use existing separately authorized recovery
procedures, never manual namespace edits or fabricated returns.

RPO/RTO remain properties of the configured journal/database deployment. This
RFC adds no measured capacity, synchronous-standby RPO or failover claim.
Qualification must measure replay memory with bounded ancestor heads and an
active leaf, and demonstrate process loss/restore of nonempty frame stacks.
The real reference deployment and independent security/pilot gates remain open.

## Compatibility

The implementation must preserve the current Rust/MSRV and runtime-neutral Core
boundary unless an explicit reviewed change is necessary. Prefer existing
bounded types, SHA-256/JCS, transactional store and owned driver primitives.
No new workspace crate, model SDK, generic workflow interpreter or in-memory
production store is proposed.

New declaration/frame types and modified generated graph schemas require the
closed public inventory, complete enum alternatives, canonical fixtures and
both serialization profiles to be reviewed. Existing valid root wires remain
regression controls. New vectors are current-source evidence and receive their
own versioned catalog entries; they do not replace history. No crate, tag or
stable public API is published by merging a Draft or constructor prototype.

## Alternatives considered

- Remove root checks: namespace/checkpoint ownership and wait continuation would
  remain unauthenticated, so execution could alias another logical node.
- Flatten templates: useful for existing shared-state composition, but it changes
  partial-update visibility and has no independent child checkpoint identity.
- Use an isolated child Run: useful under RFC-0004, but it changes identity,
  policy/accounting and Join semantics rather than implementing this contract.
- Keep a child future in the parent executor: loses durable continuation and
  retains process state for waits; takeover cannot prove completed work.
- Put nested declarations only in the runtime registry: preserves the old graph
  digest while changing execution, violating the immutable closure contract.
- Recursive digest-pinned call cycles: require a separate template/recursion
  contract rather than mutually dependent definition hashes. Reject them;
  bounded loops and repeated calls to the same pinned child remain available.
- Parallel frame stacks in this first profile: require a different Run readiness
  and wait aggregation contract. Reject such callers explicitly; bounded
  parallel work inside a leaf remains available.

## Validation and rollout

Before acceptance, collect source design/security/operations/compatibility review
and an executable compiler/runtime/store prototype with these mandatory gates:

1. Closed constructors/readers and independent models for scope, pins, exact
   checkpoint/intent/record bytes, parent continuation and finite bounds. Reject
   crossed tenants/Runs/owners, changed pins, raw duplicates/unknown fields,
   positional objects, excessive state/depth/count and forged waits/returns.
2. Compile/freeze a complete nested graph closure. Reject unregistered or changed
   targets, cycles, conflicting owner/name/version pins, excessive closure depth/
   snapshot bytes, wrong schemas, parallel callers, call-site executors, invalid routes
   and any widening of resolved policy/limits before application calls.
3. PostgreSQL 16/17: identical node IDs and local supersteps in multiple namespaces
   never alias. Exact-scope foreign keys and root-pointer guards reject every
   crossed row. Test complete idempotent admission/return and conflicting intents.
4. Lost acknowledgment and process kill before/after each frame/attempt/journal/
   checkpoint/projection/wait/settlement write. Recover under a newer fence with
   zero accepted stale writes and zero repeated committed model/Tool/node effects.
5. Wait in a nested leaf, replace the process, authorize resolution, restore the
   exact saved leaf and return once to the original parent. Cover nested loops,
   terminal output, cancellation/deadline races and unknown-effect failure close.
6. Actual child-Run admission/Join/cancellation inside a frame: owning activation
   remains exact, parent/frame return cannot escape live ownership, committed
   Join publication is reused and every direct/unknown usage path stays charged
   to the original Run accounting boundary.
7. Nonempty migration, downgrade refusal, backup/restore, corruption and registry
   drift quarantine. Prove old root canonical bytes remain exact and explicitly
   qualify changed schema pins; retain real historical qualification separately.
8. All required local workspace checks, immutable bounded ASan and 14 final-head
   CI jobs, exact merge tree, bilingual reproducible adoption and release evidence.
   Reduced fault/memory runs do not replace R6 reference-load measurements.

The five current private constructor checks are the first evidence increment.
They do not close G3, G4, child ownership, SQL recovery, publication or the overall
R1–R7 milestones. Implementation and qualified support remain separate from RFC
acceptance.

## Unresolved questions

- Fix the complete public declaration/driver surface using the executable
  compiler/runtime/store prototype; do not expose speculative placeholder APIs.
- Validate the single-leaf Run lifecycle, parent physical-attempt replacement and
  compound journal binding against every legacy mutation and child-Join path.
- Produce and review the concrete nonempty migration and rollback constraints.

These questions materially affect the supported contract. This RFC cannot be
accepted until they are resolved with actual code and evidence.
