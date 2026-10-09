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

This document and its private constructor prototype do not add runtime support.
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
Public declaration and driver signatures must be fixed by the executable
compiler/runtime prototype before acceptance; no placeholder method is exported.

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

Return atomically commits the child terminal barrier/checkpoint, frame settlement,
parent framework-attempt completion and immutable parent pending result, and
moves the active-leaf projection to the caller. It verifies every output/schema,
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
