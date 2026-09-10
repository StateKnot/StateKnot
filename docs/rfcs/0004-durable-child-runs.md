<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0004: Isolated durable child runs

- Status: Draft — contracts, PostgreSQL ownership/accounting/cancellation/Join, opt-in Graph Driver suspend/resume, bounded publication, database-clock deadlines and settled-direct failure close implemented; full-profile qualification remains gated
- Authors: StateKnot contributors
- Created: 2026-09-07
- Tracking issue: [#24](https://github.com/StateKnot/StateKnot/issues/24)
- Supersedes: None
- Superseded by: None

## Summary

Introduce structured child execution with separate Run IDs, checkpoints,
leases, state schemas, and terminal evidence. A parent activation owns a
bounded set of version-pinned child slots. Child admission and its ownership
record commit atomically. A parent cannot close while an owned child remains
nonterminal. This is not the static shared-state expansion implemented by
`GraphComposition`.

The initial policy is **cancel and join**, not detach or force termination.
Cancellation requests stop admission and propagate durably; completion waits
for verified terminal evidence. A stuck external operation therefore remains
visible as unresolved work rather than being relabeled cancelled.

## Motivation and implementation evidence

`PostgresStore::admit_agent_run` currently commits admission and an initial
checkpoint in one transaction, but does not bind a parent activation.
`DurableAgentRuns` validates single-run terminal provenance and accounting.
The graph driver currently prepares root-namespace activations only.
Reusing these public admission calls inside a parent executor would create a
crash window between child creation and recording ownership. Reusing parent
leases would also conflate two independent execution histories.

## Goals and non-goals

Provide atomic admission, isolated state, durable joins, bounded delegation,
exact accounting, recovery after process loss, and cancellation propagation.
Children use ordinary root-namespace execution within their own Run IDs;
ancestry is a separate durable relationship, not a synthetic filesystem path.
This distinguishes cross-run ownership from RFC-0002's prospective same-run
nested checkpoint namespace. The latter remains unimplemented.

Do not add detached children, dynamic unbounded recursion, cross-tenant
delegation, remote A2A ownership, shared mutable state, or rollback of external
effects. Do not claim exactly-once external execution.

## User-facing design

The [core contract example](../durable-child-runs.md) compiles and demonstrates
logical identity and cumulative capacity arithmetic only. It does not implement
the following required runtime operations:

1. Declare finite child slots and pinned child graph/Agent references in the
   immutable parent executable definition, including input/output schemas.
2. Submit an isolated child for an exact committed parent activation and slot.
   Return either its durable identity or an explicit conflict; never silently
   select another child on retry.
3. Suspend the parent with a durable child-join predicate. Release its worker
   lease instead of polling while holding an execution slot.
4. Resume through normal scheduling when all required terminal bindings exist.
   Success exposes schema-validated output; failure and cancellation expose
   typed public-safe evidence. User code decides how to reduce these results.

No implicit JSON merge, child failure suppression, retry with fresh child IDs,
or conversion of a failed child into successful parent state is permitted.

## Detailed semantics

### Implemented preparation increment

`ChildRunAdmissionIntent` and the runtime `prepare_child` /
`validate_child_preparation` methods freeze complete retry material and validate
authoritative parent snapshots against an offline executable registry. They
perform no storage mutation. See [the contract guide](../durable-child-runs.md)
for the closed encoding, snapshot bound, same-principal profile, narrowing,
real-store tests, and remaining commit-time checks. This increment does not
resolve reservations, joins, or parent-close guards.

`GraphChildRunPolicy` version 1 now embeds node-owned Agent/graph/schema pins
in the parent graph digest, with canonical node/slot ordering, 256 aggregate
declarations and 64 slots per node. `ChildRunTopologyLimits` bounds remaining
descendant depth to 32, lifetime direct children to 256, and active descendants
to 256. Exact target closure and strictly decreasing declared depth are checked
at registry startup; both runtime preparation paths validate declaration
membership. These declarations do not provide transactional live-count or
caller authorization checks. Legacy graph encodings stay unchanged.

### Identity and admission

The ownership uniqueness key is `(tenant_id, parent_run_id,
parent_activation_digest, child_slot)`. A physical node attempt, worker ID,
or lease fence must not enter this key: a takeover must discover the same
child. The activation already binds its base checkpoint and logical input.
Slots are finite identifiers declared by the executable, not model text.

An immutable spawn digest binds the ownership key, child executable closure,
input payload digest, initial state digest, effective authority, allocated
budget, deadline, and close policy. Candidate generated IDs are excluded from
retry comparison; the first committed admission owns the persisted IDs.
Same key plus different intent is a conflict even after the child terminates.

Admission locks the parent lifecycle and validates its committed activation,
exact current fence, runnable status, declared slot, authority, and remaining
budget. In one database transaction it reserves capacity, inserts child
admission and its initial checkpoint, records ownership, and appends durable
spawn evidence. Nothing becomes schedulable before that commit.

An exact previously committed retry is a read of historical evidence, not a
new mutation, and can succeed after fence expiry or cancellation. It must
still verify the stored identity, digest, tenant, and read authority. Fresh
admission always requires current execution authority. Ambiguous commit
acknowledgements are resolved by the ownership key, not resubmission with a
new slot.

### Joining and wakeup

Use a dedicated child-join predicate and binding, not fabricated timer events
or an arbitrary user signal. A binding includes child admission digest,
terminal event identity/digest, terminal outcome digest, and verified usage.
It is immutable and unique per owned child.

Migration 22 seals the complete nonempty set for one activation and commits
registration with parent lease release under tree → parent → child locks.
Registration verifies existing ownership/terminal evidence; publication is a
separate transaction even if children already settled. Indexed discovery uses
the durable registration and all-settled predicate, so completion before or
after registration cannot lose a notification. Publication commits immutable
terminal bindings with scheduler wakeup. Pending results must carry the exact
published head and consume it atomically with physical completion. Missing or
changed evidence blocks checkpoint/success; ordinary no-Join result bytes stay
unchanged. A host schedules `DurableChildJoinPublisher` alongside cancellation,
settlement and independently leased execution workers. Opted-in exclusive nodes
return dedicated Join control; the driver releases ownership, verifies published
terminal/output evidence before recovery dispatch, and binds successful parent
completion to unique consumption. See the bilingual integration guide.

For multiple children, reduction order is declared slot order, never wall
clock completion order. A terminal child is not a completed parent node:
the parent still validates and commits its own state transition/barrier.
Retries of that transition reuse bindings without charging usage again.

### Closure, cancellation, and deadlines

Every parent terminal path, including generic store paths, must enforce the
same no-live-owned-children invariant. Checking only the Agent facade is
insufficient. Success is blocked until joined; failure/cancellation seals
new spawning and records cancel-and-join intent for outstanding children.

Cancellation delivery is a durable, bounded work queue, not an unbounded
recursive transaction. Descendants receive cancellation through the same
mechanism. Leases fence worker writes, while the durable close intent survives
worker loss. Concurrent spawn and close serialize on the parent lifecycle:
spawn either commits first and becomes owned cancellation work, or is refused.

Implemented in migration 21: every cancellation writer captures immediate-child
queue witnesses; delivery commits the request, real-wait abandonment, descendant
queue work and immutable receipt together. The bounded runtime reconciler drives
delivery and terminal settlement without dispatching external work. New leases
for cancelling parents are blocked until all children settle; existing cleanup
leases remain renewable. This drain gate does not implement dedicated successful
Join registration/wakeup. Migration 24 now seals original failure intent for
Active parents with settled, fully priced direct evidence; separately scheduled
`DurableRunFailureCloser` drains children and commits exact final accounting.
Unknown parent-side effects still require fenced recovery before registration;
see [failure close](../failure-close.md). Migration 23 and `DurableAgentDeadlineReconciler`
now provide indexed deadline discovery and post-lock database-clock cancellation,
with atomic wait abandonment/child queue capture, unchanged first reasons,
bounded tenant sweeps and explicit per-item failures. This is not implicit
behavior of the cancellation/settlement worker; see [host integration](../agent-deadlines.md).

Effective child deadlines cannot exceed the parent deadline. Deadline expiry
initiates cancellation but is not evidence that external side effects stopped.
Operators must reconcile uncertain invocations using existing recovery rules;
there is no administrative shortcut that invents terminal usage.

### Budgets and authority

The first core increment supplies `CumulativeBudgetReservation` for checked
cumulative capacity arithmetic. Graph depth, concurrent branches, and fan-out
are high-water observations and cannot be charged as cumulative expenditure.
Their topology/admission contracts remain required before child execution can
be enabled. See the [implemented boundaries](../durable-child-runs.md).

Child scope grants must be a subset of the parent's delegable authority and
pass the normal child admission policy. Same tenant alone grants no authority.
Private credentials and policy evidence are not copied into public output.

Before spawn, atomically reserve finite child allocations against remaining
parent limits, including parent direct usage and all outstanding reservations.
Independent admission budget limits alone are insufficient: concurrent
children could otherwise each spend the same remaining parent budget.

Settlement replaces the reservation with exact terminal child usage once.
Define parent subtree usage as direct usage plus immediate children's subtree
usage; never add descendants twice. Preserve direct and delegated totals
separately in audit evidence. Unknown external usage retains its reservation
and blocks exact settlement. No zero-usage fallback is permitted.

The core `ChildRunBudgetAccount` now implements these pure transitions, with
head-bound monotonic direct observations, retained first-child identities,
single-settlement conflict checks, bounded versioned restoration and account
digests for future atomic replacement. It preserves known overruns rather than
clipping true usage; subsequent capacity decisions fail closed. Child-local
topology peaks are excluded from delegated additive charges. This does not
implement a durable ledger, serialize direct work, prove terminal evidence,
or complete lifecycle joins. See the bilingual contract guide for the exact
encoding and trust boundary.

Finite depth, children-per-activation, children-per-run, and active descendant
limits are mandatory and admission-checked. Values and wire bounds remain
acceptance decisions below; no unbounded default is allowed.

## Persistence and migration

Proposed additive records cover ownership/spawn intent, budget reservation and
settlement, immutable terminal binding, join registration, and cancellation
delivery. All references and uniqueness constraints include tenant identity.
Child ownership is unique; a run cannot acquire a second parent. Children are
created fresh in the ownership transaction, so attaching an existing ancestor
or arbitrary ingress run is forbidden.

Database constraints and transaction checks must protect all terminal write
paths, not only the new API. Establish and test a lock order before implementing
multi-run writes. Prefer durable delivery between child completion and parent
settlement to taking ancestor locks inside a child's terminal transaction.

Published migrations through 22 remain unchanged. Migration 20 implements
ownership/admission/accounting and capability version 1, including populated
v19 upgrade checks. Migration 21 adds cancellation evidence, indexed bounded
discovery, immutable receipts, catalog/function verification, drain claim guards
and populated v20 cancellation backfill. Old workers cannot bypass closure or
new-lease drain guards; process configuration alone is not the compatibility
boundary. Migration 22 adds dedicated Join registration, publication and unique
pending-result consumption, a separate mixed-binary capability guard, exact
catalog/function verification and populated v21 upgrade without synthetic joins.
Failed/cancelled parents retain unconsumed successful-Join history while central
child settlement guards still protect closure; automatic close intent remains
a separate gate. Migration 23 adds an immutable deadline projection, an admission
capture trigger and due-work index, populated v22 backfill without new lifecycle
facts, exact startup catalog/function verification and atomic deadline cancellation.

Rollback after child data exists means disabling new spawn and draining with
compatible workers, not dropping ownership tables. Retention cannot remove
child evidence until all ancestor joins/accounting obligations and replay
retention requirements are satisfied. Archive relationships and evidence
together. A tombstone must retain identity/idempotency material if payloads
are removed under a supported retention policy.

## Security and privacy

Test cross-tenant references, unauthorized lookup, forged parent activations,
stale fences, undeclared slots, altered child inputs, and scope widening.
Read APIs must distinguish authorization failure without leaking another
tenant's child existence. Inputs and outputs remain subject to existing
artifact/schema/privacy boundaries; logs expose bounded IDs and reason codes,
not child prompts or secrets.

## Observability and operations

Expose spawn conflicts, oldest unsettled child, cancellation delivery age,
join wakeup lag, reserved versus settled budget, ancestry depth, and blocked
parent closure. Use bounded-cardinality metric labels; put Run IDs in traces.
Recovery scans must be paginated/indexed and report corrupt bindings rather
than silently skipping them. Committed ownership must survive process loss;
database disaster-recovery guarantees remain those of the configured store.
RTO and throughput targets require measured qualification, not a promise in
this draft.

## Compatibility

Keep existing root-run and static composition encodings unchanged. New child
declarations, bindings, lifecycle evidence, and accounting require explicit
versioned schemas. No changes to Rust 1.88 MSRV are proposed. No new distributed
coordinator or protocol dependency is necessary for same-store children.

## Alternatives considered

- Static shared-state composition is already supported but provides neither
  private state nor independent scheduling/lifecycle.
- Calling public admission from inside a node leaves an ownership crash gap.
- Keeping a parent lease while recursively driving children wastes capacity
  and couples recovery to a process stack.
- Detached children avoid closure coordination but violate structured ownership
  and make finite subtree budgets unenforceable without a different contract.

## Validation and rollout

Implement and qualify in dependent increments; none alone means child runs
are supported:

1. Core identity/declarations/accounting contracts with strict deserialization,
   canonical digest fixtures, bounds, and tamper tests.
2. Atomic PostgreSQL ownership/admission/reservation, uniqueness races,
   lost-ACK replay, stale-fence rejection, migration/catalog checks.
3. Durable terminal bindings, join registration/wakeup races, cancellation
   queue, every parent close path, exact once-only settlement.
4. Runtime spawn/suspend/resume and registry recreation with separate child
   leases and schemas; offline example plus real-store executable example.
5. PostgreSQL 16/17 qualification, full existing CI, bilingual tutorials and
   site status, then explicit capability enablement and deployment.

Fault tests must stop execution before/after admission commit, after child
terminal commit before notification, before/after wait registration, after
settlement before acknowledgement, and during higher-fence takeover. Assert
one ownership/admission, no lost wakeup, no duplicate settlement, no dispatch
after closure, and no terminal parent with a live owned child. Test concurrent
spawn versus cancel, sibling allocation races, nested cancellation, unavailable
evidence, corrupted digests, exhausted limits, and migration from populated
root-only data. Preserve all static-composition and root-run regression tests.

## Unresolved questions / acceptance blockers

- Complete-profile qualification, including failure close and arbitrary uncertain direct effects.
  Opt-in Join execution/context, bounded publication, recreated-registry parent
  recovery and unique result consumption now have real-store integration evidence.
- Concrete transactional reservation/settlement rules compatible with existing
  budget and ledger contracts, including parent direct work and unknown usage.
  Scalar/deadline/currency narrowing and cumulative arithmetic are implemented;
  the PostgreSQL adapter now enforces resource ownership and ancestor limits.
  Complete-profile runtime qualification remains a separate gate.
- Combined process-termination and provider-effect recovery qualification of
  failure-close writers. The settled-direct path has real-store rollback,
  original-failure, accounting, bounded sweep and populated-upgrade coverage
  against central terminal guards and the version-fenced lock order.
  The [committed-boundary OS-kill profile](../process-kill-qualification.md)
  additionally verifies six failure-close drain/replay checkpoints in separate
  force-killed processes. It does not cover pre-/in-commit loss, provider effects,
  or the combined Join/deadline/takeover process-termination matrix.
- Measured recovery/capacity thresholds. Cancellation now has PostgreSQL 16/17
  rollback, duplicate delivery, spawn/cancel race, nested propagation/accounting,
  unpriced-prefix pagination and populated-upgrade tests; these do not measure
  production capacity or qualify full child execution.

This RFC is deliberately Draft until these decisions and executable evidence
exist. It does not authorize advertising or enabling durable child runs.
