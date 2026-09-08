<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable child runs: transactional storage and remaining runtime work

Status: core contracts, graph-pinned declarations, read-only preparation, and
PostgreSQL atomic ownership/admission, reservation and terminal settlement are
implemented. Automatic child Join, cancel propagation and parent resumption
are **not yet implemented**; this is not an end-to-end child execution tutorial. [RFC-0004](rfcs/0004-durable-child-runs.md) remains Draft. See
the [Chinese edition](durable-child-runs.zh-CN.md) and the already implemented
[static shared-state composition](graph-composition.md) for the distinction.

## PostgreSQL transactional storage (migration 20)

This is a trusted-store building block, not an automatic child coordinator.
Authenticate the caller and authorize delegation to the declared slot before
calling it. The database pool is trusted; its custom transaction settings are
a mixed-binary compatibility fence, **not** a security boundary for untrusted
SQL users.

`PostgresStore::admit_child_run` takes the prepared intent, exact live physical
parent node-start head, worker parent append (`child-run-admitted`), independent
child admission append/checkpoint, and a complete **direct-only** usage
observation at the parent's exact journal head. Schema callbacks run before
the write transaction. The store then:

1. Serializes the tree and locks ancestor Run rows root-to-parent.
2. Rechecks current checkpoint/ready activation, unfinished physical start,
   immutable admission/declaration, live fence, all ancestor Active statuses,
   depth/active-descendant ceilings, and cumulative capacity.
3. Atomically commits a fresh child admission and initial checkpoint, immutable
   ownership/audit evidence, the parent's account reservation and journal head.

A pre-existing independent Run is never adopted. Same key plus equal spawn
digest recovers the first committed IDs before fresh lease/deadline checks;
different business intent conflicts. Lifetime children are retained (maximum
256); terminal-but-unsettled descendants occupy active topology capacity.
Depth is bounded at 32. These are safety bounds, not throughput guarantees.

The first profile serializes parent external work with children: an unfinished
parent model/tool invocation prevents spawning, and outstanding children block
new parent model/tool revisions. After settlement, direct work may resume.
`InvocationBudgetProvider` reports direct-only remaining capacity; the executor
deducts settled subtree charges and passes the exact account digest to the
start transaction. Legacy store start methods cannot bypass that requirement.
Lost-ACK recovery does not dispatch again or charge a second time.

Every terminal writer passes the central child-inclusive accounting check.
`GraphLifecycleEvidenceProvider` supplies **direct-only** evidence on fresh
success/failure/cancellation; the lifecycle coordinator adds verified child
charges exactly once. Low-level writers must provide complete subtree totals,
or use `include_child_usage` on complete direct evidence before constructing
the terminal transition. The journal CAS and terminal transaction revalidate
this observation. Child-local topology peaks are never summed into ancestors.

Owned child terminal commits capture the exact immutable lifecycle and terminal
journal anchor in the child's own transaction, with a durable notification and
**no ancestor row lock**. Later audit appends cannot replace that anchor.
`settle_child_run` locks tree → parent → child and atomically replaces one
reservation with the complete subtree usage, records the parent settlement
event (`child-run-settled`), and consumes the notification. Repeated settlement
recovers the original event. Known overruns remain recorded; unknown pricing
remains unsettled and cannot be silently treated as zero.

Use `pending_child_settlements` for the first page (at most 16 bounded keys) and
`pending_child_settlements_after` with the last returned key for subsequent
pages. The cursor remains usable after that child settles. Continue past items
requiring price reconciliation, then restart from the first page after a full
sweep; commit order can differ from event-time order. Do not use a timestamp as
a permanent delivery watermark. `load_child_run` and
`load_child_budget_account` verify canonical bytes, ownership, graph/parent
pins, physical node/checkpoint/journal anchors and settlement bindings.

Unsettled owned children block **all** parent terminal transitions and checkpoint
advancement. This does not synthesize a Join, timer, or user interrupt. Cancelling
an ancestor blocks fresh descendant admission, but automatic cancellation
delivery and parent suspend/resume are not installed yet. Do not expose this
storage API as an end-to-end child execution service without those coordinators.

Upgrade with the existing explicit `PostgresStore::migrate_database` workflow
before starting compatible workers. Migration 20 backfills capability version
1 on existing child-declaring admissions and fences new child-enabled admissions.
Older workers cannot mutate these Runs; ordinary root Runs remain compatible.
Startup verifies migration checksums, required constraints/indexes/triggers and
the installed guard function bodies. Once child evidence exists, rollback means
stopping new spawns and draining/reconciling with compatible workers, not dropping
the migration, deleting ownership, disabling guards or clearing reservations.

## Run the offline contract example

```console
cargo run -p stateknot-core --example child_run_contracts --locked
```

The example validates a synthetic checkpoint fixture, derives an ownership
key, verifies its serialization round trip, reserves two candidate cumulative
amounts in a pure calculation, and refuses a third allocation exceeding the
parent input-token ceiling. It does not connect to PostgreSQL or run an Agent.

## Logical ownership identity

`ChildRunSlot` is a distinct type using the same case-sensitive, at-most-128-byte
grammar as `NodeId`. Constructing it does not prove membership in an executable
declaration. `ChildRunKey::new(parent_activation, slot)` binds the full parent
checkpoint/graph, namespace, node, input, tenant, and Run identity. Retrying
with a different physical attempt or fence preserves the logical key.

The key wire form contains `parent`, `slot`, and `digest`, rejects unknown
fields, and recomputes its digest when deserializing. The SHA-256 preimage is
the bytes `stateknot-child-run-key-v1\0` followed by RFC 8785 canonical JSON
with `tenant_id`, `parent_run_id`, `parent_activation_digest`, and `child_slot`.
The activation digest is exactly the existing node-attempt activation digest;
the old activation encoding is unchanged. Scalar counters remain decimal
strings, not floating-point JSON numbers.

A checksum is not an authorization proof or signature. The store validates
committed readiness and the current worker fence; caller delegation authorization
and complete direct usage remain trusted server responsibilities. The key
also does not bind a child spawn intent: pinned child executable, input,
authority, budget, and initial-state comparison are handled separately by
`ChildRunAdmissionIntent`.

## Declared delegation policy

```console
cargo run -p stateknot-core --example child_run_declarations --locked
```

`CompiledGraph::with_child_runs(GraphChildRunPolicy)` seals a new graph
reference. Declare the policy **before** binding executors, registering the
graph, or admitting a parent. Changing an already registered definition requires
a new graph identity/version, not overwriting its existing pin. A graph without a policy delegates nothing; its
previous canonical bytes/digest are unchanged. Policies carry explicit wire
version `1`; unsupported versions and unknown fields are rejected.

Each `ChildRunDeclaration` belongs to one exact parent node and case-sensitive
slot, and pins the complete child Agent descriptor fingerprint, child graph,
and input/output schemas. `ChildAgentReference` hashes the complete strict
canonical descriptor with domain `stateknot.child-agent-definition.v1\0`.
Changing instructions, model, tools, or limits changes the pin even when the
Agent owner/name/version is unchanged. The parent stores the fingerprint,
not private child instructions. A pin is not a signature or a policy grant.

There are at most 256 declarations per graph and 64 per node. Canonical
`(node_id, slot)` order is the future join reduction order; input ordering does
not change the graph. Missing parent nodes, duplicate slots, empty policies,
and oversized collections fail closed, including bounded wire decoding.

`ChildRunTopologyLimits` requires positive values: maximum remaining descendant
depth 1–32, lifetime immediate children per Run 1–256, and simultaneous active
descendants 1–256. A leaf has remaining depth zero. Registry construction
resolves every declared target and requires its remaining depth to be strictly
smaller than the parent's; schema mismatch or missing targets prevents startup.
The store now enforces live counts and every ancestor ceiling atomically.
For capacity safety, terminal-but-unsettled descendants still count as active.
These hard bounds are safety ceilings, not measured throughput promises.

Both runtime preparation methods now call core `validate_declaration` against
the exact parent executable graph. Legacy undeclared preparation calls are
rejected; callers must register a new declared graph and admit a new parent,
not modify an existing admitted graph. Lower-level core callers must use both
`validate_declaration` and `validate_for`. Trusted authorization of the caller,
scope delegation, current fences, and remaining budget remain separate checks.

Static shared-state expansion rejects graphs/templates with child policies
rather than dropping or remapping ownership declarations. An empty composition
is a no-op and retains the entire graph. Declare children after static lowering
against its final node identities when combining these startup operations.

## Cumulative capacity arithmetic

`CumulativeBudgetReservation::from_budget` projects every cumulative ceiling
from a finite `ResolvedBudget`, including currency-specific amounts and
inclusive token/call subsets. `new` accepts an explicit validated amount.
Nonzero graph depth, concurrent branches, fan-out, and unpriced activity are
rejected; the generated JSON Schema expresses the same zero constraints.

`check_capacity(parent, accounted, outstanding, observed_at)` checks accounted
usage plus **all** outstanding allocations, including the candidate. It uses
checked addition and existing deadline/currency/budget enforcement. Up to 256
allocations are accepted per check; this bounds arithmetic work, not child
fan-out. Duplicate entries are counted twice, never silently removed.

`accounted` must contain parent direct usage plus settled child usage exactly
once and exclude outstanding allocations. Never record projected amounts as
spent usage. Parent usage with unknown price fails closed; an unconfigured
currency is refused even for a zero amount. Overflow and deadline equality
also fail closed.

The pure arithmetic is not itself a database reservation API. The PostgreSQL
transaction described below serializes child admission with other children **and parent direct work**,
bind allocations to unique ownership keys, read the complete outstanding set,
and settle once against verified terminal evidence. It must also check child
deadline narrowing, authorization, ancestry, active subtree concurrency, and
fan-out independently. Existing high-water usage is validated, but remaining
high-water counts returned by this calculation do not authorize new topology.

## Checked budget account transitions

`ChildRunBudgetAccount` is a pure, immutable state-transition contract, **not a
PostgreSQL ledger or permission to execute children**. `new(parent, graph,
direct_head, direct_usage)` binds the exact admitted parent, finite budget,
declared lifetime child bound, and an authoritative direct-usage observation.
It never assumes an existing Run has spent zero. Restore canonical bytes and
call `validate_for` against trusted immutable parent/graph records; separately
verify that every accounting observation is complete and durable.

`reserve(intent, parent_graph, observed_at)` rechecks the node-owned declaration
and includes direct usage, all settled contributions, and every outstanding
reservation in the capacity decision. Same ownership key and spawn digest
return the unchanged first-selected child identities, including after expiry;
changed intent conflicts. A distinct key cannot reuse an existing child Run ID.
The lifetime bound includes settled children; no slot deletion/refund exists.

`ChildRunBudgetSettlement::new(admission, lifecycle, terminal_head)` binds the
full child provenance, admission/intent fingerprints, exact terminal journal
observation, status, complete terminal lifecycle checksum and usage. Only
success, failure and cancellation can settle. `settle(key, evidence)` replaces
one reservation exactly once; equal retry is unchanged and substituted
terminal evidence conflicts. Failed and cancelled work still incurs its actual
charge. Unknown pricing keeps the reservation outstanding. Known overruns or
unbudgeted currencies are retained, not clipped; further capacity checks refuse
new work. Deadline expiry does not prevent recording actual settlement.

`observe_direct(head, usage)` accepts an absolute **direct-only** cumulative
observation, not a delta or a total already including children. Exact replay is
unchanged; different usage at the same head, older heads/clocks, other Run IDs,
or any regressing counter/known currency charge is refused. Over-budget or
unpriced direct observations remain visible and block new allocation.

`accounted_usage` is direct usage plus each immediate child's complete subtree
usage once. `delegated_usage` deliberately removes child-local depth/concurrency/
fan-out peaks using `BudgetUsage::cumulative_only`; it does not claim the maximum
of child-local peaks describes parent topology. Runtime topology observation
and enforcement remain separate. `remaining` additionally includes outstanding
reservations, checks a clock no earlier than retained evidence, and rejects
overflow, expiry, unknown price and exhausted ceilings. Never persist that
projection as expenditure or add grandchildren a second time.

Version-one snapshots have at most 256 lifetime entries and an 8 MiB canonical
encoding ceiling. The decoder bounds the entry array while reading; adapters
must cap raw request/storage bytes before decoding. Entries are sorted by key
digest for accounting integrity (this is **not** join reduction order). Closed
wire decoding recomputes the account digest over
`stateknot.child-run-budget-account.v1\0` plus canonical state with its digest
field set to SHA-256(empty). Terminal fingerprints use domain
`stateknot.child-run-budget-terminal.v1\0` plus canonical complete lifecycle.
Checksums detect drift, not forged provenance or authorization.

The PostgreSQL adapter serializes account replacement under the parent row lock
and commits it **together with** ownership/admission/settlement. Invocation starts
also compare the observed child-account digest under that lock. Computing and
saving two snapshots independently is not supported. Accounting settlement is
not a child Join; all other lifecycle and graph completion checks still apply.

## Pinned child admission preparation

`ChildRunAdmissionIntent` now retains a parent admission digest, logical child
key, candidate `AgentAdmissionIntent`, complete compiled child graph, private
initial state, and a stable `spawn_digest`. Its complete canonical envelope is
bounded to 16 MiB. Both construction and deserialization check internal scope,
graph/schema closure, interoperable JSON numbers, and digest integrity.

The retry digest binds the full child descriptor, graph reference (and hence
definition), request, budget layers and resolved budget, authority evidence,
initial state/ready set, parent admission/key, and fixed `cancel_and_join` close
policy. Candidate child Run/thread/invocation IDs are excluded, matching ingress
submission semantics; they remain in the envelope with their admission-intent
checksum. Changed IDs alone preserve `spawn_digest`, while changed business
inputs conflict. Admission event and checkpoint IDs are not allocated here.

`ResolvedBudget::validate_narrowing` rejects widening of every scalar, deadline,
or currency ceiling without clamping. Same-principal child authority and scope
subset checks are implemented. This is structural validation, not permission
to redelegate all granted scopes: a trusted admission policy still has to
authorize the particular declared child slot and target. Cross-principal
identity exchange is not supported by this first profile.

`DurableAgentAdmission::prepare_child` binds the candidate to the frozen
executable registry using a `StoredAgentAdmission` and full current checkpoint.
`validate_child_preparation` rechecks restored candidates. Both methods are
synchronous and read-only: no child Run, lease, budget reservation, journal
event, or provider call is created. A compiled usage example is in the method's
Rust documentation (`cargo test -p stateknot-runtime --doc --locked`).

Preparation requires an Active, non-quarantined parent snapshot, matching
current checkpoint pointer and deterministic ready-root activation, exact
parent admission, a declared exact child target, available parent/child executable closures, live parent/child
deadlines, and offline validation of child input/state/authority evidence.
Same-run nested namespaces are refused. Independent child state can use a
different schema; parent and child state are not implicitly merged.

The lower-level core `validate_for` requires externally supplied authoritative
parent/checkpoint/schema/clock data. Deserialization cannot authenticate those
sources. A valid historical checkpoint is not necessarily current, and a
successful read-only check can immediately race cancellation or other spending.
Commit-time validation repeats mutable readiness/authority checks while holding
the necessary locks and includes active topology and outstanding reservations.
Exact committed lost-ACK lookup must precede fresh deadline/readiness checks.

The spawn fixture digest is
`sha256:64a2d833b22e3f017d502b4a1c30281ef44680c3c0a710399b1eec02c5d595b8`.

## Verification and next implementation gates

Core tests cover canonical ownership identity, scope/input/version changes,
tampering and strict deserialization, physical retry/fence stability, every
cumulative dimension, subset ceilings, exact boundary capacity, unknown cost,
currency refusal, integer/monetary overflow, work bounds, and order-independent
arithmetic. The frozen ownership fixture digest is
`sha256:21da0ea8e1d1e20c7745a8a4f1794bffc6f42f7d62c24145db141c2dfb855a48`.

Real PostgreSQL preparation tests prove no child rows or node dispatches,
candidate-ID replay, registry recreation, schema/deployment refusal, rejection
after parent cancellation, and refusal of old/nonmatching checkpoints after
the ordinary graph driver has committed a real noninitial checkpoint. These
tests do not establish atomic child execution or budget settlement.

Before enabling automatic durable children, finish dedicated Join bindings,
lease-releasing parent suspension/resumption, durable cancel-and-join propagation,
registry recreation and end-to-end fault qualification. The website must not
advertise the full capability until those gates pass.
