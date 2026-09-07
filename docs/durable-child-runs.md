<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable child runs: core contracts and remaining work

Status: core identity, cumulative capacity, and read-only child admission
preparation implemented; durable
child admission, ownership storage, joins, and cancellation are **not yet
implemented**. [RFC-0004](rfcs/0004-durable-child-runs.md) remains Draft. See
the [Chinese edition](durable-child-runs.zh-CN.md) and the already implemented
[static shared-state composition](graph-composition.md) for the distinction.

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

A checksum is not an authorization proof or signature. The future store must
validate actual committed readiness and current admission authority. The key
also does not bind a child spawn intent: pinned child executable, input,
authority, budget, and initial-state comparison remain separate required work.

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

This is not a database reservation or settlement API. A storage transaction
must serialize child admission with other children **and parent direct work**,
bind allocations to unique ownership keys, read the complete outstanding set,
and settle once against verified terminal evidence. It must also check child
deadline narrowing, authorization, ancestry, active subtree concurrency, and
fan-out independently. Existing high-water usage is validated, but remaining
high-water counts returned by this calculation do not authorize new topology.

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
parent admission, available parent/child executable closures, live parent/child
deadlines, and offline validation of child input/state/authority evidence.
Same-run nested namespaces are refused. Independent child state can use a
different schema; parent and child state are not implicitly merged.

The lower-level core `validate_for` requires externally supplied authoritative
parent/checkpoint/schema/clock data. Deserialization cannot authenticate those
sources. A valid historical checkpoint is not necessarily current, and a
successful read-only check can immediately race cancellation or other spending.
Future commit-time validation must repeat these checks while holding the
necessary locks and include active topology and outstanding reservations.
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

Before enabling durable children, finish registered executable child-slot
declarations and explicit delegation authorization; active topology enforcement;
atomic PostgreSQL
admission/reservation and direct-work enforcement; version-safe closure guards;
terminal binding/settlement; durable join/cancel/resume; and PostgreSQL 16/17
fault qualification. No database migration or website capability claim is
introduced by the core-contract increment.
