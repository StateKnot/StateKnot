<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable child runs: core contracts and remaining work

Status: core identity and cumulative-capacity primitives implemented; durable
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

## Verification and next implementation gates

Core tests cover canonical ownership identity, scope/input/version changes,
tampering and strict deserialization, physical retry/fence stability, every
cumulative dimension, subset ceilings, exact boundary capacity, unknown cost,
currency refusal, integer/monetary overflow, work bounds, and order-independent
arithmetic. The frozen ownership fixture digest is
`sha256:21da0ea8e1d1e20c7745a8a4f1794bffc6f42f7d62c24145db141c2dfb855a48`.

Before enabling durable children, finish executable declarations and spawn
intent binding; topology/deadline/authority narrowing; atomic PostgreSQL
admission/reservation and direct-work enforcement; version-safe closure guards;
terminal binding/settlement; durable join/cancel/resume; and PostgreSQL 16/17
fault qualification. No database migration or website capability claim is
introduced by the core-contract increment.
