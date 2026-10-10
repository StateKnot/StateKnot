<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Claimed Root and lifetime-frame application replay

[中文](scoped-graph-application-replay.zh-CN.md)

`ClaimedRunRecovery::validate_graph_replay` independently executes the exact
deployed graph schemas and pure reducers over the original claimed observation.
`Some(leaf)` requires that exact active leaf. `None` requires Root scope and still
validates every previously returned frame. This Store boundary is experimental;
RFC-0022 remains Draft and the actual nested Driver is a separate implementation
and qualification gate.

Claimed recovery authenticates the complete canonical current checkpoint before
returning a session. Durable canonical corruption can quarantine the current
live owner against the exact observed journal head and fence at session creation;
a stale owner or stale session cannot quarantine its successor. Valid legacy
history whose historical disposition is unavailable remains an operational
replay error. Session-creation timing compatibility with the earlier
implementation is not claimed.

## Dependencies and coverage

The resolver receives the complete authoritative `GraphReference`, including its
canonical definition digest. Store authenticates the tenant-owned graph closure
and requires the returned reducer's complete reference to equal the compiled
graph's reducer pin. There is no latest-version selection or fallback reducer.
The same resolver can be reused across claimed sessions. The schema validator
must implement the pinned schema contract; reducers must be deterministic,
side-effect free and finite. Catching a panic does not stop a nonterminating
synchronous callback. Worker renewal and execution isolation remain Driver
requirements.

The read covers Root's initial and subsequent checkpoints, every lifetime frame
entry and transition, noninitial suspended parents, the current leaf, complete
returned-frame intents and physical caller completions, result/invocation
ownership and consumption, and scoped wait registration/resolution/firing or
abandonment proofs. A returned child is replayed before its parent's consumed
return. Application callbacks receive the actual committed successor ID. Store
compares the full scoped barrier, disposition, successor and parent return
intent; a matching state digest alone is insufficient.

Legacy Root Continue edges can authenticate the exact `Unchanged` lifecycle
projection plus the complete barrier intent. Existing generic Root writers did
not persist the original lifecycle revision and full disposition. Legacy Root
Wait/Terminal, or a Continue edge with another valid generic lifecycle
projection, therefore returns `GraphReplayHistoricalDispositionUnavailable`.
This is an operational compatibility gap: Store does not invent a revision,
quarantine valid legacy history, or return a complete replay report. A durable
Root disposition bundle and migration/old-business upgrade qualification are
required to authenticate those histories.

`GraphRecoveryReplayReport::root_report()` returns the existing Copy
`GraphReplayReport` measured during the same Root pass. Its checkpoint, barrier,
result and maximum-result-byte counters exclude scoped frames. Obtaining this
view repeats no callbacks and requires no independent Root replay.

## Transactions, memory and work

Capture and final verification each own one repeatable-read structural proof
cache. That cache cannot cross a transaction boundary. Each intervening
application edge is authenticated directly in a fresh transaction; Store closes
it before invoking the resolver, schemas or reducer. Complete owning proofs can
still revisit Root checkpoint ancestors, particularly with schema-34 Root
bundles. Total traversal complexity is not yet qualified. A constant-memory,
domain-separated, length-prefixed digest transcript binds all exact component
heads and digests across capture, application and final reads.

| Resource | Bound |
| --- | --- |
| Open frames | Seven |
| Lifetime frames/returns | 4096; compact look-ahead rejects row 4097 before full decoding |
| Graph closure | 1024 definitions and 16 MiB of canonical definitions |
| Compact lifetime manifest | Measured 64 MiB ceiling |
| One barrier's compact results | `GraphReplayLimits`: default 64 MiB, legal maximum 512 MiB |
| State and individual JSON readers | Their independent `JsonLimits::MAXIMUM` ceilings |
| Graph work | Exact scope maxima and original admitted graph-step budget, including entries and settlements |

These are separate bounds, not a claim that the whole process uses 64 MiB.
Only compact scope heads, exact pins and settlement bindings survive callbacks;
one scope's base/successor and one bounded result set are transient.
`structural_edges_verified()` counts the instrumented structural passes and
direct materializations. It does not measure every nested owning checkpoint
walk or SQL query. The earlier Root-only 8/16/32-edge fixtures report 24/48/96
edges. A seven-level cascade with 13 scoped barriers and seven returns visits
79: `5 × 13 + 2 × 7`; the two full passes each authenticate one additional
terminal edge per settlement. These reported counts do not establish linear
total work for schema-34 histories. A transaction-owned complete Root-prefix
proof and equivalence/corruption qualification remain required before making
that claim.
Scoped 8/16/32-edge histories visit 40/80/160 edges. Two returned siblings,
two Root continuations and seven open scopes with six noninitial suspended
parents visit 50: `3 × 2 + 5 × 8 + 2 × 2`. Their ten application transitions
invoke reducers exactly ten times; changing an old child's or Root's deployed
reducer is detected even when the current leaf is valid.

## Final observation and errors

The entire read uses the session's original quarantine context exactly once.
Every edge and the final verification recheck its original journal, complete
canonical lifecycle, scheduler/wait projection, Root pointer, exact leaf and
database-clock live fence. Normal lease renewal under that fence is allowed.
A report has private fields, contains
counts and exact heads, and grants no dispatch authority. The dedicated durable
start transaction remains decisive; the actual Driver must use this read before
handoff and stop on any failed observation.

| Condition | Outcome |
| --- | --- |
| Crossed tenant/Run leaf | `InvalidClaimedRunRecoveryContext`, no quarantine |
| Journal/lifecycle/scheduler/wait projection/head/lease/fence changed | Actual stale or authority error, no report |
| Missing exact dependency, dependency resource failure or panic | `GraphReplayDependencyUnavailable`, no quarantine |
| Local manifest/result bound | `GraphReplayResourceLimit`, no quarantine |
| Unprovable legacy Root disposition | `GraphReplayHistoricalDispositionUnavailable`, no quarantine |
| Database failure | Existing database error |
| Durable schema rejection or complete proof/semantic mismatch | Original exact fenced quarantine attempt |

A callback that supersedes or expires the original fence cannot make that old
context quarantine a successor worker. The read creates no application events,
node starts, results or checkpoints and performs no repair. Administrative
corruption fixtures are test-only; runtime roles retain immutable-table controls.

Real PostgreSQL tests exercise updates, seven-level open/returned stacks,
Root-after-pop consumption, wait/resume, exact pins and dependency errors,
callback journal/fence changes, a one-connection pool and independently acquired
Run mutation lock, component changes after capture, and actual linear visit
counts. Wait/resume coverage includes a later committed terminal transition;
the saved wait is replayed once and its resolved checkpoint is not duplicated.
Original-projection changes fail stale while normal renewal remains valid.
This scope does not close nested Driver, child/Join execution,
provider-effect settlement, process/ACK fault matrices or R1 qualification.
