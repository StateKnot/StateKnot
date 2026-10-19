<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental registered scoped Join owner-stop qualification

Snapshot: 2026-10-18 11:26 UTC. [中文](experimental-scoped-join-owner-stop-2026-10-18.zh-CN.md) · [Machine record](experimental-scoped-join-owner-stop-2026-10-18.json).

This record covers an experimental public Store cancellation boundary under
[Draft RFC-0022](../rfcs/0022-namespaced-graph-frames.md#registered-join-handoff-during-whole-frame-cancellation).
It does not enable scoped child Join in the Driver. At this snapshot, both
PostgreSQL majors passed the selected settlement and COMMIT-response-loss cases,
and six local quality gates completed successfully. Final composed full Native
suites and the portable raw-evidence delivery are pending. Complete CI, Root
ConfirmCancellation, RFC acceptance, Supported status and R1 remain open.

## Selected contract

After the unique latest ordinary owner in the actual current leaf registers an
exact unpublished Join, its lease is released while its original attempt remains
Executing. Cancellation can close the frame stack only after every child in the
complete Root-bound lifetime account is terminal and settled. The coordinator
supplies complete priced Run-wide DIRECT at the saved observation; structural
and account DIRECT observations remain lower bounds. Delegated usage is added
once. The proof grants no ordinary completion/result, Join publication or
consumption, lease, launch grant or further worker authority.

The owning transaction authenticates the full Root/frame lineage, original
attempt/history/fence, request/registration and complete ownership set. It locks
all settled terminal children in canonical account order, rechecks their owning
records and selected Join, and retains deferred guards plus post-guard owning
verification. Fresh quarantine, another unfinished ordinary owner, a superseded
owner, other unfinished work, unresolved Tool/Model effects, unpriced usage and
unsettled children remain refusals. Sealed failure close keeps its strict checks.
An already committed original decision is fully authenticated before evaluating
fresh candidate fields; later operator quarantine does not invalidate that ACK.

## Source and execution binding

The final execution source is the immutable composed-v2 snapshot, based on
`e372184fb1c80ffb9903ac0d9feb8380db51253e`:

| Input | Exact value |
| --- | --- |
| Source files / SHA-256 | 936 / `e8f3b8a1c93cbbd8ad00fa3622657be032a98fc775ea9580337c27c1d8ad77db` |
| Compiled inputs / SHA-256 | 544 / `acc2f7706a60201874a2dec4de5c690fcee4bf7a04409822bf409f72ca7f1c16` |
| Cargo.lock SHA-256 | `fc36c3966dc2abf990fae6a01b633459d95fedae0fd7f77c429b5f04447b2e94` |
| Store Native binary SHA-256 / inventory | `c7c5daf1e2c9606d57f2fdf5c4791b2eacffc65e37b7b50c50de60465a63d474` / 267 |
| Runtime Native binary SHA-256 / inventory | `6f633c5883ccdba9308d7ba408c044653ec4c3a95c9a3c27469ae15e0dda4e7e` / 194 |
| Exact graph-frame catalog SHA-256 | `cc9ddd58510822215dfa18cab1a956eacf2dd707860270da63f0b8d707ee0ad7` |

Native targets use Rust1.88.0, default features, test opt-level1, no debug
symbols/incremental cache, enabled debug assertions and overflow checks, and
the normal thread stack. Core, Store, Runtime and the selected Runtime test
artifact were rebuilt with `fresh=false`; that freshness does not mean a cold
dependency cache. The two saved binaries execute directly with one test thread.
PostgreSQL16.15 and17.11 use distinct fixed-image loopback containers and fresh
databases, with fsync, synchronous_commit and full_page_writes on before/after.
Source/binary bytes and database/container bindings are retained in the receipts.

## Executed composed-source cases

| Case | PG16 | PG17 |
| --- | --- | --- |
| Store settlement prerequisite | exit0; 1 passed, 266 filtered; 2.6120s | exit0; 1 passed, 266 filtered; 6.5325s |
| Public Store lost COMMIT response / cold process | test exit0; 1 passed, 193 filtered; 3.8833s | test exit0; 1 passed, 193 filtered; 3.9361s |
| Independent COMMIT-loss evidence validator | exit0 | exit0 |
| Full Store267 / Runtime194 Native suites | Pending at snapshot | Pending at snapshot |

The settlement case refuses both a live child and a terminal-but-unsettled child
through public Store APIs, checks complete tenant facts for no writes on both
refusals, and accepts only after real settlement. The stopped owner, unpublished
Join and accounting remain original facts.

The Runtime-package process test exercises public Store APIs; no Driver runs.
The protocol proxy withholds the actual backend COMMIT CommandComplete and idle
ReadyForQuery. An independent reader proves the committed v2 closure before the
owned live writer is SIGKILLed and reaped. A separate cold process loads the
identical closure and recovers Existing from a new event ID, stale revision and
zero candidate usage, without changing tenant facts. The original Executing
owner has no completion and the Join remains unconsumed. The independent
validator requires exactly22 typed marker fields, real Unix signal9, cold
success, exact libtest counts, saved source/binary and durability bindings.

PG16's original wrapper exited1 because its contiguous `test CASE ... ok`
observer did not allow the legitimate nocapture marker between the start and
independent `ok`. Actual test exit0 and all original logs/false observations
remain unchanged. Its outer-result metadata is explicitly Root-transcribed from
the real tool result. PG17 used a new observer wrapper and captured actual outer
exit0 directly. The offline validators independently returned0 for both inputs;
they do not relabel PG16's original wrapper as successful.

## Local gates and separate prior seams

All six gates bind composed-v2 source: fmt and strict workspace Clippy completed
with exit0 in gates-v2; workspace tests, doctests, warning-denying Rustdoc and
the pinned nightly check completed with exit0 in gates-v3. Workspace totals are
100 harness summaries, 2,128 passed and six existing ignored cases; doctests have
48 passed. Those all-feature, opt-level0 workspace runs have no mandatory
PostgreSQL environment and confer no Native/database qualification. Native
default-feature opt-level1 results remain separate above.

Earlier public-consumer executions separately covered persisted v2 offline
schema validation/cold owning read and retained source-v1 schema36→37 migration
through the real public SQLx Migrator on both majors (four exact cases, each
exit0/one passed/one filtered). Their product tree is
`bf329a3da4eacc9ccd9c34e681a1d53c8c9eab662e2c0a4b1831aac8f8266509`;
their consumer tree is
`5d77326d5cce4988506b7acb549cedb37eba5be62c14b857502736eeffaa806d`,
with binary `7c174940e83c9435e66ed7675b5faba4af3c8302c79732a648553b609350b30b`.
They retain exact v1 closure/Root/business bytes and original migration rows.
These source-fixture results are not published N−1/N−2 compatibility, rolling
upgrade or restore qualification, and do not automatically qualify later source.

## Migration and schema adoption

Migration37 replaces the complete closure guard, adds no table/backfill/ACL
privilege and preserves schema36's 54-table role inventory. The v2 envelope and
intent add exactly one required three-field `stopped_join`; v1 bytes, domains
and the original schema getter stay unchanged. Consumers register both exact
offline schema documents when reading both histories:

| Getter | Version | JCS document digest |
| --- | --- | --- |
| `graph_frame_closure_event_schema()` | 1.0.0 | `sha256:453045e205133eb10d3cd58f0242ee2ebbd26dd78051b0888cd6a95f8a53e520` |
| `graph_frame_join_stop_event_schema()` | 2.0.0 | `sha256:aec9b96de74a4185f288e1425a59a7fb8d8eb4363378bbd5522b2500a261ea5f` |

Drain old workers, retain a verified pre-upgrade backup, perform the normal
checksum-pinned SQLx migration through37, apply/audit the matching role profile
and exact catalog, then start matching reader/writer binaries. Never deploy by
raw function replacement or ledger edits. The test revert fixture refuses
retained v2 and provides no production downgrade path. A current role LOGIN/ACL
run remains distinct from an unchanged allowlist.

## Retained failures and remaining gates

The original baseline tracer's actual PostgreSQL16/17 `InvalidRunFailureClose`
test101/no-write evidence is retained. Earlier manifest-ordering and sandbox
Docker preflight failures remain distinct from executed product failures.
The ACK preflight Clippy101 (local Version declaration ordering), composed-v1
Clippy101 (long Runtime controller), and composed-v2 workspace101 (five
loopback-binding permission failures) remain saved. Later source extraction and
an elevated same-source workspace run resolved their respective checks; no lint
allow, budget increase, test removal or stack override was used for those fixes.

Historical CI measurement [39120579635](https://github.com/StateKnot/StateKnot/actions/runs/39120579635)
at `dc13238b766eef98783f05cabb896e840a723be6` ended11success/2cancelled under
the original30-minute PostgreSQL limits. Its source is unchanged e372 product
plus CI scheduling measurement, not this Join candidate. The60 PostgreSQL source
steps and failed/incomplete later chain remain evidence; neither that run nor a
future scheduling candidate transfers complete CI qualification here.

Before adoption, Root must deliver the indexed raw evidence and update the
pending full Native outcomes from actual receipts. The first profile continues
to exclude published-but-unconsumed Join, spawn-before-registration and a later
resumed physical owner. Broader negatives, late rollback/fault matrices,
published-version compatibility, current role qualification, Root terminal
confirmation, all three Driver guard removals and final-source complete CI need
their own evidence. Capacity, independent security and deployment/release gates
remain separate. RFC-0022, R1 and Supported status remain open.
