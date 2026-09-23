<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Durable child Join process-loss qualification

[中文](child-join-process-qualification.zh-CN.md)

The executable `child-join-committed-boundaries-v1` profile qualifies the
successful isolated-child path across real host-process loss. It complements
the failure-close and COMMIT-loss profiles; it does not enable the complete
durable-child production profile or claim exactly-once provider effects.

## Committed boundary matrix

Every row runs in a fresh OS process with fresh PostgreSQL connections. Only
the immutable `ChildRunKey` crosses process boundaries; the worker reloads and
verifies the parent admission, child intent, executable graphs, schemas,
physical attempts, accounting and Join evidence from PostgreSQL.

| Phase | Required durable observation before and after forced termination |
|---|---|
| `admission` | Parent activation, executing physical attempt, one atomic child ownership/admission and one outstanding reservation; no Join exists. |
| `join` | Complete membership is sealed and the parent lease is released; the original physical attempt remains explicitly unfinished. |
| `child_terminal` | The child executes under its own lease and reaches verified Success; the parent remains suspended and unconsumed. |
| `settlement` | The reservation is replaced exactly once by the complete child subtree usage and the Join becomes publishable. |
| `publication` | Immutable terminal/output evidence and the parent wakeup head commit; the publication is not yet consumed. |
| `takeover` | A higher parent lease epoch commits after wakeup. A retained old worker must receive `StaleFence` before recovery continues. |
| `parent_resume` | A new physical attempt binds and uniquely consumes the exact Join head; the parent graph resumes and reaches verified Success. |
| `replay` | Fresh queue scans are empty and no lifecycle, journal, accounting, attempt, checkpoint or Join evidence changes. |

For each phase, the controller independently reads a complete snapshot before
and after killing its owned worker. Unix requires signal 9 (`SIGKILL`); other
platforms require `Child::kill`. A normal worker exit, nonexistent test filter,
malformed readiness message, changed snapshot or partial evidence matrix fails
the profile. The shared harness bounds readiness to 90 seconds, kill/reap to
10 seconds and pre-readiness output to 64 KiB. These are harness safety bounds,
not production SLOs.

The retained worker captures the original live fence before Join registration,
then waits on the controller's liveness pipe. After publication, another fresh
process commits a strictly higher epoch. Only then may the retained process
attempt a write; the store must return `StaleFence`, not a journal conflict or
an apparent successful replay. Controller loss exits the worker through the
existing orphan watchdog and never counts as qualification.

## Reproduce and retain evidence

Use an isolated PostgreSQL 16 or 17 database:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::join::process::child_join_survives_process_loss_and_rejects_retained_stale_worker \
  --nocapture --test-threads=1
```

Both PostgreSQL CI jobs require exactly eight
`STATEKNOT_CHILD_JOIN_PROCESS_EVIDENCE` records and a successful test exit.
They retain `child-join-process-postgres-<version>-<run-id>` for 30 days with
the log, source/tree identities, lockfile digest, Rust toolchain, PostgreSQL
image, kernel and CPU/memory/filesystem inventory. Evidence contains no
credentials, Agent input or child output payload.

## Boundaries still open

This profile cuts only after known successful commits. The companion
[deadline cancel-and-join profile](deadline-join-process-qualification.md)
qualifies the cancellation branch. The separate
[Join/deadline COMMIT-loss profile](join-deadline-commit-loss-qualification.md)
now cuts Join registration, Join publication and deadline cancellation at an
unforwarded COMMIT and a withheld committed response. The
[child-admission profile](child-admission-commit-loss-qualification.md) covers
admission separately, and the
[child-cancellation delivery profile](child-cancellation-commit-loss-qualification.md)
now covers cancellation propagation and real Wait cleanup. Settlement,
finalization and result-consumption transactions remain unqualified while COMMIT
is in flight. Unknown provider effects or pricing,
database failover/PITR/restore, untrusted-worker SQL isolation, retained-history
capacity, latency and soak also remain open. Those gates stay explicit in
RFC-0004 and must pass before the general durable-child capability is advertised
as production ready.
