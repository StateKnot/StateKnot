<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Deadline cancel-and-join process-loss qualification

[中文](deadline-join-process-qualification.zh-CN.md)

The executable `deadline-cancel-join-committed-boundaries-v1` profile qualifies
deadline-driven cancellation of a parent suspended on an isolated child Join.
It uses the production deadline, child-reconciliation, lease-fencing and Agent
Loop paths. It does not add a test-only recovery API or claim that an in-flight
provider effect was stopped exactly once.

## Committed boundary matrix

Every row runs in a fresh OS process with fresh PostgreSQL connections. Only the
immutable `ChildRunKey` crosses process boundaries. Each worker reconstructs the
admissions, executable graph registry, physical attempts, cancellation evidence,
accounting, Join and lifecycle state from PostgreSQL.

| Phase | Required durable observation before and after forced termination |
|---|---|
| `admission` | Parent activation and physical attempt, atomic child ownership/admission, one reservation and a live parent fence. |
| `join` | Complete Join membership is sealed and the parent lease is released; no successful Join result is published or consumed. |
| `deadline` | The production tenant sweep observes both parent and child inherited deadlines and commits exactly one `agent.deadline.expired` request for each; the parent transaction also queues child cancellation. |
| `child_cancel` | The production child reconciler consumes the parent queue, records `AlreadyRequested`, and preserves the child's original deadline reason instead of overwriting it. |
| `child_terminal` | A fresh worker obtains the child lease and the real `DurableAgentLoop` commits verified cancellation usage. |
| `settlement` | The child reconciler replaces the reservation exactly once with the child's terminal usage and makes parent cleanup runnable. |
| `takeover` | A higher parent lease epoch commits. A retained worker's old-epoch lease renewal must return `StaleFence` before recovery continues. |
| `parent_terminal` | A fresh worker reconstructs the parent registry and the real Agent Loop commits cancellation with child-inclusive accounting; the successful Join remains unpublished and unconsumed. |
| `replay` | Deadline, cancellation and settlement queues are empty and no lifecycle, journal, accounting, attempt, checkpoint or Join evidence changes. |

The controller independently loads a complete snapshot before and after killing
each owned worker. Unix requires signal 9 (`SIGKILL`); other platforms require
`Child::kill`. Normal exit, a nonexistent filter, malformed readiness, a changed
snapshot, a non-`StaleFence` old-owner result or a partial evidence matrix fails
closed. The shared harness bounds readiness to 90 seconds, kill/reap to 10
seconds and pre-readiness output to 64 KiB. These are harness safety limits, not
service SLOs.

Parent and child share the inherited finite deadline. The qualification
therefore requires the real tenant sweep to return both identities rather than
artificially selecting only the parent. Later child-cancellation delivery must
be idempotent and retain the child's earlier deadline reason. This exercises the
production race without weakening first-reason-wins semantics.

## Reproduce and retain evidence

Use an isolated PostgreSQL 16 or 17 database:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::deadlines::process::deadline_join_survives_process_loss_and_rejects_retained_stale_worker \
  --nocapture --test-threads=1
```

Both PostgreSQL CI jobs require exactly nine
`STATEKNOT_DEADLINE_JOIN_PROCESS_EVIDENCE` records and a successful test exit.
They retain `deadline-join-process-postgres-<version>-<run-id>` for 30 days with
the log, source/tree identities, lockfile digest, Rust toolchain, PostgreSQL
image, kernel and CPU/memory/filesystem inventory. Evidence contains no database
URL, credential, Agent input or child output payload.

## Boundaries still open

This profile terminates processes only after known successful commits. It does
not itself cut transactions while COMMIT is in flight. The separate
[Join/deadline COMMIT-loss profile](join-deadline-commit-loss-qualification.md)
now covers the deadline cancellation transaction, and the
[child-cancellation delivery profile](child-cancellation-commit-loss-qualification.md)
covers propagation plus real Wait cleanup. The
[child-settlement profile](child-settlement-commit-loss-qualification.md)
now covers accounting settlement. The separate
[parent-finalization profile](parent-finalization-commit-loss-qualification.md)
now covers terminal closure; Join-result consumption remains open.
It does not qualify unknown
provider effects or pricing,
database failover/PITR/restore, untrusted-worker SQL isolation, retained-history
capacity, latency or soak. Those gates remain explicit in RFC-0004 and must pass
before the general durable-child capability is advertised as production ready.
