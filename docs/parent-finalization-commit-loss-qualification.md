<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Parent-finalization COMMIT-loss qualification

[中文](parent-finalization-commit-loss-qualification.zh-CN.md)

`parent-finalization-commit-loss-v1` qualifies the client-side ambiguous-COMMIT
boundary of the production `PostgresStore::complete_run_failure_close`
transaction. The parent has an immutable original failure decision and frozen,
fully priced direct usage. A real owned child has been cancelled, physically
closed and settled with separately priced subtree usage. The final transaction
must append one completion event, change the parent to Failed with exact direct
plus child usage, and mark the original close record complete atomically.

The test adds no production fault hook, SQL trigger or transaction mock. Each
cell uses a fresh tenant, child, single-session proxy and worker process. A new
process reconstructs state through PostgreSQL after the interrupted worker is
force-killed. Independent readers use nonlocking MVCC queries while the writer
holds the parent row lock.

## Required matrix

| Cut | Required result |
|---|---|
| `commit_not_forwarded` | The completion event insert and parent projection update reach PostgreSQL while the proxy holds the frontend COMMIT. Independent readers still see the Active parent, pending close, unchanged journal and settled child. After `SIGKILL` and backend disconnect, the entire snapshot is unchanged; a fresh process commits completion once. |
| `commit_response_withheld` | COMMIT reaches PostgreSQL, but the proxy consumes its response. Independent readers see the Failed parent, exact terminal usage, original failure, one completion event and completed close record. A fresh process submits a new candidate event, receives `Existing` and preserves the original terminal event. |

The compared snapshot includes both full run projections, the original close
row, immutable child ownership/settlement and cancellation receipt, the complete
budget account, pending queues, both complete journals, and raw close-row and
completion counts. The child settlement, original failure and registration head
must remain unchanged. A second replay must not change any durable field. The
ordinary failure-close sweep must have no pending candidate after recovery.

## Instrumentation and scope

The test-only proxy accepts exactly one plaintext PostgreSQL protocol 3.0
session and rejects non-loopback targets. Its closed transaction-target enum
arms on the exact parent journal-head projection `UPDATE` Parse prefix and cuts
the next exact simple-query `COMMIT`. It bounds frames, preserves partial reads,
never logs protocol frames, and pins SQLx 0.8.6 transaction framing. Unexpected
framing, a missing target, early worker exit or incomplete evidence fails
closed. The controller kills only its owned process and waits for the captured
PostgreSQL backend PID to disappear. Unix requires real `SIGKILL`. Production
TLS and database traffic never cross this proxy.

Use a disposable loopback PostgreSQL 16 or 17 database:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::failure_close::completion_commit_loss::failure_close_completion_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

Both mandatory PostgreSQL CI jobs require exactly two
`STATEKNOT_PARENT_FINALIZATION_COMMIT_LOSS_EVIDENCE` records and a successful
exit. They retain
`parent-finalization-commit-loss-postgres-<version>-<run-id>` for 30 days with
the qualification log, source/tree identities, lockfile digest, toolchain,
PostgreSQL image, kernel and host inventory. Database URLs, credentials and
Agent inputs/outputs are never evidence fields.

## Gates still open

This is a client-process fault profile, not a PostgreSQL server/WAL fault test.
Join-result consumption still lacks an equivalent in-flight COMMIT cut. Unknown
provider effects/prices, failover, PITR/restore, untrusted-worker SQL isolation,
same-run nested namespaces, retained-history capacity, latency and soak remain
unqualified. RFC-0004 stays Draft; this does not advertise the complete durable
child-run profile as production ready.
