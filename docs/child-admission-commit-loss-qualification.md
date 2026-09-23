<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Child-admission COMMIT-loss qualification

[中文](child-admission-commit-loss-qualification.zh-CN.md)

`child-admission-commit-loss-v1` qualifies the client-side ambiguous-COMMIT
boundary of the production `PostgresStore::admit_child_run` transaction. That
transaction creates the isolated child Run, admission event and initial
checkpoint while also recording ownership, reserving the complete child budget,
advancing the parent journal and preserving the active parent node attempt.

This profile uses no production fault hook, SQL trigger, mocked transaction
result or database-server crash. Each cell owns a fresh tenant, OS process,
single PostgreSQL connection and transaction. The controller reads state through
independent connections and never accepts the interrupted worker's memory as
recovery evidence.

## Required matrix

| Cut | Required result |
|---|---|
| `commit_not_forwarded` | Every transaction statement has completed, including ownership and reservation writes, but the proxy does not forward the frontend COMMIT. While the client is blocked, no child, admission, checkpoint, ownership, reservation or parent journal advance may be visible. Killing the process and closing its backend must leave the exact pre-transaction snapshot. A fresh process then commits the complete set once. |
| `commit_response_withheld` | COMMIT reaches PostgreSQL, and the proxy consumes `CommandComplete(COMMIT)` plus idle `ReadyForQuery` without forwarding either. The complete atomic set must already be visible to independent readers. A fresh process supplies new parent-event, child-event and checkpoint candidates but must return `Idempotent` with the original durable identities and without advancing either journal or reserving budget twice. |

The compared snapshots cover parent lifecycle, journal head, lease, scheduler and
checkpoint; candidate child admission, lifecycle, first event, checkpoint and
journal; ownership, ancestry and spawn event; the parent budget account; child
identity enumeration; and pending settlement work. After recovery, the child
must remain claimable and the production parent terminal guard must reject
completion while that child is unsettled.

## Instrumentation boundary

The test-only proxy accepts exactly one plaintext PostgreSQL protocol 3.0 session
and only a literal loopback address or `localhost`. A closed transaction-target
enum arms on the exact `INSERT INTO stateknot.child_run_ownership ` Parse prefix,
then cuts only the next exact simple-query `COMMIT`. Startup, authentication and
bind frames are forwarded but never logged; frame sizes are bounded and partial
reads preserve state.

Unexpected protocol exchange, missing target, early worker exit, incomplete
evidence or a non-loopback target fails closed. The controller force-kills only
its owned worker, closes the proxy session and waits for the exact PostgreSQL
backend PID to disappear. Unix must observe `SIGKILL`. This profile pins SQLx
0.8.6 transaction framing and must be requalified if that framing or target SQL
changes. Production TLS and database traffic never pass through the proxy.

## Reproduce and retain evidence

Use a disposable loopback PostgreSQL 16 or 17 database:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::child_admission_commit_loss::child_admission_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

Both mandatory PostgreSQL CI jobs require exactly two
`STATEKNOT_CHILD_ADMISSION_COMMIT_LOSS_EVIDENCE` records and a successful exit.
They retain
`child-admission-commit-loss-postgres-<version>-<run-id>` for 30 days with the
qualification log, source/tree identities, lockfile digest, Rust toolchain,
PostgreSQL image, kernel and host inventory. Database URLs, credentials, Agent
inputs and child outputs are not emitted.

## Gates still open

This is a client-process fault profile, not a PostgreSQL server/WAL fault test.
The separate [child-cancellation delivery profile](child-cancellation-commit-loss-qualification.md)
now cuts cancellation delivery. Settlement, terminal finalization and Join-result
consumption remain uncut while COMMIT is in flight. It does not qualify unknown
provider effects/prices, failover, PITR/restore, untrusted-worker SQL isolation,
same-run nested namespaces, retained-history capacity, latency or soak. RFC-0004
therefore remains Draft and the complete durable-child profile is not advertised
as production ready.
