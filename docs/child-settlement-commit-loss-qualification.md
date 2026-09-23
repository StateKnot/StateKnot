<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Child-settlement COMMIT-loss qualification

[中文](child-settlement-commit-loss-qualification.zh-CN.md)

`child-settlement-commit-loss-v1` qualifies the client-side ambiguous-COMMIT
boundary of the production `PostgresStore::settle_child_run` transaction. Its
child has a durable, fully priced failed terminal and a later unchanged audit
event. The settlement must bind the original terminal event, append exactly one
parent audit event, replace the outstanding budget reservation with immutable
subtree usage, store one settlement fact and remove the pending notification
atomically. The parent must subsequently close using that usage exactly once.

This profile adds no production fault hook, SQL trigger or transaction mock.
Each cell owns a fresh tenant, child, worker process, single PostgreSQL session
and transaction. Fresh recovery processes reconstruct state through PostgreSQL;
the controller reads through independent connections.

## Required matrix

| Cut | Required result |
|---|---|
| `commit_not_forwarded` | All settlement statements, including the settlement insert, reach PostgreSQL while the proxy holds the frontend COMMIT. Independent readers still see the original pending notification, reservation, parent journal and no settlement. After `SIGKILL` and backend disconnect, the complete snapshot remains unchanged; a fresh process commits the settlement once. |
| `commit_response_withheld` | COMMIT reaches PostgreSQL, but the proxy consumes its response without forwarding it. Independent readers see the complete settlement, account transition, parent audit and notification removal. A fresh process uses a new candidate event, receives the original event from the idempotent path and changes no durable field. |

The compared snapshot includes parent/child lifecycle, journal head, lease,
scheduler, waits and checkpoint; immutable ownership and settlement; the full
budget account; notification listing; both complete journals; and raw ownership
pending/settled flags plus the settlement row count. Partial settlement,
duplicate charge, replaced event identity or lost notification fails the
profile. The child has a post-terminal audit so the stored terminal anchor must
remain the original failed event rather than the latest child journal head.

After recovery, an incomplete parent terminal charge must be rejected. The
parent can then include the exact child usage and reach Failed without changing
the child's settlement. This verifies operational completion after the
ambiguous commit, as well as static atomicity.

## Instrumentation boundary

The test-only proxy accepts one plaintext PostgreSQL protocol 3.0 session and
rejects non-loopback targets. Its closed transaction-target enum arms only on
the exact `INSERT INTO stateknot.child_run_settlements ` Parse prefix and cuts
the next exact simple-query `COMMIT`. It bounds frames, preserves partial
reads, forwards startup/authentication/bind traffic and never logs protocol
frames.

Unexpected framing, a missing target, early worker exit, incomplete evidence
or a non-loopback database fails closed. The controller kills only its owned
worker, closes the proxy session and waits for the captured PostgreSQL backend
PID to disappear. Unix must observe `SIGKILL`. The profile pins SQLx 0.8.6
transaction framing; a driver exchange or target SQL change requires
requalification. Production TLS and database traffic never cross this proxy.

## Reproduce and retain evidence

Use a disposable loopback PostgreSQL 16 or 17 database:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::child_settlement_commit_loss::child_settlement_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

Both mandatory PostgreSQL CI jobs require exactly two
`STATEKNOT_CHILD_SETTLEMENT_COMMIT_LOSS_EVIDENCE` records and a successful
exit. They retain
`child-settlement-commit-loss-postgres-<version>-<run-id>` for 30 days with the
qualification log, source/tree identities, lockfile digest, toolchain,
PostgreSQL image, kernel and host inventory. Database URLs, credentials, Agent
inputs and outputs are never evidence fields.

## Gates still open

This is a client-process fault profile, not a PostgreSQL server/WAL fault test.
Parent terminal finalization and Join-result consumption still lack equivalent
in-flight COMMIT cuts. Unknown provider effects/prices, failover, PITR/restore,
untrusted-worker SQL isolation, same-run nested namespaces, retained-history
capacity, latency and soak remain unqualified. RFC-0004 therefore stays Draft;
the complete durable-child profile is not advertised as production ready.
