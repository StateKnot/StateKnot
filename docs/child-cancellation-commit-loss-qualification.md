<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Child-cancellation delivery COMMIT-loss qualification

[中文](child-cancellation-commit-loss-qualification.zh-CN.md)

`child-cancellation-delivery-commit-loss-v1` qualifies the client-side
ambiguous-COMMIT boundary of the production
`PostgresStore::deliver_child_cancellation` transaction. The exercised child is
durably Waiting on a real timer. One transaction must request cancellation,
append the exact control-plane event, abandon the complete wait set, clear the
wait projection, make cleanup schedulable, store an immutable delivery receipt
and consume the parent's cancellation queue item.

This profile adds no production fault hook and uses no SQL trigger, transaction
mock or database-server crash. Every cell owns a fresh tenant, OS process,
single PostgreSQL connection and transaction. Recovery is reconstructed from
PostgreSQL through fresh processes and independent reader connections.

## Required matrix

| Cut | Required result |
|---|---|
| `commit_not_forwarded` | All delivery statements, including the receipt insert, have reached the server, but the proxy holds the frontend COMMIT. Independent readers must still see the exact Waiting Run, outstanding wait, empty receipt and pending queue item. After `SIGKILL` and backend disconnect, the full snapshot must remain unchanged; a fresh process commits the complete delivery once. |
| `commit_response_withheld` | COMMIT reaches PostgreSQL, while the proxy consumes `CommandComplete(COMMIT)` and idle `ReadyForQuery` without forwarding them. Independent readers must see the complete cancellation and wait-abandonment set. A fresh process supplies new event and failure candidates, but `Idempotent` recovery must return the original receipt identities without changing any durable field. |

The compared snapshot covers parent and child lifecycle, journal head, lease,
scheduler, wait-set and checkpoint projections; ownership and settlement;
cumulative budget; the cancellation source and receipt; pending cancellation
and settlement queues; the complete child journal; and the timer-abandonment
fact. A partial cancellation, orphaned wait, duplicate event or replacement
receipt fails the profile.

After recovery a fresh worker must claim the cancellation-requested child, its
terminal confirmation must become exactly one child settlement, and the parent
must then reach confirmed cancellation. This proves that ambiguous delivery does
not merely look consistent at rest but remains operationally drainable.

## Instrumentation boundary

The test-only proxy accepts one plaintext PostgreSQL protocol 3.0 session and
rejects any target other than a literal loopback address or `localhost`. Its
closed transaction-target enum arms only on the exact
`INSERT INTO stateknot.child_run_cancellation_receipts ` Parse prefix and cuts
the next exact simple-query `COMMIT`. It bounds frames, preserves partial reads,
forwards startup/authentication/bind traffic and never logs protocol frames.

Unexpected framing, a missing target, early worker exit, incomplete evidence or
a non-loopback database fails closed. The controller kills only its owned test
process, closes that proxy session and waits for the captured PostgreSQL backend
PID to disappear. Unix must observe `SIGKILL`. This profile pins SQLx 0.8.6
transaction framing; any driver exchange or target SQL change requires
requalification. Production TLS and database traffic never cross the proxy.

## Reproduce and retain evidence

Use a disposable loopback PostgreSQL 16 or 17 database:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::child_cancellation_commit_loss::child_cancellation_delivery_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

Both mandatory PostgreSQL CI jobs require exactly two
`STATEKNOT_CHILD_CANCELLATION_COMMIT_LOSS_EVIDENCE` records plus a successful
exit. They retain
`child-cancellation-commit-loss-postgres-<version>-<run-id>` for 30 days with the
qualification log, source/tree identities, lockfile digest, toolchain,
PostgreSQL image, kernel and host inventory. Database URLs, credentials, Agent
inputs and outputs are never evidence fields.

## Gates still open

This is a client-process fault profile, not a PostgreSQL server/WAL fault test.
The separate [child-settlement profile](child-settlement-commit-loss-qualification.md)
now cuts accounting settlement. The separate
[parent-finalization profile](parent-finalization-commit-loss-qualification.md)
now cuts terminal closure. The separate
[Join-result consumption profile](child-join-consumption-commit-loss-qualification.md)
now covers its in-flight COMMIT cuts. Unknown provider
effects/prices, failover,
PITR/restore, untrusted-worker SQL isolation, same-run nested namespaces,
retained-history capacity, latency and soak also remain unqualified. RFC-0004
therefore remains Draft and the complete durable-child profile is not advertised
as production ready.
