<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Child-Join consumption COMMIT-loss qualification

[中文](child-join-consumption-commit-loss-qualification.zh-CN.md)

`child-join-consumption-commit-loss-v1` qualifies the client-side ambiguous-COMMIT
boundary of the production `PostgresStore::succeed_node_attempt` transaction
when a published Join result is consumed. The real child is admitted, closed
and settled; the Join is registered and published; and the parent has a fenced
physical node attempt. That transaction must atomically append one worker
event, persist its exact pending node result and attempt completion, advance
the run journal, and mark the Join consumed. No production fault hook, mock
transaction, or test-only trigger is involved.

## Required matrix

| Cut | Required result |
|---|---|
| `commit_not_forwarded` | The pending-result INSERT reaches PostgreSQL, but the proxy holds the frontend COMMIT. An independent reader sees the unchanged run, Join, attempt, result and complete journal. `SIGKILL` plus backend disconnect rolls back the whole transaction. A fresh process commits once. |
| `commit_response_withheld` | COMMIT reaches PostgreSQL, but the proxy withholds its response. An independent reader sees exactly one new event, a completed attempt, a matching pending result and a consumed Join. A fresh process submits a new candidate event, gets the original committed event idempotently, and changes no durable evidence. |

For both cuts, a recreated registry and `DurableGraphDriver` use the persisted
result to advance a real non-initial checkpoint. The joined node does not
execute again; its successor executes once. The test also checks the published
Join head and base checkpoint remain unchanged through result consumption.

## Instrumentation and scope

The test-only proxy accepts one plaintext PostgreSQL 3.0 session, rejects
non-loopback targets, and arms only on the production pending-result INSERT
Parse prefix. It cuts the following exact COMMIT exchange, never logs protocol
frames, and pins SQLx 0.8.6 transaction framing. A missing target, early
worker exit, incomplete evidence or an unexpected protocol frame fails closed.
The controller kills only its owned worker and waits for that backend PID to
disappear. This is a client-process fault test, not a PostgreSQL server/WAL
fault test.

Run against a disposable loopback PostgreSQL 16 or 17 database:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::join::consumption_commit_loss::child_join_consumption_commit_loss_is_recoverable \
  --nocapture --test-threads=1
```

Both mandatory PostgreSQL CI jobs require exactly two
`STATEKNOT_CHILD_JOIN_CONSUMPTION_COMMIT_LOSS_EVIDENCE` records and a successful
exit. They retain the qualification log and host/source inventory for 30 days.
Evidence never includes database credentials or Agent inputs/outputs.

## Gates still open

Unknown provider effects/prices, failover, PITR/restore, untrusted-worker SQL
isolation, same-run nested namespaces, retained-history capacity, latency and
soak remain unqualified. RFC-0004 remains Draft; this does not declare the
complete durable child-run profile production ready.
