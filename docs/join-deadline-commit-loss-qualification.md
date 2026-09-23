<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Join and deadline COMMIT-loss qualification

[中文](join-deadline-commit-loss-qualification.zh-CN.md)

`join-deadline-commit-loss-v1` closes the client-side ambiguous-COMMIT gap for
three production transactions: child Join registration, child Join publication,
and deadline cancellation of a parent suspended on that Join. It does not use
production fault hooks, SQL triggers, mocked transaction results or a database
server crash.

## Required matrix

Each row owns a fresh tenant, OS process, PostgreSQL connection and transaction.
The controller reads durable state through independent connections; no in-memory
worker state is accepted as recovery evidence.

| Operation | `commit_not_forwarded` | `commit_response_withheld` |
|---|---|---|
| Join registration | All statements finish, but the proxy withholds the complete frontend COMMIT. Killing the client must roll back the Join event, sealed membership and lease release together. A fresh process then commits them once. | COMMIT reaches PostgreSQL, but `CommandComplete(COMMIT)` and idle `ReadyForQuery` do not reach the client. A fresh process must return the original registration as `Idempotent`; membership, event identity and released lease cannot change. |
| Join publication | Terminal child settlement is already durable. An unforwarded COMMIT must expose neither binding, publication event nor scheduler wakeup; cold recovery commits all three together and the parent becomes claimable. | The binding, publication event and scheduler wakeup are visible before the blocked client is killed. A new candidate append must recover the original `Idempotent` publication without advancing the journal. |
| Deadline cancellation | The due parent is suspended on a sealed Join. An unforwarded COMMIT must roll back the audit event, first cancellation reason, lifecycle transition and direct-child cancellation queue together. A fresh process commits the complete set once. | The complete cancellation set is independently visible while the client still waits. Recovery with new event/failure IDs must return `AlreadyRequested`, preserve `agent.deadline.expired`, and create no second queue item or journal event. |

Every final snapshot includes parent/child lifecycle, journals, lease and scheduler
state, child ownership/accounting, physical attempts, Join evidence, cancellation
receipt and pending Join/cancellation/settlement queues. Downstream progress is
also required: recovered registration can settle and publish, recovered
publication makes the parent claimable, and recovered deadline cancellation can
be delivered by the production child reconciler.

## Instrumentation boundary

The test-only proxy accepts exactly one plaintext PostgreSQL protocol 3.0 session
and only a literal loopback target or `localhost`. Its transaction target is one
of five closed enum values; this profile uses the exact Parse prefixes for Join
registration, Join publication and deadline projection verification. It arms on
the target statement and cuts only the next exact simple-query `COMMIT`.

Startup/authentication/bind frames are forwarded but never logged. Frame sizes
are bounded, partial reads keep their state, and an unexpected exchange,
truncated frame, missing target, early worker exit or incomplete matrix fails
closed. The controller force-kills only its owned worker, closes that proxy
session, and waits for the exact PostgreSQL backend PID to disappear. Unix must
observe `SIGKILL`; an ordinary exit is not evidence.

This profile pins the SQLx 0.8.6 transaction exchange and must be requalified if
the driver exchange or target SQL changes. Production TLS and database traffic
do not pass through this proxy.

## Reproduce and retain evidence

Use a disposable loopback PostgreSQL 16 or 17 database:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::transaction_commit_loss::join_and_deadline_commit_loss_are_recoverable \
  --nocapture --test-threads=1
```

Both mandatory PostgreSQL CI jobs require exactly six
`STATEKNOT_JOIN_DEADLINE_COMMIT_LOSS_EVIDENCE` records and a successful exit.
They retain `join-deadline-commit-loss-postgres-<version>-<run-id>` for 30 days
with the qualification log, source/tree identities, lockfile digest, Rust
toolchain, PostgreSQL image, kernel and host inventory. Database URLs,
credentials, Agent inputs and child outputs are not emitted.

## Gates still open

This is a client-process fault profile, not a PostgreSQL server/WAL failure test.
The companion
[child-admission COMMIT-loss profile](child-admission-commit-loss-qualification.md)
covers child admission, and the
[child-cancellation delivery profile](child-cancellation-commit-loss-qualification.md)
covers cancellation propagation plus real Wait cleanup. Settlement, terminal
finalization and Join-result consumption remain uncut while COMMIT is in flight.
It does not
qualify unknown provider effects/prices, failover, PITR/restore, untrusted-worker
SQL isolation, same-run nested namespaces, retained-history capacity, latency or
soak. RFC-0004 therefore remains Draft and the complete durable-child profile is
not advertised as production ready.
