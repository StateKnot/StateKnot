<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Failure-close COMMIT loss and fencing qualification

[中文](commit-loss-qualification.zh-CN.md)

`failure-close-commit-loss-v1` extends the
[committed-boundary profile](process-kill-qualification.md) with two precisely
instrumented source-registration transactions. It tests a client process lost
while its COMMIT call has not returned, not a database server crash. Production
API, migrations and public wire contracts are unchanged. RFC-0004 remains Draft.

## Exact fault boundaries

| Cut | Evidence while the original process is alive | Recovery after real process termination |
|---|---|---|
| `commit_not_forwarded` | All source-registration statements finished, but the proxy holds the complete frontend COMMIT without forwarding any of it. Independent nonlocking MVCC reads still see the old journal, lease, checkpoint/account and empty close/cancellation queues. | SIGKILL the client and disconnect only its proxy session. Wait for that backend session to disappear; assert the entire original snapshot and absence of close evidence. A higher lease epoch can register the decision once. |
| `commit_response_withheld` | COMMIT was forwarded. The proxy received `CommandComplete(COMMIT)` and idle `ReadyForQuery` but forwarded neither. A separate store connection verifies the committed original failure, direct usage and exact source event. | SIGKILL the still-awaiting client. A fresh process using the old now-released fence and a different candidate failure/zero usage must return the original `Existing` record, not register again. |

For both cuts, the fresh recovery process is itself killed after returning its
verified recovery result. Independent maintenance then drains the child and
commits the original Failed outcome. Exactly one source event, settlement event
and final-close event exist; full journal chains validate, final usage is exactly
7 direct + 11 child input tokens, and final retries do not advance the journal.

The not-forwarded case additionally starts an **actual retained old worker**:
it loads and holds its original fence, announces readiness, then waits on a
one-shot parent command. The controller observes expiry using PostgreSQL's clock
(20-second fixture lease, no SQL timestamp rewrite), obtains a strictly higher
epoch with a different attempt ID, then resumes that same process. The old worker
reads the latest journal head but must receive **`StaleFence`**, not a journal
conflict or an already-closed-run error. Its attempted write changes no durable
snapshot. Only then does another process register through the valid new fence.

This expiry check does not claim every stale-write API, an interrupted graph node,
unknown provider effect, or the final 10,000-trial stale-race release gate.

## Instrumentation and safety

`tests/postgres/commit_proxy.rs` is an in-process test controller, not an exported
proxy or server feature. It accepts exactly one connection and targets only a
literal loopback address or `localhost`; remote/socket targets are rejected
before fixture migration/preparation. The worker has an explicit one-connection
pool and connects to the already migrated database. There are no trigger hooks,
SQL failure injections, transaction-result mocks, server kills or backend-cancel
requests in these tests.

The proxy pins SQLx 0.8.6's existing exchange: the failure-close INSERT Parse arms
the fault, and the next exact simple Query `COMMIT` reaches it. Startup must be
plaintext protocol 3.0. Message length is checked before allocation (64 KiB
startup, 4 MiB framed message); each direction keeps its partial read state.
Unsupported exchanges, truncated/oversized frames, early exit, missing cut and
missing response fail rather than count as evidence. The frame/tag interpretation
follows PostgreSQL's [message formats](https://www.postgresql.org/docs/17/protocol-message-formats.html)
and [transaction message flow](https://www.postgresql.org/docs/17/protocol-flow.html).
This intentionally narrow profile must be requalified if the pinned driver or
transaction exchange changes; it is not a general TLS/pipelining/notification
proxy. Production TLS behavior is unaffected.

Startup, authentication and bind frames are forwarded but **never logged**.
Only the backend PID is retained from BackendKeyData, never its cancellation key.
The proxy task is aborted/joined after the owned client is killed, closing its
sockets. A bounded read-only poll confirms that exact backend has disconnected;
no unrelated session or container is stopped. Parent-loss pipe cleanup still
exits an orphan with code 24, which is not accepted as successful SIGKILL evidence.
The retained-worker resume path has a separate smoke test and observes libtest's
successful exit only after its stale-fence assertion executes.

Operational bounds are 90 seconds to reach a cut/recover or finish a resumed
worker, 30 seconds to observe backend disconnect, and 45 seconds to observe
fixture lease expiry. These are harness deadlines, **not service RTO/SLOs**.

## Reproduce and retain

Use a disposable PostgreSQL 16 or 17 database accessible only through loopback,
provide `STATEKNOT_TEST_DATABASE_URL` through the environment, and run:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::failure_close::commit_loss::failure_close_commit_loss_and_fence_takeover_are_recoverable \
  --nocapture --test-threads=1
```

Both mandatory PostgreSQL CI jobs run this test in the integration suite and an
explicit evidence invocation. Each existing
`failure-close-process-postgres-<version>-<run-id>` artifact now also contains
`failure-close-commit-loss.log` beside the original six-point profile and shared
source/tree/lockfile/toolchain/kernel/CPU/memory/image metadata, retained for 30
days. The new log must contain **two ordered** `STATEKNOT_COMMIT_LOSS_EVIDENCE`
JSON records, one for each cut, and a successful test/job exit. CI rejects an
empty test filter. In the response-withheld row, `expired_fence_rejection_verified`
is false because that separate check belongs to the not-forwarded case; it is
not a skipped expectation. Helper worker entry points are not independent tests
of real-database recovery during ordinary discovery.

## Gates still open

These are source failure-close transactions with fully priced direct evidence
and deterministic fixture child cleanup. They do not recover real provider
effects/prices, interrupt the database server inside COMMIT/WAL flush, qualify
database failover/PITR/restore, or cover every admission/Join/deadline/settlement/
finalization transaction. SQL role isolation, combined runtime process-loss
matrices and measured retained-history capacity/fairness/latency remain required
before full durable-child production enablement. Do not infer a general
exactly-once external-effect guarantee from once-only durable accounting.
