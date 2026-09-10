<!-- Copyright 2026 StateKnot contributors; SPDX-License-Identifier: Apache-2.0 -->

# Failure-close process-kill qualification

[中文](process-kill-qualification.zh-CN.md)

The executable `failure-close-committed-boundaries-v1` profile qualifies one
maintenance path: an Active parent with complete priced direct usage, one owned
child, and a sealed original failure. This is partial evidence for
[GS-002](scenarios/002-long-running-approval.md), **not full durable-child
production enablement**, database failover, or a provider-effect recovery claim.

## Kill-point matrix

Each row runs in a new OS process with fresh PostgreSQL connections. Maintenance
workers construct their offline schema registry and begin their sweep at `None`.
The only recovery input is the immutable child ownership key; no executor,
notification, cursor, failure, price, or accounting snapshot crosses processes.

| Phase, after commit | Required durable observation before and after forced termination |
|---|---|
| `source` | Original failure and direct usage sealed, child cancellation queued, parent lease released; parent still Active but cannot be leased |
| `delivery` | Exactly one cancellation receipt and public-safe child reason; delivery is not terminal confirmation |
| `child_terminal` | Child genuinely terminal in the fixture with exact priced usage; parent remains nonterminal and child reservation not yet settled |
| `settlement` | Once-only child settlement and account projection; parent still nonterminal |
| `close` | Original complete Failure, direct + child usage, and completion timestamp committed together |
| `replay` | Fresh sweeps find no pending work; complete journals, lifecycle, receipt, account and settlement are unchanged |

The child reports readiness only after the relevant public store/runtime call
returns. An independent controller verifies the durable database evidence,
force-kills its **own child process**, checks abnormal exit, reaps it, and reads
the evidence again. Unix requires actual signal 9 (`SIGKILL`); other platforms
use `std::process::Child::kill`. Dropping a future, closing a pool or reconstructing
a coordinator is not accepted as process termination. No production fault hook,
database migration, public API, new dependency or database-server kill is used.

Readiness has a unique per-process token, a 64 KiB total output bound and a
90-second operational timeout. A nonexistent test filter, early normal exit,
malformed message or absent PostgreSQL in subprocess mode fails closed. The
kill/reap path has a 10-second observation deadline; an RAII fallback kills and
waits for the owned process on assertion failure. These are test-harness timeouts,
not recovery SLOs or capacity measurements. The harness never spawns descendants.
An inherited pipe detects controller loss and exits the orphan worker with code
24; a dedicated test checks this cleanup. That exit is not accepted as a
successful kill-point result.

## Run and retain evidence

Use an **isolated, disposable** PostgreSQL 16 or 17 database. Set
`STATEKNOT_TEST_DATABASE_URL` through your environment/secret mechanism, then:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 cargo test -p stateknot-runtime \
  --test postgres --locked -- \
  --exact child_admission::ownership::failure_close::process::failure_close_survives_os_kill_at_each_committed_drain_boundary \
  --nocapture --test-threads=1
```

The test is also part of the existing mandatory real-PostgreSQL integration
suite. The helper `failure_close_process_worker` does no work during ordinary
test discovery; only the parent selects it with private, command-scoped
environment variables. It is not an independently qualified scenario.

Both PostgreSQL CI jobs run an additional evidence-producing invocation and
retain `failure-close-process-postgres-<version>-<run-id>` for 30 days, including
source/tree IDs, lockfile digest, Rust toolchain, kernel, CPU/memory inventory and
the test log. Each successful kill point emits a JSON
`STATEKNOT_PROCESS_KILL_EVIDENCE` record with phase, actual PostgreSQL version,
OS/architecture, termination method and verified outcome. A passing profile
requires all six phase records **and** a successful test/job exit; partial logs
must never be interpreted as a pass. Preserve the artifact with immutable PR
source and merged-tree provenance when retaining release evidence beyond 30 days.
Records contain no credentials, user prompts or private failure messages.

## Deliberately unqualified boundaries

This fixture supplies deterministic direct/child usage (7 + 11 input tokens)
and an already-settled-direct parent. Child terminal confirmation is fixture
evidence, not a live provider cancellation acknowledgement. It does not invoke a
model/tool provider or prove unknown external effects/fees can be recovered.

Kills happen **after a known successful commit**, not while COMMIT is in flight,
before a transaction commits, or between a server commit and its client response.
Existing transaction rollback and logical retry tests are separate evidence;
they do not turn this profile into ambiguous-commit qualification. Remaining
gates include pre-/in-commit OS kills, real provider effects and pricing recovery,
Join/deadline/higher-fence combined process-loss paths, SQL role isolation,
failover/PITR/restore, retained-history capacity, fairness and latency. Do not
remove the RFC Draft status or advertise complete-profile support from this test.
