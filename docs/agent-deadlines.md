<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Durable Agent deadline cancellation

[中文](agent-deadlines.zh-CN.md)

The PostgreSQL/runtime profile now supplies database-clock deadline supervision
for **admitted** root and child Runs. It requests cooperative cancellation;
it never claims that time expiry stopped an external operation or established
its final cost. Bare bootstrap Runs without an Agent admission are outside this
profile. [Failure close](failure-close.md) now preserves an earlier failure
decision; full durable-child profile qualification remains a separate gate.

## Host integration

Register `register_standard_agent_deadline_event_schema` in the offline
`JsonSchemaRegistryBuilder`, freeze the registry, and construct
`DurableAgentDeadlineReconciler` with the store, schemas and validated
`DurableGraphLifecycleOptions`. The compiled Rustdoc example on that type shows
the complete finite configuration/tick API.

Schedule `tick(authorized_tenant, cursor, shutdown)` alongside independently
leased execution/cleanup workers, `DurableChildReconciler`, and the optional
`DurableChildJoinPublisher`. This maintenance lane must run even if a tenant has
no runnable execution: Waiting, delayed retry, active leases, and unpublished
child Joins do not exempt a Run from its admitted deadline. No background task
is silently spawned by a constructor.

Each tick examines at most 16 indexed candidates and retains typed per-item
errors while progressing past them. The existing lifecycle retry policy defaults
to three attempts, 25 ms initial exponential delay capped at one second; the
validated hard ceiling is ten attempts. Mutation event/failure IDs stay fixed
within retries. A cancellation committed before a lost acknowledgement is
recovered logically even if a restarted process proposes different IDs.

Retain `AgentDeadlineSweepCursor` between ticks, including after individual
errors. Short pages reset the next sweep automatically. After a crash, start at
`None`; never persist a monotonically advancing deadline watermark that excludes
previously failed work. Both runtime and low-level cursors reject another tenant.
Shutdown is cooperative, preserves the last completed candidate, and may race
an ambiguous database commit; the next sweep converges on durable lifecycle
state. Discovery failures leave the previous cursor usable.

The host owns authentication/tenant authorization, tenant fairness, cadence,
shutdown, error handling and alerts. Inspect every item result. Monitor full
sweep duration, oldest overdue candidate, cancellation-to-settlement lag,
quarantined work, retry exhaustion and unavailable usage/effect evidence. This
API does not promise a hard expiry latency or a capacity/SLA qualification.

## Atomic semantics

Migration 23 copies the finite immutable admission budget deadline to
`runs.agent_deadline_at`. The partial `(tenant_id, agent_deadline_at, run_id)`
index contains pending/active/waiting work and automatically excludes cancelling
or terminal Runs. Quarantined due Runs remain visible as explicit errors instead
of silently disappearing from supervision. Discovery is advisory, not a lease.

`request_agent_deadline_cancellation` locks the Run, verifies canonical admission,
graph/checkpoint/event anchors, current wait evidence and the indexed deadline,
then observes the **database clock after lock acquisition**. Equality is due;
the caller cannot supply a clock or widen the deadline. Fresh quarantined
mutations fail closed. Earlier terminal outcomes and cancellation reasons win
without replacement. Existing cancellation reads do not re-charge usage.

One transaction commits the `agent-deadline-cancellation-requested` audit event,
`CancellationRequested`, all real-wait abandonment records and immediate-child
cancellation witnesses. A failure in any component rolls back the whole request.
The exact offline schema records only operation, admission digest, deadline and
failure ID; no output, credentials or private diagnostics. The public reason is
`agent.deadline.expired`, category `Cancelled`, retry advice `Never`, and is
explicitly distinct from a caller-initiated cancellation.

The Run row serializes this operation with ordinary lifecycle and spawn writers.
It acquires no ancestor/tree lock and does not invoke providers. Existing child
delivery uses its established tree → parent → child order in later transactions.
New spawn is sealed when cancellation wins; a child already admitted is queued.
Delivery never fabricates acknowledgement. Parents cannot reclaim cleanup
capacity or become terminal until every owned child has verified, fully priced
settlement. A successful Join can remain unconsumed in cancelled history.

Execution workers use existing cancellation observation and durable cleanup.
Unknown usage or uncertain external effects remain unresolved. Missing evidence
does not fall back to zero, convert a physical attempt into success, or consume
the successful Join. Parent accounting adds settled child usage exactly once.
The maintenance lane is not a preemptive kill switch: budget/dispatch checks and
provider reconciliation remain required, and an in-flight side effect can finish
after the deadline.

## Upgrade and verification

Migrations 1–22 and existing serialized contracts/audit schemas are unchanged.
Migration 23 transactionally backfills existing admissions without changing their
events, checkpoints or outcomes. An admission INSERT trigger captures deadlines
even for already-connected compatible writers using the unchanged admission SQL.
A Run guard rejects removal/widening of an established deadline. Startup verifies
exact column/check/index/enabled-trigger definitions and function bodies, in
addition to all migration checksums. This is compatibility/integrity protection
for trusted SQL roles, not a security boundary for database administrators.

Plan a maintenance window: this additive migration takes table locks, backfills
admitted Runs and builds a normal index. Test duration on a representative copy,
take a recoverable backup, stop scheduling during the migration, then start
compatible workers and enable the tenant maintenance lane. Do not edit historical
migrations or drop evidence as a rollback procedure. The downgrade scripts in
tests are only for isolated upgrade fixtures.

Run the mandatory real-store suite separately on PostgreSQL 16 and 17:

```sh
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL='<isolated PostgreSQL URL>' \
cargo test -p stateknot-runtime --test postgres --locked deadline_ -- --test-threads=1
```

Coverage includes future versus expired clocks, concurrent/lost-ack requests,
post-lock clock observation, unavailable cleanup evidence, real timer abandonment,
sealed Join cancellation/queue rollback/restart/child-inclusive accounting,
17-candidate paging past failures, cursor tenant scope, shutdown/quarantine,
preservation of first reasons and terminal outcomes, populated v22 upgrade,
index access and altered/disabled guard rejection. These are deterministic
executors and trusted fixture accounting, not live-provider or capacity tests.
