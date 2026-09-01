<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Cross-tenant durable fair scheduling

`DurableFairScheduler` layers replica-safe weighted tenant selection over the
existing tenant-isolated scheduler worker. It is pre-alpha and unpublished.
The implementation provides an exact reservation-count starvation bound; it
does not promise wall-clock latency or successful work for an idle or contended
tenant queue.

A [Simplified Chinese edition](cross-tenant-fair-scheduler.zh-CN.md) is
maintained alongside this document.

## Why the order is durable

An in-memory round-robin cursor diverges when scheduler replicas restart or
scale horizontally. StateKnot instead compiles one deterministic smooth
weighted-round-robin cycle and binds its canonical bytes to an explicit
`SchedulerShardId` in PostgreSQL.

Before a replica scans any tenant queue, it atomically reserves the next global
slot. The reservation contains:

- a stable UUIDv7 `SchedulerReservationId`;
- the immutable shard and policy digest;
- a monotonic shard-global sequence;
- the selected cycle slot; and
- the authoritative database reservation time.

The PostgreSQL cursor lock exists only for this short reservation transaction.
Queue discovery, lease claiming, Graph execution, lifecycle commits, and
external work happen after it closes.

## Compile an immutable weighted policy

```rust,ignore
let tenant_a = TenantId::try_from("tenant-a")?;
let tenant_b = TenantId::try_from("tenant-b")?;

let policy = WeightedFairnessPolicy::new(
    SchedulerShardId::try_from("primary-v1")?,
    [
        TenantFairnessWeight::new(tenant_a.clone(), 3)?,
        TenantFairnessWeight::new(tenant_b.clone(), 1)?,
    ],
)?;

assert_eq!(policy.cycle_length(), 4);
let bound = policy
    .starvation_bound(&tenant_b)
    .expect("tenant belongs to the immutable policy");
assert!(bound.maximum_reservations_until_selection() <= 4);
```

Construction sorts tenants by exact identifier, rejects duplicates, builds one
complete deterministic cycle, verifies that each tenant appears exactly its
configured weight, and derives the largest circular gap between each tenant's
selections.

Hard bounds prevent configuration from becoming unbounded runtime work:

- at most 1,024 tenant queues per shard;
- individual weights from 1 through 1,024; and
- at most 4,096 slots in one complete cycle.

The configured ratio is exact over every full cycle of reservations. It is a
share of scheduling opportunities, not a share of tokens, money, CPU time, or
completed runs.

## Register before claiming work

```rust,ignore
let scheduler = DurableFairScheduler::register(
    store,
    executable_registry,
    lifecycle_evidence_provider,
    DurableGraphDriverOptions::default(),
    DurableGraphLifecycleOptions::default(),
    DurableTenantSchedulerOptions::default(),
    policy,
    DurableFairSchedulerOptions::default(),
)
.await?;
```

Registration constructs the existing tenant-scoped worker and idempotently
persists the exact policy. The same shard identity may only be reused for
byte-identical policy content. A weight, tenant, ordering, or algorithm change
must publish a new shard identity, for example `primary-v2`, then deliberately
drain the old scheduler deployment before activating the new one.

Do not run replicas for two different shard identities against the same logical
worker pool during a rollout: each shard owns an independent cursor, so doing
so intentionally creates two independent schedules.

## Execute one global scheduling quantum

```rust,ignore
let tick = scheduler.tick(shutdown.clone()).await?;

record_selection(
    tick.reservation().sequence(),
    tick.tenant_id(),
    tick.starvation_bound(),
    tick.reservation_retries(),
    tick.tenant_tick(),
);
```

Each call allocates one reservation identity, retries only that same identity
after transient database errors, maps the durable slot through the immutable
local cycle, and executes one bounded `DurableTenantScheduler` tick for the
selected tenant.

A reservation is consumed even when the selected tenant queue is empty or a
candidate loses lease contention. This preserves global order and prevents
busy tenants from stealing another tenant's share. Callers should keep ticking
while capacity is available rather than scanning another tenant inside the
same reservation.

Shutdown before the next selection is implemented by ceasing to call `tick`.
Once a reservation is durable, the selected tenant tick returns its normal
closed cancellation/no-work/work outcome; the slot is never rolled back.

## Exact starvation boundary

For a tenant that remains continuously eligible, `TenantStarvationBound`
returns the maximum number of global slot reservations between that tenant's
selections, including the selected slot. The value is derived from the actual
circular cycle, not estimated from the weight ratio.

This becomes a wall-clock service objective only when the deployment also
bounds:

- time between scheduler `tick` calls;
- database reservation latency and retry count;
- the selected tenant's page scan and claim duration;
- each tenant scheduling quantum; and
- available replica/worker capacity.

Queue contention can still prevent a selected tenant from claiming a run.
Therefore report both selection lag and successful-claim/service lag.

## Lost acknowledgement and retention

PostgreSQL migration 14 stores immutable reservation rows. A retry with the
same reservation ID returns the original sequence and slot without advancing
the shard cursor twice. Replica concurrency is serialized only at the cursor;
unique reservations form one contiguous global sequence.

Old reservation evidence can be pruned in short, cooperative batches:

```rust,ignore
let policy = SchedulerFairnessRetentionPolicy::new(
    Duration::from_secs(24 * 60 * 60),
    1_000,
)?;
let report = store
    .prune_scheduler_fairness_reservations(policy)
    .await?;
```

The database clock determines the exclusive cutoff. Candidates use the
retention index and `FOR UPDATE SKIP LOCKED`; concurrent maintenance workers do
not alter policy rows or cursor positions. The supported retention window is
one hour through 366 days, with at most 10,000 rows per transaction.

A deployment must never retry a reservation identity after its configured
retention window. Once the evidence is deleted, the same UUID cannot recover
its original slot and would conflict with the runtime's lost-ack guarantee.
Choose retention longer than every scheduler retry, incident replay, and audit
window, then monitor the oldest retained row and deletion backlog.

## Operational metrics

Record at least:

- reservations and database retries by shard;
- selected slots and tenants by global sequence;
- configured weight, observed selections per complete cycle, and exact
  starvation bound per tenant;
- tenant tick outcomes, pages/candidates scanned, contention skips, claim
  retries, and execution outcomes;
- reservation-to-tick and selection-to-successful-claim latency; and
- retention cutoff, rows deleted, oldest retained reservation, and backlog.

Alert on immutable policy conflicts, projection mismatches, sequence
exhaustion, a continuously eligible tenant exceeding its reservation-count
bound, or retention approaching a live retry/audit horizon.

## Qualification evidence and remaining blockers

Property tests cover arbitrary bounded tenant order and weight sets, exact
cycle counts, order independence, and every circular starvation bound. Real
PostgreSQL tests prove immutable registration, lost-ack recovery, same-ID and
unique-ID concurrency, contiguous ordering, bounded database-time retention,
cursor neutrality, migration verification, and a 3:1 two-tenant share across
four distributed scheduler ticks.

This does not yet complete production scheduling. Role-separated credentials,
multi-replica soak and kill testing, admission/rate policy, capacity-aware
sharding, telemetry, failover/restore evidence, stale-race qualification, and
operator rollout tooling remain required before v1 support.
