<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Owned Agent maintenance

The experimental `agent_maintenance` module owns four existing jobs separately
from HTTP ingress and the execution Worker. Both `stateknot` and
`stateknot-runtime` export it. This is not a stable release or complete multi-role
production qualification. See [RFC-0014](rfcs/0014-owned-agent-maintenance.md) and
the [Chinese guide](agent-maintenance.zh-CN.md).

## Bind actual dependencies

```rust,no_run
use std::sync::Arc;
use stateknot::{agent_maintenance::*, core::TenantId, postgres::PostgresStore,
    runtime::{JsonSchemaRegistryBuilder,
        register_standard_agent_deadline_event_schema,
        register_standard_child_reconciliation_event_schema,
        register_standard_child_join_event_schema,
        register_standard_run_failure_close_event_schema}};

async fn start(store: PostgresStore, authorized_tenants: Vec<TenantId>,
    dependencies: Arc<dyn AgentMaintenanceReadiness>)
    -> Result<AgentMaintenance, Box<dyn std::error::Error>> {
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    register_standard_agent_deadline_event_schema(&mut schemas)?;
    register_standard_child_reconciliation_event_schema(&mut schemas)?;
    register_standard_child_join_event_schema(&mut schemas)?;
    register_standard_run_failure_close_event_schema(&mut schemas)?;
    let binding = AgentMaintenanceBinding::new(store, schemas.build()?,
        authorized_tenants, AgentMaintenanceMutationOptions::default())?;
    Ok(AgentMaintenance::start(binding, dependencies,
        AgentMaintenanceOptions::default()).await?)
}
```

Authorize 1–128 distinct tenants before binding. No wildcard discovery or implicit
grants exist. Every binding owns all four concrete jobs, private service handles,
the same actual store and frozen exact schemas. Startup checks the actual store
schema and mandatory host dependency check before spawning tasks or mutating work.
Readiness callbacks must be read-only, asynchronous and cancellation-cooperative.
The application qualifies real credentials, tenant configuration, dependencies
and [restricted SQL roles](postgresql-roles.md); a constant success callback is
not production qualification. This remains trusted server-side SQL code, not an
untrusted tenant-isolated Worker sandbox.

## Finite quanta and recovery

| Job | Existing service | Per-tick bound |
| --- | --- | --- |
| Deadline | `DurableAgentDeadlineReconciler` | 16 due admissions |
| Child | `DurableChildReconciler` | 16 cancellation + 16 settlement candidates |
| Join | `DurableChildJoinPublisher` | 16 eligible parents |
| FailureClose | `DurableRunFailureCloser` | 16 close candidates |

One tick at a time rotates through sorted tenants and these four jobs. Each pair
receives a turn within 4N admitted quanta absent shutdown/readiness loss. This is
local fairness, not global weighted reservations or a wall-clock SLA. Replicas
rely on existing durable serialization and idempotency, not an in-memory queue.
Existing independently configured mutation retries remain bounded.

Each tenant/job retains its own continuation, including after item errors. Failed
items are counted, later pages run, and exhausted scans reset to revisit failures.
Discovery errors fail-stop; a new instance starts without cursors and re-reads
durable state. There is no persisted high-water mark that skips earlier failures.
Deadline cancellation never invents cleanup evidence: execution Workers still
finish the appropriate lifecycle.

## Timing, health and shutdown

Defaults: 250 ms ordinary pacing, 1 s after item errors, 30 s absolute tick
deadline and 10 s graceful drain. Readiness runs single-flight 10 s after the last
check completes, with a 5 s whole-check deadline and 20 s freshness. Validated
`with_pacing`, `with_deadlines` and `with_readiness_limits` adapt these finite
bounds. More tenants increase sweep lag; measure real workload capacity.

`role.health()` caches Ready/Unavailable/Draining/Stopped, `active_ticks()` and
sanitized `report()`. Ready means fresh dependencies, **not zero item failures**.
`report.job(AgentMaintenanceJob::Deadline)` and the other jobs count completed
ticks, candidate observations and failures; counters saturate, include repeated
observations and reset on restart. They are not durable billing or usage.
No credentials, SQL, tenant/Run IDs or payloads are exposed. Protect any host
health endpoint; none is installed automatically. Alert on increasing item
errors, stale readiness and fail-stop status; diagnose individual records using
existing protected low-level APIs. Measure sweep lag and recovery capacity.

Readiness loss pauses new ticks while admitted work may finish. Discovery failure,
task panic or tick deadline records the first closed failure category and drains.
Panic hooks remain host-owned and must not leak private details.
`role.begin_shutdown()` closes admission synchronously. `role.shutdown().await`
joins the monitor and sweep task, aborting and joining remaining work at the drain
deadline. Cancelling `wait(&mut self)` retains coordinator ownership. Drop only
initiates cleanup, not synchronous completion: Stopped may precede destruction of
an aborted tick, so check active ticks or prefer joined shutdown. No shared pool
is closed.

Forced cancellation never proves transaction rollback, lease release, external
effect rollback or exactly-once execution. Ambiguous commits recover from existing
durable facts. Non-yielding native code requires an outer OS hard-kill deadline.
Process shutdown does not create durable user Run cancellation.

## Deployment boundaries and evidence

Operate ingress, execution and maintenance independently. Stop ingress first,
then drain execution and maintenance under application policy while preserving
recovery capacity on surviving instances. No automatic retention deletion,
timer/interrupt business policy, migration, signal handler, credential distribution,
public health endpoint or model/tool execution is added. Other services explicitly
own those duties. Rollback restarts the prior application against the same records;
no schema downgrade is needed.

Required PostgreSQL 16/17 tests exercise every job's durable effects, two replicas,
excluded tenants, a complete failed page with reachable tail, restart revisits,
real SIGKILL/fresh ownership, preserved cancellation/failure identity, readiness
loss/recovery, panic/store fail-stop and a lock-blocked tick deadline. Unit tests
cover forced abort/join and abort-before-first-poll. CI requires four nonempty
evidence markers and archives logs with source tree, lockfile hash and pinned
database image. Existing transaction/process/role suites remain required. These
are correctness qualifications, not measured production throughput or RTO.
The execution Worker suite also runs both owned roles against the same store,
joins maintenance, then verifies a new Run still completes through the Worker.
