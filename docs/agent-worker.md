<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Owned durable scheduling Worker

Status: implementation-backed pre-alpha; [RFC-0013](rfcs/0013-owned-agent-worker.md)
remains Draft. This is a concrete execution role, not a stable production release
or a remote arbitrary-code Worker API. [中文](agent-worker.zh-CN.md).

## Assemble the actual execution role

`stateknot::agent_worker` wraps the existing tenant/fair PostgreSQL scheduler.
HTTP admission persists work; it never starts this role implicitly. Construct
`AgentWorkerBinding::tenant` for an explicit tenant or `::fair(...).await` for a
registered `WeightedFairnessPolicy`. Bindings exclusively own a fresh scheduler
and cannot be cloned or expose tick producers. Fair construction registers the
immutable policy, but reserves no slots and claims no runs. Bound its startup
with a host deadline; retry ambiguous registration with the identical policy.

```rust,no_run
use std::sync::Arc;
use stateknot::{agent_worker::*, core::TenantId, postgres::PostgresStore,
    runtime::{ExecutableGraphRegistry, GraphLifecycleEvidenceProvider}};

async fn execution_role(
    store: PostgresStore,
    registry: ExecutableGraphRegistry,
    tenant: TenantId,
    evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
    dependencies: Arc<dyn AgentWorkerReadiness>,
) -> Result<AgentWorker, Box<dyn std::error::Error>> {
    let binding = AgentWorkerBinding::tenant(store, registry, evidence, tenant,
        AgentWorkerExecutionOptions::default())?;
    Ok(AgentWorker::start(binding, dependencies,
        AgentWorkerOptions::default()).await?)
}
```

These arguments are real application dependencies, not generated defaults.
Install the Driver, lifecycle and cancellation schemas, all graph/input/output
schemas, reducers and executors before freezing the registry. Child graphs need
their existing additional schemas and reconcilers. Use the qualified
[restricted runtime database role](postgresql-roles.md). The evidence provider
must recover actual admission, cumulative usage and artifacts from trusted
durable facts; zero usage is valid only for genuinely zero-cost work.

## Bounds and readiness

| Setting | Default | Accepted range |
|---|---|---|
| Concurrent scheduler slots | 4 | 1–64 |
| Absolute tick deadline | 5 min | 100 ms–1 h |
| Graceful drain | 30 s | 10 ms–5 min |
| Delay after executed / idle / run failure | 10 / 250 / 1000 ms | Each 10 ms–60 s |
| Delay between completed readiness probes | 10 s | 50 ms–60 s |
| Whole readiness deadline | 5 s | 10 ms–30 s |
| Cached evidence freshness | 20 s | At least probe delay + deadline; at most 120 s |

One slot executes at most one durable quantum; Graph node parallelism remains a
separate driver bound. There is no local unbounded queue or adaptive concurrency.
Tune the tick deadline above your maximum legitimate quantum, including database
and lifecycle accounting time; a timeout stops the whole role rather than silently
retrying potentially effectful work.

Startup and each single-flight periodic check verify the actual database schema,
nonempty frozen registry and exact persisted fair policy, then call mandatory
`AgentWorkerReadiness` for the actual provider/evidence/authorization/maintenance
dependencies. Failed or stale evidence pauses admission of new ticks. Already
admitted work may finish. A later successful check resumes dispatch; a panicking
check fail-stops the role. Readiness is not a transaction that guarantees future
dependency availability, and schema verification does not replace role audits.

## Shutdown and recovery

1. `begin_shutdown()` closes tick admission and signals cooperative cancellation.
   It does not send durable user cancellation or close shared database pools.
2. `shutdown().await` (or a cancellation-safe `wait(&mut self)`) joins slots and
   probes. After the graceful deadline, remaining futures are aborted and joined.
3. The role then waits for destruction of tracked nested Graph node futures.
   A successful return has zero owned ticks/nodes, not merely a stopped outer task.
4. Restart with the same retained graph/policy bindings. Uncertain database work
   uses existing leases, fence epochs, journals and reconciliation. It is not
   promised rolled back, immediately reclaimable or externally exactly once.

Drop initiates cancellation/abort but cannot synchronously join. Its health may
report `Stopped` while active node/tick counts are still draining. All callbacks
must yield and release resources when dropped. Non-yielding native code cannot be
forcibly interrupted by Tokio; an OS supervisor needs an outer process deadline.
Executors remain responsible for tasks they spawn outside the provided runtime.

`GraphExecutionActivity::wait_for_idle` is also available on low-level driver,
loop and scheduler handles. It does not stop dispatch or cancel tasks: first stop
and join **every** producer, including clones. The owned role enforces this order.

## Diagnostics and operational ownership

`health()` supplies cached `Ready/Unavailable/Draining/Stopped`, active tick/node
counts and saturating report counters. It retains no pool, registry or request
payload. Expose it only through a host-protected administrative surface. Reports
are local process counters, not durable billing, access logs or Run status.
`executed_quanta` includes waiting/deferred/cancelled and maintenance handoffs;
it must not be interpreted as successful Runs.

Run-local `ExecutionFailed` increments a counter and uses finite backoff; the
existing Agent Loop attempts exact-fence cleanup. Scheduler failure, tick timeout
or an unexpected task panic initiates fail-stop drain with a closed category.
Do not blindly restart in a tight loop: investigate schema/configuration drift,
provider availability, quarantine and pending reconciliation before resuming.
The host also owns panic-hook/log redaction; catching a callback panic does not
suppress the process's configured panic hook. Never include secrets in panics.

Hosts separately operate timer, deadline, child, failure-close and retention
maintenance. This role does not install those jobs, OS signal handlers, migrations,
credentials or HTTP endpoints. Shutdown order, least privilege, resource limits,
telemetry/alerts, restore tests and application-specific load/failover qualification
remain required for a real deployment.

## Executable evidence

```console
STATEKNOT_REQUIRE_POSTGRES_TESTS=1 \
STATEKNOT_TEST_DATABASE_URL=postgres://... \
cargo test -p stateknot --test agent_worker --locked -- --nocapture --test-threads=1
```

Use a dedicated test database, never production. PostgreSQL 16/17 CI retains
`agent-worker-postgres-*` artifacts with source tree, lock digest and image pins.
Tests cover execution, readiness outage/recovery, concurrency, wait cancellation,
graceful/forced drain, Drop, callback panic, deadline, failed-run backoff, a fresh
OS process completing retained terminal work without repeating the node, and
cross-process continuity of durable weighted-fair slots. The ignored child test
is invoked and required by parent tests; absence of a database fails required CI.

The website deployment publishes only these guides, not a Worker, test identity
or public Agent API. Complete identity/policy-backed host qualification remains
separate from this slice and from stable release acceptance.
