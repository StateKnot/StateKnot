<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# StateKnot qualification scenarios

These scenarios turn the v1 product claims into repeatable release evidence.
They define application behavior, workload, failures, security boundaries, and
objective acceptance criteria. They are not benchmark marketing claims for
arbitrary hardware or third-party model providers.

## Scenarios

| ID | Scenario | Primary risk covered |
|---|---|---|
| `GS-001` | [Internal tool agent](001-internal-tool-agent.md) | typed agent ergonomics, tool safety, budgets, and noisy-neighbor isolation |
| `GS-002` | [Long-running approval and recovery](002-long-running-approval.md) | interrupts, long suspension, crash recovery, fencing, migration, and retention |
| `GS-003` | [Cross-organization A2A collaboration](003-cross-organization-a2a.md) | protocol interoperability, delegated identity, untrusted networks, streaming, and reliable push |

## Reference release environment

Release-qualification results MUST record exact CPU model, memory, storage,
kernel, container runtime, database configuration, StateKnot commit, dependency
lockfile, and test dataset. The initial comparison profile is:

- three application nodes, each with 8 vCPU and 16 GiB RAM;
- three API replicas, three scheduler replicas, and at least six worker
  processes distributed across the application nodes;
- PostgreSQL 16 or 17 with an 8 vCPU, 32 GiB RAM primary, NVMe-class storage,
  synchronous commit enabled, and a synchronously replicated failover standby
  for zero-loss acknowledged-write recovery tests;
- an S3-compatible artifact service reachable over TLS;
- no more than 2 ms median application-to-database network round-trip time;
- deterministic fake model, tool, MCP, and A2A peers for load tests;
- controlled live-provider and cross-SDK runs for compatibility smoke tests,
  never for scheduler throughput scoring.

Changing the comparison profile does not invalidate a result, but results from
different profiles MUST NOT be combined without normalization and an explicit
explanation.

## Measurement rules

1. Each performance run uses a 10-minute warm-up, a 30-minute measured steady
   period, and a 5-minute drain period unless a scenario specifies longer.
2. Latency percentiles use client-observed monotonic time. Framework overhead is
   reported separately from model, remote tool, and remote-agent latency.
3. Acknowledged durable writes use PostgreSQL synchronous commit. RPO claims
   apply only to acknowledged records.
4. Success-rate calculations include framework, policy, storage, and transport
   failures but report intentionally injected dependency failures separately.
5. Slow consumers, cancelled requests, retries, duplicate deliveries, and
   rejected policy decisions remain in the event and metric evidence.
6. Every failure test identifies the exact persistence boundary and confirms
   state, event, invocation, and outbox invariants after recovery.
7. Content logging is disabled by default. Test evidence MUST prove that tokens,
   secrets, approval credentials, and sensitive payloads do not appear in
   ordinary logs, traces, metrics, crash reports, or checkpoints.

## Shared service objectives

At less than 70% measured saturation on the reference environment:

| Signal | Release threshold |
|---|---:|
| Accepted request to committed `RunQueued` event | p95 <= 150 ms, p99 <= 300 ms |
| Committed runnable work to valid worker claim | p95 <= 500 ms, p99 <= 2 s |
| Committed event to connected SSE subscriber | p95 <= 250 ms, p99 <= 1 s |
| Event stream reconnect from a valid last event ID | p95 <= 1 s |
| Unexpected framework error rate | < 0.1% |
| Lost acknowledged events/checkpoints | 0 |
| Stale worker writes accepted | 0 |
| Cross-tenant data disclosures | 0 |

Under the noisy-neighbor load defined by each scenario, an in-quota tenant's
p95 runnable queue delay MUST remain below twice its uncontended p95 and no
eligible tenant may remain continuously runnable but unclaimed for more than
five seconds.

## Test cadence

- **Every pull request:** deterministic unit, property, state-machine, schema,
  migration, and reduced fault tests using PostgreSQL containers.
- **Nightly:** the full failure matrix, cross-SDK protocol tests, sanitizer or
  fuzz targets, and a reduced performance profile.
- **Release candidate:** the complete reference load, 24-hour soak, database
  failover, backup restoration, N-1/N-2 migration, and security regression set.

All evidence is stored as versioned machine-readable output plus a concise
human-readable report. Expected failures may be tracked during development but
no required scenario may remain baselined when support is claimed.
