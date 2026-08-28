<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# GS-002: Long-running approval and recovery

> Status: M0 baseline<br>
> Owner: StateKnot runtime, storage, and server<br>
> Primary execution mode: submitted durable run

## Purpose

A change-management agent prepares a multi-step infrastructure rollout, pauses
for one or more approvals, survives worker and service replacement while
suspended, resumes days later on a compatible graph version, and completes or
rolls back with an auditable outcome.

This scenario proves that pause/resume is durable execution rather than an
in-memory callback and that acknowledged progress is neither lost nor repeated.

## Actors and trust boundaries

- a requester, technical approver, and security approver with distinct scopes;
- StateKnot API, scheduler, workers, and migration tooling;
- a model provider and versioned deployment tools;
- PostgreSQL with failover and point-in-time recovery;
- S3-compatible artifact storage for plans, evidence, and logs.

An approval is authorization for one canonical action, not permission for the
model to alter subsequent parameters. Deployment targets and tool credentials
are resolved after policy approval through opaque credential handles.

## Workflow

1. The requester submits a rollout goal and an immutable target environment.
2. The agent gathers state and produces a versioned plan artifact.
3. The graph pauses at a security approval interrupt.
4. The complete service is rolled or unavailable while the run remains waiting.
5. An authorized principal resolves the interrupt days later.
6. The scheduler claims the run with a new lease/fencing epoch and resumes from
   the committed checkpoint without re-running completed model or tool calls.
7. A canary deployment runs, followed by a second approval or an automatic
   rollback according to deterministic policy.
8. The run commits its terminal status, final artifacts, audit trail, and usage.

## Workload profile

| Dimension | Profile |
|---|---:|
| Tenants | 50 |
| Suspended runs | 100,000 |
| Concurrent active runs | 2,000 |
| New suspensions | 20/s sustained |
| Approval resolutions | 50/s sustained, 150/s for 60 seconds |
| Suspension duration | 1 minute to 30 days |
| Checkpoint state | p95 <= 256 KiB, hard limit 2 MiB |
| Events per run | p95 <= 2,000, hard limit 20,000 |
| Artifact total per run | p95 <= 100 MiB, hard limit 1 GiB |
| Graph steps | p95 <= 50, hard limit 500 |
| End-to-end run deadline | up to 45 days |

The scheduler must handle the suspended population without polling each run.
Wakeups use indexed durable deadlines or outbox-driven signals, and idle memory
must not grow linearly with the number of suspended runs.

## Recovery and lifecycle requirements

- acknowledged journal events and checkpoints have RPO 0 in the synchronous
  replication qualification configuration;
- after database service is restored, runnable work resumes within 60 seconds;
- API and scheduler service-level RTO after a full application restart is five
  minutes or less;
- a lease holder cannot commit after expiry or after a higher fencing epoch is
  issued, even if its network connection later recovers;
- run, event, checkpoint, invocation, interrupt, outbox, and artifact retention
  are tenant-policy driven and coordinated by tombstones before physical GC;
- legal hold blocks destructive retention while still allowing access policy
  changes and cryptographic erasure where configured;
- a run remains pinned to graph and state-schema versions until an explicit,
  audited migration succeeds.

## Failure matrix

| Injection | Required behavior |
|---|---|
| Kill worker before and after every persistence boundary | Recover from the last committed boundary without duplicate committed work |
| Pause the old worker beyond lease expiry | A new worker may claim; every late old-worker write is rejected |
| Terminate all API, worker, and scheduler replicas | Suspended and runnable runs remain recoverable after restart |
| PostgreSQL primary failover | No acknowledged journal record is lost; clients receive retryable errors during interruption |
| Object storage unavailable | Metadata remains consistent; actions requiring missing artifacts do not proceed |
| Duplicate or reordered approval request | Resolve once by interrupt version; reject stale or conflicting resolutions |
| Approval arrives after expiry or cancellation | Reject and leave a complete audit event without reviving the run |
| Deployment changes graph implementation | Old run remains pinned and drains, or uses an explicit compatible migration |
| N-1/N-2 schema migration fails halfway | Migration is atomic or resumable and leaves the prior version operable within documented limits |
| Outbox consumer delivers twice | Receiver deduplicates by stable event/delivery ID |
| Backup is restored into an isolated environment | Integrity hashes, artifact references, tenant boundaries, and runnable state validate before traffic |

## Acceptance criteria

In addition to the [shared objectives](README.md#shared-service-objectives):

- 100,000 suspended runs add no more than 1 GiB aggregate resident memory to
  the scheduler and do not trigger per-run polling;
- accepted approval to committed runnable state is p95 <= 250 ms and p99 <= 1 s;
- accepted approval to valid worker claim is p95 <= 1 s and p99 <= 3 s below
  70% saturation;
- every crash point preserves event sequence continuity and reuses all committed
  model, tool, node, and approval results;
- zero stale fencing writes are accepted in 10,000 forced lease-race trials;
- cancellation wins deterministically over uncommitted work and never changes a
  previously committed external effect into a fictitious cancellation;
- checkpoint and journal compaction preserve the evidence necessary to restore,
  audit, export, delete, and enforce legal hold;
- a point-in-time restore and integrity validation completes within the
  documented recovery window on the reference dataset;
- the 24-hour soak has no unbounded memory, connection, task, event-buffer, or
  storage growth outside the configured retention model.

## Required evidence

- model-based tests for the run, interrupt, lease, invocation, and outbox state
  machines;
- a deterministic kill-point matrix covering every transaction boundary;
- lease-race and network-partition test reports;
- N-1 and N-2 forward migration, rollback-limit, and interrupted-migration tests;
- retention, deletion, legal-hold, backup, restore, and integrity reports;
- a 24-hour resource graph and leak analysis;
- an operator runbook for stuck runs, ambiguous tools, expired approvals,
  failover, restoration, and incompatible graph versions.

## Non-goals

This scenario does not require arbitrary historical forking, interactive time
travel, migration between different durability engines, or automatic replay of
non-deterministic external operations.
