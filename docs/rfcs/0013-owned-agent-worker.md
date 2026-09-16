<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0013: Owned durable scheduling Worker

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-09-16
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/59
- Supersedes: None
- Superseded by: None

## Summary and motivation

Own a concrete tenant or weighted-fair PostgreSQL scheduler as an independent
execution role, separate from HTTP ingress. Existing ticks already select,
claim and execute a durable quantum. This adds bounded operation of those ticks,
not a new message broker or remote Worker protocol. The API remains pre-alpha.

## User-facing design

`agent_worker::AgentWorkerBinding` constructs and exclusively owns a fresh tenant
or fair scheduler using one frozen registry, store and lifecycle evidence
provider. `AgentWorker::start` requires an additional host readiness check for
the actual provider, evidence, authorization and maintenance dependencies.
Configuration bounds slot count, busy/idle/failure pacing, readiness cadence,
freshness, probe deadline, tick deadline and graceful drain deadline.

Startup verifies the actual database schema, nonempty frozen registry and, for
fair scheduling, persisted policy identity. No claims occur before readiness.
One periodic probe runs at a time. Fresh readiness gates every new tick; loss
pauses new work without pretending to revoke already-running work. Fair policy
registration may happen while constructing the binding but does not claim runs.

## Execution and failure semantics

A fixed number of owned slots each runs at most one tick. Every iteration has a
positive bounded delay; there is no local unbounded queue. Durable run-local
execution failures count separately and back off. Scheduler infrastructure
failure, task panic or tick deadline fail-stops the role and initiates drain.
Errors and local health expose only closed categories and counters, not payloads,
credentials, identifiers or database diagnostics. Health is not a public route.

Shutdown closes dispatch immediately, signals cooperative driver cancellation,
and waits for owned tasks. At the drain deadline remaining slot/probe tasks are
aborted and joined. Runtime node-task tracking supplies a completion barrier
after all tick producers are joined, including nodes aborted by a dropped parent
future. `wait(&mut self)` is cancellation-safe. Drop requests cancellation and
aborts owned work but cannot synchronously guarantee completion.

Callbacks must be asynchronous, nonblocking and cancellation-cooperative. Rust
cannot forcibly terminate non-yielding native code; an OS supervisor must enforce
an outer process deadline. Forced termination does not promise database rollback,
lease release, external-effect rollback or exactly-once effects. Existing fencing,
lease expiry, retained journals and reconciliation govern restart recovery.
Process shutdown is not a durable user cancellation of a Run.

## Scope and operations

No implicit migrations, timer/deadline/child/failure-close reconcilers, retention
jobs, signal handlers, credential distribution or PostgreSQL pool closure. Hosts
must operate and qualify these separately; readiness must reflect their actual
dependencies. A durable child join or failure-close handoff remains queued for
its existing reconciler, not dropped or reported as terminal success.

## Compatibility, security and alternatives

Additive Rust 1.88 APIs; no database migration. Reuse the already locked Tokio
task tracker to join nested node futures; no new dependency version. Bindings
are not cloneable and never expose their scheduler, preventing competing external
tick producers from invalidating shutdown completion. Existing low-level manual
schedulers remain available. Task tracking alone is not a cancellation mechanism;
its barrier is valid only after all producers have stopped.

Do not pass untrusted database credentials, executable registries or fairness
policies. Use the existing qualified restricted PostgreSQL role and network
security profile. No anonymous diagnostics or synthetic allow-all dependencies.
A callback-only runner would leave the actual scheduling integration unverified;
a central broker would duplicate the existing durable queue and fencing boundary.

## Executable acceptance and rollout

Tests cover option bounds, freshness, readiness loss/recovery, fixed concurrency,
cooperative and forced shutdown, nested task destruction, cancellation-safe wait,
drop, panic/deadline fail-stop, real execution and restart recovery. PostgreSQL
16/17 qualification must run with required-database flags and machine-readable
evidence, including persisted fair-slot continuity. Retain bilingual operational
guides and CI evidence. Publish only the documentation website, not test Workers,
identities or Agent APIs. Rollback uses the prior application and retained state;
no destructive state migration or automatic durable Run cancellation.

## Unresolved acceptance

Stable API review, full application-specific dependency qualification and
multi-process operational deployment remain outside this experimental profile.
