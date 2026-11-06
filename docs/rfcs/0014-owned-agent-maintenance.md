<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0014: Owned Agent maintenance role

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-09-16
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/61
- Supersedes: None
- Superseded by: None

## Summary and motivation

Own the existing deadline, child cancellation/settlement, child Join publication,
failure-close and whole-frame cancellation continuation as a separately managed
role. Hosts otherwise must drive each manually even when no execution Run is
runnable. This adds concrete bounded orchestration, not a replacement state
machine or generic callback runner. The public API remains experimental and
the framework remains pre-alpha.

## User-facing design

`agent_maintenance::AgentMaintenanceBinding::new` consumes a store, frozen schema
registry, explicit list of 1–128 distinct authorized tenants and existing bounded
lifecycle/child mutation policies. It requires all five exact existing offline
schemas, registered with `register_standard_agent_deadline_event_schema`,
`register_standard_child_reconciliation_event_schema`,
`register_standard_child_join_event_schema`,
`register_standard_run_failure_close_event_schema` and
`register_standard_agent_cancellation_event_schema`. It owns private concrete
reconciler/finalizer handles. No work is claimed during construction.
`AgentMaintenance::start` requires a host readiness implementation and validated
options. Startup verifies the actual store schema plus host dependencies before
spawning tasks. Runtime and facade expose the same module.

## Detailed semantics

One loop processes one finite `(tenant, job)` quantum at a time in deterministic
round-robin order. All five jobs are present. Deadline, Join and failure-close
scan at most 16 candidates; child reconciliation scans at most 16 in each of its
two lanes. FrameCancellation scans at most 16 raw immutable closure keys plus one
lookahead. Its tenant-bound opaque cursor fixes one round's upper key; terminal
or inapplicable rows and item errors still advance raw progress, and exhaustion
resets the round. It is an in-memory progress hint, not durable execution authority.
Each tenant/job retains its own continuation across item failures. Existing
exhausted-page reset semantics revisit failed candidates; restart begins with no
cursors. There is no
persisted high-water mark that permanently skips failures. Each pair receives a
turn within 5N admitted quanta, absent readiness loss or shutdown. This is local
fairness, not a global weighted reservation or wall-clock latency guarantee.

Positive normal/error pacing prevents empty or failed sweeps from busy-looping.
Item errors increment per-job counters, back off and do not block later pages.
Discovery errors, task panic or the absolute tick deadline initiate fail-stop.
Readiness is single-flight, time-bounded and periodically refreshed; stale or
failed evidence pauses new ticks. Already admitted work may complete. A closed
first-failure category and counters contain no tenant/run IDs, SQL or payloads.
Ready means fresh dependency evidence, not that all maintenance items succeeded.

Shutdown synchronously closes admission, signals cancellation and joins owned
tasks. After the drain deadline it aborts and joins remaining tasks. These jobs
spawn no nested node executors. `wait(&mut self)` is cancellation-safe. Drop
initiates cancellation but cannot guarantee synchronous joined completion.
Non-yielding host callbacks require an outer OS process-kill deadline. Process
shutdown is not durable Run cancellation; cancellation during COMMIT can remain
ambiguous and recovery must inspect existing durable state.

## Persistence, compatibility and security

No new tables, migrations, schemas, dependencies, wire protocols or MSRV changes.
The fifth job reuses the existing standard Agent cancellation schema; public
Rust cursor/page/getters add no serialized identity or schema. Existing row
serialization, fencing and idempotency govern concurrent replicas, restarts and
rollback. Do not claim exactly-once external effects or rollback on
process termination. No model/tool dispatch, wildcard tenant discovery, automatic
retention, timer/interrupt business policy, signal handler or shared pool closure.
Hosts authorize the tenant allowlist, qualify restricted SQL credentials and
network controls, supply actual dependency checks and protect health exposure.
The role is trusted server-side SQL code, not tenant-isolated untrusted execution.

## Operations and alternatives

Expose cached lifecycle status, active tick count, per-job completed quanta/item
counts/failures, readiness failures and first fail-stop cause. Alert on increasing
item failures even when Ready, unavailable/stale readiness and stopped roles.
Use protected low-level diagnostics for individual failed records. Measure sweep
lag/capacity against the deployment's workloads; counters are local, saturating
and reset on restart, not durable usage, billing, RPO/RTO or capacity guarantees.
Manual ticks remain supported but do not provide ownership or joined shutdown.
A configurable arbitrary job scheduler or message broker would duplicate existing
durable coordination without validating these five concrete integrations.

## Executable acceptance and rollout

Validate bounds, tenant uniqueness, exact schema requirements, readiness gating,
staleness, cancellation-safe wait, cooperative/forced shutdown, Drop and panic.
Required PostgreSQL 16/17 tests must exercise actual deadline cancellation, child
cancellation/settlement, Join publication, failure closure and cold whole-frame
cancellation continuation. The fifth job must authenticate full owning proof and
actual terminal ACK, preserve original failure/Root/usage, make bounded raw-page
progress and retain admitted mutation/COMMIT ownership through shutdown.
Continuation past item failures, restart and multiple tenant/role operation remain
required. Retain exact-source CI evidence and bilingual operational guidance.
Existing process-loss, ambiguous
COMMIT and role-privilege suites remain required. Publish the documentation
website only, not maintenance services with test identities or unqualified policy.

## Unresolved acceptance

Stable API review and measured application-specific multi-role production
deployment remain separate qualification work; this RFC does not declare either.
Ordinary scoped Join dispatch stays gated by Draft RFC-0022 and its three existing
Driver guards. The selected cancellation continuation adds no model/Tool/node
execution or new lease authority. This Draft records the current five-job contract;
it does not assert pending execution results or close R1/R6.
