<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0015: Owned co-located Agent host

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-09-16
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/63
- Supersedes: None
- Superseded by: None

## Summary and motivation

Supervise the actual owned HTTP ingress, execution Worker and maintenance role as
one co-located lifecycle. Independent roles already work, but applications still
must coordinate startup failures, dependency admission, sibling exit and ordered
shutdown. This is a concrete three-role owner, not a generic task orchestrator.
Public APIs remain experimental; this does not claim a stable production release.

## User-facing design

`AgentHostBindings::new` consumes the actual HTTP service, Worker binding and
maintenance binding. `AgentHostDependencies` requires each role's existing host
readiness implementation. `AgentHostOptions` contains their validated options.
`AgentHost::launch` takes these and a loopback listener and returns an owned handle
immediately, before asynchronous startup. `wait_ready` can be cancelled without
losing ownership. `begin_shutdown` closes admission synchronously; `shutdown` and
`wait(&mut self)` join cancellation-safely. Health contains actual role views and
closed status/counters only, without retaining pools or executable registries.

Each role verifies its own real bound dependencies. The trusted application must
qualify compatible database, tenant and registry configurations; a supervisor
cannot infer logical database equivalence from independently configured pools.
There are no default identity, resource-policy or dependency-success callbacks.

## Detailed semantics

Start maintenance, then Worker, then ingress. Ingress admission stays closed
until all roles have started. Cached Worker and maintenance freshness is checked
on every hosted HTTP request, not only at the next ingress readiness probe.
Worker tick admission additionally checks maintenance freshness. Observed loss
pauses new admission but does not revoke work already admitted; this is not a
distributed instantaneous health transaction. Existing bounded probes determine
when a previously unobserved dependency outage becomes visible.

Existing durable queue work may execute while later roles are starting. Startup
failure or panic stops and joins already-started roles. Cancellation of a
readiness wait retains the handle; shutdown during startup cancels the current
startup future and joins existing roles. No automatic restart or rollback is
attempted. Construct new bindings/services for a failed-start retry.

Unexpected exit of any role closes ingress and initiates sibling drain. Normal
shutdown drains ingress first, Worker second, maintenance last, preserving
maintenance availability during execution cleanup. Reuse each role's validated
deadlines, forced cancellation and descendant join barriers. The combined bound
is the sum of configured role drain bounds plus cooperative task destruction;
non-yielding callbacks still require an outer OS kill deadline. Cancelled waits
retain the coordinator. Drop initiates/aborts cleanup but cannot synchronously
join it. A guard created before spawning also handles abort-before-first-poll.

## Persistence, security and compatibility

No dependency, schema, migration, lease, protocol or durable policy changes.
Process shutdown is not durable user cancellation and interrupted COMMIT does not
prove rollback. Existing retained state, fencing and idempotency govern recovery.
No model/tool work occurs inline on ingress. Existing standalone role APIs remain
available. A separately served router cloned from the input HTTP service is not
covered by the host's listener admission gate and must not bypass this owner.

Listener remains loopback behind qualified TLS termination. Health is an internal
Rust view for the application's protected operations boundary, not an anonymous
HTTP route or authorization grant. No credential distribution, signal handlers,
retention, timer/interrupt policy, distributed supervisor or production deployment
is installed. Panic hooks and log redaction remain application responsibilities.

## Validation and rollout

Required PostgreSQL 16/17 tests cover authenticated HTTP submission to actual
execution, independent dependency loss/recovery and per-request fail-closed gates,
startup failure/cancellation, wait cancellation, Drop, sibling exit, forced drain
and zero owned activity after joined shutdown. Real TLS Keycloak and resource
policy qualification must exercise the composed host as well as standalone ingress.
Retain exact-source CI markers, bilingual guidance and rollback instructions.
Only the documentation site is deployed in this slice.

## Alternatives and remaining acceptance

Manual ownership remains possible but leaves the above integration unqualified.
A generic callback supervisor cannot establish concrete role admission or joined
cleanup. A central broker would duplicate durable coordination. Authenticated
operational HTTP surfaces, cross-process host rollout and measured workload
capacity/recovery SLOs remain separate acceptance work, not implicit guarantees.
