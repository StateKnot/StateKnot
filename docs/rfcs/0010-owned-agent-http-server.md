<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0010: Owned Agent HTTP ingress lifecycle

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-09-16
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/52
- Supersedes: None
- Superseded by: None

## Summary

Implement an opt-in, ingress-only HTTP/1.1 server that owns its listener and
connection tasks, checks real dependencies before serving, gates business
requests on fresh readiness evidence, and performs bounded graceful drain.
This draft records an experimental pre-alpha profile, not release acceptance.

## Motivation

RFC-0008/0009 supply an authenticated router and bounded request/stream work,
but an embedding host still owns connection admission, readiness and drain.
Merely timing out Axum's server future does not prove that detached connection
tasks were terminated. Ownership of every connection is required.

## Goals and non-goals

Own listener, readiness monitor and connection tasks; fail closed on unavailable
dependencies; bound headers, connections and connection lifetime; join or abort
all owned tasks during drain. Do not run migrations, schedulers, Workers, model
or Tool dispatch, install default identity/policy, expose public health routes,
implement TLS, or claim distributed health consensus or stable API support.

## User-facing design

`AgentHttpServer::start(listener, http, host_readiness, options).await` consumes
one loopback TCP listener and claims one shared `AgentHttpService` exactly once.
The host must provide a real `AgentHttpReadiness` implementation covering its
credential verifier and authorization-policy dependencies. There is no always-
ready production default. Startup failure/cancellation closes the claimed
service and listener; construct a new service for another attempt.

`health()` returns a cloneable, read-only local status view, not a public router.
The host maps this to its protected administrative surface or orchestrator.
`begin_shutdown()` synchronously makes readiness false and signals drain;
`shutdown(&mut self).await` requests drain and joins the owned runtime.
`wait(&mut self).await` observes completion without requesting shutdown.
Dropping either wait future retains the handle; dropping the handle aborts the
runtime and its owned tasks, without pretending that it awaited a clean drain.

## Detailed semantics

Startup checks the actual service's nonempty frozen Agent registry against its
executable graph registry, including descriptor drift and exact graph identity,
and verifies the PostgreSQL schema with read-only queries. It does not call
Agent initial-state factories, authorization decisions or executors. The same
bounded checks and host readiness run periodically, single-flight, with no
per-health-read database work. Failed or timed-out checks make requests return
the existing sanitized 503. Freshness expires independently of probe progress.
Readiness never authorizes a request and never reveals tenant/run existence.
Requests already admitted continue under existing authentication, authorization,
body, deadline and SSE limits. Existing standalone routers remain host-managed.

The listener must be loopback-only; public TLS termination and Host preservation
belong to a co-located reverse proxy. Hyper HTTP/1 bounds are explicit: at most
64 headers and 32 KiB parser buffer, configured header-read timeout, maximum
connection lifetime and maximum concurrent connection tasks. Excess accepted
sockets close without an application response. Keep-alive is bounded by the
absolute connection lifetime. HTTP/2, upgrades and forwarded identity are not
enabled. The service owns neither the Tokio runtime nor the shared DB pool.

Drain ordering: readiness false, stop/drop listener, stop periodic probes,
request graceful shutdown on every connection, allow admitted operations to
finish until the configured deadline, then cancel the ingress service, abort
remaining connection tasks and join them, then wait for tracked SSE producer
cleanup. SSE may finish voluntarily or be
force-closed at the deadline. Completion reports forced connection count and
connection errors; this is not proof of transaction rollback. Callers recover
ambiguous mutations with original submission/cancellation IDs and SSE cursors.
Host callbacks must remain nonblocking and cancellation-cooperative; Rust task
abort cannot preempt blocking code. The process supervisor must retain an outer
hard kill deadline. Readiness loss does not cancel already admitted work.

## Persistence and migration

No new tables, schema version or cursor format. Run records outlive HTTP
processes. Startup verifies schema, never migrates it and never closes a shared
pool. Deployment ACL qualification remains a separate prerequisite.

## Security and privacy

No unauthenticated business operation, identity fallback, secret-bearing health
detail or network health route. Status is Ready, Unavailable, Draining or
Stopped; errors are closed categories. Reverse proxy rate limits, TLS, trusted
loopback placement, process isolation and replica-wide quotas remain required.
Resource limits bound in-process transport state, not arbitrary host callback
allocations or external proxy/kernel buffers.

## Observability and operations

Hosts export the local status, active-connection count and final drain report
through protected telemetry. Alert on stale/unavailable readiness, repeated
connection failures or force-close counts. SIGTERM handling belongs to the host:
signal drain, await its result, then close host-owned resources. No hidden OS
signal handler or automatic restart is installed. Listener accept failure fails
closed and enters the same drain path. Preserve caller retry identities.

## Compatibility

Additive pre-alpha Rust APIs, Rust 1.88, unchanged HTTP v1 JSON/SSE wire contracts.
Use already-locked Hyper/Hyper-util through explicit dependency edges; no new
transport stack. Existing router-only embedding remains available.

## Alternatives considered

Leaving lifecycle entirely to every application is valid for existing routers
but duplicates a correctness-sensitive ownership boundary. Timeout around
`axum::serve` alone cannot account for all connection tasks. A generic supervisor
for all service roles is deferred until those roles have qualified contracts.

## Validation and rollout

Mandatory PostgreSQL 16/17 tests use real sockets for startup failure, readiness
loss/recovery and fail-closed admission, successful graceful work completion,
slow/idle/SSE force-close, header/lifetime/connection ceilings and handle drop.
Keep existing cross-process SSE and lost-response qualification. Run strict
format/Clippy/Rustdoc, workspace tests, dependency policy, bilingual website
browser/accessibility checks and live page verification. Deploy documentation
only; never deploy test tokens or a fixture Agent service publicly.

## Unresolved questions

Stable release acceptance, independent design/operations review, protected host
admin wiring and a real identity/policy pilot remain separate release gates.
