<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0008: Authenticated Agent HTTP v1 ingress

- Status: Draft (implemented bounded profile; not stable release acceptance)
- Authors: StateKnot contributors
- Created: 2026-09-14
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/48
- Supersedes: None
- Superseded by: None

## Summary

Expose the existing durable Agent service through four authenticated, bounded
JSON operations. [The implementation guide](../agent-http.md) specifies exact
types, status codes, limits, recovery behavior and deployment requirements.

## Motivation

Applications outside the embedding Rust process need to submit, locate and cancel
Runs without inventing a second admission protocol or executing providers inside
HTTP tasks. The existing tenant-key registry and two-phase cancellation already
provide the durable identities; the missing piece is a qualified transport.

## Goals and non-goals

Provide mandatory verified-credential identity, operation permissions followed by
exact resource authorization, canonical typed requests, bounded input/output and
lost-response recovery. Do not add anonymous mode, inline submit-and-wait,
listing, browser cookie sessions, SSE, OIDC issuer discovery, a new persistence
engine, or production/stability claims for the entire framework.

## User-facing design

`AgentHttpService::new(AgentServiceV1, Arc<dyn AgentHttpAuthenticator>,
AgentHttpOptions)` constructs no background work. `router()` mounts the API;
`shutdown()` rejects/cancels pending work cooperatively. Host applications supply
TLS, verified identities, exact service policy and a separately drained listener.
See the guide for API and wire examples; all public wire types derive JsonSchema.

## Detailed semantics

Validate finite host/origin/header shape, acquire a shared non-waiting permit,
authenticate, parse route, check operation permission, decode bounded JSON, then
invoke the existing authorizer-first service. The actual implementation acquires
the permit before envelope validation so those costs are covered too. No parse
error or authorization failure can dispatch a graph. POST submission is 201/200;
GET and key lookup are 200; cancellation is 202/200. Only a verified snapshot can
claim terminal output. CancellationRequested does not imply terminal cleanup.

Retain logical submission content/key and both cancellation IDs before the first
request. Disconnects, timeouts, serialization ceilings and drain can leave a
committed mutation; exact retry recovers rather than repeats it. Correlation IDs
are fresh, and retries may observe a newer revision. Conflicts never cause hidden
new keys. Concurrent HTTP requests use existing PostgreSQL atomic admission and
optimistic control-plane semantics; no per-server idempotency cache is added.

## Persistence and migration

No new table, record encoding or migration. Existing submission mapping, admission
intent, journal event and cancellation evidence remain authoritative. Pin binary,
graph registry and compatible migrations. Rolling back the HTTP binary does not
delete Runs or revoke admitted work. Resume requires compatible Worker code;
retention stays under the existing store's explicit lifecycle policy.

## Security and privacy

The verifier is mandatory and has no default implementation. Unverified claims,
caller JSON, forwarding headers and opaque keys do not grant tenant identity.
Submit/Read/Cancel are least-privilege transport grants, not resource ACLs. Resource
authorization still precedes deployment/Run/key lookup. Exact Host/HTTPS Origin
allowlists, duplicate-field/header rejection, JSON structural caps, byte ceilings,
finite total deadlines and nonqueued concurrency limit abuse. Error codes contain
no input/credential/policy/SQL messages. Authentication headers are redacted in
the owned credential wrapper; original HTTP stack allocations are not promised
zeroization. Host access logging must suppress sensitive headers/body/paths.

## Observability and operations

Expose correlation headers and fixed error classes for host metrics/traces. Do
not use high-cardinality Run/key/identity labels. Host readiness, rate limiting,
credential revocation, connection/header caps, DB pool, backup/restore and Worker
alerts are deployment obligations, not implicitly configured by a router. Monitor
latency/429/503 and investigate integrity500 before retrying. Stop accepting, signal
shutdown, bound drain, then replace. RPO/RTO depend on acknowledged PostgreSQL and
external verifier/worker recovery; HTTP adds no stronger promise.

## Compatibility

Uses pinned workspace Axum, MIME, Schemars, Tokio utilities and Zeroize. Four
dependency edges move into the facade's normal graph; versions/MSRV remain pinned.
Rust/wire structs remain pre-alpha despite the explicit `/v1` profile identifier.
No claims about SSE or A2A/MCP protocol conformance change.

## Alternatives considered

Leaving HTTP to every user duplicates security/error/recovery logic. An in-memory
request-key cache cannot recover process loss. Dispatching providers while holding
the HTTP request violates the existing durable scheduling boundary. Bundling an
OIDC discovery/cache subsystem expands network trust and key-rotation semantics
beyond this stage; a mandatory verifier extension point keeps those host-owned.

## Validation and rollout

Unit tests freeze wire/configuration/error behavior. Real TCP HTTP tests on both
qualified PostgreSQL majors require one Run across 24 concurrent submissions,
recovery after lost submit/cancel responses, zero inline node calls, policy denial
before a closed database, hostile envelope rejection, finite slow-body/auth
timeouts, overload and shutdown, and recovery after response serialization limits.
CI requires evidence markers and retains source/environment provenance. Existing
workspace/OS/quality/protocol/site gates must still pass before merge. Deploy only
the bilingual documentation to the public website; a production Agent API host
requires its own real verifier, policy, credentials and rollout qualification.

## Unresolved questions

SSE event cursor/replay semantics and full service-role readiness/health/drain
qualification remain future RFC/release work. They are not silently included in
this JSON profile. This draft does not authorize calling the full framework stable.
