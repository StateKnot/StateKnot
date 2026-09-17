<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0016: Protected read-only Agent host operations

- Status: Draft
- Authors: StateKnot contributors
- Created: 2026-09-17
- Tracking issue: https://github.com/StateKnot/StateKnot/issues/65
- Supersedes: None
- Superseded by: None

## Summary

Add `agent_host::operations`, a concrete independently owned loopback HTTP/1
listener exposing only the actual `AgentHostHealth`. Every request requires
verified bearer identity, an explicit `InspectHost` grant and a separate,
bounded, expiring operator allowlist. No anonymous health route is installed.
This remains an experimental pre-alpha contract while this RFC is Draft.

## Motivation

RFC-0015 owns role startup, admission and joined shutdown, but its health views
are local Rust values. Operators need sanitized evidence during business
dependency outages and after host shutdown, without exposing Run or tenant
data or making business readiness a prerequisite for diagnosis.

## Goals and non-goals

Provide independently bounded authenticated read-only status, liveness and
readiness; preserve exact caller/tenant binding; support policy expiry,
replacement and revocation; own and join every accepted connection.

No administrative writes, restart/cancel/configuration routes, default verifier,
anonymous probes, provider execution, per-request database probes, OTel exporter,
fleet aggregation, hard real-time guarantees or production capacity/SLO claim.
No Agent runtime or fixture identities are deployed to the public website host.

## User-facing design

The intended API, made compilable in rustdoc with implementation, is:

```rust,ignore
let policy = Arc::new(AgentHostOperationsPolicy::new(
    host.health(), operators, Duration::from_secs(300),
)?);
let options = AgentHostOperationsOptions::new(vec!["ops.example.com".into()])?;
let mut operations = AgentHostOperations::start(listener, policy.clone(), verifier, options)?;
// Stop business roles first; the protected endpoint can still observe Stopped.
let report = host.shutdown().await?;
let transport_report = operations.shutdown().await?;
```

Only `GET /v1/host/status`, `GET /v1/host/live` and `GET /v1/host/ready` exist.
Status returns 200 for authorized callers even during business outages; live
returns 503 only after host Stopped; ready returns 200 only for host Ready.
Both non-ready probes still return the sanitized snapshot. Responses use schema
version 1, a generated request ID, no-store headers and a fixed 16 KiB ceiling.
Other methods return 405 after authentication. Unknown/encoded/query routes are
rejected. No bodies, browser Origin, content encodings or duplicate credentials.

## Detailed semantics

`AgentHttpOperation::InspectHost` is distinct from Submit/Read/Cancel. The
introspection verifier gains an opt-in fourth scope, distinct from all three
business scopes; default options cannot grant InspectHost. Verified scope must
intersect an explicit trusted tenant binding. Operations then checks its own
allowlist against the complete caller (tenant, issuer, subject). A business Read
grant cannot inspect even an allowlisted caller; an ops-only principal cannot
read or mutate Runs.

The policy binds the health view at construction. At most 128 distinct callers
are accepted; empty means deny all. Finite nonzero monotonic leases are at most
one hour. Generation-checked replacement is atomic; stale generations, poison,
overflow, duplicates and invalid limits fail closed. Freshness and membership
are checked after asynchronous authentication and request-body validation.
Successful authorization is a point-in-time decision, not instantaneous
revocation of an already authorized response. No implicit lease renewal occurs.

The listener must be loopback behind separately qualified TLS termination and
an exact Host allowlist. Default limits: 16 connections, 16 concurrent requests,
5 s whole requests, 3 s headers, 60 s absolute connection lifetime and 5 s drain.
Request bounds are 1..256 and 10 ms..10 s. Existing validated HTTP transport
bounds, 64 headers and 32 KiB header buffer are reused. Authentication panics,
timeouts or dependency failures return sanitized 503; invalid credentials 401;
missing grant or membership 403; request saturation 429. Bodies are not read
without authentication. Transport saturation closes excess connections.

Independent ownership means operations can start while host Starting, remain
available while Unavailable/Draining/Stopped, and never gate or mutate business
roles. Shutdown closes admission and listener, gracefully drains to a finite
deadline, then aborts and joins remaining connection futures. Cancelled waits
retain ownership. Drop aborts owned work but cannot synchronously join; counters
track actual destruction. A guard exists before the coordinator is spawned.

## Persistence and migration

No durable schema, record or migration changes. Operator leases and health are
process-local and must be supplied afresh after restart. Removing this listener
leaves all business persistence intact. No migration of existing identity policy
is required unless explicitly enabling the fourth scope and trusted grant.

## Security and privacy

Defense layers are TLS proxy qualification, exact Host/no Origin, bounded bearer
verification, explicit inspection scope and trusted tenant grant, then separate
expiring operator policy. Forwarded headers, cookies and token tenant claims do
not supply authority. No router getter permits bypassing the owner.

Only closed lifecycle/failure labels, fixed job names and bounded numerical
counts leave the process. No caller/tenant/Run IDs, tokens, secrets, endpoints,
SQL, callback errors or provider messages appear. Wide cumulative counters use
decimal strings for lossless JSON consumers. Application panic hooks remain the
host's responsibility and must not log secrets. Permission to inspect process
health is explicitly cross-tenant operational visibility, not Run data access.

## Observability and operations

Snapshot fields describe host lifecycle/failure, optional started role status,
HTTP connection/stream counts, Worker tick/node counts and reports, and
maintenance tick/job counters. Separately synchronized reads are not an atomic
distributed snapshot. Local live does not prove executor responsiveness; ready
may become stale immediately after response. Reads perform no dependency I/O
beyond credential verification. Alerts must distinguish authentication/policy
unavailability from an authenticated non-ready snapshot.

Operators provision real verifier credentials and exact principals, renew policy
before expiry through trusted application control, restrict proxy/network access,
stop business host before operations, inspect final reports and then join the
operations listener. This adds no durable RPO claim and no recovery-time target.
Production throughput, capacity and rolling-upgrade qualification remain separate.

## Compatibility

Rust 1.88 and existing dependencies/Cargo.lock remain unchanged. Adding
InspectHost can require downstream exhaustive enum matches to be updated; this
is an explicit pre-alpha source change. Existing three-scope constructors and
business HTTP routes remain unchanged. The operations JSON schema is separate
from durable records and Agent business HTTP response types.

## Alternatives considered

Local-only health views cannot support remote diagnosis. Mounting routes on
business ingress loses them when its readiness gate closes. A business Read
permission alone conflates tenant data access and host operations. Anonymous
health leaks availability and workload counts. A generic server/metrics framework
is unnecessary: share the existing private bounded connection primitive only.

## Validation and rollout

Require real PostgreSQL 16/17 owned-host tests proving permission separation,
tenant binding, policy CAS/expiry/revocation, lifecycle independence, sanitized
snapshots, request/transport bounds, hostile input, cancellation-safe drain and
Drop cleanup. Extend pinned real TLS identity-provider evidence to prove a
dedicated ops-only principal, scope intersection and separate policy revocation.
Preserve standalone HTTP, Worker, maintenance and host regressions. CI uploads
non-vacuous operations evidence bound to exact tree, lockfile and pinned images.

Pass workspace tests, strict Clippy/rustdoc, doctests, bilingual website build and
browser/accessibility tests. Merge only the exact qualified source after required
technical gates. Deploy static documentation only; verify by server IP with valid
TLS first, then all public routes. Retain atomic website rollback.

## Unresolved questions

No unresolved design choice blocks the experimental implementation. Independent
review and production deployment/capacity qualification remain outstanding;
passing tests alone does not accept this RFC or establish a stable release.
