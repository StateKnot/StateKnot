<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Agent HTTP v1

`stateknot::agent_http::AgentHttpService` is an opt-in JSON router over
[`AgentServiceV1`](agent-service.md). It admits work durably; it does not execute
Agents in the request task. A separately deployed scheduler/Worker advances Runs.
This is an implemented pre-alpha profile, not a stable API or a complete managed
Agent hosting product. [RFC-0008](rfcs/0008-agent-http-v1.md) tracks the boundary.

## Bind the host, not caller-supplied identity

```rust
use std::sync::Arc;
use stateknot::agent_http::{AgentHttpOptions, AgentHttpService};

// `service`: fully validated AgentServiceV1 and exact executable registries.
// `verifier`: your AgentHttpAuthenticator backed by trusted identity policy.
let options = AgentHttpOptions::new(["agents.example.com".to_owned()])?;
let http = AgentHttpService::new(service, Arc::new(verifier), options);
let router = http.router();
// Mount only behind your configured TLS ingress. Start the owned listener here.
```

There is no default authenticator and no anonymous mode. Implement
`AgentHttpAuthenticator::authenticate` to validate issuer, audience, expiry,
revocation and signature/introspection evidence, then produce
`AgentHttpPrincipal::new(trusted_caller, allowed_operations)`. Constructing this
value does **not** verify a token. `Submit`, `Read` and `Cancel` are independent
permissions. Never derive tenant or principal from request JSON, unverified JWT
claims, arbitrary forwarding headers, or a submission key.

The underlying `AgentServiceAuthorizer` still evaluates each exact Agent/request,
Run or tenant-scoped key digest before resource lookup. Credential permission is
not a substitute for resource authorization. A trusted host must also configure
deployment/schema/budget policy; no client can widen its budget.

## Four operations

All requests use `Authorization: Bearer <credential>`. POST requires
`Content-Type: application/json` (optional `charset=utf-8`). Accept must be absent,
`application/json`, or `*/*`. Compressed bodies, cookies as authentication, CORS
preflight, query parameters and percent-encoded paths are not supported by these
JSON operations. [Activity SSE](agent-events.md) is a separate opt-in GET profile
with its own Accept requirement, cursor contract and connection limits.

| Method and path | Closed JSON request | Success |
| --- | --- | --- |
| `POST /v1/agent-runs` | `AgentHttpSubmission { submission_key, agent, request }` | 201 new; 200 recovered |
| `GET /v1/agent-runs/{run_id}` | No body | 200 verified snapshot |
| `POST /v1/agent-runs/lookup` | `AgentHttpLookup { submission_key }` | 200 verified snapshot |
| `POST /v1/agent-runs/{run_id}/cancellation` | `AgentCancellationIds { event_id, failure_id }` | 202 recorded; 200 recovered |

`agent` is the complete versioned `CapabilityIdentity`; `request` is the existing
schema-bound `AgentRequest`, not free-form chat messages. Serialize these Rust
types directly; their `schemars::JsonSchema` implementations describe the wire
contract. Unknown fields and duplicate keys, including nested keys, are rejected.
Run, event and failure IDs retain the core UUIDv7 contract.

Every success is `AgentHttpRunResponse { request_id, snapshot }`. The snapshot
excludes private checkpoints, transcript, input and policy payloads; terminal
output/usage/failure comes only from verified durable evidence. `Active` does not
mean a Worker is currently executing and can include a failure-closing Run.
`CancellationRequested` is not terminal cancellation; do not claim cleanup until
a verified terminal outcome exists. `request_id` is a fresh correlation ID for
this HTTP response, **not** an idempotency key or durable receipt.

## Retry without repeating work

Persist a high-entropy `AgentSubmissionKey` and the exact logical request **before**
sending the first submission. A retry with the same key/content resolves the
original Run; changed content is 409. Keep keys out of URLs, access logs and
telemetry. Persist both `AgentCancellationIds` before cancelling and reuse the
entire pair on retry. A different pair is a competing cancellation, not recovery.

Timeout, connection loss, shutdown, or a response-size failure does not prove a
transaction rolled back. Read by original key/Run or retry exact identities with
bounded exponential backoff and jitter. A recovered response may show a newer
lifecycle revision; it is not required to be byte-identical. Never generate a new
key merely because no response arrived. For 409, inspect durable state and fix a
logical conflict; an optimistic concurrency conflict can be retried with the same
identities after a fresh read. Do not automatically replay business side effects.

## Limits and errors

Defaults are 256 KiB request bytes, 2 MiB response bytes, 64 in-flight requests
per shared service instance, and a 15-second cooperative total deadline.
`with_limits` permits 1 byte–2 MiB request, 1 KiB–8 MiB response, and 1–1,024
in-flight requests. `with_deadline` permits 10 ms–60 s. JSON additionally obeys
core depth/container/node/string limits. Response serialization stops at the
configured byte ceiling; errors have small fixed envelopes. Streaming/chunked
bodies are byte-limited too. A failed body read is rejected as 413 in this profile.

The permit covers credential verification, body reads, resource policy, database
operations and response construction. Async timeouts cannot preempt blocking
host code: authenticator/authorizer implementations must be cancellation-safe,
nonblocking, and independently bound remote dependencies. Limits do not replace
HTTP header/connection/TLS limits, per-tenant quotas or replica-global admission
controls at the host ingress. The cap bounds wire buffers, not the entire DB pool
or runtime memory footprint.

| Status | Meaning |
| --- | --- |
| 400 | Invalid envelope, duplicate field/header, unsupported path shape |
| 401 | Missing/invalid credential; `WWW-Authenticate: Bearer` |
| 403 | Host/Origin, operation permission or resource policy denied |
| 404 | Authorized target not found |
| 405 / 406 / 415 | Wrong method / Accept / content representation |
| 409 | Immutable identity, admission budget or Run-state conflict |
| 413 | Request byte ceiling or body-read failure |
| 429 | Per-instance concurrency full; `Retry-After: 1` |
| 503 | Verification/storage unavailable, deadline or shutdown; `Retry-After: 1` |
| 500 | Host integrity/configuration failure or response byte ceiling |

Errors only contain `error.code` (`agent_http.*`) and `error.request_id`. They
never forward SQL, token, provider or policy diagnostics. Every response has
`Cache-Control: private, no-store`, `Pragma: no-cache`, `X-Content-Type-Options:
nosniff`, `stateknot-api-version: 1` and `x-request-id`. Overload and malformed
Host/header envelopes can reject before authentication; no resource lookup occurs.

## Deployment and shutdown checklist

1. Pin this source revision, lockfile, migrations, executable graph and schemas.
   The router adds no SQL migration and uses the existing admission/control events.
2. Bind a private listener behind TLS with an exact `Host` allowlist (including
   explicit ports). Reject unknown hosts at the edge too. Forwarded headers do
   not alter this policy. Browser `Origin` is rejected by default; optional exact
   HTTPS origins do not enable CORS or cookie authentication.
3. Configure a real credential verifier and resource authorizer. Exercise expired,
   revoked, wrong-audience, wrong-tenant and inaccessible Run credentials before
   rollout. Do not deploy the test suite's static credential verifier.
4. Run separate authenticated Worker/scheduler roles, finite DB/auth dependency
   deadlines, connection/header caps and per-tenant/replica quotas. Readiness must
   cover compatible database schema and registry/policy dependencies. Track only
   bounded error codes, latency, 429/503 counts and opaque request IDs; redact
   Authorization, keys, inputs, outputs and raw paths from logs/traces.
5. On drain, stop accepting connections and call `http.shutdown()`. Bound listener
   graceful shutdown separately. In-flight requests can have committed already;
   recover by original identities after replacement. Never compensate blindly.
6. Retain existing database records on binary rollback. Resume outstanding work
   only with compatible registry/runtime capabilities. RPO is that of the host's
   acknowledged PostgreSQL durability/backup configuration, not an HTTP promise;
   RTO depends on credential/DB recovery and Worker availability.

CI runs real HTTP + PostgreSQL 16/17 admission/cancellation response-loss recovery,
24 concurrent identical submissions, authorization-before-storage, hostile JSON,
slow body/auth timeout, overload, response limits and shutdown tests. The bounded
JSON profile does not include list/search APIs, bundled OIDC, live-provider
acceptance or a stable/public-crate release claim. The separately enabled
[activity SSE profile](agent-events.md) adds qualified cursor recovery and finite
stream lifecycle; it does not change the framework's release status.
