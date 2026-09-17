<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Protected read-only host operations

[中文](agent-operations.zh-CN.md) · [RFC-0016 (Draft)](rfcs/0016-protected-agent-host-operations.md)

`stateknot::agent_host::operations` provides an independently owned loopback
HTTP/1 listener over the **actual** `AgentHostHealth`. It does not depend on
business readiness, execute work, probe the store or expose an administrative
write. StateKnot remains pre-alpha; this is not a production release claim.

## Wire three independent authorization requirements

1. Supply a real `AgentHttpAuthenticator` verifying issuer, audience, lifetime,
   revocation and credentials. No default verifier or fixture token is installed.
2. Explicitly grant `AgentHttpOperation::InspectHost`. Business Submit/Read/Cancel
   permissions do not include it. With `AgentHttpIntrospection`, opt in using
   `with_host_inspection_scope("stateknot:host:inspect".into())`: the scope must be
   distinct from all business scopes, present in the token, and intersect an
   explicit `TenantBinding` grant. Default introspection options cannot grant it.
3. Install `AgentHostOperationsPolicy` with exact `AgentServiceCaller` entries
   (tenant + issuer + subject), separately from business resource authorization.
   This explicitly permits process-wide status visibility, not access to Run data.

```rust
use std::{sync::Arc, time::Duration};
use stateknot::{agent_host::{AgentHost, operations::*},
    agent_http::AgentHttpAuthenticator, runtime::AgentServiceCaller};
use tokio::net::TcpListener;

async fn observe(
    host: &AgentHost,
    operators: Vec<AgentServiceCaller>,
    verifier: Arc<dyn AgentHttpAuthenticator>,
) -> Result<(AgentHostOperations, Arc<AgentHostOperationsPolicy>), Box<dyn std::error::Error>> {
    let policy = Arc::new(AgentHostOperationsPolicy::new(
        host.health(), operators, Duration::from_secs(300))?);
    let server = AgentHostOperations::start(
        TcpListener::bind("127.0.0.1:8081").await?, policy.clone(), verifier,
        AgentHostOperationsOptions::new(["ops.example.com".into()])?)?;
    Ok((server, policy))
}
```

The trusted control plane supplies up to 128 distinct callers and a nonzero
monotonic lease at most one hour. Empty denies everyone. `generation()` is only a
CAS token: `replace(expected_generation, callers, lease)` validates and atomically
replaces the policy, including revocations. Do not renew an old cache without
revalidating its trusted source. Duplicates, stale CAS, invalid limits, overflow
and poison fail closed. Expired policy returns 503. Policy is rechecked after
verification and body validation; already authorized responses are not undone.

## HTTP contract

All routes require the same credentials, permission and fresh operator policy.
There is no anonymous `/readyz`/`livez`, CORS exception, cookie authentication or
forwarded-identity authority. Configure TLS at a qualified co-located reverse
proxy, preserve exact allowed Host and restrict the operations network. Do not
publish the backend over an unprotected remote network.

| Exact GET route | Authorized result |
| --- | --- |
| `/v1/host/status` | 200, including Starting/Unavailable/Draining/Stopped |
| `/v1/host/live` | 200 unless host Stopped; otherwise 503 |
| `/v1/host/ready` | 200 only while host Ready; otherwise 503 |

These 503 probes still return the status snapshot, not an authentication error.
Snapshot keys: `schema_version: 1`, `request_id` and `host`, whose fixed fields
are `status`, `live`, `ready`, `failure`, `http`, `worker`, `maintenance`.
Host failure is null or closed `{phase: "startup"|"role", role: "http"|"worker"|"maintenance"}`.
Role fields are null until started. HTTP exposes status and active connections/
streams; Worker exposes status, active ticks/nodes, tick/quanta/failure counters
and its closed failure; maintenance exposes status, active ticks, counters and
the four fixed deadline/child/join/failure_close job reports. Cumulative `u64`
counters are decimal **strings**, not lossy JSON numbers. Active/forced counts
are bounded integers. Counters reset on process restart and are not durable usage.

No token, caller, tenant, Run ID, endpoint, SQL, callback error or provider payload
is serialized. Error responses reuse the sanitized `agent_http.*` envelope and
`stateknot-agent` Bearer challenge: 401 invalid identity, 403 missing grant/ACL,
503 unavailable identity/policy, 429 request saturation. Parser-level failures
may lack a JSON envelope. No content encoding, nonempty bodies, transfer encoding,
query or percent-encoded route is accepted. Duplicate credential/Host/Accept
headers are refused. Only absent Accept, `application/json` or `*/*` is allowed.
HEAD and all writes return 405 after authorization; there is no configuration,
restart, cancellation or retention route. Responses are private/no-store.

## Bounds and shutdown

Defaults: 16 connections, 16 requests, 5 s whole-request deadline, 3 s headers,
60 s absolute connection lifetime, 5 s drain. `with_request_limits` accepts
1–256 requests and 10 ms–10 s. `with_transport_limits` uses the
[existing HTTP bounds](agent-http-server.md#topology-and-bounds); 64 headers,
32 KiB parser buffer and 16 KiB output ceiling are fixed. Transport saturation
closes excess sockets. Authentication panics/timeouts return sanitized 503.
Application panic hooks must also avoid logging secrets. Callbacks must yield;
an OS supervisor provides a separate hard-kill deadline for blocking code.

Keep the operations owner separate from `AgentHost`. Stop and join the business
host first, inspect its stopped snapshot/report if needed, then call
`operations.shutdown().await`. It closes admission/listener, gracefully drains,
then aborts and joins at the configured deadline. `wait(&mut self)` can be
cancelled and resumed without losing ownership. Inspect `AgentHttpDrainReport`
for forced connections, failures, rejections and listener failure; Ok is not
proof that every connection finished normally. `Drop` aborts but cannot await
cleanup. Stopping operations never stops business roles or closes their pools.

Health fields are independently synchronized, not an atomic distributed snapshot.
Liveness is local lifecycle, not executor responsiveness. Ready may change just
after a response and does not mean zero failed jobs. Treat identity/policy 503
differently from an authenticated non-ready snapshot; avoid restart storms when
an identity provider is unavailable. There is no per-read dependency probe except
credential verification, and no automatic authorization lease renewal.

## Qualification and remaining gates

Mandatory PostgreSQL 16/17 suites exercise actual host lifecycle and durable
execution, permission/tenant separation, expiry/revocation, hostile inputs,
auth concurrency/deadlines/panics, transport bounds and joined/Drop cleanup.
Pinned real TLS Keycloak adds a dedicated ops-only principal, opt-in scope,
business denial, separate ACL, secret rotation, identity revocation and joining.
CI stores `STATEKNOT_OPERATIONS_*_EVIDENCE` with exact tree/lock/image metadata.
Build first, then run database qualification serially to avoid test interference.

No migration or dependency change. Adding the pre-alpha `InspectHost` enum variant
requires downstream exhaustive matches to be updated. This operational JSON is
not a durable record encoding. Existing three-scope constructors are unchanged.
Production proxy topology, real host credential provisioning, capacity and
recovery SLOs, cross-process rollout, observability export and stable release
acceptance remain separate. `stknot.com` receives static documentation only.
