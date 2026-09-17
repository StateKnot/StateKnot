<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Owned Agent HTTP ingress

`AgentHttpServer` owns the listener and connection tasks for the [JSON](agent-http.md)
and [SSE](agent-events.md) profiles. It is ingress-only: no Worker, scheduler,
migration, model execution, OS signal handler or public health route starts
implicitly. [RFC-0010](rfcs/0010-owned-agent-http-server.md) remains Draft;
StateKnot remains pre-alpha, not a complete managed hosting product.

## Wire actual dependencies

```rust
use std::sync::Arc;
use stateknot::agent_http::{
    AgentHttpAuthenticator, AgentHttpOptions, AgentHttpReadiness,
    AgentHttpServer, AgentHttpServerOptions, AgentHttpService,
};
use stateknot::runtime::AgentServiceV1;
use tokio::net::TcpListener;

async fn serve(
    service: AgentServiceV1,
    verifier: Arc<dyn AgentHttpAuthenticator>,
    dependencies: Arc<dyn AgentHttpReadiness>,
) -> Result<AgentHttpServer, Box<dyn std::error::Error>> {
    // Co-located reverse proxy terminates TLS and preserves the configured Host.
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let http = AgentHttpService::new(service, verifier,
        AgentHttpOptions::new(["agents.example.com".to_owned()])?);
    Ok(AgentHttpServer::start(listener, http, dependencies,
        AgentHttpServerOptions::default()).await?)
}
```

Implement `AgentHttpReadiness::check` against the **same** credential-verifier and
policy dependencies used by requests: usable trust roots/key cache and freshness,
issuer/audience configuration, installed policy snapshot or policy-service
health, and bounded dependency timeouts. Do not use a fake tenant/token or an
always-true placeholder. The [introspection profile](agent-identity.md) has an
explicit negative-token protocol canary: it verifies client-authenticated IdP
access, never grants a synthetic principal or replaces resource-policy readiness.
An immutable local policy can check its installed
snapshot without network I/O. Readiness never replaces request authorization.

Before accepting, the runtime checks the actual `AgentServiceV1` pool's schema,
nonempty frozen Agent registry, exact executable bindings and host readiness.
One deadline covers the whole check. It never calls an initial-state factory,
policy decision or executor. Registry closure does not prove application code
is bug-free. Database ACLs still need [role qualification](postgresql-roles.md).

Periodic checks are single-flight. Failed/timed-out checks make new business
requests return the existing sanitized 503; successful checks restore readiness.
Admission and health reads independently enforce freshness, without per-read
database work. Already admitted requests/SSE retain normal authorization and
deadlines. Readiness is evidence, not a transaction or a future-success guarantee.

## Topology and bounds

Only IPv4/IPv6 loopback listeners are accepted. Terminate public TLS at a proxy
in the same trusted network namespace, preserve the exact Host, restrict local
process access, disable SSE buffering/compression and never trust forwarded
identity. A remote proxy over an unprotected backend network is not supported
by this profile. HTTP/2 and upgrades are not enabled.

| Resource | Default | Configurable bound |
| --- | --- | --- |
| Owned connections | 256 | 1–4,096 |
| Request headers | 64; 32 KiB parser buffer | Fixed |
| Header read | 5 s | 10 ms–30 s, no longer than connection lifetime |
| Absolute connection lifetime | 300 s | 50 ms–1 h |
| Graceful drain | 30 s | 10 ms–5 min |
| Interval after each completed probe | 10 s | 50 ms–60 s |
| Whole readiness check | 5 s | 10 ms–30 s |
| Evidence freshness | 20 s | Interval + timeout through 120 s |

Configure `with_transport_limits` and `with_readiness_limits`. JSON/SSE bounds
remain independent; the earliest applicable lifetime wins. Excess accepted
sockets close without an application response. Parser rejections such as 431
do not carry business JSON envelopes. Kernel/proxy buffers, DB pools, arbitrary
callback allocations and replica-global quotas remain host responsibilities.

## Health and ordered shutdown

`server.health()` provides local `status()`, `is_ready()`, `is_live()` and
`active_connections()` and `active_streams()`. States: Ready, Unavailable,
Draining, Stopped. Export
minimal status only through a protected admin/supervisor/telemetry integration.
No unauthenticated `/readyz` or `/livez` exception is added to the Agent API.
Unavailable remains live: a dependency outage should not cause restart storms.
Local liveness does not prove executor responsiveness; external watchdogs need
their own deadline.

On the host's shutdown signal:

1. `server.begin_shutdown()` synchronously marks readiness false.
2. Await `server.shutdown()`: the runtime drops the listener and active probe,
   requests graceful HTTP shutdown, and lets admitted work finish.
3. At the deadline it cancels ingress, aborts outstanding connection tasks and
   joins them, including waiting for SSE producer cleanup. Long-lived SSE may
   require this force-close phase.
4. Inspect `AgentHttpDrainReport`: forced connections, connection failures,
   capacity rejections and listener-failure flag. Release shared resources only
   after separately hosted roles also finish; the server never closes a DB pool.

Cancelling a wait/shutdown future retains the handle for another join. Dropping
the handle aborts work but is **not** awaited graceful drain; active count falls
when aborted connection futures are actually dropped. One shared ingress can be
claimed once. After claimed startup failure/cancellation, construct a new service.
Its clones share closure; separately mounted `http.router()` instances are not
retroactively gated by owned-server health.

Callbacks must be nonblocking and cancellation-cooperative. Task abort cannot
preempt blocking code: the process supervisor needs an outer hard-kill deadline
larger than drain plus cleanup headroom. Accept errors fail closed into the same
drain path; `wait()` reports `listener_failed = true`. No automatic restart or
signal handler is installed.

Forced closure never proves rollback. Preserve submission/cancellation identities
and SSE cursors, and recover on a replacement without redispatching ambiguous
business side effects. Health is not Run completion or an RPO/RTO guarantee.

## Qualification and next gate

Mandatory PostgreSQL 16/17 tests cover empty/missing executable bindings, closed
store failure at startup/runtime, host loss/recovery, probe timeout/single-flight,
startup cancellation, ownership, no inline execution, commit during drain,
cancellation-safe join, slow-body/SSE force-close, idle/drop cleanup, connection/
header ceilings and absolute lifetime. Existing fresh-process SSE and lost-response
tests remain mandatory. Each CI database artifact requires the
`STATEKNOT_AGENT_SERVER_EVIDENCE` marker in addition to prior evidence markers.

These transport tests alone do not qualify proxy load, pool failover or an entire
production deployment. See the separately qualified [identity](agent-identity.md),
[composed host](agent-host.md) and [protected operations](agent-operations.md)
profiles. Broader production hosting/capacity/release qualification remains open.
