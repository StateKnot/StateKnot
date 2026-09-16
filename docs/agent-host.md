<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Owned co-located Agent host

`stateknot::agent_host::AgentHost` owns the actual authenticated HTTP ingress,
durable scheduling Worker and maintenance role in one process. This is an
experimental pre-alpha API, not a stable release or a complete deployment.
[RFC-0015](rfcs/0015-owned-agent-host.md) remains Draft.

## Bind real dependencies

Build the actual [HTTP service](agent-http-server.md),
[Worker binding](agent-worker.md) and [maintenance binding](agent-maintenance.md).
The HTTP authenticator and resource authorizer remain mandatory. The trusted
application must qualify compatible database, tenant, deployment and executable
registry configurations across the bindings. Separate restricted-role pools may
refer to the same database; the host cannot infer that equivalence.

```rust,no_run
use stateknot::agent_host::*;
use tokio::net::TcpListener;

async fn serve(bindings: AgentHostBindings, dependencies: AgentHostDependencies)
    -> Result<AgentHostReport, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let mut host = AgentHost::launch(listener, bindings, dependencies,
        AgentHostOptions::default())?;
    let ready = tokio::time::timeout(std::time::Duration::from_secs(30),
        host.wait_ready()).await;
    if !matches!(ready, Ok(Ok(()))) {
        return Ok(host.shutdown().await?);
    }
    // Application separately selects its OS shutdown signal against host.wait().
    Ok(host.wait().await?)
}
```

Construct `AgentHostBindings::new(http, worker, maintenance)` and explicitly
supply each role's readiness object in `AgentHostDependencies`. The role checks
its own actual store/registry plus your bound identity, policy, provider and
evidence dependencies. No allow-all readiness implementation is provided.
Readiness callbacks must be read-only, bounded, yielding and cancellation-safe.
Do not serve cloned input routers independently: they bypass this host's gate.
The shared service is claimed synchronously; duplicate ownership is rejected
without shutting down the original owner.

## Startup, readiness and failures

`launch` returns the owner before asynchronous startup. Startup order is
maintenance → Worker → HTTP. Existing queued work may execute before ingress
starts. There is no durable rollback if later startup fails. Failed/panicking
startup joins roles already started and closes shared ingress; construct fresh
services/bindings for retry. Cancelling `wait_ready` retains the owner; explicitly
call `shutdown().await` when the application startup deadline expires.

`health()` is an internal payload-free view, not an anonymous health HTTP route.
It contains actual role views and the first closed failure category, without
pool or registry ownership. `Ready` requires fresh successful evidence from all
three roles. Every hosted HTTP request checks this state; Worker ticks also check
maintenance freshness before admission. Observed loss returns 503 and pauses new
execution. Recovery can reopen admission without restarting the process.
This does not revoke already admitted work or make multiple health reads a
distributed atomic snapshot. External outages become visible through bounded
periodic probes, not instantaneous remote knowledge.

Unexpected role exit closes admission and starts sibling drain. Connection-local
HTTP errors do not by themselves terminate the host. Reports retain role-local
forced-drain counters and sanitized failures, not credentials or callback errors.
Panic hooks and application logs still require redaction. There is no automatic
restart loop; inspect the failure and let a qualified OS supervisor restart.

## Shutdown and recovery

`begin_shutdown()` closes hosted ingress synchronously. Joined drain is HTTP →
Worker → maintenance, keeping maintenance available during Worker cleanup.
`shutdown()` and `wait(&mut self)` retain ownership when their waits are cancelled.
After successful joined completion, all owned connections, SSE producers, ticks
and nested node futures are destroyed. `Drop` closes admission and aborts owned
work but cannot synchronously join it; counters may take time to reach zero.

Each role retains its validated finite drain deadline. An outer shutdown budget
must cover their sum plus cooperative task destruction; non-yielding code still
needs an OS hard-kill deadline. Shutdown is not durable user cancellation, and
an interrupted database commit is not proof of rollback. Restore compatible
bindings and recover through the existing journal, idempotency and lease fencing;
do not replay external writes blindly. Shared pools are never closed by the host.

## Qualification and deployment boundary

PostgreSQL 16/17 acceptance covers HTTP-to-terminal execution, independent
readiness loss/recovery despite a 60-second HTTP probe interval, startup errors
and panics at all stages, cancellation-safe waits, unexpected role exits, ordered
forced drain, Drop and exclusive loopback ownership. The pinned real TLS Keycloak
fixture additionally recovers an existing admission to a terminal result,
verifies no repeated node execution and exercises actual resource-policy expiry
and recovery. CI retains `STATEKNOT_HOST_*_EVIDENCE` plus exact source/tree,
lockfile and image metadata. Run database qualifications serially after builds.

Only static bilingual documentation is deployed to `stknot.com`. No fixture
identity, default credentials or unqualified Agent runtime is deployed there.
Authenticated operational endpoints, cross-process rolling deployment, measured
capacity/recovery SLOs and remaining production release gates are still separate
work. Roll back a host application only to a qualified compatible version and
preserve durable state; this change adds no schema or dependency migration.
