// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{AgentHttpService, HttpError, failure};
use axum::{
    Router,
    extract::Request,
    middleware::{self, Next},
};
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use stateknot_core::{BoxFuture, EventId};
use std::{net::SocketAddr, sync::Arc};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
    time::{Instant, sleep, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;

mod health;
mod options;
pub use health::{AgentHttpServerHealth, AgentHttpServerStatus};
pub use options::AgentHttpServerOptions;

/// Mandatory host evidence for credential-verifier and policy dependencies.
/// Implementations must be nonblocking, cancellation-cooperative and read-only.
/// The runtime additionally verifies its own actual store and deployment registry.
pub trait AgentHttpReadiness: Send + Sync {
    /// Checks the same verifier/policy dependencies used by this ingress.
    fn check(&self) -> BoxFuture<'_, Result<(), AgentHttpReadinessError>>;
}

/// Sanitized host dependency failure; no token, issuer or policy details.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("HTTP ingress host dependencies are unavailable")]
pub struct AgentHttpReadinessError;

impl AgentHttpReadiness for stateknot_runtime::agent_policy::AgentResourcePolicy {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentHttpReadinessError>> {
        Box::pin(async move { self.check_readiness().map_err(|_| AgentHttpReadinessError) })
    }
}

/// Closed startup/runtime errors for the owned HTTP role.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentHttpServerError {
    /// This profile requires loopback TCP behind co-located TLS termination.
    #[error("HTTP ingress requires a loopback listener")]
    InvalidListener,
    /// A shared ingress service can be claimed only once and must not be shut down.
    #[error("HTTP ingress service has already been claimed or stopped")]
    AlreadyClaimed,
    /// A startup dependency check failed or exceeded its whole-check deadline.
    #[error("HTTP ingress dependencies are unavailable")]
    Unavailable,
    /// Owned runtime panicked/was cancelled, or its completion was already taken.
    #[error("HTTP ingress runtime is stopped")]
    Stopped,
}

/// Completion evidence, not a guarantee that interrupted database work rolled back.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AgentHttpDrainReport {
    /// Connections still pending when the drain deadline forced task cancellation.
    pub forced_connections: usize,
    /// HTTP/IO failures, lifetime expirations and unexpected connection-task failures.
    pub connection_failures: u64,
    /// Accepted sockets rejected immediately because the connection ceiling was full.
    pub rejected_connections: u64,
    /// Whether an accept error triggered fail-closed drain instead of host shutdown.
    pub listener_failed: bool,
}

/// Owned ingress-only role. Drop aborts owned work; use `shutdown().await` to drain.
///
/// No migrations, schedulers, Workers, OS signal handlers or public health routes
/// are installed. No shared `PostgreSQL` pool is closed. Host callbacks must remain
/// cooperative; a process supervisor still needs an outer hard kill deadline.
///
/// ```no_run
/// use std::sync::Arc;
/// use stateknot::{agent_http::*, runtime::AgentServiceV1};
/// use tokio::net::TcpListener;
///
/// async fn ingress(
///     service: AgentServiceV1,
///     verifier: Arc<dyn AgentHttpAuthenticator>,
///     dependencies: Arc<dyn AgentHttpReadiness>,
/// ) -> Result<AgentHttpServer, Box<dyn std::error::Error>> {
///     let listener = TcpListener::bind("127.0.0.1:8080").await?;
///     let http = AgentHttpService::new(service, verifier,
///         AgentHttpOptions::new(["agents.example.com".to_owned()])?);
///     Ok(AgentHttpServer::start(listener, http, dependencies,
///         AgentHttpServerOptions::default()).await?)
/// }
/// ```
pub struct AgentHttpServer {
    address: SocketAddr,
    health: AgentHttpServerHealth,
    stop: CancellationToken,
    task: Option<JoinHandle<AgentHttpDrainReport>>,
}

impl AgentHttpServer {
    /// Claims a loopback listener and shared ingress service, validates actual
    /// dependencies, then starts accepting. All service clones share shutdown;
    /// standalone routers are not automatically gated by this server's readiness.
    ///
    /// # Errors
    ///
    /// Rejects non-loopback listeners, repeated/stopped service ownership and
    /// failed/timed-out readiness. Once claimed, failure or cancellation closes
    /// the service; construct a new service for a startup retry.
    pub async fn start(
        listener: TcpListener,
        http: AgentHttpService,
        host: Arc<dyn AgentHttpReadiness>,
        options: AgentHttpServerOptions,
    ) -> Result<Self, AgentHttpServerError> {
        Self::start_supervised(listener, http, host, options, None).await
    }

    pub(crate) async fn start_supervised(
        listener: TcpListener,
        http: AgentHttpService,
        host: Arc<dyn AgentHttpReadiness>,
        options: AgentHttpServerOptions,
        supervisor: Option<crate::agent_host::AgentHostHealth>,
    ) -> Result<Self, AgentHttpServerError> {
        let address = listener
            .local_addr()
            .map_err(|_| AgentHttpServerError::InvalidListener)?;
        if !address.ip().is_loopback() {
            return Err(AgentHttpServerError::InvalidListener);
        }
        // AgentHost claims synchronously before starting any role. Standalone
        // ingress claims here; both paths share the same atomic ownership bit.
        if supervisor.is_none() {
            http.claim_server()?;
        }
        let health = AgentHttpServerHealth::new(options.freshness, http.inner.stream_tasks.clone());
        let guard = RuntimeGuard {
            http,
            health: health.clone(),
        };
        if !check(&guard.http, host.as_ref(), &options).await
            || guard.http.inner.shutdown.is_cancelled()
        {
            return Err(AgentHttpServerError::Unavailable);
        }
        health.update(AgentHttpServerStatus::Ready);
        let stop = CancellationToken::new();
        let task = tokio::spawn(run(
            listener,
            guard,
            host,
            options,
            stop.clone(),
            supervisor,
        ));
        Ok(Self {
            address,
            health,
            stop,
            task: Some(task),
        })
    }

    /// Returns the bound loopback address. Public URLs belong to the reverse proxy.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Returns local health without retaining ownership of the runtime.
    #[must_use]
    pub fn health(&self) -> AgentHttpServerHealth {
        self.health.clone()
    }

    /// Immediately closes readiness and requests listener/connection drain.
    pub fn begin_shutdown(&self) {
        self.health.update(AgentHttpServerStatus::Draining);
        self.stop.cancel();
    }

    /// Requests bounded drain and joins all owned connection tasks.
    /// Cancelling this future retains the runtime handle; call again to join.
    pub async fn shutdown(&mut self) -> Result<AgentHttpDrainReport, AgentHttpServerError> {
        self.begin_shutdown();
        self.wait().await
    }

    /// Waits for completion without initiating drain. Cancellation-safe while
    /// the server handle is retained. A second completed wait returns `Stopped`.
    pub async fn wait(&mut self) -> Result<AgentHttpDrainReport, AgentHttpServerError> {
        let result = self
            .task
            .as_mut()
            .ok_or(AgentHttpServerError::Stopped)?
            .await
            .map_err(|_| AgentHttpServerError::Stopped);
        self.task.take();
        result
    }
}

impl Drop for AgentHttpServer {
    fn drop(&mut self) {
        self.begin_shutdown();
        self.health.update(AgentHttpServerStatus::Stopped);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct RuntimeGuard {
    http: AgentHttpService,
    health: AgentHttpServerHealth,
}

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        self.http.shutdown();
        self.health.update(AgentHttpServerStatus::Stopped);
    }
}

async fn check(
    http: &AgentHttpService,
    host: &dyn AgentHttpReadiness,
    options: &AgentHttpServerOptions,
) -> bool {
    timeout(options.probe_timeout, async {
        http.inner
            .service
            .check_readiness()
            .await
            .map_err(|_| AgentHttpReadinessError)?;
        host.check().await
    })
    .await
    .is_ok_and(|result| result.is_ok())
}

async fn monitor(
    guard: &RuntimeGuard,
    host: &dyn AgentHttpReadiness,
    options: &AgentHttpServerOptions,
) {
    loop {
        sleep(options.probe_interval).await;
        let status = if check(&guard.http, host, options).await {
            AgentHttpServerStatus::Ready
        } else {
            AgentHttpServerStatus::Unavailable
        };
        guard.health.update(status);
    }
}

async fn run(
    listener: TcpListener,
    guard: RuntimeGuard,
    host: Arc<dyn AgentHttpReadiness>,
    options: AgentHttpServerOptions,
    stop: CancellationToken,
    supervisor: Option<crate::agent_host::AgentHostHealth>,
) -> AgentHttpDrainReport {
    let gate = guard.health.clone();
    let router =
        guard
            .http
            .router()
            .layer(middleware::from_fn(move |request: Request, next: Next| {
                let gate = gate.clone();
                let supervisor = supervisor.clone();
                async move {
                    if gate.is_ready()
                        && supervisor
                            .as_ref()
                            .is_none_or(crate::agent_host::AgentHostHealth::allows_ingress)
                    {
                        next.run(request).await
                    } else {
                        failure(HttpError::Unavailable, EventId::generate())
                    }
                }
            }));
    let mut tasks = JoinSet::new();
    let drain = CancellationToken::new();
    let mut report = AgentHttpDrainReport::default();
    {
        let probe = monitor(&guard, host.as_ref(), &options);
        tokio::pin!(probe);
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => break,
                () = guard.http.inner.shutdown.cancelled() => break,
                () = &mut probe => break,
                result = tasks.join_next(), if !tasks.is_empty() => {
                    count_failure(result.as_ref(), &mut report);
                }
                accepted = listener.accept() => {
                    let Ok((socket, _)) = accepted else { report.listener_failed = true; break; };
                    if tasks.len() == options.max_connections {
                        report.rejected_connections = report.rejected_connections.saturating_add(1);
                        drop(socket);
                    } else {
                        tasks.spawn(connection(socket, router.clone(), drain.clone(), options.clone(), guard.health.connection()));
                    }
                }
            }
        }
    } // Drop any active readiness check before drain.
    guard.health.update(AgentHttpServerStatus::Draining);
    drop(listener);
    drain.cancel();
    let deadline = Instant::now() + options.drain_timeout;
    while !tasks.is_empty() {
        if let Ok(result) = timeout_at(deadline, tasks.join_next()).await {
            count_failure(result.as_ref(), &mut report);
        } else {
            report.forced_connections = tasks.len();
            guard.http.shutdown();
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
    }
    // Body drop aborts each producer. Wait for its destructors too, so callers
    // can release host resources after shutdown without detached SSE work.
    guard.http.shutdown();
    guard.http.inner.stream_tasks.close();
    guard.http.inner.stream_tasks.wait().await;
    report
}

fn count_failure(
    result: Option<&Result<bool, tokio::task::JoinError>>,
    report: &mut AgentHttpDrainReport,
) {
    if !matches!(result, Some(Ok(true))) {
        report.connection_failures = report.connection_failures.saturating_add(1);
    }
}

async fn connection(
    socket: TcpStream,
    router: Router,
    drain: CancellationToken,
    options: AgentHttpServerOptions,
    _active: health::ConnectionGuard,
) -> bool {
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .keep_alive(true)
        .half_close(false)
        .max_headers(64)
        .max_buf_size(32 * 1024)
        .header_read_timeout(options.header_timeout);
    let connection =
        builder.serve_connection(TokioIo::new(socket), TowerToHyperService::new(router));
    tokio::pin!(connection);
    let lifetime = sleep(options.connection_lifetime);
    tokio::pin!(lifetime);
    tokio::select! {
        biased;
        () = drain.cancelled() => connection.as_mut().graceful_shutdown(),
        () = &mut lifetime => return false,
        result = &mut connection => return result.is_ok(),
    }
    tokio::select! {
        biased;
        () = &mut lifetime => false,
        result = &mut connection => result.is_ok(),
    }
}
