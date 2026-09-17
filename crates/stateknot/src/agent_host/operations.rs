// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Experimental authenticated read-only host operations. Independent of business
//! readiness; unavailable identity or operator policy always fails closed.
use super::AgentHostHealth;
use crate::{
    agent_http::{
        AgentHttpAuthenticationError, AgentHttpAuthenticator, AgentHttpCredential,
        AgentHttpDrainReport, HttpError, encode, failure, response, single, validate_media,
        validate_origin_host,
    },
    http_transport::{ConnectionGuard, connection, count_failure},
};
use axum::{
    Router,
    body::to_bytes,
    extract::Request,
    http::{Method, header},
    response::Response,
};
use futures_util::FutureExt;
use stateknot_core::EventId;
use std::{
    net::SocketAddr,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
    time::{Instant, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;

mod options;
mod policy;
#[cfg(test)]
mod tests;
mod wire;
pub use options::AgentHostOperationsOptions;
pub use policy::AgentHostOperationsPolicy;

/// Closed configuration/ownership failures; no identity or endpoint details.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum AgentHostOperationsError {
    /// Invalid operator snapshot, stale generation, overflow or poisoned policy.
    #[error("invalid host operations policy or stale replacement")]
    InvalidPolicy,
    /// Requires loopback behind separately qualified TLS termination.
    #[error("host operations requires a loopback listener")]
    InvalidListener,
    /// Coordinator failed or completion was already taken.
    #[error("host operations is stopped")]
    Stopped,
}

struct Context {
    policy: Arc<AgentHostOperationsPolicy>,
    verifier: Arc<dyn AgentHttpAuthenticator>,
    options: AgentHostOperationsOptions,
    permits: Semaphore,
    stop: CancellationToken,
}

/// Owns one operations listener and all its connections. No health router can
/// be cloned out of this owner. Stop/join the business host before this listener
/// when final stopped-state inspection is required. Drop aborts, but cannot join.
/// All host callbacks must be nonblocking and cancellation-cooperative.
///
/// ```no_run
/// use std::{sync::Arc, time::Duration};
/// use stateknot::{agent_host::{AgentHost, operations::*},
///     agent_http::AgentHttpAuthenticator, runtime::AgentServiceCaller};
/// use tokio::net::TcpListener;
/// async fn run(mut host: AgentHost, operators: Vec<AgentServiceCaller>,
///     verifier: Arc<dyn AgentHttpAuthenticator>) -> Result<(), Box<dyn std::error::Error>> {
///     let policy = Arc::new(AgentHostOperationsPolicy::new(
///         host.health(), operators, Duration::from_secs(300))?);
///     let mut operations = AgentHostOperations::start(
///         TcpListener::bind("127.0.0.1:8081").await?, policy, verifier,
///         AgentHostOperationsOptions::new(["ops.example.com".into()])?)?;
///     // The application owns signals and trusted policy renewal.
///     let host_report = host.shutdown().await?;
///     let operations_report = operations.shutdown().await?;
///     // Inspect both reports; Ok does not imply absence of forced cleanup.
///     Ok(())
/// }
/// ```
pub struct AgentHostOperations {
    address: SocketAddr,
    active: Arc<AtomicUsize>,
    stop: CancellationToken,
    task: Option<JoinHandle<AgentHttpDrainReport>>,
    completed: Option<Result<AgentHttpDrainReport, AgentHostOperationsError>>,
}
impl AgentHostOperations {
    /// Starts independently of business readiness. Requires a Tokio runtime,
    /// explicit verifier, exact Host allowlist and separate expiring operators.
    pub fn start(
        listener: TcpListener,
        policy: Arc<AgentHostOperationsPolicy>,
        verifier: Arc<dyn AgentHttpAuthenticator>,
        options: AgentHostOperationsOptions,
    ) -> Result<Self, AgentHostOperationsError> {
        let address = listener
            .local_addr()
            .map_err(|_| AgentHostOperationsError::InvalidListener)?;
        if !address.ip().is_loopback() {
            return Err(AgentHostOperationsError::InvalidListener);
        }
        let stop = CancellationToken::new();
        let active = Arc::new(AtomicUsize::new(0));
        let context = Arc::new(Context {
            policy,
            verifier,
            permits: Semaphore::new(options.requests),
            options,
            stop: stop.clone(),
        });
        // Guard exists before spawn, including abort-before-first-poll.
        let guard = Guard {
            stop: stop.clone(),
            tasks: JoinSet::new(),
        };
        let task = tokio::spawn(run(listener, context, active.clone(), guard));
        Ok(Self {
            address,
            active,
            stop,
            task: Some(task),
            completed: None,
        })
    }
    /// Actual bound loopback address, not the external TLS origin.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }
    /// Owned connection futures whose destructors have not completed.
    #[must_use]
    pub fn active_connections(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
    /// Closes request admission immediately and starts finite joined drain.
    pub fn begin_shutdown(&self) {
        self.stop.cancel();
    }
    /// Cancellation-safe wait: retaining this owner retains the join handle.
    /// Success joins all connections, including forced cancellations.
    pub async fn wait(&mut self) -> Result<AgentHttpDrainReport, AgentHostOperationsError> {
        if self.completed.is_none() {
            let result = self
                .task
                .as_mut()
                .ok_or(AgentHostOperationsError::Stopped)?
                .await;
            self.completed = Some(result.map_err(|_| AgentHostOperationsError::Stopped));
            self.task = None;
        }
        // On coordinator panic JoinSet Drop aborts descendants asynchronously.
        while self.active_connections() != 0 {
            tokio::task::yield_now().await;
        }
        self.completed
            .take()
            .ok_or(AgentHostOperationsError::Stopped)?
    }
    /// Starts drain and waits; a cancelled future may be resumed using wait.
    pub async fn shutdown(&mut self) -> Result<AgentHttpDrainReport, AgentHostOperationsError> {
        self.begin_shutdown();
        self.wait().await
    }
}
impl Drop for AgentHostOperations {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
struct Guard {
    stop: CancellationToken,
    tasks: JoinSet<bool>,
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.stop.cancel();
        self.tasks.abort_all();
    }
}

async fn run(
    listener: TcpListener,
    context: Arc<Context>,
    active: Arc<AtomicUsize>,
    mut guard: Guard,
) -> AgentHttpDrainReport {
    let handler = context.clone();
    let router = Router::new().fallback(move |request: Request| handle(handler.clone(), request));
    let options = &context.options.transport;
    let drain = CancellationToken::new();
    let mut report = AgentHttpDrainReport::default();
    loop {
        tokio::select! {
            biased;
            () = context.stop.cancelled() => break,
            result = guard.tasks.join_next(), if !guard.tasks.is_empty() => count_failure(result.as_ref(), &mut report),
            accepted = listener.accept() => {
                let Ok((socket, _)) = accepted else { report.listener_failed = true; break; };
                if guard.tasks.len() >= options.max_connections {
                    report.rejected_connections = report.rejected_connections.saturating_add(1);
                    drop(socket);
                } else {
                    guard.tasks.spawn(connection(socket, router.clone(), drain.clone(), options.clone(), ConnectionGuard::new(active.clone())));
                }
            }
        }
    }
    context.stop.cancel();
    drop(listener);
    drain.cancel();
    let deadline = Instant::now() + options.drain_timeout;
    while !guard.tasks.is_empty() {
        if let Ok(result) = timeout_at(deadline, guard.tasks.join_next()).await {
            count_failure(result.as_ref(), &mut report);
        } else {
            report.forced_connections = guard.tasks.len();
            guard.tasks.abort_all();
            while guard.tasks.join_next().await.is_some() {}
        }
    }
    report
}

async fn handle(context: Arc<Context>, request: Request) -> Response {
    let id = EventId::generate();
    if context.stop.is_cancelled() {
        return failure(HttpError::Unavailable, id);
    }
    let Ok(_permit) = context.permits.try_acquire() else {
        return failure(HttpError::Overloaded, id);
    };
    let result = timeout(
        context.options.deadline,
        AssertUnwindSafe(execute(&context, request, id)).catch_unwind(),
    )
    .await;
    match result {
        Ok(Ok(Ok(response))) => response,
        Ok(Ok(Err(error))) => failure(error, id),
        _ => failure(HttpError::Unavailable, id),
    }
}

async fn execute(context: &Context, request: Request, id: EventId) -> Result<Response, HttpError> {
    let (parts, body) = request.into_parts();
    validate_origin_host(&parts.headers, &parts.uri, &context.options.http)?;
    let auth = single(&parts.headers, header::AUTHORIZATION.as_str())?
        .ok_or(HttpError::Unauthenticated)?;
    let (scheme, token) = auth.split_once(' ').ok_or(HttpError::Unauthenticated)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(HttpError::Unauthenticated);
    }
    let credential = AgentHttpCredential::new(token).map_err(|_| HttpError::Unauthenticated)?;
    let principal =
        context
            .verifier
            .authenticate(credential)
            .await
            .map_err(|error| match error {
                AgentHttpAuthenticationError::Unauthenticated => HttpError::Unauthenticated,
                AgentHttpAuthenticationError::Unavailable => HttpError::Unavailable,
            })?;
    context.policy.authorize(&principal)?;
    if parts.uri.query().is_some() || parts.uri.path().len() > 256 || parts.uri.path().contains('%')
    {
        return Err(HttpError::Invalid);
    }
    if !matches!(
        parts.uri.path(),
        "/v1/host/status" | "/v1/host/live" | "/v1/host/ready"
    ) {
        return Err(HttpError::NotFound);
    }
    if parts.method != Method::GET {
        return Err(HttpError::Method("GET"));
    }
    validate_media(&parts.headers, &parts.method)?;
    if parts.headers.contains_key(header::TRANSFER_ENCODING)
        || single(&parts.headers, header::CONTENT_LENGTH.as_str())?
            .is_some_and(|length| length != "0")
    {
        return Err(HttpError::Invalid);
    }
    to_bytes(body, 0).await.map_err(|_| HttpError::Invalid)?;
    // Recheck after every await; no cached grants across requests or body waits.
    if context.stop.is_cancelled() {
        return Err(HttpError::Unavailable);
    }
    context.policy.authorize(&principal)?;
    let (status, value) = wire::snapshot(&context.policy.health, parts.uri.path(), id);
    Ok(response(status, id, encode(&value, 16 * 1024)?))
}
