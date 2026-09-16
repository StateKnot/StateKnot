// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Experimental concrete owner for co-located HTTP, Worker and maintenance roles.
//! Does not install OS signal handlers, public health routes or schema migrations.

use crate::{
    agent_http::{
        AgentHttpDrainReport, AgentHttpReadiness, AgentHttpServer, AgentHttpServerError,
        AgentHttpServerHealth, AgentHttpServerOptions, AgentHttpService,
    },
    agent_maintenance::{
        AgentMaintenance, AgentMaintenanceBinding, AgentMaintenanceError, AgentMaintenanceHealth,
        AgentMaintenanceOptions, AgentMaintenanceReadiness, AgentMaintenanceReport,
    },
    agent_worker::{
        AgentWorker, AgentWorkerBinding, AgentWorkerError, AgentWorkerHealth, AgentWorkerOptions,
        AgentWorkerReadiness, AgentWorkerReport,
    },
};
use futures_util::FutureExt;
use std::{net::SocketAddr, panic::AssertUnwindSafe, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{net::TcpListener, task::JoinHandle, time::sleep};
use tokio_util::sync::CancellationToken;

mod health;
use health::Phase;
pub use health::{AgentHostHealth, AgentHostStatus};

/// Concrete role identity, without application or tenant labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentHostRole {
    /// Authenticated ingress.
    Http,
    /// Durable execution.
    Worker,
    /// Durable maintenance.
    Maintenance,
}

/// First fail-stop cause. Underlying role reports remain available separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentHostFailure {
    /// Failed, timed-out or panicking startup dependency.
    Startup(AgentHostRole),
    /// A role exited without host shutdown, or failed during drain.
    Role(AgentHostRole),
}

/// Closed synchronous launch and ownership errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum AgentHostError {
    /// Listener must be loopback behind qualified TLS termination.
    #[error("Agent host requires a loopback listener")]
    InvalidListener,
    /// Shared ingress already belongs to a host/server or has been stopped.
    #[error("Agent host ingress has already been claimed or stopped")]
    AlreadyClaimed,
    /// Owner stopped, panicked or its completion was already taken.
    #[error("Agent host is stopped")]
    Stopped,
}

/// Consumes the actual three bindings, not independent readiness-only substitutes.
/// The trusted application must qualify compatible databases, registries and
/// tenant scope across bindings; logical database identity cannot be inferred.
pub struct AgentHostBindings {
    http: AgentHttpService,
    worker: AgentWorkerBinding,
    maintenance: AgentMaintenanceBinding,
}
impl AgentHostBindings {
    /// Binds all roles. Do not separately serve routers cloned from `http`:
    /// those listeners would bypass the supervisor's per-request admission gate.
    #[must_use]
    pub fn new(
        http: AgentHttpService,
        worker: AgentWorkerBinding,
        maintenance: AgentMaintenanceBinding,
    ) -> Self {
        Self {
            http,
            worker,
            maintenance,
        }
    }
}

/// Mandatory, read-only checks of each role's actual host dependencies.
/// No default identity/policy/provider success checks are installed.
pub struct AgentHostDependencies {
    /// Verifier and resource-policy availability for the bound ingress.
    pub http: Arc<dyn AgentHttpReadiness>,
    /// Actual provider/evidence/host dependencies for execution.
    pub worker: Arc<dyn AgentWorkerReadiness>,
    /// Actual dependencies and policy for maintenance.
    pub maintenance: Arc<dyn AgentMaintenanceReadiness>,
}

/// Independently validated role options. Total drain uses all three deadlines
/// sequentially plus cooperative destruction; an OS supervisor bounds hard kill.
#[derive(Clone, Debug, Default)]
pub struct AgentHostOptions {
    /// Connection, readiness and ingress drain bounds.
    pub http: AgentHttpServerOptions,
    /// Tick, readiness and execution drain bounds.
    pub worker: AgentWorkerOptions,
    /// Tick, readiness and maintenance drain bounds.
    pub maintenance: AgentMaintenanceOptions,
}

/// Joined local completion. Absent role reports mean the role never started.
/// `Ok` reports can still contain role-specific failures or forced drain counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AgentHostReport {
    /// First sanitized fail-stop cause; `None` means requested host shutdown.
    pub failure: Option<AgentHostFailure>,
    /// Ingress completion, including forced connection counts.
    pub http: Option<Result<AgentHttpDrainReport, AgentHttpServerError>>,
    /// Worker completion, including forced ticks and failures.
    pub worker: Option<Result<AgentWorkerReport, AgentWorkerError>>,
    /// Maintenance completion and per-job counters.
    pub maintenance: Option<Result<AgentMaintenanceReport, AgentMaintenanceError>>,
}

/// Owns asynchronous startup, role supervision and ordered joined drain.
/// Keep this handle across cancelled waits; Drop cannot synchronously join work.
/// Process shutdown never requests durable user cancellation or closes pools.
///
/// ```no_run
/// use stateknot::agent_host::*;
/// use tokio::net::TcpListener;
/// async fn serve(bindings: AgentHostBindings, dependencies: AgentHostDependencies)
///     -> Result<AgentHostReport, Box<dyn std::error::Error>> {
///     let listener = TcpListener::bind("127.0.0.1:8080").await?;
///     let mut host = AgentHost::launch(listener, bindings, dependencies,
///         AgentHostOptions::default())?;
///     let ready = tokio::time::timeout(std::time::Duration::from_secs(30),
///         host.wait_ready()).await;
///     if !matches!(ready, Ok(Ok(()))) { return Ok(host.shutdown().await?); }
///     // Application owns OS signal handling; health() is an internal view.
///     Ok(host.wait().await?)
/// }
/// ```
pub struct AgentHost {
    address: SocketAddr,
    health: AgentHostHealth,
    stop: CancellationToken,
    task: Option<JoinHandle<AgentHostReport>>,
}
impl AgentHost {
    /// Returns ownership before asynchronous startup. Requires a Tokio runtime.
    /// Maintenance starts first, Worker second, ingress last; already queued
    /// work can execute during startup. Failure does not roll back durable work.
    pub fn launch(
        listener: TcpListener,
        bindings: AgentHostBindings,
        dependencies: AgentHostDependencies,
        options: AgentHostOptions,
    ) -> Result<Self, AgentHostError> {
        let address = listener
            .local_addr()
            .map_err(|_| AgentHostError::InvalidListener)?;
        if !address.ip().is_loopback() {
            return Err(AgentHostError::InvalidListener);
        }
        bindings
            .http
            .claim_server()
            .map_err(|_| AgentHostError::AlreadyClaimed)?;
        let health = AgentHostHealth::new();
        let stop = CancellationToken::new();
        // Construct before spawn: abort-before-first-poll closes shared ingress too.
        let guard = Roles {
            service: bindings.http.clone(),
            health: health.clone(),
            http: None,
            worker: None,
            maintenance: None,
        };
        let task = tokio::spawn(run(
            listener,
            bindings,
            dependencies,
            options,
            guard,
            stop.clone(),
        ));
        Ok(Self {
            address,
            health,
            stop,
            task: Some(task),
        })
    }

    /// Bound loopback address, not the external TLS URL.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Payload-free internal view; expose only through a protected host boundary.
    #[must_use]
    pub fn health(&self) -> AgentHostHealth {
        self.health.clone()
    }

    /// Waits for all roles to be ready. Cancelling retains runtime ownership.
    /// An unavailable host can recover; apply the application's startup deadline.
    pub async fn wait_ready(&self) -> Result<(), AgentHostError> {
        loop {
            match self.health.status() {
                AgentHostStatus::Ready => return Ok(()),
                AgentHostStatus::Stopped | AgentHostStatus::Draining => {
                    return Err(AgentHostError::Stopped);
                }
                _ => sleep(Duration::from_millis(20)).await,
            }
        }
    }

    /// Synchronously closes hosted ingress. Drain order is HTTP, Worker, maintenance.
    pub fn begin_shutdown(&self) {
        self.health.draining();
        self.stop.cancel();
    }

    /// Cancellation-safe joined wait. A second completed wait returns `Stopped`.
    pub async fn wait(&mut self) -> Result<AgentHostReport, AgentHostError> {
        let result = self.task.as_mut().ok_or(AgentHostError::Stopped)?.await;
        self.task = None;
        result.map_err(|_| AgentHostError::Stopped)
    }

    /// Requests ordered shutdown and joins every started role and its descendants.
    pub async fn shutdown(&mut self) -> Result<AgentHostReport, AgentHostError> {
        self.begin_shutdown();
        self.wait().await
    }
}
impl Drop for AgentHost {
    fn drop(&mut self) {
        self.begin_shutdown();
        self.health.update(|state| state.phase = Phase::Stopped);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct Roles {
    service: AgentHttpService,
    health: AgentHostHealth,
    http: Option<AgentHttpServer>,
    worker: Option<AgentWorker>,
    maintenance: Option<AgentMaintenance>,
}
impl Drop for Roles {
    fn drop(&mut self) {
        self.health.update(|state| state.phase = Phase::Stopped);
        self.service.shutdown();
    }
}

async fn start(
    listener: TcpListener,
    bindings: AgentHostBindings,
    dependencies: AgentHostDependencies,
    options: AgentHostOptions,
    roles: &mut Roles,
    stage: &mut AgentHostRole,
) -> Result<(), ()> {
    *stage = AgentHostRole::Maintenance;
    let maintenance = AgentMaintenance::start(
        bindings.maintenance,
        dependencies.maintenance,
        options.maintenance,
    )
    .await
    .map_err(|_| ())?;
    let maintenance_health = maintenance.health();
    roles
        .health
        .update(|state| state.maintenance = Some(maintenance_health.clone()));
    roles.maintenance = Some(maintenance);
    *stage = AgentHostRole::Worker;
    let worker = AgentWorker::start_supervised(
        bindings.worker,
        dependencies.worker,
        options.worker,
        Some(maintenance_health),
    )
    .await
    .map_err(|_| ())?;
    roles
        .health
        .update(|state| state.worker = Some(worker.health()));
    roles.worker = Some(worker);
    *stage = AgentHostRole::Http;
    let http = AgentHttpServer::start_supervised(
        listener,
        bindings.http,
        dependencies.http,
        options.http,
        Some(roles.health.clone()),
    )
    .await
    .map_err(|_| ())?;
    roles
        .health
        .update(|state| state.http = Some(http.health()));
    roles.http = Some(http);
    // Shutdown may have raced with the last successful startup poll.
    roles.health.update(|state| {
        if state.phase == Phase::Starting {
            state.phase = Phase::Running;
        }
    });
    Ok(())
}

async fn run(
    listener: TcpListener,
    bindings: AgentHostBindings,
    dependencies: AgentHostDependencies,
    options: AgentHostOptions,
    mut roles: Roles,
    stop: CancellationToken,
) -> AgentHostReport {
    let mut report = AgentHostReport::default();
    let mut stage = AgentHostRole::Maintenance;
    let startup = tokio::select! {
        biased;
        () = stop.cancelled() => None,
        result = AssertUnwindSafe(start(listener, bindings, dependencies, options, &mut roles, &mut stage)).catch_unwind() => Some(result),
    };
    if let Some(result) = startup {
        if matches!(result, Ok(Ok(()))) {
            tokio::select! {
                biased;
                () = stop.cancelled() => {},
                result = roles.http.as_mut().expect("started ingress").wait() => {
                    report.http = Some(result); roles.health.fail(AgentHostFailure::Role(AgentHostRole::Http));
                },
                result = roles.worker.as_mut().expect("started Worker").wait() => {
                    report.worker = Some(result); roles.health.fail(AgentHostFailure::Role(AgentHostRole::Worker));
                },
                result = roles.maintenance.as_mut().expect("started maintenance").wait() => {
                    report.maintenance = Some(result); roles.health.fail(AgentHostFailure::Role(AgentHostRole::Maintenance));
                },
            }
        } else {
            roles.health.fail(AgentHostFailure::Startup(stage));
        }
    }
    roles.health.draining();
    if let Some(http) = &mut roles.http {
        if report.http.is_none() {
            report.http = Some(http.shutdown().await);
        }
    }
    if let Some(worker) = &mut roles.worker {
        if report.worker.is_none() {
            report.worker = Some(worker.shutdown().await);
        }
    }
    if let Some(maintenance) = &mut roles.maintenance {
        if report.maintenance.is_none() {
            report.maintenance = Some(maintenance.shutdown().await);
        }
    }
    if report
        .http
        .is_some_and(|r| r.is_err() || r.is_ok_and(|r| r.listener_failed))
    {
        roles
            .health
            .fail(AgentHostFailure::Role(AgentHostRole::Http));
    }
    if report
        .worker
        .is_some_and(|r| r.is_err() || r.is_ok_and(|r| r.failure.is_some()))
    {
        roles
            .health
            .fail(AgentHostFailure::Role(AgentHostRole::Worker));
    }
    if report
        .maintenance
        .is_some_and(|r| r.is_err() || r.is_ok_and(|r| r.failure.is_some()))
    {
        roles
            .health
            .fail(AgentHostFailure::Role(AgentHostRole::Maintenance));
    }
    report.failure = roles.health.failure();
    // A panicking role coordinator aborts its JoinSet on unwind. Joining that
    // coordinator alone does not join destruction of its aborted descendants.
    // All producers are now closed; wait for the actual activity guards too.
    while roles
        .health
        .http()
        .is_some_and(|h| h.active_connections() != 0 || h.active_streams() != 0)
        || roles
            .health
            .worker()
            .is_some_and(|h| h.active_ticks() != 0 || h.active_nodes() != 0)
        || roles
            .health
            .maintenance()
            .is_some_and(|h| h.active_ticks() != 0)
    {
        sleep(Duration::from_millis(5)).await;
    }
    report
}
