// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Owned, bounded maintenance over the existing durable Agent state machines.
//! No node execution, implicit migrations, retention, pool closure or public routes.

use futures_util::FutureExt;
use stateknot_core::{BoxFuture, CancellationObserver, CancellationSignal};
use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

mod binding;
mod health;
mod options;
pub use binding::{AgentMaintenanceBinding, AgentMaintenanceMutationOptions};
pub use health::{AgentMaintenanceHealth, AgentMaintenanceStatus};
pub use options::AgentMaintenanceOptions;

/// Mandatory read-only qualification of the actual host dependencies and policy.
/// Callbacks must yield and be cancellation-cooperative; no default allow-all check.
pub trait AgentMaintenanceReadiness: Send + Sync {
    /// Checks host dependencies in addition to the bound store's schema check.
    fn check(&self) -> BoxFuture<'_, Result<(), AgentMaintenanceReadinessError>>;
}

/// Sanitized readiness failure, without database, identity or payload diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("maintenance host dependencies are unavailable")]
pub struct AgentMaintenanceReadinessError;

/// Closed startup and ownership errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum AgentMaintenanceError {
    /// A finite timing bound is invalid.
    #[error("maintenance options are invalid")]
    InvalidOptions,
    /// Require 1–128 distinct explicitly authorized tenants.
    #[error("maintenance tenant allowlist is invalid")]
    InvalidTenants,
    /// At least one exact offline maintenance schema is absent.
    #[error("maintenance schemas are unavailable")]
    SchemaUnavailable,
    /// Actual database or host readiness failed, panicked or timed out.
    #[error("maintenance dependencies are unavailable")]
    Unavailable,
    /// Coordinator failed or its report has already been taken.
    #[error("maintenance coordinator is stopped")]
    Stopped,
}

/// Four concrete jobs; child reconciliation includes cancellation and settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentMaintenanceJob {
    /// Request cancellation for admitted Agent deadlines using database time.
    Deadline,
    /// Propagate child cancellation and settle terminal children.
    Child,
    /// Publish eligible durable child Join results.
    Join,
    /// Close failed parents after durable child accounting completes.
    FailureClose,
}
impl AgentMaintenanceJob {
    /// Deterministic job order within each tenant's rotation.
    pub const ALL: [Self; 4] = [Self::Deadline, Self::Child, Self::Join, Self::FailureClose];
    const fn index(self) -> usize {
        self as usize
    }
}

/// First fail-stop cause; item errors instead count and continue the sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentMaintenanceFailure {
    /// Candidate discovery failed; restart from durable records.
    Store,
    /// One admitted tick exceeded its absolute deadline.
    TickDeadline,
    /// An owned task panicked or exited unexpectedly.
    Task,
}

/// Saturating per-job observations, not durable usage or terminal Run outcomes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AgentMaintenanceJobReport {
    /// Returned finite ticks, including interrupted partial pages.
    pub completed_ticks: u64,
    /// Candidate observations (including failures and repeated observations).
    pub items: u64,
    /// Failed candidate observations; alert even when dependency status is Ready.
    pub item_failures: u64,
}

/// Process-local counters, reset on restart and safe for protected health views.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AgentMaintenanceReport {
    /// Admitted ticks, including interrupted or failed discovery.
    pub started_ticks: u64,
    /// Failed or timed-out periodic dependency checks.
    pub readiness_failures: u64,
    /// Active tick futures when forced drain began (at most one).
    pub forced_ticks: usize,
    /// First fail-stop cause; absence means requested shutdown, not Run success.
    pub failure: Option<AgentMaintenanceFailure>,
    jobs: [AgentMaintenanceJobReport; 4],
}
impl AgentMaintenanceReport {
    /// Counters for a concrete job, without tenant or Run labels.
    #[must_use]
    pub const fn job(&self, job: AgentMaintenanceJob) -> AgentMaintenanceJobReport {
        self.jobs[job.index()]
    }
}

/// Owned maintenance role. Await `shutdown` for joined completion; Drop only
/// initiates cleanup. An OS supervisor must bound non-yielding host code.
///
/// ```no_run
/// use std::sync::Arc;
/// use stateknot_runtime::{agent_maintenance::*, JsonSchemaRegistry};
/// use stateknot_core::TenantId;
/// use stateknot_store_postgres::PostgresStore;
/// async fn start(store: PostgresStore, schemas: JsonSchemaRegistry,
///     tenants: Vec<TenantId>, host: Arc<dyn AgentMaintenanceReadiness>)
///     -> Result<AgentMaintenance, AgentMaintenanceError> {
///     let binding = AgentMaintenanceBinding::new(store, schemas, tenants,
///         AgentMaintenanceMutationOptions::default())?;
///     AgentMaintenance::start(binding, host, AgentMaintenanceOptions::default()).await
/// }
/// ```
pub struct AgentMaintenance {
    health: AgentMaintenanceHealth,
    stop: CancellationToken,
    task: Option<JoinHandle<AgentMaintenanceReport>>,
}
impl AgentMaintenance {
    /// Checks actual dependencies before spawning any maintenance task. Failed
    /// or cancelled startup performs no durable mutation and claims no work.
    pub async fn start(
        binding: AgentMaintenanceBinding,
        host: Arc<dyn AgentMaintenanceReadiness>,
        options: AgentMaintenanceOptions,
    ) -> Result<Self, AgentMaintenanceError> {
        if !probe(&binding, host.as_ref(), &options)
            .await
            .unwrap_or(false)
        {
            return Err(AgentMaintenanceError::Unavailable);
        }
        let health = AgentMaintenanceHealth::new(options.freshness);
        health.probe(true);
        let stop = CancellationToken::new();
        // Construct before spawn so abort-before-first-poll still closes health.
        let guard = RuntimeGuard {
            tasks: JoinSet::new(),
            health: health.clone(),
            stop: stop.clone(),
        };
        let task = tokio::spawn(run(Arc::new(binding), host, options, guard));
        Ok(Self {
            health,
            stop,
            task: Some(task),
        })
    }
    /// Payload-free cached health without retaining store or schema handles.
    pub fn health(&self) -> AgentMaintenanceHealth {
        self.health.clone()
    }
    /// Synchronously closes admission; does not request durable Run cancellation.
    pub fn begin_shutdown(&self) {
        self.health.draining();
        self.stop.cancel();
    }
    /// Cancellation-safe joined wait; a cancelled wait retains coordinator ownership.
    pub async fn wait(&mut self) -> Result<AgentMaintenanceReport, AgentMaintenanceError> {
        let result = self
            .task
            .as_mut()
            .ok_or(AgentMaintenanceError::Stopped)?
            .await;
        self.task = None;
        result.map_err(|_| AgentMaintenanceError::Stopped)
    }
    /// Requests shutdown, then joins all owned tasks including forced cancellation.
    pub async fn shutdown(&mut self) -> Result<AgentMaintenanceReport, AgentMaintenanceError> {
        self.begin_shutdown();
        self.wait().await
    }
}
impl Drop for AgentMaintenance {
    fn drop(&mut self) {
        self.begin_shutdown();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
struct Signal(CancellationToken);
impl CancellationObserver for Signal {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.0.cancelled())
    }
}
struct RuntimeGuard {
    tasks: JoinSet<()>,
    health: AgentMaintenanceHealth,
    stop: CancellationToken,
}
impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        self.stop.cancel();
        self.tasks.abort_all();
        self.health.stopped();
    }
}
async fn probe(
    binding: &AgentMaintenanceBinding,
    host: &dyn AgentMaintenanceReadiness,
    options: &AgentMaintenanceOptions,
) -> Result<bool, ()> {
    AssertUnwindSafe(async {
        timeout(options.probe_timeout, async {
            binding.check().await?;
            host.check()
                .await
                .map_err(|_| AgentMaintenanceError::Unavailable)
        })
        .await
        .is_ok_and(|r| r.is_ok())
    })
    .catch_unwind()
    .await
    .map_err(|_| ())
}
async fn pause(duration: Duration, stop: &CancellationToken) {
    tokio::select! { biased; () = stop.cancelled() => {}, () = sleep(duration) => {} }
}
async fn monitor(
    binding: Arc<AgentMaintenanceBinding>,
    host: Arc<dyn AgentMaintenanceReadiness>,
    options: AgentMaintenanceOptions,
    health: AgentMaintenanceHealth,
    stop: CancellationToken,
) {
    loop {
        pause(options.probe_interval, &stop).await;
        let result = tokio::select! { biased; () = stop.cancelled() => return, result = probe(&binding, host.as_ref(), &options) => result };
        if let Ok(ready) = result {
            health.probe(ready);
        } else {
            health.fail(AgentMaintenanceFailure::Task);
            stop.cancel();
            return;
        }
    }
}
async fn sweep(
    binding: Arc<AgentMaintenanceBinding>,
    options: AgentMaintenanceOptions,
    health: AgentMaintenanceHealth,
    stop: CancellationToken,
) {
    let mut rotation = binding.rotation();
    let rotation_len = rotation.len();
    let mut next = 0;
    let signal = CancellationSignal::new(Signal(stop.clone()));
    while !stop.is_cancelled() {
        let Some(active) = health.admit() else {
            pause(options.delay, &stop).await;
            continue;
        };
        let progress = &mut rotation[next];
        let job = progress.job();
        let tick = binding.tick(progress, signal.clone());
        tokio::pin!(tick);
        let result = tokio::select! {
            result = &mut tick => result,
            () = sleep(options.tick_timeout) => {
                health.fail(AgentMaintenanceFailure::TickDeadline); stop.cancel();
                tick.await
            }
        };
        let delay = if let Ok(counts) = result {
            health.complete(job, counts);
            if counts.item_failures > 0 {
                options.failure_delay
            } else {
                options.delay
            }
        } else {
            health.fail(AgentMaintenanceFailure::Store);
            stop.cancel();
            options.failure_delay
        };
        next = (next + 1) % rotation_len;
        drop(active);
        pause(delay, &stop).await;
    }
}
async fn run(
    binding: Arc<AgentMaintenanceBinding>,
    host: Arc<dyn AgentMaintenanceReadiness>,
    options: AgentMaintenanceOptions,
    mut guard: RuntimeGuard,
) -> AgentMaintenanceReport {
    guard.tasks.spawn(monitor(
        binding.clone(),
        host,
        options.clone(),
        guard.health.clone(),
        guard.stop.clone(),
    ));
    guard.tasks.spawn(sweep(
        binding,
        options.clone(),
        guard.health.clone(),
        guard.stop.clone(),
    ));
    tokio::select! {
        biased;
        () = guard.stop.cancelled() => {},
        _ = guard.tasks.join_next() => {
            if !guard.stop.is_cancelled() { guard.health.fail(AgentMaintenanceFailure::Task); }
            guard.stop.cancel();
        }
    }
    guard.health.draining();
    drain(&mut guard, options.drain_timeout).await;
    guard.health.stopped();
    guard.health.report()
}

async fn drain(guard: &mut RuntimeGuard, deadline: Duration) {
    let drained = timeout(deadline, async {
        while let Some(result) = guard.tasks.join_next().await {
            if result.is_err() {
                guard.health.fail(AgentMaintenanceFailure::Task);
            }
        }
    })
    .await
    .is_ok();
    if !drained {
        guard.health.forced();
        guard.tasks.abort_all();
        while guard.tasks.join_next().await.is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn forced_drain_joins_future_destruction() {
        let health = AgentMaintenanceHealth::new(Duration::from_secs(1));
        health.probe(true);
        let active = health.admit().unwrap();
        let mut guard = RuntimeGuard {
            tasks: JoinSet::new(),
            health: health.clone(),
            stop: CancellationToken::new(),
        };
        guard.tasks.spawn(async move {
            let _active = active;
            std::future::pending::<()>().await;
        });
        health.draining();
        guard.stop.cancel();
        drain(&mut guard, Duration::from_millis(10)).await;
        assert!(guard.tasks.is_empty());
        assert_eq!(health.active_ticks(), 0);
        assert_eq!(health.report().forced_ticks, 1);
    }
    #[tokio::test]
    async fn abort_before_first_poll_closes_health() {
        let health = AgentMaintenanceHealth::new(Duration::from_secs(1));
        let guard = RuntimeGuard {
            tasks: JoinSet::new(),
            health: health.clone(),
            stop: CancellationToken::new(),
        };
        let task = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(health.status(), AgentMaintenanceStatus::Stopped);
    }
}
