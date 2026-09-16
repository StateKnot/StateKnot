// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Owned execution role over the existing fenced PostgreSQL schedulers.
//! Hosts separately operate ingress, reconcilers, credential/configuration
//! distribution and an OS supervisor. No migrations or public routes are added.

use futures_util::FutureExt;
use stateknot_core::{BoxFuture, CancellationObserver, CancellationSignal};
use stateknot_runtime::TenantSchedulerOutcome;
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
pub use binding::{AgentWorkerBinding, AgentWorkerExecutionOptions};
pub use health::{AgentWorkerHealth, AgentWorkerStatus};
pub use options::AgentWorkerOptions;

/// Mandatory host qualification of actual provider/evidence/maintenance dependencies.
/// Checks must be read-only, nonblocking and cancellation-cooperative. The role
/// additionally checks its own actual store, registry and persisted fair policy.
pub trait AgentWorkerReadiness: Send + Sync {
    /// Checks the dependencies actually used by the bound execution environment.
    fn check(&self) -> BoxFuture<'_, Result<(), AgentWorkerReadinessError>>;
}

/// Sanitized unavailable dependency, without credentials or callback diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("Worker host dependencies are unavailable")]
pub struct AgentWorkerReadinessError;

/// Closed role startup/wait errors; durable run failures are counted separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum AgentWorkerError {
    /// At least one configured bound is outside the supported finite range.
    #[error("Worker options are invalid")]
    InvalidOptions,
    /// The actual dependency check failed, panicked or exceeded its deadline.
    #[error("Worker dependencies are unavailable")]
    Unavailable,
    /// Coordinator was cancelled/panicked or its report has already been taken.
    #[error("Worker coordinator is stopped")]
    Stopped,
}

/// First failure that caused fail-stop drain, without underlying error payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentWorkerFailure {
    /// Selection/reservation failed; uncertain database outcomes require recovery.
    Scheduler,
    /// A tick exceeded its absolute execution deadline.
    TickDeadline,
    /// An owned task panicked or exited unexpectedly.
    Task,
}

/// Local saturating operational counters; not persisted usage or terminal results.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AgentWorkerReport {
    /// Admitted ticks, including interrupted ticks.
    pub started_ticks: u64,
    /// Ticks that returned a classified scheduler outcome.
    pub completed_ticks: u64,
    /// Executed durable quanta, including nonterminal/deferred/cancelled handoffs.
    pub executed_quanta: u64,
    /// Run-local execution failures after the loop attempted fenced cleanup.
    pub run_failures: u64,
    /// Failed/timed-out periodic readiness checks.
    pub readiness_failures: u64,
    /// Admitted tick futures still pending when forced cancellation began.
    pub forced_ticks: usize,
    /// First fail-stop cause; `None` means requested shutdown (not Run success).
    pub failure: Option<AgentWorkerFailure>,
}

/// Independently owned durable scheduling role. Use `shutdown().await` for joined
/// completion; Drop only initiates/aborts cleanup. Never closes the shared pool.
/// Callbacks must yield: an OS supervisor needs an outer hard-kill deadline.
///
/// ```no_run
/// use std::sync::Arc;
/// use stateknot::{agent_worker::*, core::TenantId, postgres::PostgresStore,
///     runtime::{ExecutableGraphRegistry, GraphLifecycleEvidenceProvider}};
/// async fn start(
///     store: PostgresStore, registry: ExecutableGraphRegistry, tenant: TenantId,
///     evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
///     dependencies: Arc<dyn AgentWorkerReadiness>,
/// ) -> Result<AgentWorker, Box<dyn std::error::Error>> {
///     let binding = AgentWorkerBinding::tenant(store, registry, evidence, tenant,
///         AgentWorkerExecutionOptions::default())?;
///     Ok(AgentWorker::start(binding, dependencies,
///         AgentWorkerOptions::default()).await?)
/// }
/// ```
pub struct AgentWorker {
    health: AgentWorkerHealth,
    stop: CancellationToken,
    task: Option<JoinHandle<AgentWorkerReport>>,
}

impl AgentWorker {
    /// Checks actual dependencies before spawning fixed slots. Cancelled/failed
    /// startup claims no work and spawns no runtime tasks. The consumed binding
    /// is not reusable; constructing a new identical binding is safe.
    pub async fn start(
        binding: AgentWorkerBinding,
        host: Arc<dyn AgentWorkerReadiness>,
        options: AgentWorkerOptions,
    ) -> Result<Self, AgentWorkerError> {
        if !probe(&binding, host.as_ref(), &options)
            .await
            .unwrap_or(false)
        {
            return Err(AgentWorkerError::Unavailable);
        }
        let health = AgentWorkerHealth::new(options.freshness, binding.activity());
        health.probe(true);
        let stop = CancellationToken::new();
        // Construct the guard before spawn: even abort-before-first-poll closes health.
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

    /// Returns a payload-free view independent of pool/executor lifetimes.
    pub fn health(&self) -> AgentWorkerHealth {
        self.health.clone()
    }

    /// Closes tick admission synchronously and requests cooperative cancellation.
    /// Already admitted ticks may still complete; this is not a durable Run cancel.
    pub fn begin_shutdown(&self) {
        self.health.draining();
        self.stop.cancel();
    }

    /// Joins completion. Cancelling this wait retains ownership of the coordinator.
    /// Successful completion includes destruction of all tracked node futures.
    pub async fn wait(&mut self) -> Result<AgentWorkerReport, AgentWorkerError> {
        let result = self.task.as_mut().ok_or(AgentWorkerError::Stopped)?.await;
        self.task = None;
        result.map_err(|_| AgentWorkerError::Stopped)
    }

    /// Requests shutdown then performs a cancellation-safe joined wait.
    pub async fn shutdown(&mut self) -> Result<AgentWorkerReport, AgentWorkerError> {
        self.begin_shutdown();
        self.wait().await
    }
}

impl Drop for AgentWorker {
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
    health: AgentWorkerHealth,
    stop: CancellationToken,
}
impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        self.stop.cancel();
        self.tasks.abort_all();
        self.health.stopped();
    }
}

// Catch both synchronous callback construction and future panics.
async fn probe(
    binding: &AgentWorkerBinding,
    host: &dyn AgentWorkerReadiness,
    options: &AgentWorkerOptions,
) -> Result<bool, ()> {
    AssertUnwindSafe(async {
        timeout(options.probe_timeout, async {
            binding.check().await?;
            host.check()
                .await
                .map_err(|_| AgentWorkerError::Unavailable)
        })
        .await
        .is_ok_and(|result| result.is_ok())
    })
    .catch_unwind()
    .await
    .map_err(|_| ())
}

async fn pause(duration: Duration, stop: &CancellationToken) {
    tokio::select! { biased; () = stop.cancelled() => {}, () = sleep(duration) => {} }
}

async fn monitor(
    binding: Arc<AgentWorkerBinding>,
    host: Arc<dyn AgentWorkerReadiness>,
    options: AgentWorkerOptions,
    health: AgentWorkerHealth,
    stop: CancellationToken,
) {
    loop {
        pause(options.probe_interval, &stop).await;
        if stop.is_cancelled() {
            return;
        }
        let result = tokio::select! {
            biased;
            () = stop.cancelled() => return,
            result = probe(&binding, host.as_ref(), &options) => result,
        };
        if let Ok(ready) = result {
            health.probe(ready);
        } else {
            health.fail(AgentWorkerFailure::Task);
            stop.cancel();
            return;
        }
    }
}

async fn slot(
    binding: Arc<AgentWorkerBinding>,
    options: AgentWorkerOptions,
    health: AgentWorkerHealth,
    stop: CancellationToken,
) {
    let signal = CancellationSignal::new(Signal(stop.clone()));
    while !stop.is_cancelled() {
        let Some(active) = health.admit() else {
            pause(options.idle_delay, &stop).await;
            continue;
        };
        let tick = binding.tick(signal.clone());
        tokio::pin!(tick);
        let result = tokio::select! {
            result = &mut tick => result,
            () = sleep(options.tick_timeout) => {
                health.fail(AgentWorkerFailure::TickDeadline);
                stop.cancel();
                // Keep the future alive for cooperative cleanup; coordinator owns
                // the finite drain deadline and joins forced cancellation.
                tick.await
            }
        };
        let delay = if let Ok(tick) = result {
            health.complete(tick.outcome());
            match tick.outcome() {
                TenantSchedulerOutcome::Executed { .. } => options.busy_delay,
                TenantSchedulerOutcome::ExecutionFailed { .. } => options.failure_delay,
                _ => options.idle_delay,
            }
        } else {
            health.fail(AgentWorkerFailure::Scheduler);
            stop.cancel();
            options.failure_delay
        };
        drop(active);
        pause(delay, &stop).await;
    }
}

async fn run(
    binding: Arc<AgentWorkerBinding>,
    host: Arc<dyn AgentWorkerReadiness>,
    options: AgentWorkerOptions,
    mut guard: RuntimeGuard,
) -> AgentWorkerReport {
    guard.tasks.spawn(monitor(
        binding.clone(),
        host,
        options.clone(),
        guard.health.clone(),
        guard.stop.clone(),
    ));
    for _ in 0..options.slots {
        guard.tasks.spawn(slot(
            binding.clone(),
            options.clone(),
            guard.health.clone(),
            guard.stop.clone(),
        ));
    }
    tokio::select! {
        biased;
        () = guard.stop.cancelled() => {},
        _ = guard.tasks.join_next() => {
            if !guard.stop.is_cancelled() { guard.health.fail(AgentWorkerFailure::Task); }
            guard.stop.cancel();
        }
    }
    guard.health.draining();
    let drained = timeout(options.drain_timeout, async {
        while let Some(result) = guard.tasks.join_next().await {
            if result.is_err() {
                guard.health.fail(AgentWorkerFailure::Task);
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
    // All producers are now joined, so the closed TaskTracker cannot acquire
    // new node futures. Aborted child futures must actually be dropped as well.
    guard.health.activity.wait_for_idle().await;
    guard.health.stopped();
    guard.health.report()
}
