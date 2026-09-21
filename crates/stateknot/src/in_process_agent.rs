// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Strongly typed, HTTP-free Agent execution that preserves durable admission,
//! authorization, PostgreSQL recovery, Worker fencing, and maintenance roles.

use crate::{
    agent_maintenance::{
        AgentMaintenance, AgentMaintenanceBinding, AgentMaintenanceError, AgentMaintenanceHealth,
        AgentMaintenanceMutationOptions, AgentMaintenanceOptions, AgentMaintenanceReadiness,
        AgentMaintenanceReport, AgentMaintenanceStatus,
    },
    agent_worker::{
        AgentWorker, AgentWorkerBinding, AgentWorkerError, AgentWorkerExecutionOptions,
        AgentWorkerHealth, AgentWorkerOptions, AgentWorkerReadiness, AgentWorkerReport,
        AgentWorkerStatus,
    },
};
use serde::{Serialize, de::DeserializeOwned};
use stateknot_core::{AgentSubmissionKey, BudgetLimits, TenantId};
use stateknot_runtime::{
    AgentRunSnapshot, AgentRunTerminalOutcome, AgentServiceAuthorizer, AgentServiceBuildError,
    AgentServiceCaller, AgentServiceError, AgentServiceRegistry, AgentServiceV1,
    DurableTenantSchedulerBuildError, ExecutableGraphRegistry, GraphLifecycleEvidenceProvider,
    JsonSchemaRegistryBuilder, TypedAgent, TypedAgentInputError, TypedAgentOutputError,
    register_standard_agent_deadline_event_schema, register_standard_child_join_event_schema,
    register_standard_child_reconciliation_event_schema,
    register_standard_run_failure_close_event_schema,
};
use stateknot_store_postgres::{PostgresStore, StoreError};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

const MIN_POLL_INTERVAL: Duration = Duration::from_millis(10);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(10);
const MIN_WAIT_TIMEOUT: Duration = Duration::from_millis(10);
const MAX_WAIT_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// Construction failure before either owned background role starts.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InProcessAgentBindingError {
    /// The durable service could not bind to its frozen deployment.
    #[error(transparent)]
    Service(#[from] AgentServiceBuildError),
    /// The tenant scheduler could not bind to its executable deployment.
    #[error(transparent)]
    Worker(#[from] DurableTenantSchedulerBuildError),
    /// A framework-owned maintenance schema could not be constructed.
    #[error("standard Agent maintenance schemas are unavailable")]
    MaintenanceSchemas,
    /// The maintenance role rejected its tenant or schema binding.
    #[error("Agent maintenance binding is invalid: {0}")]
    Maintenance(AgentMaintenanceError),
}

/// Exact dependencies for one tenant-scoped, HTTP-free runtime.
///
/// Construction performs no migration and starts no task. Migrations and the
/// immutable executable/service registries must already be qualified.
pub struct InProcessAgentBinding {
    store: PostgresStore,
    tenant: TenantId,
    service: AgentServiceV1,
    worker: AgentWorkerBinding,
    maintenance: AgentMaintenanceBinding,
}

impl InProcessAgentBinding {
    /// Binds one durable service, scheduler, and complete maintenance role.
    ///
    /// # Errors
    ///
    /// Rejects incomplete executable schemas, invalid deployments, scheduler
    /// construction failure, or unavailable framework maintenance schemas.
    #[allow(clippy::too_many_arguments)]
    pub fn tenant(
        store: PostgresStore,
        executable: ExecutableGraphRegistry,
        deployments: AgentServiceRegistry,
        authorizer: Arc<dyn AgentServiceAuthorizer>,
        evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
        tenant: TenantId,
        worker_options: AgentWorkerExecutionOptions,
        maintenance_options: AgentMaintenanceMutationOptions,
    ) -> Result<Self, InProcessAgentBindingError> {
        let service =
            AgentServiceV1::new(store.clone(), executable.clone(), deployments, authorizer)?;
        let worker = AgentWorkerBinding::tenant(
            store.clone(),
            executable,
            evidence,
            tenant.clone(),
            worker_options,
        )?;
        let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
        register_standard_agent_deadline_event_schema(&mut schemas)
            .map_err(|_| InProcessAgentBindingError::MaintenanceSchemas)?;
        register_standard_child_reconciliation_event_schema(&mut schemas)
            .map_err(|_| InProcessAgentBindingError::MaintenanceSchemas)?;
        register_standard_child_join_event_schema(&mut schemas)
            .map_err(|_| InProcessAgentBindingError::MaintenanceSchemas)?;
        register_standard_run_failure_close_event_schema(&mut schemas)
            .map_err(|_| InProcessAgentBindingError::MaintenanceSchemas)?;
        let schemas = schemas
            .build()
            .map_err(|_| InProcessAgentBindingError::MaintenanceSchemas)?;
        let maintenance = AgentMaintenanceBinding::new(
            store.clone(),
            schemas,
            vec![tenant.clone()],
            maintenance_options,
        )
        .map_err(InProcessAgentBindingError::Maintenance)?;
        Ok(Self {
            store,
            tenant,
            service,
            worker,
            maintenance,
        })
    }
}

/// Mandatory readiness checks for the actual Worker and maintenance dependencies.
pub struct InProcessAgentDependencies {
    /// Provider, evidence, and execution-host readiness.
    pub worker: Arc<dyn AgentWorkerReadiness>,
    /// Maintenance policy and host readiness.
    pub maintenance: Arc<dyn AgentMaintenanceReadiness>,
}

/// Independently bounded scheduling and maintenance role options.
#[derive(Clone, Debug, Default)]
pub struct InProcessAgentRuntimeOptions {
    /// Owned Worker lifecycle limits.
    pub worker: AgentWorkerOptions,
    /// Owned maintenance lifecycle limits.
    pub maintenance: AgentMaintenanceOptions,
}

/// Role whose unexpected exit initiated fail-stop sibling drain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InProcessAgentRole {
    /// Durable scheduling and graph execution.
    Worker,
    /// Deadline, child, Join, and failure-close maintenance.
    Maintenance,
}

/// Joined completion for the two owned roles.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InProcessAgentRuntimeReport {
    /// First role that exited before requested shutdown.
    pub failure: Option<InProcessAgentRole>,
    /// Worker drain result.
    pub worker: Option<Result<AgentWorkerReport, AgentWorkerError>>,
    /// Maintenance drain result.
    pub maintenance: Option<Result<AgentMaintenanceReport, AgentMaintenanceError>>,
}

/// Startup failure with cleanup of every role that had already started.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum InProcessAgentStartError {
    /// Maintenance readiness failed before task ownership was returned.
    #[error("Agent maintenance could not start: {0}")]
    Maintenance(AgentMaintenanceError),
    /// Worker readiness failed; the already-started maintenance role was joined.
    #[error("Agent Worker could not start: {0}")]
    Worker(AgentWorkerError),
    /// Worker startup failed and maintenance cleanup also failed.
    #[error("Agent Worker startup failed and maintenance cleanup did not join")]
    WorkerCleanup,
}

/// Local aggregate state derived from both payload-free role health views.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InProcessAgentRuntimeStatus {
    /// Both roles have fresh successful dependency evidence.
    Ready,
    /// A role is temporarily unavailable but can recover.
    Unavailable,
    /// New work is closed and role tasks are being reclaimed.
    Draining,
    /// At least one owned role has stopped.
    Stopped,
}

/// Cloneable, payload-free observation of the owned runtime.
#[derive(Clone)]
pub struct InProcessAgentRuntimeHealth {
    worker: AgentWorkerHealth,
    maintenance: AgentMaintenanceHealth,
    stopping: Arc<AtomicBool>,
}

impl InProcessAgentRuntimeHealth {
    /// Computes the conservative aggregate state without I/O.
    #[must_use]
    pub fn status(&self) -> InProcessAgentRuntimeStatus {
        if self.stopping.load(Ordering::Acquire)
            && !matches!(
                (self.worker.status(), self.maintenance.status()),
                (AgentWorkerStatus::Stopped, _) | (_, AgentMaintenanceStatus::Stopped)
            )
        {
            return InProcessAgentRuntimeStatus::Draining;
        }
        match (self.worker.status(), self.maintenance.status()) {
            (AgentWorkerStatus::Stopped, _) | (_, AgentMaintenanceStatus::Stopped) => {
                InProcessAgentRuntimeStatus::Stopped
            }
            (AgentWorkerStatus::Draining, _) | (_, AgentMaintenanceStatus::Draining) => {
                InProcessAgentRuntimeStatus::Draining
            }
            (AgentWorkerStatus::Ready, AgentMaintenanceStatus::Ready) => {
                InProcessAgentRuntimeStatus::Ready
            }
            _ => InProcessAgentRuntimeStatus::Unavailable,
        }
    }

    /// Returns the owned Worker health view.
    #[must_use]
    pub const fn worker(&self) -> &AgentWorkerHealth {
        &self.worker
    }

    /// Returns the owned maintenance health view.
    #[must_use]
    pub const fn maintenance(&self) -> &AgentMaintenanceHealth {
        &self.maintenance
    }
}

/// Runtime ownership error after startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum InProcessAgentRuntimeError {
    /// The supervisor stopped or its joined result was already taken.
    #[error("in-process Agent runtime is stopped")]
    Stopped,
}

/// Owns the HTTP-free service, Worker, maintenance role, and joined supervisor.
///
/// Dropping initiates cancellation, but callers must use [`Self::shutdown`] to
/// prove that all admitted local tasks were joined. Durable runs remain in
/// `PostgreSQL` and can be resumed by a replacement runtime.
pub struct InProcessAgentRuntime {
    store: PostgresStore,
    tenant: TenantId,
    service: AgentServiceV1,
    health: InProcessAgentRuntimeHealth,
    stop: CancellationToken,
    task: Option<JoinHandle<InProcessAgentRuntimeReport>>,
}

impl InProcessAgentRuntime {
    /// Qualifies dependencies, starts maintenance first, then starts the Worker
    /// under the maintenance gate. No HTTP listener or implicit migration exists.
    ///
    /// # Errors
    ///
    /// Returns only after a failed startup has joined every role that started.
    pub async fn start(
        binding: InProcessAgentBinding,
        dependencies: InProcessAgentDependencies,
        options: InProcessAgentRuntimeOptions,
    ) -> Result<Self, InProcessAgentStartError> {
        let mut maintenance = AgentMaintenance::start(
            binding.maintenance,
            dependencies.maintenance,
            options.maintenance,
        )
        .await
        .map_err(InProcessAgentStartError::Maintenance)?;
        let maintenance_health = maintenance.health();
        let worker = match AgentWorker::start_supervised(
            binding.worker,
            dependencies.worker,
            options.worker,
            Some(maintenance_health.clone()),
        )
        .await
        {
            Ok(worker) => worker,
            Err(error) => {
                return if maintenance.shutdown().await.is_ok() {
                    Err(InProcessAgentStartError::Worker(error))
                } else {
                    Err(InProcessAgentStartError::WorkerCleanup)
                };
            }
        };
        let stopping = Arc::new(AtomicBool::new(false));
        let health = InProcessAgentRuntimeHealth {
            worker: worker.health(),
            maintenance: maintenance_health,
            stopping,
        };
        let stop = CancellationToken::new();
        let task = tokio::spawn(supervise(worker, maintenance, stop.clone()));
        Ok(Self {
            store: binding.store,
            tenant: binding.tenant,
            service: binding.service,
            health,
            stop,
            task: Some(task),
        })
    }

    /// Returns a payload-free local health view.
    #[must_use]
    pub fn health(&self) -> InProcessAgentRuntimeHealth {
        self.health.clone()
    }

    /// Binds one typed codec and authenticated caller to this tenant runtime.
    ///
    /// # Errors
    ///
    /// Rejects a caller for another tenant before any durable lookup.
    pub fn agent<I, O>(
        &self,
        codec: TypedAgent<I, O>,
        caller: AgentServiceCaller,
    ) -> Result<InProcessAgent<'_, I, O>, InProcessAgentBindError> {
        if caller.tenant_id() != &self.tenant {
            return Err(InProcessAgentBindError::TenantMismatch);
        }
        Ok(InProcessAgent {
            runtime: self,
            codec,
            caller,
            options: InProcessAgentRunOptions::default(),
        })
    }

    /// Stops admission to owned roles. This does not cancel durable user runs.
    pub fn begin_shutdown(&self) {
        self.health.stopping.store(true, Ordering::Release);
        self.stop.cancel();
    }

    /// Cancellation-safe joined wait for the supervisor and both roles.
    ///
    /// # Errors
    ///
    /// Returns [`InProcessAgentRuntimeError::Stopped`] after completion was taken
    /// or if the supervisor task itself could not be joined.
    pub async fn wait(
        &mut self,
    ) -> Result<InProcessAgentRuntimeReport, InProcessAgentRuntimeError> {
        let result = self
            .task
            .as_mut()
            .ok_or(InProcessAgentRuntimeError::Stopped)?
            .await;
        self.task = None;
        result.map_err(|_| InProcessAgentRuntimeError::Stopped)
    }

    /// Requests Worker-first drain, then joins maintenance.
    ///
    /// # Errors
    ///
    /// Returns an ownership error only if the supervisor could not be joined.
    pub async fn shutdown(
        &mut self,
    ) -> Result<InProcessAgentRuntimeReport, InProcessAgentRuntimeError> {
        self.begin_shutdown();
        self.wait().await
    }
}

impl Drop for InProcessAgentRuntime {
    fn drop(&mut self) {
        self.begin_shutdown();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn supervise(
    mut worker: AgentWorker,
    mut maintenance: AgentMaintenance,
    stop: CancellationToken,
) -> InProcessAgentRuntimeReport {
    let mut report = InProcessAgentRuntimeReport::default();
    tokio::select! {
        biased;
        () = stop.cancelled() => {},
        result = worker.wait() => {
            report.worker = Some(result);
            report.failure = Some(InProcessAgentRole::Worker);
        },
        result = maintenance.wait() => {
            report.maintenance = Some(result);
            report.failure = Some(InProcessAgentRole::Maintenance);
        },
    }
    if report.worker.is_none() {
        report.worker = Some(worker.shutdown().await);
    }
    if report.maintenance.is_none() {
        report.maintenance = Some(maintenance.shutdown().await);
    }
    report
}

/// Typed binding failure before a run is submitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
pub enum InProcessAgentBindError {
    /// Caller tenant and owned scheduler tenant differ.
    #[error("Agent caller tenant does not match the in-process runtime tenant")]
    TenantMismatch,
}

/// Finite polling and caller-wait bounds. These do not alter durable run budgets.
#[derive(Clone, Copy, Debug)]
pub struct InProcessAgentRunOptions {
    poll_interval: Duration,
    wait_timeout: Duration,
}

impl InProcessAgentRunOptions {
    /// Constructs bounded wait options.
    ///
    /// # Errors
    ///
    /// Poll interval must be 10 ms–10 s and wait timeout 10 ms–24 h.
    pub fn new(
        poll_interval: Duration,
        wait_timeout: Duration,
    ) -> Result<Self, InProcessAgentRunOptionsError> {
        if !(MIN_POLL_INTERVAL..=MAX_POLL_INTERVAL).contains(&poll_interval)
            || !(MIN_WAIT_TIMEOUT..=MAX_WAIT_TIMEOUT).contains(&wait_timeout)
        {
            return Err(InProcessAgentRunOptionsError);
        }
        Ok(Self {
            poll_interval,
            wait_timeout,
        })
    }

    /// Durable snapshot polling interval.
    #[must_use]
    pub const fn poll_interval(self) -> Duration {
        self.poll_interval
    }

    /// Maximum duration this caller waits; expiry does not cancel the run.
    #[must_use]
    pub const fn wait_timeout(self) -> Duration {
        self.wait_timeout
    }
}

impl Default for InProcessAgentRunOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(50),
            wait_timeout: Duration::from_secs(30),
        }
    }
}

/// Invalid finite in-process wait options.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[error("in-process Agent wait options are outside supported bounds")]
pub struct InProcessAgentRunOptionsError;

/// One caller-retained idempotent typed submission.
pub struct InProcessAgentRequest<I> {
    key: AgentSubmissionKey,
    input: I,
    budget_limits: BudgetLimits,
}

impl<I> InProcessAgentRequest<I> {
    /// Creates a recoverable request. Reuse the same key and content after a
    /// timeout, dropped future, lost acknowledgement, or process restart.
    #[must_use]
    pub const fn new(key: AgentSubmissionKey, input: I, budget_limits: BudgetLimits) -> Self {
        Self {
            key,
            input,
            budget_limits,
        }
    }

    /// Returns the caller-retained durable idempotency key.
    #[must_use]
    pub const fn submission_key(&self) -> &AgentSubmissionKey {
        &self.key
    }

    /// Returns the typed input.
    #[must_use]
    pub const fn input(&self) -> &I {
        &self.input
    }

    /// Returns request-local restrictions layered into admission.
    #[must_use]
    pub const fn budget_limits(&self) -> &BudgetLimits {
        &self.budget_limits
    }
}

/// Why a still-durable run was returned before terminal completion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InProcessAgentPendingReason {
    /// The caller's finite wait elapsed. The run remains eligible for progress.
    WaitTimeout,
    /// An owned role is draining or stopped; a replacement runtime can resume.
    RuntimeUnavailable,
}

/// Typed observation returned without discarding durable failure or recovery state.
#[derive(Debug)]
#[non_exhaustive]
pub enum InProcessAgentRun<O> {
    /// Successful result decoded only after provenance, budget, and schema checks.
    Succeeded {
        /// Strongly typed output.
        output: O,
        /// Exact terminal durable snapshot.
        snapshot: AgentRunSnapshot,
    },
    /// Durable non-cancellation failure.
    Failed {
        /// Exact terminal durable snapshot and public failure.
        snapshot: AgentRunSnapshot,
    },
    /// Durable cancellation acknowledgement.
    Cancelled {
        /// Exact terminal durable snapshot and public cancellation failure.
        snapshot: AgentRunSnapshot,
    },
    /// Nonterminal run that can be observed again with the same submission key.
    Pending {
        /// Latest authorized durable snapshot.
        snapshot: AgentRunSnapshot,
        /// Local reason the wait returned.
        reason: InProcessAgentPendingReason,
    },
    /// Integrity or operator policy removed the run from execution.
    Quarantined {
        /// Latest authorized durable snapshot.
        snapshot: AgentRunSnapshot,
    },
}

impl<O> InProcessAgentRun<O> {
    /// Returns the durable snapshot for every outcome.
    #[must_use]
    pub const fn snapshot(&self) -> &AgentRunSnapshot {
        match self {
            Self::Succeeded { snapshot, .. }
            | Self::Failed { snapshot }
            | Self::Cancelled { snapshot }
            | Self::Pending { snapshot, .. }
            | Self::Quarantined { snapshot } => snapshot,
        }
    }
}

/// Submission, authorization, storage-integrity, or typed-codec failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InProcessAgentRunError {
    /// Typed input serialization or schema validation failed before admission.
    #[error(transparent)]
    Input(#[from] TypedAgentInputError),
    /// Durable service submission or authorized observation failed.
    #[error(transparent)]
    Service(#[from] AgentServiceError),
    /// The immutable admission could not be reloaded for terminal decoding.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Successful durable output failed provenance, accounting, schema, or decode checks.
    #[error(transparent)]
    Output(#[from] TypedAgentOutputError),
    /// Public snapshot and immutable admission contradicted each other.
    #[error("durable Agent snapshot does not match its immutable admission")]
    AdmissionMismatch,
    /// A terminal lifecycle omitted its mandatory outcome.
    #[error("terminal Agent snapshot omitted its durable outcome")]
    MissingTerminalOutcome,
    /// The owned roles are not currently ready for a new durable submission.
    #[error("in-process Agent runtime is unavailable for new submissions")]
    RuntimeUnavailable,
    /// The runtime does not understand a newer terminal outcome variant.
    #[error("terminal Agent outcome is unsupported by this runtime")]
    UnsupportedTerminalOutcome,
}

/// Typed, authenticated handle over an owned in-process runtime.
pub struct InProcessAgent<'runtime, I, O> {
    runtime: &'runtime InProcessAgentRuntime,
    codec: TypedAgent<I, O>,
    caller: AgentServiceCaller,
    options: InProcessAgentRunOptions,
}

impl<I, O> InProcessAgent<'_, I, O> {
    /// Replaces finite local wait options.
    #[must_use]
    pub const fn with_options(mut self, options: InProcessAgentRunOptions) -> Self {
        self.options = options;
        self
    }

    /// Returns the exact typed codec.
    #[must_use]
    pub const fn codec(&self) -> &TypedAgent<I, O> {
        &self.codec
    }
}

impl<I, O> InProcessAgent<'_, I, O>
where
    I: Serialize,
    O: DeserializeOwned,
{
    /// Submits durably, polls authorized snapshots, and returns a typed terminal
    /// result or a resumable nonterminal observation. Cancelling this future
    /// does not cancel the durable run.
    ///
    /// # Errors
    ///
    /// Rejects invalid typed input, authorization/service/storage failures, or
    /// terminal output whose durable provenance and accounting do not validate.
    pub async fn run(
        &self,
        request: InProcessAgentRequest<I>,
    ) -> Result<InProcessAgentRun<O>, InProcessAgentRunError> {
        if self.runtime.health.status() != InProcessAgentRuntimeStatus::Ready {
            return Err(InProcessAgentRunError::RuntimeUnavailable);
        }
        let durable_request = self
            .codec
            .prepare_request(&request.input, request.budget_limits)?;
        let admission = self
            .runtime
            .service
            .submit(
                self.caller.clone(),
                &request.key,
                self.codec.descriptor().metadata().identity(),
                durable_request,
            )
            .await?;
        let run_id = admission.snapshot().provenance().run_id();
        // Submission and result-read policy can differ. Always perform a fresh
        // authorized read before exposing or decoding any durable outcome.
        let mut snapshot = self
            .runtime
            .service
            .load(self.caller.clone(), run_id)
            .await?;
        let deadline = Instant::now() + self.options.wait_timeout;
        loop {
            if snapshot.is_quarantined() {
                return Ok(InProcessAgentRun::Quarantined { snapshot });
            }
            if snapshot.status().is_terminal() {
                return self.decode_terminal(snapshot).await;
            }
            if matches!(
                self.runtime.health.status(),
                InProcessAgentRuntimeStatus::Draining | InProcessAgentRuntimeStatus::Stopped
            ) {
                return Ok(InProcessAgentRun::Pending {
                    snapshot,
                    reason: InProcessAgentPendingReason::RuntimeUnavailable,
                });
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(InProcessAgentRun::Pending {
                    snapshot,
                    reason: InProcessAgentPendingReason::WaitTimeout,
                });
            }
            tokio::time::sleep(
                self.options
                    .poll_interval
                    .min(deadline.saturating_duration_since(now)),
            )
            .await;
            snapshot = self
                .runtime
                .service
                .load(self.caller.clone(), run_id)
                .await?;
        }
    }

    async fn decode_terminal(
        &self,
        snapshot: AgentRunSnapshot,
    ) -> Result<InProcessAgentRun<O>, InProcessAgentRunError> {
        let stored = self
            .runtime
            .store
            .load_agent_admission(self.caller.tenant_id(), snapshot.provenance().run_id())
            .await?;
        let admission = stored.admission();
        let intent = admission.intent();
        if admission.digest() != snapshot.admission_digest()
            || intent.provenance() != snapshot.provenance()
            || intent.descriptor().metadata().identity()
                != self.codec.descriptor().metadata().identity()
        {
            return Err(InProcessAgentRunError::AdmissionMismatch);
        }
        match snapshot
            .outcome()
            .ok_or(InProcessAgentRunError::MissingTerminalOutcome)?
        {
            AgentRunTerminalOutcome::Succeeded { result } => {
                let output = self.codec.decode_result(
                    result,
                    snapshot.provenance(),
                    intent.request(),
                    intent.budget(),
                )?;
                Ok(InProcessAgentRun::Succeeded { output, snapshot })
            }
            AgentRunTerminalOutcome::Failed { .. } => Ok(InProcessAgentRun::Failed { snapshot }),
            AgentRunTerminalOutcome::Cancelled { .. } => {
                Ok(InProcessAgentRun::Cancelled { snapshot })
            }
            _ => Err(InProcessAgentRunError::UnsupportedTerminalOutcome),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_options_are_finite() {
        assert!(
            InProcessAgentRunOptions::new(Duration::from_millis(10), Duration::from_millis(10))
                .is_ok()
        );
        assert!(
            InProcessAgentRunOptions::new(Duration::from_millis(9), Duration::from_secs(1))
                .is_err()
        );
        assert!(
            InProcessAgentRunOptions::new(Duration::from_millis(10), Duration::from_secs(86_401))
                .is_err()
        );
    }
}
