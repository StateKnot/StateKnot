// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! One bounded, lease-safe durable graph agent loop.

use std::{sync::Arc, time::Duration};

use stateknot_core::{BoxFuture, CancellationSignal, RunFence, Timestamp};
use stateknot_store_postgres::{
    AppendOutcome, BarrierCommitOutcome, DelayedRetryScheduleOutcome, LeaseReleaseOutcome,
    PostgresStore, StoreError, WaitCheckpointCommitOutcome,
};
use thiserror::Error;

use crate::{
    DurableGraphDriver, DurableGraphDriverBuildError, DurableGraphDriverOptions,
    DurableGraphLifecycle, DurableGraphLifecycleBuildError, DurableGraphLifecycleOptions,
    ExecutableGraphRegistry, GraphBarrierLifecycleOutcome, GraphDriveOutcome, GraphDriveReport,
    GraphDriverError, GraphLifecycleError, GraphLifecycleEvidenceProvider,
};

const MAX_MUTATION_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Closed result of one bounded claimed-run execution quantum.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AgentLoopOutcome {
    /// Durable cancellation intent and exact cumulative usage were acknowledged.
    CancellationConfirmed(AppendOutcome),
    /// The graph suspended with a complete durable wait batch.
    Waiting(WaitCheckpointCommitOutcome),
    /// The graph and admitted agent invocation succeeded.
    Succeeded(BarrierCommitOutcome),
    /// Durable supervision committed terminal failure.
    Failed(AppendOutcome),
    /// Same-fence in-flight work released ownership for crash takeover.
    RecoveryReleased(LeaseReleaseOutcome),
    /// All unsettled work is durably gated until this database instant.
    Deferred {
        /// Inclusive database retry boundary.
        not_before: Timestamp,
        /// Exact durable scheduling convergence result.
        schedule: DelayedRetryScheduleOutcome,
    },
    /// The configured durable work quantum was exhausted safely.
    Yielded {
        /// Exact-fence lease release convergence.
        release: LeaseReleaseOutcome,
    },
    /// Cooperative shutdown won and ownership was released.
    Cancelled {
        /// Exact-fence lease release convergence.
        release: LeaseReleaseOutcome,
    },
}

/// Outcome and bounded execution counters for one loop call.
#[derive(Debug)]
pub struct AgentLoopResult {
    outcome: AgentLoopOutcome,
    report: GraphDriveReport,
}

impl AgentLoopResult {
    const fn new(outcome: AgentLoopOutcome, report: GraphDriveReport) -> Self {
        Self { outcome, report }
    }

    /// Returns why this bounded loop call stopped.
    #[must_use]
    pub const fn outcome(&self) -> &AgentLoopOutcome {
        &self.outcome
    }

    /// Returns replay, execution, renewal, and retry counters.
    #[must_use]
    pub const fn report(&self) -> GraphDriveReport {
        self.report
    }

    /// Consumes the result into its outcome and report.
    #[must_use]
    pub fn into_parts(self) -> (AgentLoopOutcome, GraphDriveReport) {
        (self.outcome, self.report)
    }
}

/// Production binding of driver execution and lifecycle coordination.
#[derive(Clone)]
pub struct DurableAgentLoop {
    store: PostgresStore,
    driver: DurableGraphDriver,
    lifecycle: DurableGraphLifecycle,
    cleanup_options: DurableGraphLifecycleOptions,
}

impl DurableAgentLoop {
    /// Builds a loop over one shared store and immutable deployment registry.
    ///
    /// Constructing both layers here prevents a caller from accidentally
    /// pairing a driver and lifecycle coordinator backed by different pools or
    /// executable snapshots.
    ///
    /// # Errors
    ///
    /// Returns the exact driver or lifecycle startup binding failure.
    pub fn new(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
        evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
        driver_options: DurableGraphDriverOptions,
        lifecycle_options: DurableGraphLifecycleOptions,
    ) -> Result<Self, DurableAgentLoopBuildError> {
        let driver = DurableGraphDriver::new(store.clone(), registry.clone(), driver_options)?;
        let lifecycle =
            DurableGraphLifecycle::new(store.clone(), registry, evidence, lifecycle_options)?;
        Ok(Self {
            store,
            driver,
            lifecycle,
            cleanup_options: lifecycle_options,
        })
    }

    /// Drives one already-claimed exact fence to the next durable scheduling or
    /// lifecycle boundary.
    ///
    /// Driver and lifecycle failures trigger a bounded best-effort exact-fence
    /// release. A cleanup database failure is returned alongside the primary
    /// failure; stale, expired, absent, non-runnable, or quarantined ownership
    /// already permits safe takeover and does not replace the primary error.
    pub fn run(
        &self,
        fence: RunFence,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<AgentLoopResult, AgentLoopError>> {
        Box::pin(self.run_inner(fence, shutdown))
    }

    async fn run_inner(
        &self,
        fence: RunFence,
        shutdown: CancellationSignal,
    ) -> Result<AgentLoopResult, AgentLoopError> {
        let driven = match self.driver.drive(fence.clone(), shutdown).await {
            Ok(driven) => driven,
            Err(source) => return Err(self.cleanup_driver_error(&fence, source).await),
        };
        let (outcome, report) = driven.into_parts();
        let outcome = match outcome {
            GraphDriveOutcome::CancellationRequested(handoff) => {
                let lifecycle = match self.lifecycle.confirm_cancellation(*handoff).await {
                    Ok(outcome) => outcome,
                    Err(source) => {
                        return Err(self.cleanup_lifecycle_error(&fence, source).await);
                    }
                };
                lifecycle_outcome(lifecycle)
            }
            GraphDriveOutcome::LifecycleBarrierReady(handoff) => {
                let lifecycle = match self.lifecycle.commit_barrier(*handoff).await {
                    Ok(outcome) => outcome,
                    Err(source) => {
                        return Err(self.cleanup_lifecycle_error(&fence, source).await);
                    }
                };
                lifecycle_outcome(lifecycle)
            }
            GraphDriveOutcome::Blocked(handoff) => {
                let lifecycle = match self.lifecycle.resolve_blocked(*handoff).await {
                    Ok(outcome) => outcome,
                    Err(source) => {
                        return Err(self.cleanup_lifecycle_error(&fence, source).await);
                    }
                };
                lifecycle_outcome(lifecycle)
            }
            GraphDriveOutcome::Deferred {
                not_before,
                schedule,
            } => AgentLoopOutcome::Deferred {
                not_before,
                schedule,
            },
            GraphDriveOutcome::Yielded { release } => AgentLoopOutcome::Yielded { release },
            GraphDriveOutcome::Cancelled { release } => AgentLoopOutcome::Cancelled { release },
        };
        Ok(AgentLoopResult::new(outcome, report))
    }

    async fn cleanup_driver_error(
        &self,
        fence: &RunFence,
        source: GraphDriverError,
    ) -> AgentLoopError {
        match self.release_with_retry(fence).await {
            Ok(_) => AgentLoopError::Driver {
                source: Box::new(source),
            },
            Err(cleanup) if cleanup_error_is_benign(&cleanup) => AgentLoopError::Driver {
                source: Box::new(source),
            },
            Err(cleanup) => AgentLoopError::DriverCleanup {
                source: Box::new(source),
                cleanup: Box::new(cleanup),
            },
        }
    }

    async fn cleanup_lifecycle_error(
        &self,
        fence: &RunFence,
        source: GraphLifecycleError,
    ) -> AgentLoopError {
        match self.release_with_retry(fence).await {
            Ok(_) => AgentLoopError::Lifecycle {
                source: Box::new(source),
            },
            Err(cleanup) if cleanup_error_is_benign(&cleanup) => AgentLoopError::Lifecycle {
                source: Box::new(source),
            },
            Err(cleanup) => AgentLoopError::LifecycleCleanup {
                source: Box::new(source),
                cleanup: Box::new(cleanup),
            },
        }
    }

    async fn release_with_retry(
        &self,
        fence: &RunFence,
    ) -> Result<LeaseReleaseOutcome, StoreError> {
        let mut attempt = 1_u8;
        loop {
            match self.store.release_lease(fence).await {
                Ok(outcome) => return Ok(outcome),
                Err(error)
                    if attempt < self.cleanup_options.maximum_mutation_attempts()
                        && error.is_retryable() =>
                {
                    self.cleanup_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn cleanup_backoff(&self, attempt: u8) {
        let multiplier = 1_u32
            .checked_shl(u32::from(attempt.saturating_sub(1)))
            .unwrap_or(u32::MAX);
        let delay = self
            .cleanup_options
            .mutation_retry_initial_delay()
            .checked_mul(multiplier)
            .unwrap_or(MAX_MUTATION_RETRY_DELAY)
            .min(MAX_MUTATION_RETRY_DELAY);
        tokio::time::sleep(delay).await;
    }
}

fn lifecycle_outcome(outcome: GraphBarrierLifecycleOutcome) -> AgentLoopOutcome {
    match outcome {
        GraphBarrierLifecycleOutcome::Cancelled(outcome) => {
            AgentLoopOutcome::CancellationConfirmed(outcome)
        }
        GraphBarrierLifecycleOutcome::Waiting(outcome) => AgentLoopOutcome::Waiting(outcome),
        GraphBarrierLifecycleOutcome::Succeeded(outcome) => AgentLoopOutcome::Succeeded(outcome),
        GraphBarrierLifecycleOutcome::Failed(outcome) => AgentLoopOutcome::Failed(outcome),
        GraphBarrierLifecycleOutcome::Released(outcome) => {
            AgentLoopOutcome::RecoveryReleased(outcome)
        }
    }
}

fn cleanup_error_is_benign(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::NoActiveLease
            | StoreError::StaleFence
            | StoreError::LeaseExpired
            | StoreError::RunNotRunnable
            | StoreError::RunQuarantined
    )
}

/// Startup failure while building a consistent durable loop.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableAgentLoopBuildError {
    /// The graph driver could not bind to the deployment snapshot.
    #[error(transparent)]
    Driver(#[from] DurableGraphDriverBuildError),
    /// The lifecycle coordinator could not bind to the deployment snapshot.
    #[error(transparent)]
    Lifecycle(#[from] DurableGraphLifecycleBuildError),
}

/// Payload-redacted durable loop failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentLoopError {
    /// Graph driving failed; ownership was released or already reclaimable.
    #[error("durable agent graph driving failed: {source}")]
    Driver {
        /// Exact payload-redacted driver failure.
        #[source]
        source: Box<GraphDriverError>,
    },
    /// Lifecycle coordination failed; ownership was released or reclaimable.
    #[error("durable agent lifecycle coordination failed: {source}")]
    Lifecycle {
        /// Exact payload-redacted lifecycle failure.
        #[source]
        source: Box<GraphLifecycleError>,
    },
    /// Driver failure was followed by a database cleanup failure.
    #[error("durable agent graph driving and lease cleanup both failed")]
    DriverCleanup {
        /// Exact primary driver failure.
        source: Box<GraphDriverError>,
        /// Exact payload-redacted lease cleanup failure.
        cleanup: Box<StoreError>,
    },
    /// Lifecycle failure was followed by a database cleanup failure.
    #[error("durable agent lifecycle coordination and lease cleanup both failed")]
    LifecycleCleanup {
        /// Exact primary lifecycle failure.
        source: Box<GraphLifecycleError>,
        /// Exact payload-redacted lease cleanup failure.
        cleanup: Box<StoreError>,
    },
}

impl AgentLoopError {
    /// Returns a cleanup failure when both the primary operation and release
    /// failed.
    #[must_use]
    pub const fn cleanup_error(&self) -> Option<&StoreError> {
        match self {
            Self::DriverCleanup { cleanup, .. } | Self::LifecycleCleanup { cleanup, .. } => {
                Some(cleanup)
            }
            Self::Driver { .. } | Self::Lifecycle { .. } => None,
        }
    }
}
