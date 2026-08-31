// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded tenant-scoped durable scheduler worker.

use std::{sync::Arc, time::Duration};

use stateknot_core::{AttemptId, BoxFuture, CancellationSignal, RunId, TenantId};
use stateknot_store_postgres::{
    PostgresStore, RunnableRunPageCursor, RunnableRunPageSize, StoreError,
};
use thiserror::Error;

use crate::{
    AgentLoopError, AgentLoopResult, DurableAgentLoop, DurableAgentLoopBuildError,
    DurableGraphDriverOptions, DurableGraphLifecycleOptions, ExecutableGraphRegistry,
    GraphLifecycleEvidenceProvider,
};

const MAX_CLAIM_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Bounded scan and claim policy for one tenant scheduler worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableTenantSchedulerOptions {
    page_size: RunnableRunPageSize,
    maximum_pages_per_tick: u8,
    maximum_claim_attempts: u8,
    claim_retry_initial_delay: Duration,
}

impl DurableTenantSchedulerOptions {
    /// Absolute number of stable-snapshot pages one tick may inspect.
    pub const HARD_MAXIMUM_PAGES_PER_TICK: u8 = 64;
    /// Absolute number of identical lease-claim attempts.
    pub const HARD_MAXIMUM_CLAIM_ATTEMPTS: u8 = 10;

    /// Constructs an explicit resource and retry policy.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive page or claim counts, and a zero or
    /// greater-than-one-second initial retry delay.
    pub fn new(
        page_size: RunnableRunPageSize,
        maximum_pages_per_tick: u8,
        maximum_claim_attempts: u8,
        claim_retry_initial_delay: Duration,
    ) -> Result<Self, DurableTenantSchedulerOptionsError> {
        if maximum_pages_per_tick == 0 || maximum_pages_per_tick > Self::HARD_MAXIMUM_PAGES_PER_TICK
        {
            return Err(DurableTenantSchedulerOptionsError::InvalidPageCount);
        }
        if maximum_claim_attempts == 0 || maximum_claim_attempts > Self::HARD_MAXIMUM_CLAIM_ATTEMPTS
        {
            return Err(DurableTenantSchedulerOptionsError::InvalidClaimAttempts);
        }
        if claim_retry_initial_delay.is_zero() || claim_retry_initial_delay > MAX_CLAIM_RETRY_DELAY
        {
            return Err(DurableTenantSchedulerOptionsError::InvalidClaimRetryDelay);
        }
        Ok(Self {
            page_size,
            maximum_pages_per_tick,
            maximum_claim_attempts,
            claim_retry_initial_delay,
        })
    }

    /// Returns the bounded decoded candidate page size.
    #[must_use]
    pub const fn page_size(self) -> RunnableRunPageSize {
        self.page_size
    }

    /// Returns the maximum fixed-cutoff pages inspected per tick.
    #[must_use]
    pub const fn maximum_pages_per_tick(self) -> u8 {
        self.maximum_pages_per_tick
    }

    /// Returns the maximum identical attempts for one claim.
    #[must_use]
    pub const fn maximum_claim_attempts(self) -> u8 {
        self.maximum_claim_attempts
    }

    /// Returns the first exponential claim retry delay.
    #[must_use]
    pub const fn claim_retry_initial_delay(self) -> Duration {
        self.claim_retry_initial_delay
    }
}

impl Default for DurableTenantSchedulerOptions {
    fn default() -> Self {
        Self {
            page_size: RunnableRunPageSize::new(RunnableRunPageSize::MAX)
                .expect("provider maximum is a valid page size"),
            maximum_pages_per_tick: 4,
            maximum_claim_attempts: 3,
            claim_retry_initial_delay: Duration::from_millis(25),
        }
    }
}

/// Invalid tenant scheduler policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableTenantSchedulerOptionsError {
    /// The page-chain bound was zero or above the hard ceiling.
    #[error("tenant scheduler page count is invalid")]
    InvalidPageCount,
    /// Claim attempts were zero or above the hard ceiling.
    #[error("tenant scheduler claim attempt count is invalid")]
    InvalidClaimAttempts,
    /// Initial claim backoff was zero or above one second.
    #[error("tenant scheduler claim retry delay is invalid")]
    InvalidClaimRetryDelay,
}

/// Bounded observations made while selecting one run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TenantSchedulerReport {
    pages_scanned: u8,
    candidates_scanned: u16,
    contention_skips: u16,
    claim_retries: u16,
}

impl TenantSchedulerReport {
    /// Returns stable-snapshot pages loaded in this tick.
    #[must_use]
    pub const fn pages_scanned(self) -> u8 {
        self.pages_scanned
    }

    /// Returns candidates inspected in durable queue order.
    #[must_use]
    pub const fn candidates_scanned(self) -> u16 {
        self.candidates_scanned
    }

    /// Returns candidates that changed or were owned before claim.
    #[must_use]
    pub const fn contention_skips(self) -> u16 {
        self.contention_skips
    }

    /// Returns transient claim retries using the same attempt identity.
    #[must_use]
    pub const fn claim_retries(self) -> u16 {
        self.claim_retries
    }
}

/// Closed result of one tenant scheduler tick.
#[derive(Debug)]
#[non_exhaustive]
pub enum TenantSchedulerOutcome {
    /// One claimed run reached its next agent-loop boundary.
    Executed {
        /// Exact selected run.
        run_id: RunId,
        /// Complete bounded loop result.
        result: AgentLoopResult,
    },
    /// One claimed run failed safely; the loop attempted lease cleanup.
    ExecutionFailed {
        /// Exact selected run.
        run_id: RunId,
        /// Payload-redacted run-local failure.
        error: AgentLoopError,
    },
    /// No claimable run existed in the complete stable snapshot.
    Idle,
    /// The configured scan ceiling stopped before the snapshot ended.
    ScanLimitReached,
    /// Cooperative shutdown won before a claim.
    Cancelled,
}

/// Scheduler outcome plus bounded selection counters.
#[derive(Debug)]
pub struct TenantSchedulerTick {
    outcome: TenantSchedulerOutcome,
    report: TenantSchedulerReport,
}

impl TenantSchedulerTick {
    const fn new(outcome: TenantSchedulerOutcome, report: TenantSchedulerReport) -> Self {
        Self { outcome, report }
    }

    /// Returns the tick's closed scheduling result.
    #[must_use]
    pub const fn outcome(&self) -> &TenantSchedulerOutcome {
        &self.outcome
    }

    /// Returns bounded scan and contention counters.
    #[must_use]
    pub const fn report(&self) -> TenantSchedulerReport {
        self.report
    }

    /// Consumes the tick into its outcome and report.
    #[must_use]
    pub fn into_parts(self) -> (TenantSchedulerOutcome, TenantSchedulerReport) {
        (self.outcome, self.report)
    }
}

/// One tenant-isolated durable scheduler worker.
///
/// A tick always scans the provider's `(available_at, run_id)` order under one
/// fixed database cutoff and claims at most one run. Deployments obtain bounded
/// concurrency by running a configured number of workers; database fencing
/// resolves races. Cross-tenant selection intentionally belongs to a separate
/// fairness layer so storage credentials and queue scans never cross tenant
/// scope implicitly.
#[derive(Clone)]
pub struct DurableTenantScheduler {
    store: PostgresStore,
    agent_loop: DurableAgentLoop,
    options: DurableTenantSchedulerOptions,
}

impl DurableTenantScheduler {
    /// Builds a tenant worker and its internally consistent durable loop.
    ///
    /// # Errors
    ///
    /// Returns an exact driver/lifecycle startup binding failure.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
        evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
        driver_options: DurableGraphDriverOptions,
        lifecycle_options: DurableGraphLifecycleOptions,
        options: DurableTenantSchedulerOptions,
    ) -> Result<Self, DurableTenantSchedulerBuildError> {
        let agent_loop = DurableAgentLoop::new(
            store.clone(),
            registry,
            evidence,
            driver_options,
            lifecycle_options,
        )?;
        Ok(Self {
            store,
            agent_loop,
            options,
        })
    }

    /// Returns the immutable tenant scan and claim policy.
    #[must_use]
    pub const fn options(&self) -> DurableTenantSchedulerOptions {
        self.options
    }

    /// Selects, claims, and executes at most one run for the supplied tenant.
    ///
    /// A stable attempt ID is allocated once per candidate and retained across
    /// transient database retries, preserving lost-acknowledgement convergence.
    /// Lease contention and candidates that became unavailable are normal scan
    /// skips, not scheduler failures.
    pub fn tick(
        &self,
        tenant_id: TenantId,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<TenantSchedulerTick, TenantSchedulerError>> {
        Box::pin(self.tick_inner(tenant_id, shutdown))
    }

    async fn tick_inner(
        &self,
        tenant_id: TenantId,
        shutdown: CancellationSignal,
    ) -> Result<TenantSchedulerTick, TenantSchedulerError> {
        let mut cursor: Option<RunnableRunPageCursor> = None;
        let mut report = TenantSchedulerReport::default();
        loop {
            if shutdown.is_cancelled() {
                return Ok(TenantSchedulerTick::new(
                    TenantSchedulerOutcome::Cancelled,
                    report,
                ));
            }
            let page = self
                .store
                .load_runnable_run_page(&tenant_id, cursor.as_ref(), self.options.page_size)
                .await?;
            report.pages_scanned = report.pages_scanned.saturating_add(1);
            for candidate in page.records() {
                if shutdown.is_cancelled() {
                    return Ok(TenantSchedulerTick::new(
                        TenantSchedulerOutcome::Cancelled,
                        report,
                    ));
                }
                report.candidates_scanned = report.candidates_scanned.saturating_add(1);
                let run_id = candidate.run().lifecycle().provenance().run_id();
                let attempt_id = AttemptId::generate();
                let Some(lease) = self
                    .claim_with_retry(&tenant_id, run_id, attempt_id, &mut report)
                    .await?
                else {
                    continue;
                };
                let outcome = match self
                    .agent_loop
                    .run(lease.fence().clone(), shutdown.clone())
                    .await
                {
                    Ok(result) => TenantSchedulerOutcome::Executed { run_id, result },
                    Err(error) => TenantSchedulerOutcome::ExecutionFailed { run_id, error },
                };
                return Ok(TenantSchedulerTick::new(outcome, report));
            }

            if !page.has_more() {
                return Ok(TenantSchedulerTick::new(
                    TenantSchedulerOutcome::Idle,
                    report,
                ));
            }
            if report.pages_scanned == self.options.maximum_pages_per_tick {
                return Ok(TenantSchedulerTick::new(
                    TenantSchedulerOutcome::ScanLimitReached,
                    report,
                ));
            }
            cursor = page.next_cursor();
            if cursor.is_none() {
                return Err(TenantSchedulerError::RuntimeInvariant);
            }
        }
    }

    async fn claim_with_retry(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        attempt_id: AttemptId,
        report: &mut TenantSchedulerReport,
    ) -> Result<Option<stateknot_core::RunLease>, TenantSchedulerError> {
        let mut attempt = 1_u8;
        loop {
            match self.store.claim_lease(tenant_id, run_id, attempt_id).await {
                Ok(outcome) => return Ok(Some(outcome.lease().clone())),
                Err(error) if claim_contention(&error) => {
                    report.contention_skips = report.contention_skips.saturating_add(1);
                    return Ok(None);
                }
                Err(error)
                    if attempt < self.options.maximum_claim_attempts && error.is_retryable() =>
                {
                    report.claim_retries = report.claim_retries.saturating_add(1);
                    self.claim_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn claim_backoff(&self, attempt: u8) {
        let multiplier = 1_u32
            .checked_shl(u32::from(attempt.saturating_sub(1)))
            .unwrap_or(u32::MAX);
        let delay = self
            .options
            .claim_retry_initial_delay
            .checked_mul(multiplier)
            .unwrap_or(MAX_CLAIM_RETRY_DELAY)
            .min(MAX_CLAIM_RETRY_DELAY);
        tokio::time::sleep(delay).await;
    }
}

fn claim_contention(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::LeaseHeld
            | StoreError::RunNotRunnable
            | StoreError::RunNotYetAvailable
            | StoreError::RunQuarantined
            | StoreError::RunNotFound
    )
}

/// Startup failure while building a tenant scheduler worker.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableTenantSchedulerBuildError {
    /// The internally consistent durable agent loop could not be built.
    #[error(transparent)]
    AgentLoop(#[from] DurableAgentLoopBuildError),
}

/// Payload-redacted scheduler infrastructure failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TenantSchedulerError {
    /// `PostgreSQL` rejected or could not complete a scan or claim.
    #[error(transparent)]
    Store {
        /// Exact payload-redacted provider failure.
        source: Box<StoreError>,
    },
    /// A provider page claimed continuation without a usable cursor.
    #[error("tenant scheduler stable page chain is internally inconsistent")]
    RuntimeInvariant,
}

impl From<StoreError> for TenantSchedulerError {
    fn from(source: StoreError) -> Self {
        Self::Store {
            source: Box::new(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_enforce_bounded_scan_and_stable_claim_retry_limits() {
        let page = RunnableRunPageSize::new(8).unwrap();
        assert_eq!(
            DurableTenantSchedulerOptions::new(page, 0, 1, Duration::from_millis(1)),
            Err(DurableTenantSchedulerOptionsError::InvalidPageCount)
        );
        assert_eq!(
            DurableTenantSchedulerOptions::new(page, 1, 0, Duration::from_millis(1)),
            Err(DurableTenantSchedulerOptionsError::InvalidClaimAttempts)
        );
        assert_eq!(
            DurableTenantSchedulerOptions::new(page, 1, 1, Duration::ZERO),
            Err(DurableTenantSchedulerOptionsError::InvalidClaimRetryDelay)
        );
        let options =
            DurableTenantSchedulerOptions::new(page, 3, 4, Duration::from_millis(50)).unwrap();
        assert_eq!(options.page_size().get(), 8);
        assert_eq!(options.maximum_pages_per_tick(), 3);
        assert_eq!(options.maximum_claim_attempts(), 4);
    }
}
