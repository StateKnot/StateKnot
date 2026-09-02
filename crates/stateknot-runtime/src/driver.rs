// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Fenced, crash-recoverable execution of registered graph nodes.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};
use stateknot_core::{
    AttemptId, BoundedJson, BoxFuture, BudgetUsage, CancellationObserver, CancellationSignal,
    Checkpoint, CheckpointId, Digest, EventId, Failure, FailureCategory, FailureCode, FailureId,
    FailureMessage, FailureOrigin, GraphBarrierDisposition, GraphBarrierPlan,
    GraphBarrierPlanError, GraphReference, JournalAppend, JournalEventIntent, JournalEventKind,
    JournalExpectation, JournalHead, JournalPayload, NodeAttemptStartHead, PendingNodeResult,
    PendingNodeResultIntent, QuarantineId, ReadyNodeRecoveryPlan, RecoveryNodeKind, RetryAdvice,
    RunFence, RunLease, RunRevision, RunStatus, Timestamp,
};
use stateknot_store_postgres::{
    BarrierCommitOutcome, ClaimedRunRecovery, CorruptionQuarantineContext,
    DelayedRetryScheduleOutcome, GraphReplayLimits, GraphReplayReport, LeaseReleaseOutcome,
    LeaseRenewalOutcome, NodeAttemptCommitOutcome, PendingNodeResultPageSize, PostgresStore,
    RunProjection, StoreError, StoredRun,
};
use thiserror::Error;
use tokio::{
    sync::Notify,
    task::{JoinError, JoinHandle},
    time::Instant,
};

use crate::{
    ExecutableGraph, ExecutableGraphRegistry, GraphNodeContext, GraphNodeContextError,
    GraphNodeExecution, GraphNodeExecutionError, StandardGraphDriverSchemaError,
    standard_graph_driver_event_schema,
};

const MAX_NODE_TIMEOUT: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const MAX_MUTATION_RETRY_DELAY: Duration = Duration::from_secs(1);
const RECOVERY_EVIDENCE_DOMAIN: &[u8] = b"stateknot/runtime/graph-driver/recovery/v1\0";

/// Resource, lease, deadline, and retry policy for one durable driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableGraphDriverOptions {
    replay_limits: GraphReplayLimits,
    maximum_durable_events: u32,
    lease_renewal_interval: Duration,
    node_execution_timeout: Duration,
    cancellation_poll_interval: Duration,
    cancellation_grace_period: Duration,
    maximum_mutation_attempts: u8,
    mutation_retry_initial_delay: Duration,
}

impl DurableGraphDriverOptions {
    /// Absolute work quantum accepted for one call to [`DurableGraphDriver::drive`].
    pub const HARD_MAXIMUM_DURABLE_EVENTS: u32 = 65_536;
    /// Absolute number of identical durable mutation attempts.
    pub const HARD_MAXIMUM_MUTATION_ATTEMPTS: u8 = 10;
    /// Fastest accepted database polling cadence for durable cancellation intent.
    pub const HARD_MINIMUM_CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(10);
    /// Slowest accepted observation cadence for durable cancellation intent.
    pub const HARD_MAXIMUM_CANCELLATION_POLL_INTERVAL: Duration = Duration::from_secs(60);
    /// Longest cooperative cleanup window after durable cancellation wins.
    pub const HARD_MAXIMUM_CANCELLATION_GRACE_PERIOD: Duration = Duration::from_secs(5 * 60);

    /// Constructs a fully explicit driver policy.
    ///
    /// # Errors
    ///
    /// Rejects a work quantum below one complete node start/completion pair,
    /// invalid renewal precision, a zero or excessive node deadline, and unsafe
    /// mutation retry parameters.
    pub fn new(
        replay_limits: GraphReplayLimits,
        maximum_durable_events: u32,
        lease_renewal_interval: Duration,
        node_execution_timeout: Duration,
        maximum_mutation_attempts: u8,
        mutation_retry_initial_delay: Duration,
    ) -> Result<Self, DurableGraphDriverOptionsError> {
        if !(2..=Self::HARD_MAXIMUM_DURABLE_EVENTS).contains(&maximum_durable_events) {
            return Err(DurableGraphDriverOptionsError::InvalidWorkQuantum);
        }
        if lease_renewal_interval.is_zero()
            || lease_renewal_interval.subsec_nanos() % 1_000 != 0
            || lease_renewal_interval.as_micros() > i64::MAX as u128
        {
            return Err(DurableGraphDriverOptionsError::InvalidLeaseRenewalInterval);
        }
        if node_execution_timeout.is_zero() || node_execution_timeout > MAX_NODE_TIMEOUT {
            return Err(DurableGraphDriverOptionsError::InvalidNodeExecutionTimeout);
        }
        if !(1..=Self::HARD_MAXIMUM_MUTATION_ATTEMPTS).contains(&maximum_mutation_attempts) {
            return Err(DurableGraphDriverOptionsError::InvalidMutationAttempts);
        }
        if mutation_retry_initial_delay.is_zero()
            || mutation_retry_initial_delay > MAX_MUTATION_RETRY_DELAY
        {
            return Err(DurableGraphDriverOptionsError::InvalidMutationRetryDelay);
        }
        Ok(Self {
            replay_limits,
            maximum_durable_events,
            lease_renewal_interval,
            node_execution_timeout,
            cancellation_poll_interval: Duration::from_millis(250),
            cancellation_grace_period: Duration::from_secs(5),
            maximum_mutation_attempts,
            mutation_retry_initial_delay,
        })
    }

    /// Overrides the database cancellation-observation cadence and cooperative
    /// node cleanup window.
    ///
    /// # Errors
    ///
    /// Rejects non-microsecond-aligned or production-unbounded values. The poll
    /// interval is also bounded below so one bad configuration cannot turn each
    /// active node into an unbounded database query loop.
    pub fn with_cancellation_timing(
        mut self,
        poll_interval: Duration,
        grace_period: Duration,
    ) -> Result<Self, DurableGraphDriverOptionsError> {
        if poll_interval < Self::HARD_MINIMUM_CANCELLATION_POLL_INTERVAL
            || poll_interval.subsec_nanos() % 1_000 != 0
            || poll_interval > Self::HARD_MAXIMUM_CANCELLATION_POLL_INTERVAL
        {
            return Err(DurableGraphDriverOptionsError::InvalidCancellationPollInterval);
        }
        if grace_period.is_zero()
            || grace_period.subsec_nanos() % 1_000 != 0
            || grace_period > Self::HARD_MAXIMUM_CANCELLATION_GRACE_PERIOD
        {
            return Err(DurableGraphDriverOptionsError::InvalidCancellationGracePeriod);
        }
        self.cancellation_poll_interval = poll_interval;
        self.cancellation_grace_period = grace_period;
        Ok(self)
    }

    /// Returns the memory ceiling shared by historical replay and current
    /// barrier materialization.
    #[must_use]
    pub const fn replay_limits(self) -> GraphReplayLimits {
        self.replay_limits
    }

    /// Returns the maximum driver-owned durable events in one drive call.
    #[must_use]
    pub const fn maximum_durable_events(self) -> u32 {
        self.maximum_durable_events
    }

    /// Returns the monotonic interval between renewal attempts.
    #[must_use]
    pub const fn lease_renewal_interval(self) -> Duration {
        self.lease_renewal_interval
    }

    /// Returns the hard wall-clock deadline for one node task.
    #[must_use]
    pub const fn node_execution_timeout(self) -> Duration {
        self.node_execution_timeout
    }

    /// Returns the bounded database polling cadence for cancellation intent.
    #[must_use]
    pub const fn cancellation_poll_interval(self) -> Duration {
        self.cancellation_poll_interval
    }

    /// Returns the cooperative cleanup window after cancellation is observed.
    #[must_use]
    pub const fn cancellation_grace_period(self) -> Duration {
        self.cancellation_grace_period
    }

    /// Returns the maximum identical attempts for one idempotent mutation.
    #[must_use]
    pub const fn maximum_mutation_attempts(self) -> u8 {
        self.maximum_mutation_attempts
    }

    /// Returns the first exponential mutation retry delay.
    #[must_use]
    pub const fn mutation_retry_initial_delay(self) -> Duration {
        self.mutation_retry_initial_delay
    }
}

impl Default for DurableGraphDriverOptions {
    fn default() -> Self {
        Self {
            replay_limits: GraphReplayLimits::default(),
            maximum_durable_events: 1_024,
            lease_renewal_interval: Duration::from_secs(10),
            node_execution_timeout: Duration::from_secs(15 * 60),
            cancellation_poll_interval: Duration::from_millis(250),
            cancellation_grace_period: Duration::from_secs(5),
            maximum_mutation_attempts: 3,
            mutation_retry_initial_delay: Duration::from_millis(25),
        }
    }
}

/// Invalid durable driver options.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableGraphDriverOptionsError {
    /// A call could not durably start and finish one node, or was unbounded.
    #[error("graph driver durable-event quantum is outside its production bounds")]
    InvalidWorkQuantum,
    /// Renewal timing was zero, not microsecond aligned, or unrepresentable.
    #[error("graph driver lease renewal interval is invalid")]
    InvalidLeaseRenewalInterval,
    /// Node execution had no deadline or exceeded seven days.
    #[error("graph driver node execution timeout is invalid")]
    InvalidNodeExecutionTimeout,
    /// Cancellation polling was imprecise, faster than 10 ms, or slower than one minute.
    #[error("graph driver cancellation poll interval is invalid")]
    InvalidCancellationPollInterval,
    /// Cooperative cancellation cleanup was zero, imprecise, or above five minutes.
    #[error("graph driver cancellation grace period is invalid")]
    InvalidCancellationGracePeriod,
    /// Mutation attempts were zero or above the hard ceiling.
    #[error("graph driver mutation attempt count is invalid")]
    InvalidMutationAttempts,
    /// Initial mutation backoff was zero or above one second.
    #[error("graph driver mutation retry delay is invalid")]
    InvalidMutationRetryDelay,
}

/// Fenced durable graph execution service over one `PostgreSQL` provider pool.
#[derive(Clone)]
pub struct DurableGraphDriver {
    store: PostgresStore,
    registry: ExecutableGraphRegistry,
    journal_schema: stateknot_core::SchemaReference,
    options: DurableGraphDriverOptions,
}

impl DurableGraphDriver {
    /// Binds one provider pool and frozen executable registry.
    ///
    /// The standard event schema must have been installed before the schema
    /// registry was frozen. Renewal cadence must fit at least three times in a
    /// newly acquired lease, preserving time for two delayed renewal cycles.
    ///
    /// # Errors
    ///
    /// Returns [`DurableGraphDriverBuildError`] for an invalid release schema,
    /// absent journal schema, or unsafe lease cadence.
    pub fn new(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
        options: DurableGraphDriverOptions,
    ) -> Result<Self, DurableGraphDriverBuildError> {
        let (journal_schema, _) = standard_graph_driver_event_schema()?;
        if !registry.schemas().contains(&journal_schema) {
            return Err(DurableGraphDriverBuildError::JournalSchemaUnavailable);
        }
        let lease_duration = store.options().lease_duration();
        let renewal_budget = options
            .lease_renewal_interval
            .checked_mul(3)
            .ok_or(DurableGraphDriverBuildError::UnsafeLeaseRenewalCadence)?;
        if renewal_budget > lease_duration
            || options.lease_renewal_interval > store.options().maximum_lease_horizon()
        {
            return Err(DurableGraphDriverBuildError::UnsafeLeaseRenewalCadence);
        }
        Ok(Self {
            store,
            registry,
            journal_schema,
            options,
        })
    }

    /// Returns the immutable execution policy.
    #[must_use]
    pub const fn options(&self) -> DurableGraphDriverOptions {
        self.options
    }

    /// Replays and drives one already-claimed exact worker fence.
    ///
    /// Every node start commits before node code is spawned. An idempotently
    /// observed start is never launched. The node task holds no database
    /// transaction, receives a monotonic cancellation signal, and is aborted
    /// if renewal, shutdown, or its deadline wins. Successful and failed node
    /// completions use the latest exact journal head so invocation-ledger facts
    /// committed by the node remain ordered before its result.
    ///
    /// Continue barriers commit automatically. Wait and successful-terminal
    /// barriers, plus node failure/exhaustion, return a lease-bound handoff:
    /// their complete registration, admitted result, or cumulative failure
    /// metadata belongs to the lifecycle integration layer and is never guessed
    /// by this driver.
    ///
    /// # Errors
    ///
    /// Returns explicit storage, fencing, executable-deployment, schema,
    /// planning, resource, or runtime invariant failures. Durable corruption is
    /// quarantined by the claimed recovery session before it reaches this API.
    pub fn drive(
        &self,
        fence: RunFence,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<GraphDriveResult, GraphDriverError>> {
        Box::pin(self.drive_inner(fence, shutdown))
    }

    #[allow(clippy::too_many_lines)]
    async fn drive_inner(
        &self,
        fence: RunFence,
        shutdown: CancellationSignal,
    ) -> Result<GraphDriveResult, GraphDriverError> {
        // Recovery, graph loading, and replay are bounded but may still consume
        // a meaningful fraction of a short lease. Refresh below half-life
        // before those reads so a valid near-expiry claim is not lost merely
        // because integrity verification ran before the first durable start.
        let (startup_renewals, startup_retries) = self.refresh_near_expiry_lease(&fence).await?;
        let initial_recovery = self.begin_recovery(&fence).await?;
        let stored_graph = initial_recovery.load_pinned_graph().await?;
        let graph_reference = stored_graph.graph().reference();
        let executable = self
            .registry
            .resolve(&graph_reference)
            .cloned()
            .ok_or_else(|| GraphDriverError::ExecutableGraphUnavailable {
                graph: Box::new(graph_reference),
            })?;
        let replay = initial_recovery
            .validate_noninitial_replay(
                executable.schemas(),
                executable.reducer(),
                self.options.replay_limits,
            )
            .await?;
        drop(initial_recovery);

        let mut report = GraphDriveReport::new(replay);
        report.lease_renewals = startup_renewals;
        report.mutation_retries = startup_retries;
        loop {
            if shutdown.is_cancelled() {
                let release = self.release_with_retry(&fence, &mut report).await?;
                return Ok(GraphDriveResult::new(
                    GraphDriveOutcome::Cancelled { release },
                    report,
                ));
            }
            if report.durable_events >= self.options.maximum_durable_events {
                let release = self.release_with_retry(&fence, &mut report).await?;
                return Ok(GraphDriveResult::new(
                    GraphDriveOutcome::Yielded { release },
                    report,
                ));
            }

            let recovery = self.begin_recovery(&fence).await?;
            let plan = recovery.plan_ready_nodes().await?;
            let live = recovery.revalidate().await?;
            if live.lifecycle().status() == RunStatus::CancellationRequested {
                let lease = exact_live_lease(&live, &fence)?.clone();
                let request = live.lifecycle().cancellation_request().ok_or(
                    GraphDriverError::RuntimeInvariant {
                        operation: "recover durable cancellation intent",
                    },
                )?;
                let checkpoint = plan.checkpoint().head();
                let pointer = live
                    .checkpoint()
                    .ok_or(GraphDriverError::RuntimeInvariant {
                        operation: "bind cancellation to the current checkpoint",
                    })?;
                if pointer.checkpoint_id() != checkpoint.checkpoint_id()
                    || pointer.superstep() != checkpoint.superstep()
                    || pointer.digest() != checkpoint.digest()
                    || live.journal_head() != Some(plan.journal_head())
                {
                    return Err(GraphDriverError::StaleBarrierSnapshot);
                }
                return Ok(GraphDriveResult::new(
                    GraphDriveOutcome::CancellationRequested(Box::new(GraphCancellationHandoff {
                        checkpoint,
                        journal_head: plan.journal_head().clone(),
                        event_id: EventId::generate(),
                        expected_revision: live.lifecycle().revision(),
                        lease,
                        failure_id: request.failure().id(),
                    })),
                    report,
                ));
            }
            let blockers = GraphDriveBlockers::from_plan(&plan);
            if !blockers.is_empty() {
                let lease = exact_live_lease(&live, &fence)?.clone();
                return Ok(GraphDriveResult::new(
                    GraphDriveOutcome::Blocked(Box::new(GraphBlockedHandoff {
                        plan,
                        lease,
                        event_id: EventId::generate(),
                        expected_revision: live.lifecycle().revision(),
                        blockers,
                    })),
                    report,
                ));
            }

            if plan.is_barrier_ready() {
                if report.durable_events == self.options.maximum_durable_events {
                    drop(recovery);
                    let release = self.release_with_retry(&fence, &mut report).await?;
                    return Ok(GraphDriveResult::new(
                        GraphDriveOutcome::Yielded { release },
                        report,
                    ));
                }
                let results = self.load_barrier_results(&recovery, &plan).await?;
                let barrier_plan = executable
                    .graph()
                    .plan_barrier(
                        plan.checkpoint(),
                        &results,
                        CheckpointId::generate(),
                        executable.schemas(),
                        executable.reducer(),
                    )
                    .map_err(GraphDriverError::graph_plan)?;
                let lease = exact_live_lease(&live, &fence)?.clone();
                match barrier_plan.disposition() {
                    GraphBarrierDisposition::Continue => {
                        drop(recovery);
                        self.commit_continue_barrier(&fence, &plan, barrier_plan, &mut report)
                            .await?;
                    }
                    GraphBarrierDisposition::Wait { .. }
                    | GraphBarrierDisposition::Terminal { .. } => {
                        return Ok(GraphDriveResult::new(
                            GraphDriveOutcome::LifecycleBarrierReady(Box::new(
                                GraphLifecycleBarrierHandoff {
                                    plan: barrier_plan,
                                    journal_head: plan.journal_head().clone(),
                                    event_id: EventId::generate(),
                                    expected_revision: live.lifecycle().revision(),
                                    lease,
                                },
                            )),
                            report,
                        ));
                    }
                    _ => {
                        return Err(GraphDriverError::RuntimeInvariant {
                            operation: "handle unsupported graph barrier disposition",
                        });
                    }
                }
                continue;
            }

            if let Some(not_before) = plan.earliest_deferred_at() {
                if plan.nodes().iter().all(|node| {
                    matches!(
                        node.kind(),
                        RecoveryNodeKind::Completed | RecoveryNodeKind::Deferred
                    )
                }) {
                    let outcome = self
                        .schedule_deferred_with_retry(&plan, &mut report)
                        .await?;
                    if matches!(outcome, DelayedRetryScheduleOutcome::Due { .. }) {
                        continue;
                    }
                    return Ok(GraphDriveResult::new(
                        GraphDriveOutcome::Deferred {
                            not_before,
                            schedule: outcome,
                        },
                        report,
                    ));
                }
            }

            if report.durable_events.saturating_add(2) > self.options.maximum_durable_events {
                drop(recovery);
                let release = self.release_with_retry(&fence, &mut report).await?;
                return Ok(GraphDriveResult::new(
                    GraphDriveOutcome::Yielded { release },
                    report,
                ));
            }
            let node = plan
                .nodes()
                .iter()
                .find(|node| node.kind() == RecoveryNodeKind::Dispatchable)
                .ok_or(GraphDriverError::RuntimeInvariant {
                    operation: "select dispatchable graph node",
                })?;
            let node_id = node.activation().node_id().clone();
            let executor = executable.node_executor(&node_id).ok_or_else(|| {
                GraphDriverError::ExecutableNodeUnavailable {
                    graph: Box::new(executable.graph().reference()),
                    node_id: node_id.clone(),
                }
            })?;
            let attempt_id = AttemptId::generate();
            let start_event_id = EventId::generate();
            let payload = self.node_started_payload(
                &executable.graph().reference(),
                plan.checkpoint(),
                &node_id,
                attempt_id,
            )?;
            let append =
                worker_append(&fence, plan.journal_head().clone(), start_event_id, payload)?;
            let start = self
                .start_node_with_retry(append, &plan, &node_id, attempt_id, &mut report)
                .await?;
            let start_head = match start {
                NodeAttemptCommitOutcome::Committed { event: _, attempt } => {
                    report.durable_events = report.durable_events.saturating_add(1);
                    report.node_attempts_started = report.node_attempts_started.saturating_add(1);
                    attempt.start().head()
                }
                NodeAttemptCommitOutcome::Idempotent { .. } => {
                    // Lost acknowledgement convergence never grants launch authority.
                    report.durable_events = report.durable_events.saturating_add(1);
                    continue;
                }
                _ => {
                    return Err(GraphDriverError::RuntimeInvariant {
                        operation: "handle unsupported node attempt start outcome",
                    });
                }
            };
            let checkpoint = Arc::new(plan.checkpoint().clone());
            drop(recovery);

            // A recovery/dispatch pass may have consumed most of the lease that
            // was originally acquired. Re-observe the database clock only after
            // the durable start commits, then restore a full configured lease
            // before node code can perform external work. If this preflight
            // fails, the committed start remains available for fenced takeover
            // and the executor is never launched under an unsafe lease margin.
            let current_lease = self.prepare_execution_lease(&fence, &mut report).await?;

            let execution = self
                .execute_started_node(
                    executor,
                    start_head.clone(),
                    checkpoint,
                    current_lease,
                    shutdown.clone(),
                    &mut report,
                )
                .await?;
            match execution {
                StartedNodeExecution::Cancelled => {
                    let release = self.release_with_retry(&fence, &mut report).await?;
                    return Ok(GraphDriveResult::new(
                        GraphDriveOutcome::Cancelled { release },
                        report,
                    ));
                }
                StartedNodeExecution::RunCancellationObserved => continue,
                StartedNodeExecution::Finished(result) => {
                    match self
                        .commit_node_execution(
                            &fence,
                            &executable,
                            &start_head,
                            result,
                            &mut report,
                        )
                        .await?
                    {
                        NodeExecutionCommit::Committed => {}
                        NodeExecutionCommit::RunCancellationObserved => continue,
                    }
                }
            }
        }
    }

    async fn begin_recovery<'driver>(
        &'driver self,
        fence: &RunFence,
    ) -> Result<ClaimedRunRecovery<'driver>, GraphDriverError> {
        let run = self
            .store
            .load_run(fence.tenant_id(), fence.run_id())
            .await?;
        exact_live_lease(&run, fence)?;
        let head = run
            .journal_head()
            .cloned()
            .ok_or(GraphDriverError::MissingJournalHead)?;
        let evidence_digest = recovery_evidence_digest(&run, fence)?;
        let context = CorruptionQuarantineContext::new(
            fence.tenant_id().clone(),
            fence.run_id(),
            QuarantineId::generate(),
            JournalExpectation::exact(head),
            evidence_digest,
        )?;
        self.store
            .begin_claimed_run_recovery(fence.clone(), context)
            .await
            .map_err(GraphDriverError::from)
    }

    async fn refresh_near_expiry_lease(
        &self,
        fence: &RunFence,
    ) -> Result<(u32, u32), GraphDriverError> {
        let observation = self.store.observe_live_lease(fence).await?;
        let remaining_micros = observation
            .lease()
            .expires_at()
            .unix_micros()
            .checked_sub(observation.observed_at().unix_micros())
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(StoreError::LeaseExpired)?;
        let remaining = Duration::from_micros(remaining_micros);
        if remaining > self.store.options().lease_duration() / 2 {
            return Ok((0, 0));
        }

        let desired = extend_timestamp(
            observation.observed_at(),
            self.store.options().lease_duration(),
        )?;
        let mut attempt = 1_u8;
        let mut retries = 0_u32;
        loop {
            match self.store.renew_lease(fence, desired).await {
                Ok(LeaseRenewalOutcome::Renewed(_) | LeaseRenewalOutcome::Idempotent(_)) => {
                    return Ok((1, retries));
                }
                Ok(_) => {
                    return Err(GraphDriverError::RuntimeInvariant {
                        operation: "refresh a near-expiry drive lease",
                    });
                }
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    retries = retries.saturating_add(1);
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn load_barrier_results(
        &self,
        recovery: &ClaimedRunRecovery<'_>,
        plan: &ReadyNodeRecoveryPlan,
    ) -> Result<Vec<PendingNodeResult>, GraphDriverError> {
        let page_size = PendingNodeResultPageSize::new(PendingNodeResultPageSize::MAX)?;
        let mut cursor = None;
        let mut results = Vec::with_capacity(plan.nodes().len());
        let mut compact_bytes = 0_usize;
        loop {
            let page = recovery
                .load_unconsumed_pending_node_result_page(
                    &plan.checkpoint().head(),
                    cursor.as_ref(),
                    page_size,
                )
                .await?;
            if page.snapshot_journal_head() != plan.journal_head() {
                return Err(GraphDriverError::StaleBarrierSnapshot);
            }
            let next = if page.has_more() {
                Some(
                    page.next_cursor()
                        .ok_or(GraphDriverError::RuntimeInvariant {
                            operation: "advance graph barrier result cursor",
                        })?,
                )
            } else {
                None
            };
            for result in page.records() {
                let mut counter = CompactByteCounter::default();
                serde_json::to_writer(&mut counter, result)
                    .map_err(|_| GraphDriverError::BarrierResultEncoding)?;
                compact_bytes = compact_bytes
                    .checked_add(counter.bytes)
                    .ok_or(GraphDriverError::BarrierResultResourceLimit)?;
                if compact_bytes > self.options.replay_limits.maximum_barrier_result_bytes() {
                    return Err(GraphDriverError::BarrierResultResourceLimit);
                }
            }
            results.extend(page.into_records());
            let Some(next) = next else {
                break;
            };
            cursor = Some(next);
        }
        if results.len() != plan.nodes().len() {
            return Err(GraphDriverError::RuntimeInvariant {
                operation: "materialize complete graph barrier result set",
            });
        }
        Ok(results)
    }

    async fn commit_continue_barrier(
        &self,
        fence: &RunFence,
        ready_plan: &ReadyNodeRecoveryPlan,
        barrier_plan: GraphBarrierPlan,
        report: &mut GraphDriveReport,
    ) -> Result<(), GraphDriverError> {
        if !matches!(
            barrier_plan.disposition(),
            GraphBarrierDisposition::Continue
        ) {
            return Err(GraphDriverError::RuntimeInvariant {
                operation: "commit non-continue graph barrier",
            });
        }
        let payload = self.barrier_continued_payload(
            ready_plan.checkpoint().graph(),
            ready_plan.checkpoint(),
            barrier_plan.barrier().successor().checkpoint_id(),
            barrier_plan.barrier().successor().superstep(),
        )?;
        let append = worker_append(
            fence,
            ready_plan.journal_head().clone(),
            EventId::generate(),
            payload,
        )?;
        let barrier = barrier_plan.barrier().clone();
        let mut attempt = 1_u8;
        loop {
            let result = self
                .store
                .append_worker_barrier(append.clone(), RunProjection::unchanged(), barrier.clone())
                .await;
            match result {
                Ok(
                    BarrierCommitOutcome::Committed { .. }
                    | BarrierCommitOutcome::Idempotent { .. },
                ) => {
                    report.durable_events = report.durable_events.saturating_add(1);
                    report.barriers_committed = report.barriers_committed.saturating_add(1);
                    return Ok(());
                }
                Ok(_) => {
                    return Err(GraphDriverError::RuntimeInvariant {
                        operation: "handle unsupported graph barrier commit outcome",
                    });
                }
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    report.mutation_retries = report.mutation_retries.saturating_add(1);
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn execute_started_node(
        &self,
        executor: Arc<dyn crate::GraphNodeExecutor>,
        start: NodeAttemptStartHead,
        checkpoint: Arc<Checkpoint>,
        lease: GuardedRunLease,
        shutdown: CancellationSignal,
        report: &mut GraphDriveReport,
    ) -> Result<StartedNodeExecution, GraphDriverError> {
        if shutdown.is_cancelled() {
            return Ok(StartedNodeExecution::Cancelled);
        }
        let cancellation = DriverCancellation::new();
        let context = GraphNodeContext::new(start, checkpoint, cancellation.signal())?;
        let mut task = tokio::spawn(async move { executor.execute(context).await });
        let mut timeout = Box::pin(tokio::time::sleep(self.options.node_execution_timeout));
        let fence = lease.lease.fence().clone();
        let lease_deadline = lease.deadline;
        let maintenance = self.maintain_execution_lease(lease, report);
        tokio::pin!(maintenance);
        let durable_cancellation = self.observe_run_cancellation(&fence);
        tokio::pin!(durable_cancellation);

        tokio::select! {
            result = &mut task => {
                match result {
                    Ok(result) => Ok(StartedNodeExecution::Finished(result)),
                    Err(source) => Ok(StartedNodeExecution::Finished(Err(
                        Self::node_task_failure(source)?,
                    ))),
                }
            }
            result = &mut maintenance => {
                cancellation.cancel();
                abort_node_task(&mut task).await;
                match result {
                    Ok(()) => Err(GraphDriverError::RuntimeInvariant {
                        operation: "maintain a live node execution lease",
                    }),
                    Err(error) => Err(error),
                }
            }
            () = shutdown.cancelled() => {
                cancellation.cancel();
                abort_node_task(&mut task).await;
                Ok(StartedNodeExecution::Cancelled)
            }
            result = &mut durable_cancellation => {
                cancellation.cancel();
                if let Err(error) = result {
                    abort_node_task(&mut task).await;
                    return Err(error);
                }
                let grace_deadline = Instant::now()
                    .checked_add(self.options.cancellation_grace_period)
                    .unwrap_or(lease_deadline)
                    .min(lease_deadline);
                if grace_deadline <= Instant::now() {
                    abort_node_task(&mut task).await;
                } else {
                    tokio::select! {
                        _ = &mut task => {}
                        () = tokio::time::sleep_until(grace_deadline) => {
                            abort_node_task(&mut task).await;
                        }
                    }
                }
                Ok(StartedNodeExecution::RunCancellationObserved)
            }
            () = &mut timeout => {
                cancellation.cancel();
                abort_node_task(&mut task).await;
                Ok(StartedNodeExecution::Finished(Err(
                    Self::node_timeout_failure()?,
                )))
            }
        }
    }

    async fn observe_run_cancellation(&self, fence: &RunFence) -> Result<(), GraphDriverError> {
        loop {
            let run = self
                .store
                .load_run(fence.tenant_id(), fence.run_id())
                .await?;
            exact_live_lease(&run, fence)?;
            match run.lifecycle().status() {
                RunStatus::CancellationRequested => return Ok(()),
                RunStatus::Pending | RunStatus::Active => {}
                _ => return Err(StoreError::RunNotRunnable.into()),
            }
            tokio::time::sleep(self.options.cancellation_poll_interval).await;
        }
    }

    async fn prepare_execution_lease(
        &self,
        fence: &RunFence,
        report: &mut GraphDriveReport,
    ) -> Result<GuardedRunLease, GraphDriverError> {
        let observation_started = Instant::now();
        let recovery = self.begin_recovery(fence).await?;
        let lease = exact_live_lease(recovery.initial_run(), fence)?.clone();
        let observed_at = recovery.initial_observed_at();
        let desired = extend_timestamp(observed_at, self.store.options().lease_duration())?;
        drop(recovery);

        if desired <= lease.expires_at() {
            return guarded_run_lease(lease, observed_at, observation_started);
        }
        let lease = self.renew_with_retry(fence, desired, report).await?;
        report.lease_renewals = report.lease_renewals.saturating_add(1);
        Ok(lease)
    }

    async fn maintain_execution_lease(
        &self,
        mut lease: GuardedRunLease,
        report: &mut GraphDriveReport,
    ) -> Result<(), GraphDriverError> {
        loop {
            let renewal_due = Instant::now()
                .checked_add(self.options.lease_renewal_interval)
                .ok_or(GraphDriverError::LeaseTimestampOverflow)?;
            tokio::select! {
                () = tokio::time::sleep_until(renewal_due) => {}
                () = tokio::time::sleep_until(lease.deadline) => {
                    return Err(StoreError::LeaseExpired.into());
                }
            }

            let desired = extend_timestamp(
                lease.lease.expires_at(),
                self.options.lease_renewal_interval,
            )?;
            let fence = lease.lease.fence().clone();
            let deadline = lease.deadline;
            let renewed = {
                let renewal = self.renew_with_retry(&fence, desired, report);
                tokio::pin!(renewal);
                tokio::select! {
                    result = &mut renewal => result?,
                    () = tokio::time::sleep_until(deadline) => {
                        return Err(StoreError::LeaseExpired.into());
                    }
                }
            };
            lease = renewed;
            report.lease_renewals = report.lease_renewals.saturating_add(1);
        }
    }

    async fn commit_node_execution(
        &self,
        fence: &RunFence,
        executable: &ExecutableGraph,
        start: &NodeAttemptStartHead,
        execution: Result<GraphNodeExecution, GraphNodeExecutionError>,
        report: &mut GraphDriveReport,
    ) -> Result<NodeExecutionCommit, GraphDriverError> {
        let run = self
            .store
            .load_run(fence.tenant_id(), fence.run_id())
            .await?;
        exact_live_lease(&run, fence)?;
        if run.lifecycle().status() == RunStatus::CancellationRequested {
            return Ok(NodeExecutionCommit::RunCancellationObserved);
        }
        let head = run
            .journal_head()
            .cloned()
            .ok_or(GraphDriverError::MissingJournalHead)?;
        match execution {
            Ok(execution) => {
                let (state_change, control, bindings, usage) = execution.into_parts();
                let intent = PendingNodeResultIntent::new(
                    start.activation().clone(),
                    state_change,
                    control,
                    bindings,
                )
                .map_err(|_| GraphDriverError::InvalidNodeResult)?;
                let payload = self.node_succeeded_payload(
                    &executable.graph().reference(),
                    start,
                    intent.intent_digest(),
                )?;
                let append = worker_append(fence, head, EventId::generate(), payload)?;
                if let Err(error) = self
                    .succeed_node_with_retry(append, start, intent, usage, report)
                    .await
                {
                    if graph_error_is_run_not_runnable(&error)
                        && self.run_cancellation_requested(fence).await?
                    {
                        return Ok(NodeExecutionCommit::RunCancellationObserved);
                    }
                    return Err(error);
                }
            }
            Err(execution) => {
                let (failure, usage) = execution.into_parts();
                let event_id = EventId::generate();
                let payload =
                    self.node_failed_payload(&executable.graph().reference(), start, failure.id())?;
                let append = worker_append(fence, head, event_id, payload)?;
                let failure = failure.with_caused_by_event(event_id);
                if let Err(error) = self
                    .fail_node_with_retry(append, start, failure, usage, report)
                    .await
                {
                    if graph_error_is_run_not_runnable(&error)
                        && self.run_cancellation_requested(fence).await?
                    {
                        return Ok(NodeExecutionCommit::RunCancellationObserved);
                    }
                    return Err(error);
                }
            }
        }
        report.durable_events = report.durable_events.saturating_add(1);
        report.node_attempts_completed = report.node_attempts_completed.saturating_add(1);
        Ok(NodeExecutionCommit::Committed)
    }

    async fn run_cancellation_requested(&self, fence: &RunFence) -> Result<bool, GraphDriverError> {
        let run = self
            .store
            .load_run(fence.tenant_id(), fence.run_id())
            .await?;
        exact_live_lease(&run, fence)?;
        Ok(run.lifecycle().status() == RunStatus::CancellationRequested)
    }

    async fn start_node_with_retry(
        &self,
        append: JournalAppend,
        plan: &ReadyNodeRecoveryPlan,
        node_id: &stateknot_core::NodeId,
        attempt_id: AttemptId,
        report: &mut GraphDriveReport,
    ) -> Result<NodeAttemptCommitOutcome, GraphDriverError> {
        let mut attempt = 1_u8;
        loop {
            match self
                .store
                .start_recovered_node_attempt(append.clone(), plan, node_id, attempt_id)
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    report.mutation_retries = report.mutation_retries.saturating_add(1);
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn succeed_node_with_retry(
        &self,
        append: JournalAppend,
        start: &NodeAttemptStartHead,
        intent: PendingNodeResultIntent,
        usage: BudgetUsage,
        report: &mut GraphDriveReport,
    ) -> Result<NodeAttemptCommitOutcome, GraphDriverError> {
        let mut attempt = 1_u8;
        loop {
            match self
                .store
                .succeed_node_attempt(append.clone(), start, intent.clone(), usage.clone())
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    report.mutation_retries = report.mutation_retries.saturating_add(1);
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn fail_node_with_retry(
        &self,
        append: JournalAppend,
        start: &NodeAttemptStartHead,
        failure: Failure,
        usage: BudgetUsage,
        report: &mut GraphDriveReport,
    ) -> Result<NodeAttemptCommitOutcome, GraphDriverError> {
        let mut attempt = 1_u8;
        loop {
            match self
                .store
                .fail_node_attempt(append.clone(), start, failure.clone(), usage.clone())
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    report.mutation_retries = report.mutation_retries.saturating_add(1);
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn renew_with_retry(
        &self,
        fence: &RunFence,
        desired: Timestamp,
        report: &mut GraphDriveReport,
    ) -> Result<GuardedRunLease, GraphDriverError> {
        let renewal_started = Instant::now();
        let mut attempt = 1_u8;
        loop {
            match self.store.renew_lease(fence, desired).await {
                Ok(LeaseRenewalOutcome::Renewed(lease)) => {
                    let renewed_at = lease.renewed_at();
                    return guarded_run_lease(lease, renewed_at, renewal_started);
                }
                Ok(LeaseRenewalOutcome::Idempotent(_)) => {
                    let observation_started = Instant::now();
                    let observation = self.store.observe_live_lease(fence).await?;
                    let (lease, observed_at) = observation.into_parts();
                    return guarded_run_lease(lease, observed_at, observation_started);
                }
                Ok(_) => {
                    return Err(GraphDriverError::RuntimeInvariant {
                        operation: "handle a lease renewal outcome",
                    });
                }
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    report.mutation_retries = report.mutation_retries.saturating_add(1);
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn schedule_deferred_with_retry(
        &self,
        plan: &ReadyNodeRecoveryPlan,
        report: &mut GraphDriveReport,
    ) -> Result<DelayedRetryScheduleOutcome, GraphDriverError> {
        let mut attempt = 1_u8;
        loop {
            match self.store.schedule_delayed_retry_wakeup(plan).await {
                Ok(outcome) => return Ok(outcome),
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    report.mutation_retries = report.mutation_retries.saturating_add(1);
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn release_with_retry(
        &self,
        fence: &RunFence,
        report: &mut GraphDriveReport,
    ) -> Result<LeaseReleaseOutcome, GraphDriverError> {
        let mut attempt = 1_u8;
        loop {
            match self.store.release_lease(fence).await {
                Ok(outcome) => return Ok(outcome),
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    report.mutation_retries = report.mutation_retries.saturating_add(1);
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn can_retry_mutation(&self, error: &StoreError, attempt: u8) -> bool {
        attempt < self.options.maximum_mutation_attempts && error.is_retryable()
    }

    async fn mutation_backoff(&self, attempt: u8) {
        let multiplier = 1_u32
            .checked_shl(u32::from(attempt.saturating_sub(1)))
            .unwrap_or(u32::MAX);
        let delay = self
            .options
            .mutation_retry_initial_delay
            .checked_mul(multiplier)
            .unwrap_or(MAX_MUTATION_RETRY_DELAY)
            .min(MAX_MUTATION_RETRY_DELAY);
        tokio::time::sleep(delay).await;
    }

    fn node_started_payload(
        &self,
        graph: &GraphReference,
        checkpoint: &Checkpoint,
        node_id: &stateknot_core::NodeId,
        attempt_id: AttemptId,
    ) -> Result<JournalPayload, GraphDriverError> {
        self.event_payload(
            "graph-node-attempt-started",
            json!({
                "operation": "node_attempt_started",
                "graph_digest": digest_hex(graph.definition_digest()),
                "checkpoint_id": checkpoint.checkpoint_id().to_string(),
                "superstep": checkpoint.superstep().get().to_string(),
                "node_id": node_id.as_str(),
                "attempt_id": attempt_id.to_string()
            }),
        )
    }

    fn node_succeeded_payload(
        &self,
        graph: &GraphReference,
        start: &NodeAttemptStartHead,
        result_digest: Digest,
    ) -> Result<JournalPayload, GraphDriverError> {
        let base = start.activation().base_checkpoint();
        self.event_payload(
            "graph-node-attempt-succeeded",
            json!({
                "operation": "node_attempt_succeeded",
                "graph_digest": digest_hex(graph.definition_digest()),
                "checkpoint_id": base.checkpoint_id().to_string(),
                "superstep": base.superstep().get().to_string(),
                "node_id": start.activation().node_id().as_str(),
                "attempt_id": start.attempt_id().to_string(),
                "result_digest": digest_hex(result_digest)
            }),
        )
    }

    fn node_failed_payload(
        &self,
        graph: &GraphReference,
        start: &NodeAttemptStartHead,
        failure_id: FailureId,
    ) -> Result<JournalPayload, GraphDriverError> {
        let base = start.activation().base_checkpoint();
        self.event_payload(
            "graph-node-attempt-failed",
            json!({
                "operation": "node_attempt_failed",
                "graph_digest": digest_hex(graph.definition_digest()),
                "checkpoint_id": base.checkpoint_id().to_string(),
                "superstep": base.superstep().get().to_string(),
                "node_id": start.activation().node_id().as_str(),
                "attempt_id": start.attempt_id().to_string(),
                "failure_id": failure_id.to_string()
            }),
        )
    }

    fn barrier_continued_payload(
        &self,
        graph: &GraphReference,
        checkpoint: &Checkpoint,
        successor_id: CheckpointId,
        successor_superstep: stateknot_core::Superstep,
    ) -> Result<JournalPayload, GraphDriverError> {
        self.event_payload(
            "graph-barrier-continued",
            json!({
                "operation": "graph_barrier_continued",
                "graph_digest": digest_hex(graph.definition_digest()),
                "checkpoint_id": checkpoint.checkpoint_id().to_string(),
                "superstep": checkpoint.superstep().get().to_string(),
                "successor_checkpoint_id": successor_id.to_string(),
                "successor_superstep": successor_superstep.get().to_string(),
                "disposition": "continue"
            }),
        )
    }

    fn event_payload(
        &self,
        kind: &'static str,
        data: Value,
    ) -> Result<JournalPayload, GraphDriverError> {
        let data =
            BoundedJson::try_from_value(data).map_err(|_| GraphDriverError::EventPayloadInvalid)?;
        self.registry
            .schemas()
            .validate_bounded(&self.journal_schema, &data)
            .map_err(|_| GraphDriverError::EventPayloadInvalid)?;
        let kind =
            JournalEventKind::new(kind).map_err(|_| GraphDriverError::EventPayloadInvalid)?;
        JournalPayload::new(self.journal_schema.clone(), kind, data)
            .map_err(|_| GraphDriverError::EventPayloadInvalid)
    }

    fn node_timeout_failure() -> Result<GraphNodeExecutionError, GraphDriverError> {
        let failure = runtime_failure(
            FailureCategory::DeadlineExceeded,
            "runtime.node_timeout",
            "graph node execution exceeded its configured deadline",
            None::<JoinError>,
        )?;
        GraphNodeExecutionError::new(failure, BudgetUsage::zero())
            .map_err(|_| GraphDriverError::RuntimeFailureInvalid)
    }

    fn node_task_failure(source: JoinError) -> Result<GraphNodeExecutionError, GraphDriverError> {
        let failure = runtime_failure(
            FailureCategory::Internal,
            "runtime.node_task_failed",
            "graph node execution task terminated unexpectedly",
            Some(source),
        )?;
        GraphNodeExecutionError::new(failure, BudgetUsage::zero())
            .map_err(|_| GraphDriverError::RuntimeFailureInvalid)
    }
}

impl fmt::Debug for DurableGraphDriver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableGraphDriver")
            .field("registry", &self.registry)
            .field("journal_schema", &self.journal_schema)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

/// Startup failure while binding the driver to a provider and registry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableGraphDriverBuildError {
    /// The release's embedded standard schema was malformed.
    #[error(transparent)]
    StandardSchema(#[from] StandardGraphDriverSchemaError),
    /// The standard event schema was not installed before registry freeze.
    #[error("standard graph-driver journal schema is not installed")]
    JournalSchemaUnavailable,
    /// Fewer than three renewal intervals fit inside the provider lease.
    #[error("graph driver renewal cadence is unsafe for the provider lease timing")]
    UnsafeLeaseRenewalCadence,
}

/// Observable work and recovery evidence from one drive call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphDriveReport {
    replay: GraphReplayReport,
    durable_events: u32,
    node_attempts_started: u32,
    node_attempts_completed: u32,
    barriers_committed: u32,
    lease_renewals: u32,
    mutation_retries: u32,
}

impl GraphDriveReport {
    const fn new(replay: GraphReplayReport) -> Self {
        Self {
            replay,
            durable_events: 0,
            node_attempts_started: 0,
            node_attempts_completed: 0,
            barriers_committed: 0,
            lease_renewals: 0,
            mutation_retries: 0,
        }
    }

    /// Returns the complete noninitial replay evidence.
    #[must_use]
    pub const fn replay(self) -> GraphReplayReport {
        self.replay
    }

    /// Returns driver-owned journal events observed or committed this call.
    #[must_use]
    pub const fn durable_events(self) -> u32 {
        self.durable_events
    }

    /// Returns fresh attempt starts authorized for execution.
    #[must_use]
    pub const fn node_attempts_started(self) -> u32 {
        self.node_attempts_started
    }

    /// Returns attempt completions converged by this call.
    #[must_use]
    pub const fn node_attempts_completed(self) -> u32 {
        self.node_attempts_completed
    }

    /// Returns Continue barriers converged by this call.
    #[must_use]
    pub const fn barriers_committed(self) -> u32 {
        self.barriers_committed
    }

    /// Returns successful or idempotent lease renewals during node work.
    #[must_use]
    pub const fn lease_renewals(self) -> u32 {
        self.lease_renewals
    }

    /// Returns extra identical attempts after retryable storage failures.
    #[must_use]
    pub const fn mutation_retries(self) -> u32 {
        self.mutation_retries
    }
}

/// Complete result of one bounded drive call.
#[derive(Debug)]
pub struct GraphDriveResult {
    outcome: GraphDriveOutcome,
    report: GraphDriveReport,
}

impl GraphDriveResult {
    const fn new(outcome: GraphDriveOutcome, report: GraphDriveReport) -> Self {
        Self { outcome, report }
    }

    /// Returns why the driver stopped or handed execution off.
    #[must_use]
    pub const fn outcome(&self) -> &GraphDriveOutcome {
        &self.outcome
    }

    /// Returns replay, execution, renewal, and retry counters.
    #[must_use]
    pub const fn report(&self) -> GraphDriveReport {
        self.report
    }

    /// Consumes the result into its outcome and report.
    #[must_use]
    pub fn into_parts(self) -> (GraphDriveOutcome, GraphDriveReport) {
        (self.outcome, self.report)
    }
}

/// Closed reason one bounded drive call stopped.
#[derive(Debug)]
#[non_exhaustive]
pub enum GraphDriveOutcome {
    /// Durable cancellation intent requires exact evidence and terminal acknowledgement.
    CancellationRequested(Box<GraphCancellationHandoff>),
    /// A Wait or successful Terminal barrier requires lifecycle metadata.
    LifecycleBarrierReady(Box<GraphLifecycleBarrierHandoff>),
    /// In-flight, terminally failed, or exhausted nodes require supervision.
    Blocked(Box<GraphBlockedHandoff>),
    /// Deferred-only work was durably scheduled and its lease released.
    Deferred {
        /// Inclusive database retry boundary.
        not_before: Timestamp,
        /// Exact scheduling convergence result.
        schedule: DelayedRetryScheduleOutcome,
    },
    /// The bounded work quantum was exhausted between durable operations.
    Yielded {
        /// Exact-fence lease release convergence.
        release: LeaseReleaseOutcome,
    },
    /// Cooperative shutdown won between or during node execution.
    Cancelled {
        /// Exact-fence lease release convergence.
        release: LeaseReleaseOutcome,
    },
}

/// Exact lease-bound input for terminal cancellation acknowledgement.
#[derive(Clone, Debug)]
pub struct GraphCancellationHandoff {
    checkpoint: stateknot_core::CheckpointHead,
    journal_head: JournalHead,
    event_id: EventId,
    expected_revision: RunRevision,
    lease: RunLease,
    failure_id: FailureId,
}

impl GraphCancellationHandoff {
    /// Returns the exact checkpoint at which cancellation stopped graph progress.
    #[must_use]
    pub const fn checkpoint(&self) -> &stateknot_core::CheckpointHead {
        &self.checkpoint
    }

    /// Returns the cancellation-request event as the exact journal predecessor.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the stable lost-acknowledgement identity for confirmation.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Returns the cancellation-request lifecycle revision to consume.
    #[must_use]
    pub const fn expected_revision(&self) -> RunRevision {
        self.expected_revision
    }

    /// Returns the exact live lease retained for confirmation.
    #[must_use]
    pub const fn lease(&self) -> &RunLease {
        &self.lease
    }

    /// Returns the immutable cancellation occurrence selected by the request.
    #[must_use]
    pub const fn failure_id(&self) -> FailureId {
        self.failure_id
    }

    /// Consumes the handoff into storage-ready parts.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        stateknot_core::CheckpointHead,
        JournalHead,
        EventId,
        RunRevision,
        RunLease,
        FailureId,
    ) {
        (
            self.checkpoint,
            self.journal_head,
            self.event_id,
            self.expected_revision,
            self.lease,
            self.failure_id,
        )
    }
}

/// Exact lease-bound Wait/Terminal commit input for a lifecycle integrator.
#[derive(Clone, Debug)]
pub struct GraphLifecycleBarrierHandoff {
    plan: GraphBarrierPlan,
    journal_head: JournalHead,
    event_id: EventId,
    expected_revision: RunRevision,
    lease: RunLease,
}

impl GraphLifecycleBarrierHandoff {
    /// Returns the complete deterministic barrier and disposition.
    #[must_use]
    pub const fn plan(&self) -> &GraphBarrierPlan {
        &self.plan
    }

    /// Returns the exact current journal head for the lifecycle append.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the stable lost-acknowledgement identity for the lifecycle event.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Returns the lifecycle revision that must still match atomically.
    #[must_use]
    pub const fn expected_revision(&self) -> RunRevision {
        self.expected_revision
    }

    /// Returns the exact lease that must remain unexpired at commit.
    #[must_use]
    pub const fn lease(&self) -> &RunLease {
        &self.lease
    }

    /// Consumes the handoff into storage-ready parts.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        GraphBarrierPlan,
        JournalHead,
        EventId,
        RunRevision,
        RunLease,
    ) {
        (
            self.plan,
            self.journal_head,
            self.event_id,
            self.expected_revision,
            self.lease,
        )
    }
}

/// Aggregate classifications that prevent further node dispatch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GraphDriveBlockers {
    in_flight: u16,
    failed: u16,
    exhausted: u16,
    unsupported: u16,
}

impl GraphDriveBlockers {
    fn from_plan(plan: &ReadyNodeRecoveryPlan) -> Self {
        let mut blockers = Self::default();
        for node in plan.nodes() {
            match node.kind() {
                RecoveryNodeKind::InFlight => {
                    blockers.in_flight = blockers.in_flight.saturating_add(1);
                }
                RecoveryNodeKind::Failed => {
                    blockers.failed = blockers.failed.saturating_add(1);
                }
                RecoveryNodeKind::Exhausted => {
                    blockers.exhausted = blockers.exhausted.saturating_add(1);
                }
                RecoveryNodeKind::Completed
                | RecoveryNodeKind::Dispatchable
                | RecoveryNodeKind::Deferred => {}
                _ => {
                    blockers.unsupported = blockers.unsupported.saturating_add(1);
                }
            }
        }
        blockers
    }

    /// Returns whether any same-fence unfinished attempt exists.
    #[must_use]
    pub const fn in_flight(self) -> u16 {
        self.in_flight
    }

    /// Returns terminal node failures without retry authority.
    #[must_use]
    pub const fn failed(self) -> u16 {
        self.failed
    }

    /// Returns activations at the hard physical-attempt ceiling.
    #[must_use]
    pub const fn exhausted(self) -> u16 {
        self.exhausted
    }

    /// Returns future scheduler classifications this driver cannot execute.
    #[must_use]
    pub const fn unsupported(self) -> u16 {
        self.unsupported
    }

    /// Returns whether no blocking classification exists.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.in_flight == 0 && self.failed == 0 && self.exhausted == 0 && self.unsupported == 0
    }
}

/// Exact recovery plan and lease retained for supervision.
#[derive(Clone, Debug)]
pub struct GraphBlockedHandoff {
    plan: ReadyNodeRecoveryPlan,
    lease: RunLease,
    event_id: EventId,
    expected_revision: RunRevision,
    blockers: GraphDriveBlockers,
}

impl GraphBlockedHandoff {
    /// Returns every deterministic ready-node classification and evidence.
    #[must_use]
    pub const fn plan(&self) -> &ReadyNodeRecoveryPlan {
        &self.plan
    }

    /// Returns the exact lease retained while supervision decides the edge.
    #[must_use]
    pub const fn lease(&self) -> &RunLease {
        &self.lease
    }

    /// Returns the stable lost-acknowledgement identity for failure supervision.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Returns the lifecycle revision observed with the retained live lease.
    #[must_use]
    pub const fn expected_revision(&self) -> RunRevision {
        self.expected_revision
    }

    /// Returns aggregate blocking counts.
    #[must_use]
    pub const fn blockers(&self) -> GraphDriveBlockers {
        self.blockers
    }

    /// Consumes the handoff into supervision-ready parts.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ReadyNodeRecoveryPlan,
        RunLease,
        EventId,
        RunRevision,
        GraphDriveBlockers,
    ) {
        (
            self.plan,
            self.lease,
            self.event_id,
            self.expected_revision,
            self.blockers,
        )
    }
}

/// Durable driver failure with payload-redacted diagnostics.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GraphDriverError {
    /// `PostgreSQL` rejected or could not complete an operation.
    #[error(transparent)]
    Store {
        /// Exact payload-redacted provider failure.
        source: Box<StoreError>,
    },
    /// A claimed checkpoint had no journal anchor.
    #[error("graph driver requires a non-empty run journal")]
    MissingJournalHead,
    /// The exact pinned graph code was absent from this deployment.
    #[error("pinned graph has no executable deployment binding")]
    ExecutableGraphUnavailable {
        /// Complete unavailable graph reference.
        graph: Box<GraphReference>,
    },
    /// A graph registry invariant lost one node executor.
    #[error("registered graph node executor is unavailable")]
    ExecutableNodeUnavailable {
        /// Complete graph reference.
        graph: Box<GraphReference>,
        /// Missing compiled node identity.
        node_id: stateknot_core::NodeId,
    },
    /// A durable attempt could not bind to its immutable checkpoint.
    #[error(transparent)]
    NodeContext(#[from] GraphNodeContextError),
    /// A node returned a crossed or noncanonical semantic result.
    #[error("graph node returned an invalid durable result")]
    InvalidNodeResult,
    /// Pure barrier planning rejected current result semantics.
    #[error("graph barrier planning failed: {source}")]
    GraphPlanning {
        /// Exact public-safe planner failure.
        #[source]
        source: Box<GraphBarrierPlanError>,
    },
    /// Barrier result serialization failed closed.
    #[error("graph barrier results could not be measured")]
    BarrierResultEncoding,
    /// Current barrier results exceeded the configured memory ceiling.
    #[error("graph barrier results exceed the configured memory limit")]
    BarrierResultResourceLimit,
    /// Result pagination did not preserve the recovery journal observation.
    #[error("graph barrier result snapshot became stale")]
    StaleBarrierSnapshot,
    /// Standard audit event data failed its pinned local schema.
    #[error("graph driver event payload failed its pinned schema")]
    EventPayloadInvalid,
    /// A worker journal append could not be constructed.
    #[error("graph driver worker journal append is invalid")]
    JournalAppendInvalid,
    /// Lease extension overflowed canonical timestamp bounds.
    #[error("graph driver lease extension exceeds canonical time bounds")]
    LeaseTimestampOverflow,
    /// The driver's own public-safe failure could not be constructed.
    #[error("graph driver could not construct runtime failure evidence")]
    RuntimeFailureInvalid,
    /// A supposedly closed registry/recovery invariant failed.
    #[error("graph driver invariant failed while attempting to {operation}")]
    RuntimeInvariant {
        /// Stable payload-free operation label.
        operation: &'static str,
    },
}

impl GraphDriverError {
    fn graph_plan(source: GraphBarrierPlanError) -> Self {
        Self::GraphPlanning {
            source: Box::new(source),
        }
    }
}

impl From<StoreError> for GraphDriverError {
    fn from(source: StoreError) -> Self {
        Self::Store {
            source: Box::new(source),
        }
    }
}

enum StartedNodeExecution {
    Finished(Result<GraphNodeExecution, GraphNodeExecutionError>),
    Cancelled,
    RunCancellationObserved,
}

enum NodeExecutionCommit {
    Committed,
    RunCancellationObserved,
}

struct GuardedRunLease {
    lease: RunLease,
    deadline: Instant,
}

fn guarded_run_lease(
    lease: RunLease,
    observed_at: Timestamp,
    observation_started: Instant,
) -> Result<GuardedRunLease, GraphDriverError> {
    let remaining_micros = lease
        .expires_at()
        .unix_micros()
        .checked_sub(observed_at.unix_micros())
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or(StoreError::LeaseExpired)?;
    let deadline = observation_started
        .checked_add(Duration::from_micros(remaining_micros))
        .ok_or(GraphDriverError::LeaseTimestampOverflow)?;
    if deadline <= Instant::now() {
        return Err(StoreError::LeaseExpired.into());
    }
    Ok(GuardedRunLease { lease, deadline })
}

fn exact_live_lease<'run>(
    run: &'run StoredRun,
    fence: &RunFence,
) -> Result<&'run RunLease, GraphDriverError> {
    if run.is_quarantined() {
        return Err(StoreError::RunQuarantined.into());
    }
    let lease = run.lease().ok_or(StoreError::NoActiveLease)?;
    if lease.fence() != fence {
        return Err(StoreError::StaleFence.into());
    }
    Ok(lease)
}

fn graph_error_is_run_not_runnable(error: &GraphDriverError) -> bool {
    matches!(
        error,
        GraphDriverError::Store { source } if matches!(source.as_ref(), StoreError::RunNotRunnable)
    )
}

fn recovery_evidence_digest(run: &StoredRun, fence: &RunFence) -> Result<Digest, GraphDriverError> {
    let head = run
        .journal_head()
        .ok_or(GraphDriverError::MissingJournalHead)?;
    let checkpoint = run
        .checkpoint()
        .ok_or(StoreError::ReadyNodeRecoveryCheckpointMissing)?;
    let mut evidence =
        Vec::with_capacity(RECOVERY_EVIDENCE_DOMAIN.len() + Digest::SHA256_LEN * 2 + 16 + 8);
    evidence.extend_from_slice(RECOVERY_EVIDENCE_DOMAIN);
    evidence.extend_from_slice(head.digest().as_bytes());
    evidence.extend_from_slice(checkpoint.digest().as_bytes());
    evidence.extend_from_slice(fence.attempt_id().as_uuid().as_bytes());
    evidence.extend_from_slice(&fence.epoch().get().to_be_bytes());
    Ok(Digest::sha256(evidence))
}

fn worker_append(
    fence: &RunFence,
    head: JournalHead,
    event_id: EventId,
    payload: JournalPayload,
) -> Result<JournalAppend, GraphDriverError> {
    let intent = JournalEventIntent::worker(
        fence.tenant_id().clone(),
        fence.run_id(),
        event_id,
        fence.clone(),
        payload,
    )
    .map_err(|_| GraphDriverError::JournalAppendInvalid)?;
    JournalAppend::new(JournalExpectation::exact(head), intent)
        .map_err(|_| GraphDriverError::JournalAppendInvalid)
}

fn extend_timestamp(
    timestamp: Timestamp,
    duration: Duration,
) -> Result<Timestamp, GraphDriverError> {
    let micros = i64::try_from(duration.as_micros())
        .map_err(|_| GraphDriverError::LeaseTimestampOverflow)?;
    let value = timestamp
        .unix_micros()
        .checked_add(micros)
        .ok_or(GraphDriverError::LeaseTimestampOverflow)?;
    Timestamp::from_unix_micros(value).map_err(|_| GraphDriverError::LeaseTimestampOverflow)
}

fn digest_hex(digest: Digest) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(Digest::SHA256_LEN * 2);
    for byte in digest.as_bytes() {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn runtime_failure<E>(
    category: FailureCategory,
    code: &'static str,
    message: &'static str,
    source: Option<E>,
) -> Result<Failure, GraphDriverError>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let code = FailureCode::new(code).map_err(|_| GraphDriverError::RuntimeFailureInvalid)?;
    let origin = FailureOrigin::new("stateknot.runtime.graph-driver")
        .map_err(|_| GraphDriverError::RuntimeFailureInvalid)?;
    let message =
        FailureMessage::new(message).map_err(|_| GraphDriverError::RuntimeFailureInvalid)?;
    let mut failure = Failure::new(
        FailureId::generate(),
        category,
        code,
        origin,
        message,
        RetryAdvice::Never,
    )
    .map_err(|_| GraphDriverError::RuntimeFailureInvalid)?;
    if let Some(source) = source {
        failure = failure.with_private_source(source);
    }
    Ok(failure)
}

async fn abort_node_task<T>(task: &mut JoinHandle<T>) {
    task.abort();
    let _ = task.await;
}

#[derive(Default)]
struct CompactByteCounter {
    bytes: usize,
}

impl std::io::Write for CompactByteCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buffer.len());
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct DriverCancellation {
    state: Arc<DriverCancellationState>,
}

struct DriverCancellationState {
    cancelled: AtomicBool,
    notify: Notify,
}

impl DriverCancellation {
    fn new() -> Self {
        Self {
            state: Arc::new(DriverCancellationState {
                cancelled: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    fn signal(&self) -> CancellationSignal {
        CancellationSignal::new(self.clone())
    }

    fn cancel(&self) {
        if !self.state.cancelled.swap(true, Ordering::AcqRel) {
            self.state.notify.notify_waiters();
        }
    }
}

impl CancellationObserver for DriverCancellation {
    fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            loop {
                let notified = self.state.notify.notified();
                if self.is_cancelled() {
                    return;
                }
                notified.await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_reject_unsafe_local_bounds() {
        let limits = GraphReplayLimits::default();
        assert!(
            DurableGraphDriverOptions::new(
                limits,
                1,
                Duration::from_secs(10),
                Duration::from_secs(60),
                3,
                Duration::from_millis(25),
            )
            .is_err()
        );
        assert!(
            DurableGraphDriverOptions::new(
                limits,
                2,
                Duration::from_nanos(1),
                Duration::from_secs(60),
                3,
                Duration::from_millis(25),
            )
            .is_err()
        );
        assert!(
            DurableGraphDriverOptions::new(
                limits,
                2,
                Duration::from_secs(1),
                Duration::ZERO,
                3,
                Duration::from_millis(25),
            )
            .is_err()
        );
        let options = DurableGraphDriverOptions::default();
        assert!(matches!(
            options.with_cancellation_timing(Duration::ZERO, Duration::from_secs(1)),
            Err(DurableGraphDriverOptionsError::InvalidCancellationPollInterval)
        ));
        assert!(matches!(
            options.with_cancellation_timing(Duration::from_millis(9), Duration::from_secs(1),),
            Err(DurableGraphDriverOptionsError::InvalidCancellationPollInterval)
        ));
        assert!(matches!(
            options.with_cancellation_timing(
                DurableGraphDriverOptions::HARD_MINIMUM_CANCELLATION_POLL_INTERVAL,
                Duration::from_secs(5 * 60 + 1),
            ),
            Err(DurableGraphDriverOptionsError::InvalidCancellationGracePeriod)
        ));
    }

    #[tokio::test]
    async fn cancellation_is_monotonic_and_race_safe() {
        let cancellation = DriverCancellation::new();
        let signal = cancellation.signal();
        assert!(!signal.is_cancelled());
        let waiter = tokio::spawn({
            let signal = signal.clone();
            async move { signal.cancelled().await }
        });
        cancellation.cancel();
        waiter.await.unwrap();
        cancellation.cancel();
        assert!(signal.is_cancelled());
        signal.cancelled().await;
    }

    #[test]
    fn digest_data_uses_schema_hex_without_algorithm_prefix() {
        let value = digest_hex(Digest::sha256(b"driver"));
        assert_eq!(value.len(), 64);
        assert!(value.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!value.contains(':'));
    }
}
