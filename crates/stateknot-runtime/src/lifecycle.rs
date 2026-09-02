// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Fenced graph-to-agent lifecycle commits.

use std::{fmt, sync::Arc, time::Duration};

use serde_json::{Value, json};
use stateknot_core::{
    AgentArtifacts, AgentDescriptor, AgentRequest, AgentResult, AgentResultError,
    AgentResultProvenance, AgentResultValidationError, BoundedJson, BoxFuture, BudgetUsage,
    CheckpointHead, CheckpointId, Digest, DurableWaitError, EventId, Failure, FailureId,
    GraphBarrierDisposition, GraphReference, GraphSchemaValidationError, JournalAppend,
    JournalEventIntent, JournalEventKind, JournalExpectation, JournalHead, JournalPayload,
    ResolvedBudget, RunFailure, RunFailureError, RunFence, RunLease, RunRevision, RunStatus,
    RunTransition, SchemaReference, Superstep, Timestamp,
};
use stateknot_store_postgres::{
    AppendOutcome, BarrierCommitOutcome, LeaseReleaseOutcome, PostgresStore, RunProjection,
    StoreError, StoredRun, WaitCheckpointCommitOutcome,
};
use thiserror::Error;

use crate::{
    ExecutableGraphRegistry, GraphBlockedHandoff, GraphCancellationHandoff, GraphDriveBlockers,
    GraphLifecycleBarrierHandoff, StandardAgentCancellationSchemaError,
    StandardGraphLifecycleSchemaError, standard_agent_cancellation_event_schema,
    standard_graph_lifecycle_event_schema,
};

const MAX_MUTATION_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Retry policy for atomic graph lifecycle mutations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableGraphLifecycleOptions {
    maximum_mutation_attempts: u8,
    mutation_retry_initial_delay: Duration,
}

impl DurableGraphLifecycleOptions {
    /// Absolute number of identical durable mutation attempts.
    pub const HARD_MAXIMUM_MUTATION_ATTEMPTS: u8 = 10;

    /// Constructs an explicit bounded retry policy.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive attempts or a zero/greater-than-one-second
    /// initial delay.
    pub fn new(
        maximum_mutation_attempts: u8,
        mutation_retry_initial_delay: Duration,
    ) -> Result<Self, DurableGraphLifecycleOptionsError> {
        if maximum_mutation_attempts == 0
            || maximum_mutation_attempts > Self::HARD_MAXIMUM_MUTATION_ATTEMPTS
        {
            return Err(DurableGraphLifecycleOptionsError::InvalidMutationAttempts);
        }
        if mutation_retry_initial_delay.is_zero()
            || mutation_retry_initial_delay > MAX_MUTATION_RETRY_DELAY
        {
            return Err(DurableGraphLifecycleOptionsError::InvalidMutationRetryDelay);
        }
        Ok(Self {
            maximum_mutation_attempts,
            mutation_retry_initial_delay,
        })
    }

    /// Returns the maximum identical attempts for one mutation.
    #[must_use]
    pub const fn maximum_mutation_attempts(self) -> u8 {
        self.maximum_mutation_attempts
    }

    /// Returns the first exponential retry delay.
    #[must_use]
    pub const fn mutation_retry_initial_delay(self) -> Duration {
        self.mutation_retry_initial_delay
    }
}

impl Default for DurableGraphLifecycleOptions {
    fn default() -> Self {
        Self {
            maximum_mutation_attempts: 3,
            mutation_retry_initial_delay: Duration::from_millis(25),
        }
    }
}

/// Invalid lifecycle retry policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableGraphLifecycleOptionsError {
    /// Mutation attempts were zero or above the hard ceiling.
    #[error("graph lifecycle mutation attempt count is invalid")]
    InvalidMutationAttempts,
    /// Initial mutation backoff was zero or above one second.
    #[error("graph lifecycle mutation retry delay is invalid")]
    InvalidMutationRetryDelay,
}

/// Payload-free trusted context for terminal evidence recovery.
#[derive(Clone, Debug)]
pub struct GraphTerminalEvidenceContext {
    provenance: AgentResultProvenance,
    graph: GraphReference,
    checkpoint: CheckpointHead,
    successor_checkpoint_id: CheckpointId,
    successor_superstep: Superstep,
    output_schema: SchemaReference,
    output_digest: Digest,
    expected_revision: RunRevision,
}

impl GraphTerminalEvidenceContext {
    /// Returns trusted run provenance from the current lifecycle snapshot.
    #[must_use]
    pub const fn provenance(&self) -> &AgentResultProvenance {
        &self.provenance
    }

    /// Returns the exact pinned executable graph.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }

    /// Returns the barrier's exact base checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &CheckpointHead {
        &self.checkpoint
    }

    /// Returns the pending terminal successor identity.
    #[must_use]
    pub const fn successor_checkpoint_id(&self) -> CheckpointId {
        self.successor_checkpoint_id
    }

    /// Returns the pending terminal successor position.
    #[must_use]
    pub const fn successor_superstep(&self) -> Superstep {
        self.successor_superstep
    }

    /// Returns the exact terminal output schema.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaReference {
        &self.output_schema
    }

    /// Returns the terminal output's integrity digest without its data.
    #[must_use]
    pub const fn output_digest(&self) -> Digest {
        self.output_digest
    }

    /// Returns the lifecycle revision that the final transaction must consume.
    #[must_use]
    pub const fn expected_revision(&self) -> RunRevision {
        self.expected_revision
    }
}

/// Trusted admission and cumulative accounting required for success.
#[derive(Clone)]
pub struct GraphTerminalEvidence {
    descriptor: AgentDescriptor,
    request: AgentRequest,
    budget: ResolvedBudget,
    artifacts: AgentArtifacts,
    usage: BudgetUsage,
}

impl GraphTerminalEvidence {
    /// Bundles immutable snapshots recovered from trusted durable sources.
    #[must_use]
    pub const fn new(
        descriptor: AgentDescriptor,
        request: AgentRequest,
        budget: ResolvedBudget,
        artifacts: AgentArtifacts,
        usage: BudgetUsage,
    ) -> Self {
        Self {
            descriptor,
            request,
            budget,
            artifacts,
            usage,
        }
    }

    /// Returns the immutable admitted agent descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &AgentDescriptor {
        &self.descriptor
    }

    /// Returns the immutable admitted request.
    #[must_use]
    pub const fn request(&self) -> &AgentRequest {
        &self.request
    }

    /// Returns the exact resolved finite budget snapshot.
    #[must_use]
    pub const fn budget(&self) -> &ResolvedBudget {
        &self.budget
    }

    /// Returns final durable artifact references.
    #[must_use]
    pub const fn artifacts(&self) -> &AgentArtifacts {
        &self.artifacts
    }

    /// Returns complete cumulative run usage.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    fn into_parts(
        self,
    ) -> (
        AgentDescriptor,
        AgentRequest,
        ResolvedBudget,
        AgentArtifacts,
        BudgetUsage,
    ) {
        (
            self.descriptor,
            self.request,
            self.budget,
            self.artifacts,
            self.usage,
        )
    }
}

impl fmt::Debug for GraphTerminalEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GraphTerminalEvidence")
            .field("agent", self.descriptor.metadata().identity())
            .field("input_schema", self.request.input_schema())
            .field("output_schema", self.descriptor.output_schema())
            .field("artifact_count", &self.artifacts.len())
            .field("usage_recorded", &true)
            .finish_non_exhaustive()
    }
}

/// Payload-free trusted context for blocked-run failure evidence recovery.
#[derive(Clone, Debug)]
pub struct GraphFailureEvidenceContext {
    provenance: AgentResultProvenance,
    graph: GraphReference,
    checkpoint: CheckpointHead,
    observed_at: Timestamp,
    expected_revision: RunRevision,
    blockers: GraphDriveBlockers,
}

impl GraphFailureEvidenceContext {
    /// Returns trusted run provenance from the current lifecycle snapshot.
    #[must_use]
    pub const fn provenance(&self) -> &AgentResultProvenance {
        &self.provenance
    }

    /// Returns the exact pinned graph.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }

    /// Returns the blocked checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &CheckpointHead {
        &self.checkpoint
    }

    /// Returns the database observation used by recovery classification.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the lifecycle revision that the failure must consume.
    #[must_use]
    pub const fn expected_revision(&self) -> RunRevision {
        self.expected_revision
    }

    /// Returns aggregate blocked-node classifications.
    #[must_use]
    pub const fn blockers(&self) -> GraphDriveBlockers {
        self.blockers
    }
}

/// Public failure and cumulative accounting selected by durable supervision.
#[derive(Clone, Debug)]
pub struct GraphFailureEvidence {
    failure: Failure,
    usage: BudgetUsage,
}

/// Payload-free trusted context for terminal cancellation accounting recovery.
#[derive(Clone, Debug)]
pub struct GraphCancellationEvidenceContext {
    provenance: AgentResultProvenance,
    graph: GraphReference,
    checkpoint: CheckpointHead,
    expected_revision: RunRevision,
    cancellation_failure_id: FailureId,
}

impl GraphCancellationEvidenceContext {
    /// Returns trusted run provenance from the current lifecycle snapshot.
    #[must_use]
    pub const fn provenance(&self) -> &AgentResultProvenance {
        &self.provenance
    }

    /// Returns the exact pinned graph.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }

    /// Returns the checkpoint at which cancellation stopped graph progress.
    #[must_use]
    pub const fn checkpoint(&self) -> &CheckpointHead {
        &self.checkpoint
    }

    /// Returns the cancellation-request lifecycle revision to consume.
    #[must_use]
    pub const fn expected_revision(&self) -> RunRevision {
        self.expected_revision
    }

    /// Returns the immutable cancellation occurrence selected by the request.
    #[must_use]
    pub const fn cancellation_failure_id(&self) -> FailureId {
        self.cancellation_failure_id
    }
}

/// Complete cumulative accounting recovered before cancellation acknowledgement.
#[derive(Clone, Debug)]
pub struct GraphCancellationEvidence {
    usage: BudgetUsage,
}

impl GraphCancellationEvidence {
    /// Wraps exact cumulative usage reconstructed from trusted durable ledgers.
    #[must_use]
    pub const fn new(usage: BudgetUsage) -> Self {
        Self { usage }
    }

    /// Returns complete cumulative usage at cancellation acknowledgement.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    fn into_usage(self) -> BudgetUsage {
        self.usage
    }
}

impl GraphFailureEvidence {
    /// Bundles a public-safe occurrence and complete cumulative run usage.
    #[must_use]
    pub const fn new(failure: Failure, usage: BudgetUsage) -> Self {
        Self { failure, usage }
    }

    /// Returns the terminal failure occurrence.
    #[must_use]
    pub const fn failure(&self) -> &Failure {
        &self.failure
    }

    /// Returns complete cumulative run usage.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    fn into_parts(self) -> (Failure, BudgetUsage) {
        (self.failure, self.usage)
    }
}

/// Closed, payload-redacted evidence-provider failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphLifecycleEvidenceError {
    /// A trusted durable dependency could not be reached now.
    #[error("graph lifecycle evidence is temporarily unavailable")]
    TemporarilyUnavailable,
    /// Required evidence was permanently absent for this admitted run.
    #[error("graph lifecycle evidence is unavailable")]
    Unavailable,
    /// Durable evidence failed integrity or relationship validation.
    #[error("graph lifecycle evidence is corrupt")]
    Corrupt,
}

/// Trusted recovery boundary for admission snapshots and cumulative accounting.
///
/// Implementations must be read-only, deterministic for one context, bounded,
/// and free of external side effects. They must recover already-durable facts;
/// they must never infer missing usage, fabricate a request, or re-run model or
/// tool work. Provider diagnostics belong in protected telemetry and are mapped
/// to the closed public-safe error above.
pub trait GraphLifecycleEvidenceProvider: Send + Sync + 'static {
    /// Recovers success evidence for an exact terminal barrier.
    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>>;

    /// Recovers terminal failure evidence for an exact blocked checkpoint.
    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>>;

    /// Recovers complete cumulative usage for an exact cancellation request.
    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>>;
}

/// Converged result of one lifecycle handoff.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum GraphBarrierLifecycleOutcome {
    /// Cancellation acknowledgement, usage, event, and lease release committed atomically.
    Cancelled(AppendOutcome),
    /// Barrier, successor checkpoint, wait transition, registrations, and lease
    /// release committed atomically.
    Waiting(WaitCheckpointCommitOutcome),
    /// Barrier, terminal checkpoint, successful result, and lease release
    /// committed atomically.
    Succeeded(BarrierCommitOutcome),
    /// Terminal failure event, lifecycle projection, and lease release committed
    /// atomically.
    Failed(AppendOutcome),
    /// An unfinished same-fence attempt requires a successor fence to recover.
    Released(LeaseReleaseOutcome),
}

/// Fenced graph-to-agent lifecycle coordinator.
#[derive(Clone)]
pub struct DurableGraphLifecycle {
    store: PostgresStore,
    registry: ExecutableGraphRegistry,
    evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
    journal_schema: SchemaReference,
    cancellation_schema: SchemaReference,
    options: DurableGraphLifecycleOptions,
}

enum GraphLifecycleHandoffSnapshot {
    Fresh(StoredRun),
    Committed(StoredRun),
}

impl DurableGraphLifecycle {
    /// Binds one provider pool, frozen executable registry, and trusted evidence
    /// source.
    ///
    /// # Errors
    ///
    /// Rejects a malformed embedded release schema or a registry that omitted
    /// it before freezing.
    pub fn new(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
        evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
        options: DurableGraphLifecycleOptions,
    ) -> Result<Self, DurableGraphLifecycleBuildError> {
        let (journal_schema, _) = standard_graph_lifecycle_event_schema()?;
        if !registry.schemas().contains(&journal_schema) {
            return Err(DurableGraphLifecycleBuildError::JournalSchemaUnavailable);
        }
        let (cancellation_schema, _) = standard_agent_cancellation_event_schema()?;
        if !registry.schemas().contains(&cancellation_schema) {
            return Err(DurableGraphLifecycleBuildError::CancellationSchemaUnavailable);
        }
        Ok(Self {
            store,
            registry,
            evidence,
            journal_schema,
            cancellation_schema,
            options,
        })
    }

    /// Returns the immutable mutation retry policy.
    #[must_use]
    pub const fn options(&self) -> DurableGraphLifecycleOptions {
        self.options
    }

    /// Acknowledges one exact durable cancellation request after cumulative
    /// usage has been recovered from trusted ledgers.
    ///
    /// A fresh commit uses a database clock observed with the exact live fence.
    /// An identical retry reconstructs that timestamp and usage from the
    /// terminal lifecycle, producing the same projection digest even after the
    /// original lease was atomically released.
    pub fn confirm_cancellation(
        &self,
        handoff: GraphCancellationHandoff,
    ) -> BoxFuture<'_, Result<GraphBarrierLifecycleOutcome, GraphLifecycleError>> {
        Box::pin(self.confirm_cancellation_inner(handoff))
    }

    async fn confirm_cancellation_inner(
        &self,
        handoff: GraphCancellationHandoff,
    ) -> Result<GraphBarrierLifecycleOutcome, GraphLifecycleError> {
        let (checkpoint, journal_head, event_id, expected_revision, lease, failure_id) =
            handoff.into_parts();
        let fence = lease.fence().clone();
        validate_cancellation_scope(&fence, &journal_head, &checkpoint)?;
        let run = self
            .store
            .load_run(fence.tenant_id(), fence.run_id())
            .await?;
        validate_cancellation_checkpoint(&run, &checkpoint)?;

        let fresh = run.lifecycle().revision() == expected_revision
            && run.lifecycle().status() == RunStatus::CancellationRequested
            && run.journal_head() == Some(&journal_head)
            && run.lease().map(RunLease::fence) == Some(&fence)
            && run
                .lifecycle()
                .cancellation_request()
                .is_some_and(|request| request.failure().id() == failure_id);
        let (completed_at, usage) = if fresh {
            let observation = self.store.observe_live_lease(&fence).await?;
            if observation.lease().fence() != &fence {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            let context = GraphCancellationEvidenceContext {
                provenance: run.lifecycle().provenance().clone(),
                graph: checkpoint.graph().clone(),
                checkpoint: checkpoint.clone(),
                expected_revision,
                cancellation_failure_id: failure_id,
            };
            let evidence = self
                .evidence
                .cancellation_evidence(context)
                .await
                .map_err(GraphLifecycleError::Evidence)?;
            (observation.observed_at(), evidence.into_usage())
        } else {
            let committed_revision = expected_revision.get().checked_add(1);
            let committed = committed_revision
                .is_some_and(|revision| run.lifecycle().revision() == RunRevision::new(revision))
                && run.lifecycle().status() == RunStatus::Cancelled
                && run
                    .journal_head()
                    .is_some_and(|head| head.event_id() == event_id)
                && run.lease().is_none()
                && run
                    .lifecycle()
                    .cancellation_request()
                    .is_some_and(|request| request.failure().id() == failure_id);
            if !committed {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            let usage = run.lifecycle().terminal_usage().cloned().ok_or(
                GraphLifecycleError::InvalidHandoff {
                    operation: "recover committed cancellation accounting",
                },
            )?;
            (run.lifecycle().changed_at(), usage)
        };

        let payload = self.cancelled_payload(&checkpoint, failure_id)?;
        let append = worker_append(&fence, journal_head, event_id, payload)?;
        let projection = RunProjection::transition(
            expected_revision,
            RunTransition::ConfirmCancellation {
                completed_at,
                usage,
            },
        );
        let outcome = self.commit_failure_with_retry(append, projection).await?;
        Ok(GraphBarrierLifecycleOutcome::Cancelled(outcome))
    }

    /// Commits an exact Wait or successful-Terminal graph barrier.
    ///
    /// The terminal path recovers trusted admission/accounting evidence, then
    /// revalidates provenance, request, budget, input/output schemas, artifacts,
    /// and usage before the fenced transaction. Wait registration timestamps
    /// come only from the database transaction.
    pub fn commit_barrier(
        &self,
        handoff: GraphLifecycleBarrierHandoff,
    ) -> BoxFuture<'_, Result<GraphBarrierLifecycleOutcome, GraphLifecycleError>> {
        Box::pin(self.commit_barrier_inner(handoff))
    }

    async fn commit_barrier_inner(
        &self,
        handoff: GraphLifecycleBarrierHandoff,
    ) -> Result<GraphBarrierLifecycleOutcome, GraphLifecycleError> {
        let (plan, journal_head, event_id, expected_revision, lease) = handoff.into_parts();
        let fence = lease.fence().clone();
        let (barrier, disposition) = plan.into_parts();
        let base = barrier.base_checkpoint().clone();
        validate_barrier_scope(&fence, &journal_head, &base, barrier.successor().graph())?;

        match disposition {
            GraphBarrierDisposition::Wait { waits } => {
                // The provider performs the authoritative fresh-fence checks
                // and looks up the stable event before inspecting current run
                // status. Calling it directly also preserves exact lost-ACK
                // convergence if a resolver has already advanced the run past
                // Waiting before this caller receives the original commit.
                let payload = self.waiting_payload(
                    barrier.successor().graph(),
                    &base,
                    barrier.successor().checkpoint_id(),
                    barrier.successor().superstep(),
                    waits.len(),
                )?;
                let append = worker_append(&fence, journal_head, event_id, payload)?;
                let registrations = waits
                    .registration_intents(fence.tenant_id(), fence.run_id(), event_id)
                    .map_err(GraphLifecycleError::wait_registration)?;
                let outcome = self
                    .commit_wait_with_retry(append, expected_revision, barrier, registrations)
                    .await?;
                Ok(GraphBarrierLifecycleOutcome::Waiting(outcome))
            }
            GraphBarrierDisposition::Terminal { output } => {
                let snapshot = self
                    .validate_handoff_snapshot(
                        &fence,
                        &journal_head,
                        event_id,
                        expected_revision,
                        RunStatus::Succeeded,
                    )
                    .await?;
                let result = match snapshot {
                    GraphLifecycleHandoffSnapshot::Fresh(run) => {
                        let context = GraphTerminalEvidenceContext {
                            provenance: run.lifecycle().provenance().clone(),
                            graph: barrier.successor().graph().clone(),
                            checkpoint: base.clone(),
                            successor_checkpoint_id: barrier.successor().checkpoint_id(),
                            successor_superstep: barrier.successor().superstep(),
                            output_schema: output.schema().clone(),
                            output_digest: output.digest(),
                            expected_revision,
                        };
                        let evidence = self
                            .evidence
                            .terminal_evidence(context)
                            .await
                            .map_err(GraphLifecycleError::Evidence)?;
                        self.validate_terminal_result(
                            run.lifecycle().provenance(),
                            journal_head.recorded_at(),
                            &output,
                            evidence,
                        )?
                    }
                    GraphLifecycleHandoffSnapshot::Committed(run) => {
                        run.lifecycle().result().cloned().ok_or(
                            GraphLifecycleError::InvalidHandoff {
                                operation: "recover the committed successful result",
                            },
                        )?
                    }
                };
                let payload = self.succeeded_payload(
                    barrier.successor().graph(),
                    &base,
                    barrier.successor().checkpoint_id(),
                    barrier.successor().superstep(),
                    output.digest(),
                )?;
                let append = worker_append(&fence, journal_head, event_id, payload)?;
                let projection =
                    RunProjection::transition(expected_revision, RunTransition::Succeed { result });
                let outcome = self
                    .commit_success_with_retry(append, projection, barrier)
                    .await?;
                Ok(GraphBarrierLifecycleOutcome::Succeeded(outcome))
            }
            GraphBarrierDisposition::Continue => Err(GraphLifecycleError::InvalidHandoff {
                operation: "commit a Continue disposition through lifecycle coordination",
            }),
            _ => Err(GraphLifecycleError::InvalidHandoff {
                operation: "commit an unsupported graph barrier disposition",
            }),
        }
    }

    /// Resolves a blocked driver handoff without guessing node outcomes.
    ///
    /// Any same-fence in-flight start releases ownership so a higher fence can
    /// apply the provider's crash-recovery rules. Only a plan with no in-flight
    /// work may enter terminal failure supervision.
    pub fn resolve_blocked(
        &self,
        handoff: GraphBlockedHandoff,
    ) -> BoxFuture<'_, Result<GraphBarrierLifecycleOutcome, GraphLifecycleError>> {
        Box::pin(self.resolve_blocked_inner(handoff))
    }

    async fn resolve_blocked_inner(
        &self,
        handoff: GraphBlockedHandoff,
    ) -> Result<GraphBarrierLifecycleOutcome, GraphLifecycleError> {
        let (plan, lease, event_id, expected_revision, blockers) = handoff.into_parts();
        let fence = lease.fence().clone();
        if plan.fence() != &fence {
            return Err(GraphLifecycleError::InvalidHandoff {
                operation: "bind blocked recovery plan to its retained lease",
            });
        }
        if blockers.in_flight() > 0 {
            let release = self.release_with_retry(&fence).await?;
            return Ok(GraphBarrierLifecycleOutcome::Released(release));
        }
        if blockers.failed() == 0 && blockers.exhausted() == 0 && blockers.unsupported() == 0 {
            return Err(GraphLifecycleError::InvalidHandoff {
                operation: "supervise a blocked plan without terminal blockers",
            });
        }

        let journal_head = plan.journal_head().clone();
        let snapshot = self
            .validate_handoff_snapshot(
                &fence,
                &journal_head,
                event_id,
                expected_revision,
                RunStatus::Failed,
            )
            .await?;
        let checkpoint = plan.checkpoint().head();
        validate_blocked_checkpoint_scope(&fence, &journal_head, &checkpoint)?;
        let failure = match snapshot {
            GraphLifecycleHandoffSnapshot::Fresh(run) => {
                let context = GraphFailureEvidenceContext {
                    provenance: run.lifecycle().provenance().clone(),
                    graph: checkpoint.graph().clone(),
                    checkpoint: checkpoint.clone(),
                    observed_at: plan.observed_at(),
                    expected_revision,
                    blockers,
                };
                let evidence = self
                    .evidence
                    .failure_evidence(context)
                    .await
                    .map_err(GraphLifecycleError::Evidence)?;
                let (failure, usage) = evidence.into_parts();
                RunFailure::new(failure, plan.observed_at(), usage)
                    .map_err(GraphLifecycleError::run_failure)?
            }
            GraphLifecycleHandoffSnapshot::Committed(run) => {
                let lifecycle = run.lifecycle();
                let failure = lifecycle.terminal_failure().cloned().ok_or(
                    GraphLifecycleError::InvalidHandoff {
                        operation: "recover the committed terminal failure",
                    },
                )?;
                let usage = lifecycle.terminal_usage().cloned().ok_or(
                    GraphLifecycleError::InvalidHandoff {
                        operation: "recover committed terminal failure accounting",
                    },
                )?;
                RunFailure::new(failure, lifecycle.changed_at(), usage)
                    .map_err(GraphLifecycleError::run_failure)?
            }
        };
        let failure_id = failure.failure().id();
        let payload = self.failed_payload(checkpoint.graph(), &checkpoint, failure_id, blockers)?;
        let append = worker_append(&fence, journal_head, event_id, payload)?;
        let projection =
            RunProjection::transition(expected_revision, RunTransition::Fail { failure });
        let outcome = self.commit_failure_with_retry(append, projection).await?;
        Ok(GraphBarrierLifecycleOutcome::Failed(outcome))
    }

    async fn validate_handoff_snapshot(
        &self,
        fence: &RunFence,
        journal_head: &JournalHead,
        event_id: EventId,
        expected_revision: RunRevision,
        committed_status: RunStatus,
    ) -> Result<GraphLifecycleHandoffSnapshot, GraphLifecycleError> {
        let run = self
            .store
            .load_run(fence.tenant_id(), fence.run_id())
            .await?;
        let is_fresh = run.lifecycle().revision() == expected_revision
            && run.lifecycle().status() == RunStatus::Active
            && run.journal_head() == Some(journal_head)
            && run.lease().map(RunLease::fence) == Some(fence);
        if is_fresh {
            let observation = self.store.observe_live_lease(fence).await?;
            if observation.lease().fence() != fence {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            return Ok(GraphLifecycleHandoffSnapshot::Fresh(run));
        }

        let committed_revision = expected_revision.get().checked_add(1);
        let is_committed_retry = committed_revision
            .is_some_and(|revision| run.lifecycle().revision() == RunRevision::new(revision))
            && run.lifecycle().status() == committed_status
            && run
                .journal_head()
                .is_some_and(|head| head.event_id() == event_id)
            && run.lease().is_none();
        if is_committed_retry {
            return Ok(GraphLifecycleHandoffSnapshot::Committed(run));
        }
        Err(GraphLifecycleError::StaleHandoff)
    }

    fn validate_terminal_result(
        &self,
        provenance: &AgentResultProvenance,
        completed_at: Timestamp,
        output: &stateknot_core::NodeTerminalOutput,
        evidence: GraphTerminalEvidence,
    ) -> Result<AgentResult, GraphLifecycleError> {
        let (descriptor, request, budget, artifacts, usage) = evidence.into_parts();
        self.registry
            .schemas()
            .validate_bounded(request.input_schema(), request.input())
            .map_err(GraphLifecycleError::input_schema)?;
        self.registry
            .schemas()
            .validate_bounded(output.schema(), output.data())
            .map_err(GraphLifecycleError::output_schema)?;
        let result = AgentResult::new(
            provenance.clone(),
            completed_at,
            output.schema().clone(),
            output.data().clone(),
            artifacts,
            usage,
        )
        .map_err(GraphLifecycleError::agent_result)?;
        result
            .validate_for(provenance, &request, &descriptor, &budget)
            .map_err(GraphLifecycleError::agent_result_validation)?;
        Ok(result)
    }

    async fn commit_wait_with_retry(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        barrier: stateknot_core::CheckpointBarrier,
        registrations: Vec<stateknot_core::WaitRegistrationIntent>,
    ) -> Result<WaitCheckpointCommitOutcome, GraphLifecycleError> {
        let mut attempt = 1_u8;
        loop {
            match self
                .store
                .append_worker_wait_barrier(
                    append.clone(),
                    expected_revision,
                    barrier.clone(),
                    registrations.clone(),
                )
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn commit_success_with_retry(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        barrier: stateknot_core::CheckpointBarrier,
    ) -> Result<BarrierCommitOutcome, GraphLifecycleError> {
        let mut attempt = 1_u8;
        loop {
            match self
                .store
                .append_worker_barrier(append.clone(), projection.clone(), barrier.clone())
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(error) if self.can_retry_mutation(&error, attempt) => {
                    self.mutation_backoff(attempt).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn commit_failure_with_retry(
        &self,
        append: JournalAppend,
        projection: RunProjection,
    ) -> Result<AppendOutcome, GraphLifecycleError> {
        let mut attempt = 1_u8;
        loop {
            match self
                .store
                .append_worker(append.clone(), projection.clone())
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(error) if self.can_retry_mutation(&error, attempt) => {
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
    ) -> Result<LeaseReleaseOutcome, GraphLifecycleError> {
        let mut attempt = 1_u8;
        loop {
            match self.store.release_lease(fence).await {
                Ok(outcome) => return Ok(outcome),
                Err(error) if self.can_retry_mutation(&error, attempt) => {
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

    fn waiting_payload(
        &self,
        graph: &GraphReference,
        checkpoint: &CheckpointHead,
        successor_id: CheckpointId,
        successor_superstep: Superstep,
        wait_count: usize,
    ) -> Result<JournalPayload, GraphLifecycleError> {
        self.event_payload(
            "graph-barrier-waiting",
            json!({
                "operation": "graph_barrier_waiting",
                "graph_digest": digest_hex(graph.definition_digest()),
                "checkpoint_id": checkpoint.checkpoint_id().to_string(),
                "superstep": checkpoint.superstep().get().to_string(),
                "successor_checkpoint_id": successor_id.to_string(),
                "successor_superstep": successor_superstep.get().to_string(),
                "disposition": "wait",
                "wait_count": wait_count
            }),
        )
    }

    fn succeeded_payload(
        &self,
        graph: &GraphReference,
        checkpoint: &CheckpointHead,
        successor_id: CheckpointId,
        successor_superstep: Superstep,
        output_digest: Digest,
    ) -> Result<JournalPayload, GraphLifecycleError> {
        self.event_payload(
            "graph-barrier-succeeded",
            json!({
                "operation": "graph_barrier_succeeded",
                "graph_digest": digest_hex(graph.definition_digest()),
                "checkpoint_id": checkpoint.checkpoint_id().to_string(),
                "superstep": checkpoint.superstep().get().to_string(),
                "successor_checkpoint_id": successor_id.to_string(),
                "successor_superstep": successor_superstep.get().to_string(),
                "disposition": "succeeded",
                "output_digest": digest_hex(output_digest)
            }),
        )
    }

    fn failed_payload(
        &self,
        graph: &GraphReference,
        checkpoint: &CheckpointHead,
        failure_id: stateknot_core::FailureId,
        blockers: GraphDriveBlockers,
    ) -> Result<JournalPayload, GraphLifecycleError> {
        self.event_payload(
            "graph-run-failed",
            json!({
                "operation": "graph_run_failed",
                "graph_digest": digest_hex(graph.definition_digest()),
                "checkpoint_id": checkpoint.checkpoint_id().to_string(),
                "superstep": checkpoint.superstep().get().to_string(),
                "disposition": "failed",
                "failure_id": failure_id.to_string(),
                "in_flight": blockers.in_flight(),
                "failed": blockers.failed(),
                "exhausted": blockers.exhausted(),
                "unsupported": blockers.unsupported()
            }),
        )
    }

    fn cancelled_payload(
        &self,
        checkpoint: &CheckpointHead,
        failure_id: FailureId,
    ) -> Result<JournalPayload, GraphLifecycleError> {
        self.event_payload_with_schema(
            &self.cancellation_schema,
            "agent-cancellation-confirmed",
            json!({
                "operation": "agent_cancellation_confirmed",
                "graph_digest": digest_hex(checkpoint.graph().definition_digest()),
                "checkpoint_id": checkpoint.checkpoint_id().to_string(),
                "superstep": checkpoint.superstep().get().to_string(),
                "failure_id": failure_id.to_string()
            }),
        )
    }

    fn event_payload(
        &self,
        kind: &'static str,
        data: Value,
    ) -> Result<JournalPayload, GraphLifecycleError> {
        self.event_payload_with_schema(&self.journal_schema, kind, data)
    }

    fn event_payload_with_schema(
        &self,
        schema: &SchemaReference,
        kind: &'static str,
        data: Value,
    ) -> Result<JournalPayload, GraphLifecycleError> {
        let data = BoundedJson::try_from_value(data)
            .map_err(|_| GraphLifecycleError::EventPayloadInvalid)?;
        self.registry
            .schemas()
            .validate_bounded(schema, &data)
            .map_err(|_| GraphLifecycleError::EventPayloadInvalid)?;
        let kind =
            JournalEventKind::new(kind).map_err(|_| GraphLifecycleError::EventPayloadInvalid)?;
        JournalPayload::new(schema.clone(), kind, data)
            .map_err(|_| GraphLifecycleError::EventPayloadInvalid)
    }
}

impl fmt::Debug for DurableGraphLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableGraphLifecycle")
            .field("registry", &self.registry)
            .field("journal_schema", &self.journal_schema)
            .field("cancellation_schema", &self.cancellation_schema)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

/// Startup failure while binding graph lifecycle coordination.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableGraphLifecycleBuildError {
    /// The embedded lifecycle schema release artifact was malformed.
    #[error(transparent)]
    StandardSchema(#[from] StandardGraphLifecycleSchemaError),
    /// The embedded cancellation schema release artifact was malformed.
    #[error(transparent)]
    CancellationSchema(#[from] StandardAgentCancellationSchemaError),
    /// The executable registry omitted the required standard lifecycle schema.
    #[error("standard graph lifecycle event schema is unavailable")]
    JournalSchemaUnavailable,
    /// The executable registry omitted the required cancellation schema.
    #[error("standard agent cancellation event schema is unavailable")]
    CancellationSchemaUnavailable,
}

/// Payload-redacted graph lifecycle failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum GraphLifecycleError {
    /// `PostgreSQL` rejected or could not complete an operation.
    #[error(transparent)]
    Store {
        /// Exact payload-redacted provider failure.
        source: Box<StoreError>,
    },
    /// The handoff no longer matches the current run, journal, revision, lease,
    /// or runnable lifecycle.
    #[error("graph lifecycle handoff is stale")]
    StaleHandoff,
    /// A supposedly closed driver/lifecycle relationship failed.
    #[error("graph lifecycle handoff is invalid while attempting to {operation}")]
    InvalidHandoff {
        /// Stable payload-free operation label.
        operation: &'static str,
    },
    /// Trusted evidence could not be recovered.
    #[error("graph lifecycle evidence recovery failed: {0}")]
    Evidence(GraphLifecycleEvidenceError),
    /// A graph wait could not become an event-bound registration intent.
    #[error("graph lifecycle wait registration is invalid: {source}")]
    WaitRegistration {
        /// Exact public-safe wait validation failure.
        #[source]
        source: Box<DurableWaitError>,
    },
    /// The recovered request failed its pinned local input schema.
    #[error("graph lifecycle input failed its pinned schema: {source}")]
    InputSchema {
        /// Closed validation classification.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// The graph output failed its pinned local output schema.
    #[error("graph lifecycle output failed its pinned schema: {source}")]
    OutputSchema {
        /// Closed validation classification.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// Terminal output, artifacts, or accounting were intrinsically invalid.
    #[error("graph lifecycle successful result is invalid: {source}")]
    AgentResult {
        /// Exact public-safe intrinsic validation failure.
        #[source]
        source: Box<AgentResultError>,
    },
    /// Terminal success did not match trusted admission and budget snapshots.
    #[error("graph lifecycle successful result binding is invalid: {source}")]
    AgentResultValidation {
        /// Exact public-safe relationship failure.
        #[source]
        source: Box<AgentResultValidationError>,
    },
    /// Supervision attempted an invalid ordinary failure transition.
    #[error("graph lifecycle terminal failure is invalid: {source}")]
    RunFailure {
        /// Exact public-safe terminal failure validation error.
        #[source]
        source: RunFailureError,
    },
    /// Standard audit event data failed its pinned local schema or bounds.
    #[error("graph lifecycle event payload failed its pinned schema")]
    EventPayloadInvalid,
    /// A worker journal append could not be constructed.
    #[error("graph lifecycle worker journal append is invalid")]
    JournalAppendInvalid,
}

impl GraphLifecycleError {
    fn wait_registration(source: DurableWaitError) -> Self {
        Self::WaitRegistration {
            source: Box::new(source),
        }
    }

    const fn input_schema(source: GraphSchemaValidationError) -> Self {
        Self::InputSchema { source }
    }

    const fn output_schema(source: GraphSchemaValidationError) -> Self {
        Self::OutputSchema { source }
    }

    fn agent_result(source: AgentResultError) -> Self {
        Self::AgentResult {
            source: Box::new(source),
        }
    }

    fn agent_result_validation(source: AgentResultValidationError) -> Self {
        Self::AgentResultValidation {
            source: Box::new(source),
        }
    }

    const fn run_failure(source: RunFailureError) -> Self {
        Self::RunFailure { source }
    }
}

impl From<StoreError> for GraphLifecycleError {
    fn from(source: StoreError) -> Self {
        Self::Store {
            source: Box::new(source),
        }
    }
}

fn validate_barrier_scope(
    fence: &RunFence,
    journal_head: &JournalHead,
    checkpoint: &CheckpointHead,
    graph: &GraphReference,
) -> Result<(), GraphLifecycleError> {
    if journal_head.tenant_id() != fence.tenant_id()
        || journal_head.run_id() != fence.run_id()
        || checkpoint.tenant_id() != fence.tenant_id()
        || checkpoint.run_id() != fence.run_id()
        || checkpoint.graph() != graph
    {
        return Err(GraphLifecycleError::InvalidHandoff {
            operation: "bind graph barrier scope to its journal and fence",
        });
    }
    Ok(())
}

fn validate_cancellation_scope(
    fence: &RunFence,
    journal_head: &JournalHead,
    checkpoint: &CheckpointHead,
) -> Result<(), GraphLifecycleError> {
    if journal_head.tenant_id() != fence.tenant_id()
        || journal_head.run_id() != fence.run_id()
        || checkpoint.tenant_id() != fence.tenant_id()
        || checkpoint.run_id() != fence.run_id()
    {
        return Err(GraphLifecycleError::InvalidHandoff {
            operation: "bind cancellation confirmation to one run scope",
        });
    }
    Ok(())
}

fn validate_cancellation_checkpoint(
    run: &StoredRun,
    checkpoint: &CheckpointHead,
) -> Result<(), GraphLifecycleError> {
    let pointer = run
        .checkpoint()
        .ok_or(GraphLifecycleError::InvalidHandoff {
            operation: "bind cancellation confirmation to a checkpoint",
        })?;
    if pointer.checkpoint_id() != checkpoint.checkpoint_id()
        || pointer.superstep() != checkpoint.superstep()
        || pointer.digest() != checkpoint.digest()
    {
        return Err(GraphLifecycleError::StaleHandoff);
    }
    Ok(())
}

fn validate_blocked_checkpoint_scope(
    fence: &RunFence,
    journal_head: &JournalHead,
    checkpoint: &CheckpointHead,
) -> Result<(), GraphLifecycleError> {
    let checkpoint_journal = checkpoint.journal_head();
    let journal_position_is_valid = checkpoint_journal == journal_head
        || (checkpoint_journal.sequence() < journal_head.sequence()
            && checkpoint_journal.recorded_at() <= journal_head.recorded_at());
    if journal_head.tenant_id() != fence.tenant_id()
        || journal_head.run_id() != fence.run_id()
        || checkpoint.tenant_id() != fence.tenant_id()
        || checkpoint.run_id() != fence.run_id()
        || checkpoint_journal.tenant_id() != fence.tenant_id()
        || checkpoint_journal.run_id() != fence.run_id()
        || !journal_position_is_valid
    {
        return Err(GraphLifecycleError::InvalidHandoff {
            operation: "bind blocked checkpoint to its journal and fence",
        });
    }
    Ok(())
}

fn worker_append(
    fence: &RunFence,
    head: JournalHead,
    event_id: EventId,
    payload: JournalPayload,
) -> Result<JournalAppend, GraphLifecycleError> {
    let intent = JournalEventIntent::worker(
        fence.tenant_id().clone(),
        fence.run_id(),
        event_id,
        fence.clone(),
        payload,
    )
    .map_err(|_| GraphLifecycleError::JournalAppendInvalid)?;
    JournalAppend::new(JournalExpectation::exact(head), intent)
        .map_err(|_| GraphLifecycleError::JournalAppendInvalid)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_options_enforce_production_retry_bounds() {
        assert_eq!(
            DurableGraphLifecycleOptions::new(0, Duration::from_millis(1)),
            Err(DurableGraphLifecycleOptionsError::InvalidMutationAttempts)
        );
        assert_eq!(
            DurableGraphLifecycleOptions::new(1, Duration::ZERO),
            Err(DurableGraphLifecycleOptionsError::InvalidMutationRetryDelay)
        );
        let options = DurableGraphLifecycleOptions::new(4, Duration::from_millis(50)).unwrap();
        assert_eq!(options.maximum_mutation_attempts(), 4);
        assert_eq!(
            options.mutation_retry_initial_delay(),
            Duration::from_millis(50)
        );
    }
}
