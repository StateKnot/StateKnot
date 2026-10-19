// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Concrete current-scope recovery and framework actions.

use super::{
    AssertUnwindSafe, AttemptId, BoxFuture, BudgetUsage, Checkpoint, CheckpointId,
    ClaimedRunRecovery, CompactByteCounter, DurableGraphDriver, EventId, ExecutableGraph,
    GraphBarrierDisposition, GraphBarrierPlanError, GraphDriveReplayReport, GraphDriveReport,
    GraphDriverError, GraphNodeScheduling, JournalHead, NodeAttemptCommitOutcome,
    ReadyNodeRecoveryPlan, RecoveryNodeKind, RunFence, RunRevision, StoreError, StoredRun,
};
use futures_util::FutureExt;
use stateknot_core::{
    CompiledGraph, GraphFrameBarrierPlan, GraphFrameEntryPlan, GraphReducerError,
    GraphSchemaValidationError, NodeId,
};
use stateknot_store_postgres::{
    GraphFrameBarrierCommitOutcome, GraphFrameEntryCommitOutcome, GraphFrameReturnCommitOutcome,
    StoredActiveGraphFrame,
};

pub(super) struct IterationScope {
    pub(super) root: Box<Checkpoint>,
    pub(super) active: Option<Box<StoredActiveGraphFrame>>,
    pub(super) executable: ExecutableGraph,
    // Read-only observation control; never part of recovery or launch authority.
    shutdown: super::CancellationSignal,
}

// Complete owning values for this actual read only. Allocation changes layout;
// the original replay, quarantine and live-fence checks remain authoritative.
pub(super) enum DriveStartup {
    Dispatch {
        continuation: Box<ReplayContinuation>,
        report: GraphDriveReport,
    },
    Cancellation {
        handoff: Box<super::GraphCancellationHandoff>,
        report: GraphDriveReport,
    },
}

pub(super) struct IterationRead<'driver> {
    pub(super) recovery: Box<ClaimedRunRecovery<'driver>>,
    pub(super) scope: Box<IterationScope>,
    pub(super) plan: Box<ReadyNodeRecoveryPlan>,
    pub(super) live: Box<StoredRun>,
}

pub(super) struct ReplayContinuation {
    pub(super) root: stateknot_core::CheckpointHead,
    pub(super) leaf: Option<stateknot_core::GraphFrameCheckpointHead>,
    pub(super) journal: JournalHead,
}

impl ReplayContinuation {
    pub(super) fn matches(&self, scope: &IterationScope, run: &StoredRun) -> bool {
        self.root == scope.root.head()
            && self.leaf == scope.leaf()
            && run.journal_head() == Some(&self.journal)
    }
}

impl IterationScope {
    pub(super) fn leaf(&self) -> Option<stateknot_core::GraphFrameCheckpointHead> {
        self.active
            .as_ref()
            .map(|active| active.checkpoint().head())
    }
}

impl DurableGraphDriver {
    // G1 resumes an existing published Join. It does not admit a new frame
    // whose application would need fresh scoped child suspension.
    fn validate_fresh_frame_dispatch_profile(
        &self,
        target: &CompiledGraph,
    ) -> Result<(), GraphDriverError> {
        if target.child_runs().is_some() {
            return Err(GraphDriverError::ScopedChildJoinUnsupported);
        }
        let mut pending = target
            .frame_calls()
            .into_iter()
            .flat_map(|policy| policy.calls().iter())
            .map(|call| call.target().clone())
            .collect::<Vec<_>>();
        let mut visited = std::collections::HashSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            let executable = self.registry.resolve(&reference).ok_or_else(|| {
                GraphDriverError::ExecutableGraphUnavailable {
                    graph: Box::new(reference.clone()),
                }
            })?;
            if executable.graph().child_runs().is_some() {
                return Err(GraphDriverError::ScopedChildJoinUnsupported);
            }
            if let Some(policy) = executable.graph().frame_calls() {
                pending.extend(policy.calls().iter().map(|call| call.target().clone()));
            }
        }
        Ok(())
    }

    fn validate_iteration_dispatch_profile<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        scope: &'a IterationScope,
        plan: &'a ReadyNodeRecoveryPlan,
    ) -> BoxFuture<'a, Result<(), GraphDriverError>> {
        Box::pin(async move {
            if recovery.initial_run().lifecycle().status()
                == super::RunStatus::CancellationRequested
            {
                return Ok(());
            }
            let checkpoint = scope.active.as_ref().map_or_else(
                || scope.root.head(),
                |active| active.checkpoint().checkpoint().head(),
            );
            if plan.fence() != recovery.fence()
                || Some(plan.journal_head()) != recovery.initial_run().journal_head()
                || plan.checkpoint().head() != checkpoint
                || plan.frame()
                    != scope
                        .active
                        .as_ref()
                        .map(|active| active.checkpoint().frame())
            {
                return Err(GraphDriverError::StaleBarrierSnapshot);
            }
            for node in plan
                .nodes()
                .iter()
                .filter(|node| node.kind() != RecoveryNodeKind::Completed)
            {
                if let Some(call) = scope
                    .executable
                    .graph()
                    .frame_calls()
                    .and_then(|policy| policy.call(node.activation().node_id()))
                {
                    if node.kind() != RecoveryNodeKind::Dispatchable {
                        continue;
                    }
                    let target = self.registry.resolve(call.target()).ok_or_else(|| {
                        GraphDriverError::ExecutableGraphUnavailable {
                            graph: Box::new(call.target().clone()),
                        }
                    })?;
                    self.validate_fresh_frame_dispatch_profile(target.graph())?;
                    continue;
                }
                let declares_children =
                    scope.executable.graph().child_runs().is_some_and(|policy| {
                        policy
                            .declarations()
                            .iter()
                            .any(|declaration| declaration.node_id() == node.activation().node_id())
                    });
                if scope.active.is_none() || !declares_children {
                    continue;
                }
                let executor =
                    Self::resolve_node_executor(&scope.executable, node.activation().node_id())?;
                if !executor.supports_child_join()
                    || executor.scheduling() != GraphNodeScheduling::Exclusive
                {
                    return Err(GraphDriverError::ScopedChildJoinUnsupported);
                }
                let loading = self.store.load_child_join(node.activation());
                let record = self
                    .store
                    .with_corruption_quarantine(recovery.quarantine_context().clone(), loading)
                    .await?
                    .ok_or(GraphDriverError::ScopedChildJoinUnsupported)?;
                if record.head().is_none()
                    || record.binding().is_none()
                    || record.consumed().is_some()
                {
                    return Err(GraphDriverError::ScopedChildJoinUnsupported);
                }
                // Reuse the full slot, terminal, quarantine, executable and
                // output-schema checks before any caller rebind or fresh Start.
                // This read is not launch authority; Store rechecks each write.
                let _join = Box::pin(crate::GraphChildJoin::prepare(
                    self.store.clone(),
                    self.registry.clone(),
                    &record,
                ))
                .await?;
            }
            Ok(())
        })
    }

    pub(super) fn load_iteration_scope<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        shutdown: &'a super::CancellationSignal,
    ) -> BoxFuture<'a, Result<Box<IterationScope>, GraphDriverError>> {
        Box::pin(async move {
            let initial = recovery.initial_run();
            let pointer = initial
                .checkpoint()
                .ok_or(StoreError::ReadyNodeRecoveryCheckpointMissing)?;
            let root = self
                .load_iteration_root(recovery, pointer.checkpoint_id())
                .await?;
            let active = self.load_iteration_active(recovery).await?;
            if root.head().checkpoint_id() != pointer.checkpoint_id()
                || root.head().digest() != pointer.digest()
                || root.head().superstep() != pointer.superstep()
                || active.as_ref().is_some_and(|active| {
                    active.run().journal_head() != initial.journal_head()
                        || active.run().checkpoint() != initial.checkpoint()
                })
            {
                return Err(GraphDriverError::StaleBarrierSnapshot);
            }
            let reference = active.as_ref().map_or_else(
                || root.graph(),
                |active| active.checkpoint().frame().target(),
            );
            let executable = self.registry.resolve(reference).cloned().ok_or_else(|| {
                GraphDriverError::ExecutableGraphUnavailable {
                    graph: Box::new(reference.clone()),
                }
            })?;
            recovery.revalidate().await?;
            Ok(Box::new(IterationScope {
                root,
                active,
                executable,
                shutdown: shutdown.clone(),
            }))
        })
    }

    pub(super) fn prepare_drive_start<'a>(
        &'a self,
        fence: &'a RunFence,
        shutdown: &'a super::CancellationSignal,
    ) -> BoxFuture<'a, Result<Box<DriveStartup>, GraphDriverError>> {
        Box::pin(async move {
            // Recovery, graph loading, and replay are bounded but may still consume
            // a meaningful fraction of a short lease. Refresh below half-life
            // before those reads so a valid near-expiry claim is not lost merely
            // because integrity verification ran before the first durable start.
            let (startup_renewals, startup_retries) = self.refresh_near_expiry_lease(fence).await?;
            let initial_recovery = Box::new(self.begin_recovery(fence).await?);
            let initial_scope = self
                .load_iteration_scope(&initial_recovery, shutdown)
                .await?;
            let replay = Box::new(
                initial_recovery
                    .validate_graph_replay(
                        initial_scope.leaf().as_ref(),
                        self.registry.schemas(),
                        |reference| {
                            self.registry
                                .resolve(reference)
                                .map(ExecutableGraph::reducer)
                        },
                        self.options.replay_limits,
                    )
                    .await?,
            );
            let continuation = ReplayContinuation {
                root: initial_scope.root.head(),
                leaf: replay.leaf().cloned(),
                journal: replay.journal_head().clone(),
            };
            let mut report = GraphDriveReport::new(replay.root_report());
            report.whole_replay_passes = 1;
            report.whole_replay = GraphDriveReplayReport::from_completed(&replay);
            report.lease_renewals = startup_renewals;
            report.mutation_retries = startup_retries;
            if initial_scope.active.is_some()
                && initial_scope.executable.graph().child_runs().is_some()
                && initial_recovery.initial_run().lifecycle().status()
                    == super::RunStatus::CancellationRequested
            {
                // Only the complete selected owning proof can bypass ordinary
                // dispatch preparation. This branch never plans or starts nodes.
                let preparing = async {
                    let leaf = initial_scope
                        .leaf()
                        .ok_or(GraphDriverError::StaleBarrierSnapshot)?;
                    initial_recovery
                        .validate_registered_join_frame_cancellation(&leaf)
                        .await?;
                    let live = initial_recovery.revalidate().await?;
                    if !continuation.matches(&initial_scope, &live) {
                        return Err(GraphDriverError::StaleBarrierSnapshot);
                    }
                    super::cancellation_handoff(
                        &initial_recovery,
                        &initial_scope,
                        &live,
                        &continuation.journal,
                    )
                };
                let handoff = match preparing.await {
                    Ok(handoff) => handoff,
                    Err(error) => {
                        // Retain exact-fence cleanup and its complete failure.
                        self.release_with_retry(fence, &mut report).await?;
                        return Err(error);
                    }
                };
                return Ok(Box::new(DriveStartup::Cancellation { handoff, report }));
            }
            drop(initial_recovery);
            drop(initial_scope);

            Ok(Box::new(DriveStartup::Dispatch {
                continuation: Box::new(continuation),
                report,
            }))
        })
    }

    pub(super) fn load_drive_iteration<'a, 'driver: 'a>(
        &'driver self,
        fence: &'a RunFence,
        shutdown: &'a super::CancellationSignal,
        report: &'a mut GraphDriveReport,
        continuation: &'a mut ReplayContinuation,
    ) -> BoxFuture<'a, Result<Box<IterationRead<'driver>>, GraphDriverError>> {
        Box::pin(async move {
            let recovery = Box::new(self.begin_recovery(fence).await?);
            let scope = self.load_iteration_scope(&recovery, shutdown).await?;
            if !continuation.matches(&scope, recovery.initial_run()) {
                let replay = recovery
                    .validate_graph_replay(
                        scope.leaf().as_ref(),
                        self.registry.schemas(),
                        |reference| {
                            self.registry
                                .resolve(reference)
                                .map(ExecutableGraph::reducer)
                        },
                        self.options.replay_limits,
                    )
                    .await?;
                *continuation = ReplayContinuation {
                    root: scope.root.head(),
                    leaf: replay.leaf().cloned(),
                    journal: replay.journal_head().clone(),
                };
                report.whole_replay_passes = report.whole_replay_passes.saturating_add(1);
                report.whole_replay = GraphDriveReplayReport::from_completed(&replay);
            }
            let plan = Box::new(match scope.active.as_ref() {
                Some(active) => {
                    recovery
                        .plan_graph_frame_ready_nodes(&active.checkpoint().head())
                        .await?
                }
                None => recovery.plan_ready_nodes().await?,
            });
            if let Err(error) = self
                .validate_iteration_dispatch_profile(&recovery, &scope, &plan)
                .await
            {
                // Preserve exact-fence cleanup, including its actual failure.
                self.release_with_retry(fence, report).await?;
                return Err(error);
            }
            let live = Box::new(recovery.revalidate().await?);
            Ok(Box::new(IterationRead {
                recovery,
                scope,
                plan,
                live,
            }))
        })
    }

    fn load_iteration_root<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        checkpoint: CheckpointId,
    ) -> BoxFuture<'a, Result<Box<Checkpoint>, GraphDriverError>> {
        Box::pin(async move {
            let loading: BoxFuture<'_, Result<Box<Checkpoint>, StoreError>> = Box::pin(async {
                self.store
                    .load_checkpoint(
                        recovery.fence().tenant_id(),
                        recovery.fence().run_id(),
                        checkpoint,
                    )
                    .await
                    .map(Box::new)
            });
            self.store
                .with_corruption_quarantine(recovery.quarantine_context().clone(), loading)
                .await
                .map_err(GraphDriverError::from)
        })
    }

    fn load_iteration_active<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
    ) -> BoxFuture<'a, Result<Option<Box<StoredActiveGraphFrame>>, GraphDriverError>> {
        Box::pin(async move {
            let loading: BoxFuture<'_, Result<Option<Box<StoredActiveGraphFrame>>, StoreError>> =
                Box::pin(async {
                    self.store
                        .load_active_graph_frame(
                            recovery.fence().tenant_id(),
                            recovery.fence().run_id(),
                        )
                        .await
                        .map(|active| active.map(Box::new))
                });
            self.store
                .with_corruption_quarantine(recovery.quarantine_context().clone(), loading)
                .await
                .map_err(GraphDriverError::from)
        })
    }

    fn complete_direct_usage<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        scope: &'a IterationScope,
        report: &'a mut GraphDriveReport,
    ) -> BoxFuture<'a, Result<Option<BudgetUsage>, GraphDriverError>> {
        Box::pin(async move {
            let context = Box::new(crate::GraphDirectUsageEvidenceContext::new(
                recovery.initial_run().lifecycle().provenance().clone(),
                scope.root.head(),
                scope.leaf(),
                recovery
                    .initial_run()
                    .journal_head()
                    .cloned()
                    .ok_or(GraphDriverError::MissingJournalHead)?,
                recovery.fence().clone(),
            ));
            let result = self
                .observe_complete_direct_usage(recovery, scope, &context)
                .await;
            if result.is_err() {
                self.release_with_retry(recovery.fence(), report).await?;
            }
            result
        })
    }

    fn direct_usage_deadline<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
    ) -> BoxFuture<'a, Result<tokio::time::Instant, GraphDriverError>> {
        Box::pin(async move {
            let started = tokio::time::Instant::now();
            let observation = self.store.observe_live_lease(recovery.fence()).await?;
            let admission = Box::new(
                Box::pin(self.store.with_corruption_quarantine(
                    recovery.quarantine_context().clone(),
                    self.store.load_agent_admission(
                        recovery.fence().tenant_id(),
                        recovery.fence().run_id(),
                    ),
                ))
                .await?,
            );
            let remaining = admission
                .admission()
                .intent()
                .budget()
                .deadline()
                .unix_micros()
                .checked_sub(observation.observed_at().unix_micros())
                .and_then(|value| u64::try_from(value).ok())
                .filter(|value| *value > 0)
                .ok_or(GraphDriverError::DirectUsageEvidence {
                    source: crate::GraphLifecycleEvidenceError::TemporarilyUnavailable,
                })?;
            drop(admission);
            let deadline = started
                .checked_add(
                    std::time::Duration::from_micros(remaining)
                        .min(self.options.node_execution_timeout),
                )
                .ok_or(GraphDriverError::LeaseTimestampOverflow)?;
            Ok(deadline)
        })
    }

    fn observe_complete_direct_usage<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        scope: &'a IterationScope,
        context: &'a crate::GraphDirectUsageEvidenceContext,
    ) -> BoxFuture<'a, Result<Option<BudgetUsage>, GraphDriverError>> {
        Box::pin(async move {
            let deadline = self.direct_usage_deadline(recovery).await?;
            let observation = async {
                match self.evidence.as_ref() {
                    Some(provider) => AssertUnwindSafe(async {
                        provider.direct_usage_evidence(context.clone()).await
                    })
                    .catch_unwind()
                    .await
                    .map_err(|_| crate::GraphLifecycleEvidenceError::Unavailable)
                    .and_then(std::convert::identity),
                    None => Err(crate::GraphLifecycleEvidenceError::Unavailable),
                }
            };
            tokio::pin!(observation);
            let evidence = tokio::select! {
                biased;
                () = scope.shutdown.cancelled() => return Ok(None),
                cancellation = self.observe_run_cancellation(recovery.fence()) => {
                    cancellation?;
                    return Ok(None);
                }
                evidence = &mut observation => evidence,
                () = tokio::time::sleep_until(deadline) => {
                    Err(crate::GraphLifecycleEvidenceError::TemporarilyUnavailable)
                }
            }
            .map_err(|source| GraphDriverError::DirectUsageEvidence { source })?;
            if evidence.context() != context
                || scope.active.as_ref().is_some_and(|active| {
                    evidence
                        .usage()
                        .validate_monotonic_after(active.minimum_direct_usage())
                        .is_err()
                })
            {
                return Err(GraphDriverError::InvalidDirectUsageEvidence);
            }
            Ok(Some(evidence.into_usage()))
        })
    }

    pub(super) fn enter_declared_frame<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        scope: &'a IterationScope,
        plan: &'a ReadyNodeRecoveryPlan,
        node_id: &'a NodeId,
        report: &'a mut GraphDriveReport,
        continuation: &'a mut ReplayContinuation,
    ) -> BoxFuture<'a, Result<(), GraphDriverError>> {
        Box::pin(async move {
            let call = scope
                .executable
                .graph()
                .frame_calls()
                .and_then(|policy| policy.call(node_id))
                .ok_or(GraphDriverError::RuntimeInvariant {
                    operation: "recover declared framework call",
                })?;
            let target = self.registry.resolve(call.target()).ok_or_else(|| {
                GraphDriverError::ExecutableGraphUnavailable {
                    graph: Box::new(call.target().clone()),
                }
            })?;
            let checkpoint_id = CheckpointId::generate();
            let attempt_id = AttemptId::generate();
            let entry = match scope.active.as_ref() {
                Some(active) => GraphFrameEntryPlan::for_frame(
                    call,
                    scope.executable.graph(),
                    active.checkpoint(),
                    target.graph(),
                    checkpoint_id,
                    attempt_id,
                    recovery.fence().clone(),
                ),
                None => GraphFrameEntryPlan::for_root(
                    call,
                    scope.executable.graph(),
                    &scope.root,
                    target.graph(),
                    checkpoint_id,
                    attempt_id,
                    recovery.fence().clone(),
                ),
            }
            .map_err(|_| GraphDriverError::RuntimeInvariant {
                operation: "prepare exact declared framework entry",
            })?;
            let Some(usage) = self.complete_direct_usage(recovery, scope, report).await? else {
                return Ok(());
            };
            recovery.revalidate().await?;
            let event_id = EventId::generate();
            let mut attempt = 1_u8;
            loop {
                match self
                    .store
                    .enter_graph_frame(
                        entry.clone(),
                        event_id,
                        plan.journal_head().clone(),
                        usage.clone(),
                        self.registry.schemas(),
                    )
                    .await
                {
                    Ok(
                        GraphFrameEntryCommitOutcome::Committed(record)
                        | GraphFrameEntryCommitOutcome::Idempotent(record),
                    ) => {
                        continuation.leaf = Some(record.entry().checkpoint().head());
                        continuation.journal = record.event().head();
                        report.frame_entries_committed =
                            report.frame_entries_committed.saturating_add(1);
                        report.durable_events = report.durable_events.saturating_add(1);
                        return Ok(());
                    }
                    Ok(_) => {
                        return Err(GraphDriverError::RuntimeInvariant {
                            operation: "observe framework entry outcome",
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
        })
    }
}

impl DurableGraphDriver {
    pub(super) fn commit_scoped_barrier<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        scope: &'a IterationScope,
        plan: Box<GraphFrameBarrierPlan>,
        revision: RunRevision,
        report: &'a mut GraphDriveReport,
        continuation: &'a mut ReplayContinuation,
    ) -> BoxFuture<'a, Result<Option<Box<GraphFrameBarrierCommitOutcome>>, GraphDriverError>> {
        Box::pin(async move {
            let Some(usage) = self.complete_direct_usage(recovery, scope, report).await? else {
                return Ok(None);
            };
            recovery.revalidate().await?;
            let event_id = EventId::generate();
            let observed = recovery
                .initial_run()
                .journal_head()
                .cloned()
                .ok_or(GraphDriverError::MissingJournalHead)?;
            let waiting = matches!(plan.disposition(), GraphBarrierDisposition::Wait { .. });
            let mut attempt = 1_u8;
            loop {
                let outcome = if waiting {
                    self.store
                        .commit_graph_frame_wait(
                            plan.as_ref().clone(),
                            event_id,
                            recovery.fence().clone(),
                            observed.clone(),
                            usage.clone(),
                            revision,
                            self.registry.schemas(),
                            scope.executable.reducer(),
                        )
                        .await
                } else {
                    self.store
                        .commit_graph_frame_barrier(
                            plan.as_ref().clone(),
                            event_id,
                            recovery.fence().clone(),
                            observed.clone(),
                            usage.clone(),
                            self.registry.schemas(),
                            scope.executable.reducer(),
                        )
                        .await
                };
                match outcome {
                    Ok(
                        outcome @ (GraphFrameBarrierCommitOutcome::Committed(_)
                        | GraphFrameBarrierCommitOutcome::Idempotent(_)),
                    ) => {
                        let (GraphFrameBarrierCommitOutcome::Committed(record)
                        | GraphFrameBarrierCommitOutcome::Idempotent(record)) = &outcome
                        else {
                            return Err(GraphDriverError::RuntimeInvariant {
                                operation: "retain scoped barrier observation",
                            });
                        };
                        continuation.leaf = Some(record.checkpoint().head());
                        continuation.journal = record.event().head();
                        report.durable_events = report.durable_events.saturating_add(1);
                        report.frame_barriers_committed =
                            report.frame_barriers_committed.saturating_add(1);
                        return Ok(Some(Box::new(outcome)));
                    }
                    Ok(_) => {
                        return Err(GraphDriverError::RuntimeInvariant {
                            operation: "observe complete scoped barrier",
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
        })
    }

    #[allow(clippy::too_many_lines)]
    pub(super) fn return_terminal_frame<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        scope: &'a IterationScope,
        report: &'a mut GraphDriveReport,
        continuation: &'a mut ReplayContinuation,
    ) -> BoxFuture<'a, Result<(), GraphDriverError>> {
        Box::pin(async move {
            let active = scope
                .active
                .as_ref()
                .ok_or(GraphDriverError::RuntimeInvariant {
                    operation: "return current frame",
                })?;
            let terminal = Box::pin(self.store.with_corruption_quarantine(
                recovery.quarantine_context().clone(),
                async {
                    let checkpoint = active.checkpoint();
                    let base_head =
                        checkpoint
                            .checkpoint()
                            .parent()
                            .ok_or(StoreError::CorruptData {
                                record: "frame terminal base",
                            })?;
                    let namespace = checkpoint.frame().namespace();
                    let stored = self
                        .store
                        .load_graph_frame_barrier(
                            recovery.fence().tenant_id(),
                            recovery.fence().run_id(),
                            namespace,
                            base_head.checkpoint_id(),
                        )
                        .await?
                        .ok_or(StoreError::CorruptData {
                            record: "frame terminal whole barrier",
                        })?;
                    if stored.checkpoint().head() != checkpoint.head()
                        || !matches!(
                            stored.disposition(),
                            GraphBarrierDisposition::Terminal { .. }
                        )
                    {
                        return Err(StoreError::CorruptData {
                            record: "frame terminal disposition",
                        });
                    }
                    let base = self
                        .store
                        .load_graph_frame_checkpoint(
                            recovery.fence().tenant_id(),
                            recovery.fence().run_id(),
                            namespace,
                            base_head.checkpoint_id(),
                        )
                        .await?;
                    let mut results = Vec::with_capacity(stored.barrier().result_heads().len());
                    let mut bytes = 0_usize;
                    for head in stored.barrier().result_heads() {
                        let result = self
                            .store
                            .load_pending_node_result(head.activation())
                            .await?;
                        if result.head() != *head {
                            return Err(StoreError::CorruptData {
                                record: "frame terminal result head",
                            });
                        }
                        let mut counter = CompactByteCounter::default();
                        serde_json::to_writer(&mut counter, &result).map_err(|_| {
                            StoreError::Encoding {
                                record: "frame terminal result",
                            }
                        })?;
                        bytes = bytes
                            .checked_add(counter.bytes)
                            .ok_or(StoreError::GraphReplayResourceLimit)?;
                        if bytes > self.options.replay_limits.maximum_barrier_result_bytes() {
                            return Err(StoreError::GraphReplayResourceLimit);
                        }
                        results.push(result);
                    }
                    let terminal = scope
                        .executable
                        .graph()
                        .plan_frame_barrier(
                            &base,
                            &results,
                            checkpoint.checkpoint().checkpoint_id(),
                            self.registry.schemas(),
                            scope.executable.reducer(),
                        )
                        .map_err(|error| historical_plan_error(&error))?;
                    if terminal.barrier() != stored.barrier()
                        || terminal.disposition() != &stored.disposition()
                        || !checkpoint
                            .checkpoint()
                            .matches_write(terminal.barrier().successor())
                    {
                        return Err(StoreError::CorruptData {
                            record: "frame terminal complete application replay",
                        });
                    }
                    Ok(terminal)
                },
            ))
            .await?;
            let parent_graph = active
                .checkpoint()
                .frame()
                .origin()
                .base_checkpoint()
                .graph();
            let parent = self.registry.resolve(parent_graph).ok_or_else(|| {
                GraphDriverError::ExecutableGraphUnavailable {
                    graph: Box::new(parent_graph.clone()),
                }
            })?;
            let Some(usage) = self.complete_direct_usage(recovery, scope, report).await? else {
                return Ok(());
            };
            recovery.revalidate().await?;
            let event_id = EventId::generate();
            let observed = recovery
                .initial_run()
                .journal_head()
                .cloned()
                .ok_or(GraphDriverError::MissingJournalHead)?;
            let mut attempt = 1_u8;
            loop {
                match self
                    .store
                    .return_graph_frame(
                        terminal.clone(),
                        event_id,
                        recovery.fence().clone(),
                        observed.clone(),
                        usage.clone(),
                        self.registry.schemas(),
                        parent.reducer(),
                    )
                    .await
                {
                    Ok(
                        GraphFrameReturnCommitOutcome::Committed(record)
                        | GraphFrameReturnCommitOutcome::Idempotent(record),
                    ) => {
                        continuation.leaf = active
                            .open_frames()
                            .len()
                            .checked_sub(2)
                            .map(|index| active.open_frames()[index].checkpoint().clone());
                        continuation.journal = record.event().head();
                        report.frame_returns_committed =
                            report.frame_returns_committed.saturating_add(1);
                        report.durable_events = report.durable_events.saturating_add(1);
                        return Ok(());
                    }
                    Ok(_) => {
                        return Err(GraphDriverError::RuntimeInvariant {
                            operation: "observe complete frame return",
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
        })
    }
}

fn historical_plan_error(error: &GraphBarrierPlanError) -> StoreError {
    match error {
        GraphBarrierPlanError::ReducerReferenceMismatch
        | GraphBarrierPlanError::SchemaValidatorPanicked { .. }
        | GraphBarrierPlanError::ReducerPanicked
        | GraphBarrierPlanError::SchemaValidation {
            source: GraphSchemaValidationError::Unavailable,
            ..
        }
        | GraphBarrierPlanError::ReducerFailed {
            source: GraphReducerError::Unavailable | GraphReducerError::ResourceLimit,
        } => StoreError::GraphReplayDependencyUnavailable,
        _ => StoreError::CorruptData {
            record: "frame terminal replay plan",
        },
    }
}

impl DurableGraphDriver {
    pub(super) fn rebind_current_caller<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        scope: &'a IterationScope,
        report: &'a mut GraphDriveReport,
        continuation: &'a mut ReplayContinuation,
    ) -> BoxFuture<'a, Result<bool, GraphDriverError>> {
        Box::pin(async move {
            let Some(active) = scope.active.as_ref() else {
                return Ok(false);
            };
            if active.caller().fence() == recovery.fence() {
                return Ok(false);
            }
            let Some(usage) = self.complete_direct_usage(recovery, scope, report).await? else {
                return Ok(true);
            };
            recovery.revalidate().await?;
            let event_id = EventId::generate();
            let physical_attempt = AttemptId::generate();
            let observed = recovery
                .initial_run()
                .journal_head()
                .cloned()
                .ok_or(GraphDriverError::MissingJournalHead)?;
            let mut attempt = 1_u8;
            loop {
                match self
                    .store
                    .rebind_graph_frame_caller(
                        active.checkpoint().frame(),
                        physical_attempt,
                        event_id,
                        recovery.fence().clone(),
                        observed.clone(),
                        usage.clone(),
                    )
                    .await
                {
                    Ok(
                        NodeAttemptCommitOutcome::Committed { event, .. }
                        | NodeAttemptCommitOutcome::Idempotent { event, .. },
                    ) => {
                        continuation.journal = event.head();
                        report.durable_events = report.durable_events.saturating_add(1);
                        report.frame_callers_rebound =
                            report.frame_callers_rebound.saturating_add(1);
                        return Ok(true);
                    }
                    Ok(_) => {
                        return Err(GraphDriverError::RuntimeInvariant {
                            operation: "observe leaf caller binding",
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
        })
    }
}
