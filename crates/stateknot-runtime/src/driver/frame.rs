// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Concrete current-scope recovery and framework actions.

use super::{
    AssertUnwindSafe, AttemptId, BoxFuture, BudgetUsage, Checkpoint, CheckpointId,
    ClaimedRunRecovery, CompactByteCounter, DurableGraphDriver, EventId, ExecutableGraph,
    GraphBarrierDisposition, GraphBarrierPlanError, GraphDriveReport, GraphDriverError,
    JournalHead, NodeAttemptCommitOutcome, ReadyNodeRecoveryPlan, RunRevision, StoreError,
    StoredRun,
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
    pub(super) root: Checkpoint,
    pub(super) active: Option<StoredActiveGraphFrame>,
    pub(super) executable: ExecutableGraph,
    // Read-only observation control; never part of recovery or launch authority.
    shutdown: super::CancellationSignal,
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
    // Only the actual Root's reachable frame-call closure is restricted. A
    // registry can also serve independent Root child graphs without dispatching
    // those graphs under a frame namespace.
    pub(super) fn validate_frame_dispatch_profile(
        &self,
        root: &CompiledGraph,
    ) -> Result<(), GraphDriverError> {
        let mut pending = root
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

    pub(super) fn load_iteration_scope<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        shutdown: &'a super::CancellationSignal,
    ) -> BoxFuture<'a, Result<IterationScope, GraphDriverError>> {
        Box::pin(async move {
            let initial = recovery.initial_run();
            let pointer = initial
                .checkpoint()
                .ok_or(StoreError::ReadyNodeRecoveryCheckpointMissing)?;
            let root = self
                .store
                .with_corruption_quarantine(
                    recovery.quarantine_context().clone(),
                    self.store.load_checkpoint(
                        recovery.fence().tenant_id(),
                        recovery.fence().run_id(),
                        pointer.checkpoint_id(),
                    ),
                )
                .await?;
            let active = self
                .store
                .with_corruption_quarantine(
                    recovery.quarantine_context().clone(),
                    self.store.load_active_graph_frame(
                        recovery.fence().tenant_id(),
                        recovery.fence().run_id(),
                    ),
                )
                .await?;
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
            Ok(IterationScope {
                root,
                active,
                executable,
                shutdown: shutdown.clone(),
            })
        })
    }

    fn complete_direct_usage<'a>(
        &'a self,
        recovery: &'a ClaimedRunRecovery<'_>,
        scope: &'a IterationScope,
        report: &'a mut GraphDriveReport,
    ) -> BoxFuture<'a, Result<Option<BudgetUsage>, GraphDriverError>> {
        Box::pin(async move {
            let context = crate::GraphDirectUsageEvidenceContext::new(
                recovery.initial_run().lifecycle().provenance().clone(),
                scope.root.head(),
                scope.leaf(),
                recovery
                    .initial_run()
                    .journal_head()
                    .cloned()
                    .ok_or(GraphDriverError::MissingJournalHead)?,
                recovery.fence().clone(),
            );
            let result: BoxFuture<'_, Result<Option<BudgetUsage>, GraphDriverError>> =
                Box::pin(async {
                    let started = tokio::time::Instant::now();
                    let observation = self.store.observe_live_lease(recovery.fence()).await?;
                    let admission = Box::pin(self.store.with_corruption_quarantine(
                        recovery.quarantine_context().clone(),
                        self.store.load_agent_admission(
                            recovery.fence().tenant_id(),
                            recovery.fence().run_id(),
                        ),
                    ))
                    .await?;
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
                    if evidence.context() != &context
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
                });
            let result = result.await;
            if result.is_err() {
                self.release_with_retry(recovery.fence(), report).await?;
            }
            result
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
        plan: GraphFrameBarrierPlan,
        revision: RunRevision,
        report: &'a mut GraphDriveReport,
        continuation: &'a mut ReplayContinuation,
    ) -> BoxFuture<'a, Result<Option<GraphFrameBarrierCommitOutcome>, GraphDriverError>> {
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
                            plan.clone(),
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
                            plan.clone(),
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
                        return Ok(Some(outcome));
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
