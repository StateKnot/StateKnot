// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Concrete scoped stop, whole-stack closure and Root cancellation continuation.

use super::{
    AppendOutcome, BoxFuture, CheckpointHead, DurableGraphLifecycle, EventId, Failure,
    GraphBarrierLifecycleOutcome, GraphCancellationHandoff, GraphDirectUsageEvidenceContext,
    GraphLifecycleError, GraphLifecycleEvidenceError, JournalAppend, JournalEventIntent,
    JournalExpectation, JournalHead, RunFence, RunProjection, RunStatus, RunTransition, StoredRun,
    validate_cancellation_checkpoint, validate_cancellation_scope,
};
use futures_util::FutureExt;
use stateknot_core::{GraphFrameCheckpointHead, JournalEventSource};
use stateknot_store_postgres::{
    CorruptionQuarantineContext, JournalPageSize, StoredActiveGraphFrame,
};
use std::panic::AssertUnwindSafe;

impl DurableGraphLifecycle {
    pub(super) fn validate_frame_handoff<'a>(
        &'a self,
        fence: &'a RunFence,
        root: &'a CheckpointHead,
        leaf: &'a GraphFrameCheckpointHead,
        journal: &'a JournalHead,
        quarantine: CorruptionQuarantineContext,
    ) -> BoxFuture<'a, Result<StoredActiveGraphFrame, GraphLifecycleError>> {
        Box::pin(async move {
            let recovery = self
                .store
                .begin_claimed_run_recovery(fence.clone(), quarantine)
                .await?;
            if recovery.initial_run().journal_head() != Some(journal) {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            validate_cancellation_checkpoint(recovery.initial_run(), root)?;
            let checkpoint = self
                .store
                .with_corruption_quarantine(
                    recovery.quarantine_context().clone(),
                    self.store.load_checkpoint(
                        fence.tenant_id(),
                        fence.run_id(),
                        root.checkpoint_id(),
                    ),
                )
                .await?;
            let active = self
                .store
                .with_corruption_quarantine(
                    recovery.quarantine_context().clone(),
                    self.store
                        .load_active_graph_frame(fence.tenant_id(), fence.run_id()),
                )
                .await?
                .ok_or(GraphLifecycleError::StaleHandoff)?;
            if checkpoint.head() != *root
                || active.checkpoint().head() != *leaf
                || active.run().journal_head() != Some(journal)
                || active.run().checkpoint() != recovery.initial_run().checkpoint()
            {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            recovery.revalidate().await?;
            Ok(active)
        })
    }

    #[allow(clippy::too_many_lines)]
    pub(super) fn confirm_frame_cancellation(
        &self,
        handoff: GraphCancellationHandoff,
    ) -> BoxFuture<'_, Result<GraphBarrierLifecycleOutcome, GraphLifecycleError>> {
        Box::pin(async move {
            let fence = handoff.lease().fence();
            let root = handoff.checkpoint();
            let original = handoff.journal_head();
            let leaf = handoff.leaf().ok_or(GraphLifecycleError::StaleHandoff)?;
            validate_cancellation_scope(fence, original, root)?;
            let saved = self
                .store
                .load_graph_frame_closure(fence.tenant_id(), fence.run_id())
                .await?;
            let closure = if let Some(saved) = saved {
                saved
            } else {
                let active = self
                    .validate_frame_handoff(
                        fence,
                        root,
                        leaf,
                        original,
                        handoff.quarantine_context().clone(),
                    )
                    .await?;
                let run = active.run();
                if run.lifecycle().status() != RunStatus::CancellationRequested
                    || run.lifecycle().revision() != handoff.expected_revision()
                    || run
                        .lifecycle()
                        .cancellation_request()
                        .is_none_or(|request| request.failure().id() != handoff.failure_id())
                {
                    return Err(GraphLifecycleError::StaleHandoff);
                }
                let context = GraphDirectUsageEvidenceContext::new(
                    run.lifecycle().provenance().clone(),
                    root.clone(),
                    Some(leaf.clone()),
                    original.clone(),
                    fence.clone(),
                );
                let evidence = AssertUnwindSafe(async {
                    self.evidence.direct_usage_evidence(context.clone()).await
                })
                .catch_unwind()
                .await
                .map_err(|_| {
                    GraphLifecycleError::Evidence(GraphLifecycleEvidenceError::Unavailable)
                })?
                .map_err(GraphLifecycleError::Evidence)?;
                if evidence.context() != &context
                    || evidence.usage().unpriced_cost_events().get() != 0
                    || evidence
                        .usage()
                        .validate_monotonic_after(active.minimum_direct_usage())
                        .is_err()
                {
                    return Err(GraphLifecycleError::Evidence(
                        GraphLifecycleEvidenceError::Unavailable,
                    ));
                }
                // This read requires every actual child to have settled, while
                // retaining DIRECT-only evidence for the whole-frame writer.
                self.store
                    .include_child_usage(
                        fence.tenant_id(),
                        fence.run_id(),
                        evidence.usage().clone(),
                    )
                    .await?;
                self.store.observe_live_lease(fence).await?;
                let event = EventId::generate();
                let mut attempt = 1_u8;
                loop {
                    match self
                        .store
                        .close_graph_frames(
                            fence.tenant_id(),
                            fence.run_id(),
                            event,
                            original.clone(),
                            handoff.expected_revision(),
                            evidence.usage().clone(),
                        )
                        .await
                    {
                        Ok(outcome) => break outcome.record().clone(),
                        Err(error) if self.can_retry_mutation(&error, attempt) => {
                            self.mutation_backoff(attempt).await;
                            attempt = attempt.saturating_add(1);
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            };
            if closure.failure().id() != handoff.failure_id()
                || closure.event().sequence().get()
                    != original
                        .sequence()
                        .get()
                        .checked_add(1)
                        .ok_or(GraphLifecycleError::StaleHandoff)?
                || closure.event().previous_digest() != Some(original.digest())
            {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            let run = self
                .store
                .load_run(fence.tenant_id(), fence.run_id())
                .await?;
            validate_cancellation_checkpoint(&run, root)?;
            if run.lifecycle().status() == RunStatus::Cancelled {
                return self
                    .recover_frame_cancellation(&handoff, &closure, &run)
                    .await;
            }
            if run.lifecycle().status() != RunStatus::CancellationRequested
                || run.lifecycle().revision() != handoff.expected_revision()
                || run.lease().is_some()
                || run.journal_head() != Some(&closure.event().head())
            {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            let usage = self
                .store
                .include_child_usage(
                    fence.tenant_id(),
                    fence.run_id(),
                    closure.direct_usage().clone(),
                )
                .await?;
            let completed_at = self.store.observe_database_clock().await?;
            let current = self
                .store
                .load_run(fence.tenant_id(), fence.run_id())
                .await?;
            validate_cancellation_checkpoint(&current, root)?;
            if current.lifecycle().status() != RunStatus::CancellationRequested
                || current.lifecycle().revision() != handoff.expected_revision()
                || current.lease().is_some()
                || current.journal_head() != Some(&closure.event().head())
                || !same_cancellation_failure(&current, closure.failure())?
            {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            let append = self.frame_cancellation_append(&handoff, closure.event().head())?;
            let projection = RunProjection::transition(
                handoff.expected_revision(),
                RunTransition::ConfirmCancellation {
                    completed_at,
                    usage,
                },
            );
            let outcome = self
                .commit_frame_cancellation_with_retry(append, projection)
                .await?;
            Ok(GraphBarrierLifecycleOutcome::Cancelled(outcome))
        })
    }

    fn recover_frame_cancellation<'a>(
        &'a self,
        handoff: &'a GraphCancellationHandoff,
        closure: &'a stateknot_store_postgres::StoredGraphFrameClosure,
        run: &'a StoredRun,
    ) -> BoxFuture<'a, Result<GraphBarrierLifecycleOutcome, GraphLifecycleError>> {
        Box::pin(async move {
            if run.lifecycle().revision().get()
                != handoff
                    .expected_revision()
                    .get()
                    .checked_add(1)
                    .ok_or(GraphLifecycleError::StaleHandoff)?
                || run.lease().is_some()
                || run
                    .lifecycle()
                    .cancellation_request()
                    .is_none_or(|request| request.failure().id() != handoff.failure_id())
                || run
                    .journal_head()
                    .is_none_or(|head| head.event_id() != handoff.event_id())
            {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            let page = self
                .store
                .load_journal_page(
                    handoff.lease().fence().tenant_id(),
                    handoff.lease().fence().run_id(),
                    Some(&closure.event().head()),
                    JournalPageSize::new(1).map_err(|_| GraphLifecycleError::StaleHandoff)?,
                )
                .await?;
            let event = page
                .events()
                .first()
                .ok_or(GraphLifecycleError::StaleHandoff)?;
            if page.events().len() != 1
                || page.has_more()
                || Some(&event.head()) != run.journal_head()
                || event.event_id() != handoff.event_id()
            {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            let payload = self.cancelled_payload(handoff.checkpoint(), handoff.failure_id())?;
            if event.source() != &JournalEventSource::ControlPlane || event.payload() != &payload {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            let usage = self
                .store
                .include_child_usage(
                    handoff.lease().fence().tenant_id(),
                    handoff.lease().fence().run_id(),
                    closure.direct_usage().clone(),
                )
                .await?;
            if run.lifecycle().terminal_usage() != Some(&usage)
                || !same_cancellation_failure(run, closure.failure())?
            {
                return Err(GraphLifecycleError::StaleHandoff);
            }
            let append = self.frame_cancellation_append(handoff, closure.event().head())?;
            let projection = RunProjection::transition(
                handoff.expected_revision(),
                RunTransition::ConfirmCancellation {
                    completed_at: run.lifecycle().changed_at(),
                    usage,
                },
            );
            // Exact original event and projection recovery grants no launch.
            let outcome = self
                .commit_frame_cancellation_with_retry(append, projection)
                .await?;
            Ok(GraphBarrierLifecycleOutcome::Cancelled(outcome))
        })
    }

    fn frame_cancellation_append(
        &self,
        handoff: &GraphCancellationHandoff,
        head: JournalHead,
    ) -> Result<JournalAppend, GraphLifecycleError> {
        let fence = handoff.lease().fence();
        let payload = self.cancelled_payload(handoff.checkpoint(), handoff.failure_id())?;
        JournalAppend::new(
            JournalExpectation::exact(head),
            JournalEventIntent::control_plane(
                fence.tenant_id().clone(),
                fence.run_id(),
                handoff.event_id(),
                payload,
            )
            .map_err(|_| GraphLifecycleError::EventPayloadInvalid)?,
        )
        .map_err(|_| GraphLifecycleError::EventPayloadInvalid)
    }

    fn commit_frame_cancellation_with_retry(
        &self,
        append: JournalAppend,
        projection: RunProjection,
    ) -> BoxFuture<'_, Result<AppendOutcome, GraphLifecycleError>> {
        Box::pin(async move {
            let mut attempt = 1_u8;
            loop {
                match self
                    .store
                    .append_control_plane(append.clone(), projection.clone())
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
        })
    }
}

fn same_cancellation_failure(
    run: &StoredRun,
    failure: &Failure,
) -> Result<bool, GraphLifecycleError> {
    let Some(request) = run.lifecycle().cancellation_request() else {
        return Ok(false);
    };
    let encode = |failure: &Failure| {
        serde_json_canonicalizer::to_vec(failure).map_err(|_| GraphLifecycleError::InvalidHandoff {
            operation: "compare complete cancellation decision",
        })
    };
    Ok(encode(request.failure())? == encode(failure)?)
}
