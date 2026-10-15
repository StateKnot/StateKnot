// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! The original complete Root/leaf barrier phase with heap-owned results.

use super::{
    BoxFuture, CheckpointId, ClaimedRunRecovery, DurableGraphDriver, EventId,
    GraphBarrierDisposition, GraphDriveOutcome, GraphDriveReport, GraphDriverError,
    GraphLifecycleBarrierHandoff, ReadyNodeRecoveryPlan, RunFence, StoredRun, exact_live_lease,
    frame::{IterationScope, ReplayContinuation},
};

pub(super) struct BarrierContext<'iteration, 'driver> {
    pub(super) scope: &'iteration IterationScope,
    pub(super) plan: &'iteration ReadyNodeRecoveryPlan,
    pub(super) live: &'iteration StoredRun,
    pub(super) recovery: Box<ClaimedRunRecovery<'driver>>,
    pub(super) fence: &'iteration RunFence,
}

impl DurableGraphDriver {
    #[allow(clippy::too_many_lines)]
    pub(super) fn commit_ready_barrier<'a, 'driver: 'a>(
        &'a self,
        context: BarrierContext<'a, 'driver>,
        report: &'a mut GraphDriveReport,
        continuation: &'a mut ReplayContinuation,
    ) -> BoxFuture<'a, Result<Option<Box<GraphDriveOutcome>>, GraphDriverError>> {
        Box::pin(async move {
            let BarrierContext {
                scope,
                plan,
                live,
                recovery,
                fence,
            } = context;
            let executable = &scope.executable;
            let results = self.load_barrier_results(&recovery, plan, scope).await?;
            if let Some(active) = scope.active.as_ref() {
                let barrier_plan = Box::new(
                    executable
                        .graph()
                        .plan_frame_barrier(
                            active.checkpoint(),
                            &results,
                            CheckpointId::generate(),
                            executable.schemas(),
                            executable.reducer(),
                        )
                        .map_err(GraphDriverError::graph_plan)?,
                );
                let waiting = matches!(
                    barrier_plan.disposition(),
                    GraphBarrierDisposition::Wait { .. }
                );
                let outcome = self
                    .commit_scoped_barrier(
                        &recovery,
                        scope,
                        barrier_plan,
                        live.lifecycle().revision(),
                        report,
                        continuation,
                    )
                    .await?;
                let Some(outcome) = outcome else {
                    return Ok(None);
                };
                if waiting {
                    return Ok(Some(Box::new(GraphDriveOutcome::FrameWaiting(outcome))));
                }
                return Ok(None);
            }
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
            let lease = exact_live_lease(live, fence)?.clone();
            match barrier_plan.disposition() {
                GraphBarrierDisposition::Continue => {
                    drop(recovery);
                    let record = self
                        .commit_continue_barrier(
                            fence,
                            plan,
                            barrier_plan,
                            live.lifecycle(),
                            report,
                        )
                        .await?;
                    continuation.root = record.checkpoint().head();
                    continuation.leaf = None;
                    continuation.journal = record.event().head();
                }
                GraphBarrierDisposition::Wait { .. } | GraphBarrierDisposition::Terminal { .. } => {
                    return Ok(Some(Box::new(GraphDriveOutcome::LifecycleBarrierReady(
                        Box::new(GraphLifecycleBarrierHandoff {
                            plan: barrier_plan,
                            journal_head: plan.journal_head().clone(),
                            event_id: EventId::generate(),
                            expected_revision: live.lifecycle().revision(),
                            original_lifecycle: Box::new(live.lifecycle().clone()),
                            lease,
                        }),
                    ))));
                }
                _ => {
                    return Err(GraphDriverError::RuntimeInvariant {
                        operation: "handle unsupported graph barrier disposition",
                    });
                }
            }
            Ok(None)
        })
    }
}
