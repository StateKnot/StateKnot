// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! The original complete start/execute/commit batch kept off the drive stack.

use super::{
    Arc, AtomicBool, AttemptId, BoxFuture, ClaimedRunRecovery, DurableGraphDriver, EventId,
    ExecutableGraph, GraphDriveOutcome, GraphDriveReport, GraphDriverError, GraphNodeScheduling,
    NodeAttemptCommitOutcome, NodeExecutionCommit, Ordering, ReadyNodeRecoveryPlan,
    RecoveryNodeKind, RunFence, StartedNode, StartedNodesExecution, frame::ReplayContinuation,
    worker_append,
};

pub(super) struct NodeBatchContext<'iteration, 'driver> {
    pub(super) executable: &'iteration ExecutableGraph,
    pub(super) plan: &'iteration ReadyNodeRecoveryPlan,
    pub(super) recovery: Box<ClaimedRunRecovery<'driver>>,
    pub(super) fence: &'iteration RunFence,
}

impl DurableGraphDriver {
    #[allow(clippy::too_many_lines)]
    pub(super) fn dispatch_node_batch<'a, 'driver: 'a>(
        &'a self,
        context: NodeBatchContext<'a, 'driver>,
        shutdown: super::CancellationSignal,
        node_phase: &'a AtomicBool,
        report: &'a mut GraphDriveReport,
        continuation: &'a mut ReplayContinuation,
    ) -> BoxFuture<'a, Result<Option<Box<GraphDriveOutcome>>, GraphDriverError>> {
        Box::pin(async move {
            let NodeBatchContext {
                executable,
                plan,
                recovery,
                fence,
            } = context;
            let first_node = plan
                .nodes()
                .iter()
                .find(|node| node.kind() == RecoveryNodeKind::Dispatchable)
                .ok_or(GraphDriverError::RuntimeInvariant {
                    operation: "select dispatchable graph node",
                })?;
            let first_node_id = first_node.activation().node_id().clone();
            let first_executor = Self::resolve_node_executor(executable, &first_node_id)?;
            let batch_limit = usize::from(self.options.maximum_parallel_nodes)
                .min(usize::from(
                    executable.graph().limits().maximum_parallelism(),
                ))
                .min(
                    usize::try_from(
                        (self
                            .options
                            .maximum_durable_events
                            .saturating_sub(report.durable_events))
                            / 2,
                    )
                    .unwrap_or(usize::MAX),
                );
            let first_scheduling = first_executor.scheduling();
            let mut selected = Vec::with_capacity(batch_limit);
            selected.push((first_node_id, first_executor));
            if first_scheduling == GraphNodeScheduling::JournalIsolated && batch_limit > 1 {
                for node in plan
                    .nodes()
                    .iter()
                    .filter(|node| node.kind() == RecoveryNodeKind::Dispatchable)
                    .skip(1)
                    .take(batch_limit.saturating_sub(1))
                {
                    let node_id = node.activation().node_id().clone();
                    let executor = Self::resolve_node_executor(executable, &node_id)?;
                    if executor.scheduling() != GraphNodeScheduling::JournalIsolated {
                        break;
                    }
                    selected.push((node_id, executor));
                }
            }

            // Start facts are serialized in canonical NodeId order before any
            // journal-isolated executor is spawned. This makes both launch
            // authority and the later result-commit order independent of task
            // completion timing.
            let mut journal_head = plan.journal_head().clone();
            let mut started = Vec::with_capacity(selected.len());
            for (node_id, executor) in selected {
                let attempt_id = AttemptId::generate();
                let start_event_id = EventId::generate();
                let payload = self.node_started_payload(
                    &executable.graph().reference(),
                    plan.checkpoint(),
                    &node_id,
                    attempt_id,
                )?;
                let append = worker_append(fence, journal_head.clone(), start_event_id, payload)?;
                let start = self
                    .start_node_with_retry(append, plan, &node_id, attempt_id, report)
                    .await?;
                match start {
                    NodeAttemptCommitOutcome::Committed { event, attempt } => {
                        report.durable_events = report.durable_events.saturating_add(1);
                        report.node_attempts_started =
                            report.node_attempts_started.saturating_add(1);
                        journal_head = event.head();
                        continuation.journal = journal_head.clone();
                        started.push(StartedNode {
                            executor,
                            start: attempt.start().head(),
                        });
                    }
                    NodeAttemptCommitOutcome::Idempotent { event, .. } => {
                        continuation.journal = event.head();
                        // Lost acknowledgement convergence never grants launch
                        // authority. Any starts committed earlier in this batch
                        // still execute and settle normally.
                        report.durable_events = report.durable_events.saturating_add(1);
                        break;
                    }
                    _ => {
                        return Err(GraphDriverError::RuntimeInvariant {
                            operation: "handle unsupported node attempt start outcome",
                        });
                    }
                }
            }
            if started.is_empty() {
                return Ok(None);
            }
            let checkpoint = Arc::new(plan.checkpoint().clone());
            drop(recovery);

            // A recovery/dispatch pass may have consumed most of the lease that
            // was originally acquired. Re-observe the database clock only after
            // the durable start commits, then restore a full configured lease
            // before node code can perform external work. If this preflight
            // fails, the committed start remains available for fenced takeover
            // and the executor is never launched under an unsafe lease margin.
            node_phase.store(true, Ordering::Release);
            let current_lease = self.prepare_execution_lease(fence, report).await?;

            report.node_batches_started = report.node_batches_started.saturating_add(1);
            report.maximum_nodes_in_batch = report
                .maximum_nodes_in_batch
                .max(u16::try_from(started.len()).unwrap_or(u16::MAX));
            let execution = self
                .execute_started_nodes(
                    &started,
                    checkpoint,
                    current_lease,
                    shutdown.clone(),
                    report,
                )
                .await?;
            node_phase.store(false, Ordering::Release);
            match execution {
                StartedNodesExecution::Cancelled => {
                    let release = self.release_with_retry(fence, report).await?;
                    return Ok(Some(Box::new(GraphDriveOutcome::Cancelled { release })));
                }
                StartedNodesExecution::RunCancellationObserved => {}
                StartedNodesExecution::Finished(results) => {
                    for (started, result) in started.iter().zip(results) {
                        match self
                            .commit_node_execution(fence, executable, started, result, report)
                            .await?
                        {
                            NodeExecutionCommit::RunCancellationObserved => break,
                            NodeExecutionCommit::Committed(head) => {
                                continuation.journal = *head;
                            }
                            NodeExecutionCommit::ChildJoin(request) => {
                                return Ok(Some(Box::new(GraphDriveOutcome::ChildJoin(request))));
                            }
                        }
                    }
                }
            }
            Ok(None)
        })
    }
}
