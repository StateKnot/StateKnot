// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic ready-node replay and crash-recovery planning.
//!
//! A plan is derived only from one fully verified checkpoint, its canonical
//! root-node activations, immutable pending results, complete physical-attempt
//! histories, one exact live worker fence, and a database-observed timestamp.
//! It performs no I/O and authorizes no dispatch. Storage/runtime code must
//! revalidate ownership and commit a durable node-attempt start before invoking
//! node code; an idempotently observed pre-existing start is in-flight evidence,
//! not fresh launch authority.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::{
    BarrierResultHeads, BarrierResultHeadsError, Checkpoint, Failure, JournalHead, NodeActivation,
    NodeActivationError, NodeAttempt, NodeAttemptHistoryError, NodeAttemptHistoryVerifier,
    NodeAttemptOutcome, NodeAttemptStartHead, NodeAttemptStatus, NodeId, PendingNodeResult,
    PendingNodeResultHead, RetryAdvice, RunFence, Timestamp,
};

/// Stable reason that one logical activation may start a new physical attempt.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum NodeDispatchReason {
    /// No physical attempt has ever started for the activation.
    FirstAttempt,
    /// An unfinished attempt belongs to a superseded lower worker fence.
    SupersededAttempt,
    /// A failed attempt explicitly authorized a retry whose delay elapsed.
    SafeRetry,
}

/// Closed scheduler classification for one checkpoint-ready node.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum RecoveryNodeKind {
    /// An immutable logical result already exists and must be reused.
    Completed,
    /// A durable physical start may be prepared after ownership revalidation.
    Dispatchable,
    /// A safe retry exists but its database-time delay has not elapsed.
    Deferred,
    /// An unfinished attempt already belongs to the current worker fence.
    InFlight,
    /// A terminal node failure does not authorize automatic retry.
    Failed,
    /// The hard physical-attempt safety ceiling forbids another start.
    Exhausted,
}

/// Deterministic recovery decision for one logical ready-node activation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RecoveryNode {
    /// Reuse a fully verified immutable result without executing the node.
    Completed {
        /// Canonical activation derived from the checkpoint.
        activation: NodeActivation,
        /// Exact durable result to consume at the barrier.
        result: PendingNodeResultHead,
    },
    /// The runtime may durably start a fresh physical attempt.
    Dispatchable {
        /// Canonical activation derived from the checkpoint.
        activation: NodeActivation,
        /// Why another physical start is semantically permitted.
        reason: NodeDispatchReason,
    },
    /// The activation must remain durably deferred until database time reaches
    /// `not_before`.
    Deferred {
        /// Canonical activation derived from the checkpoint.
        activation: NodeActivation,
        /// Inclusive earliest database instant for a safe successor start.
        not_before: Timestamp,
    },
    /// An unfinished start already belongs to the exact current worker fence.
    /// Re-execution under the same fence would duplicate in-process work.
    InFlight {
        /// Canonical activation derived from the checkpoint.
        activation: NodeActivation,
        /// Exact durable start that remains in flight.
        attempt: NodeAttemptStartHead,
    },
    /// The latest physical attempt failed without automatic retry authority.
    Failed {
        /// Canonical activation derived from the checkpoint.
        activation: NodeActivation,
        /// Public-safe durable failure evidence.
        failure: Failure,
    },
    /// No further physical start is allowed for this logical activation.
    Exhausted {
        /// Canonical activation derived from the checkpoint.
        activation: NodeActivation,
        /// Most recent durable physical start.
        attempt: NodeAttemptStartHead,
        /// Latest failure evidence when exhaustion followed a safe failure.
        failure: Option<Failure>,
    },
}

impl RecoveryNode {
    /// Returns the closed scheduler classification.
    #[must_use]
    pub const fn kind(&self) -> RecoveryNodeKind {
        match self {
            Self::Completed { .. } => RecoveryNodeKind::Completed,
            Self::Dispatchable { .. } => RecoveryNodeKind::Dispatchable,
            Self::Deferred { .. } => RecoveryNodeKind::Deferred,
            Self::InFlight { .. } => RecoveryNodeKind::InFlight,
            Self::Failed { .. } => RecoveryNodeKind::Failed,
            Self::Exhausted { .. } => RecoveryNodeKind::Exhausted,
        }
    }

    /// Returns the canonical logical activation.
    #[must_use]
    pub const fn activation(&self) -> &NodeActivation {
        match self {
            Self::Completed { activation, .. }
            | Self::Dispatchable { activation, .. }
            | Self::Deferred { activation, .. }
            | Self::InFlight { activation, .. }
            | Self::Failed { activation, .. }
            | Self::Exhausted { activation, .. } => activation,
        }
    }

    /// Returns the dispatch reason when a new start is permitted.
    #[must_use]
    pub const fn dispatch_reason(&self) -> Option<NodeDispatchReason> {
        match self {
            Self::Dispatchable { reason, .. } => Some(*reason),
            _ => None,
        }
    }

    /// Returns the immutable completed result, if any.
    #[must_use]
    pub const fn result(&self) -> Option<&PendingNodeResultHead> {
        match self {
            Self::Completed { result, .. } => Some(result),
            _ => None,
        }
    }

    /// Returns the inclusive retry instant for a deferred node, if any.
    #[must_use]
    pub const fn not_before(&self) -> Option<Timestamp> {
        match self {
            Self::Deferred { not_before, .. } => Some(*not_before),
            _ => None,
        }
    }

    /// Returns the same-fence unfinished start, if any.
    #[must_use]
    pub const fn in_flight_attempt(&self) -> Option<&NodeAttemptStartHead> {
        match self {
            Self::InFlight { attempt, .. } => Some(attempt),
            _ => None,
        }
    }

    /// Returns terminal public-safe node failure evidence, if any.
    #[must_use]
    pub const fn failure(&self) -> Option<&Failure> {
        match self {
            Self::Failed { failure, .. } => Some(failure),
            Self::Exhausted { failure, .. } => failure.as_ref(),
            _ => None,
        }
    }

    /// Returns the final physical start when the hard attempt limit was
    /// reached, if applicable.
    #[must_use]
    pub const fn exhausted_attempt(&self) -> Option<&NodeAttemptStartHead> {
        match self {
            Self::Exhausted { attempt, .. } => Some(attempt),
            _ => None,
        }
    }
}

/// Immutable deterministic plan for the exact ready set of one checkpoint.
#[derive(Clone, Debug)]
pub struct ReadyNodeRecoveryPlan {
    checkpoint: Checkpoint,
    fence: RunFence,
    journal_head: JournalHead,
    observed_at: Timestamp,
    nodes: Box<[RecoveryNode]>,
}

impl ReadyNodeRecoveryPlan {
    /// Returns the fully verified immutable state snapshot shared by every node.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Returns the exact worker ownership used to classify attempt histories.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Returns the exact verified run-journal observation behind this plan.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the database timestamp used for retry classification.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns every ready node in deterministic ascending `NodeId` order.
    #[must_use]
    pub const fn nodes(&self) -> &[RecoveryNode] {
        &self.nodes
    }

    /// Returns whether every ready activation already has an immutable result.
    #[must_use]
    pub fn is_barrier_ready(&self) -> bool {
        !self.nodes.is_empty()
            && self
                .nodes
                .iter()
                .all(|node| node.kind() == RecoveryNodeKind::Completed)
    }

    /// Builds the exact compact result set when the checkpoint can enter its
    /// barrier, or returns `None` while any activation remains unsettled.
    ///
    /// # Errors
    ///
    /// Returns [`BarrierResultHeadsError`] only if an internal invariant is
    /// violated while materializing the already validated canonical set.
    pub fn barrier_result_heads(
        &self,
    ) -> Result<Option<BarrierResultHeads>, BarrierResultHeadsError> {
        if !self.is_barrier_ready() {
            return Ok(None);
        }
        let results = self.nodes.iter().filter_map(RecoveryNode::result).cloned();
        BarrierResultHeads::try_new(results).map(Some)
    }

    /// Returns the earliest durable retry instant when at least one node is
    /// deferred.
    #[must_use]
    pub fn earliest_deferred_at(&self) -> Option<Timestamp> {
        self.nodes.iter().filter_map(RecoveryNode::not_before).min()
    }
}

#[derive(Clone, Debug)]
struct RecoveryEntry {
    activation: NodeActivation,
    result: Option<PendingNodeResultHead>,
    attempts: NodeAttemptHistoryVerifier,
    attempt_count: usize,
}

/// Incremental, bounded verifier for one ready-node recovery snapshot.
///
/// Callers may stream large durable records through this builder. It retains
/// one compact result head, the last full attempt, and at most 64 physical
/// attempt identities per ready node. Combined with the checkpoint's 1024-node
/// ceiling, both decoded-record and identity-set memory have a hard bound.
#[derive(Clone, Debug)]
pub struct ReadyNodeRecoveryPlanner {
    checkpoint: Checkpoint,
    fence: RunFence,
    entries: BTreeMap<NodeId, RecoveryEntry>,
}

impl ReadyNodeRecoveryPlanner {
    /// Hard physical-attempt ceiling for one logical activation.
    ///
    /// Runtime policy should normally stop much earlier. This defense-in-depth
    /// bound prevents corrupt or adversarial durable history from turning
    /// recovery verification into unbounded memory growth.
    pub const MAX_ATTEMPTS_PER_NODE: usize = 64;

    /// Seeds an exact canonical activation for every checkpoint-ready root node.
    ///
    /// # Errors
    ///
    /// Returns [`ReadyNodeRecoveryError`] when the fence crosses checkpoint
    /// scope or deterministic activation construction fails.
    pub fn new(checkpoint: Checkpoint, fence: RunFence) -> Result<Self, ReadyNodeRecoveryError> {
        if fence.tenant_id() != checkpoint.tenant_id() {
            return Err(ReadyNodeRecoveryError::FenceTenantMismatch);
        }
        if fence.run_id() != checkpoint.run_id() {
            return Err(ReadyNodeRecoveryError::FenceRunMismatch);
        }
        let mut entries = BTreeMap::new();
        for node_id in checkpoint.ready_nodes().iter().cloned() {
            let activation = NodeActivation::for_ready_root(&checkpoint, node_id.clone())
                .map_err(ReadyNodeRecoveryError::activation)?;
            entries.insert(
                node_id,
                RecoveryEntry {
                    activation,
                    result: None,
                    attempts: NodeAttemptHistoryVerifier::new(),
                    attempt_count: 0,
                },
            );
        }
        Ok(Self {
            checkpoint,
            fence,
            entries,
        })
    }

    /// Observes one fully verified immutable pending result.
    ///
    /// # Errors
    ///
    /// Returns [`ReadyNodeRecoveryError::UnexpectedResult`] for an activation
    /// outside the exact derived ready set or
    /// [`ReadyNodeRecoveryError::DuplicateResult`] when a stream repeats one
    /// logical activation.
    pub fn observe_result(
        &mut self,
        result: &PendingNodeResult,
    ) -> Result<(), ReadyNodeRecoveryError> {
        let activation = result.intent().activation();
        let Some(entry) = self.entries.get_mut(activation.node_id()) else {
            return Err(ReadyNodeRecoveryError::UnexpectedResult {
                node_id: activation.node_id().clone(),
            });
        };
        if activation != &entry.activation {
            return Err(ReadyNodeRecoveryError::UnexpectedResult {
                node_id: activation.node_id().clone(),
            });
        }
        if entry.result.is_some() {
            return Err(ReadyNodeRecoveryError::DuplicateResult {
                node_id: activation.node_id().clone(),
            });
        }
        entry.result = Some(result.head());
        Ok(())
    }

    /// Observes the next ascending physical attempt for one activation.
    ///
    /// # Errors
    ///
    /// Returns [`ReadyNodeRecoveryError::UnexpectedAttempt`] when the attempt
    /// does not match the exact canonical activation, or wraps a complete
    /// physical-history invariant failure.
    pub fn observe_attempt(&mut self, attempt: &NodeAttempt) -> Result<(), ReadyNodeRecoveryError> {
        let activation = attempt.start().activation();
        let Some(entry) = self.entries.get_mut(activation.node_id()) else {
            return Err(ReadyNodeRecoveryError::UnexpectedAttempt {
                node_id: activation.node_id().clone(),
            });
        };
        if activation != &entry.activation {
            return Err(ReadyNodeRecoveryError::UnexpectedAttempt {
                node_id: activation.node_id().clone(),
            });
        }
        if entry.attempt_count >= Self::MAX_ATTEMPTS_PER_NODE {
            return Err(ReadyNodeRecoveryError::AttemptLimitExceeded {
                node_id: activation.node_id().clone(),
                maximum: Self::MAX_ATTEMPTS_PER_NODE,
            });
        }
        entry.attempts.verify_next(attempt).map_err(|source| {
            ReadyNodeRecoveryError::AttemptHistory {
                node_id: activation.node_id().clone(),
                source,
            }
        })?;
        entry.attempt_count += 1;
        Ok(())
    }

    /// Returns every canonical ready-node activation in deterministic order.
    ///
    /// Providers use this bounded list to verify complete physical histories
    /// even for completed siblings. That additional pass detects impossible
    /// attempts before or after an otherwise valid immutable result.
    #[must_use]
    pub fn activations(&self) -> Vec<NodeActivation> {
        self.entries
            .values()
            .map(|entry| entry.activation.clone())
            .collect()
    }

    /// Finalizes every node decision at one database-observed instant.
    ///
    /// # Errors
    ///
    /// Returns [`ReadyNodeRecoveryError`] for a current fence that contradicts
    /// durable attempt history, a success missing its result, a mismatched
    /// success/result pair, unsupported retry evidence, or timestamp overflow.
    pub fn finish(
        self,
        journal_head: JournalHead,
        observed_at: Timestamp,
    ) -> Result<ReadyNodeRecoveryPlan, ReadyNodeRecoveryError> {
        validate_observation(&self, &journal_head, observed_at)?;
        let mut nodes = Vec::with_capacity(self.entries.len());
        for (node_id, entry) in self.entries {
            nodes.push(classify_entry(node_id, entry, &self.fence, observed_at)?);
        }
        Ok(ReadyNodeRecoveryPlan {
            checkpoint: self.checkpoint,
            fence: self.fence,
            journal_head,
            observed_at,
            nodes: nodes.into_boxed_slice(),
        })
    }
}

/// Invalid or contradictory inputs to deterministic ready-node recovery.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ReadyNodeRecoveryError {
    /// The live worker fence crossed the checkpoint tenant boundary.
    #[error("ready-node recovery fence crosses the checkpoint tenant boundary")]
    FenceTenantMismatch,
    /// The live worker fence named another run.
    #[error("ready-node recovery fence does not belong to the checkpoint run")]
    FenceRunMismatch,
    /// Canonical activation derivation failed.
    #[error("ready-node activation derivation failed: {source}")]
    Activation {
        /// Exact activation construction failure.
        #[source]
        source: NodeActivationError,
    },
    /// A pending result was outside the exact canonical ready-activation set.
    #[error("pending result for node {node_id:?} is not part of the recovery ready set")]
    UnexpectedResult {
        /// Rejected node identity.
        node_id: NodeId,
    },
    /// A streamed recovery result repeated one logical activation.
    #[error("pending result for node {node_id:?} was observed more than once")]
    DuplicateResult {
        /// Repeated node identity.
        node_id: NodeId,
    },
    /// A physical attempt was outside the exact canonical activation set.
    #[error("physical attempt for node {node_id:?} is not part of the recovery ready set")]
    UnexpectedAttempt {
        /// Rejected node identity.
        node_id: NodeId,
    },
    /// One activation's physical-attempt history was contradictory.
    #[error("physical attempt history for node {node_id:?} is invalid: {source}")]
    AttemptHistory {
        /// Affected node identity.
        node_id: NodeId,
        /// Exact history invariant failure.
        #[source]
        source: NodeAttemptHistoryError,
    },
    /// One logical activation exceeded the hard recovery history ceiling.
    #[error("physical attempt history for node {node_id:?} exceeds {maximum} records")]
    AttemptLimitExceeded {
        /// Affected node identity.
        node_id: NodeId,
        /// Closed implementation ceiling.
        maximum: usize,
    },
    /// Durable history contains a worker epoch newer than the claimed fence.
    #[error("physical attempt history for node {node_id:?} is newer than the recovery fence")]
    CurrentFenceBehindHistory {
        /// Affected node identity.
        node_id: NodeId,
    },
    /// The same fencing epoch names a different worker attempt.
    #[error("physical attempt history for node {node_id:?} conflicts with the recovery fence")]
    CurrentFenceConflict {
        /// Affected node identity.
        node_id: NodeId,
    },
    /// A higher worker epoch improperly reused an earlier worker attempt ID.
    #[error("recovery fence reused an earlier worker attempt for node {node_id:?}")]
    CurrentFenceAttemptReused {
        /// Affected node identity.
        node_id: NodeId,
    },
    /// A successful attempt exists without its required immutable result.
    #[error("successful physical attempt for node {node_id:?} has no pending result")]
    SucceededWithoutResult {
        /// Affected node identity.
        node_id: NodeId,
    },
    /// The successful attempt and observed immutable result disagree.
    #[error("successful physical attempt for node {node_id:?} references another result")]
    ResultAttemptMismatch {
        /// Affected node identity.
        node_id: NodeId,
    },
    /// Node-attempt failure contained a retry class forbidden by its contract.
    #[error("physical node failure for node {node_id:?} has unsupported retry advice")]
    UnsupportedRetryAdvice {
        /// Affected node identity.
        node_id: NodeId,
    },
    /// Safe-after arithmetic exceeded the canonical timestamp range.
    #[error("physical node retry time for node {node_id:?} exceeds the timestamp range")]
    RetryNotBeforeOutOfRange {
        /// Affected node identity.
        node_id: NodeId,
    },
    /// The final journal observation crossed the checkpoint tenant boundary.
    #[error("ready-node recovery journal observation crosses the checkpoint tenant boundary")]
    ObservationTenantMismatch,
    /// The final journal observation named another run.
    #[error("ready-node recovery journal observation does not belong to the checkpoint run")]
    ObservationRunMismatch,
    /// The final journal observation preceded the checkpoint anchor.
    #[error("ready-node recovery journal observation precedes the checkpoint")]
    ObservationBeforeCheckpoint,
    /// The final journal observation preceded a streamed result or attempt.
    #[error("ready-node recovery journal observation precedes durable node evidence")]
    ObservationBeforeEvidence,
    /// The database clock observation preceded the verified journal head.
    #[error("ready-node recovery database clock precedes its journal observation")]
    ObservationClockRegression,
}

impl ReadyNodeRecoveryError {
    const fn activation(source: NodeActivationError) -> Self {
        Self::Activation { source }
    }
}

fn classify_entry(
    node_id: NodeId,
    entry: RecoveryEntry,
    current_fence: &RunFence,
    observed_at: Timestamp,
) -> Result<RecoveryNode, ReadyNodeRecoveryError> {
    let RecoveryEntry {
        activation,
        result,
        attempts,
        attempt_count,
    } = entry;
    let latest = attempts.last();

    if let Some(result) = result {
        validate_current_fence(&node_id, result.fence(), current_fence)?;
        if let Some(attempt) = latest {
            let Some(completion) = attempt.completion() else {
                return Err(ReadyNodeRecoveryError::ResultAttemptMismatch { node_id });
            };
            if completion.outcome().result() != Some(&result) {
                return Err(ReadyNodeRecoveryError::ResultAttemptMismatch { node_id });
            }
        }
        return Ok(RecoveryNode::Completed { activation, result });
    }

    let Some(attempt) = latest else {
        return Ok(RecoveryNode::Dispatchable {
            activation,
            reason: NodeDispatchReason::FirstAttempt,
        });
    };
    validate_current_fence(&node_id, attempt.start().fence(), current_fence)?;

    match attempt.status() {
        NodeAttemptStatus::Executing => {
            if attempt.start().fence() == current_fence {
                Ok(RecoveryNode::InFlight {
                    activation,
                    attempt: attempt.start().head(),
                })
            } else if attempt_count >= ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
                Ok(RecoveryNode::Exhausted {
                    activation,
                    attempt: attempt.start().head(),
                    failure: None,
                })
            } else {
                Ok(RecoveryNode::Dispatchable {
                    activation,
                    reason: NodeDispatchReason::SupersededAttempt,
                })
            }
        }
        NodeAttemptStatus::Succeeded => {
            Err(ReadyNodeRecoveryError::SucceededWithoutResult { node_id })
        }
        NodeAttemptStatus::Failed => {
            let completion = attempt.completion().ok_or_else(|| {
                ReadyNodeRecoveryError::SucceededWithoutResult {
                    node_id: node_id.clone(),
                }
            })?;
            let NodeAttemptOutcome::Failed { failure } = completion.outcome() else {
                return Err(ReadyNodeRecoveryError::SucceededWithoutResult { node_id });
            };
            match failure.retry_advice() {
                RetryAdvice::Never => Ok(RecoveryNode::Failed {
                    activation,
                    failure: failure.clone(),
                }),
                RetryAdvice::SafeAfter { delay } => {
                    if attempt_count >= ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
                        return Ok(RecoveryNode::Exhausted {
                            activation,
                            attempt: attempt.start().head(),
                            failure: Some(failure.clone()),
                        });
                    }
                    let not_before =
                        retry_not_before(completion.journal_head().recorded_at(), delay.as_i64())
                            .ok_or_else(|| ReadyNodeRecoveryError::RetryNotBeforeOutOfRange {
                            node_id: node_id.clone(),
                        })?;
                    if observed_at >= not_before {
                        Ok(RecoveryNode::Dispatchable {
                            activation,
                            reason: NodeDispatchReason::SafeRetry,
                        })
                    } else {
                        Ok(RecoveryNode::Deferred {
                            activation,
                            not_before,
                        })
                    }
                }
                RetryAdvice::ReconcileFirst => {
                    Err(ReadyNodeRecoveryError::UnsupportedRetryAdvice { node_id })
                }
            }
        }
    }
}

fn validate_observation(
    planner: &ReadyNodeRecoveryPlanner,
    journal_head: &JournalHead,
    observed_at: Timestamp,
) -> Result<(), ReadyNodeRecoveryError> {
    if journal_head.tenant_id() != planner.checkpoint.tenant_id() {
        return Err(ReadyNodeRecoveryError::ObservationTenantMismatch);
    }
    if journal_head.run_id() != planner.checkpoint.run_id() {
        return Err(ReadyNodeRecoveryError::ObservationRunMismatch);
    }
    let checkpoint_head = planner.checkpoint.journal_head();
    if journal_head.sequence() < checkpoint_head.sequence()
        || journal_head.recorded_at() < checkpoint_head.recorded_at()
    {
        return Err(ReadyNodeRecoveryError::ObservationBeforeCheckpoint);
    }
    if observed_at < journal_head.recorded_at() {
        return Err(ReadyNodeRecoveryError::ObservationClockRegression);
    }
    for entry in planner.entries.values() {
        let result_head = entry
            .result
            .as_ref()
            .map(PendingNodeResultHead::journal_head);
        let attempt_head = entry.attempts.last().map(|attempt| {
            attempt.completion().map_or_else(
                || attempt.start().journal_head(),
                |value| value.journal_head(),
            )
        });
        if result_head.into_iter().chain(attempt_head).any(|evidence| {
            evidence.sequence() > journal_head.sequence()
                || evidence.recorded_at() > journal_head.recorded_at()
        }) {
            return Err(ReadyNodeRecoveryError::ObservationBeforeEvidence);
        }
    }
    Ok(())
}

fn validate_current_fence(
    node_id: &NodeId,
    historical: &RunFence,
    current: &RunFence,
) -> Result<(), ReadyNodeRecoveryError> {
    if historical.epoch() > current.epoch() {
        return Err(ReadyNodeRecoveryError::CurrentFenceBehindHistory {
            node_id: node_id.clone(),
        });
    }
    if historical.epoch() == current.epoch() && historical.attempt_id() != current.attempt_id() {
        return Err(ReadyNodeRecoveryError::CurrentFenceConflict {
            node_id: node_id.clone(),
        });
    }
    if historical.epoch() < current.epoch() && historical.attempt_id() == current.attempt_id() {
        return Err(ReadyNodeRecoveryError::CurrentFenceAttemptReused {
            node_id: node_id.clone(),
        });
    }
    Ok(())
}

fn retry_not_before(failed_at: Timestamp, delay_millis: i64) -> Option<Timestamp> {
    let micros = i128::from(failed_at.unix_micros())
        .checked_add(i128::from(delay_millis).checked_mul(1_000)?)?;
    let micros = i64::try_from(micros).ok()?;
    Timestamp::from_unix_micros(micros).ok()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::from_value;

    use super::*;
    use crate::{
        AttemptId, BudgetUsage, CheckpointId, CheckpointWrite, Digest, DurationMillis, EventId,
        FailureCategory, FailureCode, FailureId, FailureMessage, FailureOrigin, FencingEpoch,
        GraphNamespace, JournalHead, JournalSequence, NodeAttemptCompletion, NodeControl,
        NodeInvocationBindings, NodeStateChange, PendingNodeResultIntent, ReadyNodes, RunId,
        TenantId,
    };

    fn checkpoint() -> Checkpoint {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-checkpoint-v1.json"))
                .unwrap();
        from_value(fixture["checkpoints"][0].clone()).unwrap()
    }

    fn checkpoint_with_nodes(count: usize) -> Checkpoint {
        let template = checkpoint();
        let tenant_id = template.tenant_id().clone();
        let run_id = RunId::generate();
        let ready_nodes = ReadyNodes::try_new(
            (0..count).map(|index| NodeId::new(format!("node-{index:04}")).unwrap()),
        )
        .unwrap();
        let write = CheckpointWrite::initial(
            tenant_id.clone(),
            run_id,
            CheckpointId::generate(),
            template.graph().clone(),
            template.state().clone(),
            ready_nodes,
        )
        .unwrap();
        let journal_head = JournalHead::new(
            tenant_id,
            run_id,
            JournalSequence::FIRST,
            EventId::generate(),
            template.journal_head().recorded_at(),
            Digest::sha256(b"property checkpoint journal"),
        );
        Checkpoint::commit(write, journal_head).unwrap()
    }

    fn node(name: &str) -> NodeId {
        NodeId::new(name).unwrap()
    }

    fn activation(checkpoint: &Checkpoint, name: &str) -> NodeActivation {
        NodeActivation::for_ready_root(checkpoint, node(name)).unwrap()
    }

    fn fence(checkpoint: &Checkpoint, epoch: u64) -> RunFence {
        RunFence::new(
            checkpoint.tenant_id().clone(),
            checkpoint.run_id(),
            AttemptId::generate(),
            FencingEpoch::new(epoch).unwrap(),
        )
    }

    fn journal(checkpoint: &Checkpoint, offset: u64) -> JournalHead {
        let base = checkpoint.journal_head();
        let sequence = JournalSequence::new(base.sequence().get() + offset).unwrap();
        let recorded_at = Timestamp::from_unix_micros(
            base.recorded_at().unix_micros() + i64::try_from(offset).unwrap() * 1_000_000,
        )
        .unwrap();
        JournalHead::new(
            checkpoint.tenant_id().clone(),
            checkpoint.run_id(),
            sequence,
            EventId::generate(),
            recorded_at,
            Digest::sha256(format!("recovery journal {offset}")),
        )
    }

    fn executing_attempt(
        checkpoint: &Checkpoint,
        name: &str,
        owner: RunFence,
        offset: u64,
    ) -> NodeAttempt {
        let start = crate::NodeAttemptStart::new(
            activation(checkpoint, name),
            AttemptId::generate(),
            owner,
            journal(checkpoint, offset),
        )
        .unwrap();
        NodeAttempt::executing(start)
    }

    fn result(
        checkpoint: &Checkpoint,
        name: &str,
        owner: RunFence,
        offset: u64,
    ) -> PendingNodeResult {
        let intent = PendingNodeResultIntent::new(
            activation(checkpoint, name),
            NodeStateChange::Unchanged,
            NodeControl::Continue,
            NodeInvocationBindings::empty(),
        )
        .unwrap();
        PendingNodeResult::commit(intent, owner, journal(checkpoint, offset)).unwrap()
    }

    fn succeeded_attempt(
        checkpoint: &Checkpoint,
        name: &str,
        owner: RunFence,
        start_offset: u64,
        result_offset: u64,
    ) -> (NodeAttempt, PendingNodeResult) {
        let start = crate::NodeAttemptStart::new(
            activation(checkpoint, name),
            AttemptId::generate(),
            owner.clone(),
            journal(checkpoint, start_offset),
        )
        .unwrap();
        let result = result(checkpoint, name, owner, result_offset);
        let completion =
            NodeAttemptCompletion::succeed(&start, result.head(), BudgetUsage::zero()).unwrap();
        (
            NodeAttempt::restore(start, Some(completion)).unwrap(),
            result,
        )
    }

    fn failed_attempt(
        checkpoint: &Checkpoint,
        name: &str,
        owner: RunFence,
        start_offset: u64,
        completion_offset: u64,
        advice: RetryAdvice,
    ) -> NodeAttempt {
        let start = crate::NodeAttemptStart::new(
            activation(checkpoint, name),
            AttemptId::generate(),
            owner,
            journal(checkpoint, start_offset),
        )
        .unwrap();
        let completion_journal = journal(checkpoint, completion_offset);
        let failure = Failure::new(
            FailureId::generate(),
            FailureCategory::DependencyUnavailable,
            FailureCode::new("node.test_failure").unwrap(),
            FailureOrigin::new("runtime.test").unwrap(),
            FailureMessage::new("The deterministic test node failed.").unwrap(),
            advice,
        )
        .unwrap()
        .with_caused_by_event(completion_journal.event_id());
        let completion =
            NodeAttemptCompletion::fail(&start, failure, BudgetUsage::zero(), completion_journal)
                .unwrap();
        NodeAttempt::restore(start, Some(completion)).unwrap()
    }

    fn decision<'a>(plan: &'a ReadyNodeRecoveryPlan, name: &str) -> &'a RecoveryNode {
        plan.nodes()
            .iter()
            .find(|decision| decision.activation().node_id().as_str() == name)
            .unwrap()
    }

    #[test]
    fn pristine_ready_set_is_dispatchable_in_canonical_order() {
        let checkpoint = checkpoint();
        let owner = fence(&checkpoint, 1);
        let journal_head = journal(&checkpoint, 10);
        let observed_at = journal_head.recorded_at();
        let plan = ReadyNodeRecoveryPlanner::new(checkpoint.clone(), owner.clone())
            .unwrap()
            .finish(journal_head.clone(), observed_at)
            .unwrap();

        assert_eq!(plan.checkpoint(), &checkpoint);
        assert_eq!(plan.fence(), &owner);
        assert_eq!(plan.journal_head(), &journal_head);
        assert_eq!(plan.observed_at(), observed_at);
        assert_eq!(
            plan.nodes()
                .iter()
                .map(|decision| decision.activation().node_id().as_str())
                .collect::<Vec<_>>(),
            ["authorize", "reserve-stock"]
        );
        assert!(plan.nodes().iter().all(|decision| {
            decision.kind() == RecoveryNodeKind::Dispatchable
                && decision.dispatch_reason() == Some(NodeDispatchReason::FirstAttempt)
        }));
        assert!(!plan.is_barrier_ready());
        assert!(plan.barrier_result_heads().unwrap().is_none());
    }

    #[test]
    fn committed_siblings_are_reused_independent_of_observation_order() {
        let checkpoint = checkpoint();
        let owner = fence(&checkpoint, 1);
        let (authorize_attempt, authorize) =
            succeeded_attempt(&checkpoint, "authorize", owner.clone(), 1, 2);
        let (reserve_attempt, reserve) =
            succeeded_attempt(&checkpoint, "reserve-stock", owner.clone(), 3, 4);
        let observation = journal(&checkpoint, 10);

        let mut forward = ReadyNodeRecoveryPlanner::new(checkpoint.clone(), owner.clone()).unwrap();
        forward.observe_result(&authorize).unwrap();
        forward.observe_result(&reserve).unwrap();
        forward.observe_attempt(&reserve_attempt).unwrap();
        forward.observe_attempt(&authorize_attempt).unwrap();
        let forward = forward
            .finish(observation.clone(), observation.recorded_at())
            .unwrap();

        let mut reverse = ReadyNodeRecoveryPlanner::new(checkpoint, owner).unwrap();
        reverse.observe_result(&reserve).unwrap();
        reverse.observe_result(&authorize).unwrap();
        let reverse = reverse
            .finish(observation.clone(), observation.recorded_at())
            .unwrap();

        assert!(forward.is_barrier_ready());
        assert!(reverse.is_barrier_ready());
        let forward_heads = forward.barrier_result_heads().unwrap().unwrap();
        let reverse_heads = reverse.barrier_result_heads().unwrap().unwrap();
        assert_eq!(forward_heads, reverse_heads);
        assert_eq!(
            forward_heads
                .iter()
                .map(|head| head.activation().node_id().as_str())
                .collect::<Vec<_>>(),
            ["authorize", "reserve-stock"]
        );
    }

    #[test]
    fn unfinished_attempt_requires_higher_fence_and_same_fence_stays_in_flight() {
        let checkpoint = checkpoint();
        let old_owner = fence(&checkpoint, 1);
        let attempt = executing_attempt(&checkpoint, "authorize", old_owner.clone(), 1);

        let mut same =
            ReadyNodeRecoveryPlanner::new(checkpoint.clone(), old_owner.clone()).unwrap();
        same.observe_attempt(&attempt).unwrap();
        let observation = journal(&checkpoint, 10);
        let same = same
            .finish(observation.clone(), observation.recorded_at())
            .unwrap();
        assert_eq!(
            decision(&same, "authorize").kind(),
            RecoveryNodeKind::InFlight
        );

        let successor = fence(&checkpoint, 2);
        let mut recovered = ReadyNodeRecoveryPlanner::new(checkpoint.clone(), successor).unwrap();
        recovered.observe_attempt(&attempt).unwrap();
        let recovered = recovered
            .finish(observation.clone(), observation.recorded_at())
            .unwrap();
        assert_eq!(
            decision(&recovered, "authorize").dispatch_reason(),
            Some(NodeDispatchReason::SupersededAttempt)
        );

        let stale_owner = fence(&checkpoint, 1);
        let newer_attempt = executing_attempt(&checkpoint, "authorize", fence(&checkpoint, 2), 1);
        let mut stale = ReadyNodeRecoveryPlanner::new(checkpoint, stale_owner).unwrap();
        stale.observe_attempt(&newer_attempt).unwrap();
        assert!(matches!(
            stale.finish(observation.clone(), observation.recorded_at()),
            Err(ReadyNodeRecoveryError::CurrentFenceBehindHistory { .. })
        ));
    }

    #[test]
    fn safe_retry_uses_inclusive_database_time_and_never_failure_blocks() {
        let checkpoint = checkpoint();
        let owner = fence(&checkpoint, 1);
        let retry = failed_attempt(
            &checkpoint,
            "authorize",
            owner.clone(),
            1,
            2,
            RetryAdvice::SafeAfter {
                delay: DurationMillis::new(5_000).unwrap(),
            },
        );
        let failed_at = retry.completion().unwrap().journal_head().recorded_at();
        let retry_observation = retry.completion().unwrap().journal_head().clone();
        let not_before = Timestamp::from_unix_micros(failed_at.unix_micros() + 5_000_000).unwrap();

        let mut early = ReadyNodeRecoveryPlanner::new(checkpoint.clone(), owner.clone()).unwrap();
        early.observe_attempt(&retry).unwrap();
        let early = early
            .finish(
                retry_observation.clone(),
                Timestamp::from_unix_micros(not_before.unix_micros() - 1).unwrap(),
            )
            .unwrap();
        assert_eq!(
            decision(&early, "authorize").kind(),
            RecoveryNodeKind::Deferred
        );
        assert_eq!(decision(&early, "authorize").not_before(), Some(not_before));
        assert_eq!(early.earliest_deferred_at(), Some(not_before));

        let mut due = ReadyNodeRecoveryPlanner::new(checkpoint.clone(), owner.clone()).unwrap();
        due.observe_attempt(&retry).unwrap();
        let due = due.finish(retry_observation, not_before).unwrap();
        assert_eq!(
            decision(&due, "authorize").dispatch_reason(),
            Some(NodeDispatchReason::SafeRetry)
        );

        let terminal = failed_attempt(
            &checkpoint,
            "authorize",
            owner.clone(),
            3,
            4,
            RetryAdvice::Never,
        );
        let terminal_observation = terminal.completion().unwrap().journal_head().clone();
        let mut blocked = ReadyNodeRecoveryPlanner::new(checkpoint, owner).unwrap();
        blocked.observe_attempt(&terminal).unwrap();
        let blocked = blocked.finish(terminal_observation, not_before).unwrap();
        assert_eq!(
            decision(&blocked, "authorize").kind(),
            RecoveryNodeKind::Failed
        );
        assert!(decision(&blocked, "authorize").failure().is_some());
    }

    #[test]
    fn activation_drift_and_success_without_result_fail_closed() {
        let checkpoint = checkpoint();
        let owner = fence(&checkpoint, 1);
        let drifted = NodeActivation::new(
            checkpoint.head(),
            GraphNamespace::root(),
            node("authorize"),
            Digest::sha256(b"drifted scheduler input"),
        );
        let drifted = PendingNodeResult::commit(
            PendingNodeResultIntent::new(
                drifted,
                NodeStateChange::Unchanged,
                NodeControl::Continue,
                NodeInvocationBindings::empty(),
            )
            .unwrap(),
            owner.clone(),
            journal(&checkpoint, 2),
        )
        .unwrap();
        let mut planner = ReadyNodeRecoveryPlanner::new(checkpoint.clone(), owner.clone()).unwrap();
        assert!(matches!(
            planner.observe_result(&drifted),
            Err(ReadyNodeRecoveryError::UnexpectedResult { .. })
        ));

        let (success, _) = succeeded_attempt(&checkpoint, "authorize", owner.clone(), 1, 2);
        let observation = journal(&checkpoint, 10);
        let observed_at = observation.recorded_at();
        let mut missing = ReadyNodeRecoveryPlanner::new(checkpoint, owner).unwrap();
        missing.observe_attempt(&success).unwrap();
        assert!(matches!(
            missing.finish(observation, observed_at),
            Err(ReadyNodeRecoveryError::SucceededWithoutResult { .. })
        ));

        let mismatched_checkpoint = self::checkpoint();
        let owner = fence(&mismatched_checkpoint, 1);
        let durable_result = result(&mismatched_checkpoint, "authorize", owner.clone(), 4);
        let unrelated_failure = failed_attempt(
            &mismatched_checkpoint,
            "authorize",
            owner.clone(),
            1,
            2,
            RetryAdvice::Never,
        );
        let observation = journal(&mismatched_checkpoint, 10);
        let mut mismatched = ReadyNodeRecoveryPlanner::new(mismatched_checkpoint, owner).unwrap();
        mismatched.observe_result(&durable_result).unwrap();
        mismatched.observe_attempt(&unrelated_failure).unwrap();
        assert!(matches!(
            mismatched.finish(observation.clone(), observation.recorded_at()),
            Err(ReadyNodeRecoveryError::ResultAttemptMismatch { .. })
        ));
    }

    #[test]
    fn completed_result_cannot_come_from_a_future_or_conflicting_fence() {
        let checkpoint = checkpoint();
        let old = result(&checkpoint, "authorize", fence(&checkpoint, 1), 2);
        let successor = fence(&checkpoint, 2);
        let observation = journal(&checkpoint, 10);
        let mut reusable = ReadyNodeRecoveryPlanner::new(checkpoint.clone(), successor).unwrap();
        reusable.observe_result(&old).unwrap();
        assert_eq!(
            decision(
                &reusable
                    .finish(observation.clone(), observation.recorded_at())
                    .unwrap(),
                "authorize",
            )
            .kind(),
            RecoveryNodeKind::Completed
        );

        let current = fence(&checkpoint, 1);
        let future = result(&checkpoint, "authorize", fence(&checkpoint, 2), 2);
        let mut future_plan =
            ReadyNodeRecoveryPlanner::new(checkpoint.clone(), current.clone()).unwrap();
        future_plan.observe_result(&future).unwrap();
        assert!(matches!(
            future_plan.finish(observation.clone(), observation.recorded_at()),
            Err(ReadyNodeRecoveryError::CurrentFenceBehindHistory { .. })
        ));

        let conflict = result(&checkpoint, "authorize", fence(&checkpoint, 1), 2);
        let mut conflict_plan = ReadyNodeRecoveryPlanner::new(checkpoint, current).unwrap();
        conflict_plan.observe_result(&conflict).unwrap();
        assert!(matches!(
            conflict_plan.finish(observation.clone(), observation.recorded_at()),
            Err(ReadyNodeRecoveryError::CurrentFenceConflict { .. })
        ));
    }

    #[test]
    fn crossed_scope_is_rejected_before_planning() {
        let checkpoint = checkpoint();
        let crossed_tenant = RunFence::new(
            TenantId::new("another-tenant").unwrap(),
            checkpoint.run_id(),
            AttemptId::generate(),
            FencingEpoch::new(1).unwrap(),
        );
        assert!(matches!(
            ReadyNodeRecoveryPlanner::new(checkpoint.clone(), crossed_tenant),
            Err(ReadyNodeRecoveryError::FenceTenantMismatch)
        ));

        let crossed_run = RunFence::new(
            checkpoint.tenant_id().clone(),
            RunId::generate(),
            AttemptId::generate(),
            FencingEpoch::new(1).unwrap(),
        );
        assert!(matches!(
            ReadyNodeRecoveryPlanner::new(checkpoint, crossed_run),
            Err(ReadyNodeRecoveryError::FenceRunMismatch)
        ));
    }

    #[test]
    fn final_observation_cannot_precede_evidence_or_database_clock() {
        let checkpoint = checkpoint();
        let owner = fence(&checkpoint, 1);
        let durable_result = result(&checkpoint, "authorize", owner.clone(), 2);
        let mut behind_evidence =
            ReadyNodeRecoveryPlanner::new(checkpoint.clone(), owner.clone()).unwrap();
        behind_evidence.observe_result(&durable_result).unwrap();
        assert!(matches!(
            behind_evidence.finish(
                checkpoint.journal_head().clone(),
                durable_result.journal_head().recorded_at(),
            ),
            Err(ReadyNodeRecoveryError::ObservationBeforeEvidence)
        ));

        let observation = journal(&checkpoint, 10);
        let before_observation =
            Timestamp::from_unix_micros(observation.recorded_at().unix_micros() - 1).unwrap();
        assert!(matches!(
            ReadyNodeRecoveryPlanner::new(checkpoint, owner)
                .unwrap()
                .finish(observation, before_observation),
            Err(ReadyNodeRecoveryError::ObservationClockRegression)
        ));
    }

    #[test]
    fn physical_attempt_history_has_a_hard_recovery_bound() {
        let checkpoint = checkpoint();
        let owner = fence(&checkpoint, 1);
        let mut planner = ReadyNodeRecoveryPlanner::new(checkpoint.clone(), owner.clone()).unwrap();
        for index in 0..ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
            let start_offset = u64::try_from(index * 2 + 1).unwrap();
            planner
                .observe_attempt(&failed_attempt(
                    &checkpoint,
                    "authorize",
                    owner.clone(),
                    start_offset,
                    start_offset + 1,
                    RetryAdvice::SafeAfter {
                        delay: DurationMillis::ZERO,
                    },
                ))
                .unwrap();
        }
        let observation = journal(&checkpoint, 200);
        let exhausted = planner
            .clone()
            .finish(observation.clone(), observation.recorded_at())
            .unwrap();
        assert_eq!(
            decision(&exhausted, "authorize").kind(),
            RecoveryNodeKind::Exhausted
        );
        assert!(
            decision(&exhausted, "authorize")
                .exhausted_attempt()
                .is_some()
        );
        let overflow_offset =
            u64::try_from(ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE * 2 + 1).unwrap();
        assert!(matches!(
            planner.observe_attempt(&failed_attempt(
                &checkpoint,
                "authorize",
                owner,
                overflow_offset,
                overflow_offset + 1,
                RetryAdvice::SafeAfter {
                    delay: DurationMillis::ZERO,
                },
            )),
            Err(ReadyNodeRecoveryError::AttemptLimitExceeded {
                maximum: ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE,
                ..
            })
        ));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn arbitrary_result_observation_order_produces_canonical_barrier(
            (count, priorities) in (1_usize..=32).prop_flat_map(|count| {
                (Just(count), prop::collection::vec(any::<u16>(), count))
            }),
        ) {
            let checkpoint = checkpoint_with_nodes(count);
            let owner = fence(&checkpoint, 1);
            let results = checkpoint
                .ready_nodes()
                .iter()
                .enumerate()
                .map(|(index, node_id)| {
                    result(
                        &checkpoint,
                        node_id.as_str(),
                        owner.clone(),
                        u64::try_from(index + 1).unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            let mut order = (0..count).collect::<Vec<_>>();
            order.sort_unstable_by_key(|index| (priorities[*index], *index));

            let mut planner =
                ReadyNodeRecoveryPlanner::new(checkpoint.clone(), owner).unwrap();
            for index in order {
                planner.observe_result(&results[index]).unwrap();
            }
            let observation = journal(&checkpoint, u64::try_from(count + 1).unwrap());
            let plan = planner
                .finish(observation.clone(), observation.recorded_at())
                .unwrap();
            let expected = checkpoint
                .ready_nodes()
                .iter()
                .map(NodeId::as_str)
                .collect::<Vec<_>>();
            let actual = plan
                .nodes()
                .iter()
                .map(|decision| decision.activation().node_id().as_str())
                .collect::<Vec<_>>();
            prop_assert_eq!(actual, expected.clone());
            let barrier = plan.barrier_result_heads().unwrap().unwrap();
            let barrier_order = barrier
                .iter()
                .map(|head| head.activation().node_id().as_str())
                .collect::<Vec<_>>();
            prop_assert_eq!(barrier_order, expected);
        }
    }
}
