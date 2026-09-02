// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable physical node-attempt starts, completions, and recovery history.
//!
//! A worker must commit [`NodeAttemptStart`] before invoking user node code.
//! The start owns a fresh physical [`AttemptId`] while [`RunFence`] proves the
//! worker lease that authorized it. Completion is append-only: success binds
//! the exact immutable pending result committed by the same worker event, and
//! failure binds public-safe evidence plus explicit retry advice. A crash may
//! leave a start without a completion; only a higher fencing epoch may recover
//! that in-flight attempt.

use std::{collections::BTreeSet, fmt};

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    AttemptId, BudgetUsage, Digest, Failure, JournalHead, NodeActivation, PendingNodeResultHead,
    RetryAdvice, RunFence, Timestamp,
};

const ACTIVATION_DIGEST_DOMAIN: &[u8] = b"stateknot-node-activation-v1\0";
const START_DIGEST_DOMAIN: &[u8] = b"stateknot-node-attempt-start-v1\0";
const COMPLETION_DIGEST_DOMAIN: &[u8] = b"stateknot-node-attempt-completion-v1\0";

/// Durable lifecycle state of one physical node attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NodeAttemptStatus {
    /// The start committed, but no terminal completion exists.
    Executing,
    /// The exact pending node result committed atomically with completion.
    Succeeded,
    /// Public-safe failure evidence committed without a logical node result.
    Failed,
}

/// Immutable terminal outcome of one physical node attempt.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, JsonSchema, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum NodeAttemptOutcome {
    /// The attempt produced one exact immutable pending result.
    Succeeded {
        /// Result committed in the same journal transaction as completion.
        result: Box<PendingNodeResultHead>,
    },
    /// The attempt stopped without a logical node result.
    Failed {
        /// Public-safe failure evidence with explicit recovery advice.
        failure: Failure,
    },
}

impl NodeAttemptOutcome {
    /// Returns the terminal lifecycle discriminator.
    #[must_use]
    pub const fn status(&self) -> NodeAttemptStatus {
        match self {
            Self::Succeeded { .. } => NodeAttemptStatus::Succeeded,
            Self::Failed { .. } => NodeAttemptStatus::Failed,
        }
    }

    /// Returns the successful pending-result head, if present.
    #[must_use]
    pub fn result(&self) -> Option<&PendingNodeResultHead> {
        match self {
            Self::Succeeded { result } => Some(result.as_ref()),
            Self::Failed { .. } => None,
        }
    }

    /// Returns the public-safe failure, if present.
    #[must_use]
    pub const fn failure(&self) -> Option<&Failure> {
        match self {
            Self::Succeeded { .. } => None,
            Self::Failed { failure } => Some(failure),
        }
    }
}

impl fmt::Debug for NodeAttemptOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Succeeded { result } => formatter
                .debug_struct("Succeeded")
                .field("result", result)
                .finish(),
            Self::Failed { failure } => formatter
                .debug_struct("Failed")
                .field("failure_id", &failure.id())
                .field("category", &failure.category())
                .field("code", failure.code())
                .field("origin", failure.origin())
                .field("retry_advice", &failure.retry_advice())
                .finish_non_exhaustive(),
        }
    }
}

impl<'de> Deserialize<'de> for NodeAttemptOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[allow(clippy::large_enum_variant)]
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Succeeded { result: Box<PendingNodeResultHead> },
            Failed { failure: Failure },
        }

        Ok(match Wire::deserialize(deserializer)? {
            Wire::Succeeded { result } => Self::Succeeded { result },
            Wire::Failed { failure } => Self::Failed { failure },
        })
    }
}

/// Immutable proof that one physical node execution was durably admitted.
///
/// `attempt_id` identifies this node execution and is distinct from the
/// worker-run attempt carried by `fence`. A production store must claim it in
/// the run-wide physical-attempt namespace and repeat the exact live fence on
/// the event and start-row inserts.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeAttemptStart {
    activation: NodeActivation,
    activation_digest: Digest,
    attempt_id: AttemptId,
    fence: RunFence,
    journal_head: JournalHead,
    digest: Digest,
}

impl NodeAttemptStart {
    /// Constructs an integrity-bound durable start after its event commits.
    ///
    /// # Errors
    ///
    /// Returns [`NodeAttemptError`] for crossed scope, invalid ordering,
    /// worker/node attempt identity reuse, or integrity failure.
    pub fn new(
        activation: NodeActivation,
        attempt_id: AttemptId,
        fence: RunFence,
        journal_head: JournalHead,
    ) -> Result<Self, NodeAttemptError> {
        validate_start_scope(&activation, attempt_id, &fence, &journal_head)?;
        let activation_digest = compute_activation_digest(&activation)?;
        let digest = compute_start_digest(&NodeAttemptStartDigestWire {
            activation_digest,
            attempt_id,
            fence: &fence,
            journal_head: &journal_head,
        })?;
        Ok(Self {
            activation,
            activation_digest,
            attempt_id,
            fence,
            journal_head,
            digest,
        })
    }

    /// Restores and verifies a durable start record.
    ///
    /// # Errors
    ///
    /// Returns [`NodeAttemptError`] when any invariant or checksum differs.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        activation: NodeActivation,
        activation_digest: Digest,
        attempt_id: AttemptId,
        fence: RunFence,
        journal_head: JournalHead,
        digest: Digest,
    ) -> Result<Self, NodeAttemptError> {
        let restored = Self::new(activation, attempt_id, fence, journal_head)?;
        if restored.activation_digest != activation_digest {
            return Err(NodeAttemptError::ActivationDigestMismatch);
        }
        if restored.digest != digest {
            return Err(NodeAttemptError::StartDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the exact logical activation.
    #[must_use]
    pub const fn activation(&self) -> &NodeActivation {
        &self.activation
    }

    /// Returns the stable logical-activation fingerprint.
    #[must_use]
    pub const fn activation_digest(&self) -> Digest {
        self.activation_digest
    }

    /// Returns the unique physical node-attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the worker lease that admitted this attempt.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Returns the exact start journal anchor.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the complete start-record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns a compact exact reference used by completion records.
    #[must_use]
    pub fn head(&self) -> NodeAttemptStartHead {
        NodeAttemptStartHead {
            activation: self.activation.clone(),
            attempt_id: self.attempt_id,
            fence: self.fence.clone(),
            journal_head: self.journal_head.clone(),
            digest: self.digest,
        }
    }
}

impl fmt::Debug for NodeAttemptStart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeAttemptStart")
            .field("activation", &self.activation)
            .field("activation_digest", &self.activation_digest)
            .field("attempt_id", &self.attempt_id)
            .field("fence", &self.fence)
            .field("journal_head", &self.journal_head)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for NodeAttemptStart {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            activation: NodeActivation,
            activation_digest: Digest,
            attempt_id: AttemptId,
            fence: RunFence,
            journal_head: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.activation,
            wire.activation_digest,
            wire.attempt_id,
            wire.fence,
            wire.journal_head,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Exact compact identity of one durable node-attempt start.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeAttemptStartHead {
    activation: NodeActivation,
    attempt_id: AttemptId,
    fence: RunFence,
    journal_head: JournalHead,
    digest: Digest,
}

impl NodeAttemptStartHead {
    /// Restores a compact start identity and verifies its complete checksum.
    ///
    /// # Errors
    ///
    /// Returns [`NodeAttemptError`] for invalid scope, ordering, or checksum.
    pub fn restore(
        activation: NodeActivation,
        attempt_id: AttemptId,
        fence: RunFence,
        journal_head: JournalHead,
        digest: Digest,
    ) -> Result<Self, NodeAttemptError> {
        let activation_digest = compute_activation_digest(&activation)?;
        Ok(NodeAttemptStart::restore(
            activation,
            activation_digest,
            attempt_id,
            fence,
            journal_head,
            digest,
        )?
        .head())
    }

    /// Returns the exact logical activation.
    #[must_use]
    pub const fn activation(&self) -> &NodeActivation {
        &self.activation
    }

    /// Returns the physical node-attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the worker lease that admitted the attempt.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Returns the exact start journal anchor.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the exact start-record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for NodeAttemptStartHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            activation: NodeActivation,
            attempt_id: AttemptId,
            fence: RunFence,
            journal_head: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        validate_start_scope(
            &wire.activation,
            wire.attempt_id,
            &wire.fence,
            &wire.journal_head,
        )
        .map_err(de::Error::custom)?;
        let activation_digest =
            compute_activation_digest(&wire.activation).map_err(de::Error::custom)?;
        let expected = compute_start_digest(&NodeAttemptStartDigestWire {
            activation_digest,
            attempt_id: wire.attempt_id,
            fence: &wire.fence,
            journal_head: &wire.journal_head,
        })
        .map_err(de::Error::custom)?;
        if expected != wire.digest {
            return Err(de::Error::custom(NodeAttemptError::StartDigestMismatch));
        }
        Ok(Self {
            activation: wire.activation,
            attempt_id: wire.attempt_id,
            fence: wire.fence,
            journal_head: wire.journal_head,
            digest: wire.digest,
        })
    }
}

/// Immutable terminal record for one already-started physical node attempt.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeAttemptCompletion {
    start: NodeAttemptStartHead,
    outcome: NodeAttemptOutcome,
    usage: BudgetUsage,
    journal_head: JournalHead,
    digest: Digest,
}

impl NodeAttemptCompletion {
    /// Commits success against the exact pending result produced by the start.
    ///
    /// The completion and pending result deliberately share one journal head;
    /// the storage adapter must insert both in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns [`NodeAttemptError`] for substituted activation, fence, journal,
    /// or integrity material.
    pub fn succeed(
        start: &NodeAttemptStart,
        result: PendingNodeResultHead,
        usage: BudgetUsage,
    ) -> Result<Self, NodeAttemptError> {
        let journal_head = result.journal_head().clone();
        Self::materialize(
            start.head(),
            NodeAttemptOutcome::Succeeded {
                result: Box::new(result),
            },
            usage,
            journal_head,
        )
    }

    /// Commits a failed attempt with explicit public recovery evidence.
    ///
    /// The failure must name `journal_head.event_id()` as its durable cause.
    /// Reconcile-first failures are rejected because node code may perform
    /// external effects only through the separate invocation ledgers.
    ///
    /// # Errors
    ///
    /// Returns [`NodeAttemptError`] for invalid causation, retry semantics,
    /// scope, ordering, or integrity material.
    pub fn fail(
        start: &NodeAttemptStart,
        failure: Failure,
        usage: BudgetUsage,
        journal_head: JournalHead,
    ) -> Result<Self, NodeAttemptError> {
        Self::materialize(
            start.head(),
            NodeAttemptOutcome::Failed { failure },
            usage,
            journal_head,
        )
    }

    /// Restores and verifies a durable terminal record.
    ///
    /// # Errors
    ///
    /// Returns [`NodeAttemptError`] when any local invariant or checksum fails.
    pub fn restore(
        start: NodeAttemptStartHead,
        outcome: NodeAttemptOutcome,
        usage: BudgetUsage,
        journal_head: JournalHead,
        digest: Digest,
    ) -> Result<Self, NodeAttemptError> {
        let restored = Self::materialize(start, outcome, usage, journal_head)?;
        if restored.digest != digest {
            return Err(NodeAttemptError::CompletionDigestMismatch);
        }
        Ok(restored)
    }

    fn materialize(
        start: NodeAttemptStartHead,
        outcome: NodeAttemptOutcome,
        usage: BudgetUsage,
        journal_head: JournalHead,
    ) -> Result<Self, NodeAttemptError> {
        validate_completion_shape(&start, &outcome, &journal_head)?;
        let digest = compute_completion_digest(&NodeAttemptCompletionDigestWire {
            start: &start,
            outcome: &outcome,
            usage: &usage,
            journal_head: &journal_head,
        })?;
        Ok(Self {
            start,
            outcome,
            usage,
            journal_head,
            digest,
        })
    }

    /// Returns the exact start completed by this record.
    #[must_use]
    pub const fn start(&self) -> &NodeAttemptStartHead {
        &self.start
    }

    /// Returns the terminal outcome.
    #[must_use]
    pub const fn outcome(&self) -> &NodeAttemptOutcome {
        &self.outcome
    }

    /// Returns the terminal status.
    #[must_use]
    pub const fn status(&self) -> NodeAttemptStatus {
        self.outcome.status()
    }

    /// Returns the exact usage attributed to this physical attempt.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    /// Returns the exact terminal journal anchor.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the complete completion-record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl fmt::Debug for NodeAttemptCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeAttemptCompletion")
            .field("start", &self.start)
            .field("outcome", &self.outcome)
            .field("usage_recorded", &true)
            .field("journal_head", &self.journal_head)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for NodeAttemptCompletion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            start: NodeAttemptStartHead,
            outcome: NodeAttemptOutcome,
            usage: BudgetUsage,
            journal_head: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.start,
            wire.outcome,
            wire.usage,
            wire.journal_head,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Fully restored physical node attempt, executing or terminal.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeAttempt {
    start: NodeAttemptStart,
    #[serde(skip_serializing_if = "Option::is_none")]
    completion: Option<NodeAttemptCompletion>,
}

impl NodeAttempt {
    /// Constructs an executing attempt from its durable start.
    #[must_use]
    pub const fn executing(start: NodeAttemptStart) -> Self {
        Self {
            start,
            completion: None,
        }
    }

    /// Restores an attempt and verifies the exact start/completion join.
    ///
    /// # Errors
    ///
    /// Returns [`NodeAttemptError::CompletionStartMismatch`] when completion
    /// belongs to another physical attempt.
    pub fn restore(
        start: NodeAttemptStart,
        completion: Option<NodeAttemptCompletion>,
    ) -> Result<Self, NodeAttemptError> {
        if completion
            .as_ref()
            .is_some_and(|completion| completion.start() != &start.head())
        {
            return Err(NodeAttemptError::CompletionStartMismatch);
        }
        Ok(Self { start, completion })
    }

    /// Returns the immutable durable start.
    #[must_use]
    pub const fn start(&self) -> &NodeAttemptStart {
        &self.start
    }

    /// Returns the optional terminal completion.
    #[must_use]
    pub const fn completion(&self) -> Option<&NodeAttemptCompletion> {
        self.completion.as_ref()
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub const fn status(&self) -> NodeAttemptStatus {
        match &self.completion {
            Some(completion) => completion.status(),
            None => NodeAttemptStatus::Executing,
        }
    }
}

impl fmt::Debug for NodeAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeAttempt")
            .field("start", &self.start)
            .field("completion", &self.completion)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for NodeAttempt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            start: NodeAttemptStart,
            #[serde(default)]
            completion: Option<NodeAttemptCompletion>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.start, wire.completion).map_err(de::Error::custom)
    }
}

/// Streaming validator for one logical activation's physical attempts.
///
/// Histories are ordered by start journal position. A failed attempt may be
/// followed only after its explicit safe-after delay; an unfinished attempt
/// may be replaced only by a higher worker fencing epoch. Rejections never
/// advance verifier state.
#[derive(Clone, Debug, Default)]
pub struct NodeAttemptHistoryVerifier {
    last: Option<NodeAttempt>,
    attempt_ids: BTreeSet<AttemptId>,
}

impl NodeAttemptHistoryVerifier {
    /// Constructs an empty history verifier.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            last: None,
            attempt_ids: BTreeSet::new(),
        }
    }

    /// Continues after one trusted full record used as a page cursor.
    ///
    /// The durable run-wide attempt registry must still prove that attempts
    /// preceding the cursor did not reuse the next ID.
    #[must_use]
    pub fn after(record: NodeAttempt) -> Self {
        let attempt_id = record.start.attempt_id;
        Self {
            last: Some(record),
            attempt_ids: BTreeSet::from([attempt_id]),
        }
    }

    /// Returns whether at least one attempt has been verified.
    #[must_use]
    pub const fn has_records(&self) -> bool {
        self.last.is_some()
    }

    /// Returns the last verified full attempt, if present.
    #[must_use]
    pub const fn last(&self) -> Option<&NodeAttempt> {
        self.last.as_ref()
    }

    /// Verifies and advances to the next physical attempt.
    ///
    /// # Errors
    ///
    /// Returns [`NodeAttemptHistoryError`] for activation substitution, reused
    /// physical identity, regressing ownership/journal order, unsafe retry, or
    /// retry before the durable safe-after delay.
    pub fn verify_next(&mut self, attempt: &NodeAttempt) -> Result<(), NodeAttemptHistoryError> {
        let attempt_id = attempt.start.attempt_id;
        if self.attempt_ids.contains(&attempt_id) {
            return Err(NodeAttemptHistoryError::AttemptIdReused { attempt_id });
        }

        if let Some(previous) = self.last.as_ref() {
            validate_history_successor(previous, attempt)?;
        }

        self.attempt_ids.insert(attempt_id);
        self.last = Some(attempt.clone());
        Ok(())
    }
}

/// Invalid physical-attempt history for one logical node activation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeAttemptHistoryError {
    /// A physical attempt identity was used twice in the supplied history.
    #[error("node attempt history reused physical attempt {attempt_id}")]
    AttemptIdReused {
        /// Reused identity.
        attempt_id: AttemptId,
    },
    /// A successor changed the logical node activation.
    #[error("node attempt history changed its logical activation")]
    ActivationMismatch,
    /// A successor start did not strictly follow the preceding durable record.
    #[error("node attempt history journal position did not advance")]
    JournalNotAfterPrevious,
    /// A successor durable clock preceded the preceding record.
    #[error("node attempt history durable clock regressed")]
    ClockRegression,
    /// Worker ownership epoch moved backwards.
    #[error("node attempt history worker fencing epoch regressed")]
    FenceEpochRegression,
    /// One epoch named two different worker attempts.
    #[error("node attempt history changed worker attempt without advancing the epoch")]
    WorkerChangedWithinEpoch,
    /// A higher epoch improperly reused an earlier worker attempt identity.
    #[error("node attempt history reused a worker attempt across fencing epochs")]
    WorkerReusedAcrossEpoch,
    /// An unfinished attempt was retried without a higher fencing epoch.
    #[error("an unfinished node attempt may be recovered only under a higher fencing epoch")]
    UnfinishedAttemptNotSuperseded,
    /// A successful logical activation was executed again.
    #[error("a successful node activation cannot start another physical attempt")]
    PreviousAttemptSucceeded,
    /// Failure evidence did not authorize an automatic retry.
    #[error("node failure recovery advice does not authorize automatic retry")]
    RetryNotAuthorized {
        /// Explicit advice that blocked retry.
        advice: RetryAdvice,
    },
    /// A successor start preceded the explicit safe-after delay.
    #[error("node attempt retry started before its explicit safe-after delay elapsed")]
    RetryDelayNotElapsed {
        /// Required delay.
        delay_millis: i64,
        /// Failure commit time.
        failed_at: Timestamp,
        /// Rejected successor start time.
        started_at: Timestamp,
    },
}

/// Invalid node-attempt scope, causation, lifecycle, or integrity material.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeAttemptError {
    /// Node physical attempt reused the worker-run attempt identity.
    #[error("node physical attempt must differ from the worker-run attempt")]
    WorkerAttemptReused,
    /// Worker fence crossed the activation tenant boundary.
    #[error("node attempt fence crosses the activation tenant boundary")]
    FenceTenantMismatch,
    /// Worker fence named another run.
    #[error("node attempt fence does not belong to the activation run")]
    FenceRunMismatch,
    /// Journal head crossed the activation tenant boundary.
    #[error("node attempt journal crosses the activation tenant boundary")]
    JournalTenantMismatch,
    /// Journal head named another run.
    #[error("node attempt journal does not belong to the activation run")]
    JournalRunMismatch,
    /// Start journal did not strictly follow the base checkpoint.
    #[error("node attempt start journal does not follow its base checkpoint")]
    StartJournalNotAfterBase,
    /// Start durable time preceded the base checkpoint.
    #[error("node attempt start clock precedes its base checkpoint")]
    StartClockRegression,
    /// Completion journal did not strictly follow its start.
    #[error("node attempt completion journal does not follow its start")]
    CompletionJournalNotAfterStart,
    /// Completion durable time preceded its start.
    #[error("node attempt completion clock precedes its start")]
    CompletionClockRegression,
    /// Successful result belonged to another logical activation.
    #[error("node attempt success result belongs to another activation")]
    ResultActivationMismatch,
    /// Successful result was committed by another worker fence.
    #[error("node attempt success result uses another worker fence")]
    ResultFenceMismatch,
    /// Successful result and attempt completion did not share one journal event.
    #[error("node attempt success result and completion journal differ")]
    ResultJournalMismatch,
    /// Failed attempt did not name its completion event as the direct cause.
    #[error("node attempt failure must name its exact completion event as cause")]
    FailureEventMismatch,
    /// Pure node execution cannot own an ambiguous external side effect.
    #[error("node attempt failure cannot require external reconciliation")]
    ReconciliationUnsupported,
    /// Completion joined to another start record.
    #[error("node attempt completion does not belong to its start")]
    CompletionStartMismatch,
    /// Persisted activation checksum did not match the activation.
    #[error("node activation digest does not match its fields")]
    ActivationDigestMismatch,
    /// Persisted start checksum did not match the start fields.
    #[error("node attempt start digest does not match its fields")]
    StartDigestMismatch,
    /// Persisted completion checksum did not match completion fields.
    #[error("node attempt completion digest does not match its fields")]
    CompletionDigestMismatch,
    /// Canonical integrity material could not be serialized.
    #[error("node attempt integrity calculation failed: {source}")]
    Integrity {
        /// Exact integrity failure.
        #[source]
        source: NodeAttemptIntegrityError,
    },
}

impl From<NodeAttemptIntegrityError> for NodeAttemptError {
    fn from(source: NodeAttemptIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Failure to canonicalize node-attempt integrity material.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeAttemptIntegrityError {
    /// A closed typed checksum preimage could not be canonicalized.
    #[error("node attempt checksum preimage serialization failed")]
    CanonicalSerialization,
}

#[derive(Serialize)]
struct NodeAttemptStartDigestWire<'a> {
    activation_digest: Digest,
    attempt_id: AttemptId,
    fence: &'a RunFence,
    journal_head: &'a JournalHead,
}

#[derive(Serialize)]
struct NodeAttemptCompletionDigestWire<'a> {
    start: &'a NodeAttemptStartHead,
    outcome: &'a NodeAttemptOutcome,
    usage: &'a BudgetUsage,
    journal_head: &'a JournalHead,
}

fn compute_activation_digest(
    activation: &NodeActivation,
) -> Result<Digest, NodeAttemptIntegrityError> {
    domain_separated_digest(ACTIVATION_DIGEST_DOMAIN, activation)
}

fn compute_start_digest(
    value: &NodeAttemptStartDigestWire<'_>,
) -> Result<Digest, NodeAttemptIntegrityError> {
    domain_separated_digest(START_DIGEST_DOMAIN, value)
}

fn compute_completion_digest(
    value: &NodeAttemptCompletionDigestWire<'_>,
) -> Result<Digest, NodeAttemptIntegrityError> {
    domain_separated_digest(COMPLETION_DIGEST_DOMAIN, value)
}

fn domain_separated_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, NodeAttemptIntegrityError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| NodeAttemptIntegrityError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn validate_start_scope(
    activation: &NodeActivation,
    attempt_id: AttemptId,
    fence: &RunFence,
    journal_head: &JournalHead,
) -> Result<(), NodeAttemptError> {
    if attempt_id == fence.attempt_id() {
        return Err(NodeAttemptError::WorkerAttemptReused);
    }
    validate_scope(activation, fence, journal_head)?;
    let base = activation.base_checkpoint().journal_head();
    if journal_head.sequence() <= base.sequence() {
        return Err(NodeAttemptError::StartJournalNotAfterBase);
    }
    if journal_head.recorded_at() < base.recorded_at() {
        return Err(NodeAttemptError::StartClockRegression);
    }
    Ok(())
}

fn validate_scope(
    activation: &NodeActivation,
    fence: &RunFence,
    journal_head: &JournalHead,
) -> Result<(), NodeAttemptError> {
    if fence.tenant_id() != activation.tenant_id() {
        return Err(NodeAttemptError::FenceTenantMismatch);
    }
    if fence.run_id() != activation.run_id() {
        return Err(NodeAttemptError::FenceRunMismatch);
    }
    if journal_head.tenant_id() != activation.tenant_id() {
        return Err(NodeAttemptError::JournalTenantMismatch);
    }
    if journal_head.run_id() != activation.run_id() {
        return Err(NodeAttemptError::JournalRunMismatch);
    }
    Ok(())
}

fn validate_completion_shape(
    start: &NodeAttemptStartHead,
    outcome: &NodeAttemptOutcome,
    journal_head: &JournalHead,
) -> Result<(), NodeAttemptError> {
    validate_scope(start.activation(), start.fence(), journal_head)?;
    if journal_head.sequence() <= start.journal_head().sequence() {
        return Err(NodeAttemptError::CompletionJournalNotAfterStart);
    }
    if journal_head.recorded_at() < start.journal_head().recorded_at() {
        return Err(NodeAttemptError::CompletionClockRegression);
    }

    match outcome {
        NodeAttemptOutcome::Succeeded { result } => {
            if result.activation() != start.activation() {
                return Err(NodeAttemptError::ResultActivationMismatch);
            }
            if result.fence() != start.fence() {
                return Err(NodeAttemptError::ResultFenceMismatch);
            }
            if result.journal_head() != journal_head {
                return Err(NodeAttemptError::ResultJournalMismatch);
            }
        }
        NodeAttemptOutcome::Failed { failure } => {
            if failure.retry_advice().requires_reconciliation() {
                return Err(NodeAttemptError::ReconciliationUnsupported);
            }
            if failure.caused_by_event_id() != Some(journal_head.event_id()) {
                return Err(NodeAttemptError::FailureEventMismatch);
            }
        }
    }
    Ok(())
}

fn validate_history_successor(
    previous: &NodeAttempt,
    next: &NodeAttempt,
) -> Result<(), NodeAttemptHistoryError> {
    if next.start.activation != previous.start.activation {
        return Err(NodeAttemptHistoryError::ActivationMismatch);
    }

    let previous_anchor = previous.completion.as_ref().map_or_else(
        || previous.start.journal_head(),
        |value| value.journal_head(),
    );
    let next_anchor = next.start.journal_head();
    if next_anchor.sequence() <= previous_anchor.sequence() {
        return Err(NodeAttemptHistoryError::JournalNotAfterPrevious);
    }
    if next_anchor.recorded_at() < previous_anchor.recorded_at() {
        return Err(NodeAttemptHistoryError::ClockRegression);
    }

    let previous_fence = previous.start.fence();
    let next_fence = next.start.fence();
    if next_fence.epoch() < previous_fence.epoch() {
        return Err(NodeAttemptHistoryError::FenceEpochRegression);
    }
    if next_fence.epoch() == previous_fence.epoch()
        && next_fence.attempt_id() != previous_fence.attempt_id()
    {
        return Err(NodeAttemptHistoryError::WorkerChangedWithinEpoch);
    }
    if next_fence.epoch() > previous_fence.epoch()
        && next_fence.attempt_id() == previous_fence.attempt_id()
    {
        return Err(NodeAttemptHistoryError::WorkerReusedAcrossEpoch);
    }

    let Some(completion) = previous.completion.as_ref() else {
        if next_fence.epoch() <= previous_fence.epoch() {
            return Err(NodeAttemptHistoryError::UnfinishedAttemptNotSuperseded);
        }
        return Ok(());
    };

    match completion.outcome() {
        NodeAttemptOutcome::Succeeded { .. } => {
            Err(NodeAttemptHistoryError::PreviousAttemptSucceeded)
        }
        NodeAttemptOutcome::Failed { failure } => match failure.retry_advice() {
            RetryAdvice::SafeAfter { delay } => {
                let failed_at = completion.journal_head().recorded_at();
                let started_at = next.start.journal_head().recorded_at();
                let eligible_at =
                    i128::from(failed_at.unix_micros()) + i128::from(delay.as_i64()) * 1_000;
                if i128::from(started_at.unix_micros()) < eligible_at {
                    Err(NodeAttemptHistoryError::RetryDelayNotElapsed {
                        delay_millis: delay.as_i64(),
                        failed_at,
                        started_at,
                    })
                } else {
                    Ok(())
                }
            }
            advice @ (RetryAdvice::Never | RetryAdvice::ReconcileFirst) => {
                Err(NodeAttemptHistoryError::RetryNotAuthorized { advice })
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DurationMillis, EventId, FailureCategory, FailureCode, FailureId, FailureMessage,
        FailureOrigin, FencingEpoch, GraphNamespace, JournalSequence, NodeControl,
        NodeInvocationBindings, NodeStateChange, PendingNodeResult, PendingNodeResultIntent,
    };
    use serde_json::{Value, from_value, json, to_value};

    fn checkpoint() -> crate::Checkpoint {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-checkpoint-v1.json"))
                .unwrap();
        from_value(fixture["checkpoints"][0].clone()).unwrap()
    }

    fn activation(node: &str) -> NodeActivation {
        NodeActivation::new(
            checkpoint().head(),
            GraphNamespace::root(),
            crate::NodeId::new(node).unwrap(),
            Digest::sha256(format!("{node}-input")),
        )
    }

    fn id<T: std::str::FromStr>(suffix: u8) -> T
    where
        T::Err: fmt::Debug,
    {
        format!("01912345-6789-7abc-8def-0123456789{suffix:02x}")
            .parse()
            .unwrap()
    }

    fn fence(activation: &NodeActivation, worker_suffix: u8, epoch: u64) -> RunFence {
        RunFence::new(
            activation.tenant_id().clone(),
            activation.run_id(),
            id(worker_suffix),
            FencingEpoch::new(epoch).unwrap(),
        )
    }

    fn journal(activation: &NodeActivation, sequence: u64) -> JournalHead {
        let base = activation.base_checkpoint().journal_head();
        JournalHead::new(
            activation.tenant_id().clone(),
            activation.run_id(),
            JournalSequence::new(sequence).unwrap(),
            id::<EventId>(u8::try_from(0xd0 + sequence).unwrap()),
            Timestamp::from_unix_micros(
                base.recorded_at().unix_micros()
                    + i64::try_from(sequence - base.sequence().get()).unwrap() * 1_000_000,
            )
            .unwrap(),
            Digest::sha256(sequence.to_be_bytes()),
        )
    }

    fn start(
        activation: &NodeActivation,
        node_suffix: u8,
        worker_suffix: u8,
        epoch: u64,
        sequence: u64,
    ) -> NodeAttemptStart {
        NodeAttemptStart::new(
            activation.clone(),
            id(node_suffix),
            fence(activation, worker_suffix, epoch),
            journal(activation, sequence),
        )
        .unwrap()
    }

    fn pending_result(start: &NodeAttemptStart, sequence: u64) -> PendingNodeResultHead {
        let intent = PendingNodeResultIntent::new(
            start.activation().clone(),
            NodeStateChange::Unchanged,
            NodeControl::Continue,
            NodeInvocationBindings::empty(),
        )
        .unwrap();
        PendingNodeResult::commit(
            intent,
            start.fence().clone(),
            journal(start.activation(), sequence),
        )
        .unwrap()
        .head()
    }

    fn failure(event_id: EventId, suffix: u8, retry_advice: RetryAdvice) -> Failure {
        Failure::new(
            id::<FailureId>(suffix),
            FailureCategory::Internal,
            FailureCode::new("node.failed").unwrap(),
            FailureOrigin::new("graph.node").unwrap(),
            FailureMessage::new("Node execution failed safely").unwrap(),
            retry_advice,
        )
        .unwrap()
        .with_caused_by_event(event_id)
    }

    #[test]
    fn starts_and_successes_are_exact_closed_and_integrity_bound() {
        let activation = activation("authorize");
        let start = start(&activation, 0xa1, 0xb1, 1, 2);
        let result = pending_result(&start, 3);
        let completion =
            NodeAttemptCompletion::succeed(&start, result, BudgetUsage::zero()).unwrap();
        let attempt = NodeAttempt::restore(start.clone(), Some(completion.clone())).unwrap();

        assert_eq!(attempt.status(), NodeAttemptStatus::Succeeded);
        assert_eq!(
            completion.outcome().result().unwrap().activation(),
            &activation
        );
        assert_eq!(
            from_value::<NodeAttempt>(to_value(&attempt).unwrap())
                .unwrap()
                .status(),
            NodeAttemptStatus::Succeeded
        );
        assert_eq!(
            from_value::<NodeAttemptStartHead>(to_value(start.head()).unwrap()).unwrap(),
            start.head()
        );
        assert_eq!(
            completion.digest().to_string(),
            "sha256:f6a5ea9ec8026144396c17a583ee350ac5097d28998e1613893501515b438ac7"
        );

        let mut changed = to_value(&start).unwrap();
        changed["attempt_id"] = json!(id::<AttemptId>(0xa2));
        assert!(from_value::<NodeAttemptStart>(changed).is_err());

        let mut extra = to_value(&completion).unwrap();
        extra["unsafe_extension"] = json!(true);
        assert!(from_value::<NodeAttemptCompletion>(extra).is_err());
    }

    #[test]
    fn completion_rejects_substitution_and_ambiguous_node_effects() {
        let activation = activation("authorize");
        let first_start = start(&activation, 0xa1, 0xb1, 1, 2);
        let another_start = start(&activation, 0xa2, 0xb2, 2, 3);
        let another_result = pending_result(&another_start, 4);
        assert!(matches!(
            NodeAttemptCompletion::succeed(&first_start, another_result, BudgetUsage::zero()),
            Err(NodeAttemptError::ResultFenceMismatch)
        ));

        let head = journal(&activation, 3);
        let ambiguous = Failure::new(
            id(0xf1),
            FailureCategory::AmbiguousExternalOutcome,
            FailureCode::new("node.external_unknown").unwrap(),
            FailureOrigin::new("graph.node").unwrap(),
            FailureMessage::new("External outcome requires reconciliation").unwrap(),
            RetryAdvice::ReconcileFirst,
        )
        .unwrap()
        .with_caused_by_event(head.event_id());
        assert!(matches!(
            NodeAttemptCompletion::fail(&first_start, ambiguous, BudgetUsage::zero(), head),
            Err(NodeAttemptError::ReconciliationUnsupported)
        ));

        let wrong_cause = failure(id(0xee), 0xf2, RetryAdvice::Never);
        assert!(matches!(
            NodeAttemptCompletion::fail(
                &first_start,
                wrong_cause,
                BudgetUsage::zero(),
                journal(&activation, 3),
            ),
            Err(NodeAttemptError::FailureEventMismatch)
        ));
    }

    #[test]
    fn history_enforces_safe_retry_and_fenced_crash_recovery() {
        let activation = activation("authorize");
        let first_start = start(&activation, 0xa1, 0xb1, 1, 2);
        let failed_head = journal(&activation, 3);
        let failed = NodeAttempt::restore(
            first_start.clone(),
            Some(
                NodeAttemptCompletion::fail(
                    &first_start,
                    failure(
                        failed_head.event_id(),
                        0xf1,
                        RetryAdvice::SafeAfter {
                            delay: DurationMillis::new(2_000).unwrap(),
                        },
                    ),
                    BudgetUsage::zero(),
                    failed_head,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let too_early = NodeAttempt::executing(start(&activation, 0xa2, 0xb1, 1, 4));
        let allowed = NodeAttempt::executing(start(&activation, 0xa3, 0xb1, 1, 5));

        let mut verifier = NodeAttemptHistoryVerifier::new();
        verifier.verify_next(&failed).unwrap();
        assert!(matches!(
            verifier.verify_next(&too_early),
            Err(NodeAttemptHistoryError::RetryDelayNotElapsed { .. })
        ));
        verifier.verify_next(&allowed).unwrap();

        let crashed = NodeAttempt::executing(start(&activation, 0xa4, 0xb2, 2, 6));
        let same_fence_retry = NodeAttempt::executing(start(&activation, 0xa5, 0xb2, 2, 7));
        let takeover = NodeAttempt::executing(start(&activation, 0xa6, 0xb3, 3, 7));
        let mut recovery = NodeAttemptHistoryVerifier::new();
        recovery.verify_next(&crashed).unwrap();
        assert_eq!(
            recovery.verify_next(&same_fence_retry),
            Err(NodeAttemptHistoryError::UnfinishedAttemptNotSuperseded)
        );
        recovery.verify_next(&takeover).unwrap();
    }

    #[test]
    fn success_and_non_retryable_failure_are_absorbing() {
        let activation = activation("authorize");
        let success_start = start(&activation, 0xa1, 0xb1, 1, 2);
        let success = NodeAttempt::restore(
            success_start.clone(),
            Some(
                NodeAttemptCompletion::succeed(
                    &success_start,
                    pending_result(&success_start, 3),
                    BudgetUsage::zero(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let successor = NodeAttempt::executing(start(&activation, 0xa2, 0xb2, 2, 4));
        let mut verifier = NodeAttemptHistoryVerifier::new();
        verifier.verify_next(&success).unwrap();
        assert_eq!(
            verifier.verify_next(&successor),
            Err(NodeAttemptHistoryError::PreviousAttemptSucceeded)
        );

        let failed_start = start(&activation, 0xa3, 0xb3, 3, 5);
        let failed_head = journal(&activation, 6);
        let failed = NodeAttempt::restore(
            failed_start.clone(),
            Some(
                NodeAttemptCompletion::fail(
                    &failed_start,
                    failure(failed_head.event_id(), 0xf2, RetryAdvice::Never),
                    BudgetUsage::zero(),
                    failed_head,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let mut verifier = NodeAttemptHistoryVerifier::new();
        verifier.verify_next(&failed).unwrap();
        assert_eq!(
            verifier.verify_next(&NodeAttempt::executing(start(
                &activation,
                0xa4,
                0xb4,
                4,
                7,
            ))),
            Err(NodeAttemptHistoryError::RetryNotAuthorized {
                advice: RetryAdvice::Never,
            })
        );
    }

    #[test]
    fn diagnostics_redact_failure_messages() {
        let activation = activation("authorize");
        let start = start(&activation, 0xa1, 0xb1, 1, 2);
        let head = journal(&activation, 3);
        let completion = NodeAttemptCompletion::fail(
            &start,
            failure(head.event_id(), 0xf1, RetryAdvice::Never),
            BudgetUsage::zero(),
            head,
        )
        .unwrap();
        let debug = format!("{completion:?}");
        assert!(!debug.contains("Node execution failed safely"));
        assert!(debug.contains("node.failed"));
    }
}
