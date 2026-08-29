// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use stateknot_core::{
    Checkpoint, CheckpointHead, CheckpointId, DeliveryFence, Digest, DurableTimer,
    DurableTimerRecord, DurableWait, FencingEpoch, InterruptRecord, InterruptRequest, JournalEvent,
    JournalExpectation, JournalHead, JournalPayload, ModelInvocation, NodeAttempt, OutboxAttempt,
    OutboxAttemptCompletion, OutboxAttemptStart, OutboxDelivery, OutboxDestinationRef,
    PendingNodeResult, PendingNodeResultHead, QuarantineId, RunId, RunLease, RunLifecycle,
    RunRevision, RunTransition, Superstep, TenantId, Timestamp, ToolInvocation,
};

use crate::StoreError;

/// Closed reason taxonomy for stopping all execution of one durable run.
///
/// The caller stores only a stable component code and an integrity digest; raw
/// payloads, credentials, SQL, and private error text are intentionally outside
/// this record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunQuarantineCause {
    /// Canonical bytes, digests, links, or immutable history failed validation.
    IntegrityFailure,
    /// A required durable schema or graph format is unsupported by this binary.
    UnsupportedSchema,
    /// Required integrity-bound external storage evidence is unavailable.
    MissingArtifact,
    /// A durable reference escaped its tenant boundary.
    CrossTenantReference,
    /// Authoritative history and a mutable projection disagree.
    ProjectionMismatch,
    /// No higher safe fencing epoch can be issued.
    FencingEpochExhausted,
    /// A trusted operator policy explicitly prohibited further execution.
    OperatorPolicy,
}

impl RunQuarantineCause {
    /// Returns the stable storage and metrics code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IntegrityFailure => "integrity_failure",
            Self::UnsupportedSchema => "unsupported_schema",
            Self::MissingArtifact => "missing_artifact",
            Self::CrossTenantReference => "cross_tenant_reference",
            Self::ProjectionMismatch => "projection_mismatch",
            Self::FencingEpochExhausted => "fencing_epoch_exhausted",
            Self::OperatorPolicy => "operator_policy",
        }
    }
}

/// Bounded non-secret machine code identifying the failed recovery component.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RunQuarantineComponent(Box<str>);

impl RunQuarantineComponent {
    /// Maximum encoded component-code length.
    pub const MAX_LEN: usize = 128;

    /// Validates a lowercase ASCII component code.
    ///
    /// Codes use `a-z`, `0-9`, `.`, `_`, `:`, and `-`. They are suitable for
    /// metrics and audit routing, not for user-controlled diagnostic text.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRunQuarantineComponent`] when the code is
    /// empty, oversized, or outside the documented grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, StoreError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > Self::MAX_LEN
            || value.bytes().any(|byte| {
                !byte.is_ascii_lowercase()
                    && !byte.is_ascii_digit()
                    && !matches!(byte, b'.' | b'_' | b':' | b'-')
            })
        {
            return Err(StoreError::InvalidRunQuarantineComponent);
        }
        Ok(Self(value.into_boxed_str()))
    }

    /// Derives a stable `store.*` component from a payload-redacted corruption.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRunQuarantineRequest`] when `error` is not
    /// [`StoreError::CorruptData`], or
    /// [`StoreError::InvalidRunQuarantineComponent`] if a future record category
    /// cannot fit the bounded component grammar.
    pub fn from_corrupt_store_error(error: &StoreError) -> Result<Self, StoreError> {
        let record = error
            .corrupt_record()
            .ok_or(StoreError::InvalidRunQuarantineRequest)?;
        let mut component = String::with_capacity("store.".len() + record.len());
        component.push_str("store.");
        let mut previous_separator = false;
        for byte in record.bytes() {
            let normalized = if byte.is_ascii_alphanumeric() {
                byte.to_ascii_lowercase()
            } else if matches!(byte, b'.' | b':' | b'-') {
                byte
            } else {
                b'_'
            };
            if normalized == b'_' {
                if previous_separator {
                    continue;
                }
                previous_separator = true;
            } else {
                previous_separator = false;
            }
            component.push(char::from(normalized));
        }
        while component.ends_with('_') {
            component.pop();
        }
        Self::new(component)
    }

    /// Returns the stable component code.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Immutable idempotency intent for quarantining one tenant-scoped run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunQuarantineRequest {
    pub(crate) tenant_id: TenantId,
    pub(crate) run_id: RunId,
    pub(crate) quarantine_id: QuarantineId,
    pub(crate) expectation: JournalExpectation,
    pub(crate) cause: RunQuarantineCause,
    pub(crate) component: RunQuarantineComponent,
    pub(crate) evidence_digest: Digest,
}

impl RunQuarantineRequest {
    /// Constructs an exact, tenant-bound quarantine observation intent.
    ///
    /// `evidence_digest` identifies a caller-retained, redacted evidence bundle;
    /// evidence bytes are deliberately not copied into the operational store.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRunQuarantineRequest`] when an exact journal
    /// expectation belongs to another tenant or run.
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        quarantine_id: QuarantineId,
        expectation: JournalExpectation,
        cause: RunQuarantineCause,
        component: RunQuarantineComponent,
        evidence_digest: Digest,
    ) -> Result<Self, StoreError> {
        if expectation
            .head()
            .is_some_and(|head| head.tenant_id() != &tenant_id || head.run_id() != run_id)
        {
            return Err(StoreError::InvalidRunQuarantineRequest);
        }
        Ok(Self {
            tenant_id,
            run_id,
            quarantine_id,
            expectation,
            cause,
            component,
            evidence_digest,
        })
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the quarantined run identity.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the stable lost-acknowledgement identity.
    #[must_use]
    pub const fn quarantine_id(&self) -> QuarantineId {
        self.quarantine_id
    }

    /// Returns the exact journal observation that this evidence describes.
    #[must_use]
    pub const fn expectation(&self) -> &JournalExpectation {
        &self.expectation
    }

    /// Returns the closed quarantine cause.
    #[must_use]
    pub const fn cause(&self) -> RunQuarantineCause {
        self.cause
    }

    /// Returns the non-secret recovery component code.
    #[must_use]
    pub const fn component(&self) -> &RunQuarantineComponent {
        &self.component
    }

    /// Returns the caller-retained evidence checksum.
    #[must_use]
    pub const fn evidence_digest(&self) -> Digest {
        self.evidence_digest
    }
}

/// Fully verified immutable quarantine observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunQuarantine {
    pub(crate) request: RunQuarantineRequest,
    pub(crate) quarantined_at: Timestamp,
    pub(crate) digest: Digest,
}

impl RunQuarantine {
    /// Returns the immutable quarantine request.
    #[must_use]
    pub const fn request(&self) -> &RunQuarantineRequest {
        &self.request
    }

    /// Returns the database-clock observation that removed the run from execution.
    #[must_use]
    pub const fn quarantined_at(&self) -> Timestamp {
        self.quarantined_at
    }

    /// Returns the canonical observation checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// Result of atomically recording evidence and removing a run from execution.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RunQuarantineCommitOutcome {
    /// New immutable evidence and the run quarantine projection committed.
    Committed(RunQuarantine),
    /// The exact stable request had already committed.
    Idempotent(RunQuarantine),
}

impl RunQuarantineCommitOutcome {
    /// Returns the fully verified durable quarantine observation.
    #[must_use]
    pub const fn quarantine(&self) -> &RunQuarantine {
        match self {
            Self::Committed(quarantine) | Self::Idempotent(quarantine) => quarantine,
        }
    }
}

/// Result of idempotent run admission.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AdmissionOutcome {
    /// A new pending run row committed.
    Committed(RunLifecycle),
    /// The same provenance was already admitted; the current snapshot is returned.
    Idempotent(RunLifecycle),
}

impl AdmissionOutcome {
    /// Returns the validated lifecycle snapshot.
    #[must_use]
    pub const fn lifecycle(&self) -> &RunLifecycle {
        match self {
            Self::Committed(lifecycle) | Self::Idempotent(lifecycle) => lifecycle,
        }
    }
}

/// Result of an idempotent journal append.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AppendOutcome {
    /// A new event and projection committed atomically.
    Committed(JournalEvent),
    /// The identical event intent was already committed.
    Idempotent(JournalEvent),
}

/// Result of atomically committing a journal event and initial graph
/// checkpoint.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum CheckpointCommitOutcome {
    /// A new event, checkpoint, run head, and optional lifecycle projection committed.
    Committed {
        /// Newly committed journal event.
        event: JournalEvent,
        /// Newly committed immutable checkpoint.
        checkpoint: Checkpoint,
    },
    /// The exact event, projection, and checkpoint intent had already committed.
    Idempotent {
        /// Previously committed journal event.
        event: JournalEvent,
        /// Previously committed immutable checkpoint.
        checkpoint: Checkpoint,
    },
}

impl CheckpointCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        match self {
            Self::Committed { checkpoint, .. } | Self::Idempotent { checkpoint, .. } => checkpoint,
        }
    }
}

/// Result of atomically entering `waiting` with a checkpoint and complete
/// durable interrupt/timer registration set.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum WaitCheckpointCommitOutcome {
    /// The event, checkpoint, lifecycle projection, and registrations committed.
    Committed {
        /// Newly committed anchoring journal event.
        event: JournalEvent,
        /// Newly committed immutable checkpoint.
        checkpoint: Checkpoint,
        /// Complete materialized registration batch in semantic order.
        waits: Vec<DurableWait>,
    },
    /// The exact complete transaction had already committed.
    Idempotent {
        /// Previously committed anchoring journal event.
        event: JournalEvent,
        /// Previously committed immutable checkpoint.
        checkpoint: Checkpoint,
        /// Complete verified registration batch in semantic order.
        waits: Vec<DurableWait>,
    },
}

impl WaitCheckpointCommitOutcome {
    /// Returns the exact anchoring event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the immutable checkpoint committed at the wait barrier.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        match self {
            Self::Committed { checkpoint, .. } | Self::Idempotent { checkpoint, .. } => checkpoint,
        }
    }

    /// Returns every durable registration in lifecycle semantic order.
    #[must_use]
    pub fn waits(&self) -> &[DurableWait] {
        match self {
            Self::Committed { waits, .. } | Self::Idempotent { waits, .. } => waits,
        }
    }
}

/// Result of atomically resolving one exact durable interrupt.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum InterruptResolutionCommitOutcome {
    /// The event, resolution, lifecycle, and wait projection committed.
    Committed {
        /// Newly committed resolution event.
        event: JournalEvent,
        /// Complete immutable interrupt history.
        record: InterruptRecord,
    },
    /// The exact complete resolution transaction had already committed.
    Idempotent {
        /// Previously committed resolution event.
        event: JournalEvent,
        /// Complete verified interrupt history.
        record: InterruptRecord,
    },
}

impl InterruptResolutionCommitOutcome {
    /// Returns the exact resolution event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the complete interrupt history.
    #[must_use]
    pub const fn record(&self) -> &InterruptRecord {
        match self {
            Self::Committed { record, .. } | Self::Idempotent { record, .. } => record,
        }
    }
}

/// Result of atomically firing one exact due durable timer.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum TimerFiringCommitOutcome {
    /// The event, firing, lifecycle, and wait projection committed.
    Committed {
        /// Newly committed firing event.
        event: JournalEvent,
        /// Complete immutable timer history.
        record: DurableTimerRecord,
    },
    /// The exact complete firing transaction had already committed.
    Idempotent {
        /// Previously committed firing event.
        event: JournalEvent,
        /// Complete verified timer history.
        record: DurableTimerRecord,
    },
}

impl TimerFiringCommitOutcome {
    /// Returns the exact firing event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the complete timer history.
    #[must_use]
    pub const fn record(&self) -> &DurableTimerRecord {
        match self {
            Self::Committed { record, .. } | Self::Idempotent { record, .. } => record,
        }
    }
}

/// Durable reason an outstanding wait was abandoned without resolution/firing.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WaitAbandonmentReason {
    /// The run entered cooperative cancellation.
    RunCancellation,
    /// The run committed a terminal non-cancellation failure.
    RunFailure,
}

/// One integrity-bound audit fact for a wait abandoned by a run-level edge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitAbandonment {
    pub(crate) wait: DurableWait,
    pub(crate) reason: WaitAbandonmentReason,
    pub(crate) journal: JournalHead,
    pub(crate) digest: Digest,
}

impl WaitAbandonment {
    /// Returns the immutable registration that was abandoned.
    #[must_use]
    pub const fn wait(&self) -> &DurableWait {
        &self.wait
    }

    /// Returns the run-level reason for abandonment.
    #[must_use]
    pub const fn reason(&self) -> WaitAbandonmentReason {
        self.reason
    }

    /// Returns the exact journal event that abandoned the wait.
    #[must_use]
    pub const fn journal(&self) -> &JournalHead {
        &self.journal
    }

    /// Returns the provider-domain integrity checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// Result of atomically cancelling/failing a waiting run and abandoning its
/// complete outstanding wait set.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum WaitAbandonmentCommitOutcome {
    /// The event, lifecycle edge, and every abandonment fact committed.
    Committed {
        /// Newly committed run-level event.
        event: JournalEvent,
        /// Complete abandoned set in deterministic registration identity order.
        abandonments: Vec<WaitAbandonment>,
    },
    /// The exact complete abandonment transaction had already committed.
    Idempotent {
        /// Previously committed run-level event.
        event: JournalEvent,
        /// Complete verified abandoned set.
        abandonments: Vec<WaitAbandonment>,
    },
}

impl WaitAbandonmentCommitOutcome {
    /// Returns the exact run-level event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns every abandonment fact.
    #[must_use]
    pub fn abandonments(&self) -> &[WaitAbandonment] {
        match self {
            Self::Committed { abandonments, .. } | Self::Idempotent { abandonments, .. } => {
                abandonments
            }
        }
    }
}

/// Result of atomically consuming one complete result barrier and committing
/// its anchoring event and successor checkpoint.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum BarrierCommitOutcome {
    /// The event, successor checkpoint, and every result-consumption record
    /// committed in one transaction.
    Committed {
        /// Newly committed anchoring journal event.
        event: JournalEvent,
        /// Newly committed immutable successor checkpoint.
        checkpoint: Checkpoint,
    },
    /// The exact barrier transaction had already committed.
    Idempotent {
        /// Previously committed anchoring journal event.
        event: JournalEvent,
        /// Previously committed immutable successor checkpoint.
        checkpoint: Checkpoint,
    },
}

impl BarrierCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable successor checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        match self {
            Self::Committed { checkpoint, .. } | Self::Idempotent { checkpoint, .. } => checkpoint,
        }
    }
}

/// Result of atomically committing a journal event and tool invocation revision.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ToolInvocationCommitOutcome {
    /// A new event and immutable invocation revision committed atomically.
    Committed {
        /// Newly committed journal event.
        event: JournalEvent,
        /// Newly committed invocation revision.
        invocation: ToolInvocation,
    },
    /// The exact event and invocation mutation had already committed.
    Idempotent {
        /// Previously committed journal event.
        event: JournalEvent,
        /// Previously committed invocation revision.
        invocation: ToolInvocation,
    },
}

impl ToolInvocationCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable invocation revision.
    #[must_use]
    pub const fn invocation(&self) -> &ToolInvocation {
        match self {
            Self::Committed { invocation, .. } | Self::Idempotent { invocation, .. } => invocation,
        }
    }
}

/// Result of atomically committing a journal event and model invocation revision.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ModelInvocationCommitOutcome {
    /// A new event and immutable invocation revision committed atomically.
    Committed {
        /// Newly committed journal event.
        event: JournalEvent,
        /// Newly committed invocation revision.
        invocation: ModelInvocation,
    },
    /// The exact event and invocation mutation had already committed.
    Idempotent {
        /// Previously committed journal event.
        event: JournalEvent,
        /// Previously committed invocation revision.
        invocation: ModelInvocation,
    },
}

impl ModelInvocationCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable invocation revision.
    #[must_use]
    pub const fn invocation(&self) -> &ModelInvocation {
        match self {
            Self::Committed { invocation, .. } | Self::Idempotent { invocation, .. } => invocation,
        }
    }
}

/// Legacy outcome shape for the pre-v6 direct pending-result API.
///
/// New writes return [`NodeAttemptCommitOutcome`] because every result must be
/// owned by a durable physical node attempt. This type remains public so the
/// fail-closed compatibility method can preserve its original signature.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum PendingNodeResultCommitOutcome {
    /// A new event and immutable pending result committed atomically.
    Committed {
        /// Newly committed journal event.
        event: JournalEvent,
        /// Newly committed pending node result.
        result: PendingNodeResult,
    },
    /// The same semantic node result had already committed.
    Idempotent {
        /// Original journal event that anchored the stored winner.
        event: JournalEvent,
        /// Previously committed pending node result.
        result: PendingNodeResult,
    },
}

impl PendingNodeResultCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the validated immutable pending result.
    #[must_use]
    pub const fn result(&self) -> &PendingNodeResult {
        match self {
            Self::Committed { result, .. } | Self::Idempotent { result, .. } => result,
        }
    }
}

/// Result of atomically committing a node-attempt start or completion event.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum NodeAttemptCommitOutcome {
    /// A new event and immutable node-attempt record committed atomically.
    Committed {
        /// Newly committed anchoring journal event.
        event: JournalEvent,
        /// Fully restored attempt after this mutation.
        attempt: NodeAttempt,
    },
    /// The exact start or completion had already committed.
    Idempotent {
        /// Original journal event that anchored the stored mutation.
        event: JournalEvent,
        /// Fully restored durable attempt.
        attempt: NodeAttempt,
    },
}

impl NodeAttemptCommitOutcome {
    /// Returns the validated anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the fully restored physical attempt.
    #[must_use]
    pub const fn attempt(&self) -> &NodeAttempt {
        match self {
            Self::Committed { attempt, .. } | Self::Idempotent { attempt, .. } => attempt,
        }
    }
}

/// Fully validated immutable destination snapshot used by dispatch adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredOutboxDestination {
    pub(crate) destination: OutboxDestinationRef,
    pub(crate) config: JournalPayload,
    pub(crate) created_at: Timestamp,
}

impl StoredOutboxDestination {
    /// Returns the immutable destination identity and snapshot checksum.
    #[must_use]
    pub const fn destination(&self) -> &OutboxDestinationRef {
        &self.destination
    }

    /// Returns the canonical schema-pinned non-secret routing configuration.
    #[must_use]
    pub const fn config(&self) -> &JournalPayload {
        &self.config
    }

    /// Returns the database registration observation.
    #[must_use]
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }
}

/// Result of idempotently registering one immutable destination snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OutboxDestinationRegistrationOutcome {
    /// A new immutable destination snapshot committed.
    Registered(StoredOutboxDestination),
    /// The exact snapshot was already registered.
    Idempotent(StoredOutboxDestination),
}

impl OutboxDestinationRegistrationOutcome {
    /// Returns the validated immutable destination snapshot.
    #[must_use]
    pub const fn destination(&self) -> &StoredOutboxDestination {
        match self {
            Self::Registered(destination) | Self::Idempotent(destination) => destination,
        }
    }
}

/// Result of atomically appending one journal fact and its exact outbox set.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum OutboxEnqueueOutcome {
    /// The event and every requested delivery committed in one transaction.
    Committed {
        /// Newly committed journal event.
        event: JournalEvent,
        /// Immutable deliveries in caller order.
        deliveries: Vec<OutboxDelivery>,
    },
    /// The exact event and complete delivery set had already committed.
    Idempotent {
        /// Previously committed journal event.
        event: JournalEvent,
        /// Fully validated durable deliveries in caller order.
        deliveries: Vec<OutboxDelivery>,
    },
}

impl OutboxEnqueueOutcome {
    /// Returns the atomically anchoring journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed { event, .. } | Self::Idempotent { event, .. } => event,
        }
    }

    /// Returns the complete immutable delivery set.
    #[must_use]
    pub fn deliveries(&self) -> &[OutboxDelivery] {
        match self {
            Self::Committed { deliveries, .. } | Self::Idempotent { deliveries, .. } => deliveries,
        }
    }
}

/// One atomically claimed delivery and its durable-before-dispatch start.
#[derive(Clone, Debug)]
pub struct OutboxClaim {
    pub(crate) destination: StoredOutboxDestination,
    pub(crate) delivery: OutboxDelivery,
    pub(crate) start: OutboxAttemptStart,
}

impl OutboxClaim {
    /// Returns the pinned destination snapshot needed by the adapter.
    #[must_use]
    pub const fn destination(&self) -> &StoredOutboxDestination {
        &self.destination
    }

    /// Returns the immutable delivery and protocol payload.
    #[must_use]
    pub const fn delivery(&self) -> &OutboxDelivery {
        &self.delivery
    }

    /// Returns the durable attempt start that must precede network I/O.
    #[must_use]
    pub const fn start(&self) -> &OutboxAttemptStart {
        &self.start
    }

    /// Returns the exact completion fence.
    #[must_use]
    pub const fn fence(&self) -> &DeliveryFence {
        self.start.fence()
    }
}

/// Result of an atomic tenant queue claim with a stable physical attempt ID.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum OutboxClaimOutcome {
    /// New ownership committed before dispatch may begin.
    Claimed(OutboxClaim),
    /// The same attempt ID had already claimed this exact delivery.
    Idempotent(OutboxClaim),
    /// No eligible unlocked delivery was visible at the database observation.
    NoWork,
}

impl OutboxClaimOutcome {
    /// Returns the claimed work, if any.
    #[must_use]
    pub const fn claim(&self) -> Option<&OutboxClaim> {
        match self {
            Self::Claimed(claim) | Self::Idempotent(claim) => Some(claim),
            Self::NoWork => None,
        }
    }
}

/// Result of idempotently committing one delivery-attempt completion.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum OutboxCompletionOutcome {
    /// A new immutable completion and projection update committed.
    Committed {
        /// Fully restored terminal physical attempt.
        attempt: OutboxAttempt,
    },
    /// The exact semantic completion had already committed.
    Idempotent {
        /// Fully restored terminal physical attempt.
        attempt: OutboxAttempt,
    },
}

impl OutboxCompletionOutcome {
    /// Returns the fully restored completed attempt.
    #[must_use]
    pub const fn attempt(&self) -> &OutboxAttempt {
        match self {
            Self::Committed { attempt } | Self::Idempotent { attempt } => attempt,
        }
    }

    /// Returns the immutable completion.
    #[must_use]
    pub const fn completion(&self) -> Option<&OutboxAttemptCompletion> {
        self.attempt().completion()
    }
}

/// Hard-bounded number of outbox attempts returned in one history page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboxAttemptHistoryPageSize(u8);

impl OutboxAttemptHistoryPageSize {
    /// Largest decoded page accepted by the provider.
    pub const MAX: u8 = 16;

    /// Constructs a positive bounded page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidOutboxAttemptPageSize`] outside `1..=16`.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidOutboxAttemptPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One bounded, fully verified ascending outbox-attempt history page.
#[derive(Clone, Debug)]
pub struct OutboxAttemptHistoryPage {
    pub(crate) records: Vec<OutboxAttempt>,
    pub(crate) has_more: bool,
}

impl OutboxAttemptHistoryPage {
    /// Returns immutable attempts in ascending fencing-epoch order.
    #[must_use]
    pub fn records(&self) -> &[OutboxAttempt] {
        &self.records
    }

    /// Returns whether a later attempt remains.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the exact next-page cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<OutboxAttempt> {
        self.records.last().cloned()
    }
}

/// Hard-bounded number of physical node attempts in one history page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeAttemptHistoryPageSize(u8);

impl NodeAttemptHistoryPageSize {
    /// Largest page accepted by the provider.
    ///
    /// A start and completion can occupy about 17 MiB together, so two
    /// records cap provider-owned decoded page memory near 34 MiB before
    /// driver overhead.
    pub const MAX: u8 = 2;

    /// Constructs a positive bounded history page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidNodeAttemptPageSize`] for zero or values
    /// above two.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidNodeAttemptPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One bounded, fully verified ascending physical-attempt history page.
#[derive(Clone, Debug)]
pub struct NodeAttemptHistoryPage {
    pub(crate) records: Vec<NodeAttempt>,
    pub(crate) has_more: bool,
}

impl NodeAttemptHistoryPage {
    /// Returns immutable physical attempts in ascending start order.
    #[must_use]
    pub fn records(&self) -> &[NodeAttempt] {
        &self.records
    }

    /// Returns whether a later physical attempt remains in the snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the full last attempt required as the exact next-page cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<NodeAttempt> {
        self.records.last().cloned()
    }
}

/// Hard-bounded number of fully decoded pending results in one recovery page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PendingNodeResultPageSize(u8);

impl PendingNodeResultPageSize {
    /// Largest page accepted by the provider.
    ///
    /// One canonical pending result may occupy 16 MiB and can reference large
    /// model records that are verified in bounded sub-batches. Two retained
    /// results cap provider-owned decoded page memory near 32 MiB before driver
    /// and invocation-verification overhead. Look-ahead rows remain compact.
    pub const MAX: u8 = 2;

    /// Constructs a positive bounded pending-result page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidPendingNodeResultPageSize`] for zero or
    /// values above two.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidPendingNodeResultPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// Exact stable-snapshot continuation for unconsumed pending-result scanning.
///
/// The cursor binds the immutable base checkpoint, the run journal head seen
/// by the first page, and the last fully verified result. A later result commit
/// changes the run journal head, making continuation fail explicitly instead
/// of silently skipping a newly inserted lower sort key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingNodeResultPageCursor {
    pub(crate) base_checkpoint: CheckpointHead,
    pub(crate) snapshot_journal_head: JournalHead,
    pub(crate) after: PendingNodeResultHead,
}

impl PendingNodeResultPageCursor {
    /// Returns the exact checkpoint whose unconsumed set is being scanned.
    #[must_use]
    pub const fn base_checkpoint(&self) -> &CheckpointHead {
        &self.base_checkpoint
    }

    /// Returns the run head that must remain unchanged across page calls.
    #[must_use]
    pub const fn snapshot_journal_head(&self) -> &JournalHead {
        &self.snapshot_journal_head
    }

    /// Returns the last fully verified result from the preceding page.
    #[must_use]
    pub const fn after(&self) -> &PendingNodeResultHead {
        &self.after
    }
}

/// One bounded, fully verified page of unconsumed pending node results.
#[derive(Clone, Debug)]
pub struct PendingNodeResultPage {
    pub(crate) base_checkpoint: CheckpointHead,
    pub(crate) snapshot_journal_head: JournalHead,
    pub(crate) records: Vec<PendingNodeResult>,
    pub(crate) has_more: bool,
}

impl PendingNodeResultPage {
    /// Returns the exact base checkpoint observed by this page.
    #[must_use]
    pub const fn base_checkpoint(&self) -> &CheckpointHead {
        &self.base_checkpoint
    }

    /// Returns the stable run journal head shared by every page continuation.
    #[must_use]
    pub const fn snapshot_journal_head(&self) -> &JournalHead {
        &self.snapshot_journal_head
    }

    /// Returns immutable pending results in canonical activation order.
    #[must_use]
    pub fn records(&self) -> &[PendingNodeResult] {
        &self.records
    }

    /// Returns whether a later result remains in the observed snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the exact continuation required for the next page.
    #[must_use]
    pub fn next_cursor(&self) -> Option<PendingNodeResultPageCursor> {
        self.records
            .last()
            .map(|result| PendingNodeResultPageCursor {
                base_checkpoint: self.base_checkpoint.clone(),
                snapshot_journal_head: self.snapshot_journal_head.clone(),
                after: result.head(),
            })
    }
}

/// Hard-bounded number of model invocation revisions in one history page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ModelInvocationHistoryPageSize(u8);

impl ModelInvocationHistoryPageSize {
    /// Largest page accepted by the provider.
    ///
    /// A compact revision and its separately loaded intent may each occupy up
    /// to 128 MiB, so the provider admits only one revision per decoded page.
    pub const MAX: u8 = 1;

    /// Constructs a positive bounded history page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidModelInvocationPageSize`] unless `value` is one.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value != Self::MAX {
            return Err(StoreError::InvalidModelInvocationPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One bounded, fully verified ascending model-invocation history page.
#[derive(Clone, Debug)]
pub struct ModelInvocationHistoryPage {
    pub(crate) records: Vec<ModelInvocation>,
    pub(crate) has_more: bool,
}

impl ModelInvocationHistoryPage {
    /// Returns immutable revisions in ascending order.
    #[must_use]
    pub fn records(&self) -> &[ModelInvocation] {
        &self.records
    }

    /// Returns whether a later revision remains in the observed snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the full last record required as the exact next-page cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<ModelInvocation> {
        self.records.last().cloned()
    }
}

/// Hard-bounded number of invocation revisions returned in one history page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ToolInvocationHistoryPageSize(u8);

impl ToolInvocationHistoryPageSize {
    /// Largest page accepted by the provider.
    ///
    /// One canonical record may occupy up to 16 MiB, so two records cap the
    /// provider-owned decoded page near 32 MiB before driver overhead.
    pub const MAX: u8 = 2;

    /// Constructs a positive bounded history page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidToolInvocationPageSize`] for zero or values
    /// above two.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidToolInvocationPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One bounded, fully verified ascending invocation-history page.
#[derive(Clone, Debug)]
pub struct ToolInvocationHistoryPage {
    pub(crate) records: Vec<ToolInvocation>,
    pub(crate) has_more: bool,
}

impl ToolInvocationHistoryPage {
    /// Returns immutable revisions in ascending order.
    #[must_use]
    pub fn records(&self) -> &[ToolInvocation] {
        &self.records
    }

    /// Returns whether a later revision remains in the observed snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the full last record required as the exact next-page cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<ToolInvocation> {
        self.records.last().cloned()
    }
}

impl AppendOutcome {
    /// Returns the validated committed event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        match self {
            Self::Committed(event) | Self::Idempotent(event) => event,
        }
    }
}

/// Lifecycle projection to commit atomically with one journal append.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum RunProjection {
    /// Preserve the current lifecycle bytes and revision.
    Unchanged,
    /// Apply one pure lifecycle transition after matching the exact revision.
    Transition {
        /// Revision from which the pure lifecycle transition was derived.
        expected_revision: RunRevision,
        /// Transition to apply to the locked durable lifecycle.
        transition: RunTransition,
    },
}

/// Compact current-checkpoint pointer projected into the locked run row.
///
/// The full checkpoint must still be loaded and validated before recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointPointer {
    pub(crate) checkpoint_id: CheckpointId,
    pub(crate) superstep: Superstep,
    pub(crate) digest: Digest,
}

impl CheckpointPointer {
    /// Returns the immutable checkpoint identity.
    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    /// Returns the current committed barrier position.
    #[must_use]
    pub const fn superstep(&self) -> Superstep {
        self.superstep
    }

    /// Returns the complete checkpoint checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// Hard-bounded number of checkpoints returned in one lineage page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CheckpointLineagePageSize(u8);

impl CheckpointLineagePageSize {
    /// Largest page accepted by the provider.
    ///
    /// One checkpoint envelope may occupy roughly 2.5 MiB, so this deliberately
    /// remains much smaller than [`JournalPageSize::MAX`]. The provider may
    /// decode one additional parent as a fail-closed lineage look-ahead.
    pub const MAX: u8 = 8;

    /// Constructs a positive bounded checkpoint-lineage page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidCheckpointPageSize`] for zero or values
    /// above eight.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidCheckpointPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One bounded, fully verified reverse checkpoint-lineage page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointLineagePage {
    pub(crate) checkpoints: Vec<Checkpoint>,
    pub(crate) next_cursor: Option<CheckpointHead>,
}

impl CheckpointLineagePage {
    /// Returns checkpoints in newest-to-oldest lineage order.
    #[must_use]
    pub fn checkpoints(&self) -> &[Checkpoint] {
        &self.checkpoints
    }

    /// Returns whether older ancestors remain before the superstep-zero root.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.next_cursor.is_some()
    }

    /// Returns the exact parent head at which the next reverse page must start.
    #[must_use]
    pub fn next_cursor(&self) -> Option<CheckpointHead> {
        self.next_cursor.clone()
    }
}

impl RunProjection {
    /// Constructs a no-op lifecycle projection.
    #[must_use]
    pub const fn unchanged() -> Self {
        Self::Unchanged
    }

    /// Constructs an exact-revision pure lifecycle transition.
    #[must_use]
    pub const fn transition(expected_revision: RunRevision, transition: RunTransition) -> Self {
        Self::Transition {
            expected_revision,
            transition,
        }
    }
}

/// Hard-bounded size for tenant-level timer/interrupt discovery pages.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WaitDiscoveryPageSize(u8);

impl WaitDiscoveryPageSize {
    /// Largest page accepted by the provider.
    pub const MAX: u8 = 16;

    /// Constructs a positive bounded discovery page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidWaitDiscoveryPageSize`] outside `1..=16`.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidWaitDiscoveryPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// Opaque continuation for one fixed-cutoff due-timer scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DueTimerPageCursor {
    pub(crate) tenant_id: stateknot_core::TenantId,
    pub(crate) snapshot_at: Timestamp,
    pub(crate) due_at: Timestamp,
    pub(crate) run_id: stateknot_core::RunId,
    pub(crate) timer_id: stateknot_core::TimerId,
}

/// One bounded tenant-level page of fully verified outstanding due timers.
#[derive(Clone, Debug)]
pub struct DueTimerPage {
    pub(crate) tenant_id: stateknot_core::TenantId,
    pub(crate) snapshot_at: Timestamp,
    pub(crate) records: Vec<DurableTimer>,
    pub(crate) has_more: bool,
}

impl DueTimerPage {
    /// Returns the database-time cutoff fixed for this page chain.
    #[must_use]
    pub const fn snapshot_at(&self) -> Timestamp {
        self.snapshot_at
    }

    /// Returns timers ordered by due time, run identity, then timer identity.
    #[must_use]
    pub fn records(&self) -> &[DurableTimer] {
        &self.records
    }

    /// Returns whether another matching row remained after this page.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the exact key required to continue the fixed-cutoff scan.
    #[must_use]
    pub fn next_cursor(&self) -> Option<DueTimerPageCursor> {
        self.records.last().map(|timer| DueTimerPageCursor {
            tenant_id: self.tenant_id.clone(),
            snapshot_at: self.snapshot_at,
            due_at: timer.marker().due_at(),
            run_id: timer.intent().run_id(),
            timer_id: timer.marker().timer_id(),
        })
    }
}

/// Opaque continuation for one fixed-cutoff expired-interrupt scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpiredInterruptPageCursor {
    pub(crate) tenant_id: stateknot_core::TenantId,
    pub(crate) snapshot_at: Timestamp,
    pub(crate) expires_at: Timestamp,
    pub(crate) run_id: stateknot_core::RunId,
    pub(crate) interrupt_id: stateknot_core::InterruptId,
}

/// One bounded tenant-level page of fully verified outstanding expired interrupts.
#[derive(Clone, Debug)]
pub struct ExpiredInterruptPage {
    pub(crate) tenant_id: stateknot_core::TenantId,
    pub(crate) snapshot_at: Timestamp,
    pub(crate) records: Vec<InterruptRequest>,
    pub(crate) has_more: bool,
}

impl ExpiredInterruptPage {
    /// Returns the database-time cutoff fixed for this page chain.
    #[must_use]
    pub const fn snapshot_at(&self) -> Timestamp {
        self.snapshot_at
    }

    /// Returns requests ordered by expiry, run identity, then interrupt identity.
    #[must_use]
    pub fn records(&self) -> &[InterruptRequest] {
        &self.records
    }

    /// Returns whether another matching row remained after this page.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the exact key required to continue the fixed-cutoff scan.
    #[must_use]
    pub fn next_cursor(&self) -> Option<ExpiredInterruptPageCursor> {
        let request = self.records.last()?;
        Some(ExpiredInterruptPageCursor {
            tenant_id: self.tenant_id.clone(),
            snapshot_at: self.snapshot_at,
            expires_at: request.marker().expires_at()?,
            run_id: request.intent().run_id(),
            interrupt_id: request.marker().interrupt_id(),
        })
    }
}

/// Validated durable snapshot of one tenant-scoped run row.
#[derive(Clone, Debug)]
pub struct StoredRun {
    pub(crate) lifecycle: RunLifecycle,
    pub(crate) journal_head: Option<JournalHead>,
    pub(crate) lease: Option<RunLease>,
    pub(crate) last_fencing_epoch: Option<FencingEpoch>,
    pub(crate) checkpoint: Option<CheckpointPointer>,
    pub(crate) scheduler_ready_at: Option<stateknot_core::Timestamp>,
    pub(crate) wait_set_digest: Option<Digest>,
    pub(crate) unresolved_wait_count: u8,
    pub(crate) next_timer_due_at: Option<Timestamp>,
    pub(crate) next_interrupt_expiry_at: Option<Timestamp>,
    pub(crate) quarantined: bool,
}

impl StoredRun {
    /// Returns current validated business lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> &RunLifecycle {
        &self.lifecycle
    }

    /// Returns the current exact journal head, if non-empty.
    #[must_use]
    pub const fn journal_head(&self) -> Option<&JournalHead> {
        self.journal_head.as_ref()
    }

    /// Returns the active lease, including an expired lease not yet reclaimed.
    #[must_use]
    pub const fn lease(&self) -> Option<&RunLease> {
        self.lease.as_ref()
    }

    /// Returns the last issued fencing epoch; an unleased new run has none.
    #[must_use]
    pub const fn last_fencing_epoch(&self) -> Option<FencingEpoch> {
        self.last_fencing_epoch
    }

    /// Returns the compact current-checkpoint pointer, if the graph has reached
    /// its first durable barrier.
    #[must_use]
    pub const fn checkpoint(&self) -> Option<&CheckpointPointer> {
        self.checkpoint.as_ref()
    }

    /// Returns when the run most recently entered the durable scheduler queue.
    ///
    /// Waiting and terminal runs have no readiness observation. A live or
    /// expired lease can delay actual availability beyond this instant.
    #[must_use]
    pub const fn scheduler_ready_at(&self) -> Option<stateknot_core::Timestamp> {
        self.scheduler_ready_at
    }

    /// Returns the integrity checksum of the current ordered outstanding wait set.
    #[must_use]
    pub const fn wait_set_digest(&self) -> Option<Digest> {
        self.wait_set_digest
    }

    /// Returns the number of outstanding interrupt/timer registrations.
    #[must_use]
    pub const fn unresolved_wait_count(&self) -> u8 {
        self.unresolved_wait_count
    }

    /// Returns the earliest outstanding timer due instant, when present.
    #[must_use]
    pub const fn next_timer_due_at(&self) -> Option<Timestamp> {
        self.next_timer_due_at
    }

    /// Returns the earliest finite outstanding interrupt expiry, when present.
    #[must_use]
    pub const fn next_interrupt_expiry_at(&self) -> Option<Timestamp> {
        self.next_interrupt_expiry_at
    }

    /// Returns whether integrity or operator policy quarantined the run.
    #[must_use]
    pub const fn is_quarantined(&self) -> bool {
        self.quarantined
    }
}

/// Hard-bounded number of fully decoded runnable runs in one scheduler page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RunnableRunPageSize(u8);

impl RunnableRunPageSize {
    /// Largest page accepted by the provider.
    ///
    /// One lifecycle envelope may occupy two MiB. Sixteen retained records plus
    /// one driver look-ahead row bound provider-owned page memory near 34 MiB.
    pub const MAX: u8 = 16;

    /// Constructs a positive bounded runnable-run page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRunnableRunPageSize`] for zero or values
    /// above sixteen.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidRunnableRunPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One fully validated run visible to the tenant-level scheduler scan.
#[derive(Clone, Debug)]
pub struct RunnableRunCandidate {
    pub(crate) run: StoredRun,
    pub(crate) ready_at: stateknot_core::Timestamp,
    pub(crate) available_at: stateknot_core::Timestamp,
}

impl RunnableRunCandidate {
    /// Returns the complete validated durable run projection.
    #[must_use]
    pub const fn run(&self) -> &StoredRun {
        &self.run
    }

    /// Returns the database observation that inserted the run into the queue.
    #[must_use]
    pub const fn ready_at(&self) -> stateknot_core::Timestamp {
        self.ready_at
    }

    /// Returns the effective claim time after applying any lease expiry.
    #[must_use]
    pub const fn available_at(&self) -> stateknot_core::Timestamp {
        self.available_at
    }
}

/// Opaque continuation for one fixed runnable-run database snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunnableRunPageCursor {
    pub(crate) tenant_id: stateknot_core::TenantId,
    pub(crate) snapshot_at: stateknot_core::Timestamp,
    pub(crate) available_at: stateknot_core::Timestamp,
    pub(crate) run_id: stateknot_core::RunId,
}

/// One stable, bounded tenant-level scheduler candidate page.
#[derive(Clone, Debug)]
pub struct RunnableRunPage {
    pub(crate) tenant_id: stateknot_core::TenantId,
    pub(crate) snapshot_at: stateknot_core::Timestamp,
    pub(crate) records: Vec<RunnableRunCandidate>,
    pub(crate) has_more: bool,
}

impl RunnableRunPage {
    /// Returns the database time fixed for the complete page chain.
    #[must_use]
    pub const fn snapshot_at(&self) -> stateknot_core::Timestamp {
        self.snapshot_at
    }

    /// Returns candidates ordered by effective availability then run identity.
    #[must_use]
    pub fn records(&self) -> &[RunnableRunCandidate] {
        &self.records
    }

    /// Returns whether another candidate remained in this fixed snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the opaque exact key required to continue this snapshot.
    #[must_use]
    pub fn next_cursor(&self) -> Option<RunnableRunPageCursor> {
        self.records.last().map(|candidate| RunnableRunPageCursor {
            tenant_id: self.tenant_id.clone(),
            snapshot_at: self.snapshot_at,
            available_at: candidate.available_at,
            run_id: candidate.run.lifecycle().provenance().run_id(),
        })
    }
}

/// Result of a lease renewal with stable desired expiry.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LeaseRenewalOutcome {
    /// Renewal extended the durable expiry.
    Renewed(RunLease),
    /// The same desired expiry had already committed.
    Idempotent(RunLease),
}

/// Result of claiming or explicitly superseding execution ownership.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LeaseClaimOutcome {
    /// A new attempt and successor fencing epoch committed.
    Claimed(RunLease),
    /// The same stable attempt already owns the current unexpired lease.
    Idempotent(RunLease),
}

impl LeaseClaimOutcome {
    /// Returns the current validated lease.
    #[must_use]
    pub const fn lease(&self) -> &RunLease {
        match self {
            Self::Claimed(lease) | Self::Idempotent(lease) => lease,
        }
    }
}

impl LeaseRenewalOutcome {
    /// Returns the current validated lease.
    #[must_use]
    pub const fn lease(&self) -> &RunLease {
        match self {
            Self::Renewed(lease) | Self::Idempotent(lease) => lease,
        }
    }
}

/// Result of an exact-fence lease release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LeaseReleaseOutcome {
    /// The active lease was cleared.
    Released,
    /// The exact epoch was already released and no successor was issued.
    Idempotent,
}

/// Hard-bounded number of events returned by one journal read.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JournalPageSize(u16);

impl JournalPageSize {
    /// Largest page accepted by the provider.
    pub const MAX: u16 = 1_000;

    /// Constructs a positive bounded page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidPageSize`] for zero or values above 1,000.
    pub const fn new(value: u16) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the page size as an integer.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// One bounded, validated page from a run journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalPage {
    pub(crate) events: Vec<JournalEvent>,
    pub(crate) has_more: bool,
}

impl JournalPage {
    /// Returns events in ascending contiguous sequence order.
    #[must_use]
    pub fn events(&self) -> &[JournalEvent] {
        &self.events
    }

    /// Returns whether another page exists after the final returned sequence.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the complete final event head to use as the next exact cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<JournalHead> {
        self.events.last().map(JournalEvent::head)
    }
}
