// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use serde::{Deserialize, Serialize};
use stateknot_core::{
    AgentAdmission, Checkpoint, CheckpointHead, CheckpointId, CompiledGraph, DeliveryFence, Digest,
    DurableTimer, DurableTimerRecord, DurableWait, FencingEpoch, InterruptRecord, InterruptRequest,
    JournalEvent, JournalExpectation, JournalHead, JournalPayload, ModelInvocation, NodeAttempt,
    OutboxAttempt, OutboxAttemptCompletion, OutboxAttemptStart, OutboxDelivery,
    OutboxDestinationRef, PendingNodeResult, PendingNodeResultHead, QuarantineId, RunFence, RunId,
    RunLease, RunLifecycle, RunRevision, RunTransition, SchedulerReservationId, SchedulerShardId,
    Superstep, TenantId, Timestamp, ToolInvocation,
};

use crate::StoreError;

const SCHEDULER_FAIRNESS_POLICY_DIGEST_DOMAIN: &[u8] = b"stateknot.scheduler-fairness-policy.v1\0";

/// Immutable canonical policy bytes registered for one scheduler shard.
///
/// The store deliberately treats the policy body as opaque canonical bytes;
/// `stateknot-runtime` owns the weighted-schedule schema and verifies it before
/// registration and after loading. The domain-separated digest prevents policy
/// bytes from being substituted with another digest-bearing record type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerFairnessPolicyRegistration {
    shard_id: SchedulerShardId,
    policy_digest: Digest,
    policy_bytes: Box<[u8]>,
    cycle_length: u16,
}

impl SchedulerFairnessPolicyRegistration {
    /// Maximum canonical policy byte length accepted by the provider.
    pub const MAX_POLICY_BYTES: usize = 262_144;
    /// Maximum number of deterministic slots in one weighted cycle.
    pub const MAX_CYCLE_LENGTH: u16 = 4096;

    /// Constructs and checksums one immutable shard policy registration.
    ///
    /// # Errors
    ///
    /// Rejects empty/oversized policy bytes or a zero/excessive cycle length.
    pub fn new(
        shard_id: SchedulerShardId,
        policy_bytes: impl Into<Vec<u8>>,
        cycle_length: u16,
    ) -> Result<Self, StoreError> {
        let policy_bytes = policy_bytes.into();
        if policy_bytes.is_empty()
            || policy_bytes.len() > Self::MAX_POLICY_BYTES
            || cycle_length == 0
            || cycle_length > Self::MAX_CYCLE_LENGTH
        {
            return Err(StoreError::InvalidSchedulerFairnessPolicy);
        }
        let mut preimage =
            Vec::with_capacity(SCHEDULER_FAIRNESS_POLICY_DIGEST_DOMAIN.len() + policy_bytes.len());
        preimage.extend_from_slice(SCHEDULER_FAIRNESS_POLICY_DIGEST_DOMAIN);
        preimage.extend_from_slice(&policy_bytes);
        Ok(Self {
            shard_id,
            policy_digest: Digest::sha256(preimage),
            policy_bytes: policy_bytes.into_boxed_slice(),
            cycle_length,
        })
    }

    /// Returns the immutable distributed-scheduler shard identity.
    #[must_use]
    pub const fn shard_id(&self) -> &SchedulerShardId {
        &self.shard_id
    }

    /// Returns the domain-separated canonical policy checksum.
    #[must_use]
    pub const fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// Returns the canonical runtime policy bytes.
    #[must_use]
    pub const fn policy_bytes(&self) -> &[u8] {
        &self.policy_bytes
    }

    /// Returns the exact number of slots in one weighted cycle.
    #[must_use]
    pub const fn cycle_length(&self) -> u16 {
        self.cycle_length
    }
}

/// Fully verified durable scheduler fairness policy snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredSchedulerFairnessPolicy {
    pub(crate) registration: SchedulerFairnessPolicyRegistration,
    pub(crate) registered_at: Timestamp,
}

impl StoredSchedulerFairnessPolicy {
    /// Returns the immutable registration payload.
    #[must_use]
    pub const fn registration(&self) -> &SchedulerFairnessPolicyRegistration {
        &self.registration
    }

    /// Returns the database clock at first registration.
    #[must_use]
    pub const fn registered_at(&self) -> Timestamp {
        self.registered_at
    }
}

/// Result of registering one immutable scheduler fairness shard.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SchedulerFairnessPolicyRegistrationOutcome {
    /// A new policy and global cursor committed.
    Registered(StoredSchedulerFairnessPolicy),
    /// The exact policy was already registered.
    Idempotent(StoredSchedulerFairnessPolicy),
}

impl SchedulerFairnessPolicyRegistrationOutcome {
    /// Returns the verified durable policy in either outcome.
    #[must_use]
    pub const fn policy(&self) -> &StoredSchedulerFairnessPolicy {
        match self {
            Self::Registered(policy) | Self::Idempotent(policy) => policy,
        }
    }
}

/// One globally ordered, lost-acknowledgement-safe fairness slot reservation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerFairnessReservation {
    pub(crate) shard_id: SchedulerShardId,
    pub(crate) reservation_id: SchedulerReservationId,
    pub(crate) policy_digest: Digest,
    pub(crate) sequence: u64,
    pub(crate) slot: u16,
    pub(crate) reserved_at: Timestamp,
}

impl SchedulerFairnessReservation {
    /// Returns the immutable scheduler shard.
    #[must_use]
    pub const fn shard_id(&self) -> &SchedulerShardId {
        &self.shard_id
    }

    /// Returns the stable idempotency identity supplied by the worker.
    #[must_use]
    pub const fn reservation_id(&self) -> SchedulerReservationId {
        self.reservation_id
    }

    /// Returns the immutable policy snapshot used for selection.
    #[must_use]
    pub const fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// Returns the zero-based global reservation sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the zero-based slot within the weighted policy cycle.
    #[must_use]
    pub const fn slot(&self) -> u16 {
        self.slot
    }

    /// Returns the database time at first reservation.
    #[must_use]
    pub const fn reserved_at(&self) -> Timestamp {
        self.reserved_at
    }
}

/// Bounded maintenance policy for durable fairness reservation evidence.
///
/// A deployment must stop retrying a reservation identity before `retain_for`
/// elapses. Deleting an identity and retrying it later would legitimately
/// allocate a new slot, so the hard minimum provides an operational safety
/// margin above the runtime's bounded immediate retry loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerFairnessRetentionPolicy {
    retain_for: Duration,
    batch_size: u16,
}

impl SchedulerFairnessRetentionPolicy {
    /// Smallest supported evidence retention window.
    pub const MIN_RETAIN_FOR: Duration = Duration::from_secs(60 * 60);
    /// Largest supported evidence retention window.
    pub const MAX_RETAIN_FOR: Duration = Duration::from_secs(366 * 24 * 60 * 60);
    /// Largest row count deleted by one short transaction.
    pub const MAX_BATCH_SIZE: u16 = 10_000;

    /// Constructs one bounded retention policy.
    ///
    /// # Errors
    ///
    /// Rejects retention outside the hard window or a zero/excessive batch.
    pub const fn new(retain_for: Duration, batch_size: u16) -> Result<Self, StoreError> {
        if retain_for.as_nanos() < Self::MIN_RETAIN_FOR.as_nanos()
            || retain_for.as_nanos() > Self::MAX_RETAIN_FOR.as_nanos()
            || batch_size == 0
            || batch_size > Self::MAX_BATCH_SIZE
        {
            return Err(StoreError::InvalidSchedulerFairnessRetention);
        }
        Ok(Self {
            retain_for,
            batch_size,
        })
    }

    /// Returns the minimum age of evidence eligible for deletion.
    #[must_use]
    pub const fn retain_for(self) -> Duration {
        self.retain_for
    }

    /// Returns the maximum rows deleted by one transaction.
    #[must_use]
    pub const fn batch_size(self) -> u16 {
        self.batch_size
    }
}

/// Auditable result of one bounded fairness-reservation retention pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerFairnessRetentionReport {
    pub(crate) observed_at: Timestamp,
    pub(crate) cutoff: Timestamp,
    pub(crate) deleted: u16,
}

impl SchedulerFairnessRetentionReport {
    /// Returns the authoritative database clock for this maintenance pass.
    #[must_use]
    pub const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    /// Returns the exclusive oldest-retained cutoff.
    #[must_use]
    pub const fn cutoff(self) -> Timestamp {
        self.cutoff
    }

    /// Returns the exact number of deleted reservations.
    #[must_use]
    pub const fn deleted(self) -> u16 {
        self.deleted
    }
}

/// Fully validated immutable graph definition in one tenant registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredGraphDefinition {
    pub(crate) tenant_id: TenantId,
    pub(crate) graph: CompiledGraph,
    pub(crate) registered_at: Timestamp,
}

impl StoredGraphDefinition {
    /// Returns the tenant registry that owns this durable registration.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the fully recompiled and integrity-checked graph definition.
    #[must_use]
    pub const fn graph(&self) -> &CompiledGraph {
        &self.graph
    }

    /// Returns the database-clock registration observation.
    #[must_use]
    pub const fn registered_at(&self) -> Timestamp {
        self.registered_at
    }
}

/// Result of idempotently registering one immutable compiled graph version.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GraphDefinitionRegistrationOutcome {
    /// A new owner-qualified graph version committed.
    Registered(StoredGraphDefinition),
    /// The exact canonical graph definition was already registered.
    Idempotent(StoredGraphDefinition),
}

impl GraphDefinitionRegistrationOutcome {
    /// Returns the fully validated durable graph definition.
    #[must_use]
    pub const fn definition(&self) -> &StoredGraphDefinition {
        match self {
            Self::Registered(definition) | Self::Idempotent(definition) => definition,
        }
    }
}

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
    pub(crate) expected_fence: Option<RunFence>,
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
            expected_fence: None,
        })
    }

    /// Binds this observation to the exact live worker fence that detected it.
    ///
    /// A fenced quarantine commits only while that attempt and epoch still own
    /// an unexpired lease. This prevents a superseded or expired recovery worker
    /// from stopping a successor that happens to share the same journal head.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRunQuarantineRequest`] when the fence
    /// crosses the request tenant or run.
    pub fn with_expected_fence(mut self, fence: RunFence) -> Result<Self, StoreError> {
        if fence.tenant_id() != &self.tenant_id || fence.run_id() != self.run_id {
            return Err(StoreError::InvalidRunQuarantineRequest);
        }
        if self
            .expected_fence
            .as_ref()
            .is_some_and(|expected| expected != &fence)
        {
            return Err(StoreError::InvalidRunQuarantineRequest);
        }
        self.expected_fence = Some(fence);
        Ok(self)
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

    /// Returns the exact recovery fence that must still own the run, if any.
    #[must_use]
    pub const fn expected_fence(&self) -> Option<&RunFence> {
        self.expected_fence.as_ref()
    }
}

/// Stable context for automatically quarantining one failed recovery read.
///
/// Clone and reuse the complete value after an ambiguous database outcome. The
/// eventual corruption category is derived from [`StoreError::CorruptData`]
/// rather than accepted from the caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorruptionQuarantineContext {
    pub(crate) tenant_id: TenantId,
    pub(crate) run_id: RunId,
    pub(crate) quarantine_id: QuarantineId,
    pub(crate) expectation: JournalExpectation,
    pub(crate) evidence_digest: Digest,
    pub(crate) expected_fence: Option<RunFence>,
}

impl CorruptionQuarantineContext {
    /// Constructs a tenant-bound recovery observation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRunQuarantineRequest`] when an exact head
    /// belongs to another tenant or run.
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        quarantine_id: QuarantineId,
        expectation: JournalExpectation,
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
            evidence_digest,
            expected_fence: None,
        })
    }

    /// Binds all corruption quarantines from this context to one live fence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidClaimedRunRecoveryContext`] when the fence
    /// crosses the context tenant or run.
    pub fn with_expected_fence(mut self, fence: RunFence) -> Result<Self, StoreError> {
        if fence.tenant_id() != &self.tenant_id || fence.run_id() != self.run_id {
            return Err(StoreError::InvalidClaimedRunRecoveryContext);
        }
        if self
            .expected_fence
            .as_ref()
            .is_some_and(|expected| expected != &fence)
        {
            return Err(StoreError::InvalidClaimedRunRecoveryContext);
        }
        self.expected_fence = Some(fence);
        Ok(self)
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the observed run identity.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the stable quarantine identity.
    #[must_use]
    pub const fn quarantine_id(&self) -> QuarantineId {
        self.quarantine_id
    }

    /// Returns the exact journal observation made by recovery.
    #[must_use]
    pub const fn expectation(&self) -> &JournalExpectation {
        &self.expectation
    }

    /// Returns the caller-retained redacted evidence checksum.
    #[must_use]
    pub const fn evidence_digest(&self) -> Digest {
        self.evidence_digest
    }

    /// Returns the exact live fence required for quarantine, when bound.
    #[must_use]
    pub const fn expected_fence(&self) -> Option<&RunFence> {
        self.expected_fence.as_ref()
    }

    pub(crate) fn into_request(
        self,
        component: RunQuarantineComponent,
    ) -> Result<RunQuarantineRequest, StoreError> {
        let request = RunQuarantineRequest::new(
            self.tenant_id,
            self.run_id,
            self.quarantine_id,
            self.expectation,
            RunQuarantineCause::IntegrityFailure,
            component,
            self.evidence_digest,
        )?;
        match self.expected_fence {
            Some(fence) => request.with_expected_fence(fence),
            None => Ok(request),
        }
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

/// Fully verified durable Agent admission and its current run projection.
///
/// `event` and `checkpoint` are always the immutable sequence-one and
/// superstep-zero anchors. `run` is the current snapshot and may therefore have
/// advanced beyond both anchors when this value is returned by a retry or load.
#[derive(Clone, Debug)]
pub struct StoredAgentAdmission {
    pub(crate) admission: AgentAdmission,
    pub(crate) run: StoredRun,
    pub(crate) event: JournalEvent,
    pub(crate) checkpoint: Checkpoint,
}

impl StoredAgentAdmission {
    /// Returns the immutable database-clock admission snapshot.
    #[must_use]
    pub const fn admission(&self) -> &AgentAdmission {
        &self.admission
    }

    /// Returns the current validated run row.
    #[must_use]
    pub const fn run(&self) -> &StoredRun {
        &self.run
    }

    /// Returns the immutable first journal event.
    #[must_use]
    pub const fn event(&self) -> &JournalEvent {
        &self.event
    }

    /// Returns the immutable superstep-zero checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
}

/// Result of atomically admitting and initializing one executable Agent run.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AgentAdmissionCommitOutcome {
    /// The admission, run, event, lifecycle start, and checkpoint committed.
    Committed(StoredAgentAdmission),
    /// The exact immutable request had already committed.
    Idempotent(StoredAgentAdmission),
}

impl AgentAdmissionCommitOutcome {
    /// Returns the fully verified durable admission in either outcome.
    #[must_use]
    pub const fn stored(&self) -> &StoredAgentAdmission {
        match self {
            Self::Committed(stored) | Self::Idempotent(stored) => stored,
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

    /// Consumes the page and returns its fully verified records without cloning
    /// their bounded JSON payloads.
    #[must_use]
    pub fn into_records(self) -> Vec<PendingNodeResult> {
        self.records
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

/// Memory ceiling for independently replaying one historical graph barrier.
///
/// The provider measures the compact serialized size of every fully verified
/// pending result before retaining it for the deterministic planner. A single
/// bounded result may transiently sit beside the retained set while the limit
/// check runs, but the configured aggregate is never exceeded after insertion.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct GraphReplayLimits {
    maximum_barrier_result_bytes: usize,
}

impl GraphReplayLimits {
    /// Absolute implementation maximum for retained results in one barrier.
    pub const HARD_MAXIMUM_BARRIER_RESULT_BYTES: usize = 512 * 1024 * 1024;

    /// Constructs a positive replay memory ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidGraphReplayLimits`] for zero or more than
    /// 512 MiB.
    pub const fn new(maximum_barrier_result_bytes: usize) -> Result<Self, StoreError> {
        if maximum_barrier_result_bytes == 0
            || maximum_barrier_result_bytes > Self::HARD_MAXIMUM_BARRIER_RESULT_BYTES
        {
            return Err(StoreError::InvalidGraphReplayLimits);
        }
        Ok(Self {
            maximum_barrier_result_bytes,
        })
    }

    /// Returns the retained compact-result byte ceiling per barrier.
    #[must_use]
    pub const fn maximum_barrier_result_bytes(self) -> usize {
        self.maximum_barrier_result_bytes
    }
}

impl Default for GraphReplayLimits {
    fn default() -> Self {
        Self {
            maximum_barrier_result_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Evidence summary returned after complete noninitial checkpoint replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GraphReplayReport {
    pub(crate) checkpoints_validated: u64,
    pub(crate) barriers_replayed: u64,
    pub(crate) results_replayed: u64,
    pub(crate) maximum_barrier_result_bytes: usize,
}

impl GraphReplayReport {
    /// Returns every checkpoint whose lineage and graph binding was verified.
    #[must_use]
    pub const fn checkpoints_validated(self) -> u64 {
        self.checkpoints_validated
    }

    /// Returns the number of parent-to-child transitions independently planned.
    #[must_use]
    pub const fn barriers_replayed(self) -> u64 {
        self.barriers_replayed
    }

    /// Returns the total number of immutable node results re-evaluated.
    #[must_use]
    pub const fn results_replayed(self) -> u64 {
        self.results_replayed
    }

    /// Returns the largest retained compact result set observed for one barrier.
    #[must_use]
    pub const fn maximum_barrier_result_bytes(self) -> usize {
        self.maximum_barrier_result_bytes
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
    pub(crate) scheduler_not_before: Option<stateknot_core::Timestamp>,
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

    /// Returns the durable execution gate for a verified delayed node retry.
    ///
    /// Queue age remains available through [`Self::scheduler_ready_at`]. A
    /// scheduler must not claim this run before the inclusive instant returned
    /// here. Claiming at or after the boundary clears the gate atomically.
    #[must_use]
    pub const fn scheduler_not_before(&self) -> Option<stateknot_core::Timestamp> {
        self.scheduler_not_before
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

    /// Returns the effective claim time after applying retry and lease gates.
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

/// Exact lease proven live at one database-clock observation.
///
/// This is short-lived authority evidence, not a promise that the lease remains
/// live after [`Self::observed_at`]. Callers performing external work must
/// translate the remaining database duration into a conservative monotonic
/// deadline and continue renewing beneath the same fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveLeaseObservation {
    pub(crate) lease: RunLease,
    pub(crate) observed_at: Timestamp,
}

impl LiveLeaseObservation {
    /// Returns the exact fence and exclusive durable expiry observed.
    #[must_use]
    pub const fn lease(&self) -> &RunLease {
        &self.lease
    }

    /// Returns the database clock used to prove the lease unexpired.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Consumes the observation into its lease and database time.
    #[must_use]
    pub fn into_parts(self) -> (RunLease, Timestamp) {
        (self.lease, self.observed_at)
    }
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

/// Result of projecting a recovery plan's delayed retry into scheduler state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DelayedRetryScheduleOutcome {
    /// The exact live lease was released and the durable wakeup was committed.
    Scheduled {
        /// Inclusive earliest database instant for another scheduler claim.
        not_before: Timestamp,
    },
    /// The same fence and wakeup had already committed before acknowledgement.
    Idempotent {
        /// Inclusive earliest database instant already stored for the run.
        not_before: Timestamp,
    },
    /// Database time reached the retry boundary before the deferral committed.
    ///
    /// The caller retains its lease and should rebuild the recovery plan rather
    /// than release and immediately reclaim the same run.
    Due {
        /// Inclusive retry boundary from the supplied verified plan.
        not_before: Timestamp,
    },
}

impl DelayedRetryScheduleOutcome {
    /// Returns the inclusive retry boundary shared by every outcome.
    #[must_use]
    pub const fn not_before(self) -> Timestamp {
        match self {
            Self::Scheduled { not_before }
            | Self::Idempotent { not_before }
            | Self::Due { not_before } => not_before,
        }
    }
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
