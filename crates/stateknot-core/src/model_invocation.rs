// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable, integrity-bound state machine for logical model invocations.
//!
//! A logical invocation snapshots one validated model binding and request while
//! every real provider exchange receives a new [`AttemptId`]. The runtime first
//! commits `prepared`, durably claims `executing`, performs exactly one provider
//! exchange with SDK retries disabled, and finally commits a complete response
//! or public-safe failure. Partial streaming output is never a committed result.

use std::{fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::decimal::{UnsignedDecimalError, parse_bounded_u64};
use crate::{
    AttemptId, CapabilityLifecycleState, Digest, InvocationId, JournalHead, JournalSequence,
    ModelCapabilityMismatch, ModelDescriptor, ModelError, ModelErrorPhase,
    ModelErrorValidationError, ModelRequest, ModelResponse, ModelResponseError, NodeActivation,
    RunId, TenantId, Timestamp,
};

const MAX_DATABASE_ORDINAL: u64 = i64::MAX as u64;
const INVOCATION_REVISION_PATTERN: &str = "^(0|[1-9][0-9]{0,18})$";
const INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-model-invocation-intent-v1\0";
const TRANSITION_DIGEST_DOMAIN: &[u8] = b"stateknot-model-invocation-transition-v1\0";
const RECORD_DIGEST_DOMAIN: &[u8] = b"stateknot-model-invocation-record-v1\0";

/// Monotonic zero-based revision of one logical model invocation.
///
/// The decimal-string wire form preserves exact values across languages and
/// the maximum matches a signed `PostgreSQL BIGINT`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ModelInvocationRevision(u64);

impl ModelInvocationRevision {
    /// Preparation record revision.
    pub const INITIAL: Self = Self(0);
    /// Largest revision supported by the v1 storage contract.
    pub const MAX: Self = Self(MAX_DATABASE_ORDINAL);

    /// Constructs a storage-compatible revision.
    ///
    /// # Errors
    ///
    /// Returns [`ModelInvocationRevisionError::AboveMaximum`] above signed
    /// `BIGINT`.
    pub const fn new(value: u64) -> Result<Self, ModelInvocationRevisionError> {
        if value > MAX_DATABASE_ORDINAL {
            return Err(ModelInvocationRevisionError::AboveMaximum);
        }
        Ok(Self(value))
    }

    /// Returns the integer revision.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the exact next revision or `None` at the storage ceiling.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        if self.0 == MAX_DATABASE_ORDINAL {
            None
        } else {
            Some(Self(self.0 + 1))
        }
    }
}

impl fmt::Display for ModelInvocationRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ModelInvocationRevision {
    type Err = ModelInvocationRevisionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = parse_bounded_u64(value, MAX_DATABASE_ORDINAL)
            .map_err(ModelInvocationRevisionError::from_decimal_error)?;
        Self::new(value)
    }
}

impl Serialize for ModelInvocationRevision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ModelInvocationRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ModelInvocationRevisionVisitor)
    }
}

struct ModelInvocationRevisionVisitor;

impl de::Visitor<'_> for ModelInvocationRevisionVisitor {
    type Value = ModelInvocationRevision;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical non-negative decimal PostgreSQL BIGINT revision")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

impl JsonSchema for ModelInvocationRevision {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelInvocationRevision".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ModelInvocationRevision").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 19,
            "pattern": INVOCATION_REVISION_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid canonical model invocation revision.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelInvocationRevisionError {
    /// The wire value was empty or contained a non-decimal byte.
    #[error("model invocation revision must contain only unsigned ASCII decimal digits")]
    InvalidFormat,
    /// The decimal text contained a leading zero.
    #[error("model invocation revision must use canonical decimal text")]
    NonCanonical,
    /// The value exceeded signed `PostgreSQL BIGINT`.
    #[error("model invocation revision exceeds the PostgreSQL BIGINT maximum")]
    AboveMaximum,
}

impl ModelInvocationRevisionError {
    const fn from_decimal_error(error: UnsignedDecimalError) -> Self {
        match error {
            UnsignedDecimalError::Empty | UnsignedDecimalError::InvalidCharacter { .. } => {
                Self::InvalidFormat
            }
            UnsignedDecimalError::LeadingZero => Self::NonCanonical,
            UnsignedDecimalError::TooLong { .. } | UnsignedDecimalError::Overflow => {
                Self::AboveMaximum
            }
        }
    }
}

/// Immutable preparation intent for one logical model invocation.
///
/// The descriptor and request are complete execution snapshots. Credentials,
/// SDK clients, provider aliases, process-local cancellation state, and mutable
/// budget counters remain outside this durable value.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInvocationIntent {
    activation: NodeActivation,
    invocation_id: InvocationId,
    descriptor: ModelDescriptor,
    request: ModelRequest,
    intent_digest: Digest,
}

impl ModelInvocationIntent {
    /// Constructs, negotiates, and checksums a durable model invocation intent.
    ///
    /// # Errors
    ///
    /// Returns [`ModelInvocationIntentError`] when a retired binding is selected,
    /// the exact descriptor does not satisfy the request, or canonical integrity
    /// material cannot be produced.
    pub fn new(
        activation: NodeActivation,
        invocation_id: InvocationId,
        descriptor: ModelDescriptor,
        request: ModelRequest,
    ) -> Result<Self, ModelInvocationIntentError> {
        validate_intent_shape(&descriptor, &request)?;
        let intent_digest = compute_intent_digest(&ModelInvocationIntentDigestWire {
            activation: &activation,
            invocation_id,
            descriptor: &descriptor,
            request: &request,
        })?;
        Ok(Self {
            activation,
            invocation_id,
            descriptor,
            request,
            intent_digest,
        })
    }

    /// Restores an intent and verifies its negotiation and checksum layers.
    ///
    /// # Errors
    ///
    /// Returns [`ModelInvocationIntentError`] when validation or integrity fails.
    pub fn restore(
        activation: NodeActivation,
        invocation_id: InvocationId,
        descriptor: ModelDescriptor,
        request: ModelRequest,
        intent_digest: Digest,
    ) -> Result<Self, ModelInvocationIntentError> {
        let restored = Self::new(activation, invocation_id, descriptor, request)?;
        if restored.intent_digest != intent_digest {
            return Err(ModelInvocationIntentError::DigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the owning graph-node activation.
    #[must_use]
    pub const fn activation(&self) -> &NodeActivation {
        &self.activation
    }

    /// Returns the stable logical invocation identifier.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the exact registered model binding snapshot.
    #[must_use]
    pub const fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    /// Returns the complete immutable provider-neutral request.
    #[must_use]
    pub const fn request(&self) -> &ModelRequest {
        &self.request
    }

    /// Returns the domain-separated preparation fingerprint.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the tenant boundary inherited from the activation.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        self.activation.tenant_id()
    }

    /// Returns the run identity inherited from the activation.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.activation.run_id()
    }
}

impl fmt::Debug for ModelInvocationIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelInvocationIntent")
            .field("activation", &self.activation)
            .field("invocation_id", &self.invocation_id)
            .field("descriptor", &self.descriptor)
            .field("request", &self.request)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ModelInvocationIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            activation: NodeActivation,
            invocation_id: InvocationId,
            descriptor: ModelDescriptor,
            request: ModelRequest,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.activation,
            wire.invocation_id,
            wire.descriptor,
            wire.request,
            wire.intent_digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid durable model invocation intent.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelInvocationIntentError {
    /// A historical-only model binding was selected for new execution.
    #[error("retired model binding cannot be selected for a new invocation")]
    RetiredModel,
    /// The exact binding cannot satisfy every request requirement.
    #[error("model binding does not satisfy the invocation request: {mismatch:?}")]
    CapabilityMismatch {
        /// Deterministic complete mismatch set.
        mismatch: Box<ModelCapabilityMismatch>,
    },
    /// Canonical integrity material could not be serialized.
    #[error("model invocation intent integrity calculation failed: {source}")]
    Integrity {
        /// Exact integrity failure.
        #[source]
        source: ModelInvocationIntegrityError,
    },
    /// Persisted intent checksum did not match caller-controlled fields.
    #[error("model invocation intent digest does not match its fields")]
    DigestMismatch,
}

impl From<ModelInvocationIntegrityError> for ModelInvocationIntentError {
    fn from(source: ModelInvocationIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Durable lifecycle state of one logical model invocation.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ModelInvocationStatus {
    /// Intent is committed but no provider attempt has started.
    Prepared,
    /// Exactly one physical provider attempt has been durably claimed.
    Executing,
    /// A complete validated response is durable.
    Committed,
    /// A public-safe failed attempt is durable and retry remains policy-gated.
    Failed,
}

/// Integrity-bound state payload of one model invocation revision.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
pub enum ModelInvocationState {
    /// Prepared request awaiting an attempt claim.
    Prepared,
    /// In-flight physical provider attempt.
    Executing {
        /// Unique physical attempt identifier.
        attempt_id: AttemptId,
    },
    /// Complete validated provider-neutral response.
    Committed {
        /// Exact response and attempt provenance.
        response: ModelResponse,
    },
    /// Failed provider attempt.
    Failed {
        /// Public-safe failure, correlation, and optional usage evidence.
        error: ModelError,
    },
}

impl ModelInvocationState {
    /// Returns the lifecycle discriminator.
    #[must_use]
    pub const fn status(&self) -> ModelInvocationStatus {
        match self {
            Self::Prepared => ModelInvocationStatus::Prepared,
            Self::Executing { .. } => ModelInvocationStatus::Executing,
            Self::Committed { .. } => ModelInvocationStatus::Committed,
            Self::Failed { .. } => ModelInvocationStatus::Failed,
        }
    }

    /// Returns the physical attempt represented by this state, if any.
    #[must_use]
    pub const fn attempt_id(&self) -> Option<AttemptId> {
        match self {
            Self::Prepared => None,
            Self::Executing { attempt_id } => Some(*attempt_id),
            Self::Committed { response } => Some(response.provenance().attempt_id()),
            Self::Failed { error } => Some(error.provenance().attempt_id()),
        }
    }
}

impl<'de> Deserialize<'de> for ModelInvocationState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
        #[allow(clippy::large_enum_variant)]
        enum Wire {
            Prepared,
            Executing { attempt_id: AttemptId },
            Committed { response: ModelResponse },
            Failed { error: ModelError },
        }

        Ok(match Wire::deserialize(deserializer)? {
            Wire::Prepared => Self::Prepared,
            Wire::Executing { attempt_id } => Self::Executing { attempt_id },
            Wire::Committed { response } => Self::Committed { response },
            Wire::Failed { error } => Self::Failed { error },
        })
    }
}

/// Kind of one explicit model invocation state transition.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ModelInvocationTransitionKind {
    /// Claim a new physical provider attempt.
    StartAttempt,
    /// Commit the executing attempt's complete response.
    RecordResponse,
    /// Commit the executing attempt's failure evidence.
    RecordError,
}

/// Explicit transition appended to the run journal and model history.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
pub enum ModelInvocationTransition {
    /// Claim a unique physical attempt after preparation or a safe retry.
    StartAttempt {
        /// New physical attempt identity.
        attempt_id: AttemptId,
    },
    /// Commit a complete response from the current executing attempt.
    RecordResponse {
        /// Validated provider-neutral response.
        response: ModelResponse,
    },
    /// Commit public-safe failure and optional normalized usage evidence.
    RecordError {
        /// Validated attempt failure.
        error: ModelError,
    },
}

impl ModelInvocationTransition {
    /// Returns the closed transition discriminator.
    #[must_use]
    pub const fn kind(&self) -> ModelInvocationTransitionKind {
        match self {
            Self::StartAttempt { .. } => ModelInvocationTransitionKind::StartAttempt,
            Self::RecordResponse { .. } => ModelInvocationTransitionKind::RecordResponse,
            Self::RecordError { .. } => ModelInvocationTransitionKind::RecordError,
        }
    }
}

/// Compact exact identity of a validated model invocation revision.
///
/// A head is an optimistic comparison token. It intentionally omits prompts,
/// response content, and failures; obtain it from [`ModelInvocation::head`] or
/// storage that restored and verified the corresponding full record.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInvocationHead {
    tenant_id: TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    revision: ModelInvocationRevision,
    status: ModelInvocationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<AttemptId>,
    journal_head: JournalHead,
    digest: Digest,
}

impl ModelInvocationHead {
    /// Constructs a trusted compact head while enforcing scope and state shape.
    ///
    /// # Errors
    ///
    /// Returns [`ModelInvocationHeadError`] for crossed journal scope or an
    /// impossible status/attempt/revision combination.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        invocation_id: InvocationId,
        revision: ModelInvocationRevision,
        status: ModelInvocationStatus,
        attempt_id: Option<AttemptId>,
        journal_head: JournalHead,
        digest: Digest,
    ) -> Result<Self, ModelInvocationHeadError> {
        validate_journal_scope(&tenant_id, run_id, &journal_head)
            .map_err(ModelInvocationHeadError::from_scope)?;
        validate_status_attempt(status, attempt_id)?;
        validate_revision_status(revision, status)?;
        Ok(Self {
            tenant_id,
            run_id,
            invocation_id,
            revision,
            status,
            attempt_id,
            journal_head,
            digest,
        })
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the durable run identity.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the stable logical invocation identity.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the exact record revision.
    #[must_use]
    pub const fn revision(&self) -> ModelInvocationRevision {
        self.revision
    }

    /// Returns the lifecycle state at this revision.
    #[must_use]
    pub const fn status(&self) -> ModelInvocationStatus {
        self.status
    }

    /// Returns the physical attempt represented by this revision, if any.
    #[must_use]
    pub const fn attempt_id(&self) -> Option<AttemptId> {
        self.attempt_id
    }

    /// Returns the exact journal prefix anchoring this revision.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the complete model-invocation record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for ModelInvocationHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            invocation_id: InvocationId,
            revision: ModelInvocationRevision,
            status: ModelInvocationStatus,
            attempt_id: Option<AttemptId>,
            journal_head: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.tenant_id,
            wire.run_id,
            wire.invocation_id,
            wire.revision,
            wire.status,
            wire.attempt_id,
            wire.journal_head,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid compact model invocation head.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelInvocationHeadError {
    /// Journal head crossed the invocation tenant boundary.
    #[error("model invocation journal head crosses the tenant boundary")]
    JournalTenantMismatch,
    /// Journal head named another run.
    #[error("model invocation journal head does not belong to the run")]
    JournalRunMismatch,
    /// Prepared status unexpectedly named a physical attempt.
    #[error("prepared model invocation head must not contain an attempt")]
    PreparedHasAttempt,
    /// A non-prepared status omitted its physical attempt.
    #[error("non-prepared model invocation head must contain an attempt")]
    AttemptMissing,
    /// Revision zero named a state other than preparation.
    #[error("model invocation head revision zero must be prepared")]
    InitialStatusMismatch,
    /// A later revision tried to return to preparation.
    #[error("prepared model invocation head must use revision zero")]
    PreparedRevisionMismatch,
}

impl ModelInvocationHeadError {
    const fn from_scope(error: InvocationScopeError) -> Self {
        match error {
            InvocationScopeError::Tenant => Self::JournalTenantMismatch,
            InvocationScopeError::Run => Self::JournalRunMismatch,
        }
    }
}

/// One immutable, journal-anchored revision of a logical model invocation.
///
/// Deserialization verifies local checksums, scope, descriptor/request/result
/// binding, and predecessor-head shape. Stream the complete ascending history
/// through [`ModelInvocationHistoryVerifier`] to prove delayed retry legality.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelInvocation {
    intent: ModelInvocationIntent,
    revision: ModelInvocationRevision,
    state: ModelInvocationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous: Option<ModelInvocationHead>,
    journal_head: JournalHead,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition: Option<ModelInvocationTransition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition_digest: Option<Digest>,
    digest: Digest,
}

impl ModelInvocation {
    /// Materializes the initial prepared revision after its journal event commits.
    ///
    /// The journal head must belong to the activation's tenant/run and strictly
    /// advance the exact base checkpoint without regressing durable time.
    ///
    /// # Errors
    ///
    /// Returns [`ModelInvocationError`] for scope, ordering, or integrity failure.
    pub fn prepare(
        intent: ModelInvocationIntent,
        journal_head: JournalHead,
    ) -> Result<Self, ModelInvocationError> {
        validate_preparation_journal(&intent, &journal_head)?;
        let revision = ModelInvocationRevision::INITIAL;
        let state = ModelInvocationState::Prepared;
        let digest = compute_record_digest(&ModelInvocationRecordDigestWire {
            intent_digest: intent.intent_digest,
            revision,
            state: &state,
            previous: None,
            journal_head: &journal_head,
            transition_digest: None,
        })?;
        Ok(Self {
            intent,
            revision,
            state,
            previous: None,
            journal_head,
            transition: None,
            transition_digest: None,
            digest,
        })
    }

    /// Applies one legal transition and constructs its next immutable revision.
    ///
    /// This enforces exact response/error provenance, request/descriptor
    /// bindings, minimum retry delay, journal order, and revision overflow.
    /// Stores must still compare [`Self::head`] under the run lock and current
    /// fencing token, and must enforce run-wide physical attempt uniqueness.
    ///
    /// # Errors
    ///
    /// Returns [`ModelInvocationError`] when the transition cannot safely commit.
    pub fn advance(
        &self,
        transition: ModelInvocationTransition,
        journal_head: JournalHead,
    ) -> Result<Self, ModelInvocationError> {
        validate_successor_journal(self, &journal_head)?;
        let revision = self
            .revision
            .checked_next()
            .ok_or(ModelInvocationError::RevisionOverflow)?;
        let state = apply_transition(self, &transition, journal_head.recorded_at())?;
        let transition_digest = compute_transition_digest(&transition)?;
        let previous = self.head();
        let digest = compute_record_digest(&ModelInvocationRecordDigestWire {
            intent_digest: self.intent.intent_digest,
            revision,
            state: &state,
            previous: Some(&previous),
            journal_head: &journal_head,
            transition_digest: Some(transition_digest),
        })?;
        Ok(Self {
            intent: self.intent.clone(),
            revision,
            state,
            previous: Some(previous),
            journal_head,
            transition: Some(transition),
            transition_digest: Some(transition_digest),
            digest,
        })
    }

    /// Restores a record and verifies every invariant available locally.
    ///
    /// Failed-to-executing retry authorization depends on the predecessor's full
    /// error and is therefore verified by [`ModelInvocationHistoryVerifier`],
    /// not by a compact predecessor head.
    ///
    /// # Errors
    ///
    /// Returns [`ModelInvocationError`] for malformed state, provenance, scope,
    /// ordering, transition checksum, or record checksum.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        intent: ModelInvocationIntent,
        revision: ModelInvocationRevision,
        state: ModelInvocationState,
        previous: Option<ModelInvocationHead>,
        journal_head: JournalHead,
        transition: Option<ModelInvocationTransition>,
        transition_digest: Option<Digest>,
        digest: Digest,
    ) -> Result<Self, ModelInvocationError> {
        validate_record_shape(
            &intent,
            revision,
            &state,
            previous.as_ref(),
            &journal_head,
            transition.as_ref(),
            transition_digest,
        )?;
        let expected = compute_record_digest(&ModelInvocationRecordDigestWire {
            intent_digest: intent.intent_digest,
            revision,
            state: &state,
            previous: previous.as_ref(),
            journal_head: &journal_head,
            transition_digest,
        })?;
        if digest != expected {
            return Err(ModelInvocationError::DigestMismatch);
        }
        Ok(Self {
            intent,
            revision,
            state,
            previous,
            journal_head,
            transition,
            transition_digest,
            digest,
        })
    }

    /// Returns the immutable preparation intent.
    #[must_use]
    pub const fn intent(&self) -> &ModelInvocationIntent {
        &self.intent
    }

    /// Returns this record's monotonic revision.
    #[must_use]
    pub const fn revision(&self) -> ModelInvocationRevision {
        self.revision
    }

    /// Returns the integrity-bound lifecycle state.
    #[must_use]
    pub const fn state(&self) -> &ModelInvocationState {
        &self.state
    }

    /// Returns the lifecycle discriminator.
    #[must_use]
    pub const fn status(&self) -> ModelInvocationStatus {
        self.state.status()
    }

    /// Returns the physical attempt represented by this revision, if any.
    #[must_use]
    pub const fn attempt_id(&self) -> Option<AttemptId> {
        self.state.attempt_id()
    }

    /// Returns the exact predecessor head, absent only for preparation.
    #[must_use]
    pub const fn previous(&self) -> Option<&ModelInvocationHead> {
        self.previous.as_ref()
    }

    /// Returns the exact journal prefix anchoring this revision.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the explicit transition, absent only for preparation.
    #[must_use]
    pub const fn transition(&self) -> Option<&ModelInvocationTransition> {
        self.transition.as_ref()
    }

    /// Returns the transition fingerprint, absent only for preparation.
    #[must_use]
    pub const fn transition_digest(&self) -> Option<Digest> {
        self.transition_digest
    }

    /// Returns the complete domain-separated record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns a compact exact optimistic-comparison token.
    #[must_use]
    pub fn head(&self) -> ModelInvocationHead {
        ModelInvocationHead {
            tenant_id: self.intent.tenant_id().clone(),
            run_id: self.intent.run_id(),
            invocation_id: self.intent.invocation_id,
            revision: self.revision,
            status: self.status(),
            attempt_id: self.attempt_id(),
            journal_head: self.journal_head.clone(),
            digest: self.digest,
        }
    }
}

impl fmt::Debug for ModelInvocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelInvocation")
            .field("intent", &self.intent)
            .field("revision", &self.revision)
            .field("state", &self.state)
            .field("previous", &self.previous)
            .field("journal_head", &self.journal_head)
            .field("transition", &self.transition)
            .field("transition_digest", &self.transition_digest)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ModelInvocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: ModelInvocationIntent,
            revision: ModelInvocationRevision,
            state: ModelInvocationState,
            previous: Option<ModelInvocationHead>,
            journal_head: JournalHead,
            transition: Option<ModelInvocationTransition>,
            transition_digest: Option<Digest>,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.intent,
            wire.revision,
            wire.state,
            wire.previous,
            wire.journal_head,
            wire.transition,
            wire.transition_digest,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Streaming validator for one complete ascending model invocation history.
///
/// Rejections are transactional: [`Self::verify_next`] never advances the last
/// accepted record on failure.
#[derive(Clone, Debug, Default)]
pub struct ModelInvocationHistoryVerifier {
    last: Option<ModelInvocation>,
}

impl ModelInvocationHistoryVerifier {
    /// Constructs an empty verifier expecting revision zero.
    #[must_use]
    pub const fn new() -> Self {
        Self { last: None }
    }

    /// Continues after one already trusted, fully restored record.
    ///
    /// A paged store must reload and compare the cursor's exact canonical record
    /// before using it. The full predecessor failure is needed for retry checks.
    #[must_use]
    pub fn after(record: ModelInvocation) -> Self {
        Self { last: Some(record) }
    }

    /// Returns the last verified head, if any.
    #[must_use]
    pub fn head(&self) -> Option<ModelInvocationHead> {
        self.last.as_ref().map(ModelInvocation::head)
    }

    /// Returns whether at least one revision has been verified.
    #[must_use]
    pub const fn has_records(&self) -> bool {
        self.last.is_some()
    }

    /// Verifies and then advances to the next ascending revision.
    ///
    /// # Errors
    ///
    /// Returns [`ModelInvocationHistoryError`] for a non-initial first record,
    /// intent substitution, head mismatch, unsafe retry, or any other state
    /// transition failure.
    pub fn verify_next(
        &mut self,
        record: &ModelInvocation,
    ) -> Result<(), ModelInvocationHistoryError> {
        let Some(previous) = self.last.as_ref() else {
            if record.revision != ModelInvocationRevision::INITIAL {
                return Err(ModelInvocationHistoryError::FirstRecordNotInitial {
                    actual: record.revision,
                });
            }
            self.last = Some(record.clone());
            return Ok(());
        };

        if record.intent != previous.intent {
            return Err(ModelInvocationHistoryError::IntentMismatch);
        }
        if record.previous.as_ref() != Some(&previous.head()) {
            return Err(ModelInvocationHistoryError::PreviousHeadMismatch);
        }
        let transition = record
            .transition
            .as_ref()
            .ok_or(ModelInvocationHistoryError::TransitionMissing)?;
        let expected = previous
            .advance(transition.clone(), record.journal_head.clone())
            .map_err(|source| ModelInvocationHistoryError::Transition { source })?;
        if !canonical_equal(&expected, record)
            .map_err(|source| ModelInvocationHistoryError::Integrity { source })?
        {
            return Err(ModelInvocationHistoryError::RecordMismatch);
        }

        self.last = Some(record.clone());
        Ok(())
    }
}

/// Invalid ascending model invocation history.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelInvocationHistoryError {
    /// The first supplied record was not preparation revision zero.
    #[error("first model invocation record is revision {actual}; expected 0")]
    FirstRecordNotInitial {
        /// Rejected first revision.
        actual: ModelInvocationRevision,
    },
    /// A successor substituted immutable preparation fields.
    #[error("model invocation history changed its immutable intent")]
    IntentMismatch,
    /// A successor did not name the exact previously verified head.
    #[error("model invocation history predecessor head mismatch")]
    PreviousHeadMismatch,
    /// A non-initial record omitted its transition.
    #[error("model invocation history successor is missing its transition")]
    TransitionMissing,
    /// Applying the transition to the full predecessor failed.
    #[error("model invocation history transition is invalid: {source}")]
    Transition {
        /// Exact state-machine failure.
        #[source]
        source: ModelInvocationError,
    },
    /// Expected and persisted successor bytes did not match exactly.
    #[error("model invocation history successor does not match the applied transition")]
    RecordMismatch,
    /// Exact record comparison could not be canonicalized.
    #[error("model invocation history integrity comparison failed: {source}")]
    Integrity {
        /// Exact canonicalization failure.
        #[source]
        source: ModelInvocationIntegrityError,
    },
}

/// Invalid model invocation record, transition, provenance, or integrity layer.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelInvocationError {
    /// Canonical typed integrity material could not be serialized.
    #[error("model invocation integrity calculation failed: {source}")]
    Integrity {
        /// Exact canonical integrity failure.
        #[source]
        source: ModelInvocationIntegrityError,
    },
    /// Journal head crossed the invocation tenant boundary.
    #[error("model invocation journal head crosses the tenant boundary")]
    JournalTenantMismatch,
    /// Journal head named another run.
    #[error("model invocation journal head does not belong to the run")]
    JournalRunMismatch,
    /// A revision did not strictly advance its required journal predecessor.
    #[error("model invocation journal sequence {actual} does not advance {previous}")]
    JournalDidNotAdvance {
        /// Required prior sequence.
        previous: JournalSequence,
        /// Rejected current sequence.
        actual: JournalSequence,
    },
    /// A revision's durable clock preceded its required predecessor.
    #[error("model invocation journal clock regressed from {previous} to {actual}")]
    ClockRegression {
        /// Required prior durable timestamp.
        previous: Timestamp,
        /// Rejected current timestamp.
        actual: Timestamp,
    },
    /// No storage-compatible successor revision exists.
    #[error("model invocation revision exceeds the PostgreSQL BIGINT maximum")]
    RevisionOverflow,
    /// Revision zero did not use the exact preparation shape.
    #[error("initial model invocation revision must be prepared with no predecessor or transition")]
    InvalidInitialShape,
    /// A successor omitted or unexpectedly added predecessor/transition fields.
    #[error(
        "model invocation successor must contain predecessor, transition, and transition digest"
    )]
    InvalidSuccessorShape,
    /// Compact predecessor crossed the immutable tenant boundary.
    #[error("model invocation predecessor crosses the tenant boundary")]
    PreviousTenantMismatch,
    /// Compact predecessor named another run.
    #[error("model invocation predecessor names another run")]
    PreviousRunMismatch,
    /// Compact predecessor named another logical invocation.
    #[error("model invocation predecessor names another invocation")]
    PreviousInvocationMismatch,
    /// Current revision was not the exact successor of its compact predecessor.
    #[error("model invocation revision {actual} does not follow predecessor {previous}")]
    PreviousRevisionMismatch {
        /// Predecessor revision.
        previous: ModelInvocationRevision,
        /// Rejected current revision.
        actual: ModelInvocationRevision,
    },
    /// Persisted transition checksum did not match its transition payload.
    #[error("model invocation transition digest does not match its payload")]
    TransitionDigestMismatch,
    /// Transition is not legal from the predecessor lifecycle state.
    #[error("model invocation transition {transition:?} is invalid from {status:?}")]
    InvalidTransition {
        /// Predecessor status.
        status: ModelInvocationStatus,
        /// Rejected transition kind.
        transition: ModelInvocationTransitionKind,
    },
    /// Transition payload and resulting state differed.
    #[error("model invocation transition payload does not match its resulting state")]
    TransitionStateMismatch,
    /// A retry reused the immediately preceding physical attempt identity.
    #[error("model invocation retry must use a new physical attempt identifier")]
    ReusedAttemptId,
    /// Failed outcome did not explicitly authorize a safe retry.
    #[error("model invocation failure does not authorize retry")]
    RetryNotAuthorized,
    /// Retry time could not be represented in the canonical timestamp range.
    #[error("model invocation retry delay exceeds the supported timestamp range")]
    RetryDelayOutOfRange,
    /// Retry occurred before the failure's explicit minimum delay elapsed.
    #[error("model invocation retry at {actual} precedes not-before time {not_before}")]
    RetryDelayNotElapsed {
        /// Earliest permitted durable retry timestamp.
        not_before: Timestamp,
        /// Rejected retry timestamp.
        actual: Timestamp,
    },
    /// Successful response named another physical attempt.
    #[error("model response names another physical attempt")]
    ResponseAttemptMismatch,
    /// Successful response did not match the descriptor/request snapshot.
    #[error("model response does not match the invocation: {source}")]
    ResponseBinding {
        /// Exact response validation failure.
        #[source]
        source: ModelResponseError,
    },
    /// Failure evidence did not match attempt, model, mode, or request limits.
    #[error("model failure does not match the invocation: {source}")]
    ErrorBinding {
        /// Exact failure validation error.
        #[source]
        source: ModelErrorValidationError,
    },
    /// A pre-dispatch preparation failure claimed provider usage.
    #[error("model preparation failure cannot contain provider usage")]
    PreparationFailureHasUsage,
    /// Persisted complete record checksum did not match its fields.
    #[error("model invocation record digest does not match its fields")]
    DigestMismatch,
}

impl From<ModelInvocationIntegrityError> for ModelInvocationError {
    fn from(source: ModelInvocationIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Failure to canonicalize a closed model invocation checksum preimage.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelInvocationIntegrityError {
    /// Typed values could not be represented as canonical JSON.
    #[error("model invocation checksum preimage canonical serialization failed")]
    CanonicalSerialization,
}

#[derive(Serialize)]
struct ModelInvocationIntentDigestWire<'a> {
    activation: &'a NodeActivation,
    invocation_id: InvocationId,
    descriptor: &'a ModelDescriptor,
    request: &'a ModelRequest,
}

#[derive(Serialize)]
struct ModelInvocationRecordDigestWire<'a> {
    intent_digest: Digest,
    revision: ModelInvocationRevision,
    state: &'a ModelInvocationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous: Option<&'a ModelInvocationHead>,
    journal_head: &'a JournalHead,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition_digest: Option<Digest>,
}

fn compute_intent_digest(
    value: &ModelInvocationIntentDigestWire<'_>,
) -> Result<Digest, ModelInvocationIntegrityError> {
    domain_separated_digest(INTENT_DIGEST_DOMAIN, value)
}

fn compute_transition_digest(
    value: &ModelInvocationTransition,
) -> Result<Digest, ModelInvocationIntegrityError> {
    domain_separated_digest(TRANSITION_DIGEST_DOMAIN, value)
}

fn compute_record_digest(
    value: &ModelInvocationRecordDigestWire<'_>,
) -> Result<Digest, ModelInvocationIntegrityError> {
    domain_separated_digest(RECORD_DIGEST_DOMAIN, value)
}

fn domain_separated_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, ModelInvocationIntegrityError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| ModelInvocationIntegrityError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn canonical_equal<T: Serialize>(
    left: &T,
    right: &T,
) -> Result<bool, ModelInvocationIntegrityError> {
    let left = serde_json_canonicalizer::to_vec(left)
        .map_err(|_| ModelInvocationIntegrityError::CanonicalSerialization)?;
    let right = serde_json_canonicalizer::to_vec(right)
        .map_err(|_| ModelInvocationIntegrityError::CanonicalSerialization)?;
    Ok(left == right)
}

fn validate_intent_shape(
    descriptor: &ModelDescriptor,
    request: &ModelRequest,
) -> Result<(), ModelInvocationIntentError> {
    if descriptor.metadata().lifecycle().state() == CapabilityLifecycleState::Retired {
        return Err(ModelInvocationIntentError::RetiredModel);
    }
    descriptor
        .capabilities()
        .satisfies(request.requirements())
        .map_err(|mismatch| ModelInvocationIntentError::CapabilityMismatch {
            mismatch: Box::new(mismatch),
        })
}

#[derive(Clone, Copy)]
enum InvocationScopeError {
    Tenant,
    Run,
}

fn validate_journal_scope(
    tenant_id: &TenantId,
    run_id: RunId,
    head: &JournalHead,
) -> Result<(), InvocationScopeError> {
    if head.tenant_id() != tenant_id {
        return Err(InvocationScopeError::Tenant);
    }
    if head.run_id() != run_id {
        return Err(InvocationScopeError::Run);
    }
    Ok(())
}

fn validate_status_attempt(
    status: ModelInvocationStatus,
    attempt_id: Option<AttemptId>,
) -> Result<(), ModelInvocationHeadError> {
    match (status, attempt_id) {
        (ModelInvocationStatus::Prepared, None)
        | (
            ModelInvocationStatus::Executing
            | ModelInvocationStatus::Committed
            | ModelInvocationStatus::Failed,
            Some(_),
        ) => Ok(()),
        (ModelInvocationStatus::Prepared, Some(_)) => {
            Err(ModelInvocationHeadError::PreparedHasAttempt)
        }
        (
            ModelInvocationStatus::Executing
            | ModelInvocationStatus::Committed
            | ModelInvocationStatus::Failed,
            None,
        ) => Err(ModelInvocationHeadError::AttemptMissing),
    }
}

fn validate_revision_status(
    revision: ModelInvocationRevision,
    status: ModelInvocationStatus,
) -> Result<(), ModelInvocationHeadError> {
    match (revision == ModelInvocationRevision::INITIAL, status) {
        (true, ModelInvocationStatus::Prepared)
        | (
            false,
            ModelInvocationStatus::Executing
            | ModelInvocationStatus::Committed
            | ModelInvocationStatus::Failed,
        ) => Ok(()),
        (true, _) => Err(ModelInvocationHeadError::InitialStatusMismatch),
        (false, ModelInvocationStatus::Prepared) => {
            Err(ModelInvocationHeadError::PreparedRevisionMismatch)
        }
    }
}

fn map_scope_error(error: InvocationScopeError) -> ModelInvocationError {
    match error {
        InvocationScopeError::Tenant => ModelInvocationError::JournalTenantMismatch,
        InvocationScopeError::Run => ModelInvocationError::JournalRunMismatch,
    }
}

fn validate_journal_advances(
    previous: &JournalHead,
    actual: &JournalHead,
) -> Result<(), ModelInvocationError> {
    if actual.sequence() <= previous.sequence() {
        return Err(ModelInvocationError::JournalDidNotAdvance {
            previous: previous.sequence(),
            actual: actual.sequence(),
        });
    }
    if actual.recorded_at() < previous.recorded_at() {
        return Err(ModelInvocationError::ClockRegression {
            previous: previous.recorded_at(),
            actual: actual.recorded_at(),
        });
    }
    Ok(())
}

fn validate_preparation_journal(
    intent: &ModelInvocationIntent,
    journal_head: &JournalHead,
) -> Result<(), ModelInvocationError> {
    validate_journal_scope(intent.tenant_id(), intent.run_id(), journal_head)
        .map_err(map_scope_error)?;
    validate_journal_advances(
        intent.activation.base_checkpoint().journal_head(),
        journal_head,
    )
}

fn validate_successor_journal(
    invocation: &ModelInvocation,
    journal_head: &JournalHead,
) -> Result<(), ModelInvocationError> {
    validate_journal_scope(
        invocation.intent.tenant_id(),
        invocation.intent.run_id(),
        journal_head,
    )
    .map_err(map_scope_error)?;
    validate_journal_advances(&invocation.journal_head, journal_head)
}

#[allow(clippy::too_many_arguments)]
fn validate_record_shape(
    intent: &ModelInvocationIntent,
    revision: ModelInvocationRevision,
    state: &ModelInvocationState,
    previous: Option<&ModelInvocationHead>,
    journal_head: &JournalHead,
    transition: Option<&ModelInvocationTransition>,
    transition_digest: Option<Digest>,
) -> Result<(), ModelInvocationError> {
    validate_journal_scope(intent.tenant_id(), intent.run_id(), journal_head)
        .map_err(map_scope_error)?;
    validate_state_binding(intent, state)?;

    if revision == ModelInvocationRevision::INITIAL {
        if !matches!(state, ModelInvocationState::Prepared)
            || previous.is_some()
            || transition.is_some()
            || transition_digest.is_some()
        {
            return Err(ModelInvocationError::InvalidInitialShape);
        }
        return validate_preparation_journal(intent, journal_head);
    }

    let (Some(previous), Some(transition), Some(transition_digest)) =
        (previous, transition, transition_digest)
    else {
        return Err(ModelInvocationError::InvalidSuccessorShape);
    };
    if previous.tenant_id() != intent.tenant_id() {
        return Err(ModelInvocationError::PreviousTenantMismatch);
    }
    if previous.run_id() != intent.run_id() {
        return Err(ModelInvocationError::PreviousRunMismatch);
    }
    if previous.invocation_id() != intent.invocation_id() {
        return Err(ModelInvocationError::PreviousInvocationMismatch);
    }
    if previous.revision().checked_next() != Some(revision) {
        return Err(ModelInvocationError::PreviousRevisionMismatch {
            previous: previous.revision(),
            actual: revision,
        });
    }
    validate_journal_advances(previous.journal_head(), journal_head)?;

    if compute_transition_digest(transition)? != transition_digest {
        return Err(ModelInvocationError::TransitionDigestMismatch);
    }
    validate_transition_shape(intent, previous, transition, state)
}

fn validate_state_binding(
    intent: &ModelInvocationIntent,
    state: &ModelInvocationState,
) -> Result<(), ModelInvocationError> {
    match state {
        ModelInvocationState::Prepared | ModelInvocationState::Executing { .. } => Ok(()),
        ModelInvocationState::Committed { response } => {
            validate_response_binding(intent, response.provenance().attempt_id(), response)
        }
        ModelInvocationState::Failed { error } => {
            validate_error_binding(intent, error.provenance().attempt_id(), error)
        }
    }
}

fn validate_response_binding(
    intent: &ModelInvocationIntent,
    expected_attempt: AttemptId,
    response: &ModelResponse,
) -> Result<(), ModelInvocationError> {
    if response.provenance().attempt_id() != expected_attempt {
        return Err(ModelInvocationError::ResponseAttemptMismatch);
    }
    response
        .validate_for(&intent.descriptor, &intent.request)
        .map_err(|source| ModelInvocationError::ResponseBinding { source })
}

fn validate_error_binding(
    intent: &ModelInvocationIntent,
    expected_attempt: AttemptId,
    error: &ModelError,
) -> Result<(), ModelInvocationError> {
    error
        .validate_for_attempt(expected_attempt, &intent.descriptor, &intent.request)
        .map_err(|source| ModelInvocationError::ErrorBinding { source })?;
    if error.phase() == ModelErrorPhase::Preparation && error.usage().is_some() {
        return Err(ModelInvocationError::PreparationFailureHasUsage);
    }
    Ok(())
}

fn validate_transition_shape(
    intent: &ModelInvocationIntent,
    previous: &ModelInvocationHead,
    transition: &ModelInvocationTransition,
    state: &ModelInvocationState,
) -> Result<(), ModelInvocationError> {
    let valid = match (previous.status(), transition, state) {
        (
            ModelInvocationStatus::Prepared | ModelInvocationStatus::Failed,
            ModelInvocationTransition::StartAttempt { attempt_id },
            ModelInvocationState::Executing {
                attempt_id: state_attempt,
            },
        ) => {
            if previous.attempt_id() == Some(*attempt_id) {
                return Err(ModelInvocationError::ReusedAttemptId);
            }
            attempt_id == state_attempt
        }
        (
            ModelInvocationStatus::Executing,
            ModelInvocationTransition::RecordResponse { response },
            ModelInvocationState::Committed {
                response: state_response,
            },
        ) => canonical_equal(response, state_response)?,
        (
            ModelInvocationStatus::Executing,
            ModelInvocationTransition::RecordError { error },
            ModelInvocationState::Failed { error: state_error },
        ) => canonical_equal(error, state_error)?,
        _ => {
            return Err(ModelInvocationError::InvalidTransition {
                status: previous.status(),
                transition: transition.kind(),
            });
        }
    };
    if !valid {
        return Err(ModelInvocationError::TransitionStateMismatch);
    }

    let expected_attempt = previous.attempt_id();
    match transition {
        ModelInvocationTransition::StartAttempt { .. } => {}
        ModelInvocationTransition::RecordResponse { response } => {
            validate_response_binding(
                intent,
                expected_attempt.ok_or(ModelInvocationError::TransitionStateMismatch)?,
                response,
            )?;
        }
        ModelInvocationTransition::RecordError { error } => {
            validate_error_binding(
                intent,
                expected_attempt.ok_or(ModelInvocationError::TransitionStateMismatch)?,
                error,
            )?;
        }
    }
    Ok(())
}

fn apply_transition(
    invocation: &ModelInvocation,
    transition: &ModelInvocationTransition,
    recorded_at: Timestamp,
) -> Result<ModelInvocationState, ModelInvocationError> {
    match (&invocation.state, transition) {
        (
            ModelInvocationState::Prepared,
            ModelInvocationTransition::StartAttempt { attempt_id },
        ) => Ok(ModelInvocationState::Executing {
            attempt_id: *attempt_id,
        }),
        (
            ModelInvocationState::Failed { error },
            ModelInvocationTransition::StartAttempt { attempt_id },
        ) => {
            let previous_attempt = error.provenance().attempt_id();
            if previous_attempt == *attempt_id {
                return Err(ModelInvocationError::ReusedAttemptId);
            }
            validate_retry(invocation, error, recorded_at)?;
            Ok(ModelInvocationState::Executing {
                attempt_id: *attempt_id,
            })
        }
        (
            ModelInvocationState::Executing { attempt_id },
            ModelInvocationTransition::RecordResponse { response },
        ) => {
            validate_response_binding(&invocation.intent, *attempt_id, response)?;
            Ok(ModelInvocationState::Committed {
                response: response.clone(),
            })
        }
        (
            ModelInvocationState::Executing { attempt_id },
            ModelInvocationTransition::RecordError { error },
        ) => {
            validate_error_binding(&invocation.intent, *attempt_id, error)?;
            Ok(ModelInvocationState::Failed {
                error: error.clone(),
            })
        }
        _ => Err(ModelInvocationError::InvalidTransition {
            status: invocation.status(),
            transition: transition.kind(),
        }),
    }
}

fn validate_retry(
    invocation: &ModelInvocation,
    error: &ModelError,
    recorded_at: Timestamp,
) -> Result<(), ModelInvocationError> {
    let Some(delay) = error.failure().retry_advice().safe_after_delay() else {
        return Err(ModelInvocationError::RetryNotAuthorized);
    };
    let not_before_micros = i128::from(invocation.journal_head.recorded_at().unix_micros())
        + i128::from(delay.as_i64()) * 1_000;
    if not_before_micros > i128::from(Timestamp::MAX.unix_micros()) {
        return Err(ModelInvocationError::RetryDelayOutOfRange);
    }
    let not_before = Timestamp::from_unix_micros(
        i64::try_from(not_before_micros).map_err(|_| ModelInvocationError::RetryDelayOutOfRange)?,
    )
    .map_err(|_| ModelInvocationError::RetryDelayOutOfRange)?;
    if recorded_at < not_before {
        return Err(ModelInvocationError::RetryDelayNotElapsed {
            not_before,
            actual: recorded_at,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use schemars::schema_for;
    use serde_json::{Value, from_value, json, to_value};

    use super::*;
    use crate::{
        Checkpoint, DurationMillis, EventId, Failure, FailureCategory, FailureCode, FailureId,
        FailureMessage, FailureOrigin, GraphNamespace, ModelErrorProvenance, ModelUsage, NodeId,
        RetryAdvice,
    };

    fn fixture(path: &[&str], source: &str) -> Value {
        let mut value: Value = serde_json::from_str(source).unwrap();
        for component in path {
            value = match component.parse::<usize>() {
                Ok(index) => value[index].clone(),
                Err(_) => value[*component].clone(),
            };
        }
        value
    }

    fn checkpoint() -> Checkpoint {
        from_value(fixture(
            &["checkpoints", "0"],
            include_str!("../tests/fixtures/core-checkpoint-v1.json"),
        ))
        .unwrap()
    }

    fn descriptor() -> ModelDescriptor {
        from_value(fixture(
            &["descriptors", "valid", "0", "model"],
            include_str!("../tests/fixtures/core-agent-v1.json"),
        ))
        .unwrap()
    }

    fn request() -> ModelRequest {
        from_value(fixture(
            &["requests", "valid", "0"],
            include_str!("../tests/fixtures/core-model-request-v1.json"),
        ))
        .unwrap()
    }

    fn invocation_id() -> InvocationId {
        "01912345-6789-7abc-8def-0123456789d0".parse().unwrap()
    }

    fn attempt(suffix: &str) -> AttemptId {
        format!("01912345-6789-7abc-8def-0123456789{suffix}")
            .parse()
            .unwrap()
    }

    fn intent() -> ModelInvocationIntent {
        let checkpoint = checkpoint();
        ModelInvocationIntent::new(
            NodeActivation::new(
                checkpoint.head(),
                GraphNamespace::root(),
                NodeId::new("reason").unwrap(),
                Digest::sha256(b"model-node-input"),
            ),
            invocation_id(),
            descriptor(),
            request(),
        )
        .unwrap()
    }

    fn journal(intent: &ModelInvocationIntent, sequence: u64) -> JournalHead {
        let base = intent
            .activation()
            .base_checkpoint()
            .journal_head()
            .recorded_at();
        let offset = i64::try_from(sequence - 1).unwrap() * 1_000_000;
        journal_at(
            intent,
            sequence,
            Timestamp::from_unix_micros(base.unix_micros() + offset).unwrap(),
        )
    }

    fn journal_at(
        intent: &ModelInvocationIntent,
        sequence: u64,
        recorded_at: Timestamp,
    ) -> JournalHead {
        let event_id: EventId =
            format!("01912345-6789-7abc-8def-0123456789{:02x}", 0xd0 + sequence)
                .parse()
                .unwrap();
        JournalHead::new(
            intent.tenant_id().clone(),
            intent.run_id(),
            JournalSequence::new(sequence).unwrap(),
            event_id,
            recorded_at,
            Digest::sha256(sequence.to_be_bytes()),
        )
    }

    fn response(attempt_id: AttemptId) -> ModelResponse {
        let descriptor = descriptor();
        let request = request();
        let mut value = fixture(
            &["responses", "valid", "0"],
            include_str!("../tests/fixtures/core-model-response-v1.json"),
        );
        value["provenance"]["attempt_id"] = json!(attempt_id);
        value["provenance"]["model"] = to_value(descriptor.metadata().identity()).unwrap();
        let response = from_value::<ModelResponse>(value).unwrap();
        response.validate_for(&descriptor, &request).unwrap();
        response
    }

    fn model_error(
        attempt_id: AttemptId,
        phase: ModelErrorPhase,
        retry_advice: RetryAdvice,
        usage: Option<ModelUsage>,
    ) -> ModelError {
        let descriptor = descriptor();
        let failure = Failure::new(
            "01912345-6789-7abc-8def-0123456789b8"
                .parse::<FailureId>()
                .unwrap(),
            FailureCategory::DependencyUnavailable,
            FailureCode::new("model.dependency_unavailable").unwrap(),
            FailureOrigin::new("model.provider").unwrap(),
            FailureMessage::new("The model provider is temporarily unavailable.").unwrap(),
            retry_advice,
        )
        .unwrap();
        ModelError::new(
            failure,
            phase,
            ModelErrorProvenance::new(
                attempt_id,
                descriptor.metadata().identity().clone(),
                None,
                None,
                None,
            ),
            usage,
        )
    }

    fn prepared() -> ModelInvocation {
        let intent = intent();
        let head = journal(&intent, 2);
        ModelInvocation::prepare(intent, head).unwrap()
    }

    #[test]
    fn revision_and_intent_wires_are_canonical_negotiated_and_bound() {
        for value in [0, 1, i64::MAX as u64] {
            let revision = ModelInvocationRevision::new(value).unwrap();
            assert_eq!(to_value(revision).unwrap(), json!(value.to_string()));
            assert_eq!(
                from_value::<ModelInvocationRevision>(json!(value.to_string())).unwrap(),
                revision
            );
        }
        for invalid in [
            json!(""),
            json!("01"),
            json!("9223372036854775808"),
            json!(1),
        ] {
            assert!(from_value::<ModelInvocationRevision>(invalid).is_err());
        }

        let intent = intent();
        let wire = to_value(&intent).unwrap();
        assert_eq!(
            from_value::<ModelInvocationIntent>(wire.clone()).unwrap(),
            intent
        );

        let mut tampered = wire;
        tampered["request"]["instructions"][0]["content"]["content"]["text"] =
            json!("changed prompt");
        assert!(from_value::<ModelInvocationIntent>(tampered).is_err());

        let mut excessive = to_value(request()).unwrap();
        excessive["limits"]["max_output_tokens"] = json!("20000");
        excessive["requirements"]["min_output_tokens"] = json!("20000");
        excessive["requirements"]["min_context_tokens"] = json!("28192");
        let excessive = from_value::<ModelRequest>(excessive).unwrap();
        assert!(matches!(
            ModelInvocationIntent::new(
                intent.activation().clone(),
                InvocationId::generate(),
                descriptor(),
                excessive,
            ),
            Err(ModelInvocationIntentError::CapabilityMismatch { .. })
        ));

        let mut retired = to_value(descriptor()).unwrap();
        retired["metadata"]["lifecycle"] = json!({
            "status": "retired",
            "retired_at": "2027-02-28T00:00:00.000001Z",
            "notice": "This binding is retained only for durable history."
        });
        let retired = from_value::<ModelDescriptor>(retired).unwrap();
        assert_eq!(
            ModelInvocationIntent::new(
                intent.activation().clone(),
                InvocationId::generate(),
                retired,
                request(),
            ),
            Err(ModelInvocationIntentError::RetiredModel)
        );
    }

    #[test]
    fn prepared_attempt_and_response_form_one_verified_terminal_history() {
        let prepared = prepared();
        let attempt_id = attempt("ab");
        let executing = prepared
            .advance(
                ModelInvocationTransition::StartAttempt { attempt_id },
                journal(prepared.intent(), 3),
            )
            .unwrap();
        let committed = executing
            .advance(
                ModelInvocationTransition::RecordResponse {
                    response: response(attempt_id),
                },
                journal(executing.intent(), 4),
            )
            .unwrap();

        assert_eq!(prepared.status(), ModelInvocationStatus::Prepared);
        assert_eq!(executing.status(), ModelInvocationStatus::Executing);
        assert_eq!(committed.status(), ModelInvocationStatus::Committed);
        assert_eq!(committed.attempt_id(), Some(attempt_id));
        assert!(matches!(
            committed.advance(
                ModelInvocationTransition::StartAttempt {
                    attempt_id: attempt("ac"),
                },
                journal(committed.intent(), 5),
            ),
            Err(ModelInvocationError::InvalidTransition { .. })
        ));

        let restored = from_value::<ModelInvocation>(to_value(&committed).unwrap()).unwrap();
        assert_eq!(to_value(restored).unwrap(), to_value(&committed).unwrap());
        let mut verifier = ModelInvocationHistoryVerifier::new();
        for record in [&prepared, &executing, &committed] {
            verifier.verify_next(record).unwrap();
        }
        assert_eq!(verifier.head(), Some(committed.head()));
    }

    #[test]
    fn failed_attempt_retry_requires_new_identity_explicit_advice_and_durable_delay() {
        let prepared = prepared();
        let first_attempt = attempt("ab");
        let executing = prepared
            .advance(
                ModelInvocationTransition::StartAttempt {
                    attempt_id: first_attempt,
                },
                journal(prepared.intent(), 3),
            )
            .unwrap();
        let failed = executing
            .advance(
                ModelInvocationTransition::RecordError {
                    error: model_error(
                        first_attempt,
                        ModelErrorPhase::Dispatch,
                        RetryAdvice::SafeAfter {
                            delay: DurationMillis::new(1_000).unwrap(),
                        },
                        None,
                    ),
                },
                journal(executing.intent(), 4),
            )
            .unwrap();

        assert!(matches!(
            failed.advance(
                ModelInvocationTransition::StartAttempt {
                    attempt_id: first_attempt,
                },
                journal(failed.intent(), 5),
            ),
            Err(ModelInvocationError::ReusedAttemptId)
        ));

        let too_early = journal_at(
            failed.intent(),
            5,
            Timestamp::from_unix_micros(
                failed.journal_head().recorded_at().unix_micros() + 999_999,
            )
            .unwrap(),
        );
        assert!(matches!(
            failed.advance(
                ModelInvocationTransition::StartAttempt {
                    attempt_id: attempt("ac"),
                },
                too_early,
            ),
            Err(ModelInvocationError::RetryDelayNotElapsed { .. })
        ));
        let retried = failed
            .advance(
                ModelInvocationTransition::StartAttempt {
                    attempt_id: attempt("ac"),
                },
                journal(failed.intent(), 5),
            )
            .unwrap();
        assert_eq!(retried.status(), ModelInvocationStatus::Executing);

        let never_executing = prepared
            .advance(
                ModelInvocationTransition::StartAttempt {
                    attempt_id: attempt("ad"),
                },
                journal(prepared.intent(), 3),
            )
            .unwrap();
        let never_failed = never_executing
            .advance(
                ModelInvocationTransition::RecordError {
                    error: model_error(
                        attempt("ad"),
                        ModelErrorPhase::Preparation,
                        RetryAdvice::Never,
                        None,
                    ),
                },
                journal(never_executing.intent(), 4),
            )
            .unwrap();
        assert!(matches!(
            never_failed.advance(
                ModelInvocationTransition::StartAttempt {
                    attempt_id: attempt("ae"),
                },
                journal(never_failed.intent(), 5),
            ),
            Err(ModelInvocationError::RetryNotAuthorized)
        ));
    }

    #[test]
    fn attempt_result_error_and_integrity_substitution_fail_closed() {
        let prepared = prepared();
        let attempt_id = attempt("ab");
        let executing = prepared
            .advance(
                ModelInvocationTransition::StartAttempt { attempt_id },
                journal(prepared.intent(), 3),
            )
            .unwrap();

        assert!(matches!(
            executing.advance(
                ModelInvocationTransition::RecordResponse {
                    response: response(attempt("ac")),
                },
                journal(executing.intent(), 4),
            ),
            Err(ModelInvocationError::ResponseAttemptMismatch)
        ));
        assert!(matches!(
            executing.advance(
                ModelInvocationTransition::RecordError {
                    error: model_error(
                        attempt("ac"),
                        ModelErrorPhase::Dispatch,
                        RetryAdvice::Never,
                        None,
                    ),
                },
                journal(executing.intent(), 4),
            ),
            Err(ModelInvocationError::ErrorBinding {
                source: ModelErrorValidationError::AttemptMismatch { .. }
            })
        ));
        assert!(matches!(
            executing.advance(
                ModelInvocationTransition::RecordError {
                    error: model_error(
                        attempt_id,
                        ModelErrorPhase::Preparation,
                        RetryAdvice::Never,
                        Some(response(attempt_id).usage().clone()),
                    ),
                },
                journal(executing.intent(), 4),
            ),
            Err(ModelInvocationError::PreparationFailureHasUsage)
        ));

        let mut wire = to_value(&executing).unwrap();
        wire["state"]["attempt_id"] = json!(attempt("ac"));
        assert!(from_value::<ModelInvocation>(wire).is_err());
        let mut wire = to_value(&executing).unwrap();
        wire["transition"]["attempt_id"] = json!(attempt("ac"));
        assert!(from_value::<ModelInvocation>(wire).is_err());

        let mut verifier = ModelInvocationHistoryVerifier::new();
        assert!(matches!(
            verifier.verify_next(&executing),
            Err(ModelInvocationHistoryError::FirstRecordNotInitial { .. })
        ));
        assert!(!verifier.has_records());
    }

    #[test]
    fn public_model_invocation_schemas_are_closed() {
        for schema in [
            to_value(schema_for!(ModelInvocationIntent)).unwrap(),
            to_value(schema_for!(ModelInvocationHead)).unwrap(),
            to_value(schema_for!(ModelInvocation)).unwrap(),
        ] {
            assert_eq!(schema["additionalProperties"], Value::Bool(false));
        }
    }
}
