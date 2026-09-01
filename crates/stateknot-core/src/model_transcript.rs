// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable, lossless model/tool continuation evidence.

use std::{collections::BTreeSet, fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{
    AttemptId, BoundedJson, ByteCount, CapabilityIdentity, Digest, FailureCategory, FailureCode,
    FailureId, FailureMessage, InvocationId, ModelFinishReason, ModelProviderToolCallId,
    ModelResponse, RetryAdvice, ToolError, ToolExternalEffect, ToolResult,
};

const MEBIBYTE: u64 = 1024 * 1024;
const FORMAT_PATTERN: &str = "^[a-z][a-z0-9]*(\\.[a-z0-9]+){2,7}$";
const REPLAY_DIGEST_DOMAIN: &[u8] = b"stateknot.model-provider-replay.v1\0";

/// Versioned adapter-owned syntax for one exact provider replay fragment.
///
/// The identifier is data, not executable dispatch authority. A selected
/// adapter must explicitly recognize the exact value before provider I/O.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ModelProviderReplayFormat(Box<str>);

impl ModelProviderReplayFormat {
    /// Maximum encoded format identifier length.
    pub const MAX_BYTES: usize = 128;

    /// Validates one lowercase dot-separated adapter format identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ModelProviderReplayFormatError`] for an empty, oversized, or
    /// syntactically unstable identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, ModelProviderReplayFormatError> {
        let value = value.into();
        validate_format(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact case-sensitive identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ModelProviderReplayFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ModelProviderReplayFormat")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for ModelProviderReplayFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ModelProviderReplayFormat {
    type Err = ModelProviderReplayFormatError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ModelProviderReplayFormat {
    type Error = ModelProviderReplayFormatError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for ModelProviderReplayFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ModelProviderReplayFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

impl JsonSchema for ModelProviderReplayFormat {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelProviderReplayFormat".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ModelProviderReplayFormat").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 5,
            "maxLength": 128,
            "pattern": FORMAT_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

fn validate_format(value: &str) -> Result<(), ModelProviderReplayFormatError> {
    if value.is_empty() {
        return Err(ModelProviderReplayFormatError::Empty);
    }
    if value.len() > ModelProviderReplayFormat::MAX_BYTES {
        return Err(ModelProviderReplayFormatError::TooLong {
            maximum: ModelProviderReplayFormat::MAX_BYTES,
            actual: value.len(),
        });
    }
    let segments = value.split('.').collect::<Vec<_>>();
    if !(3..=8).contains(&segments.len())
        || segments.iter().any(|segment| {
            segment.is_empty()
                || !segment.as_bytes()[0].is_ascii_lowercase()
                || segment
                    .bytes()
                    .any(|byte| !byte.is_ascii_lowercase() && !byte.is_ascii_digit())
        })
    {
        return Err(ModelProviderReplayFormatError::InvalidSyntax);
    }
    Ok(())
}

/// Invalid provider replay format identifier.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelProviderReplayFormatError {
    /// No identifier was supplied.
    #[error("model provider replay format must not be empty")]
    Empty,
    /// The encoded identifier exceeded the immutable ceiling.
    #[error("model provider replay format is {actual} bytes; maximum is {maximum}")]
    TooLong {
        /// Immutable byte ceiling.
        maximum: usize,
        /// Rejected encoded length.
        actual: usize,
    },
    /// The identifier was not a stable lowercase dot-separated value.
    #[error("model provider replay format has invalid syntax")]
    InvalidSyntax,
}

/// Exact bounded provider response fragment required for stateless continuation.
///
/// The payload is deliberately opaque to core semantics. Adapters validate its
/// complete provider shape when producing and consuming it. The
/// domain-separated digest binds both the format and canonical payload so a
/// stored fragment cannot be reinterpreted as another continuation syntax.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProviderReplay {
    format: ModelProviderReplayFormat,
    payload: BoundedJson,
    digest: Digest,
}

impl ModelProviderReplay {
    /// Constructs an array-root replay fragment and computes its digest.
    ///
    /// # Errors
    ///
    /// Returns [`ModelProviderReplayError`] unless the payload is an array.
    pub fn new(
        format: ModelProviderReplayFormat,
        payload: BoundedJson,
    ) -> Result<Self, ModelProviderReplayError> {
        if !payload.as_value().is_array() {
            return Err(ModelProviderReplayError::PayloadMustBeArray);
        }
        let digest = replay_digest(&format, &payload);
        Ok(Self {
            format,
            payload,
            digest,
        })
    }

    /// Returns the exact adapter-owned format identity.
    #[must_use]
    pub const fn format(&self) -> &ModelProviderReplayFormat {
        &self.format
    }

    /// Returns the opaque bounded array without permitting mutation.
    #[must_use]
    pub const fn payload(&self) -> &BoundedJson {
        &self.payload
    }

    /// Returns the digest binding format and canonical payload bytes.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns compact payload bytes retained by this fragment.
    #[must_use]
    pub fn payload_bytes(&self) -> ByteCount {
        ByteCount::new(self.payload.stats().compact_bytes() as u64)
    }
}

#[derive(Serialize)]
struct ReplayDigestWire<'a> {
    format: &'a ModelProviderReplayFormat,
    payload: &'a BoundedJson,
}

fn replay_digest(format: &ModelProviderReplayFormat, payload: &BoundedJson) -> Digest {
    let canonical = serde_json_canonicalizer::to_vec(&ReplayDigestWire { format, payload })
        .expect("bounded provider replay always has canonical JSON");
    let mut preimage = Vec::with_capacity(REPLAY_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(REPLAY_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Digest::sha256(preimage)
}

impl fmt::Debug for ModelProviderReplay {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelProviderReplay")
            .field("format", &self.format)
            .field("payload_stats", &self.payload.stats())
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ModelProviderReplay {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            format: ModelProviderReplayFormat,
            payload: BoundedJson,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        let replay = Self::new(wire.format, wire.payload).map_err(de::Error::custom)?;
        if replay.digest != wire.digest {
            return Err(de::Error::custom(ModelProviderReplayError::DigestMismatch));
        }
        Ok(replay)
    }
}

/// Invalid opaque provider replay evidence.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelProviderReplayError {
    /// Provider continuation fragments are ordered arrays.
    #[error("model provider replay payload must be a JSON array")]
    PayloadMustBeArray,
    /// Persisted digest did not match canonical payload bytes.
    #[error("model provider replay digest does not match its payload")]
    DigestMismatch,
}

/// Public-safe durable failure snapshot supplied back to a model.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolFailure {
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    tool: CapabilityIdentity,
    failure_id: FailureId,
    category: FailureCategory,
    code: FailureCode,
    message: FailureMessage,
    retry_advice: RetryAdvice,
    external_effect: ToolExternalEffect,
}

impl ModelToolFailure {
    /// Copies only public-safe, identity-bound evidence from a tool error.
    #[must_use]
    pub fn from_tool_error(error: &ToolError) -> Self {
        Self {
            invocation_id: error.provenance().invocation_id(),
            attempt_id: error.provenance().attempt_id(),
            tool: error.provenance().tool().clone(),
            failure_id: error.failure().id(),
            category: error.failure().category(),
            code: error.failure().code().clone(),
            message: error.failure().message().clone(),
            retry_advice: error.failure().retry_advice(),
            external_effect: error.external_effect(),
        }
    }

    fn validate(&self) -> Result<(), ModelToolFailureError> {
        let ambiguous = self.category == FailureCategory::AmbiguousExternalOutcome;
        if ambiguous != matches!(self.retry_advice, RetryAdvice::ReconcileFirst)
            || ambiguous != (self.external_effect == ToolExternalEffect::Unknown)
        {
            return Err(ModelToolFailureError::InvalidAmbiguityEvidence);
        }
        Ok(())
    }

    /// Returns the durable logical invocation identity.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the physical failed attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the exact tool descriptor identity.
    #[must_use]
    pub const fn tool(&self) -> &CapabilityIdentity {
        &self.tool
    }

    /// Returns the public failure occurrence identity.
    #[must_use]
    pub const fn failure_id(&self) -> FailureId {
        self.failure_id
    }

    /// Returns the public failure category.
    #[must_use]
    pub const fn category(&self) -> FailureCategory {
        self.category
    }

    /// Returns the stable public failure code.
    #[must_use]
    pub const fn code(&self) -> &FailureCode {
        &self.code
    }

    /// Returns the bounded public-safe failure message.
    #[must_use]
    pub const fn message(&self) -> &FailureMessage {
        &self.message
    }

    /// Returns explicit retry advice from the originating tool boundary.
    #[must_use]
    pub const fn retry_advice(&self) -> RetryAdvice {
        self.retry_advice
    }

    /// Returns authoritative external-effect evidence.
    #[must_use]
    pub const fn external_effect(&self) -> ToolExternalEffect {
        self.external_effect
    }
}

impl<'de> Deserialize<'de> for ModelToolFailure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            invocation_id: InvocationId,
            attempt_id: AttemptId,
            tool: CapabilityIdentity,
            failure_id: FailureId,
            category: FailureCategory,
            code: FailureCode,
            message: FailureMessage,
            retry_advice: RetryAdvice,
            external_effect: ToolExternalEffect,
        }
        let wire = Wire::deserialize(deserializer)?;
        let failure = Self {
            invocation_id: wire.invocation_id,
            attempt_id: wire.attempt_id,
            tool: wire.tool,
            failure_id: wire.failure_id,
            category: wire.category,
            code: wire.code,
            message: wire.message,
            retry_advice: wire.retry_advice,
            external_effect: wire.external_effect,
        };
        failure.validate().map_err(de::Error::custom)?;
        Ok(failure)
    }
}

/// Invalid public-safe tool failure continuation evidence.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelToolFailureError {
    /// Ambiguous category, reconcile advice, and unknown effect must be exact peers.
    #[error("model tool failure ambiguity evidence is inconsistent")]
    InvalidAmbiguityEvidence,
}

/// One exact tool outcome paired to a provider call identifier.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelToolOutcome {
    /// A committed successful tool invocation.
    Succeeded {
        /// Exact provider call identity from the preceding model output.
        provider_call_id: ModelProviderToolCallId,
        /// Complete durable tool result.
        result: ToolResult,
    },
    /// A committed known failure safe to expose to the next model turn.
    Failed {
        /// Exact provider call identity from the preceding model output.
        provider_call_id: ModelProviderToolCallId,
        /// Redacted durable failure evidence.
        error: ModelToolFailure,
    },
}

impl ModelToolOutcome {
    /// Constructs one successful provider-call outcome.
    #[must_use]
    pub const fn succeeded(provider_call_id: ModelProviderToolCallId, result: ToolResult) -> Self {
        Self::Succeeded {
            provider_call_id,
            result,
        }
    }

    /// Constructs one public-safe failed provider-call outcome.
    #[must_use]
    pub fn failed(provider_call_id: ModelProviderToolCallId, error: &ToolError) -> Self {
        Self::Failed {
            provider_call_id,
            error: ModelToolFailure::from_tool_error(error),
        }
    }

    /// Returns the provider call identity being answered.
    #[must_use]
    pub const fn provider_call_id(&self) -> &ModelProviderToolCallId {
        match self {
            Self::Succeeded {
                provider_call_id, ..
            }
            | Self::Failed {
                provider_call_id, ..
            } => provider_call_id,
        }
    }

    /// Returns the durable logical tool invocation identity.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        match self {
            Self::Succeeded { result, .. } => result.provenance().invocation_id(),
            Self::Failed { error, .. } => error.invocation_id(),
        }
    }

    /// Returns the exact physical tool attempt that produced this outcome.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        match self {
            Self::Succeeded { result, .. } => result.provenance().attempt_id(),
            Self::Failed { error, .. } => error.attempt_id(),
        }
    }

    /// Returns the exact tool descriptor identity.
    #[must_use]
    pub const fn tool(&self) -> &CapabilityIdentity {
        match self {
            Self::Succeeded { result, .. } => result.provenance().tool(),
            Self::Failed { error, .. } => error.tool(),
        }
    }

    /// Returns a committed successful result when present.
    #[must_use]
    pub const fn result(&self) -> Option<&ToolResult> {
        match self {
            Self::Succeeded { result, .. } => Some(result),
            Self::Failed { .. } => None,
        }
    }

    /// Returns public-safe committed failure evidence when present.
    #[must_use]
    pub const fn error(&self) -> Option<&ModelToolFailure> {
        match self {
            Self::Succeeded { .. } => None,
            Self::Failed { error, .. } => Some(error),
        }
    }

    /// Returns whether this outcome represents a known failure.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }
}

impl fmt::Debug for ModelToolOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelToolOutcome")
            .field("provider_call_id", self.provider_call_id())
            .field("invocation_id", &self.invocation_id())
            .field("tool", self.tool())
            .field("is_error", &self.is_error())
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ModelToolOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Succeeded {
                provider_call_id: ModelProviderToolCallId,
                result: ToolResult,
            },
            Failed {
                provider_call_id: ModelProviderToolCallId,
                error: ModelToolFailure,
            },
        }
        Ok(match Wire::deserialize(deserializer)? {
            Wire::Succeeded {
                provider_call_id,
                result,
            } => Self::Succeeded {
                provider_call_id,
                result,
            },
            Wire::Failed {
                provider_call_id,
                error,
            } => Self::Failed {
                provider_call_id,
                error,
            },
        })
    }
}

/// One completed model tool-call turn and its complete durable outcomes.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTranscriptTurn {
    response: ModelResponse,
    outcomes: Box<[ModelToolOutcome]>,
}

impl ModelTranscriptTurn {
    /// Binds every provider call to exactly one durable tool outcome in model order.
    ///
    /// # Errors
    ///
    /// Rejects missing replay data, missing or substituted provider call IDs,
    /// tool identity drift, duplicate invocation IDs, and ambiguous external
    /// write outcomes that require reconciliation rather than model continuation.
    pub fn new<I>(response: ModelResponse, outcomes: I) -> Result<Self, ModelTranscriptTurnError>
    where
        I: IntoIterator<Item = ModelToolOutcome>,
    {
        if response.finish_reason() != ModelFinishReason::ToolCalls {
            return Err(ModelTranscriptTurnError::ResponseHasNoToolCalls);
        }
        if response.provider_replay().is_none() {
            return Err(ModelTranscriptTurnError::MissingProviderReplay);
        }
        let outcomes = outcomes.into_iter().collect::<Vec<_>>();
        if outcomes.len() != response.tool_call_count() {
            return Err(ModelTranscriptTurnError::OutcomeCountMismatch {
                expected: response.tool_call_count(),
                actual: outcomes.len(),
            });
        }
        let mut invocation_ids = BTreeSet::new();
        for (index, (proposal, outcome)) in response.tool_calls().zip(&outcomes).enumerate() {
            let expected_call_id = proposal
                .provider_call_id()
                .ok_or(ModelTranscriptTurnError::MissingProviderCallId { index })?;
            if outcome.provider_call_id() != expected_call_id {
                return Err(ModelTranscriptTurnError::ProviderCallIdMismatch { index });
            }
            if outcome.tool() != proposal.tool() {
                return Err(ModelTranscriptTurnError::ToolIdentityMismatch {
                    index,
                    expected: Box::new(proposal.tool().clone()),
                    actual: Box::new(outcome.tool().clone()),
                });
            }
            if !invocation_ids.insert(outcome.invocation_id()) {
                return Err(ModelTranscriptTurnError::DuplicateInvocationId {
                    invocation_id: outcome.invocation_id(),
                });
            }
            if outcome
                .error()
                .is_some_and(|error| error.external_effect() == ToolExternalEffect::Unknown)
            {
                return Err(ModelTranscriptTurnError::AmbiguousExternalOutcome { index });
            }
        }
        Ok(Self {
            response,
            outcomes: outcomes.into_boxed_slice(),
        })
    }

    /// Returns the complete normalized preceding response.
    #[must_use]
    pub const fn response(&self) -> &ModelResponse {
        &self.response
    }

    /// Returns complete tool outcomes in proposal order.
    #[must_use]
    pub const fn outcomes(&self) -> &[ModelToolOutcome] {
        &self.outcomes
    }
}

impl fmt::Debug for ModelTranscriptTurn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelTranscriptTurn")
            .field("attempt_id", &self.response.provenance().attempt_id())
            .field("model", self.response.provenance().model())
            .field(
                "provider_replay",
                self.response
                    .provider_replay()
                    .expect("constructor invariant"),
            )
            .field("outcomes", &self.outcomes)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ModelTranscriptTurn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            response: ModelResponse,
            outcomes: Vec<ModelToolOutcome>,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.response, wire.outcomes).map_err(de::Error::custom)
    }
}

/// Invalid model/tool transcript turn.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelTranscriptTurnError {
    /// Only a tool-calling response can precede tool results.
    #[error("model transcript turn response must finish with tool calls")]
    ResponseHasNoToolCalls,
    /// Stateless continuation requires exact provider output evidence.
    #[error("model transcript turn is missing provider replay evidence")]
    MissingProviderReplay,
    /// The complete proposal set was not answered exactly once.
    #[error("model transcript expects {expected} tool outcomes, received {actual}")]
    OutcomeCountMismatch {
        /// Number of model proposals.
        expected: usize,
        /// Number of supplied outcomes.
        actual: usize,
    },
    /// One model proposal omitted its provider correlation identity.
    #[error("model transcript proposal {index} has no provider call identifier")]
    MissingProviderCallId {
        /// Zero-based proposal position.
        index: usize,
    },
    /// An outcome answered another provider call.
    #[error("model transcript outcome {index} provider call identifier does not match")]
    ProviderCallIdMismatch {
        /// Zero-based proposal position.
        index: usize,
    },
    /// An outcome claimed another registered tool version.
    #[error("model transcript outcome {index} tool identity does not match its proposal")]
    ToolIdentityMismatch {
        /// Zero-based proposal position.
        index: usize,
        /// Exact proposed tool identity.
        expected: Box<CapabilityIdentity>,
        /// Rejected outcome identity.
        actual: Box<CapabilityIdentity>,
    },
    /// One logical tool invocation was reused for two proposals.
    #[error("model transcript repeats tool invocation {invocation_id}")]
    DuplicateInvocationId {
        /// Reused durable invocation identity.
        invocation_id: InvocationId,
    },
    /// Unknown external effects must be reconciled before continuation.
    #[error("model transcript outcome {index} has an ambiguous external effect")]
    AmbiguousExternalOutcome {
        /// Zero-based proposal position.
        index: usize,
    },
}

/// Bounded ordered prior model/tool turns for one stateless request.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct ModelTranscript {
    turns: Box<[ModelTranscriptTurn]>,
    payload_bytes: ByteCount,
}

impl ModelTranscript {
    /// Maximum retained prior tool-calling turns.
    pub const MAX_TURNS: usize = 1024;
    /// Maximum canonical transcript bytes in one request.
    pub const MAX_PAYLOAD_BYTES: ByteCount = ByteCount::new(64 * MEBIBYTE);

    /// Returns an empty first-turn transcript.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Validates a complete ordered transcript.
    ///
    /// # Errors
    ///
    /// Rejects excessive size/count, model or provider-format drift, and reuse
    /// of any attempt, provider-call, or logical tool-invocation identity.
    pub fn try_new<I>(turns: I) -> Result<Self, ModelTranscriptError>
    where
        I: IntoIterator<Item = ModelTranscriptTurn>,
    {
        let mut values = Vec::new();
        let mut payload_bytes = ByteCount::ZERO;
        let mut model: Option<CapabilityIdentity> = None;
        let mut format: Option<ModelProviderReplayFormat> = None;
        let mut attempt_ids = BTreeSet::new();
        let mut provider_call_ids = BTreeSet::new();
        let mut invocation_ids = BTreeSet::new();
        for turn in turns {
            if values.len() == Self::MAX_TURNS {
                return Err(ModelTranscriptError::TooManyTurns {
                    maximum: Self::MAX_TURNS,
                    actual: Self::MAX_TURNS + 1,
                });
            }
            let turn_model = turn.response.provenance().model();
            if let Some(expected) = model.as_ref() {
                if turn_model != expected {
                    return Err(ModelTranscriptError::ModelIdentityDrift);
                }
            } else {
                model = Some(turn_model.clone());
            }
            let turn_format = turn
                .response
                .provider_replay()
                .expect("turn constructor requires provider replay")
                .format();
            if let Some(expected) = format.as_ref() {
                if turn_format != expected {
                    return Err(ModelTranscriptError::ProviderFormatDrift);
                }
            } else {
                format = Some(turn_format.clone());
            }
            let attempt_id = turn.response.provenance().attempt_id();
            if !attempt_ids.insert(attempt_id) {
                return Err(ModelTranscriptError::DuplicateAttemptId { attempt_id });
            }
            for outcome in &turn.outcomes {
                let attempt_id = outcome.attempt_id();
                if !attempt_ids.insert(attempt_id) {
                    return Err(ModelTranscriptError::DuplicateAttemptId { attempt_id });
                }
                if !provider_call_ids.insert(outcome.provider_call_id().clone()) {
                    return Err(ModelTranscriptError::DuplicateProviderCallId);
                }
                let invocation_id = outcome.invocation_id();
                if !invocation_ids.insert(invocation_id) {
                    return Err(ModelTranscriptError::DuplicateInvocationId { invocation_id });
                }
            }
            let encoded = serde_json_canonicalizer::to_vec(&turn)
                .expect("closed transcript turn always has canonical JSON");
            let encoded_bytes = u64::try_from(encoded.len())
                .map_err(|_| ModelTranscriptError::PayloadBytesOverflow)?;
            let framing_bytes = if values.is_empty() { 2 } else { 1 };
            let additional = encoded_bytes
                .checked_add(framing_bytes)
                .map(ByteCount::new)
                .ok_or(ModelTranscriptError::PayloadBytesOverflow)?;
            let Some(actual) = payload_bytes.checked_add(additional) else {
                return Err(ModelTranscriptError::PayloadBytesOverflow);
            };
            if actual > Self::MAX_PAYLOAD_BYTES {
                return Err(ModelTranscriptError::PayloadTooLarge {
                    maximum: Self::MAX_PAYLOAD_BYTES,
                    actual,
                });
            }
            payload_bytes = actual;
            values.push(turn);
        }
        Ok(Self {
            turns: values.into_boxed_slice(),
            payload_bytes,
        })
    }

    /// Returns the number of prior tool-calling turns.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.turns.len()
    }

    /// Returns whether this is the first provider turn.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.turns.is_empty()
    }

    /// Returns turns in exact provider continuation order.
    #[must_use]
    pub const fn as_slice(&self) -> &[ModelTranscriptTurn] {
        &self.turns
    }

    /// Iterates turns in exact provider continuation order.
    pub fn iter(&self) -> std::slice::Iter<'_, ModelTranscriptTurn> {
        self.turns.iter()
    }

    /// Returns aggregate canonical bytes retained by the transcript.
    #[must_use]
    pub const fn payload_bytes(&self) -> ByteCount {
        self.payload_bytes
    }

    /// Returns the pinned model identity, absent for an empty transcript.
    #[must_use]
    pub fn model(&self) -> Option<&CapabilityIdentity> {
        self.turns
            .first()
            .map(|turn| turn.response.provenance().model())
    }

    /// Returns the pinned provider replay format, absent for an empty transcript.
    #[must_use]
    pub fn format(&self) -> Option<&ModelProviderReplayFormat> {
        self.turns.first().map(|turn| {
            turn.response
                .provider_replay()
                .expect("turn constructor requires replay")
                .format()
        })
    }
}

impl fmt::Debug for ModelTranscript {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelTranscript")
            .field("turns", &self.len())
            .field("payload_bytes", &self.payload_bytes)
            .field("model", &self.model())
            .field("format", &self.format())
            .finish_non_exhaustive()
    }
}

impl<'a> IntoIterator for &'a ModelTranscript {
    type Item = &'a ModelTranscriptTurn;
    type IntoIter = std::slice::Iter<'a, ModelTranscriptTurn>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Serialize for ModelTranscript {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.turns.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ModelTranscript {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let turns = Vec::<ModelTranscriptTurn>::deserialize(deserializer)?;
        Self::try_new(turns).map_err(de::Error::custom)
    }
}

impl JsonSchema for ModelTranscript {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelTranscript".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ModelTranscript").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ModelTranscriptTurn>(),
            "maxItems": 1024,
            "description": "Ordered durable model/tool turns. Runtime additionally enforces one model and provider format, globally unique attempt/call/invocation identities, and at most 67108864 canonical bytes."
        })
    }
}

/// Invalid complete model/tool transcript.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelTranscriptError {
    /// The immutable prior-turn ceiling was exceeded.
    #[error("model transcript has {actual} turns; maximum is {maximum}")]
    TooManyTurns {
        /// Immutable turn ceiling.
        maximum: usize,
        /// First observed count above the ceiling.
        actual: usize,
    },
    /// A later turn used another pinned model binding.
    #[error("model transcript changes model identity between turns")]
    ModelIdentityDrift,
    /// A later turn changed provider continuation syntax.
    #[error("model transcript changes provider replay format between turns")]
    ProviderFormatDrift,
    /// One physical model or tool attempt appeared twice.
    #[error("model transcript repeats physical attempt {attempt_id}")]
    DuplicateAttemptId {
        /// Reused model attempt identity.
        attempt_id: AttemptId,
    },
    /// A provider tool-call identifier was reused.
    #[error("model transcript repeats a provider tool call identifier")]
    DuplicateProviderCallId,
    /// A logical tool invocation was reused.
    #[error("model transcript repeats tool invocation {invocation_id}")]
    DuplicateInvocationId {
        /// Reused logical invocation identity.
        invocation_id: InvocationId,
    },
    /// Aggregate byte accounting overflowed.
    #[error("model transcript payload byte accounting overflowed")]
    PayloadBytesOverflow,
    /// Aggregate canonical transcript bytes exceeded the hard request boundary.
    #[error("model transcript is {actual}; maximum is {maximum}")]
    PayloadTooLarge {
        /// Immutable transcript byte ceiling.
        maximum: ByteCount,
        /// Rejected aggregate bytes.
        actual: ByteCount,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn provider_replay_deserialization_rejects_payload_substitution() {
        let replay = ModelProviderReplay::new(
            ModelProviderReplayFormat::new("provider.responses.output.v1").unwrap(),
            BoundedJson::try_from(json!([{
                "type": "function_call",
                "call_id": "call_01",
                "name": "weather.lookup",
                "arguments": "{}"
            }]))
            .unwrap(),
        )
        .unwrap();
        let mut wire = serde_json::to_value(replay).unwrap();
        wire["payload"][0]["arguments"] = json!("{\"city\":\"Shanghai\"}");

        let error = serde_json::from_value::<ModelProviderReplay>(wire).unwrap_err();
        assert!(error.to_string().contains("digest does not match"));
    }

    #[test]
    fn replay_debug_never_formats_opaque_provider_payload() {
        let replay = ModelProviderReplay::new(
            ModelProviderReplayFormat::new("provider.responses.output.v1").unwrap(),
            BoundedJson::try_from(json!([{
                "type": "reasoning",
                "encrypted_content": "must-not-appear-in-debug"
            }]))
            .unwrap(),
        )
        .unwrap();

        assert!(!format!("{replay:?}").contains("must-not-appear-in-debug"));
    }
}
