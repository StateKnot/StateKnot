// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded semantic model-stream events and deterministic response assembly.

use std::{collections::BTreeSet, fmt, mem};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{
    ArtifactRef, AttemptId, BoundedJson, BoundedJsonError, ByteCount, CapabilityIdentity,
    ContentMetadata, ContentPart, ContentSource, ExecutionCount, Extensions, JsonContent,
    JsonLimits, LanguageTag, ModelCapabilityMismatch, ModelDescriptor, ModelFinishReason,
    ModelOutputItem, ModelOutputItemError, ModelOutputItemKind, ModelProviderToolCallId,
    ModelRequest, ModelResponse, ModelResponseError, ModelResponseMode, ModelResponseProvenance,
    ModelToolCallProposal, ModelToolCallProposalError, ModelUsage, SchemaReference, TextContent,
    TextContentError, TokenCount,
};

const KIBIBYTE: usize = 1024;
const MAX_EVENTS_PER_ATTEMPT_VALUE: u64 = 1_048_576;

/// One exact, non-empty provider-normalized stream fragment.
///
/// Fragments preserve UTF-8 bytes without trimming or normalization. A JSON
/// or tool-argument fragment need not be valid JSON by itself; the accumulator
/// parses the exact concatenation only when its output item closes. Debug
/// output reveals only the encoded length.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ModelStreamChunk(Box<str>);

impl ModelStreamChunk {
    /// Maximum UTF-8 byte length of one semantic delta event.
    pub const MAX_BYTES: usize = 64 * KIBIBYTE;

    /// Validates and retains an owned stream fragment.
    ///
    /// # Errors
    ///
    /// Returns [`ModelStreamChunkError`] for empty, oversized, control-bearing,
    /// or Unicode-noncharacter input.
    pub fn new(value: impl Into<String>) -> Result<Self, ModelStreamChunkError> {
        let value = value.into();
        validate_stream_chunk(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact provider-normalized UTF-8 fragment.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the retained UTF-8 byte length without disclosing content.
    #[must_use]
    pub fn len_bytes(&self) -> usize {
        self.0.len()
    }

    /// Consumes the fragment and returns its allocation.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0.into()
    }
}

fn validate_stream_chunk(value: &str) -> Result<(), ModelStreamChunkError> {
    if value.is_empty() {
        return Err(ModelStreamChunkError::Empty);
    }
    if value.len() > ModelStreamChunk::MAX_BYTES {
        return Err(ModelStreamChunkError::TooLong {
            maximum: ModelStreamChunk::MAX_BYTES,
            actual: value.len(),
        });
    }
    if let Some((byte_index, _)) = value.char_indices().find(|(_, scalar)| {
        (scalar.is_control() && !matches!(scalar, '\t' | '\n' | '\r'))
            || is_unicode_noncharacter(*scalar)
    }) {
        return Err(ModelStreamChunkError::DisallowedCodePoint { byte_index });
    }
    Ok(())
}

const fn is_unicode_noncharacter(value: char) -> bool {
    let value = value as u32;
    (value >= 0xfdd0 && value <= 0xfdef) || (value & 0xfffe) == 0xfffe
}

impl AsRef<str> for ModelStreamChunk {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for ModelStreamChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelStreamChunk")
            .field("bytes", &self.len_bytes())
            .finish_non_exhaustive()
    }
}

impl Serialize for ModelStreamChunk {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ModelStreamChunk {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl de::Visitor<'_> for Visitor {
            type Value = ModelStreamChunk;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a non-empty bounded model stream fragment")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                ModelStreamChunk::new(value).map_err(E::custom)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                ModelStreamChunk::new(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_string(Visitor)
    }
}

impl JsonSchema for ModelStreamChunk {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelStreamChunk".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ModelStreamChunk").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 65_536,
            "description": "Exact UTF-8 fragment. maxLength is a necessary code-point ceiling; runtime validation separately enforces 65536 bytes and excludes disallowed controls and Unicode noncharacters."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid semantic model-stream fragment.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelStreamChunkError {
    /// The fragment contained no bytes.
    #[error("model stream chunk must not be empty")]
    Empty,
    /// The fragment exceeded its immutable byte ceiling.
    #[error("model stream chunk is {actual} UTF-8 bytes; maximum is {maximum}")]
    TooLong {
        /// Immutable maximum.
        maximum: usize,
        /// Observed encoded length.
        actual: usize,
    },
    /// The fragment contained a control or Unicode noncharacter.
    #[error("model stream chunk contains a disallowed Unicode scalar at byte {byte_index}")]
    DisallowedCodePoint {
        /// Zero-based UTF-8 offset without content disclosure.
        byte_index: usize,
    },
}

/// Header that fixes the type and immutable metadata of one streamed output.
///
/// Text, JSON, reasoning, and tool arguments arrive through subsequent deltas.
/// Artifact bytes never cross this boundary: an artifact start carries only a
/// complete immutable [`ArtifactRef`].
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "content",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ModelOutputStart {
    /// User-visible text header.
    Text {
        /// Optional stable language tag for the completed text.
        #[serde(skip_serializing_if = "Option::is_none")]
        language: Option<LanguageTag>,
        /// Mandatory model-source, untrusted security metadata.
        metadata: ContentMetadata,
    },
    /// Structured JSON header.
    Json {
        /// Optional digest-pinned schema identity asserted for the completed value.
        #[serde(skip_serializing_if = "Option::is_none")]
        schema: Option<SchemaReference>,
        /// Mandatory model-source, untrusted security metadata.
        metadata: ContentMetadata,
    },
    /// Complete immutable artifact reference; no inline deltas are accepted.
    Artifact(Box<ArtifactRef>),
    /// Human-readable reasoning-summary header, never hidden chain of thought.
    ReasoningSummary {
        /// Optional stable language tag for the completed summary.
        #[serde(skip_serializing_if = "Option::is_none")]
        language: Option<LanguageTag>,
        /// Mandatory model-source, untrusted security metadata.
        metadata: ContentMetadata,
    },
    /// Complete identity header for an unapproved model tool-call proposal.
    ToolCall {
        /// Exact requested, owner-qualified tool identity.
        tool: CapabilityIdentity,
        /// Optional opaque provider correlation identifier.
        #[serde(skip_serializing_if = "Option::is_none")]
        provider_call_id: Option<ModelProviderToolCallId>,
        /// Registered bounded proposal extensions.
        extensions: Extensions,
    },
}

impl ModelOutputStart {
    /// Constructs a validated user-visible text header.
    pub fn text(
        language: Option<LanguageTag>,
        metadata: ContentMetadata,
    ) -> Result<Self, ModelOutputItemError> {
        let value = Self::Text { language, metadata };
        value.validate_intrinsic()?;
        Ok(value)
    }

    /// Constructs a validated structured JSON header.
    pub fn json(
        schema: Option<SchemaReference>,
        metadata: ContentMetadata,
    ) -> Result<Self, ModelOutputItemError> {
        let value = Self::Json { schema, metadata };
        value.validate_intrinsic()?;
        Ok(value)
    }

    /// Constructs a validated immutable artifact header.
    pub fn artifact(artifact: ArtifactRef) -> Result<Self, ModelOutputItemError> {
        let value = Self::Artifact(Box::new(artifact));
        value.validate_intrinsic()?;
        Ok(value)
    }

    /// Constructs a validated readable reasoning-summary header.
    pub fn reasoning_summary(
        language: Option<LanguageTag>,
        metadata: ContentMetadata,
    ) -> Result<Self, ModelOutputItemError> {
        let value = Self::ReasoningSummary { language, metadata };
        value.validate_intrinsic()?;
        Ok(value)
    }

    /// Constructs a tool-call header whose arguments will arrive as JSON deltas.
    #[must_use]
    pub fn tool_call(
        tool: CapabilityIdentity,
        provider_call_id: Option<ModelProviderToolCallId>,
        extensions: Extensions,
    ) -> Self {
        Self::ToolCall {
            tool,
            provider_call_id,
            extensions,
        }
    }

    /// Returns the final response-item classification.
    #[must_use]
    pub const fn kind(&self) -> ModelOutputItemKind {
        match self {
            Self::Text { .. } | Self::Json { .. } | Self::Artifact(_) => {
                ModelOutputItemKind::Content
            }
            Self::ReasoningSummary { .. } => ModelOutputItemKind::ReasoningSummary,
            Self::ToolCall { .. } => ModelOutputItemKind::ToolCall,
        }
    }

    /// Returns the only accepted delta classification, or `None` for artifacts.
    #[must_use]
    pub const fn delta_kind(&self) -> Option<ModelOutputDeltaKind> {
        match self {
            Self::Text { .. } => Some(ModelOutputDeltaKind::Text),
            Self::Json { .. } => Some(ModelOutputDeltaKind::Json),
            Self::Artifact(_) => None,
            Self::ReasoningSummary { .. } => Some(ModelOutputDeltaKind::ReasoningSummary),
            Self::ToolCall { .. } => Some(ModelOutputDeltaKind::ToolArguments),
        }
    }

    fn validate_intrinsic(&self) -> Result<(), ModelOutputItemError> {
        match self {
            Self::Text { metadata, .. } | Self::Json { metadata, .. } => {
                crate::model_response::validate_output_metadata(
                    ModelOutputItemKind::Content,
                    metadata,
                    ContentSource::Model,
                )
            }
            Self::Artifact(artifact) => crate::model_response::validate_output_metadata(
                ModelOutputItemKind::Content,
                artifact.metadata(),
                ContentSource::Artifact,
            ),
            Self::ReasoningSummary { metadata, .. } => {
                crate::model_response::validate_output_metadata(
                    ModelOutputItemKind::ReasoningSummary,
                    metadata,
                    ContentSource::Model,
                )
            }
            Self::ToolCall { .. } => Ok(()),
        }
    }

    fn inline_header_bytes(&self) -> usize {
        match self {
            Self::ToolCall { extensions, .. } => extensions.compact_bytes(),
            Self::Text { .. }
            | Self::Json { .. }
            | Self::Artifact(_)
            | Self::ReasoningSummary { .. } => 0,
        }
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    content = "content",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ModelOutputStartWire {
    Text {
        #[serde(default)]
        language: Option<LanguageTag>,
        metadata: ContentMetadata,
    },
    Json {
        #[serde(default)]
        schema: Option<SchemaReference>,
        metadata: ContentMetadata,
    },
    Artifact(Box<ArtifactRef>),
    ReasoningSummary {
        #[serde(default)]
        language: Option<LanguageTag>,
        metadata: ContentMetadata,
    },
    ToolCall {
        tool: CapabilityIdentity,
        #[serde(default)]
        provider_call_id: Option<ModelProviderToolCallId>,
        extensions: Extensions,
    },
}

impl<'de> Deserialize<'de> for ModelOutputStart {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = match ModelOutputStartWire::deserialize(deserializer)? {
            ModelOutputStartWire::Text { language, metadata } => Self::Text { language, metadata },
            ModelOutputStartWire::Json { schema, metadata } => Self::Json { schema, metadata },
            ModelOutputStartWire::Artifact(artifact) => Self::Artifact(artifact),
            ModelOutputStartWire::ReasoningSummary { language, metadata } => {
                Self::ReasoningSummary { language, metadata }
            }
            ModelOutputStartWire::ToolCall {
                tool,
                provider_call_id,
                extensions,
            } => Self::ToolCall {
                tool,
                provider_call_id,
                extensions,
            },
        };
        value.validate_intrinsic().map_err(de::Error::custom)?;
        Ok(value)
    }
}

/// Closed classification of a semantic model-output delta.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ModelOutputDeltaKind {
    /// User-visible text bytes.
    Text,
    /// Partial structured-output JSON text.
    Json,
    /// Partial readable reasoning-summary text.
    ReasoningSummary,
    /// Partial tool-argument JSON text.
    ToolArguments,
}

/// One typed exact fragment for an already-started output item.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "delta",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ModelOutputDelta {
    /// User-visible text delta.
    Text(ModelStreamChunk),
    /// Structured-output JSON delta.
    Json(ModelStreamChunk),
    /// Readable reasoning-summary delta.
    ReasoningSummary(ModelStreamChunk),
    /// Tool-argument JSON delta.
    ToolArguments(ModelStreamChunk),
}

impl ModelOutputDelta {
    /// Returns the closed fragment classification.
    #[must_use]
    pub const fn kind(&self) -> ModelOutputDeltaKind {
        match self {
            Self::Text(_) => ModelOutputDeltaKind::Text,
            Self::Json(_) => ModelOutputDeltaKind::Json,
            Self::ReasoningSummary(_) => ModelOutputDeltaKind::ReasoningSummary,
            Self::ToolArguments(_) => ModelOutputDeltaKind::ToolArguments,
        }
    }

    /// Returns the exact bounded fragment.
    #[must_use]
    pub const fn chunk(&self) -> &ModelStreamChunk {
        match self {
            Self::Text(chunk)
            | Self::Json(chunk)
            | Self::ReasoningSummary(chunk)
            | Self::ToolArguments(chunk) => chunk,
        }
    }

    fn into_chunk(self) -> ModelStreamChunk {
        match self {
            Self::Text(chunk)
            | Self::Json(chunk)
            | Self::ReasoningSummary(chunk)
            | Self::ToolArguments(chunk) => chunk,
        }
    }
}

/// Provider-neutral semantic event emitted by one model attempt.
///
/// `sequence` starts at zero and is contiguous after provider pings, empty
/// deltas, and transport framing have been removed. `output_index` values are
/// registered contiguously by `output_started`, while deltas and completion of
/// already-started outputs may interleave.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelEvent {
    attempt_id: AttemptId,
    sequence: ExecutionCount,
    event: ModelEventKind,
}

impl ModelEvent {
    /// Maximum number of retained semantic events in one attempt stream.
    pub const MAX_EVENTS_PER_ATTEMPT: ExecutionCount =
        ExecutionCount::new(MAX_EVENTS_PER_ATTEMPT_VALUE);
    /// Largest valid zero-based event sequence.
    pub const MAX_SEQUENCE: ExecutionCount = ExecutionCount::new(MAX_EVENTS_PER_ATTEMPT_VALUE - 1);

    /// Constructs and intrinsically validates one semantic event.
    ///
    /// # Errors
    ///
    /// Returns [`ModelEventError`] for an out-of-range sequence or output
    /// index, mismatched start provenance, or invalid output-start metadata.
    pub fn new(
        attempt_id: AttemptId,
        sequence: ExecutionCount,
        event: ModelEventKind,
    ) -> Result<Self, ModelEventError> {
        if sequence >= Self::MAX_EVENTS_PER_ATTEMPT {
            return Err(ModelEventError::SequenceOutOfRange {
                maximum: Self::MAX_SEQUENCE,
                actual: sequence,
            });
        }
        if let Some(output_index) = event.output_index() {
            let maximum_exclusive = ExecutionCount::new(ModelResponse::MAX_OUTPUT_ITEMS as u64);
            if output_index >= maximum_exclusive {
                return Err(ModelEventError::OutputIndexOutOfRange {
                    maximum_exclusive,
                    actual: output_index,
                });
            }
        }
        match &event {
            ModelEventKind::Started { provenance } if provenance.attempt_id() != attempt_id => {
                return Err(ModelEventError::StartedAttemptMismatch {
                    event: attempt_id,
                    provenance: provenance.attempt_id(),
                });
            }
            ModelEventKind::OutputStarted { start, .. } => start
                .validate_intrinsic()
                .map_err(|error| ModelEventError::InvalidOutputStart { error })?,
            ModelEventKind::Started { .. }
            | ModelEventKind::OutputDelta { .. }
            | ModelEventKind::OutputCompleted { .. }
            | ModelEventKind::UsageUpdated { .. }
            | ModelEventKind::Completed { .. } => {}
        }
        Ok(Self {
            attempt_id,
            sequence,
            event,
        })
    }

    /// Returns the exact model-attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the zero-based contiguous semantic sequence.
    #[must_use]
    pub const fn sequence(&self) -> ExecutionCount {
        self.sequence
    }

    /// Returns the semantic event body.
    #[must_use]
    pub const fn event(&self) -> &ModelEventKind {
        &self.event
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelEventWire {
    attempt_id: AttemptId,
    sequence: ExecutionCount,
    event: ModelEventKind,
}

impl<'de> Deserialize<'de> for ModelEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelEventWire::deserialize(deserializer)?;
        Self::new(wire.attempt_id, wire.sequence, wire.event).map_err(de::Error::custom)
    }
}

/// Closed semantic body of a provider-neutral model event.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "content",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ModelEventKind {
    /// Opens one attempt stream and fixes response provenance.
    Started {
        /// Attempt and exact model identity plus optional provider correlation IDs.
        provenance: ModelResponseProvenance,
    },
    /// Registers the next ordered output item and its immutable header.
    OutputStarted {
        /// Zero-based final-response output position.
        output_index: ExecutionCount,
        /// Typed output header.
        start: Box<ModelOutputStart>,
    },
    /// Appends an exact fragment to an active output item.
    OutputDelta {
        /// Previously registered output position.
        output_index: ExecutionCount,
        /// Typed non-empty fragment.
        delta: ModelOutputDelta,
    },
    /// Closes and validates one active output item.
    OutputCompleted {
        /// Previously registered output position.
        output_index: ExecutionCount,
    },
    /// Reports a complete cumulative usage snapshot observed so far.
    UsageUpdated {
        /// Inclusive, normalized, monotonic per-attempt token accounting.
        usage: ModelUsage,
    },
    /// Terminates the stream and supplies authoritative final accounting.
    Completed {
        /// Portable successful or incomplete terminal reason.
        finish_reason: ModelFinishReason,
        /// Authoritative inclusive token accounting for the whole attempt.
        usage: ModelUsage,
        /// Bounded registered provider/adapter terminal metadata.
        extensions: Extensions,
    },
}

impl ModelEventKind {
    fn output_index(&self) -> Option<ExecutionCount> {
        match self {
            Self::OutputStarted { output_index, .. }
            | Self::OutputDelta { output_index, .. }
            | Self::OutputCompleted { output_index } => Some(*output_index),
            Self::Started { .. } | Self::UsageUpdated { .. } | Self::Completed { .. } => None,
        }
    }
}

/// Intrinsically invalid provider-neutral model event.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelEventError {
    /// The zero-based semantic event sequence exceeded the hard ceiling.
    #[error("model event sequence {actual} exceeds maximum {maximum}")]
    SequenceOutOfRange {
        /// Largest accepted sequence.
        maximum: ExecutionCount,
        /// Rejected sequence.
        actual: ExecutionCount,
    },
    /// The zero-based output position exceeded the response item ceiling.
    #[error("model output index {actual} must be less than {maximum_exclusive}")]
    OutputIndexOutOfRange {
        /// Exclusive immutable upper bound.
        maximum_exclusive: ExecutionCount,
        /// Rejected index.
        actual: ExecutionCount,
    },
    /// The stream-start provenance named a different attempt.
    #[error("model event attempt {event} does not match start provenance attempt {provenance}")]
    StartedAttemptMismatch {
        /// Attempt attached to the event envelope.
        event: AttemptId,
        /// Attempt asserted by start provenance.
        provenance: AttemptId,
    },
    /// An output header carried invalid source or trust metadata.
    #[error("invalid streamed model output header: {error}")]
    InvalidOutputStart {
        /// Intrinsic metadata failure.
        #[source]
        error: ModelOutputItemError,
    },
}

/// Token-usage dimension used in monotonic stream diagnostics.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ModelUsageField {
    /// Inclusive input tokens.
    InputTokens,
    /// Cached-input subset.
    CachedInputTokens,
    /// Inclusive generated-output tokens.
    OutputTokens,
    /// Reasoning-output subset.
    ReasoningTokens,
}

enum OutputSlot {
    Active {
        start: ModelOutputStart,
        buffer: String,
    },
    Completed(ModelOutputItem),
    Transitioning,
}

/// Fail-closed state machine that assembles one verified streaming response.
///
/// Any rejected event permanently poisons the accumulator, preventing callers
/// from ignoring a gap or invalid fragment and resuming. A successful terminal
/// event is retained and returned by [`Self::finish`]. Transport end before a
/// terminal event is always an error.
pub struct ModelEventAccumulator<'a> {
    descriptor: &'a ModelDescriptor,
    request: &'a ModelRequest,
    attempt_id: AttemptId,
    next_sequence: ExecutionCount,
    started: bool,
    poisoned: bool,
    provenance: Option<ModelResponseProvenance>,
    slots: Vec<OutputSlot>,
    content_items: usize,
    tool_calls: usize,
    provider_call_ids: BTreeSet<ModelProviderToolCallId>,
    retained_inline_bytes: usize,
    last_usage: Option<ModelUsage>,
    response: Option<ModelResponse>,
}

impl<'a> ModelEventAccumulator<'a> {
    /// Constructs an empty state machine for one immutable attempt snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ModelEventStreamError::RequestNotStreaming`] unless the exact
    /// request selected streaming delivery, or
    /// [`ModelEventStreamError::CapabilityMismatch`] when the descriptor does
    /// not satisfy every derived request requirement.
    pub fn new(
        attempt_id: AttemptId,
        descriptor: &'a ModelDescriptor,
        request: &'a ModelRequest,
    ) -> Result<Self, ModelEventStreamError> {
        if request.response_mode() != ModelResponseMode::Streaming {
            return Err(ModelEventStreamError::RequestNotStreaming);
        }
        descriptor
            .capabilities()
            .satisfies(request.requirements())
            .map_err(|mismatch| ModelEventStreamError::CapabilityMismatch { mismatch })?;
        Ok(Self {
            descriptor,
            request,
            attempt_id,
            next_sequence: ExecutionCount::ZERO,
            started: false,
            poisoned: false,
            provenance: None,
            slots: Vec::new(),
            content_items: 0,
            tool_calls: 0,
            provider_call_ids: BTreeSet::new(),
            retained_inline_bytes: 0,
            last_usage: None,
            response: None,
        })
    }

    /// Applies one event atomically to the logical stream state.
    ///
    /// A successful terminal event retains the final response but does not
    /// expose it. Call [`Self::finish`] only after the transport ends. Any
    /// rejected event, including one after terminal, poisons the state machine.
    pub fn push(&mut self, event: ModelEvent) -> Result<(), ModelEventStreamError> {
        if self.poisoned {
            return Err(ModelEventStreamError::Poisoned);
        }
        if self.response.is_some() {
            self.poisoned = true;
            return Err(ModelEventStreamError::AlreadyCompleted);
        }
        if let Err(error) = self.try_push(event) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    /// Returns the terminal response after a complete stream.
    ///
    /// # Errors
    ///
    /// Returns an error when a prior event poisoned the state machine or the
    /// transport ended before a valid `completed` event.
    pub fn finish(self) -> Result<ModelResponse, ModelEventStreamError> {
        if self.poisoned {
            return Err(ModelEventStreamError::Poisoned);
        }
        self.response.ok_or(ModelEventStreamError::UnexpectedEnd {
            next_sequence: self.next_sequence,
        })
    }

    /// Returns the expected attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the sequence required for the next event.
    #[must_use]
    pub const fn next_sequence(&self) -> ExecutionCount {
        self.next_sequence
    }

    /// Returns whether the state machine accepted a valid terminal event.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.response.is_some() && !self.poisoned
    }

    /// Returns whether a rejected event permanently invalidated this stream.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    fn try_push(&mut self, event: ModelEvent) -> Result<(), ModelEventStreamError> {
        let ModelEvent {
            attempt_id,
            sequence,
            event,
        } = event;
        if attempt_id != self.attempt_id {
            return Err(ModelEventStreamError::AttemptMismatch {
                expected: self.attempt_id,
                actual: attempt_id,
            });
        }
        if sequence != self.next_sequence {
            return Err(ModelEventStreamError::SequenceMismatch {
                expected: self.next_sequence,
                actual: sequence,
            });
        }

        let is_start = matches!(event, ModelEventKind::Started { .. });
        if !self.started && !is_start {
            return Err(ModelEventStreamError::EventBeforeStart);
        }
        if self.started && is_start {
            return Err(ModelEventStreamError::DuplicateStart);
        }

        match event {
            ModelEventKind::Started { provenance } => self.process_start(provenance)?,
            ModelEventKind::OutputStarted {
                output_index,
                start,
            } => self.process_output_start(output_index, *start)?,
            ModelEventKind::OutputDelta {
                output_index,
                delta,
            } => self.process_output_delta(output_index, delta)?,
            ModelEventKind::OutputCompleted { output_index } => {
                self.process_output_completed(output_index)?;
            }
            ModelEventKind::UsageUpdated { usage } => self.process_usage(usage)?,
            ModelEventKind::Completed {
                finish_reason,
                usage,
                extensions,
            } => self.process_completed(finish_reason, usage, extensions)?,
        }

        self.next_sequence = ExecutionCount::new(
            sequence
                .get()
                .checked_add(1)
                .expect("intrinsically bounded model event sequence cannot overflow"),
        );
        Ok(())
    }

    fn process_start(
        &mut self,
        provenance: ModelResponseProvenance,
    ) -> Result<(), ModelEventStreamError> {
        let expected = self.descriptor.metadata().identity();
        if provenance.model() != expected {
            return Err(invalid_response(
                ModelResponseError::ModelIdentityMismatch {
                    expected: Box::new(expected.clone()),
                    actual: Box::new(provenance.model().clone()),
                },
            ));
        }
        self.provenance = Some(provenance);
        self.started = true;
        Ok(())
    }

    fn process_output_start(
        &mut self,
        output_index: ExecutionCount,
        start: ModelOutputStart,
    ) -> Result<(), ModelEventStreamError> {
        let expected = ExecutionCount::new(self.slots.len() as u64);
        if output_index != expected {
            return Err(ModelEventStreamError::OutputStartOutOfOrder {
                expected,
                actual: output_index,
            });
        }

        match start.kind() {
            ModelOutputItemKind::Content | ModelOutputItemKind::ReasoningSummary => {
                if self.content_items == ModelResponse::MAX_CONTENT_ITEMS {
                    return Err(invalid_response(ModelResponseError::TooManyContentItems {
                        max: ModelResponse::MAX_CONTENT_ITEMS,
                        observed: ModelResponse::MAX_CONTENT_ITEMS + 1,
                    }));
                }
                self.content_items += 1;
            }
            ModelOutputItemKind::ToolCall => {
                if self.tool_calls == ModelResponse::MAX_TOOL_CALLS {
                    return Err(invalid_response(ModelResponseError::TooManyToolCalls {
                        max: ModelResponse::MAX_TOOL_CALLS,
                        observed: ModelResponse::MAX_TOOL_CALLS + 1,
                    }));
                }
                let next_calls = self.tool_calls + 1;
                if ExecutionCount::new(next_calls as u64)
                    > self.request.max_tool_calls_per_response()
                {
                    return Err(invalid_response(
                        ModelResponseError::ToolCallsExceedRequest {
                            maximum: self.request.max_tool_calls_per_response(),
                            actual: ExecutionCount::new(next_calls as u64),
                        },
                    ));
                }
                if let ModelOutputStart::ToolCall {
                    provider_call_id: Some(provider_call_id),
                    ..
                } = &start
                {
                    if !self.provider_call_ids.insert(provider_call_id.clone()) {
                        return Err(invalid_response(
                            ModelResponseError::DuplicateProviderToolCallId,
                        ));
                    }
                }
                self.tool_calls = next_calls;
            }
        }

        let observed = self
            .retained_inline_bytes
            .checked_add(start.inline_header_bytes())
            .ok_or(ModelEventStreamError::PayloadAccountingOverflow)?;
        validate_aggregate_payload(observed)?;
        self.retained_inline_bytes = observed;
        self.slots.push(OutputSlot::Active {
            start,
            buffer: String::new(),
        });
        Ok(())
    }

    fn process_output_delta(
        &mut self,
        output_index: ExecutionCount,
        delta: ModelOutputDelta,
    ) -> Result<(), ModelEventStreamError> {
        let index = usize::try_from(output_index.get())
            .expect("intrinsically bounded model output index fits usize");
        let slot = self
            .slots
            .get_mut(index)
            .ok_or(ModelEventStreamError::OutputNotStarted { output_index })?;
        let (start, buffer) = match slot {
            OutputSlot::Active { start, buffer } => (start, buffer),
            OutputSlot::Completed(_) | OutputSlot::Transitioning => {
                return Err(ModelEventStreamError::OutputAlreadyCompleted { output_index });
            }
        };

        let actual_kind = delta.kind();
        let Some(expected_kind) = start.delta_kind() else {
            return Err(ModelEventStreamError::OutputDoesNotAcceptDeltas { output_index });
        };
        if actual_kind != expected_kind {
            return Err(ModelEventStreamError::DeltaKindMismatch {
                output_index,
                expected: expected_kind,
                actual: actual_kind,
            });
        }

        let chunk = delta.into_chunk();
        let observed_item = buffer
            .len()
            .checked_add(chunk.len_bytes())
            .ok_or(ModelEventStreamError::PayloadAccountingOverflow)?;
        let maximum = match expected_kind {
            ModelOutputDeltaKind::Text | ModelOutputDeltaKind::ReasoningSummary => {
                TextContent::MAX_BYTES
            }
            ModelOutputDeltaKind::Json | ModelOutputDeltaKind::ToolArguments => {
                JsonLimits::DEFAULT.max_bytes()
            }
        };
        if observed_item > maximum {
            return Err(ModelEventStreamError::OutputPayloadTooLarge {
                output_index,
                maximum,
                actual: observed_item,
            });
        }
        let observed_total = self
            .retained_inline_bytes
            .checked_add(chunk.len_bytes())
            .ok_or(ModelEventStreamError::PayloadAccountingOverflow)?;
        validate_aggregate_payload(observed_total)?;

        buffer.push_str(chunk.as_str());
        self.retained_inline_bytes = observed_total;
        Ok(())
    }

    fn process_output_completed(
        &mut self,
        output_index: ExecutionCount,
    ) -> Result<(), ModelEventStreamError> {
        let index = usize::try_from(output_index.get())
            .expect("intrinsically bounded model output index fits usize");
        let slot = self
            .slots
            .get_mut(index)
            .ok_or(ModelEventStreamError::OutputNotStarted { output_index })?;
        let active = match mem::replace(slot, OutputSlot::Transitioning) {
            OutputSlot::Active { start, buffer } => (start, buffer),
            OutputSlot::Completed(item) => {
                *slot = OutputSlot::Completed(item);
                return Err(ModelEventStreamError::OutputAlreadyCompleted { output_index });
            }
            OutputSlot::Transitioning => {
                return Err(ModelEventStreamError::OutputAlreadyCompleted { output_index });
            }
        };
        let old_bytes = active
            .0
            .inline_header_bytes()
            .checked_add(active.1.len())
            .ok_or(ModelEventStreamError::PayloadAccountingOverflow)?;
        let item = finish_output_item(output_index, active.0, active.1)?;
        let base = self
            .retained_inline_bytes
            .checked_sub(old_bytes)
            .ok_or(ModelEventStreamError::PayloadAccountingOverflow)?;
        let observed = base
            .checked_add(item.inline_payload_bytes())
            .ok_or(ModelEventStreamError::PayloadAccountingOverflow)?;
        validate_aggregate_payload(observed)?;
        self.retained_inline_bytes = observed;
        *slot = OutputSlot::Completed(item);
        Ok(())
    }

    fn process_usage(&mut self, usage: ModelUsage) -> Result<(), ModelEventStreamError> {
        validate_usage_progress(self.last_usage.as_ref(), &usage)?;
        validate_usage_limits(self.request, &usage)?;
        self.last_usage = Some(usage);
        Ok(())
    }

    fn process_completed(
        &mut self,
        finish_reason: ModelFinishReason,
        usage: ModelUsage,
        extensions: Extensions,
    ) -> Result<(), ModelEventStreamError> {
        validate_usage_progress(self.last_usage.as_ref(), &usage)?;
        validate_usage_limits(self.request, &usage)?;
        if let Some((index, _)) = self
            .slots
            .iter()
            .enumerate()
            .find(|(_, slot)| !matches!(slot, OutputSlot::Completed(_)))
        {
            return Err(ModelEventStreamError::OutputStillOpen {
                output_index: ExecutionCount::new(index as u64),
            });
        }

        let provenance = self
            .provenance
            .take()
            .expect("a terminal model event is impossible before a validated start");
        let output = mem::take(&mut self.slots)
            .into_iter()
            .map(|slot| match slot {
                OutputSlot::Completed(item) => item,
                OutputSlot::Active { .. } | OutputSlot::Transitioning => {
                    unreachable!("all streamed model outputs were checked complete")
                }
            })
            .collect::<Vec<_>>();
        let response = ModelResponse::new(
            provenance,
            self.descriptor,
            self.request,
            output,
            finish_reason,
            usage,
            extensions,
        )
        .map_err(invalid_response)?;
        debug_assert_eq!(
            response.inline_payload_bytes().get(),
            self.retained_inline_bytes as u64
        );
        self.response = Some(response);
        Ok(())
    }
}

impl fmt::Debug for ModelEventAccumulator<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelEventAccumulator")
            .field("attempt_id", &self.attempt_id)
            .field("next_sequence", &self.next_sequence)
            .field("started", &self.started)
            .field("poisoned", &self.poisoned)
            .field("output_slots", &self.slots.len())
            .field("content_items", &self.content_items)
            .field("tool_calls", &self.tool_calls)
            .field("retained_inline_bytes", &self.retained_inline_bytes)
            .field("has_usage", &self.last_usage.is_some())
            .field("complete", &self.is_complete())
            .finish_non_exhaustive()
    }
}

fn finish_output_item(
    output_index: ExecutionCount,
    start: ModelOutputStart,
    buffer: String,
) -> Result<ModelOutputItem, ModelEventStreamError> {
    match start {
        ModelOutputStart::Text { language, metadata } => {
            let content =
                TextContent::from_string(buffer, language, metadata).map_err(|error| {
                    ModelEventStreamError::InvalidTextPayload {
                        output_index,
                        error,
                    }
                })?;
            ModelOutputItem::content(ContentPart::Text(content)).map_err(|error| {
                ModelEventStreamError::InvalidOutputItem {
                    output_index,
                    error,
                }
            })
        }
        ModelOutputStart::Json { schema, metadata } => {
            let value = BoundedJson::from_slice(buffer.as_bytes()).map_err(|error| {
                ModelEventStreamError::InvalidJsonPayload {
                    output_index,
                    error,
                }
            })?;
            ModelOutputItem::content(ContentPart::Json(JsonContent::new(value, schema, metadata)))
                .map_err(|error| ModelEventStreamError::InvalidOutputItem {
                    output_index,
                    error,
                })
        }
        ModelOutputStart::Artifact(artifact) => {
            debug_assert!(buffer.is_empty());
            ModelOutputItem::content(ContentPart::Artifact(artifact)).map_err(|error| {
                ModelEventStreamError::InvalidOutputItem {
                    output_index,
                    error,
                }
            })
        }
        ModelOutputStart::ReasoningSummary { language, metadata } => {
            let content =
                TextContent::from_string(buffer, language, metadata).map_err(|error| {
                    ModelEventStreamError::InvalidTextPayload {
                        output_index,
                        error,
                    }
                })?;
            ModelOutputItem::reasoning_summary(content).map_err(|error| {
                ModelEventStreamError::InvalidOutputItem {
                    output_index,
                    error,
                }
            })
        }
        ModelOutputStart::ToolCall {
            tool,
            provider_call_id,
            extensions,
        } => {
            let arguments = BoundedJson::from_slice(buffer.as_bytes()).map_err(|error| {
                ModelEventStreamError::InvalidJsonPayload {
                    output_index,
                    error,
                }
            })?;
            let proposal =
                ModelToolCallProposal::new(tool, provider_call_id, arguments, extensions).map_err(
                    |error| ModelEventStreamError::InvalidToolCall {
                        output_index,
                        error,
                    },
                )?;
            Ok(ModelOutputItem::tool_call(proposal))
        }
    }
}

fn validate_aggregate_payload(observed: usize) -> Result<(), ModelEventStreamError> {
    let maximum = ModelResponse::MAX_INLINE_PAYLOAD_BYTES;
    if observed as u64 > maximum.get() {
        return Err(ModelEventStreamError::InlinePayloadTooLarge {
            maximum,
            observed: ByteCount::new(observed as u64),
        });
    }
    Ok(())
}

fn validate_usage_limits(
    request: &ModelRequest,
    usage: &ModelUsage,
) -> Result<(), ModelEventStreamError> {
    if usage.input_tokens() > request.limits().max_input_tokens() {
        return Err(invalid_response(
            ModelResponseError::InputUsageExceedsRequest {
                maximum: request.limits().max_input_tokens(),
                actual: usage.input_tokens(),
            },
        ));
    }
    if usage.output_tokens() > request.limits().max_output_tokens() {
        return Err(invalid_response(
            ModelResponseError::OutputUsageExceedsRequest {
                maximum: request.limits().max_output_tokens(),
                actual: usage.output_tokens(),
            },
        ));
    }
    Ok(())
}

fn validate_usage_progress(
    previous: Option<&ModelUsage>,
    actual: &ModelUsage,
) -> Result<(), ModelEventStreamError> {
    let Some(previous) = previous else {
        return Ok(());
    };
    validate_required_usage_field(
        ModelUsageField::InputTokens,
        previous.input_tokens(),
        actual.input_tokens(),
    )?;
    validate_optional_usage_field(
        ModelUsageField::CachedInputTokens,
        previous.cached_input_tokens(),
        actual.cached_input_tokens(),
    )?;
    validate_required_usage_field(
        ModelUsageField::OutputTokens,
        previous.output_tokens(),
        actual.output_tokens(),
    )?;
    validate_optional_usage_field(
        ModelUsageField::ReasoningTokens,
        previous.reasoning_tokens(),
        actual.reasoning_tokens(),
    )
}

fn validate_required_usage_field(
    field: ModelUsageField,
    previous: TokenCount,
    actual: TokenCount,
) -> Result<(), ModelEventStreamError> {
    if actual < previous {
        return Err(ModelEventStreamError::UsageDecreased {
            field,
            previous,
            actual,
        });
    }
    Ok(())
}

fn validate_optional_usage_field(
    field: ModelUsageField,
    previous: Option<TokenCount>,
    actual: Option<TokenCount>,
) -> Result<(), ModelEventStreamError> {
    match (previous, actual) {
        (Some(_), None) => Err(ModelEventStreamError::UsageBreakdownDisappeared { field }),
        (Some(previous), Some(actual)) => validate_required_usage_field(field, previous, actual),
        (None, None | Some(_)) => Ok(()),
    }
}

fn invalid_response(error: ModelResponseError) -> ModelEventStreamError {
    ModelEventStreamError::InvalidResponse { error }
}

/// Invalid sequence or content in one normalized model event stream.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelEventStreamError {
    /// The accumulator was constructed for a complete-only request.
    #[error("model event accumulator requires a streaming model request")]
    RequestNotStreaming,
    /// The selected descriptor could not satisfy the exact request.
    #[error("model descriptor cannot satisfy streaming request: {mismatch}")]
    CapabilityMismatch {
        /// Deterministic set of every unmet provider-neutral requirement.
        #[source]
        mismatch: ModelCapabilityMismatch,
    },
    /// A prior rejected event permanently invalidated the stream.
    #[error("model event accumulator is poisoned by a prior validation failure")]
    Poisoned,
    /// An event followed an already accepted terminal event.
    #[error("model event stream is already complete")]
    AlreadyCompleted,
    /// An event belonged to a different attempt.
    #[error("model event attempt {actual} does not match expected attempt {expected}")]
    AttemptMismatch {
        /// Immutable accumulator attempt.
        expected: AttemptId,
        /// Rejected event attempt.
        actual: AttemptId,
    },
    /// The event sequence was duplicated, skipped, or reordered.
    #[error("model event sequence {actual} does not match expected sequence {expected}")]
    SequenceMismatch {
        /// Next required zero-based sequence.
        expected: ExecutionCount,
        /// Rejected event sequence.
        actual: ExecutionCount,
    },
    /// A non-start event appeared before stream provenance.
    #[error("model event stream must begin with started")]
    EventBeforeStart,
    /// A second start event attempted to replace immutable provenance.
    #[error("model event stream contains more than one started event")]
    DuplicateStart,
    /// Output items were not registered in contiguous provider order.
    #[error("model output start index {actual} does not match next index {expected}")]
    OutputStartOutOfOrder {
        /// Next required output position.
        expected: ExecutionCount,
        /// Rejected output position.
        actual: ExecutionCount,
    },
    /// A delta or completion named an output that had not started.
    #[error("model output index {output_index} has not started")]
    OutputNotStarted {
        /// Unknown zero-based output position.
        output_index: ExecutionCount,
    },
    /// A completed output received another delta or completion.
    #[error("model output index {output_index} is already complete")]
    OutputAlreadyCompleted {
        /// Closed zero-based output position.
        output_index: ExecutionCount,
    },
    /// An artifact reference received an inline fragment.
    #[error("model output index {output_index} does not accept deltas")]
    OutputDoesNotAcceptDeltas {
        /// Artifact output position.
        output_index: ExecutionCount,
    },
    /// A fragment type did not match its immutable output header.
    #[error("model output index {output_index} expected {expected:?} delta, got {actual:?}")]
    DeltaKindMismatch {
        /// Active zero-based output position.
        output_index: ExecutionCount,
        /// Fragment classification fixed by the start event.
        expected: ModelOutputDeltaKind,
        /// Rejected fragment classification.
        actual: ModelOutputDeltaKind,
    },
    /// One accumulated inline item exceeded its parser/content ceiling.
    #[error("model output index {output_index} accumulated {actual} bytes; maximum is {maximum}")]
    OutputPayloadTooLarge {
        /// Active zero-based output position.
        output_index: ExecutionCount,
        /// Immutable per-item byte ceiling.
        maximum: usize,
        /// First rejected accumulated byte count.
        actual: usize,
    },
    /// Payload byte accounting could not be represented.
    #[error("model event stream payload-byte accounting overflowed")]
    PayloadAccountingOverflow,
    /// Aggregate retained response and active-fragment bytes exceeded the hard ceiling.
    #[error("model event stream retains {observed} inline bytes; maximum is {maximum}")]
    InlinePayloadTooLarge {
        /// Immutable aggregate ceiling.
        maximum: ByteCount,
        /// First rejected aggregate.
        observed: ByteCount,
    },
    /// Completed text or reasoning bytes violated the text contract.
    #[error("model output index {output_index} contains invalid text: {error}")]
    InvalidTextPayload {
        /// Zero-based output position.
        output_index: ExecutionCount,
        /// Bounded text validation failure.
        #[source]
        error: TextContentError,
    },
    /// Completed JSON or tool-argument fragments did not form one bounded value.
    #[error("model output index {output_index} contains invalid JSON: {error}")]
    InvalidJsonPayload {
        /// Zero-based output position.
        output_index: ExecutionCount,
        /// Exact bounded-parser failure.
        #[source]
        error: BoundedJsonError,
    },
    /// Complete tool arguments were not a valid proposal object.
    #[error("model output index {output_index} contains an invalid tool call: {error}")]
    InvalidToolCall {
        /// Zero-based output position.
        output_index: ExecutionCount,
        /// Proposal validation failure.
        #[source]
        error: ModelToolCallProposalError,
    },
    /// A materialized output item carried invalid intrinsic metadata.
    #[error("model output index {output_index} is invalid: {error}")]
    InvalidOutputItem {
        /// Zero-based output position.
        output_index: ExecutionCount,
        /// Output-item validation failure.
        #[source]
        error: ModelOutputItemError,
    },
    /// A cumulative usage counter moved backwards.
    #[error("model usage {field:?} decreased from {previous} to {actual}")]
    UsageDecreased {
        /// Regressed normalized dimension.
        field: ModelUsageField,
        /// Last accepted cumulative count.
        previous: TokenCount,
        /// Rejected smaller cumulative count.
        actual: TokenCount,
    },
    /// A previously known optional usage breakdown became absent.
    #[error("model usage {field:?} disappeared after it was reported")]
    UsageBreakdownDisappeared {
        /// Removed optional breakdown.
        field: ModelUsageField,
    },
    /// The terminal event arrived while an output item was still active.
    #[error("model output index {output_index} is still open at stream completion")]
    OutputStillOpen {
        /// First incomplete zero-based output position.
        output_index: ExecutionCount,
    },
    /// The assembled terminal response violated its descriptor/request contract.
    #[error("invalid assembled model response: {error}")]
    InvalidResponse {
        /// Existing complete-response invariant failure.
        #[source]
        error: ModelResponseError,
    },
    /// The transport ended without an accepted semantic terminal event.
    #[error("model event stream ended before completion; next sequence is {next_sequence}")]
    UnexpectedEnd {
        /// Sequence that would have been required next.
        next_sequence: ExecutionCount,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Instruction, ModelProviderModelId, ModelProviderResponseId, ModelRequestBuilder,
        ModelRequestLimits, ModelTextOutputFormat, ModelToolSelection, SecurityLabel,
        ToolDescriptor,
    };
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    const MEBIBYTE: u64 = 1024 * 1024;

    fn fixture(path: &[&str], source: &str) -> Value {
        let mut value = serde_json::from_str::<Value>(source).unwrap();
        for segment in path {
            value = value[*segment].clone();
        }
        value
    }

    fn descriptor(name: &str) -> ModelDescriptor {
        let values = fixture(
            &["descriptors", "valid"],
            include_str!("../tests/fixtures/core-model-descriptor-v1.json"),
        );
        let mut value = values[0].clone();
        value["metadata"]["identity"]["capability"]["name"] = Value::from(name);
        value["capabilities"] = fixture(
            &["capabilities", "valid"],
            include_str!("../tests/fixtures/core-model-capability-v1.json"),
        )[1]
        .clone();
        from_value(value).unwrap()
    }

    fn non_streaming_descriptor(name: &str) -> ModelDescriptor {
        let values = fixture(
            &["descriptors", "valid"],
            include_str!("../tests/fixtures/core-model-descriptor-v1.json"),
        );
        let mut value = values[0].clone();
        value["metadata"]["identity"]["capability"]["name"] = Value::from(name);
        from_value(value).unwrap()
    }

    fn instruction() -> Instruction {
        let values = fixture(
            &["instructions", "valid"],
            include_str!("../tests/fixtures/core-message-v1.json"),
        );
        from_value(values[0].clone()).unwrap()
    }

    fn tool(name: &str) -> ToolDescriptor {
        let values = fixture(
            &["descriptors", "valid"],
            include_str!("../tests/fixtures/core-tool-v1.json"),
        );
        let mut value = values[0].clone();
        value["metadata"]["identity"]["capability"]["name"] = Value::from(name);
        from_value(value).unwrap()
    }

    fn limits() -> ModelRequestLimits {
        ModelRequestLimits::new(
            TokenCount::new(8_192),
            TokenCount::new(1_024),
            ByteCount::new(MEBIBYTE),
        )
        .unwrap()
    }

    fn streaming_builder() -> ModelRequestBuilder {
        ModelRequest::builder(limits())
            .instruction(instruction())
            .response_mode(ModelResponseMode::Streaming)
    }

    fn streaming_request() -> ModelRequest {
        streaming_builder().build().unwrap()
    }

    fn attempt() -> AttemptId {
        "01912345-6789-7abc-8def-0123456789ab".parse().unwrap()
    }

    fn provenance(descriptor: &ModelDescriptor) -> ModelResponseProvenance {
        ModelResponseProvenance::new(
            attempt(),
            descriptor.metadata().identity().clone(),
            Some(ModelProviderModelId::new("provider/model-v1").unwrap()),
            Some(ModelProviderResponseId::new("response_opaque-42").unwrap()),
        )
    }

    fn model_metadata() -> ContentMetadata {
        ContentMetadata::untrusted(
            ContentSource::Model,
            SecurityLabel::new("internal/model-output").unwrap(),
        )
    }

    fn usage(input: u64, cached: Option<u64>, output: u64, reasoning: Option<u64>) -> ModelUsage {
        ModelUsage::new(
            TokenCount::new(input),
            cached.map(TokenCount::new),
            TokenCount::new(output),
            reasoning.map(TokenCount::new),
        )
        .unwrap()
    }

    fn event(sequence: u64, event: ModelEventKind) -> ModelEvent {
        ModelEvent::new(attempt(), ExecutionCount::new(sequence), event).unwrap()
    }

    fn started(sequence: u64, descriptor: &ModelDescriptor) -> ModelEvent {
        event(
            sequence,
            ModelEventKind::Started {
                provenance: provenance(descriptor),
            },
        )
    }

    fn text_start(sequence: u64, index: u64) -> ModelEvent {
        event(
            sequence,
            ModelEventKind::OutputStarted {
                output_index: ExecutionCount::new(index),
                start: Box::new(ModelOutputStart::text(None, model_metadata()).unwrap()),
            },
        )
    }

    fn text_delta(sequence: u64, index: u64, value: &str) -> ModelEvent {
        event(
            sequence,
            ModelEventKind::OutputDelta {
                output_index: ExecutionCount::new(index),
                delta: ModelOutputDelta::Text(ModelStreamChunk::new(value).unwrap()),
            },
        )
    }

    fn output_completed(sequence: u64, index: u64) -> ModelEvent {
        event(
            sequence,
            ModelEventKind::OutputCompleted {
                output_index: ExecutionCount::new(index),
            },
        )
    }

    fn completed(sequence: u64, finish_reason: ModelFinishReason, usage: ModelUsage) -> ModelEvent {
        event(
            sequence,
            ModelEventKind::Completed {
                finish_reason,
                usage,
                extensions: Extensions::default(),
            },
        )
    }

    #[test]
    fn stream_chunks_are_exact_bounded_redacted_values() {
        let chunk = ModelStreamChunk::new("confidential\n片段").unwrap();
        assert_eq!(chunk.as_str(), "confidential\n片段");
        assert_eq!(chunk.len_bytes(), "confidential\n片段".len());
        assert_eq!(to_value(&chunk).unwrap(), "confidential\n片段");
        assert_eq!(
            from_value::<ModelStreamChunk>(json!("exact"))
                .unwrap()
                .as_str(),
            "exact"
        );
        assert!(!format!("{chunk:?}").contains("confidential"));

        assert_eq!(ModelStreamChunk::new(""), Err(ModelStreamChunkError::Empty));
        assert!(matches!(
            ModelStreamChunk::new("x".repeat(ModelStreamChunk::MAX_BYTES + 1)),
            Err(ModelStreamChunkError::TooLong { .. })
        ));
        assert!(matches!(
            ModelStreamChunk::new("bad\u{0000}"),
            Err(ModelStreamChunkError::DisallowedCodePoint { .. })
        ));
        assert!(matches!(
            ModelStreamChunk::new("bad\u{ffff}"),
            Err(ModelStreamChunkError::DisallowedCodePoint { .. })
        ));
        assert!(from_value::<ModelStreamChunk>(json!(42)).is_err());

        let schema = to_value(schemars::schema_for!(ModelStreamChunk)).unwrap();
        assert_eq!(schema["minLength"], 1);
        assert_eq!(schema["maxLength"], ModelStreamChunk::MAX_BYTES);
    }

    #[test]
    fn output_headers_reject_false_trust_and_wrong_sources() {
        let controlled = ContentMetadata::new(
            ContentSource::Model,
            crate::ContentTrust::ApplicationControlled,
            SecurityLabel::new("internal/model-output").unwrap(),
            crate::RedactionState::NotApplied,
        );
        assert!(matches!(
            ModelOutputStart::text(None, controlled),
            Err(ModelOutputItemError::InvalidTrust { .. })
        ));

        let wrong = ContentMetadata::untrusted(
            ContentSource::User,
            SecurityLabel::new("internal/model-output").unwrap(),
        );
        assert!(matches!(
            ModelOutputStart::json(None, wrong),
            Err(ModelOutputItemError::InvalidSource { .. })
        ));

        let mut encoded =
            to_value(ModelOutputStart::text(None, model_metadata()).unwrap()).unwrap();
        encoded["content"]["metadata"]["trust"] = Value::from("application_controlled");
        assert!(from_value::<ModelOutputStart>(encoded).is_err());
    }

    #[test]
    fn event_envelopes_enforce_attempt_and_resource_bounds() {
        let descriptor = descriptor("models.primary");
        let other_attempt = "01912345-6789-7abc-8def-0123456789ac".parse().unwrap();
        assert!(matches!(
            ModelEvent::new(
                other_attempt,
                ExecutionCount::ZERO,
                ModelEventKind::Started {
                    provenance: provenance(&descriptor)
                }
            ),
            Err(ModelEventError::StartedAttemptMismatch { .. })
        ));
        assert!(matches!(
            ModelEvent::new(
                attempt(),
                ModelEvent::MAX_EVENTS_PER_ATTEMPT,
                ModelEventKind::UsageUpdated {
                    usage: usage(1, None, 0, None)
                }
            ),
            Err(ModelEventError::SequenceOutOfRange { .. })
        ));
        assert!(matches!(
            ModelEvent::new(
                attempt(),
                ExecutionCount::ZERO,
                ModelEventKind::OutputCompleted {
                    output_index: ExecutionCount::new(ModelResponse::MAX_OUTPUT_ITEMS as u64)
                }
            ),
            Err(ModelEventError::OutputIndexOutOfRange { .. })
        ));

        let encoded = to_value(started(0, &descriptor)).unwrap();
        assert_eq!(encoded["sequence"], "0");
        assert_eq!(
            from_value::<ModelEvent>(encoded.clone()).unwrap(),
            started(0, &descriptor)
        );
        let mut unknown = encoded;
        unknown["unknown"] = Value::Bool(true);
        assert!(from_value::<ModelEvent>(unknown).is_err());
    }

    #[test]
    fn text_stream_converges_to_one_bound_response() {
        let descriptor = descriptor("models.primary");
        let request = streaming_request();
        let mut accumulator = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        let events = [
            started(0, &descriptor),
            text_start(1, 0),
            text_delta(2, 0, "hel"),
            event(
                3,
                ModelEventKind::UsageUpdated {
                    usage: usage(120, Some(20), 1, None),
                },
            ),
            text_delta(4, 0, "lo"),
            output_completed(5, 0),
            completed(
                6,
                ModelFinishReason::Completed,
                usage(120, Some(20), 2, None),
            ),
        ];
        for event in events {
            accumulator.push(event).unwrap();
        }

        assert!(accumulator.is_complete());
        let response = accumulator.finish().unwrap();
        assert_eq!(response.provenance(), &provenance(&descriptor));
        assert_eq!(response.finish_reason(), ModelFinishReason::Completed);
        assert_eq!(response.usage(), &usage(120, Some(20), 2, None));
        assert_eq!(response.inline_payload_bytes(), ByteCount::new(5));
        assert_eq!(
            response.output()[0]
                .as_content()
                .and_then(|content| match content {
                    ContentPart::Text(text) => Some(text.text()),
                    ContentPart::Json(_) | ContentPart::Artifact(_) => None,
                }),
            Some("hello")
        );
    }

    #[test]
    fn json_fragments_are_parsed_once_when_the_item_closes() {
        let descriptor = descriptor("models.primary");
        let request = streaming_builder()
            .text_output_format(Some(ModelTextOutputFormat::json()))
            .build()
            .unwrap();
        let mut accumulator = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        let events = [
            started(0, &descriptor),
            event(
                1,
                ModelEventKind::OutputStarted {
                    output_index: ExecutionCount::ZERO,
                    start: Box::new(ModelOutputStart::json(None, model_metadata()).unwrap()),
                },
            ),
            event(
                2,
                ModelEventKind::OutputDelta {
                    output_index: ExecutionCount::ZERO,
                    delta: ModelOutputDelta::Json(ModelStreamChunk::new("{\"answer\":").unwrap()),
                },
            ),
            event(
                3,
                ModelEventKind::OutputDelta {
                    output_index: ExecutionCount::ZERO,
                    delta: ModelOutputDelta::Json(ModelStreamChunk::new("42}").unwrap()),
                },
            ),
            output_completed(4, 0),
            completed(5, ModelFinishReason::Completed, usage(10, None, 3, None)),
        ];
        for event in events {
            accumulator.push(event).unwrap();
        }
        let response = accumulator.finish().unwrap();
        let json = match response.output()[0].as_content().unwrap() {
            ContentPart::Json(json) => json,
            ContentPart::Text(_) | ContentPart::Artifact(_) => panic!("expected JSON"),
        };
        assert_eq!(json.value().as_value(), &json!({"answer": 42}));
    }

    #[test]
    fn malformed_or_duplicate_json_poison_the_stream() {
        for raw in ["{", "{\"a\":1,\"a\":2}", "{} trailing"] {
            let descriptor = descriptor("models.primary");
            let request = streaming_builder()
                .text_output_format(Some(ModelTextOutputFormat::json()))
                .build()
                .unwrap();
            let mut accumulator =
                ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
            accumulator.push(started(0, &descriptor)).unwrap();
            accumulator
                .push(event(
                    1,
                    ModelEventKind::OutputStarted {
                        output_index: ExecutionCount::ZERO,
                        start: Box::new(ModelOutputStart::json(None, model_metadata()).unwrap()),
                    },
                ))
                .unwrap();
            accumulator
                .push(event(
                    2,
                    ModelEventKind::OutputDelta {
                        output_index: ExecutionCount::ZERO,
                        delta: ModelOutputDelta::Json(ModelStreamChunk::new(raw).unwrap()),
                    },
                ))
                .unwrap();
            assert!(matches!(
                accumulator.push(output_completed(3, 0)),
                Err(ModelEventStreamError::InvalidJsonPayload { .. })
            ));
            assert_eq!(
                accumulator.push(completed(
                    3,
                    ModelFinishReason::Completed,
                    usage(1, None, 1, None)
                )),
                Err(ModelEventStreamError::Poisoned)
            );
        }
    }

    #[test]
    fn already_started_outputs_may_interleave_without_reordering_results() {
        let descriptor = descriptor("models.primary");
        let request = streaming_builder()
            .reasoning_summaries(true)
            .build()
            .unwrap();
        let mut accumulator = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        let events = [
            started(0, &descriptor),
            text_start(1, 0),
            text_delta(2, 0, "visible "),
            event(
                3,
                ModelEventKind::OutputStarted {
                    output_index: ExecutionCount::new(1),
                    start: Box::new(
                        ModelOutputStart::reasoning_summary(None, model_metadata()).unwrap(),
                    ),
                },
            ),
            event(
                4,
                ModelEventKind::OutputDelta {
                    output_index: ExecutionCount::new(1),
                    delta: ModelOutputDelta::ReasoningSummary(
                        ModelStreamChunk::new("summary").unwrap(),
                    ),
                },
            ),
            text_delta(5, 0, "answer"),
            output_completed(6, 1),
            output_completed(7, 0),
            completed(8, ModelFinishReason::Completed, usage(10, None, 4, Some(1))),
        ];
        for event in events {
            accumulator.push(event).unwrap();
        }
        let response = accumulator.finish().unwrap();
        assert_eq!(response.output()[0].kind(), ModelOutputItemKind::Content);
        assert_eq!(
            response.output()[1].kind(),
            ModelOutputItemKind::ReasoningSummary
        );
    }

    #[test]
    fn tool_argument_fragments_become_an_unapproved_proposal() {
        let descriptor = descriptor("models.primary");
        let requested_tool = tool("tools.lookup");
        let request = streaming_builder()
            .tool(requested_tool.clone())
            .tool_selection(ModelToolSelection::auto())
            .max_tool_calls_per_response(ExecutionCount::new(2))
            .build()
            .unwrap();
        let mut accumulator = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        let events = [
            started(0, &descriptor),
            event(
                1,
                ModelEventKind::OutputStarted {
                    output_index: ExecutionCount::ZERO,
                    start: Box::new(ModelOutputStart::tool_call(
                        requested_tool.metadata().identity().clone(),
                        Some(ModelProviderToolCallId::new("call_42").unwrap()),
                        Extensions::default(),
                    )),
                },
            ),
            event(
                2,
                ModelEventKind::OutputDelta {
                    output_index: ExecutionCount::ZERO,
                    delta: ModelOutputDelta::ToolArguments(
                        ModelStreamChunk::new("{\"incident_id\":").unwrap(),
                    ),
                },
            ),
            event(
                3,
                ModelEventKind::OutputDelta {
                    output_index: ExecutionCount::ZERO,
                    delta: ModelOutputDelta::ToolArguments(ModelStreamChunk::new("42}").unwrap()),
                },
            ),
            output_completed(4, 0),
            completed(5, ModelFinishReason::ToolCalls, usage(20, None, 8, None)),
        ];
        for event in events {
            accumulator.push(event).unwrap();
        }
        let response = accumulator.finish().unwrap();
        let proposal = response.tool_calls().next().unwrap();
        assert_eq!(proposal.tool(), requested_tool.metadata().identity());
        assert_eq!(proposal.provider_call_id().unwrap().as_str(), "call_42");
        assert_eq!(proposal.arguments().as_value(), &json!({"incident_id": 42}));
    }

    #[test]
    fn duplicate_tool_ids_and_non_object_arguments_never_commit() {
        let descriptor = descriptor("models.primary");
        let requested_tool = tool("tools.lookup");
        let request = streaming_builder()
            .tool(requested_tool.clone())
            .tool_selection(ModelToolSelection::auto())
            .max_tool_calls_per_response(ExecutionCount::new(2))
            .build()
            .unwrap();

        let start = |sequence, index| {
            event(
                sequence,
                ModelEventKind::OutputStarted {
                    output_index: ExecutionCount::new(index),
                    start: Box::new(ModelOutputStart::tool_call(
                        requested_tool.metadata().identity().clone(),
                        Some(ModelProviderToolCallId::new("call_duplicate").unwrap()),
                        Extensions::default(),
                    )),
                },
            )
        };
        let mut duplicate = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        duplicate.push(started(0, &descriptor)).unwrap();
        duplicate.push(start(1, 0)).unwrap();
        assert!(matches!(
            duplicate.push(start(2, 1)),
            Err(ModelEventStreamError::InvalidResponse {
                error: ModelResponseError::DuplicateProviderToolCallId
            })
        ));

        let mut scalar = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        scalar.push(started(0, &descriptor)).unwrap();
        scalar.push(start(1, 0)).unwrap();
        scalar
            .push(event(
                2,
                ModelEventKind::OutputDelta {
                    output_index: ExecutionCount::ZERO,
                    delta: ModelOutputDelta::ToolArguments(ModelStreamChunk::new("42").unwrap()),
                },
            ))
            .unwrap();
        assert!(matches!(
            scalar.push(output_completed(3, 0)),
            Err(ModelEventStreamError::InvalidToolCall {
                error: ModelToolCallProposalError::ArgumentsMustBeObject,
                ..
            })
        ));
    }

    #[test]
    fn noncomplete_text_may_commit_but_tool_fragments_cannot_masquerade_as_output() {
        let descriptor = descriptor("models.primary");
        let text_request = streaming_request();
        let mut partial =
            ModelEventAccumulator::new(attempt(), &descriptor, &text_request).unwrap();
        for event in [
            started(0, &descriptor),
            text_start(1, 0),
            text_delta(2, 0, "partial but typed"),
            output_completed(3, 0),
            completed(4, ModelFinishReason::OutputLimit, usage(5, None, 3, None)),
        ] {
            partial.push(event).unwrap();
        }
        let response = partial.finish().unwrap();
        assert_eq!(response.finish_reason(), ModelFinishReason::OutputLimit);
        assert_eq!(response.output().len(), 1);

        let requested_tool = tool("tools.lookup");
        let tool_request = streaming_builder()
            .tool(requested_tool.clone())
            .tool_selection(ModelToolSelection::auto())
            .max_tool_calls_per_response(ExecutionCount::new(1))
            .build()
            .unwrap();
        let mut tool_stream =
            ModelEventAccumulator::new(attempt(), &descriptor, &tool_request).unwrap();
        tool_stream.push(started(0, &descriptor)).unwrap();
        tool_stream
            .push(event(
                1,
                ModelEventKind::OutputStarted {
                    output_index: ExecutionCount::ZERO,
                    start: Box::new(ModelOutputStart::tool_call(
                        requested_tool.metadata().identity().clone(),
                        None,
                        Extensions::default(),
                    )),
                },
            ))
            .unwrap();
        tool_stream
            .push(event(
                2,
                ModelEventKind::OutputDelta {
                    output_index: ExecutionCount::ZERO,
                    delta: ModelOutputDelta::ToolArguments(
                        ModelStreamChunk::new("{\"id\":").unwrap(),
                    ),
                },
            ))
            .unwrap();
        assert!(matches!(
            tool_stream.push(completed(
                3,
                ModelFinishReason::OutputLimit,
                usage(5, None, 3, None)
            )),
            Err(ModelEventStreamError::OutputStillOpen { .. })
        ));
    }

    #[test]
    fn stream_order_attempt_and_delta_type_fail_closed() {
        let descriptor = descriptor("models.primary");
        let request = streaming_request();

        let mut before_start =
            ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        assert_eq!(
            before_start.push(text_start(0, 0)),
            Err(ModelEventStreamError::EventBeforeStart)
        );
        assert!(before_start.is_poisoned());

        let mut gap = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        gap.push(started(0, &descriptor)).unwrap();
        assert!(matches!(
            gap.push(text_start(2, 0)),
            Err(ModelEventStreamError::SequenceMismatch { .. })
        ));

        let mut output_gap = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        output_gap.push(started(0, &descriptor)).unwrap();
        assert!(matches!(
            output_gap.push(text_start(1, 1)),
            Err(ModelEventStreamError::OutputStartOutOfOrder { .. })
        ));

        let mut wrong_delta = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        wrong_delta.push(started(0, &descriptor)).unwrap();
        wrong_delta.push(text_start(1, 0)).unwrap();
        assert!(matches!(
            wrong_delta.push(event(
                2,
                ModelEventKind::OutputDelta {
                    output_index: ExecutionCount::ZERO,
                    delta: ModelOutputDelta::Json(ModelStreamChunk::new("{}").unwrap())
                }
            )),
            Err(ModelEventStreamError::DeltaKindMismatch { .. })
        ));

        let mut wrong_attempt =
            ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        wrong_attempt.push(started(0, &descriptor)).unwrap();
        let other_attempt = "01912345-6789-7abc-8def-0123456789ac".parse().unwrap();
        let other = ModelEvent::new(
            other_attempt,
            ExecutionCount::new(1),
            ModelEventKind::UsageUpdated {
                usage: usage(1, None, 0, None),
            },
        )
        .unwrap();
        assert!(matches!(
            wrong_attempt.push(other),
            Err(ModelEventStreamError::AttemptMismatch { .. })
        ));
    }

    #[test]
    fn usage_snapshots_are_cumulative_monotonic_and_bounded() {
        let descriptor = descriptor("models.primary");
        let request = streaming_request();
        let mut accumulator = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        accumulator.push(started(0, &descriptor)).unwrap();
        accumulator
            .push(event(
                1,
                ModelEventKind::UsageUpdated {
                    usage: usage(100, Some(20), 10, Some(2)),
                },
            ))
            .unwrap();
        assert!(matches!(
            accumulator.push(event(
                2,
                ModelEventKind::UsageUpdated {
                    usage: usage(100, Some(19), 11, Some(2))
                }
            )),
            Err(ModelEventStreamError::UsageDecreased {
                field: ModelUsageField::CachedInputTokens,
                ..
            })
        ));

        let mut disappeared = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        disappeared.push(started(0, &descriptor)).unwrap();
        disappeared
            .push(event(
                1,
                ModelEventKind::UsageUpdated {
                    usage: usage(100, Some(20), 10, None),
                },
            ))
            .unwrap();
        assert!(matches!(
            disappeared.push(completed(
                2,
                ModelFinishReason::Completed,
                usage(100, None, 11, None)
            )),
            Err(ModelEventStreamError::UsageBreakdownDisappeared {
                field: ModelUsageField::CachedInputTokens
            })
        ));

        let mut excessive = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        excessive.push(started(0, &descriptor)).unwrap();
        assert!(matches!(
            excessive.push(event(
                1,
                ModelEventKind::UsageUpdated {
                    usage: usage(8_193, None, 1, None)
                }
            )),
            Err(ModelEventStreamError::InvalidResponse {
                error: ModelResponseError::InputUsageExceedsRequest { .. }
            })
        ));
    }

    #[test]
    fn open_or_truncated_streams_never_become_responses() {
        let descriptor = descriptor("models.primary");
        let request = streaming_request();
        let mut open = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        open.push(started(0, &descriptor)).unwrap();
        open.push(text_start(1, 0)).unwrap();
        open.push(text_delta(2, 0, "partial")).unwrap();
        assert!(matches!(
            open.push(completed(
                3,
                ModelFinishReason::OutputLimit,
                usage(4, None, 2, None)
            )),
            Err(ModelEventStreamError::OutputStillOpen { .. })
        ));

        let mut truncated = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        truncated.push(started(0, &descriptor)).unwrap();
        assert_eq!(
            truncated.finish(),
            Err(ModelEventStreamError::UnexpectedEnd {
                next_sequence: ExecutionCount::new(1)
            })
        );
    }

    #[test]
    fn empty_plain_completion_needs_only_start_and_terminal() {
        let descriptor = descriptor("models.primary");
        let request = streaming_request();
        let mut accumulator = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        accumulator.push(started(0, &descriptor)).unwrap();
        accumulator
            .push(completed(
                1,
                ModelFinishReason::Completed,
                usage(4, None, 1, None),
            ))
            .unwrap();
        assert!(accumulator.finish().unwrap().output().is_empty());
    }

    #[test]
    fn trailing_events_poison_an_otherwise_complete_stream() {
        let descriptor = descriptor("models.primary");
        let request = streaming_request();
        let mut accumulator = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        accumulator.push(started(0, &descriptor)).unwrap();
        accumulator
            .push(completed(
                1,
                ModelFinishReason::Completed,
                usage(4, None, 1, None),
            ))
            .unwrap();
        assert!(accumulator.is_complete());
        assert_eq!(
            accumulator.push(event(
                2,
                ModelEventKind::UsageUpdated {
                    usage: usage(4, None, 1, None)
                }
            )),
            Err(ModelEventStreamError::AlreadyCompleted)
        );
        assert!(accumulator.is_poisoned());
        assert!(!accumulator.is_complete());
        assert_eq!(accumulator.finish(), Err(ModelEventStreamError::Poisoned));
    }

    #[test]
    fn complete_requests_and_wrong_model_provenance_are_rejected() {
        let primary = descriptor("models.primary");
        let complete_request = ModelRequest::builder(limits())
            .instruction(instruction())
            .build()
            .unwrap();
        assert!(matches!(
            ModelEventAccumulator::new(attempt(), &primary, &complete_request),
            Err(ModelEventStreamError::RequestNotStreaming)
        ));

        let request = streaming_request();
        let non_streaming = non_streaming_descriptor("models.primary");
        assert!(matches!(
            ModelEventAccumulator::new(attempt(), &non_streaming, &request),
            Err(ModelEventStreamError::CapabilityMismatch { .. })
        ));

        let other = descriptor("models.other");
        let mut accumulator = ModelEventAccumulator::new(attempt(), &primary, &request).unwrap();
        assert!(matches!(
            accumulator.push(started(0, &other)),
            Err(ModelEventStreamError::InvalidResponse {
                error: ModelResponseError::ModelIdentityMismatch { .. }
            })
        ));
    }

    #[test]
    fn per_item_stream_buffer_is_bounded_before_materialization() {
        let descriptor = descriptor("models.primary");
        let request = streaming_request();
        let mut accumulator = ModelEventAccumulator::new(attempt(), &descriptor, &request).unwrap();
        accumulator.push(started(0, &descriptor)).unwrap();
        accumulator.push(text_start(1, 0)).unwrap();
        let full_chunk = "x".repeat(ModelStreamChunk::MAX_BYTES);
        for sequence in 2..6 {
            accumulator
                .push(text_delta(sequence, 0, &full_chunk))
                .unwrap();
        }
        assert!(matches!(
            accumulator.push(text_delta(6, 0, "x")),
            Err(ModelEventStreamError::OutputPayloadTooLarge {
                maximum: TextContent::MAX_BYTES,
                actual,
                ..
            }) if actual == TextContent::MAX_BYTES + 1
        ));
    }

    #[test]
    fn event_schemas_close_envelopes_and_tagged_variants() {
        let event = to_value(schemars::schema_for!(ModelEvent)).unwrap();
        assert_eq!(event["type"], "object");
        assert_eq!(event["additionalProperties"], false);
        let required = event["required"].as_array().unwrap();
        for field in ["attempt_id", "sequence", "event"] {
            assert!(required.contains(&Value::from(field)));
        }

        for schema in [
            to_value(schemars::schema_for!(ModelOutputStart)).unwrap(),
            to_value(schemars::schema_for!(ModelOutputDelta)).unwrap(),
            to_value(schemars::schema_for!(ModelEventKind)).unwrap(),
        ] {
            let variants = schema["oneOf"].as_array().unwrap();
            assert!(!variants.is_empty());
            assert!(
                variants
                    .iter()
                    .all(|variant| variant["additionalProperties"] == false)
            );
        }
    }

    proptest! {
        #[test]
        fn safe_ascii_chunks_round_trip_exactly(value in "[ -~\\n\\r\\t]{1,4096}") {
            let chunk = ModelStreamChunk::new(value.clone()).unwrap();
            let encoded = to_value(&chunk).unwrap();
            let decoded = from_value::<ModelStreamChunk>(encoded).unwrap();
            prop_assert_eq!(decoded.as_str(), value);
        }
    }
}
