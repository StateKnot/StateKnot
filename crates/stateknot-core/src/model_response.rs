// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded, provider-neutral model response contracts.

use std::{borrow::Borrow, collections::BTreeSet, fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess},
};
use thiserror::Error;

use crate::{
    ArtifactModality, AttemptId, BoundedJson, ByteCount, CapabilityIdentity, CapabilityName,
    ContentPart, ContentSource, ContentTrust, ExecutionCount, Extensions, JsonContent,
    ModelDescriptor, ModelModality, ModelRequest, ModelTextOutputFormat, ModelToolSelection,
    SchemaReference, TextContent, TokenCount,
};

const MEBIBYTE: u64 = 1024 * 1024;
const PROVIDER_IDENTIFIER_PATTERN: &str = "^[!-~]+$";

/// Validation failure for an opaque provider identifier.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelProviderIdentifierError {
    /// The identifier contained no bytes.
    #[error("model provider identifier must not be empty")]
    Empty,
    /// The identifier exceeded the immutable byte ceiling.
    #[error("model provider identifier is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted encoded length.
        max: usize,
        /// Observed encoded length.
        actual: usize,
    },
    /// The identifier contained whitespace, a control, or non-ASCII data.
    #[error("model provider identifier contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first invalid byte.
        index: usize,
    },
}

fn validate_provider_identifier(
    value: &str,
    maximum: usize,
) -> Result<(), ModelProviderIdentifierError> {
    if value.is_empty() {
        return Err(ModelProviderIdentifierError::Empty);
    }
    if value.len() > maximum {
        return Err(ModelProviderIdentifierError::TooLong {
            max: maximum,
            actual: value.len(),
        });
    }
    if let Some((index, _)) = value
        .bytes()
        .enumerate()
        .find(|(_, byte)| !(b'!'..=b'~').contains(byte))
    {
        return Err(ModelProviderIdentifierError::InvalidByte { index });
    }
    Ok(())
}

macro_rules! define_provider_identifier {
    ($name:ident, $visitor:ident, $schema_name:literal, $documentation:literal) => {
        #[doc = $documentation]
        ///
        /// The exact visible-ASCII value is retained without trimming or
        /// normalization and redacted from `Debug` output.
        #[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Box<str>);

        impl $name {
            /// Maximum encoded identifier length in bytes.
            pub const MAX_BYTES: usize = 512;

            /// Validates and constructs an identifier without copying an owned string.
            ///
            /// # Errors
            ///
            /// Returns [`ModelProviderIdentifierError`] when the value is empty,
            /// oversized, or not entirely visible ASCII without spaces.
            pub fn new(value: impl Into<String>) -> Result<Self, ModelProviderIdentifierError> {
                let value = value.into();
                validate_provider_identifier(&value, Self::MAX_BYTES)?;
                Ok(Self(value.into_boxed_str()))
            }

            /// Returns the exact provider value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Returns the encoded length without disclosing the identifier.
            #[must_use]
            pub fn len_bytes(&self) -> usize {
                self.0.len()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("bytes", &self.len_bytes())
                    .finish_non_exhaustive()
            }
        }

        impl FromStr for $name {
            type Err = ModelProviderIdentifierError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = ModelProviderIdentifierError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = ModelProviderIdentifierError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_string($visitor)
            }
        }

        struct $visitor;

        impl de::Visitor<'_> for $visitor {
            type Value = $name;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an opaque visible-ASCII model provider identifier")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                $name::try_from(value).map_err(E::custom)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                $name::try_from(value).map_err(E::custom)
            }
        }

        impl JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                $schema_name.into()
            }

            fn schema_id() -> std::borrow::Cow<'static, str> {
                concat!(module_path!(), "::", $schema_name).into()
            }

            fn json_schema(_: &mut SchemaGenerator) -> Schema {
                json_schema!({
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 512,
                    "pattern": PROVIDER_IDENTIFIER_PATTERN
                })
            }

            fn inline_schema() -> bool {
                true
            }
        }
    };
}

define_provider_identifier!(
    ModelProviderModelId,
    ModelProviderModelIdVisitor,
    "ModelProviderModelId",
    "An opaque model identifier reported by the selected provider binding."
);
define_provider_identifier!(
    ModelProviderResponseId,
    ModelProviderResponseIdVisitor,
    "ModelProviderResponseId",
    "An opaque response identifier reported by a model provider."
);
define_provider_identifier!(
    ModelProviderToolCallId,
    ModelProviderToolCallIdVisitor,
    "ModelProviderToolCallId",
    "An opaque provider correlation identifier for one proposed tool call."
);

/// Portable reason why model generation stopped.
///
/// Adapters must reject unknown, malformed, cancelled, or failed provider
/// terminal states instead of mapping them to a successful response.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ModelFinishReason {
    /// Normal generation completed without an executable tool proposal.
    Completed,
    /// Generation yielded one or more complete tool-call proposals.
    ToolCalls,
    /// The configured generated-output limit ended generation.
    OutputLimit,
    /// The provider's model context limit ended generation.
    ContextLimit,
    /// The model explicitly refused the request.
    Refused,
    /// Provider safety, policy, or guardrail filtering stopped generation.
    ContentFiltered,
    /// The provider requires a continuation before a final result is available.
    Paused,
}

/// Stable and provider-reported identity for one model attempt response.
///
/// Provider identifiers are diagnostic correlation values, not registry keys,
/// authorization claims, or safe replay tokens.
#[derive(Clone, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponseProvenance {
    attempt_id: AttemptId,
    model: CapabilityIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_model_id: Option<ModelProviderModelId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_response_id: Option<ModelProviderResponseId>,
}

impl ModelResponseProvenance {
    /// Constructs response provenance from validated components.
    #[must_use]
    pub fn new(
        attempt_id: AttemptId,
        model: CapabilityIdentity,
        provider_model_id: Option<ModelProviderModelId>,
        provider_response_id: Option<ModelProviderResponseId>,
    ) -> Self {
        Self {
            attempt_id,
            model,
            provider_model_id,
            provider_response_id,
        }
    }

    /// Returns the exact execution-attempt identifier.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the stable, owner-qualified model identity.
    #[must_use]
    pub const fn model(&self) -> &CapabilityIdentity {
        &self.model
    }

    /// Returns the optional opaque provider model identifier.
    #[must_use]
    pub const fn provider_model_id(&self) -> Option<&ModelProviderModelId> {
        self.provider_model_id.as_ref()
    }

    /// Returns the optional opaque provider response identifier.
    #[must_use]
    pub const fn provider_response_id(&self) -> Option<&ModelProviderResponseId> {
        self.provider_response_id.as_ref()
    }
}

impl fmt::Debug for ModelResponseProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelResponseProvenance")
            .field("attempt_id", &self.attempt_id)
            .field("model", &self.model)
            .field("provider_model_id", &self.provider_model_id)
            .field("provider_response_id", &self.provider_response_id)
            .finish_non_exhaustive()
    }
}

/// Normalized token accounting for exactly one model attempt.
///
/// Input and output counts are inclusive. A present cached-input count is a
/// subset of input; a present reasoning count is a subset of output. Missing
/// optional breakdowns mean the provider did not report them and must never be
/// converted to zero. Adapters normalize Anthropic cache categories into input
/// and Gemini thought tokens into output before construction.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelUsage {
    input_tokens: TokenCount,
    #[serde(skip_serializing_if = "Option::is_none")]
    cached_input_tokens: Option<TokenCount>,
    output_tokens: TokenCount,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_tokens: Option<TokenCount>,
}

impl ModelUsage {
    /// Constructs normalized usage and validates inclusive-subset arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`ModelUsageError`] when a reported breakdown exceeds its
    /// inclusive total or input plus output overflows.
    pub const fn new(
        input_tokens: TokenCount,
        cached_input_tokens: Option<TokenCount>,
        output_tokens: TokenCount,
        reasoning_tokens: Option<TokenCount>,
    ) -> Result<Self, ModelUsageError> {
        if let Some(cached) = cached_input_tokens {
            if cached.get() > input_tokens.get() {
                return Err(ModelUsageError::CachedInputExceedsInput {
                    input_tokens,
                    cached_input_tokens: cached,
                });
            }
        }
        if let Some(reasoning) = reasoning_tokens {
            if reasoning.get() > output_tokens.get() {
                return Err(ModelUsageError::ReasoningExceedsOutput {
                    output_tokens,
                    reasoning_tokens: reasoning,
                });
            }
        }
        if input_tokens.checked_add(output_tokens).is_none() {
            return Err(ModelUsageError::TotalTokensOverflow);
        }
        Ok(Self {
            input_tokens,
            cached_input_tokens,
            output_tokens,
            reasoning_tokens,
        })
    }

    /// Returns inclusive normalized input tokens.
    #[must_use]
    pub const fn input_tokens(&self) -> TokenCount {
        self.input_tokens
    }

    /// Returns the provider-reported cached-input subset when available.
    #[must_use]
    pub const fn cached_input_tokens(&self) -> Option<TokenCount> {
        self.cached_input_tokens
    }

    /// Returns inclusive normalized generated-output tokens.
    #[must_use]
    pub const fn output_tokens(&self) -> TokenCount {
        self.output_tokens
    }

    /// Returns the provider-reported reasoning subset when available.
    #[must_use]
    pub const fn reasoning_tokens(&self) -> Option<TokenCount> {
        self.reasoning_tokens
    }

    /// Returns checked input plus output tokens.
    #[must_use]
    pub fn total_tokens(&self) -> TokenCount {
        self.input_tokens
            .checked_add(self.output_tokens)
            .expect("validated model usage cannot overflow")
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
struct ModelUsageWire {
    input_tokens: TokenCount,
    #[serde(default)]
    cached_input_tokens: Option<TokenCount>,
    output_tokens: TokenCount,
    #[serde(default)]
    reasoning_tokens: Option<TokenCount>,
}

impl<'de> Deserialize<'de> for ModelUsage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelUsageWire::deserialize(deserializer)?;
        Self::new(
            wire.input_tokens,
            wire.cached_input_tokens,
            wire.output_tokens,
            wire.reasoning_tokens,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid normalized model-attempt usage.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelUsageError {
    /// Cached input was larger than inclusive input.
    #[error(
        "cached input tokens {cached_input_tokens} exceed inclusive input tokens {input_tokens}"
    )]
    CachedInputExceedsInput {
        /// Inclusive input total.
        input_tokens: TokenCount,
        /// Invalid cached-input subset.
        cached_input_tokens: TokenCount,
    },
    /// Reasoning output was larger than inclusive output.
    #[error("reasoning tokens {reasoning_tokens} exceed inclusive output tokens {output_tokens}")]
    ReasoningExceedsOutput {
        /// Inclusive output total.
        output_tokens: TokenCount,
        /// Invalid reasoning subset.
        reasoning_tokens: TokenCount,
    },
    /// Inclusive input plus output could not be represented.
    #[error("model usage input and output tokens overflow total tokens")]
    TotalTokensOverflow,
}

/// A complete, unapproved tool-call proposal emitted by a model.
///
/// This value intentionally has no [`crate::InvocationId`]. Before assigning
/// one, the runtime resolves the exact descriptor from the tenant registry,
/// validates arguments against its digest-pinned schema, and applies policy,
/// budget, approval, and ledger checks. The provider call identifier is only a
/// correlation value; when absent, attempt ID plus ordered output index is the
/// durable proposal identity.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolCallProposal {
    tool: CapabilityIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_call_id: Option<ModelProviderToolCallId>,
    arguments: BoundedJson,
    extensions: Extensions,
}

impl ModelToolCallProposal {
    /// Constructs a structurally valid, unapproved proposal.
    ///
    /// # Errors
    ///
    /// Returns [`ModelToolCallProposalError`] unless arguments are a JSON
    /// object. This constructor does not perform tool-schema validation.
    pub fn new(
        tool: CapabilityIdentity,
        provider_call_id: Option<ModelProviderToolCallId>,
        arguments: BoundedJson,
        extensions: Extensions,
    ) -> Result<Self, ModelToolCallProposalError> {
        if !arguments.as_value().is_object() {
            return Err(ModelToolCallProposalError::ArgumentsMustBeObject);
        }
        Ok(Self {
            tool,
            provider_call_id,
            arguments,
            extensions,
        })
    }

    /// Returns the exact requested tool identity claimed by the model adapter.
    #[must_use]
    pub const fn tool(&self) -> &CapabilityIdentity {
        &self.tool
    }

    /// Returns the optional opaque provider correlation identifier.
    #[must_use]
    pub const fn provider_call_id(&self) -> Option<&ModelProviderToolCallId> {
        self.provider_call_id.as_ref()
    }

    /// Returns bounded, untrusted JSON object arguments.
    #[must_use]
    pub const fn arguments(&self) -> &BoundedJson {
        &self.arguments
    }

    /// Returns registered opaque provider/adapter extension data.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl fmt::Debug for ModelToolCallProposal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelToolCallProposal")
            .field("tool", &self.tool)
            .field("provider_call_id", &self.provider_call_id)
            .field("arguments", &self.arguments)
            .field("extensions", &self.extensions)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelToolCallProposalWire {
    tool: CapabilityIdentity,
    #[serde(default)]
    provider_call_id: Option<ModelProviderToolCallId>,
    arguments: BoundedJson,
    extensions: Extensions,
}

impl<'de> Deserialize<'de> for ModelToolCallProposal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelToolCallProposalWire::deserialize(deserializer)?;
        Self::new(
            wire.tool,
            wire.provider_call_id,
            wire.arguments,
            wire.extensions,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid model-produced tool-call proposal.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelToolCallProposalError {
    /// Complete tool arguments were not a JSON object.
    #[error("model tool-call arguments must be a JSON object")]
    ArgumentsMustBeObject,
}

/// Classification of one ordered model output item.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ModelOutputItemKind {
    /// User-visible model content.
    Content,
    /// Human-readable reasoning summary, never hidden chain of thought.
    ReasoningSummary,
    /// Complete but unapproved tool-call proposal.
    ToolCall,
}

/// One item in provider order from a model response.
///
/// Content and summaries must be marked untrusted. Inline text and JSON use a
/// model source; artifact references retain their artifact source. These
/// invariants are rechecked by [`ModelResponse`] even for directly constructed
/// enum variants.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(
    tag = "type",
    content = "content",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ModelOutputItem {
    /// User-visible model output content.
    Content(ContentPart),
    /// Provider-supplied readable reasoning summary.
    ReasoningSummary(TextContent),
    /// Complete, unapproved tool-call proposal.
    ToolCall(Box<ModelToolCallProposal>),
}

impl ModelOutputItem {
    /// Constructs model output content with strict source and trust metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ModelOutputItemError`] for invalid source or trust metadata.
    pub fn content(content: ContentPart) -> Result<Self, ModelOutputItemError> {
        let item = Self::Content(content);
        item.validate_intrinsic()?;
        Ok(item)
    }

    /// Constructs a readable reasoning summary with strict model metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ModelOutputItemError`] for invalid source or trust metadata.
    pub fn reasoning_summary(summary: TextContent) -> Result<Self, ModelOutputItemError> {
        let item = Self::ReasoningSummary(summary);
        item.validate_intrinsic()?;
        Ok(item)
    }

    /// Constructs an ordered tool-call proposal item.
    #[must_use]
    pub fn tool_call(proposal: ModelToolCallProposal) -> Self {
        Self::ToolCall(Box::new(proposal))
    }

    /// Returns the closed item classification.
    #[must_use]
    pub const fn kind(&self) -> ModelOutputItemKind {
        match self {
            Self::Content(_) => ModelOutputItemKind::Content,
            Self::ReasoningSummary(_) => ModelOutputItemKind::ReasoningSummary,
            Self::ToolCall(_) => ModelOutputItemKind::ToolCall,
        }
    }

    /// Returns user-visible content when this is a content item.
    #[must_use]
    pub const fn as_content(&self) -> Option<&ContentPart> {
        match self {
            Self::Content(content) => Some(content),
            Self::ReasoningSummary(_) | Self::ToolCall(_) => None,
        }
    }

    /// Returns readable reasoning text when this is a summary item.
    #[must_use]
    pub const fn as_reasoning_summary(&self) -> Option<&TextContent> {
        match self {
            Self::ReasoningSummary(summary) => Some(summary),
            Self::Content(_) | Self::ToolCall(_) => None,
        }
    }

    /// Returns the unapproved proposal when this is a tool-call item.
    #[must_use]
    pub const fn as_tool_call(&self) -> Option<&ModelToolCallProposal> {
        match self {
            Self::ToolCall(proposal) => Some(proposal),
            Self::Content(_) | Self::ReasoningSummary(_) => None,
        }
    }

    pub(crate) fn validate_intrinsic(&self) -> Result<(), ModelOutputItemError> {
        match self {
            Self::Content(content) => {
                let expected_source = match content {
                    ContentPart::Text(_) | ContentPart::Json(_) => ContentSource::Model,
                    ContentPart::Artifact(_) => ContentSource::Artifact,
                };
                validate_output_metadata(self.kind(), content.metadata(), expected_source)
            }
            Self::ReasoningSummary(summary) => {
                validate_output_metadata(self.kind(), summary.metadata(), ContentSource::Model)
            }
            Self::ToolCall(_) => Ok(()),
        }
    }

    pub(crate) fn inline_payload_bytes(&self) -> usize {
        match self {
            Self::Content(content) => content.inline_payload_bytes(),
            Self::ReasoningSummary(summary) => summary.text().len(),
            Self::ToolCall(proposal) => {
                proposal.arguments().stats().compact_bytes() + proposal.extensions().compact_bytes()
            }
        }
    }
}

pub(crate) fn validate_output_metadata(
    kind: ModelOutputItemKind,
    metadata: &crate::ContentMetadata,
    expected_source: ContentSource,
) -> Result<(), ModelOutputItemError> {
    if metadata.source() != expected_source {
        return Err(ModelOutputItemError::InvalidSource {
            kind,
            expected: expected_source,
            actual: metadata.source(),
        });
    }
    if metadata.trust() != ContentTrust::Untrusted {
        return Err(ModelOutputItemError::InvalidTrust {
            kind,
            actual: metadata.trust(),
        });
    }
    Ok(())
}

impl fmt::Debug for ModelOutputItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Content(content) => formatter
                .debug_tuple("ModelOutputItem::Content")
                .field(content)
                .finish(),
            Self::ReasoningSummary(summary) => formatter
                .debug_tuple("ModelOutputItem::ReasoningSummary")
                .field(summary)
                .finish(),
            Self::ToolCall(proposal) => formatter
                .debug_tuple("ModelOutputItem::ToolCall")
                .field(proposal)
                .finish(),
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
enum ModelOutputItemWire {
    Content(ContentPart),
    ReasoningSummary(TextContent),
    ToolCall(Box<ModelToolCallProposal>),
}

impl<'de> Deserialize<'de> for ModelOutputItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let item = match ModelOutputItemWire::deserialize(deserializer)? {
            ModelOutputItemWire::Content(content) => Self::Content(content),
            ModelOutputItemWire::ReasoningSummary(summary) => Self::ReasoningSummary(summary),
            ModelOutputItemWire::ToolCall(proposal) => Self::ToolCall(proposal),
        };
        item.validate_intrinsic().map_err(de::Error::custom)?;
        Ok(item)
    }
}

/// Invalid intrinsic metadata on an ordered model output item.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelOutputItemError {
    /// Content was attributed to the wrong immediate domain source.
    #[error("model output {kind:?} source must be {expected:?}, got {actual:?}")]
    InvalidSource {
        /// Output item classification.
        kind: ModelOutputItemKind,
        /// Required immediate source.
        expected: ContentSource,
        /// Rejected asserted source.
        actual: ContentSource,
    },
    /// Model output was incorrectly marked application-controlled.
    #[error("model output {kind:?} must be untrusted, got {actual:?}")]
    InvalidTrust {
        /// Output item classification.
        kind: ModelOutputItemKind,
        /// Rejected asserted trust.
        actual: ContentTrust,
    },
}

#[derive(Clone, Eq, PartialEq)]
struct ModelOutputItems {
    values: Box<[ModelOutputItem]>,
    content_items: usize,
    tool_calls: usize,
    inline_payload_bytes: ByteCount,
    modalities: crate::ModelModalities,
}

impl ModelOutputItems {
    const MAX_CONTENT_ITEMS: usize = 256;
    const MAX_TOOL_CALLS: usize = 1024;
    const MAX_ITEMS: usize = Self::MAX_CONTENT_ITEMS + Self::MAX_TOOL_CALLS;
    const MAX_INLINE_PAYLOAD_BYTES: ByteCount = ByteCount::new(64 * MEBIBYTE);

    fn try_new<I>(values: I) -> Result<Self, ModelResponseError>
    where
        I: IntoIterator<Item = ModelOutputItem>,
    {
        let mut output = Vec::new();
        let mut content_items = 0;
        let mut tool_calls = 0;
        let mut inline_payload_bytes = ByteCount::ZERO;
        let mut modalities = BTreeSet::new();
        let mut provider_call_ids = BTreeSet::new();
        for value in values {
            push_output_item(
                &mut output,
                &mut content_items,
                &mut tool_calls,
                &mut inline_payload_bytes,
                &mut modalities,
                &mut provider_call_ids,
                value,
            )?;
        }
        Ok(Self {
            values: output.into_boxed_slice(),
            content_items,
            tool_calls,
            inline_payload_bytes,
            modalities: crate::ModelModalities::try_new(modalities)
                .expect("closed output modality set is always valid"),
        })
    }

    fn as_slice(&self) -> &[ModelOutputItem] {
        &self.values
    }
}

impl fmt::Debug for ModelOutputItems {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelOutputItems")
            .field("items", &self.values.len())
            .field("content_items", &self.content_items)
            .field("tool_calls", &self.tool_calls)
            .field("inline_payload_bytes", &self.inline_payload_bytes)
            .field("modalities", &self.modalities)
            .finish_non_exhaustive()
    }
}

impl Serialize for ModelOutputItems {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ModelOutputItems {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ModelOutputItemsVisitor)
    }
}

struct ModelOutputItemsVisitor;

impl<'de> de::Visitor<'de> for ModelOutputItemsVisitor {
    type Value = ModelOutputItems;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an ordered array containing at most {} bounded model output items",
            ModelOutputItems::MAX_ITEMS
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(
            sequence
                .size_hint()
                .unwrap_or_default()
                .min(ModelOutputItems::MAX_ITEMS),
        );
        let mut content_items = 0;
        let mut tool_calls = 0;
        let mut inline_payload_bytes = ByteCount::ZERO;
        let mut modalities = BTreeSet::new();
        let mut provider_call_ids = BTreeSet::new();
        while let Some(value) = sequence.next_element::<ModelOutputItem>()? {
            push_output_item(
                &mut values,
                &mut content_items,
                &mut tool_calls,
                &mut inline_payload_bytes,
                &mut modalities,
                &mut provider_call_ids,
                value,
            )
            .map_err(de::Error::custom)?;
        }
        Ok(ModelOutputItems {
            values: values.into_boxed_slice(),
            content_items,
            tool_calls,
            inline_payload_bytes,
            modalities: crate::ModelModalities::try_new(modalities).map_err(de::Error::custom)?,
        })
    }
}

impl JsonSchema for ModelOutputItems {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelResponseOutput".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ModelOutputItems").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ModelOutputItem>(),
            "maxItems": 1280,
            "description": "Provider-ordered output. Runtime additionally permits at most 256 content/summary items, at most 1024 tool proposals, unique present provider call IDs, and at most 67108864 aggregate inline content, argument, and per-proposal extension bytes."
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn push_output_item(
    values: &mut Vec<ModelOutputItem>,
    content_items: &mut usize,
    tool_calls: &mut usize,
    inline_payload_bytes: &mut ByteCount,
    modalities: &mut BTreeSet<ModelModality>,
    provider_call_ids: &mut BTreeSet<ModelProviderToolCallId>,
    value: ModelOutputItem,
) -> Result<(), ModelResponseError> {
    if values.len() == ModelOutputItems::MAX_ITEMS {
        return Err(ModelResponseError::TooManyOutputItems {
            max: ModelOutputItems::MAX_ITEMS,
            observed: ModelOutputItems::MAX_ITEMS + 1,
        });
    }
    value
        .validate_intrinsic()
        .map_err(|error| ModelResponseError::InvalidOutputItem {
            index: values.len(),
            error,
        })?;

    match &value {
        ModelOutputItem::Content(content) => {
            if *content_items == ModelOutputItems::MAX_CONTENT_ITEMS {
                return Err(ModelResponseError::TooManyContentItems {
                    max: ModelOutputItems::MAX_CONTENT_ITEMS,
                    observed: ModelOutputItems::MAX_CONTENT_ITEMS + 1,
                });
            }
            *content_items += 1;
            let modality = content_modality(content).ok_or_else(|| {
                ModelResponseError::UnsupportedArtifactModality {
                    index: values.len(),
                    modality: match content {
                        ContentPart::Artifact(artifact) => artifact.representation().modality(),
                        ContentPart::Text(_) | ContentPart::Json(_) => {
                            unreachable!("inline content always has a portable modality")
                        }
                    },
                }
            })?;
            modalities.insert(modality);
        }
        ModelOutputItem::ReasoningSummary(_) => {
            if *content_items == ModelOutputItems::MAX_CONTENT_ITEMS {
                return Err(ModelResponseError::TooManyContentItems {
                    max: ModelOutputItems::MAX_CONTENT_ITEMS,
                    observed: ModelOutputItems::MAX_CONTENT_ITEMS + 1,
                });
            }
            *content_items += 1;
        }
        ModelOutputItem::ToolCall(proposal) => {
            if *tool_calls == ModelOutputItems::MAX_TOOL_CALLS {
                return Err(ModelResponseError::TooManyToolCalls {
                    max: ModelOutputItems::MAX_TOOL_CALLS,
                    observed: ModelOutputItems::MAX_TOOL_CALLS + 1,
                });
            }
            if let Some(provider_call_id) = proposal.provider_call_id() {
                if !provider_call_ids.insert(provider_call_id.clone()) {
                    return Err(ModelResponseError::DuplicateProviderToolCallId);
                }
            }
            *tool_calls += 1;
        }
    }

    let additional = ByteCount::new(value.inline_payload_bytes() as u64);
    let Some(observed) = inline_payload_bytes.checked_add(additional) else {
        return Err(ModelResponseError::InlinePayloadBytesOverflow);
    };
    if observed > ModelOutputItems::MAX_INLINE_PAYLOAD_BYTES {
        return Err(ModelResponseError::InlinePayloadTooLarge {
            maximum: ModelOutputItems::MAX_INLINE_PAYLOAD_BYTES,
            observed,
        });
    }
    *inline_payload_bytes = observed;
    values.push(value);
    Ok(())
}

const fn content_modality(content: &ContentPart) -> Option<ModelModality> {
    match content {
        ContentPart::Text(_) | ContentPart::Json(_) => Some(ModelModality::Text),
        ContentPart::Artifact(artifact) => match artifact.representation().modality() {
            ArtifactModality::Text => Some(ModelModality::Text),
            ArtifactModality::Image => Some(ModelModality::Image),
            ArtifactModality::Audio => Some(ModelModality::Audio),
            ArtifactModality::Video => Some(ModelModality::Video),
            ArtifactModality::Document => Some(ModelModality::Document),
            ArtifactModality::StructuredData
            | ArtifactModality::Archive
            | ArtifactModality::Binary => None,
        },
    }
}

/// Immutable, fully normalized result of one model attempt.
///
/// Deserialization enforces intrinsic resource, metadata, usage, and finish
/// invariants, but serialized model/tool identities remain unauthenticated
/// claims. Call [`Self::validate_for`] against the attempt's immutable descriptor
/// and request snapshot before consuming a durable or remote value. Adapter
/// code should prefer [`Self::new`], which performs both validation layers.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelResponse {
    provenance: ModelResponseProvenance,
    output: ModelOutputItems,
    finish_reason: ModelFinishReason,
    usage: ModelUsage,
    extensions: Extensions,
}

impl ModelResponse {
    /// Maximum user-visible content and reasoning-summary items in one response.
    pub const MAX_CONTENT_ITEMS: usize = ModelOutputItems::MAX_CONTENT_ITEMS;
    /// Maximum complete tool proposals in one response.
    pub const MAX_TOOL_CALLS: usize = ModelOutputItems::MAX_TOOL_CALLS;
    /// Maximum total ordered output items in one response.
    pub const MAX_OUTPUT_ITEMS: usize = ModelOutputItems::MAX_ITEMS;
    /// Maximum aggregate inline content, summary, tool-argument, and proposal-extension bytes.
    pub const MAX_INLINE_PAYLOAD_BYTES: ByteCount = ModelOutputItems::MAX_INLINE_PAYLOAD_BYTES;

    /// Constructs and binds one response to an immutable descriptor and request.
    ///
    /// # Errors
    ///
    /// Returns [`ModelResponseError`] for any structural, resource, finish,
    /// capability-identity, request-limit, output-format, modality, or tool
    /// mismatch. Referenced JSON Schemas still require trusted registry
    /// resolution and validation by the adapter before construction.
    pub fn new<I>(
        provenance: ModelResponseProvenance,
        descriptor: &ModelDescriptor,
        request: &ModelRequest,
        output: I,
        finish_reason: ModelFinishReason,
        usage: ModelUsage,
        extensions: Extensions,
    ) -> Result<Self, ModelResponseError>
    where
        I: IntoIterator<Item = ModelOutputItem>,
    {
        let response = Self::from_parts(
            provenance,
            ModelOutputItems::try_new(output)?,
            finish_reason,
            usage,
            extensions,
        )?;
        response.validate_for(descriptor, request)?;
        Ok(response)
    }

    fn from_parts(
        provenance: ModelResponseProvenance,
        output: ModelOutputItems,
        finish_reason: ModelFinishReason,
        usage: ModelUsage,
        extensions: Extensions,
    ) -> Result<Self, ModelResponseError> {
        match (finish_reason, output.tool_calls) {
            (ModelFinishReason::ToolCalls, 0) => {
                return Err(ModelResponseError::ToolCallsFinishRequiresProposal);
            }
            (ModelFinishReason::ToolCalls, _) => {}
            (reason, calls) if calls > 0 => {
                return Err(ModelResponseError::FinishForbidsToolCalls {
                    finish_reason: reason,
                    tool_calls: ExecutionCount::new(calls as u64),
                });
            }
            _ => {}
        }
        Ok(Self {
            provenance,
            output,
            finish_reason,
            usage,
            extensions,
        })
    }

    /// Revalidates request- and registry-dependent response invariants.
    ///
    /// # Errors
    ///
    /// Returns [`ModelResponseError`] when provenance, usage, output modality,
    /// reasoning summaries, final text format, or tool proposals do not match
    /// the exact descriptor and request snapshot.
    pub fn validate_for(
        &self,
        descriptor: &ModelDescriptor,
        request: &ModelRequest,
    ) -> Result<(), ModelResponseError> {
        let expected_model = descriptor.metadata().identity();
        if self.provenance.model() != expected_model {
            return Err(ModelResponseError::ModelIdentityMismatch {
                expected: Box::new(expected_model.clone()),
                actual: Box::new(self.provenance.model().clone()),
            });
        }
        if self.usage.input_tokens() > request.limits().max_input_tokens() {
            return Err(ModelResponseError::InputUsageExceedsRequest {
                maximum: request.limits().max_input_tokens(),
                actual: self.usage.input_tokens(),
            });
        }
        if self.usage.output_tokens() > request.limits().max_output_tokens() {
            return Err(ModelResponseError::OutputUsageExceedsRequest {
                maximum: request.limits().max_output_tokens(),
                actual: self.usage.output_tokens(),
            });
        }

        for (index, item) in self.output.values.iter().enumerate() {
            match item {
                ModelOutputItem::Content(content) => {
                    let modality = content_modality(content).ok_or_else(|| {
                        ModelResponseError::UnsupportedArtifactModality {
                            index,
                            modality: match content {
                                ContentPart::Artifact(artifact) => {
                                    artifact.representation().modality()
                                }
                                ContentPart::Text(_) | ContentPart::Json(_) => {
                                    unreachable!("inline content always has a portable modality")
                                }
                            },
                        }
                    })?;
                    if !request.output_modalities().contains(modality) {
                        return Err(ModelResponseError::OutputModalityNotRequested {
                            index,
                            modality,
                        });
                    }
                }
                ModelOutputItem::ReasoningSummary(_) => {
                    if !request.requires_reasoning_summaries() {
                        return Err(ModelResponseError::ReasoningSummaryNotRequested { index });
                    }
                }
                ModelOutputItem::ToolCall(proposal) => {
                    validate_tool_proposal(index, proposal, request)?;
                }
            }
        }

        let actual_calls = ExecutionCount::new(self.output.tool_calls as u64);
        if actual_calls > request.max_tool_calls_per_response() {
            return Err(ModelResponseError::ToolCallsExceedRequest {
                maximum: request.max_tool_calls_per_response(),
                actual: actual_calls,
            });
        }

        if self.finish_reason == ModelFinishReason::Completed
            && matches!(
                request.tool_selection(),
                ModelToolSelection::Required {} | ModelToolSelection::Specific { .. }
            )
        {
            return Err(ModelResponseError::RequiredToolCallMissing);
        }

        if self.finish_reason == ModelFinishReason::Completed {
            validate_completed_text_format(&self.output, request.text_output_format())?;
        }
        Ok(())
    }

    /// Returns attempt and model identity provenance.
    #[must_use]
    pub const fn provenance(&self) -> &ModelResponseProvenance {
        &self.provenance
    }

    /// Returns all output items in exact provider order.
    #[must_use]
    pub fn output(&self) -> &[ModelOutputItem] {
        self.output.as_slice()
    }

    /// Iterates complete, unapproved tool-call proposals in provider order.
    pub fn tool_calls(&self) -> impl Iterator<Item = &ModelToolCallProposal> {
        self.output
            .values
            .iter()
            .filter_map(ModelOutputItem::as_tool_call)
    }

    /// Returns the exact number of complete, unapproved tool proposals.
    #[must_use]
    pub const fn tool_call_count(&self) -> usize {
        self.output.tool_calls
    }

    /// Returns the normalized portable finish reason.
    #[must_use]
    pub const fn finish_reason(&self) -> ModelFinishReason {
        self.finish_reason
    }

    /// Returns normalized per-attempt token usage.
    #[must_use]
    pub const fn usage(&self) -> &ModelUsage {
        &self.usage
    }

    /// Returns registered opaque provider/adapter extension values.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Returns aggregate inline payload bytes retained in the response value.
    #[must_use]
    pub const fn inline_payload_bytes(&self) -> ByteCount {
        self.output.inline_payload_bytes
    }
}

impl fmt::Debug for ModelResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelResponse")
            .field("provenance", &self.provenance)
            .field("output", &self.output)
            .field("finish_reason", &self.finish_reason)
            .field("usage", &self.usage)
            .field("extensions", &self.extensions)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelResponseWire {
    provenance: ModelResponseProvenance,
    output: ModelOutputItems,
    finish_reason: ModelFinishReason,
    usage: ModelUsage,
    extensions: Extensions,
}

impl<'de> Deserialize<'de> for ModelResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelResponseWire::deserialize(deserializer)?;
        Self::from_parts(
            wire.provenance,
            wire.output,
            wire.finish_reason,
            wire.usage,
            wire.extensions,
        )
        .map_err(de::Error::custom)
    }
}

fn validate_tool_proposal(
    index: usize,
    proposal: &ModelToolCallProposal,
    request: &ModelRequest,
) -> Result<(), ModelResponseError> {
    let Some(expected) = request.tool(proposal.tool().name()) else {
        return Err(ModelResponseError::ToolNotRequested {
            index,
            tool: Box::new(proposal.tool().clone()),
        });
    };
    let expected_identity = expected.metadata().identity();
    if proposal.tool() != expected_identity {
        return Err(ModelResponseError::ToolIdentityMismatch {
            index,
            expected: Box::new(expected_identity.clone()),
            actual: Box::new(proposal.tool().clone()),
        });
    }
    if let ModelToolSelection::Specific { name } = request.tool_selection() {
        if proposal.tool().name() != name {
            return Err(ModelResponseError::SpecificToolMismatch {
                index,
                expected: name.clone(),
                actual: proposal.tool().name().clone(),
            });
        }
    }
    Ok(())
}

fn validate_completed_text_format(
    output: &ModelOutputItems,
    format: Option<&ModelTextOutputFormat>,
) -> Result<(), ModelResponseError> {
    let Some(format) = format else {
        return Ok(());
    };

    let textual = output
        .values
        .iter()
        .enumerate()
        .filter_map(|(index, item)| match item {
            ModelOutputItem::Content(ContentPart::Text(_)) => {
                Some((index, CompletedTextItem::Text))
            }
            ModelOutputItem::Content(ContentPart::Json(content)) => {
                Some((index, CompletedTextItem::Json(content)))
            }
            ModelOutputItem::Content(ContentPart::Artifact(artifact))
                if artifact.representation().modality() == ArtifactModality::Text =>
            {
                Some((index, CompletedTextItem::Artifact))
            }
            ModelOutputItem::Content(ContentPart::Artifact(_))
            | ModelOutputItem::ReasoningSummary(_)
            | ModelOutputItem::ToolCall(_) => None,
        })
        .collect::<Vec<_>>();

    match format {
        ModelTextOutputFormat::Text {} => {
            if let Some((index, _)) = textual
                .iter()
                .find(|(_, item)| matches!(item, CompletedTextItem::Json(_)))
            {
                return Err(ModelResponseError::PlainTextCompletionContainsJson { index: *index });
            }
        }
        ModelTextOutputFormat::Json {} | ModelTextOutputFormat::JsonSchema { .. } => {
            if let Some((index, _)) = textual
                .iter()
                .find(|(_, item)| !matches!(item, CompletedTextItem::Json(_)))
            {
                return Err(ModelResponseError::StructuredCompletionContainsText { index: *index });
            }
            let json = textual
                .iter()
                .filter_map(|(index, item)| match item {
                    CompletedTextItem::Json(content) => Some((*index, *content)),
                    CompletedTextItem::Text | CompletedTextItem::Artifact => None,
                })
                .collect::<Vec<_>>();
            if json.len() != 1 {
                return Err(ModelResponseError::StructuredCompletionJsonCount {
                    actual: json.len(),
                });
            }
            let (index, content) = json[0];
            match format {
                ModelTextOutputFormat::Json {} if content.schema().is_some() => {
                    return Err(ModelResponseError::UnexpectedOutputSchema { index });
                }
                ModelTextOutputFormat::JsonSchema { schema }
                    if content.schema() != Some(schema) =>
                {
                    return Err(ModelResponseError::OutputSchemaMismatch {
                        index,
                        expected: Box::new(schema.clone()),
                        actual: content.schema().cloned().map(Box::new),
                    });
                }
                ModelTextOutputFormat::Json {}
                | ModelTextOutputFormat::JsonSchema { .. }
                | ModelTextOutputFormat::Text {} => {}
            }
        }
    }
    Ok(())
}

enum CompletedTextItem<'a> {
    Text,
    Json(&'a JsonContent),
    Artifact,
}

/// Invalid provider-neutral model response.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelResponseError {
    /// The ordered output exceeded its total hard ceiling.
    #[error("model response contains at least {observed} output items; maximum is {max}")]
    TooManyOutputItems {
        /// Immutable v1 maximum.
        max: usize,
        /// Minimum count observed before validation stopped.
        observed: usize,
    },
    /// User-visible content plus summaries exceeded their hard ceiling.
    #[error("model response contains at least {observed} content items; maximum is {max}")]
    TooManyContentItems {
        /// Immutable v1 maximum.
        max: usize,
        /// Minimum count observed before validation stopped.
        observed: usize,
    },
    /// Tool proposals exceeded their hard ceiling.
    #[error("model response contains at least {observed} tool calls; maximum is {max}")]
    TooManyToolCalls {
        /// Immutable v1 maximum.
        max: usize,
        /// Minimum count observed before validation stopped.
        observed: usize,
    },
    /// An ordered item carried invalid intrinsic metadata.
    #[error("model response output item {index} is invalid: {error}")]
    InvalidOutputItem {
        /// Zero-based provider-order index.
        index: usize,
        /// Intrinsic item validation failure.
        error: ModelOutputItemError,
    },
    /// Two proposals carried the same present provider correlation identifier.
    #[error("model response provider tool-call identifiers must be unique when present")]
    DuplicateProviderToolCallId,
    /// Inline payload accounting could not be represented.
    #[error("model response inline payload-byte accounting overflowed")]
    InlinePayloadBytesOverflow,
    /// Aggregate retained inline payload exceeded the hard ceiling.
    #[error("model response inline payload is {observed} bytes; maximum is {maximum}")]
    InlinePayloadTooLarge {
        /// Immutable v1 maximum.
        maximum: ByteCount,
        /// First observed aggregate beyond the maximum.
        observed: ByteCount,
    },
    /// A tool-call terminal state had no complete proposal.
    #[error("model response tool_calls finish requires at least one complete proposal")]
    ToolCallsFinishRequiresProposal,
    /// A non-tool terminal state carried executable proposals.
    #[error("model response finish {finish_reason:?} forbids {tool_calls} tool calls")]
    FinishForbidsToolCalls {
        /// Rejected portable terminal state.
        finish_reason: ModelFinishReason,
        /// Number of complete proposals present.
        tool_calls: ExecutionCount,
    },
    /// An output artifact had no portable model modality.
    #[error("model response output item {index} has unsupported artifact modality {modality:?}")]
    UnsupportedArtifactModality {
        /// Zero-based provider-order index.
        index: usize,
        /// Rejected artifact modality.
        modality: ArtifactModality,
    },
    /// The response claimed a different stable model binding.
    #[error("model response identity {actual:?} does not match descriptor {expected:?}")]
    ModelIdentityMismatch {
        /// Exact immutable descriptor identity.
        expected: Box<CapabilityIdentity>,
        /// Rejected response claim.
        actual: Box<CapabilityIdentity>,
    },
    /// Provider input accounting exceeded the request ceiling.
    #[error("model response input usage {actual} exceeds request maximum {maximum}")]
    InputUsageExceedsRequest {
        /// Request input-token ceiling.
        maximum: TokenCount,
        /// Provider-reported normalized input.
        actual: TokenCount,
    },
    /// Provider output accounting exceeded the request ceiling.
    #[error("model response output usage {actual} exceeds request maximum {maximum}")]
    OutputUsageExceedsRequest {
        /// Request generated-output ceiling.
        maximum: TokenCount,
        /// Provider-reported normalized output.
        actual: TokenCount,
    },
    /// A user-visible output modality was not requested.
    #[error("model response output item {index} uses unrequested modality {modality:?}")]
    OutputModalityNotRequested {
        /// Zero-based provider-order index.
        index: usize,
        /// Unrequested modality.
        modality: ModelModality,
    },
    /// A reasoning summary was returned without explicit opt-in.
    #[error("model response output item {index} is an unrequested reasoning summary")]
    ReasoningSummaryNotRequested {
        /// Zero-based provider-order index.
        index: usize,
    },
    /// Tool-call count exceeded the request-specific ceiling.
    #[error("model response has {actual} tool calls; request maximum is {maximum}")]
    ToolCallsExceedRequest {
        /// Request-specific call ceiling.
        maximum: ExecutionCount,
        /// Complete proposal count.
        actual: ExecutionCount,
    },
    /// A proposal named no tool in the request snapshot.
    #[error("model response output item {index} proposes unrequested tool {tool:?}")]
    ToolNotRequested {
        /// Zero-based provider-order index.
        index: usize,
        /// Rejected identity claim.
        tool: Box<CapabilityIdentity>,
    },
    /// A proposal reused a requested name with a different owner or version.
    #[error("model response output item {index} tool {actual:?} does not match {expected:?}")]
    ToolIdentityMismatch {
        /// Zero-based provider-order index.
        index: usize,
        /// Exact identity from the request snapshot.
        expected: Box<CapabilityIdentity>,
        /// Rejected model claim.
        actual: Box<CapabilityIdentity>,
    },
    /// A specific-tool request received a different proposed name.
    #[error(
        "model response output item {index} proposes {actual}; expected specific tool {expected}"
    )]
    SpecificToolMismatch {
        /// Zero-based provider-order index.
        index: usize,
        /// Requested exact tool name.
        expected: CapabilityName,
        /// Rejected proposed name.
        actual: CapabilityName,
    },
    /// A nominal completion violated required or specific tool selection.
    #[error("model response completed without the required tool call")]
    RequiredToolCallMissing,
    /// A nominal plain-text completion returned typed JSON content.
    #[error("model response plain-text completion contains JSON at output item {index}")]
    PlainTextCompletionContainsJson {
        /// Zero-based provider-order index.
        index: usize,
    },
    /// A nominal structured completion returned text or a text artifact.
    #[error("model response structured completion contains text at output item {index}")]
    StructuredCompletionContainsText {
        /// Zero-based provider-order index.
        index: usize,
    },
    /// A nominal structured completion did not contain exactly one JSON value.
    #[error(
        "model response structured completion contains {actual} JSON values; expected exactly one"
    )]
    StructuredCompletionJsonCount {
        /// Observed typed JSON item count.
        actual: usize,
    },
    /// Generic JSON output asserted an unrequested schema binding.
    #[error("model response JSON output item {index} has an unrequested schema")]
    UnexpectedOutputSchema {
        /// Zero-based provider-order index.
        index: usize,
    },
    /// Schema-constrained output did not retain the exact requested binding.
    #[error("model response JSON output item {index} schema does not match the requested schema")]
    OutputSchemaMismatch {
        /// Zero-based provider-order index.
        index: usize,
        /// Exact digest-pinned requested schema.
        expected: Box<SchemaReference>,
        /// Missing or different response claim.
        actual: Option<Box<SchemaReference>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArtifactRef, ContentMetadata, Instruction, ModelRequestBuilder, ModelRequestLimits,
        RedactionState, SecurityLabel, ToolDescriptor,
    };
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

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

    fn artifact(modality: ArtifactModality) -> ArtifactRef {
        let values = fixture(
            &["artifact_refs", "valid"],
            include_str!("../tests/fixtures/core-artifact-v1.json"),
        );
        let mut value = values[0].clone();
        value["representation"]["modality"] = to_value(modality).unwrap();
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

    fn base_builder() -> ModelRequestBuilder {
        ModelRequest::builder(limits()).instruction(instruction())
    }

    fn base_request() -> ModelRequest {
        base_builder().build().unwrap()
    }

    fn provenance(descriptor: &ModelDescriptor) -> ModelResponseProvenance {
        ModelResponseProvenance::new(
            "01912345-6789-7abc-8def-0123456789ab".parse().unwrap(),
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

    fn text_item(text: &str) -> ModelOutputItem {
        ModelOutputItem::content(
            TextContent::new(text, None, model_metadata())
                .unwrap()
                .into(),
        )
        .unwrap()
    }

    fn json_item(value: Value, schema: Option<SchemaReference>) -> ModelOutputItem {
        ModelOutputItem::content(
            JsonContent::new(
                BoundedJson::try_from_value(value).unwrap(),
                schema,
                model_metadata(),
            )
            .into(),
        )
        .unwrap()
    }

    fn usage() -> ModelUsage {
        ModelUsage::new(
            TokenCount::new(120),
            Some(TokenCount::new(20)),
            TokenCount::new(30),
            Some(TokenCount::new(5)),
        )
        .unwrap()
    }

    fn proposal(
        descriptor: &ToolDescriptor,
        provider_call_id: Option<&str>,
    ) -> ModelToolCallProposal {
        ModelToolCallProposal::new(
            descriptor.metadata().identity().clone(),
            provider_call_id.map(|value| ModelProviderToolCallId::new(value).unwrap()),
            BoundedJson::try_from_value(json!({"incident_id": 42})).unwrap(),
            Extensions::default(),
        )
        .unwrap()
    }

    #[test]
    fn provider_identifiers_are_exact_bounded_and_redacted() {
        let value = ModelProviderResponseId::new("resp/abc:def_42.v1").unwrap();
        assert_eq!(value.as_str(), "resp/abc:def_42.v1");
        assert_eq!(to_value(&value).unwrap(), "resp/abc:def_42.v1");
        assert_eq!(
            from_value::<ModelProviderResponseId>(json!(value.as_str())).unwrap(),
            value
        );
        assert!(!format!("{value:?}").contains("resp/abc"));

        for invalid in ["", "has space", "line\nbreak", "响应"] {
            assert!(ModelProviderResponseId::new(invalid).is_err());
        }
        assert_eq!(
            ModelProviderResponseId::new("x".repeat(ModelProviderResponseId::MAX_BYTES + 1)),
            Err(ModelProviderIdentifierError::TooLong {
                max: ModelProviderResponseId::MAX_BYTES,
                actual: ModelProviderResponseId::MAX_BYTES + 1,
            })
        );
        assert!(from_value::<ModelProviderResponseId>(json!(42)).is_err());
    }

    #[test]
    fn finish_reasons_have_closed_canonical_wire_forms() {
        for (reason, expected) in [
            (ModelFinishReason::Completed, "completed"),
            (ModelFinishReason::ToolCalls, "tool_calls"),
            (ModelFinishReason::OutputLimit, "output_limit"),
            (ModelFinishReason::ContextLimit, "context_limit"),
            (ModelFinishReason::Refused, "refused"),
            (ModelFinishReason::ContentFiltered, "content_filtered"),
            (ModelFinishReason::Paused, "paused"),
        ] {
            assert_eq!(to_value(reason).unwrap(), expected);
            assert_eq!(
                from_value::<ModelFinishReason>(json!(expected)).unwrap(),
                reason
            );
        }
        assert!(from_value::<ModelFinishReason>(json!("provider_other")).is_err());
        assert!(from_value::<ModelFinishReason>(Value::Null).is_err());
    }

    #[test]
    fn usage_requires_inclusive_checked_accounting() {
        assert_eq!(usage().total_tokens(), TokenCount::new(150));
        assert_eq!(
            ModelUsage::new(
                TokenCount::new(2),
                Some(TokenCount::new(3)),
                TokenCount::ZERO,
                None,
            ),
            Err(ModelUsageError::CachedInputExceedsInput {
                input_tokens: TokenCount::new(2),
                cached_input_tokens: TokenCount::new(3),
            })
        );
        assert_eq!(
            ModelUsage::new(
                TokenCount::ZERO,
                None,
                TokenCount::new(2),
                Some(TokenCount::new(3)),
            ),
            Err(ModelUsageError::ReasoningExceedsOutput {
                output_tokens: TokenCount::new(2),
                reasoning_tokens: TokenCount::new(3),
            })
        );
        assert_eq!(
            ModelUsage::new(TokenCount::MAX, None, TokenCount::new(1), None),
            Err(ModelUsageError::TotalTokensOverflow)
        );

        let known_zero = ModelUsage::new(
            TokenCount::ZERO,
            Some(TokenCount::ZERO),
            TokenCount::ZERO,
            Some(TokenCount::ZERO),
        )
        .unwrap();
        assert!(
            to_value(known_zero)
                .unwrap()
                .get("cached_input_tokens")
                .is_some()
        );
        let unknown = ModelUsage::new(TokenCount::ZERO, None, TokenCount::ZERO, None).unwrap();
        let encoded = to_value(unknown).unwrap();
        assert!(encoded.get("cached_input_tokens").is_none());
        assert!(encoded.get("reasoning_tokens").is_none());
        assert!(from_value::<ModelUsage>(json!({"input_tokens":"0"})).is_err());
    }

    #[test]
    fn output_items_enforce_untrusted_source_metadata() {
        let controlled = ContentMetadata::new(
            ContentSource::Model,
            ContentTrust::ApplicationControlled,
            SecurityLabel::new("internal/model-output").unwrap(),
            RedactionState::NotApplied,
        );
        let invalid = TextContent::new("unsafe", None, controlled).unwrap();
        assert!(matches!(
            ModelOutputItem::content(invalid.into()),
            Err(ModelOutputItemError::InvalidTrust { .. })
        ));

        let wrong_source = ContentMetadata::untrusted(
            ContentSource::User,
            SecurityLabel::new("internal/model-output").unwrap(),
        );
        let invalid = TextContent::new("unsafe", None, wrong_source).unwrap();
        assert!(matches!(
            ModelOutputItem::content(invalid.into()),
            Err(ModelOutputItemError::InvalidSource { .. })
        ));

        let summary = TextContent::new("Reason summary", None, model_metadata()).unwrap();
        assert_eq!(
            ModelOutputItem::reasoning_summary(summary).unwrap().kind(),
            ModelOutputItemKind::ReasoningSummary
        );
    }

    #[test]
    fn completed_text_response_round_trips_and_redacts_debug() {
        let descriptor = descriptor("models.primary");
        let request = base_request();
        let response = ModelResponse::new(
            provenance(&descriptor),
            &descriptor,
            &request,
            [text_item("confidential model result")],
            ModelFinishReason::Completed,
            usage(),
            Extensions::default(),
        )
        .unwrap();

        assert_eq!(response.output().len(), 1);
        assert_eq!(response.tool_call_count(), 0);
        assert_eq!(response.inline_payload_bytes(), ByteCount::new(25));
        assert_eq!(response.finish_reason(), ModelFinishReason::Completed);
        let encoded = to_value(&response).unwrap();
        let decoded = from_value::<ModelResponse>(encoded.clone()).unwrap();
        assert_eq!(decoded, response);
        decoded.validate_for(&descriptor, &request).unwrap();
        let debug = format!("{response:?}");
        assert!(!debug.contains("confidential model result"));
        assert!(!debug.contains("provider/model-v1"));
        assert!(!debug.contains("response_opaque-42"));

        let mut unknown = encoded;
        unknown["provider"] = Value::from("openai");
        assert!(from_value::<ModelResponse>(unknown).is_err());
    }

    #[test]
    fn empty_plain_text_completion_is_valid_but_usage_stays_required() {
        let descriptor = descriptor("models.primary");
        let request = base_request();
        let response = ModelResponse::new(
            provenance(&descriptor),
            &descriptor,
            &request,
            [],
            ModelFinishReason::Completed,
            ModelUsage::new(TokenCount::new(4), None, TokenCount::new(1), None).unwrap(),
            Extensions::default(),
        )
        .unwrap();
        assert!(response.output().is_empty());

        let mut encoded = to_value(response).unwrap();
        encoded.as_object_mut().unwrap().remove("usage");
        assert!(from_value::<ModelResponse>(encoded).is_err());
    }

    #[test]
    fn provenance_and_usage_are_bound_to_the_attempt_snapshot() {
        let primary_descriptor = descriptor("models.primary");
        let other = descriptor("models.other");
        let request = base_request();
        let response = ModelResponse::new(
            provenance(&primary_descriptor),
            &primary_descriptor,
            &request,
            [text_item("ok")],
            ModelFinishReason::Completed,
            usage(),
            Extensions::default(),
        )
        .unwrap();
        assert!(matches!(
            response.validate_for(&other, &request),
            Err(ModelResponseError::ModelIdentityMismatch { .. })
        ));

        let excessive =
            ModelUsage::new(TokenCount::new(8_193), None, TokenCount::new(1), None).unwrap();
        assert!(matches!(
            ModelResponse::new(
                provenance(&primary_descriptor),
                &primary_descriptor,
                &request,
                [text_item("ok")],
                ModelFinishReason::Completed,
                excessive,
                Extensions::default(),
            ),
            Err(ModelResponseError::InputUsageExceedsRequest { .. })
        ));
    }

    #[test]
    fn structured_completions_are_typed_and_schema_bound() {
        let descriptor = descriptor("models.primary");
        let generic_request = base_builder()
            .text_output_format(Some(ModelTextOutputFormat::json()))
            .build()
            .unwrap();
        ModelResponse::new(
            provenance(&descriptor),
            &descriptor,
            &generic_request,
            [json_item(json!({"status": "ok"}), None)],
            ModelFinishReason::Completed,
            usage(),
            Extensions::default(),
        )
        .unwrap();
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &generic_request,
                [text_item("{\"status\":\"ok\"}")],
                ModelFinishReason::Completed,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::StructuredCompletionContainsText { .. })
        ));
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &generic_request,
                [],
                ModelFinishReason::Completed,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::StructuredCompletionJsonCount { actual: 0 })
        ));

        let schema = tool("incidents.lookup").input_schema().clone();
        let schema_request = base_builder()
            .text_output_format(Some(ModelTextOutputFormat::json_schema(schema.clone())))
            .build()
            .unwrap();
        ModelResponse::new(
            provenance(&descriptor),
            &descriptor,
            &schema_request,
            [json_item(json!({"incident_id": 42}), Some(schema.clone()))],
            ModelFinishReason::Completed,
            usage(),
            Extensions::default(),
        )
        .unwrap();
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &schema_request,
                [json_item(json!({"incident_id": 42}), None)],
                ModelFinishReason::Completed,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::OutputSchemaMismatch { .. })
        ));

        ModelResponse::new(
            provenance(&descriptor),
            &descriptor,
            &schema_request,
            [text_item("partial JSON")],
            ModelFinishReason::OutputLimit,
            usage(),
            Extensions::default(),
        )
        .unwrap();
    }

    #[test]
    fn tool_calls_are_unapproved_ordered_and_request_bound() {
        let descriptor = descriptor("models.primary");
        let first = tool("incidents.lookup");
        let second = tool("incidents.update");
        let request = base_builder()
            .tool(first.clone())
            .tool(second.clone())
            .tool_selection(ModelToolSelection::specific(
                CapabilityName::new("incidents.lookup").unwrap(),
            ))
            .max_tool_calls_per_response(ExecutionCount::new(2))
            .build()
            .unwrap();
        let response = ModelResponse::new(
            provenance(&descriptor),
            &descriptor,
            &request,
            [
                text_item("I will inspect the incident."),
                ModelOutputItem::tool_call(proposal(&first, Some("call_1"))),
            ],
            ModelFinishReason::ToolCalls,
            usage(),
            Extensions::default(),
        )
        .unwrap();
        assert_eq!(response.tool_call_count(), 1);
        assert_eq!(response.tool_calls().count(), 1);
        assert_eq!(response.output()[0].kind(), ModelOutputItemKind::Content);
        assert_eq!(response.output()[1].kind(), ModelOutputItemKind::ToolCall);

        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &request,
                [ModelOutputItem::tool_call(proposal(
                    &second,
                    Some("call_2")
                ))],
                ModelFinishReason::ToolCalls,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::SpecificToolMismatch { .. })
        ));
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &request,
                [ModelOutputItem::tool_call(proposal(&first, Some("call_3")))],
                ModelFinishReason::Completed,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::FinishForbidsToolCalls { .. })
        ));
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &request,
                [],
                ModelFinishReason::ToolCalls,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::ToolCallsFinishRequiresProposal)
        ));
    }

    #[test]
    fn tool_argument_and_correlation_invariants_fail_closed() {
        let descriptor = descriptor("models.primary");
        let selected = tool("incidents.lookup");
        assert_eq!(
            ModelToolCallProposal::new(
                selected.metadata().identity().clone(),
                None,
                BoundedJson::try_from_value(json!([1, 2])).unwrap(),
                Extensions::default(),
            ),
            Err(ModelToolCallProposalError::ArgumentsMustBeObject)
        );

        let request = base_builder()
            .tool(selected.clone())
            .tool_selection(ModelToolSelection::auto())
            .max_tool_calls_per_response(ExecutionCount::new(2))
            .build()
            .unwrap();
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &request,
                [
                    ModelOutputItem::tool_call(proposal(&selected, Some("duplicate"))),
                    ModelOutputItem::tool_call(proposal(&selected, Some("duplicate"))),
                ],
                ModelFinishReason::ToolCalls,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::DuplicateProviderToolCallId)
        ));

        let one_call_request = base_builder()
            .tool(selected.clone())
            .tool_selection(ModelToolSelection::auto())
            .max_tool_calls_per_response(ExecutionCount::new(1))
            .build()
            .unwrap();
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &one_call_request,
                [
                    ModelOutputItem::tool_call(proposal(&selected, Some("one"))),
                    ModelOutputItem::tool_call(proposal(&selected, Some("two"))),
                ],
                ModelFinishReason::ToolCalls,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::ToolCallsExceedRequest { .. })
        ));
    }

    #[test]
    fn required_tools_and_reasoning_opt_in_are_enforced() {
        let descriptor = descriptor("models.primary");
        let selected = tool("incidents.lookup");
        let required = base_builder()
            .tool(selected)
            .tool_selection(ModelToolSelection::required())
            .max_tool_calls_per_response(ExecutionCount::new(1))
            .build()
            .unwrap();
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &required,
                [text_item("done")],
                ModelFinishReason::Completed,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::RequiredToolCallMissing)
        ));

        let summary = ModelOutputItem::reasoning_summary(
            TextContent::new("Readable summary", None, model_metadata()).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &base_request(),
                [summary.clone(), text_item("done")],
                ModelFinishReason::Completed,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::ReasoningSummaryNotRequested { index: 0 })
        ));
        let requested = base_builder().reasoning_summaries(true).build().unwrap();
        ModelResponse::new(
            provenance(&descriptor),
            &descriptor,
            &requested,
            [summary, text_item("done")],
            ModelFinishReason::Completed,
            usage(),
            Extensions::default(),
        )
        .unwrap();
    }

    #[test]
    fn artifact_modalities_are_portable_requested_and_externally_bounded() {
        let descriptor = descriptor("models.primary");
        let document =
            ModelOutputItem::content(artifact(ArtifactModality::Document).into()).unwrap();
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &base_request(),
                [document.clone()],
                ModelFinishReason::Completed,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::OutputModalityNotRequested {
                modality: ModelModality::Document,
                ..
            })
        ));

        let document_request = base_builder()
            .output_modalities(crate::ModelModalities::try_new([ModelModality::Document]).unwrap())
            .text_output_format(None)
            .build()
            .unwrap();
        let response = ModelResponse::new(
            provenance(&descriptor),
            &descriptor,
            &document_request,
            [document],
            ModelFinishReason::Completed,
            usage(),
            Extensions::default(),
        )
        .unwrap();
        assert_eq!(response.inline_payload_bytes(), ByteCount::ZERO);

        let unsupported =
            ModelOutputItem::Content(ContentPart::from(artifact(ArtifactModality::Archive)));
        assert!(matches!(
            ModelResponse::new(
                provenance(&descriptor),
                &descriptor,
                &document_request,
                [unsupported],
                ModelFinishReason::Completed,
                usage(),
                Extensions::default(),
            ),
            Err(ModelResponseError::UnsupportedArtifactModality {
                modality: ArtifactModality::Archive,
                ..
            })
        ));
    }

    #[test]
    fn collection_and_inline_resource_ceilings_stop_at_first_excess() {
        let content = text_item("x");
        assert_eq!(
            ModelOutputItems::try_new(
                std::iter::repeat(content).take(ModelOutputItems::MAX_CONTENT_ITEMS + 1)
            ),
            Err(ModelResponseError::TooManyContentItems {
                max: ModelOutputItems::MAX_CONTENT_ITEMS,
                observed: ModelOutputItems::MAX_CONTENT_ITEMS + 1,
            })
        );

        let selected = tool("incidents.lookup");
        let proposal = ModelOutputItem::tool_call(proposal(&selected, None));
        assert_eq!(
            ModelOutputItems::try_new(
                std::iter::repeat(proposal).take(ModelOutputItems::MAX_TOOL_CALLS + 1)
            ),
            Err(ModelResponseError::TooManyToolCalls {
                max: ModelOutputItems::MAX_TOOL_CALLS,
                observed: ModelOutputItems::MAX_TOOL_CALLS + 1,
            })
        );

        let mut values = Vec::new();
        let mut content_items = 0;
        let mut tool_calls = 0;
        let mut bytes = ModelOutputItems::MAX_INLINE_PAYLOAD_BYTES;
        let mut modalities = BTreeSet::new();
        let mut provider_call_ids = BTreeSet::new();
        assert_eq!(
            push_output_item(
                &mut values,
                &mut content_items,
                &mut tool_calls,
                &mut bytes,
                &mut modalities,
                &mut provider_call_ids,
                text_item("x"),
            ),
            Err(ModelResponseError::InlinePayloadTooLarge {
                maximum: ModelOutputItems::MAX_INLINE_PAYLOAD_BYTES,
                observed: ByteCount::new(ModelOutputItems::MAX_INLINE_PAYLOAD_BYTES.get() + 1),
            })
        );
    }

    #[test]
    fn response_schemas_publish_closed_objects_and_hard_bounds() {
        for schema in [
            to_value(schemars::schema_for!(ModelUsage)).unwrap(),
            to_value(schemars::schema_for!(ModelToolCallProposal)).unwrap(),
            to_value(schemars::schema_for!(ModelResponseProvenance)).unwrap(),
            to_value(schemars::schema_for!(ModelResponse)).unwrap(),
        ] {
            assert_eq!(schema["type"], "object");
            assert_eq!(schema["additionalProperties"], false);
        }

        let response = to_value(schemars::schema_for!(ModelResponse)).unwrap();
        assert_eq!(
            response["$defs"]["ModelResponseOutput"]["maxItems"],
            ModelResponse::MAX_OUTPUT_ITEMS
        );
        let provider_id = to_value(schemars::schema_for!(ModelProviderResponseId)).unwrap();
        assert_eq!(provider_id["maxLength"], ModelProviderResponseId::MAX_BYTES);
        assert_eq!(provider_id["pattern"], PROVIDER_IDENTIFIER_PATTERN);
    }

    proptest! {
        #[test]
        fn valid_usage_round_trips_without_losing_known_breakdowns(
            input in 0_u64..=1_000_000,
            output in 0_u64..=1_000_000,
            cached_fraction in 0_u64..=1_000_000,
            reasoning_fraction in 0_u64..=1_000_000,
        ) {
            let cached = TokenCount::new(cached_fraction.min(input));
            let reasoning = TokenCount::new(reasoning_fraction.min(output));
            let usage = ModelUsage::new(
                TokenCount::new(input),
                Some(cached),
                TokenCount::new(output),
                Some(reasoning),
            ).unwrap();
            let encoded = serde_json::to_vec(&usage).unwrap();
            prop_assert_eq!(serde_json::from_slice::<ModelUsage>(&encoded).unwrap(), usage);
        }

        #[test]
        fn visible_ascii_provider_identifiers_round_trip(bytes in prop::collection::vec(33_u8..127, 1..100)) {
            let text = String::from_utf8(bytes).unwrap();
            let id = ModelProviderToolCallId::new(text.clone()).unwrap();
            let encoded = serde_json::to_vec(&id).unwrap();
            prop_assert_eq!(serde_json::from_slice::<ModelProviderToolCallId>(&encoded).unwrap(), id.clone());
            prop_assert_eq!(id.as_str(), text);
        }
    }
}
