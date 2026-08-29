// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded, provider-neutral model request contracts.

use std::{
    collections::{BTreeMap, BTreeSet, btree_map},
    fmt,
};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess},
};
use thiserror::Error;

use crate::{
    ArtifactModality, ByteCount, CapabilityLifecycleState, CapabilityName, ContentPart,
    ExecutionCount, Extensions, Instruction, InstructionContent, InstructionIdentity, Message,
    MessageId, ModelModalities, ModelModality, ModelRequirements, ModelRequirementsError,
    ModelStructuredOutputLevel, ModelToolChoice, ModelToolChoices, ModelToolRequirements,
    SchemaReference, TokenCount, ToolDescriptor,
};

const MEBIBYTE: u64 = 1024 * 1024;

/// How a model invocation delivers its response.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ModelResponseMode {
    /// One complete response is returned after generation terminates.
    Complete,
    /// Validated semantic events are emitted incrementally.
    Streaming,
}

/// Tool-selection behavior for one model request.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelToolSelection {
    /// No tool definitions are supplied and no tool call may be emitted.
    None {},
    /// The model decides whether and which supplied tool to call.
    Auto {},
    /// At least one of the supplied tools must be called.
    Required {},
    /// One exact supplied tool must be called.
    Specific {
        /// Registry-local tool name exposed to the model.
        name: CapabilityName,
    },
}

impl ModelToolSelection {
    /// Constructs disabled tool selection.
    #[must_use]
    pub const fn none() -> Self {
        Self::None {}
    }

    /// Constructs automatic tool selection.
    #[must_use]
    pub const fn auto() -> Self {
        Self::Auto {}
    }

    /// Constructs required tool selection.
    #[must_use]
    pub const fn required() -> Self {
        Self::Required {}
    }

    /// Constructs selection of one named tool.
    #[must_use]
    pub const fn specific(name: CapabilityName) -> Self {
        Self::Specific { name }
    }

    /// Returns the capability-negotiation choice class.
    #[must_use]
    pub const fn choice(&self) -> ModelToolChoice {
        match self {
            Self::None {} => ModelToolChoice::None,
            Self::Auto {} => ModelToolChoice::Auto,
            Self::Required {} => ModelToolChoice::Required,
            Self::Specific { .. } => ModelToolChoice::Specific,
        }
    }

    /// Returns the selected name for specific selection.
    #[must_use]
    pub const fn specific_name(&self) -> Option<&CapabilityName> {
        match self {
            Self::Specific { name } => Some(name),
            Self::None {} | Self::Auto {} | Self::Required {} => None,
        }
    }
}

/// Requested format for the text portion of a model response.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelTextOutputFormat {
    /// Unstructured validated text.
    Text {},
    /// Syntactically valid JSON without a schema guarantee.
    Json {},
    /// JSON constrained by one digest-pinned schema.
    JsonSchema {
        /// Exact output schema resolved from the trusted local registry.
        schema: SchemaReference,
    },
}

impl ModelTextOutputFormat {
    /// Constructs plain-text output.
    #[must_use]
    pub const fn text() -> Self {
        Self::Text {}
    }

    /// Constructs valid-JSON output.
    #[must_use]
    pub const fn json() -> Self {
        Self::Json {}
    }

    /// Constructs JSON Schema-constrained output.
    #[must_use]
    pub const fn json_schema(schema: SchemaReference) -> Self {
        Self::JsonSchema { schema }
    }

    /// Returns the corresponding capability requirement level.
    #[must_use]
    pub const fn required_level(&self) -> ModelStructuredOutputLevel {
        match self {
            Self::Text {} => ModelStructuredOutputLevel::Unsupported,
            Self::Json {} => ModelStructuredOutputLevel::Json,
            Self::JsonSchema { .. } => ModelStructuredOutputLevel::JsonSchema,
        }
    }

    /// Returns the requested JSON Schema identity when applicable.
    #[must_use]
    pub const fn schema(&self) -> Option<&SchemaReference> {
        match self {
            Self::JsonSchema { schema } => Some(schema),
            Self::Text {} | Self::Json {} => None,
        }
    }
}

/// Finite, provider-neutral ceilings for one model request.
///
/// Input and output token ceilings are both positive. Their checked sum is the
/// minimum total context capacity used during model negotiation. Content bytes
/// cover inline text/JSON plus every referenced instruction/message artifact;
/// tool-schema bytes are governed by the trusted schema registry profile.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequestLimits {
    max_input_tokens: TokenCount,
    max_output_tokens: TokenCount,
    max_content_bytes: ByteCount,
}

impl ModelRequestLimits {
    /// Immutable v1 ceiling for resolved instruction and message bytes.
    pub const HARD_MAX_CONTENT_BYTES: ByteCount = ByteCount::new(64 * MEBIBYTE);

    /// Constructs finite request ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRequestLimitsError`] for zero token/byte capacity, a
    /// content ceiling above the hard resource limit, or token-sum overflow.
    pub const fn new(
        max_input_tokens: TokenCount,
        max_output_tokens: TokenCount,
        max_content_bytes: ByteCount,
    ) -> Result<Self, ModelRequestLimitsError> {
        if max_input_tokens.get() == 0 {
            return Err(ModelRequestLimitsError::ZeroInputTokens);
        }
        if max_output_tokens.get() == 0 {
            return Err(ModelRequestLimitsError::ZeroOutputTokens);
        }
        if max_content_bytes.get() == 0 {
            return Err(ModelRequestLimitsError::ZeroContentBytes);
        }
        if max_content_bytes.get() > Self::HARD_MAX_CONTENT_BYTES.get() {
            return Err(ModelRequestLimitsError::ContentBytesAboveHardMaximum {
                maximum: Self::HARD_MAX_CONTENT_BYTES,
                actual: max_content_bytes,
            });
        }
        if max_input_tokens.checked_add(max_output_tokens).is_none() {
            return Err(ModelRequestLimitsError::ContextTokensOverflow);
        }
        Ok(Self {
            max_input_tokens,
            max_output_tokens,
            max_content_bytes,
        })
    }

    /// Returns the local input-token ceiling used for preflight and selection.
    #[must_use]
    pub const fn max_input_tokens(&self) -> TokenCount {
        self.max_input_tokens
    }

    /// Returns the inclusive generated output-token ceiling.
    #[must_use]
    pub const fn max_output_tokens(&self) -> TokenCount {
        self.max_output_tokens
    }

    /// Returns the maximum resolved instruction and message bytes.
    #[must_use]
    pub const fn max_content_bytes(&self) -> ByteCount {
        self.max_content_bytes
    }

    /// Returns the checked total context capacity required by this request.
    #[must_use]
    pub fn required_context_tokens(&self) -> TokenCount {
        self.max_input_tokens
            .checked_add(self.max_output_tokens)
            .expect("validated model request token ceilings cannot overflow")
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
struct ModelRequestLimitsWire {
    max_input_tokens: TokenCount,
    max_output_tokens: TokenCount,
    max_content_bytes: ByteCount,
}

impl<'de> Deserialize<'de> for ModelRequestLimits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelRequestLimitsWire::deserialize(deserializer)?;
        Self::new(
            wire.max_input_tokens,
            wire.max_output_tokens,
            wire.max_content_bytes,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid finite request limits.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelRequestLimitsError {
    /// No input-token capacity was available.
    #[error("model request max input tokens must be greater than zero")]
    ZeroInputTokens,
    /// No generated token could be returned.
    #[error("model request max output tokens must be greater than zero")]
    ZeroOutputTokens,
    /// No request content could be supplied.
    #[error("model request max content bytes must be greater than zero")]
    ZeroContentBytes,
    /// The caller tried to widen the immutable content resource bound.
    #[error("model request content ceiling {actual} exceeds hard maximum {maximum}")]
    ContentBytesAboveHardMaximum {
        /// Immutable v1 maximum.
        maximum: ByteCount,
        /// Rejected caller ceiling.
        actual: ByteCount,
    },
    /// Input and output token ceilings could not form one total context bound.
    #[error("model request input and output token ceilings overflow total context capacity")]
    ContextTokensOverflow,
}

#[derive(Clone, Eq, PartialEq)]
struct RequestInstructions {
    values: Box<[Instruction]>,
    content_bytes: ByteCount,
}

impl RequestInstructions {
    const MAX_LEN: usize = 32;
    const MAX_CONTENT_BYTES: ByteCount = ByteCount::new(8 * MEBIBYTE);

    fn as_slice(&self) -> &[Instruction] {
        &self.values
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Debug for RequestInstructions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestInstructions")
            .field("count", &self.len())
            .field("content_bytes", &self.content_bytes)
            .finish_non_exhaustive()
    }
}

impl Serialize for RequestInstructions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RequestInstructions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(RequestInstructionsVisitor)
    }
}

struct RequestInstructionsVisitor;

impl<'de> de::Visitor<'de> for RequestInstructionsVisitor {
    type Value = RequestInstructions;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {} unique trusted instructions",
            RequestInstructions::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        let mut content_bytes = ByteCount::ZERO;
        while let Some(value) = sequence.next_element::<Instruction>()? {
            push_instruction(&mut values, &mut content_bytes, value).map_err(de::Error::custom)?;
        }
        Ok(RequestInstructions {
            values: values.into_boxed_slice(),
            content_bytes,
        })
    }
}

impl JsonSchema for RequestInstructions {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelRequestInstructions".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::RequestInstructions").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<Instruction>(),
            "maxItems": 32,
            "uniqueItems": true,
            "description": "Ordered application-controlled instructions; adapters preserve this precedence order. Runtime additionally rejects duplicate instruction identities and enforces an 8388608-byte resolved-content ceiling."
        })
    }
}

fn push_instruction(
    values: &mut Vec<Instruction>,
    content_bytes: &mut ByteCount,
    value: Instruction,
) -> Result<(), ModelRequestError> {
    if values.len() == RequestInstructions::MAX_LEN {
        return Err(ModelRequestError::TooManyInstructions {
            max: RequestInstructions::MAX_LEN,
            observed: RequestInstructions::MAX_LEN + 1,
        });
    }
    if values
        .iter()
        .any(|existing| existing.identity() == value.identity())
    {
        return Err(ModelRequestError::DuplicateInstruction {
            identity: value.identity().clone(),
        });
    }

    let additional = match value.content() {
        InstructionContent::Text(text) => ByteCount::new(text.text().len() as u64),
        InstructionContent::Artifact(artifact) => {
            let modality = artifact.representation().modality();
            if modality != ArtifactModality::Text {
                return Err(ModelRequestError::UnsupportedInstructionArtifact {
                    index: values.len(),
                    modality,
                });
            }
            artifact.representation().byte_length()
        }
    };
    *content_bytes = checked_content_add(
        *content_bytes,
        additional,
        RequestInstructions::MAX_CONTENT_BYTES,
        ModelContentCollection::Instructions,
    )?;
    values.push(value);
    Ok(())
}

#[derive(Clone, Eq, PartialEq)]
struct RequestMessages {
    values: Box<[Message]>,
    content_bytes: ByteCount,
    modalities: ModelModalities,
}

impl RequestMessages {
    const MAX_LEN: usize = 256;
    const MAX_CONTENT_BYTES: ByteCount = ModelRequestLimits::HARD_MAX_CONTENT_BYTES;

    fn as_slice(&self) -> &[Message] {
        &self.values
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Debug for RequestMessages {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestMessages")
            .field("count", &self.len())
            .field("content_bytes", &self.content_bytes)
            .field("modalities", &self.modalities)
            .finish_non_exhaustive()
    }
}

impl Serialize for RequestMessages {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RequestMessages {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(RequestMessagesVisitor)
    }
}

struct RequestMessagesVisitor;

impl<'de> de::Visitor<'de> for RequestMessagesVisitor {
    type Value = RequestMessages;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {} unique durable messages",
            RequestMessages::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        let mut content_bytes = ByteCount::ZERO;
        let mut modalities = BTreeSet::new();
        while let Some(value) = sequence.next_element::<Message>()? {
            push_message(&mut values, &mut content_bytes, &mut modalities, value)
                .map_err(de::Error::custom)?;
        }
        Ok(RequestMessages {
            values: values.into_boxed_slice(),
            content_bytes,
            modalities: ModelModalities::try_new(modalities).map_err(de::Error::custom)?,
        })
    }
}

impl JsonSchema for RequestMessages {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelRequestMessages".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::RequestMessages").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<Message>(),
            "maxItems": 256,
            "uniqueItems": true,
            "description": "Ordered durable messages. Runtime additionally rejects duplicate message IDs, unsupported artifact modalities, and more than 67108864 resolved content bytes."
        })
    }
}

fn push_message(
    values: &mut Vec<Message>,
    content_bytes: &mut ByteCount,
    modalities: &mut BTreeSet<ModelModality>,
    value: Message,
) -> Result<(), ModelRequestError> {
    if values.len() == RequestMessages::MAX_LEN {
        return Err(ModelRequestError::TooManyMessages {
            max: RequestMessages::MAX_LEN,
            observed: RequestMessages::MAX_LEN + 1,
        });
    }
    if values
        .iter()
        .any(|existing| existing.message_id() == value.message_id())
    {
        return Err(ModelRequestError::DuplicateMessage {
            message_id: value.message_id(),
        });
    }

    for (part_index, part) in value.parts().iter().enumerate() {
        let additional = match part {
            ContentPart::Text(text) => {
                modalities.insert(ModelModality::Text);
                ByteCount::new(text.text().len() as u64)
            }
            ContentPart::Json(json) => {
                modalities.insert(ModelModality::Text);
                ByteCount::new(json.value().stats().compact_bytes() as u64)
            }
            ContentPart::Artifact(artifact) => {
                let artifact_modality = artifact.representation().modality();
                let model_modality = map_artifact_modality(artifact_modality).ok_or(
                    ModelRequestError::UnsupportedMessageArtifact {
                        message_index: values.len(),
                        part_index,
                        modality: artifact_modality,
                    },
                )?;
                modalities.insert(model_modality);
                artifact.representation().byte_length()
            }
        };
        *content_bytes = checked_content_add(
            *content_bytes,
            additional,
            RequestMessages::MAX_CONTENT_BYTES,
            ModelContentCollection::Messages,
        )?;
    }
    values.push(value);
    Ok(())
}

const fn map_artifact_modality(modality: ArtifactModality) -> Option<ModelModality> {
    match modality {
        ArtifactModality::Text => Some(ModelModality::Text),
        ArtifactModality::Image => Some(ModelModality::Image),
        ArtifactModality::Audio => Some(ModelModality::Audio),
        ArtifactModality::Video => Some(ModelModality::Video),
        ArtifactModality::Document => Some(ModelModality::Document),
        ArtifactModality::StructuredData | ArtifactModality::Archive | ArtifactModality::Binary => {
            None
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
struct RequestTools(BTreeMap<CapabilityName, ToolDescriptor>);

impl RequestTools {
    const MAX_LEN: usize = 128;

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn get(&self, name: &CapabilityName) -> Option<&ToolDescriptor> {
        self.0.get(name)
    }

    fn iter(&self) -> btree_map::Values<'_, CapabilityName, ToolDescriptor> {
        self.0.values()
    }
}

impl fmt::Debug for RequestTools {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestTools")
            .field("count", &self.len())
            .finish_non_exhaustive()
    }
}

impl Serialize for RequestTools {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for RequestTools {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(RequestToolsVisitor)
    }
}

struct RequestToolsVisitor;

impl<'de> de::Visitor<'de> for RequestToolsVisitor {
    type Value = RequestTools;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {} active or deprecated tools with unique names",
            RequestTools::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = BTreeMap::new();
        while let Some(value) = sequence.next_element::<ToolDescriptor>()? {
            insert_tool(&mut values, value).map_err(de::Error::custom)?;
        }
        Ok(RequestTools(values))
    }
}

impl JsonSchema for RequestTools {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelRequestTools".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::RequestTools").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ToolDescriptor>(),
            "maxItems": 128,
            "uniqueItems": true,
            "description": "Tools serialize in registry-local name order. Runtime rejects duplicate names across owners or versions and retired descriptors."
        })
    }
}

fn insert_tool(
    values: &mut BTreeMap<CapabilityName, ToolDescriptor>,
    value: ToolDescriptor,
) -> Result<(), ModelRequestError> {
    let name = value.metadata().identity().capability().name().clone();
    if values.contains_key(&name) {
        return Err(ModelRequestError::DuplicateToolName { name });
    }
    if values.len() == RequestTools::MAX_LEN {
        return Err(ModelRequestError::TooManyTools {
            max: RequestTools::MAX_LEN,
            observed: RequestTools::MAX_LEN + 1,
        });
    }
    if value.metadata().lifecycle().state() == CapabilityLifecycleState::Retired {
        return Err(ModelRequestError::RetiredTool { name });
    }
    values.insert(name, value);
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelContentCollection {
    Instructions,
    Messages,
}

fn checked_content_add(
    current: ByteCount,
    additional: ByteCount,
    maximum: ByteCount,
    collection: ModelContentCollection,
) -> Result<ByteCount, ModelRequestError> {
    let Some(actual) = current.checked_add(additional) else {
        return Err(match collection {
            ModelContentCollection::Instructions => {
                ModelRequestError::InstructionContentBytesOverflow
            }
            ModelContentCollection::Messages => ModelRequestError::MessageContentBytesOverflow,
        });
    };
    if actual > maximum {
        return Err(match collection {
            ModelContentCollection::Instructions => ModelRequestError::InstructionContentTooLarge {
                maximum,
                observed: actual,
            },
            ModelContentCollection::Messages => ModelRequestError::MessageContentTooLarge {
                maximum,
                observed: actual,
            },
        });
    }
    Ok(actual)
}

/// Immutable, fully normalized request passed to a model adapter.
///
/// Construction derives input modalities and every provider-neutral capability
/// requirement from the supplied content and controls. Adapters must validate
/// registered extensions and referenced schemas before provider I/O; unsupported
/// extensions are errors and are never silently dropped. Provider-side history,
/// automatic truncation, storage, background execution, and multi-candidate
/// generation are intentionally outside this durable core request.
///
/// Structural validation does not authenticate serialized instruction owners or
/// tool descriptors. Public protocol/API adapters must resolve those values from
/// an authenticated tenant registry instead of accepting caller-supplied claims.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequest {
    instructions: RequestInstructions,
    messages: RequestMessages,
    tools: RequestTools,
    tool_selection: ModelToolSelection,
    max_tool_calls_per_response: ExecutionCount,
    strict_tool_arguments: bool,
    output_modalities: ModelModalities,
    #[serde(skip_serializing_if = "Option::is_none")]
    text_output_format: Option<ModelTextOutputFormat>,
    response_mode: ModelResponseMode,
    reasoning_summaries: bool,
    limits: ModelRequestLimits,
    extensions: Extensions,
    requirements: ModelRequirements,
}

impl ModelRequest {
    /// Maximum tool calls accepted from one model response.
    pub const MAX_TOOL_CALLS_PER_RESPONSE: ExecutionCount = ExecutionCount::new(1024);

    /// Starts a bounded request builder with explicit finite limits.
    #[must_use]
    pub fn builder(limits: ModelRequestLimits) -> ModelRequestBuilder {
        ModelRequestBuilder::new(limits)
    }

    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        instructions: RequestInstructions,
        messages: RequestMessages,
        tools: RequestTools,
        tool_selection: ModelToolSelection,
        max_tool_calls_per_response: ExecutionCount,
        strict_tool_arguments: bool,
        output_modalities: ModelModalities,
        text_output_format: Option<ModelTextOutputFormat>,
        response_mode: ModelResponseMode,
        reasoning_summaries: bool,
        limits: ModelRequestLimits,
        extensions: Extensions,
        wire_requirements: Option<&ModelRequirements>,
    ) -> Result<Self, ModelRequestError> {
        if instructions.is_empty() && messages.is_empty() {
            return Err(ModelRequestError::EmptyInput);
        }
        if output_modalities.is_empty() {
            return Err(ModelRequestError::EmptyOutputModalities);
        }
        let has_text_output = output_modalities.contains(ModelModality::Text);
        match (has_text_output, text_output_format.is_some()) {
            (true, false) => return Err(ModelRequestError::MissingTextOutputFormat),
            (false, true) => return Err(ModelRequestError::UnexpectedTextOutputFormat),
            _ => {}
        }

        validate_tool_controls(
            &tools,
            &tool_selection,
            max_tool_calls_per_response,
            strict_tool_arguments,
        )?;

        let content_bytes = instructions
            .content_bytes
            .checked_add(messages.content_bytes)
            .expect("validated instruction and message content bounds cannot overflow");
        if content_bytes > limits.max_content_bytes() {
            return Err(ModelRequestError::RequestContentTooLarge {
                maximum: limits.max_content_bytes(),
                observed: content_bytes,
            });
        }

        let mut input_modalities = BTreeSet::new();
        if !instructions.is_empty() {
            input_modalities.insert(ModelModality::Text);
        }
        input_modalities.extend(messages.modalities.iter().copied());
        let input_modalities = ModelModalities::try_new(input_modalities)
            .expect("a BTreeSet of closed modalities is valid");

        let tool_requirements = if tools.is_empty() {
            ModelToolRequirements::none()
        } else {
            ModelToolRequirements::new(
                ExecutionCount::new(tools.len() as u64),
                max_tool_calls_per_response,
                ModelToolChoices::try_new([tool_selection.choice()])
                    .expect("one closed tool choice is valid"),
                strict_tool_arguments,
            )
            .map_err(ModelRequestError::InvalidToolRequirements)?
        };
        let structured_output = text_output_format
            .as_ref()
            .map_or(ModelStructuredOutputLevel::Unsupported, |format| {
                format.required_level()
            });
        let requirements = ModelRequirements::new(
            input_modalities,
            output_modalities.clone(),
            response_mode == ModelResponseMode::Streaming,
            tool_requirements,
            structured_output,
            reasoning_summaries,
            Some(limits.required_context_tokens()),
            Some(limits.max_input_tokens()),
            Some(limits.max_output_tokens()),
        )
        .map_err(ModelRequestError::InvalidRequirements)?;

        if wire_requirements.is_some_and(|wire| wire != &requirements) {
            return Err(ModelRequestError::RequirementsMismatch);
        }

        Ok(Self {
            instructions,
            messages,
            tools,
            tool_selection,
            max_tool_calls_per_response,
            strict_tool_arguments,
            output_modalities,
            text_output_format,
            response_mode,
            reasoning_summaries,
            limits,
            extensions,
            requirements,
        })
    }

    /// Returns ordered application-owned instructions.
    #[must_use]
    pub fn instructions(&self) -> &[Instruction] {
        self.instructions.as_slice()
    }

    /// Returns ordered durable conversation messages.
    #[must_use]
    pub fn messages(&self) -> &[Message] {
        self.messages.as_slice()
    }

    /// Iterates available tool descriptors in canonical name order.
    pub fn tools(&self) -> impl ExactSizeIterator<Item = &ToolDescriptor> {
        self.tools.iter()
    }

    /// Returns one available tool by registry-local name.
    #[must_use]
    pub fn tool(&self, name: &CapabilityName) -> Option<&ToolDescriptor> {
        self.tools.get(name)
    }

    /// Returns the requested tool-selection behavior.
    #[must_use]
    pub const fn tool_selection(&self) -> &ModelToolSelection {
        &self.tool_selection
    }

    /// Returns the finite tool-call ceiling for one response.
    #[must_use]
    pub const fn max_tool_calls_per_response(&self) -> ExecutionCount {
        self.max_tool_calls_per_response
    }

    /// Returns whether every complete tool argument must be provider-constrained.
    #[must_use]
    pub const fn requires_strict_tool_arguments(&self) -> bool {
        self.strict_tool_arguments
    }

    /// Returns requested output modalities.
    #[must_use]
    pub const fn output_modalities(&self) -> &ModelModalities {
        &self.output_modalities
    }

    /// Returns text output format, absent for non-text-only output.
    #[must_use]
    pub const fn text_output_format(&self) -> Option<&ModelTextOutputFormat> {
        self.text_output_format.as_ref()
    }

    /// Returns complete or streaming response delivery.
    #[must_use]
    pub const fn response_mode(&self) -> ModelResponseMode {
        self.response_mode
    }

    /// Returns whether a readable provider reasoning summary was requested.
    #[must_use]
    pub const fn requires_reasoning_summaries(&self) -> bool {
        self.reasoning_summaries
    }

    /// Returns finite request limits.
    #[must_use]
    pub const fn limits(&self) -> &ModelRequestLimits {
        &self.limits
    }

    /// Returns bounded registered provider/adapter extension values.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Returns normalized requirements derived from this exact request.
    #[must_use]
    pub const fn requirements(&self) -> &ModelRequirements {
        &self.requirements
    }

    /// Returns resolved instruction and message content bytes.
    #[must_use]
    pub fn content_bytes(&self) -> ByteCount {
        self.instructions
            .content_bytes
            .checked_add(self.messages.content_bytes)
            .expect("validated model request content bytes cannot overflow")
    }
}

fn validate_tool_controls(
    tools: &RequestTools,
    selection: &ModelToolSelection,
    max_calls: ExecutionCount,
    strict_arguments: bool,
) -> Result<(), ModelRequestError> {
    if tools.is_empty() {
        if selection.choice() != ModelToolChoice::None {
            return Err(ModelRequestError::ToolSelectionWithoutTools {
                selection: selection.choice(),
            });
        }
        if max_calls.get() != 0 {
            return Err(ModelRequestError::ToolCallsWithoutTools { actual: max_calls });
        }
        if strict_arguments {
            return Err(ModelRequestError::StrictArgumentsWithoutTools);
        }
        return Ok(());
    }

    if selection.choice() == ModelToolChoice::None {
        return Err(ModelRequestError::ToolsRequireActiveSelection);
    }
    if max_calls.get() == 0 {
        return Err(ModelRequestError::ZeroToolCalls);
    }
    if max_calls > ModelRequest::MAX_TOOL_CALLS_PER_RESPONSE {
        return Err(ModelRequestError::TooManyToolCalls {
            maximum: ModelRequest::MAX_TOOL_CALLS_PER_RESPONSE,
            actual: max_calls,
        });
    }
    if let Some(name) = selection.specific_name() {
        if tools.get(name).is_none() {
            return Err(ModelRequestError::SpecificToolNotFound { name: name.clone() });
        }
    }
    Ok(())
}

impl fmt::Debug for ModelRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelRequest")
            .field("instructions", &self.instructions)
            .field("messages", &self.messages)
            .field("tools", &self.tools)
            .field("tool_selection", &self.tool_selection)
            .field(
                "max_tool_calls_per_response",
                &self.max_tool_calls_per_response,
            )
            .field("strict_tool_arguments", &self.strict_tool_arguments)
            .field("output_modalities", &self.output_modalities)
            .field("text_output_format", &self.text_output_format)
            .field("response_mode", &self.response_mode)
            .field("reasoning_summaries", &self.reasoning_summaries)
            .field("limits", &self.limits)
            .field("extensions", &self.extensions)
            .field("requirements", &self.requirements)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelRequestWire {
    instructions: RequestInstructions,
    messages: RequestMessages,
    tools: RequestTools,
    tool_selection: ModelToolSelection,
    max_tool_calls_per_response: ExecutionCount,
    strict_tool_arguments: bool,
    output_modalities: ModelModalities,
    #[serde(default)]
    text_output_format: Option<ModelTextOutputFormat>,
    response_mode: ModelResponseMode,
    reasoning_summaries: bool,
    limits: ModelRequestLimits,
    extensions: Extensions,
    requirements: ModelRequirements,
}

impl<'de> Deserialize<'de> for ModelRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelRequestWire::deserialize(deserializer)?;
        Self::from_parts(
            wire.instructions,
            wire.messages,
            wire.tools,
            wire.tool_selection,
            wire.max_tool_calls_per_response,
            wire.strict_tool_arguments,
            wire.output_modalities,
            wire.text_output_format,
            wire.response_mode,
            wire.reasoning_summaries,
            wire.limits,
            wire.extensions,
            Some(&wire.requirements),
        )
        .map_err(de::Error::custom)
    }
}

/// Fluent, fail-closed builder for [`ModelRequest`].
///
/// Collection methods retain the first validation error and stop accepting
/// further collection values, so fluent construction cannot grow beyond hard
/// limits before [`Self::build`] reports the error.
pub struct ModelRequestBuilder {
    instructions: Vec<Instruction>,
    instruction_content_bytes: ByteCount,
    messages: Vec<Message>,
    message_content_bytes: ByteCount,
    message_modalities: BTreeSet<ModelModality>,
    tools: BTreeMap<CapabilityName, ToolDescriptor>,
    tool_selection: ModelToolSelection,
    max_tool_calls_per_response: ExecutionCount,
    strict_tool_arguments: bool,
    output_modalities: ModelModalities,
    text_output_format: Option<ModelTextOutputFormat>,
    response_mode: ModelResponseMode,
    reasoning_summaries: bool,
    limits: ModelRequestLimits,
    extensions: Extensions,
    error: Option<ModelRequestError>,
}

impl ModelRequestBuilder {
    fn new(limits: ModelRequestLimits) -> Self {
        Self {
            instructions: Vec::new(),
            instruction_content_bytes: ByteCount::ZERO,
            messages: Vec::new(),
            message_content_bytes: ByteCount::ZERO,
            message_modalities: BTreeSet::new(),
            tools: BTreeMap::new(),
            tool_selection: ModelToolSelection::none(),
            max_tool_calls_per_response: ExecutionCount::ZERO,
            strict_tool_arguments: false,
            output_modalities: ModelModalities::try_new([ModelModality::Text])
                .expect("one closed modality is valid"),
            text_output_format: Some(ModelTextOutputFormat::text()),
            response_mode: ModelResponseMode::Complete,
            reasoning_summaries: false,
            limits,
            extensions: Extensions::default(),
            error: None,
        }
    }

    /// Appends one ordered trusted instruction.
    #[must_use]
    pub fn instruction(mut self, instruction: Instruction) -> Self {
        if self.error.is_none() {
            if let Err(error) = push_instruction(
                &mut self.instructions,
                &mut self.instruction_content_bytes,
                instruction,
            ) {
                self.error = Some(error);
            }
        }
        self
    }

    /// Appends one ordered durable message.
    #[must_use]
    pub fn message(mut self, message: Message) -> Self {
        if self.error.is_none() {
            if let Err(error) = push_message(
                &mut self.messages,
                &mut self.message_content_bytes,
                &mut self.message_modalities,
                message,
            ) {
                self.error = Some(error);
            }
        }
        self
    }

    /// Adds one tool descriptor under its registry-local name.
    #[must_use]
    pub fn tool(mut self, tool: ToolDescriptor) -> Self {
        if self.error.is_none() {
            if let Err(error) = insert_tool(&mut self.tools, tool) {
                self.error = Some(error);
            }
        }
        self
    }

    /// Sets tool-selection behavior.
    #[must_use]
    pub fn tool_selection(mut self, selection: ModelToolSelection) -> Self {
        self.tool_selection = selection;
        self
    }

    /// Sets the finite per-response tool-call ceiling.
    #[must_use]
    pub const fn max_tool_calls_per_response(mut self, maximum: ExecutionCount) -> Self {
        self.max_tool_calls_per_response = maximum;
        self
    }

    /// Requires provider-constrained complete tool arguments.
    #[must_use]
    pub const fn strict_tool_arguments(mut self, required: bool) -> Self {
        self.strict_tool_arguments = required;
        self
    }

    /// Replaces requested output modalities.
    #[must_use]
    pub fn output_modalities(mut self, modalities: ModelModalities) -> Self {
        self.output_modalities = modalities;
        self
    }

    /// Sets text output format, or removes it for non-text-only output.
    #[must_use]
    pub fn text_output_format(mut self, format: Option<ModelTextOutputFormat>) -> Self {
        self.text_output_format = format;
        self
    }

    /// Selects complete or streaming response delivery.
    #[must_use]
    pub const fn response_mode(mut self, mode: ModelResponseMode) -> Self {
        self.response_mode = mode;
        self
    }

    /// Requests or disables a readable provider reasoning summary.
    #[must_use]
    pub const fn reasoning_summaries(mut self, required: bool) -> Self {
        self.reasoning_summaries = required;
        self
    }

    /// Replaces bounded registered extension values.
    #[must_use]
    pub fn extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Validates cross-field invariants and derives normalized requirements.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRequestError`] for the first resource, identity, modality,
    /// tool-control, output-format, or derived-requirement violation.
    pub fn build(self) -> Result<ModelRequest, ModelRequestError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        ModelRequest::from_parts(
            RequestInstructions {
                values: self.instructions.into_boxed_slice(),
                content_bytes: self.instruction_content_bytes,
            },
            RequestMessages {
                values: self.messages.into_boxed_slice(),
                content_bytes: self.message_content_bytes,
                modalities: ModelModalities::try_new(self.message_modalities)
                    .expect("a BTreeSet of closed modalities is valid"),
            },
            RequestTools(self.tools),
            self.tool_selection,
            self.max_tool_calls_per_response,
            self.strict_tool_arguments,
            self.output_modalities,
            self.text_output_format,
            self.response_mode,
            self.reasoning_summaries,
            self.limits,
            self.extensions,
            None,
        )
    }
}

impl fmt::Debug for ModelRequestBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelRequestBuilder")
            .field("instruction_count", &self.instructions.len())
            .field("instruction_content_bytes", &self.instruction_content_bytes)
            .field("message_count", &self.messages.len())
            .field("message_content_bytes", &self.message_content_bytes)
            .field("tool_count", &self.tools.len())
            .field("tool_selection", &self.tool_selection)
            .field(
                "max_tool_calls_per_response",
                &self.max_tool_calls_per_response,
            )
            .field("strict_tool_arguments", &self.strict_tool_arguments)
            .field("output_modalities", &self.output_modalities)
            .field("text_output_format", &self.text_output_format)
            .field("response_mode", &self.response_mode)
            .field("reasoning_summaries", &self.reasoning_summaries)
            .field("limits", &self.limits)
            .field("extensions", &self.extensions)
            .field("has_error", &self.error.is_some())
            .finish_non_exhaustive()
    }
}

/// Invalid provider-neutral model request.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelRequestError {
    /// No trusted instruction or durable message was supplied.
    #[error("model request requires at least one instruction or message")]
    EmptyInput,
    /// Trusted instruction count exceeded its hard ceiling.
    #[error("model request contains at least {observed} instructions; maximum is {max}")]
    TooManyInstructions {
        /// Immutable v1 maximum.
        max: usize,
        /// Minimum observed count before validation stopped.
        observed: usize,
    },
    /// One instruction identity appeared more than once.
    #[error("model request repeats instruction identity {identity:?}")]
    DuplicateInstruction {
        /// Repeated identity.
        identity: InstructionIdentity,
    },
    /// A non-text artifact was used as a trusted model instruction.
    #[error("model request instruction {index} has unsupported artifact modality {modality:?}")]
    UnsupportedInstructionArtifact {
        /// Zero-based instruction index.
        index: usize,
        /// Rejected artifact modality.
        modality: ArtifactModality,
    },
    /// Durable message count exceeded its hard ceiling.
    #[error("model request contains at least {observed} messages; maximum is {max}")]
    TooManyMessages {
        /// Immutable v1 maximum.
        max: usize,
        /// Minimum observed count before validation stopped.
        observed: usize,
    },
    /// One durable message ID appeared more than once.
    #[error("model request repeats message ID {message_id}")]
    DuplicateMessage {
        /// Repeated durable ID.
        message_id: MessageId,
    },
    /// A message artifact has no portable model modality mapping.
    #[error(
        "model request message {message_index} part {part_index} has unsupported artifact modality {modality:?}"
    )]
    UnsupportedMessageArtifact {
        /// Zero-based message index.
        message_index: usize,
        /// Zero-based content-part index.
        part_index: usize,
        /// Rejected artifact modality.
        modality: ArtifactModality,
    },
    /// Resolved instruction content exceeded its hard ceiling.
    #[error("model request instruction content is {observed} bytes; maximum is {maximum}")]
    InstructionContentTooLarge {
        /// Immutable instruction-content maximum.
        maximum: ByteCount,
        /// First observed aggregate above the maximum.
        observed: ByteCount,
    },
    /// Resolved message content exceeded its hard ceiling.
    #[error("model request message content is {observed} bytes; maximum is {maximum}")]
    MessageContentTooLarge {
        /// Immutable message-content maximum.
        maximum: ByteCount,
        /// First observed aggregate above the maximum.
        observed: ByteCount,
    },
    /// Instruction content byte accounting could not be represented.
    #[error("model request instruction content-byte accounting overflowed")]
    InstructionContentBytesOverflow,
    /// Message content byte accounting could not be represented.
    #[error("model request message content-byte accounting overflowed")]
    MessageContentBytesOverflow,
    /// Combined instruction and message bytes exceeded the caller ceiling.
    #[error("model request content is {observed} bytes; configured maximum is {maximum}")]
    RequestContentTooLarge {
        /// Configured finite ceiling.
        maximum: ByteCount,
        /// Resolved content bytes.
        observed: ByteCount,
    },
    /// Tool count exceeded its hard ceiling.
    #[error("model request contains at least {observed} tools; maximum is {max}")]
    TooManyTools {
        /// Immutable v1 maximum.
        max: usize,
        /// Minimum observed count before validation stopped.
        observed: usize,
    },
    /// Two descriptors exposed the same provider-visible name.
    #[error("model request contains duplicate tool name {name}")]
    DuplicateToolName {
        /// Colliding registry-local name.
        name: CapabilityName,
    },
    /// A retired descriptor was selected for new execution.
    #[error("model request cannot select retired tool {name}")]
    RetiredTool {
        /// Retired tool name.
        name: CapabilityName,
    },
    /// Tool selection was active without any definitions.
    #[error("model request tool selection {selection:?} requires at least one tool")]
    ToolSelectionWithoutTools {
        /// Invalid active selection.
        selection: ModelToolChoice,
    },
    /// A call ceiling was present without tool definitions.
    #[error("model request without tools cannot allow {actual} tool calls")]
    ToolCallsWithoutTools {
        /// Invalid non-zero call ceiling.
        actual: ExecutionCount,
    },
    /// Strict arguments were requested without tools.
    #[error("model request without tools cannot require strict tool arguments")]
    StrictArgumentsWithoutTools,
    /// Tool definitions were supplied while selection was disabled.
    #[error("model request with tools requires auto, required, or specific selection")]
    ToolsRequireActiveSelection,
    /// Active tools had no possible response call capacity.
    #[error("model request with tools requires a positive tool-call ceiling")]
    ZeroToolCalls,
    /// The per-response call ceiling exceeded the immutable resource limit.
    #[error("model request tool-call ceiling {actual} exceeds maximum {maximum}")]
    TooManyToolCalls {
        /// Immutable v1 maximum.
        maximum: ExecutionCount,
        /// Rejected caller ceiling.
        actual: ExecutionCount,
    },
    /// Specific selection named no supplied descriptor.
    #[error("model request selected unavailable tool {name}")]
    SpecificToolNotFound {
        /// Missing provider-visible name.
        name: CapabilityName,
    },
    /// No response modality was requested.
    #[error("model request requires at least one output modality")]
    EmptyOutputModalities,
    /// Text output lacked an explicit format.
    #[error("model request with text output requires an explicit text output format")]
    MissingTextOutputFormat,
    /// A text format was attached to non-text-only output.
    #[error("model request without text output cannot declare a text output format")]
    UnexpectedTextOutputFormat,
    /// Internal normalized tool requirements were not coherent.
    #[error("model request could not derive coherent tool requirements: {0}")]
    InvalidToolRequirements(crate::ModelToolRequirementsError),
    /// Internal normalized model requirements were not coherent.
    #[error("model request could not derive coherent requirements: {0}")]
    InvalidRequirements(ModelRequirementsError),
    /// Serialized derived requirements did not match request fields.
    #[error("model request serialized requirements do not match normalized request fields")]
    RequirementsMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArtifactRef, MessageParts};
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    fn fixture(path: &[&str], source: &str) -> Value {
        let mut value = serde_json::from_str::<Value>(source).unwrap();
        for segment in path {
            value = value[*segment].clone();
        }
        value
    }

    fn instruction() -> Instruction {
        let values = fixture(
            &["instructions", "valid"],
            include_str!("../tests/fixtures/core-message-v1.json"),
        );
        from_value(values[0].clone()).unwrap()
    }

    fn message() -> Message {
        let values = fixture(
            &["messages", "valid"],
            include_str!("../tests/fixtures/core-message-v1.json"),
        );
        from_value(values[0].clone()).unwrap()
    }

    fn tool_value(name: &str) -> Value {
        let values = fixture(
            &["descriptors", "valid"],
            include_str!("../tests/fixtures/core-tool-v1.json"),
        );
        let mut value = values[0].clone();
        value["metadata"]["identity"]["capability"]["name"] = Value::from(name);
        value
    }

    fn tool(name: &str) -> ToolDescriptor {
        from_value(tool_value(name)).unwrap()
    }

    fn artifact(
        modality: ArtifactModality,
        byte_length: ByteCount,
        application_controlled: bool,
    ) -> ArtifactRef {
        let values = fixture(
            &["artifact_refs", "valid"],
            include_str!("../tests/fixtures/core-artifact-v1.json"),
        );
        let mut value = values[0].clone();
        value["representation"]["modality"] = to_value(modality).unwrap();
        value["representation"]["byte_length"] = Value::from(byte_length.get().to_string());
        if application_controlled {
            value["metadata"]["trust"] = Value::from("application_controlled");
        }
        from_value(value).unwrap()
    }

    fn instruction_artifact(modality: ArtifactModality, byte_length: ByteCount) -> Instruction {
        let base = instruction();
        Instruction::new(
            base.identity().clone(),
            artifact(modality, byte_length, true).into(),
            base.provenance().clone(),
        )
        .unwrap()
    }

    fn message_artifact(modality: ArtifactModality, byte_length: ByteCount) -> Message {
        let base = message();
        Message::new(
            base.message_id(),
            base.role(),
            MessageParts::try_new([ContentPart::from(artifact(modality, byte_length, false))])
                .unwrap(),
            base.provenance().clone(),
        )
        .unwrap()
    }

    fn retired_tool(name: &str) -> ToolDescriptor {
        let mut value = tool_value(name);
        value["metadata"]["lifecycle"] = json!({
            "status": "retired",
            "retired_at": "2027-02-28T00:00:00.000001Z",
            "notice": "Retained only for durable history."
        });
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

    fn modalities(values: impl IntoIterator<Item = ModelModality>) -> ModelModalities {
        ModelModalities::try_new(values).unwrap()
    }

    fn base_builder() -> ModelRequestBuilder {
        ModelRequest::builder(limits()).instruction(instruction())
    }

    fn base_request() -> ModelRequest {
        base_builder().message(message()).build().unwrap()
    }

    #[test]
    fn request_control_enums_have_closed_canonical_wire_forms() {
        assert_eq!(to_value(ModelResponseMode::Complete).unwrap(), "complete");
        assert_eq!(to_value(ModelResponseMode::Streaming).unwrap(), "streaming");
        assert!(from_value::<ModelResponseMode>(json!("incremental")).is_err());

        assert_eq!(
            to_value(ModelToolSelection::none()).unwrap(),
            json!({"mode": "none"})
        );
        assert_eq!(
            to_value(ModelToolSelection::auto()).unwrap(),
            json!({"mode": "auto"})
        );
        assert_eq!(
            to_value(ModelToolSelection::required()).unwrap(),
            json!({"mode": "required"})
        );
        let name = CapabilityName::new("payments.capture").unwrap();
        let specific = ModelToolSelection::specific(name.clone());
        assert_eq!(specific.choice(), ModelToolChoice::Specific);
        assert_eq!(specific.specific_name(), Some(&name));
        assert_eq!(
            to_value(specific).unwrap(),
            json!({"mode": "specific", "name": "payments.capture"})
        );
        assert!(from_value::<ModelToolSelection>(json!({"mode": "none", "name": "x"})).is_err());

        assert_eq!(
            to_value(ModelTextOutputFormat::text()).unwrap(),
            json!({"type": "text"})
        );
        assert_eq!(
            to_value(ModelTextOutputFormat::json()).unwrap(),
            json!({"type": "json"})
        );
        assert!(from_value::<ModelTextOutputFormat>(json!({"type": "xml"})).is_err());
    }

    #[test]
    fn limits_are_positive_bounded_checked_and_closed() {
        assert_eq!(limits().required_context_tokens(), TokenCount::new(9_216));
        assert_eq!(
            ModelRequestLimits::new(TokenCount::ZERO, TokenCount::new(1), ByteCount::new(1)),
            Err(ModelRequestLimitsError::ZeroInputTokens)
        );
        assert_eq!(
            ModelRequestLimits::new(TokenCount::new(1), TokenCount::ZERO, ByteCount::new(1)),
            Err(ModelRequestLimitsError::ZeroOutputTokens)
        );
        assert_eq!(
            ModelRequestLimits::new(TokenCount::new(1), TokenCount::new(1), ByteCount::ZERO),
            Err(ModelRequestLimitsError::ZeroContentBytes)
        );
        assert_eq!(
            ModelRequestLimits::new(
                TokenCount::new(1),
                TokenCount::new(1),
                ByteCount::new(ModelRequestLimits::HARD_MAX_CONTENT_BYTES.get() + 1)
            ),
            Err(ModelRequestLimitsError::ContentBytesAboveHardMaximum {
                maximum: ModelRequestLimits::HARD_MAX_CONTENT_BYTES,
                actual: ByteCount::new(ModelRequestLimits::HARD_MAX_CONTENT_BYTES.get() + 1),
            })
        );
        assert_eq!(
            ModelRequestLimits::new(TokenCount::MAX, TokenCount::new(1), ByteCount::new(1)),
            Err(ModelRequestLimitsError::ContextTokensOverflow)
        );

        let encoded = to_value(limits()).unwrap();
        assert_eq!(
            from_value::<ModelRequestLimits>(encoded.clone()).unwrap(),
            limits()
        );
        let mut unknown = encoded;
        unknown["unlimited"] = Value::Bool(true);
        assert!(from_value::<ModelRequestLimits>(unknown).is_err());
    }

    #[test]
    fn default_request_derives_exact_requirements_and_redacts_payloads() {
        let request = base_request();
        assert_eq!(request.instructions(), &[instruction()]);
        assert_eq!(request.messages(), &[message()]);
        assert_eq!(request.tools().len(), 0);
        assert_eq!(request.tool_selection(), &ModelToolSelection::none());
        assert_eq!(request.max_tool_calls_per_response(), ExecutionCount::ZERO);
        assert!(!request.requires_strict_tool_arguments());
        assert_eq!(request.response_mode(), ModelResponseMode::Complete);
        assert_eq!(
            request.text_output_format(),
            Some(&ModelTextOutputFormat::text())
        );

        let requirements = request.requirements();
        assert!(
            requirements
                .input_modalities()
                .contains(ModelModality::Text)
        );
        assert!(
            requirements
                .output_modalities()
                .contains(ModelModality::Text)
        );
        assert!(!requirements.requires_streaming());
        assert!(!requirements.tools().requires_tool_calling());
        assert_eq!(
            requirements.structured_output(),
            ModelStructuredOutputLevel::Unsupported
        );
        assert!(!requirements.requires_reasoning_summaries());
        assert_eq!(
            requirements.min_context_tokens(),
            Some(TokenCount::new(9_216))
        );
        assert_eq!(
            requirements.min_input_tokens(),
            Some(TokenCount::new(8_192))
        );
        assert_eq!(
            requirements.min_output_tokens(),
            Some(TokenCount::new(1_024))
        );
        assert!(request.content_bytes().get() > 0);

        let encoded = to_value(&request).unwrap();
        assert_eq!(from_value::<ModelRequest>(encoded).unwrap(), request);
        let debug = format!("{request:?}");
        assert!(!debug.contains("Return a typed incident summary"));
        assert!(!debug.contains("Investigate incident 42"));
    }

    #[test]
    fn tools_are_canonical_and_derive_exact_selection_requirements() {
        let selected = CapabilityName::new("alpha.lookup").unwrap();
        let request = base_builder()
            .tool(tool("zeta.lookup"))
            .tool(tool("alpha.lookup"))
            .tool_selection(ModelToolSelection::specific(selected.clone()))
            .max_tool_calls_per_response(ExecutionCount::new(4))
            .strict_tool_arguments(true)
            .build()
            .unwrap();

        let names = request
            .tools()
            .map(|descriptor| {
                descriptor
                    .metadata()
                    .identity()
                    .capability()
                    .name()
                    .as_str()
            })
            .collect::<Vec<_>>();
        assert_eq!(names, ["alpha.lookup", "zeta.lookup"]);
        assert!(request.tool(&selected).is_some());
        let requirements = request.requirements().tools();
        assert_eq!(requirements.min_definitions(), ExecutionCount::new(2));
        assert_eq!(
            requirements.min_calls_per_response(),
            ExecutionCount::new(4)
        );
        assert!(requirements.choices().contains(ModelToolChoice::Specific));
        assert!(requirements.requires_strict_arguments());
    }

    #[test]
    fn tool_controls_and_registry_names_fail_closed() {
        assert_eq!(
            base_builder()
                .tool_selection(ModelToolSelection::auto())
                .build(),
            Err(ModelRequestError::ToolSelectionWithoutTools {
                selection: ModelToolChoice::Auto,
            })
        );
        assert_eq!(
            base_builder()
                .max_tool_calls_per_response(ExecutionCount::new(1))
                .build(),
            Err(ModelRequestError::ToolCallsWithoutTools {
                actual: ExecutionCount::new(1),
            })
        );
        assert_eq!(
            base_builder().strict_tool_arguments(true).build(),
            Err(ModelRequestError::StrictArgumentsWithoutTools)
        );
        assert_eq!(
            base_builder().tool(tool("payments.capture")).build(),
            Err(ModelRequestError::ToolsRequireActiveSelection)
        );
        assert_eq!(
            base_builder()
                .tool(tool("payments.capture"))
                .tool_selection(ModelToolSelection::auto())
                .build(),
            Err(ModelRequestError::ZeroToolCalls)
        );
        assert_eq!(
            base_builder()
                .tool(tool("payments.capture"))
                .tool_selection(ModelToolSelection::auto())
                .max_tool_calls_per_response(ExecutionCount::new(1_025))
                .build(),
            Err(ModelRequestError::TooManyToolCalls {
                maximum: ModelRequest::MAX_TOOL_CALLS_PER_RESPONSE,
                actual: ExecutionCount::new(1_025),
            })
        );

        let missing = CapabilityName::new("missing.tool").unwrap();
        assert_eq!(
            base_builder()
                .tool(tool("payments.capture"))
                .tool_selection(ModelToolSelection::specific(missing.clone()))
                .max_tool_calls_per_response(ExecutionCount::new(1))
                .build(),
            Err(ModelRequestError::SpecificToolNotFound { name: missing })
        );
        assert_eq!(
            base_builder()
                .tool(tool("payments.capture"))
                .tool(tool("payments.capture"))
                .build(),
            Err(ModelRequestError::DuplicateToolName {
                name: CapabilityName::new("payments.capture").unwrap(),
            })
        );
        assert_eq!(
            base_builder().tool(retired_tool("retired.tool")).build(),
            Err(ModelRequestError::RetiredTool {
                name: CapabilityName::new("retired.tool").unwrap(),
            })
        );
    }

    #[test]
    fn output_controls_derive_structured_streaming_and_reasoning_requirements() {
        let schema = tool("payments.capture").input_schema().clone();
        let request = base_builder()
            .text_output_format(Some(ModelTextOutputFormat::json_schema(schema.clone())))
            .response_mode(ModelResponseMode::Streaming)
            .reasoning_summaries(true)
            .build()
            .unwrap();
        assert_eq!(
            request
                .text_output_format()
                .and_then(ModelTextOutputFormat::schema),
            Some(&schema)
        );
        assert!(request.requirements().requires_streaming());
        assert!(request.requirements().requires_reasoning_summaries());
        assert_eq!(
            request.requirements().structured_output(),
            ModelStructuredOutputLevel::JsonSchema
        );

        assert_eq!(
            base_builder()
                .output_modalities(ModelModalities::empty())
                .text_output_format(None)
                .build(),
            Err(ModelRequestError::EmptyOutputModalities)
        );
        assert_eq!(
            base_builder().text_output_format(None).build(),
            Err(ModelRequestError::MissingTextOutputFormat)
        );
        assert_eq!(
            base_builder()
                .output_modalities(modalities([ModelModality::Image]))
                .build(),
            Err(ModelRequestError::UnexpectedTextOutputFormat)
        );

        let image = base_builder()
            .output_modalities(modalities([ModelModality::Image]))
            .text_output_format(None)
            .build()
            .unwrap();
        assert_eq!(image.text_output_format(), None);
        assert!(
            image
                .requirements()
                .output_modalities()
                .contains(ModelModality::Image)
        );
    }

    #[test]
    fn duplicate_inputs_and_request_content_limits_are_enforced() {
        let duplicate_instruction = instruction();
        assert_eq!(
            ModelRequest::builder(limits())
                .instruction(duplicate_instruction.clone())
                .instruction(duplicate_instruction.clone())
                .build(),
            Err(ModelRequestError::DuplicateInstruction {
                identity: duplicate_instruction.identity().clone(),
            })
        );

        let duplicate_message = message();
        assert_eq!(
            ModelRequest::builder(limits())
                .message(duplicate_message.clone())
                .message(duplicate_message.clone())
                .build(),
            Err(ModelRequestError::DuplicateMessage {
                message_id: duplicate_message.message_id(),
            })
        );

        let tiny =
            ModelRequestLimits::new(TokenCount::new(1), TokenCount::new(1), ByteCount::new(1))
                .unwrap();
        assert!(matches!(
            ModelRequest::builder(tiny)
                .instruction(instruction())
                .build(),
            Err(ModelRequestError::RequestContentTooLarge { .. })
        ));
        assert_eq!(
            ModelRequest::builder(limits()).build(),
            Err(ModelRequestError::EmptyInput)
        );
    }

    #[test]
    fn collection_count_ceilings_stop_at_the_first_excess_value() {
        let instruction_wire = to_value(instruction()).unwrap();
        let mut instruction_builder = ModelRequest::builder(limits());
        for index in 0..=RequestInstructions::MAX_LEN {
            let mut value = instruction_wire.clone();
            value["identity"]["name"] = Value::from(format!("policy.{index}"));
            instruction_builder = instruction_builder.instruction(from_value(value).unwrap());
        }
        assert_eq!(
            instruction_builder.build(),
            Err(ModelRequestError::TooManyInstructions {
                max: RequestInstructions::MAX_LEN,
                observed: RequestInstructions::MAX_LEN + 1,
            })
        );

        let message_wire = to_value(message()).unwrap();
        let mut message_builder = ModelRequest::builder(limits());
        for index in 0..=RequestMessages::MAX_LEN {
            let mut value = message_wire.clone();
            value["message_id"] = Value::from(format!("01912345-6789-7abc-8def-{index:012x}"));
            message_builder = message_builder.message(from_value(value).unwrap());
        }
        assert_eq!(
            message_builder.build(),
            Err(ModelRequestError::TooManyMessages {
                max: RequestMessages::MAX_LEN,
                observed: RequestMessages::MAX_LEN + 1,
            })
        );

        let tool_wire = to_value(tool("bounded.tool")).unwrap();
        let mut tool_builder = base_builder();
        for index in 0..=RequestTools::MAX_LEN {
            let mut value = tool_wire.clone();
            value["metadata"]["identity"]["capability"]["name"] =
                Value::from(format!("tool.{index}"));
            tool_builder = tool_builder.tool(from_value(value).unwrap());
        }
        assert_eq!(
            tool_builder.build(),
            Err(ModelRequestError::TooManyTools {
                max: RequestTools::MAX_LEN,
                observed: RequestTools::MAX_LEN + 1,
            })
        );
    }

    #[test]
    fn artifact_modalities_and_resolved_byte_bounds_fail_closed() {
        for (artifact_modality, model_modality) in [
            (ArtifactModality::Text, ModelModality::Text),
            (ArtifactModality::Image, ModelModality::Image),
            (ArtifactModality::Audio, ModelModality::Audio),
            (ArtifactModality::Video, ModelModality::Video),
            (ArtifactModality::Document, ModelModality::Document),
        ] {
            let request = ModelRequest::builder(limits())
                .message(message_artifact(artifact_modality, ByteCount::new(12)))
                .build()
                .unwrap();
            assert!(
                request
                    .requirements()
                    .input_modalities()
                    .contains(model_modality)
            );
        }

        for modality in [
            ArtifactModality::StructuredData,
            ArtifactModality::Archive,
            ArtifactModality::Binary,
        ] {
            assert!(matches!(
                ModelRequest::builder(limits())
                    .message(message_artifact(modality, ByteCount::new(12)))
                    .build(),
                Err(ModelRequestError::UnsupportedMessageArtifact {
                    message_index: 0,
                    part_index: 0,
                    modality: actual,
                }) if actual == modality
            ));
        }

        assert_eq!(
            ModelRequest::builder(limits())
                .instruction(instruction_artifact(
                    ArtifactModality::Image,
                    ByteCount::new(12),
                ))
                .build(),
            Err(ModelRequestError::UnsupportedInstructionArtifact {
                index: 0,
                modality: ArtifactModality::Image,
            })
        );
        assert!(matches!(
            ModelRequest::builder(limits())
                .instruction(instruction_artifact(
                    ArtifactModality::Text,
                    ByteCount::new(RequestInstructions::MAX_CONTENT_BYTES.get() + 1),
                ))
                .build(),
            Err(ModelRequestError::InstructionContentTooLarge { .. })
        ));
        assert!(matches!(
            ModelRequest::builder(limits())
                .message(message_artifact(
                    ArtifactModality::Document,
                    ByteCount::new(RequestMessages::MAX_CONTENT_BYTES.get() + 1),
                ))
                .build(),
            Err(ModelRequestError::MessageContentTooLarge { .. })
        ));
    }

    #[test]
    fn derived_wire_requirements_and_closed_fields_cannot_be_tampered() {
        let request = base_request();
        let mut tampered = to_value(&request).unwrap();
        tampered["requirements"]["streaming"] = Value::Bool(true);
        assert!(from_value::<ModelRequest>(tampered).is_err());

        let mut unknown = to_value(&request).unwrap();
        unknown["store"] = Value::Bool(true);
        assert!(from_value::<ModelRequest>(unknown).is_err());

        let encoded = serde_json::to_string(&request).unwrap();
        let duplicate = encoded.replacen(
            "\"response_mode\":\"complete\"",
            "\"response_mode\":\"complete\",\"response_mode\":\"streaming\"",
            1,
        );
        assert_ne!(duplicate, encoded);
        assert!(serde_json::from_str::<ModelRequest>(&duplicate).is_err());
    }

    #[test]
    fn request_schemas_publish_closed_objects_and_resource_bounds() {
        for schema in [
            to_value(schemars::schema_for!(ModelRequestLimits)).unwrap(),
            to_value(schemars::schema_for!(ModelRequest)).unwrap(),
        ] {
            assert_eq!(schema["type"], "object");
            assert_eq!(schema["additionalProperties"], false);
        }

        let request = to_value(schemars::schema_for!(ModelRequest)).unwrap();
        let definitions = request["$defs"].as_object().unwrap();
        assert_eq!(
            definitions["ModelRequestInstructions"]["maxItems"],
            RequestInstructions::MAX_LEN
        );
        assert_eq!(
            definitions["ModelRequestMessages"]["maxItems"],
            RequestMessages::MAX_LEN
        );
        assert_eq!(
            definitions["ModelRequestTools"]["maxItems"],
            RequestTools::MAX_LEN
        );
    }

    proptest! {
        #[test]
        fn every_valid_request_limit_tuple_and_derived_request_round_trip(
            input in 1_u64..=1_000_000,
            output in 1_u64..=1_000_000,
            bytes in 1_u64..=ModelRequestLimits::HARD_MAX_CONTENT_BYTES.get(),
        ) {
            let limits = ModelRequestLimits::new(
                TokenCount::new(input),
                TokenCount::new(output),
                ByteCount::new(bytes),
            ).unwrap();
            let encoded = serde_json::to_vec(&limits).unwrap();
            prop_assert_eq!(
                serde_json::from_slice::<ModelRequestLimits>(&encoded).unwrap(),
                limits.clone()
            );
            prop_assert_eq!(limits.required_context_tokens(), TokenCount::new(input + output));

            if bytes >= 31 {
                let request = ModelRequest::builder(limits).instruction(instruction()).build().unwrap();
                let encoded = serde_json::to_vec(&request).unwrap();
                prop_assert_eq!(serde_json::from_slice::<ModelRequest>(&encoded).unwrap(), request);
            }
        }
    }
}
