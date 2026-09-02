// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Provider-neutral model capability discovery and negotiation contracts.

use std::{
    collections::{BTreeSet, btree_set},
    fmt,
};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess},
};
use thiserror::Error;

use crate::{CapabilityKind, CapabilityMetadata, ExecutionCount, SchemaReference, TokenCount};

/// Coarse semantic media understood or produced by a model binding.
///
/// A modality is a negotiation hint, not a media-type allowlist, byte
/// validator, or authorization decision. Adapter profiles retain the exact
/// formats, counts, dimensions, durations, and byte ceilings accepted by one
/// provider endpoint.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ModelModality {
    /// Natural-language or code text.
    Text,
    /// Raster or vector image input or output.
    Image,
    /// Audio input or output.
    Audio,
    /// Video input or output.
    Video,
    /// Human-oriented document input or output.
    Document,
}

/// Sorted, duplicate-free model modality set.
///
/// The JSON wire form is an array. Serialization always uses enum order;
/// construction and deserialization reject duplicates and resource excess.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelModalities(BTreeSet<ModelModality>);

impl ModelModalities {
    /// Maximum number of distinct closed modalities.
    pub const MAX_LEN: usize = 5;

    /// Constructs an empty modality set.
    #[must_use]
    pub const fn empty() -> Self {
        Self(BTreeSet::new())
    }

    /// Constructs a sorted, duplicate-free modality set.
    ///
    /// # Errors
    ///
    /// Returns [`ModelModalitiesError`] when a modality is repeated or the
    /// hard collection ceiling is exceeded.
    pub fn try_new<I>(modalities: I) -> Result<Self, ModelModalitiesError>
    where
        I: IntoIterator<Item = ModelModality>,
    {
        let mut values = BTreeSet::new();
        for modality in modalities {
            if values.contains(&modality) {
                return Err(ModelModalitiesError::Duplicate { modality });
            }
            if values.len() == Self::MAX_LEN {
                return Err(ModelModalitiesError::TooMany {
                    max: Self::MAX_LEN,
                    observed: Self::MAX_LEN + 1,
                });
            }
            values.insert(modality);
        }
        Ok(Self(values))
    }

    /// Returns the number of modalities.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the set contains no modality.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns whether the exact modality is present.
    #[must_use]
    pub fn contains(&self, modality: ModelModality) -> bool {
        self.0.contains(&modality)
    }

    /// Iterates in stable enum order.
    pub fn iter(&self) -> btree_set::Iter<'_, ModelModality> {
        self.0.iter()
    }

    /// Returns whether every modality is present in another set.
    #[must_use]
    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }
}

impl<'a> IntoIterator for &'a ModelModalities {
    type Item = &'a ModelModality;
    type IntoIter = btree_set::Iter<'a, ModelModality>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<Vec<ModelModality>> for ModelModalities {
    type Error = ModelModalitiesError;

    fn try_from(modalities: Vec<ModelModality>) -> Result<Self, Self::Error> {
        Self::try_new(modalities)
    }
}

impl Serialize for ModelModalities {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for ModelModalities {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ModelModalitiesVisitor)
    }
}

struct ModelModalitiesVisitor;

impl<'de> de::Visitor<'de> for ModelModalitiesVisitor {
    type Value = ModelModalities;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {} unique model modalities",
            ModelModalities::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = BTreeSet::new();
        while let Some(modality) = sequence.next_element::<ModelModality>()? {
            if values.contains(&modality) {
                return Err(de::Error::custom(ModelModalitiesError::Duplicate {
                    modality,
                }));
            }
            if values.len() == ModelModalities::MAX_LEN {
                return Err(de::Error::custom(ModelModalitiesError::TooMany {
                    max: ModelModalities::MAX_LEN,
                    observed: ModelModalities::MAX_LEN + 1,
                }));
            }
            values.insert(modality);
        }
        Ok(ModelModalities(values))
    }
}

impl JsonSchema for ModelModalities {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelModalities".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ModelModalities").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ModelModality>(),
            "maxItems": 5,
            "uniqueItems": true
        })
    }
}

/// Invalid model modality set.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelModalitiesError {
    /// One modality appeared more than once.
    #[error("model modality set contains duplicate {modality:?}")]
    Duplicate {
        /// Repeated modality.
        modality: ModelModality,
    },

    /// The set exceeded its closed hard ceiling.
    #[error("model modality set contains at least {observed} values; maximum is {max}")]
    TooMany {
        /// Maximum accepted number of values.
        max: usize,
        /// Minimum number observed before validation stopped.
        observed: usize,
    },
}

/// Provider-neutral tool-selection control.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ModelToolChoice {
    /// The model decides whether and which tool to call.
    Auto,
    /// Tool calls are disabled for this request.
    None,
    /// At least one supplied tool must be called.
    Required,
    /// One named supplied tool must be called.
    Specific,
}

/// Sorted, duplicate-free set of tool-selection controls.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelToolChoices(BTreeSet<ModelToolChoice>);

impl ModelToolChoices {
    /// Maximum number of distinct closed tool-choice modes.
    pub const MAX_LEN: usize = 4;

    /// Constructs an empty choice set.
    #[must_use]
    pub const fn empty() -> Self {
        Self(BTreeSet::new())
    }

    /// Constructs a sorted, duplicate-free choice set.
    ///
    /// # Errors
    ///
    /// Returns [`ModelToolChoicesError`] for duplicates or resource excess.
    pub fn try_new<I>(choices: I) -> Result<Self, ModelToolChoicesError>
    where
        I: IntoIterator<Item = ModelToolChoice>,
    {
        let mut values = BTreeSet::new();
        for choice in choices {
            if values.contains(&choice) {
                return Err(ModelToolChoicesError::Duplicate { choice });
            }
            if values.len() == Self::MAX_LEN {
                return Err(ModelToolChoicesError::TooMany {
                    max: Self::MAX_LEN,
                    observed: Self::MAX_LEN + 1,
                });
            }
            values.insert(choice);
        }
        Ok(Self(values))
    }

    /// Returns the number of choice modes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no explicit mode is represented.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns whether a choice mode is present.
    #[must_use]
    pub fn contains(&self, choice: ModelToolChoice) -> bool {
        self.0.contains(&choice)
    }

    /// Iterates in stable enum order.
    pub fn iter(&self) -> btree_set::Iter<'_, ModelToolChoice> {
        self.0.iter()
    }

    /// Returns whether every choice is present in another set.
    #[must_use]
    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }
}

impl<'a> IntoIterator for &'a ModelToolChoices {
    type Item = &'a ModelToolChoice;
    type IntoIter = btree_set::Iter<'a, ModelToolChoice>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<Vec<ModelToolChoice>> for ModelToolChoices {
    type Error = ModelToolChoicesError;

    fn try_from(choices: Vec<ModelToolChoice>) -> Result<Self, Self::Error> {
        Self::try_new(choices)
    }
}

impl Serialize for ModelToolChoices {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for ModelToolChoices {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ModelToolChoicesVisitor)
    }
}

struct ModelToolChoicesVisitor;

impl<'de> de::Visitor<'de> for ModelToolChoicesVisitor {
    type Value = ModelToolChoices;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {} unique model tool choices",
            ModelToolChoices::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = BTreeSet::new();
        while let Some(choice) = sequence.next_element::<ModelToolChoice>()? {
            if values.contains(&choice) {
                return Err(de::Error::custom(ModelToolChoicesError::Duplicate {
                    choice,
                }));
            }
            if values.len() == ModelToolChoices::MAX_LEN {
                return Err(de::Error::custom(ModelToolChoicesError::TooMany {
                    max: ModelToolChoices::MAX_LEN,
                    observed: ModelToolChoices::MAX_LEN + 1,
                }));
            }
            values.insert(choice);
        }
        Ok(ModelToolChoices(values))
    }
}

impl JsonSchema for ModelToolChoices {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelToolChoices".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ModelToolChoices").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ModelToolChoice>(),
            "maxItems": 4,
            "uniqueItems": true
        })
    }
}

/// Invalid model tool-choice set.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelToolChoicesError {
    /// One tool-choice mode appeared more than once.
    #[error("model tool-choice set contains duplicate {choice:?}")]
    Duplicate {
        /// Repeated choice mode.
        choice: ModelToolChoice,
    },

    /// The set exceeded its closed hard ceiling.
    #[error("model tool-choice set contains at least {observed} values; maximum is {max}")]
    TooMany {
        /// Maximum accepted number of values.
        max: usize,
        /// Minimum number observed before validation stopped.
        observed: usize,
    },
}

/// Explicitly known token ceilings for one model-and-endpoint binding.
///
/// Context is the total provider-tokenized active context, including request
/// content and reserved/generated output. Input and output are independent
/// provider ceilings and need not be simultaneously attainable. Unknown values
/// are represented by absence and fail any positive capacity requirement; they
/// are never treated as unlimited.
#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTokenLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    max_context_tokens: Option<TokenCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_input_tokens: Option<TokenCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<TokenCount>,
}

impl ModelTokenLimits {
    /// Constructs and cross-validates known token ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`ModelTokenLimitsError`] when a known ceiling is zero or a
    /// component ceiling exceeds a known total context ceiling.
    pub const fn new(
        max_context_tokens: Option<TokenCount>,
        max_input_tokens: Option<TokenCount>,
        max_output_tokens: Option<TokenCount>,
    ) -> Result<Self, ModelTokenLimitsError> {
        if matches!(max_context_tokens, Some(value) if value.get() == 0) {
            return Err(ModelTokenLimitsError::ZeroContext);
        }
        if matches!(max_input_tokens, Some(value) if value.get() == 0) {
            return Err(ModelTokenLimitsError::ZeroInput);
        }
        if matches!(max_output_tokens, Some(value) if value.get() == 0) {
            return Err(ModelTokenLimitsError::ZeroOutput);
        }
        if let (Some(context), Some(input)) = (max_context_tokens, max_input_tokens) {
            if input.get() > context.get() {
                return Err(ModelTokenLimitsError::InputExceedsContext { input, context });
            }
        }
        if let (Some(context), Some(output)) = (max_context_tokens, max_output_tokens) {
            if output.get() > context.get() {
                return Err(ModelTokenLimitsError::OutputExceedsContext { output, context });
            }
        }
        Ok(Self {
            max_context_tokens,
            max_input_tokens,
            max_output_tokens,
        })
    }

    /// Constructs a capability snapshot with no provider-published token limits.
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            max_context_tokens: None,
            max_input_tokens: None,
            max_output_tokens: None,
        }
    }

    /// Returns the known total context ceiling.
    #[must_use]
    pub const fn max_context_tokens(&self) -> Option<TokenCount> {
        self.max_context_tokens
    }

    /// Returns the known input-token ceiling.
    #[must_use]
    pub const fn max_input_tokens(&self) -> Option<TokenCount> {
        self.max_input_tokens
    }

    /// Returns the known output-token ceiling.
    #[must_use]
    pub const fn max_output_tokens(&self) -> Option<TokenCount> {
        self.max_output_tokens
    }
}

#[allow(clippy::struct_field_names)]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelTokenLimitsWire {
    #[serde(default)]
    max_context_tokens: Option<TokenCount>,
    #[serde(default)]
    max_input_tokens: Option<TokenCount>,
    #[serde(default)]
    max_output_tokens: Option<TokenCount>,
}

impl<'de> Deserialize<'de> for ModelTokenLimits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelTokenLimitsWire::deserialize(deserializer)?;
        Self::new(
            wire.max_context_tokens,
            wire.max_input_tokens,
            wire.max_output_tokens,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid model token-limit snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelTokenLimitsError {
    /// A published total context ceiling was zero.
    #[error("known model context token limit must be greater than zero")]
    ZeroContext,
    /// A published input ceiling was zero.
    #[error("known model input token limit must be greater than zero")]
    ZeroInput,
    /// A published output ceiling was zero.
    #[error("known model output token limit must be greater than zero")]
    ZeroOutput,
    /// The independent input ceiling exceeded the total context ceiling.
    #[error("model input token limit {input} exceeds context limit {context}")]
    InputExceedsContext {
        /// Invalid input ceiling.
        input: TokenCount,
        /// Known total context ceiling.
        context: TokenCount,
    },
    /// The independent output ceiling exceeded the total context ceiling.
    #[error("model output token limit {output} exceeds context limit {context}")]
    OutputExceedsContext {
        /// Invalid output ceiling.
        output: TokenCount,
        /// Known total context ceiling.
        context: TokenCount,
    },
}

/// Model support for tool definitions, selection, and emitted calls.
///
/// The schema profile identifies a trusted, digest-pinned document describing
/// the provider subset accepted for tool input schemas. It is not fetched from
/// the network at invocation time. Strict arguments guarantee provider-side
/// schema-constrained generation only for complete tool-call items; `StateKnot`
/// still validates every decoded argument locally.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_profile: Option<SchemaReference>,
    max_definitions: ExecutionCount,
    max_calls_per_response: ExecutionCount,
    choices: ModelToolChoices,
    strict_arguments: bool,
}

impl ModelToolCapabilities {
    /// Constructs and cross-validates tool-call capabilities.
    ///
    /// Absence of a schema profile represents unsupported tool calling and
    /// requires every other tool field to be empty, zero, or false.
    ///
    /// # Errors
    ///
    /// Returns [`ModelToolCapabilitiesError`] for an incoherent unsupported
    /// state, zero supported capacity, or missing automatic selection.
    pub fn new(
        schema_profile: Option<SchemaReference>,
        max_definitions: ExecutionCount,
        max_calls_per_response: ExecutionCount,
        choices: ModelToolChoices,
        strict_arguments: bool,
    ) -> Result<Self, ModelToolCapabilitiesError> {
        if schema_profile.is_none() {
            if max_definitions.get() != 0
                || max_calls_per_response.get() != 0
                || !choices.is_empty()
                || strict_arguments
            {
                return Err(ModelToolCapabilitiesError::UnsupportedHasCapabilities);
            }
        } else {
            if max_definitions.get() == 0 {
                return Err(ModelToolCapabilitiesError::ZeroDefinitions);
            }
            if max_calls_per_response.get() == 0 {
                return Err(ModelToolCapabilitiesError::ZeroCallsPerResponse);
            }
            if !choices.contains(ModelToolChoice::Auto) {
                return Err(ModelToolCapabilitiesError::MissingAutoChoice);
            }
        }
        Ok(Self {
            schema_profile,
            max_definitions,
            max_calls_per_response,
            choices,
            strict_arguments,
        })
    }

    /// Constructs an unsupported tool-call capability.
    #[must_use]
    pub fn unsupported() -> Self {
        Self {
            schema_profile: None,
            max_definitions: ExecutionCount::ZERO,
            max_calls_per_response: ExecutionCount::ZERO,
            choices: ModelToolChoices::empty(),
            strict_arguments: false,
        }
    }

    /// Returns whether this model binding accepts tool definitions.
    #[must_use]
    pub const fn supports_tool_calling(&self) -> bool {
        self.schema_profile.is_some()
    }

    /// Returns the accepted tool-schema profile when tool calling is supported.
    #[must_use]
    pub const fn schema_profile(&self) -> Option<&SchemaReference> {
        self.schema_profile.as_ref()
    }

    /// Returns the maximum tool definitions accepted in one request.
    #[must_use]
    pub const fn max_definitions(&self) -> ExecutionCount {
        self.max_definitions
    }

    /// Returns the maximum tool calls emitted in one response.
    #[must_use]
    pub const fn max_calls_per_response(&self) -> ExecutionCount {
        self.max_calls_per_response
    }

    /// Returns whether more than one tool call may appear in one response.
    #[must_use]
    pub const fn supports_parallel_calls(&self) -> bool {
        self.max_calls_per_response.get() > 1
    }

    /// Returns supported tool-selection controls.
    #[must_use]
    pub const fn choices(&self) -> &ModelToolChoices {
        &self.choices
    }

    /// Returns whether complete tool arguments are provider-constrained to schema.
    #[must_use]
    pub const fn supports_strict_arguments(&self) -> bool {
        self.strict_arguments
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelToolCapabilitiesWire {
    #[serde(default)]
    schema_profile: Option<SchemaReference>,
    max_definitions: ExecutionCount,
    max_calls_per_response: ExecutionCount,
    choices: ModelToolChoices,
    strict_arguments: bool,
}

impl<'de> Deserialize<'de> for ModelToolCapabilities {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelToolCapabilitiesWire::deserialize(deserializer)?;
        Self::new(
            wire.schema_profile,
            wire.max_definitions,
            wire.max_calls_per_response,
            wire.choices,
            wire.strict_arguments,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid model tool capability declaration.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelToolCapabilitiesError {
    /// Unsupported tool calling carried active tool fields.
    #[error("unsupported model tool calling cannot declare capacities, choices, or strict mode")]
    UnsupportedHasCapabilities,
    /// A supported binding accepted no tool definitions.
    #[error("supported model tool calling requires a positive definition capacity")]
    ZeroDefinitions,
    /// A supported binding could emit no tool call.
    #[error("supported model tool calling requires a positive call capacity")]
    ZeroCallsPerResponse,
    /// Tool calling lacked its baseline automatic selection mode.
    #[error("supported model tool calling must include the auto choice")]
    MissingAutoChoice,
}

/// Strength of structured final-output support.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ModelStructuredOutputLevel {
    /// No machine-enforced structured output is required or supported.
    Unsupported,
    /// Complete normal output is guaranteed to be valid JSON, not a schema.
    Json,
    /// Complete normal output is constrained to an accepted JSON Schema subset.
    JsonSchema,
}

/// Structured final-output support for one model binding.
///
/// Refusals, safety interruptions, and token truncation remain distinct terminal
/// outcomes and need not satisfy a requested schema. Every nominally complete
/// structured output is still locally parsed and validated.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelStructuredOutputCapabilities {
    level: ModelStructuredOutputLevel,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_profile: Option<SchemaReference>,
}

impl ModelStructuredOutputCapabilities {
    /// Constructs a structured-output declaration.
    ///
    /// # Errors
    ///
    /// Returns [`ModelStructuredOutputCapabilitiesError`] unless a schema
    /// profile is present exactly for the JSON Schema capability level.
    pub fn new(
        level: ModelStructuredOutputLevel,
        schema_profile: Option<SchemaReference>,
    ) -> Result<Self, ModelStructuredOutputCapabilitiesError> {
        match (level, schema_profile.is_some()) {
            (ModelStructuredOutputLevel::JsonSchema, false) => {
                return Err(ModelStructuredOutputCapabilitiesError::MissingSchemaProfile);
            }
            (ModelStructuredOutputLevel::Unsupported | ModelStructuredOutputLevel::Json, true) => {
                return Err(ModelStructuredOutputCapabilitiesError::UnexpectedSchemaProfile);
            }
            _ => {}
        }
        Ok(Self {
            level,
            schema_profile,
        })
    }

    /// Constructs an unsupported structured-output declaration.
    #[must_use]
    pub const fn unsupported() -> Self {
        Self {
            level: ModelStructuredOutputLevel::Unsupported,
            schema_profile: None,
        }
    }

    /// Constructs valid-JSON support without schema adherence.
    #[must_use]
    pub const fn json() -> Self {
        Self {
            level: ModelStructuredOutputLevel::Json,
            schema_profile: None,
        }
    }

    /// Constructs JSON Schema-constrained support.
    #[must_use]
    pub const fn json_schema(schema_profile: SchemaReference) -> Self {
        Self {
            level: ModelStructuredOutputLevel::JsonSchema,
            schema_profile: Some(schema_profile),
        }
    }

    /// Returns the structured-output support level.
    #[must_use]
    pub const fn level(&self) -> ModelStructuredOutputLevel {
        self.level
    }

    /// Returns the accepted structured-output schema profile.
    #[must_use]
    pub const fn schema_profile(&self) -> Option<&SchemaReference> {
        self.schema_profile.as_ref()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelStructuredOutputCapabilitiesWire {
    level: ModelStructuredOutputLevel,
    #[serde(default)]
    schema_profile: Option<SchemaReference>,
}

impl<'de> Deserialize<'de> for ModelStructuredOutputCapabilities {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelStructuredOutputCapabilitiesWire::deserialize(deserializer)?;
        Self::new(wire.level, wire.schema_profile).map_err(de::Error::custom)
    }
}

/// Invalid structured-output capability declaration.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelStructuredOutputCapabilitiesError {
    /// JSON Schema support omitted the accepted profile identity.
    #[error("JSON Schema structured output requires a schema profile")]
    MissingSchemaProfile,
    /// A weaker capability carried a meaningless schema profile.
    #[error("only JSON Schema structured output may declare a schema profile")]
    UnexpectedSchemaProfile,
}

/// Immutable capabilities of one exact model, adapter, and endpoint binding.
///
/// These values are trusted-registry evidence, not permanent facts about model
/// weights. A runtime snapshots them with an attempt, applies policy and tighter
/// limits, validates schemas against local profiles, and still handles provider
/// rejection or drift.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCapabilities {
    input_modalities: ModelModalities,
    output_modalities: ModelModalities,
    streaming: bool,
    tools: ModelToolCapabilities,
    structured_output: ModelStructuredOutputCapabilities,
    reasoning_summaries: bool,
    token_limits: ModelTokenLimits,
}

impl ModelCapabilities {
    /// Constructs and cross-validates a model capability snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ModelCapabilitiesError`] for empty modality sets or a
    /// text-oriented feature on a binding that lacks the corresponding text
    /// input or output modality.
    pub fn new(
        input_modalities: ModelModalities,
        output_modalities: ModelModalities,
        streaming: bool,
        tools: ModelToolCapabilities,
        structured_output: ModelStructuredOutputCapabilities,
        reasoning_summaries: bool,
        token_limits: ModelTokenLimits,
    ) -> Result<Self, ModelCapabilitiesError> {
        if input_modalities.is_empty() {
            return Err(ModelCapabilitiesError::EmptyInputModalities);
        }
        if output_modalities.is_empty() {
            return Err(ModelCapabilitiesError::EmptyOutputModalities);
        }
        if tools.supports_tool_calling() && !input_modalities.contains(ModelModality::Text) {
            return Err(ModelCapabilitiesError::ToolCallingRequiresTextInput);
        }
        if tools.supports_tool_calling() && !output_modalities.contains(ModelModality::Text) {
            return Err(ModelCapabilitiesError::ToolCallingRequiresTextOutput);
        }
        if structured_output.level() != ModelStructuredOutputLevel::Unsupported
            && !output_modalities.contains(ModelModality::Text)
        {
            return Err(ModelCapabilitiesError::StructuredOutputRequiresTextOutput);
        }
        if reasoning_summaries && !output_modalities.contains(ModelModality::Text) {
            return Err(ModelCapabilitiesError::ReasoningSummaryRequiresTextOutput);
        }
        Ok(Self {
            input_modalities,
            output_modalities,
            streaming,
            tools,
            structured_output,
            reasoning_summaries,
            token_limits,
        })
    }

    /// Returns accepted coarse input modalities.
    #[must_use]
    pub const fn input_modalities(&self) -> &ModelModalities {
        &self.input_modalities
    }

    /// Returns emitted coarse output modalities.
    #[must_use]
    pub const fn output_modalities(&self) -> &ModelModalities {
        &self.output_modalities
    }

    /// Returns whether incremental response events are supported.
    #[must_use]
    pub const fn supports_streaming(&self) -> bool {
        self.streaming
    }

    /// Returns tool-call capabilities.
    #[must_use]
    pub const fn tools(&self) -> &ModelToolCapabilities {
        &self.tools
    }

    /// Returns structured final-output capabilities.
    #[must_use]
    pub const fn structured_output(&self) -> &ModelStructuredOutputCapabilities {
        &self.structured_output
    }

    /// Returns whether an explicitly requested readable reasoning summary can be emitted.
    #[must_use]
    pub const fn supports_reasoning_summaries(&self) -> bool {
        self.reasoning_summaries
    }

    /// Returns explicitly known token ceilings.
    #[must_use]
    pub const fn token_limits(&self) -> &ModelTokenLimits {
        &self.token_limits
    }

    /// Validates all provider-neutral requirements without invoking the provider.
    ///
    /// An actual tool or output schema must additionally be resolved from the
    /// trusted local registry and validated against the returned pinned schema
    /// profile. This method never silently drops a requirement.
    ///
    /// # Errors
    ///
    /// Returns [`ModelCapabilityMismatch`] containing every unmet dimension.
    pub fn satisfies(
        &self,
        requirements: &ModelRequirements,
    ) -> Result<(), ModelCapabilityMismatch> {
        let mut unmet = BTreeSet::new();

        for modality in requirements.input_modalities() {
            if !self.input_modalities.contains(*modality) {
                unmet.insert(ModelCapabilityIssue::InputModality {
                    required: *modality,
                });
            }
        }
        for modality in requirements.output_modalities() {
            if !self.output_modalities.contains(*modality) {
                unmet.insert(ModelCapabilityIssue::OutputModality {
                    required: *modality,
                });
            }
        }
        if requirements.requires_streaming() && !self.streaming {
            unmet.insert(ModelCapabilityIssue::Streaming {});
        }

        let required_tools = requirements.tools();
        if required_tools.requires_tool_calling() {
            if !self.tools.supports_tool_calling() {
                unmet.insert(ModelCapabilityIssue::ToolCalling {});
            }
            if self.tools.max_definitions() < required_tools.min_definitions() {
                unmet.insert(ModelCapabilityIssue::ToolDefinitions {
                    required: required_tools.min_definitions(),
                    available: self.tools.max_definitions(),
                });
            }
            if self.tools.max_calls_per_response() < required_tools.min_calls_per_response() {
                unmet.insert(ModelCapabilityIssue::ToolCallsPerResponse {
                    required: required_tools.min_calls_per_response(),
                    available: self.tools.max_calls_per_response(),
                });
            }
            for choice in required_tools.choices() {
                if !self.tools.choices().contains(*choice) {
                    unmet.insert(ModelCapabilityIssue::ToolChoice { required: *choice });
                }
            }
            if required_tools.requires_strict_arguments() && !self.tools.supports_strict_arguments()
            {
                unmet.insert(ModelCapabilityIssue::StrictToolArguments {});
            }
        }

        let required_structured = requirements.structured_output();
        if self.structured_output.level() < required_structured {
            unmet.insert(ModelCapabilityIssue::StructuredOutput {
                required: required_structured,
                available: self.structured_output.level(),
            });
        }
        if requirements.requires_reasoning_summaries() && !self.reasoning_summaries {
            unmet.insert(ModelCapabilityIssue::ReasoningSummary {});
        }

        if let Some(required) = requirements.min_context_tokens() {
            if !known_capacity_satisfies(self.token_limits.max_context_tokens(), required) {
                unmet.insert(ModelCapabilityIssue::ContextTokens {
                    required,
                    available: self.token_limits.max_context_tokens(),
                });
            }
        }
        if let Some(required) = requirements.min_input_tokens() {
            if !known_capacity_satisfies(self.token_limits.max_input_tokens(), required) {
                unmet.insert(ModelCapabilityIssue::InputTokens {
                    required,
                    available: self.token_limits.max_input_tokens(),
                });
            }
        }
        if let Some(required) = requirements.min_output_tokens() {
            if !known_capacity_satisfies(self.token_limits.max_output_tokens(), required) {
                unmet.insert(ModelCapabilityIssue::OutputTokens {
                    required,
                    available: self.token_limits.max_output_tokens(),
                });
            }
        }

        if unmet.is_empty() {
            Ok(())
        } else {
            Err(ModelCapabilityMismatch(unmet))
        }
    }
}

fn known_capacity_satisfies(available: Option<TokenCount>, required: TokenCount) -> bool {
    available.is_some_and(|available| available >= required)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelCapabilitiesWire {
    input_modalities: ModelModalities,
    output_modalities: ModelModalities,
    streaming: bool,
    tools: ModelToolCapabilities,
    structured_output: ModelStructuredOutputCapabilities,
    reasoning_summaries: bool,
    token_limits: ModelTokenLimits,
}

impl<'de> Deserialize<'de> for ModelCapabilities {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelCapabilitiesWire::deserialize(deserializer)?;
        Self::new(
            wire.input_modalities,
            wire.output_modalities,
            wire.streaming,
            wire.tools,
            wire.structured_output,
            wire.reasoning_summaries,
            wire.token_limits,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid cross-component model capability declaration.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelCapabilitiesError {
    /// No accepted input was declared.
    #[error("model capabilities require at least one input modality")]
    EmptyInputModalities,
    /// No emitted output was declared.
    #[error("model capabilities require at least one output modality")]
    EmptyOutputModalities,
    /// Tool definitions and prompts require text input.
    #[error("model tool calling requires the text input modality")]
    ToolCallingRequiresTextInput,
    /// Agent tool loops require text-capable model output.
    #[error("model tool calling requires the text output modality")]
    ToolCallingRequiresTextOutput,
    /// JSON output is carried through the text output boundary.
    #[error("model structured output requires the text output modality")]
    StructuredOutputRequiresTextOutput,
    /// Readable reasoning summaries are text output blocks.
    #[error("model reasoning summaries require the text output modality")]
    ReasoningSummaryRequiresTextOutput,
}

/// Immutable, protocol-neutral description of one executable model binding.
///
/// The owner-qualified metadata identity is the stable `StateKnot` registry key;
/// provider model names, aliases, endpoints, regions, and adapter configuration
/// remain in the registry's versioned execution binding because their formats
/// and mutability differ across providers. Registration authenticates the
/// metadata owner, resolves aliases to the intended provider binding, validates
/// every referenced schema profile locally, and snapshots this descriptor for
/// each execution attempt.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDescriptor {
    metadata: CapabilityMetadata,
    capabilities: ModelCapabilities,
}

impl ModelDescriptor {
    /// Constructs a descriptor and validates its specialized classification.
    ///
    /// # Errors
    ///
    /// Returns [`ModelDescriptorError`] when common metadata is not classified
    /// as a model.
    pub fn new(
        metadata: CapabilityMetadata,
        capabilities: ModelCapabilities,
    ) -> Result<Self, ModelDescriptorError> {
        if metadata.kind() != CapabilityKind::Model {
            return Err(ModelDescriptorError::WrongCapabilityKind {
                actual: metadata.kind(),
            });
        }
        Ok(Self {
            metadata,
            capabilities,
        })
    }

    /// Returns common identity, discovery, lifecycle, scope, and extension data.
    #[must_use]
    pub const fn metadata(&self) -> &CapabilityMetadata {
        &self.metadata
    }

    /// Returns capabilities for this exact registered execution binding.
    #[must_use]
    pub const fn capabilities(&self) -> &ModelCapabilities {
        &self.capabilities
    }
}

impl fmt::Debug for ModelDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelDescriptor")
            .field("metadata", &self.metadata)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelDescriptorWire {
    metadata: CapabilityMetadata,
    capabilities: ModelCapabilities,
}

impl<'de> Deserialize<'de> for ModelDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelDescriptorWire::deserialize(deserializer)?;
        Self::new(wire.metadata, wire.capabilities).map_err(de::Error::custom)
    }
}

/// Invalid cross-component model descriptor.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelDescriptorError {
    /// Common metadata classified the capability as something other than a model.
    #[error("model descriptor requires kind=model, received {actual:?}")]
    WrongCapabilityKind {
        /// Conflicting capability kind.
        actual: CapabilityKind,
    },
}

/// Minimum tool support needed by one model request.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelToolRequirements {
    min_definitions: ExecutionCount,
    min_calls_per_response: ExecutionCount,
    choices: ModelToolChoices,
    strict_arguments: bool,
}

impl ModelToolRequirements {
    /// Constructs and validates tool requirements.
    ///
    /// Definition and call minima must both be zero or both be positive. Choice
    /// and strict-mode requirements are meaningful only in the positive case.
    ///
    /// # Errors
    ///
    /// Returns [`ModelToolRequirementsError`] for an incoherent inactive state
    /// or mismatched zero capacities.
    pub fn new(
        min_definitions: ExecutionCount,
        min_calls_per_response: ExecutionCount,
        choices: ModelToolChoices,
        strict_arguments: bool,
    ) -> Result<Self, ModelToolRequirementsError> {
        if (min_definitions.get() == 0) != (min_calls_per_response.get() == 0) {
            return Err(ModelToolRequirementsError::CapacityMismatch);
        }
        if min_definitions.get() == 0 && (!choices.is_empty() || strict_arguments) {
            return Err(ModelToolRequirementsError::InactiveHasRequirements);
        }
        Ok(Self {
            min_definitions,
            min_calls_per_response,
            choices,
            strict_arguments,
        })
    }

    /// Constructs no tool-call requirement.
    #[must_use]
    pub fn none() -> Self {
        Self {
            min_definitions: ExecutionCount::ZERO,
            min_calls_per_response: ExecutionCount::ZERO,
            choices: ModelToolChoices::empty(),
            strict_arguments: false,
        }
    }

    /// Returns whether tool calling is required.
    #[must_use]
    pub const fn requires_tool_calling(&self) -> bool {
        self.min_definitions.get() != 0
    }

    /// Returns the minimum number of accepted tool definitions.
    #[must_use]
    pub const fn min_definitions(&self) -> ExecutionCount {
        self.min_definitions
    }

    /// Returns the minimum calls allowed in one model response.
    #[must_use]
    pub const fn min_calls_per_response(&self) -> ExecutionCount {
        self.min_calls_per_response
    }

    /// Returns required request tool-selection modes.
    #[must_use]
    pub const fn choices(&self) -> &ModelToolChoices {
        &self.choices
    }

    /// Returns whether strict tool arguments are required.
    #[must_use]
    pub const fn requires_strict_arguments(&self) -> bool {
        self.strict_arguments
    }
}

impl Default for ModelToolRequirements {
    fn default() -> Self {
        Self::none()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelToolRequirementsWire {
    min_definitions: ExecutionCount,
    min_calls_per_response: ExecutionCount,
    choices: ModelToolChoices,
    strict_arguments: bool,
}

impl<'de> Deserialize<'de> for ModelToolRequirements {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelToolRequirementsWire::deserialize(deserializer)?;
        Self::new(
            wire.min_definitions,
            wire.min_calls_per_response,
            wire.choices,
            wire.strict_arguments,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid model tool requirements.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelToolRequirementsError {
    /// Only one capacity dimension was active.
    #[error("model tool requirements need both definition and call minima to be zero or positive")]
    CapacityMismatch,
    /// An inactive requirement carried active controls.
    #[error("inactive model tool requirements cannot require choices or strict arguments")]
    InactiveHasRequirements,
}

/// Provider-neutral requirements derived from one concrete model request.
///
/// Positive token requirements fail closed when a provider limit is unknown.
/// Actual media formats, schema compatibility, request bytes, and policy are
/// validated at their dedicated adapter and runtime boundaries.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRequirements {
    input_modalities: ModelModalities,
    output_modalities: ModelModalities,
    streaming: bool,
    tools: ModelToolRequirements,
    structured_output: ModelStructuredOutputLevel,
    reasoning_summaries: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_context_tokens: Option<TokenCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_input_tokens: Option<TokenCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_output_tokens: Option<TokenCount>,
}

impl ModelRequirements {
    /// Constructs validated request requirements.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRequirementsError`] when a present token minimum is zero;
    /// absence is the only representation of no minimum.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_modalities: ModelModalities,
        output_modalities: ModelModalities,
        streaming: bool,
        tools: ModelToolRequirements,
        structured_output: ModelStructuredOutputLevel,
        reasoning_summaries: bool,
        min_context_tokens: Option<TokenCount>,
        min_input_tokens: Option<TokenCount>,
        min_output_tokens: Option<TokenCount>,
    ) -> Result<Self, ModelRequirementsError> {
        if matches!(min_context_tokens, Some(value) if value.get() == 0) {
            return Err(ModelRequirementsError::ZeroContextTokens);
        }
        if matches!(min_input_tokens, Some(value) if value.get() == 0) {
            return Err(ModelRequirementsError::ZeroInputTokens);
        }
        if matches!(min_output_tokens, Some(value) if value.get() == 0) {
            return Err(ModelRequirementsError::ZeroOutputTokens);
        }
        Ok(Self {
            input_modalities,
            output_modalities,
            streaming,
            tools,
            structured_output,
            reasoning_summaries,
            min_context_tokens,
            min_input_tokens,
            min_output_tokens,
        })
    }

    /// Returns required input modalities.
    #[must_use]
    pub const fn input_modalities(&self) -> &ModelModalities {
        &self.input_modalities
    }

    /// Returns required output modalities.
    #[must_use]
    pub const fn output_modalities(&self) -> &ModelModalities {
        &self.output_modalities
    }

    /// Returns whether streaming is required.
    #[must_use]
    pub const fn requires_streaming(&self) -> bool {
        self.streaming
    }

    /// Returns tool-call requirements.
    #[must_use]
    pub const fn tools(&self) -> &ModelToolRequirements {
        &self.tools
    }

    /// Returns the minimum structured-output level.
    #[must_use]
    pub const fn structured_output(&self) -> ModelStructuredOutputLevel {
        self.structured_output
    }

    /// Returns whether a readable reasoning summary is required.
    #[must_use]
    pub const fn requires_reasoning_summaries(&self) -> bool {
        self.reasoning_summaries
    }

    /// Returns the minimum known total context capacity.
    #[must_use]
    pub const fn min_context_tokens(&self) -> Option<TokenCount> {
        self.min_context_tokens
    }

    /// Returns the minimum known input capacity.
    #[must_use]
    pub const fn min_input_tokens(&self) -> Option<TokenCount> {
        self.min_input_tokens
    }

    /// Returns the minimum known output capacity.
    #[must_use]
    pub const fn min_output_tokens(&self) -> Option<TokenCount> {
        self.min_output_tokens
    }
}

impl Default for ModelRequirements {
    fn default() -> Self {
        Self {
            input_modalities: ModelModalities::empty(),
            output_modalities: ModelModalities::empty(),
            streaming: false,
            tools: ModelToolRequirements::none(),
            structured_output: ModelStructuredOutputLevel::Unsupported,
            reasoning_summaries: false,
            min_context_tokens: None,
            min_input_tokens: None,
            min_output_tokens: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelRequirementsWire {
    input_modalities: ModelModalities,
    output_modalities: ModelModalities,
    streaming: bool,
    tools: ModelToolRequirements,
    structured_output: ModelStructuredOutputLevel,
    reasoning_summaries: bool,
    #[serde(default)]
    min_context_tokens: Option<TokenCount>,
    #[serde(default)]
    min_input_tokens: Option<TokenCount>,
    #[serde(default)]
    min_output_tokens: Option<TokenCount>,
}

impl<'de> Deserialize<'de> for ModelRequirements {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ModelRequirementsWire::deserialize(deserializer)?;
        Self::new(
            wire.input_modalities,
            wire.output_modalities,
            wire.streaming,
            wire.tools,
            wire.structured_output,
            wire.reasoning_summaries,
            wire.min_context_tokens,
            wire.min_input_tokens,
            wire.min_output_tokens,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid model requirements.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelRequirementsError {
    /// A present total context minimum was zero.
    #[error("model context-token requirement must be positive or absent")]
    ZeroContextTokens,
    /// A present input minimum was zero.
    #[error("model input-token requirement must be positive or absent")]
    ZeroInputTokens,
    /// A present output minimum was zero.
    #[error("model output-token requirement must be positive or absent")]
    ZeroOutputTokens,
}

/// One unmet model requirement with known available capacity where useful.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(tag = "requirement", rename_all = "snake_case", deny_unknown_fields)]
pub enum ModelCapabilityIssue {
    /// A required input modality is absent.
    InputModality {
        /// Missing modality.
        required: ModelModality,
    },
    /// A required output modality is absent.
    OutputModality {
        /// Missing modality.
        required: ModelModality,
    },
    /// Incremental response events are unsupported.
    Streaming {},
    /// Tool definitions are unsupported.
    ToolCalling {},
    /// Tool-definition capacity is insufficient.
    ToolDefinitions {
        /// Minimum definitions needed by the request.
        required: ExecutionCount,
        /// Maximum definitions accepted by the model binding.
        available: ExecutionCount,
    },
    /// Per-response tool-call capacity is insufficient.
    ToolCallsPerResponse {
        /// Minimum calls needed in one response.
        required: ExecutionCount,
        /// Maximum calls emitted in one response.
        available: ExecutionCount,
    },
    /// A requested tool-selection control is unsupported.
    ToolChoice {
        /// Missing control.
        required: ModelToolChoice,
    },
    /// Provider-constrained tool arguments are unsupported.
    StrictToolArguments {},
    /// Structured-output support is weaker than required.
    StructuredOutput {
        /// Minimum requested strength.
        required: ModelStructuredOutputLevel,
        /// Available strength.
        available: ModelStructuredOutputLevel,
    },
    /// A readable reasoning summary is unavailable.
    ReasoningSummary {},
    /// Known total context capacity is absent or insufficient.
    ContextTokens {
        /// Minimum required tokens.
        required: TokenCount,
        /// Known available tokens, absent when the provider did not publish one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        available: Option<TokenCount>,
    },
    /// Known input capacity is absent or insufficient.
    InputTokens {
        /// Minimum required tokens.
        required: TokenCount,
        /// Known available tokens, absent when the provider did not publish one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        available: Option<TokenCount>,
    },
    /// Known output capacity is absent or insufficient.
    OutputTokens {
        /// Minimum required tokens.
        required: TokenCount,
        /// Known available tokens, absent when the provider did not publish one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        available: Option<TokenCount>,
    },
}

impl ModelCapabilityIssue {
    fn is_coherent(&self) -> bool {
        match self {
            Self::ToolDefinitions {
                required,
                available,
            }
            | Self::ToolCallsPerResponse {
                required,
                available,
            } => required.get() != 0 && required > available,
            Self::StructuredOutput {
                required,
                available,
            } => required > available,
            Self::ContextTokens {
                required,
                available,
            }
            | Self::InputTokens {
                required,
                available,
            }
            | Self::OutputTokens {
                required,
                available,
            } => required.get() != 0 && available.is_none_or(|available| required > &available),
            Self::InputModality { .. }
            | Self::OutputModality { .. }
            | Self::Streaming {}
            | Self::ToolCalling {}
            | Self::ToolChoice { .. }
            | Self::StrictToolArguments {}
            | Self::ReasoningSummary {} => true,
        }
    }

    fn has_same_dimension(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::InputModality { required: left }, Self::InputModality { required: right })
            | (Self::OutputModality { required: left }, Self::OutputModality { required: right }) => {
                left == right
            }
            (Self::ToolChoice { required: left }, Self::ToolChoice { required: right }) => {
                left == right
            }
            (Self::Streaming {}, Self::Streaming {})
            | (Self::ToolCalling {}, Self::ToolCalling {})
            | (Self::ToolDefinitions { .. }, Self::ToolDefinitions { .. })
            | (Self::ToolCallsPerResponse { .. }, Self::ToolCallsPerResponse { .. })
            | (Self::StrictToolArguments {}, Self::StrictToolArguments {})
            | (Self::StructuredOutput { .. }, Self::StructuredOutput { .. })
            | (Self::ReasoningSummary {}, Self::ReasoningSummary {})
            | (Self::ContextTokens { .. }, Self::ContextTokens { .. })
            | (Self::InputTokens { .. }, Self::InputTokens { .. })
            | (Self::OutputTokens { .. }, Self::OutputTokens { .. }) => true,
            _ => false,
        }
    }
}

/// Deterministic, non-empty set of every unmet model requirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelCapabilityMismatch(BTreeSet<ModelCapabilityIssue>);

impl ModelCapabilityMismatch {
    /// Maximum distinct issue count accepted from diagnostic wire data.
    pub const MAX_ISSUES: usize = 32;

    /// Constructs a deterministic non-empty mismatch.
    ///
    /// # Errors
    ///
    /// Returns [`ModelCapabilityMismatchError`] for empty, duplicate, or
    /// resource-excessive issue input.
    pub fn try_new<I>(issues: I) -> Result<Self, ModelCapabilityMismatchError>
    where
        I: IntoIterator<Item = ModelCapabilityIssue>,
    {
        let mut values: BTreeSet<ModelCapabilityIssue> = BTreeSet::new();
        for issue in issues {
            if !issue.is_coherent() {
                return Err(ModelCapabilityMismatchError::IncoherentIssue { issue });
            }
            if values.contains(&issue) {
                return Err(ModelCapabilityMismatchError::Duplicate { issue });
            }
            if values
                .iter()
                .any(|existing| existing.has_same_dimension(&issue))
            {
                return Err(ModelCapabilityMismatchError::DuplicateDimension { issue });
            }
            if values.len() == Self::MAX_ISSUES {
                return Err(ModelCapabilityMismatchError::TooMany {
                    max: Self::MAX_ISSUES,
                    observed: Self::MAX_ISSUES + 1,
                });
            }
            values.insert(issue);
        }
        if values.is_empty() {
            return Err(ModelCapabilityMismatchError::Empty);
        }
        Ok(Self(values))
    }

    /// Returns the number of unmet dimensions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no issue is present; validated values always return `false`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates in deterministic issue order.
    pub fn iter(&self) -> btree_set::Iter<'_, ModelCapabilityIssue> {
        self.0.iter()
    }

    /// Returns whether an exact issue is present.
    #[must_use]
    pub fn contains(&self, issue: &ModelCapabilityIssue) -> bool {
        self.0.contains(issue)
    }
}

impl<'a> IntoIterator for &'a ModelCapabilityMismatch {
    type Item = &'a ModelCapabilityIssue;
    type IntoIter = btree_set::Iter<'a, ModelCapabilityIssue>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl fmt::Display for ModelCapabilityMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "model binding does not satisfy {} requirement(s)",
            self.len()
        )
    }
}

impl std::error::Error for ModelCapabilityMismatch {}

impl Serialize for ModelCapabilityMismatch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for ModelCapabilityMismatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ModelCapabilityMismatchVisitor)
    }
}

struct ModelCapabilityMismatchVisitor;

impl<'de> de::Visitor<'de> for ModelCapabilityMismatchVisitor {
    type Value = ModelCapabilityMismatch;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a non-empty array containing at most {} unique model capability issues",
            ModelCapabilityMismatch::MAX_ISSUES
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut issues = Vec::new();
        while let Some(issue) = sequence.next_element::<ModelCapabilityIssue>()? {
            if issues.len() == ModelCapabilityMismatch::MAX_ISSUES {
                return Err(de::Error::custom(ModelCapabilityMismatchError::TooMany {
                    max: ModelCapabilityMismatch::MAX_ISSUES,
                    observed: ModelCapabilityMismatch::MAX_ISSUES + 1,
                }));
            }
            issues.push(issue);
        }
        ModelCapabilityMismatch::try_new(issues).map_err(de::Error::custom)
    }
}

impl JsonSchema for ModelCapabilityMismatch {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ModelCapabilityMismatch".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ModelCapabilityMismatch").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ModelCapabilityIssue>(),
            "minItems": 1,
            "maxItems": 32,
            "uniqueItems": true
        })
    }
}

/// Invalid mismatch diagnostic set.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelCapabilityMismatchError {
    /// A successful negotiation was incorrectly represented as an error.
    #[error("model capability mismatch must contain at least one issue")]
    Empty,
    /// One diagnostic issue appeared more than once.
    #[error("model capability mismatch contains duplicate issue {issue:?}")]
    Duplicate {
        /// Repeated diagnostic.
        issue: ModelCapabilityIssue,
    },
    /// Two different diagnostics described the same requirement dimension.
    #[error("model capability mismatch repeats requirement dimension in {issue:?}")]
    DuplicateDimension {
        /// Later conflicting diagnostic.
        issue: ModelCapabilityIssue,
    },
    /// A diagnostic claimed a mismatch despite sufficient or empty capacity.
    #[error("model capability mismatch contains incoherent issue {issue:?}")]
    IncoherentIssue {
        /// Illogical diagnostic.
        issue: ModelCapabilityIssue,
    },
    /// Diagnostic input exceeded the hard ceiling.
    #[error("model capability mismatch contains at least {observed} issues; maximum is {max}")]
    TooMany {
        /// Maximum accepted issue count.
        max: usize,
        /// Minimum count observed before validation stopped.
        observed: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    use crate::{
        CapabilityDescription, CapabilityIdentity, CapabilityLifecycle, CapabilityName,
        CapabilityReference, Digest, Extensions, IssuerId, PrincipalIdentity, SchemaId, ScopeSet,
        SubjectId, Version,
    };

    fn schema(name: &str) -> SchemaReference {
        SchemaReference::new(
            format!("https://schemas.example.com/model-profiles/{name}/1.0.0")
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(name),
        )
    }

    fn modalities(values: impl IntoIterator<Item = ModelModality>) -> ModelModalities {
        ModelModalities::try_new(values).unwrap()
    }

    fn choices(values: impl IntoIterator<Item = ModelToolChoice>) -> ModelToolChoices {
        ModelToolChoices::try_new(values).unwrap()
    }

    fn tool_capabilities() -> ModelToolCapabilities {
        ModelToolCapabilities::new(
            Some(schema("tool-input")),
            ExecutionCount::new(64),
            ExecutionCount::new(8),
            choices([
                ModelToolChoice::Specific,
                ModelToolChoice::Auto,
                ModelToolChoice::Required,
                ModelToolChoice::None,
            ]),
            true,
        )
        .unwrap()
    }

    fn capabilities() -> ModelCapabilities {
        ModelCapabilities::new(
            modalities([
                ModelModality::Document,
                ModelModality::Text,
                ModelModality::Image,
            ]),
            modalities([ModelModality::Text]),
            true,
            tool_capabilities(),
            ModelStructuredOutputCapabilities::json_schema(schema("structured-output")),
            true,
            ModelTokenLimits::new(
                Some(TokenCount::new(128_000)),
                Some(TokenCount::new(120_000)),
                Some(TokenCount::new(16_384)),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn metadata(kind: CapabilityKind, description: &str) -> CapabilityMetadata {
        CapabilityMetadata::new(
            CapabilityIdentity::new(
                PrincipalIdentity::new(
                    "https://issuer.example.com/tenant"
                        .parse::<IssuerId>()
                        .unwrap(),
                    "model-registry".parse::<SubjectId>().unwrap(),
                ),
                CapabilityReference::new(
                    "models.primary".parse::<CapabilityName>().unwrap(),
                    Version::new(1, 0, 0),
                ),
            ),
            kind,
            None,
            CapabilityDescription::new(description).unwrap(),
            CapabilityLifecycle::active(),
            ScopeSet::empty(),
            Extensions::default(),
        )
        .unwrap()
    }

    fn descriptor(description: &str) -> ModelDescriptor {
        ModelDescriptor::new(metadata(CapabilityKind::Model, description), capabilities()).unwrap()
    }

    #[test]
    fn model_enums_and_sets_have_closed_deterministic_wire_forms() {
        for (value, expected) in [
            (ModelModality::Text, "text"),
            (ModelModality::Image, "image"),
            (ModelModality::Audio, "audio"),
            (ModelModality::Video, "video"),
            (ModelModality::Document, "document"),
        ] {
            assert_eq!(to_value(value).unwrap(), Value::from(expected));
            assert_eq!(from_value::<ModelModality>(json!(expected)).unwrap(), value);
        }
        for (value, expected) in [
            (ModelToolChoice::Auto, "auto"),
            (ModelToolChoice::None, "none"),
            (ModelToolChoice::Required, "required"),
            (ModelToolChoice::Specific, "specific"),
        ] {
            assert_eq!(to_value(value).unwrap(), Value::from(expected));
            assert_eq!(
                from_value::<ModelToolChoice>(json!(expected)).unwrap(),
                value
            );
        }
        for (value, expected) in [
            (ModelStructuredOutputLevel::Unsupported, "unsupported"),
            (ModelStructuredOutputLevel::Json, "json"),
            (ModelStructuredOutputLevel::JsonSchema, "json_schema"),
        ] {
            assert_eq!(to_value(value).unwrap(), Value::from(expected));
            assert_eq!(
                from_value::<ModelStructuredOutputLevel>(json!(expected)).unwrap(),
                value
            );
        }

        let modality_set = modalities([
            ModelModality::Document,
            ModelModality::Text,
            ModelModality::Audio,
        ]);
        assert_eq!(
            to_value(&modality_set).unwrap(),
            json!(["text", "audio", "document"])
        );
        assert!(modality_set.contains(ModelModality::Audio));
        assert_eq!(modality_set.len(), 3);
        assert!(ModelModalities::try_new([ModelModality::Text, ModelModality::Text]).is_err());
        assert!(from_value::<ModelModalities>(json!(["text", "text"])).is_err());

        let choice_set = choices([
            ModelToolChoice::Specific,
            ModelToolChoice::Auto,
            ModelToolChoice::Required,
        ]);
        assert_eq!(
            to_value(&choice_set).unwrap(),
            json!(["auto", "required", "specific"])
        );
        assert!(choice_set.contains(ModelToolChoice::Specific));
        assert!(ModelToolChoices::try_new([ModelToolChoice::Auto, ModelToolChoice::Auto]).is_err());
        assert!(from_value::<ModelToolChoices>(json!(["auto", "auto"])).is_err());

        assert!(from_value::<ModelModality>(json!("archive")).is_err());
        assert!(from_value::<ModelToolChoice>(json!("forced")).is_err());
        assert!(from_value::<ModelStructuredOutputLevel>(Value::Null).is_err());
    }

    #[test]
    fn token_limits_preserve_unknown_and_reject_false_capacity() {
        let limits = ModelTokenLimits::new(
            Some(TokenCount::new(128_000)),
            Some(TokenCount::new(120_000)),
            Some(TokenCount::new(16_384)),
        )
        .unwrap();
        assert_eq!(limits.max_context_tokens(), Some(TokenCount::new(128_000)));
        assert_eq!(limits.max_input_tokens(), Some(TokenCount::new(120_000)));
        assert_eq!(limits.max_output_tokens(), Some(TokenCount::new(16_384)));
        assert_eq!(
            to_value(&limits).unwrap(),
            json!({
                "max_context_tokens": "128000",
                "max_input_tokens": "120000",
                "max_output_tokens": "16384"
            })
        );
        assert_eq!(
            from_value::<ModelTokenLimits>(to_value(&limits).unwrap()).unwrap(),
            limits
        );

        let unknown = ModelTokenLimits::unknown();
        assert_eq!(to_value(&unknown).unwrap(), json!({}));
        assert_eq!(from_value::<ModelTokenLimits>(json!({})).unwrap(), unknown);

        assert_eq!(
            ModelTokenLimits::new(Some(TokenCount::ZERO), None, None),
            Err(ModelTokenLimitsError::ZeroContext)
        );
        assert_eq!(
            ModelTokenLimits::new(None, Some(TokenCount::ZERO), None),
            Err(ModelTokenLimitsError::ZeroInput)
        );
        assert_eq!(
            ModelTokenLimits::new(None, None, Some(TokenCount::ZERO)),
            Err(ModelTokenLimitsError::ZeroOutput)
        );
        assert_eq!(
            ModelTokenLimits::new(Some(TokenCount::new(10)), Some(TokenCount::new(11)), None,),
            Err(ModelTokenLimitsError::InputExceedsContext {
                input: TokenCount::new(11),
                context: TokenCount::new(10),
            })
        );
        assert_eq!(
            ModelTokenLimits::new(Some(TokenCount::new(10)), None, Some(TokenCount::new(11)),),
            Err(ModelTokenLimitsError::OutputExceedsContext {
                output: TokenCount::new(11),
                context: TokenCount::new(10),
            })
        );
        for invalid in [
            json!({"max_context_tokens": "0"}),
            json!({"max_input_tokens": "0"}),
            json!({"max_output_tokens": "0"}),
            json!({"max_context_tokens": "10", "max_input_tokens": "11"}),
            json!({"unlimited": true}),
            Value::Null,
        ] {
            assert!(
                from_value::<ModelTokenLimits>(invalid.clone()).is_err(),
                "accepted token limits {invalid}"
            );
        }
    }

    #[test]
    fn tool_capabilities_bind_profile_capacity_choice_and_strictness() {
        let tools = tool_capabilities();
        assert!(tools.supports_tool_calling());
        assert!(tools.supports_parallel_calls());
        assert!(tools.supports_strict_arguments());
        assert_eq!(tools.max_definitions(), ExecutionCount::new(64));
        assert_eq!(tools.max_calls_per_response(), ExecutionCount::new(8));
        assert_eq!(
            tools.schema_profile().unwrap().id().as_str(),
            "https://schemas.example.com/model-profiles/tool-input/1.0.0"
        );
        assert!(tools.choices().contains(ModelToolChoice::Auto));
        assert_eq!(
            from_value::<ModelToolCapabilities>(to_value(&tools).unwrap()).unwrap(),
            tools
        );

        let unsupported = ModelToolCapabilities::unsupported();
        assert!(!unsupported.supports_tool_calling());
        assert!(!unsupported.supports_parallel_calls());
        assert!(!unsupported.supports_strict_arguments());
        assert_eq!(
            to_value(&unsupported).unwrap(),
            json!({
                "max_definitions": "0",
                "max_calls_per_response": "0",
                "choices": [],
                "strict_arguments": false
            })
        );

        assert_eq!(
            ModelToolCapabilities::new(
                None,
                ExecutionCount::new(1),
                ExecutionCount::new(1),
                choices([ModelToolChoice::Auto]),
                false,
            ),
            Err(ModelToolCapabilitiesError::UnsupportedHasCapabilities)
        );
        assert_eq!(
            ModelToolCapabilities::new(
                Some(schema("tool")),
                ExecutionCount::ZERO,
                ExecutionCount::new(1),
                choices([ModelToolChoice::Auto]),
                false,
            ),
            Err(ModelToolCapabilitiesError::ZeroDefinitions)
        );
        assert_eq!(
            ModelToolCapabilities::new(
                Some(schema("tool")),
                ExecutionCount::new(1),
                ExecutionCount::ZERO,
                choices([ModelToolChoice::Auto]),
                false,
            ),
            Err(ModelToolCapabilitiesError::ZeroCallsPerResponse)
        );
        assert_eq!(
            ModelToolCapabilities::new(
                Some(schema("tool")),
                ExecutionCount::new(1),
                ExecutionCount::new(1),
                choices([ModelToolChoice::Required]),
                false,
            ),
            Err(ModelToolCapabilitiesError::MissingAutoChoice)
        );

        let mut unknown = to_value(&tools).unwrap();
        unknown["provider_parallel"] = json!(true);
        assert!(from_value::<ModelToolCapabilities>(unknown).is_err());
    }

    #[test]
    fn structured_output_profile_exists_exactly_for_schema_support() {
        let unsupported = ModelStructuredOutputCapabilities::unsupported();
        let json_only = ModelStructuredOutputCapabilities::json();
        let schema_bound =
            ModelStructuredOutputCapabilities::json_schema(schema("structured-output"));

        assert_eq!(
            to_value(&unsupported).unwrap(),
            json!({"level": "unsupported"})
        );
        assert_eq!(to_value(&json_only).unwrap(), json!({"level": "json"}));
        assert_eq!(schema_bound.level(), ModelStructuredOutputLevel::JsonSchema);
        assert!(schema_bound.schema_profile().is_some());
        assert_eq!(
            from_value::<ModelStructuredOutputCapabilities>(to_value(&schema_bound).unwrap())
                .unwrap(),
            schema_bound
        );

        assert_eq!(
            ModelStructuredOutputCapabilities::new(ModelStructuredOutputLevel::JsonSchema, None,),
            Err(ModelStructuredOutputCapabilitiesError::MissingSchemaProfile)
        );
        assert_eq!(
            ModelStructuredOutputCapabilities::new(
                ModelStructuredOutputLevel::Json,
                Some(schema("unexpected")),
            ),
            Err(ModelStructuredOutputCapabilitiesError::UnexpectedSchemaProfile)
        );
        assert!(
            from_value::<ModelStructuredOutputCapabilities>(json!({
                "level": "unsupported",
                "schema_profile": to_value(schema("unexpected")).unwrap()
            }))
            .is_err()
        );
    }

    #[test]
    fn model_capabilities_revalidate_text_feature_invariants() {
        let capabilities = capabilities();
        assert_eq!(
            capabilities.input_modalities(),
            &modalities([
                ModelModality::Text,
                ModelModality::Image,
                ModelModality::Document,
            ])
        );
        assert_eq!(
            capabilities.output_modalities(),
            &modalities([ModelModality::Text])
        );
        assert!(capabilities.supports_streaming());
        assert!(capabilities.tools().supports_tool_calling());
        assert!(capabilities.supports_reasoning_summaries());
        assert_eq!(
            from_value::<ModelCapabilities>(to_value(&capabilities).unwrap()).unwrap(),
            capabilities
        );

        assert_eq!(
            ModelCapabilities::new(
                ModelModalities::empty(),
                modalities([ModelModality::Text]),
                false,
                ModelToolCapabilities::unsupported(),
                ModelStructuredOutputCapabilities::unsupported(),
                false,
                ModelTokenLimits::unknown(),
            ),
            Err(ModelCapabilitiesError::EmptyInputModalities)
        );
        assert_eq!(
            ModelCapabilities::new(
                modalities([ModelModality::Text]),
                ModelModalities::empty(),
                false,
                ModelToolCapabilities::unsupported(),
                ModelStructuredOutputCapabilities::unsupported(),
                false,
                ModelTokenLimits::unknown(),
            ),
            Err(ModelCapabilitiesError::EmptyOutputModalities)
        );
        assert_eq!(
            ModelCapabilities::new(
                modalities([ModelModality::Image]),
                modalities([ModelModality::Text]),
                false,
                tool_capabilities(),
                ModelStructuredOutputCapabilities::unsupported(),
                false,
                ModelTokenLimits::unknown(),
            ),
            Err(ModelCapabilitiesError::ToolCallingRequiresTextInput)
        );
        assert_eq!(
            ModelCapabilities::new(
                modalities([ModelModality::Text]),
                modalities([ModelModality::Image]),
                false,
                tool_capabilities(),
                ModelStructuredOutputCapabilities::unsupported(),
                false,
                ModelTokenLimits::unknown(),
            ),
            Err(ModelCapabilitiesError::ToolCallingRequiresTextOutput)
        );
        assert_eq!(
            ModelCapabilities::new(
                modalities([ModelModality::Image]),
                modalities([ModelModality::Image]),
                false,
                ModelToolCapabilities::unsupported(),
                ModelStructuredOutputCapabilities::json(),
                false,
                ModelTokenLimits::unknown(),
            ),
            Err(ModelCapabilitiesError::StructuredOutputRequiresTextOutput)
        );
        assert_eq!(
            ModelCapabilities::new(
                modalities([ModelModality::Audio]),
                modalities([ModelModality::Audio]),
                true,
                ModelToolCapabilities::unsupported(),
                ModelStructuredOutputCapabilities::unsupported(),
                true,
                ModelTokenLimits::unknown(),
            ),
            Err(ModelCapabilitiesError::ReasoningSummaryRequiresTextOutput)
        );

        let mut invalid = to_value(&capabilities).unwrap();
        invalid["output_modalities"] = json!(["image"]);
        assert!(from_value::<ModelCapabilities>(invalid).is_err());
    }

    #[test]
    fn descriptors_bind_model_metadata_to_one_capability_snapshot() {
        let descriptor = descriptor("Private model registry description.");
        assert_eq!(descriptor.metadata().kind(), CapabilityKind::Model);
        assert_eq!(
            descriptor
                .metadata()
                .identity()
                .capability()
                .name()
                .as_str(),
            "models.primary"
        );
        assert!(descriptor.capabilities().supports_streaming());

        let encoded = to_value(&descriptor).unwrap();
        assert_eq!(
            from_value::<ModelDescriptor>(encoded.clone()).unwrap(),
            descriptor
        );
        assert!(!format!("{descriptor:?}").contains("Private model registry description."));

        assert_eq!(
            ModelDescriptor::new(
                metadata(CapabilityKind::Tool, "Wrong kind."),
                capabilities(),
            ),
            Err(ModelDescriptorError::WrongCapabilityKind {
                actual: CapabilityKind::Tool,
            })
        );

        let mut wrong_kind = encoded.clone();
        wrong_kind["metadata"]["kind"] = json!("tool");
        assert!(from_value::<ModelDescriptor>(wrong_kind).is_err());

        let mut unknown = encoded;
        unknown["provider_model"] = json!("mutable-alias");
        assert!(from_value::<ModelDescriptor>(unknown).is_err());
    }

    #[test]
    fn requirements_reject_implicit_or_incoherent_demands() {
        let required_tools = ModelToolRequirements::new(
            ExecutionCount::new(12),
            ExecutionCount::new(4),
            choices([ModelToolChoice::Auto, ModelToolChoice::Specific]),
            true,
        )
        .unwrap();
        assert!(required_tools.requires_tool_calling());
        assert_eq!(required_tools.min_definitions(), ExecutionCount::new(12));
        assert_eq!(
            required_tools.min_calls_per_response(),
            ExecutionCount::new(4)
        );
        assert!(required_tools.requires_strict_arguments());
        assert_eq!(
            from_value::<ModelToolRequirements>(to_value(&required_tools).unwrap()).unwrap(),
            required_tools
        );
        assert!(!ModelToolRequirements::none().requires_tool_calling());

        assert_eq!(
            ModelToolRequirements::new(
                ExecutionCount::new(1),
                ExecutionCount::ZERO,
                ModelToolChoices::empty(),
                false,
            ),
            Err(ModelToolRequirementsError::CapacityMismatch)
        );
        assert_eq!(
            ModelToolRequirements::new(
                ExecutionCount::ZERO,
                ExecutionCount::ZERO,
                choices([ModelToolChoice::Auto]),
                false,
            ),
            Err(ModelToolRequirementsError::InactiveHasRequirements)
        );

        let requirements = ModelRequirements::new(
            modalities([ModelModality::Text, ModelModality::Image]),
            modalities([ModelModality::Text]),
            true,
            required_tools,
            ModelStructuredOutputLevel::JsonSchema,
            true,
            Some(TokenCount::new(64_000)),
            Some(TokenCount::new(48_000)),
            Some(TokenCount::new(8_192)),
        )
        .unwrap();
        assert_eq!(
            from_value::<ModelRequirements>(to_value(&requirements).unwrap()).unwrap(),
            requirements
        );
        assert_eq!(
            ModelRequirements::default().structured_output(),
            ModelStructuredOutputLevel::Unsupported
        );

        for (context, input, output, expected) in [
            (
                Some(TokenCount::ZERO),
                None,
                None,
                ModelRequirementsError::ZeroContextTokens,
            ),
            (
                None,
                Some(TokenCount::ZERO),
                None,
                ModelRequirementsError::ZeroInputTokens,
            ),
            (
                None,
                None,
                Some(TokenCount::ZERO),
                ModelRequirementsError::ZeroOutputTokens,
            ),
        ] {
            assert_eq!(
                ModelRequirements::new(
                    ModelModalities::empty(),
                    ModelModalities::empty(),
                    false,
                    ModelToolRequirements::none(),
                    ModelStructuredOutputLevel::Unsupported,
                    false,
                    context,
                    input,
                    output,
                ),
                Err(expected)
            );
        }

        let mut unknown = to_value(&requirements).unwrap();
        unknown["fallback"] = json!(true);
        assert!(from_value::<ModelRequirements>(unknown).is_err());
    }

    #[test]
    fn negotiation_reports_every_unmet_dimension_and_unknown_limit() {
        let weak = ModelCapabilities::new(
            modalities([ModelModality::Text]),
            modalities([ModelModality::Text]),
            false,
            ModelToolCapabilities::unsupported(),
            ModelStructuredOutputCapabilities::unsupported(),
            false,
            ModelTokenLimits::unknown(),
        )
        .unwrap();
        let requirements = ModelRequirements::new(
            modalities([ModelModality::Text, ModelModality::Image]),
            modalities([ModelModality::Text, ModelModality::Audio]),
            true,
            ModelToolRequirements::new(
                ExecutionCount::new(5),
                ExecutionCount::new(3),
                choices([ModelToolChoice::Required, ModelToolChoice::Specific]),
                true,
            )
            .unwrap(),
            ModelStructuredOutputLevel::JsonSchema,
            true,
            Some(TokenCount::new(100)),
            Some(TokenCount::new(50)),
            Some(TokenCount::new(20)),
        )
        .unwrap();

        let mismatch = weak.satisfies(&requirements).unwrap_err();
        assert_eq!(mismatch.len(), 14);
        assert!(!mismatch.is_empty());
        for issue in [
            ModelCapabilityIssue::InputModality {
                required: ModelModality::Image,
            },
            ModelCapabilityIssue::OutputModality {
                required: ModelModality::Audio,
            },
            ModelCapabilityIssue::Streaming {},
            ModelCapabilityIssue::ToolCalling {},
            ModelCapabilityIssue::ToolDefinitions {
                required: ExecutionCount::new(5),
                available: ExecutionCount::ZERO,
            },
            ModelCapabilityIssue::ToolCallsPerResponse {
                required: ExecutionCount::new(3),
                available: ExecutionCount::ZERO,
            },
            ModelCapabilityIssue::ToolChoice {
                required: ModelToolChoice::Required,
            },
            ModelCapabilityIssue::ToolChoice {
                required: ModelToolChoice::Specific,
            },
            ModelCapabilityIssue::StrictToolArguments {},
            ModelCapabilityIssue::StructuredOutput {
                required: ModelStructuredOutputLevel::JsonSchema,
                available: ModelStructuredOutputLevel::Unsupported,
            },
            ModelCapabilityIssue::ReasoningSummary {},
            ModelCapabilityIssue::ContextTokens {
                required: TokenCount::new(100),
                available: None,
            },
            ModelCapabilityIssue::InputTokens {
                required: TokenCount::new(50),
                available: None,
            },
            ModelCapabilityIssue::OutputTokens {
                required: TokenCount::new(20),
                available: None,
            },
        ] {
            assert!(mismatch.contains(&issue), "missing issue {issue:?}");
        }
        assert_eq!(
            from_value::<ModelCapabilityMismatch>(to_value(&mismatch).unwrap()).unwrap(),
            mismatch
        );
    }

    #[test]
    fn mismatch_wire_rejects_empty_duplicate_and_incoherent_diagnostics() {
        assert_eq!(
            ModelCapabilityMismatch::try_new([]),
            Err(ModelCapabilityMismatchError::Empty)
        );
        let issue = ModelCapabilityIssue::Streaming {};
        assert_eq!(
            ModelCapabilityMismatch::try_new([issue.clone(), issue.clone()]),
            Err(ModelCapabilityMismatchError::Duplicate { issue })
        );
        let first_capacity = ModelCapabilityIssue::ToolDefinitions {
            required: ExecutionCount::new(5),
            available: ExecutionCount::new(2),
        };
        let second_capacity = ModelCapabilityIssue::ToolDefinitions {
            required: ExecutionCount::new(6),
            available: ExecutionCount::new(2),
        };
        assert_eq!(
            ModelCapabilityMismatch::try_new([first_capacity, second_capacity.clone(),]),
            Err(ModelCapabilityMismatchError::DuplicateDimension {
                issue: second_capacity,
            })
        );
        let incoherent = ModelCapabilityIssue::OutputTokens {
            required: TokenCount::new(10),
            available: Some(TokenCount::new(10)),
        };
        assert_eq!(
            ModelCapabilityMismatch::try_new([incoherent.clone()]),
            Err(ModelCapabilityMismatchError::IncoherentIssue { issue: incoherent })
        );
        assert!(from_value::<ModelCapabilityMismatch>(json!([])).is_err());
    }

    #[test]
    fn exact_or_weaker_requirements_pass_without_silent_downgrade() {
        let requirements = ModelRequirements::new(
            modalities([ModelModality::Text, ModelModality::Image]),
            modalities([ModelModality::Text]),
            true,
            ModelToolRequirements::new(
                ExecutionCount::new(32),
                ExecutionCount::new(4),
                choices([ModelToolChoice::Auto, ModelToolChoice::Specific]),
                true,
            )
            .unwrap(),
            ModelStructuredOutputLevel::JsonSchema,
            true,
            Some(TokenCount::new(128_000)),
            Some(TokenCount::new(100_000)),
            Some(TokenCount::new(16_384)),
        )
        .unwrap();
        assert!(capabilities().satisfies(&requirements).is_ok());

        let excessive_output = ModelRequirements::new(
            ModelModalities::empty(),
            ModelModalities::empty(),
            false,
            ModelToolRequirements::none(),
            ModelStructuredOutputLevel::Unsupported,
            false,
            None,
            None,
            Some(TokenCount::new(16_385)),
        )
        .unwrap();
        let mismatch = capabilities().satisfies(&excessive_output).unwrap_err();
        assert_eq!(mismatch.len(), 1);
        assert!(mismatch.contains(&ModelCapabilityIssue::OutputTokens {
            required: TokenCount::new(16_385),
            available: Some(TokenCount::new(16_384)),
        }));
    }

    #[test]
    fn model_contract_schemas_publish_closed_objects_and_set_bounds() {
        for schema in [
            to_value(schemars::schema_for!(ModelTokenLimits)).unwrap(),
            to_value(schemars::schema_for!(ModelToolCapabilities)).unwrap(),
            to_value(schemars::schema_for!(ModelStructuredOutputCapabilities)).unwrap(),
            to_value(schemars::schema_for!(ModelCapabilities)).unwrap(),
            to_value(schemars::schema_for!(ModelDescriptor)).unwrap(),
            to_value(schemars::schema_for!(ModelToolRequirements)).unwrap(),
            to_value(schemars::schema_for!(ModelRequirements)).unwrap(),
        ] {
            assert_eq!(schema["type"], "object");
            assert_eq!(schema["additionalProperties"], false);
        }

        let modalities = to_value(schemars::schema_for!(ModelModalities)).unwrap();
        assert_eq!(modalities["maxItems"], ModelModalities::MAX_LEN);
        assert_eq!(modalities["uniqueItems"], true);
        let choices = to_value(schemars::schema_for!(ModelToolChoices)).unwrap();
        assert_eq!(choices["maxItems"], ModelToolChoices::MAX_LEN);
        assert_eq!(choices["uniqueItems"], true);
        let mismatch = to_value(schemars::schema_for!(ModelCapabilityMismatch)).unwrap();
        assert_eq!(mismatch["minItems"], 1);
        assert_eq!(mismatch["maxItems"], ModelCapabilityMismatch::MAX_ISSUES);
        assert_eq!(mismatch["uniqueItems"], true);
    }

    proptest! {
        #[test]
        fn every_valid_token_limit_tuple_round_trips(
            context in 1_u64..=1_000_000_u64,
            input_seed in any::<u64>(),
            output_seed in any::<u64>(),
        ) {
            let input = 1 + input_seed % context;
            let output = 1 + output_seed % context;
            let limits = ModelTokenLimits::new(
                Some(TokenCount::new(context)),
                Some(TokenCount::new(input)),
                Some(TokenCount::new(output)),
            ).unwrap();
            let encoded = serde_json::to_vec(&limits).unwrap();
            let decoded = serde_json::from_slice::<ModelTokenLimits>(&encoded).unwrap();
            prop_assert_eq!(decoded, limits);
        }

        #[test]
        fn every_requirement_within_known_token_capacity_is_satisfied(
            context in 1_u64..=1_000_000_u64,
            input_seed in any::<u64>(),
            output_seed in any::<u64>(),
            required_context_seed in any::<u64>(),
            required_input_seed in any::<u64>(),
            required_output_seed in any::<u64>(),
        ) {
            let input = 1 + input_seed % context;
            let output = 1 + output_seed % context;
            let capabilities = ModelCapabilities::new(
                modalities([ModelModality::Text]),
                modalities([ModelModality::Text]),
                false,
                ModelToolCapabilities::unsupported(),
                ModelStructuredOutputCapabilities::unsupported(),
                false,
                ModelTokenLimits::new(
                    Some(TokenCount::new(context)),
                    Some(TokenCount::new(input)),
                    Some(TokenCount::new(output)),
                ).unwrap(),
            ).unwrap();
            let requirements = ModelRequirements::new(
                ModelModalities::empty(),
                ModelModalities::empty(),
                false,
                ModelToolRequirements::none(),
                ModelStructuredOutputLevel::Unsupported,
                false,
                Some(TokenCount::new(1 + required_context_seed % context)),
                Some(TokenCount::new(1 + required_input_seed % input)),
                Some(TokenCount::new(1 + required_output_seed % output)),
            ).unwrap();
            prop_assert!(capabilities.satisfies(&requirements).is_ok());
        }
    }
}
