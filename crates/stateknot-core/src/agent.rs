// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Immutable, runtime-neutral agent definition contracts.

use std::{
    collections::{BTreeMap, btree_map},
    fmt, slice,
};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self},
};
use thiserror::Error;

use crate::{
    ArtifactModality, BudgetLimits, ByteCount, CapabilityIdentity, CapabilityKind,
    CapabilityLifecycleState, CapabilityMetadata, CapabilityName, ExecutionCount, Instruction,
    InstructionContent, InstructionIdentity, ModelDescriptor, ModelRequest,
    ModelStructuredOutputLevel, SchemaReference, ToolDescriptor,
};

const MEBIBYTE: u64 = 1024 * 1024;

/// Ordered, non-empty trusted instructions pinned into one agent definition.
///
/// Serialized instruction claims are not authority. A trusted registry must
/// authenticate every owner and resolve artifact bytes before making an agent
/// selectable. The collection itself prevents identity ambiguity and unbounded
/// prompt material.
#[derive(Clone, Eq, PartialEq)]
pub struct AgentInstructions {
    values: Box<[Instruction]>,
    content_bytes: ByteCount,
}

impl AgentInstructions {
    /// Maximum number of instructions in one agent definition.
    pub const MAX_LEN: usize = 32;

    /// Maximum aggregate resolved instruction content.
    pub const MAX_CONTENT_BYTES: ByteCount = ByteCount::new(8 * MEBIBYTE);

    /// Validates and constructs an ordered non-empty instruction collection.
    ///
    /// # Errors
    ///
    /// Returns [`AgentInstructionsError`] for an empty, duplicate, unsupported,
    /// oversized, or arithmetically unrepresentable collection.
    pub fn try_new<I>(values: I) -> Result<Self, AgentInstructionsError>
    where
        I: IntoIterator<Item = Instruction>,
    {
        let mut normalized = Vec::new();
        let mut content_bytes = ByteCount::ZERO;
        for value in values {
            push_instruction(&mut normalized, &mut content_bytes, value)?;
        }
        if normalized.is_empty() {
            return Err(AgentInstructionsError::Empty);
        }
        Ok(Self {
            values: normalized.into_boxed_slice(),
            content_bytes,
        })
    }

    /// Returns the ordered instruction slice.
    #[must_use]
    pub const fn as_slice(&self) -> &[Instruction] {
        &self.values
    }

    /// Iterates in declared precedence order.
    pub fn iter(&self) -> slice::Iter<'_, Instruction> {
        self.values.iter()
    }

    /// Returns the number of instructions.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether this collection is empty.
    ///
    /// Valid constructed values always return `false`.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns aggregate resolved text or artifact bytes.
    #[must_use]
    pub const fn content_bytes(&self) -> ByteCount {
        self.content_bytes
    }
}

impl<'a> IntoIterator for &'a AgentInstructions {
    type Item = &'a Instruction;
    type IntoIter = slice::Iter<'a, Instruction>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<Vec<Instruction>> for AgentInstructions {
    type Error = AgentInstructionsError;

    fn try_from(value: Vec<Instruction>) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl fmt::Debug for AgentInstructions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentInstructions")
            .field("count", &self.len())
            .field("content_bytes", &self.content_bytes)
            .finish_non_exhaustive()
    }
}

impl Serialize for AgentInstructions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AgentInstructions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(AgentInstructionsVisitor)
    }
}

struct AgentInstructionsVisitor;

impl<'de> de::Visitor<'de> for AgentInstructionsVisitor {
    type Value = AgentInstructions;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing 1 to {} unique trusted instructions",
            AgentInstructions::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(
            sequence
                .size_hint()
                .unwrap_or_default()
                .min(AgentInstructions::MAX_LEN),
        );
        let mut content_bytes = ByteCount::ZERO;
        while let Some(value) = sequence.next_element::<Instruction>()? {
            push_instruction(&mut values, &mut content_bytes, value).map_err(de::Error::custom)?;
        }
        if values.is_empty() {
            return Err(de::Error::custom(AgentInstructionsError::Empty));
        }
        Ok(AgentInstructions {
            values: values.into_boxed_slice(),
            content_bytes,
        })
    }
}

impl JsonSchema for AgentInstructions {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "AgentInstructions".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::AgentInstructions").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<Instruction>(),
            "minItems": 1,
            "maxItems": 32,
            "uniqueItems": true,
            "description": "Ordered application-controlled instructions. Runtime additionally rejects duplicate identities, non-text instruction artifacts, and more than 8388608 resolved content bytes."
        })
    }
}

fn push_instruction(
    values: &mut Vec<Instruction>,
    content_bytes: &mut ByteCount,
    value: Instruction,
) -> Result<(), AgentInstructionsError> {
    if values.len() == AgentInstructions::MAX_LEN {
        return Err(AgentInstructionsError::TooMany {
            max: AgentInstructions::MAX_LEN,
            observed: AgentInstructions::MAX_LEN + 1,
        });
    }
    if values
        .iter()
        .any(|existing| existing.identity() == value.identity())
    {
        return Err(AgentInstructionsError::Duplicate {
            identity: value.identity().clone(),
        });
    }

    let additional = match value.content() {
        InstructionContent::Text(text) => ByteCount::new(text.text().len() as u64),
        InstructionContent::Artifact(artifact) => {
            let modality = artifact.representation().modality();
            if modality != ArtifactModality::Text {
                return Err(AgentInstructionsError::UnsupportedArtifact {
                    index: values.len(),
                    modality,
                });
            }
            artifact.representation().byte_length()
        }
    };
    let actual = content_bytes
        .checked_add(additional)
        .ok_or(AgentInstructionsError::ContentBytesOverflow)?;
    if actual > AgentInstructions::MAX_CONTENT_BYTES {
        return Err(AgentInstructionsError::ContentTooLarge {
            maximum: AgentInstructions::MAX_CONTENT_BYTES,
            actual,
        });
    }
    *content_bytes = actual;
    values.push(value);
    Ok(())
}

/// Invalid agent instruction collection.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentInstructionsError {
    /// An agent had no trusted instruction.
    #[error("agent instructions must not be empty")]
    Empty,
    /// The hard instruction-count ceiling was exceeded.
    #[error("agent instructions contain at least {observed} values; maximum is {max}")]
    TooMany {
        /// Maximum accepted instruction count.
        max: usize,
        /// Minimum count observed before validation stopped.
        observed: usize,
    },
    /// One versioned instruction identity appeared more than once.
    #[error("agent instructions contain duplicate identity {identity:?}")]
    Duplicate {
        /// Repeated instruction identity.
        identity: InstructionIdentity,
    },
    /// An instruction artifact was not text material.
    #[error("agent instruction {index} uses unsupported artifact modality {modality:?}")]
    UnsupportedArtifact {
        /// Zero-based instruction position.
        index: usize,
        /// Rejected artifact modality.
        modality: ArtifactModality,
    },
    /// Aggregate content-byte accounting overflowed.
    #[error("agent instruction content byte count overflowed")]
    ContentBytesOverflow,
    /// Aggregate resolved content exceeded the hard ceiling.
    #[error("agent instruction content is {actual}; maximum is {maximum}")]
    ContentTooLarge {
        /// Immutable aggregate limit.
        maximum: ByteCount,
        /// Rejected aggregate size.
        actual: ByteCount,
    },
}

/// Canonically ordered, registry-resolved tools exposed by one agent.
///
/// Model tool names are registry-local. This collection rejects collisions
/// across owners and versions because a model request cannot disambiguate them.
#[derive(Clone, Eq, PartialEq)]
pub struct AgentTools(BTreeMap<CapabilityName, ToolDescriptor>);

impl AgentTools {
    /// Maximum number of ordinary tools exposed by one agent.
    pub const MAX_LEN: usize = 128;

    /// Constructs an empty tool collection.
    #[must_use]
    pub const fn empty() -> Self {
        Self(BTreeMap::new())
    }

    /// Validates and constructs a canonical tool collection.
    ///
    /// # Errors
    ///
    /// Returns [`AgentToolsError`] for a duplicate model-visible name, retired
    /// dependency, or count above the hard ceiling.
    pub fn try_new<I>(values: I) -> Result<Self, AgentToolsError>
    where
        I: IntoIterator<Item = ToolDescriptor>,
    {
        let mut normalized = BTreeMap::new();
        for value in values {
            insert_tool(&mut normalized, value)?;
        }
        Ok(Self(normalized))
    }

    /// Returns the number of tools.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no ordinary tool is exposed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns one tool by its model-visible registry-local name.
    #[must_use]
    pub fn get(&self, name: &CapabilityName) -> Option<&ToolDescriptor> {
        self.0.get(name)
    }

    /// Iterates descriptors in canonical name order.
    pub fn iter(&self) -> btree_map::Values<'_, CapabilityName, ToolDescriptor> {
        self.0.values()
    }

    fn contains_name(&self, name: &str) -> bool {
        self.0.keys().any(|candidate| candidate.as_str() == name)
    }
}

impl Default for AgentTools {
    fn default() -> Self {
        Self::empty()
    }
}

impl<'a> IntoIterator for &'a AgentTools {
    type Item = &'a ToolDescriptor;
    type IntoIter = btree_map::Values<'a, CapabilityName, ToolDescriptor>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<Vec<ToolDescriptor>> for AgentTools {
    type Error = AgentToolsError;

    fn try_from(value: Vec<ToolDescriptor>) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl fmt::Debug for AgentTools {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentTools")
            .field("count", &self.len())
            .finish_non_exhaustive()
    }
}

impl Serialize for AgentTools {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for AgentTools {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(AgentToolsVisitor)
    }
}

struct AgentToolsVisitor;

impl<'de> de::Visitor<'de> for AgentToolsVisitor {
    type Value = AgentTools;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {} active or deprecated tools with unique names",
            AgentTools::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = BTreeMap::new();
        while let Some(value) = sequence.next_element::<ToolDescriptor>()? {
            insert_tool(&mut values, value).map_err(de::Error::custom)?;
        }
        Ok(AgentTools(values))
    }
}

impl JsonSchema for AgentTools {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "AgentTools".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::AgentTools").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ToolDescriptor>(),
            "maxItems": 128,
            "uniqueItems": true,
            "description": "Tools serialize in model-visible name order. Runtime rejects duplicate names across owners or versions and retired descriptors."
        })
    }
}

fn insert_tool(
    values: &mut BTreeMap<CapabilityName, ToolDescriptor>,
    value: ToolDescriptor,
) -> Result<(), AgentToolsError> {
    let name = value.metadata().identity().capability().name().clone();
    if values.contains_key(&name) {
        return Err(AgentToolsError::DuplicateName { name });
    }
    if values.len() == AgentTools::MAX_LEN {
        return Err(AgentToolsError::TooMany {
            max: AgentTools::MAX_LEN,
            observed: AgentTools::MAX_LEN + 1,
        });
    }
    if value.metadata().lifecycle().state() == CapabilityLifecycleState::Retired {
        return Err(AgentToolsError::Retired { name });
    }
    values.insert(name, value);
    Ok(())
}

/// Invalid agent tool collection.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentToolsError {
    /// Two descriptors would expose the same name to the model.
    #[error("agent tools contain duplicate model-visible name {name}")]
    DuplicateName {
        /// Colliding registry-local name.
        name: CapabilityName,
    },
    /// The collection exceeded its hard count ceiling.
    #[error("agent tools contain at least {observed} values; maximum is {max}")]
    TooMany {
        /// Maximum accepted tool count.
        max: usize,
        /// Minimum count observed before validation stopped.
        observed: usize,
    },
    /// A retired tool cannot enter a new executable definition.
    #[error("agent tool {name} is retired")]
    Retired {
        /// Retired model-visible name.
        name: CapabilityName,
    },
}

/// Resolved mechanism used to obtain a typed final output.
///
/// Builders may negotiate a strategy, but durable descriptors store only the
/// resolved value so recovery cannot change behavior after a model upgrade.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStructuredOutputStrategy {
    /// Use the model binding's native JSON Schema response format.
    ModelNative,
    /// Use one framework-owned synthetic final-output tool definition.
    ToolCall,
}

/// Deterministic scheduling mode for ordinary tool calls from one response.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentToolConcurrency {
    /// Execute every accepted tool call in model proposal order.
    Sequential {},
    /// Parallelize read-only calls up to a finite bound; serialize all writes.
    ParallelReadOnly {
        /// Maximum concurrently executing read-only calls.
        max_concurrency: ExecutionCount,
    },
}

impl AgentToolConcurrency {
    /// Constructs deterministic sequential execution.
    #[must_use]
    pub const fn sequential() -> Self {
        Self::Sequential {}
    }

    /// Constructs bounded parallel execution for read-only calls.
    #[must_use]
    pub const fn parallel_read_only(max_concurrency: ExecutionCount) -> Self {
        Self::ParallelReadOnly { max_concurrency }
    }

    /// Returns the maximum ordinary tool concurrency.
    #[must_use]
    pub const fn maximum(self) -> ExecutionCount {
        match self {
            Self::Sequential {} => ExecutionCount::new(1),
            Self::ParallelReadOnly { max_concurrency } => max_concurrency,
        }
    }
}

/// Finite, deterministic controls for the prebuilt model/tool agent loop.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentExecutionConfig {
    structured_output: AgentStructuredOutputStrategy,
    max_model_turns: ExecutionCount,
    max_output_repair_turns: ExecutionCount,
    max_tool_calls_per_turn: ExecutionCount,
    tool_concurrency: AgentToolConcurrency,
}

impl AgentExecutionConfig {
    /// Immutable hard model-turn ceiling for one agent run.
    pub const HARD_MAX_MODEL_TURNS: ExecutionCount = ExecutionCount::new(1024);

    /// Immutable hard output-repair ceiling for one agent run.
    pub const HARD_MAX_OUTPUT_REPAIR_TURNS: ExecutionCount = ExecutionCount::new(64);

    /// Immutable hard ordinary tool-call ceiling for one model response.
    pub const HARD_MAX_TOOL_CALLS_PER_TURN: ExecutionCount = ExecutionCount::new(1024);

    /// Constructs cross-field validated loop controls.
    ///
    /// # Errors
    ///
    /// Returns [`AgentExecutionConfigError`] for zero or excessive model
    /// turns, excessive repairs/calls, impossible repair capacity, or invalid
    /// read-only concurrency.
    pub const fn new(
        structured_output: AgentStructuredOutputStrategy,
        max_model_turns: ExecutionCount,
        max_output_repair_turns: ExecutionCount,
        max_tool_calls_per_turn: ExecutionCount,
        tool_concurrency: AgentToolConcurrency,
    ) -> Result<Self, AgentExecutionConfigError> {
        if max_model_turns.get() == 0 {
            return Err(AgentExecutionConfigError::ZeroModelTurns);
        }
        if max_model_turns.get() > Self::HARD_MAX_MODEL_TURNS.get() {
            return Err(AgentExecutionConfigError::ModelTurnsAboveHardMaximum {
                maximum: Self::HARD_MAX_MODEL_TURNS,
                actual: max_model_turns,
            });
        }
        if max_output_repair_turns.get() > Self::HARD_MAX_OUTPUT_REPAIR_TURNS.get() {
            return Err(AgentExecutionConfigError::OutputRepairsAboveHardMaximum {
                maximum: Self::HARD_MAX_OUTPUT_REPAIR_TURNS,
                actual: max_output_repair_turns,
            });
        }
        if max_output_repair_turns.get() >= max_model_turns.get() {
            return Err(AgentExecutionConfigError::OutputRepairsConsumeAllTurns {
                repairs: max_output_repair_turns,
                turns: max_model_turns,
            });
        }
        if max_tool_calls_per_turn.get() > Self::HARD_MAX_TOOL_CALLS_PER_TURN.get() {
            return Err(AgentExecutionConfigError::ToolCallsAboveHardMaximum {
                maximum: Self::HARD_MAX_TOOL_CALLS_PER_TURN,
                actual: max_tool_calls_per_turn,
            });
        }
        if let AgentToolConcurrency::ParallelReadOnly { max_concurrency } = tool_concurrency {
            if max_concurrency.get() < 2 {
                return Err(AgentExecutionConfigError::ParallelConcurrencyBelowTwo {
                    actual: max_concurrency,
                });
            }
            if max_concurrency.get() > max_tool_calls_per_turn.get() {
                return Err(AgentExecutionConfigError::ConcurrencyExceedsToolCalls {
                    concurrency: max_concurrency,
                    tool_calls: max_tool_calls_per_turn,
                });
            }
        }
        Ok(Self {
            structured_output,
            max_model_turns,
            max_output_repair_turns,
            max_tool_calls_per_turn,
            tool_concurrency,
        })
    }

    /// Returns the resolved structured-output mechanism.
    #[must_use]
    pub const fn structured_output(&self) -> AgentStructuredOutputStrategy {
        self.structured_output
    }

    /// Returns the inclusive logical model-turn ceiling.
    #[must_use]
    pub const fn max_model_turns(&self) -> ExecutionCount {
        self.max_model_turns
    }

    /// Returns the number of invalid final-output repair turns permitted.
    #[must_use]
    pub const fn max_output_repair_turns(&self) -> ExecutionCount {
        self.max_output_repair_turns
    }

    /// Returns the ordinary tool-call ceiling for one model response.
    #[must_use]
    pub const fn max_tool_calls_per_turn(&self) -> ExecutionCount {
        self.max_tool_calls_per_turn
    }

    /// Returns deterministic ordinary-tool scheduling behavior.
    #[must_use]
    pub const fn tool_concurrency(&self) -> AgentToolConcurrency {
        self.tool_concurrency
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentExecutionConfigWire {
    structured_output: AgentStructuredOutputStrategy,
    max_model_turns: ExecutionCount,
    max_output_repair_turns: ExecutionCount,
    max_tool_calls_per_turn: ExecutionCount,
    tool_concurrency: AgentToolConcurrency,
}

impl<'de> Deserialize<'de> for AgentExecutionConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AgentExecutionConfigWire::deserialize(deserializer)?;
        Self::new(
            wire.structured_output,
            wire.max_model_turns,
            wire.max_output_repair_turns,
            wire.max_tool_calls_per_turn,
            wire.tool_concurrency,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid prebuilt agent-loop controls.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentExecutionConfigError {
    /// A loop allowed no model invocation.
    #[error("agent max model turns must be greater than zero")]
    ZeroModelTurns,
    /// Model-turn capacity exceeded the immutable resource bound.
    #[error("agent max model turns {actual} exceeds hard maximum {maximum}")]
    ModelTurnsAboveHardMaximum {
        /// Immutable ceiling.
        maximum: ExecutionCount,
        /// Rejected capacity.
        actual: ExecutionCount,
    },
    /// Output-repair capacity exceeded the immutable resource bound.
    #[error("agent max output repair turns {actual} exceeds hard maximum {maximum}")]
    OutputRepairsAboveHardMaximum {
        /// Immutable ceiling.
        maximum: ExecutionCount,
        /// Rejected capacity.
        actual: ExecutionCount,
    },
    /// Repair turns left no capacity for an initial model result.
    #[error("agent output repair turns {repairs} must be less than model turns {turns}")]
    OutputRepairsConsumeAllTurns {
        /// Rejected repair capacity.
        repairs: ExecutionCount,
        /// Total model-turn capacity.
        turns: ExecutionCount,
    },
    /// Per-turn tool calls exceeded the immutable resource bound.
    #[error("agent max tool calls per turn {actual} exceeds hard maximum {maximum}")]
    ToolCallsAboveHardMaximum {
        /// Immutable ceiling.
        maximum: ExecutionCount,
        /// Rejected capacity.
        actual: ExecutionCount,
    },
    /// Parallel mode was selected with sequential capacity.
    #[error("parallel read-only tool concurrency {actual} must be at least 2")]
    ParallelConcurrencyBelowTwo {
        /// Rejected concurrency.
        actual: ExecutionCount,
    },
    /// Parallel concurrency exceeded possible calls in one response.
    #[error(
        "parallel read-only concurrency {concurrency} exceeds tool calls per turn {tool_calls}"
    )]
    ConcurrencyExceedsToolCalls {
        /// Rejected concurrency.
        concurrency: ExecutionCount,
        /// Per-response call ceiling.
        tool_calls: ExecutionCount,
    },
}

/// Immutable, registry-resolved definition of one typed agent version.
///
/// This value is a definition snapshot, not a mutable runtime object. The
/// durable runtime compiles it onto journaled graph semantics and records model
/// attempts, tool invocations, approvals, and checkpoints independently.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDescriptor {
    metadata: CapabilityMetadata,
    input_schema: SchemaReference,
    output_schema: SchemaReference,
    model: ModelDescriptor,
    instructions: AgentInstructions,
    tools: AgentTools,
    execution: AgentExecutionConfig,
    budget_limits: BudgetLimits,
}

impl AgentDescriptor {
    /// Framework-owned model-visible name used by tool-call structured output.
    pub const FINAL_OUTPUT_TOOL_NAME: &'static str = "stateknot_final_output";

    /// Constructs and validates one executable agent definition snapshot.
    ///
    /// Schema documents and provider compatibility profiles must additionally
    /// be resolved and validated by the trusted local registry before this
    /// descriptor becomes selectable.
    ///
    /// # Errors
    ///
    /// Returns [`AgentDescriptorError`] for invalid classification, retired
    /// model binding, incoherent ordinary-tool controls, reserved-name
    /// shadowing, or a model capability mismatch.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metadata: CapabilityMetadata,
        input_schema: SchemaReference,
        output_schema: SchemaReference,
        model: ModelDescriptor,
        instructions: AgentInstructions,
        tools: AgentTools,
        execution: AgentExecutionConfig,
        budget_limits: BudgetLimits,
    ) -> Result<Self, AgentDescriptorError> {
        if metadata.kind() != CapabilityKind::Agent {
            return Err(AgentDescriptorError::WrongCapabilityKind {
                actual: metadata.kind(),
            });
        }
        if model.metadata().lifecycle().state() == CapabilityLifecycleState::Retired {
            return Err(AgentDescriptorError::RetiredModel {
                identity: Box::new(model.metadata().identity().clone()),
            });
        }

        let ordinary_calls = execution.max_tool_calls_per_turn();
        match (tools.is_empty(), ordinary_calls.get() == 0) {
            (true, false) => {
                return Err(AgentDescriptorError::ToolCallsWithoutTools {
                    actual: ordinary_calls,
                });
            }
            (false, true) => return Err(AgentDescriptorError::ToolsWithoutToolCalls),
            _ => {}
        }

        let synthetic_output =
            execution.structured_output() == AgentStructuredOutputStrategy::ToolCall;
        if synthetic_output && tools.contains_name(Self::FINAL_OUTPUT_TOOL_NAME) {
            return Err(AgentDescriptorError::ReservedOutputToolName);
        }

        let required_definitions = tools.len() + usize::from(synthetic_output);
        let hard_definition_maximum = ModelRequest::MAX_TOOL_DEFINITIONS;
        if required_definitions as u64 > hard_definition_maximum.get() {
            return Err(
                AgentDescriptorError::ModelRequestToolDefinitionsAboveHardMaximum {
                    maximum: hard_definition_maximum,
                    actual: ExecutionCount::new(required_definitions as u64),
                },
            );
        }
        if required_definitions > 0 {
            let capabilities = model.capabilities().tools();
            if !capabilities.supports_tool_calling() {
                return Err(AgentDescriptorError::ModelToolCallingUnsupported);
            }
            let required = ExecutionCount::new(required_definitions as u64);
            if capabilities.max_definitions() < required {
                return Err(AgentDescriptorError::ModelToolDefinitionsInsufficient {
                    required,
                    available: capabilities.max_definitions(),
                });
            }
            let required_calls = if synthetic_output {
                ordinary_calls.max(ExecutionCount::new(1))
            } else {
                ordinary_calls
            };
            if capabilities.max_calls_per_response() < required_calls {
                return Err(AgentDescriptorError::ModelToolCallsInsufficient {
                    required: required_calls,
                    available: capabilities.max_calls_per_response(),
                });
            }
        }

        if execution.structured_output() == AgentStructuredOutputStrategy::ModelNative {
            let available = model.capabilities().structured_output().level();
            if available < ModelStructuredOutputLevel::JsonSchema {
                return Err(
                    AgentDescriptorError::ModelNativeStructuredOutputUnsupported { available },
                );
            }
        }
        if let Some(deadline) = budget_limits.deadline() {
            return Err(AgentDescriptorError::AbsoluteDeadlineIsNotReusable { deadline });
        }

        Ok(Self {
            metadata,
            input_schema,
            output_schema,
            model,
            instructions,
            tools,
            execution,
            budget_limits,
        })
    }

    /// Returns common identity, discovery, lifecycle, scope, and extension data.
    #[must_use]
    pub const fn metadata(&self) -> &CapabilityMetadata {
        &self.metadata
    }

    /// Returns the immutable typed input schema identity.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaReference {
        &self.input_schema
    }

    /// Returns the immutable typed final-output schema identity.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaReference {
        &self.output_schema
    }

    /// Returns the exact resolved model binding snapshot.
    #[must_use]
    pub const fn model(&self) -> &ModelDescriptor {
        &self.model
    }

    /// Returns ordered trusted instructions.
    #[must_use]
    pub const fn instructions(&self) -> &AgentInstructions {
        &self.instructions
    }

    /// Returns canonically ordered ordinary tools.
    #[must_use]
    pub const fn tools(&self) -> &AgentTools {
        &self.tools
    }

    /// Returns finite deterministic loop controls.
    #[must_use]
    pub const fn execution(&self) -> &AgentExecutionConfig {
        &self.execution
    }

    /// Returns the optional agent-level run-budget layer.
    #[must_use]
    pub const fn budget_limits(&self) -> &BudgetLimits {
        &self.budget_limits
    }
}

impl fmt::Debug for AgentDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentDescriptor")
            .field("metadata", &self.metadata)
            .field("input_schema", &self.input_schema)
            .field("output_schema", &self.output_schema)
            .field("model", self.model.metadata().identity())
            .field("instructions", &self.instructions)
            .field("tools", &self.tools)
            .field("execution", &self.execution)
            .field("has_budget_limits", &!self.budget_limits.is_empty())
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentDescriptorWire {
    metadata: CapabilityMetadata,
    input_schema: SchemaReference,
    output_schema: SchemaReference,
    model: ModelDescriptor,
    instructions: AgentInstructions,
    tools: AgentTools,
    execution: AgentExecutionConfig,
    budget_limits: BudgetLimits,
}

impl<'de> Deserialize<'de> for AgentDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AgentDescriptorWire::deserialize(deserializer)?;
        Self::new(
            wire.metadata,
            wire.input_schema,
            wire.output_schema,
            wire.model,
            wire.instructions,
            wire.tools,
            wire.execution,
            wire.budget_limits,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid cross-component agent definition snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentDescriptorError {
    /// Common metadata classified the capability as something other than an agent.
    #[error("agent descriptor requires kind=agent, received {actual:?}")]
    WrongCapabilityKind {
        /// Conflicting capability kind.
        actual: CapabilityKind,
    },
    /// The resolved model binding is unavailable for a new definition.
    #[error("agent model binding {identity:?} is retired")]
    RetiredModel {
        /// Retired owner-qualified model identity.
        identity: Box<CapabilityIdentity>,
    },
    /// Ordinary call capacity was declared without an ordinary tool.
    #[error("agent has no ordinary tools but allows {actual} tool calls per turn")]
    ToolCallsWithoutTools {
        /// Rejected nonzero capacity.
        actual: ExecutionCount,
    },
    /// Ordinary tools were present but the loop could never invoke one.
    #[error("agent ordinary tools require a positive tool-call ceiling")]
    ToolsWithoutToolCalls,
    /// A user tool shadowed the framework-owned final-output marker.
    #[error("agent tool name stateknot_final_output is reserved for structured output")]
    ReservedOutputToolName,
    /// The pinned model cannot accept any tool definition.
    #[error("agent definition requires model tool calling, but the model does not support it")]
    ModelToolCallingUnsupported,
    /// The pinned model cannot accept all ordinary and synthetic definitions.
    #[error("agent requires {required} tool definitions, model accepts {available}")]
    ModelToolDefinitionsInsufficient {
        /// Required ordinary plus synthetic definition count.
        required: ExecutionCount,
        /// Pinned model capacity.
        available: ExecutionCount,
    },
    /// Ordinary plus synthetic definitions exceeded the core request boundary.
    #[error("agent requires {actual} tool definitions; core request maximum is {maximum}")]
    ModelRequestToolDefinitionsAboveHardMaximum {
        /// Immutable core request ceiling.
        maximum: ExecutionCount,
        /// Rejected ordinary plus synthetic definition count.
        actual: ExecutionCount,
    },
    /// The pinned model cannot emit the configured per-response calls.
    #[error("agent requires {required} tool calls per response, model allows {available}")]
    ModelToolCallsInsufficient {
        /// Required model response call capacity.
        required: ExecutionCount,
        /// Pinned model capacity.
        available: ExecutionCount,
    },
    /// Native strategy was selected for a weaker model binding.
    #[error("model-native agent output requires JSON Schema support, model provides {available:?}")]
    ModelNativeStructuredOutputUnsupported {
        /// Pinned model capability level.
        available: ModelStructuredOutputLevel,
    },
    /// A reusable definition attempted to embed a one-run absolute deadline.
    #[error("agent budget cannot embed absolute deadline {deadline}")]
    AbsoluteDeadlineIsNotReusable {
        /// Rejected absolute instant.
        deadline: crate::Timestamp,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{from_value, json, to_value};

    use crate::{
        CapabilityDescription, CapabilityLifecycle, CapabilityReference, ContentMetadata,
        ContentSource, ContentTrust, Digest, DurationMillis, Extensions, InstructionName,
        InstructionProvenance, IssuerId, ModelCapabilities, ModelModalities, ModelModality,
        ModelStructuredOutputCapabilities, ModelTokenLimits, ModelToolCapabilities,
        ModelToolChoice, ModelToolChoices, PrincipalIdentity, RedactionState, SchemaId, ScopeSet,
        SecurityLabel, SubjectId, TextContent, Timestamp, TokenCount, ToolCancellationSupport,
        ToolExecutionLimits, ToolExecutionSemantics, ToolIdempotency, ToolInvocationCapabilities,
        ToolResourceRequirements, ToolRisk, Version,
    };

    fn principal(subject: &str) -> PrincipalIdentity {
        PrincipalIdentity::new(
            "https://issuer.example.com/tenant"
                .parse::<IssuerId>()
                .unwrap(),
            subject.parse::<SubjectId>().unwrap(),
        )
    }

    fn lifecycle(retired: bool) -> CapabilityLifecycle {
        if retired {
            CapabilityLifecycle::retired(
                "2026-08-29T00:00:00.000000Z".parse::<Timestamp>().unwrap(),
                CapabilityDescription::new("Retired test binding").unwrap(),
                None,
            )
        } else {
            CapabilityLifecycle::active()
        }
    }

    fn metadata(
        kind: CapabilityKind,
        name: &str,
        owner: &str,
        retired: bool,
        description: &str,
    ) -> CapabilityMetadata {
        CapabilityMetadata::new(
            CapabilityIdentity::new(
                principal(owner),
                CapabilityReference::new(
                    name.parse::<CapabilityName>().unwrap(),
                    Version::new(1, 0, 0),
                ),
            ),
            kind,
            None,
            CapabilityDescription::new(description).unwrap(),
            lifecycle(retired),
            ScopeSet::empty(),
            Extensions::default(),
        )
        .unwrap()
    }

    fn schema(name: &str) -> SchemaReference {
        SchemaReference::new(
            format!("https://schemas.example.com/{name}/1.0.0")
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(name),
        )
    }

    fn instruction(name: &str, content: &str) -> Instruction {
        let metadata = ContentMetadata::new(
            ContentSource::Application,
            ContentTrust::ApplicationControlled,
            "internal".parse::<SecurityLabel>().unwrap(),
            RedactionState::NotApplied,
        );
        Instruction::new(
            crate::InstructionIdentity::new(
                name.parse::<InstructionName>().unwrap(),
                Version::new(1, 0, 0),
            ),
            TextContent::new(content, None, metadata).unwrap().into(),
            InstructionProvenance::new(principal("instruction-owner")),
        )
        .unwrap()
    }

    fn model(
        max_definitions: u64,
        max_calls: u64,
        structured_output: ModelStructuredOutputLevel,
        retired: bool,
    ) -> ModelDescriptor {
        let tool_capabilities = if max_definitions == 0 {
            ModelToolCapabilities::unsupported()
        } else {
            ModelToolCapabilities::new(
                Some(schema("model-tool-profile")),
                ExecutionCount::new(max_definitions),
                ExecutionCount::new(max_calls),
                ModelToolChoices::try_new([ModelToolChoice::Auto]).unwrap(),
                true,
            )
            .unwrap()
        };
        let structured_output = match structured_output {
            ModelStructuredOutputLevel::Unsupported => {
                ModelStructuredOutputCapabilities::unsupported()
            }
            ModelStructuredOutputLevel::Json => ModelStructuredOutputCapabilities::json(),
            ModelStructuredOutputLevel::JsonSchema => {
                ModelStructuredOutputCapabilities::json_schema(schema("model-output-profile"))
            }
        };
        ModelDescriptor::new(
            metadata(
                CapabilityKind::Model,
                "models.primary",
                "model-owner",
                retired,
                "Pinned model binding",
            ),
            ModelCapabilities::new(
                ModelModalities::try_new([ModelModality::Text]).unwrap(),
                ModelModalities::try_new([ModelModality::Text]).unwrap(),
                true,
                tool_capabilities,
                structured_output,
                false,
                ModelTokenLimits::new(
                    Some(TokenCount::new(128_000)),
                    Some(TokenCount::new(120_000)),
                    Some(TokenCount::new(16_384)),
                )
                .unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn tool(name: &str, retired: bool) -> ToolDescriptor {
        ToolDescriptor::new(
            metadata(
                CapabilityKind::Tool,
                name,
                "tool-owner",
                retired,
                "Read-only test tool",
            ),
            schema(&format!("{name}-input")),
            schema(&format!("{name}-output")),
            ToolExecutionSemantics::new(
                ToolRisk::ReadOnly,
                ToolIdempotency::NotApplicable,
                false,
                false,
            )
            .unwrap(),
            ToolResourceRequirements::none(),
            ToolInvocationCapabilities::new(
                ToolCancellationSupport::Cooperative,
                ExecutionCount::new(32),
            ),
            ToolExecutionLimits::new(
                DurationMillis::new(30_000).unwrap(),
                ExecutionCount::new(16),
                ByteCount::new(64 * 1024),
                ByteCount::new(256 * 1024),
                ExecutionCount::new(4),
                ByteCount::new(25 * 1024 * 1024),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn execution(strategy: AgentStructuredOutputStrategy, tool_calls: u64) -> AgentExecutionConfig {
        AgentExecutionConfig::new(
            strategy,
            ExecutionCount::new(12),
            ExecutionCount::new(2),
            ExecutionCount::new(tool_calls),
            AgentToolConcurrency::sequential(),
        )
        .unwrap()
    }

    fn descriptor(
        model: ModelDescriptor,
        tools: AgentTools,
        execution: AgentExecutionConfig,
        secret: &str,
    ) -> Result<AgentDescriptor, AgentDescriptorError> {
        AgentDescriptor::new(
            metadata(
                CapabilityKind::Agent,
                "agents.incident",
                "agent-owner",
                false,
                secret,
            ),
            schema("incident-request"),
            schema("incident-report"),
            model,
            AgentInstructions::try_new([instruction("incident.base", secret)]).unwrap(),
            tools,
            execution,
            BudgetLimits::empty()
                .with_model_turns(ExecutionCount::new(12))
                .with_tool_calls(ExecutionCount::new(24))
                .with_write_calls(ExecutionCount::new(1)),
        )
    }

    #[test]
    fn execution_config_is_finite_closed_and_revalidated() {
        let config = AgentExecutionConfig::new(
            AgentStructuredOutputStrategy::ToolCall,
            ExecutionCount::new(10),
            ExecutionCount::new(2),
            ExecutionCount::new(4),
            AgentToolConcurrency::parallel_read_only(ExecutionCount::new(3)),
        )
        .unwrap();
        assert_eq!(config.max_model_turns(), ExecutionCount::new(10));
        assert_eq!(config.max_output_repair_turns(), ExecutionCount::new(2));
        assert_eq!(config.max_tool_calls_per_turn(), ExecutionCount::new(4));
        assert_eq!(config.tool_concurrency().maximum(), ExecutionCount::new(3));
        assert_eq!(
            from_value::<AgentExecutionConfig>(to_value(&config).unwrap()).unwrap(),
            config
        );

        assert_eq!(
            AgentExecutionConfig::new(
                AgentStructuredOutputStrategy::ModelNative,
                ExecutionCount::ZERO,
                ExecutionCount::ZERO,
                ExecutionCount::ZERO,
                AgentToolConcurrency::sequential(),
            ),
            Err(AgentExecutionConfigError::ZeroModelTurns)
        );
        assert!(matches!(
            AgentExecutionConfig::new(
                AgentStructuredOutputStrategy::ModelNative,
                ExecutionCount::new(1025),
                ExecutionCount::ZERO,
                ExecutionCount::ZERO,
                AgentToolConcurrency::sequential(),
            ),
            Err(AgentExecutionConfigError::ModelTurnsAboveHardMaximum { .. })
        ));
        assert!(matches!(
            AgentExecutionConfig::new(
                AgentStructuredOutputStrategy::ModelNative,
                ExecutionCount::new(3),
                ExecutionCount::new(3),
                ExecutionCount::ZERO,
                AgentToolConcurrency::sequential(),
            ),
            Err(AgentExecutionConfigError::OutputRepairsConsumeAllTurns { .. })
        ));
        assert!(matches!(
            AgentExecutionConfig::new(
                AgentStructuredOutputStrategy::ToolCall,
                ExecutionCount::new(10),
                ExecutionCount::ZERO,
                ExecutionCount::new(4),
                AgentToolConcurrency::parallel_read_only(ExecutionCount::new(1)),
            ),
            Err(AgentExecutionConfigError::ParallelConcurrencyBelowTwo { .. })
        ));
        assert!(matches!(
            AgentExecutionConfig::new(
                AgentStructuredOutputStrategy::ToolCall,
                ExecutionCount::new(10),
                ExecutionCount::ZERO,
                ExecutionCount::new(2),
                AgentToolConcurrency::parallel_read_only(ExecutionCount::new(3)),
            ),
            Err(AgentExecutionConfigError::ConcurrencyExceedsToolCalls { .. })
        ));

        let mut unknown = to_value(config).unwrap();
        unknown["unlimited"] = json!(true);
        assert!(from_value::<AgentExecutionConfig>(unknown).is_err());
        assert!(from_value::<AgentStructuredOutputStrategy>(json!("auto")).is_err());
        assert!(from_value::<AgentToolConcurrency>(json!({ "mode": "parallel" })).is_err());
    }

    #[test]
    fn instructions_are_nonempty_ordered_unique_bounded_and_redacted() {
        assert_eq!(
            AgentInstructions::try_new([]),
            Err(AgentInstructionsError::Empty)
        );
        let first = instruction("base.first", "secret-first");
        let second = instruction("base.second", "secret-second");
        let instructions = AgentInstructions::try_new([first.clone(), second.clone()]).unwrap();
        assert_eq!(instructions.len(), 2);
        assert!(!instructions.is_empty());
        assert_eq!(instructions.as_slice()[0].identity(), first.identity());
        assert_eq!(instructions.as_slice()[1].identity(), second.identity());
        assert_eq!(
            AgentInstructions::try_new([first.clone(), first]),
            Err(AgentInstructionsError::Duplicate {
                identity: instruction("base.first", "replacement").identity().clone(),
            })
        );
        let encoded = to_value(&instructions).unwrap();
        assert_eq!(
            from_value::<AgentInstructions>(encoded).unwrap(),
            instructions
        );
        let debug = format!("{instructions:?}");
        assert!(!debug.contains("secret-first"));
        assert!(!debug.contains("secret-second"));

        let too_many = (0..=AgentInstructions::MAX_LEN)
            .map(|index| instruction(&format!("instruction.{index}"), "bounded"));
        assert!(matches!(
            AgentInstructions::try_new(too_many),
            Err(AgentInstructionsError::TooMany { .. })
        ));

        let schema = to_value(schemars::schema_for!(AgentInstructions)).unwrap();
        assert_eq!(schema["minItems"], 1);
        assert_eq!(schema["maxItems"], AgentInstructions::MAX_LEN);
    }

    #[test]
    fn tools_are_canonical_unique_bounded_and_active() {
        let tools =
            AgentTools::try_new([tool("tools.zeta", false), tool("tools.alpha", false)]).unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(
            tools
                .iter()
                .map(|tool| tool.metadata().identity().capability().name().as_str())
                .collect::<Vec<_>>(),
            ["tools.alpha", "tools.zeta"]
        );
        assert!(tools.get(&"tools.alpha".parse().unwrap()).is_some());
        assert_eq!(
            from_value::<AgentTools>(to_value(&tools).unwrap()).unwrap(),
            tools
        );
        assert!(matches!(
            AgentTools::try_new([tool("tools.same", false), tool("tools.same", false)]),
            Err(AgentToolsError::DuplicateName { .. })
        ));
        assert!(matches!(
            AgentTools::try_new([tool("tools.retired", true)]),
            Err(AgentToolsError::Retired { .. })
        ));

        let too_many =
            (0..=AgentTools::MAX_LEN).map(|index| tool(&format!("tools.t{index}"), false));
        assert!(matches!(
            AgentTools::try_new(too_many),
            Err(AgentToolsError::TooMany { .. })
        ));

        let schema = to_value(schemars::schema_for!(AgentTools)).unwrap();
        assert_eq!(schema["maxItems"], AgentTools::MAX_LEN);
    }

    #[test]
    fn descriptor_binds_every_dependency_and_redacts_instruction_content() {
        let descriptor = descriptor(
            model(8, 4, ModelStructuredOutputLevel::JsonSchema, false),
            AgentTools::try_new([tool("tools.lookup", false)]).unwrap(),
            execution(AgentStructuredOutputStrategy::ModelNative, 4),
            "never-log-this-instruction",
        )
        .unwrap();
        assert_eq!(descriptor.metadata().kind(), CapabilityKind::Agent);
        assert_eq!(descriptor.input_schema(), &schema("incident-request"));
        assert_eq!(descriptor.output_schema(), &schema("incident-report"));
        assert_eq!(descriptor.instructions().len(), 1);
        assert_eq!(descriptor.tools().len(), 1);
        assert_eq!(
            descriptor.execution().structured_output(),
            AgentStructuredOutputStrategy::ModelNative
        );
        assert_eq!(
            descriptor.budget_limits().tool_calls(),
            Some(ExecutionCount::new(24))
        );

        let encoded = to_value(&descriptor).unwrap();
        assert_eq!(from_value::<AgentDescriptor>(encoded).unwrap(), descriptor);
        let debug = format!("{descriptor:?}");
        assert!(!debug.contains("never-log-this-instruction"));
        assert!(!debug.contains("instruction-owner"));
    }

    #[test]
    fn descriptor_rejects_incoherent_tool_and_output_capabilities() {
        let native_model = || model(8, 4, ModelStructuredOutputLevel::JsonSchema, false);
        let no_tools = AgentTools::empty();
        assert!(matches!(
            descriptor(
                native_model(),
                no_tools.clone(),
                execution(AgentStructuredOutputStrategy::ModelNative, 1),
                "agent"
            ),
            Err(AgentDescriptorError::ToolCallsWithoutTools { .. })
        ));
        assert_eq!(
            descriptor(
                native_model(),
                AgentTools::try_new([tool("tools.lookup", false)]).unwrap(),
                execution(AgentStructuredOutputStrategy::ModelNative, 0),
                "agent"
            ),
            Err(AgentDescriptorError::ToolsWithoutToolCalls)
        );
        assert_eq!(
            descriptor(
                model(0, 0, ModelStructuredOutputLevel::JsonSchema, false),
                AgentTools::try_new([tool("tools.lookup", false)]).unwrap(),
                execution(AgentStructuredOutputStrategy::ModelNative, 1),
                "agent"
            ),
            Err(AgentDescriptorError::ModelToolCallingUnsupported)
        );
        assert!(matches!(
            descriptor(
                model(1, 4, ModelStructuredOutputLevel::Unsupported, false),
                AgentTools::try_new([tool("tools.lookup", false)]).unwrap(),
                execution(AgentStructuredOutputStrategy::ToolCall, 1),
                "agent"
            ),
            Err(AgentDescriptorError::ModelToolDefinitionsInsufficient { .. })
        ));
        assert!(matches!(
            descriptor(
                model(8, 1, ModelStructuredOutputLevel::JsonSchema, false),
                AgentTools::try_new([tool("tools.lookup", false)]).unwrap(),
                execution(AgentStructuredOutputStrategy::ModelNative, 2),
                "agent"
            ),
            Err(AgentDescriptorError::ModelToolCallsInsufficient { .. })
        ));
        assert!(matches!(
            descriptor(
                model(8, 4, ModelStructuredOutputLevel::Json, false),
                AgentTools::empty(),
                execution(AgentStructuredOutputStrategy::ModelNative, 0),
                "agent"
            ),
            Err(
                AgentDescriptorError::ModelNativeStructuredOutputUnsupported {
                    available: ModelStructuredOutputLevel::Json
                }
            )
        ));

        descriptor(
            model(2, 1, ModelStructuredOutputLevel::Unsupported, false),
            AgentTools::try_new([tool("tools.lookup", false)]).unwrap(),
            execution(AgentStructuredOutputStrategy::ToolCall, 1),
            "tool-output-agent",
        )
        .unwrap();
        assert_eq!(
            descriptor(
                model(1, 1, ModelStructuredOutputLevel::Unsupported, false),
                AgentTools::try_new([tool(AgentDescriptor::FINAL_OUTPUT_TOOL_NAME, false)])
                    .unwrap(),
                execution(AgentStructuredOutputStrategy::ToolCall, 1),
                "agent"
            ),
            Err(AgentDescriptorError::ReservedOutputToolName)
        );
    }

    #[test]
    fn descriptor_rejects_wrong_kind_and_retired_model() {
        let instructions = AgentInstructions::try_new([instruction("base", "trusted")]).unwrap();
        let config = execution(AgentStructuredOutputStrategy::ModelNative, 0);
        assert!(matches!(
            AgentDescriptor::new(
                metadata(
                    CapabilityKind::Workflow,
                    "agents.invalid",
                    "agent-owner",
                    false,
                    "Wrong kind"
                ),
                schema("input"),
                schema("output"),
                model(0, 0, ModelStructuredOutputLevel::JsonSchema, false),
                instructions.clone(),
                AgentTools::empty(),
                config.clone(),
                BudgetLimits::empty(),
            ),
            Err(AgentDescriptorError::WrongCapabilityKind {
                actual: CapabilityKind::Workflow
            })
        ));
        assert!(matches!(
            AgentDescriptor::new(
                metadata(
                    CapabilityKind::Agent,
                    "agents.invalid",
                    "agent-owner",
                    false,
                    "Retired model"
                ),
                schema("input"),
                schema("output"),
                model(0, 0, ModelStructuredOutputLevel::JsonSchema, true),
                instructions,
                AgentTools::empty(),
                config,
                BudgetLimits::empty(),
            ),
            Err(AgentDescriptorError::RetiredModel { .. })
        ));
    }

    #[test]
    fn descriptor_rejects_nonreusable_deadlines_and_request_definition_overflow() {
        let instructions =
            || AgentInstructions::try_new([instruction("base", "trusted definition")]).unwrap();
        let deadline = "2027-01-01T00:00:00.000000Z".parse::<Timestamp>().unwrap();
        assert_eq!(
            AgentDescriptor::new(
                metadata(
                    CapabilityKind::Agent,
                    "agents.deadline",
                    "agent-owner",
                    false,
                    "Absolute deadline"
                ),
                schema("input"),
                schema("output"),
                model(0, 0, ModelStructuredOutputLevel::JsonSchema, false),
                instructions(),
                AgentTools::empty(),
                execution(AgentStructuredOutputStrategy::ModelNative, 0),
                BudgetLimits::empty().with_deadline(deadline),
            ),
            Err(AgentDescriptorError::AbsoluteDeadlineIsNotReusable { deadline })
        );

        let tools = AgentTools::try_new(
            (0..AgentTools::MAX_LEN).map(|index| tool(&format!("tools.t{index}"), false)),
        )
        .unwrap();
        assert!(matches!(
            AgentDescriptor::new(
                metadata(
                    CapabilityKind::Agent,
                    "agents.capacity",
                    "agent-owner",
                    false,
                    "Definition capacity"
                ),
                schema("input"),
                schema("output"),
                model(129, 1, ModelStructuredOutputLevel::Unsupported, false),
                instructions(),
                tools,
                execution(AgentStructuredOutputStrategy::ToolCall, 1),
                BudgetLimits::empty(),
            ),
            Err(AgentDescriptorError::ModelRequestToolDefinitionsAboveHardMaximum {
                maximum: ModelRequest::MAX_TOOL_DEFINITIONS,
                actual
            }) if actual == ExecutionCount::new(129)
        ));
    }

    #[test]
    fn agent_schemas_close_objects_and_publish_collection_bounds() {
        let descriptor_schema = to_value(schemars::schema_for!(AgentDescriptor)).unwrap();
        assert_eq!(descriptor_schema["additionalProperties"], false);
        let execution_schema = to_value(schemars::schema_for!(AgentExecutionConfig)).unwrap();
        assert_eq!(execution_schema["additionalProperties"], false);

        let descriptor = descriptor(
            model(0, 0, ModelStructuredOutputLevel::JsonSchema, false),
            AgentTools::empty(),
            execution(AgentStructuredOutputStrategy::ModelNative, 0),
            "closed-wire",
        )
        .unwrap();
        let mut wire = to_value(descriptor).unwrap();
        wire["dynamic_tools"] = json!(true);
        assert!(from_value::<AgentDescriptor>(wire).is_err());
    }

    proptest! {
        #[test]
        fn valid_sequential_execution_configs_round_trip(
            turns in 1_u64..=1024,
            repairs in 0_u64..=64,
            tool_calls in 0_u64..=1024,
            tool_output in any::<bool>(),
        ) {
            prop_assume!(repairs < turns);
            let strategy = if tool_output {
                AgentStructuredOutputStrategy::ToolCall
            } else {
                AgentStructuredOutputStrategy::ModelNative
            };
            let config = AgentExecutionConfig::new(
                strategy,
                ExecutionCount::new(turns),
                ExecutionCount::new(repairs),
                ExecutionCount::new(tool_calls),
                AgentToolConcurrency::sequential(),
            ).unwrap();
            let wire = to_value(&config).unwrap();
            prop_assert_eq!(from_value::<AgentExecutionConfig>(wire).unwrap(), config);
        }
    }
}
