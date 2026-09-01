// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Strongly typed startup ergonomics for durable agent definitions.

use std::{
    fmt,
    io::{self, Write},
    marker::PhantomData,
    sync::Arc,
};

use schemars::{JsonSchema, generate::SchemaSettings};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{
    AgentDescriptor, AgentDescriptorError, AgentExecutionConfig, AgentInstructions, AgentRequest,
    AgentResult, AgentResultProvenance, AgentResultValidationError, AgentStructuredOutputStrategy,
    AgentTools, BoundedJson, BudgetLimits, Digest, GraphSchemaValidationError, JsonLimits,
    ModelDescriptor, SchemaId, SchemaReference,
};
use thiserror::Error;

use crate::{JsonSchemaRegistry, JsonSchemaRegistryBuilder, JsonSchemaRegistryError};

/// Position of a generated schema in one typed agent definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AgentSchemaRole {
    /// Typed request input serialized by the application.
    Input,
    /// Typed successful output deserialized by the application.
    Output,
}

impl fmt::Display for AgentSchemaRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Input => "input",
            Self::Output => "output",
        })
    }
}

/// Startup-only builder for one immutable, strongly typed agent definition.
///
/// The builder generates explicit JSON Schema 2020-12 documents using the
/// serialization contract of `I` and deserialization contract of `O`. It then
/// digest-pins those documents into the ordinary provider-neutral
/// [`AgentDescriptor`]. No execution or persistence boundary is bypassed.
pub struct AgentBuilder<I, O> {
    metadata: stateknot_core::CapabilityMetadata,
    input_schema_id: SchemaId,
    output_schema_id: SchemaId,
    model: ModelDescriptor,
    instructions: AgentInstructions,
    tools: AgentTools,
    execution: AgentExecutionConfig,
    budget_limits: BudgetLimits,
    input_json_limits: JsonLimits,
    marker: PhantomData<fn(I) -> O>,
}

impl<I, O> AgentBuilder<I, O> {
    /// Creates a builder with no ordinary tools, no agent-local budget layer,
    /// and the standard bounded-JSON input limits.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metadata: stateknot_core::CapabilityMetadata,
        input_schema_id: SchemaId,
        output_schema_id: SchemaId,
        model: ModelDescriptor,
        instructions: AgentInstructions,
        execution: AgentExecutionConfig,
    ) -> Self {
        Self {
            metadata,
            input_schema_id,
            output_schema_id,
            model,
            instructions,
            tools: AgentTools::empty(),
            execution,
            budget_limits: BudgetLimits::empty(),
            input_json_limits: JsonLimits::DEFAULT,
            marker: PhantomData,
        }
    }

    /// Installs the exact ordinary-tool descriptor snapshots exposed by the agent.
    #[must_use]
    pub fn with_tools(mut self, tools: AgentTools) -> Self {
        self.tools = tools;
        self
    }

    /// Adds an immutable agent-level budget layer.
    #[must_use]
    pub fn with_budget_limits(mut self, budget_limits: BudgetLimits) -> Self {
        self.budget_limits = budget_limits;
        self
    }

    /// Selects validated resource ceilings for typed input serialization.
    #[must_use]
    pub const fn with_input_json_limits(mut self, limits: JsonLimits) -> Self {
        self.input_json_limits = limits;
        self
    }
}

impl<I, O> AgentBuilder<I, O>
where
    I: JsonSchema,
    O: JsonSchema,
{
    /// Generates, bounds, canonicalizes, and digest-pins both typed schemas,
    /// then constructs the ordinary immutable agent descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`AgentBuilderError`] for ambiguous schema identity, schema
    /// generation/resource failure, or an incoherent agent descriptor.
    pub fn build(self) -> Result<TypedAgentDefinition<I, O>, AgentBuilderError> {
        if self.input_schema_id == self.output_schema_id {
            return Err(AgentBuilderError::DuplicateSchemaId);
        }

        let version = self.metadata.identity().version();
        let (input_schema, input_document) = generate_schema::<I>(
            self.input_schema_id,
            version,
            AgentSchemaRole::Input,
            SchemaContract::Serialize,
        )?;
        let (output_schema, output_document) = generate_schema::<O>(
            self.output_schema_id,
            version,
            AgentSchemaRole::Output,
            SchemaContract::Deserialize,
        )?;
        let descriptor = AgentDescriptor::new(
            self.metadata,
            input_schema,
            output_schema,
            self.model,
            self.instructions,
            self.tools,
            self.execution,
            self.budget_limits,
        )
        .map_err(AgentBuilderError::descriptor)?;

        Ok(TypedAgentDefinition {
            descriptor,
            input_document,
            output_document,
            input_json_limits: self.input_json_limits,
            marker: PhantomData,
        })
    }
}

impl<I, O> fmt::Debug for AgentBuilder<I, O> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentBuilder")
            .field("agent", self.metadata.identity())
            .field("input_schema_id", &self.input_schema_id)
            .field("output_schema_id", &self.output_schema_id)
            .field("model", self.model.metadata().identity())
            .field("tool_count", &self.tools.len())
            .field("has_budget_limits", &!self.budget_limits.is_empty())
            .finish_non_exhaustive()
    }
}

/// Generated typed schemas plus an immutable agent descriptor, before binding
/// to one complete offline schema registry.
pub struct TypedAgentDefinition<I, O> {
    descriptor: AgentDescriptor,
    input_document: Value,
    output_document: Value,
    input_json_limits: JsonLimits,
    marker: PhantomData<fn(I) -> O>,
}

impl<I, O> TypedAgentDefinition<I, O> {
    /// Returns the immutable provider-neutral descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &AgentDescriptor {
        &self.descriptor
    }

    /// Returns the generated JSON Schema 2020-12 input document.
    #[must_use]
    pub const fn input_schema_document(&self) -> &Value {
        &self.input_document
    }

    /// Returns the generated JSON Schema 2020-12 output document.
    #[must_use]
    pub const fn output_schema_document(&self) -> &Value {
        &self.output_document
    }

    /// Registers both generated schema resources in a startup-only registry builder.
    ///
    /// The builder is moved in and returned only after both registrations
    /// succeed, so a caller can never observe a partially installed pair.
    ///
    /// # Errors
    ///
    /// Returns [`AgentSchemaRegistrationError`] when either generated resource
    /// conflicts with, or exceeds the limits of, the registry.
    pub fn register_schemas(
        &self,
        mut registry: JsonSchemaRegistryBuilder,
    ) -> Result<JsonSchemaRegistryBuilder, AgentSchemaRegistrationError> {
        registry
            .register(
                self.descriptor.input_schema().clone(),
                self.input_document.clone(),
            )
            .map_err(|source| AgentSchemaRegistrationError::new(AgentSchemaRole::Input, source))?;
        registry
            .register(
                self.descriptor.output_schema().clone(),
                self.output_document.clone(),
            )
            .map_err(|source| AgentSchemaRegistrationError::new(AgentSchemaRole::Output, source))?;
        Ok(registry)
    }

    /// Binds this definition to an immutable offline registry and validates
    /// every agent, tool, and provider-profile schema before admission begins.
    ///
    /// # Errors
    ///
    /// Returns [`TypedAgentBindError`] for any missing schema resource,
    /// structurally unsafe schema document, or provider-profile rejection.
    pub fn bind(
        self,
        registry: Arc<JsonSchemaRegistry>,
    ) -> Result<TypedAgent<I, O>, TypedAgentBindError> {
        validate_definition_schemas(&self.descriptor, &registry)?;
        Ok(TypedAgent {
            descriptor: self.descriptor,
            schemas: registry,
            input_json_limits: self.input_json_limits,
            marker: PhantomData,
        })
    }
}

impl<I, O> fmt::Debug for TypedAgentDefinition<I, O> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedAgentDefinition")
            .field("descriptor", &self.descriptor)
            .field("input_schema_bytes", &canonical_len(&self.input_document))
            .field("output_schema_bytes", &canonical_len(&self.output_document))
            .finish_non_exhaustive()
    }
}

/// Strongly typed codec bound to a fully frozen agent and schema registry.
///
/// `TypedAgent` prepares and validates durable request/result values. Actual
/// execution remains the responsibility of the durable admission, invocation,
/// graph Driver, lifecycle, and [`crate::DurableAgentLoop`] boundaries.
pub struct TypedAgent<I, O> {
    descriptor: AgentDescriptor,
    schemas: Arc<JsonSchemaRegistry>,
    input_json_limits: JsonLimits,
    marker: PhantomData<fn(I) -> O>,
}

impl<I, O> TypedAgent<I, O> {
    /// Returns the exact immutable agent descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &AgentDescriptor {
        &self.descriptor
    }

    /// Returns the complete offline schema registry used for local validation.
    #[must_use]
    pub const fn schema_registry(&self) -> &Arc<JsonSchemaRegistry> {
        &self.schemas
    }
}

impl<I, O> TypedAgent<I, O>
where
    I: Serialize,
{
    /// Serializes and locally schema-validates one typed request under finite
    /// JSON limits. Tenant/run/thread/invocation identity is intentionally not
    /// accepted here; trusted run admission assigns it later.
    ///
    /// # Errors
    ///
    /// Returns [`TypedAgentInputError`] when serialization is unsuccessful,
    /// resource limits are exceeded, or the value violates the generated schema.
    pub fn prepare_request(
        &self,
        input: &I,
        budget_limits: BudgetLimits,
    ) -> Result<AgentRequest, TypedAgentInputError> {
        let mut writer = BoundedBuffer::new(self.input_json_limits.max_bytes());
        if serde_json::to_writer(&mut writer, input).is_err() {
            return Err(if writer.overflowed {
                TypedAgentInputError::ResourceLimit
            } else {
                TypedAgentInputError::Serialization
            });
        }
        let input = BoundedJson::from_slice_with_limits(&writer.bytes, self.input_json_limits)
            .map_err(|_| TypedAgentInputError::ResourceLimit)?;
        match self
            .schemas
            .validate_bounded(self.descriptor.input_schema(), &input)
        {
            Ok(()) => Ok(AgentRequest::new(
                self.descriptor.input_schema().clone(),
                input,
                budget_limits,
            )),
            Err(GraphSchemaValidationError::Rejected) => Err(TypedAgentInputError::SchemaRejected),
            Err(GraphSchemaValidationError::Unavailable) => {
                Err(TypedAgentInputError::RegistryInvariant)
            }
            Err(_) => Err(TypedAgentInputError::RegistryInvariant),
        }
    }
}

impl<I, O> TypedAgent<I, O>
where
    O: DeserializeOwned,
{
    /// Revalidates trusted admission/result provenance, complete budget
    /// accounting, the pinned output schema, and finally decodes `O`.
    ///
    /// # Errors
    ///
    /// Returns [`TypedAgentOutputError`] for substituted durable evidence,
    /// budget/accounting failure, schema rejection, or typed decoding failure.
    pub fn decode_result(
        &self,
        result: &AgentResult,
        expected_provenance: &AgentResultProvenance,
        request: &AgentRequest,
        budget: &stateknot_core::ResolvedBudget,
    ) -> Result<O, TypedAgentOutputError> {
        result
            .validate_for(expected_provenance, request, &self.descriptor, budget)
            .map_err(TypedAgentOutputError::validation)?;
        match self
            .schemas
            .validate_bounded(self.descriptor.output_schema(), result.output())
        {
            Ok(()) => {}
            Err(GraphSchemaValidationError::Rejected) => {
                return Err(TypedAgentOutputError::SchemaRejected);
            }
            Err(GraphSchemaValidationError::Unavailable) => {
                return Err(TypedAgentOutputError::RegistryInvariant);
            }
            Err(_) => return Err(TypedAgentOutputError::RegistryInvariant),
        }
        serde::Deserialize::deserialize(result.output().as_value())
            .map_err(|_| TypedAgentOutputError::Deserialization)
    }
}

impl<I, O> Clone for TypedAgent<I, O> {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            schemas: Arc::clone(&self.schemas),
            input_json_limits: self.input_json_limits,
            marker: PhantomData,
        }
    }
}

impl<I, O> fmt::Debug for TypedAgent<I, O> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TypedAgent")
            .field("descriptor", &self.descriptor)
            .field("schemas", &self.schemas)
            .field("input_json_limits", &self.input_json_limits)
            .finish_non_exhaustive()
    }
}

/// Invalid typed-agent construction.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentBuilderError {
    /// Input and output attempted to use one registry URI.
    #[error("typed agent input and output schema identifiers must be distinct")]
    DuplicateSchemaId,
    /// A generated schema could not be represented as JSON.
    #[error("generated {role} schema could not be represented as JSON")]
    SchemaSerialization {
        /// Schema being generated.
        role: AgentSchemaRole,
    },
    /// A generated schema exceeded the runtime-safe JSON envelope.
    #[error("generated {role} schema exceeds the bounded schema envelope")]
    SchemaResourceLimit {
        /// Schema being generated.
        role: AgentSchemaRole,
    },
    /// The resulting immutable descriptor was internally incoherent.
    #[error("typed agent descriptor is invalid: {source}")]
    Descriptor {
        /// Underlying provider-neutral descriptor error.
        #[source]
        source: AgentDescriptorError,
    },
}

impl AgentBuilderError {
    const fn descriptor(source: AgentDescriptorError) -> Self {
        Self::Descriptor { source }
    }
}

/// Failure while registering generated agent schemas.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("generated {role} schema registration failed: {source}")]
pub struct AgentSchemaRegistrationError {
    role: AgentSchemaRole,
    #[source]
    source: JsonSchemaRegistryError,
}

impl AgentSchemaRegistrationError {
    const fn new(role: AgentSchemaRole, source: JsonSchemaRegistryError) -> Self {
        Self { role, source }
    }

    /// Returns which generated schema failed to register.
    #[must_use]
    pub const fn role(&self) -> AgentSchemaRole {
        self.role
    }

    /// Returns the underlying registry validation failure.
    #[must_use]
    pub const fn source_error(&self) -> &JsonSchemaRegistryError {
        &self.source
    }
}

/// Startup-time typed-agent binding failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TypedAgentBindError {
    /// One exact referenced schema was not installed locally.
    #[error("typed agent referenced a schema that is absent from the offline registry")]
    SchemaUnavailable {
        /// Missing immutable schema identity.
        schema: Box<SchemaReference>,
    },
    /// A schema document could not enter the bounded profile-validation envelope.
    #[error("typed agent schema document exceeds the bounded profile-validation envelope")]
    InvalidSchemaDocument {
        /// Rejected immutable schema identity.
        schema: Box<SchemaReference>,
    },
    /// A provider compatibility profile rejected an exact schema document.
    #[error("typed agent schema is incompatible with its pinned provider profile")]
    ProfileRejected {
        /// Rejected schema identity.
        schema: Box<SchemaReference>,
        /// Provider-profile identity that rejected it.
        profile: Box<SchemaReference>,
    },
}

/// Typed input preparation failure. No input value is retained or formatted.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TypedAgentInputError {
    /// The application serializer failed.
    #[error("typed agent input serialization failed")]
    Serialization,
    /// Serialized JSON exceeded a configured structural or byte ceiling.
    #[error("typed agent input exceeds its bounded JSON limits")]
    ResourceLimit,
    /// Serialized JSON did not satisfy the pinned generated input schema.
    #[error("typed agent input was rejected by its pinned schema")]
    SchemaRejected,
    /// A schema disappeared from an immutable registry after startup binding.
    #[error("typed agent schema registry invariant was violated")]
    RegistryInvariant,
}

/// Typed terminal-output verification or decoding failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TypedAgentOutputError {
    /// Durable identity, schema, accounting, or budget evidence did not match admission.
    #[error("typed agent result validation failed: {source}")]
    ResultValidation {
        /// Underlying durable result validation failure.
        #[source]
        source: AgentResultValidationError,
    },
    /// Final JSON did not satisfy the pinned generated output schema.
    #[error("typed agent output was rejected by its pinned schema")]
    SchemaRejected,
    /// A schema disappeared from an immutable registry after startup binding.
    #[error("typed agent schema registry invariant was violated")]
    RegistryInvariant,
    /// Schema-valid JSON could not be decoded by the application type.
    #[error("typed agent output deserialization failed")]
    Deserialization,
}

impl TypedAgentOutputError {
    const fn validation(source: AgentResultValidationError) -> Self {
        Self::ResultValidation { source }
    }
}

#[derive(Clone, Copy)]
enum SchemaContract {
    Serialize,
    Deserialize,
}

fn generate_schema<T: JsonSchema>(
    id: SchemaId,
    version: stateknot_core::Version,
    role: AgentSchemaRole,
    contract: SchemaContract,
) -> Result<(SchemaReference, Value), AgentBuilderError> {
    let settings = match contract {
        SchemaContract::Serialize => SchemaSettings::draft2020_12().for_serialize(),
        SchemaContract::Deserialize => SchemaSettings::draft2020_12().for_deserialize(),
    };
    let schema = settings.into_generator().into_root_schema_for::<T>();
    let mut document = serde_json::to_value(schema)
        .map_err(|_| AgentBuilderError::SchemaSerialization { role })?;
    let object = document
        .as_object_mut()
        .ok_or(AgentBuilderError::SchemaSerialization { role })?;
    object.insert("$id".to_owned(), Value::String(id.as_str().to_owned()));
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| AgentBuilderError::SchemaSerialization { role })?;
    BoundedJson::from_slice_with_limits(&canonical, JsonLimits::MAXIMUM)
        .map_err(|_| AgentBuilderError::SchemaResourceLimit { role })?;
    let reference = SchemaReference::new(id, version, Digest::sha256(&canonical));
    Ok((reference, document))
}

fn validate_definition_schemas(
    descriptor: &AgentDescriptor,
    registry: &JsonSchemaRegistry,
) -> Result<(), TypedAgentBindError> {
    require_schema(registry, descriptor.input_schema())?;
    require_schema(registry, descriptor.output_schema())?;

    for profile in [
        descriptor.model().capabilities().tools().schema_profile(),
        descriptor
            .model()
            .capabilities()
            .structured_output()
            .schema_profile(),
    ]
    .into_iter()
    .flatten()
    {
        require_schema(registry, profile)?;
    }

    let output_profile = match descriptor.execution().structured_output() {
        AgentStructuredOutputStrategy::ModelNative => descriptor
            .model()
            .capabilities()
            .structured_output()
            .schema_profile(),
        AgentStructuredOutputStrategy::ToolCall => {
            descriptor.model().capabilities().tools().schema_profile()
        }
    }
    .expect("AgentDescriptor guarantees a profile for its structured-output strategy");
    validate_profile(registry, output_profile, descriptor.output_schema())?;

    if !descriptor.tools().is_empty() {
        let tool_profile = descriptor
            .model()
            .capabilities()
            .tools()
            .schema_profile()
            .expect("AgentDescriptor guarantees a tool profile for ordinary tools");
        for tool in descriptor.tools() {
            require_schema(registry, tool.input_schema())?;
            require_schema(registry, tool.output_schema())?;
            validate_profile(registry, tool_profile, tool.input_schema())?;
        }
    }
    Ok(())
}

fn require_schema(
    registry: &JsonSchemaRegistry,
    schema: &SchemaReference,
) -> Result<(), TypedAgentBindError> {
    if registry.contains(schema) {
        Ok(())
    } else {
        Err(TypedAgentBindError::SchemaUnavailable {
            schema: Box::new(schema.clone()),
        })
    }
}

fn validate_profile(
    registry: &JsonSchemaRegistry,
    profile: &SchemaReference,
    schema: &SchemaReference,
) -> Result<(), TypedAgentBindError> {
    let bytes =
        registry
            .canonical_bytes(schema)
            .ok_or_else(|| TypedAgentBindError::SchemaUnavailable {
                schema: Box::new(schema.clone()),
            })?;
    let document =
        BoundedJson::from_slice_with_limits(bytes, JsonLimits::MAXIMUM).map_err(|_| {
            TypedAgentBindError::InvalidSchemaDocument {
                schema: Box::new(schema.clone()),
            }
        })?;
    match registry.validate_bounded(profile, &document) {
        Ok(()) => Ok(()),
        Err(GraphSchemaValidationError::Unavailable) => {
            Err(TypedAgentBindError::SchemaUnavailable {
                schema: Box::new(profile.clone()),
            })
        }
        Err(_) => Err(TypedAgentBindError::ProfileRejected {
            schema: Box::new(schema.clone()),
            profile: Box::new(profile.clone()),
        }),
    }
}

fn canonical_len(document: &Value) -> usize {
    serde_json_canonicalizer::to_vec(document).map_or(0, |bytes| bytes.len())
}

struct BoundedBuffer {
    bytes: Vec<u8>,
    maximum: usize,
    overflowed: bool,
}

impl BoundedBuffer {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum.min(4096)),
            maximum,
            overflowed: false,
        }
    }
}

impl Write for BoundedBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(length) = self.bytes.len().checked_add(buffer.len()) else {
            self.overflowed = true;
            return Err(io::Error::other("typed JSON byte limit exceeded"));
        };
        if length > self.maximum {
            self.overflowed = true;
            return Err(io::Error::other("typed JSON byte limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
