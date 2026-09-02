// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Compiles a provider-native model/tool graph without external I/O.

use std::{error::Error, sync::Arc};

use futures_core::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use stateknot_core::{
    AgentExecutionConfig, AgentInstructions, AgentStructuredOutputStrategy, AgentToolConcurrency,
    AgentTools, ByteCount, CapabilityDescription, CapabilityIdentity, CapabilityKind,
    CapabilityLifecycle, CapabilityMetadata, CapabilityName, CapabilityReference, ContentMetadata,
    ContentSource, ContentTrust, Digest, DurationMillis, ExecutionCount, Extensions, Instruction,
    InstructionContent, InstructionIdentity, InstructionName, InstructionProvenance, IssuerId,
    ModelCapabilities, ModelDescriptor, ModelModalities, ModelModality,
    ModelStructuredOutputCapabilities, ModelTokenLimits, ModelToolCapabilities, ModelToolChoice,
    ModelToolChoices, PrincipalIdentity, RedactionState, SchemaId, SchemaReference, ScopeSet,
    SecurityLabel, SubjectId, TextContent, ToolCancellationSupport, ToolDescriptor,
    ToolExecutionLimits, ToolExecutionSemantics, ToolIdempotency, ToolInvocationCapabilities,
    ToolResourceRequirements, ToolRisk, Version,
};
use stateknot_runtime::{
    AgentBuilder, AgentInvocationAccounting, AgentInvocationAccountingReference,
    AgentInvocationCharge, AgentToolPolicy, AgentToolPolicyContext, AgentToolPolicyDecision,
    AgentToolPolicyError, AgentToolPolicyReference, JsonSchemaRegistryBuilder,
    ProviderNativeAgentGraph, TypedAgentDefinition, register_standard_agent_admission_event_schema,
    register_standard_agent_cancellation_event_schema, register_standard_graph_driver_event_schema,
    register_standard_graph_lifecycle_event_schema,
    register_standard_invocation_execution_event_schema,
};

const VERSION: Version = Version::new(1, 0, 0);
const DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";

#[derive(JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct IncidentRequest {
    incident_id: String,
    question: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct IncidentReport {
    summary: String,
    evidence_count: u32,
}

struct RegisteredReadOnlyPolicy {
    reference: AgentToolPolicyReference,
    allowed_tool: CapabilityIdentity,
}

impl AgentToolPolicy for RegisteredReadOnlyPolicy {
    fn reference(&self) -> &AgentToolPolicyReference {
        &self.reference
    }

    fn evaluate(
        &self,
        context: AgentToolPolicyContext,
    ) -> BoxFuture<'_, Result<AgentToolPolicyDecision, AgentToolPolicyError>> {
        let tool_is_allowed = context.proposal().tool() == &self.allowed_tool;
        let mut evidence = b"stateknot.example.read-only-policy.v1\0".to_vec();
        evidence.extend_from_slice(context.action_digest().as_bytes());
        let evidence_digest = Digest::sha256(evidence);
        Box::pin(async move {
            if tool_is_allowed {
                Ok(AgentToolPolicyDecision::Allow { evidence_digest })
            } else {
                Err(AgentToolPolicyError::InvalidEvidence)
            }
        })
    }
}

struct UnpricedAccounting {
    reference: AgentInvocationAccountingReference,
}

impl AgentInvocationAccounting for UnpricedAccounting {
    fn reference(&self) -> &AgentInvocationAccountingReference {
        &self.reference
    }

    fn model_charge(&self, _: &stateknot_core::ModelInvocation) -> AgentInvocationCharge {
        AgentInvocationCharge::Unpriced
    }

    fn tool_charge(&self, _: &stateknot_core::ToolInvocation) -> AgentInvocationCharge {
        AgentInvocationCharge::Unpriced
    }
}

fn metadata(
    owner: &PrincipalIdentity,
    name: &str,
    kind: CapabilityKind,
    description: &str,
) -> Result<CapabilityMetadata, Box<dyn Error>> {
    Ok(CapabilityMetadata::new(
        CapabilityIdentity::new(
            owner.clone(),
            CapabilityReference::new(CapabilityName::new(name)?, VERSION),
        ),
        kind,
        None,
        CapabilityDescription::new(description)?,
        CapabilityLifecycle::active(),
        ScopeSet::empty(),
        Extensions::default(),
    )?)
}

fn schema_resource(id: &str, document: Value) -> Result<(SchemaReference, Value), Box<dyn Error>> {
    let canonical = serde_json_canonicalizer::to_vec(&document)?;
    Ok((
        SchemaReference::new(id.parse::<SchemaId>()?, VERSION, Digest::sha256(canonical)),
        document,
    ))
}

struct ExampleResources {
    schema_profile: SchemaReference,
    schema_documents: Vec<(SchemaReference, Value)>,
    tool: ToolDescriptor,
}

fn example_resources(owner: &PrincipalIdentity) -> Result<ExampleResources, Box<dyn Error>> {
    let profile_id = "https://schemas.example.com/providers/json-schema-profile/1.0.0";
    let (schema_profile, profile_document) = schema_resource(
        profile_id,
        json!({
            "$schema": DIALECT,
            "$id": profile_id,
            "type": "object"
        }),
    )?;
    let tool_input_id = "https://schemas.example.com/tools/evidence/input/1.0.0";
    let (tool_input, tool_input_document) = schema_resource(
        tool_input_id,
        json!({
            "$schema": DIALECT,
            "$id": tool_input_id,
            "type": "object",
            "additionalProperties": false,
            "required": ["incident_id"],
            "properties": { "incident_id": { "type": "string", "minLength": 1 } }
        }),
    )?;
    let tool_output_id = "https://schemas.example.com/tools/evidence/output/1.0.0";
    let (tool_output, tool_output_document) = schema_resource(
        tool_output_id,
        json!({
            "$schema": DIALECT,
            "$id": tool_output_id,
            "type": "object",
            "additionalProperties": false,
            "required": ["matches"],
            "properties": { "matches": { "type": "integer", "minimum": 0 } }
        }),
    )?;

    let tool = ToolDescriptor::new(
        metadata(
            owner,
            "tools.evidence.lookup",
            CapabilityKind::Tool,
            "Read-only incident evidence lookup",
        )?,
        tool_input.clone(),
        tool_output.clone(),
        ToolExecutionSemantics::new(
            ToolRisk::ReadOnly,
            ToolIdempotency::NotApplicable,
            false,
            false,
        )?,
        ToolResourceRequirements::none(),
        ToolInvocationCapabilities::new(ToolCancellationSupport::Cooperative, ExecutionCount::ZERO),
        ToolExecutionLimits::new(
            DurationMillis::new(5_000)?,
            ExecutionCount::new(16),
            ByteCount::new(16 * 1024),
            ByteCount::new(64 * 1024),
            ExecutionCount::ZERO,
            ByteCount::ZERO,
        )?,
    )?;
    Ok(ExampleResources {
        schema_profile: schema_profile.clone(),
        schema_documents: vec![
            (schema_profile, profile_document),
            (tool_input, tool_input_document),
            (tool_output, tool_output_document),
        ],
        tool,
    })
}

fn agent_definition(
    owner: &PrincipalIdentity,
    schema_profile: &SchemaReference,
    tool: &ToolDescriptor,
) -> Result<TypedAgentDefinition<IncidentRequest, IncidentReport>, Box<dyn Error>> {
    let text = ModelModalities::try_new([ModelModality::Text])?;
    let model = ModelDescriptor::new(
        metadata(
            owner,
            "models.primary",
            CapabilityKind::Model,
            "Pinned text model binding",
        )?,
        ModelCapabilities::new(
            text.clone(),
            text,
            true,
            ModelToolCapabilities::new(
                Some(schema_profile.clone()),
                ExecutionCount::new(1),
                ExecutionCount::new(1),
                ModelToolChoices::try_new([ModelToolChoice::Auto, ModelToolChoice::None])?,
                true,
            )?,
            ModelStructuredOutputCapabilities::json_schema(schema_profile.clone()),
            false,
            ModelTokenLimits::unknown(),
        )?,
    )?;
    let instruction_metadata = ContentMetadata::new(
        ContentSource::Application,
        ContentTrust::ApplicationControlled,
        SecurityLabel::new("internal/config")?,
        RedactionState::NotApplied,
    );
    let instructions = AgentInstructions::try_new([Instruction::new(
        InstructionIdentity::new(InstructionName::new("incident.base")?, VERSION),
        InstructionContent::from(TextContent::new(
            "Use the evidence tool when needed, then return a schema-valid incident report.",
            None,
            instruction_metadata,
        )?),
        InstructionProvenance::new(owner.clone()),
    )?])?;
    let execution = AgentExecutionConfig::new(
        AgentStructuredOutputStrategy::ModelNative,
        ExecutionCount::new(3),
        ExecutionCount::ZERO,
        ExecutionCount::new(1),
        AgentToolConcurrency::sequential(),
    )?;
    Ok(AgentBuilder::<IncidentRequest, IncidentReport>::new(
        metadata(
            owner,
            "agents.incident",
            CapabilityKind::Agent,
            "Durable provider-native incident agent",
        )?,
        "https://schemas.example.com/agents/incident/input/1.0.0".parse()?,
        "https://schemas.example.com/agents/incident/output/1.0.0".parse()?,
        model,
        instructions,
        execution,
    )
    .with_tools(AgentTools::try_new([tool.clone()])?)
    .build()?)
}

fn provider_native_graph(
    owner: &PrincipalIdentity,
    typed_definition: &TypedAgentDefinition<IncidentRequest, IncidentReport>,
    tool: &ToolDescriptor,
) -> Result<ProviderNativeAgentGraph, Box<dyn Error>> {
    let policy = Arc::new(RegisteredReadOnlyPolicy {
        reference: AgentToolPolicyReference::new(
            CapabilityIdentity::new(
                owner.clone(),
                CapabilityReference::new(CapabilityName::new("policies.read-only")?, VERSION),
            ),
            Digest::sha256(b"stateknot.example.read-only-policy.v1"),
        ),
        allowed_tool: tool.metadata().identity().clone(),
    });
    let accounting = Arc::new(UnpricedAccounting {
        reference: AgentInvocationAccountingReference::new(
            CapabilityIdentity::new(
                owner.clone(),
                CapabilityReference::new(CapabilityName::new("accounting.unpriced")?, VERSION),
            ),
            Digest::sha256(b"stateknot.example.unpriced-accounting.v1"),
        ),
    });
    Ok(ProviderNativeAgentGraph::compile(
        typed_definition.descriptor().clone(),
        CapabilityIdentity::new(
            owner.clone(),
            CapabilityReference::new(CapabilityName::new("graphs.provider-native")?, VERSION),
        ),
        CapabilityIdentity::new(
            owner.clone(),
            CapabilityReference::new(CapabilityName::new("reducers.provider-native")?, VERSION),
        ),
        "https://schemas.example.com/agents/incident/state/1.0.0".parse()?,
        SecurityLabel::new("tenant/user-input")?,
        policy,
        accounting,
    )?)
}

fn main() -> Result<(), Box<dyn Error>> {
    let owner = PrincipalIdentity::new(
        "https://issuer.example.com/stateknot".parse::<IssuerId>()?,
        "incident-service".parse::<SubjectId>()?,
    );
    let resources = example_resources(&owner)?;
    let typed_definition = agent_definition(&owner, &resources.schema_profile, &resources.tool)?;
    let graph = provider_native_graph(&owner, &typed_definition, &resources.tool)?;

    let mut schemas = JsonSchemaRegistryBuilder::default();
    for (reference, document) in resources.schema_documents {
        schemas.register(reference, document)?;
    }
    let mut schemas = typed_definition.register_schemas(schemas)?;
    graph.register_schema(&mut schemas)?;
    register_standard_graph_driver_event_schema(&mut schemas)?;
    register_standard_graph_lifecycle_event_schema(&mut schemas)?;
    register_standard_agent_cancellation_event_schema(&mut schemas)?;
    register_standard_agent_admission_event_schema(&mut schemas)?;
    register_standard_invocation_execution_event_schema(&mut schemas)?;
    let schemas = schemas.build()?;
    let typed_agent = typed_definition.bind(Arc::new(schemas))?;
    let initial_state = graph.initial_state()?;

    println!("agent={:?}", typed_agent.descriptor().metadata().identity());
    println!("graph={}", graph.graph().reference().definition_digest());
    println!("composition={}", graph.contract_digest());
    println!("initial_state={}", initial_state.digest());
    println!("tools={}", typed_agent.descriptor().tools().len());
    println!("external_io=none");
    Ok(())
}
