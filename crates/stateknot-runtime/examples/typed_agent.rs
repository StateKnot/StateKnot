// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Builds and freezes a typed agent contract without dispatching external work.

use std::{error::Error, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use stateknot_core::{
    AgentExecutionConfig, AgentInstructions, AgentStructuredOutputStrategy, AgentToolConcurrency,
    BudgetLimits, CapabilityDescription, CapabilityIdentity, CapabilityKind, CapabilityLifecycle,
    CapabilityMetadata, CapabilityName, CapabilityReference, ContentMetadata, ContentSource,
    ContentTrust, Digest, ExecutionCount, Extensions, Instruction, InstructionContent,
    InstructionIdentity, InstructionName, InstructionProvenance, IssuerId, ModelCapabilities,
    ModelDescriptor, ModelModalities, ModelModality, ModelStructuredOutputCapabilities,
    ModelTokenLimits, ModelToolCapabilities, PrincipalIdentity, RedactionState, SchemaId,
    SchemaReference, ScopeSet, SecurityLabel, SubjectId, TextContent, Version,
};
use stateknot_runtime::{AgentBuilder, JsonSchemaRegistryBuilder};

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
    severity: String,
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

fn main() -> Result<(), Box<dyn Error>> {
    let owner = PrincipalIdentity::new(
        "https://issuer.example.com/stateknot".parse::<IssuerId>()?,
        "incident-service".parse::<SubjectId>()?,
    );

    // Provider profiles are trusted, digest-pinned JSON Schemas. This compact
    // tutorial profile accepts object-shaped JSON Schema documents; production
    // profiles should encode the exact subset accepted by the provider binding.
    let profile_id = "https://schemas.example.com/providers/json-schema-profile/1.0.0";
    let (profile, profile_document) = schema_resource(
        profile_id,
        json!({
            "$schema": DIALECT,
            "$id": profile_id,
            "type": "object"
        }),
    )?;

    let text = ModelModalities::try_new([ModelModality::Text])?;
    let model = ModelDescriptor::new(
        metadata(
            &owner,
            "models.primary",
            CapabilityKind::Model,
            "Pinned text model binding",
        )?,
        ModelCapabilities::new(
            text.clone(),
            text,
            true,
            ModelToolCapabilities::unsupported(),
            ModelStructuredOutputCapabilities::json_schema(profile.clone()),
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
            "Return a concise incident report that follows the output schema.",
            None,
            instruction_metadata,
        )?),
        InstructionProvenance::new(owner.clone()),
    )?])?;

    let execution = AgentExecutionConfig::new(
        AgentStructuredOutputStrategy::ModelNative,
        ExecutionCount::new(4),
        ExecutionCount::new(1),
        ExecutionCount::ZERO,
        AgentToolConcurrency::sequential(),
    )?;
    let definition = AgentBuilder::<IncidentRequest, IncidentReport>::new(
        metadata(
            &owner,
            "agents.incident",
            CapabilityKind::Agent,
            "Typed incident-report agent",
        )?,
        "https://schemas.example.com/agents/incident/input/1.0.0".parse()?,
        "https://schemas.example.com/agents/incident/output/1.0.0".parse()?,
        model,
        instructions,
        execution,
    )
    .build()?;

    let mut schemas = JsonSchemaRegistryBuilder::default();
    schemas.register(profile, profile_document)?;
    let schemas = definition.register_schemas(schemas)?.build()?;
    let agent = definition.bind(Arc::new(schemas))?;
    let request = agent.prepare_request(
        &IncidentRequest {
            incident_id: "INC-42".to_owned(),
            question: "Summarize the evidence".to_owned(),
        },
        BudgetLimits::empty(),
    )?;

    // Tenant/run/thread/invocation IDs are still assigned by durable admission.
    // The example intentionally stops before pretending to execute a run.
    println!("agent={:?}", agent.descriptor().metadata().identity());
    println!("input_schema={:?}", request.input_schema());
    println!("input_bytes={}", request.input().stats().compact_bytes());
    Ok(())
}
