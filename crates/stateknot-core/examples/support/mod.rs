// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared, offline constructors for the public core contract examples.
//!
//! Every example is compiled as a separate binary, so each one uses only a
//! subset of these helpers.

#![allow(dead_code)]

use std::error::Error;

use stateknot_core::{
    AgentDescriptor, AgentExecutionConfig, AgentInstructions, AgentStructuredOutputStrategy,
    AgentToolConcurrency, AgentTools, BudgetLimits, ByteCount, CapabilityDescription,
    CapabilityIdentity, CapabilityKind, CapabilityLifecycle, CapabilityMetadata, CapabilityName,
    CapabilityReference, ContentMetadata, ContentSource, ContentTrust, CostLimits, Digest,
    DurationMillis, ExecutionCount, Extensions, Instruction, InstructionIdentity, InstructionName,
    InstructionProvenance, IssuerId, ModelCapabilities, ModelDescriptor, ModelModalities,
    ModelModality, ModelStructuredOutputCapabilities, ModelTokenLimits, ModelToolCapabilities,
    PrincipalIdentity, RedactionState, SchemaId, SchemaReference, ScopeSet, SecurityLabel,
    SubjectId, TextContent, TokenCount, ToolCancellationSupport, ToolDescriptor,
    ToolExecutionLimits, ToolExecutionSemantics, ToolIdempotency, ToolInvocationCapabilities,
    ToolResourceRequirements, ToolRisk, Version,
};

pub(crate) fn principal(subject: &str) -> Result<PrincipalIdentity, Box<dyn Error>> {
    Ok(PrincipalIdentity::new(
        "https://identity.example.com/stateknot".parse::<IssuerId>()?,
        subject.parse::<SubjectId>()?,
    ))
}

pub(crate) fn metadata(
    kind: CapabilityKind,
    name: &str,
    owner: &str,
    description: &str,
) -> Result<CapabilityMetadata, Box<dyn Error>> {
    Ok(CapabilityMetadata::new(
        CapabilityIdentity::new(
            principal(owner)?,
            CapabilityReference::new(name.parse::<CapabilityName>()?, Version::new(1, 0, 0)),
        ),
        kind,
        None,
        CapabilityDescription::new(description)?,
        CapabilityLifecycle::active(),
        ScopeSet::empty(),
        Extensions::default(),
    )?)
}

pub(crate) fn schema(name: &str) -> SchemaReference {
    schema_with_digest(name, Digest::sha256(name))
}

pub(crate) fn schema_with_digest(name: &str, digest: Digest) -> SchemaReference {
    SchemaReference::new(
        format!("https://schemas.example.com/stateknot/{name}/1.0.0")
            .parse::<SchemaId>()
            .expect("the controlled schema URI is valid"),
        Version::new(1, 0, 0),
        digest,
    )
}

pub(crate) fn instruction(name: &str, content: &str) -> Result<Instruction, Box<dyn Error>> {
    let metadata = ContentMetadata::new(
        ContentSource::Application,
        ContentTrust::ApplicationControlled,
        "internal".parse::<SecurityLabel>()?,
        RedactionState::NotApplied,
    );
    Ok(Instruction::new(
        InstructionIdentity::new(name.parse::<InstructionName>()?, Version::new(1, 0, 0)),
        TextContent::new(content, Some("en".parse()?), metadata)?.into(),
        InstructionProvenance::new(principal("instruction-owner")?),
    )?)
}

pub(crate) fn model_descriptor() -> Result<ModelDescriptor, Box<dyn Error>> {
    Ok(ModelDescriptor::new(
        metadata(
            CapabilityKind::Model,
            "models.example-streaming",
            "model-owner",
            "Provider-neutral streaming model binding",
        )?,
        ModelCapabilities::new(
            ModelModalities::try_new([ModelModality::Text])?,
            ModelModalities::try_new([ModelModality::Text])?,
            true,
            ModelToolCapabilities::unsupported(),
            ModelStructuredOutputCapabilities::json_schema(schema("model-output-profile")),
            false,
            ModelTokenLimits::new(
                Some(TokenCount::new(32_768)),
                Some(TokenCount::new(30_720)),
                Some(TokenCount::new(2_048)),
            )?,
        )?,
    )?)
}

pub(crate) fn tool_descriptor(
    input_schema: SchemaReference,
    output_schema: SchemaReference,
) -> Result<ToolDescriptor, Box<dyn Error>> {
    Ok(ToolDescriptor::new(
        metadata(
            CapabilityKind::Tool,
            "tools.lookup-incident",
            "tool-owner",
            "Read-only incident lookup",
        )?,
        input_schema,
        output_schema,
        ToolExecutionSemantics::new(
            ToolRisk::ReadOnly,
            ToolIdempotency::NotApplicable,
            false,
            false,
        )?,
        ToolResourceRequirements::none(),
        ToolInvocationCapabilities::new(
            ToolCancellationSupport::Cooperative,
            ExecutionCount::new(8),
        ),
        ToolExecutionLimits::new(
            DurationMillis::new(10_000)?,
            ExecutionCount::new(4),
            ByteCount::new(16 * 1024),
            ByteCount::new(64 * 1024),
            ExecutionCount::new(2),
            ByteCount::new(1024 * 1024),
        )?,
    )?)
}

pub(crate) fn agent_descriptor() -> Result<AgentDescriptor, Box<dyn Error>> {
    Ok(AgentDescriptor::new(
        metadata(
            CapabilityKind::Agent,
            "agents.incident-summary",
            "agent-owner",
            "Summarizes one incident from bounded input",
        )?,
        schema("incident-summary-input"),
        schema("incident-summary-output"),
        model_descriptor()?,
        AgentInstructions::try_new([instruction(
            "incident-summary.base",
            "Summarize only the supplied incident evidence.",
        )?])?,
        AgentTools::empty(),
        AgentExecutionConfig::new(
            AgentStructuredOutputStrategy::ModelNative,
            ExecutionCount::new(4),
            ExecutionCount::new(1),
            ExecutionCount::ZERO,
            AgentToolConcurrency::sequential(),
        )?,
        BudgetLimits::empty()
            .with_model_turns(ExecutionCount::new(4))
            .with_output_tokens(TokenCount::new(1_024)),
    )?)
}

pub(crate) fn complete_budget() -> Result<BudgetLimits, Box<dyn Error>> {
    let count = ExecutionCount::new(64);
    let tokens = TokenCount::new(32_768);
    let bytes = ByteCount::new(64 * 1024 * 1024);
    Ok(BudgetLimits::empty()
        .with_deadline("2031-01-01T00:00:00.000000Z".parse()?)
        .with_graph_depth(ExecutionCount::new(8))
        .with_graph_steps(count)
        .with_model_attempts(count)
        .with_model_turns(count)
        .with_input_tokens(tokens)
        .with_cached_input_tokens(tokens)
        .with_reasoning_tokens(tokens)
        .with_output_tokens(tokens)
        .with_tool_calls(count)
        .with_write_calls(count)
        .with_remote_agent_delegations(count)
        .with_retries(count)
        .with_concurrent_branches(ExecutionCount::new(8))
        .with_fan_out(ExecutionCount::new(16))
        .with_input_bytes(bytes)
        .with_output_bytes(bytes)
        .with_event_bytes(bytes)
        .with_checkpoint_bytes(bytes)
        .with_artifact_bytes(bytes)
        .with_costs(CostLimits::try_new([])?))
}
