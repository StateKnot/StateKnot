// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Registers one strongly typed local Rust tool in the production registries.
//!
//! This startup-only example performs no tool call or external I/O. Durable
//! execution is owned by `DurableInvocationExecutor`, which resolves the exact
//! frozen descriptor after it has committed an attempt start.

use std::{error::Error, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stateknot_core::{
    AgentTools, ByteCount, CapabilityDescription, CapabilityIdentity, CapabilityKind,
    CapabilityLifecycle, CapabilityMetadata, CapabilityName, CapabilityReference, DurationMillis,
    ExecutionCount, Extensions, IssuerId, PrincipalIdentity, SchemaId, ScopeSet, SubjectId, Tool,
    ToolAdapter, ToolCancellationSupport, ToolContext, ToolDescriptor, ToolError,
    ToolExecutionLimits, ToolExecutionSemantics, ToolIdempotency, ToolInvocationCapabilities,
    ToolOutput, ToolResourceRequirements, ToolRisk, Version,
};
use stateknot_runtime::{JsonSchemaRegistryBuilder, ToolProviderRegistryBuilder};

const VERSION: Version = Version::new(1, 0, 0);

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncidentLookup {
    incident_id: String,
}

#[derive(Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct IncidentSummary {
    found: bool,
    severity: String,
}

struct IncidentLookupTool {
    descriptor: ToolDescriptor,
}

impl Tool for IncidentLookupTool {
    type Input = IncidentLookup;
    type Output = IncidentSummary;

    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn call(
        &self,
        _context: ToolContext,
        input: Self::Input,
    ) -> stateknot_core::BoxFuture<'_, Result<ToolOutput<Self::Output>, ToolError>> {
        Box::pin(async move {
            Ok(ToolOutput::inline(IncidentSummary {
                found: input.incident_id == "INC-42",
                severity: "high".to_owned(),
            }))
        })
    }
}

fn owner() -> Result<PrincipalIdentity, Box<dyn Error>> {
    Ok(PrincipalIdentity::new(
        "https://identity.example.com/stateknot".parse::<IssuerId>()?,
        "incident-service".parse::<SubjectId>()?,
    ))
}

fn descriptor(
    input: stateknot_core::SchemaReference,
    output: stateknot_core::SchemaReference,
) -> Result<ToolDescriptor, Box<dyn Error>> {
    let identity = CapabilityIdentity::new(
        owner()?,
        CapabilityReference::new("tools.lookup-incident".parse::<CapabilityName>()?, VERSION),
    );
    let metadata = CapabilityMetadata::new(
        identity,
        CapabilityKind::Tool,
        None,
        CapabilityDescription::new("Read-only incident lookup")?,
        CapabilityLifecycle::active(),
        ScopeSet::empty(),
        Extensions::default(),
    )?;
    Ok(ToolDescriptor::new(
        metadata,
        input,
        output,
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

fn main() -> Result<(), Box<dyn Error>> {
    let mut schema_builder = JsonSchemaRegistryBuilder::with_default_limits();
    let input = schema_builder.register_rust_type::<IncidentLookup>(
        "https://schemas.example.com/tools/lookup-incident/input/1.0.0".parse::<SchemaId>()?,
        VERSION,
    )?;
    let output = schema_builder.register_rust_type::<IncidentSummary>(
        "https://schemas.example.com/tools/lookup-incident/output/1.0.0".parse::<SchemaId>()?,
        VERSION,
    )?;
    let schemas = schema_builder.build()?;

    let descriptor = descriptor(input, output)?;
    let adapter = ToolAdapter::new(
        IncidentLookupTool {
            descriptor: descriptor.clone(),
        },
        schemas,
    )?;
    let mut providers = ToolProviderRegistryBuilder::new();
    providers.register(Arc::new(adapter))?;
    let providers = providers.build();

    let exposed_tools = AgentTools::try_new([descriptor.clone()])?;
    let resolved = providers.resolve(&descriptor)?;
    assert_eq!(resolved.descriptor(), &descriptor);
    assert_eq!(exposed_tools.len(), 1);
    println!("registered one exact local tool revision in an immutable startup snapshot");
    println!("durable execution remains owned by DurableInvocationExecutor");
    Ok(())
}
