// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Builds a typed Tool adapter against an offline, digest-pinned registry.
//!
//! This example performs no tool call or external I/O.

mod support;

use std::{error::Error, io};

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use stateknot_core::{
    BoundedJson, CanonicalJson, SchemaReference, Tool, ToolAdapter, ToolContext, ToolError,
    ToolOutput, ToolSchemaRegistry, ToolSchemaRole, ToolSchemaValidationError,
};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncidentLookup {
    incident_id: String,
}

#[derive(Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
struct IncidentSummary {
    found: bool,
    severity: String,
}

struct IncidentLookupTool {
    descriptor: stateknot_core::ToolDescriptor,
}

impl Tool for IncidentLookupTool {
    type Input = IncidentLookup;
    type Output = IncidentSummary;

    fn descriptor(&self) -> &stateknot_core::ToolDescriptor {
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

struct OfflineRegistry {
    input_reference: SchemaReference,
    input_schema: Schema,
    output_reference: SchemaReference,
    output_schema: Schema,
}

impl ToolSchemaRegistry for OfflineRegistry {
    fn validate_type_schema(
        &self,
        reference: &SchemaReference,
        role: ToolSchemaRole,
        generated: &Schema,
    ) -> Result<(), ToolSchemaValidationError> {
        let matches = match role {
            ToolSchemaRole::Input => {
                reference == &self.input_reference && generated == &self.input_schema
            }
            ToolSchemaRole::Output => {
                reference == &self.output_reference && generated == &self.output_schema
            }
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            Err(schema_error(
                "generated Rust schema does not match the pinned contract",
            ))
        }
    }

    fn validate_instance(
        &self,
        reference: &SchemaReference,
        role: ToolSchemaRole,
        value: &BoundedJson,
    ) -> Result<(), ToolSchemaValidationError> {
        match role {
            ToolSchemaRole::Input if reference == &self.input_reference => {
                decode::<IncidentLookup>(value)
            }
            ToolSchemaRole::Output if reference == &self.output_reference => {
                decode::<IncidentSummary>(value)
            }
            ToolSchemaRole::Input | ToolSchemaRole::Output => Err(schema_error(
                "instance used an unregistered schema reference",
            )),
            _ => Err(schema_error("instance used an unsupported schema role")),
        }
    }
}

fn decode<T: DeserializeOwned>(value: &BoundedJson) -> Result<(), ToolSchemaValidationError> {
    serde_json::from_value::<T>(value.as_value().clone())
        .map(|_| ())
        .map_err(ToolSchemaValidationError::new)
}

fn schema_error(message: &'static str) -> ToolSchemaValidationError {
    ToolSchemaValidationError::new(io::Error::other(message))
}

fn schema_reference<T: JsonSchema>(
    name: &str,
) -> Result<(SchemaReference, Schema), Box<dyn Error>> {
    let generated = SchemaGenerator::default().into_root_schema_for::<T>();
    let bounded = BoundedJson::try_from_value(serde_json::to_value(&generated)?)?;
    let digest = CanonicalJson::new(&bounded)?.digest();
    Ok((support::schema_with_digest(name, digest), generated))
}

fn main() -> Result<(), Box<dyn Error>> {
    let (input_reference, input_schema) = schema_reference::<IncidentLookup>("incident-lookup")?;
    let (output_reference, output_schema) =
        schema_reference::<IncidentSummary>("incident-summary")?;
    let descriptor = support::tool_descriptor(input_reference.clone(), output_reference.clone())?;
    let registry = OfflineRegistry {
        input_reference,
        input_schema,
        output_reference,
        output_schema,
    };

    let adapter = ToolAdapter::new(IncidentLookupTool { descriptor }, registry)?;
    assert_eq!(
        adapter
            .tool()
            .descriptor()
            .metadata()
            .identity()
            .name()
            .as_str(),
        "tools.lookup-incident"
    );
    println!("typed Tool schemas matched their canonical digest pins");
    println!("adapter registered; no Tool attempt or external effect was created");
    Ok(())
}
