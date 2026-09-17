// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Maps a small external wire request into the trusted core contract.
//!
//! Protocol adapters must assign trusted schema identity themselves and reject
//! unknown authority fields instead of copying them into the core request.

mod support;

use std::{error::Error, io};

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{AgentRequest, BoundedJson, BudgetLimits, ByteCount, SchemaReference};

const MAX_REQUEST_OUTPUT_BYTES: u64 = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRunRequest {
    input: Value,
    max_output_bytes: u64,
}

fn map_request(
    wire: WireRunRequest,
    trusted_input_schema: &SchemaReference,
) -> Result<AgentRequest, Box<dyn Error>> {
    if !(1..=MAX_REQUEST_OUTPUT_BYTES).contains(&wire.max_output_bytes) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "max_output_bytes must be within 1..=1048576",
        )
        .into());
    }
    Ok(AgentRequest::new(
        trusted_input_schema.clone(),
        BoundedJson::try_from_value(wire.input)?,
        BudgetLimits::empty().with_output_bytes(ByteCount::new(wire.max_output_bytes)),
    ))
}

fn main() -> Result<(), Box<dyn Error>> {
    let descriptor = support::agent_descriptor()?;
    let wire: WireRunRequest =
        serde_json::from_str(r#"{"input":{"incident_id":"INC-42"},"max_output_bytes":16384}"#)?;
    let request = map_request(wire, descriptor.input_schema())?;
    request.resolve_for(
        &descriptor,
        &[support::complete_budget()?],
        "2030-01-01T00:00:00.000000Z".parse()?,
    )?;

    // Tenant/run authority is not part of this wire type and unknown fields fail.
    let injected = r#"{"input":{},"max_output_bytes":1,"tenant_id":"victim"}"#;
    assert!(serde_json::from_str::<WireRunRequest>(injected).is_err());
    assert_eq!(request.input_schema(), descriptor.input_schema());
    println!("wire input mapped to a trusted schema-bound Agent request");
    println!("unknown authority fields rejected; no durable admission was created");
    Ok(())
}
