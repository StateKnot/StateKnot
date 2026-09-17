// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Constructs and validates the provider-neutral contract for a first Agent.
//!
//! This example performs no admission commit, provider I/O, or graph execution.

mod support;

use std::error::Error;

use serde_json::json;
use stateknot_core::{AgentRequest, BoundedJson, BudgetLimits, ByteCount, TokenCount};

fn main() -> Result<(), Box<dyn Error>> {
    let descriptor = support::agent_descriptor()?;
    let request = AgentRequest::new(
        descriptor.input_schema().clone(),
        BoundedJson::try_from_value(json!({
            "incident_id": "INC-42",
            "evidence": "Database latency exceeded the alert threshold."
        }))?,
        BudgetLimits::empty()
            .with_output_tokens(TokenCount::new(512))
            .with_output_bytes(ByteCount::new(16 * 1024)),
    );

    let observed_at = "2030-01-01T00:00:00.000000Z".parse()?;
    let budget = request.resolve_for(&descriptor, &[support::complete_budget()?], observed_at)?;

    assert_eq!(request.input_schema(), descriptor.input_schema());
    assert_eq!(budget.output_tokens(), TokenCount::new(512));
    assert_eq!(budget.output_bytes(), ByteCount::new(16 * 1024));
    println!("agent: {:?}", descriptor.metadata().identity());
    println!("request bytes: {}", request.input().stats().compact_bytes());
    println!("resolved output-token ceiling: {}", budget.output_tokens());
    println!("contract validated; no durable run or provider call was created");
    Ok(())
}
