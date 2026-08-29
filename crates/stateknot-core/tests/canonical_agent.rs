// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for immutable agent definitions.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{
    AgentDescriptor, AgentExecutionConfig, AgentInstructions, AgentStructuredOutputStrategy,
    AgentToolConcurrency, AgentTools,
};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-agent/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    structured_output_strategies: WireFixtures,
    tool_concurrencies: WireFixtures,
    execution_configs: WireFixtures,
    descriptors: WireFixtures,
    raw_invalid_execution_configs: Vec<String>,
    raw_invalid_descriptors: Vec<String>,
    descriptor_mutations: Vec<DescriptorMutation>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DescriptorMutation {
    name: String,
    changes: Vec<Mutation>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Mutation {
    Replace { pointer: String, value: Value },
    DuplicateFirst { pointer: String },
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-agent-v1.json"))
        .expect("canonical agent fixture must be valid JSON")
}

fn assert_wire_fixtures<T>(fixtures: WireFixtures, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    for expected in fixtures.valid {
        let decoded = serde_json::from_value::<T>(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
    }

    for invalid in fixtures.invalid {
        assert!(
            serde_json::from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
}

fn apply_mutation(target: &mut Value, mutation: Mutation) {
    match mutation {
        Mutation::Replace { pointer, value } => {
            *target
                .pointer_mut(&pointer)
                .unwrap_or_else(|| panic!("fixture pointer {pointer} must exist")) = value;
        }
        Mutation::DuplicateFirst { pointer } => {
            let values = target
                .pointer_mut(&pointer)
                .unwrap_or_else(|| panic!("fixture pointer {pointer} must exist"))
                .as_array_mut()
                .unwrap_or_else(|| panic!("fixture pointer {pointer} must name an array"));
            let first = values
                .first()
                .cloned()
                .unwrap_or_else(|| panic!("fixture array {pointer} must not be empty"));
            values.push(first);
        }
    }
}

#[test]
fn canonical_agent_fixture_matches_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixtures::<AgentStructuredOutputStrategy>(
        fixture.structured_output_strategies,
        "AgentStructuredOutputStrategy",
    );
    assert_wire_fixtures::<AgentToolConcurrency>(
        fixture.tool_concurrencies,
        "AgentToolConcurrency",
    );
    assert_wire_fixtures::<AgentExecutionConfig>(fixture.execution_configs, "AgentExecutionConfig");
    for invalid in fixture.raw_invalid_execution_configs {
        assert!(
            serde_json::from_str::<AgentExecutionConfig>(&invalid).is_err(),
            "AgentExecutionConfig accepted raw wire {invalid}"
        );
    }

    let canonical_descriptor = fixture
        .descriptors
        .valid
        .first()
        .expect("fixture must contain a canonical descriptor")
        .clone();
    let instructions = canonical_descriptor["instructions"].clone();
    let decoded_instructions = serde_json::from_value::<AgentInstructions>(instructions.clone())
        .expect("canonical descriptor instructions must decode independently");
    assert_eq!(
        serde_json::to_value(decoded_instructions).unwrap(),
        instructions
    );
    let tools = canonical_descriptor["tools"].clone();
    let decoded_tools = serde_json::from_value::<AgentTools>(tools.clone())
        .expect("canonical descriptor tools must decode independently");
    assert_eq!(serde_json::to_value(decoded_tools).unwrap(), tools);

    assert_wire_fixtures::<AgentDescriptor>(fixture.descriptors, "AgentDescriptor");
    for invalid in fixture.raw_invalid_descriptors {
        assert!(
            serde_json::from_str::<AgentDescriptor>(&invalid).is_err(),
            "AgentDescriptor accepted raw wire {invalid}"
        );
    }
    for mutation in fixture.descriptor_mutations {
        let mut invalid = canonical_descriptor.clone();
        for change in mutation.changes {
            apply_mutation(&mut invalid, change);
        }
        assert!(
            serde_json::from_value::<AgentDescriptor>(invalid).is_err(),
            "AgentDescriptor accepted mutation {}",
            mutation.name
        );
    }
}
