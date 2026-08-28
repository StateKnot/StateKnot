// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for instructions and messages.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{Instruction, InstructionName, Message};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-message/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    instruction_names: TextFixtures,
    instructions: ObjectFixtures,
    messages: ObjectFixtures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TextFixtures {
    valid: Vec<String>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-message-v1.json"))
        .expect("canonical message fixture must be valid JSON")
}

#[test]
fn canonical_instruction_name_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.instruction_names.valid {
        let name = expected.parse::<InstructionName>().unwrap();
        assert_eq!(name.as_str(), expected);
        assert_eq!(serde_json::to_value(name).unwrap(), Value::from(expected));
    }

    for invalid in fixture.instruction_names.invalid {
        assert!(
            serde_json::from_value::<InstructionName>(invalid.clone()).is_err(),
            "InstructionName accepted {invalid}"
        );
    }
}

#[test]
fn canonical_instruction_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_object_fixture::<Instruction>(fixture.instructions, "Instruction");
}

#[test]
fn canonical_message_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_object_fixture::<Message>(fixture.messages, "Message");
}

fn assert_object_fixture<T>(fixture: ObjectFixtures, type_name: &str)
where
    T: for<'de> Deserialize<'de> + serde::Serialize,
{
    for expected in fixture.valid {
        let decoded = serde_json::from_value::<T>(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
    }

    for invalid in fixture.invalid {
        assert!(
            serde_json::from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
}
