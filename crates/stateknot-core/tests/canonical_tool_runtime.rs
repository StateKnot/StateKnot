// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for the callable tool boundary.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{
    ToolError, ToolErrorPhase, ToolErrorProvenance, ToolExternalEffect, ToolInput,
    ToolProgressEvent, ToolProgressProvenance, ToolProgressUpdate, ToolResult,
    ToolResultProvenance,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-tool-runtime/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    phases: WireFixtures,
    external_effects: WireFixtures,
    progress_updates: WireFixtures,
    progress_provenances: WireFixtures,
    progress_events: WireFixtures,
    inputs: WireFixtures,
    result_provenances: WireFixtures,
    results: WireFixtures,
    error_provenances: WireFixtures,
    errors: WireFixtures,
    raw_invalid_errors: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-tool-runtime-v1.json"))
        .expect("canonical tool runtime fixture must be valid JSON")
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

#[test]
fn canonical_tool_runtime_fixture_matches_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixtures::<ToolErrorPhase>(fixture.phases, "ToolErrorPhase");
    assert_wire_fixtures::<ToolExternalEffect>(fixture.external_effects, "ToolExternalEffect");
    assert_wire_fixtures::<ToolProgressUpdate>(fixture.progress_updates, "ToolProgressUpdate");
    assert_wire_fixtures::<ToolProgressProvenance>(
        fixture.progress_provenances,
        "ToolProgressProvenance",
    );
    assert_wire_fixtures::<ToolProgressEvent>(fixture.progress_events, "ToolProgressEvent");
    assert_wire_fixtures::<ToolInput>(fixture.inputs, "ToolInput");
    assert_wire_fixtures::<ToolResultProvenance>(
        fixture.result_provenances,
        "ToolResultProvenance",
    );
    assert_wire_fixtures::<ToolResult>(fixture.results, "ToolResult");
    assert_wire_fixtures::<ToolErrorProvenance>(fixture.error_provenances, "ToolErrorProvenance");
    assert_wire_fixtures::<ToolError>(fixture.errors, "ToolError");

    for invalid in fixture.raw_invalid_errors {
        assert!(
            serde_json::from_str::<ToolError>(&invalid).is_err(),
            "ToolError accepted raw wire {invalid}"
        );
    }
}
