// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for tool descriptor contracts.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{
    ToolCancellationSupport, ToolDescriptor, ToolExecutionLimits, ToolExecutionSemantics,
    ToolIdempotency, ToolInvocationCapabilities, ToolResourceAccess, ToolResourceRequirements,
    ToolRisk,
};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-tool/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    risks: WireFixtures,
    idempotency: WireFixtures,
    resource_access: WireFixtures,
    cancellation: WireFixtures,
    semantics: WireFixtures,
    resources: WireFixtures,
    invocation: WireFixtures,
    limits: WireFixtures,
    descriptors: WireFixtures,
    raw_invalid: RawInvalidFixtures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInvalidFixtures {
    semantics: Vec<String>,
    descriptors: Vec<String>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-tool-v1.json"))
        .expect("canonical tool fixture must be valid JSON")
}

#[test]
fn canonical_tool_enum_fixtures_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ToolRisk>(fixture.risks, "ToolRisk");
    assert_wire_fixture::<ToolIdempotency>(fixture.idempotency, "ToolIdempotency");
    assert_wire_fixture::<ToolResourceAccess>(fixture.resource_access, "ToolResourceAccess");
    assert_wire_fixture::<ToolCancellationSupport>(fixture.cancellation, "ToolCancellationSupport");
}

#[test]
fn canonical_tool_semantics_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ToolExecutionSemantics>(fixture.semantics, "ToolExecutionSemantics");
    for invalid in fixture.raw_invalid.semantics {
        assert!(
            serde_json::from_str::<ToolExecutionSemantics>(&invalid).is_err(),
            "ToolExecutionSemantics accepted raw wire {invalid}"
        );
    }
}

#[test]
fn canonical_tool_resource_and_invocation_fixtures_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ToolResourceRequirements>(fixture.resources, "ToolResourceRequirements");
    assert_wire_fixture::<ToolInvocationCapabilities>(
        fixture.invocation,
        "ToolInvocationCapabilities",
    );
}

#[test]
fn canonical_tool_limit_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ToolExecutionLimits>(fixture.limits, "ToolExecutionLimits");
}

#[test]
fn canonical_tool_descriptor_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ToolDescriptor>(fixture.descriptors, "ToolDescriptor");
    for invalid in fixture.raw_invalid.descriptors {
        assert!(
            serde_json::from_str::<ToolDescriptor>(&invalid).is_err(),
            "ToolDescriptor accepted raw wire {invalid}"
        );
    }
}

fn assert_wire_fixture<T>(fixture: WireFixtures, type_name: &str)
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
