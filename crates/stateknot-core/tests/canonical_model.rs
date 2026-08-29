// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for model capability contracts.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{
    ModelCapabilities, ModelCapabilityMismatch, ModelModalities, ModelModality, ModelRequirements,
    ModelStructuredOutputCapabilities, ModelStructuredOutputLevel, ModelTokenLimits,
    ModelToolCapabilities, ModelToolChoice, ModelToolChoices, ModelToolRequirements,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-model-capability/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    modalities: WireFixtures,
    tool_choices: WireFixtures,
    structured_levels: WireFixtures,
    modality_sets: WireFixtures,
    choice_sets: WireFixtures,
    token_limits: WireFixtures,
    tool_capabilities: WireFixtures,
    structured_outputs: WireFixtures,
    tool_requirements: WireFixtures,
    requirements: WireFixtures,
    capabilities: WireFixtures,
    mismatches: WireFixtures,
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
    token_limits: Vec<String>,
    capabilities: Vec<String>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-model-capability-v1.json"))
        .expect("canonical model capability fixture must be valid JSON")
}

#[test]
fn canonical_model_enum_fixtures_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ModelModality>(fixture.modalities, "ModelModality");
    assert_wire_fixture::<ModelToolChoice>(fixture.tool_choices, "ModelToolChoice");
    assert_wire_fixture::<ModelStructuredOutputLevel>(
        fixture.structured_levels,
        "ModelStructuredOutputLevel",
    );
}

#[test]
fn canonical_model_set_fixtures_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ModelModalities>(fixture.modality_sets, "ModelModalities");
    assert_wire_fixture::<ModelToolChoices>(fixture.choice_sets, "ModelToolChoices");
}

#[test]
fn canonical_model_limit_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ModelTokenLimits>(fixture.token_limits, "ModelTokenLimits");
    for invalid in fixture.raw_invalid.token_limits {
        assert!(
            serde_json::from_str::<ModelTokenLimits>(&invalid).is_err(),
            "ModelTokenLimits accepted raw wire {invalid}"
        );
    }
}

#[test]
fn canonical_model_feature_fixtures_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ModelToolCapabilities>(
        fixture.tool_capabilities,
        "ModelToolCapabilities",
    );
    assert_wire_fixture::<ModelStructuredOutputCapabilities>(
        fixture.structured_outputs,
        "ModelStructuredOutputCapabilities",
    );
}

#[test]
fn canonical_model_requirement_fixtures_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ModelToolRequirements>(
        fixture.tool_requirements,
        "ModelToolRequirements",
    );
    assert_wire_fixture::<ModelRequirements>(fixture.requirements, "ModelRequirements");
}

#[test]
fn canonical_model_capability_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ModelCapabilities>(fixture.capabilities, "ModelCapabilities");
    for invalid in fixture.raw_invalid.capabilities {
        assert!(
            serde_json::from_str::<ModelCapabilities>(&invalid).is_err(),
            "ModelCapabilities accepted raw wire {invalid}"
        );
    }
}

#[test]
fn canonical_model_mismatch_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<ModelCapabilityMismatch>(fixture.mismatches, "ModelCapabilityMismatch");
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
