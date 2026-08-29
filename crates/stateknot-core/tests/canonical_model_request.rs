// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for provider-neutral model requests.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{
    ModelRequest, ModelRequestLimits, ModelResponseMode, ModelTextOutputFormat, ModelToolSelection,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-model-request/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    response_modes: WireFixtures,
    tool_selections: WireFixtures,
    text_output_formats: WireFixtures,
    limits: WireFixtures,
    requests: WireFixtures,
    raw_invalid_requests: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-model-request-v1.json"))
        .expect("canonical model request fixture must be valid JSON")
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
fn canonical_model_request_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_wire_fixtures::<ModelResponseMode>(fixture.response_modes, "ModelResponseMode");
    assert_wire_fixtures::<ModelToolSelection>(fixture.tool_selections, "ModelToolSelection");
    assert_wire_fixtures::<ModelTextOutputFormat>(
        fixture.text_output_formats,
        "ModelTextOutputFormat",
    );
    assert_wire_fixtures::<ModelRequestLimits>(fixture.limits, "ModelRequestLimits");
    assert_wire_fixtures::<ModelRequest>(fixture.requests, "ModelRequest");

    for invalid in fixture.raw_invalid_requests {
        assert!(
            serde_json::from_str::<ModelRequest>(&invalid).is_err(),
            "ModelRequest accepted raw wire {invalid}"
        );
    }
}
