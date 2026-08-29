// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for model descriptors.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::ModelDescriptor;

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-model-descriptor/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    descriptors: WireFixtures,
    raw_invalid: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-model-descriptor-v1.json"))
        .expect("canonical model descriptor fixture must be valid JSON")
}

#[test]
fn canonical_model_descriptor_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.descriptors.valid {
        let decoded = serde_json::from_value::<ModelDescriptor>(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
    }

    for invalid in fixture.descriptors.invalid {
        assert!(
            serde_json::from_value::<ModelDescriptor>(invalid.clone()).is_err(),
            "ModelDescriptor accepted {invalid}"
        );
    }

    for invalid in fixture.raw_invalid {
        assert!(
            serde_json::from_str::<ModelDescriptor>(&invalid).is_err(),
            "ModelDescriptor accepted raw wire {invalid}"
        );
    }
}
