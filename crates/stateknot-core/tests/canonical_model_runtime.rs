// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for the callable model boundary.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{ModelError, ModelErrorPhase, ModelErrorProvenance, ModelProviderRequestId};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-model-runtime/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    provider_request_ids: ScalarFixtures,
    phases: WireFixtures,
    provenances: WireFixtures,
    errors: WireFixtures,
    raw_invalid_errors: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScalarFixtures {
    valid: Vec<String>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-model-runtime-v1.json"))
        .expect("canonical model runtime fixture must be valid JSON")
}

fn assert_scalar_fixtures<T>(fixtures: ScalarFixtures, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    for expected in fixtures.valid {
        let decoded = serde_json::from_value::<T>(Value::from(expected.clone())).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            Value::from(expected)
        );
    }

    for invalid in fixtures.invalid {
        assert!(
            serde_json::from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
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
fn canonical_model_runtime_fixture_matches_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_scalar_fixtures::<ModelProviderRequestId>(
        fixture.provider_request_ids,
        "ModelProviderRequestId",
    );
    assert_wire_fixtures::<ModelErrorPhase>(fixture.phases, "ModelErrorPhase");
    assert_wire_fixtures::<ModelErrorProvenance>(fixture.provenances, "ModelErrorProvenance");
    assert_wire_fixtures::<ModelError>(fixture.errors, "ModelError");

    for invalid in fixture.raw_invalid_errors {
        assert!(
            serde_json::from_str::<ModelError>(&invalid).is_err(),
            "ModelError accepted raw wire {invalid}"
        );
    }
}
