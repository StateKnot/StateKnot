// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for capability and authorization names.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{CapabilityName, Scope, ScopeSet};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-authorization/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    capability_names: TextFixtures,
    scopes: TextFixtures,
    scope_sets: SetFixtures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TextFixtures {
    valid: Vec<String>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SetFixtures {
    valid: Vec<Vec<String>>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-authorization-v1.json"))
        .expect("canonical authorization fixture must be valid JSON")
}

#[test]
fn canonical_capability_name_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.capability_names.valid {
        let name = expected.parse::<CapabilityName>().unwrap();
        assert_eq!(name.as_str(), expected);
        assert_eq!(serde_json::to_value(name).unwrap(), Value::from(expected));
    }

    for invalid in fixture.capability_names.invalid {
        assert!(
            serde_json::from_value::<CapabilityName>(invalid.clone()).is_err(),
            "CapabilityName accepted {invalid}"
        );
    }
}

#[test]
fn canonical_scope_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.scopes.valid {
        let scope = expected.parse::<Scope>().unwrap();
        assert_eq!(scope.as_str(), expected);
        assert_eq!(serde_json::to_value(scope).unwrap(), Value::from(expected));
    }

    for invalid in fixture.scopes.invalid {
        assert!(
            serde_json::from_value::<Scope>(invalid.clone()).is_err(),
            "Scope accepted {invalid}"
        );
    }
}

#[test]
fn canonical_scope_set_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.scope_sets.valid {
        let encoded = serde_json::to_value(expected).unwrap();
        let set = serde_json::from_value::<ScopeSet>(encoded.clone()).unwrap();
        assert_eq!(serde_json::to_value(set).unwrap(), encoded);
    }

    for invalid in fixture.scope_sets.invalid {
        assert!(
            serde_json::from_value::<ScopeSet>(invalid.clone()).is_err(),
            "ScopeSet accepted {invalid}"
        );
    }
}
