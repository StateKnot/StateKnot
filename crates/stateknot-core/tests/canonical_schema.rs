// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for schema identities.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{SchemaId, SchemaReference};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-schema/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    ids: IdFixtures,
    references: ReferenceFixtures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdFixtures {
    valid: Vec<String>,
    invalid: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-schema-v1.json"))
        .expect("canonical schema fixture must be valid JSON")
}

#[test]
fn canonical_schema_id_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.ids.valid {
        let id = expected.parse::<SchemaId>().unwrap();
        assert_eq!(id.as_str(), expected);
        assert_eq!(serde_json::to_value(id).unwrap(), Value::from(expected));
    }

    for invalid in fixture.ids.invalid {
        assert!(
            invalid.parse::<SchemaId>().is_err(),
            "SchemaId accepted {invalid:?}"
        );
    }
}

#[test]
fn canonical_schema_reference_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.references.valid {
        let reference = serde_json::from_value::<SchemaReference>(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(reference).unwrap(), expected);
    }

    for invalid in fixture.references.invalid {
        assert!(
            serde_json::from_value::<SchemaReference>(invalid.clone()).is_err(),
            "SchemaReference accepted {invalid}"
        );
    }
}
