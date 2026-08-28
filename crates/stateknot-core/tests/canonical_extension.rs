// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for the bounded extension contract.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{ExtensionKey, ExtensionValue, Extensions};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-extension/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    keys: TextFixtures,
    values: ObjectFixtures,
    maps: MapFixtures,
    raw_invalid: Vec<String>,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MapFixtures {
    canonical: Vec<CanonicalMap>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalMap {
    wire: String,
    entries: usize,
    compact_bytes: usize,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-extension-v1.json"))
        .expect("canonical extension fixture must be valid JSON")
}

#[test]
fn canonical_extension_keys_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.keys.valid {
        let decoded =
            serde_json::from_value::<ExtensionKey>(Value::from(expected.clone())).unwrap();
        assert_eq!(decoded.as_str(), expected);
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            Value::from(expected)
        );
    }

    for invalid in fixture.keys.invalid {
        assert!(
            serde_json::from_value::<ExtensionKey>(invalid.clone()).is_err(),
            "ExtensionKey accepted {invalid}"
        );
    }
}

#[test]
fn canonical_extension_values_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.values.valid {
        let decoded = serde_json::from_value::<ExtensionValue>(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
    }

    for invalid in fixture.values.invalid {
        assert!(
            serde_json::from_value::<ExtensionValue>(invalid.clone()).is_err(),
            "ExtensionValue accepted {invalid}"
        );
    }
}

#[test]
fn canonical_extension_maps_preserve_sorted_exact_wire_bytes() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.maps.canonical {
        let decoded = serde_json::from_str::<Extensions>(&expected.wire).unwrap();
        assert_eq!(decoded.len(), expected.entries);
        assert_eq!(decoded.compact_bytes(), expected.compact_bytes);
        assert_eq!(expected.wire.len(), expected.compact_bytes);
        assert_eq!(serde_json::to_string(&decoded).unwrap(), expected.wire);
    }
}

#[test]
fn canonical_extension_maps_reject_invalid_or_ambiguous_wire_data() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for invalid in fixture.maps.invalid {
        assert!(
            serde_json::from_value::<Extensions>(invalid.clone()).is_err(),
            "Extensions accepted {invalid}"
        );
    }

    for invalid in fixture.raw_invalid {
        assert!(
            serde_json::from_str::<Extensions>(&invalid).is_err(),
            "Extensions accepted raw wire {invalid}"
        );
    }
}
