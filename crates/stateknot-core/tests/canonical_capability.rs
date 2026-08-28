// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for common capability metadata.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{
    CapabilityDescription, CapabilityIdentity, CapabilityKind, CapabilityLifecycle,
    CapabilityMetadata, CapabilityTitle,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-capability/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    identities: WireFixtures,
    titles: WireFixtures,
    descriptions: WireFixtures,
    kinds: WireFixtures,
    lifecycles: WireFixtures,
    metadata: WireFixtures,
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
    lifecycles: Vec<String>,
    metadata: Vec<String>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-capability-v1.json"))
        .expect("canonical capability fixture must be valid JSON")
}

#[test]
fn canonical_capability_identity_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<CapabilityIdentity>(fixture.identities, "CapabilityIdentity");
}

#[test]
fn canonical_capability_text_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<CapabilityTitle>(fixture.titles, "CapabilityTitle");
    assert_wire_fixture::<CapabilityDescription>(fixture.descriptions, "CapabilityDescription");
}

#[test]
fn canonical_capability_kind_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<CapabilityKind>(fixture.kinds, "CapabilityKind");
}

#[test]
fn canonical_capability_lifecycle_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<CapabilityLifecycle>(fixture.lifecycles, "CapabilityLifecycle");

    for invalid in fixture.raw_invalid.lifecycles {
        assert!(
            serde_json::from_str::<CapabilityLifecycle>(&invalid).is_err(),
            "CapabilityLifecycle accepted raw wire {invalid}"
        );
    }
}

#[test]
fn canonical_capability_metadata_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixture::<CapabilityMetadata>(fixture.metadata, "CapabilityMetadata");

    for invalid in fixture.raw_invalid.metadata {
        assert!(
            serde_json::from_str::<CapabilityMetadata>(&invalid).is_err(),
            "CapabilityMetadata accepted raw wire {invalid}"
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
