// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for bounded JSON wire values.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{BoundedJson, BoundedJsonError};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-json/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    valid: Vec<ValidFixture>,
    invalid: Vec<InvalidFixture>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidFixture {
    input: String,
    compact: String,
    value: Value,
    stats: ExpectedStats,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedStats {
    compact_bytes: usize,
    max_depth: usize,
    nodes: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvalidFixture {
    input: String,
    kind: InvalidKind,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InvalidKind {
    DuplicateObjectKey,
    InvalidJson,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-json-v1.json"))
        .expect("canonical bounded JSON fixture must be valid JSON")
}

#[test]
fn bounded_json_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.valid {
        let bounded = BoundedJson::from_str(&expected.input).unwrap();
        assert_eq!(bounded.as_value(), &expected.value);
        assert_eq!(serde_json::to_string(&bounded).unwrap(), expected.compact);
        assert_eq!(
            bounded.stats().compact_bytes(),
            expected.stats.compact_bytes
        );
        assert_eq!(bounded.stats().max_depth(), expected.stats.max_depth);
        assert_eq!(bounded.stats().nodes(), expected.stats.nodes);

        let decoded: BoundedJson = serde_json::from_str(&expected.input).unwrap();
        assert_eq!(decoded, bounded);
    }
}

#[test]
fn bounded_json_fixture_rejects_ambiguous_or_invalid_wire_values() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for invalid in fixture.invalid {
        let error = BoundedJson::from_str(&invalid.input).unwrap_err();
        match invalid.kind {
            InvalidKind::DuplicateObjectKey => {
                assert_eq!(error, BoundedJsonError::DuplicateObjectKey);
            }
            InvalidKind::InvalidJson => {
                assert!(matches!(error, BoundedJsonError::InvalidJson { .. }));
            }
        }
    }
}
