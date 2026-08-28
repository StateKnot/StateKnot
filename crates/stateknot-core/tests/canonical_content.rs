// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for validated content wire values.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{ContentMetadata, JsonContent, LanguageTag, SecurityLabel, TextContent};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-content/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    language_tags: CanonicalTextFixtures,
    security_labels: TextFixtures,
    metadata: ObjectFixtures,
    text_content: ObjectFixtures,
    json_content: ObjectFixtures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalTextFixtures {
    valid: Vec<CanonicalText>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalText {
    input: String,
    canonical: String,
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

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-content-v1.json"))
        .expect("canonical content fixture must be valid JSON")
}

#[test]
fn canonical_language_tag_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.language_tags.valid {
        let tag = expected.input.parse::<LanguageTag>().unwrap();
        assert_eq!(tag.as_str(), expected.canonical);
        assert_eq!(
            serde_json::to_value(tag).unwrap(),
            Value::from(expected.canonical)
        );
    }

    for invalid in fixture.language_tags.invalid {
        assert!(
            serde_json::from_value::<LanguageTag>(invalid.clone()).is_err(),
            "LanguageTag accepted {invalid}"
        );
    }
}

#[test]
fn canonical_security_label_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.security_labels.valid {
        let label = expected.parse::<SecurityLabel>().unwrap();
        assert_eq!(label.as_str(), expected);
        assert_eq!(serde_json::to_value(label).unwrap(), Value::from(expected));
    }

    for invalid in fixture.security_labels.invalid {
        assert!(
            serde_json::from_value::<SecurityLabel>(invalid.clone()).is_err(),
            "SecurityLabel accepted {invalid}"
        );
    }
}

#[test]
fn canonical_content_metadata_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_object_fixture::<ContentMetadata>(fixture.metadata);
}

#[test]
fn canonical_text_content_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_object_fixture::<TextContent>(fixture.text_content);
}

#[test]
fn canonical_json_content_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_object_fixture::<JsonContent>(fixture.json_content);
}

fn assert_object_fixture<T>(fixture: ObjectFixtures)
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
            "content type accepted {invalid}"
        );
    }
}
