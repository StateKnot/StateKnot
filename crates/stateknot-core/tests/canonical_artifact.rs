// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for artifact and content-part wire values.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{
    ArtifactDescription, ArtifactName, ArtifactParents, ArtifactRef, ArtifactRepresentation,
    ContentPart, MediaType, RetentionClass,
};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-artifact/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    media_types: CanonicalTextFixtures,
    artifact_names: TextFixtures,
    artifact_descriptions: TextFixtures,
    retention_classes: TextFixtures,
    representations: ObjectFixtures,
    parents: CanonicalObjectFixtures,
    artifact_refs: ObjectFixtures,
    content_parts: ObjectFixtures,
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalObjectFixtures {
    valid: Vec<CanonicalObject>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CanonicalObject {
    input: Value,
    canonical: Value,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-artifact-v1.json"))
        .expect("canonical artifact fixture must be valid JSON")
}

#[test]
fn canonical_media_type_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.media_types.valid {
        let media_type = expected.input.parse::<MediaType>().unwrap();
        assert_eq!(media_type.as_str(), expected.canonical);
        assert_eq!(
            serde_json::to_value(media_type).unwrap(),
            Value::from(expected.canonical)
        );
    }

    for invalid in fixture.media_types.invalid {
        assert!(
            serde_json::from_value::<MediaType>(invalid.clone()).is_err(),
            "MediaType accepted {invalid}"
        );
    }
}

#[test]
fn canonical_artifact_text_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_text_fixture::<ArtifactName>(fixture.artifact_names, "ArtifactName");
    assert_text_fixture::<ArtifactDescription>(
        fixture.artifact_descriptions,
        "ArtifactDescription",
    );
    assert_text_fixture::<RetentionClass>(fixture.retention_classes, "RetentionClass");
}

#[test]
fn canonical_representation_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_object_fixture::<ArtifactRepresentation>(fixture.representations, "representation");
}

#[test]
fn canonical_parents_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.parents.valid {
        let decoded = serde_json::from_value::<ArtifactParents>(expected.input).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), expected.canonical);
    }

    for invalid in fixture.parents.invalid {
        assert!(
            serde_json::from_value::<ArtifactParents>(invalid.clone()).is_err(),
            "ArtifactParents accepted {invalid}"
        );
    }
}

#[test]
fn canonical_artifact_reference_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_object_fixture::<ArtifactRef>(fixture.artifact_refs, "ArtifactRef");
}

#[test]
fn canonical_content_part_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_object_fixture::<ContentPart>(fixture.content_parts, "ContentPart");
}

fn assert_text_fixture<T>(fixture: TextFixtures, type_name: &str)
where
    T: for<'de> Deserialize<'de> + serde::Serialize,
{
    for expected in fixture.valid {
        let decoded = serde_json::from_value::<T>(Value::from(expected.clone())).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            Value::from(expected)
        );
    }

    for invalid in fixture.invalid {
        assert!(
            serde_json::from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
}

fn assert_object_fixture<T>(fixture: ObjectFixtures, type_name: &str)
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
