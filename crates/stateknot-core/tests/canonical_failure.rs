// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for the common failure wire contract.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{
    Failure, FailureCategory, FailureCode, FailureDetails, FailureMessage, FailureOrigin,
    RetryAdvice,
};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-failure/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    codes: TextFixtures,
    origins: TextFixtures,
    messages: TextFixtures,
    categories: TextFixtures,
    retry_advice: ObjectFixtures,
    details: ObjectFixtures,
    failures: ObjectFixtures,
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
    serde_json::from_str(include_str!("fixtures/core-failure-v1.json"))
        .expect("canonical failure fixture must be valid JSON")
}

#[test]
fn canonical_failure_identifiers_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_text_fixture::<FailureCode>(fixture.codes, "FailureCode");
    assert_text_fixture::<FailureOrigin>(fixture.origins, "FailureOrigin");
}

#[test]
fn canonical_failure_messages_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_text_fixture::<FailureMessage>(fixture.messages, "FailureMessage");
}

#[test]
fn canonical_failure_categories_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_text_fixture::<FailureCategory>(fixture.categories, "FailureCategory");
}

#[test]
fn canonical_retry_advice_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_object_fixture::<RetryAdvice>(fixture.retry_advice, "RetryAdvice");
}

#[test]
fn canonical_failure_details_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_object_fixture::<FailureDetails>(fixture.details, "FailureDetails");
}

#[test]
fn canonical_failures_match_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_object_fixture::<Failure>(fixture.failures, "Failure");
}

#[test]
fn duplicate_wire_members_are_rejected_at_every_nested_boundary() {
    assert!(serde_json::from_str::<RetryAdvice>(r#"{"kind":"never","kind":"never"}"#).is_err());
    assert!(
        serde_json::from_str::<Failure>(
            r#"{
            "id":"01890f3e-7b2a-7cc1-98f1-1234567890ab",
            "id":"01890f3e-7b2a-7cc1-98f1-1234567890ab",
            "category":"internal",
            "code":"runtime.internal",
            "origin":"stateknot.runtime",
            "message":"The operation could not be completed.",
            "retry_advice":{"kind":"never"}
        }"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<FailureDetails>(
            r#"{
            "schema":{
                "id":"https://stateknot.github.io/schema/failure-details/1.0.0",
                "version":"1.0.0",
                "digest":"sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            },
            "value":{"field":"first","field":"second"}
        }"#
        )
        .is_err()
    );
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
