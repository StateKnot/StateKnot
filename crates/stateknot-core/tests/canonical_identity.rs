// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for external principal identities.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{IssuerId, PrincipalIdentity, SubjectId};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-identity/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    issuers: TextFixtures,
    subjects: TextFixtures,
    principal_identities: ObjectFixtures,
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
    serde_json::from_str(include_str!("fixtures/core-identity-v1.json"))
        .expect("canonical identity fixture must be valid JSON")
}

#[test]
fn canonical_issuer_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.issuers.valid {
        let issuer = expected.parse::<IssuerId>().unwrap();
        assert_eq!(issuer.as_str(), expected);
        assert_eq!(serde_json::to_value(issuer).unwrap(), Value::from(expected));
    }

    for invalid in fixture.issuers.invalid {
        assert!(
            serde_json::from_value::<IssuerId>(invalid.clone()).is_err(),
            "IssuerId accepted {invalid}"
        );
    }
}

#[test]
fn canonical_subject_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.subjects.valid {
        let subject = expected.parse::<SubjectId>().unwrap();
        assert_eq!(subject.as_str(), expected);
        assert_eq!(
            serde_json::to_value(subject).unwrap(),
            Value::from(expected)
        );
    }

    for invalid in fixture.subjects.invalid {
        assert!(
            serde_json::from_value::<SubjectId>(invalid.clone()).is_err(),
            "SubjectId accepted {invalid}"
        );
    }
}

#[test]
fn canonical_principal_identity_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.principal_identities.valid {
        let identity = serde_json::from_value::<PrincipalIdentity>(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(identity).unwrap(), expected);
    }

    for invalid in fixture.principal_identities.invalid {
        assert!(
            serde_json::from_value::<PrincipalIdentity>(invalid.clone()).is_err(),
            "PrincipalIdentity accepted {invalid}"
        );
    }
}
