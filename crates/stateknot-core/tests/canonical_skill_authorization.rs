// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version fixtures for durable Skill authorization evidence.

use schemars::schema_for;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    BoundedJson, CanonicalJson, Digest, JsonLimits, SkillActingWindow, SkillActingWindowDuration,
    SkillActingWindowOpenRequest, SkillActingWindowRevocation, SkillActingWindowRevocationReason,
    SkillActivationApproval, SkillActivationScope, SkillActivationSource,
    SkillAuthorizationSubject,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-skill-authorization/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    scopes: WireFixtures,
    subjects: WireFixtures,
    sources: WireFixtures,
    durations: WireFixtures,
    revocation_reasons: WireFixtures,
    records: RecordFixtures,
    expected: ExpectedDigests,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordFixtures {
    approval: Value,
    open_request: Value,
    window: Value,
    revocation: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedDigests {
    subject: Digest,
    approval_canonical_wire: Digest,
    open_request_canonical_wire: Digest,
    window_canonical_wire: Digest,
    revocation_canonical_wire: Digest,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-skill-authorization-v1.json"))
        .expect("canonical Skill authorization fixture must be valid JSON")
}

fn assert_wire_fixtures<T>(fixtures: WireFixtures, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    for expected in fixtures.valid {
        let decoded = from_value::<T>(expected.clone())
            .unwrap_or_else(|error| panic!("{type_name} rejected {expected}: {error}"));
        assert_eq!(to_value(decoded).unwrap(), expected);
    }
    for invalid in fixtures.invalid {
        assert!(
            from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
}

fn canonical_wire_digest(value: Value) -> Digest {
    let bounded = BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM).unwrap();
    CanonicalJson::new(&bounded).unwrap().digest()
}

fn assert_object_schemas_closed(schema: &Value) {
    match schema {
        Value::Object(object) => {
            if object.get("type") == Some(&Value::String("object".to_owned())) {
                assert_eq!(
                    object.get("additionalProperties"),
                    Some(&Value::Bool(false))
                );
            }
            for value in object.values() {
                assert_object_schemas_closed(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_object_schemas_closed(value);
            }
        }
        _ => {}
    }
}

#[test]
fn canonical_skill_authorization_components_match_the_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixtures::<SkillActivationScope>(fixture.scopes, "activation scope");
    assert_wire_fixtures::<SkillAuthorizationSubject>(fixture.subjects, "subject");
    assert_wire_fixtures::<SkillActivationSource>(fixture.sources, "activation source");
    assert_wire_fixtures::<SkillActingWindowDuration>(fixture.durations, "duration");
    assert_wire_fixtures::<SkillActingWindowRevocationReason>(
        fixture.revocation_reasons,
        "revocation reason",
    );
}

#[test]
fn canonical_skill_authorization_records_freeze_complete_wire_and_digests() {
    let fixture = load_fixture();
    let approval = from_value::<SkillActivationApproval>(fixture.records.approval.clone()).unwrap();
    let open_request =
        from_value::<SkillActingWindowOpenRequest>(fixture.records.open_request.clone()).unwrap();
    let window = from_value::<SkillActingWindow>(fixture.records.window.clone()).unwrap();
    let revocation =
        from_value::<SkillActingWindowRevocation>(fixture.records.revocation.clone()).unwrap();

    assert_eq!(to_value(&approval).unwrap(), fixture.records.approval);
    assert_eq!(
        to_value(&open_request).unwrap(),
        fixture.records.open_request
    );
    assert_eq!(to_value(&window).unwrap(), fixture.records.window);
    assert_eq!(to_value(&revocation).unwrap(), fixture.records.revocation);
    assert_eq!(
        approval.subject().subject_digest(),
        fixture.expected.subject
    );
    assert_eq!(
        canonical_wire_digest(to_value(&approval).unwrap()),
        fixture.expected.approval_canonical_wire
    );
    assert_eq!(
        canonical_wire_digest(to_value(&open_request).unwrap()),
        fixture.expected.open_request_canonical_wire
    );
    assert_eq!(
        canonical_wire_digest(to_value(&window).unwrap()),
        fixture.expected.window_canonical_wire
    );
    assert_eq!(
        canonical_wire_digest(to_value(&revocation).unwrap()),
        fixture.expected.revocation_canonical_wire
    );
    assert_eq!(open_request.approval(), &approval);
    assert_eq!(window.approval(), &approval);
    assert_eq!(open_request.window_id(), window.window_id());
    assert_eq!(revocation.window_id(), window.window_id());

    let serialized = serde_json::to_string(&window).unwrap();
    for secret in ["policy payload", "decision payload", "remote Skill bytes"] {
        assert!(!serialized.contains(secret));
    }
}

#[test]
fn canonical_skill_authorization_records_fail_closed_after_tampering() {
    let fixture = load_fixture();

    let mut changed_subject = fixture.records.approval.clone();
    changed_subject["subject"]["uri"] = json!("skill://substituted/SKILL.md");
    assert!(from_value::<SkillActivationApproval>(changed_subject).is_err());

    let mut changed_expiry = fixture.records.window.clone();
    changed_expiry["expires_at"] = json!("2026-09-20T12:02:00.000000Z");
    assert!(from_value::<SkillActingWindow>(changed_expiry).is_err());

    let mut forged_window = fixture.records.window.clone();
    forged_window["window_digest"] = json!(Digest::sha256(b"forged window"));
    assert!(from_value::<SkillActingWindow>(forged_window).is_err());

    let mut changed_revocation = fixture.records.revocation;
    changed_revocation["reason"] = json!("administrative");
    assert!(from_value::<SkillActingWindowRevocation>(changed_revocation).is_err());

    let mut unknown = fixture.records.open_request;
    unknown["unsafe_extension"] = json!(true);
    assert!(from_value::<SkillActingWindowOpenRequest>(unknown).is_err());
}

#[test]
fn skill_authorization_schema_objects_remain_closed() {
    for schema in [
        to_value(schema_for!(SkillActivationScope)).unwrap(),
        to_value(schema_for!(SkillAuthorizationSubject)).unwrap(),
        to_value(schema_for!(SkillActivationApproval)).unwrap(),
        to_value(schema_for!(SkillActingWindowOpenRequest)).unwrap(),
        to_value(schema_for!(SkillActingWindow)).unwrap(),
        to_value(schema_for!(SkillActingWindowRevocation)).unwrap(),
    ] {
        let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }

    let source_schema = to_value(schema_for!(SkillActivationSource)).unwrap();
    assert_object_schemas_closed(&source_schema);

    let duration_schema = to_value(schema_for!(SkillActingWindowDuration)).unwrap();
    assert_eq!(duration_schema["type"], "string");
    assert_eq!(duration_schema["minLength"], 4);
    assert_eq!(duration_schema["maxLength"], 8);
    assert_eq!(
        duration_schema["pattern"],
        "^(?:[1-9][0-9]{3,6}|[1-7][0-9]{7}|8[0-5][0-9]{6}|86[0-3][0-9]{5}|86400000)$"
    );
}
