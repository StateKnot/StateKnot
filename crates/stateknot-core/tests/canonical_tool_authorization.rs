// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version fixtures for payload-redacted Tool authorization evidence.

use schemars::schema_for;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    BoundedJson, CanonicalJson, Digest, JsonLimits, ToolAuthorizationOperation,
    ToolAuthorizationProvenance, ToolAuthorizationReceipt,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-tool-authorization/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    operations: WireFixtures,
    provenance: WireFixtures,
    receipts: ReceiptFixtures,
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
struct ReceiptFixtures {
    without_window: Value,
    with_window: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedDigests {
    without_window_canonical_wire: Digest,
    with_window_canonical_wire: Digest,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-tool-authorization-v1.json"))
        .expect("canonical Tool authorization fixture must be valid JSON")
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

#[test]
fn canonical_tool_authorization_components_match_the_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixtures::<ToolAuthorizationOperation>(fixture.operations, "operation");
    assert_wire_fixtures::<ToolAuthorizationProvenance>(fixture.provenance, "provenance");
}

#[test]
fn canonical_tool_authorization_receipts_freeze_complete_wire_and_digests() {
    let fixture = load_fixture();
    let without_window =
        from_value::<ToolAuthorizationReceipt>(fixture.receipts.without_window.clone()).unwrap();
    let with_window =
        from_value::<ToolAuthorizationReceipt>(fixture.receipts.with_window.clone()).unwrap();

    assert_eq!(
        to_value(&without_window).unwrap(),
        fixture.receipts.without_window
    );
    assert_eq!(
        to_value(&with_window).unwrap(),
        fixture.receipts.with_window
    );
    assert_eq!(
        canonical_wire_digest(to_value(&without_window).unwrap()),
        fixture.expected.without_window_canonical_wire
    );
    assert_eq!(
        canonical_wire_digest(to_value(&with_window).unwrap()),
        fixture.expected.with_window_canonical_wire
    );
    assert_eq!(without_window.authorization_window_id(), None);
    assert!(with_window.authorization_window_id().is_some());
    assert_ne!(
        without_window.receipt_digest(),
        with_window.receipt_digest()
    );

    let serialized = serde_json::to_string(&with_window).unwrap();
    for secret in [
        "complete descriptor",
        "private tool input",
        "policy artifact",
        "decision evidence",
    ] {
        assert!(!serialized.contains(secret));
    }
}

#[test]
fn canonical_tool_authorization_receipts_fail_closed_after_tampering() {
    let fixture = load_fixture();

    let mut changed_input = fixture.receipts.without_window.clone();
    changed_input["input_digest"] = json!(Digest::sha256(b"substituted input"));
    assert!(from_value::<ToolAuthorizationReceipt>(changed_input).is_err());

    let mut changed_window = fixture.receipts.with_window.clone();
    changed_window["authorization_window_id"] = json!("018f1f65-45d3-7a2e-8a19-4de38b783466");
    assert!(from_value::<ToolAuthorizationReceipt>(changed_window).is_err());

    let mut forged_digest = fixture.receipts.without_window.clone();
    forged_digest["receipt_digest"] = json!(Digest::sha256(b"forged receipt"));
    assert!(from_value::<ToolAuthorizationReceipt>(forged_digest).is_err());

    let mut unknown = fixture.receipts.with_window;
    unknown["unsafe_extension"] = json!(true);
    assert!(from_value::<ToolAuthorizationReceipt>(unknown).is_err());
}

#[test]
fn tool_authorization_schema_objects_remain_closed() {
    for schema in [
        to_value(schema_for!(ToolAuthorizationProvenance)).unwrap(),
        to_value(schema_for!(ToolAuthorizationReceipt)).unwrap(),
    ] {
        let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }
}
