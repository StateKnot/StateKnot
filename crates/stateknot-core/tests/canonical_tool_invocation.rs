// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for durable tool invocation history.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, from_value, to_value};
use stateknot_core::{
    GraphNamespace, ToolInvocation, ToolInvocationHead, ToolInvocationHistoryVerifier,
    ToolInvocationIntent, ToolInvocationRevision,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-tool-invocation/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    graph_namespaces: WireFixtures,
    revisions: WireFixtures,
    intent: Value,
    records: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-tool-invocation-v1.json"))
        .expect("canonical tool invocation fixture must be valid JSON")
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

fn complete_record(intent: &Value, fragment: &Value) -> Value {
    let mut record = fragment
        .as_object()
        .expect("tool invocation record fragment must be an object")
        .clone();
    assert!(record.insert("intent".into(), intent.clone()).is_none());
    Value::Object(record)
}

#[test]
fn canonical_invocation_scalars_match_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixtures::<GraphNamespace>(fixture.graph_namespaces, "GraphNamespace");
    assert_wire_fixtures::<ToolInvocationRevision>(fixture.revisions, "ToolInvocationRevision");
}

#[test]
fn canonical_intent_and_history_freeze_every_integrity_value() {
    let fixture = load_fixture();
    let intent = from_value::<ToolInvocationIntent>(fixture.intent.clone()).unwrap();
    assert_eq!(to_value(&intent).unwrap(), fixture.intent);
    assert_eq!(fixture.records.len(), 3);

    let mut verifier = ToolInvocationHistoryVerifier::new();
    let mut records = Vec::new();
    for fragment in &fixture.records {
        let expected = complete_record(&fixture.intent, fragment);
        let record = from_value::<ToolInvocation>(expected.clone()).unwrap();
        assert_eq!(to_value(&record).unwrap(), expected);
        assert_eq!(record.intent(), &intent);
        verifier.verify_next(&record).unwrap();
        records.push(record);
    }

    assert_eq!(records[0].revision().get(), 0);
    assert_eq!(records[1].revision().get(), 1);
    assert_eq!(records[2].revision().get(), 2);
    assert_eq!(verifier.head(), Some(records[2].head()));

    for record in &records[1..] {
        let previous = record.previous().expect("successor head");
        let wire = to_value(previous).unwrap();
        let decoded = from_value::<ToolInvocationHead>(wire.clone()).unwrap();
        assert_eq!(decoded, *previous);
        assert_eq!(to_value(decoded).unwrap(), wire);
    }
}

#[test]
fn canonical_invocation_wires_fail_closed_after_tampering() {
    let fixture = load_fixture();

    let mut changed_input = fixture.intent.clone();
    changed_input["input"]["value"]["amount"] = Value::from(43);
    assert!(from_value::<ToolInvocationIntent>(changed_input).is_err());

    let mut changed_state = complete_record(&fixture.intent, &fixture.records[1]);
    changed_state["state"]["attempt_id"] = Value::from("01912345-6789-7abc-8def-0123456789ac");
    assert!(from_value::<ToolInvocation>(changed_state).is_err());

    let mut changed_transition = complete_record(&fixture.intent, &fixture.records[1]);
    changed_transition["transition"]["attempt_id"] =
        Value::from("01912345-6789-7abc-8def-0123456789ac");
    assert!(from_value::<ToolInvocation>(changed_transition).is_err());

    let mut changed_previous = complete_record(&fixture.intent, &fixture.records[2]);
    changed_previous["previous"]["digest"] = fixture.records[0]["digest"].clone();
    assert!(from_value::<ToolInvocation>(changed_previous).is_err());

    let mut extra = complete_record(&fixture.intent, &fixture.records[0]);
    extra["unsafe_extension"] = Value::Bool(true);
    assert!(from_value::<ToolInvocation>(extra).is_err());
}

#[test]
fn invocation_schema_objects_remain_closed() {
    for schema in [
        to_value(schemars::schema_for!(ToolInvocationIntent)).unwrap(),
        to_value(schemars::schema_for!(ToolInvocationHead)).unwrap(),
        to_value(schemars::schema_for!(ToolInvocation)).unwrap(),
    ] {
        let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }
}
