// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for deterministic graph checkpoints.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    Checkpoint, CheckpointHead, CheckpointState, CheckpointWrite, GraphReference, NodeId,
    ReadyNodes, Superstep,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-checkpoint/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    supersteps: WireFixtures,
    node_ids: WireFixtures,
    checkpoints: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-checkpoint-v1.json"))
        .expect("canonical checkpoint fixture must be valid JSON")
}

fn assert_wire_value<T>(expected: &Value, type_name: &str) -> T
where
    T: DeserializeOwned + Serialize,
{
    let decoded = from_value::<T>(expected.clone())
        .unwrap_or_else(|error| panic!("{type_name} rejected {expected}: {error}"));
    assert_eq!(&to_value(&decoded).unwrap(), expected);
    decoded
}

fn assert_wire_fixtures<T>(fixtures: WireFixtures, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    for expected in fixtures.valid {
        assert_wire_value::<T>(&expected, type_name);
    }
    for invalid in fixtures.invalid {
        assert!(
            from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
}

fn write_value(checkpoint: &Value) -> Value {
    let mut fields = checkpoint
        .as_object()
        .expect("checkpoint fixture must be an object")
        .clone();
    fields.remove("journal_head");
    fields.remove("digest");
    Value::Object(fields)
}

#[test]
fn canonical_checkpoint_scalars_match_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixtures::<Superstep>(fixture.supersteps, "Superstep");
    assert_wire_fixtures::<NodeId>(fixture.node_ids, "NodeId");
}

#[test]
fn canonical_checkpoints_and_all_nested_integrity_values_are_frozen() {
    let fixture = load_fixture();
    assert_eq!(fixture.checkpoints.len(), 2);

    let initial = assert_wire_value::<Checkpoint>(&fixture.checkpoints[0], "Checkpoint");
    let successor = assert_wire_value::<Checkpoint>(&fixture.checkpoints[1], "Checkpoint");
    assert_eq!(successor.parent(), Some(&initial.head()));
    assert_eq!(successor.superstep().get(), 1);
    assert_eq!(successor.journal_head().sequence().get(), 3);

    for (value, checkpoint) in fixture.checkpoints.iter().zip([&initial, &successor]) {
        let write_wire = write_value(value);
        let write = assert_wire_value::<CheckpointWrite>(&write_wire, "CheckpointWrite");
        assert_eq!(write, checkpoint.write_intent());
        assert!(checkpoint.matches_write(&write));

        assert_wire_value::<GraphReference>(&value["graph"], "GraphReference");
        assert_wire_value::<CheckpointState>(&value["state"], "CheckpointState");
        assert_wire_value::<ReadyNodes>(&value["ready_nodes"], "ReadyNodes");
        if let Some(parent) = value.get("parent") {
            assert_wire_value::<CheckpointHead>(parent, "CheckpointHead");
        }
    }
}

#[test]
fn canonical_checkpoint_wires_fail_closed_after_tampering() {
    let fixture = load_fixture();
    let initial = &fixture.checkpoints[0];
    let successor = &fixture.checkpoints[1];

    let mut tampered_state = successor.clone();
    tampered_state["state"]["data"]["status"] = json!("captured");
    assert!(from_value::<Checkpoint>(tampered_state).is_err());

    let mut tampered_ready = successor.clone();
    tampered_ready["ready_nodes"] = json!(["another-node"]);
    assert!(from_value::<Checkpoint>(tampered_ready).is_err());

    let mut tampered_parent = successor.clone();
    tampered_parent["parent"]["digest"] = initial["intent_digest"].clone();
    assert!(from_value::<Checkpoint>(tampered_parent).is_err());

    let mut non_contiguous = successor.clone();
    non_contiguous["superstep"] = json!("2");
    assert!(from_value::<Checkpoint>(non_contiguous).is_err());

    let mut non_advancing = successor.clone();
    non_advancing["journal_head"] = initial["journal_head"].clone();
    assert!(from_value::<Checkpoint>(non_advancing).is_err());

    let mut initial_with_parent = initial.clone();
    initial_with_parent["parent"] = successor["parent"].clone();
    assert!(from_value::<Checkpoint>(initial_with_parent).is_err());

    let mut missing_parent = successor.clone();
    missing_parent
        .as_object_mut()
        .expect("checkpoint fixture object")
        .remove("parent");
    assert!(from_value::<Checkpoint>(missing_parent).is_err());

    let mut extra = successor.clone();
    extra["unsafe_extension"] = json!(true);
    assert!(from_value::<Checkpoint>(extra).is_err());
}

#[test]
fn ready_node_wire_is_unique_bounded_and_canonicalized() {
    assert!(from_value::<ReadyNodes>(json!(["same", "same"])).is_err());
    let unsorted = from_value::<ReadyNodes>(json!(["z-node", "a-node"])).unwrap();
    assert_eq!(to_value(unsorted).unwrap(), json!(["a-node", "z-node"]));

    let oversized = Value::Array(
        (0..=ReadyNodes::MAX_LEN)
            .map(|index| Value::from(format!("node-{index:04}")))
            .collect(),
    );
    assert!(from_value::<ReadyNodes>(oversized).is_err());
}

#[test]
fn checkpoint_schema_objects_remain_closed() {
    for schema in [
        to_value(schemars::schema_for!(GraphReference)).unwrap(),
        to_value(schemars::schema_for!(CheckpointState)).unwrap(),
        to_value(schemars::schema_for!(CheckpointHead)).unwrap(),
        to_value(schemars::schema_for!(CheckpointWrite)).unwrap(),
        to_value(schemars::schema_for!(Checkpoint)).unwrap(),
    ] {
        let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }
}
