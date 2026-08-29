// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixture for immutable pending node results.

use schemars::schema_for;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    AttemptId, BoundedJson, CanonicalJson, Digest, EventId, FencingEpoch, JournalHead,
    JournalSequence, JsonLimits, NodeControl, NodeInvocationBinding, NodeInvocationBindings,
    NodeStateChange, NodeStateUpdate, PendingNodeResult, PendingNodeResultHead,
    PendingNodeResultIntent, RouteId, RunFence, Timestamp, ToolInvocation,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-node-result/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    route_ids: WireFixtures,
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
struct ExpectedDigests {
    update: Digest,
    intent: Digest,
    record: Digest,
    canonical_wire: Digest,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-node-result-v1.json"))
        .expect("canonical pending node result fixture must be valid JSON")
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

fn tool_fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/core-tool-invocation-v1.json")).unwrap()
}

fn committed_tool_invocation() -> ToolInvocation {
    let fixture = tool_fixture();
    let intent = fixture["intent"].clone();
    let mut record = fixture["records"][2].as_object().unwrap().clone();
    assert!(record.insert("intent".into(), intent).is_none());
    from_value(Value::Object(record)).unwrap()
}

fn constructed_result() -> PendingNodeResult {
    let invocation = committed_tool_invocation();
    let activation = invocation.intent().activation().clone();
    let update = NodeStateUpdate::new(
        activation.base_checkpoint().graph().state_schema().clone(),
        BoundedJson::try_from_value_with_limits(
            json!({"approved": true, "transaction_id": "txn_42"}),
            JsonLimits::MAXIMUM,
        )
        .unwrap(),
    )
    .unwrap();
    let binding = NodeInvocationBinding::from_tool(&invocation).unwrap();
    let bindings = NodeInvocationBindings::try_new(&activation, [binding]).unwrap();
    let intent = PendingNodeResultIntent::new(
        activation.clone(),
        NodeStateChange::Update { update },
        NodeControl::Route {
            route_id: RouteId::new("captured").unwrap(),
        },
        bindings,
    )
    .unwrap();
    let fence = RunFence::new(
        activation.tenant_id().clone(),
        activation.run_id(),
        "01912345-6789-7abc-8def-0123456789b1"
            .parse::<AttemptId>()
            .unwrap(),
        FencingEpoch::FIRST,
    );
    let journal_head = JournalHead::new(
        activation.tenant_id().clone(),
        activation.run_id(),
        JournalSequence::new(5).unwrap(),
        "01912345-6789-7abc-8def-0123456789c5"
            .parse::<EventId>()
            .unwrap(),
        "2030-01-01T00:00:05.000000Z".parse::<Timestamp>().unwrap(),
        Digest::sha256(b"node-result-event"),
    );
    PendingNodeResult::commit(intent, fence, journal_head).unwrap()
}

#[test]
fn canonical_route_ids_match_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_fixtures::<RouteId>(fixture.route_ids, "RouteId");
}

#[test]
fn canonical_pending_result_freezes_the_complete_wire_and_digests() {
    let fixture = load_fixture();
    let result = constructed_result();
    let wire = to_value(&result).unwrap();
    let bounded_wire =
        BoundedJson::try_from_value_with_limits(wire.clone(), JsonLimits::MAXIMUM).unwrap();
    let canonical_wire = CanonicalJson::new(&bounded_wire).unwrap();

    assert_eq!(
        result.intent().state_change().update().unwrap().digest(),
        fixture.expected.update
    );
    assert_eq!(result.intent().intent_digest(), fixture.expected.intent);
    assert_eq!(result.digest(), fixture.expected.record);
    assert_eq!(canonical_wire.digest(), fixture.expected.canonical_wire);

    let decoded = from_value::<PendingNodeResult>(wire).unwrap();
    assert_eq!(decoded, result);
    let head_wire = to_value(result.head()).unwrap();
    assert_eq!(
        from_value::<PendingNodeResultHead>(head_wire.clone()).unwrap(),
        result.head()
    );
    assert_eq!(to_value(result.head()).unwrap(), head_wire);
}

#[test]
fn canonical_pending_result_fails_closed_after_tampering() {
    let result = to_value(constructed_result()).unwrap();

    let mut changed_update = result.clone();
    changed_update["intent"]["state_change"]["update"]["data"]["approved"] = json!(false);
    assert!(from_value::<PendingNodeResult>(changed_update).is_err());

    let mut changed_route = result.clone();
    changed_route["intent"]["control"]["route_id"] = json!("rejected");
    assert!(from_value::<PendingNodeResult>(changed_route).is_err());

    let mut changed_binding = result.clone();
    changed_binding["intent"]["bindings"][0]["head"]["digest"] =
        json!(Digest::sha256(b"substituted tool revision"));
    assert!(from_value::<PendingNodeResult>(changed_binding).is_err());

    let mut changed_fence = result.clone();
    changed_fence["fence"]["epoch"] = json!("2");
    assert!(from_value::<PendingNodeResult>(changed_fence).is_err());

    let mut extra = result;
    extra["unsafe_extension"] = json!(true);
    assert!(from_value::<PendingNodeResult>(extra).is_err());
}

#[test]
fn pending_result_schema_objects_remain_closed() {
    for schema in [
        to_value(schema_for!(NodeStateUpdate)).unwrap(),
        to_value(schema_for!(PendingNodeResultIntent)).unwrap(),
        to_value(schema_for!(PendingNodeResultHead)).unwrap(),
        to_value(schema_for!(PendingNodeResult)).unwrap(),
    ] {
        let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }
}
