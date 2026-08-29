// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for semantic model-stream events.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{
    AttemptId, ModelDescriptor, ModelEvent, ModelEventAccumulator, ModelOutputDelta,
    ModelOutputDeltaKind, ModelOutputStart, ModelRequest, ModelResponse, ModelStreamChunk,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-model-event/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    chunks: WireFixtures,
    output_starts: WireFixtures,
    delta_kinds: WireFixtures,
    deltas: WireFixtures,
    events: WireFixtures,
    raw_invalid_events: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-model-event-v1.json"))
        .expect("canonical model event fixture must be valid JSON")
}

fn assert_wire_fixtures<T>(fixtures: WireFixtures, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    for expected in fixtures.valid {
        let decoded = serde_json::from_value::<T>(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
    }

    for invalid in fixtures.invalid {
        assert!(
            serde_json::from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
}

#[test]
fn canonical_model_event_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    assert_wire_fixtures::<ModelStreamChunk>(fixture.chunks, "ModelStreamChunk");
    assert_wire_fixtures::<ModelOutputStart>(fixture.output_starts, "ModelOutputStart");
    assert_wire_fixtures::<ModelOutputDeltaKind>(fixture.delta_kinds, "ModelOutputDeltaKind");
    assert_wire_fixtures::<ModelOutputDelta>(fixture.deltas, "ModelOutputDelta");

    let event_values = fixture.events.valid.clone();
    assert_wire_fixtures::<ModelEvent>(fixture.events, "ModelEvent");
    for invalid in fixture.raw_invalid_events {
        assert!(
            serde_json::from_str::<ModelEvent>(&invalid).is_err(),
            "ModelEvent accepted raw wire {invalid}"
        );
    }

    let descriptor_fixture =
        serde_json::from_str::<Value>(include_str!("fixtures/core-model-descriptor-v1.json"))
            .unwrap();
    let capability_fixture =
        serde_json::from_str::<Value>(include_str!("fixtures/core-model-capability-v1.json"))
            .unwrap();
    let mut descriptor_value = descriptor_fixture["descriptors"]["valid"][0].clone();
    descriptor_value["capabilities"] = capability_fixture["capabilities"]["valid"][1].clone();
    let descriptor = serde_json::from_value::<ModelDescriptor>(descriptor_value).unwrap();
    let request_fixture =
        serde_json::from_str::<Value>(include_str!("fixtures/core-model-request-v1.json")).unwrap();
    let mut request_value = request_fixture["requests"]["valid"][0].clone();
    request_value["response_mode"] = Value::from("streaming");
    request_value["requirements"]["streaming"] = Value::Bool(true);
    let request = serde_json::from_value::<ModelRequest>(request_value).unwrap();
    let attempt = event_values[0]["attempt_id"]
        .as_str()
        .unwrap()
        .parse::<AttemptId>()
        .unwrap();

    let mut accumulator = ModelEventAccumulator::new(attempt, &descriptor, &request).unwrap();
    for value in event_values {
        accumulator
            .push(serde_json::from_value::<ModelEvent>(value).unwrap())
            .unwrap();
    }
    let actual = accumulator.finish().unwrap();
    let response_fixture =
        serde_json::from_str::<Value>(include_str!("fixtures/core-model-response-v1.json"))
            .unwrap();
    let expected =
        serde_json::from_value::<ModelResponse>(response_fixture["responses"]["valid"][0].clone())
            .unwrap();
    assert_eq!(actual, expected);
}
