// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for provider-neutral model responses.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{
    ModelFinishReason, ModelOutputItem, ModelProviderModelId, ModelProviderResponseId,
    ModelProviderToolCallId, ModelRequest, ModelResponse, ModelResponseProvenance,
    ModelToolCallProposal, ModelUsage,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-model-response/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    provider_model_ids: ScalarFixtures,
    provider_response_ids: ScalarFixtures,
    provider_tool_call_ids: ScalarFixtures,
    finish_reasons: WireFixtures,
    provenances: WireFixtures,
    usages: WireFixtures,
    tool_call_proposals: WireFixtures,
    output_items: WireFixtures,
    responses: WireFixtures,
    raw_invalid_responses: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScalarFixtures {
    valid: Vec<String>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-model-response-v1.json"))
        .expect("canonical model response fixture must be valid JSON")
}

fn assert_scalar_fixtures<T>(fixtures: ScalarFixtures, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    for expected in fixtures.valid {
        let decoded = serde_json::from_value::<T>(Value::from(expected.clone())).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            Value::from(expected)
        );
    }

    for invalid in fixtures.invalid {
        assert!(
            serde_json::from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
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
fn canonical_model_response_fixture_matches_the_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    let bound_response =
        serde_json::from_value::<ModelResponse>(fixture.responses.valid[0].clone())
            .expect("first canonical response must be structurally valid");
    let descriptor_fixture =
        serde_json::from_str::<Value>(include_str!("fixtures/core-model-descriptor-v1.json"))
            .unwrap();
    let descriptor =
        serde_json::from_value(descriptor_fixture["descriptors"]["valid"][0].clone()).unwrap();
    let request_fixture =
        serde_json::from_str::<Value>(include_str!("fixtures/core-model-request-v1.json")).unwrap();
    let request =
        serde_json::from_value::<ModelRequest>(request_fixture["requests"]["valid"][0].clone())
            .unwrap();
    bound_response.validate_for(&descriptor, &request).unwrap();

    assert_scalar_fixtures::<ModelProviderModelId>(
        fixture.provider_model_ids,
        "ModelProviderModelId",
    );
    assert_scalar_fixtures::<ModelProviderResponseId>(
        fixture.provider_response_ids,
        "ModelProviderResponseId",
    );
    assert_scalar_fixtures::<ModelProviderToolCallId>(
        fixture.provider_tool_call_ids,
        "ModelProviderToolCallId",
    );
    assert_wire_fixtures::<ModelFinishReason>(fixture.finish_reasons, "ModelFinishReason");
    assert_wire_fixtures::<ModelResponseProvenance>(fixture.provenances, "ModelResponseProvenance");
    assert_wire_fixtures::<ModelUsage>(fixture.usages, "ModelUsage");
    assert_wire_fixtures::<ModelToolCallProposal>(
        fixture.tool_call_proposals,
        "ModelToolCallProposal",
    );
    assert_wire_fixtures::<ModelOutputItem>(fixture.output_items, "ModelOutputItem");
    assert_wire_fixtures::<ModelResponse>(fixture.responses, "ModelResponse");

    for invalid in fixture.raw_invalid_responses {
        assert!(
            serde_json::from_str::<ModelResponse>(&invalid).is_err(),
            "ModelResponse accepted raw wire {invalid}"
        );
    }
}
