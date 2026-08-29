// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for agent admission and terminal results.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{
    AgentArtifacts, AgentDescriptor, AgentRequest, AgentResult, AgentResultProvenance,
    BudgetLimits, Timestamp,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-agent-runtime/1.0.0";
const ADMISSION_OBSERVED_AT: &str = "2029-12-31T23:59:58.000000Z";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    base_budget_layers: Vec<BudgetLimits>,
    requests: WireFixtures,
    result_provenances: WireFixtures,
    artifacts: WireFixtures,
    results: WireFixtures,
    raw_invalid_requests: Vec<String>,
    raw_invalid_result_provenances: Vec<String>,
    raw_invalid_results: Vec<String>,
    request_validation_mutations: Vec<NamedMutations>,
    result_decode_mutations: Vec<NamedMutations>,
    result_validation_mutations: Vec<NamedMutations>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedMutations {
    name: String,
    changes: Vec<Mutation>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Mutation {
    Replace { pointer: String, value: Value },
    DuplicateFirst { pointer: String },
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-agent-runtime-v1.json"))
        .expect("canonical agent runtime fixture must be valid JSON")
}

fn descriptor() -> AgentDescriptor {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/core-agent-v1.json"))
        .expect("canonical agent descriptor fixture must be valid JSON");
    serde_json::from_value(fixture["descriptors"]["valid"][0].clone())
        .expect("canonical agent descriptor must decode")
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

fn apply_mutation(target: &mut Value, mutation: Mutation) {
    match mutation {
        Mutation::Replace { pointer, value } => {
            *target
                .pointer_mut(&pointer)
                .unwrap_or_else(|| panic!("fixture pointer {pointer} must exist")) = value;
        }
        Mutation::DuplicateFirst { pointer } => {
            let values = target
                .pointer_mut(&pointer)
                .unwrap_or_else(|| panic!("fixture pointer {pointer} must exist"))
                .as_array_mut()
                .unwrap_or_else(|| panic!("fixture pointer {pointer} must name an array"));
            let first = values
                .first()
                .cloned()
                .unwrap_or_else(|| panic!("fixture array {pointer} must not be empty"));
            values.push(first);
        }
    }
}

fn apply_mutations(target: &mut Value, changes: Vec<Mutation>) {
    for change in changes {
        apply_mutation(target, change);
    }
}

#[test]
fn canonical_agent_runtime_fixture_matches_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    let canonical_request = fixture
        .requests
        .valid
        .first()
        .expect("fixture must contain a canonical request")
        .clone();
    let canonical_provenance = fixture
        .result_provenances
        .valid
        .first()
        .expect("fixture must contain canonical result provenance")
        .clone();
    let canonical_result = fixture
        .results
        .valid
        .first()
        .expect("fixture must contain a canonical result")
        .clone();

    assert_wire_fixtures::<AgentRequest>(fixture.requests, "AgentRequest");
    assert_wire_fixtures::<AgentResultProvenance>(
        fixture.result_provenances,
        "AgentResultProvenance",
    );
    assert_wire_fixtures::<AgentArtifacts>(fixture.artifacts, "AgentArtifacts");
    assert_wire_fixtures::<AgentResult>(fixture.results, "AgentResult");

    for invalid in fixture.raw_invalid_requests {
        assert!(
            serde_json::from_str::<AgentRequest>(&invalid).is_err(),
            "AgentRequest accepted raw wire {invalid}"
        );
    }
    for invalid in fixture.raw_invalid_result_provenances {
        assert!(
            serde_json::from_str::<AgentResultProvenance>(&invalid).is_err(),
            "AgentResultProvenance accepted raw wire {invalid}"
        );
    }
    for invalid in fixture.raw_invalid_results {
        assert!(
            serde_json::from_str::<AgentResult>(&invalid).is_err(),
            "AgentResult accepted raw wire {invalid}"
        );
    }

    let descriptor = descriptor();
    let request = serde_json::from_value::<AgentRequest>(canonical_request.clone()).unwrap();
    let budget = request
        .resolve_for(
            &descriptor,
            &fixture.base_budget_layers,
            ADMISSION_OBSERVED_AT.parse::<Timestamp>().unwrap(),
        )
        .expect("canonical request must resolve a finite budget");
    let provenance = serde_json::from_value::<AgentResultProvenance>(canonical_provenance).unwrap();
    let result = serde_json::from_value::<AgentResult>(canonical_result.clone()).unwrap();
    result
        .validate_for(&provenance, &request, &descriptor, &budget)
        .expect("canonical terminal result must match all trusted snapshots");

    for mutation in fixture.request_validation_mutations {
        let mut invalid = canonical_request.clone();
        apply_mutations(&mut invalid, mutation.changes);
        let request = serde_json::from_value::<AgentRequest>(invalid)
            .unwrap_or_else(|error| panic!("{} must remain valid wire: {error}", mutation.name));
        assert!(
            request
                .resolve_for(
                    &descriptor,
                    &fixture.base_budget_layers,
                    ADMISSION_OBSERVED_AT.parse::<Timestamp>().unwrap(),
                )
                .is_err(),
            "AgentRequest accepted validation mutation {}",
            mutation.name
        );
    }

    for mutation in fixture.result_decode_mutations {
        let mut invalid = canonical_result.clone();
        apply_mutations(&mut invalid, mutation.changes);
        assert!(
            serde_json::from_value::<AgentResult>(invalid).is_err(),
            "AgentResult accepted intrinsic mutation {}",
            mutation.name
        );
    }

    for mutation in fixture.result_validation_mutations {
        let mut invalid = canonical_result.clone();
        apply_mutations(&mut invalid, mutation.changes);
        let result = serde_json::from_value::<AgentResult>(invalid)
            .unwrap_or_else(|error| panic!("{} must remain valid wire: {error}", mutation.name));
        assert!(
            result
                .validate_for(&provenance, &request, &descriptor, &budget)
                .is_err(),
            "AgentResult accepted validation mutation {}",
            mutation.name
        );
    }
}
