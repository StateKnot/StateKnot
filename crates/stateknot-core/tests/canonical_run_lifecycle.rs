// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for the durable run lifecycle.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, from_value, json, to_value};
use stateknot_core::{
    AgentResult, AgentResultProvenance, BudgetUsage, InterruptId, RunCancellation,
    RunCancellationRequest, RunFailure, RunInterrupt, RunInterruptKind, RunLifecycle, RunRevision,
    RunStatus, RunTimer, RunTimerKind, RunTransition, RunTransitionKind, RunWaits, TimerId,
    Timestamp,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-run-lifecycle/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    provenance: Value,
    zero_usage: Value,
    revisions: WireFixtures,
    statuses: WireFixtures,
    interrupt_kinds: WireFixtures,
    timer_kinds: WireFixtures,
    transition_kinds: WireFixtures,
    interrupts: WireFixtures,
    timers: WireFixtures,
    waits: WireFixtures,
    cancellation_requests: WireFixtures,
    cancellations: WireFixtures,
    failures: WireFixtures,
    raw_invalid_transitions: Vec<String>,
    raw_invalid_lifecycles: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-run-lifecycle-v1.json"))
        .expect("canonical run lifecycle fixture must be valid JSON")
}

fn canonical_agent_result(completed_at: &str) -> (AgentResult, Value) {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/core-agent-runtime-v1.json"))
        .expect("canonical agent runtime fixture must be valid JSON");
    let mut value = fixture["results"]["valid"][0].clone();
    value["completed_at"] = Value::from(completed_at);
    let result = from_value::<AgentResult>(value.clone()).expect("canonical result must decode");
    (result, value)
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

fn assert_wire_value<T>(expected: &Value, type_name: &str)
where
    T: DeserializeOwned + Serialize,
{
    let decoded = from_value::<T>(expected.clone())
        .unwrap_or_else(|error| panic!("{type_name} rejected {expected}: {error}"));
    assert_eq!(&to_value(decoded).unwrap(), expected);
}

#[test]
fn canonical_run_lifecycle_components_match_the_public_wire_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_wire_value::<AgentResultProvenance>(&fixture.provenance, "AgentResultProvenance");
    assert_wire_value::<BudgetUsage>(&fixture.zero_usage, "BudgetUsage");
    assert_wire_fixtures::<RunRevision>(fixture.revisions, "RunRevision");
    assert_wire_fixtures::<RunStatus>(fixture.statuses, "RunStatus");
    assert_wire_fixtures::<RunInterruptKind>(fixture.interrupt_kinds, "RunInterruptKind");
    assert_wire_fixtures::<RunTimerKind>(fixture.timer_kinds, "RunTimerKind");
    assert_wire_fixtures::<RunTransitionKind>(fixture.transition_kinds, "RunTransitionKind");
    assert_wire_fixtures::<RunInterrupt>(fixture.interrupts, "RunInterrupt");
    assert_wire_fixtures::<RunTimer>(fixture.timers, "RunTimer");
    assert_wire_fixtures::<RunWaits>(fixture.waits, "RunWaits");
    assert_wire_fixtures::<RunCancellationRequest>(
        fixture.cancellation_requests,
        "RunCancellationRequest",
    );
    assert_wire_fixtures::<RunCancellation>(fixture.cancellations, "RunCancellation");
    assert_wire_fixtures::<RunFailure>(fixture.failures, "RunFailure");

    for invalid in fixture.raw_invalid_transitions {
        assert!(
            serde_json::from_str::<RunTransition>(&invalid).is_err(),
            "RunTransition accepted raw wire {invalid}"
        );
    }
    for invalid in fixture.raw_invalid_lifecycles {
        assert!(
            serde_json::from_str::<RunLifecycle>(&invalid).is_err(),
            "RunLifecycle accepted raw wire {invalid}"
        );
    }
}

#[test]
fn every_transition_and_state_variant_has_a_canonical_closed_shape() {
    let fixture = load_fixture();
    let provenance = from_value::<AgentResultProvenance>(fixture.provenance.clone()).unwrap();
    let usage = from_value::<BudgetUsage>(fixture.zero_usage.clone()).unwrap();
    let waits_value = fixture.waits.valid[0].clone();
    let waits = from_value::<RunWaits>(waits_value.clone()).unwrap();
    let interrupt_id: InterruptId = "01912345-6789-7abc-8def-0123456789b1".parse().unwrap();
    let timer_id: TimerId = "01912345-6789-7abc-8def-0123456789b3".parse().unwrap();
    let request_value = fixture.cancellation_requests.valid[0].clone();
    let request = from_value::<RunCancellationRequest>(request_value.clone()).unwrap();
    let cancellation_value = fixture.cancellations.valid[0].clone();
    let failure_value = fixture.failures.valid[0].clone();
    let failure = from_value::<RunFailure>(failure_value.clone()).unwrap();
    let (result, result_value) = canonical_agent_result("2030-01-01T00:00:08.000000Z");

    let transition_values = [
        json!({
            "kind": "start",
            "started_at": "2030-01-01T00:00:01.000000Z"
        }),
        json!({"kind": "wait", "waits": waits_value}),
        json!({
            "kind": "resolve_interrupt",
            "interrupt_id": interrupt_id.to_string(),
            "resolved_at": "2030-01-01T00:00:03.000000Z"
        }),
        json!({
            "kind": "fire_timer",
            "timer_id": timer_id.to_string(),
            "fired_at": "2030-01-01T00:00:05.000000Z"
        }),
        json!({"kind": "request_cancellation", "request": request_value}),
        json!({
            "kind": "confirm_cancellation",
            "completed_at": "2030-01-01T00:00:07.000000Z",
            "usage": fixture.zero_usage
        }),
        json!({"kind": "succeed", "result": result_value}),
        json!({"kind": "fail", "failure": failure_value}),
    ];
    for expected in transition_values {
        assert_wire_value::<RunTransition>(&expected, "RunTransition");
    }

    let admitted_at = "2030-01-01T00:00:00.000000Z".parse::<Timestamp>().unwrap();
    let pending = RunLifecycle::admitted(provenance.clone(), admitted_at);
    assert_eq!(
        to_value(&pending).unwrap(),
        json!({
            "provenance": fixture.provenance,
            "admitted_at": "2030-01-01T00:00:00.000000Z",
            "revision": "0",
            "state": {"kind": "pending"}
        })
    );

    let active = pending
        .apply(RunTransition::Start {
            started_at: "2030-01-01T00:00:01.000000Z".parse().unwrap(),
        })
        .unwrap();
    let waiting = active.clone().apply(RunTransition::Wait { waits }).unwrap();
    assert_eq!(waiting.status(), RunStatus::Waiting);
    assert_eq!(waiting.revision(), RunRevision::new(2));
    assert_eq!(
        to_value(&waiting).unwrap()["state"],
        json!({
            "kind": "waiting",
            "waits": fixture.waits.valid[0],
            "changed_at": "2030-01-01T00:00:02.000000Z"
        })
    );
    assert_wire_value::<RunLifecycle>(&to_value(waiting).unwrap(), "RunLifecycle");

    let succeeded = active
        .clone()
        .apply(RunTransition::Succeed { result })
        .unwrap();
    assert_eq!(succeeded.status(), RunStatus::Succeeded);
    assert_eq!(to_value(&succeeded).unwrap()["state"]["kind"], "succeeded");
    assert_wire_value::<RunLifecycle>(&to_value(succeeded).unwrap(), "RunLifecycle");

    let cancelled = active
        .clone()
        .apply(RunTransition::RequestCancellation { request })
        .unwrap()
        .apply(RunTransition::ConfirmCancellation {
            completed_at: "2030-01-01T00:00:07.000000Z".parse().unwrap(),
            usage: usage.clone(),
        })
        .unwrap();
    assert_eq!(cancelled.status(), RunStatus::Cancelled);
    assert_eq!(
        to_value(&cancelled).unwrap()["state"],
        json!({"kind": "cancelled", "cancellation": cancellation_value})
    );
    assert_wire_value::<RunLifecycle>(&to_value(cancelled).unwrap(), "RunLifecycle");

    let failed = active.apply(RunTransition::Fail { failure }).unwrap();
    assert_eq!(failed.status(), RunStatus::Failed);
    assert_wire_value::<RunLifecycle>(&to_value(failed).unwrap(), "RunLifecycle");
}

#[test]
fn run_lifecycle_schemas_publish_closed_objects_and_wait_bounds() {
    let lifecycle_schema = to_value(schemars::schema_for!(RunLifecycle)).unwrap();
    assert_eq!(lifecycle_schema["type"], "object");
    assert_eq!(lifecycle_schema["additionalProperties"], false);
    assert_eq!(
        lifecycle_schema["required"],
        json!(["provenance", "admitted_at", "revision", "state"])
    );

    let waits_schema = to_value(schemars::schema_for!(RunWaits)).unwrap();
    assert_eq!(waits_schema["type"], "array");
    assert_eq!(waits_schema["minItems"], 1);
    assert_eq!(waits_schema["maxItems"], RunWaits::MAX_LEN);

    let transition_schema = to_value(schemars::schema_for!(RunTransition)).unwrap();
    assert!(transition_schema["oneOf"].is_array());
}
