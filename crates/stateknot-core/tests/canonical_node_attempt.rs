// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixture for durable physical node attempts.

use schemars::schema_for;
use serde::Deserialize;
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    AttemptId, BoundedJson, BudgetUsage, ByteCount, CanonicalJson, Digest, DurationMillis, EventId,
    ExecutionCount, Failure, FailureCategory, FailureCode, FailureId, FailureMessage,
    FailureOrigin, FencingEpoch, GraphNamespace, JournalHead, JournalSequence, JsonLimits,
    NodeActivation, NodeAttempt, NodeAttemptCompletion, NodeAttemptOutcome, NodeAttemptStart,
    NodeAttemptStartHead, NodeControl, NodeInvocationBindings, NodeStateChange, PendingNodeResult,
    PendingNodeResultIntent, RetryAdvice, RunFence, Timestamp,
};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-node-attempt/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    expected: ExpectedDigests,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedDigests {
    activation: Digest,
    start: Digest,
    success_completion: Digest,
    success_wire: Digest,
    failure_completion: Digest,
    failure_wire: Digest,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-node-attempt-v1.json"))
        .expect("canonical node-attempt fixture must be valid JSON")
}

fn checkpoint() -> stateknot_core::Checkpoint {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/core-checkpoint-v1.json")).unwrap();
    from_value(fixture["checkpoints"][0].clone()).unwrap()
}

fn activation() -> NodeActivation {
    let checkpoint = checkpoint();
    NodeActivation::new(
        checkpoint.head(),
        GraphNamespace::root(),
        stateknot_core::NodeId::new("authorize").unwrap(),
        Digest::sha256(b"canonical-node-attempt-input"),
    )
}

fn journal(activation: &NodeActivation, sequence: u64, label: &str) -> JournalHead {
    let base = activation.base_checkpoint().journal_head();
    JournalHead::new(
        activation.tenant_id().clone(),
        activation.run_id(),
        JournalSequence::new(sequence).unwrap(),
        format!("01912345-6789-7abc-8def-0123456789c{sequence}")
            .parse::<EventId>()
            .unwrap(),
        Timestamp::from_unix_micros(
            base.recorded_at().unix_micros()
                + i64::try_from(sequence - base.sequence().get()).unwrap() * 1_000_000,
        )
        .unwrap(),
        Digest::sha256(label),
    )
}

fn start() -> NodeAttemptStart {
    let activation = activation();
    NodeAttemptStart::new(
        activation.clone(),
        "01912345-6789-7abc-8def-0123456789a1"
            .parse::<AttemptId>()
            .unwrap(),
        RunFence::new(
            activation.tenant_id().clone(),
            activation.run_id(),
            "01912345-6789-7abc-8def-0123456789b1"
                .parse::<AttemptId>()
                .unwrap(),
            FencingEpoch::FIRST,
        ),
        journal(&activation, 2, "canonical-node-attempt-start-event"),
    )
    .unwrap()
}

fn usage() -> BudgetUsage {
    BudgetUsage::builder()
        .graph_steps(ExecutionCount::new(1))
        .input_bytes(ByteCount::new(128))
        .output_bytes(ByteCount::new(64))
        .build()
        .unwrap()
}

fn success_completion(start: &NodeAttemptStart) -> NodeAttemptCompletion {
    let intent = PendingNodeResultIntent::new(
        start.activation().clone(),
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    let result = PendingNodeResult::commit(
        intent,
        start.fence().clone(),
        journal(
            start.activation(),
            3,
            "canonical-node-attempt-success-event",
        ),
    )
    .unwrap();
    NodeAttemptCompletion::succeed(start, result.head(), usage()).unwrap()
}

fn failure_completion(start: &NodeAttemptStart) -> NodeAttemptCompletion {
    let head = journal(
        start.activation(),
        4,
        "canonical-node-attempt-failure-event",
    );
    let failure = Failure::new(
        "01912345-6789-7abc-8def-0123456789f1"
            .parse::<FailureId>()
            .unwrap(),
        FailureCategory::Internal,
        FailureCode::new("graph.node_failed").unwrap(),
        FailureOrigin::new("stateknot.runtime.node").unwrap(),
        FailureMessage::new("Node execution failed safely").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(2_500).unwrap(),
        },
    )
    .unwrap()
    .with_caused_by_event(head.event_id());
    NodeAttemptCompletion::fail(start, failure, usage(), head).unwrap()
}

fn canonical_wire_digest(value: Value) -> Digest {
    CanonicalJson::new(
        &BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM).unwrap(),
    )
    .unwrap()
    .digest()
}

#[test]
fn canonical_node_attempt_freezes_success_and_failure_wires() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    let start = start();
    let success = success_completion(&start);
    let failure = failure_completion(&start);
    let success_attempt = NodeAttempt::restore(start.clone(), Some(success.clone())).unwrap();
    let failure_attempt = NodeAttempt::restore(start.clone(), Some(failure.clone())).unwrap();

    assert_eq!(
        [
            start.activation_digest(),
            start.digest(),
            success.digest(),
            canonical_wire_digest(to_value(&success_attempt).unwrap()),
            failure.digest(),
            canonical_wire_digest(to_value(&failure_attempt).unwrap()),
        ],
        [
            fixture.expected.activation,
            fixture.expected.start,
            fixture.expected.success_completion,
            fixture.expected.success_wire,
            fixture.expected.failure_completion,
            fixture.expected.failure_wire,
        ]
    );
    assert_eq!(
        from_value::<NodeAttemptStart>(to_value(&start).unwrap()).unwrap(),
        start
    );
    assert_eq!(
        from_value::<NodeAttemptStartHead>(to_value(start.head()).unwrap()).unwrap(),
        start.head()
    );
    assert_eq!(
        from_value::<NodeAttempt>(to_value(&success_attempt).unwrap())
            .unwrap()
            .status(),
        success_attempt.status()
    );
    assert_eq!(
        from_value::<NodeAttempt>(to_value(&failure_attempt).unwrap())
            .unwrap()
            .status(),
        failure_attempt.status()
    );
}

#[test]
fn canonical_node_attempt_fails_closed_after_tampering() {
    let start = start();
    let success = success_completion(&start);
    let failure = failure_completion(&start);

    let mut changed_start = to_value(&start).unwrap();
    changed_start["attempt_id"] = json!(
        "01912345-6789-7abc-8def-0123456789a2"
            .parse::<AttemptId>()
            .unwrap()
    );
    assert!(from_value::<NodeAttemptStart>(changed_start).is_err());

    let mut changed_result = to_value(&success).unwrap();
    changed_result["outcome"]["result"]["digest"] = json!(Digest::sha256(b"substituted result"));
    assert!(from_value::<NodeAttemptCompletion>(changed_result).is_err());

    let mut changed_failure = to_value(&failure).unwrap();
    changed_failure["outcome"]["failure"]["retry_advice"] = json!({"kind": "never"});
    assert!(from_value::<NodeAttemptCompletion>(changed_failure).is_err());

    let mut crossed =
        to_value(NodeAttempt::restore(start.clone(), Some(success)).unwrap()).unwrap();
    crossed["completion"]["start"]["attempt_id"] = json!(
        "01912345-6789-7abc-8def-0123456789a2"
            .parse::<AttemptId>()
            .unwrap()
    );
    assert!(from_value::<NodeAttempt>(crossed).is_err());

    let mut extra = to_value(start).unwrap();
    extra["unsafe_extension"] = json!(true);
    assert!(from_value::<NodeAttemptStart>(extra).is_err());
}

#[test]
fn node_attempt_schema_objects_remain_closed() {
    for schema in [
        to_value(schema_for!(NodeAttemptStart)).unwrap(),
        to_value(schema_for!(NodeAttemptStartHead)).unwrap(),
        to_value(schema_for!(NodeAttemptCompletion)).unwrap(),
        to_value(schema_for!(NodeAttempt)).unwrap(),
    ] {
        let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
        assert_eq!(
            schema.get("additionalProperties"),
            Some(&Value::Bool(false))
        );
    }

    let outcome = to_value(schema_for!(NodeAttemptOutcome)).unwrap();
    assert!(outcome.get("oneOf").is_some());
}
