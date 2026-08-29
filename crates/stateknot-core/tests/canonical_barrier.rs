// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixture for deterministic checkpoint barriers.

use schemars::schema_for;
use serde::Deserialize;
use serde_json::{Map, Value, from_value, json, to_value};
use stateknot_core::{
    AttemptId, BarrierResultHeads, BoundedJson, CanonicalJson, Checkpoint, CheckpointBarrier,
    CheckpointId, CheckpointWrite, Digest, EventId, FencingEpoch, GraphNamespace, JournalHead,
    JournalSequence, JsonLimits, NodeActivation, NodeControl, NodeInvocationBindings,
    NodeStateChange, PendingNodeResult, PendingNodeResultIntent, ReadyNodes, RunFence, Timestamp,
};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-barrier/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    expected: ExpectedDigests,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedDigests {
    intent: Digest,
    canonical_wire: Digest,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-barrier-v1.json"))
        .expect("canonical checkpoint barrier fixture must be valid JSON")
}

fn base_checkpoint() -> Checkpoint {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/core-checkpoint-v1.json")).unwrap();
    from_value(fixture["checkpoints"][0].clone()).unwrap()
}

fn result(base: &Checkpoint, node_id: stateknot_core::NodeId, index: u64) -> PendingNodeResult {
    let activation = NodeActivation::new(
        base.head(),
        GraphNamespace::root(),
        node_id,
        Digest::sha256(format!("canonical-barrier-input-{index}")),
    );
    let intent = PendingNodeResultIntent::new(
        activation.clone(),
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    let timestamp = Timestamp::from_unix_micros(
        base.journal_head().recorded_at().unix_micros() + i64::try_from(index).unwrap() * 1_000_000,
    )
    .unwrap();
    let event_id = format!("01912345-6789-7abc-8def-0123456789c{}", index + 1)
        .parse::<EventId>()
        .unwrap();
    let journal = JournalHead::new(
        base.tenant_id().clone(),
        base.run_id(),
        JournalSequence::new(index + 1).unwrap(),
        event_id,
        timestamp,
        Digest::sha256(format!("canonical-barrier-result-event-{index}")),
    );
    let attempt_id = format!("01912345-6789-7abc-8def-0123456789b{}", index + 1)
        .parse::<AttemptId>()
        .unwrap();
    PendingNodeResult::commit(
        intent,
        RunFence::new(
            base.tenant_id().clone(),
            base.run_id(),
            attempt_id,
            FencingEpoch::new(index).unwrap(),
        ),
        journal,
    )
    .unwrap()
}

fn constructed_barrier() -> CheckpointBarrier {
    let base = base_checkpoint();
    let results = base
        .ready_nodes()
        .iter()
        .rev()
        .enumerate()
        .map(|(index, node_id)| result(&base, node_id.clone(), u64::try_from(index + 1).unwrap()))
        .map(|result| result.head())
        .collect::<Vec<_>>();
    let successor = CheckpointWrite::successor(
        "01912345-6789-7abc-8def-0123456789d2"
            .parse::<CheckpointId>()
            .unwrap(),
        &base,
        base.state().clone(),
        ReadyNodes::empty(),
    )
    .unwrap();
    CheckpointBarrier::new(&base, successor, results).unwrap()
}

#[test]
fn canonical_barrier_freezes_the_complete_wire_and_digest() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    let barrier = constructed_barrier();
    let wire = to_value(&barrier).unwrap();
    let canonical = CanonicalJson::new(
        &BoundedJson::try_from_value_with_limits(wire.clone(), JsonLimits::MAXIMUM).unwrap(),
    )
    .unwrap();

    assert_eq!(barrier.intent_digest(), fixture.expected.intent);
    assert_eq!(canonical.digest(), fixture.expected.canonical_wire);
    assert_eq!(from_value::<CheckpointBarrier>(wire).unwrap(), barrier);
}

#[test]
fn canonical_barrier_wire_fails_closed_after_tampering() {
    let barrier = constructed_barrier();
    let mut changed_result = to_value(&barrier).unwrap();
    changed_result["result_heads"][0]["digest"] = json!(Digest::sha256(b"substituted result"));
    assert!(from_value::<CheckpointBarrier>(changed_result).is_err());

    let mut changed_ready = to_value(&barrier).unwrap();
    changed_ready["base_ready_nodes"] = json!(["authorize"]);
    assert!(from_value::<CheckpointBarrier>(changed_ready).is_err());

    let mut extra = to_value(&barrier).unwrap();
    extra["unsafe_extension"] = json!(true);
    assert!(from_value::<CheckpointBarrier>(extra).is_err());
}

#[test]
fn checkpoint_barrier_schemas_are_closed_and_bounded() {
    let schema = to_value(schema_for!(CheckpointBarrier)).unwrap();
    let schema = schema.as_object().cloned().unwrap_or_else(Map::new);
    assert_eq!(
        schema.get("additionalProperties"),
        Some(&Value::Bool(false))
    );
    let results = to_value(schema_for!(BarrierResultHeads)).unwrap();
    assert_eq!(results["minItems"], 1);
    assert_eq!(results["maxItems"], BarrierResultHeads::MAX_LEN);
    assert_eq!(results["uniqueItems"], true);
}
