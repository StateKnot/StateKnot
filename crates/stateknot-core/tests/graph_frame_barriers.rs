// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Experimental scoped planner correctness, not durable nested execution.
use proptest::prelude::*;
use serde_json::{Value, json};
use stateknot_core::*;
use std::sync::Mutex;

#[path = "support/frame_barriers.rs"]
mod data;
#[path = "support/canonical_reference.rs"]
mod reference;

struct Schemas;
impl GraphSchemaValidator for Schemas {
    fn validate(
        &self,
        schema: &SchemaReference,
        value: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        if schema == data::graph().state_schema()
            && value
                .as_value()
                .as_object()
                .is_some_and(|object| object.len() == 1 && object.values().all(Value::is_u64))
        {
            Ok(())
        } else {
            Err(GraphSchemaValidationError::Rejected)
        }
    }
}
struct Sum {
    reference: GraphReducerReference,
    order: Mutex<Vec<NodeId>>,
}
impl Sum {
    fn new() -> Self {
        Self {
            reference: data::graph().reducer().clone(),
            order: Mutex::new(Vec::new()),
        }
    }
}
impl GraphReducer for Sum {
    fn reference(&self) -> &GraphReducerReference {
        &self.reference
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        let mut total = state.as_value()["total"]
            .as_u64()
            .ok_or(GraphReducerError::Rejected)?;
        for input in updates {
            self.order.lock().unwrap().push(input.node_id().clone());
            total = total
                .checked_add(
                    input.update().data().as_value()["amount"]
                        .as_u64()
                        .ok_or(GraphReducerError::Rejected)?,
                )
                .ok_or(GraphReducerError::ResourceLimit)?;
        }
        BoundedJson::try_from_value(json!({"total":total}))
            .map_err(|_| GraphReducerError::ResourceLimit)
    }
}

#[test]
fn constructor_and_independent_digest_reproduce_the_current_fixture() {
    let frame = data::frame("slot.a");
    let barrier = data::barrier(&frame);
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/core-graph-frame-barrier-v1.json")).unwrap();
    assert_eq!(serde_json::to_value(&frame).unwrap(), fixture["checkpoint"]);
    assert_eq!(
        serde_json::to_value(data::results(&frame)).unwrap(),
        fixture["results"]
    );
    assert_eq!(serde_json::to_value(&barrier).unwrap(), fixture["barrier"]);
    let preimage = json!({"base_checkpoint":frame.head(), "base_ready_nodes":frame.checkpoint().ready_nodes(),
        "result_heads":barrier.result_heads(), "successor_intent_digest":barrier.successor().intent_digest()});
    let mut bytes = b"stateknot-graph-frame-barrier-intent-v1\0".to_vec();
    bytes.extend_from_slice(reference::canonical_reference(&preimage).as_bytes());
    assert_eq!(barrier.intent_digest(), Digest::sha256(bytes));
    assert_eq!(
        serde_json::from_value::<GraphFrameBarrier>(fixture["barrier"].clone()).unwrap(),
        barrier
    );
}

#[test]
fn planner_reduces_in_node_order_and_keeps_root_and_frame_intents_distinct() {
    let graph = data::graph();
    let frame = data::frame("slot.a");
    let reducer = Sum::new();
    let mut results = data::results(&frame);
    results.reverse();
    let plan = graph
        .plan_frame_barrier(
            &frame,
            &results,
            data::successor(&frame).checkpoint_id(),
            &Schemas,
            &reducer,
        )
        .unwrap();
    assert_eq!(plan.barrier(), &data::barrier(&frame));
    assert_eq!(plan.disposition(), &GraphBarrierDisposition::Continue);
    assert_eq!(
        reducer.order.into_inner().unwrap(),
        ["alpha".parse().unwrap(), "beta".parse().unwrap()]
    );
    assert!(matches!(
        graph.plan_barrier(
            frame.checkpoint(),
            &results,
            data::successor(&frame).checkpoint_id(),
            &Schemas,
            &Sum::new()
        ),
        Err(GraphBarrierPlanError::NonRootActivation { .. })
    ));
    let root: Vec<_> = [("alpha", 2, 3), ("beta", 3, 4)]
        .into_iter()
        .map(|(node, amount, ordinal)| {
            data::result(
                NodeActivation::for_ready_root(frame.checkpoint(), node.parse().unwrap()).unwrap(),
                NodeControl::Continue,
                Some(amount),
                ordinal,
            )
        })
        .collect();
    assert!(matches!(
        graph.plan_frame_barrier(
            &frame,
            &root,
            data::successor(&frame).checkpoint_id(),
            &Schemas,
            &Sum::new()
        ),
        Err(GraphBarrierPlanError::ActivationMismatch { .. })
    ));
    let root_plan = graph
        .plan_barrier(
            frame.checkpoint(),
            &root,
            data::successor(&frame).checkpoint_id(),
            &Schemas,
            &Sum::new(),
        )
        .unwrap();
    assert_eq!(root_plan.barrier().successor(), plan.barrier().successor());
    assert_ne!(
        root_plan.barrier().intent_digest(),
        plan.barrier().intent_digest()
    );
}

#[test]
fn exact_scope_input_and_complete_ready_set_are_required() {
    let frame = data::frame("slot.a");
    let heads: Vec<_> = data::results(&frame)
        .iter()
        .map(PendingNodeResult::head)
        .collect();
    assert!(matches!(
        GraphFrameBarrier::new(&frame, data::successor(&frame), [heads[0].clone()]),
        Err(GraphFrameBarrierError::MissingNode { .. })
    ));
    assert!(matches!(
        GraphFrameBarrier::new(
            &frame,
            data::successor(&frame),
            [heads[0].clone(), heads[0].clone()]
        ),
        Err(GraphFrameBarrierError::DuplicateNode { .. })
    ));
    let sibling = GraphFrameCheckpoint::new(
        data::frame("slot.b").frame().clone(),
        frame.checkpoint().clone(),
    )
    .unwrap();
    for activation in [
        sibling.activation("alpha".parse().unwrap()).unwrap(),
        NodeActivation::for_ready_root(frame.checkpoint(), "alpha".parse().unwrap()).unwrap(),
        NodeActivation::new(
            frame.checkpoint().head(),
            frame.frame().namespace().clone(),
            "alpha".parse().unwrap(),
            Digest::sha256(b"substituted input"),
        ),
        NodeActivation::new(
            frame.checkpoint().head(),
            frame.frame().namespace().clone(),
            "not.ready".parse().unwrap(),
            Digest::sha256(b"input"),
        ),
    ] {
        let result = data::result(activation, NodeControl::Continue, None, 3);
        assert!(matches!(
            GraphFrameBarrier::new(
                &frame,
                data::successor(&frame),
                [result.head(), heads[1].clone()]
            ),
            Err(GraphFrameBarrierError::ActivationMismatch { .. })
        ));
    }
    let wrong = CheckpointWrite::successor(
        "01912345-6789-7abc-8def-0123456789b2".parse().unwrap(),
        &Checkpoint::commit(data::successor(&frame), data::journal(5)).unwrap(),
        frame.checkpoint().state().clone(),
        frame.checkpoint().ready_nodes().clone(),
    )
    .unwrap();
    assert_eq!(
        GraphFrameBarrier::new(&frame, wrong, heads),
        Err(GraphFrameBarrierError::SuccessorParentMismatch)
    );
}

#[test]
fn strict_reader_checks_order_digest_duplicate_fields_and_streaming_ceiling() {
    let frame = data::frame("slot.a");
    let barrier = data::barrier(&frame);
    let wire = serde_json::to_value(&barrier).unwrap();
    let mut reversed = wire.clone();
    reversed["result_heads"].as_array_mut().unwrap().reverse();
    assert!(serde_json::from_value::<GraphFrameBarrier>(reversed).is_err());
    let mut tampered = wire.clone();
    tampered["intent_digest"] = json!(Digest::sha256(b"wrong"));
    assert!(serde_json::from_value::<GraphFrameBarrier>(tampered).is_err());
    let mut oversized = wire.clone();
    oversized["result_heads"] = json!(vec![wire["result_heads"][0].clone(); 1025]);
    assert!(
        serde_json::from_value::<GraphFrameBarrier>(oversized)
            .unwrap_err()
            .to_string()
            .contains("1024")
    );
    assert_eq!(
        GraphFrameBarrier::new(
            &frame,
            data::successor(&frame),
            std::iter::repeat_n(barrier.result_heads()[0].clone(), 1025)
        ),
        Err(GraphFrameBarrierError::TooManyResults)
    );
    let duplicate = format!(
        "{{\"intent_digest\":{},{}",
        wire["intent_digest"],
        &serde_json::to_string(&wire).unwrap()[1..]
    );
    assert!(serde_json::from_str::<GraphFrameBarrier>(&duplicate).is_err());
    let positional: Vec<_> = wire.as_object().unwrap().values().cloned().collect();
    assert!(serde_json::from_value::<GraphFrameBarrier>(json!(positional)).is_err());
}

#[test]
fn wait_and_terminal_plans_preserve_the_exact_frame() {
    let graph = data::graph();
    let frame = data::frame("slot.a");
    let waits = NodeWaits::try_new([NodeWait::timer(
        "01912345-6789-7abc-8def-0123456789d1".parse().unwrap(),
        RunTimerKind::Sleep,
        Timestamp::from_unix_micros(6_000_000).unwrap(),
    )])
    .unwrap();
    let mut results = data::results(&frame);
    results[0] = data::result(
        frame.activation("alpha".parse().unwrap()).unwrap(),
        NodeControl::Wait {
            waits: waits.clone(),
        },
        Some(2),
        3,
    );
    let wait = graph
        .plan_frame_barrier(
            &frame,
            &results,
            data::successor(&frame).checkpoint_id(),
            &Schemas,
            &Sum::new(),
        )
        .unwrap();
    assert_eq!(wait.disposition().waits(), Some(&waits));
    let next = GraphFrameCheckpoint::new(
        frame.frame().clone(),
        Checkpoint::commit(wait.barrier().successor().clone(), data::journal(5)).unwrap(),
    )
    .unwrap();
    let output = NodeTerminalOutput::new(
        graph.output_schema().clone(),
        BoundedJson::try_from_value(json!({"total":5})).unwrap(),
    )
    .unwrap();
    let result = data::result(
        next.activation("finish".parse().unwrap()).unwrap(),
        NodeControl::Terminal {
            output: output.clone(),
        },
        None,
        6,
    );
    let terminal = graph
        .plan_frame_barrier(
            &next,
            &[result],
            "01912345-6789-7abc-8def-0123456789b2".parse().unwrap(),
            &Schemas,
            &Sum::new(),
        )
        .unwrap();
    assert_eq!(terminal.disposition().terminal_output(), Some(&output));
    assert!(terminal.barrier().successor().ready_nodes().is_empty());
    assert_eq!(terminal.barrier().base_checkpoint().frame(), frame.frame());
    assert!(matches!(
        graph.plan_frame_barrier(
            &frame,
            &[],
            data::successor(&frame).checkpoint_id(),
            &Schemas,
            &Sum::new()
        ),
        Err(GraphBarrierPlanError::ResultCountMismatch { .. })
    ));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn frame_barrier_matches_independent_sum_and_canonical_preimage(
        left in 0u64..1_000_000, right in 0u64..1_000_000,
        slot in any::<u16>(), reverse in any::<bool>(),
    ) {
        let frame = data::frame(&format!("slot.{slot}"));
        let graph = data::graph();
        let mut results: Vec<_> = [("alpha", left, 3), ("beta", right, 4)]
            .into_iter().map(|(node, amount, ordinal)| data::result(
                frame.activation(node.parse().unwrap()).unwrap(),
                NodeControl::Continue, Some(amount), ordinal,
            )).collect();
        if reverse { results.reverse(); }
        let plan = graph.plan_frame_barrier(&frame, &results,
            data::successor(&frame).checkpoint_id(), &Schemas, &Sum::new()).unwrap();
        let expected = CheckpointWrite::successor(data::successor(&frame).checkpoint_id(),
            frame.checkpoint(), CheckpointState::new(graph.state_schema().clone(),
                BoundedJson::try_from_value(json!({"total":left+right})).unwrap()).unwrap(),
            ReadyNodes::try_new(["finish".parse().unwrap()]).unwrap()).unwrap();
        prop_assert_eq!(plan.barrier().successor(), &expected);
        let mut ordered = results.clone();
        ordered.sort_by(|left, right| left.intent().activation().node_id().cmp(right.intent().activation().node_id()));
        let heads: Vec<_> = ordered.iter().map(PendingNodeResult::head).collect();
        prop_assert_eq!(plan.barrier().result_heads(), heads.as_slice());
        let value = json!({"base_checkpoint":frame.head(),
            "base_ready_nodes":frame.checkpoint().ready_nodes(), "result_heads":heads,
            "successor_intent_digest":expected.intent_digest()});
        let mut bytes = b"stateknot-graph-frame-barrier-intent-v1\0".to_vec();
        bytes.extend_from_slice(reference::canonical_reference(&value).as_bytes());
        prop_assert_eq!(plan.barrier().intent_digest(), Digest::sha256(bytes));
        let wire = serde_json::to_value(plan.barrier()).unwrap();
        let restored: GraphFrameBarrier = serde_json::from_value(wire.clone()).unwrap();
        prop_assert_eq!(&restored, plan.barrier());
        let mut changed = wire;
        changed["result_heads"][0]["activation"]["input_digest"] = json!(Digest::sha256(b"crossed input"));
        prop_assert!(serde_json::from_value::<GraphFrameBarrier>(changed).is_err());
    }
}
