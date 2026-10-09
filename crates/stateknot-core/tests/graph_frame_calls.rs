// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Compiled nested-call profile; runtime/persistence acceptance remains separate.
use serde_json::{Value, json};
use stateknot_core::*;
#[path = "support/frame_calls.rs"]
mod data;

#[test]
fn real_compiled_declarations_reproduce_current_source_vectors() {
    let wire: Value =
        serde_json::from_str(include_str!("fixtures/core-graph-frame-call-v1.json")).unwrap();
    assert_eq!(
        serde_json::to_value(data::call("call", "slot.a")).unwrap(),
        wire["call"]
    );
    assert_eq!(
        serde_json::to_value(data::policy()).unwrap(),
        wire["policy"]
    );
    let compiled = data::parent(1).with_frame_calls(data::policy()).unwrap();
    assert_eq!(serde_json::to_value(&compiled).unwrap(), wire["compiled"]);
    assert_eq!(serde_json::to_value(data::child()).unwrap(), wire["child"]);
    assert_eq!(
        compiled,
        serde_json::from_value(wire["compiled"].clone()).unwrap()
    );
}

#[test]
fn calls_change_definition_pins_and_no_call_graphs_retain_exact_bytes() {
    let parent = data::parent(1);
    let original = parent.canonical_definition_bytes().unwrap();
    assert!(
        !serde_json::from_slice::<Value>(&original)
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("frame_calls")
    );
    let compiled = parent.clone().with_frame_calls(data::policy()).unwrap();
    assert_ne!(parent.reference(), compiled.reference());
    assert_eq!(parent.definition_digest(), Digest::sha256(&original));
    let restored: CompiledGraph =
        serde_json::from_slice(&serde_json::to_vec(&parent).unwrap()).unwrap();
    assert_eq!(restored.canonical_definition_bytes().unwrap(), original);
    let changed = parent
        .with_frame_calls(
            GraphFrameCallPolicy::new(6, 4095, [data::call("call", "slot.b")]).unwrap(),
        )
        .unwrap();
    assert_ne!(changed.reference(), compiled.reference());
    let mut tampered = serde_json::to_value(compiled).unwrap();
    tampered["frame_calls"]["maximum_frame_starts"] = json!(1);
    assert!(serde_json::from_value::<CompiledGraph>(tampered).is_err());
}

#[test]
fn initial_profile_rejects_parallel_callers_and_undeclared_or_mixed_controls() {
    assert_eq!(
        data::parent(2).with_frame_calls(data::policy()),
        Err(GraphCompileError::FramePolicy(
            GraphFrameCompileError::ParallelCaller
        ))
    );
    let absent = GraphFrameCallPolicy::new(1, 1, [data::call("absent", "slot")]).unwrap();
    assert_eq!(
        data::parent(1).with_frame_calls(absent),
        Err(GraphCompileError::FramePolicy(
            GraphFrameCompileError::MissingCaller
        ))
    );
    let terminal = GraphFrameCallPolicy::new(1, 1, [data::call("finish", "slot")]).unwrap();
    assert_eq!(
        data::parent(1).with_frame_calls(terminal),
        Err(GraphCompileError::FramePolicy(
            GraphFrameCompileError::ReturnRouteMismatch
        ))
    );
    let call = GraphFrameCall::new(
        NodeId::new("call").unwrap(),
        NodeId::new("slot").unwrap(),
        &data::child(),
        RouteId::new("another-return").unwrap(),
    )
    .unwrap();
    assert_eq!(
        data::parent(1).with_frame_calls(GraphFrameCallPolicy::new(1, 1, [call]).unwrap()),
        Err(GraphCompileError::FramePolicy(
            GraphFrameCompileError::ReturnRouteMismatch
        ))
    );
}

#[test]
fn finite_policy_and_streaming_reader_reject_invalid_bounds_and_order() {
    for (depth, starts) in [(0, 1), (8, 1), (1, 0), (1, 4097)] {
        assert_eq!(
            GraphFrameCallPolicy::new(depth, starts, [data::call("call", "slot")]),
            Err(GraphFrameCompileError::InvalidLimits)
        );
    }
    assert_eq!(
        GraphFrameCallPolicy::new(1, 1, []),
        Err(GraphFrameCompileError::EmptyCalls)
    );
    assert_eq!(
        GraphFrameCallPolicy::new(1, 1, [data::call("call", "a"), data::call("call", "b")]),
        Err(GraphFrameCompileError::DuplicateNode)
    );
    assert_eq!(
        GraphFrameCallPolicy::new(
            1,
            1,
            (0..=1024).map(|i| data::call(&format!("call-{i:04}"), "slot"))
        ),
        Err(GraphFrameCompileError::TooManyCalls)
    );
    let policy =
        GraphFrameCallPolicy::new(1, 1, [data::call("b", "slot"), data::call("a", "slot")])
            .unwrap();
    assert_eq!(policy.calls()[0].node_id().as_str(), "a");
    assert!(policy.call(&NodeId::new("a").unwrap()).is_some());
    let mut wire = serde_json::to_value(policy).unwrap();
    wire["calls"].as_array_mut().unwrap().reverse();
    assert!(serde_json::from_value::<GraphFrameCallPolicy>(wire).is_err());
    let mut wire = serde_json::to_value(data::policy()).unwrap();
    wire["version"] = json!(2);
    assert!(serde_json::from_value::<GraphFrameCallPolicy>(wire).is_err());
    let mut wire = serde_json::to_value(data::policy()).unwrap();
    wire["calls"] = Value::Array(vec![
        serde_json::to_value(data::call("call", "slot"))
            .unwrap();
        1025
    ]);
    assert!(
        serde_json::from_slice::<GraphFrameCallPolicy>(&serde_json::to_vec(&wire).unwrap())
            .is_err()
    );
}

#[test]
fn exact_target_and_parent_schema_pins_are_required() {
    let call = data::call("call", "slot");
    call.validate_target(&data::child()).unwrap();
    assert_eq!(
        call.validate_target(&data::parent(1)),
        Err(GraphFrameCompileError::TargetPinMismatch)
    );
    let mut wire = serde_json::to_value(&call).unwrap();
    wire["input_schema"]["digest"] = json!(Digest::sha256(b"substituted input"));
    assert!(serde_json::from_value::<GraphFrameCall>(wire).is_err());
    let mut wire = serde_json::to_value(call).unwrap();
    wire["output_schema"]["digest"] = json!(Digest::sha256(b"substituted output"));
    let altered: GraphFrameCall = serde_json::from_value(wire).unwrap();
    assert_eq!(
        data::parent(1).with_frame_calls(GraphFrameCallPolicy::new(1, 1, [altered]).unwrap()),
        Err(GraphCompileError::FramePolicy(
            GraphFrameCompileError::ParentSchemaMismatch
        ))
    );
}

#[test]
fn static_shared_state_expansion_cannot_discard_frame_call_pins() {
    let compiled = data::parent(1).with_frame_calls(data::policy()).unwrap();
    assert!(matches!(
        SharedStateSubgraph::new(compiled.clone()),
        Err(GraphCompositionError::FrameCallsUnsupported)
    ));
    let body = SharedStateSubgraph::new(data::parent(1)).unwrap();
    let call = GraphSubgraphCall::once(NodeId::new("call").unwrap(), body)
        .with_return_routes([(
            NodeId::new("finish").unwrap(),
            RouteId::new("return").unwrap(),
        )])
        .unwrap();
    assert!(matches!(
        GraphComposition::compile(compiled, [call]),
        Err(GraphCompositionError::FrameCallsUnsupported)
    ));
}
