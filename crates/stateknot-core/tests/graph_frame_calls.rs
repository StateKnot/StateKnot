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

fn checkpoint_id(ordinal: u64) -> CheckpointId {
    format!("01912345-6789-7abc-8def-{ordinal:012x}")
        .parse()
        .unwrap()
}

fn journal(ordinal: u64) -> JournalHead {
    JournalHead::new(
        "frame-call-tenant".parse().unwrap(),
        "01912345-6789-7abc-8def-0123456789a1".parse().unwrap(),
        JournalSequence::new(ordinal).unwrap(),
        format!("01912345-6789-7abc-8def-{ordinal:012x}")
            .parse()
            .unwrap(),
        Timestamp::from_unix_micros(i64::try_from(ordinal).unwrap()).unwrap(),
        Digest::sha256(ordinal.to_be_bytes()),
    )
}

fn initial_checkpoint(graph: &CompiledGraph, ready: ReadyNodes) -> Checkpoint {
    let head = journal(1);
    let write = CheckpointWrite::initial(
        head.tenant_id().clone(),
        head.run_id(),
        checkpoint_id(1),
        graph.reference(),
        CheckpointState::new(
            graph.state_schema().clone(),
            BoundedJson::try_from_value(json!({"value": ["原始状态", 7]})).unwrap(),
        )
        .unwrap(),
        ready,
    )
    .unwrap();
    Checkpoint::commit(write, head).unwrap()
}

#[test]
fn root_entry_copies_the_exact_ready_caller_snapshot_and_logical_identity() {
    let parent = data::parent(1).with_frame_calls(data::policy()).unwrap();
    let base = initial_checkpoint(&parent, parent.entry_nodes().clone());
    let original = serde_json::to_vec(&base).unwrap();
    let call = parent.frame_calls().unwrap().calls()[0].clone();
    let target = data::child();
    let (frame, write) = call
        .prepare_root_entry(&parent, &base, &target, checkpoint_id(2))
        .unwrap();
    let (retry_frame, retry_write) = call
        .prepare_root_entry(&parent, &base, &target, checkpoint_id(3))
        .unwrap();
    assert_eq!(frame, retry_frame);
    assert_ne!(write.intent_digest(), retry_write.intent_digest());
    assert_eq!(frame.origin().base_checkpoint(), &base.head());
    assert!(frame.origin().graph_namespace().is_root());
    assert_eq!(write.tenant_id(), base.tenant_id());
    assert_eq!(write.run_id(), base.run_id());
    assert_eq!(write.graph(), &target.reference());
    assert_eq!(write.state(), base.state());
    assert_eq!(write.ready_nodes(), target.entry_nodes());
    assert_eq!(write.superstep(), Superstep::INITIAL);
    assert!(write.parent().is_none());
    let child =
        GraphFrameCheckpoint::new(frame, Checkpoint::commit(write, journal(2)).unwrap()).unwrap();
    assert_eq!(child.checkpoint().state(), base.state());
    assert_eq!(serde_json::to_vec(&base).unwrap(), original);
}

#[test]
fn entry_rejects_declaration_target_base_ready_and_position_substitution() {
    let parent = data::parent(1).with_frame_calls(data::policy()).unwrap();
    let base = initial_checkpoint(&parent, parent.entry_nodes().clone());
    let call = parent.frame_calls().unwrap().calls()[0].clone();
    let target = data::child();
    assert_eq!(
        data::call("call", "forged-slot").prepare_root_entry(
            &parent,
            &base,
            &target,
            checkpoint_id(2)
        ),
        Err(GraphFrameCompileError::UndeclaredCaller)
    );
    assert_eq!(
        call.prepare_root_entry(&data::parent(1), &base, &target, checkpoint_id(2)),
        Err(GraphFrameCompileError::UndeclaredCaller)
    );
    assert_eq!(
        call.prepare_root_entry(&parent, &base, &data::parent(1), checkpoint_id(2)),
        Err(GraphFrameCompileError::TargetPinMismatch)
    );
    let other = initial_checkpoint(&data::parent(1), data::ready("call"));
    assert_eq!(
        call.prepare_root_entry(&parent, &other, &target, checkpoint_id(2)),
        Err(GraphFrameCompileError::ParentCheckpointMismatch)
    );
    let invalid_entry = initial_checkpoint(&parent, data::ready("finish"));
    assert_eq!(
        call.prepare_root_entry(&parent, &invalid_entry, &target, checkpoint_id(2)),
        Err(GraphFrameCompileError::ParentCheckpointMismatch)
    );
    assert_eq!(
        call.prepare_root_entry(&parent, &base, &target, base.checkpoint_id()),
        Err(GraphFrameCompileError::ParentCheckpointMismatch)
    );
    let write = CheckpointWrite::successor(
        checkpoint_id(2),
        &base,
        base.state().clone(),
        data::ready("finish"),
    )
    .unwrap();
    let not_ready = Checkpoint::commit(write, journal(2)).unwrap();
    assert_eq!(
        call.prepare_root_entry(&parent, &not_ready, &target, checkpoint_id(3)),
        Err(GraphFrameCompileError::CallerNotReady)
    );
    let mut exhausted = base;
    for ordinal in 2..=9 {
        let write = CheckpointWrite::successor(
            checkpoint_id(ordinal),
            &exhausted,
            exhausted.state().clone(),
            data::ready("call"),
        )
        .unwrap();
        exhausted = Checkpoint::commit(write, journal(ordinal)).unwrap();
    }
    assert_eq!(
        call.prepare_root_entry(&parent, &exhausted, &target, checkpoint_id(10)),
        Err(GraphFrameCompileError::ParentCheckpointMismatch)
    );
}

#[test]
fn scoped_entry_uses_the_actual_parent_frame_and_isolates_sibling_origins() {
    let middle = data::parent(1).with_frame_calls(data::policy()).unwrap();
    let target = data::child();
    let outer_call = GraphFrameCall::new(
        "call".parse().unwrap(),
        "outer-slot".parse().unwrap(),
        &middle,
        "return".parse().unwrap(),
    )
    .unwrap();
    let outer = data::parent_named("outer", 1)
        .with_frame_calls(GraphFrameCallPolicy::new(2, 16, [outer_call.clone()]).unwrap())
        .unwrap();
    let base = initial_checkpoint(&outer, outer.entry_nodes().clone());
    let (frame, write) = outer_call
        .prepare_root_entry(&outer, &base, &middle, checkpoint_id(2))
        .unwrap();
    let parent =
        GraphFrameCheckpoint::new(frame, Checkpoint::commit(write, journal(2)).unwrap()).unwrap();
    let original = serde_json::to_vec(&parent).unwrap();
    let call = middle.frame_calls().unwrap().calls()[0].clone();
    let (child, write) = call
        .prepare_frame_entry(&middle, &parent, &target, checkpoint_id(3))
        .unwrap();
    assert_eq!(
        child.origin(),
        &parent.activation("call".parse().unwrap()).unwrap()
    );
    assert_eq!(child.origin().graph_namespace(), parent.frame().namespace());
    assert_ne!(child.namespace(), parent.frame().namespace());
    assert_eq!(child.namespace().as_str().split('/').count(), 2);
    assert_eq!(write.state(), parent.checkpoint().state());
    assert_eq!(write.ready_nodes(), target.entry_nodes());
    assert_eq!(write.superstep(), Superstep::INITIAL);
    assert_eq!(serde_json::to_vec(&parent).unwrap(), original);
    let sibling = GraphFrameIdentity::new(
        parent.frame().origin().clone(),
        "another-outer-slot".parse().unwrap(),
        middle.reference(),
    )
    .unwrap();
    let sibling_parent = GraphFrameCheckpoint::new(sibling, parent.checkpoint().clone()).unwrap();
    let (sibling_child, _) = call
        .prepare_frame_entry(&middle, &sibling_parent, &target, checkpoint_id(3))
        .unwrap();
    assert_ne!(child.origin(), sibling_child.origin());
    assert_ne!(child.namespace(), sibling_child.namespace());
    assert_eq!(child.target(), sibling_child.target());
    let child =
        GraphFrameCheckpoint::new(child, Checkpoint::commit(write, journal(3)).unwrap()).unwrap();
    let terminal = terminal_plan(&target, &child);
    let returned = call
        .prepare_frame_return(&middle, &parent, &target, &child, &terminal)
        .unwrap();
    assert_eq!(returned.activation(), child.frame().origin());
    assert_eq!(
        returned.activation().graph_namespace(),
        parent.frame().namespace()
    );
    assert_return(&call, &returned, &terminal);
    assert_eq!(serde_json::to_vec(&parent).unwrap(), original);
}

struct Schemas;
impl GraphSchemaValidator for Schemas {
    fn validate(
        &self,
        schema: &SchemaReference,
        value: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        let valid = value.as_value().as_object().is_some_and(|object| {
            object.len() == 1
                && object
                    .get("value")
                    .and_then(Value::as_array)
                    .is_some_and(|array| {
                        array.len() == 2 && array[0].is_string() && array[1].is_u64()
                    })
        });
        if schema == &data::schema() && valid {
            Ok(())
        } else {
            Err(GraphSchemaValidationError::Rejected)
        }
    }
}

struct Preserve(GraphReducerReference);
impl GraphReducer for Preserve {
    fn reference(&self) -> &GraphReducerReference {
        &self.0
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        if updates.is_empty() {
            Ok(state.clone())
        } else {
            Err(GraphReducerError::Rejected)
        }
    }
}

fn control_plan(
    graph: &CompiledGraph,
    child: &GraphFrameCheckpoint,
    control: NodeControl,
) -> GraphFrameBarrierPlan {
    let activation = child
        .activation(
            child
                .checkpoint()
                .ready_nodes()
                .iter()
                .next()
                .unwrap()
                .clone(),
        )
        .unwrap();
    let fence = RunFence::new(
        activation.base_checkpoint().tenant_id().clone(),
        activation.base_checkpoint().run_id(),
        "01912345-6789-7abc-8def-0123456789f1".parse().unwrap(),
        FencingEpoch::new(1).unwrap(),
    );
    let sequence = child.checkpoint().journal_head().sequence().get();
    let result = PendingNodeResult::commit(
        PendingNodeResultIntent::new(
            activation,
            NodeStateChange::Unchanged,
            control,
            NodeInvocationBindings::empty(),
        )
        .unwrap(),
        fence,
        journal(sequence + 1),
    )
    .unwrap();
    graph
        .plan_frame_barrier(
            child,
            &[result],
            checkpoint_id(sequence + 2),
            &Schemas,
            &Preserve(graph.reducer().clone()),
        )
        .unwrap()
}

fn terminal_plan(graph: &CompiledGraph, child: &GraphFrameCheckpoint) -> GraphFrameBarrierPlan {
    control_plan(
        graph,
        child,
        NodeControl::Terminal {
            output: NodeTerminalOutput::new(
                graph.output_schema().clone(),
                BoundedJson::try_from_value(json!({"value": ["终止输出", 11]})).unwrap(),
            )
            .unwrap(),
        },
    )
}

fn assert_return(
    call: &GraphFrameCall,
    returned: &PendingNodeResultIntent,
    terminal: &GraphFrameBarrierPlan,
) {
    let output = terminal.disposition().terminal_output().unwrap();
    let update = returned.state_change().update().unwrap();
    assert_eq!(update.schema(), output.schema());
    assert_eq!(update.data(), output.data());
    assert_eq!(
        returned.control(),
        &NodeControl::Route {
            route_id: call.return_route().clone()
        }
    );
    assert!(returned.bindings().is_empty());
    assert!(returned.child_join().is_none());
}

#[test]
fn root_return_requires_the_exact_terminal_child_and_preserves_parent_state() {
    let parent = data::parent(1).with_frame_calls(data::policy()).unwrap();
    let base = initial_checkpoint(&parent, parent.entry_nodes().clone());
    let original = serde_json::to_vec(&base).unwrap();
    let call = parent.frame_calls().unwrap().calls()[0].clone();
    let target = data::child();
    let (identity, write) = call
        .prepare_root_entry(&parent, &base, &target, checkpoint_id(2))
        .unwrap();
    let child = GraphFrameCheckpoint::new(identity, Checkpoint::commit(write, journal(2)).unwrap())
        .unwrap();
    let terminal = terminal_plan(&target, &child);
    let returned = call
        .prepare_root_return(&parent, &base, &target, &child, &terminal)
        .unwrap();
    assert_return(&call, &returned, &terminal);
    assert_eq!(returned.activation(), child.frame().origin());
    assert!(returned.activation().graph_namespace().is_root());
    assert_eq!(serde_json::to_vec(&base).unwrap(), original);
    let sibling = GraphFrameCheckpoint::new(
        GraphFrameIdentity::new(
            child.frame().origin().clone(),
            "sibling-slot".parse().unwrap(),
            target.reference(),
        )
        .unwrap(),
        child.checkpoint().clone(),
    )
    .unwrap();
    assert_eq!(
        call.prepare_root_return(&parent, &base, &target, &sibling, &terminal),
        Err(GraphFrameCompileError::ReturnFrameMismatch)
    );
    let next = GraphFrameCheckpoint::new(
        child.frame().clone(),
        Checkpoint::commit(
            CheckpointWrite::successor(
                checkpoint_id(5),
                child.checkpoint(),
                child.checkpoint().state().clone(),
                child.checkpoint().ready_nodes().clone(),
            )
            .unwrap(),
            journal(5),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        call.prepare_root_return(&parent, &base, &target, &next, &terminal),
        Err(GraphFrameCompileError::ReturnFrameMismatch)
    );
}

#[test]
fn continuation_barrier_cannot_be_converted_to_a_frame_return() {
    let target = data::parent(1);
    let call = GraphFrameCall::new(
        "call".parse().unwrap(),
        "slot".parse().unwrap(),
        &target,
        "return".parse().unwrap(),
    )
    .unwrap();
    let parent = data::parent_named("outer", 1)
        .with_frame_calls(GraphFrameCallPolicy::new(1, 1, [call.clone()]).unwrap())
        .unwrap();
    let base = initial_checkpoint(&parent, parent.entry_nodes().clone());
    let (identity, write) = call
        .prepare_root_entry(&parent, &base, &target, checkpoint_id(2))
        .unwrap();
    let child = GraphFrameCheckpoint::new(identity, Checkpoint::commit(write, journal(2)).unwrap())
        .unwrap();
    let continuing = control_plan(
        &target,
        &child,
        NodeControl::Route {
            route_id: "return".parse().unwrap(),
        },
    );
    assert_eq!(continuing.disposition(), &GraphBarrierDisposition::Continue);
    assert_eq!(
        call.prepare_root_return(&parent, &base, &target, &child, &continuing),
        Err(GraphFrameCompileError::ChildNotTerminal)
    );
}
