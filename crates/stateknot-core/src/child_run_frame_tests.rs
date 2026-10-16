// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    ChildRunBudgetAccount, ChildRunBudgetError, ChildRunDeclaration, ChildRunTopologyLimits,
    GraphChildRunPolicy, GraphFrameCheckpoint, GraphFrameIdentity, JournalHead, JournalSequence,
};

fn frame_fixture(maximum_children: u16) -> (Fixture, CompiledGraph, GraphFrameCheckpoint) {
    let (value, root) = policy_fixture(true);
    let mut identity = to_value(root.identity()).unwrap();
    identity["capability"]["name"] = json!("frame.owner");
    let node = value.key.parent().node_id().clone();
    let graph = CompiledGraph::compile(
        from_value(identity).unwrap(),
        root.input_schema().clone(),
        root.state_schema().clone(),
        root.update_schema().clone(),
        root.output_schema().clone(),
        root.reducer().clone(),
        root.entry_nodes().clone(),
        root.nodes().iter().cloned(),
        root.limits(),
    )
    .unwrap()
    .with_child_runs(
        GraphChildRunPolicy::new(
            ChildRunTopologyLimits::new(1, maximum_children, 4).unwrap(),
            [ChildRunDeclaration::new(
                node,
                value.key.slot().clone(),
                value.child.descriptor(),
                &value.graph,
            )
            .unwrap()],
        )
        .unwrap(),
    )
    .unwrap();
    let frame = GraphFrameIdentity::new(
        value.key.parent().clone(),
        NodeId::new("frame.slot").unwrap(),
        graph.reference(),
    )
    .unwrap();
    let journal = JournalHead::new(
        value.key.tenant_id().clone(),
        value.key.parent_run_id(),
        JournalSequence::new(value.checkpoint.journal_head().sequence().get() + 1).unwrap(),
        crate::EventId::generate(),
        now(),
        Digest::sha256("frame-entry"),
    );
    let checkpoint = Checkpoint::commit(
        crate::CheckpointWrite::initial(
            value.key.tenant_id().clone(),
            value.key.parent_run_id(),
            crate::CheckpointId::generate(),
            graph.reference(),
            value.checkpoint.state().clone(),
            graph.entry_nodes().clone(),
        )
        .unwrap(),
        journal,
    )
    .unwrap();
    (
        value,
        graph,
        GraphFrameCheckpoint::new(frame, checkpoint).unwrap(),
    )
}

fn scoped_intent(
    value: &Fixture,
    frame: &GraphFrameCheckpoint,
) -> Result<ChildRunAdmissionIntent, ChildRunAdmissionIntentError> {
    let activation = frame
        .activation(value.key.parent().node_id().clone())
        .unwrap();
    ChildRunAdmissionIntent::for_frame(
        &value.parent,
        frame,
        ChildRunKey::new(activation, value.key.slot().clone()).unwrap(),
        value.child.clone(),
        value.graph.clone(),
        value.state.clone(),
    )
}

#[test]
fn scoped_child_round_trip_binds_the_full_frame_and_keeps_root_apis_closed() {
    let (value, graph, frame) = frame_fixture(8);
    let intent = scoped_intent(&value, &frame).unwrap();
    intent.validate_declaration(&graph).unwrap();
    let schemas = Schemas::allow();
    intent
        .validate_for_frame(&value.parent, &frame, &schemas, now())
        .unwrap();
    assert_eq!(schemas.count.load(Ordering::SeqCst), 3);
    assert_eq!(
        from_value::<ChildRunAdmissionIntent>(to_value(&intent).unwrap()).unwrap(),
        intent
    );
    assert_eq!(
        intent.validate_for(&value.parent, frame.checkpoint(), &Schemas::allow(), now()),
        Err(ChildRunAdmissionIntentError::UnsupportedParentNamespace)
    );
    assert_eq!(
        ChildRunAdmissionIntent::new(
            &value.parent,
            intent.key().clone(),
            value.child,
            value.graph,
            value.state
        ),
        Err(ChildRunAdmissionIntentError::UnsupportedParentNamespace)
    );
}

#[test]
fn scoped_child_rejects_sibling_namespace_and_nonready_activation() {
    let (value, _, frame) = frame_fixture(8);
    let intent = scoped_intent(&value, &frame).unwrap();
    let sibling = GraphFrameCheckpoint::new(
        GraphFrameIdentity::new(
            value.key.parent().clone(),
            NodeId::new("other.slot").unwrap(),
            frame.checkpoint().graph().clone(),
        )
        .unwrap(),
        frame.checkpoint().clone(),
    )
    .unwrap();
    assert_eq!(
        intent.validate_for_frame(&value.parent, &sibling, &Schemas::allow(), now()),
        Err(ChildRunAdmissionIntentError::ParentActivationMismatch)
    );
    let ghost = ChildRunKey::new(
        NodeActivation::new(
            frame.checkpoint().head(),
            frame.frame().namespace().clone(),
            NodeId::new("not.ready").unwrap(),
            Digest::sha256("input"),
        ),
        value.key.slot().clone(),
    )
    .unwrap();
    assert_eq!(
        ChildRunAdmissionIntent::for_frame(
            &value.parent,
            &frame,
            ghost,
            value.child,
            value.graph,
            value.state
        ),
        Err(ChildRunAdmissionIntentError::ParentNotReady)
    );
}

#[test]
fn scoped_child_rechecks_clock_deadline_and_every_schema() {
    let (value, _, frame) = frame_fixture(8);
    let intent = scoped_intent(&value, &frame).unwrap();
    assert_eq!(
        intent.validate_for_frame(
            &value.parent,
            &frame,
            &Schemas::allow(),
            "2029-01-01T00:00:00.000000Z".parse().unwrap()
        ),
        Err(ChildRunAdmissionIntentError::ClockBeforeCheckpoint)
    );
    assert_eq!(
        intent.validate_for_frame(
            &value.parent,
            &frame,
            &Schemas::allow(),
            value.child.budget().deadline()
        ),
        Err(ChildRunAdmissionIntentError::DeadlineExpired)
    );
    for reject in 0..3 {
        let schemas = Schemas {
            count: AtomicUsize::new(0),
            reject: Some(reject),
        };
        assert!(
            intent
                .validate_for_frame(&value.parent, &frame, &schemas, now())
                .is_err()
        );
        assert_eq!(schemas.count.load(Ordering::SeqCst), reject + 1);
    }
}

#[test]
fn scoped_reservation_uses_the_root_account_and_blocks_an_additional_root_child() {
    let (mut value, graph, frame) = frame_fixture(8);
    let intent = scoped_intent(&value, &frame).unwrap();
    let root_graph: CompiledGraph = policy_fixture(true).1;
    let account = ChildRunBudgetAccount::new(
        &value.parent,
        &root_graph,
        frame.checkpoint().journal_head().clone(),
        crate::BudgetUsage::zero(),
    )
    .unwrap();
    let reserved = account
        .reserve_for_frame(&intent, &frame, &graph, now())
        .unwrap();
    assert_eq!(reserved.children().len(), 1);
    assert_eq!(
        reserved.entry(intent.key()).unwrap().child(),
        value.child.provenance()
    );
    assert_eq!(
        reserved
            .reserve_for_frame(&intent, &frame, &graph, now())
            .unwrap(),
        reserved
    );
    assert_eq!(
        from_value::<ChildRunBudgetAccount>(to_value(&reserved).unwrap()).unwrap(),
        reserved
    );
    let mut provenance = to_value(value.child.provenance()).unwrap();
    provenance["run_id"] = json!(crate::RunId::generate());
    value.replace_child(
        from_value(provenance).unwrap(),
        value.child.request().clone(),
        value.child.budget_layers().to_vec(),
        value.child.authority().clone(),
    );
    assert!(
        reserved
            .reserve(&value.intent().unwrap(), &root_graph, now())
            .is_err()
    );
    assert_eq!(reserved.children().len(), 1);
}

#[test]
fn scoped_reservation_cannot_widen_root_lifetime_cap_or_use_a_root_graph_alias() {
    let (value, graph, frame) = frame_fixture(9);
    let intent = scoped_intent(&value, &frame).unwrap();
    let root = policy_fixture(true).1;
    let account = ChildRunBudgetAccount::new(
        &value.parent,
        &root,
        frame.checkpoint().journal_head().clone(),
        crate::BudgetUsage::zero(),
    )
    .unwrap();
    assert_eq!(
        account.reserve_for_frame(&intent, &frame, &graph, now()),
        Err(ChildRunBudgetError::ParentMismatch)
    );
    assert_eq!(
        account.reserve_for_frame(&intent, &frame, &root, now()),
        Err(ChildRunBudgetError::ParentMismatch)
    );
    assert_eq!(
        account.reserve(&intent, &graph, now()),
        Err(ChildRunBudgetError::ParentMismatch)
    );
}
