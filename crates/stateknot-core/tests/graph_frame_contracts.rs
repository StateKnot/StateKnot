// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Experimental RFC-0022 data integrity, not runtime or SQL qualification.
use proptest::prelude::*;
use serde_json::{Value, json};
use stateknot_core::*;

#[path = "support/canonical_reference.rs"]
mod canonical_reference;
#[path = "support/graph_frames.rs"]
mod data;
use canonical_reference::canonical_reference;

fn checksum(domain: &[u8], value: &Value) -> Digest {
    let mut bytes = domain.to_vec();
    bytes.extend_from_slice(canonical_reference(value).as_bytes());
    Digest::sha256(bytes)
}

fn bound(slot: &str) -> GraphFrameCheckpoint {
    let parent = data::checkpoint(
        data::graph("root"),
        1,
        "tenant-frame",
        json!({"input":true}),
    );
    GraphFrameCheckpoint::new(
        data::frame(&parent, slot),
        data::checkpoint(
            data::graph("child"),
            2,
            "tenant-frame",
            json!({"input":true}),
        ),
    )
    .unwrap()
}

fn fence(checkpoint: &Checkpoint, epoch: u64) -> RunFence {
    RunFence::new(
        checkpoint.tenant_id().clone(),
        checkpoint.run_id(),
        AttemptId::generate(),
        FencingEpoch::new(epoch).unwrap(),
    )
}

fn result(activation: NodeActivation, fence: RunFence, ordinal: u64) -> PendingNodeResult {
    let tenant = activation.base_checkpoint().tenant_id().clone();
    PendingNodeResult::commit(
        PendingNodeResultIntent::new(
            activation,
            NodeStateChange::Unchanged,
            NodeControl::Continue,
            NodeInvocationBindings::empty(),
        )
        .unwrap(),
        fence,
        data::journal(ordinal, tenant.as_str()),
    )
    .unwrap()
}

fn executing(activation: NodeActivation, fence: RunFence, ordinal: u64) -> NodeAttempt {
    let tenant = activation.base_checkpoint().tenant_id().clone();
    NodeAttempt::executing(
        NodeAttemptStart::new(
            activation,
            AttemptId::generate(),
            fence,
            data::journal(ordinal, tenant.as_str()),
        )
        .unwrap(),
    )
}

#[test]
fn scoped_recovery_retains_the_frame_and_derives_the_exact_ready_set() {
    let checkpoint = bound("slot.a");
    let owner = fence(checkpoint.checkpoint(), 1);
    let planner = ReadyNodeRecoveryPlanner::for_frame(checkpoint.clone(), owner.clone()).unwrap();
    assert_eq!(
        planner.activations(),
        [checkpoint.activation("call".parse().unwrap()).unwrap()]
    );
    let observed = data::journal(2, "tenant-frame");
    let plan = planner
        .finish(observed.clone(), observed.recorded_at())
        .unwrap();
    assert_eq!(plan.frame(), Some(checkpoint.frame()));
    assert_eq!(plan.checkpoint(), checkpoint.checkpoint());
    assert_eq!(plan.fence(), &owner);
    assert_eq!(
        plan.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::FirstAttempt)
    );
    assert!(!plan.is_barrier_ready());
    assert!(plan.completed_result_heads().is_none());
    let root = ReadyNodeRecoveryPlanner::new(checkpoint.checkpoint().clone(), owner)
        .unwrap()
        .finish(observed.clone(), observed.recorded_at())
        .unwrap();
    assert!(root.frame().is_none());
    assert_ne!(root.nodes()[0].activation(), plan.nodes()[0].activation());
}

#[test]
fn recovery_rejects_sibling_and_root_evidence_at_the_same_checkpoint() {
    let checkpoint = bound("slot.a");
    let sibling = GraphFrameCheckpoint::new(
        bound("slot.b").frame().clone(),
        checkpoint.checkpoint().clone(),
    )
    .unwrap();
    let owner = fence(checkpoint.checkpoint(), 1);
    let correct = checkpoint.activation("call".parse().unwrap()).unwrap();
    let root =
        NodeActivation::for_ready_root(checkpoint.checkpoint(), "call".parse().unwrap()).unwrap();
    let other = sibling.activation("call".parse().unwrap()).unwrap();
    for activation in [root, other] {
        let mut planner =
            ReadyNodeRecoveryPlanner::for_frame(checkpoint.clone(), owner.clone()).unwrap();
        assert!(matches!(
            planner.observe_result(&result(activation.clone(), owner.clone(), 4)),
            Err(ReadyNodeRecoveryError::UnexpectedResult { .. })
        ));
        assert!(matches!(
            planner.observe_attempt(&executing(activation, owner.clone(), 3)),
            Err(ReadyNodeRecoveryError::UnexpectedAttempt { .. })
        ));
    }
    let mut root =
        ReadyNodeRecoveryPlanner::new(checkpoint.checkpoint().clone(), owner.clone()).unwrap();
    assert!(matches!(
        root.observe_result(&result(correct.clone(), owner.clone(), 4)),
        Err(ReadyNodeRecoveryError::UnexpectedResult { .. })
    ));
    assert!(matches!(
        root.observe_attempt(&executing(correct, owner, 3)),
        Err(ReadyNodeRecoveryError::UnexpectedAttempt { .. })
    ));
}

#[test]
fn scoped_recovery_distinguishes_in_flight_work_from_takeover() {
    let checkpoint = bound("slot.a");
    let owner = fence(checkpoint.checkpoint(), 1);
    let attempt = executing(
        checkpoint.activation("call".parse().unwrap()).unwrap(),
        owner.clone(),
        3,
    );
    let observed = data::journal(3, "tenant-frame");
    let mut same = ReadyNodeRecoveryPlanner::for_frame(checkpoint.clone(), owner).unwrap();
    same.observe_attempt(&attempt).unwrap();
    let plan = same
        .finish(observed.clone(), observed.recorded_at())
        .unwrap();
    assert_eq!(plan.nodes()[0].kind(), RecoveryNodeKind::InFlight);
    assert_eq!(
        plan.nodes()[0].in_flight_attempt(),
        Some(&attempt.start().head())
    );
    let mut takeover =
        ReadyNodeRecoveryPlanner::for_frame(checkpoint.clone(), fence(checkpoint.checkpoint(), 2))
            .unwrap();
    takeover.observe_attempt(&attempt).unwrap();
    let plan = takeover
        .finish(observed.clone(), observed.recorded_at())
        .unwrap();
    assert_eq!(
        plan.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::SupersededAttempt)
    );
    assert_eq!(plan.frame(), Some(checkpoint.frame()));
}

#[test]
fn scoped_recovery_reuses_success_and_keeps_the_legacy_root_barrier_closed() {
    let checkpoint = bound("slot.a");
    let owner = fence(checkpoint.checkpoint(), 1);
    let activation = checkpoint.activation("call".parse().unwrap()).unwrap();
    let start = executing(activation.clone(), owner.clone(), 3);
    let result = result(activation, owner, 4);
    let completion =
        NodeAttemptCompletion::succeed(start.start(), result.head(), BudgetUsage::zero()).unwrap();
    let attempt = NodeAttempt::restore(start.start().clone(), Some(completion)).unwrap();
    let mut planner =
        ReadyNodeRecoveryPlanner::for_frame(checkpoint.clone(), fence(checkpoint.checkpoint(), 2))
            .unwrap();
    planner.observe_result(&result).unwrap();
    planner.observe_attempt(&attempt).unwrap();
    let observed = data::journal(4, "tenant-frame");
    let plan = planner
        .finish(observed.clone(), observed.recorded_at())
        .unwrap();
    assert_eq!(plan.nodes()[0].kind(), RecoveryNodeKind::Completed);
    assert!(plan.nodes()[0].dispatch_reason().is_none());
    assert_eq!(
        plan.completed_result_heads().unwrap().as_ref(),
        [result.head()]
    );
    assert!(matches!(
        plan.barrier_result_heads(),
        Err(BarrierResultHeadsError::NestedGraphNamespace { .. })
    ));
}

#[test]
fn committed_current_source_vectors_are_reproduced_by_real_constructors() {
    let wire: Value =
        serde_json::from_str(include_str!("fixtures/core-graph-frame-v1.json")).unwrap();
    let first = bound("slot.a");
    let next = GraphFrameCheckpoint::new(
        first.frame().clone(),
        data::successor(first.checkpoint(), 3, json!({"input":false})),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(first.frame()).unwrap(),
        wire["identity"]
    );
    assert_eq!(serde_json::to_value(&first).unwrap(), wire["checkpoint"]);
    assert_eq!(serde_json::to_value(first.head()).unwrap(), wire["head"]);
    assert_eq!(serde_json::to_value(&next).unwrap(), wire["successor"]);
    first.verify_successor(&next).unwrap();
}

#[test]
fn identity_rejects_another_owner_or_state_schema() {
    let parent = data::checkpoint(data::graph("root"), 1, "tenant-frame", json!({}));
    let origin = NodeActivation::for_ready_root(&parent, NodeId::new("call").unwrap()).unwrap();
    let target = data::graph("child");
    let other_owner = GraphReference::new(
        CapabilityIdentity::new(
            PrincipalIdentity::new(
                "https://issuer.example.com/other".parse().unwrap(),
                "frame-contract".parse().unwrap(),
            ),
            target.identity().capability().clone(),
        ),
        target.definition_digest(),
        target.state_schema().clone(),
    );
    assert_eq!(
        GraphFrameIdentity::new(origin.clone(), NodeId::new("slot").unwrap(), other_owner),
        Err(GraphFrameError::OwnerMismatch)
    );
    let other_schema = GraphReference::new(
        target.identity().clone(),
        target.definition_digest(),
        SchemaReference::new(
            target.state_schema().id().clone(),
            Version::new(1, 0, 0),
            Digest::sha256(b"changed schema"),
        ),
    );
    assert_eq!(
        GraphFrameIdentity::new(origin, NodeId::new("slot").unwrap(), other_schema),
        Err(GraphFrameError::StateSchemaMismatch)
    );
}

#[test]
fn ancestry_accepts_only_full_lowercase_digest_segments() {
    let parent = data::checkpoint(data::graph("root"), 1, "tenant-frame", json!({}));
    for namespace in [
        "arbitrary".to_owned(),
        "a".repeat(63),
        "A".repeat(64),
        format!("{}/invalid", "a".repeat(64)),
    ] {
        let origin = NodeActivation::new(
            parent.head(),
            GraphNamespace::new(namespace).unwrap(),
            NodeId::new("call").unwrap(),
            Digest::sha256(b"input"),
        );
        assert_eq!(
            GraphFrameIdentity::new(origin, NodeId::new("slot").unwrap(), data::graph("child")),
            Err(GraphFrameError::InvalidNamespace)
        );
    }
}

#[test]
fn real_scoped_ready_activations_form_seven_levels_and_reject_eight() {
    let parent = data::checkpoint(data::graph("root"), 1, "tenant-frame", json!({}));
    let mut origin = NodeActivation::for_ready_root(&parent, NodeId::new("call").unwrap()).unwrap();
    for depth in 1..=GraphFrameIdentity::MAX_DEPTH {
        let identity =
            GraphFrameIdentity::new(origin, NodeId::new("slot").unwrap(), data::graph("child"))
                .unwrap();
        assert_eq!(identity.namespace().as_str().len(), depth * 65 - 1);
        let checkpoint = data::checkpoint(
            data::graph("child"),
            u64::try_from(depth).unwrap() + 1,
            "tenant-frame",
            json!({}),
        );
        let bound = GraphFrameCheckpoint::new(identity, checkpoint).unwrap();
        origin = bound.activation(NodeId::new("call").unwrap()).unwrap();
    }
    assert_eq!(
        GraphFrameIdentity::new(origin, NodeId::new("slot").unwrap(), data::graph("child")),
        Err(GraphFrameError::DepthExceeded)
    );
}

#[test]
fn changing_target_pin_conflicts_at_the_same_namespace() {
    let bound = bound("slot.a");
    let identity = bound.frame();
    let target = GraphReference::new(
        identity.target().identity().clone(),
        Digest::sha256(b"substituted implementation"),
        identity.target().state_schema().clone(),
    );
    let changed =
        GraphFrameIdentity::new(identity.origin().clone(), identity.slot().clone(), target)
            .unwrap();
    assert_eq!(identity.namespace(), changed.namespace());
    assert_ne!(identity.digest(), changed.digest());
    assert_eq!(
        GraphFrameCheckpoint::new(changed, bound.checkpoint().clone()),
        Err(GraphFrameError::CheckpointGraphMismatch)
    );
}

#[test]
fn bound_checkpoint_rejects_tenant_run_graph_and_journal_substitution() {
    let bound = bound("slot.a");
    for (graph, ordinal, tenant, expected) in [
        (
            data::graph("child"),
            2,
            "other-tenant",
            GraphFrameError::CheckpointScopeMismatch,
        ),
        (
            data::graph("other"),
            2,
            "tenant-frame",
            GraphFrameError::CheckpointGraphMismatch,
        ),
        (
            data::graph("child"),
            1,
            "tenant-frame",
            GraphFrameError::JournalOrder,
        ),
    ] {
        assert_eq!(
            GraphFrameCheckpoint::new(
                bound.frame().clone(),
                data::checkpoint(graph, ordinal, tenant, json!({}))
            ),
            Err(expected)
        );
    }
    // Reconstruct a valid checkpoint with another Run before testing the binding.
    let head = data::journal(2, "tenant-frame");
    let other_run = "01912345-6789-7abc-8def-0123456789a2".parse().unwrap();
    let head = JournalHead::new(
        head.tenant_id().clone(),
        other_run,
        head.sequence(),
        head.event_id(),
        head.recorded_at(),
        head.digest(),
    );
    let write = CheckpointWrite::initial(
        head.tenant_id().clone(),
        other_run,
        bound.checkpoint().checkpoint_id(),
        data::graph("child"),
        bound.checkpoint().state().clone(),
        bound.checkpoint().ready_nodes().clone(),
    )
    .unwrap();
    assert_eq!(
        GraphFrameCheckpoint::new(
            bound.frame().clone(),
            Checkpoint::commit(write, head).unwrap()
        ),
        Err(GraphFrameError::CheckpointScopeMismatch)
    );
    let head = data::journal(2, "tenant-frame");
    let earlier = JournalHead::new(
        head.tenant_id().clone(),
        head.run_id(),
        head.sequence(),
        head.event_id(),
        Timestamp::from_unix_micros(0).unwrap(),
        head.digest(),
    );
    let checkpoint = Checkpoint::commit(bound.checkpoint().write_intent(), earlier).unwrap();
    assert_eq!(
        GraphFrameCheckpoint::new(bound.frame().clone(), checkpoint),
        Err(GraphFrameError::JournalOrder)
    );
}

#[test]
fn successor_verification_rejects_a_sibling_frame_and_a_skipped_predecessor() {
    let first = bound("slot.a");
    let next_cp = data::successor(first.checkpoint(), 3, json!({}));
    let next = GraphFrameCheckpoint::new(first.frame().clone(), next_cp.clone()).unwrap();
    first.verify_successor(&next).unwrap();
    let sibling = GraphFrameCheckpoint::new(bound("slot.b").frame().clone(), next_cp).unwrap();
    assert_eq!(
        first.verify_successor(&sibling),
        Err(GraphFrameError::FrameMismatch)
    );
    let third = GraphFrameCheckpoint::new(
        first.frame().clone(),
        data::successor(next.checkpoint(), 4, json!({})),
    )
    .unwrap();
    assert_eq!(
        first.verify_successor(&third),
        Err(GraphFrameError::PredecessorMismatch)
    );
}

#[test]
fn ready_activation_binds_scope_without_changing_root_derivation() {
    let bound = bound("slot.a");
    let node_id = NodeId::new("call").unwrap();
    let activation = bound.activation(node_id.clone()).unwrap();
    let expected = checksum(
        b"stateknot-ready-node-input-v1\0",
        &json!({"base_checkpoint_digest":bound.checkpoint().digest(), "graph_namespace":bound.frame().namespace(), "node_id":node_id}),
    );
    assert_eq!(activation.input_digest(), expected);
    assert_eq!(activation.graph_namespace(), bound.frame().namespace());
    assert_ne!(
        activation,
        NodeActivation::for_ready_root(bound.checkpoint(), node_id).unwrap()
    );
    assert_eq!(
        bound.activation(NodeId::new("absent").unwrap()),
        Err(GraphFrameError::NodeNotReady {
            node_id: NodeId::new("absent").unwrap()
        })
    );
}

#[test]
fn restored_bindings_reject_altered_identity_head_and_state() {
    let bound = bound("slot.a");
    for field in ["namespace", "slot", "digest"] {
        let mut wire = serde_json::to_value(bound.frame()).unwrap();
        wire[field] = if field == "digest" {
            json!(Digest::sha256(b"corrupt"))
        } else {
            json!("replaced")
        };
        assert!(serde_json::from_value::<GraphFrameIdentity>(wire).is_err());
    }
    let mut wire = serde_json::to_value(&bound).unwrap();
    wire["checkpoint"]["state"]["data"] = json!({"substituted":true});
    assert!(serde_json::from_value::<GraphFrameCheckpoint>(wire).is_err());
    let mut wire = serde_json::to_value(bound.head()).unwrap();
    wire["checkpoint"]["digest"] = json!(Digest::sha256(b"substituted"));
    assert!(serde_json::from_value::<GraphFrameCheckpointHead>(wire).is_err());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn independent_identity_preimages_and_changed_slots_are_distinct(slot in "[a-z][a-z0-9]{0,30}", ordinal in 1_u64..100_000) {
        let parent = data::checkpoint(data::graph("root"), ordinal, "tenant-frame", json!({}));
        let identity = data::frame(&parent, &slot);
        let segment = checksum(b"stateknot-graph-frame-namespace-v1\0", &json!({"origin":identity.origin(), "slot":identity.slot()}));
        prop_assert_eq!(identity.namespace().as_str(), &segment.to_string()[7..]);
        prop_assert_eq!(identity.digest(), checksum(b"stateknot-graph-frame-identity-v1\0", &json!({"origin":identity.origin(), "slot":identity.slot(), "target":identity.target(), "namespace":identity.namespace()})));
        let other = data::frame(&parent, &format!("{slot}.other"));
        prop_assert_ne!(identity.namespace(), other.namespace());
        prop_assert_ne!(identity.digest(), other.digest());
        prop_assert_eq!(&identity, &serde_json::from_slice::<GraphFrameIdentity>(&serde_json::to_vec(&identity).unwrap()).unwrap());
    }
    #[test]
    fn independent_scoped_checkpoint_binding_survives_a_state_chain(values in prop::collection::vec(any::<i32>(), 1..10)) {
        let mut first = bound("slot.a");
        for (index, value) in values.into_iter().enumerate() {
            let cp = data::successor(first.checkpoint(), u64::try_from(index).unwrap() + 3, json!({"😀":value,"\u{e000}":true}));
            let next = GraphFrameCheckpoint::new(first.frame().clone(), cp.clone()).unwrap();
            first.verify_successor(&next).unwrap();
            prop_assert_eq!(next.digest(), checksum(b"stateknot-graph-frame-checkpoint-v1\0", &json!({"frame_identity":next.frame().digest(),"checkpoint_head":cp.head()})));
            let sibling = GraphFrameCheckpoint::new(bound("slot.b").frame().clone(), cp).unwrap();
            prop_assert_ne!(next.digest(), sibling.digest());
            let restored: GraphFrameCheckpoint = serde_json::from_slice(&serde_json::to_vec(&next).unwrap()).unwrap();
            prop_assert_eq!(&restored, &next);
            prop_assert_eq!(&restored.head(), &serde_json::from_value::<GraphFrameCheckpointHead>(serde_json::to_value(next.head()).unwrap()).unwrap());
            first = next;
        }
    }
}
