// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Actual compiled graph and scoped barrier constructors; synthetic schema pins.
use serde_json::json;
use stateknot_core::*;

pub(crate) fn graph() -> CompiledGraph {
    let identity = |name: &str| {
        CapabilityIdentity::new(
            PrincipalIdentity::new(
                "https://issuer.example.com/frame-barriers".parse().unwrap(),
                "frame-barrier-contract".parse().unwrap(),
            ),
            CapabilityReference::new(name.parse().unwrap(), Version::new(1, 0, 0)),
        )
    };
    let schema = SchemaReference::new(
        "https://schemas.example.com/frame-barrier-state/1.0.0"
            .parse()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(b"synthetic frame barrier object schema"),
    );
    let finish = || ReadyNodes::try_new(["finish".parse().unwrap()]).unwrap();
    CompiledGraph::compile(
        identity("child"),
        schema.clone(),
        schema.clone(),
        schema.clone(),
        schema,
        GraphReducerReference::new(identity("reducer"), Digest::sha256(b"frame-barrier-sum-v1")),
        ReadyNodes::try_new(["alpha".parse().unwrap(), "beta".parse().unwrap()]).unwrap(),
        [
            GraphNode::new(
                "alpha".parse().unwrap(),
                Some(finish()),
                GraphRoutes::default(),
                Some(finish()),
                false,
            )
            .unwrap(),
            GraphNode::new(
                "beta".parse().unwrap(),
                Some(finish()),
                GraphRoutes::default(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                "finish".parse().unwrap(),
                None,
                GraphRoutes::default(),
                None,
                true,
            )
            .unwrap(),
        ],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 2).unwrap(),
    )
    .unwrap()
}

pub(crate) fn journal(ordinal: u64) -> JournalHead {
    JournalHead::new(
        "tenant-barrier".parse().unwrap(),
        "01912345-6789-7abc-8def-0123456789a1".parse().unwrap(),
        JournalSequence::new(ordinal).unwrap(),
        format!("01912345-6789-7abc-8def-{ordinal:012x}")
            .parse()
            .unwrap(),
        Timestamp::from_unix_micros(i64::try_from(ordinal).unwrap() * 1_000_000).unwrap(),
        Digest::sha256(ordinal.to_be_bytes()),
    )
}

pub(crate) fn frame(slot: &str) -> GraphFrameCheckpoint {
    let graph = graph();
    let state = CheckpointState::new(
        graph.state_schema().clone(),
        BoundedJson::try_from_value(json!({"total":0})).unwrap(),
    )
    .unwrap();
    let initial = |ordinal, ready| {
        Checkpoint::commit(
            CheckpointWrite::initial(
                journal(ordinal).tenant_id().clone(),
                journal(ordinal).run_id(),
                journal(ordinal).event_id().to_string().parse().unwrap(),
                graph.reference(),
                state.clone(),
                ready,
            )
            .unwrap(),
            journal(ordinal),
        )
        .unwrap()
    };
    // Caller data is not a registry/admission proof; its exact ready activation
    // seeds the experimental frame binding used by the pure planner.
    let parent = initial(1, ReadyNodes::try_new(["alpha".parse().unwrap()]).unwrap());
    let identity = GraphFrameIdentity::new(
        NodeActivation::for_ready_root(&parent, "alpha".parse().unwrap()).unwrap(),
        slot.parse().unwrap(),
        graph.reference(),
    )
    .unwrap();
    GraphFrameCheckpoint::new(identity, initial(2, graph.entry_nodes().clone())).unwrap()
}

pub(crate) fn result(
    activation: NodeActivation,
    control: NodeControl,
    amount: Option<u64>,
    ordinal: u64,
) -> PendingNodeResult {
    let state_change = amount.map_or(NodeStateChange::Unchanged, |amount| {
        NodeStateChange::Update {
            update: NodeStateUpdate::new(
                graph().update_schema().clone(),
                BoundedJson::try_from_value(json!({"amount":amount})).unwrap(),
            )
            .unwrap(),
        }
    });
    let fence = RunFence::new(
        activation.base_checkpoint().tenant_id().clone(),
        activation.base_checkpoint().run_id(),
        "01912345-6789-7abc-8def-0123456789f1".parse().unwrap(),
        FencingEpoch::new(1).unwrap(),
    );
    PendingNodeResult::commit(
        PendingNodeResultIntent::new(
            activation,
            state_change,
            control,
            NodeInvocationBindings::empty(),
        )
        .unwrap(),
        fence,
        journal(ordinal),
    )
    .unwrap()
}

pub(crate) fn results(base: &GraphFrameCheckpoint) -> Vec<PendingNodeResult> {
    [("alpha", 2, 3), ("beta", 3, 4)]
        .into_iter()
        .map(|(node, amount, ordinal)| {
            result(
                base.activation(node.parse().unwrap()).unwrap(),
                NodeControl::Continue,
                Some(amount),
                ordinal,
            )
        })
        .collect()
}

pub(crate) fn successor(base: &GraphFrameCheckpoint) -> CheckpointWrite {
    CheckpointWrite::successor(
        "01912345-6789-7abc-8def-0123456789b1".parse().unwrap(),
        base.checkpoint(),
        CheckpointState::new(
            base.checkpoint().graph().state_schema().clone(),
            BoundedJson::try_from_value(json!({"total":5})).unwrap(),
        )
        .unwrap(),
        ReadyNodes::try_new(["finish".parse().unwrap()]).unwrap(),
    )
    .unwrap()
}

pub(crate) fn barrier(base: &GraphFrameCheckpoint) -> GraphFrameBarrier {
    GraphFrameBarrier::new(
        base,
        successor(base),
        results(base).into_iter().rev().map(|result| result.head()),
    )
    .unwrap()
}
