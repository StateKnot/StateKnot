// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real compiled call declarations with synthetic, explicitly pinned schemas.
use stateknot_core::*;

pub(crate) fn identity(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.com/frame-calls".parse().unwrap(),
            "frame-call-contract".parse().unwrap(),
        ),
        CapabilityReference::new(name.parse().unwrap(), Version::new(1, 0, 0)),
    )
}
pub(crate) fn schema() -> SchemaReference {
    SchemaReference::new(
        "https://schemas.example.com/frame-call-state/1.0.0"
            .parse()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(b"synthetic frame call state schema"),
    )
}
pub(crate) fn ready(node: &str) -> ReadyNodes {
    ReadyNodes::try_new([NodeId::new(node).unwrap()]).unwrap()
}
pub(crate) fn child() -> CompiledGraph {
    let schema = schema();
    CompiledGraph::compile(
        identity("child"),
        schema.clone(),
        schema.clone(),
        schema.clone(),
        schema,
        GraphReducerReference::new(identity("reducer"), Digest::sha256(b"frame-call-reducer")),
        ready("finish"),
        [GraphNode::new(
            NodeId::new("finish").unwrap(),
            None,
            GraphRoutes::default(),
            None,
            true,
        )
        .unwrap()],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap()
}
pub(crate) fn parent(parallelism: u16) -> CompiledGraph {
    let schema = schema();
    CompiledGraph::compile(
        identity("parent"),
        schema.clone(),
        schema.clone(),
        schema.clone(),
        schema,
        GraphReducerReference::new(identity("reducer"), Digest::sha256(b"frame-call-reducer")),
        ready("call"),
        [
            GraphNode::new(
                NodeId::new("call").unwrap(),
                None,
                GraphRoutes::try_new([GraphRoute::new(
                    RouteId::new("return").unwrap(),
                    ready("finish"),
                )
                .unwrap()])
                .unwrap(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("finish").unwrap(),
                None,
                GraphRoutes::default(),
                None,
                true,
            )
            .unwrap(),
        ],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), parallelism).unwrap(),
    )
    .unwrap()
}
pub(crate) fn call(node: &str, slot: &str) -> GraphFrameCall {
    GraphFrameCall::new(
        NodeId::new(node).unwrap(),
        NodeId::new(slot).unwrap(),
        &child(),
        RouteId::new("return").unwrap(),
    )
    .unwrap()
}
pub(crate) fn policy() -> GraphFrameCallPolicy {
    GraphFrameCallPolicy::new(7, 4096, [call("call", "slot.a")]).unwrap()
}
