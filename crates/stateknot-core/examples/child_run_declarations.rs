// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Compile and restore exact child delegation declarations, without spawning.

use stateknot_core::{
    AgentDescriptor, CapabilityIdentity, CapabilityName, CapabilityReference, ChildRunDeclaration,
    ChildRunSlot, ChildRunTopologyLimits, CompiledGraph, Digest, GraphChildRunPolicy,
    GraphExecutionLimits, GraphNode, GraphReducerReference, GraphRoutes, NodeId, ReadyNodes,
    Superstep, Version,
};
use std::error::Error;

fn graph(name: &str, agent: &AgentDescriptor) -> Result<CompiledGraph, Box<dyn Error>> {
    let identity = CapabilityIdentity::new(
        agent.metadata().identity().owner().clone(),
        CapabilityReference::new(CapabilityName::new(name)?, Version::new(1, 0, 0)),
    );
    Ok(CompiledGraph::compile(
        identity.clone(),
        agent.input_schema().clone(),
        agent.input_schema().clone(),
        agent.input_schema().clone(),
        agent.output_schema().clone(),
        GraphReducerReference::new(identity, Digest::sha256("example-reducer-v1")),
        ReadyNodes::try_new([NodeId::new("work")?])?,
        [GraphNode::new(
            NodeId::new("work")?,
            None,
            GraphRoutes::empty(),
            None,
            true,
        )?],
        GraphExecutionLimits::new(Superstep::new(10)?, 1)?,
    )?)
}

fn main() -> Result<(), Box<dyn Error>> {
    // Public synthetic descriptor for a deterministic offline example. Real
    // applications supply an approved, registry-resolved Agent descriptor.
    let wire: serde_json::Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-agent-v1.json"))?;
    let agent: AgentDescriptor = serde_json::from_value(wire["descriptors"]["valid"][0].clone())?;
    let child = graph("child", &agent)?;
    let node = NodeId::new("work")?;
    let slot = ChildRunSlot::new("analysis")?;
    let policy = GraphChildRunPolicy::new(
        ChildRunTopologyLimits::new(1, 16, 8)?,
        [ChildRunDeclaration::new(
            node.clone(),
            slot.clone(),
            &agent,
            &child,
        )?],
    )?;
    let parent = graph("parent", &agent)?.with_child_runs(policy)?;
    let restored: CompiledGraph = serde_json::from_slice(&serde_json::to_vec(&parent)?)?;
    assert_eq!(restored, parent);
    let policy = restored.child_runs().ok_or("missing policy")?;
    let declaration = policy.declaration(&node, &slot).ok_or("missing slot")?;
    declaration.validate_target(&child, policy.limits())?;
    assert!(
        policy
            .declaration(&node, &ChildRunSlot::new("undeclared")?)
            .is_none()
    );
    assert!(
        declaration
            .validate_target(&graph("substituted-child", &agent)?, policy.limits())
            .is_err()
    );
    println!("parent graph: {}", parent.definition_digest());
    println!("pinned Agent: {}", declaration.agent().definition_digest());
    println!("declared target restored; unknown slots and substituted graphs refused");
    println!("no child Run, reservation, or provider call was created");
    Ok(())
}
