// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Compiles a minimal schema-pinned graph and prints its canonical digest.

use std::error::Error;

use stateknot_core::{
    CapabilityIdentity, CapabilityName, CapabilityReference, CompiledGraph, Digest,
    GraphExecutionLimits, GraphNode, GraphReducerReference, GraphRoutes, IssuerId, NodeId,
    PrincipalIdentity, ReadyNodes, SchemaId, SchemaReference, SubjectId, Superstep, Version,
};

fn capability(owner: &PrincipalIdentity, name: &str) -> Result<CapabilityIdentity, Box<dyn Error>> {
    Ok(CapabilityIdentity::new(
        owner.clone(),
        CapabilityReference::new(CapabilityName::new(name)?, Version::new(1, 0, 0)),
    ))
}

fn schema(name: &str) -> Result<SchemaReference, Box<dyn Error>> {
    Ok(SchemaReference::new(
        format!("https://schemas.example.com/orders/{name}/1.0.0").parse::<SchemaId>()?,
        Version::new(1, 0, 0),
        Digest::sha256(format!("orders:{name}:schema:v1")),
    ))
}

fn ready(nodes: &[&str]) -> Result<ReadyNodes, Box<dyn Error>> {
    Ok(ReadyNodes::try_new(
        nodes
            .iter()
            .map(|node| NodeId::new(*node))
            .collect::<Result<Vec<_>, _>>()?,
    )?)
}

fn main() -> Result<(), Box<dyn Error>> {
    let owner = PrincipalIdentity::new(
        "https://issuer.example.com/stateknot".parse::<IssuerId>()?,
        "orders-service".parse::<SubjectId>()?,
    );
    let finish = GraphNode::new(
        NodeId::new("finish")?,
        None,
        GraphRoutes::empty(),
        None,
        true,
    )?;
    let validate = GraphNode::new(
        NodeId::new("validate")?,
        Some(ready(&["finish"])?),
        GraphRoutes::empty(),
        None,
        false,
    )?;
    let graph = CompiledGraph::compile(
        capability(&owner, "orders.workflow")?,
        schema("input")?,
        schema("state")?,
        schema("update")?,
        schema("output")?,
        GraphReducerReference::new(
            capability(&owner, "orders.reducer")?,
            Digest::sha256(b"orders-reducer-v1"),
        ),
        ready(&["validate"])?,
        [finish, validate],
        GraphExecutionLimits::new(Superstep::new(128)?, 4)?,
    )?;

    assert!(graph.node(&NodeId::new("validate")?).is_some());
    println!("{}", graph.definition_digest());
    Ok(())
}
