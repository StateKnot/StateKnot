// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    CapabilityName, CapabilityReference, GraphComposition, GraphCompositionError,
    GraphExecutionLimits, GraphNode, GraphReducerReference, GraphRoutes, ReadyNodes,
    SharedStateSubgraph, Superstep, Version,
};
use serde_json::{Value, from_value, json, to_value};

fn agent() -> AgentDescriptor {
    let wire: Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-agent-v1.json")).unwrap();
    from_value(wire["descriptors"]["valid"][0].clone()).unwrap()
}

fn graph(name: &str) -> CompiledGraph {
    let agent = agent();
    let identity = CapabilityIdentity::new(
        agent.metadata().identity().owner().clone(),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    );
    CompiledGraph::compile(
        identity.clone(),
        agent.input_schema().clone(),
        agent.input_schema().clone(),
        agent.input_schema().clone(),
        agent.output_schema().clone(),
        GraphReducerReference::new(identity, Digest::sha256("reducer")),
        ReadyNodes::try_new([NodeId::new("work").unwrap()]).unwrap(),
        [GraphNode::new(
            NodeId::new("work").unwrap(),
            None,
            GraphRoutes::empty(),
            None,
            true,
        )
        .unwrap()],
        GraphExecutionLimits::new(Superstep::new(10).unwrap(), 1).unwrap(),
    )
    .unwrap()
}

fn limits() -> ChildRunTopologyLimits {
    ChildRunTopologyLimits::new(2, 16, 8).unwrap()
}

fn declaration(node: &str, slot: &str) -> ChildRunDeclaration {
    ChildRunDeclaration::new(
        NodeId::new(node).unwrap(),
        ChildRunSlot::new(slot).unwrap(),
        &agent(),
        &graph("leaf"),
    )
    .unwrap()
}

fn policy() -> GraphChildRunPolicy {
    GraphChildRunPolicy::new(limits(), [declaration("work", "analysis")]).unwrap()
}

#[test]
fn topology_limits_validate_every_wire_boundary() {
    for values in [
        (0, 1, 1),
        (33, 1, 1),
        (1, 0, 1),
        (1, 257, 1),
        (1, 1, 0),
        (1, 1, 257),
    ] {
        assert_eq!(
            ChildRunTopologyLimits::new(values.0, values.1, values.2),
            Err(ChildRunPolicyError::InvalidLimits)
        );
        assert!(
            from_value::<ChildRunTopologyLimits>(json!({
                "maximum_descendant_depth": values.0,
                "maximum_children_per_run": values.1,
                "maximum_active_descendants": values.2
            }))
            .is_err()
        );
    }
    let maximum = ChildRunTopologyLimits::new(32, 256, 256).unwrap();
    assert_eq!(
        from_value::<ChildRunTopologyLimits>(to_value(maximum).unwrap()).unwrap(),
        maximum
    );
    let mut wire = to_value(maximum).unwrap();
    wire["unlimited"] = json!(true);
    assert!(from_value::<ChildRunTopologyLimits>(wire).is_err());
}

#[test]
fn policy_sorts_node_owned_slots_and_rejects_duplicates_and_empty() {
    let first = declaration("work", "a");
    let second = declaration("work", "b");
    let other_node = declaration("other", "a");
    let one = GraphChildRunPolicy::new(
        limits(),
        [first.clone(), second.clone(), other_node.clone()],
    )
    .unwrap();
    let two = GraphChildRunPolicy::new(limits(), [other_node, second, first.clone()]).unwrap();
    assert_eq!(one, two);
    assert_eq!(to_value(&one).unwrap(), to_value(&two).unwrap());
    assert_eq!(one.declaration(first.node_id(), first.slot()), Some(&first));
    assert!(
        one.declaration(first.node_id(), &ChildRunSlot::new("A").unwrap())
            .is_none()
    );
    assert_eq!(
        GraphChildRunPolicy::new(limits(), [first.clone(), first]),
        Err(ChildRunPolicyError::DuplicateSlot)
    );
    assert_eq!(
        GraphChildRunPolicy::new(limits(), []),
        Err(ChildRunPolicyError::EmptyDeclarations)
    );
}

#[test]
fn policy_bounds_apply_to_construction_and_deserialization() {
    let base = declaration("work", "a");
    let node_slots = (0..65)
        .map(|index| {
            let mut value = base.clone();
            value.slot = ChildRunSlot::new(format!("slot{index}")).unwrap();
            value
        })
        .collect::<Vec<_>>();
    assert_eq!(
        GraphChildRunPolicy::new(limits(), node_slots.clone()),
        Err(ChildRunPolicyError::TooManyNodeSlots)
    );
    let all_slots = (0..257)
        .map(|index| {
            let mut value = base.clone();
            value.node_id = NodeId::new(format!("node{index}")).unwrap();
            value
        })
        .collect::<Vec<_>>();
    assert_eq!(
        GraphChildRunPolicy::new(limits(), all_slots.clone()),
        Err(ChildRunPolicyError::TooManyDeclarations)
    );
    for declarations in [node_slots, all_slots] {
        let mut wire = to_value(policy()).unwrap();
        wire["declarations"] = to_value(declarations).unwrap();
        assert!(from_value::<GraphChildRunPolicy>(wire).is_err());
    }
}

#[test]
fn policy_roundtrip_schema_and_version_are_closed() {
    let original = policy();
    assert_eq!(
        from_value::<GraphChildRunPolicy>(to_value(&original).unwrap()).unwrap(),
        original
    );
    for field in ["version", "detach"] {
        let mut wire = to_value(&original).unwrap();
        wire[field] = json!(2);
        assert!(from_value::<GraphChildRunPolicy>(wire).is_err());
    }
    let schema = to_value(schemars::schema_for!(GraphChildRunPolicy)).unwrap();
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["properties"]["declarations"]["maxItems"], 256);
    assert_eq!(schema["properties"]["version"]["maximum"], 1);
}

#[test]
fn agent_pin_includes_definition_not_just_identity_and_redacts_content() {
    let original = agent();
    let pin = ChildAgentReference::from_descriptor(&original).unwrap();
    let mut wire = to_value(&original).unwrap();
    wire["budget_limits"]["model_turns"] = json!("1");
    let changed: AgentDescriptor = from_value(wire).unwrap();
    let changed_pin = ChildAgentReference::from_descriptor(&changed).unwrap();
    assert_eq!(pin.identity(), changed_pin.identity());
    assert_ne!(pin.definition_digest(), changed_pin.definition_digest());
    let wire = to_value(&pin).unwrap();
    assert!(wire.get("instructions").is_none());
    assert_eq!(from_value::<ChildAgentReference>(wire).unwrap(), pin);
    assert_eq!(
        pin.definition_digest().to_string(),
        "sha256:3258d63505036aabfa25c745e554afa1e185d85a1585ff94e87b0a96db9c6172"
    );
}

#[test]
fn declared_graph_roundtrips_and_legacy_encoding_remains_unchanged() {
    let original = graph("parent");
    let legacy = original.canonical_definition_bytes().unwrap();
    assert!(to_value(&original).unwrap().get("child_runs").is_none());
    let declared = original.clone().with_child_runs(policy()).unwrap();
    assert_ne!(declared.reference(), original.reference());
    assert_eq!(
        from_value::<CompiledGraph>(to_value(&declared).unwrap()).unwrap(),
        declared
    );
    assert_eq!(original.canonical_definition_bytes().unwrap(), legacy);
    let mut altered = to_value(&declared).unwrap();
    altered["child_runs"]["limits"]["maximum_children_per_run"] = json!(17);
    assert!(from_value::<CompiledGraph>(altered).is_err());
    let mut omitted = to_value(&declared).unwrap();
    omitted.as_object_mut().unwrap().remove("child_runs");
    assert!(from_value::<CompiledGraph>(omitted).is_err());
    assert_eq!(
        declared.definition_digest().to_string(),
        "sha256:585d99d8a6914720a3b744a320dac4a236f55af1050b2c9727298508cddc3034"
    );
}

#[test]
fn declarations_reject_missing_nodes_and_drifted_target_pins() {
    let unknown = GraphChildRunPolicy::new(limits(), [declaration("unknown", "a")]).unwrap();
    assert!(matches!(
        graph("parent").with_child_runs(unknown),
        Err(crate::GraphCompileError::ChildPolicy(
            ChildRunPolicyError::UndeclaredParentNode
        ))
    ));
    let declaration = declaration("work", "a");
    declaration
        .validate_target(&graph("leaf"), limits())
        .unwrap();
    assert_eq!(
        declaration.validate_target(&graph("different"), limits()),
        Err(ChildRunPolicyError::TargetGraphMismatch)
    );
    let mut changed = to_value(&declaration).unwrap();
    changed["input_schema"] = to_value(agent().output_schema()).unwrap();
    let changed: ChildRunDeclaration = from_value(changed).unwrap();
    assert_eq!(
        changed.validate_target(&graph("leaf"), limits()),
        Err(ChildRunPolicyError::TargetSchemaMismatch)
    );
}

#[test]
fn nested_targets_must_strictly_narrow_remaining_depth() {
    let nested = graph("nested").with_child_runs(policy()).unwrap();
    let declaration = ChildRunDeclaration::new(
        NodeId::new("work").unwrap(),
        ChildRunSlot::new("nested").unwrap(),
        &agent(),
        &nested,
    )
    .unwrap();
    assert_eq!(
        declaration.validate_target(&nested, limits()),
        Err(ChildRunPolicyError::DepthNotNarrowed)
    );
    declaration
        .validate_target(&nested, ChildRunTopologyLimits::new(3, 16, 8).unwrap())
        .unwrap();
}

#[test]
fn static_template_rejects_delegation_and_noop_composition_preserves_it() {
    let graph = graph("parent").with_child_runs(policy()).unwrap();
    assert!(matches!(
        SharedStateSubgraph::new(graph.clone()),
        Err(GraphCompositionError::ChildDelegationUnsupported)
    ));
    let composed = GraphComposition::compile(graph.clone(), []).unwrap();
    assert_eq!(composed.graph(), &graph);
}
