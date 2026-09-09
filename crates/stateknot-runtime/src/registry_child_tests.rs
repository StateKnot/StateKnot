// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::JsonSchemaRegistryBuilder;
use serde_json::{Value, json};
use stateknot_core::{
    AgentDescriptor, BoundedJson, CapabilityIdentity, CapabilityName, CapabilityReference,
    ChildRunDeclaration, ChildRunSlot, ChildRunTopologyLimits, Digest, GraphChildRunPolicy,
    GraphExecutionLimits, GraphNode, GraphReducerError, GraphReducerInput, GraphRoutes,
    PrincipalIdentity, ReadyNodes, SchemaReference, Superstep, Version,
};

fn identity(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://example.com".parse().unwrap(),
            "child-registry".parse().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}

fn fixture() -> (JsonSchemaRegistry, CompiledGraph, AgentDescriptor) {
    fixture_with_join_schema(false)
}

fn fixture_with_join_schema(
    join_schema: bool,
) -> (JsonSchemaRegistry, CompiledGraph, AgentDescriptor) {
    let id = "https://example.com/child-value/1.0.0";
    let document =
        json!({"$id":id,"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"});
    let schema = SchemaReference::new(
        id.parse().unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(serde_json_canonicalizer::to_vec(&document).unwrap()),
    );
    let mut schemas = JsonSchemaRegistryBuilder::default();
    schemas.register(schema.clone(), document).unwrap();
    if join_schema {
        crate::register_standard_child_join_event_schema(&mut schemas).unwrap();
    }
    let graph = CompiledGraph::compile(
        identity("leaf"),
        schema.clone(),
        schema.clone(),
        schema.clone(),
        schema.clone(),
        GraphReducerReference::new(identity("reducer"), Digest::sha256("reducer")),
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
    .unwrap();
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-v1.json"
    ))
    .unwrap();
    let mut agent = fixture["descriptors"]["valid"][0].clone();
    agent["input_schema"] = serde_json::to_value(&schema).unwrap();
    agent["output_schema"] = serde_json::to_value(&schema).unwrap();
    (
        schemas.build().unwrap(),
        graph,
        serde_json::from_value(agent).unwrap(),
    )
}

fn parent(
    name: &str,
    target: &CompiledGraph,
    agent: &AgentDescriptor,
    depth: u16,
) -> CompiledGraph {
    CompiledGraph::compile(
        identity(name),
        target.input_schema().clone(),
        target.state_schema().clone(),
        target.update_schema().clone(),
        target.output_schema().clone(),
        target.reducer().clone(),
        target.entry_nodes().clone(),
        target.nodes().iter().cloned(),
        target.limits(),
    )
    .unwrap()
    .with_child_runs(
        GraphChildRunPolicy::new(
            ChildRunTopologyLimits::new(depth, 16, 8).unwrap(),
            [ChildRunDeclaration::new(
                NodeId::new("work").unwrap(),
                ChildRunSlot::new("analysis").unwrap(),
                agent,
                target,
            )
            .unwrap()],
        )
        .unwrap(),
    )
    .unwrap()
}

struct Reducer(GraphReducerReference);
impl GraphReducer for Reducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.0
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        _: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        Ok(state.clone())
    }
}
struct Executor(GraphReference, NodeId);
impl GraphNodeExecutor for Executor {
    fn graph(&self) -> &GraphReference {
        &self.0
    }
    fn node_id(&self) -> &NodeId {
        &self.1
    }
    fn execute(
        &self,
        _: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async { panic!("registry validation must not dispatch executors") })
    }
}

#[test]
fn registry_freezes_nested_child_closure_after_roundtrip() {
    let (schemas, leaf, agent) = fixture();
    let parent = parent("parent", &leaf, &agent, 1);
    let root = self::parent("root", &parent, &agent, 2);
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas);
    registry
        .register_reducer(Arc::new(Reducer(leaf.reducer().clone())))
        .unwrap();
    for graph in [&root, &parent, &leaf] {
        let restored: CompiledGraph =
            serde_json::from_slice(&serde_json::to_vec(graph).unwrap()).unwrap();
        registry.register_graph(restored).unwrap();
        registry
            .register_node(Arc::new(Executor(
                graph.reference(),
                NodeId::new("work").unwrap(),
            )))
            .unwrap();
    }
    let registry = registry.build().unwrap();
    assert_eq!(registry.len(), 3);
    assert_eq!(registry.resolve(&root.reference()).unwrap().graph(), &root);
}

#[test]
fn registry_refuses_missing_or_depth_widened_child_targets() {
    let (schemas, leaf, agent) = fixture();
    let parent = parent("parent", &leaf, &agent, 1);
    let mut missing = ExecutableGraphRegistryBuilder::new(schemas.clone());
    missing.register_graph(parent.clone()).unwrap();
    assert!(matches!(
        missing.build(),
        Err(ExecutableGraphRegistryError::MissingChildGraph { .. })
    ));
    let root = self::parent("root", &parent, &agent, 1);
    let mut widened = ExecutableGraphRegistryBuilder::new(schemas);
    for graph in [leaf, parent, root] {
        widened.register_graph(graph).unwrap();
    }
    assert!(matches!(
        widened.build(),
        Err(ExecutableGraphRegistryError::ChildPolicy {
            source: ChildRunPolicyError::DepthNotNarrowed,
            ..
        })
    ));
}

#[test]
fn registry_refuses_same_target_with_drifted_io_schema() {
    let (schemas, leaf, agent) = fixture();
    let parent = parent("parent", &leaf, &agent, 1);
    let mut policy = serde_json::to_value(parent.child_runs().unwrap()).unwrap();
    policy["declarations"][0]["output_schema"]["digest"] =
        json!(Digest::sha256("different-schema"));
    let changed = parent
        .with_child_runs(serde_json::from_value(policy).unwrap())
        .unwrap();
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas);
    registry.register_graph(leaf).unwrap();
    registry.register_graph(changed).unwrap();
    assert!(matches!(
        registry.build(),
        Err(ExecutableGraphRegistryError::ChildPolicy {
            source: ChildRunPolicyError::TargetSchemaMismatch,
            ..
        })
    ));
}

struct JoinExecutor(Executor, GraphNodeScheduling);
impl GraphNodeExecutor for JoinExecutor {
    fn graph(&self) -> &GraphReference {
        self.0.graph()
    }
    fn node_id(&self) -> &NodeId {
        self.0.node_id()
    }
    fn scheduling(&self) -> GraphNodeScheduling {
        self.1
    }
    fn supports_child_join(&self) -> bool {
        true
    }
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        self.0.execute(context)
    }
}

#[test]
fn join_registry_rejects_undeclared_unregistered_and_parallel_executors_before_dispatch() {
    for has_schema in [false, true] {
        let (schemas, leaf, agent) = fixture_with_join_schema(has_schema);
        for declared in [false, true] {
            for scheduling in [
                GraphNodeScheduling::Exclusive,
                GraphNodeScheduling::JournalIsolated,
            ] {
                let parent = parent("parent", &leaf, &agent, 1);
                let mut registry = ExecutableGraphRegistryBuilder::new(schemas.clone());
                registry
                    .register_reducer(Arc::new(Reducer(leaf.reducer().clone())))
                    .unwrap();
                registry.register_graph(leaf.clone()).unwrap();
                let target = if declared {
                    registry.register_graph(parent.clone()).unwrap();
                    registry
                        .register_node(Arc::new(Executor(
                            leaf.reference(),
                            NodeId::new("work").unwrap(),
                        )))
                        .unwrap();
                    &parent
                } else {
                    &leaf
                };
                registry
                    .register_node(Arc::new(JoinExecutor(
                        Executor(target.reference(), NodeId::new("work").unwrap()),
                        scheduling,
                    )))
                    .unwrap();
                let result = registry.build();
                if has_schema && declared && scheduling == GraphNodeScheduling::Exclusive {
                    result.unwrap();
                } else {
                    assert!(matches!(
                        result,
                        Err(ExecutableGraphRegistryError::InvalidChildJoinExecutor)
                    ));
                }
            }
        }
    }
}
