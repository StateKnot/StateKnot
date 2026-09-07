// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Offline startup example. Install this registry in a `DurableAgentLoop` backed
//! by `PostgreSQL` to execute it; no in-memory execution backend is introduced.

use std::{error::Error, sync::Arc};

use serde_json::json;
use stateknot_core::{
    BoundedJson, BoxFuture, BudgetUsage, CapabilityIdentity, CapabilityName, CapabilityReference,
    CompiledGraph, Digest, Failure, FailureCategory, FailureCode, FailureId, FailureMessage,
    FailureOrigin, GraphComposition, GraphExecutionLimits, GraphNode, GraphReducer,
    GraphReducerError, GraphReducerInput, GraphReducerReference, GraphReference, GraphRoute,
    GraphRoutes, GraphSubgraphCall, NodeControl, NodeId, NodeInvocationBindings, NodeStateChange,
    NodeStateUpdate, NodeTerminalOutput, PrincipalIdentity, ReadyNodes, RetryAdvice, RouteId,
    SchemaReference, SharedStateSubgraph, Superstep, Version,
};
use stateknot_runtime::{
    ExecutableGraphRegistryBuilder, GraphNodeContext, GraphNodeExecution, GraphNodeExecutionError,
    GraphNodeExecutor, JsonSchemaRegistryBuilder, JsonSchemaRegistryLimits,
    register_standard_agent_admission_event_schema,
    register_standard_agent_cancellation_event_schema, register_standard_graph_driver_event_schema,
    register_standard_graph_lifecycle_event_schema,
};

fn identity(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://example.com/stateknot".parse().unwrap(),
            "review-service".parse().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}
fn id(name: &str) -> NodeId {
    NodeId::new(name).unwrap()
}
fn ids(names: &[&str]) -> ReadyNodes {
    ReadyNodes::try_new(names.iter().map(|name| id(name))).unwrap()
}
fn route(name: &str, successor: &str) -> GraphRoute {
    GraphRoute::new(RouteId::new(name).unwrap(), ids(&[successor])).unwrap()
}
fn terminal(name: &str) -> GraphNode {
    GraphNode::new(id(name), None, GraphRoutes::empty(), None, true).unwrap()
}

struct CounterReducer(GraphReducerReference);
impl GraphReducer for CounterReducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.0
    }
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        let mut count = state.as_value()["count"]
            .as_u64()
            .ok_or(GraphReducerError::Rejected)?;
        for update in updates {
            count = count
                .checked_add(
                    update.update().data().as_value()["count"]
                        .as_u64()
                        .ok_or(GraphReducerError::Rejected)?,
                )
                .ok_or(GraphReducerError::Rejected)?;
        }
        BoundedJson::try_from_value(json!({"count": count}))
            .map_err(|_| GraphReducerError::Rejected)
    }
}

enum Role {
    Work,
    Decide { accept: RouteId, retry: RouteId },
    Finish,
    Exhausted,
}
struct ReviewNode {
    graph: GraphReference,
    id: NodeId,
    schema: SchemaReference,
    role: Role,
}

impl GraphNodeExecutor for ReviewNode {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }
    fn node_id(&self) -> &NodeId {
        &self.id
    }
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            let mut change = NodeStateChange::Unchanged;
            let control = match &self.role {
                Role::Work => {
                    // Model/Tool I/O belongs in DurableInvocationExecutor with
                    // this actual context. This example only changes typed state.
                    change = NodeStateChange::Update {
                        update: NodeStateUpdate::new(
                            self.schema.clone(),
                            BoundedJson::try_from_value(json!({"count": 1})).unwrap(),
                        )
                        .unwrap(),
                    };
                    NodeControl::Continue
                }
                Role::Decide { accept, retry } => NodeControl::Route {
                    route_id: if context.checkpoint().state().data().as_value()["count"]
                        .as_u64()
                        .unwrap_or(0)
                        >= 2
                    {
                        accept.clone()
                    } else {
                        retry.clone()
                    },
                },
                Role::Finish => NodeControl::Terminal {
                    output: NodeTerminalOutput::new(
                        self.schema.clone(),
                        context.checkpoint().state().data().clone(),
                    )
                    .unwrap(),
                },
                Role::Exhausted => {
                    return Err(GraphNodeExecutionError::new(
                        Failure::new(
                            FailureId::generate(),
                            FailureCategory::InvalidInput,
                            FailureCode::new("review.iterations_exhausted").unwrap(),
                            FailureOrigin::new("example.review").unwrap(),
                            FailureMessage::new(
                                "The review did not converge within its iteration limit.",
                            )
                            .unwrap(),
                            RetryAdvice::Never,
                        )
                        .unwrap(),
                        BudgetUsage::zero(),
                    )
                    .unwrap());
                }
            };
            Ok(GraphNodeExecution::new(
                change,
                control,
                NodeInvocationBindings::empty(),
                BudgetUsage::zero(),
            ))
        })
    }
}

#[allow(clippy::too_many_lines)]
fn main() -> Result<(), Box<dyn Error>> {
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://example.com/schemas/review-counter/1.0.0",
        "type": "object", "properties": { "count": { "type": "integer", "minimum": 0, "maximum": 3 } },
        "required": ["count"], "additionalProperties": false,
    });
    let schema = SchemaReference::new(
        "https://example.com/schemas/review-counter/1.0.0".parse()?,
        Version::new(1, 0, 0),
        Digest::sha256(serde_json_canonicalizer::to_vec(&document)?),
    );
    let reducer = GraphReducerReference::new(
        identity("review.counter-reducer"),
        Digest::sha256(b"ordered checked counter addition v1"),
    );
    let compile = |name, entry, nodes| {
        CompiledGraph::compile(
            identity(name),
            schema.clone(),
            schema.clone(),
            schema.clone(),
            schema.clone(),
            reducer.clone(),
            entry,
            nodes,
            GraphExecutionLimits::new(Superstep::new(7).unwrap(), 1).unwrap(),
        )
    };
    let body = compile(
        "review.body",
        ids(&["work"]),
        vec![
            GraphNode::new(
                id("work"),
                Some(ids(&["decide"])),
                GraphRoutes::empty(),
                None,
                false,
            )?,
            GraphNode::new(
                id("decide"),
                None,
                GraphRoutes::try_new([route("accept", "done"), route("retry", "again")])?,
                None,
                false,
            )?,
            terminal("done"),
            terminal("again"),
        ],
    )?;
    let parent = compile(
        "review.workflow",
        ids(&["review"]),
        vec![
            GraphNode::new(
                id("review"),
                None,
                GraphRoutes::try_new([route("done", "finish"), route("again", "exhausted")])?,
                None,
                false,
            )?,
            terminal("finish"),
            terminal("exhausted"),
        ],
    )?;
    let composition = GraphComposition::compile(
        parent,
        [GraphSubgraphCall::bounded_loop(
            id("review"),
            SharedStateSubgraph::new(body)?,
            id("again"),
            3,
        )?],
    )?;

    let mut schemas = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    schemas.register(schema.clone(), document)?;
    register_standard_graph_driver_event_schema(&mut schemas)?;
    register_standard_graph_lifecycle_event_schema(&mut schemas)?;
    register_standard_agent_cancellation_event_schema(&mut schemas)?;
    register_standard_agent_admission_event_schema(&mut schemas)?;
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas.build()?);
    // Register only the expanded graph: call-site and return-port descriptors
    // are authoring templates, not additional deployable executors.
    registry.register_graph(composition.graph().clone())?;
    registry.register_reducer(Arc::new(CounterReducer(reducer)))?;
    for node in composition.graph().nodes() {
        let role = if let Some(source) = composition.node_source(node.node_id()) {
            match source.node_id().as_str() {
                "work" => Role::Work,
                "decide" => Role::Decide {
                    accept: source
                        .route_id(&RouteId::new("accept")?)
                        .ok_or("missing accept route")?
                        .clone(),
                    retry: source
                        .route_id(&RouteId::new("retry")?)
                        .ok_or("missing retry route")?
                        .clone(),
                },
                _ => return Err("unknown template node".into()),
            }
        } else if node.node_id() == &id("finish") {
            Role::Finish
        } else {
            Role::Exhausted
        };
        registry.register_node(Arc::new(ReviewNode {
            graph: composition.graph().reference(),
            id: node.node_id().clone(),
            schema: schema.clone(),
            role,
        }))?;
    }
    let registry = registry.build()?;
    println!("Pinned graph: {}", composition.graph().definition_digest());
    println!(
        "{} executable nodes; {} template bindings; {} frozen graph",
        composition.graph().nodes().len(),
        composition.node_sources().len(),
        registry.len()
    );
    println!(
        "Initial state: {{\"count\":0}}. Execute with DurableAgentLoop + PostgreSQL; this example performs no I/O."
    );
    Ok(())
}
