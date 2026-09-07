// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Startup-time shared-state subgraph composition. The resulting ordinary graph
//! uses the existing checkpoint, pending-result, and invocation wire contracts.

#[cfg(test)]
#[path = "graph_composition_tests.rs"]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use serde::Serialize;
use thiserror::Error;

use crate::{
    CompiledGraph, Digest, GraphCompileError, GraphNode, GraphReference, GraphRoute, GraphRoutes,
    NodeId, ReadyNodes, RouteId,
};

/// A finite, shared-state subgraph template with explicit return ports.
///
/// Pure terminal nodes in the template are symbolic return ports, not executable
/// nodes. Every other node executes normally and communicates through the pinned
/// state/update schema and reducer. Returns never silently discard terminal
/// output: an executable body node cannot declare `terminal`.
///
/// Templates must be acyclic. Use [`GraphSubgraphCall::bounded_loop`] for an
/// explicitly bounded repeat, with a parent route for exhaustion. An already
/// composed acyclic graph can itself become a template, allowing static nesting.
#[derive(Clone, Debug)]
pub struct SharedStateSubgraph {
    graph: Arc<CompiledGraph>,
    ports: BTreeSet<NodeId>,
}

impl SharedStateSubgraph {
    /// Validates a compiled template before it can be composed.
    ///
    /// # Errors
    ///
    /// Rejects mixed executable/terminal nodes, entry return ports, cycles,
    /// absent return ports, and paths exceeding the template's step ceiling.
    pub fn new(graph: CompiledGraph) -> Result<Self, GraphCompositionError> {
        let mut ports = BTreeSet::new();
        for node in graph.nodes() {
            if node.allows_terminal() {
                if node.continue_to().is_some()
                    || !node.routes().is_empty()
                    || node.wait_to().is_some()
                {
                    return Err(GraphCompositionError::MixedReturnPort {
                        node_id: node.node_id().clone(),
                    });
                }
                ports.insert(node.node_id().clone());
            }
        }
        if ports.is_empty() {
            return Err(GraphCompositionError::MissingReturnPorts);
        }
        if graph.entry_nodes().iter().any(|id| ports.contains(id)) {
            return Err(GraphCompositionError::EntryIsReturnPort);
        }
        validate_finite_template(&graph, &ports)?;
        Ok(Self {
            graph: Arc::new(graph),
            ports,
        })
    }

    /// Returns the exact original template, including its symbolic ports.
    #[must_use]
    pub fn graph(&self) -> &CompiledGraph {
        &self.graph
    }

    /// Returns symbolic ports in canonical identity order.
    pub fn return_ports(&self) -> impl ExactSizeIterator<Item = &NodeId> {
        self.ports.iter()
    }
}

/// One static call-site replacement in a parent graph.
///
/// The parent call-site must declare only routes, with exactly one route named
/// for each template return port. Their successors are the explicit return
/// continuations. No executor is registered for the call-site or return ports.
#[derive(Clone, Debug)]
pub struct GraphSubgraphCall {
    node_id: NodeId,
    subgraph: SharedStateSubgraph,
    maximum_iterations: u16,
    repeat_port: Option<NodeId>,
    return_routes: BTreeMap<NodeId, RouteId>,
}

impl GraphSubgraphCall {
    /// Instantiates a template once at the given parent call-site.
    #[must_use]
    pub const fn once(node_id: NodeId, subgraph: SharedStateSubgraph) -> Self {
        Self {
            node_id,
            subgraph,
            maximum_iterations: 1,
            repeat_port: None,
            return_routes: BTreeMap::new(),
        }
    }

    /// Instantiates a bounded do/while-style loop.
    ///
    /// Selecting `repeat_port` enters the next copy of the body, or follows the
    /// parent's matching route on the last iteration. All other ports exit
    /// immediately. The exhaustion route is mandatory even if callers expect
    /// the loop to finish early. The bound is per call-site entry, not per run;
    /// the containing graph's global superstep ceiling still applies.
    ///
    /// # Errors
    ///
    /// Rejects zero/oversized iteration bounds and a repeat port absent from the
    /// template. Composition additionally caps the entire expanded graph.
    pub fn bounded_loop(
        node_id: NodeId,
        subgraph: SharedStateSubgraph,
        repeat_port: NodeId,
        maximum_iterations: u16,
    ) -> Result<Self, GraphCompositionError> {
        if maximum_iterations == 0 || usize::from(maximum_iterations) > CompiledGraph::MAX_NODES {
            return Err(GraphCompositionError::InvalidIterationLimit);
        }
        if !subgraph.ports.contains(&repeat_port) {
            return Err(GraphCompositionError::UnknownRepeatPort { port: repeat_port });
        }
        Ok(Self {
            node_id,
            subgraph,
            maximum_iterations,
            repeat_port: Some(repeat_port),
            return_routes: BTreeMap::new(),
        })
    }

    /// Explicitly binds each return port to a graph-global parent route ID.
    ///
    /// By default route names equal port names. Use this when instantiating the
    /// same template at several call-sites, since parent route IDs are globally
    /// unique. The complete mapping must be bijective; partial overrides and
    /// duplicate port/route identities are rejected.
    pub fn with_return_routes<I>(mut self, routes: I) -> Result<Self, GraphCompositionError>
    where
        I: IntoIterator<Item = (NodeId, RouteId)>,
    {
        let mut bindings = BTreeMap::new();
        let mut used = BTreeSet::new();
        for (port, route) in routes {
            if !self.subgraph.ports.contains(&port)
                || !used.insert(route.clone())
                || bindings.insert(port, route).is_some()
            {
                return Err(GraphCompositionError::ReturnRoutesMismatch {
                    node_id: self.node_id,
                });
            }
        }
        if bindings.len() != self.subgraph.ports.len() {
            return Err(GraphCompositionError::ReturnRoutesMismatch {
                node_id: self.node_id,
            });
        }
        self.return_routes = bindings;
        Ok(self)
    }

    fn matches_return(&self, port: &NodeId, route: &RouteId) -> bool {
        self.return_routes
            .get(port)
            .map_or_else(|| port.as_str() == route.as_str(), |bound| bound == route)
    }
}

/// Original executable role for one generated node identity.
///
/// Register executable code against the **composed** graph reference and the
/// generated node ID, selected by this source mapping. Pass its actual durable
/// context unchanged; never manufacture a child checkpoint or invocation ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GraphNodeSource {
    graph: GraphReference,
    node_id: NodeId,
    call_site: NodeId,
    iteration: u16,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    routes: BTreeMap<RouteId, RouteId>,
}

impl GraphNodeSource {
    /// Returns the exact template revision.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }
    /// Returns the node identity inside the template.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    /// Returns the parent call-site identity.
    #[must_use]
    pub const fn call_site(&self) -> &NodeId {
        &self.call_site
    }
    /// Returns the zero-based statically assigned loop iteration.
    #[must_use]
    pub const fn iteration(&self) -> u16 {
        self.iteration
    }

    /// Resolves a template-local route to its generated graph-global identity.
    /// Executors must return this identity, not the template's route name.
    #[must_use]
    pub fn route_id(&self, source: &RouteId) -> Option<&RouteId> {
        self.routes.get(source)
    }
}

/// A canonical ordinary graph plus its deterministic executor source map.
///
/// Compilation lowers shared-state calls and finite loops into existing nodes
/// and edges. Generated IDs use full SHA-256, binding the parent descriptor,
/// template revision, call-site, repeat policy, iteration, and source node.
/// In-flight runs therefore cannot silently adopt another template or bound.
/// No new checkpoint grammar, process-local loop counter, or nested driver is
/// involved. Existing canonical bytes are unchanged when no calls are supplied.
#[derive(Clone, Debug)]
pub struct GraphComposition {
    graph: CompiledGraph,
    sources: BTreeMap<NodeId, GraphNodeSource>,
}

impl GraphComposition {
    /// Replaces all declared call-sites and validates the complete result.
    ///
    /// Input/state/update/output schemas and reducer references must match
    /// exactly. The parent's parallelism ceiling must not exceed any template's
    /// ceiling, a conservative enforceable bound for shared-state composition.
    /// Every loop iteration counts against the 1,024-node graph ceiling before
    /// allocation. The existing route, ready-set, and byte ceilings also apply.
    ///
    /// # Errors
    ///
    /// Rejects duplicate/absent call-sites, schema/reducer drift, missing or
    /// extra return routes, resource exhaustion, and invalid expanded graphs.
    #[allow(clippy::too_many_lines)]
    pub fn compile<I>(parent: CompiledGraph, calls: I) -> Result<Self, GraphCompositionError>
    where
        I: IntoIterator<Item = GraphSubgraphCall>,
    {
        let (by_id, count) = collect_calls(&parent, calls)?;
        if by_id.is_empty() {
            return Ok(Self {
                graph: parent,
                sources: BTreeMap::new(),
            });
        }

        let mut sources = BTreeMap::new();
        let mut identities = BTreeMap::new();
        for call in by_id.values() {
            for iteration in 0..call.maximum_iterations {
                for (ordinal, node) in call.subgraph.graph.nodes().iter().enumerate() {
                    if call.subgraph.ports.contains(node.node_id()) {
                        continue;
                    }
                    let (generated, source) =
                        lower_identity(&parent, call, node, iteration, ordinal)?;
                    if parent.node(&generated).is_some()
                        || sources.insert(generated.clone(), source).is_some()
                    {
                        return Err(GraphCompositionError::IdentityCollision);
                    }
                    identities.insert(
                        (call.node_id.clone(), iteration, node.node_id().clone()),
                        generated,
                    );
                }
            }
        }
        let mut entries = BTreeMap::new();
        for call in by_id.values() {
            for iteration in 0..call.maximum_iterations {
                let nodes =
                    call.subgraph.graph.entry_nodes().iter().map(|id| {
                        identities[&(call.node_id.clone(), iteration, id.clone())].clone()
                    });
                entries.insert((call.node_id.clone(), iteration), ready(nodes)?);
            }
        }
        let redirect_parent = |id: &NodeId| -> Vec<NodeId> {
            entries
                .get(&(id.clone(), 0))
                .map_or_else(|| vec![id.clone()], |nodes| nodes.iter().cloned().collect())
        };
        let mut nodes = Vec::with_capacity(count);
        for node in parent.nodes() {
            if let Some(call) = by_id.get(node.node_id()) {
                for iteration in 0..call.maximum_iterations {
                    let redirect_child = |id: &NodeId| -> Vec<NodeId> {
                        if !call.subgraph.ports.contains(id) {
                            return vec![
                                identities[&(call.node_id.clone(), iteration, id.clone())].clone(),
                            ];
                        }
                        if call.repeat_port.as_ref() == Some(id)
                            && iteration + 1 < call.maximum_iterations
                        {
                            return entries[&(call.node_id.clone(), iteration + 1)]
                                .iter()
                                .cloned()
                                .collect();
                        }
                        // validate_call proved that every port has one matching route.
                        node.routes()
                            .iter()
                            .find(|route| call.matches_return(id, route.route_id()))
                            .into_iter()
                            .flat_map(|route| route.successors().iter().flat_map(&redirect_parent))
                            .collect()
                    };
                    for child in call.subgraph.graph.nodes() {
                        if !call.subgraph.ports.contains(child.node_id()) {
                            let generated = identities
                                [&(call.node_id.clone(), iteration, child.node_id().clone())]
                                .clone();
                            let routes = &sources[&generated].routes;
                            nodes.push(remap_node(
                                child,
                                generated,
                                &redirect_child,
                                Some(routes),
                            )?);
                        }
                    }
                }
            } else {
                nodes.push(remap_node(
                    node,
                    node.node_id().clone(),
                    &redirect_parent,
                    None,
                )?);
            }
        }
        let entry_nodes = ready(parent.entry_nodes().iter().flat_map(redirect_parent))?;
        let graph = CompiledGraph::compile(
            parent.identity().clone(),
            parent.input_schema().clone(),
            parent.state_schema().clone(),
            parent.update_schema().clone(),
            parent.output_schema().clone(),
            parent.reducer().clone(),
            entry_nodes,
            nodes,
            parent.limits(),
        )?;
        Ok(Self { graph, sources })
    }

    /// Returns the exact graph to register and admit into durable storage.
    #[must_use]
    pub const fn graph(&self) -> &CompiledGraph {
        &self.graph
    }
    /// Returns generated IDs and source roles in canonical generated-ID order.
    pub fn node_sources(&self) -> impl ExactSizeIterator<Item = (&NodeId, &GraphNodeSource)> {
        self.sources.iter()
    }
    /// Looks up one generated role; ordinary parent nodes have no source entry.
    #[must_use]
    pub fn node_source(&self, node_id: &NodeId) -> Option<&GraphNodeSource> {
        self.sources.get(node_id)
    }
    /// Consumes the composition after executable source bindings are installed.
    #[must_use]
    pub fn into_graph(self) -> CompiledGraph {
        self.graph
    }
}

fn collect_calls(
    parent: &CompiledGraph,
    calls: impl IntoIterator<Item = GraphSubgraphCall>,
) -> Result<(BTreeMap<NodeId, GraphSubgraphCall>, usize), GraphCompositionError> {
    let mut by_id = BTreeMap::new();
    let mut count = parent.nodes().len();
    for call in calls {
        if by_id.contains_key(&call.node_id) {
            return Err(GraphCompositionError::DuplicateCallSite {
                node_id: call.node_id,
            });
        }
        validate_call(parent, &call)?;
        let body_count = call.subgraph.graph.nodes().len() - call.subgraph.ports.len();
        count = count - 1 + body_count * usize::from(call.maximum_iterations);
        if count > CompiledGraph::MAX_NODES {
            return Err(GraphCompositionError::ExpansionTooLarge);
        }
        by_id.insert(call.node_id.clone(), call);
    }
    Ok((by_id, count))
}

fn lower_identity(
    parent: &CompiledGraph,
    call: &GraphSubgraphCall,
    node: &GraphNode,
    iteration: u16,
    ordinal: usize,
) -> Result<(NodeId, GraphNodeSource), GraphCompositionError> {
    let mut source = GraphNodeSource {
        graph: call.subgraph.graph.reference(),
        node_id: node.node_id().clone(),
        call_site: call.node_id.clone(),
        iteration,
        routes: BTreeMap::new(),
    };
    let wire = serde_json::json!({
        "domain": "stateknot.graph-composition.v1", "parent": parent.reference(),
        "graph": source.graph, "call_site": source.call_site, "iteration": iteration,
        "maximum_iterations": call.maximum_iterations, "repeat_port": call.repeat_port,
        "return_routes": call.return_routes,
    });
    let bytes = serde_json_canonicalizer::to_vec(&wire)
        .map_err(|_| GraphCompositionError::CanonicalSerialization)?;
    let digest = Digest::sha256(bytes).to_string();
    // A common scope digest plus the canonical template ordinal preserves
    // reducer input order within an instance. Hashing each node independently
    // would silently permute an order-sensitive parallel reduction.
    let generated = NodeId::new(format!("skc-{}-{ordinal:04}", &digest[7..]))
        .map_err(|_| GraphCompositionError::CanonicalSerialization)?;
    for route in node.routes().iter() {
        let bytes = serde_json_canonicalizer::to_vec(&serde_json::json!({
            "domain": "stateknot.graph-composition.route.v1", "node_id": generated, "route_id": route.route_id(),
        })).map_err(|_| GraphCompositionError::CanonicalSerialization)?;
        let digest = Digest::sha256(bytes).to_string();
        let route_id = RouteId::new(format!("skr-{}", &digest[7..]))
            .map_err(|_| GraphCompositionError::CanonicalSerialization)?;
        source.routes.insert(route.route_id().clone(), route_id);
    }
    Ok((generated, source))
}

fn successors(node: &GraphNode) -> impl Iterator<Item = &NodeId> {
    node.continue_to()
        .into_iter()
        .chain(node.routes().iter().map(GraphRoute::successors))
        .chain(node.wait_to())
        .flat_map(ReadyNodes::iter)
}

fn validate_finite_template(
    graph: &CompiledGraph,
    ports: &BTreeSet<NodeId>,
) -> Result<(), GraphCompositionError> {
    let mut incoming: BTreeMap<_, usize> = graph
        .nodes()
        .iter()
        .map(|node| (node.node_id(), 0))
        .collect();
    let mut depths: BTreeMap<_, u64> = graph
        .nodes()
        .iter()
        .map(|node| (node.node_id(), 1))
        .collect();
    for node in graph.nodes() {
        for id in successors(node) {
            *incoming
                .get_mut(id)
                .ok_or(GraphCompositionError::IdentityCollision)? += 1;
        }
    }
    let mut queue: VecDeque<_> = incoming
        .iter()
        .filter_map(|(&id, &count)| (count == 0).then_some(id))
        .collect();
    let mut visited = 0;
    while let Some(id) = queue.pop_front() {
        visited += 1;
        let depth = depths[id];
        if !ports.contains(id) && depth > graph.limits().maximum_supersteps().get() {
            return Err(GraphCompositionError::TemplateStepLimit);
        }
        let node = graph
            .node(id)
            .ok_or(GraphCompositionError::IdentityCollision)?;
        for next in successors(node) {
            let next_depth = depths
                .get_mut(next)
                .ok_or(GraphCompositionError::IdentityCollision)?;
            *next_depth = (*next_depth).max(depth + 1);
            let count = incoming
                .get_mut(next)
                .ok_or(GraphCompositionError::IdentityCollision)?;
            *count -= 1;
            if *count == 0 {
                queue.push_back(next);
            }
        }
    }
    if visited != graph.nodes().len() {
        return Err(GraphCompositionError::CyclicTemplate);
    }
    Ok(())
}

fn validate_call(
    parent: &CompiledGraph,
    call: &GraphSubgraphCall,
) -> Result<(), GraphCompositionError> {
    let node =
        parent
            .node(&call.node_id)
            .ok_or_else(|| GraphCompositionError::UnknownCallSite {
                node_id: call.node_id.clone(),
            })?;
    if node.continue_to().is_some()
        || node.wait_to().is_some()
        || node.allows_terminal()
        || node.routes().len() != call.subgraph.ports.len()
        || node.routes().iter().any(|route| {
            !call
                .subgraph
                .ports
                .iter()
                .any(|id| call.matches_return(id, route.route_id()))
        })
    {
        return Err(GraphCompositionError::ReturnRoutesMismatch {
            node_id: call.node_id.clone(),
        });
    }
    let child = &call.subgraph.graph;
    if parent.input_schema() != child.input_schema()
        || parent.state_schema() != child.state_schema()
        || parent.update_schema() != child.update_schema()
        || parent.output_schema() != child.output_schema()
        || parent.reducer() != child.reducer()
    {
        return Err(GraphCompositionError::SharedStateContractMismatch {
            node_id: call.node_id.clone(),
        });
    }
    if parent.limits().maximum_parallelism() > child.limits().maximum_parallelism() {
        return Err(GraphCompositionError::TemplateParallelismLimit {
            node_id: call.node_id.clone(),
        });
    }
    Ok(())
}

fn ready(nodes: impl IntoIterator<Item = NodeId>) -> Result<ReadyNodes, GraphCompositionError> {
    // Deduplicate joins, just like the ordinary barrier planner, while bounding
    // allocation before collecting potentially multiplied successor sets.
    let mut unique = BTreeSet::new();
    for id in nodes {
        unique.insert(id);
        if unique.len() > ReadyNodes::MAX_LEN {
            return Err(GraphCompositionError::ExpansionTooLarge);
        }
    }
    ReadyNodes::try_new(unique).map_err(|_| GraphCompositionError::ExpansionTooLarge)
}

fn remap_node(
    node: &GraphNode,
    id: NodeId,
    redirect: &impl Fn(&NodeId) -> Vec<NodeId>,
    route_ids: Option<&BTreeMap<RouteId, RouteId>>,
) -> Result<GraphNode, GraphCompositionError> {
    let remap = |nodes: &ReadyNodes| ready(nodes.iter().flat_map(redirect));
    let routes = node
        .routes()
        .iter()
        .map(|route| {
            let route_id = route_ids.map_or_else(
                || route.route_id().clone(),
                |ids| ids[route.route_id()].clone(),
            );
            GraphRoute::new(route_id, remap(route.successors())?)
                .map_err(|_| GraphCompositionError::ExpansionTooLarge)
        })
        .collect::<Result<Vec<_>, _>>()?;
    GraphNode::new(
        id,
        node.continue_to().map(remap).transpose()?,
        GraphRoutes::try_new(routes).map_err(|_| GraphCompositionError::ExpansionTooLarge)?,
        node.wait_to().map(remap).transpose()?,
        node.allows_terminal(),
    )
    .map_err(|_| GraphCompositionError::ExpansionTooLarge)
}

/// Fail-closed rejection of a shared-state composition before admission.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphCompositionError {
    /// A return port also declared executable control outcomes.
    #[error("subgraph return port {node_id} also declares executable controls")]
    MixedReturnPort {
        /// Rejected symbolic identity.
        node_id: NodeId,
    },
    /// A subgraph must expose explicit returns.
    #[error("subgraph has no return ports")]
    MissingReturnPorts,
    /// Empty-body entry/return forwarding is not supported.
    #[error("subgraph entry cannot be a return port")]
    EntryIsReturnPort,
    /// Loops must use a finite explicit expansion.
    #[error("subgraph template contains a cycle; use a bounded loop call")]
    CyclicTemplate,
    /// A body path exceeded the template step ceiling.
    #[error("subgraph body path exceeds its maximum supersteps")]
    TemplateStepLimit,
    /// The finite repetition count is outside immutable bounds.
    #[error("loop iterations must be between 1 and 1024")]
    InvalidIterationLimit,
    /// A loop referenced a non-return identity.
    #[error("loop repeat port {port} is not a subgraph return port")]
    UnknownRepeatPort {
        /// Rejected repeat port.
        port: NodeId,
    },
    /// A call-site was supplied twice.
    #[error("duplicate subgraph call-site {node_id}")]
    DuplicateCallSite {
        /// Duplicate parent identity.
        node_id: NodeId,
    },
    /// No parent node names the call-site.
    #[error("unknown subgraph call-site {node_id}")]
    UnknownCallSite {
        /// Missing parent identity.
        node_id: NodeId,
    },
    /// Every port must have exactly one explicit parent route.
    #[error("subgraph call-site {node_id} must declare exactly its return routes")]
    ReturnRoutesMismatch {
        /// Rejected parent identity.
        node_id: NodeId,
    },
    /// Shared state cannot silently adapt schemas or reducer implementations.
    #[error("subgraph call-site {node_id} has incompatible schema or reducer pins")]
    SharedStateContractMismatch {
        /// Rejected parent identity.
        node_id: NodeId,
    },
    /// Parent parallelism must obey each included template's ceiling.
    #[error("parent parallelism exceeds subgraph ceiling at {node_id}")]
    TemplateParallelismLimit {
        /// Rejected parent identity.
        node_id: NodeId,
    },
    /// Expanded metadata exceeded an immutable graph bound.
    #[error("expanded graph exceeds a node, route, or ready-set ceiling")]
    ExpansionTooLarge,
    /// Source metadata could not be canonicalized.
    #[error("graph composition identity cannot be canonicalized")]
    CanonicalSerialization,
    /// A generated identity collided with another executable identity.
    #[error("graph composition node identity collision")]
    IdentityCollision,
    /// The complete ordinary graph failed its existing compiler gates.
    #[error(transparent)]
    Graph(#[from] GraphCompileError),
}
