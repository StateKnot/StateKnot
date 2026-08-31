// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Canonical graph definitions and deterministic superstep planning.
//!
//! A compiled graph is declarative, bounded, schema-pinned, and independent of
//! executable Rust code. Its digest is computed from canonical descriptor
//! bytes and is the exact value retained by checkpoints. Barrier planning then
//! verifies complete pending results, validates every typed value, invokes one
//! exactly pinned pure reducer outside storage transactions, resolves control
//! branches in stable node order, and produces the existing atomic
//! [`CheckpointBarrier`] intent.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{
    BoundedJson, CapabilityIdentity, Checkpoint, CheckpointBarrier, CheckpointBarrierError,
    CheckpointId, CheckpointState, CheckpointStateError, CheckpointWrite, CheckpointWriteError,
    Digest, GraphReference, NodeActivation, NodeControl, NodeControlKind, NodeId, NodeStateUpdate,
    NodeTerminalOutput, NodeWait, NodeWaits, NodeWaitsError, PendingNodeResult, ReadyNodes,
    RouteId, SchemaReference, Superstep,
};

const MEBIBYTE: usize = 1024 * 1024;

/// Exact executable reducer revision required by a compiled graph.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphReducerReference {
    identity: CapabilityIdentity,
    definition_digest: Digest,
}

impl GraphReducerReference {
    /// Constructs an immutable reducer reference.
    #[must_use]
    pub const fn new(identity: CapabilityIdentity, definition_digest: Digest) -> Self {
        Self {
            identity,
            definition_digest,
        }
    }

    /// Returns the owner-qualified reducer identity.
    #[must_use]
    pub const fn identity(&self) -> &CapabilityIdentity {
        &self.identity
    }

    /// Returns the checksum of the exact reducer implementation contract.
    #[must_use]
    pub const fn definition_digest(&self) -> Digest {
        self.definition_digest
    }
}

/// Hard execution ceilings embedded in one graph definition.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphExecutionLimits {
    maximum_supersteps: Superstep,
    maximum_parallelism: u16,
}

impl GraphExecutionLimits {
    /// Constructs positive limits below framework and storage ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`GraphExecutionLimitsError`] for a zero superstep or
    /// parallelism limit, or for parallelism above [`ReadyNodes::MAX_LEN`].
    pub const fn new(
        maximum_supersteps: Superstep,
        maximum_parallelism: u16,
    ) -> Result<Self, GraphExecutionLimitsError> {
        if maximum_supersteps.get() == 0 {
            return Err(GraphExecutionLimitsError::ZeroSupersteps);
        }
        if maximum_parallelism == 0 {
            return Err(GraphExecutionLimitsError::ZeroParallelism);
        }
        if maximum_parallelism as usize > ReadyNodes::MAX_LEN {
            return Err(
                GraphExecutionLimitsError::ParallelismAboveFrameworkMaximum {
                    maximum: ReadyNodes::MAX_LEN,
                    actual: maximum_parallelism as usize,
                },
            );
        }
        Ok(Self {
            maximum_supersteps,
            maximum_parallelism,
        })
    }

    /// Returns the maximum number of barriers that may execute.
    #[must_use]
    pub const fn maximum_supersteps(self) -> Superstep {
        self.maximum_supersteps
    }

    /// Returns the maximum number of ready nodes in one superstep.
    #[must_use]
    pub const fn maximum_parallelism(self) -> u16 {
        self.maximum_parallelism
    }
}

impl<'de> Deserialize<'de> for GraphExecutionLimits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            maximum_supersteps: Superstep,
            maximum_parallelism: u16,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.maximum_supersteps, wire.maximum_parallelism).map_err(de::Error::custom)
    }
}

/// Invalid compiled graph execution limits.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphExecutionLimitsError {
    /// A graph could execute no barrier.
    #[error("graph maximum supersteps must be positive")]
    ZeroSupersteps,
    /// A graph could schedule no entry or successor node.
    #[error("graph maximum parallelism must be positive")]
    ZeroParallelism,
    /// A graph limit exceeded the immutable ready-set ceiling.
    #[error("graph maximum parallelism is {actual}; framework maximum is {maximum}")]
    ParallelismAboveFrameworkMaximum {
        /// Immutable framework ceiling.
        maximum: usize,
        /// Rejected graph limit.
        actual: usize,
    },
}

/// One declared conditional branch and its non-empty successor set.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphRoute {
    route_id: RouteId,
    successors: ReadyNodes,
}

impl GraphRoute {
    /// Constructs one declared route.
    ///
    /// # Errors
    ///
    /// Returns [`GraphRouteError::EmptySuccessors`] when selecting the route
    /// would silently dead-end without a terminal result.
    pub fn new(route_id: RouteId, successors: ReadyNodes) -> Result<Self, GraphRouteError> {
        if successors.is_empty() {
            return Err(GraphRouteError::EmptySuccessors);
        }
        Ok(Self {
            route_id,
            successors,
        })
    }

    /// Returns the stable route identity.
    #[must_use]
    pub const fn route_id(&self) -> &RouteId {
        &self.route_id
    }

    /// Returns the route's deterministic successor set.
    #[must_use]
    pub const fn successors(&self) -> &ReadyNodes {
        &self.successors
    }
}

impl<'de> Deserialize<'de> for GraphRoute {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            route_id: RouteId,
            successors: ReadyNodes,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.route_id, wire.successors).map_err(de::Error::custom)
    }
}

/// Invalid conditional graph route.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphRouteError {
    /// A non-terminal branch contained no successor.
    #[error("graph route successors must not be empty")]
    EmptySuccessors,
}

/// Canonically ordered, duplicate-free route collection for one node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRoutes(Box<[GraphRoute]>);

impl GraphRoutes {
    /// Hard maximum number of routes declared by one node.
    pub const MAX_LEN: usize = 256;

    /// Constructs a stable route collection.
    ///
    /// # Errors
    ///
    /// Returns [`GraphRoutesError`] for duplicate identities or an oversized
    /// collection.
    pub fn try_new<I>(routes: I) -> Result<Self, GraphRoutesError>
    where
        I: IntoIterator<Item = GraphRoute>,
    {
        let mut values = Vec::new();
        let mut identities = BTreeSet::new();
        for route in routes {
            if values.len() == Self::MAX_LEN {
                return Err(GraphRoutesError::TooMany {
                    maximum: Self::MAX_LEN,
                    actual: Self::MAX_LEN + 1,
                });
            }
            if !identities.insert(route.route_id.clone()) {
                return Err(GraphRoutesError::Duplicate {
                    route_id: route.route_id,
                });
            }
            values.push(route);
        }
        values.sort_unstable_by(|left, right| left.route_id.cmp(&right.route_id));
        Ok(Self(values.into_boxed_slice()))
    }

    /// Returns an empty route collection.
    #[must_use]
    pub fn empty() -> Self {
        Self(Box::new([]))
    }

    /// Returns the number of declared routes.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no conditional route is declared.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates routes in stable identity order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &GraphRoute> {
        self.0.iter()
    }

    /// Resolves one exact route identity.
    #[must_use]
    pub fn get(&self, route_id: &RouteId) -> Option<&GraphRoute> {
        self.0
            .binary_search_by(|route| route.route_id.cmp(route_id))
            .ok()
            .map(|index| &self.0[index])
    }
}

impl Default for GraphRoutes {
    fn default() -> Self {
        Self::empty()
    }
}

impl Serialize for GraphRoutes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for GraphRoutes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<GraphRoute>::deserialize(deserializer)?;
        Self::try_new(values).map_err(de::Error::custom)
    }
}

impl JsonSchema for GraphRoutes {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "GraphRoutes".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::GraphRoutes").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<GraphRoute>(),
            "maxItems": 256,
            "description": "Routes are serialized in ascending RouteId order and runtime rejects duplicate identities."
        })
    }
}

/// Invalid route collection for one node.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphRoutesError {
    /// The per-node route ceiling was exceeded.
    #[error("graph node contains {actual} routes; maximum is {maximum}")]
    TooMany {
        /// Immutable per-node ceiling.
        maximum: usize,
        /// First count beyond the ceiling.
        actual: usize,
    },
    /// One route identity was repeated by the same node.
    #[error("graph node contains duplicate route {route_id}")]
    Duplicate {
        /// Repeated identity.
        route_id: RouteId,
    },
}

/// Declarative control surface for one stable graph node.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphNode {
    node_id: NodeId,
    #[serde(skip_serializing_if = "Option::is_none")]
    continue_to: Option<ReadyNodes>,
    routes: GraphRoutes,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_to: Option<ReadyNodes>,
    terminal: bool,
}

impl GraphNode {
    /// Constructs one compiled node control declaration.
    ///
    /// `continue_to` is selected by [`NodeControl::Continue`], `routes` by
    /// [`NodeControl::Route`], and `wait_to` is the ready set retained while a
    /// [`NodeControl::Wait`] suspends the run. `terminal` authorizes one exact
    /// terminal output. Every non-terminal successor set must be non-empty.
    ///
    /// # Errors
    ///
    /// Returns [`GraphNodeError`] when the node exposes no control outcome or
    /// declares an empty continuation/wait successor set.
    pub fn new(
        node_id: NodeId,
        continue_to: Option<ReadyNodes>,
        routes: GraphRoutes,
        wait_to: Option<ReadyNodes>,
        terminal: bool,
    ) -> Result<Self, GraphNodeError> {
        if continue_to.as_ref().is_some_and(ReadyNodes::is_empty) {
            return Err(GraphNodeError::EmptyContinueSuccessors);
        }
        if wait_to.as_ref().is_some_and(ReadyNodes::is_empty) {
            return Err(GraphNodeError::EmptyWaitSuccessors);
        }
        if continue_to.is_none() && routes.is_empty() && wait_to.is_none() && !terminal {
            return Err(GraphNodeError::NoControlOutcome);
        }
        Ok(Self {
            node_id,
            continue_to,
            routes,
            wait_to,
            terminal,
        })
    }

    /// Returns the stable node identity.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns successors selected by an unconditional continuation.
    #[must_use]
    pub const fn continue_to(&self) -> Option<&ReadyNodes> {
        self.continue_to.as_ref()
    }

    /// Returns declared conditional routes.
    #[must_use]
    pub const fn routes(&self) -> &GraphRoutes {
        &self.routes
    }

    /// Returns successors retained by a durable wait outcome.
    #[must_use]
    pub const fn wait_to(&self) -> Option<&ReadyNodes> {
        self.wait_to.as_ref()
    }

    /// Returns whether this node may complete the graph.
    #[must_use]
    pub const fn allows_terminal(&self) -> bool {
        self.terminal
    }

    fn successor_sets(&self) -> impl Iterator<Item = &ReadyNodes> {
        self.continue_to
            .iter()
            .chain(self.routes.iter().map(GraphRoute::successors))
            .chain(self.wait_to.iter())
    }

    const fn is_boundary(&self) -> bool {
        self.terminal || self.wait_to.is_some()
    }
}

impl<'de> Deserialize<'de> for GraphNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            node_id: NodeId,
            continue_to: Option<ReadyNodes>,
            routes: GraphRoutes,
            wait_to: Option<ReadyNodes>,
            terminal: bool,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.node_id,
            wire.continue_to,
            wire.routes,
            wire.wait_to,
            wire.terminal,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid local control shape for one graph node.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphNodeError {
    /// No result control value could be accepted.
    #[error("graph node must declare at least one control outcome")]
    NoControlOutcome,
    /// `continue` would silently dead-end.
    #[error("graph continue successors must not be empty")]
    EmptyContinueSuccessors,
    /// A resolved wait would have no declared continuation.
    #[error("graph wait successors must not be empty")]
    EmptyWaitSuccessors,
}

/// Canonical, schema-pinned declarative graph compiled before admission.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompiledGraph {
    identity: CapabilityIdentity,
    input_schema: SchemaReference,
    state_schema: SchemaReference,
    update_schema: SchemaReference,
    output_schema: SchemaReference,
    reducer: GraphReducerReference,
    entry_nodes: ReadyNodes,
    #[schemars(length(max = 1024))]
    nodes: Box<[GraphNode]>,
    limits: GraphExecutionLimits,
    definition_digest: Digest,
}

impl CompiledGraph {
    /// Hard node ceiling for one compiled graph.
    pub const MAX_NODES: usize = ReadyNodes::MAX_LEN;
    /// Hard aggregate route ceiling for one compiled graph.
    pub const MAX_ROUTES: usize = Self::MAX_NODES * GraphRoutes::MAX_LEN;
    /// Hard canonical descriptor byte ceiling.
    pub const MAX_DEFINITION_BYTES: usize = 2 * MEBIBYTE;

    /// Compiles and checksums one trusted declarative graph descriptor.
    ///
    /// # Errors
    ///
    /// Rejects duplicate/missing identities, unreachable nodes, paths that
    /// cannot reach a wait or terminal boundary, limit violations, and
    /// canonical descriptors above the hard byte ceiling.
    #[allow(clippy::too_many_arguments)]
    pub fn compile<I>(
        identity: CapabilityIdentity,
        input_schema: SchemaReference,
        state_schema: SchemaReference,
        update_schema: SchemaReference,
        output_schema: SchemaReference,
        reducer: GraphReducerReference,
        entry_nodes: ReadyNodes,
        nodes: I,
        limits: GraphExecutionLimits,
    ) -> Result<Self, GraphCompileError>
    where
        I: IntoIterator<Item = GraphNode>,
    {
        Self::build(
            identity,
            input_schema,
            state_schema,
            update_schema,
            output_schema,
            reducer,
            entry_nodes,
            nodes,
            limits,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build<I>(
        identity: CapabilityIdentity,
        input_schema: SchemaReference,
        state_schema: SchemaReference,
        update_schema: SchemaReference,
        output_schema: SchemaReference,
        reducer: GraphReducerReference,
        entry_nodes: ReadyNodes,
        nodes: I,
        limits: GraphExecutionLimits,
        supplied_digest: Option<Digest>,
    ) -> Result<Self, GraphCompileError>
    where
        I: IntoIterator<Item = GraphNode>,
    {
        let by_id = collect_graph_nodes(nodes)?;
        validate_compiled_shape(&by_id, &entry_nodes, limits)?;

        let nodes = by_id.into_values().collect::<Vec<_>>().into_boxed_slice();
        let mut graph = Self {
            identity,
            input_schema,
            state_schema,
            update_schema,
            output_schema,
            reducer,
            entry_nodes,
            nodes,
            limits,
            definition_digest: Digest::sha256([]),
        };
        let canonical = graph.canonical_definition_bytes()?;
        if canonical.len() > Self::MAX_DEFINITION_BYTES {
            return Err(GraphCompileError::DefinitionTooLarge {
                maximum: Self::MAX_DEFINITION_BYTES,
                actual: canonical.len(),
            });
        }
        graph.definition_digest = Digest::sha256(&canonical);
        if supplied_digest.is_some_and(|digest| digest != graph.definition_digest) {
            return Err(GraphCompileError::DigestMismatch);
        }
        Ok(graph)
    }

    /// Returns the owner-qualified graph identity.
    #[must_use]
    pub const fn identity(&self) -> &CapabilityIdentity {
        &self.identity
    }

    /// Returns the admitted graph input schema.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaReference {
        &self.input_schema
    }

    /// Returns the immutable checkpoint state schema.
    #[must_use]
    pub const fn state_schema(&self) -> &SchemaReference {
        &self.state_schema
    }

    /// Returns the schema every node update must use.
    #[must_use]
    pub const fn update_schema(&self) -> &SchemaReference {
        &self.update_schema
    }

    /// Returns the successful terminal output schema.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaReference {
        &self.output_schema
    }

    /// Returns the exact executable reducer revision.
    #[must_use]
    pub const fn reducer(&self) -> &GraphReducerReference {
        &self.reducer
    }

    /// Returns the deterministic initial ready set.
    #[must_use]
    pub const fn entry_nodes(&self) -> &ReadyNodes {
        &self.entry_nodes
    }

    /// Returns compiled nodes in stable identity order.
    #[must_use]
    pub const fn nodes(&self) -> &[GraphNode] {
        &self.nodes
    }

    /// Returns immutable graph execution limits.
    #[must_use]
    pub const fn limits(&self) -> GraphExecutionLimits {
        self.limits
    }

    /// Returns SHA-256 over the exact canonical definition bytes.
    #[must_use]
    pub const fn definition_digest(&self) -> Digest {
        self.definition_digest
    }

    /// Returns the compact reference stored by every checkpoint.
    #[must_use]
    pub fn reference(&self) -> GraphReference {
        GraphReference::new(
            self.identity.clone(),
            self.definition_digest,
            self.state_schema.clone(),
        )
    }

    /// Returns exact canonical descriptor bytes excluding the redundant digest.
    ///
    /// # Errors
    ///
    /// Fails closed if the canonical serializer rejects the closed descriptor.
    pub fn canonical_definition_bytes(&self) -> Result<Vec<u8>, GraphCompileError> {
        serde_json_canonicalizer::to_vec(&GraphDefinitionDigestWire {
            identity: &self.identity,
            input_schema: &self.input_schema,
            state_schema: &self.state_schema,
            update_schema: &self.update_schema,
            output_schema: &self.output_schema,
            reducer: &self.reducer,
            entry_nodes: &self.entry_nodes,
            nodes: &self.nodes,
            limits: self.limits,
        })
        .map_err(|_| GraphCompileError::CanonicalSerialization)
    }

    /// Looks up one compiled node by exact identity.
    #[must_use]
    pub fn node(&self, node_id: &NodeId) -> Option<&GraphNode> {
        self.nodes
            .binary_search_by(|node| node.node_id.cmp(node_id))
            .ok()
            .map(|index| &self.nodes[index])
    }

    /// Plans one complete deterministic barrier outside a storage transaction.
    ///
    /// # Errors
    ///
    /// Rejects graph/checkpoint drift, incomplete or substituted results,
    /// schema failures, unpinned reducers, invalid controls, step/parallelism
    /// limits, reducer panic/failure, and successor/barrier construction errors.
    pub fn plan_barrier<V, R>(
        &self,
        base: &Checkpoint,
        results: &[PendingNodeResult],
        successor_id: CheckpointId,
        schemas: &V,
        reducer: &R,
    ) -> Result<GraphBarrierPlan, GraphBarrierPlanError>
    where
        V: GraphSchemaValidator + ?Sized,
        R: GraphReducer + ?Sized,
    {
        GraphBarrierPlanner::new(self, base, results, successor_id, schemas, reducer).plan()
    }
}

fn collect_graph_nodes<I>(nodes: I) -> Result<BTreeMap<NodeId, GraphNode>, GraphCompileError>
where
    I: IntoIterator<Item = GraphNode>,
{
    let mut by_id = BTreeMap::new();
    let mut route_count = 0_usize;
    let mut route_owners = BTreeMap::new();
    for node in nodes {
        if by_id.len() == CompiledGraph::MAX_NODES {
            return Err(GraphCompileError::TooManyNodes {
                maximum: CompiledGraph::MAX_NODES,
                actual: CompiledGraph::MAX_NODES + 1,
            });
        }
        route_count =
            route_count
                .checked_add(node.routes.len())
                .ok_or(GraphCompileError::TooManyRoutes {
                    maximum: CompiledGraph::MAX_ROUTES,
                    actual: usize::MAX,
                })?;
        if route_count > CompiledGraph::MAX_ROUTES {
            return Err(GraphCompileError::TooManyRoutes {
                maximum: CompiledGraph::MAX_ROUTES,
                actual: route_count,
            });
        }
        for route in node.routes.iter() {
            if let Some(first_node) =
                route_owners.insert(route.route_id.clone(), node.node_id.clone())
            {
                return Err(GraphCompileError::DuplicateRoute {
                    route_id: route.route_id.clone(),
                    first_node,
                    second_node: node.node_id.clone(),
                });
            }
        }
        let node_id = node.node_id.clone();
        if by_id.insert(node_id.clone(), node).is_some() {
            return Err(GraphCompileError::DuplicateNode { node_id });
        }
    }
    if by_id.is_empty() {
        return Err(GraphCompileError::EmptyNodes);
    }
    Ok(by_id)
}

fn validate_compiled_shape(
    nodes: &BTreeMap<NodeId, GraphNode>,
    entry_nodes: &ReadyNodes,
    limits: GraphExecutionLimits,
) -> Result<(), GraphCompileError> {
    if entry_nodes.is_empty() {
        return Err(GraphCompileError::EmptyEntryNodes);
    }
    if entry_nodes.len() > usize::from(limits.maximum_parallelism) {
        return Err(GraphCompileError::ParallelismExceeded {
            node_id: None,
            maximum: usize::from(limits.maximum_parallelism),
            actual: entry_nodes.len(),
        });
    }
    for entry in entry_nodes {
        if !nodes.contains_key(entry) {
            return Err(GraphCompileError::MissingEntryNode {
                node_id: entry.clone(),
            });
        }
    }
    for node in nodes.values() {
        validate_node_successors(nodes, node, limits)?;
    }
    validate_reachability(nodes, entry_nodes)?;
    validate_boundary_paths(nodes)
}

fn validate_node_successors(
    nodes: &BTreeMap<NodeId, GraphNode>,
    node: &GraphNode,
    limits: GraphExecutionLimits,
) -> Result<(), GraphCompileError> {
    for successors in node.successor_sets() {
        if successors.len() > usize::from(limits.maximum_parallelism) {
            return Err(GraphCompileError::ParallelismExceeded {
                node_id: Some(node.node_id.clone()),
                maximum: usize::from(limits.maximum_parallelism),
                actual: successors.len(),
            });
        }
        for successor in successors {
            if !nodes.contains_key(successor) {
                return Err(GraphCompileError::MissingSuccessorNode {
                    from_node: node.node_id.clone(),
                    target: successor.clone(),
                });
            }
        }
    }
    Ok(())
}

impl fmt::Debug for CompiledGraph {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledGraph")
            .field("identity", &self.identity)
            .field("input_schema", &self.input_schema)
            .field("state_schema", &self.state_schema)
            .field("update_schema", &self.update_schema)
            .field("output_schema", &self.output_schema)
            .field("reducer", &self.reducer)
            .field("entry_nodes", &self.entry_nodes)
            .field("node_count", &self.nodes.len())
            .field("limits", &self.limits)
            .field("definition_digest", &self.definition_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for CompiledGraph {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            identity: CapabilityIdentity,
            input_schema: SchemaReference,
            state_schema: SchemaReference,
            update_schema: SchemaReference,
            output_schema: SchemaReference,
            reducer: GraphReducerReference,
            entry_nodes: ReadyNodes,
            nodes: Vec<GraphNode>,
            limits: GraphExecutionLimits,
            definition_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::build(
            wire.identity,
            wire.input_schema,
            wire.state_schema,
            wire.update_schema,
            wire.output_schema,
            wire.reducer,
            wire.entry_nodes,
            wire.nodes,
            wire.limits,
            Some(wire.definition_digest),
        )
        .map_err(de::Error::custom)
    }
}

#[derive(Serialize)]
struct GraphDefinitionDigestWire<'a> {
    identity: &'a CapabilityIdentity,
    input_schema: &'a SchemaReference,
    state_schema: &'a SchemaReference,
    update_schema: &'a SchemaReference,
    output_schema: &'a SchemaReference,
    reducer: &'a GraphReducerReference,
    entry_nodes: &'a ReadyNodes,
    nodes: &'a [GraphNode],
    limits: GraphExecutionLimits,
}

fn validate_reachability(
    nodes: &BTreeMap<NodeId, GraphNode>,
    entries: &ReadyNodes,
) -> Result<(), GraphCompileError> {
    let mut reachable = BTreeSet::new();
    let mut queue = VecDeque::new();
    for entry in entries {
        queue.push_back(entry.clone());
    }
    while let Some(node_id) = queue.pop_front() {
        if !reachable.insert(node_id.clone()) {
            continue;
        }
        let node = &nodes[&node_id];
        for successors in node.successor_sets() {
            for successor in successors {
                queue.push_back(successor.clone());
            }
        }
    }
    if let Some(node_id) = nodes.keys().find(|node_id| !reachable.contains(*node_id)) {
        return Err(GraphCompileError::UnreachableNode {
            node_id: node_id.clone(),
        });
    }
    Ok(())
}

fn validate_boundary_paths(nodes: &BTreeMap<NodeId, GraphNode>) -> Result<(), GraphCompileError> {
    let mut can_reach_boundary = nodes
        .values()
        .filter(|node| node.is_boundary())
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    loop {
        let before = can_reach_boundary.len();
        for node in nodes.values() {
            if can_reach_boundary.contains(&node.node_id) {
                continue;
            }
            if node.successor_sets().any(|successors| {
                successors
                    .iter()
                    .any(|successor| can_reach_boundary.contains(successor))
            }) {
                can_reach_boundary.insert(node.node_id.clone());
            }
        }
        if can_reach_boundary.len() == before {
            break;
        }
    }
    if let Some(node_id) = nodes
        .keys()
        .find(|node_id| !can_reach_boundary.contains(*node_id))
    {
        return Err(GraphCompileError::NoBoundaryPath {
            node_id: node_id.clone(),
        });
    }
    Ok(())
}

/// Invalid canonical graph definition.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphCompileError {
    /// No executable node was declared.
    #[error("compiled graph must contain at least one node")]
    EmptyNodes,
    /// The graph exceeded the immutable node ceiling.
    #[error("compiled graph contains {actual} nodes; maximum is {maximum}")]
    TooManyNodes {
        /// Immutable ceiling.
        maximum: usize,
        /// First count beyond the ceiling.
        actual: usize,
    },
    /// A stable node identity was repeated.
    #[error("compiled graph contains duplicate node {node_id}")]
    DuplicateNode {
        /// Repeated node identity.
        node_id: NodeId,
    },
    /// No initial runnable node was declared.
    #[error("compiled graph must contain at least one entry node")]
    EmptyEntryNodes,
    /// An entry identity was absent from the node table.
    #[error("compiled graph entry node {node_id} does not exist")]
    MissingEntryNode {
        /// Missing identity.
        node_id: NodeId,
    },
    /// A control branch referenced an absent node.
    #[error("compiled graph node {from_node} references missing successor {target}")]
    MissingSuccessorNode {
        /// Declaring node.
        from_node: NodeId,
        /// Missing target.
        target: NodeId,
    },
    /// A route identity was reused by two nodes.
    #[error("compiled graph route {route_id} is declared by both {first_node} and {second_node}")]
    DuplicateRoute {
        /// Reused route identity.
        route_id: RouteId,
        /// First declaring node.
        first_node: NodeId,
        /// Second declaring node.
        second_node: NodeId,
    },
    /// The graph exceeded its aggregate route ceiling.
    #[error("compiled graph contains {actual} routes; maximum is {maximum}")]
    TooManyRoutes {
        /// Immutable ceiling.
        maximum: usize,
        /// Rejected count.
        actual: usize,
    },
    /// A declared ready set exceeded graph-specific parallelism.
    #[error("compiled graph ready set contains {actual} nodes; graph maximum is {maximum}")]
    ParallelismExceeded {
        /// Declaring node, or `None` for the entry set.
        node_id: Option<NodeId>,
        /// Graph-specific ceiling.
        maximum: usize,
        /// Rejected count.
        actual: usize,
    },
    /// A node could never be activated from any entry.
    #[error("compiled graph node {node_id} is unreachable from every entry")]
    UnreachableNode {
        /// Unreachable identity.
        node_id: NodeId,
    },
    /// A node could not reach a wait or terminal boundary.
    #[error("compiled graph node {node_id} cannot reach a wait or terminal boundary")]
    NoBoundaryPath {
        /// Non-terminating identity.
        node_id: NodeId,
    },
    /// The canonical descriptor exceeded its resource ceiling.
    #[error("compiled graph descriptor is {actual} bytes; maximum is {maximum}")]
    DefinitionTooLarge {
        /// Immutable byte ceiling.
        maximum: usize,
        /// Observed canonical bytes.
        actual: usize,
    },
    /// Canonical JSON serialization unexpectedly failed.
    #[error("compiled graph canonical serialization failed")]
    CanonicalSerialization,
    /// Persisted definition checksum did not match the descriptor.
    #[error("compiled graph definition digest does not match its fields")]
    DigestMismatch,
}

/// Public-safe reason a schema registry could not validate a value.
///
/// Registries retain detailed diagnostics in protected telemetry. The graph
/// planner intentionally carries only this closed classification so schema or
/// user data cannot leak through errors, logs, or protocol adapters.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphSchemaValidationError {
    /// The value did not satisfy the exact pinned schema.
    #[error("value was rejected by the pinned schema")]
    Rejected,
    /// The exact schema implementation was unavailable.
    #[error("pinned schema was unavailable")]
    Unavailable,
}

/// Local registry capable of validating one exact schema-pinned JSON value.
///
/// Implementations must resolve only pre-registered canonical schema bytes and
/// verify [`SchemaReference::digest`] before validation. They must not fetch a
/// schema from its URI while a run is executing.
pub trait GraphSchemaValidator: Send + Sync {
    /// Validates `value` against the exact immutable schema reference.
    fn validate(
        &self,
        schema: &SchemaReference,
        value: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError>;
}

/// Public-safe failure returned by one pure graph reducer.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphReducerError {
    /// The ordered state/update combination was semantically invalid.
    #[error("graph reducer rejected the ordered update batch")]
    Rejected,
    /// The pinned implementation could not execute on this worker.
    #[error("pinned graph reducer was unavailable")]
    Unavailable,
    /// The reducer could not satisfy an enforced resource bound.
    #[error("graph reducer exceeded an enforced resource limit")]
    ResourceLimit,
}

/// One immutable update presented to a reducer in canonical node order.
#[derive(Clone, Copy, Debug)]
pub struct GraphReducerInput<'a> {
    node_id: &'a NodeId,
    update: &'a NodeStateUpdate,
}

impl<'a> GraphReducerInput<'a> {
    const fn new(node_id: &'a NodeId, update: &'a NodeStateUpdate) -> Self {
        Self { node_id, update }
    }

    /// Returns the stable logical node identity controlling reduction order.
    #[must_use]
    pub const fn node_id(self) -> &'a NodeId {
        self.node_id
    }

    /// Returns the exact schema-pinned update.
    #[must_use]
    pub const fn update(self) -> &'a NodeStateUpdate {
        self.update
    }
}

/// Exactly pinned pure reducer implementation used by graph planning.
///
/// The runtime invokes this trait only after all external model/tool work has
/// committed and never while a storage transaction is open. Implementations
/// must be deterministic, side-effect free, bounded, and must treat `updates`
/// in the supplied order. Reducer conformance tests remain a release gate;
/// recovery re-executes the reducer and compares the resulting barrier intent.
pub trait GraphReducer: Send + Sync {
    /// Returns the immutable implementation identity loaded by the registry.
    fn reference(&self) -> &GraphReducerReference;

    /// Reduces an ordered update batch into candidate successor state.
    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError>;
}

/// Semantic lifecycle outcome selected by one complete graph barrier.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GraphBarrierDisposition {
    /// Commit the successor and keep ordinary active execution.
    Continue,
    /// Commit the successor and atomically suspend on these conditions.
    Wait {
        /// Complete stable condition batch gathered from node results.
        waits: NodeWaits,
    },
    /// Commit the terminal successor and complete with this validated output.
    Terminal {
        /// Exact graph output to convert into the admitted agent result.
        output: NodeTerminalOutput,
    },
}

impl GraphBarrierDisposition {
    /// Returns the wait batch for a suspending barrier.
    #[must_use]
    pub const fn waits(&self) -> Option<&NodeWaits> {
        match self {
            Self::Wait { waits } => Some(waits),
            Self::Continue | Self::Terminal { .. } => None,
        }
    }

    /// Returns the terminal output for a successful terminal barrier.
    #[must_use]
    pub const fn terminal_output(&self) -> Option<&NodeTerminalOutput> {
        match self {
            Self::Terminal { output } => Some(output),
            Self::Continue | Self::Wait { .. } => None,
        }
    }
}

/// Complete deterministic plan ready for an existing atomic barrier API.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphBarrierPlan {
    barrier: CheckpointBarrier,
    disposition: GraphBarrierDisposition,
}

impl GraphBarrierPlan {
    /// Returns the integrity-bound atomic checkpoint barrier intent.
    #[must_use]
    pub const fn barrier(&self) -> &CheckpointBarrier {
        &self.barrier
    }

    /// Returns the lifecycle action selected by complete node controls.
    #[must_use]
    pub const fn disposition(&self) -> &GraphBarrierDisposition {
        &self.disposition
    }

    /// Consumes the plan into its storage barrier and lifecycle disposition.
    #[must_use]
    pub fn into_parts(self) -> (CheckpointBarrier, GraphBarrierDisposition) {
        (self.barrier, self.disposition)
    }
}

struct GraphBarrierPlanner<'a, V: ?Sized, R: ?Sized> {
    graph: &'a CompiledGraph,
    base: &'a Checkpoint,
    results: &'a [PendingNodeResult],
    successor_id: CheckpointId,
    schemas: &'a V,
    reducer: &'a R,
}

impl<'a, V, R> GraphBarrierPlanner<'a, V, R>
where
    V: GraphSchemaValidator + ?Sized,
    R: GraphReducer + ?Sized,
{
    const fn new(
        graph: &'a CompiledGraph,
        base: &'a Checkpoint,
        results: &'a [PendingNodeResult],
        successor_id: CheckpointId,
        schemas: &'a V,
        reducer: &'a R,
    ) -> Self {
        Self {
            graph,
            base,
            results,
            successor_id,
            schemas,
            reducer,
        }
    }

    fn plan(self) -> Result<GraphBarrierPlan, GraphBarrierPlanError> {
        self.validate_checkpoint()?;
        if self.reducer.reference() != self.graph.reducer() {
            return Err(GraphBarrierPlanError::ReducerReferenceMismatch);
        }
        validate_schema(
            self.schemas,
            self.graph.state_schema(),
            self.base.state().data(),
            GraphValueKind::BaseState,
            None,
        )?;

        let ordered = self.ordered_results()?;
        let resolved = self.resolve_controls(&ordered)?;
        let state = self.reduce_state(&resolved.reducer_inputs)?;
        let successor =
            CheckpointWrite::successor(self.successor_id, self.base, state, resolved.ready_nodes)
                .map_err(|source| GraphBarrierPlanError::InvalidSuccessor { source })?;
        let heads = ordered.iter().map(|result| result.head());
        let barrier = CheckpointBarrier::new(self.base, successor, heads)
            .map_err(|source| GraphBarrierPlanError::InvalidBarrier { source })?;
        Ok(GraphBarrierPlan {
            barrier,
            disposition: resolved.disposition,
        })
    }

    fn resolve_controls(
        &self,
        ordered: &[&'a PendingNodeResult],
    ) -> Result<ResolvedControls<'a>, GraphBarrierPlanError> {
        let mut reducer_inputs = Vec::with_capacity(ordered.len());
        let mut next_nodes = BTreeSet::new();
        let mut waits = Vec::<NodeWait>::new();
        let mut terminal = None;

        for result in ordered {
            let intent = result.intent();
            let node_id = intent.activation().node_id();
            let node = self.graph.node(node_id).ok_or_else(|| {
                GraphBarrierPlanError::UnknownReadyNode {
                    node_id: node_id.clone(),
                }
            })?;
            if let Some(update) = intent.state_change().update() {
                if update.schema() != self.graph.update_schema() {
                    return Err(GraphBarrierPlanError::UpdateSchemaMismatch {
                        node_id: node_id.clone(),
                    });
                }
                validate_schema(
                    self.schemas,
                    update.schema(),
                    update.data(),
                    GraphValueKind::NodeUpdate,
                    Some(node_id),
                )?;
                reducer_inputs.push(GraphReducerInput::new(node_id, update));
            }

            match intent.control() {
                NodeControl::Continue => {
                    let successors = node.continue_to().ok_or_else(|| {
                        GraphBarrierPlanError::UndeclaredControl {
                            node_id: node_id.clone(),
                            control: NodeControlKind::Continue,
                        }
                    })?;
                    next_nodes.extend(successors.iter().cloned());
                }
                NodeControl::Route { route_id } => {
                    let route = node.routes().get(route_id).ok_or_else(|| {
                        GraphBarrierPlanError::UndeclaredRoute {
                            node_id: node_id.clone(),
                            route_id: route_id.clone(),
                        }
                    })?;
                    next_nodes.extend(route.successors().iter().cloned());
                }
                NodeControl::Wait { waits: node_waits } => {
                    let successors =
                        node.wait_to()
                            .ok_or_else(|| GraphBarrierPlanError::UndeclaredControl {
                                node_id: node_id.clone(),
                                control: NodeControlKind::Wait,
                            })?;
                    next_nodes.extend(successors.iter().cloned());
                    waits.extend(node_waits.iter().cloned());
                }
                NodeControl::Terminal { output } => {
                    if !node.allows_terminal() {
                        return Err(GraphBarrierPlanError::UndeclaredControl {
                            node_id: node_id.clone(),
                            control: NodeControlKind::Terminal,
                        });
                    }
                    if output.schema() != self.graph.output_schema() {
                        return Err(GraphBarrierPlanError::TerminalOutputSchemaMismatch {
                            node_id: node_id.clone(),
                        });
                    }
                    validate_schema(
                        self.schemas,
                        output.schema(),
                        output.data(),
                        GraphValueKind::TerminalOutput,
                        Some(node_id),
                    )?;
                    if terminal.replace(output.clone()).is_some() {
                        return Err(GraphBarrierPlanError::TerminalNotExclusive);
                    }
                }
            }
        }

        let disposition = resolve_disposition(ordered.len(), &next_nodes, waits, terminal)?;
        if next_nodes.len() > usize::from(self.graph.limits.maximum_parallelism) {
            return Err(GraphBarrierPlanError::SuccessorParallelismExceeded {
                maximum: usize::from(self.graph.limits.maximum_parallelism),
                actual: next_nodes.len(),
            });
        }
        let ready_nodes = ReadyNodes::try_new(next_nodes).map_err(|_| {
            GraphBarrierPlanError::SuccessorParallelismExceeded {
                maximum: ReadyNodes::MAX_LEN,
                actual: ReadyNodes::MAX_LEN + 1,
            }
        })?;
        Ok(ResolvedControls {
            reducer_inputs,
            ready_nodes,
            disposition,
        })
    }

    fn reduce_state(
        &self,
        reducer_inputs: &[GraphReducerInput<'_>],
    ) -> Result<CheckpointState, GraphBarrierPlanError> {
        let reduced = if reducer_inputs.is_empty() {
            self.base.state().data().clone()
        } else {
            catch_unwind(AssertUnwindSafe(|| {
                self.reducer
                    .reduce(self.base.state().data(), reducer_inputs)
            }))
            .map_err(|_| GraphBarrierPlanError::ReducerPanicked)?
            .map_err(|source| GraphBarrierPlanError::ReducerFailed { source })?
        };
        validate_schema(
            self.schemas,
            self.graph.state_schema(),
            &reduced,
            GraphValueKind::ReducedState,
            None,
        )?;
        CheckpointState::new(self.graph.state_schema().clone(), reduced)
            .map_err(|source| GraphBarrierPlanError::InvalidReducedState { source })
    }

    fn validate_checkpoint(&self) -> Result<(), GraphBarrierPlanError> {
        if self.base.graph() != &self.graph.reference() {
            return Err(GraphBarrierPlanError::GraphReferenceMismatch);
        }
        if self.base.superstep() == Superstep::INITIAL
            && self.base.ready_nodes() != self.graph.entry_nodes()
        {
            return Err(GraphBarrierPlanError::InitialReadySetMismatch);
        }
        if self.base.superstep().get() >= self.graph.limits.maximum_supersteps.get() {
            return Err(GraphBarrierPlanError::SuperstepLimitReached {
                maximum: self.graph.limits.maximum_supersteps,
                actual: self.base.superstep(),
            });
        }
        if self.base.ready_nodes().len() > usize::from(self.graph.limits.maximum_parallelism) {
            return Err(GraphBarrierPlanError::BaseParallelismExceeded {
                maximum: usize::from(self.graph.limits.maximum_parallelism),
                actual: self.base.ready_nodes().len(),
            });
        }
        for node_id in self.base.ready_nodes() {
            if self.graph.node(node_id).is_none() {
                return Err(GraphBarrierPlanError::UnknownReadyNode {
                    node_id: node_id.clone(),
                });
            }
        }
        Ok(())
    }

    fn ordered_results(&self) -> Result<Vec<&PendingNodeResult>, GraphBarrierPlanError> {
        if self.results.len() != self.base.ready_nodes().len() {
            return Err(GraphBarrierPlanError::ResultCountMismatch {
                expected: self.base.ready_nodes().len(),
                actual: self.results.len(),
            });
        }
        let mut ordered = BTreeMap::new();
        for result in self.results {
            let activation = result.intent().activation();
            if activation.base_checkpoint() != &self.base.head() {
                return Err(GraphBarrierPlanError::ResultBaseMismatch {
                    node_id: activation.node_id().clone(),
                });
            }
            if !activation.graph_namespace().is_root() {
                return Err(GraphBarrierPlanError::NonRootActivation {
                    node_id: activation.node_id().clone(),
                });
            }
            let expected = NodeActivation::for_ready_root(self.base, activation.node_id().clone())
                .map_err(|_| GraphBarrierPlanError::ActivationMismatch {
                    node_id: activation.node_id().clone(),
                })?;
            if activation != &expected {
                return Err(GraphBarrierPlanError::ActivationMismatch {
                    node_id: activation.node_id().clone(),
                });
            }
            if ordered
                .insert(activation.node_id().clone(), result)
                .is_some()
            {
                return Err(GraphBarrierPlanError::DuplicateResultNode {
                    node_id: activation.node_id().clone(),
                });
            }
        }
        for node_id in self.base.ready_nodes() {
            if !ordered.contains_key(node_id) {
                return Err(GraphBarrierPlanError::MissingResultNode {
                    node_id: node_id.clone(),
                });
            }
        }
        Ok(ordered.into_values().collect())
    }
}

struct ResolvedControls<'a> {
    reducer_inputs: Vec<GraphReducerInput<'a>>,
    ready_nodes: ReadyNodes,
    disposition: GraphBarrierDisposition,
}

fn resolve_disposition(
    result_count: usize,
    next_nodes: &BTreeSet<NodeId>,
    waits: Vec<NodeWait>,
    terminal: Option<NodeTerminalOutput>,
) -> Result<GraphBarrierDisposition, GraphBarrierPlanError> {
    if let Some(output) = terminal {
        if result_count != 1 || !waits.is_empty() || !next_nodes.is_empty() {
            return Err(GraphBarrierPlanError::TerminalNotExclusive);
        }
        return Ok(GraphBarrierDisposition::Terminal { output });
    }
    if waits.is_empty() {
        if next_nodes.is_empty() {
            return Err(GraphBarrierPlanError::AdvancingBarrierWithoutSuccessor);
        }
        return Ok(GraphBarrierDisposition::Continue);
    }
    NodeWaits::try_new(waits)
        .map(|waits| GraphBarrierDisposition::Wait { waits })
        .map_err(|source| GraphBarrierPlanError::InvalidWaitBatch { source })
}

fn validate_schema<V>(
    schemas: &V,
    schema: &SchemaReference,
    value: &BoundedJson,
    kind: GraphValueKind,
    node_id: Option<&NodeId>,
) -> Result<(), GraphBarrierPlanError>
where
    V: GraphSchemaValidator + ?Sized,
{
    match catch_unwind(AssertUnwindSafe(|| schemas.validate(schema, value))) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(source)) => Err(GraphBarrierPlanError::SchemaValidation {
            kind,
            node_id: node_id.cloned(),
            source,
        }),
        Err(_) => Err(GraphBarrierPlanError::SchemaValidatorPanicked {
            kind,
            node_id: node_id.cloned(),
        }),
    }
}

/// Closed value class used in public-safe graph validation failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GraphValueKind {
    /// Immutable base checkpoint state.
    BaseState,
    /// One node's typed update.
    NodeUpdate,
    /// One graph terminal output.
    TerminalOutput,
    /// Reducer-produced successor state.
    ReducedState,
}

impl fmt::Display for GraphValueKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BaseState => "base state",
            Self::NodeUpdate => "node update",
            Self::TerminalOutput => "terminal output",
            Self::ReducedState => "reduced state",
        })
    }
}

/// Failure to derive one deterministic, storage-ready barrier plan.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphBarrierPlanError {
    /// The checkpoint pinned another graph definition or state schema.
    #[error("checkpoint graph reference does not match the compiled graph")]
    GraphReferenceMismatch,
    /// Superstep zero did not contain the exact compiled entry set.
    #[error("initial checkpoint ready set does not match compiled graph entries")]
    InitialReadySetMismatch,
    /// The next barrier would exceed the graph step ceiling.
    #[error("graph superstep limit {maximum} was reached at checkpoint {actual}")]
    SuperstepLimitReached {
        /// Compiled ceiling.
        maximum: Superstep,
        /// Current checkpoint position.
        actual: Superstep,
    },
    /// The current checkpoint already exceeded graph parallelism.
    #[error("base checkpoint has {actual} ready nodes; graph maximum is {maximum}")]
    BaseParallelismExceeded {
        /// Compiled ceiling.
        maximum: usize,
        /// Rejected count.
        actual: usize,
    },
    /// A checkpoint referenced a node absent from the compiled graph.
    #[error("base checkpoint contains unknown ready node {node_id}")]
    UnknownReadyNode {
        /// Unknown identity.
        node_id: NodeId,
    },
    /// The reducer implementation did not match its pinned reference.
    #[error("loaded reducer does not match the compiled graph reference")]
    ReducerReferenceMismatch,
    /// The complete result count differed from the ready set.
    #[error("barrier contains {actual} results; expected {expected}")]
    ResultCountMismatch {
        /// Exact ready-node count.
        expected: usize,
        /// Supplied result count.
        actual: usize,
    },
    /// One ready node appeared more than once.
    #[error("barrier contains duplicate result node {node_id}")]
    DuplicateResultNode {
        /// Duplicated identity.
        node_id: NodeId,
    },
    /// One expected ready node had no result.
    #[error("barrier is missing result node {node_id}")]
    MissingResultNode {
        /// Missing identity.
        node_id: NodeId,
    },
    /// A result belonged to another immutable checkpoint.
    #[error("result for node {node_id} does not belong to the base checkpoint")]
    ResultBaseMismatch {
        /// Rejected result node.
        node_id: NodeId,
    },
    /// Root graph planning received a nested activation.
    #[error("result for node {node_id} belongs to a non-root graph namespace")]
    NonRootActivation {
        /// Rejected result node.
        node_id: NodeId,
    },
    /// A result's logical input did not match deterministic activation derivation.
    #[error("result activation for node {node_id} does not match the base checkpoint")]
    ActivationMismatch {
        /// Rejected result node.
        node_id: NodeId,
    },
    /// One update substituted another schema.
    #[error("node {node_id} update schema does not match the compiled graph")]
    UpdateSchemaMismatch {
        /// Rejected result node.
        node_id: NodeId,
    },
    /// One terminal output substituted another schema.
    #[error("node {node_id} terminal output schema does not match the compiled graph")]
    TerminalOutputSchemaMismatch {
        /// Rejected result node.
        node_id: NodeId,
    },
    /// The exact schema implementation rejected or could not validate a value.
    #[error("graph {kind} schema validation failed")]
    SchemaValidation {
        /// Closed value class.
        kind: GraphValueKind,
        /// Producing node for update/output values.
        node_id: Option<NodeId>,
        /// Public-safe registry classification.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// A schema adapter panicked instead of returning a closed failure.
    #[error("graph {kind} schema validator panicked")]
    SchemaValidatorPanicked {
        /// Closed value class.
        kind: GraphValueKind,
        /// Producing node for update/output values.
        node_id: Option<NodeId>,
    },
    /// A result selected a control kind absent from its node declaration.
    #[error("node {node_id} selected undeclared {control:?} control")]
    UndeclaredControl {
        /// Producing node.
        node_id: NodeId,
        /// Rejected control kind.
        control: NodeControlKind,
    },
    /// A result selected an undeclared conditional route.
    #[error("node {node_id} selected undeclared route {route_id}")]
    UndeclaredRoute {
        /// Producing node.
        node_id: NodeId,
        /// Rejected route.
        route_id: RouteId,
    },
    /// Terminal completion was mixed with another active result/control.
    #[error("terminal graph result must be the only activation in its barrier")]
    TerminalNotExclusive,
    /// An ordinary advancing barrier resolved no successor.
    #[error("non-terminal graph barrier must produce at least one successor")]
    AdvancingBarrierWithoutSuccessor,
    /// Combined branches exceeded graph parallelism.
    #[error("successor contains {actual} ready nodes; graph maximum is {maximum}")]
    SuccessorParallelismExceeded {
        /// Compiled ceiling.
        maximum: usize,
        /// Rejected combined count.
        actual: usize,
    },
    /// Parallel waits could not form one atomic registration batch.
    #[error("graph wait controls do not form one valid batch: {source}")]
    InvalidWaitBatch {
        /// Exact wait collection failure.
        #[source]
        source: NodeWaitsError,
    },
    /// The pure reducer returned a closed failure.
    #[error("graph reducer failed: {source}")]
    ReducerFailed {
        /// Public-safe reducer classification.
        #[source]
        source: GraphReducerError,
    },
    /// Reducer code panicked instead of returning a closed failure.
    #[error("graph reducer panicked")]
    ReducerPanicked,
    /// Reducer output could not become integrity-bound checkpoint state.
    #[error("reduced graph state is invalid: {source}")]
    InvalidReducedState {
        /// Exact state construction failure.
        #[source]
        source: CheckpointStateError,
    },
    /// The derived successor violated checkpoint invariants.
    #[error("derived graph successor is invalid: {source}")]
    InvalidSuccessor {
        /// Exact checkpoint-write failure.
        #[source]
        source: CheckpointWriteError,
    },
    /// The final barrier violated complete-set or integrity invariants.
    #[error("derived graph barrier is invalid: {source}")]
    InvalidBarrier {
        /// Exact barrier failure.
        #[source]
        source: CheckpointBarrierError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AttemptId, CapabilityName, CapabilityReference, EventId, FencingEpoch, IssuerId,
        JournalHead, JournalSequence, JsonLimits, NodeInvocationBindings, NodeStateChange,
        PendingNodeResultIntent, PrincipalIdentity, RunFence, RunId, RunTimerKind, SchemaId,
        SubjectId, TenantId, TimerId, Timestamp, Version,
    };
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    fn identity(name: &str) -> CapabilityIdentity {
        CapabilityIdentity::new(
            PrincipalIdentity::new(
                "https://issuer.example.com/stateknot"
                    .parse::<IssuerId>()
                    .unwrap(),
                "graph-registry".parse::<SubjectId>().unwrap(),
            ),
            CapabilityReference::new(
                name.parse::<CapabilityName>().unwrap(),
                Version::new(1, 0, 0),
            ),
        )
    }

    fn schema(name: &str) -> SchemaReference {
        SchemaReference::new(
            format!("https://schemas.example.com/{name}")
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(name),
        )
    }

    fn node_ids(values: &[&str]) -> ReadyNodes {
        ReadyNodes::try_new(values.iter().map(|value| NodeId::new(*value).unwrap())).unwrap()
    }

    fn route(route_id: &str, successors: &[&str]) -> GraphRoute {
        GraphRoute::new(RouteId::new(route_id).unwrap(), node_ids(successors)).unwrap()
    }

    fn node(
        node_id: &str,
        continue_to: Option<&[&str]>,
        routes: Vec<GraphRoute>,
        wait_to: Option<&[&str]>,
        terminal: bool,
    ) -> GraphNode {
        GraphNode::new(
            NodeId::new(node_id).unwrap(),
            continue_to.map(node_ids),
            GraphRoutes::try_new(routes).unwrap(),
            wait_to.map(node_ids),
            terminal,
        )
        .unwrap()
    }

    fn limits(maximum_parallelism: u16) -> GraphExecutionLimits {
        GraphExecutionLimits::new(Superstep::new(64).unwrap(), maximum_parallelism).unwrap()
    }

    fn parallel_graph() -> CompiledGraph {
        CompiledGraph::compile(
            identity("orders.graph"),
            schema("input"),
            schema("state"),
            schema("update"),
            schema("output"),
            GraphReducerReference::new(identity("orders.reducer"), Digest::sha256(b"reducer-v1")),
            node_ids(&["alpha", "beta"]),
            [
                node("join", None, Vec::new(), None, true),
                node("beta", None, vec![route("beta.ok", &["join"])], None, false),
                node("alpha", Some(&["join"]), Vec::new(), None, false),
            ],
            limits(4),
        )
        .unwrap()
    }

    fn parallel_four_graph() -> CompiledGraph {
        CompiledGraph::compile(
            identity("parallel-four.graph"),
            schema("parallel-four-input"),
            schema("parallel-four-state"),
            schema("parallel-four-update"),
            schema("parallel-four-output"),
            GraphReducerReference::new(
                identity("parallel-four.reducer"),
                Digest::sha256(b"parallel-four-reducer-v1"),
            ),
            node_ids(&["alpha", "beta", "delta", "gamma"]),
            [
                node("join", None, Vec::new(), None, true),
                node("alpha", Some(&["join"]), Vec::new(), None, false),
                node("beta", Some(&["join"]), Vec::new(), None, false),
                node("delta", Some(&["join"]), Vec::new(), None, false),
                node("gamma", Some(&["join"]), Vec::new(), None, false),
            ],
            limits(4),
        )
        .unwrap()
    }

    fn tenant() -> TenantId {
        TenantId::new("tenant-graph-test").unwrap()
    }

    fn run_id() -> RunId {
        "01912345-6789-7abc-8def-0123456789a1".parse().unwrap()
    }

    fn timestamp(offset: i64) -> Timestamp {
        let base = "2030-01-01T00:00:00.000000Z".parse::<Timestamp>().unwrap();
        Timestamp::from_unix_micros(base.unix_micros() + offset * 1_000_000).unwrap()
    }

    fn journal(sequence: u64) -> JournalHead {
        JournalHead::new(
            tenant(),
            run_id(),
            JournalSequence::new(sequence).unwrap(),
            format!("01912345-6789-7abc-8def-0123456789{sequence:02x}")
                .parse::<EventId>()
                .unwrap(),
            timestamp(i64::try_from(sequence).unwrap()),
            Digest::sha256(sequence.to_be_bytes()),
        )
    }

    fn checkpoint_id(suffix: &str) -> CheckpointId {
        format!("01912345-6789-7abc-8def-0123456788{suffix}")
            .parse()
            .unwrap()
    }

    fn checkpoint(graph: &CompiledGraph) -> Checkpoint {
        let state = CheckpointState::new(
            graph.state_schema().clone(),
            BoundedJson::try_from_value_with_limits(json!({"order": []}), JsonLimits::MAXIMUM)
                .unwrap(),
        )
        .unwrap();
        let write = CheckpointWrite::initial(
            tenant(),
            run_id(),
            checkpoint_id("a1"),
            graph.reference(),
            state,
            graph.entry_nodes().clone(),
        )
        .unwrap();
        Checkpoint::commit(write, journal(1)).unwrap()
    }

    fn result(
        graph: &CompiledGraph,
        base: &Checkpoint,
        node_name: &str,
        sequence: u64,
        control: NodeControl,
    ) -> PendingNodeResult {
        let node_id = NodeId::new(node_name).unwrap();
        let activation = NodeActivation::for_ready_root(base, node_id).unwrap();
        let update = NodeStateUpdate::new(
            graph.update_schema().clone(),
            BoundedJson::try_from_value_with_limits(
                json!({"value": node_name}),
                JsonLimits::MAXIMUM,
            )
            .unwrap(),
        )
        .unwrap();
        let intent = PendingNodeResultIntent::new(
            activation.clone(),
            NodeStateChange::Update { update },
            control,
            NodeInvocationBindings::empty(),
        )
        .unwrap();
        PendingNodeResult::commit(
            intent,
            RunFence::new(
                tenant(),
                run_id(),
                format!("01912345-6789-7abc-8def-0123456787{sequence:02x}")
                    .parse::<AttemptId>()
                    .unwrap(),
                FencingEpoch::new(sequence).unwrap(),
            ),
            journal(sequence + 1),
        )
        .unwrap()
    }

    struct AcceptSchemas;

    impl GraphSchemaValidator for AcceptSchemas {
        fn validate(
            &self,
            _schema: &SchemaReference,
            _value: &BoundedJson,
        ) -> Result<(), GraphSchemaValidationError> {
            Ok(())
        }
    }

    struct StableReducer {
        reference: GraphReducerReference,
    }

    impl GraphReducer for StableReducer {
        fn reference(&self) -> &GraphReducerReference {
            &self.reference
        }

        fn reduce(
            &self,
            _state: &BoundedJson,
            updates: &[GraphReducerInput<'_>],
        ) -> Result<BoundedJson, GraphReducerError> {
            BoundedJson::try_from_value_with_limits(
                json!({
                    "order": updates
                        .iter()
                        .map(|input| input.node_id().as_str())
                        .collect::<Vec<_>>()
                }),
                JsonLimits::MAXIMUM,
            )
            .map_err(|_| GraphReducerError::ResourceLimit)
        }
    }

    #[test]
    fn compilation_is_canonical_reachable_and_integrity_bound() {
        let forward = parallel_graph();
        let reverse = CompiledGraph::compile(
            forward.identity().clone(),
            forward.input_schema().clone(),
            forward.state_schema().clone(),
            forward.update_schema().clone(),
            forward.output_schema().clone(),
            forward.reducer().clone(),
            forward.entry_nodes().clone(),
            forward.nodes().iter().cloned().rev(),
            forward.limits(),
        )
        .unwrap();
        assert_eq!(forward, reverse);
        assert_eq!(
            forward.canonical_definition_bytes().unwrap(),
            reverse.canonical_definition_bytes().unwrap()
        );
        assert_eq!(
            forward.definition_digest(),
            Digest::sha256(forward.canonical_definition_bytes().unwrap())
        );
        assert_eq!(
            from_value::<CompiledGraph>(to_value(&forward).unwrap()).unwrap(),
            forward
        );
        assert_eq!(
            forward.reference().definition_digest(),
            forward.definition_digest()
        );
        assert!(forward.nodes().iter().map(GraphNode::node_id).is_sorted());

        let mut tampered = to_value(&forward).unwrap();
        tampered["nodes"][2]["terminal"] = json!(false);
        assert!(from_value::<CompiledGraph>(tampered).is_err());
    }

    #[test]
    fn compilation_rejects_missing_unreachable_nonterminating_and_duplicate_routes() {
        let compile = |nodes: Vec<GraphNode>| {
            CompiledGraph::compile(
                identity("invalid.graph"),
                schema("input"),
                schema("state"),
                schema("update"),
                schema("output"),
                GraphReducerReference::new(
                    identity("invalid.reducer"),
                    Digest::sha256(b"invalid-reducer"),
                ),
                node_ids(&["entry"]),
                nodes,
                limits(8),
            )
        };

        assert!(matches!(
            compile(vec![node(
                "entry",
                Some(&["missing"]),
                Vec::new(),
                None,
                false
            )]),
            Err(GraphCompileError::MissingSuccessorNode { .. })
        ));
        assert!(matches!(
            compile(vec![
                node("entry", None, Vec::new(), None, true),
                node("orphan", None, Vec::new(), None, true),
            ]),
            Err(GraphCompileError::UnreachableNode { .. })
        ));
        assert!(matches!(
            compile(vec![node(
                "entry",
                Some(&["entry"]),
                Vec::new(),
                None,
                false
            )]),
            Err(GraphCompileError::NoBoundaryPath { .. })
        ));
        assert!(matches!(
            compile(vec![
                node("entry", None, vec![route("same", &["second"])], None, true,),
                node("second", None, vec![route("same", &["entry"])], None, true,),
            ]),
            Err(GraphCompileError::DuplicateRoute { .. })
        ));
    }

    #[test]
    fn parallel_reduction_and_routing_ignore_completion_order() {
        let graph = parallel_graph();
        let base = checkpoint(&graph);
        let alpha = result(&graph, &base, "alpha", 2, NodeControl::Continue);
        let beta = result(
            &graph,
            &base,
            "beta",
            3,
            NodeControl::Route {
                route_id: RouteId::new("beta.ok").unwrap(),
            },
        );
        let reducer = StableReducer {
            reference: graph.reducer().clone(),
        };
        let successor_id = checkpoint_id("a2");
        let forward = graph
            .plan_barrier(
                &base,
                &[alpha.clone(), beta.clone()],
                successor_id,
                &AcceptSchemas,
                &reducer,
            )
            .unwrap();
        let reverse = graph
            .plan_barrier(
                &base,
                &[beta, alpha],
                successor_id,
                &AcceptSchemas,
                &reducer,
            )
            .unwrap();

        assert_eq!(forward, reverse);
        assert_eq!(forward.disposition(), &GraphBarrierDisposition::Continue);
        assert_eq!(
            forward.barrier().successor().ready_nodes(),
            &node_ids(&["join"])
        );
        assert_eq!(
            forward.barrier().successor().state().data().as_value(),
            &json!({"order": ["alpha", "beta"]})
        );
        assert!(
            forward
                .barrier()
                .result_heads()
                .iter()
                .map(|head| head.activation().node_id())
                .is_sorted()
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn every_parallel_completion_permutation_produces_the_same_barrier(
            ordering_keys in proptest::collection::vec(any::<u32>(), 4),
        ) {
            let graph = parallel_four_graph();
            let base = checkpoint(&graph);
            let results = [
                result(&graph, &base, "alpha", 2, NodeControl::Continue),
                result(&graph, &base, "beta", 3, NodeControl::Continue),
                result(&graph, &base, "delta", 4, NodeControl::Continue),
                result(&graph, &base, "gamma", 5, NodeControl::Continue),
            ];
            let reducer = StableReducer {
                reference: graph.reducer().clone(),
            };
            let successor_id = checkpoint_id("a4");
            let expected = graph
                .plan_barrier(
                    &base,
                    &results,
                    successor_id,
                    &AcceptSchemas,
                    &reducer,
                )
                .unwrap();
            let mut indices = [0_usize, 1, 2, 3];
            indices.sort_unstable_by_key(|index| (ordering_keys[*index], *index));
            let permuted = indices
                .into_iter()
                .map(|index| results[index].clone())
                .collect::<Vec<_>>();
            let actual = graph
                .plan_barrier(
                    &base,
                    &permuted,
                    successor_id,
                    &AcceptSchemas,
                    &reducer,
                )
                .unwrap();

            prop_assert_eq!(actual, expected);
        }
    }

    fn wait_graph() -> CompiledGraph {
        CompiledGraph::compile(
            identity("wait.graph"),
            schema("wait-input"),
            schema("wait-state"),
            schema("wait-update"),
            schema("wait-output"),
            GraphReducerReference::new(identity("wait.reducer"), Digest::sha256(b"wait-reducer")),
            node_ids(&["sleep-a", "sleep-b"]),
            [
                node("done", None, Vec::new(), None, true),
                node("sleep-a", None, Vec::new(), Some(&["done"]), false),
                node("sleep-b", None, Vec::new(), Some(&["done"]), false),
            ],
            limits(4),
        )
        .unwrap()
    }

    fn timer_wait(suffix: &str) -> NodeWaits {
        NodeWaits::try_new([NodeWait::timer(
            format!("01912345-6789-7abc-8def-0123456786{suffix}")
                .parse::<TimerId>()
                .unwrap(),
            RunTimerKind::Sleep,
            timestamp(10),
        )])
        .unwrap()
    }

    #[test]
    fn parallel_waits_form_one_batch_and_retain_declared_resume_successors() {
        let graph = wait_graph();
        let base = checkpoint(&graph);
        let a = result(
            &graph,
            &base,
            "sleep-a",
            2,
            NodeControl::Wait {
                waits: timer_wait("a1"),
            },
        );
        let b = result(
            &graph,
            &base,
            "sleep-b",
            3,
            NodeControl::Wait {
                waits: timer_wait("a2"),
            },
        );
        let reducer = StableReducer {
            reference: graph.reducer().clone(),
        };
        let plan = graph
            .plan_barrier(
                &base,
                &[b, a],
                checkpoint_id("a3"),
                &AcceptSchemas,
                &reducer,
            )
            .unwrap();

        assert_eq!(plan.disposition().waits().unwrap().len(), 2);
        assert_eq!(
            plan.barrier().successor().ready_nodes(),
            &node_ids(&["done"])
        );
    }

    #[test]
    fn terminal_control_is_schema_checked_and_exclusive() {
        let graph = CompiledGraph::compile(
            identity("terminal.graph"),
            schema("terminal-input"),
            schema("terminal-state"),
            schema("terminal-update"),
            schema("terminal-output"),
            GraphReducerReference::new(
                identity("terminal.reducer"),
                Digest::sha256(b"terminal-reducer"),
            ),
            node_ids(&["finish"]),
            [node("finish", None, Vec::new(), None, true)],
            limits(1),
        )
        .unwrap();
        let base = checkpoint(&graph);
        let output = NodeTerminalOutput::new(
            graph.output_schema().clone(),
            BoundedJson::try_from_value_with_limits(json!({"ok": true}), JsonLimits::MAXIMUM)
                .unwrap(),
        )
        .unwrap();
        let finished = result(
            &graph,
            &base,
            "finish",
            2,
            NodeControl::Terminal {
                output: output.clone(),
            },
        );
        let reducer = StableReducer {
            reference: graph.reducer().clone(),
        };
        let plan = graph
            .plan_barrier(
                &base,
                &[finished],
                checkpoint_id("a4"),
                &AcceptSchemas,
                &reducer,
            )
            .unwrap();
        assert_eq!(plan.disposition().terminal_output(), Some(&output));
        assert!(plan.barrier().successor().ready_nodes().is_empty());

        let wrong_reducer = StableReducer {
            reference: GraphReducerReference::new(
                identity("other.reducer"),
                Digest::sha256(b"other"),
            ),
        };
        assert_eq!(
            graph.plan_barrier(
                &base,
                &[],
                checkpoint_id("a5"),
                &AcceptSchemas,
                &wrong_reducer,
            ),
            Err(GraphBarrierPlanError::ReducerReferenceMismatch)
        );
    }

    #[test]
    fn schema_and_reducer_panics_fail_closed() {
        struct PanicSchemas;
        impl GraphSchemaValidator for PanicSchemas {
            fn validate(
                &self,
                _schema: &SchemaReference,
                _value: &BoundedJson,
            ) -> Result<(), GraphSchemaValidationError> {
                panic!("schema adapter bug")
            }
        }
        struct PanicReducer {
            reference: GraphReducerReference,
        }
        impl GraphReducer for PanicReducer {
            fn reference(&self) -> &GraphReducerReference {
                &self.reference
            }

            fn reduce(
                &self,
                _state: &BoundedJson,
                _updates: &[GraphReducerInput<'_>],
            ) -> Result<BoundedJson, GraphReducerError> {
                panic!("reducer bug")
            }
        }

        let graph = parallel_graph();
        let base = checkpoint(&graph);
        let alpha = result(&graph, &base, "alpha", 2, NodeControl::Continue);
        let beta = result(
            &graph,
            &base,
            "beta",
            3,
            NodeControl::Route {
                route_id: RouteId::new("beta.ok").unwrap(),
            },
        );
        let stable = StableReducer {
            reference: graph.reducer().clone(),
        };
        assert!(matches!(
            graph.plan_barrier(
                &base,
                &[alpha.clone(), beta.clone()],
                checkpoint_id("b1"),
                &PanicSchemas,
                &stable,
            ),
            Err(GraphBarrierPlanError::SchemaValidatorPanicked { .. })
        ));
        let panicking = PanicReducer {
            reference: graph.reducer().clone(),
        };
        assert_eq!(
            graph.plan_barrier(
                &base,
                &[alpha, beta],
                checkpoint_id("b2"),
                &AcceptSchemas,
                &panicking,
            ),
            Err(GraphBarrierPlanError::ReducerPanicked)
        );
    }

    #[test]
    fn graph_schemas_are_closed_and_publish_collection_bounds() {
        let graph_schema = to_value(schemars::schema_for!(CompiledGraph)).unwrap();
        assert_eq!(graph_schema["additionalProperties"], Value::Bool(false));
        assert_eq!(
            graph_schema["properties"]["nodes"]["maxItems"],
            json!(CompiledGraph::MAX_NODES)
        );
        let routes_schema = to_value(schemars::schema_for!(GraphRoutes)).unwrap();
        assert_eq!(routes_schema["maxItems"], json!(GraphRoutes::MAX_LEN));
    }
}
