// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Frozen graph, reducer, and node-executor bindings.

use std::{collections::HashMap, fmt, sync::Arc};

use stateknot_core::{
    BoxFuture, BudgetUsage, CancellationSignal, Checkpoint, CompiledGraph, Failure, GraphReducer,
    GraphReducerReference, GraphReference, NodeAttemptStartHead, NodeControl, NodeId,
    NodeInvocationBindings, NodeStateChange, RetryAdvice,
};
use thiserror::Error;

use crate::JsonSchemaRegistry;

/// Exact, already-started execution context passed to one graph node.
///
/// Construction proves that the durable attempt and immutable state snapshot
/// describe the same logical activation. The runtime creates this only after a
/// node-attempt start has committed. Node code receives no storage transaction
/// or mutable checkpoint handle.
#[derive(Clone)]
pub struct GraphNodeContext {
    attempt: NodeAttemptStartHead,
    checkpoint: Arc<Checkpoint>,
    cancellation: CancellationSignal,
}

impl GraphNodeContext {
    /// Binds one committed attempt to its exact immutable checkpoint snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`GraphNodeContextError`] if the attempt names another base
    /// checkpoint, tenant, or run.
    pub fn new(
        attempt: NodeAttemptStartHead,
        checkpoint: Arc<Checkpoint>,
        cancellation: CancellationSignal,
    ) -> Result<Self, GraphNodeContextError> {
        let activation = attempt.activation();
        if activation.tenant_id() != checkpoint.tenant_id() {
            return Err(GraphNodeContextError::TenantMismatch);
        }
        if activation.run_id() != checkpoint.run_id() {
            return Err(GraphNodeContextError::RunMismatch);
        }
        if activation.base_checkpoint() != &checkpoint.head() {
            return Err(GraphNodeContextError::CheckpointMismatch);
        }
        Ok(Self {
            attempt,
            checkpoint,
            cancellation,
        })
    }

    /// Returns the durable physical attempt proof.
    #[must_use]
    pub const fn attempt(&self) -> &NodeAttemptStartHead {
        &self.attempt
    }

    /// Returns the exact immutable state and ready-set snapshot.
    #[must_use]
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Returns the cooperative stop signal owned by the driver.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }
}

impl fmt::Debug for GraphNodeContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GraphNodeContext")
            .field("attempt", &self.attempt)
            .field("checkpoint", &self.checkpoint.head())
            .field("cancellation", &self.cancellation)
            .finish_non_exhaustive()
    }
}

/// Invalid durable attempt/checkpoint binding for node execution.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphNodeContextError {
    /// Attempt and checkpoint crossed tenant isolation.
    #[error("node execution context crosses a tenant boundary")]
    TenantMismatch,
    /// Attempt and checkpoint named different runs.
    #[error("node execution context crosses a run boundary")]
    RunMismatch,
    /// Attempt activation did not derive from the supplied checkpoint.
    #[error("node execution attempt does not belong to the supplied checkpoint")]
    CheckpointMismatch,
}

/// Successful semantic output of one graph-node attempt.
///
/// The durable driver rebinds these fields to the exact context activation by
/// constructing [`stateknot_core::PendingNodeResultIntent`]. Invocation
/// bindings are therefore revalidated before any success can commit.
#[derive(Clone, Debug)]
pub struct GraphNodeExecution {
    state_change: NodeStateChange,
    control: NodeControl,
    bindings: NodeInvocationBindings,
    usage: BudgetUsage,
}

impl GraphNodeExecution {
    /// Constructs one bounded semantic node result and its attempt usage.
    #[must_use]
    pub const fn new(
        state_change: NodeStateChange,
        control: NodeControl,
        bindings: NodeInvocationBindings,
        usage: BudgetUsage,
    ) -> Self {
        Self {
            state_change,
            control,
            bindings,
            usage,
        }
    }

    /// Returns the typed state contribution.
    #[must_use]
    pub const fn state_change(&self) -> &NodeStateChange {
        &self.state_change
    }

    /// Returns the closed graph control outcome.
    #[must_use]
    pub const fn control(&self) -> &NodeControl {
        &self.control
    }

    /// Returns exact committed external invocation references.
    #[must_use]
    pub const fn bindings(&self) -> &NodeInvocationBindings {
        &self.bindings
    }

    /// Returns normalized usage for this physical node attempt.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    /// Consumes the execution into persistence-ready parts.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        NodeStateChange,
        NodeControl,
        NodeInvocationBindings,
        BudgetUsage,
    ) {
        (self.state_change, self.control, self.bindings, self.usage)
    }
}

/// Public-safe failure of one graph-node attempt before journal causation.
///
/// Node code cannot know the completion event ID chosen by the durable driver,
/// so it must return an uncaused failure. The driver attaches the exact event
/// immediately before the atomic completion append. `ReconcileFirst` is also
/// rejected because node side effects must live in the separate invocation
/// ledgers, whose reconciliation is not a node-attempt retry decision.
#[derive(Clone, Debug)]
pub struct GraphNodeExecutionError {
    failure: Failure,
    usage: BudgetUsage,
}

impl GraphNodeExecutionError {
    /// Constructs a persistence-safe node failure.
    ///
    /// # Errors
    ///
    /// Returns [`GraphNodeExecutionErrorBuildError`] for pre-attached durable
    /// causation or reconcile-first retry advice.
    pub fn new(
        failure: Failure,
        usage: BudgetUsage,
    ) -> Result<Self, GraphNodeExecutionErrorBuildError> {
        if failure.caused_by_event_id().is_some() {
            return Err(GraphNodeExecutionErrorBuildError::CausationAlreadyAttached);
        }
        if failure.retry_advice() == RetryAdvice::ReconcileFirst {
            return Err(GraphNodeExecutionErrorBuildError::ReconciliationNotAllowed);
        }
        Ok(Self { failure, usage })
    }

    /// Returns public-safe failure evidence.
    #[must_use]
    pub const fn failure(&self) -> &Failure {
        &self.failure
    }

    /// Returns normalized usage observed before failure.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    /// Consumes the error into failure and usage components.
    #[must_use]
    pub fn into_parts(self) -> (Failure, BudgetUsage) {
        (self.failure, self.usage)
    }
}

/// Invalid node-execution failure returned before durable completion.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphNodeExecutionErrorBuildError {
    /// Only the driver may attach the exact completion event.
    #[error("node failure already contains durable event causation")]
    CausationAlreadyAttached,
    /// Invocation ledgers, not node attempts, reconcile uncertain side effects.
    #[error("node failure cannot request reconcile-first recovery")]
    ReconciliationNotAllowed,
}

/// Object-safe executable implementation of one exact graph node.
pub trait GraphNodeExecutor: Send + Sync + 'static {
    /// Returns the whole immutable graph definition this code was built for.
    fn graph(&self) -> &GraphReference;

    /// Returns the stable compiled node identity implemented by this code.
    fn node_id(&self) -> &NodeId;

    /// Executes exactly one already-durably-started physical attempt.
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>>;
}

/// One declarative graph with all executable bindings frozen and verified.
#[derive(Clone)]
pub struct ExecutableGraph {
    graph: Arc<CompiledGraph>,
    schemas: JsonSchemaRegistry,
    reducer: Arc<dyn GraphReducer>,
    nodes: Arc<HashMap<NodeId, Arc<dyn GraphNodeExecutor>>>,
}

impl ExecutableGraph {
    /// Returns the exact immutable declarative graph.
    #[must_use]
    pub fn graph(&self) -> &CompiledGraph {
        &self.graph
    }

    /// Returns the frozen offline schema registry.
    #[must_use]
    pub const fn schemas(&self) -> &JsonSchemaRegistry {
        &self.schemas
    }

    /// Returns the exactly pinned pure reducer.
    #[must_use]
    pub fn reducer(&self) -> &(dyn GraphReducer + 'static) {
        self.reducer.as_ref()
    }

    /// Resolves one exact compiled node implementation.
    #[must_use]
    pub fn node(&self, node_id: &NodeId) -> Option<&(dyn GraphNodeExecutor + 'static)> {
        self.nodes.get(node_id).map(Arc::as_ref)
    }

    /// Resolves a shared executor handle suitable for an owned async task.
    #[must_use]
    pub fn node_executor(&self, node_id: &NodeId) -> Option<Arc<dyn GraphNodeExecutor>> {
        self.nodes.get(node_id).cloned()
    }

    /// Returns the number of executable node bindings.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl fmt::Debug for ExecutableGraph {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutableGraph")
            .field("graph", &self.graph.reference())
            .field("reducer", self.reducer.reference())
            .field("nodes", &self.nodes.len())
            .finish_non_exhaustive()
    }
}

/// Startup-only builder for a closed executable deployment snapshot.
pub struct ExecutableGraphRegistryBuilder {
    schemas: JsonSchemaRegistry,
    graphs: HashMap<GraphReference, Arc<CompiledGraph>>,
    graph_identities: HashMap<stateknot_core::CapabilityIdentity, GraphReference>,
    reducers: HashMap<GraphReducerReference, Arc<dyn GraphReducer>>,
    reducer_identities: HashMap<stateknot_core::CapabilityIdentity, GraphReducerReference>,
    nodes: HashMap<(GraphReference, NodeId), Arc<dyn GraphNodeExecutor>>,
}

impl ExecutableGraphRegistryBuilder {
    /// Maximum graph definitions in one process snapshot.
    pub const MAX_GRAPHS: usize = 1024;
    /// Maximum reducer revisions in one process snapshot.
    pub const MAX_REDUCERS: usize = 1024;
    /// Maximum graph-node implementations in one process snapshot.
    pub const MAX_NODE_EXECUTORS: usize = 65_536;

    /// Creates an empty executable binding builder over a frozen schema set.
    #[must_use]
    pub fn new(schemas: JsonSchemaRegistry) -> Self {
        Self {
            schemas,
            graphs: HashMap::new(),
            graph_identities: HashMap::new(),
            reducers: HashMap::new(),
            reducer_identities: HashMap::new(),
            nodes: HashMap::new(),
        }
    }

    /// Registers one canonical declarative graph revision.
    ///
    /// # Errors
    ///
    /// Rejects ceiling exhaustion, an exact duplicate, or reuse of the same
    /// owner/name/version identity with different immutable bytes.
    pub fn register_graph(
        &mut self,
        graph: CompiledGraph,
    ) -> Result<(), ExecutableGraphRegistryError> {
        if self.graphs.len() == Self::MAX_GRAPHS {
            return Err(ExecutableGraphRegistryError::TooManyGraphs {
                maximum: Self::MAX_GRAPHS,
                actual: Self::MAX_GRAPHS + 1,
            });
        }
        let reference = graph.reference();
        if self.graphs.contains_key(&reference) {
            return Err(ExecutableGraphRegistryError::DuplicateGraph {
                reference: Box::new(reference),
            });
        }
        if let Some(existing) = self.graph_identities.get(graph.identity()) {
            return Err(ExecutableGraphRegistryError::GraphIdentityConflict {
                existing: Box::new(existing.clone()),
                rejected: Box::new(reference),
            });
        }
        self.graph_identities
            .insert(graph.identity().clone(), reference.clone());
        self.graphs.insert(reference, Arc::new(graph));
        Ok(())
    }

    /// Registers one exactly referenced pure reducer implementation.
    ///
    /// # Errors
    ///
    /// Rejects ceiling exhaustion, duplicate references, or identity reuse
    /// with another digest.
    pub fn register_reducer(
        &mut self,
        reducer: Arc<dyn GraphReducer>,
    ) -> Result<(), ExecutableGraphRegistryError> {
        if self.reducers.len() == Self::MAX_REDUCERS {
            return Err(ExecutableGraphRegistryError::TooManyReducers {
                maximum: Self::MAX_REDUCERS,
                actual: Self::MAX_REDUCERS + 1,
            });
        }
        let reference = reducer.reference().clone();
        if self.reducers.contains_key(&reference) {
            return Err(ExecutableGraphRegistryError::DuplicateReducer {
                reference: Box::new(reference),
            });
        }
        if let Some(existing) = self.reducer_identities.get(reference.identity()) {
            return Err(ExecutableGraphRegistryError::ReducerIdentityConflict {
                existing: Box::new(existing.clone()),
                rejected: Box::new(reference),
            });
        }
        self.reducer_identities
            .insert(reference.identity().clone(), reference.clone());
        self.reducers.insert(reference, reducer);
        Ok(())
    }

    /// Registers one node implementation bound to a whole graph digest.
    ///
    /// # Errors
    ///
    /// Rejects ceiling exhaustion or a repeated `(graph, node)` binding.
    pub fn register_node(
        &mut self,
        executor: Arc<dyn GraphNodeExecutor>,
    ) -> Result<(), ExecutableGraphRegistryError> {
        if self.nodes.len() == Self::MAX_NODE_EXECUTORS {
            return Err(ExecutableGraphRegistryError::TooManyNodeExecutors {
                maximum: Self::MAX_NODE_EXECUTORS,
                actual: Self::MAX_NODE_EXECUTORS + 1,
            });
        }
        let key = (executor.graph().clone(), executor.node_id().clone());
        if self.nodes.contains_key(&key) {
            return Err(ExecutableGraphRegistryError::DuplicateNodeExecutor {
                graph: Box::new(key.0),
                node_id: key.1,
            });
        }
        self.nodes.insert(key, executor);
        Ok(())
    }

    /// Verifies complete graph/schema/reducer/node closure and freezes lookup.
    ///
    /// # Errors
    ///
    /// Rejects an empty deployment; absent graph schemas, reducers, or nodes;
    /// node code not declared by its graph; and orphan executable code that no
    /// registered graph can ever resolve.
    pub fn build(self) -> Result<ExecutableGraphRegistry, ExecutableGraphRegistryError> {
        if self.graphs.is_empty() {
            return Err(ExecutableGraphRegistryError::EmptyGraphs);
        }

        let mut executable = HashMap::with_capacity(self.graphs.len());
        let mut used_reducers = std::collections::HashSet::new();
        let mut used_nodes = std::collections::HashSet::new();
        for (reference, graph) in &self.graphs {
            for schema in [
                graph.input_schema(),
                graph.state_schema(),
                graph.update_schema(),
                graph.output_schema(),
            ] {
                if !self.schemas.contains(schema) {
                    return Err(ExecutableGraphRegistryError::MissingSchema {
                        graph: Box::new(reference.clone()),
                        schema: Box::new(schema.clone()),
                    });
                }
            }
            let reducer = self.reducers.get(graph.reducer()).cloned().ok_or_else(|| {
                ExecutableGraphRegistryError::MissingReducer {
                    graph: Box::new(reference.clone()),
                    reducer: Box::new(graph.reducer().clone()),
                }
            })?;
            used_reducers.insert(graph.reducer().clone());

            let mut graph_nodes = HashMap::with_capacity(graph.nodes().len());
            for node in graph.nodes() {
                let key = (reference.clone(), node.node_id().clone());
                let executor = self.nodes.get(&key).cloned().ok_or_else(|| {
                    ExecutableGraphRegistryError::MissingNodeExecutor {
                        graph: Box::new(reference.clone()),
                        node_id: node.node_id().clone(),
                    }
                })?;
                used_nodes.insert(key);
                graph_nodes.insert(node.node_id().clone(), executor);
            }
            executable.insert(
                reference.clone(),
                ExecutableGraph {
                    graph: Arc::clone(graph),
                    schemas: self.schemas.clone(),
                    reducer,
                    nodes: Arc::new(graph_nodes),
                },
            );
        }

        if let Some(reference) = self
            .reducers
            .keys()
            .find(|reference| !used_reducers.contains(*reference))
        {
            return Err(ExecutableGraphRegistryError::OrphanReducer {
                reducer: Box::new(reference.clone()),
            });
        }
        if let Some((graph, node_id)) = self.nodes.keys().find(|key| !used_nodes.contains(*key)) {
            return Err(ExecutableGraphRegistryError::OrphanNodeExecutor {
                graph: Box::new(graph.clone()),
                node_id: node_id.clone(),
            });
        }

        Ok(ExecutableGraphRegistry {
            schemas: self.schemas,
            graphs: Arc::new(executable),
        })
    }
}

impl fmt::Debug for ExecutableGraphRegistryBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutableGraphRegistryBuilder")
            .field("schemas", &self.schemas)
            .field("graphs", &self.graphs.len())
            .field("reducers", &self.reducers.len())
            .field("nodes", &self.nodes.len())
            .finish_non_exhaustive()
    }
}

/// Immutable deployment snapshot resolved only by complete graph reference.
#[derive(Clone)]
pub struct ExecutableGraphRegistry {
    schemas: JsonSchemaRegistry,
    graphs: Arc<HashMap<GraphReference, ExecutableGraph>>,
}

impl ExecutableGraphRegistry {
    /// Resolves an exact graph descriptor and all local executable bindings.
    #[must_use]
    pub fn resolve(&self, reference: &GraphReference) -> Option<&ExecutableGraph> {
        self.graphs.get(reference)
    }

    /// Returns the shared frozen schema registry.
    #[must_use]
    pub const fn schemas(&self) -> &JsonSchemaRegistry {
        &self.schemas
    }

    /// Returns the number of executable graph revisions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.graphs.len()
    }

    /// Returns whether no executable graph was installed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.graphs.is_empty()
    }
}

impl fmt::Debug for ExecutableGraphRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutableGraphRegistry")
            .field("schemas", &self.schemas.len())
            .field("graphs", &self.graphs.len())
            .finish_non_exhaustive()
    }
}

/// Startup-time failure while freezing executable graph bindings.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ExecutableGraphRegistryError {
    /// No executable graph could be resolved.
    #[error("executable graph registry must contain at least one graph")]
    EmptyGraphs,
    /// Graph count exceeded the immutable deployment ceiling.
    #[error("executable registry contains {actual} graphs; maximum is {maximum}")]
    TooManyGraphs {
        /// Immutable maximum.
        maximum: usize,
        /// First rejected count.
        actual: usize,
    },
    /// Reducer count exceeded the immutable deployment ceiling.
    #[error("executable registry contains {actual} reducers; maximum is {maximum}")]
    TooManyReducers {
        /// Immutable maximum.
        maximum: usize,
        /// First rejected count.
        actual: usize,
    },
    /// Node implementation count exceeded the deployment ceiling.
    #[error("executable registry contains {actual} nodes; maximum is {maximum}")]
    TooManyNodeExecutors {
        /// Immutable maximum.
        maximum: usize,
        /// First rejected count.
        actual: usize,
    },
    /// The exact canonical graph was registered twice.
    #[error("compiled graph reference was registered more than once")]
    DuplicateGraph {
        /// Repeated graph reference.
        reference: Box<GraphReference>,
    },
    /// One graph identity attempted to carry different canonical bytes.
    #[error("graph owner/name/version identity was reused with another definition")]
    GraphIdentityConflict {
        /// First graph reference.
        existing: Box<GraphReference>,
        /// Rejected graph reference.
        rejected: Box<GraphReference>,
    },
    /// The exact reducer revision was registered twice.
    #[error("graph reducer reference was registered more than once")]
    DuplicateReducer {
        /// Repeated reducer reference.
        reference: Box<GraphReducerReference>,
    },
    /// One reducer identity attempted to carry different implementation bytes.
    #[error("reducer owner/name/version identity was reused with another definition")]
    ReducerIdentityConflict {
        /// First reducer reference.
        existing: Box<GraphReducerReference>,
        /// Rejected reducer reference.
        rejected: Box<GraphReducerReference>,
    },
    /// One whole-graph node binding was registered twice.
    #[error("graph node executor was registered more than once")]
    DuplicateNodeExecutor {
        /// Exact graph definition.
        graph: Box<GraphReference>,
        /// Repeated node identity.
        node_id: NodeId,
    },
    /// A graph referenced a schema absent from the frozen offline set.
    #[error("compiled graph references an unavailable schema")]
    MissingSchema {
        /// Graph with the incomplete dependency closure.
        graph: Box<GraphReference>,
        /// Missing schema reference.
        schema: Box<stateknot_core::SchemaReference>,
    },
    /// A graph's exactly pinned reducer was absent.
    #[error("compiled graph references an unavailable reducer")]
    MissingReducer {
        /// Graph with the incomplete dependency closure.
        graph: Box<GraphReference>,
        /// Missing reducer reference.
        reducer: Box<GraphReducerReference>,
    },
    /// One compiled node had no exact implementation.
    #[error("compiled graph node has no executable implementation")]
    MissingNodeExecutor {
        /// Graph with the incomplete dependency closure.
        graph: Box<GraphReference>,
        /// Missing node implementation.
        node_id: NodeId,
    },
    /// Installed reducer code was unreachable from every registered graph.
    #[error("registered reducer is not referenced by any installed graph")]
    OrphanReducer {
        /// Unused reducer implementation.
        reducer: Box<GraphReducerReference>,
    },
    /// Installed node code did not belong to a declared graph node.
    #[error("registered node executor is not declared by an installed graph")]
    OrphanNodeExecutor {
        /// Claimed graph definition.
        graph: Box<GraphReference>,
        /// Undeclared or unavailable node.
        node_id: NodeId,
    },
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use stateknot_core::{
        BoundedJson, CapabilityIdentity, CapabilityName, CapabilityReference, GraphExecutionLimits,
        GraphNode, GraphReducerError, GraphReducerInput, GraphRoutes, IssuerId, PrincipalIdentity,
        ReadyNodes, SchemaId, SchemaReference, SubjectId, Superstep, Version,
    };

    use super::*;
    use crate::JsonSchemaRegistryBuilder;

    const DRAFT: &str = "https://json-schema.org/draft/2020-12/schema";

    fn identity(name: &str) -> CapabilityIdentity {
        CapabilityIdentity::new(
            PrincipalIdentity::new(
                IssuerId::new("https://issuer.example.com").unwrap(),
                SubjectId::new("runtime-tests").unwrap(),
            ),
            CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
        )
    }

    fn schema_reference(id: &str, document: &Value) -> SchemaReference {
        SchemaReference::new(
            id.parse::<SchemaId>().unwrap(),
            Version::new(1, 0, 0),
            stateknot_core::Digest::sha256(serde_json_canonicalizer::to_vec(document).unwrap()),
        )
    }

    fn schemas() -> (JsonSchemaRegistry, SchemaReference) {
        let id = "https://schemas.example.com/graph-value/1.0.0";
        let document = json!({
            "$schema": DRAFT,
            "$id": id,
            "type": "object",
            "additionalProperties": true
        });
        let reference = schema_reference(id, &document);
        let mut builder = JsonSchemaRegistryBuilder::default();
        builder.register(reference.clone(), document).unwrap();
        (builder.build().unwrap(), reference)
    }

    fn graph(schema: &SchemaReference, reducer: GraphReducerReference) -> (CompiledGraph, NodeId) {
        let node_id = NodeId::new("finish").unwrap();
        let node = GraphNode::new(node_id.clone(), None, GraphRoutes::empty(), None, true).unwrap();
        let graph = CompiledGraph::compile(
            identity("graph"),
            schema.clone(),
            schema.clone(),
            schema.clone(),
            schema.clone(),
            reducer,
            ReadyNodes::try_new([node_id.clone()]).unwrap(),
            [node],
            GraphExecutionLimits::new(Superstep::new(32).unwrap(), 4).unwrap(),
        )
        .unwrap();
        (graph, node_id)
    }

    struct Reducer {
        reference: GraphReducerReference,
    }

    impl GraphReducer for Reducer {
        fn reference(&self) -> &GraphReducerReference {
            &self.reference
        }

        fn reduce(
            &self,
            state: &BoundedJson,
            _: &[GraphReducerInput<'_>],
        ) -> Result<BoundedJson, GraphReducerError> {
            Ok(state.clone())
        }
    }

    struct Executor {
        graph: GraphReference,
        node_id: NodeId,
    }

    impl GraphNodeExecutor for Executor {
        fn graph(&self) -> &GraphReference {
            &self.graph
        }

        fn node_id(&self) -> &NodeId {
            &self.node_id
        }

        fn execute(
            &self,
            _: GraphNodeContext,
        ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
            Box::pin(async { panic!("registry tests do not execute node code") })
        }
    }

    #[test]
    fn complete_registry_freezes_exact_graph_dependencies() {
        let (schemas, schema) = schemas();
        let reducer_reference =
            GraphReducerReference::new(identity("reducer"), stateknot_core::Digest::sha256(b"v1"));
        let reducer = Arc::new(Reducer {
            reference: reducer_reference.clone(),
        });
        let (graph, node_id) = graph(&schema, reducer_reference.clone());
        let graph_reference = graph.reference();
        let executor: Arc<dyn GraphNodeExecutor> = Arc::new(Executor {
            graph: graph_reference.clone(),
            node_id: node_id.clone(),
        });
        let mut builder = ExecutableGraphRegistryBuilder::new(schemas);
        builder.register_node(Arc::clone(&executor)).unwrap();
        assert!(matches!(
            builder.register_node(Arc::new(Executor {
                graph: graph_reference.clone(),
                node_id: node_id.clone(),
            })),
            Err(ExecutableGraphRegistryError::DuplicateNodeExecutor { .. })
        ));
        builder.register_reducer(reducer).unwrap();
        builder.register_graph(graph).unwrap();

        let registry = builder.build().unwrap();
        let executable = registry.resolve(&graph_reference).unwrap();
        assert_eq!(registry.len(), 1);
        assert_eq!(executable.graph().reference(), graph_reference);
        assert_eq!(executable.reducer().reference(), &reducer_reference);
        assert_eq!(executable.node(&node_id).unwrap().node_id(), &node_id);
        assert!(Arc::ptr_eq(
            &executable.node_executor(&node_id).unwrap(),
            &executor
        ));
    }

    #[test]
    fn build_rejects_missing_and_orphan_node_code() {
        let (schemas, schema) = schemas();
        let reducer_reference = GraphReducerReference::new(
            identity("reducer"),
            stateknot_core::Digest::sha256(b"reducer"),
        );
        let reducer = Arc::new(Reducer {
            reference: reducer_reference.clone(),
        });
        let (graph, node_id) = graph(&schema, reducer_reference);
        let graph_reference = graph.reference();
        let mut missing = ExecutableGraphRegistryBuilder::new(schemas.clone());
        missing.register_reducer(reducer.clone()).unwrap();
        missing.register_graph(graph.clone()).unwrap();
        assert!(matches!(
            missing.build(),
            Err(ExecutableGraphRegistryError::MissingNodeExecutor { .. })
        ));

        let mut orphan = ExecutableGraphRegistryBuilder::new(schemas);
        orphan.register_reducer(reducer).unwrap();
        orphan.register_graph(graph).unwrap();
        orphan
            .register_node(Arc::new(Executor {
                graph: graph_reference.clone(),
                node_id,
            }))
            .unwrap();
        orphan
            .register_node(Arc::new(Executor {
                graph: graph_reference,
                node_id: NodeId::new("undeclared").unwrap(),
            }))
            .unwrap();
        assert!(matches!(
            orphan.build(),
            Err(ExecutableGraphRegistryError::OrphanNodeExecutor { .. })
        ));
    }
}
