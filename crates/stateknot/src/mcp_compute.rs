// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Credential-free remote computation; durable authority stays in the Driver.

use std::{collections::BTreeSet, fmt, time::Duration};

use serde_json::{Map, Value};
use stateknot_core::{
    BoundedJson, BoxFuture, BudgetUsage, CompiledGraph, Digest, Failure, FailureCategory,
    FailureCode, FailureId, FailureMessage, FailureOrigin, GraphReference, NodeControl, NodeId,
    NodeInvocationBindings, NodeStateChange, NodeStateUpdate, NodeTerminalOutput, RetryAdvice,
    SchemaReference,
};
use stateknot_integrations::{McpClient, McpServerIdentity, McpTool, McpToolCall};
use stateknot_runtime::{
    GraphNodeContext, GraphNodeExecution, GraphNodeExecutionError, GraphNodeExecutor,
    GraphNodeScheduling, JsonSchemaRegistry,
};
use thiserror::Error;

/// Explicit top-level state fields that a compute Worker may receive.
///
/// There is no whole-checkpoint/default projection. A selected field includes
/// its entire bounded value: the operator must not select a secret-bearing
/// object. Unselected fields and all runtime metadata are never serialized.
#[derive(Clone)]
pub struct WorkerInputProjection {
    fields: BTreeSet<String>,
}

impl WorkerInputProjection {
    /// Maximum number of selected fields, including for an unbounded iterator.
    pub const MAX_FIELDS: usize = 64;

    /// Constructs a finite, duplicate-free allowlist. Empty input is permitted
    /// for a constant computation and produces `{}`, never the complete state.
    pub fn new<I, S>(fields: I) -> Result<Self, WorkerInputProjectionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut selected = BTreeSet::new();
        for field in fields {
            if selected.len() == Self::MAX_FIELDS {
                return Err(WorkerInputProjectionError);
            }
            let field = field.into();
            if field.is_empty()
                || field.len() > 256
                || field.chars().any(char::is_control)
                || !selected.insert(field)
            {
                return Err(WorkerInputProjectionError);
            }
        }
        Ok(Self { fields: selected })
    }

    /// Returns the canonical field order, without state values.
    pub fn fields(&self) -> impl ExactSizeIterator<Item = &str> {
        self.fields.iter().map(String::as_str)
    }

    fn project(&self, state: &BoundedJson) -> Result<BoundedJson, WorkerInputProjectionError> {
        let mut input = Map::new();
        for field in &self.fields {
            let value = state
                .as_value()
                .as_object()
                .and_then(|object| object.get(field))
                .ok_or(WorkerInputProjectionError)?;
            input.insert(field.clone(), value.clone());
        }
        BoundedJson::try_from_value(Value::Object(input)).map_err(|_| WorkerInputProjectionError)
    }
}

impl fmt::Debug for WorkerInputProjection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerInputProjection")
            .field("field_count", &self.fields.len())
            .finish_non_exhaustive()
    }
}

/// Invalid, duplicate or excessive projection field selection.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invalid compute Worker input projection")]
pub struct WorkerInputProjectionError;

/// Host-selected interpretation of a Worker's structured JSON result.
///
/// The Worker cannot select routes, waits, children, invocation evidence,
/// usage, schemas or lifecycle transitions through its response envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpComputeOutput {
    /// Validate against the Graph update schema and continue its fixed edges.
    UpdateAndContinue,
    /// Validate against the Graph output schema and request its terminal barrier.
    Terminal,
}

/// Immutable, operator-reviewed binding for a repeatable, effect-free Worker.
///
/// This is a trusted assertion, not inferred from remote `readOnlyHint` or
/// `idempotentHint`. Never use it for models, billable calls, mutable reads,
/// writes or child admission; those require the durable invocation ledgers.
/// Version the Graph when changing projection, Worker code, schema or policy.
#[derive(Clone, Debug)]
pub struct McpComputeNodeBinding {
    graph: GraphReference,
    node_id: NodeId,
    input_schema: SchemaReference,
    output_schema: SchemaReference,
    projection: WorkerInputProjection,
    output: McpComputeOutput,
    server: McpServerIdentity,
    tool_name: String,
    tool_digest: Digest,
}

impl McpComputeNodeBinding {
    /// Binds a reviewed descriptor digest, exact server revision and explicit
    /// input projection to a real compiled Graph node. No discovery-derived
    /// pin or automatic approval is supplied by this constructor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        graph: &CompiledGraph,
        node_id: NodeId,
        input_schema: SchemaReference,
        projection: WorkerInputProjection,
        output: McpComputeOutput,
        server: McpServerIdentity,
        tool_name: impl Into<String>,
        tool_digest: Digest,
    ) -> Result<Self, McpComputeNodeBuildError> {
        let node = graph
            .node(&node_id)
            .ok_or(McpComputeNodeBuildError::Graph)?;
        let output_schema = match output {
            McpComputeOutput::UpdateAndContinue if node.continue_to().is_some() => {
                graph.update_schema().clone()
            }
            McpComputeOutput::Terminal if node.allows_terminal() => graph.output_schema().clone(),
            _ => return Err(McpComputeNodeBuildError::Graph),
        };
        let tool_name = tool_name.into();
        if tool_name.is_empty()
            || tool_name.len() > 128
            || !tool_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
        {
            return Err(McpComputeNodeBuildError::Descriptor);
        }
        Ok(Self {
            graph: graph.reference(),
            node_id,
            input_schema,
            output_schema,
            projection,
            output,
            server,
            tool_name,
            tool_digest,
        })
    }
}

/// Computes the SHA-256 pin of a reviewed MCP Tool's complete RFC 8785
/// descriptor. Obtain the descriptor through a trusted release process, not
/// trust-on-first-use against a running Worker.
pub fn mcp_compute_tool_digest(descriptor: &Value) -> Result<Digest, McpComputeNodeBuildError> {
    let bounded = BoundedJson::try_from_value(descriptor.clone())
        .map_err(|_| McpComputeNodeBuildError::Descriptor)?;
    serde_json_canonicalizer::to_vec(bounded.as_value())
        .map(Digest::sha256)
        .map_err(|_| McpComputeNodeBuildError::Descriptor)
}

/// MCP-backed pure Graph computation without Worker database credentials.
///
/// This adapter runs in the trusted service; only its projected arguments go
/// to the remote MCP server. The Driver creates IDs, owns fencing and commits
/// the result. The endpoint and scoped MCP credential belong to the immutable
/// client. Production requires TLS/authentication and separate OS/network
/// credentials; this API is not an arbitrary-code sandbox.
#[derive(Clone)]
pub struct McpComputeNode {
    client: McpClient,
    tool: McpTool,
    binding: McpComputeNodeBinding,
    schemas: JsonSchemaRegistry,
    timeout: Duration,
}

impl McpComputeNode {
    /// Verifies the frozen discovery identity, complete Tool descriptor and
    /// local schema bytes before a node can enter the executable registry.
    /// `timeout` bounds both startup catalog validation and each execution,
    /// including authorization, queuing and the response body (1 ms–5 min).
    pub async fn connect(
        client: McpClient,
        binding: McpComputeNodeBinding,
        schemas: JsonSchemaRegistry,
        timeout: Duration,
    ) -> Result<Self, McpComputeNodeBuildError> {
        if timeout < Duration::from_millis(1) || timeout > Duration::from_secs(300) {
            return Err(McpComputeNodeBuildError::Timeout);
        }
        if client.server().name() != Some(binding.server.name())
            || client.server().version() != Some(binding.server.version())
        {
            return Err(McpComputeNodeBuildError::Identity);
        }
        if !schemas.contains(&binding.input_schema) || !schemas.contains(&binding.output_schema) {
            return Err(McpComputeNodeBuildError::Schema);
        }
        let catalog = tokio::time::timeout(timeout, client.list_tools())
            .await
            .map_err(|_| McpComputeNodeBuildError::Discovery)?
            .map_err(|_| McpComputeNodeBuildError::Discovery)?;
        // A rejected entry can hide a duplicate name. No partially accepted
        // catalog may establish a compute binding.
        if !catalog.rejected_tools().is_empty() {
            return Err(McpComputeNodeBuildError::Descriptor);
        }
        let tool = catalog
            .tools()
            .iter()
            .find(|tool| tool.name() == binding.tool_name)
            .ok_or(McpComputeNodeBuildError::Descriptor)?
            .clone();
        if mcp_compute_tool_digest(tool.raw())? != binding.tool_digest
            || contains_header_annotation(tool.input_schema())
            || tool.raw().get("execution").is_some_and(|execution| {
                execution
                    .get("taskSupport")
                    .is_some_and(|support| support != "forbidden")
            })
        {
            return Err(McpComputeNodeBuildError::Descriptor);
        }
        for (remote, local) in [
            (Some(tool.input_schema()), &binding.input_schema),
            (tool.output_schema(), &binding.output_schema),
        ] {
            let remote = remote.ok_or(McpComputeNodeBuildError::Schema)?;
            let canonical = serde_json_canonicalizer::to_vec(remote)
                .map_err(|_| McpComputeNodeBuildError::Schema)?;
            if schemas.canonical_bytes(local) != Some(canonical.as_slice()) {
                return Err(McpComputeNodeBuildError::Schema);
            }
        }
        Ok(Self {
            client,
            tool,
            binding,
            schemas,
            timeout,
        })
    }

    async fn compute(
        &self,
        context: &GraphNodeContext,
    ) -> Result<GraphNodeExecution, GraphNodeExecutionError> {
        if context.checkpoint().graph() != &self.binding.graph
            || context.attempt().activation().node_id() != &self.binding.node_id
            || context.child_join().is_some()
        {
            return Err(invalid_input());
        }
        let input = self
            .binding
            .projection
            .project(context.checkpoint().state().data())
            .map_err(|_| invalid_input())?;
        self.schemas
            .validate_bounded(&self.binding.input_schema, &input)
            .map_err(|_| invalid_input())?;
        let response = self
            .client
            .call_tool_once(&self.tool, input.as_value().clone())
            .await
            .map_err(|_| {
                worker_failure("worker.unavailable", FailureCategory::DependencyUnavailable)
            })?;
        // Notifications and interactive inputs are outside the compute
        // contract, and may never initiate callbacks or credential access.
        if !response.notifications().is_empty() {
            return Err(invalid_output());
        }
        let McpToolCall::Complete(result) = response.into_outcome() else {
            return Err(invalid_output());
        };
        if result.is_error()
            || !result.content().is_empty()
            || result.raw().as_object().is_none_or(|object| {
                object.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "content" | "structuredContent" | "isError" | "_meta" | "resultType"
                    )
                })
            })
        {
            return Err(invalid_output());
        }
        let output = BoundedJson::try_from_value(
            result
                .structured_content()
                .ok_or_else(invalid_output)?
                .clone(),
        )
        .map_err(|_| invalid_output())?;
        self.schemas
            .validate_bounded(&self.binding.output_schema, &output)
            .map_err(|_| invalid_output())?;
        let (state_change, control) = match self.binding.output {
            McpComputeOutput::UpdateAndContinue => (
                NodeStateChange::Update {
                    update: NodeStateUpdate::new(self.binding.output_schema.clone(), output)
                        .map_err(|_| invalid_output())?,
                },
                NodeControl::Continue,
            ),
            McpComputeOutput::Terminal => (
                NodeStateChange::Unchanged,
                NodeControl::Terminal {
                    output: NodeTerminalOutput::new(self.binding.output_schema.clone(), output)
                        .map_err(|_| invalid_output())?,
                },
            ),
        };
        Ok(GraphNodeExecution::new(
            state_change,
            control,
            NodeInvocationBindings::empty(),
            BudgetUsage::zero(),
        ))
    }
}

impl GraphNodeExecutor for McpComputeNode {
    fn graph(&self) -> &GraphReference {
        &self.binding.graph
    }
    fn node_id(&self) -> &NodeId {
        &self.binding.node_id
    }
    fn scheduling(&self) -> GraphNodeScheduling {
        GraphNodeScheduling::JournalIsolated
    }
    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            tokio::select! {
                biased;
                () = context.cancellation().cancelled() => Err(worker_failure("worker.cancelled", FailureCategory::Cancelled)),
                result = tokio::time::timeout(self.timeout, self.compute(&context)) => {
                    result.map_err(|_| worker_failure("worker.timeout", FailureCategory::DeadlineExceeded))?
                }
            }
        })
    }
}

impl fmt::Debug for McpComputeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpComputeNode")
            .field("binding", &self.binding)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

/// Public-safe startup failure; never includes a remote payload or credential.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpComputeNodeBuildError {
    /// The compiled node does not support the host-selected output mode.
    #[error("compute Worker Graph binding is invalid")]
    Graph,
    /// The execution/startup deadline is outside its hard bounds.
    #[error("compute Worker deadline must be between 1 ms and 5 min")]
    Timeout,
    /// The server's exact implementation revision does not match deployment.
    #[error("compute Worker server identity does not match deployment")]
    Identity,
    /// The Tool is missing, changed or outside this restricted profile.
    #[error("compute Worker descriptor does not match deployment")]
    Descriptor,
    /// Local and remote schema resources do not exactly agree.
    #[error("compute Worker schemas do not match deployment")]
    Schema,
    /// Bounded startup discovery did not complete successfully.
    #[error("compute Worker discovery is unavailable")]
    Discovery,
}

fn contains_header_annotation(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key("x-mcp-header") || object.values().any(contains_header_annotation)
        }
        Value::Array(array) => array.iter().any(contains_header_annotation),
        _ => false,
    }
}

fn invalid_input() -> GraphNodeExecutionError {
    worker_failure("worker.invalid_input", FailureCategory::InvalidInput)
}
fn invalid_output() -> GraphNodeExecutionError {
    worker_failure("worker.invalid_output", FailureCategory::DataCorruption)
}

fn worker_failure(code: &str, category: FailureCategory) -> GraphNodeExecutionError {
    let failure = Failure::new(
        FailureId::generate(),
        category,
        FailureCode::new(code).expect("static Worker code"),
        FailureOrigin::new("stateknot.mcp_compute").expect("static Worker origin"),
        FailureMessage::new("Remote computation did not produce an accepted result")
            .expect("static Worker message"),
        RetryAdvice::Never,
    )
    .expect("non-reconciling Worker failure");
    GraphNodeExecutionError::new(failure, BudgetUsage::zero())
        .expect("uncaused non-reconciling Worker failure")
}
