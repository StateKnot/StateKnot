// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real `PostgreSQL` durable graph-driver tests.

use std::{
    collections::VecDeque,
    future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures_core::Stream;
use serde_json::{Value, json};
use stateknot_core::{
    AgentAdmissionAuthority, AgentAdmissionBudgetLayer, AgentArtifacts, AgentDescriptor,
    AgentRequest, AgentResultProvenance, AgentSubmissionKey, AttemptId, BoundedJson, BoxFuture,
    BoxStream, BudgetLimits, BudgetRemaining, BudgetUsage, ByteCount, CancellationSignal,
    CapabilityIdentity, CapabilityName, CapabilityReference, Checkpoint, CheckpointId,
    CheckpointState, CheckpointWrite, CompiledGraph, Digest, ErasedTool, EventId, ExecutionCount,
    Failure, FailureCategory, FailureCode, FailureId, FailureMessage, FailureOrigin,
    GraphBarrierDisposition, GraphExecutionLimits, GraphNode, GraphReducer, GraphReducerError,
    GraphReducerInput, GraphReducerReference, GraphReference, GraphRoutes, InvocationId, IssuerId,
    JournalAppend, JournalEventIntent, JournalEventKind, JournalExpectation, JournalPayload,
    KnownCosts, Model, ModelContext, ModelDescriptor, ModelError, ModelEvent,
    ModelInvocationIntent, ModelInvocationStatus, ModelRequest, ModelResponse, ModelResponseMode,
    NodeActivation, NodeControl, NodeId, NodeInvocationBindings, NodeStateChange,
    NodeTerminalOutput, NodeWait, NodeWaits, PrincipalIdentity, QuarantineId, ReadyNodes,
    ResolvedBudget, RetryAdvice, RunCancellationRequest, RunFence, RunId, RunStatus, RunTimerKind,
    RunTransition, SchedulerShardId, SchemaId, SchemaReference, SubjectId, Superstep, TenantId,
    ThreadId, TimerId, Timestamp, ToolContext, ToolDescriptor, ToolError, ToolExternalEffect,
    ToolInput, ToolInvocationIntent, ToolInvocationState, ToolInvocationStatus, ToolResult,
    Version,
};
use stateknot_runtime::{
    AgentLoopError, AgentLoopOutcome, AgentRunAdmissionOutcome, AgentRunIds,
    AgentRunTerminalOutcome, DurableAgentAdmission, DurableAgentAdmissionError,
    DurableAgentAdmissionRequest, DurableAgentLoop, DurableAgentRuns, DurableAgentRunsError,
    DurableFairScheduler, DurableFairSchedulerOptions, DurableGraphDriver,
    DurableGraphDriverOptions, DurableGraphLifecycle, DurableGraphLifecycleOptions,
    DurableInvocationExecutor, DurableInvocationExecutorOptions, DurableTenantScheduler,
    DurableTenantSchedulerOptions, ExecutableGraphRegistry, ExecutableGraphRegistryBuilder,
    GraphBarrierLifecycleOutcome, GraphDriveOutcome, GraphDriverError, GraphFailureEvidence,
    GraphFailureEvidenceContext, GraphLifecycleEvidenceError, GraphLifecycleEvidenceProvider,
    GraphNodeContext, GraphNodeExecution, GraphNodeExecutionError, GraphNodeExecutor,
    GraphTerminalEvidence, GraphTerminalEvidenceContext, InvocationAttemptEventIds,
    InvocationBudgetContext, InvocationBudgetProvider, InvocationBudgetProviderError,
    InvocationClock, InvocationClockError, InvocationClockObservation, JsonSchemaRegistry,
    JsonSchemaRegistryBuilder, JsonSchemaRegistryLimits, ModelAttemptExecutionError,
    ModelAttemptHandoff, ModelAttemptOutcome, ModelAttemptTerminalKind, ModelEventSink,
    ModelEventSinkError, ModelProviderRegistryBuilder, TenantFairnessWeight,
    TenantSchedulerOutcome, ToolAttemptHandoff, ToolAttemptOutcome, ToolAttemptTerminalKind,
    ToolProviderRegistryBuilder, WeightedFairnessPolicy,
    register_standard_agent_admission_event_schema, register_standard_graph_driver_event_schema,
    register_standard_graph_lifecycle_event_schema,
    register_standard_invocation_execution_event_schema,
};
use stateknot_store_postgres::{
    AgentAdmissionCommitOutcome, BarrierCommitOutcome, CheckpointCommitOutcome,
    CorruptionQuarantineContext, GraphDefinitionRegistrationOutcome, GraphReplayLimits,
    LeaseReleaseOutcome, NodeAttemptCommitOutcome, PostgresStore, PostgresStoreOptions,
    PostgresTransportSecurity, RunProjection, RunnableRunPageSize, StoreError,
    WaitCheckpointCommitOutcome,
};

const DATABASE_URL_ENV: &str = "STATEKNOT_TEST_DATABASE_URL";
const REQUIRE_DATABASE_ENV: &str = "STATEKNOT_REQUIRE_POSTGRES_TESTS";
static DATABASE_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone)]
enum TestNodeBehavior {
    Continue,
    Fail,
    Wait(NodeWaits),
    Terminal(SchemaReference),
}

struct TestNodeExecutor {
    graph: GraphReference,
    node_id: NodeId,
    behavior: TestNodeBehavior,
    delay: Duration,
    calls: Arc<AtomicUsize>,
}

impl GraphNodeExecutor for TestNodeExecutor {
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
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if matches!(self.behavior, TestNodeBehavior::Fail) {
                return Err(GraphNodeExecutionError::new(
                    test_failure("graph.node_failed", "Graph node failed safely."),
                    BudgetUsage::zero(),
                )
                .unwrap());
            }
            let control = match &self.behavior {
                TestNodeBehavior::Continue => NodeControl::Continue,
                TestNodeBehavior::Fail => unreachable!("failure returned before control"),
                TestNodeBehavior::Wait(waits) => NodeControl::Wait {
                    waits: waits.clone(),
                },
                TestNodeBehavior::Terminal(schema) => NodeControl::Terminal {
                    output: NodeTerminalOutput::new(
                        schema.clone(),
                        BoundedJson::try_from_value(json!({"ok": true})).unwrap(),
                    )
                    .unwrap(),
                },
            };
            Ok(GraphNodeExecution::new(
                NodeStateChange::Unchanged,
                control,
                NodeInvocationBindings::empty(),
                BudgetUsage::zero(),
            ))
        })
    }
}

struct TestReducer {
    reference: GraphReducerReference,
}

impl GraphReducer for TestReducer {
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

struct EmptyModelStream;

impl Stream for EmptyModelStream {
    type Item = Result<ModelEvent, ModelError>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

struct FiniteModelStream {
    events: VecDeque<Result<ModelEvent, ModelError>>,
}

impl Stream for FiniteModelStream {
    type Item = Result<ModelEvent, ModelError>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.events.pop_front())
    }
}

struct StreamingModel {
    descriptor: ModelDescriptor,
    calls: Arc<AtomicUsize>,
}

impl Model for StreamingModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxFuture<'_, Result<ModelResponse, ModelError>> {
        let response = model_response_for(&self.descriptor, &request, context.attempt_id());
        Box::pin(async move { Ok(response) })
    }

    fn stream(
        &self,
        context: ModelContext,
        _: ModelRequest,
    ) -> BoxStream<'_, Result<ModelEvent, ModelError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(FiniteModelStream {
            events: model_events_for(&self.descriptor, context.attempt_id())
                .into_iter()
                .map(Ok)
                .collect(),
        })
    }
}

#[derive(Default)]
struct RecordingModelEventSink {
    events: tokio::sync::Mutex<Vec<ModelEvent>>,
}

impl ModelEventSink for RecordingModelEventSink {
    fn emit(&self, event: ModelEvent) -> BoxFuture<'_, Result<(), ModelEventSinkError>> {
        Box::pin(async move {
            self.events.lock().await.push(event);
            Ok(())
        })
    }
}

struct LeaseRotatingModel {
    descriptor: ModelDescriptor,
    store: PostgresStore,
    calls: Arc<AtomicUsize>,
    replacement_fence: Arc<tokio::sync::Mutex<Option<RunFence>>>,
}

impl Model for LeaseRotatingModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxFuture<'_, Result<ModelResponse, ModelError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let response = model_response_for(&self.descriptor, &request, context.attempt_id());
        let store = self.store.clone();
        let tenant_id = context.tenant_id().clone();
        let run_id = context.run_id();
        let replacement_fence = Arc::clone(&self.replacement_fence);
        Box::pin(async move {
            let replacement = store
                .supersede_lease(&tenant_id, run_id, AttemptId::generate())
                .await
                .expect("test provider must rotate the live lease")
                .lease()
                .fence()
                .clone();
            *replacement_fence.lock().await = Some(replacement);
            Ok(response)
        })
    }

    fn stream(
        &self,
        _: ModelContext,
        _: ModelRequest,
    ) -> BoxStream<'_, Result<ModelEvent, ModelError>> {
        Box::pin(EmptyModelStream)
    }
}

struct PendingWriteTool {
    descriptor: ToolDescriptor,
    calls: Arc<AtomicUsize>,
}

impl ErasedTool for PendingWriteTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn call(&self, _: ToolContext, _: ToolInput) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(future::pending())
    }
}

struct StaticInvocationBudget {
    resolved: ResolvedBudget,
}

impl InvocationBudgetProvider for StaticInvocationBudget {
    fn remaining(
        &self,
        context: InvocationBudgetContext,
    ) -> BoxFuture<'_, Result<BudgetRemaining, InvocationBudgetProviderError>> {
        let remaining = self
            .resolved
            .remaining(&BudgetUsage::zero(), context.observed_at())
            .map_err(InvocationBudgetProviderError::new);
        Box::pin(async move { remaining })
    }
}

struct OneShotInvocationBudget {
    resolved: ResolvedBudget,
    calls: Arc<AtomicUsize>,
}

impl InvocationBudgetProvider for OneShotInvocationBudget {
    fn remaining(
        &self,
        context: InvocationBudgetContext,
    ) -> BoxFuture<'_, Result<BudgetRemaining, InvocationBudgetProviderError>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let remaining = if call == 0 {
            self.resolved
                .remaining(&BudgetUsage::zero(), context.observed_at())
                .map_err(InvocationBudgetProviderError::new)
        } else {
            Err(InvocationBudgetProviderError::new(std::io::Error::other(
                "budget must not be reevaluated during recovery",
            )))
        };
        Box::pin(async move { remaining })
    }
}

#[derive(Clone, Copy)]
struct FixedInvocationClock {
    observed_at: Timestamp,
}

impl InvocationClock for FixedInvocationClock {
    fn observe(&self) -> Result<InvocationClockObservation, InvocationClockError> {
        Ok(InvocationClockObservation::new(
            self.observed_at,
            Instant::now(),
        ))
    }
}

struct DriverFixture {
    graph: CompiledGraph,
    registry: ExecutableGraphRegistry,
    first_calls: Arc<AtomicUsize>,
    second_calls: Arc<AtomicUsize>,
}

struct StaticLifecycleEvidence {
    terminal: GraphTerminalEvidence,
    failure: Option<GraphFailureEvidence>,
}

impl GraphLifecycleEvidenceProvider for StaticLifecycleEvidence {
    fn terminal_evidence(
        &self,
        _: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        let evidence = self.terminal.clone();
        Box::pin(async move { Ok(evidence) })
    }

    fn failure_evidence(
        &self,
        _: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        let evidence = self.failure.clone();
        Box::pin(async move { evidence.ok_or(GraphLifecycleEvidenceError::Unavailable) })
    }
}

struct UnavailableLifecycleEvidence;

impl GraphLifecycleEvidenceProvider for UnavailableLifecycleEvidence {
    fn terminal_evidence(
        &self,
        _: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Err(GraphLifecycleEvidenceError::TemporarilyUnavailable) })
    }

    fn failure_evidence(
        &self,
        _: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(async { Err(GraphLifecycleEvidenceError::TemporarilyUnavailable) })
    }
}

fn driver_fixture() -> DriverFixture {
    driver_fixture_with_first_delay(Duration::ZERO)
}

fn driver_fixture_with_first_delay(first_delay: Duration) -> DriverFixture {
    let (input_schema, input_document) = schema("driver-input");
    let (state_schema, state_document) = state_schema();
    let (update_schema, update_document) = schema("driver-update");
    let (output_schema, output_document) = schema("driver-output");
    let reducer_reference = GraphReducerReference::new(
        capability("driver-reducer"),
        Digest::sha256(b"stateknot runtime integration reducer v1"),
    );
    let first_id = NodeId::new("Step_A").unwrap();
    let second_id = NodeId::new("Step.B").unwrap();
    let graph = CompiledGraph::compile(
        capability("driver-graph"),
        input_schema.clone(),
        state_schema,
        update_schema,
        output_schema.clone(),
        reducer_reference.clone(),
        ReadyNodes::try_new([first_id.clone()]).unwrap(),
        [
            GraphNode::new(
                first_id.clone(),
                Some(ReadyNodes::try_new([second_id.clone()]).unwrap()),
                GraphRoutes::empty(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(second_id.clone(), None, GraphRoutes::empty(), None, true).unwrap(),
        ],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap();

    let mut schemas = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    for (reference, document) in [
        (input_schema, input_document),
        (graph.state_schema().clone(), state_document),
        (graph.update_schema().clone(), update_document),
        (output_schema.clone(), output_document),
    ] {
        schemas.register(reference, document).unwrap();
    }
    register_standard_graph_driver_event_schema(&mut schemas).unwrap();
    register_standard_graph_lifecycle_event_schema(&mut schemas).unwrap();
    register_standard_agent_admission_event_schema(&mut schemas).unwrap();
    let schemas = schemas.build().unwrap();

    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let graph_reference = graph.reference();
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas);
    registry.register_graph(graph.clone()).unwrap();
    registry
        .register_reducer(Arc::new(TestReducer {
            reference: reducer_reference,
        }))
        .unwrap();
    registry
        .register_node(Arc::new(TestNodeExecutor {
            graph: graph_reference.clone(),
            node_id: first_id,
            behavior: TestNodeBehavior::Continue,
            delay: first_delay,
            calls: Arc::clone(&first_calls),
        }))
        .unwrap();
    registry
        .register_node(Arc::new(TestNodeExecutor {
            graph: graph_reference,
            node_id: second_id,
            behavior: TestNodeBehavior::Terminal(output_schema),
            delay: Duration::ZERO,
            calls: Arc::clone(&second_calls),
        }))
        .unwrap();

    DriverFixture {
        graph,
        registry: registry.build().unwrap(),
        first_calls,
        second_calls,
    }
}

fn wait_fixture() -> (DriverFixture, TimerId, Timestamp) {
    let (input_schema, input_document) = schema("wait-input");
    let (state_schema, state_document) = state_schema();
    let (update_schema, update_document) = schema("wait-update");
    let (output_schema, output_document) = schema("wait-output");
    let reducer_reference = GraphReducerReference::new(
        capability("wait-reducer"),
        Digest::sha256(b"stateknot runtime wait integration reducer v1"),
    );
    let pause_id = NodeId::new("Pause").unwrap();
    let resume_id = NodeId::new("Resume").unwrap();
    let graph = CompiledGraph::compile(
        capability("wait-graph"),
        input_schema.clone(),
        state_schema,
        update_schema,
        output_schema.clone(),
        reducer_reference.clone(),
        ReadyNodes::try_new([pause_id.clone()]).unwrap(),
        [
            GraphNode::new(
                pause_id.clone(),
                None,
                GraphRoutes::empty(),
                Some(ReadyNodes::try_new([resume_id.clone()]).unwrap()),
                false,
            )
            .unwrap(),
            GraphNode::new(resume_id.clone(), None, GraphRoutes::empty(), None, true).unwrap(),
        ],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap();

    let mut schemas = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    for (reference, document) in [
        (input_schema, input_document),
        (graph.state_schema().clone(), state_document),
        (graph.update_schema().clone(), update_document),
        (output_schema.clone(), output_document),
    ] {
        schemas.register(reference, document).unwrap();
    }
    register_standard_graph_driver_event_schema(&mut schemas).unwrap();
    register_standard_graph_lifecycle_event_schema(&mut schemas).unwrap();
    let schemas = schemas.build().unwrap();

    let timer_id = TimerId::generate();
    let due_at = "2099-01-01T00:00:00.000000Z".parse::<Timestamp>().unwrap();
    let waits =
        NodeWaits::try_new([NodeWait::timer(timer_id, RunTimerKind::Sleep, due_at)]).unwrap();
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let graph_reference = graph.reference();
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas);
    registry.register_graph(graph.clone()).unwrap();
    registry
        .register_reducer(Arc::new(TestReducer {
            reference: reducer_reference,
        }))
        .unwrap();
    registry
        .register_node(Arc::new(TestNodeExecutor {
            graph: graph_reference.clone(),
            node_id: pause_id,
            behavior: TestNodeBehavior::Wait(waits),
            delay: Duration::ZERO,
            calls: Arc::clone(&first_calls),
        }))
        .unwrap();
    registry
        .register_node(Arc::new(TestNodeExecutor {
            graph: graph_reference,
            node_id: resume_id,
            behavior: TestNodeBehavior::Terminal(output_schema),
            delay: Duration::ZERO,
            calls: Arc::clone(&second_calls),
        }))
        .unwrap();

    (
        DriverFixture {
            graph,
            registry: registry.build().unwrap(),
            first_calls,
            second_calls,
        },
        timer_id,
        due_at,
    )
}

fn failure_fixture() -> DriverFixture {
    let (input_schema, input_document) = schema("failure-input");
    let (state_schema, state_document) = state_schema();
    let (update_schema, update_document) = schema("failure-update");
    let (output_schema, output_document) = schema("failure-output");
    let reducer_reference = GraphReducerReference::new(
        capability("failure-reducer"),
        Digest::sha256(b"stateknot runtime failure integration reducer v1"),
    );
    let node_id = NodeId::new("Fail").unwrap();
    let graph = CompiledGraph::compile(
        capability("failure-graph"),
        input_schema.clone(),
        state_schema,
        update_schema,
        output_schema.clone(),
        reducer_reference.clone(),
        ReadyNodes::try_new([node_id.clone()]).unwrap(),
        [GraphNode::new(node_id.clone(), None, GraphRoutes::empty(), None, true).unwrap()],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap();

    let mut schemas = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    for (reference, document) in [
        (input_schema, input_document),
        (graph.state_schema().clone(), state_document),
        (graph.update_schema().clone(), update_document),
        (output_schema, output_document),
    ] {
        schemas.register(reference, document).unwrap();
    }
    register_standard_graph_driver_event_schema(&mut schemas).unwrap();
    register_standard_graph_lifecycle_event_schema(&mut schemas).unwrap();
    register_standard_agent_admission_event_schema(&mut schemas).unwrap();
    let schemas = schemas.build().unwrap();

    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let graph_reference = graph.reference();
    let mut registry = ExecutableGraphRegistryBuilder::new(schemas);
    registry.register_graph(graph.clone()).unwrap();
    registry
        .register_reducer(Arc::new(TestReducer {
            reference: reducer_reference,
        }))
        .unwrap();
    registry
        .register_node(Arc::new(TestNodeExecutor {
            graph: graph_reference,
            node_id,
            behavior: TestNodeBehavior::Fail,
            delay: Duration::ZERO,
            calls: Arc::clone(&first_calls),
        }))
        .unwrap();
    DriverFixture {
        graph,
        registry: registry.build().unwrap(),
        first_calls,
        second_calls,
    }
}

fn test_failure(code: &str, message: &str) -> Failure {
    Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new(code).unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new(message).unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
}

fn terminal_evidence(graph: &CompiledGraph) -> GraphTerminalEvidence {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-v1.json"
    ))
    .unwrap();
    let template =
        serde_json::from_value::<AgentDescriptor>(fixture["descriptors"]["valid"][0].clone())
            .unwrap();
    let descriptor = AgentDescriptor::new(
        template.metadata().clone(),
        graph.input_schema().clone(),
        graph.output_schema().clone(),
        template.model().clone(),
        template.instructions().clone(),
        template.tools().clone(),
        template.execution().clone(),
        template.budget_limits().clone(),
    )
    .unwrap();
    let request = AgentRequest::new(
        graph.input_schema().clone(),
        BoundedJson::try_from_value(json!({"request": true})).unwrap(),
        BudgetLimits::empty(),
    );
    let budget_fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-budget-v1.json"
    ))
    .unwrap();
    let budget =
        serde_json::from_value::<ResolvedBudget>(budget_fixture["resolved"]["valid"][0].clone())
            .unwrap();
    let usage = BudgetUsage::builder()
        .model_attempts(ExecutionCount::new(1))
        .model_turns(ExecutionCount::new(1))
        .input_bytes(ByteCount::new(1_024))
        .output_bytes(ByteCount::new(1_024))
        .known_costs(KnownCosts::empty())
        .build()
        .unwrap();
    GraphTerminalEvidence::new(descriptor, request, budget, AgentArtifacts::empty(), usage)
}

fn invocation_budget() -> ResolvedBudget {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-budget-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["resolved"]["valid"][0].clone()).unwrap()
}

fn invocation_schema_registry() -> JsonSchemaRegistry {
    let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    register_standard_invocation_execution_event_schema(&mut builder).unwrap();
    builder.build().unwrap()
}

fn invocation_activation(checkpoint: &Checkpoint) -> NodeActivation {
    let node_id = checkpoint.ready_nodes().iter().next().unwrap().clone();
    NodeActivation::for_ready_root(checkpoint, node_id).unwrap()
}

fn model_descriptor() -> ModelDescriptor {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["descriptors"]["valid"][0]["model"].clone()).unwrap()
}

fn model_request() -> ModelRequest {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-model-request-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["requests"]["valid"][0].clone()).unwrap()
}

fn streaming_model_request() -> ModelRequest {
    let complete = model_request();
    let mut builder = ModelRequest::builder(complete.limits().clone())
        .tool_selection(complete.tool_selection().clone())
        .max_tool_calls_per_response(complete.max_tool_calls_per_response())
        .strict_tool_arguments(complete.requires_strict_tool_arguments())
        .output_modalities(complete.output_modalities().clone())
        .text_output_format(complete.text_output_format().cloned())
        .response_mode(ModelResponseMode::Streaming)
        .reasoning_summaries(complete.requires_reasoning_summaries())
        .extensions(complete.extensions().clone());
    for instruction in complete.instructions() {
        builder = builder.instruction(instruction.clone());
    }
    for message in complete.messages() {
        builder = builder.message(message.clone());
    }
    for tool in complete.tools() {
        builder = builder.tool(tool.clone());
    }
    builder.build().unwrap()
}

fn model_response_for(
    descriptor: &ModelDescriptor,
    request: &ModelRequest,
    attempt_id: AttemptId,
) -> ModelResponse {
    let mut fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-model-response-v1.json"
    ))
    .unwrap();
    let mut value = fixture["responses"]["valid"][0].take();
    value["provenance"]["attempt_id"] = serde_json::to_value(attempt_id).unwrap();
    value["provenance"]["model"] = serde_json::to_value(descriptor.metadata().identity()).unwrap();
    let response = serde_json::from_value::<ModelResponse>(value).unwrap();
    response.validate_for(descriptor, request).unwrap();
    response
}

fn model_events_for(descriptor: &ModelDescriptor, attempt_id: AttemptId) -> Vec<ModelEvent> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-model-event-v1.json"
    ))
    .unwrap();
    fixture["events"]["valid"]
        .as_array()
        .unwrap()
        .iter()
        .cloned()
        .map(|mut value| {
            value["attempt_id"] = serde_json::to_value(attempt_id).unwrap();
            if value["event"]["type"] == "started" {
                value["event"]["content"]["provenance"]["attempt_id"] =
                    serde_json::to_value(attempt_id).unwrap();
                value["event"]["content"]["provenance"]["model"] =
                    serde_json::to_value(descriptor.metadata().identity()).unwrap();
            }
            serde_json::from_value(value).unwrap()
        })
        .collect()
}

fn tool_descriptor() -> ToolDescriptor {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-tool-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["descriptors"]["valid"][0].clone()).unwrap()
}

fn tool_input(descriptor: &ToolDescriptor) -> ToolInput {
    ToolInput::new(
        descriptor.input_schema().clone(),
        BoundedJson::try_from_value(json!({
            "amount": 42,
            "currency": "CNY"
        }))
        .unwrap(),
    )
    .unwrap()
}

fn durable_admission_request(
    fixture: &DriverFixture,
    tenant_id: TenantId,
    ids: AgentRunIds,
    output_schema: SchemaReference,
    authority_schema: SchemaReference,
) -> DurableAgentAdmissionRequest {
    let template = terminal_evidence(&fixture.graph);
    let descriptor = AgentDescriptor::new(
        template.descriptor().metadata().clone(),
        fixture.graph.input_schema().clone(),
        output_schema,
        template.descriptor().model().clone(),
        template.descriptor().instructions().clone(),
        template.descriptor().tools().clone(),
        template.descriptor().execution().clone(),
        template.descriptor().budget_limits().clone(),
    )
    .unwrap();
    let request = AgentRequest::new(
        fixture.graph.input_schema().clone(),
        BoundedJson::try_from_value(json!({"request": true})).unwrap(),
        BudgetLimits::empty(),
    );
    let policy = capability("runtime-agent-admission-policy");
    let evidence = JournalPayload::new(
        authority_schema,
        JournalEventKind::new(AgentAdmissionAuthority::EVIDENCE_KIND).unwrap(),
        BoundedJson::try_from_value(json!({"decision": "allow"})).unwrap(),
    )
    .unwrap();
    let authority = AgentAdmissionAuthority::new(
        policy.owner().clone(),
        descriptor.metadata().required_scopes().clone(),
        policy,
        Digest::sha256(b"runtime Agent admission policy v1"),
        evidence,
    )
    .unwrap();
    let mut budget_fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-runtime-v1.json"
    ))
    .unwrap();
    budget_fixture["base_budget_layers"][0]["deadline"] = json!("2099-01-01T00:00:00.000000Z");
    let limits =
        serde_json::from_value::<BudgetLimits>(budget_fixture["base_budget_layers"][0].clone())
            .unwrap();
    let layer = AgentAdmissionBudgetLayer::new(
        capability("runtime-agent-admission-budget"),
        authority.evidence().digest(),
        limits,
    )
    .unwrap();
    let initial_state = CheckpointState::new(
        fixture.graph.state_schema().clone(),
        BoundedJson::try_from_value(json!({"step": "initial"})).unwrap(),
    )
    .unwrap();
    DurableAgentAdmissionRequest::new(
        tenant_id,
        ids,
        descriptor,
        request,
        [layer],
        fixture.graph.reference(),
        authority,
        initial_state,
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_agent_admission_facade_validates_and_converges_exact_retries() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-agent-admission");
    store
        .register_graph_definition(tenant_id.clone(), fixture.graph.clone())
        .await
        .unwrap();
    let facade = DurableAgentAdmission::new(store.clone(), fixture.registry.clone()).unwrap();
    let ids = AgentRunIds::generate();
    let request = durable_admission_request(
        &fixture,
        tenant_id.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let restored = serde_json::from_value::<DurableAgentAdmissionRequest>(
        serde_json::to_value(&request).unwrap(),
    )
    .unwrap();
    assert_eq!(restored, request);

    let committed = facade.admit(request.clone()).await.unwrap();
    assert!(matches!(
        committed,
        AgentAdmissionCommitOutcome::Committed(_)
    ));
    let retry = facade.admit(restored).await.unwrap();
    assert!(matches!(retry, AgentAdmissionCommitOutcome::Idempotent(_)));
    let stored = retry.stored();
    assert_eq!(stored.run().lifecycle().status(), RunStatus::Active);
    assert_eq!(stored.event().sequence().get(), 1);
    assert_eq!(stored.checkpoint().superstep(), Superstep::INITIAL);
    assert_eq!(
        stored.event().payload().schema().id().as_str(),
        "https://stknot.com/schemas/runtime/agent-admission-event/1.0.0"
    );

    let mismatch_ids = AgentRunIds::generate();
    let mismatch = durable_admission_request(
        &fixture,
        tenant_id.clone(),
        mismatch_ids,
        fixture.graph.input_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    assert!(matches!(
        facade.admit(mismatch).await,
        Err(DurableAgentAdmissionError::GraphOutputSchemaMismatch)
    ));
    assert!(matches!(
        store.load_run(&tenant_id, mismatch_ids.run_id()).await,
        Err(StoreError::RunNotFound)
    ));

    let rejected_ids = AgentRunIds::generate();
    let rejected = durable_admission_request(
        &fixture,
        tenant_id.clone(),
        rejected_ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.state_schema().clone(),
    );
    assert!(matches!(
        facade.admit(rejected).await,
        Err(DurableAgentAdmissionError::AuthoritySchema {
            source: stateknot_core::GraphSchemaValidationError::Rejected
        })
    ));
    assert!(matches!(
        store.load_run(&tenant_id, rejected_ids.run_id()).await,
        Err(StoreError::RunNotFound)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn public_agent_run_facade_revalidates_active_and_successful_snapshots() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-public-agent-run");
    store
        .register_graph_definition(tenant_id.clone(), fixture.graph.clone())
        .await
        .unwrap();
    let ids = AgentRunIds::generate();
    let request = durable_admission_request(
        &fixture,
        tenant_id.clone(),
        ids,
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let intent = request.intent();
    let usage = BudgetUsage::builder()
        .model_attempts(ExecutionCount::new(1))
        .model_turns(ExecutionCount::new(1))
        .input_bytes(ByteCount::new(1_024))
        .output_bytes(ByteCount::new(1_024))
        .known_costs(KnownCosts::empty())
        .build()
        .unwrap();
    let terminal = GraphTerminalEvidence::new(
        intent.descriptor().clone(),
        intent.request().clone(),
        intent.budget().clone(),
        AgentArtifacts::empty(),
        usage,
    );
    let facade = DurableAgentRuns::new(store.clone(), fixture.registry.clone()).unwrap();
    let key = AgentSubmissionKey::new("request_runtime_public_agent_run_01").unwrap();

    let admitted = facade.submit(&key, request.clone()).await.unwrap();
    assert!(matches!(admitted, AgentRunAdmissionOutcome::Committed(_)));
    let active = admitted.snapshot();
    assert_eq!(active.provenance().run_id(), ids.run_id());
    assert_eq!(active.status(), RunStatus::Active);
    assert_eq!(active.revision().get(), 1);
    assert!(active.outcome().is_none());
    assert!(!active.is_quarantined());
    let retry_request = durable_admission_request(
        &fixture,
        tenant_id.clone(),
        AgentRunIds::generate(),
        fixture.graph.output_schema().clone(),
        fixture.graph.input_schema().clone(),
    );
    let retry = facade.submit(&key, retry_request.clone()).await.unwrap();
    assert!(matches!(retry, AgentRunAdmissionOutcome::Idempotent(_)));
    assert_eq!(retry.snapshot().provenance().run_id(), ids.run_id());
    assert_eq!(
        facade
            .load_by_key(&tenant_id, &key)
            .await
            .unwrap()
            .provenance()
            .run_id(),
        ids.run_id()
    );

    let conflicting_request = DurableAgentAdmissionRequest::new(
        tenant_id.clone(),
        AgentRunIds::generate(),
        retry_request.intent().descriptor().clone(),
        AgentRequest::new(
            fixture.graph.input_schema().clone(),
            BoundedJson::try_from_value(json!({"request": false})).unwrap(),
            BudgetLimits::empty(),
        ),
        retry_request.intent().budget_layers().iter().cloned(),
        retry_request.intent().graph().clone(),
        retry_request.intent().authority().clone(),
        retry_request.initial_state().clone(),
    )
    .unwrap();
    assert!(matches!(
        facade.submit(&key, conflicting_request).await,
        Err(DurableAgentRunsError::Admission(
            DurableAgentAdmissionError::Store(StoreError::AgentSubmissionConflict)
        ))
    ));

    let lease = store
        .claim_lease(&tenant_id, ids.run_id(), AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        fixture.registry,
        Arc::new(StaticLifecycleEvidence {
            terminal,
            failure: None,
        }),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let driven = agent_loop
        .run(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        driven.outcome(),
        AgentLoopOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));

    let succeeded = facade.load(&tenant_id, ids.run_id()).await.unwrap();
    assert_eq!(succeeded.status(), RunStatus::Succeeded);
    let result = match succeeded.outcome().unwrap() {
        AgentRunTerminalOutcome::Succeeded { result } => result,
        outcome => panic!("expected a successful public result, got {outcome:?}"),
    };
    assert_eq!(result.output().as_value(), &json!({"ok": true}));
    assert_eq!(result.provenance(), succeeded.provenance());
    let succeeded_by_key = facade.load_by_key(&tenant_id, &key).await.unwrap();
    assert_eq!(succeeded_by_key.status(), RunStatus::Succeeded);
    assert_eq!(succeeded_by_key.revision(), succeeded.revision());
    assert!(matches!(
        succeeded_by_key.outcome(),
        Some(AgentRunTerminalOutcome::Succeeded { .. })
    ));

    let wire = serde_json::to_value(&succeeded).unwrap();
    let restored = serde_json::from_value::<stateknot_runtime::AgentRunSnapshot>(wire).unwrap();
    assert_eq!(restored.status(), RunStatus::Succeeded);
    assert!(matches!(
        restored.outcome(),
        Some(AgentRunTerminalOutcome::Succeeded { .. })
    ));
    let mut impossible = serde_json::to_value(&restored).unwrap();
    impossible["status"] = serde_json::to_value(RunStatus::Active).unwrap();
    assert!(serde_json::from_value::<stateknot_runtime::AgentRunSnapshot>(impossible).is_err());
    let mut incomplete = serde_json::to_value(&restored).unwrap();
    incomplete.as_object_mut().unwrap().remove("outcome");
    assert!(serde_json::from_value::<stateknot_runtime::AgentRunSnapshot>(incomplete).is_err());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn public_agent_run_facade_exposes_only_confirmed_cancellation_as_terminal() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-public-agent-cancelled");
    store
        .register_graph_definition(tenant_id.clone(), fixture.graph.clone())
        .await
        .unwrap();
    let facade = DurableAgentRuns::new(store.clone(), fixture.registry.clone()).unwrap();
    let ids = AgentRunIds::generate();
    let run_id = ids.run_id();
    let key = AgentSubmissionKey::new("request_runtime_public_cancelled_01").unwrap();
    facade
        .submit(
            &key,
            durable_admission_request(
                &fixture,
                tenant_id.clone(),
                ids,
                fixture.graph.output_schema().clone(),
                fixture.graph.input_schema().clone(),
            ),
        )
        .await
        .unwrap();

    let admitted = store
        .load_agent_admission(&tenant_id, run_id)
        .await
        .unwrap();
    let cancellation_failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Cancelled,
        FailureCode::new("runtime.public_cancelled").unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new("The public Agent run was cancelled safely.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap();
    let cancellation_failure_id = cancellation_failure.id();
    let cancellation = RunCancellationRequest::new(
        cancellation_failure,
        admitted.run().lifecycle().changed_at(),
    )
    .unwrap();
    let requested = store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(admitted.event().head()),
                JournalEventIntent::control_plane(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    test_payload(),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                admitted.run().lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: cancellation,
                },
            ),
        )
        .await
        .unwrap();
    let cancelling = facade.load_by_key(&tenant_id, &key).await.unwrap();
    assert_eq!(cancelling.status(), RunStatus::CancellationRequested);
    assert!(cancelling.outcome().is_none());

    let requested_run = store.load_run(&tenant_id, run_id).await.unwrap();
    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(requested.event().head()),
                JournalEventIntent::control_plane(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    test_payload(),
                )
                .unwrap(),
            )
            .unwrap(),
            RunProjection::transition(
                requested_run.lifecycle().revision(),
                RunTransition::ConfirmCancellation {
                    completed_at: requested.event().recorded_at(),
                    usage: BudgetUsage::zero(),
                },
            ),
        )
        .await
        .unwrap();

    let cancelled = facade.load(&tenant_id, run_id).await.unwrap();
    assert_eq!(cancelled.status(), RunStatus::Cancelled);
    let AgentRunTerminalOutcome::Cancelled {
        failure,
        completed_at,
        usage,
    } = cancelled.outcome().unwrap()
    else {
        panic!("expected a cancelled public outcome")
    };
    assert_eq!(failure.id(), cancellation_failure_id);
    assert_eq!(failure.category(), FailureCategory::Cancelled);
    assert_eq!(failure.retry_advice(), RetryAdvice::Never);
    assert_eq!(*completed_at, cancelled.changed_at());
    assert_eq!(usage, &BudgetUsage::zero());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn model_executor_rebinds_terminal_evidence_after_lease_takeover_without_redispatch() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-model-executor");
    let run_id = RunId::generate();
    let checkpoint = start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let descriptor = model_descriptor();
    let request = model_request();
    let invocation_id = InvocationId::generate();
    let intent = ModelInvocationIntent::new(
        invocation_activation(checkpoint.checkpoint()),
        invocation_id,
        descriptor.clone(),
        request,
    )
    .unwrap();
    let prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                checkpoint.event().head(),
                lease.fence().clone(),
            ),
            intent,
        )
        .await
        .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let budget_calls = Arc::new(AtomicUsize::new(0));
    let replacement_fence = Arc::new(tokio::sync::Mutex::new(None));
    let mut models = ModelProviderRegistryBuilder::new();
    models
        .register(Arc::new(LeaseRotatingModel {
            descriptor,
            store: store.clone(),
            calls: Arc::clone(&calls),
            replacement_fence: Arc::clone(&replacement_fence),
        }))
        .unwrap();
    let executor = DurableInvocationExecutor::new(
        store.clone(),
        invocation_schema_registry(),
        models.build(),
        ToolProviderRegistryBuilder::new().build(),
        Arc::new(OneShotInvocationBudget {
            resolved: invocation_budget(),
            calls: Arc::clone(&budget_calls),
        }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let handoff = ModelAttemptHandoff::new(
        lease.fence().clone(),
        prepared.invocation().clone(),
        AttemptId::generate(),
        InvocationAttemptEventIds::generate(),
        CancellationSignal::never(),
        None,
    )
    .unwrap();

    let terminal_error = match executor.execute_model(handoff.clone()).await {
        Err(ModelAttemptExecutionError::Terminal(error)) => error,
        outcome => panic!("lease takeover must retain terminal model evidence: {outcome:?}"),
    };
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let new_fence = replacement_fence
        .lock()
        .await
        .take()
        .expect("provider must publish the replacement fence");
    assert_ne!(new_fence.epoch(), lease.fence().epoch());
    let recovery = terminal_error
        .into_recovery()
        .rebind_fence(new_fence)
        .unwrap();
    assert_eq!(recovery.kind(), ModelAttemptTerminalKind::Response);
    let committed = executor.commit_model_terminal(recovery).await.unwrap();
    assert!(matches!(
        committed,
        ModelAttemptOutcome::Dispatched {
            terminal: ModelAttemptTerminalKind::Response,
            ..
        }
    ));
    assert_eq!(
        store
            .load_model_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .status(),
        ModelInvocationStatus::Committed
    );

    assert!(matches!(
        executor.execute_model(handoff).await.unwrap(),
        ModelAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(budget_calls.load(Ordering::SeqCst), 1);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn streaming_model_executor_validates_emits_accumulates_and_deduplicates() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-streaming-model");
    let run_id = RunId::generate();
    let checkpoint = start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let descriptor = model_descriptor();
    let invocation_id = InvocationId::generate();
    let intent = ModelInvocationIntent::new(
        invocation_activation(checkpoint.checkpoint()),
        invocation_id,
        descriptor.clone(),
        streaming_model_request(),
    )
    .unwrap();
    let prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                checkpoint.event().head(),
                lease.fence().clone(),
            ),
            intent,
        )
        .await
        .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(RecordingModelEventSink::default());
    let mut models = ModelProviderRegistryBuilder::new();
    models
        .register(Arc::new(StreamingModel {
            descriptor,
            calls: Arc::clone(&calls),
        }))
        .unwrap();
    let executor = DurableInvocationExecutor::new(
        store.clone(),
        invocation_schema_registry(),
        models.build(),
        ToolProviderRegistryBuilder::new().build(),
        Arc::new(StaticInvocationBudget {
            resolved: invocation_budget(),
        }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let handoff = ModelAttemptHandoff::new(
        lease.fence().clone(),
        prepared.invocation().clone(),
        AttemptId::generate(),
        InvocationAttemptEventIds::generate(),
        CancellationSignal::never(),
        Some(sink.clone()),
    )
    .unwrap();
    assert!(matches!(
        executor.execute_model(handoff.clone()).await.unwrap(),
        ModelAttemptOutcome::Dispatched {
            terminal: ModelAttemptTerminalKind::Response,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let events = sink.events.lock().await;
    assert_eq!(events.len(), 7);
    for (sequence, event) in events.iter().enumerate() {
        assert_eq!(event.sequence().get(), u64::try_from(sequence).unwrap());
    }
    drop(events);
    assert_eq!(
        store
            .load_model_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .status(),
        ModelInvocationStatus::Committed
    );

    assert!(matches!(
        executor.execute_model(handoff).await.unwrap(),
        ModelAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(sink.events.lock().await.len(), 7);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn timed_out_tool_write_commits_unknown_and_duplicate_start_never_redispatches() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-tool-executor");
    let run_id = RunId::generate();
    let checkpoint = start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let descriptor = tool_descriptor();
    let invocation_id = InvocationId::generate();
    let intent = ToolInvocationIntent::new(
        invocation_activation(checkpoint.checkpoint()),
        invocation_id,
        descriptor.clone(),
        tool_input(&descriptor),
        descriptor.limits().clone(),
    )
    .unwrap();
    let prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                checkpoint.event().head(),
                lease.fence().clone(),
            ),
            intent,
        )
        .await
        .unwrap();

    let calls = Arc::new(AtomicUsize::new(0));
    let mut tools = ToolProviderRegistryBuilder::new();
    tools
        .register(Arc::new(PendingWriteTool {
            descriptor,
            calls: Arc::clone(&calls),
        }))
        .unwrap();
    let executor = DurableInvocationExecutor::with_clock(
        store.clone(),
        invocation_schema_registry(),
        ModelProviderRegistryBuilder::new().build(),
        tools.build(),
        Arc::new(StaticInvocationBudget {
            resolved: invocation_budget(),
        }),
        Arc::new(FixedInvocationClock {
            observed_at: "2029-12-31T23:59:59.000000Z".parse().unwrap(),
        }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let handoff = ToolAttemptHandoff::new(
        lease.fence().clone(),
        prepared.invocation().clone(),
        AttemptId::generate(),
        InvocationAttemptEventIds::generate(),
        CancellationSignal::never(),
        None,
    )
    .unwrap();
    assert!(matches!(
        executor.execute_tool(handoff.clone()).await.unwrap(),
        ToolAttemptOutcome::Dispatched {
            terminal: ToolAttemptTerminalKind::Error,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let stored = store
        .load_tool_invocation(&tenant_id, run_id, invocation_id)
        .await
        .unwrap();
    assert_eq!(stored.status(), ToolInvocationStatus::Unknown);
    let ToolInvocationState::Unknown { error } = stored.state() else {
        panic!("timed-out write must retain ambiguous outcome evidence")
    };
    assert_eq!(error.external_effect(), ToolExternalEffect::Unknown);
    assert_eq!(
        error.failure().category(),
        FailureCategory::AmbiguousExternalOutcome
    );
    assert_eq!(error.failure().retry_advice(), RetryAdvice::ReconcileFirst);

    assert!(matches!(
        executor.execute_tool(handoff).await.unwrap(),
        ToolAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_fair_scheduler_preserves_exact_cross_tenant_share_across_ticks() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let primary = tenant("fair-runtime-primary");
    let secondary = tenant("fair-runtime-secondary");
    let policy = WeightedFairnessPolicy::new(
        SchedulerShardId::new(format!("runtime-fairness-{}", RunId::generate())).unwrap(),
        [
            TenantFairnessWeight::new(primary.clone(), 3).unwrap(),
            TenantFairnessWeight::new(secondary.clone(), 1).unwrap(),
        ],
    )
    .unwrap();
    let scheduler = DurableFairScheduler::register(
        store.clone(),
        fixture.registry,
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
        DurableTenantSchedulerOptions::default(),
        policy,
        DurableFairSchedulerOptions::default(),
    )
    .await
    .unwrap();

    let mut selected = Vec::new();
    for expected_sequence in 0..4_u64 {
        let tick = scheduler.tick(CancellationSignal::never()).await.unwrap();
        assert_eq!(tick.reservation().sequence(), expected_sequence);
        assert_eq!(u64::from(tick.reservation().slot()), expected_sequence);
        assert!(matches!(
            tick.tenant_tick().outcome(),
            TenantSchedulerOutcome::Idle
        ));
        selected.push(tick.tenant_id().clone());
    }
    assert_eq!(
        selected.iter().filter(|tenant| **tenant == primary).count(),
        3
    );
    assert_eq!(
        selected
            .iter()
            .filter(|tenant| **tenant == secondary)
            .count(),
        1
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_handoff_lost_ack_retries_without_rereading_success_evidence() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let evidence = terminal_evidence(&fixture.graph);
    let tenant_id = tenant("runtime-lifecycle-terminal");
    let run_id = RunId::generate();
    let admitted_provenance = AgentResultProvenance::for_agent(
        tenant_id.clone(),
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        evidence.descriptor(),
    );
    start_run_with_provenance(
        &store,
        &fixture.graph,
        admitted_provenance,
        json!({"step": "initial"}),
    )
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let drive_result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let (outcome, _) = drive_result.into_parts();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = outcome else {
        panic!("graph must reach a terminal lifecycle handoff")
    };
    let handoff = *handoff;
    let terminal_event_id = handoff.event_id();
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry.clone(),
        Arc::new(StaticLifecycleEvidence {
            terminal: evidence,
            failure: None,
        }),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();

    let committed = lifecycle.commit_barrier(handoff.clone()).await.unwrap();
    assert!(matches!(
        committed,
        GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));
    let retry_lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let retry = retry_lifecycle.commit_barrier(handoff).await.unwrap();
    assert!(matches!(
        retry,
        GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Idempotent { .. })
    ));

    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(stored.lifecycle().status(), RunStatus::Succeeded);
    assert!(stored.lease().is_none());
    assert_eq!(stored.journal_head().unwrap().event_id(), terminal_event_id);
    assert_eq!(
        stored.lifecycle().result().unwrap().output().as_value(),
        &json!({"ok": true})
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn wait_handoff_replays_exactly_after_a_later_cancellation_advances_the_run() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let (fixture, timer_id, due_at) = wait_fixture();
    let evidence = terminal_evidence(&fixture.graph);
    let tenant_id = tenant("runtime-lifecycle-wait");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let drive_result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let (outcome, _) = drive_result.into_parts();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = outcome else {
        panic!("graph must reach a wait lifecycle handoff")
    };
    assert!(matches!(
        handoff.plan().disposition(),
        GraphBarrierDisposition::Wait { .. }
    ));
    let handoff = *handoff;
    let wait_event_id = handoff.event_id();
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(StaticLifecycleEvidence {
            terminal: evidence,
            failure: None,
        }),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();

    let committed = lifecycle.commit_barrier(handoff.clone()).await.unwrap();
    let GraphBarrierLifecycleOutcome::Waiting(wait_commit) = committed else {
        panic!("wait handoff must commit a wait barrier")
    };
    assert!(matches!(
        &wait_commit,
        WaitCheckpointCommitOutcome::Committed { .. }
    ));
    assert_eq!(wait_commit.event().head().event_id(), wait_event_id);
    assert_eq!(wait_commit.waits().len(), 1);
    let stateknot_core::DurableWait::Timer { timer } = &wait_commit.waits()[0] else {
        panic!("registered wait must remain a timer")
    };
    assert_eq!(timer.marker().timer_id(), timer_id);
    assert_eq!(timer.marker().due_at(), due_at);
    assert_eq!(
        timer.marker().scheduled_at(),
        wait_commit.event().head().recorded_at()
    );

    let retry = lifecycle.commit_barrier(handoff.clone()).await.unwrap();
    assert!(matches!(
        retry,
        GraphBarrierLifecycleOutcome::Waiting(WaitCheckpointCommitOutcome::Idempotent { .. })
    ));
    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(stored.lifecycle().status(), RunStatus::Waiting);
    assert!(stored.lease().is_none());
    assert_eq!(stored.unresolved_wait_count(), 1);
    assert_eq!(stored.next_timer_due_at(), Some(due_at));
    assert_eq!(stored.journal_head().unwrap().event_id(), wait_event_id);

    let cancellation_event_id = EventId::generate();
    let cancellation = RunCancellationRequest::new(
        Failure::new(
            FailureId::generate(),
            FailureCategory::Cancelled,
            FailureCode::new("runtime.test_cancelled").unwrap(),
            FailureOrigin::new("stateknot.runtime.integration").unwrap(),
            FailureMessage::new("The integration run was cancelled after waiting.").unwrap(),
            RetryAdvice::Never,
        )
        .unwrap(),
        stored.lifecycle().changed_at(),
    )
    .unwrap();
    let cancellation_append = JournalAppend::new(
        JournalExpectation::exact(wait_commit.event().head()),
        JournalEventIntent::control_plane(
            tenant_id.clone(),
            run_id,
            cancellation_event_id,
            test_payload(),
        )
        .unwrap(),
    )
    .unwrap();
    let abandoned = store
        .append_control_plane_abandon_waits(
            cancellation_append,
            stored.lifecycle().revision(),
            RunTransition::RequestCancellation {
                request: cancellation,
            },
        )
        .await
        .unwrap();
    assert_eq!(abandoned.abandonments().len(), 1);

    let late_retry = lifecycle.commit_barrier(handoff).await.unwrap();
    let GraphBarrierLifecycleOutcome::Waiting(late_retry) = late_retry else {
        panic!("the original wait handoff must remain replayable")
    };
    assert!(matches!(
        late_retry,
        WaitCheckpointCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(late_retry.event().head().event_id(), wait_event_id);
    assert_eq!(late_retry.waits(), wait_commit.waits());

    let advanced = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(
        advanced.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(advanced.unresolved_wait_count(), 0);
    assert!(advanced.lease().is_none());
    assert_eq!(
        advanced.journal_head().unwrap().event_id(),
        cancellation_event_id
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn failed_supervision_lost_ack_retries_without_rereading_failure_evidence() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = failure_fixture();
    let terminal = terminal_evidence(&fixture.graph);
    let aggregate_failure = test_failure(
        "graph.run_failed",
        "Graph execution exhausted its durable recovery policy.",
    );
    let aggregate_failure_id = aggregate_failure.id();
    let tenant_id = tenant("runtime-lifecycle-failure");
    store
        .register_graph_definition(tenant_id.clone(), fixture.graph.clone())
        .await
        .unwrap();
    let facade = DurableAgentRuns::new(store.clone(), fixture.registry.clone()).unwrap();
    let ids = AgentRunIds::generate();
    let run_id = ids.run_id();
    let key = AgentSubmissionKey::new("request_runtime_public_failed_01").unwrap();
    facade
        .submit(
            &key,
            durable_admission_request(
                &fixture,
                tenant_id.clone(),
                ids,
                fixture.graph.output_schema().clone(),
                fixture.graph.input_schema().clone(),
            ),
        )
        .await
        .unwrap();
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let drive_result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let (outcome, _) = drive_result.into_parts();
    let GraphDriveOutcome::Blocked(blocked) = outcome else {
        panic!("terminal node failure must produce a supervision handoff")
    };
    assert_eq!(blocked.blockers().failed(), 1);
    assert_eq!(blocked.blockers().in_flight(), 0);
    let blocked = *blocked;
    let failure_event_id = blocked.event_id();
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry.clone(),
        Arc::new(StaticLifecycleEvidence {
            terminal,
            failure: Some(GraphFailureEvidence::new(
                aggregate_failure,
                BudgetUsage::zero(),
            )),
        }),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();

    let committed = lifecycle.resolve_blocked(blocked.clone()).await.unwrap();
    assert!(matches!(
        committed,
        GraphBarrierLifecycleOutcome::Failed(stateknot_store_postgres::AppendOutcome::Committed(_))
    ));
    let retry_lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let retry = retry_lifecycle.resolve_blocked(blocked).await.unwrap();
    assert!(matches!(
        retry,
        GraphBarrierLifecycleOutcome::Failed(stateknot_store_postgres::AppendOutcome::Idempotent(
            _
        ))
    ));
    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(stored.lifecycle().status(), RunStatus::Failed);
    assert_eq!(
        stored.lifecycle().terminal_failure().unwrap().id(),
        aggregate_failure_id
    );
    assert!(stored.lease().is_none());
    assert_eq!(stored.journal_head().unwrap().event_id(), failure_event_id);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    let public = facade.load_by_key(&tenant_id, &key).await.unwrap();
    assert_eq!(public.status(), RunStatus::Failed);
    let AgentRunTerminalOutcome::Failed {
        failure,
        completed_at,
        usage,
    } = public.outcome().unwrap()
    else {
        panic!("expected a failed public outcome")
    };
    assert_eq!(failure.id(), aggregate_failure_id);
    assert_eq!(*completed_at, public.changed_at());
    assert_eq!(usage, &BudgetUsage::zero());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_agent_loop_drives_and_commits_one_claimed_run_to_success() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let evidence = terminal_evidence(&fixture.graph);
    let tenant_id = tenant("runtime-agent-loop-success");
    let run_id = RunId::generate();
    let admitted_provenance = AgentResultProvenance::for_agent(
        tenant_id.clone(),
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        evidence.descriptor(),
    );
    start_run_with_provenance(
        &store,
        &fixture.graph,
        admitted_provenance,
        json!({"step": "initial"}),
    )
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        fixture.registry,
        Arc::new(StaticLifecycleEvidence {
            terminal: evidence,
            failure: None,
        }),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();

    let result = agent_loop
        .run(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        AgentLoopOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));
    assert_eq!(result.report().node_attempts_completed(), 2);
    assert_eq!(result.report().barriers_committed(), 1);
    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(stored.lifecycle().status(), RunStatus::Succeeded);
    assert!(stored.lease().is_none());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_agent_loop_releases_lease_when_terminal_evidence_is_unavailable() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-agent-loop-evidence-error");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        fixture.registry,
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();

    let error = agent_loop
        .run(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap_err();
    assert!(matches!(error, AgentLoopError::Lifecycle { .. }));
    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(stored.lifecycle().status(), RunStatus::Active);
    assert!(stored.lease().is_none());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_scheduler_scans_claims_executes_and_then_observes_idle() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let evidence = terminal_evidence(&fixture.graph);
    let tenant_id = tenant("runtime-tenant-scheduler");
    let run_id = RunId::generate();
    let admitted_provenance = AgentResultProvenance::for_agent(
        tenant_id.clone(),
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        evidence.descriptor(),
    );
    start_run_with_provenance(
        &store,
        &fixture.graph,
        admitted_provenance,
        json!({"step": "initial"}),
    )
    .await;
    let scheduler = DurableTenantScheduler::new(
        store.clone(),
        fixture.registry,
        Arc::new(StaticLifecycleEvidence {
            terminal: evidence,
            failure: None,
        }),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
        DurableTenantSchedulerOptions::new(
            RunnableRunPageSize::new(4).unwrap(),
            2,
            3,
            Duration::from_millis(25),
        )
        .unwrap(),
    )
    .unwrap();

    let tick = scheduler
        .tick(tenant_id.clone(), CancellationSignal::never())
        .await
        .unwrap();
    let TenantSchedulerOutcome::Executed {
        run_id: selected,
        result,
    } = tick.outcome()
    else {
        panic!("scheduler must execute the only runnable run")
    };
    assert_eq!(*selected, run_id);
    assert!(matches!(result.outcome(), AgentLoopOutcome::Succeeded(_)));
    assert_eq!(tick.report().pages_scanned(), 1);
    assert_eq!(tick.report().candidates_scanned(), 1);

    let idle = scheduler
        .tick(tenant_id.clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(idle.outcome(), TenantSchedulerOutcome::Idle));
    assert_eq!(idle.report().pages_scanned(), 1);
    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(stored.lifecycle().status(), RunStatus::Succeeded);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_advances_then_replays_a_noninitial_checkpoint_before_terminal_handoff() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-driver-replay");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;

    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        3,
        Duration::from_secs(10),
        Duration::from_secs(60),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.registry.clone(), options).unwrap();
    let first = driver
        .drive(first_lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        first.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert_eq!(first.report().durable_events(), 3);
    assert_eq!(first.report().node_attempts_started(), 1);
    assert_eq!(first.report().node_attempts_completed(), 1);
    assert_eq!(first.report().barriers_committed(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap()
            .unwrap()
            .superstep()
            .get(),
        1
    );

    let second_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let second = driver
        .drive(second_lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let GraphDriveOutcome::LifecycleBarrierReady(handoff) = second.outcome() else {
        panic!("second superstep must reach a terminal lifecycle handoff")
    };
    assert!(matches!(
        handoff.plan().disposition(),
        GraphBarrierDisposition::Terminal { .. }
    ));
    assert_eq!(handoff.plan().barrier().successor().superstep().get(), 2);
    assert_eq!(second.report().replay().barriers_replayed(), 1);
    assert_eq!(second.report().replay().checkpoints_validated(), 2);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.release_lease(second_lease.fence()).await.unwrap(),
        LeaseReleaseOutcome::Released
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_fence_inflight_start_is_never_executed_twice() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let evidence = terminal_evidence(&fixture.graph);
    let tenant_id = tenant("runtime-driver-inflight");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        Digest::sha256(b"runtime driver in-flight integration evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(lease.fence().clone(), context)
        .await
        .unwrap();
    let plan = recovery.plan_ready_nodes().await.unwrap();
    let node_id = NodeId::new("Step_A").unwrap();
    let append = worker_append(
        tenant_id.clone(),
        run_id,
        EventId::generate(),
        plan.journal_head().clone(),
        lease.fence().clone(),
    );
    let started = store
        .start_recovered_node_attempt(append, &plan, &node_id, AttemptId::generate())
        .await
        .unwrap();
    assert!(matches!(
        started,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    drop(recovery);

    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();
    let drive_result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    let (outcome, _) = drive_result.into_parts();
    let GraphDriveOutcome::Blocked(blocked) = outcome else {
        panic!("same-fence unfinished start must block duplicate execution")
    };
    assert_eq!(blocked.blockers().in_flight(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(StaticLifecycleEvidence {
            terminal: evidence,
            failure: None,
        }),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let released = lifecycle.resolve_blocked(*blocked).await.unwrap();
    assert!(matches!(
        released,
        GraphBarrierLifecycleOutcome::Released(LeaseReleaseOutcome::Released)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn long_running_node_renews_its_lease_until_completion() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(1)).await else {
        return;
    };
    let fixture = driver_fixture_with_first_delay(Duration::from_millis(1_300));
    let tenant_id = tenant("runtime-driver-renewal");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        3,
        Duration::from_millis(100),
        Duration::from_secs(3),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.registry, options).unwrap();
    let result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert!(result.report().lease_renewals() >= 5);
    assert_eq!(result.report().node_attempts_completed(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn near_expiry_lease_is_refreshed_before_node_code_is_launched() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(1)).await else {
        return;
    };
    let fixture = driver_fixture_with_first_delay(Duration::from_millis(500));
    let tenant_id = tenant("runtime-driver-preflight-renewal");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    tokio::time::sleep(Duration::from_millis(750)).await;

    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        2,
        Duration::from_millis(200),
        Duration::from_secs(2),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.registry, options).unwrap();
    let result = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();

    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert!(result.report().lease_renewals() >= 2);
    assert_eq!(result.report().node_attempts_completed(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_initial_checkpoint_state_is_quarantined_before_node_launch() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-driver-invalid-initial-state");
    let run_id = RunId::generate();
    start_run_with_state(
        &store,
        &fixture.graph,
        tenant_id.clone(),
        run_id,
        json!({"step": 1}),
    )
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry,
        DurableGraphDriverOptions::default(),
    )
    .unwrap();

    let error = driver
        .drive(lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        GraphDriverError::Store { source }
            if matches!(source.as_ref(), StoreError::RunQuarantined)
    ));
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.second_calls.load(Ordering::SeqCst), 0);
    let quarantined = store.load_run(&tenant_id, run_id).await.unwrap();
    assert!(quarantined.is_quarantined());
    assert!(quarantined.lease().is_none());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn successor_fence_takes_over_an_unfinished_attempt_once() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-driver-takeover");
    let run_id = RunId::generate();
    start_run(&store, &fixture.graph, tenant_id.clone(), run_id).await;
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(run.journal_head().unwrap().clone()),
        Digest::sha256(b"runtime driver takeover integration evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(first_lease.fence().clone(), context)
        .await
        .unwrap();
    let plan = recovery.plan_ready_nodes().await.unwrap();
    let node_id = NodeId::new("Step_A").unwrap();
    let started = store
        .start_recovered_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                plan.journal_head().clone(),
                first_lease.fence().clone(),
            ),
            &plan,
            &node_id,
            AttemptId::generate(),
        )
        .await
        .unwrap();
    assert!(matches!(
        started,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    drop(recovery);

    let successor = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let options = DurableGraphDriverOptions::new(
        GraphReplayLimits::default(),
        2,
        Duration::from_secs(10),
        Duration::from_secs(60),
        3,
        Duration::from_millis(25),
    )
    .unwrap();
    let driver = DurableGraphDriver::new(store.clone(), fixture.registry, options).unwrap();
    let result = driver
        .drive(successor.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(matches!(
        result.outcome(),
        GraphDriveOutcome::Yielded {
            release: LeaseReleaseOutcome::Released
        }
    ));
    assert_eq!(result.report().node_attempts_started(), 1);
    assert_eq!(result.report().node_attempts_completed(), 1);
    assert_eq!(fixture.first_calls.load(Ordering::SeqCst), 1);
    store.close().await;
}

async fn test_store() -> Option<PostgresStore> {
    test_store_with_lease_duration(Duration::from_secs(30)).await
}

async fn test_store_with_lease_duration(lease_duration: Duration) -> Option<PostgresStore> {
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, 8)
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
        .with_lease_timing(lease_duration, Duration::from_secs(5 * 60));
    PostgresStore::migrate_database(&database_url, options.clone())
        .await
        .expect("migrations must succeed");
    Some(
        PostgresStore::connect(&database_url, options)
            .await
            .expect("test PostgreSQL must connect with an exact schema"),
    )
}

async fn start_run(
    store: &PostgresStore,
    graph: &CompiledGraph,
    tenant_id: TenantId,
    run_id: RunId,
) -> CheckpointCommitOutcome {
    start_run_with_state(store, graph, tenant_id, run_id, json!({"step": "initial"})).await
}

async fn start_run_with_state(
    store: &PostgresStore,
    graph: &CompiledGraph,
    tenant_id: TenantId,
    run_id: RunId,
    state_value: Value,
) -> CheckpointCommitOutcome {
    start_run_with_provenance(store, graph, provenance(tenant_id, run_id), state_value).await
}

async fn start_run_with_provenance(
    store: &PostgresStore,
    graph: &CompiledGraph,
    provenance: AgentResultProvenance,
    state_value: Value,
) -> CheckpointCommitOutcome {
    let tenant_id = provenance.tenant_id().clone();
    let run_id = provenance.run_id();
    let registered = store
        .register_graph_definition(tenant_id.clone(), graph.clone())
        .await
        .unwrap();
    assert!(matches!(
        registered,
        GraphDefinitionRegistrationOutcome::Registered(_)
            | GraphDefinitionRegistrationOutcome::Idempotent(_)
    ));
    let admitted = store.admit_run(provenance).await.unwrap();
    let state = CheckpointState::new(
        graph.state_schema().clone(),
        BoundedJson::try_from_value(state_value).unwrap(),
    )
    .unwrap();
    let write = CheckpointWrite::initial(
        tenant_id.clone(),
        run_id,
        CheckpointId::generate(),
        graph.reference(),
        state,
        graph.entry_nodes().clone(),
    )
    .unwrap();
    store
        .append_control_plane_checkpoint(
            control_append(tenant_id, run_id, EventId::generate()),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
            write,
        )
        .await
        .unwrap()
}

fn schema(name: &str) -> (SchemaReference, Value) {
    let id = format!("https://stknot.com/schemas/tests/{name}/1.0.0");
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": id,
        "type": "object"
    });
    let canonical = serde_json_canonicalizer::to_vec(&document).unwrap();
    (
        SchemaReference::new(
            id.parse::<SchemaId>().unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(canonical),
        ),
        document,
    )
}

fn state_schema() -> (SchemaReference, Value) {
    let id = "https://stknot.com/schemas/tests/driver-state/1.0.0";
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": id,
        "type": "object",
        "properties": {"step": {"type": "string"}},
        "required": ["step"],
        "additionalProperties": false
    });
    let canonical = serde_json_canonicalizer::to_vec(&document).unwrap();
    (
        SchemaReference::new(
            id.parse::<SchemaId>().unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(canonical),
        ),
        document,
    )
}

fn capability(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.com/stateknot"
                .parse::<IssuerId>()
                .unwrap(),
            "runtime-driver-tests".parse::<SubjectId>().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}

fn provenance(tenant_id: TenantId, run_id: RunId) -> AgentResultProvenance {
    AgentResultProvenance::new(
        tenant_id,
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        capability("driver-agent"),
    )
}

fn tenant(prefix: &str) -> TenantId {
    TenantId::new(format!("{prefix}-{}", RunId::generate())).unwrap()
}

fn test_payload() -> JournalPayload {
    JournalPayload::new(
        SchemaReference::new(
            "https://stknot.com/schemas/tests/control-event/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"runtime integration control event schema"),
        ),
        JournalEventKind::new("runtime-integration").unwrap(),
        BoundedJson::try_from_value(json!({"test": true})).unwrap(),
    )
    .unwrap()
}

fn control_append(tenant_id: TenantId, run_id: RunId, event_id: EventId) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(tenant_id, run_id, event_id, test_payload()).unwrap(),
    )
    .unwrap()
}

fn worker_append(
    tenant_id: TenantId,
    run_id: RunId,
    event_id: EventId,
    head: stateknot_core::JournalHead,
    fence: RunFence,
) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(head),
        JournalEventIntent::worker(tenant_id, run_id, event_id, fence, test_payload()).unwrap(),
    )
    .unwrap()
}
