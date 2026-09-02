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
    AgentExecutionConfig, AgentRequest, AgentResultProvenance, AgentStructuredOutputStrategy,
    AgentSubmissionKey, AgentToolConcurrency, AgentTools, AttemptId, BoundedJson, BoxFuture,
    BoxStream, BudgetLimits, BudgetRemaining, BudgetUsage, ByteCount, CancellationSignal,
    CapabilityIdentity, CapabilityName, CapabilityReference, Checkpoint, CheckpointId,
    CheckpointState, CheckpointWrite, CompiledGraph, ContentMetadata, ContentPart, ContentSource,
    Digest, ErasedTool, EventId, ExecutionCount, Extensions, Failure, FailureCategory, FailureCode,
    FailureId, FailureMessage, FailureOrigin, GraphBarrierDisposition, GraphExecutionLimits,
    GraphNode, GraphReducer, GraphReducerError, GraphReducerInput, GraphReducerReference,
    GraphReference, GraphRoutes, GraphSchemaValidationError, InvocationId, IssuerId, JournalAppend,
    JournalEventIntent, JournalEventKind, JournalExpectation, JournalPayload, JsonContent,
    KnownCosts, Model, ModelContext, ModelDescriptor, ModelError, ModelEvent, ModelFinishReason,
    ModelInvocationIntent, ModelInvocationStatus, ModelOutputItem, ModelProviderReplay,
    ModelProviderReplayFormat, ModelProviderToolCallId, ModelRequest, ModelResponse,
    ModelResponseMode, ModelResponseProvenance, ModelToolCallProposal, ModelUsage, NodeActivation,
    NodeControl, NodeId, NodeInvocationBindings, NodeStateChange, NodeTerminalOutput, NodeWait,
    NodeWaits, PrincipalIdentity, QuarantineId, ReadyNodes, ResolvedBudget, RetryAdvice,
    RunCancellationRequest, RunFence, RunId, RunStatus, RunTimerKind, RunTransition,
    SchedulerShardId, SchemaId, SchemaReference, SecurityLabel, SubjectId, Superstep, TenantId,
    ThreadId, TimerId, Timestamp, TokenCount, ToolArtifacts, ToolContext, ToolDescriptor,
    ToolError, ToolErrorPhase, ToolErrorProvenance, ToolExternalEffect, ToolInput,
    ToolInvocationIntent, ToolInvocationState, ToolInvocationStatus, ToolResult,
    ToolResultProvenance, Version,
};
use stateknot_runtime::{
    AgentCancellationIds, AgentCancellationOutcome, AgentInvocationAccounting,
    AgentInvocationAccountingReference, AgentInvocationCharge, AgentLoopError, AgentLoopOutcome,
    AgentRunAdmissionOutcome, AgentRunIds, AgentRunTerminalOutcome, AgentServiceAuthorizationError,
    AgentServiceAuthorizer, AgentServiceCaller, AgentServiceError, AgentServiceRegistryBuilder,
    AgentServiceRunAuthorization, AgentServiceRunGrant, AgentServiceSubmissionAuthorization,
    AgentServiceSubmissionGrant, AgentServiceV1, AgentToolPolicy, AgentToolPolicyContext,
    AgentToolPolicyDecision, AgentToolPolicyError, AgentToolPolicyReference, DurableAgentAdmission,
    DurableAgentAdmissionError, DurableAgentAdmissionRequest, DurableAgentLoop, DurableAgentRuns,
    DurableAgentRunsError, DurableFairScheduler, DurableFairSchedulerOptions, DurableGraphDriver,
    DurableGraphDriverOptions, DurableGraphLifecycle, DurableGraphLifecycleOptions,
    DurableInvocationExecutor, DurableInvocationExecutorOptions, DurableTenantScheduler,
    DurableTenantSchedulerOptions, ExecutableGraphRegistry, ExecutableGraphRegistryBuilder,
    GraphBarrierLifecycleOutcome, GraphCancellationEvidence, GraphCancellationEvidenceContext,
    GraphDriveOutcome, GraphDriverError, GraphFailureEvidence, GraphFailureEvidenceContext,
    GraphLifecycleEvidenceError, GraphLifecycleEvidenceProvider, GraphNodeContext,
    GraphNodeExecution, GraphNodeExecutionError, GraphNodeExecutor, GraphTerminalEvidence,
    GraphTerminalEvidenceContext, InvocationAttemptEventIds, InvocationBudgetContext,
    InvocationBudgetProvider, InvocationBudgetProviderError, InvocationClock, InvocationClockError,
    InvocationClockObservation, JsonSchemaRegistry, JsonSchemaRegistryBuilder,
    JsonSchemaRegistryLimits, ModelAttemptExecutionError, ModelAttemptHandoff, ModelAttemptOutcome,
    ModelAttemptTerminalKind, ModelEventSink, ModelEventSinkError, ModelProviderRegistryBuilder,
    ProviderNativeAgentGraph, ProviderNativeAgentLifecycleEvidence, TenantFairnessWeight,
    TenantSchedulerOutcome, ToolAttemptHandoff, ToolAttemptOutcome, ToolAttemptTerminalKind,
    ToolProviderRegistryBuilder, ToolReconciliationCommitFailure, ToolReconciliationHandoff,
    ToolReconciliationKind, ToolReconciliationOutcome, WeightedFairnessPolicy,
    register_standard_agent_admission_event_schema,
    register_standard_agent_cancellation_event_schema,
    register_standard_agent_service_control_event_schema,
    register_standard_graph_driver_event_schema, register_standard_graph_lifecycle_event_schema,
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

struct CancellingModel {
    descriptor: ModelDescriptor,
    store: PostgresStore,
    calls: Arc<AtomicUsize>,
}

impl Model for CancellingModel {
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
        Box::pin(async move {
            let run = store.load_run(&tenant_id, run_id).await.unwrap();
            let cancellation = RunCancellationRequest::new(
                Failure::new(
                    FailureId::generate(),
                    FailureCategory::Cancelled,
                    FailureCode::new("runtime.test.provider_cancel_race").unwrap(),
                    FailureOrigin::new("stateknot.runtime.integration").unwrap(),
                    FailureMessage::new("Cancellation raced with a model response.").unwrap(),
                    RetryAdvice::Never,
                )
                .unwrap(),
                run.lifecycle().changed_at(),
            )
            .unwrap();
            store
                .append_control_plane(
                    JournalAppend::new(
                        JournalExpectation::exact(run.journal_head().unwrap().clone()),
                        JournalEventIntent::control_plane(
                            tenant_id,
                            run_id,
                            EventId::generate(),
                            test_payload(),
                        )
                        .unwrap(),
                    )
                    .unwrap(),
                    RunProjection::transition(
                        run.lifecycle().revision(),
                        RunTransition::RequestCancellation {
                            request: cancellation,
                        },
                    ),
                )
                .await
                .unwrap();
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

struct ProviderNativeScriptedModel {
    descriptor: ModelDescriptor,
    tool: ToolDescriptor,
    output_schema: SchemaReference,
    calls: Arc<AtomicUsize>,
    transcript_lengths: Arc<tokio::sync::Mutex<Vec<usize>>>,
    failed_outcomes: Arc<AtomicUsize>,
}

impl Model for ProviderNativeScriptedModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxFuture<'_, Result<ModelResponse, ModelError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let transcript_len = request.transcript().len();
        let descriptor = self.descriptor.clone();
        let tool = self.tool.clone();
        let output_schema = self.output_schema.clone();
        let transcript_lengths = Arc::clone(&self.transcript_lengths);
        let failed_outcomes = Arc::clone(&self.failed_outcomes);
        Box::pin(async move {
            transcript_lengths.lock().await.push(transcript_len);
            let provenance = ModelResponseProvenance::new(
                context.attempt_id(),
                descriptor.metadata().identity().clone(),
                None,
                None,
            );
            let usage = ModelUsage::new(
                TokenCount::new(12),
                Some(TokenCount::new(2)),
                TokenCount::new(4),
                Some(TokenCount::new(1)),
            )
            .unwrap();
            if transcript_len == 0 {
                let proposal = ModelToolCallProposal::new(
                    tool.metadata().identity().clone(),
                    Some(ModelProviderToolCallId::new("call_stateknot_lookup_01").unwrap()),
                    BoundedJson::try_from_value(json!({"query": "durable agents"})).unwrap(),
                    Extensions::default(),
                )
                .unwrap();
                let response = ModelResponse::new(
                    provenance,
                    &descriptor,
                    &request,
                    [ModelOutputItem::tool_call(proposal)],
                    ModelFinishReason::ToolCalls,
                    usage,
                    Extensions::default(),
                )
                .unwrap();
                let replay = ModelProviderReplay::new(
                    ModelProviderReplayFormat::new("stateknot.test.v1").unwrap(),
                    BoundedJson::try_from_value(json!([{
                        "type": "function_call",
                        "call_id": "call_stateknot_lookup_01"
                    }]))
                    .unwrap(),
                )
                .unwrap();
                Ok(response.with_provider_replay(replay).unwrap())
            } else {
                assert_eq!(transcript_len, 1, "only one prior tool turn is expected");
                assert_eq!(request.transcript().as_slice()[0].outcomes().len(), 1);
                if request.transcript().as_slice()[0].outcomes()[0]
                    .error()
                    .is_some()
                {
                    failed_outcomes.fetch_add(1, Ordering::SeqCst);
                }
                let json_content = ContentPart::Json(JsonContent::new(
                    BoundedJson::try_from_value(json!({
                        "answer": "StateKnot resumed from its durable invocation ledger."
                    }))
                    .unwrap(),
                    Some(output_schema),
                    ContentMetadata::untrusted(
                        ContentSource::Model,
                        "internal/provider-native-test"
                            .parse::<SecurityLabel>()
                            .unwrap(),
                    ),
                ));
                let output = ModelOutputItem::content(json_content).unwrap();
                Ok(ModelResponse::new(
                    provenance,
                    &descriptor,
                    &request,
                    [output],
                    ModelFinishReason::Completed,
                    usage,
                    Extensions::default(),
                )
                .unwrap())
            }
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

struct ProviderNativeLookupTool {
    descriptor: ToolDescriptor,
    calls: Arc<AtomicUsize>,
    fail: bool,
}

impl ErasedTool for ProviderNativeLookupTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn call(
        &self,
        context: ToolContext,
        _: ToolInput,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            let failure = Failure::new(
                FailureId::generate(),
                FailureCategory::DependencyUnavailable,
                FailureCode::new("runtime.test.lookup_unavailable").unwrap(),
                FailureOrigin::new("stateknot.runtime.integration").unwrap(),
                FailureMessage::new("The test lookup dependency is unavailable.").unwrap(),
                RetryAdvice::Never,
            )
            .unwrap();
            let error = ToolError::new(
                failure,
                ToolErrorPhase::Execution,
                ToolExternalEffect::NotApplicable,
                ToolErrorProvenance::for_invocation(&context, &self.descriptor),
            )
            .unwrap();
            return Box::pin(async move { Err(error) });
        }
        let result = ToolResult::for_invocation(
            &context,
            &self.descriptor,
            BoundedJson::try_from_value(json!({"matches": 1})).unwrap(),
            ToolArtifacts::empty(),
        );
        Box::pin(async move { Ok(result) })
    }
}

struct FailOnceAgentToolPolicy {
    reference: AgentToolPolicyReference,
    calls: Arc<AtomicUsize>,
    fail_once: bool,
    pause_first: Option<Arc<PolicyPause>>,
}

struct PolicyPause {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl AgentToolPolicy for FailOnceAgentToolPolicy {
    fn reference(&self) -> &AgentToolPolicyReference {
        &self.reference
    }

    fn evaluate(
        &self,
        context: AgentToolPolicyContext,
    ) -> BoxFuture<'_, Result<AgentToolPolicyDecision, AgentToolPolicyError>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let fail_once = self.fail_once;
        let pause_first = self.pause_first.clone();
        Box::pin(async move {
            if call == 0 {
                if let Some(pause) = pause_first {
                    pause.entered.notify_one();
                    pause.release.notified().await;
                }
            }
            if fail_once && call == 0 {
                Err(AgentToolPolicyError::TemporarilyUnavailable)
            } else {
                Ok(AgentToolPolicyDecision::Allow {
                    evidence_digest: context.action_digest(),
                })
            }
        })
    }
}

struct KnownFreeInvocationAccounting {
    reference: AgentInvocationAccountingReference,
}

impl AgentInvocationAccounting for KnownFreeInvocationAccounting {
    fn reference(&self) -> &AgentInvocationAccountingReference {
        &self.reference
    }

    fn model_charge(&self, _: &stateknot_core::ModelInvocation) -> AgentInvocationCharge {
        AgentInvocationCharge::Known(KnownCosts::empty())
    }

    fn tool_charge(&self, _: &stateknot_core::ToolInvocation) -> AgentInvocationCharge {
        AgentInvocationCharge::Known(KnownCosts::empty())
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

struct ProviderNativeFixture {
    definition: ProviderNativeAgentGraph,
    registry: ExecutableGraphRegistry,
    model_calls: Arc<AtomicUsize>,
    tool_calls: Arc<AtomicUsize>,
    policy_calls: Arc<AtomicUsize>,
    transcript_lengths: Arc<tokio::sync::Mutex<Vec<usize>>>,
    failed_outcomes: Arc<AtomicUsize>,
    input_schema: SchemaReference,
    policy_pause: Option<Arc<PolicyPause>>,
}

struct StaticAgentServiceAuthorizer {
    submission: AgentServiceSubmissionGrant,
    run: AgentServiceRunGrant,
    deny_submission: bool,
    deny_run_access: bool,
    submission_calls: Arc<AtomicUsize>,
    run_calls: Arc<AtomicUsize>,
}

impl AgentServiceAuthorizer for StaticAgentServiceAuthorizer {
    fn authorize_submission(
        &self,
        _: AgentServiceSubmissionAuthorization,
    ) -> BoxFuture<'_, Result<AgentServiceSubmissionGrant, AgentServiceAuthorizationError>> {
        self.submission_calls.fetch_add(1, Ordering::SeqCst);
        let denied = self.deny_submission;
        let grant = self.submission.clone();
        Box::pin(async move {
            if denied {
                Err(AgentServiceAuthorizationError::Denied)
            } else {
                Ok(grant)
            }
        })
    }

    fn authorize_run(
        &self,
        _: AgentServiceRunAuthorization,
    ) -> BoxFuture<'_, Result<AgentServiceRunGrant, AgentServiceAuthorizationError>> {
        self.run_calls.fetch_add(1, Ordering::SeqCst);
        let denied = self.deny_run_access;
        let grant = self.run.clone();
        Box::pin(async move {
            if denied {
                Err(AgentServiceAuthorizationError::Denied)
            } else {
                Ok(grant)
            }
        })
    }
}

fn provider_native_fixture(store: PostgresStore) -> ProviderNativeFixture {
    provider_native_fixture_with(store, true, false, None)
}

fn provider_native_failed_tool_fixture(store: PostgresStore) -> ProviderNativeFixture {
    provider_native_fixture_with(store, false, true, None)
}

fn provider_native_stale_race_fixture(store: PostgresStore) -> ProviderNativeFixture {
    provider_native_fixture_with(
        store,
        false,
        false,
        Some(Arc::new(PolicyPause {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        })),
    )
}

#[allow(clippy::too_many_lines)]
fn provider_native_fixture_with(
    store: PostgresStore,
    fail_policy_once: bool,
    fail_tool: bool,
    policy_pause: Option<Arc<PolicyPause>>,
) -> ProviderNativeFixture {
    let (input_schema, input_document) = schema("provider-native-input");
    let (output_schema, output_document) = schema("provider-native-output");
    let (tool_input_schema, tool_input_document) = schema("provider-native-tool-input");
    let (tool_output_schema, tool_output_document) = schema("provider-native-tool-output");
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-v1.json"
    ))
    .unwrap();
    let template =
        serde_json::from_value::<AgentDescriptor>(fixture["descriptors"]["valid"][0].clone())
            .unwrap();
    let tool_template = template.tools().iter().next().unwrap().clone();
    let tool = ToolDescriptor::new(
        tool_template.metadata().clone(),
        tool_input_schema.clone(),
        tool_output_schema.clone(),
        tool_template.semantics().clone(),
        tool_template.resources().clone(),
        tool_template.invocation().clone(),
        tool_template.limits().clone(),
    )
    .unwrap();
    let execution = AgentExecutionConfig::new(
        AgentStructuredOutputStrategy::ModelNative,
        ExecutionCount::new(3),
        ExecutionCount::ZERO,
        ExecutionCount::new(1),
        AgentToolConcurrency::sequential(),
    )
    .unwrap();
    let descriptor = AgentDescriptor::new(
        template.metadata().clone(),
        input_schema.clone(),
        output_schema.clone(),
        template.model().clone(),
        template.instructions().clone(),
        AgentTools::try_new([tool.clone()]).unwrap(),
        execution,
        template.budget_limits().clone(),
    )
    .unwrap();
    let model_calls = Arc::new(AtomicUsize::new(0));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = Arc::new(AtomicUsize::new(0));
    let transcript_lengths = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let failed_outcomes = Arc::new(AtomicUsize::new(0));
    let policy = Arc::new(FailOnceAgentToolPolicy {
        reference: AgentToolPolicyReference::new(
            capability("provider-native-tool-policy"),
            Digest::sha256(b"provider-native integration policy v1"),
        ),
        calls: Arc::clone(&policy_calls),
        fail_once: fail_policy_once,
        pause_first: policy_pause.clone(),
    });
    let definition = ProviderNativeAgentGraph::compile(
        descriptor,
        capability("provider-native-agent-graph"),
        capability("provider-native-agent-reducer"),
        "https://stknot.com/schemas/tests/provider-native-state/1.0.0"
            .parse::<SchemaId>()
            .unwrap(),
        "internal/provider-native-input"
            .parse::<SecurityLabel>()
            .unwrap(),
        policy,
        Arc::new(KnownFreeInvocationAccounting {
            reference: AgentInvocationAccountingReference::new(
                capability("provider-native-invocation-accounting"),
                Digest::sha256(b"provider-native known-free accounting v1"),
            ),
        }),
    )
    .unwrap();

    let mut schema_builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    for (reference, document) in [
        (input_schema.clone(), input_document),
        (output_schema.clone(), output_document),
        (tool_input_schema, tool_input_document),
        (tool_output_schema, tool_output_document),
    ] {
        schema_builder.register(reference, document).unwrap();
    }
    definition.register_schema(&mut schema_builder).unwrap();
    register_standard_graph_driver_event_schema(&mut schema_builder).unwrap();
    register_standard_graph_lifecycle_event_schema(&mut schema_builder).unwrap();
    register_standard_agent_cancellation_event_schema(&mut schema_builder).unwrap();
    register_standard_agent_service_control_event_schema(&mut schema_builder).unwrap();
    register_standard_agent_admission_event_schema(&mut schema_builder).unwrap();
    register_standard_invocation_execution_event_schema(&mut schema_builder).unwrap();
    let schemas = schema_builder.build().unwrap();

    let mut models = ModelProviderRegistryBuilder::new();
    models
        .register(Arc::new(ProviderNativeScriptedModel {
            descriptor: definition.descriptor().model().clone(),
            tool: tool.clone(),
            output_schema,
            calls: Arc::clone(&model_calls),
            transcript_lengths: Arc::clone(&transcript_lengths),
            failed_outcomes: Arc::clone(&failed_outcomes),
        }))
        .unwrap();
    let mut tools = ToolProviderRegistryBuilder::new();
    tools
        .register(Arc::new(ProviderNativeLookupTool {
            descriptor: tool,
            calls: Arc::clone(&tool_calls),
            fail: fail_tool,
        }))
        .unwrap();
    let invocation_executor = DurableInvocationExecutor::with_clock(
        store.clone(),
        schemas.clone(),
        models.build(),
        tools.build(),
        Arc::new(StaticInvocationBudget {
            resolved: invocation_budget(),
        }),
        Arc::new(FixedInvocationClock {
            observed_at: "2029-01-01T00:00:00.000000Z".parse().unwrap(),
        }),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let mut registry_builder = ExecutableGraphRegistryBuilder::new(schemas.clone());
    definition
        .register_executable(&mut registry_builder, store, invocation_executor, schemas)
        .unwrap();
    ProviderNativeFixture {
        definition,
        registry: registry_builder.build().unwrap(),
        model_calls,
        tool_calls,
        policy_calls,
        transcript_lengths,
        failed_outcomes,
        input_schema,
        policy_pause,
    }
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

    fn cancellation_evidence(
        &self,
        _: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        let evidence = GraphCancellationEvidence::new(self.terminal.usage().clone());
        Box::pin(async move { Ok(evidence) })
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

    fn cancellation_evidence(
        &self,
        _: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
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
    register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
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
    register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
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
    register_standard_agent_cancellation_event_schema(&mut schemas).unwrap();
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

fn invocation_schema_registry_with(
    reference: SchemaReference,
    document: Value,
) -> JsonSchemaRegistry {
    let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
    register_standard_invocation_execution_event_schema(&mut builder).unwrap();
    builder.register(reference, document).unwrap();
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

fn provider_native_admission_request(
    fixture: &ProviderNativeFixture,
    tenant_id: TenantId,
    ids: AgentRunIds,
) -> DurableAgentAdmissionRequest {
    let descriptor = fixture.definition.descriptor().clone();
    let request = AgentRequest::new(
        fixture.input_schema.clone(),
        BoundedJson::try_from_value(json!({
            "question": "Can this run continue without repeating provider I/O?"
        }))
        .unwrap(),
        BudgetLimits::empty(),
    );
    let policy = capability("provider-native-admission-policy");
    let evidence = JournalPayload::new(
        fixture.input_schema.clone(),
        JournalEventKind::new(AgentAdmissionAuthority::EVIDENCE_KIND).unwrap(),
        BoundedJson::try_from_value(json!({"decision": "allow"})).unwrap(),
    )
    .unwrap();
    let authority = AgentAdmissionAuthority::new(
        policy.owner().clone(),
        descriptor.metadata().required_scopes().clone(),
        policy,
        Digest::sha256(b"provider-native admission policy v1"),
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
        capability("provider-native-admission-budget"),
        authority.evidence().digest(),
        limits,
    )
    .unwrap();
    DurableAgentAdmissionRequest::new(
        tenant_id,
        ids,
        descriptor,
        request,
        [layer],
        fixture.definition.graph().reference(),
        authority,
        fixture.definition.initial_state().unwrap(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn agent_service_authorizes_submits_recovers_and_cancels_without_redispatch() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = provider_native_fixture(store.clone());
    let tenant_id = tenant("runtime-agent-service-v1");
    store
        .register_graph_definition(tenant_id.clone(), fixture.definition.graph().clone())
        .await
        .unwrap();

    let descriptor = fixture.definition.descriptor().clone();
    let admission_policy = capability("agent-service-admission-policy");
    let principal = admission_policy.owner().clone();
    let evidence = JournalPayload::new(
        fixture.input_schema.clone(),
        JournalEventKind::new(AgentAdmissionAuthority::EVIDENCE_KIND).unwrap(),
        BoundedJson::try_from_value(json!({"decision": "allow"})).unwrap(),
    )
    .unwrap();
    let authority = AgentAdmissionAuthority::new(
        principal.clone(),
        descriptor.metadata().required_scopes().clone(),
        admission_policy,
        Digest::sha256(b"agent service admission policy v1"),
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
    let budget_layer = AgentAdmissionBudgetLayer::new(
        capability("agent-service-admission-budget"),
        authority.evidence().digest(),
        limits,
    )
    .unwrap();
    let run_policy = capability("agent-service-run-policy");
    let run_grant = AgentServiceRunGrant::new(
        principal.clone(),
        run_policy,
        Digest::sha256(b"agent service run policy v1"),
        Digest::sha256(b"agent service run decision allow"),
    );
    let submission_grant = AgentServiceSubmissionGrant::new(authority, vec![budget_layer]);
    let submission_calls = Arc::new(AtomicUsize::new(0));
    let run_calls = Arc::new(AtomicUsize::new(0));
    let authorizer = Arc::new(StaticAgentServiceAuthorizer {
        submission: submission_grant.clone(),
        run: run_grant.clone(),
        deny_submission: false,
        deny_run_access: false,
        submission_calls: Arc::clone(&submission_calls),
        run_calls: Arc::clone(&run_calls),
    });
    let mut deployments = AgentServiceRegistryBuilder::new();
    deployments
        .register(Arc::new(fixture.definition.clone()))
        .unwrap();
    let deployments = deployments.build();
    let service = AgentServiceV1::new(
        store.clone(),
        fixture.registry.clone(),
        deployments.clone(),
        authorizer,
    )
    .unwrap();
    let caller = AgentServiceCaller::new(tenant_id.clone(), principal.clone());
    let key = AgentSubmissionKey::new("request_runtime_agent_service_v1_01").unwrap();
    let request = AgentRequest::new(
        fixture.input_schema.clone(),
        BoundedJson::try_from_value(json!({"question": "Is this durable?"})).unwrap(),
        BudgetLimits::empty(),
    );
    let agent = descriptor.metadata().identity().clone();

    let admitted = service
        .submit(caller.clone(), &key, &agent, request.clone())
        .await
        .unwrap();
    assert!(matches!(admitted, AgentRunAdmissionOutcome::Committed(_)));
    let run_id = admitted.snapshot().provenance().run_id();

    // A retry regenerates provider-native initial IDs, but the service first
    // compares the durable logical request and resolves the original run.
    let retry = service
        .submit(caller.clone(), &key, &agent, request.clone())
        .await
        .unwrap();
    assert!(matches!(retry, AgentRunAdmissionOutcome::Idempotent(_)));
    assert_eq!(retry.snapshot().provenance().run_id(), run_id);
    assert_eq!(
        service
            .load_by_key(caller.clone(), &key)
            .await
            .unwrap()
            .provenance()
            .run_id(),
        run_id
    );

    let ids = AgentCancellationIds::generate();
    let cancelled = service
        .request_cancellation(caller.clone(), run_id, ids)
        .await
        .unwrap();
    assert!(matches!(cancelled, AgentCancellationOutcome::Committed(_)));
    assert_eq!(
        cancelled.snapshot().status(),
        RunStatus::CancellationRequested
    );
    let retry = service
        .request_cancellation(caller.clone(), run_id, ids)
        .await
        .unwrap();
    assert!(matches!(retry, AgentCancellationOutcome::Idempotent(_)));
    assert_eq!(retry.snapshot().status(), RunStatus::CancellationRequested);
    let stored = store
        .load_agent_admission(&tenant_id, run_id)
        .await
        .unwrap();
    let cancellation = stored.run().lifecycle().cancellation_request().unwrap();
    assert_eq!(cancellation.failure().id(), ids.failure_id());
    assert_eq!(
        cancellation.failure().caused_by_event_id(),
        Some(ids.event_id())
    );
    assert!(cancellation.requested_at() >= admitted.snapshot().changed_at());
    assert!(cancellation.requested_at() <= stored.run().journal_head().unwrap().recorded_at());
    assert!(matches!(
        service
            .request_cancellation(
                caller.clone(),
                run_id,
                AgentCancellationIds::new(EventId::generate(), ids.failure_id()),
            )
            .await,
        Err(AgentServiceError::ConflictingCancellation)
    ));
    assert!(matches!(
        service
            .request_cancellation(caller.clone(), run_id, AgentCancellationIds::generate())
            .await,
        Err(AgentServiceError::ConflictingCancellation)
    ));

    let denied = AgentServiceV1::new(
        store.clone(),
        fixture.registry,
        deployments,
        Arc::new(StaticAgentServiceAuthorizer {
            // The denial proof still carries a structurally valid grant, but
            // policy rejects before either run or deployment lookup.
            submission: submission_grant,
            run: run_grant,
            deny_submission: true,
            deny_run_access: true,
            submission_calls: Arc::clone(&submission_calls),
            run_calls: Arc::clone(&run_calls),
        }),
    )
    .unwrap();
    let missing_agent = capability("agent-service-missing-deployment");
    let missing_key = AgentSubmissionKey::new("request_runtime_agent_service_v1_denied").unwrap();
    assert!(matches!(
        denied
            .submit(caller.clone(), &missing_key, &missing_agent, request,)
            .await,
        Err(AgentServiceError::Authorization(
            AgentServiceAuthorizationError::Denied
        ))
    ));
    assert!(matches!(
        denied.load(caller, RunId::generate()).await,
        Err(AgentServiceError::Authorization(
            AgentServiceAuthorizationError::Denied
        ))
    ));
    assert!(submission_calls.load(Ordering::SeqCst) >= 3);
    assert!(run_calls.load(Ordering::SeqCst) >= 5);
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 0);
    store.close().await;
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
async fn provider_native_graph_recovers_committed_model_without_redispatch_and_completes_lifecycle()
{
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = provider_native_fixture(store.clone());
    let tenant_id = tenant("runtime-provider-native-agent");
    let ids = AgentRunIds::generate();
    let run_id = ids.run_id();
    assert!(matches!(
        store
            .register_graph_definition(tenant_id.clone(), fixture.definition.graph().clone())
            .await
            .unwrap(),
        GraphDefinitionRegistrationOutcome::Registered(_)
            | GraphDefinitionRegistrationOutcome::Idempotent(_)
    ));
    let admission = DurableAgentAdmission::new(store.clone(), fixture.registry.clone()).unwrap();
    admission
        .admit(provider_native_admission_request(
            &fixture,
            tenant_id.clone(),
            ids,
        ))
        .await
        .unwrap();
    let driver = DurableGraphDriver::new(
        store.clone(),
        fixture.registry.clone(),
        DurableGraphDriverOptions::default(),
    )
    .unwrap();

    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let first = driver
        .drive(first_lease.fence().clone(), CancellationSignal::never())
        .await
        .unwrap();
    assert!(
        matches!(first.outcome(), GraphDriveOutcome::Deferred { .. }),
        "unexpected first drive outcome: {:?}",
        first.outcome()
    );
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.policy_calls.load(Ordering::SeqCst), 1);

    // The first policy evaluation failed only after the model response was
    // durably committed. A later physical node attempt must consume that exact
    // ledger response and must not call the model again for the same turn.
    tokio::time::sleep(Duration::from_millis(250)).await;
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
    let (outcome, report) = second.into_parts();
    let handoff = match outcome {
        GraphDriveOutcome::LifecycleBarrierReady(handoff) => handoff,
        other => {
            panic!("provider-native graph must reach its terminal lifecycle barrier: {other:?}")
        }
    };
    assert!(report.node_attempts_completed() >= 3);
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.policy_calls.load(Ordering::SeqCst), 2);
    assert_eq!(*fixture.transcript_lengths.lock().await, vec![0, 1]);

    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(ProviderNativeAgentLifecycleEvidence::new(
            fixture.definition,
            store.clone(),
        )),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let committed = lifecycle.commit_barrier(*handoff).await.unwrap();
    assert!(matches!(
        committed,
        GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Succeeded);
    assert!(run.lease().is_none());
    let result = run.lifecycle().result().unwrap();
    assert_eq!(
        result.output().as_value(),
        &json!({"answer": "StateKnot resumed from its durable invocation ledger."})
    );
    assert_eq!(result.usage().model_attempts(), ExecutionCount::new(2));
    assert_eq!(result.usage().model_turns(), ExecutionCount::new(2));
    assert_eq!(result.usage().tool_calls(), ExecutionCount::new(1));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn provider_native_higher_fence_wins_stale_policy_race_without_external_redispatch() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = provider_native_stale_race_fixture(store.clone());
    let pause = fixture.policy_pause.clone().unwrap();
    let tenant_id = tenant("runtime-provider-native-stale-race");
    let ids = AgentRunIds::generate();
    let run_id = ids.run_id();
    store
        .register_graph_definition(tenant_id.clone(), fixture.definition.graph().clone())
        .await
        .unwrap();
    DurableAgentAdmission::new(store.clone(), fixture.registry.clone())
        .unwrap()
        .admit(provider_native_admission_request(
            &fixture,
            tenant_id.clone(),
            ids,
        ))
        .await
        .unwrap();
    let first_lease = store
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
    let first_driver = driver.clone();
    let first_fence = first_lease.fence().clone();
    let first = tokio::spawn(async move {
        first_driver
            .drive(first_fence, CancellationSignal::never())
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), pause.entered.notified())
        .await
        .expect("the old worker must pause after committing its first model response");
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 0);

    let successor = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let recovered = driver
        .drive(successor.fence().clone(), CancellationSignal::never())
        .await
        .expect("the higher fence must recover the committed model response");
    let handoff = match recovered.into_parts().0 {
        GraphDriveOutcome::LifecycleBarrierReady(handoff) => handoff,
        other => panic!("the higher fence must finish the provider-native graph: {other:?}"),
    };
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(*fixture.transcript_lengths.lock().await, vec![0, 1]);

    pause.release.notify_one();
    let stale = tokio::time::timeout(Duration::from_secs(10), first)
        .await
        .expect("the stale worker must stop after its policy call is released")
        .expect("the stale worker task must not panic");
    assert!(
        stale.is_err(),
        "the old fence must not commit a competing node result"
    );
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.policy_calls.load(Ordering::SeqCst), 2);

    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(ProviderNativeAgentLifecycleEvidence::new(
            fixture.definition,
            store.clone(),
        )),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        lifecycle.commit_barrier(*handoff).await.unwrap(),
        GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));
    assert_eq!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::Succeeded
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn provider_native_cancellation_recovers_exact_usage_and_replays_confirmation() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = provider_native_stale_race_fixture(store.clone());
    let pause = fixture.policy_pause.clone().unwrap();
    let tenant_id = tenant("runtime-provider-native-cancellation");
    let ids = AgentRunIds::generate();
    let run_id = ids.run_id();
    store
        .register_graph_definition(tenant_id.clone(), fixture.definition.graph().clone())
        .await
        .unwrap();
    DurableAgentAdmission::new(store.clone(), fixture.registry.clone())
        .unwrap()
        .admit(provider_native_admission_request(
            &fixture,
            tenant_id.clone(),
            ids,
        ))
        .await
        .unwrap();
    let facade = DurableAgentRuns::new(store.clone(), fixture.registry.clone()).unwrap();
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let driver_options = DurableGraphDriverOptions::default()
        .with_cancellation_timing(Duration::from_millis(25), Duration::from_millis(100))
        .unwrap();
    let driver =
        DurableGraphDriver::new(store.clone(), fixture.registry.clone(), driver_options).unwrap();
    let driver_task = tokio::spawn({
        let driver = driver.clone();
        let fence = lease.fence().clone();
        async move { driver.drive(fence, CancellationSignal::never()).await }
    });

    tokio::time::timeout(Duration::from_secs(10), pause.entered.notified())
        .await
        .expect("policy must pause after the first model response is durable");
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 0);
    let live = store.load_run(&tenant_id, run_id).await.unwrap();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Cancelled,
        FailureCode::new("runtime.provider_native.cancelled").unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new("Provider-native execution was cancelled safely.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap();
    let failure_id = failure.id();
    let request = RunCancellationRequest::new(failure, live.lifecycle().changed_at()).unwrap();
    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(live.journal_head().unwrap().clone()),
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
                live.lifecycle().revision(),
                RunTransition::RequestCancellation { request },
            ),
        )
        .await
        .unwrap();

    let drive_result = tokio::time::timeout(Duration::from_secs(10), driver_task)
        .await
        .expect("driver must observe durable cancellation within its bounded cadence")
        .expect("driver task must not panic")
        .unwrap();
    let handoff = match drive_result.into_parts().0 {
        GraphDriveOutcome::CancellationRequested(handoff) => handoff,
        other => panic!("durable cancellation must reach lifecycle handoff: {other:?}"),
    };
    assert_eq!(handoff.failure_id(), failure_id);
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.policy_calls.load(Ordering::SeqCst), 1);

    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(ProviderNativeAgentLifecycleEvidence::new(
            fixture.definition,
            store.clone(),
        )),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    let committed = lifecycle
        .confirm_cancellation((*handoff).clone())
        .await
        .unwrap();
    let committed_event_id = match committed {
        GraphBarrierLifecycleOutcome::Cancelled(
            stateknot_store_postgres::AppendOutcome::Committed(event),
        ) => {
            assert_eq!(
                event.payload().schema().id().as_str(),
                "https://stknot.com/schemas/runtime/agent-cancellation-event/1.0.0"
            );
            event.event_id()
        }
        other => panic!("first cancellation confirmation must commit: {other:?}"),
    };
    assert!(matches!(
        lifecycle.confirm_cancellation(*handoff).await.unwrap(),
        GraphBarrierLifecycleOutcome::Cancelled(
            stateknot_store_postgres::AppendOutcome::Idempotent(_)
        )
    ));

    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Cancelled);
    assert!(run.lease().is_none());
    assert_eq!(run.journal_head().unwrap().event_id(), committed_event_id);
    let usage = run.lifecycle().terminal_usage().unwrap();
    assert_eq!(usage.model_attempts(), ExecutionCount::new(1));
    assert_eq!(usage.model_turns(), ExecutionCount::new(1));
    assert_eq!(usage.tool_calls(), ExecutionCount::ZERO);
    let public = facade.load(&tenant_id, run_id).await.unwrap();
    assert!(matches!(
        public.outcome(),
        Some(AgentRunTerminalOutcome::Cancelled { failure, usage, .. })
            if failure.id() == failure_id
                && usage.model_attempts() == ExecutionCount::new(1)
                && usage.tool_calls() == ExecutionCount::ZERO
    ));
    pause.release.notify_one();
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn agent_loop_confirms_preexecution_cancellation_without_provider_dispatch() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = provider_native_fixture(store.clone());
    let tenant_id = tenant("runtime-agent-loop-preexecution-cancellation");
    let ids = AgentRunIds::generate();
    let run_id = ids.run_id();
    store
        .register_graph_definition(tenant_id.clone(), fixture.definition.graph().clone())
        .await
        .unwrap();
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.registry.clone())
        .unwrap()
        .admit(provider_native_admission_request(
            &fixture,
            tenant_id.clone(),
            ids,
        ))
        .await
        .unwrap();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Cancelled,
        FailureCode::new("runtime.provider_native.preexecution_cancelled").unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new("Provider-native execution was cancelled before dispatch.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap();
    let request =
        RunCancellationRequest::new(failure, admitted.stored().run().lifecycle().changed_at())
            .unwrap();
    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(admitted.stored().event().head()),
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
                admitted.stored().run().lifecycle().revision(),
                RunTransition::RequestCancellation { request },
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
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        fixture.registry,
        Arc::new(ProviderNativeAgentLifecycleEvidence::new(
            fixture.definition,
            store.clone(),
        )),
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
        AgentLoopOutcome::CancellationConfirmed(
            stateknot_store_postgres::AppendOutcome::Committed(_)
        )
    ));
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 0);
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(run.lifecycle().terminal_usage(), Some(&BudgetUsage::zero()));
    assert!(run.lease().is_none());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn cancellation_evidence_unavailability_fails_closed_without_zero_usage() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = provider_native_fixture(store.clone());
    let tenant_id = tenant("runtime-cancellation-evidence-unavailable");
    let ids = AgentRunIds::generate();
    let run_id = ids.run_id();
    store
        .register_graph_definition(tenant_id.clone(), fixture.definition.graph().clone())
        .await
        .unwrap();
    let admitted = DurableAgentAdmission::new(store.clone(), fixture.registry.clone())
        .unwrap()
        .admit(provider_native_admission_request(
            &fixture,
            tenant_id.clone(),
            ids,
        ))
        .await
        .unwrap();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Cancelled,
        FailureCode::new("runtime.cancellation.evidence_unavailable").unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new("Cancellation evidence is temporarily unavailable.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap();
    let request =
        RunCancellationRequest::new(failure, admitted.stored().run().lifecycle().changed_at())
            .unwrap();
    store
        .append_control_plane(
            JournalAppend::new(
                JournalExpectation::exact(admitted.stored().event().head()),
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
                admitted.stored().run().lifecycle().revision(),
                RunTransition::RequestCancellation { request },
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
    let agent_loop = DurableAgentLoop::new(
        store.clone(),
        fixture.registry,
        Arc::new(UnavailableLifecycleEvidence),
        DurableGraphDriverOptions::default(),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        agent_loop
            .run(lease.fence().clone(), CancellationSignal::never())
            .await,
        Err(AgentLoopError::Lifecycle { .. })
    ));
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 0);
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::CancellationRequested);
    assert!(run.lifecycle().terminal_usage().is_none());
    assert!(run.lease().is_none());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn provider_native_graph_continues_after_known_failed_tool_and_binds_terminal_revision() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = provider_native_failed_tool_fixture(store.clone());
    let tenant_id = tenant("runtime-provider-native-failed-tool");
    let ids = AgentRunIds::generate();
    let run_id = ids.run_id();
    store
        .register_graph_definition(tenant_id.clone(), fixture.definition.graph().clone())
        .await
        .unwrap();
    DurableAgentAdmission::new(store.clone(), fixture.registry.clone())
        .unwrap()
        .admit(provider_native_admission_request(
            &fixture,
            tenant_id.clone(),
            ids,
        ))
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
    let handoff = match drive_result.into_parts().0 {
        GraphDriveOutcome::LifecycleBarrierReady(handoff) => handoff,
        other => panic!("failed tool must remain a consumable transcript outcome: {other:?}"),
    };
    assert_eq!(fixture.model_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.policy_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.failed_outcomes.load(Ordering::SeqCst), 1);
    assert_eq!(*fixture.transcript_lengths.lock().await, vec![0, 1]);

    let lifecycle = DurableGraphLifecycle::new(
        store.clone(),
        fixture.registry,
        Arc::new(ProviderNativeAgentLifecycleEvidence::new(
            fixture.definition,
            store.clone(),
        )),
        DurableGraphLifecycleOptions::default(),
    )
    .unwrap();
    assert!(matches!(
        lifecycle.commit_barrier(*handoff).await.unwrap(),
        GraphBarrierLifecycleOutcome::Succeeded(BarrierCommitOutcome::Committed { .. })
    ));
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    let result = run.lifecycle().result().unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::Succeeded);
    assert_eq!(result.usage().model_attempts(), ExecutionCount::new(2));
    assert_eq!(result.usage().tool_calls(), ExecutionCount::new(1));
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
async fn model_executor_commits_real_terminal_evidence_after_cancellation_advances_journal() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let fixture = driver_fixture();
    let tenant_id = tenant("runtime-model-cancellation-race");
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
    let prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                checkpoint.event().head(),
                lease.fence().clone(),
            ),
            ModelInvocationIntent::new(
                invocation_activation(checkpoint.checkpoint()),
                invocation_id,
                descriptor.clone(),
                model_request(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut models = ModelProviderRegistryBuilder::new();
    models
        .register(Arc::new(CancellingModel {
            descriptor,
            store: store.clone(),
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
        None,
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
    assert_eq!(
        store
            .load_model_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .status(),
        ModelInvocationStatus::Committed
    );
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(run.lifecycle().status(), RunStatus::CancellationRequested);
    assert_eq!(run.journal_head().unwrap().sequence().get(), 5);
    assert!(matches!(
        executor.execute_model(handoff).await.unwrap(),
        ModelAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
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
async fn timed_out_tool_write_reconciles_without_redispatch_or_invalid_schema_commit() {
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
    let template = tool_descriptor();
    let (tool_input_schema, _) = schema("reconciliation-tool-input");
    let (tool_output_schema, tool_output_document) = schema("reconciliation-tool-output");
    let descriptor = ToolDescriptor::new(
        template.metadata().clone(),
        tool_input_schema,
        tool_output_schema.clone(),
        template.semantics().clone(),
        template.resources().clone(),
        template.invocation().clone(),
        template.limits().clone(),
    )
    .unwrap();
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
            descriptor: descriptor.clone(),
            calls: Arc::clone(&calls),
        }))
        .unwrap();
    let executor = DurableInvocationExecutor::with_clock(
        store.clone(),
        invocation_schema_registry_with(tool_output_schema, tool_output_document),
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
        executor.execute_tool(handoff.clone()).await.unwrap(),
        ToolAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let attempt_id = stored.attempt_id().unwrap();
    let invalid_result = ToolResult::new(
        ToolResultProvenance::new(
            invocation_id,
            attempt_id,
            descriptor.metadata().identity().clone(),
        ),
        descriptor.output_schema().clone(),
        BoundedJson::try_from_value(json!("not-an-object")).unwrap(),
        ToolArtifacts::empty(),
    );
    let invalid_reconciliation = ToolReconciliationHandoff::result(
        lease.fence().clone(),
        stored.clone(),
        EventId::generate(),
        invalid_result,
    )
    .unwrap();
    let invalid_error = executor
        .commit_tool_reconciliation(invalid_reconciliation)
        .await
        .unwrap_err();
    assert!(matches!(
        invalid_error.source_error(),
        ToolReconciliationCommitFailure::OutputSchema {
            source: GraphSchemaValidationError::Rejected,
        }
    ));
    assert_eq!(
        store
            .load_tool_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .status(),
        ToolInvocationStatus::Unknown
    );

    let result = ToolResult::new(
        ToolResultProvenance::new(
            invocation_id,
            attempt_id,
            descriptor.metadata().identity().clone(),
        ),
        descriptor.output_schema().clone(),
        BoundedJson::try_from_value(json!({"receipt": "confirmed"})).unwrap(),
        ToolArtifacts::empty(),
    );
    let reconciliation = ToolReconciliationHandoff::result(
        lease.fence().clone(),
        stored,
        EventId::generate(),
        result,
    )
    .unwrap();
    let committed = executor
        .commit_tool_reconciliation(reconciliation.clone())
        .await
        .unwrap();
    assert!(matches!(
        committed,
        ToolReconciliationOutcome::Committed { .. }
    ));
    assert_eq!(committed.kind(), ToolReconciliationKind::Result);
    assert_eq!(
        committed.invocation().status(),
        ToolInvocationStatus::Committed
    );
    assert_eq!(
        committed.event().payload().kind().as_str(),
        "tool-reconciliation-result-committed"
    );
    let repeated = executor
        .commit_tool_reconciliation(reconciliation)
        .await
        .unwrap();
    assert!(matches!(
        repeated,
        ToolReconciliationOutcome::Idempotent { .. }
    ));
    assert_eq!(repeated.event().event_id(), committed.event().event_id());

    assert!(matches!(
        executor.execute_tool(handoff).await.unwrap(),
        ToolAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let second_invocation_id = InvocationId::generate();
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    let second_prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                run.journal_head().unwrap().clone(),
                lease.fence().clone(),
            ),
            ToolInvocationIntent::new(
                invocation_activation(checkpoint.checkpoint()),
                second_invocation_id,
                descriptor.clone(),
                tool_input(&descriptor),
                descriptor.limits().clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let second_handoff = ToolAttemptHandoff::new(
        lease.fence().clone(),
        second_prepared.invocation().clone(),
        AttemptId::generate(),
        InvocationAttemptEventIds::generate(),
        CancellationSignal::never(),
        None,
    )
    .unwrap();
    assert!(matches!(
        executor.execute_tool(second_handoff.clone()).await.unwrap(),
        ToolAttemptOutcome::Dispatched {
            terminal: ToolAttemptTerminalKind::Error,
            ..
        }
    ));
    let second_unknown = store
        .load_tool_invocation(&tenant_id, run_id, second_invocation_id)
        .await
        .unwrap();
    assert_eq!(second_unknown.status(), ToolInvocationStatus::Unknown);
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let reconciled_failure = Failure::new(
        FailureId::generate(),
        FailureCategory::DependencyUnavailable,
        FailureCode::new("runtime.test.reconciled_not_applied").unwrap(),
        FailureOrigin::new("stateknot.runtime.integration").unwrap(),
        FailureMessage::new("Authoritative status proves the write was not applied.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap();
    let reconciled_error = ToolError::new(
        reconciled_failure,
        ToolErrorPhase::Execution,
        ToolExternalEffect::NotApplied,
        ToolErrorProvenance::new(
            second_invocation_id,
            second_unknown.attempt_id().unwrap(),
            descriptor.metadata().identity().clone(),
        ),
    )
    .unwrap();
    let error_reconciliation = ToolReconciliationHandoff::error(
        lease.fence().clone(),
        second_unknown,
        EventId::generate(),
        reconciled_error,
    )
    .unwrap();
    let failed = executor
        .commit_tool_reconciliation(error_reconciliation.clone())
        .await
        .unwrap();
    assert_eq!(failed.kind(), ToolReconciliationKind::Error);
    assert_eq!(failed.invocation().status(), ToolInvocationStatus::Failed);
    assert_eq!(
        failed.event().payload().kind().as_str(),
        "tool-reconciliation-error-committed"
    );
    assert!(
        executor
            .commit_tool_reconciliation(error_reconciliation)
            .await
            .unwrap()
            .is_idempotent()
    );
    assert!(matches!(
        executor.execute_tool(second_handoff).await.unwrap(),
        ToolAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
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
    // Enter the driver's below-half-life refresh path while retaining enough
    // scheduling margin for a loaded CI host to reach the first database read.
    tokio::time::sleep(Duration::from_millis(600)).await;

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
        .with_acquire_timeout(Duration::from_secs(30))
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
