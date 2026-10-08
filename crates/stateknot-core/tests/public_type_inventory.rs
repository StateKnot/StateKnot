// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Closed root-export inventory: compiler-checked Serde boundaries, typed wire
//! round trips, and current generated schema pins. These are current-source
//! vectors, not historical N-1/N-2 records or exhaustive variant/fuzz coverage.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use stateknot_core::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::LazyLock,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    schema: String,
    types: BTreeMap<String, TypeEntry>,
    non_types: BTreeMap<String, Export>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TypeEntry {
    module: String,
    kind: String,
    rust_type: String,
    mode: String,
    vectors: Vec<Vector>,
    schema_digest: Option<Digest>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Vector {
    fixture: String,
    pointer: String,
    canonical_digest: Digest,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Export {
    module: String,
    kind: String,
}

static INVENTORY: LazyLock<Inventory> = LazyLock::new(|| {
    let json = BoundedJson::from_str_with_limits(
        include_str!("fixtures/core-public-type-inventory-v1.json"),
        JsonLimits::MAXIMUM,
    )
    .unwrap();
    serde_json::from_value(json.into_value()).unwrap()
});
static DOCUMENTS: LazyLock<BTreeMap<String, Value>> = LazyLock::new(|| {
    let mut documents = BTreeMap::new();
    for entry in INVENTORY.types.values() {
        for vector in &entry.vectors {
            assert!(
                vector.fixture.starts_with("core-")
                    && Path::new(&vector.fixture).extension() == Some(std::ffi::OsStr::new("json"))
            );
            assert_eq!(Path::new(&vector.fixture).components().count(), 1);
            documents.entry(vector.fixture.clone()).or_insert_with(|| {
                let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures")
                    .join(&vector.fixture);
                let source = std::fs::read_to_string(path).unwrap();
                assert!(source.len() <= 512 * 1024);
                BoundedJson::from_str_with_limits(&source, JsonLimits::MAXIMUM)
                    .unwrap()
                    .into_value()
            });
        }
    }
    documents
});
fn canonical(value: Value) -> CanonicalJson {
    CanonicalJson::new(
        &BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM).unwrap(),
    )
    .unwrap()
}
fn schema_pin<T: JsonSchema>(name: &str) {
    let schema = serde_json::to_value(schemars::schema_for!(T)).unwrap();
    assert_eq!(
        Some(canonical(schema).digest()),
        INVENTORY.types[name].schema_digest,
        "{name} schema changed"
    );
}
fn verify_reader<T: DeserializeOwned + Serialize + JsonSchema>(name: &str) {
    let entry = &INVENTORY.types[name];
    assert_eq!(entry.mode, "read_write");
    assert!(!entry.vectors.is_empty(), "{name} has no positive wire");
    schema_pin::<T>(name);
    for vector in &entry.vectors {
        let wire = DOCUMENTS[&vector.fixture].pointer(&vector.pointer).unwrap();
        let typed: T = serde_json::from_value(wire.clone())
            .unwrap_or_else(|e| panic!("{name}: {}{}: {e}", vector.fixture, vector.pointer));
        assert_eq!(&serde_json::to_value(&typed).unwrap(), wire, "{name}");
        let expected = canonical(wire.clone());
        assert_eq!(
            expected.digest(),
            vector.canonical_digest,
            "{name} wire pin changed"
        );
        let restored: T = serde_json::from_slice(expected.as_bytes()).unwrap();
        assert_eq!(
            canonical(serde_json::to_value(restored).unwrap()),
            expected,
            "{name}"
        );
        if wire.is_string() || wire.is_array() {
            let mut invalid = vec![Value::Null, json!(true), json!(7), json!({})];
            invalid.push(if wire.is_string() {
                json!([])
            } else {
                json!("wire")
            });
            for value in invalid {
                assert!(
                    serde_json::from_value::<T>(value).is_err(),
                    "{name} accepted an incompatible wire shape"
                );
            }
        }
        // Raw JSON maps keep duplicate keys; Value alone would discard them.
        // The bounded JSON value and extension map have intentionally open keys.
        if let Some(object) = wire
            .as_object()
            .filter(|_| name != "BoundedJson" && name != "Extensions")
        {
            for invalid in [Value::Null, json!(true), json!(7), json!("wire")] {
                assert!(
                    serde_json::from_value::<T>(invalid).is_err(),
                    "{name} accepted a scalar in place of an object"
                );
            }
            let mut extra = object.clone();
            assert!(
                extra
                    .insert("untrusted_authority".into(), json!(true))
                    .is_none()
            );
            assert!(
                serde_json::from_value::<T>(Value::Object(extra)).is_err(),
                "{name} accepted an unknown field"
            );
            let encoded = serde_json::to_string(wire).unwrap();
            for (key, value) in object {
                let duplicate = format!(
                    "{{{}:{},{}",
                    serde_json::to_string(key).unwrap(),
                    serde_json::to_string(value).unwrap(),
                    &encoded[1..]
                );
                assert!(
                    serde_json::from_str::<T>(&duplicate).is_err(),
                    "{name} accepted duplicate {key}"
                );
            }
        }
    }
}

macro_rules! readers {
    ($( $test:ident: $ty:ty ),+ $(,)?) => {
        const READERS: &[&str] = &[$(stringify!($ty)),+];
        $(#[test] fn $test() { verify_reader::<$ty>(stringify!($ty)); })+
    };
}
// Two possible marker implementations make inference ambiguous if the bound
// exists. This fails compilation when a reviewed Rust-only type gains Serde.
macro_rules! cannot_implement {
    ($ty:ty : $bound:path) => {{
        trait AmbiguousIfImplemented<Marker> {
            fn check() {}
        }
        impl<T: ?Sized> AmbiguousIfImplemented<()> for T {}
        struct Implemented;
        impl<T: ?Sized + $bound> AmbiguousIfImplemented<Implemented> for T {}
        let _ = <$ty as AmbiguousIfImplemented<_>>::check;
    }};
}
macro_rules! rust_only {
    ($($ty:ty),+ $(,)?) => {
        const RUST_ONLY: &[&str] = &[$(stringify!($ty)),+];
        #[test]
        fn reviewed_rust_only_instantiations_have_no_owned_wire_impl() {
            $(
                cannot_implement!($ty: serde::Serialize);
                cannot_implement!($ty: serde::de::DeserializeOwned);
            )+
        }
    };
}

readers! {
    agent_admission: AgentAdmission,
    agent_admission_authority: AgentAdmissionAuthority,
    agent_admission_budget_layer: AgentAdmissionBudgetLayer,
    agent_admission_intent: AgentAdmissionIntent,
    agent_artifacts: AgentArtifacts,
    agent_descriptor: AgentDescriptor,
    agent_execution_config: AgentExecutionConfig,
    agent_instructions: AgentInstructions,
    agent_request: AgentRequest,
    agent_result: AgentResult,
    agent_result_provenance: AgentResultProvenance,
    agent_structured_output_strategy: AgentStructuredOutputStrategy,
    agent_submission_key: AgentSubmissionKey,
    agent_tool_concurrency: AgentToolConcurrency,
    agent_tools: AgentTools,
    artifact_description: ArtifactDescription,
    artifact_id: ArtifactId,
    artifact_identity: ArtifactIdentity,
    artifact_modality: ArtifactModality,
    artifact_name: ArtifactName,
    artifact_parents: ArtifactParents,
    artifact_presentation: ArtifactPresentation,
    artifact_provenance: ArtifactProvenance,
    artifact_ref: ArtifactRef,
    artifact_representation: ArtifactRepresentation,
    attempt_id: AttemptId,
    authorization_receipt_id: AuthorizationReceiptId,
    barrier_result_heads: BarrierResultHeads,
    bounded_json: BoundedJson,
    budget_dimension: BudgetDimension,
    budget_limits: BudgetLimits,
    budget_usage: BudgetUsage,
    byte_count: ByteCount,
    capability_description: CapabilityDescription,
    capability_identity: CapabilityIdentity,
    capability_kind: CapabilityKind,
    capability_lifecycle: CapabilityLifecycle,
    capability_metadata: CapabilityMetadata,
    capability_name: CapabilityName,
    capability_reference: CapabilityReference,
    capability_title: CapabilityTitle,
    checkpoint: Checkpoint,
    checkpoint_barrier: CheckpointBarrier,
    checkpoint_head: CheckpointHead,
    checkpoint_id: CheckpointId,
    checkpoint_state: CheckpointState,
    checkpoint_write: CheckpointWrite,
    child_agent_reference: ChildAgentReference,
    child_run_admission_intent: ChildRunAdmissionIntent,
    child_run_budget_account: ChildRunBudgetAccount,
    child_run_budget_entry: ChildRunBudgetEntry,
    child_run_budget_settlement: ChildRunBudgetSettlement,
    child_run_declaration: ChildRunDeclaration,
    child_run_join_binding: ChildRunJoinBinding,
    child_run_join_head: ChildRunJoinHead,
    child_run_join_request: ChildRunJoinRequest,
    child_run_key: ChildRunKey,
    child_run_slot: ChildRunSlot,
    child_run_topology_limits: ChildRunTopologyLimits,
    compiled_graph: CompiledGraph,
    content_metadata: ContentMetadata,
    content_part: ContentPart,
    content_source: ContentSource,
    content_trust: ContentTrust,
    cost_limits: CostLimits,
    cumulative_budget_reservation: CumulativeBudgetReservation,
    currency_code: CurrencyCode,
    delivery_fence: DeliveryFence,
    delivery_id: DeliveryId,
    destination_id: DestinationId,
    digest: Digest,
    durable_timer: DurableTimer,
    durable_timer_head: DurableTimerHead,
    durable_timer_record: DurableTimerRecord,
    durable_wait: DurableWait,
    duration_millis: DurationMillis,
    event_id: EventId,
    execution_count: ExecutionCount,
    extension_key: ExtensionKey,
    extension_value: ExtensionValue,
    extensions: Extensions,
    failure: Failure,
    failure_category: FailureCategory,
    failure_code: FailureCode,
    failure_details: FailureDetails,
    failure_id: FailureId,
    failure_message: FailureMessage,
    failure_origin: FailureOrigin,
    fencing_epoch: FencingEpoch,
    graph_child_run_policy: GraphChildRunPolicy,
    graph_execution_limits: GraphExecutionLimits,
    graph_namespace: GraphNamespace,
    graph_node: GraphNode,
    graph_reducer_reference: GraphReducerReference,
    graph_reference: GraphReference,
    graph_route: GraphRoute,
    graph_routes: GraphRoutes,
    instruction: Instruction,
    instruction_content: InstructionContent,
    instruction_identity: InstructionIdentity,
    instruction_name: InstructionName,
    instruction_provenance: InstructionProvenance,
    interrupt_id: InterruptId,
    interrupt_record: InterruptRecord,
    interrupt_request: InterruptRequest,
    interrupt_request_head: InterruptRequestHead,
    interrupt_request_intent: InterruptRequestIntent,
    interrupt_resolution: InterruptResolution,
    interrupt_resolution_intent: InterruptResolutionIntent,
    interrupt_resolver: InterruptResolver,
    invocation_id: InvocationId,
    issuer_id: IssuerId,
    journal_append: JournalAppend,
    journal_event: JournalEvent,
    journal_event_intent: JournalEventIntent,
    journal_event_kind: JournalEventKind,
    journal_event_source: JournalEventSource,
    journal_expectation: JournalExpectation,
    journal_head: JournalHead,
    journal_payload: JournalPayload,
    journal_sequence: JournalSequence,
    json_content: JsonContent,
    known_costs: KnownCosts,
    language_tag: LanguageTag,
    media_type: MediaType,
    message: Message,
    message_id: MessageId,
    message_parts: MessageParts,
    message_producer: MessageProducer,
    message_producer_kind: MessageProducerKind,
    message_provenance: MessageProvenance,
    message_role: MessageRole,
    model_capabilities: ModelCapabilities,
    model_capability_issue: ModelCapabilityIssue,
    model_capability_mismatch: ModelCapabilityMismatch,
    model_descriptor: ModelDescriptor,
    model_error: ModelError,
    model_error_phase: ModelErrorPhase,
    model_error_provenance: ModelErrorProvenance,
    model_event: ModelEvent,
    model_event_kind: ModelEventKind,
    model_finish_reason: ModelFinishReason,
    model_invocation: ModelInvocation,
    model_invocation_head: ModelInvocationHead,
    model_invocation_intent: ModelInvocationIntent,
    model_invocation_revision: ModelInvocationRevision,
    model_invocation_state: ModelInvocationState,
    model_invocation_status: ModelInvocationStatus,
    model_invocation_transition: ModelInvocationTransition,
    model_invocation_transition_kind: ModelInvocationTransitionKind,
    model_modalities: ModelModalities,
    model_modality: ModelModality,
    model_output_delta: ModelOutputDelta,
    model_output_delta_kind: ModelOutputDeltaKind,
    model_output_item: ModelOutputItem,
    model_output_start: ModelOutputStart,
    model_provider_model_id: ModelProviderModelId,
    model_provider_replay: ModelProviderReplay,
    model_provider_replay_format: ModelProviderReplayFormat,
    model_provider_request_id: ModelProviderRequestId,
    model_provider_response_id: ModelProviderResponseId,
    model_provider_tool_call_id: ModelProviderToolCallId,
    model_request: ModelRequest,
    model_request_limits: ModelRequestLimits,
    model_requirements: ModelRequirements,
    model_response: ModelResponse,
    model_response_mode: ModelResponseMode,
    model_response_provenance: ModelResponseProvenance,
    model_stream_chunk: ModelStreamChunk,
    model_structured_output_capabilities: ModelStructuredOutputCapabilities,
    model_structured_output_level: ModelStructuredOutputLevel,
    model_text_output_format: ModelTextOutputFormat,
    model_token_limits: ModelTokenLimits,
    model_tool_call_proposal: ModelToolCallProposal,
    model_tool_capabilities: ModelToolCapabilities,
    model_tool_choice: ModelToolChoice,
    model_tool_choices: ModelToolChoices,
    model_tool_failure: ModelToolFailure,
    model_tool_outcome: ModelToolOutcome,
    model_tool_requirements: ModelToolRequirements,
    model_tool_selection: ModelToolSelection,
    model_transcript: ModelTranscript,
    model_transcript_turn: ModelTranscriptTurn,
    model_usage: ModelUsage,
    model_usage_field: ModelUsageField,
    money: Money,
    node_activation: NodeActivation,
    node_attempt: NodeAttempt,
    node_attempt_completion: NodeAttemptCompletion,
    node_attempt_outcome: NodeAttemptOutcome,
    node_attempt_start: NodeAttemptStart,
    node_attempt_start_head: NodeAttemptStartHead,
    node_attempt_status: NodeAttemptStatus,
    node_control: NodeControl,
    node_control_kind: NodeControlKind,
    node_id: NodeId,
    node_invocation_binding: NodeInvocationBinding,
    node_invocation_binding_kind: NodeInvocationBindingKind,
    node_invocation_bindings: NodeInvocationBindings,
    node_state_change: NodeStateChange,
    node_state_update: NodeStateUpdate,
    node_terminal_output: NodeTerminalOutput,
    node_wait: NodeWait,
    node_waits: NodeWaits,
    outbox_attempt: OutboxAttempt,
    outbox_attempt_completion: OutboxAttemptCompletion,
    outbox_attempt_outcome: OutboxAttemptOutcome,
    outbox_attempt_start: OutboxAttemptStart,
    outbox_attempt_start_head: OutboxAttemptStartHead,
    outbox_attempt_status: OutboxAttemptStatus,
    outbox_delivery: OutboxDelivery,
    outbox_delivery_head: OutboxDeliveryHead,
    outbox_delivery_intent: OutboxDeliveryIntent,
    outbox_delivery_status: OutboxDeliveryStatus,
    outbox_destination_ref: OutboxDestinationRef,
    pending_node_result: PendingNodeResult,
    pending_node_result_head: PendingNodeResultHead,
    pending_node_result_intent: PendingNodeResultIntent,
    principal_identity: PrincipalIdentity,
    quarantine_id: QuarantineId,
    ready_nodes: ReadyNodes,
    redaction_state: RedactionState,
    resolved_budget: ResolvedBudget,
    retention_class: RetentionClass,
    retry_advice: RetryAdvice,
    route_id: RouteId,
    run_cancellation: RunCancellation,
    run_cancellation_request: RunCancellationRequest,
    run_failure: RunFailure,
    run_fence: RunFence,
    run_id: RunId,
    run_interrupt: RunInterrupt,
    run_interrupt_kind: RunInterruptKind,
    run_lease: RunLease,
    run_lifecycle: RunLifecycle,
    run_revision: RunRevision,
    run_status: RunStatus,
    run_timer: RunTimer,
    run_timer_kind: RunTimerKind,
    run_transition: RunTransition,
    run_transition_kind: RunTransitionKind,
    run_wait: RunWait,
    run_waits: RunWaits,
    scheduler_reservation_id: SchedulerReservationId,
    scheduler_shard_id: SchedulerShardId,
    schema_id: SchemaId,
    schema_reference: SchemaReference,
    scope: Scope,
    scope_set: ScopeSet,
    security_label: SecurityLabel,
    skill_acting_window: SkillActingWindow,
    skill_acting_window_duration: SkillActingWindowDuration,
    skill_acting_window_id: SkillActingWindowId,
    skill_acting_window_open_request: SkillActingWindowOpenRequest,
    skill_acting_window_revocation: SkillActingWindowRevocation,
    skill_acting_window_revocation_reason: SkillActingWindowRevocationReason,
    skill_activation_approval: SkillActivationApproval,
    skill_activation_approval_id: SkillActivationApprovalId,
    skill_activation_scope: SkillActivationScope,
    skill_activation_source: SkillActivationSource,
    skill_authorization_subject: SkillAuthorizationSubject,
    subject_id: SubjectId,
    superstep: Superstep,
    tenant_id: TenantId,
    text_content: TextContent,
    thread_id: ThreadId,
    timer_firing: TimerFiring,
    timer_firing_intent: TimerFiringIntent,
    timer_id: TimerId,
    timer_registration_intent: TimerRegistrationIntent,
    timestamp: Timestamp,
    token_count: TokenCount,
    tool_artifacts: ToolArtifacts,
    tool_authorization_operation: ToolAuthorizationOperation,
    tool_authorization_provenance: ToolAuthorizationProvenance,
    tool_authorization_receipt: ToolAuthorizationReceipt,
    tool_cancellation_support: ToolCancellationSupport,
    tool_descriptor: ToolDescriptor,
    tool_error: ToolError,
    tool_error_phase: ToolErrorPhase,
    tool_error_provenance: ToolErrorProvenance,
    tool_execution_limits: ToolExecutionLimits,
    tool_execution_semantics: ToolExecutionSemantics,
    tool_external_effect: ToolExternalEffect,
    tool_idempotency: ToolIdempotency,
    tool_input: ToolInput,
    tool_invocation: ToolInvocation,
    tool_invocation_capabilities: ToolInvocationCapabilities,
    tool_invocation_head: ToolInvocationHead,
    tool_invocation_intent: ToolInvocationIntent,
    tool_invocation_limit: ToolInvocationLimit,
    tool_invocation_revision: ToolInvocationRevision,
    tool_invocation_state: ToolInvocationState,
    tool_invocation_status: ToolInvocationStatus,
    tool_invocation_transition: ToolInvocationTransition,
    tool_invocation_transition_kind: ToolInvocationTransitionKind,
    tool_progress_event: ToolProgressEvent,
    tool_progress_provenance: ToolProgressProvenance,
    tool_progress_update: ToolProgressUpdate,
    tool_recovery_handle: ToolRecoveryHandle,
    tool_resource_access: ToolResourceAccess,
    tool_resource_requirements: ToolResourceRequirements,
    tool_result: ToolResult,
    tool_result_provenance: ToolResultProvenance,
    tool_risk: ToolRisk,
    version: Version,
    wait_registration_intent: WaitRegistrationIntent,
}

rust_only! {
    AgentAdmissionAuthorityError,
    AgentAdmissionBudgetLayerError,
    AgentAdmissionError,
    AgentAdmissionIntentError,
    AgentArtifactsError,
    AgentDescriptorError,
    AgentExecutionConfigError,
    AgentInstructionsError,
    AgentRequestValidationError,
    AgentResultError,
    AgentResultProvenanceError,
    AgentResultValidationError,
    AgentSubmissionKeyError,
    AgentToolsError,
    ArtifactDescriptionError,
    ArtifactNameError,
    ArtifactParentsError,
    ArtifactRefError,
    ArtifactRepresentationError,
    BarrierResultHeadsError,
    BoundedJsonError,
    BoxFuture<'static, ()>,
    BoxStream<'static, ()>,
    BudgetEvaluationError,
    BudgetNarrowingError,
    BudgetResolutionError,
    BudgetUsageBuilder,
    BudgetUsageError,
    CancellationSignal,
    CanonicalJson,
    CanonicalJsonError,
    CapabilityDescriptionError,
    CapabilityLifecycleError,
    CapabilityLifecycleState,
    CapabilityMetadataError,
    CapabilityNameError,
    CapabilityTitleError,
    CheckpointBarrierError,
    CheckpointBarrierIntegrityError,
    CheckpointError,
    CheckpointHeadError,
    CheckpointIntegrityError,
    CheckpointLineageError,
    CheckpointLineageVerifier,
    CheckpointStateError,
    CheckpointWriteError,
    ChildRunAdmissionIntentError,
    ChildRunBudgetError,
    ChildRunJoinError,
    ChildRunKeyError,
    ChildRunPolicyError,
    CostCollectionError,
    CountParseError,
    CumulativeBudgetReservationError,
    CurrencyCodeError,
    DigestAlgorithm,
    DigestError,
    DurableWaitError,
    DurationMillisError,
    ExtensionKeyError,
    ExtensionKeyKind,
    ExtensionLimit,
    ExtensionLimits,
    ExtensionLimitsError,
    ExtensionsError,
    FailureBuildError,
    FailureDetailsError,
    FailureIdentifierError,
    FailureMessageError,
    FencingEpochError,
    GeneratedIdError,
    GraphBarrierDisposition,
    GraphBarrierPlan,
    GraphBarrierPlanError,
    GraphCompileError,
    GraphComposition,
    GraphCompositionError,
    GraphExecutionLimitsError,
    GraphNamespaceError,
    GraphNodeError,
    GraphReducerError,
    GraphReducerInput<'static>,
    GraphRouteError,
    GraphRoutesError,
    GraphSchemaValidationError,
    GraphSubgraphCall,
    GraphValueKind,
    InstructionError,
    InstructionNameError,
    IssuerIdError,
    JournalAppendError,
    JournalAuthorityError,
    JournalChainError,
    JournalChainVerifier,
    JournalEventError,
    JournalEventKindError,
    JournalIntegrityError,
    JournalIntentError,
    JournalPayloadError,
    JournalSequenceError,
    JsonLimit,
    JsonLimits,
    JsonLimitsError,
    JsonStats,
    LanguageTagError,
    MediaTypeError,
    MessageError,
    MessagePartsError,
    ModelCapabilitiesError,
    ModelCapabilityMismatchError,
    ModelContext,
    ModelContextError,
    ModelDescriptorError,
    ModelErrorValidationError,
    ModelEventAccumulator<'static>,
    ModelEventError,
    ModelEventStreamError,
    ModelInvocationError,
    ModelInvocationHeadError,
    ModelInvocationHistoryError,
    ModelInvocationHistoryVerifier,
    ModelInvocationIntegrityError,
    ModelInvocationIntentError,
    ModelInvocationRevisionError,
    ModelModalitiesError,
    ModelOutputItemError,
    ModelOutputItemKind,
    ModelProviderIdentifierError,
    ModelProviderReplayError,
    ModelProviderReplayFormatError,
    ModelRequestBuilder,
    ModelRequestError,
    ModelRequestLimitsError,
    ModelRequirementsError,
    ModelResponseError,
    ModelStopReason,
    ModelStreamChunkError,
    ModelStructuredOutputCapabilitiesError,
    ModelTokenLimitsError,
    ModelToolCallProposalError,
    ModelToolCapabilitiesError,
    ModelToolChoicesError,
    ModelToolFailureError,
    ModelToolRequirementsError,
    ModelTranscriptError,
    ModelTranscriptTurnError,
    ModelUsageError,
    MoneyArithmeticError,
    NodeActivationError,
    NodeAttemptError,
    NodeAttemptHistoryError,
    NodeAttemptHistoryVerifier,
    NodeAttemptIntegrityError,
    NodeDispatchReason,
    NodeIdError,
    NodeInvocationBindingError,
    NodeInvocationBindingsError,
    NodeStateUpdateError,
    NodeTerminalOutputError,
    NodeWaitsError,
    OutboxAttemptError,
    OutboxAttemptHistoryError,
    OutboxAttemptHistoryVerifier,
    OutboxAttemptIntegrityError,
    OutboxDeliveryError,
    OutboxDeliveryIntegrityError,
    PendingNodeResultError,
    PendingNodeResultIntegrityError,
    PendingNodeResultIntentError,
    ReadyNodeRecoveryError,
    ReadyNodeRecoveryPlan,
    ReadyNodeRecoveryPlanner,
    ReadyNodesError,
    RecoveryNode,
    RecoveryNodeKind,
    RetentionClassError,
    RouteIdError,
    RunCancellationError,
    RunFailureError,
    RunInterruptError,
    RunLeaseError,
    RunLeaseValidationError,
    RunLifecycleError,
    RunTimerError,
    RunTransitionError,
    RunWaitsError,
    SchedulerShardIdError,
    SchemaIdError,
    ScopeError,
    ScopeSetError,
    SecurityLabelError,
    SharedStateSubgraph,
    SkillActingWindowError,
    SkillActivationStoreError,
    SkillActivationStoreFailure,
    SkillAuthorizationSubjectError,
    SubjectIdError,
    SuperstepError,
    TenantIdError,
    TextContentError,
    TimestampError,
    ToolAdapter<(), ()>,
    ToolAdapterBuildError,
    ToolArtifactBinding,
    ToolArtifactsError,
    ToolAuthorizationReceiptError,
    ToolAuthorizationReceiptSinkError,
    ToolAuthorizationReceiptSinkFailure,
    ToolContext,
    ToolContextBindingError,
    ToolContextError,
    ToolDescriptorError,
    ToolErrorBuildError,
    ToolErrorValidationError,
    ToolExecutionLimitsError,
    ToolExecutionSemanticsError,
    ToolIdempotencyKey,
    ToolInputError,
    ToolInputValidationError,
    ToolInvocationError,
    ToolInvocationHeadError,
    ToolInvocationHistoryError,
    ToolInvocationHistoryVerifier,
    ToolInvocationIntegrityError,
    ToolInvocationIntentError,
    ToolInvocationRevisionError,
    ToolOutput<()>,
    ToolProgressError,
    ToolProgressEventValidationError,
    ToolProgressReporter,
    ToolProgressSinkError,
    ToolProgressUpdateError,
    ToolReconciliationContext,
    ToolReconciliationContextBindingError,
    ToolReconciliationContextError,
    ToolReconciliationObservation,
    ToolReconciliationObservationError,
    ToolReconciliationProbeError,
    ToolReconciliationProbeErrorBuildError,
    ToolRecoveryHandleError,
    ToolResultValidationError,
    ToolSchemaRole,
    ToolSchemaValidationError,
    ToolStopReason,
    VersionComponent,
    VersionError,
}

#[test]
fn output_only_types_preserve_producer_wires_and_have_no_owned_reader() {
    fn serializable<T: Serialize>() {}
    serializable::<BudgetRemaining>();
    serializable::<GraphNodeSource>();
    cannot_implement!(BudgetRemaining: serde::de::DeserializeOwned);
    cannot_implement!(GraphNodeSource: serde::de::DeserializeOwned);
    schema_pin::<BudgetRemaining>("BudgetRemaining");
    for name in ["BudgetRemaining", "GraphNodeSource"] {
        let entry = &INVENTORY.types[name];
        assert_eq!(entry.mode, "write_only");
        assert!(!entry.vectors.is_empty());
        for vector in &entry.vectors {
            let wire = DOCUMENTS[&vector.fixture].pointer(&vector.pointer).unwrap();
            assert_eq!(canonical(wire.clone()).digest(), vector.canonical_digest);
        }
    }
    // Real constructors are compared with these wires by reservation and
    // graph-composition unit tests, without inventing a deserialization API.
    assert!(INVENTORY.types["GraphNodeSource"].schema_digest.is_none());
}

fn identifier(text: &str) -> bool {
    !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && !text.as_bytes()[0].is_ascii_digit()
}
fn root_exports(source: &str) -> Result<BTreeMap<String, String>, &'static str> {
    let mut exports = BTreeMap::new();
    let mut declaration = String::new();
    for line in source.lines().map(str::trim) {
        if declaration.is_empty() {
            if !line.starts_with("pub ") {
                continue;
            }
            if !line.starts_with("pub use ") {
                return Err("unclassified direct public declaration");
            }
        }
        declaration.push_str(line);
        if !line.ends_with(';') {
            continue;
        }
        let body = declaration
            .strip_prefix("pub use ")
            .ok_or("invalid export prefix")?;
        let (module, names) = body
            .split_once("::{")
            .ok_or("unsupported public export syntax")?;
        if !identifier(module) {
            return Err("unsupported public module path");
        }
        let names = names
            .strip_suffix("};")
            .ok_or("unsupported public export suffix")?;
        for name in names.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            if !identifier(name) {
                return Err("public alias or glob needs explicit inventory review");
            }
            if exports.insert(name.into(), module.into()).is_some() {
                return Err("duplicate root export");
            }
        }
        declaration.clear();
    }
    if !declaration.is_empty() {
        return Err("unfinished public export");
    }
    Ok(exports)
}

#[test]
fn every_named_root_export_has_exactly_one_reviewed_classification() {
    assert_eq!(
        INVENTORY.schema,
        "https://stateknot.github.io/schema/test-fixture/core-public-type-inventory/1.0.0"
    );
    let actual = root_exports(include_str!("../src/lib.rs")).unwrap();
    let mut expected = BTreeMap::new();
    let mut modes = BTreeMap::<&str, usize>::new();
    for (name, entry) in &INVENTORY.types {
        assert!(matches!(
            entry.kind.as_str(),
            "struct" | "enum" | "type" | "macro_or_reexport"
        ));
        assert!(
            expected
                .insert(name.clone(), entry.module.clone())
                .is_none()
        );
        *modes.entry(entry.mode.as_str()).or_default() += 1;
        let groups = match entry.mode.as_str() {
            "read_write" => READERS,
            "write_only" => &["BudgetRemaining", "GraphNodeSource"],
            "none" => RUST_ONLY,
            other => panic!("unreviewed Serde classification {other}"),
        };
        assert!(
            groups
                .iter()
                .any(|ty| ty.split_whitespace().collect::<String>()
                    == entry.rust_type.split_whitespace().collect::<String>()),
            "{name} lacks compiler evidence"
        );
        if entry.mode == "none" {
            assert!(entry.vectors.is_empty() && entry.schema_digest.is_none());
        }
    }
    for (name, entry) in &INVENTORY.non_types {
        assert!(matches!(entry.kind.as_str(), "trait" | "const"));
        assert!(
            expected
                .insert(name.clone(), entry.module.clone())
                .is_none()
        );
    }
    assert_eq!(
        actual, expected,
        "root API changed: classify new exports and add typed evidence"
    );
    assert_eq!(
        modes,
        BTreeMap::from([("none", 246), ("read_write", 307), ("write_only", 2)])
    );
    let names = READERS
        .iter()
        .chain(RUST_ONLY)
        .copied()
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), READERS.len() + RUST_ONLY.len());
    assert_eq!(expected.len(), 570);
}

#[test]
fn export_inventory_parser_fails_closed_on_unreviewed_syntax() {
    assert_eq!(
        root_exports("pub use foo::{Existing, NewType};").unwrap(),
        BTreeMap::from([
            ("Existing".into(), "foo".into()),
            ("NewType".into(), "foo".into())
        ])
    );
    for source in [
        "pub mod new_module;",
        "pub struct NewType;",
        "pub use foo::*;",
        "pub use foo::{Existing as Alias};",
        "pub use foo::{Existing, Existing};",
        "pub use foo::{Existing",
    ] {
        assert!(
            root_exports(source).is_err(),
            "accepted unsupported export {source}"
        );
    }
}

#[test]
fn scalar_tags_are_frozen_from_their_own_public_enum_variants() {
    let document: Value = serde_json::from_str(include_str!(
        "fixtures/core-admission-transcript-wires-v1.json"
    ))
    .unwrap();
    assert_eq!(
        document["public_tags"],
        json!({
            "NodeAttemptStatus": NodeAttemptStatus::Succeeded,
            "OutboxAttemptStatus": OutboxAttemptStatus::Acknowledged,
            "OutboxDeliveryStatus": OutboxDeliveryStatus::Pending,
            "NodeInvocationBindingKind": NodeInvocationBindingKind::Tool,
            "ModelUsageField": ModelUsageField::InputTokens,
            "ToolInvocationStatus": ToolInvocationStatus::Prepared,
            "ModelInvocationStatus": ModelInvocationStatus::Prepared,
            "ToolInvocationTransitionKind": ToolInvocationTransitionKind::StartAttempt,
            "ModelInvocationTransitionKind": ModelInvocationTransitionKind::StartAttempt,
            "NodeControlKind": NodeControlKind::Continue
        })
    );
}
