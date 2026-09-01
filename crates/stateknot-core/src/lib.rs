// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Provider-independent domain types shared across `StateKnot` runtime boundaries.
//!
//! This crate is intentionally independent of model providers, wire protocols,
//! databases, HTTP servers, and async executors. Implemented contracts are
//! introduced only with strict validation, schemas, and versioned wire
//! fixtures from RFC-0001.

#![forbid(unsafe_code)]

mod accounting;
mod agent;
mod agent_runtime;
mod artifact;
mod barrier;
mod budget;
mod canonical;
mod capability;
mod checkpoint;
mod content;
mod decimal;
mod digest;
mod extension;
mod failure;
mod graph;
mod identity;
mod ids;
mod journal;
mod json;
mod lease;
mod message;
mod model;
mod model_event;
mod model_invocation;
mod model_request;
mod model_response;
mod model_runtime;
mod node_attempt;
mod node_result;
mod outbox;
mod recovery;
mod run;
mod schema;
mod scope;
mod time;
mod tool;
mod tool_invocation;
mod tool_runtime;
mod version;
mod wait;

pub use accounting::{
    ByteCount, CountParseError, CurrencyCode, CurrencyCodeError, ExecutionCount, Money,
    MoneyArithmeticError, TokenCount,
};
pub use agent::{
    AgentDescriptor, AgentDescriptorError, AgentExecutionConfig, AgentExecutionConfigError,
    AgentInstructions, AgentInstructionsError, AgentStructuredOutputStrategy, AgentToolConcurrency,
    AgentTools, AgentToolsError,
};
pub use agent_runtime::{
    AgentArtifacts, AgentArtifactsError, AgentRequest, AgentRequestValidationError, AgentResult,
    AgentResultError, AgentResultProvenance, AgentResultProvenanceError,
    AgentResultValidationError,
};
pub use artifact::{
    ArtifactDescription, ArtifactDescriptionError, ArtifactIdentity, ArtifactModality,
    ArtifactName, ArtifactNameError, ArtifactParents, ArtifactParentsError, ArtifactPresentation,
    ArtifactProvenance, ArtifactRef, ArtifactRefError, ArtifactRepresentation,
    ArtifactRepresentationError, ContentPart, MediaType, MediaTypeError, RetentionClass,
    RetentionClassError,
};
pub use barrier::{
    BarrierResultHeads, BarrierResultHeadsError, CheckpointBarrier, CheckpointBarrierError,
    CheckpointBarrierIntegrityError,
};
pub use budget::{
    BudgetDimension, BudgetEvaluationError, BudgetLimits, BudgetRemaining, BudgetResolutionError,
    BudgetUsage, BudgetUsageBuilder, BudgetUsageError, CostCollectionError, CostLimits, KnownCosts,
    MAX_BUDGET_LAYERS, MAX_COST_CURRENCIES, ResolvedBudget,
};
pub use canonical::{CanonicalJson, CanonicalJsonError};
pub use capability::{
    CapabilityDescription, CapabilityDescriptionError, CapabilityIdentity, CapabilityKind,
    CapabilityLifecycle, CapabilityLifecycleError, CapabilityLifecycleState, CapabilityMetadata,
    CapabilityMetadataError, CapabilityName, CapabilityNameError, CapabilityReference,
    CapabilityTitle, CapabilityTitleError,
};
pub use checkpoint::{
    Checkpoint, CheckpointError, CheckpointHead, CheckpointHeadError, CheckpointIntegrityError,
    CheckpointLineageError, CheckpointLineageVerifier, CheckpointState, CheckpointStateError,
    CheckpointWrite, CheckpointWriteError, GraphReference, NodeId, NodeIdError, ReadyNodes,
    ReadyNodesError, Superstep, SuperstepError,
};
pub use content::{
    ContentMetadata, ContentSource, ContentTrust, JsonContent, LanguageTag, LanguageTagError,
    RedactionState, SecurityLabel, SecurityLabelError, TextContent, TextContentError,
};
pub use digest::{Digest, DigestAlgorithm, DigestError};
pub use extension::{
    ExtensionKey, ExtensionKeyError, ExtensionKeyKind, ExtensionLimit, ExtensionLimits,
    ExtensionLimitsError, ExtensionValue, Extensions, ExtensionsError,
};
pub use failure::{
    Failure, FailureBuildError, FailureCategory, FailureCode, FailureDetails, FailureDetailsError,
    FailureIdentifierError, FailureMessage, FailureMessageError, FailureOrigin, RetryAdvice,
};
pub use graph::{
    CompiledGraph, GraphBarrierDisposition, GraphBarrierPlan, GraphBarrierPlanError,
    GraphCompileError, GraphExecutionLimits, GraphExecutionLimitsError, GraphNode, GraphNodeError,
    GraphReducer, GraphReducerError, GraphReducerInput, GraphReducerReference, GraphRoute,
    GraphRouteError, GraphRoutes, GraphRoutesError, GraphSchemaValidationError,
    GraphSchemaValidator, GraphValueKind,
};
pub use identity::{IssuerId, IssuerIdError, PrincipalIdentity, SubjectId, SubjectIdError};
pub use ids::{
    ArtifactId, AttemptId, CheckpointId, DeliveryId, DestinationId, EventId, FailureId,
    GeneratedIdError, InterruptId, InvocationId, MessageId, QuarantineId, RunId,
    SchedulerReservationId, SchedulerShardId, SchedulerShardIdError, TenantId, TenantIdError,
    ThreadId, TimerId,
};
pub use journal::{
    JournalAppend, JournalAppendError, JournalAuthorityError, JournalChainError,
    JournalChainVerifier, JournalEvent, JournalEventError, JournalEventIntent, JournalEventKind,
    JournalEventKindError, JournalEventSource, JournalExpectation, JournalHead,
    JournalIntegrityError, JournalIntentError, JournalPayload, JournalPayloadError,
    JournalSequence, JournalSequenceError,
};
pub use json::{BoundedJson, BoundedJsonError, JsonLimit, JsonLimits, JsonLimitsError, JsonStats};
pub use lease::{
    FencingEpoch, FencingEpochError, RunFence, RunLease, RunLeaseError, RunLeaseValidationError,
};
pub use message::{
    Instruction, InstructionContent, InstructionError, InstructionIdentity, InstructionName,
    InstructionNameError, InstructionProvenance, Message, MessageError, MessageParts,
    MessagePartsError, MessageProducer, MessageProducerKind, MessageProvenance, MessageRole,
};
pub use model::{
    ModelCapabilities, ModelCapabilitiesError, ModelCapabilityIssue, ModelCapabilityMismatch,
    ModelCapabilityMismatchError, ModelDescriptor, ModelDescriptorError, ModelModalities,
    ModelModalitiesError, ModelModality, ModelRequirements, ModelRequirementsError,
    ModelStructuredOutputCapabilities, ModelStructuredOutputCapabilitiesError,
    ModelStructuredOutputLevel, ModelTokenLimits, ModelTokenLimitsError, ModelToolCapabilities,
    ModelToolCapabilitiesError, ModelToolChoice, ModelToolChoices, ModelToolChoicesError,
    ModelToolRequirements, ModelToolRequirementsError,
};
pub use model_event::{
    ModelEvent, ModelEventAccumulator, ModelEventError, ModelEventKind, ModelEventStreamError,
    ModelOutputDelta, ModelOutputDeltaKind, ModelOutputStart, ModelStreamChunk,
    ModelStreamChunkError, ModelUsageField,
};
pub use model_invocation::{
    ModelInvocation, ModelInvocationError, ModelInvocationHead, ModelInvocationHeadError,
    ModelInvocationHistoryError, ModelInvocationHistoryVerifier, ModelInvocationIntegrityError,
    ModelInvocationIntent, ModelInvocationIntentError, ModelInvocationRevision,
    ModelInvocationRevisionError, ModelInvocationState, ModelInvocationStatus,
    ModelInvocationTransition, ModelInvocationTransitionKind,
};
pub use model_request::{
    ModelRequest, ModelRequestBuilder, ModelRequestError, ModelRequestLimits,
    ModelRequestLimitsError, ModelResponseMode, ModelTextOutputFormat, ModelToolSelection,
};
pub use model_response::{
    ModelFinishReason, ModelOutputItem, ModelOutputItemError, ModelOutputItemKind,
    ModelProviderIdentifierError, ModelProviderModelId, ModelProviderRequestId,
    ModelProviderResponseId, ModelProviderToolCallId, ModelResponse, ModelResponseError,
    ModelResponseProvenance, ModelToolCallProposal, ModelToolCallProposalError, ModelUsage,
    ModelUsageError,
};
pub use model_runtime::{
    BoxFuture, BoxStream, CancellationObserver, CancellationSignal, Model, ModelContext,
    ModelContextError, ModelError, ModelErrorPhase, ModelErrorProvenance,
    ModelErrorValidationError, ModelSchemaRegistry, ModelStopReason,
};
pub use node_attempt::{
    NodeAttempt, NodeAttemptCompletion, NodeAttemptError, NodeAttemptHistoryError,
    NodeAttemptHistoryVerifier, NodeAttemptIntegrityError, NodeAttemptOutcome, NodeAttemptStart,
    NodeAttemptStartHead, NodeAttemptStatus,
};
pub use node_result::{
    NodeControl, NodeControlKind, NodeInvocationBinding, NodeInvocationBindingError,
    NodeInvocationBindingKind, NodeInvocationBindings, NodeInvocationBindingsError,
    NodeStateChange, NodeStateUpdate, NodeStateUpdateError, NodeTerminalOutput,
    NodeTerminalOutputError, NodeWait, NodeWaits, NodeWaitsError, PendingNodeResult,
    PendingNodeResultError, PendingNodeResultHead, PendingNodeResultIntegrityError,
    PendingNodeResultIntent, PendingNodeResultIntentError, RouteId, RouteIdError,
};
pub use outbox::{
    DeliveryFence, MAX_OUTBOX_ATTEMPT_LEASE_MILLIS, MAX_OUTBOX_ATTEMPTS, OutboxAttempt,
    OutboxAttemptCompletion, OutboxAttemptError, OutboxAttemptHistoryError,
    OutboxAttemptHistoryVerifier, OutboxAttemptIntegrityError, OutboxAttemptOutcome,
    OutboxAttemptStart, OutboxAttemptStartHead, OutboxAttemptStatus, OutboxDelivery,
    OutboxDeliveryError, OutboxDeliveryHead, OutboxDeliveryIntegrityError, OutboxDeliveryIntent,
    OutboxDeliveryStatus, OutboxDestinationRef,
};
pub use recovery::{
    NodeDispatchReason, ReadyNodeRecoveryError, ReadyNodeRecoveryPlan, ReadyNodeRecoveryPlanner,
    RecoveryNode, RecoveryNodeKind,
};
pub use run::{
    RunCancellation, RunCancellationError, RunCancellationRequest, RunFailure, RunFailureError,
    RunInterrupt, RunInterruptError, RunInterruptKind, RunLifecycle, RunLifecycleError,
    RunRevision, RunStatus, RunTimer, RunTimerError, RunTimerKind, RunTransition,
    RunTransitionError, RunTransitionKind, RunWait, RunWaits, RunWaitsError,
};
pub use schema::{SchemaId, SchemaIdError, SchemaReference};
pub use scope::{Scope, ScopeError, ScopeSet, ScopeSetError};
pub use time::{DurationMillis, DurationMillisError, Timestamp, TimestampError};
pub use tool::{
    ToolCancellationSupport, ToolDescriptor, ToolDescriptorError, ToolExecutionLimits,
    ToolExecutionLimitsError, ToolExecutionSemantics, ToolExecutionSemanticsError, ToolIdempotency,
    ToolInvocationCapabilities, ToolResourceAccess, ToolResourceRequirements, ToolRisk,
};
pub use tool_invocation::{
    GraphNamespace, GraphNamespaceError, NodeActivation, NodeActivationError, ToolArtifactBinding,
    ToolInvocation, ToolInvocationError, ToolInvocationHead, ToolInvocationHeadError,
    ToolInvocationHistoryError, ToolInvocationHistoryVerifier, ToolInvocationIntegrityError,
    ToolInvocationIntent, ToolInvocationIntentError, ToolInvocationLimit, ToolInvocationRevision,
    ToolInvocationRevisionError, ToolInvocationState, ToolInvocationStatus,
    ToolInvocationTransition, ToolInvocationTransitionKind,
};
pub use tool_runtime::{
    ErasedTool, Tool, ToolAdapter, ToolAdapterBuildError, ToolArtifacts, ToolArtifactsError,
    ToolContext, ToolContextBindingError, ToolContextError, ToolError, ToolErrorBuildError,
    ToolErrorPhase, ToolErrorProvenance, ToolErrorValidationError, ToolExternalEffect,
    ToolIdempotencyKey, ToolInput, ToolInputError, ToolInputValidationError, ToolOutput,
    ToolProgressError, ToolProgressEvent, ToolProgressEventValidationError, ToolProgressProvenance,
    ToolProgressReporter, ToolProgressSink, ToolProgressSinkError, ToolProgressUpdate,
    ToolProgressUpdateError, ToolResult, ToolResultProvenance, ToolResultValidationError,
    ToolSchemaRegistry, ToolSchemaRole, ToolSchemaValidationError, ToolStopReason,
};
pub use version::{Version, VersionComponent, VersionError};
pub use wait::{
    DurableTimer, DurableTimerHead, DurableTimerRecord, DurableWait, DurableWaitError,
    InterruptRecord, InterruptRequest, InterruptRequestHead, InterruptRequestIntent,
    InterruptResolution, InterruptResolutionIntent, InterruptResolver, TimerFiring,
    TimerFiringIntent, TimerRegistrationIntent, WaitRegistrationIntent,
};
