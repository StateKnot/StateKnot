// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Production-safe prebuilt graph for provider-native model/tool agents.
//!
//! Checkpoints retain only stable invocation identities and policy evidence.
//! Provider replay fragments, tool results, and failures remain authoritative in
//! their dedicated invocation ledgers and are reconstructed before each model
//! continuation. This keeps checkpoint size bounded and prevents a crashed node
//! from redispatching an external call whose durable start already committed.

use std::{collections::BTreeSet, fmt, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use stateknot_core::{
    AgentArtifacts, AgentDescriptor, AgentInstructions, AgentStructuredOutputStrategy,
    AgentToolConcurrency, AttemptId, BoundedJson, BoxFuture, BudgetUsage, ByteCount,
    CapabilityIdentity, Checkpoint, CheckpointState, CompiledGraph, ContentMetadata, ContentPart,
    ContentSource, ContentTrust, Digest, DurationMillis, EventId, ExecutionCount, Failure,
    FailureCategory, FailureCode, FailureId, FailureMessage, FailureOrigin, GraphCompileError,
    GraphExecutionLimits, GraphNode, GraphReducer, GraphReducerError, GraphReducerInput,
    GraphReducerReference, GraphReference, GraphRoute, GraphRoutes, Instruction,
    InstructionIdentity, InstructionName, InstructionProvenance, InvocationId, JournalAppend,
    JournalEventIntent, JournalEventKind, JournalExpectation, JournalPayload, JsonContent,
    KnownCosts, Message, MessageId, MessageParts, MessageProducer, MessageProvenance, MessageRole,
    ModelError, ModelErrorPhase, ModelFinishReason, ModelInvocation, ModelInvocationIntent,
    ModelInvocationState, ModelOutputItem, ModelRequest, ModelRequestLimits, ModelTextOutputFormat,
    ModelToolCallProposal, ModelToolChoice, ModelToolOutcome, ModelToolSelection, ModelTranscript,
    ModelTranscriptTurn, NodeActivation, NodeControl, NodeId, NodeInvocationBinding,
    NodeInvocationBindings, NodeStateChange, NodeStateUpdate, NodeTerminalOutput, ReadyNodes,
    RedactionState, RetryAdvice, RouteId, SchemaId, SchemaReference, SecurityLabel, Superstep,
    TextContent, TokenCount, ToolInput, ToolInvocation, ToolInvocationIntent, ToolInvocationState,
    ToolRisk, Version,
};
use stateknot_store_postgres::{NodeAttemptHistoryPageSize, PostgresStore, StoreError};
use thiserror::Error;

use crate::{
    DurableInvocationExecutor, ExecutableGraphRegistryBuilder, ExecutableGraphRegistryError,
    GraphCancellationEvidence, GraphCancellationEvidenceContext, GraphFailureEvidence,
    GraphFailureEvidenceContext, GraphLifecycleEvidenceError, GraphLifecycleEvidenceProvider,
    GraphNodeContext, GraphNodeExecution, GraphNodeExecutionError, GraphNodeExecutor,
    GraphTerminalEvidence, GraphTerminalEvidenceContext, InvocationAttemptEventIds,
    JsonSchemaRegistry, JsonSchemaRegistryBuilder, JsonSchemaRegistryError, ModelAttemptHandoff,
    ModelAttemptOutcome, ToolAttemptHandoff, ToolAttemptOutcome, ToolAttemptStartOutcome,
    ToolReconciliationAttemptExecutionError, ToolReconciliationAttemptHandoff,
    ToolReconciliationAttemptOutcome, ToolTerminalCommitHandoff,
    standard_invocation_execution_event_schema,
};

const IMPLEMENTATION_VERSION: &str = "stateknot.provider-native-agent.v1";
const MAX_CHECKPOINT_INVOCATION_REFERENCES: u64 = 4096;
const OUTPUT_REPAIR_INSTRUCTION_NAME: &str = "stateknot.output_repair";
const OUTPUT_REPAIR_INSTRUCTION_TEXT: &str = "A prior model attempt did not produce exactly one JSON value that satisfies the required JSON Schema. Return exactly one JSON value that conforms to the requested schema. Do not call tools. Do not include prose, Markdown, or additional content.";

/// Stable model node identifier used by every v1 provider-native graph.
pub const PROVIDER_NATIVE_MODEL_NODE_ID: &str = "agent.model";
/// Stable ordered tool node identifier used by every v1 provider-native graph.
pub const PROVIDER_NATIVE_TOOLS_NODE_ID: &str = "agent.tools";
/// Conditional route selected when a committed response proposes tools.
pub const PROVIDER_NATIVE_TOOLS_ROUTE_ID: &str = "tools";

/// Immutable identity of a locally installed tool-authorization policy.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentToolPolicyReference {
    identity: CapabilityIdentity,
    definition_digest: Digest,
}

impl AgentToolPolicyReference {
    /// Constructs a digest-pinned policy reference.
    #[must_use]
    pub const fn new(identity: CapabilityIdentity, definition_digest: Digest) -> Self {
        Self {
            identity,
            definition_digest,
        }
    }

    /// Returns the owner-qualified policy version.
    #[must_use]
    pub const fn identity(&self) -> &CapabilityIdentity {
        &self.identity
    }

    /// Returns the checksum of the exact installed policy contract.
    #[must_use]
    pub const fn definition_digest(&self) -> Digest {
        self.definition_digest
    }
}

/// Immutable input to one ordinary tool policy decision.
#[derive(Clone, Debug)]
pub struct AgentToolPolicyContext {
    agent: CapabilityIdentity,
    admission_digest: Digest,
    model_invocation_id: InvocationId,
    proposal_index: usize,
    proposal: ModelToolCallProposal,
    action_digest: Digest,
}

impl AgentToolPolicyContext {
    /// Returns the admitted agent version.
    #[must_use]
    pub const fn agent(&self) -> &CapabilityIdentity {
        &self.agent
    }

    /// Returns the immutable admission snapshot digest.
    #[must_use]
    pub const fn admission_digest(&self) -> Digest {
        self.admission_digest
    }

    /// Returns the committed model invocation that proposed the action.
    #[must_use]
    pub const fn model_invocation_id(&self) -> InvocationId {
        self.model_invocation_id
    }

    /// Returns the zero-based provider proposal position.
    #[must_use]
    pub const fn proposal_index(&self) -> usize {
        self.proposal_index
    }

    /// Returns the exact untrusted model proposal.
    #[must_use]
    pub const fn proposal(&self) -> &ModelToolCallProposal {
        &self.proposal
    }

    /// Returns the domain-separated action checksum persisted on allow.
    #[must_use]
    pub const fn action_digest(&self) -> Digest {
        self.action_digest
    }
}

/// Closed result of a tool policy evaluation.
#[derive(Clone, Debug)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum AgentToolPolicyDecision {
    /// The exact action may proceed and is bound to decision evidence.
    Allow {
        /// Digest of the policy engine's immutable decision artifact.
        evidence_digest: Digest,
    },
    /// The action is denied with public-safe failure evidence.
    Deny {
        /// Uncaused failure suitable for the graph node result.
        failure: Failure,
    },
}

/// Payload-redacted dependency failure while evaluating tool policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentToolPolicyError {
    /// The pinned policy implementation cannot evaluate the decision now.
    #[error("agent tool policy is temporarily unavailable")]
    TemporarilyUnavailable,
    /// The policy implementation returned corrupt or unverifiable evidence.
    #[error("agent tool policy returned invalid evidence")]
    InvalidEvidence,
}

/// Side-effect-free policy boundary invoked before any tool preparation.
///
/// Implementations must be deterministic for one context and must not perform
/// the proposed action. Network policy engines need their own durable decision
/// ledger; this synchronous graph boundary is for already-local, pinned policy.
pub trait AgentToolPolicy: Send + Sync + 'static {
    /// Returns the immutable implementation reference.
    fn reference(&self) -> &AgentToolPolicyReference;

    /// Evaluates exactly one proposal without executing it.
    fn evaluate(
        &self,
        context: AgentToolPolicyContext,
    ) -> BoxFuture<'_, Result<AgentToolPolicyDecision, AgentToolPolicyError>>;
}

/// Immutable identity of an offline invocation-accounting implementation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentInvocationAccountingReference {
    identity: CapabilityIdentity,
    definition_digest: Digest,
}

impl AgentInvocationAccountingReference {
    /// Constructs a digest-pinned accounting reference.
    #[must_use]
    pub const fn new(identity: CapabilityIdentity, definition_digest: Digest) -> Self {
        Self {
            identity,
            definition_digest,
        }
    }

    /// Returns the owner-qualified accounting implementation version.
    #[must_use]
    pub const fn identity(&self) -> &CapabilityIdentity {
        &self.identity
    }

    /// Returns the checksum of the installed pricing contract and tables.
    #[must_use]
    pub const fn definition_digest(&self) -> Digest {
        self.definition_digest
    }
}

/// Monetary accounting result for one terminal external invocation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AgentInvocationCharge {
    /// The complete charge is known, including a legitimately free invocation.
    Known(KnownCosts),
    /// No complete price was available for the exact provider evidence.
    Unpriced,
}

/// Offline, deterministic pricing boundary for durable model and tool ledgers.
///
/// Implementations must perform no I/O and must be deterministic for the exact
/// terminal invocation snapshot. Their reference is part of the graph contract
/// digest, so changing a price table requires a new graph version. Returning
/// [`AgentInvocationCharge::Unpriced`] preserves the event in usage evidence
/// and causes finite monetary budget evaluation to stop before another call.
pub trait AgentInvocationAccounting: Send + Sync + 'static {
    /// Returns the immutable implementation and price-table reference.
    fn reference(&self) -> &AgentInvocationAccountingReference;

    /// Prices one committed or failed model attempt.
    fn model_charge(&self, invocation: &ModelInvocation) -> AgentInvocationCharge;

    /// Prices one committed or failed tool attempt.
    fn tool_charge(&self, invocation: &ToolInvocation) -> AgentInvocationCharge;
}

/// Stable identities retained before one model invocation can start.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
pub struct ProviderNativeModelPlan {
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    prepared_event_id: EventId,
    attempt_start_event_id: EventId,
    attempt_terminal_event_id: EventId,
}

impl ProviderNativeModelPlan {
    fn generate() -> Self {
        Self {
            invocation_id: InvocationId::generate(),
            attempt_id: AttemptId::generate(),
            prepared_event_id: EventId::generate(),
            attempt_start_event_id: EventId::generate(),
            attempt_terminal_event_id: EventId::generate(),
        }
    }

    /// Returns the stable logical model invocation identity.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the sole planned physical attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the stable preparation journal event identity.
    #[must_use]
    pub const fn prepared_event_id(&self) -> EventId {
        self.prepared_event_id
    }

    /// Returns the stable attempt-start journal event identity.
    #[must_use]
    pub const fn attempt_start_event_id(&self) -> EventId {
        self.attempt_start_event_id
    }

    /// Returns the stable attempt-terminal journal event identity.
    #[must_use]
    pub const fn attempt_terminal_event_id(&self) -> EventId {
        self.attempt_terminal_event_id
    }
}

/// Stable identities and policy proof retained before a tool can start.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderNativeToolPlan {
    proposal_index: u16,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    prepared_event_id: EventId,
    attempt_start_event_id: EventId,
    attempt_terminal_event_id: EventId,
    action_digest: Digest,
    policy_evidence_digest: Digest,
}

impl ProviderNativeToolPlan {
    fn generate(
        proposal_index: u16,
        action_digest: Digest,
        policy_evidence_digest: Digest,
    ) -> Self {
        Self {
            proposal_index,
            invocation_id: InvocationId::generate(),
            attempt_id: AttemptId::generate(),
            prepared_event_id: EventId::generate(),
            attempt_start_event_id: EventId::generate(),
            attempt_terminal_event_id: EventId::generate(),
            action_digest,
            policy_evidence_digest,
        }
    }

    /// Returns the zero-based proposal position.
    #[must_use]
    pub const fn proposal_index(&self) -> u16 {
        self.proposal_index
    }

    /// Returns the stable logical tool invocation identity.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the sole planned physical attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the stable preparation journal event identity.
    #[must_use]
    pub const fn prepared_event_id(&self) -> EventId {
        self.prepared_event_id
    }

    /// Returns the stable attempt-start journal event identity.
    #[must_use]
    pub const fn attempt_start_event_id(&self) -> EventId {
        self.attempt_start_event_id
    }

    /// Returns the stable attempt-terminal journal event identity.
    #[must_use]
    pub const fn attempt_terminal_event_id(&self) -> EventId {
        self.attempt_terminal_event_id
    }

    /// Derives the stable automated-reconciliation audit event identity.
    ///
    /// The identity is domain-separated from the immutable plan rather than
    /// persisted as another field. Existing checkpoint schemas and graph
    /// references therefore remain byte-for-byte compatible across this
    /// runtime upgrade.
    #[must_use]
    pub fn reconciliation_event_id(&self) -> EventId {
        let mut material =
            b"stateknot.provider-native-agent.tool-reconciliation-event.v1\0".to_vec();
        material.extend_from_slice(self.invocation_id.as_uuid().as_bytes());
        material.extend_from_slice(self.attempt_id.as_uuid().as_bytes());
        material.extend_from_slice(self.attempt_terminal_event_id.as_uuid().as_bytes());
        material.extend_from_slice(self.action_digest.as_bytes());
        material.extend_from_slice(self.policy_evidence_digest.as_bytes());
        let digest = Digest::sha256(material);

        let mut bytes = [0_u8; 16];
        bytes[..6].copy_from_slice(&self.attempt_terminal_event_id.as_uuid().as_bytes()[..6]);
        bytes[6..].copy_from_slice(&digest.as_bytes()[..10]);
        bytes[6] = (bytes[6] & 0x0f) | 0x70;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        EventId::from_uuid(uuid::Uuid::from_bytes(bytes))
            .expect("derived reconciliation event bytes are a valid UUIDv7")
    }

    /// Returns the action digest approved by policy.
    #[must_use]
    pub const fn action_digest(&self) -> Digest {
        self.action_digest
    }

    /// Returns the exact policy evidence digest.
    #[must_use]
    pub const fn policy_evidence_digest(&self) -> Digest {
        self.policy_evidence_digest
    }
}

/// Compact ledger references for one completed model/tool turn.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderNativeCompletedTurn {
    model_invocation_id: InvocationId,
    tool_invocation_ids: Vec<InvocationId>,
}

impl ProviderNativeCompletedTurn {
    /// Returns the terminal model invocation for this completed turn or repair marker.
    #[must_use]
    pub const fn model_invocation_id(&self) -> InvocationId {
        self.model_invocation_id
    }

    /// Returns tool invocation identities in provider proposal order.
    #[must_use]
    pub fn tool_invocation_ids(&self) -> &[InvocationId] {
        &self.tool_invocation_ids
    }

    /// Returns whether this model turn is a durable output-repair marker.
    ///
    /// Repair markers deliberately retain no invalid model payload in the
    /// checkpoint. The exact terminal outcome remains in the immutable model ledger
    /// named by [`Self::model_invocation_id`].
    #[must_use]
    pub fn requires_output_repair(&self) -> bool {
        self.tool_invocation_ids.is_empty()
    }
}

/// Current durable phase of the prebuilt agent graph.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderNativeAgentPhase {
    /// A model request is ready to prepare or recover.
    Model {
        /// Stable identities chosen before provider I/O.
        plan: ProviderNativeModelPlan,
    },
    /// An approved ordered tool batch is ready to execute or recover.
    Tools {
        /// Committed tool-calling model invocation.
        model_invocation_id: InvocationId,
        /// Exact ordered tool plans in provider proposal order.
        plans: Vec<ProviderNativeToolPlan>,
    },
}

/// Bounded checkpoint state for the provider-native agent graph.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderNativeAgentState {
    contract_digest: Digest,
    input_message_id: MessageId,
    completed_turns: Vec<ProviderNativeCompletedTurn>,
    phase: ProviderNativeAgentPhase,
}

impl ProviderNativeAgentState {
    /// Returns the exact composition contract digest.
    #[must_use]
    pub const fn contract_digest(&self) -> Digest {
        self.contract_digest
    }

    /// Returns the stable user-message identity used in every request.
    #[must_use]
    pub const fn input_message_id(&self) -> MessageId {
        self.input_message_id
    }

    /// Returns completed turns represented only by invocation references.
    #[must_use]
    pub fn completed_turns(&self) -> &[ProviderNativeCompletedTurn] {
        &self.completed_turns
    }

    /// Returns the exact current execution phase.
    #[must_use]
    pub const fn phase(&self) -> &ProviderNativeAgentPhase {
        &self.phase
    }
}

/// Compiled schemas, state machine, and declarative graph for one agent version.
#[derive(Clone)]
pub struct ProviderNativeAgentGraph {
    descriptor: AgentDescriptor,
    policy: Arc<dyn AgentToolPolicy>,
    accounting: Arc<dyn AgentInvocationAccounting>,
    input_security_label: SecurityLabel,
    contract_digest: Digest,
    state_schema: SchemaReference,
    state_schema_document: Value,
    graph: CompiledGraph,
    model_node_id: NodeId,
    tools_node_id: NodeId,
    tools_route_id: RouteId,
    output_repair_instruction: Option<Instruction>,
}

impl ProviderNativeAgentGraph {
    /// Maximum tool invocation references retained across one checkpoint.
    pub const MAX_CHECKPOINT_INVOCATION_REFERENCES: u64 = MAX_CHECKPOINT_INVOCATION_REFERENCES;

    /// Compiles the supported v1 provider-native execution subset.
    ///
    /// V1 deliberately rejects tool-call emulated final output. Model-native
    /// structured output supports finite durable repair turns. Sequential
    /// Tools and bounded parallel read-only Tools are supported without
    /// allowing writes or completion timing to reorder the durable transcript.
    pub fn compile(
        descriptor: AgentDescriptor,
        graph_identity: CapabilityIdentity,
        reducer_identity: CapabilityIdentity,
        state_schema_id: SchemaId,
        input_security_label: SecurityLabel,
        policy: Arc<dyn AgentToolPolicy>,
        accounting: Arc<dyn AgentInvocationAccounting>,
    ) -> Result<Self, ProviderNativeAgentGraphBuildError> {
        validate_supported_execution(&descriptor)?;
        let output_repair_instruction = build_output_repair_instruction(&descriptor)?;
        let contract_digest = composition_digest(
            &descriptor,
            policy.reference(),
            accounting.reference(),
            &input_security_label,
        )?;
        let maximum_turns = descriptor.execution().max_model_turns().get();
        let maximum_calls = descriptor.execution().max_tool_calls_per_turn().get();
        let maximum_references = maximum_turns
            .checked_mul(maximum_calls)
            .ok_or(ProviderNativeAgentGraphBuildError::CheckpointReferenceLimit)?;
        if maximum_references > MAX_CHECKPOINT_INVOCATION_REFERENCES {
            return Err(ProviderNativeAgentGraphBuildError::CheckpointReferenceLimit);
        }

        let (state_schema, state_schema_document) = build_state_schema(
            state_schema_id,
            contract_digest,
            maximum_turns,
            maximum_calls,
            descriptor.execution().max_output_repair_turns().get(),
        )?;
        let reducer = GraphReducerReference::new(reducer_identity, contract_digest);
        let model_node_id = NodeId::new(PROVIDER_NATIVE_MODEL_NODE_ID)
            .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?;
        let tools_node_id = NodeId::new(PROVIDER_NATIVE_TOOLS_NODE_ID)
            .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?;
        let tools_route_id = RouteId::new(PROVIDER_NATIVE_TOOLS_ROUTE_ID)
            .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?;
        let model_continue_to = if output_repair_instruction.is_some() {
            Some(
                ReadyNodes::try_new([model_node_id.clone()])
                    .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?,
            )
        } else {
            None
        };
        let model = GraphNode::new(
            model_node_id.clone(),
            model_continue_to,
            GraphRoutes::try_new([GraphRoute::new(
                tools_route_id.clone(),
                ReadyNodes::try_new([tools_node_id.clone()])
                    .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?,
            )
            .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?])
            .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?,
            None,
            true,
        )
        .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?;
        let tools = GraphNode::new(
            tools_node_id.clone(),
            Some(
                ReadyNodes::try_new([model_node_id.clone()])
                    .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?,
            ),
            GraphRoutes::empty(),
            None,
            false,
        )
        .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?;
        let maximum_supersteps = maximum_turns
            .checked_mul(2)
            .ok_or(ProviderNativeAgentGraphBuildError::SuperstepLimit)?;
        let graph = CompiledGraph::compile(
            graph_identity,
            descriptor.input_schema().clone(),
            state_schema.clone(),
            state_schema.clone(),
            descriptor.output_schema().clone(),
            reducer,
            ReadyNodes::try_new([model_node_id.clone()])
                .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?,
            [model, tools],
            GraphExecutionLimits::new(
                Superstep::new(maximum_supersteps)
                    .map_err(|_| ProviderNativeAgentGraphBuildError::SuperstepLimit)?,
                1,
            )
            .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?,
        )?;
        Ok(Self {
            descriptor,
            policy,
            accounting,
            input_security_label,
            contract_digest,
            state_schema,
            state_schema_document,
            graph,
            model_node_id,
            tools_node_id,
            tools_route_id,
            output_repair_instruction,
        })
    }

    /// Returns the exact compiled graph.
    #[must_use]
    pub const fn graph(&self) -> &CompiledGraph {
        &self.graph
    }

    /// Returns the immutable agent descriptor snapshot.
    #[must_use]
    pub const fn descriptor(&self) -> &AgentDescriptor {
        &self.descriptor
    }

    /// Returns the exact graph composition checksum.
    #[must_use]
    pub const fn contract_digest(&self) -> Digest {
        self.contract_digest
    }

    /// Registers the generated state/update schema before the registry freezes.
    pub fn register_schema(
        &self,
        builder: &mut JsonSchemaRegistryBuilder,
    ) -> Result<(), JsonSchemaRegistryError> {
        builder.register(
            self.state_schema.clone(),
            self.state_schema_document.clone(),
        )
    }

    /// Registers the graph, reducer, and both executable nodes into one frozen
    /// deployment snapshot.
    ///
    /// The supplied schema registry must be the exact registry used to create
    /// `builder` and must already contain the generated state schema, the
    /// descriptor input/output/tool schemas, and the standard invocation event
    /// schema. Registry closure is rechecked again when the builder freezes.
    pub fn register_executable(
        &self,
        builder: &mut ExecutableGraphRegistryBuilder,
        store: PostgresStore,
        invocation_executor: DurableInvocationExecutor,
        schemas: JsonSchemaRegistry,
    ) -> Result<(), ProviderNativeAgentRegistrationError> {
        let (invocation_event_schema, _) = standard_invocation_execution_event_schema()
            .map_err(|_| ProviderNativeAgentRegistrationError::EmbeddedEventSchema)?;
        if !schemas.contains(&self.state_schema)
            || !schemas.contains(self.descriptor.input_schema())
            || !schemas.contains(self.descriptor.output_schema())
            || !schemas.contains(&invocation_event_schema)
            || self.descriptor.tools().iter().any(|tool| {
                !schemas.contains(tool.input_schema()) || !schemas.contains(tool.output_schema())
            })
        {
            return Err(ProviderNativeAgentRegistrationError::MissingSchema);
        }
        let definition = Arc::new(self.clone());
        let shared = Arc::new(ProviderNativeBindings {
            definition: Arc::clone(&definition),
            store,
            invocation_executor,
            schemas,
            invocation_event_schema,
        });
        let reducer: Arc<dyn GraphReducer> = Arc::new(ProviderNativeReducer {
            reference: self.graph.reducer().clone(),
            graph: Arc::clone(&definition),
        });
        builder.register_reducer(reducer)?;
        builder.register_node(Arc::new(ProviderNativeModelNode {
            graph: self.graph.reference(),
            node_id: self.model_node_id.clone(),
            shared: Arc::clone(&shared),
        }))?;
        builder.register_node(Arc::new(ProviderNativeToolsNode {
            graph: self.graph.reference(),
            node_id: self.tools_node_id.clone(),
            shared,
        }))?;
        builder.register_graph(self.graph.clone())?;
        Ok(())
    }

    /// Generates a fresh bounded initial state before durable admission.
    pub fn initial_state(&self) -> Result<CheckpointState, ProviderNativeAgentStateError> {
        self.encode_checkpoint_state(&ProviderNativeAgentState {
            contract_digest: self.contract_digest,
            input_message_id: MessageId::generate(),
            completed_turns: Vec::new(),
            phase: ProviderNativeAgentPhase::Model {
                plan: ProviderNativeModelPlan::generate(),
            },
        })
    }

    /// Decodes and revalidates a checkpoint against this exact compiled
    /// composition contract.
    ///
    /// This is the supported inspection boundary for recovery tooling; callers
    /// never need to duplicate the generated schema or state invariants.
    pub fn restore_state(
        &self,
        state: &CheckpointState,
    ) -> Result<ProviderNativeAgentState, ProviderNativeAgentStateError> {
        if state.schema() != &self.state_schema {
            return Err(ProviderNativeAgentStateError::Integrity);
        }
        let decoded = decode_state(state.data())?;
        validate_state(&decoded, self)?;
        Ok(decoded)
    }

    fn encode_checkpoint_state(
        &self,
        state: &ProviderNativeAgentState,
    ) -> Result<CheckpointState, ProviderNativeAgentStateError> {
        validate_state(state, self)?;
        let value = serde_json::to_value(state)
            .map_err(|_| ProviderNativeAgentStateError::Serialization)?;
        let bounded = BoundedJson::try_from_value(value)
            .map_err(|_| ProviderNativeAgentStateError::ResourceLimit)?;
        CheckpointState::new(self.state_schema.clone(), bounded)
            .map_err(|_| ProviderNativeAgentStateError::Integrity)
    }

    fn encode_update(
        &self,
        state: &ProviderNativeAgentState,
    ) -> Result<NodeStateUpdate, ProviderNativeAgentNodeError> {
        validate_state(state, self).map_err(|_| ProviderNativeAgentNodeError::InvalidState)?;
        let value =
            serde_json::to_value(state).map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        let bounded = BoundedJson::try_from_value(value)
            .map_err(|_| ProviderNativeAgentNodeError::InvalidState)?;
        NodeStateUpdate::new(self.state_schema.clone(), bounded)
            .map_err(|_| ProviderNativeAgentNodeError::Integrity)
    }

    fn decode_checkpoint(
        &self,
        state: &CheckpointState,
    ) -> Result<ProviderNativeAgentState, ProviderNativeAgentNodeError> {
        if state.schema() != &self.state_schema {
            return Err(ProviderNativeAgentNodeError::InvalidState);
        }
        let decoded =
            decode_state(state.data()).map_err(|_| ProviderNativeAgentNodeError::InvalidState)?;
        validate_state(&decoded, self).map_err(|_| ProviderNativeAgentNodeError::InvalidState)?;
        Ok(decoded)
    }
}

impl fmt::Debug for ProviderNativeAgentGraph {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderNativeAgentGraph")
            .field("graph", &self.graph.reference())
            .field("agent", self.descriptor.metadata().identity())
            .field("policy", self.policy.reference())
            .field("accounting", self.accounting.reference())
            .field("contract_digest", &self.contract_digest)
            .finish_non_exhaustive()
    }
}

/// Read-only lifecycle evidence recovery for one compiled provider-native
/// graph.
///
/// The provider rebuilds cumulative accounting from the invocation ledgers and
/// revalidates the terminal model output against the exact base checkpoint. It
/// never invokes a model, tool, or policy. Unknown external outcomes and model
/// failures without a complete usage snapshot remain unavailable for explicit
/// reconciliation instead of being reported as zero-cost terminal results.
#[derive(Clone)]
pub struct ProviderNativeAgentLifecycleEvidence {
    definition: Arc<ProviderNativeAgentGraph>,
    store: PostgresStore,
}

impl ProviderNativeAgentLifecycleEvidence {
    /// Binds one immutable graph composition to its durable evidence store.
    #[must_use]
    pub fn new(definition: ProviderNativeAgentGraph, store: PostgresStore) -> Self {
        Self {
            definition: Arc::new(definition),
            store,
        }
    }

    async fn terminal_evidence_inner(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> Result<GraphTerminalEvidence, GraphLifecycleEvidenceError> {
        let (stored, checkpoint, state) = self
            .load_context(context.provenance(), context.graph(), context.checkpoint())
            .await?;
        if context.output_schema() != self.definition.descriptor.output_schema() {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        let ProviderNativeAgentPhase::Model { plan } = state.phase() else {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        };
        let (transcript, prior_usage, output_repair_ordinal) = reconstruct_transcript(
            &self.store,
            checkpoint.tenant_id(),
            checkpoint.run_id(),
            &state,
            &self.definition,
        )
        .await
        .map_err(evidence_node_error)?;
        let expected_request = build_model_request(
            &stored,
            &self.definition,
            state.input_message_id(),
            transcript,
            &prior_usage,
            output_repair_ordinal,
        )
        .map_err(evidence_node_error)?;
        let invocation = self
            .store
            .load_model_invocation(
                checkpoint.tenant_id(),
                checkpoint.run_id(),
                plan.invocation_id(),
            )
            .await
            .map_err(|error| evidence_store_error(&error))?;
        validate_terminal_model_binding(
            &invocation,
            plan,
            &checkpoint,
            &self.definition.model_node_id,
            &expected_request,
        )?;
        let response = match invocation.state() {
            ModelInvocationState::Committed { response }
                if response.finish_reason() == ModelFinishReason::Completed =>
            {
                response
            }
            _ => return Err(GraphLifecycleEvidenceError::Corrupt),
        };
        let output = completed_json_output(response).map_err(evidence_node_error)?;
        let terminal = NodeTerminalOutput::new(context.output_schema().clone(), output)
            .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;
        if terminal.digest() != context.output_digest() {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        let delta = model_usage(&invocation, response, self.definition.accounting.as_ref())
            .map_err(evidence_node_error)?;
        let usage = prior_usage
            .checked_accumulate(&delta)
            .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;
        let intent = stored.admission().intent();
        Ok(GraphTerminalEvidence::new(
            intent.descriptor().clone(),
            intent.request().clone(),
            intent.budget().clone(),
            AgentArtifacts::empty(),
            usage,
        ))
    }

    async fn failure_evidence_inner(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> Result<GraphFailureEvidence, GraphLifecycleEvidenceError> {
        if context.blockers().in_flight() != 0 || context.blockers().unsupported() != 0 {
            return Err(GraphLifecycleEvidenceError::Unavailable);
        }
        let (_, checkpoint, state) = self
            .load_context(context.provenance(), context.graph(), context.checkpoint())
            .await?;
        let usage =
            recover_failure_usage(&self.store, &self.definition, &checkpoint, &state).await?;
        let failure = load_current_node_failure(&self.store, &checkpoint).await?;
        Ok(GraphFailureEvidence::new(failure, usage))
    }

    async fn cancellation_evidence_inner(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> Result<GraphCancellationEvidence, GraphLifecycleEvidenceError> {
        let (_, checkpoint, state) = self
            .load_context(context.provenance(), context.graph(), context.checkpoint())
            .await?;
        let usage =
            recover_failure_usage(&self.store, &self.definition, &checkpoint, &state).await?;
        Ok(GraphCancellationEvidence::new(usage))
    }

    async fn load_context(
        &self,
        provenance: &stateknot_core::AgentResultProvenance,
        graph: &GraphReference,
        head: &stateknot_core::CheckpointHead,
    ) -> Result<
        (
            stateknot_store_postgres::StoredAgentAdmission,
            Checkpoint,
            ProviderNativeAgentState,
        ),
        GraphLifecycleEvidenceError,
    > {
        if graph != &self.definition.graph.reference()
            || head.graph() != graph
            || provenance.tenant_id() != head.tenant_id()
            || provenance.run_id() != head.run_id()
        {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        let checkpoint = self
            .store
            .load_checkpoint(head.tenant_id(), head.run_id(), head.checkpoint_id())
            .await
            .map_err(|error| evidence_store_error(&error))?;
        if checkpoint.head() != *head {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        let stored = self
            .store
            .load_agent_admission(head.tenant_id(), head.run_id())
            .await
            .map_err(|error| evidence_store_error(&error))?;
        validate_admission(&stored, &self.definition)
            .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;
        if stored.admission().intent().provenance() != provenance {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        let state = self
            .definition
            .restore_state(checkpoint.state())
            .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;
        Ok((stored, checkpoint, state))
    }
}

impl GraphLifecycleEvidenceProvider for ProviderNativeAgentLifecycleEvidence {
    fn terminal_evidence(
        &self,
        context: GraphTerminalEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphTerminalEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(self.terminal_evidence_inner(context))
    }

    fn failure_evidence(
        &self,
        context: GraphFailureEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphFailureEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(self.failure_evidence_inner(context))
    }

    fn cancellation_evidence(
        &self,
        context: GraphCancellationEvidenceContext,
    ) -> BoxFuture<'_, Result<GraphCancellationEvidence, GraphLifecycleEvidenceError>> {
        Box::pin(self.cancellation_evidence_inner(context))
    }
}

impl fmt::Debug for ProviderNativeAgentLifecycleEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderNativeAgentLifecycleEvidence")
            .field("graph", &self.definition.graph.reference())
            .finish_non_exhaustive()
    }
}

/// Startup failure while compiling a prebuilt provider-native graph.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProviderNativeAgentGraphBuildError {
    /// Only model-native structured output is implemented in v1.
    #[error("provider-native agent graph requires model-native structured output")]
    StructuredOutputUnsupported,
    /// The Agent already occupies the framework-owned repair instruction name.
    #[error("provider-native agent instructions use the reserved output-repair identity")]
    OutputRepairInstructionConflict,
    /// Adding the required repair instruction would exceed the request bound.
    #[error("provider-native agent has no instruction capacity for output repair")]
    OutputRepairInstructionCapacity,
    /// Tools may occur before repair, so their history needs disabled selection.
    #[error("output repair with Tools requires model support for tool selection none")]
    OutputRepairToolSelectionUnsupported,
    /// One tool node cannot bind more than 256 terminal invocation revisions.
    #[error("provider-native agent graph allows at most 256 tool calls per turn")]
    ToolCallsPerTurnLimit,
    /// Worst-case compact invocation references would exceed the checkpoint bound.
    #[error("provider-native agent graph exceeds the 4096 checkpoint invocation-reference limit")]
    CheckpointReferenceLimit,
    /// The derived graph cannot fit the durable superstep range.
    #[error("provider-native agent graph superstep limit is not representable")]
    SuperstepLimit,
    /// A static framework-owned identifier or edge declaration was invalid.
    #[error("provider-native agent graph embedded definition is invalid")]
    StaticDefinition,
    /// The generated state schema could not be constructed.
    #[error("provider-native agent graph state schema is invalid")]
    StateSchema,
    /// Contract integrity bytes could not be canonicalized.
    #[error("provider-native agent graph contract cannot be canonicalized")]
    Canonicalization,
    /// Declarative graph compilation rejected the generated definition.
    #[error(transparent)]
    Graph(#[from] GraphCompileError),
}

/// Startup failure while binding one compiled provider-native graph.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProviderNativeAgentRegistrationError {
    /// The embedded invocation journal schema could not be loaded.
    #[error("provider-native agent invocation event schema is invalid")]
    EmbeddedEventSchema,
    /// One required digest-pinned schema was absent from the frozen snapshot.
    #[error("provider-native agent requires schemas missing from the registry")]
    MissingSchema,
    /// The executable registry rejected a duplicate, conflict, or invalid binding.
    #[error(transparent)]
    Registry(#[from] ExecutableGraphRegistryError),
}

/// Invalid generated or restored provider-native checkpoint state.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ProviderNativeAgentStateError {
    /// State named another compiled composition contract.
    #[error("provider-native agent state contract digest does not match")]
    ContractMismatch,
    /// State exceeded configured turn or call bounds.
    #[error("provider-native agent state exceeds its configured bounds")]
    Bounds,
    /// State phase and completed history are not semantically coherent.
    #[error("provider-native agent state transition is invalid")]
    Transition,
    /// State JSON could not be serialized.
    #[error("provider-native agent state cannot be serialized")]
    Serialization,
    /// State exceeded the bounded JSON resource limit.
    #[error("provider-native agent state exceeds the checkpoint resource limit")]
    ResourceLimit,
    /// State checksum construction failed.
    #[error("provider-native agent checkpoint state integrity failed")]
    Integrity,
}

#[derive(Clone)]
struct ProviderNativeReducer {
    reference: GraphReducerReference,
    graph: Arc<ProviderNativeAgentGraph>,
}

impl GraphReducer for ProviderNativeReducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.reference
    }

    fn reduce(
        &self,
        state: &BoundedJson,
        updates: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        let current = decode_state(state).map_err(|_| GraphReducerError::Rejected)?;
        validate_state(&current, &self.graph).map_err(|_| GraphReducerError::Rejected)?;
        if updates.is_empty() {
            return Ok(state.clone());
        }
        if updates.len() != 1 {
            return Err(GraphReducerError::Rejected);
        }
        let update = updates[0];
        let next = decode_state(update.update().data()).map_err(|_| GraphReducerError::Rejected)?;
        validate_state(&next, &self.graph).map_err(|_| GraphReducerError::Rejected)?;
        validate_transition(update.node_id(), &current, &next, &self.graph)
            .map_err(|_| GraphReducerError::Rejected)?;
        Ok(update.update().data().clone())
    }
}

#[derive(Clone)]
struct ProviderNativeBindings {
    definition: Arc<ProviderNativeAgentGraph>,
    store: PostgresStore,
    invocation_executor: DurableInvocationExecutor,
    schemas: JsonSchemaRegistry,
    invocation_event_schema: SchemaReference,
}

struct ProviderNativeModelNode {
    graph: GraphReference,
    node_id: NodeId,
    shared: Arc<ProviderNativeBindings>,
}

impl GraphNodeExecutor for ProviderNativeModelNode {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            Box::pin(self.execute_model(context))
                .await
                .map_err(ProviderNativeAgentExecutionError::into_graph_error)
        })
    }
}

impl ProviderNativeModelNode {
    async fn execute_model(
        &self,
        context: GraphNodeContext,
    ) -> Result<GraphNodeExecution, ProviderNativeAgentExecutionError> {
        let observation = Box::pin(self.observe_model(&context)).await?;
        let usage = observation.usage.clone();
        self.finish_model(observation)
            .await
            .map_err(|source| ProviderNativeAgentExecutionError::observed(source, usage))
    }

    async fn observe_model(
        &self,
        context: &GraphNodeContext,
    ) -> Result<ProviderNativeModelObservation, ProviderNativeAgentExecutionError> {
        let state = self
            .shared
            .definition
            .decode_checkpoint(context.checkpoint().state())?;
        let plan = match &state.phase {
            ProviderNativeAgentPhase::Model { plan } => plan.clone(),
            ProviderNativeAgentPhase::Tools { .. } => {
                return Err(ProviderNativeAgentNodeError::InvalidState.into());
            }
        };
        let stored = self
            .shared
            .store
            .load_agent_admission(
                context.attempt().fence().tenant_id(),
                context.attempt().fence().run_id(),
            )
            .await
            .map_err(ProviderNativeAgentNodeError::Store)?;
        validate_admission(&stored, &self.shared.definition)?;
        let (transcript, prior_usage, output_repair_ordinal) = reconstruct_transcript(
            &self.shared.store,
            context.attempt().fence().tenant_id(),
            context.attempt().fence().run_id(),
            &state,
            &self.shared.definition,
        )
        .await?;
        let request = build_model_request(
            &stored,
            &self.shared.definition,
            state.input_message_id,
            transcript,
            &prior_usage,
            output_repair_ordinal,
        )?;
        let expected_intent = ModelInvocationIntent::new(
            context.attempt().activation().clone(),
            plan.invocation_id,
            self.shared.definition.descriptor.model().clone(),
            request,
        )
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        let invocation = self
            .load_or_prepare_model(context, &plan, expected_intent)
            .await?;
        let invocation = self
            .dispatch_or_recover_model(context, &plan, invocation)
            .await?;
        let ProviderNativeTerminalModelEvidence {
            response,
            usage,
            failure,
        } = self.terminal_model_evidence(&invocation)?;
        if let Some(failure) = failure {
            return Err(ProviderNativeAgentExecutionError::observed(
                ProviderNativeAgentNodeError::ModelFailed(failure),
                usage,
            ));
        }
        let binding = NodeInvocationBinding::from_model(&invocation)
            .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        let bindings = NodeInvocationBindings::try_new(context.attempt().activation(), [binding])
            .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        Ok(ProviderNativeModelObservation {
            state,
            stored,
            plan,
            response,
            usage,
            bindings,
        })
    }

    fn terminal_model_evidence(
        &self,
        invocation: &ModelInvocation,
    ) -> Result<ProviderNativeTerminalModelEvidence, ProviderNativeAgentNodeError> {
        let evidence = match invocation.state() {
            ModelInvocationState::Committed { response } => ProviderNativeTerminalModelEvidence {
                response: Some(response.clone()),
                usage: model_usage(
                    invocation,
                    response,
                    self.shared.definition.accounting.as_ref(),
                )?,
                failure: None,
            },
            ModelInvocationState::Failed { error }
                if repairable_model_output_error(error)
                    && self
                        .shared
                        .definition
                        .descriptor
                        .execution()
                        .max_output_repair_turns()
                        != ExecutionCount::ZERO =>
            {
                ProviderNativeTerminalModelEvidence {
                    response: None,
                    usage: failed_model_usage(
                        invocation,
                        error,
                        self.shared.definition.accounting.as_ref(),
                        true,
                    )?,
                    failure: None,
                }
            }
            ModelInvocationState::Failed { error } => {
                let usage = failed_model_usage(
                    invocation,
                    error,
                    self.shared.definition.accounting.as_ref(),
                    false,
                )?;
                ProviderNativeTerminalModelEvidence {
                    response: None,
                    usage,
                    failure: Some(Box::new(error.failure().clone())),
                }
            }
            ModelInvocationState::Prepared | ModelInvocationState::Executing { .. } => {
                return Err(ProviderNativeAgentNodeError::UncertainInvocation);
            }
        };
        Ok(evidence)
    }

    async fn load_or_prepare_model(
        &self,
        context: &GraphNodeContext,
        plan: &ProviderNativeModelPlan,
        expected_intent: ModelInvocationIntent,
    ) -> Result<ModelInvocation, ProviderNativeAgentNodeError> {
        let invocation = match self
            .shared
            .store
            .load_model_invocation(
                context.attempt().fence().tenant_id(),
                context.attempt().fence().run_id(),
                plan.invocation_id,
            )
            .await
        {
            Ok(invocation) => {
                if invocation.intent() != &expected_intent {
                    return Err(ProviderNativeAgentNodeError::Integrity);
                }
                invocation
            }
            Err(StoreError::ModelInvocationNotFound) => {
                let payload = invocation_payload(
                    &self.shared.schemas,
                    &self.shared.invocation_event_schema,
                    "model-invocation-prepared",
                    "model_invocation_prepared",
                    "model",
                    plan.invocation_id,
                    plan.attempt_id,
                    expected_intent.intent_digest(),
                )?;
                let append = worker_append(
                    context.attempt().fence(),
                    context.attempt().journal_head().clone(),
                    plan.prepared_event_id,
                    payload,
                )?;
                self.shared
                    .store
                    .prepare_model_invocation(append, expected_intent)
                    .await
                    .map_err(ProviderNativeAgentNodeError::Store)?
                    .invocation()
                    .clone()
            }
            Err(error) => return Err(ProviderNativeAgentNodeError::Store(error)),
        };
        Ok(invocation)
    }

    async fn dispatch_or_recover_model(
        &self,
        context: &GraphNodeContext,
        plan: &ProviderNativeModelPlan,
        invocation: ModelInvocation,
    ) -> Result<ModelInvocation, ProviderNativeAgentNodeError> {
        let invocation = match invocation.state() {
            ModelInvocationState::Prepared => {
                let events = InvocationAttemptEventIds::new(
                    plan.attempt_start_event_id,
                    plan.attempt_terminal_event_id,
                )
                .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
                let handoff = ModelAttemptHandoff::new(
                    context.attempt().fence().clone(),
                    invocation,
                    plan.attempt_id,
                    events,
                    context.cancellation().clone(),
                    None,
                )
                .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
                match self
                    .shared
                    .invocation_executor
                    .execute_model(handoff)
                    .await
                    .map_err(|_| ProviderNativeAgentNodeError::InvocationExecutor)?
                {
                    ModelAttemptOutcome::Dispatched { invocation, .. }
                    | ModelAttemptOutcome::Recovered { invocation } => invocation,
                }
            }
            ModelInvocationState::Executing { .. } => {
                Err(ProviderNativeAgentNodeError::UncertainInvocation)?
            }
            ModelInvocationState::Committed { .. } | ModelInvocationState::Failed { .. } => {
                invocation
            }
        };
        Ok(invocation)
    }

    async fn finish_model(
        &self,
        mut observation: ProviderNativeModelObservation,
    ) -> Result<GraphNodeExecution, ProviderNativeAgentNodeError> {
        let Some(response) = observation.response.as_ref() else {
            return self.output_repair_execution(observation);
        };
        match response.finish_reason() {
            ModelFinishReason::Completed => self.completed_model_execution(observation),
            ModelFinishReason::ToolCalls if output_repair_active(&observation.state) => {
                self.output_repair_execution(observation)
            }
            ModelFinishReason::ToolCalls => {
                let turn = observation
                    .state
                    .completed_turns
                    .len()
                    .checked_add(1)
                    .ok_or(ProviderNativeAgentNodeError::Budget)?;
                let maximum_turns = usize::try_from(
                    self.shared
                        .definition
                        .descriptor
                        .execution()
                        .max_model_turns()
                        .get(),
                )
                .map_err(|_| ProviderNativeAgentNodeError::Budget)?;
                if turn >= maximum_turns {
                    return Err(ProviderNativeAgentNodeError::Budget);
                }
                let plans = self
                    .authorize_tool_plans(&observation.stored, &observation.plan, response)
                    .await?;
                observation.state.phase = ProviderNativeAgentPhase::Tools {
                    model_invocation_id: observation.plan.invocation_id,
                    plans,
                };
                let update = self.shared.definition.encode_update(&observation.state)?;
                Ok(GraphNodeExecution::new(
                    NodeStateChange::Update { update },
                    NodeControl::Route {
                        route_id: self.shared.definition.tools_route_id.clone(),
                    },
                    observation.bindings,
                    observation.usage,
                ))
            }
            _ => Err(ProviderNativeAgentNodeError::IncompleteModelOutput),
        }
    }

    fn completed_model_execution(
        &self,
        observation: ProviderNativeModelObservation,
    ) -> Result<GraphNodeExecution, ProviderNativeAgentNodeError> {
        let response = observation
            .response
            .as_ref()
            .ok_or(ProviderNativeAgentNodeError::Integrity)?;
        let output = match completed_json_output(response).and_then(|output| {
            self.shared
                .schemas
                .validate_bounded(self.shared.definition.descriptor.output_schema(), &output)
                .map(|()| output)
                .map_err(|_| ProviderNativeAgentNodeError::InvalidModelOutput)
        }) {
            Ok(output) => output,
            Err(ProviderNativeAgentNodeError::InvalidModelOutput) => {
                return self.output_repair_execution(observation);
            }
            Err(error) => return Err(error),
        };
        let terminal = NodeTerminalOutput::new(
            self.shared.definition.descriptor.output_schema().clone(),
            output,
        )
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        Ok(GraphNodeExecution::new(
            NodeStateChange::Unchanged,
            NodeControl::Terminal { output: terminal },
            observation.bindings,
            observation.usage,
        ))
    }

    fn output_repair_execution(
        &self,
        mut observation: ProviderNativeModelObservation,
    ) -> Result<GraphNodeExecution, ProviderNativeAgentNodeError> {
        let completed_repairs = output_repair_ordinal(&observation.state)?;
        let maximum_repairs = self
            .shared
            .definition
            .descriptor
            .execution()
            .max_output_repair_turns();
        if completed_repairs >= maximum_repairs {
            return Err(if maximum_repairs == ExecutionCount::ZERO {
                ProviderNativeAgentNodeError::InvalidModelOutput
            } else {
                ProviderNativeAgentNodeError::OutputRepairExhausted
            });
        }
        let completed_model_turns = u64::try_from(observation.state.completed_turns.len())
            .map_err(|_| ProviderNativeAgentNodeError::Budget)?
            .checked_add(1)
            .ok_or(ProviderNativeAgentNodeError::Budget)?;
        if completed_model_turns
            >= self
                .shared
                .definition
                .descriptor
                .execution()
                .max_model_turns()
                .get()
        {
            return Err(ProviderNativeAgentNodeError::Budget);
        }
        observation
            .state
            .completed_turns
            .push(ProviderNativeCompletedTurn {
                model_invocation_id: observation.plan.invocation_id,
                tool_invocation_ids: Vec::new(),
            });
        observation.state.phase = ProviderNativeAgentPhase::Model {
            plan: ProviderNativeModelPlan::generate(),
        };
        let update = self.shared.definition.encode_update(&observation.state)?;
        Ok(GraphNodeExecution::new(
            NodeStateChange::Update { update },
            NodeControl::Continue,
            observation.bindings,
            observation.usage,
        ))
    }

    async fn authorize_tool_plans(
        &self,
        stored: &stateknot_store_postgres::StoredAgentAdmission,
        plan: &ProviderNativeModelPlan,
        response: &stateknot_core::ModelResponse,
    ) -> Result<Vec<ProviderNativeToolPlan>, ProviderNativeAgentNodeError> {
        let mut plans = Vec::with_capacity(response.tool_call_count());
        for (index, proposal) in response.tool_calls().enumerate() {
            if proposal.provider_call_id().is_none() {
                return Err(ProviderNativeAgentNodeError::InvalidModelOutput);
            }
            let digest = action_digest(
                stored.admission().digest(),
                plan.invocation_id,
                index,
                proposal,
            )?;
            let policy_context = AgentToolPolicyContext {
                agent: self
                    .shared
                    .definition
                    .descriptor
                    .metadata()
                    .identity()
                    .clone(),
                admission_digest: stored.admission().digest(),
                model_invocation_id: plan.invocation_id,
                proposal_index: index,
                proposal: proposal.clone(),
                action_digest: digest,
            };
            let decision = self
                .shared
                .definition
                .policy
                .evaluate(policy_context)
                .await
                .map_err(|_| ProviderNativeAgentNodeError::Policy)?;
            match decision {
                AgentToolPolicyDecision::Allow { evidence_digest } => {
                    let proposal_index = u16::try_from(index)
                        .map_err(|_| ProviderNativeAgentNodeError::InvalidModelOutput)?;
                    plans.push(ProviderNativeToolPlan::generate(
                        proposal_index,
                        digest,
                        evidence_digest,
                    ));
                }
                AgentToolPolicyDecision::Deny { failure } => {
                    return Err(ProviderNativeAgentNodeError::PolicyDenied(Box::new(
                        failure,
                    )));
                }
            }
        }
        if plans.is_empty() {
            return Err(ProviderNativeAgentNodeError::InvalidModelOutput);
        }
        Ok(plans)
    }
}

struct ProviderNativeModelObservation {
    state: ProviderNativeAgentState,
    stored: stateknot_store_postgres::StoredAgentAdmission,
    plan: ProviderNativeModelPlan,
    response: Option<stateknot_core::ModelResponse>,
    usage: BudgetUsage,
    bindings: NodeInvocationBindings,
}

struct ProviderNativeTerminalModelEvidence {
    response: Option<stateknot_core::ModelResponse>,
    usage: BudgetUsage,
    failure: Option<Box<Failure>>,
}

struct ProviderNativeToolsNode {
    graph: GraphReference,
    node_id: NodeId,
    shared: Arc<ProviderNativeBindings>,
}

impl GraphNodeExecutor for ProviderNativeToolsNode {
    fn graph(&self) -> &GraphReference {
        &self.graph
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn execute(
        &self,
        context: GraphNodeContext,
    ) -> BoxFuture<'_, Result<GraphNodeExecution, GraphNodeExecutionError>> {
        Box::pin(async move {
            Box::pin(self.execute_tools(context))
                .await
                .map_err(ProviderNativeAgentExecutionError::into_graph_error)
        })
    }
}

impl ProviderNativeToolsNode {
    #[allow(clippy::too_many_lines)]
    async fn execute_tools(
        &self,
        context: GraphNodeContext,
    ) -> Result<GraphNodeExecution, ProviderNativeAgentExecutionError> {
        let mut observation = self.observe_tools(&context).await?;

        let mut head = context.attempt().journal_head().clone();
        let mut outcomes = Vec::with_capacity(observation.plans.len());
        let mut bindings = Vec::with_capacity(observation.plans.len());
        let mut usage = BudgetUsage::zero();
        let proposals = observation
            .response
            .tool_calls()
            .cloned()
            .collect::<Vec<_>>();
        let concurrency = self
            .shared
            .definition
            .descriptor
            .execution()
            .tool_concurrency();
        let mut index = 0_usize;
        while index < proposals.len() {
            let parallel_end = match concurrency {
                AgentToolConcurrency::Sequential {} => index + 1,
                AgentToolConcurrency::ParallelReadOnly { max_concurrency } => {
                    let maximum = usize::try_from(max_concurrency.get())
                        .map_err(|_| ProviderNativeAgentNodeError::Budget)?;
                    let mut end = index;
                    while end < proposals.len()
                        && end.saturating_sub(index) < maximum
                        && self.proposal_tool_risk(&proposals[end])? == ToolRisk::ReadOnly
                    {
                        end = end.saturating_add(1);
                    }
                    end.max(index + 1)
                }
            };
            let steps = if parallel_end.saturating_sub(index) > 1 {
                Box::pin(self.execute_read_only_tool_wave(
                    &context,
                    &observation.stored,
                    observation.model_invocation_id,
                    head.clone(),
                    index,
                    &proposals[index..parallel_end],
                    &observation.plans[index..parallel_end],
                ))
                .await
                .map_err(|source| {
                    ProviderNativeAgentExecutionError::observed(source, usage.clone())
                })?
            } else {
                vec![
                    self.execute_tool_plan(
                        &context,
                        &observation.stored,
                        observation.model_invocation_id,
                        head.clone(),
                        index,
                        &proposals[index],
                        &observation.plans[index],
                    )
                    .await
                    .map_err(|source| {
                        ProviderNativeAgentExecutionError::observed(source, usage.clone())
                    })?,
                ]
            };
            for step in steps {
                usage = usage.checked_accumulate(&step.usage).map_err(|_| {
                    ProviderNativeAgentExecutionError::observed(
                        ProviderNativeAgentNodeError::Budget,
                        usage.clone(),
                    )
                })?;
                bindings.push(step.binding);
                head = latest_journal_head(head, step.journal_head).map_err(|source| {
                    ProviderNativeAgentExecutionError::observed(source, usage.clone())
                })?;
                outcomes.push(step.outcome);
            }
            index = parallel_end;
        }
        ModelTranscriptTurn::new(observation.response, outcomes).map_err(|_| {
            ProviderNativeAgentExecutionError::observed(
                ProviderNativeAgentNodeError::Integrity,
                usage.clone(),
            )
        })?;
        observation
            .state
            .completed_turns
            .push(ProviderNativeCompletedTurn {
                model_invocation_id: observation.model_invocation_id,
                tool_invocation_ids: observation
                    .plans
                    .iter()
                    .map(|plan| plan.invocation_id)
                    .collect(),
            });
        observation.state.phase = ProviderNativeAgentPhase::Model {
            plan: ProviderNativeModelPlan::generate(),
        };
        let update = self
            .shared
            .definition
            .encode_update(&observation.state)
            .map_err(|source| ProviderNativeAgentExecutionError::observed(source, usage.clone()))?;
        let bindings = NodeInvocationBindings::try_new(context.attempt().activation(), bindings)
            .map_err(|_| {
                ProviderNativeAgentExecutionError::observed(
                    ProviderNativeAgentNodeError::Integrity,
                    usage.clone(),
                )
            })?;
        Ok(GraphNodeExecution::new(
            NodeStateChange::Update { update },
            NodeControl::Continue,
            bindings,
            usage,
        ))
    }

    async fn observe_tools(
        &self,
        context: &GraphNodeContext,
    ) -> Result<ProviderNativeToolsObservation, ProviderNativeAgentNodeError> {
        let state = self
            .shared
            .definition
            .decode_checkpoint(context.checkpoint().state())?;
        let (model_invocation_id, plans) = match &state.phase {
            ProviderNativeAgentPhase::Tools {
                model_invocation_id,
                plans,
            } => (*model_invocation_id, plans.clone()),
            ProviderNativeAgentPhase::Model { .. } => {
                return Err(ProviderNativeAgentNodeError::InvalidState);
            }
        };
        let stored = self
            .shared
            .store
            .load_agent_admission(
                context.attempt().fence().tenant_id(),
                context.attempt().fence().run_id(),
            )
            .await
            .map_err(ProviderNativeAgentNodeError::Store)?;
        validate_admission(&stored, &self.shared.definition)?;
        let model = self
            .shared
            .store
            .load_model_invocation(
                context.attempt().fence().tenant_id(),
                context.attempt().fence().run_id(),
                model_invocation_id,
            )
            .await
            .map_err(ProviderNativeAgentNodeError::Store)?;
        let response = match model.state() {
            ModelInvocationState::Committed { response }
                if response.finish_reason() == ModelFinishReason::ToolCalls =>
            {
                response.clone()
            }
            _ => return Err(ProviderNativeAgentNodeError::Integrity),
        };
        if response.tool_call_count() != plans.len() {
            return Err(ProviderNativeAgentNodeError::Integrity);
        }
        Ok(ProviderNativeToolsObservation {
            state,
            stored,
            model_invocation_id,
            plans,
            response,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_tool_plan(
        &self,
        context: &GraphNodeContext,
        stored: &stateknot_store_postgres::StoredAgentAdmission,
        model_invocation_id: InvocationId,
        head: stateknot_core::JournalHead,
        index: usize,
        proposal: &ModelToolCallProposal,
        plan: &ProviderNativeToolPlan,
    ) -> Result<ProviderNativeToolStep, ProviderNativeAgentNodeError> {
        let (provider_call_id, expected_intent) =
            self.prepare_tool_intent(context, stored, model_invocation_id, index, proposal, plan)?;
        let invocation = self
            .load_or_prepare_tool(context, head, plan, expected_intent)
            .await?;
        let invocation = self
            .dispatch_or_recover_tool(context, plan, invocation)
            .await?;
        self.completed_tool_step(provider_call_id, &invocation)
    }

    fn completed_tool_step(
        &self,
        provider_call_id: stateknot_core::ModelProviderToolCallId,
        invocation: &ToolInvocation,
    ) -> Result<ProviderNativeToolStep, ProviderNativeAgentNodeError> {
        let outcome = match invocation.state() {
            ToolInvocationState::Committed { result } => {
                ModelToolOutcome::succeeded(provider_call_id, result.clone())
            }
            ToolInvocationState::Failed { error } => {
                ModelToolOutcome::failed(provider_call_id, error)
            }
            ToolInvocationState::Prepared
            | ToolInvocationState::Executing { .. }
            | ToolInvocationState::Unknown { .. } => {
                return Err(ProviderNativeAgentNodeError::UncertainInvocation);
            }
        };
        let usage = tool_usage(invocation, self.shared.definition.accounting.as_ref())?;
        let binding = NodeInvocationBinding::from_tool(invocation)
            .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        Ok(ProviderNativeToolStep {
            outcome,
            binding,
            journal_head: invocation.journal_head().clone(),
            usage,
        })
    }

    fn proposal_tool_risk(
        &self,
        proposal: &ModelToolCallProposal,
    ) -> Result<ToolRisk, ProviderNativeAgentNodeError> {
        self.shared
            .definition
            .descriptor
            .tools()
            .iter()
            .find(|tool| tool.metadata().identity() == proposal.tool())
            .map(|tool| tool.semantics().risk())
            .ok_or(ProviderNativeAgentNodeError::InvalidModelOutput)
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_read_only_tool_wave(
        &self,
        context: &GraphNodeContext,
        stored: &stateknot_store_postgres::StoredAgentAdmission,
        model_invocation_id: InvocationId,
        mut head: stateknot_core::JournalHead,
        first_index: usize,
        proposals: &[ModelToolCallProposal],
        plans: &[ProviderNativeToolPlan],
    ) -> Result<Vec<ProviderNativeToolStep>, ProviderNativeAgentNodeError> {
        if proposals.len() < 2 || proposals.len() != plans.len() {
            return Err(ProviderNativeAgentNodeError::Integrity);
        }
        let mut entries = Vec::with_capacity(proposals.len());
        let mut launch_error = None;
        for (offset, (proposal, plan)) in proposals.iter().zip(plans).enumerate() {
            let index = first_index
                .checked_add(offset)
                .ok_or(ProviderNativeAgentNodeError::Budget)?;
            let prepared = Box::pin(self.launch_read_only_tool(
                context,
                stored,
                model_invocation_id,
                head.clone(),
                index,
                proposal,
                plan,
            ))
            .await;
            match prepared {
                Ok((entry, next_head)) => {
                    entries.push(entry);
                    head = next_head;
                }
                Err(error) => {
                    launch_error = Some(error);
                    break;
                }
            }
        }

        let settled = self.settle_read_only_tool_wave(entries).await;
        if let Some(error) = launch_error {
            // Every already-started provider call was awaited and offered to
            // the ordered terminal commit path before the launch error wins.
            let _ = settled;
            return Err(error);
        }
        settled
    }

    #[allow(clippy::too_many_arguments)]
    async fn launch_read_only_tool(
        &self,
        context: &GraphNodeContext,
        stored: &stateknot_store_postgres::StoredAgentAdmission,
        model_invocation_id: InvocationId,
        mut head: stateknot_core::JournalHead,
        index: usize,
        proposal: &ModelToolCallProposal,
        plan: &ProviderNativeToolPlan,
    ) -> Result<
        (ProviderNativeToolWaveEntry, stateknot_core::JournalHead),
        ProviderNativeAgentNodeError,
    > {
        let (provider_call_id, expected_intent) =
            self.prepare_tool_intent(context, stored, model_invocation_id, index, proposal, plan)?;
        if expected_intent.descriptor().semantics().risk() != ToolRisk::ReadOnly {
            return Err(ProviderNativeAgentNodeError::Integrity);
        }
        let invocation = self
            .load_or_prepare_tool(context, head.clone(), plan, expected_intent)
            .await?;
        head = latest_journal_head(head, invocation.journal_head().clone())?;
        if !matches!(invocation.state(), ToolInvocationState::Prepared) {
            let invocation = self
                .dispatch_or_recover_tool(context, plan, invocation)
                .await?;
            head = latest_journal_head(head, invocation.journal_head().clone())?;
            return Ok((
                ProviderNativeToolWaveEntry::Resolved {
                    provider_call_id,
                    invocation: Box::new(invocation),
                },
                head,
            ));
        }
        let events = InvocationAttemptEventIds::new(
            plan.attempt_start_event_id,
            plan.attempt_terminal_event_id,
        )
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        let handoff = ToolAttemptHandoff::new(
            context.attempt().fence().clone(),
            invocation,
            plan.attempt_id,
            events,
            context.cancellation().clone(),
            None,
        )
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        match self
            .shared
            .invocation_executor
            .start_tool_attempt(handoff)
            .await
            .map_err(|_| ProviderNativeAgentNodeError::InvocationExecutor)?
        {
            ToolAttemptStartOutcome::Started(dispatch) => {
                head = latest_journal_head(head, dispatch.invocation().journal_head().clone())?;
                let executor = self.shared.invocation_executor.clone();
                Ok((
                    ProviderNativeToolWaveEntry::Dispatched {
                        provider_call_id,
                        task: AbortOnDropToolTask::new(tokio::spawn(async move {
                            executor.dispatch_started_tool(*dispatch).await
                        })),
                    },
                    head,
                ))
            }
            ToolAttemptStartOutcome::Recovered { invocation } => {
                let invocation = self
                    .dispatch_or_recover_tool(context, plan, *invocation)
                    .await?;
                head = latest_journal_head(head, invocation.journal_head().clone())?;
                Ok((
                    ProviderNativeToolWaveEntry::Resolved {
                        provider_call_id,
                        invocation: Box::new(invocation),
                    },
                    head,
                ))
            }
        }
    }

    async fn settle_read_only_tool_wave(
        &self,
        entries: Vec<ProviderNativeToolWaveEntry>,
    ) -> Result<Vec<ProviderNativeToolStep>, ProviderNativeAgentNodeError> {
        let mut steps = Vec::with_capacity(entries.len());
        let mut first_error = None;
        for entry in entries {
            let result = match entry {
                ProviderNativeToolWaveEntry::Resolved {
                    provider_call_id,
                    invocation,
                } => self.completed_tool_step(provider_call_id, invocation.as_ref()),
                ProviderNativeToolWaveEntry::Dispatched {
                    provider_call_id,
                    task,
                } => match task.join().await {
                    Ok(terminal) => match self
                        .shared
                        .invocation_executor
                        .commit_tool_terminal(terminal)
                        .await
                    {
                        Ok(
                            ToolAttemptOutcome::Dispatched { invocation, .. }
                            | ToolAttemptOutcome::Recovered { invocation },
                        ) => self.completed_tool_step(provider_call_id, &invocation),
                        Err(_) => Err(ProviderNativeAgentNodeError::InvocationExecutor),
                    },
                    Err(_) => Err(ProviderNativeAgentNodeError::InvocationExecutor),
                },
            };
            match result {
                Ok(step) if first_error.is_none() => steps.push(step),
                Err(error) if first_error.is_none() => first_error = Some(error),
                Ok(_) | Err(_) => {}
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(steps)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_tool_intent(
        &self,
        context: &GraphNodeContext,
        stored: &stateknot_store_postgres::StoredAgentAdmission,
        model_invocation_id: InvocationId,
        index: usize,
        proposal: &ModelToolCallProposal,
        plan: &ProviderNativeToolPlan,
    ) -> Result<
        (
            stateknot_core::ModelProviderToolCallId,
            ToolInvocationIntent,
        ),
        ProviderNativeAgentNodeError,
    > {
        if usize::from(plan.proposal_index) != index {
            return Err(ProviderNativeAgentNodeError::Integrity);
        }
        let expected_action = action_digest(
            stored.admission().digest(),
            model_invocation_id,
            index,
            proposal,
        )?;
        if expected_action != plan.action_digest {
            return Err(ProviderNativeAgentNodeError::Integrity);
        }
        let provider_call_id = proposal
            .provider_call_id()
            .cloned()
            .ok_or(ProviderNativeAgentNodeError::InvalidModelOutput)?;
        let descriptor = self
            .shared
            .definition
            .descriptor
            .tools()
            .iter()
            .find(|tool| tool.metadata().identity() == proposal.tool())
            .cloned()
            .ok_or(ProviderNativeAgentNodeError::InvalidModelOutput)?;
        self.shared
            .schemas
            .validate_bounded(descriptor.input_schema(), proposal.arguments())
            .map_err(|_| ProviderNativeAgentNodeError::InvalidModelOutput)?;
        let input = ToolInput::new(
            descriptor.input_schema().clone(),
            proposal.arguments().clone(),
        )
        .map_err(|_| ProviderNativeAgentNodeError::InvalidModelOutput)?;
        let expected_intent = ToolInvocationIntent::new(
            context.attempt().activation().clone(),
            plan.invocation_id,
            descriptor.clone(),
            input,
            descriptor.limits().clone(),
        )
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        Ok((provider_call_id, expected_intent))
    }

    async fn load_or_prepare_tool(
        &self,
        context: &GraphNodeContext,
        head: stateknot_core::JournalHead,
        plan: &ProviderNativeToolPlan,
        expected_intent: ToolInvocationIntent,
    ) -> Result<ToolInvocation, ProviderNativeAgentNodeError> {
        let invocation = match self
            .shared
            .store
            .load_tool_invocation(
                context.attempt().fence().tenant_id(),
                context.attempt().fence().run_id(),
                plan.invocation_id,
            )
            .await
        {
            Ok(invocation) => {
                if invocation.intent() != &expected_intent {
                    return Err(ProviderNativeAgentNodeError::Integrity);
                }
                invocation
            }
            Err(StoreError::ToolInvocationNotFound) => {
                let payload = invocation_payload(
                    &self.shared.schemas,
                    &self.shared.invocation_event_schema,
                    "tool-invocation-prepared",
                    "tool_invocation_prepared",
                    "tool",
                    plan.invocation_id,
                    plan.attempt_id,
                    expected_intent.intent_digest(),
                )?;
                let append = worker_append(
                    context.attempt().fence(),
                    head,
                    plan.prepared_event_id,
                    payload,
                )?;
                self.shared
                    .store
                    .prepare_tool_invocation(append, expected_intent)
                    .await
                    .map_err(ProviderNativeAgentNodeError::Store)?
                    .invocation()
                    .clone()
            }
            Err(error) => return Err(ProviderNativeAgentNodeError::Store(error)),
        };
        Ok(invocation)
    }

    async fn dispatch_or_recover_tool(
        &self,
        context: &GraphNodeContext,
        plan: &ProviderNativeToolPlan,
        invocation: ToolInvocation,
    ) -> Result<ToolInvocation, ProviderNativeAgentNodeError> {
        let invocation = match invocation.state() {
            ToolInvocationState::Prepared => {
                let events = InvocationAttemptEventIds::new(
                    plan.attempt_start_event_id,
                    plan.attempt_terminal_event_id,
                )
                .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
                let handoff = ToolAttemptHandoff::new(
                    context.attempt().fence().clone(),
                    invocation,
                    plan.attempt_id,
                    events,
                    context.cancellation().clone(),
                    None,
                )
                .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
                match self
                    .shared
                    .invocation_executor
                    .execute_tool(handoff)
                    .await
                    .map_err(|_| ProviderNativeAgentNodeError::InvocationExecutor)?
                {
                    ToolAttemptOutcome::Dispatched { invocation, .. }
                    | ToolAttemptOutcome::Recovered { invocation } => invocation,
                }
            }
            ToolInvocationState::Executing { .. } => {
                return Err(ProviderNativeAgentNodeError::UncertainInvocation);
            }
            ToolInvocationState::Unknown { .. }
            | ToolInvocationState::Committed { .. }
            | ToolInvocationState::Failed { .. } => invocation,
        };

        let ToolInvocationState::Unknown { .. } = invocation.state() else {
            return Ok(invocation);
        };
        if !invocation
            .intent()
            .descriptor()
            .semantics()
            .supports_status_query()
        {
            return Err(ProviderNativeAgentNodeError::UncertainInvocation);
        }
        let available = self
            .shared
            .invocation_executor
            .supports_tool_reconciliation(invocation.intent().descriptor())
            .map_err(|_| ProviderNativeAgentNodeError::InvocationExecutor)?;
        if !available {
            return Err(ProviderNativeAgentNodeError::UncertainInvocation);
        }
        let handoff = ToolReconciliationAttemptHandoff::new(
            context.attempt().fence().clone(),
            invocation,
            plan.reconciliation_event_id(),
            context.cancellation().clone(),
        )
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
        let invocation = match self
            .shared
            .invocation_executor
            .reconcile_tool(handoff)
            .await
            .map_err(ProviderNativeAgentNodeError::Reconciliation)?
        {
            ToolReconciliationAttemptOutcome::Resolved { invocation, .. } => invocation,
            ToolReconciliationAttemptOutcome::Pending {
                retry_after,
                invocation: _,
            } => {
                return Err(ProviderNativeAgentNodeError::ReconciliationPending { retry_after });
            }
        };
        Ok(invocation)
    }
}

struct ProviderNativeToolsObservation {
    state: ProviderNativeAgentState,
    stored: stateknot_store_postgres::StoredAgentAdmission,
    model_invocation_id: InvocationId,
    plans: Vec<ProviderNativeToolPlan>,
    response: stateknot_core::ModelResponse,
}

struct ProviderNativeToolStep {
    outcome: ModelToolOutcome,
    binding: NodeInvocationBinding,
    journal_head: stateknot_core::JournalHead,
    usage: BudgetUsage,
}

enum ProviderNativeToolWaveEntry {
    Resolved {
        provider_call_id: stateknot_core::ModelProviderToolCallId,
        invocation: Box<ToolInvocation>,
    },
    Dispatched {
        provider_call_id: stateknot_core::ModelProviderToolCallId,
        task: AbortOnDropToolTask,
    },
}

/// Prevents a cancelled Graph node from detaching provider I/O into the
/// runtime. A started attempt remains durable for fenced supervision, but the
/// process-local provider future must not outlive its owning Tool wave.
struct AbortOnDropToolTask {
    task: tokio::task::JoinHandle<ToolTerminalCommitHandoff>,
}

impl AbortOnDropToolTask {
    const fn new(task: tokio::task::JoinHandle<ToolTerminalCommitHandoff>) -> Self {
        Self { task }
    }

    async fn join(mut self) -> Result<ToolTerminalCommitHandoff, tokio::task::JoinError> {
        (&mut self.task).await
    }
}

impl Drop for AbortOnDropToolTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn latest_journal_head(
    current: stateknot_core::JournalHead,
    candidate: stateknot_core::JournalHead,
) -> Result<stateknot_core::JournalHead, ProviderNativeAgentNodeError> {
    if current.tenant_id() != candidate.tenant_id() || current.run_id() != candidate.run_id() {
        return Err(ProviderNativeAgentNodeError::Integrity);
    }
    match current.sequence().cmp(&candidate.sequence()) {
        std::cmp::Ordering::Less => Ok(candidate),
        std::cmp::Ordering::Greater => Ok(current),
        std::cmp::Ordering::Equal if current == candidate => Ok(current),
        std::cmp::Ordering::Equal => Err(ProviderNativeAgentNodeError::Integrity),
    }
}

fn validate_admission(
    stored: &stateknot_store_postgres::StoredAgentAdmission,
    definition: &ProviderNativeAgentGraph,
) -> Result<(), ProviderNativeAgentNodeError> {
    let intent = stored.admission().intent();
    if intent.descriptor() != &definition.descriptor
        || intent.graph() != &definition.graph.reference()
    {
        return Err(ProviderNativeAgentNodeError::Integrity);
    }
    Ok(())
}

async fn reconstruct_transcript(
    store: &PostgresStore,
    tenant_id: &stateknot_core::TenantId,
    run_id: stateknot_core::RunId,
    state: &ProviderNativeAgentState,
    definition: &ProviderNativeAgentGraph,
) -> Result<(ModelTranscript, BudgetUsage, ExecutionCount), ProviderNativeAgentNodeError> {
    let mut turns = Vec::with_capacity(state.completed_turns.len());
    let mut usage = BudgetUsage::zero();
    let mut output_repair_turns = 0_u64;
    for turn in &state.completed_turns {
        let model = store
            .load_model_invocation(tenant_id, run_id, turn.model_invocation_id)
            .await
            .map_err(ProviderNativeAgentNodeError::Store)?;
        if turn.requires_output_repair() {
            let model_delta = match model.state() {
                ModelInvocationState::Committed { response } => {
                    let valid_repair_marker = match response.finish_reason() {
                        ModelFinishReason::Completed => true,
                        ModelFinishReason::ToolCalls => output_repair_turns != 0,
                        _ => false,
                    };
                    if !valid_repair_marker {
                        return Err(ProviderNativeAgentNodeError::Integrity);
                    }
                    model_usage(&model, response, definition.accounting.as_ref())?
                }
                ModelInvocationState::Failed { error } if repairable_model_output_error(error) => {
                    failed_model_usage(&model, error, definition.accounting.as_ref(), true)?
                }
                _ => return Err(ProviderNativeAgentNodeError::Integrity),
            };
            usage = usage
                .checked_accumulate(&model_delta)
                .map_err(|_| ProviderNativeAgentNodeError::Budget)?;
            output_repair_turns = output_repair_turns
                .checked_add(1)
                .ok_or(ProviderNativeAgentNodeError::Budget)?;
            continue;
        }
        let ModelInvocationState::Committed { response } = model.state() else {
            return Err(ProviderNativeAgentNodeError::Integrity);
        };
        if output_repair_turns != 0 || response.finish_reason() != ModelFinishReason::ToolCalls {
            return Err(ProviderNativeAgentNodeError::Integrity);
        }
        let model_delta = model_usage(&model, response, definition.accounting.as_ref())?;
        usage = usage
            .checked_accumulate(&model_delta)
            .map_err(|_| ProviderNativeAgentNodeError::Budget)?;
        if response.tool_call_count() != turn.tool_invocation_ids.len() {
            return Err(ProviderNativeAgentNodeError::Integrity);
        }
        let mut outcomes = Vec::with_capacity(turn.tool_invocation_ids.len());
        for (proposal, invocation_id) in response.tool_calls().zip(&turn.tool_invocation_ids) {
            let provider_call_id = proposal
                .provider_call_id()
                .cloned()
                .ok_or(ProviderNativeAgentNodeError::Integrity)?;
            let tool = store
                .load_tool_invocation(tenant_id, run_id, *invocation_id)
                .await
                .map_err(ProviderNativeAgentNodeError::Store)?;
            let outcome = match tool.state() {
                ToolInvocationState::Committed { result } => {
                    ModelToolOutcome::succeeded(provider_call_id, result.clone())
                }
                ToolInvocationState::Failed { error } => {
                    ModelToolOutcome::failed(provider_call_id, error)
                }
                ToolInvocationState::Prepared
                | ToolInvocationState::Executing { .. }
                | ToolInvocationState::Unknown { .. } => {
                    return Err(ProviderNativeAgentNodeError::UncertainInvocation);
                }
            };
            let tool_delta = tool_usage(&tool, definition.accounting.as_ref())?;
            usage = usage
                .checked_accumulate(&tool_delta)
                .map_err(|_| ProviderNativeAgentNodeError::Budget)?;
            outcomes.push(outcome);
        }
        turns.push(
            ModelTranscriptTurn::new(response.clone(), outcomes)
                .map_err(|_| ProviderNativeAgentNodeError::Integrity)?,
        );
    }
    let transcript =
        ModelTranscript::try_new(turns).map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
    Ok((transcript, usage, ExecutionCount::new(output_repair_turns)))
}

fn validate_terminal_model_binding(
    invocation: &ModelInvocation,
    plan: &ProviderNativeModelPlan,
    checkpoint: &Checkpoint,
    model_node_id: &NodeId,
    expected_request: &ModelRequest,
) -> Result<(), GraphLifecycleEvidenceError> {
    let intent = invocation.intent();
    if intent.invocation_id() != plan.invocation_id()
        || invocation.attempt_id() != Some(plan.attempt_id())
        || intent.activation().base_checkpoint() != &checkpoint.head()
        || intent.activation().node_id() != model_node_id
        || intent.request() != expected_request
    {
        return Err(GraphLifecycleEvidenceError::Corrupt);
    }
    Ok(())
}

async fn recover_failure_usage(
    store: &PostgresStore,
    definition: &ProviderNativeAgentGraph,
    checkpoint: &Checkpoint,
    state: &ProviderNativeAgentState,
) -> Result<BudgetUsage, GraphLifecycleEvidenceError> {
    let (_, usage, _) = reconstruct_transcript(
        store,
        checkpoint.tenant_id(),
        checkpoint.run_id(),
        state,
        definition,
    )
    .await
    .map_err(evidence_node_error)?;
    match state.phase() {
        ProviderNativeAgentPhase::Model { plan } => {
            recover_model_phase_usage(store, definition, checkpoint, plan, usage).await
        }
        ProviderNativeAgentPhase::Tools {
            model_invocation_id,
            plans,
        } => {
            recover_tools_phase_usage(
                store,
                definition,
                checkpoint,
                *model_invocation_id,
                plans,
                usage,
            )
            .await
        }
    }
}

async fn recover_model_phase_usage(
    store: &PostgresStore,
    definition: &ProviderNativeAgentGraph,
    checkpoint: &Checkpoint,
    plan: &ProviderNativeModelPlan,
    usage: BudgetUsage,
) -> Result<BudgetUsage, GraphLifecycleEvidenceError> {
    let invocation = match store
        .load_model_invocation(
            checkpoint.tenant_id(),
            checkpoint.run_id(),
            plan.invocation_id(),
        )
        .await
    {
        Ok(invocation) => invocation,
        Err(StoreError::ModelInvocationNotFound) => return Ok(usage),
        Err(error) => return Err(evidence_store_error(&error)),
    };
    if invocation.intent().activation().base_checkpoint() != &checkpoint.head()
        || invocation.intent().activation().node_id() != &definition.model_node_id
        || invocation
            .attempt_id()
            .is_some_and(|id| id != plan.attempt_id())
    {
        return Err(GraphLifecycleEvidenceError::Corrupt);
    }
    let delta = match invocation.state() {
        ModelInvocationState::Prepared => return Ok(usage),
        ModelInvocationState::Executing { .. } => {
            return Err(GraphLifecycleEvidenceError::Unavailable);
        }
        ModelInvocationState::Committed { response } => {
            model_usage(&invocation, response, definition.accounting.as_ref())
                .map_err(evidence_node_error)?
        }
        ModelInvocationState::Failed { error } => {
            if error.usage().is_none() {
                return Err(GraphLifecycleEvidenceError::Unavailable);
            }
            failed_model_usage(
                &invocation,
                error,
                definition.accounting.as_ref(),
                definition.descriptor.execution().max_output_repair_turns() != ExecutionCount::ZERO
                    && repairable_model_output_error(error),
            )
            .map_err(evidence_node_error)?
        }
    };
    usage
        .checked_accumulate(&delta)
        .map_err(|_| GraphLifecycleEvidenceError::Corrupt)
}

async fn recover_tools_phase_usage(
    store: &PostgresStore,
    definition: &ProviderNativeAgentGraph,
    checkpoint: &Checkpoint,
    model_invocation_id: InvocationId,
    plans: &[ProviderNativeToolPlan],
    mut usage: BudgetUsage,
) -> Result<BudgetUsage, GraphLifecycleEvidenceError> {
    let model = store
        .load_model_invocation(
            checkpoint.tenant_id(),
            checkpoint.run_id(),
            model_invocation_id,
        )
        .await
        .map_err(|error| evidence_store_error(&error))?;
    let response = match model.state() {
        ModelInvocationState::Committed { response }
            if response.finish_reason() == ModelFinishReason::ToolCalls =>
        {
            response
        }
        _ => return Err(GraphLifecycleEvidenceError::Corrupt),
    };
    if response.tool_call_count() != plans.len()
        || checkpoint.parent().is_none_or(|parent| {
            model.intent().activation().base_checkpoint() != parent
                || model.intent().activation().node_id() != &definition.model_node_id
        })
    {
        return Err(GraphLifecycleEvidenceError::Corrupt);
    }
    let model_delta = model_usage(&model, response, definition.accounting.as_ref())
        .map_err(evidence_node_error)?;
    usage = usage
        .checked_accumulate(&model_delta)
        .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;

    let mut undispatched = false;
    for plan in plans {
        let tool = match store
            .load_tool_invocation(
                checkpoint.tenant_id(),
                checkpoint.run_id(),
                plan.invocation_id(),
            )
            .await
        {
            Ok(tool) if !undispatched => tool,
            Ok(_) => return Err(GraphLifecycleEvidenceError::Corrupt),
            Err(StoreError::ToolInvocationNotFound) => {
                undispatched = true;
                continue;
            }
            Err(error) => return Err(evidence_store_error(&error)),
        };
        if tool.intent().activation().base_checkpoint() != &checkpoint.head()
            || tool.intent().activation().node_id() != &definition.tools_node_id
            || tool.attempt_id().is_some_and(|id| id != plan.attempt_id())
        {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        match tool.state() {
            ToolInvocationState::Prepared => undispatched = true,
            ToolInvocationState::Executing { .. } | ToolInvocationState::Unknown { .. } => {
                return Err(GraphLifecycleEvidenceError::Unavailable);
            }
            ToolInvocationState::Committed { .. } | ToolInvocationState::Failed { .. } => {
                let delta = tool_usage(&tool, definition.accounting.as_ref())
                    .map_err(evidence_node_error)?;
                usage = usage
                    .checked_accumulate(&delta)
                    .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;
            }
        }
    }
    Ok(usage)
}

async fn load_current_node_failure(
    store: &PostgresStore,
    checkpoint: &Checkpoint,
) -> Result<Failure, GraphLifecycleEvidenceError> {
    if checkpoint.ready_nodes().len() != 1 {
        return Err(GraphLifecycleEvidenceError::Corrupt);
    }
    let node_id = checkpoint
        .ready_nodes()
        .iter()
        .next()
        .cloned()
        .ok_or(GraphLifecycleEvidenceError::Corrupt)?;
    let activation = NodeActivation::for_ready_root(checkpoint, node_id)
        .map_err(|_| GraphLifecycleEvidenceError::Corrupt)?;
    let page_size = NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX)
        .map_err(|error| evidence_store_error(&error))?;
    let mut cursor = None;
    let mut latest = None;
    let mut observed = 0_usize;
    loop {
        let page = store
            .load_node_attempt_history_page(&activation, cursor.as_ref(), page_size)
            .await
            .map_err(|error| evidence_store_error(&error))?;
        observed = observed.saturating_add(page.records().len());
        if observed > 64 {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
        for attempt in page.records() {
            if let Some(failure) = attempt
                .completion()
                .and_then(|completion| completion.outcome().failure())
            {
                latest = Some(failure.clone());
            }
        }
        if !page.has_more() {
            break;
        }
        cursor = page.next_cursor();
        if cursor.is_none() {
            return Err(GraphLifecycleEvidenceError::Corrupt);
        }
    }
    latest.ok_or(GraphLifecycleEvidenceError::Unavailable)
}

fn evidence_node_error(error: ProviderNativeAgentNodeError) -> GraphLifecycleEvidenceError {
    match error {
        ProviderNativeAgentNodeError::Store(error) => evidence_store_error(&error),
        ProviderNativeAgentNodeError::UncertainInvocation => {
            GraphLifecycleEvidenceError::Unavailable
        }
        _ => GraphLifecycleEvidenceError::Corrupt,
    }
}

fn evidence_store_error(error: &StoreError) -> GraphLifecycleEvidenceError {
    if error.corrupt_record().is_some() {
        GraphLifecycleEvidenceError::Corrupt
    } else if error.is_retryable() {
        GraphLifecycleEvidenceError::TemporarilyUnavailable
    } else {
        GraphLifecycleEvidenceError::Unavailable
    }
}

fn build_model_request(
    stored: &stateknot_store_postgres::StoredAgentAdmission,
    definition: &ProviderNativeAgentGraph,
    input_message_id: MessageId,
    transcript: ModelTranscript,
    prior_usage: &BudgetUsage,
    output_repair_ordinal: ExecutionCount,
) -> Result<ModelRequest, ProviderNativeAgentNodeError> {
    let admission = stored.admission();
    let intent = admission.intent();
    let remaining = intent
        .budget()
        .remaining(prior_usage, admission.admitted_at())
        .map_err(|_| ProviderNativeAgentNodeError::Budget)?;
    let (max_input_tokens, max_output_tokens) = model_token_limits(
        definition.descriptor.model(),
        remaining.input_tokens(),
        remaining.output_tokens(),
    )?;
    let max_content_bytes = remaining
        .input_bytes()
        .min(ModelRequestLimits::HARD_MAX_CONTENT_BYTES);
    let limits = ModelRequestLimits::new(max_input_tokens, max_output_tokens, max_content_bytes)
        .map_err(|_| ProviderNativeAgentNodeError::Budget)?;
    let metadata =
        ContentMetadata::untrusted(ContentSource::User, definition.input_security_label.clone());
    let content = ContentPart::Json(JsonContent::new(
        intent.request().input().clone(),
        Some(intent.request().input_schema().clone()),
        metadata,
    ));
    let parts =
        MessageParts::try_new([content]).map_err(|_| ProviderNativeAgentNodeError::InvalidState)?;
    let message = Message::new(
        input_message_id,
        MessageRole::User,
        parts,
        MessageProvenance::new(
            intent.provenance().run_id(),
            stored.event().event_id(),
            MessageProducer::Principal {
                principal: intent.authority().principal().clone(),
            },
        ),
    )
    .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
    let repairing_output = output_repair_ordinal != ExecutionCount::ZERO;
    let historical_tools = if repairing_output {
        transcript
            .iter()
            .flat_map(|turn| turn.response().tool_calls())
            .map(|proposal| proposal.tool().clone())
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    let mut builder = ModelRequest::builder(limits)
        .message(message)
        .transcript(transcript)
        .text_output_format(Some(ModelTextOutputFormat::json_schema(
            definition.descriptor.output_schema().clone(),
        )));
    for instruction in definition.descriptor.instructions() {
        builder = builder.instruction(instruction.clone());
    }
    if repairing_output {
        let instruction = definition
            .output_repair_instruction
            .as_ref()
            .ok_or(ProviderNativeAgentNodeError::Integrity)?;
        builder = builder.instruction(instruction.clone());
    }
    if repairing_output || definition.descriptor.tools().is_empty() {
        for tool in definition.descriptor.tools() {
            if historical_tools.contains(tool.metadata().identity()) {
                builder = builder.tool(tool.clone());
            }
        }
        builder = builder
            .tool_selection(ModelToolSelection::none())
            .max_tool_calls_per_response(ExecutionCount::ZERO)
            .strict_tool_arguments(false);
    } else {
        for tool in definition.descriptor.tools() {
            builder = builder.tool(tool.clone());
        }
        builder = builder
            .tool_selection(ModelToolSelection::auto())
            .max_tool_calls_per_response(
                definition.descriptor.execution().max_tool_calls_per_turn(),
            )
            .strict_tool_arguments(true);
    }
    builder
        .build()
        .map_err(|_| ProviderNativeAgentNodeError::InvalidState)
}

fn model_token_limits(
    descriptor: &stateknot_core::ModelDescriptor,
    remaining_input: TokenCount,
    remaining_output: TokenCount,
) -> Result<(TokenCount, TokenCount), ProviderNativeAgentNodeError> {
    let published = descriptor.capabilities().token_limits();
    let mut input = published
        .max_input_tokens()
        .map_or(remaining_input, |maximum| maximum.min(remaining_input));
    let mut output = published
        .max_output_tokens()
        .map_or(remaining_output, |maximum| maximum.min(remaining_output));
    if input == TokenCount::ZERO || output == TokenCount::ZERO {
        return Err(ProviderNativeAgentNodeError::Budget);
    }
    if let Some(context) = published.max_context_tokens() {
        let sum = input
            .checked_add(output)
            .ok_or(ProviderNativeAgentNodeError::Budget)?;
        if sum > context {
            if context.get() < 2 {
                return Err(ProviderNativeAgentNodeError::Budget);
            }
            output = output.min(TokenCount::new(context.get() - 1));
            input = input.min(TokenCount::new(context.get() - output.get()));
        }
    }
    if input == TokenCount::ZERO || output == TokenCount::ZERO {
        return Err(ProviderNativeAgentNodeError::Budget);
    }
    Ok((input, output))
}

fn completed_json_output(
    response: &stateknot_core::ModelResponse,
) -> Result<BoundedJson, ProviderNativeAgentNodeError> {
    let mut content = response
        .output()
        .iter()
        .filter_map(ModelOutputItem::as_content);
    let Some(ContentPart::Json(json)) = content.next() else {
        return Err(ProviderNativeAgentNodeError::InvalidModelOutput);
    };
    if content.next().is_some() {
        return Err(ProviderNativeAgentNodeError::InvalidModelOutput);
    }
    Ok(json.value().clone())
}

fn output_repair_active(state: &ProviderNativeAgentState) -> bool {
    state
        .completed_turns
        .last()
        .is_some_and(ProviderNativeCompletedTurn::requires_output_repair)
}

fn output_repair_ordinal(
    state: &ProviderNativeAgentState,
) -> Result<ExecutionCount, ProviderNativeAgentNodeError> {
    let repairs = state
        .completed_turns
        .iter()
        .filter(|turn| turn.requires_output_repair())
        .count();
    Ok(ExecutionCount::new(
        u64::try_from(repairs).map_err(|_| ProviderNativeAgentNodeError::Budget)?,
    ))
}

fn repairable_model_output_error(error: &ModelError) -> bool {
    error.phase() == ModelErrorPhase::Response
        && error.failure().code().as_str() == "response.malformed"
        && error.usage().is_some()
}

fn model_usage(
    invocation: &ModelInvocation,
    response: &stateknot_core::ModelResponse,
    accounting: &dyn AgentInvocationAccounting,
) -> Result<BudgetUsage, ProviderNativeAgentNodeError> {
    let observed = response.usage();
    let input_bytes = serde_json_canonicalizer::to_vec(invocation.intent().request())
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)?
        .len();
    let builder = BudgetUsage::builder()
        .model_attempts(ExecutionCount::new(1))
        .model_turns(ExecutionCount::new(1))
        .input_tokens(observed.input_tokens())
        .cached_input_tokens(
            observed
                .cached_input_tokens()
                .unwrap_or_else(|| observed.input_tokens()),
        )
        .output_tokens(observed.output_tokens())
        .reasoning_tokens(
            observed
                .reasoning_tokens()
                .unwrap_or_else(|| observed.output_tokens()),
        )
        .input_bytes(ByteCount::new(input_bytes as u64))
        .output_bytes(response.inline_payload_bytes());
    let builder = match accounting.model_charge(invocation) {
        AgentInvocationCharge::Known(costs) => builder.known_costs(costs),
        AgentInvocationCharge::Unpriced => builder.unpriced_cost_events(ExecutionCount::new(1)),
    };
    builder
        .build()
        .map_err(|_| ProviderNativeAgentNodeError::Budget)
}

fn failed_model_usage(
    invocation: &ModelInvocation,
    error: &stateknot_core::ModelError,
    accounting: &dyn AgentInvocationAccounting,
    count_model_turn: bool,
) -> Result<BudgetUsage, ProviderNativeAgentNodeError> {
    let input_bytes = serde_json_canonicalizer::to_vec(invocation.intent().request())
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)?
        .len();
    let builder = BudgetUsage::builder()
        .model_attempts(ExecutionCount::new(1))
        .model_turns(if count_model_turn {
            ExecutionCount::new(1)
        } else {
            ExecutionCount::ZERO
        })
        .input_bytes(ByteCount::new(input_bytes as u64));
    let mut builder = match accounting.model_charge(invocation) {
        AgentInvocationCharge::Known(costs) => builder.known_costs(costs),
        AgentInvocationCharge::Unpriced => builder.unpriced_cost_events(ExecutionCount::new(1)),
    };
    if let Some(observed) = error.usage() {
        builder = builder
            .input_tokens(observed.input_tokens())
            .cached_input_tokens(
                observed
                    .cached_input_tokens()
                    .unwrap_or_else(|| observed.input_tokens()),
            )
            .output_tokens(observed.output_tokens())
            .reasoning_tokens(
                observed
                    .reasoning_tokens()
                    .unwrap_or_else(|| observed.output_tokens()),
            );
    }
    builder
        .build()
        .map_err(|_| ProviderNativeAgentNodeError::Budget)
}

fn tool_usage(
    invocation: &ToolInvocation,
    accounting: &dyn AgentInvocationAccounting,
) -> Result<BudgetUsage, ProviderNativeAgentNodeError> {
    let descriptor = invocation.intent().descriptor();
    let input_bytes =
        ByteCount::new(invocation.intent().input().value().stats().compact_bytes() as u64);
    let (output_bytes, artifact_bytes) = match invocation.state() {
        ToolInvocationState::Committed { result } => (
            ByteCount::new(result.output().stats().compact_bytes() as u64),
            result.artifacts().total_bytes(),
        ),
        ToolInvocationState::Failed { .. } => (ByteCount::ZERO, ByteCount::ZERO),
        ToolInvocationState::Prepared
        | ToolInvocationState::Executing { .. }
        | ToolInvocationState::Unknown { .. } => {
            return Err(ProviderNativeAgentNodeError::UncertainInvocation);
        }
    };
    let builder = BudgetUsage::builder()
        .tool_calls(ExecutionCount::new(1))
        .write_calls(if descriptor.semantics().risk() == ToolRisk::ReadOnly {
            ExecutionCount::ZERO
        } else {
            ExecutionCount::new(1)
        })
        .input_bytes(input_bytes)
        .output_bytes(output_bytes)
        .artifact_bytes(artifact_bytes);
    let builder = match accounting.tool_charge(invocation) {
        AgentInvocationCharge::Known(costs) => builder.known_costs(costs),
        AgentInvocationCharge::Unpriced => builder.unpriced_cost_events(ExecutionCount::new(1)),
    };
    builder
        .build()
        .map_err(|_| ProviderNativeAgentNodeError::Budget)
}

#[allow(clippy::too_many_arguments)]
fn invocation_payload(
    schemas: &JsonSchemaRegistry,
    schema: &SchemaReference,
    event_kind: &'static str,
    operation: &'static str,
    binding_kind: &'static str,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    intent_digest: Digest,
) -> Result<JournalPayload, ProviderNativeAgentNodeError> {
    let data = BoundedJson::try_from_value(json!({
        "operation": operation,
        "binding_kind": binding_kind,
        "invocation_id": invocation_id.to_string(),
        "attempt_id": attempt_id.to_string(),
        "intent_digest": digest_hex(intent_digest),
    }))
    .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
    schemas
        .validate_bounded(schema, &data)
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
    JournalPayload::new(
        schema.clone(),
        JournalEventKind::new(event_kind).map_err(|_| ProviderNativeAgentNodeError::Integrity)?,
        data,
    )
    .map_err(|_| ProviderNativeAgentNodeError::Integrity)
}

fn digest_hex(digest: Digest) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(Digest::SHA256_LEN * 2);
    for byte in digest.as_bytes() {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn worker_append(
    fence: &stateknot_core::RunFence,
    head: stateknot_core::JournalHead,
    event_id: EventId,
    payload: JournalPayload,
) -> Result<JournalAppend, ProviderNativeAgentNodeError> {
    let intent = JournalEventIntent::worker(
        fence.tenant_id().clone(),
        fence.run_id(),
        event_id,
        fence.clone(),
        payload,
    )
    .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
    JournalAppend::new(JournalExpectation::exact(head), intent)
        .map_err(|_| ProviderNativeAgentNodeError::Integrity)
}

#[allow(clippy::too_many_lines)]
fn node_execution_error(
    error: ProviderNativeAgentNodeError,
    usage: BudgetUsage,
) -> GraphNodeExecutionError {
    match &error {
        ProviderNativeAgentNodeError::ModelFailed(failure)
        | ProviderNativeAgentNodeError::PolicyDenied(failure) => {
            if let Ok(error) = GraphNodeExecutionError::new(failure.as_ref().clone(), usage.clone())
            {
                return error;
            }
        }
        _ => {}
    }
    let (category, code, message, advice) = match &error {
        ProviderNativeAgentNodeError::Store(source) if source.corrupt_record().is_some() => (
            FailureCategory::DataCorruption,
            "runtime.agent.corrupt_evidence",
            "Durable agent evidence failed integrity validation.",
            RetryAdvice::Never,
        ),
        ProviderNativeAgentNodeError::Store(source) if source.is_retryable() => (
            FailureCategory::DependencyUnavailable,
            "runtime.agent.store_unavailable",
            "Durable agent storage is temporarily unavailable.",
            RetryAdvice::SafeAfter {
                delay: DurationMillis::new(100).expect("static duration is valid"),
            },
        ),
        ProviderNativeAgentNodeError::UncertainInvocation => (
            FailureCategory::Conflict,
            "runtime.agent.invocation_uncertain",
            "An external invocation must be reconciled before the run can continue.",
            RetryAdvice::Never,
        ),
        ProviderNativeAgentNodeError::ReconciliationPending { retry_after } => (
            FailureCategory::DependencyUnavailable,
            "runtime.agent.reconciliation_pending",
            "The external invocation has no authoritative outcome yet.",
            RetryAdvice::SafeAfter {
                delay: *retry_after,
            },
        ),
        ProviderNativeAgentNodeError::Reconciliation(source) => {
            let advice = match source.retry_advice() {
                RetryAdvice::ReconcileFirst => RetryAdvice::Never,
                advice => advice,
            };
            (
                match advice {
                    RetryAdvice::SafeAfter { .. } => FailureCategory::DependencyUnavailable,
                    RetryAdvice::Never | RetryAdvice::ReconcileFirst => FailureCategory::Internal,
                },
                "runtime.agent.reconciliation_failed",
                "The external invocation could not be reconciled safely.",
                advice,
            )
        }
        ProviderNativeAgentNodeError::Budget => (
            FailureCategory::RateLimited,
            "runtime.agent.budget_exhausted",
            "The agent exhausted a finite execution budget.",
            RetryAdvice::Never,
        ),
        ProviderNativeAgentNodeError::InvalidModelOutput
        | ProviderNativeAgentNodeError::IncompleteModelOutput => (
            FailureCategory::InvalidInput,
            "runtime.agent.invalid_model_output",
            "The model did not produce a valid terminal output or tool turn.",
            RetryAdvice::Never,
        ),
        ProviderNativeAgentNodeError::OutputRepairExhausted => (
            FailureCategory::InvalidInput,
            "runtime.agent.output_repair_exhausted",
            "The model exhausted the configured structured-output repair turns.",
            RetryAdvice::Never,
        ),
        ProviderNativeAgentNodeError::Policy => (
            FailureCategory::DependencyUnavailable,
            "runtime.agent.policy_unavailable",
            "The pinned agent tool policy is unavailable.",
            RetryAdvice::SafeAfter {
                delay: DurationMillis::new(100).expect("static duration is valid"),
            },
        ),
        ProviderNativeAgentNodeError::InvocationExecutor => (
            FailureCategory::DependencyUnavailable,
            "runtime.agent.invocation_executor",
            "The durable invocation executor could not complete the attempt.",
            RetryAdvice::Never,
        ),
        ProviderNativeAgentNodeError::Store(_)
        | ProviderNativeAgentNodeError::Integrity
        | ProviderNativeAgentNodeError::InvalidState
        | ProviderNativeAgentNodeError::ModelFailed(_)
        | ProviderNativeAgentNodeError::PolicyDenied(_) => (
            FailureCategory::Internal,
            "runtime.agent.invalid_state",
            "The provider-native agent state could not be executed safely.",
            RetryAdvice::Never,
        ),
    };
    let failure = Failure::new(
        FailureId::generate(),
        category,
        FailureCode::new(code).expect("static failure code is valid"),
        FailureOrigin::new("stateknot.runtime.provider-native-agent")
            .expect("static failure origin is valid"),
        FailureMessage::new(message).expect("static failure message is valid"),
        advice,
    )
    .expect("static failure semantics are coherent")
    .with_private_source(error);
    GraphNodeExecutionError::new(failure, usage)
        .expect("provider-native node failures are uncaused and never reconcile-first")
}

fn validate_supported_execution(
    descriptor: &AgentDescriptor,
) -> Result<(), ProviderNativeAgentGraphBuildError> {
    if descriptor.execution().structured_output() != AgentStructuredOutputStrategy::ModelNative {
        return Err(ProviderNativeAgentGraphBuildError::StructuredOutputUnsupported);
    }
    if descriptor.execution().max_output_repair_turns() != ExecutionCount::ZERO
        && !descriptor.tools().is_empty()
        && !descriptor
            .model()
            .capabilities()
            .tools()
            .choices()
            .contains(ModelToolChoice::None)
    {
        return Err(ProviderNativeAgentGraphBuildError::OutputRepairToolSelectionUnsupported);
    }
    if descriptor.execution().max_tool_calls_per_turn().get() > 256 {
        return Err(ProviderNativeAgentGraphBuildError::ToolCallsPerTurnLimit);
    }
    Ok(())
}

fn build_output_repair_instruction(
    descriptor: &AgentDescriptor,
) -> Result<Option<Instruction>, ProviderNativeAgentGraphBuildError> {
    if descriptor.execution().max_output_repair_turns() == ExecutionCount::ZERO {
        return Ok(None);
    }
    if descriptor.instructions().len() == AgentInstructions::MAX_LEN {
        return Err(ProviderNativeAgentGraphBuildError::OutputRepairInstructionCapacity);
    }
    if descriptor
        .instructions()
        .iter()
        .any(|instruction| instruction.identity().name().as_str() == OUTPUT_REPAIR_INSTRUCTION_NAME)
    {
        return Err(ProviderNativeAgentGraphBuildError::OutputRepairInstructionConflict);
    }
    let metadata = ContentMetadata::new(
        ContentSource::Application,
        ContentTrust::ApplicationControlled,
        SecurityLabel::new("stateknot/internal/output-repair")
            .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?,
        RedactionState::NotApplied,
    );
    let content = TextContent::new(OUTPUT_REPAIR_INSTRUCTION_TEXT, None, metadata)
        .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?;
    let identity = InstructionIdentity::new(
        InstructionName::new(OUTPUT_REPAIR_INSTRUCTION_NAME)
            .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)?,
        Version::new(1, 0, 0),
    );
    Instruction::new(
        identity,
        content.into(),
        InstructionProvenance::new(descriptor.metadata().identity().owner().clone()),
    )
    .map(Some)
    .map_err(|_| ProviderNativeAgentGraphBuildError::StaticDefinition)
}

fn composition_digest(
    descriptor: &AgentDescriptor,
    policy: &AgentToolPolicyReference,
    accounting: &AgentInvocationAccountingReference,
    input_security_label: &SecurityLabel,
) -> Result<Digest, ProviderNativeAgentGraphBuildError> {
    #[derive(Serialize)]
    struct Wire<'a> {
        implementation: &'static str,
        descriptor: &'a AgentDescriptor,
        policy: &'a AgentToolPolicyReference,
        accounting: &'a AgentInvocationAccountingReference,
        input_security_label: &'a SecurityLabel,
    }
    let canonical = serde_json_canonicalizer::to_vec(&Wire {
        implementation: IMPLEMENTATION_VERSION,
        descriptor,
        policy,
        accounting,
        input_security_label,
    })
    .map_err(|_| ProviderNativeAgentGraphBuildError::Canonicalization)?;
    let mut preimage = b"stateknot.provider-native-agent.contract.v1\0".to_vec();
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn build_state_schema(
    schema_id: SchemaId,
    contract_digest: Digest,
    maximum_turns: u64,
    maximum_calls: u64,
    maximum_repairs: u64,
) -> Result<(SchemaReference, Value), ProviderNativeAgentGraphBuildError> {
    let mut document = serde_json::to_value(schemars::schema_for!(ProviderNativeAgentState))
        .map_err(|_| ProviderNativeAgentGraphBuildError::StateSchema)?;
    let object = document
        .as_object_mut()
        .ok_or(ProviderNativeAgentGraphBuildError::StateSchema)?;
    object.insert(
        "$schema".into(),
        Value::String("https://json-schema.org/draft/2020-12/schema".into()),
    );
    object.insert("$id".into(), Value::String(schema_id.as_str().into()));
    let description = if maximum_repairs == 0 {
        format!(
            "StateKnot provider-native agent state; contract={contract_digest:?}; max_turns={maximum_turns}; max_calls_per_turn={maximum_calls}"
        )
    } else {
        format!(
            "StateKnot provider-native agent state; contract={contract_digest:?}; max_turns={maximum_turns}; max_calls_per_turn={maximum_calls}; max_output_repair_turns={maximum_repairs}"
        )
    };
    object.insert("description".into(), Value::String(description));
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| ProviderNativeAgentGraphBuildError::Canonicalization)?;
    Ok((
        SchemaReference::new(schema_id, Version::new(1, 0, 0), Digest::sha256(canonical)),
        document,
    ))
}

fn decode_state(
    value: &BoundedJson,
) -> Result<ProviderNativeAgentState, ProviderNativeAgentStateError> {
    serde_json::from_value(value.as_value().clone())
        .map_err(|_| ProviderNativeAgentStateError::Transition)
}

fn validate_state(
    state: &ProviderNativeAgentState,
    graph: &ProviderNativeAgentGraph,
) -> Result<(), ProviderNativeAgentStateError> {
    if state.contract_digest != graph.contract_digest {
        return Err(ProviderNativeAgentStateError::ContractMismatch);
    }
    let execution = graph.descriptor.execution();
    let maximum_turns = usize::try_from(execution.max_model_turns().get())
        .map_err(|_| ProviderNativeAgentStateError::Bounds)?;
    let maximum_calls = usize::try_from(execution.max_tool_calls_per_turn().get())
        .map_err(|_| ProviderNativeAgentStateError::Bounds)?;
    let maximum_repairs = usize::try_from(execution.max_output_repair_turns().get())
        .map_err(|_| ProviderNativeAgentStateError::Bounds)?;
    if state.completed_turns.len() >= maximum_turns {
        return Err(ProviderNativeAgentStateError::Bounds);
    }
    let mut reference_count = 0_usize;
    let mut invocation_ids = BTreeSet::new();
    let mut output_repair_turns = 0_usize;
    let mut output_repair_active = false;
    for turn in &state.completed_turns {
        if !invocation_ids.insert(turn.model_invocation_id) {
            return Err(ProviderNativeAgentStateError::Transition);
        }
        if turn.requires_output_repair() {
            output_repair_active = true;
            output_repair_turns = output_repair_turns
                .checked_add(1)
                .ok_or(ProviderNativeAgentStateError::Bounds)?;
            continue;
        }
        if output_repair_active || turn.tool_invocation_ids.len() > maximum_calls {
            return Err(ProviderNativeAgentStateError::Bounds);
        }
        for invocation_id in &turn.tool_invocation_ids {
            if !invocation_ids.insert(*invocation_id) {
                return Err(ProviderNativeAgentStateError::Transition);
            }
        }
        reference_count = reference_count
            .checked_add(turn.tool_invocation_ids.len())
            .ok_or(ProviderNativeAgentStateError::Bounds)?;
    }
    if output_repair_turns > maximum_repairs {
        return Err(ProviderNativeAgentStateError::Bounds);
    }
    if u64::try_from(reference_count).map_err(|_| ProviderNativeAgentStateError::Bounds)?
        > MAX_CHECKPOINT_INVOCATION_REFERENCES
    {
        return Err(ProviderNativeAgentStateError::Bounds);
    }
    match &state.phase {
        ProviderNativeAgentPhase::Model { plan } => {
            if !invocation_ids.insert(plan.invocation_id) {
                return Err(ProviderNativeAgentStateError::Transition);
            }
        }
        ProviderNativeAgentPhase::Tools {
            model_invocation_id,
            plans,
        } => {
            let consumed_model_turns = state
                .completed_turns
                .len()
                .checked_add(1)
                .ok_or(ProviderNativeAgentStateError::Bounds)?;
            if output_repair_active
                || consumed_model_turns >= maximum_turns
                || plans.is_empty()
                || plans.len() > maximum_calls
                || !invocation_ids.insert(*model_invocation_id)
            {
                return Err(ProviderNativeAgentStateError::Bounds);
            }
            reference_count = reference_count
                .checked_add(plans.len())
                .ok_or(ProviderNativeAgentStateError::Bounds)?;
            if u64::try_from(reference_count).map_err(|_| ProviderNativeAgentStateError::Bounds)?
                > MAX_CHECKPOINT_INVOCATION_REFERENCES
            {
                return Err(ProviderNativeAgentStateError::Bounds);
            }
            for (index, plan) in plans.iter().enumerate() {
                if usize::from(plan.proposal_index) != index
                    || !invocation_ids.insert(plan.invocation_id)
                {
                    return Err(ProviderNativeAgentStateError::Transition);
                }
            }
        }
    }
    Ok(())
}

fn validate_transition(
    node_id: &NodeId,
    current: &ProviderNativeAgentState,
    next: &ProviderNativeAgentState,
    graph: &ProviderNativeAgentGraph,
) -> Result<(), ProviderNativeAgentStateError> {
    if current.contract_digest != next.contract_digest
        || current.input_message_id != next.input_message_id
    {
        return Err(ProviderNativeAgentStateError::Transition);
    }
    if node_id == &graph.model_node_id {
        let ProviderNativeAgentPhase::Model { plan } = &current.phase else {
            return Err(ProviderNativeAgentStateError::Transition);
        };
        match &next.phase {
            ProviderNativeAgentPhase::Tools {
                model_invocation_id,
                ..
            } if !output_repair_active(current)
                && current.completed_turns == next.completed_turns
                && plan.invocation_id == *model_invocation_id =>
            {
                Ok(())
            }
            ProviderNativeAgentPhase::Model { plan: next_plan }
                if next.completed_turns.len() == current.completed_turns.len() + 1
                    && next.completed_turns[..current.completed_turns.len()]
                        == current.completed_turns
                    && next.completed_turns.last().is_some_and(|turn| {
                        turn.requires_output_repair()
                            && turn.model_invocation_id == plan.invocation_id
                    })
                    && next_plan.invocation_id != plan.invocation_id
                    && next_plan.attempt_id != plan.attempt_id =>
            {
                Ok(())
            }
            _ => Err(ProviderNativeAgentStateError::Transition),
        }
    } else if node_id == &graph.tools_node_id {
        let ProviderNativeAgentPhase::Tools {
            model_invocation_id,
            plans,
        } = &current.phase
        else {
            return Err(ProviderNativeAgentStateError::Transition);
        };
        if !matches!(next.phase, ProviderNativeAgentPhase::Model { .. })
            || next.completed_turns.len() != current.completed_turns.len() + 1
            || next.completed_turns[..current.completed_turns.len()] != current.completed_turns
        {
            return Err(ProviderNativeAgentStateError::Transition);
        }
        let appended = next
            .completed_turns
            .last()
            .ok_or(ProviderNativeAgentStateError::Transition)?;
        let expected_tools = plans
            .iter()
            .map(|plan| plan.invocation_id)
            .collect::<Vec<_>>();
        if appended.model_invocation_id != *model_invocation_id
            || appended.tool_invocation_ids != expected_tools
        {
            return Err(ProviderNativeAgentStateError::Transition);
        }
        Ok(())
    } else {
        Err(ProviderNativeAgentStateError::Transition)
    }
}

fn action_digest(
    admission_digest: Digest,
    model_invocation_id: InvocationId,
    proposal_index: usize,
    proposal: &ModelToolCallProposal,
) -> Result<Digest, ProviderNativeAgentNodeError> {
    #[derive(Serialize)]
    struct Wire<'a> {
        admission_digest: Digest,
        model_invocation_id: InvocationId,
        proposal_index: usize,
        proposal: &'a ModelToolCallProposal,
    }
    let canonical = serde_json_canonicalizer::to_vec(&Wire {
        admission_digest,
        model_invocation_id,
        proposal_index,
        proposal,
    })
    .map_err(|_| ProviderNativeAgentNodeError::Integrity)?;
    let mut preimage = b"stateknot.provider-native-agent.action.v1\0".to_vec();
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

#[derive(Debug, Error)]
enum ProviderNativeAgentNodeError {
    #[error("provider-native agent durable evidence is unavailable")]
    Store(#[source] StoreError),
    #[error("provider-native agent durable evidence failed integrity validation")]
    Integrity,
    #[error("provider-native agent reached an unsupported or inconsistent state")]
    InvalidState,
    #[error("provider-native agent exhausted a finite execution budget")]
    Budget,
    #[error("provider-native agent observed an uncertain external invocation")]
    UncertainInvocation,
    #[error("provider-native agent reconciliation is still pending")]
    ReconciliationPending { retry_after: DurationMillis },
    #[error("provider-native agent reconciliation failed")]
    Reconciliation(#[source] ToolReconciliationAttemptExecutionError),
    #[error("provider-native agent policy dependency is unavailable")]
    Policy,
    #[error("provider-native agent invocation executor failed")]
    InvocationExecutor,
    #[error("provider-native agent model attempt failed")]
    ModelFailed(Box<Failure>),
    #[error("provider-native agent tool policy denied the action")]
    PolicyDenied(Box<Failure>),
    #[error("provider-native agent model output is invalid")]
    InvalidModelOutput,
    #[error("provider-native agent exhausted configured output-repair turns")]
    OutputRepairExhausted,
    #[error("provider-native agent model output is incomplete")]
    IncompleteModelOutput,
}

#[derive(Debug)]
struct ProviderNativeAgentExecutionError {
    source: ProviderNativeAgentNodeError,
    usage: BudgetUsage,
}

impl ProviderNativeAgentExecutionError {
    fn observed(source: ProviderNativeAgentNodeError, usage: BudgetUsage) -> Self {
        Self { source, usage }
    }

    fn into_graph_error(self) -> GraphNodeExecutionError {
        node_execution_error(self.source, self.usage)
    }
}

impl From<ProviderNativeAgentNodeError> for ProviderNativeAgentExecutionError {
    fn from(source: ProviderNativeAgentNodeError) -> Self {
        Self {
            source,
            usage: BudgetUsage::zero(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{future::pending, time::Duration};

    use serde_json::Value;
    use stateknot_core::{
        CapabilityName, CapabilityReference, IssuerId, PrincipalIdentity, SubjectId,
    };

    use super::*;

    struct AllowAllPolicy {
        reference: AgentToolPolicyReference,
    }

    impl AgentToolPolicy for AllowAllPolicy {
        fn reference(&self) -> &AgentToolPolicyReference {
            &self.reference
        }

        fn evaluate(
            &self,
            context: AgentToolPolicyContext,
        ) -> BoxFuture<'_, Result<AgentToolPolicyDecision, AgentToolPolicyError>> {
            Box::pin(async move {
                Ok(AgentToolPolicyDecision::Allow {
                    evidence_digest: context.action_digest(),
                })
            })
        }
    }

    struct KnownFreeAccounting {
        reference: AgentInvocationAccountingReference,
    }

    impl AgentInvocationAccounting for KnownFreeAccounting {
        fn reference(&self) -> &AgentInvocationAccountingReference {
            &self.reference
        }

        fn model_charge(&self, _: &ModelInvocation) -> AgentInvocationCharge {
            AgentInvocationCharge::Known(KnownCosts::empty())
        }

        fn tool_charge(&self, _: &ToolInvocation) -> AgentInvocationCharge {
            AgentInvocationCharge::Known(KnownCosts::empty())
        }
    }

    fn accounting() -> Arc<dyn AgentInvocationAccounting> {
        Arc::new(KnownFreeAccounting {
            reference: AgentInvocationAccountingReference::new(
                identity("invocation-accounting"),
                Digest::sha256(b"known-free-test-accounting-v1"),
            ),
        })
    }

    fn identity(name: &str) -> CapabilityIdentity {
        CapabilityIdentity::new(
            PrincipalIdentity::new(
                IssuerId::new("https://issuer.example.com").unwrap(),
                SubjectId::new("provider-native-tests").unwrap(),
            ),
            CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
        )
    }

    fn descriptor_with_repairs(repairs: u64) -> AgentDescriptor {
        let mut fixture: Value = serde_json::from_str(include_str!(
            "../../stateknot-core/tests/fixtures/core-agent-v1.json"
        ))
        .unwrap();
        let mut descriptor = fixture["descriptors"]["valid"][0].take();
        descriptor["execution"]["max_output_repair_turns"] = Value::String(repairs.to_string());
        if repairs != 0 {
            descriptor["model"]["capabilities"]["tools"]["choices"] = json!(["auto", "none"]);
        }
        serde_json::from_value(descriptor).unwrap()
    }

    fn descriptor() -> AgentDescriptor {
        descriptor_with_repairs(0)
    }

    fn definition_with_descriptor(descriptor: AgentDescriptor) -> ProviderNativeAgentGraph {
        let policy: Arc<dyn AgentToolPolicy> = Arc::new(AllowAllPolicy {
            reference: AgentToolPolicyReference::new(
                identity("tool-policy"),
                Digest::sha256(b"allow-all-v1"),
            ),
        });
        ProviderNativeAgentGraph::compile(
            descriptor,
            identity("provider-native-graph"),
            identity("provider-native-reducer"),
            "https://schemas.example.com/provider-native/state/1.0.0"
                .parse()
                .unwrap(),
            SecurityLabel::new("tenant/user-input").unwrap(),
            policy,
            accounting(),
        )
        .unwrap()
    }

    fn definition() -> ProviderNativeAgentGraph {
        definition_with_descriptor(descriptor())
    }

    #[test]
    fn definition_is_digest_pinned_bounded_and_schema_validated() {
        let definition = definition();
        assert_eq!(definition.graph().nodes().len(), 2);
        assert_eq!(definition.graph().limits().maximum_parallelism(), 1);
        assert_eq!(
            definition.graph().limits().maximum_supersteps().get(),
            definition.descriptor().execution().max_model_turns().get() * 2
        );
        let initial = definition.initial_state().unwrap();
        let state = definition.decode_checkpoint(&initial).unwrap();
        assert!(state.completed_turns().is_empty());
        assert!(matches!(
            state.phase(),
            ProviderNativeAgentPhase::Model { .. }
        ));

        let mut schemas = JsonSchemaRegistryBuilder::default();
        definition.register_schema(&mut schemas).unwrap();
        let schemas = schemas.build().unwrap();
        schemas
            .validate_bounded(initial.schema(), initial.data())
            .unwrap();
    }

    #[test]
    fn output_repair_compiles_as_a_bounded_model_self_loop_without_new_state_fields() {
        let definition = definition_with_descriptor(descriptor_with_repairs(2));
        let mut unsupported = serde_json::to_value(definition.descriptor()).unwrap();
        unsupported["model"]["capabilities"]["tools"]["choices"] = json!(["auto"]);
        let unsupported = serde_json::from_value::<AgentDescriptor>(unsupported).unwrap();
        assert!(matches!(
            validate_supported_execution(&unsupported),
            Err(ProviderNativeAgentGraphBuildError::OutputRepairToolSelectionUnsupported)
        ));
        let model = definition.graph().node(&definition.model_node_id).unwrap();
        assert_eq!(
            model.continue_to().unwrap().iter().collect::<Vec<_>>(),
            vec![&definition.model_node_id]
        );
        let instruction = definition.output_repair_instruction.as_ref().unwrap();
        assert_eq!(
            instruction.identity().name().as_str(),
            OUTPUT_REPAIR_INSTRUCTION_NAME
        );
        let initial = definition.initial_state().unwrap();
        assert_eq!(
            initial
                .data()
                .as_value()
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>(),
            [
                "completed_turns",
                "contract_digest",
                "input_message_id",
                "phase"
            ]
            .into_iter()
            .map(str::to_owned)
            .collect()
        );
    }

    #[test]
    fn reducer_accepts_only_an_exact_distinct_output_repair_transition() {
        let definition = definition_with_descriptor(descriptor_with_repairs(2));
        let initial = definition.initial_state().unwrap();
        let current = definition.decode_checkpoint(&initial).unwrap();
        let current_plan = match current.phase() {
            ProviderNativeAgentPhase::Model { plan } => plan.clone(),
            ProviderNativeAgentPhase::Tools { .. } => panic!("initial phase must be model"),
        };
        let next_plan = ProviderNativeModelPlan::generate();
        let mut next = current.clone();
        next.completed_turns.push(ProviderNativeCompletedTurn {
            model_invocation_id: current_plan.invocation_id(),
            tool_invocation_ids: Vec::new(),
        });
        next.phase = ProviderNativeAgentPhase::Model {
            plan: next_plan.clone(),
        };
        validate_state(&next, &definition).unwrap();
        validate_transition(&definition.model_node_id, &current, &next, &definition).unwrap();
        assert!(next.completed_turns()[0].requires_output_repair());
        assert_ne!(current_plan.invocation_id(), next_plan.invocation_id());
        assert_ne!(current_plan.attempt_id(), next_plan.attempt_id());

        let mut reused_attempt = next.clone();
        reused_attempt.phase = ProviderNativeAgentPhase::Model {
            plan: ProviderNativeModelPlan {
                invocation_id: next_plan.invocation_id(),
                attempt_id: current_plan.attempt_id(),
                ..next_plan.clone()
            },
        };
        assert_eq!(
            validate_transition(
                &definition.model_node_id,
                &current,
                &reused_attempt,
                &definition,
            ),
            Err(ProviderNativeAgentStateError::Transition)
        );

        let mut reused = next.clone();
        reused.phase = ProviderNativeAgentPhase::Model { plan: current_plan };
        assert_eq!(
            validate_state(&reused, &definition),
            Err(ProviderNativeAgentStateError::Transition)
        );
    }

    #[test]
    fn output_repair_reserves_one_framework_instruction_slot() {
        let mut fixture: Value = serde_json::from_str(include_str!(
            "../../stateknot-core/tests/fixtures/core-agent-v1.json"
        ))
        .unwrap();
        let descriptor = &mut fixture["descriptors"]["valid"][0];
        descriptor["execution"]["max_output_repair_turns"] = Value::String("1".into());
        descriptor["instructions"][0]["identity"]["name"] =
            Value::String(OUTPUT_REPAIR_INSTRUCTION_NAME.into());
        let conflict: AgentDescriptor = serde_json::from_value(descriptor.clone()).unwrap();
        assert!(matches!(
            build_output_repair_instruction(&conflict),
            Err(ProviderNativeAgentGraphBuildError::OutputRepairInstructionConflict)
        ));

        let template = descriptor["instructions"][0].clone();
        descriptor["instructions"] = Value::Array(
            (0..AgentInstructions::MAX_LEN)
                .map(|index| {
                    let mut instruction = template.clone();
                    instruction["identity"]["name"] =
                        Value::String(format!("application.instruction.{index}"));
                    instruction
                })
                .collect(),
        );
        let full: AgentDescriptor = serde_json::from_value(descriptor.clone()).unwrap();
        assert!(matches!(
            build_output_repair_instruction(&full),
            Err(ProviderNativeAgentGraphBuildError::OutputRepairInstructionCapacity)
        ));
    }

    #[test]
    fn reducer_accepts_only_the_exact_model_tools_model_transition() {
        let definition = Arc::new(definition());
        let initial = definition.initial_state().unwrap();
        let current = definition.decode_checkpoint(&initial).unwrap();
        let model_plan = match current.phase() {
            ProviderNativeAgentPhase::Model { plan } => plan.clone(),
            ProviderNativeAgentPhase::Tools { .. } => panic!("initial phase must be model"),
        };
        let mut next = current.clone();
        next.phase = ProviderNativeAgentPhase::Tools {
            model_invocation_id: model_plan.invocation_id,
            plans: vec![ProviderNativeToolPlan::generate(
                0,
                Digest::sha256(b"action"),
                Digest::sha256(b"policy"),
            )],
        };
        let model_node = NodeId::new(PROVIDER_NATIVE_MODEL_NODE_ID).unwrap();
        validate_transition(&model_node, &current, &next, &definition).unwrap();

        let mut tampered = next;
        tampered.input_message_id = MessageId::generate();
        assert_eq!(
            validate_transition(&model_node, &current, &tampered, &definition),
            Err(ProviderNativeAgentStateError::Transition)
        );
    }

    #[test]
    fn reconciliation_event_identity_is_stable_without_changing_checkpoint_wire_shape() {
        let plan = ProviderNativeToolPlan::generate(
            0,
            Digest::sha256(b"action"),
            Digest::sha256(b"policy"),
        );
        let event_id = plan.reconciliation_event_id();
        let wire = serde_json::to_value(&plan).unwrap();
        assert!(wire.get("reconciliation_event_id").is_none());
        let restored: ProviderNativeToolPlan = serde_json::from_value(wire).unwrap();
        assert_eq!(restored.reconciliation_event_id(), event_id);
        assert_ne!(event_id, plan.prepared_event_id());
        assert_ne!(event_id, plan.attempt_start_event_id());
        assert_ne!(event_id, plan.attempt_terminal_event_id());
    }

    #[test]
    fn pending_reconciliation_becomes_a_durable_safe_after_node_retry() {
        let delay = DurationMillis::new(750).unwrap();
        let error = node_execution_error(
            ProviderNativeAgentNodeError::ReconciliationPending { retry_after: delay },
            BudgetUsage::zero(),
        );
        assert_eq!(
            error.failure().retry_advice(),
            RetryAdvice::SafeAfter { delay }
        );
        assert_eq!(
            error.failure().code().as_str(),
            "runtime.agent.reconciliation_pending"
        );
    }

    #[test]
    fn unsupported_semantics_fail_closed_at_compile_time() {
        let mut fixture: Value = serde_json::from_str(include_str!(
            "../../stateknot-core/tests/fixtures/core-agent-v1.json"
        ))
        .unwrap();
        let mut value = fixture["descriptors"]["valid"][0].take();
        value["execution"]["structured_output"] = Value::String("tool_call".into());
        value["execution"]["max_output_repair_turns"] = Value::String("0".into());
        let unsupported: AgentDescriptor = serde_json::from_value(value).unwrap();
        let policy: Arc<dyn AgentToolPolicy> = Arc::new(AllowAllPolicy {
            reference: AgentToolPolicyReference::new(
                identity("tool-policy"),
                Digest::sha256(b"allow-all-v1"),
            ),
        });
        assert!(matches!(
            ProviderNativeAgentGraph::compile(
                unsupported,
                identity("provider-native-graph"),
                identity("provider-native-reducer"),
                "https://schemas.example.com/provider-native/state/1.0.0"
                    .parse()
                    .unwrap(),
                SecurityLabel::new("tenant/user-input").unwrap(),
                policy,
                accounting(),
            ),
            Err(ProviderNativeAgentGraphBuildError::StructuredOutputUnsupported)
        ));
    }

    #[test]
    fn bounded_parallel_read_only_execution_compiles_without_changing_graph_wire_shape() {
        let mut fixture: Value = serde_json::from_str(include_str!(
            "../../stateknot-core/tests/fixtures/core-agent-v1.json"
        ))
        .unwrap();
        let mut value = fixture["descriptors"]["valid"][0].take();
        value["execution"]["max_output_repair_turns"] = Value::String("0".into());
        value["execution"]["tool_concurrency"] = serde_json::json!({
            "mode": "parallel_read_only",
            "max_concurrency": "2"
        });
        let descriptor: AgentDescriptor = serde_json::from_value(value).unwrap();
        let definition = ProviderNativeAgentGraph::compile(
            descriptor,
            identity("provider-native-parallel-graph"),
            identity("provider-native-parallel-reducer"),
            "https://schemas.example.com/provider-native/parallel-state/1.0.0"
                .parse()
                .unwrap(),
            SecurityLabel::new("tenant/user-input").unwrap(),
            Arc::new(AllowAllPolicy {
                reference: AgentToolPolicyReference::new(
                    identity("parallel-tool-policy"),
                    Digest::sha256(b"parallel-allow-all-v1"),
                ),
            }),
            accounting(),
        )
        .unwrap();
        assert_eq!(
            definition.descriptor().execution().tool_concurrency(),
            AgentToolConcurrency::parallel_read_only(ExecutionCount::new(2))
        );
        assert_eq!(definition.graph().limits().maximum_parallelism(), 1);
    }

    #[tokio::test]
    async fn dropping_a_parallel_tool_wave_aborts_its_provider_task() {
        struct NotifyOnDrop(Arc<tokio::sync::Notify>);

        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let dropped = Arc::new(tokio::sync::Notify::new());
        let task_guard = NotifyOnDrop(Arc::clone(&dropped));
        let task = tokio::spawn(async move {
            let _task_guard = task_guard;
            pending::<ToolTerminalCommitHandoff>().await
        });
        drop(AbortOnDropToolTask::new(task));

        tokio::time::timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("dropping a Tool wave must cancel its detached provider future");
    }
}
