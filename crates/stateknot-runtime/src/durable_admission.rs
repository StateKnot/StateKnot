// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Trusted control-plane preparation for atomic durable Agent admission.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::json;
use stateknot_core::{
    AgentAdmissionAuthority, AgentAdmissionBudgetLayer, AgentAdmissionIntent,
    AgentAdmissionIntentError, AgentDescriptor, AgentRequest, AgentResultProvenance,
    AgentSubmissionKey, BoundedJson, BoundedJsonError, CanonicalJson, CanonicalJsonError,
    CheckpointId, CheckpointState, CheckpointWrite, CheckpointWriteError, Digest, EventId,
    GraphReference, GraphSchemaValidationError, InvocationId, JournalAppend, JournalAppendError,
    JournalEventIntent, JournalEventKind, JournalEventKindError, JournalExpectation,
    JournalIntentError, JournalPayload, JournalPayloadError, RunId, TenantId, ThreadId,
};
use stateknot_store_postgres::{
    AgentAdmissionCommitOutcome, AgentSubmissionCommitOutcome, PostgresStore, StoreError,
};
use thiserror::Error;

use crate::{
    ExecutableGraphRegistry, StandardAgentAdmissionSchemaError,
    standard_agent_admission_event_schema,
};

/// Stable identities allocated once at trusted request ingress.
///
/// Retrying an ambiguous admission must reuse this exact value. Generating a
/// replacement event or checkpoint identity after a lost acknowledgement is a
/// conflicting submission, not an idempotent retry.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
pub struct AgentRunIds {
    run_id: RunId,
    thread_id: ThreadId,
    invocation_id: InvocationId,
    admission_event_id: EventId,
    initial_checkpoint_id: CheckpointId,
}

impl AgentRunIds {
    /// Binds caller-retained `UUIDv7` identities for one root Agent run.
    #[must_use]
    pub const fn new(
        run_id: RunId,
        thread_id: ThreadId,
        invocation_id: InvocationId,
        admission_event_id: EventId,
        initial_checkpoint_id: CheckpointId,
    ) -> Self {
        Self {
            run_id,
            thread_id,
            invocation_id,
            admission_event_id,
            initial_checkpoint_id,
        }
    }

    /// Generates one complete identity bundle for a new submission.
    #[must_use]
    pub fn generate() -> Self {
        Self::new(
            RunId::generate(),
            ThreadId::generate(),
            InvocationId::generate(),
            EventId::generate(),
            CheckpointId::generate(),
        )
    }

    /// Returns the durable run identity.
    #[must_use]
    pub const fn run_id(self) -> RunId {
        self.run_id
    }

    /// Returns the durable conversation identity.
    #[must_use]
    pub const fn thread_id(self) -> ThreadId {
        self.thread_id
    }

    /// Returns the root logical Agent invocation identity.
    #[must_use]
    pub const fn invocation_id(self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the immutable sequence-one event identity.
    #[must_use]
    pub const fn admission_event_id(self) -> EventId {
        self.admission_event_id
    }

    /// Returns the immutable superstep-zero checkpoint identity.
    #[must_use]
    pub const fn initial_checkpoint_id(self) -> CheckpointId {
        self.initial_checkpoint_id
    }
}

/// Fully retained input for an exact atomic Agent-admission retry.
///
/// The request carries no journal head or ready set. Those are derived from the
/// empty journal and the frozen executable graph inside [`DurableAgentAdmission`].
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableAgentAdmissionRequest {
    intent: AgentAdmissionIntent,
    admission_event_id: EventId,
    initial_checkpoint_id: CheckpointId,
    initial_state: CheckpointState,
}

impl DurableAgentAdmissionRequest {
    /// Constructs one complete request and resolves its immutable budget.
    ///
    /// # Errors
    ///
    /// Returns [`DurableAgentAdmissionRequestError`] for an invalid Agent,
    /// request, policy, budget, provenance, graph, or initial-state binding.
    #[allow(clippy::too_many_arguments)]
    pub fn new<I>(
        tenant_id: TenantId,
        ids: AgentRunIds,
        descriptor: AgentDescriptor,
        request: AgentRequest,
        budget_layers: I,
        graph: GraphReference,
        authority: AgentAdmissionAuthority,
        initial_state: CheckpointState,
    ) -> Result<Self, DurableAgentAdmissionRequestError>
    where
        I: IntoIterator<Item = AgentAdmissionBudgetLayer>,
    {
        let provenance = AgentResultProvenance::for_agent(
            tenant_id,
            ids.run_id,
            ids.thread_id,
            ids.invocation_id,
            &descriptor,
        );
        let intent = AgentAdmissionIntent::new(
            provenance,
            descriptor,
            request,
            budget_layers,
            graph,
            authority,
        )?;
        Self::from_intent(
            intent,
            ids.admission_event_id,
            ids.initial_checkpoint_id,
            initial_state,
        )
    }

    fn from_intent(
        intent: AgentAdmissionIntent,
        admission_event_id: EventId,
        initial_checkpoint_id: CheckpointId,
        initial_state: CheckpointState,
    ) -> Result<Self, DurableAgentAdmissionRequestError> {
        if initial_state.schema() != intent.graph().state_schema() {
            return Err(DurableAgentAdmissionRequestError::InitialStateSchemaMismatch);
        }
        Ok(Self {
            intent,
            admission_event_id,
            initial_checkpoint_id,
            initial_state,
        })
    }

    /// Returns the complete immutable caller-controlled admission intent.
    #[must_use]
    pub const fn intent(&self) -> &AgentAdmissionIntent {
        &self.intent
    }

    /// Returns the stable first event identity.
    #[must_use]
    pub const fn admission_event_id(&self) -> EventId {
        self.admission_event_id
    }

    /// Returns the stable initial checkpoint identity.
    #[must_use]
    pub const fn initial_checkpoint_id(&self) -> CheckpointId {
        self.initial_checkpoint_id
    }

    /// Returns the exact schema-pinned initial graph state.
    #[must_use]
    pub const fn initial_state(&self) -> &CheckpointState {
        &self.initial_state
    }
}

impl fmt::Debug for DurableAgentAdmissionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableAgentAdmissionRequest")
            .field("intent", &self.intent)
            .field("admission_event_id", &self.admission_event_id)
            .field("initial_checkpoint_id", &self.initial_checkpoint_id)
            .field("initial_state_schema", self.initial_state.schema())
            .field("initial_state_digest", &self.initial_state.digest())
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for DurableAgentAdmissionRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: AgentAdmissionIntent,
            admission_event_id: EventId,
            initial_checkpoint_id: CheckpointId,
            initial_state: CheckpointState,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::from_intent(
            wire.intent,
            wire.admission_event_id,
            wire.initial_checkpoint_id,
            wire.initial_state,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid retained admission request.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableAgentAdmissionRequestError {
    /// The complete Agent admission intent was invalid.
    #[error(transparent)]
    Intent(#[from] AgentAdmissionIntentError),
    /// Initial state named a schema other than the pinned graph state schema.
    #[error("initial Agent state schema does not match the pinned graph")]
    InitialStateSchemaMismatch,
}

/// Trusted runtime facade for one all-or-nothing durable Agent admission.
#[derive(Clone)]
pub struct DurableAgentAdmission {
    store: PostgresStore,
    registry: ExecutableGraphRegistry,
    event_schema: stateknot_core::SchemaReference,
}

impl DurableAgentAdmission {
    /// Binds one provider pool and the exact executable deployment snapshot.
    ///
    /// # Errors
    ///
    /// Rejects a malformed embedded release schema or a deployment registry
    /// that omitted its exact digest-pinned reference before freezing.
    pub fn new(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
    ) -> Result<Self, DurableAgentAdmissionBuildError> {
        let (event_schema, _) = standard_agent_admission_event_schema()?;
        if !registry.schemas().contains(&event_schema) {
            return Err(DurableAgentAdmissionBuildError::EventSchemaUnavailable);
        }
        Ok(Self {
            store,
            registry,
            event_schema,
        })
    }

    pub(crate) const fn event_schema(&self) -> &stateknot_core::SchemaReference {
        &self.event_schema
    }

    /// Validates and atomically commits one new executable Agent run.
    ///
    /// Exact retries recover durable admission evidence before time-sensitive
    /// evaluation. The request, authorization evidence, initial state, standard
    /// event data, graph input/output schemas, and complete executable graph are
    /// all checked against the same immutable offline deployment snapshot before
    /// the `PostgreSQL` transaction begins.
    ///
    /// # Errors
    ///
    /// Returns [`DurableAgentAdmissionError`] for deployment drift, schema
    /// rejection, malformed retained input, durable conflict/corruption, or a
    /// database failure.
    pub async fn admit(
        &self,
        request: DurableAgentAdmissionRequest,
    ) -> Result<AgentAdmissionCommitOutcome, DurableAgentAdmissionError> {
        let prepared = self.prepare(request)?;
        Box::pin(self.store.admit_agent_run(
            prepared.intent,
            prepared.append,
            prepared.checkpoint,
            self.registry.schemas(),
        ))
        .await
        .map_err(DurableAgentAdmissionError::Store)
    }

    /// Resolves an ingress idempotency key to one atomic executable Agent run.
    ///
    /// Candidate IDs inside `request` are deliberately excluded from the
    /// submission fingerprint. A retry may therefore create a fresh candidate
    /// bundle: the same key and content return the original run, while changed
    /// content fails closed. The raw key is never stored.
    ///
    /// # Errors
    ///
    /// Returns [`DurableAgentAdmissionError`] for deployment/schema failures,
    /// conflicting key reuse, durable corruption, or database failure.
    pub async fn submit(
        &self,
        key: &AgentSubmissionKey,
        request: DurableAgentAdmissionRequest,
    ) -> Result<AgentSubmissionCommitOutcome, DurableAgentAdmissionError> {
        let prepared = self.prepare(request)?;
        Box::pin(self.store.submit_agent_run(
            key,
            prepared.intent,
            prepared.append,
            prepared.checkpoint,
            self.registry.schemas(),
        ))
        .await
        .map_err(DurableAgentAdmissionError::Store)
    }

    fn prepare(
        &self,
        request: DurableAgentAdmissionRequest,
    ) -> Result<PreparedDurableAgentAdmission, DurableAgentAdmissionError> {
        let executable = self
            .registry
            .resolve(request.intent.graph())
            .ok_or(DurableAgentAdmissionError::ExecutableGraphUnavailable)?;
        let graph = executable.graph();
        let descriptor = request.intent.descriptor();
        if descriptor.input_schema() != graph.input_schema() {
            return Err(DurableAgentAdmissionError::GraphInputSchemaMismatch);
        }
        if descriptor.output_schema() != graph.output_schema() {
            return Err(DurableAgentAdmissionError::GraphOutputSchemaMismatch);
        }

        self.registry
            .schemas()
            .validate_bounded(
                request.intent.request().input_schema(),
                request.intent.request().input(),
            )
            .map_err(DurableAgentAdmissionError::input_schema)?;
        self.registry
            .schemas()
            .validate_bounded(
                request.intent.authority().evidence().schema(),
                request.intent.authority().evidence().data(),
            )
            .map_err(DurableAgentAdmissionError::authority_schema)?;
        self.registry
            .schemas()
            .validate_bounded(request.initial_state.schema(), request.initial_state.data())
            .map_err(DurableAgentAdmissionError::initial_state_schema)?;

        let input_digest = CanonicalJson::new(request.intent.request().input())
            .map_err(DurableAgentAdmissionError::CanonicalInput)?
            .digest();
        let event_data = BoundedJson::try_from_value(json!({
            "operation": "agent_admitted",
            "intent_digest": digest_hex(request.intent.intent_digest()),
            "graph_digest": digest_hex(request.intent.graph().definition_digest()),
            "policy_digest": digest_hex(request.intent.authority().policy_digest()),
            "input_digest": digest_hex(input_digest)
        }))
        .map_err(DurableAgentAdmissionError::EventData)?;
        self.registry
            .schemas()
            .validate_bounded(&self.event_schema, &event_data)
            .map_err(DurableAgentAdmissionError::event_schema)?;
        let payload = JournalPayload::new(
            self.event_schema.clone(),
            JournalEventKind::new(stateknot_core::AgentAdmission::JOURNAL_EVENT_KIND)
                .map_err(DurableAgentAdmissionError::EventKind)?,
            event_data,
        )
        .map_err(DurableAgentAdmissionError::EventPayload)?;
        let event = JournalEventIntent::control_plane(
            request.intent.provenance().tenant_id().clone(),
            request.intent.provenance().run_id(),
            request.admission_event_id,
            payload,
        )
        .map_err(DurableAgentAdmissionError::EventIntent)?;
        let append = JournalAppend::new(JournalExpectation::empty(), event)
            .map_err(DurableAgentAdmissionError::JournalAppend)?;
        let checkpoint = CheckpointWrite::initial(
            request.intent.provenance().tenant_id().clone(),
            request.intent.provenance().run_id(),
            request.initial_checkpoint_id,
            request.intent.graph().clone(),
            request.initial_state,
            graph.entry_nodes().clone(),
        )
        .map_err(DurableAgentAdmissionError::Checkpoint)?;

        Ok(PreparedDurableAgentAdmission {
            intent: request.intent,
            append,
            checkpoint,
        })
    }
}

struct PreparedDurableAgentAdmission {
    intent: AgentAdmissionIntent,
    append: JournalAppend,
    checkpoint: CheckpointWrite,
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

impl fmt::Debug for DurableAgentAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableAgentAdmission")
            .field("registry", &self.registry)
            .field("event_schema", &self.event_schema)
            .finish_non_exhaustive()
    }
}

/// Startup failure while binding the durable admission facade.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableAgentAdmissionBuildError {
    /// The embedded standard event schema was malformed.
    #[error(transparent)]
    EventSchema(#[from] StandardAgentAdmissionSchemaError),
    /// The deployment registry omitted the exact embedded event schema.
    #[error("standard Agent-admission event schema is unavailable")]
    EventSchemaUnavailable,
}

/// Payload-redacted failure while preparing or committing Agent admission.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DurableAgentAdmissionError {
    /// The intent's exact graph and executable closure are absent locally.
    #[error("Agent admission graph is unavailable in the executable registry")]
    ExecutableGraphUnavailable,
    /// The Agent input contract and graph input contract differ.
    #[error("Agent input schema does not match the executable graph input schema")]
    GraphInputSchemaMismatch,
    /// The Agent output contract and graph terminal contract differ.
    #[error("Agent output schema does not match the executable graph output schema")]
    GraphOutputSchemaMismatch,
    /// The typed request failed its exact local input schema.
    #[error("Agent admission input schema validation failed: {source}")]
    InputSchema {
        /// Closed schema result.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// Authorization evidence failed its exact local schema.
    #[error("Agent admission authorization evidence validation failed: {source}")]
    AuthoritySchema {
        /// Closed schema result.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// Initial graph state failed its exact local schema.
    #[error("Agent admission initial state validation failed: {source}")]
    InitialStateSchema {
        /// Closed schema result.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// The framework-owned event data failed its embedded schema.
    #[error("Agent admission standard event schema validation failed: {source}")]
    EventSchema {
        /// Closed schema result.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// Input JSON could not be represented in the canonical audit digest.
    #[error("Agent admission input canonicalization failed: {0}")]
    CanonicalInput(#[source] CanonicalJsonError),
    /// Framework-owned event data unexpectedly exceeded JSON bounds.
    #[error("Agent admission event data is invalid: {0}")]
    EventData(#[source] BoundedJsonError),
    /// The framework-owned stable event kind was malformed.
    #[error("Agent admission event kind is invalid: {0}")]
    EventKind(#[source] JournalEventKindError),
    /// The framework-owned payload envelope was malformed.
    #[error("Agent admission event payload is invalid: {0}")]
    EventPayload(#[source] JournalPayloadError),
    /// The control-plane event intent could not be constructed.
    #[error("Agent admission event intent is invalid: {0}")]
    EventIntent(#[source] JournalIntentError),
    /// The empty-head append could not be constructed.
    #[error("Agent admission journal append is invalid: {0}")]
    JournalAppend(#[source] JournalAppendError),
    /// The initial checkpoint intent could not be constructed.
    #[error("Agent admission initial checkpoint is invalid: {0}")]
    Checkpoint(#[source] CheckpointWriteError),
    /// Durable commit, conflict, or integrity failure.
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl DurableAgentAdmissionError {
    const fn input_schema(source: GraphSchemaValidationError) -> Self {
        Self::InputSchema { source }
    }

    const fn authority_schema(source: GraphSchemaValidationError) -> Self {
        Self::AuthoritySchema { source }
    }

    const fn initial_state_schema(source: GraphSchemaValidationError) -> Self {
        Self::InitialStateSchema { source }
    }

    const fn event_schema(source: GraphSchemaValidationError) -> Self {
        Self::EventSchema { source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_digest_data_uses_schema_hex_without_algorithm_prefix() {
        let value = digest_hex(Digest::sha256(b"agent admission"));
        assert_eq!(value.len(), Digest::SHA256_LEN * 2);
        assert!(value.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!value.contains(':'));
    }
}
