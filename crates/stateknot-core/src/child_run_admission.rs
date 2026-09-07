// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Closed child-admission preparation. No durable spawn or authorization grant.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    AgentAdmission, AgentAdmissionIntent, AgentAdmissionIntentError, BudgetNarrowingError,
    Checkpoint, CheckpointState, ChildRunKey, CompiledGraph, Digest, GraphSchemaValidationError,
    GraphSchemaValidator, NodeActivation, Timestamp,
};

const SPAWN_DOMAIN: &[u8] = b"stateknot.child-run-admission-intent.v1\0";

/// A bounded, immutable child admission candidate and its stable retry digest.
///
/// Binds one parent admission and logical child key to the full child Agent,
/// graph, request, initial state, authority, and resolved budget. Candidate
/// child Run/thread/invocation IDs deliberately do not enter `spawn_digest`;
/// they are retained in `child` and checked by its own integrity encoding.
/// The first atomic commit must choose the durable IDs for all later retries.
///
/// This is preparation, not permission to spawn. Neither construction nor
/// deserialization proves live authority, declaration membership, lease,
/// remaining budget, depth, or concurrency. A future store must call
/// [`Self::validate_for`] against authoritative snapshots and perform those
/// additional checks in its ownership/admission transaction. No existing root
/// admission API may be used as a substitute for that transaction.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunAdmissionIntent {
    key: ChildRunKey,
    parent_admission_digest: Digest,
    child: AgentAdmissionIntent,
    child_graph: CompiledGraph,
    initial_state: CheckpointState,
    spawn_digest: Digest,
}

impl ChildRunAdmissionIntent {
    /// Maximum complete canonical preparation envelope size, in bytes.
    pub const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;

    /// Constructs an intent and checks parent scope, principal, and narrowing.
    ///
    /// `parent` must be restored by trusted storage before use. Its checksum
    /// alone is not authentication. Current readiness and schema validation
    /// are deliberately repeated by [`Self::validate_for`].
    pub fn new(
        parent: &AgentAdmission,
        key: ChildRunKey,
        child: AgentAdmissionIntent,
        child_graph: CompiledGraph,
        initial_state: CheckpointState,
    ) -> Result<Self, ChildRunAdmissionIntentError> {
        let intent = Self::build(key, parent.digest(), child, child_graph, initial_state)?;
        intent.validate_parent(parent)?;
        Ok(intent)
    }

    fn build(
        key: ChildRunKey,
        parent_admission_digest: Digest,
        child: AgentAdmissionIntent,
        child_graph: CompiledGraph,
        initial_state: CheckpointState,
    ) -> Result<Self, ChildRunAdmissionIntentError> {
        if key.tenant_id() != child.provenance().tenant_id()
            || key.parent_run_id() == child.provenance().run_id()
        {
            return Err(ChildRunAdmissionIntentError::ChildScopeMismatch);
        }
        if !key.parent().graph_namespace().is_root() {
            return Err(ChildRunAdmissionIntentError::UnsupportedParentNamespace);
        }
        if child.graph() != &child_graph.reference()
            || child.descriptor().input_schema() != child_graph.input_schema()
            || child.descriptor().output_schema() != child_graph.output_schema()
            || initial_state.schema() != child_graph.state_schema()
        {
            return Err(ChildRunAdmissionIntentError::ChildGraphMismatch);
        }
        let mut intent = Self {
            key,
            parent_admission_digest,
            child,
            child_graph,
            initial_state,
            spawn_digest: Digest::sha256([]),
        };
        intent.spawn_digest = intent.compute_spawn_digest()?;
        intent.canonical_bytes()?;
        Ok(intent)
    }

    fn validate_parent(&self, parent: &AgentAdmission) -> Result<(), ChildRunAdmissionIntentError> {
        let admitted = parent.intent();
        if self.parent_admission_digest != parent.digest()
            || self.key.tenant_id() != admitted.provenance().tenant_id()
            || self.key.parent_run_id() != admitted.provenance().run_id()
            || self.key.parent().base_checkpoint().graph() != admitted.graph()
            || self
                .key
                .parent()
                .base_checkpoint()
                .journal_head()
                .recorded_at()
                < parent.admitted_at()
        {
            return Err(ChildRunAdmissionIntentError::ParentMismatch);
        }
        // The first local delegation profile is same-principal only. A future
        // identity-exchange profile needs an explicit authenticated contract.
        if self.child.authority().principal() != admitted.authority().principal() {
            return Err(ChildRunAdmissionIntentError::PrincipalMismatch);
        }
        if !self
            .child
            .authority()
            .granted_scopes()
            .is_subset(admitted.authority().granted_scopes())
        {
            return Err(ChildRunAdmissionIntentError::ScopeWidening);
        }
        self.child.budget().validate_narrowing(admitted.budget())?;
        Ok(())
    }

    /// Revalidates against a trusted parent admission, current checkpoint,
    /// offline schemas, and an authoritative fresh-admission clock observation.
    ///
    /// Deserialization alone cannot perform these external checks. The supplied
    /// checkpoint must be the locked current head for actual admission; an
    /// unlocked preview does not authorize a later commit. Lost-ACK recovery
    /// must first look up exact committed spawn
    /// evidence; do not use this fresh check to invalidate a historical commit.
    pub fn validate_for<V: GraphSchemaValidator + ?Sized>(
        &self,
        parent: &AgentAdmission,
        checkpoint: &Checkpoint,
        schemas: &V,
        observed_at: Timestamp,
    ) -> Result<(), ChildRunAdmissionIntentError> {
        self.validate_parent(parent)?;
        if self.key.parent().base_checkpoint() != &checkpoint.head() {
            return Err(ChildRunAdmissionIntentError::ParentCheckpointMismatch);
        }
        let activation =
            NodeActivation::for_ready_root(checkpoint, self.key.parent().node_id().clone())
                .map_err(|_| ChildRunAdmissionIntentError::ParentNotReady)?;
        if &activation != self.key.parent() {
            return Err(ChildRunAdmissionIntentError::ParentActivationMismatch);
        }
        if observed_at < checkpoint.journal_head().recorded_at() {
            return Err(ChildRunAdmissionIntentError::ClockBeforeCheckpoint);
        }
        if observed_at >= self.child.budget().deadline()
            || observed_at >= parent.intent().budget().deadline()
        {
            return Err(ChildRunAdmissionIntentError::DeadlineExpired);
        }
        for (schema, data) in [
            (
                self.child.request().input_schema(),
                self.child.request().input(),
            ),
            (self.initial_state.schema(), self.initial_state.data()),
            (
                self.child.authority().evidence().schema(),
                self.child.authority().evidence().data(),
            ),
        ] {
            schemas.validate(schema, data)?;
        }
        Ok(())
    }

    fn compute_spawn_digest(&self) -> Result<Digest, ChildRunAdmissionIntentError> {
        // Use named closed fields, never the entire admission intent: that
        // would accidentally bind generated candidate provenance to retries.
        let wire = serde_json::json!({
            "key": self.key.digest(),
            "parent_admission_digest": self.parent_admission_digest,
            "tenant_id": self.child.provenance().tenant_id(),
            "descriptor": self.child.descriptor(),
            "request": self.child.request(),
            "budget_layers": self.child.budget_layers(),
            "budget": self.child.budget(),
            "graph": self.child.graph(),
            "authority": self.child.authority(),
            "initial_state": self.initial_state,
            "initial_ready_nodes": self.child_graph.entry_nodes(),
            "parent_close_policy": "cancel_and_join"
        });
        let canonical = canonical(&wire)?;
        let mut bytes = Vec::with_capacity(SPAWN_DOMAIN.len() + canonical.len());
        bytes.extend_from_slice(SPAWN_DOMAIN);
        bytes.extend_from_slice(&canonical);
        Ok(Digest::sha256(bytes))
    }

    /// Returns the complete bounded canonical envelope, including candidate IDs.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ChildRunAdmissionIntentError> {
        let bytes = canonical(self)?;
        if bytes.len() > Self::MAX_SNAPSHOT_BYTES {
            return Err(ChildRunAdmissionIntentError::SnapshotTooLarge);
        }
        Ok(bytes)
    }

    /// Returns the exact parent activation and slot.
    #[must_use]
    pub const fn key(&self) -> &ChildRunKey {
        &self.key
    }
    /// Returns the pinned committed parent admission digest.
    #[must_use]
    pub const fn parent_admission_digest(&self) -> Digest {
        self.parent_admission_digest
    }
    /// Returns the candidate child admission with its generated provenance.
    #[must_use]
    pub const fn child(&self) -> &AgentAdmissionIntent {
        &self.child
    }
    /// Returns the complete pinned child graph, including its initial ready set.
    #[must_use]
    pub const fn child_graph(&self) -> &CompiledGraph {
        &self.child_graph
    }
    /// Returns the explicitly supplied private child initial state.
    #[must_use]
    pub const fn initial_state(&self) -> &CheckpointState {
        &self.initial_state
    }
    /// Returns stable spawn comparison material, not an authorization signature.
    #[must_use]
    pub const fn spawn_digest(&self) -> Digest {
        self.spawn_digest
    }
}

fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>, ChildRunAdmissionIntentError> {
    crate::agent_admission::canonical_bytes(value).map_err(|error| match error {
        AgentAdmissionIntentError::NonInteroperableNumber => {
            ChildRunAdmissionIntentError::NonInteroperableNumber
        }
        _ => ChildRunAdmissionIntentError::IntegritySerialization,
    })
}

impl fmt::Debug for ChildRunAdmissionIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildRunAdmissionIntent")
            .field("key_digest", &self.key.digest())
            .field("parent_admission_digest", &self.parent_admission_digest)
            .field("spawn_digest", &self.spawn_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ChildRunAdmissionIntent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            key: ChildRunKey,
            parent_admission_digest: Digest,
            child: AgentAdmissionIntent,
            child_graph: CompiledGraph,
            initial_state: CheckpointState,
            spawn_digest: Digest,
        }
        let wire = Wire::deserialize(deserializer)?;
        let intent = Self::build(
            wire.key,
            wire.parent_admission_digest,
            wire.child,
            wire.child_graph,
            wire.initial_state,
        )
        .map_err(de::Error::custom)?;
        if intent.spawn_digest != wire.spawn_digest {
            return Err(de::Error::custom(
                ChildRunAdmissionIntentError::DigestMismatch,
            ));
        }
        Ok(intent)
    }
}

/// Closed public-safe child preparation or fresh-admission validation failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ChildRunAdmissionIntentError {
    /// Child tenant differs or the child reuses the parent Run ID.
    #[error("child admission crosses scope or reuses its parent run")]
    ChildScopeMismatch,
    /// Same-run nested namespaces do not yet have a readiness contract.
    #[error("child admission requires a root-namespace parent activation")]
    UnsupportedParentNamespace,
    /// Child definition, graph, or schema pins differ.
    #[error("child admission graph or schema mismatch")]
    ChildGraphMismatch,
    /// The supplied parent does not match the pinned admission and graph scope.
    #[error("child admission parent mismatch")]
    ParentMismatch,
    /// Cross-principal identity exchange is not supported by this profile.
    #[error("child admission principal differs from parent")]
    PrincipalMismatch,
    /// Child scopes are not a subset of the parent grant.
    #[error("child admission widens granted scopes")]
    ScopeWidening,
    /// A child budget limit exceeds its immutable parent limit.
    #[error(transparent)]
    Budget(#[from] BudgetNarrowingError),
    /// Locked parent checkpoint differs from the candidate's base head.
    #[error("child admission parent checkpoint mismatch")]
    ParentCheckpointMismatch,
    /// The requested parent node is not ready.
    #[error("child admission parent node is not ready")]
    ParentNotReady,
    /// Logical activation input differs from deterministic readiness derivation.
    #[error("child admission parent activation mismatch")]
    ParentActivationMismatch,
    /// The fresh clock observation predates the committed parent checkpoint.
    #[error("child admission clock predates parent checkpoint")]
    ClockBeforeCheckpoint,
    /// The child or parent deadline has expired, including equality.
    #[error("child admission deadline expired")]
    DeadlineExpired,
    /// A pinned offline schema could not validate input, state, or authority evidence.
    #[error(transparent)]
    Schema(#[from] GraphSchemaValidationError),
    /// Complete canonical envelope exceeded its fixed bound.
    #[error("child admission snapshot exceeds its byte limit")]
    SnapshotTooLarge,
    /// Integrity material could not be serialized.
    #[error("child admission canonical serialization failed")]
    IntegritySerialization,
    /// JSON numbers could not be represented interoperably without rounding.
    #[error("child admission contains a non-interoperable JSON integer")]
    NonInteroperableNumber,
    /// Persisted retry digest no longer matches the intent.
    #[error("child admission spawn digest mismatch")]
    DigestMismatch,
}

#[cfg(test)]
#[path = "child_run_admission_tests.rs"]
mod tests;
