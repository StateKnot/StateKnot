// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Public, integrity-verifying access to durable Agent runs and results.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use stateknot_core::{
    AgentResult, AgentResultProvenance, AgentResultValidationError, AgentSubmissionKey,
    BudgetUsage, Digest, Failure, FailureCategory, GraphReference, GraphSchemaValidationError,
    RetryAdvice, RunId, RunRevision, RunStatus, TenantId, Timestamp,
};
use stateknot_store_postgres::{
    AgentAdmissionCommitOutcome, AgentSubmissionCommitOutcome, PostgresStore, StoreError,
    StoredAgentAdmission,
};
use thiserror::Error;

use crate::{
    DurableAgentAdmission, DurableAgentAdmissionBuildError, DurableAgentAdmissionError,
    DurableAgentAdmissionRequest, ExecutableGraphRegistry,
};

/// A closed terminal outcome returned by the durable Agent run facade.
///
/// Successful output is the compact, schema-bound [`AgentResult`]. Failure and
/// cancellation expose only the framework's public-safe [`Failure`] plus exact
/// terminal accounting; private diagnostics remain outside this value.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AgentRunTerminalOutcome {
    /// The Agent committed a verified final output.
    Succeeded {
        /// Schema-bound output, artifacts, provenance, and cumulative usage.
        result: AgentResult,
    },
    /// The Agent committed a non-cancellation terminal failure.
    Failed {
        /// Public-safe terminal failure occurrence.
        failure: Failure,
        /// Authoritative durable completion observation.
        completed_at: Timestamp,
        /// Complete cumulative run accounting.
        usage: BudgetUsage,
    },
    /// A previously committed cancellation request reached acknowledgement.
    Cancelled {
        /// Public-safe cancellation occurrence.
        failure: Failure,
        /// Authoritative durable acknowledgement observation.
        completed_at: Timestamp,
        /// Complete cumulative run accounting.
        usage: BudgetUsage,
    },
}

impl AgentRunTerminalOutcome {
    /// Returns the lifecycle status represented by this terminal value.
    #[must_use]
    pub const fn status(&self) -> RunStatus {
        match self {
            Self::Succeeded { .. } => RunStatus::Succeeded,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
        }
    }

    /// Returns the authoritative durable completion observation.
    #[must_use]
    pub const fn completed_at(&self) -> Timestamp {
        match self {
            Self::Succeeded { result } => result.completed_at(),
            Self::Failed { completed_at, .. } | Self::Cancelled { completed_at, .. } => {
                *completed_at
            }
        }
    }

    /// Returns complete cumulative run accounting.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        match self {
            Self::Succeeded { result } => result.usage(),
            Self::Failed { usage, .. } | Self::Cancelled { usage, .. } => usage,
        }
    }

    /// Returns the successful result, when this is a successful outcome.
    #[must_use]
    pub const fn result(&self) -> Option<&AgentResult> {
        match self {
            Self::Succeeded { result } => Some(result),
            Self::Failed { .. } | Self::Cancelled { .. } => None,
        }
    }

    /// Returns the public failure for failed or cancelled outcomes.
    #[must_use]
    pub const fn failure(&self) -> Option<&Failure> {
        match self {
            Self::Succeeded { .. } => None,
            Self::Failed { failure, .. } | Self::Cancelled { failure, .. } => Some(failure),
        }
    }
}

impl<'de> Deserialize<'de> for AgentRunTerminalOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Succeeded {
                result: AgentResult,
            },
            Failed {
                failure: Failure,
                completed_at: Timestamp,
                usage: BudgetUsage,
            },
            Cancelled {
                failure: Failure,
                completed_at: Timestamp,
                usage: BudgetUsage,
            },
        }

        let outcome = match Wire::deserialize(deserializer)? {
            Wire::Succeeded { result } => Self::Succeeded { result },
            Wire::Failed {
                failure,
                completed_at,
                usage,
            } => Self::Failed {
                failure,
                completed_at,
                usage,
            },
            Wire::Cancelled {
                failure,
                completed_at,
                usage,
            } => Self::Cancelled {
                failure,
                completed_at,
                usage,
            },
        };
        if !terminal_failure_kind_matches(&outcome) {
            return Err(de::Error::custom(
                "Agent run failure does not match its terminal lifecycle path",
            ));
        }
        Ok(outcome)
    }
}

fn terminal_failure_kind_matches(outcome: &AgentRunTerminalOutcome) -> bool {
    match outcome {
        AgentRunTerminalOutcome::Succeeded { .. } => true,
        AgentRunTerminalOutcome::Failed { failure, .. } => {
            failure.category() != FailureCategory::Cancelled
        }
        AgentRunTerminalOutcome::Cancelled { failure, .. } => {
            failure.category() == FailureCategory::Cancelled
                && failure.retry_advice() == RetryAdvice::Never
        }
    }
}

/// Public, fully revalidated snapshot of one durably admitted Agent run.
///
/// The snapshot intentionally excludes authorization evidence, request input,
/// graph state, leases, scheduler internals, and private diagnostics. Pollers
/// can use `(run_id, revision)` as a monotonic observation key. A quarantined
/// run remains visible but must not be represented as executable work.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRunSnapshot {
    provenance: AgentResultProvenance,
    graph: GraphReference,
    admission_digest: Digest,
    admitted_at: Timestamp,
    revision: RunRevision,
    status: RunStatus,
    changed_at: Timestamp,
    quarantined: bool,
    #[schemars(required)]
    outcome: Option<AgentRunTerminalOutcome>,
}

impl AgentRunSnapshot {
    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        provenance: AgentResultProvenance,
        graph: GraphReference,
        admission_digest: Digest,
        admitted_at: Timestamp,
        revision: RunRevision,
        status: RunStatus,
        changed_at: Timestamp,
        quarantined: bool,
        outcome: Option<AgentRunTerminalOutcome>,
    ) -> Result<Self, AgentRunSnapshotError> {
        if status == RunStatus::Pending {
            return Err(AgentRunSnapshotError::PendingAtomicAdmission);
        }
        if revision == RunRevision::ZERO {
            return Err(AgentRunSnapshotError::AdvancedAtZeroRevision);
        }
        if changed_at < admitted_at {
            return Err(AgentRunSnapshotError::ClockRegression);
        }
        validate_outcome_shape(status, outcome.as_ref(), &provenance, changed_at)?;
        Ok(Self {
            provenance,
            graph,
            admission_digest,
            admitted_at,
            revision,
            status,
            changed_at,
            quarantined,
            outcome,
        })
    }

    /// Returns trusted tenant, run, thread, invocation, and Agent identity.
    #[must_use]
    pub const fn provenance(&self) -> &AgentResultProvenance {
        &self.provenance
    }

    /// Returns the immutable graph version pinned at admission.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }

    /// Returns the complete immutable admission checksum.
    #[must_use]
    pub const fn admission_digest(&self) -> Digest {
        self.admission_digest
    }

    /// Returns the authoritative database admission observation.
    #[must_use]
    pub const fn admitted_at(&self) -> Timestamp {
        self.admitted_at
    }

    /// Returns the current monotonic lifecycle revision.
    #[must_use]
    pub const fn revision(&self) -> RunRevision {
        self.revision
    }

    /// Returns the current protocol-neutral lifecycle status.
    #[must_use]
    pub const fn status(&self) -> RunStatus {
        self.status
    }

    /// Returns the latest committed lifecycle observation.
    #[must_use]
    pub const fn changed_at(&self) -> Timestamp {
        self.changed_at
    }

    /// Returns whether integrity or operator policy removed this run from execution.
    #[must_use]
    pub const fn is_quarantined(&self) -> bool {
        self.quarantined
    }

    /// Returns a terminal outcome only after one has durably committed.
    #[must_use]
    pub const fn outcome(&self) -> Option<&AgentRunTerminalOutcome> {
        self.outcome.as_ref()
    }
}

impl fmt::Debug for AgentRunSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentRunSnapshot")
            .field("provenance", &self.provenance)
            .field("graph", &self.graph)
            .field("admission_digest", &self.admission_digest)
            .field("admitted_at", &self.admitted_at)
            .field("revision", &self.revision)
            .field("status", &self.status)
            .field("changed_at", &self.changed_at)
            .field("quarantined", &self.quarantined)
            .field("has_outcome", &self.outcome.is_some())
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for AgentRunSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            provenance: AgentResultProvenance,
            graph: GraphReference,
            admission_digest: Digest,
            admitted_at: Timestamp,
            revision: RunRevision,
            status: RunStatus,
            changed_at: Timestamp,
            quarantined: bool,
            #[serde(deserialize_with = "deserialize_nullable_outcome")]
            outcome: Option<AgentRunTerminalOutcome>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::from_parts(
            wire.provenance,
            wire.graph,
            wire.admission_digest,
            wire.admitted_at,
            wire.revision,
            wire.status,
            wire.changed_at,
            wire.quarantined,
            wire.outcome,
        )
        .map_err(de::Error::custom)
    }
}

fn deserialize_nullable_outcome<'de, D>(
    deserializer: D,
) -> Result<Option<AgentRunTerminalOutcome>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<AgentRunTerminalOutcome>::deserialize(deserializer)
}

fn validate_outcome_shape(
    status: RunStatus,
    outcome: Option<&AgentRunTerminalOutcome>,
    provenance: &AgentResultProvenance,
    changed_at: Timestamp,
) -> Result<(), AgentRunSnapshotError> {
    match (status.is_terminal(), outcome) {
        (false, None) => return Ok(()),
        (true, Some(outcome)) if outcome.status() == status => {
            if outcome.completed_at() != changed_at {
                return Err(AgentRunSnapshotError::CompletionTimeMismatch);
            }
            if !terminal_failure_kind_matches(outcome) {
                return Err(AgentRunSnapshotError::FailureKindMismatch);
            }
            if let Some(result) = outcome.result() {
                if result.provenance() != provenance {
                    return Err(AgentRunSnapshotError::ResultProvenanceMismatch);
                }
            }
            return Ok(());
        }
        _ => {}
    }
    Err(AgentRunSnapshotError::OutcomeStatusMismatch)
}

/// Invalid public Agent run snapshot shape.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentRunSnapshotError {
    /// Atomic Agent admission cannot expose its internal pending precursor.
    #[error("atomic Agent admission cannot expose a pending lifecycle")]
    PendingAtomicAdmission,
    /// Every visible atomic Agent run has advanced past revision zero.
    #[error("visible Agent lifecycle cannot remain at revision zero")]
    AdvancedAtZeroRevision,
    /// A lifecycle observation preceded admission.
    #[error("Agent run change observation precedes admission")]
    ClockRegression,
    /// Terminal status and outcome presence or kind differed.
    #[error("Agent run terminal outcome does not match lifecycle status")]
    OutcomeStatusMismatch,
    /// The terminal outcome observation differed from lifecycle time.
    #[error("Agent run terminal completion does not match lifecycle time")]
    CompletionTimeMismatch,
    /// A successful result named another trusted run identity.
    #[error("Agent run result provenance does not match lifecycle provenance")]
    ResultProvenanceMismatch,
    /// Failure category or retry semantics did not match its terminal path.
    #[error("Agent run failure does not match its terminal lifecycle path")]
    FailureKindMismatch,
}

/// Result of public-facade Agent admission.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AgentRunAdmissionOutcome {
    /// A new atomic Agent run committed.
    Committed(AgentRunSnapshot),
    /// The exact retained admission or ingress-keyed submission had already committed.
    Idempotent(AgentRunSnapshot),
}

impl AgentRunAdmissionOutcome {
    /// Returns the public verified run snapshot in either case.
    #[must_use]
    pub const fn snapshot(&self) -> &AgentRunSnapshot {
        match self {
            Self::Committed(snapshot) | Self::Idempotent(snapshot) => snapshot,
        }
    }
}

/// Public durable Agent admission and run/result query facade.
///
/// This value keeps the exact executable/schema deployment registry beside the
/// store. Every read reloads the immutable admission and current lifecycle in
/// one repeatable-read snapshot, then revalidates request, authority, initial
/// state, audit event, graph closure, terminal output, provenance, and budget
/// before returning a public value.
#[derive(Clone)]
pub struct DurableAgentRuns {
    admission: DurableAgentAdmission,
    store: PostgresStore,
    registry: ExecutableGraphRegistry,
}

impl DurableAgentRuns {
    /// Binds the durability provider to one immutable executable deployment.
    ///
    /// # Errors
    ///
    /// Rejects an unavailable or malformed standard Agent-admission schema.
    pub fn new(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
    ) -> Result<Self, DurableAgentRunsBuildError> {
        let admission = DurableAgentAdmission::new(store.clone(), registry.clone())?;
        Ok(Self {
            admission,
            store,
            registry,
        })
    }

    /// Validates, atomically admits, and returns a public run snapshot.
    ///
    /// Ambiguous retries must retain the exact [`DurableAgentAdmissionRequest`].
    /// Ingress idempotency-key allocation is deliberately a stronger boundary
    /// and is not emulated with process-local memory by this method.
    ///
    /// # Errors
    ///
    /// Returns a closed admission, storage, deployment, schema, integrity, or
    /// terminal-accounting failure.
    pub async fn admit(
        &self,
        request: DurableAgentAdmissionRequest,
    ) -> Result<AgentRunAdmissionOutcome, DurableAgentRunsError> {
        let outcome = self.admission.admit(request).await?;
        let committed = match &outcome {
            AgentAdmissionCommitOutcome::Committed(_) => true,
            AgentAdmissionCommitOutcome::Idempotent(_) => false,
            _ => return Err(DurableAgentRunsError::InvalidDurableSnapshot),
        };
        let snapshot = self.snapshot(outcome.stored())?;
        Ok(if committed {
            AgentRunAdmissionOutcome::Committed(snapshot)
        } else {
            AgentRunAdmissionOutcome::Idempotent(snapshot)
        })
    }

    /// Submits one candidate under a durable tenant-scoped idempotency key.
    ///
    /// Unlike [`Self::admit`], callers do not retain candidate IDs after an
    /// ambiguous response. They recreate the same caller content with the same
    /// key; the provider returns the originally selected run even if the new
    /// request contains a fresh [`crate::AgentRunIds`] bundle.
    ///
    /// # Errors
    ///
    /// Reusing a key for changed content returns a durable conflict. Deployment,
    /// schema, integrity, and database failures remain closed and payload-safe.
    pub async fn submit(
        &self,
        key: &AgentSubmissionKey,
        request: DurableAgentAdmissionRequest,
    ) -> Result<AgentRunAdmissionOutcome, DurableAgentRunsError> {
        let outcome = self.admission.submit(key, request).await?;
        let committed = match &outcome {
            AgentSubmissionCommitOutcome::Committed(_) => true,
            AgentSubmissionCommitOutcome::Idempotent(_) => false,
            _ => return Err(DurableAgentRunsError::InvalidDurableSnapshot),
        };
        let snapshot = self.snapshot(outcome.stored().admission())?;
        Ok(if committed {
            AgentRunAdmissionOutcome::Committed(snapshot)
        } else {
            AgentRunAdmissionOutcome::Idempotent(snapshot)
        })
    }

    /// Loads one tenant-scoped Agent run and fully revalidates its public view.
    ///
    /// Calling services remain responsible for authenticating and authorizing
    /// the tenant/run lookup before invoking this trusted server-side method.
    ///
    /// # Errors
    ///
    /// Returns not-found, corruption, deployment drift, schema rejection,
    /// invalid terminal accounting, or database failures.
    pub async fn load(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
    ) -> Result<AgentRunSnapshot, DurableAgentRunsError> {
        let stored = self.store.load_agent_admission(tenant_id, run_id).await?;
        self.snapshot(&stored)
    }

    /// Resolves an ingress key and loads its current fully verified public run.
    ///
    /// # Errors
    ///
    /// Returns key-not-found, corruption, deployment drift, schema rejection,
    /// invalid terminal result, or database failures.
    pub async fn load_by_key(
        &self,
        tenant_id: &TenantId,
        key: &AgentSubmissionKey,
    ) -> Result<AgentRunSnapshot, DurableAgentRunsError> {
        let stored = self.store.load_agent_submission(tenant_id, key).await?;
        self.snapshot(stored.admission())
    }

    fn snapshot(
        &self,
        stored: &StoredAgentAdmission,
    ) -> Result<AgentRunSnapshot, DurableAgentRunsError> {
        let admission = stored.admission();
        let intent = admission.intent();
        let executable = self
            .registry
            .resolve(intent.graph())
            .ok_or(DurableAgentRunsError::ExecutableGraphUnavailable)?;
        if intent.descriptor().input_schema() != executable.graph().input_schema() {
            return Err(DurableAgentRunsError::GraphInputSchemaMismatch);
        }
        if intent.descriptor().output_schema() != executable.graph().output_schema() {
            return Err(DurableAgentRunsError::GraphOutputSchemaMismatch);
        }

        self.registry
            .schemas()
            .validate_bounded(intent.request().input_schema(), intent.request().input())
            .map_err(DurableAgentRunsError::input_schema)?;
        self.registry
            .schemas()
            .validate_bounded(
                intent.authority().evidence().schema(),
                intent.authority().evidence().data(),
            )
            .map_err(DurableAgentRunsError::authority_schema)?;
        self.registry
            .schemas()
            .validate_bounded(
                stored.checkpoint().state().schema(),
                stored.checkpoint().state().data(),
            )
            .map_err(DurableAgentRunsError::initial_state_schema)?;

        let event_schema = self.admission.event_schema();
        if stored.event().payload().schema() != event_schema {
            return Err(DurableAgentRunsError::AdmissionEventSchemaMismatch);
        }
        self.registry
            .schemas()
            .validate_bounded(event_schema, stored.event().payload().data())
            .map_err(DurableAgentRunsError::event_schema)?;

        let run = stored.run();
        let lifecycle = run.lifecycle();
        let outcome = match lifecycle.status() {
            RunStatus::Succeeded => {
                let result = lifecycle
                    .result()
                    .cloned()
                    .ok_or(DurableAgentRunsError::InvalidDurableSnapshot)?;
                result
                    .validate_for(
                        intent.provenance(),
                        intent.request(),
                        intent.descriptor(),
                        intent.budget(),
                    )
                    .map_err(DurableAgentRunsError::result_validation)?;
                self.registry
                    .schemas()
                    .validate_bounded(result.output_schema(), result.output())
                    .map_err(DurableAgentRunsError::output_schema)?;
                Some(AgentRunTerminalOutcome::Succeeded { result })
            }
            RunStatus::Failed | RunStatus::Cancelled => {
                let failure = lifecycle
                    .terminal_failure()
                    .cloned()
                    .ok_or(DurableAgentRunsError::InvalidDurableSnapshot)?;
                let usage = lifecycle
                    .terminal_usage()
                    .cloned()
                    .ok_or(DurableAgentRunsError::InvalidDurableSnapshot)?;
                Some(if lifecycle.status() == RunStatus::Failed {
                    AgentRunTerminalOutcome::Failed {
                        failure,
                        completed_at: lifecycle.changed_at(),
                        usage,
                    }
                } else {
                    AgentRunTerminalOutcome::Cancelled {
                        failure,
                        completed_at: lifecycle.changed_at(),
                        usage,
                    }
                })
            }
            RunStatus::Pending
            | RunStatus::Active
            | RunStatus::Waiting
            | RunStatus::CancellationRequested => None,
        };

        AgentRunSnapshot::from_parts(
            lifecycle.provenance().clone(),
            intent.graph().clone(),
            admission.digest(),
            admission.admitted_at(),
            lifecycle.revision(),
            lifecycle.status(),
            lifecycle.changed_at(),
            run.is_quarantined(),
            outcome,
        )
        .map_err(DurableAgentRunsError::Snapshot)
    }
}

impl fmt::Debug for DurableAgentRuns {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableAgentRuns")
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}

/// Startup failure for the public durable Agent run facade.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableAgentRunsBuildError {
    /// The underlying atomic admission facade could not bind its standard schema.
    #[error(transparent)]
    Admission(#[from] DurableAgentAdmissionBuildError),
}

/// Payload-redacted failure from public durable Agent admission or reads.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DurableAgentRunsError {
    /// Atomic Agent admission failed.
    #[error(transparent)]
    Admission(#[from] DurableAgentAdmissionError),
    /// Durable storage or integrity verification failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The exact graph and executable closure are absent from this deployment.
    #[error("Agent run graph is unavailable in the executable registry")]
    ExecutableGraphUnavailable,
    /// The Agent and executable graph disagree about input schema.
    #[error("Agent input schema does not match the executable graph input schema")]
    GraphInputSchemaMismatch,
    /// The Agent and executable graph disagree about output schema.
    #[error("Agent output schema does not match the executable graph output schema")]
    GraphOutputSchemaMismatch,
    /// Durable request input no longer passes its pinned schema.
    #[error("durable Agent input schema validation failed: {source}")]
    InputSchema {
        /// Closed schema validation result.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// Durable authorization evidence no longer passes its pinned schema.
    #[error("durable Agent authorization evidence validation failed: {source}")]
    AuthoritySchema {
        /// Closed schema validation result.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// Durable initial state no longer passes its pinned schema.
    #[error("durable Agent initial state schema validation failed: {source}")]
    InitialStateSchema {
        /// Closed schema validation result.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// The immutable first event named another schema version.
    #[error("durable Agent admission event schema does not match this runtime release")]
    AdmissionEventSchemaMismatch,
    /// Durable admission audit data no longer passes the standard schema.
    #[error("durable Agent admission event schema validation failed: {source}")]
    EventSchema {
        /// Closed schema validation result.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// A successful terminal output no longer passes its pinned schema.
    #[error("durable Agent result schema validation failed: {source}")]
    OutputSchema {
        /// Closed schema validation result.
        #[source]
        source: GraphSchemaValidationError,
    },
    /// A successful result failed provenance, request, schema, or accounting binding.
    #[error("durable Agent result integrity validation failed: {source}")]
    ResultValidation {
        /// Core result validation failure.
        #[source]
        source: AgentResultValidationError,
    },
    /// Durable facts formed a relationship the public snapshot cannot represent.
    #[error("durable Agent run snapshot is inconsistent")]
    InvalidDurableSnapshot,
    /// Public snapshot fields formed an impossible lifecycle view.
    #[error(transparent)]
    Snapshot(#[from] AgentRunSnapshotError),
}

impl DurableAgentRunsError {
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

    const fn output_schema(source: GraphSchemaValidationError) -> Self {
        Self::OutputSchema { source }
    }

    const fn result_validation(source: AgentResultValidationError) -> Self {
        Self::ResultValidation { source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_status_requires_the_matching_outcome_kind() {
        assert_eq!(
            validate_outcome_shape(
                RunStatus::Succeeded,
                None,
                &test_provenance(),
                test_timestamp(),
            ),
            Err(AgentRunSnapshotError::OutcomeStatusMismatch)
        );
        assert!(
            validate_outcome_shape(
                RunStatus::Active,
                None,
                &test_provenance(),
                test_timestamp(),
            )
            .is_ok()
        );
    }

    #[test]
    fn snapshot_schema_is_a_closed_response_object() {
        let schema = serde_json::to_value(schemars::schema_for!(AgentRunSnapshot)).unwrap();
        assert_eq!(
            schema["additionalProperties"],
            serde_json::Value::Bool(false)
        );
        assert_eq!(schema["required"].as_array().map(Vec::len), Some(9));
    }

    #[test]
    fn terminal_failure_kind_remains_bound_to_its_lifecycle_path() {
        use stateknot_core::{FailureCode, FailureId, FailureMessage, FailureOrigin};

        let failure = Failure::new(
            FailureId::generate(),
            FailureCategory::Internal,
            FailureCode::new("agent.public_failed").unwrap(),
            FailureOrigin::new("test.public-run").unwrap(),
            FailureMessage::new("The public run failed safely.").unwrap(),
            RetryAdvice::Never,
        )
        .unwrap();
        let outcome = AgentRunTerminalOutcome::Cancelled {
            failure,
            completed_at: test_timestamp(),
            usage: BudgetUsage::zero(),
        };
        assert_eq!(
            validate_outcome_shape(
                RunStatus::Cancelled,
                Some(&outcome),
                &test_provenance(),
                test_timestamp(),
            ),
            Err(AgentRunSnapshotError::FailureKindMismatch)
        );
        assert!(
            serde_json::from_value::<AgentRunTerminalOutcome>(
                serde_json::to_value(&outcome).unwrap()
            )
            .is_err()
        );
    }

    fn test_provenance() -> AgentResultProvenance {
        use stateknot_core::{
            CapabilityIdentity, CapabilityName, CapabilityReference, IssuerId, PrincipalIdentity,
            SubjectId, Version,
        };

        AgentResultProvenance::new(
            TenantId::new("tenant-public-run").unwrap(),
            RunId::generate(),
            stateknot_core::ThreadId::generate(),
            stateknot_core::InvocationId::generate(),
            CapabilityIdentity::new(
                PrincipalIdentity::new(
                    IssuerId::new("https://issuer.example.com").unwrap(),
                    SubjectId::new("public-run-tests").unwrap(),
                ),
                CapabilityReference::new(
                    CapabilityName::new("agent.public-run").unwrap(),
                    Version::new(1, 0, 0),
                ),
            ),
        )
    }

    fn test_timestamp() -> Timestamp {
        "2026-09-01T00:00:00.000000Z".parse().unwrap()
    }
}
