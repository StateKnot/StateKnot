// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Versioned, authorization-first public service boundary for durable Agents.

use std::{collections::BTreeMap, fmt, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use stateknot_core::{
    AgentAdmissionAuthority, AgentAdmissionBudgetLayer, AgentDescriptor, AgentRequest,
    AgentSubmissionKey, BoundedJson, BoxFuture, CapabilityIdentity, CheckpointState, CompiledGraph,
    Digest, EventId, Failure, FailureCategory, FailureCode, FailureId, FailureMessage,
    FailureOrigin, GraphReference, GraphSchemaValidationError, JournalAppend, JournalEventIntent,
    JournalEventKind, JournalExpectation, JournalPayload, PrincipalIdentity, RetryAdvice,
    RunCancellationRequest, RunId, RunStatus, RunTransition, TenantId,
};
use stateknot_store_postgres::{
    AppendOutcome, PostgresStore, RunProjection, StoreError, WaitAbandonmentCommitOutcome,
};
use thiserror::Error;

use crate::{
    AgentRunAdmissionOutcome, AgentRunIds, AgentRunSnapshot, DurableAgentAdmissionError,
    DurableAgentAdmissionRequest, DurableAgentAdmissionRequestError, DurableAgentRuns,
    DurableAgentRunsBuildError, DurableAgentRunsError, ExecutableGraphRegistry,
    ProviderNativeAgentGraph, StandardAgentServiceControlSchemaError,
    standard_agent_service_control_event_schema,
};

/// The stable major version of this service contract.
pub const AGENT_SERVICE_API_VERSION: u16 = 1;

/// An authenticated caller supplied by the embedding transport after token or
/// mTLS verification.
///
/// Construction does not authenticate identity. The embedding service must
/// derive this value from a verified credential and install an
/// [`AgentServiceAuthorizer`] that independently evaluates every operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentServiceCaller {
    tenant_id: TenantId,
    principal: PrincipalIdentity,
}

impl AgentServiceCaller {
    /// Binds one authenticated principal to an exact tenant boundary.
    #[must_use]
    pub const fn new(tenant_id: TenantId, principal: PrincipalIdentity) -> Self {
        Self {
            tenant_id,
            principal,
        }
    }

    /// Returns the tenant selected by authenticated ingress policy.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the exact authenticated issuer/subject pair.
    #[must_use]
    pub const fn principal(&self) -> &PrincipalIdentity {
        &self.principal
    }
}

/// Complete trusted input to an Agent submission authorization decision.
///
/// The requested identity is carried instead of a resolved deployment so this
/// decision can run before the service reveals whether that exact revision is
/// installed.
#[derive(Clone, Debug)]
pub struct AgentServiceSubmissionAuthorization {
    caller: AgentServiceCaller,
    agent: CapabilityIdentity,
    request: AgentRequest,
}

impl AgentServiceSubmissionAuthorization {
    /// Returns the authenticated, tenant-bound caller.
    #[must_use]
    pub const fn caller(&self) -> &AgentServiceCaller {
        &self.caller
    }

    /// Returns the exact Agent revision requested by the caller.
    #[must_use]
    pub const fn agent(&self) -> &CapabilityIdentity {
        &self.agent
    }

    /// Returns the schema-bound request under consideration.
    #[must_use]
    pub const fn request(&self) -> &AgentRequest {
        &self.request
    }
}

/// Target form supplied to authorization without exposing a raw idempotency key.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AgentServiceRunTarget {
    /// A caller addressed one exact tenant-scoped run.
    Run(RunId),
    /// A caller addressed a tenant-scoped one-way submission-key digest.
    Submission(Digest),
}

/// Run operation evaluated before any durable existence lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AgentServiceRunOperation {
    /// Read the public, integrity-verified run snapshot.
    Read,
    /// Commit a durable cancellation request.
    Cancel,
}

/// Complete trusted input to a run read or cancellation authorization decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentServiceRunAuthorization {
    caller: AgentServiceCaller,
    target: AgentServiceRunTarget,
    operation: AgentServiceRunOperation,
}

impl AgentServiceRunAuthorization {
    /// Returns the authenticated, tenant-bound caller.
    #[must_use]
    pub const fn caller(&self) -> &AgentServiceCaller {
        &self.caller
    }

    /// Returns the non-secret durable lookup target.
    #[must_use]
    pub const fn target(&self) -> &AgentServiceRunTarget {
        &self.target
    }

    /// Returns the exact requested operation.
    #[must_use]
    pub const fn operation(&self) -> AgentServiceRunOperation {
        self.operation
    }
}

/// Granted submission policy snapshot returned by the trusted authorizer.
#[derive(Clone, Debug)]
pub struct AgentServiceSubmissionGrant {
    authority: AgentAdmissionAuthority,
    budget_layers: Vec<AgentAdmissionBudgetLayer>,
}

impl AgentServiceSubmissionGrant {
    /// Constructs a granted admission snapshot and its restrictive policy layers.
    #[must_use]
    pub fn new(
        authority: AgentAdmissionAuthority,
        budget_layers: Vec<AgentAdmissionBudgetLayer>,
    ) -> Self {
        Self {
            authority,
            budget_layers,
        }
    }

    /// Returns the immutable admission authority evidence.
    #[must_use]
    pub const fn authority(&self) -> &AgentAdmissionAuthority {
        &self.authority
    }

    /// Returns policy-selected restrictive budget layers.
    #[must_use]
    pub fn budget_layers(&self) -> &[AgentAdmissionBudgetLayer] {
        &self.budget_layers
    }
}

/// Public-safe proof that a trusted policy granted run access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentServiceRunGrant {
    principal: PrincipalIdentity,
    policy: CapabilityIdentity,
    policy_digest: Digest,
    decision_digest: Digest,
}

impl AgentServiceRunGrant {
    /// Constructs an exact run-access policy decision reference.
    #[must_use]
    pub const fn new(
        principal: PrincipalIdentity,
        policy: CapabilityIdentity,
        policy_digest: Digest,
        decision_digest: Digest,
    ) -> Self {
        Self {
            principal,
            policy,
            policy_digest,
            decision_digest,
        }
    }

    /// Returns the principal for which the policy decision was made.
    #[must_use]
    pub const fn principal(&self) -> &PrincipalIdentity {
        &self.principal
    }

    /// Returns the exact policy implementation identity.
    #[must_use]
    pub const fn policy(&self) -> &CapabilityIdentity {
        &self.policy
    }

    /// Returns the immutable policy artifact checksum.
    #[must_use]
    pub const fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// Returns the immutable decision evidence checksum.
    #[must_use]
    pub const fn decision_digest(&self) -> Digest {
        self.decision_digest
    }
}

/// Closed authorization failure returned to the service boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentServiceAuthorizationError {
    /// No authenticated identity was accepted for this operation.
    #[error("Agent service authentication was not accepted")]
    Unauthenticated,
    /// Authenticated policy denied the exact operation.
    #[error("Agent service authorization was denied")]
    Denied,
    /// The pinned policy implementation could not make a decision now.
    #[error("Agent service authorization is temporarily unavailable")]
    Unavailable,
    /// The policy returned unverifiable decision evidence.
    #[error("Agent service authorization evidence is invalid")]
    InvalidEvidence,
}

/// Mandatory authorization boundary for every public Agent service operation.
///
/// Implementations run before database lookup. They must be deterministic for
/// retained decision evidence, must not execute the Agent or its tools, and
/// must never grant access solely because a run identifier is syntactically
/// valid. Network policy engines require their own durable, bounded decision
/// ledger outside this synchronous service facade.
pub trait AgentServiceAuthorizer: Send + Sync + 'static {
    /// Authorizes one exact Agent version and request before deployment lookup.
    fn authorize_submission(
        &self,
        context: AgentServiceSubmissionAuthorization,
    ) -> BoxFuture<'_, Result<AgentServiceSubmissionGrant, AgentServiceAuthorizationError>>;

    /// Authorizes a read or cancellation before durable target lookup.
    fn authorize_run(
        &self,
        context: AgentServiceRunAuthorization,
    ) -> BoxFuture<'_, Result<AgentServiceRunGrant, AgentServiceAuthorizationError>>;
}

/// Executable Agent definition installed into the service registry.
pub trait AgentServiceDeployment: Send + Sync + 'static {
    /// Returns the immutable Agent descriptor selected at public ingress.
    fn descriptor(&self) -> &AgentDescriptor;

    /// Returns the exact executable graph pinned into durable admission.
    fn graph(&self) -> &CompiledGraph;

    /// Generates fresh bounded initial state for one candidate submission.
    fn initial_state(&self) -> Result<CheckpointState, AgentServiceDeploymentError>;
}

impl AgentServiceDeployment for ProviderNativeAgentGraph {
    fn descriptor(&self) -> &AgentDescriptor {
        ProviderNativeAgentGraph::descriptor(self)
    }

    fn graph(&self) -> &CompiledGraph {
        ProviderNativeAgentGraph::graph(self)
    }

    fn initial_state(&self) -> Result<CheckpointState, AgentServiceDeploymentError> {
        ProviderNativeAgentGraph::initial_state(self)
            .map_err(|_| AgentServiceDeploymentError::InvalidInitialState)
    }
}

/// Closed startup/runtime failure from an installed Agent definition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentServiceDeploymentError {
    /// The installed implementation could not construct state matching its graph.
    #[error("Agent service deployment produced invalid initial state")]
    InvalidInitialState,
}

struct AgentServiceBinding {
    descriptor: AgentDescriptor,
    graph: GraphReference,
    deployment: Arc<dyn AgentServiceDeployment>,
}

/// Startup-only builder for immutable, exact-version Agent service bindings.
#[derive(Default)]
pub struct AgentServiceRegistryBuilder {
    bindings: BTreeMap<CapabilityIdentity, AgentServiceBinding>,
}

impl AgentServiceRegistryBuilder {
    /// Maximum exact Agent revisions installed in one service process.
    pub const MAX_BINDINGS: usize = 4096;

    /// Creates an empty service registry builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one exact executable Agent definition.
    ///
    /// # Errors
    ///
    /// Rejects capacity exhaustion, repeated identity, or descriptor/graph
    /// input-output schema disagreement.
    pub fn register(
        &mut self,
        deployment: Arc<dyn AgentServiceDeployment>,
    ) -> Result<(), AgentServiceRegistryError> {
        if self.bindings.len() == Self::MAX_BINDINGS {
            return Err(AgentServiceRegistryError::TooManyBindings);
        }
        let descriptor = deployment.descriptor().clone();
        let graph = deployment.graph();
        if descriptor.input_schema() != graph.input_schema()
            || descriptor.output_schema() != graph.output_schema()
        {
            return Err(AgentServiceRegistryError::SchemaMismatch);
        }
        let graph = graph.reference();
        let identity = descriptor.metadata().identity().clone();
        if self.bindings.contains_key(&identity) {
            return Err(AgentServiceRegistryError::DuplicateIdentity {
                identity: Box::new(identity),
            });
        }
        self.bindings.insert(
            identity,
            AgentServiceBinding {
                descriptor,
                graph,
                deployment,
            },
        );
        Ok(())
    }

    /// Freezes the startup snapshot.
    #[must_use]
    pub fn build(self) -> AgentServiceRegistry {
        AgentServiceRegistry {
            bindings: Arc::new(self.bindings),
        }
    }
}

impl fmt::Debug for AgentServiceRegistryBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentServiceRegistryBuilder")
            .field("bindings", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

/// Immutable exact-version registry used by [`AgentServiceV1`].
#[derive(Clone)]
pub struct AgentServiceRegistry {
    bindings: Arc<BTreeMap<CapabilityIdentity, AgentServiceBinding>>,
}

impl AgentServiceRegistry {
    fn resolve(
        &self,
        identity: &CapabilityIdentity,
    ) -> Result<&AgentServiceBinding, AgentServiceRegistryError> {
        let binding = self.bindings.get(identity).ok_or_else(|| {
            AgentServiceRegistryError::MissingBinding {
                identity: Box::new(identity.clone()),
            }
        })?;
        if binding.deployment.descriptor() != &binding.descriptor
            || binding.deployment.graph().reference() != binding.graph
        {
            return Err(AgentServiceRegistryError::DeploymentDrift {
                identity: Box::new(identity.clone()),
            });
        }
        Ok(binding)
    }

    /// Returns the number of installed exact Agent revisions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    /// Returns whether no Agent service deployment is installed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

impl fmt::Debug for AgentServiceRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentServiceRegistry")
            .field("bindings", &self.bindings.len())
            .finish_non_exhaustive()
    }
}

/// Exact service-deployment registry failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentServiceRegistryError {
    /// The immutable process binding ceiling was reached.
    #[error("Agent service registry contains too many bindings")]
    TooManyBindings,
    /// One exact owner/name/version identity was registered twice.
    #[error("Agent service identity was registered more than once")]
    DuplicateIdentity {
        /// Repeated exact binding identity.
        identity: Box<CapabilityIdentity>,
    },
    /// No installed service owns the requested exact Agent identity.
    #[error("Agent service identity has no installed deployment")]
    MissingBinding {
        /// Missing exact binding identity.
        identity: Box<CapabilityIdentity>,
    },
    /// An Agent descriptor and graph disagree about input or output schema.
    #[error("Agent service descriptor and graph schemas do not match")]
    SchemaMismatch,
    /// An installed definition changed after the startup snapshot froze.
    #[error("Agent service deployment differs from its startup snapshot")]
    DeploymentDrift {
        /// Drifted exact binding identity.
        identity: Box<CapabilityIdentity>,
    },
}

/// Stable caller-retained identities for one cancellation request.
///
/// The same value must be reused after timeout or lost acknowledgement. A new
/// value represents a competing cancellation request rather than a retry.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCancellationIds {
    event_id: EventId,
    failure_id: FailureId,
}

impl AgentCancellationIds {
    /// Binds caller-retained event and public failure identities.
    #[must_use]
    pub const fn new(event_id: EventId, failure_id: FailureId) -> Self {
        Self {
            event_id,
            failure_id,
        }
    }

    /// Generates a fresh cancellation identity pair.
    #[must_use]
    pub fn generate() -> Self {
        Self::new(EventId::generate(), FailureId::generate())
    }

    /// Returns the stable control-plane journal event identity.
    #[must_use]
    pub const fn event_id(self) -> EventId {
        self.event_id
    }

    /// Returns the stable cancellation failure occurrence identity.
    #[must_use]
    pub const fn failure_id(self) -> FailureId {
        self.failure_id
    }
}

/// Result of a durable, idempotent public cancellation request.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum AgentCancellationOutcome {
    /// A new cancellation intent committed atomically.
    Committed(AgentRunSnapshot),
    /// The same cancellation failure had already committed.
    Idempotent(AgentRunSnapshot),
}

impl AgentCancellationOutcome {
    /// Returns the fully revalidated current public snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &AgentRunSnapshot {
        match self {
            Self::Committed(snapshot) | Self::Idempotent(snapshot) => snapshot,
        }
    }
}

/// Production service facade for exact-version durable Agent submission,
/// authorization-first reads, and two-phase cancellation.
#[derive(Clone)]
pub struct AgentServiceV1 {
    store: PostgresStore,
    runs: DurableAgentRuns,
    deployments: AgentServiceRegistry,
    authorizer: Arc<dyn AgentServiceAuthorizer>,
    schemas: crate::JsonSchemaRegistry,
    control_event_schema: stateknot_core::SchemaReference,
}

impl AgentServiceV1 {
    /// Binds one durable provider, executable deployment, public Agent registry,
    /// and mandatory authorizer.
    ///
    /// # Errors
    ///
    /// Rejects malformed embedded schemas or a deployment registry that omitted
    /// either the admission or service-control schema.
    pub fn new(
        store: PostgresStore,
        executable: ExecutableGraphRegistry,
        deployments: AgentServiceRegistry,
        authorizer: Arc<dyn AgentServiceAuthorizer>,
    ) -> Result<Self, AgentServiceBuildError> {
        let (control_event_schema, _) = standard_agent_service_control_event_schema()?;
        if !executable.schemas().contains(&control_event_schema) {
            return Err(AgentServiceBuildError::ControlEventSchemaUnavailable);
        }
        let schemas = executable.schemas().clone();
        let runs = DurableAgentRuns::new(store.clone(), executable)?;
        Ok(Self {
            store,
            runs,
            deployments,
            authorizer,
            schemas,
            control_event_schema,
        })
    }

    /// Authorizes and durably submits one exact Agent version under a
    /// tenant-scoped idempotency key.
    ///
    /// Candidate run/checkpoint/event identities and initial state are generated
    /// only after authorization grants the request. Lost acknowledgements are
    /// recovered by calling this method again with the same key and content.
    ///
    /// # Errors
    ///
    /// Returns closed registry, authorization, state, admission, or storage
    /// failures. No process-local execution path can bypass durable admission.
    pub fn submit<'service>(
        &'service self,
        caller: AgentServiceCaller,
        key: &'service AgentSubmissionKey,
        agent: &'service CapabilityIdentity,
        request: AgentRequest,
    ) -> BoxFuture<'service, Result<AgentRunAdmissionOutcome, AgentServiceError>> {
        Box::pin(self.submit_inner(caller, key, agent, request))
    }

    async fn submit_inner(
        &self,
        caller: AgentServiceCaller,
        key: &AgentSubmissionKey,
        agent: &CapabilityIdentity,
        request: AgentRequest,
    ) -> Result<AgentRunAdmissionOutcome, AgentServiceError> {
        let grant = self
            .authorizer
            .authorize_submission(AgentServiceSubmissionAuthorization {
                caller: caller.clone(),
                agent: agent.clone(),
                request: request.clone(),
            })
            .await?;
        if grant.authority().principal() != caller.principal() {
            return Err(AgentServiceError::AuthorizationPrincipalMismatch);
        }
        let binding = self.deployments.resolve(agent)?;
        if let Some(outcome) = self
            .recover_submission(
                &caller,
                key,
                &binding.descriptor,
                &request,
                &binding.graph,
                &grant,
            )
            .await?
        {
            return Ok(outcome);
        }
        let initial_state = binding.deployment.initial_state()?;
        if initial_state.schema() != binding.graph.state_schema() {
            return Err(AgentServiceError::Deployment(
                AgentServiceDeploymentError::InvalidInitialState,
            ));
        }
        let durable = DurableAgentAdmissionRequest::new(
            caller.tenant_id().clone(),
            AgentRunIds::generate(),
            binding.descriptor.clone(),
            request.clone(),
            grant.budget_layers.clone(),
            binding.graph.clone(),
            grant.authority.clone(),
            initial_state,
        )?;
        match self.runs.submit(key, durable).await {
            Ok(outcome) => Ok(outcome),
            Err(DurableAgentRunsError::Admission(DurableAgentAdmissionError::Store(
                StoreError::AgentSubmissionConflict,
            ))) => self
                .recover_submission(
                    &caller,
                    key,
                    &binding.descriptor,
                    &request,
                    &binding.graph,
                    &grant,
                )
                .await?
                .ok_or(AgentServiceError::SubmissionConflict),
            Err(error) => Err(error.into()),
        }
    }

    /// Authorizes before loading one tenant-scoped public run snapshot.
    ///
    /// # Errors
    ///
    /// Authorization failures precede and therefore hide not-found results.
    pub async fn load(
        &self,
        caller: AgentServiceCaller,
        run_id: RunId,
    ) -> Result<AgentRunSnapshot, AgentServiceError> {
        self.authorize_run(
            &caller,
            AgentServiceRunTarget::Run(run_id),
            AgentServiceRunOperation::Read,
        )
        .await?;
        self.runs
            .load(caller.tenant_id(), run_id)
            .await
            .map_err(Into::into)
    }

    /// Authorizes before resolving an opaque submission key to a public run.
    ///
    /// # Errors
    ///
    /// Authorization failures precede and therefore hide key existence.
    pub async fn load_by_key(
        &self,
        caller: AgentServiceCaller,
        key: &AgentSubmissionKey,
    ) -> Result<AgentRunSnapshot, AgentServiceError> {
        self.authorize_run(
            &caller,
            AgentServiceRunTarget::Submission(key.digest_for(caller.tenant_id())),
            AgentServiceRunOperation::Read,
        )
        .await?;
        self.runs
            .load_by_key(caller.tenant_id(), key)
            .await
            .map_err(Into::into)
    }

    /// Authorizes and atomically commits cancellation intent before workers
    /// cooperatively stop and the durable lifecycle confirms cancellation.
    ///
    /// Waiting runs abandon every outstanding interrupt/timer in the same
    /// transaction. Active runs append the request and lifecycle projection in
    /// one transaction. A retry with the same [`AgentCancellationIds`] returns
    /// idempotently after either request or final confirmation has committed.
    ///
    /// # Errors
    ///
    /// Returns authorization, terminal/conflicting cancellation, schema,
    /// optimistic concurrency, integrity, or database failures.
    pub async fn request_cancellation(
        &self,
        caller: AgentServiceCaller,
        run_id: RunId,
        ids: AgentCancellationIds,
    ) -> Result<AgentCancellationOutcome, AgentServiceError> {
        let grant = self
            .authorize_run(
                &caller,
                AgentServiceRunTarget::Run(run_id),
                AgentServiceRunOperation::Cancel,
            )
            .await?;
        let stored = self
            .store
            .load_agent_admission(caller.tenant_id(), run_id)
            .await?;
        let lifecycle = stored.run().lifecycle();
        if let Some(existing) = lifecycle.cancellation_request() {
            if existing.failure().id() != ids.failure_id
                || existing.failure().caused_by_event_id() != Some(ids.event_id)
            {
                return Err(AgentServiceError::ConflictingCancellation);
            }
            let snapshot = self.runs.load(caller.tenant_id(), run_id).await?;
            return Ok(AgentCancellationOutcome::Idempotent(snapshot));
        }
        if lifecycle.status().is_terminal() {
            return Err(AgentServiceError::TerminalRun);
        }
        if lifecycle.status() == RunStatus::Pending {
            return Err(AgentServiceError::InvalidRunState);
        }

        let requested_at = self.store.observe_database_clock().await?;
        let failure = cancellation_failure(ids.failure_id).with_caused_by_event(ids.event_id);
        let request = RunCancellationRequest::new(failure, requested_at)
            .map_err(|_| AgentServiceError::ControlEventInvariant)?;
        let data = BoundedJson::try_from_value(json!({
            "operation": "agent_cancellation_requested",
            "admission_digest": digest_hex(stored.admission().digest()),
            "policy_digest": digest_hex(grant.policy_digest()),
            "decision_digest": digest_hex(grant.decision_digest()),
            "failure_id": ids.failure_id.to_string()
        }))
        .map_err(|_| AgentServiceError::ControlEventInvariant)?;
        self.runs_registry()
            .validate_bounded(&self.control_event_schema, &data)
            .map_err(AgentServiceError::control_schema)?;
        let payload = JournalPayload::new(
            self.control_event_schema.clone(),
            JournalEventKind::new("agent-cancellation-requested")
                .map_err(|_| AgentServiceError::ControlEventInvariant)?,
            data,
        )
        .map_err(|_| AgentServiceError::ControlEventInvariant)?;
        let intent = JournalEventIntent::control_plane(
            caller.tenant_id().clone(),
            run_id,
            ids.event_id,
            payload,
        )
        .map_err(|_| AgentServiceError::ControlEventInvariant)?;
        let head = stored
            .run()
            .journal_head()
            .cloned()
            .ok_or(AgentServiceError::InvalidRunState)?;
        let append = JournalAppend::new(JournalExpectation::exact(head), intent)
            .map_err(|_| AgentServiceError::ControlEventInvariant)?;
        let transition = RunTransition::RequestCancellation { request };
        let committed = if lifecycle.status() == RunStatus::Waiting {
            matches!(
                self.store
                    .append_control_plane_abandon_waits(append, lifecycle.revision(), transition,)
                    .await?,
                WaitAbandonmentCommitOutcome::Committed { .. }
            )
        } else {
            matches!(
                self.store
                    .append_control_plane(
                        append,
                        RunProjection::transition(lifecycle.revision(), transition),
                    )
                    .await?,
                AppendOutcome::Committed(_)
            )
        };
        let snapshot = self.runs.load(caller.tenant_id(), run_id).await?;
        Ok(if committed {
            AgentCancellationOutcome::Committed(snapshot)
        } else {
            AgentCancellationOutcome::Idempotent(snapshot)
        })
    }

    async fn authorize_run(
        &self,
        caller: &AgentServiceCaller,
        target: AgentServiceRunTarget,
        operation: AgentServiceRunOperation,
    ) -> Result<AgentServiceRunGrant, AgentServiceError> {
        let grant = self
            .authorizer
            .authorize_run(AgentServiceRunAuthorization {
                caller: caller.clone(),
                target,
                operation,
            })
            .await?;
        if grant.principal() != caller.principal() {
            return Err(AgentServiceError::AuthorizationPrincipalMismatch);
        }
        Ok(grant)
    }

    async fn recover_submission(
        &self,
        caller: &AgentServiceCaller,
        key: &AgentSubmissionKey,
        descriptor: &AgentDescriptor,
        request: &AgentRequest,
        graph: &GraphReference,
        grant: &AgentServiceSubmissionGrant,
    ) -> Result<Option<AgentRunAdmissionOutcome>, AgentServiceError> {
        let stored = match self
            .store
            .load_agent_submission(caller.tenant_id(), key)
            .await
        {
            Ok(stored) => stored,
            Err(StoreError::AgentSubmissionNotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let intent = stored.admission().admission().intent();
        if intent.provenance().tenant_id() != caller.tenant_id()
            || intent.descriptor() != descriptor
            || intent.request() != request
            || intent.budget_layers() != grant.budget_layers()
            || intent.graph() != graph
            || intent.authority() != grant.authority()
        {
            return Err(AgentServiceError::SubmissionConflict);
        }
        let snapshot = self.runs.load_by_key(caller.tenant_id(), key).await?;
        Ok(Some(AgentRunAdmissionOutcome::Idempotent(snapshot)))
    }

    fn runs_registry(&self) -> &crate::JsonSchemaRegistry {
        &self.schemas
    }
}

impl fmt::Debug for AgentServiceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentServiceV1")
            .field("api_version", &AGENT_SERVICE_API_VERSION)
            .field("deployments", &self.deployments)
            .field("control_event_schema", &self.control_event_schema)
            .finish_non_exhaustive()
    }
}

/// Startup failure for [`AgentServiceV1`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentServiceBuildError {
    /// The lower-level public durable run facade could not start.
    #[error(transparent)]
    Runs(#[from] DurableAgentRunsBuildError),
    /// The embedded control event schema was malformed.
    #[error(transparent)]
    ControlEventSchema(#[from] StandardAgentServiceControlSchemaError),
    /// The executable deployment omitted the standard service control schema.
    #[error("standard Agent service control event schema is unavailable")]
    ControlEventSchemaUnavailable,
}

/// Payload-redacted failure from the versioned Agent service boundary.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentServiceError {
    /// Exact Agent service deployment selection failed.
    #[error(transparent)]
    Registry(#[from] AgentServiceRegistryError),
    /// Mandatory ingress policy rejected or could not evaluate the operation.
    #[error(transparent)]
    Authorization(#[from] AgentServiceAuthorizationError),
    /// Authorization evidence named a different authenticated principal.
    #[error("Agent service authorization principal does not match the caller")]
    AuthorizationPrincipalMismatch,
    /// Installed initial-state generation failed closed.
    #[error(transparent)]
    Deployment(#[from] AgentServiceDeploymentError),
    /// Durable admission request construction failed.
    #[error(transparent)]
    AdmissionRequest(#[from] DurableAgentAdmissionRequestError),
    /// Public durable run admission or verification failed.
    #[error(transparent)]
    Runs(#[from] DurableAgentRunsError),
    /// Durable provider mutation or integrity verification failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Another cancellation occurrence already owns this lifecycle.
    #[error("Agent run has a different committed cancellation request")]
    ConflictingCancellation,
    /// An idempotency key was reused for different logical submission content.
    #[error("Agent submission key was reused with different content")]
    SubmissionConflict,
    /// A non-cancelled terminal run cannot accept cancellation.
    #[error("terminal Agent run cannot accept cancellation")]
    TerminalRun,
    /// Durable facts formed a state unsupported by this service mutation.
    #[error("Agent run is not in a cancellable durable state")]
    InvalidRunState,
    /// Framework-owned control event construction failed an invariant.
    #[error("Agent service control event could not be constructed")]
    ControlEventInvariant,
    /// Framework-owned control event data failed its embedded schema.
    #[error("Agent service control event schema validation failed: {source}")]
    ControlEventSchema {
        /// Closed schema validation result.
        #[source]
        source: GraphSchemaValidationError,
    },
}

impl AgentServiceError {
    const fn control_schema(source: GraphSchemaValidationError) -> Self {
        Self::ControlEventSchema { source }
    }
}

fn cancellation_failure(id: FailureId) -> Failure {
    Failure::new(
        id,
        FailureCategory::Cancelled,
        FailureCode::new("agent.service.cancelled_by_caller")
            .expect("static cancellation failure code is valid"),
        FailureOrigin::new("stateknot.runtime.agent_service")
            .expect("static cancellation failure origin is valid"),
        FailureMessage::new("The Agent run was cancelled by an authorized caller.")
            .expect("static cancellation message is valid"),
        RetryAdvice::Never,
    )
    .expect("static cancellation failure semantics are coherent")
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
