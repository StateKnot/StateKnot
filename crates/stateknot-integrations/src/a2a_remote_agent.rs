// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable `StateKnot` tool binding for one discovered A2A 1.0 agent skill.
//!
//! A binding freezes one Agent Card, one preferred and egress-pinned interface,
//! one advertised skill, local input/output schemas, and an explicit delivery
//! claim. A2A does not define a skill-routing field or require servers to
//! deduplicate `messageId`; the selected skill is discovery/security/media
//! evidence, while message-id deduplication is an operator-attested deployment
//! property. The adapter never retries a send. Once dispatch may have begun,
//! an uncertain result is reported as `Unknown` with `ReconcileFirst`.

use std::{
    collections::HashSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use stateknot_core::{
    BoundedJson, BoxFuture, Digest, DurationMillis, ErasedTool, Failure, FailureCategory,
    FailureCode, FailureId, FailureMessage, FailureOrigin, GraphSchemaValidator,
    ModelSchemaRegistry, RetryAdvice, ToolArtifacts, ToolCancellationSupport, ToolContext,
    ToolDescriptor, ToolError, ToolErrorPhase, ToolErrorProvenance, ToolExternalEffect,
    ToolIdempotency, ToolInput, ToolReconciliationContext, ToolReconciliationObservation,
    ToolReconciliationObservationError, ToolReconciliationProbeError, ToolResourceAccess,
    ToolResult, ToolResultProvenance, ToolRisk, ToolStopReason,
};
use thiserror::Error;

use crate::{
    A2aClient, A2aClientAttemptIdentity, A2aClientAuthorizationError, A2aClientError,
    A2aClientErrorKind, A2aClientSecurityError, A2aContractError, A2aListTasksRequest, A2aMessage,
    A2aMessageRole, A2aPart, A2aSendConfiguration, A2aSendMessageRequest, A2aSendMessageResponse,
    A2aTask,
};

/// Schema capabilities required by the A2A remote-agent adapter.
///
/// Agent Cards do not carry JSON Schemas for skill arguments or the resulting
/// task/message projection. A trusted local registry therefore remains the
/// executable schema authority for both boundaries.
pub trait A2aSchemaRegistry: ModelSchemaRegistry + GraphSchemaValidator {}

impl<T> A2aSchemaRegistry for T where T: ModelSchemaRegistry + GraphSchemaValidator {}

/// Delivery guarantee asserted for one exact remote deployment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum A2aRemoteAgentDelivery {
    /// Every physical attempt gets a new message ID and is sent at most once.
    /// An uncertain write must be reconciled rather than sent again blindly.
    AtMostOnce,
    /// The remote deployment durably deduplicates `messageId` for the complete
    /// retention/recovery window. A2A itself does not make this guarantee.
    MessageIdDeduplicated,
}

/// Enabled A2A reconciliation mechanism.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum A2aRemoteAgentRecoveryMode {
    /// No automated recovery claim is installed.
    Disabled,
    /// Query task pages by an operator-attested client context and exact
    /// original message ID in retained task history.
    ContextTaskHistory,
    /// Replay the exact original message ID against an operator-attested
    /// durable deduplication implementation.
    MessageIdReplay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum A2aRemoteAgentRecoveryKind {
    Disabled,
    ContextTaskHistory {
        maximum_pages: u8,
        history_length: u16,
        retry_after: DurationMillis,
    },
    MessageIdReplay {
        retry_after: DurationMillis,
    },
}

/// Explicit operator attestation controlling automated A2A reconciliation.
///
/// A2A 1.0 does not guarantee client context retention or `messageId`
/// deduplication. Enabling either strategy asserts a deployment-specific
/// property for at least the complete local invocation-ledger retention window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct A2aRemoteAgentRecovery {
    kind: A2aRemoteAgentRecoveryKind,
}

impl A2aRemoteAgentRecovery {
    /// Maximum number of 100-task pages inspected by one probe.
    pub const MAXIMUM_CONTEXT_PAGES: u8 = 16;
    /// A2A 1.0 maximum retained history suffix accepted by this client.
    pub const MAXIMUM_HISTORY_LENGTH: u16 = 256;

    /// Disables automatic reconciliation.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            kind: A2aRemoteAgentRecoveryKind::Disabled,
        }
    }

    /// Enables bounded context/task-history lookup under an operator attestation.
    ///
    /// The operator asserts that this exact remote deployment accepts and
    /// preserves client-supplied context IDs, exposes all matching tasks through
    /// stable `ListTasks` pagination, and retains the original user `messageId`
    /// in the requested history suffix for the full recovery window.
    pub fn operator_attested_context_task_history(
        maximum_pages: u8,
        history_length: u16,
        retry_after: DurationMillis,
    ) -> Result<Self, A2aRemoteAgentRecoveryError> {
        if maximum_pages == 0 || maximum_pages > Self::MAXIMUM_CONTEXT_PAGES {
            return Err(A2aRemoteAgentRecoveryError::InvalidMaximumPages);
        }
        if history_length == 0 || history_length > Self::MAXIMUM_HISTORY_LENGTH {
            return Err(A2aRemoteAgentRecoveryError::InvalidHistoryLength);
        }
        validate_recovery_delay(retry_after)?;
        Ok(Self {
            kind: A2aRemoteAgentRecoveryKind::ContextTaskHistory {
                maximum_pages,
                history_length,
                retry_after,
            },
        })
    }

    /// Enables exact-message replay under an operator deduplication attestation.
    ///
    /// The operator asserts that duplicate `messageId` values return the same
    /// semantic operation without reapplying the write for the full recovery
    /// window. This strategy is accepted only with `MessageIdDeduplicated`.
    pub fn operator_attested_message_id_replay(
        retry_after: DurationMillis,
    ) -> Result<Self, A2aRemoteAgentRecoveryError> {
        validate_recovery_delay(retry_after)?;
        Ok(Self {
            kind: A2aRemoteAgentRecoveryKind::MessageIdReplay { retry_after },
        })
    }

    /// Returns the selected recovery mechanism without exposing mutable policy.
    #[must_use]
    pub const fn mode(self) -> A2aRemoteAgentRecoveryMode {
        match self.kind {
            A2aRemoteAgentRecoveryKind::Disabled => A2aRemoteAgentRecoveryMode::Disabled,
            A2aRemoteAgentRecoveryKind::ContextTaskHistory { .. } => {
                A2aRemoteAgentRecoveryMode::ContextTaskHistory
            }
            A2aRemoteAgentRecoveryKind::MessageIdReplay { .. } => {
                A2aRemoteAgentRecoveryMode::MessageIdReplay
            }
        }
    }

    const fn retry_after(self) -> Option<DurationMillis> {
        match self.kind {
            A2aRemoteAgentRecoveryKind::Disabled => None,
            A2aRemoteAgentRecoveryKind::ContextTaskHistory { retry_after, .. }
            | A2aRemoteAgentRecoveryKind::MessageIdReplay { retry_after } => Some(retry_after),
        }
    }
}

impl Default for A2aRemoteAgentRecovery {
    fn default() -> Self {
        Self::disabled()
    }
}

fn validate_recovery_delay(retry_after: DurationMillis) -> Result<(), A2aRemoteAgentRecoveryError> {
    ToolReconciliationObservation::pending(retry_after)
        .map(|_| ())
        .map_err(A2aRemoteAgentRecoveryError::invalid_retry_delay)
}

/// Invalid operator-attested A2A recovery configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aRemoteAgentRecoveryError {
    /// A probe must inspect between one and sixteen pages.
    #[error("A2A reconciliation maximum pages must be between 1 and 16")]
    InvalidMaximumPages,
    /// A history suffix must contain between one and 256 messages.
    #[error("A2A reconciliation history length must be between 1 and 256")]
    InvalidHistoryLength,
    /// The durable polling interval violated core bounds.
    #[error("A2A reconciliation retry delay is invalid: {source}")]
    InvalidRetryDelay {
        /// Exact bounded-delay violation.
        #[source]
        source: ToolReconciliationObservationError,
    },
}

impl A2aRemoteAgentRecoveryError {
    const fn invalid_retry_delay(source: ToolReconciliationObservationError) -> Self {
        Self::InvalidRetryDelay { source }
    }
}

/// One immutable A2A agent-skill binding exposed as a durable `StateKnot` tool.
pub struct A2aRemoteAgent {
    descriptor: ToolDescriptor,
    client: A2aClient,
    skill_id: Box<str>,
    delivery: A2aRemoteAgentDelivery,
    recovery: A2aRemoteAgentRecovery,
    required_scopes: Arc<[Box<str>]>,
    output_modes: Arc<[Box<str>]>,
    schemas: Arc<dyn A2aSchemaRegistry>,
}

impl A2aRemoteAgent {
    /// Freezes an already discovered client as one exact executable tool.
    ///
    /// `skill_id` selects advertised security and media constraints; it is not
    /// placed on the wire because A2A 1.0 defines no skill-routing request
    /// field. The receiving agent chooses behavior from the user message.
    ///
    /// `MessageIdDeduplicated` is a trusted deployment assertion. Operators
    /// must prove that the remote service stores and deduplicates message IDs
    /// for at least the local invocation-ledger retention window.
    pub fn bind(
        descriptor: ToolDescriptor,
        client: A2aClient,
        skill_id: impl Into<String>,
        delivery: A2aRemoteAgentDelivery,
        schemas: Arc<dyn A2aSchemaRegistry>,
    ) -> Result<Self, A2aRemoteAgentBuildError> {
        Self::bind_with_recovery(
            descriptor,
            client,
            skill_id,
            delivery,
            A2aRemoteAgentRecovery::disabled(),
            schemas,
        )
    }

    /// Freezes an A2A binding with an explicit operator-attested recovery mode.
    ///
    /// # Errors
    ///
    /// Performs the same schema, card, security, media, descriptor, and
    /// delivery validation as [`Self::bind`], plus exact reconciliation-policy
    /// compatibility checks.
    pub fn bind_with_recovery(
        descriptor: ToolDescriptor,
        client: A2aClient,
        skill_id: impl Into<String>,
        delivery: A2aRemoteAgentDelivery,
        recovery: A2aRemoteAgentRecovery,
        schemas: Arc<dyn A2aSchemaRegistry>,
    ) -> Result<Self, A2aRemoteAgentBuildError> {
        validate_descriptor(&descriptor, &client, delivery, recovery)?;
        schemas
            .canonical_schema_bytes(descriptor.input_schema())
            .ok_or(A2aRemoteAgentBuildError::InputSchemaUnavailable)?;
        schemas
            .canonical_schema_bytes(descriptor.output_schema())
            .ok_or(A2aRemoteAgentBuildError::OutputSchemaUnavailable)?;

        let skill_id = skill_id.into();
        let skill = client
            .agent_card()
            .skills()
            .into_iter()
            .find(|skill| skill.id() == skill_id)
            .ok_or(A2aRemoteAgentBuildError::SkillUnavailable)?;
        let input_modes = if skill.input_modes().is_empty() {
            client.agent_card().default_input_modes()
        } else {
            skill.input_modes()
        };
        if !input_modes.iter().any(|mode| accepts_json(mode)) {
            return Err(A2aRemoteAgentBuildError::JsonInputModeUnsupported);
        }
        let output_modes = if skill.output_modes().is_empty() {
            client.agent_card().default_output_modes().to_vec()
        } else {
            skill.output_modes().to_vec()
        };
        if output_modes.is_empty() {
            return Err(A2aRemoteAgentBuildError::OutputModesUnavailable);
        }
        let required_scopes = client
            .skill_required_scopes(&skill_id)
            .ok_or(A2aRemoteAgentBuildError::SkillUnavailable)??;

        Ok(Self {
            descriptor,
            client,
            skill_id: skill_id.into_boxed_str(),
            delivery,
            recovery,
            required_scopes,
            output_modes: output_modes
                .into_iter()
                .map(String::into_boxed_str)
                .collect::<Vec<_>>()
                .into(),
            schemas,
        })
    }

    /// Returns the exact advertised skill used as binding evidence.
    #[must_use]
    pub fn skill_id(&self) -> &str {
        &self.skill_id
    }

    /// Returns the operator-attested delivery guarantee.
    #[must_use]
    pub const fn delivery(&self) -> A2aRemoteAgentDelivery {
        self.delivery
    }

    /// Returns the immutable operator-attested recovery policy.
    #[must_use]
    pub const fn recovery(&self) -> A2aRemoteAgentRecovery {
        self.recovery
    }

    fn preparation_error(
        &self,
        context: &ToolContext,
        category: FailureCategory,
        code: &'static str,
        message: &'static str,
        retry: RetryAdvice,
    ) -> ToolError {
        self.error(
            context,
            ToolErrorPhase::Preparation,
            category,
            code,
            message,
            retry,
            ToolExternalEffect::NotStarted,
        )
    }

    fn rejected_error(
        &self,
        context: &ToolContext,
        category: FailureCategory,
        code: &'static str,
        message: &'static str,
    ) -> ToolError {
        self.error(
            context,
            ToolErrorPhase::Execution,
            category,
            code,
            message,
            RetryAdvice::Never,
            ToolExternalEffect::NotApplied,
        )
    }

    fn unknown_outcome(&self, context: &ToolContext) -> ToolError {
        self.error(
            context,
            ToolErrorPhase::Execution,
            FailureCategory::AmbiguousExternalOutcome,
            "call.outcome_unknown",
            "The A2A message may have been accepted; reconcile it before recovery.",
            RetryAdvice::ReconcileFirst,
            ToolExternalEffect::Unknown,
        )
    }

    fn invalid_result(&self, context: &ToolContext) -> ToolError {
        self.error(
            context,
            ToolErrorPhase::Result,
            FailureCategory::DataCorruption,
            "result.invalid",
            "The A2A agent returned a result outside the pinned local contract.",
            RetryAdvice::Never,
            ToolExternalEffect::Applied,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn error(
        &self,
        context: &ToolContext,
        phase: ToolErrorPhase,
        category: FailureCategory,
        code: &'static str,
        message: &'static str,
        retry: RetryAdvice,
        effect: ToolExternalEffect,
    ) -> ToolError {
        let failure = Failure::new(
            FailureId::generate(),
            category,
            FailureCode::new(code).expect("A2A failure codes are valid constants"),
            FailureOrigin::new("protocol.a2a").expect("A2A failure origin is valid"),
            FailureMessage::new(message).expect("A2A public failure messages are valid"),
            retry,
        )
        .expect("A2A failure category and retry advice are coherent");
        ToolError::new(
            failure,
            phase,
            effect,
            ToolErrorProvenance::for_invocation(context, &self.descriptor),
        )
        .expect("A2A phase, risk evidence, and failure category are coherent")
    }

    #[allow(clippy::too_many_lines)]
    async fn call_inner(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> Result<ToolResult, ToolError> {
        input
            .validate_for(&context, &self.descriptor)
            .map_err(|_| {
                self.preparation_error(
                    &context,
                    FailureCategory::InvalidInput,
                    "input.binding_invalid",
                    "The A2A input does not match this invocation binding.",
                    RetryAdvice::Never,
                )
            })?;
        self.schemas
            .validate(self.descriptor.input_schema(), input.value())
            .map_err(|_| {
                self.preparation_error(
                    &context,
                    FailureCategory::InvalidInput,
                    "input.schema_rejected",
                    "The A2A input failed its pinned local schema.",
                    RetryAdvice::Never,
                )
            })?;

        if let Some(reason) = context.stop_reason_at(Instant::now()) {
            return Err(self.stop_before_dispatch(&context, reason));
        }
        let message_id = self.message_id(&context).ok_or_else(|| {
            self.preparation_error(
                &context,
                FailureCategory::Internal,
                "request.idempotency_key_missing",
                "The durable A2A idempotency key is unavailable.",
                RetryAdvice::Never,
            )
        })?;
        let context_id = self.context_id(
            context.tenant_id().as_str(),
            &context.run_id().to_string(),
            &context.thread_id().to_string(),
            &context.invocation_id().to_string(),
            &context.attempt_id().to_string(),
        );
        let request = self
            .request(message_id, context_id, input.value())
            .map_err(|_| {
                self.preparation_error(
                    &context,
                    FailureCategory::Internal,
                    "request.mapping_failed",
                    "The bounded A2A request could not be constructed.",
                    RetryAdvice::Never,
                )
            })?;
        let attempt = A2aClientAttemptIdentity::new(
            context.tenant_id().clone(),
            context.run_id(),
            context.invocation_id(),
            context.attempt_id(),
        );
        let dispatched = Arc::new(AtomicBool::new(false));
        let call = self.client.send_message_with_attempt(
            request,
            attempt,
            self.required_scopes.clone(),
            dispatched.clone(),
        );
        let response = wait_for_tool(&context, call)
            .await
            .map_err(|reason| {
                if dispatched.load(Ordering::Acquire) {
                    self.stop_after_dispatch(&context, reason)
                } else {
                    self.stop_before_dispatch(&context, reason)
                }
            })?
            .map_err(|error| self.client_error(&context, &error))?;

        let value = response
            .to_json()
            .map_err(|_| self.invalid_result(&context))?;
        let output =
            BoundedJson::try_from_value(value).map_err(|_| self.invalid_result(&context))?;
        self.schemas
            .validate(self.descriptor.output_schema(), &output)
            .map_err(|_| self.invalid_result(&context))?;
        let result =
            ToolResult::for_invocation(&context, &self.descriptor, output, ToolArtifacts::empty());
        result
            .validate_for(&context, &self.descriptor)
            .map_err(|_| self.invalid_result(&context))?;
        Ok(result)
    }

    fn message_id(&self, context: &ToolContext) -> Option<String> {
        match self.delivery {
            A2aRemoteAgentDelivery::AtMostOnce => {
                Some(format!("stateknot-attempt-{}", context.attempt_id()))
            }
            A2aRemoteAgentDelivery::MessageIdDeduplicated => context
                .idempotency_key()
                .map(|key| format!("stateknot-invocation-{key}")),
        }
    }

    fn request(
        &self,
        message_id: String,
        context_id: Option<String>,
        input: &BoundedJson,
    ) -> Result<A2aSendMessageRequest, A2aContractError> {
        let part = A2aPart::data(input.as_value().clone())?.with_media_type("application/json")?;
        let mut message = A2aMessage::new(message_id, A2aMessageRole::User, vec![part])?;
        if let Some(context_id) = context_id {
            message = message.with_context_id(context_id)?;
        }
        let configuration = A2aSendConfiguration::new()
            .with_accepted_output_modes(
                self.output_modes.iter().map(ToString::to_string).collect(),
            )?
            .return_immediately(true);
        Ok(A2aSendMessageRequest::new(message).with_configuration(configuration))
    }

    fn reconciliation_message_id(&self, context: &ToolReconciliationContext) -> Option<String> {
        match self.delivery {
            A2aRemoteAgentDelivery::AtMostOnce => {
                Some(format!("stateknot-attempt-{}", context.attempt_id()))
            }
            A2aRemoteAgentDelivery::MessageIdDeduplicated => context
                .idempotency_key()
                .map(|key| format!("stateknot-invocation-{key}")),
        }
    }

    fn reconciliation_context_id(&self, context: &ToolReconciliationContext) -> Option<String> {
        self.context_id(
            context.tenant_id().as_str(),
            &context.run_id().to_string(),
            &context.thread_id().to_string(),
            &context.invocation_id().to_string(),
            &context.attempt_id().to_string(),
        )
    }

    fn context_id(
        &self,
        tenant_id: &str,
        run_id: &str,
        thread_id: &str,
        invocation_id: &str,
        attempt_id: &str,
    ) -> Option<String> {
        if !matches!(
            self.recovery.kind,
            A2aRemoteAgentRecoveryKind::ContextTaskHistory { .. }
        ) {
            return None;
        }
        let mut material = b"stateknot.a2a.reconciliation-context.v1\0".to_vec();
        for value in [tenant_id, run_id, thread_id, invocation_id, attempt_id] {
            material.extend_from_slice(value.as_bytes());
            material.push(0);
        }
        let identity = serde_json_canonicalizer::to_vec(self.descriptor.metadata().identity())
            .expect("validated capability identities have a canonical JSON representation");
        material.extend_from_slice(&identity);
        material.push(0);
        material.extend_from_slice(self.client.agent_card_digest().as_bytes());
        Some(format!(
            "stateknot-context-{}",
            digest_bytes_hex(Digest::sha256(material).as_bytes())
        ))
    }

    fn probe_error(
        category: FailureCategory,
        code: &'static str,
        message: &'static str,
        retry: RetryAdvice,
    ) -> ToolReconciliationProbeError {
        let failure = Failure::new(
            FailureId::generate(),
            category,
            FailureCode::new(code).expect("A2A reconciliation failure code is valid"),
            FailureOrigin::new("protocol.a2a.reconciliation")
                .expect("A2A reconciliation failure origin is valid"),
            FailureMessage::new(message)
                .expect("A2A reconciliation public failure message is valid"),
            retry,
        )
        .expect("A2A reconciliation failure semantics are coherent");
        ToolReconciliationProbeError::new(failure)
            .expect("A2A reconciliation retry advice is bounded and non-recursive")
    }

    fn invalid_probe_contract(
        code: &'static str,
        message: &'static str,
    ) -> ToolReconciliationProbeError {
        Self::probe_error(
            FailureCategory::DataCorruption,
            code,
            message,
            RetryAdvice::Never,
        )
    }

    fn probe_client_error(&self, error: &A2aClientError) -> ToolReconciliationProbeError {
        if error.authorization_error() == Some(A2aClientAuthorizationError::PermissionDenied) {
            return Self::probe_error(
                FailureCategory::PermissionDenied,
                "authorization.permission_denied",
                "A2A reconciliation authorization was denied.",
                RetryAdvice::Never,
            );
        }
        if error.kind() == A2aClientErrorKind::Remote
            && definitive_rejection(error.remote_code()).is_some()
        {
            return Self::probe_error(
                FailureCategory::Unsupported,
                "probe.remote_operation_rejected",
                "The A2A agent rejected the configured reconciliation operation.",
                RetryAdvice::Never,
            );
        }
        match error.kind() {
            A2aClientErrorKind::Authorization | A2aClientErrorKind::Transport => Self::probe_error(
                FailureCategory::DependencyUnavailable,
                "probe.dependency_unavailable",
                "The A2A reconciliation dependency is temporarily unavailable.",
                RetryAdvice::SafeAfter {
                    delay: self
                        .recovery
                        .retry_after()
                        .expect("enabled reconciliation has a retry delay"),
                },
            ),
            A2aClientErrorKind::Remote => Self::probe_error(
                FailureCategory::DependencyUnavailable,
                "probe.remote_unavailable",
                "The A2A agent could not complete reconciliation.",
                RetryAdvice::SafeAfter {
                    delay: self
                        .recovery
                        .retry_after()
                        .expect("enabled reconciliation has a retry delay"),
                },
            ),
            A2aClientErrorKind::Request | A2aClientErrorKind::Capability => Self::probe_error(
                FailureCategory::Unsupported,
                "probe.operation_unsupported",
                "The pinned A2A reconciliation operation is unavailable.",
                RetryAdvice::Never,
            ),
            A2aClientErrorKind::HttpProtocol | A2aClientErrorKind::InvalidResponse => {
                Self::invalid_probe_contract(
                    "probe.invalid_response",
                    "The A2A reconciliation response violated the pinned protocol contract.",
                )
            }
        }
    }

    fn validate_reconciliation_input(
        &self,
        context: &ToolReconciliationContext,
        input: &ToolInput,
    ) -> Result<(), ToolReconciliationProbeError> {
        if context.validate_for(&self.descriptor).is_err()
            || input.schema() != self.descriptor.input_schema()
            || u64::try_from(input.value().stats().compact_bytes()).unwrap_or(u64::MAX)
                > self.descriptor.limits().max_input_bytes().get()
            || self
                .schemas
                .validate(self.descriptor.input_schema(), input.value())
                .is_err()
        {
            return Err(Self::invalid_probe_contract(
                "probe.input_binding_invalid",
                "The durable A2A reconciliation input violated its pinned contract.",
            ));
        }
        Ok(())
    }

    fn reconciled_result(
        &self,
        context: &ToolReconciliationContext,
        response: &A2aSendMessageResponse,
    ) -> Result<ToolResult, ToolReconciliationProbeError> {
        let value = response.to_json().map_err(|_| {
            Self::invalid_probe_contract(
                "probe.result_encoding_invalid",
                "The reconciled A2A result could not be encoded safely.",
            )
        })?;
        let output = BoundedJson::try_from_value(value).map_err(|_| {
            Self::invalid_probe_contract(
                "probe.result_limit_exceeded",
                "The reconciled A2A result exceeded bounded JSON limits.",
            )
        })?;
        self.schemas
            .validate(self.descriptor.output_schema(), &output)
            .map_err(|_| {
                Self::invalid_probe_contract(
                    "probe.result_schema_invalid",
                    "The reconciled A2A result violated its pinned output schema.",
                )
            })?;
        Ok(ToolResult::new(
            ToolResultProvenance::new(
                context.invocation_id(),
                context.attempt_id(),
                self.descriptor.metadata().identity().clone(),
            ),
            self.descriptor.output_schema().clone(),
            output,
            ToolArtifacts::empty(),
        ))
    }

    fn matching_message_occurrences(
        task: &A2aTask,
        expected_message: &A2aMessage,
    ) -> Result<usize, ToolReconciliationProbeError> {
        let task_bound_message = expected_message
            .clone()
            .with_task_id(task.id().to_owned())
            .map_err(|_| {
                Self::invalid_probe_contract(
                    "probe.task_binding_invalid",
                    "The A2A reconciliation task identity could not be validated.",
                )
            })?;
        let mut occurrences = 0_usize;
        for message in task
            .history()
            .iter()
            .filter(|message| message.message_id() == expected_message.message_id())
        {
            if message != expected_message && message != &task_bound_message {
                return Err(Self::invalid_probe_contract(
                    "probe.message_mismatch",
                    "The A2A task history changed the original message payload.",
                ));
            }
            occurrences += 1;
        }
        Ok(occurrences)
    }

    async fn reconcile_context_task_history(
        &self,
        context: &ToolReconciliationContext,
        expected_message: &A2aMessage,
        context_id: String,
        maximum_pages: u8,
        history_length: u16,
        retry_after: DurationMillis,
    ) -> Result<ToolReconciliationObservation, ToolReconciliationProbeError> {
        let attempt = A2aClientAttemptIdentity::new(
            context.tenant_id().clone(),
            context.run_id(),
            context.invocation_id(),
            context.attempt_id(),
        );
        let mut page_token = None;
        let mut seen_tasks = HashSet::new();
        let mut matched_task: Option<A2aTask> = None;
        for page_index in 0..maximum_pages {
            let mut request = A2aListTasksRequest::new()
                .with_context_id(context_id.clone())
                .and_then(|request| request.with_page_size(A2aListTasksRequest::MAX_PAGE_SIZE))
                .and_then(|request| request.with_history_length(u32::from(history_length)))
                .map(|request| request.include_artifacts(true))
                .map_err(|_| {
                    Self::invalid_probe_contract(
                        "probe.request_invalid",
                        "The bounded A2A reconciliation query could not be constructed.",
                    )
                })?;
            if let Some(token) = page_token.take() {
                request = request.with_page_token(token).map_err(|_| {
                    Self::invalid_probe_contract(
                        "probe.cursor_invalid",
                        "The A2A reconciliation cursor violated protocol bounds.",
                    )
                })?;
            }
            let page = self
                .client
                .list_tasks_with_attempt(request, attempt.clone(), self.required_scopes.clone())
                .await
                .map_err(|error| self.probe_client_error(&error))?;
            for task in page.tasks() {
                if !seen_tasks.insert(task.id().to_owned()) {
                    return Err(Self::invalid_probe_contract(
                        "probe.duplicate_task",
                        "The A2A stable task snapshot repeated a task identity.",
                    ));
                }
                let occurrences = Self::matching_message_occurrences(task, expected_message)?;
                if occurrences > 1 || (occurrences == 1 && matched_task.is_some()) {
                    return Err(Self::invalid_probe_contract(
                        "probe.ambiguous_match",
                        "More than one A2A task history matched the original message.",
                    ));
                }
                if occurrences == 1 {
                    matched_task = Some(task.clone());
                }
            }
            page_token = page.next_page_token().map(ToOwned::to_owned);
            if page_token.is_none() {
                return match matched_task {
                    Some(task) => {
                        let response = A2aSendMessageResponse::Task(task);
                        self.reconciled_result(context, &response)
                            .map(ToolReconciliationObservation::Result)
                    }
                    None => ToolReconciliationObservation::pending(retry_after).map_err(|_| {
                        Self::invalid_probe_contract(
                            "probe.retry_delay_invalid",
                            "The configured A2A reconciliation delay is invalid.",
                        )
                    }),
                };
            }
            if page_index + 1 == maximum_pages {
                return Err(Self::probe_error(
                    FailureCategory::RateLimited,
                    "probe.scan_limit_exceeded",
                    "The A2A task snapshot exceeds the configured reconciliation scan bound.",
                    RetryAdvice::Never,
                ));
            }
        }
        Err(Self::invalid_probe_contract(
            "probe.scan_invariant",
            "The bounded A2A reconciliation scan ended unexpectedly.",
        ))
    }

    async fn reconcile_inner(
        &self,
        context: ToolReconciliationContext,
        input: ToolInput,
    ) -> Result<ToolReconciliationObservation, ToolReconciliationProbeError> {
        self.validate_reconciliation_input(&context, &input)?;
        let message_id = self.reconciliation_message_id(&context).ok_or_else(|| {
            Self::invalid_probe_contract(
                "probe.idempotency_key_missing",
                "The original A2A message identity cannot be reconstructed.",
            )
        })?;
        match self.recovery.kind {
            A2aRemoteAgentRecoveryKind::Disabled => Err(Self::probe_error(
                FailureCategory::Unsupported,
                "probe.disabled",
                "Automated A2A reconciliation is disabled.",
                RetryAdvice::Never,
            )),
            A2aRemoteAgentRecoveryKind::ContextTaskHistory {
                maximum_pages,
                history_length,
                retry_after,
            } => {
                let context_id = self.reconciliation_context_id(&context).ok_or_else(|| {
                    Self::invalid_probe_contract(
                        "probe.context_missing",
                        "The opaque A2A reconciliation context cannot be reconstructed.",
                    )
                })?;
                let expected_request = self
                    .request(message_id, Some(context_id.clone()), input.value())
                    .map_err(|_| {
                        Self::invalid_probe_contract(
                            "probe.request_invalid",
                            "The original A2A message cannot be reconstructed.",
                        )
                    })?;
                self.reconcile_context_task_history(
                    &context,
                    expected_request.message(),
                    context_id,
                    maximum_pages,
                    history_length,
                    retry_after,
                )
                .await
            }
            A2aRemoteAgentRecoveryKind::MessageIdReplay { .. } => {
                let request = self.request(message_id, None, input.value()).map_err(|_| {
                    Self::invalid_probe_contract(
                        "probe.request_invalid",
                        "The original A2A request cannot be reconstructed.",
                    )
                })?;
                let attempt = A2aClientAttemptIdentity::new(
                    context.tenant_id().clone(),
                    context.run_id(),
                    context.invocation_id(),
                    context.attempt_id(),
                );
                let response = self
                    .client
                    .send_message_with_attempt(
                        request,
                        attempt,
                        self.required_scopes.clone(),
                        Arc::new(AtomicBool::new(false)),
                    )
                    .await
                    .map_err(|error| self.probe_client_error(&error))?;
                self.reconciled_result(&context, &response)
                    .map(ToolReconciliationObservation::Result)
            }
        }
    }

    fn client_error(&self, context: &ToolContext, error: &A2aClientError) -> ToolError {
        if !error.was_dispatched() {
            return match (error.kind(), error.authorization_error()) {
                (
                    A2aClientErrorKind::Authorization,
                    Some(A2aClientAuthorizationError::PermissionDenied),
                ) => self.preparation_error(
                    context,
                    FailureCategory::PermissionDenied,
                    "authorization.permission_denied",
                    "A2A authorization access was denied.",
                    RetryAdvice::Never,
                ),
                (A2aClientErrorKind::Authorization, _) => self.preparation_error(
                    context,
                    FailureCategory::DependencyUnavailable,
                    "authorization.unavailable",
                    "The A2A authorization source is temporarily unavailable.",
                    RetryAdvice::SafeAfter {
                        delay: DurationMillis::new(250).expect("positive constant"),
                    },
                ),
                (A2aClientErrorKind::Capability, _) => self.preparation_error(
                    context,
                    FailureCategory::Unsupported,
                    "call.capability_unavailable",
                    "The pinned A2A agent does not support this operation.",
                    RetryAdvice::Never,
                ),
                (A2aClientErrorKind::Request, _) => self.preparation_error(
                    context,
                    FailureCategory::Internal,
                    "request.encoding_failed",
                    "The bounded A2A request could not be encoded.",
                    RetryAdvice::Never,
                ),
                _ => self.preparation_error(
                    context,
                    FailureCategory::Internal,
                    "call.failed_before_dispatch",
                    "The A2A attempt failed before dispatch.",
                    RetryAdvice::Never,
                ),
            };
        }

        if error.kind() == A2aClientErrorKind::Remote {
            if let Some((category, code, message)) = definitive_rejection(error.remote_code()) {
                return self.rejected_error(context, category, code, message);
            }
        }
        self.unknown_outcome(context)
    }

    fn stop_before_dispatch(&self, context: &ToolContext, reason: ToolStopReason) -> ToolError {
        let (category, code, message) = stop_failure(reason);
        self.preparation_error(context, category, code, message, RetryAdvice::Never)
    }

    fn stop_after_dispatch(&self, context: &ToolContext, _reason: ToolStopReason) -> ToolError {
        self.unknown_outcome(context)
    }
}

impl ErasedTool for A2aRemoteAgent {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn supports_reconciliation(&self) -> bool {
        self.recovery.mode() != A2aRemoteAgentRecoveryMode::Disabled
    }

    fn call(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(self.call_inner(context, input))
    }

    fn reconcile(
        &self,
        context: ToolReconciliationContext,
        input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolReconciliationObservation, ToolReconciliationProbeError>> {
        Box::pin(self.reconcile_inner(context, input))
    }
}

impl fmt::Debug for A2aRemoteAgent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("A2aRemoteAgent")
            .field("descriptor", &self.descriptor)
            .field("skill_id", &self.skill_id)
            .field("delivery", &self.delivery)
            .field("recovery", &self.recovery.mode())
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

fn validate_descriptor(
    descriptor: &ToolDescriptor,
    client: &A2aClient,
    delivery: A2aRemoteAgentDelivery,
    recovery: A2aRemoteAgentRecovery,
) -> Result<(), A2aRemoteAgentBuildError> {
    let semantics = descriptor.semantics();
    let valid_delivery = matches!(
        (delivery, semantics.risk(), semantics.idempotency()),
        (
            A2aRemoteAgentDelivery::AtMostOnce,
            ToolRisk::NonIdempotentWrite,
            ToolIdempotency::Unsupported
        ) | (
            A2aRemoteAgentDelivery::MessageIdDeduplicated,
            ToolRisk::IdempotentWrite,
            ToolIdempotency::RequiredKey
        )
    );
    if !valid_delivery {
        return Err(A2aRemoteAgentBuildError::DeliverySemanticsMismatch);
    }
    let recovery_enabled = recovery.mode() != A2aRemoteAgentRecoveryMode::Disabled;
    if semantics.supports_status_query() != recovery_enabled || semantics.supports_compensation() {
        return Err(A2aRemoteAgentBuildError::RecoveryOperationsUnsupported);
    }
    if recovery.mode() == A2aRemoteAgentRecoveryMode::MessageIdReplay
        && delivery != A2aRemoteAgentDelivery::MessageIdDeduplicated
    {
        return Err(A2aRemoteAgentBuildError::RecoveryDeliveryMismatch);
    }
    if descriptor.invocation().cancellation() != ToolCancellationSupport::Cooperative {
        return Err(A2aRemoteAgentBuildError::CancellationContractMismatch);
    }
    if descriptor.invocation().supports_progress_events() {
        return Err(A2aRemoteAgentBuildError::ProgressUnsupported);
    }
    let resources = descriptor.resources();
    if resources.network() != ToolResourceAccess::ReadWrite {
        return Err(A2aRemoteAgentBuildError::NetworkAccessMismatch);
    }
    if resources.filesystem() != ToolResourceAccess::None {
        return Err(A2aRemoteAgentBuildError::FilesystemAccessUnsupported);
    }
    if resources.executes_dynamic_code() {
        return Err(A2aRemoteAgentBuildError::DynamicCodeUnsupported);
    }
    if resources.requires_credentials() != client.requires_credentials() {
        return Err(A2aRemoteAgentBuildError::CredentialRequirementMismatch);
    }
    Ok(())
}

fn accepts_json(value: &str) -> bool {
    value.parse::<mime::Mime>().is_ok_and(|mode| {
        let type_name = mode.type_().as_str();
        let subtype = mode.subtype().as_str();
        (type_name == "application" && matches!(subtype, "json" | "*"))
            || (type_name == "*" && subtype == "*")
    })
}

fn digest_bytes_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn definitive_rejection(
    code: Option<i32>,
) -> Option<(FailureCategory, &'static str, &'static str)> {
    match code? {
        a2a::error_code::PARSE_ERROR
        | a2a::error_code::INVALID_REQUEST
        | a2a::error_code::INVALID_PARAMS => Some((
            FailureCategory::InvalidInput,
            "call.remote_request_rejected",
            "The A2A agent rejected the request before applying it.",
        )),
        a2a::error_code::METHOD_NOT_FOUND
        | a2a::error_code::UNSUPPORTED_OPERATION
        | a2a::error_code::CONTENT_TYPE_NOT_SUPPORTED
        | a2a::error_code::EXTENSION_SUPPORT_REQUIRED
        | a2a::error_code::VERSION_NOT_SUPPORTED => Some((
            FailureCategory::Unsupported,
            "call.remote_operation_unsupported",
            "The A2A agent rejected the pinned protocol operation.",
        )),
        _ => None,
    }
}

async fn wait_for_tool<T, F>(context: &ToolContext, future: F) -> Result<T, ToolStopReason>
where
    F: std::future::Future<Output = T>,
{
    if let Some(reason) = context.stop_reason_at(Instant::now()) {
        return Err(reason);
    }
    let deadline = tokio::time::Instant::from_std(context.deadline_instant());
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => Err(ToolStopReason::Cancelled),
        () = tokio::time::sleep_until(deadline) => {
            Err(context.stop_reason_at(Instant::now()).unwrap_or(ToolStopReason::DeadlineExceeded))
        }
        output = future => Ok(output),
    }
}

const fn stop_failure(reason: ToolStopReason) -> (FailureCategory, &'static str, &'static str) {
    match reason {
        ToolStopReason::Cancelled => (
            FailureCategory::Cancelled,
            "call.cancelled",
            "The A2A attempt was cancelled before dispatch.",
        ),
        ToolStopReason::DeadlineExceeded => (
            FailureCategory::DeadlineExceeded,
            "call.deadline_exceeded",
            "The A2A attempt deadline elapsed before dispatch.",
        ),
        _ => (
            FailureCategory::Internal,
            "call.unknown_stop_reason",
            "The A2A attempt stopped for an unsupported reason.",
        ),
    }
}

/// Closed failure while constructing an exact A2A remote-agent binding.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aRemoteAgentBuildError {
    /// Descriptor risk/idempotency does not match the delivery assertion.
    #[error("A2A delivery assertion does not match descriptor semantics")]
    DeliverySemanticsMismatch,
    /// Descriptor recovery claims differ from the configured adapter policy,
    /// or unsupported compensation was declared.
    #[error("A2A remote-agent recovery policy does not match descriptor semantics")]
    RecoveryOperationsUnsupported,
    /// Replay recovery requires an exact durable message-ID deduplication claim.
    #[error("A2A message replay recovery requires message-ID-deduplicated delivery")]
    RecoveryDeliveryMismatch,
    /// The adapter observes cancellation and requires that fact in the descriptor.
    #[error("A2A remote-agent binding requires cooperative cancellation semantics")]
    CancellationContractMismatch,
    /// Streaming progress is not bridged by this unary durable binding.
    #[error("A2A remote-agent binding does not support declared progress events")]
    ProgressUnsupported,
    /// Message submission requires read/write network policy.
    #[error("A2A remote-agent binding requires read/write network access")]
    NetworkAccessMismatch,
    /// This adapter does not access a local filesystem.
    #[error("A2A remote-agent binding does not support filesystem access")]
    FilesystemAccessUnsupported,
    /// This adapter does not execute invocation-supplied code.
    #[error("A2A remote-agent binding does not support dynamic code execution")]
    DynamicCodeUnsupported,
    /// Descriptor credential policy differed from the discovered client selection.
    #[error("A2A descriptor credential requirement does not match client security")]
    CredentialRequirementMismatch,
    /// The exact local input schema was absent.
    #[error("A2A input schema is unavailable in the local registry")]
    InputSchemaUnavailable,
    /// The exact local output schema was absent.
    #[error("A2A output schema is unavailable in the local registry")]
    OutputSchemaUnavailable,
    /// The exact advertised skill was absent.
    #[error("A2A skill is unavailable in the frozen Agent Card")]
    SkillUnavailable,
    /// The selected skill/card cannot accept a structured JSON data part.
    #[error("A2A skill does not advertise a compatible JSON input mode")]
    JsonInputModeUnsupported,
    /// The selected skill/card did not advertise any output mode.
    #[error("A2A skill does not advertise an output mode")]
    OutputModesUnavailable,
    /// Skill-level security cannot be satisfied by the frozen client selection.
    #[error(transparent)]
    SkillSecurity(#[from] A2aClientSecurityError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_mode_matching_is_deliberately_narrow() {
        for accepted in ["application/json", "application/*", "*/*"] {
            assert!(accepts_json(accepted), "expected {accepted} to accept JSON");
        }
        for rejected in [
            "text/plain",
            "application/problem+json",
            "application/json invalid",
            "",
        ] {
            assert!(
                !accepts_json(rejected),
                "expected {rejected} to reject the exact JSON data-part mapping"
            );
        }
    }

    #[test]
    fn only_pre_execution_protocol_rejections_prove_not_applied() {
        assert!(definitive_rejection(Some(a2a::error_code::INVALID_PARAMS)).is_some());
        assert!(definitive_rejection(Some(a2a::error_code::CONTENT_TYPE_NOT_SUPPORTED)).is_some());
        assert!(definitive_rejection(Some(a2a::error_code::INTERNAL_ERROR)).is_none());
        assert!(definitive_rejection(Some(a2a::error_code::INVALID_AGENT_RESPONSE)).is_none());
        assert!(definitive_rejection(None).is_none());
    }

    #[test]
    fn recovery_attestations_enforce_bounded_polling_and_scan_work() {
        assert_eq!(
            A2aRemoteAgentRecovery::operator_attested_context_task_history(
                0,
                1,
                DurationMillis::new(250).unwrap(),
            )
            .unwrap_err(),
            A2aRemoteAgentRecoveryError::InvalidMaximumPages
        );
        assert_eq!(
            A2aRemoteAgentRecovery::operator_attested_context_task_history(
                1,
                0,
                DurationMillis::new(250).unwrap(),
            )
            .unwrap_err(),
            A2aRemoteAgentRecoveryError::InvalidHistoryLength
        );
        assert!(matches!(
            A2aRemoteAgentRecovery::operator_attested_message_id_replay(DurationMillis::ZERO),
            Err(A2aRemoteAgentRecoveryError::InvalidRetryDelay { .. })
        ));
        assert_eq!(
            A2aRemoteAgentRecovery::operator_attested_context_task_history(
                2,
                32,
                DurationMillis::new(250).unwrap(),
            )
            .unwrap()
            .mode(),
            A2aRemoteAgentRecoveryMode::ContextTaskHistory
        );
    }
}
