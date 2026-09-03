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
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use stateknot_core::{
    BoundedJson, BoxFuture, DurationMillis, ErasedTool, Failure, FailureCategory, FailureCode,
    FailureId, FailureMessage, FailureOrigin, GraphSchemaValidator, ModelSchemaRegistry,
    RetryAdvice, ToolArtifacts, ToolCancellationSupport, ToolContext, ToolDescriptor, ToolError,
    ToolErrorPhase, ToolErrorProvenance, ToolExternalEffect, ToolIdempotency, ToolInput,
    ToolResourceAccess, ToolResult, ToolRisk, ToolStopReason,
};
use thiserror::Error;

use crate::{
    A2aClient, A2aClientAttemptIdentity, A2aClientAuthorizationError, A2aClientError,
    A2aClientErrorKind, A2aClientSecurityError, A2aMessage, A2aMessageRole, A2aPart,
    A2aSendConfiguration, A2aSendMessageRequest,
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

/// One immutable A2A agent-skill binding exposed as a durable `StateKnot` tool.
pub struct A2aRemoteAgent {
    descriptor: ToolDescriptor,
    client: A2aClient,
    skill_id: Box<str>,
    delivery: A2aRemoteAgentDelivery,
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
        validate_descriptor(&descriptor, &client, delivery)?;
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
        let part = A2aPart::data(input.value().as_value().clone())
            .and_then(|part| part.with_media_type("application/json"))
            .map_err(|_| {
                self.preparation_error(
                    &context,
                    FailureCategory::InvalidInput,
                    "input.protocol_mapping_failed",
                    "The A2A input could not be mapped into a bounded data part.",
                    RetryAdvice::Never,
                )
            })?;
        let message =
            A2aMessage::new(message_id, A2aMessageRole::User, vec![part]).map_err(|_| {
                self.preparation_error(
                    &context,
                    FailureCategory::Internal,
                    "request.message_invalid",
                    "The A2A request message could not be constructed.",
                    RetryAdvice::Never,
                )
            })?;
        let configuration = A2aSendConfiguration::new()
            .with_accepted_output_modes(self.output_modes.iter().map(ToString::to_string).collect())
            .map_err(|_| {
                self.preparation_error(
                    &context,
                    FailureCategory::Internal,
                    "request.output_modes_invalid",
                    "The pinned A2A output modes could not be encoded.",
                    RetryAdvice::Never,
                )
            })?
            .return_immediately(true);
        let request = A2aSendMessageRequest::new(message).with_configuration(configuration);
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

    fn call(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(self.call_inner(context, input))
    }
}

impl fmt::Debug for A2aRemoteAgent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("A2aRemoteAgent")
            .field("descriptor", &self.descriptor)
            .field("skill_id", &self.skill_id)
            .field("delivery", &self.delivery)
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

fn validate_descriptor(
    descriptor: &ToolDescriptor,
    client: &A2aClient,
    delivery: A2aRemoteAgentDelivery,
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
    if semantics.supports_status_query() || semantics.supports_compensation() {
        return Err(A2aRemoteAgentBuildError::RecoveryOperationsUnsupported);
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
    /// This adapter does not expose a separately authorized status or compensation operation.
    #[error("A2A remote-agent binding does not implement declared recovery operations")]
    RecoveryOperationsUnsupported,
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
}
