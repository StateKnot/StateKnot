// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt,
    io::{self, Write},
    sync::Arc,
    time::Instant,
};

use futures_util::StreamExt;
use reqwest::{Response, StatusCode, header::HeaderMap};
use serde::Serialize;
use serde_json::Value;
use stateknot_core::{
    BoundedJson, BoxStream, ContentPart, DurationMillis, ExecutionCount, Extensions, Failure,
    FailureCategory, FailureCode, FailureId, FailureMessage, FailureOrigin,
    GraphSchemaValidationError, InstructionContent, JsonLimits, ModelContext, ModelDescriptor,
    ModelError, ModelErrorPhase, ModelErrorProvenance, ModelEvent, ModelEventAccumulator,
    ModelEventKind, ModelModality, ModelProviderModelId, ModelProviderRequestId,
    ModelProviderResponseId, ModelRequest, ModelResponseMode, ModelSchemaRegistry, ModelStopReason,
    ModelUsage, RetryAdvice, SchemaReference, SecurityLabel,
};
use thiserror::Error;

use crate::{
    ApiKey, ApiKeyProvider, ApiKeyResolutionError, ProviderEndpoint, ProviderEndpointError,
    ProviderHttpOptions, http::build_client,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProviderKind {
    OpenAi,
    Anthropic,
}

impl ProviderKind {
    const fn origin(self) -> &'static str {
        match self {
            Self::OpenAi => "provider.openai",
            Self::Anthropic => "provider.anthropic",
        }
    }

    const fn path(self) -> &'static str {
        match self {
            Self::OpenAi => "responses",
            Self::Anthropic => "messages",
        }
    }
}

#[derive(Clone)]
pub(crate) struct AdapterCore {
    pub(crate) descriptor: ModelDescriptor,
    pub(crate) model_id: ModelProviderModelId,
    pub(crate) output_label: SecurityLabel,
    pub(crate) schemas: Arc<dyn ModelSchemaRegistry>,
    credentials: Arc<dyn ApiKeyProvider>,
    endpoint: ProviderEndpoint,
    pub(crate) options: ProviderHttpOptions,
    pub(crate) client: reqwest::Client,
    pub(crate) kind: ProviderKind,
}

impl AdapterCore {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        descriptor: ModelDescriptor,
        model_id: ModelProviderModelId,
        output_label: SecurityLabel,
        schemas: Arc<dyn ModelSchemaRegistry>,
        credentials: Arc<dyn ApiKeyProvider>,
        endpoint: ProviderEndpoint,
        options: ProviderHttpOptions,
        kind: ProviderKind,
    ) -> Result<Self, ModelAdapterBuildError> {
        if descriptor.capabilities().input_modalities().len() != 1
            || !descriptor
                .capabilities()
                .input_modalities()
                .contains(ModelModality::Text)
            || descriptor.capabilities().output_modalities().len() != 1
            || !descriptor
                .capabilities()
                .output_modalities()
                .contains(ModelModality::Text)
        {
            return Err(ModelAdapterBuildError::TextOnlyBindingRequired);
        }
        for profile in [
            descriptor.capabilities().tools().schema_profile(),
            descriptor
                .capabilities()
                .structured_output()
                .schema_profile(),
        ]
        .into_iter()
        .flatten()
        {
            if schemas.canonical_schema_bytes(profile).is_none() {
                return Err(ModelAdapterBuildError::SchemaProfileUnavailable);
            }
        }
        if kind == ProviderKind::Anthropic
            && descriptor.capabilities().supports_reasoning_summaries()
        {
            return Err(ModelAdapterBuildError::AnthropicReasoningSummariesUnsupported);
        }
        let client = build_client(options).map_err(|_| ModelAdapterBuildError::HttpClient)?;
        endpoint
            .join(kind.path())
            .map_err(ModelAdapterBuildError::Endpoint)?;
        Ok(Self {
            descriptor,
            model_id,
            output_label,
            schemas,
            credentials,
            endpoint,
            options,
            client,
            kind,
        })
    }

    pub(crate) fn url(&self) -> reqwest::Url {
        self.endpoint
            .join(self.kind.path())
            .expect("adapter endpoint path was validated during construction")
    }

    pub(crate) fn preflight(
        &self,
        context: &ModelContext,
        request: &ModelRequest,
        mode: ModelResponseMode,
    ) -> Result<(), ModelError> {
        if request.response_mode() != mode {
            return Err(self.error(
                context,
                ModelErrorPhase::Preparation,
                FailureCategory::InvalidInput,
                "request.response_mode",
                "The model request selected the wrong delivery mode.",
                RetryAdvice::Never,
                None,
                None,
                None,
            ));
        }
        if self
            .descriptor
            .capabilities()
            .satisfies(request.requirements())
            .is_err()
        {
            return Err(self.error(
                context,
                ModelErrorPhase::Preparation,
                FailureCategory::Unsupported,
                "request.capability_mismatch",
                "The model binding does not satisfy this request.",
                RetryAdvice::Never,
                None,
                None,
                None,
            ));
        }
        if !request.extensions().is_empty() {
            return Err(self.unsupported_request(context, "request.extensions"));
        }
        if request
            .instructions()
            .iter()
            .any(|instruction| matches!(instruction.content(), InstructionContent::Artifact(_)))
            || request.messages().iter().any(|message| {
                message
                    .parts()
                    .iter()
                    .any(|part| matches!(part, ContentPart::Artifact(_)))
            })
        {
            return Err(self.unsupported_request(context, "request.artifact_input"));
        }
        if request
            .messages()
            .iter()
            .any(|message| message.role() == stateknot_core::MessageRole::Tool)
        {
            return Err(self.unsupported_request(context, "request.tool_result_history"));
        }

        if let Some(schema) = request
            .text_output_format()
            .and_then(|format| format.schema())
        {
            let Some(profile) = self
                .descriptor
                .capabilities()
                .structured_output()
                .schema_profile()
            else {
                return Err(self.unsupported_request(context, "request.output_schema"));
            };
            self.validate_schema_document(context, profile, schema)?;
        }
        if request.tools().len() > 0 {
            let Some(profile) = self.descriptor.capabilities().tools().schema_profile() else {
                return Err(self.unsupported_request(context, "request.tool_schema"));
            };
            for tool in request.tools() {
                self.validate_schema_document(context, profile, tool.input_schema())?;
            }
        }
        Ok(())
    }

    fn validate_schema_document(
        &self,
        context: &ModelContext,
        profile: &SchemaReference,
        schema: &SchemaReference,
    ) -> Result<(), ModelError> {
        let Some(bytes) = self.schemas.canonical_schema_bytes(schema) else {
            return Err(self.schema_error(context, "schema.unavailable"));
        };
        let document = BoundedJson::from_slice_with_limits(bytes, JsonLimits::MAXIMUM)
            .map_err(|_| self.schema_error(context, "schema.invalid_document"))?;
        match self.schemas.validate(profile, &document) {
            Ok(()) => Ok(()),
            Err(GraphSchemaValidationError::Rejected) => {
                Err(self.schema_error(context, "schema.profile_rejected"))
            }
            Err(GraphSchemaValidationError::Unavailable) => {
                Err(self.schema_error(context, "schema.profile_unavailable"))
            }
            Err(_) => Err(self.schema_error(context, "schema.profile_unknown_failure")),
        }
    }

    pub(crate) fn schema_value(
        &self,
        context: &ModelContext,
        schema: &SchemaReference,
    ) -> Result<Value, ModelError> {
        let bytes = self
            .schemas
            .canonical_schema_bytes(schema)
            .ok_or_else(|| self.schema_error(context, "schema.unavailable"))?;
        BoundedJson::from_slice_with_limits(bytes, JsonLimits::MAXIMUM)
            .map(|value| value.as_value().clone())
            .map_err(|_| self.schema_error(context, "schema.invalid_document"))
    }

    fn schema_error(&self, context: &ModelContext, code: &'static str) -> ModelError {
        self.error(
            context,
            ModelErrorPhase::Preparation,
            FailureCategory::InvalidInput,
            code,
            "A pinned model schema could not be used safely.",
            RetryAdvice::Never,
            None,
            None,
            None,
        )
    }

    fn unsupported_request(&self, context: &ModelContext, code: &'static str) -> ModelError {
        self.error(
            context,
            ModelErrorPhase::Preparation,
            FailureCategory::Unsupported,
            code,
            "The provider adapter cannot represent this request without losing semantics.",
            RetryAdvice::Never,
            None,
            None,
            None,
        )
    }

    pub(crate) async fn resolve_key(&self, context: &ModelContext) -> Result<ApiKey, ModelError> {
        let resolved = wait_for(context, self.credentials.resolve(context)).await;
        match resolved {
            Ok(Ok(key)) => Ok(key),
            Ok(Err(ApiKeyResolutionError::Unavailable)) => Err(self.error(
                context,
                ModelErrorPhase::Preparation,
                FailureCategory::DependencyUnavailable,
                "credential.unavailable",
                "The provider credential source is temporarily unavailable.",
                RetryAdvice::SafeAfter {
                    delay: DurationMillis::new(250).expect("positive constant"),
                },
                None,
                None,
                None,
            )),
            Ok(Err(ApiKeyResolutionError::PermissionDenied)) => Err(self.error(
                context,
                ModelErrorPhase::Preparation,
                FailureCategory::PermissionDenied,
                "credential.permission_denied",
                "Provider credential access was denied.",
                RetryAdvice::Never,
                None,
                None,
                None,
            )),
            Err(reason) => Err(self.stop_error(context, ModelErrorPhase::Preparation, reason)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn error(
        &self,
        context: &ModelContext,
        phase: ModelErrorPhase,
        category: FailureCategory,
        code: &'static str,
        message: &'static str,
        retry: RetryAdvice,
        provider_request_id: Option<ModelProviderRequestId>,
        provider_response_id: Option<ModelProviderResponseId>,
        usage: Option<ModelUsage>,
    ) -> ModelError {
        let failure = Failure::new(
            FailureId::generate(),
            category,
            FailureCode::new(code).expect("adapter failure code constants are valid"),
            FailureOrigin::new(self.kind.origin())
                .expect("adapter failure origin constants are valid"),
            FailureMessage::new(message).expect("adapter public messages are valid"),
            retry,
        )
        .expect("adapter category and retry constants are coherent");
        ModelError::new(
            failure,
            phase,
            ModelErrorProvenance::new(
                context.attempt_id(),
                self.descriptor.metadata().identity().clone(),
                Some(self.model_id.clone()),
                provider_request_id,
                provider_response_id,
            ),
            usage,
        )
    }

    pub(crate) fn stop_error(
        &self,
        context: &ModelContext,
        phase: ModelErrorPhase,
        reason: ModelStopReason,
    ) -> ModelError {
        let (category, code, message) = match reason {
            ModelStopReason::Cancelled => (
                FailureCategory::Cancelled,
                "request.cancelled",
                "The model attempt was cancelled.",
            ),
            ModelStopReason::DeadlineExceeded => (
                FailureCategory::DeadlineExceeded,
                "request.deadline_exceeded",
                "The model attempt exceeded its deadline.",
            ),
            _ => (
                FailureCategory::Internal,
                "request.unknown_stop_reason",
                "The model attempt stopped for an unsupported reason.",
            ),
        };
        self.error(
            context,
            phase,
            category,
            code,
            message,
            RetryAdvice::Never,
            None,
            None,
            None,
        )
    }

    pub(crate) fn status_error(
        &self,
        context: &ModelContext,
        phase: ModelErrorPhase,
        status: StatusCode,
        headers: &HeaderMap,
        provider_request_id: Option<ModelProviderRequestId>,
    ) -> ModelError {
        let (category, code, message, retry) = match status.as_u16() {
            400 | 413 | 422 => (
                FailureCategory::InvalidInput,
                "http.invalid_request",
                "The model provider rejected the request.",
                RetryAdvice::Never,
            ),
            401 => (
                FailureCategory::Unauthenticated,
                "http.unauthenticated",
                "The model provider rejected authentication.",
                RetryAdvice::Never,
            ),
            403 => (
                FailureCategory::PermissionDenied,
                "http.permission_denied",
                "The model provider denied this operation.",
                RetryAdvice::Never,
            ),
            404 => (
                FailureCategory::NotFound,
                "http.not_found",
                "The configured model provider resource was not found.",
                RetryAdvice::Never,
            ),
            429 => (
                FailureCategory::RateLimited,
                "http.rate_limited",
                "The model provider rate limit prevented execution.",
                parse_retry_after(headers)
                    .map_or(RetryAdvice::Never, |delay| RetryAdvice::SafeAfter { delay }),
            ),
            500..=599 => (
                FailureCategory::DependencyUnavailable,
                "http.provider_unavailable",
                "The model provider is temporarily unavailable.",
                RetryAdvice::SafeAfter {
                    delay: DurationMillis::new(250).expect("positive constant"),
                },
            ),
            _ => (
                FailureCategory::DependencyUnavailable,
                "http.unexpected_status",
                "The model provider returned an unsupported status.",
                RetryAdvice::Never,
            ),
        };
        self.error(
            context,
            phase,
            category,
            code,
            message,
            retry,
            provider_request_id,
            None,
            None,
        )
    }

    pub(crate) fn transport_error(
        &self,
        context: &ModelContext,
        phase: ModelErrorPhase,
    ) -> ModelError {
        self.error(
            context,
            phase,
            FailureCategory::DependencyUnavailable,
            "http.transport",
            "The model provider transport failed.",
            RetryAdvice::Never,
            None,
            None,
            None,
        )
    }

    pub(crate) fn malformed_error(
        &self,
        context: &ModelContext,
        phase: ModelErrorPhase,
        provider_request_id: Option<ModelProviderRequestId>,
        provider_response_id: Option<ModelProviderResponseId>,
        usage: Option<ModelUsage>,
    ) -> ModelError {
        self.error(
            context,
            phase,
            FailureCategory::DataCorruption,
            "response.malformed",
            "The model provider returned a malformed response.",
            RetryAdvice::Never,
            provider_request_id,
            provider_response_id,
            usage,
        )
    }
}

impl fmt::Debug for AdapterCore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdapterCore")
            .field("descriptor", &self.descriptor)
            .field("model_id", &self.model_id)
            .field("endpoint", &self.endpoint)
            .field("options", &self.options)
            .field("provider", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Invalid first-party model adapter construction.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ModelAdapterBuildError {
    /// The descriptor included a modality not implemented by this binding.
    #[error("first-party v1 provider adapters require text-only input and output bindings")]
    TextOnlyBindingRequired,
    /// A descriptor schema profile was not installed in the offline registry.
    #[error("model descriptor schema profile is unavailable in the offline registry")]
    SchemaProfileUnavailable,
    /// The current Anthropic adapter does not expose readable reasoning summaries.
    #[error("Anthropic reasoning summaries are not supported by this adapter version")]
    AnthropicReasoningSummariesUnsupported,
    /// Secure client construction failed.
    #[error("provider HTTP client construction failed")]
    HttpClient,
    /// The fixed provider route could not be joined.
    #[error("invalid provider endpoint: {0}")]
    Endpoint(ProviderEndpointError),
}

pub(crate) async fn wait_for<T, F>(context: &ModelContext, future: F) -> Result<T, ModelStopReason>
where
    F: std::future::Future<Output = T>,
{
    if let Some(reason) = context.stop_reason_at(Instant::now()) {
        return Err(reason);
    }
    let deadline = tokio::time::Instant::from_std(context.deadline_instant());
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => Err(ModelStopReason::Cancelled),
        () = tokio::time::sleep_until(deadline) => {
            Err(context.stop_reason_at(Instant::now()).unwrap_or(ModelStopReason::DeadlineExceeded))
        }
        output = future => Ok(output),
    }
}

pub(crate) async fn bounded_body(
    core: &AdapterCore,
    context: &ModelContext,
    response: Response,
    maximum: usize,
    phase: ModelErrorPhase,
) -> Result<Vec<u8>, ModelError> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    loop {
        let next = wait_for(context, stream.next())
            .await
            .map_err(|reason| core.stop_error(context, phase, reason))?;
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(|_| core.transport_error(context, phase))?;
        if body.len().saturating_add(chunk.len()) > maximum {
            return Err(core.error(
                context,
                phase,
                FailureCategory::DataCorruption,
                "response.too_large",
                "The model provider response exceeded its configured byte ceiling.",
                RetryAdvice::Never,
                None,
                None,
                None,
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(crate) fn serialize_request<T: Serialize>(
    core: &AdapterCore,
    context: &ModelContext,
    value: &T,
) -> Result<Vec<u8>, ModelError> {
    let mut writer = RequestBodyWriter::new(core.options.maximum_request_bytes());
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.overflowed {
            core.error(
                context,
                ModelErrorPhase::Preparation,
                FailureCategory::InvalidInput,
                "request.too_large",
                "The serialized model request exceeded its configured byte ceiling.",
                RetryAdvice::Never,
                None,
                None,
                None,
            )
        } else {
            core.malformed_error(context, ModelErrorPhase::Preparation, None, None, None)
        });
    }
    Ok(writer.bytes)
}

struct RequestBodyWriter {
    bytes: Vec<u8>,
    maximum: usize,
    overflowed: bool,
}

impl RequestBodyWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum.min(4096)),
            maximum,
            overflowed: false,
        }
    }
}

impl Write for RequestBodyWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(length) = self.bytes.len().checked_add(buffer.len()) else {
            self.overflowed = true;
            return Err(io::Error::other("provider request byte limit exceeded"));
        };
        if length > self.maximum {
            self.overflowed = true;
            return Err(io::Error::other("provider request byte limit exceeded"));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn parse_provider_request_id(headers: &HeaderMap) -> Option<ModelProviderRequestId> {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| ModelProviderRequestId::new(value).ok())
}

fn parse_retry_after(headers: &HeaderMap) -> Option<DurationMillis> {
    const MAX_RETRY_SECONDS: u64 = 86_400;
    let seconds = headers
        .get("retry-after")?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()?;
    if seconds > MAX_RETRY_SECONDS {
        return None;
    }
    let millis = seconds.checked_mul(1000)?;
    DurationMillis::try_from(millis).ok()
}

pub(crate) fn empty_extensions() -> Extensions {
    Extensions::default()
}

pub(crate) struct EventEmitter<'a> {
    core: &'a AdapterCore,
    context: &'a ModelContext,
    accumulator: ModelEventAccumulator<'a>,
    next_sequence: u64,
    sender: tokio::sync::mpsc::Sender<Result<ModelEvent, ModelError>>,
}

impl<'a> EventEmitter<'a> {
    pub(crate) fn new(
        core: &'a AdapterCore,
        context: &'a ModelContext,
        request: &'a ModelRequest,
        sender: tokio::sync::mpsc::Sender<Result<ModelEvent, ModelError>>,
    ) -> Result<Self, ModelError> {
        let accumulator =
            ModelEventAccumulator::new(context.attempt_id(), &core.descriptor, request).map_err(
                |_| core.malformed_error(context, ModelErrorPhase::Preparation, None, None, None),
            )?;
        Ok(Self {
            core,
            context,
            accumulator,
            next_sequence: 0,
            sender,
        })
    }

    pub(crate) async fn emit(&mut self, kind: ModelEventKind) -> Result<(), EmitError> {
        let event = ModelEvent::new(
            self.context.attempt_id(),
            ExecutionCount::new(self.next_sequence),
            kind,
        )
        .map_err(|_| EmitError::Invalid(self.invalid()))?;
        self.accumulator
            .push(event.clone())
            .map_err(|_| EmitError::Invalid(self.invalid()))?;
        self.sender.send(Ok(event)).await.map_err(|_| {
            EmitError::Invalid(self.core.stop_error(
                self.context,
                ModelErrorPhase::Stream,
                ModelStopReason::Cancelled,
            ))
        })?;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| EmitError::Invalid(self.invalid()))?;
        Ok(())
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.accumulator.is_complete()
    }

    fn invalid(&self) -> ModelError {
        self.core
            .malformed_error(self.context, ModelErrorPhase::Stream, None, None, None)
    }
}

pub(crate) enum EmitError {
    Invalid(ModelError),
}

pub(crate) fn receiver_stream(
    receiver: tokio::sync::mpsc::Receiver<Result<ModelEvent, ModelError>>,
) -> BoxStream<'static, Result<ModelEvent, ModelError>> {
    Box::pin(futures_util::stream::unfold(
        receiver,
        |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
    ))
}
