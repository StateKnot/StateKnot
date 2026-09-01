// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt, sync::Arc};

use futures_util::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderValue};
use serde_json::{Map, Value, json};
use stateknot_core::{
    BoundedJson, BoxFuture, BoxStream, CapabilityIdentity, ContentMetadata, ContentPart,
    ContentSource, ExecutionCount, Extensions, InstructionContent, JsonContent, MessageRole, Model,
    ModelContext, ModelDescriptor, ModelError, ModelErrorPhase, ModelEventKind, ModelFinishReason,
    ModelOutputDelta, ModelOutputItem, ModelOutputStart, ModelProviderModelId,
    ModelProviderRequestId, ModelProviderResponseId, ModelRequest, ModelResponse,
    ModelResponseMode, ModelResponseProvenance, ModelSchemaRegistry, ModelStreamChunk,
    ModelTextOutputFormat, ModelToolCallProposal, ModelToolSelection, ModelUsage, RetryAdvice,
    SchemaReference, SecurityLabel, TextContent, TokenCount,
};

use crate::{
    ApiKeyProvider, ModelAdapterBuildError, ProviderEndpoint, ProviderHttpOptions,
    adapter::{
        AdapterCore, EmitError, EventEmitter, ProviderKind, bounded_body, empty_extensions,
        parse_provider_request_id, receiver_stream, serialize_request, wait_for,
    },
    sse::{SseDecoder, SseEvent},
};

const ANTHROPIC_VERSION: &str = "2023-06-01";

/// Anthropic Messages API model binding with explicit usage normalization.
#[derive(Clone)]
pub struct AnthropicMessagesModel {
    core: AdapterCore,
}

impl AnthropicMessagesModel {
    /// Constructs one immutable Anthropic Messages binding.
    ///
    /// The descriptor must be text-only and must not advertise readable
    /// reasoning summaries. Generic JSON mode is rejected per attempt because
    /// Anthropic's stable contract exposes schema-constrained output instead.
    ///
    /// # Errors
    ///
    /// Returns [`ModelAdapterBuildError`] for an incoherent descriptor,
    /// unavailable profile, invalid endpoint, or HTTP client failure.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        descriptor: ModelDescriptor,
        model_id: ModelProviderModelId,
        output_label: SecurityLabel,
        schemas: Arc<dyn ModelSchemaRegistry>,
        credentials: Arc<dyn ApiKeyProvider>,
        endpoint: ProviderEndpoint,
        options: ProviderHttpOptions,
    ) -> Result<Self, ModelAdapterBuildError> {
        Ok(Self {
            core: AdapterCore::new(
                descriptor,
                model_id,
                output_label,
                schemas,
                credentials,
                endpoint,
                options,
                ProviderKind::Anthropic,
            )?,
        })
    }

    async fn invoke_once(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> Result<ModelResponse, ModelError> {
        self.core
            .preflight(&context, &request, ModelResponseMode::Complete)?;
        provider_preflight(&self.core, &context, &request)?;
        let body = build_request(&self.core, &context, &request, false)?;
        let body = serialize_request(&self.core, &context, &body)?;
        let key = self.core.resolve_key(&context).await?;
        let api_key = HeaderValue::from_str(key.expose_secret()).map_err(|_| {
            self.core
                .malformed_error(&context, ModelErrorPhase::Preparation, None, None, None)
        })?;
        let response = wait_for(
            &context,
            self.core
                .client
                .post(self.core.url())
                .header("x-api-key", api_key)
                .header("anthropic-version", ANTHROPIC_VERSION)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json")
                .body(body)
                .send(),
        )
        .await
        .map_err(|reason| {
            self.core
                .stop_error(&context, ModelErrorPhase::Dispatch, reason)
        })?
        .map_err(|_| {
            self.core
                .transport_error(&context, ModelErrorPhase::Dispatch)
        })?;

        let status = response.status();
        let provider_request_id = parse_provider_request_id(response.headers());
        if !status.is_success() {
            return Err(self.core.status_error(
                &context,
                ModelErrorPhase::Response,
                status,
                response.headers(),
                provider_request_id,
            ));
        }
        if !is_json_content_type(response.headers()) {
            return Err(self.core.malformed_error(
                &context,
                ModelErrorPhase::Response,
                provider_request_id,
                None,
                None,
            ));
        }
        let bytes = bounded_body(
            &self.core,
            &context,
            response,
            self.core.options.maximum_response_bytes(),
            ModelErrorPhase::Response,
        )
        .await?;
        let value = BoundedJson::from_slice(&bytes).map_err(|_| {
            self.core.malformed_error(
                &context,
                ModelErrorPhase::Response,
                provider_request_id.clone(),
                None,
                None,
            )
        })?;
        parse_response(
            &self.core,
            &context,
            &request,
            value.as_value(),
            provider_request_id,
            ModelErrorPhase::Response,
        )
    }
}

impl fmt::Debug for AnthropicMessagesModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicMessagesModel")
            .field("binding", &self.core)
            .finish_non_exhaustive()
    }
}

impl Model for AnthropicMessagesModel {
    fn descriptor(&self) -> &ModelDescriptor {
        &self.core.descriptor
    }

    fn invoke(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxFuture<'_, Result<ModelResponse, ModelError>> {
        Box::pin(self.invoke_once(context, request))
    }

    fn stream(
        &self,
        context: ModelContext,
        request: ModelRequest,
    ) -> BoxStream<'_, Result<stateknot_core::ModelEvent, ModelError>> {
        if let Err(error) = self
            .core
            .preflight(&context, &request, ModelResponseMode::Streaming)
            .and_then(|()| provider_preflight(&self.core, &context, &request))
        {
            return Box::pin(futures_util::stream::once(async move { Err(error) }));
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            let error = self.core.error(
                &context,
                ModelErrorPhase::Preparation,
                stateknot_core::FailureCategory::Internal,
                "stream.runtime_unavailable",
                "The model streaming runtime is unavailable.",
                RetryAdvice::Never,
                None,
                None,
                None,
            );
            return Box::pin(futures_util::stream::once(async move { Err(error) }));
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(32);
        let adapter = self.clone();
        runtime.spawn(async move {
            let terminal_sender = sender.clone();
            if let Err(error) = adapter.stream_once(context, request, sender).await {
                let _ = terminal_sender.send(Err(error)).await;
            }
        });
        receiver_stream(receiver)
    }
}

impl AnthropicMessagesModel {
    #[allow(clippy::too_many_lines)]
    async fn stream_once(
        self,
        context: ModelContext,
        request: ModelRequest,
        sender: tokio::sync::mpsc::Sender<Result<stateknot_core::ModelEvent, ModelError>>,
    ) -> Result<(), ModelError> {
        let body = build_request(&self.core, &context, &request, true)?;
        let body = serialize_request(&self.core, &context, &body)?;
        let key = self.core.resolve_key(&context).await?;
        let api_key = HeaderValue::from_str(key.expose_secret()).map_err(|_| {
            self.core
                .malformed_error(&context, ModelErrorPhase::Preparation, None, None, None)
        })?;
        let response = wait_for(
            &context,
            self.core
                .client
                .post(self.core.url())
                .header("x-api-key", api_key)
                .header("anthropic-version", ANTHROPIC_VERSION)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "text/event-stream")
                .body(body)
                .send(),
        )
        .await
        .map_err(|reason| {
            self.core
                .stop_error(&context, ModelErrorPhase::Dispatch, reason)
        })?
        .map_err(|_| {
            self.core
                .transport_error(&context, ModelErrorPhase::Dispatch)
        })?;
        let status = response.status();
        let provider_request_id = parse_provider_request_id(response.headers());
        if !status.is_success() {
            return Err(self.core.status_error(
                &context,
                ModelErrorPhase::Stream,
                status,
                response.headers(),
                provider_request_id,
            ));
        }
        if !is_event_stream_content_type(response.headers()) {
            return Err(self.core.malformed_error(
                &context,
                ModelErrorPhase::Stream,
                provider_request_id,
                None,
                None,
            ));
        }

        let mut emitter = EventEmitter::new(&self.core, &context, &request, sender)?;
        let mut state = AnthropicStreamState::new(provider_request_id);
        let mut decoder = SseDecoder::new(self.core.options);
        let mut stream = response.bytes_stream();
        loop {
            let next = wait_for(&context, stream.next()).await.map_err(|reason| {
                self.core
                    .stop_error(&context, ModelErrorPhase::Stream, reason)
            })?;
            let Some(chunk) = next else {
                for event in decoder
                    .finish()
                    .map_err(|_| state.malformed(&self.core, &context))?
                {
                    if process_stream_event(
                        &self.core,
                        &context,
                        &request,
                        &mut state,
                        &mut emitter,
                        event,
                    )
                    .await?
                    {
                        return Ok(());
                    }
                }
                return Err(state.malformed(&self.core, &context));
            };
            let chunk =
                chunk.map_err(|_| self.core.transport_error(&context, ModelErrorPhase::Stream))?;
            for event in decoder
                .push(&chunk)
                .map_err(|_| state.malformed(&self.core, &context))?
            {
                if process_stream_event(
                    &self.core,
                    &context,
                    &request,
                    &mut state,
                    &mut emitter,
                    event,
                )
                .await?
                {
                    return Ok(());
                }
            }
        }
    }
}

struct AnthropicStreamState {
    provider_request_id: Option<ModelProviderRequestId>,
    response_id: Option<ModelProviderResponseId>,
    started: bool,
    input_tokens: Option<TokenCount>,
    cached_input_tokens: Option<TokenCount>,
    last_usage: Option<ModelUsage>,
    finish_reason: Option<ModelFinishReason>,
    blocks: BTreeMap<u64, AnthropicBlock>,
    next_output_index: u64,
}

impl AnthropicStreamState {
    fn new(provider_request_id: Option<ModelProviderRequestId>) -> Self {
        Self {
            provider_request_id,
            response_id: None,
            started: false,
            input_tokens: None,
            cached_input_tokens: None,
            last_usage: None,
            finish_reason: None,
            blocks: BTreeMap::new(),
            next_output_index: 0,
        }
    }

    fn malformed(&self, core: &AdapterCore, context: &ModelContext) -> ModelError {
        core.malformed_error(
            context,
            ModelErrorPhase::Stream,
            self.provider_request_id.clone(),
            self.response_id.clone(),
            self.last_usage.clone(),
        )
    }
}

enum AnthropicBlockKind {
    Text,
    Json(SchemaReference),
    Tool {
        identity: CapabilityIdentity,
        schema: SchemaReference,
        provider_call_id: stateknot_core::ModelProviderToolCallId,
    },
}

struct AnthropicBlock {
    kind: AnthropicBlockKind,
    buffer: String,
    output_index: Option<u64>,
    initial_complete_json: bool,
}

#[allow(clippy::match_same_arms, clippy::too_many_lines)]
async fn process_stream_event(
    core: &AdapterCore,
    context: &ModelContext,
    request: &ModelRequest,
    state: &mut AnthropicStreamState,
    emitter: &mut EventEmitter<'_>,
    event: SseEvent,
) -> Result<bool, ModelError> {
    let value = BoundedJson::from_slice(event.data.as_bytes())
        .map_err(|_| state.malformed(core, context))?;
    let root = value
        .as_value()
        .as_object()
        .ok_or_else(|| state.malformed(core, context))?;
    let event_type = root
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| state.malformed(core, context))?;
    if event
        .event
        .as_deref()
        .is_some_and(|name| name != event_type)
    {
        return Err(state.malformed(core, context));
    }
    match event_type {
        "ping" => Ok(false),
        "message_start" => {
            if state.started {
                return Err(state.malformed(core, context));
            }
            let message = root
                .get("message")
                .and_then(Value::as_object)
                .ok_or_else(|| state.malformed(core, context))?;
            if message.get("type").and_then(Value::as_str) != Some("message")
                || message.get("role").and_then(Value::as_str) != Some("assistant")
                || message.get("model").and_then(Value::as_str) != Some(core.model_id.as_str())
            {
                return Err(state.malformed(core, context));
            }
            let response_id = ModelProviderResponseId::new(
                message
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| state.malformed(core, context))?,
            )
            .map_err(|_| state.malformed(core, context))?;
            let usage = parse_usage(
                message
                    .get("usage")
                    .ok_or_else(|| state.malformed(core, context))?,
            )
            .ok_or_else(|| state.malformed(core, context))?;
            state.response_id = Some(response_id.clone());
            state.input_tokens = Some(usage.input_tokens());
            state.cached_input_tokens = usage.cached_input_tokens();
            state.last_usage = Some(usage.clone());
            let mut provenance = ModelResponseProvenance::new(
                context.attempt_id(),
                core.descriptor.metadata().identity().clone(),
                Some(core.model_id.clone()),
                Some(response_id),
            );
            if let Some(request_id) = state.provider_request_id.clone() {
                provenance = provenance.with_provider_request_id(request_id);
            }
            emit(emitter, ModelEventKind::Started { provenance }).await?;
            emit(emitter, ModelEventKind::UsageUpdated { usage }).await?;
            state.started = true;
            Ok(false)
        }
        "content_block_start" => {
            require_started(state, core, context)?;
            let index = root
                .get("index")
                .and_then(Value::as_u64)
                .ok_or_else(|| state.malformed(core, context))?;
            if state.blocks.contains_key(&index) {
                return Err(state.malformed(core, context));
            }
            let block = root
                .get("content_block")
                .and_then(Value::as_object)
                .ok_or_else(|| state.malformed(core, context))?;
            let mut pending = match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    let kind = match request.text_output_format() {
                        Some(ModelTextOutputFormat::JsonSchema { schema }) => {
                            AnthropicBlockKind::Json(schema.clone())
                        }
                        _ => AnthropicBlockKind::Text,
                    };
                    AnthropicBlock {
                        kind,
                        buffer: String::new(),
                        output_index: None,
                        initial_complete_json: false,
                    }
                }
                Some("tool_use") => {
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or_else(|| state.malformed(core, context))?;
                    let tool = request
                        .tools()
                        .find(|tool| tool.metadata().identity().name().as_str() == name)
                        .ok_or_else(|| state.malformed(core, context))?;
                    let provider_call_id = stateknot_core::ModelProviderToolCallId::new(
                        block
                            .get("id")
                            .and_then(Value::as_str)
                            .ok_or_else(|| state.malformed(core, context))?,
                    )
                    .map_err(|_| state.malformed(core, context))?;
                    let initial = block.get("input").cloned().unwrap_or_else(|| json!({}));
                    let initial_complete_json =
                        initial.as_object().is_some_and(|object| !object.is_empty());
                    AnthropicBlock {
                        kind: AnthropicBlockKind::Tool {
                            identity: tool.metadata().identity().clone(),
                            schema: tool.input_schema().clone(),
                            provider_call_id,
                        },
                        buffer: if initial_complete_json {
                            serde_json::to_string(&initial)
                                .map_err(|_| state.malformed(core, context))?
                        } else {
                            String::new()
                        },
                        output_index: None,
                        initial_complete_json,
                    }
                }
                _ => return Err(state.malformed(core, context)),
            };
            let initial_json = if pending.initial_complete_json {
                Some(std::mem::take(&mut pending.buffer))
            } else {
                None
            };
            start_anthropic_block(core, context, state, emitter, &mut pending).await?;
            if let Some(initial) = initial_json.as_deref() {
                append_block(core, context, state, emitter, &mut pending, initial).await?;
            }
            if let Some(text) = block
                .get("text")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                append_block(core, context, state, emitter, &mut pending, text).await?;
            }
            state.blocks.insert(index, pending);
            Ok(false)
        }
        "content_block_delta" => {
            require_started(state, core, context)?;
            let index = root
                .get("index")
                .and_then(Value::as_u64)
                .ok_or_else(|| state.malformed(core, context))?;
            let delta = root
                .get("delta")
                .and_then(Value::as_object)
                .ok_or_else(|| state.malformed(core, context))?;
            let fragment = match delta.get("type").and_then(Value::as_str) {
                Some("text_delta") => delta.get("text").and_then(Value::as_str),
                Some("input_json_delta") => delta.get("partial_json").and_then(Value::as_str),
                _ => return Err(state.malformed(core, context)),
            }
            .ok_or_else(|| state.malformed(core, context))?;
            if fragment.is_empty() {
                return Ok(false);
            }
            let mut block = state
                .blocks
                .remove(&index)
                .ok_or_else(|| state.malformed(core, context))?;
            if block.initial_complete_json {
                return Err(state.malformed(core, context));
            }
            append_block(core, context, state, emitter, &mut block, fragment).await?;
            state.blocks.insert(index, block);
            Ok(false)
        }
        "content_block_stop" => {
            require_started(state, core, context)?;
            let index = root
                .get("index")
                .and_then(Value::as_u64)
                .ok_or_else(|| state.malformed(core, context))?;
            let mut block = state
                .blocks
                .remove(&index)
                .ok_or_else(|| state.malformed(core, context))?;
            if matches!(block.kind, AnthropicBlockKind::Tool { .. }) && block.buffer.is_empty() {
                append_block(core, context, state, emitter, &mut block, "{}").await?;
            }
            if block.buffer.is_empty() {
                return Err(state.malformed(core, context));
            }
            validate_block(core, context, state, &block)?;
            let output_index = block
                .output_index
                .ok_or_else(|| state.malformed(core, context))?;
            emit(
                emitter,
                ModelEventKind::OutputCompleted {
                    output_index: ExecutionCount::new(output_index),
                },
            )
            .await?;
            Ok(false)
        }
        "message_delta" => {
            require_started(state, core, context)?;
            let delta = root
                .get("delta")
                .and_then(Value::as_object)
                .ok_or_else(|| state.malformed(core, context))?;
            if let Some(reason) = delta.get("stop_reason").and_then(Value::as_str) {
                state.finish_reason =
                    Some(map_stop_reason(reason).ok_or_else(|| state.malformed(core, context))?);
            }
            let usage = root
                .get("usage")
                .and_then(Value::as_object)
                .ok_or_else(|| state.malformed(core, context))?;
            let output = TokenCount::new(
                usage
                    .get("output_tokens")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| state.malformed(core, context))?,
            );
            let reasoning = usage
                .get("output_tokens_details")
                .and_then(Value::as_object)
                .and_then(|details| details.get("thinking_tokens"))
                .and_then(Value::as_u64)
                .map(TokenCount::new);
            let normalized = ModelUsage::new(
                state
                    .input_tokens
                    .ok_or_else(|| state.malformed(core, context))?,
                state.cached_input_tokens,
                output,
                reasoning,
            )
            .map_err(|_| state.malformed(core, context))?;
            state.last_usage = Some(normalized.clone());
            emit(emitter, ModelEventKind::UsageUpdated { usage: normalized }).await?;
            Ok(false)
        }
        "message_stop" => {
            require_started(state, core, context)?;
            if !state.blocks.is_empty() {
                return Err(state.malformed(core, context));
            }
            let finish_reason = state
                .finish_reason
                .ok_or_else(|| state.malformed(core, context))?;
            let usage = state
                .last_usage
                .clone()
                .ok_or_else(|| state.malformed(core, context))?;
            emit(
                emitter,
                ModelEventKind::Completed {
                    finish_reason,
                    usage,
                    extensions: Extensions::default(),
                },
            )
            .await?;
            if !emitter.is_complete() {
                return Err(state.malformed(core, context));
            }
            Ok(true)
        }
        "error" => {
            let error_type = root
                .get("error")
                .and_then(Value::as_object)
                .and_then(|error| error.get("type"))
                .and_then(Value::as_str);
            let (category, retry) = match error_type {
                Some("overloaded_error") => (
                    stateknot_core::FailureCategory::DependencyUnavailable,
                    RetryAdvice::SafeAfter {
                        delay: stateknot_core::DurationMillis::new(250).expect("positive constant"),
                    },
                ),
                Some("rate_limit_error") => (
                    stateknot_core::FailureCategory::RateLimited,
                    RetryAdvice::Never,
                ),
                _ => (
                    stateknot_core::FailureCategory::DependencyUnavailable,
                    RetryAdvice::Never,
                ),
            };
            Err(core.error(
                context,
                ModelErrorPhase::Stream,
                category,
                "stream.provider_error",
                "The model provider terminated the response stream with an error.",
                retry,
                state.provider_request_id.clone(),
                state.response_id.clone(),
                state.last_usage.clone(),
            ))
        }
        _ => Ok(false),
    }
}

fn require_started(
    state: &AnthropicStreamState,
    core: &AdapterCore,
    context: &ModelContext,
) -> Result<(), ModelError> {
    if state.started {
        Ok(())
    } else {
        Err(state.malformed(core, context))
    }
}

async fn append_block(
    core: &AdapterCore,
    context: &ModelContext,
    state: &mut AnthropicStreamState,
    emitter: &mut EventEmitter<'_>,
    block: &mut AnthropicBlock,
    fragment: &str,
) -> Result<(), ModelError> {
    start_anthropic_block(core, context, state, emitter, block).await?;
    block.buffer.push_str(fragment);
    for chunk in split_stream_chunks(fragment) {
        let chunk = ModelStreamChunk::new(chunk).map_err(|_| state.malformed(core, context))?;
        let delta = match block.kind {
            AnthropicBlockKind::Text => ModelOutputDelta::Text(chunk),
            AnthropicBlockKind::Json(_) => ModelOutputDelta::Json(chunk),
            AnthropicBlockKind::Tool { .. } => ModelOutputDelta::ToolArguments(chunk),
        };
        emit(
            emitter,
            ModelEventKind::OutputDelta {
                output_index: ExecutionCount::new(
                    block
                        .output_index
                        .expect("output starts before its first delta"),
                ),
                delta,
            },
        )
        .await?;
    }
    Ok(())
}

async fn start_anthropic_block(
    core: &AdapterCore,
    context: &ModelContext,
    state: &mut AnthropicStreamState,
    emitter: &mut EventEmitter<'_>,
    block: &mut AnthropicBlock,
) -> Result<(), ModelError> {
    if block.output_index.is_none() {
        let output_index = state.next_output_index;
        state.next_output_index = state
            .next_output_index
            .checked_add(1)
            .ok_or_else(|| state.malformed(core, context))?;
        let start = match &block.kind {
            AnthropicBlockKind::Text => ModelOutputStart::text(
                None,
                ContentMetadata::untrusted(ContentSource::Model, core.output_label.clone()),
            )
            .map_err(|_| state.malformed(core, context))?,
            AnthropicBlockKind::Json(schema) => ModelOutputStart::json(
                Some(schema.clone()),
                ContentMetadata::untrusted(ContentSource::Model, core.output_label.clone()),
            )
            .map_err(|_| state.malformed(core, context))?,
            AnthropicBlockKind::Tool {
                identity,
                provider_call_id,
                ..
            } => ModelOutputStart::tool_call(
                identity.clone(),
                Some(provider_call_id.clone()),
                Extensions::default(),
            ),
        };
        emit(
            emitter,
            ModelEventKind::OutputStarted {
                output_index: ExecutionCount::new(output_index),
                start: Box::new(start),
            },
        )
        .await?;
        block.output_index = Some(output_index);
    }
    Ok(())
}

fn validate_block(
    core: &AdapterCore,
    context: &ModelContext,
    state: &AnthropicStreamState,
    block: &AnthropicBlock,
) -> Result<(), ModelError> {
    match &block.kind {
        AnthropicBlockKind::Text => Ok(()),
        AnthropicBlockKind::Json(schema) | AnthropicBlockKind::Tool { schema, .. } => {
            let value = BoundedJson::from_slice(block.buffer.as_bytes())
                .map_err(|_| state.malformed(core, context))?;
            core.schemas
                .validate(schema, &value)
                .map_err(|_| state.malformed(core, context))
        }
    }
}

async fn emit(emitter: &mut EventEmitter<'_>, event: ModelEventKind) -> Result<(), ModelError> {
    match emitter.emit(event).await {
        Ok(()) => Ok(()),
        Err(EmitError::Invalid(error)) => Err(error),
    }
}

fn split_stream_chunks(mut value: &str) -> Vec<&str> {
    let mut chunks = Vec::new();
    while !value.is_empty() {
        let mut end = value.len().min(ModelStreamChunk::MAX_BYTES);
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        chunks.push(&value[..end]);
        value = &value[end..];
    }
    chunks
}

fn map_stop_reason(value: &str) -> Option<ModelFinishReason> {
    match value {
        "end_turn" | "stop_sequence" => Some(ModelFinishReason::Completed),
        "max_tokens" => Some(ModelFinishReason::OutputLimit),
        "tool_use" => Some(ModelFinishReason::ToolCalls),
        "pause_turn" => Some(ModelFinishReason::Paused),
        "refusal" => Some(ModelFinishReason::Refused),
        "context_window_exceeded" => Some(ModelFinishReason::ContextLimit),
        _ => None,
    }
}

fn is_event_stream_content_type(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
}

fn provider_preflight(
    core: &AdapterCore,
    context: &ModelContext,
    request: &ModelRequest,
) -> Result<(), ModelError> {
    if request.messages().is_empty() {
        return Err(core.error(
            context,
            ModelErrorPhase::Preparation,
            stateknot_core::FailureCategory::InvalidInput,
            "request.messages_required",
            "Anthropic Messages requires at least one conversation message.",
            RetryAdvice::Never,
            None,
            None,
            None,
        ));
    }
    if matches!(
        request.text_output_format(),
        Some(ModelTextOutputFormat::Json {})
    ) {
        return Err(core.error(
            context,
            ModelErrorPhase::Preparation,
            stateknot_core::FailureCategory::Unsupported,
            "request.generic_json_unsupported",
            "Anthropic requires a pinned schema for structured model output.",
            RetryAdvice::Never,
            None,
            None,
            None,
        ));
    }
    Ok(())
}

#[allow(clippy::similar_names, clippy::too_many_lines)]
fn build_request(
    core: &AdapterCore,
    context: &ModelContext,
    request: &ModelRequest,
    stream: bool,
) -> Result<Value, ModelError> {
    let mut root = Map::new();
    root.insert("model".to_owned(), Value::from(core.model_id.as_str()));
    root.insert(
        "max_tokens".to_owned(),
        Value::from(request.limits().max_output_tokens().get()),
    );
    root.insert("stream".to_owned(), Value::Bool(stream));

    let system = request
        .instructions()
        .iter()
        .map(|instruction| match instruction.content() {
            InstructionContent::Text(text) => Ok(json!({
                "type": "text",
                "text": text.text(),
            })),
            _ => Err(core.malformed_error(context, ModelErrorPhase::Preparation, None, None, None)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !system.is_empty() {
        root.insert("system".to_owned(), Value::Array(system));
    }

    let messages = request
        .messages()
        .iter()
        .map(|message| {
            let role = match message.role() {
                MessageRole::User => "user",
                MessageRole::Assistant => "assistant",
                _ => {
                    return Err(core.malformed_error(
                        context,
                        ModelErrorPhase::Preparation,
                        None,
                        None,
                        None,
                    ));
                }
            };
            let content = message
                .parts()
                .iter()
                .map(|part| match part {
                    ContentPart::Text(text) => Ok(json!({"type": "text", "text": text.text()})),
                    ContentPart::Json(json) => Ok(json!({
                        "type": "text",
                        "text": serde_json::to_string(json.value().as_value())
                            .expect("bounded JSON always serializes"),
                    })),
                    _ => Err(core.malformed_error(
                        context,
                        ModelErrorPhase::Preparation,
                        None,
                        None,
                        None,
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(json!({"role": role, "content": content}))
        })
        .collect::<Result<Vec<_>, ModelError>>()?;
    root.insert("messages".to_owned(), Value::Array(messages));

    let tools = request
        .tools()
        .map(|tool| {
            Ok(json!({
                "name": tool.metadata().identity().name().as_str(),
                "description": tool.metadata().description().as_str(),
                "input_schema": core.schema_value(context, tool.input_schema())?,
                "strict": request.requires_strict_tool_arguments(),
            }))
        })
        .collect::<Result<Vec<_>, ModelError>>()?;
    if !tools.is_empty() {
        root.insert("tools".to_owned(), Value::Array(tools));
        let disable_parallel = !core
            .descriptor
            .capabilities()
            .tools()
            .supports_parallel_calls()
            || request.max_tool_calls_per_response().get() <= 1;
        root.insert(
            "tool_choice".to_owned(),
            match request.tool_selection() {
                ModelToolSelection::None {} => {
                    json!({"type": "none", "disable_parallel_tool_use": true})
                }
                ModelToolSelection::Auto {} => json!({
                    "type": "auto",
                    "disable_parallel_tool_use": disable_parallel,
                }),
                ModelToolSelection::Required {} => json!({
                    "type": "any",
                    "disable_parallel_tool_use": disable_parallel,
                }),
                ModelToolSelection::Specific { name } => json!({
                    "type": "tool",
                    "name": name.as_str(),
                    "disable_parallel_tool_use": disable_parallel,
                }),
            },
        );
    }

    if let Some(ModelTextOutputFormat::JsonSchema { schema }) = request.text_output_format() {
        root.insert(
            "output_config".to_owned(),
            json!({
                "format": {
                    "type": "json_schema",
                    "schema": core.schema_value(context, schema)?,
                }
            }),
        );
    }
    Ok(Value::Object(root))
}

#[allow(clippy::similar_names, clippy::too_many_lines)]
pub(crate) fn parse_response(
    core: &AdapterCore,
    context: &ModelContext,
    request: &ModelRequest,
    value: &Value,
    provider_request_id: Option<ModelProviderRequestId>,
    phase: ModelErrorPhase,
) -> Result<ModelResponse, ModelError> {
    let parse = || -> Option<ModelResponse> {
        let root = value.as_object()?;
        if root.get("type")?.as_str()? != "message" || root.get("role")?.as_str()? != "assistant" {
            return None;
        }
        let response_id = ModelProviderResponseId::new(root.get("id")?.as_str()?).ok()?;
        let response_model = ModelProviderModelId::new(root.get("model")?.as_str()?).ok()?;
        if response_model != core.model_id {
            return None;
        }
        let usage = parse_usage(root.get("usage")?)?;
        let stop_reason = root.get("stop_reason")?.as_str()?;
        let finish = match stop_reason {
            "end_turn" | "stop_sequence" => ModelFinishReason::Completed,
            "max_tokens" => ModelFinishReason::OutputLimit,
            "tool_use" => ModelFinishReason::ToolCalls,
            "pause_turn" => ModelFinishReason::Paused,
            "refusal" => ModelFinishReason::Refused,
            "context_window_exceeded" => ModelFinishReason::ContextLimit,
            _ => return None,
        };
        let structured = finish == ModelFinishReason::Completed
            && matches!(
                request.text_output_format(),
                Some(ModelTextOutputFormat::JsonSchema { .. })
            );
        let mut output = Vec::new();
        for content in root.get("content")?.as_array()? {
            let content = content.as_object()?;
            match content.get("type")?.as_str()? {
                "text" if structured => {
                    let text = content.get("text")?.as_str()?;
                    let ModelTextOutputFormat::JsonSchema { schema } =
                        request.text_output_format()?
                    else {
                        return None;
                    };
                    let value = BoundedJson::from_slice(text.as_bytes()).ok()?;
                    core.schemas.validate(schema, &value).ok()?;
                    output.push(
                        ModelOutputItem::content(ContentPart::Json(JsonContent::new(
                            value,
                            Some(schema.clone()),
                            ContentMetadata::untrusted(
                                ContentSource::Model,
                                core.output_label.clone(),
                            ),
                        )))
                        .ok()?,
                    );
                }
                "text" => {
                    output.push(
                        ModelOutputItem::content(ContentPart::Text(
                            TextContent::new(
                                content.get("text")?.as_str()?,
                                None,
                                ContentMetadata::untrusted(
                                    ContentSource::Model,
                                    core.output_label.clone(),
                                ),
                            )
                            .ok()?,
                        ))
                        .ok()?,
                    );
                }
                "tool_use" => {
                    let name = content.get("name")?.as_str()?;
                    let tool = request
                        .tools()
                        .find(|tool| tool.metadata().identity().name().as_str() == name)?;
                    let arguments = BoundedJson::try_from(content.get("input")?.clone()).ok()?;
                    core.schemas
                        .validate(tool.input_schema(), &arguments)
                        .ok()?;
                    let call_id =
                        stateknot_core::ModelProviderToolCallId::new(content.get("id")?.as_str()?)
                            .ok()?;
                    output.push(ModelOutputItem::tool_call(
                        ModelToolCallProposal::new(
                            tool.metadata().identity().clone(),
                            Some(call_id),
                            arguments,
                            Extensions::default(),
                        )
                        .ok()?,
                    ));
                }
                _ => return None,
            }
        }
        let mut provenance = ModelResponseProvenance::new(
            context.attempt_id(),
            core.descriptor.metadata().identity().clone(),
            Some(response_model),
            Some(response_id),
        );
        if let Some(request_id) = provider_request_id.clone() {
            provenance = provenance.with_provider_request_id(request_id);
        }
        ModelResponse::new(
            provenance,
            &core.descriptor,
            request,
            output,
            finish,
            usage,
            empty_extensions(),
        )
        .ok()
    };
    parse().ok_or_else(|| {
        core.malformed_error(
            context,
            phase,
            provider_request_id,
            value
                .as_object()
                .and_then(|root| root.get("id"))
                .and_then(Value::as_str)
                .and_then(|id| ModelProviderResponseId::new(id).ok()),
            value
                .as_object()
                .and_then(|root| root.get("usage"))
                .and_then(parse_usage),
        )
    })
}

pub(crate) fn parse_usage(value: &Value) -> Option<ModelUsage> {
    let value = value.as_object()?;
    let direct_input = TokenCount::new(value.get("input_tokens")?.as_u64()?);
    let cache_creation = value
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .map_or(TokenCount::ZERO, TokenCount::new);
    let cache_read = value
        .get("cache_read_input_tokens")
        .and_then(Value::as_u64)
        .map_or(TokenCount::ZERO, TokenCount::new);
    let input = direct_input
        .checked_add(cache_creation)?
        .checked_add(cache_read)?;
    let cached = (cache_read.get() > 0).then_some(cache_read);
    let output = TokenCount::new(value.get("output_tokens")?.as_u64()?);
    let reasoning = value
        .get("output_tokens_details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("thinking_tokens"))
        .and_then(Value::as_u64)
        .map(TokenCount::new);
    ModelUsage::new(input, cached, output, reasoning).ok()
}

fn is_json_content_type(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
}
