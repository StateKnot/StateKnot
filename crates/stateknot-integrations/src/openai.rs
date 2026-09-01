// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, fmt, sync::Arc};

use futures_util::StreamExt;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
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

/// `OpenAI` Responses API model binding with provider retries and redirects disabled.
#[derive(Clone)]
pub struct OpenAiResponsesModel {
    core: AdapterCore,
}

impl OpenAiResponsesModel {
    /// Constructs one immutable OpenAI-compatible Responses binding.
    ///
    /// The descriptor must declare only text input/output. Every advertised
    /// schema profile must already be installed in `schemas`. `endpoint`
    /// normally ends in `/v1/`; the adapter appends `responses`.
    ///
    /// # Errors
    ///
    /// Returns [`ModelAdapterBuildError`] when the binding cannot preserve its
    /// descriptor or secure transport invariants.
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
                ProviderKind::OpenAi,
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
        let body = build_request(&self.core, &context, &request, false)?;
        let body = serialize_request(&self.core, &context, &body)?;
        let key = self.core.resolve_key(&context).await?;
        let authorization = HeaderValue::from_str(&format!("Bearer {}", key.expose_secret()))
            .map_err(|_| {
                self.core
                    .malformed_error(&context, ModelErrorPhase::Preparation, None, None, None)
            })?;
        let response = wait_for(
            &context,
            self.core
                .client
                .post(self.core.url())
                .header(AUTHORIZATION, authorization)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json")
                .header("x-client-request-id", context.attempt_id().to_string())
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

impl fmt::Debug for OpenAiResponsesModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiResponsesModel")
            .field("binding", &self.core)
            .finish_non_exhaustive()
    }
}

impl Model for OpenAiResponsesModel {
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

impl OpenAiResponsesModel {
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
        let authorization = HeaderValue::from_str(&format!("Bearer {}", key.expose_secret()))
            .map_err(|_| {
                self.core
                    .malformed_error(&context, ModelErrorPhase::Preparation, None, None, None)
            })?;
        let response = wait_for(
            &context,
            self.core
                .client
                .post(self.core.url())
                .header(AUTHORIZATION, authorization)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "text/event-stream")
                .header("x-client-request-id", context.attempt_id().to_string())
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
        let mut state = OpenAiStreamState::new(provider_request_id);
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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum OpenAiBlockKey {
    Content(u64, u64),
    Summary(u64, u64),
    Tool(u64),
}

enum OpenAiBlockKind {
    Text,
    Json(SchemaReference),
    JsonGeneric,
    Reasoning,
    Tool {
        identity: CapabilityIdentity,
        schema: SchemaReference,
        provider_call_id: Option<stateknot_core::ModelProviderToolCallId>,
    },
}

struct OpenAiBlock {
    kind: OpenAiBlockKind,
    buffer: String,
    output_index: u64,
}

struct OpenAiStreamState {
    provider_request_id: Option<ModelProviderRequestId>,
    response_id: Option<ModelProviderResponseId>,
    started: bool,
    blocks: BTreeMap<OpenAiBlockKey, OpenAiBlock>,
    completed: BTreeMap<u64, ModelOutputItem>,
    next_output_index: u64,
    last_usage: Option<ModelUsage>,
}

impl OpenAiStreamState {
    fn new(provider_request_id: Option<ModelProviderRequestId>) -> Self {
        Self {
            provider_request_id,
            response_id: None,
            started: false,
            blocks: BTreeMap::new(),
            completed: BTreeMap::new(),
            next_output_index: 0,
            last_usage: None,
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

#[allow(clippy::match_same_arms, clippy::too_many_lines)]
async fn process_stream_event(
    core: &AdapterCore,
    context: &ModelContext,
    request: &ModelRequest,
    state: &mut OpenAiStreamState,
    emitter: &mut EventEmitter<'_>,
    event: SseEvent,
) -> Result<bool, ModelError> {
    if event.data == "[DONE]" {
        return Err(state.malformed(core, context));
    }
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
        "response.created" => {
            if state.started {
                return Err(state.malformed(core, context));
            }
            let response = root
                .get("response")
                .and_then(Value::as_object)
                .ok_or_else(|| state.malformed(core, context))?;
            if response.get("model").and_then(Value::as_str) != Some(core.model_id.as_str()) {
                return Err(state.malformed(core, context));
            }
            let response_id = ModelProviderResponseId::new(
                response
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| state.malformed(core, context))?,
            )
            .map_err(|_| state.malformed(core, context))?;
            state.response_id = Some(response_id.clone());
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
            state.started = true;
            Ok(false)
        }
        "response.output_item.added" => {
            require_started(state, core, context)?;
            let provider_index = required_index(root, "output_index", state, core, context)?;
            let item = root
                .get("item")
                .and_then(Value::as_object)
                .ok_or_else(|| state.malformed(core, context))?;
            match item.get("type").and_then(Value::as_str) {
                Some("message" | "reasoning") => Ok(false),
                Some("function_call") => {
                    let key = OpenAiBlockKey::Tool(provider_index);
                    if state.blocks.contains_key(&key) {
                        return Err(state.malformed(core, context));
                    }
                    let name = item
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or_else(|| state.malformed(core, context))?;
                    let tool = request
                        .tools()
                        .find(|tool| tool.metadata().identity().name().as_str() == name)
                        .ok_or_else(|| state.malformed(core, context))?;
                    let provider_call_id = item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .map(stateknot_core::ModelProviderToolCallId::new)
                        .transpose()
                        .map_err(|_| state.malformed(core, context))?;
                    let kind = OpenAiBlockKind::Tool {
                        identity: tool.metadata().identity().clone(),
                        schema: tool.input_schema().clone(),
                        provider_call_id,
                    };
                    let initial = item
                        .get("arguments")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    begin_block(core, context, state, emitter, key.clone(), kind).await?;
                    if !initial.is_empty() {
                        append_block(core, context, state, emitter, &key, initial).await?;
                    }
                    Ok(false)
                }
                _ => Err(state.malformed(core, context)),
            }
        }
        "response.content_part.added" => {
            require_started(state, core, context)?;
            let output_index = required_index(root, "output_index", state, core, context)?;
            let content_index = required_index(root, "content_index", state, core, context)?;
            let key = OpenAiBlockKey::Content(output_index, content_index);
            if state.blocks.contains_key(&key) {
                return Err(state.malformed(core, context));
            }
            let part = root
                .get("part")
                .and_then(Value::as_object)
                .ok_or_else(|| state.malformed(core, context))?;
            let (kind, initial) = match part.get("type").and_then(Value::as_str) {
                Some("output_text") => {
                    if part
                        .get("annotations")
                        .and_then(Value::as_array)
                        .is_some_and(|values| !values.is_empty())
                    {
                        return Err(state.malformed(core, context));
                    }
                    let kind = match request.text_output_format() {
                        Some(ModelTextOutputFormat::JsonSchema { schema }) => {
                            OpenAiBlockKind::Json(schema.clone())
                        }
                        Some(ModelTextOutputFormat::Json {}) => OpenAiBlockKind::JsonGeneric,
                        _ => OpenAiBlockKind::Text,
                    };
                    (
                        kind,
                        part.get("text").and_then(Value::as_str).unwrap_or_default(),
                    )
                }
                Some("refusal") => (
                    OpenAiBlockKind::Text,
                    part.get("refusal")
                        .and_then(Value::as_str)
                        .unwrap_or_default(),
                ),
                _ => return Err(state.malformed(core, context)),
            };
            begin_block(core, context, state, emitter, key.clone(), kind).await?;
            if !initial.is_empty() {
                append_block(core, context, state, emitter, &key, initial).await?;
            }
            Ok(false)
        }
        "response.reasoning_summary_part.added" => {
            require_started(state, core, context)?;
            let output_index = required_index(root, "output_index", state, core, context)?;
            let summary_index = root
                .get("summary_index")
                .or_else(|| root.get("content_index"))
                .and_then(Value::as_u64)
                .ok_or_else(|| state.malformed(core, context))?;
            let key = OpenAiBlockKey::Summary(output_index, summary_index);
            let part = root
                .get("part")
                .and_then(Value::as_object)
                .ok_or_else(|| state.malformed(core, context))?;
            if part.get("type").and_then(Value::as_str) != Some("summary_text") {
                return Err(state.malformed(core, context));
            }
            begin_block(
                core,
                context,
                state,
                emitter,
                key.clone(),
                OpenAiBlockKind::Reasoning,
            )
            .await?;
            if let Some(initial) = part
                .get("text")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                append_block(core, context, state, emitter, &key, initial).await?;
            }
            Ok(false)
        }
        "response.output_text.delta" | "response.refusal.delta" => {
            let key = content_key(root, state, core, context)?;
            let fragment = root
                .get("delta")
                .and_then(Value::as_str)
                .ok_or_else(|| state.malformed(core, context))?;
            if !fragment.is_empty() {
                append_block(core, context, state, emitter, &key, fragment).await?;
            }
            Ok(false)
        }
        "response.reasoning_summary_text.delta" => {
            let key = summary_key(root, state, core, context)?;
            let fragment = root
                .get("delta")
                .and_then(Value::as_str)
                .ok_or_else(|| state.malformed(core, context))?;
            if !fragment.is_empty() {
                append_block(core, context, state, emitter, &key, fragment).await?;
            }
            Ok(false)
        }
        "response.function_call_arguments.delta" => {
            let key =
                OpenAiBlockKey::Tool(required_index(root, "output_index", state, core, context)?);
            let fragment = root
                .get("delta")
                .and_then(Value::as_str)
                .ok_or_else(|| state.malformed(core, context))?;
            if !fragment.is_empty() {
                append_block(core, context, state, emitter, &key, fragment).await?;
            }
            Ok(false)
        }
        "response.output_text.done" | "response.refusal.done" => {
            let key = content_key(root, state, core, context)?;
            let complete = root
                .get(if event_type == "response.refusal.done" {
                    "refusal"
                } else {
                    "text"
                })
                .and_then(Value::as_str);
            complete_block(core, context, state, emitter, key, complete).await?;
            Ok(false)
        }
        "response.reasoning_summary_text.done" => {
            let key = summary_key(root, state, core, context)?;
            let complete = root.get("text").and_then(Value::as_str);
            complete_block(core, context, state, emitter, key, complete).await?;
            Ok(false)
        }
        "response.function_call_arguments.done" => {
            let key =
                OpenAiBlockKey::Tool(required_index(root, "output_index", state, core, context)?);
            let complete = root.get("arguments").and_then(Value::as_str);
            complete_block(core, context, state, emitter, key, complete).await?;
            Ok(false)
        }
        "response.completed" | "response.incomplete" => {
            require_started(state, core, context)?;
            if !state.blocks.is_empty() {
                return Err(state.malformed(core, context));
            }
            let response_value = root
                .get("response")
                .ok_or_else(|| state.malformed(core, context))?;
            let expected = parse_response(
                core,
                context,
                request,
                response_value,
                state.provider_request_id.clone(),
                ModelErrorPhase::Stream,
            )?;
            if expected.provenance().provider_response_id() != state.response_id.as_ref()
                || expected.output().len() != state.completed.len()
                || expected
                    .output()
                    .iter()
                    .enumerate()
                    .any(|(index, item)| state.completed.get(&(index as u64)) != Some(item))
            {
                return Err(state.malformed(core, context));
            }
            state.last_usage = Some(expected.usage().clone());
            emit(
                emitter,
                ModelEventKind::Completed {
                    finish_reason: expected.finish_reason(),
                    usage: expected.usage().clone(),
                    extensions: Extensions::default(),
                },
            )
            .await?;
            if !emitter.is_complete() {
                return Err(state.malformed(core, context));
            }
            Ok(true)
        }
        "response.failed" | "response.cancelled" | "error" => Err(core.error(
            context,
            ModelErrorPhase::Stream,
            stateknot_core::FailureCategory::DependencyUnavailable,
            "stream.provider_error",
            "The model provider terminated the response stream with an error.",
            RetryAdvice::Never,
            state.provider_request_id.clone(),
            state.response_id.clone(),
            state.last_usage.clone(),
        )),
        "response.output_item.done"
        | "response.content_part.done"
        | "response.reasoning_summary_part.done"
        | "response.in_progress"
        | "response.queued" => Ok(false),
        _ => Ok(false),
    }
}

fn require_started(
    state: &OpenAiStreamState,
    core: &AdapterCore,
    context: &ModelContext,
) -> Result<(), ModelError> {
    if state.started {
        Ok(())
    } else {
        Err(state.malformed(core, context))
    }
}

fn required_index(
    root: &Map<String, Value>,
    name: &str,
    state: &OpenAiStreamState,
    core: &AdapterCore,
    context: &ModelContext,
) -> Result<u64, ModelError> {
    root.get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| state.malformed(core, context))
}

fn content_key(
    root: &Map<String, Value>,
    state: &OpenAiStreamState,
    core: &AdapterCore,
    context: &ModelContext,
) -> Result<OpenAiBlockKey, ModelError> {
    Ok(OpenAiBlockKey::Content(
        required_index(root, "output_index", state, core, context)?,
        required_index(root, "content_index", state, core, context)?,
    ))
}

fn summary_key(
    root: &Map<String, Value>,
    state: &OpenAiStreamState,
    core: &AdapterCore,
    context: &ModelContext,
) -> Result<OpenAiBlockKey, ModelError> {
    let output_index = required_index(root, "output_index", state, core, context)?;
    let summary_index = root
        .get("summary_index")
        .or_else(|| root.get("content_index"))
        .and_then(Value::as_u64)
        .ok_or_else(|| state.malformed(core, context))?;
    Ok(OpenAiBlockKey::Summary(output_index, summary_index))
}

async fn begin_block(
    core: &AdapterCore,
    context: &ModelContext,
    state: &mut OpenAiStreamState,
    emitter: &mut EventEmitter<'_>,
    key: OpenAiBlockKey,
    kind: OpenAiBlockKind,
) -> Result<(), ModelError> {
    if state.blocks.contains_key(&key) {
        return Err(state.malformed(core, context));
    }
    let output_index = state.next_output_index;
    state.next_output_index = state
        .next_output_index
        .checked_add(1)
        .ok_or_else(|| state.malformed(core, context))?;
    let start = match &kind {
        OpenAiBlockKind::Text => ModelOutputStart::text(
            None,
            ContentMetadata::untrusted(ContentSource::Model, core.output_label.clone()),
        )
        .map_err(|_| state.malformed(core, context))?,
        OpenAiBlockKind::Json(schema) => ModelOutputStart::json(
            Some(schema.clone()),
            ContentMetadata::untrusted(ContentSource::Model, core.output_label.clone()),
        )
        .map_err(|_| state.malformed(core, context))?,
        OpenAiBlockKind::JsonGeneric => ModelOutputStart::json(
            None,
            ContentMetadata::untrusted(ContentSource::Model, core.output_label.clone()),
        )
        .map_err(|_| state.malformed(core, context))?,
        OpenAiBlockKind::Reasoning => ModelOutputStart::reasoning_summary(
            None,
            ContentMetadata::untrusted(ContentSource::Model, core.output_label.clone()),
        )
        .map_err(|_| state.malformed(core, context))?,
        OpenAiBlockKind::Tool {
            identity,
            provider_call_id,
            ..
        } => ModelOutputStart::tool_call(
            identity.clone(),
            provider_call_id.clone(),
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
    state.blocks.insert(
        key,
        OpenAiBlock {
            kind,
            buffer: String::new(),
            output_index,
        },
    );
    Ok(())
}

async fn append_block(
    core: &AdapterCore,
    context: &ModelContext,
    state: &mut OpenAiStreamState,
    emitter: &mut EventEmitter<'_>,
    key: &OpenAiBlockKey,
    fragment: &str,
) -> Result<(), ModelError> {
    if !state.blocks.contains_key(key) {
        return Err(state.malformed(core, context));
    }
    let block = state
        .blocks
        .get_mut(key)
        .expect("block presence was checked immediately above");
    block.buffer.push_str(fragment);
    let output_index = block.output_index;
    let delta_kind = match block.kind {
        OpenAiBlockKind::Text => 0,
        OpenAiBlockKind::Json(_) | OpenAiBlockKind::JsonGeneric => 1,
        OpenAiBlockKind::Reasoning => 2,
        OpenAiBlockKind::Tool { .. } => 3,
    };
    for fragment in split_stream_chunks(fragment) {
        let chunk = ModelStreamChunk::new(fragment).map_err(|_| state.malformed(core, context))?;
        let delta = match delta_kind {
            0 => ModelOutputDelta::Text(chunk),
            1 => ModelOutputDelta::Json(chunk),
            2 => ModelOutputDelta::ReasoningSummary(chunk),
            3 => ModelOutputDelta::ToolArguments(chunk),
            _ => unreachable!("closed delta kind"),
        };
        emit(
            emitter,
            ModelEventKind::OutputDelta {
                output_index: ExecutionCount::new(output_index),
                delta,
            },
        )
        .await?;
    }
    Ok(())
}

async fn complete_block(
    core: &AdapterCore,
    context: &ModelContext,
    state: &mut OpenAiStreamState,
    emitter: &mut EventEmitter<'_>,
    key: OpenAiBlockKey,
    provider_complete: Option<&str>,
) -> Result<(), ModelError> {
    let mut block = state
        .blocks
        .remove(&key)
        .ok_or_else(|| state.malformed(core, context))?;
    if let Some(complete) = provider_complete {
        if block.buffer.is_empty() {
            append_detached_block(core, context, state, emitter, &mut block, complete).await?;
        } else if block.buffer != complete {
            return Err(state.malformed(core, context));
        }
    }
    if block.buffer.is_empty() {
        return Err(state.malformed(core, context));
    }
    let metadata = ContentMetadata::untrusted(ContentSource::Model, core.output_label.clone());
    let item = match &block.kind {
        OpenAiBlockKind::Text => ModelOutputItem::content(ContentPart::Text(
            TextContent::new(&block.buffer, None, metadata)
                .map_err(|_| state.malformed(core, context))?,
        ))
        .map_err(|_| state.malformed(core, context))?,
        OpenAiBlockKind::Json(schema) => {
            let value = BoundedJson::from_slice(block.buffer.as_bytes())
                .map_err(|_| state.malformed(core, context))?;
            core.schemas
                .validate(schema, &value)
                .map_err(|_| state.malformed(core, context))?;
            ModelOutputItem::content(ContentPart::Json(JsonContent::new(
                value,
                Some(schema.clone()),
                metadata,
            )))
            .map_err(|_| state.malformed(core, context))?
        }
        OpenAiBlockKind::JsonGeneric => {
            let value = BoundedJson::from_slice(block.buffer.as_bytes())
                .map_err(|_| state.malformed(core, context))?;
            ModelOutputItem::content(ContentPart::Json(JsonContent::new(value, None, metadata)))
                .map_err(|_| state.malformed(core, context))?
        }
        OpenAiBlockKind::Reasoning => ModelOutputItem::reasoning_summary(
            TextContent::new(&block.buffer, None, metadata)
                .map_err(|_| state.malformed(core, context))?,
        )
        .map_err(|_| state.malformed(core, context))?,
        OpenAiBlockKind::Tool {
            identity,
            schema,
            provider_call_id,
        } => {
            let arguments = BoundedJson::from_slice(block.buffer.as_bytes())
                .map_err(|_| state.malformed(core, context))?;
            core.schemas
                .validate(schema, &arguments)
                .map_err(|_| state.malformed(core, context))?;
            ModelOutputItem::tool_call(
                ModelToolCallProposal::new(
                    identity.clone(),
                    provider_call_id.clone(),
                    arguments,
                    Extensions::default(),
                )
                .map_err(|_| state.malformed(core, context))?,
            )
        }
    };
    emit(
        emitter,
        ModelEventKind::OutputCompleted {
            output_index: ExecutionCount::new(block.output_index),
        },
    )
    .await?;
    if state.completed.insert(block.output_index, item).is_some() {
        return Err(state.malformed(core, context));
    }
    Ok(())
}

async fn append_detached_block(
    core: &AdapterCore,
    context: &ModelContext,
    state: &OpenAiStreamState,
    emitter: &mut EventEmitter<'_>,
    block: &mut OpenAiBlock,
    fragment: &str,
) -> Result<(), ModelError> {
    block.buffer.push_str(fragment);
    for fragment in split_stream_chunks(fragment) {
        let chunk = ModelStreamChunk::new(fragment).map_err(|_| state.malformed(core, context))?;
        let delta = match block.kind {
            OpenAiBlockKind::Text => ModelOutputDelta::Text(chunk),
            OpenAiBlockKind::Json(_) | OpenAiBlockKind::JsonGeneric => {
                ModelOutputDelta::Json(chunk)
            }
            OpenAiBlockKind::Reasoning => ModelOutputDelta::ReasoningSummary(chunk),
            OpenAiBlockKind::Tool { .. } => ModelOutputDelta::ToolArguments(chunk),
        };
        emit(
            emitter,
            ModelEventKind::OutputDelta {
                output_index: ExecutionCount::new(block.output_index),
                delta,
            },
        )
        .await?;
    }
    Ok(())
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

fn is_event_stream_content_type(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"))
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
        "max_output_tokens".to_owned(),
        Value::from(request.limits().max_output_tokens().get()),
    );
    root.insert("stream".to_owned(), Value::Bool(stream));
    root.insert("store".to_owned(), Value::Bool(false));
    root.insert("truncation".to_owned(), Value::from("disabled"));

    let instructions = request
        .instructions()
        .iter()
        .map(|instruction| match instruction.content() {
            InstructionContent::Text(text) => Ok(text.text()),
            _ => Err(core.malformed_error(context, ModelErrorPhase::Preparation, None, None, None)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !instructions.is_empty() {
        root.insert(
            "instructions".to_owned(),
            Value::from(instructions.join("\n\n")),
        );
    }

    let input = request
        .messages()
        .iter()
        .map(|message| {
            let (role, content_type) = match message.role() {
                MessageRole::User => ("user", "input_text"),
                MessageRole::Assistant => ("assistant", "output_text"),
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
                    ContentPart::Text(text) => Ok(json!({
                        "type": content_type,
                        "text": text.text(),
                    })),
                    ContentPart::Json(json) => Ok(json!({
                        "type": content_type,
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
    if !input.is_empty() {
        root.insert("input".to_owned(), Value::Array(input));
    }

    let tools = request
        .tools()
        .map(|tool| {
            let schema = core.schema_value(context, tool.input_schema())?;
            Ok(json!({
                "type": "function",
                "name": tool.metadata().identity().name().as_str(),
                "description": tool.metadata().description().as_str(),
                "parameters": schema,
                "strict": request.requires_strict_tool_arguments(),
            }))
        })
        .collect::<Result<Vec<_>, ModelError>>()?;
    if !tools.is_empty() {
        root.insert("tools".to_owned(), Value::Array(tools));
        root.insert(
            "parallel_tool_calls".to_owned(),
            Value::Bool(
                core.descriptor
                    .capabilities()
                    .tools()
                    .supports_parallel_calls()
                    && request.max_tool_calls_per_response().get() > 1,
            ),
        );
        root.insert(
            "tool_choice".to_owned(),
            match request.tool_selection() {
                ModelToolSelection::None {} => Value::from("none"),
                ModelToolSelection::Auto {} => Value::from("auto"),
                ModelToolSelection::Required {} => Value::from("required"),
                ModelToolSelection::Specific { name } => {
                    json!({"type": "function", "name": name.as_str()})
                }
            },
        );
    }

    if let Some(format) = request.text_output_format() {
        let format = match format {
            ModelTextOutputFormat::Text {} => json!({"type": "text"}),
            ModelTextOutputFormat::Json {} => json!({"type": "json_object"}),
            ModelTextOutputFormat::JsonSchema { schema } => json!({
                "type": "json_schema",
                "name": "stateknot_output",
                "strict": true,
                "schema": core.schema_value(context, schema)?,
            }),
        };
        root.insert("text".to_owned(), json!({"format": format}));
    }
    if request.requires_reasoning_summaries() {
        root.insert("reasoning".to_owned(), json!({"summary": "auto"}));
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
    if value
        .as_object()
        .and_then(|root| root.get("status"))
        .and_then(Value::as_str)
        .is_some_and(|status| matches!(status, "failed" | "cancelled"))
    {
        return Err(core.error(
            context,
            phase,
            stateknot_core::FailureCategory::DependencyUnavailable,
            "response.provider_failure",
            "The model provider reported an unsuccessful response.",
            RetryAdvice::Never,
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
        ));
    }
    let parse = || -> Option<(ModelResponse, Option<ModelUsage>)> {
        let root = value.as_object()?;
        let response_id = ModelProviderResponseId::new(root.get("id")?.as_str()?).ok()?;
        let response_model = ModelProviderModelId::new(root.get("model")?.as_str()?).ok()?;
        if response_model != core.model_id {
            return None;
        }
        let usage = parse_usage(root.get("usage")?)?;
        let status = root.get("status")?.as_str()?;
        let mut output = Vec::new();
        let mut tool_calls = 0usize;
        let mut refusal = false;
        for item in root.get("output")?.as_array()? {
            let item = item.as_object()?;
            match item.get("type")?.as_str()? {
                "message" => {
                    if item.get("role")?.as_str()? != "assistant" {
                        return None;
                    }
                    for content in item.get("content")?.as_array()? {
                        let content = content.as_object()?;
                        match content.get("type")?.as_str()? {
                            "output_text" => {
                                if content
                                    .get("annotations")
                                    .and_then(Value::as_array)
                                    .is_some_and(|values| !values.is_empty())
                                    || content
                                        .get("logprobs")
                                        .and_then(Value::as_array)
                                        .is_some_and(|values| !values.is_empty())
                                {
                                    return None;
                                }
                                output.push(parse_text_output(
                                    core,
                                    request,
                                    content.get("text")?.as_str()?,
                                )?);
                            }
                            "refusal" => {
                                refusal = true;
                                output.push(
                                    ModelOutputItem::content(ContentPart::Text(
                                        TextContent::new(
                                            content.get("refusal")?.as_str()?,
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
                            _ => return None,
                        }
                    }
                }
                "function_call" => {
                    tool_calls = tool_calls.checked_add(1)?;
                    let name = item.get("name")?.as_str()?;
                    let tool = request
                        .tools()
                        .find(|tool| tool.metadata().identity().name().as_str() == name)?;
                    let arguments =
                        BoundedJson::from_slice(item.get("arguments")?.as_str()?.as_bytes())
                            .ok()?;
                    core.schemas
                        .validate(tool.input_schema(), &arguments)
                        .ok()?;
                    let provider_call_id = item
                        .get("call_id")
                        .and_then(Value::as_str)
                        .map(stateknot_core::ModelProviderToolCallId::new)
                        .transpose()
                        .ok()?;
                    output.push(ModelOutputItem::tool_call(
                        ModelToolCallProposal::new(
                            tool.metadata().identity().clone(),
                            provider_call_id,
                            arguments,
                            Extensions::default(),
                        )
                        .ok()?,
                    ));
                }
                "reasoning" => {
                    for summary in item.get("summary")?.as_array()? {
                        let summary = summary.as_object()?;
                        if summary.get("type")?.as_str()? != "summary_text" {
                            return None;
                        }
                        output.push(
                            ModelOutputItem::reasoning_summary(
                                TextContent::new(
                                    summary.get("text")?.as_str()?,
                                    None,
                                    ContentMetadata::untrusted(
                                        ContentSource::Model,
                                        core.output_label.clone(),
                                    ),
                                )
                                .ok()?,
                            )
                            .ok()?,
                        );
                    }
                }
                _ => return None,
            }
        }
        let finish = match status {
            "completed" if tool_calls > 0 => ModelFinishReason::ToolCalls,
            "completed" if refusal => ModelFinishReason::Refused,
            "completed" => ModelFinishReason::Completed,
            "incomplete" => match root
                .get("incomplete_details")?
                .as_object()?
                .get("reason")?
                .as_str()?
            {
                "max_output_tokens" => ModelFinishReason::OutputLimit,
                "context_window_exceeded" => ModelFinishReason::ContextLimit,
                "content_filter" => ModelFinishReason::ContentFiltered,
                _ => return None,
            },
            _ => return None,
        };
        let mut provenance = ModelResponseProvenance::new(
            context.attempt_id(),
            core.descriptor.metadata().identity().clone(),
            Some(response_model),
            Some(response_id),
        );
        if let Some(request_id) = provider_request_id.clone() {
            provenance = provenance.with_provider_request_id(request_id);
        }
        let response = ModelResponse::new(
            provenance,
            &core.descriptor,
            request,
            output,
            finish,
            usage.clone(),
            empty_extensions(),
        )
        .ok()?;
        Some((response, Some(usage)))
    };
    match parse() {
        Some((response, _)) => Ok(response),
        None => Err(core.malformed_error(
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
        )),
    }
}

fn parse_text_output(
    core: &AdapterCore,
    request: &ModelRequest,
    text: &str,
) -> Option<ModelOutputItem> {
    let metadata = ContentMetadata::untrusted(ContentSource::Model, core.output_label.clone());
    match request.text_output_format()? {
        ModelTextOutputFormat::Text {} => ModelOutputItem::content(ContentPart::Text(
            TextContent::new(text, None, metadata).ok()?,
        ))
        .ok(),
        ModelTextOutputFormat::Json {} => {
            let value = BoundedJson::from_slice(text.as_bytes()).ok()?;
            ModelOutputItem::content(ContentPart::Json(JsonContent::new(value, None, metadata)))
                .ok()
        }
        ModelTextOutputFormat::JsonSchema { schema } => {
            let value = BoundedJson::from_slice(text.as_bytes()).ok()?;
            core.schemas.validate(schema, &value).ok()?;
            ModelOutputItem::content(ContentPart::Json(JsonContent::new(
                value,
                Some(schema.clone()),
                metadata,
            )))
            .ok()
        }
    }
}

fn parse_usage(value: &Value) -> Option<ModelUsage> {
    let value = value.as_object()?;
    let input = TokenCount::new(value.get("input_tokens")?.as_u64()?);
    let cached = value
        .get("input_tokens_details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
        .map(TokenCount::new);
    let output = TokenCount::new(value.get("output_tokens")?.as_u64()?);
    let reasoning = value
        .get("output_tokens_details")
        .and_then(Value::as_object)
        .and_then(|details| details.get("reasoning_tokens"))
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
