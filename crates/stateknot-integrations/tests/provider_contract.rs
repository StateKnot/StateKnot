// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Local HTTP/SSE contract tests for first-party provider adapters.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use serde_json::{Value, json};
use stateknot_core::{
    AttemptId, BoundedJson, BoxFuture, BudgetUsage, CancellationObserver, CancellationSignal,
    ContentPart, Digest, FailureCategory, InvocationId, Model, ModelDescriptor,
    ModelEventAccumulator, ModelEventKind, ModelProviderModelId, ModelProviderReplay,
    ModelProviderReplayFormat, ModelProviderToolCallId, ModelRequest, ModelResponseMode,
    ModelToolOutcome, ModelTranscript, ModelTranscriptTurn, ModelTranscriptTurnError,
    ResolvedBudget, SchemaId, SchemaReference, SecurityLabel, TenantId, Timestamp, ToolArtifacts,
    ToolDescriptor, ToolResult, ToolResultProvenance, Version,
};
use stateknot_integrations::{
    AnthropicMessagesModel, ApiKey, ApiKeyProvider, ApiKeyResolutionError, OpenAiResponsesModel,
    ProviderEndpoint, ProviderHttpOptions, StaticApiKey,
};
use stateknot_runtime::{JsonSchemaRegistry, JsonSchemaRegistryBuilder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

const ATTEMPT_ID: &str = "01912345-6789-7abc-8def-0123456789ab";
const RUN_ID: &str = "01912345-6789-7abc-8def-0123456789ac";
const THREAD_ID: &str = "01912345-6789-7abc-8def-0123456789ad";
const MODEL_ID: &str = "provider-model-v1";

struct TestServer {
    endpoint: ProviderEndpoint,
    request: oneshot::Receiver<Vec<u8>>,
    requests: Arc<AtomicUsize>,
}

impl TestServer {
    async fn one(response: Vec<Vec<u8>>, content_type: &str, status: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = ProviderEndpoint::loopback_http(&format!("http://{address}/v1/")).unwrap();
        let (request_sender, request) = oneshot::channel();
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&requests);
        let content_type = content_type.to_owned();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            let captured = read_request(&mut socket).await;
            let body_len = response.iter().map(Vec::len).sum::<usize>();
            let reason = match status {
                200 => "OK",
                429 => "Too Many Requests",
                500 => "Internal Server Error",
                _ => "Test",
            };
            let retry = if status == 429 {
                "Retry-After: 2\r\n"
            } else {
                ""
            };
            let headers = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {body_len}\r\nX-Request-Id: req_test_01\r\n{retry}Connection: close\r\n\r\n"
            );
            socket.write_all(headers.as_bytes()).await.unwrap();
            for chunk in response {
                socket.write_all(&chunk).await.unwrap();
                tokio::task::yield_now().await;
            }
            let _ = socket.shutdown().await;
            let _ = request_sender.send(captured);

            if tokio::time::timeout(Duration::from_millis(250), listener.accept())
                .await
                .is_ok()
            {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        Self {
            endpoint,
            request,
            requests,
        }
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    loop {
        let read = socket.read(&mut buffer).await.unwrap();
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
        let Some(header_end) = find_bytes(&bytes, b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or_default();
        if bytes.len() >= header_end + 4 + content_length {
            break;
        }
    }
    bytes
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn request_header<'a>(request: &'a str, expected_name: &str) -> Option<&'a str> {
    request
        .lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case(expected_name)
                .then_some(value.trim())
        })
}

fn schema_registry() -> Arc<JsonSchemaRegistry> {
    let reference = placeholder_schema();
    let id = "https://schemas.stateknot.test/placeholder/1.0.0";
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": id,
        "type": "object"
    });
    let canonical = serde_json_canonicalizer::to_vec(&document).unwrap();
    assert_eq!(reference.digest(), Digest::sha256(canonical));
    let mut builder = JsonSchemaRegistryBuilder::with_default_limits();
    builder.register(reference, document).unwrap();
    Arc::new(builder.build().unwrap())
}

fn placeholder_schema() -> SchemaReference {
    let id = "https://schemas.stateknot.test/placeholder/1.0.0";
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": id,
        "type": "object"
    });
    SchemaReference::new(
        id.parse::<SchemaId>().unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(serde_json_canonicalizer::to_vec(&document).unwrap()),
    )
}

fn descriptor(streaming: bool) -> ModelDescriptor {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-model-descriptor-v1.json"
    ))
    .unwrap();
    let mut value = fixture["descriptors"]["valid"][0].clone();
    value["capabilities"]["streaming"] = Value::Bool(streaming);
    value["capabilities"]["token_limits"] = json!({
        "max_context_tokens": "128000",
        "max_input_tokens": "120000",
        "max_output_tokens": "16384"
    });
    serde_json::from_value(value).unwrap()
}

fn tool_descriptor() -> ToolDescriptor {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-tool-v1.json"
    ))
    .unwrap();
    let mut value = fixture["descriptors"]["valid"][0].clone();
    let schema = serde_json::to_value(placeholder_schema()).unwrap();
    value["input_schema"] = schema.clone();
    value["output_schema"] = schema;
    serde_json::from_value(value).unwrap()
}

fn descriptor_with_tools(streaming: bool) -> ModelDescriptor {
    let mut value = serde_json::to_value(descriptor(streaming)).unwrap();
    value["capabilities"]["tools"] = json!({
        "schema_profile": placeholder_schema(),
        "max_definitions": "4",
        "max_calls_per_response": "4",
        "choices": ["auto"],
        "strict_arguments": true
    });
    serde_json::from_value(value).unwrap()
}

fn request(mode: ModelResponseMode, include_message: bool) -> ModelRequest {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-model-request-v1.json"
    ))
    .unwrap();
    let mut value = fixture["requests"]["valid"][0].clone();
    if mode == ModelResponseMode::Streaming {
        value["response_mode"] = Value::from("streaming");
        value["requirements"]["streaming"] = Value::Bool(true);
    }
    if include_message {
        value["messages"] = json!([{
            "message_id": "01912345-6789-7abc-8def-0123456789b0",
            "role": "user",
            "parts": [{
                "type": "text",
                "content": {
                    "text": "Hello from StateKnot",
                    "metadata": {
                        "source": "user",
                        "trust": "untrusted",
                        "security_label": "tenant/input",
                        "redaction": "not_applied"
                    }
                }
            }],
            "provenance": {
                "run_id": RUN_ID,
                "event_id": "01912345-6789-7abc-8def-0123456789b1",
                "producer": {
                    "type": "principal",
                    "principal": {
                        "issuer": "https://issuer.example.com",
                        "subject": "user-42"
                    }
                }
            }
        }]);
    }
    serde_json::from_value(value).unwrap()
}

fn request_with_tool(mode: ModelResponseMode, transcript: ModelTranscript) -> ModelRequest {
    let mut value = serde_json::to_value(request(mode, true)).unwrap();
    value["tools"] = json!([tool_descriptor()]);
    value["tool_selection"] = json!({"mode": "auto"});
    value["max_tool_calls_per_response"] = Value::from("4");
    value["strict_tool_arguments"] = Value::Bool(true);
    value["requirements"]["tools"] = json!({
        "min_definitions": "1",
        "min_calls_per_response": "4",
        "choices": ["auto"],
        "strict_arguments": true
    });
    if !transcript.is_empty() {
        value["transcript"] = serde_json::to_value(transcript).unwrap();
    }
    serde_json::from_value(value).unwrap()
}

fn successful_tool_outcome(call_id: &str) -> ModelToolOutcome {
    let tool = tool_descriptor();
    ModelToolOutcome::succeeded(
        ModelProviderToolCallId::new(call_id).unwrap(),
        ToolResult::new(
            ToolResultProvenance::new(
                "01912345-6789-7abc-8def-0123456789b2"
                    .parse::<InvocationId>()
                    .unwrap(),
                "01912345-6789-7abc-8def-0123456789b3"
                    .parse::<AttemptId>()
                    .unwrap(),
                tool.metadata().identity().clone(),
            ),
            tool.output_schema().clone(),
            BoundedJson::try_from(json!({"temperature_celsius": 23})).unwrap(),
            ToolArtifacts::empty(),
        ),
    )
}

fn context_with(
    observed_at: &str,
    cancellation: CancellationSignal,
) -> stateknot_core::ModelContext {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-budget-v1.json"
    ))
    .unwrap();
    let resolved =
        serde_json::from_value::<ResolvedBudget>(fixture["resolved"]["valid"][0].clone()).unwrap();
    let observed_at = observed_at.parse::<Timestamp>().unwrap();
    let remaining = resolved
        .remaining(&BudgetUsage::zero(), observed_at)
        .unwrap();
    stateknot_core::ModelContext::new(
        TenantId::new("tenant-production").unwrap(),
        RUN_ID.parse().unwrap(),
        THREAD_ID.parse().unwrap(),
        ATTEMPT_ID.parse().unwrap(),
        remaining,
        observed_at,
        Instant::now(),
        cancellation,
    )
    .unwrap()
}

fn context() -> stateknot_core::ModelContext {
    context_with("2029-12-31T23:59:59.000000Z", CancellationSignal::never())
}

struct AlwaysCancelled;

impl CancellationObserver for AlwaysCancelled {
    fn is_cancelled(&self) -> bool {
        true
    }

    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

struct PendingCredentials;

impl ApiKeyProvider for PendingCredentials {
    fn resolve(
        &self,
        _context: &stateknot_core::ModelContext,
    ) -> BoxFuture<'_, Result<ApiKey, ApiKeyResolutionError>> {
        Box::pin(std::future::pending())
    }
}

fn credentials() -> Arc<StaticApiKey> {
    Arc::new(StaticApiKey::new(ApiKey::new("super-secret-key").unwrap()))
}

fn output_label() -> SecurityLabel {
    SecurityLabel::new("tenant/model-output").unwrap()
}

fn openai_response() -> Value {
    json!({
        "id": "resp_test_01",
        "model": MODEL_ID,
        "status": "completed",
        "output": [{
            "id": "msg_test_01",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{
                "type": "output_text",
                "text": "hello",
                "annotations": []
            }]
        }],
        "usage": {
            "input_tokens": 10,
            "input_tokens_details": {"cached_tokens": 2},
            "output_tokens": 2,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": 12
        }
    })
}

fn anthropic_response() -> Value {
    json!({
        "id": "msg_test_01",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": "hello"}],
        "model": MODEL_ID,
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": 10,
            "cache_creation_input_tokens": 2,
            "cache_read_input_tokens": 3,
            "output_tokens": 2
        }
    })
}

fn openai_tool_response() -> Value {
    json!({
        "id": "resp_tool_01",
        "model": MODEL_ID,
        "status": "completed",
        "output": [
            {
                "id": "reasoning_01",
                "type": "reasoning",
                "encrypted_content": "opaque-reasoning-token",
                "summary": []
            },
            {
                "id": "call_item_01",
                "type": "function_call",
                "call_id": "call_weather_01",
                "name": "payments.capture",
                "arguments": "{\"city\":\"Shanghai\"}"
            }
        ],
        "usage": {
            "input_tokens": 20,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens": 8,
            "output_tokens_details": {"reasoning_tokens": 2},
            "total_tokens": 28
        }
    })
}

fn anthropic_tool_response() -> Value {
    json!({
        "id": "msg_tool_01",
        "type": "message",
        "role": "assistant",
        "content": [
            {"type": "text", "text": "I will check the tool."},
            {
                "type": "tool_use",
                "id": "toolu_weather_01",
                "name": "payments.capture",
                "input": {"city": "Shanghai"}
            }
        ],
        "model": MODEL_ID,
        "stop_reason": "tool_use",
        "stop_sequence": null,
        "usage": {"input_tokens": 20, "output_tokens": 8}
    })
}

async fn invoke_openai_tool_response() -> stateknot_core::ModelResponse {
    let server = TestServer::one(
        vec![serde_json::to_vec(&openai_tool_response()).unwrap()],
        "application/json",
        200,
    )
    .await;
    let model = OpenAiResponsesModel::new(
        descriptor_with_tools(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let response = model
        .invoke(
            context(),
            request_with_tool(ModelResponseMode::Complete, ModelTranscript::empty()),
        )
        .await
        .unwrap();
    let _ = server.request.await.unwrap();
    response
}

#[tokio::test]
async fn openai_unary_maps_request_response_and_redacts_secrets() {
    let body = serde_json::to_vec(&openai_response()).unwrap();
    let server = TestServer::one(vec![body], "application/json", 200).await;
    let model = OpenAiResponsesModel::new(
        descriptor(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    assert!(!format!("{model:?}").contains(MODEL_ID));
    assert!(!format!("{model:?}").contains("super-secret-key"));

    let response = model
        .invoke(context(), request(ModelResponseMode::Complete, true))
        .await
        .unwrap();
    assert_eq!(response.usage().input_tokens().get(), 10);
    assert_eq!(response.usage().cached_input_tokens().unwrap().get(), 2);
    let ContentPart::Text(text) = response.output()[0].as_content().unwrap() else {
        panic!("expected text output")
    };
    assert_eq!(text.text(), "hello");

    let captured = String::from_utf8(server.request.await.unwrap()).unwrap();
    assert_eq!(
        request_header(&captured, "authorization"),
        Some("Bearer super-secret-key")
    );
    assert_eq!(
        request_header(&captured, "x-client-request-id"),
        Some(ATTEMPT_ID)
    );
    let body = captured.split("\r\n\r\n").nth(1).unwrap();
    let body: Value = serde_json::from_str(body).unwrap();
    assert_eq!(body["store"], false);
    assert_eq!(body["truncation"], "disabled");
    assert_eq!(body["stream"], false);
    assert_eq!(body["input"][0]["role"], "user");
}

#[tokio::test]
async fn anthropic_unary_normalizes_cache_usage_and_headers() {
    let body = serde_json::to_vec(&anthropic_response()).unwrap();
    let server = TestServer::one(vec![body], "application/json", 200).await;
    let model = AnthropicMessagesModel::new(
        descriptor(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let response = model
        .invoke(context(), request(ModelResponseMode::Complete, true))
        .await
        .unwrap();
    assert_eq!(response.usage().input_tokens().get(), 15);
    assert_eq!(response.usage().cached_input_tokens().unwrap().get(), 3);
    let captured = String::from_utf8(server.request.await.unwrap()).unwrap();
    assert_eq!(
        request_header(&captured, "x-api-key"),
        Some("super-secret-key")
    );
    assert_eq!(
        request_header(&captured, "anthropic-version"),
        Some("2023-06-01")
    );
}

#[tokio::test]
async fn openai_tool_transcript_replays_complete_output_before_call_results() {
    let first_server = TestServer::one(
        vec![serde_json::to_vec(&openai_tool_response()).unwrap()],
        "application/json",
        200,
    )
    .await;
    let first_model = OpenAiResponsesModel::new(
        descriptor_with_tools(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        first_server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let response = first_model
        .invoke(
            context(),
            request_with_tool(ModelResponseMode::Complete, ModelTranscript::empty()),
        )
        .await
        .unwrap();
    assert_eq!(
        response.provider_replay().unwrap().format().as_str(),
        "openai.responses.output.v1"
    );
    let _ = first_server.request.await.unwrap();

    let turn =
        ModelTranscriptTurn::new(response, [successful_tool_outcome("call_weather_01")]).unwrap();
    let transcript = ModelTranscript::try_new([turn]).unwrap();

    let second_server = TestServer::one(
        vec![serde_json::to_vec(&openai_response()).unwrap()],
        "application/json",
        200,
    )
    .await;
    let second_model = OpenAiResponsesModel::new(
        descriptor_with_tools(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        second_server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    second_model
        .invoke(
            context(),
            request_with_tool(ModelResponseMode::Complete, transcript),
        )
        .await
        .unwrap();
    let captured = String::from_utf8(second_server.request.await.unwrap()).unwrap();
    let body: Value = serde_json::from_str(captured.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let input = body["input"].as_array().unwrap();
    assert_eq!(input.len(), 4);
    assert_eq!(input[1]["type"], "reasoning");
    assert_eq!(input[1]["encrypted_content"], "opaque-reasoning-token");
    assert_eq!(input[2]["type"], "function_call");
    assert_eq!(input[3]["type"], "function_call_output");
    assert_eq!(input[3]["call_id"], "call_weather_01");
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
}

#[tokio::test]
async fn anthropic_tool_transcript_preserves_assistant_then_grouped_results() {
    let first_server = TestServer::one(
        vec![serde_json::to_vec(&anthropic_tool_response()).unwrap()],
        "application/json",
        200,
    )
    .await;
    let first_model = AnthropicMessagesModel::new(
        descriptor_with_tools(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        first_server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let response = first_model
        .invoke(
            context(),
            request_with_tool(ModelResponseMode::Complete, ModelTranscript::empty()),
        )
        .await
        .unwrap();
    assert_eq!(
        response.provider_replay().unwrap().format().as_str(),
        "anthropic.messages.content.v1"
    );
    let _ = first_server.request.await.unwrap();

    let turn =
        ModelTranscriptTurn::new(response, [successful_tool_outcome("toolu_weather_01")]).unwrap();
    let transcript = ModelTranscript::try_new([turn]).unwrap();
    let second_server = TestServer::one(
        vec![serde_json::to_vec(&anthropic_response()).unwrap()],
        "application/json",
        200,
    )
    .await;
    let second_model = AnthropicMessagesModel::new(
        descriptor_with_tools(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        second_server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    second_model
        .invoke(
            context(),
            request_with_tool(ModelResponseMode::Complete, transcript),
        )
        .await
        .unwrap();
    let captured = String::from_utf8(second_server.request.await.unwrap()).unwrap();
    let body: Value = serde_json::from_str(captured.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[1]["role"], "assistant");
    assert_eq!(messages[1]["content"][1]["type"], "tool_use");
    assert_eq!(messages[2]["role"], "user");
    assert_eq!(messages[2]["content"][0]["type"], "tool_result");
    assert_eq!(messages[2]["content"][0]["tool_use_id"], "toolu_weather_01");
    assert_eq!(messages[2]["content"][0]["is_error"], false);
}

#[tokio::test]
async fn transcript_rejects_a_substituted_provider_call_id() {
    let response = invoke_openai_tool_response().await;
    let error = ModelTranscriptTurn::new(response, [successful_tool_outcome("call_substituted")])
        .unwrap_err();
    assert!(matches!(
        error,
        ModelTranscriptTurnError::ProviderCallIdMismatch { index: 0 }
    ));
}

#[tokio::test]
async fn openai_revalidates_resealed_replay_semantics_before_io() {
    let response = invoke_openai_tool_response().await;
    let turn =
        ModelTranscriptTurn::new(response, [successful_tool_outcome("call_weather_01")]).unwrap();
    let transcript = ModelTranscript::try_new([turn]).unwrap();
    let mut wire = serde_json::to_value(transcript).unwrap();
    wire[0]["response"]["provider_replay"]["payload"][1]["arguments"] =
        json!("{\"city\":\"Beijing\"}");
    let format = serde_json::from_value::<ModelProviderReplayFormat>(
        wire[0]["response"]["provider_replay"]["format"].clone(),
    )
    .unwrap();
    let payload =
        BoundedJson::try_from(wire[0]["response"]["provider_replay"]["payload"].clone()).unwrap();
    let resealed = ModelProviderReplay::new(format, payload).unwrap();
    wire[0]["response"]["provider_replay"]["digest"] =
        serde_json::to_value(resealed.digest()).unwrap();
    let transcript = serde_json::from_value::<ModelTranscript>(wire).unwrap();

    let server = TestServer::one(
        vec![serde_json::to_vec(&openai_response()).unwrap()],
        "application/json",
        200,
    )
    .await;
    let requests = Arc::clone(&server.requests);
    let model = OpenAiResponsesModel::new(
        descriptor_with_tools(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let error = model
        .invoke(
            context(),
            request_with_tool(ModelResponseMode::Complete, transcript),
        )
        .await
        .unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::DataCorruption);
    assert_eq!(
        error.failure().code().as_str(),
        "request.transcript_replay_corrupt"
    );
    assert_eq!(requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn provider_replay_format_mismatch_fails_before_io() {
    let response = invoke_openai_tool_response().await;
    let turn =
        ModelTranscriptTurn::new(response, [successful_tool_outcome("call_weather_01")]).unwrap();
    let transcript = ModelTranscript::try_new([turn]).unwrap();

    let server = TestServer::one(
        vec![serde_json::to_vec(&anthropic_response()).unwrap()],
        "application/json",
        200,
    )
    .await;
    let requests = Arc::clone(&server.requests);
    let model = AnthropicMessagesModel::new(
        descriptor_with_tools(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let error = model
        .invoke(
            context(),
            request_with_tool(ModelResponseMode::Complete, transcript),
        )
        .await
        .unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::Unsupported);
    assert_eq!(error.failure().code().as_str(), "request.transcript_format");
    assert_eq!(requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn client_never_hides_a_provider_retry() {
    let server = TestServer::one(
        vec![br#"{"error":{"type":"api_error"}}"#.to_vec()],
        "application/json",
        500,
    )
    .await;
    let requests = Arc::clone(&server.requests);
    let model = OpenAiResponsesModel::new(
        descriptor(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let error = model
        .invoke(context(), request(ModelResponseMode::Complete, true))
        .await
        .unwrap_err();
    assert_eq!(
        error.failure().category(),
        FailureCategory::DependencyUnavailable
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rate_limit_uses_only_bounded_delta_retry_after() {
    let server = TestServer::one(vec![b"{}".to_vec()], "application/json", 429).await;
    let model = OpenAiResponsesModel::new(
        descriptor(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let error = model
        .invoke(context(), request(ModelResponseMode::Complete, true))
        .await
        .unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::RateLimited);
    assert_eq!(
        error
            .failure()
            .retry_advice()
            .safe_after_delay()
            .unwrap()
            .as_i64(),
        2_000
    );
}

#[tokio::test]
async fn request_and_response_byte_limits_fail_closed_without_unbounded_buffers() {
    let endpoint = ProviderEndpoint::loopback_http("http://127.0.0.1:9/v1/").unwrap();
    let request_options = ProviderHttpOptions::new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        32,
        1024,
        1024,
        2048,
        4096,
    )
    .unwrap();
    let model = OpenAiResponsesModel::new(
        descriptor(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        endpoint,
        request_options,
    )
    .unwrap();
    let error = model
        .invoke(context(), request(ModelResponseMode::Complete, true))
        .await
        .unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::InvalidInput);

    let body = serde_json::to_vec(&openai_response()).unwrap();
    let server = TestServer::one(vec![body], "application/json", 200).await;
    let response_options = ProviderHttpOptions::new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        1024 * 1024,
        64,
        1024,
        2048,
        4096,
    )
    .unwrap();
    let model = OpenAiResponsesModel::new(
        descriptor(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        response_options,
    )
    .unwrap();
    let error = model
        .invoke(context(), request(ModelResponseMode::Complete, true))
        .await
        .unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::DataCorruption);
    let _ = server.request.await.unwrap();
}

#[tokio::test]
async fn cancellation_precedes_dispatch_and_deadline_bounds_credential_resolution() {
    let endpoint = ProviderEndpoint::loopback_http("http://127.0.0.1:9/v1/").unwrap();
    let cancelled = OpenAiResponsesModel::new(
        descriptor(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        endpoint.clone(),
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let error = cancelled
        .invoke(
            context_with(
                "2029-12-31T23:59:59.000000Z",
                CancellationSignal::new(AlwaysCancelled),
            ),
            request(ModelResponseMode::Complete, true),
        )
        .await
        .unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::Cancelled);

    let deadline = OpenAiResponsesModel::new(
        descriptor(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        Arc::new(PendingCredentials),
        endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        deadline.invoke(
            context_with("2029-12-31T23:59:59.980000Z", CancellationSignal::never()),
            request(ModelResponseMode::Complete, true),
        ),
    )
    .await
    .expect("model deadline must wake the pending credential lookup")
    .unwrap_err();
    assert_eq!(
        error.failure().category(),
        FailureCategory::DeadlineExceeded
    );
}

#[allow(clippy::needless_pass_by_value)]
fn sse_event(name: &str, value: Value) -> String {
    format!(
        "event: {name}\ndata: {}\n\n",
        serde_json::to_string(&value).unwrap()
    )
}

#[tokio::test]
async fn openai_stream_is_incremental_and_terminally_cross_checked() {
    let final_response = openai_response();
    let body = [
        sse_event(
            "response.created",
            json!({
                "type": "response.created",
                "response": {"id": "resp_test_01", "model": MODEL_ID, "status": "in_progress"}
            }),
        ),
        sse_event(
            "response.output_item.added",
            json!({
                "type": "response.output_item.added", "output_index": 0,
                "item": {"id": "msg_test_01", "type": "message", "role": "assistant", "content": []}
            }),
        ),
        sse_event(
            "response.content_part.added",
            json!({
                "type": "response.content_part.added", "output_index": 0, "content_index": 0,
                "part": {"type": "output_text", "text": "", "annotations": []}
            }),
        ),
        sse_event(
            "response.output_text.delta",
            json!({
                "type": "response.output_text.delta", "output_index": 0, "content_index": 0,
                "delta": "hel"
            }),
        ),
        sse_event("ping", json!({"type": "ping"})),
        sse_event(
            "response.output_text.delta",
            json!({
                "type": "response.output_text.delta", "output_index": 0, "content_index": 0,
                "delta": "lo"
            }),
        ),
        sse_event(
            "response.output_text.done",
            json!({
                "type": "response.output_text.done", "output_index": 0, "content_index": 0,
                "text": "hello"
            }),
        ),
        sse_event(
            "response.completed",
            json!({
                "type": "response.completed", "response": final_response
            }),
        ),
    ]
    .concat()
    .into_bytes();
    let chunks = body.chunks(17).map(<[u8]>::to_vec).collect();
    let server = TestServer::one(chunks, "text/event-stream; charset=utf-8", 200).await;
    let model = OpenAiResponsesModel::new(
        descriptor(true),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let request = request(ModelResponseMode::Streaming, true);
    let context = context();
    let mut accumulator =
        ModelEventAccumulator::new(context.attempt_id(), model.descriptor(), &request).unwrap();
    let events = model
        .stream(context, request.clone())
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok));
    assert!(events.iter().any(|event| matches!(
        event.as_ref().unwrap().event(),
        ModelEventKind::OutputDelta { .. }
    )));
    for event in events {
        accumulator.push(event.unwrap()).unwrap();
    }
    let response = accumulator.finish().unwrap();
    let ContentPart::Text(text) = response.output()[0].as_content().unwrap() else {
        panic!("expected text output")
    };
    assert_eq!(text.text(), "hello");
}

#[tokio::test]
async fn anthropic_stream_is_incremental_and_usage_stays_inclusive() {
    let body = [
        sse_event("message_start", json!({
            "type": "message_start",
            "message": {
                "id": "msg_test_01", "type": "message", "role": "assistant",
                "model": MODEL_ID, "content": [], "stop_reason": null,
                "usage": {
                    "input_tokens": 10, "cache_creation_input_tokens": 2,
                    "cache_read_input_tokens": 3, "output_tokens": 0
                }
            }
        })),
        sse_event("ping", json!({"type": "ping"})),
        sse_event("content_block_start", json!({
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "text", "text": ""}
        })),
        sse_event("content_block_delta", json!({
            "type": "content_block_delta", "index": 0,
            "delta": {"type": "text_delta", "text": "hello"}
        })),
        sse_event("content_block_stop", json!({"type": "content_block_stop", "index": 0})),
        sse_event("message_delta", json!({
            "type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"output_tokens": 2}
        })),
        sse_event("message_stop", json!({"type": "message_stop"})),
    ]
    .concat()
    .into_bytes();
    let chunks = body.chunks(13).map(<[u8]>::to_vec).collect();
    let server = TestServer::one(chunks, "text/event-stream", 200).await;
    let model = AnthropicMessagesModel::new(
        descriptor(true),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let request = request(ModelResponseMode::Streaming, true);
    let context = context();
    let mut accumulator =
        ModelEventAccumulator::new(context.attempt_id(), model.descriptor(), &request).unwrap();
    let events = model
        .stream(context, request.clone())
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().all(Result::is_ok));
    for event in events {
        accumulator.push(event.unwrap()).unwrap();
    }
    let response = accumulator.finish().unwrap();
    assert_eq!(response.usage().input_tokens().get(), 15);
    assert_eq!(response.usage().cached_input_tokens().unwrap().get(), 3);
}

#[tokio::test]
async fn truncated_stream_fails_closed_after_partial_events() {
    let body = sse_event(
        "response.created",
        json!({
            "type": "response.created",
            "response": {"id": "resp_test_01", "model": MODEL_ID, "status": "in_progress"}
        }),
    )
    .into_bytes();
    let server = TestServer::one(vec![body], "text/event-stream", 200).await;
    let model = OpenAiResponsesModel::new(
        descriptor(true),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let events = model
        .stream(context(), request(ModelResponseMode::Streaming, true))
        .collect::<Vec<_>>()
        .await;
    assert!(events.first().unwrap().is_ok());
    let error = events.last().unwrap().as_ref().unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::DataCorruption);
}

#[tokio::test]
async fn duplicate_provider_json_members_are_rejected_before_mapping() {
    let body = br#"{"id":"resp_test_01","id":"resp_substituted","model":"provider-model-v1","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}"#
        .to_vec();
    let server = TestServer::one(vec![body], "application/json", 200).await;
    let model = OpenAiResponsesModel::new(
        descriptor(false),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let error = model
        .invoke(context(), request(ModelResponseMode::Complete, true))
        .await
        .unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::DataCorruption);
}

#[tokio::test]
async fn openai_terminal_snapshot_must_match_every_streamed_semantic_item() {
    let mut final_response = openai_response();
    final_response["output"][0]["content"][0]["text"] = Value::from("substituted");
    let body = [
        sse_event(
            "response.created",
            json!({
                "type": "response.created",
                "response": {"id": "resp_test_01", "model": MODEL_ID, "status": "in_progress"}
            }),
        ),
        sse_event(
            "response.output_item.added",
            json!({
                "type": "response.output_item.added", "output_index": 0,
                "item": {"id": "msg_test_01", "type": "message", "role": "assistant", "content": []}
            }),
        ),
        sse_event(
            "response.content_part.added",
            json!({
                "type": "response.content_part.added", "output_index": 0, "content_index": 0,
                "part": {"type": "output_text", "text": "", "annotations": []}
            }),
        ),
        sse_event(
            "response.output_text.delta",
            json!({
                "type": "response.output_text.delta", "output_index": 0, "content_index": 0,
                "delta": "hello"
            }),
        ),
        sse_event(
            "response.output_text.done",
            json!({
                "type": "response.output_text.done", "output_index": 0, "content_index": 0,
                "text": "hello"
            }),
        ),
        sse_event(
            "response.completed",
            json!({
                "type": "response.completed", "response": final_response
            }),
        ),
    ]
    .concat()
    .into_bytes();
    let server = TestServer::one(vec![body], "text/event-stream", 200).await;
    let model = OpenAiResponsesModel::new(
        descriptor(true),
        ModelProviderModelId::new(MODEL_ID).unwrap(),
        output_label(),
        schema_registry(),
        credentials(),
        server.endpoint,
        ProviderHttpOptions::default(),
    )
    .unwrap();
    let events = model
        .stream(context(), request(ModelResponseMode::Streaming, true))
        .collect::<Vec<_>>()
        .await;
    assert!(events.iter().any(Result::is_ok));
    assert!(!events.iter().any(|event| matches!(
        event,
        Ok(event) if matches!(event.event(), ModelEventKind::Completed { .. })
    )));
    assert_eq!(
        events
            .last()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .failure()
            .category(),
        FailureCategory::DataCorruption
    );
}
