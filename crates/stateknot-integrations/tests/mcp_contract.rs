// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end contract tests for the strict MCP 2026-07-28 binding.

use std::{sync::Arc, time::Instant};

use serde_json::{Value, json};
use stateknot_core::{
    AttemptId, BoundedJson, BudgetUsage, CancellationSignal, Digest, DurationMillis, ErasedTool,
    FailureCategory, InvocationId, ResolvedBudget, RetryAdvice, SchemaId, SchemaReference,
    TenantId, Timestamp, ToolContext, ToolDescriptor, ToolExternalEffect, ToolInput, Version,
};
use stateknot_integrations::{
    AnonymousMcpAuthorization, ApiKey, McpHttpOptions, McpRemoteTool, McpRemoteToolBuildError,
    McpServerIdentity, ProviderEndpoint, StaticMcpBearerAuthorization,
};
use stateknot_runtime::{JsonSchemaRegistry, JsonSchemaRegistryBuilder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

const SECRET: &str = "mcp-contract-secret";
const RUN_ID: &str = "01912345-6789-7abc-8def-0123456789ac";
const THREAD_ID: &str = "01912345-6789-7abc-8def-0123456789ad";
const INVOCATION_ID: &str = "01912345-6789-7abc-8def-0123456789ae";
const ATTEMPT_ID: &str = "01912345-6789-7abc-8def-0123456789af";

struct TestMcpServer {
    endpoint: ProviderEndpoint,
    requests: mpsc::Receiver<Vec<u8>>,
}

#[derive(Clone, Copy)]
enum CallBehavior {
    StructuredSuccess,
    CloseWithoutResponse,
}

impl TestMcpServer {
    async fn start(input_schema: Value, output_schema: Value) -> Self {
        Self::start_with(
            input_schema,
            output_schema,
            CallBehavior::StructuredSuccess,
            3,
        )
        .await
    }

    async fn start_with(
        input_schema: Value,
        output_schema: Value,
        call_behavior: CallBehavior,
        request_count: usize,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = ProviderEndpoint::loopback_http(&format!("http://{address}/mcp/")).unwrap();
        let (sender, requests) = mpsc::channel(request_count);
        tokio::spawn(async move {
            for _ in 0..request_count {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                let body = request_body(&request);
                let message: Value = serde_json::from_slice(body).unwrap();
                let id = message["id"].clone();
                let response = match message["method"].as_str().unwrap() {
                    "server/discover" => json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "resultType": "complete",
                            "supportedVersions": ["2026-07-28"],
                            "capabilities": {"tools": {}},
                            "ttlMs": 0,
                            "cacheScope": "private",
                            "_meta": {
                                "io.modelcontextprotocol/serverInfo": {
                                    "name": "stateknot-test-mcp",
                                    "version": "1.0.0"
                                }
                            }
                        }
                    }),
                    "tools/list" => json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "resultType": "complete",
                            "tools": [{
                                "name": "echo",
                                "description": "Returns a pinned structured response.",
                                "inputSchema": input_schema,
                                "outputSchema": output_schema,
                                "annotations": {
                                    "readOnlyHint": false,
                                    "destructiveHint": true
                                }
                            }],
                            "ttlMs": 0,
                            "cacheScope": "private"
                        }
                    }),
                    "tools/call" => match call_behavior {
                        CallBehavior::StructuredSuccess => json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": {
                                "resultType": "complete",
                                "content": [{
                                    "type": "text",
                                    "text": "{\"answer\":\"durable\"}"
                                }],
                                "structuredContent": {"answer": "durable"},
                                "isError": false
                            }
                        }),
                        CallBehavior::CloseWithoutResponse => {
                            sender.send(request).await.unwrap();
                            socket.shutdown().await.unwrap();
                            continue;
                        }
                    },
                    method => panic!("unexpected MCP method {method}"),
                };
                let encoded = serde_json::to_vec(&response).unwrap();
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    encoded.len()
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                socket.write_all(&encoded).await.unwrap();
                socket.shutdown().await.unwrap();
                sender.send(request).await.unwrap();
            }
        });
        Self { endpoint, requests }
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 2048];
    loop {
        let read = socket.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0, "request ended before its declared body");
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
            .unwrap();
        if bytes.len() >= header_end + 4 + content_length {
            return bytes;
        }
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn request_body(request: &[u8]) -> &[u8] {
    let header_end = find_bytes(request, b"\r\n\r\n").unwrap();
    &request[header_end + 4..]
}

fn schemas() -> (
    Arc<JsonSchemaRegistry>,
    SchemaReference,
    Value,
    SchemaReference,
    Value,
) {
    let input = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://schemas.stateknot.test/mcp-input/1.0.0",
        "type": "object",
        "properties": {"question": {"type": "string", "maxLength": 256}},
        "required": ["question"],
        "additionalProperties": false
    });
    let output = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://schemas.stateknot.test/mcp-output/1.0.0",
        "type": "object",
        "properties": {"answer": {"type": "string", "maxLength": 256}},
        "required": ["answer"],
        "additionalProperties": false
    });
    let input_reference = schema_reference(&input);
    let output_reference = schema_reference(&output);
    let mut builder = JsonSchemaRegistryBuilder::with_default_limits();
    builder
        .register(input_reference.clone(), input.clone())
        .unwrap();
    builder
        .register(output_reference.clone(), output.clone())
        .unwrap();
    (
        Arc::new(builder.build().unwrap()),
        input_reference,
        input,
        output_reference,
        output,
    )
}

fn schema_reference(document: &Value) -> SchemaReference {
    SchemaReference::new(
        document["$id"]
            .as_str()
            .unwrap()
            .parse::<SchemaId>()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(serde_json_canonicalizer::to_vec(document).unwrap()),
    )
}

fn descriptor(input: &SchemaReference, output: &SchemaReference) -> ToolDescriptor {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-tool-v1.json"
    ))
    .unwrap();
    let mut value = fixture["descriptors"]["valid"][0].clone();
    value["input_schema"] = serde_json::to_value(input).unwrap();
    value["output_schema"] = serde_json::to_value(output).unwrap();
    value["semantics"] = json!({
        "risk": "read_only",
        "idempotency": "not_applicable",
        "status_query": false,
        "compensation": false
    });
    value["resources"] = json!({
        "network": "read_only",
        "filesystem": "none",
        "credentials": true,
        "dynamic_code": false
    });
    value["invocation"] = json!({
        "cancellation": "cooperative",
        "max_progress_events": "0"
    });
    serde_json::from_value(value).unwrap()
}

fn write_descriptor(input: &SchemaReference, output: &SchemaReference) -> ToolDescriptor {
    let mut value = serde_json::to_value(descriptor(input, output)).unwrap();
    value["semantics"] = json!({
        "risk": "idempotent_write",
        "idempotency": "intrinsic",
        "status_query": true,
        "compensation": false
    });
    value["resources"]["network"] = json!("read_write");
    serde_json::from_value(value).unwrap()
}

fn context(descriptor: &ToolDescriptor) -> ToolContext {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-budget-v1.json"
    ))
    .unwrap();
    let resolved =
        serde_json::from_value::<ResolvedBudget>(fixture["resolved"]["valid"][0].clone()).unwrap();
    let observed_at = "2029-12-31T23:59:59.000000Z".parse::<Timestamp>().unwrap();
    ToolContext::new(
        TenantId::new("tenant-mcp-contract").unwrap(),
        RUN_ID.parse().unwrap(),
        THREAD_ID.parse().unwrap(),
        INVOCATION_ID.parse::<InvocationId>().unwrap(),
        ATTEMPT_ID.parse::<AttemptId>().unwrap(),
        descriptor,
        resolved
            .remaining(&BudgetUsage::zero(), observed_at)
            .unwrap(),
        DurationMillis::new(30_000).unwrap(),
        observed_at,
        Instant::now(),
        CancellationSignal::never(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modern_mcp_binding_discovers_once_and_calls_with_pinned_contract() {
    let (registry, input_reference, input_schema, output_reference, output_schema) = schemas();
    let mut server = TestMcpServer::start(input_schema, output_schema).await;
    let descriptor = descriptor(&input_reference, &output_reference);
    let adapter = McpRemoteTool::connect(
        descriptor.clone(),
        "echo",
        server.endpoint.clone(),
        McpServerIdentity::new("stateknot-test-mcp", "1.0.0").unwrap(),
        registry,
        Arc::new(StaticMcpBearerAuthorization::new(
            ApiKey::new(SECRET).unwrap(),
        )),
        McpHttpOptions::default(),
    )
    .await
    .unwrap();
    let result = adapter
        .call(
            context(&descriptor),
            ToolInput::new(
                input_reference,
                BoundedJson::try_from_value(json!({"question": "Is it pinned?"})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result.output().as_value(), &json!({"answer": "durable"}));

    let mut captured = Vec::new();
    for _ in 0..3 {
        captured.push(
            tokio::time::timeout(std::time::Duration::from_secs(1), server.requests.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let methods = captured
        .iter()
        .map(|request| {
            serde_json::from_slice::<Value>(request_body(request)).unwrap()["method"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(methods, ["server/discover", "tools/list", "tools/call"]);
    for request in &captured {
        let headers = String::from_utf8_lossy(request).to_ascii_lowercase();
        assert!(headers.contains(&format!("authorization: bearer {SECRET}")));
        assert!(headers.contains("mcp-protocol-version: 2026-07-28"));
    }
    let call_headers = String::from_utf8_lossy(&captured[2]).to_ascii_lowercase();
    assert!(call_headers.contains("mcp-method: tools/call"));
    assert!(call_headers.contains("mcp-name: echo"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_rejects_remote_schema_drift_before_registration() {
    let (registry, input_reference, mut input_schema, output_reference, output_schema) = schemas();
    input_schema["properties"]["question"]["maxLength"] = json!(255);
    let server = TestMcpServer::start_with(
        input_schema,
        output_schema,
        CallBehavior::StructuredSuccess,
        2,
    )
    .await;
    let error = McpRemoteTool::connect(
        descriptor(&input_reference, &output_reference),
        "echo",
        server.endpoint,
        McpServerIdentity::new("stateknot-test-mcp", "1.0.0").unwrap(),
        registry,
        Arc::new(AnonymousMcpAuthorization),
        McpHttpOptions::default(),
    )
    .await
    .unwrap_err();
    assert_eq!(error, McpRemoteToolBuildError::InputSchemaMismatch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_write_response_is_ambiguous_and_never_hidden_as_a_safe_retry() {
    let (registry, input_reference, input_schema, output_reference, output_schema) = schemas();
    let server = TestMcpServer::start_with(
        input_schema,
        output_schema,
        CallBehavior::CloseWithoutResponse,
        3,
    )
    .await;
    let descriptor = write_descriptor(&input_reference, &output_reference);
    let adapter = McpRemoteTool::connect(
        descriptor.clone(),
        "echo",
        server.endpoint,
        McpServerIdentity::new("stateknot-test-mcp", "1.0.0").unwrap(),
        registry,
        Arc::new(AnonymousMcpAuthorization),
        McpHttpOptions::default(),
    )
    .await
    .unwrap();
    let error = adapter
        .call(
            context(&descriptor),
            ToolInput::new(
                input_reference,
                BoundedJson::try_from_value(json!({"question": "Apply once"})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.external_effect(), ToolExternalEffect::Unknown);
    assert_eq!(
        error.failure().category(),
        FailureCategory::AmbiguousExternalOutcome
    );
    assert_eq!(error.failure().retry_advice(), RetryAdvice::ReconcileFirst);
}
