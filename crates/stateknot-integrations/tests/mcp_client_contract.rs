// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end transport contract for the general stateless MCP Tool client.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use serde_json::{Value, json};
use stateknot_core::BoxFuture;
use stateknot_integrations::{
    ApiKey, McpAuthorization, McpAuthorizationError, McpClient, McpClientAuthorizationChallenge,
    McpClientAuthorizationChallengeStatus, McpClientAuthorizationProvider,
    McpClientAuthorizationRequest, McpClientAuthorizationRetry, McpClientIdentity,
    McpClientOptions, McpToolCall, ProviderEndpoint, StaticMcpBearerAuthorization,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

const SECRET: &str = "general-mcp-client-secret";

struct TestServer {
    endpoint: ProviderEndpoint,
    requests: mpsc::Receiver<Vec<u8>>,
}

impl TestServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = ProviderEndpoint::loopback_http(&format!("http://{address}/mcp/")).unwrap();
        let (sender, requests) = mpsc::channel(3);
        tokio::spawn(async move {
            for _ in 0..3 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                let message: Value = serde_json::from_slice(request_body(&request)).unwrap();
                let id = message["id"].clone();
                match message["method"].as_str().unwrap() {
                    "server/discover" => {
                        write_json(
                            &mut socket,
                            &json!({
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
                                            "name": "stateknot-general-client-test",
                                            "version": "1.0.0"
                                        }
                                    }
                                }
                            }),
                        )
                        .await;
                    }
                    "tools/list" => {
                        write_json(
                            &mut socket,
                            &json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {
                                    "resultType": "complete",
                                    "tools": [{
                                        "name": "deploy",
                                        "description": "Test a nested promoted header.",
                                        "inputSchema": {
                                            "type": "object",
                                            "properties": {
                                                "target": {
                                                    "type": "object",
                                                    "properties": {
                                                        "region": {
                                                            "type": "string",
                                                            "x-mcp-header": "Region"
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }]
                                }
                            }),
                        )
                        .await;
                    }
                    "tools/call" => write_fragmented_sse(&mut socket, id).await,
                    method => panic!("unexpected MCP method {method}"),
                }
                socket.shutdown().await.unwrap();
                sender.send(request).await.unwrap();
            }
        });
        Self { endpoint, requests }
    }
}

async fn write_json(socket: &mut tokio::net::TcpStream, value: &Value) {
    let encoded = serde_json::to_vec(value).unwrap();
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        encoded.len()
    );
    socket.write_all(headers.as_bytes()).await.unwrap();
    socket.write_all(&encoded).await.unwrap();
}

async fn write_fragmented_sse(socket: &mut tokio::net::TcpStream, id: Value) {
    let notification = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "method": "notifications/progress",
        "params": {"progress": 1}
    }))
    .unwrap();
    let result = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "resultType": "complete",
            "content": [{"type": "text", "text": "deployed"}],
            "structuredContent": {"status": "deployed"},
            "isError": false
        }
    }))
    .unwrap();
    socket
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream; charset=utf-8\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let payload = format!("data: {notification}\r\n\r\ndata: {result}\r\n\r\n");
    for chunk in payload.as_bytes().chunks(7) {
        socket.write_all(chunk).await.unwrap();
        tokio::task::yield_now().await;
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn general_client_sends_modern_metadata_and_decodes_fragmented_sse() {
    let mut server = TestServer::start().await;
    let client = McpClient::connect(
        server.endpoint,
        McpClientIdentity::new("stateknot-test", "1.0.0").unwrap(),
        Arc::new(StaticMcpBearerAuthorization::new(
            ApiKey::new(SECRET).unwrap(),
        )),
        McpClientOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        client.server().name(),
        Some("stateknot-general-client-test")
    );

    let catalog = client.list_tools().await.unwrap();
    let tool = catalog.find("deploy").unwrap();
    let response = client
        .call_tool(tool, json!({"target": {"region": "cn-north-1"}}))
        .await
        .unwrap();
    assert_eq!(response.notifications().len(), 1);
    assert_eq!(
        response.notifications()[0].method(),
        "notifications/progress"
    );
    match response.outcome() {
        McpToolCall::Complete(result) => {
            assert!(!result.is_error());
            assert_eq!(
                result.structured_content(),
                Some(&json!({"status": "deployed"}))
            );
        }
        _ => panic!("expected a complete Tool result"),
    }

    let mut captured = Vec::new();
    for _ in 0..3 {
        captured.push(
            tokio::time::timeout(std::time::Duration::from_secs(1), server.requests.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let expected_methods = ["server/discover", "tools/list", "tools/call"];
    for (request, expected_method) in captured.iter().zip(expected_methods) {
        let headers = String::from_utf8_lossy(request).to_ascii_lowercase();
        assert!(headers.contains(&format!("authorization: bearer {SECRET}")));
        assert!(headers.contains("mcp-protocol-version: 2026-07-28"));
        assert!(headers.contains(&format!("mcp-method: {expected_method}")));
        let body: Value = serde_json::from_slice(request_body(request)).unwrap();
        assert_eq!(
            body["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"],
            "2026-07-28"
        );
        assert_eq!(
            body["params"]["_meta"]["io.modelcontextprotocol/clientInfo"]["name"],
            "stateknot-test"
        );
    }
    let call_headers = String::from_utf8_lossy(&captured[2]).to_ascii_lowercase();
    assert!(call_headers.contains("mcp-name: deploy"));
    assert!(call_headers.contains("mcp-param-region: cn-north-1"));
}

struct ChallengeAuthorization {
    handled: AtomicUsize,
}

impl McpClientAuthorizationProvider for ChallengeAuthorization {
    fn resolve(
        &self,
        _request: &McpClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        Box::pin(async move {
            if self.handled.load(Ordering::Acquire) == 0 {
                Ok(McpAuthorization::Anonymous)
            } else {
                Ok(McpAuthorization::Bearer(ApiKey::new(SECRET).unwrap()))
            }
        })
    }

    fn handle_challenge<'a>(
        &'a self,
        request: &'a McpClientAuthorizationRequest,
        challenge: &'a McpClientAuthorizationChallenge,
    ) -> BoxFuture<'a, Result<McpClientAuthorizationRetry, McpAuthorizationError>> {
        Box::pin(async move {
            assert_eq!(request.method(), "server/discover");
            assert_eq!(
                challenge.status(),
                McpClientAuthorizationChallengeStatus::Unauthorized
            );
            assert!(challenge.bearer().unwrap().contains("invalid_token"));
            self.handled.fetch_add(1, Ordering::AcqRel);
            Ok(McpClientAuthorizationRetry::Retry)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bearer_challenge_is_bounded_and_replays_exactly_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = ProviderEndpoint::loopback_http(&format!("http://{address}/mcp/")).unwrap();
    let server = tokio::spawn(async move {
        let mut captured = Vec::new();
        for attempt in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            if attempt == 0 {
                socket
                    .write_all(
                        b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Bearer error=\"invalid_token\", resource_metadata=\"http://127.0.0.1/metadata\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            } else {
                let message: Value = serde_json::from_slice(request_body(&request)).unwrap();
                write_json(
                    &mut socket,
                    &json!({
                        "jsonrpc": "2.0",
                        "id": message["id"],
                        "result": {
                            "resultType": "complete",
                            "supportedVersions": ["2026-07-28"],
                            "capabilities": {},
                            "ttlMs": 0,
                            "cacheScope": "private"
                        }
                    }),
                )
                .await;
            }
            socket.shutdown().await.unwrap();
            captured.push(request);
        }
        captured
    });

    let authorization = Arc::new(ChallengeAuthorization {
        handled: AtomicUsize::new(0),
    });
    McpClient::connect(
        endpoint,
        McpClientIdentity::new("stateknot-challenge-test", "1.0.0").unwrap(),
        authorization.clone(),
        McpClientOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(authorization.handled.load(Ordering::Acquire), 1);

    let captured = server.await.unwrap();
    let first = String::from_utf8_lossy(&captured[0]).to_ascii_lowercase();
    let second = String::from_utf8_lossy(&captured[1]).to_ascii_lowercase();
    assert!(!first.contains("authorization:"));
    assert!(second.contains(&format!("authorization: bearer {SECRET}")));
    let first_body: Value = serde_json::from_slice(request_body(&captured[0])).unwrap();
    let second_body: Value = serde_json::from_slice(request_body(&captured[1])).unwrap();
    assert_eq!(first_body["method"], second_body["method"]);
    assert_ne!(first_body["id"], second_body["id"]);
}

const DIAGNOSTIC_CANARY: &str = "DEBUG_CANARY_";

fn diagnostic_private(field: &str) -> String {
    format!("{DIAGNOSTIC_CANARY}{field}")
}

fn diagnostic_tool() -> Value {
    json!({
        "name": diagnostic_private("tool_name"),
        "title": diagnostic_private("tool_title"),
        "description": diagnostic_private("tool_description"),
        "inputSchema": {"type":"object"}
    })
}

fn diagnostic_complete(is_error: bool) -> Value {
    let mut result = json!({
        "resultType":"complete",
        "content":[{"type":"text","text":diagnostic_private("result_text")}],
        "isError":is_error,
        (diagnostic_private("result_raw_key")):diagnostic_private("result_raw_value")
    });
    if !is_error {
        result["structuredContent"] = json!({
            (diagnostic_private("structured_key")):diagnostic_private("structured_value")
        });
    }
    result
}

fn diagnostic_input_required() -> Value {
    json!({
        "resultType":"input_required",
        "requestState":diagnostic_private("opaque_state"),
        "inputRequests":{
            (diagnostic_private("input_key")):{
                "method":"roots/list",
                "params":{(diagnostic_private("input_param_key")):diagnostic_private("input_param_value")}
            }
        }
    })
}

fn diagnostic_notification(has_params: bool) -> Value {
    let mut notification = json!({
        "jsonrpc":"2.0",
        "method":format!("notifications/{}", diagnostic_private("notification_method")),
        (diagnostic_private("notification_raw_key")):diagnostic_private("notification_raw_value")
    });
    if has_params {
        notification["params"] = json!({
            (diagnostic_private("notification_param_key")):diagnostic_private("notification_param_value")
        });
    }
    notification
}

#[derive(Clone, Copy)]
enum DiagnosticOutcome {
    Complete(bool),
    InputRequired,
    Remote,
}

fn diagnostic_rpc_response(
    method: &str,
    id: &Value,
    outcome: DiagnosticOutcome,
    index: usize,
) -> Value {
    match method {
        "server/discover" => json!({
            "jsonrpc":"2.0","id":id,
            "result":{"resultType":"complete","supportedVersions":["2026-07-28"],
                      "capabilities":{"tools":{}},"ttlMs":0,"cacheScope":"private"}
        }),
        "tools/list" => json!({
            "jsonrpc":"2.0","id":id,
            "result":{"resultType":"complete","tools":[diagnostic_tool()]}
        }),
        "tools/call" => match outcome {
            DiagnosticOutcome::Remote => json!({
                "jsonrpc":"2.0","id":id,
                "error":{"code":-32041,"message":diagnostic_private("error_message"),
                         "data":{(diagnostic_private("error_data_key")):diagnostic_private("error_data_value")}}
            }),
            DiagnosticOutcome::InputRequired if index == 2 => {
                json!({"jsonrpc":"2.0","id":id,"result":diagnostic_input_required()})
            }
            DiagnosticOutcome::InputRequired => {
                json!({"jsonrpc":"2.0","id":id,"result":diagnostic_complete(false)})
            }
            DiagnosticOutcome::Complete(is_error) => {
                json!({"jsonrpc":"2.0","id":id,"result":diagnostic_complete(is_error)})
            }
        },
        method => panic!("unexpected diagnostic fixture method {method}"),
    }
}

async fn write_diagnostic_sse(socket: &mut tokio::net::TcpStream, response: &Value) {
    socket
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let payload = format!(
        "data: {}\r\n\r\ndata: {}\r\n\r\ndata: {response}\r\n\r\n",
        diagnostic_notification(true),
        diagnostic_notification(false)
    );
    for chunk in payload.as_bytes().chunks(7) {
        socket.write_all(chunk).await.unwrap();
        tokio::task::yield_now().await;
    }
}

struct DiagnosticServer {
    endpoint: ProviderEndpoint,
    task: tokio::task::JoinHandle<Vec<Vec<u8>>>,
}

impl Drop for DiagnosticServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl DiagnosticServer {
    async fn start(sse: bool, outcome: DiagnosticOutcome) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = ProviderEndpoint::loopback_http(&format!("http://{address}/mcp/")).unwrap();
        let count = if matches!(outcome, DiagnosticOutcome::InputRequired) {
            4
        } else {
            3
        };
        let task = tokio::spawn(async move {
            let mut captured = Vec::new();
            for index in 0..count {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                let message: Value = serde_json::from_slice(request_body(&request)).unwrap();
                let method = message["method"].as_str().unwrap();
                let response = diagnostic_rpc_response(method, &message["id"], outcome, index);
                if sse && method == "tools/call" {
                    write_diagnostic_sse(&mut socket, &response).await;
                } else {
                    write_json(&mut socket, &response).await;
                }
                socket.shutdown().await.unwrap();
                captured.push(request);
            }
            captured
        });
        Self { endpoint, task }
    }

    async fn finish(mut self) -> Vec<Vec<u8>> {
        // Keep the handle in its owner across the timeout; Drop aborts on error.
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut self.task)
            .await
            .unwrap()
            .unwrap()
    }
}

fn assert_diagnostic_redacted<T: std::fmt::Debug + ?Sized>(value: &T) {
    for rendered in [format!("{value:?}"), format!("{value:#?}")] {
        assert!(!rendered.contains(DIAGNOSTIC_CANARY), "{rendered}");
        assert!(!rendered.contains(SECRET), "{rendered}");
    }
}

fn assert_diagnostic_notifications(
    response: &stateknot_integrations::McpToolCallResponse,
    sse: bool,
) {
    let notifications = response.notifications();
    assert_eq!(notifications.len(), if sse { 2 } else { 0 });
    for (notification, has_params) in notifications.iter().zip([true, false]) {
        assert_diagnostic_redacted(notification);
        let expected = diagnostic_notification(has_params);
        assert_eq!(notification.method(), expected["method"].as_str().unwrap());
        assert_eq!(notification.params(), expected.get("params"));
        assert_eq!(notification.raw(), &expected);
        assert_eq!(
            serde_json::to_vec(notification.raw()).unwrap(),
            serde_json::to_vec(&expected).unwrap()
        );
        assert_eq!(&notification.clone(), notification);
    }
}

fn assert_diagnostic_complete(
    response: &stateknot_integrations::McpToolCallResponse,
    is_error: bool,
    sse: bool,
) {
    assert_diagnostic_redacted(response);
    assert_diagnostic_redacted(response.outcome());
    assert_diagnostic_notifications(response, sse);
    let McpToolCall::Complete(result) = response.outcome() else {
        panic!("expected a diagnostic complete result");
    };
    assert_diagnostic_redacted(result);
    let expected = diagnostic_complete(is_error);
    assert_eq!(result.is_error(), is_error);
    assert_eq!(
        result.content(),
        expected["content"].as_array().unwrap().as_slice()
    );
    assert_eq!(
        result.structured_content(),
        expected.get("structuredContent")
    );
    assert_eq!(result.raw(), &expected);
    assert_eq!(
        serde_json::to_vec(result.raw()).unwrap(),
        serde_json::to_vec(&expected).unwrap()
    );
    assert_eq!(&result.clone(), result);
}

fn assert_diagnostic_remote(error: &stateknot_integrations::StatelessMcpClientError) {
    assert_diagnostic_redacted(error);
    let stateknot_integrations::StatelessMcpClientError::Remote(remote) = error else {
        panic!("expected a diagnostic remote error");
    };
    assert_diagnostic_redacted(remote);
    assert_eq!(remote.code(), -32041);
    assert_eq!(remote.message(), diagnostic_private("error_message"));
    let expected =
        json!({(diagnostic_private("error_data_key")):diagnostic_private("error_data_value")});
    assert_eq!(remote.data(), Some(&expected));
    assert_eq!(
        serde_json::to_vec(remote.data().unwrap()).unwrap(),
        serde_json::to_vec(&expected).unwrap()
    );
    assert_eq!(&remote.clone(), remote);
}

fn assert_diagnostic_wire(captured: &[Vec<u8>], outcome: DiagnosticOutcome, arguments: &Value) {
    let expected_count = if matches!(outcome, DiagnosticOutcome::InputRequired) {
        4
    } else {
        3
    };
    assert_eq!(captured.len(), expected_count);
    let mut ids = std::collections::HashSet::new();
    for (index, request) in captured.iter().enumerate() {
        let message: Value = serde_json::from_slice(request_body(request)).unwrap();
        assert!(ids.insert(message["id"].as_u64().unwrap()));
        assert_eq!(
            message["method"],
            if index == 0 {
                "server/discover"
            } else if index == 1 {
                "tools/list"
            } else {
                "tools/call"
            }
        );
        if index >= 2 {
            assert_eq!(message["params"]["name"], diagnostic_private("tool_name"));
            assert_eq!(&message["params"]["arguments"], arguments);
            if index == 3 {
                assert_eq!(
                    message["params"]["requestState"],
                    diagnostic_private("opaque_state")
                );
                assert_eq!(
                    message["params"]["inputResponses"],
                    json!({(diagnostic_private("input_key")):{"roots":[]}})
                );
            } else {
                assert!(message["params"].get("requestState").is_none());
                assert!(message["params"].get("inputResponses").is_none());
            }
        }
    }
}

async fn diagnostic_http_case(sse: bool, outcome: DiagnosticOutcome) {
    let server = DiagnosticServer::start(sse, outcome).await;
    let client = McpClient::connect(
        server.endpoint.clone(),
        McpClientIdentity::new("diagnostic-contract-client", "1.0.0").unwrap(),
        Arc::new(StaticMcpBearerAuthorization::new(
            ApiKey::new(SECRET).unwrap(),
        )),
        McpClientOptions::default(),
    )
    .await
    .unwrap();
    let catalog = client.list_tools().await.unwrap();
    let tool = catalog.find(&diagnostic_private("tool_name")).unwrap();
    assert_eq!(tool.raw(), &diagnostic_tool());
    assert_eq!(
        tool.title(),
        Some(diagnostic_private("tool_title").as_str())
    );
    assert_eq!(
        tool.description(),
        Some(diagnostic_private("tool_description").as_str())
    );
    let arguments = json!({"question":diagnostic_private("original_argument")});
    let result = client.call_tool(tool, arguments.clone()).await;
    match outcome {
        DiagnosticOutcome::Remote => assert_diagnostic_remote(&result.unwrap_err()),
        DiagnosticOutcome::Complete(is_error) => {
            assert_diagnostic_complete(&result.unwrap(), is_error, sse);
        }
        DiagnosticOutcome::InputRequired => {
            let response = result.unwrap();
            assert_diagnostic_redacted(&response);
            assert_diagnostic_notifications(&response, sse);
            // The bare continuation's Debug is outside this scope. Its Tool
            // name is still private metadata that the outer response must hide.
            let McpToolCall::InputRequired(required) = response.into_outcome() else {
                panic!("expected a diagnostic continuation");
            };
            assert!(required.has_request_state());
            let expected = diagnostic_input_required();
            assert_eq!(
                required.input_requests(),
                expected["inputRequests"].as_object().unwrap()
            );
            let mut responses = serde_json::Map::new();
            responses.insert(diagnostic_private("input_key"), json!({"roots":[]}));
            let complete = required.resume(responses).await.unwrap();
            assert_diagnostic_complete(&complete, false, sse);
        }
    }
    let captured = server.finish().await;
    assert_diagnostic_wire(&captured, outcome, &arguments);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_response_debug_hides_payloads_without_changing_raw_values_or_resume() {
    for sse in [false, true] {
        for outcome in [
            DiagnosticOutcome::Complete(false),
            DiagnosticOutcome::Complete(true),
            DiagnosticOutcome::InputRequired,
            DiagnosticOutcome::Remote,
        ] {
            diagnostic_http_case(sse, outcome).await;
        }
    }
}
