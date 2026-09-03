// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end network contracts for the strict A2A 1.0 client and durable tool binding.

use std::{
    collections::HashMap,
    fmt::Write as _,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use serde_json::{Value, json};
use stateknot_core::{
    ArtifactId, ArtifactIdentity, ArtifactModality, ArtifactName, ArtifactParents,
    ArtifactPresentation, ArtifactProvenance, ArtifactRef, ArtifactRepresentation, AttemptId,
    BoundedJson, BoxFuture, BudgetUsage, ByteCount, CancellationSignal, ContentMetadata,
    ContentSource, Digest, DurationMillis, ErasedTool, FailureCategory, InvocationId,
    ResolvedBudget, RetentionClass, RetryAdvice, SchemaId, SchemaReference, SecurityLabel,
    TenantId, Timestamp, ToolContext, ToolDescriptor, ToolExternalEffect, ToolInput,
    ToolReconciliationContext, ToolReconciliationObservation, ToolRecoveryHandle, Version,
};
use stateknot_integrations::{
    A2aAgentCapabilities, A2aAgentCard, A2aAgentCardEndpoint, A2aAgentCardTrust, A2aAgentInterface,
    A2aAgentSkill, A2aArtifactIngestionError, A2aArtifactIngestionRequest, A2aArtifactIngestor,
    A2aBearerTokenProvider, A2aBinding, A2aCancelTaskRequest, A2aClient,
    A2aClientAuthorizationError, A2aClientAuthorizationRequest, A2aClientBuildError,
    A2aClientInterfacePin, A2aClientOperation, A2aClientOptions, A2aClientSecurity,
    A2aDeletePushConfigRequest, A2aGetPushConfigRequest, A2aGetTaskRequest,
    A2aListPushConfigsRequest, A2aListTasksRequest, A2aMessage, A2aMessageRole, A2aPart,
    A2aPartContent, A2aPushConfig, A2aRemoteAgent, A2aRemoteAgentDelivery, A2aRemoteAgentRecovery,
    A2aSecurityScheme, A2aSendMessageRequest, A2aSendMessageResponse, A2aStreamEvent,
    A2aSubscribeTaskRequest, A2aTaskState, ApiKey, ProviderHttpOptions, StaticA2aBearerToken,
    a2a_agent_card_digest,
};
use stateknot_runtime::{JsonSchemaRegistry, JsonSchemaRegistryBuilder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
    task::JoinHandle,
};

const TOKEN: &str = "a2a-contract-secret";
const RUN_ID: &str = "01912345-6789-7abc-8def-0123456789b1";
const THREAD_ID: &str = "01912345-6789-7abc-8def-0123456789b2";
const INVOCATION_ID: &str = "01912345-6789-7abc-8def-0123456789b3";
const ATTEMPT_ID: &str = "01912345-6789-7abc-8def-0123456789b4";
const ORIGIN_EVENT_ID: &str = "01912345-6789-7abc-8def-0123456789b5";
const ROUTING_TENANT: &str = "remote-tenant";

#[derive(Clone, Copy)]
enum OperationBehavior {
    Success,
    CloseWithoutResponse,
    DefiniteRejection,
    TwoEventStream,
    PrematureTaskStream,
    CrossResourceTask,
    CrossResourceStream,
    TaskBoundSuccess,
    JsonRpcNoContent,
    OperationMatrix,
    ContextHistoryRecovery,
    ContextHistoryPayloadMismatch,
    DeduplicatedReplayRecovery,
    DurableTaskRecovery,
}

struct TestA2aServer {
    card_endpoint: A2aAgentCardEndpoint,
    interface_pin: A2aClientInterfacePin,
    card: Value,
    requests: mpsc::Receiver<Vec<u8>>,
    task: JoinHandle<()>,
}

impl TestA2aServer {
    #[allow(clippy::too_many_lines)]
    async fn start(
        binding: A2aBinding,
        tenant: Option<&str>,
        secured: bool,
        streaming: bool,
        behavior: OperationBehavior,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let interface_url = match binding {
            A2aBinding::HttpJson => format!("http://{address}/a2a"),
            A2aBinding::JsonRpc => format!("http://{address}/rpc"),
            _ => unreachable!("test covers the StateKnot A2A 1.0 bindings"),
        };
        let operation_matrix = matches!(
            behavior,
            OperationBehavior::OperationMatrix | OperationBehavior::JsonRpcNoContent
        );
        let capabilities = A2aAgentCapabilities::new()
            .streaming(streaming || operation_matrix)
            .push_notifications(operation_matrix)
            .extended_agent_card(operation_matrix);
        let card = agent_card(&interface_url, binding, tenant, secured, capabilities);
        let encoded_card = serde_json::to_vec(&card).unwrap();
        let extended_card = card.clone();
        let success = successful_send_response();
        let (sender, requests) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let mut reconciliation_message = None;
            let mut replay_send_count = 0_u8;
            let mut task_get_count = 0_u8;
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let request = read_request(&mut socket).await;
                if sender.send(request.clone()).await.is_err() {
                    return;
                }
                let target = request_target(&request);
                if target == "/.well-known/agent-card.json" {
                    write_response(&mut socket, "200 OK", "application/json", &encoded_card).await;
                    continue;
                }
                match behavior {
                    OperationBehavior::Success => {
                        let body = request_body(&request);
                        let value: Value = serde_json::from_slice(body).unwrap();
                        let response = if binding == A2aBinding::JsonRpc {
                            json!({
                                "jsonrpc": "2.0",
                                "id": value["id"].clone(),
                                "result": success.clone()
                            })
                        } else {
                            success.clone()
                        };
                        write_json(&mut socket, binding, "200 OK", &response).await;
                    }
                    OperationBehavior::CloseWithoutResponse => {
                        socket.shutdown().await.unwrap();
                    }
                    OperationBehavior::DefiniteRejection => {
                        let response = json!({
                            "error": {
                                "code": 400,
                                "message": "invalid request",
                                "status": "INVALID_ARGUMENT",
                                "details": [{
                                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                                    "reason": "INVALID_PARAMS",
                                    "domain": "a2a-protocol.org",
                                    "metadata": {}
                                }]
                            }
                        });
                        write_json(&mut socket, binding, "400 Bad Request", &response).await;
                    }
                    OperationBehavior::TwoEventStream => {
                        let first = json!({"task": matrix_task()});
                        let second = matrix_status_update();
                        let body = format!(
                            "event: message\ndata: {}\n\nevent: message\ndata: {}\n\n",
                            serde_json::to_string(&first).unwrap(),
                            serde_json::to_string(&second).unwrap()
                        );
                        write_response(&mut socket, "200 OK", "text/event-stream", body.as_bytes())
                            .await;
                    }
                    OperationBehavior::PrematureTaskStream => {
                        let event = json!({"task": matrix_task()});
                        let body = format!(
                            "event: message\ndata: {}\n\n",
                            serde_json::to_string(&event).unwrap()
                        );
                        write_response(&mut socket, "200 OK", "text/event-stream", body.as_bytes())
                            .await;
                    }
                    OperationBehavior::CrossResourceTask => {
                        let mut task = matrix_task();
                        task["id"] = json!("task-from-another-request");
                        let response = if binding == A2aBinding::JsonRpc {
                            let request: Value =
                                serde_json::from_slice(request_body(&request)).unwrap();
                            json!({
                                "jsonrpc": "2.0",
                                "id": request["id"].clone(),
                                "result": task
                            })
                        } else {
                            task
                        };
                        write_json(&mut socket, binding, "200 OK", &response).await;
                    }
                    OperationBehavior::CrossResourceStream => {
                        let task = json!({"task": matrix_task()});
                        let mut update = matrix_status_update();
                        update["statusUpdate"]["taskId"] = json!("task-from-another-stream");
                        let request_id = (binding == A2aBinding::JsonRpc).then(|| {
                            serde_json::from_slice::<Value>(request_body(&request)).unwrap()["id"]
                                .clone()
                        });
                        let wrap = |value: Value| {
                            request_id.as_ref().map_or(
                                value.clone(),
                                |id| json!({"jsonrpc": "2.0", "id": id, "result": value}),
                            )
                        };
                        let body = format!(
                            "event: message\ndata: {}\n\nevent: message\ndata: {}\n\n",
                            serde_json::to_string(&wrap(task)).unwrap(),
                            serde_json::to_string(&wrap(update)).unwrap()
                        );
                        write_response(&mut socket, "200 OK", "text/event-stream", body.as_bytes())
                            .await;
                    }
                    OperationBehavior::TaskBoundSuccess => {
                        let value: Value = serde_json::from_slice(request_body(&request)).unwrap();
                        let payload = if binding == A2aBinding::JsonRpc {
                            &value["params"]
                        } else {
                            &value
                        };
                        let task_id = payload["message"]["taskId"].as_str().unwrap();
                        let context_id = payload["message"]["contextId"].as_str().unwrap();
                        let result = json!({
                            "task": {
                                "id": task_id,
                                "contextId": context_id,
                                "status": {"state": "TASK_STATE_COMPLETED"}
                            }
                        });
                        let response = if binding == A2aBinding::JsonRpc {
                            json!({
                                "jsonrpc": "2.0",
                                "id": value["id"].clone(),
                                "result": result
                            })
                        } else {
                            result
                        };
                        write_json(&mut socket, binding, "200 OK", &response).await;
                    }
                    OperationBehavior::JsonRpcNoContent => {
                        assert_eq!(binding, A2aBinding::JsonRpc);
                        write_response(&mut socket, "204 No Content", "application/json", &[])
                            .await;
                    }
                    OperationBehavior::OperationMatrix => {
                        write_operation_matrix_response(
                            &mut socket,
                            binding,
                            &request,
                            &extended_card,
                        )
                        .await;
                    }
                    OperationBehavior::ContextHistoryRecovery
                    | OperationBehavior::ContextHistoryPayloadMismatch => {
                        if target.contains("message:send") {
                            let value: Value =
                                serde_json::from_slice(request_body(&request)).unwrap();
                            reconciliation_message = Some(value["message"].clone());
                            socket.shutdown().await.unwrap();
                            continue;
                        }
                        let mut message = reconciliation_message
                            .clone()
                            .expect("the lost send precedes reconciliation");
                        if matches!(behavior, OperationBehavior::ContextHistoryPayloadMismatch) {
                            message["parts"][0]["data"]["question"] =
                                Value::String("substituted payload".to_string());
                        }
                        let context_id = message["contextId"].clone();
                        let response = json!({
                            "tasks": [{
                                "id": "recovered-task-1",
                                "contextId": context_id,
                                "status": {"state": "TASK_STATE_WORKING"},
                                "history": [message]
                            }],
                            "nextPageToken": "",
                            "pageSize": 100,
                            "totalSize": 1
                        });
                        write_json(&mut socket, binding, "200 OK", &response).await;
                    }
                    OperationBehavior::DeduplicatedReplayRecovery => {
                        replay_send_count = replay_send_count.saturating_add(1);
                        if replay_send_count == 1 {
                            let value: Value =
                                serde_json::from_slice(request_body(&request)).unwrap();
                            reconciliation_message = Some(value["message"].clone());
                            socket.shutdown().await.unwrap();
                            continue;
                        }
                        let value: Value = serde_json::from_slice(request_body(&request)).unwrap();
                        assert_eq!(
                            value["message"]["messageId"],
                            reconciliation_message.as_ref().unwrap()["messageId"]
                        );
                        write_json(&mut socket, binding, "200 OK", &success).await;
                    }
                    OperationBehavior::DurableTaskRecovery => {
                        let value: Value =
                            serde_json::from_slice(request_body(&request)).unwrap_or(Value::Null);
                        let operation = if binding == A2aBinding::JsonRpc {
                            value["method"].as_str()
                        } else if target.contains("message:send") {
                            Some("SendMessage")
                        } else {
                            Some("GetTask")
                        };
                        if operation == Some("SendMessage") {
                            let payload = if binding == A2aBinding::JsonRpc {
                                &value["params"]
                            } else {
                                &value
                            };
                            reconciliation_message = Some(payload["message"].clone());
                            let result = json!({
                                "task": {
                                    "id": "durable-task-1",
                                    "contextId": payload["message"]["contextId"].clone(),
                                    "status": {"state": "TASK_STATE_WORKING"}
                                }
                            });
                            let response = if binding == A2aBinding::JsonRpc {
                                json!({
                                    "jsonrpc": "2.0",
                                    "id": value["id"].clone(),
                                    "result": result
                                })
                            } else {
                                result
                            };
                            write_json(&mut socket, binding, "200 OK", &response).await;
                            continue;
                        }
                        assert_eq!(operation, Some("GetTask"));
                        task_get_count = task_get_count.saturating_add(1);
                        let state = if task_get_count == 1 {
                            "TASK_STATE_WORKING"
                        } else {
                            "TASK_STATE_COMPLETED"
                        };
                        let mut task = json!({
                            "id": "durable-task-1",
                            "contextId": reconciliation_message.as_ref().unwrap()["contextId"],
                            "status": {"state": state}
                        });
                        if task_get_count > 1 {
                            task["artifacts"] = json!([{
                                "artifactId": "remote-artifact-1",
                                "name": "answer.txt",
                                "description": "Remote answer",
                                "parts": [{
                                    "text": "durable artifact",
                                    "mediaType": "text/plain;charset=utf-8",
                                    "filename": "answer.txt"
                                }]
                            }]);
                        }
                        let response = if binding == A2aBinding::JsonRpc {
                            json!({
                                "jsonrpc": "2.0",
                                "id": value["id"].clone(),
                                "result": task
                            })
                        } else {
                            task
                        };
                        write_json(&mut socket, binding, "200 OK", &response).await;
                    }
                }
            }
        });
        let mut interface_pin =
            A2aClientInterfacePin::loopback_http(&interface_url, binding).unwrap();
        if let Some(tenant) = tenant {
            interface_pin = interface_pin.with_tenant(tenant).unwrap();
        }
        Self {
            card_endpoint: A2aAgentCardEndpoint::loopback_http(&format!(
                "http://{address}/.well-known/agent-card.json"
            ))
            .unwrap(),
            interface_pin,
            card,
            requests,
            task,
        }
    }

    async fn next_request(&mut self) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(2), self.requests.recv())
            .await
            .expect("A2A request must reach the loopback server")
            .expect("A2A request channel must remain open")
    }
}

impl Drop for TestA2aServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn agent_card(
    interface_url: &str,
    binding: A2aBinding,
    tenant: Option<&str>,
    secured: bool,
    capabilities: A2aAgentCapabilities,
) -> Value {
    let mut interface = A2aAgentInterface::new(interface_url, binding).unwrap();
    if let Some(tenant) = tenant {
        interface = interface.with_tenant(tenant).unwrap();
    }
    let mut skill = A2aAgentSkill::new(
        "answer",
        "Answer",
        "Returns a bounded structured answer.",
        vec!["test".to_string()],
    )
    .unwrap()
    .with_input_modes(vec!["application/json".to_string()])
    .unwrap()
    .with_output_modes(vec!["application/json".to_string()])
    .unwrap();
    if secured {
        skill = skill
            .with_security_requirements(vec![security_requirement("skill.invoke")])
            .unwrap();
    }
    let mut builder = A2aAgentCard::builder(
        "StateKnot contract agent",
        "Loopback A2A contract server.",
        "1.0.0",
    )
    .unwrap()
    .capabilities(capabilities)
    .interface(interface)
    .unwrap()
    .default_input_modes(vec!["application/json".to_string()])
    .unwrap()
    .default_output_modes(vec!["application/json".to_string()])
    .unwrap()
    .skill(skill)
    .unwrap();
    if secured {
        builder = builder
            .security_scheme(
                "oidc",
                A2aSecurityScheme::open_id_connect(
                    "https://issuer.example/.well-known/openid-configuration",
                    None,
                )
                .unwrap(),
            )
            .unwrap()
            .security_requirements(vec![security_requirement("agent.invoke")])
            .unwrap();
    }
    builder.build().unwrap().to_json().unwrap()
}

async fn write_operation_matrix_response(
    socket: &mut tokio::net::TcpStream,
    binding: A2aBinding,
    request: &[u8],
    extended_card: &Value,
) {
    let target = request_target(request);
    let method = request_method(request);
    let envelope = (binding == A2aBinding::JsonRpc).then(|| {
        serde_json::from_slice::<Value>(request_body(request))
            .expect("JSON-RPC matrix request must be JSON")
    });
    let operation = envelope
        .as_ref()
        .and_then(|value| value["method"].as_str())
        .unwrap_or_else(|| rest_operation(method, target));
    let request_id = envelope.as_ref().map(|value| value["id"].clone());

    if matches!(operation, "SendStreamingMessage" | "SubscribeToTask") {
        let events = if operation == "SendStreamingMessage" {
            vec![stream_message("matrix-event", "bounded")]
        } else {
            vec![json!({"task": matrix_task()}), matrix_status_update()]
        };
        let mut body = String::new();
        for event in events {
            let event = request_id.as_ref().map_or(
                event.clone(),
                |id| json!({"jsonrpc": "2.0", "id": id, "result": event}),
            );
            write!(
                &mut body,
                "event: message\ndata: {}\n\n",
                serde_json::to_string(&event).unwrap()
            )
            .unwrap();
        }
        write_response(socket, "200 OK", "text/event-stream", body.as_bytes()).await;
        return;
    }

    let result = match operation {
        "SendMessage" => successful_send_response(),
        "GetTask" | "CancelTask" => matrix_task(),
        "ListTasks" => json!({
            "tasks": [matrix_task()],
            "nextPageToken": "",
            "pageSize": 1,
            "totalSize": 1
        }),
        "CreateTaskPushNotificationConfig" | "GetTaskPushNotificationConfig" => {
            matrix_push_config()
        }
        "ListTaskPushNotificationConfigs" => json!({
            "configs": [matrix_push_config()],
            "nextPageToken": null
        }),
        "DeleteTaskPushNotificationConfig" => Value::Null,
        "GetExtendedAgentCard" => extended_card.clone(),
        unexpected => panic!("unexpected A2A matrix operation: {unexpected}"),
    };
    let response = request_id.map_or(
        result.clone(),
        |id| json!({"jsonrpc": "2.0", "id": id, "result": result}),
    );
    write_json(socket, binding, "200 OK", &response).await;
}

fn request_method(request: &[u8]) -> &str {
    let line_end = find_bytes(request, b"\r\n").unwrap();
    let line = std::str::from_utf8(&request[..line_end]).unwrap();
    line.split_ascii_whitespace().next().unwrap()
}

fn rest_operation(method: &str, target: &str) -> &'static str {
    let path = target.split('?').next().unwrap();
    match (method, path) {
        ("POST", "/a2a/message:send") => "SendMessage",
        ("POST", "/a2a/message:stream") => "SendStreamingMessage",
        ("GET", "/a2a/tasks/task-1") => "GetTask",
        ("GET", "/a2a/tasks") => "ListTasks",
        ("POST", "/a2a/tasks/task-1:cancel") => "CancelTask",
        ("POST", "/a2a/tasks/task-1:subscribe") => "SubscribeToTask",
        ("POST", "/a2a/tasks/task-1/pushNotificationConfigs") => "CreateTaskPushNotificationConfig",
        ("GET", "/a2a/tasks/task-1/pushNotificationConfigs") => "ListTaskPushNotificationConfigs",
        ("GET", "/a2a/tasks/task-1/pushNotificationConfigs/config-1") => {
            "GetTaskPushNotificationConfig"
        }
        ("DELETE", "/a2a/tasks/task-1/pushNotificationConfigs/config-1") => {
            "DeleteTaskPushNotificationConfig"
        }
        ("GET", "/a2a/extendedAgentCard") => "GetExtendedAgentCard",
        unexpected => panic!("unexpected A2A REST matrix request: {unexpected:?}"),
    }
}

fn matrix_task() -> Value {
    json!({
        "id": "task-1",
        "contextId": "context-1",
        "status": {"state": "TASK_STATE_WORKING"}
    })
}

fn matrix_push_config() -> Value {
    json!({
        "url": "https://hooks.example.test/a2a",
        "id": "config-1",
        "taskId": "task-1"
    })
}

fn matrix_status_update() -> Value {
    json!({
        "statusUpdate": {
            "taskId": "task-1",
            "contextId": "context-1",
            "status": {"state": "TASK_STATE_COMPLETED"}
        }
    })
}

fn security_requirement(scope: &str) -> HashMap<String, Vec<String>> {
    HashMap::from([("oidc".to_string(), vec![scope.to_string()])])
}

fn successful_send_response() -> Value {
    A2aSendMessageResponse::Message(
        A2aMessage::new(
            "remote-message-1",
            A2aMessageRole::Agent,
            vec![
                A2aPart::data(json!({"answer": "durable"}))
                    .unwrap()
                    .with_media_type("application/json")
                    .unwrap(),
            ],
        )
        .unwrap()
        .with_context_id("context-1")
        .unwrap(),
    )
    .to_json()
    .unwrap()
}

fn stream_message(id: &str, value: &str) -> Value {
    json!({
        "message": {
            "messageId": id,
            "contextId": "context-1",
            "role": "ROLE_AGENT",
            "parts": [{"text": value, "mediaType": "text/plain"}]
        }
    })
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 2048];
    loop {
        let read = socket.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0, "request ended before its complete headers/body");
        bytes.extend_from_slice(&buffer[..read]);
        assert!(
            bytes.len() <= 2 * 1024 * 1024,
            "test request exceeded bound"
        );
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
            .unwrap_or(0);
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

fn request_target(request: &[u8]) -> &str {
    let line_end = find_bytes(request, b"\r\n").unwrap();
    let line = std::str::from_utf8(&request[..line_end]).unwrap();
    line.split_ascii_whitespace().nth(1).unwrap()
}

fn request_body(request: &[u8]) -> &[u8] {
    let header_end = find_bytes(request, b"\r\n\r\n").unwrap();
    &request[header_end + 4..]
}

async fn write_json(
    socket: &mut tokio::net::TcpStream,
    binding: A2aBinding,
    status: &str,
    response: &Value,
) {
    let content_type = match binding {
        A2aBinding::HttpJson => "application/a2a+json",
        A2aBinding::JsonRpc => "application/json",
        _ => unreachable!("test covers the StateKnot A2A 1.0 bindings"),
    };
    write_response(
        socket,
        status,
        content_type,
        &serde_json::to_vec(response).unwrap(),
    )
    .await;
}

async fn write_response(
    socket: &mut tokio::net::TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) {
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    socket.shutdown().await.unwrap();
}

fn schemas() -> (Arc<JsonSchemaRegistry>, SchemaReference, SchemaReference) {
    let input = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://schemas.stateknot.test/a2a-input/1.0.0",
        "type": "object",
        "properties": {"question": {"type": "string", "maxLength": 256}},
        "required": ["question"],
        "additionalProperties": false
    });
    let output = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://schemas.stateknot.test/a2a-output/1.0.0",
        "type": "object",
        "minProperties": 1,
        "maxProperties": 1,
        "properties": {
            "message": {"type": "object"},
            "task": {"type": "object"}
        },
        "oneOf": [
            {"required": ["message"]},
            {"required": ["task"]}
        ],
        "additionalProperties": false
    });
    let input_reference = schema_reference(&input);
    let output_reference = schema_reference(&output);
    let mut builder = JsonSchemaRegistryBuilder::with_default_limits();
    builder.register(input_reference.clone(), input).unwrap();
    builder.register(output_reference.clone(), output).unwrap();
    (
        Arc::new(builder.build().unwrap()),
        input_reference,
        output_reference,
    )
}

fn durable_schemas() -> (Arc<JsonSchemaRegistry>, SchemaReference, SchemaReference) {
    let input = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://schemas.stateknot.test/a2a-durable-input/1.0.0",
        "type": "object",
        "properties": {"question": {"type": "string", "maxLength": 256}},
        "required": ["question"],
        "additionalProperties": false
    });
    let output = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://schemas.stateknot.test/a2a-durable-output/1.0.0",
        "type": "object",
        "properties": {
            "kind": {"const": "task"},
            "task_id": {"type": "string", "minLength": 1, "maxLength": 1024},
            "context_id": {"type": "string", "minLength": 1, "maxLength": 1024},
            "state": {"const": "completed"},
            "artifact_count": {"type": "integer", "minimum": 0, "maximum": 64}
        },
        "required": ["kind", "task_id", "context_id", "state", "artifact_count"],
        "additionalProperties": false
    });
    let input_reference = schema_reference(&input);
    let output_reference = schema_reference(&output);
    let mut builder = JsonSchemaRegistryBuilder::with_default_limits();
    builder.register(input_reference.clone(), input).unwrap();
    builder.register(output_reference.clone(), output).unwrap();
    (
        Arc::new(builder.build().unwrap()),
        input_reference,
        output_reference,
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

fn descriptor(
    input: &SchemaReference,
    output: &SchemaReference,
    delivery: A2aRemoteAgentDelivery,
    credentials: bool,
) -> ToolDescriptor {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-tool-v1.json"
    ))
    .unwrap();
    let mut value = fixture["descriptors"]["valid"][0].clone();
    value["input_schema"] = serde_json::to_value(input).unwrap();
    value["output_schema"] = serde_json::to_value(output).unwrap();
    value["semantics"] = match delivery {
        A2aRemoteAgentDelivery::AtMostOnce => json!({
            "risk": "non_idempotent_write",
            "idempotency": "unsupported",
            "status_query": false,
            "compensation": false
        }),
        A2aRemoteAgentDelivery::MessageIdDeduplicated => json!({
            "risk": "idempotent_write",
            "idempotency": "required_key",
            "status_query": false,
            "compensation": false
        }),
        _ => unreachable!("test covers the current delivery profiles"),
    };
    value["resources"] = json!({
        "network": "read_write",
        "filesystem": "none",
        "credentials": credentials,
        "dynamic_code": false
    });
    value["invocation"] = json!({
        "cancellation": "cooperative",
        "max_progress_events": "0"
    });
    serde_json::from_value(value).unwrap()
}

fn reconciliation_descriptor(
    input: &SchemaReference,
    output: &SchemaReference,
    delivery: A2aRemoteAgentDelivery,
) -> ToolDescriptor {
    let mut value = serde_json::to_value(descriptor(input, output, delivery, false)).unwrap();
    value["semantics"]["status_query"] = Value::Bool(true);
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
        TenantId::new("tenant-a2a-contract").unwrap(),
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

fn reconciliation_context(descriptor: &ToolDescriptor) -> ToolReconciliationContext {
    let observed_at = "2029-12-31T23:59:59.000000Z".parse::<Timestamp>().unwrap();
    ToolReconciliationContext::new(
        TenantId::new("tenant-a2a-contract").unwrap(),
        RUN_ID.parse().unwrap(),
        THREAD_ID.parse().unwrap(),
        INVOCATION_ID.parse().unwrap(),
        ATTEMPT_ID.parse().unwrap(),
        descriptor,
        DurationMillis::new(30_000).unwrap(),
        observed_at,
        Instant::now(),
        "2030-01-01T00:00:00.000000Z".parse().unwrap(),
        CancellationSignal::never(),
    )
    .unwrap()
}

fn durable_context(descriptor: &ToolDescriptor) -> ToolContext {
    context(descriptor).with_durable_origin_event(ORIGIN_EVENT_ID.parse().unwrap())
}

fn durable_reconciliation_context(
    descriptor: &ToolDescriptor,
    handle: ToolRecoveryHandle,
) -> ToolReconciliationContext {
    reconciliation_context(descriptor)
        .with_durable_recovery(ORIGIN_EVENT_ID.parse().unwrap(), Some(handle))
}

#[derive(Default)]
struct CapturingArtifactIngestor {
    requests: Mutex<Vec<(String, String, u16, u16)>>,
}

impl A2aArtifactIngestor for CapturingArtifactIngestor {
    fn ingest(
        &self,
        request: A2aArtifactIngestionRequest,
    ) -> BoxFuture<'_, Result<ArtifactRef, A2aArtifactIngestionError>> {
        self.requests.lock().unwrap().push((
            request.source().task_id().to_owned(),
            request.source().artifact_id().to_owned(),
            request.source().artifact_index(),
            request.source().part_index(),
        ));
        Box::pin(async move {
            let bytes = match request.part().content() {
                A2aPartContent::Text(value) => value.as_bytes().to_vec(),
                A2aPartContent::Data(value) => serde_json_canonicalizer::to_vec(value).unwrap(),
                A2aPartContent::Raw(value) => value.to_vec(),
                A2aPartContent::Url(_) => panic!("the capture ingestor does not fetch URLs"),
                _ => panic!("unsupported A2A part type in the capture ingestor"),
            };
            assert!(u64::try_from(bytes.len()).unwrap() <= request.maximum_bytes().get());
            let media_type = request
                .part()
                .media_type()
                .unwrap_or("application/octet-stream")
                .parse()
                .unwrap();
            let modality = match request.part().content() {
                A2aPartContent::Text(_) => ArtifactModality::Text,
                A2aPartContent::Data(_) => ArtifactModality::StructuredData,
                A2aPartContent::Raw(_) | A2aPartContent::Url(_) => ArtifactModality::Binary,
                _ => panic!("unsupported A2A part type in the capture ingestor"),
            };
            let name = request
                .part()
                .filename()
                .or_else(|| request.artifact_name())
                .unwrap_or("a2a-artifact.bin");
            Ok(ArtifactRef::new(
                ArtifactIdentity::new(
                    request.tenant_id().clone(),
                    "01912345-6789-7abc-8def-0123456789c1"
                        .parse::<ArtifactId>()
                        .unwrap(),
                ),
                ArtifactPresentation::new(ArtifactName::new(name).unwrap(), None),
                ArtifactRepresentation::new(
                    media_type,
                    modality,
                    ByteCount::new(u64::try_from(bytes.len()).unwrap()),
                    Digest::sha256(&bytes),
                    None,
                )
                .unwrap(),
                ContentMetadata::untrusted(
                    ContentSource::Artifact,
                    SecurityLabel::new("external/a2a").unwrap(),
                ),
                RetentionClass::new("standard").unwrap(),
                ArtifactProvenance::new(
                    request.tool().owner().clone(),
                    Some(request.tool().capability().clone()),
                    request.run_id(),
                    request.origin_event_id(),
                ),
                ArtifactParents::empty(),
            )
            .unwrap())
        })
    }
}

async fn discover(
    server: &TestA2aServer,
    security: A2aClientSecurity,
    options: A2aClientOptions,
) -> A2aClient {
    A2aClient::discover(
        server.card_endpoint.clone(),
        vec![server.interface_pin.clone()],
        A2aAgentCardTrust::CanonicalSha256(a2a_agent_card_digest(&server.card).unwrap()),
        security,
        Vec::new(),
        options,
    )
    .await
    .unwrap()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthorizationObservation {
    operation: A2aClientOperation,
    scopes: Vec<String>,
    local_tenant: String,
    remote_tenant: Option<String>,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
}

#[derive(Default)]
struct CapturingTokenProvider {
    observations: Mutex<Vec<AuthorizationObservation>>,
}

#[derive(Default)]
struct DirectCapturingTokenProvider {
    observations: Mutex<Vec<(A2aClientOperation, Option<String>)>>,
}

impl A2aBearerTokenProvider for DirectCapturingTokenProvider {
    fn resolve(
        &self,
        request: &A2aClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<ApiKey, A2aClientAuthorizationError>> {
        self.observations.lock().unwrap().push((
            request.operation(),
            request.task_id().map(ToOwned::to_owned),
        ));
        Box::pin(async { Ok(ApiKey::new(TOKEN).unwrap()) })
    }
}

impl A2aBearerTokenProvider for CapturingTokenProvider {
    fn resolve(
        &self,
        request: &A2aClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<ApiKey, A2aClientAuthorizationError>> {
        let attempt = request
            .attempt()
            .expect("adapter supplies attempt identity");
        self.observations
            .lock()
            .unwrap()
            .push(AuthorizationObservation {
                operation: request.operation(),
                scopes: request.required_scopes().map(ToOwned::to_owned).collect(),
                local_tenant: attempt.tenant_id().to_string(),
                remote_tenant: request.remote_tenant().map(ToOwned::to_owned),
                invocation_id: attempt.invocation_id(),
                attempt_id: attempt.attempt_id(),
            });
        Box::pin(async { Ok(ApiKey::new(TOKEN).unwrap()) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_json_adapter_preserves_tenant_headers_and_attempt_message_id() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        Some(ROUTING_TENANT),
        false,
        false,
        OperationBehavior::Success,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;
    let (registry, input_schema, output_schema) = schemas();
    let descriptor = descriptor(
        &input_schema,
        &output_schema,
        A2aRemoteAgentDelivery::AtMostOnce,
        false,
    );
    let adapter = A2aRemoteAgent::bind(
        descriptor.clone(),
        client,
        "answer",
        A2aRemoteAgentDelivery::AtMostOnce,
        registry,
    )
    .unwrap();
    let result = adapter
        .call(
            context(&descriptor),
            ToolInput::new(
                input_schema,
                BoundedJson::try_from_value(json!({"question": "Is this durable?"})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result.output().as_value(), &successful_send_response());

    let discovery = server.next_request().await;
    assert_eq!(request_target(&discovery), "/.well-known/agent-card.json");
    let call = server.next_request().await;
    assert_eq!(request_target(&call), "/a2a/remote-tenant/message:send");
    let headers = String::from_utf8_lossy(&call).to_ascii_lowercase();
    assert!(headers.contains("a2a-version: 1.0"));
    assert!(headers.contains("content-type: application/a2a+json"));
    assert!(headers.contains("accept: application/a2a+json, application/json"));
    let body: Value = serde_json::from_slice(request_body(&call)).unwrap();
    assert_eq!(body["tenant"], ROUTING_TENANT);
    assert_eq!(
        body["message"]["messageId"],
        format!("stateknot-attempt-{ATTEMPT_ID}")
    );
    assert_eq!(
        body["message"]["parts"][0]["data"]["question"],
        "Is this durable?"
    );
    assert_eq!(body["message"]["parts"][0]["mediaType"], "application/json");
    assert_eq!(body["configuration"]["returnImmediately"], true);
    assert_eq!(
        body["configuration"]["acceptedOutputModes"],
        json!(["application/json"])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_history_reconciliation_recovers_lost_send_without_replay() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        false,
        OperationBehavior::ContextHistoryRecovery,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;
    let (registry, input_schema, output_schema) = schemas();
    let descriptor = reconciliation_descriptor(
        &input_schema,
        &output_schema,
        A2aRemoteAgentDelivery::AtMostOnce,
    );
    let recovery = A2aRemoteAgentRecovery::operator_attested_context_task_history(
        2,
        16,
        DurationMillis::new(250).unwrap(),
    )
    .unwrap();
    let adapter = A2aRemoteAgent::bind_with_recovery(
        descriptor.clone(),
        client,
        "answer",
        A2aRemoteAgentDelivery::AtMostOnce,
        recovery,
        registry,
    )
    .unwrap();
    let input = ToolInput::new(
        input_schema,
        BoundedJson::try_from_value(json!({"question": "Recover without replay"})).unwrap(),
    )
    .unwrap();
    let error = adapter
        .call(context(&descriptor), input.clone())
        .await
        .unwrap_err();
    assert_eq!(error.external_effect(), ToolExternalEffect::Unknown);
    assert_eq!(error.failure().retry_advice(), RetryAdvice::ReconcileFirst);

    let _discovery = server.next_request().await;
    let lost_send = server.next_request().await;
    let sent: Value = serde_json::from_slice(request_body(&lost_send)).unwrap();
    let opaque_context = sent["message"]["contextId"].as_str().unwrap();
    assert!(opaque_context.starts_with("stateknot-context-"));
    assert!(!opaque_context.contains(RUN_ID));

    let observation = adapter
        .reconcile(reconciliation_context(&descriptor), input)
        .await
        .unwrap();
    let result = match observation {
        ToolReconciliationObservation::Result(result) => result,
        other => panic!("expected reconciled result, got {other:?}"),
    };
    assert_eq!(
        result.provenance().invocation_id().to_string(),
        INVOCATION_ID
    );
    assert_eq!(result.provenance().attempt_id().to_string(), ATTEMPT_ID);
    assert_eq!(result.output().as_value()["task"]["id"], "recovered-task-1");

    let query = server.next_request().await;
    assert_eq!(request_method(&query), "GET");
    let target = request_target(&query);
    assert!(target.starts_with("/a2a/tasks?"));
    assert!(target.contains("contextId=stateknot-context-"));
    assert!(target.contains("historyLength=16"));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), server.requests.recv())
            .await
            .is_err(),
        "context-history recovery must not send the message again"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_json_durable_task_handle_polls_without_resend_and_materializes_artifacts() {
    exercise_durable_task_recovery(A2aBinding::HttpJson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_rpc_durable_task_handle_polls_without_resend_and_materializes_artifacts() {
    exercise_durable_task_recovery(A2aBinding::JsonRpc).await;
}

#[allow(clippy::too_many_lines)]
async fn exercise_durable_task_recovery(binding: A2aBinding) {
    let mut server = TestA2aServer::start(
        binding,
        None,
        false,
        false,
        OperationBehavior::DurableTaskRecovery,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;
    let (registry, input_schema, output_schema) = durable_schemas();
    let descriptor = reconciliation_descriptor(
        &input_schema,
        &output_schema,
        A2aRemoteAgentDelivery::AtMostOnce,
    );
    let recovery = A2aRemoteAgentRecovery::operator_attested_context_task_history(
        2,
        16,
        DurationMillis::new(25).unwrap(),
    )
    .unwrap();
    let ingestor = Arc::new(CapturingArtifactIngestor::default());
    let adapter = A2aRemoteAgent::bind_with_durable_artifacts(
        descriptor.clone(),
        client,
        "answer",
        A2aRemoteAgentDelivery::AtMostOnce,
        recovery,
        registry,
        ingestor.clone(),
    )
    .unwrap();
    let input = ToolInput::new(
        input_schema,
        BoundedJson::try_from_value(json!({"question": "Return one artifact"})).unwrap(),
    )
    .unwrap();

    let error = adapter
        .call(durable_context(&descriptor), input.clone())
        .await
        .unwrap_err();
    assert_eq!(error.external_effect(), ToolExternalEffect::Unknown);
    let handle = error.recovery_handle().unwrap().clone();
    assert_eq!(handle.opaque_id(), "durable-task-1");
    assert!(!format!("{error:?}").contains("durable-task-1"));
    assert_eq!(
        serde_json::to_value(&error).unwrap()["recovery_handle"]["opaque_id"],
        "durable-task-1"
    );

    let first = adapter
        .reconcile(
            durable_reconciliation_context(&descriptor, handle.clone()),
            input.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        first,
        ToolReconciliationObservation::Pending { retry_after }
            if retry_after == DurationMillis::new(25).unwrap()
    ));

    let second = adapter
        .reconcile(durable_reconciliation_context(&descriptor, handle), input)
        .await
        .unwrap();
    let result = match second {
        ToolReconciliationObservation::Result(result) => result,
        other => panic!("expected a durable artifact result, got {other:?}"),
    };
    assert_eq!(result.output().as_value()["state"], "completed");
    assert_eq!(result.output().as_value()["artifact_count"], 1);
    assert_eq!(result.artifacts().len(), 1);
    assert_eq!(
        result
            .artifacts()
            .iter()
            .next()
            .unwrap()
            .representation()
            .byte_length(),
        ByteCount::new(16)
    );
    assert_eq!(
        ingestor.requests.lock().unwrap().as_slice(),
        &[(
            "durable-task-1".to_string(),
            "remote-artifact-1".to_string(),
            0,
            0,
        )]
    );

    let discovery = server.next_request().await;
    let send = server.next_request().await;
    let first_get = server.next_request().await;
    let second_get = server.next_request().await;
    assert_eq!(request_target(&discovery), "/.well-known/agent-card.json");
    match binding {
        A2aBinding::HttpJson => {
            assert_eq!(request_target(&send), "/a2a/message:send");
            assert_eq!(request_target(&first_get), "/a2a/tasks/durable-task-1");
            assert_eq!(request_target(&second_get), "/a2a/tasks/durable-task-1");
        }
        A2aBinding::JsonRpc => {
            for (request, method) in [
                (&send, "SendMessage"),
                (&first_get, "GetTask"),
                (&second_get, "GetTask"),
            ] {
                assert_eq!(request_target(request), "/rpc");
                assert_eq!(
                    serde_json::from_slice::<Value>(request_body(request)).unwrap()["method"],
                    method
                );
            }
        }
        _ => unreachable!("test covers the implemented A2A bindings"),
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), server.requests.recv())
            .await
            .is_err(),
        "task polling must not replay the original business message"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_history_reconciliation_rejects_same_id_payload_substitution() {
    let server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        false,
        OperationBehavior::ContextHistoryPayloadMismatch,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;
    let (registry, input_schema, output_schema) = schemas();
    let descriptor = reconciliation_descriptor(
        &input_schema,
        &output_schema,
        A2aRemoteAgentDelivery::AtMostOnce,
    );
    let recovery = A2aRemoteAgentRecovery::operator_attested_context_task_history(
        2,
        16,
        DurationMillis::new(250).unwrap(),
    )
    .unwrap();
    let adapter = A2aRemoteAgent::bind_with_recovery(
        descriptor.clone(),
        client,
        "answer",
        A2aRemoteAgentDelivery::AtMostOnce,
        recovery,
        registry,
    )
    .unwrap();
    let input = ToolInput::new(
        input_schema,
        BoundedJson::try_from_value(json!({"question": "Reject substitution"})).unwrap(),
    )
    .unwrap();
    assert_eq!(
        adapter
            .call(context(&descriptor), input.clone())
            .await
            .unwrap_err()
            .external_effect(),
        ToolExternalEffect::Unknown
    );

    let error = adapter
        .reconcile(reconciliation_context(&descriptor), input)
        .await
        .unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::DataCorruption);
    assert_eq!(error.failure().code().as_str(), "probe.message_mismatch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deduplicated_replay_uses_the_exact_original_message_identity() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        false,
        OperationBehavior::DeduplicatedReplayRecovery,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;
    let (registry, input_schema, output_schema) = schemas();
    let descriptor = reconciliation_descriptor(
        &input_schema,
        &output_schema,
        A2aRemoteAgentDelivery::MessageIdDeduplicated,
    );
    let recovery = A2aRemoteAgentRecovery::operator_attested_message_id_replay(
        DurationMillis::new(250).unwrap(),
    )
    .unwrap();
    let adapter = A2aRemoteAgent::bind_with_recovery(
        descriptor.clone(),
        client,
        "answer",
        A2aRemoteAgentDelivery::MessageIdDeduplicated,
        recovery,
        registry,
    )
    .unwrap();
    let input = ToolInput::new(
        input_schema,
        BoundedJson::try_from_value(json!({"question": "Replay once safely"})).unwrap(),
    )
    .unwrap();
    assert_eq!(
        adapter
            .call(context(&descriptor), input.clone())
            .await
            .unwrap_err()
            .external_effect(),
        ToolExternalEffect::Unknown
    );
    let observation = adapter
        .reconcile(reconciliation_context(&descriptor), input)
        .await
        .unwrap();
    assert!(matches!(
        observation,
        ToolReconciliationObservation::Result(_)
    ));

    let _discovery = server.next_request().await;
    let initial = server.next_request().await;
    let replay = server.next_request().await;
    let initial: Value = serde_json::from_slice(request_body(&initial)).unwrap();
    let replay: Value = serde_json::from_slice(request_body(&replay)).unwrap();
    assert_eq!(
        initial["message"]["messageId"],
        format!("stateknot-invocation-{INVOCATION_ID}")
    );
    assert_eq!(
        replay["message"]["messageId"],
        initial["message"]["messageId"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_rpc_adapter_uses_stable_invocation_id_and_skill_scoped_authorization() {
    let mut server = TestA2aServer::start(
        A2aBinding::JsonRpc,
        Some(ROUTING_TENANT),
        true,
        false,
        OperationBehavior::Success,
    )
    .await;
    let provider = Arc::new(CapturingTokenProvider::default());
    let client = discover(
        &server,
        A2aClientSecurity::bearer("oidc", provider.clone()).unwrap(),
        A2aClientOptions::default(),
    )
    .await;
    let (registry, input_schema, output_schema) = schemas();
    let descriptor = descriptor(
        &input_schema,
        &output_schema,
        A2aRemoteAgentDelivery::MessageIdDeduplicated,
        true,
    );
    let adapter = A2aRemoteAgent::bind(
        descriptor.clone(),
        client,
        "answer",
        A2aRemoteAgentDelivery::MessageIdDeduplicated,
        registry,
    )
    .unwrap();
    adapter
        .call(
            context(&descriptor),
            ToolInput::new(
                input_schema,
                BoundedJson::try_from_value(json!({"question": "Use the stable key"})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let _discovery = server.next_request().await;
    let call = server.next_request().await;
    assert_eq!(request_target(&call), "/rpc");
    let headers = String::from_utf8_lossy(&call).to_ascii_lowercase();
    assert!(headers.contains(&format!("authorization: bearer {TOKEN}")));
    assert!(headers.contains("content-type: application/json"));
    let body: Value = serde_json::from_slice(request_body(&call)).unwrap();
    assert_eq!(body["jsonrpc"], "2.0");
    assert_eq!(body["method"], "SendMessage");
    assert_eq!(body["id"], 1);
    assert_eq!(body["params"]["tenant"], ROUTING_TENANT);
    assert_eq!(
        body["params"]["message"]["messageId"],
        format!("stateknot-invocation-{INVOCATION_ID}")
    );
    assert_eq!(
        provider.observations.lock().unwrap().as_slice(),
        &[AuthorizationObservation {
            operation: A2aClientOperation::SendMessage,
            scopes: vec!["skill.invoke".to_string()],
            local_tenant: "tenant-a2a-contract".to_string(),
            remote_tenant: Some(ROUTING_TENANT.to_string()),
            invocation_id: INVOCATION_ID.parse().unwrap(),
            attempt_id: ATTEMPT_ID.parse().unwrap(),
        }]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_bound_send_scopes_credential_resolution_to_the_task() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        true,
        false,
        OperationBehavior::TaskBoundSuccess,
    )
    .await;
    let provider = Arc::new(DirectCapturingTokenProvider::default());
    let client = discover(
        &server,
        A2aClientSecurity::bearer("oidc", provider.clone()).unwrap(),
        A2aClientOptions::default(),
    )
    .await;
    let message = A2aMessage::new(
        "message-1",
        A2aMessageRole::User,
        vec![A2aPart::text("continue").unwrap()],
    )
    .unwrap()
    .with_task_id("task-1")
    .unwrap()
    .with_context_id("context-1")
    .unwrap();

    let response = client
        .send_message(A2aSendMessageRequest::new(message))
        .await
        .unwrap();
    assert!(matches!(response, A2aSendMessageResponse::Task(task) if task.id() == "task-1"));
    assert_eq!(
        provider.observations.lock().unwrap().as_slice(),
        &[(A2aClientOperation::SendMessage, Some("task-1".to_string()))]
    );
    let _discovery = server.next_request().await;
    let call = server.next_request().await;
    assert!(
        String::from_utf8_lossy(&call)
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {TOKEN}"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_send_response_is_unknown_and_is_not_retried() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        false,
        OperationBehavior::CloseWithoutResponse,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;
    let (registry, input_schema, output_schema) = schemas();
    let descriptor = descriptor(
        &input_schema,
        &output_schema,
        A2aRemoteAgentDelivery::AtMostOnce,
        false,
    );
    let adapter = A2aRemoteAgent::bind(
        descriptor.clone(),
        client,
        "answer",
        A2aRemoteAgentDelivery::AtMostOnce,
        registry,
    )
    .unwrap();
    let error = adapter
        .call(
            context(&descriptor),
            ToolInput::new(
                input_schema,
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
    let _discovery = server.next_request().await;
    let _call = server.next_request().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), server.requests.recv())
            .await
            .is_err(),
        "the client must not perform a hidden retry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standard_invalid_params_error_is_authoritative_not_applied_evidence() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        false,
        OperationBehavior::DefiniteRejection,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;
    let (registry, input_schema, output_schema) = schemas();
    let descriptor = descriptor(
        &input_schema,
        &output_schema,
        A2aRemoteAgentDelivery::AtMostOnce,
        false,
    );
    let adapter = A2aRemoteAgent::bind(
        descriptor.clone(),
        client,
        "answer",
        A2aRemoteAgentDelivery::AtMostOnce,
        registry,
    )
    .unwrap();
    let error = adapter
        .call(
            context(&descriptor),
            ToolInput::new(
                input_schema,
                BoundedJson::try_from_value(json!({"question": "Reject this"})).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.external_effect(), ToolExternalEffect::NotApplied);
    assert_eq!(error.failure().category(), FailureCategory::InvalidInput);
    assert_eq!(error.failure().retry_advice(), RetryAdvice::Never);
    let _discovery = server.next_request().await;
    let _call = server.next_request().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_events_remain_ordered_and_stop_at_the_configured_event_bound() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        true,
        OperationBehavior::TwoEventStream,
    )
    .await;
    let options = A2aClientOptions::new(
        ProviderHttpOptions::default(),
        Duration::from_secs(2),
        Duration::from_secs(2),
        Duration::from_secs(2),
        1,
    )
    .unwrap();
    let client = discover(&server, A2aClientSecurity::Anonymous, options).await;
    let request = A2aSendMessageRequest::new(
        A2aMessage::new(
            "stream-input",
            A2aMessageRole::User,
            vec![A2aPart::data(json!({"question": "stream"})).unwrap()],
        )
        .unwrap(),
    );
    let mut stream = client.send_streaming_message(request).await.unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert!(matches!(
        first,
        A2aStreamEvent::Task(task) if task.id() == "task-1"
    ));
    let bounded = stream.next().await.unwrap().unwrap_err();
    assert_eq!(
        bounded.kind(),
        stateknot_integrations::A2aClientErrorKind::InvalidResponse
    );
    assert!(stream.next().await.is_none());
    let _discovery = server.next_request().await;
    let call = server.next_request().await;
    assert_eq!(request_target(&call), "/a2a/message:stream");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn premature_task_stream_fails_closed_instead_of_looking_complete() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        true,
        OperationBehavior::PrematureTaskStream,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;
    let request = A2aSendMessageRequest::new(
        A2aMessage::new(
            "premature-stream-input",
            A2aMessageRole::User,
            vec![A2aPart::data(json!({"question": "stream"})).unwrap()],
        )
        .unwrap(),
    );
    let mut stream = client.send_streaming_message(request).await.unwrap();
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        A2aStreamEvent::Task(task) if task.id() == "task-1"
    ));
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(
        error.kind(),
        stateknot_integrations::A2aClientErrorKind::InvalidResponse
    );
    assert!(stream.next().await.is_none());
    let _discovery = server.next_request().await;
    let _call = server.next_request().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_lookup_rejects_a_cross_resource_response() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        false,
        OperationBehavior::CrossResourceTask,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;

    let error = client
        .get_task(A2aGetTaskRequest::new("task-1").unwrap())
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        stateknot_integrations::A2aClientErrorKind::InvalidResponse
    );
    assert!(error.was_dispatched());
    let _discovery = server.next_request().await;
    let _call = server.next_request().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_rejects_an_update_from_another_task() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        true,
        OperationBehavior::CrossResourceStream,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;

    let mut stream = client
        .subscribe_to_task(A2aSubscribeTaskRequest::new("task-1").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        A2aStreamEvent::Task(task) if task.id() == "task-1"
    ));
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(
        error.kind(),
        stateknot_integrations::A2aClientErrorKind::InvalidResponse
    );
    assert!(stream.next().await.is_none());
    let _discovery = server.next_request().await;
    let _call = server.next_request().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_agent_card_requires_an_authenticated_client() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        false,
        OperationBehavior::OperationMatrix,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;

    let error = client.get_extended_agent_card().await.unwrap_err();
    assert_eq!(
        error.kind(),
        stateknot_integrations::A2aClientErrorKind::Authorization
    );
    assert!(!error.was_dispatched());
    let _discovery = server.next_request().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), server.requests.recv())
            .await
            .is_err(),
        "an unauthenticated extended-card request must not be dispatched"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_rpc_delete_requires_a_correlated_response_envelope() {
    let mut server = TestA2aServer::start(
        A2aBinding::JsonRpc,
        None,
        false,
        false,
        OperationBehavior::JsonRpcNoContent,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::Anonymous,
        A2aClientOptions::default(),
    )
    .await;

    let error = client
        .delete_push_config(A2aDeletePushConfigRequest::new("task-1", "config-1").unwrap())
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        stateknot_integrations::A2aClientErrorKind::InvalidResponse
    );
    assert!(error.was_dispatched());
    assert_eq!(error.http_status(), Some(204));

    let _discovery = server.next_request().await;
    let call = server.next_request().await;
    let request: Value = serde_json::from_slice(request_body(&call)).unwrap();
    assert_eq!(request["method"], "DeleteTaskPushNotificationConfig");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discovery_fails_closed_on_digest_or_preferred_interface_pin_drift() {
    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        false,
        OperationBehavior::Success,
    )
    .await;
    let error = A2aClient::discover(
        server.card_endpoint.clone(),
        vec![server.interface_pin.clone()],
        A2aAgentCardTrust::CanonicalSha256(Digest::sha256(b"different card")),
        A2aClientSecurity::Anonymous,
        Vec::new(),
        A2aClientOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        A2aClientBuildError::AgentCardDigestMismatch
    ));
    let _discovery = server.next_request().await;

    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        None,
        false,
        false,
        OperationBehavior::Success,
    )
    .await;
    let wrong_pin =
        A2aClientInterfacePin::loopback_http("http://127.0.0.1:9/different", A2aBinding::HttpJson)
            .unwrap();
    let error = A2aClient::discover(
        server.card_endpoint.clone(),
        vec![wrong_pin],
        A2aAgentCardTrust::CanonicalSha256(a2a_agent_card_digest(&server.card).unwrap()),
        A2aClientSecurity::Anonymous,
        Vec::new(),
        A2aClientOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, A2aClientBuildError::InterfacePinMismatch));
    let _discovery = server.next_request().await;

    let mut server = TestA2aServer::start(
        A2aBinding::HttpJson,
        Some(ROUTING_TENANT),
        false,
        false,
        OperationBehavior::Success,
    )
    .await;
    let interface_url = server.card["supportedInterfaces"][0]["url"]
        .as_str()
        .unwrap();
    let pin_without_tenant =
        A2aClientInterfacePin::loopback_http(interface_url, A2aBinding::HttpJson).unwrap();
    let error = A2aClient::discover(
        server.card_endpoint.clone(),
        vec![pin_without_tenant],
        A2aAgentCardTrust::CanonicalSha256(a2a_agent_card_digest(&server.card).unwrap()),
        A2aClientSecurity::Anonymous,
        Vec::new(),
        A2aClientOptions::default(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, A2aClientBuildError::InterfacePinMismatch));
    let _discovery = server.next_request().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_eleven_operations_round_trip_over_http_json() {
    exercise_operation_matrix(A2aBinding::HttpJson).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_eleven_operations_round_trip_over_json_rpc() {
    exercise_operation_matrix(A2aBinding::JsonRpc).await;
}

#[allow(clippy::too_many_lines)]
async fn exercise_operation_matrix(binding: A2aBinding) {
    let mut server = TestA2aServer::start(
        binding,
        None,
        true,
        true,
        OperationBehavior::OperationMatrix,
    )
    .await;
    let client = discover(
        &server,
        A2aClientSecurity::bearer(
            "oidc",
            Arc::new(StaticA2aBearerToken::new(ApiKey::new(TOKEN).unwrap())),
        )
        .unwrap(),
        A2aClientOptions::default(),
    )
    .await;

    let request_message = || {
        A2aSendMessageRequest::new(
            A2aMessage::new(
                "matrix-request",
                A2aMessageRole::User,
                vec![A2aPart::data(json!({"question": "exercise every operation"})).unwrap()],
            )
            .unwrap(),
        )
    };
    assert!(matches!(
        client.send_message(request_message()).await.unwrap(),
        A2aSendMessageResponse::Message(_)
    ));

    let mut stream = client
        .send_streaming_message(request_message())
        .await
        .unwrap();
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        A2aStreamEvent::Message(_)
    ));
    assert!(stream.next().await.is_none());

    let task = client
        .get_task(
            A2aGetTaskRequest::new("task-1")
                .unwrap()
                .with_history_length(1)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(task.id(), "task-1");

    let tasks = client
        .list_tasks(
            A2aListTasksRequest::new()
                .with_context_id("context-1")
                .unwrap()
                .with_page_size(1)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tasks.tasks().len(), 1);
    assert_eq!(tasks.page_size(), 1);
    assert_eq!(tasks.total_size(), 1);

    assert_eq!(
        client
            .cancel_task(A2aCancelTaskRequest::new("task-1").unwrap())
            .await
            .unwrap()
            .id(),
        "task-1"
    );

    let mut subscription = client
        .subscribe_to_task(A2aSubscribeTaskRequest::new("task-1").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        subscription.next().await.unwrap().unwrap(),
        A2aStreamEvent::Task(_)
    ));
    assert!(matches!(
        subscription.next().await.unwrap().unwrap(),
        A2aStreamEvent::StatusUpdate(update) if update.status().state() == A2aTaskState::Completed
    ));
    assert!(subscription.next().await.is_none());

    let created = client
        .create_push_config(
            A2aPushConfig::new("https://hooks.example.test/a2a")
                .unwrap()
                .with_task_id("task-1")
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(created.id(), Some("config-1"));
    assert_eq!(created.task_id(), Some("task-1"));

    let push_request = A2aGetPushConfigRequest::new("task-1", "config-1").unwrap();
    assert_eq!(
        client
            .get_push_config(push_request.clone())
            .await
            .unwrap()
            .id(),
        Some("config-1")
    );
    assert_eq!(
        client
            .list_push_configs(
                A2aListPushConfigsRequest::new("task-1")
                    .unwrap()
                    .with_page_size(1)
                    .unwrap(),
            )
            .await
            .unwrap()
            .configs()
            .len(),
        1
    );
    client
        .delete_push_config(A2aDeletePushConfigRequest::new("task-1", "config-1").unwrap())
        .await
        .unwrap();
    assert_eq!(
        client.get_extended_agent_card().await.unwrap().name(),
        server.card["name"].as_str().unwrap()
    );

    let discovery = server.next_request().await;
    assert_eq!(request_target(&discovery), "/.well-known/agent-card.json");
    let mut calls = Vec::new();
    for _ in 0..11 {
        calls.push(server.next_request().await);
    }
    match binding {
        A2aBinding::HttpJson => {
            let expected = [
                ("POST", "/a2a/message:send"),
                ("POST", "/a2a/message:stream"),
                ("GET", "/a2a/tasks/task-1?historyLength=1"),
                ("GET", "/a2a/tasks?contextId=context-1&pageSize=1"),
                ("POST", "/a2a/tasks/task-1:cancel"),
                ("POST", "/a2a/tasks/task-1:subscribe"),
                ("POST", "/a2a/tasks/task-1/pushNotificationConfigs"),
                ("GET", "/a2a/tasks/task-1/pushNotificationConfigs/config-1"),
                (
                    "GET",
                    "/a2a/tasks/task-1/pushNotificationConfigs?pageSize=1",
                ),
                (
                    "DELETE",
                    "/a2a/tasks/task-1/pushNotificationConfigs/config-1",
                ),
                ("GET", "/a2a/extendedAgentCard"),
            ];
            let observed = calls
                .iter()
                .map(|request| (request_method(request), request_target(request)))
                .collect::<Vec<_>>();
            assert_eq!(observed, expected);
        }
        A2aBinding::JsonRpc => {
            let expected_methods = [
                "SendMessage",
                "SendStreamingMessage",
                "GetTask",
                "ListTasks",
                "CancelTask",
                "SubscribeToTask",
                "CreateTaskPushNotificationConfig",
                "GetTaskPushNotificationConfig",
                "ListTaskPushNotificationConfigs",
                "DeleteTaskPushNotificationConfig",
                "GetExtendedAgentCard",
            ];
            for (index, (request, expected_method)) in
                calls.iter().zip(expected_methods).enumerate()
            {
                assert_eq!(request_method(request), "POST");
                assert_eq!(request_target(request), "/rpc");
                let value: Value = serde_json::from_slice(request_body(request)).unwrap();
                assert_eq!(value["jsonrpc"], "2.0");
                assert_eq!(value["method"], expected_method);
                assert_eq!(value["id"], i64::try_from(index).unwrap() + 1);
            }
        }
        _ => unreachable!("matrix covers the implemented A2A client bindings"),
    }
}
