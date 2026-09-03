// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real `PostgreSQL` + loopback protocol-adapter durability tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use stateknot_core::{
    AgentResultProvenance, AttemptId, BoundedJson, BoxFuture, BudgetRemaining, BudgetUsage,
    CancellationSignal, CapabilityIdentity, CapabilityName, CapabilityReference, Checkpoint,
    CheckpointId, CheckpointState, CheckpointWrite, CompiledGraph, Digest, EventId,
    GraphExecutionLimits, GraphNode, GraphReducerReference, GraphRoutes, InvocationId, IssuerId,
    JournalAppend, JournalEventIntent, JournalEventKind, JournalExpectation, JournalPayload,
    NodeActivation, NodeId, PrincipalIdentity, ReadyNodes, ResolvedBudget, RunId, RunTransition,
    SchemaId, SchemaReference, SubjectId, Superstep, TenantId, ThreadId, ToolArtifacts,
    ToolDescriptor, ToolInput, ToolInvocationIntent, ToolInvocationStatus, ToolResult,
    ToolResultProvenance, Version,
};
use stateknot_integrations::{
    A2aAgentCapabilities, A2aAgentCard, A2aAgentCardEndpoint, A2aAgentCardTrust, A2aAgentInterface,
    A2aAgentSkill, A2aBinding, A2aClient, A2aClientInterfacePin, A2aClientOptions,
    A2aClientSecurity, A2aRemoteAgent, A2aRemoteAgentDelivery, AnonymousMcpAuthorization,
    McpHttpOptions, McpRemoteTool, McpServerIdentity, ProviderEndpoint, a2a_agent_card_digest,
};
use stateknot_runtime::{
    DurableInvocationExecutor, DurableInvocationExecutorOptions, InvocationAttemptEventIds,
    InvocationBudgetContext, InvocationBudgetProvider, InvocationBudgetProviderError,
    InvocationClock, InvocationClockError, InvocationClockObservation, JsonSchemaRegistry,
    JsonSchemaRegistryBuilder, ModelProviderRegistryBuilder, ToolAttemptHandoff,
    ToolAttemptOutcome, ToolAttemptTerminalKind, ToolProviderRegistryBuilder,
    ToolReconciliationHandoff, ToolReconciliationKind, ToolReconciliationOutcome,
    register_standard_invocation_execution_event_schema,
};
use stateknot_store_postgres::{
    CheckpointCommitOutcome, GraphDefinitionRegistrationOutcome, PostgresStore,
    PostgresStoreOptions, PostgresTransportSecurity, RunProjection,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Notify, mpsc},
    task::JoinHandle,
};

const DATABASE_URL_ENV: &str = "STATEKNOT_TEST_DATABASE_URL";
const REQUIRE_DATABASE_ENV: &str = "STATEKNOT_REQUIRE_POSTGRES_TESTS";

struct PausedLostResponseMcpServer {
    endpoint: ProviderEndpoint,
    call_seen: mpsc::Receiver<()>,
    release_call: Arc<Notify>,
    tool_calls: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

struct PausedLostResponseA2aServer {
    card_endpoint: A2aAgentCardEndpoint,
    interface_pin: A2aClientInterfacePin,
    card: Value,
    call_seen: mpsc::Receiver<()>,
    release_call: Arc<Notify>,
    message_sends: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl PausedLostResponseA2aServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let interface_url = format!("http://{address}/a2a");
        let card = A2aAgentCard::builder(
            "StateKnot durable A2A test",
            "Applies one test message before losing its response.",
            "1.0.0",
        )
        .unwrap()
        .capabilities(A2aAgentCapabilities::new())
        .interface(A2aAgentInterface::new(&interface_url, A2aBinding::HttpJson).unwrap())
        .unwrap()
        .default_input_modes(vec!["application/json".to_string()])
        .unwrap()
        .default_output_modes(vec!["application/json".to_string()])
        .unwrap()
        .skill(
            A2aAgentSkill::new(
                "write_once",
                "Write once",
                "Applies exactly one durable test write.",
                vec!["test".to_string()],
            )
            .unwrap()
            .with_input_modes(vec!["application/json".to_string()])
            .unwrap()
            .with_output_modes(vec!["application/json".to_string()])
            .unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        .to_json()
        .unwrap();
        let encoded_card = serde_json::to_vec(&card).unwrap();
        let (call_sender, call_seen) = mpsc::channel(1);
        let release_call = Arc::new(Notify::new());
        let release = Arc::clone(&release_call);
        let message_sends = Arc::new(AtomicUsize::new(0));
        let sends = Arc::clone(&message_sends);
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let discovery = read_request(&mut socket).await;
            assert!(
                String::from_utf8_lossy(&discovery)
                    .starts_with("GET /.well-known/agent-card.json HTTP/1.1")
            );
            write_a2a_response(&mut socket, "application/json", &encoded_card).await;

            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(
                String::from_utf8_lossy(&request).starts_with("POST /a2a/message:send HTTP/1.1")
            );
            let body: Value = serde_json::from_slice(request_body(&request)).unwrap();
            assert_eq!(body["configuration"]["returnImmediately"], true);
            sends.fetch_add(1, Ordering::SeqCst);
            call_sender.send(()).await.unwrap();
            release.notified().await;
            socket.shutdown().await.unwrap();
        });
        Self {
            card_endpoint: A2aAgentCardEndpoint::loopback_http(&format!(
                "http://{address}/.well-known/agent-card.json"
            ))
            .unwrap(),
            interface_pin: A2aClientInterfacePin::loopback_http(
                &interface_url,
                A2aBinding::HttpJson,
            )
            .unwrap(),
            card,
            call_seen,
            release_call,
            message_sends,
            task,
        }
    }
}

impl PausedLostResponseMcpServer {
    async fn start(input_schema: Value, output_schema: Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let endpoint = ProviderEndpoint::loopback_http(&format!("http://{address}/mcp/")).unwrap();
        let (call_sender, call_seen) = mpsc::channel(1);
        let release_call = Arc::new(Notify::new());
        let release = Arc::clone(&release_call);
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&tool_calls);
        let task = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                let message: Value = serde_json::from_slice(request_body(&request)).unwrap();
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
                                    "name": "stateknot-durable-test-mcp",
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
                                "name": "write_once",
                                "description": "Applies exactly one test write.",
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
                    "tools/call" => {
                        calls.fetch_add(1, Ordering::SeqCst);
                        call_sender.send(()).await.unwrap();
                        release.notified().await;
                        socket.shutdown().await.unwrap();
                        continue;
                    }
                    method => panic!("unexpected MCP method {method}"),
                };
                write_json_response(&mut socket, &response).await;
            }
        });
        Self {
            endpoint,
            call_seen,
            release_call,
            tool_calls,
            task,
        }
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

fn request_body(request: &[u8]) -> &[u8] {
    let header_end = find_bytes(request, b"\r\n\r\n").unwrap();
    &request[header_end + 4..]
}

async fn write_json_response(socket: &mut tokio::net::TcpStream, response: &Value) {
    let encoded = serde_json::to_vec(response).unwrap();
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        encoded.len()
    );
    socket.write_all(headers.as_bytes()).await.unwrap();
    socket.write_all(&encoded).await.unwrap();
    socket.shutdown().await.unwrap();
}

async fn write_a2a_response(socket: &mut tokio::net::TcpStream, content_type: &str, body: &[u8]) {
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(headers.as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    socket.shutdown().await.unwrap();
}

struct StaticInvocationBudget {
    resolved: ResolvedBudget,
}

impl InvocationBudgetProvider for StaticInvocationBudget {
    fn remaining(
        &self,
        context: InvocationBudgetContext,
    ) -> BoxFuture<'_, Result<BudgetRemaining, InvocationBudgetProviderError>> {
        let remaining = self
            .resolved
            .remaining(&BudgetUsage::zero(), context.observed_at())
            .map_err(InvocationBudgetProviderError::new);
        Box::pin(async move { remaining })
    }
}

struct FixedInvocationClock;

impl InvocationClock for FixedInvocationClock {
    fn observe(&self) -> Result<InvocationClockObservation, InvocationClockError> {
        Ok(InvocationClockObservation::new(
            "2029-12-31T23:59:30.000000Z".parse().unwrap(),
            Instant::now(),
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn mcp_write_is_durable_before_dispatch_and_reconciles_without_redispatch() {
    let Some(store) = test_store().await else {
        return;
    };
    let (schemas, input_schema, input_document, output_schema, output_document) = tool_schemas();
    let descriptor = write_descriptor(&input_schema, &output_schema);
    let mut server = PausedLostResponseMcpServer::start(input_document, output_document).await;
    let adapter = McpRemoteTool::connect(
        descriptor.clone(),
        "write_once",
        server.endpoint.clone(),
        McpServerIdentity::new("stateknot-durable-test-mcp", "1.0.0").unwrap(),
        Arc::new(schemas.clone()),
        Arc::new(AnonymousMcpAuthorization),
        McpHttpOptions::default(),
    )
    .await
    .unwrap();

    let graph = graph();
    let tenant_id = TenantId::new(format!("mcp-durable-{}", RunId::generate())).unwrap();
    let run_id = RunId::generate();
    let checkpoint = start_run(&store, &graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let invocation_id = InvocationId::generate();
    let prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                checkpoint.event().head(),
                lease.fence().clone(),
            ),
            ToolInvocationIntent::new(
                invocation_activation(checkpoint.checkpoint()),
                invocation_id,
                descriptor.clone(),
                ToolInput::new(
                    input_schema,
                    BoundedJson::try_from_value(json!({"request": "apply-once"})).unwrap(),
                )
                .unwrap(),
                descriptor.limits().clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut tools = ToolProviderRegistryBuilder::new();
    tools.register(Arc::new(adapter)).unwrap();
    let executor = DurableInvocationExecutor::with_clock(
        store.clone(),
        schemas,
        ModelProviderRegistryBuilder::new().build(),
        tools.build(),
        Arc::new(StaticInvocationBudget {
            resolved: invocation_budget(),
        }),
        Arc::new(FixedInvocationClock),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let handoff = ToolAttemptHandoff::new(
        lease.fence().clone(),
        prepared.invocation().clone(),
        AttemptId::generate(),
        InvocationAttemptEventIds::generate(),
        CancellationSignal::never(),
        None,
    )
    .unwrap();
    let executing_task = {
        let executor = executor.clone();
        let handoff = handoff.clone();
        tokio::spawn(async move { executor.execute_tool(handoff).await })
    };

    tokio::time::timeout(Duration::from_secs(5), server.call_seen.recv())
        .await
        .expect("MCP tools/call must reach the loopback server")
        .expect("MCP call observation channel must remain open");
    assert_eq!(
        store
            .load_tool_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .status(),
        ToolInvocationStatus::Executing,
        "the durable start must commit before MCP request I/O"
    );
    server.release_call.notify_one();

    let outcome = executing_task.await.unwrap().unwrap();
    assert!(matches!(
        outcome,
        ToolAttemptOutcome::Dispatched {
            terminal: ToolAttemptTerminalKind::Error,
            ..
        }
    ));
    let unknown = store
        .load_tool_invocation(&tenant_id, run_id, invocation_id)
        .await
        .unwrap();
    assert_eq!(unknown.status(), ToolInvocationStatus::Unknown);
    assert_eq!(server.tool_calls.load(Ordering::SeqCst), 1);

    assert!(matches!(
        executor.execute_tool(handoff.clone()).await.unwrap(),
        ToolAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(server.tool_calls.load(Ordering::SeqCst), 1);

    let result = ToolResult::new(
        ToolResultProvenance::new(
            invocation_id,
            unknown.attempt_id().unwrap(),
            descriptor.metadata().identity().clone(),
        ),
        descriptor.output_schema().clone(),
        BoundedJson::try_from_value(json!({"status": "applied"})).unwrap(),
        ToolArtifacts::empty(),
    );
    let reconciliation = ToolReconciliationHandoff::result(
        lease.fence().clone(),
        unknown,
        EventId::generate(),
        result,
    )
    .unwrap();
    let committed = executor
        .commit_tool_reconciliation(reconciliation.clone())
        .await
        .unwrap();
    assert!(matches!(
        committed,
        ToolReconciliationOutcome::Committed { .. }
    ));
    assert_eq!(committed.kind(), ToolReconciliationKind::Result);
    assert_eq!(
        committed.invocation().status(),
        ToolInvocationStatus::Committed
    );
    assert_eq!(
        committed.event().payload().kind().as_str(),
        "tool-reconciliation-result-committed"
    );
    assert!(
        executor
            .commit_tool_reconciliation(reconciliation)
            .await
            .unwrap()
            .is_idempotent()
    );
    assert!(matches!(
        executor.execute_tool(handoff).await.unwrap(),
        ToolAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(server.tool_calls.load(Ordering::SeqCst), 1);

    server.task.await.unwrap();
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn a2a_send_is_durable_before_dispatch_and_unknown_is_not_redispatched() {
    let Some(store) = test_store().await else {
        return;
    };
    let (schemas, input_schema, _, output_schema, _) = tool_schemas();
    let descriptor = a2a_write_descriptor(&input_schema, &output_schema);
    let mut server = PausedLostResponseA2aServer::start().await;
    let client = A2aClient::discover(
        server.card_endpoint.clone(),
        vec![server.interface_pin.clone()],
        A2aAgentCardTrust::CanonicalSha256(a2a_agent_card_digest(&server.card).unwrap()),
        A2aClientSecurity::Anonymous,
        Vec::new(),
        A2aClientOptions::default(),
    )
    .await
    .unwrap();
    let adapter = A2aRemoteAgent::bind(
        descriptor.clone(),
        client,
        "write_once",
        A2aRemoteAgentDelivery::AtMostOnce,
        Arc::new(schemas.clone()),
    )
    .unwrap();

    let graph = graph();
    let tenant_id = TenantId::new(format!("a2a-durable-{}", RunId::generate())).unwrap();
    let run_id = RunId::generate();
    let checkpoint = start_run(&store, &graph, tenant_id.clone(), run_id).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let invocation_id = InvocationId::generate();
    let prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                checkpoint.event().head(),
                lease.fence().clone(),
            ),
            ToolInvocationIntent::new(
                invocation_activation(checkpoint.checkpoint()),
                invocation_id,
                descriptor.clone(),
                ToolInput::new(
                    input_schema,
                    BoundedJson::try_from_value(json!({"request": "apply-once"})).unwrap(),
                )
                .unwrap(),
                descriptor.limits().clone(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let mut tools = ToolProviderRegistryBuilder::new();
    tools.register(Arc::new(adapter)).unwrap();
    let executor = DurableInvocationExecutor::with_clock(
        store.clone(),
        schemas,
        ModelProviderRegistryBuilder::new().build(),
        tools.build(),
        Arc::new(StaticInvocationBudget {
            resolved: invocation_budget(),
        }),
        Arc::new(FixedInvocationClock),
        DurableInvocationExecutorOptions::default(),
    )
    .unwrap();
    let handoff = ToolAttemptHandoff::new(
        lease.fence().clone(),
        prepared.invocation().clone(),
        AttemptId::generate(),
        InvocationAttemptEventIds::generate(),
        CancellationSignal::never(),
        None,
    )
    .unwrap();
    let executing_task = {
        let executor = executor.clone();
        let handoff = handoff.clone();
        tokio::spawn(async move { executor.execute_tool(handoff).await })
    };

    tokio::time::timeout(Duration::from_secs(5), server.call_seen.recv())
        .await
        .expect("A2A message/send must reach the loopback server")
        .expect("A2A call observation channel must remain open");
    assert_eq!(
        store
            .load_tool_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .status(),
        ToolInvocationStatus::Executing,
        "the durable start must commit before A2A request I/O"
    );
    server.release_call.notify_one();

    let outcome = executing_task.await.unwrap().unwrap();
    assert!(matches!(
        outcome,
        ToolAttemptOutcome::Dispatched {
            terminal: ToolAttemptTerminalKind::Error,
            ..
        }
    ));
    assert_eq!(
        store
            .load_tool_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .status(),
        ToolInvocationStatus::Unknown
    );
    assert_eq!(server.message_sends.load(Ordering::SeqCst), 1);
    assert!(matches!(
        executor.execute_tool(handoff).await.unwrap(),
        ToolAttemptOutcome::Recovered { .. }
    ));
    assert_eq!(server.message_sends.load(Ordering::SeqCst), 1);

    server.task.await.unwrap();
    store.close().await;
}

fn tool_schemas() -> (
    JsonSchemaRegistry,
    SchemaReference,
    Value,
    SchemaReference,
    Value,
) {
    let input = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://schemas.stateknot.test/mcp-durable-input/1.0.0",
        "type": "object",
        "properties": {"request": {"type": "string", "maxLength": 64}},
        "required": ["request"],
        "additionalProperties": false
    });
    let output = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://schemas.stateknot.test/mcp-durable-output/1.0.0",
        "type": "object",
        "properties": {"status": {"const": "applied"}},
        "required": ["status"],
        "additionalProperties": false
    });
    let input_reference = schema_reference(&input);
    let output_reference = schema_reference(&output);
    let mut builder = JsonSchemaRegistryBuilder::with_default_limits();
    register_standard_invocation_execution_event_schema(&mut builder).unwrap();
    builder
        .register(input_reference.clone(), input.clone())
        .unwrap();
    builder
        .register(output_reference.clone(), output.clone())
        .unwrap();
    (
        builder.build().unwrap(),
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

fn write_descriptor(input: &SchemaReference, output: &SchemaReference) -> ToolDescriptor {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-tool-v1.json"
    ))
    .unwrap();
    let mut value = fixture["descriptors"]["valid"][0].clone();
    value["input_schema"] = serde_json::to_value(input).unwrap();
    value["output_schema"] = serde_json::to_value(output).unwrap();
    value["semantics"] = json!({
        "risk": "idempotent_write",
        "idempotency": "intrinsic",
        "status_query": true,
        "compensation": false
    });
    value["resources"] = json!({
        "network": "read_write",
        "filesystem": "none",
        "credentials": false,
        "dynamic_code": false
    });
    value["invocation"] = json!({
        "cancellation": "cooperative",
        "max_progress_events": "0"
    });
    serde_json::from_value(value).unwrap()
}

fn a2a_write_descriptor(input: &SchemaReference, output: &SchemaReference) -> ToolDescriptor {
    let mut value = serde_json::to_value(write_descriptor(input, output)).unwrap();
    value["semantics"] = json!({
        "risk": "non_idempotent_write",
        "idempotency": "unsupported",
        "status_query": false,
        "compensation": false
    });
    serde_json::from_value(value).unwrap()
}

fn graph() -> CompiledGraph {
    let (input_schema, _) = generic_schema("mcp-durable-graph-input");
    let (state_schema, _) = generic_schema("mcp-durable-graph-state");
    let (update_schema, _) = generic_schema("mcp-durable-graph-update");
    let (output_schema, _) = generic_schema("mcp-durable-graph-output");
    let node_id = NodeId::new("McpWrite").unwrap();
    CompiledGraph::compile(
        capability("mcp-durable-graph"),
        input_schema,
        state_schema,
        update_schema,
        output_schema,
        GraphReducerReference::new(
            capability("mcp-durable-reducer"),
            Digest::sha256(b"stateknot mcp durable integration reducer v1"),
        ),
        ReadyNodes::try_new([node_id.clone()]).unwrap(),
        [GraphNode::new(node_id, None, GraphRoutes::empty(), None, true).unwrap()],
        GraphExecutionLimits::new(Superstep::new(4).unwrap(), 1).unwrap(),
    )
    .unwrap()
}

fn generic_schema(name: &str) -> (SchemaReference, Value) {
    let id = format!("https://schemas.stateknot.test/{name}/1.0.0");
    let document = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": id,
        "type": "object"
    });
    let reference = schema_reference(&document);
    (reference, document)
}

fn capability(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.com/stateknot"
                .parse::<IssuerId>()
                .unwrap(),
            "mcp-durable-tests".parse::<SubjectId>().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}

fn invocation_budget() -> ResolvedBudget {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-budget-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["resolved"]["valid"][0].clone()).unwrap()
}

fn invocation_activation(checkpoint: &Checkpoint) -> NodeActivation {
    let node_id = checkpoint.ready_nodes().iter().next().unwrap().clone();
    NodeActivation::for_ready_root(checkpoint, node_id).unwrap()
}

async fn test_store() -> Option<PostgresStore> {
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let options = PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, 8)
        .with_acquire_timeout(Duration::from_secs(30))
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20));
    PostgresStore::migrate_database(&database_url, options.clone())
        .await
        .expect("migrations must succeed");
    Some(
        PostgresStore::connect(&database_url, options)
            .await
            .expect("test PostgreSQL must connect with an exact schema"),
    )
}

async fn start_run(
    store: &PostgresStore,
    graph: &CompiledGraph,
    tenant_id: TenantId,
    run_id: RunId,
) -> CheckpointCommitOutcome {
    let provenance = AgentResultProvenance::new(
        tenant_id.clone(),
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        capability("mcp-durable-agent"),
    );
    let registered = store
        .register_graph_definition(tenant_id.clone(), graph.clone())
        .await
        .unwrap();
    assert!(matches!(
        registered,
        GraphDefinitionRegistrationOutcome::Registered(_)
            | GraphDefinitionRegistrationOutcome::Idempotent(_)
    ));
    let admitted = store.admit_run(provenance).await.unwrap();
    let state = CheckpointState::new(
        graph.state_schema().clone(),
        BoundedJson::try_from_value(json!({"step": "initial"})).unwrap(),
    )
    .unwrap();
    let write = CheckpointWrite::initial(
        tenant_id.clone(),
        run_id,
        CheckpointId::generate(),
        graph.reference(),
        state,
        graph.entry_nodes().clone(),
    )
    .unwrap();
    store
        .append_control_plane_checkpoint(
            control_append(tenant_id, run_id, EventId::generate()),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
            write,
        )
        .await
        .unwrap()
}

fn test_payload() -> JournalPayload {
    JournalPayload::new(
        SchemaReference::new(
            "https://schemas.stateknot.test/mcp-durable-control/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"stateknot mcp durable control schema"),
        ),
        JournalEventKind::new("mcp-durable-integration").unwrap(),
        BoundedJson::try_from_value(json!({"test": true})).unwrap(),
    )
    .unwrap()
}

fn control_append(tenant_id: TenantId, run_id: RunId, event_id: EventId) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::empty(),
        JournalEventIntent::control_plane(tenant_id, run_id, event_id, test_payload()).unwrap(),
    )
    .unwrap()
}

fn worker_append(
    tenant_id: TenantId,
    run_id: RunId,
    event_id: EventId,
    head: stateknot_core::JournalHead,
    fence: stateknot_core::RunFence,
) -> JournalAppend {
    JournalAppend::new(
        JournalExpectation::exact(head),
        JournalEventIntent::worker(tenant_id, run_id, event_id, fence, test_payload()).unwrap(),
    )
    .unwrap()
}
