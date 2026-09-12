// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Hostile peer and real durable Driver qualification for the compute boundary.
#![allow(
    clippy::wildcard_imports,
    clippy::too_many_lines,
    clippy::similar_names
)]

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use stateknot::{core::*, integrations::*, postgres::*, runtime::*, *};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn capability(name: &str) -> CapabilityIdentity {
    CapabilityIdentity::new(
        PrincipalIdentity::new(
            "https://issuer.example.test/compute".parse().unwrap(),
            "compute-tests".parse().unwrap(),
        ),
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}

fn value_schema() -> Value {
    json!({"$schema":"https://json-schema.org/draft/2020-12/schema", "$id":"https://stknot.com/schemas/tests/compute-value/1.0.0",
        "type":"object", "properties":{"value":{"type":"integer","minimum":0,"maximum":10000}}, "required":["value"], "additionalProperties":false})
}

fn reference(document: &Value) -> SchemaReference {
    SchemaReference::new(
        document["$id"].as_str().unwrap().parse().unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(serde_json_canonicalizer::to_vec(document).unwrap()),
    )
}

fn descriptor() -> Value {
    json!({"name":"double", "inputSchema":value_schema(), "outputSchema":value_schema()})
}

fn fixture() -> (CompiledGraph, JsonSchemaRegistry) {
    let value = value_schema();
    let state = json!({"$schema":"https://json-schema.org/draft/2020-12/schema", "$id":"https://stknot.com/schemas/tests/compute-state/1.0.0",
        "type":"object", "properties":{"value":{"type":"integer"}, "secret":{"type":"string"}}, "required":["value","secret"], "additionalProperties":false});
    let graph = CompiledGraph::compile(
        capability("compute-graph"),
        reference(&value),
        reference(&state),
        reference(&value),
        reference(&value),
        GraphReducerReference::new(
            capability("compute-reducer"),
            Digest::sha256(b"compute-reducer-v1"),
        ),
        ReadyNodes::try_new([NodeId::new("double").unwrap()]).unwrap(),
        [
            GraphNode::new(
                NodeId::new("double").unwrap(),
                Some(ReadyNodes::try_new([NodeId::new("finish").unwrap()]).unwrap()),
                GraphRoutes::empty(),
                None,
                false,
            )
            .unwrap(),
            GraphNode::new(
                NodeId::new("finish").unwrap(),
                None,
                GraphRoutes::empty(),
                None,
                true,
            )
            .unwrap(),
        ],
        GraphExecutionLimits::new(Superstep::new(8).unwrap(), 1).unwrap(),
    )
    .unwrap();
    let mut schemas = JsonSchemaRegistryBuilder::with_default_limits();
    schemas.register(reference(&value), value).unwrap();
    schemas.register(reference(&state), state).unwrap();
    register_standard_graph_driver_event_schema(&mut schemas).unwrap();
    (graph, schemas.build().unwrap())
}

fn binding(graph: &CompiledGraph, node: &str) -> McpComputeNodeBinding {
    McpComputeNodeBinding::new(
        graph,
        NodeId::new(node).unwrap(),
        graph.input_schema().clone(),
        WorkerInputProjection::new(["value"]).unwrap(),
        if node == "double" {
            McpComputeOutput::UpdateAndContinue
        } else {
            McpComputeOutput::Terminal
        },
        McpServerIdentity::new("compute-worker", "1.0.0").unwrap(),
        "double",
        mcp_compute_tool_digest(&descriptor()).unwrap(),
    )
    .unwrap()
}

fn context(
    graph: &CompiledGraph,
    node: &str,
    value: Value,
    cancellation: CancellationSignal,
) -> GraphNodeContext {
    let tenant = TenantId::new("compute-private-tenant").unwrap();
    let run = RunId::generate();
    let journal = |sequence| {
        JournalHead::new(
            tenant.clone(),
            run,
            JournalSequence::new(sequence).unwrap(),
            EventId::generate(),
            Timestamp::from_unix_micros(1_000_000).unwrap(),
            Digest::sha256(b"test-only-context"),
        )
    };
    let checkpoint = Checkpoint::commit(
        CheckpointWrite::initial(
            tenant.clone(),
            run,
            CheckpointId::generate(),
            graph.reference(),
            CheckpointState::new(
                graph.state_schema().clone(),
                BoundedJson::try_from_value(value).unwrap(),
            )
            .unwrap(),
            ReadyNodes::try_new([NodeId::new(node).unwrap()]).unwrap(),
        )
        .unwrap(),
        journal(1),
    )
    .unwrap();
    let activation = NodeActivation::new(
        checkpoint.head(),
        GraphNamespace::root(),
        NodeId::new(node).unwrap(),
        Digest::sha256(b"input"),
    );
    let start = NodeAttemptStart::new(
        activation,
        AttemptId::generate(),
        RunFence::new(
            tenant.clone(),
            run,
            AttemptId::generate(),
            FencingEpoch::FIRST,
        ),
        journal(2),
    )
    .unwrap();
    GraphNodeContext::new(start.head(), Arc::new(checkpoint), cancellation).unwrap()
}

#[derive(Clone, Default)]
struct Peer {
    requests: Arc<Mutex<Vec<Value>>>,
    mode: Arc<AtomicUsize>,
    catalog: Arc<Mutex<Option<Value>>>,
    gate: Arc<tokio::sync::Notify>,
    redirected: Arc<AtomicUsize>,
}

async fn rpc(State(peer): State<Peer>, headers: HeaderMap, Json(request): Json<Value>) -> Response {
    assert_eq!(
        headers["authorization"],
        "Bearer compute-only-fixture-token"
    );
    peer.requests.lock().unwrap().push(request.clone());
    let id = request["id"].clone();
    let result = match request["method"].as_str().unwrap() {
        "server/discover" => {
            json!({"supportedVersions":["2026-07-28"], "capabilities":{"tools":{}}, "serverInfo":{"name":"compute-worker","version":"1.0.0"}})
        }
        "tools/list" => peer
            .catalog
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| json!({"tools":[descriptor()]})),
        "tools/call" => {
            assert_eq!(request["params"]["name"], "double");
            let value = request["params"]["arguments"]["value"].as_u64().unwrap() * 2;
            match peer.mode.load(Ordering::SeqCst) {
                1 => json!({"content":[],"structuredContent":{"value":value},"usage":{"tokens":0},"fence":"forged"}),
                2 => json!({"content":[],"structuredContent":{"value":value,"control":"cancel-other-run"}}),
                3 => json!({"content":[{"type":"text","text":"private-worker-secret"}],"structuredContent":{"value":value}}),
                4 => json!({"resultType":"input_required","inputRequests":{"x":{"method":"sampling/createMessage","params":{}}},"requestState":"private-worker-secret"}),
                5 => return Json(json!({"jsonrpc":"2.0","id":id,"error":{"code":-32022,"message":"private-worker-secret","data":{"supported":["2026-07-28"]}}})).into_response(),
                6 => return (StatusCode::UNAUTHORIZED, [("www-authenticate", "Bearer error=\"invalid_token\"")], "private-worker-secret").into_response(),
                7 => return (StatusCode::TEMPORARY_REDIRECT, [("location", "/redirect-must-not-run")]).into_response(),
                8 => {tokio::time::sleep(Duration::from_secs(30)).await; json!({"content":[],"structuredContent":{"value":value}})},
                9 => return Json(json!({"jsonrpc":"2.0","id":"wrong-id","result":{"content":[],"structuredContent":{"value":value}}})).into_response(),
                10 => json!({"content":[],"structuredContent":{"value":"x".repeat(2 * 1024 * 1024 + 1)}}),
                11 => json!({"content":[],"structuredContent":{"value":value},"isError":true}),
                12 => {peer.gate.notified().await; json!({"content":[],"structuredContent":{"value":value}})},
                _ => json!({"resultType":"complete","content":[],"structuredContent":{"value":value}}),
            }
        }
        other => panic!("unexpected method {other}"),
    };
    Json(json!({"jsonrpc":"2.0","id":id,"result":result})).into_response()
}

struct Server {
    peer: Peer,
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

async fn redirected(State(peer): State<Peer>) -> StatusCode {
    peer.redirected.fetch_add(1, Ordering::SeqCst);
    StatusCode::NOT_FOUND
}

impl Server {
    async fn start() -> Self {
        let peer = Peer::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/", post(rpc))
            .fallback(redirected)
            .with_state(peer.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            peer,
            address,
            task,
        }
    }
    fn calls(&self) -> usize {
        self.peer
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["method"] == "tools/call")
            .count()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn client(address: std::net::SocketAddr) -> McpClient {
    McpClient::connect(
        ProviderEndpoint::loopback_http(&format!("http://{address}")).unwrap(),
        McpClientIdentity::new("compute-control-plane", "1.0.0").unwrap(),
        Arc::new(StaticMcpBearerAuthorization::new(
            ApiKey::new("compute-only-fixture-token").unwrap(),
        )),
        McpClientOptions::default(),
    )
    .await
    .unwrap()
}

#[test]
fn projection_is_finite_explicit_and_not_a_whole_state_escape() {
    assert!(WorkerInputProjection::new(["value", "value"]).is_err());
    assert!(WorkerInputProjection::new([""]).is_err());
    assert!(WorkerInputProjection::new(["\n"]).is_err());
    assert!(WorkerInputProjection::new(["x".repeat(257)]).is_err());
    assert!(WorkerInputProjection::new((0..).map(|n| n.to_string())).is_err());
    assert_eq!(
        WorkerInputProjection::new(Vec::<String>::new())
            .unwrap()
            .fields()
            .len(),
        0
    );
    assert_eq!(
        WorkerInputProjection::new(["z", "a"])
            .unwrap()
            .fields()
            .collect::<Vec<_>>(),
        ["a", "z"]
    );
}

#[tokio::test]
async fn projects_only_authorized_data_and_host_owns_control_and_usage() {
    let server = Server::start().await;
    let (graph, schemas) = fixture();
    let node = McpComputeNode::connect(
        client(server.address).await,
        binding(&graph, "double"),
        schemas.clone(),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let context = context(
        &graph,
        "double",
        json!({"value":7,"secret":"database-password-sentinel"}),
        CancellationSignal::never(),
    );
    let execution = node.execute(context.clone()).await.unwrap();
    let GraphNodeExecution::Completed {
        state_change,
        control,
        bindings,
        usage,
    } = execution
    else {
        panic!("no Join authority")
    };
    assert_eq!(
        state_change.update().unwrap().data().as_value(),
        &json!({"value":14})
    );
    assert!(matches!(control, NodeControl::Continue));
    assert!(bindings.is_empty());
    assert_eq!(usage, BudgetUsage::zero());
    let requests = server.peer.requests.lock().unwrap().clone();
    let call = requests
        .iter()
        .find(|request| request["method"] == "tools/call")
        .unwrap();
    assert_eq!(call["params"]["arguments"], json!({"value":7}));
    let wire = serde_json::to_string(&requests).unwrap();
    for excluded in [
        "database-password-sentinel",
        "compute-private-tenant",
        &context.attempt().fence().attempt_id().to_string(),
        &context.checkpoint().run_id().to_string(),
    ] {
        assert!(!wire.contains(excluded));
    }
    assert!(!format!("{node:?}").contains("compute-only-fixture-token"));
    assert_eq!(node.scheduling(), GraphNodeScheduling::JournalIsolated);
    assert!(!node.supports_child_join());

    let missing = context_with_value(&graph, json!({"secret":"hidden"}));
    assert!(node.execute(missing).await.is_err());
    let wrong_node = self::context(
        &graph,
        "finish",
        json!({"value":7,"secret":"hidden"}),
        CancellationSignal::never(),
    );
    assert!(node.execute(wrong_node).await.is_err());
    assert_eq!(server.calls(), 1);
}

fn context_with_value(graph: &CompiledGraph, value: Value) -> GraphNodeContext {
    context(graph, "double", value, CancellationSignal::never())
}

#[tokio::test]
async fn hostile_results_challenges_redirects_and_timeouts_never_replay_or_leak() {
    let server = Server::start().await;
    let (graph, schemas) = fixture();
    let node = McpComputeNode::connect(
        client(server.address).await,
        binding(&graph, "double"),
        schemas,
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    for mode in 1..=11 {
        server.peer.mode.store(mode, Ordering::SeqCst);
        let before = server.calls();
        let error = node
            .execute(context_with_value(
                &graph,
                json!({"value":7,"secret":"hidden"}),
            ))
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains("private-worker-secret"));
        assert_eq!(error.failure().retry_advice(), RetryAdvice::Never);
        assert_eq!(
            server.calls(),
            before + 1,
            "mode {mode} must make exactly one call"
        );
    }
    assert_eq!(server.peer.redirected.load(Ordering::SeqCst), 0);
}

struct RetryAuth(Arc<AtomicUsize>);
impl McpClientAuthorizationProvider for RetryAuth {
    fn resolve(
        &self,
        _: &McpClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        Box::pin(async {
            Ok(McpAuthorization::Bearer(
                ApiKey::new("compute-only-fixture-token").unwrap(),
            ))
        })
    }
    fn handle_challenge<'a>(
        &'a self,
        _: &'a McpClientAuthorizationRequest,
        _: &'a McpClientAuthorizationChallenge,
    ) -> BoxFuture<'a, Result<McpClientAuthorizationRetry, McpAuthorizationError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(McpClientAuthorizationRetry::Retry) })
    }
}

#[tokio::test]
async fn once_disables_real_auth_and_version_recovery_without_changing_general_client() {
    let server = Server::start().await;
    let callbacks = Arc::new(AtomicUsize::new(0));
    let client = McpClient::connect(
        ProviderEndpoint::loopback_http(&format!("http://{}", server.address)).unwrap(),
        McpClientIdentity::new("compute-control-plane", "1.0.0").unwrap(),
        Arc::new(RetryAuth(callbacks.clone())),
        McpClientOptions::default(),
    )
    .await
    .unwrap();
    let catalog = client.list_tools().await.unwrap();
    let tool = catalog.find("double").unwrap();
    for mode in [5, 6] {
        server.peer.mode.store(mode, Ordering::SeqCst);
        let before = server.calls();
        assert!(
            client
                .call_tool_once(tool, json!({"value":7}))
                .await
                .is_err()
        );
        assert_eq!(server.calls(), before + 1);
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
        assert!(client.call_tool(tool, json!({"value":7})).await.is_err());
        assert_eq!(
            server.calls(),
            before + 3,
            "general client retains its bounded retry"
        );
    }
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn startup_rejects_drift_extensions_missing_schemas_and_invalid_binding() {
    let server = Server::start().await;
    let (graph, schemas) = fixture();
    let client = client(server.address).await;
    assert!(
        McpComputeNode::connect(
            client.clone(),
            binding(&graph, "double"),
            schemas.clone(),
            Duration::ZERO
        )
        .await
        .is_err()
    );
    let mut changed = descriptor();
    changed["description"] = json!("changed deployment");
    *server.peer.catalog.lock().unwrap() = Some(json!({"tools":[changed]}));
    assert_eq!(
        McpComputeNode::connect(
            client.clone(),
            binding(&graph, "double"),
            schemas.clone(),
            Duration::from_secs(3)
        )
        .await
        .unwrap_err(),
        McpComputeNodeBuildError::Descriptor
    );
    for field in ["header", "task"] {
        let mut changed = descriptor();
        if field == "header" {
            changed["inputSchema"]["properties"]["value"]["x-mcp-header"] = json!("Authorization");
        } else {
            changed["execution"] = json!({"taskSupport":"required"});
        }
        let custom = McpComputeNodeBinding::new(
            &graph,
            NodeId::new("double").unwrap(),
            graph.input_schema().clone(),
            WorkerInputProjection::new(["value"]).unwrap(),
            McpComputeOutput::UpdateAndContinue,
            McpServerIdentity::new("compute-worker", "1.0.0").unwrap(),
            "double",
            mcp_compute_tool_digest(&changed).unwrap(),
        )
        .unwrap();
        *server.peer.catalog.lock().unwrap() = Some(json!({"tools":[changed]}));
        assert!(
            McpComputeNode::connect(
                client.clone(),
                custom,
                schemas.clone(),
                Duration::from_secs(3)
            )
            .await
            .is_err()
        );
    }
    *server.peer.catalog.lock().unwrap() = None;
    let mut unrelated = JsonSchemaRegistryBuilder::with_default_limits();
    register_standard_graph_driver_event_schema(&mut unrelated).unwrap();
    let empty = unrelated.build().unwrap();
    assert_eq!(
        McpComputeNode::connect(
            client.clone(),
            binding(&graph, "double"),
            empty,
            Duration::from_secs(3)
        )
        .await
        .unwrap_err(),
        McpComputeNodeBuildError::Schema
    );
    assert!(
        McpComputeNodeBinding::new(
            &graph,
            NodeId::new("double").unwrap(),
            graph.input_schema().clone(),
            WorkerInputProjection::new(["value"]).unwrap(),
            McpComputeOutput::Terminal,
            McpServerIdentity::new("compute-worker", "1.0.0").unwrap(),
            "double",
            mcp_compute_tool_digest(&descriptor()).unwrap()
        )
        .is_err()
    );
    let wrong_identity = McpComputeNodeBinding::new(
        &graph,
        NodeId::new("double").unwrap(),
        graph.input_schema().clone(),
        WorkerInputProjection::new(["value"]).unwrap(),
        McpComputeOutput::UpdateAndContinue,
        McpServerIdentity::new("compute-worker", "2.0.0").unwrap(),
        "double",
        mcp_compute_tool_digest(&descriptor()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        McpComputeNode::connect(client, wrong_identity, schemas, Duration::from_secs(3))
            .await
            .unwrap_err(),
        McpComputeNodeBuildError::Identity
    );
    assert_eq!(server.calls(), 0);
}

#[derive(Clone)]
struct Stop(Arc<tokio::sync::Notify>, Arc<std::sync::atomic::AtomicBool>);
impl CancellationObserver for Stop {
    fn is_cancelled(&self) -> bool {
        self.1.load(Ordering::SeqCst)
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let notified = self.0.notified();
            if !self.is_cancelled() {
                notified.await;
            }
        })
    }
}

#[tokio::test]
async fn cancellation_stops_before_dispatch_and_during_wait_without_retry() {
    let server = Server::start().await;
    let (graph, schemas) = fixture();
    let node = McpComputeNode::connect(
        client(server.address).await,
        binding(&graph, "double"),
        schemas,
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let stop = Stop(
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(std::sync::atomic::AtomicBool::new(true)),
    );
    assert!(
        node.execute(context(
            &graph,
            "double",
            json!({"value":7,"secret":"hidden"}),
            CancellationSignal::new(stop.clone())
        ))
        .await
        .is_err()
    );
    assert_eq!(server.calls(), 0);
    stop.1.store(false, Ordering::SeqCst);
    server.peer.mode.store(8, Ordering::SeqCst);
    let future = node.execute(context(
        &graph,
        "double",
        json!({"value":7,"secret":"hidden"}),
        CancellationSignal::new(stop.clone()),
    ));
    let cancel = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.calls() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        stop.1.store(true, Ordering::SeqCst);
        stop.0.notify_one();
    };
    let (result, ()) = tokio::join!(future, cancel);
    assert_eq!(
        result.unwrap_err().failure().code().as_str(),
        "worker.cancelled"
    );
    assert_eq!(server.calls(), 1);
}

#[path = "mcp_compute/postgres.rs"]
mod postgres;
