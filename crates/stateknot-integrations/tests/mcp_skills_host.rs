// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end security contract for the static MCP Skills client and Host.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use serde_json::{Value, json};
use stateknot_core::{
    AttemptId, BoundedJson, BoxFuture, BudgetUsage, CancellationSignal, CapabilityIdentity, Digest,
    DurationMillis, ErasedTool, EventId, FailureCategory, InvocationId, ResolvedBudget, RunId,
    TenantId, ThreadId, Timestamp, ToolArtifacts, ToolContext, ToolDescriptor, ToolError,
    ToolExternalEffect, ToolInput, ToolReconciliationContext, ToolReconciliationObservation,
    ToolReconciliationProbeError, ToolResult,
};
use stateknot_integrations::{
    AnonymousMcpAuthorization, MCP_SKILLS_EXTENSION_ID, McpActivatedSkill, McpClient,
    McpClientIdentity, McpClientOptions, McpSkillActivationRequest, McpSkillActivationSource,
    McpSkillBoundTool, McpSkillHost, McpSkillHostCodeExecution, McpSkillHostError,
    McpSkillHostOptions, McpSkillHostPolicy, McpSkillHostPolicyError, McpSkillOrigin,
    McpSkillToolAuthorizationRequest, McpSkillToolInvocation, McpSkillToolOperation,
    ProviderEndpoint,
};
use stateknot_runtime::ToolProviderRegistryBuilder;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

const ROOT_URI: &str = "skill://root-skill/SKILL.md";
const REFERENCE_URI: &str = "skill://root-skill/references/checklist.md";
const NESTED_URI: &str = "skill://root-skill/nested/child/SKILL.md";
const ROOT_SKILL: &[u8] = b"---\nname: root-skill\ndescription: Root skill.\nallowed-tools: deploy\n---\nUse the checklist.\n";
const REFERENCE: &[u8] = b"# Checklist\n\nVerify before deployment.\n";
const NESTED_SKILL: &[u8] =
    b"---\nname: child\ndescription: Child skill.\n---\nPerform the nested check.\n";
const RUN_ID: &str = "01912345-6789-7abc-8def-0123456789ab";
const THREAD_ID: &str = "01912345-6789-7abc-8def-0123456789ac";
const INVOCATION_ID: &str = "01912345-6789-7abc-8def-0123456789ad";
const ATTEMPT_ID: &str = "01912345-6789-7abc-8def-0123456789ae";
const ORIGIN_EVENT_ID: &str = "01912345-6789-7abc-8def-0123456789af";

#[derive(Debug)]
struct CapturedRequest {
    headers: String,
    body: Value,
}

async fn start_server(results: Vec<Value>) -> (ProviderEndpoint, JoinHandle<Vec<CapturedRequest>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = ProviderEndpoint::loopback_http(&format!("http://{address}/mcp/")).unwrap();
    let server = tokio::spawn(async move {
        let mut requests = Vec::with_capacity(results.len());
        for result in results {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            let header_end = find_bytes(&request, b"\r\n\r\n").unwrap();
            let body: Value = serde_json::from_slice(&request[header_end + 4..]).unwrap();
            let response = json!({
                "jsonrpc": "2.0",
                "id": body["id"],
                "result": result,
            });
            write_json(&mut socket, &response).await;
            socket.shutdown().await.unwrap();
            requests.push(CapturedRequest {
                headers: String::from_utf8_lossy(&request[..header_end]).into_owned(),
                body,
            });
        }
        requests
    });
    (endpoint, server)
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

async fn write_json(socket: &mut tokio::net::TcpStream, value: &Value) {
    let encoded = serde_json::to_vec(value).unwrap();
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        encoded.len()
    );
    socket.write_all(headers.as_bytes()).await.unwrap();
    socket.write_all(&encoded).await.unwrap();
}

fn discovery() -> Value {
    json!({
        "resultType": "complete",
        "supportedVersions": ["2026-07-28"],
        "capabilities": {
            "resources": {},
            "extensions": {(MCP_SKILLS_EXTENSION_ID): {"directoryRead": false}}
        },
        "ttlMs": 0,
        "cacheScope": "private",
        "_meta": {
            "io.modelcontextprotocol/serverInfo": {
                "name": "untrusted-self-reported-name",
                "version": "1.0.0"
            }
        }
    })
}

fn resource(uri: &str, bytes: &[u8]) -> Value {
    json!({
        "uri": uri,
        "digest": Digest::sha256(bytes).to_string(),
        "size": bytes.len(),
    })
}

fn root_entry() -> Value {
    json!({
        "uri": ROOT_URI,
        "frontmatter": {
            "name": "root-skill",
            "description": "Root skill.",
            "allowed-tools": "deploy"
        },
        "resources": [
            resource(ROOT_URI, ROOT_SKILL),
            resource(REFERENCE_URI, REFERENCE),
            resource(NESTED_URI, NESTED_SKILL)
        ]
    })
}

fn nested_entry() -> Value {
    json!({
        "uri": NESTED_URI,
        "frontmatter": {"name": "child", "description": "Child skill."},
        "resources": [resource(NESTED_URI, NESTED_SKILL)]
    })
}

fn list_result(skills: &[Value], next_cursor: Option<&str>) -> Value {
    let mut value = json!({
        "resultType": "complete",
        "skills": skills,
        "ttlMs": 60_000,
        "cacheScope": "private"
    });
    if let Some(cursor) = next_cursor {
        value["nextCursor"] = json!(cursor);
    }
    value
}

fn get_result(skill: &Value) -> Value {
    json!({
        "resultType": "complete",
        "skill": skill,
        "ttlMs": 60_000,
        "cacheScope": "private"
    })
}

fn text_resource(uri: &str, bytes: &[u8]) -> Value {
    json!({
        "resultType": "complete",
        "contents": [{
            "uri": uri,
            "mimeType": "text/markdown; charset=utf-8",
            "text": std::str::from_utf8(bytes).unwrap()
        }]
    })
}

async fn connect(endpoint: ProviderEndpoint, options: McpClientOptions) -> McpClient {
    McpClient::connect(
        endpoint,
        McpClientIdentity::new("stateknot-skills-test", "1.0.0").unwrap(),
        Arc::new(AnonymousMcpAuthorization),
        options,
    )
    .await
    .unwrap()
}

#[derive(Default)]
struct RecordingPolicy {
    activations: Mutex<Vec<(String, McpSkillActivationSource, String)>>,
    tool_calls: Mutex<Vec<RecordedToolCall>>,
}

#[derive(Debug)]
struct RecordedToolCall {
    skill_uri: String,
    tool_name: String,
    tool_identity: Option<CapabilityIdentity>,
    descriptor_digest: Option<Digest>,
    operation: McpSkillToolOperation,
    invocation: Option<McpSkillToolInvocation>,
    host_code_execution: bool,
    requested_allowed_tools: Option<String>,
}

impl McpSkillHostPolicy for RecordingPolicy {
    fn approve_activation(
        &self,
        request: McpSkillActivationRequest,
    ) -> BoxFuture<'_, Result<(), McpSkillHostPolicyError>> {
        self.activations.lock().unwrap().push((
            request.identity().origin().as_str().to_owned(),
            request.source().clone(),
            request.entry().manifest_digest().to_owned(),
        ));
        Box::pin(async { Ok(()) })
    }

    fn authorize_tool_call(
        &self,
        request: McpSkillToolAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpSkillHostPolicyError>> {
        self.tool_calls.lock().unwrap().push(RecordedToolCall {
            skill_uri: request.identity().uri().to_owned(),
            tool_name: request.tool_name().to_owned(),
            tool_identity: request.tool_identity().cloned(),
            descriptor_digest: request.tool_descriptor_digest(),
            operation: request.operation(),
            invocation: request.invocation().cloned(),
            host_code_execution: request.host_code_execution(),
            requested_allowed_tools: request.requested_allowed_tools().map(str::to_owned),
        });
        Box::pin(async { Ok(()) })
    }
}

struct DenyActivation;

impl McpSkillHostPolicy for DenyActivation {
    fn approve_activation(
        &self,
        _request: McpSkillActivationRequest,
    ) -> BoxFuture<'_, Result<(), McpSkillHostPolicyError>> {
        Box::pin(async { Err(McpSkillHostPolicyError::Denied) })
    }

    fn authorize_tool_call(
        &self,
        _request: McpSkillToolAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpSkillHostPolicyError>> {
        Box::pin(async { Err(McpSkillHostPolicyError::Denied) })
    }
}

struct DenyToolExecution;

impl McpSkillHostPolicy for DenyToolExecution {
    fn approve_activation(
        &self,
        _request: McpSkillActivationRequest,
    ) -> BoxFuture<'_, Result<(), McpSkillHostPolicyError>> {
        Box::pin(async { Ok(()) })
    }

    fn authorize_tool_call(
        &self,
        _request: McpSkillToolAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpSkillHostPolicyError>> {
        Box::pin(async { Err(McpSkillHostPolicyError::Denied) })
    }
}

struct CountingTool {
    descriptor: ToolDescriptor,
    calls: AtomicUsize,
    reconciliations: AtomicUsize,
}

impl CountingTool {
    fn new(descriptor: ToolDescriptor) -> Self {
        Self {
            descriptor,
            calls: AtomicUsize::new(0),
            reconciliations: AtomicUsize::new(0),
        }
    }
}

impl ErasedTool for CountingTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn supports_reconciliation(&self) -> bool {
        true
    }

    fn call(
        &self,
        context: ToolContext,
        _input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let result = ToolResult::for_invocation(
            &context,
            &self.descriptor,
            BoundedJson::try_from_value(json!({"status": "ok"})).unwrap(),
            ToolArtifacts::empty(),
        );
        Box::pin(async move { Ok(result) })
    }

    fn reconcile(
        &self,
        _context: ToolReconciliationContext,
        _input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolReconciliationObservation, ToolReconciliationProbeError>> {
        self.reconciliations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(
                ToolReconciliationObservation::pending(DurationMillis::new(1_000).unwrap())
                    .unwrap(),
            )
        })
    }
}

fn tool_descriptor() -> ToolDescriptor {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-tool-v1.json"
    ))
    .unwrap();
    let mut descriptor = fixture["descriptors"]["valid"][0].clone();
    descriptor["metadata"]["identity"]["capability"]["name"] = json!("deploy");
    serde_json::from_value(descriptor).unwrap()
}

fn resolved_budget() -> ResolvedBudget {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-budget-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["resolved"]["valid"][0].clone()).unwrap()
}

fn tool_context(descriptor: &ToolDescriptor) -> ToolContext {
    let observed_at = "2029-12-31T23:59:59.000000Z".parse::<Timestamp>().unwrap();
    ToolContext::new(
        TenantId::new("tenant-mcp-skills").unwrap(),
        RUN_ID.parse::<RunId>().unwrap(),
        THREAD_ID.parse::<ThreadId>().unwrap(),
        INVOCATION_ID.parse::<InvocationId>().unwrap(),
        ATTEMPT_ID.parse::<AttemptId>().unwrap(),
        descriptor,
        resolved_budget()
            .remaining(&BudgetUsage::zero(), observed_at)
            .unwrap(),
        DurationMillis::new(30_000).unwrap(),
        observed_at,
        Instant::now(),
        CancellationSignal::never(),
    )
    .unwrap()
    .with_durable_origin_event(ORIGIN_EVENT_ID.parse::<EventId>().unwrap())
}

fn reconciliation_context(descriptor: &ToolDescriptor) -> ToolReconciliationContext {
    let observed_at = "2029-12-31T23:59:59.000000Z".parse::<Timestamp>().unwrap();
    ToolReconciliationContext::new(
        TenantId::new("tenant-mcp-skills").unwrap(),
        RUN_ID.parse::<RunId>().unwrap(),
        THREAD_ID.parse::<ThreadId>().unwrap(),
        INVOCATION_ID.parse::<InvocationId>().unwrap(),
        ATTEMPT_ID.parse::<AttemptId>().unwrap(),
        descriptor,
        DurationMillis::new(30_000).unwrap(),
        observed_at,
        Instant::now(),
        "2030-01-01T00:00:00.000000Z".parse().unwrap(),
        CancellationSignal::never(),
    )
    .unwrap()
    .with_durable_recovery(ORIGIN_EVENT_ID.parse::<EventId>().unwrap(), None)
}

fn tool_input(descriptor: &ToolDescriptor) -> ToolInput {
    ToolInput::new(
        descriptor.input_schema().clone(),
        BoundedJson::try_from_value(json!({"amount_minor": 42})).unwrap(),
    )
    .unwrap()
}

fn host(client: McpClient, policy: Arc<dyn McpSkillHostPolicy>) -> McpSkillHost {
    McpSkillHost::new(
        client,
        McpSkillOrigin::new("production-docs").unwrap(),
        policy,
        McpSkillHostOptions::default(),
    )
    .unwrap()
}

async fn exercise_bound_tool(
    active: Arc<McpActivatedSkill>,
) -> (ToolDescriptor, Arc<McpSkillBoundTool>) {
    let descriptor = tool_descriptor();
    let provider = Arc::new(CountingTool::new(descriptor.clone()));
    let guarded = Arc::new(
        McpSkillBoundTool::new(
            active,
            Arc::clone(&provider) as Arc<dyn ErasedTool>,
            McpSkillHostCodeExecution::Possible,
        )
        .unwrap(),
    );
    assert_eq!(
        guarded.binding().identity(),
        descriptor.metadata().identity()
    );
    let mut registry = ToolProviderRegistryBuilder::new();
    registry
        .register(Arc::clone(&guarded) as Arc<dyn ErasedTool>)
        .unwrap();
    let installed = registry.build().resolve(&descriptor).unwrap();
    installed
        .call(tool_context(&descriptor), tool_input(&descriptor))
        .await
        .unwrap();
    installed
        .reconcile(reconciliation_context(&descriptor), tool_input(&descriptor))
        .await
        .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.reconciliations.load(Ordering::SeqCst), 1);
    (descriptor, guarded)
}

fn assert_tool_policy_calls(
    policy: &RecordingPolicy,
    descriptor: &ToolDescriptor,
    guarded: &McpSkillBoundTool,
) {
    let tool_calls = policy.tool_calls.lock().unwrap();
    assert_eq!(tool_calls.len(), 3);
    assert_eq!(tool_calls[0].skill_uri, ROOT_URI);
    assert_eq!(tool_calls[0].tool_name, "deploy");
    assert!(tool_calls[0].tool_identity.is_none());
    assert!(tool_calls[0].descriptor_digest.is_none());
    assert_eq!(tool_calls[0].operation, McpSkillToolOperation::Execute);
    assert!(tool_calls[0].invocation.is_none());
    assert!(tool_calls[0].host_code_execution);
    assert_eq!(
        tool_calls[0].requested_allowed_tools.as_deref(),
        Some("deploy")
    );
    for (recorded, operation) in tool_calls[1..].iter().zip([
        McpSkillToolOperation::Execute,
        McpSkillToolOperation::Reconcile,
    ]) {
        assert_eq!(recorded.skill_uri, ROOT_URI);
        assert_eq!(recorded.tool_name, "deploy");
        assert_eq!(
            recorded.tool_identity.as_ref(),
            Some(descriptor.metadata().identity())
        );
        assert_eq!(
            recorded.descriptor_digest,
            Some(guarded.binding().descriptor_digest())
        );
        assert_eq!(recorded.operation, operation);
        let invocation = recorded.invocation.as_ref().unwrap();
        assert_eq!(invocation.tenant_id().to_string(), "tenant-mcp-skills");
        assert_eq!(invocation.run_id().to_string(), RUN_ID);
        assert_eq!(invocation.thread_id().to_string(), THREAD_ID);
        assert_eq!(invocation.invocation_id().to_string(), INVOCATION_ID);
        assert_eq!(invocation.attempt_id().to_string(), ATTEMPT_ID);
        assert_eq!(
            invocation.origin_event_id().unwrap().to_string(),
            ORIGIN_EVENT_ID
        );
        assert!(!invocation.has_recovery_handle());
        assert_eq!(invocation.input(), &tool_input(descriptor));
        assert!(recorded.host_code_execution);
        assert_eq!(recorded.requested_allowed_tools.as_deref(), Some("deploy"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_host_is_lazy_content_bound_origin_scoped_and_freshly_authorized() {
    let (endpoint, server) = start_server(vec![
        discovery(),
        list_result(&[root_entry()], Some("page-2")),
        list_result(&[nested_entry()], None),
        text_resource(ROOT_URI, ROOT_SKILL),
        text_resource(REFERENCE_URI, REFERENCE),
        get_result(&nested_entry()),
        text_resource(NESTED_URI, NESTED_SKILL),
    ])
    .await;
    let policy = Arc::new(RecordingPolicy::default());
    let skill_host = host(
        connect(endpoint, McpClientOptions::for_skills()).await,
        policy.clone(),
    );

    let catalog = skill_host.list_skills().await.unwrap();
    assert_eq!(catalog.skills().len(), 2);
    let active = skill_host
        .activate(catalog.find_uri(ROOT_URI).unwrap())
        .await
        .unwrap();
    assert_eq!(active.identity().origin().as_str(), "production-docs");
    assert_eq!(active.instructions().identity(), active.identity());
    let root_text = str::from_utf8(ROOT_SKILL).unwrap();
    assert_eq!(active.instructions().text(), Some(root_text));

    let root_children = active.list_directory("").unwrap();
    assert_eq!(root_children.len(), 3);
    assert!(
        root_children
            .iter()
            .any(|child| child.relative_path() == "references" && child.is_directory())
    );
    assert!(
        root_children
            .iter()
            .any(|child| child.relative_path() == "nested" && child.is_directory())
    );

    let first = active.read_file("references/checklist.md").await.unwrap();
    let second = active.read_file("references/checklist.md").await.unwrap();
    assert_eq!(first.bytes(), REFERENCE);
    assert_eq!(second.bytes(), REFERENCE);
    assert_eq!(first.identity().origin().as_str(), "production-docs");

    let permit = active.authorize_tool_call("deploy", true).await.unwrap();
    assert_eq!(permit.identity(), active.identity());
    assert_eq!(permit.tool_name(), "deploy");
    assert!(permit.tool_identity().is_none());
    assert!(permit.tool_descriptor_digest().is_none());
    assert_eq!(permit.operation(), McpSkillToolOperation::Execute);
    assert!(permit.invocation().is_none());
    assert!(permit.host_code_execution());
    assert_eq!(permit.manifest_digest(), active.entry().manifest_digest());
    drop(permit);

    let active = Arc::new(active);
    let (descriptor, guarded) = exercise_bound_tool(Arc::clone(&active)).await;

    let nested = active
        .activate_nested("nested/child/SKILL.md")
        .await
        .unwrap();
    assert_eq!(nested.identity().uri(), NESTED_URI);
    assert_eq!(nested.instructions().bytes(), NESTED_SKILL);

    {
        let activations = policy.activations.lock().unwrap();
        assert_eq!(activations.len(), 2);
        assert!(matches!(activations[0].1, McpSkillActivationSource::Direct));
        assert!(matches!(
            &activations[1].1,
            McpSkillActivationSource::Nested(parent) if parent.uri() == ROOT_URI
        ));
    }
    assert_tool_policy_calls(&policy, &descriptor, &guarded);

    let requests = server.await.unwrap();
    let methods = requests
        .iter()
        .map(|request| request.body["method"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        methods,
        [
            "server/discover",
            "skills/list",
            "skills/list",
            "resources/read",
            "resources/read",
            "skills/get",
            "resources/read"
        ]
    );
    for request in &requests {
        assert_eq!(
            request.body["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"]["extensions"]
                [MCP_SKILLS_EXTENSION_ID],
            json!({})
        );
    }
    for (request, method) in requests.iter().zip(methods) {
        let has_name = header(&request.headers, "mcp-name").is_some();
        assert_eq!(has_name, method == "resources/read");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn denial_happens_before_any_skill_file_read() {
    let (endpoint, server) = start_server(vec![discovery(), get_result(&root_entry())]).await;
    let skill_host = host(
        connect(endpoint, McpClientOptions::for_skills()).await,
        Arc::new(DenyActivation),
    );
    assert!(matches!(
        skill_host.activate_uri(ROOT_URI).await,
        Err(McpSkillHostError::ActivationDenied)
    ));
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].body["method"], "skills/get");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_tool_denial_fails_before_provider_execution() {
    let (endpoint, server) = start_server(vec![
        discovery(),
        get_result(&root_entry()),
        text_resource(ROOT_URI, ROOT_SKILL),
    ])
    .await;
    let skill_host = host(
        connect(endpoint, McpClientOptions::for_skills()).await,
        Arc::new(DenyToolExecution),
    );
    let active = Arc::new(skill_host.activate_uri(ROOT_URI).await.unwrap());
    let descriptor = tool_descriptor();
    let provider = Arc::new(CountingTool::new(descriptor.clone()));
    let guarded = McpSkillBoundTool::new(
        active,
        Arc::clone(&provider) as Arc<dyn ErasedTool>,
        McpSkillHostCodeExecution::NotPossible,
    )
    .unwrap();

    let error = guarded
        .call(tool_context(&descriptor), tool_input(&descriptor))
        .await
        .unwrap_err();
    assert_eq!(error.failure().category(), FailureCategory::PolicyDenied);
    assert_eq!(error.external_effect(), ToolExternalEffect::NotStarted);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(server.await.unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resource_digest_mismatch_fails_closed() {
    let (endpoint, server) = start_server(vec![
        discovery(),
        get_result(&root_entry()),
        text_resource(ROOT_URI, b"tampered"),
    ])
    .await;
    let skill_host = host(
        connect(endpoint, McpClientOptions::for_skills()).await,
        Arc::new(RecordingPolicy::default()),
    );
    assert!(matches!(
        skill_host.activate_uri(ROOT_URI).await,
        Err(McpSkillHostError::ResourceVerificationFailed)
    ));
    assert_eq!(server.await.unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verified_skill_document_must_match_advertised_frontmatter() {
    let changed = b"---\nname: root-skill\ndescription: Changed after listing.\nallowed-tools: deploy\n---\nBody.\n";
    let mut entry = root_entry();
    entry["resources"][0] = resource(ROOT_URI, changed);
    let (endpoint, server) = start_server(vec![
        discovery(),
        get_result(&entry),
        text_resource(ROOT_URI, changed),
    ])
    .await;
    let skill_host = host(
        connect(endpoint, McpClientOptions::for_skills()).await,
        Arc::new(RecordingPolicy::default()),
    );
    assert!(matches!(
        skill_host.activate_uri(ROOT_URI).await,
        Err(McpSkillHostError::FrontmatterVerificationFailed)
    ));
    assert_eq!(server.await.unwrap().len(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_client_does_not_advertise_or_host_skills() {
    let (endpoint, server) = start_server(vec![discovery()]).await;
    let client = connect(endpoint, McpClientOptions::default()).await;
    assert!(matches!(
        McpSkillHost::new(
            client,
            McpSkillOrigin::new("production-docs").unwrap(),
            Arc::new(DenyActivation),
            McpSkillHostOptions::default()
        ),
        Err(McpSkillHostError::TransportProfileTooSmall)
    ));
    let requests = server.await.unwrap();
    assert_eq!(
        requests[0].body["params"]["_meta"]["io.modelcontextprotocol/clientCapabilities"],
        json!({})
    );
}

fn header<'a>(headers: &'a str, expected: &str) -> Option<&'a str> {
    headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case(expected).then(|| value.trim())
    })
}
