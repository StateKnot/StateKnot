// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end security contract for the static MCP Skills client and Host.

use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use stateknot_core::{BoxFuture, Digest};
use stateknot_integrations::{
    AnonymousMcpAuthorization, MCP_SKILLS_EXTENSION_ID, McpClient, McpClientIdentity,
    McpClientOptions, McpSkillActivationRequest, McpSkillActivationSource, McpSkillHost,
    McpSkillHostError, McpSkillHostOptions, McpSkillHostPolicy, McpSkillHostPolicyError,
    McpSkillOrigin, McpSkillToolAuthorizationRequest, ProviderEndpoint,
};
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

type RecordedToolCall = (String, String, bool, Option<String>);

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
        self.tool_calls.lock().unwrap().push((
            request.identity().uri().to_owned(),
            request.tool_name().to_owned(),
            request.host_code_execution(),
            request.requested_allowed_tools().map(str::to_owned),
        ));
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

fn host(client: McpClient, policy: Arc<dyn McpSkillHostPolicy>) -> McpSkillHost {
    McpSkillHost::new(
        client,
        McpSkillOrigin::new("production-docs").unwrap(),
        policy,
        McpSkillHostOptions::default(),
    )
    .unwrap()
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
    assert!(permit.host_code_execution());
    assert_eq!(permit.manifest_digest(), active.entry().manifest_digest());

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
    assert_eq!(
        policy.tool_calls.lock().unwrap().as_slice(),
        &[(
            ROOT_URI.to_owned(),
            "deploy".to_owned(),
            true,
            Some("deploy".to_owned())
        )]
    );

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
