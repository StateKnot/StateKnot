// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end HTTP contract tests for the static MCP Skills server profile.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt as _, Full};
use serde_json::{Value, json};
use stateknot_core::{BoxFuture, Digest};
use stateknot_integrations::{
    AllowMcpServerSkillAuthorization, MCP_SKILLS_EXTENSION_ID, McpServerApplicationBuilder,
    McpServerApplicationOptions, McpServerAuthentication, McpServerCacheScope,
    McpServerHttpOptions, McpServerHttpService, McpServerSkillAuthorization,
    McpServerSkillAuthorizationError, McpServerSkillAuthorizationRequest,
    McpServerSkillCatalogBuilder, McpServerSkillDefinition, McpServerSkillFile,
    McpServerSkillOperation, McpServerSkillService, McpServerSkillServiceBuildError,
};
use tower_service::Service as _;

const PORT: u16 = 32_141;

fn skill(name: &str, scoped: bool) -> McpServerSkillDefinition {
    let document = format!(
        "---\nname: {name}\ndescription: Review code with the StateKnot checklist.\nmetadata:\n  owner: stateknot\n---\n\n# Review\n"
    );
    let definition = McpServerSkillDefinition::new(
        format!("skill://{name}/SKILL.md"),
        [
            McpServerSkillFile::text("SKILL.md", "text/markdown", document).unwrap(),
            McpServerSkillFile::binary(
                "assets/signature.bin",
                "application/octet-stream",
                [0_u8, 159, 255],
            )
            .unwrap(),
            McpServerSkillFile::text(
                "references/checklist.md",
                "text/markdown",
                "# Checklist\n\n- Correctness\n",
            )
            .unwrap(),
        ],
    )
    .unwrap();
    if scoped {
        definition.with_required_scopes(["skills:private"]).unwrap()
    } else {
        definition
    }
}

fn service() -> McpServerHttpService<stateknot_integrations::McpServerApplication> {
    service_with_authorization(AllowMcpServerSkillAuthorization)
}

fn service_with_authorization<A>(
    authorization: A,
) -> McpServerHttpService<stateknot_integrations::McpServerApplication>
where
    A: McpServerSkillAuthorization,
{
    let mut catalog = McpServerSkillCatalogBuilder::default();
    catalog.register(skill("code-review", false)).unwrap();
    catalog.register(skill("incident-review", false)).unwrap();
    catalog.register(skill("private-review", true)).unwrap();
    let options = McpServerApplicationOptions::new(
        "stateknot-skills-test",
        "0.0.0",
        1,
        Duration::from_secs(300),
        McpServerCacheScope::Private,
    )
    .unwrap();
    let application = McpServerApplicationBuilder::new(options)
        .with_skills(catalog.build().unwrap(), authorization)
        .unwrap()
        .build()
        .unwrap();
    McpServerHttpService::new(
        application,
        McpServerHttpOptions::loopback(PORT).unwrap(),
        McpServerAuthentication::anonymous_loopback(),
    )
    .unwrap()
}

#[derive(Clone, Default)]
struct RecordingAuthorization {
    seen: Arc<Mutex<Vec<(McpServerSkillOperation, String)>>>,
}

impl McpServerSkillAuthorization for RecordingAuthorization {
    fn authorize(
        &self,
        request: McpServerSkillAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerSkillAuthorizationError>> {
        let operation = request.operation();
        let uri = request.uri().to_owned();
        self.seen.lock().unwrap().push((operation, uri.clone()));
        Box::pin(async move {
            if uri == "skill://incident-review/SKILL.md" {
                Err(McpServerSkillAuthorizationError::Forbidden)
            } else {
                Ok(())
            }
        })
    }
}

fn request(method: &str, mut params: Value) -> Request<Full<Bytes>> {
    let name = (method == "resources/read")
        .then(|| params.get("uri").and_then(Value::as_str).map(str::to_owned))
        .flatten();
    params.as_object_mut().unwrap().insert(
        "_meta".to_owned(),
        json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": { "name": "skills-test", "version": "0" },
            "io.modelcontextprotocol/clientCapabilities": {
                "extensions": { (MCP_SKILLS_EXTENSION_ID): {} }
            }
        }),
    );
    let body = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }))
    .unwrap();
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("http://127.0.0.1:{PORT}/mcp"))
        .header("host", format!("127.0.0.1:{PORT}"))
        .header("origin", format!("http://127.0.0.1:{PORT}"))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", method);
    if let Some(name) = name {
        builder = builder.header("mcp-name", name);
    }
    builder.body(Full::new(Bytes::from(body))).unwrap()
}

async fn call(
    service: &mut McpServerHttpService<stateknot_integrations::McpServerApplication>,
    method: &str,
    params: Value,
) -> Value {
    let (status, value) = invoke(service, method, params).await;
    assert_eq!(status, StatusCode::OK, "{method}: {value}");
    value
}

async fn invoke(
    service: &mut McpServerHttpService<stateknot_integrations::McpServerApplication>,
    method: &str,
    params: Value,
) -> (StatusCode, Value) {
    let response = service.call(request(method, params)).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn discovery_and_paginated_skill_methods_match_sep_2640() {
    let mut service = service();
    let discovery = call(&mut service, "server/discover", json!({})).await;
    assert_eq!(
        discovery.pointer("/result/capabilities/extensions/io.modelcontextprotocol~1skills"),
        Some(&json!({}))
    );
    assert_eq!(
        discovery.pointer("/result/capabilities/resources"),
        Some(&json!({}))
    );

    let first = call(&mut service, "skills/list", json!({})).await;
    assert_eq!(
        first.pointer("/result/resultType"),
        Some(&json!("complete"))
    );
    assert_eq!(first.pointer("/result/cacheScope"), Some(&json!("private")));
    assert_eq!(first.pointer("/result/ttlMs"), Some(&json!(300_000)));
    assert_eq!(
        first.pointer("/result/skills/0/frontmatter/name"),
        Some(&json!("code-review"))
    );
    let cursor = first
        .pointer("/result/nextCursor")
        .and_then(Value::as_str)
        .unwrap();
    let second = call(&mut service, "skills/list", json!({ "cursor": cursor })).await;
    assert_eq!(
        second.pointer("/result/skills/0/frontmatter/name"),
        Some(&json!("incident-review"))
    );
    assert!(second.pointer("/result/nextCursor").is_none());
    assert!(
        first
            .pointer("/result/skills")
            .and_then(Value::as_array)
            .unwrap()
            .iter()
            .chain(
                second
                    .pointer("/result/skills")
                    .and_then(Value::as_array)
                    .unwrap()
            )
            .all(|entry| entry.pointer("/frontmatter/name") != Some(&json!("private-review")))
    );

    let exact = call(
        &mut service,
        "skills/get",
        json!({ "uri": "skill://code-review/SKILL.md" }),
    )
    .await;
    assert_eq!(
        exact.pointer("/result/skill/frontmatter/metadata/owner"),
        Some(&json!("stateknot"))
    );
    assert_eq!(
        exact
            .pointer("/result/skill/resources")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(3)
    );
}

#[tokio::test]
async fn resource_read_returns_the_exact_manifest_bytes() {
    let mut service = service();
    let entry = call(
        &mut service,
        "skills/get",
        json!({ "uri": "skill://code-review/SKILL.md" }),
    )
    .await;
    let expected = &entry["result"]["skill"]["resources"][0];
    let read = call(
        &mut service,
        "resources/read",
        json!({ "uri": expected["uri"] }),
    )
    .await;
    let text = read
        .pointer("/result/contents/0/text")
        .and_then(Value::as_str)
        .unwrap();
    assert_eq!(
        Digest::sha256(text.as_bytes()).to_string(),
        expected["digest"].as_str().unwrap()
    );
    assert_eq!(text.len() as u64, expected["size"].as_u64().unwrap());
    assert_eq!(
        read.pointer("/result/contents/0/mimeType"),
        Some(&json!("text/markdown"))
    );

    let binary = entry["result"]["skill"]["resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|resource| {
            resource["uri"]
                .as_str()
                .is_some_and(|uri| uri.ends_with("/assets/signature.bin"))
        })
        .unwrap();
    let binary_read = call(
        &mut service,
        "resources/read",
        json!({ "uri": binary["uri"] }),
    )
    .await;
    assert_eq!(
        binary_read.pointer("/result/contents/0/blob"),
        Some(&json!("AJ//"))
    );
    assert_eq!(
        binary_read.pointer("/result/contents/0/mimeType"),
        Some(&json!("application/octet-stream"))
    );
    assert_eq!(
        binary.pointer("/digest"),
        Some(&json!(Digest::sha256([0_u8, 159, 255]).to_string()))
    );
    assert_eq!(binary.pointer("/size"), Some(&json!(3)));
}

#[tokio::test]
async fn hidden_unknown_and_malformed_requests_do_not_disclose_skills() {
    let mut service = service();
    for uri in [
        "skill://private-review/SKILL.md",
        "skill://missing/SKILL.md",
    ] {
        let (status, get) = invoke(&mut service, "skills/get", json!({ "uri": uri })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(get.pointer("/error/code"), Some(&json!(-32602)));
    }
    let (status, read) = invoke(
        &mut service,
        "resources/read",
        json!({ "uri": "skill://private-review/SKILL.md" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(read.pointer("/error").is_some());
    let (status, invalid) =
        invoke(&mut service, "skills/list", json!({ "unexpected": true })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid.pointer("/error/code"), Some(&json!(-32602)));
}

#[tokio::test]
async fn dynamic_authorization_runs_before_direct_lookup_and_filters_discovery() {
    let authorization = RecordingAuthorization::default();
    let seen = Arc::clone(&authorization.seen);
    let mut service = service_with_authorization(authorization);
    let listed = call(&mut service, "skills/list", json!({})).await;
    assert_eq!(
        listed.pointer("/result/skills/0/frontmatter/name"),
        Some(&json!("code-review"))
    );
    assert!(listed.pointer("/result/nextCursor").is_none());

    let unknown = "skill://unknown/SKILL.md";
    let (status, response) = invoke(&mut service, "skills/get", json!({ "uri": unknown })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response.pointer("/error/code"), Some(&json!(-32602)));
    assert!(
        seen.lock()
            .unwrap()
            .contains(&(McpServerSkillOperation::Get, unknown.to_owned()))
    );
}

#[test]
fn protected_skills_and_policies_cannot_use_public_cache_metadata() {
    let mut catalog = McpServerSkillCatalogBuilder::default();
    catalog.register(skill("private-review", true)).unwrap();
    let options = McpServerApplicationOptions::new(
        "stateknot-skills-test",
        "0.0.0",
        16,
        Duration::from_secs(300),
        McpServerCacheScope::Public,
    )
    .unwrap();
    assert_eq!(
        McpServerSkillService::new(
            catalog.build().unwrap(),
            options,
            AllowMcpServerSkillAuthorization,
        )
        .unwrap_err(),
        McpServerSkillServiceBuildError::PublicCacheWithScopedSkills
    );

    let mut catalog = McpServerSkillCatalogBuilder::default();
    catalog.register(skill("public-review", false)).unwrap();
    let options = McpServerApplicationOptions::new(
        "stateknot-skills-test",
        "0.0.0",
        16,
        Duration::from_secs(300),
        McpServerCacheScope::Public,
    )
    .unwrap();
    assert_eq!(
        McpServerSkillService::new(
            catalog.build().unwrap(),
            options,
            RecordingAuthorization::default(),
        )
        .unwrap_err(),
        McpServerSkillServiceBuildError::PublicCacheWithDynamicPolicy
    );
}
