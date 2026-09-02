// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Driver used only by the pinned official MCP conformance runner.

use std::{env, error::Error, fmt, sync::Arc, time::Duration};

use reqwest::header;
use serde_json::{Map, Value, json};
use stateknot_core::BoxFuture;
use stateknot_integrations::{
    AnonymousMcpAuthorization, MCP_PROTOCOL_VERSION_2026_07_28, McpClient, McpClientIdentity,
    McpClientOptions, McpInputRequired, McpOAuthAuthorization, McpOAuthOptions,
    McpOAuthRegistration, McpOAuthResource, McpOAuthUserAgent, McpOAuthUserAgentError,
    McpOAuthUserAuthorizationRequest, McpTool, McpToolCall, McpToolCatalog, ProviderEndpoint,
};

const CONFORMANCE_REDIRECT_URI: &str = "http://localhost:3000/callback";
const CONFORMANCE_CIMD_URL: &str = "https://conformance-test.local/client-metadata.json";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let scenario = env::var("MCP_CONFORMANCE_SCENARIO")?;
    let protocol_version = env::var("MCP_CONFORMANCE_PROTOCOL_VERSION")?;
    if protocol_version != MCP_PROTOCOL_VERSION_2026_07_28 {
        return Err(format!("unsupported conformance protocol version: {protocol_version}").into());
    }
    let resource_url = env::args()
        .nth(1)
        .ok_or("the conformance runner did not append a server URL")?;
    let endpoint = conformance_endpoint(&resource_url)?;
    let authorization: Arc<dyn stateknot_integrations::McpClientAuthorizationProvider> =
        if scenario.starts_with("auth/") {
            let registration = conformance_registration(&scenario)?;
            let options = McpOAuthOptions::native(
                CONFORMANCE_REDIRECT_URI,
                "stateknot-conformance",
                registration,
            )?
            .with_authorization_timeout(Duration::from_secs(20))?;
            Arc::new(
                McpOAuthAuthorization::new(
                    McpOAuthResource::loopback_http(&resource_url)?,
                    options,
                    Arc::new(ConformanceOAuthUserAgent::new()?),
                )
                .await?,
            )
        } else {
            Arc::new(AnonymousMcpAuthorization)
        };
    let client = McpClient::connect(
        endpoint,
        McpClientIdentity::new("stateknot-conformance", env!("CARGO_PKG_VERSION"))?,
        authorization,
        McpClientOptions::default(),
    )
    .await?;

    match scenario.as_str() {
        scenario if scenario.starts_with("auth/") => run_oauth(&client).await?,
        "request-metadata" => {}
        "tools_call" => run_tools_call(&client).await?,
        "http-standard-headers" => run_standard_headers(&client).await?,
        "http-custom-headers" => run_custom_headers(&client).await?,
        "http-invalid-tool-headers" => run_invalid_headers(&client).await?,
        "json-schema-ref-no-deref" => {
            let _catalog = client.list_tools().await?;
        }
        "sep-2322-client-request-state" => run_mrtr(&client).await?,
        unsupported => {
            return Err(format!("unsupported conformance scenario: {unsupported}").into());
        }
    }
    Ok(())
}

fn conformance_registration(scenario: &str) -> Result<McpOAuthRegistration, Box<dyn Error>> {
    if scenario == "auth/pre-registration" {
        let context: Value = serde_json::from_str(&env::var("MCP_CONFORMANCE_CONTEXT")?)?;
        let client_id = context
            .get("client_id")
            .and_then(Value::as_str)
            .ok_or("pre-registration context omitted client_id")?;
        let client_secret = context
            .get("client_secret")
            .and_then(Value::as_str)
            .ok_or("pre-registration context omitted client_secret")?;
        return Ok(McpOAuthRegistration::pre_registered(
            client_id,
            Some(stateknot_integrations::ApiKey::new(client_secret)?),
        )?);
    }
    Ok(McpOAuthRegistration::client_metadata_document(
        CONFORMANCE_CIMD_URL,
    )?)
}

async fn run_oauth(client: &McpClient) -> Result<(), Box<dyn Error>> {
    let catalog = client.list_tools().await?;
    if let Some(tool) = catalog.find("test-tool") {
        require_complete(client.call_tool(tool, json!({})).await?)?;
    }
    Ok(())
}

struct ConformanceOAuthUserAgent {
    http: reqwest::Client,
}

impl ConformanceOAuthUserAgent {
    fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .build()?,
        })
    }
}

impl fmt::Debug for ConformanceOAuthUserAgent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConformanceOAuthUserAgent")
            .finish_non_exhaustive()
    }
}

impl McpOAuthUserAgent for ConformanceOAuthUserAgent {
    fn authorize(
        &self,
        request: &McpOAuthUserAuthorizationRequest,
    ) -> BoxFuture<'_, Result<Box<str>, McpOAuthUserAgentError>> {
        let authorization_url = request.authorization_url().to_owned();
        Box::pin(async move {
            let response = self
                .http
                .get(authorization_url)
                .send()
                .await
                .map_err(|_| McpOAuthUserAgentError::Unavailable)?;
            if !response.status().is_redirection() {
                return Err(McpOAuthUserAgentError::InvalidCallback);
            }
            response
                .headers()
                .get(header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_owned().into_boxed_str())
                .ok_or(McpOAuthUserAgentError::InvalidCallback)
        })
    }
}

fn conformance_endpoint(value: &str) -> Result<ProviderEndpoint, Box<dyn Error>> {
    let mut url = reqwest::Url::parse(value)?;
    if url.scheme() != "http" {
        return Err("the local conformance endpoint must use HTTP".into());
    }
    if url.host_str() == Some("localhost") {
        url.set_host(Some("127.0.0.1"))?;
    }
    Ok(ProviderEndpoint::loopback_http(url.as_str())?)
}

async fn run_tools_call(client: &McpClient) -> Result<(), Box<dyn Error>> {
    let catalog = client.list_tools().await?;
    let tool = required_tool(&catalog, "add_numbers")?;
    require_complete(client.call_tool(tool, json!({ "a": 2, "b": 3 })).await?)?;
    Ok(())
}

async fn run_standard_headers(client: &McpClient) -> Result<(), Box<dyn Error>> {
    let catalog = client.list_tools().await?;
    let tool = required_tool(&catalog, "test_headers")?;
    require_complete(client.call_tool(tool, json!({})).await?)?;
    Ok(())
}

async fn run_custom_headers(client: &McpClient) -> Result<(), Box<dyn Error>> {
    let context: Value = serde_json::from_str(&env::var("MCP_CONFORMANCE_CONTEXT")?)?;
    let calls = context
        .get("toolCalls")
        .and_then(Value::as_array)
        .ok_or("custom-header scenario did not supply toolCalls")?;
    let catalog = client.list_tools().await?;
    for call in calls {
        let name = call
            .get("name")
            .and_then(Value::as_str)
            .ok_or("toolCalls entry omitted name")?;
        let arguments = call
            .get("arguments")
            .cloned()
            .ok_or("toolCalls entry omitted arguments")?;
        require_complete(
            client
                .call_tool(required_tool(&catalog, name)?, arguments)
                .await?,
        )?;
    }
    Ok(())
}

async fn run_invalid_headers(client: &McpClient) -> Result<(), Box<dyn Error>> {
    let catalog = client.list_tools().await?;
    if catalog.rejected_tools().is_empty() {
        return Err("invalid x-mcp-header tools were not excluded".into());
    }
    let valid = required_tool(&catalog, "valid_tool")?;
    require_complete(
        client
            .call_tool(valid, json!({ "region": "us-west1" }))
            .await?,
    )?;
    Ok(())
}

async fn run_mrtr(client: &McpClient) -> Result<(), Box<dyn Error>> {
    let catalog = client.list_tools().await?;

    let echo_pending = require_input(
        client
            .call_tool(required_tool(&catalog, "test_mrtr_echo_state")?, json!({}))
            .await?,
    )?;
    require_complete(
        client
            .call_tool(required_tool(&catalog, "test_mrtr_unrelated")?, json!({}))
            .await?,
    )?;
    let echo_inputs = accepted_inputs(&echo_pending);
    require_complete(echo_pending.resume(echo_inputs).await?)?;

    let no_state_pending = require_input(
        client
            .call_tool(required_tool(&catalog, "test_mrtr_no_state")?, json!({}))
            .await?,
    )?;
    let no_state_inputs = accepted_inputs(&no_state_pending);
    require_complete(no_state_pending.resume(no_state_inputs).await?)?;

    require_complete(
        client
            .call_tool(
                required_tool(&catalog, "test_mrtr_no_result_type")?,
                json!({}),
            )
            .await?,
    )?;
    Ok(())
}

fn accepted_inputs(pending: &McpInputRequired) -> Map<String, Value> {
    pending
        .input_requests()
        .keys()
        .map(|key| {
            (
                key.clone(),
                json!({ "action": "accept", "content": { "confirmed": true } }),
            )
        })
        .collect()
}

fn required_tool<'a>(
    catalog: &'a McpToolCatalog,
    name: &str,
) -> Result<&'a McpTool, Box<dyn Error>> {
    catalog
        .find(name)
        .ok_or_else(|| format!("required Tool was not usable: {name}").into())
}

fn require_input(
    response: stateknot_integrations::McpToolCallResponse,
) -> Result<McpInputRequired, Box<dyn Error>> {
    match response.into_outcome() {
        McpToolCall::InputRequired(pending) => Ok(pending),
        McpToolCall::Complete(_) => Err("expected input_required Tool result".into()),
        _ => Err("unsupported Tool result".into()),
    }
}

fn require_complete(
    response: stateknot_integrations::McpToolCallResponse,
) -> Result<(), Box<dyn Error>> {
    match response.into_outcome() {
        McpToolCall::Complete(_) => Ok(()),
        McpToolCall::InputRequired(_) => Err("expected complete Tool result".into()),
        _ => Err("unsupported Tool result".into()),
    }
}
