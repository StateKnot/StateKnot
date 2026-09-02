// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Driver used only by the pinned official MCP conformance runner.

use std::{env, error::Error, sync::Arc};

use serde_json::{Map, Value, json};
use stateknot_integrations::{
    AnonymousMcpAuthorization, MCP_PROTOCOL_VERSION_2026_07_28, McpClient, McpClientIdentity,
    McpClientOptions, McpInputRequired, McpTool, McpToolCall, McpToolCatalog, ProviderEndpoint,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let scenario = env::var("MCP_CONFORMANCE_SCENARIO")?;
    let protocol_version = env::var("MCP_CONFORMANCE_PROTOCOL_VERSION")?;
    if protocol_version != MCP_PROTOCOL_VERSION_2026_07_28 {
        return Err(format!("unsupported conformance protocol version: {protocol_version}").into());
    }
    let endpoint = env::args()
        .nth(1)
        .ok_or("the conformance runner did not append a server URL")?;
    let endpoint = conformance_endpoint(&endpoint)?;
    let client = McpClient::connect(
        endpoint,
        McpClientIdentity::new("stateknot-conformance", env!("CARGO_PKG_VERSION"))?,
        Arc::new(AnonymousMcpAuthorization),
        McpClientOptions::default(),
    )
    .await?;

    match scenario.as_str() {
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
