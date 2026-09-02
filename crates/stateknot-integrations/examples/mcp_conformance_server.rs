// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Hermetic MCP 2026-07-28 server fixture for the pinned official runner.
//!
//! Fixture names and payloads intentionally follow the official conformance
//! contract. This is acceptance evidence, not an application template.

#![allow(
    deprecated,
    clippy::needless_pass_by_value,
    clippy::too_many_lines,
    clippy::unused_async,
    clippy::wildcard_imports,
    reason = "the hermetic fixture mirrors official scenario payloads and method groupings"
)]

use std::{borrow::Cow, collections::BTreeMap};

use rmcp::{ErrorData, RoleServer, ServerHandler, model::*, service::RequestContext};
use serde_json::{Value, json};
use stateknot_integrations::{McpServerAuthentication, McpServerHttpOptions, McpServerHttpService};

const TEST_IMAGE_DATA: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg==";
const TEST_AUDIO_DATA: &str = "UklGRiQAAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YQAAAAA=";
const CACHE_TTL_MS: u64 = 60_000;
const REQUEST_STATE_KEY: &[u8] = b"stateknot-conformance-request-state-key";

fn json_object(value: Value) -> JsonObject {
    value
        .as_object()
        .expect("fixture JSON schema is an object")
        .clone()
}

fn empty_schema() -> JsonObject {
    json_object(json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    }))
}

fn custom_header_tool() -> Tool {
    Tool::new(
        "test_custom_header",
        "Validates SEP-2243 custom parameter headers",
        json_object(json!({
            "type": "object",
            "properties": {
                "value": { "type": "string", "x-mcp-header": "Value" }
            },
            "required": ["value"],
            "additionalProperties": false
        })),
    )
}

fn conformance_tools() -> Vec<Tool> {
    let mut tools = [
        ("test_simple_text", "Returns simple text content"),
        ("test_image_content", "Returns image content"),
        ("test_audio_content", "Returns audio content"),
        (
            "test_embedded_resource",
            "Returns embedded resource content",
        ),
        (
            "test_multiple_content_types",
            "Returns multiple content types",
        ),
        ("test_error_handling", "Always returns a Tool error"),
        ("test_tool_with_progress", "Reports progress notifications"),
        (
            "test_input_required_result_elicitation",
            "Requires elicitation input via MRTR",
        ),
        (
            "test_input_required_result_sampling",
            "Requires sampling input via MRTR",
        ),
        (
            "test_input_required_result_list_roots",
            "Requires roots/list input via MRTR",
        ),
        (
            "test_input_required_result_request_state",
            "Round-trips integrity-protected requestState",
        ),
        (
            "test_input_required_result_multiple_inputs",
            "Requires multiple MRTR inputs",
        ),
        (
            "test_input_required_result_multi_round",
            "Drives multiple MRTR rounds",
        ),
        (
            "test_input_required_result_tampered_state",
            "Rejects tampered requestState",
        ),
        (
            "test_input_required_result_capabilities",
            "Requests only declared client capabilities",
        ),
        (
            "test_missing_capability",
            "Requires the sampling client capability",
        ),
        (
            "test_streaming_elicitation",
            "Returns an MRTR input_required result",
        ),
        (
            "test_logging_tool",
            "Logs only when request metadata asks for a level",
        ),
    ]
    .into_iter()
    .map(|(name, description)| Tool::new(name, description, empty_schema()))
    .collect::<Vec<_>>();
    tools.push(custom_header_tool());
    tools.push(Tool::new(
        "json_schema_2020_12_tool",
        "Tool with JSON Schema 2020-12 features",
        json_object(json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "$defs": {
                "address": {
                    "$anchor": "address",
                    "type": "object",
                    "properties": {
                        "street": { "type": "string" },
                        "city": { "type": "string" }
                    }
                }
            },
            "properties": {
                "name": { "type": "string" },
                "address": { "$ref": "#/$defs/address" }
            },
            "allOf": [{
                "anyOf": [
                    { "required": ["name"] },
                    { "required": ["address"] }
                ]
            }],
            "if": { "required": ["address"] },
            "then": { "properties": { "address": { "required": ["street"] } } },
            "else": { "required": ["name"] },
            "additionalProperties": false
        })),
    ));
    tools
}

fn elicitation_request(message: &str, properties: Value, required: Value) -> InputRequest {
    InputRequest::Elicitation(ElicitRequest::new(
        ElicitRequestParams::FormElicitationParams {
            meta: None,
            message: message.to_owned(),
            requested_schema: serde_json::from_value(json!({
                "type": "object",
                "properties": properties,
                "required": required
            }))
            .expect("fixture elicitation schema is valid"),
        },
    ))
}

fn sampling_request(prompt: &str) -> InputRequest {
    InputRequest::CreateMessage(CreateMessageRequest::new(CreateMessageRequestParams::new(
        vec![SamplingMessage::user_text(prompt)],
        100,
    )))
}

fn roots_request() -> InputRequest {
    InputRequest::ListRoots(ListRootsRequest::default())
}

fn input_response<'a>(
    responses: Option<&'a InputResponses>,
    key: &str,
) -> Option<&'a serde_json::Map<String, Value>> {
    responses
        .and_then(|values| values.get(key))
        .and_then(Value::as_object)
}

#[derive(Clone)]
struct ConformanceServer {
    request_state: RequestStateCodec,
}

impl ConformanceServer {
    fn new() -> Self {
        Self {
            request_state: RequestStateCodec::try_new(REQUEST_STATE_KEY)
                .expect("fixture key meets the minimum length"),
        }
    }

    fn tampered_state_error() -> ErrorData {
        ErrorData::invalid_params("requestState failed integrity verification", None)
    }

    fn seal_json(&self, value: &Value) -> Result<String, ErrorData> {
        self.request_state
            .seal_json(value)
            .map_err(|_| ErrorData::internal_error("requestState sealing failed", None))
    }

    async fn call_mrtr(
        &self,
        request: CallToolRequestParams,
        context: &RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let responses = request.input_responses.as_ref();
        match request.name.as_ref() {
            "test_input_required_result_elicitation" => {
                if let Some(response) = input_response(responses, "user_name") {
                    let name = response
                        .get("content")
                        .and_then(|content| content.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or("friend");
                    return Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                        "Hello, {name}!"
                    ))])
                    .into());
                }
                let requests = BTreeMap::from([(
                    "user_name".to_owned(),
                    elicitation_request(
                        "What is your name?",
                        json!({ "name": { "type": "string" } }),
                        json!(["name"]),
                    ),
                )]);
                Ok(InputRequiredResult::from_input_requests(requests).into())
            }
            "test_input_required_result_sampling" => {
                if let Some(response) = input_response(responses, "capital_question") {
                    let text = response
                        .get("content")
                        .and_then(|content| content.get("text"))
                        .and_then(Value::as_str)
                        .unwrap_or("(no sampling text)");
                    return Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                        "Sampling response: {text}"
                    ))])
                    .into());
                }
                let requests = BTreeMap::from([(
                    "capital_question".to_owned(),
                    sampling_request("What is the capital of France?"),
                )]);
                Ok(InputRequiredResult::from_input_requests(requests).into())
            }
            "test_input_required_result_list_roots" => {
                if let Some(response) = input_response(responses, "client_roots") {
                    let roots = response
                        .get("roots")
                        .and_then(Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(|value| value.get("uri").and_then(Value::as_str))
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_default();
                    return Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                        "Client roots: [{roots}]"
                    ))])
                    .into());
                }
                let requests = BTreeMap::from([("client_roots".to_owned(), roots_request())]);
                Ok(InputRequiredResult::from_input_requests(requests).into())
            }
            "test_input_required_result_request_state"
            | "test_input_required_result_tampered_state" => {
                if let Some(sealed) = request.request_state.as_deref() {
                    self.request_state
                        .open(sealed)
                        .map_err(|_| Self::tampered_state_error())?;
                    return Ok(CallToolResult::success(vec![ContentBlock::text(
                        "Confirmed: state-ok",
                    )])
                    .into());
                }
                let sealed = self.seal_json(&json!({ "stage": "confirm" }))?;
                let requests = BTreeMap::from([(
                    "confirm".to_owned(),
                    elicitation_request(
                        "Please confirm",
                        json!({ "ok": { "type": "boolean" } }),
                        json!(["ok"]),
                    ),
                )]);
                Ok(InputRequiredResult::new(Some(requests), Some(sealed)).into())
            }
            "test_input_required_result_multiple_inputs" => {
                if let Some(sealed) = request.request_state.as_deref() {
                    self.request_state
                        .open(sealed)
                        .map_err(|_| Self::tampered_state_error())?;
                }
                let complete = input_response(responses, "user_name").is_some()
                    && input_response(responses, "greeting").is_some()
                    && input_response(responses, "client_roots").is_some()
                    && request.request_state.is_some();
                if complete {
                    return Ok(CallToolResult::success(vec![ContentBlock::text(
                        "All inputs received",
                    )])
                    .into());
                }
                let sealed = self.seal_json(&json!({ "stage": "gather" }))?;
                let requests = BTreeMap::from([
                    (
                        "user_name".to_owned(),
                        elicitation_request(
                            "What is your name?",
                            json!({ "name": { "type": "string" } }),
                            json!(["name"]),
                        ),
                    ),
                    (
                        "greeting".to_owned(),
                        sampling_request("Generate a greeting"),
                    ),
                    ("client_roots".to_owned(), roots_request()),
                ]);
                Ok(InputRequiredResult::new(Some(requests), Some(sealed)).into())
            }
            "test_input_required_result_multi_round" => {
                let round = match request.request_state.as_deref() {
                    None => 0,
                    Some(sealed) => {
                        let state: Value = self
                            .request_state
                            .open_json(sealed)
                            .map_err(|_| Self::tampered_state_error())?;
                        state.get("round").and_then(Value::as_i64).unwrap_or(0)
                    }
                };
                if round == 0 {
                    let sealed = self.seal_json(&json!({ "round": 1 }))?;
                    let requests = BTreeMap::from([(
                        "step1".to_owned(),
                        elicitation_request(
                            "Step 1: What is your name?",
                            json!({ "name": { "type": "string" } }),
                            json!(["name"]),
                        ),
                    )]);
                    return Ok(InputRequiredResult::new(Some(requests), Some(sealed)).into());
                }
                if round == 1 {
                    let sealed = self.seal_json(&json!({ "round": 2 }))?;
                    let requests = BTreeMap::from([(
                        "step2".to_owned(),
                        elicitation_request(
                            "Step 2: What is your favorite color?",
                            json!({ "color": { "type": "string" } }),
                            json!(["color"]),
                        ),
                    )]);
                    return Ok(InputRequiredResult::new(Some(requests), Some(sealed)).into());
                }
                Ok(
                    CallToolResult::success(vec![ContentBlock::text("Multi-round flow complete")])
                        .into(),
                )
            }
            "test_input_required_result_capabilities" => {
                if responses.is_some() {
                    return Ok(CallToolResult::success(vec![ContentBlock::text(
                        "Capability-aware flow complete",
                    )])
                    .into());
                }
                let capabilities = context.client_capabilities().unwrap_or_default();
                let mut requests = InputRequests::new();
                if capabilities.elicitation.is_some() {
                    requests.insert(
                        "user_name".to_owned(),
                        elicitation_request(
                            "What is your name?",
                            json!({ "name": { "type": "string" } }),
                            json!(["name"]),
                        ),
                    );
                }
                if capabilities.sampling.is_some() {
                    requests.insert(
                        "greeting".to_owned(),
                        sampling_request("Generate a greeting"),
                    );
                }
                if capabilities.roots.is_some() {
                    requests.insert("client_roots".to_owned(), roots_request());
                }
                if requests.is_empty() {
                    Ok(CallToolResult::success(vec![ContentBlock::text(
                        "Client declared no MRTR-capable capabilities",
                    )])
                    .into())
                } else {
                    Ok(InputRequiredResult::from_input_requests(requests).into())
                }
            }
            _ => Err(ErrorData::invalid_params("Unknown MRTR fixture", None)),
        }
    }
}

impl ServerHandler for ConformanceServer {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        conformance_tools()
            .into_iter()
            .find(|tool| tool.name == name)
    }

    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_prompts()
                .enable_resources()
                .enable_tools()
                .build(),
        )
        .with_server_info(Implementation::new(
            "stateknot-conformance-server",
            env!("CARGO_PKG_VERSION"),
        ))
        .with_instructions("StateKnot MCP server conformance fixture")
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        Ok(ListToolsResult {
            tools: conformance_tools(),
            ..Default::default()
        }
        .with_ttl_ms(CACHE_TTL_MS)
        .with_cache_scope(CacheScope::Public))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if request.name.starts_with("test_input_required_result_") {
            return self.call_mrtr(request, &context).await;
        }
        let arguments = request.arguments.unwrap_or_default();
        let result = match request.name.as_ref() {
            "test_simple_text" => CallToolResult::success(vec![ContentBlock::text(
                "This is a simple text response for testing.",
            )]),
            "test_image_content" => {
                CallToolResult::success(vec![ContentBlock::image(TEST_IMAGE_DATA, "image/png")])
            }
            "test_audio_content" => {
                CallToolResult::success(vec![ContentBlock::audio(TEST_AUDIO_DATA, "audio/wav")])
            }
            "test_embedded_resource" => CallToolResult::success(vec![ContentBlock::resource(
                ResourceContents::TextResourceContents {
                    uri: "test://embedded-resource".to_owned(),
                    mime_type: Some("text/plain".to_owned()),
                    text: "This is an embedded resource content.".to_owned(),
                    meta: None,
                },
            )]),
            "test_multiple_content_types" => CallToolResult::success(vec![
                ContentBlock::text("Multiple content types test:"),
                ContentBlock::image(TEST_IMAGE_DATA, "image/png"),
                ContentBlock::resource(ResourceContents::TextResourceContents {
                    uri: "test://mixed-content-resource".to_owned(),
                    mime_type: Some("application/json".to_owned()),
                    text: r#"{"test":"data","value":123}"#.to_owned(),
                    meta: None,
                }),
            ]),
            "test_error_handling" => CallToolResult::error(vec![ContentBlock::text(
                "This tool intentionally returns an error for testing",
            )]),
            "test_tool_with_progress" => {
                if let Some(token) = context.meta.get_progress_token() {
                    for (progress, message) in
                        [(0.0, "Starting"), (50.0, "Halfway"), (100.0, "Complete")]
                    {
                        let _ = context
                            .peer
                            .notify_progress(
                                ProgressNotificationParam::new(token.clone(), progress)
                                    .with_total(100.0)
                                    .with_message(message),
                            )
                            .await;
                    }
                }
                CallToolResult::success(vec![ContentBlock::text("Progress test completed")])
            }
            "test_missing_capability" => {
                let capabilities = context.client_capabilities().unwrap_or_default();
                if capabilities.sampling.is_none() {
                    return Err(ErrorData::missing_required_client_capability(
                        ClientCapabilities::builder().enable_sampling().build(),
                    ));
                }
                CallToolResult::success(vec![ContentBlock::text("Required capability declared")])
            }
            "test_streaming_elicitation" => {
                let requests = BTreeMap::from([(
                    "streaming_elicitation".to_owned(),
                    elicitation_request(
                        "Provide a value",
                        json!({ "value": { "type": "string" } }),
                        json!(["value"]),
                    ),
                )]);
                return Ok(InputRequiredResult::from_input_requests(requests).into());
            }
            "test_logging_tool" => {
                if let Some(level) = context.meta.log_level() {
                    let _ = context
                        .peer
                        .notify_logging_message(
                            LoggingMessageNotificationParam::new(
                                level,
                                json!("logLevel was requested"),
                            )
                            .with_logger("stateknot-conformance-server"),
                        )
                        .await;
                }
                CallToolResult::success(vec![ContentBlock::text("Logging tool completed")])
            }
            "test_custom_header" => {
                let value = arguments
                    .get("value")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ErrorData::invalid_params("value must be a string", None))?;
                CallToolResult::success(vec![ContentBlock::text(value)])
            }
            "json_schema_2020_12_tool" => {
                let name = arguments
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("world");
                CallToolResult::success(vec![ContentBlock::text(format!("Hello, {name}!"))])
            }
            _ => {
                return Err(ErrorData::invalid_params(
                    format!("Unknown tool: {}", request.name),
                    None,
                ));
            }
        };
        Ok(result.into())
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        Ok(ListResourcesResult {
            resources: vec![
                Resource::new("test://static-text", "Static Text Resource")
                    .with_description("A static text resource for testing")
                    .with_mime_type("text/plain"),
                Resource::new("test://static-binary", "Static Binary Resource")
                    .with_description("A static binary resource for testing")
                    .with_mime_type("image/png"),
            ],
            ..Default::default()
        }
        .with_ttl_ms(CACHE_TTL_MS)
        .with_cache_scope(CacheScope::Public))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let uri = request.uri.as_str();
        let contents = match uri {
            "test://static-text" => vec![ResourceContents::TextResourceContents {
                uri: uri.to_owned(),
                mime_type: Some("text/plain".to_owned()),
                text: "This is the content of the static text resource.".to_owned(),
                meta: None,
            }],
            "test://static-binary" => vec![ResourceContents::BlobResourceContents {
                uri: uri.to_owned(),
                mime_type: Some("image/png".to_owned()),
                blob: TEST_IMAGE_DATA.to_owned(),
                meta: None,
            }],
            _ if uri.starts_with("test://template/") && uri.ends_with("/data") => {
                let id = uri
                    .strip_prefix("test://template/")
                    .and_then(|value| value.strip_suffix("/data"))
                    .unwrap_or("unknown");
                vec![ResourceContents::TextResourceContents {
                    uri: uri.to_owned(),
                    mime_type: Some("application/json".to_owned()),
                    text: format!(
                        r#"{{"id":"{id}","templateTest":true,"data":"Data for ID: {id}"}}"#
                    ),
                    meta: None,
                }]
            }
            _ => {
                return Err(ErrorData::resource_not_found(
                    format!("Resource not found: {uri}"),
                    Some(json!({ "uri": uri })),
                ));
            }
        };
        Ok(ReadResourceResult::new(contents)
            .with_ttl_ms(CACHE_TTL_MS)
            .with_cache_scope(CacheScope::Public)
            .into())
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        Ok(ListResourceTemplatesResult {
            resource_templates: vec![
                ResourceTemplate::new("test://template/{id}/data", "Dynamic Resource")
                    .with_description("A dynamic resource with parameter substitution")
                    .with_mime_type("application/json"),
            ],
            ..Default::default()
        }
        .with_ttl_ms(CACHE_TTL_MS)
        .with_cache_scope(CacheScope::Public))
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, ErrorData> {
        Ok(ListPromptsResult {
            prompts: vec![
                Prompt::new(
                    "test_simple_prompt",
                    Some("A simple test prompt with no arguments"),
                    None,
                ),
                Prompt::new(
                    "test_prompt_with_arguments",
                    Some("A test prompt that accepts arguments"),
                    Some(vec![
                        PromptArgument::new("arg1")
                            .with_description("First test argument")
                            .with_required(true),
                        PromptArgument::new("arg2")
                            .with_description("Second test argument")
                            .with_required(false),
                    ]),
                ),
                Prompt::new(
                    "test_prompt_with_embedded_resource",
                    Some("A test prompt that includes an embedded resource"),
                    None,
                ),
                Prompt::new(
                    "test_prompt_with_image",
                    Some("A test prompt that includes an image"),
                    None,
                ),
                Prompt::new(
                    "test_input_required_result_prompt",
                    Some("A prompt that requires elicitation input via MRTR"),
                    None,
                ),
            ],
            ..Default::default()
        }
        .with_ttl_ms(CACHE_TTL_MS)
        .with_cache_scope(CacheScope::Public))
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        if request.name == "test_input_required_result_prompt" {
            if let Some(response) = input_response(request.input_responses.as_ref(), "user_context")
            {
                let value = response
                    .get("content")
                    .and_then(|content| content.get("context"))
                    .and_then(Value::as_str)
                    .unwrap_or("(no context)");
                return Ok(GetPromptResult::new(vec![PromptMessage::new_text(
                    Role::User,
                    format!("Prompt with elicited context: {value}"),
                )])
                .with_description("A prompt built from elicited context")
                .into());
            }
            let requests = BTreeMap::from([(
                "user_context".to_owned(),
                elicitation_request(
                    "What context should the prompt use?",
                    json!({ "context": { "type": "string" } }),
                    json!(["context"]),
                ),
            )]);
            return Ok(InputRequiredResult::from_input_requests(requests).into());
        }

        let result = match request.name.as_str() {
            "test_simple_prompt" => GetPromptResult::new(vec![PromptMessage::new_text(
                Role::User,
                "This is a simple test prompt.",
            )])
            .with_description("A simple test prompt"),
            "test_prompt_with_arguments" => {
                let arguments = request.arguments.unwrap_or_default();
                let arg1 = arguments.get("arg1").and_then(Value::as_str).unwrap_or("");
                let arg2 = arguments.get("arg2").and_then(Value::as_str).unwrap_or("");
                GetPromptResult::new(vec![PromptMessage::new_text(
                    Role::User,
                    format!("Prompt with arguments: arg1='{arg1}', arg2='{arg2}'"),
                )])
                .with_description("A prompt with arguments")
            }
            "test_prompt_with_embedded_resource" => GetPromptResult::new(vec![
                PromptMessage::new_text(Role::User, "Here is a resource:"),
                PromptMessage::new_resource(
                    Role::User,
                    "test://static-text".to_owned(),
                    Some("text/plain".to_owned()),
                    Some("Resource content for prompt".to_owned()),
                    None,
                    None,
                    None,
                ),
            ])
            .with_description("A prompt with an embedded resource"),
            "test_prompt_with_image" => GetPromptResult::new(vec![
                PromptMessage::new_text(Role::User, "Here is an image:"),
                PromptMessage::new(
                    Role::User,
                    ContentBlock::image(TEST_IMAGE_DATA, "image/png"),
                ),
            ])
            .with_description("A prompt with an image"),
            _ => {
                return Err(ErrorData::invalid_params(
                    format!("Unknown prompt: {}", request.name),
                    None,
                ));
            }
        };
        Ok(result.into())
    }

    async fn complete(
        &self,
        request: CompleteRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, ErrorData> {
        let values = match &request.r#ref {
            Reference::Resource(_) if request.argument.name == "id" => {
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()]
            }
            Reference::Prompt(_) if request.argument.name == "name" => {
                vec!["Alice".to_owned(), "Bob".to_owned(), "Charlie".to_owned()]
            }
            Reference::Prompt(_) if request.argument.name == "style" => vec![
                "friendly".to_owned(),
                "formal".to_owned(),
                "casual".to_owned(),
            ],
            _ => Vec::new(),
        };
        Ok(CompleteResult::new(CompletionInfo::new(values).map_err(
            |_| ErrorData::internal_error("completion result invalid", None),
        )?))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::var("STATEKNOT_MCP_SERVER_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8001);
    let address = format!("127.0.0.1:{port}");
    let service = McpServerHttpService::new(
        ConformanceServer::new(),
        McpServerHttpOptions::loopback(port)?,
        McpServerAuthentication::anonymous_loopback(),
    )?;
    let router = axum::Router::new().nest_service("/mcp", service);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("StateKnot MCP conformance server listening on http://{address}/mcp");
    axum::serve(listener, router).await?;
    Ok(())
}
