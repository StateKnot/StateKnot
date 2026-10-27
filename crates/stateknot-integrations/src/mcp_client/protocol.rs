// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Private MCP request metadata, response and Tool-outcome decoding, and checked IDs.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde_json::{Map, Value, json};

use super::{
    MAX_IDENTITY_COMPONENT_BYTES, MCP_META_SERVER_INFO, MCP_PROTOCOL_VERSION_2026_07_28,
    McpCachePolicy, McpClientIdentity, McpClientServer, McpCompleteToolResult, McpNotification,
    McpRemoteError, StatelessMcpClientError,
};

const MCP_META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const MCP_META_CLIENT_INFO: &str = "io.modelcontextprotocol/clientInfo";
const MCP_META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
const MAX_PROTOCOL_VERSIONS: usize = 64;
const MAX_PROTOCOL_VERSION_BYTES: usize = 128;
const MAX_REMOTE_ERROR_MESSAGE_BYTES: usize = 4096;

static NEXT_CLIENT_BINDING_ID: AtomicU64 = AtomicU64::new(1);

pub(super) enum ParsedMessage {
    Result(Value),
    Remote(McpRemoteError),
    Notification(McpNotification),
}

// HTTP and future stdio callers bind continuation ownership after wire decoding.
// This value contains no client, owner, credentials, or transport policy.
pub(super) enum DecodedToolResult {
    Complete(McpCompleteToolResult),
    InputRequired {
        input_requests: Map<String, Value>,
        request_state: Option<Box<str>>,
    },
}

#[derive(Debug)]
pub(super) struct ToolResultDecodeError;

pub(super) fn allocate_monotonic_id(counter: &AtomicU64) -> Option<u64> {
    crate::atomic_counter::allocate(counter, u64::MAX, Ordering::Relaxed, Ordering::Relaxed)
}

pub(super) fn allocate_client_binding_id() -> Option<u64> {
    allocate_monotonic_id(&NEXT_CLIENT_BINDING_ID)
}

pub(super) fn request_metadata(
    identity: &McpClientIdentity,
    skills_enabled: bool,
) -> Map<String, Value> {
    let mut metadata = Map::new();
    metadata.insert(
        MCP_META_PROTOCOL_VERSION.to_owned(),
        Value::String(MCP_PROTOCOL_VERSION_2026_07_28.to_owned()),
    );
    metadata.insert(
        MCP_META_CLIENT_INFO.to_owned(),
        json!({
            "name": identity.name(),
            "version": identity.version(),
        }),
    );
    let client_capabilities = if skills_enabled {
        json!({
            "extensions": {
                (crate::MCP_SKILLS_EXTENSION_ID): {}
            }
        })
    } else {
        Value::Object(Map::new())
    };
    metadata.insert(MCP_META_CLIENT_CAPABILITIES.to_owned(), client_capabilities);
    metadata
}

pub(super) fn classify_rpc_message(
    value: Value,
    expected_id: u64,
) -> Result<ParsedMessage, StatelessMcpClientError> {
    let object = value.as_object().ok_or(StatelessMcpClientError::Protocol)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(StatelessMcpClientError::Protocol);
    }
    if let Some(method) = object.get("method").and_then(Value::as_str) {
        if object.contains_key("id")
            || object.contains_key("result")
            || object.contains_key("error")
        {
            return Err(StatelessMcpClientError::Protocol);
        }
        return Ok(ParsedMessage::Notification(McpNotification {
            method: method.to_owned().into_boxed_str(),
            params: object.get("params").cloned(),
            raw: value,
        }));
    }
    if object.get("id") != Some(&Value::from(expected_id)) {
        return Err(StatelessMcpClientError::Protocol);
    }
    match (object.get("result"), object.get("error")) {
        (Some(result), None) => Ok(ParsedMessage::Result(result.clone())),
        (None, Some(error)) => Ok(ParsedMessage::Remote(parse_remote_error(error)?)),
        _ => Err(StatelessMcpClientError::Protocol),
    }
}

fn parse_remote_error(value: &Value) -> Result<McpRemoteError, StatelessMcpClientError> {
    let object = value.as_object().ok_or(StatelessMcpClientError::Protocol)?;
    let code = object
        .get("code")
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .ok_or(StatelessMcpClientError::Protocol)?;
    let message = object
        .get("message")
        .and_then(Value::as_str)
        .ok_or(StatelessMcpClientError::Protocol)?;
    if message.len() > MAX_REMOTE_ERROR_MESSAGE_BYTES {
        return Err(StatelessMcpClientError::Protocol);
    }
    Ok(McpRemoteError {
        code,
        message: message.to_owned().into_boxed_str(),
        data: object.get("data").cloned(),
    })
}

pub(super) fn parse_discovery(value: &Value) -> Result<McpClientServer, StatelessMcpClientError> {
    let object = value
        .as_object()
        .ok_or(StatelessMcpClientError::InvalidDiscovery)?;
    validate_complete_result_type(object, StatelessMcpClientError::InvalidDiscovery)?;
    let versions = object
        .get("supportedVersions")
        .and_then(Value::as_array)
        .ok_or(StatelessMcpClientError::InvalidDiscovery)?;
    if versions.is_empty() || versions.len() > MAX_PROTOCOL_VERSIONS {
        return Err(StatelessMcpClientError::InvalidDiscovery);
    }
    let mut supported_versions = Vec::with_capacity(versions.len());
    let mut unique_versions = HashSet::new();
    for version in versions {
        let version = version
            .as_str()
            .filter(|value| !value.is_empty() && value.len() <= MAX_PROTOCOL_VERSION_BYTES)
            .ok_or(StatelessMcpClientError::InvalidDiscovery)?;
        if !unique_versions.insert(version) {
            return Err(StatelessMcpClientError::InvalidDiscovery);
        }
        supported_versions.push(version.to_owned().into_boxed_str());
    }
    let capabilities = object
        .get("capabilities")
        .and_then(Value::as_object)
        .cloned()
        .ok_or(StatelessMcpClientError::InvalidDiscovery)?;
    let server_info = object
        .get("_meta")
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get(MCP_META_SERVER_INFO))
        .or_else(|| object.get("serverInfo"));
    let (name, version) = match server_info {
        Some(value) => {
            let implementation = value
                .as_object()
                .ok_or(StatelessMcpClientError::InvalidDiscovery)?;
            let name =
                bounded_optional_string(implementation.get("name"), MAX_IDENTITY_COMPONENT_BYTES)
                    .map_err(|_| StatelessMcpClientError::InvalidDiscovery)?;
            let version = bounded_optional_string(
                implementation.get("version"),
                MAX_IDENTITY_COMPONENT_BYTES,
            )
            .map_err(|_| StatelessMcpClientError::InvalidDiscovery)?;
            (name, version)
        }
        None => (None, None),
    };
    let instructions = bounded_optional_string(object.get("instructions"), 64 * 1024)
        .map_err(|_| StatelessMcpClientError::InvalidDiscovery)?;
    Ok(McpClientServer {
        supported_versions: supported_versions.into(),
        capabilities: Arc::new(capabilities),
        name,
        version,
        instructions,
        cache: parse_cache_policy(object).map_err(|_| StatelessMcpClientError::InvalidDiscovery)?,
    })
}

fn bounded_optional_string(
    value: Option<&Value>,
    maximum_bytes: usize,
) -> Result<Option<Box<str>>, StatelessMcpClientError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value
        .as_str()
        .filter(|text| text.len() <= maximum_bytes)
        .ok_or(StatelessMcpClientError::Protocol)?;
    Ok(Some(value.to_owned().into_boxed_str()))
}

pub(super) fn parse_cache_policy(
    object: &Map<String, Value>,
) -> Result<McpCachePolicy, StatelessMcpClientError> {
    let ttl_ms = match object.get("ttlMs") {
        Some(value) => Some(value.as_u64().ok_or(StatelessMcpClientError::Protocol)?),
        None => None,
    };
    let scope = bounded_optional_string(object.get("cacheScope"), 64)?;
    Ok(McpCachePolicy { ttl_ms, scope })
}

pub(super) fn validate_complete_result_type(
    object: &Map<String, Value>,
    error: StatelessMcpClientError,
) -> Result<(), StatelessMcpClientError> {
    if object
        .get("resultType")
        .is_some_and(|value| value.as_str() != Some("complete"))
    {
        Err(error)
    } else {
        Ok(())
    }
}

pub(super) fn decode_tool_result(value: Value) -> Result<DecodedToolResult, ToolResultDecodeError> {
    let object = value.as_object().ok_or(ToolResultDecodeError)?;
    let result_type = match object.get("resultType") {
        Some(value) => Some(value.as_str().ok_or(ToolResultDecodeError)?),
        None => None,
    };
    match result_type {
        None | Some("complete") => {
            let content = object
                .get("content")
                .and_then(Value::as_array)
                .cloned()
                .ok_or(ToolResultDecodeError)?;
            let is_error = match object.get("isError") {
                Some(value) => value.as_bool().ok_or(ToolResultDecodeError)?,
                None => false,
            };
            Ok(DecodedToolResult::Complete(McpCompleteToolResult {
                content,
                structured_content: object.get("structuredContent").cloned(),
                is_error,
                raw: value,
            }))
        }
        Some("input_required") => {
            let input_requests = match object.get("inputRequests") {
                Some(value) => value.as_object().cloned().ok_or(ToolResultDecodeError)?,
                None => Map::new(),
            };
            for (key, request) in &input_requests {
                let request = request.as_object().ok_or(ToolResultDecodeError)?;
                if key.is_empty()
                    || key.len() > 1024
                    || !matches!(
                        request.get("method").and_then(Value::as_str),
                        Some("elicitation/create" | "sampling/createMessage" | "roots/list")
                    )
                {
                    return Err(ToolResultDecodeError);
                }
            }
            let request_state = match object.get("requestState") {
                Some(value) => Some(
                    value
                        .as_str()
                        .ok_or(ToolResultDecodeError)?
                        .to_owned()
                        .into_boxed_str(),
                ),
                None => None,
            };
            Ok(DecodedToolResult::InputRequired {
                input_requests,
                request_state,
            })
        }
        Some(_) => Err(ToolResultDecodeError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_outcomes_preserve_untrusted_values_and_tool_errors() {
        for (raw, expected_error) in [
            (json!({"content": []}), false),
            (
                json!({
                    "resultType": "complete",
                    "content": [{"type": "vendor/example", "value": "opaque 世界"}],
                    "structuredContent": {"ok": true},
                    "isError": false,
                    "vendor_extension": {"keep": [null, 17]}
                }),
                false,
            ),
            (
                json!({
                    "resultType": "complete",
                    "content": [{"type": "text", "text": "tool failed"}],
                    "structuredContent": null,
                    "isError": true
                }),
                true,
            ),
        ] {
            let DecodedToolResult::Complete(result) = decode_tool_result(raw.clone()).unwrap()
            else {
                panic!("expected a complete outcome");
            };
            assert_eq!(result.is_error(), expected_error);
            assert_eq!(
                result.content(),
                raw["content"].as_array().unwrap().as_slice()
            );
            assert_eq!(result.structured_content(), raw.get("structuredContent"));
            assert_eq!(result.raw(), &raw);
        }
    }

    #[test]
    fn input_required_preserves_supported_requests_and_opaque_state() {
        let state = "  opaque:世界/\\n/\"/e\u{301}  ";
        let raw = json!({
            "resultType": "input_required",
            "requestState": state,
            "inputRequests": {
                "consent": {"method": "elicitation/create", "params": {"message": "ask"}},
                "sample": {"method": "sampling/createMessage", "params": {"messages": []}},
                ("r".repeat(1024)): {"method": "roots/list", "params": {"extension": true}}
            }
        });
        let DecodedToolResult::InputRequired {
            input_requests,
            request_state,
        } = decode_tool_result(raw.clone()).unwrap()
        else {
            panic!("expected an input-required outcome");
        };
        assert_eq!(&input_requests, raw["inputRequests"].as_object().unwrap());
        assert_eq!(request_state.as_deref(), Some(state));
    }

    #[test]
    fn input_required_retains_existing_empty_defaults() {
        for raw in [
            json!({"resultType": "input_required"}),
            json!({"resultType": "input_required", "inputRequests": {}, "requestState": ""}),
        ] {
            let DecodedToolResult::InputRequired {
                input_requests,
                request_state,
            } = decode_tool_result(raw.clone()).unwrap()
            else {
                panic!("expected an input-required outcome");
            };
            assert!(input_requests.is_empty());
            assert_eq!(
                request_state.as_deref(),
                raw.get("requestState").and_then(Value::as_str)
            );
        }
    }

    #[test]
    fn malformed_outcomes_are_rejected_before_continuation_binding() {
        let malformed = [
            ("null root", Value::Null),
            ("array root", json!([])),
            ("string root", json!("result")),
            (
                "null result type",
                json!({"resultType": null, "content": []}),
            ),
            (
                "numeric result type",
                json!({"resultType": 7, "content": []}),
            ),
            (
                "unknown result type",
                json!({"resultType": "partial", "content": []}),
            ),
            ("missing content", json!({"resultType": "complete"})),
            ("object content", json!({"content": {}})),
            (
                "non-boolean tool error",
                json!({"content": [], "isError": "false"}),
            ),
            (
                "null requests",
                json!({"resultType": "input_required", "inputRequests": null}),
            ),
            (
                "array requests",
                json!({"resultType": "input_required", "inputRequests": []}),
            ),
            (
                "non-object request",
                json!({"resultType": "input_required", "inputRequests": {"a": 1}}),
            ),
            (
                "missing method",
                json!({"resultType": "input_required", "inputRequests": {"a": {}}}),
            ),
            (
                "unsupported method",
                json!({"resultType": "input_required", "inputRequests": {"a": {"method": "tools/call"}}}),
            ),
            (
                "empty key",
                json!({"resultType": "input_required", "inputRequests": {"": {"method": "roots/list"}}}),
            ),
            (
                "long key",
                json!({"resultType": "input_required", "inputRequests": {("a".repeat(1025)): {"method": "roots/list"}}}),
            ),
            (
                "long UTF-8 key",
                json!({"resultType": "input_required", "inputRequests": {("界".repeat(342)): {"method": "roots/list"}}}),
            ),
            (
                "non-string state",
                json!({"resultType": "input_required", "requestState": {"opaque": true}}),
            ),
        ];
        for (case, raw) in malformed {
            assert!(decode_tool_result(raw).is_err(), "{case}");
        }
    }
}
