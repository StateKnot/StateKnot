// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Private stateless MCP request metadata, response validation, and checked IDs.

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
    McpCachePolicy, McpClientIdentity, McpClientServer, McpNotification, McpRemoteError,
    StatelessMcpClientError,
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
