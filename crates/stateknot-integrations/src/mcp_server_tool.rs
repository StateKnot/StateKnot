// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Immutable, schema-validating MCP 2026-07-28 Tool server application layer.
//!
//! The public API in this module is owned by `StateKnot`. The official MCP Rust
//! SDK remains a private wire adapter so an SDK release cannot silently become
//! the framework's domain model. Definitions and JSON Schema validators are
//! frozen at startup; request dispatch performs decoded authorization, bounded
//! input validation, handler execution, bounded output validation, and no
//! network schema retrieval.

use std::{borrow::Cow, collections::BTreeMap, fmt, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use jsonschema::Validator;
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CacheScope, CallToolRequestParams, CallToolResponse, CallToolResult, ClientCapabilities,
        ContentBlock, DiscoverResult, ElicitationCapability, Implementation, InputRequest,
        InputRequiredResult, ListToolsResult, PaginatedRequestParams, ProgressNotificationParam,
        ProtocolVersion, RequestStateCodec, RootsCapabilities, SamplingCapability, SealOptions,
        ServerCapabilities, ServerInfo, Tool, ToolAnnotations,
    },
    service::{Peer, RequestContext},
};
use serde_json::{Map, Value};
use stateknot_core::{BoxFuture, Digest};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{McpServerPrincipal, mcp_server_principal};

const JSON_SCHEMA_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";
const MEBIBYTE: usize = 1024 * 1024;
const MAX_TOOL_NAME_BYTES: usize = 64;
const MAX_DISPLAY_BYTES: usize = 16 * 1024;
const MAX_SCOPE_COUNT: usize = 128;
const MAX_SCOPE_BYTES: usize = 256;
const MAX_CONTENT_BLOCK_BYTES: usize = 16 * MEBIBYTE;
const MAX_BINARY_CONTENT_BYTES: usize = 12 * MEBIBYTE;
const MAX_TEXT_CONTENT_BYTES: usize = 4 * MEBIBYTE;
const MAX_CONTENT_BLOCKS: usize = 256;
const MAX_STRUCTURED_CONTENT_BYTES: usize = 16 * MEBIBYTE;
const MAX_TOOL_RESULT_BYTES: usize = 32 * MEBIBYTE;
const MAX_INPUT_REQUEST_BYTES: usize = 2 * MEBIBYTE;
const MAX_INPUT_REQUESTS: usize = 128;
const MAX_INPUT_REQUEST_ID_BYTES: usize = 128;
const MAX_REQUEST_STATE_BYTES: usize = 128 * 1024;
const MAX_REQUEST_STATE_PAYLOAD_BYTES: usize = 64 * 1024;
const MAX_REQUEST_STATE_BINDING_BYTES: usize = 16 * 1024;
const MAX_REQUEST_STATE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_PROGRESS_MESSAGE_BYTES: usize = 2048;

/// MCP cache scope without exposing the wire SDK type.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpServerCacheScope {
    /// The catalog is identical across authorization contexts.
    Public,
    /// The catalog is scoped to the requesting principal.
    #[default]
    Private,
}

impl From<McpServerCacheScope> for CacheScope {
    fn from(value: McpServerCacheScope) -> Self {
        match value {
            McpServerCacheScope::Public => Self::Public,
            McpServerCacheScope::Private => Self::Private,
        }
    }
}

/// Non-authoritative MCP Tool annotations.
///
/// These values are hints for clients. `StateKnot` never uses them as policy or
/// side-effect evidence; authoritative policy belongs in the decoded
/// authorization boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct McpServerToolAnnotations {
    title: Option<Box<str>>,
    read_only_hint: Option<bool>,
    destructive_hint: Option<bool>,
    idempotent_hint: Option<bool>,
    open_world_hint: Option<bool>,
}

impl McpServerToolAnnotations {
    /// Creates annotations with no hints.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            title: None,
            read_only_hint: None,
            destructive_hint: None,
            idempotent_hint: None,
            open_world_hint: None,
        }
    }

    /// Adds a bounded display title.
    pub fn with_title(
        mut self,
        title: impl Into<String>,
    ) -> Result<Self, McpServerToolDefinitionError> {
        let title = title.into();
        validate_display(&title)?;
        self.title = Some(title.into_boxed_str());
        Ok(self)
    }

    /// Sets the protocol read-only hint.
    #[must_use]
    pub const fn with_read_only_hint(mut self, value: bool) -> Self {
        self.read_only_hint = Some(value);
        self
    }

    /// Sets the protocol destructive hint.
    #[must_use]
    pub const fn with_destructive_hint(mut self, value: bool) -> Self {
        self.destructive_hint = Some(value);
        self
    }

    /// Sets the protocol idempotent hint.
    #[must_use]
    pub const fn with_idempotent_hint(mut self, value: bool) -> Self {
        self.idempotent_hint = Some(value);
        self
    }

    /// Sets the protocol open-world hint.
    #[must_use]
    pub const fn with_open_world_hint(mut self, value: bool) -> Self {
        self.open_world_hint = Some(value);
        self
    }

    fn to_protocol(&self) -> ToolAnnotations {
        ToolAnnotations::from_raw(
            self.title.as_deref().map(str::to_owned),
            self.read_only_hint,
            self.destructive_hint,
            self.idempotent_hint,
            self.open_world_hint,
        )
    }
}

/// Immutable StateKnot-owned MCP Tool definition.
#[derive(Clone)]
pub struct McpServerToolDefinition {
    name: Arc<str>,
    title: Option<Arc<str>>,
    description: Option<Arc<str>>,
    input_schema: Arc<Value>,
    output_schema: Option<Arc<Value>>,
    annotations: Option<McpServerToolAnnotations>,
    required_scopes: Arc<[Box<str>]>,
    canonical_schema_bytes: usize,
}

impl McpServerToolDefinition {
    /// Creates a definition after validating its name and input schema.
    ///
    /// JSON Schema defaults to draft 2020-12 when `$schema` is absent. If the
    /// keyword is present it must name that exact dialect. Network `$ref`
    /// retrieval is not attempted; unresolved references fail registry build.
    pub fn new(
        name: impl Into<String>,
        input_schema: Value,
    ) -> Result<Self, McpServerToolDefinitionError> {
        let name = name.into();
        validate_tool_name(&name)?;
        let canonical_schema_bytes = validate_schema_document(&input_schema)?;
        Ok(Self {
            name: Arc::from(name),
            title: None,
            description: None,
            input_schema: Arc::new(input_schema),
            output_schema: None,
            annotations: None,
            required_scopes: Arc::from([]),
            canonical_schema_bytes,
        })
    }

    /// Adds a bounded human-readable title.
    pub fn with_title(
        mut self,
        title: impl Into<String>,
    ) -> Result<Self, McpServerToolDefinitionError> {
        let title = title.into();
        validate_display(&title)?;
        self.title = Some(Arc::from(title));
        Ok(self)
    }

    /// Adds a bounded human-readable description.
    pub fn with_description(
        mut self,
        description: impl Into<String>,
    ) -> Result<Self, McpServerToolDefinitionError> {
        let description = description.into();
        validate_display(&description)?;
        self.description = Some(Arc::from(description));
        Ok(self)
    }

    /// Adds and validates an output schema.
    pub fn with_output_schema(
        mut self,
        output_schema: Value,
    ) -> Result<Self, McpServerToolDefinitionError> {
        let bytes = validate_schema_document(&output_schema)?;
        self.canonical_schema_bytes = self
            .canonical_schema_bytes
            .checked_add(bytes)
            .ok_or(McpServerToolDefinitionError::SchemaTooLarge)?;
        if self.canonical_schema_bytes > McpServerToolRegistryLimits::HARD_MAXIMUM_SCHEMA_BYTES * 2
        {
            return Err(McpServerToolDefinitionError::SchemaTooLarge);
        }
        self.output_schema = Some(Arc::new(output_schema));
        Ok(self)
    }

    /// Adds non-authoritative client hints.
    #[must_use]
    pub fn with_annotations(mut self, annotations: McpServerToolAnnotations) -> Self {
        self.annotations = Some(annotations);
        self
    }

    /// Requires every listed OAuth-style scope before dispatch.
    pub fn with_required_scopes(
        mut self,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, McpServerToolDefinitionError> {
        let mut scopes = scopes.into_iter().map(Into::into).collect::<Vec<_>>();
        if scopes.len() > MAX_SCOPE_COUNT {
            return Err(McpServerToolDefinitionError::TooManyScopes);
        }
        for scope in &scopes {
            validate_scope(scope)?;
        }
        scopes.sort_unstable();
        if scopes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(McpServerToolDefinitionError::DuplicateScope);
        }
        self.required_scopes = scopes
            .into_iter()
            .map(String::into_boxed_str)
            .collect::<Vec<_>>()
            .into();
        Ok(self)
    }

    /// Returns the stable tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the optional display title.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Returns the optional description.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the exact input schema.
    #[must_use]
    pub fn input_schema(&self) -> &Value {
        &self.input_schema
    }

    /// Returns the optional output schema.
    #[must_use]
    pub fn output_schema(&self) -> Option<&Value> {
        self.output_schema.as_deref()
    }

    /// Returns required scopes in canonical ASCII order.
    pub fn required_scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.required_scopes.iter().map(AsRef::as_ref)
    }

    fn to_protocol(&self) -> Tool {
        let input = self
            .input_schema
            .as_object()
            .expect("validated MCP input schema remains an object")
            .clone();
        let mut tool = Tool::new_with_raw(
            self.name.to_string(),
            self.description
                .as_deref()
                .map(|value| Cow::Owned(value.to_owned())),
            input,
        );
        tool.title = self.title.as_deref().map(str::to_owned);
        if let Some(output) = &self.output_schema {
            tool = tool.with_raw_output_schema(Arc::new(
                output
                    .as_object()
                    .expect("validated MCP output schema remains an object")
                    .clone(),
            ));
        }
        if let Some(annotations) = &self.annotations {
            tool = tool.with_annotations(annotations.to_protocol());
        }
        tool
    }
}

impl fmt::Debug for McpServerToolDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerToolDefinition")
            .field("name", &self.name)
            .field("title", &self.title)
            .field("description", &self.description)
            .field("has_output_schema", &self.output_schema.is_some())
            .field("required_scopes", &self.required_scopes)
            .field("canonical_schema_bytes", &self.canonical_schema_bytes)
            .finish_non_exhaustive()
    }
}

/// Invalid MCP Tool definition.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerToolDefinitionError {
    /// Names must contain 1–64 characters from the SEP-986 portable grammar.
    #[error("invalid MCP tool name")]
    InvalidName,
    /// A title or description was empty, padded, oversized, or contained controls.
    #[error("invalid MCP tool display text")]
    InvalidDisplayText,
    /// A schema root was not a JSON object.
    #[error("MCP tool schema root must be an object")]
    SchemaRootNotObject,
    /// The schema selected a dialect other than JSON Schema 2020-12.
    #[error("MCP tool schema must use JSON Schema 2020-12")]
    UnsupportedSchemaDialect,
    /// The schema violated the 2020-12 meta-schema.
    #[error("invalid MCP JSON Schema 2020-12 document: {diagnostic}")]
    InvalidSchema {
        /// Bounded startup diagnostic.
        diagnostic: Box<str>,
    },
    /// Canonical schema bytes exceeded the implementation ceiling.
    #[error("MCP tool schema exceeds the hard byte ceiling")]
    SchemaTooLarge,
    /// More than the hard required-scope count was supplied.
    #[error("too many required MCP tool scopes")]
    TooManyScopes,
    /// A required scope was outside the OAuth scope-token grammar.
    #[error("invalid required MCP tool scope")]
    InvalidScope,
    /// A required scope appeared more than once.
    #[error("duplicate required MCP tool scope")]
    DuplicateScope,
}

fn validate_tool_name(name: &str) -> Result<(), McpServerToolDefinitionError> {
    if name.is_empty()
        || name.len() > MAX_TOOL_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'/' | b'-'))
    {
        return Err(McpServerToolDefinitionError::InvalidName);
    }
    Ok(())
}

fn validate_display(value: &str) -> Result<(), McpServerToolDefinitionError> {
    if value.is_empty()
        || value.len() > MAX_DISPLAY_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(McpServerToolDefinitionError::InvalidDisplayText);
    }
    Ok(())
}

fn validate_scope(scope: &str) -> Result<(), McpServerToolDefinitionError> {
    if scope.is_empty()
        || scope.len() > MAX_SCOPE_BYTES
        || scope
            .bytes()
            .any(|byte| !matches!(byte, b'!' | b'#'..=b'[' | b']'..=b'~'))
    {
        return Err(McpServerToolDefinitionError::InvalidScope);
    }
    Ok(())
}

fn validate_schema_document(value: &Value) -> Result<usize, McpServerToolDefinitionError> {
    let object = value
        .as_object()
        .ok_or(McpServerToolDefinitionError::SchemaRootNotObject)?;
    if object
        .get("$schema")
        .and_then(Value::as_str)
        .is_some_and(|dialect| dialect != JSON_SCHEMA_2020_12)
    {
        return Err(McpServerToolDefinitionError::UnsupportedSchemaDialect);
    }
    jsonschema::draft202012::meta::validate(value).map_err(|source| {
        McpServerToolDefinitionError::InvalidSchema {
            diagnostic: bounded_diagnostic(&source.to_string()),
        }
    })?;
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| McpServerToolDefinitionError::SchemaTooLarge)?;
    if canonical.len() > McpServerToolRegistryLimits::HARD_MAXIMUM_SCHEMA_BYTES {
        return Err(McpServerToolDefinitionError::SchemaTooLarge);
    }
    Ok(canonical.len())
}

fn bounded_diagnostic(value: &str) -> Box<str> {
    let end = value
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= 2048)
        .last()
        .unwrap_or(0);
    if value.len() <= 2048 {
        value.to_owned().into_boxed_str()
    } else {
        value[..end].to_owned().into_boxed_str()
    }
}

/// Resource ceilings for one immutable Tool registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct McpServerToolRegistryLimits {
    maximum_tools: usize,
    maximum_schema_bytes: usize,
    maximum_total_schema_bytes: usize,
}

impl McpServerToolRegistryLimits {
    /// Absolute tool count ceiling.
    pub const HARD_MAXIMUM_TOOLS: usize = 4096;
    /// Absolute canonical byte ceiling for one input or output schema.
    pub const HARD_MAXIMUM_SCHEMA_BYTES: usize = 8 * MEBIBYTE;
    /// Absolute aggregate schema byte ceiling.
    pub const HARD_MAXIMUM_TOTAL_SCHEMA_BYTES: usize = 256 * MEBIBYTE;

    /// Constructs explicit positive limits within hard ceilings.
    pub const fn new(
        maximum_tools: usize,
        maximum_schema_bytes: usize,
        maximum_total_schema_bytes: usize,
    ) -> Result<Self, McpServerToolRegistryLimitsError> {
        if maximum_tools == 0 || maximum_schema_bytes == 0 || maximum_total_schema_bytes == 0 {
            return Err(McpServerToolRegistryLimitsError::ZeroLimit);
        }
        if maximum_schema_bytes > Self::HARD_MAXIMUM_SCHEMA_BYTES
            || maximum_total_schema_bytes > Self::HARD_MAXIMUM_TOTAL_SCHEMA_BYTES
            || maximum_tools > Self::HARD_MAXIMUM_TOOLS
            || maximum_total_schema_bytes < maximum_schema_bytes
        {
            return Err(McpServerToolRegistryLimitsError::InvalidLimit);
        }
        Ok(Self {
            maximum_tools,
            maximum_schema_bytes,
            maximum_total_schema_bytes,
        })
    }

    /// Returns the tool count ceiling.
    #[must_use]
    pub const fn maximum_tools(self) -> usize {
        self.maximum_tools
    }

    /// Returns the per-schema byte ceiling.
    #[must_use]
    pub const fn maximum_schema_bytes(self) -> usize {
        self.maximum_schema_bytes
    }

    /// Returns the aggregate schema byte ceiling.
    #[must_use]
    pub const fn maximum_total_schema_bytes(self) -> usize {
        self.maximum_total_schema_bytes
    }
}

impl Default for McpServerToolRegistryLimits {
    fn default() -> Self {
        Self {
            maximum_tools: 1024,
            maximum_schema_bytes: 2 * MEBIBYTE,
            maximum_total_schema_bytes: 64 * MEBIBYTE,
        }
    }
}

/// Invalid registry resource policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerToolRegistryLimitsError {
    /// Every resource ceiling must be positive.
    #[error("MCP tool registry limits must be positive")]
    ZeroLimit,
    /// A limit exceeded a hard maximum or aggregate capacity was inconsistent.
    #[error("invalid MCP tool registry limit")]
    InvalidLimit,
}

/// One bounded protocol content block owned by `StateKnot`.
#[derive(Clone, Debug)]
pub struct McpServerContent {
    inner: ContentBlock,
    wire_bytes: usize,
}

impl McpServerContent {
    /// Creates bounded text content.
    pub fn text(text: impl Into<String>) -> Result<Self, McpServerContentError> {
        let text = text.into();
        if text.len() > MAX_TEXT_CONTENT_BYTES {
            return Err(McpServerContentError::TooLarge);
        }
        Self::from_protocol(ContentBlock::text(text))
    }

    /// Creates image content from raw bytes and canonical standard Base64.
    pub fn image(
        data: impl AsRef<[u8]>,
        mime_type: impl Into<String>,
    ) -> Result<Self, McpServerContentError> {
        let data = data.as_ref();
        validate_binary(data)?;
        let mime_type = validate_mime(mime_type.into(), "image")?;
        Self::from_protocol(ContentBlock::image(STANDARD.encode(data), mime_type))
    }

    /// Creates audio content from raw bytes and canonical standard Base64.
    pub fn audio(
        data: impl AsRef<[u8]>,
        mime_type: impl Into<String>,
    ) -> Result<Self, McpServerContentError> {
        let data = data.as_ref();
        validate_binary(data)?;
        let mime_type = validate_mime(mime_type.into(), "audio")?;
        Self::from_protocol(ContentBlock::audio(STANDARD.encode(data), mime_type))
    }

    /// Validates an extension content block represented as protocol JSON.
    ///
    /// Prefer typed constructors for ordinary content. This escape hatch
    /// retains future MCP variants without exposing SDK types.
    pub fn try_from_protocol_json(value: Value) -> Result<Self, McpServerContentError> {
        let wire_bytes = canonical_json_len(&value).map_err(|_| McpServerContentError::Invalid)?;
        if wire_bytes > MAX_CONTENT_BLOCK_BYTES {
            return Err(McpServerContentError::TooLarge);
        }
        let inner: ContentBlock =
            serde_json::from_value(value).map_err(|_| McpServerContentError::Invalid)?;
        validate_protocol_content(&inner)?;
        Ok(Self { inner, wire_bytes })
    }

    fn from_protocol(inner: ContentBlock) -> Result<Self, McpServerContentError> {
        validate_protocol_content(&inner)?;
        let value = serde_json::to_value(&inner).map_err(|_| McpServerContentError::Invalid)?;
        let wire_bytes = canonical_json_len(&value).map_err(|_| McpServerContentError::Invalid)?;
        if wire_bytes > MAX_CONTENT_BLOCK_BYTES {
            return Err(McpServerContentError::TooLarge);
        }
        Ok(Self { inner, wire_bytes })
    }

    pub(crate) fn protocol_clone(&self) -> ContentBlock {
        self.inner.clone()
    }

    pub(crate) const fn wire_bytes(&self) -> usize {
        self.wire_bytes
    }
}

/// Invalid or oversized MCP content.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerContentError {
    /// The content did not match an MCP content block or had invalid media data.
    #[error("invalid MCP content block")]
    Invalid,
    /// The content exceeded a hard byte ceiling.
    #[error("MCP content block is too large")]
    TooLarge,
}

fn validate_binary(data: &[u8]) -> Result<(), McpServerContentError> {
    if data.len() > MAX_BINARY_CONTENT_BYTES {
        return Err(McpServerContentError::TooLarge);
    }
    Ok(())
}

fn validate_mime(value: String, expected_top_level: &str) -> Result<String, McpServerContentError> {
    if value.len() > 255
        || value.trim() != value
        || value
            .parse::<mime::Mime>()
            .map_or(true, |mime| mime.type_().as_str() != expected_top_level)
    {
        return Err(McpServerContentError::Invalid);
    }
    Ok(value)
}

fn validate_protocol_content(content: &ContentBlock) -> Result<(), McpServerContentError> {
    match content {
        ContentBlock::Text(value) => {
            if value.text.len() > MAX_TEXT_CONTENT_BYTES {
                return Err(McpServerContentError::TooLarge);
            }
        }
        ContentBlock::Image(value) => {
            validate_encoded_binary(&value.data)?;
            validate_mime(value.mime_type.clone(), "image")?;
        }
        ContentBlock::Audio(value) => {
            validate_encoded_binary(&value.data)?;
            validate_mime(value.mime_type.clone(), "audio")?;
        }
        ContentBlock::Resource(value) => match &value.resource {
            rmcp::model::ResourceContents::TextResourceContents { uri, text, .. } => {
                validate_resource_uri(uri)?;
                if text.len() > MAX_TEXT_CONTENT_BYTES {
                    return Err(McpServerContentError::TooLarge);
                }
            }
            rmcp::model::ResourceContents::BlobResourceContents { uri, blob, .. } => {
                validate_resource_uri(uri)?;
                validate_encoded_binary(blob)?;
            }
            _ => return Err(McpServerContentError::Invalid),
        },
        ContentBlock::ResourceLink(value) => validate_resource_uri(&value.uri)?,
        _ => return Err(McpServerContentError::Invalid),
    }
    Ok(())
}

fn validate_encoded_binary(value: &str) -> Result<(), McpServerContentError> {
    let decoded = STANDARD
        .decode(value)
        .map_err(|_| McpServerContentError::Invalid)?;
    validate_binary(&decoded)
}

fn validate_resource_uri(value: &str) -> Result<(), McpServerContentError> {
    if value.is_empty()
        || value.len() > 4096
        || value.chars().any(char::is_control)
        || value
            .parse::<http::Uri>()
            .map_or(true, |uri| uri.scheme().is_none())
    {
        return Err(McpServerContentError::Invalid);
    }
    Ok(())
}

/// Bounded complete MCP Tool result.
#[derive(Clone, Debug)]
pub struct McpServerToolResult {
    content: Arc<[McpServerContent]>,
    structured_content: Option<Arc<Value>>,
    is_error: bool,
}

impl McpServerToolResult {
    /// Creates an unstructured successful result.
    pub fn success(
        content: impl IntoIterator<Item = McpServerContent>,
    ) -> Result<Self, McpServerToolResultError> {
        Self::new(content, None, false)
    }

    /// Creates a structured successful result.
    pub fn structured(
        content: impl IntoIterator<Item = McpServerContent>,
        structured_content: Value,
    ) -> Result<Self, McpServerToolResultError> {
        Self::new(content, Some(structured_content), false)
    }

    /// Creates a caller-visible tool-level error.
    pub fn error(
        content: impl IntoIterator<Item = McpServerContent>,
    ) -> Result<Self, McpServerToolResultError> {
        Self::new(content, None, true)
    }

    fn new(
        content: impl IntoIterator<Item = McpServerContent>,
        structured_content: Option<Value>,
        is_error: bool,
    ) -> Result<Self, McpServerToolResultError> {
        let content = content.into_iter().collect::<Vec<_>>();
        if content.len() > MAX_CONTENT_BLOCKS {
            return Err(McpServerToolResultError::TooManyContentBlocks);
        }
        let mut total = content
            .iter()
            .try_fold(0_usize, |total, item| total.checked_add(item.wire_bytes));
        let structured_content = if let Some(value) = structured_content {
            let bytes = canonical_json_len(&value)
                .map_err(|_| McpServerToolResultError::InvalidStructuredContent)?;
            if bytes > MAX_STRUCTURED_CONTENT_BYTES {
                return Err(McpServerToolResultError::TooLarge);
            }
            total = total.and_then(|total| total.checked_add(bytes));
            Some(Arc::new(value))
        } else {
            None
        };
        if total.is_none_or(|total| total > MAX_TOOL_RESULT_BYTES) {
            return Err(McpServerToolResultError::TooLarge);
        }
        Ok(Self {
            content: content.into(),
            structured_content,
            is_error,
        })
    }

    fn into_protocol(self) -> CallToolResult {
        let content = self
            .content
            .iter()
            .map(|value| value.inner.clone())
            .collect();
        let mut result = if self.is_error {
            CallToolResult::error(content)
        } else {
            CallToolResult::success(content)
        };
        result.structured_content = self.structured_content.as_deref().cloned();
        result
    }
}

/// Invalid bounded Tool result.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerToolResultError {
    /// The result contained too many content blocks.
    #[error("too many MCP tool result content blocks")]
    TooManyContentBlocks,
    /// Structured content could not be canonically represented.
    #[error("invalid MCP structured content")]
    InvalidStructuredContent,
    /// The result exceeded a hard byte ceiling.
    #[error("MCP tool result is too large")]
    TooLarge,
}

/// Validated server-initiated MRTR request without an SDK type in the API.
#[derive(Clone, Debug)]
pub struct McpServerInputRequest {
    inner: InputRequest,
}

impl McpServerInputRequest {
    /// Validates one `elicitation/create`, `sampling/createMessage`, or
    /// `roots/list` request represented as protocol JSON.
    pub fn try_from_protocol_json(value: Value) -> Result<Self, McpServerInputRequiredError> {
        if canonical_json_len(&value).map_or(true, |bytes| bytes > MAX_INPUT_REQUEST_BYTES) {
            return Err(McpServerInputRequiredError::InvalidRequest);
        }
        let inner = serde_json::from_value(value)
            .map_err(|_| McpServerInputRequiredError::InvalidRequest)?;
        Ok(Self { inner })
    }
}

/// Bounded MRTR `input_required` result.
#[derive(Clone, Debug)]
pub struct McpServerInputRequired {
    requests: Arc<BTreeMap<String, McpServerInputRequest>>,
    request_state: Option<Box<str>>,
}

impl McpServerInputRequired {
    /// Creates an input-required result with at least one request or state.
    pub fn new(
        requests: BTreeMap<String, McpServerInputRequest>,
        request_state: Option<impl Into<String>>,
    ) -> Result<Self, McpServerInputRequiredError> {
        if requests.len() > MAX_INPUT_REQUESTS {
            return Err(McpServerInputRequiredError::TooManyRequests);
        }
        for id in requests.keys() {
            if id.is_empty()
                || id.len() > MAX_INPUT_REQUEST_ID_BYTES
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
            {
                return Err(McpServerInputRequiredError::InvalidRequestId);
            }
        }
        let request_state = request_state.map(Into::into);
        if requests.is_empty() && request_state.is_none() {
            return Err(McpServerInputRequiredError::Empty);
        }
        if request_state
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_REQUEST_STATE_BYTES)
        {
            return Err(McpServerInputRequiredError::InvalidRequestState);
        }
        Ok(Self {
            requests: Arc::new(requests),
            request_state: request_state.map(String::into_boxed_str),
        })
    }

    pub(crate) fn into_protocol(self) -> InputRequiredResult {
        let requests = if self.requests.is_empty() {
            None
        } else {
            Some(
                self.requests
                    .iter()
                    .map(|(key, value)| (key.clone(), value.inner.clone()))
                    .collect(),
            )
        };
        InputRequiredResult::new(requests, self.request_state.map(Into::into))
    }
}

/// Invalid MRTR input-required result.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerInputRequiredError {
    /// Neither input requests nor request state was supplied.
    #[error("MCP input-required result is empty")]
    Empty,
    /// Too many simultaneous input requests were supplied.
    #[error("too many MCP input requests")]
    TooManyRequests,
    /// A request identifier was invalid.
    #[error("invalid MCP input request identifier")]
    InvalidRequestId,
    /// A request did not match an allowed MRTR request envelope.
    #[error("invalid MCP input request")]
    InvalidRequest,
    /// Request state was empty or oversized.
    #[error("invalid MCP request state")]
    InvalidRequestState,
}

/// StateKnot-owned wrapper around integrity-protected MCP `requestState`.
///
/// Tokens are authenticated, not encrypted. Bind them to
/// [`McpServerToolContext::request_state_binding`] and apply a TTL. Single-use
/// redemption remains application-owned durable state.
#[derive(Clone)]
pub struct McpServerRequestStateCodec {
    inner: RequestStateCodec,
}

impl McpServerRequestStateCodec {
    /// Builds a codec from at least 32 bytes of high-entropy key material.
    pub fn new(key: impl Into<Vec<u8>>) -> Result<Self, McpServerRequestStateCodecBuildError> {
        RequestStateCodec::try_new(key)
            .map(|inner| Self { inner })
            .map_err(|_| McpServerRequestStateCodecBuildError::InvalidConfiguration)
    }

    /// Builds a rotating keyring that emits the active `rs2` key id.
    pub fn with_keyring<K, V>(
        active_key_id: impl Into<String>,
        keys: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self, McpServerRequestStateCodecBuildError>
    where
        K: Into<String>,
        V: Into<Vec<u8>>,
    {
        RequestStateCodec::new_with_keyring(active_key_id, keys)
            .map(|inner| Self { inner })
            .map_err(|_| McpServerRequestStateCodecBuildError::InvalidConfiguration)
    }

    /// Seals bounded JSON with caller binding and mandatory positive TTL.
    pub fn seal_json(
        &self,
        value: &Value,
        associated_data: &[u8],
        ttl: Duration,
    ) -> Result<String, McpServerRequestStateError> {
        validate_state_operation(value, associated_data, ttl)?;
        let options = SealOptions::new().associated_data(associated_data).ttl(ttl);
        let sealed = self
            .inner
            .seal_json_with(value, &options)
            .map_err(|_| McpServerRequestStateError::InvalidState)?;
        if sealed.len() > MAX_REQUEST_STATE_BYTES {
            return Err(McpServerRequestStateError::StateTooLarge);
        }
        Ok(sealed)
    }

    /// Opens bounded JSON with the exact same caller binding.
    ///
    /// All malformed, expired, unknown-key, and integrity failures collapse to
    /// [`McpServerRequestStateError::InvalidState`] to avoid an oracle.
    pub fn open_json(
        &self,
        sealed: &str,
        associated_data: &[u8],
    ) -> Result<Value, McpServerRequestStateError> {
        if sealed.is_empty()
            || sealed.len() > MAX_REQUEST_STATE_BYTES
            || associated_data.len() > MAX_REQUEST_STATE_BINDING_BYTES
        {
            return Err(McpServerRequestStateError::InvalidState);
        }
        let value = self
            .inner
            .open_json_with(sealed, associated_data)
            .map_err(|_| McpServerRequestStateError::InvalidState)?;
        if canonical_json_len(&value).map_or(true, |bytes| bytes > MAX_REQUEST_STATE_PAYLOAD_BYTES)
        {
            return Err(McpServerRequestStateError::InvalidState);
        }
        Ok(value)
    }
}

impl fmt::Debug for McpServerRequestStateCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerRequestStateCodec")
            .field("keys", &"[REDACTED]")
            .finish()
    }
}

/// Invalid request-state key or keyring configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerRequestStateCodecBuildError {
    /// The key was short or the keyring/key identifiers were inconsistent.
    #[error("invalid MCP request-state key configuration")]
    InvalidConfiguration,
}

/// Safe request-state processing failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerRequestStateError {
    /// Payload, associated data, or TTL violated a hard bound.
    #[error("invalid MCP request-state operation")]
    InvalidOperation,
    /// The generated token exceeded the hard transport ceiling.
    #[error("MCP request state is too large")]
    StateTooLarge,
    /// State was malformed, expired, signed by an unknown key, or failed integrity.
    #[error("invalid MCP request state")]
    InvalidState,
}

fn validate_state_operation(
    value: &Value,
    associated_data: &[u8],
    ttl: Duration,
) -> Result<(), McpServerRequestStateError> {
    if ttl.is_zero()
        || ttl > MAX_REQUEST_STATE_TTL
        || associated_data.is_empty()
        || associated_data.len() > MAX_REQUEST_STATE_BINDING_BYTES
        || canonical_json_len(value).map_or(true, |bytes| bytes > MAX_REQUEST_STATE_PAYLOAD_BYTES)
    {
        return Err(McpServerRequestStateError::InvalidOperation);
    }
    Ok(())
}

/// Complete or input-required Tool outcome.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum McpServerToolOutcome {
    /// Tool execution completed, including caller-visible Tool errors.
    Complete(McpServerToolResult),
    /// The client must satisfy MRTR input and retry the same logical call.
    InputRequired(McpServerInputRequired),
}

impl From<McpServerToolResult> for McpServerToolOutcome {
    fn from(value: McpServerToolResult) -> Self {
        Self::Complete(value)
    }
}

impl From<McpServerInputRequired> for McpServerToolOutcome {
    fn from(value: McpServerInputRequired) -> Self {
        Self::InputRequired(value)
    }
}

/// Handler-internal failure category with no arbitrary diagnostic leakage.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerToolHandlerError {
    /// A downstream dependency is temporarily unavailable.
    #[error("MCP tool dependency is unavailable")]
    Unavailable,
    /// Execution observed cooperative cancellation.
    #[error("MCP tool execution was cancelled")]
    Cancelled,
    /// An invariant failed inside the Tool implementation.
    #[error("MCP tool execution failed internally")]
    Internal,
    /// The operation requires a core capability the client did not declare.
    #[error("MCP client is missing a required capability")]
    MissingRequiredClientCapability(McpServerRequiredClientCapability),
}

/// Core client capability a Tool may require before it can continue.
///
/// This StateKnot-owned enum prevents the wire SDK capability model from
/// becoming part of the public application API. Extensions, including Tasks,
/// require their own independently versioned profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpServerRequiredClientCapability {
    /// Interactive Elicitation requests.
    Elicitation,
    /// Server-initiated Sampling requests.
    Sampling,
    /// Client Roots discovery.
    Roots,
}

/// Executes one registered Tool.
pub trait McpServerToolHandler: Send + Sync + 'static {
    /// Executes a validated, authorized request.
    fn call(
        &self,
        call: McpServerToolCall,
        context: McpServerToolContext,
    ) -> BoxFuture<'_, Result<McpServerToolOutcome, McpServerToolHandlerError>>;
}

/// StateKnot-owned inbound Tool call.
#[derive(Clone, Debug)]
pub struct McpServerToolCall {
    name: Arc<str>,
    arguments: Arc<Map<String, Value>>,
    input_responses: Arc<BTreeMap<String, Value>>,
    request_state: Option<Arc<str>>,
}

impl McpServerToolCall {
    /// Returns the registered Tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns syntactically decoded arguments.
    ///
    /// The decoded authorization policy runs before input-schema validation to
    /// avoid exposing schema diagnostics to a denied caller. Authorization
    /// code must therefore treat these values as untrusted.
    #[must_use]
    pub fn arguments(&self) -> &Map<String, Value> {
        &self.arguments
    }

    /// Returns untrusted MRTR responses supplied by the client.
    #[must_use]
    pub fn input_responses(&self) -> &BTreeMap<String, Value> {
        &self.input_responses
    }

    /// Returns untrusted request state echoed by the client.
    #[must_use]
    pub fn request_state(&self) -> Option<&str> {
        self.request_state.as_deref()
    }
}

#[derive(Clone)]
struct McpServerProgressSink {
    peer: Peer<RoleServer>,
    token: rmcp::model::ProgressToken,
}

/// Authenticated, request-scoped Tool execution context.
#[derive(Clone)]
pub struct McpServerToolContext {
    principal: McpServerPrincipal,
    client_name: Option<Arc<str>>,
    client_version: Option<Arc<str>>,
    client_capabilities: Arc<Value>,
    cancellation: CancellationToken,
    progress: Option<McpServerProgressSink>,
    request_state_binding: Arc<[u8]>,
}

impl McpServerToolContext {
    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &McpServerPrincipal {
        &self.principal
    }

    /// Returns the validated client implementation name.
    #[must_use]
    pub fn client_name(&self) -> Option<&str> {
        self.client_name.as_deref()
    }

    /// Returns the validated client implementation version.
    #[must_use]
    pub fn client_version(&self) -> Option<&str> {
        self.client_version.as_deref()
    }

    /// Returns the request-scoped client capability document.
    #[must_use]
    pub fn client_capabilities(&self) -> &Value {
        &self.client_capabilities
    }

    /// Returns associated data bound to principal, Tool, and canonical arguments.
    #[must_use]
    pub fn request_state_binding(&self) -> &[u8] {
        &self.request_state_binding
    }

    /// Returns whether the client cancelled this request.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Waits for cooperative cancellation.
    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    /// Sends bounded progress when the request supplied a progress token.
    ///
    /// Returns `Ok(false)` when the client did not request progress.
    pub async fn report_progress(
        &self,
        progress: f64,
        total: Option<f64>,
        message: Option<&str>,
    ) -> Result<bool, McpServerToolProgressError> {
        if !progress.is_finite()
            || progress < 0.0
            || total.is_some_and(|value| !value.is_finite() || value <= 0.0 || progress > value)
            || message.is_some_and(|value| {
                value.len() > MAX_PROGRESS_MESSAGE_BYTES || value.chars().any(char::is_control)
            })
        {
            return Err(McpServerToolProgressError::InvalidProgress);
        }
        let Some(sink) = &self.progress else {
            return Ok(false);
        };
        let mut notification = ProgressNotificationParam::new(sink.token.clone(), progress);
        if let Some(total) = total {
            notification = notification.with_total(total);
        }
        if let Some(message) = message {
            notification = notification.with_message(message);
        }
        sink.peer
            .notify_progress(notification)
            .await
            .map_err(|_| McpServerToolProgressError::TransportClosed)?;
        Ok(true)
    }
}

impl fmt::Debug for McpServerToolContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerToolContext")
            .field("principal", &self.principal)
            .field("client_name", &self.client_name)
            .field("client_version", &self.client_version)
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("progress_requested", &self.progress.is_some())
            .field("request_state_binding", &"[DIGEST]")
            .finish_non_exhaustive()
    }
}

/// Progress notification failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerToolProgressError {
    /// Progress, total, or message violated protocol bounds.
    #[error("invalid MCP progress notification")]
    InvalidProgress,
    /// The client transport closed before notification delivery.
    #[error("MCP client transport is closed")]
    TransportClosed,
}

/// Owned facts for decoded Tool authorization.
#[derive(Clone, Debug)]
pub struct McpServerToolAuthorizationRequest {
    principal: McpServerPrincipal,
    definition: McpServerToolDefinition,
    call: McpServerToolCall,
}

impl McpServerToolAuthorizationRequest {
    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &McpServerPrincipal {
        &self.principal
    }

    /// Returns the immutable registered definition.
    #[must_use]
    pub const fn definition(&self) -> &McpServerToolDefinition {
        &self.definition
    }

    /// Returns the decoded, resource-bounded call.
    ///
    /// Arguments remain untrusted until authorization succeeds.
    #[must_use]
    pub const fn call(&self) -> &McpServerToolCall {
        &self.call
    }
}

/// Decoded per-call authorization policy.
pub trait McpServerToolAuthorization: Send + Sync + 'static {
    /// Authorizes one call after Tool lookup and resource-bound checks, but
    /// before input-schema validation.
    fn authorize(
        &self,
        request: McpServerToolAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerToolAuthorizationError>>;
}

/// Explicit policy that admits every scope-qualified Tool call.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowMcpServerToolAuthorization;

impl McpServerToolAuthorization for AllowMcpServerToolAuthorization {
    fn authorize(
        &self,
        _request: McpServerToolAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), McpServerToolAuthorizationError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Public-safe decoded authorization failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerToolAuthorizationError {
    /// Policy denied this call.
    #[error("MCP tool call is forbidden")]
    Forbidden,
    /// The policy authority is temporarily unavailable.
    #[error("MCP tool authorization is unavailable")]
    Unavailable,
}

struct PendingTool {
    definition: McpServerToolDefinition,
    handler: Arc<dyn McpServerToolHandler>,
}

struct RegisteredTool {
    definition: McpServerToolDefinition,
    protocol: Tool,
    input_validator: Validator,
    output_validator: Option<Validator>,
    handler: Arc<dyn McpServerToolHandler>,
}

/// Startup-only builder for an immutable executable Tool registry.
pub struct McpServerToolRegistryBuilder {
    limits: McpServerToolRegistryLimits,
    pending: BTreeMap<String, PendingTool>,
    total_schema_bytes: usize,
}

impl McpServerToolRegistryBuilder {
    /// Creates an empty builder with explicit ceilings.
    #[must_use]
    pub fn new(limits: McpServerToolRegistryLimits) -> Self {
        Self {
            limits,
            pending: BTreeMap::new(),
            total_schema_bytes: 0,
        }
    }

    /// Creates an empty builder with production defaults.
    #[must_use]
    pub fn with_default_limits() -> Self {
        Self::new(McpServerToolRegistryLimits::default())
    }

    /// Registers one definition and handler without mutating on failure.
    pub fn register<H>(
        &mut self,
        definition: McpServerToolDefinition,
        handler: H,
    ) -> Result<(), McpServerToolRegistryError>
    where
        H: McpServerToolHandler,
    {
        self.register_shared(definition, Arc::new(handler))
    }

    /// Registers one definition with an already shared handler.
    pub fn register_shared(
        &mut self,
        definition: McpServerToolDefinition,
        handler: Arc<dyn McpServerToolHandler>,
    ) -> Result<(), McpServerToolRegistryError> {
        if self.pending.len() == self.limits.maximum_tools {
            return Err(McpServerToolRegistryError::TooManyTools);
        }
        if self.pending.contains_key(definition.name()) {
            return Err(McpServerToolRegistryError::DuplicateTool);
        }
        let input_bytes = canonical_json_len(definition.input_schema())
            .map_err(|_| McpServerToolRegistryError::SchemaTooLarge)?;
        let output_bytes = definition
            .output_schema()
            .map(canonical_json_len)
            .transpose()
            .map_err(|_| McpServerToolRegistryError::SchemaTooLarge)?
            .unwrap_or(0);
        if input_bytes > self.limits.maximum_schema_bytes
            || output_bytes > self.limits.maximum_schema_bytes
        {
            return Err(McpServerToolRegistryError::SchemaTooLarge);
        }
        let added = input_bytes
            .checked_add(output_bytes)
            .ok_or(McpServerToolRegistryError::TotalSchemaBytesTooLarge)?;
        let total = self
            .total_schema_bytes
            .checked_add(added)
            .ok_or(McpServerToolRegistryError::TotalSchemaBytesTooLarge)?;
        if total > self.limits.maximum_total_schema_bytes {
            return Err(McpServerToolRegistryError::TotalSchemaBytesTooLarge);
        }
        self.pending.insert(
            definition.name().to_owned(),
            PendingTool {
                definition,
                handler,
            },
        );
        self.total_schema_bytes = total;
        Ok(())
    }

    /// Eagerly compiles every validator in offline mode and freezes the catalog.
    pub fn build(self) -> Result<McpServerToolRegistry, McpServerToolRegistryError> {
        if self.pending.is_empty() {
            return Err(McpServerToolRegistryError::Empty);
        }
        let mut registered = BTreeMap::new();
        let mut digest_material = Vec::new();
        for (name, pending) in self.pending {
            let input_validator = compile_schema(name.as_str(), pending.definition.input_schema())?;
            let output_validator = pending
                .definition
                .output_schema()
                .map(|schema| compile_schema(name.as_str(), schema))
                .transpose()?;
            append_digest_part(&mut digest_material, name.as_bytes());
            let input = serde_json_canonicalizer::to_vec(pending.definition.input_schema())
                .map_err(|_| McpServerToolRegistryError::SchemaTooLarge)?;
            append_digest_part(&mut digest_material, &input);
            if let Some(output) = pending.definition.output_schema() {
                let output = serde_json_canonicalizer::to_vec(output)
                    .map_err(|_| McpServerToolRegistryError::SchemaTooLarge)?;
                append_digest_part(&mut digest_material, &output);
            } else {
                append_digest_part(&mut digest_material, &[]);
            }
            let protocol = pending.definition.to_protocol();
            registered.insert(
                name,
                Arc::new(RegisteredTool {
                    definition: pending.definition,
                    protocol,
                    input_validator,
                    output_validator,
                    handler: pending.handler,
                }),
            );
        }
        let digest = Digest::sha256(digest_material).to_string();
        Ok(McpServerToolRegistry {
            inner: Arc::new(McpServerToolRegistryInner {
                tools: registered,
                digest: Arc::from(
                    digest
                        .strip_prefix("sha256:")
                        .expect("StateKnot SHA-256 digest has stable prefix"),
                ),
                total_schema_bytes: self.total_schema_bytes,
            }),
        })
    }
}

impl Default for McpServerToolRegistryBuilder {
    fn default() -> Self {
        Self::with_default_limits()
    }
}

impl fmt::Debug for McpServerToolRegistryBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerToolRegistryBuilder")
            .field("limits", &self.limits)
            .field("tool_count", &self.pending.len())
            .field("total_schema_bytes", &self.total_schema_bytes)
            .finish_non_exhaustive()
    }
}

fn compile_schema(
    tool_name: &str,
    schema: &Value,
) -> Result<Validator, McpServerToolRegistryError> {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .offline()
        .build(schema)
        .map_err(|source| McpServerToolRegistryError::SchemaCompilation {
            tool_name: tool_name.to_owned().into_boxed_str(),
            diagnostic: bounded_diagnostic(&source.to_string()),
        })
}

pub(crate) fn append_digest_part(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

/// Immutable executable Tool registry.
#[derive(Clone)]
pub struct McpServerToolRegistry {
    inner: Arc<McpServerToolRegistryInner>,
}

struct McpServerToolRegistryInner {
    tools: BTreeMap<String, Arc<RegisteredTool>>,
    digest: Arc<str>,
    total_schema_bytes: usize,
}

impl McpServerToolRegistry {
    /// Returns the registered Tool count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.tools.len()
    }

    /// Returns whether no Tool is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.tools.is_empty()
    }

    /// Returns one StateKnot-owned definition.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&McpServerToolDefinition> {
        self.inner.tools.get(name).map(|entry| &entry.definition)
    }

    /// Iterates definitions in stable ASCII name order.
    pub fn definitions(&self) -> impl ExactSizeIterator<Item = &McpServerToolDefinition> {
        self.inner.tools.values().map(|entry| &entry.definition)
    }

    /// Returns retained canonical input/output schema bytes.
    #[must_use]
    pub fn total_schema_bytes(&self) -> usize {
        self.inner.total_schema_bytes
    }

    fn contains_scoped_tools(&self) -> bool {
        self.inner
            .tools
            .values()
            .any(|entry| !entry.definition.required_scopes.is_empty())
    }
}

impl fmt::Debug for McpServerToolRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerToolRegistry")
            .field("tool_count", &self.len())
            .field("catalog_digest", &self.inner.digest)
            .field("total_schema_bytes", &self.total_schema_bytes())
            .finish_non_exhaustive()
    }
}

/// Startup failure for an executable registry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerToolRegistryError {
    /// At least one Tool is required.
    #[error("MCP tool registry is empty")]
    Empty,
    /// A name appeared more than once.
    #[error("duplicate MCP tool")]
    DuplicateTool,
    /// The configured Tool count ceiling was reached.
    #[error("too many MCP tools")]
    TooManyTools,
    /// An input or output schema exceeded the configured ceiling.
    #[error("MCP tool schema is too large")]
    SchemaTooLarge,
    /// Aggregate schema bytes exceeded the configured ceiling.
    #[error("MCP tool aggregate schema bytes are too large")]
    TotalSchemaBytesTooLarge,
    /// Offline validator compilation failed, including unresolved `$ref`.
    #[error("failed to compile schema for MCP tool {tool_name}: {diagnostic}")]
    SchemaCompilation {
        /// Tool whose schema failed.
        tool_name: Box<str>,
        /// Bounded startup diagnostic.
        diagnostic: Box<str>,
    },
}

/// Stable service identity, pagination, and cache policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerApplicationOptions {
    pub(crate) server_name: Arc<str>,
    pub(crate) server_version: Arc<str>,
    pub(crate) instructions: Option<Arc<str>>,
    pub(crate) page_size: usize,
    pub(crate) cache_ttl_ms: u64,
    pub(crate) cache_scope: McpServerCacheScope,
}

impl McpServerApplicationOptions {
    /// Hard page size ceiling.
    pub const HARD_MAXIMUM_PAGE_SIZE: usize = 256;
    /// Hard cache TTL ceiling.
    pub const HARD_MAXIMUM_CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

    /// Constructs explicit service metadata and catalog policy.
    pub fn new(
        server_name: impl Into<String>,
        server_version: impl Into<String>,
        page_size: usize,
        cache_ttl: Duration,
        cache_scope: McpServerCacheScope,
    ) -> Result<Self, McpServerApplicationOptionsError> {
        let server_name = server_name.into();
        let server_version = server_version.into();
        validate_service_component(&server_name)?;
        validate_service_component(&server_version)?;
        if page_size == 0 || page_size > Self::HARD_MAXIMUM_PAGE_SIZE {
            return Err(McpServerApplicationOptionsError::InvalidPageSize);
        }
        if cache_ttl > Self::HARD_MAXIMUM_CACHE_TTL {
            return Err(McpServerApplicationOptionsError::InvalidCacheTtl);
        }
        Ok(Self {
            server_name: Arc::from(server_name),
            server_version: Arc::from(server_version),
            instructions: None,
            page_size,
            cache_ttl_ms: u64::try_from(cache_ttl.as_millis())
                .map_err(|_| McpServerApplicationOptionsError::InvalidCacheTtl)?,
            cache_scope,
        })
    }

    /// Adds bounded client guidance.
    pub fn with_instructions(
        mut self,
        instructions: impl Into<String>,
    ) -> Result<Self, McpServerApplicationOptionsError> {
        let instructions = instructions.into();
        if instructions.is_empty()
            || instructions.len() > MAX_DISPLAY_BYTES
            || instructions.trim() != instructions
            || instructions
                .chars()
                .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        {
            return Err(McpServerApplicationOptionsError::InvalidInstructions);
        }
        self.instructions = Some(Arc::from(instructions));
        Ok(self)
    }
}

fn validate_service_component(value: &str) -> Result<(), McpServerApplicationOptionsError> {
    if value.is_empty()
        || value.len() > 256
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(McpServerApplicationOptionsError::InvalidIdentity);
    }
    Ok(())
}

/// Invalid Tool service configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerApplicationOptionsError {
    /// Server name or version was invalid.
    #[error("invalid MCP server identity")]
    InvalidIdentity,
    /// The page size was zero or exceeded the hard maximum.
    #[error("invalid MCP tool catalog page size")]
    InvalidPageSize,
    /// Cache TTL exceeded the hard maximum.
    #[error("invalid MCP tool catalog cache TTL")]
    InvalidCacheTtl,
    /// Instructions were empty, padded, oversized, or contained unsafe controls.
    #[error("invalid MCP server instructions")]
    InvalidInstructions,
}

/// Production MCP Tool handler backed by an immutable `StateKnot` registry.
#[derive(Clone)]
pub struct McpServerToolService {
    registry: McpServerToolRegistry,
    options: McpServerApplicationOptions,
    authorization: Arc<dyn McpServerToolAuthorization>,
}

impl McpServerToolService {
    /// Creates a service with an explicit decoded authorization policy.
    pub fn new<A>(
        registry: McpServerToolRegistry,
        options: McpServerApplicationOptions,
        authorization: A,
    ) -> Result<Self, McpServerToolServiceBuildError>
    where
        A: McpServerToolAuthorization,
    {
        Self::with_shared_authorization(registry, options, Arc::new(authorization))
    }

    /// Creates a service with an already shared authorization policy.
    pub fn with_shared_authorization(
        registry: McpServerToolRegistry,
        options: McpServerApplicationOptions,
        authorization: Arc<dyn McpServerToolAuthorization>,
    ) -> Result<Self, McpServerToolServiceBuildError> {
        if registry.contains_scoped_tools()
            && matches!(options.cache_scope, McpServerCacheScope::Public)
        {
            return Err(McpServerToolServiceBuildError::PublicCacheWithScopedTools);
        }
        Ok(Self {
            registry,
            options,
            authorization,
        })
    }

    fn page(
        &self,
        principal: &McpServerPrincipal,
        cursor: Option<&str>,
    ) -> Result<(Vec<Tool>, Option<String>), ErrorData> {
        let scope_tag = self.catalog_scope_tag(principal);
        let start = cursor.map_or(Ok(0), |cursor| self.parse_cursor(cursor, &scope_tag))?;
        let visible = self
            .registry
            .inner
            .tools
            .values()
            .filter(|entry| {
                entry
                    .definition
                    .required_scopes()
                    .all(|scope| principal.has_scope(scope))
            })
            .collect::<Vec<_>>();
        if start > visible.len() {
            return Err(ErrorData::invalid_params(
                "Invalid tool catalog cursor",
                None,
            ));
        }
        let end = start
            .saturating_add(self.options.page_size)
            .min(visible.len());
        let tools = visible
            .iter()
            .skip(start)
            .take(end - start)
            .map(|entry| entry.protocol.clone())
            .collect();
        let next = (end < visible.len()).then(|| self.cursor(&scope_tag, end));
        Ok((tools, next))
    }

    fn catalog_scope_tag(&self, principal: &McpServerPrincipal) -> String {
        if matches!(self.options.cache_scope, McpServerCacheScope::Public) {
            return "public".to_owned();
        }
        let mut material = principal.subject().as_bytes().to_vec();
        for scope in principal.scopes() {
            append_digest_part(&mut material, scope.as_bytes());
        }
        let digest = Digest::sha256(material).to_string();
        digest
            .strip_prefix("sha256:")
            .expect("StateKnot SHA-256 digest has stable prefix")
            .to_owned()
    }

    fn cursor(&self, scope_tag: &str, offset: usize) -> String {
        format!("v1.{}.{}.{}", self.registry.inner.digest, scope_tag, offset)
    }

    fn parse_cursor(&self, cursor: &str, scope_tag: &str) -> Result<usize, ErrorData> {
        if cursor.len() > 192 {
            return Err(ErrorData::invalid_params(
                "Invalid tool catalog cursor",
                None,
            ));
        }
        let mut parts = cursor.split('.');
        let valid_version = parts.next() == Some("v1");
        let valid_digest = parts.next() == Some(self.registry.inner.digest.as_ref());
        let valid_scope = parts.next() == Some(scope_tag);
        let offset = parts.next().and_then(|value| value.parse::<usize>().ok());
        if !valid_version
            || !valid_digest
            || !valid_scope
            || parts.next().is_some()
            || offset.is_none()
        {
            return Err(ErrorData::invalid_params(
                "Invalid tool catalog cursor",
                None,
            ));
        }
        Ok(offset.expect("validated cursor offset exists"))
    }

    async fn dispatch(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let Some(registered) = self
            .registry
            .inner
            .tools
            .get(request.name.as_ref())
            .cloned()
        else {
            return Err(unavailable_tool());
        };
        let principal = mcp_server_principal(&context)
            .cloned()
            .ok_or_else(|| ErrorData::internal_error("MCP server boundary is missing", None))?;
        if registered
            .definition
            .required_scopes()
            .any(|scope| !principal.has_scope(scope))
        {
            return Err(unavailable_tool());
        }

        let arguments = request.arguments.unwrap_or_default();
        let input_responses = request.input_responses.unwrap_or_default();
        validate_inbound_mrtr(&input_responses, request.request_state.as_deref())?;
        let call = McpServerToolCall {
            name: Arc::from(request.name.into_owned()),
            arguments: Arc::new(arguments),
            input_responses: Arc::new(input_responses),
            request_state: request.request_state.map(Arc::from),
        };
        let authorization_request = McpServerToolAuthorizationRequest {
            principal: principal.clone(),
            definition: registered.definition.clone(),
            call: call.clone(),
        };
        match self.authorization.authorize(authorization_request).await {
            Ok(()) => {}
            Err(McpServerToolAuthorizationError::Forbidden) => return Err(unavailable_tool()),
            Err(McpServerToolAuthorizationError::Unavailable) => {
                return Err(ErrorData::internal_error(
                    "MCP tool authorization is unavailable",
                    None,
                ));
            }
        }

        let argument_value = Value::Object((*call.arguments).clone());
        if !registered.input_validator.is_valid(&argument_value) {
            return Err(ErrorData::invalid_params(
                "Tool arguments do not match input schema",
                None,
            ));
        }

        let tool_context = build_tool_context(&context, principal, &call)?;
        let outcome = registered
            .handler
            .call(call, tool_context)
            .await
            .map_err(handler_error_result)?;
        match outcome {
            McpServerToolOutcome::Complete(result) => {
                if !result.is_error
                    && let Some(validator) = &registered.output_validator
                {
                    let Some(structured) = result.structured_content.as_deref() else {
                        return Err(ErrorData::internal_error(
                            "MCP tool omitted required structured output",
                            None,
                        ));
                    };
                    if !validator.is_valid(structured) {
                        return Err(ErrorData::internal_error(
                            "MCP tool output failed schema validation",
                            None,
                        ));
                    }
                }
                Ok(result.into_protocol().into())
            }
            McpServerToolOutcome::InputRequired(result) => Ok(result.into_protocol().into()),
        }
    }
}

impl fmt::Debug for McpServerToolService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerToolService")
            .field("registry", &self.registry)
            .field("options", &self.options)
            .field("authorization", &"[POLICY]")
            .finish_non_exhaustive()
    }
}

impl ServerHandler for McpServerToolService {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }

    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                self.options.server_name.to_string(),
                self.options.server_version.to_string(),
            ));
        if let Some(instructions) = &self.options.instructions {
            info = info.with_instructions(instructions.to_string());
        }
        info
    }

    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<DiscoverResult, ErrorData> {
        Ok(
            DiscoverResult::from_server_info(vec![ProtocolVersion::V_2026_07_28], self.get_info())
                .with_ttl_ms(self.options.cache_ttl_ms)
                .with_cache_scope(self.options.cache_scope.into()),
        )
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.registry
            .inner
            .tools
            .get(name)
            .map(|entry| entry.protocol.clone())
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let principal = mcp_server_principal(&context)
            .ok_or_else(|| ErrorData::internal_error("MCP server boundary is missing", None))?;
        let (tools, next_cursor) = self.page(
            principal,
            request.as_ref().and_then(|value| value.cursor.as_deref()),
        )?;
        let mut result = ListToolsResult::with_all_items(tools)
            .with_ttl_ms(self.options.cache_ttl_ms)
            .with_cache_scope(self.options.cache_scope.into());
        result.next_cursor = next_cursor;
        Ok(result)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        self.dispatch(request, context).await
    }
}

/// Invalid combination of frozen catalog and service policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerToolServiceBuildError {
    /// A catalog with scope-restricted entries cannot be shared-cacheable.
    #[error("scope-restricted MCP tools require a private catalog cache")]
    PublicCacheWithScopedTools,
}

fn unavailable_tool() -> ErrorData {
    ErrorData::invalid_params("Unknown or unavailable tool", None)
}

pub(crate) fn validate_inbound_mrtr(
    responses: &BTreeMap<String, Value>,
    request_state: Option<&str>,
) -> Result<(), ErrorData> {
    if responses.len() > MAX_INPUT_REQUESTS
        || request_state.is_some_and(|value| {
            value.is_empty()
                || value.len() > MAX_REQUEST_STATE_BYTES
                || value.chars().any(char::is_control)
        })
    {
        return Err(ErrorData::invalid_params(
            "Invalid MCP input response",
            None,
        ));
    }
    let value = serde_json::to_value(responses)
        .map_err(|_| ErrorData::invalid_params("Invalid MCP input response", None))?;
    if canonical_json_len(&value).map_or(true, |bytes| bytes > 8 * MEBIBYTE) {
        return Err(ErrorData::invalid_params(
            "Invalid MCP input response",
            None,
        ));
    }
    Ok(())
}

fn build_tool_context(
    context: &RequestContext<RoleServer>,
    principal: McpServerPrincipal,
    call: &McpServerToolCall,
) -> Result<McpServerToolContext, ErrorData> {
    let client = context.client_info();
    if let Some(client) = &client {
        validate_client_component(&client.name)?;
        validate_client_component(&client.version)?;
    }
    let capabilities = context.client_capabilities().unwrap_or_default();
    let capabilities_value = serde_json::to_value(&capabilities)
        .map_err(|_| ErrorData::internal_error("Failed to retain client capabilities", None))?;
    let binding = request_state_binding(&principal, call)?;
    let progress = context
        .meta
        .get_progress_token()
        .map(|token| McpServerProgressSink {
            peer: context.peer.clone(),
            token,
        });
    Ok(McpServerToolContext {
        principal,
        client_name: client.as_ref().map(|value| Arc::from(value.name.as_str())),
        client_version: client
            .as_ref()
            .map(|value| Arc::from(value.version.as_str())),
        client_capabilities: Arc::new(capabilities_value),
        cancellation: context.ct.clone(),
        progress,
        request_state_binding: binding.into(),
    })
}

fn validate_client_component(value: &str) -> Result<(), ErrorData> {
    if value.is_empty()
        || value.len() > 512
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(ErrorData::invalid_params(
            "Invalid MCP client identity",
            None,
        ));
    }
    Ok(())
}

fn request_state_binding(
    principal: &McpServerPrincipal,
    call: &McpServerToolCall,
) -> Result<Vec<u8>, ErrorData> {
    let arguments = serde_json_canonicalizer::to_vec(&Value::Object((*call.arguments).clone()))
        .map_err(|_| ErrorData::internal_error("Failed to bind MCP request state", None))?;
    let mut material = b"stateknot/mcp-server/request-state/v1".to_vec();
    append_digest_part(&mut material, principal.subject().as_bytes());
    append_digest_part(&mut material, call.name().as_bytes());
    append_digest_part(&mut material, &arguments);
    Ok(Digest::sha256(material).as_bytes().to_vec())
}

fn handler_error_result(error: McpServerToolHandlerError) -> ErrorData {
    match error {
        McpServerToolHandlerError::Unavailable => {
            ErrorData::internal_error("MCP tool dependency is unavailable", None)
        }
        McpServerToolHandlerError::Cancelled => {
            ErrorData::internal_error("MCP tool execution was cancelled", None)
        }
        McpServerToolHandlerError::Internal => {
            ErrorData::internal_error("MCP tool execution failed", None)
        }
        McpServerToolHandlerError::MissingRequiredClientCapability(capability) => {
            let mut required = ClientCapabilities::default();
            match capability {
                McpServerRequiredClientCapability::Elicitation => {
                    required.elicitation = Some(ElicitationCapability::default());
                }
                McpServerRequiredClientCapability::Sampling => {
                    required.sampling = Some(SamplingCapability::default());
                }
                McpServerRequiredClientCapability::Roots => {
                    required.roots = Some(RootsCapabilities::default());
                }
            }
            ErrorData::missing_required_client_capability(required)
        }
    }
}

fn canonical_json_len(value: &Value) -> Result<usize, serde_json::Error> {
    serde_json_canonicalizer::to_vec(value).map(|bytes| bytes.len())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use http::{Request, StatusCode};
    use http_body_util::{BodyExt as _, Full};
    use serde_json::json;
    use tower_service::Service as _;

    use super::*;
    use crate::{McpServerAuthentication, McpServerHttpOptions, McpServerHttpService};

    #[derive(Clone)]
    struct EchoHandler {
        calls: Arc<AtomicUsize>,
    }

    impl McpServerToolHandler for EchoHandler {
        fn call(
            &self,
            call: McpServerToolCall,
            execution_context: McpServerToolContext,
        ) -> BoxFuture<'_, Result<McpServerToolOutcome, McpServerToolHandlerError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let subject = execution_context.principal().subject().to_owned();
            let value = call
                .arguments()
                .get("value")
                .cloned()
                .unwrap_or(Value::Null);
            Box::pin(async move {
                let block = McpServerContent::text(format!("{subject}:{value}"))
                    .map_err(|_| McpServerToolHandlerError::Internal)?;
                McpServerToolResult::structured([block], json!({ "echo": value }))
                    .map(Into::into)
                    .map_err(|_| McpServerToolHandlerError::Internal)
            })
        }
    }

    #[derive(Clone, Copy)]
    struct MissingCapabilityHandler;

    impl McpServerToolHandler for MissingCapabilityHandler {
        fn call(
            &self,
            _call: McpServerToolCall,
            _context: McpServerToolContext,
        ) -> BoxFuture<'_, Result<McpServerToolOutcome, McpServerToolHandlerError>> {
            Box::pin(async {
                Err(McpServerToolHandlerError::MissingRequiredClientCapability(
                    McpServerRequiredClientCapability::Sampling,
                ))
            })
        }
    }

    #[derive(Clone)]
    struct DenyAuthorization {
        calls: Arc<AtomicUsize>,
    }

    impl McpServerToolAuthorization for DenyAuthorization {
        fn authorize(
            &self,
            _request: McpServerToolAuthorizationRequest,
        ) -> BoxFuture<'_, Result<(), McpServerToolAuthorizationError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(McpServerToolAuthorizationError::Forbidden) })
        }
    }

    fn echo_definition(name: &str) -> McpServerToolDefinition {
        McpServerToolDefinition::new(
            name,
            json!({
                "$schema": JSON_SCHEMA_2020_12,
                "type": "object",
                "properties": { "value": { "type": "string" } },
                "required": ["value"],
                "additionalProperties": false
            }),
        )
        .unwrap()
        .with_output_schema(json!({
            "$schema": JSON_SCHEMA_2020_12,
            "type": "object",
            "properties": { "echo": { "type": "string" } },
            "required": ["echo"],
            "additionalProperties": false
        }))
        .unwrap()
    }

    fn tool_service<A>(
        definitions: impl IntoIterator<Item = McpServerToolDefinition>,
        handler_calls: &Arc<AtomicUsize>,
        authorization: A,
    ) -> McpServerHttpService<McpServerToolService>
    where
        A: McpServerToolAuthorization,
    {
        let mut builder = McpServerToolRegistryBuilder::default();
        for definition in definitions {
            builder
                .register(
                    definition,
                    EchoHandler {
                        calls: handler_calls.clone(),
                    },
                )
                .unwrap();
        }
        let options = McpServerApplicationOptions::new(
            "stateknot-tool-test",
            "0.0.0",
            1,
            Duration::from_secs(60),
            McpServerCacheScope::Private,
        )
        .unwrap();
        let handler =
            McpServerToolService::new(builder.build().unwrap(), options, authorization).unwrap();
        McpServerHttpService::new(
            handler,
            McpServerHttpOptions::loopback(32124).unwrap(),
            McpServerAuthentication::anonymous_loopback(),
        )
        .unwrap()
    }

    fn protocol_request(
        method: &str,
        mut params: Value,
        name: Option<&str>,
    ) -> Request<Full<Bytes>> {
        params.as_object_mut().unwrap().insert(
            "_meta".to_owned(),
            json!({
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": {
                    "name": "stateknot-tool-test-client",
                    "version": "0.0.0"
                },
                "io.modelcontextprotocol/clientCapabilities": {}
            }),
        );
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params
        }))
        .unwrap();
        let mut builder = Request::builder()
            .method("POST")
            .uri("http://127.0.0.1:32124/mcp")
            .header("host", "127.0.0.1:32124")
            .header("origin", "http://127.0.0.1:32124")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", method);
        if let Some(name) = name {
            builder = builder.header("mcp-name", name);
        }
        builder.body(Full::new(Bytes::from(body))).unwrap()
    }

    async fn invoke(
        service: &mut McpServerHttpService<McpServerToolService>,
        method: &str,
        params: Value,
        name: Option<&str>,
    ) -> (StatusCode, Value) {
        let response = service
            .call(protocol_request(method, params, name))
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = serde_json::from_slice(&bytes).unwrap_or_else(|error| {
            panic!(
                "invalid MCP response ({status}): {}; {error}",
                String::from_utf8_lossy(&bytes)
            )
        });
        (status, value)
    }

    #[test]
    fn registry_is_sorted_offline_and_rejects_remote_refs() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut builder = McpServerToolRegistryBuilder::default();
        for name in ["zeta", "alpha"] {
            builder
                .register(
                    echo_definition(name),
                    EchoHandler {
                        calls: calls.clone(),
                    },
                )
                .unwrap();
        }
        let registry = builder.build().unwrap();
        assert_eq!(
            registry
                .definitions()
                .map(McpServerToolDefinition::name)
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );

        let mut remote = McpServerToolRegistryBuilder::default();
        remote
            .register(
                McpServerToolDefinition::new(
                    "remote",
                    json!({
                        "$schema": JSON_SCHEMA_2020_12,
                        "$ref": "https://schemas.example.test/remote.json"
                    }),
                )
                .unwrap(),
                EchoHandler { calls },
            )
            .unwrap();
        assert!(matches!(
            remote.build(),
            Err(McpServerToolRegistryError::SchemaCompilation { .. })
        ));
    }

    #[test]
    fn content_and_results_are_bounded() {
        assert!(McpServerContent::image([1_u8, 2, 3], "image/png").is_ok());
        assert!(matches!(
            McpServerContent::audio([1_u8], "image/png"),
            Err(McpServerContentError::Invalid)
        ));
        assert!(matches!(
            McpServerContent::text("x".repeat(MAX_TEXT_CONTENT_BYTES + 1)),
            Err(McpServerContentError::TooLarge)
        ));
    }

    #[test]
    fn request_state_is_bound_expires_and_redacts_keys() {
        let codec = McpServerRequestStateCodec::new([7_u8; 32]).unwrap();
        assert!(!format!("{codec:?}").contains("777"));
        let binding = b"tenant-a|tool-a";
        let sealed = codec
            .seal_json(&json!({ "round": 1 }), binding, Duration::from_secs(60))
            .unwrap();
        assert_eq!(
            codec.open_json(&sealed, binding).unwrap(),
            json!({ "round": 1 })
        );
        assert!(matches!(
            codec.open_json(&sealed, b"tenant-b|tool-a"),
            Err(McpServerRequestStateError::InvalidState)
        ));
        assert!(matches!(
            codec.seal_json(&json!({}), binding, Duration::ZERO),
            Err(McpServerRequestStateError::InvalidOperation)
        ));
    }

    #[test]
    fn tool_names_and_scopes_fail_closed() {
        assert!(matches!(
            McpServerToolDefinition::new("not allowed", json!({ "type": "object" })),
            Err(McpServerToolDefinitionError::InvalidName)
        ));
        assert!(matches!(
            echo_definition("echo").with_required_scopes(["tool:call", "tool:call"]),
            Err(McpServerToolDefinitionError::DuplicateScope)
        ));
    }

    #[tokio::test]
    async fn frozen_registry_pages_and_dispatches_through_the_http_boundary() {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let mut service = tool_service(
            [echo_definition("zeta"), echo_definition("alpha")],
            &handler_calls,
            AllowMcpServerToolAuthorization,
        );

        let (status, first) = invoke(&mut service, "tools/list", json!({}), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first.pointer("/result/tools/0/name"), Some(&json!("alpha")));
        assert_eq!(first.pointer("/result/ttlMs"), Some(&json!(60_000)));
        assert_eq!(first.pointer("/result/cacheScope"), Some(&json!("private")));
        let cursor = first
            .pointer("/result/nextCursor")
            .and_then(Value::as_str)
            .unwrap();
        let (status, second) = invoke(
            &mut service,
            "tools/list",
            json!({ "cursor": cursor }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(second.pointer("/result/tools/0/name"), Some(&json!("zeta")));
        assert!(second.pointer("/result/nextCursor").is_none());

        let (status, invalid) = invoke(
            &mut service,
            "tools/call",
            json!({ "name": "alpha", "arguments": { "value": 7 } }),
            Some("alpha"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid.pointer("/error/code"), Some(&json!(-32602)));
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);

        let (status, valid) = invoke(
            &mut service,
            "tools/call",
            json!({ "name": "alpha", "arguments": { "value": "ok" } }),
            Some("alpha"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            valid.pointer("/result/structuredContent/echo"),
            Some(&json!("ok"))
        );
        assert_eq!(valid.pointer("/result/isError"), Some(&json!(false)));
        assert_eq!(handler_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn decoded_authorization_precedes_schema_diagnostics_and_dispatch() {
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let authorization_calls = Arc::new(AtomicUsize::new(0));
        let mut service = tool_service(
            [echo_definition("protected")],
            &handler_calls,
            DenyAuthorization {
                calls: authorization_calls.clone(),
            },
        );
        let (status, denied) = invoke(
            &mut service,
            "tools/call",
            json!({ "name": "protected", "arguments": { "value": 7 } }),
            Some("protected"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(denied.pointer("/error/code"), Some(&json!(-32602)));
        assert_eq!(
            denied.pointer("/error/message"),
            Some(&json!("Unknown or unavailable tool"))
        );
        assert_eq!(authorization_calls.load(Ordering::SeqCst), 1);
        assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn handler_can_report_a_missing_core_client_capability() {
        let mut builder = McpServerToolRegistryBuilder::default();
        builder
            .register(echo_definition("needs_sampling"), MissingCapabilityHandler)
            .unwrap();
        let options = McpServerApplicationOptions::new(
            "stateknot-capability-test",
            "0.0.0",
            10,
            Duration::from_secs(60),
            McpServerCacheScope::Private,
        )
        .unwrap();
        let handler = McpServerToolService::new(
            builder.build().unwrap(),
            options,
            AllowMcpServerToolAuthorization,
        )
        .unwrap();
        let mut service = McpServerHttpService::new(
            handler,
            McpServerHttpOptions::loopback(32124).unwrap(),
            McpServerAuthentication::anonymous_loopback(),
        )
        .unwrap();

        let (status, response) = invoke(
            &mut service,
            "tools/call",
            json!({ "name": "needs_sampling", "arguments": { "value": "ok" } }),
            Some("needs_sampling"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(response.pointer("/error/code"), Some(&json!(-32021)));
        assert_eq!(
            response.pointer("/error/data/requiredCapabilities/sampling"),
            Some(&json!({}))
        );
    }
}
