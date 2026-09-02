// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! General MCP 2026-07-28 stateless Tool client.
//!
//! This surface is deliberately separate from the durable, exact-schema
//! [`crate::McpRemoteTool`] binding. It implements broad protocol
//! interoperability without weakening the runtime's stronger admission and
//! reconciliation contract.

use std::{
    collections::HashSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::StreamExt;
use http::{HeaderName, HeaderValue};
use reqwest::{StatusCode, header};
use serde_json::{Map, Value, json};
use stateknot_core::BoxFuture;
use thiserror::Error;

use crate::{
    McpAuthorization, McpAuthorizationError, ProviderEndpoint, ProviderEndpointError,
    ProviderHttpOptions,
    http::build_client,
    sse::{SseDecodeError, SseDecoder},
};

/// The modern, stateless MCP revision implemented by this client.
pub const MCP_PROTOCOL_VERSION_2026_07_28: &str = "2026-07-28";

const MCP_JSON: &str = "application/json";
const MCP_EVENT_STREAM: &str = "text/event-stream";
const MCP_ACCEPT: &str = "application/json, text/event-stream";
const MCP_HEADER_PROTOCOL_VERSION: &str = "mcp-protocol-version";
const MCP_HEADER_METHOD: &str = "mcp-method";
const MCP_HEADER_NAME: &str = "mcp-name";
const MCP_HEADER_PARAMETER_PREFIX: &str = "mcp-param-";
const MCP_META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
const MCP_META_CLIENT_INFO: &str = "io.modelcontextprotocol/clientInfo";
const MCP_META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
const MCP_META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";
const BASE64_PREFIX: &str = "=?base64?";
const BASE64_SUFFIX: &str = "?=";
const MAX_IDENTITY_COMPONENT_BYTES: usize = 256;
const MAX_PROTOCOL_VERSIONS: usize = 64;
const MAX_PROTOCOL_VERSION_BYTES: usize = 128;
const MAX_TOOL_NAME_BYTES: usize = 1024;
const MAX_CURSOR_BYTES: usize = 4096;
const MAX_REMOTE_ERROR_MESSAGE_BYTES: usize = 4096;

static NEXT_CLIENT_BINDING_ID: AtomicU64 = AtomicU64::new(1);

/// Validated identity sent in every stateless MCP request.
#[derive(Clone, Eq, PartialEq)]
pub struct McpClientIdentity {
    name: Box<str>,
    version: Box<str>,
}

impl McpClientIdentity {
    /// Constructs a bounded printable client identity.
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, McpClientIdentityError> {
        let name = name.into();
        let version = version.into();
        validate_identity_component(&name)?;
        validate_identity_component(&version)?;
        Ok(Self {
            name: name.into_boxed_str(),
            version: version.into_boxed_str(),
        })
    }

    /// Returns the implementation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the implementation version.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }
}

impl fmt::Debug for McpClientIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpClientIdentity")
            .field("name", &self.name)
            .field("version", &self.version)
            .finish()
    }
}

/// Invalid MCP client identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpClientIdentityError {
    /// A required component was empty.
    #[error("MCP client identity component is empty")]
    Empty,
    /// A component exceeded its hard byte ceiling.
    #[error("MCP client identity component is too long")]
    TooLong,
    /// Trimming would change a component.
    #[error("MCP client identity component has boundary whitespace")]
    BoundaryWhitespace,
    /// A component contained a control character.
    #[error("MCP client identity component contains a control character")]
    ControlCharacter,
}

fn validate_identity_component(value: &str) -> Result<(), McpClientIdentityError> {
    if value.is_empty() {
        return Err(McpClientIdentityError::Empty);
    }
    if value.len() > MAX_IDENTITY_COMPONENT_BYTES {
        return Err(McpClientIdentityError::TooLong);
    }
    if value.trim() != value {
        return Err(McpClientIdentityError::BoundaryWhitespace);
    }
    if value.chars().any(char::is_control) {
        return Err(McpClientIdentityError::ControlCharacter);
    }
    Ok(())
}

/// Bounded resource policy for a general stateless MCP client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpClientOptions {
    transport: ProviderHttpOptions,
    request_timeout: Duration,
    maximum_concurrent_requests: usize,
    maximum_catalog_pages: usize,
    maximum_catalog_tools: usize,
    maximum_notifications_per_response: usize,
}

impl McpClientOptions {
    /// Hard total deadline ceiling for one logical request.
    pub const HARD_MAXIMUM_REQUEST_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
    /// Hard concurrent request ceiling.
    pub const HARD_MAXIMUM_CONCURRENT_REQUESTS: usize = 1024;
    /// Hard tool-catalog page ceiling.
    pub const HARD_MAXIMUM_CATALOG_PAGES: usize = 64;
    /// Hard tool-catalog entry ceiling.
    pub const HARD_MAXIMUM_CATALOG_TOOLS: usize = 4096;
    /// Hard request-scoped notification ceiling.
    pub const HARD_MAXIMUM_NOTIFICATIONS_PER_RESPONSE: usize = 4096;

    /// Constructs an explicit bounded client policy.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        transport: ProviderHttpOptions,
        request_timeout: Duration,
        maximum_concurrent_requests: usize,
        maximum_catalog_pages: usize,
        maximum_catalog_tools: usize,
        maximum_notifications_per_response: usize,
    ) -> Result<Self, McpClientOptionsError> {
        if request_timeout.is_zero() {
            return Err(McpClientOptionsError::ZeroTimeout);
        }
        if request_timeout.as_nanos() > Self::HARD_MAXIMUM_REQUEST_TIMEOUT.as_nanos() {
            return Err(McpClientOptionsError::AboveHardMaximum);
        }
        if maximum_concurrent_requests == 0
            || maximum_catalog_pages == 0
            || maximum_catalog_tools == 0
            || maximum_notifications_per_response == 0
        {
            return Err(McpClientOptionsError::ZeroLimit);
        }
        if maximum_concurrent_requests > Self::HARD_MAXIMUM_CONCURRENT_REQUESTS
            || maximum_catalog_pages > Self::HARD_MAXIMUM_CATALOG_PAGES
            || maximum_catalog_tools > Self::HARD_MAXIMUM_CATALOG_TOOLS
            || maximum_notifications_per_response > Self::HARD_MAXIMUM_NOTIFICATIONS_PER_RESPONSE
        {
            return Err(McpClientOptionsError::AboveHardMaximum);
        }
        Ok(Self {
            transport,
            request_timeout,
            maximum_concurrent_requests,
            maximum_catalog_pages,
            maximum_catalog_tools,
            maximum_notifications_per_response,
        })
    }

    /// Returns the underlying bounded HTTP policy.
    #[must_use]
    pub const fn transport(self) -> ProviderHttpOptions {
        self.transport
    }

    /// Returns the total deadline for one logical request, including a single
    /// safe protocol-version retry.
    #[must_use]
    pub const fn request_timeout(self) -> Duration {
        self.request_timeout
    }

    /// Returns the maximum in-flight requests for this client.
    #[must_use]
    pub const fn maximum_concurrent_requests(self) -> usize {
        self.maximum_concurrent_requests
    }

    /// Returns the catalog page ceiling.
    #[must_use]
    pub const fn maximum_catalog_pages(self) -> usize {
        self.maximum_catalog_pages
    }

    /// Returns the catalog entry ceiling.
    #[must_use]
    pub const fn maximum_catalog_tools(self) -> usize {
        self.maximum_catalog_tools
    }

    /// Returns the request-scoped notification ceiling.
    #[must_use]
    pub const fn maximum_notifications_per_response(self) -> usize {
        self.maximum_notifications_per_response
    }
}

impl Default for McpClientOptions {
    fn default() -> Self {
        Self {
            transport: ProviderHttpOptions::default(),
            request_timeout: Duration::from_secs(30),
            maximum_concurrent_requests: 16,
            maximum_catalog_pages: 16,
            maximum_catalog_tools: 1024,
            maximum_notifications_per_response: 1024,
        }
    }
}

/// Invalid general MCP client policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpClientOptionsError {
    /// The request deadline must be positive.
    #[error("MCP client request timeout must be positive")]
    ZeroTimeout,
    /// All resource ceilings must be positive.
    #[error("MCP client resource limits must be positive")]
    ZeroLimit,
    /// A resource ceiling exceeded the implementation maximum.
    #[error("MCP client resource limit exceeds the implementation maximum")]
    AboveHardMaximum,
}

/// Public-safe request facts supplied to an authorization provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpClientAuthorizationRequest {
    method: Box<str>,
    name: Option<Box<str>>,
}

impl McpClientAuthorizationRequest {
    /// Returns the exact MCP method.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Returns the tool, prompt, or resource name when the method has one.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the protocol version carried by this request.
    #[must_use]
    pub const fn protocol_version(&self) -> &'static str {
        MCP_PROTOCOL_VERSION_2026_07_28
    }
}

/// Resolves authorization independently for every stateless MCP POST.
pub trait McpClientAuthorizationProvider: Send + Sync + 'static {
    /// Resolves authorization without exposing it through request metadata.
    fn resolve(
        &self,
        request: &McpClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>>;
}

/// Cache metadata attached to a modern MCP result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct McpCachePolicy {
    ttl_ms: Option<u64>,
    scope: Option<Box<str>>,
}

impl McpCachePolicy {
    /// Returns the advertised freshness lifetime in milliseconds.
    #[must_use]
    pub const fn ttl_ms(&self) -> Option<u64> {
        self.ttl_ms
    }

    /// Returns the server-provided cache scope.
    #[must_use]
    pub fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }
}

/// Immutable server facts observed through `server/discover`.
#[derive(Clone, Debug)]
pub struct McpClientServer {
    supported_versions: Arc<[Box<str>]>,
    capabilities: Arc<Map<String, Value>>,
    name: Option<Box<str>>,
    version: Option<Box<str>>,
    instructions: Option<Box<str>>,
    cache: McpCachePolicy,
}

impl McpClientServer {
    /// Returns the protocol versions in the server's advertised order.
    #[must_use]
    pub fn supported_versions(&self) -> impl ExactSizeIterator<Item = &str> {
        self.supported_versions.iter().map(AsRef::as_ref)
    }

    /// Returns the untrusted server capability object.
    #[must_use]
    pub fn capabilities(&self) -> &Map<String, Value> {
        &self.capabilities
    }

    /// Returns whether discovery advertised the Tool capability.
    #[must_use]
    pub fn supports_tools(&self) -> bool {
        self.capabilities.contains_key("tools")
    }

    /// Returns the self-reported server implementation name.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the self-reported server implementation version.
    #[must_use]
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Returns untrusted natural-language usage instructions.
    #[must_use]
    pub fn instructions(&self) -> Option<&str> {
        self.instructions.as_deref()
    }

    /// Returns the discovery result's cache metadata.
    #[must_use]
    pub const fn cache(&self) -> &McpCachePolicy {
        &self.cache
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeaderValueKind {
    String,
    Integer,
    Boolean,
}

#[derive(Clone, Debug)]
struct HeaderBinding {
    path: Arc<[Box<str>]>,
    name: Box<str>,
    kind: HeaderValueKind,
}

/// One validated Tool discovered from this exact client binding.
#[derive(Clone)]
pub struct McpTool {
    binding_id: u64,
    name: Box<str>,
    title: Option<Box<str>>,
    description: Option<Box<str>>,
    input_schema: Arc<Value>,
    output_schema: Option<Arc<Value>>,
    raw: Arc<Value>,
    headers: Arc<[HeaderBinding]>,
}

impl McpTool {
    /// Returns the server-scoped Tool name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the optional human-readable title.
    #[must_use]
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Returns the optional untrusted Tool description.
    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    /// Returns the Tool's untrusted input JSON Schema without resolving refs.
    #[must_use]
    pub fn input_schema(&self) -> &Value {
        &self.input_schema
    }

    /// Returns the optional untrusted output JSON Schema without resolving refs.
    #[must_use]
    pub fn output_schema(&self) -> Option<&Value> {
        self.output_schema.as_deref()
    }

    /// Returns the complete bounded wire descriptor, including extensions.
    #[must_use]
    pub fn raw(&self) -> &Value {
        &self.raw
    }
}

impl fmt::Debug for McpTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpTool")
            .field("name", &self.name)
            .field("title", &self.title)
            .field("has_output_schema", &self.output_schema.is_some())
            .field("promoted_header_count", &self.headers.len())
            .finish_non_exhaustive()
    }
}

/// Why one advertised Tool was excluded from the usable catalog.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpToolRejectionReason {
    /// The descriptor was not a JSON object.
    #[error("tool descriptor is not an object")]
    DescriptorNotObject,
    /// The Tool name was absent or outside its implementation bound.
    #[error("tool name is invalid")]
    InvalidName,
    /// `inputSchema` was absent or not a JSON object.
    #[error("tool input schema is invalid")]
    InvalidInputSchema,
    /// `outputSchema`, when present, was not a JSON object.
    #[error("tool output schema is invalid")]
    InvalidOutputSchema,
    /// An `x-mcp-header` annotation was not a string.
    #[error("x-mcp-header annotation is not a string")]
    HeaderNameNotString,
    /// An `x-mcp-header` annotation was empty or not an RFC 9110 token.
    #[error("x-mcp-header annotation is not a valid HTTP token")]
    InvalidHeaderName,
    /// Header annotations were not case-insensitively unique.
    #[error("x-mcp-header annotations contain a duplicate name")]
    DuplicateHeaderName,
    /// An annotation targeted a non-string/integer/boolean property.
    #[error("x-mcp-header annotation targets a non-primitive property")]
    HeaderOnNonPrimitive,
    /// An annotation was reachable only through a forbidden schema keyword.
    #[error("x-mcp-header annotation is not statically property-reachable")]
    HeaderNotStaticallyReachable,
}

/// One excluded Tool and its public-safe reason.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpRejectedTool {
    name: Option<Box<str>>,
    reason: McpToolRejectionReason,
}

impl McpRejectedTool {
    /// Returns the bounded Tool name when it could be read safely.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the exclusion reason.
    #[must_use]
    pub const fn reason(&self) -> McpToolRejectionReason {
        self.reason
    }
}

/// One bounded page from `tools/list`.
#[derive(Clone, Debug)]
pub struct McpToolPage {
    tools: Vec<McpTool>,
    rejected: Vec<McpRejectedTool>,
    next_cursor: Option<Box<str>>,
    cache: McpCachePolicy,
    advertised_count: usize,
}

impl McpToolPage {
    /// Returns usable Tools after invalid header annotations were excluded.
    #[must_use]
    pub fn tools(&self) -> &[McpTool] {
        &self.tools
    }

    /// Returns auditable exclusions without making invalid Tools callable.
    #[must_use]
    pub fn rejected_tools(&self) -> &[McpRejectedTool] {
        &self.rejected
    }

    /// Returns the opaque next-page cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    /// Returns cache metadata for this page.
    #[must_use]
    pub const fn cache(&self) -> &McpCachePolicy {
        &self.cache
    }
}

/// A complete bounded Tool catalog.
#[derive(Clone, Debug)]
pub struct McpToolCatalog {
    tools: Vec<McpTool>,
    rejected: Vec<McpRejectedTool>,
}

impl McpToolCatalog {
    /// Returns all usable Tools in server page order.
    #[must_use]
    pub fn tools(&self) -> &[McpTool] {
        &self.tools
    }

    /// Returns all excluded Tool descriptors in server page order.
    #[must_use]
    pub fn rejected_tools(&self) -> &[McpRejectedTool] {
        &self.rejected
    }

    /// Finds one usable Tool by its exact case-sensitive name.
    #[must_use]
    pub fn find(&self, name: &str) -> Option<&McpTool> {
        self.tools.iter().find(|tool| tool.name() == name)
    }
}

/// One request-scoped server notification received before an SSE final response.
#[derive(Clone, Debug, PartialEq)]
pub struct McpNotification {
    method: Box<str>,
    params: Option<Value>,
    raw: Value,
}

impl McpNotification {
    /// Returns the notification method.
    #[must_use]
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Returns optional untrusted notification parameters.
    #[must_use]
    pub const fn params(&self) -> Option<&Value> {
        self.params.as_ref()
    }

    /// Returns the complete bounded notification.
    #[must_use]
    pub const fn raw(&self) -> &Value {
        &self.raw
    }
}

/// A complete Tool result. Tool-level failures remain results through
/// [`Self::is_error`] so an Agent can inspect and recover from them.
#[derive(Clone, Debug, PartialEq)]
pub struct McpCompleteToolResult {
    content: Vec<Value>,
    structured_content: Option<Value>,
    is_error: bool,
    raw: Value,
}

impl McpCompleteToolResult {
    /// Returns untrusted MCP content blocks.
    #[must_use]
    pub fn content(&self) -> &[Value] {
        &self.content
    }

    /// Returns optional structured output without applying a local trust policy.
    #[must_use]
    pub const fn structured_content(&self) -> Option<&Value> {
        self.structured_content.as_ref()
    }

    /// Returns whether the remote Tool reported an execution failure.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        self.is_error
    }

    /// Returns the complete bounded result object.
    #[must_use]
    pub const fn raw(&self) -> &Value {
        &self.raw
    }
}

/// An input-required Tool result bound to the exact original request.
///
/// Resuming consumes this value, ensuring the opaque `requestState` cannot be
/// accidentally reused by a second retry through the safe API.
pub struct McpInputRequired {
    client: McpClient,
    tool: McpTool,
    arguments: Map<String, Value>,
    input_requests: Map<String, Value>,
    request_state: Option<Box<str>>,
}

impl McpInputRequired {
    /// Returns the server-client requests that must be fulfilled.
    #[must_use]
    pub const fn input_requests(&self) -> &Map<String, Value> {
        &self.input_requests
    }

    /// Returns whether the server supplied opaque state. Its bytes are kept
    /// private so callers cannot accidentally parse and reconstruct it.
    #[must_use]
    pub const fn has_request_state(&self) -> bool {
        self.request_state.is_some()
    }

    /// Retries the original Tool call with one response for every requested key.
    pub async fn resume(
        self,
        input_responses: Map<String, Value>,
    ) -> Result<McpToolCallResponse, StatelessMcpClientError> {
        let expected = self.input_requests.keys().collect::<HashSet<_>>();
        let actual = input_responses.keys().collect::<HashSet<_>>();
        if expected != actual {
            return Err(StatelessMcpClientError::InputResponseKeysMismatch);
        }
        self.client
            .call_tool_round(
                &self.tool,
                self.arguments,
                Some(input_responses),
                self.request_state,
            )
            .await
    }
}

impl fmt::Debug for McpInputRequired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpInputRequired")
            .field("tool", &self.tool.name())
            .field("input_request_count", &self.input_requests.len())
            .field("has_request_state", &self.request_state.is_some())
            .finish_non_exhaustive()
    }
}

/// The protocol outcome of one Tool-call round.
#[derive(Debug)]
#[non_exhaustive]
pub enum McpToolCall {
    /// The server completed the Tool call.
    Complete(McpCompleteToolResult),
    /// The server requires client-side input before a distinct retry.
    InputRequired(McpInputRequired),
}

/// One Tool-call round plus any request-scoped SSE notifications.
#[derive(Debug)]
pub struct McpToolCallResponse {
    outcome: McpToolCall,
    notifications: Vec<McpNotification>,
}

impl McpToolCallResponse {
    /// Returns the complete or input-required outcome.
    #[must_use]
    pub const fn outcome(&self) -> &McpToolCall {
        &self.outcome
    }

    /// Consumes the envelope and returns the outcome.
    #[must_use]
    pub fn into_outcome(self) -> McpToolCall {
        self.outcome
    }

    /// Returns request-scoped notifications in arrival order.
    #[must_use]
    pub fn notifications(&self) -> &[McpNotification] {
        &self.notifications
    }
}

/// A bounded JSON-RPC error returned by the MCP server.
#[derive(Clone, Debug, PartialEq)]
pub struct McpRemoteError {
    code: i32,
    message: Box<str>,
    data: Option<Value>,
}

impl McpRemoteError {
    /// Returns the JSON-RPC error code.
    #[must_use]
    pub const fn code(&self) -> i32 {
        self.code
    }

    /// Returns the untrusted bounded error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns optional untrusted error data.
    #[must_use]
    pub const fn data(&self) -> Option<&Value> {
        self.data.as_ref()
    }
}

impl fmt::Display for McpRemoteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "JSON-RPC error {}", self.code)
    }
}

impl std::error::Error for McpRemoteError {}

/// Closed failure from the modern stateless MCP client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StatelessMcpClientError {
    /// Endpoint normalization failed.
    #[error("invalid MCP endpoint: {0}")]
    Endpoint(#[from] ProviderEndpointError),
    /// The bounded no-redirect HTTP client could not be built.
    #[error("MCP HTTP client construction failed")]
    HttpClient,
    /// Request authorization could not be resolved.
    #[error(transparent)]
    Authorization(#[from] McpAuthorizationError),
    /// The request deadline elapsed. A Tool with external effects may require
    /// authoritative reconciliation before any retry.
    #[error("MCP request deadline elapsed")]
    Timeout,
    /// A unique local request identifier could no longer be allocated.
    #[error("MCP request identifier space is exhausted")]
    RequestIdExhausted,
    /// A unique client binding identifier could no longer be allocated.
    #[error("MCP client binding identifier space is exhausted")]
    ClientBindingIdExhausted,
    /// JSON request serialization failed.
    #[error("MCP request serialization failed")]
    RequestSerialization,
    /// The serialized request exceeded its configured byte ceiling.
    #[error("MCP request exceeded its byte ceiling")]
    RequestTooLarge,
    /// The network exchange failed without a trustworthy protocol outcome.
    #[error("MCP HTTP transport failed")]
    Transport,
    /// The endpoint requires authorization.
    #[error("MCP endpoint requires authorization")]
    AuthorizationRequired,
    /// The supplied authorization lacks permission.
    #[error("MCP endpoint denied permission")]
    PermissionDenied,
    /// The complete response exceeded its configured byte ceiling.
    #[error("MCP response exceeded its byte ceiling")]
    ResponseTooLarge,
    /// The request-scoped event stream violated framing or resource bounds.
    #[error("MCP event stream is invalid")]
    InvalidEventStream,
    /// The response did not use JSON or request-scoped SSE.
    #[error("MCP response has an unsupported content type")]
    UnexpectedContentType,
    /// The endpoint returned an HTTP failure without a valid JSON-RPC error.
    #[error("MCP endpoint returned HTTP status {0}")]
    HttpStatus(u16),
    /// The response was not a valid matching JSON-RPC response.
    #[error("MCP response violated the JSON-RPC contract")]
    Protocol,
    /// The server returned a bounded JSON-RPC error.
    #[error("MCP server returned {0}")]
    Remote(McpRemoteError),
    /// Version negotiation produced no mutually supported modern revision.
    #[error("MCP server does not support protocol version 2026-07-28")]
    UnsupportedProtocolVersion,
    /// Discovery did not advertise the required modern revision.
    #[error("MCP discovery omitted protocol version 2026-07-28")]
    DiscoveryVersionMismatch,
    /// Discovery returned an invalid result.
    #[error("MCP discovery result is invalid")]
    InvalidDiscovery,
    /// A `tools/list` result was invalid.
    #[error("MCP Tool catalog response is invalid")]
    InvalidToolCatalog,
    /// Discovery did not advertise the Tool capability.
    #[error("MCP server did not advertise the Tool capability")]
    ToolsNotAdvertised,
    /// Catalog entries exceeded the configured ceiling.
    #[error("MCP Tool catalog exceeds its entry limit")]
    ToolCatalogTooLarge,
    /// Catalog pagination did not terminate within its configured ceiling.
    #[error("MCP Tool catalog exceeds its page limit")]
    ToolCatalogPageLimit,
    /// A cursor was empty, too large, or repeated.
    #[error("MCP Tool catalog pagination cursor is invalid")]
    InvalidPagination,
    /// A usable Tool name appeared more than once.
    #[error("MCP Tool catalog contains a duplicate usable name")]
    DuplicateToolName,
    /// The Tool descriptor belongs to another endpoint binding.
    #[error("MCP Tool belongs to a different client binding")]
    ForeignToolBinding,
    /// Tool arguments were not a JSON object.
    #[error("MCP Tool arguments must be a JSON object")]
    InvalidToolArguments,
    /// An annotated argument was not a safe value for its declared header type.
    #[error("MCP Tool header argument is invalid")]
    InvalidHeaderArgument,
    /// MRTR response keys did not exactly match the pending input requests.
    #[error("MCP input response keys do not match the pending requests")]
    InputResponseKeysMismatch,
    /// The Tool result was neither complete nor a valid input-required result.
    #[error("MCP Tool result is invalid")]
    InvalidToolResult,
    /// Too many notifications preceded the final SSE response.
    #[error("MCP response exceeded its notification limit")]
    TooManyNotifications,
}

struct McpClientInner {
    http: reqwest::Client,
    endpoint: reqwest::Url,
    identity: McpClientIdentity,
    authorization: Arc<dyn McpClientAuthorizationProvider>,
    options: McpClientOptions,
    concurrency: tokio::sync::Semaphore,
    next_request_id: AtomicU64,
    binding_id: u64,
    server: std::sync::OnceLock<McpClientServer>,
}

/// Connected general-purpose client for the stateless MCP 2026-07-28 Tool
/// surface.
#[derive(Clone)]
pub struct McpClient {
    inner: Arc<McpClientInner>,
}

impl McpClient {
    /// Builds a bounded transport and validates the endpoint through
    /// `server/discover`.
    ///
    /// The only automatic network retry is one `-32022`
    /// `UnsupportedProtocolVersion` retry when the server explicitly lists
    /// this client's version. Redirects and generic HTTP retries remain off.
    pub async fn connect(
        endpoint: ProviderEndpoint,
        identity: McpClientIdentity,
        authorization: Arc<dyn McpClientAuthorizationProvider>,
        options: McpClientOptions,
    ) -> Result<Self, StatelessMcpClientError> {
        let endpoint = endpoint.join("")?;
        let http =
            build_client(options.transport()).map_err(|_| StatelessMcpClientError::HttpClient)?;
        let binding_id = allocate_monotonic_id(&NEXT_CLIENT_BINDING_ID)
            .ok_or(StatelessMcpClientError::ClientBindingIdExhausted)?;
        let client = Self {
            inner: Arc::new(McpClientInner {
                http,
                endpoint,
                identity,
                authorization,
                options,
                concurrency: tokio::sync::Semaphore::new(options.maximum_concurrent_requests()),
                next_request_id: AtomicU64::new(1),
                binding_id,
                server: std::sync::OnceLock::new(),
            }),
        };
        let exchange = client
            .send_rpc("server/discover", None, Map::new(), Vec::new())
            .await?;
        let server = parse_discovery(&exchange.result)?;
        if !server
            .supported_versions()
            .any(|version| version == MCP_PROTOCOL_VERSION_2026_07_28)
        {
            return Err(StatelessMcpClientError::DiscoveryVersionMismatch);
        }
        client
            .inner
            .server
            .set(server)
            .map_err(|_| StatelessMcpClientError::InvalidDiscovery)?;
        Ok(client)
    }

    /// Returns the identity sent on every request.
    #[must_use]
    pub fn identity(&self) -> &McpClientIdentity {
        &self.inner.identity
    }

    /// Returns the immutable discovery snapshot.
    #[must_use]
    pub fn server(&self) -> &McpClientServer {
        self.inner
            .server
            .get()
            .expect("connected MCP clients always retain discovery")
    }

    /// Requests one bounded page of usable Tools.
    pub async fn list_tools_page(
        &self,
        cursor: Option<&str>,
    ) -> Result<McpToolPage, StatelessMcpClientError> {
        if !self.server().supports_tools() {
            return Err(StatelessMcpClientError::ToolsNotAdvertised);
        }
        if cursor.is_some_and(|value| value.is_empty() || value.len() > MAX_CURSOR_BYTES) {
            return Err(StatelessMcpClientError::InvalidPagination);
        }
        let mut params = Map::new();
        if let Some(cursor) = cursor {
            params.insert("cursor".to_owned(), Value::String(cursor.to_owned()));
        }
        let exchange = self
            .send_rpc("tools/list", None, params, Vec::new())
            .await?;
        parse_tool_page(&exchange.result, self.inner.binding_id)
    }

    /// Traverses the complete bounded Tool catalog and rejects cyclic cursors
    /// or duplicate usable names.
    pub async fn list_tools(&self) -> Result<McpToolCatalog, StatelessMcpClientError> {
        let mut cursor: Option<Box<str>> = None;
        let mut seen_cursors = HashSet::new();
        let mut seen_names = HashSet::new();
        let mut tools = Vec::new();
        let mut rejected = Vec::new();
        let mut advertised = 0usize;

        for _ in 0..self.inner.options.maximum_catalog_pages() {
            let page = self.list_tools_page(cursor.as_deref()).await?;
            advertised = advertised
                .checked_add(page.advertised_count)
                .ok_or(StatelessMcpClientError::ToolCatalogTooLarge)?;
            if advertised > self.inner.options.maximum_catalog_tools() {
                return Err(StatelessMcpClientError::ToolCatalogTooLarge);
            }
            for tool in page.tools {
                if !seen_names.insert(tool.name.to_string()) {
                    return Err(StatelessMcpClientError::DuplicateToolName);
                }
                tools.push(tool);
            }
            rejected.extend(page.rejected);
            let Some(next) = page.next_cursor else {
                return Ok(McpToolCatalog { tools, rejected });
            };
            if next.is_empty()
                || next.len() > MAX_CURSOR_BYTES
                || !seen_cursors.insert(next.to_string())
            {
                return Err(StatelessMcpClientError::InvalidPagination);
            }
            cursor = Some(next);
        }
        Err(StatelessMcpClientError::ToolCatalogPageLimit)
    }

    /// Calls one Tool discovered from this exact client binding.
    pub async fn call_tool(
        &self,
        tool: &McpTool,
        arguments: Value,
    ) -> Result<McpToolCallResponse, StatelessMcpClientError> {
        let arguments = arguments
            .as_object()
            .cloned()
            .ok_or(StatelessMcpClientError::InvalidToolArguments)?;
        self.call_tool_round(tool, arguments, None, None).await
    }

    async fn call_tool_round(
        &self,
        tool: &McpTool,
        arguments: Map<String, Value>,
        input_responses: Option<Map<String, Value>>,
        request_state: Option<Box<str>>,
    ) -> Result<McpToolCallResponse, StatelessMcpClientError> {
        if tool.binding_id != self.inner.binding_id {
            return Err(StatelessMcpClientError::ForeignToolBinding);
        }
        let headers = promoted_headers(tool, &arguments)?;
        let mut params = Map::new();
        params.insert("name".to_owned(), Value::String(tool.name.to_string()));
        params.insert("arguments".to_owned(), Value::Object(arguments.clone()));
        if let Some(input_responses) = input_responses {
            params.insert("inputResponses".to_owned(), Value::Object(input_responses));
        }
        if let Some(request_state) = request_state.as_deref() {
            params.insert(
                "requestState".to_owned(),
                Value::String(request_state.to_owned()),
            );
        }
        let exchange = self
            .send_rpc("tools/call", Some(tool.name()), params, headers)
            .await?;
        let outcome = parse_tool_result(exchange.result, self.clone(), tool.clone(), arguments)?;
        Ok(McpToolCallResponse {
            outcome,
            notifications: exchange.notifications,
        })
    }

    async fn send_rpc(
        &self,
        method: &str,
        name: Option<&str>,
        params: Map<String, Value>,
        promoted_headers: Vec<(HeaderName, HeaderValue)>,
    ) -> Result<RpcExchange, StatelessMcpClientError> {
        let deadline = tokio::time::Instant::now() + self.inner.options.request_timeout();
        let mut attempted_version_retry = false;
        loop {
            let response = self
                .send_rpc_once(
                    deadline,
                    method,
                    name,
                    params.clone(),
                    promoted_headers.clone(),
                )
                .await;
            match response {
                Err(StatelessMcpClientError::Remote(error))
                    if error.code() == -32_022 && !attempted_version_retry =>
                {
                    if !remote_supports_current_version(&error) {
                        return Err(StatelessMcpClientError::UnsupportedProtocolVersion);
                    }
                    attempted_version_retry = true;
                }
                other => return other,
            }
        }
    }

    async fn send_rpc_once(
        &self,
        deadline: tokio::time::Instant,
        method: &str,
        name: Option<&str>,
        mut params: Map<String, Value>,
        promoted_headers: Vec<(HeaderName, HeaderValue)>,
    ) -> Result<RpcExchange, StatelessMcpClientError> {
        let id = allocate_monotonic_id(&self.inner.next_request_id)
            .ok_or(StatelessMcpClientError::RequestIdExhausted)?;
        params.insert("_meta".to_owned(), Value::Object(self.request_metadata()));
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .map_err(|_| StatelessMcpClientError::RequestSerialization)?;
        if body.len() > self.inner.options.transport().maximum_request_bytes() {
            return Err(StatelessMcpClientError::RequestTooLarge);
        }

        let authorization_request = McpClientAuthorizationRequest {
            method: method.to_owned().into_boxed_str(),
            name: name.map(|value| value.to_owned().into_boxed_str()),
        };
        let permit = wait_until(deadline, self.inner.concurrency.acquire())
            .await?
            .map_err(|_| StatelessMcpClientError::Transport)?;
        let authorization = wait_until(
            deadline,
            self.inner.authorization.resolve(&authorization_request),
        )
        .await??;

        let mut request = self
            .inner
            .http
            .post(self.inner.endpoint.clone())
            .header(header::ACCEPT, MCP_ACCEPT)
            .header(header::CONTENT_TYPE, MCP_JSON)
            .header(MCP_HEADER_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_2026_07_28)
            .header(MCP_HEADER_METHOD, method);
        if let Some(name) = name {
            request = request.header(MCP_HEADER_NAME, encode_header_value(name));
        }
        match authorization {
            McpAuthorization::Anonymous => {}
            McpAuthorization::Bearer(key) => {
                request = request.bearer_auth(key.expose_secret());
            }
        }
        for (header_name, header_value) in promoted_headers {
            request = request.header(header_name, header_value);
        }
        let response = wait_until(deadline, request.body(body).send())
            .await?
            .map_err(|_| StatelessMcpClientError::Transport)?;
        let output = self.consume_response(deadline, id, response).await;
        drop(permit);
        output
    }

    fn request_metadata(&self) -> Map<String, Value> {
        let mut metadata = Map::new();
        metadata.insert(
            MCP_META_PROTOCOL_VERSION.to_owned(),
            Value::String(MCP_PROTOCOL_VERSION_2026_07_28.to_owned()),
        );
        metadata.insert(
            MCP_META_CLIENT_INFO.to_owned(),
            json!({
                "name": self.inner.identity.name(),
                "version": self.inner.identity.version(),
            }),
        );
        metadata.insert(
            MCP_META_CLIENT_CAPABILITIES.to_owned(),
            Value::Object(Map::new()),
        );
        metadata
    }

    async fn consume_response(
        &self,
        deadline: tokio::time::Instant,
        id: u64,
        response: reqwest::Response,
    ) -> Result<RpcExchange, StatelessMcpClientError> {
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(StatelessMcpClientError::AuthorizationRequired);
        }
        if status == StatusCode::FORBIDDEN {
            return Err(StatelessMcpClientError::PermissionDenied);
        }
        if matches!(status, StatusCode::ACCEPTED | StatusCode::NO_CONTENT) {
            return Err(StatelessMcpClientError::Protocol);
        }
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(normalize_content_type);
        match content_type {
            Some(MCP_JSON) => {
                let body = wait_until(
                    deadline,
                    bounded_body(
                        response,
                        self.inner.options.transport().maximum_response_bytes(),
                    ),
                )
                .await??;
                let value = serde_json::from_slice::<Value>(&body)
                    .map_err(|_| StatelessMcpClientError::Protocol)?;
                match classify_rpc_message(value, id)? {
                    ParsedMessage::Result(result) if status.is_success() => Ok(RpcExchange {
                        result,
                        notifications: Vec::new(),
                    }),
                    ParsedMessage::Remote(error) => Err(StatelessMcpClientError::Remote(error)),
                    _ if !status.is_success() => {
                        Err(StatelessMcpClientError::HttpStatus(status.as_u16()))
                    }
                    _ => Err(StatelessMcpClientError::Protocol),
                }
            }
            Some(MCP_EVENT_STREAM) if status.is_success() => {
                self.consume_event_stream(deadline, id, response).await
            }
            Some(_) | None if !status.is_success() => {
                Err(StatelessMcpClientError::HttpStatus(status.as_u16()))
            }
            Some(_) | None => Err(StatelessMcpClientError::UnexpectedContentType),
        }
    }

    async fn consume_event_stream(
        &self,
        deadline: tokio::time::Instant,
        id: u64,
        response: reqwest::Response,
    ) -> Result<RpcExchange, StatelessMcpClientError> {
        let mut stream = response.bytes_stream();
        let mut decoder = SseDecoder::new(self.inner.options.transport());
        let mut notifications = Vec::new();
        while let Some(chunk) = wait_until(deadline, stream.next()).await? {
            let chunk = chunk.map_err(|_| StatelessMcpClientError::Transport)?;
            let events = decoder.push(&chunk).map_err(map_sse_error)?;
            if let Some(result) = consume_sse_events(
                events,
                id,
                &mut notifications,
                self.inner.options.maximum_notifications_per_response(),
            )? {
                return Ok(RpcExchange {
                    result,
                    notifications,
                });
            }
        }
        let events = decoder.finish().map_err(map_sse_error)?;
        if let Some(result) = consume_sse_events(
            events,
            id,
            &mut notifications,
            self.inner.options.maximum_notifications_per_response(),
        )? {
            return Ok(RpcExchange {
                result,
                notifications,
            });
        }
        Err(StatelessMcpClientError::Protocol)
    }
}

impl fmt::Debug for McpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpClient")
            .field("identity", &self.inner.identity)
            .field("protocol_version", &MCP_PROTOCOL_VERSION_2026_07_28)
            .field("endpoint", &"[REDACTED]")
            .field("connected", &self.inner.server.get().is_some())
            .finish_non_exhaustive()
    }
}

struct RpcExchange {
    result: Value,
    notifications: Vec<McpNotification>,
}

enum ParsedMessage {
    Result(Value),
    Remote(McpRemoteError),
    Notification(McpNotification),
}

fn allocate_monotonic_id(counter: &AtomicU64) -> Option<u64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .ok()
}

async fn wait_until<T, F>(
    deadline: tokio::time::Instant,
    future: F,
) -> Result<T, StatelessMcpClientError>
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| StatelessMcpClientError::Timeout)
}

fn normalize_content_type(value: &str) -> Option<&'static str> {
    let mime = value.split(';').next()?.trim();
    if mime.eq_ignore_ascii_case(MCP_JSON) {
        Some(MCP_JSON)
    } else if mime.eq_ignore_ascii_case(MCP_EVENT_STREAM) {
        Some(MCP_EVENT_STREAM)
    } else {
        None
    }
}

async fn bounded_body(
    response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, StatelessMcpClientError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(StatelessMcpClientError::ResponseTooLarge);
    }
    let mut output = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| StatelessMcpClientError::Transport)?;
        if output.len().saturating_add(chunk.len()) > maximum {
            return Err(StatelessMcpClientError::ResponseTooLarge);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

fn map_sse_error(_error: SseDecodeError) -> StatelessMcpClientError {
    StatelessMcpClientError::InvalidEventStream
}

fn consume_sse_events(
    events: Vec<crate::sse::SseEvent>,
    id: u64,
    notifications: &mut Vec<McpNotification>,
    maximum_notifications: usize,
) -> Result<Option<Value>, StatelessMcpClientError> {
    for event in events {
        let value = serde_json::from_str::<Value>(&event.data)
            .map_err(|_| StatelessMcpClientError::Protocol)?;
        match classify_rpc_message(value, id)? {
            ParsedMessage::Result(result) => return Ok(Some(result)),
            ParsedMessage::Remote(error) => {
                return Err(StatelessMcpClientError::Remote(error));
            }
            ParsedMessage::Notification(notification) => {
                if notifications.len() >= maximum_notifications {
                    return Err(StatelessMcpClientError::TooManyNotifications);
                }
                notifications.push(notification);
            }
        }
    }
    Ok(None)
}

fn classify_rpc_message(
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

fn remote_supports_current_version(error: &McpRemoteError) -> bool {
    error
        .data()
        .and_then(Value::as_object)
        .and_then(|data| data.get("supported"))
        .and_then(Value::as_array)
        .is_some_and(|versions| {
            versions
                .iter()
                .any(|version| version.as_str() == Some(MCP_PROTOCOL_VERSION_2026_07_28))
        })
}

fn parse_discovery(value: &Value) -> Result<McpClientServer, StatelessMcpClientError> {
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

fn parse_cache_policy(
    object: &Map<String, Value>,
) -> Result<McpCachePolicy, StatelessMcpClientError> {
    let ttl_ms = match object.get("ttlMs") {
        Some(value) => Some(value.as_u64().ok_or(StatelessMcpClientError::Protocol)?),
        None => None,
    };
    let scope = bounded_optional_string(object.get("cacheScope"), 64)?;
    Ok(McpCachePolicy { ttl_ms, scope })
}

fn validate_complete_result_type(
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

fn parse_tool_page(value: &Value, binding_id: u64) -> Result<McpToolPage, StatelessMcpClientError> {
    let object = value
        .as_object()
        .ok_or(StatelessMcpClientError::InvalidToolCatalog)?;
    validate_complete_result_type(object, StatelessMcpClientError::InvalidToolCatalog)?;
    let advertised = object
        .get("tools")
        .and_then(Value::as_array)
        .ok_or(StatelessMcpClientError::InvalidToolCatalog)?;
    let advertised_count = advertised.len();
    let mut tools = Vec::with_capacity(advertised_count);
    let mut rejected = Vec::new();
    for descriptor in advertised {
        match parse_tool(descriptor, binding_id) {
            Ok(tool) => tools.push(tool),
            Err((name, reason)) => rejected.push(McpRejectedTool { name, reason }),
        }
    }
    let next_cursor = match object.get("nextCursor") {
        Some(value) => {
            let value = value
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= MAX_CURSOR_BYTES)
                .ok_or(StatelessMcpClientError::InvalidPagination)?;
            Some(value.to_owned().into_boxed_str())
        }
        None => None,
    };
    let cache =
        parse_cache_policy(object).map_err(|_| StatelessMcpClientError::InvalidToolCatalog)?;
    Ok(McpToolPage {
        tools,
        rejected,
        next_cursor,
        cache,
        advertised_count,
    })
}

fn parse_tool(
    descriptor: &Value,
    binding_id: u64,
) -> Result<McpTool, (Option<Box<str>>, McpToolRejectionReason)> {
    let Some(object) = descriptor.as_object() else {
        return Err((None, McpToolRejectionReason::DescriptorNotObject));
    };
    let rejected_name = object
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty() && name.len() <= MAX_TOOL_NAME_BYTES)
        .map(|name| name.to_owned().into_boxed_str());
    let name = rejected_name
        .as_deref()
        .ok_or((None, McpToolRejectionReason::InvalidName))?;
    let input_schema = object
        .get("inputSchema")
        .filter(|schema| schema.is_object())
        .ok_or_else(|| {
            (
                rejected_name.clone(),
                McpToolRejectionReason::InvalidInputSchema,
            )
        })?;
    let output_schema = match object.get("outputSchema") {
        Some(schema) if schema.is_object() => Some(Arc::new(schema.clone())),
        Some(_) => {
            return Err((rejected_name, McpToolRejectionReason::InvalidOutputSchema));
        }
        None => None,
    };
    let headers =
        collect_header_bindings(input_schema).map_err(|reason| (rejected_name.clone(), reason))?;
    Ok(McpTool {
        binding_id,
        name: name.to_owned().into_boxed_str(),
        title: optional_descriptor_text(object.get("title")),
        description: optional_descriptor_text(object.get("description")),
        input_schema: Arc::new(input_schema.clone()),
        output_schema,
        raw: Arc::new(descriptor.clone()),
        headers: headers.into(),
    })
}

fn optional_descriptor_text(value: Option<&Value>) -> Option<Box<str>> {
    value
        .and_then(Value::as_str)
        .filter(|text| text.len() <= 64 * 1024)
        .map(|text| text.to_owned().into_boxed_str())
}

fn collect_header_bindings(
    input_schema: &Value,
) -> Result<Vec<HeaderBinding>, McpToolRejectionReason> {
    let mut output = Vec::new();
    let mut seen = HashSet::new();
    scan_header_schema(input_schema, false, &mut Vec::new(), &mut seen, &mut output)?;
    Ok(output)
}

fn scan_header_schema(
    schema: &Value,
    annotation_allowed: bool,
    path: &mut Vec<Box<str>>,
    seen: &mut HashSet<String>,
    output: &mut Vec<HeaderBinding>,
) -> Result<(), McpToolRejectionReason> {
    let Some(object) = schema.as_object() else {
        if contains_header_annotation(schema) {
            return Err(McpToolRejectionReason::HeaderNotStaticallyReachable);
        }
        return Ok(());
    };
    if let Some(annotation) = object.get("x-mcp-header") {
        if !annotation_allowed {
            return Err(McpToolRejectionReason::HeaderNotStaticallyReachable);
        }
        let name = annotation
            .as_str()
            .ok_or(McpToolRejectionReason::HeaderNameNotString)?;
        if name.is_empty() || !name.chars().all(is_http_token_character) {
            return Err(McpToolRejectionReason::InvalidHeaderName);
        }
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(McpToolRejectionReason::DuplicateHeaderName);
        }
        let kind = match object.get("type").and_then(Value::as_str) {
            Some("string") => HeaderValueKind::String,
            Some("integer") => HeaderValueKind::Integer,
            Some("boolean") => HeaderValueKind::Boolean,
            _ => return Err(McpToolRejectionReason::HeaderOnNonPrimitive),
        };
        output.push(HeaderBinding {
            path: path.clone().into(),
            name: name.to_owned().into_boxed_str(),
            kind,
        });
    }

    for (keyword, child) in object {
        if keyword == "properties" {
            if let Some(properties) = child.as_object() {
                for (property, property_schema) in properties {
                    path.push(property.to_owned().into_boxed_str());
                    scan_header_schema(property_schema, true, path, seen, output)?;
                    path.pop();
                }
            } else if contains_header_annotation(child) {
                return Err(McpToolRejectionReason::HeaderNotStaticallyReachable);
            }
        } else if keyword != "x-mcp-header" && contains_header_annotation(child) {
            return Err(McpToolRejectionReason::HeaderNotStaticallyReachable);
        }
    }
    Ok(())
}

fn contains_header_annotation(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key("x-mcp-header") || object.values().any(contains_header_annotation)
        }
        Value::Array(values) => values.iter().any(contains_header_annotation),
        _ => false,
    }
}

const fn is_http_token_character(character: char) -> bool {
    character.is_ascii_alphanumeric()
        || matches!(
            character,
            '!' | '#'
                | '$'
                | '%'
                | '&'
                | '\''
                | '*'
                | '+'
                | '-'
                | '.'
                | '^'
                | '_'
                | '`'
                | '|'
                | '~'
        )
}

fn promoted_headers(
    tool: &McpTool,
    arguments: &Map<String, Value>,
) -> Result<Vec<(HeaderName, HeaderValue)>, StatelessMcpClientError> {
    let mut output = Vec::with_capacity(tool.headers.len());
    for binding in tool.headers.iter() {
        let Some(value) = value_at_path(arguments, &binding.path) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let value = match binding.kind {
            HeaderValueKind::String => value
                .as_str()
                .map(ToOwned::to_owned)
                .ok_or(StatelessMcpClientError::InvalidHeaderArgument)?,
            HeaderValueKind::Boolean => value
                .as_bool()
                .map(|value| value.to_string())
                .ok_or(StatelessMcpClientError::InvalidHeaderArgument)?,
            HeaderValueKind::Integer => {
                safe_integer_string(value).ok_or(StatelessMcpClientError::InvalidHeaderArgument)?
            }
        };
        let name = format!("{MCP_HEADER_PARAMETER_PREFIX}{}", binding.name);
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| StatelessMcpClientError::InvalidHeaderArgument)?;
        let value = HeaderValue::from_str(&encode_header_value(&value))
            .map_err(|_| StatelessMcpClientError::InvalidHeaderArgument)?;
        output.push((name, value));
    }
    Ok(output)
}

fn value_at_path<'a>(arguments: &'a Map<String, Value>, path: &[Box<str>]) -> Option<&'a Value> {
    let (first, remaining) = path.split_first()?;
    let mut current = arguments.get(first.as_ref())?;
    for component in remaining {
        current = current.as_object()?.get(component.as_ref())?;
    }
    Some(current)
}

fn safe_integer_string(value: &Value) -> Option<String> {
    const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
    if let Some(value) = value.as_i64() {
        return (value.unsigned_abs() <= MAX_SAFE_INTEGER.unsigned_abs())
            .then(|| value.to_string());
    }
    if let Some(value) = value.as_u64() {
        return (value <= MAX_SAFE_INTEGER as u64).then(|| value.to_string());
    }
    let value = value.as_f64()?;
    if !value.is_finite() || value.fract() != 0.0 || value.abs() > 9_007_199_254_740_991.0 {
        return None;
    }
    Some(format!("{value:.0}"))
}

fn encode_header_value(value: &str) -> String {
    if requires_base64(value) {
        format!(
            "{BASE64_PREFIX}{}{BASE64_SUFFIX}",
            BASE64_STANDARD.encode(value.as_bytes())
        )
    } else {
        value.to_owned()
    }
}

fn requires_base64(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let bytes = value.as_bytes();
    matches!(bytes.first(), Some(b' ' | b'\t'))
        || matches!(bytes.last(), Some(b' ' | b'\t'))
        || bytes.iter().any(|byte| !(0x20..=0x7e).contains(byte))
        || (value.starts_with(BASE64_PREFIX) && value.ends_with(BASE64_SUFFIX))
}

fn parse_tool_result(
    value: Value,
    client: McpClient,
    tool: McpTool,
    arguments: Map<String, Value>,
) -> Result<McpToolCall, StatelessMcpClientError> {
    let object = value
        .as_object()
        .ok_or(StatelessMcpClientError::InvalidToolResult)?;
    let result_type = match object.get("resultType") {
        Some(value) => Some(
            value
                .as_str()
                .ok_or(StatelessMcpClientError::InvalidToolResult)?,
        ),
        None => None,
    };
    match result_type {
        None | Some("complete") => {
            let content = object
                .get("content")
                .and_then(Value::as_array)
                .cloned()
                .ok_or(StatelessMcpClientError::InvalidToolResult)?;
            let is_error = match object.get("isError") {
                Some(value) => value
                    .as_bool()
                    .ok_or(StatelessMcpClientError::InvalidToolResult)?,
                None => false,
            };
            Ok(McpToolCall::Complete(McpCompleteToolResult {
                content,
                structured_content: object.get("structuredContent").cloned(),
                is_error,
                raw: value,
            }))
        }
        Some("input_required") => {
            let input_requests = match object.get("inputRequests") {
                Some(value) => value
                    .as_object()
                    .cloned()
                    .ok_or(StatelessMcpClientError::InvalidToolResult)?,
                None => Map::new(),
            };
            for (key, request) in &input_requests {
                let request = request
                    .as_object()
                    .ok_or(StatelessMcpClientError::InvalidToolResult)?;
                if key.is_empty()
                    || key.len() > 1024
                    || !matches!(
                        request.get("method").and_then(Value::as_str),
                        Some("elicitation/create" | "sampling/createMessage" | "roots/list")
                    )
                {
                    return Err(StatelessMcpClientError::InvalidToolResult);
                }
            }
            let request_state = match object.get("requestState") {
                Some(value) => Some(
                    value
                        .as_str()
                        .ok_or(StatelessMcpClientError::InvalidToolResult)?
                        .to_owned()
                        .into_boxed_str(),
                ),
                None => None,
            };
            Ok(McpToolCall::InputRequired(McpInputRequired {
                client,
                tool,
                arguments,
                input_requests,
                request_state,
            }))
        }
        Some(_) => Err(StatelessMcpClientError::InvalidToolResult),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn identity_and_options_are_bounded() {
        assert!(McpClientIdentity::new("stateknot", "0.0.0").is_ok());
        assert_eq!(
            McpClientIdentity::new(" stateknot", "0.0.0"),
            Err(McpClientIdentityError::BoundaryWhitespace)
        );
        assert_eq!(
            McpClientOptions::new(ProviderHttpOptions::default(), Duration::ZERO, 1, 1, 1, 1,),
            Err(McpClientOptionsError::ZeroTimeout)
        );
        assert_eq!(
            McpClientOptions::new(
                ProviderHttpOptions::default(),
                Duration::from_secs(24 * 60 * 60 + 1),
                1,
                1,
                1,
                1,
            ),
            Err(McpClientOptionsError::AboveHardMaximum)
        );
    }

    #[test]
    fn nested_headers_are_validated_and_encoded_without_schema_fetches() {
        let descriptor = json!({
            "name": "deploy",
            "inputSchema": {
                "type": "object",
                "$defs": {
                    "remote": { "$ref": "https://canary.invalid/schema.json" }
                },
                "properties": {
                    "region": { "type": "string", "x-mcp-header": "Region" },
                    "options": {
                        "type": "object",
                        "properties": {
                            "priority": { "type": "integer", "x-mcp-header": "Priority" },
                            "confirm": { "type": "boolean", "x-mcp-header": "Confirm" },
                            "note": { "type": "string", "x-mcp-header": "Note" }
                        }
                    }
                }
            }
        });
        let tool = parse_tool(&descriptor, 7).unwrap();
        let arguments = json!({
            "region": "us-west1",
            "options": {
                "priority": 42,
                "confirm": false,
                "note": "Hello, 世界"
            }
        });
        let headers = promoted_headers(&tool, arguments.as_object().unwrap()).unwrap();
        let headers = headers
            .into_iter()
            .map(|(name, value)| (name.as_str().to_owned(), value.to_str().unwrap().to_owned()))
            .collect::<HashMap<_, _>>();
        assert_eq!(headers.get("mcp-param-region").unwrap(), "us-west1");
        assert_eq!(headers.get("mcp-param-priority").unwrap(), "42");
        assert_eq!(headers.get("mcp-param-confirm").unwrap(), "false");
        assert_eq!(
            headers.get("mcp-param-note").unwrap(),
            "=?base64?SGVsbG8sIOS4lueVjA==?="
        );
    }

    #[test]
    fn invalid_or_unreachable_header_annotations_exclude_only_that_tool() {
        let duplicate = json!({
            "name": "duplicate",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "a": { "type": "string", "x-mcp-header": "Region" },
                    "b": { "type": "string", "x-mcp-header": "region" }
                }
            }
        });
        assert_eq!(
            parse_tool(&duplicate, 1).unwrap_err().1,
            McpToolRejectionReason::DuplicateHeaderName
        );

        let through_ref = json!({
            "name": "through-ref",
            "inputSchema": {
                "type": "object",
                "$defs": {
                    "tenant": { "type": "string", "x-mcp-header": "Tenant" }
                },
                "properties": { "tenant": { "$ref": "#/$defs/tenant" } }
            }
        });
        assert_eq!(
            parse_tool(&through_ref, 1).unwrap_err().1,
            McpToolRejectionReason::HeaderNotStaticallyReachable
        );

        let malformed_properties = json!({
            "name": "malformed-properties",
            "inputSchema": {
                "type": "object",
                "properties": [{
                    "type": "string",
                    "x-mcp-header": "Tenant"
                }]
            }
        });
        assert_eq!(
            parse_tool(&malformed_properties, 1).unwrap_err().1,
            McpToolRejectionReason::HeaderNotStaticallyReachable
        );

        let number = json!({
            "name": "number",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "ratio": { "type": "number", "x-mcp-header": "Ratio" }
                }
            }
        });
        assert_eq!(
            parse_tool(&number, 1).unwrap_err().1,
            McpToolRejectionReason::HeaderOnNonPrimitive
        );
    }

    #[test]
    fn parses_modern_discovery_and_matching_json_rpc() {
        let server = parse_discovery(&json!({
            "resultType": "complete",
            "supportedVersions": [MCP_PROTOCOL_VERSION_2026_07_28],
            "capabilities": { "tools": {} },
            "_meta": {
                (MCP_META_SERVER_INFO): { "name": "example", "version": "1.0.0" }
            },
            "ttlMs": 1000,
            "cacheScope": "private"
        }))
        .unwrap();
        assert_eq!(server.name(), Some("example"));
        assert_eq!(server.cache().ttl_ms(), Some(1000));
        assert!(server.supports_tools());

        assert!(matches!(
            classify_rpc_message(
                json!({ "jsonrpc": "2.0", "id": 9, "result": { "ok": true } }),
                9,
            )
            .unwrap(),
            ParsedMessage::Result(_)
        ));
        assert!(
            classify_rpc_message(json!({ "jsonrpc": "2.0", "id": 8, "result": {} }), 9,).is_err()
        );
    }

    #[test]
    fn unsafe_header_values_and_sentinel_are_base64_encoded() {
        assert_eq!(encode_header_value("plain value"), "plain value");
        assert_eq!(encode_header_value(""), "");
        assert_eq!(encode_header_value(" padded "), "=?base64?IHBhZGRlZCA=?=");
        assert_eq!(
            encode_header_value("=?base64?literal?="),
            "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?="
        );
        assert_eq!(safe_integer_string(&Value::from(i64::MIN)), None);
    }
}
