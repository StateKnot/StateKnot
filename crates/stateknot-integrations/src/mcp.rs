// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Strict MCP 2026-07-28 remote-tool binding.
//!
//! This first production profile intentionally supports modern, stateless
//! Streamable HTTP with complete JSON responses only. Discovery happens once
//! while the immutable binding is built. Each admitted tool attempt then sends
//! exactly one `tools/call` request. Stateful sessions, SSE, transparent
//! reconnects, legacy initialization, MRTR, Tasks, progress forwarding, and
//! artifact materialization fail closed instead of changing retry semantics.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use futures_util::{StreamExt, stream::BoxStream};
use http::{HeaderName, HeaderValue};
use reqwest::{StatusCode, header};
use rmcp::{
    ClientLifecycleMode, ClientServiceExt, RoleClient,
    model::{
        CallToolRequestParams, CallToolResponse, ClientCapabilities, ClientInfo,
        ClientJsonRpcMessage, Implementation, JsonRpcMessage, PaginatedRequestParams,
        ProtocolVersion, ServerJsonRpcMessage, Tool,
    },
    service::RunningService,
    transport::{
        StreamableHttpClientTransport,
        common::client_side_sse::NeverRetry,
        streamable_http_client::{
            AuthRequiredError, InsufficientScopeError, SseError, StreamableHttpClient,
            StreamableHttpClientTransportConfig, StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use serde_json::Value;
use sse_stream::Sse;
use stateknot_core::{
    BoundedJson, BoxFuture, DurationMillis, ErasedTool, Failure, FailureCategory, FailureCode,
    FailureId, FailureMessage, FailureOrigin, GraphSchemaValidator, ModelSchemaRegistry,
    RetryAdvice, ToolArtifacts, ToolContext, ToolDescriptor, ToolError, ToolErrorPhase,
    ToolErrorProvenance, ToolExternalEffect, ToolInput, ToolResult, ToolRisk, ToolStopReason,
};
use thiserror::Error;

use crate::{
    ApiKey, ProviderEndpoint, ProviderEndpointError, ProviderHttpOptions, http::build_client,
};

const MCP_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::V_2026_07_28;
const MCP_JSON: &str = "application/json";
const MCP_SESSION_ID: &str = "mcp-session-id";

/// Schema capabilities required by the MCP adapter.
///
/// The local registry remains the authority. Remote schema documents are used
/// only as discovery evidence and must exactly match the locally pinned RFC
/// 8785 canonical bytes.
pub trait McpSchemaRegistry: ModelSchemaRegistry + GraphSchemaValidator {}

impl<T> McpSchemaRegistry for T where T: ModelSchemaRegistry + GraphSchemaValidator {}

/// Exact implementation identity expected from modern MCP discovery.
#[derive(Clone, Eq, PartialEq)]
pub struct McpServerIdentity {
    name: Box<str>,
    version: Box<str>,
}

impl McpServerIdentity {
    /// Maximum bytes accepted for either identity component.
    pub const MAX_COMPONENT_BYTES: usize = 256;

    /// Constructs a bounded, printable exact server identity.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, whitespace-padded, or control-containing
    /// components.
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self, McpServerIdentityError> {
        let name = name.into();
        let version = version.into();
        validate_identity_component(&name, McpServerIdentityComponent::Name)?;
        validate_identity_component(&version, McpServerIdentityComponent::Version)?;
        Ok(Self {
            name: name.into_boxed_str(),
            version: version.into_boxed_str(),
        })
    }

    /// Returns the exact implementation name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the exact implementation version.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    fn matches(&self, implementation: &Implementation) -> bool {
        self.name() == implementation.name && self.version() == implementation.version
    }
}

impl fmt::Debug for McpServerIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerIdentity")
            .field("name", &self.name)
            .field("version", &self.version)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum McpServerIdentityComponent {
    Name,
    Version,
}

/// Invalid expected MCP server identity.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerIdentityError {
    /// A required component was empty.
    #[error("MCP server identity component is empty")]
    Empty,
    /// A component exceeded its hard byte ceiling.
    #[error("MCP server identity component is too long")]
    TooLong,
    /// Trimming would change a component.
    #[error("MCP server identity component has boundary whitespace")]
    BoundaryWhitespace,
    /// A component contained a control character.
    #[error("MCP server identity component contains a control character")]
    ControlCharacter,
}

fn validate_identity_component(
    value: &str,
    _component: McpServerIdentityComponent,
) -> Result<(), McpServerIdentityError> {
    if value.is_empty() {
        return Err(McpServerIdentityError::Empty);
    }
    if value.len() > McpServerIdentity::MAX_COMPONENT_BYTES {
        return Err(McpServerIdentityError::TooLong);
    }
    if value.trim() != value {
        return Err(McpServerIdentityError::BoundaryWhitespace);
    }
    if value.chars().any(char::is_control) {
        return Err(McpServerIdentityError::ControlCharacter);
    }
    Ok(())
}

/// Bounded transport and discovery policy for one MCP binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpHttpOptions {
    transport: ProviderHttpOptions,
    startup_timeout: Duration,
    shutdown_timeout: Duration,
    maximum_discovery_pages: usize,
    maximum_discovered_tools: usize,
}

impl McpHttpOptions {
    /// Hard page ceiling for a single tool-catalog discovery.
    pub const HARD_MAXIMUM_DISCOVERY_PAGES: usize = 64;
    /// Hard tool ceiling for a single endpoint binding.
    pub const HARD_MAXIMUM_DISCOVERED_TOOLS: usize = 4096;

    /// Constructs explicit bounded MCP transport policy.
    ///
    /// # Errors
    ///
    /// Rejects zero or implementation-exceeding limits.
    pub const fn new(
        transport: ProviderHttpOptions,
        startup_timeout: Duration,
        shutdown_timeout: Duration,
        maximum_discovery_pages: usize,
        maximum_discovered_tools: usize,
    ) -> Result<Self, McpHttpOptionsError> {
        if startup_timeout.is_zero() || shutdown_timeout.is_zero() {
            return Err(McpHttpOptionsError::ZeroTimeout);
        }
        if maximum_discovery_pages == 0 || maximum_discovered_tools == 0 {
            return Err(McpHttpOptionsError::ZeroDiscoveryLimit);
        }
        if maximum_discovery_pages > Self::HARD_MAXIMUM_DISCOVERY_PAGES
            || maximum_discovered_tools > Self::HARD_MAXIMUM_DISCOVERED_TOOLS
        {
            return Err(McpHttpOptionsError::AboveHardMaximum);
        }
        Ok(Self {
            transport,
            startup_timeout,
            shutdown_timeout,
            maximum_discovery_pages,
            maximum_discovered_tools,
        })
    }

    /// Returns the complete HTTP resource policy.
    #[must_use]
    pub const fn transport(self) -> ProviderHttpOptions {
        self.transport
    }

    /// Returns the total startup discovery deadline.
    #[must_use]
    pub const fn startup_timeout(self) -> Duration {
        self.startup_timeout
    }

    /// Returns the graceful transport shutdown bound.
    #[must_use]
    pub const fn shutdown_timeout(self) -> Duration {
        self.shutdown_timeout
    }

    /// Returns the maximum accepted catalog pages.
    #[must_use]
    pub const fn maximum_discovery_pages(self) -> usize {
        self.maximum_discovery_pages
    }

    /// Returns the maximum accepted tools across all pages.
    #[must_use]
    pub const fn maximum_discovered_tools(self) -> usize {
        self.maximum_discovered_tools
    }
}

impl Default for McpHttpOptions {
    fn default() -> Self {
        Self {
            transport: ProviderHttpOptions::default(),
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
            maximum_discovery_pages: 16,
            maximum_discovered_tools: 1024,
        }
    }
}

/// Invalid MCP transport policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpHttpOptionsError {
    /// Startup and shutdown deadlines must be positive.
    #[error("MCP startup and shutdown timeouts must be positive")]
    ZeroTimeout,
    /// Catalog bounds must be positive.
    #[error("MCP discovery limits must be positive")]
    ZeroDiscoveryLimit,
    /// A catalog bound exceeded the implementation ceiling.
    #[error("MCP discovery limit exceeds the implementation maximum")]
    AboveHardMaximum,
}

/// Attempt-scoped HTTP authorization material.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum McpAuthorization {
    /// The endpoint is intentionally called without an Authorization header.
    Anonymous,
    /// The endpoint receives an RFC 6750 bearer credential.
    Bearer(ApiKey),
}

/// Public-safe failure from an MCP authorization source.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpAuthorizationError {
    /// The secret backend is temporarily unavailable.
    #[error("MCP authorization source is unavailable")]
    Unavailable,
    /// Policy refused secret access for this binding or attempt.
    #[error("MCP authorization access was denied")]
    PermissionDenied,
}

/// Credential source for startup discovery and each admitted tool attempt.
///
/// Implementations should resolve secret handles rather than embedding tenant
/// credentials in descriptors. The transport copies no secret into MCP
/// metadata and never formats authorization material.
pub trait McpAuthorizationProvider: Send + Sync + 'static {
    /// Resolves authorization for bounded startup discovery.
    fn resolve_startup(
        &self,
        descriptor: &ToolDescriptor,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>>;

    /// Resolves authorization for one exact durable attempt.
    fn resolve_attempt(
        &self,
        context: &ToolContext,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>>;
}

/// Explicit anonymous authorization provider.
#[derive(Clone, Copy, Debug, Default)]
pub struct AnonymousMcpAuthorization;

impl McpAuthorizationProvider for AnonymousMcpAuthorization {
    fn resolve_startup(
        &self,
        _descriptor: &ToolDescriptor,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        Box::pin(async { Ok(McpAuthorization::Anonymous) })
    }

    fn resolve_attempt(
        &self,
        _context: &ToolContext,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        Box::pin(async { Ok(McpAuthorization::Anonymous) })
    }
}

impl crate::mcp_client::McpClientAuthorizationProvider for AnonymousMcpAuthorization {
    fn resolve(
        &self,
        _request: &crate::mcp_client::McpClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        Box::pin(async { Ok(McpAuthorization::Anonymous) })
    }
}

/// Immutable bearer authorization for controlled single-tenant bindings.
#[derive(Clone)]
pub struct StaticMcpBearerAuthorization {
    key: Arc<ApiKey>,
}

impl StaticMcpBearerAuthorization {
    /// Wraps one validated, zeroizing bearer credential.
    #[must_use]
    pub fn new(key: ApiKey) -> Self {
        Self { key: Arc::new(key) }
    }
}

impl fmt::Debug for StaticMcpBearerAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StaticMcpBearerAuthorization([REDACTED])")
    }
}

impl McpAuthorizationProvider for StaticMcpBearerAuthorization {
    fn resolve_startup(
        &self,
        _descriptor: &ToolDescriptor,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        let key = self.key.as_ref().clone();
        Box::pin(async move { Ok(McpAuthorization::Bearer(key)) })
    }

    fn resolve_attempt(
        &self,
        _context: &ToolContext,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        let key = self.key.as_ref().clone();
        Box::pin(async move { Ok(McpAuthorization::Bearer(key)) })
    }
}

impl crate::mcp_client::McpClientAuthorizationProvider for StaticMcpBearerAuthorization {
    fn resolve(
        &self,
        _request: &crate::mcp_client::McpClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        let key = self.key.as_ref().clone();
        Box::pin(async move { Ok(McpAuthorization::Bearer(key)) })
    }
}

#[derive(Clone)]
struct AuthorizationSlot {
    inner: Arc<RwLock<McpAuthorization>>,
}

impl AuthorizationSlot {
    fn anonymous() -> Self {
        Self {
            inner: Arc::new(RwLock::new(McpAuthorization::Anonymous)),
        }
    }

    fn replace(&self, authorization: McpAuthorization) -> Result<(), StrictHttpError> {
        *self
            .inner
            .write()
            .map_err(|_| StrictHttpError::AuthorizationState)? = authorization;
        Ok(())
    }

    fn snapshot(&self) -> Result<McpAuthorization, StrictHttpError> {
        self.inner
            .read()
            .map_err(|_| StrictHttpError::AuthorizationState)
            .map(|value| value.clone())
    }
}

struct AuthorizationReset<'a>(&'a AuthorizationSlot);

impl Drop for AuthorizationReset<'_> {
    fn drop(&mut self) {
        let _ = self.0.replace(McpAuthorization::Anonymous);
    }
}

#[derive(Clone)]
struct StrictJsonHttpClient {
    client: reqwest::Client,
    authorization: AuthorizationSlot,
    maximum_request_bytes: usize,
    maximum_response_bytes: usize,
}

impl fmt::Debug for StrictJsonHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StrictJsonHttpClient")
            .field("maximum_request_bytes", &self.maximum_request_bytes)
            .field("maximum_response_bytes", &self.maximum_response_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Error)]
enum StrictHttpError {
    #[error("MCP HTTP authorization state is unavailable")]
    AuthorizationState,
    #[error("MCP JSON request serialization failed")]
    RequestSerialization,
    #[error("MCP JSON request exceeded its byte ceiling")]
    RequestTooLarge,
    #[error("MCP HTTP transport failed")]
    Transport,
    #[error("MCP HTTP response exceeded its byte ceiling")]
    ResponseTooLarge,
}

impl StreamableHttpClient for StrictJsonHttpClient {
    type Error = StrictHttpError;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        if session_id.is_some() {
            return Err(StreamableHttpError::UnexpectedServerResponse(
                "StateKnot MCP bindings require a stateless server".into(),
            ));
        }
        if auth_header.is_some() {
            return Err(StreamableHttpError::ReservedHeaderConflict(
                "authorization".to_owned(),
            ));
        }
        let body = serde_json::to_vec(&message)
            .map_err(|_| StreamableHttpError::Client(StrictHttpError::RequestSerialization))?;
        if body.len() > self.maximum_request_bytes {
            return Err(StreamableHttpError::Client(
                StrictHttpError::RequestTooLarge,
            ));
        }

        let mut request = self
            .client
            .post(uri.as_ref())
            .header(header::ACCEPT, MCP_JSON)
            .header(header::CONTENT_TYPE, MCP_JSON);
        match self
            .authorization
            .snapshot()
            .map_err(StreamableHttpError::Client)?
        {
            McpAuthorization::Anonymous => {}
            McpAuthorization::Bearer(key) => {
                request = request.bearer_auth(key.expose_secret());
            }
        }
        request = apply_protocol_headers(request, custom_headers)?;
        let response = request
            .body(body)
            .send()
            .await
            .map_err(|_| StreamableHttpError::Client(StrictHttpError::Transport))?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            return Err(StreamableHttpError::AuthRequired(AuthRequiredError::new(
                "Bearer".to_owned(),
            )));
        }
        if status == StatusCode::FORBIDDEN {
            return Err(StreamableHttpError::InsufficientScope(
                InsufficientScopeError::new("Bearer".to_owned(), None),
            ));
        }
        if response.headers().contains_key(MCP_SESSION_ID) {
            return Err(StreamableHttpError::UnexpectedServerResponse(
                "StateKnot MCP bindings reject stateful sessions".into(),
            ));
        }

        let is_control_message = matches!(
            message,
            ClientJsonRpcMessage::Notification(_)
                | ClientJsonRpcMessage::Response(_)
                | ClientJsonRpcMessage::Error(_)
        );
        if matches!(status, StatusCode::ACCEPTED | StatusCode::NO_CONTENT) {
            return if is_control_message {
                Ok(StreamableHttpPostResponse::Accepted)
            } else {
                Err(StreamableHttpError::UnexpectedServerResponse(
                    "MCP request returned no complete JSON response".into(),
                ))
            };
        }

        let content_type_is_json = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(is_json_content_type);
        let body = bounded_response_body(response, self.maximum_response_bytes)
            .await
            .map_err(StreamableHttpError::Client)?;
        if status.is_success() && body.is_empty() && is_control_message {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if !content_type_is_json {
            return Err(StreamableHttpError::UnexpectedContentType(None));
        }
        let parsed = serde_json::from_slice::<ServerJsonRpcMessage>(&body)?;
        if !status.is_success() && !matches!(parsed, JsonRpcMessage::Error(_)) {
            return Err(StreamableHttpError::UnexpectedServerResponse(
                "MCP endpoint returned a non-success response".into(),
            ));
        }
        Ok(StreamableHttpPostResponse::Json(parsed, None))
    }

    async fn delete_session(
        &self,
        _uri: Arc<str>,
        _session_id: Arc<str>,
        _auth_header: Option<String>,
        _custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        Err(StreamableHttpError::ServerDoesNotSupportDeleteSession)
    }

    async fn get_stream(
        &self,
        _uri: Arc<str>,
        _session_id: Option<Arc<str>>,
        _last_event_id: Option<String>,
        _auth_header: Option<String>,
        _custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        Err(StreamableHttpError::ServerDoesNotSupportSse)
    }
}

fn apply_protocol_headers(
    mut request: reqwest::RequestBuilder,
    headers: HashMap<HeaderName, HeaderValue>,
) -> Result<reqwest::RequestBuilder, StreamableHttpError<StrictHttpError>> {
    for (name, value) in headers {
        if is_reserved_header(&name) {
            return Err(StreamableHttpError::ReservedHeaderConflict(
                name.as_str().to_owned(),
            ));
        }
        request = request.header(name, value);
    }
    Ok(request)
}

fn is_reserved_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "accept"
            | "authorization"
            | "connection"
            | "content-length"
            | "content-type"
            | "cookie"
            | "host"
            | "proxy-authorization"
            | "set-cookie"
            | "transfer-encoding"
    )
}

fn is_json_content_type(value: &str) -> bool {
    value
        .split(';')
        .next()
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case(MCP_JSON))
}

async fn bounded_response_body(
    response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, StrictHttpError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(StrictHttpError::ResponseTooLarge);
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| StrictHttpError::Transport)?;
        if body.len().saturating_add(chunk.len()) > maximum {
            return Err(StrictHttpError::ResponseTooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// A connected, exact-schema MCP tool exposed through `StateKnot`'s erased tool
/// contract.
pub struct McpRemoteTool {
    descriptor: ToolDescriptor,
    remote_name: Box<str>,
    schemas: Arc<dyn McpSchemaRegistry>,
    authorizations: Arc<dyn McpAuthorizationProvider>,
    authorization_slot: AuthorizationSlot,
    call_gate: tokio::sync::Mutex<()>,
    service: RunningService<RoleClient, ClientInfo>,
}

impl McpRemoteTool {
    /// Connects, performs strict modern discovery, verifies server identity,
    /// scans a bounded catalog, and freezes exact input/output schema bytes.
    ///
    /// The endpoint is never contacted again for discovery during tool calls.
    /// A server upgrade therefore requires constructing and registering a new
    /// exact adapter binding.
    ///
    /// # Errors
    ///
    /// Returns a closed build failure for unsafe descriptor semantics,
    /// unavailable local schemas, authorization failure, timeout, protocol
    /// downgrade, identity/schema drift, or bounded catalog exhaustion.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        descriptor: ToolDescriptor,
        remote_name: impl Into<String>,
        endpoint: ProviderEndpoint,
        expected_server: McpServerIdentity,
        schemas: Arc<dyn McpSchemaRegistry>,
        authorizations: Arc<dyn McpAuthorizationProvider>,
        options: McpHttpOptions,
    ) -> Result<Self, McpRemoteToolBuildError> {
        let remote_name = remote_name.into();
        validate_remote_tool_name(&remote_name)?;
        if descriptor.semantics().requires_idempotency_key() {
            return Err(McpRemoteToolBuildError::RequiredIdempotencyKeyUnsupported);
        }
        if descriptor.invocation().supports_progress_events() {
            return Err(McpRemoteToolBuildError::ProgressUnsupported);
        }
        let local_input = schemas
            .canonical_schema_bytes(descriptor.input_schema())
            .ok_or(McpRemoteToolBuildError::InputSchemaUnavailable)?
            .to_vec();
        let local_output = schemas
            .canonical_schema_bytes(descriptor.output_schema())
            .ok_or(McpRemoteToolBuildError::OutputSchemaUnavailable)?
            .to_vec();
        let url = endpoint
            .join("")
            .map_err(McpRemoteToolBuildError::Endpoint)?;
        let client =
            build_client(options.transport()).map_err(|_| McpRemoteToolBuildError::HttpClient)?;
        let authorization_slot = AuthorizationSlot::anonymous();
        let http_client = StrictJsonHttpClient {
            client,
            authorization: authorization_slot.clone(),
            maximum_request_bytes: options.transport().maximum_request_bytes(),
            maximum_response_bytes: options.transport().maximum_response_bytes(),
        };
        let mut transport_config =
            StreamableHttpClientTransportConfig::with_uri(Arc::<str>::from(url.as_str()));
        transport_config.retry_config = Arc::new(NeverRetry::default());
        transport_config.channel_buffer_capacity = 8;
        transport_config.max_concurrent_requests = 1;
        transport_config.control_request_timeout = options.shutdown_timeout();
        transport_config.session_recovery_timeout = options.shutdown_timeout();
        transport_config.allow_stateless = true;
        transport_config.auth_header = None;
        transport_config.custom_headers = HashMap::new();
        transport_config.max_sse_event_size = options.transport().maximum_response_bytes();
        transport_config.reinit_on_expired_session = false;
        let transport = StreamableHttpClientTransport::with_client(http_client, transport_config);
        let startup_deadline = tokio::time::Instant::now() + options.startup_timeout();
        let startup_authorization = startup_wait(
            startup_deadline,
            authorizations.resolve_startup(&descriptor),
        )
        .await??;
        authorization_slot
            .replace(startup_authorization)
            .map_err(|_| McpRemoteToolBuildError::AuthorizationState)?;
        let _authorization_reset = AuthorizationReset(&authorization_slot);

        let client_info = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("stateknot", env!("CARGO_PKG_VERSION")),
        )
        .with_protocol_version(MCP_PROTOCOL_VERSION.clone());
        let mut service = startup_wait(
            startup_deadline,
            client_info.serve_with_lifecycle(
                transport,
                ClientLifecycleMode::Discover {
                    preferred_versions: vec![MCP_PROTOCOL_VERSION.clone()],
                },
            ),
        )
        .await?
        .map_err(|_| McpRemoteToolBuildError::DiscoveryProtocol)?;

        let verification = verify_remote_binding(
            &service,
            startup_deadline,
            &expected_server,
            &remote_name,
            &local_input,
            &local_output,
            options,
        )
        .await;
        if let Err(error) = verification {
            let _ = service.close_with_timeout(options.shutdown_timeout()).await;
            return Err(error);
        }

        Ok(Self {
            descriptor,
            remote_name: remote_name.into_boxed_str(),
            schemas,
            authorizations,
            authorization_slot: authorization_slot.clone(),
            call_gate: tokio::sync::Mutex::new(()),
            service,
        })
    }

    fn preparation_error(
        &self,
        context: &ToolContext,
        category: FailureCategory,
        code: &'static str,
        message: &'static str,
        retry: RetryAdvice,
    ) -> ToolError {
        self.error(
            context,
            ToolErrorPhase::Preparation,
            category,
            code,
            message,
            retry,
            effect_before_dispatch(self.descriptor.semantics().risk()),
        )
    }

    fn dispatched_error(
        &self,
        context: &ToolContext,
        read_category: FailureCategory,
        read_code: &'static str,
        read_message: &'static str,
        read_retry: RetryAdvice,
    ) -> ToolError {
        if self.descriptor.semantics().risk() == ToolRisk::ReadOnly {
            self.error(
                context,
                ToolErrorPhase::Execution,
                read_category,
                read_code,
                read_message,
                read_retry,
                ToolExternalEffect::NotApplicable,
            )
        } else {
            self.error(
                context,
                ToolErrorPhase::Execution,
                FailureCategory::AmbiguousExternalOutcome,
                "call.outcome_unknown",
                "The MCP write may have been applied; reconcile it before recovery.",
                RetryAdvice::ReconcileFirst,
                ToolExternalEffect::Unknown,
            )
        }
    }

    fn invalid_result(&self, context: &ToolContext) -> ToolError {
        self.error(
            context,
            ToolErrorPhase::Result,
            FailureCategory::DataCorruption,
            "result.invalid",
            "The MCP server returned a result outside the pinned tool contract.",
            RetryAdvice::Never,
            effect_after_success(self.descriptor.semantics().risk()),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn error(
        &self,
        context: &ToolContext,
        phase: ToolErrorPhase,
        category: FailureCategory,
        code: &'static str,
        message: &'static str,
        retry: RetryAdvice,
        effect: ToolExternalEffect,
    ) -> ToolError {
        let failure = Failure::new(
            FailureId::generate(),
            category,
            FailureCode::new(code).expect("MCP failure codes are valid constants"),
            FailureOrigin::new("protocol.mcp").expect("MCP failure origin is valid"),
            FailureMessage::new(message).expect("MCP public failure messages are valid"),
            retry,
        )
        .expect("MCP failure category and retry advice are coherent");
        ToolError::new(
            failure,
            phase,
            effect,
            ToolErrorProvenance::for_invocation(context, &self.descriptor),
        )
        .expect("MCP phase, risk evidence, and failure category are coherent")
    }

    #[allow(clippy::too_many_lines)]
    async fn call_inner(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> Result<ToolResult, ToolError> {
        input
            .validate_for(&context, &self.descriptor)
            .map_err(|_| {
                self.preparation_error(
                    &context,
                    FailureCategory::InvalidInput,
                    "input.binding_invalid",
                    "The MCP tool input does not match this invocation binding.",
                    RetryAdvice::Never,
                )
            })?;
        self.schemas
            .validate(self.descriptor.input_schema(), input.value())
            .map_err(|_| {
                self.preparation_error(
                    &context,
                    FailureCategory::InvalidInput,
                    "input.schema_rejected",
                    "The MCP tool input failed its pinned schema.",
                    RetryAdvice::Never,
                )
            })?;

        let _call_guard = wait_for_tool(&context, self.call_gate.lock())
            .await
            .map_err(|reason| self.stop_before_dispatch(&context, reason))?;
        let authorization = wait_for_tool(&context, self.authorizations.resolve_attempt(&context))
            .await
            .map_err(|reason| self.stop_before_dispatch(&context, reason))?
            .map_err(|error| match error {
                McpAuthorizationError::Unavailable => self.preparation_error(
                    &context,
                    FailureCategory::DependencyUnavailable,
                    "authorization.unavailable",
                    "The MCP authorization source is temporarily unavailable.",
                    RetryAdvice::SafeAfter {
                        delay: DurationMillis::new(250).expect("positive constant"),
                    },
                ),
                McpAuthorizationError::PermissionDenied => self.preparation_error(
                    &context,
                    FailureCategory::PermissionDenied,
                    "authorization.permission_denied",
                    "MCP authorization access was denied.",
                    RetryAdvice::Never,
                ),
            })?;
        self.authorization_slot
            .replace(authorization)
            .map_err(|_| {
                self.preparation_error(
                    &context,
                    FailureCategory::Internal,
                    "authorization.state_unavailable",
                    "The MCP authorization state is unavailable.",
                    RetryAdvice::Never,
                )
            })?;
        let _authorization_reset = AuthorizationReset(&self.authorization_slot);

        let arguments = input
            .value()
            .as_value()
            .as_object()
            .cloned()
            .expect("ToolInput construction requires an object root");
        let params =
            CallToolRequestParams::new(self.remote_name.to_string()).with_arguments(arguments);
        let response = wait_for_tool(&context, self.service.call_tool_once(params))
            .await
            .map_err(|reason| self.stop_after_dispatch(&context, reason))?
            .map_err(|_| {
                self.dispatched_error(
                    &context,
                    FailureCategory::DependencyUnavailable,
                    "call.protocol_failed",
                    "The MCP tool call failed at the protocol boundary.",
                    RetryAdvice::SafeAfter {
                        delay: DurationMillis::new(250).expect("positive constant"),
                    },
                )
            })?;
        let CallToolResponse::Complete(result) = response else {
            return Err(self.dispatched_error(
                &context,
                FailureCategory::Unsupported,
                "call.incomplete_unsupported",
                "The MCP server returned an unsupported incomplete result.",
                RetryAdvice::Never,
            ));
        };
        if !result
            .result_type
            .as_ref()
            .is_some_and(rmcp::model::ResultType::is_complete)
        {
            return Err(self.invalid_result(&context));
        }
        if result.is_error.unwrap_or(false) {
            return Err(self.dispatched_error(
                &context,
                FailureCategory::DependencyUnavailable,
                "call.remote_tool_error",
                "The remote MCP tool reported an execution failure.",
                RetryAdvice::Never,
            ));
        }
        if result
            .content
            .iter()
            .any(|content| content.as_text().is_none())
        {
            return Err(self.invalid_result(&context));
        }
        let structured = result
            .structured_content
            .ok_or_else(|| self.invalid_result(&context))?;
        let output =
            BoundedJson::try_from_value(structured).map_err(|_| self.invalid_result(&context))?;
        self.schemas
            .validate(self.descriptor.output_schema(), &output)
            .map_err(|_| self.invalid_result(&context))?;
        let result =
            ToolResult::for_invocation(&context, &self.descriptor, output, ToolArtifacts::empty());
        result
            .validate_for(&context, &self.descriptor)
            .map_err(|_| self.invalid_result(&context))?;
        Ok(result)
    }

    fn stop_before_dispatch(&self, context: &ToolContext, reason: ToolStopReason) -> ToolError {
        let (category, code, message) = stop_failure(reason);
        self.preparation_error(context, category, code, message, RetryAdvice::Never)
    }

    fn stop_after_dispatch(&self, context: &ToolContext, reason: ToolStopReason) -> ToolError {
        let (category, code, message) = stop_failure(reason);
        self.dispatched_error(context, category, code, message, RetryAdvice::Never)
    }
}

impl ErasedTool for McpRemoteTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn call(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(self.call_inner(context, input))
    }
}

impl fmt::Debug for McpRemoteTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpRemoteTool")
            .field("descriptor", &self.descriptor)
            .field("remote_name", &self.remote_name)
            .field("connected", &!self.service.is_closed())
            .finish_non_exhaustive()
    }
}

async fn verify_remote_binding(
    service: &RunningService<RoleClient, ClientInfo>,
    deadline: tokio::time::Instant,
    expected_server: &McpServerIdentity,
    remote_name: &str,
    local_input: &[u8],
    local_output: &[u8],
    options: McpHttpOptions,
) -> Result<(), McpRemoteToolBuildError> {
    let peer = service
        .peer_info()
        .ok_or(McpRemoteToolBuildError::MissingPeerInfo)?;
    if peer.protocol_version != MCP_PROTOCOL_VERSION {
        return Err(McpRemoteToolBuildError::ProtocolVersionMismatch);
    }
    if peer.capabilities.tools.is_none() {
        return Err(McpRemoteToolBuildError::ToolsCapabilityUnavailable);
    }
    let implementation = peer
        .server_info
        .as_ref()
        .ok_or(McpRemoteToolBuildError::ServerIdentityUnavailable)?;
    if !expected_server.matches(implementation) {
        return Err(McpRemoteToolBuildError::ServerIdentityMismatch);
    }

    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    let mut seen_names = HashSet::new();
    let mut discovered = 0usize;
    let mut selected = None;
    let mut catalog_complete = false;
    for _ in 0..options.maximum_discovery_pages() {
        let result = startup_wait(
            deadline,
            service.list_tools(Some(
                PaginatedRequestParams::default().with_cursor(cursor.clone()),
            )),
        )
        .await?
        .map_err(|_| McpRemoteToolBuildError::ToolCatalogProtocol)?;
        if !result
            .result_type
            .as_ref()
            .is_some_and(rmcp::model::ResultType::is_complete)
            || result.ttl_ms.is_none()
            || result.cache_scope.is_none()
        {
            return Err(McpRemoteToolBuildError::ToolCatalogProtocol);
        }
        discovered = discovered
            .checked_add(result.tools.len())
            .ok_or(McpRemoteToolBuildError::ToolCatalogTooLarge)?;
        if discovered > options.maximum_discovered_tools() {
            return Err(McpRemoteToolBuildError::ToolCatalogTooLarge);
        }
        for tool in result.tools {
            let name = tool.name.to_string();
            if !seen_names.insert(name.clone()) {
                return Err(McpRemoteToolBuildError::DuplicateRemoteTool);
            }
            if name == remote_name {
                selected = Some(tool);
            }
        }
        let Some(next) = result.next_cursor else {
            catalog_complete = true;
            break;
        };
        if next.is_empty() || !seen_cursors.insert(next.clone()) {
            return Err(McpRemoteToolBuildError::InvalidPagination);
        }
        cursor = Some(next);
    }
    if !catalog_complete {
        return Err(McpRemoteToolBuildError::ToolCatalogPageLimit);
    }
    let selected = selected.ok_or(McpRemoteToolBuildError::RemoteToolUnavailable)?;
    verify_remote_schemas(&selected, local_input, local_output)
}

fn verify_remote_schemas(
    tool: &Tool,
    local_input: &[u8],
    local_output: &[u8],
) -> Result<(), McpRemoteToolBuildError> {
    let input =
        serde_json_canonicalizer::to_vec(&Value::Object(tool.input_schema.as_ref().clone()))
            .map_err(|_| McpRemoteToolBuildError::RemoteSchemaInvalid)?;
    if input != local_input {
        return Err(McpRemoteToolBuildError::InputSchemaMismatch);
    }
    let output = tool
        .output_schema
        .as_ref()
        .ok_or(McpRemoteToolBuildError::RemoteOutputSchemaUnavailable)?;
    let output = serde_json_canonicalizer::to_vec(&Value::Object(output.as_ref().clone()))
        .map_err(|_| McpRemoteToolBuildError::RemoteSchemaInvalid)?;
    if output != local_output {
        return Err(McpRemoteToolBuildError::OutputSchemaMismatch);
    }
    Ok(())
}

async fn startup_wait<T, F>(
    deadline: tokio::time::Instant,
    future: F,
) -> Result<T, McpRemoteToolBuildError>
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| McpRemoteToolBuildError::StartupTimeout)
}

async fn wait_for_tool<T, F>(context: &ToolContext, future: F) -> Result<T, ToolStopReason>
where
    F: std::future::Future<Output = T>,
{
    if let Some(reason) = context.stop_reason_at(Instant::now()) {
        return Err(reason);
    }
    let deadline = tokio::time::Instant::from_std(context.deadline_instant());
    tokio::select! {
        biased;
        () = context.cancellation().cancelled() => Err(ToolStopReason::Cancelled),
        () = tokio::time::sleep_until(deadline) => {
            Err(context.stop_reason_at(Instant::now()).unwrap_or(ToolStopReason::DeadlineExceeded))
        }
        output = future => Ok(output),
    }
}

fn validate_remote_tool_name(value: &str) -> Result<(), McpRemoteToolBuildError> {
    const MAX_BYTES: usize = 256;
    if value.is_empty()
        || value.len() > MAX_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(McpRemoteToolBuildError::InvalidRemoteToolName);
    }
    Ok(())
}

const fn effect_before_dispatch(risk: ToolRisk) -> ToolExternalEffect {
    match risk {
        ToolRisk::ReadOnly => ToolExternalEffect::NotApplicable,
        ToolRisk::IdempotentWrite | ToolRisk::NonIdempotentWrite => ToolExternalEffect::NotStarted,
    }
}

const fn effect_after_success(risk: ToolRisk) -> ToolExternalEffect {
    match risk {
        ToolRisk::ReadOnly => ToolExternalEffect::NotApplicable,
        ToolRisk::IdempotentWrite | ToolRisk::NonIdempotentWrite => ToolExternalEffect::Applied,
    }
}

const fn stop_failure(reason: ToolStopReason) -> (FailureCategory, &'static str, &'static str) {
    match reason {
        ToolStopReason::Cancelled => (
            FailureCategory::Cancelled,
            "call.cancelled",
            "The MCP tool attempt was cancelled.",
        ),
        ToolStopReason::DeadlineExceeded => (
            FailureCategory::DeadlineExceeded,
            "call.deadline_exceeded",
            "The MCP tool attempt exceeded its deadline.",
        ),
        _ => (
            FailureCategory::Internal,
            "call.unknown_stop_reason",
            "The MCP tool attempt stopped for an unsupported reason.",
        ),
    }
}

/// Closed failure while constructing an exact MCP remote-tool binding.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpRemoteToolBuildError {
    /// The configured remote tool name was not a bounded printable value.
    #[error("MCP remote tool name is invalid")]
    InvalidRemoteToolName,
    /// Generic MCP cannot inject `StateKnot`'s durable idempotency key safely.
    #[error("MCP binding does not support descriptor-required idempotency keys")]
    RequiredIdempotencyKeyUnsupported,
    /// This adapter version does not bridge MCP progress into the durable sink.
    #[error("MCP binding does not support declared progress events")]
    ProgressUnsupported,
    /// The exact local input schema was absent.
    #[error("MCP input schema is unavailable in the local registry")]
    InputSchemaUnavailable,
    /// The exact local output schema was absent.
    #[error("MCP output schema is unavailable in the local registry")]
    OutputSchemaUnavailable,
    /// Endpoint normalization failed.
    #[error("invalid MCP endpoint: {0}")]
    Endpoint(ProviderEndpointError),
    /// The bounded, no-redirect HTTP client could not be built.
    #[error("MCP HTTP client construction failed")]
    HttpClient,
    /// Startup authorization was unavailable or denied.
    #[error(transparent)]
    Authorization(#[from] McpAuthorizationError),
    /// Shared authorization state could not be installed.
    #[error("MCP authorization state is unavailable")]
    AuthorizationState,
    /// The complete discovery deadline elapsed.
    #[error("MCP startup discovery timed out")]
    StartupTimeout,
    /// `server/discover` failed or returned an unsupported shape.
    #[error("MCP modern discovery failed")]
    DiscoveryProtocol,
    /// Discovery did not retain peer information.
    #[error("MCP discovery returned no peer information")]
    MissingPeerInfo,
    /// The negotiated version was not exactly 2026-07-28.
    #[error("MCP protocol version does not match 2026-07-28")]
    ProtocolVersionMismatch,
    /// The server did not advertise tools.
    #[error("MCP server does not advertise the tools capability")]
    ToolsCapabilityUnavailable,
    /// Modern discovery omitted implementation identity.
    #[error("MCP server identity is unavailable")]
    ServerIdentityUnavailable,
    /// The discovered implementation identity drifted.
    #[error("MCP server identity does not match the pinned binding")]
    ServerIdentityMismatch,
    /// A tools/list exchange failed or omitted required modern fields.
    #[error("MCP tool catalog response is invalid")]
    ToolCatalogProtocol,
    /// Catalog entries exceeded the configured bound.
    #[error("MCP tool catalog exceeds its tool limit")]
    ToolCatalogTooLarge,
    /// Catalog pagination did not terminate within its bound.
    #[error("MCP tool catalog exceeds its page limit")]
    ToolCatalogPageLimit,
    /// A cursor was empty or repeated.
    #[error("MCP tool catalog pagination is invalid")]
    InvalidPagination,
    /// A tool name appeared more than once.
    #[error("MCP tool catalog contains a duplicate name")]
    DuplicateRemoteTool,
    /// The exact configured tool was not present.
    #[error("MCP remote tool is unavailable")]
    RemoteToolUnavailable,
    /// Remote schema canonicalization failed.
    #[error("MCP remote schema is invalid")]
    RemoteSchemaInvalid,
    /// The remote input schema differed from the local pin.
    #[error("MCP remote input schema does not match the local pin")]
    InputSchemaMismatch,
    /// The remote tool omitted an output schema.
    #[error("MCP remote output schema is unavailable")]
    RemoteOutputSchemaUnavailable,
    /// The remote output schema differed from the local pin.
    #[error("MCP remote output schema does not match the local pin")]
    OutputSchemaMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_and_options_are_hard_bounded() {
        assert!(McpServerIdentity::new("server", "1.0.0").is_ok());
        assert_eq!(
            McpServerIdentity::new(" server", "1.0.0"),
            Err(McpServerIdentityError::BoundaryWhitespace)
        );
        assert_eq!(
            McpHttpOptions::new(
                ProviderHttpOptions::default(),
                Duration::ZERO,
                Duration::from_secs(1),
                1,
                1,
            ),
            Err(McpHttpOptionsError::ZeroTimeout)
        );
    }

    #[test]
    fn reserved_headers_and_content_types_fail_closed() {
        assert!(is_reserved_header(&header::AUTHORIZATION));
        assert!(!is_reserved_header(&HeaderName::from_static("mcp-method")));
        assert!(is_json_content_type("application/json; charset=utf-8"));
        assert!(!is_json_content_type("text/event-stream"));
    }

    #[test]
    fn credentials_are_redacted() {
        let value = "mcp-production-secret";
        let authorization = McpAuthorization::Bearer(ApiKey::new(value).unwrap());
        assert!(!format!("{authorization:?}").contains(value));
        assert!(
            !format!(
                "{:?}",
                StaticMcpBearerAuthorization::new(ApiKey::new(value).unwrap())
            )
            .contains(value)
        );
    }
}
