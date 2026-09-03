// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Strict, resource-bounded A2A 1.0 client boundary.
//!
//! Discovery and interface endpoints are separately pinned. The client never
//! follows redirects, never retries HTTP requests, validates one immutable
//! Agent Card snapshot before use, and resolves bearer credentials for every
//! operation. HTTP+JSON and JSON-RPC are implemented behind the same
//! StateKnot-owned contracts; wire SDK types never escape this crate.

use std::{
    collections::{HashSet, VecDeque},
    fmt,
    net::IpAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use a2a as wire;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::{StreamExt, stream};
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use reqwest::Url;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use stateknot_core::{
    AttemptId, BoundedJson, BoxFuture, Digest, InvocationId, JsonLimits, RunId, TenantId,
};
use thiserror::Error;

use crate::{
    A2A_AGENT_CARD_PATH, A2A_PROTOCOL_VERSION_1_0, A2aAgentCard, A2aAgentInterface, A2aBinding,
    A2aCancelTaskRequest, A2aContractError, A2aDeletePushConfigRequest, A2aGetPushConfigRequest,
    A2aGetTaskRequest, A2aListPushConfigsRequest, A2aListTasksRequest, A2aMessage, A2aMessageRole,
    A2aPushConfig, A2aPushConfigPage, A2aSendMessageRequest, A2aSendMessageResponse,
    A2aStreamEvent, A2aSubscribeTaskRequest, A2aTask, A2aTaskPage, A2aTaskState, ApiKey,
    ProviderHttpOptions,
    http::build_client,
    sse::{SseDecoder, SseEvent},
};

const A2A_JSON: &str = "application/a2a+json";
const JSON: &str = "application/json";
const A2A_JSON_ACCEPT: &str = "application/a2a+json, application/json";
const EVENT_STREAM: &str = "text/event-stream";
const A2A_VERSION_HEADER: &str = "a2a-version";
const A2A_EXTENSIONS_HEADER: &str = "a2a-extensions";
const MAX_EXTENSION_HEADER_BYTES: usize = 8 * 1024;
const MAX_EXTENSION_COUNT: usize = 64;
const MAX_SECURITY_SCHEME_BYTES: usize = 512;
const MAX_TENANT_BYTES: usize = 512;
const MAX_REMOTE_ERROR_MESSAGE_BYTES: usize = 16 * 1024;

/// Exact public Agent Card endpoint used for one discovery exchange.
#[derive(Clone, Eq, PartialEq)]
pub struct A2aAgentCardEndpoint {
    url: Url,
    transport: A2aEndpointTransport,
}

/// Exact egress-approved Agent Card interface.
#[derive(Clone, Eq, PartialEq)]
pub struct A2aClientInterfacePin {
    url: Url,
    binding: A2aBinding,
    tenant: Option<Box<str>>,
    transport: A2aEndpointTransport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum A2aEndpointTransport {
    Https,
    LoopbackHttp,
}

impl A2aAgentCardEndpoint {
    /// Constructs an HTTPS well-known Agent Card endpoint.
    pub fn https(value: &str) -> Result<Self, A2aClientEndpointError> {
        Self::parse(value, A2aEndpointTransport::Https)
    }

    /// Constructs a test/sidecar endpoint using a literal loopback address.
    pub fn loopback_http(value: &str) -> Result<Self, A2aClientEndpointError> {
        Self::parse(value, A2aEndpointTransport::LoopbackHttp)
    }

    fn parse(value: &str, transport: A2aEndpointTransport) -> Result<Self, A2aClientEndpointError> {
        let url = parse_a2a_url(value, transport)?;
        if url.path() != A2A_AGENT_CARD_PATH {
            return Err(A2aClientEndpointError::AgentCardPathRequired);
        }
        Ok(Self { url, transport })
    }
}

impl A2aClientInterfacePin {
    /// Pins one production HTTPS interface and its binding.
    pub fn https(value: &str, binding: A2aBinding) -> Result<Self, A2aClientEndpointError> {
        Self::parse(value, binding, A2aEndpointTransport::Https)
    }

    /// Pins one literal-loopback interface for tests or a managed sidecar.
    pub fn loopback_http(value: &str, binding: A2aBinding) -> Result<Self, A2aClientEndpointError> {
        Self::parse(value, binding, A2aEndpointTransport::LoopbackHttp)
    }

    fn parse(
        value: &str,
        binding: A2aBinding,
        transport: A2aEndpointTransport,
    ) -> Result<Self, A2aClientEndpointError> {
        let url = parse_a2a_url(value, transport)?;
        Ok(Self {
            url,
            binding,
            tenant: None,
            transport,
        })
    }

    /// Pins the exact opaque tenant advertised by this interface.
    pub fn with_tenant(
        mut self,
        tenant: impl Into<String>,
    ) -> Result<Self, A2aClientEndpointError> {
        let tenant = tenant.into();
        if tenant.is_empty()
            || tenant.len() > MAX_TENANT_BYTES
            || tenant.trim() != tenant
            || tenant.chars().any(char::is_control)
        {
            return Err(A2aClientEndpointError::InvalidTenant);
        }
        self.tenant = Some(tenant.into_boxed_str());
        Ok(self)
    }

    /// Returns the pinned transport binding.
    #[must_use]
    pub const fn binding(&self) -> A2aBinding {
        self.binding
    }

    /// Returns the exact remote tenant approved for this interface.
    #[must_use]
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }
}

fn parse_a2a_url(
    value: &str,
    transport: A2aEndpointTransport,
) -> Result<Url, A2aClientEndpointError> {
    let url = Url::parse(value).map_err(|_| A2aClientEndpointError::InvalidUrl)?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(A2aClientEndpointError::EmbeddedCredentials);
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(A2aClientEndpointError::QueryOrFragment);
    }
    let host = url.host_str().ok_or(A2aClientEndpointError::MissingHost)?;
    match transport {
        A2aEndpointTransport::Https if url.scheme() != "https" => {
            return Err(A2aClientEndpointError::HttpsRequired);
        }
        A2aEndpointTransport::LoopbackHttp => {
            if url.scheme() != "http" {
                return Err(A2aClientEndpointError::LoopbackHttpRequired);
            }
            let address = host
                .parse::<IpAddr>()
                .map_err(|_| A2aClientEndpointError::LiteralLoopbackRequired)?;
            if !address.is_loopback() {
                return Err(A2aClientEndpointError::LiteralLoopbackRequired);
            }
        }
        A2aEndpointTransport::Https => {}
    }
    Ok(url)
}

impl fmt::Debug for A2aAgentCardEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("A2aAgentCardEndpoint")
            .field("transport", &self.transport)
            .field("url", &"[REDACTED]")
            .finish()
    }
}

impl fmt::Debug for A2aClientInterfacePin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("A2aClientInterfacePin")
            .field("binding", &self.binding)
            .field("transport", &self.transport)
            .field("url", &"[REDACTED]")
            .field("tenant", &self.tenant.as_ref().map(|_| "[PINNED]"))
            .finish()
    }
}

/// Invalid A2A discovery or interface endpoint.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aClientEndpointError {
    /// The value was not an absolute URL.
    #[error("A2A endpoint is not an absolute URL")]
    InvalidUrl,
    /// Production endpoints require HTTPS.
    #[error("A2A endpoint must use HTTPS")]
    HttpsRequired,
    /// A local endpoint must use HTTP.
    #[error("loopback A2A endpoint must use HTTP")]
    LoopbackHttpRequired,
    /// Local HTTP is restricted to literal loopback addresses.
    #[error("loopback A2A endpoint must use a literal loopback IP address")]
    LiteralLoopbackRequired,
    /// The URL had no host.
    #[error("A2A endpoint must include a host")]
    MissingHost,
    /// Userinfo could disclose credentials.
    #[error("A2A endpoint must not contain embedded credentials")]
    EmbeddedCredentials,
    /// Endpoint query or fragment data is not immutable routing state.
    #[error("A2A endpoint must not contain a query or fragment")]
    QueryOrFragment,
    /// Discovery must use the registered well-known path.
    #[error("A2A Agent Card endpoint must use /.well-known/agent-card.json")]
    AgentCardPathRequired,
    /// The interface URL could not accept a protocol path segment.
    #[error("A2A interface cannot be used as an HTTP+JSON base URL")]
    InvalidInterfaceBase,
    /// The exact remote tenant pin was empty, unbounded, or not printable.
    #[error("A2A interface tenant pin is invalid")]
    InvalidTenant,
}

/// Trust applied to the exact public Agent Card bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum A2aAgentCardTrust {
    /// Trust the verified HTTPS server identity and an exact interface pin.
    TlsServerIdentity,
    /// Require the RFC 8785 canonical Agent Card bytes to match this SHA-256.
    CanonicalSha256(Digest),
}

/// Calculates the content digest used by [`A2aAgentCardTrust::CanonicalSha256`].
pub fn a2a_agent_card_digest(value: &Value) -> Result<Digest, A2aContractError> {
    let bytes =
        serde_json_canonicalizer::to_vec(value).map_err(|_| A2aContractError::InvalidJson {
            field: "Agent Card canonical form",
        })?;
    Ok(Digest::sha256(bytes))
}

/// Bounded client resource and deadline policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct A2aClientOptions {
    transport: ProviderHttpOptions,
    discovery_timeout: Duration,
    request_timeout: Duration,
    stream_idle_timeout: Duration,
    maximum_stream_events: u32,
}

impl A2aClientOptions {
    /// Hard maximum for one response stream.
    pub const HARD_MAXIMUM_STREAM_EVENTS: u32 = 65_536;
    /// Hard maximum for any client-managed timeout.
    pub const HARD_MAXIMUM_TIMEOUT: Duration = Duration::from_secs(15 * 60);

    /// Constructs explicit finite client policy.
    pub const fn new(
        transport: ProviderHttpOptions,
        discovery_timeout: Duration,
        request_timeout: Duration,
        stream_idle_timeout: Duration,
        maximum_stream_events: u32,
    ) -> Result<Self, A2aClientOptionsError> {
        if discovery_timeout.is_zero() || request_timeout.is_zero() || stream_idle_timeout.is_zero()
        {
            return Err(A2aClientOptionsError::ZeroTimeout);
        }
        if discovery_timeout.as_nanos() > Self::HARD_MAXIMUM_TIMEOUT.as_nanos()
            || request_timeout.as_nanos() > Self::HARD_MAXIMUM_TIMEOUT.as_nanos()
            || stream_idle_timeout.as_nanos() > Self::HARD_MAXIMUM_TIMEOUT.as_nanos()
        {
            return Err(A2aClientOptionsError::TimeoutAboveHardMaximum);
        }
        if maximum_stream_events == 0 || maximum_stream_events > Self::HARD_MAXIMUM_STREAM_EVENTS {
            return Err(A2aClientOptionsError::InvalidStreamEventLimit);
        }
        Ok(Self {
            transport,
            discovery_timeout,
            request_timeout,
            stream_idle_timeout,
            maximum_stream_events,
        })
    }

    /// Returns underlying HTTP byte and connection limits.
    #[must_use]
    pub const fn transport(self) -> ProviderHttpOptions {
        self.transport
    }
}

impl Default for A2aClientOptions {
    fn default() -> Self {
        Self {
            transport: ProviderHttpOptions::default(),
            discovery_timeout: Duration::from_secs(15),
            request_timeout: Duration::from_secs(60),
            stream_idle_timeout: Duration::from_secs(60),
            maximum_stream_events: 4096,
        }
    }
}

/// Invalid bounded client policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aClientOptionsError {
    /// Every timeout must be positive.
    #[error("A2A client timeouts must be positive")]
    ZeroTimeout,
    /// A timeout exceeded fifteen minutes.
    #[error("A2A client timeout exceeds the hard maximum")]
    TimeoutAboveHardMaximum,
    /// Stream count was zero or above the hard ceiling.
    #[error("A2A stream event limit is invalid")]
    InvalidStreamEventLimit,
}

/// One A2A operation used for authorization and audit correlation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum A2aClientOperation {
    /// Send one message.
    SendMessage,
    /// Send one message and receive SSE events.
    SendStreamingMessage,
    /// Read one task.
    GetTask,
    /// List tasks.
    ListTasks,
    /// Request task cancellation.
    CancelTask,
    /// Subscribe to an existing task.
    SubscribeToTask,
    /// Create a task push configuration.
    CreatePushConfig,
    /// Read a push configuration.
    GetPushConfig,
    /// List task push configurations.
    ListPushConfigs,
    /// Delete a push configuration.
    DeletePushConfig,
    /// Fetch the authenticated extended Agent Card.
    GetExtendedAgentCard,
}

/// Optional durable attempt identity visible to a scoped token provider.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::struct_field_names)]
pub struct A2aClientAttemptIdentity {
    tenant_id: TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
}

impl A2aClientAttemptIdentity {
    pub(crate) fn new(
        tenant_id: TenantId,
        run_id: RunId,
        invocation_id: InvocationId,
        attempt_id: AttemptId,
    ) -> Self {
        Self {
            tenant_id,
            run_id,
            invocation_id,
            attempt_id,
        }
    }

    /// Returns the local tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the durable run identifier.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the logical outbound invocation identifier.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the physical dispatch attempt identifier.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }
}

/// Public-safe authorization context for one exact request.
#[derive(Clone, Debug)]
pub struct A2aClientAuthorizationRequest {
    operation: A2aClientOperation,
    card_digest: Digest,
    binding: A2aBinding,
    remote_tenant: Option<Box<str>>,
    task_id: Option<Box<str>>,
    required_scopes: Arc<[Box<str>]>,
    attempt: Option<A2aClientAttemptIdentity>,
}

impl A2aClientAuthorizationRequest {
    /// Returns the exact operation.
    #[must_use]
    pub const fn operation(&self) -> A2aClientOperation {
        self.operation
    }

    /// Returns the immutable public-card digest.
    #[must_use]
    pub const fn card_digest(&self) -> Digest {
        self.card_digest
    }

    /// Returns the selected binding.
    #[must_use]
    pub const fn binding(&self) -> A2aBinding {
        self.binding
    }

    /// Returns the exact remote routing tenant selected from the Agent Card.
    #[must_use]
    pub fn remote_tenant(&self) -> Option<&str> {
        self.remote_tenant.as_deref()
    }

    /// Returns the target task when the operation is task-scoped.
    #[must_use]
    pub fn task_id(&self) -> Option<&str> {
        self.task_id.as_deref()
    }

    /// Returns scopes declared by the selected Agent Card requirement.
    pub fn required_scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.required_scopes.iter().map(AsRef::as_ref)
    }

    /// Returns durable attempt correlation when invoked by the graph runtime.
    #[must_use]
    pub const fn attempt(&self) -> Option<&A2aClientAttemptIdentity> {
        self.attempt.as_ref()
    }
}

/// Public-safe failure from an out-of-band bearer-token source.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aClientAuthorizationError {
    /// The credential backend is temporarily unavailable.
    #[error("A2A authorization source is unavailable")]
    Unavailable,
    /// Policy denied credential access for this request.
    #[error("A2A authorization access was denied")]
    PermissionDenied,
}

/// Attempt-scoped bearer-token source. Token acquisition remains out of band.
pub trait A2aBearerTokenProvider: Send + Sync + 'static {
    /// Resolves a short-lived token for one exact request.
    fn resolve(
        &self,
        request: &A2aClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<ApiKey, A2aClientAuthorizationError>>;
}

/// Immutable bearer token for controlled deployments.
#[derive(Clone)]
pub struct StaticA2aBearerToken {
    token: Arc<ApiKey>,
}

impl StaticA2aBearerToken {
    /// Wraps one validated, zeroizing token.
    #[must_use]
    pub fn new(token: ApiKey) -> Self {
        Self {
            token: Arc::new(token),
        }
    }
}

impl A2aBearerTokenProvider for StaticA2aBearerToken {
    fn resolve(
        &self,
        _request: &A2aClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<ApiKey, A2aClientAuthorizationError>> {
        let token = self.token.as_ref().clone();
        Box::pin(async move { Ok(token) })
    }
}

impl fmt::Debug for StaticA2aBearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StaticA2aBearerToken([REDACTED])")
    }
}

/// Security selection for the immutable Agent Card binding.
#[derive(Clone)]
#[non_exhaustive]
pub enum A2aClientSecurity {
    /// Select an anonymous alternative from the Agent Card.
    Anonymous,
    /// Select one HTTP Bearer, `OAuth2`, or `OpenID` Connect requirement.
    Bearer {
        /// Exact Agent Card security-scheme name.
        scheme: Box<str>,
        /// Out-of-band token source.
        provider: Arc<dyn A2aBearerTokenProvider>,
    },
}

impl A2aClientSecurity {
    /// Constructs one named bearer security selection.
    pub fn bearer(
        scheme: impl Into<String>,
        provider: Arc<dyn A2aBearerTokenProvider>,
    ) -> Result<Self, A2aClientSecurityError> {
        let scheme = scheme.into();
        if scheme.is_empty()
            || scheme.len() > MAX_SECURITY_SCHEME_BYTES
            || scheme.trim() != scheme
            || scheme.chars().any(char::is_control)
        {
            return Err(A2aClientSecurityError::InvalidSchemeName);
        }
        Ok(Self::Bearer {
            scheme: scheme.into_boxed_str(),
            provider,
        })
    }

    pub(crate) fn requires_credentials(&self) -> bool {
        matches!(self, Self::Bearer { .. })
    }
}

impl fmt::Debug for A2aClientSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Anonymous => formatter.write_str("A2aClientSecurity::Anonymous"),
            Self::Bearer { scheme, .. } => formatter
                .debug_struct("A2aClientSecurity::Bearer")
                .field("scheme", scheme)
                .field("provider", &"[REDACTED]")
                .finish(),
        }
    }
}

/// Invalid or unsupported Agent Card security selection.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aClientSecurityError {
    /// The configured scheme name was not a bounded printable token.
    #[error("A2A security scheme name is invalid")]
    InvalidSchemeName,
    /// The card has no anonymous alternative.
    #[error("A2A Agent Card requires authentication")]
    AnonymousNotAllowed,
    /// The named scheme was absent or not bearer-compatible.
    #[error("A2A bearer scheme is unavailable or unsupported")]
    BearerSchemeUnsupported,
    /// No complete single-scheme alternative selected the named bearer scheme.
    #[error("A2A bearer scheme does not satisfy a complete security requirement")]
    RequirementNotSatisfied,
}

/// Failure classification for a strict A2A exchange.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum A2aClientErrorKind {
    /// Credential resolution failed.
    Authorization,
    /// Local request encoding or byte limits rejected the call.
    Request,
    /// Network I/O or a deadline failed after dispatch began.
    Transport,
    /// The HTTP response violated status or media-type rules.
    HttpProtocol,
    /// The remote endpoint returned a standard A2A error.
    Remote,
    /// Response JSON, JSON-RPC, SSE, or bounded contracts were invalid.
    InvalidResponse,
    /// The requested capability was not advertised.
    Capability,
}

/// Public-safe strict-client failure. Remote text and response bodies are not
/// retained because they may contain secrets or tenant data.
#[derive(Clone, Debug, Error)]
#[error("A2A client operation failed: {kind:?}")]
pub struct A2aClientError {
    kind: A2aClientErrorKind,
    dispatched: bool,
    remote_code: Option<i32>,
    http_status: Option<u16>,
    authorization_error: Option<A2aClientAuthorizationError>,
}

impl A2aClientError {
    const fn before(kind: A2aClientErrorKind) -> Self {
        Self {
            kind,
            dispatched: false,
            remote_code: None,
            http_status: None,
            authorization_error: None,
        }
    }

    const fn after(kind: A2aClientErrorKind) -> Self {
        Self {
            kind,
            dispatched: true,
            remote_code: None,
            http_status: None,
            authorization_error: None,
        }
    }

    const fn after_http(kind: A2aClientErrorKind, status: StatusCode) -> Self {
        Self {
            kind,
            dispatched: true,
            remote_code: None,
            http_status: Some(status.as_u16()),
            authorization_error: None,
        }
    }

    const fn remote(code: Option<i32>, status: Option<StatusCode>) -> Self {
        Self {
            kind: A2aClientErrorKind::Remote,
            dispatched: true,
            remote_code: code,
            http_status: match status {
                Some(status) => Some(status.as_u16()),
                None => None,
            },
            authorization_error: None,
        }
    }

    const fn authorization(error: A2aClientAuthorizationError) -> Self {
        Self {
            kind: A2aClientErrorKind::Authorization,
            dispatched: false,
            remote_code: None,
            http_status: None,
            authorization_error: Some(error),
        }
    }

    /// Returns the stable public classification.
    #[must_use]
    pub const fn kind(&self) -> A2aClientErrorKind {
        self.kind
    }

    /// Returns whether request dispatch had begun before the failure.
    #[must_use]
    pub const fn was_dispatched(&self) -> bool {
        self.dispatched
    }

    /// Returns an A2A/JSON-RPC error code without remote text.
    #[must_use]
    pub const fn remote_code(&self) -> Option<i32> {
        self.remote_code
    }

    /// Returns the HTTP status without response content.
    #[must_use]
    pub const fn http_status(&self) -> Option<u16> {
        self.http_status
    }

    /// Returns the credential-source outcome without credential contents.
    #[must_use]
    pub const fn authorization_error(&self) -> Option<A2aClientAuthorizationError> {
        self.authorization_error
    }
}

/// Failure while constructing and verifying an immutable client binding.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum A2aClientBuildError {
    /// At least one egress-approved interface must be pinned.
    #[error("A2A client requires at least one interface pin")]
    MissingInterfacePin,
    /// Too many interface pins were configured.
    #[error("A2A client interface pin limit exceeded")]
    TooManyInterfacePins,
    /// The same binding and URL was pinned more than once.
    #[error("A2A client contains a duplicate interface pin")]
    DuplicateInterfacePin,
    /// Extension configuration was invalid or unbounded.
    #[error("A2A client extension selection is invalid")]
    InvalidExtensions,
    /// The HTTP client could not be constructed.
    #[error("A2A HTTP client construction failed")]
    HttpClient,
    /// Agent Card discovery failed.
    #[error("A2A Agent Card discovery failed")]
    Discovery,
    /// The discovery response was not bounded valid A2A JSON.
    #[error("A2A Agent Card response is invalid")]
    InvalidAgentCard,
    /// TLS-only trust cannot authorize a plaintext loopback card.
    #[error("A2A TLS server-identity trust requires HTTPS discovery")]
    TlsTrustRequiresHttps,
    /// The exact canonical card digest did not match.
    #[error("A2A Agent Card digest does not match the configured trust anchor")]
    AgentCardDigestMismatch,
    /// No StateKnot-supported A2A 1.0 interface was advertised.
    #[error("A2A Agent Card has no supported 1.0 HTTP interface")]
    NoSupportedInterface,
    /// The preferred supported interface was not egress-approved exactly.
    #[error("A2A preferred interface does not match an egress pin")]
    InterfacePinMismatch,
    /// A required card extension was not selected or a selected extension was absent.
    #[error("A2A extension negotiation failed")]
    ExtensionMismatch,
    /// Agent Card security could not satisfy the configured selection.
    #[error(transparent)]
    Security(#[from] A2aClientSecurityError),
}

/// One decoded, bounded SSE stream.
pub type A2aClientEventStream =
    Pin<Box<dyn Stream<Item = Result<A2aStreamEvent, A2aClientError>> + Send + 'static>>;

#[derive(Clone)]
struct A2aSendExpectation {
    task_id: Option<Box<str>>,
    context_id: Option<Box<str>>,
    history_length: Option<u32>,
    return_immediately: bool,
}

impl A2aSendExpectation {
    fn from_request(request: &A2aSendMessageRequest) -> Self {
        Self {
            task_id: request.message().task_id().map(Into::into),
            context_id: request.message().context_id().map(Into::into),
            history_length: request
                .configuration()
                .and_then(crate::A2aSendConfiguration::history_length),
            return_immediately: request
                .configuration()
                .is_some_and(crate::A2aSendConfiguration::should_return_immediately),
        }
    }
}

struct A2aListTasksExpectation {
    context_id: Option<Box<str>>,
    status: Option<A2aTaskState>,
    status_timestamp_after: Option<chrono::DateTime<chrono::Utc>>,
    page_size: u16,
    history_length: Option<u32>,
    include_artifacts: bool,
}

impl A2aListTasksExpectation {
    fn from_request(request: &A2aListTasksRequest) -> Self {
        Self {
            context_id: request.context_id().map(Into::into),
            status: request.status(),
            status_timestamp_after: request.status_timestamp_after(),
            page_size: request.page_size(),
            history_length: request.history_length(),
            include_artifacts: request.should_include_artifacts(),
        }
    }
}

#[derive(Clone)]
struct A2aCallScope {
    task_id: Option<Box<str>>,
    attempt: Option<A2aClientAttemptIdentity>,
    required_scopes: Option<Arc<[Box<str>]>>,
    dispatch_observer: Option<Arc<AtomicBool>>,
}

impl A2aCallScope {
    fn new(task_id: Option<&str>) -> Self {
        Self {
            task_id: task_id.map(Into::into),
            attempt: None,
            required_scopes: None,
            dispatch_observer: None,
        }
    }

    fn with_attempt(mut self, attempt: A2aClientAttemptIdentity) -> Self {
        self.attempt = Some(attempt);
        self
    }

    fn with_required_scopes(mut self, required_scopes: Arc<[Box<str>]>) -> Self {
        self.required_scopes = Some(required_scopes);
        self
    }

    fn with_dispatch_observer(mut self, observer: Arc<AtomicBool>) -> Self {
        self.dispatch_observer = Some(observer);
        self
    }

    fn mark_dispatched(&self) {
        if let Some(observer) = &self.dispatch_observer {
            observer.store(true, Ordering::Release);
        }
    }
}

/// Immutable, verified A2A 1.0 client binding.
#[derive(Clone)]
pub struct A2aClient {
    card: A2aAgentCard,
    card_digest: Digest,
    interface: A2aAgentInterface,
    interface_url: Url,
    security: A2aClientSecurity,
    required_scopes: Arc<[Box<str>]>,
    extensions: Arc<[Box<str>]>,
    options: A2aClientOptions,
    http: reqwest::Client,
    next_request_id: Arc<AtomicU64>,
}

impl fmt::Debug for A2aClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("A2aClient")
            .field("agent", &self.card.name())
            .field("agent_version", &self.card.agent_version())
            .field("card_digest", &self.card_digest)
            .field("binding", &self.interface.binding())
            .field("interface_url", &"[REDACTED]")
            .field("security", &self.security)
            .field("extensions", &self.extensions.len())
            .finish_non_exhaustive()
    }
}

impl A2aClient {
    /// Discovers, validates, pins, and freezes one A2A 1.0 client binding.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    pub async fn discover(
        card_endpoint: A2aAgentCardEndpoint,
        interface_pins: Vec<A2aClientInterfacePin>,
        trust: A2aAgentCardTrust,
        security: A2aClientSecurity,
        extensions: Vec<String>,
        options: A2aClientOptions,
    ) -> Result<Self, A2aClientBuildError> {
        validate_interface_pins(&interface_pins)?;
        validate_extension_selection(&extensions)?;
        if matches!(trust, A2aAgentCardTrust::TlsServerIdentity)
            && card_endpoint.transport != A2aEndpointTransport::Https
        {
            return Err(A2aClientBuildError::TlsTrustRequiresHttps);
        }

        let http =
            build_client(options.transport()).map_err(|_| A2aClientBuildError::HttpClient)?;
        let deadline = tokio::time::Instant::now() + options.discovery_timeout;
        let response = wait_until(deadline, async {
            http.get(card_endpoint.url.clone())
                .header(header::ACCEPT, format!("{JSON}, {A2A_JSON}"))
                .send()
                .await
        })
        .await
        .map_err(|()| A2aClientBuildError::Discovery)?
        .map_err(|_| A2aClientBuildError::Discovery)?;
        if !response.status().is_success() || !is_json_content_type(response.headers()) {
            return Err(A2aClientBuildError::Discovery);
        }
        let bytes = wait_until(
            deadline,
            bounded_body(response, options.transport().maximum_response_bytes()),
        )
        .await
        .map_err(|()| A2aClientBuildError::Discovery)?
        .map_err(|error| match error {
            BoundedBodyError::Transport => A2aClientBuildError::Discovery,
            BoundedBodyError::TooLarge => A2aClientBuildError::InvalidAgentCard,
        })?;
        let value = BoundedJson::from_slice_with_limits(&bytes, JsonLimits::MAXIMUM)
            .map_err(|_| A2aClientBuildError::InvalidAgentCard)?
            .into_value();
        let card_digest =
            a2a_agent_card_digest(&value).map_err(|_| A2aClientBuildError::InvalidAgentCard)?;
        if let A2aAgentCardTrust::CanonicalSha256(expected) = trust {
            if card_digest != expected {
                return Err(A2aClientBuildError::AgentCardDigestMismatch);
            }
        }
        let card =
            A2aAgentCard::from_json(value).map_err(|_| A2aClientBuildError::InvalidAgentCard)?;
        let selected = card
            .wire()
            .supported_interfaces
            .iter()
            .find(|candidate| {
                candidate.protocol_version == A2A_PROTOCOL_VERSION_1_0
                    && A2aBinding::try_from(candidate.protocol_binding.as_str()).is_ok()
            })
            .cloned()
            .ok_or(A2aClientBuildError::NoSupportedInterface)?;
        let interface = A2aAgentInterface::try_from_wire(selected)
            .map_err(|_| A2aClientBuildError::NoSupportedInterface)?;
        let interface_url =
            Url::parse(interface.url()).map_err(|_| A2aClientBuildError::NoSupportedInterface)?;
        let pin = interface_pins
            .iter()
            .find(|pin| {
                pin.binding == interface.binding()
                    && pin.url == interface_url
                    && pin.tenant.as_deref() == interface.tenant()
            })
            .ok_or(A2aClientBuildError::InterfacePinMismatch)?;
        match pin.transport {
            A2aEndpointTransport::Https if interface_url.scheme() != "https" => {
                return Err(A2aClientBuildError::InterfacePinMismatch);
            }
            A2aEndpointTransport::LoopbackHttp if interface_url.scheme() != "http" => {
                return Err(A2aClientBuildError::InterfacePinMismatch);
            }
            A2aEndpointTransport::Https | A2aEndpointTransport::LoopbackHttp => {}
        }

        let capabilities = card.capabilities();
        let declared_extensions = capabilities.extensions();
        for extension in &extensions {
            if !declared_extensions
                .iter()
                .any(|candidate| candidate.uri() == extension)
            {
                return Err(A2aClientBuildError::ExtensionMismatch);
            }
        }
        if declared_extensions.iter().any(|required| {
            required.is_required() && !extensions.iter().any(|value| value == required.uri())
        }) {
            return Err(A2aClientBuildError::ExtensionMismatch);
        }

        let required_scopes = select_security_requirements(
            card.wire().security_requirements.as_deref(),
            card.wire().security_schemes.as_ref(),
            &security,
        )?;
        Ok(Self {
            card,
            card_digest,
            interface,
            interface_url,
            security,
            required_scopes,
            extensions: extensions
                .into_iter()
                .map(String::into_boxed_str)
                .collect::<Vec<_>>()
                .into(),
            options,
            http,
            next_request_id: Arc::new(AtomicU64::new(1)),
        })
    }

    /// Returns the exact immutable public Agent Card snapshot.
    #[must_use]
    pub const fn agent_card(&self) -> &A2aAgentCard {
        &self.card
    }

    /// Returns the RFC 8785 canonical SHA-256 of that snapshot.
    #[must_use]
    pub const fn agent_card_digest(&self) -> Digest {
        self.card_digest
    }

    /// Returns the selected interface in server preference order.
    #[must_use]
    pub const fn interface(&self) -> &A2aAgentInterface {
        &self.interface
    }

    /// Returns negotiated extension URIs in request-header order.
    pub fn negotiated_extensions(&self) -> impl ExactSizeIterator<Item = &str> {
        self.extensions.iter().map(AsRef::as_ref)
    }

    pub(crate) fn requires_credentials(&self) -> bool {
        self.security.requires_credentials()
    }

    /// Sends one message without an automatic retry.
    pub async fn send_message(
        &self,
        request: A2aSendMessageRequest,
    ) -> Result<A2aSendMessageResponse, A2aClientError> {
        self.send_message_scoped(request, A2aCallScope::new(None))
            .await
    }

    pub(crate) async fn send_message_with_attempt(
        &self,
        request: A2aSendMessageRequest,
        attempt: A2aClientAttemptIdentity,
        required_scopes: Arc<[Box<str>]>,
        dispatch_observer: Arc<AtomicBool>,
    ) -> Result<A2aSendMessageResponse, A2aClientError> {
        self.send_message_scoped(
            request,
            A2aCallScope::new(None)
                .with_attempt(attempt)
                .with_required_scopes(required_scopes)
                .with_dispatch_observer(dispatch_observer),
        )
        .await
    }

    async fn send_message_scoped(
        &self,
        request: A2aSendMessageRequest,
        mut scope: A2aCallScope,
    ) -> Result<A2aSendMessageResponse, A2aClientError> {
        if request
            .configuration()
            .and_then(|configuration| configuration.push_config())
            .is_some()
            && !self.card.capabilities().supports_push_notifications()
        {
            return Err(A2aClientError::before(A2aClientErrorKind::Capability));
        }
        let expectation = A2aSendExpectation::from_request(&request);
        scope.task_id.clone_from(&expectation.task_id);
        let wire = request
            .into_wire(self.tenant())
            .map_err(|_| A2aClientError::before(A2aClientErrorKind::Request))?;
        let params = encode_value(&wire)?;
        let rest = RestRequest::json(
            Method::POST,
            self.rest_url(&["message:send"])?,
            params.clone(),
        );
        let value = self
            .call_unary(
                A2aClientOperation::SendMessage,
                wire::methods::SEND_MESSAGE,
                params,
                rest,
                scope,
            )
            .await?;
        require_exact_variant(&value, &["task", "message"])?;
        let response = decode_value::<wire::SendMessageResponse>(value).and_then(|value| {
            A2aSendMessageResponse::try_from_wire(value)
                .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))
        })?;
        validate_send_response(&response, &expectation)?;
        Ok(response)
    }

    /// Sends one message and yields ordered, bounded SSE events.
    pub async fn send_streaming_message(
        &self,
        request: A2aSendMessageRequest,
    ) -> Result<A2aClientEventStream, A2aClientError> {
        if !self.card.capabilities().supports_streaming() {
            return Err(A2aClientError::before(A2aClientErrorKind::Capability));
        }
        if request
            .configuration()
            .and_then(|configuration| configuration.push_config())
            .is_some()
            && !self.card.capabilities().supports_push_notifications()
        {
            return Err(A2aClientError::before(A2aClientErrorKind::Capability));
        }
        let expectation = A2aSendExpectation::from_request(&request);
        let scope = A2aCallScope::new(expectation.task_id.as_deref());
        let wire = request
            .into_wire(self.tenant())
            .map_err(|_| A2aClientError::before(A2aClientErrorKind::Request))?;
        let params = encode_value(&wire)?;
        let rest = RestRequest::json(
            Method::POST,
            self.rest_url(&["message:stream"])?,
            params.clone(),
        );
        self.call_stream(
            A2aClientOperation::SendStreamingMessage,
            wire::methods::SEND_STREAMING_MESSAGE,
            params,
            rest,
            scope,
            A2aStreamContract::SendMessage(expectation),
        )
        .await
    }

    /// Gets one task snapshot.
    pub async fn get_task(&self, request: A2aGetTaskRequest) -> Result<A2aTask, A2aClientError> {
        self.get_task_scoped(request, A2aCallScope::new(None)).await
    }

    pub(crate) async fn get_task_with_attempt(
        &self,
        request: A2aGetTaskRequest,
        attempt: A2aClientAttemptIdentity,
        required_scopes: Arc<[Box<str>]>,
    ) -> Result<A2aTask, A2aClientError> {
        self.get_task_scoped(
            request,
            A2aCallScope::new(None)
                .with_attempt(attempt)
                .with_required_scopes(required_scopes),
        )
        .await
    }

    async fn get_task_scoped(
        &self,
        request: A2aGetTaskRequest,
        mut scope: A2aCallScope,
    ) -> Result<A2aTask, A2aClientError> {
        let task_id = request.id().to_owned();
        let history_length = request.history_length();
        scope.task_id = Some(task_id.clone().into_boxed_str());
        let wire = request
            .into_wire(self.tenant())
            .map_err(|_| A2aClientError::before(A2aClientErrorKind::Request))?;
        let mut url = self.rest_url(&["tasks", &task_id])?;
        if let Some(length) = wire.history_length {
            url.query_pairs_mut()
                .append_pair("historyLength", &length.to_string());
        }
        let params = encode_value(&wire)?;
        let value = self
            .call_unary(
                A2aClientOperation::GetTask,
                wire::methods::GET_TASK,
                params,
                RestRequest::empty(Method::GET, url),
                scope,
            )
            .await?;
        let task = decode_value::<wire::Task>(value).and_then(|value| {
            A2aTask::try_from_wire(value)
                .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))
        })?;
        validate_task_response(&task, Some(&task_id), None, history_length)?;
        Ok(task)
    }

    /// Lists a bounded stable-snapshot task page.
    pub async fn list_tasks(
        &self,
        request: A2aListTasksRequest,
    ) -> Result<A2aTaskPage, A2aClientError> {
        self.list_tasks_scoped(request, A2aCallScope::new(None))
            .await
    }

    pub(crate) async fn list_tasks_with_attempt(
        &self,
        request: A2aListTasksRequest,
        attempt: A2aClientAttemptIdentity,
        required_scopes: Arc<[Box<str>]>,
    ) -> Result<A2aTaskPage, A2aClientError> {
        self.list_tasks_scoped(
            request,
            A2aCallScope::new(None)
                .with_attempt(attempt)
                .with_required_scopes(required_scopes),
        )
        .await
    }

    async fn list_tasks_scoped(
        &self,
        request: A2aListTasksRequest,
        scope: A2aCallScope,
    ) -> Result<A2aTaskPage, A2aClientError> {
        let expectation = A2aListTasksExpectation::from_request(&request);
        let wire = request.into_wire(self.tenant());
        let mut url = self.rest_url(&["tasks"])?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(value) = wire.context_id.as_deref() {
                query.append_pair("contextId", value);
            }
            if let Some(value) = wire.status.as_ref() {
                let value = serde_json::to_value(value)
                    .ok()
                    .and_then(|value| value.as_str().map(ToOwned::to_owned))
                    .ok_or_else(|| A2aClientError::before(A2aClientErrorKind::Request))?;
                query.append_pair("status", &value);
            }
            if let Some(value) = wire.page_size {
                query.append_pair("pageSize", &value.to_string());
            }
            if let Some(value) = wire.page_token.as_deref() {
                query.append_pair("pageToken", value);
            }
            if let Some(value) = wire.history_length {
                query.append_pair("historyLength", &value.to_string());
            }
            if let Some(value) = wire.status_timestamp_after {
                query.append_pair(
                    "statusTimestampAfter",
                    &value.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
                );
            }
            if let Some(value) = wire.include_artifacts {
                query.append_pair("includeArtifacts", if value { "true" } else { "false" });
            }
        }
        let params = encode_value(&wire)?;
        let value = self
            .call_unary(
                A2aClientOperation::ListTasks,
                wire::methods::LIST_TASKS,
                params,
                RestRequest::empty(Method::GET, url),
                scope,
            )
            .await?;
        let page = decode_value::<wire::ListTasksResponse>(value).and_then(|value| {
            A2aTaskPage::try_from_wire(value)
                .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))
        })?;
        validate_task_page(&page, &expectation)?;
        Ok(page)
    }

    /// Requests task cancellation. The client itself never retries this call.
    pub async fn cancel_task(
        &self,
        request: A2aCancelTaskRequest,
    ) -> Result<A2aTask, A2aClientError> {
        let task_id = request.id().to_owned();
        let wire = request.into_wire(self.tenant());
        let params = encode_value(&wire)?;
        let action = format!("{task_id}:cancel");
        let value = self
            .call_unary(
                A2aClientOperation::CancelTask,
                wire::methods::CANCEL_TASK,
                params.clone(),
                RestRequest::json(Method::POST, self.rest_url(&["tasks", &action])?, params),
                A2aCallScope::new(Some(&task_id)),
            )
            .await?;
        let task = decode_value::<wire::Task>(value).and_then(|value| {
            A2aTask::try_from_wire(value)
                .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))
        })?;
        validate_task_response(&task, Some(&task_id), None, None)?;
        Ok(task)
    }

    /// Subscribes to future task events over one bounded SSE exchange.
    pub async fn subscribe_to_task(
        &self,
        request: A2aSubscribeTaskRequest,
    ) -> Result<A2aClientEventStream, A2aClientError> {
        if !self.card.capabilities().supports_streaming() {
            return Err(A2aClientError::before(A2aClientErrorKind::Capability));
        }
        let task_id = request.id().to_owned();
        let wire = request.into_wire(self.tenant());
        let params = encode_value(&wire)?;
        let action = format!("{task_id}:subscribe");
        self.call_stream(
            A2aClientOperation::SubscribeToTask,
            wire::methods::SUBSCRIBE_TO_TASK,
            params,
            RestRequest::empty(Method::POST, self.rest_url(&["tasks", &action])?),
            A2aCallScope::new(Some(&task_id)),
            A2aStreamContract::SubscribeToTask(task_id.into_boxed_str()),
        )
        .await
    }

    /// Creates a task push-notification configuration.
    pub async fn create_push_config(
        &self,
        config: A2aPushConfig,
    ) -> Result<A2aPushConfig, A2aClientError> {
        self.require_push_capability()?;
        let task_id = config
            .task_id()
            .ok_or_else(|| A2aClientError::before(A2aClientErrorKind::Request))?
            .to_owned();
        let wire = config.into_wire_for_tenant(self.tenant());
        let params = encode_value(&wire)?;
        let value = self
            .call_unary(
                A2aClientOperation::CreatePushConfig,
                wire::methods::CREATE_PUSH_CONFIG,
                params.clone(),
                RestRequest::json(
                    Method::POST,
                    self.rest_url(&["tasks", &task_id, "pushNotificationConfigs"])?,
                    params,
                ),
                A2aCallScope::new(Some(&task_id)),
            )
            .await?;
        let value = decode_value::<wire::TaskPushNotificationConfig>(value)?;
        let config = decode_push_config(value, self.interface.tenant())?;
        validate_push_config_identity(&config, &task_id, None, true)?;
        Ok(config)
    }

    /// Gets one task push-notification configuration.
    pub async fn get_push_config(
        &self,
        request: A2aGetPushConfigRequest,
    ) -> Result<A2aPushConfig, A2aClientError> {
        self.require_push_capability()?;
        let task_id = request.task_id().to_owned();
        let config_id = request.config_id().to_owned();
        let wire = request.into_get_wire(self.tenant());
        let params = encode_value(&wire)?;
        let value = self
            .call_unary(
                A2aClientOperation::GetPushConfig,
                wire::methods::GET_PUSH_CONFIG,
                params,
                RestRequest::empty(
                    Method::GET,
                    self.rest_url(&["tasks", &task_id, "pushNotificationConfigs", &config_id])?,
                ),
                A2aCallScope::new(Some(&task_id)),
            )
            .await?;
        let value = decode_value::<wire::TaskPushNotificationConfig>(value)?;
        let config = decode_push_config(value, self.interface.tenant())?;
        validate_push_config_identity(&config, &task_id, Some(&config_id), true)?;
        Ok(config)
    }

    /// Lists one bounded page of task push-notification configurations.
    pub async fn list_push_configs(
        &self,
        request: A2aListPushConfigsRequest,
    ) -> Result<A2aPushConfigPage, A2aClientError> {
        self.require_push_capability()?;
        let task_id = request.task_id().to_owned();
        let page_size = request.page_size();
        let wire = request.into_wire(self.tenant());
        let mut url = self.rest_url(&["tasks", &task_id, "pushNotificationConfigs"])?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(value) = wire.page_size {
                query.append_pair("pageSize", &value.to_string());
            }
            if let Some(value) = wire.page_token.as_deref() {
                query.append_pair("pageToken", value);
            }
        }
        let params = encode_value(&wire)?;
        let value = self
            .call_unary(
                A2aClientOperation::ListPushConfigs,
                wire::methods::LIST_PUSH_CONFIGS,
                params,
                RestRequest::empty(Method::GET, url),
                A2aCallScope::new(Some(&task_id)),
            )
            .await?;
        let mut value = decode_value::<wire::ListTaskPushNotificationConfigsResponse>(value)?;
        for config in &mut value.configs {
            strip_response_tenant(config, self.interface.tenant())?;
        }
        let page = A2aPushConfigPage::try_from_wire(value)
            .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))?;
        validate_push_config_page(&page, &task_id, page_size)?;
        Ok(page)
    }

    /// Deletes one task push-notification configuration.
    pub async fn delete_push_config(
        &self,
        request: A2aDeletePushConfigRequest,
    ) -> Result<(), A2aClientError> {
        self.require_push_capability()?;
        let task_id = request.task_id().to_owned();
        let config_id = request.config_id().to_owned();
        let wire = request.into_delete_wire(self.tenant());
        let params = encode_value(&wire)?;
        let value = self
            .call_unary(
                A2aClientOperation::DeletePushConfig,
                wire::methods::DELETE_PUSH_CONFIG,
                params,
                RestRequest::empty_response(
                    Method::DELETE,
                    self.rest_url(&["tasks", &task_id, "pushNotificationConfigs", &config_id])?,
                ),
                A2aCallScope::new(Some(&task_id)),
            )
            .await?;
        if value.is_null() {
            Ok(())
        } else {
            Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse))
        }
    }

    /// Fetches and validates the authenticated extended Agent Card.
    pub async fn get_extended_agent_card(&self) -> Result<A2aAgentCard, A2aClientError> {
        if !self.card.capabilities().supports_extended_agent_card() {
            return Err(A2aClientError::before(A2aClientErrorKind::Capability));
        }
        if !self.requires_credentials() {
            return Err(A2aClientError::before(A2aClientErrorKind::Authorization));
        }
        let wire = wire::GetExtendedAgentCardRequest {
            tenant: self.tenant(),
        };
        let params = encode_value(&wire)?;
        let value = self
            .call_unary(
                A2aClientOperation::GetExtendedAgentCard,
                wire::methods::GET_EXTENDED_AGENT_CARD,
                params,
                RestRequest::empty(Method::GET, self.rest_url(&["extendedAgentCard"])?),
                A2aCallScope::new(None),
            )
            .await?;
        decode_value::<wire::AgentCard>(value).and_then(|value| {
            A2aAgentCard::try_from_wire(value)
                .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))
        })
    }

    pub(crate) fn skill_required_scopes(
        &self,
        skill_id: &str,
    ) -> Option<Result<Arc<[Box<str>]>, A2aClientSecurityError>> {
        let skill = self
            .card
            .skills()
            .into_iter()
            .find(|skill| skill.id() == skill_id)?;
        Some(match skill.security_requirements() {
            Some(requirements) => select_security_requirements(
                Some(requirements),
                self.card.wire().security_schemes.as_ref(),
                &self.security,
            ),
            None => Ok(self.required_scopes.clone()),
        })
    }

    fn tenant(&self) -> Option<String> {
        self.interface.tenant().map(ToOwned::to_owned)
    }

    fn require_push_capability(&self) -> Result<(), A2aClientError> {
        if self.card.capabilities().supports_push_notifications() {
            Ok(())
        } else {
            Err(A2aClientError::before(A2aClientErrorKind::Capability))
        }
    }

    fn rest_url(&self, suffix: &[&str]) -> Result<Url, A2aClientError> {
        let mut url = self.interface_url.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| A2aClientError::before(A2aClientErrorKind::Request))?;
            segments.pop_if_empty();
            if let Some(tenant) = self.interface.tenant() {
                segments.push(tenant);
            }
            for segment in suffix {
                segments.push(segment);
            }
        }
        Ok(url)
    }
}

impl A2aClient {
    async fn call_unary(
        &self,
        operation: A2aClientOperation,
        rpc_method: &'static str,
        params: Value,
        rest: RestRequest,
        scope: A2aCallScope,
    ) -> Result<Value, A2aClientError> {
        let deadline = tokio::time::Instant::now() + self.options.request_timeout;
        let (method, url, body, response_kind, allow_empty) = match self.interface.binding() {
            A2aBinding::HttpJson => (
                rest.method,
                rest.url,
                rest.body
                    .map(|value| serialize_request(&value, self.options.transport())),
                UnaryResponseKind::HttpJson,
                rest.allow_empty,
            ),
            A2aBinding::JsonRpc => {
                let id = allocate_request_id(&self.next_request_id)
                    .ok_or_else(|| A2aClientError::before(A2aClientErrorKind::Request))?;
                let envelope = wire::JsonRpcRequest::new(
                    wire::JsonRpcId::Number(id),
                    rpc_method,
                    Some(params),
                );
                (
                    Method::POST,
                    self.interface_url.clone(),
                    Some(serialize_request(&envelope, self.options.transport())),
                    UnaryResponseKind::JsonRpc(id),
                    rest.allow_empty,
                )
            }
        };
        let body = body.transpose()?;
        let authorization = self
            .resolve_authorization(deadline, operation, &scope)
            .await?;
        let request = self.request_builder(method, url, false, body, authorization.as_ref())?;
        scope.mark_dispatched();
        let response = wait_until(deadline, request.send())
            .await
            .map_err(|()| A2aClientError::after(A2aClientErrorKind::Transport))?
            .map_err(|_| A2aClientError::after(A2aClientErrorKind::Transport))?;
        self.consume_unary(deadline, response, response_kind, allow_empty)
            .await
    }

    async fn call_stream(
        &self,
        operation: A2aClientOperation,
        rpc_method: &'static str,
        params: Value,
        rest: RestRequest,
        scope: A2aCallScope,
        contract: A2aStreamContract,
    ) -> Result<A2aClientEventStream, A2aClientError> {
        let deadline = tokio::time::Instant::now() + self.options.request_timeout;
        let (method, url, body, response_kind) = match self.interface.binding() {
            A2aBinding::HttpJson => (
                rest.method,
                rest.url,
                rest.body
                    .map(|value| serialize_request(&value, self.options.transport())),
                StreamResponseKind::HttpJson,
            ),
            A2aBinding::JsonRpc => {
                let id = allocate_request_id(&self.next_request_id)
                    .ok_or_else(|| A2aClientError::before(A2aClientErrorKind::Request))?;
                let envelope = wire::JsonRpcRequest::new(
                    wire::JsonRpcId::Number(id),
                    rpc_method,
                    Some(params),
                );
                (
                    Method::POST,
                    self.interface_url.clone(),
                    Some(serialize_request(&envelope, self.options.transport())),
                    StreamResponseKind::JsonRpc(id),
                )
            }
        };
        let body = body.transpose()?;
        let authorization = self
            .resolve_authorization(deadline, operation, &scope)
            .await?;
        let request = self.request_builder(method, url, true, body, authorization.as_ref())?;
        scope.mark_dispatched();
        let response = wait_until(deadline, request.send())
            .await
            .map_err(|()| A2aClientError::after(A2aClientErrorKind::Transport))?
            .map_err(|_| A2aClientError::after(A2aClientErrorKind::Transport))?;
        let status = response.status();
        if !status.is_success() {
            return Err(self
                .consume_failed_response(deadline, response, response_kind.into())
                .await);
        }
        if !is_event_stream_content_type(response.headers()) {
            if matches!(response_kind, StreamResponseKind::JsonRpc(_))
                && is_json_content_type(response.headers())
            {
                let bytes = self.read_response(deadline, response).await?;
                let value = BoundedJson::from_slice_with_limits(&bytes, JsonLimits::MAXIMUM)
                    .map_err(|_| {
                        A2aClientError::after_http(A2aClientErrorKind::InvalidResponse, status)
                    })?
                    .into_value();
                return match response_kind {
                    StreamResponseKind::JsonRpc(id) => {
                        match parse_rpc_response(&value, id, Some(status)) {
                            Err(error) => Err(error),
                            Ok(_) => Err(A2aClientError::after_http(
                                A2aClientErrorKind::HttpProtocol,
                                status,
                            )),
                        }
                    }
                    StreamResponseKind::HttpJson => unreachable!(),
                };
            }
            return Err(A2aClientError::after_http(
                A2aClientErrorKind::HttpProtocol,
                status,
            ));
        }
        let state = A2aStreamState {
            body: Box::pin(response.bytes_stream()),
            decoder: Some(SseDecoder::new(self.options.transport())),
            pending: VecDeque::new(),
            kind: response_kind,
            idle_timeout: self.options.stream_idle_timeout,
            maximum_events: self.options.maximum_stream_events,
            events_seen: 0,
            terminated: false,
            validator: A2aStreamValidator::new(contract),
        };
        Ok(Box::pin(stream::unfold(state, next_a2a_stream_item)))
    }

    async fn resolve_authorization(
        &self,
        deadline: tokio::time::Instant,
        operation: A2aClientOperation,
        scope: &A2aCallScope,
    ) -> Result<Option<ApiKey>, A2aClientError> {
        let A2aClientSecurity::Bearer { provider, .. } = &self.security else {
            return Ok(None);
        };
        let request = A2aClientAuthorizationRequest {
            operation,
            card_digest: self.card_digest,
            binding: self.interface.binding(),
            remote_tenant: self.interface.tenant().map(Into::into),
            task_id: scope.task_id.clone(),
            required_scopes: scope
                .required_scopes
                .clone()
                .unwrap_or_else(|| self.required_scopes.clone()),
            attempt: scope.attempt.clone(),
        };
        wait_until(deadline, provider.resolve(&request))
            .await
            .map_err(|()| A2aClientError::authorization(A2aClientAuthorizationError::Unavailable))?
            .map(Some)
            .map_err(A2aClientError::authorization)
    }

    fn request_builder(
        &self,
        method: Method,
        url: Url,
        streaming: bool,
        body: Option<Vec<u8>>,
        authorization: Option<&ApiKey>,
    ) -> Result<reqwest::RequestBuilder, A2aClientError> {
        let carries_json = body.is_some() || method == Method::POST;
        let mut request = self
            .http
            .request(method, url)
            .header(A2A_VERSION_HEADER, A2A_PROTOCOL_VERSION_1_0)
            .header(
                header::ACCEPT,
                if streaming {
                    EVENT_STREAM
                } else {
                    A2A_JSON_ACCEPT
                },
            );
        if !self.extensions.is_empty() {
            let extensions = self
                .extensions
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>()
                .join(",");
            let value = HeaderValue::from_str(&extensions)
                .map_err(|_| A2aClientError::before(A2aClientErrorKind::Request))?;
            request = request.header(A2A_EXTENSIONS_HEADER, value);
        }
        if let Some(key) = authorization {
            request = request.bearer_auth(key.expose_secret());
        }
        if carries_json {
            request = request.header(
                header::CONTENT_TYPE,
                match self.interface.binding() {
                    A2aBinding::HttpJson => A2A_JSON,
                    A2aBinding::JsonRpc => JSON,
                },
            );
        }
        if let Some(body) = body {
            request = request.body(body);
        }
        Ok(request)
    }

    async fn consume_unary(
        &self,
        deadline: tokio::time::Instant,
        response: reqwest::Response,
        kind: UnaryResponseKind,
        allow_empty: bool,
    ) -> Result<Value, A2aClientError> {
        let status = response.status();
        if status == StatusCode::NO_CONTENT
            && allow_empty
            && matches!(kind, UnaryResponseKind::HttpJson)
        {
            return Ok(Value::Null);
        }
        if !is_json_content_type(response.headers()) {
            if status.is_success() && allow_empty && matches!(kind, UnaryResponseKind::HttpJson) {
                let bytes = self.read_response(deadline, response).await?;
                return if bytes.is_empty() {
                    Ok(Value::Null)
                } else {
                    Err(A2aClientError::after_http(
                        A2aClientErrorKind::HttpProtocol,
                        status,
                    ))
                };
            }
            return Err(A2aClientError::after_http(
                A2aClientErrorKind::HttpProtocol,
                status,
            ));
        }
        let bytes = self.read_response(deadline, response).await?;
        let value = BoundedJson::from_slice_with_limits(&bytes, JsonLimits::MAXIMUM)
            .map_err(|_| A2aClientError::after_http(A2aClientErrorKind::InvalidResponse, status))?
            .into_value();
        match kind {
            UnaryResponseKind::HttpJson if status.is_success() => {
                if allow_empty
                    && (value.is_null() || value.as_object().is_some_and(serde_json::Map::is_empty))
                {
                    Ok(Value::Null)
                } else {
                    Ok(value)
                }
            }
            UnaryResponseKind::HttpJson => match parse_rest_error(&value, Some(status)) {
                Some(error) => Err(A2aClientError::remote(error.code, Some(status))),
                None => Err(A2aClientError::after_http(
                    A2aClientErrorKind::HttpProtocol,
                    status,
                )),
            },
            UnaryResponseKind::JsonRpc(id) => {
                let result = parse_rpc_response(&value, id, Some(status))?;
                if !status.is_success() {
                    return Err(A2aClientError::after_http(
                        A2aClientErrorKind::HttpProtocol,
                        status,
                    ));
                }
                if allow_empty
                    && (result.is_null()
                        || result.as_object().is_some_and(serde_json::Map::is_empty))
                {
                    Ok(Value::Null)
                } else {
                    Ok(result)
                }
            }
        }
    }

    async fn consume_failed_response(
        &self,
        deadline: tokio::time::Instant,
        response: reqwest::Response,
        kind: UnaryResponseKind,
    ) -> A2aClientError {
        let status = response.status();
        if !is_json_content_type(response.headers()) {
            return A2aClientError::after_http(A2aClientErrorKind::HttpProtocol, status);
        }
        let bytes = match self.read_response(deadline, response).await {
            Ok(bytes) => bytes,
            Err(error) => return error,
        };
        let Ok(value) = BoundedJson::from_slice_with_limits(&bytes, JsonLimits::MAXIMUM) else {
            return A2aClientError::after_http(A2aClientErrorKind::InvalidResponse, status);
        };
        let value = value.into_value();
        match kind {
            UnaryResponseKind::HttpJson => parse_rest_error(&value, Some(status)).map_or_else(
                || A2aClientError::after_http(A2aClientErrorKind::HttpProtocol, status),
                |error| A2aClientError::remote(error.code, Some(status)),
            ),
            UnaryResponseKind::JsonRpc(id) => match parse_rpc_response(&value, id, Some(status)) {
                Err(error) if error.kind() == A2aClientErrorKind::Remote => error,
                Ok(_) | Err(_) => {
                    A2aClientError::after_http(A2aClientErrorKind::HttpProtocol, status)
                }
            },
        }
    }

    async fn read_response(
        &self,
        deadline: tokio::time::Instant,
        response: reqwest::Response,
    ) -> Result<Vec<u8>, A2aClientError> {
        wait_until(
            deadline,
            bounded_body(response, self.options.transport().maximum_response_bytes()),
        )
        .await
        .map_err(|()| A2aClientError::after(A2aClientErrorKind::Transport))?
        .map_err(|error| match error {
            BoundedBodyError::Transport => A2aClientError::after(A2aClientErrorKind::Transport),
            BoundedBodyError::TooLarge => {
                A2aClientError::after(A2aClientErrorKind::InvalidResponse)
            }
        })
    }
}

struct RestRequest {
    method: Method,
    url: Url,
    body: Option<Value>,
    allow_empty: bool,
}

impl RestRequest {
    fn empty(method: Method, url: Url) -> Self {
        Self {
            method,
            url,
            body: None,
            allow_empty: false,
        }
    }

    fn empty_response(method: Method, url: Url) -> Self {
        Self {
            method,
            url,
            body: None,
            allow_empty: true,
        }
    }

    fn json(method: Method, url: Url, body: Value) -> Self {
        Self {
            method,
            url,
            body: Some(body),
            allow_empty: false,
        }
    }
}

#[derive(Clone, Copy)]
enum UnaryResponseKind {
    HttpJson,
    JsonRpc(i64),
}

#[derive(Clone, Copy)]
enum StreamResponseKind {
    HttpJson,
    JsonRpc(i64),
}

impl From<StreamResponseKind> for UnaryResponseKind {
    fn from(value: StreamResponseKind) -> Self {
        match value {
            StreamResponseKind::HttpJson => Self::HttpJson,
            StreamResponseKind::JsonRpc(id) => Self::JsonRpc(id),
        }
    }
}

enum A2aStreamContract {
    SendMessage(A2aSendExpectation),
    SubscribeToTask(Box<str>),
}

#[derive(Clone)]
struct A2aStreamArtifacts {
    ids: HashSet<Box<str>>,
    sealed_ids: HashSet<Box<str>>,
}

impl A2aStreamArtifacts {
    fn from_task(task: &A2aTask) -> Self {
        Self {
            ids: task.artifact_ids().map(Into::into).collect(),
            sealed_ids: HashSet::new(),
        }
    }
}

enum A2aStreamProgress {
    AwaitingFirst,
    MessageComplete,
    Task {
        task_id: Box<str>,
        context_id: Box<str>,
        completion_seen: bool,
        artifacts: A2aStreamArtifacts,
    },
}

struct A2aStreamValidator {
    contract: A2aStreamContract,
    progress: A2aStreamProgress,
}

impl A2aStreamValidator {
    const fn new(contract: A2aStreamContract) -> Self {
        Self {
            contract,
            progress: A2aStreamProgress::AwaitingFirst,
        }
    }

    fn observe(&mut self, event: &A2aStreamEvent) -> Result<(), A2aClientError> {
        let progress = match (&self.progress, event) {
            (A2aStreamProgress::AwaitingFirst, A2aStreamEvent::Message(message)) => {
                let A2aStreamContract::SendMessage(expectation) = &self.contract else {
                    return Err(invalid_response());
                };
                validate_agent_message(message, expectation)?;
                A2aStreamProgress::MessageComplete
            }
            (A2aStreamProgress::AwaitingFirst, A2aStreamEvent::Task(task)) => {
                let (task_id, context_id, history_length) = match &self.contract {
                    A2aStreamContract::SendMessage(expectation) => (
                        expectation.task_id.as_deref(),
                        expectation.context_id.as_deref(),
                        expectation.history_length,
                    ),
                    A2aStreamContract::SubscribeToTask(task_id) => {
                        (Some(task_id.as_ref()), None, None)
                    }
                };
                validate_task_response(task, task_id, context_id, history_length)?;
                A2aStreamProgress::Task {
                    task_id: task.id().into(),
                    context_id: task.context_id().into(),
                    completion_seen: stream_state_closes(task.state()),
                    artifacts: A2aStreamArtifacts::from_task(task),
                }
            }
            (
                A2aStreamProgress::Task {
                    task_id,
                    context_id,
                    completion_seen: false,
                    artifacts,
                },
                A2aStreamEvent::StatusUpdate(update),
            ) => {
                validate_status_update(update, task_id, context_id)?;
                A2aStreamProgress::Task {
                    task_id: task_id.clone(),
                    context_id: context_id.clone(),
                    completion_seen: stream_state_closes(update.status().state()),
                    artifacts: artifacts.clone(),
                }
            }
            (
                A2aStreamProgress::Task {
                    task_id,
                    context_id,
                    completion_seen: false,
                    artifacts,
                },
                A2aStreamEvent::ArtifactUpdate(update),
            ) => {
                let artifacts = validate_artifact_update(update, task_id, context_id, artifacts)?;
                A2aStreamProgress::Task {
                    task_id: task_id.clone(),
                    context_id: context_id.clone(),
                    completion_seen: false,
                    artifacts,
                }
            }
            _ => return Err(invalid_response()),
        };
        self.progress = progress;
        Ok(())
    }

    const fn can_end(&self) -> bool {
        matches!(
            self.progress,
            A2aStreamProgress::MessageComplete
                | A2aStreamProgress::Task {
                    completion_seen: true,
                    ..
                }
        )
    }
}

const fn stream_state_closes(state: A2aTaskState) -> bool {
    state.is_terminal()
        || matches!(
            state,
            A2aTaskState::InputRequired | A2aTaskState::AuthRequired
        )
}

struct A2aStreamState {
    body: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send + 'static>>,
    decoder: Option<SseDecoder>,
    pending: VecDeque<SseEvent>,
    kind: StreamResponseKind,
    idle_timeout: Duration,
    maximum_events: u32,
    events_seen: u32,
    terminated: bool,
    validator: A2aStreamValidator,
}

async fn next_a2a_stream_item(
    mut state: A2aStreamState,
) -> Option<(Result<A2aStreamEvent, A2aClientError>, A2aStreamState)> {
    loop {
        if state.terminated {
            return None;
        }
        if let Some(event) = state.pending.pop_front() {
            state.events_seen = match state.events_seen.checked_add(1) {
                Some(value) if value <= state.maximum_events => value,
                Some(_) | None => {
                    state.terminated = true;
                    return Some((
                        Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse)),
                        state,
                    ));
                }
            };
            let parsed = parse_sse_event(&event, state.kind).and_then(|event| {
                state.validator.observe(&event)?;
                Ok(event)
            });
            if parsed.is_err() {
                state.terminated = true;
            }
            return Some((parsed, state));
        }

        if state.decoder.is_none() {
            state.terminated = true;
            return if state.validator.can_end() {
                None
            } else {
                Some((Err(invalid_response()), state))
            };
        }
        match tokio::time::timeout(state.idle_timeout, state.body.next()).await {
            Err(_) | Ok(Some(Err(_))) => {
                state.terminated = true;
                return Some((
                    Err(A2aClientError::after(A2aClientErrorKind::Transport)),
                    state,
                ));
            }
            Ok(Some(Ok(chunk))) => {
                if let Ok(events) = state
                    .decoder
                    .as_mut()
                    .expect("decoder presence was checked")
                    .push(&chunk)
                {
                    state.pending.extend(events);
                } else {
                    state.terminated = true;
                    return Some((
                        Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse)),
                        state,
                    ));
                }
            }
            Ok(None) => {
                let decoder = state.decoder.take().expect("decoder was present");
                if let Ok(events) = decoder.finish() {
                    state.pending.extend(events);
                } else {
                    state.terminated = true;
                    return Some((
                        Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse)),
                        state,
                    ));
                }
            }
        }
    }
}

fn parse_sse_event(
    event: &SseEvent,
    kind: StreamResponseKind,
) -> Result<A2aStreamEvent, A2aClientError> {
    if event
        .event
        .as_deref()
        .is_some_and(|event| !matches!(event, "message" | "error"))
    {
        return Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse));
    }
    let value = BoundedJson::from_str_with_limits(&event.data, JsonLimits::MAXIMUM)
        .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))?
        .into_value();
    match kind {
        StreamResponseKind::JsonRpc(id) => {
            let value = match parse_rpc_response(&value, id, None) {
                Ok(_) if event.event.as_deref() == Some("error") => {
                    return Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse));
                }
                Ok(value) => value,
                Err(error) => return Err(error),
            };
            require_exact_variant(
                &value,
                &["task", "message", "statusUpdate", "artifactUpdate"],
            )?;
            let wire = decode_value::<wire::StreamResponse>(value)?;
            A2aStreamEvent::try_from_wire(wire)
                .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))
        }
        StreamResponseKind::HttpJson => {
            if event.event.as_deref() == Some("error") {
                if let Some(error) =
                    parse_rest_error(&value, None).or_else(|| parse_bare_rpc_error(&value))
                {
                    return Err(A2aClientError::remote(error.code, None));
                }
                return Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse));
            }
            require_exact_variant(
                &value,
                &["task", "message", "statusUpdate", "artifactUpdate"],
            )?;
            match serde_json::from_value::<wire::StreamResponse>(value.clone()) {
                Ok(wire) => A2aStreamEvent::try_from_wire(wire)
                    .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse)),
                Err(_) => {
                    if let Some(error) =
                        parse_rest_error(&value, None).or_else(|| parse_bare_rpc_error(&value))
                    {
                        Err(A2aClientError::remote(error.code, None))
                    } else {
                        Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse))
                    }
                }
            }
        }
    }
}

fn validate_interface_pins(
    interface_pins: &[A2aClientInterfacePin],
) -> Result<(), A2aClientBuildError> {
    const MAX_INTERFACE_PINS: usize = 16;
    if interface_pins.is_empty() {
        return Err(A2aClientBuildError::MissingInterfacePin);
    }
    if interface_pins.len() > MAX_INTERFACE_PINS {
        return Err(A2aClientBuildError::TooManyInterfacePins);
    }
    let mut unique = HashSet::with_capacity(interface_pins.len());
    for pin in interface_pins {
        if !unique.insert((pin.binding, pin.url.as_str(), pin.tenant.as_deref())) {
            return Err(A2aClientBuildError::DuplicateInterfacePin);
        }
    }
    Ok(())
}

fn validate_extension_selection(extensions: &[String]) -> Result<(), A2aClientBuildError> {
    if extensions.len() > MAX_EXTENSION_COUNT {
        return Err(A2aClientBuildError::InvalidExtensions);
    }
    let mut unique = HashSet::with_capacity(extensions.len());
    let mut total = extensions.len().saturating_sub(1);
    for extension in extensions {
        if extension.is_empty()
            || extension.trim() != extension
            || extension.contains(',')
            || extension.chars().any(char::is_control)
            || extension.len() > 4096
            || Url::parse(extension)
                .ok()
                .and_then(|url| url.host_str().map(ToOwned::to_owned))
                .is_none()
            || !unique.insert(extension.as_str())
        {
            return Err(A2aClientBuildError::InvalidExtensions);
        }
        total = total
            .checked_add(extension.len())
            .ok_or(A2aClientBuildError::InvalidExtensions)?;
    }
    if total > MAX_EXTENSION_HEADER_BYTES || HeaderValue::from_str(&extensions.join(",")).is_err() {
        return Err(A2aClientBuildError::InvalidExtensions);
    }
    Ok(())
}

fn select_security_requirements(
    requirements: Option<&[wire::SecurityRequirement]>,
    schemes: Option<&std::collections::HashMap<String, wire::SecurityScheme>>,
    selection: &A2aClientSecurity,
) -> Result<Arc<[Box<str>]>, A2aClientSecurityError> {
    match selection {
        A2aClientSecurity::Anonymous => {
            let allowed = requirements.is_none_or(|requirements| {
                requirements.is_empty()
                    || requirements.iter().any(std::collections::HashMap::is_empty)
            });
            if allowed {
                Ok(Arc::<[Box<str>]>::from([]))
            } else {
                Err(A2aClientSecurityError::AnonymousNotAllowed)
            }
        }
        A2aClientSecurity::Bearer { scheme, .. } => {
            let compatible = schemes
                .and_then(|schemes| schemes.get(scheme.as_ref()))
                .is_some_and(|scheme| match scheme {
                    wire::SecurityScheme::HttpAuth(value) => {
                        value.scheme.eq_ignore_ascii_case("bearer")
                    }
                    wire::SecurityScheme::OAuth2(_) | wire::SecurityScheme::OpenIdConnect(_) => {
                        true
                    }
                    wire::SecurityScheme::ApiKey(_) | wire::SecurityScheme::MutualTls(_) => false,
                });
            if !compatible || !selection.requires_credentials() {
                return Err(A2aClientSecurityError::BearerSchemeUnsupported);
            }
            let scopes = requirements
                .unwrap_or_default()
                .iter()
                .find(|requirement| {
                    requirement.len() == 1 && requirement.contains_key(scheme.as_ref())
                })
                .and_then(|requirement| requirement.get(scheme.as_ref()))
                .ok_or(A2aClientSecurityError::RequirementNotSatisfied)?;
            let mut scopes = scopes
                .iter()
                .cloned()
                .map(String::into_boxed_str)
                .collect::<Vec<_>>();
            scopes.sort_unstable();
            scopes.dedup();
            Ok(scopes.into())
        }
    }
}

const fn invalid_response() -> A2aClientError {
    A2aClientError::after(A2aClientErrorKind::InvalidResponse)
}

fn validate_send_response(
    response: &A2aSendMessageResponse,
    expectation: &A2aSendExpectation,
) -> Result<(), A2aClientError> {
    match response {
        A2aSendMessageResponse::Task(task) => {
            if !expectation.return_immediately && !stream_state_closes(task.state()) {
                return Err(invalid_response());
            }
            validate_task_response(
                task,
                expectation.task_id.as_deref(),
                expectation.context_id.as_deref(),
                expectation.history_length,
            )
        }
        A2aSendMessageResponse::Message(message) => validate_agent_message(message, expectation),
    }
}

fn validate_agent_message(
    message: &A2aMessage,
    expectation: &A2aSendExpectation,
) -> Result<(), A2aClientError> {
    if message.role() != A2aMessageRole::Agent || message.context_id().is_none() {
        return Err(invalid_response());
    }
    if expectation
        .context_id
        .as_deref()
        .is_some_and(|expected| message.context_id() != Some(expected))
    {
        return Err(invalid_response());
    }
    match (expectation.task_id.as_deref(), message.task_id()) {
        (Some(expected), Some(observed)) if expected == observed => {}
        (None, None) => {}
        _ => return Err(invalid_response()),
    }
    Ok(())
}

fn validate_task_response(
    task: &A2aTask,
    expected_task_id: Option<&str>,
    expected_context_id: Option<&str>,
    history_length: Option<u32>,
) -> Result<(), A2aClientError> {
    if expected_task_id.is_some_and(|expected| task.id() != expected)
        || expected_context_id.is_some_and(|expected| task.context_id() != expected)
        || history_length.is_some_and(|maximum| task.history_len() > maximum as usize)
    {
        return Err(invalid_response());
    }
    let task_expectation = A2aSendExpectation {
        task_id: Some(task.id().into()),
        context_id: Some(task.context_id().into()),
        history_length: None,
        return_immediately: false,
    };
    if let Some(message) = task.status().message() {
        validate_agent_message(&message, &task_expectation)?;
    }
    for message in task.history() {
        if message
            .context_id()
            .is_some_and(|context_id| context_id != task.context_id())
            || message
                .task_id()
                .is_some_and(|task_id| task_id != task.id())
        {
            return Err(invalid_response());
        }
        if message.role() == A2aMessageRole::Agent {
            validate_agent_message(&message, &task_expectation)?;
        }
    }
    Ok(())
}

fn validate_status_update(
    update: &crate::A2aStatusUpdate,
    task_id: &str,
    context_id: &str,
) -> Result<(), A2aClientError> {
    if update.task_id() != task_id || update.context_id() != context_id {
        return Err(invalid_response());
    }
    if let Some(message) = update.status().message() {
        validate_agent_message(
            &message,
            &A2aSendExpectation {
                task_id: Some(task_id.into()),
                context_id: Some(context_id.into()),
                history_length: None,
                return_immediately: false,
            },
        )?;
    }
    Ok(())
}

fn validate_artifact_update(
    update: &crate::A2aArtifactUpdate,
    task_id: &str,
    context_id: &str,
    current: &A2aStreamArtifacts,
) -> Result<A2aStreamArtifacts, A2aClientError> {
    if update.task_id() != task_id || update.context_id() != context_id {
        return Err(invalid_response());
    }
    let artifact_id = update.artifact().artifact_id();
    if update.append()
        && (!current.ids.contains(artifact_id) || current.sealed_ids.contains(artifact_id))
    {
        return Err(invalid_response());
    }

    let mut next = current.clone();
    if !update.append() {
        next.ids.insert(artifact_id.into());
        next.sealed_ids.remove(artifact_id);
    }
    if update.last_chunk() {
        next.sealed_ids.insert(artifact_id.into());
    }
    Ok(next)
}

fn validate_task_page(
    page: &A2aTaskPage,
    expectation: &A2aListTasksExpectation,
) -> Result<(), A2aClientError> {
    if page.page_size() > expectation.page_size
        || page.tasks().len() > usize::from(expectation.page_size)
    {
        return Err(invalid_response());
    }
    for task in page.tasks() {
        let timestamp = task.status().timestamp();
        if expectation
            .context_id
            .as_deref()
            .is_some_and(|context_id| task.context_id() != context_id)
            || expectation
                .status
                .is_some_and(|status| task.state() != status)
            || expectation
                .status_timestamp_after
                .is_some_and(|after| timestamp.is_none_or(|value| value < after))
            || (!expectation.include_artifacts && task.has_artifact_projection())
        {
            return Err(invalid_response());
        }
        validate_task_response(task, None, None, expectation.history_length)?;
    }
    Ok(())
}

fn validate_push_config_identity(
    config: &A2aPushConfig,
    task_id: &str,
    config_id: Option<&str>,
    require_id: bool,
) -> Result<(), A2aClientError> {
    if config.task_id() != Some(task_id)
        || config_id.is_some_and(|expected| config.id() != Some(expected))
        || (require_id && config.id().is_none())
    {
        return Err(invalid_response());
    }
    Ok(())
}

fn validate_push_config_page(
    page: &A2aPushConfigPage,
    task_id: &str,
    page_size: u16,
) -> Result<(), A2aClientError> {
    if page.configs().len() > usize::from(page_size) {
        return Err(invalid_response());
    }
    let mut ids = HashSet::with_capacity(page.configs().len());
    for config in page.configs() {
        validate_push_config_identity(config, task_id, None, true)?;
        if !ids.insert(config.id().expect("configuration ID presence was checked")) {
            return Err(invalid_response());
        }
    }
    Ok(())
}

fn serialize_request<T: Serialize>(
    value: &T,
    options: ProviderHttpOptions,
) -> Result<Vec<u8>, A2aClientError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| A2aClientError::before(A2aClientErrorKind::Request))?;
    if bytes.len() > options.maximum_request_bytes() {
        return Err(A2aClientError::before(A2aClientErrorKind::Request));
    }
    Ok(bytes)
}

fn encode_value<T: Serialize>(value: &T) -> Result<Value, A2aClientError> {
    serde_json::to_value(value).map_err(|_| A2aClientError::before(A2aClientErrorKind::Request))
}

fn decode_value<T: DeserializeOwned>(value: Value) -> Result<T, A2aClientError> {
    serde_json::from_value(value)
        .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))
}

fn require_exact_variant(value: &Value, variants: &[&str]) -> Result<(), A2aClientError> {
    let object = value
        .as_object()
        .ok_or_else(|| A2aClientError::after(A2aClientErrorKind::InvalidResponse))?;
    if object.len() != 1 || !variants.iter().any(|variant| object.contains_key(*variant)) {
        return Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse));
    }
    Ok(())
}

fn parse_rpc_response(
    value: &Value,
    expected_id: i64,
    status: Option<StatusCode>,
) -> Result<Value, A2aClientError> {
    let object = value
        .as_object()
        .ok_or_else(|| A2aClientError::after(A2aClientErrorKind::InvalidResponse))?;
    let version = object.get("jsonrpc").and_then(Value::as_str);
    let id = object
        .get("id")
        .cloned()
        .and_then(|value| serde_json::from_value::<wire::JsonRpcId>(value).ok());
    let has_result = object.contains_key("result");
    let has_error = object.contains_key("error");
    if version != Some("2.0")
        || id != Some(wire::JsonRpcId::Number(expected_id))
        || has_result == has_error
    {
        return Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse));
    }
    if let Some(error) = object.get("error") {
        let error = serde_json::from_value::<wire::JsonRpcError>(error.clone())
            .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))?;
        if error.message.is_empty() || error.message.len() > MAX_REMOTE_ERROR_MESSAGE_BYTES {
            return Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse));
        }
        let authoritative_code = status
            .is_none_or(|status| status.is_success())
            .then_some(error.code);
        return Err(A2aClientError::remote(authoritative_code, status));
    }
    Ok(object
        .get("result")
        .expect("result presence was checked")
        .clone())
}

#[derive(Clone, Copy)]
struct ParsedRemoteError {
    code: Option<i32>,
}

fn parse_rest_error(
    value: &Value,
    expected_http_status: Option<StatusCode>,
) -> Option<ParsedRemoteError> {
    let error = value.get("error")?.as_object()?;
    let code = error.get("code")?.as_u64()?;
    let code = u16::try_from(code).ok()?;
    if expected_http_status.is_some_and(|expected| expected.as_u16() != code) {
        return None;
    }
    let status = error.get("status")?.as_str()?;
    if status.is_empty() || status.len() > 256 {
        return None;
    }
    let message = error.get("message")?.as_str()?;
    if message.is_empty() || message.len() > MAX_REMOTE_ERROR_MESSAGE_BYTES {
        return None;
    }
    let details = error.get("details")?.as_array()?;
    let mut remote_codes = details.iter().filter_map(|detail| {
        let detail = detail.as_object()?;
        if detail.get("@type")?.as_str()? != "type.googleapis.com/google.rpc.ErrorInfo" {
            return None;
        }
        if detail.get("domain")?.as_str()? != "a2a-protocol.org" {
            return None;
        }
        wire::reason_to_error_code(detail.get("reason")?.as_str()?)
    });
    let remote_code = match (remote_codes.next(), remote_codes.next()) {
        (Some(remote_code), None)
            if rest_error_identity(remote_code).is_some_and(
                |(expected_code, expected_status)| {
                    code == expected_code && status == expected_status
                },
            ) =>
        {
            Some(remote_code)
        }
        _ => None,
    };
    Some(ParsedRemoteError { code: remote_code })
}

fn rest_error_identity(code: i32) -> Option<(u16, &'static str)> {
    match code {
        wire::error_code::TASK_NOT_FOUND | wire::error_code::METHOD_NOT_FOUND => {
            Some((StatusCode::NOT_FOUND.as_u16(), "NOT_FOUND"))
        }
        wire::error_code::TASK_NOT_CANCELABLE => {
            Some((StatusCode::CONFLICT.as_u16(), "FAILED_PRECONDITION"))
        }
        wire::error_code::PUSH_NOTIFICATION_NOT_SUPPORTED
        | wire::error_code::UNSUPPORTED_OPERATION
        | wire::error_code::VERSION_NOT_SUPPORTED => {
            Some((StatusCode::BAD_REQUEST.as_u16(), "UNIMPLEMENTED"))
        }
        wire::error_code::CONTENT_TYPE_NOT_SUPPORTED => Some((
            StatusCode::UNSUPPORTED_MEDIA_TYPE.as_u16(),
            "INVALID_ARGUMENT",
        )),
        wire::error_code::INVALID_AGENT_RESPONSE => {
            Some((StatusCode::BAD_GATEWAY.as_u16(), "INTERNAL"))
        }
        wire::error_code::EXTENDED_CARD_NOT_CONFIGURED
        | wire::error_code::EXTENSION_SUPPORT_REQUIRED => {
            Some((StatusCode::BAD_REQUEST.as_u16(), "FAILED_PRECONDITION"))
        }
        wire::error_code::INVALID_REQUEST
        | wire::error_code::INVALID_PARAMS
        | wire::error_code::PARSE_ERROR => {
            Some((StatusCode::BAD_REQUEST.as_u16(), "INVALID_ARGUMENT"))
        }
        wire::error_code::INTERNAL_ERROR => {
            Some((StatusCode::INTERNAL_SERVER_ERROR.as_u16(), "INTERNAL"))
        }
        _ => None,
    }
}

fn parse_bare_rpc_error(value: &Value) -> Option<ParsedRemoteError> {
    let error = serde_json::from_value::<wire::JsonRpcError>(value.clone()).ok()?;
    if error.message.is_empty() || error.message.len() > MAX_REMOTE_ERROR_MESSAGE_BYTES {
        return None;
    }
    Some(ParsedRemoteError {
        code: Some(error.code),
    })
}

fn decode_push_config(
    mut value: wire::TaskPushNotificationConfig,
    expected_tenant: Option<&str>,
) -> Result<A2aPushConfig, A2aClientError> {
    strip_response_tenant(&mut value, expected_tenant)?;
    A2aPushConfig::try_from_wire(value)
        .map_err(|_| A2aClientError::after(A2aClientErrorKind::InvalidResponse))
}

fn strip_response_tenant(
    value: &mut wire::TaskPushNotificationConfig,
    expected_tenant: Option<&str>,
) -> Result<(), A2aClientError> {
    if value
        .tenant
        .as_deref()
        .is_some_and(|tenant| Some(tenant) != expected_tenant)
    {
        return Err(A2aClientError::after(A2aClientErrorKind::InvalidResponse));
    }
    value.tenant = None;
    Ok(())
}

fn allocate_request_id(counter: &AtomicU64) -> Option<i64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            (value < i64::MAX as u64).then_some(value + 1)
        })
        .ok()
        .and_then(|value| i64::try_from(value).ok())
}

async fn wait_until<T, F>(deadline: tokio::time::Instant, future: F) -> Result<T, ()>
where
    F: std::future::Future<Output = T>,
{
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| ())
}

#[derive(Clone, Copy)]
enum BoundedBodyError {
    Transport,
    TooLarge,
}

async fn bounded_body(
    response: reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, BoundedBodyError> {
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(BoundedBodyError::TooLarge);
    }
    let mut output = Vec::new();
    let mut body = response.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| BoundedBodyError::Transport)?;
        if output.len().saturating_add(chunk.len()) > maximum {
            return Err(BoundedBodyError::TooLarge);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

fn is_json_content_type(headers: &HeaderMap) -> bool {
    exactly_one_header(headers, header::CONTENT_TYPE).is_some_and(|value| {
        value.parse::<mime::Mime>().is_ok_and(|mime| {
            mime.essence_str().eq_ignore_ascii_case(JSON)
                || mime.essence_str().eq_ignore_ascii_case(A2A_JSON)
        })
    })
}

fn is_event_stream_content_type(headers: &HeaderMap) -> bool {
    exactly_one_header(headers, header::CONTENT_TYPE).is_some_and(|value| {
        value
            .parse::<mime::Mime>()
            .is_ok_and(|mime| mime.essence_str().eq_ignore_ascii_case(EVENT_STREAM))
    })
}

fn exactly_one_header(headers: &HeaderMap, name: header::HeaderName) -> Option<&str> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?.to_str().ok()?;
    values.next().is_none().then_some(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn endpoints_fail_closed_outside_exact_https_or_literal_loopback_profiles() {
        assert!(
            A2aAgentCardEndpoint::https("https://agent.example/.well-known/agent-card.json")
                .is_ok()
        );
        assert_eq!(
            A2aAgentCardEndpoint::https("http://agent.example/.well-known/agent-card.json")
                .unwrap_err(),
            A2aClientEndpointError::HttpsRequired
        );
        assert_eq!(
            A2aAgentCardEndpoint::loopback_http(
                "http://localhost:8080/.well-known/agent-card.json"
            )
            .unwrap_err(),
            A2aClientEndpointError::LiteralLoopbackRequired
        );
        assert_eq!(
            A2aAgentCardEndpoint::loopback_http("http://127.0.0.1:8080/card.json").unwrap_err(),
            A2aClientEndpointError::AgentCardPathRequired
        );
        assert_eq!(
            A2aClientInterfacePin::https(
                "https://agent.example/a2a?tenant=untrusted",
                A2aBinding::HttpJson,
            )
            .unwrap_err(),
            A2aClientEndpointError::QueryOrFragment
        );
        assert_eq!(
            A2aClientInterfacePin::https("https://agent.example/a2a", A2aBinding::HttpJson,)
                .unwrap()
                .with_tenant(" tenant-a")
                .unwrap_err(),
            A2aClientEndpointError::InvalidTenant
        );
    }

    #[test]
    fn options_and_extension_headers_have_hard_finite_bounds() {
        assert_eq!(
            A2aClientOptions::new(
                ProviderHttpOptions::default(),
                Duration::ZERO,
                Duration::from_secs(1),
                Duration::from_secs(1),
                1,
            ),
            Err(A2aClientOptionsError::ZeroTimeout)
        );
        assert_eq!(
            A2aClientOptions::new(
                ProviderHttpOptions::default(),
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
                A2aClientOptions::HARD_MAXIMUM_STREAM_EVENTS + 1,
            ),
            Err(A2aClientOptionsError::InvalidStreamEventLimit)
        );
        assert!(
            validate_extension_selection(&["https://extensions.example/v1".to_string()]).is_ok()
        );
        assert!(
            validate_extension_selection(&["https://extensions.example/v1,other".to_string()])
                .is_err()
        );
    }

    #[test]
    fn response_unions_require_one_and_only_one_wire_variant() {
        assert!(require_exact_variant(&json!({"message": {}}), &["task", "message"]).is_ok());
        for invalid in [
            json!({}),
            json!({"task": {}, "message": {}}),
            json!({"message": {}, "unknown": true}),
            json!({"unknown": {}}),
            json!([]),
        ] {
            let error = require_exact_variant(&invalid, &["task", "message"]).unwrap_err();
            assert_eq!(error.kind(), A2aClientErrorKind::InvalidResponse);
            assert!(error.was_dispatched());
        }
    }

    #[test]
    fn rest_errors_only_expose_standard_codes_not_remote_text() {
        let canonical = json!({
            "error": {
                "code": 400,
                "status": "INVALID_ARGUMENT",
                "message": "sensitive remote diagnostic",
                "details": [{
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "INVALID_PARAMS",
                    "domain": "a2a-protocol.org"
                }]
            }
        });
        let parsed = parse_rest_error(&canonical, Some(StatusCode::BAD_REQUEST)).unwrap();
        assert_eq!(parsed.code, Some(wire::error_code::INVALID_PARAMS));
        let error = A2aClientError::remote(parsed.code, Some(StatusCode::BAD_REQUEST));
        let debug = format!("{error:?}");
        assert!(!debug.contains("sensitive remote diagnostic"));
        assert_eq!(error.http_status(), Some(400));

        assert!(parse_rest_error(&canonical, Some(StatusCode::INTERNAL_SERVER_ERROR)).is_none());
        let mut inconsistent = canonical.clone();
        inconsistent["error"]["code"] = json!(500);
        inconsistent["error"]["status"] = json!("INTERNAL");
        assert_eq!(
            parse_rest_error(&inconsistent, Some(StatusCode::INTERNAL_SERVER_ERROR))
                .unwrap()
                .code,
            None
        );
        let mut foreign_domain = canonical;
        foreign_domain["error"]["details"][0]["domain"] = json!("example.invalid");
        assert_eq!(
            parse_rest_error(&foreign_domain, Some(StatusCode::BAD_REQUEST))
                .unwrap()
                .code,
            None
        );
        let mut ambiguous = inconsistent;
        ambiguous["error"]["code"] = json!(400);
        ambiguous["error"]["status"] = json!("INVALID_ARGUMENT");
        let duplicate = ambiguous["error"]["details"][0].clone();
        ambiguous["error"]["details"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        assert_eq!(
            parse_rest_error(&ambiguous, Some(StatusCode::BAD_REQUEST))
                .unwrap()
                .code,
            None
        );

        let rpc_error = parse_rpc_response(
            &json!({
                "jsonrpc": "2.0",
                "id": 7,
                "error": {"code": wire::error_code::INVALID_PARAMS, "message": "invalid"}
            }),
            7,
            Some(StatusCode::INTERNAL_SERVER_ERROR),
        )
        .unwrap_err();
        assert_eq!(rpc_error.kind(), A2aClientErrorKind::Remote);
        assert_eq!(rpc_error.remote_code(), None);
    }

    #[test]
    fn stream_artifact_appends_require_an_unsealed_prior_artifact() {
        let task = A2aTask::new(
            "task-1",
            "context-1",
            crate::A2aTaskStatus::new(A2aTaskState::Working, None, None).unwrap(),
        )
        .unwrap();
        let artifact =
            crate::A2aArtifact::new("artifact-1", vec![crate::A2aPart::text("chunk").unwrap()])
                .unwrap();
        let append = crate::A2aArtifactUpdate::new("task-1", "context-1", artifact.clone())
            .unwrap()
            .as_chunk(false);
        let initial = crate::A2aArtifactUpdate::new("task-1", "context-1", artifact.clone())
            .unwrap()
            .as_initial_chunk();
        let final_chunk = crate::A2aArtifactUpdate::new("task-1", "context-1", artifact)
            .unwrap()
            .as_chunk(true);
        let mut validator =
            A2aStreamValidator::new(A2aStreamContract::SubscribeToTask("task-1".into()));
        validator.observe(&A2aStreamEvent::Task(task)).unwrap();

        assert!(
            validator
                .observe(&A2aStreamEvent::ArtifactUpdate(append.clone()))
                .is_err()
        );
        validator
            .observe(&A2aStreamEvent::ArtifactUpdate(initial))
            .unwrap();
        validator
            .observe(&A2aStreamEvent::ArtifactUpdate(append.clone()))
            .unwrap();
        validator
            .observe(&A2aStreamEvent::ArtifactUpdate(final_chunk))
            .unwrap();
        assert!(
            validator
                .observe(&A2aStreamEvent::ArtifactUpdate(append))
                .is_err()
        );
    }

    #[test]
    fn task_streams_close_on_interruption_and_reject_repeated_task_snapshots() {
        let task = |state| {
            A2aTask::new(
                "task-1",
                "context-1",
                crate::A2aTaskStatus::new(state, None, None).unwrap(),
            )
            .unwrap()
        };
        let mut validator =
            A2aStreamValidator::new(A2aStreamContract::SubscribeToTask("task-1".into()));
        validator
            .observe(&A2aStreamEvent::Task(task(A2aTaskState::Working)))
            .unwrap();
        let interrupted = crate::A2aStatusUpdate::new(
            "task-1",
            "context-1",
            crate::A2aTaskStatus::new(A2aTaskState::InputRequired, None, None).unwrap(),
        )
        .unwrap();
        validator
            .observe(&A2aStreamEvent::StatusUpdate(interrupted))
            .unwrap();
        assert!(validator.can_end());

        let mut repeated =
            A2aStreamValidator::new(A2aStreamContract::SubscribeToTask("task-1".into()));
        repeated
            .observe(&A2aStreamEvent::Task(task(A2aTaskState::Working)))
            .unwrap();
        assert!(
            repeated
                .observe(&A2aStreamEvent::Task(task(A2aTaskState::Completed)))
                .is_err()
        );
    }

    #[test]
    fn blocking_send_rejects_an_in_progress_task_snapshot() {
        let response = |state| {
            A2aSendMessageResponse::Task(
                A2aTask::new(
                    "task-1",
                    "context-1",
                    crate::A2aTaskStatus::new(state, None, None).unwrap(),
                )
                .unwrap(),
            )
        };
        let mut expectation = A2aSendExpectation {
            task_id: None,
            context_id: None,
            history_length: None,
            return_immediately: false,
        };
        assert!(validate_send_response(&response(A2aTaskState::Working), &expectation).is_err());
        validate_send_response(&response(A2aTaskState::InputRequired), &expectation).unwrap();
        expectation.return_immediately = true;
        validate_send_response(&response(A2aTaskState::Working), &expectation).unwrap();
    }

    #[test]
    fn task_pages_enforce_timestamp_filters_and_newest_first_ordering() {
        let after = "2026-09-03T08:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let newer = "2026-09-03T08:00:02Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let before = "2026-09-03T07:59:59Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        let request = A2aListTasksRequest::new()
            .with_page_size(2)
            .unwrap()
            .with_status_timestamp_after(after);
        let expectation = A2aListTasksExpectation::from_request(&request);
        let task = |id, timestamp| {
            A2aTask::new(
                id,
                "context-1",
                crate::A2aTaskStatus::new(A2aTaskState::Working, None, timestamp).unwrap(),
            )
            .unwrap()
        };

        let valid = A2aTaskPage::new(
            vec![task("task-new", Some(newer)), task("task-at", Some(after))],
            None,
            2,
            2,
        )
        .unwrap();
        validate_task_page(&valid, &expectation).unwrap();

        assert!(
            A2aTaskPage::new(
                vec![task("task-at", Some(after)), task("task-new", Some(newer))],
                None,
                2,
                2,
            )
            .is_err()
        );

        let missing_timestamp =
            A2aTaskPage::new(vec![task("task-unknown", None)], None, 2, 1).unwrap();
        assert!(validate_task_page(&missing_timestamp, &expectation).is_err());

        let before_bound =
            A2aTaskPage::new(vec![task("task-before", Some(before))], None, 2, 1).unwrap();
        assert!(validate_task_page(&before_bound, &expectation).is_err());
    }
}
