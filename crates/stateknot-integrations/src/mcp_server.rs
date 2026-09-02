// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Production Streamable HTTP boundary for MCP 2026-07-28 servers.
//!
//! The official Rust SDK owns JSON-RPC and protocol serialization. This module
//! owns the deployment invariants that an application must not have to rebuild:
//! one stateless protocol revision, strict Host and Origin allowlists, bounded
//! bodies and execution, overload rejection, redacted bearer authentication,
//! caller-provided admission control, request-principal propagation, and
//! cooperative shutdown.

use std::{
    convert::Infallible,
    fmt,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http::{
    HeaderValue, Request, Response, StatusCode,
    header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, PRAGMA, RETRY_AFTER, WWW_AUTHENTICATE},
    request::Parts,
    uri::Authority,
};
use http_body::Body;
use http_body_util::{BodyExt as _, Full, combinators::BoxBody};
use rmcp::{
    RoleServer, ServerHandler,
    model::ProtocolVersion,
    service::RequestContext,
    transport::{
        StreamableHttpServerConfig, StreamableHttpService,
        streamable_http_server::session::never::NeverSessionManager,
    },
};
use stateknot_core::BoxFuture;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tower_service::Service;
use zeroize::Zeroizing;

const MCP_METHOD_HEADER: &str = "mcp-method";
const MCP_NAME_HEADER: &str = "mcp-name";
const MAX_HINT_BYTES: usize = 1024;

type McpHttpResponse = Response<BoxBody<Bytes, Infallible>>;

/// An inbound RFC 6750 bearer credential.
///
/// The value is zeroized on final drop and its `Debug` representation is
/// always redacted. An authenticator must call [`Self::expose_secret`] only for
/// verification and must not retain, log, or return the plaintext value.
#[derive(Clone)]
pub struct McpServerBearerCredential(Zeroizing<String>);

impl McpServerBearerCredential {
    /// Maximum accepted credential length in bytes.
    pub const MAX_BYTES: usize = 8192;

    /// Validates an RFC 6750 `b64token` value.
    pub fn new(value: impl Into<String>) -> Result<Self, McpServerBearerCredentialError> {
        let value = Zeroizing::new(value.into());
        if value.is_empty() {
            return Err(McpServerBearerCredentialError::Empty);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(McpServerBearerCredentialError::TooLong);
        }

        let mut padding = false;
        for byte in value.bytes() {
            if byte == b'=' {
                padding = true;
                continue;
            }
            if padding || !is_b64_token_byte(byte) {
                return Err(McpServerBearerCredentialError::InvalidSyntax);
            }
        }
        Ok(Self(value))
    }

    /// Borrows the credential for immediate verification.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for McpServerBearerCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpServerBearerCredential([REDACTED])")
    }
}

fn is_b64_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
}

/// Invalid inbound bearer credential syntax.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerBearerCredentialError {
    /// No credential was supplied after the scheme.
    #[error("MCP bearer credential is empty")]
    Empty,
    /// The credential exceeded the hard byte ceiling.
    #[error("MCP bearer credential is too long")]
    TooLong,
    /// The credential is not an RFC 6750 `b64token`.
    #[error("MCP bearer credential has invalid syntax")]
    InvalidSyntax,
}

/// Authenticated identity injected into an MCP request context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerPrincipal {
    subject: Arc<str>,
    scopes: Arc<[Box<str>]>,
}

impl McpServerPrincipal {
    /// Maximum subject length in bytes.
    pub const MAX_SUBJECT_BYTES: usize = 512;
    /// Maximum number of normalized scopes.
    pub const MAX_SCOPES: usize = 128;
    /// Maximum scope length in bytes.
    pub const MAX_SCOPE_BYTES: usize = 256;

    /// Constructs a bounded principal and canonicalizes scopes by ASCII order.
    pub fn new(
        subject: impl Into<String>,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, McpServerPrincipalError> {
        let subject = subject.into();
        validate_display_value(&subject, Self::MAX_SUBJECT_BYTES)
            .map_err(|_| McpServerPrincipalError::InvalidSubject)?;

        let mut scopes = scopes.into_iter().map(Into::into).collect::<Vec<_>>();
        if scopes.len() > Self::MAX_SCOPES {
            return Err(McpServerPrincipalError::TooManyScopes);
        }
        for scope in &scopes {
            validate_scope(scope)?;
        }
        scopes.sort_unstable();
        if scopes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(McpServerPrincipalError::DuplicateScope);
        }

        Ok(Self {
            subject: Arc::from(subject),
            scopes: scopes
                .into_iter()
                .map(String::into_boxed_str)
                .collect::<Vec<_>>()
                .into(),
        })
    }

    fn anonymous() -> Self {
        Self {
            subject: Arc::from("anonymous"),
            scopes: Arc::from([]),
        }
    }

    /// Returns the stable subject identifier.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns canonical scopes in exact ASCII order.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes.iter().map(AsRef::as_ref)
    }

    /// Returns whether the exact scope was granted.
    #[must_use]
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes
            .binary_search_by(|candidate| candidate.as_ref().cmp(scope))
            .is_ok()
    }
}

/// Invalid authenticated principal.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerPrincipalError {
    /// The subject is empty, padded, oversized, or contains a control character.
    #[error("invalid MCP principal subject")]
    InvalidSubject,
    /// More than the hard scope ceiling was supplied.
    #[error("too many MCP principal scopes")]
    TooManyScopes,
    /// A scope is empty, oversized, or outside the OAuth scope-token grammar.
    #[error("invalid MCP principal scope")]
    InvalidScope,
    /// A scope appeared more than once.
    #[error("duplicate MCP principal scope")]
    DuplicateScope,
}

fn validate_scope(scope: &str) -> Result<(), McpServerPrincipalError> {
    if scope.is_empty()
        || scope.len() > McpServerPrincipal::MAX_SCOPE_BYTES
        || scope
            .bytes()
            .any(|byte| !matches!(byte, b'!' | b'#'..=b'[' | b']'..=b'~'))
    {
        return Err(McpServerPrincipalError::InvalidScope);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
enum McpServerTextError {
    #[error("value is empty")]
    Empty,
    #[error("value is too long")]
    TooLong,
    #[error("value has boundary whitespace")]
    BoundaryWhitespace,
    #[error("value contains a control character")]
    ControlCharacter,
}

fn validate_display_value(value: &str, maximum: usize) -> Result<(), McpServerTextError> {
    if value.is_empty() {
        return Err(McpServerTextError::Empty);
    }
    if value.len() > maximum {
        return Err(McpServerTextError::TooLong);
    }
    if value.trim() != value {
        return Err(McpServerTextError::BoundaryWhitespace);
    }
    if value.chars().any(char::is_control) {
        return Err(McpServerTextError::ControlCharacter);
    }
    Ok(())
}

/// Public-safe facts passed to a bearer authenticator.
#[derive(Debug)]
pub struct McpServerAuthenticationRequest {
    credential: McpServerBearerCredential,
    transport_method_hint: Option<Box<str>>,
    transport_name_hint: Option<Box<str>>,
}

impl McpServerAuthenticationRequest {
    /// Returns the redacted credential container.
    #[must_use]
    pub const fn credential(&self) -> &McpServerBearerCredential {
        &self.credential
    }

    /// Returns the untrusted `Mcp-Method` transport hint.
    ///
    /// Authentication may use this for telemetry, but authorization must use
    /// the decoded method after the transport has validated standard headers.
    #[must_use]
    pub fn transport_method_hint(&self) -> Option<&str> {
        self.transport_method_hint.as_deref()
    }

    /// Returns the untrusted `Mcp-Name` transport hint.
    #[must_use]
    pub fn transport_name_hint(&self) -> Option<&str> {
        self.transport_name_hint.as_deref()
    }
}

/// Public-safe authentication failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerAuthenticationError {
    /// The credential is unknown, expired, revoked, or otherwise invalid.
    #[error("MCP bearer credential is invalid")]
    InvalidCredential,
    /// The authenticated identity is not permitted to access this resource.
    #[error("MCP access is forbidden")]
    Forbidden,
    /// The identity provider or credential backend is temporarily unavailable.
    #[error("MCP authentication service is unavailable")]
    Unavailable,
}

/// Verifies one inbound bearer credential.
pub trait McpServerAuthenticator: Send + Sync + 'static {
    /// Authenticates the request and returns a bounded principal.
    fn authenticate(
        &self,
        request: McpServerAuthenticationRequest,
    ) -> BoxFuture<'_, Result<McpServerPrincipal, McpServerAuthenticationError>>;
}

/// A validated RFC 6750 challenge emitted without reflecting request input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerBearerChallenge {
    value: HeaderValue,
}

impl McpServerBearerChallenge {
    /// Builds a Bearer challenge with an optional RFC 9728 resource-metadata URL.
    pub fn new(
        realm: impl Into<String>,
        resource_metadata: Option<impl Into<String>>,
    ) -> Result<Self, McpServerBearerChallengeError> {
        let realm = realm.into();
        validate_challenge_component(&realm)?;
        let resource_metadata = resource_metadata.map(Into::into);
        if let Some(value) = &resource_metadata {
            validate_challenge_component(value)?;
            let uri = value
                .parse::<http::Uri>()
                .map_err(|_| McpServerBearerChallengeError::InvalidResourceMetadata)?;
            if !matches!(uri.scheme_str(), Some("https")) || uri.authority().is_none() {
                return Err(McpServerBearerChallengeError::InvalidResourceMetadata);
            }
        }

        let rendered = resource_metadata.map_or_else(
            || format!("Bearer realm=\"{realm}\""),
            |metadata| format!("Bearer realm=\"{realm}\", resource_metadata=\"{metadata}\""),
        );
        let value = HeaderValue::from_str(&rendered)
            .map_err(|_| McpServerBearerChallengeError::InvalidHeader)?;
        Ok(Self { value })
    }
}

fn validate_challenge_component(value: &str) -> Result<(), McpServerBearerChallengeError> {
    if value.is_empty()
        || value.len() > 4096
        || value
            .bytes()
            .any(|byte| !(b' '..=b'~').contains(&byte) || matches!(byte, b'"' | b'\\'))
    {
        return Err(McpServerBearerChallengeError::InvalidComponent);
    }
    Ok(())
}

/// Invalid static Bearer challenge configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerBearerChallengeError {
    /// A challenge component was empty, oversized, non-ASCII, or quote-unsafe.
    #[error("invalid MCP Bearer challenge component")]
    InvalidComponent,
    /// Resource metadata must be an absolute HTTPS URL.
    #[error("invalid MCP protected-resource metadata URL")]
    InvalidResourceMetadata,
    /// The complete challenge could not be represented as an HTTP header.
    #[error("invalid MCP Bearer challenge header")]
    InvalidHeader,
}

#[derive(Clone)]
enum McpServerAuthenticationMode {
    AnonymousLoopback,
    Bearer {
        authenticator: Arc<dyn McpServerAuthenticator>,
        challenge: McpServerBearerChallenge,
    },
}

/// Authentication policy for an MCP HTTP service.
#[derive(Clone)]
pub struct McpServerAuthentication {
    mode: McpServerAuthenticationMode,
}

impl McpServerAuthentication {
    /// Allows an anonymous principal only when every configured host is loopback.
    #[must_use]
    pub const fn anonymous_loopback() -> Self {
        Self {
            mode: McpServerAuthenticationMode::AnonymousLoopback,
        }
    }

    /// Requires a Bearer credential and delegates verification.
    #[must_use]
    pub fn bearer<A>(authenticator: A, challenge: McpServerBearerChallenge) -> Self
    where
        A: McpServerAuthenticator,
    {
        Self::bearer_shared(Arc::new(authenticator), challenge)
    }

    /// Requires a Bearer credential using an already shared authenticator.
    #[must_use]
    pub fn bearer_shared(
        authenticator: Arc<dyn McpServerAuthenticator>,
        challenge: McpServerBearerChallenge,
    ) -> Self {
        Self {
            mode: McpServerAuthenticationMode::Bearer {
                authenticator,
                challenge,
            },
        }
    }
}

impl fmt::Debug for McpServerAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.mode {
            McpServerAuthenticationMode::AnonymousLoopback => {
                formatter.write_str("McpServerAuthentication::AnonymousLoopback")
            }
            McpServerAuthenticationMode::Bearer { .. } => {
                formatter.write_str("McpServerAuthentication::Bearer([REDACTED])")
            }
        }
    }
}

/// Authenticated, transport-level facts passed to admission control.
#[derive(Clone, Debug)]
pub struct McpServerAdmissionRequest {
    principal: McpServerPrincipal,
    transport_method_hint: Option<Box<str>>,
    transport_name_hint: Option<Box<str>>,
}

impl McpServerAdmissionRequest {
    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &McpServerPrincipal {
        &self.principal
    }

    /// Returns the untrusted transport method hint.
    #[must_use]
    pub fn transport_method_hint(&self) -> Option<&str> {
        self.transport_method_hint.as_deref()
    }

    /// Returns the untrusted transport name hint.
    #[must_use]
    pub fn transport_name_hint(&self) -> Option<&str> {
        self.transport_name_hint.as_deref()
    }
}

/// Public-safe admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerAdmissionError {
    /// Policy denied this authenticated request.
    #[error("MCP request is forbidden")]
    Forbidden,
    /// A quota or rate limit was reached.
    #[error("MCP request is rate limited")]
    RateLimited {
        /// Minimum delay before a new request should be attempted.
        retry_after: Duration,
    },
    /// The policy backend is temporarily unavailable.
    #[error("MCP admission service is unavailable")]
    Unavailable,
}

/// Caller-owned admission boundary for shared quota and coarse request policy.
///
/// The built-in semaphore protects one process. Cross-replica quotas belong in
/// this trait so production deployments can use one authoritative policy store.
/// Tool/resource/prompt authorization must still run in the decoded handler;
/// transport hints have not yet been validated against the JSON-RPC body.
pub trait McpServerAdmissionControl: Send + Sync + 'static {
    /// Admits or rejects one authenticated request.
    fn admit(
        &self,
        request: McpServerAdmissionRequest,
    ) -> BoxFuture<'_, Result<(), McpServerAdmissionError>>;
}

/// Explicit admission policy that accepts every authenticated request.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowMcpServerAdmission;

impl McpServerAdmissionControl for AllowMcpServerAdmission {
    fn admit(
        &self,
        _request: McpServerAdmissionRequest,
    ) -> BoxFuture<'_, Result<(), McpServerAdmissionError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Strict bounded Streamable HTTP policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpServerHttpOptions {
    allowed_hosts: Arc<[Box<str>]>,
    allowed_origins: Arc<[Box<str>]>,
    maximum_request_body_bytes: usize,
    maximum_concurrent_requests: usize,
    request_timeout: Duration,
}

impl McpServerHttpOptions {
    /// Maximum number of Host or Origin entries.
    pub const HARD_MAXIMUM_ALLOWLIST_ENTRIES: usize = 128;
    /// Maximum accepted request body ceiling.
    pub const HARD_MAXIMUM_REQUEST_BODY_BYTES: usize = 64 * 1024 * 1024;
    /// Maximum local in-flight request ceiling.
    pub const HARD_MAXIMUM_CONCURRENT_REQUESTS: usize = 4096;
    /// Maximum request deadline.
    pub const HARD_MAXIMUM_REQUEST_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

    /// Constructs an explicit public or private deployment policy.
    pub fn new(
        allowed_hosts: impl IntoIterator<Item = impl Into<String>>,
        allowed_origins: impl IntoIterator<Item = impl Into<String>>,
        maximum_request_body_bytes: usize,
        maximum_concurrent_requests: usize,
        request_timeout: Duration,
    ) -> Result<Self, McpServerHttpOptionsError> {
        let allowed_hosts = allowed_hosts
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let allowed_origins = allowed_origins
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();

        validate_allowed_hosts(&allowed_hosts)?;
        validate_allowed_origins(&allowed_origins)?;
        if maximum_request_body_bytes == 0
            || maximum_concurrent_requests == 0
            || request_timeout.is_zero()
        {
            return Err(McpServerHttpOptionsError::ZeroLimit);
        }
        if maximum_request_body_bytes > Self::HARD_MAXIMUM_REQUEST_BODY_BYTES
            || maximum_concurrent_requests > Self::HARD_MAXIMUM_CONCURRENT_REQUESTS
            || request_timeout.as_nanos() > Self::HARD_MAXIMUM_REQUEST_TIMEOUT.as_nanos()
        {
            return Err(McpServerHttpOptionsError::AboveHardMaximum);
        }

        Ok(Self {
            allowed_hosts: allowed_hosts
                .into_iter()
                .map(String::into_boxed_str)
                .collect::<Vec<_>>()
                .into(),
            allowed_origins: allowed_origins
                .into_iter()
                .map(String::into_boxed_str)
                .collect::<Vec<_>>()
                .into(),
            maximum_request_body_bytes,
            maximum_concurrent_requests,
            request_timeout,
        })
    }

    /// Constructs a strict development or conformance policy for one loopback port.
    pub fn loopback(port: u16) -> Result<Self, McpServerHttpOptionsError> {
        Self::new(
            [
                format!("127.0.0.1:{port}"),
                format!("localhost:{port}"),
                format!("[::1]:{port}"),
            ],
            [
                format!("http://127.0.0.1:{port}"),
                format!("http://localhost:{port}"),
                format!("http://[::1]:{port}"),
            ],
            4 * 1024 * 1024,
            128,
            Duration::from_secs(30),
        )
    }

    /// Returns allowed HTTP Host authorities.
    pub fn allowed_hosts(&self) -> impl ExactSizeIterator<Item = &str> {
        self.allowed_hosts.iter().map(AsRef::as_ref)
    }

    /// Returns allowed browser origins.
    pub fn allowed_origins(&self) -> impl ExactSizeIterator<Item = &str> {
        self.allowed_origins.iter().map(AsRef::as_ref)
    }

    /// Returns the streaming body ceiling.
    #[must_use]
    pub const fn maximum_request_body_bytes(&self) -> usize {
        self.maximum_request_body_bytes
    }

    /// Returns the local in-flight request ceiling.
    #[must_use]
    pub const fn maximum_concurrent_requests(&self) -> usize {
        self.maximum_concurrent_requests
    }

    /// Returns the deadline for authentication, admission, and handler dispatch.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    fn is_loopback_only(&self) -> bool {
        self.allowed_hosts.iter().all(|allowed| {
            Authority::try_from(allowed.as_ref()).is_ok_and(|authority| {
                matches!(
                    authority
                        .host()
                        .trim_matches(|character| matches!(character, '[' | ']'))
                        .to_ascii_lowercase()
                        .as_str(),
                    "localhost" | "127.0.0.1" | "::1"
                )
            })
        })
    }
}

fn validate_allowed_hosts(values: &[String]) -> Result<(), McpServerHttpOptionsError> {
    if values.is_empty() || values.len() > McpServerHttpOptions::HARD_MAXIMUM_ALLOWLIST_ENTRIES {
        return Err(McpServerHttpOptionsError::InvalidHostAllowlist);
    }
    for value in values {
        if value.trim() != value
            || value.len() > 1024
            || value.contains('*')
            || Authority::try_from(value.as_str()).is_err()
        {
            return Err(McpServerHttpOptionsError::InvalidHostAllowlist);
        }
    }
    if has_duplicates(values) {
        return Err(McpServerHttpOptionsError::DuplicateAllowlistEntry);
    }
    Ok(())
}

fn validate_allowed_origins(values: &[String]) -> Result<(), McpServerHttpOptionsError> {
    if values.is_empty() || values.len() > McpServerHttpOptions::HARD_MAXIMUM_ALLOWLIST_ENTRIES {
        return Err(McpServerHttpOptionsError::InvalidOriginAllowlist);
    }
    for value in values {
        let Ok(uri) = value.parse::<http::Uri>() else {
            return Err(McpServerHttpOptionsError::InvalidOriginAllowlist);
        };
        if value.trim() != value
            || value.len() > 2048
            || !matches!(uri.scheme_str(), Some("http" | "https"))
            || uri.authority().is_none()
            || !matches!(uri.path(), "" | "/")
            || uri.query().is_some()
        {
            return Err(McpServerHttpOptionsError::InvalidOriginAllowlist);
        }
    }
    if has_duplicates(values) {
        return Err(McpServerHttpOptionsError::DuplicateAllowlistEntry);
    }
    Ok(())
}

fn has_duplicates(values: &[String]) -> bool {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted.windows(2).any(|pair| pair[0] == pair[1])
}

/// Invalid MCP HTTP deployment policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerHttpOptionsError {
    /// Host validation cannot be disabled and wildcards are rejected.
    #[error("invalid MCP Host allowlist")]
    InvalidHostAllowlist,
    /// Origin validation cannot be disabled and entries must be HTTP(S) origins.
    #[error("invalid MCP Origin allowlist")]
    InvalidOriginAllowlist,
    /// An allowlist entry appeared more than once.
    #[error("duplicate MCP allowlist entry")]
    DuplicateAllowlistEntry,
    /// A body, concurrency, or time limit was zero.
    #[error("MCP server resource limits must be positive")]
    ZeroLimit,
    /// A resource limit exceeded the implementation hard maximum.
    #[error("MCP server resource limit exceeds the hard maximum")]
    AboveHardMaximum,
}

/// Construction failure for the production MCP service.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpServerHttpServiceBuildError {
    /// The handler advertised a revision outside the frozen server profile.
    #[error("MCP server handler must support exactly protocol version 2026-07-28")]
    UnsupportedProtocolProfile,
    /// Anonymous serving is confined to literal loopback hosts.
    #[error("anonymous MCP serving is allowed only on loopback hosts")]
    AnonymousNonLoopback,
}

/// Cloneable production HTTP service for one MCP handler.
///
/// The service implements Tower's [`Service`] and can be mounted directly by
/// Axum, Hyper, or another compatible server. Its transport is always
/// stateless: legacy sessions and `initialize` are disabled.
pub struct McpServerHttpService<H> {
    inner: StreamableHttpService<H, NeverSessionManager>,
    options: McpServerHttpOptions,
    authentication: McpServerAuthentication,
    admission: Arc<dyn McpServerAdmissionControl>,
    concurrency: Arc<Semaphore>,
    shutdown: CancellationToken,
}

impl<H> Clone for McpServerHttpService<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            options: self.options.clone(),
            authentication: self.authentication.clone(),
            admission: self.admission.clone(),
            concurrency: self.concurrency.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
}

impl<H> fmt::Debug for McpServerHttpService<H> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpServerHttpService")
            .field("options", &self.options)
            .field("authentication", &self.authentication)
            .field("shutting_down", &self.shutdown.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl<H> McpServerHttpService<H>
where
    H: ServerHandler + Clone + Send + Sync + 'static,
{
    /// Builds a strict service with permissive authenticated admission.
    pub fn new(
        handler: H,
        options: McpServerHttpOptions,
        authentication: McpServerAuthentication,
    ) -> Result<Self, McpServerHttpServiceBuildError> {
        Self::with_admission_control(
            handler,
            options,
            authentication,
            Arc::new(AllowMcpServerAdmission),
        )
    }

    /// Builds a strict service with caller-owned shared admission control.
    pub fn with_admission_control(
        handler: H,
        options: McpServerHttpOptions,
        authentication: McpServerAuthentication,
        admission: Arc<dyn McpServerAdmissionControl>,
    ) -> Result<Self, McpServerHttpServiceBuildError> {
        let supported = handler.supported_protocol_versions();
        if supported.as_ref() != [ProtocolVersion::V_2026_07_28] {
            return Err(McpServerHttpServiceBuildError::UnsupportedProtocolProfile);
        }
        if matches!(
            &authentication.mode,
            McpServerAuthenticationMode::AnonymousLoopback
        ) && !options.is_loopback_only()
        {
            return Err(McpServerHttpServiceBuildError::AnonymousNonLoopback);
        }

        let shutdown = CancellationToken::new();
        let config = StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .with_stateless_protocol_metadata_required(true)
            .with_allowed_hosts(options.allowed_hosts().map(str::to_owned))
            .with_allowed_origins(options.allowed_origins().map(str::to_owned))
            .with_max_request_body_bytes(options.maximum_request_body_bytes())
            .with_cancellation_token(shutdown.clone());
        let shared_handler = Arc::new(handler);
        let inner = StreamableHttpService::new(
            move || Ok(shared_handler.as_ref().clone()),
            Arc::new(NeverSessionManager::default()),
            config,
        );
        let maximum_concurrent_requests = options.maximum_concurrent_requests();
        Ok(Self {
            inner,
            options,
            authentication,
            admission,
            concurrency: Arc::new(Semaphore::new(maximum_concurrent_requests)),
            shutdown,
        })
    }

    /// Stops accepting new work and cancels active transport contexts.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Returns whether shutdown has begun.
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    async fn handle<B>(&self, mut request: Request<B>) -> McpHttpResponse
    where
        B: Body + Send + 'static,
        B::Error: fmt::Display,
        B::Data: Send + 'static,
    {
        if self.shutdown.is_cancelled() {
            return public_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Service unavailable",
                Some(1),
            );
        }

        let Ok(_permit) = self.concurrency.clone().try_acquire_owned() else {
            return public_error(StatusCode::TOO_MANY_REQUESTS, "Too many requests", Some(1));
        };

        let method_hint = bounded_header_hint(request.headers(), MCP_METHOD_HEADER);
        let name_hint = bounded_header_hint(request.headers(), MCP_NAME_HEADER);
        let operation = async {
            let principal = match &self.authentication.mode {
                McpServerAuthenticationMode::AnonymousLoopback => McpServerPrincipal::anonymous(),
                McpServerAuthenticationMode::Bearer {
                    authenticator,
                    challenge,
                } => {
                    let Ok(credential) = parse_bearer(request.headers()) else {
                        return unauthorized(challenge);
                    };
                    let authentication_request = McpServerAuthenticationRequest {
                        credential,
                        transport_method_hint: method_hint.clone(),
                        transport_name_hint: name_hint.clone(),
                    };
                    match authenticator.authenticate(authentication_request).await {
                        Ok(principal) => principal,
                        Err(McpServerAuthenticationError::InvalidCredential) => {
                            return unauthorized(challenge);
                        }
                        Err(McpServerAuthenticationError::Forbidden) => {
                            return forbidden(Some(challenge));
                        }
                        Err(McpServerAuthenticationError::Unavailable) => {
                            return public_error(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "Service unavailable",
                                Some(1),
                            );
                        }
                    }
                }
            };

            let admission_request = McpServerAdmissionRequest {
                principal: principal.clone(),
                transport_method_hint: method_hint,
                transport_name_hint: name_hint,
            };
            match self.admission.admit(admission_request).await {
                Ok(()) => {}
                Err(McpServerAdmissionError::Forbidden) => return forbidden(None),
                Err(McpServerAdmissionError::RateLimited { retry_after }) => {
                    return public_error(
                        StatusCode::TOO_MANY_REQUESTS,
                        "Too many requests",
                        Some(retry_after_seconds(retry_after)),
                    );
                }
                Err(McpServerAdmissionError::Unavailable) => {
                    return public_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Service unavailable",
                        Some(1),
                    );
                }
            }

            request.extensions_mut().insert(principal);
            self.inner.handle(request).await
        };

        match tokio::time::timeout(self.options.request_timeout(), operation).await {
            Ok(response) => response,
            Err(_) => public_error(StatusCode::GATEWAY_TIMEOUT, "Request timed out", None),
        }
    }
}

impl<B, H> Service<Request<B>> for McpServerHttpService<H>
where
    B: Body + Send + 'static,
    B::Error: fmt::Display,
    B::Data: Send + 'static,
    H: ServerHandler + Clone + Send + Sync + 'static,
{
    type Response = McpHttpResponse;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move { Ok(service.handle(request).await) })
    }
}

fn parse_bearer(headers: &http::HeaderMap) -> Result<McpServerBearerCredential, ()> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let value = values.next().ok_or(())?;
    if values.next().is_some() {
        return Err(());
    }
    let value = value.to_str().map_err(|_| ())?;
    let (scheme, credential) = value.split_once(' ').ok_or(())?;
    if !scheme.eq_ignore_ascii_case("Bearer") || credential.contains(' ') {
        return Err(());
    }
    McpServerBearerCredential::new(credential).map_err(|_| ())
}

fn bounded_header_hint(headers: &http::HeaderMap, name: &'static str) -> Option<Box<str>> {
    let value = headers.get(name)?.to_str().ok()?;
    if value.is_empty() || value.len() > MAX_HINT_BYTES || value.chars().any(char::is_control) {
        return None;
    }
    Some(value.to_owned().into_boxed_str())
}

fn retry_after_seconds(duration: Duration) -> u64 {
    duration.as_secs().clamp(1, 24 * 60 * 60)
}

fn unauthorized(challenge: &McpServerBearerChallenge) -> McpHttpResponse {
    let mut response = public_error(StatusCode::UNAUTHORIZED, "Unauthorized", None);
    response
        .headers_mut()
        .insert(WWW_AUTHENTICATE, challenge.value.clone());
    response
}

fn forbidden(challenge: Option<&McpServerBearerChallenge>) -> McpHttpResponse {
    let mut response = public_error(StatusCode::FORBIDDEN, "Forbidden", None);
    if let Some(challenge) = challenge {
        response
            .headers_mut()
            .insert(WWW_AUTHENTICATE, challenge.value.clone());
    }
    response
}

fn public_error(
    status: StatusCode,
    message: &'static str,
    retry_after: Option<u64>,
) -> McpHttpResponse {
    let mut response = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .header(CACHE_CONTROL, "no-store")
        .header(PRAGMA, "no-cache")
        .body(Full::new(Bytes::from_static(message.as_bytes())).boxed())
        .expect("static MCP HTTP response is valid");
    if let Some(seconds) = retry_after {
        response.headers_mut().insert(
            RETRY_AFTER,
            HeaderValue::from_str(&seconds.to_string()).expect("bounded seconds are header-safe"),
        );
    }
    response
}

/// Returns the authenticated principal injected by [`McpServerHttpService`].
///
/// A handler mounted without this service receives `None` and should fail
/// closed whenever its deployment requires authentication.
#[must_use]
pub fn mcp_server_principal(context: &RequestContext<RoleServer>) -> Option<&McpServerPrincipal> {
    context
        .extensions
        .get::<Parts>()
        .and_then(|parts| parts.extensions.get::<McpServerPrincipal>())
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicBool, Ordering};

    use http_body_util::BodyExt as _;
    use rmcp::{
        ErrorData,
        model::{DiscoverResult, Implementation, ServerCapabilities, ServerInfo},
    };
    use serde_json::json;

    use super::*;

    #[derive(Clone)]
    struct TestHandler {
        saw_principal: Arc<AtomicBool>,
    }

    impl ServerHandler for TestHandler {
        fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
            Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
        }

        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
                .with_server_info(Implementation::new("stateknot-test", "0.0.0"))
        }

        async fn discover(
            &self,
            context: RequestContext<RoleServer>,
        ) -> Result<DiscoverResult, ErrorData> {
            self.saw_principal.store(
                mcp_server_principal(&context).is_some_and(|value| value.subject() == "tenant-a"),
                Ordering::SeqCst,
            );
            Ok(DiscoverResult::from_server_info(
                vec![ProtocolVersion::V_2026_07_28],
                self.get_info(),
            ))
        }
    }

    #[derive(Clone, Copy)]
    struct StaticAuthenticator;

    impl McpServerAuthenticator for StaticAuthenticator {
        fn authenticate(
            &self,
            request: McpServerAuthenticationRequest,
        ) -> BoxFuture<'_, Result<McpServerPrincipal, McpServerAuthenticationError>> {
            let accepted = request.credential().expose_secret() == "test-token";
            Box::pin(async move {
                if accepted {
                    McpServerPrincipal::new("tenant-a", ["mcp:invoke"])
                        .map_err(|_| McpServerAuthenticationError::Unavailable)
                } else {
                    Err(McpServerAuthenticationError::InvalidCredential)
                }
            })
        }
    }

    fn test_request(authorization: Option<&str>) -> Request<Full<Bytes>> {
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientInfo": {
                        "name": "stateknot-test-client",
                        "version": "0.0.0"
                    },
                    "io.modelcontextprotocol/clientCapabilities": {}
                }
            }
        }))
        .unwrap();
        let mut builder = Request::builder()
            .method("POST")
            .uri("http://127.0.0.1:32123/mcp")
            .header("host", "127.0.0.1:32123")
            .header("origin", "http://127.0.0.1:32123")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "server/discover");
        if let Some(authorization) = authorization {
            builder = builder.header(AUTHORIZATION, authorization);
        }
        builder.body(Full::new(Bytes::from(body))).unwrap()
    }

    fn bearer_service(saw_principal: Arc<AtomicBool>) -> McpServerHttpService<TestHandler> {
        let challenge = McpServerBearerChallenge::new(
            "stateknot-test",
            Some("https://auth.example.test/.well-known/oauth-protected-resource"),
        )
        .unwrap();
        McpServerHttpService::new(
            TestHandler { saw_principal },
            McpServerHttpOptions::loopback(32123).unwrap(),
            McpServerAuthentication::bearer(StaticAuthenticator, challenge),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn bearer_authentication_is_fail_closed_and_injects_principal() {
        let observed = Arc::new(AtomicBool::new(false));
        let mut service = bearer_service(observed.clone());

        let missing = service.call(test_request(None)).await.unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        assert!(missing.headers().contains_key(WWW_AUTHENTICATE));
        assert!(!observed.load(Ordering::SeqCst));

        let invalid = service
            .call(test_request(Some("Bearer wrong-token")))
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::UNAUTHORIZED);
        assert!(!observed.load(Ordering::SeqCst));

        let accepted = service
            .call(test_request(Some("Bearer test-token")))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        assert!(observed.load(Ordering::SeqCst));
        let bytes = accepted.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(payload.pointer("/id"), Some(&json!(1)));
        assert_eq!(
            payload.pointer("/result/supportedVersions/0"),
            Some(&json!("2026-07-28"))
        );
    }

    #[test]
    fn deployment_policy_rejects_unsafe_configuration() {
        assert!(matches!(
            McpServerHttpOptions::new(
                std::iter::empty::<String>(),
                ["https://app.example.test"],
                1,
                1,
                Duration::from_secs(1),
            ),
            Err(McpServerHttpOptionsError::InvalidHostAllowlist)
        ));
        assert!(matches!(
            McpServerHttpOptions::new(
                ["*.example.test"],
                ["https://app.example.test"],
                1,
                1,
                Duration::from_secs(1),
            ),
            Err(McpServerHttpOptionsError::InvalidHostAllowlist)
        ));

        let public = McpServerHttpOptions::new(
            ["mcp.example.test"],
            ["https://app.example.test"],
            1024,
            8,
            Duration::from_secs(5),
        )
        .unwrap();
        let handler = TestHandler {
            saw_principal: Arc::new(AtomicBool::new(false)),
        };
        assert!(matches!(
            McpServerHttpService::new(
                handler,
                public,
                McpServerAuthentication::anonymous_loopback(),
            ),
            Err(McpServerHttpServiceBuildError::AnonymousNonLoopback)
        ));
    }

    #[test]
    fn credentials_and_principals_are_bounded_and_redacted() {
        let credential = McpServerBearerCredential::new("abc.def_123~+/=").unwrap();
        assert_eq!(credential.expose_secret(), "abc.def_123~+/=");
        assert!(!format!("{credential:?}").contains("abc.def"));
        assert!(matches!(
            McpServerBearerCredential::new("bad token"),
            Err(McpServerBearerCredentialError::InvalidSyntax)
        ));

        let principal = McpServerPrincipal::new("tenant-a", ["write", "read"]).unwrap();
        assert_eq!(principal.scopes().collect::<Vec<_>>(), ["read", "write"]);
        assert!(principal.has_scope("read"));
        assert!(matches!(
            McpServerPrincipal::new("tenant-a", ["read", "read"]),
            Err(McpServerPrincipalError::DuplicateScope)
        ));
    }
}
