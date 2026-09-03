// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Production HTTP boundary for the `StateKnot` A2A 1.0 server profile.
//!
//! The official SDK owns HTTP+JSON, JSON-RPC, `ProtoJSON`, and SSE wire details.
//! `StateKnot` owns authentication-before-parsing, authorization-before-lookup,
//! exact tenant identity, request and stream admission, bounded contracts,
//! canonical routes, version/extension negotiation, and graceful shutdown.

use std::{
    collections::HashSet,
    fmt,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};

use a2a as wire;
use a2a_server::{RequestHandler, ServiceParams};
use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Request, State},
    http::{
        HeaderMap, HeaderValue, Method, Response, StatusCode,
        header::{
            ACCESS_CONTROL_ALLOW_ORIGIN, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, ETAG, HOST,
            IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, RETRY_AFTER, VARY, WWW_AUTHENTICATE,
        },
    },
    middleware::{self, Next},
    response::IntoResponse,
    routing::get,
};
use futures_core::Stream;
use futures_util::StreamExt as _;
use http_body_util::{BodyExt as _, Limited};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};
use stateknot_core::{BoxFuture, PrincipalIdentity, TenantId};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    A2A_AGENT_CARD_PATH, A2A_PROTOCOL_VERSION_1_0, A2aAgentCard, A2aCancelTaskRequest,
    A2aContractError, A2aDeletePushConfigRequest, A2aGetPushConfigRequest, A2aGetTaskRequest,
    A2aListPushConfigsRequest, A2aListTasksRequest, A2aPushConfig, A2aPushConfigPage, A2aSecret,
    A2aSendMessageRequest, A2aSendMessageResponse, A2aStreamEvent, A2aSubscribeTaskRequest,
    A2aTask, A2aTaskPage,
};

const A2A_VERSION_HEADER: &str = "a2a-version";
const A2A_EXTENSIONS_HEADER: &str = "a2a-extensions";
const DEFAULT_REST_PREFIX: &str = "/a2a/rest";
const DEFAULT_JSONRPC_PATH: &str = "/a2a/jsonrpc";
const MAX_BEARER_BYTES: usize = 16 * 1024;
const MAX_EXTENSION_HEADER_BYTES: usize = 16 * 1024;
const MAX_NEGOTIATED_EXTENSIONS: usize = 64;

tokio::task_local! {
    static A2A_REQUEST_PRINCIPAL: A2aServerPrincipal;
}

/// An A2A protocol operation after transport decoding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum A2aServerOperation {
    /// Send a unary message.
    SendMessage,
    /// Send a message and stream committed updates.
    SendStreamingMessage,
    /// Read one task.
    GetTask,
    /// List caller-authorized tasks.
    ListTasks,
    /// Request cancellation.
    CancelTask,
    /// Subscribe to committed task updates.
    SubscribeToTask,
    /// Create a push configuration.
    CreatePushConfig,
    /// Read a push configuration.
    GetPushConfig,
    /// List push configurations.
    ListPushConfigs,
    /// Delete a push configuration.
    DeletePushConfig,
    /// Read the authenticated extended Agent Card.
    GetExtendedAgentCard,
}

/// Exact authenticated identity and tenant boundary for one A2A caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct A2aServerPrincipal {
    tenant: TenantId,
    identity: PrincipalIdentity,
    scopes: Arc<[Box<str>]>,
}

impl A2aServerPrincipal {
    /// Maximum number of accepted OAuth/policy scopes.
    pub const MAX_SCOPES: usize = 128;
    /// Maximum bytes in one scope token.
    pub const MAX_SCOPE_BYTES: usize = 256;

    /// Constructs a tenant-bound principal and canonicalizes exact scopes.
    pub fn new(
        tenant: TenantId,
        identity: PrincipalIdentity,
        scopes: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, A2aServerPrincipalError> {
        let mut scopes = scopes.into_iter().map(Into::into).collect::<Vec<_>>();
        if scopes.len() > Self::MAX_SCOPES {
            return Err(A2aServerPrincipalError::TooManyScopes);
        }
        for scope in &scopes {
            if scope.is_empty()
                || scope.len() > Self::MAX_SCOPE_BYTES
                || scope
                    .bytes()
                    .any(|byte| !matches!(byte, b'!' | b'#'..=b'[' | b']'..=b'~'))
            {
                return Err(A2aServerPrincipalError::InvalidScope);
            }
        }
        scopes.sort_unstable();
        if scopes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(A2aServerPrincipalError::DuplicateScope);
        }
        Ok(Self {
            tenant,
            identity,
            scopes: scopes
                .into_iter()
                .map(String::into_boxed_str)
                .collect::<Vec<_>>()
                .into(),
        })
    }

    /// Returns the exact storage tenant.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Returns the exact issuer/subject identity.
    #[must_use]
    pub const fn identity(&self) -> &PrincipalIdentity {
        &self.identity
    }

    /// Iterates canonical exact scopes.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = &str> {
        self.scopes.iter().map(AsRef::as_ref)
    }

    /// Returns whether the exact scope was granted.
    #[must_use]
    pub fn has_scope(&self, value: &str) -> bool {
        self.scopes
            .binary_search_by(|candidate| candidate.as_ref().cmp(value))
            .is_ok()
    }
}

/// Invalid principal scope set.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aServerPrincipalError {
    /// More than the hard scope ceiling was supplied.
    #[error("too many A2A principal scopes")]
    TooManyScopes,
    /// A scope is empty, oversized, or outside the OAuth scope-token grammar.
    #[error("invalid A2A principal scope")]
    InvalidScope,
    /// The same exact scope was supplied more than once.
    #[error("duplicate A2A principal scope")]
    DuplicateScope,
}

/// Authentication material parsed before any A2A request body is decoded.
pub struct A2aServerAuthenticationRequest {
    bearer: Option<A2aSecret>,
    method: Method,
    path: Box<str>,
}

impl A2aServerAuthenticationRequest {
    /// Returns the optional redacted bearer credential.
    #[must_use]
    pub const fn bearer(&self) -> Option<&A2aSecret> {
        self.bearer.as_ref()
    }

    /// Returns the HTTP method.
    #[must_use]
    pub const fn method(&self) -> &Method {
        &self.method
    }

    /// Returns the request path. It is a routing hint, not an authorization
    /// substitute for the decoded operation.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl fmt::Debug for A2aServerAuthenticationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("A2aServerAuthenticationRequest")
            .field("bearer", &self.bearer.as_ref().map(|_| "[REDACTED]"))
            .field("method", &self.method)
            .field("path", &self.path)
            .finish()
    }
}

/// Public-safe authentication failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aServerAuthenticationError {
    /// Credential was absent, invalid, expired, revoked, or used for the wrong
    /// issuer/audience/resource.
    #[error("A2A credential is invalid")]
    InvalidCredential,
    /// Credential is valid but cannot access this A2A service.
    #[error("A2A access is forbidden")]
    Forbidden,
    /// Authentication infrastructure is temporarily unavailable.
    #[error("A2A authentication is unavailable")]
    Unavailable,
}

/// Authenticates every non-discovery request.
pub trait A2aServerAuthenticator: Send + Sync + 'static {
    /// Resolves an exact tenant-bound principal. Implementations must validate
    /// issuer, audience, expiry, delegation, and resource binding.
    fn authenticate(
        &self,
        request: A2aServerAuthenticationRequest,
    ) -> BoxFuture<'_, Result<A2aServerPrincipal, A2aServerAuthenticationError>>;
}

/// Fine-grained authorization facts evaluated before task/config lookup.
#[derive(Clone, Debug)]
pub struct A2aServerAuthorizationRequest {
    principal: A2aServerPrincipal,
    operation: A2aServerOperation,
    task_id: Option<Box<str>>,
    config_id: Option<Box<str>>,
}

impl A2aServerAuthorizationRequest {
    /// Returns the authenticated principal.
    #[must_use]
    pub const fn principal(&self) -> &A2aServerPrincipal {
        &self.principal
    }

    /// Returns the decoded operation.
    #[must_use]
    pub const fn operation(&self) -> A2aServerOperation {
        self.operation
    }

    /// Returns an untrusted opaque task ID when the operation carries one.
    #[must_use]
    pub fn task_id(&self) -> Option<&str> {
        self.task_id.as_deref()
    }

    /// Returns an untrusted opaque push-config ID when present.
    #[must_use]
    pub fn config_id(&self) -> Option<&str> {
        self.config_id.as_deref()
    }
}

/// Public-safe authorization failure. Both variants intentionally map to an
/// existence-hiding response before any backend lookup.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aServerAuthorizationError {
    /// Principal lacks required authority.
    #[error("A2A operation is forbidden")]
    Forbidden,
    /// Policy infrastructure is temporarily unavailable.
    #[error("A2A authorization is unavailable")]
    Unavailable,
}

/// Authorizes each decoded operation before resource lookup or creation.
pub trait A2aServerAuthorizer: Send + Sync + 'static {
    /// Applies tenant, subject, scope, and resource-ID policy without revealing
    /// whether the named task/config exists.
    fn authorize(
        &self,
        request: A2aServerAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), A2aServerAuthorizationError>>;
}

/// Replica-wide admission facts for an already authenticated and authorized
/// operation.
#[derive(Clone, Debug)]
pub struct A2aServerAdmissionRequest {
    principal: A2aServerPrincipal,
    operation: A2aServerOperation,
}

impl A2aServerAdmissionRequest {
    /// Returns the caller.
    #[must_use]
    pub const fn principal(&self) -> &A2aServerPrincipal {
        &self.principal
    }

    /// Returns the decoded operation.
    #[must_use]
    pub const fn operation(&self) -> A2aServerOperation {
        self.operation
    }
}

/// Public-safe admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aServerAdmissionError {
    /// Caller or tenant is over a durable quota/rate boundary.
    #[error("A2A request is rate limited")]
    Rejected,
    /// Admission infrastructure is unavailable.
    #[error("A2A admission is unavailable")]
    Unavailable,
}

/// Applies replica-wide quotas after authorization and before backend work.
pub trait A2aServerAdmissionControl: Send + Sync + 'static {
    /// Admits or rejects one decoded operation.
    fn admit(
        &self,
        request: A2aServerAdmissionRequest,
    ) -> BoxFuture<'_, Result<(), A2aServerAdmissionError>>;
}

/// Explicit permissive authorizer for a deployment whose authenticator has
/// already established all required authority.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowA2aServerAuthorization;

impl A2aServerAuthorizer for AllowA2aServerAuthorization {
    fn authorize(
        &self,
        _request: A2aServerAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), A2aServerAuthorizationError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Explicit permissive admission control for tests or an externally enforced
/// replica-wide admission layer.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowA2aServerAdmission;

impl A2aServerAdmissionControl for AllowA2aServerAdmission {
    fn admit(
        &self,
        _request: A2aServerAdmissionRequest,
    ) -> BoxFuture<'_, Result<(), A2aServerAdmissionError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Capabilities actually implemented by a task backend.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct A2aTaskServiceCapabilities {
    /// Durable SSE send/subscription support.
    pub streaming: bool,
    /// Durable push config and at-least-once delivery support.
    pub push_notifications: bool,
    /// Authenticated extended Agent Card support.
    pub extended_agent_card: bool,
}

/// Negotiated, authenticated context passed to the task backend.
#[derive(Clone, Debug)]
pub struct A2aRequestContext {
    principal: A2aServerPrincipal,
    extensions: Arc<[Box<str>]>,
}

impl A2aRequestContext {
    /// Returns the exact tenant-bound caller.
    #[must_use]
    pub const fn principal(&self) -> &A2aServerPrincipal {
        &self.principal
    }

    /// Iterates extensions both declared by the server and requested by the
    /// client. Unknown extension URIs are not activated.
    pub fn negotiated_extensions(&self) -> impl ExactSizeIterator<Item = &str> {
        self.extensions.iter().map(AsRef::as_ref)
    }
}

/// Public-safe application/projection failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aTaskServiceError {
    /// Authorized task is absent.
    #[error("A2A task was not found")]
    TaskNotFound,
    /// Current terminal or policy state does not allow cancellation.
    #[error("A2A task cannot be canceled")]
    TaskNotCancelable,
    /// Push notifications are not supported by this service profile.
    #[error("A2A push notifications are not supported")]
    PushNotificationsNotSupported,
    /// Operation is not supported by this exact server profile.
    #[error("A2A operation is not supported")]
    UnsupportedOperation,
    /// Requested input/output content mode is not supported.
    #[error("A2A content type is not supported")]
    ContentTypeNotSupported,
    /// A committed internal projection cannot form a valid A2A response.
    #[error("A2A agent response is invalid")]
    InvalidAgentResponse,
    /// Extended Agent Card was advertised incorrectly or is not configured.
    #[error("A2A extended Agent Card is not configured")]
    ExtendedCardNotConfigured,
    /// Required extension negotiation is incomplete.
    #[error("required A2A extension was not negotiated")]
    ExtensionSupportRequired,
    /// Caller input is semantically invalid after wire decoding.
    #[error("A2A request is invalid")]
    InvalidRequest,
    /// Durable storage, scheduling, or policy infrastructure is unavailable.
    #[error("A2A service is unavailable")]
    Unavailable,
}

/// A send/subscription stream. Production implementations must source this
/// from committed durable events, not a process-local broadcast channel.
pub type A2aEventStream =
    Pin<Box<dyn Stream<Item = Result<A2aStreamEvent, A2aTaskServiceError>> + Send + 'static>>;

/// Tenant-aware durable A2A task backend.
///
/// The contract intentionally contains no in-memory implementation. A
/// production backend must preserve message idempotency, task/context
/// projection, ordered durable streams, authorization scoping, cancellation
/// races, encrypted push secrets, and transactional at-least-once outbox work.
pub trait A2aTaskService: Send + Sync + 'static {
    /// Returns capabilities actually backed by durable implementations.
    fn capabilities(&self) -> A2aTaskServiceCapabilities;

    /// Durably accepts or continues a message.
    fn send_message(
        &self,
        context: A2aRequestContext,
        request: A2aSendMessageRequest,
    ) -> BoxFuture<'_, Result<A2aSendMessageResponse, A2aTaskServiceError>>;

    /// Durably accepts a message and streams committed projections.
    fn send_streaming_message(
        &self,
        context: A2aRequestContext,
        request: A2aSendMessageRequest,
    ) -> BoxFuture<'_, Result<A2aEventStream, A2aTaskServiceError>>;

    /// Loads one caller-authorized task projection.
    fn get_task(
        &self,
        context: A2aRequestContext,
        request: A2aGetTaskRequest,
    ) -> BoxFuture<'_, Result<A2aTask, A2aTaskServiceError>>;

    /// Lists one stable-snapshot page scoped to the caller.
    fn list_tasks(
        &self,
        context: A2aRequestContext,
        request: A2aListTasksRequest,
    ) -> BoxFuture<'_, Result<A2aTaskPage, A2aTaskServiceError>>;

    /// Records a cancellation request and projects its current state.
    fn cancel_task(
        &self,
        context: A2aRequestContext,
        request: A2aCancelTaskRequest,
    ) -> BoxFuture<'_, Result<A2aTask, A2aTaskServiceError>>;

    /// Streams a snapshot followed by newly committed task events.
    fn subscribe_to_task(
        &self,
        context: A2aRequestContext,
        request: A2aSubscribeTaskRequest,
    ) -> BoxFuture<'_, Result<A2aEventStream, A2aTaskServiceError>>;

    /// Encrypts and durably registers a push configuration.
    fn create_push_config(
        &self,
        context: A2aRequestContext,
        config: A2aPushConfig,
    ) -> BoxFuture<'_, Result<A2aPushConfig, A2aTaskServiceError>>;

    /// Loads one authorized push configuration.
    fn get_push_config(
        &self,
        context: A2aRequestContext,
        request: A2aGetPushConfigRequest,
    ) -> BoxFuture<'_, Result<A2aPushConfig, A2aTaskServiceError>>;

    /// Lists one stable-snapshot push-config page.
    fn list_push_configs(
        &self,
        context: A2aRequestContext,
        request: A2aListPushConfigsRequest,
    ) -> BoxFuture<'_, Result<A2aPushConfigPage, A2aTaskServiceError>>;

    /// Durably tombstones a push configuration.
    fn delete_push_config(
        &self,
        context: A2aRequestContext,
        request: A2aDeletePushConfigRequest,
    ) -> BoxFuture<'_, Result<(), A2aTaskServiceError>>;

    /// Produces an authenticated, caller-scoped extended Agent Card.
    fn get_extended_agent_card(
        &self,
        context: A2aRequestContext,
    ) -> BoxFuture<'_, Result<A2aAgentCard, A2aTaskServiceError>>;
}

/// Production HTTP boundary options.
#[derive(Clone, Debug)]
pub struct A2aServerHttpOptions {
    rest_prefix: Box<str>,
    jsonrpc_path: Box<str>,
    allowed_authorities: Arc<[Box<str>]>,
    allowed_origins: Arc<[Box<str>]>,
    maximum_body_bytes: usize,
    maximum_response_body_bytes: usize,
    maximum_in_flight_requests: usize,
    maximum_in_flight_operations: usize,
    request_timeout: Duration,
    stream_idle_timeout: Duration,
    maximum_stream_events: u32,
    card_max_age: Duration,
    bearer_challenge: Option<Box<str>>,
}

impl A2aServerHttpOptions {
    /// Constructs strict defaults. At least one exact public authority must be
    /// added before build.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rest_prefix: DEFAULT_REST_PREFIX.into(),
            jsonrpc_path: DEFAULT_JSONRPC_PATH.into(),
            allowed_authorities: Arc::from([]),
            allowed_origins: Arc::from([]),
            maximum_body_bytes: 1024 * 1024,
            maximum_response_body_bytes: 8 * 1024 * 1024,
            maximum_in_flight_requests: 1024,
            maximum_in_flight_operations: 512,
            request_timeout: Duration::from_secs(30),
            stream_idle_timeout: Duration::from_secs(60),
            maximum_stream_events: 16_384,
            card_max_age: Duration::from_secs(300),
            bearer_challenge: None,
        }
    }

    /// Sets canonical REST and JSON-RPC mount points.
    pub fn with_paths(
        mut self,
        rest_prefix: impl Into<String>,
        jsonrpc_path: impl Into<String>,
    ) -> Result<Self, A2aServerHttpOptionsError> {
        self.rest_prefix = validate_mount_path(rest_prefix.into())?;
        self.jsonrpc_path = validate_mount_path(jsonrpc_path.into())?;
        if self.rest_prefix == self.jsonrpc_path {
            return Err(A2aServerHttpOptionsError::DuplicatePaths);
        }
        Ok(self)
    }

    /// Sets exact allowed `Host` authorities, including ports when externally
    /// visible. Wildcards and forwarded headers are intentionally unsupported.
    pub fn with_allowed_authorities(
        mut self,
        authorities: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, A2aServerHttpOptionsError> {
        let mut values = authorities.into_iter().map(Into::into).collect::<Vec<_>>();
        if values.is_empty() || values.len() > 64 {
            return Err(A2aServerHttpOptionsError::InvalidAuthorities);
        }
        for value in &mut values {
            if value.trim() != value
                || value.is_empty()
                || value.len() > 512
                || value.contains('/')
                || value.contains('@')
            {
                return Err(A2aServerHttpOptionsError::InvalidAuthorities);
            }
            value.make_ascii_lowercase();
        }
        values.sort_unstable();
        values.dedup();
        self.allowed_authorities = values
            .into_iter()
            .map(String::into_boxed_str)
            .collect::<Vec<_>>()
            .into();
        Ok(self)
    }

    /// Sets exact browser origins. Requests without `Origin` remain valid for
    /// server-to-server A2A. An empty list rejects every supplied Origin.
    pub fn with_allowed_origins(
        mut self,
        origins: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, A2aServerHttpOptionsError> {
        let mut values = origins.into_iter().map(Into::into).collect::<Vec<_>>();
        if values.len() > 64 {
            return Err(A2aServerHttpOptionsError::InvalidOrigins);
        }
        for value in &values {
            let url = reqwest::Url::parse(value)
                .map_err(|_| A2aServerHttpOptionsError::InvalidOrigins)?;
            if url.host_str().is_none()
                || url.path() != "/"
                || url.query().is_some()
                || url.fragment().is_some()
                || url.username() != ""
                || url.password().is_some()
            {
                return Err(A2aServerHttpOptionsError::InvalidOrigins);
            }
        }
        values.sort_unstable();
        values.dedup();
        self.allowed_origins = values
            .into_iter()
            .map(String::into_boxed_str)
            .collect::<Vec<_>>()
            .into();
        Ok(self)
    }

    /// Sets hard body and concurrency ceilings.
    pub fn with_limits(
        mut self,
        maximum_body_bytes: usize,
        maximum_in_flight_requests: usize,
        maximum_in_flight_operations: usize,
    ) -> Result<Self, A2aServerHttpOptionsError> {
        if !(1024..=16 * 1024 * 1024).contains(&maximum_body_bytes)
            || maximum_in_flight_requests == 0
            || maximum_in_flight_requests > 65_536
            || maximum_in_flight_operations == 0
            || maximum_in_flight_operations > maximum_in_flight_requests
        {
            return Err(A2aServerHttpOptionsError::InvalidLimits);
        }
        self.maximum_body_bytes = maximum_body_bytes;
        self.maximum_in_flight_requests = maximum_in_flight_requests;
        self.maximum_in_flight_operations = maximum_in_flight_operations;
        Ok(self)
    }

    /// Sets the hard ceiling for a serialized unary protocol response.
    /// Streaming responses are bounded separately by event count and each
    /// event's contract limits.
    pub fn with_maximum_response_body_bytes(
        mut self,
        value: usize,
    ) -> Result<Self, A2aServerHttpOptionsError> {
        if !(1024..=64 * 1024 * 1024).contains(&value) {
            return Err(A2aServerHttpOptionsError::InvalidLimits);
        }
        self.maximum_response_body_bytes = value;
        Ok(self)
    }

    /// Sets unary/authentication and stream-idle deadlines.
    pub fn with_timeouts(
        mut self,
        request_timeout: Duration,
        stream_idle_timeout: Duration,
    ) -> Result<Self, A2aServerHttpOptionsError> {
        if request_timeout < Duration::from_millis(10)
            || request_timeout > Duration::from_secs(300)
            || stream_idle_timeout < Duration::from_secs(1)
            || stream_idle_timeout > Duration::from_secs(3600)
        {
            return Err(A2aServerHttpOptionsError::InvalidTimeouts);
        }
        self.request_timeout = request_timeout;
        self.stream_idle_timeout = stream_idle_timeout;
        Ok(self)
    }

    /// Sets the event ceiling for one stream/subscription response.
    pub fn with_maximum_stream_events(
        mut self,
        value: u32,
    ) -> Result<Self, A2aServerHttpOptionsError> {
        if value == 0 || value > 1_000_000 {
            return Err(A2aServerHttpOptionsError::InvalidLimits);
        }
        self.maximum_stream_events = value;
        Ok(self)
    }

    /// Sets public Agent Card cache duration.
    pub fn with_card_max_age(mut self, value: Duration) -> Result<Self, A2aServerHttpOptionsError> {
        if value > Duration::from_secs(86_400) {
            return Err(A2aServerHttpOptionsError::InvalidCardCache);
        }
        self.card_max_age = value;
        Ok(self)
    }

    /// Configures the RFC 6750 challenge emitted for invalid/missing bearer
    /// credentials, for example `Bearer realm="stateknot"`.
    pub fn with_bearer_challenge(
        mut self,
        value: impl Into<String>,
    ) -> Result<Self, A2aServerHttpOptionsError> {
        let value = value.into();
        let header = HeaderValue::from_str(&value)
            .map_err(|_| A2aServerHttpOptionsError::InvalidBearerChallenge)?;
        if !value.starts_with("Bearer ") || header.as_bytes().len() > 1024 {
            return Err(A2aServerHttpOptionsError::InvalidBearerChallenge);
        }
        self.bearer_challenge = Some(value.into_boxed_str());
        Ok(self)
    }
}

impl Default for A2aServerHttpOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Invalid production HTTP boundary options.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum A2aServerHttpOptionsError {
    /// A mount path is not absolute, canonical, or bounded.
    #[error("invalid A2A mount path")]
    InvalidPath,
    /// REST and JSON-RPC use the same mount path.
    #[error("A2A REST and JSON-RPC paths must differ")]
    DuplicatePaths,
    /// Exact Host allowlist is absent or invalid.
    #[error("invalid A2A Host authority allowlist")]
    InvalidAuthorities,
    /// Browser Origin allowlist is invalid.
    #[error("invalid A2A Origin allowlist")]
    InvalidOrigins,
    /// Body, concurrency, or event limits are invalid.
    #[error("invalid A2A HTTP limits")]
    InvalidLimits,
    /// Timeouts are outside supported production bounds.
    #[error("invalid A2A timeouts")]
    InvalidTimeouts,
    /// Agent Card cache lifetime is invalid.
    #[error("invalid Agent Card cache lifetime")]
    InvalidCardCache,
    /// Bearer challenge is malformed.
    #[error("invalid A2A Bearer challenge")]
    InvalidBearerChallenge,
}

fn validate_mount_path(value: String) -> Result<Box<str>, A2aServerHttpOptionsError> {
    if value.len() < 2
        || value.len() > 256
        || !value.starts_with('/')
        || value.ends_with('/')
        || value.contains("//")
        || value.contains('?')
        || value.contains('#')
        || value
            .split('/')
            .any(|segment| matches!(segment, "." | ".."))
    {
        return Err(A2aServerHttpOptionsError::InvalidPath);
    }
    Ok(value.into_boxed_str())
}

/// Application assembly failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum A2aServerBuildError {
    /// HTTP options are incomplete.
    #[error("A2A server requires at least one exact allowed Host authority")]
    MissingAllowedAuthority,
    /// Agent Card capability flags differ from the durable backend.
    #[error("A2A Agent Card capabilities do not match the task service")]
    CapabilityMismatch,
    /// Agent Card does not advertise both mounted bindings at their configured
    /// public paths.
    #[error("A2A Agent Card interfaces do not match mounted bindings")]
    InterfaceMismatch,
    /// Agent Card serialization failed.
    #[error("A2A Agent Card serialization failed")]
    InvalidAgentCard(#[source] A2aContractError),
}

/// Fully assembled A2A HTTP application.
pub struct A2aServer {
    router: Router,
    shutdown: CancellationToken,
}

impl A2aServer {
    /// Builds the production boundary around a durable task service.
    pub fn new(
        card: A2aAgentCard,
        authenticator: impl A2aServerAuthenticator,
        authorizer: impl A2aServerAuthorizer,
        admission: impl A2aServerAdmissionControl,
        service: impl A2aTaskService,
        options: A2aServerHttpOptions,
        shutdown: CancellationToken,
    ) -> Result<Self, A2aServerBuildError> {
        if options.allowed_authorities.is_empty() {
            return Err(A2aServerBuildError::MissingAllowedAuthority);
        }
        let card_capabilities = card.capabilities();
        let service_capabilities = service.capabilities();
        if card_capabilities.supports_streaming() != service_capabilities.streaming
            || card_capabilities.supports_push_notifications()
                != service_capabilities.push_notifications
            || card_capabilities.supports_extended_agent_card()
                != service_capabilities.extended_agent_card
        {
            return Err(A2aServerBuildError::CapabilityMismatch);
        }
        validate_advertised_interfaces(&card, &options)?;

        let card_json = card
            .to_json()
            .map_err(A2aServerBuildError::InvalidAgentCard)?;
        let card_bytes = serde_json::to_vec(&card_json).map_err(|_| {
            A2aServerBuildError::InvalidAgentCard(A2aContractError::InvalidJson {
                field: "Agent Card",
            })
        })?;
        let card_state = Arc::new(AgentCardState::new(
            card_json,
            &card_bytes,
            options.card_max_age,
        ));
        let task_handler = Arc::new(A2aSdkRequestHandler {
            card,
            authorizer: Arc::new(authorizer),
            admission: Arc::new(admission),
            service: Arc::new(service),
            operation_slots: Arc::new(Semaphore::new(options.maximum_in_flight_operations)),
            request_timeout: options.request_timeout,
            stream_idle_timeout: options.stream_idle_timeout,
            maximum_stream_events: options.maximum_stream_events,
            shutdown: shutdown.clone(),
        });

        let rest = a2a_server::rest::rest_router(Arc::clone(&task_handler));
        let jsonrpc = a2a_server::jsonrpc::jsonrpc_router(task_handler);
        let router = Router::new()
            .route(A2A_AGENT_CARD_PATH, get(serve_agent_card))
            .with_state(card_state)
            .nest(options.rest_prefix.as_ref(), rest)
            // `nest_service` preserves both the exact endpoint and its slash
            // form without a redirect. Generic base-URL clients commonly add
            // the slash when resolving `/`, and a direct route avoids replay
            // and transient proxy failures for authenticated POST requests.
            .nest_service(options.jsonrpc_path.as_ref(), jsonrpc)
            .layer(DefaultBodyLimit::max(options.maximum_body_bytes))
            .layer(middleware::from_fn_with_state(
                Arc::new(A2aBoundaryState {
                    authenticator: Arc::new(authenticator),
                    request_slots: Arc::new(Semaphore::new(options.maximum_in_flight_requests)),
                    options,
                    shutdown: shutdown.clone(),
                }),
                enforce_boundary,
            ));

        Ok(Self { router, shutdown })
    }

    /// Returns a cloneable Axum router for mounting or direct serving.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Starts cooperative shutdown. Existing unary calls and streams observe
    /// the same token and stop at their next await boundary.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

fn validate_advertised_interfaces(
    card: &A2aAgentCard,
    options: &A2aServerHttpOptions,
) -> Result<(), A2aServerBuildError> {
    let mut rest = false;
    let mut jsonrpc = false;
    for interface in card.interfaces() {
        let Ok(url) = reqwest::Url::parse(interface.url()) else {
            return Err(A2aServerBuildError::InterfaceMismatch);
        };
        match interface.binding() {
            crate::A2aBinding::HttpJson if url.path() == options.rest_prefix.as_ref() => {
                rest = true;
            }
            crate::A2aBinding::JsonRpc if url.path() == options.jsonrpc_path.as_ref() => {
                jsonrpc = true;
            }
            _ => return Err(A2aServerBuildError::InterfaceMismatch),
        }
    }
    if rest && jsonrpc {
        Ok(())
    } else {
        Err(A2aServerBuildError::InterfaceMismatch)
    }
}

struct AgentCardState {
    value: Json<serde_json::Value>,
    etag: HeaderValue,
    last_modified: HeaderValue,
    modified_at: SystemTime,
    cache_control: HeaderValue,
}

impl AgentCardState {
    fn new(value: serde_json::Value, bytes: &[u8], max_age: Duration) -> Self {
        let hash = Sha256::digest(bytes);
        let etag = format!("\"{}\"", hex_digest(&hash));
        // HTTP dates have one-second precision. Store the same normalized
        // instant that is emitted in Last-Modified so a round-tripped
        // If-Modified-Since value compares equal instead of losing nanos.
        let modified_at = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(SystemTime::UNIX_EPOCH, |elapsed| {
                SystemTime::UNIX_EPOCH + Duration::from_secs(elapsed.as_secs())
            });
        Self {
            value: Json(value),
            etag: HeaderValue::from_str(&etag).expect("SHA-256 ETag is a valid header"),
            last_modified: HeaderValue::from_str(&httpdate::fmt_http_date(modified_at))
                .expect("HTTP date is a valid header"),
            modified_at,
            cache_control: HeaderValue::from_str(&format!(
                "public, max-age={}, must-revalidate",
                max_age.as_secs()
            ))
            .expect("bounded max-age is a valid header"),
        }
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}

async fn serve_agent_card(
    State(state): State<Arc<AgentCardState>>,
    headers: HeaderMap,
) -> Response<Body> {
    let not_modified = headers
        .get(IF_NONE_MATCH)
        .is_some_and(|value| value == state.etag)
        || headers
            .get(IF_MODIFIED_SINCE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| httpdate::parse_http_date(value).ok())
            .is_some_and(|value| value >= state.modified_at);

    let mut response = if not_modified {
        StatusCode::NOT_MODIFIED.into_response()
    } else {
        state.value.clone().into_response()
    };
    response.headers_mut().insert(ETAG, state.etag.clone());
    response
        .headers_mut()
        .insert(LAST_MODIFIED, state.last_modified.clone());
    response
        .headers_mut()
        .insert(CACHE_CONTROL, state.cache_control.clone());
    response
}

struct A2aBoundaryState {
    authenticator: Arc<dyn A2aServerAuthenticator>,
    request_slots: Arc<Semaphore>,
    options: A2aServerHttpOptions,
    shutdown: CancellationToken,
}

async fn enforce_boundary(
    State(state): State<Arc<A2aBoundaryState>>,
    request: Request,
    next: Next,
) -> Response<Body> {
    if state.shutdown.is_cancelled() {
        return plain_response(StatusCode::SERVICE_UNAVAILABLE, "service unavailable");
    }
    if !allowed_host(request.headers(), &state.options.allowed_authorities) {
        return plain_response(StatusCode::MISDIRECTED_REQUEST, "misdirected request");
    }
    let Ok(origin) = allowed_origin(request.headers(), &state.options.allowed_origins) else {
        return plain_response(StatusCode::FORBIDDEN, "origin forbidden");
    };
    if !canonical_path(request.uri().path(), &state.options) {
        return plain_response(StatusCode::NOT_FOUND, "not found");
    }
    if request.uri().path() != A2A_AGENT_CARD_PATH && !valid_content_type(&request) {
        if request
            .uri()
            .path()
            .starts_with(state.options.rest_prefix.as_ref())
        {
            return rest_protocol_error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "INVALID_ARGUMENT",
                "incompatible content types",
                "CONTENT_TYPE_NOT_SUPPORTED",
            );
        }
        return plain_response(StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported media type");
    }

    let Ok(permit) = Arc::clone(&state.request_slots).try_acquire_owned() else {
        let mut response = plain_response(StatusCode::TOO_MANY_REQUESTS, "overloaded");
        response
            .headers_mut()
            .insert(RETRY_AFTER, HeaderValue::from_static("1"));
        return response;
    };

    if request.uri().path() == A2A_AGENT_CARD_PATH {
        let mut response = next.run(request).await;
        drop(permit);
        apply_origin_header(&mut response, origin);
        return response;
    }

    let Ok(authentication) = authentication_request(&request) else {
        return authentication_response(
            A2aServerAuthenticationError::InvalidCredential,
            state.options.bearer_challenge.as_deref(),
        );
    };
    let principal = match tokio::time::timeout(
        state.options.request_timeout,
        state.authenticator.authenticate(authentication),
    )
    .await
    {
        Ok(Ok(principal)) => principal,
        Ok(Err(error)) => {
            return authentication_response(error, state.options.bearer_challenge.as_deref());
        }
        Err(_) => {
            return authentication_response(
                A2aServerAuthenticationError::Unavailable,
                state.options.bearer_challenge.as_deref(),
            );
        }
    };

    let is_rest = request
        .uri()
        .path()
        .starts_with(state.options.rest_prefix.as_ref());
    let request = match canonicalize_request_payload(
        request,
        state.options.rest_prefix.as_ref(),
        state.options.jsonrpc_path.as_ref(),
        state.options.maximum_body_bytes,
    )
    .await
    {
        Ok(request) => request,
        Err(response) => return response,
    };
    let mut response = A2A_REQUEST_PRINCIPAL
        .scope(principal, next.run(request))
        .await;
    drop(permit);
    response =
        normalize_protocol_response(response, is_rest, state.options.maximum_response_body_bytes)
            .await;
    apply_origin_header(&mut response, origin);
    response
}

async fn canonicalize_request_payload(
    request: Request,
    rest_prefix: &str,
    jsonrpc_path: &str,
    maximum_bytes: usize,
) -> Result<Request, Response<Body>> {
    if request.method() != Method::POST {
        return Ok(request);
    }
    let path = request.uri().path();
    let rest_send = matches!(
        path.strip_prefix(rest_prefix),
        Some("/message:send" | "/message:stream")
    );
    let jsonrpc = path == jsonrpc_path
        || path
            .strip_suffix('/')
            .is_some_and(|value| value == jsonrpc_path);
    if !rest_send && !jsonrpc {
        return Ok(request);
    }

    let (mut parts, body) = request.into_parts();
    let collected = Limited::new(body, maximum_bytes)
        .collect()
        .await
        .map_err(|_| {
            if rest_send {
                rest_protocol_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "RESOURCE_EXHAUSTED",
                    "A2A request exceeds the configured boundary",
                    "INVALID_REQUEST",
                )
            } else {
                plain_response(StatusCode::PAYLOAD_TOO_LARGE, "request body is too large")
            }
        })?;
    let bytes = collected.to_bytes();
    let normalized = if jsonrpc {
        canonicalize_jsonrpc_request(&bytes).map_err(|error| match error {
            JsonRpcPayloadError::Parse => jsonrpc_protocol_error(-32700, "Parse error"),
            JsonRpcPayloadError::InvalidRequest => {
                jsonrpc_protocol_error(-32600, "Invalid Request")
            }
        })?
    } else {
        canonicalize_native::<wire::SendMessageRequest>(&bytes).unwrap_or_else(|| bytes.to_vec())
    };
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Ok(Request::from_parts(parts, Body::from(normalized)))
}

#[derive(Clone, Copy)]
enum JsonRpcPayloadError {
    Parse,
    InvalidRequest,
}

fn canonicalize_jsonrpc_request(bytes: &[u8]) -> Result<Vec<u8>, JsonRpcPayloadError> {
    let value = serde_json::from_slice::<serde_json::Value>(bytes)
        .map_err(|_| JsonRpcPayloadError::Parse)?;
    let mut request = serde_json::from_value::<wire::JsonRpcRequest>(value)
        .map_err(|_| JsonRpcPayloadError::InvalidRequest)?;
    if let Some(params) = request.params.take() {
        request.params = Some(match request.method.as_str() {
            wire::methods::SEND_MESSAGE | wire::methods::SEND_STREAMING_MESSAGE => {
                canonicalize_value::<wire::SendMessageRequest>(params)
            }
            wire::methods::GET_TASK => canonicalize_value::<wire::GetTaskRequest>(params),
            wire::methods::LIST_TASKS => canonicalize_value::<wire::ListTasksRequest>(params),
            wire::methods::CANCEL_TASK => canonicalize_value::<wire::CancelTaskRequest>(params),
            wire::methods::SUBSCRIBE_TO_TASK => {
                canonicalize_value::<wire::SubscribeToTaskRequest>(params)
            }
            wire::methods::CREATE_PUSH_CONFIG => {
                canonicalize_value::<wire::TaskPushNotificationConfig>(params)
            }
            wire::methods::GET_PUSH_CONFIG => {
                canonicalize_value::<wire::GetTaskPushNotificationConfigRequest>(params)
            }
            wire::methods::LIST_PUSH_CONFIGS => {
                canonicalize_value::<wire::ListTaskPushNotificationConfigsRequest>(params)
            }
            wire::methods::DELETE_PUSH_CONFIG => {
                canonicalize_value::<wire::DeleteTaskPushNotificationConfigRequest>(params)
            }
            wire::methods::GET_EXTENDED_AGENT_CARD => {
                canonicalize_value::<wire::GetExtendedAgentCardRequest>(params)
            }
            _ => params,
        });
    }
    serde_json::to_vec(&request).map_err(|_| JsonRpcPayloadError::InvalidRequest)
}

fn canonicalize_native<T>(bytes: &[u8]) -> Option<Vec<u8>>
where
    T: DeserializeOwned + Serialize,
{
    let value = serde_json::from_slice::<T>(bytes).ok()?;
    serde_json::to_vec(&value).ok()
}

fn canonicalize_value<T>(value: serde_json::Value) -> serde_json::Value
where
    T: DeserializeOwned + Serialize,
{
    serde_json::from_value::<T>(value.clone())
        .and_then(serde_json::to_value)
        .unwrap_or(value)
}

async fn normalize_protocol_response(
    response: Response<Body>,
    is_rest: bool,
    maximum_bytes: usize,
) -> Response<Body> {
    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("application/json"));
    if !is_json {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let Ok(collected) = Limited::new(body, maximum_bytes).collect().await else {
        return rest_protocol_error(
            StatusCode::BAD_GATEWAY,
            "INTERNAL",
            "A2A response exceeds the configured boundary",
            "INVALID_AGENT_RESPONSE",
        );
    };
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(&collected.to_bytes()) else {
        return rest_protocol_error(
            StatusCode::BAD_GATEWAY,
            "INTERNAL",
            "A2A response is not valid JSON",
            "INVALID_AGENT_RESPONSE",
        );
    };

    normalize_utc_timestamps(&mut value);
    if is_rest {
        normalize_rest_error_status(&mut parts.status, &mut value);
    }
    let Ok(bytes) = serde_json::to_vec(&value) else {
        return rest_protocol_error(
            StatusCode::BAD_GATEWAY,
            "INTERNAL",
            "A2A response could not be serialized",
            "INVALID_AGENT_RESPONSE",
        );
    };
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(bytes))
}

fn normalize_utc_timestamps(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(
                    name.as_str(),
                    "timestamp" | "createdAt" | "updatedAt" | "statusTimestamp"
                ) {
                    if let serde_json::Value::String(timestamp) = value {
                        if let Some(prefix) = timestamp.strip_suffix("+00:00") {
                            *timestamp = format!("{prefix}Z");
                        }
                    }
                } else {
                    normalize_utc_timestamps(value);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_utc_timestamps(value);
            }
        }
        _ => {}
    }
}

fn normalize_rest_error_status(status: &mut StatusCode, value: &mut serde_json::Value) {
    let Some(error) = value
        .get_mut("error")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    let reason = error
        .get("details")
        .and_then(serde_json::Value::as_array)
        .and_then(|details| {
            details
                .iter()
                .find_map(|detail| detail.get("reason").and_then(serde_json::Value::as_str))
        });
    let Some((http_status, canonical_status)) = reason.and_then(rest_error_mapping) else {
        return;
    };
    *status = http_status;
    error.insert(
        "code".to_string(),
        serde_json::Value::from(http_status.as_u16()),
    );
    error.insert(
        "status".to_string(),
        serde_json::Value::from(canonical_status),
    );
}

fn rest_error_mapping(reason: &str) -> Option<(StatusCode, &'static str)> {
    match reason {
        "TASK_NOT_FOUND" | "METHOD_NOT_FOUND" => Some((StatusCode::NOT_FOUND, "NOT_FOUND")),
        "TASK_NOT_CANCELABLE" => Some((StatusCode::CONFLICT, "FAILED_PRECONDITION")),
        "PUSH_NOTIFICATION_NOT_SUPPORTED" | "UNSUPPORTED_OPERATION" | "VERSION_NOT_SUPPORTED" => {
            Some((StatusCode::BAD_REQUEST, "UNIMPLEMENTED"))
        }
        "CONTENT_TYPE_NOT_SUPPORTED" => {
            Some((StatusCode::UNSUPPORTED_MEDIA_TYPE, "INVALID_ARGUMENT"))
        }
        "INVALID_AGENT_RESPONSE" => Some((StatusCode::BAD_GATEWAY, "INTERNAL")),
        "EXTENDED_AGENT_CARD_NOT_CONFIGURED" | "EXTENSION_SUPPORT_REQUIRED" => {
            Some((StatusCode::BAD_REQUEST, "FAILED_PRECONDITION"))
        }
        "INVALID_REQUEST" | "INVALID_PARAMS" | "PARSE_ERROR" => {
            Some((StatusCode::BAD_REQUEST, "INVALID_ARGUMENT"))
        }
        "INTERNAL_ERROR" => Some((StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL")),
        _ => None,
    }
}

fn rest_protocol_error(
    status: StatusCode,
    canonical_status: &'static str,
    message: &'static str,
    reason: &'static str,
) -> Response<Body> {
    (
        status,
        Json(serde_json::json!({
            "error": {
                "code": status.as_u16(),
                "status": canonical_status,
                "message": message,
                "details": [{
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": reason,
                    "domain": "a2a-protocol.org",
                    "metadata": {}
                }]
            }
        })),
    )
        .into_response()
}

fn jsonrpc_protocol_error(code: i32, message: &'static str) -> Response<Body> {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": {
                "code": code,
                "message": message
            }
        })),
    )
        .into_response()
}

fn authentication_request(request: &Request) -> Result<A2aServerAuthenticationRequest, ()> {
    let values = request.headers().get_all(AUTHORIZATION);
    let mut iter = values.iter();
    let first = iter.next();
    if iter.next().is_some() {
        return Err(());
    }
    let bearer = first
        .map(|value| {
            let value = value.to_str().map_err(|_| ())?;
            let credential = value.strip_prefix("Bearer ").ok_or(())?;
            if credential.is_empty() || credential.len() > MAX_BEARER_BYTES {
                return Err(());
            }
            A2aSecret::new(credential.to_string()).map_err(|_| ())
        })
        .transpose()?;
    Ok(A2aServerAuthenticationRequest {
        bearer,
        method: request.method().clone(),
        path: request.uri().path().into(),
    })
}

fn authentication_response(
    error: A2aServerAuthenticationError,
    challenge: Option<&str>,
) -> Response<Body> {
    let status = match error {
        A2aServerAuthenticationError::InvalidCredential => StatusCode::UNAUTHORIZED,
        A2aServerAuthenticationError::Forbidden => StatusCode::FORBIDDEN,
        A2aServerAuthenticationError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    };
    let mut response = plain_response(status, error.to_string());
    if status == StatusCode::UNAUTHORIZED {
        if let Some(challenge) = challenge.and_then(|value| HeaderValue::from_str(value).ok()) {
            response.headers_mut().insert(WWW_AUTHENTICATE, challenge);
        }
    }
    response
}

fn plain_response(status: StatusCode, message: impl Into<String>) -> Response<Body> {
    let mut response = Response::new(Body::from(message.into()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn allowed_host(headers: &HeaderMap, allowed: &[Box<str>]) -> bool {
    let values = headers.get_all(HOST);
    let mut iter = values.iter();
    let Some(first) = iter.next().and_then(|value| value.to_str().ok()) else {
        return false;
    };
    if iter.next().is_some() {
        return false;
    }
    let candidate = first.to_ascii_lowercase();
    allowed
        .binary_search_by(|value| value.as_ref().cmp(&candidate))
        .is_ok()
}

fn allowed_origin(headers: &HeaderMap, allowed: &[Box<str>]) -> Result<Option<HeaderValue>, ()> {
    let values = headers.get_all(axum::http::header::ORIGIN);
    let mut iter = values.iter();
    let Some(first) = iter.next() else {
        return Ok(None);
    };
    if iter.next().is_some() {
        return Err(());
    }
    let text = first.to_str().map_err(|_| ())?;
    if allowed.iter().any(|value| value.as_ref() == text) {
        Ok(Some(first.clone()))
    } else {
        Err(())
    }
}

fn apply_origin_header(response: &mut Response<Body>, origin: Option<HeaderValue>) {
    if let Some(origin) = origin {
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
        response
            .headers_mut()
            .insert(VARY, HeaderValue::from_static("Origin"));
    }
}

fn valid_content_type(request: &Request) -> bool {
    if matches!(*request.method(), Method::GET | Method::DELETE) {
        return true;
    }
    let values = request.headers().get_all(CONTENT_TYPE);
    let mut iter = values.iter();
    let Some(value) = iter.next().and_then(|value| value.to_str().ok()) else {
        return false;
    };
    if iter.next().is_some() {
        return false;
    }
    value.parse::<mime::Mime>().is_ok_and(|value| {
        (value.type_() == mime::APPLICATION && value.subtype() == mime::JSON)
            || value.essence_str() == "application/a2a+json"
    })
}

fn canonical_path(path: &str, options: &A2aServerHttpOptions) -> bool {
    if path == A2A_AGENT_CARD_PATH
        || path == options.jsonrpc_path.as_ref()
        || path
            .strip_suffix('/')
            .is_some_and(|value| value == options.jsonrpc_path.as_ref())
    {
        return true;
    }
    let Some(suffix) = path.strip_prefix(options.rest_prefix.as_ref()) else {
        return false;
    };
    if matches!(
        suffix,
        "/message:send" | "/message:stream" | "/tasks" | "/extendedAgentCard"
    ) {
        return true;
    }
    let Some(task_suffix) = suffix.strip_prefix("/tasks/") else {
        return false;
    };
    if task_suffix.is_empty() || task_suffix.contains("//") {
        return false;
    }
    if let Some(task_id) = task_suffix.strip_suffix(":cancel") {
        return valid_path_segment(task_id);
    }
    if let Some(task_id) = task_suffix.strip_suffix(":subscribe") {
        return valid_path_segment(task_id);
    }
    if let Some((task_id, config_suffix)) = task_suffix.split_once("/pushNotificationConfigs") {
        return valid_path_segment(task_id)
            && (config_suffix.is_empty()
                || config_suffix
                    .strip_prefix('/')
                    .is_some_and(valid_path_segment));
    }
    valid_path_segment(task_suffix)
}

fn valid_path_segment(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1536 && !value.contains('/') && !matches!(value, "." | "..")
}

struct A2aSdkRequestHandler {
    card: A2aAgentCard,
    authorizer: Arc<dyn A2aServerAuthorizer>,
    admission: Arc<dyn A2aServerAdmissionControl>,
    service: Arc<dyn A2aTaskService>,
    operation_slots: Arc<Semaphore>,
    request_timeout: Duration,
    stream_idle_timeout: Duration,
    maximum_stream_events: u32,
    shutdown: CancellationToken,
}

impl A2aSdkRequestHandler {
    async fn prepare(
        &self,
        params: &ServiceParams,
        operation: A2aServerOperation,
        task_id: Option<&str>,
        config_id: Option<&str>,
    ) -> Result<(A2aRequestContext, OwnedSemaphorePermit), wire::A2AError> {
        let principal = A2A_REQUEST_PRINCIPAL
            .try_with(Clone::clone)
            .map_err(|_| wire::A2AError::internal("request authentication context is absent"))?;
        let extensions = negotiate_extensions(params, &self.card)?;
        let capabilities = self.card.capabilities();
        match operation {
            A2aServerOperation::SendStreamingMessage | A2aServerOperation::SubscribeToTask
                if !capabilities.supports_streaming() =>
            {
                return Err(wire::A2AError::unsupported_operation(
                    "A2A streaming is not supported",
                ));
            }
            A2aServerOperation::CreatePushConfig
            | A2aServerOperation::GetPushConfig
            | A2aServerOperation::ListPushConfigs
            | A2aServerOperation::DeletePushConfig
                if !capabilities.supports_push_notifications() =>
            {
                return Err(wire::A2AError::push_notification_not_supported());
            }
            A2aServerOperation::GetExtendedAgentCard
                if !capabilities.supports_extended_agent_card() =>
            {
                return Err(wire::A2AError::unsupported_operation(
                    "A2A extended Agent Card is not supported",
                ));
            }
            _ => {}
        }
        self.authorizer
            .authorize(A2aServerAuthorizationRequest {
                principal: principal.clone(),
                operation,
                task_id: task_id.map(Into::into),
                config_id: config_id.map(Into::into),
            })
            .await
            .map_err(map_authorization_error)?;
        self.admission
            .admit(A2aServerAdmissionRequest {
                principal: principal.clone(),
                operation,
            })
            .await
            .map_err(map_admission_error)?;
        let permit = Arc::clone(&self.operation_slots)
            .try_acquire_owned()
            .map_err(|_| wire::A2AError::internal("A2A service is overloaded"))?;
        Ok((
            A2aRequestContext {
                principal,
                extensions: extensions
                    .into_iter()
                    .map(String::into_boxed_str)
                    .collect::<Vec<_>>()
                    .into(),
            },
            permit,
        ))
    }

    async fn unary<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, A2aTaskServiceError>>,
        _permit: OwnedSemaphorePermit,
    ) -> Result<T, wire::A2AError> {
        tokio::select! {
            () = self.shutdown.cancelled() => Err(wire::A2AError::internal("A2A service is shutting down")),
            result = tokio::time::timeout(self.request_timeout, future) => {
                match result {
                    Ok(result) => result.map_err(map_service_error),
                    Err(_) => Err(wire::A2AError::internal("A2A operation timed out")),
                }
            }
        }
    }

    async fn stream(
        &self,
        future: impl std::future::Future<Output = Result<A2aEventStream, A2aTaskServiceError>>,
        permit: OwnedSemaphorePermit,
    ) -> Result<
        futures_util::stream::BoxStream<'static, Result<wire::StreamResponse, wire::A2AError>>,
        wire::A2AError,
    > {
        let stream = tokio::select! {
            () = self.shutdown.cancelled() => {
                return Err(wire::A2AError::internal("A2A service is shutting down"));
            }
            result = tokio::time::timeout(self.request_timeout, future) => {
                match result {
                    Ok(result) => result.map_err(map_service_error)?,
                    Err(_) => return Err(wire::A2AError::internal("A2A operation timed out")),
                }
            }
        };
        // The admission permit acquired before the backend call remains held
        // for the complete stream lifetime. This prevents a release/reacquire
        // race from orphaning a durable subscription after it was created.
        Ok(self.bounded_stream(stream, permit))
    }

    fn bounded_stream(
        &self,
        stream: A2aEventStream,
        permit: OwnedSemaphorePermit,
    ) -> futures_util::stream::BoxStream<'static, Result<wire::StreamResponse, wire::A2AError>>
    {
        struct StreamState {
            stream: A2aEventStream,
            _permit: OwnedSemaphorePermit,
            shutdown: CancellationToken,
            idle_timeout: Duration,
            remaining: u32,
            terminal_seen: bool,
        }

        Box::pin(futures_util::stream::unfold(
            StreamState {
                stream,
                _permit: permit,
                shutdown: self.shutdown.clone(),
                idle_timeout: self.stream_idle_timeout,
                remaining: self.maximum_stream_events,
                terminal_seen: false,
            },
            |mut state| async move {
                if state.terminal_seen {
                    return None;
                }
                if state.remaining == 0 {
                    return Some((
                        Err(wire::A2AError::internal("A2A stream event limit reached")),
                        StreamState {
                            terminal_seen: true,
                            ..state
                        },
                    ));
                }
                let next = tokio::select! {
                    () = state.shutdown.cancelled() => {
                        return Some((
                            Err(wire::A2AError::internal("A2A service is shutting down")),
                            StreamState { terminal_seen: true, ..state },
                        ));
                    }
                    next = tokio::time::timeout(state.idle_timeout, state.stream.next()) => next,
                };
                match next {
                    Err(_) => Some((
                        Err(wire::A2AError::internal("A2A stream idle timeout")),
                        StreamState {
                            terminal_seen: true,
                            ..state
                        },
                    )),
                    Ok(None) => None,
                    Ok(Some(Err(error))) => Some((
                        Err(map_service_error(error)),
                        StreamState {
                            terminal_seen: true,
                            ..state
                        },
                    )),
                    Ok(Some(Ok(event))) => {
                        state.remaining -= 1;
                        state.terminal_seen = event.is_terminal();
                        Some((Ok(event.into_wire()), state))
                    }
                }
            },
        ))
    }
}

fn negotiate_extensions(
    params: &ServiceParams,
    card: &A2aAgentCard,
) -> Result<Vec<String>, wire::A2AError> {
    validate_version(params)?;
    let mut requested = Vec::new();
    if let Some(values) = params.get(A2A_EXTENSIONS_HEADER) {
        let encoded_size = values.iter().map(String::len).sum::<usize>();
        if encoded_size > MAX_EXTENSION_HEADER_BYTES {
            return Err(wire::A2AError::invalid_request(
                "A2A-Extensions header is too large",
            ));
        }
        for value in values {
            for extension in value.split(',') {
                let extension = extension.trim();
                if extension.is_empty() {
                    continue;
                }
                if reqwest::Url::parse(extension).is_err() {
                    return Err(wire::A2AError::invalid_request(
                        "A2A-Extensions contains an invalid URI",
                    ));
                }
                requested.push(extension.to_string());
                if requested.len() > MAX_NEGOTIATED_EXTENSIONS {
                    return Err(wire::A2AError::invalid_request(
                        "too many A2A extensions requested",
                    ));
                }
            }
        }
    }
    requested.sort_unstable();
    requested.dedup();

    let declared = card.capabilities().extensions().to_vec();
    let supported = declared
        .iter()
        .map(crate::A2aAgentExtension::uri)
        .collect::<HashSet<_>>();
    for extension in &declared {
        if extension.is_required() && !requested.iter().any(|value| value == extension.uri()) {
            return Err(wire::A2AError::new(
                wire::error_code::EXTENSION_SUPPORT_REQUIRED,
                "required A2A extension was not requested",
            ));
        }
    }
    requested.retain(|extension| supported.contains(extension.as_str()));
    Ok(requested)
}

fn validate_version(params: &ServiceParams) -> Result<(), wire::A2AError> {
    let Some(values) = params.get(A2A_VERSION_HEADER) else {
        return Ok(());
    };
    if values.len() != 1 {
        return Err(wire::A2AError::invalid_request(
            "A2A-Version must appear at most once",
        ));
    }
    let version = values[0].trim();
    if version.is_empty() || version == A2A_PROTOCOL_VERSION_1_0 {
        Ok(())
    } else {
        Err(wire::A2AError::version_not_supported(version))
    }
}

fn contract_error(_: A2aContractError) -> wire::A2AError {
    wire::A2AError::invalid_params("A2A payload violates bounded contract")
}

fn map_authorization_error(error: A2aServerAuthorizationError) -> wire::A2AError {
    match error {
        A2aServerAuthorizationError::Forbidden => {
            wire::A2AError::task_not_found("authorized resource")
        }
        A2aServerAuthorizationError::Unavailable => {
            wire::A2AError::internal("A2A authorization is unavailable")
        }
    }
}

fn map_admission_error(error: A2aServerAdmissionError) -> wire::A2AError {
    match error {
        A2aServerAdmissionError::Rejected => {
            wire::A2AError::internal("A2A request is rate limited")
        }
        A2aServerAdmissionError::Unavailable => {
            wire::A2AError::internal("A2A admission is unavailable")
        }
    }
}

fn map_service_error(error: A2aTaskServiceError) -> wire::A2AError {
    match error {
        A2aTaskServiceError::TaskNotFound => wire::A2AError::task_not_found("task"),
        A2aTaskServiceError::TaskNotCancelable => wire::A2AError::task_not_cancelable("task"),
        A2aTaskServiceError::PushNotificationsNotSupported => {
            wire::A2AError::push_notification_not_supported()
        }
        A2aTaskServiceError::UnsupportedOperation => {
            wire::A2AError::unsupported_operation("A2A operation is not supported")
        }
        A2aTaskServiceError::ContentTypeNotSupported => {
            wire::A2AError::content_type_not_supported()
        }
        A2aTaskServiceError::InvalidAgentResponse => wire::A2AError::invalid_agent_response(),
        A2aTaskServiceError::ExtendedCardNotConfigured => wire::A2AError::new(
            wire::error_code::EXTENDED_CARD_NOT_CONFIGURED,
            "extended Agent Card is not configured",
        ),
        A2aTaskServiceError::ExtensionSupportRequired => wire::A2AError::new(
            wire::error_code::EXTENSION_SUPPORT_REQUIRED,
            "required A2A extension is not supported",
        ),
        A2aTaskServiceError::InvalidRequest => {
            wire::A2AError::invalid_params("A2A request is invalid")
        }
        A2aTaskServiceError::Unavailable => wire::A2AError::internal("A2A service is unavailable"),
    }
}

#[async_trait]
impl RequestHandler for A2aSdkRequestHandler {
    async fn send_message(
        &self,
        params: &ServiceParams,
        request: wire::SendMessageRequest,
    ) -> Result<wire::SendMessageResponse, wire::A2AError> {
        let task_hint = request.message.task_id.clone();
        let request = A2aSendMessageRequest::try_from_wire(request).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(
                params,
                A2aServerOperation::SendMessage,
                task_hint.as_deref(),
                None,
            )
            .await?;
        let response = self
            .unary(self.service.send_message(context, request), permit)
            .await?;
        Ok(response.into_wire())
    }

    async fn send_streaming_message(
        &self,
        params: &ServiceParams,
        request: wire::SendMessageRequest,
    ) -> Result<
        futures_util::stream::BoxStream<'static, Result<wire::StreamResponse, wire::A2AError>>,
        wire::A2AError,
    > {
        let task_hint = request.message.task_id.clone();
        let request = A2aSendMessageRequest::try_from_wire(request).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(
                params,
                A2aServerOperation::SendStreamingMessage,
                task_hint.as_deref(),
                None,
            )
            .await?;
        self.stream(
            self.service.send_streaming_message(context, request),
            permit,
        )
        .await
    }

    async fn get_task(
        &self,
        params: &ServiceParams,
        request: wire::GetTaskRequest,
    ) -> Result<wire::Task, wire::A2AError> {
        let task_id = request.id.clone();
        let request = A2aGetTaskRequest::try_from_wire(request).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(params, A2aServerOperation::GetTask, Some(&task_id), None)
            .await?;
        let task = self
            .unary(self.service.get_task(context, request), permit)
            .await?;
        Ok(task.into_wire())
    }

    async fn list_tasks(
        &self,
        params: &ServiceParams,
        request: wire::ListTasksRequest,
    ) -> Result<wire::ListTasksResponse, wire::A2AError> {
        let request = A2aListTasksRequest::try_from_wire(request).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(params, A2aServerOperation::ListTasks, None, None)
            .await?;
        let page = self
            .unary(self.service.list_tasks(context, request), permit)
            .await?;
        page.into_wire().map_err(contract_error)
    }

    async fn cancel_task(
        &self,
        params: &ServiceParams,
        request: wire::CancelTaskRequest,
    ) -> Result<wire::Task, wire::A2AError> {
        let task_id = request.id.clone();
        let request = A2aCancelTaskRequest::try_from_wire(request).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(params, A2aServerOperation::CancelTask, Some(&task_id), None)
            .await?;
        let task = self
            .unary(self.service.cancel_task(context, request), permit)
            .await?;
        Ok(task.into_wire())
    }

    async fn subscribe_to_task(
        &self,
        params: &ServiceParams,
        request: wire::SubscribeToTaskRequest,
    ) -> Result<
        futures_util::stream::BoxStream<'static, Result<wire::StreamResponse, wire::A2AError>>,
        wire::A2AError,
    > {
        let task_id = request.id.clone();
        let request = A2aSubscribeTaskRequest::try_from_wire(request).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(
                params,
                A2aServerOperation::SubscribeToTask,
                Some(&task_id),
                None,
            )
            .await?;
        self.stream(self.service.subscribe_to_task(context, request), permit)
            .await
    }

    async fn create_push_config(
        &self,
        params: &ServiceParams,
        request: wire::TaskPushNotificationConfig,
    ) -> Result<wire::TaskPushNotificationConfig, wire::A2AError> {
        let task_hint = (!request.task_id.is_empty()).then_some(request.task_id.clone());
        let request = A2aPushConfig::try_from_wire(request).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(
                params,
                A2aServerOperation::CreatePushConfig,
                task_hint.as_deref(),
                None,
            )
            .await?;
        let config = self
            .unary(self.service.create_push_config(context, request), permit)
            .await?;
        Ok(config.into_wire())
    }

    async fn get_push_config(
        &self,
        params: &ServiceParams,
        request: wire::GetTaskPushNotificationConfigRequest,
    ) -> Result<wire::TaskPushNotificationConfig, wire::A2AError> {
        let task_id = request.task_id.clone();
        let config_id = request.id.clone();
        let request = A2aGetPushConfigRequest::try_from_wire(request).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(
                params,
                A2aServerOperation::GetPushConfig,
                Some(&task_id),
                Some(&config_id),
            )
            .await?;
        let config = self
            .unary(self.service.get_push_config(context, request), permit)
            .await?;
        Ok(config.into_wire())
    }

    async fn list_push_configs(
        &self,
        params: &ServiceParams,
        request: wire::ListTaskPushNotificationConfigsRequest,
    ) -> Result<wire::ListTaskPushNotificationConfigsResponse, wire::A2AError> {
        let task_id = request.task_id.clone();
        let request = A2aListPushConfigsRequest::try_from_wire(request).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(
                params,
                A2aServerOperation::ListPushConfigs,
                Some(&task_id),
                None,
            )
            .await?;
        let page = self
            .unary(self.service.list_push_configs(context, request), permit)
            .await?;
        Ok(page.into_wire())
    }

    async fn delete_push_config(
        &self,
        params: &ServiceParams,
        request: wire::DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), wire::A2AError> {
        if request.tenant.is_some() {
            return Err(contract_error(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            }));
        }
        let task_id = request.task_id.clone();
        let config_id = request.id.clone();
        let request =
            A2aDeletePushConfigRequest::new(request.task_id, request.id).map_err(contract_error)?;
        let (context, permit) = self
            .prepare(
                params,
                A2aServerOperation::DeletePushConfig,
                Some(&task_id),
                Some(&config_id),
            )
            .await?;
        self.unary(self.service.delete_push_config(context, request), permit)
            .await
    }

    async fn get_extended_agent_card(
        &self,
        params: &ServiceParams,
        request: wire::GetExtendedAgentCardRequest,
    ) -> Result<wire::AgentCard, wire::A2AError> {
        if request.tenant.is_some() {
            return Err(contract_error(A2aContractError::Unsupported {
                field: "request tenant override",
                value: "tenant".to_string(),
            }));
        }
        let (context, permit) = self
            .prepare(params, A2aServerOperation::GetExtendedAgentCard, None, None)
            .await?;
        let card = self
            .unary(self.service.get_extended_agent_card(context), permit)
            .await?;
        Ok(card.into_wire())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        A2aAgentCapabilities, A2aAgentInterface, A2aAgentSkill, A2aBinding, A2aMessage,
        A2aMessageRole, A2aPart, A2aTaskState, A2aTaskStatus,
    };
    use axum::body::Body;
    use stateknot_core::{IssuerId, SubjectId};
    use tower_service::Service;

    #[derive(Clone)]
    struct TestAuthenticator;

    impl A2aServerAuthenticator for TestAuthenticator {
        fn authenticate(
            &self,
            request: A2aServerAuthenticationRequest,
        ) -> BoxFuture<'_, Result<A2aServerPrincipal, A2aServerAuthenticationError>> {
            Box::pin(async move {
                if request
                    .bearer()
                    .is_none_or(|value| value.expose_secret() != "token")
                {
                    return Err(A2aServerAuthenticationError::InvalidCredential);
                }
                Ok(A2aServerPrincipal::new(
                    TenantId::new("tenant-a").unwrap(),
                    PrincipalIdentity::new(
                        IssuerId::new("https://issuer.example").unwrap(),
                        SubjectId::new("caller").unwrap(),
                    ),
                    ["a2a.invoke"],
                )
                .unwrap())
            })
        }
    }

    struct TestService;

    impl A2aTaskService for TestService {
        fn capabilities(&self) -> A2aTaskServiceCapabilities {
            A2aTaskServiceCapabilities::default()
        }

        fn send_message(
            &self,
            _context: A2aRequestContext,
            request: A2aSendMessageRequest,
        ) -> BoxFuture<'_, Result<A2aSendMessageResponse, A2aTaskServiceError>> {
            Box::pin(async move {
                let response = A2aMessage::new(
                    "response-1",
                    A2aMessageRole::Agent,
                    vec![
                        A2aPart::text(format!("echo:{}", request.message().message_id())).unwrap(),
                    ],
                )
                .unwrap();
                Ok(A2aSendMessageResponse::Message(response))
            })
        }

        fn send_streaming_message(
            &self,
            _context: A2aRequestContext,
            _request: A2aSendMessageRequest,
        ) -> BoxFuture<'_, Result<A2aEventStream, A2aTaskServiceError>> {
            Box::pin(async { Err(A2aTaskServiceError::UnsupportedOperation) })
        }

        fn get_task(
            &self,
            _context: A2aRequestContext,
            _request: A2aGetTaskRequest,
        ) -> BoxFuture<'_, Result<A2aTask, A2aTaskServiceError>> {
            Box::pin(async { Err(A2aTaskServiceError::TaskNotFound) })
        }

        fn list_tasks(
            &self,
            _context: A2aRequestContext,
            _request: A2aListTasksRequest,
        ) -> BoxFuture<'_, Result<A2aTaskPage, A2aTaskServiceError>> {
            Box::pin(async {
                A2aTaskPage::new(Vec::new(), None, A2aListTasksRequest::DEFAULT_PAGE_SIZE, 0)
                    .map_err(|_| A2aTaskServiceError::InvalidAgentResponse)
            })
        }

        fn cancel_task(
            &self,
            _context: A2aRequestContext,
            _request: A2aCancelTaskRequest,
        ) -> BoxFuture<'_, Result<A2aTask, A2aTaskServiceError>> {
            Box::pin(async { Err(A2aTaskServiceError::TaskNotFound) })
        }

        fn subscribe_to_task(
            &self,
            _context: A2aRequestContext,
            _request: A2aSubscribeTaskRequest,
        ) -> BoxFuture<'_, Result<A2aEventStream, A2aTaskServiceError>> {
            Box::pin(async { Err(A2aTaskServiceError::UnsupportedOperation) })
        }

        fn create_push_config(
            &self,
            _context: A2aRequestContext,
            _config: A2aPushConfig,
        ) -> BoxFuture<'_, Result<A2aPushConfig, A2aTaskServiceError>> {
            Box::pin(async { Err(A2aTaskServiceError::PushNotificationsNotSupported) })
        }

        fn get_push_config(
            &self,
            _context: A2aRequestContext,
            _request: A2aGetPushConfigRequest,
        ) -> BoxFuture<'_, Result<A2aPushConfig, A2aTaskServiceError>> {
            Box::pin(async { Err(A2aTaskServiceError::PushNotificationsNotSupported) })
        }

        fn list_push_configs(
            &self,
            _context: A2aRequestContext,
            _request: A2aListPushConfigsRequest,
        ) -> BoxFuture<'_, Result<A2aPushConfigPage, A2aTaskServiceError>> {
            Box::pin(async { Err(A2aTaskServiceError::PushNotificationsNotSupported) })
        }

        fn delete_push_config(
            &self,
            _context: A2aRequestContext,
            _request: A2aDeletePushConfigRequest,
        ) -> BoxFuture<'_, Result<(), A2aTaskServiceError>> {
            Box::pin(async { Err(A2aTaskServiceError::PushNotificationsNotSupported) })
        }

        fn get_extended_agent_card(
            &self,
            _context: A2aRequestContext,
        ) -> BoxFuture<'_, Result<A2aAgentCard, A2aTaskServiceError>> {
            Box::pin(async { Err(A2aTaskServiceError::ExtendedCardNotConfigured) })
        }
    }

    fn card() -> A2aAgentCard {
        A2aAgentCard::builder("Test", "Test A2A agent", "0.0.0")
            .unwrap()
            .capabilities(A2aAgentCapabilities::new())
            .interface(
                A2aAgentInterface::new("https://agent.example/a2a/rest", A2aBinding::HttpJson)
                    .unwrap(),
            )
            .unwrap()
            .interface(
                A2aAgentInterface::new("https://agent.example/a2a/jsonrpc", A2aBinding::JsonRpc)
                    .unwrap(),
            )
            .unwrap()
            .default_input_modes(vec!["text/plain".to_string()])
            .unwrap()
            .default_output_modes(vec!["text/plain".to_string()])
            .unwrap()
            .skill(
                A2aAgentSkill::new(
                    "echo",
                    "Echo",
                    "Echoes a bounded message.",
                    vec!["test".to_string()],
                )
                .unwrap(),
            )
            .unwrap()
            .build()
            .unwrap()
    }

    fn server() -> A2aServer {
        A2aServer::new(
            card(),
            TestAuthenticator,
            AllowA2aServerAuthorization,
            AllowA2aServerAdmission,
            TestService,
            A2aServerHttpOptions::new()
                .with_allowed_authorities(["agent.example"])
                .unwrap()
                .with_bearer_challenge("Bearer realm=\"stateknot\"")
                .unwrap(),
            CancellationToken::new(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn authenticates_before_jsonrpc_dispatch() {
        let mut router = server().router();
        let request = Request::builder()
            .method(Method::POST)
            .uri("/a2a/jsonrpc")
            .header(HOST, "agent.example")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("not json"))
            .unwrap();
        let response = router.call(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().contains_key(WWW_AUTHENTICATE));
    }

    #[tokio::test]
    async fn serves_cacheable_card_and_rejects_legacy_routes() {
        let mut router = server().router();
        let response = router
            .call(
                Request::builder()
                    .uri(A2A_AGENT_CARD_PATH)
                    .header(HOST, "agent.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().contains_key(ETAG));
        let last_modified = response.headers()[LAST_MODIFIED].clone();
        assert!(response.headers().contains_key(CACHE_CONTROL));

        let response = router
            .call(
                Request::builder()
                    .uri(A2A_AGENT_CARD_PATH)
                    .header(HOST, "agent.example")
                    .header(IF_MODIFIED_SINCE, last_modified)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/a2a/rest/message/send")
                    .header(HOST, "agent.example")
                    .header(CONTENT_TYPE, "application/json")
                    .header(AUTHORIZATION, "Bearer token")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn serves_base_url_clients_at_the_jsonrpc_slash_form() {
        let mut router = server().router();
        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/a2a/jsonrpc/")
                    .header(HOST, "agent.example")
                    .header(CONTENT_TYPE, "application/json")
                    .header(AUTHORIZATION, "Bearer token")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[CONTENT_TYPE],
            HeaderValue::from_static("application/json")
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], -32600);
        assert_eq!(value["id"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn returns_jsonrpc_parse_error_after_authentication() {
        let mut router = server().router();
        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/a2a/jsonrpc")
                    .header(HOST, "agent.example")
                    .header(CONTENT_TYPE, "application/json")
                    .header(AUTHORIZATION, "Bearer token")
                    .body(Body::from("{"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], -32700);
        assert_eq!(value["id"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn rejects_rest_media_type_with_aip193_error() {
        let mut router = server().router();
        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/a2a/rest/message:send")
                    .header(HOST, "agent.example")
                    .header(CONTENT_TYPE, "text/plain")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["code"], 415);
        assert_eq!(
            value["error"]["details"][0]["reason"],
            "CONTENT_TYPE_NOT_SUPPORTED"
        );
    }

    #[tokio::test]
    async fn dispatches_bounded_rest_message() {
        let mut router = server().router();
        let body = serde_json::json!({
            "message": {
                "messageId": "message-1",
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}]
            }
        });
        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/a2a/rest/message:send")
                    .header(HOST, "agent.example")
                    .header(CONTENT_TYPE, "application/a2a+json")
                    .header(AUTHORIZATION, "Bearer token")
                    .header(A2A_VERSION_HEADER, A2A_PROTOCOL_VERSION_1_0)
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["message"]["parts"][0]["text"], "echo:message-1");
    }

    #[tokio::test]
    async fn ignores_unknown_fields_for_forward_compatibility() {
        let mut router = server().router();
        let params = serde_json::json!({
            "message": {
                "messageId": "message-with-future-fields",
                "role": "ROLE_USER",
                "parts": [{"text": "hello", "futurePartField": true}],
                "futureMessageField": "ignored"
            },
            "futureRequestField": 42
        });

        let jsonrpc = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "SendMessage",
            "params": params
        });
        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/a2a/jsonrpc/")
                    .header(HOST, "agent.example")
                    .header(CONTENT_TYPE, "application/json")
                    .header(AUTHORIZATION, "Bearer token")
                    .header(A2A_VERSION_HEADER, A2A_PROTOCOL_VERSION_1_0)
                    .body(Body::from(serde_json::to_vec(&jsonrpc).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(value.get("error").is_none(), "{value}");

        let response = router
            .call(
                Request::builder()
                    .method(Method::POST)
                    .uri("/a2a/rest/message:send")
                    .header(HOST, "agent.example")
                    .header(CONTENT_TYPE, "application/a2a+json")
                    .header(AUTHORIZATION, "Bearer token")
                    .header(A2A_VERSION_HEADER, A2A_PROTOCOL_VERSION_1_0)
                    .body(Body::from(serde_json::to_vec(&params).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            value["message"]["parts"][0]["text"],
            "echo:message-with-future-fields"
        );
    }

    #[test]
    fn task_status_construction_stays_independent() {
        let status = A2aTaskStatus::new(A2aTaskState::Completed, None, None).unwrap();
        assert_eq!(status.state(), A2aTaskState::Completed);
    }

    #[test]
    fn normalizes_sdk_rest_statuses_and_utc_timestamps() {
        let mut status = StatusCode::BAD_REQUEST;
        let mut value = serde_json::json!({
            "error": {
                "code": 400,
                "status": "FAILED_PRECONDITION",
                "details": [{
                    "reason": "TASK_NOT_CANCELABLE",
                    "metadata": {"timestamp": "2026-09-03T02:35:41.624133+00:00"}
                }]
            }
        });
        normalize_utc_timestamps(&mut value);
        normalize_rest_error_status(&mut status, &mut value);
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(value["error"]["code"], 409);
        assert_eq!(
            value["error"]["details"][0]["metadata"]["timestamp"],
            "2026-09-03T02:35:41.624133Z"
        );
    }
}
