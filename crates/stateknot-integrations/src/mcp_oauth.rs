// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Production OAuth 2.1 authorization for the stateless MCP client.
//!
//! The transport owns bounded challenge replay while this module owns OAuth
//! discovery, registration, PKCE, issuer validation, token refresh, and the
//! user-agent handoff. Tokens never enter MCP request metadata or URLs.

use std::{fmt, net::IpAddr, sync::Arc, time::Duration};

use reqwest::Url;
use rmcp::transport::auth::{
    AuthError, AuthorizationManager, AuthorizationRequest, AuthorizationSession,
    ScopeUpgradeConfig, WWWAuthenticateParams,
};
use stateknot_core::BoxFuture;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    ApiKey, McpAuthorization, McpAuthorizationError, McpClientAuthorizationChallenge,
    McpClientAuthorizationChallengeStatus, McpClientAuthorizationProvider,
    McpClientAuthorizationRequest, McpClientAuthorizationRetry, ProviderEndpoint,
};

pub use rmcp::transport::auth::{
    CredentialRefreshGuard as McpOAuthCredentialRefreshGuard,
    CredentialStore as McpOAuthCredentialStore,
    InMemoryCredentialStore as InMemoryMcpOAuthCredentialStore,
    InMemoryStateStore as InMemoryMcpOAuthStateStore, StateStore as McpOAuthStateStore,
    StoredAuthorizationState as McpOAuthStoredAuthorizationState,
    StoredCredentials as McpOAuthStoredCredentials,
};

const MAX_CLIENT_NAME_BYTES: usize = 256;
const MAX_CLIENT_ID_BYTES: usize = 4096;
const MAX_OAUTH_URL_BYTES: usize = 16 * 1024;
const DEFAULT_AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const HARD_MAXIMUM_AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_MAXIMUM_SCOPE_UPGRADES: u32 = 1;
const HARD_MAXIMUM_SCOPE_UPGRADES: u32 = 3;

/// Canonical MCP resource URI used for OAuth discovery and RFC 8707 resource
/// indicators.
#[derive(Clone, Eq, PartialEq)]
pub struct McpOAuthResource {
    url: Url,
    local_http: bool,
}

impl McpOAuthResource {
    /// Constructs a production HTTPS resource URI.
    pub fn https(value: &str) -> Result<Self, McpOAuthResourceError> {
        Self::parse(value, false)
    }

    /// Constructs an HTTP resource URI restricted to a loopback host.
    ///
    /// Literal loopback addresses are preferred. `localhost` is accepted for
    /// interoperability with local OAuth authorization servers and official
    /// MCP conformance fixtures; callers must not use this constructor for a
    /// remotely reachable service.
    pub fn loopback_http(value: &str) -> Result<Self, McpOAuthResourceError> {
        Self::parse(value, true)
    }

    /// Derives the OAuth resource URI from a validated MCP endpoint.
    pub fn from_endpoint(endpoint: &ProviderEndpoint) -> Result<Self, McpOAuthResourceError> {
        let url = endpoint
            .join("")
            .map_err(|_| McpOAuthResourceError::InvalidUrl)?;
        if endpoint.is_https() {
            Self::https(url.as_str())
        } else {
            Self::loopback_http(url.as_str())
        }
    }

    fn parse(value: &str, local_http: bool) -> Result<Self, McpOAuthResourceError> {
        if value.is_empty() || value.len() > MAX_OAUTH_URL_BYTES {
            return Err(McpOAuthResourceError::InvalidUrl);
        }
        let url = Url::parse(value).map_err(|_| McpOAuthResourceError::InvalidUrl)?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(McpOAuthResourceError::EmbeddedCredentials);
        }
        if url.fragment().is_some() {
            return Err(McpOAuthResourceError::Fragment);
        }
        let host = url.host_str().ok_or(McpOAuthResourceError::MissingHost)?;
        if local_http {
            if url.scheme() != "http" || !is_loopback_host(host) {
                return Err(McpOAuthResourceError::LoopbackHttpRequired);
            }
        } else if url.scheme() != "https" {
            return Err(McpOAuthResourceError::HttpsRequired);
        }
        Ok(Self { url, local_http })
    }

    fn as_url(&self) -> &Url {
        &self.url
    }
}

impl fmt::Debug for McpOAuthResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthResource")
            .field(
                "transport",
                &if self.local_http {
                    "loopback-http"
                } else {
                    "https"
                },
            )
            .field("authority", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

/// Invalid OAuth resource URI.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpOAuthResourceError {
    /// The value was not an absolute URL.
    #[error("MCP OAuth resource is not an absolute URL")]
    InvalidUrl,
    /// Production resources require HTTPS.
    #[error("MCP OAuth resource must use HTTPS")]
    HttpsRequired,
    /// Local HTTP resources require a loopback host.
    #[error("MCP OAuth local resource must use HTTP on a loopback host")]
    LoopbackHttpRequired,
    /// The URL omitted its host.
    #[error("MCP OAuth resource must include a host")]
    MissingHost,
    /// URL userinfo could leak credentials.
    #[error("MCP OAuth resource must not contain embedded credentials")]
    EmbeddedCredentials,
    /// RFC 8707 resource indicators prohibit fragments.
    #[error("MCP OAuth resource must not contain a fragment")]
    Fragment,
}

/// OAuth client registration material and priority.
#[derive(Clone)]
#[non_exhaustive]
pub enum McpOAuthRegistration {
    /// Prefer CIMD when configured and advertised, then use DCR as the
    /// deprecated compatibility fallback.
    Automatic {
        /// Optional HTTPS Client ID Metadata Document URL.
        client_metadata_url: Option<Box<str>>,
    },
    /// Use client credentials provisioned out of band and never perform DCR.
    PreRegistered {
        /// Issuer-bound public client identifier.
        client_id: Box<str>,
        /// Optional confidential-client secret.
        client_secret: Option<ApiKey>,
    },
}

impl McpOAuthRegistration {
    /// Creates automatic registration with a DCR compatibility fallback.
    #[must_use]
    pub const fn automatic() -> Self {
        Self::Automatic {
            client_metadata_url: None,
        }
    }

    /// Creates automatic registration that prefers the supplied CIMD URL.
    pub fn client_metadata_document(value: &str) -> Result<Self, McpOAuthOptionsError> {
        validate_client_metadata_url(value)?;
        Ok(Self::Automatic {
            client_metadata_url: Some(value.to_owned().into_boxed_str()),
        })
    }

    /// Creates an issuer-bound pre-registered client.
    pub fn pre_registered(
        client_id: impl Into<String>,
        client_secret: Option<ApiKey>,
    ) -> Result<Self, McpOAuthOptionsError> {
        let client_id = client_id.into();
        validate_bounded_text(&client_id, MAX_CLIENT_ID_BYTES)
            .map_err(|()| McpOAuthOptionsError::InvalidClientId)?;
        Ok(Self::PreRegistered {
            client_id: client_id.into_boxed_str(),
            client_secret,
        })
    }
}

impl fmt::Debug for McpOAuthRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Automatic {
                client_metadata_url,
            } => formatter
                .debug_struct("Automatic")
                .field("has_client_metadata_url", &client_metadata_url.is_some())
                .finish(),
            Self::PreRegistered { client_secret, .. } => formatter
                .debug_struct("PreRegistered")
                .field("client_id", &"[REDACTED]")
                .field("has_client_secret", &client_secret.is_some())
                .finish(),
        }
    }
}

/// Bounded OAuth policy for one MCP resource binding.
#[derive(Clone, Debug)]
pub struct McpOAuthOptions {
    redirect_uri: Url,
    client_name: Box<str>,
    registration: McpOAuthRegistration,
    authorization_timeout: Duration,
    maximum_scope_upgrades: u32,
}

impl McpOAuthOptions {
    /// Constructs a native-client authorization-code policy.
    pub fn native(
        redirect_uri: &str,
        client_name: impl Into<String>,
        registration: McpOAuthRegistration,
    ) -> Result<Self, McpOAuthOptionsError> {
        let redirect_uri = validate_redirect_uri(redirect_uri)?;
        let client_name = client_name.into();
        validate_bounded_text(&client_name, MAX_CLIENT_NAME_BYTES)
            .map_err(|()| McpOAuthOptionsError::InvalidClientName)?;
        Ok(Self {
            redirect_uri,
            client_name: client_name.into_boxed_str(),
            registration,
            authorization_timeout: DEFAULT_AUTHORIZATION_TIMEOUT,
            maximum_scope_upgrades: DEFAULT_MAXIMUM_SCOPE_UPGRADES,
        })
    }

    /// Sets the maximum duration of one interactive authorization handoff.
    pub fn with_authorization_timeout(
        mut self,
        timeout: Duration,
    ) -> Result<Self, McpOAuthOptionsError> {
        if timeout.is_zero() || timeout.as_nanos() > HARD_MAXIMUM_AUTHORIZATION_TIMEOUT.as_nanos() {
            return Err(McpOAuthOptionsError::InvalidAuthorizationTimeout);
        }
        self.authorization_timeout = timeout;
        Ok(self)
    }

    /// Sets the process-wide scope-upgrade ceiling retained by the OAuth
    /// manager. The MCP transport separately bounds replay per request.
    pub fn with_maximum_scope_upgrades(
        mut self,
        maximum: u32,
    ) -> Result<Self, McpOAuthOptionsError> {
        if maximum == 0 || maximum > HARD_MAXIMUM_SCOPE_UPGRADES {
            return Err(McpOAuthOptionsError::InvalidScopeUpgradeLimit);
        }
        self.maximum_scope_upgrades = maximum;
        Ok(self)
    }

    fn authorization_request(&self) -> AuthorizationRequest {
        let mut request = AuthorizationRequest::new(self.redirect_uri.as_str())
            .with_client_name(self.client_name.as_ref())
            .with_application_type("native");
        match &self.registration {
            McpOAuthRegistration::Automatic {
                client_metadata_url,
            } => {
                if let Some(url) = client_metadata_url {
                    request = request.with_client_metadata_url(url.as_ref());
                }
            }
            McpOAuthRegistration::PreRegistered {
                client_id,
                client_secret,
            } => {
                request = request.with_preregistered_client(client_id.as_ref());
                if let Some(secret) = client_secret {
                    request = request.with_client_secret(secret.expose_secret());
                }
            }
        }
        request
    }
}

/// Invalid OAuth client policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpOAuthOptionsError {
    /// The redirect URI was invalid or used an unsafe transport.
    #[error("MCP OAuth redirect URI is invalid")]
    InvalidRedirectUri,
    /// The DCR client name was empty, unbounded, or contained controls.
    #[error("MCP OAuth client name is invalid")]
    InvalidClientName,
    /// A pre-registered client ID was empty, unbounded, or contained controls.
    #[error("MCP OAuth client ID is invalid")]
    InvalidClientId,
    /// A CIMD URL was not an HTTPS document URL.
    #[error("MCP OAuth client metadata URL is invalid")]
    InvalidClientMetadataUrl,
    /// The interactive authorization timeout was outside its hard bounds.
    #[error("MCP OAuth authorization timeout is invalid")]
    InvalidAuthorizationTimeout,
    /// The scope-upgrade ceiling was outside its hard bounds.
    #[error("MCP OAuth scope-upgrade limit is invalid")]
    InvalidScopeUpgradeLimit,
}

/// Redacted user-agent handoff for one authorization-code flow.
pub struct McpOAuthUserAuthorizationRequest {
    authorization_url: Box<str>,
    redirect_uri: Box<str>,
}

impl McpOAuthUserAuthorizationRequest {
    /// Returns the validated authorization URL to open in the user agent.
    #[must_use]
    pub fn authorization_url(&self) -> &str {
        &self.authorization_url
    }

    /// Returns the exact registered redirect URI expected in the callback.
    #[must_use]
    pub fn redirect_uri(&self) -> &str {
        &self.redirect_uri
    }
}

impl fmt::Debug for McpOAuthUserAuthorizationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthUserAuthorizationRequest")
            .field("authorization_url", &"[REDACTED]")
            .field("redirect_uri", &"[REDACTED]")
            .finish()
    }
}

/// Public-safe failure from an OAuth user-agent integration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpOAuthUserAgentError {
    /// The user declined or cancelled authorization.
    #[error("MCP OAuth authorization was cancelled")]
    Cancelled,
    /// The user-agent or callback listener was unavailable.
    #[error("MCP OAuth user-agent integration is unavailable")]
    Unavailable,
    /// The callback was absent or malformed.
    #[error("MCP OAuth user-agent returned an invalid callback")]
    InvalidCallback,
}

/// Integrates a browser, device UI, or managed user-agent with OAuth.
pub trait McpOAuthUserAgent: Send + Sync + 'static {
    /// Presents the authorization URL and returns the complete callback URL.
    fn authorize(
        &self,
        request: &McpOAuthUserAuthorizationRequest,
    ) -> BoxFuture<'_, Result<Box<str>, McpOAuthUserAgentError>>;
}

struct McpOAuthState {
    manager: Option<AuthorizationManager>,
    issuer: Option<Box<str>>,
    ready: bool,
}

/// Single-resource OAuth provider for [`crate::McpClient`].
pub struct McpOAuthAuthorization {
    resource: McpOAuthResource,
    options: McpOAuthOptions,
    user_agent: Arc<dyn McpOAuthUserAgent>,
    state: Mutex<McpOAuthState>,
}

impl McpOAuthAuthorization {
    /// Creates an in-memory OAuth binding. Use [`Self::new_with_stores`] when
    /// credentials and pending authorization state must survive a restart.
    pub async fn new(
        resource: McpOAuthResource,
        options: McpOAuthOptions,
        user_agent: Arc<dyn McpOAuthUserAgent>,
    ) -> Result<Self, McpAuthorizationError> {
        let manager = AuthorizationManager::new(resource.as_url().as_str())
            .await
            .map_err(|error| map_auth_error(&error))?;
        Ok(Self::from_manager(resource, options, user_agent, manager))
    }

    /// Creates an OAuth binding with caller-owned durable credential and PKCE
    /// state stores. Store implementations are responsible for encryption,
    /// tenant isolation, atomic writes, and TTL expiry of abandoned states.
    pub async fn new_with_stores<C, S>(
        resource: McpOAuthResource,
        options: McpOAuthOptions,
        user_agent: Arc<dyn McpOAuthUserAgent>,
        credential_store: C,
        state_store: S,
    ) -> Result<Self, McpAuthorizationError>
    where
        C: McpOAuthCredentialStore + 'static,
        S: McpOAuthStateStore + 'static,
    {
        let mut manager = AuthorizationManager::new(resource.as_url().as_str())
            .await
            .map_err(|error| map_auth_error(&error))?;
        manager.set_credential_store(credential_store);
        manager.set_state_store(state_store);
        Ok(Self::from_manager(resource, options, user_agent, manager))
    }

    fn from_manager(
        resource: McpOAuthResource,
        options: McpOAuthOptions,
        user_agent: Arc<dyn McpOAuthUserAgent>,
        mut manager: AuthorizationManager,
    ) -> Self {
        let mut scope_upgrade = ScopeUpgradeConfig::default();
        scope_upgrade.max_upgrade_attempts = options.maximum_scope_upgrades;
        scope_upgrade.auto_upgrade = true;
        manager.set_scope_upgrade_config(scope_upgrade);
        Self {
            resource,
            options,
            user_agent,
            state: Mutex::new(McpOAuthState {
                manager: Some(manager),
                issuer: None,
                ready: false,
            }),
        }
    }

    async fn handle_bearer_challenge(
        &self,
        challenge: &McpClientAuthorizationChallenge,
    ) -> Result<McpClientAuthorizationRetry, McpAuthorizationError> {
        let raw = challenge
            .bearer()
            .ok_or(McpAuthorizationError::PermissionDenied)?;
        let parameters = WWWAuthenticateParams::parse(raw, self.resource.as_url());
        if challenge.status() == McpClientAuthorizationChallengeStatus::Forbidden
            && (!parameters.is_insufficient_scope() || parameters.scope.as_deref().is_none())
        {
            return Ok(McpClientAuthorizationRetry::Decline);
        }

        let mut state = self.state.lock().await;
        let mut manager = state
            .manager
            .take()
            .ok_or(McpAuthorizationError::Unavailable)?;

        let resolution = match manager.resolve_metadata_from_challenge(Some(raw)).await {
            Ok(resolution) => resolution,
            Err(error) => {
                state.manager = Some(manager);
                return Err(map_auth_error(&error));
            }
        };
        let issuer = resolution
            .metadata
            .issuer
            .clone()
            .map(String::into_boxed_str);
        let issuer_changed = state.ready && state.issuer.as_deref() != issuer.as_deref();
        manager.set_metadata(resolution.metadata);

        if !state.ready {
            match manager.initialize_from_store().await {
                Ok(true) => {
                    state.manager = Some(manager);
                    state.issuer = issuer;
                    state.ready = true;
                    return Ok(McpClientAuthorizationRetry::Retry);
                }
                Ok(false) => {}
                Err(error) => {
                    state.manager = Some(manager);
                    return Err(map_auth_error(&error));
                }
            }
        }

        let session = match self
            .start_session(
                manager,
                state.ready && !issuer_changed,
                parameters.scope.as_deref(),
            )
            .await
        {
            Ok(session) => session,
            Err((manager, error)) => {
                state.manager = Some(manager);
                return Err(error);
            }
        };
        let manager = match self.complete_session(session).await {
            Ok(manager) => manager,
            Err((manager, error)) => {
                state.manager = Some(manager);
                return Err(error);
            }
        };
        state.manager = Some(manager);
        state.issuer = issuer;
        state.ready = true;
        Ok(McpClientAuthorizationRetry::Retry)
    }

    async fn start_session(
        &self,
        manager: AuthorizationManager,
        reuse_registration: bool,
        challenged_scope: Option<&str>,
    ) -> Result<AuthorizationSession, (AuthorizationManager, McpAuthorizationError)> {
        if !reuse_registration {
            return AuthorizationSession::new(manager, self.options.authorization_request())
                .await
                .map_err(|(manager, error)| (manager, map_auth_error(&error)));
        }

        let required_scope = challenged_scope.unwrap_or("");
        let authorization_url = if required_scope.is_empty() {
            match manager.get_authorization_url(&[]).await {
                Ok(url) => url,
                Err(error) => return Err((manager, map_auth_error(&error))),
            }
        } else {
            match manager.request_scope_upgrade(required_scope).await {
                Ok(url) => url,
                Err(error) => return Err((manager, map_auth_error(&error))),
            }
        };
        Ok(AuthorizationSession::for_scope_upgrade(
            manager,
            authorization_url,
            self.options.redirect_uri.as_str(),
        ))
    }

    async fn complete_session(
        &self,
        session: AuthorizationSession,
    ) -> Result<AuthorizationManager, (AuthorizationManager, McpAuthorizationError)> {
        let Ok(authorization_url) =
            validate_authorization_url(session.get_authorization_url(), self.resource.local_http)
        else {
            return Err((
                session.auth_manager,
                McpAuthorizationError::PermissionDenied,
            ));
        };
        let handoff = McpOAuthUserAuthorizationRequest {
            authorization_url: authorization_url.as_str().to_owned().into_boxed_str(),
            redirect_uri: self
                .options
                .redirect_uri
                .as_str()
                .to_owned()
                .into_boxed_str(),
        };
        let callback = match tokio::time::timeout(
            self.options.authorization_timeout,
            self.user_agent.authorize(&handoff),
        )
        .await
        {
            Ok(Ok(callback)) => callback,
            Ok(Err(McpOAuthUserAgentError::Unavailable)) | Err(_) => {
                return Err((session.auth_manager, McpAuthorizationError::Unavailable));
            }
            Ok(Err(
                McpOAuthUserAgentError::Cancelled | McpOAuthUserAgentError::InvalidCallback,
            )) => {
                return Err((
                    session.auth_manager,
                    McpAuthorizationError::PermissionDenied,
                ));
            }
        };
        if !callback_matches_redirect(&callback, &self.options.redirect_uri) {
            return Err((
                session.auth_manager,
                McpAuthorizationError::PermissionDenied,
            ));
        }
        if let Err(error) = session.handle_callback_url(&callback).await {
            return Err((session.auth_manager, map_auth_error(&error)));
        }
        Ok(session.auth_manager)
    }
}

impl fmt::Debug for McpOAuthAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpOAuthAuthorization")
            .field("resource", &self.resource)
            .field("options", &self.options)
            .field("credentials", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl McpClientAuthorizationProvider for McpOAuthAuthorization {
    fn resolve(
        &self,
        _request: &McpClientAuthorizationRequest,
    ) -> BoxFuture<'_, Result<McpAuthorization, McpAuthorizationError>> {
        Box::pin(async move {
            let state = self.state.lock().await;
            if !state.ready {
                return Ok(McpAuthorization::Anonymous);
            }
            let manager = state
                .manager
                .as_ref()
                .ok_or(McpAuthorizationError::Unavailable)?;
            match manager.get_access_token().await {
                Ok(token) => ApiKey::new(token)
                    .map(McpAuthorization::Bearer)
                    .map_err(|_| McpAuthorizationError::PermissionDenied),
                Err(AuthError::AuthorizationRequired | AuthError::TokenExpired) => {
                    Ok(McpAuthorization::Anonymous)
                }
                Err(error) => Err(map_auth_error(&error)),
            }
        })
    }

    fn handle_challenge<'a>(
        &'a self,
        _request: &'a McpClientAuthorizationRequest,
        challenge: &'a McpClientAuthorizationChallenge,
    ) -> BoxFuture<'a, Result<McpClientAuthorizationRetry, McpAuthorizationError>> {
        Box::pin(async move { self.handle_bearer_challenge(challenge).await })
    }
}

fn validate_redirect_uri(value: &str) -> Result<Url, McpOAuthOptionsError> {
    if value.is_empty() || value.len() > MAX_OAUTH_URL_BYTES {
        return Err(McpOAuthOptionsError::InvalidRedirectUri);
    }
    let url = Url::parse(value).map_err(|_| McpOAuthOptionsError::InvalidRedirectUri)?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(McpOAuthOptionsError::InvalidRedirectUri);
    }
    let secure = url.scheme() == "https";
    let loopback = url.scheme() == "http" && url.host_str().is_some_and(is_loopback_host);
    if !secure && !loopback {
        return Err(McpOAuthOptionsError::InvalidRedirectUri);
    }
    Ok(url)
}

fn validate_client_metadata_url(value: &str) -> Result<(), McpOAuthOptionsError> {
    if value.is_empty() || value.len() > MAX_OAUTH_URL_BYTES {
        return Err(McpOAuthOptionsError::InvalidClientMetadataUrl);
    }
    let url = Url::parse(value).map_err(|_| McpOAuthOptionsError::InvalidClientMetadataUrl)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || matches!(url.path(), "" | "/")
    {
        return Err(McpOAuthOptionsError::InvalidClientMetadataUrl);
    }
    Ok(())
}

fn validate_authorization_url(value: &str, local_http: bool) -> Result<Url, ()> {
    if value.is_empty() || value.len() > MAX_OAUTH_URL_BYTES {
        return Err(());
    }
    let url = Url::parse(value).map_err(|_| ())?;
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(());
    }
    if url.scheme() == "https"
        || (local_http && url.scheme() == "http" && url.host_str().is_some_and(is_loopback_host))
    {
        Ok(url)
    } else {
        Err(())
    }
}

fn callback_matches_redirect(callback: &str, expected: &Url) -> bool {
    if callback.is_empty() || callback.len() > MAX_OAUTH_URL_BYTES {
        return false;
    }
    let Ok(callback) = Url::parse(callback) else {
        return false;
    };
    callback.scheme() == expected.scheme()
        && callback
            .host_str()
            .zip(expected.host_str())
            .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right))
        && callback.port_or_known_default() == expected.port_or_known_default()
        && callback.path() == expected.path()
        && callback.username().is_empty()
        && callback.password().is_none()
        && callback.fragment().is_none()
}

fn validate_bounded_text(value: &str, maximum: usize) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > maximum
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        Err(())
    } else {
        Ok(())
    }
}

fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_end_matches('.');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn map_auth_error(error: &AuthError) -> McpAuthorizationError {
    match error {
        AuthError::HttpError(_)
        | AuthError::TokenRefreshFailed(_)
        | AuthError::CredentialStoreError(_)
        | AuthError::InternalError(_) => McpAuthorizationError::Unavailable,
        _ => McpAuthorizationError::PermissionDenied,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_and_redirect_policies_fail_closed() {
        assert!(McpOAuthResource::https("https://mcp.example.com/mcp").is_ok());
        assert_eq!(
            McpOAuthResource::https("http://mcp.example.com/mcp"),
            Err(McpOAuthResourceError::HttpsRequired)
        );
        assert!(McpOAuthResource::loopback_http("http://127.0.0.1:8000/mcp").is_ok());
        assert!(McpOAuthResource::loopback_http("http://localhost:8000/mcp").is_ok());
        assert_eq!(
            McpOAuthResource::loopback_http("http://10.0.0.1/mcp"),
            Err(McpOAuthResourceError::LoopbackHttpRequired)
        );

        assert!(validate_redirect_uri("http://127.0.0.1:49152/callback").is_ok());
        assert!(validate_redirect_uri("https://agent.example.com/callback").is_ok());
        assert!(validate_redirect_uri("http://agent.example.com/callback").is_err());
        assert!(validate_redirect_uri("https://agent.example.com/callback?fixed=1").is_err());
        assert!(McpOAuthResource::loopback_http("http://tenant.localhost/mcp").is_err());
    }

    #[test]
    fn callback_binding_is_exact_for_origin_and_path() {
        let expected = Url::parse("http://localhost:3000/callback").unwrap();
        assert!(callback_matches_redirect(
            "http://localhost:3000/callback?code=one&state=two",
            &expected
        ));
        assert!(!callback_matches_redirect(
            "http://localhost:3001/callback?code=one&state=two",
            &expected
        ));
        assert!(!callback_matches_redirect(
            "http://localhost:3000/other?code=one&state=two",
            &expected
        ));
    }

    #[test]
    fn debug_surfaces_redact_identity_material() {
        let registration = McpOAuthRegistration::pre_registered(
            "sensitive-client-id",
            Some(ApiKey::new("sensitive-client-secret").unwrap()),
        )
        .unwrap();
        let debug = format!("{registration:?}");
        assert!(!debug.contains("sensitive-client-id"));
        assert!(!debug.contains("sensitive-client-secret"));
    }
}
