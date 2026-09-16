// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Strict RFC 7662 access-token profile. Online verification is not JWT/JWKS
//! validation or a replacement for the mandatory AgentService resource policy.
//!
//! ```no_run
//! use std::{sync::Arc, time::Duration};
//! use stateknot::{agent_http::introspection::*, core::{IssuerId, PrincipalIdentity, TenantId}};
//!
//! fn verifier(
//!     issuer: IssuerId, principal: PrincipalIdentity, tenant: TenantId,
//!     delivered_secret: String,
//! ) -> Result<Arc<AgentHttpIntrospection>, IntrospectionConfigurationError> {
//!     use stateknot::agent_http::AgentHttpOperation;
//!     // These bindings come from the trusted control plane, never token claims.
//!     let policy = Arc::new(TenantPolicy::new(vec![TenantBinding::new(
//!         tenant, principal, &[AgentHttpOperation::Read],
//!     )], Duration::from_secs(300))?);
//!     let options = IntrospectionOptions::new(
//!         "https://identity.example.com/oauth/introspect", issuer,
//!         "stateknot-http".into(), "ingress-introspection".into(),
//!         ["stateknot:submit".into(), "stateknot:read".into(), "stateknot:cancel".into()],
//!     )?;
//!     Ok(Arc::new(AgentHttpIntrospection::new(
//!         options, ClientSecret::new(delivered_secret)?, policy,
//!     )?))
//! }
//! // Compose verifier.check() with the actual resource-policy readiness.
//! // AgentServiceAuthorizer is still mandatory for every resource operation.
//! ```

mod claims;
mod policy;
pub use policy::{TenantBinding, TenantPolicy};

use super::{
    AgentHttpAuthenticationError, AgentHttpAuthenticator, AgentHttpCredential, AgentHttpPrincipal,
    AgentHttpReadiness, AgentHttpReadinessError,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{Client, Url, header};
use serde_json::Value;
use stateknot_core::{BoundedJson, BoxFuture, EventId, IssuerId, JsonLimits};
use std::{
    fmt,
    sync::{Arc, RwLock},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

const MAX_RESPONSE: usize = 64 * 1024;

/// Closed configuration/replacement error; never embeds secrets or endpoint text.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invalid introspection configuration or stale replacement")]
pub struct IntrospectionConfigurationError;

/// Bounded, redacted, zeroized owned confidential-client secret.
/// HTTP/TLS library copies are outside this wrapper's ownership.
#[derive(Clone)]
pub struct ClientSecret(Zeroizing<String>);

impl ClientSecret {
    /// Validates a nonempty secret up to 4096 UTF-8 bytes, without control bytes.
    ///
    /// # Errors
    /// Rejects empty, oversized or control-containing secrets.
    pub fn new(value: String) -> Result<Self, IntrospectionConfigurationError> {
        let value = Zeroizing::new(value);
        if value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
            return Err(IntrospectionConfigurationError);
        }
        Ok(Self(value))
    }
}

impl fmt::Debug for ClientSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientSecret([REDACTED])")
    }
}

/// Explicit fixed trust configuration. No URL is selected from bearer claims.
/// Default limits: 3 seconds total, 32 concurrent requests, 64 KiB response,
/// access-token lifetime at most one hour; no positive clock-skew allowance.
#[derive(Clone)]
pub struct IntrospectionOptions {
    endpoint: Url,
    issuer: IssuerId,
    audience: String,
    client_id: String,
    required_scopes: [String; 3],
    deadline: Duration,
    max_in_flight: usize,
    max_token_lifetime: Duration,
    roots: Vec<reqwest::Certificate>,
}

impl IntrospectionOptions {
    /// Configures an exact HTTPS endpoint, expected issuer/audience, confidential
    /// client ID, and distinct submit/read/cancel scopes in that order.
    ///
    /// # Errors
    /// Rejects insecure/ambiguous URLs or unbounded/invalid identifiers/scopes.
    pub fn new(
        endpoint: &str,
        issuer: IssuerId,
        audience: String,
        client_id: String,
        required_scopes: [String; 3],
    ) -> Result<Self, IntrospectionConfigurationError> {
        if endpoint
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err(IntrospectionConfigurationError);
        }
        let endpoint = Url::parse(endpoint).map_err(|_| IntrospectionConfigurationError)?;
        if endpoint.as_str().len() > 2048
            || endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || [&audience, &client_id]
                .iter()
                .any(|v| v.is_empty() || v.len() > 512 || v.chars().any(char::is_control))
            || required_scopes.iter().any(|s| !claims::valid_scope(s))
            || required_scopes[0] == required_scopes[1]
            || required_scopes[1] == required_scopes[2]
            || required_scopes[0] == required_scopes[2]
        {
            return Err(IntrospectionConfigurationError);
        }
        Ok(Self {
            endpoint,
            issuer,
            audience,
            client_id,
            required_scopes,
            deadline: Duration::from_secs(3),
            max_in_flight: 32,
            max_token_lifetime: Duration::from_secs(3600),
            roots: Vec::new(),
        })
    }

    /// Configures finite deadline (at most 10 s), concurrent requests (1..=256),
    /// and maximum access-token lifetime (whole seconds, at most 24 hours).
    ///
    /// # Errors
    /// Rejects zero, excessive or fractional token-lifetime limits.
    pub fn with_limits(
        mut self,
        deadline: Duration,
        max_in_flight: usize,
        max_token_lifetime: Duration,
    ) -> Result<Self, IntrospectionConfigurationError> {
        if deadline.is_zero()
            || deadline > Duration::from_secs(10)
            || !(1..=256).contains(&max_in_flight)
            || max_token_lifetime.is_zero()
            || max_token_lifetime > Duration::from_secs(86400)
            || max_token_lifetime.subsec_nanos() != 0
        {
            return Err(IntrospectionConfigurationError);
        }
        self.deadline = deadline;
        self.max_in_flight = max_in_flight;
        self.max_token_lifetime = max_token_lifetime;
        Ok(self)
    }

    /// Adds one explicitly trusted PEM CA certificate (up to 16 KiB, 8 roots).
    /// Public system roots remain trusted. Certificate/hostname checks stay on.
    ///
    /// # Errors
    /// Rejects malformed/excessive certificates.
    pub fn with_root_certificate(
        mut self,
        pem: &[u8],
    ) -> Result<Self, IntrospectionConfigurationError> {
        if pem.len() > 16 * 1024 || self.roots.len() >= 8 {
            return Err(IntrospectionConfigurationError);
        }
        let mut certificates = reqwest::Certificate::from_pem_bundle(pem)
            .map_err(|_| IntrospectionConfigurationError)?;
        if certificates.len() != 1 {
            return Err(IntrospectionConfigurationError);
        }
        self.roots.push(certificates.remove(0));
        Ok(self)
    }
}

/// Online introspection verifier with bounded transport and default-deny mapping.
/// Share one Arc across authentication/readiness so limits and secrets agree.
pub struct AgentHttpIntrospection {
    client: Client,
    options: IntrospectionOptions,
    secret: RwLock<ClientSecret>,
    policy: Arc<TenantPolicy>,
    permits: Semaphore,
}

impl AgentHttpIntrospection {
    /// Builds a strict HTTPS client. No discovery, environment proxy, redirect,
    /// HTTP retry, decompression, token cache or background task is enabled.
    ///
    /// # Errors
    /// Fails if the TLS/HTTP client cannot be built.
    pub fn new(
        options: IntrospectionOptions,
        secret: ClientSecret,
        policy: Arc<TenantPolicy>,
    ) -> Result<Self, IntrospectionConfigurationError> {
        let mut builder = Client::builder()
            .https_only(true)
            .http1_only()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(options.deadline)
            .timeout(options.deadline)
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(2)
            .min_tls_version(reqwest::tls::Version::TLS_1_2);
        for root in &options.roots {
            builder = builder.add_root_certificate(root.clone());
        }
        let client = builder
            .build()
            .map_err(|_| IntrospectionConfigurationError)?;
        Ok(Self {
            client,
            permits: Semaphore::new(options.max_in_flight),
            options,
            secret: RwLock::new(secret),
            policy,
        })
    }

    /// Publishes a secret supplied by the trusted secret-delivery mechanism.
    /// Already-started attempts can complete with the old secret. No retries.
    ///
    /// # Errors
    /// Fails closed if the credential lock is poisoned.
    pub fn replace_client_secret(
        &self,
        secret: ClientSecret,
    ) -> Result<(), IntrospectionConfigurationError> {
        *self
            .secret
            .write()
            .map_err(|_| IntrospectionConfigurationError)? = secret;
        Ok(())
    }

    async fn introspect(&self, token: &str) -> Result<Value, AgentHttpAuthenticationError> {
        let unavailable = AgentHttpAuthenticationError::Unavailable;
        let _permit = self.permits.try_acquire().map_err(|_| unavailable)?;
        tokio::time::timeout(self.options.deadline, async {
            let authorization = {
                let secret = self.secret.read().map_err(|_| unavailable)?;
                let encoded = Zeroizing::new(format!(
                    "{}:{}",
                    form_encode(&self.options.client_id).as_str(),
                    form_encode(&secret.0).as_str()
                ));
                let basic =
                    Zeroizing::new(format!("Basic {}", STANDARD.encode(encoded.as_bytes())));
                let mut value = header::HeaderValue::from_str(&basic).map_err(|_| unavailable)?;
                value.set_sensitive(true);
                value
            };
            let mut response = self
                .client
                .post(self.options.endpoint.clone())
                .header(header::AUTHORIZATION, authorization)
                .header(header::ACCEPT, "application/json")
                .form(&[("token", token), ("token_type_hint", "access_token")])
                .send()
                .await
                .map_err(|_| unavailable)?;
            if response.status() != reqwest::StatusCode::OK
                || response.headers().contains_key(header::CONTENT_ENCODING)
            {
                return Err(unavailable);
            }
            let types: Vec<_> = response
                .headers()
                .get_all(header::CONTENT_TYPE)
                .iter()
                .collect();
            if types.len() != 1 {
                return Err(unavailable);
            }
            let mime: mime::Mime = types[0]
                .to_str()
                .map_err(|_| unavailable)?
                .parse()
                .map_err(|_| unavailable)?;
            if mime.essence_str() != "application/json"
                || mime
                    .params()
                    .any(|(k, v)| k != mime::CHARSET || v != "utf-8")
            {
                return Err(unavailable);
            }
            if response
                .content_length()
                .is_some_and(|n| n > MAX_RESPONSE as u64)
            {
                return Err(unavailable);
            }
            let mut bytes = Zeroizing::new(Vec::new());
            while let Some(chunk) = response.chunk().await.map_err(|_| unavailable)? {
                if chunk.len() > MAX_RESPONSE - bytes.len() {
                    return Err(unavailable);
                }
                bytes.extend_from_slice(&chunk);
            }
            let limits = JsonLimits::try_new(MAX_RESPONSE, 8, 128, 1024, 8192, 128)
                .map_err(|_| unavailable)?;
            let value =
                BoundedJson::from_slice_with_limits(&bytes, limits).map_err(|_| unavailable)?;
            if !value.as_value().is_object() || !value.as_value()["active"].is_boolean() {
                return Err(unavailable);
            }
            Ok(value.as_value().clone())
        })
        .await
        .map_err(|_| unavailable)?
    }
}

impl AgentHttpAuthenticator for AgentHttpIntrospection {
    fn authenticate(
        &self,
        credential: AgentHttpCredential,
    ) -> BoxFuture<'_, Result<AgentHttpPrincipal, AgentHttpAuthenticationError>> {
        Box::pin(async move {
            self.policy.check()?;
            let response = self.introspect(credential.expose_secret()).await?;
            let (principal, scopes) = claims::verify(&response, &self.options)?;
            self.policy
                .resolve(&principal, &scopes, &self.options.required_scopes)
        })
    }
}

impl AgentHttpReadiness for AgentHttpIntrospection {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentHttpReadinessError>> {
        Box::pin(async move {
            self.policy.check().map_err(|_| AgentHttpReadinessError)?;
            let canary = format!("stateknot-negative-canary-{}", EventId::generate());
            let response = self
                .introspect(&canary)
                .await
                .map_err(|_| AgentHttpReadinessError)?;
            if response["active"] != false {
                return Err(AgentHttpReadinessError);
            }
            self.policy.check().map_err(|_| AgentHttpReadinessError)
        })
    }
}

// RFC 6749 section 2.3.1: form-encode each credential before Basic encoding.
fn form_encode(value: &str) -> Zeroizing<String> {
    let mut result = Zeroizing::new(String::new());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                result.push(char::from(byte));
            }
            b' ' => result.push('+'),
            _ => {
                use std::fmt::Write;
                let _ = write!(result, "%{byte:02X}");
            }
        }
    }
    result
}

#[cfg(test)]
mod tests;
