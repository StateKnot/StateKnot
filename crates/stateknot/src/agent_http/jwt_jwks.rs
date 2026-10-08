// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Locally verified RFC 9068 access tokens with operator-provisioned JWKS.
//! This profile accepts only `at+jwt` RS256 access tokens; it never discovers
//! keys from token headers or treats a signed token as resource authorization.
//! The trusted control plane must refresh the bounded JWKS and tenant policy
//! before their independent leases expire. Use online introspection when
//! immediate per-token revocation is required.
//!
//! ```no_run
//! use std::{sync::Arc, time::Duration};
//! use stateknot::{agent_http::{AgentHttpOperation, jwt_jwks::*},
//!     core::{PrincipalIdentity, TenantId}};
//!
//! fn verifier(verified_jwks: &[u8], principal: PrincipalIdentity, tenant: TenantId)
//!     -> Result<AgentHttpJwtJwks, Box<dyn std::error::Error>> {
//!     let options = JwtJwksOptions::new(principal.issuer().clone(), "agents".into(),
//!         ["submit".into(), "read".into(), "cancel".into()])?;
//!     let policy = Arc::new(TenantPolicy::new(vec![TenantBinding::new(
//!         tenant, principal, &[AgentHttpOperation::Read],
//!     )], Duration::from_secs(300))?);
//!     Ok(AgentHttpJwtJwks::new(options, verified_jwks, Duration::from_secs(300), policy)?)
//! }
//! // Share an Arc of this verifier with HTTP authentication and compose its
//! // readiness with the mandatory resource-policy and actual store readiness.
//! ```

pub use super::introspection::{TenantBinding, TenantPolicy};

use super::{
    AgentHttpAuthenticationError, AgentHttpAuthenticator, AgentHttpCredential, AgentHttpPrincipal,
    AgentHttpReadiness, AgentHttpReadinessError, introspection::claims::valid_scope,
};
use aws_lc_rs::{
    encoding::AsDer,
    signature::{ParsedPublicKey, RSA_PKCS1_2048_8192_SHA256, RsaPublicKeyComponents},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::Value;
use stateknot_core::{BoundedJson, BoxFuture, IssuerId, JsonLimits, PrincipalIdentity};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{sync::Semaphore, time::Instant};

/// A configuration or stale keyset-replacement failure. No key or token text is exposed.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invalid JWT/JWKS configuration or stale replacement")]
pub struct JwtJwksConfigurationError;

/// Exact issuer/audience/scope and bounded verification policy for one resource.
#[derive(Clone)]
pub struct JwtJwksOptions {
    issuer: IssuerId,
    audience: String,
    required_scopes: [String; 3],
    host_inspection_scope: Option<String>,
    max_token_lifetime: Duration,
    max_in_flight: usize,
    deadline: Duration,
}

impl JwtJwksOptions {
    /// Creates a fixed, RS256-only RFC 9068 resource profile. Scopes are ordered
    /// submit/read/cancel and must be distinct. The default token lifetime is
    /// fifteen minutes, with at most 32 concurrent signature checks.
    pub fn new(
        issuer: IssuerId,
        audience: String,
        required_scopes: [String; 3],
    ) -> Result<Self, JwtJwksConfigurationError> {
        if audience.is_empty()
            || audience.len() > 512
            || audience.chars().any(char::is_control)
            || required_scopes.iter().any(|scope| !valid_scope(scope))
            || required_scopes[0] == required_scopes[1]
            || required_scopes[0] == required_scopes[2]
            || required_scopes[1] == required_scopes[2]
        {
            return Err(JwtJwksConfigurationError);
        }
        Ok(Self {
            issuer,
            audience,
            required_scopes,
            host_inspection_scope: None,
            max_token_lifetime: Duration::from_secs(900),
            max_in_flight: 32,
            deadline: Duration::from_secs(3),
        })
    }

    /// Adds a distinct opt-in host-inspection scope. The separate operations
    /// policy remains mandatory even when a token carries this scope.
    pub fn with_host_inspection_scope(
        mut self,
        scope: String,
    ) -> Result<Self, JwtJwksConfigurationError> {
        if !valid_scope(&scope) || self.required_scopes.contains(&scope) {
            return Err(JwtJwksConfigurationError);
        }
        self.host_inspection_scope = Some(scope);
        Ok(self)
    }

    /// Sets a whole-second token lifetime (1..=3600), a finite verification
    /// deadline (1 ms..=10 s), and signature concurrency (1..=64).
    pub fn with_limits(
        mut self,
        max_token_lifetime: Duration,
        deadline: Duration,
        max_in_flight: usize,
    ) -> Result<Self, JwtJwksConfigurationError> {
        if max_token_lifetime.is_zero()
            || max_token_lifetime > Duration::from_secs(3600)
            || max_token_lifetime.subsec_nanos() != 0
            || deadline < Duration::from_millis(1)
            || deadline > Duration::from_secs(10)
            || !(1..=64).contains(&max_in_flight)
        {
            return Err(JwtJwksConfigurationError);
        }
        self.max_token_lifetime = max_token_lifetime;
        self.deadline = deadline;
        self.max_in_flight = max_in_flight;
        Ok(self)
    }
}

struct KeySnapshot {
    generation: u64,
    expires: Instant,
    keys: BTreeMap<String, ParsedPublicKey>,
}

/// Bounded, local signature verifier. JWKS bytes must arrive over an independently
/// authenticated trusted control plane; this type does not fetch discovery URLs.
/// Keyset replacement is an explicit generation-checked operation, including
/// an empty replacement to revoke all signing keys.
pub struct AgentHttpJwtJwks {
    options: JwtJwksOptions,
    policy: Arc<TenantPolicy>,
    keys: RwLock<KeySnapshot>,
    permits: Arc<Semaphore>,
}

impl AgentHttpJwtJwks {
    /// Maximum JWKS freshness lease. The control plane must refresh before expiry.
    pub const MAX_KEY_LEASE: Duration = Duration::from_secs(3600);

    /// Loads the first verified public-key snapshot. `jwks` is bounded to 16 KiB
    /// and 16 distinct RS256 public keys; private-key material is rejected.
    pub fn new(
        options: JwtJwksOptions,
        jwks: &[u8],
        lease: Duration,
        policy: Arc<TenantPolicy>,
    ) -> Result<Self, JwtJwksConfigurationError> {
        let keys = parse_jwks(jwks)?;
        if keys.is_empty() || !valid_lease(lease) {
            return Err(JwtJwksConfigurationError);
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(options.max_in_flight)),
            options,
            policy,
            keys: RwLock::new(KeySnapshot {
                generation: 1,
                expires: Instant::now() + lease,
                keys,
            }),
        })
    }

    /// Atomically publishes a fully validated fresh JWKS from the trusted
    /// control plane. Empty `keys` intentionally disables authentication.
    /// A failed parse, stale generation or poisoned lock retains the old set.
    pub fn replace_jwks(
        &self,
        expected_generation: u64,
        jwks: &[u8],
        lease: Duration,
    ) -> Result<u64, JwtJwksConfigurationError> {
        if !valid_lease(lease) {
            return Err(JwtJwksConfigurationError);
        }
        let keys = parse_jwks(jwks)?;
        let next = expected_generation
            .checked_add(1)
            .ok_or(JwtJwksConfigurationError)?;
        let mut current = self.keys.write().map_err(|_| JwtJwksConfigurationError)?;
        if current.generation != expected_generation {
            return Err(JwtJwksConfigurationError);
        }
        *current = KeySnapshot {
            generation: next,
            expires: Instant::now() + lease,
            keys,
        };
        Ok(next)
    }

    /// Returns the process-local keyset CAS generation, not proof of freshness.
    pub fn generation(&self) -> Result<u64, JwtJwksConfigurationError> {
        self.keys
            .read()
            .map(|snapshot| snapshot.generation)
            .map_err(|_| JwtJwksConfigurationError)
    }

    fn select_key(
        &self,
        kid: &str,
    ) -> Result<(ParsedPublicKey, u64), AgentHttpAuthenticationError> {
        let snapshot = self
            .keys
            .read()
            .map_err(|_| AgentHttpAuthenticationError::Unavailable)?;
        if Instant::now() >= snapshot.expires {
            return Err(AgentHttpAuthenticationError::Unavailable);
        }
        snapshot
            .keys
            .get(kid)
            .cloned()
            .map(|key| (key, snapshot.generation))
            .ok_or(AgentHttpAuthenticationError::Unauthenticated)
    }

    fn check_generation(&self, generation: u64) -> Result<(), AgentHttpAuthenticationError> {
        let snapshot = self
            .keys
            .read()
            .map_err(|_| AgentHttpAuthenticationError::Unavailable)?;
        if snapshot.generation != generation || Instant::now() >= snapshot.expires {
            return Err(AgentHttpAuthenticationError::Unavailable);
        }
        Ok(())
    }

    fn check_keys(&self) -> Result<(), AgentHttpReadinessError> {
        let snapshot = self.keys.read().map_err(|_| AgentHttpReadinessError)?;
        if snapshot.keys.is_empty() || Instant::now() >= snapshot.expires {
            return Err(AgentHttpReadinessError);
        }
        Ok(())
    }
}

impl AgentHttpAuthenticator for AgentHttpJwtJwks {
    fn authenticate(
        &self,
        credential: AgentHttpCredential,
    ) -> BoxFuture<'_, Result<AgentHttpPrincipal, AgentHttpAuthenticationError>> {
        Box::pin(async move {
            self.policy.check()?;
            let kid = parse_header(credential.expose_secret())?;
            let (key, generation) = self.select_key(&kid)?;
            let permit = self
                .permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| AgentHttpAuthenticationError::Unavailable)?;
            let options = self.options.clone();
            let mut verified = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                verify_token(credential.expose_secret(), &key, &options)
            });
            let claims = if let Ok(result) =
                tokio::time::timeout(self.options.deadline, &mut verified).await
            {
                result.map_err(|_| AgentHttpAuthenticationError::Unavailable)??
            } else {
                verified.abort();
                return Err(AgentHttpAuthenticationError::Unavailable);
            };
            check_times(claims.exp, claims.iat, claims.nbf, &self.options)?;
            self.check_generation(generation)?;
            self.policy.resolve(
                &claims.principal,
                &claims.scopes,
                &self.options.required_scopes,
                self.options.host_inspection_scope.as_deref(),
            )
        })
    }
}

impl AgentHttpReadiness for AgentHttpJwtJwks {
    fn check(&self) -> BoxFuture<'_, Result<(), AgentHttpReadinessError>> {
        Box::pin(async move {
            self.policy.check().map_err(|_| AgentHttpReadinessError)?;
            self.check_keys()?;
            self.policy.check().map_err(|_| AgentHttpReadinessError)
        })
    }
}

fn valid_lease(lease: Duration) -> bool {
    !lease.is_zero() && lease <= AgentHttpJwtJwks::MAX_KEY_LEASE
}

fn parse_jwks(jwks: &[u8]) -> Result<BTreeMap<String, ParsedPublicKey>, JwtJwksConfigurationError> {
    let limits = JsonLimits::try_new(16 * 1024, 8, 32, 256, 8192, 128)
        .map_err(|_| JwtJwksConfigurationError)?;
    let parsed =
        BoundedJson::from_slice_with_limits(jwks, limits).map_err(|_| JwtJwksConfigurationError)?;
    let root = parsed
        .as_value()
        .as_object()
        .ok_or(JwtJwksConfigurationError)?;
    if root.len() != 1 {
        return Err(JwtJwksConfigurationError);
    }
    let entries = root
        .get("keys")
        .and_then(Value::as_array)
        .ok_or(JwtJwksConfigurationError)?;
    if entries.len() > 16 {
        return Err(JwtJwksConfigurationError);
    }
    let mut keys = BTreeMap::new();
    for entry in entries {
        let jwk = entry.as_object().ok_or(JwtJwksConfigurationError)?;
        if jwk.get("kty").and_then(Value::as_str) != Some("RSA")
            || jwk.get("alg").and_then(Value::as_str) != Some("RS256")
            || jwk.get("use").and_then(Value::as_str) != Some("sig")
            || ["d", "p", "q", "dp", "dq", "qi", "oth", "k"]
                .into_iter()
                .any(|name| jwk.contains_key(name))
            || jwk
                .get("key_ops")
                .is_some_and(|ops| ops != &serde_json::json!(["verify"]))
        {
            return Err(JwtJwksConfigurationError);
        }
        let kid = jwk
            .get("kid")
            .and_then(Value::as_str)
            .filter(|kid| valid_kid(kid))
            .ok_or(JwtJwksConfigurationError)?;
        let modulus = jwk
            .get("n")
            .and_then(Value::as_str)
            .ok_or(JwtJwksConfigurationError)?;
        let exponent = jwk
            .get("e")
            .and_then(Value::as_str)
            .ok_or(JwtJwksConfigurationError)?;
        let n = URL_SAFE_NO_PAD
            .decode(modulus)
            .map_err(|_| JwtJwksConfigurationError)?;
        let e = URL_SAFE_NO_PAD
            .decode(exponent)
            .map_err(|_| JwtJwksConfigurationError)?;
        if !(256..=512).contains(&n.len()) || n[0] & 0x80 == 0 || e != [1, 0, 1] {
            return Err(JwtJwksConfigurationError);
        }
        let der = RsaPublicKeyComponents { n, e }
            .as_der()
            .map_err(|_| JwtJwksConfigurationError)?;
        let key = ParsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, der.as_ref())
            .map_err(|_| JwtJwksConfigurationError)?;
        if keys.insert(kid.to_owned(), key).is_some() {
            return Err(JwtJwksConfigurationError);
        }
    }
    Ok(keys)
}

fn valid_kid(kid: &str) -> bool {
    !kid.is_empty() && kid.len() <= 128 && kid.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

fn token_parts(token: &str) -> Result<(&str, &str, &str), AgentHttpAuthenticationError> {
    let denied = AgentHttpAuthenticationError::Unauthenticated;
    let mut parts = token.split('.');
    let header = parts.next().ok_or(denied)?;
    let claims = parts.next().ok_or(denied)?;
    let signature = parts.next().ok_or(denied)?;
    if parts.next().is_some()
        || header.is_empty()
        || claims.is_empty()
        || signature.is_empty()
        || ![header, claims, signature].into_iter().all(|part| {
            part.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        })
    {
        return Err(denied);
    }
    Ok((header, claims, signature))
}

fn parse_header(token: &str) -> Result<String, AgentHttpAuthenticationError> {
    let denied = AgentHttpAuthenticationError::Unauthenticated;
    let (header, _, signature) = token_parts(token)?;
    if header.len() > 2048 || !(342..=683).contains(&signature.len()) {
        return Err(denied);
    }
    let bytes = URL_SAFE_NO_PAD.decode(header).map_err(|_| denied)?;
    let limits = JsonLimits::try_new(2048, 3, 8, 8, 512, 512).map_err(|_| denied)?;
    let parsed = BoundedJson::from_slice_with_limits(&bytes, limits).map_err(|_| denied)?;
    let object = parsed.as_value().as_object().ok_or(denied)?;
    if object.len() != 3
        || object.get("alg").and_then(Value::as_str) != Some("RS256")
        || !object
            .get("typ")
            .and_then(Value::as_str)
            .is_some_and(|typ| {
                typ.eq_ignore_ascii_case("at+jwt") || typ.eq_ignore_ascii_case("application/at+jwt")
            })
    {
        return Err(denied);
    }
    object
        .get("kid")
        .and_then(Value::as_str)
        .filter(|kid| valid_kid(kid))
        .map(str::to_owned)
        .ok_or(denied)
}

struct VerifiedToken {
    principal: PrincipalIdentity,
    scopes: Vec<String>,
    exp: u64,
    iat: u64,
    nbf: Option<u64>,
}

fn verify_token(
    token: &str,
    key: &ParsedPublicKey,
    options: &JwtJwksOptions,
) -> Result<VerifiedToken, AgentHttpAuthenticationError> {
    let denied = AgentHttpAuthenticationError::Unauthenticated;
    let (header, claims, signature) = token_parts(token)?;
    if claims.len() > 8192 {
        return Err(denied);
    }
    let bytes = URL_SAFE_NO_PAD.decode(claims).map_err(|_| denied)?;
    let limits = JsonLimits::try_new(8192, 8, 128, 128, 4096, 1024).map_err(|_| denied)?;
    let parsed = BoundedJson::from_slice_with_limits(&bytes, limits).map_err(|_| denied)?;
    let signature = URL_SAFE_NO_PAD.decode(signature).map_err(|_| denied)?;
    key.verify_sig(
        &token.as_bytes()[..header.len() + 1 + claims.len()],
        &signature,
    )
    .map_err(|_| denied)?;
    let claims = parsed.as_value();
    if claims.get("cnf").is_some() || claims["iss"].as_str() != Some(options.issuer.as_str()) {
        return Err(denied);
    }
    let audiences = match &claims["aud"] {
        Value::String(aud) => vec![aud.as_str()],
        Value::Array(aud) if !aud.is_empty() && aud.len() <= 32 => aud
            .iter()
            .map(|value| value.as_str().ok_or(denied))
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Err(denied),
    };
    if !audiences.contains(&options.audience.as_str())
        || audiences
            .iter()
            .any(|aud| aud.is_empty() || aud.len() > 512)
    {
        return Err(denied);
    }
    let exp = claims["exp"].as_u64().ok_or(denied)?;
    let iat = claims["iat"].as_u64().ok_or(denied)?;
    let nbf = claims
        .get("nbf")
        .map(|nbf| nbf.as_u64().ok_or(denied))
        .transpose()?;
    check_times(exp, iat, nbf, options)?;
    for name in ["client_id", "jti"] {
        if !claims[name].as_str().is_some_and(|value| {
            !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
        }) {
            return Err(denied);
        }
    }
    let subject = claims["sub"]
        .as_str()
        .ok_or(denied)?
        .parse()
        .map_err(|_| denied)?;
    let scope = match claims.get("scope") {
        None => "",
        Some(scope) => scope.as_str().ok_or(denied)?,
    };
    let mut scopes = Vec::new();
    if !scope.is_empty() {
        for scope in scope.split(' ') {
            if scopes.len() >= 64 || !valid_scope(scope) || scopes.iter().any(|s| s == scope) {
                return Err(denied);
            }
            scopes.push(scope.to_owned());
        }
    }
    Ok(VerifiedToken {
        principal: PrincipalIdentity::new(options.issuer.clone(), subject),
        scopes,
        exp,
        iat,
        nbf,
    })
}

fn check_times(
    exp: u64,
    iat: u64,
    nbf: Option<u64>,
    options: &JwtJwksOptions,
) -> Result<(), AgentHttpAuthenticationError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AgentHttpAuthenticationError::Unavailable)?
        .as_secs();
    if exp <= now
        || iat > now
        || iat >= exp
        || exp - iat > options.max_token_lifetime.as_secs()
        || nbf.is_some_and(|nbf| nbf > now || nbf >= exp)
    {
        return Err(AgentHttpAuthenticationError::Unauthenticated);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
