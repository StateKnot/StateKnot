// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use stateknot_core::BoxFuture;
use stateknot_runtime::AgentServiceCaller;
use std::fmt;
use thiserror::Error;
use zeroize::Zeroizing;

/// Bounded RFC 6750 bearer value, redacted in Debug and zeroized on drop.
/// The HTTP stack's original header storage is outside this wrapper's ownership.
#[derive(Clone)]
pub struct AgentHttpCredential(Zeroizing<String>);

impl AgentHttpCredential {
    /// Hard credential byte limit.
    pub const MAX_BYTES: usize = 8192;

    /// Validates bearer syntax, not signature, audience, expiry or identity.
    pub fn new(value: impl Into<String>) -> Result<Self, AgentHttpAuthenticationError> {
        let value = Zeroizing::new(value.into());
        if value.is_empty() || value.starts_with('=') || value.len() > Self::MAX_BYTES {
            return Err(AgentHttpAuthenticationError::Unauthenticated);
        }
        let mut padding = false;
        for byte in value.bytes() {
            if byte == b'=' {
                padding = true;
            } else if padding
                || !(byte.is_ascii_alphanumeric()
                    || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/'))
            {
                return Err(AgentHttpAuthenticationError::Unauthenticated);
            }
        }
        Ok(Self(value))
    }

    /// Borrows the secret only for immediate verification; never log it.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for AgentHttpCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AgentHttpCredential([REDACTED])")
    }
}

/// Operation permission derived from a verified credential, not body claims.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentHttpOperation {
    /// Admit or recover an exact logical submission.
    Submit,
    /// Read an authorized Run or resolve a submission key.
    Read,
    /// Request cancellation; does not confirm terminal cleanup.
    Cancel,
}

impl AgentHttpOperation {
    const fn bit(self) -> u8 {
        match self {
            Self::Submit => 1,
            Self::Read => 2,
            Self::Cancel => 4,
        }
    }
}

/// Verified transport identity with explicit least-privilege operation grants.
/// Construction is trusted host wiring, not an authentication operation.
#[derive(Clone, Debug)]
pub struct AgentHttpPrincipal {
    caller: AgentServiceCaller,
    permissions: u8,
}

impl AgentHttpPrincipal {
    /// Binds identity and allowed operations after credential verification.
    #[must_use]
    pub fn new(
        caller: AgentServiceCaller,
        operations: impl IntoIterator<Item = AgentHttpOperation>,
    ) -> Self {
        Self {
            caller,
            permissions: operations.into_iter().fold(0, |bits, op| bits | op.bit()),
        }
    }

    /// Trusted tenant/issuer/subject mapping passed to resource authorization.
    #[must_use]
    pub const fn caller(&self) -> &AgentServiceCaller {
        &self.caller
    }

    /// Whether the verified credential permits this operation class.
    #[must_use]
    pub const fn allows(&self, operation: AgentHttpOperation) -> bool {
        self.permissions & operation.bit() != 0
    }
}

/// Payload-safe verification failure; no credential/provider text is exposed.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AgentHttpAuthenticationError {
    /// Missing, invalid, expired, revoked or wrong-audience credential.
    #[error("agent_http.unauthenticated")]
    Unauthenticated,
    /// Verification cannot currently establish trusted identity.
    #[error("agent_http.unavailable")]
    Unavailable,
}

/// Mandatory credential verifier. No anonymous or allow-all implementation ships.
/// Verify issuer, audience, expiry, revocation and cryptographic/introspection
/// evidence, then map tenant/principal and operation grants from trusted policy.
/// This hook runs before route/body parsing and is covered by the total deadline.
/// It does not replace the service's exact resource/evidence authorizer.
pub trait AgentHttpAuthenticator: Send + Sync + 'static {
    /// Verifies one bounded bearer credential without retaining its plaintext.
    fn authenticate(
        &self,
        credential: AgentHttpCredential,
    ) -> BoxFuture<'_, Result<AgentHttpPrincipal, AgentHttpAuthenticationError>>;
}
