// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, sync::Arc};

use stateknot_core::{BoxFuture, ModelContext};
use thiserror::Error;
use zeroize::Zeroizing;

/// A validated provider credential whose storage is zeroized on final drop.
#[derive(Clone)]
pub struct ApiKey(Zeroizing<String>);

impl ApiKey {
    /// Maximum accepted credential length in bytes.
    pub const MAX_BYTES: usize = 8192;

    /// Validates and takes ownership of a provider credential.
    ///
    /// # Errors
    ///
    /// Returns [`ApiKeyError`] for empty, oversized, whitespace-padded, or
    /// header-unsafe values.
    pub fn new(value: impl Into<String>) -> Result<Self, ApiKeyError> {
        let value = Zeroizing::new(value.into());
        if value.is_empty() {
            return Err(ApiKeyError::Empty);
        }
        if value.len() > Self::MAX_BYTES {
            return Err(ApiKeyError::TooLong {
                maximum: Self::MAX_BYTES,
                actual: value.len(),
            });
        }
        if value.trim() != value.as_str() {
            return Err(ApiKeyError::WhitespacePadded);
        }
        if let Some((index, _)) = value
            .bytes()
            .enumerate()
            .find(|(_, byte)| !matches!(byte, b'!'..=b'~'))
        {
            return Err(ApiKeyError::InvalidByte { index });
        }
        Ok(Self(value))
    }

    pub(crate) fn expose_secret(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKey([REDACTED])")
    }
}

/// Invalid provider credential material.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ApiKeyError {
    /// The credential was empty.
    #[error("provider API key must not be empty")]
    Empty,
    /// The credential exceeded the hard resource ceiling.
    #[error("provider API key is {actual} bytes; maximum is {maximum}")]
    TooLong {
        /// Hard maximum.
        maximum: usize,
        /// Observed bytes.
        actual: usize,
    },
    /// Trimming would change the credential.
    #[error("provider API key must not have surrounding whitespace")]
    WhitespacePadded,
    /// A byte could not be represented safely in an HTTP header.
    #[error("provider API key contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based invalid byte offset.
        index: usize,
    },
}

/// Public-safe classification returned by a dynamic credential source.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ApiKeyResolutionError {
    /// The scoped secret backend was temporarily unavailable.
    #[error("provider credential source is unavailable")]
    Unavailable,
    /// Policy refused credential access for this attempt context.
    #[error("provider credential access was denied")]
    PermissionDenied,
}

/// Attempt-scoped provider credential source.
///
/// Implementations should resolve secret handles after durable attempt claim,
/// apply tenant policy using `context`, and avoid retaining plaintext longer
/// than necessary. Returned values are never formatted by `StateKnot`.
pub trait ApiKeyProvider: Send + Sync + 'static {
    /// Resolves one credential for the exact attempt.
    fn resolve(
        &self,
        context: &ModelContext,
    ) -> BoxFuture<'_, Result<ApiKey, ApiKeyResolutionError>>;
}

/// Immutable in-memory credential provider for controlled deployments.
#[derive(Clone)]
pub struct StaticApiKey {
    key: Arc<ApiKey>,
}

impl StaticApiKey {
    /// Wraps one validated key.
    #[must_use]
    pub fn new(key: ApiKey) -> Self {
        Self { key: Arc::new(key) }
    }
}

impl fmt::Debug for StaticApiKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StaticApiKey([REDACTED])")
    }
}

impl ApiKeyProvider for StaticApiKey {
    fn resolve(
        &self,
        _context: &ModelContext,
    ) -> BoxFuture<'_, Result<ApiKey, ApiKeyResolutionError>> {
        let key = self.key.as_ref().clone();
        Box::pin(async move { Ok(key) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_header_safe_and_always_redacted() {
        let secret = "sk-production-secret";
        let key = ApiKey::new(secret).unwrap();
        assert_eq!(key.expose_secret(), secret);
        assert!(!format!("{key:?}").contains(secret));
        assert!(!format!("{:?}", StaticApiKey::new(key)).contains(secret));

        assert!(matches!(ApiKey::new(""), Err(ApiKeyError::Empty)));
        assert!(matches!(
            ApiKey::new(" padded"),
            Err(ApiKeyError::WhitespacePadded)
        ));
        assert!(matches!(
            ApiKey::new("line\nbreak"),
            Err(ApiKeyError::InvalidByte { .. })
        ));
    }
}
