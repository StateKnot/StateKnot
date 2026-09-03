// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{error::Error as StdError, fmt, sync::Arc};

/// Stable public classification for artifact persistence and resolution failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ArtifactStoreErrorKind {
    /// Static provider or egress configuration is invalid.
    Configuration,
    /// Remote bytes or declared metadata are invalid.
    InvalidContent,
    /// Explicit egress or content policy denied an operation.
    PolicyDenied,
    /// A caller was not authorized to resolve an artifact.
    AuthorizationDenied,
    /// Immutable metadata, registry evidence, or object bytes disagree.
    Integrity,
    /// The requested artifact does not exist in its tenant boundary.
    NotFound,
    /// A registry, object backend, authorization provider, or remote origin is unavailable.
    Unavailable,
}

type PrivateArtifactStoreSource = dyn StdError + Send + Sync + 'static;

/// Payload- and coordinate-redacted artifact boundary failure.
///
/// Display and `Debug` contain only a stable classification. Trusted telemetry
/// may inspect [`Self::private_source`], which can contain provider diagnostics,
/// object keys, signed URLs, or database details and must never cross an API.
#[derive(Clone)]
pub struct ArtifactStoreError {
    kind: ArtifactStoreErrorKind,
    private_source: Arc<PrivateArtifactStoreSource>,
}

impl ArtifactStoreError {
    pub(crate) fn new<E>(kind: ArtifactStoreErrorKind, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            kind,
            private_source: Arc::new(source),
        }
    }

    /// Returns the public-safe failure classification.
    #[must_use]
    pub const fn kind(&self) -> ArtifactStoreErrorKind {
        self.kind
    }

    /// Returns the private diagnostic for trusted in-process telemetry only.
    #[must_use]
    pub fn private_source(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self.private_source.as_ref()
    }
}

impl fmt::Debug for ArtifactStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactStoreError")
            .field("kind", &self.kind)
            .field("has_private_source", &true)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ArtifactStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "artifact operation failed: {:?}", self.kind)
    }
}

impl StdError for ArtifactStoreError {}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InternalError {
    #[error("artifact configuration is invalid")]
    Configuration,
    #[error("remote artifact content is invalid")]
    InvalidContent,
    #[error("artifact policy denied the operation")]
    PolicyDenied,
    #[error("artifact authorization was denied")]
    AuthorizationDenied,
    #[error("artifact integrity validation failed")]
    Integrity,
    #[error("artifact was not found")]
    NotFound,
    #[error("artifact dependency is unavailable")]
    Unavailable,
}

pub(crate) fn classified(kind: ArtifactStoreErrorKind) -> ArtifactStoreError {
    let source = match kind {
        ArtifactStoreErrorKind::Configuration => InternalError::Configuration,
        ArtifactStoreErrorKind::InvalidContent => InternalError::InvalidContent,
        ArtifactStoreErrorKind::PolicyDenied => InternalError::PolicyDenied,
        ArtifactStoreErrorKind::AuthorizationDenied => InternalError::AuthorizationDenied,
        ArtifactStoreErrorKind::Integrity => InternalError::Integrity,
        ArtifactStoreErrorKind::NotFound => InternalError::NotFound,
        ArtifactStoreErrorKind::Unavailable => InternalError::Unavailable,
    };
    ArtifactStoreError::new(kind, source)
}
