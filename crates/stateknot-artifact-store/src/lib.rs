// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Integrity-checked private object persistence and resolution for `StateKnot` artifacts.

#![forbid(unsafe_code)]

mod config;
mod error;
mod registry;
mod s3;
mod store;

pub use config::{ArtifactStoreOptions, RemoteArtifactOrigin};
pub use error::{ArtifactStoreError, ArtifactStoreErrorKind};
pub use registry::ArtifactRegistry;
pub use s3::{S3CompatibleBackendBuilder, S3ConditionalCopy};
pub use store::{
    AllowArtifactRead, ArtifactReadAuthorizationError, ArtifactReadAuthorizationRequest,
    ArtifactReadAuthorizer, ResolvedArtifact, StateKnotArtifactStore,
};
