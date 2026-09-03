// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use stateknot_core::{ArtifactIdentity, BoxFuture};
use stateknot_store_postgres::{
    ArtifactRegistration, ArtifactRegistrationOutcome, PostgresStore, StoreError, StoredArtifact,
};

/// Durable metadata registry used by the object boundary.
///
/// Implementations must provide the same exact idempotency and integrity
/// guarantees as [`PostgresStore::register_artifact`]. No implementation may
/// perform object I/O while holding its database transaction.
pub trait ArtifactRegistry: Send + Sync + 'static {
    /// Verifies that the registry dependency can serve requests.
    fn health_check(&self) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Registers one exact immutable artifact intent.
    fn register(
        &self,
        registration: ArtifactRegistration,
    ) -> BoxFuture<'_, Result<ArtifactRegistrationOutcome, StoreError>>;

    /// Loads one exact tenant-qualified artifact record.
    fn load(&self, identity: ArtifactIdentity)
    -> BoxFuture<'_, Result<StoredArtifact, StoreError>>;
}

impl ArtifactRegistry for PostgresStore {
    fn health_check(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(PostgresStore::health_check(self))
    }

    fn register(
        &self,
        registration: ArtifactRegistration,
    ) -> BoxFuture<'_, Result<ArtifactRegistrationOutcome, StoreError>> {
        Box::pin(PostgresStore::register_artifact(self, registration))
    }

    fn load(
        &self,
        identity: ArtifactIdentity,
    ) -> BoxFuture<'_, Result<StoredArtifact, StoreError>> {
        Box::pin(async move { PostgresStore::load_artifact(self, &identity).await })
    }
}
