// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use object_store::{
    GetOptions, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, path::Path,
};
use reqwest::{StatusCode, Url, header};
use sha2::{Digest as _, Sha256};
use stateknot_core::{
    ArtifactDescription, ArtifactId, ArtifactIdentity, ArtifactModality, ArtifactName,
    ArtifactParents, ArtifactPresentation, ArtifactProvenance, ArtifactRef, ArtifactRepresentation,
    BoxFuture, ByteCount, ContentMetadata, ContentSource, Digest, MediaType, PrincipalIdentity,
};
use stateknot_integrations::{
    A2aArtifactIngestionError, A2aArtifactIngestionErrorKind, A2aArtifactIngestionRequest,
    A2aArtifactIngestor, A2aPartContent,
};
use stateknot_store_postgres::{ArtifactRegistration, ArtifactStorageLocator, StoreError};
use thiserror::Error;
use tokio::sync::{Semaphore, SemaphorePermit};
use uuid::Uuid;

use crate::{
    ArtifactRegistry, ArtifactStoreError, ArtifactStoreErrorKind, ArtifactStoreOptions,
    error::classified,
};

const REGISTRATION_DOMAIN: &[u8] = b"stateknot.a2a-artifact-registration.v1\0";
const PROBE_BYTES: &[u8] = b"stateknot-artifact-backend-contract-v1";

/// Public-safe authorization context for one artifact read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactReadAuthorizationRequest {
    principal: PrincipalIdentity,
    identity: ArtifactIdentity,
}

impl ArtifactReadAuthorizationRequest {
    /// Returns the authenticated caller.
    #[must_use]
    pub const fn principal(&self) -> &PrincipalIdentity {
        &self.principal
    }

    /// Returns the requested tenant-qualified artifact identity.
    #[must_use]
    pub const fn identity(&self) -> &ArtifactIdentity {
        &self.identity
    }
}

/// Stable authorization-provider failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ArtifactReadAuthorizationError {
    /// Policy denied this exact caller and artifact identity.
    #[error("artifact read is not authorized")]
    Denied,
    /// The authorization dependency could not make a decision.
    #[error("artifact authorization provider is unavailable")]
    Unavailable,
}

/// Authorization boundary evaluated before any registry or object lookup.
pub trait ArtifactReadAuthorizer: Send + Sync + 'static {
    /// Authorizes one exact caller and tenant-qualified artifact identity.
    fn authorize(
        &self,
        request: &ArtifactReadAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), ArtifactReadAuthorizationError>>;
}

/// Explicit allow-all authorizer for already isolated trusted deployments.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowArtifactRead;

impl ArtifactReadAuthorizer for AllowArtifactRead {
    fn authorize(
        &self,
        _request: &ArtifactReadAuthorizationRequest,
    ) -> BoxFuture<'_, Result<(), ArtifactReadAuthorizationError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Fully integrity-verified materialized artifact bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedArtifact {
    artifact: ArtifactRef,
    bytes: Bytes,
}

impl ResolvedArtifact {
    /// Returns the exact immutable reference used for validation.
    #[must_use]
    pub const fn artifact(&self) -> &ArtifactRef {
        &self.artifact
    }

    /// Returns bytes only after complete length and SHA-256 verification.
    #[must_use]
    pub const fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

/// Initialized, integrity-checking artifact persistence boundary.
///
/// Construction performs a real create/copy-if-absent/read/delete probe, so a
/// backend that cannot atomically publish immutable final keys is rejected
/// before this value can ingest traffic. The configured object store must also
/// have lifecycle rules for abandoned multipart uploads and the
/// `stateknot/staging/v1/` prefix, covering process death before cleanup.
pub struct StateKnotArtifactStore {
    objects: Arc<dyn ObjectStore>,
    registry: Arc<dyn ArtifactRegistry>,
    authorizer: Arc<dyn ArtifactReadAuthorizer>,
    storage_namespace: Box<str>,
    options: ArtifactStoreOptions,
    http: reqwest::Client,
    operation_admission: Semaphore,
    staging_cleanup_failures: AtomicU64,
}

impl StateKnotArtifactStore {
    /// Verifies dependencies and constructs a usable artifact boundary.
    ///
    /// `storage_namespace` is a stable deployment identifier for this exact
    /// bucket/backend, not a credential or endpoint. Changing the physical
    /// backend requires a new namespace or an integrity-preserving migration.
    ///
    /// # Errors
    ///
    /// Returns a configuration or availability error when the namespace,
    /// registry, HTTP client, or atomic object-store contract cannot be verified.
    pub async fn initialize(
        objects: Arc<dyn ObjectStore>,
        registry: Arc<dyn ArtifactRegistry>,
        authorizer: Arc<dyn ArtifactReadAuthorizer>,
        storage_namespace: impl Into<String>,
        options: ArtifactStoreOptions,
    ) -> Result<Self, ArtifactStoreError> {
        let storage_namespace = storage_namespace.into();
        ArtifactStorageLocator::new(
            storage_namespace.clone(),
            "stateknot/validation",
            None,
            None,
        )
        .map_err(map_locator_configuration_error)?;
        let http = options.build_http_client()?;
        run_with_timeout(options.operation_timeout(), registry.health_check())
            .await
            .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
            .map_err(map_registry_error)?;
        let maximum_concurrent_operations = options.maximum_concurrent_operations();
        let store = Self {
            objects,
            registry,
            authorizer,
            storage_namespace: storage_namespace.into_boxed_str(),
            options,
            http,
            operation_admission: Semaphore::new(maximum_concurrent_operations),
            staging_cleanup_failures: AtomicU64::new(0),
        };
        store.verify_backend_contract().await?;
        Ok(store)
    }

    /// Returns the number of best-effort staging deletions that have failed.
    ///
    /// Operators should alert on increases and verify the mandatory lifecycle
    /// policy; a final object may already be safely registered despite cleanup
    /// failure.
    #[must_use]
    pub fn staging_cleanup_failures(&self) -> u64 {
        self.staging_cleanup_failures.load(Ordering::Relaxed)
    }

    /// Ingests one bounded A2A part into immutable object and registry storage.
    ///
    /// # Errors
    ///
    /// Returns a stable configuration, content, policy, integrity, or
    /// availability failure. Retrying an availability failure with the exact
    /// request is safe and never overwrites the final object.
    #[allow(clippy::too_many_lines)]
    pub async fn ingest_a2a(
        &self,
        request: A2aArtifactIngestionRequest,
    ) -> Result<ArtifactRef, ArtifactStoreError> {
        let _permit = self.acquire_operation().await?;
        let (registration_key, artifact_id) = registration_identity(&request)?;
        let tenant_hash = hex_digest(Digest::sha256(request.tenant_id().as_str().as_bytes()));
        let final_key = format!("stateknot/artifacts/v1/{tenant_hash}/{artifact_id}");
        let staging_key = format!(
            "stateknot/staging/v1/{tenant_hash}/{artifact_id}/{}",
            Uuid::now_v7()
        );
        let final_path = parse_object_path(&final_key)?;
        let staging_path = parse_object_path(&staging_key)?;
        let maximum_bytes = request
            .maximum_bytes()
            .get()
            .min(self.options.maximum_remote_bytes());
        // Reject untrusted presentation metadata before any remote fetch or
        // object write. Local artifact names are intentionally stricter than
        // the A2A wire bounds and must not create unreachable final objects.
        let presentation = artifact_presentation(&request)?;

        let staged = match request.part().content() {
            A2aPartContent::Text(text) => {
                let media_type =
                    declared_media_type(request.part().media_type(), "text/plain;charset=utf-8")?;
                validate_text_media_type(&media_type)?;
                self.stage_inline(
                    &staging_path,
                    Bytes::copy_from_slice(text.as_bytes()),
                    maximum_bytes,
                    media_type,
                    ArtifactModality::Text,
                )
                .await?
            }
            A2aPartContent::Data(value) => {
                let media_type =
                    declared_media_type(request.part().media_type(), "application/json")?;
                validate_json_media_type(&media_type)?;
                let bytes = serde_json_canonicalizer::to_vec(value)
                    .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
                self.stage_inline(
                    &staging_path,
                    Bytes::from(bytes),
                    maximum_bytes,
                    media_type,
                    ArtifactModality::StructuredData,
                )
                .await?
            }
            A2aPartContent::Raw(bytes) => {
                let media_type =
                    declared_media_type(request.part().media_type(), "application/octet-stream")?;
                let modality = modality_for_media_type(&media_type);
                validate_interpreted_bytes(&media_type, bytes)?;
                self.stage_inline(
                    &staging_path,
                    Bytes::copy_from_slice(bytes),
                    maximum_bytes,
                    media_type,
                    modality,
                )
                .await?
            }
            A2aPartContent::Url(url) => {
                self.stage_remote(
                    &staging_path,
                    url,
                    request.part().media_type(),
                    maximum_bytes,
                )
                .await?
            }
            _ => return Err(classified(ArtifactStoreErrorKind::InvalidContent)),
        };

        // Finish every fallible public-reference validation before publishing
        // the deterministic final key. This prevents invalid local metadata
        // from creating a final object that can never be registered.
        let artifact = ArtifactRef::new(
            ArtifactIdentity::new(request.tenant_id().clone(), artifact_id),
            presentation,
            ArtifactRepresentation::new(
                staged.media_type,
                staged.modality,
                ByteCount::new(staged.byte_length),
                staged.digest,
                None,
            )
            .map_err(|_| classified(ArtifactStoreErrorKind::Integrity))?,
            ContentMetadata::untrusted(
                ContentSource::Artifact,
                self.options.security_label().clone(),
            ),
            self.options.retention_class().clone(),
            ArtifactProvenance::new(
                request.tool().owner().clone(),
                Some(request.tool().capability().clone()),
                request.run_id(),
                request.origin_event_id(),
            ),
            ArtifactParents::empty(),
        )
        .map_err(|_| classified(ArtifactStoreErrorKind::Integrity))?;

        let final_meta = match self
            .publish_and_verify(
                &staging_path,
                &final_path,
                staged.byte_length,
                staged.digest,
            )
            .await
        {
            Ok(meta) => meta,
            Err(error) => {
                self.best_effort_delete(&staging_path).await;
                return Err(error);
            }
        };
        self.best_effort_delete(&staging_path).await;
        let locator = ArtifactStorageLocator::new(
            self.storage_namespace.to_string(),
            final_key,
            final_meta.version,
            final_meta.e_tag,
        )
        .map_err(|_| classified(ArtifactStoreErrorKind::Integrity))?;
        let registration = ArtifactRegistration::new(registration_key, artifact, locator);
        let outcome = run_with_timeout(
            self.options.operation_timeout(),
            self.registry.register(registration.clone()),
        )
        .await
        .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
        .map_err(map_registry_error)?;
        if outcome.stored().registration() != &registration {
            return Err(classified(ArtifactStoreErrorKind::Integrity));
        }
        Ok(outcome.stored().artifact().clone())
    }

    /// Authorizes, loads, and completely verifies bounded artifact bytes.
    ///
    /// Authorization runs before registry lookup, preventing existence or
    /// locator disclosure to an unauthorized caller. No bytes are returned
    /// until the declared length and SHA-256 digest have both been checked.
    ///
    /// # Errors
    ///
    /// Returns an authorization, not-found, policy, integrity, or availability
    /// failure. `maximum_bytes` is intersected with the configured read ceiling.
    pub async fn resolve(
        &self,
        principal: PrincipalIdentity,
        artifact: &ArtifactRef,
        maximum_bytes: ByteCount,
    ) -> Result<ResolvedArtifact, ArtifactStoreError> {
        let _permit = self.acquire_operation().await?;
        let authorization = ArtifactReadAuthorizationRequest {
            principal,
            identity: artifact.identity().clone(),
        };
        run_with_timeout(
            self.options.operation_timeout(),
            self.authorizer.authorize(&authorization),
        )
        .await
        .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
        .map_err(|error| match error {
            ArtifactReadAuthorizationError::Denied => {
                classified(ArtifactStoreErrorKind::AuthorizationDenied)
            }
            ArtifactReadAuthorizationError::Unavailable => {
                classified(ArtifactStoreErrorKind::Unavailable)
            }
        })?;

        let stored = run_with_timeout(
            self.options.operation_timeout(),
            self.registry.load(artifact.identity().clone()),
        )
        .await
        .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
        .map_err(map_registry_error)?;
        if stored.artifact() != artifact
            || stored.locator().storage_namespace() != self.storage_namespace.as_ref()
        {
            return Err(classified(ArtifactStoreErrorKind::Integrity));
        }
        let permitted = maximum_bytes
            .get()
            .min(self.options.maximum_resolved_bytes());
        if artifact.representation().byte_length().get() > permitted {
            return Err(classified(ArtifactStoreErrorKind::PolicyDenied));
        }
        let location = parse_object_path(stored.locator().object_key())?;
        let bytes = self
            .read_verified_bytes(
                &location,
                stored.locator().object_version(),
                stored.locator().object_etag(),
                artifact.representation().byte_length().get(),
                artifact.representation().digest(),
                permitted,
            )
            .await?;
        Ok(ResolvedArtifact {
            artifact: artifact.clone(),
            bytes,
        })
    }

    async fn verify_backend_contract(&self) -> Result<(), ArtifactStoreError> {
        let probe_id = Uuid::now_v7();
        let source = parse_object_path(&format!(
            "stateknot/staging/v1/probes/{probe_id}/conditional-copy-source"
        ))?;
        let target = parse_object_path(&format!(
            "stateknot/staging/v1/probes/{probe_id}/conditional-copy-target"
        ))?;
        let result = async {
            self.object_timeout(self.objects.put_opts(
                &source,
                Bytes::from_static(PROBE_BYTES).into(),
                PutOptions::from(PutMode::Create),
            ))
            .await?
            .map_err(map_object_unavailable)?;
            self.object_timeout(self.objects.copy_if_not_exists(&source, &target))
                .await?
                .map_err(map_object_unavailable)?;
            let meta = self
                .verify_object(
                    &target,
                    u64::try_from(PROBE_BYTES.len()).expect("probe length fits u64"),
                    Digest::sha256(PROBE_BYTES),
                )
                .await?;
            ArtifactStorageLocator::new(
                self.storage_namespace.to_string(),
                target.to_string(),
                meta.version,
                meta.e_tag,
            )
            .map_err(map_locator_configuration_error)?;
            match self
                .object_timeout(self.objects.copy_if_not_exists(&source, &target))
                .await?
            {
                Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
                Ok(()) | Err(_) => Err(classified(ArtifactStoreErrorKind::Integrity)),
            }
        }
        .await;
        let source_delete = self.object_timeout(self.objects.delete(&source)).await;
        let target_delete = self.object_timeout(self.objects.delete(&target)).await;
        result?;
        if matches!(source_delete, Ok(Ok(()))) && matches!(target_delete, Ok(Ok(()))) {
            Ok(())
        } else {
            Err(classified(ArtifactStoreErrorKind::Unavailable))
        }
    }

    async fn stage_inline(
        &self,
        staging_path: &Path,
        bytes: Bytes,
        maximum_bytes: u64,
        media_type: MediaType,
        modality: ArtifactModality,
    ) -> Result<StagedArtifact, ArtifactStoreError> {
        let byte_length = u64::try_from(bytes.len())
            .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
        if byte_length > maximum_bytes {
            return Err(classified(ArtifactStoreErrorKind::InvalidContent));
        }
        let digest = Digest::sha256(&bytes);
        let write = self
            .object_timeout(self.objects.put_opts(
                staging_path,
                bytes.into(),
                PutOptions::from(PutMode::Create),
            ))
            .await;
        match write {
            Ok(Ok(_)) => {}
            Ok(Err(source)) => {
                self.best_effort_delete(staging_path).await;
                return Err(map_object_unavailable(source));
            }
            Err(error) => {
                self.best_effort_delete(staging_path).await;
                return Err(error);
            }
        }
        Ok(StagedArtifact {
            byte_length,
            digest,
            media_type,
            modality,
        })
    }

    async fn stage_remote(
        &self,
        staging_path: &Path,
        value: &str,
        declared_media: Option<&str>,
        maximum_bytes: u64,
    ) -> Result<StagedArtifact, ArtifactStoreError> {
        let response = self.fetch_remote(value, maximum_bytes).await?;
        let expected_length = response.content_length();
        if expected_length.is_some_and(|length| length > maximum_bytes) {
            return Err(classified(ArtifactStoreErrorKind::InvalidContent));
        }
        validate_content_encoding(response.headers())?;
        let response_media = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|value| {
                value
                    .to_str()
                    .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?
                    .parse::<MediaType>()
                    .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))
            })
            .transpose()?;
        let media_type = select_remote_media_type(declared_media, response_media)?;
        let modality = modality_for_media_type(&media_type);
        let mut writer = StagingWriter::new(
            self.objects.as_ref(),
            staging_path.clone(),
            self.options.multipart_chunk_bytes(),
            self.options.operation_timeout(),
            &self.staging_cleanup_failures,
        );
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(source) => {
                    writer.abort().await;
                    return Err(ArtifactStoreError::new(
                        ArtifactStoreErrorKind::Unavailable,
                        source,
                    ));
                }
            };
            let chunk_length = u64::try_from(chunk.len())
                .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
            if writer.byte_length().saturating_add(chunk_length) > maximum_bytes {
                writer.abort().await;
                return Err(classified(ArtifactStoreErrorKind::InvalidContent));
            }
            writer.push(chunk).await?;
        }
        let uploaded = writer.finish().await?;
        let validated = async {
            if expected_length.is_some_and(|length| length != uploaded.byte_length) {
                return Err(classified(ArtifactStoreErrorKind::Integrity));
            }
            if matches!(
                modality,
                ArtifactModality::Text | ArtifactModality::StructuredData
            ) {
                let interpreted = self
                    .read_object_bytes(staging_path, uploaded.byte_length, uploaded.byte_length)
                    .await?;
                validate_interpreted_bytes(&media_type, &interpreted)?;
            }
            Ok(StagedArtifact {
                byte_length: uploaded.byte_length,
                digest: uploaded.digest,
                media_type,
                modality,
            })
        }
        .await;
        if validated.is_err() {
            self.best_effort_delete(staging_path).await;
        }
        validated
    }

    async fn fetch_remote(
        &self,
        value: &str,
        maximum_bytes: u64,
    ) -> Result<reqwest::Response, ArtifactStoreError> {
        let mut url =
            Url::parse(value).map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
        for redirect_count in 0..=self.options.maximum_redirects() {
            self.validate_remote_url(&url)?;
            let response = self
                .http
                .get(url.clone())
                .header(header::ACCEPT_ENCODING, "identity")
                .send()
                .await
                .map_err(|source| {
                    ArtifactStoreError::new(ArtifactStoreErrorKind::Unavailable, source)
                })?;
            if response.status().is_redirection() {
                if redirect_count == self.options.maximum_redirects() {
                    return Err(classified(ArtifactStoreErrorKind::PolicyDenied));
                }
                let location = response
                    .headers()
                    .get(header::LOCATION)
                    .ok_or_else(|| classified(ArtifactStoreErrorKind::InvalidContent))?
                    .to_str()
                    .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
                url = url
                    .join(location)
                    .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
                continue;
            }
            if response.status() != StatusCode::OK {
                return Err(if response.status().is_server_error() {
                    classified(ArtifactStoreErrorKind::Unavailable)
                } else {
                    classified(ArtifactStoreErrorKind::InvalidContent)
                });
            }
            if response
                .content_length()
                .is_some_and(|length| length > maximum_bytes)
            {
                return Err(classified(ArtifactStoreErrorKind::InvalidContent));
            }
            return Ok(response);
        }
        Err(classified(ArtifactStoreErrorKind::PolicyDenied))
    }

    fn validate_remote_url(&self, url: &Url) -> Result<(), ArtifactStoreError> {
        if !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !self
                .options
                .remote_origins()
                .iter()
                .any(|origin| origin.allows(url))
        {
            return Err(classified(ArtifactStoreErrorKind::PolicyDenied));
        }
        Ok(())
    }

    async fn publish_and_verify(
        &self,
        staging_path: &Path,
        final_path: &Path,
        expected_length: u64,
        expected_digest: Digest,
    ) -> Result<ObjectMeta, ArtifactStoreError> {
        match self
            .object_timeout(self.objects.copy_if_not_exists(staging_path, final_path))
            .await?
        {
            Ok(()) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(source) => {
                return Err(ArtifactStoreError::new(
                    ArtifactStoreErrorKind::Unavailable,
                    source,
                ));
            }
        }
        self.verify_object(final_path, expected_length, expected_digest)
            .await
    }

    async fn verify_object(
        &self,
        location: &Path,
        expected_length: u64,
        expected_digest: Digest,
    ) -> Result<ObjectMeta, ArtifactStoreError> {
        let deadline = tokio::time::Instant::now() + self.options.operation_timeout();
        let result = self
            .object_until(
                deadline,
                self.objects.get_opts(location, GetOptions::default()),
            )
            .await?
            .map_err(map_object_read_error)?;
        let meta = result.meta.clone();
        if meta.size != expected_length {
            return Err(classified(ArtifactStoreErrorKind::Integrity));
        }
        let mut stream = result.into_stream();
        let mut observed = 0_u64;
        let mut hasher = Sha256::new();
        while let Some(chunk) = run_until(deadline, stream.next())
            .await
            .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
        {
            let chunk = chunk.map_err(|source| {
                ArtifactStoreError::new(ArtifactStoreErrorKind::Unavailable, source)
            })?;
            observed = observed
                .checked_add(
                    u64::try_from(chunk.len())
                        .map_err(|_| classified(ArtifactStoreErrorKind::Integrity))?,
                )
                .ok_or_else(|| classified(ArtifactStoreErrorKind::Integrity))?;
            if observed > expected_length {
                return Err(classified(ArtifactStoreErrorKind::Integrity));
            }
            hasher.update(&chunk);
        }
        if observed != expected_length || digest_from_hasher(hasher) != expected_digest {
            return Err(classified(ArtifactStoreErrorKind::Integrity));
        }
        Ok(meta)
    }

    async fn read_verified_bytes(
        &self,
        location: &Path,
        version: Option<&str>,
        etag: Option<&str>,
        expected_length: u64,
        expected_digest: Digest,
        maximum_bytes: u64,
    ) -> Result<Bytes, ArtifactStoreError> {
        if expected_length > maximum_bytes {
            return Err(classified(ArtifactStoreErrorKind::PolicyDenied));
        }
        let options = GetOptions {
            version: version.map(ToOwned::to_owned),
            if_match: etag.map(ToOwned::to_owned),
            ..GetOptions::default()
        };
        let deadline = tokio::time::Instant::now() + self.options.operation_timeout();
        let result = self
            .object_until(deadline, self.objects.get_opts(location, options))
            .await?
            .map_err(map_object_read_error)?;
        if result.meta.size != expected_length {
            return Err(classified(ArtifactStoreErrorKind::Integrity));
        }
        let mut stream = result.into_stream();
        let capacity = usize::try_from(expected_length)
            .map_err(|_| classified(ArtifactStoreErrorKind::PolicyDenied))?;
        let mut output = BytesMut::with_capacity(capacity);
        let mut hasher = Sha256::new();
        while let Some(chunk) = run_until(deadline, stream.next())
            .await
            .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
        {
            let chunk = chunk.map_err(|source| {
                ArtifactStoreError::new(ArtifactStoreErrorKind::Unavailable, source)
            })?;
            let next = u64::try_from(output.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            if next > expected_length || next > maximum_bytes {
                return Err(classified(ArtifactStoreErrorKind::Integrity));
            }
            hasher.update(&chunk);
            output.extend_from_slice(&chunk);
        }
        if u64::try_from(output.len()).ok() != Some(expected_length)
            || digest_from_hasher(hasher) != expected_digest
        {
            return Err(classified(ArtifactStoreErrorKind::Integrity));
        }
        Ok(output.freeze())
    }

    async fn read_object_bytes(
        &self,
        location: &Path,
        expected_length: u64,
        maximum_bytes: u64,
    ) -> Result<Bytes, ArtifactStoreError> {
        let digest = self
            .verify_object_and_collect(location, expected_length, maximum_bytes)
            .await?;
        Ok(digest.0)
    }

    async fn verify_object_and_collect(
        &self,
        location: &Path,
        expected_length: u64,
        maximum_bytes: u64,
    ) -> Result<(Bytes, Digest), ArtifactStoreError> {
        if expected_length > maximum_bytes {
            return Err(classified(ArtifactStoreErrorKind::PolicyDenied));
        }
        let deadline = tokio::time::Instant::now() + self.options.operation_timeout();
        let result = self
            .object_until(
                deadline,
                self.objects.get_opts(location, GetOptions::default()),
            )
            .await?
            .map_err(map_object_read_error)?;
        if result.meta.size != expected_length {
            return Err(classified(ArtifactStoreErrorKind::Integrity));
        }
        let mut stream = result.into_stream();
        let capacity = usize::try_from(expected_length)
            .map_err(|_| classified(ArtifactStoreErrorKind::PolicyDenied))?;
        let mut output = BytesMut::with_capacity(capacity);
        let mut hasher = Sha256::new();
        while let Some(chunk) = run_until(deadline, stream.next())
            .await
            .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
        {
            let chunk = chunk.map_err(|source| {
                ArtifactStoreError::new(ArtifactStoreErrorKind::Unavailable, source)
            })?;
            let next = u64::try_from(output.len())
                .unwrap_or(u64::MAX)
                .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            if next > expected_length || next > maximum_bytes {
                return Err(classified(ArtifactStoreErrorKind::Integrity));
            }
            hasher.update(&chunk);
            output.extend_from_slice(&chunk);
        }
        if u64::try_from(output.len()).ok() != Some(expected_length) {
            return Err(classified(ArtifactStoreErrorKind::Integrity));
        }
        Ok((output.freeze(), digest_from_hasher(hasher)))
    }

    async fn best_effort_delete(&self, location: &Path) {
        if !matches!(
            self.object_timeout(self.objects.delete(location)).await,
            Ok(Ok(()) | Err(object_store::Error::NotFound { .. }))
        ) {
            self.staging_cleanup_failures
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn object_timeout<T>(
        &self,
        future: impl Future<Output = object_store::Result<T>>,
    ) -> Result<object_store::Result<T>, ArtifactStoreError> {
        run_with_timeout(self.options.operation_timeout(), future)
            .await
            .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))
    }

    async fn acquire_operation(&self) -> Result<SemaphorePermit<'_>, ArtifactStoreError> {
        run_with_timeout(
            self.options.operation_timeout(),
            self.operation_admission.acquire(),
        )
        .await
        .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
        .map_err(|_| classified(ArtifactStoreErrorKind::Unavailable))
    }

    async fn object_until<T>(
        &self,
        deadline: tokio::time::Instant,
        future: impl Future<Output = object_store::Result<T>>,
    ) -> Result<object_store::Result<T>, ArtifactStoreError> {
        run_until(deadline, future)
            .await
            .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))
    }
}

impl fmt::Debug for StateKnotArtifactStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateKnotArtifactStore")
            .field("backend", &"<redacted>")
            .field("storage_namespace", &"<redacted>")
            .field("options", &self.options)
            .field("staging_cleanup_failures", &self.staging_cleanup_failures())
            .finish_non_exhaustive()
    }
}

impl A2aArtifactIngestor for StateKnotArtifactStore {
    fn ingest(
        &self,
        request: A2aArtifactIngestionRequest,
    ) -> BoxFuture<'_, Result<ArtifactRef, A2aArtifactIngestionError>> {
        Box::pin(async move {
            self.ingest_a2a(request).await.map_err(|error| {
                let kind = match error.kind() {
                    ArtifactStoreErrorKind::InvalidContent => {
                        A2aArtifactIngestionErrorKind::InvalidContent
                    }
                    ArtifactStoreErrorKind::PolicyDenied
                    | ArtifactStoreErrorKind::AuthorizationDenied => {
                        A2aArtifactIngestionErrorKind::PolicyDenied
                    }
                    ArtifactStoreErrorKind::Integrity | ArtifactStoreErrorKind::NotFound => {
                        A2aArtifactIngestionErrorKind::Integrity
                    }
                    ArtifactStoreErrorKind::Configuration | ArtifactStoreErrorKind::Unavailable => {
                        A2aArtifactIngestionErrorKind::Unavailable
                    }
                };
                A2aArtifactIngestionError::new(kind, error)
            })
        })
    }
}

struct StagedArtifact {
    byte_length: u64,
    digest: Digest,
    media_type: MediaType,
    modality: ArtifactModality,
}

struct UploadedObject {
    byte_length: u64,
    digest: Digest,
}

struct StagingWriter<'store> {
    store: &'store dyn ObjectStore,
    location: Path,
    chunk_size: usize,
    timeout: std::time::Duration,
    buffer: BytesMut,
    upload: Option<Box<dyn MultipartUpload>>,
    hasher: Sha256,
    byte_length: u64,
    cleanup_failures: &'store AtomicU64,
}

impl<'store> StagingWriter<'store> {
    fn new(
        store: &'store dyn ObjectStore,
        location: Path,
        chunk_size: usize,
        timeout: std::time::Duration,
        cleanup_failures: &'store AtomicU64,
    ) -> Self {
        Self {
            store,
            location,
            chunk_size,
            timeout,
            buffer: BytesMut::with_capacity(chunk_size),
            upload: None,
            hasher: Sha256::new(),
            byte_length: 0,
            cleanup_failures,
        }
    }

    const fn byte_length(&self) -> u64 {
        self.byte_length
    }

    async fn push(&mut self, mut bytes: Bytes) -> Result<(), ArtifactStoreError> {
        self.byte_length = self
            .byte_length
            .checked_add(
                u64::try_from(bytes.len())
                    .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?,
            )
            .ok_or_else(|| classified(ArtifactStoreErrorKind::InvalidContent))?;
        self.hasher.update(&bytes);
        while !bytes.is_empty() {
            let copied = (self.chunk_size - self.buffer.len()).min(bytes.len());
            self.buffer.extend_from_slice(&bytes.split_to(copied));
            if self.buffer.len() != self.chunk_size {
                continue;
            }
            if self.upload.is_none() {
                self.upload = Some(
                    run_with_timeout(
                        self.timeout,
                        self.store
                            .put_multipart_opts(&self.location, PutMultipartOptions::default()),
                    )
                    .await
                    .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
                    .map_err(|source| {
                        ArtifactStoreError::new(ArtifactStoreErrorKind::Unavailable, source)
                    })?,
                );
            }
            let part = self.buffer.split_to(self.chunk_size).freeze();
            let result = run_with_timeout(
                self.timeout,
                self.upload
                    .as_mut()
                    .expect("multipart upload was initialized")
                    .put_part(part.into()),
            )
            .await;
            if !matches!(result, Ok(Ok(()))) {
                self.abort().await;
                return match result {
                    Err(()) => Err(classified(ArtifactStoreErrorKind::Unavailable)),
                    Ok(Err(source)) => Err(ArtifactStoreError::new(
                        ArtifactStoreErrorKind::Unavailable,
                        source,
                    )),
                    Ok(Ok(())) => unreachable!(),
                };
            }
        }
        Ok(())
    }

    async fn finish(mut self) -> Result<UploadedObject, ArtifactStoreError> {
        if self.upload.is_some() {
            if !self.buffer.is_empty() {
                let tail = self.buffer.split().freeze();
                let result = run_with_timeout(
                    self.timeout,
                    self.upload
                        .as_mut()
                        .expect("multipart upload was initialized")
                        .put_part(tail.into()),
                )
                .await;
                if let Err(error) = map_timed_object_result(result) {
                    self.abort().await;
                    return Err(error);
                }
            }
            let result = run_with_timeout(
                self.timeout,
                self.upload
                    .as_mut()
                    .expect("multipart upload was initialized")
                    .complete(),
            )
            .await;
            if let Err(error) = map_timed_object_result(result) {
                self.abort().await;
                // Completion can have committed the object before its response
                // was lost. Delete that ambiguous staging object immediately;
                // the mandatory lifecycle rule remains the crash fallback.
                self.delete_completed_object().await;
                return Err(error);
            }
        } else {
            let payload = self.buffer.split().freeze();
            let result = run_with_timeout(
                self.timeout,
                self.store.put_opts(
                    &self.location,
                    payload.into(),
                    PutOptions::from(PutMode::Create),
                ),
            )
            .await;
            if let Err(error) = map_timed_object_result(result) {
                // A timeout is not evidence that the create failed. Cleaning
                // the UUID-scoped staging key is safe and bounds normal-failure
                // residue without weakening final-key immutability.
                self.delete_completed_object().await;
                return Err(error);
            }
        }
        Ok(UploadedObject {
            byte_length: self.byte_length,
            digest: digest_from_hasher(self.hasher),
        })
    }

    async fn abort(&mut self) {
        if let Some(upload) = self.upload.as_mut()
            && !matches!(
                run_with_timeout(self.timeout, upload.abort()).await,
                Ok(Ok(()))
            )
        {
            self.cleanup_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn delete_completed_object(&mut self) {
        if !matches!(
            run_with_timeout(self.timeout, self.store.delete(&self.location)).await,
            Ok(Ok(()) | Err(object_store::Error::NotFound { .. }))
        ) {
            self.cleanup_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn registration_identity(
    request: &A2aArtifactIngestionRequest,
) -> Result<(Digest, ArtifactId), ArtifactStoreError> {
    let tool = serde_json_canonicalizer::to_vec(request.tool())
        .map_err(|_| classified(ArtifactStoreErrorKind::Integrity))?;
    let mut hasher = Sha256::new();
    hasher.update(REGISTRATION_DOMAIN);
    update_bounded_field(&mut hasher, request.tenant_id().as_str().as_bytes())?;
    update_bounded_field(&mut hasher, request.run_id().as_uuid().as_bytes())?;
    update_bounded_field(&mut hasher, request.invocation_id().as_uuid().as_bytes())?;
    update_bounded_field(&mut hasher, request.attempt_id().as_uuid().as_bytes())?;
    update_bounded_field(&mut hasher, request.origin_event_id().as_uuid().as_bytes())?;
    update_bounded_field(&mut hasher, &tool)?;
    update_bounded_field(&mut hasher, request.source().task_id().as_bytes())?;
    update_bounded_field(&mut hasher, request.source().artifact_id().as_bytes())?;
    hasher.update(request.source().artifact_index().to_be_bytes());
    hasher.update(request.source().part_index().to_be_bytes());
    let key_bytes: [u8; 32] = hasher.finalize().into();
    let registration_key = Digest::from_sha256(key_bytes);
    let mut uuid_bytes = [0_u8; 16];
    uuid_bytes.copy_from_slice(&key_bytes[..16]);
    uuid_bytes[..6].copy_from_slice(&request.origin_event_id().as_uuid().as_bytes()[..6]);
    uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x70;
    uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80;
    let artifact_id = ArtifactId::from_uuid(Uuid::from_bytes(uuid_bytes))
        .map_err(|_| classified(ArtifactStoreErrorKind::Integrity))?;
    Ok((registration_key, artifact_id))
}

fn artifact_presentation(
    request: &A2aArtifactIngestionRequest,
) -> Result<ArtifactPresentation, ArtifactStoreError> {
    let name = request
        .part()
        .filename()
        .or_else(|| request.artifact_name())
        .map_or_else(
            || {
                ArtifactName::new(format!(
                    "a2a-artifact-{}-{}",
                    request.source().artifact_index(),
                    request.source().part_index()
                ))
            },
            ArtifactName::new,
        )
        .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
    let description = request
        .artifact_description()
        .map(ArtifactDescription::new)
        .transpose()
        .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
    Ok(ArtifactPresentation::new(name, description))
}

fn update_bounded_field(hasher: &mut Sha256, value: &[u8]) -> Result<(), ArtifactStoreError> {
    let length = u32::try_from(value.len())
        .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
    hasher.update(length.to_be_bytes());
    hasher.update(value);
    Ok(())
}

fn declared_media_type(
    declared: Option<&str>,
    default: &str,
) -> Result<MediaType, ArtifactStoreError> {
    declared
        .unwrap_or(default)
        .parse()
        .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))
}

fn select_remote_media_type(
    declared: Option<&str>,
    response: Option<MediaType>,
) -> Result<MediaType, ArtifactStoreError> {
    let declared = declared
        .map(str::parse::<MediaType>)
        .transpose()
        .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))?;
    match (declared, response) {
        (Some(declared), Some(response)) if declared != response => {
            Err(classified(ArtifactStoreErrorKind::Integrity))
        }
        (Some(declared), _) => Ok(declared),
        (None, Some(response)) => Ok(response),
        (None, None) => "application/octet-stream"
            .parse()
            .map_err(|_| classified(ArtifactStoreErrorKind::Integrity)),
    }
}

fn validate_content_encoding(
    headers: &reqwest::header::HeaderMap,
) -> Result<(), ArtifactStoreError> {
    if let Some(value) = headers.get(header::CONTENT_ENCODING)
        && !value
            .to_str()
            .is_ok_and(|value| value.eq_ignore_ascii_case("identity"))
    {
        return Err(classified(ArtifactStoreErrorKind::InvalidContent));
    }
    Ok(())
}

fn validate_text_media_type(media_type: &MediaType) -> Result<(), ArtifactStoreError> {
    if media_type.top_level() != "text" || has_non_utf8_charset(media_type) {
        return Err(classified(ArtifactStoreErrorKind::InvalidContent));
    }
    Ok(())
}

fn validate_json_media_type(media_type: &MediaType) -> Result<(), ArtifactStoreError> {
    if !(media_type.essence() == "application/json" || media_type.subtype().ends_with("+json"))
        || has_non_utf8_charset(media_type)
    {
        return Err(classified(ArtifactStoreErrorKind::InvalidContent));
    }
    Ok(())
}

fn has_non_utf8_charset(media_type: &MediaType) -> bool {
    media_type
        .as_str()
        .split(';')
        .skip(1)
        .filter_map(|parameter| parameter.split_once('='))
        .any(|(name, value)| name == "charset" && value.trim_matches('"') != "utf-8")
}

fn modality_for_media_type(media_type: &MediaType) -> ArtifactModality {
    match media_type.top_level() {
        "text" => ArtifactModality::Text,
        "image" => ArtifactModality::Image,
        "audio" => ArtifactModality::Audio,
        "video" => ArtifactModality::Video,
        _ if media_type.essence() == "application/json"
            || media_type.subtype().ends_with("+json") =>
        {
            ArtifactModality::StructuredData
        }
        _ if matches!(
            media_type.essence(),
            "application/pdf"
                | "application/msword"
                | "application/rtf"
                | "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ) =>
        {
            ArtifactModality::Document
        }
        _ if matches!(
            media_type.essence(),
            "application/gzip"
                | "application/zip"
                | "application/x-7z-compressed"
                | "application/x-tar"
        ) =>
        {
            ArtifactModality::Archive
        }
        _ => ArtifactModality::Binary,
    }
}

fn validate_interpreted_bytes(
    media_type: &MediaType,
    bytes: &[u8],
) -> Result<(), ArtifactStoreError> {
    match modality_for_media_type(media_type) {
        ArtifactModality::Text => {
            validate_text_media_type(media_type)?;
            std::str::from_utf8(bytes)
                .map(|_| ())
                .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))
        }
        ArtifactModality::StructuredData => {
            validate_json_media_type(media_type)?;
            serde_json::from_slice::<serde_json::Value>(bytes)
                .map(|_| ())
                .map_err(|_| classified(ArtifactStoreErrorKind::InvalidContent))
        }
        _ => Ok(()),
    }
}

fn parse_object_path(value: &str) -> Result<Path, ArtifactStoreError> {
    Path::parse(value).map_err(|_| classified(ArtifactStoreErrorKind::Integrity))
}

fn digest_from_hasher(hasher: Sha256) -> Digest {
    let bytes: [u8; 32] = hasher.finalize().into();
    Digest::from_sha256(bytes)
}

fn hex_digest(digest: Digest) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(Digest::SHA256_LEN * 2);
    for byte in digest.as_bytes() {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn map_registry_error(error: StoreError) -> ArtifactStoreError {
    let kind = match &error {
        StoreError::ArtifactNotFound => ArtifactStoreErrorKind::NotFound,
        StoreError::Database { .. } if error.is_retryable() => ArtifactStoreErrorKind::Unavailable,
        StoreError::Database { .. } => ArtifactStoreErrorKind::Unavailable,
        StoreError::Configuration(_)
        | StoreError::SchemaNotMigrated
        | StoreError::IncompatibleSchema
        | StoreError::IncompleteSchema
        | StoreError::UnsupportedServerVersion => ArtifactStoreErrorKind::Configuration,
        _ => ArtifactStoreErrorKind::Integrity,
    };
    ArtifactStoreError::new(kind, error)
}

fn map_locator_configuration_error(error: StoreError) -> ArtifactStoreError {
    ArtifactStoreError::new(ArtifactStoreErrorKind::Configuration, error)
}

fn map_object_unavailable(error: object_store::Error) -> ArtifactStoreError {
    ArtifactStoreError::new(ArtifactStoreErrorKind::Unavailable, error)
}

fn map_object_read_error(error: object_store::Error) -> ArtifactStoreError {
    let kind = if matches!(
        error,
        object_store::Error::NotFound { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotModified { .. }
    ) {
        ArtifactStoreErrorKind::Integrity
    } else {
        ArtifactStoreErrorKind::Unavailable
    };
    ArtifactStoreError::new(kind, error)
}

async fn run_with_timeout<T>(
    timeout: std::time::Duration,
    future: impl Future<Output = T>,
) -> Result<T, ()> {
    tokio::time::timeout(timeout, future).await.map_err(|_| ())
}

async fn run_until<T>(
    deadline: tokio::time::Instant,
    future: impl Future<Output = T>,
) -> Result<T, ()> {
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| ())
}

fn map_timed_object_result<T>(
    result: Result<object_store::Result<T>, ()>,
) -> Result<T, ArtifactStoreError> {
    result
        .map_err(|()| classified(ArtifactStoreErrorKind::Unavailable))?
        .map_err(map_object_unavailable)
}
