// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeMap,
    fmt,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use reqwest::Url;
use stateknot_core::{RetentionClass, SecurityLabel};

use crate::{ArtifactStoreError, ArtifactStoreErrorKind, error::classified};

const MEBIBYTE: usize = 1024 * 1024;

/// One exact egress-approved origin for remote A2A artifact URLs.
///
/// Production HTTPS hostnames require explicit IP pins. The HTTP constructor
/// accepts only a literal loopback address and exists for tests and managed
/// same-host sidecars. Paths, queries, and credentials are not part of an
/// origin; individual signed artifact URLs may carry paths and queries after
/// their origin is matched.
#[derive(Clone, Eq, PartialEq)]
pub struct RemoteArtifactOrigin {
    origin: Box<str>,
    host: Box<str>,
    pinned_addresses: Box<[IpAddr]>,
    loopback_http: bool,
}

impl RemoteArtifactOrigin {
    /// Pins one exact HTTPS origin and every address it may connect to.
    ///
    /// # Errors
    ///
    /// Rejects malformed origins, credentials, a non-root path, query or
    /// fragment data, non-HTTPS transport, or an empty/excessive/duplicate pin
    /// set. A literal-IP origin must be pinned only to that exact address.
    pub fn https(
        origin: &str,
        pinned_addresses: impl IntoIterator<Item = IpAddr>,
    ) -> Result<Self, ArtifactStoreError> {
        let url = parse_origin(origin, "https")?;
        let mut pinned_addresses = pinned_addresses.into_iter().collect::<Vec<_>>();
        pinned_addresses.sort_unstable();
        if pinned_addresses.is_empty()
            || pinned_addresses.len() > 16
            || pinned_addresses.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(classified(ArtifactStoreErrorKind::Configuration));
        }
        let host = url
            .host_str()
            .ok_or_else(|| classified(ArtifactStoreErrorKind::Configuration))?;
        if let Ok(literal) = host.parse::<IpAddr>() {
            if pinned_addresses.as_slice() != [literal] {
                return Err(classified(ArtifactStoreErrorKind::Configuration));
            }
        }
        Ok(Self {
            origin: url.origin().ascii_serialization().into_boxed_str(),
            host: host.to_ascii_lowercase().into_boxed_str(),
            pinned_addresses: pinned_addresses.into_boxed_slice(),
            loopback_http: false,
        })
    }

    /// Allows one literal-loopback HTTP origin for a test or managed sidecar.
    ///
    /// # Errors
    ///
    /// Rejects hostnames, non-loopback addresses, credentials, non-root paths,
    /// query/fragment data, or any scheme other than HTTP.
    pub fn loopback_http(origin: &str) -> Result<Self, ArtifactStoreError> {
        let url = parse_origin(origin, "http")?;
        let host = url
            .host_str()
            .ok_or_else(|| classified(ArtifactStoreErrorKind::Configuration))?;
        let address = host
            .parse::<IpAddr>()
            .map_err(|_| classified(ArtifactStoreErrorKind::Configuration))?;
        if !address.is_loopback() {
            return Err(classified(ArtifactStoreErrorKind::Configuration));
        }
        Ok(Self {
            origin: url.origin().ascii_serialization().into_boxed_str(),
            host: host.to_ascii_lowercase().into_boxed_str(),
            pinned_addresses: Box::new([address]),
            loopback_http: true,
        })
    }

    pub(crate) fn allows(&self, url: &Url) -> bool {
        url.origin().ascii_serialization() == self.origin.as_ref()
            && if self.loopback_http {
                url.scheme() == "http"
            } else {
                url.scheme() == "https"
            }
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) fn socket_addresses(&self) -> Vec<SocketAddr> {
        self.pinned_addresses
            .iter()
            .copied()
            .map(|address| SocketAddr::new(address, 0))
            .collect()
    }
}

impl fmt::Debug for RemoteArtifactOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteArtifactOrigin")
            .field("origin", &"<redacted>")
            .field("pinned_address_count", &self.pinned_addresses.len())
            .field("loopback_http", &self.loopback_http)
            .finish_non_exhaustive()
    }
}

/// Bounded storage, download, and resolution policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactStoreOptions {
    operation_timeout: Duration,
    remote_timeout: Duration,
    multipart_chunk_bytes: usize,
    maximum_remote_bytes: u64,
    maximum_resolved_bytes: u64,
    maximum_redirects: u8,
    maximum_concurrent_operations: usize,
    retention_class: RetentionClass,
    security_label: SecurityLabel,
    remote_origins: Box<[RemoteArtifactOrigin]>,
}

impl ArtifactStoreOptions {
    /// Smallest portable multipart part size for S3-compatible services.
    pub const MIN_MULTIPART_CHUNK_BYTES: usize = 5 * MEBIBYTE;
    /// Largest in-memory multipart accumulation permitted by this provider.
    pub const MAX_MULTIPART_CHUNK_BYTES: usize = 64 * MEBIBYTE;
    /// Hard per-object remote ingestion and materialized-read ceiling.
    pub const HARD_MAXIMUM_OBJECT_BYTES: u64 = 1024 * MEBIBYTE as u64;
    /// Largest redirect chain accepted after per-hop origin validation.
    pub const HARD_MAXIMUM_REDIRECTS: u8 = 3;
    /// Largest process-local artifact-operation concurrency admitted by one store.
    pub const HARD_MAXIMUM_CONCURRENT_OPERATIONS: usize = 256;

    /// Replaces exact egress-approved URL origins.
    ///
    /// # Errors
    ///
    /// Rejects more than 32 origins, duplicates, or two origins that use the
    /// same hostname with different DNS pins.
    pub fn with_remote_origins(
        mut self,
        origins: impl IntoIterator<Item = RemoteArtifactOrigin>,
    ) -> Result<Self, ArtifactStoreError> {
        let mut origins = origins.into_iter().collect::<Vec<_>>();
        if origins.len() > 32 {
            return Err(classified(ArtifactStoreErrorKind::Configuration));
        }
        origins.sort_by(|left, right| left.origin.cmp(&right.origin));
        if origins
            .windows(2)
            .any(|pair| pair[0].origin == pair[1].origin)
        {
            return Err(classified(ArtifactStoreErrorKind::Configuration));
        }
        let mut host_pins = BTreeMap::<&str, &[IpAddr]>::new();
        for origin in &origins {
            if let Some(existing) = host_pins.insert(origin.host(), &origin.pinned_addresses)
                && existing != origin.pinned_addresses.as_ref()
            {
                return Err(classified(ArtifactStoreErrorKind::Configuration));
            }
        }
        self.remote_origins = origins.into_boxed_slice();
        Ok(self)
    }

    /// Replaces operation and complete remote-download deadlines.
    ///
    /// # Errors
    ///
    /// Rejects zero values or durations above ten minutes.
    pub fn with_timeouts(
        mut self,
        operation_timeout: Duration,
        remote_timeout: Duration,
    ) -> Result<Self, ArtifactStoreError> {
        const MAXIMUM: Duration = Duration::from_secs(10 * 60);
        if operation_timeout.is_zero()
            || remote_timeout.is_zero()
            || operation_timeout > MAXIMUM
            || remote_timeout > MAXIMUM
        {
            return Err(classified(ArtifactStoreErrorKind::Configuration));
        }
        self.operation_timeout = operation_timeout;
        self.remote_timeout = remote_timeout;
        Ok(self)
    }

    /// Replaces byte, multipart, and redirect ceilings.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive byte ceilings, multipart parts outside the
    /// portable 5–64 MiB interval, or more than three redirects.
    pub fn with_limits(
        mut self,
        maximum_remote_bytes: u64,
        maximum_resolved_bytes: u64,
        multipart_chunk_bytes: usize,
        maximum_redirects: u8,
    ) -> Result<Self, ArtifactStoreError> {
        if maximum_remote_bytes == 0
            || maximum_remote_bytes > Self::HARD_MAXIMUM_OBJECT_BYTES
            || maximum_resolved_bytes == 0
            || maximum_resolved_bytes > Self::HARD_MAXIMUM_OBJECT_BYTES
            || !(Self::MIN_MULTIPART_CHUNK_BYTES..=Self::MAX_MULTIPART_CHUNK_BYTES)
                .contains(&multipart_chunk_bytes)
            || maximum_redirects > Self::HARD_MAXIMUM_REDIRECTS
        {
            return Err(classified(ArtifactStoreErrorKind::Configuration));
        }
        self.maximum_remote_bytes = maximum_remote_bytes;
        self.maximum_resolved_bytes = maximum_resolved_bytes;
        self.multipart_chunk_bytes = multipart_chunk_bytes;
        self.maximum_redirects = maximum_redirects;
        Ok(self)
    }

    /// Replaces the process-local artifact-operation concurrency ceiling.
    ///
    /// A permit covers the complete ingestion or materialized-read operation,
    /// including authorization, registry, network, and object-store work.
    ///
    /// # Errors
    ///
    /// Rejects zero or more than 256 concurrent operations.
    pub fn with_concurrency_limit(
        mut self,
        maximum_concurrent_operations: usize,
    ) -> Result<Self, ArtifactStoreError> {
        if !(1..=Self::HARD_MAXIMUM_CONCURRENT_OPERATIONS).contains(&maximum_concurrent_operations)
        {
            return Err(classified(ArtifactStoreErrorKind::Configuration));
        }
        self.maximum_concurrent_operations = maximum_concurrent_operations;
        Ok(self)
    }

    /// Replaces the policy-interpreted retention and untrusted-content label.
    #[must_use]
    pub fn with_artifact_metadata(
        mut self,
        retention_class: RetentionClass,
        security_label: SecurityLabel,
    ) -> Self {
        self.retention_class = retention_class;
        self.security_label = security_label;
        self
    }

    pub(crate) const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    pub(crate) const fn multipart_chunk_bytes(&self) -> usize {
        self.multipart_chunk_bytes
    }

    pub(crate) const fn maximum_remote_bytes(&self) -> u64 {
        self.maximum_remote_bytes
    }

    pub(crate) const fn maximum_resolved_bytes(&self) -> u64 {
        self.maximum_resolved_bytes
    }

    pub(crate) const fn maximum_redirects(&self) -> u8 {
        self.maximum_redirects
    }

    pub(crate) const fn maximum_concurrent_operations(&self) -> usize {
        self.maximum_concurrent_operations
    }

    pub(crate) const fn retention_class(&self) -> &RetentionClass {
        &self.retention_class
    }

    pub(crate) const fn security_label(&self) -> &SecurityLabel {
        &self.security_label
    }

    pub(crate) const fn remote_origins(&self) -> &[RemoteArtifactOrigin] {
        &self.remote_origins
    }

    pub(crate) fn build_http_client(&self) -> Result<reqwest::Client, ArtifactStoreError> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(self.operation_timeout)
            .timeout(self.remote_timeout)
            .pool_idle_timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .user_agent(concat!(
                "stateknot-artifact-store/",
                env!("CARGO_PKG_VERSION")
            ));
        let mut hosts = BTreeMap::<&str, Vec<SocketAddr>>::new();
        for origin in &self.remote_origins {
            hosts
                .entry(origin.host())
                .or_insert_with(|| origin.socket_addresses());
        }
        for (host, addresses) in hosts {
            if host.parse::<IpAddr>().is_err() {
                builder = builder.resolve_to_addrs(host, &addresses);
            }
        }
        builder.build().map_err(|source| {
            ArtifactStoreError::new(ArtifactStoreErrorKind::Configuration, source)
        })
    }
}

impl Default for ArtifactStoreOptions {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(60),
            remote_timeout: Duration::from_secs(120),
            multipart_chunk_bytes: 8 * MEBIBYTE,
            maximum_remote_bytes: 64 * MEBIBYTE as u64,
            maximum_resolved_bytes: 64 * MEBIBYTE as u64,
            maximum_redirects: 3,
            maximum_concurrent_operations: 8,
            retention_class: RetentionClass::new("standard")
                .expect("the built-in retention class is valid"),
            security_label: SecurityLabel::new("external/a2a")
                .expect("the built-in security label is valid"),
            remote_origins: Box::new([]),
        }
    }
}

fn parse_origin(value: &str, expected_scheme: &str) -> Result<Url, ArtifactStoreError> {
    let url = Url::parse(value).map_err(|_| classified(ArtifactStoreErrorKind::Configuration))?;
    if url.scheme() != expected_scheme
        || !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(classified(ArtifactStoreErrorKind::Configuration));
    }
    Ok(url)
}
