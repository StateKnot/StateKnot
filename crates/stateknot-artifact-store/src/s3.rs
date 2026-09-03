// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::Arc,
};

use object_store::{
    ObjectStore,
    aws::{AmazonS3Builder, AmazonS3ConfigKey, Checksum, S3CopyIfNotExists},
};
use reqwest::{StatusCode, Url, header::HeaderName, header::HeaderValue};

use crate::{ArtifactStoreError, ArtifactStoreErrorKind, error::classified};

/// Backend-specific atomic destination-create strategy.
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum S3ConditionalCopy {
    /// Native Amazon S3 conditional multipart-copy completion.
    AmazonMultipart,
    /// Provider-specific destination precondition header with HTTP 412 failure.
    Header {
        /// Valid HTTP header name.
        name: Box<str>,
        /// Valid HTTP header value, normally `*`.
        value: Box<str>,
    },
    /// Provider-specific destination precondition header and failure status.
    HeaderWithStatus {
        /// Valid HTTP header name.
        name: Box<str>,
        /// Valid HTTP header value.
        value: Box<str>,
        /// Expected 4xx precondition status.
        status: u16,
    },
}

impl S3ConditionalCopy {
    /// Validates a provider-specific conditional destination-copy header.
    ///
    /// # Errors
    ///
    /// Rejects invalid HTTP header syntax.
    pub fn header(
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, ArtifactStoreError> {
        let name = name.into();
        let value = value.into();
        validate_header(&name, &value)?;
        Ok(Self::Header {
            name: name.into_boxed_str(),
            value: value.into_boxed_str(),
        })
    }

    /// Validates a provider-specific header and 4xx precondition status.
    ///
    /// # Errors
    ///
    /// Rejects invalid header syntax or a non-4xx status.
    pub fn header_with_status(
        name: impl Into<String>,
        value: impl Into<String>,
        status: u16,
    ) -> Result<Self, ArtifactStoreError> {
        let name = name.into();
        let value = value.into();
        validate_header(&name, &value)?;
        let status_code = StatusCode::from_u16(status)
            .map_err(|_| classified(ArtifactStoreErrorKind::Configuration))?;
        if !status_code.is_client_error() {
            return Err(classified(ArtifactStoreErrorKind::Configuration));
        }
        Ok(Self::HeaderWithStatus {
            name: name.into_boxed_str(),
            value: value.into_boxed_str(),
            status,
        })
    }

    fn into_object_store(self) -> S3CopyIfNotExists {
        match self {
            Self::AmazonMultipart => S3CopyIfNotExists::Multipart,
            Self::Header { name, value } => S3CopyIfNotExists::Header(name.into(), value.into()),
            Self::HeaderWithStatus {
                name,
                value,
                status,
            } => S3CopyIfNotExists::HeaderWithStatus(
                name.into(),
                value.into(),
                StatusCode::from_u16(status).expect("constructor validates the status"),
            ),
        }
    }
}

impl fmt::Debug for S3ConditionalCopy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AmazonMultipart => formatter.write_str("S3ConditionalCopy::AmazonMultipart"),
            Self::Header { .. } => formatter
                .debug_struct("S3ConditionalCopy::Header")
                .field("header", &"<redacted>")
                .finish(),
            Self::HeaderWithStatus { status, .. } => formatter
                .debug_struct("S3ConditionalCopy::HeaderWithStatus")
                .field("header", &"<redacted>")
                .field("status", status)
                .finish(),
        }
    }
}

/// Credential-redacted builder for AWS S3 and compatible private stores.
///
/// Credentials are resolved by `object_store` from an allowlisted subset of
/// the standard AWS environment/IAM chain. Endpoint, transport, retry,
/// encryption, bucket, and region environment variables are deliberately not
/// inherited. This wrapper has no methods accepting static access keys.
/// Production custom endpoints require HTTPS; literal loopback HTTP is
/// available only through an explicit test/sidecar method.
pub struct S3CompatibleBackendBuilder {
    inner: AmazonS3Builder,
    conditional_copy: S3ConditionalCopy,
}

impl S3CompatibleBackendBuilder {
    /// Creates a builder using the AWS environment/IAM credential chain.
    ///
    /// Only static/session credential, web-identity, and ECS/EKS container
    /// credential variables are imported. In particular, `AWS_ENDPOINT*`,
    /// `AWS_ALLOW_HTTP`, metadata overrides, unsigned requests, and client
    /// behavior variables cannot alter this boundary.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, boundary-whitespace, or control-bearing bucket
    /// and region values.
    pub fn from_env(
        bucket: impl Into<String>,
        region: impl Into<String>,
    ) -> Result<Self, ArtifactStoreError> {
        let bucket = bucket.into();
        let region = region.into();
        validate_bounded_config(&bucket, 255)?;
        validate_bounded_config(&region, 128)?;
        Ok(Self {
            inner: credentialed_builder_from_environment()?
                .with_bucket_name(bucket)
                .with_region(region),
            conditional_copy: S3ConditionalCopy::AmazonMultipart,
        })
    }

    /// Selects the backend's exact conditional destination-copy mechanism.
    #[must_use]
    pub fn with_conditional_copy(mut self, strategy: S3ConditionalCopy) -> Self {
        self.conditional_copy = strategy;
        self
    }

    /// Sets a production HTTPS S3-compatible endpoint.
    ///
    /// # Errors
    ///
    /// Rejects credentials, query/fragment data, a non-root path, missing host,
    /// or non-HTTPS transport.
    pub fn with_https_endpoint(
        mut self,
        endpoint: impl Into<String>,
    ) -> Result<Self, ArtifactStoreError> {
        let endpoint = endpoint.into();
        validate_endpoint(&endpoint, false)?;
        self.inner = self.inner.with_endpoint(endpoint).with_allow_http(false);
        Ok(self)
    }

    /// Sets a trusted HTTPS STS endpoint for web-identity exchange.
    ///
    /// This is useful for non-default AWS partitions without permitting an
    /// unvalidated `AWS_ENDPOINT_URL_STS` environment override.
    ///
    /// # Errors
    ///
    /// Rejects credentials, query/fragment data, a non-root path, missing host,
    /// or non-HTTPS transport.
    pub fn with_https_sts_endpoint(
        mut self,
        endpoint: impl Into<String>,
    ) -> Result<Self, ArtifactStoreError> {
        let endpoint = endpoint.into();
        validate_endpoint(&endpoint, false)?;
        self.inner = self
            .inner
            .with_config(AmazonS3ConfigKey::StsEndpoint, endpoint);
        Ok(self)
    }

    /// Sets a literal-loopback HTTP endpoint for tests or a managed sidecar.
    ///
    /// # Errors
    ///
    /// Rejects hostnames, non-loopback IPs, credentials, query/fragment data,
    /// a non-root path, or non-HTTP transport.
    pub fn with_loopback_http_endpoint(
        mut self,
        endpoint: impl Into<String>,
    ) -> Result<Self, ArtifactStoreError> {
        let endpoint = endpoint.into();
        validate_endpoint(&endpoint, true)?;
        self.inner = self.inner.with_endpoint(endpoint).with_allow_http(true);
        Ok(self)
    }

    /// Selects virtual-hosted rather than path-style bucket addressing.
    #[must_use]
    pub fn with_virtual_hosted_style(mut self, enabled: bool) -> Self {
        self.inner = self.inner.with_virtual_hosted_style_request(enabled);
        self
    }

    /// Requests SHA-256 transport checksums on supported S3 implementations.
    #[must_use]
    pub fn with_sha256_checksum(mut self) -> Self {
        self.inner = self.inner.with_checksum_algorithm(Checksum::SHA256);
        self
    }

    /// Enables AWS KMS server-side encryption without exposing the key in `Debug`.
    ///
    /// # Errors
    ///
    /// Rejects an empty, oversized, boundary-whitespace, or control-bearing key ID.
    pub fn with_kms_key_id(
        mut self,
        key_id: impl Into<String>,
    ) -> Result<Self, ArtifactStoreError> {
        let key_id = key_id.into();
        validate_bounded_config(&key_id, 2048)?;
        self.inner = self.inner.with_sse_kms_encryption(key_id);
        Ok(self)
    }

    /// Builds the credentialed backend. `StateKnotArtifactStore::initialize`
    /// subsequently verifies its real atomic-copy behavior before use.
    ///
    /// # Errors
    ///
    /// Returns a redacted configuration error when provider construction fails.
    pub fn build(self) -> Result<Arc<dyn ObjectStore>, ArtifactStoreError> {
        self.inner
            .with_copy_if_not_exists(self.conditional_copy.into_object_store())
            .build()
            .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
            .map_err(|source| {
                ArtifactStoreError::new(ArtifactStoreErrorKind::Configuration, source)
            })
    }
}

impl fmt::Debug for S3CompatibleBackendBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3CompatibleBackendBuilder")
            .field("configuration", &"<redacted>")
            .field("conditional_copy", &self.conditional_copy)
            .finish_non_exhaustive()
    }
}

fn validate_header(name: &str, value: &str) -> Result<(), ArtifactStoreError> {
    HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| classified(ArtifactStoreErrorKind::Configuration))?;
    HeaderValue::from_str(value).map_err(|_| classified(ArtifactStoreErrorKind::Configuration))?;
    Ok(())
}

fn credentialed_builder_from_environment() -> Result<AmazonS3Builder, ArtifactStoreError> {
    let mut builder = AmazonS3Builder::new();
    for (name, value) in std::env::vars_os() {
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(key) = credential_environment_key(name) else {
            continue;
        };
        let value = value
            .into_string()
            .map_err(|_| classified(ArtifactStoreErrorKind::Configuration))?;
        match key {
            AmazonS3ConfigKey::ContainerCredentialsRelativeUri => {
                validate_container_credentials_relative_uri(&value)?;
            }
            AmazonS3ConfigKey::ContainerCredentialsFullUri => {
                validate_container_credentials_full_uri(&value)?;
            }
            _ => {}
        }
        builder = builder.with_config(key, value);
    }
    Ok(builder)
}

const fn credential_environment_key(name: &str) -> Option<AmazonS3ConfigKey> {
    match name.as_bytes() {
        b"AWS_ACCESS_KEY_ID" => Some(AmazonS3ConfigKey::AccessKeyId),
        b"AWS_SECRET_ACCESS_KEY" => Some(AmazonS3ConfigKey::SecretAccessKey),
        b"AWS_SESSION_TOKEN" => Some(AmazonS3ConfigKey::Token),
        b"AWS_WEB_IDENTITY_TOKEN_FILE" => Some(AmazonS3ConfigKey::WebIdentityTokenFile),
        b"AWS_ROLE_ARN" => Some(AmazonS3ConfigKey::RoleArn),
        b"AWS_ROLE_SESSION_NAME" => Some(AmazonS3ConfigKey::RoleSessionName),
        b"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" => {
            Some(AmazonS3ConfigKey::ContainerCredentialsRelativeUri)
        }
        b"AWS_CONTAINER_CREDENTIALS_FULL_URI" => {
            Some(AmazonS3ConfigKey::ContainerCredentialsFullUri)
        }
        b"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE" => {
            Some(AmazonS3ConfigKey::ContainerAuthorizationTokenFile)
        }
        _ => None,
    }
}

fn validate_container_credentials_relative_uri(value: &str) -> Result<(), ArtifactStoreError> {
    if value.len() > 2048
        || !value.starts_with('/')
        || value.starts_with("//")
        || value.chars().any(char::is_control)
    {
        return Err(classified(ArtifactStoreErrorKind::Configuration));
    }
    Ok(())
}

fn validate_container_credentials_full_uri(value: &str) -> Result<(), ArtifactStoreError> {
    if value.len() > 4096 {
        return Err(classified(ArtifactStoreErrorKind::Configuration));
    }
    let url = Url::parse(value).map_err(|_| classified(ArtifactStoreErrorKind::Configuration))?;
    let address = url
        .host_str()
        .and_then(|host| host.trim_matches(['[', ']']).parse::<IpAddr>().ok())
        .ok_or_else(|| classified(ArtifactStoreErrorKind::Configuration))?;
    let ecs = IpAddr::V4(Ipv4Addr::new(169, 254, 170, 2));
    let eks_v4 = IpAddr::V4(Ipv4Addr::new(169, 254, 170, 23));
    let eks_v6 = IpAddr::V6(Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x23));
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || !(address.is_loopback()
            || matches!(address, value if value == ecs || value == eks_v4 || value == eks_v6))
    {
        return Err(classified(ArtifactStoreErrorKind::Configuration));
    }
    Ok(())
}

fn validate_bounded_config(value: &str, maximum: usize) -> Result<(), ArtifactStoreError> {
    if value.is_empty()
        || value.len() > maximum
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(classified(ArtifactStoreErrorKind::Configuration));
    }
    Ok(())
}

fn validate_endpoint(value: &str, loopback_http: bool) -> Result<(), ArtifactStoreError> {
    let url = Url::parse(value).map_err(|_| classified(ArtifactStoreErrorKind::Configuration))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.host_str().is_none()
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(classified(ArtifactStoreErrorKind::Configuration));
    }
    if loopback_http {
        let address = url
            .host_str()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .filter(IpAddr::is_loopback)
            .ok_or_else(|| classified(ArtifactStoreErrorKind::Configuration))?;
        if url.scheme() != "http" || !address.is_loopback() {
            return Err(classified(ArtifactStoreErrorKind::Configuration));
        }
    } else if url.scheme() != "https" {
        return Err(classified(ArtifactStoreErrorKind::Configuration));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        credential_environment_key, validate_container_credentials_full_uri,
        validate_container_credentials_relative_uri,
    };

    #[test]
    fn credential_environment_allowlist_excludes_transport_and_backend_configuration() {
        for name in [
            "AWS_ENDPOINT",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
            "AWS_ENDPOINT_URL_STS",
            "AWS_ALLOW_HTTP",
            "AWS_METADATA_ENDPOINT",
            "AWS_IMDSV1_FALLBACK",
            "AWS_SKIP_SIGNATURE",
            "AWS_BUCKET",
            "AWS_REGION",
            "AWS_COPY_IF_NOT_EXISTS",
            "AWS_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY",
        ] {
            assert!(credential_environment_key(name).is_none(), "{name}");
        }
        for name in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_ROLE_ARN",
            "AWS_ROLE_SESSION_NAME",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        ] {
            assert!(credential_environment_key(name).is_some(), "{name}");
        }
    }

    #[test]
    fn container_credential_endpoints_remain_local_and_bounded() {
        for value in [
            "http://169.254.170.2/v2/credentials/id",
            "http://169.254.170.23/v1/credentials",
            "http://[fd00:ec2::23]/v1/credentials",
            "http://127.0.0.1:9911/credentials",
        ] {
            validate_container_credentials_full_uri(value).unwrap();
        }
        for value in [
            "http://example.com/credentials",
            "http://169.254.169.254/latest/meta-data",
            "http://user@169.254.170.2/credentials",
            "file:///var/run/credentials",
        ] {
            assert!(validate_container_credentials_full_uri(value).is_err());
        }
        validate_container_credentials_relative_uri("/v2/credentials/id").unwrap();
        for value in [
            "",
            "credentials/id",
            "//example.com/credentials",
            "/bad\npath",
        ] {
            assert!(validate_container_credentials_relative_uri(value).is_err());
        }
    }
}
