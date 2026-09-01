// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, net::IpAddr, time::Duration};

use reqwest::Url;
use thiserror::Error;

const MEBIBYTE: usize = 1024 * 1024;

/// Validated base endpoint for one immutable provider binding.
#[derive(Clone, Eq, PartialEq)]
pub struct ProviderEndpoint {
    base: Url,
    transport: EndpointTransport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EndpointTransport {
    Https,
    LoopbackHttp,
}

impl ProviderEndpoint {
    /// Constructs a production HTTPS endpoint.
    ///
    /// # Errors
    ///
    /// Rejects non-HTTPS URLs, embedded credentials, query/fragment data, an
    /// absent host, or a base URL that cannot be normalized as a directory.
    pub fn https(value: &str) -> Result<Self, ProviderEndpointError> {
        Self::parse(value, EndpointTransport::Https)
    }

    /// Constructs an HTTP endpoint limited to a literal loopback IP address.
    ///
    /// This constructor exists for local integration tests and explicitly
    /// managed sidecars. Hostnames such as `localhost` are rejected to avoid
    /// DNS rebinding and deployment-dependent resolution.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderEndpointError`] unless the URL uses `http` and a
    /// literal loopback address with the same structural restrictions as
    /// [`Self::https`].
    pub fn loopback_http(value: &str) -> Result<Self, ProviderEndpointError> {
        Self::parse(value, EndpointTransport::LoopbackHttp)
    }

    fn parse(value: &str, transport: EndpointTransport) -> Result<Self, ProviderEndpointError> {
        let mut base = Url::parse(value).map_err(|_| ProviderEndpointError::InvalidUrl)?;
        if !base.username().is_empty() || base.password().is_some() {
            return Err(ProviderEndpointError::EmbeddedCredentials);
        }
        if base.query().is_some() || base.fragment().is_some() {
            return Err(ProviderEndpointError::QueryOrFragment);
        }
        let host = base.host_str().ok_or(ProviderEndpointError::MissingHost)?;
        match transport {
            EndpointTransport::Https if base.scheme() != "https" => {
                return Err(ProviderEndpointError::HttpsRequired);
            }
            EndpointTransport::LoopbackHttp => {
                if base.scheme() != "http" {
                    return Err(ProviderEndpointError::LoopbackHttpRequired);
                }
                let address = host
                    .parse::<IpAddr>()
                    .map_err(|_| ProviderEndpointError::LiteralLoopbackRequired)?;
                if !address.is_loopback() {
                    return Err(ProviderEndpointError::LiteralLoopbackRequired);
                }
            }
            EndpointTransport::Https => {}
        }

        if !base.path().ends_with('/') {
            let mut path = base.path().to_owned();
            path.push('/');
            base.set_path(&path);
        }
        Ok(Self { base, transport })
    }

    pub(crate) fn join(&self, relative: &str) -> Result<Url, ProviderEndpointError> {
        self.base
            .join(relative)
            .map_err(|_| ProviderEndpointError::InvalidRelativePath)
    }

    /// Returns whether this endpoint uses TLS.
    #[must_use]
    pub const fn is_https(&self) -> bool {
        matches!(self.transport, EndpointTransport::Https)
    }
}

impl fmt::Debug for ProviderEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderEndpoint")
            .field("transport", &self.transport)
            .field("host", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

/// Invalid provider endpoint configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ProviderEndpointError {
    /// URL parsing failed.
    #[error("provider endpoint is not an absolute URL")]
    InvalidUrl,
    /// Production endpoints must use HTTPS.
    #[error("provider endpoint must use HTTPS")]
    HttpsRequired,
    /// The local-only constructor requires HTTP.
    #[error("loopback provider endpoint must use HTTP")]
    LoopbackHttpRequired,
    /// A local-only endpoint was not a literal loopback IP.
    #[error("loopback provider endpoint must use a literal loopback IP address")]
    LiteralLoopbackRequired,
    /// URL authority had no host.
    #[error("provider endpoint must include a host")]
    MissingHost,
    /// URL userinfo could leak credentials.
    #[error("provider endpoint must not contain embedded credentials")]
    EmbeddedCredentials,
    /// Query and fragment data are not part of an immutable API base.
    #[error("provider endpoint must not contain a query or fragment")]
    QueryOrFragment,
    /// An adapter requested an invalid fixed API path.
    #[error("provider API path could not be joined to the configured endpoint")]
    InvalidRelativePath,
}

/// Bounded HTTP and SSE resource policy for provider exchanges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderHttpOptions {
    connect_timeout: Duration,
    pool_idle_timeout: Duration,
    maximum_request_bytes: usize,
    maximum_response_bytes: usize,
    maximum_sse_line_bytes: usize,
    maximum_sse_event_bytes: usize,
    maximum_sse_total_bytes: usize,
}

impl ProviderHttpOptions {
    /// Absolute request body ceiling.
    pub const HARD_MAXIMUM_REQUEST_BYTES: usize = 32 * MEBIBYTE;
    /// Absolute complete-response body ceiling.
    pub const HARD_MAXIMUM_RESPONSE_BYTES: usize = 2 * MEBIBYTE;
    /// Absolute SSE line ceiling.
    pub const HARD_MAXIMUM_SSE_LINE_BYTES: usize = 2 * MEBIBYTE;
    /// Absolute SSE event ceiling.
    pub const HARD_MAXIMUM_SSE_EVENT_BYTES: usize = 2 * MEBIBYTE;
    /// Absolute total streaming body ceiling.
    pub const HARD_MAXIMUM_SSE_TOTAL_BYTES: usize = 72 * MEBIBYTE;

    /// Constructs an explicit bounded transport policy.
    ///
    /// # Errors
    ///
    /// Rejects zero durations/byte ceilings, implementation ceiling breaches,
    /// or an event ceiling smaller than its line ceiling.
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        connect_timeout: Duration,
        pool_idle_timeout: Duration,
        maximum_request_bytes: usize,
        maximum_response_bytes: usize,
        maximum_sse_line_bytes: usize,
        maximum_sse_event_bytes: usize,
        maximum_sse_total_bytes: usize,
    ) -> Result<Self, ProviderHttpOptionsError> {
        if connect_timeout.is_zero() || pool_idle_timeout.is_zero() {
            return Err(ProviderHttpOptionsError::ZeroDuration);
        }
        if maximum_request_bytes == 0
            || maximum_response_bytes == 0
            || maximum_sse_line_bytes == 0
            || maximum_sse_event_bytes == 0
            || maximum_sse_total_bytes == 0
        {
            return Err(ProviderHttpOptionsError::ZeroBytes);
        }
        if maximum_request_bytes > Self::HARD_MAXIMUM_REQUEST_BYTES
            || maximum_response_bytes > Self::HARD_MAXIMUM_RESPONSE_BYTES
            || maximum_sse_line_bytes > Self::HARD_MAXIMUM_SSE_LINE_BYTES
            || maximum_sse_event_bytes > Self::HARD_MAXIMUM_SSE_EVENT_BYTES
            || maximum_sse_total_bytes > Self::HARD_MAXIMUM_SSE_TOTAL_BYTES
        {
            return Err(ProviderHttpOptionsError::AboveHardMaximum);
        }
        if maximum_sse_event_bytes < maximum_sse_line_bytes {
            return Err(ProviderHttpOptionsError::EventBelowLine);
        }
        if maximum_sse_total_bytes < maximum_sse_event_bytes {
            return Err(ProviderHttpOptionsError::TotalBelowEvent);
        }
        Ok(Self {
            connect_timeout,
            pool_idle_timeout,
            maximum_request_bytes,
            maximum_response_bytes,
            maximum_sse_line_bytes,
            maximum_sse_event_bytes,
            maximum_sse_total_bytes,
        })
    }

    /// Returns the TCP/TLS connection timeout.
    #[must_use]
    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    /// Returns the idle connection retention bound.
    #[must_use]
    pub const fn pool_idle_timeout(self) -> Duration {
        self.pool_idle_timeout
    }

    /// Returns the serialized request ceiling.
    #[must_use]
    pub const fn maximum_request_bytes(self) -> usize {
        self.maximum_request_bytes
    }

    /// Returns the complete successful response ceiling.
    #[must_use]
    pub const fn maximum_response_bytes(self) -> usize {
        self.maximum_response_bytes
    }

    /// Returns the SSE line ceiling.
    #[must_use]
    pub const fn maximum_sse_line_bytes(self) -> usize {
        self.maximum_sse_line_bytes
    }

    /// Returns the assembled SSE event ceiling.
    #[must_use]
    pub const fn maximum_sse_event_bytes(self) -> usize {
        self.maximum_sse_event_bytes
    }

    /// Returns the total stream transport-byte ceiling.
    #[must_use]
    pub const fn maximum_sse_total_bytes(self) -> usize {
        self.maximum_sse_total_bytes
    }
}

impl Default for ProviderHttpOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            pool_idle_timeout: Duration::from_secs(90),
            maximum_request_bytes: 16 * MEBIBYTE,
            maximum_response_bytes: 2 * MEBIBYTE,
            maximum_sse_line_bytes: 512 * 1024,
            maximum_sse_event_bytes: 2 * MEBIBYTE,
            maximum_sse_total_bytes: 64 * MEBIBYTE,
        }
    }
}

/// Invalid provider transport resource policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ProviderHttpOptionsError {
    /// A timeout was zero.
    #[error("provider HTTP timeouts must be positive")]
    ZeroDuration,
    /// A byte ceiling was zero.
    #[error("provider HTTP byte ceilings must be positive")]
    ZeroBytes,
    /// A configured ceiling exceeded the implementation maximum.
    #[error("provider HTTP byte ceiling exceeds the implementation maximum")]
    AboveHardMaximum,
    /// One event could not contain one maximum-size line.
    #[error("provider SSE event ceiling must be at least its line ceiling")]
    EventBelowLine,
    /// The stream total could not contain one maximum-size event.
    #[error("provider SSE total ceiling must be at least its event ceiling")]
    TotalBelowEvent,
}

pub(crate) fn build_client(
    options: ProviderHttpOptions,
) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(options.connect_timeout())
        .pool_idle_timeout(options.pool_idle_timeout())
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .user_agent(concat!(
            "stateknot-integrations/",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_fail_closed_on_transport_and_authority_ambiguity() {
        assert_eq!(
            ProviderEndpoint::https("http://api.example.com/v1/"),
            Err(ProviderEndpointError::HttpsRequired)
        );
        assert_eq!(
            ProviderEndpoint::https("https://user:secret@api.example.com/v1/"),
            Err(ProviderEndpointError::EmbeddedCredentials)
        );
        assert_eq!(
            ProviderEndpoint::https("https://api.example.com/v1/?tenant=secret"),
            Err(ProviderEndpointError::QueryOrFragment)
        );
        assert_eq!(
            ProviderEndpoint::loopback_http("http://localhost:8080/v1/"),
            Err(ProviderEndpointError::LiteralLoopbackRequired)
        );
        assert_eq!(
            ProviderEndpoint::loopback_http("http://10.0.0.1:8080/v1/"),
            Err(ProviderEndpointError::LiteralLoopbackRequired)
        );

        let endpoint = ProviderEndpoint::https("https://private.example.com/tenant-a").unwrap();
        let debug = format!("{endpoint:?}");
        assert!(!debug.contains("private.example.com"));
        assert!(!debug.contains("tenant-a"));
    }

    #[test]
    fn transport_limits_are_positive_nested_and_hard_bounded() {
        assert_eq!(
            ProviderHttpOptions::new(Duration::ZERO, Duration::from_secs(1), 1, 1, 1, 1, 1,),
            Err(ProviderHttpOptionsError::ZeroDuration)
        );
        assert_eq!(
            ProviderHttpOptions::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
                1,
                1,
                2,
                1,
                2,
            ),
            Err(ProviderHttpOptionsError::EventBelowLine)
        );
    }
}
