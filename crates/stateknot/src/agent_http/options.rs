// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use axum::http::{Uri, uri::Authority};
use std::{collections::BTreeSet, time::Duration};
use thiserror::Error;

/// Validated, finite deployment policy for the HTTP v1 profile.
#[derive(Clone, Debug)]
pub struct AgentHttpOptions {
    pub(super) hosts: BTreeSet<String>,
    pub(super) origins: BTreeSet<String>,
    pub(super) max_request_bytes: usize,
    pub(super) max_response_bytes: usize,
    pub(super) max_in_flight: usize,
    pub(super) deadline: Duration,
}

impl AgentHttpOptions {
    /// Creates an exact Host allowlist and rejects every browser Origin by default.
    /// Defaults: 256 KiB input, 2 MiB output, 64 in-flight requests, 15 seconds.
    pub fn new(hosts: impl IntoIterator<Item = String>) -> Result<Self, AgentHttpOptionsError> {
        let mut allowed = BTreeSet::new();
        for host in hosts {
            if allowed.len() == 64
                || host.len() > 255
                || host.contains(['@', '*'])
                || host.trim() != host
            {
                return Err(AgentHttpOptionsError);
            }
            let authority = host
                .parse::<Authority>()
                .map_err(|_| AgentHttpOptionsError)?;
            if !valid_authority(&authority) || !allowed.insert(host.to_ascii_lowercase()) {
                return Err(AgentHttpOptionsError);
            }
        }
        if allowed.is_empty() {
            return Err(AgentHttpOptionsError);
        }
        Ok(Self {
            hosts: allowed,
            origins: BTreeSet::new(),
            max_request_bytes: 256 * 1024,
            max_response_bytes: 2 * 1024 * 1024,
            max_in_flight: 64,
            deadline: Duration::from_secs(15),
        })
    }

    /// Explicit loopback-only fixture configuration; this does not create a listener.
    pub fn loopback(port: u16) -> Result<Self, AgentHttpOptionsError> {
        if port == 0 {
            return Err(AgentHttpOptionsError);
        }
        Self::new([
            format!("127.0.0.1:{port}"),
            format!("localhost:{port}"),
            format!("[::1]:{port}"),
        ])
    }

    /// Accepts only these exact serialized HTTPS origins (no CORS response headers).
    /// Missing Origin remains allowed for non-browser clients. No wildcard/null.
    pub fn with_allowed_origins(
        mut self,
        origins: impl IntoIterator<Item = String>,
    ) -> Result<Self, AgentHttpOptionsError> {
        let mut allowed = BTreeSet::new();
        for origin in origins {
            let uri = origin.parse::<Uri>().map_err(|_| AgentHttpOptionsError)?;
            let authority = uri.authority().ok_or(AgentHttpOptionsError)?;
            if allowed.len() == 64
                || origin.len() > 512
                || uri.scheme_str() != Some("https")
                || origin != format!("https://{authority}")
                || authority.as_str().contains(['@', '*'])
                || !valid_authority(authority)
                || !allowed.insert(origin)
            {
                return Err(AgentHttpOptionsError);
            }
        }
        self.origins = allowed;
        Ok(self)
    }

    /// Narrows/sets bounded input, output and per-process concurrency limits.
    /// Hard maxima: 2 MiB input, 8 MiB output, 1,024 in-flight requests.
    pub fn with_limits(
        mut self,
        request_bytes: usize,
        response_bytes: usize,
        in_flight: usize,
    ) -> Result<Self, AgentHttpOptionsError> {
        if !(1..=2 * 1024 * 1024).contains(&request_bytes)
            || !(1024..=8 * 1024 * 1024).contains(&response_bytes)
            || !(1..=1024).contains(&in_flight)
        {
            return Err(AgentHttpOptionsError);
        }
        self.max_request_bytes = request_bytes;
        self.max_response_bytes = response_bytes;
        self.max_in_flight = in_flight;
        Ok(self)
    }

    /// Bounds authentication, body ingestion, policy, DB work and serialization together.
    /// Valid range: 10 milliseconds through 60 seconds; no unbounded timeout.
    pub fn with_deadline(mut self, deadline: Duration) -> Result<Self, AgentHttpOptionsError> {
        if !(Duration::from_millis(10)..=Duration::from_secs(60)).contains(&deadline) {
            return Err(AgentHttpOptionsError);
        }
        self.deadline = deadline;
        Ok(self)
    }
}

fn valid_authority(authority: &Authority) -> bool {
    let host = authority.host();
    if host.is_empty() {
        return false;
    }
    let suffix = &authority.as_str()[host.len()..];
    suffix.is_empty()
        || (suffix.starts_with(':') && authority.port_u16().is_some_and(|port| port != 0))
}

/// Invalid or unbounded ingress configuration; intentionally excludes supplied values.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invalid Agent HTTP deployment policy")]
pub struct AgentHttpOptionsError;
