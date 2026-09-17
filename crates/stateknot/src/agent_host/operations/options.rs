// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use crate::agent_http::{AgentHttpOptions, AgentHttpOptionsError, AgentHttpServerOptions};
use std::time::Duration;

/// Exact Host allowlist and finite operations-only limits. Browser Origins are
/// always denied; no router, TLS, default identity or signal handler is installed.
#[derive(Clone, Debug)]
pub struct AgentHostOperationsOptions {
    pub(super) http: AgentHttpOptions,
    pub(super) transport: AgentHttpServerOptions,
    pub(super) requests: usize,
    pub(super) deadline: Duration,
}
impl AgentHostOperationsOptions {
    /// Same strict Host syntax as business HTTP; defaults to 16 connections and
    /// requests, 5 s requests/drain, 3 s headers and 60 s connection lifetime.
    pub fn new(hosts: impl IntoIterator<Item = String>) -> Result<Self, AgentHttpOptionsError> {
        Ok(Self {
            http: AgentHttpOptions::new(hosts)?,
            transport: AgentHttpServerOptions::default().with_transport_limits(
                16,
                Duration::from_secs(3),
                Duration::from_secs(60),
                Duration::from_secs(5),
            )?,
            requests: 16,
            deadline: Duration::from_secs(5),
        })
    }
    /// At most 1..=256 whole requests, each 10 ms..=10 s including authentication.
    pub fn with_request_limits(
        mut self,
        requests: usize,
        deadline: Duration,
    ) -> Result<Self, AgentHttpOptionsError> {
        if !(1..=256).contains(&requests)
            || !(Duration::from_millis(10)..=Duration::from_secs(10)).contains(&deadline)
        {
            return Err(AgentHttpOptionsError);
        }
        self.requests = requests;
        self.deadline = deadline;
        Ok(self)
    }
    /// Uses the existing bounded HTTP/1 transport validation; no readiness probes.
    pub fn with_transport_limits(
        mut self,
        connections: usize,
        header_timeout: Duration,
        connection_lifetime: Duration,
        drain_timeout: Duration,
    ) -> Result<Self, AgentHttpOptionsError> {
        self.transport = self.transport.with_transport_limits(
            connections,
            header_timeout,
            connection_lifetime,
            drain_timeout,
        )?;
        Ok(self)
    }
}
