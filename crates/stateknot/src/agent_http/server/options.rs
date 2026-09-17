// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::super::AgentHttpOptionsError;
use std::time::Duration;

/// Validated finite bounds for the loopback HTTP/1 ingress runtime.
#[derive(Clone, Debug)]
pub struct AgentHttpServerOptions {
    pub(crate) max_connections: usize,
    pub(crate) header_timeout: Duration,
    pub(crate) connection_lifetime: Duration,
    pub(crate) drain_timeout: Duration,
    pub(super) probe_interval: Duration,
    pub(super) probe_timeout: Duration,
    pub(super) freshness: Duration,
}

impl Default for AgentHttpServerOptions {
    fn default() -> Self {
        Self {
            max_connections: 256,
            header_timeout: Duration::from_secs(5),
            connection_lifetime: Duration::from_secs(300),
            drain_timeout: Duration::from_secs(30),
            probe_interval: Duration::from_secs(10),
            probe_timeout: Duration::from_secs(5),
            freshness: Duration::from_secs(20),
        }
    }
}

impl AgentHttpServerOptions {
    /// Configures 1..=4096 connections, 10 ms..=30 s headers, 50 ms..=1 h
    /// absolute connection lifetime, and 10 ms..=5 min drain. Header timeout
    /// cannot exceed lifetime. Header count (64) and buffer (32 KiB) are fixed.
    pub fn with_transport_limits(
        mut self,
        connections: usize,
        header_timeout: Duration,
        connection_lifetime: Duration,
        drain_timeout: Duration,
    ) -> Result<Self, AgentHttpOptionsError> {
        if !(1..=4096).contains(&connections)
            || !(Duration::from_millis(10)..=Duration::from_secs(30)).contains(&header_timeout)
            || !(Duration::from_millis(50)..=Duration::from_secs(3600))
                .contains(&connection_lifetime)
            || !(Duration::from_millis(10)..=Duration::from_secs(300)).contains(&drain_timeout)
            || header_timeout > connection_lifetime
        {
            return Err(AgentHttpOptionsError);
        }
        self.max_connections = connections;
        self.header_timeout = header_timeout;
        self.connection_lifetime = connection_lifetime;
        self.drain_timeout = drain_timeout;
        Ok(self)
    }

    /// Configures single-flight checks: 50 ms..=60 s between completed checks,
    /// 10 ms..=30 s per whole check, and freshness up to 120 s, at least the
    /// interval plus timeout. Reads of health never run a probe.
    pub fn with_readiness_limits(
        mut self,
        interval: Duration,
        timeout: Duration,
        freshness: Duration,
    ) -> Result<Self, AgentHttpOptionsError> {
        if !(Duration::from_millis(50)..=Duration::from_secs(60)).contains(&interval)
            || !(Duration::from_millis(10)..=Duration::from_secs(30)).contains(&timeout)
            || freshness > Duration::from_secs(120)
            || freshness < interval + timeout
        {
            return Err(AgentHttpOptionsError);
        }
        self.probe_interval = interval;
        self.probe_timeout = timeout;
        self.freshness = freshness;
        Ok(self)
    }
}
