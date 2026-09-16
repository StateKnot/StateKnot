// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::AgentWorkerError;
use std::time::Duration;

/// Finite process-local scheduling and shutdown bounds; no durable policy changes.
#[derive(Clone, Debug)]
pub struct AgentWorkerOptions {
    pub(super) slots: usize,
    pub(super) busy_delay: Duration,
    pub(super) idle_delay: Duration,
    pub(super) failure_delay: Duration,
    pub(super) tick_timeout: Duration,
    pub(super) drain_timeout: Duration,
    pub(super) probe_interval: Duration,
    pub(super) probe_timeout: Duration,
    pub(super) freshness: Duration,
}

impl Default for AgentWorkerOptions {
    fn default() -> Self {
        Self {
            slots: 4,
            busy_delay: Duration::from_millis(10),
            idle_delay: Duration::from_millis(250),
            failure_delay: Duration::from_secs(1),
            tick_timeout: Duration::from_secs(300),
            drain_timeout: Duration::from_secs(30),
            probe_interval: Duration::from_secs(10),
            probe_timeout: Duration::from_secs(5),
            freshness: Duration::from_secs(20),
        }
    }
}

impl AgentWorkerOptions {
    /// Sets 1..=64 slots, 100 ms..=1 h per tick and 10 ms..=5 min drain.
    pub fn with_execution_limits(
        mut self,
        slots: usize,
        tick: Duration,
        drain: Duration,
    ) -> Result<Self, AgentWorkerError> {
        if !(1..=64).contains(&slots)
            || !(Duration::from_millis(100)..=Duration::from_secs(3600)).contains(&tick)
            || !(Duration::from_millis(10)..=Duration::from_secs(300)).contains(&drain)
        {
            return Err(AgentWorkerError::InvalidOptions);
        }
        self.slots = slots;
        self.tick_timeout = tick;
        self.drain_timeout = drain;
        Ok(self)
    }

    /// Sets positive 10 ms..=60 s delays after busy, idle/scan-limit, failed ticks.
    pub fn with_pacing(
        mut self,
        busy: Duration,
        idle: Duration,
        failure: Duration,
    ) -> Result<Self, AgentWorkerError> {
        if [busy, idle, failure]
            .iter()
            .any(|delay| !(Duration::from_millis(10)..=Duration::from_secs(60)).contains(delay))
        {
            return Err(AgentWorkerError::InvalidOptions);
        }
        self.busy_delay = busy;
        self.idle_delay = idle;
        self.failure_delay = failure;
        Ok(self)
    }

    /// Single-flight probes: 50 ms..=60 s between completions, 10 ms..=30 s
    /// whole-check timeout, freshness at least their sum and at most 120 s.
    pub fn with_readiness_limits(
        mut self,
        interval: Duration,
        timeout: Duration,
        freshness: Duration,
    ) -> Result<Self, AgentWorkerError> {
        if !(Duration::from_millis(50)..=Duration::from_secs(60)).contains(&interval)
            || !(Duration::from_millis(10)..=Duration::from_secs(30)).contains(&timeout)
            || freshness > Duration::from_secs(120)
            || freshness < interval + timeout
        {
            return Err(AgentWorkerError::InvalidOptions);
        }
        self.probe_interval = interval;
        self.probe_timeout = timeout;
        self.freshness = freshness;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_bounds_are_finite_and_nonzero() {
        let ms = Duration::from_millis;
        for slots in [0, 65, usize::MAX] {
            assert!(
                AgentWorkerOptions::default()
                    .with_execution_limits(slots, ms(100), ms(10))
                    .is_err()
            );
        }
        assert!(
            AgentWorkerOptions::default()
                .with_execution_limits(64, ms(100), ms(10))
                .is_ok()
        );
        assert!(
            AgentWorkerOptions::default()
                .with_execution_limits(1, ms(99), ms(10))
                .is_err()
        );
        assert!(
            AgentWorkerOptions::default()
                .with_execution_limits(1, Duration::MAX, ms(10))
                .is_err()
        );
        assert!(
            AgentWorkerOptions::default()
                .with_execution_limits(1, ms(100), ms(0))
                .is_err()
        );
        for delays in [(0, 10, 10), (10, 0, 10), (10, 10, 0), (60001, 10, 10)] {
            assert!(
                AgentWorkerOptions::default()
                    .with_pacing(ms(delays.0), ms(delays.1), ms(delays.2))
                    .is_err()
            );
        }
        assert!(
            AgentWorkerOptions::default()
                .with_readiness_limits(ms(50), ms(10), ms(59))
                .is_err()
        );
        assert!(
            AgentWorkerOptions::default()
                .with_readiness_limits(ms(50), ms(10), ms(60))
                .is_ok()
        );
        assert!(
            AgentWorkerOptions::default()
                .with_readiness_limits(Duration::MAX, ms(10), Duration::MAX)
                .is_err()
        );
    }
}
