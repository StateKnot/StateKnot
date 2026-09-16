// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::AgentMaintenanceError;
use std::time::Duration;

/// Validated finite local pacing, dependency and shutdown bounds.
#[derive(Clone, Debug)]
pub struct AgentMaintenanceOptions {
    pub(super) delay: Duration,
    pub(super) failure_delay: Duration,
    pub(super) tick_timeout: Duration,
    pub(super) drain_timeout: Duration,
    pub(super) probe_interval: Duration,
    pub(super) probe_timeout: Duration,
    pub(super) freshness: Duration,
}
impl Default for AgentMaintenanceOptions {
    fn default() -> Self {
        Self {
            delay: Duration::from_millis(250),
            failure_delay: Duration::from_secs(1),
            tick_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(10),
            probe_interval: Duration::from_secs(10),
            probe_timeout: Duration::from_secs(5),
            freshness: Duration::from_secs(20),
        }
    }
}
impl AgentMaintenanceOptions {
    /// Positive 10 ms–60 s pacing after every ordinary or item-failing tick.
    pub fn with_pacing(
        mut self,
        delay: Duration,
        failure: Duration,
    ) -> Result<Self, AgentMaintenanceError> {
        if [delay, failure]
            .iter()
            .any(|value| !(Duration::from_millis(10)..=Duration::from_secs(60)).contains(value))
        {
            return Err(AgentMaintenanceError::InvalidOptions);
        }
        self.delay = delay;
        self.failure_delay = failure;
        Ok(self)
    }
    /// Absolute tick deadline 100 ms–1 h; graceful drain deadline 10 ms–5 min.
    pub fn with_deadlines(
        mut self,
        tick: Duration,
        drain: Duration,
    ) -> Result<Self, AgentMaintenanceError> {
        if !(Duration::from_millis(100)..=Duration::from_secs(3600)).contains(&tick)
            || !(Duration::from_millis(10)..=Duration::from_secs(300)).contains(&drain)
        {
            return Err(AgentMaintenanceError::InvalidOptions);
        }
        self.tick_timeout = tick;
        self.drain_timeout = drain;
        Ok(self)
    }
    /// Single-flight probes: interval 50 ms–60 s, whole-check timeout 10 ms–30 s;
    /// freshness at least their sum and at most 120 s.
    pub fn with_readiness_limits(
        mut self,
        interval: Duration,
        timeout: Duration,
        freshness: Duration,
    ) -> Result<Self, AgentMaintenanceError> {
        if !(Duration::from_millis(50)..=Duration::from_secs(60)).contains(&interval)
            || !(Duration::from_millis(10)..=Duration::from_secs(30)).contains(&timeout)
            || freshness > Duration::from_secs(120)
            || freshness < interval + timeout
        {
            return Err(AgentMaintenanceError::InvalidOptions);
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
    fn bounds_reject_zero_overflow_and_inconsistent_freshness() {
        let ms = Duration::from_millis;
        for (a, b) in [(0, 10), (10, 0), (60001, 10), (10, 60001)] {
            assert!(
                AgentMaintenanceOptions::default()
                    .with_pacing(ms(a), ms(b))
                    .is_err()
            );
        }
        for (a, b) in [(99, 10), (100, 9), (3_600_001, 10), (100, 300_001)] {
            assert!(
                AgentMaintenanceOptions::default()
                    .with_deadlines(ms(a), ms(b))
                    .is_err()
            );
        }
        assert!(
            AgentMaintenanceOptions::default()
                .with_deadlines(ms(100), ms(10))
                .is_ok()
        );
        assert!(
            AgentMaintenanceOptions::default()
                .with_readiness_limits(ms(50), ms(10), ms(59))
                .is_err()
        );
        assert!(
            AgentMaintenanceOptions::default()
                .with_readiness_limits(ms(50), ms(10), ms(60))
                .is_ok()
        );
        assert!(
            AgentMaintenanceOptions::default()
                .with_readiness_limits(Duration::MAX, ms(10), Duration::MAX)
                .is_err()
        );
    }
}
