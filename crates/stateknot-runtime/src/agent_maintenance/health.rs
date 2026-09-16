// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{
    AgentMaintenanceFailure, AgentMaintenanceJob, AgentMaintenanceJobReport, AgentMaintenanceReport,
};
use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::time::Instant;

/// Cached dependency/lifecycle status, not the success of every maintenance item.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentMaintenanceStatus {
    /// Fresh dependency evidence admits ticks; item errors may still be present.
    Ready,
    /// Failed or stale readiness prevents new ticks.
    Unavailable,
    /// Admission is closed and tasks are being reclaimed.
    Draining,
    /// Coordinator exited; after Drop also check active ticks for task destruction.
    Stopped,
}
struct State {
    status: AgentMaintenanceStatus,
    checked: Option<Instant>,
    active: usize,
    report: AgentMaintenanceReport,
}
/// Payload-free cloneable view that does not retain the database or schema registry.
#[derive(Clone)]
pub struct AgentMaintenanceHealth {
    state: Arc<Mutex<State>>,
    freshness: Duration,
}
impl AgentMaintenanceHealth {
    pub(super) fn new(freshness: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                status: AgentMaintenanceStatus::Unavailable,
                checked: None,
                active: 0,
                report: AgentMaintenanceReport::default(),
            })),
            freshness,
        }
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn effective(&self, state: &State) -> AgentMaintenanceStatus {
        if state.status == AgentMaintenanceStatus::Ready
            && state
                .checked
                .is_none_or(|checked| checked.elapsed() >= self.freshness)
        {
            AgentMaintenanceStatus::Unavailable
        } else {
            state.status
        }
    }
    /// Pure cached check with monotonic freshness; performs no I/O.
    pub fn status(&self) -> AgentMaintenanceStatus {
        self.effective(&self.lock())
    }
    /// Admitted tick futures still alive (at most one).
    pub fn active_ticks(&self) -> usize {
        self.lock().active
    }
    /// Saturating local observations and first sanitized fail-stop category.
    pub fn report(&self) -> AgentMaintenanceReport {
        self.lock().report
    }
    pub(super) fn probe(&self, ready: bool) {
        let mut state = self.lock();
        if matches!(
            state.status,
            AgentMaintenanceStatus::Ready | AgentMaintenanceStatus::Unavailable
        ) {
            state.checked = ready.then(Instant::now);
            state.status = if ready {
                AgentMaintenanceStatus::Ready
            } else {
                AgentMaintenanceStatus::Unavailable
            };
            if !ready {
                state.report.readiness_failures = state.report.readiness_failures.saturating_add(1);
            }
        }
    }
    pub(super) fn draining(&self) {
        let mut state = self.lock();
        if state.status != AgentMaintenanceStatus::Stopped {
            state.status = AgentMaintenanceStatus::Draining;
        }
    }
    pub(super) fn stopped(&self) {
        self.lock().status = AgentMaintenanceStatus::Stopped;
    }
    pub(super) fn fail(&self, failure: AgentMaintenanceFailure) {
        let mut state = self.lock();
        state.report.failure.get_or_insert(failure);
        state.status = AgentMaintenanceStatus::Draining;
    }
    pub(super) fn forced(&self) {
        let mut state = self.lock();
        state.report.forced_ticks = state.active;
    }
    pub(super) fn admit(&self) -> Option<TickGuard> {
        let mut state = self.lock();
        if self.effective(&state) != AgentMaintenanceStatus::Ready {
            return None;
        }
        state.active += 1;
        state.report.started_ticks = state.report.started_ticks.saturating_add(1);
        Some(TickGuard(self.clone()))
    }
    pub(super) fn complete(&self, job: AgentMaintenanceJob, counts: AgentMaintenanceJobReport) {
        let mut state = self.lock();
        let report = &mut state.report.jobs[job.index()];
        report.completed_ticks = report
            .completed_ticks
            .saturating_add(counts.completed_ticks);
        report.items = report.items.saturating_add(counts.items);
        report.item_failures = report.item_failures.saturating_add(counts.item_failures);
    }
}
pub(super) struct TickGuard(AgentMaintenanceHealth);
impl Drop for TickGuard {
    fn drop(&mut self) {
        self.0.lock().active -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn stale_health_and_late_probes_cannot_reopen_shutdown() {
        let health = AgentMaintenanceHealth::new(Duration::from_millis(10));
        assert!(health.admit().is_none());
        health.probe(true);
        let tick = health.admit().unwrap();
        assert_eq!(health.active_ticks(), 1);
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert_eq!(health.status(), AgentMaintenanceStatus::Unavailable);
        assert!(health.admit().is_none());
        health.draining();
        health.probe(true);
        assert!(health.admit().is_none());
        drop(tick);
        assert_eq!(health.active_ticks(), 0);
        health.stopped();
        health.probe(true);
        assert_eq!(health.status(), AgentMaintenanceStatus::Stopped);
    }
}
