// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{AgentWorkerFailure, AgentWorkerReport};
use stateknot_runtime::{GraphExecutionActivity, TenantSchedulerOutcome};
use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::time::Instant;

/// Local lifecycle state, not a durable Run status or public health endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentWorkerStatus {
    /// Fresh successful dependency evidence admits new ticks.
    Ready,
    /// Failed or stale readiness pauses dispatch; existing ticks may finish.
    Unavailable,
    /// Dispatch is closed and owned execution is being reclaimed.
    Draining,
    /// Coordinator exited; after Drop, consult active counts until cleanup ends.
    Stopped,
}

struct State {
    status: AgentWorkerStatus,
    checked: Option<Instant>,
    active: usize,
    report: AgentWorkerReport,
}

/// Payload-free cloneable health view. Never retains a store or executable registry.
#[derive(Clone)]
pub struct AgentWorkerHealth {
    state: Arc<Mutex<State>>,
    freshness: Duration,
    pub(super) activity: GraphExecutionActivity,
}

impl AgentWorkerHealth {
    pub(super) fn new(freshness: Duration, activity: GraphExecutionActivity) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                status: AgentWorkerStatus::Unavailable,
                checked: None,
                active: 0,
                report: AgentWorkerReport::default(),
            })),
            freshness,
            activity,
        }
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn effective_status(&self, state: &State) -> AgentWorkerStatus {
        if state.status == AgentWorkerStatus::Ready
            && state
                .checked
                .is_none_or(|checked| checked.elapsed() >= self.freshness)
        {
            AgentWorkerStatus::Unavailable
        } else {
            state.status
        }
    }
    /// Checks cached monotonic freshness; does not perform I/O.
    pub fn status(&self) -> AgentWorkerStatus {
        self.effective_status(&self.lock())
    }
    /// Currently admitted tick futures (including work being cancelled).
    pub fn active_ticks(&self) -> usize {
        self.lock().active
    }
    /// Nested node futures still alive, including those aborted by parent Drop.
    pub fn active_nodes(&self) -> usize {
        self.activity.active_nodes()
    }
    /// Saturating counters and first closed failure category; no Run payloads.
    pub fn report(&self) -> AgentWorkerReport {
        self.lock().report
    }
    pub(super) fn probe(&self, ready: bool) {
        let mut state = self.lock();
        if matches!(
            state.status,
            AgentWorkerStatus::Ready | AgentWorkerStatus::Unavailable
        ) {
            state.checked = ready.then(Instant::now);
            state.status = if ready {
                AgentWorkerStatus::Ready
            } else {
                AgentWorkerStatus::Unavailable
            };
            if !ready {
                state.report.readiness_failures = state.report.readiness_failures.saturating_add(1);
            }
        }
    }
    pub(super) fn draining(&self) {
        let mut state = self.lock();
        if state.status != AgentWorkerStatus::Stopped {
            state.status = AgentWorkerStatus::Draining;
        }
    }
    pub(super) fn stopped(&self) {
        self.lock().status = AgentWorkerStatus::Stopped;
    }
    pub(super) fn fail(&self, failure: AgentWorkerFailure) {
        let mut state = self.lock();
        state.report.failure.get_or_insert(failure);
        state.status = AgentWorkerStatus::Draining;
    }
    pub(super) fn forced(&self) {
        let mut state = self.lock();
        state.report.forced_ticks = state.active;
    }
    pub(super) fn admit(&self) -> Option<TickGuard> {
        let mut state = self.lock();
        if self.effective_status(&state) != AgentWorkerStatus::Ready {
            return None;
        }
        state.active += 1;
        state.report.started_ticks = state.report.started_ticks.saturating_add(1);
        Some(TickGuard(self.clone()))
    }
    pub(super) fn complete(&self, outcome: &TenantSchedulerOutcome) {
        let mut state = self.lock();
        state.report.completed_ticks = state.report.completed_ticks.saturating_add(1);
        match outcome {
            TenantSchedulerOutcome::Executed { .. } => {
                state.report.executed_quanta = state.report.executed_quanta.saturating_add(1);
            }
            TenantSchedulerOutcome::ExecutionFailed { .. } => {
                state.report.run_failures = state.report.run_failures.saturating_add(1);
            }
            _ => {}
        }
    }
}

pub(super) struct TickGuard(AgentWorkerHealth);
impl Drop for TickGuard {
    fn drop(&mut self) {
        self.0.lock().active -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn stale_probe_and_late_success_cannot_reopen_shutdown() {
        let health =
            AgentWorkerHealth::new(Duration::from_millis(10), GraphExecutionActivity::default());
        assert!(health.admit().is_none());
        health.probe(true);
        let tick = health.admit().unwrap();
        assert_eq!(health.active_ticks(), 1);
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert_eq!(health.status(), AgentWorkerStatus::Unavailable);
        assert!(health.admit().is_none());
        health.draining();
        health.probe(true);
        assert!(health.admit().is_none());
        drop(tick);
        assert_eq!(health.active_ticks(), 0);
        health.stopped();
        health.probe(true);
        assert_eq!(health.status(), AgentWorkerStatus::Stopped);
    }
}
