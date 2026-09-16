// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{AgentHostFailure, AgentHttpServerHealth, AgentMaintenanceHealth, AgentWorkerHealth};
use std::sync::{Arc, Mutex};

/// Cached local lifecycle and effective dependency readiness, not a remote probe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentHostStatus {
    /// Ordered startup is incomplete; ingress is closed.
    Starting,
    /// All three actual role views are currently ready and fresh.
    Ready,
    /// At least one running role is unavailable or stale; ingress is closed.
    Unavailable,
    /// Joined shutdown is in progress; ingress cannot reopen.
    Draining,
    /// Coordinator finished or its owner was dropped.
    Stopped,
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Phase {
    Starting,
    Running,
    Draining,
    Stopped,
}

pub(super) struct State {
    pub phase: Phase,
    pub failure: Option<AgentHostFailure>,
    pub http: Option<AgentHttpServerHealth>,
    pub worker: Option<AgentWorkerHealth>,
    pub maintenance: Option<AgentMaintenanceHealth>,
}

/// Payload-free local view. Does not retain database pools, bindings or executors.
/// Role reads are individually synchronized, not a distributed atomic snapshot.
#[derive(Clone)]
pub struct AgentHostHealth(pub(super) Arc<Mutex<State>>);

impl AgentHostHealth {
    pub(super) fn new() -> Self {
        Self(Arc::new(Mutex::new(State {
            phase: Phase::Starting,
            failure: None,
            http: None,
            worker: None,
            maintenance: None,
        })))
    }

    pub(super) fn update(&self, f: impl FnOnce(&mut State)) {
        f(&mut self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner));
    }

    /// Effective readiness includes each role's bounded freshness checks.
    #[must_use]
    pub fn status(&self) -> AgentHostStatus {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.phase {
            Phase::Starting => AgentHostStatus::Starting,
            Phase::Draining => AgentHostStatus::Draining,
            Phase::Stopped => AgentHostStatus::Stopped,
            Phase::Running => {
                if state
                    .http
                    .as_ref()
                    .is_some_and(AgentHttpServerHealth::is_ready)
                    && state.worker.as_ref().is_some_and(|health| {
                        health.status() == crate::agent_worker::AgentWorkerStatus::Ready
                    })
                    && state.maintenance.as_ref().is_some_and(|health| {
                        health.status() == crate::agent_maintenance::AgentMaintenanceStatus::Ready
                    })
                {
                    AgentHostStatus::Ready
                } else {
                    AgentHostStatus::Unavailable
                }
            }
        }
    }

    pub(crate) fn allows_ingress(&self) -> bool {
        self.status() == AgentHostStatus::Ready
    }

    /// First sanitized startup or unexpected-exit failure, if any.
    #[must_use]
    pub fn failure(&self) -> Option<AgentHostFailure> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .failure
    }

    /// Actual ingress view, absent until this role starts successfully.
    #[must_use]
    pub fn http(&self) -> Option<AgentHttpServerHealth> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .http
            .clone()
    }

    /// Actual Worker view, absent until this role starts successfully.
    #[must_use]
    pub fn worker(&self) -> Option<AgentWorkerHealth> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .worker
            .clone()
    }

    /// Actual maintenance view, absent until this role starts successfully.
    #[must_use]
    pub fn maintenance(&self) -> Option<AgentMaintenanceHealth> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .maintenance
            .clone()
    }

    pub(super) fn draining(&self) {
        self.update(|state| {
            if state.phase != Phase::Stopped {
                state.phase = Phase::Draining;
            }
        });
    }

    pub(super) fn fail(&self, failure: AgentHostFailure) {
        self.update(|state| {
            state.failure.get_or_insert(failure);
        });
        self.draining();
    }
}
