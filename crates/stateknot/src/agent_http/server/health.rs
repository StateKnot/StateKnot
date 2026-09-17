// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use crate::http_transport::ConnectionGuard;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::task::TaskTracker;

/// Local, secret-free ingress lifecycle observation; never an authorization grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AgentHttpServerStatus {
    /// Last dependency check succeeded and is still fresh.
    Ready,
    /// Dependencies failed, timed out, or have no fresh success evidence.
    Unavailable,
    /// No new work is admitted; existing connections are draining.
    Draining,
    /// Owned runtime has exited or its handle was dropped.
    Stopped,
}

struct Record {
    status: AgentHttpServerStatus,
    success: Option<Instant>,
}

/// Cheap local status for a host's protected administrative/telemetry surface.
/// No endpoint is installed; cloning this view does not keep the server alive.
#[derive(Clone)]
pub struct AgentHttpServerHealth {
    record: Arc<Mutex<Record>>,
    active: Arc<AtomicUsize>,
    freshness: Duration,
    streams: TaskTracker,
}

impl AgentHttpServerHealth {
    pub(super) fn new(freshness: Duration, streams: TaskTracker) -> Self {
        Self {
            record: Arc::new(Mutex::new(Record {
                status: AgentHttpServerStatus::Unavailable,
                success: None,
            })),
            active: Arc::new(AtomicUsize::new(0)),
            freshness,
            streams,
        }
    }

    /// Computes current status, including freshness, without performing I/O.
    #[must_use]
    pub fn status(&self) -> AgentHttpServerStatus {
        let record = self
            .record
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if record.status == AgentHttpServerStatus::Ready
            && record
                .success
                .is_none_or(|time| time.elapsed() >= self.freshness)
        {
            AgentHttpServerStatus::Unavailable
        } else {
            record.status
        }
    }

    /// Returns true only for fresh success while not draining/stopped.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.status() == AgentHttpServerStatus::Ready
    }

    /// Returns true while the runtime exists, including unavailable and draining.
    /// This is local liveness, not a guarantee of executor responsiveness.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.status() != AgentHttpServerStatus::Stopped
    }

    /// Returns owned connection futures not yet dropped, including aborts pending cleanup.
    #[must_use]
    pub fn active_connections(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    /// Returns SSE producer futures whose cleanup has not yet completed.
    #[must_use]
    pub fn active_streams(&self) -> usize {
        self.streams.len()
    }

    pub(super) fn connection(&self) -> ConnectionGuard {
        ConnectionGuard::new(self.active.clone())
    }

    pub(super) fn update(&self, status: AgentHttpServerStatus) {
        let mut record = self
            .record
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A racing successful probe must never revive a draining/stopped role.
        if record.status == AgentHttpServerStatus::Stopped
            || (record.status == AgentHttpServerStatus::Draining
                && status != AgentHttpServerStatus::Stopped)
        {
            return;
        }
        record.status = status;
        record.success = (status == AgentHttpServerStatus::Ready).then(Instant::now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_evidence_and_shutdown_cannot_report_ready() {
        let health = AgentHttpServerHealth::new(Duration::from_secs(1), TaskTracker::new());
        health.update(AgentHttpServerStatus::Ready);
        assert!(health.is_ready());
        health.record.lock().unwrap().success = Some(Instant::now() - Duration::from_secs(2));
        assert!(!health.is_ready());
        assert!(health.is_live());
        health.update(AgentHttpServerStatus::Draining);
        health.update(AgentHttpServerStatus::Ready);
        assert_eq!(health.status(), AgentHttpServerStatus::Draining);
        health.update(AgentHttpServerStatus::Stopped);
        health.update(AgentHttpServerStatus::Ready);
        assert!(!health.is_live());
    }
}
