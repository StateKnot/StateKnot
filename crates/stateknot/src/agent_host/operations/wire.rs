// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::super::{AgentHostFailure, AgentHostHealth, AgentHostRole, AgentHostStatus};
use crate::{
    agent_http::AgentHttpServerStatus,
    agent_maintenance::{AgentMaintenanceFailure, AgentMaintenanceJob, AgentMaintenanceStatus},
    agent_worker::{AgentWorkerFailure, AgentWorkerStatus},
};
use axum::http::StatusCode;
use serde_json::{Value, json};
use stateknot_core::EventId;

// Construct only fixed labels and numbers. Never serialize Debug or host errors.
pub(super) fn snapshot(health: &AgentHostHealth, path: &str, id: EventId) -> (StatusCode, Value) {
    let status = health.status();
    let live = status != AgentHostStatus::Stopped;
    let ready = status == AgentHostStatus::Ready;
    let code = if (path.ends_with("/ready") && !ready) || (path.ends_with("/live") && !live) {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    let status = match status {
        AgentHostStatus::Starting => "starting",
        AgentHostStatus::Ready => "ready",
        AgentHostStatus::Unavailable => "unavailable",
        AgentHostStatus::Draining => "draining",
        AgentHostStatus::Stopped => "stopped",
    };
    let failure = health.failure().map(|failure| {
        let (phase, role) = match failure {
            AgentHostFailure::Startup(role) => ("startup", role),
            AgentHostFailure::Role(role) => ("role", role),
        };
        let role = match role {
            AgentHostRole::Http => "http",
            AgentHostRole::Worker => "worker",
            AgentHostRole::Maintenance => "maintenance",
        };
        json!({"phase":phase,"role":role})
    });
    let http = health.http().map(|h| {
        let status = match h.status() {
            AgentHttpServerStatus::Ready => "ready", AgentHttpServerStatus::Unavailable => "unavailable",
            AgentHttpServerStatus::Draining => "draining", AgentHttpServerStatus::Stopped => "stopped",
        };
        json!({"status":status,"active_connections":h.active_connections(),"active_streams":h.active_streams()})
    });
    let worker = health.worker().map(|h| {
        let status = match h.status() {
            AgentWorkerStatus::Ready => "ready", AgentWorkerStatus::Unavailable => "unavailable",
            AgentWorkerStatus::Draining => "draining", AgentWorkerStatus::Stopped => "stopped",
        };
        let report = h.report();
        let failure = report.failure.map(|f| match f { AgentWorkerFailure::Scheduler => "scheduler", AgentWorkerFailure::TickDeadline => "tick_deadline", AgentWorkerFailure::Task => "task" });
        json!({"status":status,"active_ticks":h.active_ticks(),"active_nodes":h.active_nodes(),
            "started_ticks":report.started_ticks.to_string(),"completed_ticks":report.completed_ticks.to_string(),
            "executed_quanta":report.executed_quanta.to_string(),"run_failures":report.run_failures.to_string(),
            "readiness_failures":report.readiness_failures.to_string(),"forced_ticks":report.forced_ticks,"failure":failure})
    });
    let maintenance = health.maintenance().map(|h| {
        let status = match h.status() {
            AgentMaintenanceStatus::Ready => "ready", AgentMaintenanceStatus::Unavailable => "unavailable",
            AgentMaintenanceStatus::Draining => "draining", AgentMaintenanceStatus::Stopped => "stopped",
        };
        let report = h.report();
        let failure = report.failure.map(|f| match f { AgentMaintenanceFailure::Store => "store", AgentMaintenanceFailure::TickDeadline => "tick_deadline", AgentMaintenanceFailure::Task => "task" });
        let jobs: serde_json::Map<String, Value> = AgentMaintenanceJob::ALL.into_iter().map(|job| {
            let name = match job { AgentMaintenanceJob::Deadline => "deadline", AgentMaintenanceJob::Child => "child", AgentMaintenanceJob::Join => "join", AgentMaintenanceJob::FailureClose => "failure_close" };
            let counts = report.job(job);
            (name.into(), json!({"completed_ticks":counts.completed_ticks.to_string(),"items":counts.items.to_string(),"item_failures":counts.item_failures.to_string()}))
        }).collect();
        json!({"status":status,"active_ticks":h.active_ticks(),"started_ticks":report.started_ticks.to_string(),
            "readiness_failures":report.readiness_failures.to_string(),"forced_ticks":report.forced_ticks,"failure":failure,"jobs":jobs})
    });
    (
        code,
        json!({"schema_version":1,"request_id":id,"host":{"status":status,"live":live,"ready":ready,
        "failure":failure,"http":http,"worker":worker,"maintenance":maintenance}}),
    )
}
