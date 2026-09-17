// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! # `StateKnot`
//!
//! Durable agent orchestration for Rust.
//!
//! The facade exposes the implementation-backed core contracts, executable
//! graph runtime, and `PostgreSQL` 16/17 durability provider under explicit
//! modules. The project remains pre-alpha: these modules are usable for the
//! current vertical validation slice, but they are not a stable compatibility
//! promise. See the
//! [project repository](https://github.com/StateKnot/StateKnot) for the current
//! status, architecture plan, and roadmap.

#![forbid(unsafe_code)]

/// Concrete ownership of co-located ingress, execution and maintenance roles.
pub mod agent_host;
/// Authenticated, bounded HTTP v1 ingress for durable Agent operations.
pub mod agent_http;
/// Independently owned, bounded durable scheduling Worker lifecycle.
pub mod agent_worker;
mod http_transport;
/// Independently owned deadline, child, Join and failure-close maintenance.
pub use stateknot_runtime::agent_maintenance;
mod mcp_compute;
mod mcp_reconciliation;
pub use mcp_compute::{
    McpComputeNode, McpComputeNodeBinding, McpComputeNodeBuildError, McpComputeOutput,
    WorkerInputProjection, WorkerInputProjectionError, mcp_compute_tool_digest,
};
pub use mcp_reconciliation::{
    McpErrorReconciliationAuthorizer, McpErrorReconciliationRequest, McpKnownToolEffect,
    McpReconciliationAuthorizer, McpReconciliationError, McpReconciliationGrant,
    McpReconciliationRequest, McpToolErrorReconciler, McpToolReconciler,
};

/// Integrity-checked S3-compatible artifact persistence and resolution.
pub use stateknot_artifact_store as artifacts;
/// Protocol-neutral domain, graph, journal, invocation, and lifecycle contracts.
pub use stateknot_core as core;
/// First-party model-provider and protocol adapters.
pub use stateknot_integrations as integrations;
/// Executable schema/reducer/node registries and the durable graph driver.
pub use stateknot_runtime as runtime;
/// `PostgreSQL` 16/17 durable storage and recovery provider.
pub use stateknot_store_postgres as postgres;
