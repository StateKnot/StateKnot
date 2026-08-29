// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL` 16/17 durability provider for `StateKnot`.
//!
//! The provider persists canonical journal bytes, serializes each run under a
//! row lock, checks worker fencing with the database clock, and commits journal
//! facts with their lifecycle projection, checkpoints, invocation revisions, or
//! durable node-attempt starts/completions and immutable pending results in one
//! transaction. An indexed, tenant-scoped readiness projection supplies bounded
//! stable-snapshot scheduler candidates without reserving them. Transactional
//! outbox deliveries bind an immutable destination and payload to the exact
//! origin event; fixed fenced attempts commit before dispatch and recover with
//! explicit at-least-once semantics. The provider never holds a database
//! transaction across node, model, tool, remote-agent, or human work.
//!
//! This pre-alpha slice assumes a trusted server-side pool. Do not distribute
//! its database credentials to untrusted workers; role-separated procedures and
//! the final worker/control-plane service boundary remain release blockers.

#![forbid(unsafe_code)]

mod config;
mod error;
mod model;
mod store;

pub use config::{PostgresStoreOptions, PostgresTransportSecurity};
pub use error::{ConfigurationError, StoreError};
pub use model::{
    AdmissionOutcome, AppendOutcome, BarrierCommitOutcome, CheckpointCommitOutcome,
    CheckpointLineagePage, CheckpointLineagePageSize, CheckpointPointer, JournalPage,
    JournalPageSize, LeaseClaimOutcome, LeaseReleaseOutcome, LeaseRenewalOutcome,
    ModelInvocationCommitOutcome, ModelInvocationHistoryPage, ModelInvocationHistoryPageSize,
    NodeAttemptCommitOutcome, NodeAttemptHistoryPage, NodeAttemptHistoryPageSize,
    OutboxAttemptHistoryPage, OutboxAttemptHistoryPageSize, OutboxClaim, OutboxClaimOutcome,
    OutboxCompletionOutcome, OutboxDestinationRegistrationOutcome, OutboxEnqueueOutcome,
    PendingNodeResultCommitOutcome, PendingNodeResultPage, PendingNodeResultPageCursor,
    PendingNodeResultPageSize, RunProjection, RunnableRunCandidate, RunnableRunPage,
    RunnableRunPageCursor, RunnableRunPageSize, StoredOutboxDestination, StoredRun,
    ToolInvocationCommitOutcome, ToolInvocationHistoryPage, ToolInvocationHistoryPageSize,
};
pub use store::PostgresStore;
