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
//! explicit at-least-once semantics. Durable interrupt/timer batches commit
//! with initial or successor checkpoint barriers; database-clock terminal APIs,
//! indexed discovery, and explicit cancellation/failure abandonment preserve
//! complete wait evidence. Structured quarantine observations live outside a
//! journal that may be corrupt and atomically revoke execution ownership with
//! exact journal-observation and lost-ack safeguards. A recovery-read combinator
//! triggers that transaction only for payload-redacted integrity failures.
//! Fence-bound claimed recovery sessions additionally scope every exposed page
//! to one run and persist/recheck the detecting attempt and epoch before
//! quarantine, preventing superseded workers from isolating a successor. Their
//! bounded ready-node planner deterministically reuses completed results,
//! classifies fresh/crash/retry/in-flight/terminal work at database time, and
//! feeds a plan-bound durable node-start handoff before any node code runs.
//! Deferred-only plans atomically release ownership into an indexed durable
//! not-before gate while preserving queue age; direct claims cannot bypass the
//! gate, and due work becomes visible without a per-run polling write. The
//! atomic Agent-admission boundary binds an authenticated immutable intent and
//! database clock to an active run, sequence-one event, superstep-zero
//! checkpoint, registered graph, and scheduler projections in one transaction;
//! exact retries fully reload and verify the original commit. Tenant-scoped
//! Agent submission keys store only one-way digests and commit their mapping in
//! that same transaction, so ambiguous client retries converge on one run even
//! when candidate IDs change. The provider never holds a database transaction
//! across node, model, tool, remote-agent, or human work.
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
    AdmissionOutcome, AgentAdmissionCommitOutcome, AgentSubmissionCommitOutcome, AppendOutcome,
    BarrierCommitOutcome, CheckpointCommitOutcome, CheckpointLineagePage,
    CheckpointLineagePageSize, CheckpointPointer, CorruptionQuarantineContext,
    DelayedRetryScheduleOutcome, DueTimerPage, DueTimerPageCursor, ExpiredInterruptPage,
    ExpiredInterruptPageCursor, GraphDefinitionRegistrationOutcome, GraphReplayLimits,
    GraphReplayReport, InterruptResolutionCommitOutcome, JournalPage, JournalPageSize,
    LeaseClaimOutcome, LeaseReleaseOutcome, LeaseRenewalOutcome, LiveLeaseObservation,
    ModelInvocationCommitOutcome, ModelInvocationHistoryPage, ModelInvocationHistoryPageSize,
    NodeAttemptCommitOutcome, NodeAttemptHistoryPage, NodeAttemptHistoryPageSize,
    OutboxAttemptHistoryPage, OutboxAttemptHistoryPageSize, OutboxClaim, OutboxClaimOutcome,
    OutboxCompletionOutcome, OutboxDestinationRegistrationOutcome, OutboxEnqueueOutcome,
    PendingNodeResultCommitOutcome, PendingNodeResultPage, PendingNodeResultPageCursor,
    PendingNodeResultPageSize, RunProjection, RunQuarantine, RunQuarantineCause,
    RunQuarantineCommitOutcome, RunQuarantineComponent, RunQuarantineRequest, RunnableRunCandidate,
    RunnableRunPage, RunnableRunPageCursor, RunnableRunPageSize,
    SchedulerFairnessPolicyRegistration, SchedulerFairnessPolicyRegistrationOutcome,
    SchedulerFairnessReservation, SchedulerFairnessRetentionPolicy,
    SchedulerFairnessRetentionReport, StoredAgentAdmission, StoredAgentSubmission,
    StoredGraphDefinition, StoredOutboxDestination, StoredRun, StoredSchedulerFairnessPolicy,
    TimerFiringCommitOutcome, ToolInvocationCommitOutcome, ToolInvocationHistoryPage,
    ToolInvocationHistoryPageSize, WaitAbandonment, WaitAbandonmentCommitOutcome,
    WaitAbandonmentReason, WaitCheckpointCommitOutcome, WaitDiscoveryPageSize,
};
pub use store::{ClaimedRunRecovery, PostgresStore};
