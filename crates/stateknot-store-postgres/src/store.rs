// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    borrow::Cow,
    collections::BTreeMap,
    fmt,
    future::Future,
    io::Write,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::LazyLock,
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx_core::{
    from_row::FromRow,
    migrate::{Migration, MigrationType, Migrator},
    query::query,
    query_as::query_as,
    query_scalar::query_scalar,
    row::Row,
    transaction::Transaction,
};
use sqlx_postgres::{PgPool, PgRow, Postgres};
use stateknot_core::{
    AgentAdmission, AgentAdmissionAuthority, AgentAdmissionBudgetLayer, AgentAdmissionIntent,
    AgentDescriptor, AgentRequest, AgentResultProvenance, AgentSubmissionKey, AttemptId,
    BarrierResultHeads, BoundedJson, BudgetUsage, CanonicalJson, Checkpoint, CheckpointBarrier,
    CheckpointHead, CheckpointId, CheckpointLineageVerifier, CheckpointState, CheckpointWrite,
    CompiledGraph, DeliveryFence, DeliveryId, DestinationId, Digest, DurableTimer,
    DurableTimerRecord, DurableWait, EventId, Failure, FencingEpoch, GraphBarrierPlanError,
    GraphNamespace, GraphReducer, GraphReducerError, GraphReference, GraphSchemaValidationError,
    GraphSchemaValidator, InterruptId, InterruptRecord, InterruptRequest, InterruptResolution,
    InterruptResolutionIntent, InvocationId, JournalAppend, JournalChainVerifier, JournalEvent,
    JournalEventError, JournalEventIntent, JournalEventSource, JournalHead, JournalPayload,
    JournalSequence, JsonLimits, MAX_OUTBOX_ATTEMPTS, ModelInvocation, ModelInvocationHead,
    ModelInvocationHistoryVerifier, ModelInvocationIntent, ModelInvocationRevision,
    ModelInvocationState, ModelInvocationStatus, ModelInvocationTransition,
    ModelInvocationTransitionKind, NodeActivation, NodeAttempt, NodeAttemptCompletion,
    NodeAttemptHistoryVerifier, NodeAttemptOutcome, NodeAttemptStart, NodeAttemptStartHead,
    NodeAttemptStatus, NodeControlKind, NodeId, NodeInvocationBinding, NodeInvocationBindingKind,
    OutboxAttempt, OutboxAttemptCompletion, OutboxAttemptHistoryVerifier, OutboxAttemptOutcome,
    OutboxAttemptStart, OutboxDelivery, OutboxDeliveryIntent, OutboxDeliveryStatus,
    OutboxDestinationRef, PendingNodeResult, PendingNodeResultError, PendingNodeResultHead,
    PendingNodeResultIntent, QuarantineId, ReadyNodeRecoveryPlan, ReadyNodeRecoveryPlanner,
    ReadyNodes, RecoveryNodeKind, ResolvedBudget, RetryAdvice, RunFence, RunId, RunInterruptKind,
    RunLease, RunLeaseValidationError, RunLifecycle, RunRevision, RunStatus, RunTimerKind,
    RunTransition, RunTransitionKind, RunWaits, SchedulerReservationId, SchedulerShardId,
    Superstep, TenantId, TimerFiring, TimerFiringIntent, TimerId, Timestamp, ToolInvocation,
    ToolInvocationHead, ToolInvocationHistoryVerifier, ToolInvocationIntent,
    ToolInvocationRevision, ToolInvocationStatus, ToolInvocationTransition,
    ToolInvocationTransitionKind, WaitRegistrationIntent,
};
use uuid::Uuid;

use crate::{
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
    PendingNodeResultPageSize, PostgresStoreOptions, RunProjection, RunQuarantine,
    RunQuarantineCause, RunQuarantineCommitOutcome, RunQuarantineComponent, RunQuarantineRequest,
    RunnableRunCandidate, RunnableRunPage, RunnableRunPageCursor, RunnableRunPageSize,
    SchedulerFairnessPolicyRegistration, SchedulerFairnessPolicyRegistrationOutcome,
    SchedulerFairnessReservation, SchedulerFairnessRetentionPolicy,
    SchedulerFairnessRetentionReport, StoreError, StoredAgentAdmission, StoredAgentSubmission,
    StoredGraphDefinition, StoredOutboxDestination, StoredRun, StoredSchedulerFairnessPolicy,
    TimerFiringCommitOutcome, ToolInvocationCommitOutcome, ToolInvocationHistoryPage,
    ToolInvocationHistoryPageSize, WaitAbandonment, WaitAbandonmentCommitOutcome,
    WaitAbandonmentReason, WaitCheckpointCommitOutcome, WaitDiscoveryPageSize,
};

static MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| Migrator {
    migrations: Cow::Owned(vec![
        Migration::new(
            1,
            Cow::Borrowed("initial"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0001_initial.sql")),
            false,
        ),
        Migration::new(
            2,
            Cow::Borrowed("checkpoints"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0002_checkpoints.sql")),
            false,
        ),
        Migration::new(
            3,
            Cow::Borrowed("tool invocations"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0003_tool_invocations.sql")),
            false,
        ),
        Migration::new(
            4,
            Cow::Borrowed("model invocations"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0004_model_invocations.sql")),
            false,
        ),
        Migration::new(
            5,
            Cow::Borrowed("pending node results"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0005_pending_node_results.sql")),
            false,
        ),
        Migration::new(
            6,
            Cow::Borrowed("node attempts"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0006_node_attempts.sql")),
            false,
        ),
        Migration::new(
            7,
            Cow::Borrowed("scheduler readiness"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0007_scheduler_readiness.sql")),
            false,
        ),
        Migration::new(
            8,
            Cow::Borrowed("transactional outbox"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0008_transactional_outbox.sql")),
            false,
        ),
        Migration::new(
            9,
            Cow::Borrowed("durable waits"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0009_durable_waits.sql")),
            false,
        ),
        Migration::new(
            10,
            Cow::Borrowed("run quarantines"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0010_run_quarantines.sql")),
            false,
        ),
        Migration::new(
            11,
            Cow::Borrowed("fenced recovery quarantines"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!(
                "../migrations/0011_fenced_recovery_quarantines.sql"
            )),
            false,
        ),
        Migration::new(
            12,
            Cow::Borrowed("delayed retry wakeup"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0012_delayed_retry_wakeup.sql")),
            false,
        ),
        Migration::new(
            13,
            Cow::Borrowed("compiled graph registry"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0013_graph_registry.sql")),
            false,
        ),
        Migration::new(
            14,
            Cow::Borrowed("distributed scheduler fairness"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0014_scheduler_fairness.sql")),
            false,
        ),
        Migration::new(
            15,
            Cow::Borrowed("atomic agent admissions"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0015_agent_admissions.sql")),
            false,
        ),
        Migration::new(
            16,
            Cow::Borrowed("durable agent submission keys"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0016_agent_submission_keys.sql")),
            false,
        ),
    ]),
    ignore_missing: false,
    locking: true,
    no_tx: false,
});

const MIN_POSTGRES_VERSION_NUMBER: i32 = 160_000;
const MAX_POSTGRES_VERSION_NUMBER: i32 = 179_999;
const MAX_CHECKPOINT_BYTES: usize = 2_621_440;
const MAX_COMPILED_GRAPH_BYTES: usize = CompiledGraph::MAX_DEFINITION_BYTES + 128;
const MAX_AGENT_ADMISSION_BYTES: usize = 16_777_216;
const MAX_TOOL_INVOCATION_INTENT_BYTES: usize = 4_194_304;
const MAX_TOOL_INVOCATION_RECORD_BYTES: usize = 16_777_216;
const MAX_MODEL_INVOCATION_INTENT_BYTES: usize = 134_217_728;
const MAX_MODEL_INVOCATION_RECORD_BYTES: usize = 134_217_728;
const MAX_PENDING_NODE_RESULT_BYTES: usize = 16_777_216;
const MAX_NODE_ATTEMPT_START_BYTES: usize = 1_048_576;
const MAX_NODE_ATTEMPT_COMPLETION_BYTES: usize = 16_777_216;
const MAX_OUTBOX_DESTINATION_BYTES: usize = 2_097_152;
const MAX_OUTBOX_DELIVERY_BYTES: usize = 4_194_304;
const MAX_OUTBOX_ATTEMPT_START_BYTES: usize = 1_048_576;
const MAX_OUTBOX_ATTEMPT_COMPLETION_BYTES: usize = 1_048_576;
const MAX_WAIT_REGISTRATION_BYTES: usize = 4_194_304;
const MAX_INTERRUPT_RESOLUTION_BYTES: usize = 4_194_304;
const MAX_TIMER_FIRING_BYTES: usize = 1_048_576;
const RUN_QUARANTINE_DIGEST_DOMAIN_V1: &[u8] = b"stateknot.run-quarantine.v1\0";
const RUN_QUARANTINE_DIGEST_DOMAIN_V2: &[u8] = b"stateknot.run-quarantine.v2\0";
const MAX_OUTBOX_DELIVERIES_PER_EVENT: usize = 64;
const OUTBOX_TERMINAL_REAP_BATCH_SIZE: i64 = 64;
const PENDING_TOOL_BINDING_BATCH_SIZE: usize = ToolInvocationHistoryPageSize::MAX as usize;
const PENDING_MODEL_BINDING_BATCH_SIZE: usize = ModelInvocationHistoryPageSize::MAX as usize;
const PENDING_INVOCATION_ANCHOR_BATCH_SIZE: usize = 8;
const MODEL_INVOCATION_RECORD_SCHEMA: &str =
    "https://stateknot.github.io/schema/storage/model-invocation-revision/1.0.0";
const PROJECTION_DIGEST_DOMAIN: &[u8] = b"stateknot-postgres-run-projection-v1\0";
const AGENT_ADMISSION_PROJECTION_DIGEST_DOMAIN: &[u8] =
    b"stateknot-postgres-agent-admission-projection-v1\0";
const AGENT_SUBMISSION_DIGEST_DOMAIN: &[u8] = b"stateknot-postgres-agent-submission-v1\0";
const BARRIER_PROJECTION_DIGEST_DOMAIN: &[u8] = b"stateknot-postgres-barrier-projection-v1\0";
const WAIT_SET_DIGEST_DOMAIN: &[u8] = b"stateknot-postgres-wait-set-v1\0";
const WAIT_REGISTRATION_PROJECTION_DIGEST_DOMAIN: &[u8] =
    b"stateknot-postgres-wait-registration-projection-v1\0";
const WAIT_BARRIER_PROJECTION_DIGEST_DOMAIN: &[u8] =
    b"stateknot-postgres-wait-barrier-projection-v1\0";
const INTERRUPT_RESOLUTION_PROJECTION_DIGEST_DOMAIN: &[u8] =
    b"stateknot-postgres-interrupt-resolution-projection-v1\0";
const TIMER_FIRING_PROJECTION_DIGEST_DOMAIN: &[u8] =
    b"stateknot-postgres-timer-firing-projection-v1\0";
const WAIT_ABANDONMENT_DIGEST_DOMAIN: &[u8] = b"stateknot-postgres-wait-abandonment-v1\0";
const WAIT_ABANDONMENT_PROJECTION_DIGEST_DOMAIN: &[u8] =
    b"stateknot-postgres-wait-abandonment-projection-v1\0";

const SELECT_GRAPH_DEFINITION: &str = r"
SELECT
    tenant_id,
    owner_issuer,
    owner_subject,
    graph_name,
    graph_version,
    definition_digest,
    definition_bytes,
    registered_at
FROM stateknot.graph_definitions
WHERE tenant_id = $1
  AND owner_issuer = $2
  AND owner_subject = $3
  AND graph_name = $4
  AND graph_version = $5
";

const SELECT_AGENT_ADMISSION: &str = r"
SELECT
    tenant_id,
    run_id,
    agent_owner_issuer,
    agent_owner_subject,
    agent_name,
    agent_version,
    graph_owner_issuer,
    graph_owner_subject,
    graph_name,
    graph_version,
    graph_definition_digest,
    policy_owner_issuer,
    policy_owner_subject,
    policy_name,
    policy_version,
    policy_digest,
    intent_digest,
    admission_digest,
    admitted_at,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    checkpoint_id,
    checkpoint_superstep,
    checkpoint_digest,
    admission_bytes,
    created_at
FROM stateknot.agent_admissions
WHERE tenant_id = $1 AND run_id = $2
";

const SELECT_AGENT_SUBMISSION: &str = r"
SELECT
    tenant_id,
    key_digest,
    submission_digest,
    run_id,
    admission_digest,
    created_at
FROM stateknot.agent_submission_keys
WHERE tenant_id = $1 AND key_digest = $2
";

const SELECT_AGENT_SUBMISSION_FOR_UPDATE: &str = r"
SELECT
    tenant_id,
    key_digest,
    submission_digest,
    run_id,
    admission_digest,
    created_at
FROM stateknot.agent_submission_keys
WHERE tenant_id = $1 AND key_digest = $2
FOR UPDATE
";

const SELECT_SCHEDULER_FAIRNESS_SHARD: &str = r"
SELECT
    shard_id,
    policy_digest,
    policy_bytes,
    cycle_length,
    next_slot,
    next_sequence,
    registered_at,
    updated_at
FROM stateknot.scheduler_fairness_shards
WHERE shard_id = $1
";

const SELECT_SCHEDULER_FAIRNESS_SHARD_FOR_UPDATE: &str = r"
SELECT
    shard_id,
    policy_digest,
    policy_bytes,
    cycle_length,
    next_slot,
    next_sequence,
    registered_at,
    updated_at
FROM stateknot.scheduler_fairness_shards
WHERE shard_id = $1
FOR UPDATE
";

const SELECT_SCHEDULER_FAIRNESS_RESERVATION: &str = r"
SELECT
    reservation.shard_id,
    reservation.reservation_id,
    reservation.policy_digest,
    reservation.sequence,
    reservation.slot,
    reservation.reserved_at,
    shard.policy_digest AS shard_policy_digest,
    shard.cycle_length
FROM stateknot.scheduler_fairness_reservations AS reservation
JOIN stateknot.scheduler_fairness_shards AS shard
  ON shard.shard_id = reservation.shard_id
WHERE reservation.reservation_id = $1
";

const VERIFY_SCHEMA_OBJECTS: &str = r"
SELECT to_regclass('stateknot.runs') IS NOT NULL
   AND to_regclass('stateknot.graph_definitions') IS NOT NULL
   AND to_regclass('stateknot.graph_definitions_digest_lookup') IS NOT NULL
   AND to_regclass('stateknot.agent_admissions') IS NOT NULL
   AND to_regclass('stateknot.agent_admissions_agent_version') IS NOT NULL
   AND to_regclass('stateknot.agent_admissions_graph_version') IS NOT NULL
   AND to_regclass('stateknot.agent_admissions_policy_version') IS NOT NULL
   AND to_regclass('stateknot.agent_admissions_digest_lookup') IS NOT NULL
   AND to_regclass('stateknot.agent_submission_keys') IS NOT NULL
   AND to_regclass('stateknot.agent_submission_keys_created') IS NOT NULL
   AND to_regclass('stateknot.run_events') IS NOT NULL
   AND to_regclass('stateknot.run_checkpoints') IS NOT NULL
   AND to_regclass('stateknot.tool_invocations') IS NOT NULL
   AND to_regclass('stateknot.tool_invocation_revisions') IS NOT NULL
   AND to_regclass('stateknot.run_attempt_claims') IS NOT NULL
   AND to_regclass('stateknot.model_invocations') IS NOT NULL
   AND to_regclass('stateknot.model_invocation_revisions') IS NOT NULL
   AND to_regclass('stateknot.pending_node_results') IS NOT NULL
   AND to_regclass('stateknot.pending_node_result_tool_bindings') IS NOT NULL
   AND to_regclass('stateknot.pending_node_result_model_bindings') IS NOT NULL
   AND to_regclass('stateknot.pending_node_result_consumptions') IS NOT NULL
   AND to_regclass('stateknot.node_attempts') IS NOT NULL
   AND to_regclass('stateknot.node_attempt_completions') IS NOT NULL
   AND to_regclass('stateknot.runs_scheduler_ready') IS NOT NULL
   AND to_regclass('stateknot.outbox_destinations') IS NOT NULL
   AND to_regclass('stateknot.outbox_deliveries') IS NOT NULL
   AND to_regclass('stateknot.outbox_attempts') IS NOT NULL
   AND to_regclass('stateknot.outbox_attempt_completions') IS NOT NULL
   AND to_regclass('stateknot.outbox_deliveries_ready') IS NOT NULL
   AND to_regclass('stateknot.outbox_deliveries_expiry') IS NOT NULL
   AND to_regclass('stateknot.outbox_deliveries_abandoned_limit') IS NOT NULL
   AND to_regclass('stateknot.run_wait_registrations') IS NOT NULL
   AND to_regclass('stateknot.interrupt_resolutions') IS NOT NULL
   AND to_regclass('stateknot.timer_firings') IS NOT NULL
   AND to_regclass('stateknot.wait_abandonments') IS NOT NULL
   AND to_regclass('stateknot.run_wait_registrations_due') IS NOT NULL
   AND to_regclass('stateknot.run_wait_registrations_expiry') IS NOT NULL
   AND to_regclass('stateknot.run_quarantines') IS NOT NULL
   AND to_regclass('stateknot.run_quarantines_observed') IS NOT NULL
   AND to_regclass('stateknot.scheduler_fairness_shards') IS NOT NULL
   AND to_regclass('stateknot.scheduler_fairness_reservations') IS NOT NULL
   AND to_regclass('stateknot.scheduler_fairness_reservations_sequence') IS NOT NULL
   AND to_regclass('stateknot.scheduler_fairness_reservations_retention') IS NOT NULL
   AND lower(pg_get_indexdef(to_regclass('stateknot.runs_scheduler_ready')))
       LIKE '%checkpoint_id is not null%'
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.graph_definitions')
         AND conname = 'graph_definitions_exact_reference_unique'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_admissions')
         AND conname = 'agent_admissions_graph_fk'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_admissions')
         AND conname = 'agent_admissions_event_fk'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_admissions')
         AND conname = 'agent_admissions_checkpoint_fk'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_admissions')
         AND conname = 'agent_admissions_bytes_bounded'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_admissions')
         AND conname = 'agent_admissions_run_digest_unique'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_submission_keys')
         AND conname = 'agent_submission_keys_run_unique'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_submission_keys')
         AND conname = 'agent_submission_keys_admission_fk'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_submission_keys')
         AND conname = 'agent_submission_keys_tenant_id_valid'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_submission_keys')
         AND conname = 'agent_submission_keys_ids_valid'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.agent_submission_keys')
         AND conname = 'agent_submission_keys_digest_lengths'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.graph_definitions')
         AND conname = 'graph_definitions_bytes_bounded'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.scheduler_fairness_shards')
         AND conname = 'scheduler_fairness_shards_policy_bounded'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.scheduler_fairness_reservations')
         AND conname = 'scheduler_fairness_reservations_id_unique'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.run_quarantines')
         AND conname = 'run_quarantines_fence_shape'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.runs')
         AND conname = 'runs_scheduler_ready_shape'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.runs')
         AND conname = 'runs_scheduler_not_before_shape'
         AND convalidated
   )
   AND EXISTS (
       SELECT 1 FROM pg_catalog.pg_constraint
       WHERE conrelid = to_regclass('stateknot.runs')
         AND conname = 'runs_wait_projection_shape'
         AND convalidated
   )
   AND to_regprocedure('stateknot.is_uuid_v7(uuid)') IS NOT NULL
";

const SELECT_RUN: &str = r"
SELECT
    tenant_id,
    run_id,
    thread_id,
    invocation_id,
    lifecycle_bytes,
    lifecycle_revision::text AS lifecycle_revision,
    lifecycle_status,
    admitted_at,
    changed_at,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    checkpoint_id,
    checkpoint_superstep,
    checkpoint_digest,
    fencing_epoch,
    lease_attempt_id,
    lease_acquired_at,
    lease_renewed_at,
    lease_expires_at,
    scheduler_ready_at,
    scheduler_not_before,
    wait_set_digest,
    unresolved_wait_count,
    next_timer_due_at,
    next_interrupt_expiry_at,
    quarantined_at
FROM stateknot.runs
WHERE tenant_id = $1 AND run_id = $2
";

const SELECT_RUN_FOR_UPDATE: &str = r"
SELECT
    tenant_id,
    run_id,
    thread_id,
    invocation_id,
    lifecycle_bytes,
    lifecycle_revision::text AS lifecycle_revision,
    lifecycle_status,
    admitted_at,
    changed_at,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    checkpoint_id,
    checkpoint_superstep,
    checkpoint_digest,
    fencing_epoch,
    lease_attempt_id,
    lease_acquired_at,
    lease_renewed_at,
    lease_expires_at,
    scheduler_ready_at,
    scheduler_not_before,
    wait_set_digest,
    unresolved_wait_count,
    next_timer_due_at,
    next_interrupt_expiry_at,
    quarantined_at
FROM stateknot.runs
WHERE tenant_id = $1 AND run_id = $2
FOR UPDATE
";

const SELECT_RUNNABLE_RUN_PAGE: &str = r"
SELECT
    tenant_id,
    run_id,
    thread_id,
    invocation_id,
    lifecycle_bytes,
    lifecycle_revision::text AS lifecycle_revision,
    lifecycle_status,
    admitted_at,
    changed_at,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    checkpoint_id,
    checkpoint_superstep,
    checkpoint_digest,
    fencing_epoch,
    lease_attempt_id,
    lease_acquired_at,
    lease_renewed_at,
    lease_expires_at,
    scheduler_ready_at,
    scheduler_not_before,
    wait_set_digest,
    unresolved_wait_count,
    next_timer_due_at,
    next_interrupt_expiry_at,
    quarantined_at
FROM stateknot.runs
WHERE tenant_id = $1
  AND quarantined_at IS NULL
  AND scheduler_ready_at IS NOT NULL
  AND checkpoint_id IS NOT NULL
  AND lifecycle_status IN ('pending', 'active', 'cancellation_requested')
  AND GREATEST(
          scheduler_ready_at,
          COALESCE(scheduler_not_before, scheduler_ready_at),
          COALESCE(lease_expires_at, scheduler_ready_at)
      ) <= $2
  AND (
      GREATEST(
          scheduler_ready_at,
          COALESCE(scheduler_not_before, scheduler_ready_at),
          COALESCE(lease_expires_at, scheduler_ready_at)
      ),
      run_id
  ) > (
      COALESCE($3::timestamptz, '-infinity'::timestamptz),
      COALESCE($4::uuid, '00000000-0000-0000-0000-000000000000'::uuid)
  )
ORDER BY
    GREATEST(
        scheduler_ready_at,
        COALESCE(scheduler_not_before, scheduler_ready_at),
        COALESCE(lease_expires_at, scheduler_ready_at)
    ),
    run_id
LIMIT $5
";

const SELECT_EVENT_BY_ID: &str = r"
SELECT
    tenant_id,
    run_id,
    sequence,
    event_id,
    recorded_at,
    source_kind,
    worker_attempt_id,
    worker_epoch,
    event_kind,
    schema_id,
    schema_version,
    schema_digest,
    payload_bytes,
    payload_digest,
    intent_digest,
    projection_digest,
    previous_digest,
    event_digest
FROM stateknot.run_events
WHERE tenant_id = $1 AND run_id = $2 AND event_id = $3
";

const SELECT_EVENT_BY_SEQUENCE: &str = r"
SELECT
    tenant_id,
    run_id,
    sequence,
    event_id,
    recorded_at,
    source_kind,
    worker_attempt_id,
    worker_epoch,
    event_kind,
    schema_id,
    schema_version,
    schema_digest,
    payload_bytes,
    payload_digest,
    intent_digest,
    projection_digest,
    previous_digest,
    event_digest
FROM stateknot.run_events
WHERE tenant_id = $1 AND run_id = $2 AND sequence = $3
";

const SELECT_EVENT_PAGE: &str = r"
SELECT
    tenant_id,
    run_id,
    sequence,
    event_id,
    recorded_at,
    source_kind,
    worker_attempt_id,
    worker_epoch,
    event_kind,
    schema_id,
    schema_version,
    schema_digest,
    payload_bytes,
    payload_digest,
    intent_digest,
    projection_digest,
    previous_digest,
    event_digest
FROM stateknot.run_events
WHERE tenant_id = $1 AND run_id = $2 AND sequence > $3
ORDER BY sequence ASC
LIMIT $4
";

const WAIT_REGISTRATION_COLUMNS: &str = r"
    tenant_id,
    run_id,
    wait_id,
    wait_kind,
    interrupt_kind,
    timer_kind,
    registered_at,
    due_at,
    expires_at,
    action_digest,
    registration_sequence,
    registration_event_id,
    registration_event_digest,
    intent_digest,
    record_digest,
    record_bytes,
    status,
    terminal_sequence,
    terminal_event_id,
    terminal_recorded_at,
    terminal_event_digest,
    resolution_digest,
    firing_digest,
    abandonment_digest,
    created_at,
    updated_at
";

static SELECT_WAIT_REGISTRATIONS_BY_ORIGIN: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {WAIT_REGISTRATION_COLUMNS} \
         FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 AND run_id = $2 AND registration_sequence = $3 \
         ORDER BY wait_id"
    )
});

static SELECT_WAIT_REGISTRATION_BY_ID: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {WAIT_REGISTRATION_COLUMNS} \
         FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 AND run_id = $2 AND wait_id = $3"
    )
});

static SELECT_WAIT_REGISTRATION_BY_ID_FOR_UPDATE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {WAIT_REGISTRATION_COLUMNS} \
         FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 AND run_id = $2 AND wait_id = $3 \
         FOR UPDATE"
    )
});

static SELECT_OUTSTANDING_WAIT_REGISTRATIONS: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {WAIT_REGISTRATION_COLUMNS} \
         FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 AND run_id = $2 AND status = 'outstanding' \
         ORDER BY wait_id"
    )
});

static SELECT_OUTSTANDING_WAIT_REGISTRATIONS_FOR_UPDATE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {WAIT_REGISTRATION_COLUMNS} \
         FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 AND run_id = $2 AND status = 'outstanding' \
         ORDER BY wait_id FOR UPDATE"
    )
});

const SELECT_INTERRUPT_RESOLUTION: &str = r"
SELECT
    tenant_id,
    run_id,
    interrupt_id,
    request_digest,
    resolution_sequence,
    resolution_event_id,
    resolved_at,
    resolution_event_digest,
    intent_digest,
    resolution_digest,
    resolution_bytes,
    created_at
FROM stateknot.interrupt_resolutions
WHERE tenant_id = $1 AND run_id = $2 AND interrupt_id = $3
";

const SELECT_TIMER_FIRING: &str = r"
SELECT
    tenant_id,
    run_id,
    timer_id,
    timer_digest,
    firing_sequence,
    firing_event_id,
    fired_at,
    firing_event_digest,
    intent_digest,
    firing_digest,
    firing_bytes,
    created_at
FROM stateknot.timer_firings
WHERE tenant_id = $1 AND run_id = $2 AND timer_id = $3
";

static SELECT_DUE_TIMER_PAGE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {WAIT_REGISTRATION_COLUMNS} \
         FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 \
           AND wait_kind = 'timer' \
           AND status = 'outstanding' \
           AND due_at <= $2 \
           AND (due_at, run_id, wait_id) > ( \
               COALESCE($3::timestamptz, '-infinity'::timestamptz), \
               COALESCE($4::uuid, '00000000-0000-0000-0000-000000000000'::uuid), \
               COALESCE($5::uuid, '00000000-0000-0000-0000-000000000000'::uuid) \
           ) \
         ORDER BY due_at, run_id, wait_id \
         LIMIT $6"
    )
});

static SELECT_EXPIRED_INTERRUPT_PAGE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {WAIT_REGISTRATION_COLUMNS} \
         FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 \
           AND wait_kind = 'interrupt' \
           AND status = 'outstanding' \
           AND expires_at IS NOT NULL \
           AND expires_at <= $2 \
           AND (expires_at, run_id, wait_id) > ( \
               COALESCE($3::timestamptz, '-infinity'::timestamptz), \
               COALESCE($4::uuid, '00000000-0000-0000-0000-000000000000'::uuid), \
               COALESCE($5::uuid, '00000000-0000-0000-0000-000000000000'::uuid) \
           ) \
         ORDER BY expires_at, run_id, wait_id \
         LIMIT $6"
    )
});

const WAIT_ABANDONMENT_COLUMNS: &str = r"
    tenant_id,
    run_id,
    wait_id,
    wait_kind,
    registration_digest,
    reason_kind,
    abandonment_sequence,
    abandonment_event_id,
    abandoned_at,
    abandonment_event_digest,
    abandonment_digest,
    created_at
";

static SELECT_WAIT_ABANDONMENTS_BY_EVENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {WAIT_ABANDONMENT_COLUMNS} \
         FROM stateknot.wait_abandonments \
         WHERE tenant_id = $1 AND run_id = $2 AND abandonment_sequence = $3 \
         ORDER BY wait_id"
    )
});

static SELECT_WAIT_ABANDONMENT_BY_ID: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {WAIT_ABANDONMENT_COLUMNS} \
         FROM stateknot.wait_abandonments \
         WHERE tenant_id = $1 AND run_id = $2 AND wait_id = $3"
    )
});

const SELECT_CHECKPOINT_BY_ID: &str = r"
SELECT
    tenant_id,
    run_id,
    checkpoint_id,
    superstep,
    parent_checkpoint_id,
    parent_superstep,
    parent_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    graph_definition_digest,
    state_schema_id,
    state_schema_version,
    state_schema_digest,
    state_digest,
    intent_digest,
    checkpoint_digest,
    checkpoint_bytes
FROM stateknot.run_checkpoints
WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $3
";

const SELECT_CHECKPOINT_BY_ANCHOR: &str = r"
SELECT
    tenant_id,
    run_id,
    checkpoint_id,
    superstep,
    parent_checkpoint_id,
    parent_superstep,
    parent_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    graph_definition_digest,
    state_schema_id,
    state_schema_version,
    state_schema_digest,
    state_digest,
    intent_digest,
    checkpoint_digest,
    checkpoint_bytes
FROM stateknot.run_checkpoints
WHERE tenant_id = $1 AND run_id = $2 AND journal_sequence = $3
";

const SELECT_CHECKPOINT_LINEAGE: &str = r"
WITH RECURSIVE checkpoint_lineage AS (
    SELECT current_checkpoint.*, 0::bigint AS lineage_depth
    FROM stateknot.run_checkpoints AS current_checkpoint
    WHERE current_checkpoint.tenant_id = $1
      AND current_checkpoint.run_id = $2
      AND current_checkpoint.checkpoint_id = $3

    UNION ALL

    SELECT parent_checkpoint.*, child.lineage_depth + 1
    FROM stateknot.run_checkpoints AS parent_checkpoint
    JOIN checkpoint_lineage AS child
      ON parent_checkpoint.tenant_id = child.tenant_id
     AND parent_checkpoint.run_id = child.run_id
     AND parent_checkpoint.checkpoint_id = child.parent_checkpoint_id
     AND parent_checkpoint.superstep = child.parent_superstep
     AND parent_checkpoint.checkpoint_digest = child.parent_digest
    WHERE child.lineage_depth + 1 < $4
)
SELECT
    tenant_id,
    run_id,
    checkpoint_id,
    superstep,
    parent_checkpoint_id,
    parent_superstep,
    parent_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    graph_definition_digest,
    state_schema_id,
    state_schema_version,
    state_schema_digest,
    state_digest,
    intent_digest,
    checkpoint_digest,
    checkpoint_bytes
FROM checkpoint_lineage
ORDER BY lineage_depth ASC
";

const SELECT_EVENTS_BY_SEQUENCES: &str = r"
SELECT
    tenant_id,
    run_id,
    sequence,
    event_id,
    recorded_at,
    source_kind,
    worker_attempt_id,
    worker_epoch,
    event_kind,
    schema_id,
    schema_version,
    schema_digest,
    payload_bytes,
    payload_digest,
    intent_digest,
    projection_digest,
    previous_digest,
    event_digest
FROM stateknot.run_events
WHERE tenant_id = $1 AND run_id = $2 AND sequence = ANY($3)
ORDER BY sequence DESC
";

const SELECT_TOOL_INVOCATION: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    intent_digest,
    intent_bytes,
    current_revision,
    current_status,
    current_attempt_id,
    current_record_digest,
    created_at,
    updated_at
FROM stateknot.tool_invocations
WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3
";

const SELECT_TOOL_INVOCATION_FOR_UPDATE: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    intent_digest,
    intent_bytes,
    current_revision,
    current_status,
    current_attempt_id,
    current_record_digest,
    created_at,
    updated_at
FROM stateknot.tool_invocations
WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3
FOR UPDATE
";

const SELECT_TOOL_INVOCATION_REVISION: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
FROM stateknot.tool_invocation_revisions
WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3 AND revision = $4
";

const SELECT_TOOL_INVOCATION_REVISION_BY_ANCHOR: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
FROM stateknot.tool_invocation_revisions
WHERE tenant_id = $1 AND run_id = $2 AND journal_sequence = $3
";

const SELECT_NODE_ATTEMPT_BY_ID: &str = r"
SELECT
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    base_journal_sequence,
    base_journal_event_id,
    base_journal_recorded_at,
    base_journal_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    activation_digest,
    attempt_id,
    fence_attempt_id,
    fence_epoch,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    start_digest,
    start_bytes,
    created_at
FROM stateknot.node_attempts
WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3
";

const SELECT_NODE_ATTEMPT_BY_ID_FOR_UPDATE: &str = r"
SELECT
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    base_journal_sequence,
    base_journal_event_id,
    base_journal_recorded_at,
    base_journal_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    activation_digest,
    attempt_id,
    fence_attempt_id,
    fence_epoch,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    start_digest,
    start_bytes,
    created_at
FROM stateknot.node_attempts
WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3
FOR UPDATE
";

const SELECT_NODE_ATTEMPT_COMPLETION: &str = r"
SELECT
    tenant_id,
    run_id,
    attempt_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    activation_digest,
    fence_attempt_id,
    fence_epoch,
    start_journal_sequence,
    start_journal_event_id,
    start_journal_recorded_at,
    start_journal_digest,
    start_digest,
    status,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    result_intent_digest,
    result_record_digest,
    failure_id,
    retry_kind,
    retry_not_before,
    completion_digest,
    completion_bytes,
    created_at
FROM stateknot.node_attempt_completions
WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3
";

const SELECT_NODE_ATTEMPT_HISTORY: &str = r"
SELECT
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    base_journal_sequence,
    base_journal_event_id,
    base_journal_recorded_at,
    base_journal_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    activation_digest,
    attempt_id,
    fence_attempt_id,
    fence_epoch,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    start_digest,
    start_bytes,
    created_at
FROM stateknot.node_attempts
WHERE tenant_id = $1
  AND run_id = $2
  AND base_checkpoint_id = $3
  AND base_superstep = $4
  AND base_checkpoint_digest = $5
  AND graph_namespace = $6
  AND node_id = $7
  AND activation_input_digest = $8
  AND journal_sequence > $9
ORDER BY journal_sequence ASC
LIMIT $10
";

const SELECT_NODE_ATTEMPT_COUNT: &str = r"
SELECT count(*)
FROM stateknot.node_attempts
WHERE tenant_id = $1
  AND run_id = $2
  AND base_checkpoint_id = $3
  AND base_superstep = $4
  AND base_checkpoint_digest = $5
  AND graph_namespace = $6
  AND node_id = $7
  AND activation_input_digest = $8
";

const SELECT_LATEST_NODE_ATTEMPT_FOR_UPDATE: &str = r"
SELECT
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    base_journal_sequence,
    base_journal_event_id,
    base_journal_recorded_at,
    base_journal_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    activation_digest,
    attempt_id,
    fence_attempt_id,
    fence_epoch,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    start_digest,
    start_bytes,
    created_at
FROM stateknot.node_attempts
WHERE tenant_id = $1
  AND run_id = $2
  AND base_checkpoint_id = $3
  AND base_superstep = $4
  AND base_checkpoint_digest = $5
  AND graph_namespace = $6
  AND node_id = $7
  AND activation_input_digest = $8
ORDER BY journal_sequence DESC
LIMIT 1
FOR UPDATE
";

const SELECT_TOOL_INVOCATION_HISTORY: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
FROM stateknot.tool_invocation_revisions
WHERE tenant_id = $1
  AND run_id = $2
  AND invocation_id = $3
  AND revision > $4
ORDER BY revision ASC
LIMIT $5
";

const SELECT_UNSETTLED_TOOL_INVOCATION_EXISTS: &str = r"
SELECT EXISTS (
    SELECT 1
    FROM stateknot.tool_invocations
    WHERE tenant_id = $1
      AND run_id = $2
      AND base_checkpoint_id = $3
      AND base_superstep = $4
      AND base_checkpoint_digest = $5
      AND current_status <> 'committed'
)
";

const SELECT_MODEL_INVOCATION: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    intent_digest,
    intent_bytes,
    current_revision,
    current_status,
    current_attempt_id,
    current_record_digest,
    created_at,
    updated_at
FROM stateknot.model_invocations
WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3
";

const SELECT_MODEL_INVOCATION_FOR_UPDATE: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    intent_digest,
    intent_bytes,
    current_revision,
    current_status,
    current_attempt_id,
    current_record_digest,
    created_at,
    updated_at
FROM stateknot.model_invocations
WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3
FOR UPDATE
";

const SELECT_MODEL_INVOCATION_REVISION: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
FROM stateknot.model_invocation_revisions
WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3 AND revision = $4
";

const SELECT_MODEL_INVOCATION_REVISION_BY_ANCHOR: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
FROM stateknot.model_invocation_revisions
WHERE tenant_id = $1 AND run_id = $2 AND journal_sequence = $3
";

const SELECT_MODEL_INVOCATION_HISTORY: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
FROM stateknot.model_invocation_revisions
WHERE tenant_id = $1
  AND run_id = $2
  AND invocation_id = $3
  AND revision > $4
ORDER BY revision ASC
LIMIT $5
";

const SELECT_UNSETTLED_MODEL_INVOCATION_EXISTS: &str = r"
SELECT EXISTS (
    SELECT 1
    FROM stateknot.model_invocations
    WHERE tenant_id = $1
      AND run_id = $2
      AND base_checkpoint_id = $3
      AND base_superstep = $4
      AND base_checkpoint_digest = $5
      AND current_status <> 'committed'
)
";

const SELECT_PENDING_NODE_RESULT: &str = r"
SELECT
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    base_journal_sequence,
    base_journal_event_id,
    base_journal_recorded_at,
    base_journal_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    node_attempt_id,
    intent_digest,
    control_kind,
    fence_attempt_id,
    fence_epoch,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    record_digest,
    result_bytes,
    created_at
FROM stateknot.pending_node_results
WHERE tenant_id = $1
  AND run_id = $2
  AND base_checkpoint_id = $3
  AND graph_namespace = $4
  AND node_id = $5
";

const SELECT_UNCONSUMED_PENDING_NODE_RESULT_HEADS: &str = r"
SELECT
    pending.tenant_id,
    pending.run_id,
    pending.base_checkpoint_id,
    pending.base_superstep,
    pending.base_checkpoint_digest,
    pending.graph_namespace,
    pending.node_id,
    pending.activation_input_digest,
    pending.intent_digest,
    pending.fence_attempt_id,
    pending.fence_epoch,
    pending.journal_sequence,
    pending.journal_event_id,
    pending.journal_recorded_at,
    pending.journal_digest,
    pending.record_digest
FROM stateknot.pending_node_results AS pending
WHERE pending.tenant_id = $1
  AND pending.run_id = $2
  AND pending.base_checkpoint_id = $3
  AND pending.base_superstep = $4
  AND pending.base_checkpoint_digest = $5
  AND NOT EXISTS (
      SELECT 1
      FROM stateknot.pending_node_result_consumptions AS consumed
      WHERE consumed.tenant_id = pending.tenant_id
        AND consumed.run_id = pending.run_id
        AND consumed.base_checkpoint_id = pending.base_checkpoint_id
        AND consumed.graph_namespace = pending.graph_namespace
        AND consumed.node_id = pending.node_id
  )
ORDER BY pending.graph_namespace ASC, pending.node_id ASC
LIMIT $6
";

const SELECT_UNCONSUMED_PENDING_NODE_RESULT_HEADS_AFTER: &str = r"
SELECT
    pending.tenant_id,
    pending.run_id,
    pending.base_checkpoint_id,
    pending.base_superstep,
    pending.base_checkpoint_digest,
    pending.graph_namespace,
    pending.node_id,
    pending.activation_input_digest,
    pending.intent_digest,
    pending.fence_attempt_id,
    pending.fence_epoch,
    pending.journal_sequence,
    pending.journal_event_id,
    pending.journal_recorded_at,
    pending.journal_digest,
    pending.record_digest
FROM stateknot.pending_node_results AS pending
WHERE pending.tenant_id = $1
  AND pending.run_id = $2
  AND pending.base_checkpoint_id = $3
  AND pending.base_superstep = $4
  AND pending.base_checkpoint_digest = $5
  AND (pending.graph_namespace, pending.node_id) > ($6, $7)
  AND NOT EXISTS (
      SELECT 1
      FROM stateknot.pending_node_result_consumptions AS consumed
      WHERE consumed.tenant_id = pending.tenant_id
        AND consumed.run_id = pending.run_id
        AND consumed.base_checkpoint_id = pending.base_checkpoint_id
        AND consumed.graph_namespace = pending.graph_namespace
        AND consumed.node_id = pending.node_id
  )
ORDER BY pending.graph_namespace ASC, pending.node_id ASC
LIMIT $8
";

const SELECT_RUN_QUARANTINE_TARGET_FOR_UPDATE: &str = r"
SELECT
    tenant_id,
    run_id,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    lease_attempt_id,
    fencing_epoch,
    lease_renewed_at,
    lease_expires_at,
    quarantined_at,
    quarantine_reason
FROM stateknot.runs
WHERE tenant_id = $1 AND run_id = $2
FOR UPDATE
";

const SELECT_RUN_QUARANTINE_BY_RUN: &str = r"
SELECT
    q.tenant_id,
    q.run_id,
    q.quarantine_id,
    q.quarantined_at,
    q.cause_kind,
    q.component,
    q.evidence_digest,
    q.expected_journal_sequence,
    q.expected_journal_event_id,
    q.expected_journal_recorded_at,
    q.expected_journal_digest,
    q.expected_fence_attempt_id,
    q.expected_fence_epoch,
    q.record_digest,
    q.created_at,
    r.quarantined_at AS run_quarantined_at,
    r.quarantine_reason AS run_quarantine_reason,
    r.lease_attempt_id AS run_lease_attempt_id,
    r.lease_acquired_at AS run_lease_acquired_at,
    r.lease_renewed_at AS run_lease_renewed_at,
    r.lease_expires_at AS run_lease_expires_at,
    r.fencing_epoch AS run_fencing_epoch,
    r.scheduler_ready_at AS run_scheduler_ready_at,
    r.updated_at AS run_updated_at
FROM stateknot.run_quarantines AS q
JOIN stateknot.runs AS r
  ON r.tenant_id = q.tenant_id AND r.run_id = q.run_id
WHERE q.tenant_id = $1 AND q.run_id = $2
";

const SELECT_RUN_QUARANTINE_BY_ID: &str = r"
SELECT
    q.tenant_id,
    q.run_id,
    q.quarantine_id,
    q.quarantined_at,
    q.cause_kind,
    q.component,
    q.evidence_digest,
    q.expected_journal_sequence,
    q.expected_journal_event_id,
    q.expected_journal_recorded_at,
    q.expected_journal_digest,
    q.expected_fence_attempt_id,
    q.expected_fence_epoch,
    q.record_digest,
    q.created_at,
    r.quarantined_at AS run_quarantined_at,
    r.quarantine_reason AS run_quarantine_reason,
    r.lease_attempt_id AS run_lease_attempt_id,
    r.lease_acquired_at AS run_lease_acquired_at,
    r.lease_renewed_at AS run_lease_renewed_at,
    r.lease_expires_at AS run_lease_expires_at,
    r.fencing_epoch AS run_fencing_epoch,
    r.scheduler_ready_at AS run_scheduler_ready_at,
    r.updated_at AS run_updated_at
FROM stateknot.run_quarantines AS q
JOIN stateknot.runs AS r
  ON r.tenant_id = q.tenant_id AND r.run_id = q.run_id
WHERE q.tenant_id = $1 AND q.quarantine_id = $2
";

const SELECT_PENDING_NODE_RESULT_HEAD: &str = r"
SELECT
    pending.tenant_id,
    pending.run_id,
    pending.base_checkpoint_id,
    pending.base_superstep,
    pending.base_checkpoint_digest,
    pending.graph_namespace,
    pending.node_id,
    pending.activation_input_digest,
    pending.intent_digest,
    pending.fence_attempt_id,
    pending.fence_epoch,
    pending.journal_sequence,
    pending.journal_event_id,
    pending.journal_recorded_at,
    pending.journal_digest,
    pending.record_digest
FROM stateknot.pending_node_results AS pending
WHERE pending.tenant_id = $1
  AND pending.run_id = $2
  AND pending.base_checkpoint_id = $3
  AND pending.graph_namespace = $4
  AND pending.node_id = $5
";

const SELECT_PENDING_NODE_RESULT_HEADS_FOR_BARRIER: &str = r"
SELECT
    pending.tenant_id,
    pending.run_id,
    pending.base_checkpoint_id,
    pending.base_superstep,
    pending.base_checkpoint_digest,
    pending.graph_namespace,
    pending.node_id,
    pending.activation_input_digest,
    pending.intent_digest,
    pending.fence_attempt_id,
    pending.fence_epoch,
    pending.journal_sequence,
    pending.journal_event_id,
    pending.journal_recorded_at,
    pending.journal_digest,
    pending.record_digest
FROM stateknot.pending_node_results AS pending
WHERE pending.tenant_id = $1
  AND pending.run_id = $2
  AND pending.base_checkpoint_id = $3
ORDER BY pending.graph_namespace ASC, pending.node_id ASC
LIMIT $4
";

const SELECT_PENDING_NODE_RESULT_CONSUMPTIONS_BY_BASE: &str = r"
SELECT
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    result_record_digest,
    successor_checkpoint_id,
    successor_superstep,
    successor_checkpoint_digest,
    successor_journal_sequence,
    successor_journal_event_id,
    successor_journal_recorded_at,
    successor_journal_digest,
    created_at
FROM stateknot.pending_node_result_consumptions
WHERE tenant_id = $1
  AND run_id = $2
  AND base_checkpoint_id = $3
ORDER BY graph_namespace ASC, node_id ASC
";

const SELECT_PENDING_NODE_RESULT_TOOL_BINDINGS: &str = r"
SELECT
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    result_record_digest,
    result_journal_sequence,
    result_journal_recorded_at,
    result_journal_digest,
    invocation_id,
    invocation_revision,
    invocation_record_digest,
    invocation_journal_sequence,
    invocation_journal_recorded_at,
    invocation_journal_digest
FROM stateknot.pending_node_result_tool_bindings
WHERE tenant_id = $1
  AND run_id = $2
  AND base_checkpoint_id = $3
  AND graph_namespace = $4
  AND node_id = $5
ORDER BY invocation_id ASC
";

const SELECT_PENDING_NODE_RESULT_MODEL_BINDINGS: &str = r"
SELECT
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    result_record_digest,
    result_journal_sequence,
    result_journal_recorded_at,
    result_journal_digest,
    invocation_id,
    invocation_revision,
    invocation_record_digest,
    invocation_journal_sequence,
    invocation_journal_recorded_at,
    invocation_journal_digest
FROM stateknot.pending_node_result_model_bindings
WHERE tenant_id = $1
  AND run_id = $2
  AND base_checkpoint_id = $3
  AND graph_namespace = $4
  AND node_id = $5
ORDER BY invocation_id ASC
";

const SELECT_TOOL_INVOCATIONS_BY_IDS: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    intent_digest,
    intent_bytes,
    current_revision,
    current_status,
    current_attempt_id,
    current_record_digest,
    created_at,
    updated_at
FROM stateknot.tool_invocations
WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = ANY($3)
ORDER BY invocation_id ASC
";

const SELECT_TOOL_INVOCATION_REVISIONS_BY_HEADS: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
FROM stateknot.tool_invocation_revisions
WHERE tenant_id = $1
  AND run_id = $2
  AND (invocation_id, revision) IN (
      SELECT * FROM UNNEST($3::uuid[], $4::bigint[])
  )
ORDER BY invocation_id ASC, revision ASC
";

const SELECT_MODEL_INVOCATIONS_BY_IDS: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    intent_digest,
    intent_bytes,
    current_revision,
    current_status,
    current_attempt_id,
    current_record_digest,
    created_at,
    updated_at
FROM stateknot.model_invocations
WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = ANY($3)
ORDER BY invocation_id ASC
";

const SELECT_MODEL_INVOCATION_REVISIONS_BY_HEADS: &str = r"
SELECT
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
FROM stateknot.model_invocation_revisions
WHERE tenant_id = $1
  AND run_id = $2
  AND (invocation_id, revision) IN (
      SELECT * FROM UNNEST($3::uuid[], $4::bigint[])
  )
ORDER BY invocation_id ASC, revision ASC
";

const SELECT_OUTBOX_DESTINATION: &str = r"
SELECT
    tenant_id,
    destination_id,
    snapshot_digest,
    config_kind,
    schema_id,
    schema_version,
    schema_digest,
    config_bytes,
    created_at
FROM stateknot.outbox_destinations
WHERE tenant_id = $1
  AND destination_id = $2
  AND snapshot_digest = $3
";

const OUTBOX_DELIVERY_COLUMNS: &str = r"
    tenant_id,
    run_id,
    delivery_id,
    origin_sequence,
    origin_event_id,
    origin_recorded_at,
    origin_digest,
    destination_id,
    destination_snapshot_digest,
    intent_digest,
    expires_at,
    delivery_digest,
    delivery_bytes,
    status,
    attempt_count,
    current_attempt_id,
    current_epoch,
    current_attempt_started_at,
    current_attempt_expires_at,
    next_attempt_at,
    last_completion_digest,
    terminal_at,
    created_at,
    updated_at
";

static SELECT_OUTBOX_DELIVERY: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {OUTBOX_DELIVERY_COLUMNS} FROM stateknot.outbox_deliveries \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3"
    )
});

static SELECT_OUTBOX_DELIVERY_FOR_UPDATE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {OUTBOX_DELIVERY_COLUMNS} FROM stateknot.outbox_deliveries \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3 FOR UPDATE"
    )
});

static SELECT_OUTBOX_DELIVERIES_BY_ORIGIN: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {OUTBOX_DELIVERY_COLUMNS} FROM stateknot.outbox_deliveries \
         WHERE tenant_id = $1 AND run_id = $2 AND origin_sequence = $3 \
         ORDER BY delivery_id ASC"
    )
});

static SELECT_OUTBOX_CLAIM_CANDIDATE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {OUTBOX_DELIVERY_COLUMNS} FROM stateknot.outbox_deliveries \
         WHERE tenant_id = $1 \
           AND status IN ('pending', 'delivering', 'retry_scheduled') \
           AND attempt_count < 64 \
           AND next_attempt_at <= $2 \
           AND expires_at > $2 \
         ORDER BY next_attempt_at ASC, delivery_id ASC \
         FOR UPDATE SKIP LOCKED LIMIT 1"
    )
});

const OUTBOX_ATTEMPT_COLUMNS: &str = r"
    tenant_id,
    run_id,
    delivery_id,
    delivery_expires_at,
    delivery_digest,
    epoch,
    attempt_id,
    started_at,
    expires_at,
    start_digest,
    start_bytes,
    created_at
";

static SELECT_OUTBOX_ATTEMPT_BY_ID: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {OUTBOX_ATTEMPT_COLUMNS} FROM stateknot.outbox_attempts \
         WHERE tenant_id = $1 AND attempt_id = $2"
    )
});

static SELECT_OUTBOX_ATTEMPT_BY_FENCE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {OUTBOX_ATTEMPT_COLUMNS} FROM stateknot.outbox_attempts \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3 \
           AND epoch = $4 AND attempt_id = $5"
    )
});

static SELECT_OUTBOX_ATTEMPT_HISTORY: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT {OUTBOX_ATTEMPT_COLUMNS} FROM stateknot.outbox_attempts \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3 AND epoch > $4 \
         ORDER BY epoch ASC LIMIT $5"
    )
});

const SELECT_OUTBOX_ATTEMPT_COMPLETION: &str = r"
SELECT
    tenant_id,
    run_id,
    delivery_id,
    epoch,
    attempt_id,
    started_at,
    attempt_expires_at,
    start_digest,
    outcome_kind,
    retry_advice_kind,
    retry_delay_millis,
    completed_at,
    completion_digest,
    completion_bytes,
    created_at
FROM stateknot.outbox_attempt_completions
WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3 AND epoch = $4
";

const SELECT_OUTBOX_ATTEMPT_COMPLETION_HISTORY: &str = r"
SELECT
    tenant_id,
    run_id,
    delivery_id,
    epoch,
    attempt_id,
    started_at,
    attempt_expires_at,
    start_digest,
    outcome_kind,
    retry_advice_kind,
    retry_delay_millis,
    completed_at,
    completion_digest,
    completion_bytes,
    created_at
FROM stateknot.outbox_attempt_completions
WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3
ORDER BY epoch ASC
LIMIT $4
";

/// Connected `PostgreSQL` durability provider.
///
/// Clones share one bounded connection pool. `Debug` intentionally omits pool
/// internals and connection credentials.
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    options: PostgresStoreOptions,
}

/// Fence-bound, corruption-quarantining read surface for one claimed run.
///
/// A session is created only after the database confirms the exact live
/// [`RunFence`], runnable projection, tenant/run scope, and journal observation.
/// Every recovery read exposed here maps durable [`StoreError::CorruptData`]
/// through one stable [`CorruptionQuarantineContext`]. Ordinary availability,
/// cursor, contention, and stale-observation failures never quarantine.
///
/// The initial snapshot is evidence about the recovery starting point, not an
/// authorization to perform external I/O. Manual page consumers call
/// [`Self::revalidate`] before handing recovered work to a durable start API.
/// [`Self::plan_ready_nodes`] performs that final revalidation itself, and
/// [`PostgresStore::start_recovered_node_attempt`] remains the authoritative
/// transactional dispatch fence.
pub struct ClaimedRunRecovery<'store> {
    store: &'store PostgresStore,
    fence: RunFence,
    context: CorruptionQuarantineContext,
    initial_run: StoredRun,
    initial_observed_at: Timestamp,
}

struct ClaimedRunRecoveryObservation {
    run: StoredRun,
    observed_at: Timestamp,
}

impl ClaimedRunRecovery<'_> {
    /// Returns the exact worker fence validated when this session was created.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Returns the stable corruption evidence intent used by every read.
    #[must_use]
    pub const fn quarantine_context(&self) -> &CorruptionQuarantineContext {
        &self.context
    }

    /// Returns the fully validated run snapshot observed at session creation.
    #[must_use]
    pub const fn initial_run(&self) -> &StoredRun {
        &self.initial_run
    }

    /// Returns the database clock observed atomically with the initial run and
    /// live-fence validation.
    #[must_use]
    pub const fn initial_observed_at(&self) -> Timestamp {
        self.initial_observed_at
    }

    /// Rechecks the live fence, runnable projection, and exact journal head.
    ///
    /// Recovery callers should invoke this after consuming their bounded pages
    /// and before preparing any durable dispatch attempt. A lease renewal under
    /// the same fence is accepted; expiry, supersession, quarantine, lifecycle
    /// removal, or journal progress aborts the handoff explicitly.
    ///
    /// # Errors
    ///
    /// Returns fencing, lifecycle, stale-observation, corruption/quarantine, or
    /// database failures. Corruption is quarantined before this method returns.
    pub async fn revalidate(&self) -> Result<StoredRun, StoreError> {
        self.store
            .with_corruption_quarantine(
                self.context.clone(),
                self.store
                    .load_claimed_run_recovery_snapshot(&self.fence, &self.context),
            )
            .await
            .map(|observation| observation.run)
    }

    /// Loads the exact compiled graph pinned by this recovery checkpoint.
    ///
    /// The method revalidates the live fence and journal observation both
    /// before and after loading the immutable checkpoint and tenant registry
    /// row. It recompiles the canonical definition, verifies its digest and
    /// owner-qualified identity, and rejects unknown ready nodes or an initial
    /// ready set that differs from the compiled entry set.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ReadyNodeRecoveryCheckpointMissing`] before an
    /// initial checkpoint exists. A missing, mismatched, or corrupt pinned
    /// definition is quarantined before returning [`StoreError::RunQuarantined`].
    pub async fn load_pinned_graph(&self) -> Result<StoredGraphDefinition, StoreError> {
        Box::pin(self.read(self.load_pinned_graph_inner())).await
    }

    async fn load_pinned_graph_inner(&self) -> Result<StoredGraphDefinition, StoreError> {
        let before = self
            .store
            .load_claimed_run_recovery_snapshot(&self.fence, &self.context)
            .await?;
        let pointer = before
            .run
            .checkpoint()
            .ok_or(StoreError::ReadyNodeRecoveryCheckpointMissing)?;
        if self.initial_run.checkpoint() != Some(pointer) {
            return Err(StoreError::corrupt("pinned graph checkpoint projection"));
        }
        let checkpoint = self.load_recovery_checkpoint(pointer).await?;
        let definition = self.load_graph_for_checkpoint(&checkpoint).await?;
        let after = self
            .store
            .load_claimed_run_recovery_snapshot(&self.fence, &self.context)
            .await?;
        if after.run.checkpoint() != Some(pointer) {
            return Err(StoreError::corrupt("pinned graph checkpoint projection"));
        }
        Ok(definition)
    }

    /// Independently replays every committed noninitial checkpoint transition.
    ///
    /// Lineage pages are streamed newest-to-oldest while retaining only the
    /// current child. For each parent, the provider reloads every exact pending
    /// result and its consumption row in one repeatable-read transaction,
    /// enforces the configured compact-byte ceiling, then closes that
    /// transaction before invoking the schema registry or reducer. The graph
    /// planner is given the already committed child's checkpoint ID and its
    /// derived successor write must match every semantic child field.
    ///
    /// A schema/reducer that is unavailable, panics, or cannot satisfy its
    /// local resource bound is an operational deployment failure and does not
    /// quarantine durable data. Missing results, invalid consumption rows,
    /// rejected durable values, nondeterministic state/routing, and any other
    /// parent-to-child mismatch are payload-redacted corruption and trigger the
    /// session's exact fence-protected quarantine before this method returns.
    /// A final snapshot revalidates the original journal observation, current
    /// checkpoint pointer, runnable lifecycle, and live fence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ReadyNodeRecoveryCheckpointMissing`] before an
    /// initial checkpoint, [`StoreError::GraphReplayDependencyUnavailable`] for
    /// missing or failed executable dependencies,
    /// [`StoreError::GraphReplayResourceLimit`] when one barrier exceeds
    /// `limits`, ordinary fencing/database/staleness errors, or
    /// [`StoreError::RunQuarantined`] after a durable replay mismatch is
    /// isolated.
    pub async fn validate_noninitial_replay<V, R>(
        &self,
        schemas: &V,
        reducer: &R,
        limits: GraphReplayLimits,
    ) -> Result<GraphReplayReport, StoreError>
    where
        V: GraphSchemaValidator + ?Sized,
        R: GraphReducer + ?Sized,
    {
        Box::pin(self.read(self.validate_noninitial_replay_inner(schemas, reducer, limits))).await
    }

    #[allow(clippy::too_many_lines)]
    async fn validate_noninitial_replay_inner<V, R>(
        &self,
        schemas: &V,
        reducer: &R,
        limits: GraphReplayLimits,
    ) -> Result<GraphReplayReport, StoreError>
    where
        V: GraphSchemaValidator + ?Sized,
        R: GraphReducer + ?Sized,
    {
        let pointer = self
            .initial_run
            .checkpoint()
            .ok_or(StoreError::ReadyNodeRecoveryCheckpointMissing)?;
        let definition = self.load_pinned_graph_inner().await?;
        let graph = definition.graph();
        if reducer.reference() != graph.reducer() {
            return Err(StoreError::GraphReplayDependencyUnavailable);
        }

        let page_size = CheckpointLineagePageSize::new(CheckpointLineagePageSize::MAX)?;
        let mut cursor = None;
        let mut child: Option<Checkpoint> = None;
        let mut checkpoints_validated = 0_u64;
        let mut barriers_replayed = 0_u64;
        let mut results_replayed = 0_u64;
        let mut maximum_barrier_result_bytes = 0_usize;

        loop {
            let page = self
                .store
                .load_checkpoint_lineage_page(
                    self.fence.tenant_id(),
                    self.fence.run_id(),
                    cursor.as_ref(),
                    page_size,
                )
                .await?;
            if page.checkpoints().is_empty() {
                return Err(StoreError::corrupt("noninitial graph replay lineage"));
            }

            for parent in page.checkpoints() {
                if checkpoints_validated == 0
                    && (parent.checkpoint_id() != pointer.checkpoint_id()
                        || parent.superstep() != pointer.superstep()
                        || parent.digest() != pointer.digest())
                {
                    return Err(StoreError::corrupt(
                        "noninitial graph replay checkpoint projection",
                    ));
                }
                if parent.graph() != &graph.reference() {
                    return Err(StoreError::corrupt("noninitial graph replay graph binding"));
                }
                validate_graph_replay_checkpoint_state(graph, parent, schemas)?;
                checkpoints_validated = checkpoints_validated
                    .checked_add(1)
                    .ok_or_else(|| StoreError::corrupt("noninitial graph replay count"))?;

                if let Some(committed_child) = &child {
                    let inputs = self
                        .store
                        .load_historical_graph_barrier_results(parent, committed_child, limits)
                        .await?;
                    let result_count = u64::try_from(inputs.results.len())
                        .map_err(|_| StoreError::corrupt("noninitial graph replay count"))?;
                    results_replayed = results_replayed
                        .checked_add(result_count)
                        .ok_or_else(|| StoreError::corrupt("noninitial graph replay count"))?;
                    maximum_barrier_result_bytes =
                        maximum_barrier_result_bytes.max(inputs.compact_bytes);

                    let plan = graph
                        .plan_barrier(
                            parent,
                            &inputs.results,
                            committed_child.checkpoint_id(),
                            schemas,
                            reducer,
                        )
                        .map_err(|error| map_graph_replay_plan_error(&error))?;
                    if !committed_child.matches_write(plan.barrier().successor()) {
                        return Err(StoreError::corrupt(
                            "noninitial graph replay successor mismatch",
                        ));
                    }
                    barriers_replayed = barriers_replayed
                        .checked_add(1)
                        .ok_or_else(|| StoreError::corrupt("noninitial graph replay count"))?;
                }
                child = Some(parent.clone());
            }

            let Some(next_cursor) = page.next_cursor() else {
                break;
            };
            cursor = Some(next_cursor);
        }

        let root = child.ok_or_else(|| StoreError::corrupt("noninitial graph replay lineage"))?;
        if root.superstep() != Superstep::INITIAL
            || root.parent().is_some()
            || root.ready_nodes() != graph.entry_nodes()
        {
            return Err(StoreError::corrupt("noninitial graph replay root"));
        }
        if barriers_replayed != checkpoints_validated.saturating_sub(1) {
            return Err(StoreError::corrupt("noninitial graph replay coverage"));
        }

        let after = self
            .store
            .load_claimed_run_recovery_snapshot(&self.fence, &self.context)
            .await?;
        if after.run.checkpoint() != Some(pointer) {
            return Err(StoreError::corrupt(
                "noninitial graph replay checkpoint projection",
            ));
        }

        Ok(GraphReplayReport {
            checkpoints_validated,
            barriers_replayed,
            results_replayed,
            maximum_barrier_result_bytes,
        })
    }

    async fn load_graph_for_checkpoint(
        &self,
        checkpoint: &Checkpoint,
    ) -> Result<StoredGraphDefinition, StoreError> {
        let definition = match self
            .store
            .load_graph_definition(self.fence.tenant_id(), checkpoint.graph())
            .await
        {
            Ok(definition) => definition,
            Err(StoreError::GraphDefinitionNotFound) => {
                return Err(StoreError::corrupt("pinned graph definition"));
            }
            Err(error) => return Err(error),
        };
        let graph = definition.graph();
        if graph.reference() != *checkpoint.graph()
            || checkpoint.state().schema() != graph.state_schema()
            || checkpoint.ready_nodes().len() > usize::from(graph.limits().maximum_parallelism())
            || checkpoint
                .ready_nodes()
                .iter()
                .any(|node_id| graph.node(node_id).is_none())
            || (checkpoint.superstep() == Superstep::INITIAL
                && checkpoint.ready_nodes() != graph.entry_nodes())
        {
            return Err(StoreError::corrupt("pinned graph checkpoint binding"));
        }
        Ok(definition)
    }

    /// Reconstructs the exact current checkpoint ready set into one
    /// deterministic, database-time recovery plan.
    ///
    /// The provider loads the immutable checkpoint pinned when this session
    /// began, streams fully verified unconsumed results in bounded pages, then
    /// streams every ready activation's complete physical-attempt history.
    /// A final database snapshot revalidates the exact journal observation and
    /// live fence before classifying fresh work, crash takeover, delayed retry,
    /// same-fence in-flight work, terminal failure, attempt exhaustion, or
    /// barrier-ready reuse.
    ///
    /// The returned plan is not dispatch authority. Hand each selected node to
    /// [`PostgresStore::start_recovered_node_attempt`]; that API binds the plan
    /// to an exact worker append and repeats the decisive checkpoint, latest
    /// history transition, database-clock retry, journal, lifecycle, and
    /// live-fence checks.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ReadyNodeRecoveryCheckpointMissing`] before the
    /// run has an initial checkpoint. Cursor, availability, lifecycle, or
    /// fencing races are explicit and do not quarantine. Any durable
    /// checkpoint/result/attempt contradiction is converted to payload-redacted
    /// corruption and fenced quarantine before return.
    pub async fn plan_ready_nodes(&self) -> Result<ReadyNodeRecoveryPlan, StoreError> {
        Box::pin(self.read(self.plan_ready_nodes_inner())).await
    }

    async fn load_ready_node_checkpoint(
        &self,
        pointer: &CheckpointPointer,
    ) -> Result<Checkpoint, StoreError> {
        let checkpoint = self.load_recovery_checkpoint(pointer).await?;
        if checkpoint.ready_nodes().is_empty() {
            return Err(StoreError::corrupt("ready node recovery empty ready set"));
        }
        Ok(checkpoint)
    }

    async fn load_recovery_checkpoint(
        &self,
        pointer: &CheckpointPointer,
    ) -> Result<Checkpoint, StoreError> {
        let checkpoint = match self
            .store
            .load_checkpoint(
                self.fence.tenant_id(),
                self.fence.run_id(),
                pointer.checkpoint_id(),
            )
            .await
        {
            Ok(checkpoint) => checkpoint,
            Err(StoreError::CheckpointNotFound) => {
                return Err(StoreError::corrupt(
                    "ready node recovery checkpoint pointer",
                ));
            }
            Err(error) => return Err(error),
        };
        if checkpoint.superstep() != pointer.superstep() || checkpoint.digest() != pointer.digest()
        {
            return Err(StoreError::corrupt(
                "ready node recovery checkpoint pointer",
            ));
        }
        Ok(checkpoint)
    }

    async fn plan_ready_nodes_inner(&self) -> Result<ReadyNodeRecoveryPlan, StoreError> {
        let pointer = self
            .initial_run
            .checkpoint()
            .ok_or(StoreError::ReadyNodeRecoveryCheckpointMissing)?;
        let checkpoint = self.load_ready_node_checkpoint(pointer).await?;
        self.load_graph_for_checkpoint(&checkpoint).await?;

        let base = checkpoint.head();
        let mut planner = ReadyNodeRecoveryPlanner::new(checkpoint, self.fence.clone())
            .map_err(|_| StoreError::corrupt("ready node recovery activation"))?;
        let result_page_size = PendingNodeResultPageSize::new(PendingNodeResultPageSize::MAX)?;
        let mut result_cursor = None;
        loop {
            let page = self
                .store
                .load_unconsumed_pending_node_result_page(
                    &base,
                    result_cursor.as_ref(),
                    result_page_size,
                )
                .await?;
            if self.context.expectation().head() != Some(page.snapshot_journal_head()) {
                return Err(StoreError::StaleClaimedRunRecoveryObservation);
            }
            for result in page.records() {
                planner
                    .observe_result(result)
                    .map_err(|_| StoreError::corrupt("ready node recovery result set"))?;
            }
            if !page.has_more() {
                break;
            }
            result_cursor = Some(
                page.next_cursor()
                    .ok_or_else(|| StoreError::corrupt("ready node recovery result cursor"))?,
            );
        }

        let attempt_page_size = NodeAttemptHistoryPageSize::new(NodeAttemptHistoryPageSize::MAX)?;
        for activation in planner.activations() {
            let mut attempt_cursor = None;
            loop {
                let page = self
                    .store
                    .load_node_attempt_history_page(
                        &activation,
                        attempt_cursor.as_ref(),
                        attempt_page_size,
                    )
                    .await?;
                for attempt in page.records() {
                    planner
                        .observe_attempt(attempt)
                        .map_err(|_| StoreError::corrupt("ready node recovery attempt history"))?;
                }
                if !page.has_more() {
                    break;
                }
                attempt_cursor =
                    Some(page.next_cursor().ok_or_else(|| {
                        StoreError::corrupt("ready node recovery attempt cursor")
                    })?);
            }
        }

        let observation = self
            .store
            .load_claimed_run_recovery_snapshot(&self.fence, &self.context)
            .await?;
        if observation.run.checkpoint() != Some(pointer) {
            return Err(StoreError::corrupt(
                "ready node recovery checkpoint projection",
            ));
        }
        let journal_head = self
            .context
            .expectation()
            .head()
            .cloned()
            .ok_or_else(|| StoreError::corrupt("ready node recovery journal observation"))?;
        planner
            .finish(journal_head, observation.observed_at)
            .map_err(|_| StoreError::corrupt("ready node recovery plan"))
    }

    /// Loads one bounded current checkpoint-lineage page through quarantine.
    ///
    /// # Errors
    ///
    /// Returns the underlying cursor, durable-state, fencing-independent read,
    /// quarantine, or database failure. Corruption is quarantined first.
    pub async fn load_checkpoint_lineage_page(
        &self,
        from: Option<&CheckpointHead>,
        page_size: CheckpointLineagePageSize,
    ) -> Result<CheckpointLineagePage, StoreError> {
        self.read(self.store.load_checkpoint_lineage_page(
            self.fence.tenant_id(),
            self.fence.run_id(),
            from,
            page_size,
        ))
        .await
    }

    /// Loads one bounded journal page through quarantine.
    ///
    /// # Errors
    ///
    /// Returns the underlying cursor, durable-state, quarantine, or database
    /// failure. Corruption is quarantined first.
    pub async fn load_journal_page(
        &self,
        after: Option<&JournalHead>,
        page_size: JournalPageSize,
    ) -> Result<JournalPage, StoreError> {
        self.read(self.store.load_journal_page(
            self.fence.tenant_id(),
            self.fence.run_id(),
            after,
            page_size,
        ))
        .await
    }

    /// Loads one bounded tool-invocation history page through quarantine.
    ///
    /// # Errors
    ///
    /// Returns the underlying not-found, cursor, durable-state, quarantine, or
    /// database failure. Corruption is quarantined first.
    pub async fn load_tool_invocation_history_page(
        &self,
        invocation_id: InvocationId,
        after: Option<&ToolInvocation>,
        page_size: ToolInvocationHistoryPageSize,
    ) -> Result<ToolInvocationHistoryPage, StoreError> {
        Box::pin(self.read(self.store.load_tool_invocation_history_page(
            self.fence.tenant_id(),
            self.fence.run_id(),
            invocation_id,
            after,
            page_size,
        )))
        .await
    }

    /// Loads one bounded model-invocation history page through quarantine.
    ///
    /// # Errors
    ///
    /// Returns the underlying not-found, cursor, durable-state, quarantine, or
    /// database failure. Corruption is quarantined first.
    pub async fn load_model_invocation_history_page(
        &self,
        invocation_id: InvocationId,
        after: Option<&ModelInvocation>,
        page_size: ModelInvocationHistoryPageSize,
    ) -> Result<ModelInvocationHistoryPage, StoreError> {
        Box::pin(self.read(self.store.load_model_invocation_history_page(
            self.fence.tenant_id(),
            self.fence.run_id(),
            invocation_id,
            after,
            page_size,
        )))
        .await
    }

    /// Loads one bounded physical node-attempt history page through quarantine.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidClaimedRunRecoveryContext`] when the
    /// activation crosses this session, otherwise the underlying cursor,
    /// durable-state, quarantine, or database failure.
    pub async fn load_node_attempt_history_page(
        &self,
        activation: &NodeActivation,
        cursor: Option<&NodeAttempt>,
        page_size: NodeAttemptHistoryPageSize,
    ) -> Result<NodeAttemptHistoryPage, StoreError> {
        self.validate_activation_scope(activation)?;
        Box::pin(
            self.read(
                self.store
                    .load_node_attempt_history_page(activation, cursor, page_size),
            ),
        )
        .await
    }

    /// Loads one bounded page of unconsumed node results through quarantine.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidClaimedRunRecoveryContext`] when the base
    /// checkpoint crosses this session, otherwise the underlying cursor,
    /// durable-state, quarantine, or database failure.
    pub async fn load_unconsumed_pending_node_result_page(
        &self,
        base: &CheckpointHead,
        cursor: Option<&PendingNodeResultPageCursor>,
        page_size: PendingNodeResultPageSize,
    ) -> Result<PendingNodeResultPage, StoreError> {
        self.validate_checkpoint_scope(base)?;
        Box::pin(
            self.read(
                self.store
                    .load_unconsumed_pending_node_result_page(base, cursor, page_size),
            ),
        )
        .await
    }

    async fn read<T, F>(&self, recovery_read: F) -> Result<T, StoreError>
    where
        F: Future<Output = Result<T, StoreError>>,
    {
        self.store
            .with_corruption_quarantine(self.context.clone(), recovery_read)
            .await
    }

    fn validate_checkpoint_scope(&self, checkpoint: &CheckpointHead) -> Result<(), StoreError> {
        if checkpoint.tenant_id() != self.fence.tenant_id()
            || checkpoint.run_id() != self.fence.run_id()
        {
            return Err(StoreError::InvalidClaimedRunRecoveryContext);
        }
        Ok(())
    }

    fn validate_activation_scope(&self, activation: &NodeActivation) -> Result<(), StoreError> {
        self.validate_checkpoint_scope(activation.base_checkpoint())
    }
}

struct HistoricalGraphBarrierResults {
    results: Vec<PendingNodeResult>,
    compact_bytes: usize,
}

#[derive(Default)]
struct CompactByteCounter {
    bytes: usize,
}

impl Write for CompactByteCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.bytes = self.bytes.saturating_add(buffer.len());
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn map_graph_replay_plan_error(error: &GraphBarrierPlanError) -> StoreError {
    match error {
        GraphBarrierPlanError::ReducerReferenceMismatch
        | GraphBarrierPlanError::SchemaValidatorPanicked { .. }
        | GraphBarrierPlanError::ReducerPanicked
        | GraphBarrierPlanError::SchemaValidation {
            source: GraphSchemaValidationError::Unavailable,
            ..
        }
        | GraphBarrierPlanError::ReducerFailed {
            source: GraphReducerError::Unavailable | GraphReducerError::ResourceLimit,
        } => StoreError::GraphReplayDependencyUnavailable,
        _ => StoreError::corrupt("noninitial graph replay plan"),
    }
}

fn validate_graph_replay_checkpoint_state<V>(
    graph: &CompiledGraph,
    checkpoint: &Checkpoint,
    schemas: &V,
) -> Result<(), StoreError>
where
    V: GraphSchemaValidator + ?Sized,
{
    match catch_unwind(AssertUnwindSafe(|| {
        schemas.validate(graph.state_schema(), checkpoint.state().data())
    })) {
        Err(_) | Ok(Err(GraphSchemaValidationError::Unavailable)) => {
            Err(StoreError::GraphReplayDependencyUnavailable)
        }
        Ok(Err(_)) => Err(StoreError::corrupt(
            "noninitial graph replay checkpoint state",
        )),
        Ok(Ok(())) => Ok(()),
    }
}

fn map_graph_replay_consumption_error(error: StoreError) -> StoreError {
    match error {
        StoreError::Database { .. } | StoreError::CorruptData { .. } => error,
        _ => StoreError::corrupt("noninitial graph replay consumption"),
    }
}

impl PostgresStore {
    /// Returns the validated immutable provider options used by this pool.
    #[must_use]
    pub const fn options(&self) -> &PostgresStoreOptions {
        &self.options
    }

    /// Connects to a qualified `PostgreSQL` 16 or 17 server and verifies its schema.
    ///
    /// Run [`Self::migrate_database`] with a deployment-authorized role before
    /// connecting an application runtime role. The connection URL is never
    /// retained in a public error.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid configuration, connection failure, an
    /// unqualified server version, or missing/incompatible migration state.
    pub async fn connect(
        database_url: &str,
        options: PostgresStoreOptions,
    ) -> Result<Self, StoreError> {
        let store = Self::connect_server(database_url, options).await?;
        if let Err(error) = store.verify_schema().await {
            store.close().await;
            return Err(error);
        }
        Ok(store)
    }

    /// Applies embedded, ordered, checksum-pinned migrations using a dedicated pool.
    ///
    /// `SQLx` serializes migration runners with the `PostgreSQL` migration lock and
    /// refuses changed checksums or database versions missing from this binary.
    /// The temporary pool is closed before this method returns so DDL credentials
    /// do not leak into the runtime connection pool.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] if connection, migration, or post-migration schema
    /// verification fails.
    pub async fn migrate_database(
        database_url: &str,
        options: PostgresStoreOptions,
    ) -> Result<(), StoreError> {
        let store = Self::connect_server(database_url, options).await?;
        let result = async {
            MIGRATOR
                .run(&store.pool)
                .await
                .map_err(|source| StoreError::Migration { source })?;
            store.verify_schema().await
        }
        .await;
        store.close().await;
        result
    }

    /// Verifies exact migration versions/checksums and required schema objects.
    ///
    /// Runtime database roles need read access to `public._sqlx_migrations` in
    /// addition to their least-privilege `StateKnot` table permissions.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::SchemaNotMigrated`],
    /// [`StoreError::IncompatibleSchema`], [`StoreError::IncompleteSchema`], or
    /// a database error.
    pub async fn verify_schema(&self) -> Result<(), StoreError> {
        let applied = match query_as::<_, (i64, bool, Vec<u8>)>(
            "SELECT version, success, checksum \
             FROM public._sqlx_migrations ORDER BY version",
        )
        .fetch_all(&self.pool)
        .await
        {
            Ok(applied) => applied,
            Err(source) if has_database_error_code(&source, "42P01") => {
                return Err(StoreError::SchemaNotMigrated);
            }
            Err(source) => return Err(StoreError::database("schema version check", source)),
        };
        if applied.is_empty() {
            return Err(StoreError::SchemaNotMigrated);
        }
        if applied.len() != MIGRATOR.iter().len()
            || applied.iter().zip(MIGRATOR.iter()).any(
                |((version, success, checksum), migration)| {
                    !success
                        || *version != migration.version
                        || checksum.as_slice() != migration.checksum.as_ref()
                },
            )
        {
            return Err(StoreError::IncompatibleSchema);
        }

        let complete = query_scalar::<_, bool>(VERIFY_SCHEMA_OBJECTS)
            .fetch_one(&self.pool)
            .await
            .map_err(|source| StoreError::database("schema object check", source))?;
        if !complete {
            return Err(StoreError::IncompleteSchema);
        }
        Ok(())
    }

    async fn connect_server(
        database_url: &str,
        options: PostgresStoreOptions,
    ) -> Result<Self, StoreError> {
        let connect_options = options.connect_options(database_url)?;
        let pool = options
            .pool_options()
            .connect_with(connect_options)
            .await
            .map_err(|source| StoreError::database("connect", source))?;

        let version =
            match query_scalar::<_, i32>("SELECT current_setting('server_version_num')::integer")
                .fetch_one(&pool)
                .await
            {
                Ok(version) => version,
                Err(source) => {
                    pool.close().await;
                    return Err(StoreError::database("server version check", source));
                }
            };
        if !(MIN_POSTGRES_VERSION_NUMBER..=MAX_POSTGRES_VERSION_NUMBER).contains(&version) {
            pool.close().await;
            return Err(StoreError::UnsupportedServerVersion);
        }

        Ok(Self { pool, options })
    }

    /// Performs a bounded pool acquisition and database round trip.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the database is unavailable.
    pub async fn health_check(&self) -> Result<(), StoreError> {
        query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|source| StoreError::database("health check", source))?;
        Ok(())
    }

    /// Gracefully closes the shared connection pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    /// Idempotently registers one immutable compiled graph version.
    ///
    /// The tenant boundary is independent from the graph's authenticated owner
    /// identity. Once an owner/name/version tuple commits in that tenant, every
    /// schema, reducer, node, route, limit, and digest is immutable; publish a
    /// new semantic version for any change.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::GraphDefinitionConflict`] when the graph identity
    /// already owns different canonical bytes, or an encoding, corruption, or
    /// database error.
    pub async fn register_graph_definition(
        &self,
        tenant_id: TenantId,
        graph: CompiledGraph,
    ) -> Result<GraphDefinitionRegistrationOutcome, StoreError> {
        let definition_bytes = encode_graph_definition(&graph)?;
        let identity = graph.identity();
        let owner = identity.owner();
        let version = identity.version().to_string();
        let reference = graph.reference();
        let mut transaction = self.begin_mutation("graph definition registration").await?;
        let registered_at = database_now(&mut transaction, "graph definition clock").await?;
        // Both the immutable identity and its exact-reference projection are
        // unique. Under speculative insertion either index can arbitrate first,
        // so every unique conflict must converge on the verified read below.
        let inserted = query(
            r"
INSERT INTO stateknot.graph_definitions (
    tenant_id,
    owner_issuer,
    owner_subject,
    graph_name,
    graph_version,
    definition_digest,
    definition_bytes,
    registered_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT DO NOTHING
",
        )
        .bind(tenant_id.as_str())
        .bind(owner.issuer().as_str())
        .bind(owner.subject().as_str())
        .bind(identity.name().as_str())
        .bind(&version)
        .bind(graph.definition_digest().as_bytes())
        .bind(&definition_bytes)
        .bind(to_database_time(registered_at)?)
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("graph definition insert", source))?
        .rows_affected();

        let row = load_graph_definition_row(&mut transaction, &tenant_id, &reference)
            .await?
            .ok_or(StoreError::GraphDefinitionConflict)?;
        let stored = decode_graph_definition(row)?;
        if stored.tenant_id() != &tenant_id || stored.graph() != &graph {
            return Err(StoreError::GraphDefinitionConflict);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("graph definition commit", source))?;
        Ok(if inserted == 1 {
            GraphDefinitionRegistrationOutcome::Registered(stored)
        } else {
            GraphDefinitionRegistrationOutcome::Idempotent(stored)
        })
    }

    /// Loads one exact graph reference from a tenant registry.
    ///
    /// Canonical bytes are deserialized through the compiler again and checked
    /// against every redundant key column before the definition is returned.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::GraphDefinitionNotFound`] when no exact identity,
    /// digest, and state-schema binding exists, or a corruption/database error.
    pub async fn load_graph_definition(
        &self,
        tenant_id: &TenantId,
        reference: &GraphReference,
    ) -> Result<StoredGraphDefinition, StoreError> {
        let mut transaction = self.begin_repeatable_read("graph definition load").await?;
        let row = load_graph_definition_row(&mut transaction, tenant_id, reference)
            .await?
            .ok_or(StoreError::GraphDefinitionNotFound)?;
        let stored = decode_graph_definition(row)?;
        if stored.tenant_id() != tenant_id || stored.graph().reference() != *reference {
            return Err(StoreError::GraphDefinitionNotFound);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("graph definition load commit", source))?;
        Ok(stored)
    }

    /// Idempotently registers one immutable distributed fairness schedule.
    ///
    /// A shard identity permanently binds canonical policy bytes, checksum, and
    /// cycle length. Deployments publish another shard identity to change
    /// weights, preventing replicas with mixed configuration from sharing a
    /// cursor accidentally.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::SchedulerFairnessPolicyConflict`] when the shard
    /// already owns different bytes, or a corruption/database failure.
    pub async fn register_scheduler_fairness_policy(
        &self,
        registration: SchedulerFairnessPolicyRegistration,
    ) -> Result<SchedulerFairnessPolicyRegistrationOutcome, StoreError> {
        let mut transaction = self
            .begin_mutation("scheduler fairness policy registration")
            .await?;
        let registered_at =
            database_now(&mut transaction, "scheduler fairness policy clock").await?;
        let inserted = query(
            r"
INSERT INTO stateknot.scheduler_fairness_shards (
    shard_id,
    policy_digest,
    policy_bytes,
    cycle_length,
    registered_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, $5)
ON CONFLICT (shard_id) DO NOTHING
",
        )
        .bind(registration.shard_id().as_str())
        .bind(registration.policy_digest().as_bytes())
        .bind(registration.policy_bytes())
        .bind(i32::from(registration.cycle_length()))
        .bind(to_database_time(registered_at)?)
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("scheduler fairness policy insert", source))?
        .rows_affected();

        let row = query_as::<_, SchedulerFairnessShardRow>(SELECT_SCHEDULER_FAIRNESS_SHARD)
            .bind(registration.shard_id().as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("scheduler fairness policy lookup", source))?
            .ok_or(StoreError::SchedulerFairnessPolicyConflict)?;
        let stored = decode_scheduler_fairness_policy(row)?;
        if stored.registration() != &registration {
            return Err(StoreError::SchedulerFairnessPolicyConflict);
        }
        transaction.commit().await.map_err(|source| {
            StoreError::database("scheduler fairness policy registration commit", source)
        })?;
        Ok(if inserted == 1 {
            SchedulerFairnessPolicyRegistrationOutcome::Registered(stored)
        } else {
            SchedulerFairnessPolicyRegistrationOutcome::Idempotent(stored)
        })
    }

    /// Loads and verifies one immutable distributed fairness policy.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::SchedulerFairnessPolicyNotFound`], a corruption
    /// failure, or a database error.
    pub async fn load_scheduler_fairness_policy(
        &self,
        shard_id: &SchedulerShardId,
    ) -> Result<StoredSchedulerFairnessPolicy, StoreError> {
        let mut transaction = self
            .begin_repeatable_read("scheduler fairness policy load")
            .await?;
        let row = query_as::<_, SchedulerFairnessShardRow>(SELECT_SCHEDULER_FAIRNESS_SHARD)
            .bind(shard_id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("scheduler fairness policy load", source))?
            .ok_or(StoreError::SchedulerFairnessPolicyNotFound)?;
        let stored = decode_scheduler_fairness_policy(row)?;
        if stored.registration().shard_id() != shard_id {
            return Err(StoreError::corrupt("scheduler fairness policy scope"));
        }
        transaction.commit().await.map_err(|source| {
            StoreError::database("scheduler fairness policy load commit", source)
        })?;
        Ok(stored)
    }

    /// Atomically reserves the next shard-global weighted schedule slot.
    ///
    /// `reservation_id` must be allocated once before the database call and
    /// retained across every retry. An ambiguous successful commit is recovered
    /// from the immutable reservation row without advancing the cursor again.
    /// The transaction never spans queue scanning, lease claiming, or run work.
    ///
    /// # Errors
    ///
    /// Returns explicit policy-not-found, policy/reservation conflict, sequence
    /// exhaustion, corruption, or database failures.
    pub async fn reserve_scheduler_fairness_slot(
        &self,
        shard_id: &SchedulerShardId,
        policy_digest: Digest,
        reservation_id: SchedulerReservationId,
    ) -> Result<SchedulerFairnessReservation, StoreError> {
        let mut transaction = self
            .begin_mutation("scheduler fairness slot reservation")
            .await?;

        if let Some(reservation) = load_valid_scheduler_fairness_reservation(
            &mut transaction,
            shard_id,
            policy_digest,
            reservation_id,
        )
        .await?
        {
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent scheduler fairness reservation commit", source)
            })?;
            return Ok(reservation);
        }

        let shard_row =
            query_as::<_, SchedulerFairnessShardRow>(SELECT_SCHEDULER_FAIRNESS_SHARD_FOR_UPDATE)
                .bind(shard_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("scheduler fairness cursor lock", source))?
                .ok_or(StoreError::SchedulerFairnessPolicyNotFound)?;

        // A same-shard contender may have committed while this transaction was
        // waiting for the cursor lock. Recheck the stable id before advancing.
        if let Some(reservation) = load_valid_scheduler_fairness_reservation(
            &mut transaction,
            shard_id,
            policy_digest,
            reservation_id,
        )
        .await?
        {
            transaction.commit().await.map_err(|source| {
                StoreError::database("raced scheduler fairness reservation commit", source)
            })?;
            return Ok(reservation);
        }

        let reservation = insert_scheduler_fairness_reservation(
            &mut transaction,
            shard_id,
            policy_digest,
            reservation_id,
            shard_row,
        )
        .await?;
        transaction.commit().await.map_err(|source| {
            StoreError::database("scheduler fairness reservation commit", source)
        })?;
        Ok(reservation)
    }

    /// Deletes one bounded batch of expired fairness reservation evidence.
    ///
    /// The cutoff is derived from the authoritative database clock. Candidates
    /// are ordered by the retention index and locked with `SKIP LOCKED`, so
    /// multiple maintenance workers can cooperate without blocking scheduler
    /// reservations or each other. Shard policies and cursor positions are
    /// never modified.
    ///
    /// A reservation handoff older than the configured retention window must
    /// be treated as expired by its owner and must not be retried after this
    /// method can delete it.
    ///
    /// # Errors
    ///
    /// Returns a database or integrity failure. Policy construction already
    /// enforces the one-hour safety floor and bounded transaction size.
    pub async fn prune_scheduler_fairness_reservations(
        &self,
        policy: SchedulerFairnessRetentionPolicy,
    ) -> Result<SchedulerFairnessRetentionReport, StoreError> {
        let retention_micros = i64::try_from(policy.retain_for().as_micros())
            .map_err(|_| StoreError::InvalidSchedulerFairnessRetention)?;
        let mut transaction = self
            .begin_mutation("scheduler fairness reservation retention")
            .await?;
        let (observed_at, cutoff) = query_as::<_, (DateTime<Utc>, DateTime<Utc>)>(
            r"
WITH observed AS MATERIALIZED (
    SELECT clock_timestamp() AS observed_at
)
SELECT
    observed_at,
    observed_at - ($1::bigint * interval '1 microsecond') AS cutoff
FROM observed
",
        )
        .bind(retention_micros)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("scheduler fairness retention clock", source))?;
        let deleted = query(
            r"
WITH candidates AS MATERIALIZED (
    SELECT reservation_id
    FROM stateknot.scheduler_fairness_reservations
    WHERE reserved_at < $1
    ORDER BY reserved_at, shard_id, sequence
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
DELETE FROM stateknot.scheduler_fairness_reservations AS reservation
USING candidates
WHERE reservation.reservation_id = candidates.reservation_id
",
        )
        .bind(cutoff)
        .bind(i64::from(policy.batch_size()))
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("scheduler fairness retention delete", source))?
        .rows_affected();
        let deleted = u16::try_from(deleted)
            .map_err(|_| StoreError::corrupt("scheduler fairness retention count"))?;
        let observed_at = from_database_time(observed_at)?;
        let cutoff = from_database_time(cutoff)?;
        if cutoff >= observed_at || deleted > policy.batch_size() {
            return Err(StoreError::corrupt(
                "scheduler fairness retention projection",
            ));
        }
        transaction.commit().await.map_err(|source| {
            StoreError::database("scheduler fairness retention commit", source)
        })?;
        Ok(SchedulerFairnessRetentionReport {
            observed_at,
            cutoff,
            deleted,
        })
    }

    /// Idempotently creates a low-level, uninitialized pending run using a
    /// database commit timestamp.
    ///
    /// A retry with the same tenant/run identity succeeds only when the durable
    /// admission provenance is identical. If the run has progressed, its current
    /// validated lifecycle is returned. Uninitialized rows are deliberately
    /// absent from scheduler discovery; only an explicit trusted low-level ID
    /// path can address them. New Agent APIs should use
    /// [`Self::admit_agent_run`] so admission and initialization cannot be
    /// observed separately.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for identity conflict, corruption, or database failure.
    pub async fn admit_run(
        &self,
        provenance: AgentResultProvenance,
    ) -> Result<AdmissionOutcome, StoreError> {
        let tenant_id = provenance.tenant_id().clone();
        let run_id = provenance.run_id();
        let thread_id = provenance.thread_id();
        let invocation_id = provenance.invocation_id();

        let mut transaction = self.begin_mutation("run admission").await?;
        let observed_at = database_now(&mut transaction, "run admission clock").await?;
        let lifecycle = RunLifecycle::admitted(provenance, observed_at);
        let lifecycle_bytes = encode_lifecycle(&lifecycle)?;
        let revision = lifecycle.revision().to_string();
        let status = run_status_text(lifecycle.status());
        let observed_db = to_database_time(observed_at)?;

        let inserted = query(
            r"
INSERT INTO stateknot.runs (
    tenant_id,
    run_id,
    thread_id,
    invocation_id,
    lifecycle_bytes,
    lifecycle_revision,
    lifecycle_status,
    admitted_at,
    changed_at,
    scheduler_ready_at
)
VALUES ($1, $2, $3, $4, $5, $6::numeric, $7, $8, $8, $8)
ON CONFLICT (tenant_id, run_id) DO NOTHING
",
        )
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(*thread_id.as_uuid())
        .bind(*invocation_id.as_uuid())
        .bind(&lifecycle_bytes)
        .bind(&revision)
        .bind(status)
        .bind(observed_db)
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("run admission insert", source))?
        .rows_affected();

        if inserted == 1 {
            transaction
                .commit()
                .await
                .map_err(|source| StoreError::database("run admission commit", source))?;
            return Ok(AdmissionOutcome::Committed(lifecycle));
        }

        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let existing = decode_run(row)?;
        if existing.lifecycle().provenance() != lifecycle.provenance() {
            return Err(StoreError::RunConflict);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("idempotent run admission commit", source))?;
        Ok(AdmissionOutcome::Idempotent(existing.lifecycle))
    }

    /// Atomically admits and initializes one executable Agent run.
    ///
    /// The immutable admission snapshot, pending-to-active lifecycle edge,
    /// sequence-one control-plane event, superstep-zero checkpoint, registered
    /// graph reference, and scheduler-visible run heads commit in one database
    /// transaction. The schema validator is invoked only outside the mutation
    /// transaction and must resolve a pre-registered digest-pinned local schema.
    ///
    /// An exact retry first recovers durable evidence and therefore succeeds
    /// even when its original deadline has passed after an ambiguous commit.
    /// The stable tenant/run key is serialized across scheduler replicas with a
    /// transaction-scoped advisory lock; hash collisions can delay unrelated
    /// admissions but cannot weaken correctness.
    ///
    /// Request and authorization-evidence schemas are trusted control-plane
    /// preconditions captured by [`AgentAdmissionIntent`]. This provider
    /// independently validates the registered graph, exact entry ready-set and
    /// initial state schema before committing.
    ///
    /// # Errors
    ///
    /// Rejects crossed scope, non-empty journals, worker authority, unregistered
    /// graphs, invalid initial state/ready sets, database-time budget expiry,
    /// identity conflicts, corruption, or database failures.
    pub async fn admit_agent_run<V>(
        &self,
        intent: AgentAdmissionIntent,
        append: JournalAppend,
        checkpoint_write: CheckpointWrite,
        schemas: &V,
    ) -> Result<AgentAdmissionCommitOutcome, StoreError>
    where
        V: GraphSchemaValidator + ?Sized,
    {
        validate_agent_admission_commit_input(&intent, &append, &checkpoint_write)?;
        let tenant_id = intent.provenance().tenant_id().clone();
        let run_id = intent.provenance().run_id();

        // Durable evidence wins over all time-sensitive revalidation. This is
        // the lost-acknowledgement path and intentionally runs before schemas or
        // the current database clock are consulted.
        let mut probe = self
            .begin_mutation("agent admission idempotency probe")
            .await?;
        if let Some(stored) = load_locked_agent_admission(&mut probe, &tenant_id, run_id).await? {
            validate_agent_admission_retry(&stored, &intent, &append, &checkpoint_write)?;
            probe.commit().await.map_err(|source| {
                StoreError::database("agent admission idempotency probe commit", source)
            })?;
            return Ok(AgentAdmissionCommitOutcome::Idempotent(stored));
        }
        probe.commit().await.map_err(|source| {
            StoreError::database("agent admission idempotency probe commit", source)
        })?;

        let graph = self
            .load_graph_definition(&tenant_id, intent.graph())
            .await?;
        validate_agent_initial_checkpoint(graph.graph(), &checkpoint_write, schemas)?;

        let mut transaction = self.begin_mutation("atomic agent admission").await?;
        if let Some(stored) =
            load_locked_agent_admission(&mut transaction, &tenant_id, run_id).await?
        {
            validate_agent_admission_retry(&stored, &intent, &append, &checkpoint_write)?;
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent atomic agent admission commit", source)
            })?;
            return Ok(AgentAdmissionCommitOutcome::Idempotent(stored));
        }

        let outcome = Box::pin(commit_new_agent_admission(
            &mut transaction,
            intent,
            append,
            checkpoint_write,
        ))
        .await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("atomic agent admission commit", source))?;
        Ok(match outcome {
            NewAgentAdmissionOutcome::Committed(stored) => {
                AgentAdmissionCommitOutcome::Committed(stored)
            }
            NewAgentAdmissionOutcome::Idempotent(stored) => {
                AgentAdmissionCommitOutcome::Idempotent(stored)
            }
        })
    }

    /// Resolves one tenant-scoped ingress key to exactly one atomic Agent run.
    ///
    /// The raw key is hashed with its tenant boundary before storage. A
    /// domain-separated submission digest binds every caller-controlled Agent,
    /// request, budget, graph, authority, initial-state, and initial-ready-set
    /// field while deliberately excluding the framework-generated provenance,
    /// admission-audit event, and checkpoint identities. Consequently, a retry
    /// after an ambiguous response may generate a fresh candidate identity
    /// bundle and still recover the original run. Reusing the key for different
    /// logical content fails closed.
    ///
    /// New mappings serialize under a transaction-scoped advisory lock and are
    /// inserted in the same transaction as a new Agent admission. No mapping
    /// can reference a missing or different admission because migration 16
    /// enforces a composite foreign key.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::AgentSubmissionConflict`] for key reuse with
    /// different content, plus the same admission, schema, integrity, and
    /// database errors as [`Self::admit_agent_run`].
    pub async fn submit_agent_run<V>(
        &self,
        key: &AgentSubmissionKey,
        intent: AgentAdmissionIntent,
        append: JournalAppend,
        checkpoint_write: CheckpointWrite,
        schemas: &V,
    ) -> Result<AgentSubmissionCommitOutcome, StoreError>
    where
        V: GraphSchemaValidator + ?Sized,
    {
        validate_agent_admission_commit_input(&intent, &append, &checkpoint_write)?;
        let tenant_id = intent.provenance().tenant_id().clone();
        let run_id = intent.provenance().run_id();
        let key_digest = key.digest_for(&tenant_id);
        let submission_digest = agent_submission_digest(
            &intent,
            checkpoint_write.state(),
            checkpoint_write.ready_nodes(),
        )?;

        // Durable key evidence wins before time-sensitive graph/schema checks.
        let mut probe = self
            .begin_mutation("agent submission idempotency probe")
            .await?;
        if let Some(stored) =
            load_locked_agent_submission(&mut probe, &tenant_id, key_digest).await?
        {
            if stored.submission_digest() != submission_digest {
                return Err(StoreError::AgentSubmissionConflict);
            }
            probe.commit().await.map_err(|source| {
                StoreError::database("agent submission idempotency probe commit", source)
            })?;
            return Ok(AgentSubmissionCommitOutcome::Idempotent(stored));
        }
        probe.commit().await.map_err(|source| {
            StoreError::database("agent submission idempotency probe commit", source)
        })?;

        let graph = self
            .load_graph_definition(&tenant_id, intent.graph())
            .await?;
        validate_agent_initial_checkpoint(graph.graph(), &checkpoint_write, schemas)?;

        let mut transaction = self.begin_mutation("atomic agent submission").await?;
        if let Some(stored) =
            load_locked_agent_submission(&mut transaction, &tenant_id, key_digest).await?
        {
            if stored.submission_digest() != submission_digest {
                return Err(StoreError::AgentSubmissionConflict);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent atomic agent submission commit", source)
            })?;
            return Ok(AgentSubmissionCommitOutcome::Idempotent(stored));
        }

        let admission = if let Some(stored) =
            load_locked_agent_admission(&mut transaction, &tenant_id, run_id).await?
        {
            validate_agent_admission_retry(&stored, &intent, &append, &checkpoint_write)?;
            stored
        } else {
            match Box::pin(commit_new_agent_admission(
                &mut transaction,
                intent,
                append,
                checkpoint_write,
            ))
            .await?
            {
                NewAgentAdmissionOutcome::Committed(stored)
                | NewAgentAdmissionOutcome::Idempotent(stored) => stored,
            }
        };
        insert_agent_submission(&mut transaction, key_digest, submission_digest, &admission)
            .await?;
        let row = load_agent_submission_row(&mut transaction, &tenant_id, key_digest)
            .await?
            .ok_or_else(|| StoreError::corrupt("agent submission committed row"))?;
        let stored = verify_agent_submission(&row, key_digest, admission)?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("atomic agent submission commit", source))?;
        Ok(AgentSubmissionCommitOutcome::Committed(stored))
    }

    /// Loads one durable ingress-key mapping and its current Agent run.
    ///
    /// The raw key is used only to derive its tenant-scoped digest. Mapping,
    /// admission, initial event/checkpoint, graph definition, current lifecycle,
    /// and redundant digests are verified inside one repeatable-read snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::AgentSubmissionNotFound`], a payload-redacted
    /// integrity failure, or a database error.
    pub async fn load_agent_submission(
        &self,
        tenant_id: &TenantId,
        key: &AgentSubmissionKey,
    ) -> Result<StoredAgentSubmission, StoreError> {
        let key_digest = key.digest_for(tenant_id);
        let mut transaction = self.begin_repeatable_read("agent submission load").await?;
        let row = load_agent_submission_row(&mut transaction, tenant_id, key_digest)
            .await?
            .ok_or(StoreError::AgentSubmissionNotFound)?;
        let run_id = RunId::from_uuid(row.run_id)
            .map_err(|_| StoreError::corrupt("agent submission run identity"))?;
        let run_row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("agent submission run load", source))?
            .ok_or_else(|| StoreError::corrupt("agent submission run reference"))?;
        let run = decode_run(run_row)?;
        verify_current_wait_set(&mut transaction, &run).await?;
        let admission_row = load_agent_admission_row(&mut transaction, tenant_id, run_id)
            .await?
            .ok_or_else(|| StoreError::corrupt("agent submission admission reference"))?;
        let admission = verify_stored_agent_admission(&mut transaction, run, admission_row).await?;
        let stored = verify_agent_submission(&row, key_digest, admission)?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("agent submission load commit", source))?;
        Ok(stored)
    }

    /// Loads and fully revalidates one immutable Agent admission snapshot.
    ///
    /// The returned value includes the current run plus its immutable initial
    /// event/checkpoint anchors. Canonical bytes, redundant columns, graph
    /// registry bytes, projection digest, foreign-key identities, and current
    /// wait-set projection are all checked inside one repeatable-read snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::AgentAdmissionNotFound`], an integrity failure, or
    /// a database error.
    pub async fn load_agent_admission(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
    ) -> Result<StoredAgentAdmission, StoreError> {
        let mut transaction = self.begin_repeatable_read("agent admission load").await?;
        let run_row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("agent admission run load", source))?
            .ok_or(StoreError::AgentAdmissionNotFound)?;
        let run = decode_run(run_row)?;
        verify_current_wait_set(&mut transaction, &run).await?;
        let admission_row = load_agent_admission_row(&mut transaction, tenant_id, run_id)
            .await?
            .ok_or(StoreError::AgentAdmissionNotFound)?;
        let stored = verify_stored_agent_admission(&mut transaction, run, admission_row).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("agent admission load commit", source))?;
        Ok(stored)
    }

    /// Loads and validates one tenant-scoped run snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::RunNotFound`], a corruption failure, or a database error.
    pub async fn load_run(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
    ) -> Result<StoredRun, StoreError> {
        let mut transaction = self.begin_repeatable_read("run load").await?;
        let row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("run load", source))?
            .ok_or(StoreError::RunNotFound)?;
        let stored = decode_run(row)?;
        if stored.lifecycle().provenance().tenant_id() != tenant_id
            || stored.lifecycle().provenance().run_id() != run_id
        {
            return Err(StoreError::corrupt("run scope"));
        }
        verify_current_wait_set(&mut transaction, &stored).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("run load commit", source))?;
        Ok(stored)
    }

    /// Atomically records integrity-safe evidence and removes a run from execution.
    ///
    /// Quarantine facts are stored outside the run journal because that journal
    /// may be the object that failed verification. The exact journal expectation
    /// prevents a stale observation from quarantining a run after durable
    /// progress. A request carrying an expected fence additionally requires that
    /// exact attempt and epoch to own an unexpired lease in this transaction, so
    /// a stale recovery worker cannot quarantine its successor. A committed
    /// quarantine clears the active lease and is excluded from the runnable
    /// index in the same transaction. Losing the commit acknowledgement is
    /// recovered by retrying the identical `quarantine_id` and request.
    ///
    /// # Errors
    ///
    /// Returns explicit not-found, stale-observation, identity/conflict,
    /// corruption, or database failures. A run quarantined before migration 10
    /// is preserved but cannot acquire fabricated audit evidence through this
    /// API.
    pub async fn quarantine_run(
        &self,
        request: RunQuarantineRequest,
    ) -> Result<RunQuarantineCommitOutcome, StoreError> {
        let mut transaction = self.begin_mutation("run quarantine").await?;

        if let Some(row) = load_run_quarantine_row_by_id(
            &mut transaction,
            request.tenant_id(),
            request.quarantine_id(),
        )
        .await?
        {
            let quarantine = decode_run_quarantine(&row)?;
            if quarantine.request() != &request {
                return Err(StoreError::RunQuarantineIdConflict);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent run quarantine commit", source)
            })?;
            return Ok(RunQuarantineCommitOutcome::Idempotent(quarantine));
        }

        let target = query_as::<_, RunQuarantineTargetRow>(SELECT_RUN_QUARANTINE_TARGET_FOR_UPDATE)
            .bind(request.tenant_id().as_str())
            .bind(*request.run_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("run quarantine target lock", source))?
            .ok_or(StoreError::RunNotFound)?;

        if target.quarantined_at.is_some() || target.quarantine_reason.is_some() {
            if let Some(row) = load_run_quarantine_row_by_id(
                &mut transaction,
                request.tenant_id(),
                request.quarantine_id(),
            )
            .await?
            {
                let quarantine = decode_run_quarantine(&row)?;
                if quarantine.request() == &request {
                    transaction.commit().await.map_err(|source| {
                        StoreError::database("concurrent run quarantine commit", source)
                    })?;
                    return Ok(RunQuarantineCommitOutcome::Idempotent(quarantine));
                }
                return Err(StoreError::RunQuarantineIdConflict);
            }
            return Err(StoreError::RunQuarantineConflict);
        }
        let observed_at = database_now(&mut transaction, "run quarantine clock").await?;
        authorize_quarantine_fence(&target, request.expected_fence(), observed_at)?;
        if !quarantine_expectation_matches(&target, request.expectation())? {
            return Err(StoreError::StaleRunQuarantineObservation);
        }

        let quarantine = materialize_run_quarantine(request, observed_at)?;
        insert_run_quarantine(&mut transaction, &quarantine).await?;
        commit_run_quarantine_projection(&mut transaction, &quarantine).await?;

        let restored_row = load_run_quarantine_row_by_run(
            &mut transaction,
            quarantine.request().tenant_id(),
            quarantine.request().run_id(),
        )
        .await?
        .ok_or(StoreError::RunQuarantineCommitConflict)?;
        let restored = decode_run_quarantine(&restored_row)?;
        if restored != quarantine {
            return Err(StoreError::RunQuarantineCommitConflict);
        }

        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("run quarantine commit", source))?;
        Ok(RunQuarantineCommitOutcome::Committed(restored))
    }

    /// Runs one read-only recovery validation and quarantines only corruption.
    ///
    /// The supplied future must not perform external side effects. It completes
    /// before a separate quarantine transaction begins. Successful values and
    /// non-corruption failures are returned unchanged. For
    /// [`StoreError::CorruptData`], the provider derives the bounded component
    /// code, commits (or exactly recovers) the quarantine, then returns
    /// [`StoreError::RunQuarantined`]. A stale journal/fence observation,
    /// expired lease, or unavailable database is returned instead of claiming
    /// that isolation committed.
    ///
    /// This helper does not grant execution authority: recovery still needs a
    /// live run fence and must re-check the quarantined run projection before
    /// dispatch.
    ///
    /// # Errors
    ///
    /// Returns the recovery read error, or an explicit quarantine transaction
    /// failure. Raw corrupt payloads are never copied into the quarantine row.
    pub async fn with_corruption_quarantine<T, F>(
        &self,
        context: CorruptionQuarantineContext,
        recovery_read: F,
    ) -> Result<T, StoreError>
    where
        F: Future<Output = Result<T, StoreError>>,
    {
        match recovery_read.await {
            Ok(value) => Ok(value),
            Err(error) => {
                let component = match RunQuarantineComponent::from_corrupt_store_error(&error) {
                    Ok(component) => component,
                    Err(StoreError::InvalidRunQuarantineRequest) => return Err(error),
                    Err(component_error) => return Err(component_error),
                };
                let request = context.into_request(component)?;
                self.quarantine_run(request).await?;
                Err(StoreError::RunQuarantined)
            }
        }
    }

    /// Starts one fence-bound recovery session for a claimed runnable run.
    ///
    /// The supplied fence and quarantine context must name the same tenant/run.
    /// In one repeatable-read transaction the provider verifies the complete
    /// run projection and wait set, exact live lease against the database clock,
    /// runnable/quarantine state, and the context's exact journal observation.
    /// The returned session then supplies only scope-bound recovery reads that
    /// automatically use the same stable corruption quarantine intent.
    ///
    /// Creating a session does not authorize external dispatch. Recovered work
    /// must still enter the corresponding durable-before-dispatch start API
    /// under the exact fence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidClaimedRunRecoveryContext`] for crossed
    /// scope, [`StoreError::StaleClaimedRunRecoveryObservation`] for journal
    /// progress, or explicit lifecycle, fencing, corruption/quarantine, and
    /// database failures. Corruption is quarantined before this method returns.
    pub async fn begin_claimed_run_recovery(
        &self,
        fence: RunFence,
        context: CorruptionQuarantineContext,
    ) -> Result<ClaimedRunRecovery<'_>, StoreError> {
        if context.tenant_id() != fence.tenant_id() || context.run_id() != fence.run_id() {
            return Err(StoreError::InvalidClaimedRunRecoveryContext);
        }
        let context = context.with_expected_fence(fence.clone())?;
        let observation = self
            .with_corruption_quarantine(
                context.clone(),
                self.load_claimed_run_recovery_snapshot(&fence, &context),
            )
            .await?;
        Ok(ClaimedRunRecovery {
            store: self,
            fence,
            context,
            initial_run: observation.run,
            initial_observed_at: observation.observed_at,
        })
    }

    async fn load_claimed_run_recovery_snapshot(
        &self,
        fence: &RunFence,
        context: &CorruptionQuarantineContext,
    ) -> Result<ClaimedRunRecoveryObservation, StoreError> {
        let mut transaction = self.begin_repeatable_read("claimed run recovery").await?;
        let row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(fence.tenant_id().as_str())
            .bind(*fence.run_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("claimed run recovery load", source))?
            .ok_or(StoreError::RunNotFound)?;
        let stored = decode_run(row)?;
        if stored.lifecycle().provenance().tenant_id() != fence.tenant_id()
            || stored.lifecycle().provenance().run_id() != fence.run_id()
        {
            return Err(StoreError::corrupt("claimed run recovery scope"));
        }
        verify_current_wait_set(&mut transaction, &stored).await?;
        validate_runnable(&stored)?;
        if !journal_expectation_matches_stored(&stored, context.expectation()) {
            return Err(StoreError::StaleClaimedRunRecoveryObservation);
        }
        let observed_at = database_now(&mut transaction, "claimed run recovery clock").await?;
        authorize_worker(&stored, fence, observed_at)?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("claimed run recovery commit", source))?;
        Ok(ClaimedRunRecoveryObservation {
            run: stored,
            observed_at,
        })
    }

    /// Loads and fully verifies the immutable quarantine observation for a run.
    ///
    /// Migration-created legacy quarantines deliberately have no synthetic row
    /// here and return [`StoreError::RunQuarantineNotFound`].
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::RunNotFound`],
    /// [`StoreError::RunQuarantineNotFound`], a corruption failure, or a
    /// database error.
    pub async fn load_run_quarantine(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
    ) -> Result<RunQuarantine, StoreError> {
        let mut transaction = self.begin_repeatable_read("run quarantine load").await?;
        let row = load_run_quarantine_row_by_run(&mut transaction, tenant_id, run_id).await?;
        let quarantine = if let Some(row) = row {
            decode_run_quarantine(&row)?
        } else {
            let exists = query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM stateknot.runs WHERE tenant_id = $1 AND run_id = $2)",
            )
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("run quarantine owner lookup", source))?;
            return Err(if exists {
                StoreError::RunQuarantineNotFound
            } else {
                StoreError::RunNotFound
            });
        };
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("run quarantine load commit", source))?;
        Ok(quarantine)
    }

    /// Loads and verifies the immutable request half of one durable interrupt.
    ///
    /// The request remains available after resolution or explicit abandonment;
    /// terminal history is exposed by the corresponding terminal APIs.
    ///
    /// # Errors
    ///
    /// Returns an exact not-found/kind mismatch, corruption, or database error.
    pub async fn load_interrupt_request(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        interrupt_id: InterruptId,
    ) -> Result<InterruptRequest, StoreError> {
        let mut transaction = self.begin_repeatable_read("interrupt request load").await?;
        let wait = load_and_verify_wait_registration(
            &mut transaction,
            tenant_id,
            run_id,
            *interrupt_id.as_uuid(),
        )
        .await?;
        let DurableWait::Interrupt { request } = wait else {
            return Err(StoreError::WaitRegistrationKindMismatch);
        };
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("interrupt request load commit", source))?;
        Ok(*request)
    }

    /// Loads and verifies one immutable durable timer registration.
    ///
    /// # Errors
    ///
    /// Returns an exact not-found/kind mismatch, corruption, or database error.
    pub async fn load_durable_timer(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        timer_id: TimerId,
    ) -> Result<DurableTimer, StoreError> {
        let mut transaction = self.begin_repeatable_read("durable timer load").await?;
        let wait = load_and_verify_wait_registration(
            &mut transaction,
            tenant_id,
            run_id,
            *timer_id.as_uuid(),
        )
        .await?;
        let DurableWait::Timer { timer } = wait else {
            return Err(StoreError::WaitRegistrationKindMismatch);
        };
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("durable timer load commit", source))?;
        Ok(*timer)
    }

    /// Loads and validates the complete request/resolution history of one interrupt.
    ///
    /// # Errors
    ///
    /// Returns not-found, kind/conflict, corruption, or database failures.
    pub async fn load_interrupt_record(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        interrupt_id: InterruptId,
    ) -> Result<InterruptRecord, StoreError> {
        let mut transaction = self.begin_repeatable_read("interrupt history load").await?;
        let row = query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID.as_str())
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*interrupt_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("interrupt registration load", source))?
            .ok_or(StoreError::WaitRegistrationNotFound)?;
        let record = load_interrupt_record_from_row(&mut transaction, &row).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("interrupt history load commit", source))?;
        Ok(record)
    }

    /// Loads and validates the complete registration/firing history of one timer.
    ///
    /// # Errors
    ///
    /// Returns not-found, kind/conflict, corruption, or database failures.
    pub async fn load_durable_timer_record(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        timer_id: TimerId,
    ) -> Result<DurableTimerRecord, StoreError> {
        let mut transaction = self.begin_repeatable_read("timer history load").await?;
        let row = query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID.as_str())
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*timer_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("timer registration load", source))?
            .ok_or(StoreError::WaitRegistrationNotFound)?;
        let record = load_timer_record_from_row(&mut transaction, &row).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("timer history load commit", source))?;
        Ok(record)
    }

    /// Loads and validates the immutable audit fact for one abandoned interrupt.
    ///
    /// # Errors
    ///
    /// Returns exact registration/abandonment not-found, kind mismatch,
    /// corruption, or database failures.
    pub async fn load_interrupt_abandonment(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        interrupt_id: InterruptId,
    ) -> Result<WaitAbandonment, StoreError> {
        let mut transaction = self
            .begin_repeatable_read("interrupt abandonment load")
            .await?;
        let abandonment = load_wait_abandonment_by_id(
            &mut transaction,
            tenant_id,
            run_id,
            *interrupt_id.as_uuid(),
        )
        .await?;
        if !matches!(abandonment.wait(), DurableWait::Interrupt { .. }) {
            return Err(StoreError::WaitRegistrationKindMismatch);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("interrupt abandonment load commit", source))?;
        Ok(abandonment)
    }

    /// Loads and validates the immutable audit fact for one abandoned timer.
    ///
    /// # Errors
    ///
    /// Returns exact registration/abandonment not-found, kind mismatch,
    /// corruption, or database failures.
    pub async fn load_timer_abandonment(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        timer_id: TimerId,
    ) -> Result<WaitAbandonment, StoreError> {
        let mut transaction = self.begin_repeatable_read("timer abandonment load").await?;
        let abandonment =
            load_wait_abandonment_by_id(&mut transaction, tenant_id, run_id, *timer_id.as_uuid())
                .await?;
        if !matches!(abandonment.wait(), DurableWait::Timer { .. }) {
            return Err(StoreError::WaitRegistrationKindMismatch);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("timer abandonment load commit", source))?;
        Ok(abandonment)
    }

    /// Loads one fixed-database-cutoff page of outstanding timers whose due
    /// instant has been reached.
    ///
    /// New registrations cannot enter an existing cutoff because a timer's due
    /// instant is strictly after its registration observation. Concurrent
    /// firings may remove rows, while the exact keyset cursor prevents repeats.
    ///
    /// # Errors
    ///
    /// Returns cursor, clock, corruption, or database failures.
    pub async fn load_due_timer_page(
        &self,
        tenant_id: &TenantId,
        cursor: Option<&DueTimerPageCursor>,
        page_size: WaitDiscoveryPageSize,
    ) -> Result<DueTimerPage, StoreError> {
        if cursor.is_some_and(|cursor| {
            &cursor.tenant_id != tenant_id || cursor.due_at > cursor.snapshot_at
        }) {
            return Err(StoreError::InvalidDueTimerCursor);
        }
        let mut transaction = self.begin_repeatable_read("due timer page").await?;
        let (transaction_started_at, observed_at) =
            database_scheduler_times(&mut transaction, "due timer page clock").await?;
        let snapshot_at = cursor.map_or(transaction_started_at, |cursor| cursor.snapshot_at);
        if observed_at < transaction_started_at || observed_at < snapshot_at {
            return Err(StoreError::DatabaseClockRegression);
        }
        let mut rows = query_as::<_, WaitRegistrationRow>(SELECT_DUE_TIMER_PAGE.as_str())
            .bind(tenant_id.as_str())
            .bind(to_database_time(snapshot_at)?)
            .bind(
                cursor
                    .map(|cursor| to_database_time(cursor.due_at))
                    .transpose()?,
            )
            .bind(cursor.map(|cursor| *cursor.run_id.as_uuid()))
            .bind(cursor.map(|cursor| *cursor.timer_id.as_uuid()))
            .bind(i64::from(page_size.get()) + 1)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("due timer page query", source))?;
        let has_more = rows.len() > usize::from(page_size.get());
        rows.truncate(usize::from(page_size.get()));
        let mut owners = BTreeMap::new();
        let mut records = Vec::with_capacity(rows.len());
        let mut previous = cursor.map(|cursor| (cursor.due_at, cursor.run_id, cursor.timer_id));
        for row in rows {
            let sequence = row.registration_sequence;
            let wait = decode_wait_registration(&row)?;
            let DurableWait::Timer { timer } = wait else {
                return Err(StoreError::corrupt("due timer page kind"));
            };
            verify_wait_registration_event(
                &mut transaction,
                &DurableWait::Timer {
                    timer: timer.clone(),
                },
                sequence,
            )
            .await?;
            let key = (
                timer.marker().due_at(),
                timer.intent().run_id(),
                timer.marker().timer_id(),
            );
            if key.0 > snapshot_at
                || previous
                    .as_ref()
                    .is_some_and(|previous| !wait_discovery_key_after(&key, previous))
            {
                return Err(StoreError::corrupt("due timer page order"));
            }
            let owner = load_discovery_wait_owner(
                &mut transaction,
                &mut owners,
                tenant_id,
                timer.intent().run_id(),
            )
            .await?;
            if owner
                .lifecycle()
                .waits()
                .and_then(|waits| waits.timer(timer.marker().timer_id()))
                != Some(timer.marker())
            {
                return Err(StoreError::corrupt("due timer lifecycle owner"));
            }
            previous = Some(key);
            records.push(*timer);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("due timer page commit", source))?;
        Ok(DueTimerPage {
            tenant_id: tenant_id.clone(),
            snapshot_at,
            records,
            has_more,
        })
    }

    /// Loads one fixed-database-cutoff page of unresolved interrupts at or past
    /// their exclusive expiry.
    ///
    /// # Errors
    ///
    /// Returns cursor, clock, corruption, or database failures.
    pub async fn load_expired_interrupt_page(
        &self,
        tenant_id: &TenantId,
        cursor: Option<&ExpiredInterruptPageCursor>,
        page_size: WaitDiscoveryPageSize,
    ) -> Result<ExpiredInterruptPage, StoreError> {
        if cursor.is_some_and(|cursor| {
            &cursor.tenant_id != tenant_id || cursor.expires_at > cursor.snapshot_at
        }) {
            return Err(StoreError::InvalidExpiredInterruptCursor);
        }
        let mut transaction = self.begin_repeatable_read("expired interrupt page").await?;
        let (transaction_started_at, observed_at) =
            database_scheduler_times(&mut transaction, "expired interrupt page clock").await?;
        let snapshot_at = cursor.map_or(transaction_started_at, |cursor| cursor.snapshot_at);
        if observed_at < transaction_started_at || observed_at < snapshot_at {
            return Err(StoreError::DatabaseClockRegression);
        }
        let mut rows = query_as::<_, WaitRegistrationRow>(SELECT_EXPIRED_INTERRUPT_PAGE.as_str())
            .bind(tenant_id.as_str())
            .bind(to_database_time(snapshot_at)?)
            .bind(
                cursor
                    .map(|cursor| to_database_time(cursor.expires_at))
                    .transpose()?,
            )
            .bind(cursor.map(|cursor| *cursor.run_id.as_uuid()))
            .bind(cursor.map(|cursor| *cursor.interrupt_id.as_uuid()))
            .bind(i64::from(page_size.get()) + 1)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("expired interrupt page query", source))?;
        let has_more = rows.len() > usize::from(page_size.get());
        rows.truncate(usize::from(page_size.get()));
        let mut owners = BTreeMap::new();
        let mut records = Vec::with_capacity(rows.len());
        let mut previous =
            cursor.map(|cursor| (cursor.expires_at, cursor.run_id, cursor.interrupt_id));
        for row in rows {
            let sequence = row.registration_sequence;
            let wait = decode_wait_registration(&row)?;
            let DurableWait::Interrupt { request } = wait else {
                return Err(StoreError::corrupt("expired interrupt page kind"));
            };
            verify_wait_registration_event(
                &mut transaction,
                &DurableWait::Interrupt {
                    request: request.clone(),
                },
                sequence,
            )
            .await?;
            let expires_at = request
                .marker()
                .expires_at()
                .ok_or_else(|| StoreError::corrupt("expired interrupt finite expiry"))?;
            let key = (
                expires_at,
                request.intent().run_id(),
                request.marker().interrupt_id(),
            );
            if expires_at > snapshot_at
                || previous
                    .as_ref()
                    .is_some_and(|previous| !wait_discovery_key_after(&key, previous))
            {
                return Err(StoreError::corrupt("expired interrupt page order"));
            }
            let owner = load_discovery_wait_owner(
                &mut transaction,
                &mut owners,
                tenant_id,
                request.intent().run_id(),
            )
            .await?;
            if owner
                .lifecycle()
                .waits()
                .and_then(|waits| waits.interrupt(request.marker().interrupt_id()))
                != Some(request.marker())
            {
                return Err(StoreError::corrupt("expired interrupt lifecycle owner"));
            }
            previous = Some(key);
            records.push(*request);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("expired interrupt page commit", source))?;
        Ok(ExpiredInterruptPage {
            tenant_id: tenant_id.clone(),
            snapshot_at,
            records,
            has_more,
        })
    }

    /// Loads one tenant-scoped, stable-snapshot page of runnable candidates.
    ///
    /// The first page fixes a database transaction timestamp. Continuations
    /// retain that cutoff even when leases expire or new work arrives, so one
    /// bounded scan never chases a moving queue. Records are ordered by their
    /// effective availability and run identity. This method does not reserve a
    /// record: a scheduler must call [`Self::claim_lease`] for the selected run
    /// and handle [`StoreError::LeaseHeld`] as normal contention.
    ///
    /// Scheduling fairness across tenants is deliberately outside the storage
    /// contract; callers choose a tenant before scanning its durable queue.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidRunnableRunCursor`] when a continuation
    /// crosses tenant scope or has an impossible key, and otherwise returns a
    /// clock-regression, corruption, or database failure.
    pub async fn load_runnable_run_page(
        &self,
        tenant_id: &TenantId,
        cursor: Option<&RunnableRunPageCursor>,
        page_size: RunnableRunPageSize,
    ) -> Result<RunnableRunPage, StoreError> {
        if cursor.is_some_and(|cursor| {
            &cursor.tenant_id != tenant_id || cursor.available_at > cursor.snapshot_at
        }) {
            return Err(StoreError::InvalidRunnableRunCursor);
        }

        let mut transaction = self.begin_repeatable_read("runnable run page").await?;
        let (transaction_started_at, observed_at) =
            database_scheduler_times(&mut transaction, "runnable run page clock").await?;
        let snapshot_at = cursor.map_or(transaction_started_at, |cursor| cursor.snapshot_at);
        if observed_at < transaction_started_at || observed_at < snapshot_at {
            return Err(StoreError::DatabaseClockRegression);
        }

        let after_available_at = cursor
            .map(|cursor| to_database_time(cursor.available_at))
            .transpose()?;
        let after_run_id = cursor.map(|cursor| *cursor.run_id.as_uuid());
        let limit = i64::from(page_size.get()) + 1;
        let mut rows = query_as::<_, RunRow>(SELECT_RUNNABLE_RUN_PAGE)
            .bind(tenant_id.as_str())
            .bind(to_database_time(snapshot_at)?)
            .bind(after_available_at)
            .bind(after_run_id)
            .bind(limit)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("runnable run page query", source))?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("runnable run page commit", source))?;

        let has_more = rows.len() > usize::from(page_size.get());
        rows.truncate(usize::from(page_size.get()));
        let mut records = Vec::with_capacity(rows.len());
        let mut previous = cursor.map(|cursor| (cursor.available_at, cursor.run_id));
        for row in rows {
            let stored = decode_run(row)?;
            let provenance = stored.lifecycle().provenance();
            if provenance.tenant_id() != tenant_id
                || stored.is_quarantined()
                || !lifecycle_is_scheduler_runnable(stored.lifecycle().status())
            {
                return Err(StoreError::corrupt("runnable run page scope"));
            }
            let ready_at = stored
                .scheduler_ready_at()
                .ok_or_else(|| StoreError::corrupt("runnable run readiness"))?;
            let available_at = scheduler_available_at(&stored)?;
            let run_id = provenance.run_id();
            if available_at > snapshot_at
                || previous.is_some_and(|(previous_available_at, previous_run_id)| {
                    available_at < previous_available_at
                        || (available_at == previous_available_at
                            && run_id.as_uuid() <= previous_run_id.as_uuid())
                })
            {
                return Err(StoreError::corrupt("runnable run page order"));
            }
            previous = Some((available_at, run_id));
            records.push(RunnableRunCandidate {
                run: stored,
                ready_at,
                available_at,
            });
        }

        Ok(RunnableRunPage {
            tenant_id: tenant_id.clone(),
            snapshot_at,
            records,
            has_more,
        })
    }

    /// Idempotently registers one immutable, tenant-owned destination snapshot.
    ///
    /// `config` is a canonical schema-pinned routing envelope. Its digest must
    /// equal `destination.snapshot_digest()`. Raw credentials are outside this
    /// contract; configurations may name only external credential handles.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for digest mismatch, conflicting durable bytes,
    /// resource bounds, corruption, or database failure.
    pub async fn register_outbox_destination(
        &self,
        destination: OutboxDestinationRef,
        config: JournalPayload,
    ) -> Result<OutboxDestinationRegistrationOutcome, StoreError> {
        if destination.snapshot_digest() != config.digest() {
            return Err(StoreError::OutboxDestinationSnapshotMismatch);
        }
        let config_bytes = encode_outbox_destination_config(&config)?;
        let schema = config.schema();
        let mut transaction = self
            .begin_mutation("outbox destination registration")
            .await?;
        let observed_at = database_now(&mut transaction, "outbox destination clock").await?;
        let inserted = query(
            r"
INSERT INTO stateknot.outbox_destinations (
    tenant_id,
    destination_id,
    snapshot_digest,
    config_kind,
    schema_id,
    schema_version,
    schema_digest,
    config_bytes,
    created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tenant_id, destination_id, snapshot_digest) DO NOTHING
",
        )
        .bind(destination.tenant_id().as_str())
        .bind(*destination.destination_id().as_uuid())
        .bind(destination.snapshot_digest().as_bytes())
        .bind(config.kind().as_str())
        .bind(schema.id().as_str())
        .bind(schema.version().to_string())
        .bind(schema.digest().as_bytes())
        .bind(&config_bytes)
        .bind(to_database_time(observed_at)?)
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("outbox destination insert", source))?
        .rows_affected();

        let row = load_outbox_destination_row(&mut transaction, &destination)
            .await?
            .ok_or(StoreError::OutboxDestinationConflict)?;
        let stored = decode_outbox_destination(row)?;
        if stored.destination() != &destination || stored.config() != &config {
            return Err(StoreError::OutboxDestinationConflict);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("outbox destination commit", source))?;
        Ok(if inserted == 1 {
            OutboxDestinationRegistrationOutcome::Registered(stored)
        } else {
            OutboxDestinationRegistrationOutcome::Idempotent(stored)
        })
    }

    /// Loads and validates one immutable destination snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::OutboxDestinationNotFound`] when absent, or a
    /// corruption/database error.
    pub async fn load_outbox_destination(
        &self,
        destination: &OutboxDestinationRef,
    ) -> Result<StoredOutboxDestination, StoreError> {
        let mut transaction = self
            .begin_repeatable_read("outbox destination load")
            .await?;
        let row = load_outbox_destination_row(&mut transaction, destination)
            .await?
            .ok_or(StoreError::OutboxDestinationNotFound)?;
        let stored = decode_outbox_destination(row)?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("outbox destination load commit", source))?;
        Ok(stored)
    }

    /// Loads and fully validates one immutable outbox delivery.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::OutboxDeliveryNotFound`] when absent, or a
    /// corruption/database error.
    pub async fn load_outbox_delivery(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        delivery_id: DeliveryId,
    ) -> Result<OutboxDelivery, StoreError> {
        let mut transaction = self.begin_repeatable_read("outbox delivery load").await?;
        let row = load_outbox_delivery_row(&mut transaction, tenant_id, run_id, delivery_id, false)
            .await?
            .ok_or(StoreError::OutboxDeliveryNotFound)?;
        let delivery = decode_outbox_delivery(&row)?;
        verify_outbox_projection(&mut transaction, &row, &delivery).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("outbox delivery load commit", source))?;
        Ok(delivery)
    }

    /// Atomically appends a control-plane event and a non-empty delivery set.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid batch/scope, idempotency conflict,
    /// stale run state, missing destinations, corruption, or database failure.
    pub async fn append_control_plane_with_outbox(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        deliveries: Vec<OutboxDeliveryIntent>,
    ) -> Result<OutboxEnqueueOutcome, StoreError> {
        self.append_with_outbox(
            append,
            projection,
            AppendAuthority::ControlPlane,
            deliveries,
        )
        .await
    }

    /// Atomically appends a worker event and a non-empty delivery set under the
    /// exact current run fence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid batch/scope, stale/expired fencing,
    /// idempotency conflict, missing destinations, corruption, or database failure.
    pub async fn append_worker_with_outbox(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        deliveries: Vec<OutboxDeliveryIntent>,
    ) -> Result<OutboxEnqueueOutcome, StoreError> {
        self.append_with_outbox(append, projection, AppendAuthority::Worker, deliveries)
            .await
    }

    /// Loads and verifies one immutable tenant/run-scoped checkpoint by ID.
    ///
    /// This does not require the checkpoint to remain the run's current head;
    /// it is suitable for audit and exact lost-acknowledgement recovery.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::CheckpointNotFound`], a corruption failure, or a
    /// database error.
    pub async fn load_checkpoint(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        checkpoint_id: CheckpointId,
    ) -> Result<Checkpoint, StoreError> {
        let mut transaction = self.begin_repeatable_read("checkpoint load").await?;
        let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*checkpoint_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("checkpoint load", source))?
            .ok_or(StoreError::CheckpointNotFound)?;
        let checkpoint = decode_checkpoint(row)?;
        if checkpoint.tenant_id() != tenant_id
            || checkpoint.run_id() != run_id
            || checkpoint.checkpoint_id() != checkpoint_id
        {
            return Err(StoreError::corrupt("checkpoint scope"));
        }
        verify_checkpoint_anchor(&mut transaction, &checkpoint).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("checkpoint load commit", source))?;
        Ok(checkpoint)
    }

    /// Loads the run's exact current checkpoint in one repeatable-read snapshot.
    ///
    /// `None` means the admitted run has not yet committed its first graph
    /// barrier. The compact pointer and full checkpoint must agree exactly.
    ///
    /// # Errors
    ///
    /// Returns a corruption failure if the pointer is dangling or mismatched,
    /// otherwise a database error.
    pub async fn load_current_checkpoint(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
    ) -> Result<Option<Checkpoint>, StoreError> {
        let mut transaction = self.begin_repeatable_read("current checkpoint").await?;
        let row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("checkpoint run snapshot", source))?
            .ok_or(StoreError::RunNotFound)?;
        let run = decode_run(row)?;
        let checkpoint = if let Some(pointer) = run.checkpoint() {
            let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*pointer.checkpoint_id().as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("current checkpoint load", source))?
                .ok_or_else(|| StoreError::corrupt("current checkpoint pointer"))?;
            let checkpoint = decode_checkpoint(row)?;
            if checkpoint.tenant_id() != tenant_id
                || checkpoint.run_id() != run_id
                || checkpoint.checkpoint_id() != pointer.checkpoint_id()
                || checkpoint.superstep() != pointer.superstep()
                || checkpoint.digest() != pointer.digest()
            {
                return Err(StoreError::corrupt("current checkpoint projection"));
            }
            verify_checkpoint_anchor(&mut transaction, &checkpoint).await?;
            Some(checkpoint)
        } else {
            None
        };
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("current checkpoint commit", source))?;
        Ok(checkpoint)
    }

    /// Loads one bounded checkpoint lineage page in newest-to-oldest order.
    ///
    /// With no `from` cursor, the page starts at the current checkpoint observed
    /// together with the run row in one repeatable-read snapshot. A continuation
    /// must use the exact [`CheckpointLineagePage::next_cursor`] returned by the
    /// preceding verified page. Checkpoints are immutable, so later barrier
    /// commits cannot change the ancestry behind that cursor.
    ///
    /// Every returned checkpoint is decoded from canonical bytes, matched to
    /// its redundant columns, linked to the preceding child, and bound to the
    /// exact fully decoded journal event at its anchor.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidCheckpointCursor`] for a crossed, future, or
    /// non-exact cursor; otherwise returns explicit run, corruption, or database
    /// failures.
    pub async fn load_checkpoint_lineage_page(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        from: Option<&CheckpointHead>,
        page_size: CheckpointLineagePageSize,
    ) -> Result<CheckpointLineagePage, StoreError> {
        if from.is_some_and(|cursor| cursor.tenant_id() != tenant_id || cursor.run_id() != run_id) {
            return Err(StoreError::InvalidCheckpointCursor);
        }

        let mut transaction = self.begin_repeatable_read("checkpoint lineage").await?;
        let row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("checkpoint lineage run snapshot", source))?
            .ok_or(StoreError::RunNotFound)?;
        let run = decode_run(row)?;
        let Some(pointer) = run.checkpoint() else {
            if from.is_some() {
                return Err(StoreError::InvalidCheckpointCursor);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("empty checkpoint lineage commit", source)
            })?;
            return Ok(CheckpointLineagePage {
                checkpoints: Vec::new(),
                next_cursor: None,
            });
        };

        if let Some(cursor) = from {
            if cursor.superstep() > pointer.superstep()
                || (cursor.superstep() == pointer.superstep()
                    && (cursor.checkpoint_id() != pointer.checkpoint_id()
                        || cursor.digest() != pointer.digest()))
            {
                return Err(StoreError::InvalidCheckpointCursor);
            }
        }
        let start_id = from.map_or(pointer.checkpoint_id(), CheckpointHead::checkpoint_id);
        let query_limit = i64::from(page_size.get()) + 1;
        let rows = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_LINEAGE)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*start_id.as_uuid())
            .bind(query_limit)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("checkpoint lineage load", source))?;
        if rows.is_empty() {
            return Err(if from.is_some() {
                StoreError::InvalidCheckpointCursor
            } else {
                StoreError::corrupt("current checkpoint lineage pointer")
            });
        }

        let (checkpoints, lookahead) =
            decode_checkpoint_lineage(rows, tenant_id, run_id, page_size)?;

        let first = checkpoints
            .first()
            .ok_or_else(|| StoreError::corrupt("checkpoint lineage page"))?;
        let expected_tip = if let Some(cursor) = from {
            if first.head() != *cursor {
                return Err(StoreError::InvalidCheckpointCursor);
            }
            cursor.clone()
        } else {
            if first.checkpoint_id() != pointer.checkpoint_id()
                || first.superstep() != pointer.superstep()
                || first.digest() != pointer.digest()
            {
                return Err(StoreError::corrupt("current checkpoint lineage projection"));
            }
            first.head()
        };

        let mut verifier = CheckpointLineageVerifier::from_tip(expected_tip);
        for checkpoint in &checkpoints {
            verifier
                .verify_next(checkpoint)
                .map_err(|_| StoreError::corrupt("checkpoint lineage"))?;
        }
        match (verifier.expected(), lookahead) {
            (None, None) => {}
            (Some(expected), Some(parent)) if parent.head() == *expected => {}
            _ => return Err(StoreError::corrupt("checkpoint lineage parent")),
        }
        verify_checkpoint_anchors(&mut transaction, &checkpoints).await?;

        let next_cursor = verifier.expected().cloned();
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("checkpoint lineage commit", source))?;
        Ok(CheckpointLineagePage {
            checkpoints,
            next_cursor,
        })
    }

    /// Loads the exact current revision of one logical tool invocation.
    ///
    /// The intent, redundant current pointer, canonical record bytes, base
    /// checkpoint, and anchoring journal event are verified in one repeatable-
    /// read snapshot. Use [`Self::load_tool_invocation_history_page`] when a
    /// complete transition-chain audit is required.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ToolInvocationNotFound`], a corruption failure, or
    /// a database error.
    pub async fn load_tool_invocation(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        invocation_id: InvocationId,
    ) -> Result<ToolInvocation, StoreError> {
        let mut transaction = self.begin_repeatable_read("tool invocation load").await?;
        let row = query_as::<_, ToolInvocationRow>(SELECT_TOOL_INVOCATION)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*invocation_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("tool invocation load", source))?
            .ok_or(StoreError::ToolInvocationNotFound)?;
        let intent = decode_tool_invocation_intent(&row)?;
        if intent.tenant_id() != tenant_id
            || intent.run_id() != run_id
            || intent.invocation_id() != invocation_id
        {
            return Err(StoreError::corrupt("tool invocation scope"));
        }
        let current_revision = nonnegative_tool_invocation_revision(row.current_revision)?;
        let revision_row = load_tool_invocation_revision_row(
            &mut transaction,
            tenant_id,
            run_id,
            invocation_id,
            current_revision,
        )
        .await?;
        let invocation = decode_tool_invocation_revision(revision_row, &intent)?;
        validate_tool_invocation_current_projection(&row, &invocation)?;
        verify_tool_invocation_base_checkpoint(&mut transaction, &intent).await?;
        verify_tool_invocation_anchor(&mut transaction, &invocation).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("tool invocation load commit", source))?;
        Ok(invocation)
    }

    /// Loads one bounded ascending page of immutable invocation revisions.
    ///
    /// The first page starts at revision zero. A continuation must pass the full
    /// exact last record returned by [`ToolInvocationHistoryPage::next_cursor`],
    /// because retry validation needs the predecessor's failure evidence rather
    /// than only a compact head. The final page must converge to the current
    /// invocation pointer observed in the same repeatable-read snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidToolInvocationCursor`] for a crossed or
    /// non-exact cursor; otherwise returns explicit not-found, corruption, or
    /// database failures.
    pub async fn load_tool_invocation_history_page(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        invocation_id: InvocationId,
        after: Option<&ToolInvocation>,
        page_size: ToolInvocationHistoryPageSize,
    ) -> Result<ToolInvocationHistoryPage, StoreError> {
        if after.is_some_and(|cursor| {
            cursor.intent().tenant_id() != tenant_id
                || cursor.intent().run_id() != run_id
                || cursor.intent().invocation_id() != invocation_id
        }) {
            return Err(StoreError::InvalidToolInvocationCursor);
        }

        let mut transaction = self
            .begin_repeatable_read("tool invocation history")
            .await?;
        let row = query_as::<_, ToolInvocationRow>(SELECT_TOOL_INVOCATION)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*invocation_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("tool invocation history intent", source))?
            .ok_or(StoreError::ToolInvocationNotFound)?;
        let intent = decode_tool_invocation_intent(&row)?;
        let current_revision = nonnegative_tool_invocation_revision(row.current_revision)?;
        verify_tool_invocation_base_checkpoint(&mut transaction, &intent).await?;

        let cursor = if let Some(cursor) = after {
            if cursor.revision() > current_revision || cursor.intent() != &intent {
                return Err(StoreError::InvalidToolInvocationCursor);
            }
            let cursor_row = load_tool_invocation_revision_row(
                &mut transaction,
                tenant_id,
                run_id,
                invocation_id,
                cursor.revision(),
            )
            .await
            .map_err(|error| match error {
                StoreError::ToolInvocationNotFound => StoreError::InvalidToolInvocationCursor,
                other => other,
            })?;
            let stored_cursor = decode_tool_invocation_revision(cursor_row, &intent)?;
            if encode_tool_invocation_record(&stored_cursor)?
                != encode_tool_invocation_record(cursor)?
            {
                return Err(StoreError::InvalidToolInvocationCursor);
            }
            verify_tool_invocation_anchor(&mut transaction, &stored_cursor).await?;
            Some(stored_cursor)
        } else {
            None
        };

        let after_revision = cursor.as_ref().map_or(-1_i64, |cursor| {
            i64::try_from(cursor.revision().get()).unwrap_or(i64::MAX)
        });
        let query_limit = i64::from(page_size.get());
        let rows = query_as::<_, ToolInvocationRevisionRow>(SELECT_TOOL_INVOCATION_HISTORY)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*invocation_id.as_uuid())
            .bind(after_revision)
            .bind(query_limit)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("tool invocation history load", source))?;

        let mut verifier = cursor
            .clone()
            .map_or_else(ToolInvocationHistoryVerifier::new, |cursor| {
                ToolInvocationHistoryVerifier::after(cursor)
            });
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let record = decode_tool_invocation_revision(row, &intent)?;
            verifier
                .verify_next(&record)
                .map_err(|_| StoreError::corrupt("tool invocation history"))?;
            verify_tool_invocation_anchor(&mut transaction, &record).await?;
            records.push(record);
        }
        let final_record = records
            .last()
            .or(cursor.as_ref())
            .ok_or_else(|| StoreError::corrupt("tool invocation empty history"))?;
        let has_more = final_record.revision() < current_revision;
        if has_more && records.is_empty() {
            return Err(StoreError::corrupt("tool invocation history gap"));
        }
        if !has_more {
            validate_tool_invocation_current_projection(&row, final_record)?;
            if verifier.head() != Some(final_record.head()) {
                return Err(StoreError::corrupt("tool invocation history head"));
            }
        }

        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("tool invocation history commit", source))?;
        Ok(ToolInvocationHistoryPage { records, has_more })
    }

    /// Loads the exact current revision of one logical model invocation.
    ///
    /// The immutable intent, compact revision wire, redundant current pointer,
    /// base checkpoint, and journal anchor are verified in one repeatable-read
    /// snapshot. Use [`Self::load_model_invocation_history_page`] to prove a
    /// complete retry chain.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::ModelInvocationNotFound`], a corruption failure,
    /// or a database error.
    pub async fn load_model_invocation(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        invocation_id: InvocationId,
    ) -> Result<ModelInvocation, StoreError> {
        let mut transaction = self.begin_repeatable_read("model invocation load").await?;
        let row = query_as::<_, ModelInvocationRow>(SELECT_MODEL_INVOCATION)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*invocation_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("model invocation load", source))?
            .ok_or(StoreError::ModelInvocationNotFound)?;
        let intent = decode_model_invocation_intent(&row)?;
        if intent.tenant_id() != tenant_id
            || intent.run_id() != run_id
            || intent.invocation_id() != invocation_id
        {
            return Err(StoreError::corrupt("model invocation scope"));
        }
        let current_revision = nonnegative_model_invocation_revision(row.current_revision)?;
        let revision_row = load_model_invocation_revision_row(
            &mut transaction,
            tenant_id,
            run_id,
            invocation_id,
            current_revision,
        )
        .await?;
        let invocation = decode_model_invocation_revision(revision_row, &intent)?;
        validate_model_invocation_current_projection(&row, &invocation)?;
        verify_model_invocation_base_checkpoint(&mut transaction, &intent).await?;
        verify_model_invocation_anchor(&mut transaction, &invocation).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("model invocation load commit", source))?;
        Ok(invocation)
    }

    /// Loads one bounded ascending page of immutable model revisions.
    ///
    /// A continuation cursor is the full prior record because a failed record's
    /// error and durable timestamp are required to validate a delayed retry.
    /// The page size is one to bound decoding of maximum-size model payloads.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidModelInvocationCursor`] for a crossed or
    /// non-exact cursor; otherwise returns not-found, corruption, or database
    /// failures.
    pub async fn load_model_invocation_history_page(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        invocation_id: InvocationId,
        after: Option<&ModelInvocation>,
        page_size: ModelInvocationHistoryPageSize,
    ) -> Result<ModelInvocationHistoryPage, StoreError> {
        if after.is_some_and(|cursor| {
            cursor.intent().tenant_id() != tenant_id
                || cursor.intent().run_id() != run_id
                || cursor.intent().invocation_id() != invocation_id
        }) {
            return Err(StoreError::InvalidModelInvocationCursor);
        }

        let mut transaction = self
            .begin_repeatable_read("model invocation history")
            .await?;
        let row = query_as::<_, ModelInvocationRow>(SELECT_MODEL_INVOCATION)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*invocation_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("model invocation history intent", source))?
            .ok_or(StoreError::ModelInvocationNotFound)?;
        let intent = decode_model_invocation_intent(&row)?;
        let current_revision = nonnegative_model_invocation_revision(row.current_revision)?;
        verify_model_invocation_base_checkpoint(&mut transaction, &intent).await?;

        let cursor = if let Some(cursor) = after {
            if cursor.revision() > current_revision || cursor.intent() != &intent {
                return Err(StoreError::InvalidModelInvocationCursor);
            }
            let cursor_row = load_model_invocation_revision_row(
                &mut transaction,
                tenant_id,
                run_id,
                invocation_id,
                cursor.revision(),
            )
            .await
            .map_err(|error| match error {
                StoreError::ModelInvocationNotFound => StoreError::InvalidModelInvocationCursor,
                other => other,
            })?;
            let stored_cursor = decode_model_invocation_revision(cursor_row, &intent)?;
            if encode_model_invocation_record(&stored_cursor)?
                != encode_model_invocation_record(cursor)?
            {
                return Err(StoreError::InvalidModelInvocationCursor);
            }
            verify_model_invocation_anchor(&mut transaction, &stored_cursor).await?;
            Some(stored_cursor)
        } else {
            None
        };

        let after_revision = cursor.as_ref().map_or(-1_i64, |cursor| {
            i64::try_from(cursor.revision().get()).unwrap_or(i64::MAX)
        });
        let rows = query_as::<_, ModelInvocationRevisionRow>(SELECT_MODEL_INVOCATION_HISTORY)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*invocation_id.as_uuid())
            .bind(after_revision)
            .bind(i64::from(page_size.get()))
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("model invocation history load", source))?;

        let mut verifier = cursor
            .clone()
            .map_or_else(ModelInvocationHistoryVerifier::new, |cursor| {
                ModelInvocationHistoryVerifier::after(cursor)
            });
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let record = decode_model_invocation_revision(row, &intent)?;
            verifier
                .verify_next(&record)
                .map_err(|_| StoreError::corrupt("model invocation history"))?;
            verify_model_invocation_anchor(&mut transaction, &record).await?;
            records.push(record);
        }
        let final_record = records
            .last()
            .or(cursor.as_ref())
            .ok_or_else(|| StoreError::corrupt("model invocation empty history"))?;
        let has_more = final_record.revision() < current_revision;
        if has_more && records.is_empty() {
            return Err(StoreError::corrupt("model invocation history gap"));
        }
        if !has_more {
            validate_model_invocation_current_projection(&row, final_record)?;
            if verifier.head() != Some(final_record.head()) {
                return Err(StoreError::corrupt("model invocation history head"));
            }
        }

        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("model invocation history commit", source))?;
        Ok(ModelInvocationHistoryPage { records, has_more })
    }

    /// Loads and fully verifies one physical node attempt.
    ///
    /// Canonical start/completion bytes, every redundant projection, the base
    /// checkpoint, both worker journal anchors, and a successful pending-result
    /// binding are checked in one repeatable-read snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NodeAttemptNotFound`] when the identity is absent
    /// from the tenant/run boundary; otherwise returns an integrity or database
    /// failure.
    pub async fn load_node_attempt(
        &self,
        tenant_id: &TenantId,
        run_id: &RunId,
        attempt_id: AttemptId,
    ) -> Result<NodeAttempt, StoreError> {
        let mut transaction = self.begin_repeatable_read("node attempt load").await?;
        let attempt = load_node_attempt_record(&mut transaction, tenant_id, run_id, attempt_id)
            .await?
            .ok_or(StoreError::NodeAttemptNotFound)?;
        verify_node_attempt(&mut transaction, &attempt).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("node attempt load commit", source))?;
        Ok(attempt)
    }

    /// Loads one bounded, fully verified page of an activation's physical history.
    ///
    /// Pass the exact full attempt returned by [`NodeAttemptHistoryPage::next_cursor`]
    /// to continue. The provider reloads and verifies that cursor before using
    /// its immutable start position, so a constructed or cross-activation
    /// cursor fails closed.
    ///
    /// # Errors
    ///
    /// Returns an invalid cursor, durable integrity, or database failure.
    pub async fn load_node_attempt_history_page(
        &self,
        activation: &NodeActivation,
        cursor: Option<&NodeAttempt>,
        page_size: NodeAttemptHistoryPageSize,
    ) -> Result<NodeAttemptHistoryPage, StoreError> {
        if cursor.is_some_and(|cursor| cursor.start().activation() != activation) {
            return Err(StoreError::InvalidNodeAttemptCursor);
        }
        let mut transaction = self
            .begin_repeatable_read("node attempt history load")
            .await?;
        if let Some(cursor) = cursor {
            let durable = load_node_attempt_record(
                &mut transaction,
                activation.tenant_id(),
                &activation.run_id(),
                cursor.start().attempt_id(),
            )
            .await?
            .ok_or(StoreError::InvalidNodeAttemptCursor)?;
            if encode_node_attempt(&durable)? != encode_node_attempt(cursor)? {
                return Err(StoreError::InvalidNodeAttemptCursor);
            }
            verify_node_attempt(&mut transaction, &durable).await?;
        }

        let base = activation.base_checkpoint();
        let base_superstep = i64::try_from(base.superstep().get())
            .map_err(|_| StoreError::InvalidNodeAttemptCursor)?;
        let after_sequence = cursor.map_or(0_i64, |cursor| {
            i64::try_from(cursor.start().journal_head().sequence().get()).unwrap_or(i64::MAX)
        });
        let query_limit = i64::from(page_size.get()) + 1;
        let mut rows = query_as::<_, NodeAttemptStartRow>(SELECT_NODE_ATTEMPT_HISTORY)
            .bind(activation.tenant_id().as_str())
            .bind(*activation.run_id().as_uuid())
            .bind(*base.checkpoint_id().as_uuid())
            .bind(base_superstep)
            .bind(base.digest().as_bytes())
            .bind(activation.graph_namespace().as_str())
            .bind(activation.node_id().as_str())
            .bind(activation.input_digest().as_bytes())
            .bind(after_sequence)
            .bind(query_limit)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("node attempt history rows", source))?;
        let has_more = rows.len() > usize::from(page_size.get());
        rows.truncate(usize::from(page_size.get()));

        let mut verifier = cursor.cloned().map_or_else(
            NodeAttemptHistoryVerifier::new,
            NodeAttemptHistoryVerifier::after,
        );
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let start = decode_node_attempt_start(&row)?;
            let completion_row =
                query_as::<_, NodeAttemptCompletionRow>(SELECT_NODE_ATTEMPT_COMPLETION)
                    .bind(activation.tenant_id().as_str())
                    .bind(*activation.run_id().as_uuid())
                    .bind(*start.attempt_id().as_uuid())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|source| {
                        StoreError::database("node attempt history completion", source)
                    })?;
            let completion = completion_row
                .map(|row| decode_node_attempt_completion(&row, &start))
                .transpose()?;
            let attempt = NodeAttempt::restore(start, completion)
                .map_err(|_| StoreError::corrupt("node attempt history join"))?;
            verifier
                .verify_next(&attempt)
                .map_err(|_| StoreError::corrupt("node attempt history"))?;
            verify_node_attempt(&mut transaction, &attempt).await?;
            records.push(attempt);
        }
        if has_more && records.is_empty() {
            return Err(StoreError::corrupt("node attempt history look-ahead"));
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("node attempt history commit", source))?;
        Ok(NodeAttemptHistoryPage { records, has_more })
    }

    /// Loads and fully verifies one immutable pending node result.
    ///
    /// The result's canonical bytes, redundant projections, base checkpoint,
    /// worker journal anchor, binding rows, full invocation intents, exact
    /// committed revisions, and invocation journal anchors are checked in one
    /// repeatable-read snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::PendingNodeResultNotFound`] when the exact
    /// activation is absent; otherwise returns an integrity or database error.
    pub async fn load_pending_node_result(
        &self,
        activation: &NodeActivation,
    ) -> Result<PendingNodeResult, StoreError> {
        let mut transaction = self
            .begin_repeatable_read("pending node result load")
            .await?;
        let row = load_pending_node_result_row(&mut transaction, activation)
            .await?
            .ok_or(StoreError::PendingNodeResultNotFound)?;
        let result = decode_pending_node_result(&row)?;
        if result.intent().activation() != activation {
            return Err(StoreError::PendingNodeResultNotFound);
        }
        verify_pending_node_result(&mut transaction, &result).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("pending node result load commit", source))?;
        Ok(result)
    }

    async fn load_historical_graph_barrier_results(
        &self,
        parent: &Checkpoint,
        child: &Checkpoint,
        limits: GraphReplayLimits,
    ) -> Result<HistoricalGraphBarrierResults, StoreError> {
        let mut transaction = self
            .begin_repeatable_read("historical graph barrier replay")
            .await?;
        let heads = load_locked_barrier_result_heads(&mut transaction, &parent.head()).await?;
        let barrier = CheckpointBarrier::new(parent, child.write_intent(), heads.clone())
            .map_err(|_| StoreError::corrupt("noninitial graph replay result set"))?;

        let mut results = Vec::with_capacity(heads.len());
        let mut compact_bytes = 0_usize;
        for head in &heads {
            let row = load_pending_node_result_row(&mut transaction, head.activation())
                .await?
                .ok_or_else(|| StoreError::corrupt("noninitial graph replay result"))?;
            let result = decode_pending_node_result(&row)?;
            if result.head() != *head {
                return Err(StoreError::corrupt("noninitial graph replay result head"));
            }
            verify_pending_node_result(&mut transaction, &result).await?;

            let mut counter = CompactByteCounter::default();
            serde_json::to_writer(&mut counter, &result)
                .map_err(|_| StoreError::corrupt("noninitial graph replay result encoding"))?;
            compact_bytes = compact_bytes.saturating_add(counter.bytes);
            if compact_bytes > limits.maximum_barrier_result_bytes() {
                return Err(StoreError::GraphReplayResourceLimit);
            }
            results.push(result);
        }
        verify_barrier_consumptions(&mut transaction, &barrier, child)
            .await
            .map_err(map_graph_replay_consumption_error)?;
        transaction.commit().await.map_err(|source| {
            StoreError::database("historical graph barrier replay commit", source)
        })?;
        Ok(HistoricalGraphBarrierResults {
            results,
            compact_bytes,
        })
    }

    /// Loads one stable-snapshot page of fully verified unconsumed node results.
    ///
    /// The first call passes no cursor. A continuation must use the exact
    /// [`PendingNodeResultPage::next_cursor`] from the preceding page. The
    /// cursor pins the run journal head, so any concurrent result commit makes
    /// continuation return [`StoreError::StalePendingNodeResultSnapshot`]
    /// instead of allowing keyset pagination to miss a lower activation key.
    /// Only the compact look-ahead row exceeds `page_size`; full result and
    /// invocation records are decoded within their independent hard bounds.
    ///
    /// # Errors
    ///
    /// Returns an invalid/stale cursor error, stale checkpoint error, durable
    /// corruption, or database failure. The supplied base must still be the
    /// exact current checkpoint of the run.
    pub async fn load_unconsumed_pending_node_result_page(
        &self,
        base: &CheckpointHead,
        cursor: Option<&PendingNodeResultPageCursor>,
        page_size: PendingNodeResultPageSize,
    ) -> Result<PendingNodeResultPage, StoreError> {
        if cursor.is_some_and(|cursor| !pending_result_cursor_matches_base(cursor, base)) {
            return Err(StoreError::InvalidPendingNodeResultCursor);
        }

        let tenant_id = base.tenant_id();
        let run_id = base.run_id();
        let mut transaction = self
            .begin_repeatable_read("unconsumed pending node result page")
            .await?;
        let run_row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("pending result page run snapshot", source))?
            .ok_or(StoreError::RunNotFound)?;
        let stored = decode_run(run_row)?;
        let checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if checkpoint.head() != *base {
            return Err(StoreError::StaleCheckpointHead);
        }
        let snapshot_journal_head = stored
            .journal_head()
            .cloned()
            .ok_or_else(|| StoreError::corrupt("pending result page run journal head"))?;
        if cursor.is_some_and(|cursor| cursor.snapshot_journal_head != snapshot_journal_head) {
            return Err(StoreError::StalePendingNodeResultSnapshot);
        }

        if let Some(cursor) = cursor {
            let row = load_pending_node_result_head_row(&mut transaction, cursor.after()).await?;
            let durable = decode_pending_node_result_head(row, base)?;
            if durable != cursor.after {
                return Err(StoreError::InvalidPendingNodeResultCursor);
            }
        }

        let base_superstep = i64::try_from(base.superstep().get())
            .map_err(|_| StoreError::InvalidPendingNodeResultCursor)?;
        let query_limit = i64::from(page_size.get()) + 1;
        let rows = if let Some(cursor) = cursor {
            query_as::<_, PendingNodeResultHeadRow>(
                SELECT_UNCONSUMED_PENDING_NODE_RESULT_HEADS_AFTER,
            )
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*base.checkpoint_id().as_uuid())
            .bind(base_superstep)
            .bind(base.digest().as_bytes())
            .bind(cursor.after().activation().graph_namespace().as_str())
            .bind(cursor.after().activation().node_id().as_str())
            .bind(query_limit)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| {
                StoreError::database("unconsumed pending result continuation", source)
            })?
        } else {
            query_as::<_, PendingNodeResultHeadRow>(SELECT_UNCONSUMED_PENDING_NODE_RESULT_HEADS)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*base.checkpoint_id().as_uuid())
                .bind(base_superstep)
                .bind(base.digest().as_bytes())
                .bind(query_limit)
                .fetch_all(&mut *transaction)
                .await
                .map_err(|source| {
                    StoreError::database("unconsumed pending result first page", source)
                })?
        };
        let has_more = rows.len() > usize::from(page_size.get());
        let retained = rows.into_iter().take(usize::from(page_size.get()));
        let mut records = Vec::with_capacity(usize::from(page_size.get()));
        for compact_row in retained {
            let compact = decode_pending_node_result_head(compact_row, base)?;
            let row = load_pending_node_result_row(&mut transaction, compact.activation())
                .await?
                .ok_or_else(|| StoreError::corrupt("pending result page row"))?;
            let result = decode_pending_node_result(&row)?;
            if result.head() != compact {
                return Err(StoreError::corrupt("pending result page compact head"));
            }
            verify_pending_node_result(&mut transaction, &result).await?;
            records.push(result);
        }
        if has_more && records.is_empty() {
            return Err(StoreError::corrupt("pending result page look-ahead"));
        }

        transaction.commit().await.map_err(|source| {
            StoreError::database("unconsumed pending result page commit", source)
        })?;
        Ok(PendingNodeResultPage {
            base_checkpoint: base.clone(),
            snapshot_journal_head,
            records,
            has_more,
        })
    }

    /// Loads one bounded, fully verified outbox-attempt history page.
    ///
    /// The provider replays the complete bounded history on every page so a
    /// cursor cannot hide an invalid predecessor. At most 64 small attempt
    /// records exist for one delivery.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidOutboxAttemptCursor`] for a crossed or
    /// non-exact cursor, and otherwise explicit not-found, corruption, or
    /// database failures.
    pub async fn load_outbox_attempt_history_page(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        delivery_id: DeliveryId,
        after: Option<&OutboxAttempt>,
        page_size: OutboxAttemptHistoryPageSize,
    ) -> Result<OutboxAttemptHistoryPage, StoreError> {
        if after.is_some_and(|attempt| {
            attempt.start().delivery().tenant_id() != tenant_id
                || attempt.start().delivery().run_id() != run_id
                || attempt.start().delivery().delivery_id() != delivery_id
        }) {
            return Err(StoreError::InvalidOutboxAttemptCursor);
        }
        let mut transaction = self.begin_repeatable_read("outbox attempt history").await?;
        let row = load_outbox_delivery_row(&mut transaction, tenant_id, run_id, delivery_id, false)
            .await?
            .ok_or(StoreError::OutboxDeliveryNotFound)?;
        let delivery = decode_outbox_delivery(&row)?;
        let all = load_and_verify_outbox_attempts(&mut transaction, &delivery).await?;
        verify_outbox_projection_records(&row, &delivery, &all)?;

        let start_index = if let Some(cursor) = after {
            all.iter()
                .position(|record| outbox_attempts_equal(record, cursor))
                .map(|index| index + 1)
                .ok_or(StoreError::InvalidOutboxAttemptCursor)?
        } else {
            0
        };
        let remaining = &all[start_index..];
        let has_more = remaining.len() > usize::from(page_size.get());
        let records = remaining
            .iter()
            .take(usize::from(page_size.get()))
            .cloned()
            .collect();
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("outbox attempt history commit", source))?;
        Ok(OutboxAttemptHistoryPage { records, has_more })
    }

    /// Atomically claims the earliest eligible unlocked delivery for one tenant.
    ///
    /// A fresh start and run-wide attempt claim commit before the caller may
    /// perform network I/O. Retrying with the same `attempt_id` returns the
    /// original claim. [`OutboxClaimOutcome::NoWork`] means no eligible *unlocked*
    /// row was visible; another worker may temporarily hold skipped work.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for attempt identity conflict, corrupt history,
    /// timing exhaustion, or database failure.
    pub async fn claim_outbox_delivery(
        &self,
        tenant_id: &TenantId,
        attempt_id: AttemptId,
    ) -> Result<OutboxClaimOutcome, StoreError> {
        let mut transaction = self.begin_mutation("outbox delivery claim").await?;
        let observed_at = database_now(&mut transaction, "outbox claim clock").await?;
        reap_outbox_terminals(&mut transaction, tenant_id, observed_at).await?;

        if let Some(claim) =
            load_idempotent_outbox_claim(&mut transaction, tenant_id, attempt_id).await?
        {
            transaction
                .commit()
                .await
                .map_err(|source| StoreError::database("idempotent outbox claim commit", source))?;
            return Ok(OutboxClaimOutcome::Idempotent(claim));
        }

        let Some(row) = query_as::<_, OutboxDeliveryRow>(SELECT_OUTBOX_CLAIM_CANDIDATE.as_str())
            .bind(tenant_id.as_str())
            .bind(to_database_time(observed_at)?)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("outbox claim candidate", source))?
        else {
            transaction
                .commit()
                .await
                .map_err(|source| StoreError::database("empty outbox claim commit", source))?;
            return Ok(OutboxClaimOutcome::NoWork);
        };

        let delivery = decode_outbox_delivery(&row)?;
        verify_outbox_delivery_anchor(&mut transaction, &delivery).await?;
        let destination =
            load_and_decode_outbox_destination(&mut transaction, delivery.intent().destination())
                .await?;
        let history = load_and_verify_outbox_attempts(&mut transaction, &delivery).await?;
        verify_outbox_projection_records(&row, &delivery, &history)?;
        let mut verifier = OutboxAttemptHistoryVerifier::new(&delivery);
        for attempt in &history {
            verifier
                .verify_next(attempt)
                .map_err(|_| StoreError::corrupt("outbox attempt history"))?;
        }
        if verifier
            .status_at(observed_at)
            .map_err(|_| StoreError::corrupt("outbox delivery status"))?
            != OutboxDeliveryStatus::Pending
        {
            return Err(StoreError::corrupt("outbox ready projection"));
        }

        let next_count = row
            .attempt_count
            .checked_add(1)
            .ok_or(StoreError::InvalidOutboxTransition)?;
        if usize::try_from(next_count)
            .ok()
            .is_none_or(|count| count > MAX_OUTBOX_ATTEMPTS)
        {
            return Err(StoreError::InvalidOutboxTransition);
        }
        let epoch = FencingEpoch::new(
            u64::try_from(next_count).map_err(|_| StoreError::InvalidOutboxTransition)?,
        )
        .map_err(|_| StoreError::InvalidOutboxTransition)?;
        let configured_expiry =
            add_duration(observed_at, self.options.outbox_attempt_lease_duration)?;
        let expires_at = configured_expiry.min(delivery.intent().expires_at());
        let fence = DeliveryFence::new(
            tenant_id.clone(),
            delivery.intent().run_id(),
            delivery.intent().delivery_id(),
            attempt_id,
            epoch,
        );
        let start = OutboxAttemptStart::new(&delivery, fence, observed_at, expires_at)
            .map_err(|_| StoreError::InvalidOutboxTransition)?;

        insert_outbox_attempt_claim(&mut transaction, &delivery, &start).await?;
        insert_outbox_attempt_start(&mut transaction, &delivery, &start).await?;
        update_outbox_delivery_claim(&mut transaction, &delivery, &start, row.attempt_count)
            .await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("outbox claim commit", source))?;
        Ok(OutboxClaimOutcome::Claimed(OutboxClaim {
            destination,
            delivery,
            start,
        }))
    }

    /// Commits a protocol acknowledgement under the exact live delivery fence.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for stale/expired fencing, conflicting lost-ack
    /// retries, corruption, or database failure.
    pub async fn acknowledge_outbox_attempt(
        &self,
        fence: &DeliveryFence,
        evidence_digest: Option<Digest>,
    ) -> Result<OutboxCompletionOutcome, StoreError> {
        self.complete_outbox_attempt(
            fence,
            OutboxAttemptOutcome::Acknowledged { evidence_digest },
        )
        .await
    }

    /// Commits public-safe failure evidence under the exact live delivery fence.
    ///
    /// `safe_after` schedules another attempt at a durable database-clock
    /// boundary; `never` moves the delivery to dead-letter. Reconcile-first is
    /// rejected by the core duplicate-tolerant outbox contract.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for stale/expired fencing, unsafe advice,
    /// conflicting lost-ack retries, corruption, or database failure.
    pub async fn fail_outbox_attempt(
        &self,
        fence: &DeliveryFence,
        failure: Failure,
    ) -> Result<OutboxCompletionOutcome, StoreError> {
        self.complete_outbox_attempt(fence, OutboxAttemptOutcome::Failed { failure })
            .await
    }

    async fn complete_outbox_attempt(
        &self,
        fence: &DeliveryFence,
        requested_outcome: OutboxAttemptOutcome,
    ) -> Result<OutboxCompletionOutcome, StoreError> {
        let mut transaction = self.begin_mutation("outbox attempt completion").await?;
        let delivery_row = load_outbox_delivery_row(
            &mut transaction,
            fence.tenant_id(),
            fence.run_id(),
            fence.delivery_id(),
            true,
        )
        .await?
        .ok_or(StoreError::OutboxDeliveryNotFound)?;
        let delivery = decode_outbox_delivery(&delivery_row)?;
        verify_outbox_projection(&mut transaction, &delivery_row, &delivery).await?;
        let epoch =
            i64::try_from(fence.epoch().get()).map_err(|_| StoreError::InvalidOutboxTransition)?;
        let start_row =
            query_as::<_, OutboxAttemptStartRow>(SELECT_OUTBOX_ATTEMPT_BY_FENCE.as_str())
                .bind(fence.tenant_id().as_str())
                .bind(*fence.run_id().as_uuid())
                .bind(*fence.delivery_id().as_uuid())
                .bind(epoch)
                .bind(*fence.attempt_id().as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("outbox completion start load", source))?
                .ok_or(StoreError::OutboxAttemptNotFound)?;
        let start = decode_outbox_attempt_start(&start_row)?;
        if start.delivery() != &delivery.head() || start.fence() != fence {
            return Err(StoreError::StaleOutboxFence);
        }

        if let Some(row) = load_outbox_attempt_completion_row(
            &mut transaction,
            fence.tenant_id(),
            fence.run_id(),
            fence.delivery_id(),
            epoch,
        )
        .await?
        {
            let completion = decode_outbox_attempt_completion(&row)?;
            if completion.start() != &start.head()
                || !outbox_outcomes_equal(completion.outcome(), &requested_outcome)
            {
                return Err(StoreError::OutboxCompletionConflict);
            }
            let attempt = OutboxAttempt::restore(start, Some(completion))
                .map_err(|_| StoreError::corrupt("outbox completed attempt"))?;
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent outbox completion commit", source)
            })?;
            return Ok(OutboxCompletionOutcome::Idempotent { attempt });
        }

        if delivery_row.status != "delivering"
            || delivery_row.current_attempt_id != Some(*fence.attempt_id().as_uuid())
            || delivery_row.current_epoch != Some(epoch)
        {
            return Err(StoreError::StaleOutboxFence);
        }
        let observed_at = database_now(&mut transaction, "outbox completion clock").await?;
        if observed_at >= start.expires_at() {
            return Err(StoreError::OutboxAttemptExpired);
        }
        let completion = match requested_outcome {
            OutboxAttemptOutcome::Acknowledged { evidence_digest } => {
                OutboxAttemptCompletion::acknowledge(&start, evidence_digest, observed_at)
            }
            OutboxAttemptOutcome::Failed { failure } => {
                OutboxAttemptCompletion::fail(&start, failure, observed_at)
            }
            _ => return Err(StoreError::InvalidOutboxTransition),
        }
        .map_err(|_| StoreError::InvalidOutboxTransition)?;
        insert_outbox_attempt_completion(&mut transaction, &completion).await?;
        update_outbox_delivery_completion(
            &mut transaction,
            &delivery,
            &start,
            &completion,
            delivery_row.attempt_count,
        )
        .await?;
        let attempt = OutboxAttempt::restore(start, Some(completion))
            .map_err(|_| StoreError::corrupt("outbox completed attempt"))?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("outbox completion commit", source))?;
        Ok(OutboxCompletionOutcome::Committed { attempt })
    }

    /// Claims an unowned or expired runnable run for a stable `UUIDv7` attempt.
    ///
    /// The database row is locked before the database clock is observed. An
    /// unexpired different owner is never replaced by this method. Retrying the
    /// same attempt returns [`LeaseClaimOutcome::Idempotent`] without allocating
    /// another fencing epoch.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the run is missing, not runnable, quarantined,
    /// currently leased, exhausted, corrupt, or unavailable.
    pub async fn claim_lease(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        attempt_id: AttemptId,
    ) -> Result<LeaseClaimOutcome, StoreError> {
        self.acquire_lease(tenant_id, run_id, attempt_id, false)
            .await
    }

    /// Explicitly supersedes any current lease with a stable successor attempt.
    ///
    /// This is a trusted control-plane operation for drain, repair, and forced
    /// takeover. It increments the epoch even when the previous lease remains
    /// unexpired, fencing the old worker in the same locked-row transaction.
    /// Retrying the same successor attempt is idempotent. Supersession also
    /// clears a future delayed-retry gate, but the successor's recovery plan
    /// still cannot authorize that node before its durable retry evidence is
    /// due.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the run is missing, not runnable, quarantined,
    /// the successor attempt has expired, epochs are exhausted, data is corrupt,
    /// or the database is unavailable.
    pub async fn supersede_lease(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        successor_attempt_id: AttemptId,
    ) -> Result<LeaseClaimOutcome, StoreError> {
        self.acquire_lease(tenant_id, run_id, successor_attempt_id, true)
            .await
    }

    async fn acquire_lease(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        attempt_id: AttemptId,
        supersede: bool,
    ) -> Result<LeaseClaimOutcome, StoreError> {
        let operation = if supersede {
            "lease supersession"
        } else {
            "lease claim"
        };
        let mut transaction = self.begin_mutation(operation).await?;
        let row = fetch_locked_run_row(&mut transaction, tenant_id, run_id).await?;
        let last_epoch = row.fencing_epoch;
        let stored = decode_run(row)?;
        let observed_at = database_now(&mut transaction, "lease acquisition clock").await?;

        if let Some(lease) = stored.lease() {
            if lease.fence().attempt_id() == attempt_id {
                if observed_at < lease.renewed_at() {
                    return Err(StoreError::DatabaseClockRegression);
                }
                if observed_at >= lease.expires_at() {
                    return Err(StoreError::LeaseExpired);
                }
                transaction.commit().await.map_err(|source| {
                    StoreError::database("idempotent lease acquisition commit", source)
                })?;
                return Ok(LeaseClaimOutcome::Idempotent(lease.clone()));
            }
            if observed_at < lease.renewed_at() {
                return Err(StoreError::DatabaseClockRegression);
            }
        }

        validate_runnable(&stored)?;

        if !supersede
            && stored
                .lease()
                .is_some_and(|lease| observed_at < lease.expires_at())
        {
            return Err(StoreError::LeaseHeld);
        }
        if !supersede && observed_at < scheduler_available_at(&stored)? {
            return Err(StoreError::RunNotYetAvailable);
        }
        if last_epoch == i64::MAX {
            return Err(StoreError::FencingEpochExhausted);
        }

        let next_epoch = last_epoch
            .checked_add(1)
            .and_then(|value| u64::try_from(value).ok())
            .and_then(|value| FencingEpoch::new(value).ok())
            .ok_or(StoreError::FencingEpochExhausted)?;
        let expires_at = add_duration(observed_at, self.options.lease_duration)?;
        let observed_db = to_database_time(observed_at)?;
        let expires_db = to_database_time(expires_at)?;

        let updated = query(
            r"
UPDATE stateknot.runs
SET fencing_epoch = $3,
    lease_attempt_id = $4,
    lease_acquired_at = $5,
    lease_renewed_at = $5,
    lease_expires_at = $6,
    scheduler_not_before = NULL,
    updated_at = $5
WHERE tenant_id = $1 AND run_id = $2 AND fencing_epoch = $7
",
        )
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(i64::try_from(next_epoch.get()).map_err(|_| StoreError::FencingEpochExhausted)?)
        .bind(*attempt_id.as_uuid())
        .bind(observed_db)
        .bind(expires_db)
        .bind(last_epoch)
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("lease acquisition update", source))?
        .rows_affected();
        if updated != 1 {
            return Err(StoreError::corrupt("lease acquisition row count"));
        }

        let lease = RunLease::new(
            RunFence::new(tenant_id.clone(), run_id, attempt_id, next_epoch),
            observed_at,
            expires_at,
        )
        .map_err(|_| StoreError::corrupt("claimed lease"))?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("lease acquisition commit", source))?;
        Ok(LeaseClaimOutcome::Claimed(lease))
    }

    /// Renews an exact live fence to a caller-stable desired exclusive expiry.
    ///
    /// Retrying the same desired expiry returns [`LeaseRenewalOutcome::Idempotent`].
    /// The expiry must strictly extend the current value and remain within the
    /// configured horizon from the database observation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for stale/expired ownership, unsafe expiry, missing
    /// run, corruption, or database failure.
    pub async fn renew_lease(
        &self,
        fence: &RunFence,
        desired_expires_at: Timestamp,
    ) -> Result<LeaseRenewalOutcome, StoreError> {
        let mut transaction = self.begin_mutation("lease renewal").await?;
        let row = fetch_locked_run_row(&mut transaction, fence.tenant_id(), fence.run_id()).await?;
        let stored = decode_run(row)?;
        let observed_at = database_now(&mut transaction, "lease renewal clock").await?;
        let lease = stored.lease().ok_or(StoreError::NoActiveLease)?;
        if lease.fence() != fence {
            return Err(StoreError::StaleFence);
        }

        if desired_expires_at == lease.expires_at() {
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent lease renewal commit", source)
            })?;
            return Ok(LeaseRenewalOutcome::Idempotent(lease.clone()));
        }
        if observed_at < lease.renewed_at() {
            return Err(StoreError::DatabaseClockRegression);
        }
        if observed_at >= lease.expires_at() {
            return Err(StoreError::LeaseExpired);
        }
        if desired_expires_at < lease.expires_at() {
            return Err(StoreError::LeaseExpiryNotExtended);
        }
        let maximum = add_duration(observed_at, self.options.maximum_lease_horizon)?;
        if desired_expires_at > maximum {
            return Err(StoreError::LeaseHorizonExceeded);
        }

        let observed_db = to_database_time(observed_at)?;
        let desired_db = to_database_time(desired_expires_at)?;
        let previous_db = to_database_time(lease.expires_at())?;
        let updated = query(
            r"
UPDATE stateknot.runs
SET lease_renewed_at = $5,
    lease_expires_at = $6,
    updated_at = $5
WHERE tenant_id = $1
  AND run_id = $2
  AND lease_attempt_id = $3
  AND fencing_epoch = $4
  AND lease_expires_at = $7
  AND lease_expires_at > $5
",
        )
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .bind(*fence.attempt_id().as_uuid())
        .bind(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?)
        .bind(observed_db)
        .bind(desired_db)
        .bind(previous_db)
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("lease renewal update", source))?
        .rows_affected();
        if updated != 1 {
            return Err(StoreError::StaleFence);
        }

        let renewed = lease
            .renewed(fence, observed_at, desired_expires_at)
            .map_err(|_| StoreError::corrupt("renewed lease"))?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("lease renewal commit", source))?;
        Ok(LeaseRenewalOutcome::Renewed(renewed))
    }

    /// Observes one exact fence against the current database clock.
    ///
    /// Unlike an idempotent renewal result, this read cannot confirm an already
    /// expired historical write. It verifies the complete run scope and current
    /// wait projection, runnable lifecycle, quarantine state, exact attempt and
    /// epoch, and exclusive lease expiry in one repeatable-read transaction.
    /// Success proves liveness only at [`LiveLeaseObservation::observed_at`].
    ///
    /// # Errors
    ///
    /// Returns explicit not-found, lifecycle, quarantine, stale-fence,
    /// lease-expiry, corruption, clock, or database failures.
    pub async fn observe_live_lease(
        &self,
        fence: &RunFence,
    ) -> Result<LiveLeaseObservation, StoreError> {
        let mut transaction = self.begin_repeatable_read("live lease observation").await?;
        let row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(fence.tenant_id().as_str())
            .bind(*fence.run_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("live lease observation load", source))?
            .ok_or(StoreError::RunNotFound)?;
        let stored = decode_run(row)?;
        if stored.lifecycle().provenance().tenant_id() != fence.tenant_id()
            || stored.lifecycle().provenance().run_id() != fence.run_id()
        {
            return Err(StoreError::corrupt("live lease observation scope"));
        }
        verify_current_wait_set(&mut transaction, &stored).await?;
        validate_runnable(&stored)?;
        let observed_at = database_now(&mut transaction, "live lease observation clock").await?;
        authorize_worker(&stored, fence, observed_at)?;
        let lease = stored.lease().cloned().ok_or(StoreError::NoActiveLease)?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("live lease observation commit", source))?;
        Ok(LiveLeaseObservation { lease, observed_at })
    }

    /// Releases an active lease under its exact fence, retaining the issued epoch.
    ///
    /// A retry after successful release is idempotent only while no successor
    /// epoch has been issued.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for a stale fence, missing run, corruption, or
    /// database failure.
    pub async fn release_lease(&self, fence: &RunFence) -> Result<LeaseReleaseOutcome, StoreError> {
        let mut transaction = self.begin_mutation("lease release").await?;
        let row = fetch_locked_run_row(&mut transaction, fence.tenant_id(), fence.run_id()).await?;
        let last_epoch = row.fencing_epoch;
        let stored = decode_run(row)?;

        let Some(lease) = stored.lease() else {
            if u64::try_from(last_epoch).ok() == Some(fence.epoch().get()) {
                transaction.commit().await.map_err(|source| {
                    StoreError::database("idempotent lease release commit", source)
                })?;
                return Ok(LeaseReleaseOutcome::Idempotent);
            }
            return Err(StoreError::StaleFence);
        };
        if lease.fence() != fence {
            return Err(StoreError::StaleFence);
        }

        let observed_at = database_now(&mut transaction, "lease release clock").await?;
        if observed_at < lease.renewed_at() {
            return Err(StoreError::DatabaseClockRegression);
        }
        let updated = query(
            r"
UPDATE stateknot.runs
SET lease_attempt_id = NULL,
    lease_acquired_at = NULL,
    lease_renewed_at = NULL,
    lease_expires_at = NULL,
    scheduler_ready_at = CASE
        WHEN lifecycle_status IN ('pending', 'active', 'cancellation_requested') THEN $5
        ELSE NULL
    END,
    scheduler_not_before = NULL,
    updated_at = $5
WHERE tenant_id = $1
  AND run_id = $2
  AND lease_attempt_id = $3
  AND fencing_epoch = $4
",
        )
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .bind(*fence.attempt_id().as_uuid())
        .bind(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?)
        .bind(to_database_time(observed_at)?)
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("lease release update", source))?
        .rows_affected();
        if updated != 1 {
            return Err(StoreError::StaleFence);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("lease release commit", source))?;
        Ok(LeaseReleaseOutcome::Released)
    }

    /// Appends a trusted control-plane event and projection atomically.
    ///
    /// # Errors
    ///
    /// Rejects worker sources and returns explicit conflict, integrity, or
    /// database failures.
    pub async fn append_control_plane(
        &self,
        append: JournalAppend,
        projection: RunProjection,
    ) -> Result<AppendOutcome, StoreError> {
        if append.worker_fence().is_some() {
            return Err(StoreError::WrongAppendAuthority);
        }
        self.append(append, projection, AppendAuthority::ControlPlane)
            .await
    }

    /// Appends a worker event under its exact unexpired database fence.
    ///
    /// An identical already-committed event is returned before stale-head or
    /// stale-fence rejection so a lost acknowledgement can converge safely.
    ///
    /// # Errors
    ///
    /// Rejects control-plane sources and returns explicit fencing, conflict,
    /// integrity, or database failures.
    pub async fn append_worker(
        &self,
        append: JournalAppend,
        projection: RunProjection,
    ) -> Result<AppendOutcome, StoreError> {
        if append.worker_fence().is_none() {
            return Err(StoreError::WrongAppendAuthority);
        }
        self.append(append, projection, AppendAuthority::Worker)
            .await
    }

    /// Atomically prepares one fenced logical tool invocation and journal event.
    ///
    /// The activation's exact base checkpoint must still be the locked run's
    /// current checkpoint. The database rechecks the worker fence while inserting
    /// the event, intent, revision zero, and updated run head. An identical event
    /// retry converges even after the lease or journal head has advanced.
    ///
    /// # Errors
    ///
    /// Returns explicit authority, lifecycle, idempotency, checkpoint,
    /// activation, fencing, integrity, transition, or database failures.
    pub async fn prepare_tool_invocation(
        &self,
        append: JournalAppend,
        intent: ToolInvocationIntent,
    ) -> Result<ToolInvocationCommitOutcome, StoreError> {
        Box::pin(self.prepare_tool_invocation_inner(append, intent)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn prepare_tool_invocation_inner(
        &self,
        append: JournalAppend,
        intent: ToolInvocationIntent,
    ) -> Result<ToolInvocationCommitOutcome, StoreError> {
        let fence = append
            .worker_fence()
            .cloned()
            .ok_or(StoreError::WrongAppendAuthority)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        if intent.tenant_id() != &tenant_id || intent.run_id() != run_id {
            return Err(StoreError::ToolInvocationCommitConflict);
        }

        let mut transaction = self.begin_mutation("tool invocation prepare").await?;
        let run_row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(run_row)?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("tool invocation event lookup", source))?;
        if let Some(row) = existing_event {
            let projection_digest = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "tool invocation projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            let intent_row = query_as::<_, ToolInvocationRow>(SELECT_TOOL_INVOCATION)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*intent.invocation_id().as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| {
                    StoreError::database("tool invocation idempotency intent", source)
                })?
                .ok_or(StoreError::ToolInvocationCommitConflict)?;
            let stored_intent = decode_tool_invocation_intent(&intent_row)?;
            if stored_intent != intent {
                return Err(StoreError::ToolInvocationIdConflict);
            }
            let revision_row =
                query_as::<_, ToolInvocationRevisionRow>(SELECT_TOOL_INVOCATION_REVISION_BY_ANCHOR)
                    .bind(tenant_id.as_str())
                    .bind(*run_id.as_uuid())
                    .bind(
                        i64::try_from(event.sequence().get())
                            .map_err(|_| StoreError::JournalSequenceExhausted)?,
                    )
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|source| {
                        StoreError::database("tool invocation idempotency revision", source)
                    })?
                    .ok_or(StoreError::ToolInvocationCommitConflict)?;
            if revision_row.invocation_id != *intent.invocation_id().as_uuid() {
                return Err(StoreError::ToolInvocationCommitConflict);
            }
            let invocation = decode_tool_invocation_revision(revision_row, &stored_intent)?;
            let expected = ToolInvocation::prepare(intent, event.head())
                .map_err(|_| StoreError::ToolInvocationCommitConflict)?;
            if projection_digest != Some(invocation.digest())
                || encode_tool_invocation_record(&invocation)?
                    != encode_tool_invocation_record(&expected)?
            {
                return Err(StoreError::ToolInvocationCommitConflict);
            }
            verify_tool_invocation_base_checkpoint(&mut transaction, &stored_intent).await?;
            verify_tool_invocation_anchor(&mut transaction, &invocation).await?;
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent tool invocation prepare commit", source)
            })?;
            return Ok(ToolInvocationCommitOutcome::Idempotent { event, invocation });
        }

        let existing_intent = query_as::<_, ToolInvocationRow>(SELECT_TOOL_INVOCATION)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*intent.invocation_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("tool invocation identity lookup", source))?;
        if existing_intent.is_some() {
            return Err(StoreError::ToolInvocationIdConflict);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current_checkpoint.head() != *intent.activation().base_checkpoint() {
            return Err(StoreError::StaleCheckpointHead);
        }
        if !tool_invocation_activation_is_ready(&current_checkpoint, &intent) {
            return Err(StoreError::InvalidToolInvocationActivation);
        }

        let observed_at = database_now(&mut transaction, "tool invocation prepare clock").await?;
        authorize_worker(&stored, &fence, observed_at)?;
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let invocation = ToolInvocation::prepare(intent, event.head())
            .map_err(|_| StoreError::InvalidToolInvocationTransition)?;

        insert_event(&mut transaction, &event, invocation.digest()).await?;
        insert_tool_invocation_intent(&mut transaction, &invocation, &fence).await?;
        insert_initial_tool_invocation_revision(&mut transaction, &invocation, &fence).await?;
        update_run_head(&mut transaction, &event, None).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("tool invocation prepare commit", source))?;
        Ok(ToolInvocationCommitOutcome::Committed { event, invocation })
    }

    /// Atomically advances one fenced logical tool invocation and journal event.
    ///
    /// The full durable current record is reloaded under the run and invocation
    /// locks, compared with `expected`, and passed through the core state machine.
    /// Every SQL mutation rechecks the exact live run fence. No transaction
    /// contains external tool work: `StartAttempt` commits before dispatch,
    /// while result, error, and reconciliation evidence is obtained before its
    /// corresponding outcome transaction.
    ///
    /// # Errors
    ///
    /// Returns explicit authority, lifecycle, idempotency, stale-head,
    /// checkpoint, fencing, transition, integrity, or database failures.
    pub async fn advance_tool_invocation(
        &self,
        append: JournalAppend,
        expected: &ToolInvocationHead,
        transition: ToolInvocationTransition,
    ) -> Result<ToolInvocationCommitOutcome, StoreError> {
        Box::pin(self.advance_tool_invocation_inner(append, expected, transition)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn advance_tool_invocation_inner(
        &self,
        append: JournalAppend,
        expected: &ToolInvocationHead,
        transition: ToolInvocationTransition,
    ) -> Result<ToolInvocationCommitOutcome, StoreError> {
        let fence = append
            .worker_fence()
            .cloned()
            .ok_or(StoreError::WrongAppendAuthority)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        if expected.tenant_id() != &tenant_id || expected.run_id() != run_id {
            return Err(StoreError::StaleToolInvocationHead);
        }

        let mut transaction = self.begin_mutation("tool invocation advance").await?;
        let run_row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(run_row)?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("tool invocation event lookup", source))?;
        if let Some(row) = existing_event {
            let projection_digest = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "tool invocation projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            let intent_row = query_as::<_, ToolInvocationRow>(SELECT_TOOL_INVOCATION)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*expected.invocation_id().as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| {
                    StoreError::database("tool invocation idempotency intent", source)
                })?
                .ok_or(StoreError::ToolInvocationCommitConflict)?;
            let intent = decode_tool_invocation_intent(&intent_row)?;
            let previous_row = load_tool_invocation_revision_row(
                &mut transaction,
                &tenant_id,
                run_id,
                expected.invocation_id(),
                expected.revision(),
            )
            .await
            .map_err(|error| match error {
                StoreError::ToolInvocationNotFound => StoreError::ToolInvocationCommitConflict,
                other => other,
            })?;
            let previous = decode_tool_invocation_revision(previous_row, &intent)?;
            if previous.head() != *expected {
                return Err(StoreError::ToolInvocationCommitConflict);
            }
            let expected_invocation = previous
                .advance(transition, event.head())
                .map_err(|_| StoreError::ToolInvocationCommitConflict)?;
            let revision_row =
                query_as::<_, ToolInvocationRevisionRow>(SELECT_TOOL_INVOCATION_REVISION_BY_ANCHOR)
                    .bind(tenant_id.as_str())
                    .bind(*run_id.as_uuid())
                    .bind(
                        i64::try_from(event.sequence().get())
                            .map_err(|_| StoreError::JournalSequenceExhausted)?,
                    )
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|source| {
                        StoreError::database("tool invocation idempotency revision", source)
                    })?
                    .ok_or(StoreError::ToolInvocationCommitConflict)?;
            if revision_row.invocation_id != *expected.invocation_id().as_uuid() {
                return Err(StoreError::ToolInvocationCommitConflict);
            }
            let invocation = decode_tool_invocation_revision(revision_row, &intent)?;
            if projection_digest != Some(invocation.digest())
                || encode_tool_invocation_record(&invocation)?
                    != encode_tool_invocation_record(&expected_invocation)?
            {
                return Err(StoreError::ToolInvocationCommitConflict);
            }
            verify_tool_invocation_base_checkpoint(&mut transaction, &intent).await?;
            verify_tool_invocation_anchor(&mut transaction, &invocation).await?;
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent tool invocation advance commit", source)
            })?;
            return Ok(ToolInvocationCommitOutcome::Idempotent { event, invocation });
        }

        let intent_row = query_as::<_, ToolInvocationRow>(SELECT_TOOL_INVOCATION_FOR_UPDATE)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*expected.invocation_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("tool invocation row lock", source))?
            .ok_or(StoreError::ToolInvocationNotFound)?;
        let intent = decode_tool_invocation_intent(&intent_row)?;
        let current_revision = nonnegative_tool_invocation_revision(intent_row.current_revision)?;
        let current_row = load_tool_invocation_revision_row(
            &mut transaction,
            &tenant_id,
            run_id,
            expected.invocation_id(),
            current_revision,
        )
        .await?;
        let current = decode_tool_invocation_revision(current_row, &intent)?;
        validate_tool_invocation_current_projection(&intent_row, &current)?;
        if current.head() != *expected {
            return Err(StoreError::StaleToolInvocationHead);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        validate_tool_invocation_transition_lifecycle(&stored, transition.kind())?;
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current_checkpoint.head() != *intent.activation().base_checkpoint() {
            return Err(StoreError::StaleCheckpointHead);
        }
        if !tool_invocation_activation_is_ready(&current_checkpoint, &intent) {
            return Err(StoreError::corrupt("tool invocation activation"));
        }

        let observed_at = database_now(&mut transaction, "tool invocation advance clock").await?;
        authorize_worker(&stored, &fence, observed_at)?;
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let invocation = current
            .advance(transition, event.head())
            .map_err(|_| StoreError::InvalidToolInvocationTransition)?;

        insert_event(&mut transaction, &event, invocation.digest()).await?;
        insert_successor_tool_invocation_revision(&mut transaction, &invocation, expected, &fence)
            .await?;
        update_tool_invocation_current(&mut transaction, &invocation, expected, &fence).await?;
        update_run_head(&mut transaction, &event, None).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("tool invocation advance commit", source))?;
        Ok(ToolInvocationCommitOutcome::Committed { event, invocation })
    }

    /// Atomically prepares one fenced logical model invocation and journal event.
    ///
    /// The exact activation checkpoint must still be current and ready. No
    /// provider I/O belongs in this transaction; dispatch starts only after the
    /// caller separately commits a `StartAttempt` revision.
    ///
    /// # Errors
    ///
    /// Returns explicit authority, lifecycle, idempotency, checkpoint,
    /// activation, fencing, integrity, transition, or database failures.
    pub async fn prepare_model_invocation(
        &self,
        append: JournalAppend,
        intent: ModelInvocationIntent,
    ) -> Result<ModelInvocationCommitOutcome, StoreError> {
        Box::pin(self.prepare_model_invocation_inner(append, intent)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn prepare_model_invocation_inner(
        &self,
        append: JournalAppend,
        intent: ModelInvocationIntent,
    ) -> Result<ModelInvocationCommitOutcome, StoreError> {
        let fence = append
            .worker_fence()
            .cloned()
            .ok_or(StoreError::WrongAppendAuthority)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        if intent.tenant_id() != &tenant_id || intent.run_id() != run_id {
            return Err(StoreError::ModelInvocationCommitConflict);
        }

        let mut transaction = self.begin_mutation("model invocation prepare").await?;
        let run_row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(run_row)?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("model invocation event lookup", source))?;
        if let Some(row) = existing_event {
            let projection_digest = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "model invocation projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            let intent_row = query_as::<_, ModelInvocationRow>(SELECT_MODEL_INVOCATION)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*intent.invocation_id().as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| {
                    StoreError::database("model invocation idempotency intent", source)
                })?
                .ok_or(StoreError::ModelInvocationCommitConflict)?;
            let stored_intent = decode_model_invocation_intent(&intent_row)?;
            if stored_intent != intent {
                return Err(StoreError::ModelInvocationIdConflict);
            }
            let revision_row = query_as::<_, ModelInvocationRevisionRow>(
                SELECT_MODEL_INVOCATION_REVISION_BY_ANCHOR,
            )
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(
                i64::try_from(event.sequence().get())
                    .map_err(|_| StoreError::JournalSequenceExhausted)?,
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| {
                StoreError::database("model invocation idempotency revision", source)
            })?
            .ok_or(StoreError::ModelInvocationCommitConflict)?;
            if revision_row.invocation_id != *intent.invocation_id().as_uuid() {
                return Err(StoreError::ModelInvocationCommitConflict);
            }
            let invocation = decode_model_invocation_revision(revision_row, &stored_intent)?;
            let expected = ModelInvocation::prepare(intent, event.head())
                .map_err(|_| StoreError::ModelInvocationCommitConflict)?;
            if projection_digest != Some(invocation.digest())
                || encode_model_invocation_record(&invocation)?
                    != encode_model_invocation_record(&expected)?
            {
                return Err(StoreError::ModelInvocationCommitConflict);
            }
            verify_model_invocation_base_checkpoint(&mut transaction, &stored_intent).await?;
            verify_model_invocation_anchor(&mut transaction, &invocation).await?;
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent model invocation prepare commit", source)
            })?;
            return Ok(ModelInvocationCommitOutcome::Idempotent { event, invocation });
        }

        let existing_intent = query_as::<_, ModelInvocationRow>(SELECT_MODEL_INVOCATION)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*intent.invocation_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("model invocation identity lookup", source))?;
        if existing_intent.is_some() {
            return Err(StoreError::ModelInvocationIdConflict);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current_checkpoint.head() != *intent.activation().base_checkpoint() {
            return Err(StoreError::StaleCheckpointHead);
        }
        if !model_invocation_activation_is_ready(&current_checkpoint, &intent) {
            return Err(StoreError::InvalidModelInvocationActivation);
        }

        let observed_at = database_now(&mut transaction, "model invocation prepare clock").await?;
        authorize_worker(&stored, &fence, observed_at)?;
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let invocation = ModelInvocation::prepare(intent, event.head())
            .map_err(|_| StoreError::InvalidModelInvocationTransition)?;

        insert_event(&mut transaction, &event, invocation.digest()).await?;
        insert_model_invocation_intent(&mut transaction, &invocation, &fence).await?;
        insert_initial_model_invocation_revision(&mut transaction, &invocation, &fence).await?;
        update_run_head(&mut transaction, &event, None).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("model invocation prepare commit", source))?;
        Ok(ModelInvocationCommitOutcome::Committed { event, invocation })
    }

    /// Atomically advances one fenced logical model invocation and journal event.
    ///
    /// The complete current record is restored under run and invocation locks,
    /// compared with `expected`, and passed through the core state machine. A
    /// `StartAttempt` revision and global attempt claim commit before exactly one
    /// provider exchange; a complete response or failure commits afterward.
    ///
    /// # Errors
    ///
    /// Returns explicit authority, lifecycle, idempotency, stale-head,
    /// checkpoint, fencing, transition, integrity, or database failures.
    pub async fn advance_model_invocation(
        &self,
        append: JournalAppend,
        expected: &ModelInvocationHead,
        transition: ModelInvocationTransition,
    ) -> Result<ModelInvocationCommitOutcome, StoreError> {
        Box::pin(self.advance_model_invocation_inner(append, expected, transition)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn advance_model_invocation_inner(
        &self,
        append: JournalAppend,
        expected: &ModelInvocationHead,
        transition: ModelInvocationTransition,
    ) -> Result<ModelInvocationCommitOutcome, StoreError> {
        let fence = append
            .worker_fence()
            .cloned()
            .ok_or(StoreError::WrongAppendAuthority)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        if expected.tenant_id() != &tenant_id || expected.run_id() != run_id {
            return Err(StoreError::StaleModelInvocationHead);
        }

        let mut transaction = self.begin_mutation("model invocation advance").await?;
        let run_row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(run_row)?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("model invocation event lookup", source))?;
        if let Some(row) = existing_event {
            let projection_digest = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "model invocation projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            let intent_row = query_as::<_, ModelInvocationRow>(SELECT_MODEL_INVOCATION)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*expected.invocation_id().as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| {
                    StoreError::database("model invocation idempotency intent", source)
                })?
                .ok_or(StoreError::ModelInvocationCommitConflict)?;
            let intent = decode_model_invocation_intent(&intent_row)?;
            let previous_row = load_model_invocation_revision_row(
                &mut transaction,
                &tenant_id,
                run_id,
                expected.invocation_id(),
                expected.revision(),
            )
            .await
            .map_err(|error| match error {
                StoreError::ModelInvocationNotFound => StoreError::ModelInvocationCommitConflict,
                other => other,
            })?;
            let previous = decode_model_invocation_revision(previous_row, &intent)?;
            if previous.head() != *expected {
                return Err(StoreError::ModelInvocationCommitConflict);
            }
            let expected_invocation = previous
                .advance(transition, event.head())
                .map_err(|_| StoreError::ModelInvocationCommitConflict)?;
            let revision_row = query_as::<_, ModelInvocationRevisionRow>(
                SELECT_MODEL_INVOCATION_REVISION_BY_ANCHOR,
            )
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(
                i64::try_from(event.sequence().get())
                    .map_err(|_| StoreError::JournalSequenceExhausted)?,
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| {
                StoreError::database("model invocation idempotency revision", source)
            })?
            .ok_or(StoreError::ModelInvocationCommitConflict)?;
            if revision_row.invocation_id != *expected.invocation_id().as_uuid() {
                return Err(StoreError::ModelInvocationCommitConflict);
            }
            let invocation = decode_model_invocation_revision(revision_row, &intent)?;
            if projection_digest != Some(invocation.digest())
                || encode_model_invocation_record(&invocation)?
                    != encode_model_invocation_record(&expected_invocation)?
            {
                return Err(StoreError::ModelInvocationCommitConflict);
            }
            verify_model_invocation_base_checkpoint(&mut transaction, &intent).await?;
            verify_model_invocation_anchor(&mut transaction, &invocation).await?;
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent model invocation advance commit", source)
            })?;
            return Ok(ModelInvocationCommitOutcome::Idempotent { event, invocation });
        }

        let intent_row = query_as::<_, ModelInvocationRow>(SELECT_MODEL_INVOCATION_FOR_UPDATE)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*expected.invocation_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("model invocation row lock", source))?
            .ok_or(StoreError::ModelInvocationNotFound)?;
        let intent = decode_model_invocation_intent(&intent_row)?;
        let current_revision = nonnegative_model_invocation_revision(intent_row.current_revision)?;
        let current_row = load_model_invocation_revision_row(
            &mut transaction,
            &tenant_id,
            run_id,
            expected.invocation_id(),
            current_revision,
        )
        .await?;
        let current = decode_model_invocation_revision(current_row, &intent)?;
        validate_model_invocation_current_projection(&intent_row, &current)?;
        if current.head() != *expected {
            return Err(StoreError::StaleModelInvocationHead);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        validate_model_invocation_transition_lifecycle(&stored, transition.kind())?;
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current_checkpoint.head() != *intent.activation().base_checkpoint() {
            return Err(StoreError::StaleCheckpointHead);
        }
        if !model_invocation_activation_is_ready(&current_checkpoint, &intent) {
            return Err(StoreError::corrupt("model invocation activation"));
        }

        let observed_at = database_now(&mut transaction, "model invocation advance clock").await?;
        authorize_worker(&stored, &fence, observed_at)?;
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let invocation = current
            .advance(transition, event.head())
            .map_err(|_| StoreError::InvalidModelInvocationTransition)?;

        insert_event(&mut transaction, &event, invocation.digest()).await?;
        insert_successor_model_invocation_revision(&mut transaction, &invocation, expected, &fence)
            .await?;
        update_model_invocation_current(&mut transaction, &invocation, expected, &fence).await?;
        update_run_head(&mut transaction, &event, None).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("model invocation advance commit", source))?;
        Ok(ModelInvocationCommitOutcome::Committed { event, invocation })
    }

    /// Durably starts one physical node attempt before user node code executes.
    ///
    /// The worker event, run-wide physical-attempt claim, immutable start, and
    /// run journal head commit in one fenced transaction. A retry may supersede
    /// an unfinished attempt only under a higher fencing epoch, and may follow
    /// a failure only after its explicit database-observed safe-after delay.
    ///
    /// For a start call, only [`NodeAttemptCommitOutcome::Committed`] grants
    /// this caller permission to launch node code. `Idempotent` proves the exact
    /// start already exists but cannot distinguish a lost acknowledgement from
    /// a concurrent owner; treat it as in flight and never launch from that
    /// outcome alone. An orphaned start is recovered under a higher run fence.
    ///
    /// # Errors
    ///
    /// Returns explicit authority, activation, retry-history/limit, lifecycle,
    /// checkpoint, journal, fencing, idempotency, integrity, or database errors.
    pub async fn start_node_attempt(
        &self,
        append: JournalAppend,
        activation: NodeActivation,
        attempt_id: AttemptId,
    ) -> Result<NodeAttemptCommitOutcome, StoreError> {
        Box::pin(self.start_node_attempt_inner(append, activation, attempt_id)).await
    }

    /// Durably starts one node selected by a verified recovery plan.
    ///
    /// This is the production handoff from deterministic replay to execution:
    /// the plan must bind the same tenant/run, checkpoint and worker fence; the
    /// selected node must be classified as dispatchable; and the append's exact
    /// journal expectation cannot precede the plan observation. The ordinary
    /// node-attempt transaction then reloads current state and the latest
    /// durable predecessor, verifies the proposed successor and retry timing
    /// with the database clock, repeats the live-fence predicate in SQL,
    /// commits the start, and only then returns.
    ///
    /// Multiple ready siblings may use one plan one at a time with an advancing
    /// exact journal expectation. A concurrent stale append fails explicitly
    /// and can be rebuilt from the newly committed head without rerunning the
    /// recovery planner. Retrying the identical event and physical attempt ID
    /// preserves the underlying lost-acknowledgement convergence.
    ///
    /// Only [`NodeAttemptCommitOutcome::Committed`] grants this caller a fresh
    /// launch. `Idempotent` is durable in-flight evidence, not dispatch
    /// authority; never launch node code from it alone. No user node code or
    /// external I/O runs inside this method.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidReadyNodeDispatchPlan`] for crossed plan,
    /// append, checkpoint, journal, or fence scope;
    /// [`StoreError::ReadyNodeNotDispatchable`] for completed, deferred,
    /// in-flight, failed, exhausted, or absent nodes; otherwise returns the same
    /// durable, idempotency, lifecycle, history, fencing, and database failures
    /// as [`Self::start_node_attempt`].
    pub async fn start_recovered_node_attempt(
        &self,
        append: JournalAppend,
        plan: &ReadyNodeRecoveryPlan,
        node_id: &NodeId,
        attempt_id: AttemptId,
    ) -> Result<NodeAttemptCommitOutcome, StoreError> {
        let fence = append
            .worker_fence()
            .ok_or(StoreError::InvalidReadyNodeDispatchPlan)?;
        let expected_head = append
            .expectation()
            .head()
            .ok_or(StoreError::InvalidReadyNodeDispatchPlan)?;
        if fence != plan.fence()
            || plan.checkpoint().tenant_id() != fence.tenant_id()
            || plan.checkpoint().run_id() != fence.run_id()
            || plan.journal_head().tenant_id() != fence.tenant_id()
            || plan.journal_head().run_id() != fence.run_id()
            || expected_head.tenant_id() != fence.tenant_id()
            || expected_head.run_id() != fence.run_id()
            || expected_head.sequence() < plan.journal_head().sequence()
            || expected_head.recorded_at() < plan.journal_head().recorded_at()
        {
            return Err(StoreError::InvalidReadyNodeDispatchPlan);
        }
        let decision = plan
            .nodes()
            .iter()
            .find(|decision| decision.activation().node_id() == node_id)
            .ok_or(StoreError::ReadyNodeNotDispatchable)?;
        if decision.dispatch_reason().is_none()
            || decision.activation().base_checkpoint() != &plan.checkpoint().head()
        {
            return Err(StoreError::ReadyNodeNotDispatchable);
        }
        self.start_node_attempt(append, decision.activation().clone(), attempt_id)
            .await
    }

    /// Durably suppresses scheduler claims until a recovery plan's next retry.
    ///
    /// The plan must contain at least one deferred node and no dispatchable,
    /// in-flight, failed, or exhausted node. Completed siblings are permitted
    /// because their immutable results remain pending while the delayed sibling
    /// waits. The transaction revalidates the exact checkpoint, journal head,
    /// live worker fence, lifecycle, and database clock, then preserves queue
    /// age, records the inclusive retry gate, and releases the lease atomically.
    ///
    /// Retrying after a lost acknowledgement returns
    /// [`DelayedRetryScheduleOutcome::Idempotent`] only while the same fencing
    /// epoch, checkpoint, journal head, and retry boundary still own the run. If
    /// database time reaches the boundary before the update, `Due` leaves the
    /// lease in place so the caller can rebuild the plan without release/reclaim
    /// churn.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidDelayedRetryPlan`] for a crossed or
    /// immediately actionable plan, and otherwise returns explicit stale
    /// checkpoint, journal, lifecycle, fencing, clock, corruption, or database
    /// failures.
    #[allow(clippy::too_many_lines)]
    pub async fn schedule_delayed_retry_wakeup(
        &self,
        plan: &ReadyNodeRecoveryPlan,
    ) -> Result<DelayedRetryScheduleOutcome, StoreError> {
        let not_before = plan
            .earliest_deferred_at()
            .ok_or(StoreError::InvalidDelayedRetryPlan)?;
        if plan.nodes().is_empty()
            || plan.nodes().iter().any(|node| {
                !matches!(
                    node.kind(),
                    RecoveryNodeKind::Completed | RecoveryNodeKind::Deferred
                )
            })
            || not_before <= plan.observed_at()
            || plan.checkpoint().tenant_id() != plan.fence().tenant_id()
            || plan.checkpoint().run_id() != plan.fence().run_id()
            || plan.journal_head().tenant_id() != plan.fence().tenant_id()
            || plan.journal_head().run_id() != plan.fence().run_id()
        {
            return Err(StoreError::InvalidDelayedRetryPlan);
        }

        let fence = plan.fence();
        let mut transaction = self.begin_mutation("delayed retry wakeup").await?;
        let row = fetch_locked_run_row(&mut transaction, fence.tenant_id(), fence.run_id()).await?;
        let stored = decode_run(row)?;
        validate_runnable(&stored)?;
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        let current_checkpoint = load_locked_current_checkpoint(
            &mut transaction,
            &stored,
            fence.tenant_id(),
            fence.run_id(),
        )
        .await?
        .ok_or(StoreError::ReadyNodeRecoveryCheckpointMissing)?;
        if current_checkpoint != *plan.checkpoint() {
            return Err(StoreError::StaleCheckpointHead);
        }
        if stored.journal_head() != Some(plan.journal_head()) {
            return Err(StoreError::StaleJournalHead);
        }

        if stored.lease().is_none()
            && stored.last_fencing_epoch() == Some(fence.epoch())
            && stored.scheduler_not_before() == Some(not_before)
        {
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent delayed retry wakeup commit", source)
            })?;
            return Ok(DelayedRetryScheduleOutcome::Idempotent { not_before });
        }

        let observed_at = database_now(&mut transaction, "delayed retry wakeup clock").await?;
        if observed_at < plan.observed_at() {
            return Err(StoreError::DatabaseClockRegression);
        }
        authorize_worker(&stored, fence, observed_at)?;
        if observed_at >= not_before {
            transaction.commit().await.map_err(|source| {
                StoreError::database("due delayed retry wakeup commit", source)
            })?;
            return Ok(DelayedRetryScheduleOutcome::Due { not_before });
        }

        let epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
        let journal_sequence = i64::try_from(plan.journal_head().sequence().get())
            .map_err(|_| StoreError::JournalSequenceExhausted)?;
        let checkpoint_superstep = i64::try_from(plan.checkpoint().superstep().get())
            .map_err(|_| StoreError::StaleCheckpointHead)?;
        let scheduled_at = query_scalar::<_, DateTime<Utc>>(
            r"
WITH observation AS MATERIALIZED (
    SELECT clock_timestamp() AS observed_at
)
UPDATE stateknot.runs
SET lease_attempt_id = NULL,
    lease_acquired_at = NULL,
    lease_renewed_at = NULL,
    lease_expires_at = NULL,
    scheduler_not_before = $5,
    updated_at = observation.observed_at
FROM observation
WHERE tenant_id = $1
  AND run_id = $2
  AND lease_attempt_id = $3
  AND fencing_epoch = $4
  AND lease_expires_at > observation.observed_at
  AND observation.observed_at < $5
  AND journal_sequence = $6
  AND journal_event_id = $7
  AND journal_recorded_at = $8
  AND journal_digest = $9
  AND checkpoint_id = $10
  AND checkpoint_superstep = $11
  AND checkpoint_digest = $12
  AND lifecycle_status = 'active'
  AND quarantined_at IS NULL
RETURNING observation.observed_at
",
        )
        .bind(fence.tenant_id().as_str())
        .bind(*fence.run_id().as_uuid())
        .bind(*fence.attempt_id().as_uuid())
        .bind(epoch)
        .bind(to_database_time(not_before)?)
        .bind(journal_sequence)
        .bind(*plan.journal_head().event_id().as_uuid())
        .bind(to_database_time(plan.journal_head().recorded_at())?)
        .bind(plan.journal_head().digest().as_bytes())
        .bind(*plan.checkpoint().checkpoint_id().as_uuid())
        .bind(checkpoint_superstep)
        .bind(plan.checkpoint().digest().as_bytes())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("delayed retry wakeup update", source))?;
        if scheduled_at.is_none() {
            let final_observed_at =
                database_now(&mut transaction, "delayed retry wakeup final clock").await?;
            if final_observed_at < observed_at {
                return Err(StoreError::DatabaseClockRegression);
            }
            authorize_worker(&stored, fence, final_observed_at)?;
            if final_observed_at >= not_before {
                transaction.commit().await.map_err(|source| {
                    StoreError::database("due delayed retry wakeup commit", source)
                })?;
                return Ok(DelayedRetryScheduleOutcome::Due { not_before });
            }
            return Err(StoreError::corrupt("delayed retry wakeup row count"));
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("delayed retry wakeup commit", source))?;
        Ok(DelayedRetryScheduleOutcome::Scheduled { not_before })
    }

    #[allow(clippy::too_many_lines)]
    async fn start_node_attempt_inner(
        &self,
        append: JournalAppend,
        activation: NodeActivation,
        attempt_id: AttemptId,
    ) -> Result<NodeAttemptCommitOutcome, StoreError> {
        let fence = append
            .worker_fence()
            .cloned()
            .ok_or(StoreError::WrongAppendAuthority)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        if activation.tenant_id() != &tenant_id || activation.run_id() != run_id {
            return Err(StoreError::NodeAttemptCommitConflict);
        }

        let mut transaction = self.begin_mutation("node attempt start").await?;
        let run_row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(run_row)?;
        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("node attempt start event lookup", source))?;
        if let Some(row) = existing_event {
            let projection_digest = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "node attempt start projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            let expected = NodeAttemptStart::new(activation, attempt_id, fence, event.head())
                .map_err(|_| StoreError::NodeAttemptCommitConflict)?;
            let attempt =
                load_node_attempt_record(&mut transaction, &tenant_id, &run_id, attempt_id)
                    .await?
                    .ok_or(StoreError::NodeAttemptCommitConflict)?;
            if projection_digest != Some(expected.digest())
                || encode_node_attempt_start(attempt.start())?
                    != encode_node_attempt_start(&expected)?
            {
                return Err(StoreError::NodeAttemptCommitConflict);
            }
            verify_node_attempt(&mut transaction, &attempt).await?;
            transaction
                .commit()
                .await
                .map_err(|source| StoreError::database("node attempt start retry", source))?;
            return Ok(NodeAttemptCommitOutcome::Idempotent { event, attempt });
        }

        if load_node_attempt_record(&mut transaction, &tenant_id, &run_id, attempt_id)
            .await?
            .is_some()
        {
            return Err(StoreError::NodeAttemptIdConflict);
        }
        if load_pending_node_result_row(&mut transaction, &activation)
            .await?
            .is_some()
        {
            return Err(StoreError::InvalidNodeAttemptTransition);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current_checkpoint.head() != *activation.base_checkpoint() {
            return Err(StoreError::StaleCheckpointHead);
        }
        if !node_attempt_activation_is_ready(&current_checkpoint, &activation) {
            return Err(StoreError::InvalidNodeAttemptActivation);
        }

        let attempt_count = count_node_attempts(&mut transaction, &activation).await?;
        if attempt_count > ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
            return Err(StoreError::corrupt(
                "node attempt history exceeds hard limit",
            ));
        }
        if attempt_count == ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
            return Err(StoreError::NodeAttemptLimitExceeded);
        }
        let previous = load_latest_locked_node_attempt(&mut transaction, &activation).await?;
        if let Some(previous) = previous.as_ref() {
            verify_node_attempt(&mut transaction, previous).await?;
        }
        let observed_at = database_now(&mut transaction, "node attempt start clock").await?;
        authorize_worker(&stored, &fence, observed_at)?;
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let start = NodeAttemptStart::new(activation.clone(), attempt_id, fence, event.head())
            .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
        let mut verifier = previous.map_or_else(
            NodeAttemptHistoryVerifier::new,
            NodeAttemptHistoryVerifier::after,
        );
        verifier
            .verify_next(&NodeAttempt::executing(start.clone()))
            .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
        reject_reused_node_worker_attempt(&mut transaction, &activation, start.fence()).await?;

        insert_event(&mut transaction, &event, start.digest()).await?;
        insert_node_attempt_claim(&mut transaction, &start).await?;
        insert_node_attempt_start(&mut transaction, &start).await?;
        update_run_head(&mut transaction, &event, None).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("node attempt start commit", source))?;
        Ok(NodeAttemptCommitOutcome::Committed {
            event,
            attempt: NodeAttempt::executing(start),
        })
    }

    /// Atomically records terminal public-safe failure evidence for an attempt.
    ///
    /// The worker event, immutable completion, and run journal head commit in
    /// one transaction. The failure must name the new event as its direct cause
    /// and may authorize retry only with `Never` or `SafeAfter` semantics.
    ///
    /// # Errors
    ///
    /// Returns explicit authority, stale-start, terminal-state, lifecycle,
    /// checkpoint, journal, fencing, causation, integrity, or database errors.
    pub async fn fail_node_attempt(
        &self,
        append: JournalAppend,
        expected: &NodeAttemptStartHead,
        failure: Failure,
        usage: BudgetUsage,
    ) -> Result<NodeAttemptCommitOutcome, StoreError> {
        Box::pin(self.fail_node_attempt_inner(append, expected, failure, usage)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn fail_node_attempt_inner(
        &self,
        append: JournalAppend,
        expected: &NodeAttemptStartHead,
        failure: Failure,
        usage: BudgetUsage,
    ) -> Result<NodeAttemptCommitOutcome, StoreError> {
        let fence = append
            .worker_fence()
            .cloned()
            .ok_or(StoreError::WrongAppendAuthority)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        if expected.activation().tenant_id() != &tenant_id
            || expected.activation().run_id() != run_id
        {
            return Err(StoreError::NodeAttemptCommitConflict);
        }
        if &fence != expected.fence() {
            return Err(StoreError::StaleFence);
        }

        let mut transaction = self.begin_mutation("node attempt failure").await?;
        let run_row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(run_row)?;
        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("node failure event lookup", source))?;
        if let Some(row) = existing_event {
            let projection_digest = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "node failure projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            let attempt = load_node_attempt_record(
                &mut transaction,
                &tenant_id,
                &run_id,
                expected.attempt_id(),
            )
            .await?
            .ok_or(StoreError::NodeAttemptCommitConflict)?;
            if attempt.start().head() != *expected {
                return Err(StoreError::StaleNodeAttemptStart);
            }
            let expected_completion = NodeAttemptCompletion::fail(
                attempt.start(),
                failure.clone(),
                usage.clone(),
                event.head(),
            )
            .map_err(|_| StoreError::NodeAttemptCommitConflict)?;
            let completion = attempt
                .completion()
                .ok_or(StoreError::NodeAttemptCommitConflict)?;
            if projection_digest != Some(expected_completion.digest())
                || encode_node_attempt_completion(completion)?
                    != encode_node_attempt_completion(&expected_completion)?
            {
                return Err(StoreError::NodeAttemptCommitConflict);
            }
            let durable_event = verify_node_attempt(&mut transaction, &attempt).await?;
            if durable_event.head() != event.head() {
                return Err(StoreError::NodeAttemptCommitConflict);
            }
            transaction
                .commit()
                .await
                .map_err(|source| StoreError::database("node failure retry", source))?;
            return Ok(NodeAttemptCommitOutcome::Idempotent { event, attempt });
        }

        let attempt = load_locked_node_attempt(&mut transaction, expected).await?;
        if let Some(completion) = attempt.completion() {
            let expected_completion = NodeAttemptCompletion::fail(
                attempt.start(),
                failure,
                usage,
                completion.journal_head().clone(),
            )
            .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
            if encode_node_attempt_completion(completion)?
                != encode_node_attempt_completion(&expected_completion)?
            {
                return Err(StoreError::InvalidNodeAttemptTransition);
            }
            let event = verify_node_attempt(&mut transaction, &attempt).await?;
            transaction
                .commit()
                .await
                .map_err(|source| StoreError::database("node failure semantic retry", source))?;
            return Ok(NodeAttemptCommitOutcome::Idempotent { event, attempt });
        }
        if load_pending_node_result_row(&mut transaction, expected.activation())
            .await?
            .is_some()
        {
            return Err(StoreError::InvalidNodeAttemptTransition);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        validate_node_attempt_completion_lifecycle(&stored)?;
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current_checkpoint.head() != *expected.activation().base_checkpoint() {
            return Err(StoreError::StaleCheckpointHead);
        }
        if !node_attempt_activation_is_ready(&current_checkpoint, expected.activation()) {
            return Err(StoreError::InvalidNodeAttemptActivation);
        }

        let observed_at = database_now(&mut transaction, "node attempt failure clock").await?;
        authorize_worker(&stored, &fence, observed_at)?;
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let completion = NodeAttemptCompletion::fail(attempt.start(), failure, usage, event.head())
            .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
        let completed = NodeAttempt::restore(attempt.start().clone(), Some(completion.clone()))
            .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;

        insert_event(&mut transaction, &event, completion.digest()).await?;
        insert_node_attempt_completion(&mut transaction, attempt.start(), &completion).await?;
        update_run_head(&mut transaction, &event, None).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("node attempt failure commit", source))?;
        Ok(NodeAttemptCommitOutcome::Committed {
            event,
            attempt: completed,
        })
    }

    /// Atomically completes an attempt with its immutable pending node result.
    ///
    /// The worker event, pending result, exact invocation bindings, successful
    /// completion, and run journal head commit together. The event projection
    /// binds the completion digest, which in turn binds the exact result head.
    ///
    /// # Errors
    ///
    /// Returns explicit authority, stale-start, semantic-result, binding,
    /// lifecycle, checkpoint, journal, fencing, integrity, or database errors.
    pub async fn succeed_node_attempt(
        &self,
        append: JournalAppend,
        expected: &NodeAttemptStartHead,
        intent: PendingNodeResultIntent,
        usage: BudgetUsage,
    ) -> Result<NodeAttemptCommitOutcome, StoreError> {
        Box::pin(self.succeed_node_attempt_inner(append, expected, intent, usage)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn succeed_node_attempt_inner(
        &self,
        append: JournalAppend,
        expected: &NodeAttemptStartHead,
        intent: PendingNodeResultIntent,
        usage: BudgetUsage,
    ) -> Result<NodeAttemptCommitOutcome, StoreError> {
        let fence = append
            .worker_fence()
            .cloned()
            .ok_or(StoreError::WrongAppendAuthority)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        if expected.activation().tenant_id() != &tenant_id
            || expected.activation().run_id() != run_id
            || intent.activation() != expected.activation()
        {
            return Err(StoreError::NodeAttemptCommitConflict);
        }
        if &fence != expected.fence() {
            return Err(StoreError::StaleFence);
        }

        let mut transaction = self.begin_mutation("node attempt success").await?;
        let run_row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(run_row)?;
        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("node success event lookup", source))?;
        if let Some(row) = existing_event {
            let projection_digest = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "node success projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            let attempt = load_node_attempt_record(
                &mut transaction,
                &tenant_id,
                &run_id,
                expected.attempt_id(),
            )
            .await?
            .ok_or(StoreError::NodeAttemptCommitConflict)?;
            if attempt.start().head() != *expected {
                return Err(StoreError::StaleNodeAttemptStart);
            }
            let expected_result = PendingNodeResult::commit(intent.clone(), fence, event.head())
                .map_err(|_| StoreError::NodeAttemptCommitConflict)?;
            let expected_completion = NodeAttemptCompletion::succeed(
                attempt.start(),
                expected_result.head(),
                usage.clone(),
            )
            .map_err(|_| StoreError::NodeAttemptCommitConflict)?;
            let completion = attempt
                .completion()
                .ok_or(StoreError::NodeAttemptCommitConflict)?;
            let result_row = load_pending_node_result_row(&mut transaction, intent.activation())
                .await?
                .ok_or(StoreError::NodeAttemptCommitConflict)?;
            let durable_result = decode_pending_node_result(&result_row)?;
            if result_row.node_attempt_id != Some(*expected.attempt_id().as_uuid())
                || encode_pending_node_result(&durable_result)?
                    != encode_pending_node_result(&expected_result)?
                || projection_digest != Some(expected_completion.digest())
                || encode_node_attempt_completion(completion)?
                    != encode_node_attempt_completion(&expected_completion)?
            {
                return Err(StoreError::NodeAttemptCommitConflict);
            }
            let durable_event = verify_node_attempt(&mut transaction, &attempt).await?;
            if durable_event.head() != event.head() {
                return Err(StoreError::NodeAttemptCommitConflict);
            }
            transaction
                .commit()
                .await
                .map_err(|source| StoreError::database("node success retry", source))?;
            return Ok(NodeAttemptCommitOutcome::Idempotent { event, attempt });
        }

        let attempt = load_locked_node_attempt(&mut transaction, expected).await?;
        if let Some(completion) = attempt.completion() {
            let expected_result = PendingNodeResult::commit(
                intent,
                attempt.start().fence().clone(),
                completion.journal_head().clone(),
            )
            .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
            let expected_completion =
                NodeAttemptCompletion::succeed(attempt.start(), expected_result.head(), usage)
                    .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
            if encode_node_attempt_completion(completion)?
                != encode_node_attempt_completion(&expected_completion)?
            {
                return Err(StoreError::InvalidNodeAttemptTransition);
            }
            let event = verify_node_attempt(&mut transaction, &attempt).await?;
            transaction
                .commit()
                .await
                .map_err(|source| StoreError::database("node success semantic retry", source))?;
            return Ok(NodeAttemptCommitOutcome::Idempotent { event, attempt });
        }
        if let Some(row) =
            load_pending_node_result_row(&mut transaction, intent.activation()).await?
        {
            return if row.node_attempt_id.is_some() {
                Err(StoreError::corrupt("node result without completion"))
            } else {
                Err(StoreError::PendingNodeResultConflict)
            };
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        validate_node_attempt_completion_lifecycle(&stored)?;
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current_checkpoint.head() != *expected.activation().base_checkpoint() {
            return Err(StoreError::StaleCheckpointHead);
        }
        if !node_attempt_activation_is_ready(&current_checkpoint, expected.activation()) {
            return Err(StoreError::InvalidNodeAttemptActivation);
        }

        let observed_at = database_now(&mut transaction, "node attempt success clock").await?;
        authorize_worker(&stored, &fence, observed_at)?;
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let result = PendingNodeResult::commit(intent, fence.clone(), event.head())
            .map_err(|error| map_pending_node_result_commit_error(&error))?;
        let completion = NodeAttemptCompletion::succeed(attempt.start(), result.head(), usage)
            .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
        let completed = NodeAttempt::restore(attempt.start().clone(), Some(completion.clone()))
            .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;

        insert_event(&mut transaction, &event, completion.digest()).await?;
        insert_pending_node_result(&mut transaction, &result, expected.attempt_id(), &fence)
            .await?;
        insert_pending_node_result_bindings(&mut transaction, &result, &fence).await?;
        insert_node_attempt_completion(&mut transaction, attempt.start(), &completion).await?;
        update_run_head(&mut transaction, &event, None).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("node attempt success commit", source))?;
        Ok(NodeAttemptCommitOutcome::Committed {
            event,
            attempt: completed,
        })
    }

    /// Rejects the pre-v6 pending-result write path.
    ///
    /// Existing migration-5 rows remain readable, but creating a new result
    /// without first committing a physical node-attempt start would fabricate
    /// execution history. Use [`Self::start_node_attempt`] followed by
    /// [`Self::succeed_node_attempt`].
    ///
    /// # Errors
    ///
    /// Always returns [`StoreError::NodeAttemptRequired`].
    #[allow(clippy::unused_async)] // Preserve the pre-v6 async API while failing closed.
    pub async fn commit_pending_node_result(
        &self,
        _append: JournalAppend,
        _intent: PendingNodeResultIntent,
    ) -> Result<PendingNodeResultCommitOutcome, StoreError> {
        Err(StoreError::NodeAttemptRequired)
    }

    /// Atomically appends a control-plane event and commits the initial graph
    /// checkpoint.
    ///
    /// Successor checkpoints must use [`Self::append_control_plane_barrier`] so
    /// the exact complete pending-result set is consumed in the same
    /// transaction.
    ///
    /// # Errors
    ///
    /// Rejects worker sources and returns explicit idempotency, parent, journal,
    /// integrity, or database failures.
    pub async fn append_control_plane_checkpoint(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        checkpoint: CheckpointWrite,
    ) -> Result<CheckpointCommitOutcome, StoreError> {
        if append.worker_fence().is_some() {
            return Err(StoreError::WrongAppendAuthority);
        }
        if checkpoint.parent().is_some() {
            return Err(StoreError::CheckpointBarrierRequired);
        }
        self.append_checkpoint(
            append,
            projection,
            checkpoint,
            AppendAuthority::ControlPlane,
        )
        .await
    }

    /// Atomically appends a fenced worker event and commits the initial graph
    /// checkpoint.
    ///
    /// The database rechecks the exact unexpired lease while inserting the
    /// event, checkpoint, and updated run heads. Successors must use
    /// [`Self::append_worker_barrier`] so results cannot be bypassed.
    ///
    /// # Errors
    ///
    /// Rejects control-plane sources and returns explicit fencing, idempotency,
    /// parent, journal, integrity, or database failures.
    pub async fn append_worker_checkpoint(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        checkpoint: CheckpointWrite,
    ) -> Result<CheckpointCommitOutcome, StoreError> {
        if append.worker_fence().is_none() {
            return Err(StoreError::WrongAppendAuthority);
        }
        if checkpoint.parent().is_some() {
            return Err(StoreError::CheckpointBarrierRequired);
        }
        self.append_checkpoint(append, projection, checkpoint, AppendAuthority::Worker)
            .await
    }

    /// Atomically commits the first graph checkpoint while suspending an active
    /// run on a complete durable interrupt/timer set.
    ///
    /// The database clock materializes every compact lifecycle marker, so the
    /// caller supplies immutable registration intents rather than guessed
    /// registration timestamps. Successor wait barriers use the separate
    /// complete-result barrier API.
    ///
    /// # Errors
    ///
    /// Rejects worker sources, non-root checkpoints, invalid wait batches,
    /// stale lifecycle/journal state, identity conflicts, or database failures.
    pub async fn append_control_plane_initial_wait_checkpoint(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        checkpoint: CheckpointWrite,
        registrations: Vec<WaitRegistrationIntent>,
    ) -> Result<WaitCheckpointCommitOutcome, StoreError> {
        if append.worker_fence().is_some() {
            return Err(StoreError::WrongAppendAuthority);
        }
        if checkpoint.parent().is_some() {
            return Err(StoreError::CheckpointBarrierRequired);
        }
        self.append_initial_wait_checkpoint(
            append,
            expected_revision,
            checkpoint,
            registrations,
            AppendAuthority::ControlPlane,
        )
        .await
    }

    /// Fenced worker form of
    /// [`Self::append_control_plane_initial_wait_checkpoint`].
    ///
    /// # Errors
    ///
    /// Rejects control-plane sources, non-root checkpoints, expired fencing,
    /// invalid wait batches, conflicts, corruption, or database failures.
    pub async fn append_worker_initial_wait_checkpoint(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        checkpoint: CheckpointWrite,
        registrations: Vec<WaitRegistrationIntent>,
    ) -> Result<WaitCheckpointCommitOutcome, StoreError> {
        if append.worker_fence().is_none() {
            return Err(StoreError::WrongAppendAuthority);
        }
        if checkpoint.parent().is_some() {
            return Err(StoreError::CheckpointBarrierRequired);
        }
        self.append_initial_wait_checkpoint(
            append,
            expected_revision,
            checkpoint,
            registrations,
            AppendAuthority::Worker,
        )
        .await
    }

    /// Atomically commits an authenticated resolution and removes exactly one
    /// outstanding interrupt from the lifecycle wait set.
    ///
    /// Resolution is a control-plane operation: the core intent already carries
    /// the authenticated principal and bounded scope snapshot evaluated again at
    /// the authoritative database observation.
    ///
    /// # Errors
    ///
    /// Rejects worker sources, stale state, expired/unauthorized/substituted
    /// resolutions, terminal conflicts, corruption, or database failures.
    pub async fn resolve_interrupt(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        resolution: InterruptResolutionIntent,
    ) -> Result<InterruptResolutionCommitOutcome, StoreError> {
        if append.worker_fence().is_some() {
            return Err(StoreError::WrongAppendAuthority);
        }
        self.commit_interrupt_resolution(append, expected_revision, resolution)
            .await
    }

    /// Atomically records a database-clock-valid firing and removes exactly one
    /// outstanding timer from the lifecycle wait set.
    ///
    /// # Errors
    ///
    /// Rejects worker sources, early/substituted firings, stale state, terminal
    /// conflicts, corruption, or database failures.
    pub async fn fire_timer(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        firing: TimerFiringIntent,
    ) -> Result<TimerFiringCommitOutcome, StoreError> {
        if append.worker_fence().is_some() {
            return Err(StoreError::WrongAppendAuthority);
        }
        self.commit_timer_firing(append, expected_revision, firing)
            .await
    }

    /// Atomically applies cancellation/failure to a waiting run and records one
    /// abandonment fact for every outstanding interrupt and timer.
    ///
    /// # Errors
    ///
    /// Rejects worker sources, non-cancellation/failure transitions, stale
    /// state, incomplete evidence sets, corruption, or database failures.
    pub async fn append_control_plane_abandon_waits(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        transition: RunTransition,
    ) -> Result<WaitAbandonmentCommitOutcome, StoreError> {
        if append.worker_fence().is_some() {
            return Err(StoreError::WrongAppendAuthority);
        }
        self.commit_wait_abandonment(
            append,
            expected_revision,
            transition,
            AppendAuthority::ControlPlane,
        )
        .await
    }

    /// Fenced worker form of [`Self::append_control_plane_abandon_waits`].
    ///
    /// # Errors
    ///
    /// Rejects control-plane sources, invalid transitions, stale/expired
    /// fencing, incomplete evidence, corruption, or database failures.
    pub async fn append_worker_abandon_waits(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        transition: RunTransition,
    ) -> Result<WaitAbandonmentCommitOutcome, StoreError> {
        if append.worker_fence().is_none() {
            return Err(StoreError::WrongAppendAuthority);
        }
        self.commit_wait_abandonment(
            append,
            expected_revision,
            transition,
            AppendAuthority::Worker,
        )
        .await
    }

    /// Atomically consumes a complete result barrier, commits its successor
    /// checkpoint, and suspends the run on one durable wait batch.
    ///
    /// # Errors
    ///
    /// Rejects worker sources, incomplete/substituted barriers, invalid wait
    /// batches, stale lifecycle/journal/checkpoint state, corruption, or
    /// database failures.
    pub async fn append_control_plane_wait_barrier(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        barrier: CheckpointBarrier,
        registrations: Vec<WaitRegistrationIntent>,
    ) -> Result<WaitCheckpointCommitOutcome, StoreError> {
        if append.worker_fence().is_some() {
            return Err(StoreError::WrongAppendAuthority);
        }
        Box::pin(self.append_wait_barrier(
            append,
            expected_revision,
            barrier,
            registrations,
            AppendAuthority::ControlPlane,
        ))
        .await
    }

    /// Fenced worker form of [`Self::append_control_plane_wait_barrier`].
    ///
    /// # Errors
    ///
    /// Rejects control-plane sources, incomplete/substituted barriers, invalid
    /// waits, stale/expired fencing or durable state, corruption, or database
    /// failures.
    pub async fn append_worker_wait_barrier(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        barrier: CheckpointBarrier,
        registrations: Vec<WaitRegistrationIntent>,
    ) -> Result<WaitCheckpointCommitOutcome, StoreError> {
        if append.worker_fence().is_none() {
            return Err(StoreError::WrongAppendAuthority);
        }
        Box::pin(self.append_wait_barrier(
            append,
            expected_revision,
            barrier,
            registrations,
            AppendAuthority::Worker,
        ))
        .await
    }

    /// Atomically consumes a complete result barrier and commits its
    /// control-plane event and successor checkpoint.
    ///
    /// Full checkpoint and result records are verified in a repeatable-read
    /// preflight without the run lock. The mutation transaction then locks the
    /// run, rechecks every compact result identity, and inserts the event,
    /// checkpoint, append-only consumption rows, lifecycle projection, and run
    /// pointers atomically.
    ///
    /// # Errors
    ///
    /// Rejects worker sources and returns explicit barrier completeness,
    /// conflict, idempotency, lifecycle, checkpoint, journal, integrity, or
    /// database failures.
    pub async fn append_control_plane_barrier(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        barrier: CheckpointBarrier,
    ) -> Result<BarrierCommitOutcome, StoreError> {
        if append.worker_fence().is_some() {
            return Err(StoreError::WrongAppendAuthority);
        }
        Box::pin(self.append_barrier(append, projection, barrier, AppendAuthority::ControlPlane))
            .await
    }

    /// Atomically consumes a complete result barrier and commits its fenced
    /// worker event and successor checkpoint.
    ///
    /// Every write statement rechecks the exact unexpired lease against the
    /// database clock. Lease expiry at any statement rolls back the event,
    /// checkpoint, every consumption row, and both run pointers.
    ///
    /// # Errors
    ///
    /// Rejects control-plane sources and returns explicit barrier completeness,
    /// conflict, idempotency, lifecycle, checkpoint, journal, fencing,
    /// integrity, or database failures.
    pub async fn append_worker_barrier(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        barrier: CheckpointBarrier,
    ) -> Result<BarrierCommitOutcome, StoreError> {
        if append.worker_fence().is_none() {
            return Err(StoreError::WrongAppendAuthority);
        }
        Box::pin(self.append_barrier(append, projection, barrier, AppendAuthority::Worker)).await
    }

    /// Reads one repeatable-read, bounded page and verifies its hash-chain suffix.
    ///
    /// `after` is an exact trusted cursor, not only a sequence number. The first
    /// page verifies from sequence one. A final page must end at the run row's
    /// exact journal head observed in the same database snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for a missing run, crossed/stale cursor, corruption,
    /// invalid page size, or database failure.
    pub async fn load_journal_page(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        after: Option<&JournalHead>,
        page_size: JournalPageSize,
    ) -> Result<JournalPage, StoreError> {
        if let Some(head) = after {
            if head.tenant_id() != tenant_id || head.run_id() != run_id {
                return Err(StoreError::InvalidJournalCursor);
            }
        }

        let mut transaction = self.begin_repeatable_read("journal page").await?;
        let row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("journal run snapshot", source))?
            .ok_or(StoreError::RunNotFound)?;
        let run = decode_run(row)?;

        let mut verifier = if let Some(head) = after {
            let cursor_row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(
                    i64::try_from(head.sequence().get())
                        .map_err(|_| StoreError::InvalidJournalCursor)?,
                )
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("journal cursor load", source))?
                .ok_or(StoreError::InvalidJournalCursor)?;
            let cursor = decode_event(cursor_row)?;
            if cursor.head() != *head {
                return Err(StoreError::InvalidJournalCursor);
            }
            JournalChainVerifier::after(head.clone())
        } else {
            JournalChainVerifier::new()
        };

        let after_sequence = after.map_or(0_i64, |head| {
            i64::try_from(head.sequence().get()).unwrap_or(i64::MAX)
        });
        let query_limit = i64::from(page_size.get()) + 1;
        let rows = query_as::<_, EventRow>(SELECT_EVENT_PAGE)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(after_sequence)
            .bind(query_limit)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("journal page load", source))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let event = decode_event(row)?;
            verifier
                .verify_next(&event)
                .map_err(|_| StoreError::corrupt("journal chain"))?;
            events.push(event);
        }
        let has_more = events.len() > usize::from(page_size.get());
        if has_more {
            events.pop();
        } else if verifier.head() != run.journal_head() {
            return Err(StoreError::corrupt("run journal head"));
        }

        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("journal page commit", source))?;
        Ok(JournalPage { events, has_more })
    }

    async fn append(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        authority: AppendAuthority,
    ) -> Result<AppendOutcome, StoreError> {
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        let projection_digest = projection_digest(&projection)?;
        let mut transaction = self.begin_mutation("journal append").await?;
        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(row)?;

        let existing = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("journal idempotency lookup", source))?;
        if let Some(row) = existing {
            let committed_projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "journal projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            if committed_projection != Some(projection_digest) {
                return Err(StoreError::ProjectionIntentConflict);
            }
            let checkpoint = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ANCHOR)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(
                    i64::try_from(event.sequence().get())
                        .map_err(|_| StoreError::JournalSequenceExhausted)?,
                )
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("journal checkpoint lookup", source))?;
            if checkpoint.is_some() {
                return Err(StoreError::CheckpointCommitConflict);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent journal append commit", source)
            })?;
            return Ok(AppendOutcome::Idempotent(event));
        }

        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }

        let observed_at = database_now(&mut transaction, "journal append clock").await?;
        match authority {
            AppendAuthority::ControlPlane => {
                if append.worker_fence().is_some() {
                    return Err(StoreError::WrongAppendAuthority);
                }
            }
            AppendAuthority::Worker => {
                let fence = append
                    .worker_fence()
                    .ok_or(StoreError::WrongAppendAuthority)?;
                authorize_worker(&stored, fence, observed_at)?;
            }
        }

        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let prepared_projection = prepare_projection(&stored, &append, projection, recorded_at)?;
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        insert_event(&mut transaction, &event, projection_digest).await?;
        update_run_head(&mut transaction, &event, prepared_projection.as_ref()).await?;

        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("journal append commit", source))?;
        Ok(AppendOutcome::Committed(event))
    }

    #[allow(clippy::too_many_lines)]
    async fn append_with_outbox(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        authority: AppendAuthority,
        intents: Vec<OutboxDeliveryIntent>,
    ) -> Result<OutboxEnqueueOutcome, StoreError> {
        validate_outbox_batch(&append, &intents)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        let projection_digest = projection_digest(&projection)?;
        let mut transaction = self.begin_mutation("journal outbox append").await?;
        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(row)?;

        let existing = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("outbox event idempotency lookup", source))?;
        if let Some(row) = existing {
            let committed_projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "journal projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            if committed_projection != Some(projection_digest) {
                return Err(StoreError::ProjectionIntentConflict);
            }
            let checkpoint = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ANCHOR)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(
                    i64::try_from(event.sequence().get())
                        .map_err(|_| StoreError::JournalSequenceExhausted)?,
                )
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("outbox checkpoint lookup", source))?;
            if checkpoint.is_some() {
                return Err(StoreError::CheckpointCommitConflict);
            }

            let expected = materialize_outbox_deliveries(intents, &event)?;
            let rows =
                query_as::<_, OutboxDeliveryRow>(SELECT_OUTBOX_DELIVERIES_BY_ORIGIN.as_str())
                    .bind(tenant_id.as_str())
                    .bind(*run_id.as_uuid())
                    .bind(
                        i64::try_from(event.sequence().get())
                            .map_err(|_| StoreError::JournalSequenceExhausted)?,
                    )
                    .fetch_all(&mut *transaction)
                    .await
                    .map_err(|source| {
                        StoreError::database("outbox idempotency set load", source)
                    })?;
            let mut durable = BTreeMap::new();
            for row in rows {
                let delivery = decode_outbox_delivery(&row)?;
                verify_outbox_projection(&mut transaction, &row, &delivery).await?;
                if durable
                    .insert(delivery.intent().delivery_id(), delivery)
                    .is_some()
                {
                    return Err(StoreError::corrupt("outbox delivery identity set"));
                }
            }
            if durable.len() != expected.len()
                || expected
                    .iter()
                    .any(|delivery| durable.get(&delivery.intent().delivery_id()) != Some(delivery))
            {
                return Err(StoreError::OutboxEnqueueConflict);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent journal outbox append commit", source)
            })?;
            return Ok(OutboxEnqueueOutcome::Idempotent {
                event,
                deliveries: expected,
            });
        }

        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let observed_at = database_now(&mut transaction, "journal outbox append clock").await?;
        match authority {
            AppendAuthority::ControlPlane => {
                if append.worker_fence().is_some() {
                    return Err(StoreError::WrongAppendAuthority);
                }
            }
            AppendAuthority::Worker => {
                let fence = append
                    .worker_fence()
                    .ok_or(StoreError::WrongAppendAuthority)?;
                authorize_worker(&stored, fence, observed_at)?;
            }
        }

        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let prepared_projection = prepare_projection(&stored, &append, projection, recorded_at)?;
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let deliveries = materialize_outbox_deliveries(intents, &event)?;
        for delivery in &deliveries {
            if load_outbox_destination_row(&mut transaction, delivery.intent().destination())
                .await?
                .is_none()
            {
                return Err(StoreError::OutboxDestinationNotFound);
            }
        }

        insert_event(&mut transaction, &event, projection_digest).await?;
        for delivery in &deliveries {
            insert_outbox_delivery(&mut transaction, delivery, event.source()).await?;
        }
        update_run_head(&mut transaction, &event, prepared_projection.as_ref()).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("journal outbox append commit", source))?;
        Ok(OutboxEnqueueOutcome::Committed { event, deliveries })
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_interrupt_resolution(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        resolution_intent: InterruptResolutionIntent,
    ) -> Result<InterruptResolutionCommitOutcome, StoreError> {
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        let request_head = resolution_intent.request();
        if request_head.tenant_id() != &tenant_id
            || request_head.run_id() != run_id
            || resolution_intent.resolution_event_id() != event_id
        {
            return Err(StoreError::InvalidInterruptResolution);
        }
        let interrupt_id = request_head.interrupt_id();
        let projection_digest = wait_terminal_projection_digest(
            INTERRUPT_RESOLUTION_PROJECTION_DIGEST_DOMAIN,
            expected_revision,
            resolution_intent.intent_digest(),
        )?;
        let mut transaction = self.begin_mutation("interrupt resolution").await?;
        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(row)?;
        verify_current_wait_set(&mut transaction, &stored).await?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("interrupt resolution event lookup", source))?;
        if let Some(row) = existing_event {
            let committed_projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "interrupt resolution projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            if committed_projection != Some(projection_digest) {
                return Err(StoreError::ProjectionIntentConflict);
            }
            let row = query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID.as_str())
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*interrupt_id.as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| {
                    StoreError::database("interrupt resolution registration lookup", source)
                })?
                .ok_or(StoreError::InterruptResolutionCommitConflict)?;
            let record = load_interrupt_record_from_row(&mut transaction, &row).await?;
            let expected = InterruptResolution::commit(resolution_intent, event.head())
                .map_err(|_| StoreError::InvalidInterruptResolution)?;
            if record.request().head() != *expected.intent().request()
                || record.resolution() != Some(&expected)
                || expected.journal() != &event.head()
            {
                return Err(StoreError::InterruptResolutionCommitConflict);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent interrupt resolution commit", source)
            })?;
            return Ok(InterruptResolutionCommitOutcome::Idempotent { event, record });
        }

        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Waiting {
            return Err(StoreError::InvalidInterruptResolution);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let registration =
            query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID_FOR_UPDATE.as_str())
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*interrupt_id.as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("interrupt registration lock", source))?
                .ok_or(StoreError::WaitRegistrationNotFound)?;
        let wait = decode_wait_registration(&registration)?;
        let DurableWait::Interrupt { request } = wait else {
            return Err(StoreError::WaitRegistrationKindMismatch);
        };
        if registration.status != "outstanding" || request.head() != *request_head {
            return Err(StoreError::InvalidInterruptResolution);
        }

        let observed_at = database_now(&mut transaction, "interrupt resolution clock").await?;
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let resolution = InterruptResolution::commit(resolution_intent, event.head())
            .map_err(|_| StoreError::InvalidInterruptResolution)?;
        let prepared_projection = prepare_durable_wait_projection(
            &stored,
            &tenant_id,
            run_id,
            expected_revision,
            RunTransition::ResolveInterrupt {
                interrupt_id,
                resolved_at: resolution.resolved_at(),
            },
            recorded_at,
        )?;
        let record = InterruptRecord::restore(*request, Some(resolution.clone()))
            .map_err(|_| StoreError::InvalidInterruptResolution)?;

        insert_event(&mut transaction, &event, projection_digest).await?;
        insert_interrupt_resolution(&mut transaction, record.request(), &resolution).await?;
        project_interrupt_resolution(&mut transaction, record.request(), &resolution).await?;
        update_run_head(&mut transaction, &event, Some(&prepared_projection)).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("interrupt resolution commit", source))?;
        Ok(InterruptResolutionCommitOutcome::Committed { event, record })
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_timer_firing(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        firing_intent: TimerFiringIntent,
    ) -> Result<TimerFiringCommitOutcome, StoreError> {
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        let timer_head = firing_intent.timer();
        if timer_head.tenant_id() != &tenant_id
            || timer_head.run_id() != run_id
            || firing_intent.firing_event_id() != event_id
        {
            return Err(StoreError::InvalidTimerFiring);
        }
        let timer_id = timer_head.timer_id();
        let projection_digest = wait_terminal_projection_digest(
            TIMER_FIRING_PROJECTION_DIGEST_DOMAIN,
            expected_revision,
            firing_intent.intent_digest(),
        )?;
        let mut transaction = self.begin_mutation("timer firing").await?;
        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(row)?;
        verify_current_wait_set(&mut transaction, &stored).await?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("timer firing event lookup", source))?;
        if let Some(row) = existing_event {
            let committed_projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "timer firing projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            if committed_projection != Some(projection_digest) {
                return Err(StoreError::ProjectionIntentConflict);
            }
            let row = query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID.as_str())
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*timer_id.as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("timer firing registration lookup", source))?
                .ok_or(StoreError::TimerFiringCommitConflict)?;
            let record = load_timer_record_from_row(&mut transaction, &row).await?;
            let expected = TimerFiring::commit(firing_intent, event.head())
                .map_err(|_| StoreError::InvalidTimerFiring)?;
            if record.timer().head() != *expected.intent().timer()
                || record.firing() != Some(&expected)
                || expected.journal() != &event.head()
            {
                return Err(StoreError::TimerFiringCommitConflict);
            }
            transaction
                .commit()
                .await
                .map_err(|source| StoreError::database("idempotent timer firing commit", source))?;
            return Ok(TimerFiringCommitOutcome::Idempotent { event, record });
        }

        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Waiting {
            return Err(StoreError::InvalidTimerFiring);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let registration =
            query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID_FOR_UPDATE.as_str())
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(*timer_id.as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("timer registration lock", source))?
                .ok_or(StoreError::WaitRegistrationNotFound)?;
        let wait = decode_wait_registration(&registration)?;
        let DurableWait::Timer { timer } = wait else {
            return Err(StoreError::WaitRegistrationKindMismatch);
        };
        if registration.status != "outstanding" || timer.head() != *timer_head {
            return Err(StoreError::InvalidTimerFiring);
        }

        let observed_at = database_now(&mut transaction, "timer firing clock").await?;
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let firing = TimerFiring::commit(firing_intent, event.head())
            .map_err(|_| StoreError::InvalidTimerFiring)?;
        let prepared_projection = prepare_durable_wait_projection(
            &stored,
            &tenant_id,
            run_id,
            expected_revision,
            RunTransition::FireTimer {
                timer_id,
                fired_at: firing.fired_at(),
            },
            recorded_at,
        )?;
        let record = DurableTimerRecord::restore(*timer, Some(firing.clone()))
            .map_err(|_| StoreError::InvalidTimerFiring)?;

        insert_event(&mut transaction, &event, projection_digest).await?;
        insert_timer_firing(&mut transaction, record.timer(), &firing).await?;
        project_timer_firing(&mut transaction, record.timer(), &firing).await?;
        update_run_head(&mut transaction, &event, Some(&prepared_projection)).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("timer firing commit", source))?;
        Ok(TimerFiringCommitOutcome::Committed { event, record })
    }

    #[allow(clippy::too_many_lines)]
    async fn commit_wait_abandonment(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        transition: RunTransition,
        authority: AppendAuthority,
    ) -> Result<WaitAbandonmentCommitOutcome, StoreError> {
        let reason = wait_abandonment_transition_reason(&transition)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        let run_projection = RunProjection::transition(expected_revision, transition.clone());

        let mut transaction = self.begin_mutation("wait abandonment").await?;
        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(row)?;
        verify_current_wait_set(&mut transaction, &stored).await?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("wait abandonment event lookup", source))?;
        if let Some(row) = existing_event {
            let committed_projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "wait abandonment projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            let abandonments = load_wait_abandonment_set(&mut transaction, &event).await?;
            if abandonments
                .iter()
                .any(|abandonment| abandonment.reason() != reason)
            {
                return Err(StoreError::WaitAbandonmentCommitConflict);
            }
            let waits = abandonments
                .iter()
                .map(|abandonment| abandonment.wait().clone())
                .collect::<Vec<_>>();
            if committed_projection
                != Some(wait_abandonment_projection_digest(&run_projection, &waits)?)
            {
                return Err(StoreError::ProjectionIntentConflict);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent wait abandonment commit", source)
            })?;
            return Ok(WaitAbandonmentCommitOutcome::Idempotent {
                event,
                abandonments,
            });
        }

        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Waiting {
            return Err(StoreError::InvalidWaitAbandonment);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        let rows = query_as::<_, WaitRegistrationRow>(
            SELECT_OUTSTANDING_WAIT_REGISTRATIONS_FOR_UPDATE.as_str(),
        )
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .fetch_all(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("wait abandonment registration lock", source))?;
        if rows.is_empty()
            || rows.len() > RunWaits::MAX_LEN
            || rows.len() != usize::from(stored.unresolved_wait_count())
        {
            return Err(StoreError::WaitAbandonmentCommitConflict);
        }
        let mut waits = Vec::with_capacity(rows.len());
        for row in rows {
            let sequence = row.registration_sequence;
            let wait = decode_wait_registration(&row)?;
            if row.status != "outstanding" {
                return Err(StoreError::WaitAbandonmentCommitConflict);
            }
            verify_wait_registration_event(&mut transaction, &wait, sequence).await?;
            waits.push(wait);
        }

        let observed_at = database_now(&mut transaction, "wait abandonment clock").await?;
        match authority {
            AppendAuthority::ControlPlane => {
                if append.worker_fence().is_some() {
                    return Err(StoreError::WrongAppendAuthority);
                }
            }
            AppendAuthority::Worker => {
                let fence = append
                    .worker_fence()
                    .ok_or(StoreError::WrongAppendAuthority)?;
                authorize_worker(&stored, fence, observed_at)?;
            }
        }
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let prepared_projection = prepare_durable_wait_projection(
            &stored,
            &tenant_id,
            run_id,
            expected_revision,
            transition,
            recorded_at,
        )?;
        let projection_digest = wait_abandonment_projection_digest(&run_projection, &waits)?;
        let abandonments = waits
            .into_iter()
            .map(|wait| materialize_wait_abandonment(wait, reason, event.head()))
            .collect::<Result<Vec<_>, _>>()?;

        insert_event(&mut transaction, &event, projection_digest).await?;
        for abandonment in &abandonments {
            insert_wait_abandonment(&mut transaction, abandonment).await?;
            project_wait_abandonment(&mut transaction, abandonment).await?;
        }
        update_run_head(&mut transaction, &event, Some(&prepared_projection)).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("wait abandonment commit", source))?;
        Ok(WaitAbandonmentCommitOutcome::Committed {
            event,
            abandonments,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn append_initial_wait_checkpoint(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        checkpoint_write: CheckpointWrite,
        registrations: Vec<WaitRegistrationIntent>,
        authority: AppendAuthority,
    ) -> Result<WaitCheckpointCommitOutcome, StoreError> {
        validate_wait_registration_batch(&append, &registrations)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        if checkpoint_write.tenant_id() != &tenant_id
            || checkpoint_write.run_id() != run_id
            || checkpoint_write.parent().is_some()
        {
            return Err(StoreError::WaitRegistrationCommitConflict);
        }
        let projection_digest = wait_registration_projection_digest(
            expected_revision,
            &checkpoint_write,
            &registrations,
        )?;

        let mut transaction = self.begin_mutation("initial wait checkpoint").await?;
        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(row)?;
        verify_current_wait_set(&mut transaction, &stored).await?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("wait checkpoint event lookup", source))?;
        if let Some(row) = existing_event {
            let committed_projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "wait registration projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            if committed_projection != Some(projection_digest) {
                return Err(StoreError::ProjectionIntentConflict);
            }
            let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ANCHOR)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(
                    i64::try_from(event.sequence().get())
                        .map_err(|_| StoreError::JournalSequenceExhausted)?,
                )
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("wait checkpoint anchor lookup", source))?
                .ok_or(StoreError::WaitRegistrationCommitConflict)?;
            let checkpoint = decode_checkpoint(row)?;
            if checkpoint.checkpoint_id() != checkpoint_write.checkpoint_id()
                || !checkpoint.matches_write(&checkpoint_write)
                || checkpoint.journal_head() != &event.head()
            {
                return Err(StoreError::WaitRegistrationCommitConflict);
            }
            let waits = materialize_wait_registrations(registrations, &event)?;
            verify_wait_registration_set(&mut transaction, &event, &waits).await?;
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent initial wait checkpoint commit", source)
            })?;
            return Ok(WaitCheckpointCommitOutcome::Idempotent {
                event,
                checkpoint,
                waits,
            });
        }

        let existing_checkpoint = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*checkpoint_write.checkpoint_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("wait checkpoint identity lookup", source))?;
        if existing_checkpoint.is_some() {
            return Err(StoreError::CheckpointIdConflict);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }
        if load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
            .await?
            .is_some()
        {
            return Err(StoreError::StaleCheckpointHead);
        }

        let observed_at = database_now(&mut transaction, "initial wait checkpoint clock").await?;
        match authority {
            AppendAuthority::ControlPlane => {
                if append.worker_fence().is_some() {
                    return Err(StoreError::WrongAppendAuthority);
                }
            }
            AppendAuthority::Worker => {
                let fence = append
                    .worker_fence()
                    .ok_or(StoreError::WrongAppendAuthority)?;
                authorize_worker(&stored, fence, observed_at)?;
            }
        }
        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let waits = materialize_wait_registrations(registrations, &event)?;
        let lifecycle_waits = RunWaits::try_new(waits.iter().map(DurableWait::marker))
            .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?;
        let prepared_projection = prepare_durable_wait_projection(
            &stored,
            &tenant_id,
            run_id,
            expected_revision,
            RunTransition::Wait {
                waits: lifecycle_waits,
            },
            recorded_at,
        )?;
        let checkpoint = Checkpoint::commit(checkpoint_write, event.head())
            .map_err(|_| StoreError::encoding("wait checkpoint commit"))?;

        insert_event(&mut transaction, &event, projection_digest).await?;
        insert_checkpoint(&mut transaction, &checkpoint, event.source()).await?;
        for wait in &waits {
            insert_wait_registration(&mut transaction, wait).await?;
        }
        update_checkpoint_pointer(&mut transaction, &checkpoint, event.source()).await?;
        update_run_head(&mut transaction, &event, Some(&prepared_projection)).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("initial wait checkpoint commit", source))?;
        Ok(WaitCheckpointCommitOutcome::Committed {
            event,
            checkpoint,
            waits,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn append_checkpoint(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        checkpoint_write: CheckpointWrite,
        authority: AppendAuthority,
    ) -> Result<CheckpointCommitOutcome, StoreError> {
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        if checkpoint_write.tenant_id() != &tenant_id || checkpoint_write.run_id() != run_id {
            return Err(StoreError::CheckpointCommitConflict);
        }
        let projection_digest = projection_digest(&projection)?;

        let mut transaction = self.begin_mutation("checkpoint append").await?;
        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(row)?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("checkpoint event lookup", source))?;
        if let Some(row) = existing_event {
            let committed_projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "journal projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            if committed_projection != Some(projection_digest) {
                return Err(StoreError::ProjectionIntentConflict);
            }
            let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ANCHOR)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(
                    i64::try_from(event.sequence().get())
                        .map_err(|_| StoreError::JournalSequenceExhausted)?,
                )
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("checkpoint anchor lookup", source))?
                .ok_or(StoreError::CheckpointCommitConflict)?;
            let checkpoint = decode_checkpoint(row)?;
            if checkpoint.checkpoint_id() != checkpoint_write.checkpoint_id()
                || !checkpoint.matches_write(&checkpoint_write)
                || checkpoint.journal_head() != &event.head()
            {
                return Err(StoreError::CheckpointCommitConflict);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent checkpoint append commit", source)
            })?;
            return Ok(CheckpointCommitOutcome::Idempotent { event, checkpoint });
        }

        let existing_checkpoint = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*checkpoint_write.checkpoint_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("checkpoint idempotency lookup", source))?;
        if existing_checkpoint.is_some() {
            return Err(StoreError::CheckpointIdConflict);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }

        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id).await?;
        let expected_parent = current_checkpoint.as_ref().map(Checkpoint::head);
        if checkpoint_write.parent() != expected_parent.as_ref() {
            return Err(StoreError::StaleCheckpointHead);
        }
        if let Some(parent) = current_checkpoint.as_ref() {
            ensure_no_unsettled_tool_invocations(&mut transaction, parent).await?;
            ensure_no_unsettled_model_invocations(&mut transaction, parent).await?;
        }

        let observed_at = database_now(&mut transaction, "checkpoint append clock").await?;
        match authority {
            AppendAuthority::ControlPlane => {
                if append.worker_fence().is_some() {
                    return Err(StoreError::WrongAppendAuthority);
                }
            }
            AppendAuthority::Worker => {
                let fence = append
                    .worker_fence()
                    .ok_or(StoreError::WrongAppendAuthority)?;
                authorize_worker(&stored, fence, observed_at)?;
            }
        }

        let recorded_at = stored
            .journal_head()
            .map_or(observed_at, |head| observed_at.max(head.recorded_at()));
        let prepared_projection = prepare_projection(&stored, &append, projection, recorded_at)?;
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let checkpoint = Checkpoint::commit(checkpoint_write, event.head())
            .map_err(|_| StoreError::encoding("checkpoint commit"))?;

        insert_event(&mut transaction, &event, projection_digest).await?;
        insert_checkpoint(&mut transaction, &checkpoint, event.source()).await?;
        update_checkpoint_pointer(&mut transaction, &checkpoint, event.source()).await?;
        update_run_head(&mut transaction, &event, prepared_projection.as_ref()).await?;

        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("checkpoint append commit", source))?;
        Ok(CheckpointCommitOutcome::Committed { event, checkpoint })
    }

    async fn verify_checkpoint_barrier_preflight(
        &self,
        barrier: &CheckpointBarrier,
    ) -> Result<(), StoreError> {
        let base = barrier.base_checkpoint();
        let mut transaction = self
            .begin_repeatable_read("checkpoint barrier preflight")
            .await?;
        let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
            .bind(base.tenant_id().as_str())
            .bind(*base.run_id().as_uuid())
            .bind(*base.checkpoint_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("checkpoint barrier base load", source))?
            .ok_or(StoreError::InvalidCheckpointBarrier)?;
        let checkpoint = decode_checkpoint(row)?;
        if checkpoint.head() != *base || checkpoint.ready_nodes() != barrier.base_ready_nodes() {
            return Err(StoreError::InvalidCheckpointBarrier);
        }
        verify_checkpoint_anchor(&mut transaction, &checkpoint).await?;

        for expected in barrier.result_heads().iter() {
            let row = load_pending_node_result_row(&mut transaction, expected.activation())
                .await?
                .ok_or(StoreError::CheckpointBarrierIncomplete)?;
            let result = decode_pending_node_result(&row)?;
            if result.head() != *expected {
                return Err(StoreError::CheckpointBarrierResultConflict);
            }
            verify_pending_node_result(&mut transaction, &result).await?;
        }

        transaction.commit().await.map_err(|source| {
            StoreError::database("checkpoint barrier preflight commit", source)
        })?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn append_wait_barrier(
        &self,
        append: JournalAppend,
        expected_revision: RunRevision,
        barrier: CheckpointBarrier,
        registrations: Vec<WaitRegistrationIntent>,
        authority: AppendAuthority,
    ) -> Result<WaitCheckpointCommitOutcome, StoreError> {
        validate_wait_registration_batch(&append, &registrations)?;
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        let base = barrier.base_checkpoint();
        let successor = barrier.successor();
        if base.tenant_id() != &tenant_id
            || base.run_id() != run_id
            || successor.tenant_id() != &tenant_id
            || successor.run_id() != run_id
        {
            return Err(StoreError::CheckpointBarrierCommitConflict);
        }
        let projection_digest = wait_barrier_projection_digest(
            expected_revision,
            barrier.intent_digest(),
            &registrations,
        )?;
        self.verify_checkpoint_barrier_preflight(&barrier).await?;

        let mut transaction = self
            .begin_mutation("wait checkpoint barrier append")
            .await?;
        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(row)?;
        verify_current_wait_set(&mut transaction, &stored).await?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("wait barrier event lookup", source))?;
        if let Some(row) = existing_event {
            let committed_projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "wait barrier projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            if committed_projection != Some(projection_digest) {
                return Err(StoreError::ProjectionIntentConflict);
            }
            let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ANCHOR)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(
                    i64::try_from(event.sequence().get())
                        .map_err(|_| StoreError::JournalSequenceExhausted)?,
                )
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("wait barrier anchor lookup", source))?
                .ok_or(StoreError::CheckpointBarrierCommitConflict)?;
            let checkpoint = decode_checkpoint(row)?;
            if checkpoint.checkpoint_id() != successor.checkpoint_id()
                || !checkpoint.matches_write(successor)
                || checkpoint.journal_head() != &event.head()
            {
                return Err(StoreError::CheckpointBarrierCommitConflict);
            }
            let waits = materialize_wait_registrations(registrations, &event)?;
            verify_barrier_consumptions(&mut transaction, &barrier, &checkpoint).await?;
            verify_wait_registration_set(&mut transaction, &event, &waits).await?;
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent wait checkpoint barrier commit", source)
            })?;
            return Ok(WaitCheckpointCommitOutcome::Idempotent {
                event,
                checkpoint,
                waits,
            });
        }

        let existing_checkpoint = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*successor.checkpoint_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("wait barrier checkpoint lookup", source))?;
        if existing_checkpoint.is_some() {
            return Err(StoreError::CheckpointIdConflict);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }

        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current_checkpoint.head() != *base {
            return Err(StoreError::StaleCheckpointHead);
        }
        if current_checkpoint.ready_nodes() != barrier.base_ready_nodes() {
            return Err(StoreError::InvalidCheckpointBarrier);
        }
        ensure_no_unsettled_tool_invocations(&mut transaction, &current_checkpoint).await?;
        ensure_no_unsettled_model_invocations(&mut transaction, &current_checkpoint).await?;

        let existing_consumptions = load_barrier_consumption_rows(&mut transaction, base).await?;
        if !existing_consumptions.is_empty() {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }
        let durable_heads = load_locked_barrier_result_heads(&mut transaction, base).await?;
        validate_complete_barrier_result_heads(&durable_heads, barrier.result_heads())?;
        let current_journal = stored
            .journal_head()
            .ok_or_else(|| StoreError::corrupt("wait barrier run journal head"))?;
        if barrier.result_heads().iter().any(|head| {
            head.journal_head().sequence() > current_journal.sequence()
                || head.journal_head().recorded_at() > current_journal.recorded_at()
        }) {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }

        let observed_at = database_now(&mut transaction, "wait checkpoint barrier clock").await?;
        match authority {
            AppendAuthority::ControlPlane => {
                if append.worker_fence().is_some() {
                    return Err(StoreError::WrongAppendAuthority);
                }
            }
            AppendAuthority::Worker => {
                let fence = append
                    .worker_fence()
                    .ok_or(StoreError::WrongAppendAuthority)?;
                authorize_worker(&stored, fence, observed_at)?;
            }
        }

        let recorded_at = observed_at.max(current_journal.recorded_at());
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let waits = materialize_wait_registrations(registrations, &event)?;
        let lifecycle_waits = RunWaits::try_new(waits.iter().map(DurableWait::marker))
            .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?;
        let prepared_projection = prepare_durable_wait_projection(
            &stored,
            &tenant_id,
            run_id,
            expected_revision,
            RunTransition::Wait {
                waits: lifecycle_waits,
            },
            recorded_at,
        )?;
        let checkpoint = Checkpoint::commit(successor.clone(), event.head())
            .map_err(|_| StoreError::encoding("wait checkpoint barrier commit"))?;

        insert_event(&mut transaction, &event, projection_digest).await?;
        insert_checkpoint(&mut transaction, &checkpoint, event.source()).await?;
        for wait in &waits {
            insert_wait_registration(&mut transaction, wait).await?;
        }
        insert_barrier_consumptions(&mut transaction, &barrier, &checkpoint, event.source())
            .await?;
        update_checkpoint_pointer(&mut transaction, &checkpoint, event.source()).await?;
        update_run_head(&mut transaction, &event, Some(&prepared_projection)).await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("wait checkpoint barrier commit", source))?;
        Ok(WaitCheckpointCommitOutcome::Committed {
            event,
            checkpoint,
            waits,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn append_barrier(
        &self,
        append: JournalAppend,
        projection: RunProjection,
        barrier: CheckpointBarrier,
        authority: AppendAuthority,
    ) -> Result<BarrierCommitOutcome, StoreError> {
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let event_id = append.intent().event_id();
        let base = barrier.base_checkpoint();
        let successor = barrier.successor();
        if base.tenant_id() != &tenant_id
            || base.run_id() != run_id
            || successor.tenant_id() != &tenant_id
            || successor.run_id() != run_id
        {
            return Err(StoreError::CheckpointBarrierCommitConflict);
        }
        let projection_digest = barrier_projection_digest(&projection, barrier.intent_digest())?;
        self.verify_checkpoint_barrier_preflight(&barrier).await?;

        let mut transaction = self.begin_mutation("checkpoint barrier append").await?;
        let row = fetch_locked_run_row(&mut transaction, &tenant_id, run_id).await?;
        let stored = decode_run(row)?;

        let existing_event = query_as::<_, EventRow>(SELECT_EVENT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*event_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("checkpoint barrier event lookup", source))?;
        if let Some(row) = existing_event {
            let committed_projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "checkpoint barrier projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
            }
            if committed_projection != Some(projection_digest) {
                return Err(StoreError::ProjectionIntentConflict);
            }
            let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ANCHOR)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .bind(
                    i64::try_from(event.sequence().get())
                        .map_err(|_| StoreError::JournalSequenceExhausted)?,
                )
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("checkpoint barrier anchor lookup", source))?
                .ok_or(StoreError::CheckpointBarrierCommitConflict)?;
            let checkpoint = decode_checkpoint(row)?;
            if checkpoint.checkpoint_id() != successor.checkpoint_id()
                || !checkpoint.matches_write(successor)
                || checkpoint.journal_head() != &event.head()
            {
                return Err(StoreError::CheckpointBarrierCommitConflict);
            }
            verify_barrier_consumptions(&mut transaction, &barrier, &checkpoint).await?;
            transaction.commit().await.map_err(|source| {
                StoreError::database("idempotent checkpoint barrier commit", source)
            })?;
            return Ok(BarrierCommitOutcome::Idempotent { event, checkpoint });
        }

        let existing_checkpoint = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*successor.checkpoint_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| {
                StoreError::database("checkpoint barrier idempotency lookup", source)
            })?;
        if existing_checkpoint.is_some() {
            return Err(StoreError::CheckpointIdConflict);
        }
        if stored.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if stored.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if append.expectation().head() != stored.journal_head() {
            return Err(StoreError::StaleJournalHead);
        }

        let current_checkpoint =
            load_locked_current_checkpoint(&mut transaction, &stored, &tenant_id, run_id)
                .await?
                .ok_or(StoreError::StaleCheckpointHead)?;
        if current_checkpoint.head() != *base {
            return Err(StoreError::StaleCheckpointHead);
        }
        if current_checkpoint.ready_nodes() != barrier.base_ready_nodes() {
            return Err(StoreError::InvalidCheckpointBarrier);
        }
        ensure_no_unsettled_tool_invocations(&mut transaction, &current_checkpoint).await?;
        ensure_no_unsettled_model_invocations(&mut transaction, &current_checkpoint).await?;

        let existing_consumptions = load_barrier_consumption_rows(&mut transaction, base).await?;
        if !existing_consumptions.is_empty() {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }
        let durable_heads = load_locked_barrier_result_heads(&mut transaction, base).await?;
        validate_complete_barrier_result_heads(&durable_heads, barrier.result_heads())?;
        let current_journal = stored
            .journal_head()
            .ok_or_else(|| StoreError::corrupt("checkpoint barrier run journal head"))?;
        if barrier.result_heads().iter().any(|head| {
            head.journal_head().sequence() > current_journal.sequence()
                || head.journal_head().recorded_at() > current_journal.recorded_at()
        }) {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }

        let observed_at = database_now(&mut transaction, "checkpoint barrier clock").await?;
        match authority {
            AppendAuthority::ControlPlane => {
                if append.worker_fence().is_some() {
                    return Err(StoreError::WrongAppendAuthority);
                }
            }
            AppendAuthority::Worker => {
                let fence = append
                    .worker_fence()
                    .ok_or(StoreError::WrongAppendAuthority)?;
                authorize_worker(&stored, fence, observed_at)?;
            }
        }

        let recorded_at = observed_at.max(current_journal.recorded_at());
        let prepared_projection = prepare_projection(&stored, &append, projection, recorded_at)?;
        let event = JournalEvent::commit(append, recorded_at)
            .map_err(|error| map_event_commit_error(&error))?;
        let checkpoint = Checkpoint::commit(successor.clone(), event.head())
            .map_err(|_| StoreError::encoding("checkpoint barrier commit"))?;

        insert_event(&mut transaction, &event, projection_digest).await?;
        insert_checkpoint(&mut transaction, &checkpoint, event.source()).await?;
        insert_barrier_consumptions(&mut transaction, &barrier, &checkpoint, event.source())
            .await?;
        update_checkpoint_pointer(&mut transaction, &checkpoint, event.source()).await?;
        update_run_head(&mut transaction, &event, prepared_projection.as_ref()).await?;

        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("checkpoint barrier commit", source))?;
        Ok(BarrierCommitOutcome::Committed { event, checkpoint })
    }

    async fn begin_mutation(
        &self,
        operation: &'static str,
    ) -> Result<Transaction<'_, Postgres>, StoreError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| StoreError::database(operation, source))?;
        query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE")
            .execute(&mut *transaction)
            .await
            .map_err(|source| StoreError::database(operation, source))?;
        apply_transaction_timeouts(&mut transaction, &self.options, operation).await?;
        Ok(transaction)
    }

    async fn begin_repeatable_read(
        &self,
        operation: &'static str,
    ) -> Result<Transaction<'_, Postgres>, StoreError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| StoreError::database(operation, source))?;
        query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await
            .map_err(|source| StoreError::database(operation, source))?;
        apply_transaction_timeouts(&mut transaction, &self.options, operation).await?;
        Ok(transaction)
    }
}

impl fmt::Debug for PostgresStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStore")
            .field("transport_security", &self.options.transport_security())
            .field("max_connections", &self.options.max_connections)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy)]
enum AppendAuthority {
    ControlPlane,
    Worker,
}

#[derive(Clone, Copy)]
enum InvocationAttemptKind {
    Tool,
    Model,
}

impl InvocationAttemptKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Tool => "tool_invocation",
            Self::Model => "model_invocation",
        }
    }

    const fn conflict(self) -> StoreError {
        match self {
            Self::Tool => StoreError::InvalidToolInvocationTransition,
            Self::Model => StoreError::InvalidModelInvocationTransition,
        }
    }
}

struct GraphDefinitionRow {
    tenant_id: String,
    owner_issuer: String,
    owner_subject: String,
    graph_name: String,
    graph_version: String,
    definition_digest: Vec<u8>,
    definition_bytes: Vec<u8>,
    registered_at: DateTime<Utc>,
}

struct AgentAdmissionRow {
    tenant_id: String,
    run_id: Uuid,
    agent_owner_issuer: String,
    agent_owner_subject: String,
    agent_name: String,
    agent_version: String,
    graph_owner_issuer: String,
    graph_owner_subject: String,
    graph_name: String,
    graph_version: String,
    graph_definition_digest: Vec<u8>,
    policy_owner_issuer: String,
    policy_owner_subject: String,
    policy_name: String,
    policy_version: String,
    policy_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    admission_digest: Vec<u8>,
    admitted_at: DateTime<Utc>,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
    checkpoint_id: Uuid,
    checkpoint_superstep: i64,
    checkpoint_digest: Vec<u8>,
    admission_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct AgentSubmissionRow {
    tenant_id: String,
    key_digest: Vec<u8>,
    submission_digest: Vec<u8>,
    run_id: Uuid,
    admission_digest: Vec<u8>,
    created_at: DateTime<Utc>,
}

#[derive(Clone)]
struct SchedulerFairnessShardRow {
    shard_id: String,
    policy_digest: Vec<u8>,
    policy_bytes: Vec<u8>,
    cycle_length: i32,
    next_slot: i32,
    next_sequence: i64,
    registered_at: DateTime<Utc>,
    #[allow(dead_code)]
    updated_at: DateTime<Utc>,
}

struct SchedulerFairnessReservationRow {
    shard_id: String,
    reservation_id: Uuid,
    policy_digest: Vec<u8>,
    sequence: i64,
    slot: i32,
    reserved_at: DateTime<Utc>,
    shard_policy_digest: Vec<u8>,
    cycle_length: i32,
}

struct RunQuarantineTargetRow {
    tenant_id: String,
    run_id: Uuid,
    journal_sequence: Option<i64>,
    journal_event_id: Option<Uuid>,
    journal_recorded_at: Option<DateTime<Utc>>,
    journal_digest: Option<Vec<u8>>,
    lease_attempt_id: Option<Uuid>,
    fencing_epoch: i64,
    lease_renewed_at: Option<DateTime<Utc>>,
    lease_expires_at: Option<DateTime<Utc>>,
    quarantined_at: Option<DateTime<Utc>>,
    quarantine_reason: Option<String>,
}

struct RunQuarantineRow {
    tenant_id: String,
    run_id: Uuid,
    quarantine_id: Uuid,
    quarantined_at: DateTime<Utc>,
    cause_kind: String,
    component: String,
    evidence_digest: Vec<u8>,
    expected_journal_sequence: Option<i64>,
    expected_journal_event_id: Option<Uuid>,
    expected_journal_recorded_at: Option<DateTime<Utc>>,
    expected_journal_digest: Option<Vec<u8>>,
    expected_fence_attempt_id: Option<Uuid>,
    expected_fence_epoch: Option<i64>,
    record_digest: Vec<u8>,
    created_at: DateTime<Utc>,
    run_quarantined_at: Option<DateTime<Utc>>,
    run_quarantine_reason: Option<String>,
    run_lease_attempt_id: Option<Uuid>,
    run_lease_acquired_at: Option<DateTime<Utc>>,
    run_lease_renewed_at: Option<DateTime<Utc>>,
    run_lease_expires_at: Option<DateTime<Utc>>,
    run_fencing_epoch: i64,
    run_scheduler_ready_at: Option<DateTime<Utc>>,
    run_updated_at: DateTime<Utc>,
}

struct RunRow {
    tenant_id: String,
    run_id: Uuid,
    thread_id: Uuid,
    invocation_id: Uuid,
    lifecycle_bytes: Vec<u8>,
    lifecycle_revision: String,
    lifecycle_status: String,
    admitted_at: DateTime<Utc>,
    changed_at: DateTime<Utc>,
    journal_sequence: Option<i64>,
    journal_event_id: Option<Uuid>,
    journal_recorded_at: Option<DateTime<Utc>>,
    journal_digest: Option<Vec<u8>>,
    checkpoint_id: Option<Uuid>,
    checkpoint_superstep: Option<i64>,
    checkpoint_digest: Option<Vec<u8>>,
    fencing_epoch: i64,
    lease_attempt_id: Option<Uuid>,
    lease_acquired_at: Option<DateTime<Utc>>,
    lease_renewed_at: Option<DateTime<Utc>>,
    lease_expires_at: Option<DateTime<Utc>>,
    scheduler_ready_at: Option<DateTime<Utc>>,
    scheduler_not_before: Option<DateTime<Utc>>,
    wait_set_digest: Option<Vec<u8>>,
    unresolved_wait_count: i16,
    next_timer_due_at: Option<DateTime<Utc>>,
    next_interrupt_expiry_at: Option<DateTime<Utc>>,
    quarantined_at: Option<DateTime<Utc>>,
}

struct EventRow {
    tenant_id: String,
    run_id: Uuid,
    sequence: i64,
    event_id: Uuid,
    recorded_at: DateTime<Utc>,
    source_kind: String,
    worker_attempt_id: Option<Uuid>,
    worker_epoch: Option<i64>,
    event_kind: String,
    schema_id: String,
    schema_version: String,
    schema_digest: Vec<u8>,
    payload_bytes: Vec<u8>,
    payload_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    projection_digest: Option<Vec<u8>>,
    previous_digest: Option<Vec<u8>>,
    event_digest: Vec<u8>,
}

struct WaitRegistrationRow {
    tenant_id: String,
    run_id: Uuid,
    wait_id: Uuid,
    wait_kind: String,
    interrupt_kind: Option<String>,
    timer_kind: Option<String>,
    registered_at: DateTime<Utc>,
    due_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
    action_digest: Option<Vec<u8>>,
    registration_sequence: i64,
    registration_event_id: Uuid,
    registration_event_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    record_digest: Vec<u8>,
    record_bytes: Vec<u8>,
    status: String,
    terminal_sequence: Option<i64>,
    terminal_event_id: Option<Uuid>,
    terminal_recorded_at: Option<DateTime<Utc>>,
    terminal_event_digest: Option<Vec<u8>>,
    resolution_digest: Option<Vec<u8>>,
    firing_digest: Option<Vec<u8>>,
    abandonment_digest: Option<Vec<u8>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

struct InterruptResolutionRow {
    tenant_id: String,
    run_id: Uuid,
    interrupt_id: Uuid,
    request_digest: Vec<u8>,
    resolution_sequence: i64,
    resolution_event_id: Uuid,
    resolved_at: DateTime<Utc>,
    resolution_event_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    resolution_digest: Vec<u8>,
    resolution_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct TimerFiringRow {
    tenant_id: String,
    run_id: Uuid,
    timer_id: Uuid,
    timer_digest: Vec<u8>,
    firing_sequence: i64,
    firing_event_id: Uuid,
    fired_at: DateTime<Utc>,
    firing_event_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    firing_digest: Vec<u8>,
    firing_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct WaitAbandonmentRow {
    tenant_id: String,
    run_id: Uuid,
    wait_id: Uuid,
    wait_kind: String,
    registration_digest: Vec<u8>,
    reason_kind: String,
    abandonment_sequence: i64,
    abandonment_event_id: Uuid,
    abandoned_at: DateTime<Utc>,
    abandonment_event_digest: Vec<u8>,
    abandonment_digest: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct CheckpointRow {
    tenant_id: String,
    run_id: Uuid,
    checkpoint_id: Uuid,
    superstep: i64,
    parent_checkpoint_id: Option<Uuid>,
    parent_superstep: Option<i64>,
    parent_digest: Option<Vec<u8>>,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
    graph_definition_digest: Vec<u8>,
    state_schema_id: String,
    state_schema_version: String,
    state_schema_digest: Vec<u8>,
    state_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    checkpoint_digest: Vec<u8>,
    checkpoint_bytes: Vec<u8>,
}

struct ToolInvocationRow {
    tenant_id: String,
    run_id: Uuid,
    invocation_id: Uuid,
    base_checkpoint_id: Uuid,
    base_superstep: i64,
    base_checkpoint_digest: Vec<u8>,
    graph_namespace: String,
    node_id: String,
    activation_input_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    intent_bytes: Vec<u8>,
    current_revision: i64,
    current_status: String,
    current_attempt_id: Option<Uuid>,
    current_record_digest: Vec<u8>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

struct ToolInvocationRevisionRow {
    tenant_id: String,
    run_id: Uuid,
    invocation_id: Uuid,
    revision: i64,
    previous_revision: Option<i64>,
    previous_digest: Option<Vec<u8>>,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
    status: String,
    attempt_id: Option<Uuid>,
    transition_kind: Option<String>,
    started_attempt_id: Option<Uuid>,
    transition_digest: Option<Vec<u8>>,
    record_digest: Vec<u8>,
    record_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct ModelInvocationRow {
    tenant_id: String,
    run_id: Uuid,
    invocation_id: Uuid,
    base_checkpoint_id: Uuid,
    base_superstep: i64,
    base_checkpoint_digest: Vec<u8>,
    graph_namespace: String,
    node_id: String,
    activation_input_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    intent_bytes: Vec<u8>,
    current_revision: i64,
    current_status: String,
    current_attempt_id: Option<Uuid>,
    current_record_digest: Vec<u8>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

struct ModelInvocationRevisionRow {
    tenant_id: String,
    run_id: Uuid,
    invocation_id: Uuid,
    revision: i64,
    previous_revision: Option<i64>,
    previous_digest: Option<Vec<u8>>,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
    status: String,
    attempt_id: Option<Uuid>,
    transition_kind: Option<String>,
    started_attempt_id: Option<Uuid>,
    transition_digest: Option<Vec<u8>>,
    record_digest: Vec<u8>,
    record_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct NodeAttemptStartRow {
    tenant_id: String,
    run_id: Uuid,
    base_checkpoint_id: Uuid,
    base_superstep: i64,
    base_checkpoint_digest: Vec<u8>,
    base_journal_sequence: i64,
    base_journal_event_id: Uuid,
    base_journal_recorded_at: DateTime<Utc>,
    base_journal_digest: Vec<u8>,
    graph_namespace: String,
    node_id: String,
    activation_input_digest: Vec<u8>,
    activation_digest: Vec<u8>,
    attempt_id: Uuid,
    fence_attempt_id: Uuid,
    fence_epoch: i64,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
    start_digest: Vec<u8>,
    start_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct NodeAttemptCompletionRow {
    tenant_id: String,
    run_id: Uuid,
    attempt_id: Uuid,
    base_checkpoint_id: Uuid,
    base_superstep: i64,
    base_checkpoint_digest: Vec<u8>,
    graph_namespace: String,
    node_id: String,
    activation_input_digest: Vec<u8>,
    activation_digest: Vec<u8>,
    fence_attempt_id: Uuid,
    fence_epoch: i64,
    start_journal_sequence: i64,
    start_journal_event_id: Uuid,
    start_journal_recorded_at: DateTime<Utc>,
    start_journal_digest: Vec<u8>,
    start_digest: Vec<u8>,
    status: String,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
    result_intent_digest: Option<Vec<u8>>,
    result_record_digest: Option<Vec<u8>>,
    failure_id: Option<Uuid>,
    retry_kind: Option<String>,
    retry_not_before: Option<DateTime<Utc>>,
    completion_digest: Vec<u8>,
    completion_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct PendingNodeResultRow {
    tenant_id: String,
    run_id: Uuid,
    base_checkpoint_id: Uuid,
    base_superstep: i64,
    base_checkpoint_digest: Vec<u8>,
    base_journal_sequence: i64,
    base_journal_event_id: Uuid,
    base_journal_recorded_at: DateTime<Utc>,
    base_journal_digest: Vec<u8>,
    graph_namespace: String,
    node_id: String,
    activation_input_digest: Vec<u8>,
    node_attempt_id: Option<Uuid>,
    intent_digest: Vec<u8>,
    control_kind: String,
    fence_attempt_id: Uuid,
    fence_epoch: i64,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
    record_digest: Vec<u8>,
    result_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct PendingNodeResultHeadRow {
    tenant_id: String,
    run_id: Uuid,
    base_checkpoint_id: Uuid,
    base_superstep: i64,
    base_checkpoint_digest: Vec<u8>,
    graph_namespace: String,
    node_id: String,
    activation_input_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    fence_attempt_id: Uuid,
    fence_epoch: i64,
    journal_sequence: i64,
    journal_event_id: Uuid,
    journal_recorded_at: DateTime<Utc>,
    journal_digest: Vec<u8>,
    record_digest: Vec<u8>,
}

struct PendingNodeResultConsumptionRow {
    tenant_id: String,
    run_id: Uuid,
    base_checkpoint_id: Uuid,
    base_superstep: i64,
    base_checkpoint_digest: Vec<u8>,
    graph_namespace: String,
    node_id: String,
    result_record_digest: Vec<u8>,
    successor_checkpoint_id: Uuid,
    successor_superstep: i64,
    successor_checkpoint_digest: Vec<u8>,
    successor_journal_sequence: i64,
    successor_journal_event_id: Uuid,
    successor_journal_recorded_at: DateTime<Utc>,
    successor_journal_digest: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct PendingNodeResultBindingRow {
    tenant_id: String,
    run_id: Uuid,
    base_checkpoint_id: Uuid,
    base_superstep: i64,
    base_checkpoint_digest: Vec<u8>,
    graph_namespace: String,
    node_id: String,
    activation_input_digest: Vec<u8>,
    result_record_digest: Vec<u8>,
    result_journal_sequence: i64,
    result_journal_recorded_at: DateTime<Utc>,
    result_journal_digest: Vec<u8>,
    invocation_id: Uuid,
    invocation_revision: i64,
    invocation_record_digest: Vec<u8>,
    invocation_journal_sequence: i64,
    invocation_journal_recorded_at: DateTime<Utc>,
    invocation_journal_digest: Vec<u8>,
}

struct OutboxDestinationRow {
    tenant_id: String,
    destination_id: Uuid,
    snapshot_digest: Vec<u8>,
    config_kind: String,
    schema_id: String,
    schema_version: String,
    schema_digest: Vec<u8>,
    config_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct OutboxDeliveryRow {
    tenant_id: String,
    run_id: Uuid,
    delivery_id: Uuid,
    origin_sequence: i64,
    origin_event_id: Uuid,
    origin_recorded_at: DateTime<Utc>,
    origin_digest: Vec<u8>,
    destination_id: Uuid,
    destination_snapshot_digest: Vec<u8>,
    intent_digest: Vec<u8>,
    expires_at: DateTime<Utc>,
    delivery_digest: Vec<u8>,
    delivery_bytes: Vec<u8>,
    status: String,
    attempt_count: i64,
    current_attempt_id: Option<Uuid>,
    current_epoch: Option<i64>,
    current_attempt_started_at: Option<DateTime<Utc>>,
    current_attempt_expires_at: Option<DateTime<Utc>>,
    next_attempt_at: Option<DateTime<Utc>>,
    last_completion_digest: Option<Vec<u8>>,
    terminal_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

struct OutboxAttemptStartRow {
    tenant_id: String,
    run_id: Uuid,
    delivery_id: Uuid,
    delivery_expires_at: DateTime<Utc>,
    delivery_digest: Vec<u8>,
    epoch: i64,
    attempt_id: Uuid,
    started_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    start_digest: Vec<u8>,
    start_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

struct OutboxAttemptCompletionRow {
    tenant_id: String,
    run_id: Uuid,
    delivery_id: Uuid,
    epoch: i64,
    attempt_id: Uuid,
    started_at: DateTime<Utc>,
    attempt_expires_at: DateTime<Utc>,
    start_digest: Vec<u8>,
    outcome_kind: String,
    retry_advice_kind: Option<String>,
    retry_delay_millis: Option<i64>,
    completed_at: DateTime<Utc>,
    completion_digest: Vec<u8>,
    completion_bytes: Vec<u8>,
    created_at: DateTime<Utc>,
}

impl<'row> FromRow<'row, PgRow> for GraphDefinitionRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            owner_issuer: row.try_get("owner_issuer")?,
            owner_subject: row.try_get("owner_subject")?,
            graph_name: row.try_get("graph_name")?,
            graph_version: row.try_get("graph_version")?,
            definition_digest: row.try_get("definition_digest")?,
            definition_bytes: row.try_get("definition_bytes")?,
            registered_at: row.try_get("registered_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for AgentAdmissionRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            agent_owner_issuer: row.try_get("agent_owner_issuer")?,
            agent_owner_subject: row.try_get("agent_owner_subject")?,
            agent_name: row.try_get("agent_name")?,
            agent_version: row.try_get("agent_version")?,
            graph_owner_issuer: row.try_get("graph_owner_issuer")?,
            graph_owner_subject: row.try_get("graph_owner_subject")?,
            graph_name: row.try_get("graph_name")?,
            graph_version: row.try_get("graph_version")?,
            graph_definition_digest: row.try_get("graph_definition_digest")?,
            policy_owner_issuer: row.try_get("policy_owner_issuer")?,
            policy_owner_subject: row.try_get("policy_owner_subject")?,
            policy_name: row.try_get("policy_name")?,
            policy_version: row.try_get("policy_version")?,
            policy_digest: row.try_get("policy_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            admission_digest: row.try_get("admission_digest")?,
            admitted_at: row.try_get("admitted_at")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            checkpoint_id: row.try_get("checkpoint_id")?,
            checkpoint_superstep: row.try_get("checkpoint_superstep")?,
            checkpoint_digest: row.try_get("checkpoint_digest")?,
            admission_bytes: row.try_get("admission_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for AgentSubmissionRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            key_digest: row.try_get("key_digest")?,
            submission_digest: row.try_get("submission_digest")?,
            run_id: row.try_get("run_id")?,
            admission_digest: row.try_get("admission_digest")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for SchedulerFairnessShardRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            shard_id: row.try_get("shard_id")?,
            policy_digest: row.try_get("policy_digest")?,
            policy_bytes: row.try_get("policy_bytes")?,
            cycle_length: row.try_get("cycle_length")?,
            next_slot: row.try_get("next_slot")?,
            next_sequence: row.try_get("next_sequence")?,
            registered_at: row.try_get("registered_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for SchedulerFairnessReservationRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            shard_id: row.try_get("shard_id")?,
            reservation_id: row.try_get("reservation_id")?,
            policy_digest: row.try_get("policy_digest")?,
            sequence: row.try_get("sequence")?,
            slot: row.try_get("slot")?,
            reserved_at: row.try_get("reserved_at")?,
            shard_policy_digest: row.try_get("shard_policy_digest")?,
            cycle_length: row.try_get("cycle_length")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for RunQuarantineTargetRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            lease_attempt_id: row.try_get("lease_attempt_id")?,
            fencing_epoch: row.try_get("fencing_epoch")?,
            lease_renewed_at: row.try_get("lease_renewed_at")?,
            lease_expires_at: row.try_get("lease_expires_at")?,
            quarantined_at: row.try_get("quarantined_at")?,
            quarantine_reason: row.try_get("quarantine_reason")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for RunQuarantineRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            quarantine_id: row.try_get("quarantine_id")?,
            quarantined_at: row.try_get("quarantined_at")?,
            cause_kind: row.try_get("cause_kind")?,
            component: row.try_get("component")?,
            evidence_digest: row.try_get("evidence_digest")?,
            expected_journal_sequence: row.try_get("expected_journal_sequence")?,
            expected_journal_event_id: row.try_get("expected_journal_event_id")?,
            expected_journal_recorded_at: row.try_get("expected_journal_recorded_at")?,
            expected_journal_digest: row.try_get("expected_journal_digest")?,
            expected_fence_attempt_id: row.try_get("expected_fence_attempt_id")?,
            expected_fence_epoch: row.try_get("expected_fence_epoch")?,
            record_digest: row.try_get("record_digest")?,
            created_at: row.try_get("created_at")?,
            run_quarantined_at: row.try_get("run_quarantined_at")?,
            run_quarantine_reason: row.try_get("run_quarantine_reason")?,
            run_lease_attempt_id: row.try_get("run_lease_attempt_id")?,
            run_lease_acquired_at: row.try_get("run_lease_acquired_at")?,
            run_lease_renewed_at: row.try_get("run_lease_renewed_at")?,
            run_lease_expires_at: row.try_get("run_lease_expires_at")?,
            run_fencing_epoch: row.try_get("run_fencing_epoch")?,
            run_scheduler_ready_at: row.try_get("run_scheduler_ready_at")?,
            run_updated_at: row.try_get("run_updated_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for RunRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            thread_id: row.try_get("thread_id")?,
            invocation_id: row.try_get("invocation_id")?,
            lifecycle_bytes: row.try_get("lifecycle_bytes")?,
            lifecycle_revision: row.try_get("lifecycle_revision")?,
            lifecycle_status: row.try_get("lifecycle_status")?,
            admitted_at: row.try_get("admitted_at")?,
            changed_at: row.try_get("changed_at")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            checkpoint_id: row.try_get("checkpoint_id")?,
            checkpoint_superstep: row.try_get("checkpoint_superstep")?,
            checkpoint_digest: row.try_get("checkpoint_digest")?,
            fencing_epoch: row.try_get("fencing_epoch")?,
            lease_attempt_id: row.try_get("lease_attempt_id")?,
            lease_acquired_at: row.try_get("lease_acquired_at")?,
            lease_renewed_at: row.try_get("lease_renewed_at")?,
            lease_expires_at: row.try_get("lease_expires_at")?,
            scheduler_ready_at: row.try_get("scheduler_ready_at")?,
            scheduler_not_before: row.try_get("scheduler_not_before")?,
            wait_set_digest: row.try_get("wait_set_digest")?,
            unresolved_wait_count: row.try_get("unresolved_wait_count")?,
            next_timer_due_at: row.try_get("next_timer_due_at")?,
            next_interrupt_expiry_at: row.try_get("next_interrupt_expiry_at")?,
            quarantined_at: row.try_get("quarantined_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for EventRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            sequence: row.try_get("sequence")?,
            event_id: row.try_get("event_id")?,
            recorded_at: row.try_get("recorded_at")?,
            source_kind: row.try_get("source_kind")?,
            worker_attempt_id: row.try_get("worker_attempt_id")?,
            worker_epoch: row.try_get("worker_epoch")?,
            event_kind: row.try_get("event_kind")?,
            schema_id: row.try_get("schema_id")?,
            schema_version: row.try_get("schema_version")?,
            schema_digest: row.try_get("schema_digest")?,
            payload_bytes: row.try_get("payload_bytes")?,
            payload_digest: row.try_get("payload_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            projection_digest: row.try_get("projection_digest")?,
            previous_digest: row.try_get("previous_digest")?,
            event_digest: row.try_get("event_digest")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for WaitRegistrationRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            wait_id: row.try_get("wait_id")?,
            wait_kind: row.try_get("wait_kind")?,
            interrupt_kind: row.try_get("interrupt_kind")?,
            timer_kind: row.try_get("timer_kind")?,
            registered_at: row.try_get("registered_at")?,
            due_at: row.try_get("due_at")?,
            expires_at: row.try_get("expires_at")?,
            action_digest: row.try_get("action_digest")?,
            registration_sequence: row.try_get("registration_sequence")?,
            registration_event_id: row.try_get("registration_event_id")?,
            registration_event_digest: row.try_get("registration_event_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            record_digest: row.try_get("record_digest")?,
            record_bytes: row.try_get("record_bytes")?,
            status: row.try_get("status")?,
            terminal_sequence: row.try_get("terminal_sequence")?,
            terminal_event_id: row.try_get("terminal_event_id")?,
            terminal_recorded_at: row.try_get("terminal_recorded_at")?,
            terminal_event_digest: row.try_get("terminal_event_digest")?,
            resolution_digest: row.try_get("resolution_digest")?,
            firing_digest: row.try_get("firing_digest")?,
            abandonment_digest: row.try_get("abandonment_digest")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for InterruptResolutionRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            interrupt_id: row.try_get("interrupt_id")?,
            request_digest: row.try_get("request_digest")?,
            resolution_sequence: row.try_get("resolution_sequence")?,
            resolution_event_id: row.try_get("resolution_event_id")?,
            resolved_at: row.try_get("resolved_at")?,
            resolution_event_digest: row.try_get("resolution_event_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            resolution_digest: row.try_get("resolution_digest")?,
            resolution_bytes: row.try_get("resolution_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for TimerFiringRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            timer_id: row.try_get("timer_id")?,
            timer_digest: row.try_get("timer_digest")?,
            firing_sequence: row.try_get("firing_sequence")?,
            firing_event_id: row.try_get("firing_event_id")?,
            fired_at: row.try_get("fired_at")?,
            firing_event_digest: row.try_get("firing_event_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            firing_digest: row.try_get("firing_digest")?,
            firing_bytes: row.try_get("firing_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for WaitAbandonmentRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            wait_id: row.try_get("wait_id")?,
            wait_kind: row.try_get("wait_kind")?,
            registration_digest: row.try_get("registration_digest")?,
            reason_kind: row.try_get("reason_kind")?,
            abandonment_sequence: row.try_get("abandonment_sequence")?,
            abandonment_event_id: row.try_get("abandonment_event_id")?,
            abandoned_at: row.try_get("abandoned_at")?,
            abandonment_event_digest: row.try_get("abandonment_event_digest")?,
            abandonment_digest: row.try_get("abandonment_digest")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for CheckpointRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            checkpoint_id: row.try_get("checkpoint_id")?,
            superstep: row.try_get("superstep")?,
            parent_checkpoint_id: row.try_get("parent_checkpoint_id")?,
            parent_superstep: row.try_get("parent_superstep")?,
            parent_digest: row.try_get("parent_digest")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            graph_definition_digest: row.try_get("graph_definition_digest")?,
            state_schema_id: row.try_get("state_schema_id")?,
            state_schema_version: row.try_get("state_schema_version")?,
            state_schema_digest: row.try_get("state_schema_digest")?,
            state_digest: row.try_get("state_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            checkpoint_digest: row.try_get("checkpoint_digest")?,
            checkpoint_bytes: row.try_get("checkpoint_bytes")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for ToolInvocationRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            invocation_id: row.try_get("invocation_id")?,
            base_checkpoint_id: row.try_get("base_checkpoint_id")?,
            base_superstep: row.try_get("base_superstep")?,
            base_checkpoint_digest: row.try_get("base_checkpoint_digest")?,
            graph_namespace: row.try_get("graph_namespace")?,
            node_id: row.try_get("node_id")?,
            activation_input_digest: row.try_get("activation_input_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            intent_bytes: row.try_get("intent_bytes")?,
            current_revision: row.try_get("current_revision")?,
            current_status: row.try_get("current_status")?,
            current_attempt_id: row.try_get("current_attempt_id")?,
            current_record_digest: row.try_get("current_record_digest")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for ToolInvocationRevisionRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            invocation_id: row.try_get("invocation_id")?,
            revision: row.try_get("revision")?,
            previous_revision: row.try_get("previous_revision")?,
            previous_digest: row.try_get("previous_digest")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            status: row.try_get("status")?,
            attempt_id: row.try_get("attempt_id")?,
            transition_kind: row.try_get("transition_kind")?,
            started_attempt_id: row.try_get("started_attempt_id")?,
            transition_digest: row.try_get("transition_digest")?,
            record_digest: row.try_get("record_digest")?,
            record_bytes: row.try_get("record_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for ModelInvocationRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            invocation_id: row.try_get("invocation_id")?,
            base_checkpoint_id: row.try_get("base_checkpoint_id")?,
            base_superstep: row.try_get("base_superstep")?,
            base_checkpoint_digest: row.try_get("base_checkpoint_digest")?,
            graph_namespace: row.try_get("graph_namespace")?,
            node_id: row.try_get("node_id")?,
            activation_input_digest: row.try_get("activation_input_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            intent_bytes: row.try_get("intent_bytes")?,
            current_revision: row.try_get("current_revision")?,
            current_status: row.try_get("current_status")?,
            current_attempt_id: row.try_get("current_attempt_id")?,
            current_record_digest: row.try_get("current_record_digest")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for ModelInvocationRevisionRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            invocation_id: row.try_get("invocation_id")?,
            revision: row.try_get("revision")?,
            previous_revision: row.try_get("previous_revision")?,
            previous_digest: row.try_get("previous_digest")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            status: row.try_get("status")?,
            attempt_id: row.try_get("attempt_id")?,
            transition_kind: row.try_get("transition_kind")?,
            started_attempt_id: row.try_get("started_attempt_id")?,
            transition_digest: row.try_get("transition_digest")?,
            record_digest: row.try_get("record_digest")?,
            record_bytes: row.try_get("record_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for NodeAttemptStartRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            base_checkpoint_id: row.try_get("base_checkpoint_id")?,
            base_superstep: row.try_get("base_superstep")?,
            base_checkpoint_digest: row.try_get("base_checkpoint_digest")?,
            base_journal_sequence: row.try_get("base_journal_sequence")?,
            base_journal_event_id: row.try_get("base_journal_event_id")?,
            base_journal_recorded_at: row.try_get("base_journal_recorded_at")?,
            base_journal_digest: row.try_get("base_journal_digest")?,
            graph_namespace: row.try_get("graph_namespace")?,
            node_id: row.try_get("node_id")?,
            activation_input_digest: row.try_get("activation_input_digest")?,
            activation_digest: row.try_get("activation_digest")?,
            attempt_id: row.try_get("attempt_id")?,
            fence_attempt_id: row.try_get("fence_attempt_id")?,
            fence_epoch: row.try_get("fence_epoch")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            start_digest: row.try_get("start_digest")?,
            start_bytes: row.try_get("start_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for NodeAttemptCompletionRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            attempt_id: row.try_get("attempt_id")?,
            base_checkpoint_id: row.try_get("base_checkpoint_id")?,
            base_superstep: row.try_get("base_superstep")?,
            base_checkpoint_digest: row.try_get("base_checkpoint_digest")?,
            graph_namespace: row.try_get("graph_namespace")?,
            node_id: row.try_get("node_id")?,
            activation_input_digest: row.try_get("activation_input_digest")?,
            activation_digest: row.try_get("activation_digest")?,
            fence_attempt_id: row.try_get("fence_attempt_id")?,
            fence_epoch: row.try_get("fence_epoch")?,
            start_journal_sequence: row.try_get("start_journal_sequence")?,
            start_journal_event_id: row.try_get("start_journal_event_id")?,
            start_journal_recorded_at: row.try_get("start_journal_recorded_at")?,
            start_journal_digest: row.try_get("start_journal_digest")?,
            start_digest: row.try_get("start_digest")?,
            status: row.try_get("status")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            result_intent_digest: row.try_get("result_intent_digest")?,
            result_record_digest: row.try_get("result_record_digest")?,
            failure_id: row.try_get("failure_id")?,
            retry_kind: row.try_get("retry_kind")?,
            retry_not_before: row.try_get("retry_not_before")?,
            completion_digest: row.try_get("completion_digest")?,
            completion_bytes: row.try_get("completion_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for PendingNodeResultRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            base_checkpoint_id: row.try_get("base_checkpoint_id")?,
            base_superstep: row.try_get("base_superstep")?,
            base_checkpoint_digest: row.try_get("base_checkpoint_digest")?,
            base_journal_sequence: row.try_get("base_journal_sequence")?,
            base_journal_event_id: row.try_get("base_journal_event_id")?,
            base_journal_recorded_at: row.try_get("base_journal_recorded_at")?,
            base_journal_digest: row.try_get("base_journal_digest")?,
            graph_namespace: row.try_get("graph_namespace")?,
            node_id: row.try_get("node_id")?,
            activation_input_digest: row.try_get("activation_input_digest")?,
            node_attempt_id: row.try_get("node_attempt_id")?,
            intent_digest: row.try_get("intent_digest")?,
            control_kind: row.try_get("control_kind")?,
            fence_attempt_id: row.try_get("fence_attempt_id")?,
            fence_epoch: row.try_get("fence_epoch")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            record_digest: row.try_get("record_digest")?,
            result_bytes: row.try_get("result_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for PendingNodeResultHeadRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            base_checkpoint_id: row.try_get("base_checkpoint_id")?,
            base_superstep: row.try_get("base_superstep")?,
            base_checkpoint_digest: row.try_get("base_checkpoint_digest")?,
            graph_namespace: row.try_get("graph_namespace")?,
            node_id: row.try_get("node_id")?,
            activation_input_digest: row.try_get("activation_input_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            fence_attempt_id: row.try_get("fence_attempt_id")?,
            fence_epoch: row.try_get("fence_epoch")?,
            journal_sequence: row.try_get("journal_sequence")?,
            journal_event_id: row.try_get("journal_event_id")?,
            journal_recorded_at: row.try_get("journal_recorded_at")?,
            journal_digest: row.try_get("journal_digest")?,
            record_digest: row.try_get("record_digest")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for PendingNodeResultConsumptionRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            base_checkpoint_id: row.try_get("base_checkpoint_id")?,
            base_superstep: row.try_get("base_superstep")?,
            base_checkpoint_digest: row.try_get("base_checkpoint_digest")?,
            graph_namespace: row.try_get("graph_namespace")?,
            node_id: row.try_get("node_id")?,
            result_record_digest: row.try_get("result_record_digest")?,
            successor_checkpoint_id: row.try_get("successor_checkpoint_id")?,
            successor_superstep: row.try_get("successor_superstep")?,
            successor_checkpoint_digest: row.try_get("successor_checkpoint_digest")?,
            successor_journal_sequence: row.try_get("successor_journal_sequence")?,
            successor_journal_event_id: row.try_get("successor_journal_event_id")?,
            successor_journal_recorded_at: row.try_get("successor_journal_recorded_at")?,
            successor_journal_digest: row.try_get("successor_journal_digest")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for PendingNodeResultBindingRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            base_checkpoint_id: row.try_get("base_checkpoint_id")?,
            base_superstep: row.try_get("base_superstep")?,
            base_checkpoint_digest: row.try_get("base_checkpoint_digest")?,
            graph_namespace: row.try_get("graph_namespace")?,
            node_id: row.try_get("node_id")?,
            activation_input_digest: row.try_get("activation_input_digest")?,
            result_record_digest: row.try_get("result_record_digest")?,
            result_journal_sequence: row.try_get("result_journal_sequence")?,
            result_journal_recorded_at: row.try_get("result_journal_recorded_at")?,
            result_journal_digest: row.try_get("result_journal_digest")?,
            invocation_id: row.try_get("invocation_id")?,
            invocation_revision: row.try_get("invocation_revision")?,
            invocation_record_digest: row.try_get("invocation_record_digest")?,
            invocation_journal_sequence: row.try_get("invocation_journal_sequence")?,
            invocation_journal_recorded_at: row.try_get("invocation_journal_recorded_at")?,
            invocation_journal_digest: row.try_get("invocation_journal_digest")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for OutboxDestinationRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            destination_id: row.try_get("destination_id")?,
            snapshot_digest: row.try_get("snapshot_digest")?,
            config_kind: row.try_get("config_kind")?,
            schema_id: row.try_get("schema_id")?,
            schema_version: row.try_get("schema_version")?,
            schema_digest: row.try_get("schema_digest")?,
            config_bytes: row.try_get("config_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for OutboxDeliveryRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            delivery_id: row.try_get("delivery_id")?,
            origin_sequence: row.try_get("origin_sequence")?,
            origin_event_id: row.try_get("origin_event_id")?,
            origin_recorded_at: row.try_get("origin_recorded_at")?,
            origin_digest: row.try_get("origin_digest")?,
            destination_id: row.try_get("destination_id")?,
            destination_snapshot_digest: row.try_get("destination_snapshot_digest")?,
            intent_digest: row.try_get("intent_digest")?,
            expires_at: row.try_get("expires_at")?,
            delivery_digest: row.try_get("delivery_digest")?,
            delivery_bytes: row.try_get("delivery_bytes")?,
            status: row.try_get("status")?,
            attempt_count: row.try_get("attempt_count")?,
            current_attempt_id: row.try_get("current_attempt_id")?,
            current_epoch: row.try_get("current_epoch")?,
            current_attempt_started_at: row.try_get("current_attempt_started_at")?,
            current_attempt_expires_at: row.try_get("current_attempt_expires_at")?,
            next_attempt_at: row.try_get("next_attempt_at")?,
            last_completion_digest: row.try_get("last_completion_digest")?,
            terminal_at: row.try_get("terminal_at")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for OutboxAttemptStartRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            delivery_id: row.try_get("delivery_id")?,
            delivery_expires_at: row.try_get("delivery_expires_at")?,
            delivery_digest: row.try_get("delivery_digest")?,
            epoch: row.try_get("epoch")?,
            attempt_id: row.try_get("attempt_id")?,
            started_at: row.try_get("started_at")?,
            expires_at: row.try_get("expires_at")?,
            start_digest: row.try_get("start_digest")?,
            start_bytes: row.try_get("start_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'row> FromRow<'row, PgRow> for OutboxAttemptCompletionRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            run_id: row.try_get("run_id")?,
            delivery_id: row.try_get("delivery_id")?,
            epoch: row.try_get("epoch")?,
            attempt_id: row.try_get("attempt_id")?,
            started_at: row.try_get("started_at")?,
            attempt_expires_at: row.try_get("attempt_expires_at")?,
            start_digest: row.try_get("start_digest")?,
            outcome_kind: row.try_get("outcome_kind")?,
            retry_advice_kind: row.try_get("retry_advice_kind")?,
            retry_delay_millis: row.try_get("retry_delay_millis")?,
            completed_at: row.try_get("completed_at")?,
            completion_digest: row.try_get("completion_digest")?,
            completion_bytes: row.try_get("completion_bytes")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

struct PreparedProjection {
    lifecycle_bytes: Vec<u8>,
    revision: String,
    status: &'static str,
    changed_at: DateTime<Utc>,
    wait_set_digest: Option<Digest>,
    unresolved_wait_count: i16,
    next_timer_due_at: Option<DateTime<Utc>>,
    next_interrupt_expiry_at: Option<DateTime<Utc>>,
}

struct WaitSetProjection {
    digest: Option<Digest>,
    count: i16,
    next_timer_due_at: Option<Timestamp>,
    next_interrupt_expiry_at: Option<Timestamp>,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProjectionDigestWire<'a> {
    Unchanged,
    Transition {
        expected_revision: &'a RunRevision,
        transition: &'a RunTransition,
    },
}

#[derive(Serialize)]
struct AgentAdmissionProjectionDigestWire {
    admission: Digest,
    event_intent: Digest,
    checkpoint_intent: Digest,
    lifecycle: Digest,
}

#[derive(Serialize)]
struct BarrierProjectionDigestWire {
    run_projection_digest: Digest,
    barrier_intent_digest: Digest,
}

#[derive(Serialize)]
struct WaitRegistrationProjectionDigestWire<'a> {
    expected_revision: &'a RunRevision,
    checkpoint_intent_digest: Digest,
    registrations: &'a [WaitRegistrationProjectionItem],
}

#[derive(Serialize)]
struct WaitBarrierProjectionDigestWire<'a> {
    expected_revision: &'a RunRevision,
    barrier_intent_digest: Digest,
    registrations: &'a [WaitRegistrationProjectionItem],
}

#[derive(Serialize)]
struct WaitRegistrationProjectionItem {
    wait_kind: &'static str,
    wait_id: String,
    intent_digest: Digest,
}

#[derive(Serialize)]
struct WaitTerminalProjectionDigestWire<'a> {
    expected_revision: &'a RunRevision,
    intent_digest: Digest,
}

#[derive(Serialize)]
struct WaitAbandonmentProjectionDigestWire<'a> {
    run_projection_digest: Digest,
    registrations: &'a [WaitAbandonmentProjectionItem],
}

#[derive(Serialize)]
struct WaitAbandonmentProjectionItem {
    wait_kind: &'static str,
    wait_id: String,
    registration_digest: Digest,
}

#[derive(Serialize)]
struct WaitAbandonmentDigestWire<'a> {
    registration_digest: Digest,
    reason: WaitAbandonmentReason,
    journal: &'a JournalHead,
}

#[derive(Serialize)]
struct RunQuarantineDigestWireV1<'a> {
    schema_version: u8,
    tenant_id: &'a TenantId,
    run_id: RunId,
    quarantine_id: QuarantineId,
    expectation: &'a stateknot_core::JournalExpectation,
    cause: RunQuarantineCause,
    component: &'a str,
    evidence_digest: Digest,
    quarantined_at: Timestamp,
}

#[derive(Serialize)]
struct RunQuarantineDigestWireV2<'a> {
    schema_version: u8,
    tenant_id: &'a TenantId,
    run_id: RunId,
    quarantine_id: QuarantineId,
    expectation: &'a stateknot_core::JournalExpectation,
    expected_fence: &'a RunFence,
    cause: RunQuarantineCause,
    component: &'a str,
    evidence_digest: Digest,
    quarantined_at: Timestamp,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ModelInvocationRecordWire {
    schema: String,
    intent_digest: Digest,
    revision: ModelInvocationRevision,
    state: ModelInvocationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous: Option<ModelInvocationHead>,
    journal_head: JournalHead,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition: Option<ModelInvocationTransition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition_digest: Option<Digest>,
    digest: Digest,
}

async fn apply_transaction_timeouts(
    transaction: &mut Transaction<'_, Postgres>,
    options: &PostgresStoreOptions,
    operation: &'static str,
) -> Result<(), StoreError> {
    query(
        "SELECT set_config('lock_timeout', $1, true), \
                set_config('statement_timeout', $2, true), \
                set_config('idle_in_transaction_session_timeout', $2, true), \
                set_config('synchronous_commit', 'on', true)",
    )
    .bind(options.lock_timeout_setting())
    .bind(options.statement_timeout_setting())
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database(operation, source))?;
    Ok(())
}

async fn database_now(
    transaction: &mut Transaction<'_, Postgres>,
    operation: &'static str,
) -> Result<Timestamp, StoreError> {
    let value = query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| StoreError::database(operation, source))?;
    from_database_time(value)
}

async fn database_scheduler_times(
    transaction: &mut Transaction<'_, Postgres>,
    operation: &'static str,
) -> Result<(Timestamp, Timestamp), StoreError> {
    let (transaction_started_at, observed_at) = query_as::<_, (DateTime<Utc>, DateTime<Utc>)>(
        "SELECT transaction_timestamp(), clock_timestamp()",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| StoreError::database(operation, source))?;
    Ok((
        from_database_time(transaction_started_at)?,
        from_database_time(observed_at)?,
    ))
}

async fn fetch_locked_run_row(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
) -> Result<RunRow, StoreError> {
    query_as::<_, RunRow>(SELECT_RUN_FOR_UPDATE)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("run row lock", source))?
        .ok_or(StoreError::RunNotFound)
}

async fn load_run_quarantine_row_by_run(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
) -> Result<Option<RunQuarantineRow>, StoreError> {
    query_as::<_, RunQuarantineRow>(SELECT_RUN_QUARANTINE_BY_RUN)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("run quarantine evidence load", source))
}

async fn load_run_quarantine_row_by_id(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    quarantine_id: QuarantineId,
) -> Result<Option<RunQuarantineRow>, StoreError> {
    query_as::<_, RunQuarantineRow>(SELECT_RUN_QUARANTINE_BY_ID)
        .bind(tenant_id.as_str())
        .bind(*quarantine_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("run quarantine identity load", source))
}

fn quarantine_expectation_matches(
    target: &RunQuarantineTargetRow,
    expectation: &stateknot_core::JournalExpectation,
) -> Result<bool, StoreError> {
    match expectation.head() {
        None => Ok(target.journal_sequence.is_none()
            && target.journal_event_id.is_none()
            && target.journal_recorded_at.is_none()
            && target.journal_digest.is_none()),
        Some(head) => {
            let sequence = i64::try_from(head.sequence().get())
                .map_err(|_| StoreError::JournalSequenceExhausted)?;
            Ok(target.tenant_id == head.tenant_id().as_str()
                && target.run_id == *head.run_id().as_uuid()
                && target.journal_sequence == Some(sequence)
                && target.journal_event_id == Some(*head.event_id().as_uuid())
                && target.journal_recorded_at == Some(to_database_time(head.recorded_at())?)
                && target.journal_digest.as_deref() == Some(head.digest().as_bytes()))
        }
    }
}

fn authorize_quarantine_fence(
    target: &RunQuarantineTargetRow,
    expected: Option<&RunFence>,
    observed_at: Timestamp,
) -> Result<(), StoreError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    if target.tenant_id != expected.tenant_id().as_str()
        || target.run_id != *expected.run_id().as_uuid()
    {
        return Err(StoreError::InvalidRunQuarantineRequest);
    }
    let expected_epoch =
        i64::try_from(expected.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    match (
        target.lease_attempt_id,
        target.lease_renewed_at,
        target.lease_expires_at,
    ) {
        (Some(attempt_id), Some(renewed_at), Some(expires_at)) => {
            if attempt_id != *expected.attempt_id().as_uuid()
                || target.fencing_epoch != expected_epoch
            {
                return Err(StoreError::StaleFence);
            }
            let renewed_at = from_database_time(renewed_at)?;
            let expires_at = from_database_time(expires_at)?;
            if observed_at < renewed_at {
                return Err(StoreError::DatabaseClockRegression);
            }
            if observed_at >= expires_at {
                return Err(StoreError::LeaseExpired);
            }
            Ok(())
        }
        (None, None, None) => Err(StoreError::StaleFence),
        _ => Err(StoreError::corrupt("run quarantine target lease")),
    }
}

fn journal_expectation_matches_stored(
    stored: &StoredRun,
    expectation: &stateknot_core::JournalExpectation,
) -> bool {
    match expectation.head() {
        None => stored.journal_head().is_none(),
        Some(expected) => stored.journal_head() == Some(expected),
    }
}

fn run_quarantine_cause_from_text(value: &str) -> Result<RunQuarantineCause, StoreError> {
    match value {
        "integrity_failure" => Ok(RunQuarantineCause::IntegrityFailure),
        "unsupported_schema" => Ok(RunQuarantineCause::UnsupportedSchema),
        "missing_artifact" => Ok(RunQuarantineCause::MissingArtifact),
        "cross_tenant_reference" => Ok(RunQuarantineCause::CrossTenantReference),
        "projection_mismatch" => Ok(RunQuarantineCause::ProjectionMismatch),
        "fencing_epoch_exhausted" => Ok(RunQuarantineCause::FencingEpochExhausted),
        "operator_policy" => Ok(RunQuarantineCause::OperatorPolicy),
        _ => Err(StoreError::corrupt("run quarantine cause")),
    }
}

fn run_quarantine_reason(request: &RunQuarantineRequest) -> String {
    format!(
        "{}:{}",
        request.cause().as_str(),
        request.component().as_str()
    )
}

fn materialize_run_quarantine(
    request: RunQuarantineRequest,
    quarantined_at: Timestamp,
) -> Result<RunQuarantine, StoreError> {
    if request
        .expectation()
        .head()
        .is_some_and(|head| head.recorded_at() > quarantined_at)
    {
        return Err(StoreError::DatabaseClockRegression);
    }
    let (domain, canonical) = if let Some(expected_fence) = request.expected_fence() {
        (
            RUN_QUARANTINE_DIGEST_DOMAIN_V2,
            serde_json_canonicalizer::to_vec(&RunQuarantineDigestWireV2 {
                schema_version: 2,
                tenant_id: request.tenant_id(),
                run_id: request.run_id(),
                quarantine_id: request.quarantine_id(),
                expectation: request.expectation(),
                expected_fence,
                cause: request.cause(),
                component: request.component().as_str(),
                evidence_digest: request.evidence_digest(),
                quarantined_at,
            })
            .map_err(|_| StoreError::encoding("run quarantine"))?,
        )
    } else {
        (
            RUN_QUARANTINE_DIGEST_DOMAIN_V1,
            serde_json_canonicalizer::to_vec(&RunQuarantineDigestWireV1 {
                schema_version: 1,
                tenant_id: request.tenant_id(),
                run_id: request.run_id(),
                quarantine_id: request.quarantine_id(),
                expectation: request.expectation(),
                cause: request.cause(),
                component: request.component().as_str(),
                evidence_digest: request.evidence_digest(),
                quarantined_at,
            })
            .map_err(|_| StoreError::encoding("run quarantine"))?,
        )
    };
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(RunQuarantine {
        request,
        quarantined_at,
        digest: Digest::sha256(preimage),
    })
}

async fn insert_run_quarantine(
    transaction: &mut Transaction<'_, Postgres>,
    quarantine: &RunQuarantine,
) -> Result<(), StoreError> {
    let request = quarantine.request();
    let expected = request.expectation().head();
    let expected_sequence = expected
        .map(|head| {
            i64::try_from(head.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)
        })
        .transpose()?;
    let expected_event_id = expected.map(|head| *head.event_id().as_uuid());
    let expected_recorded_at = expected
        .map(|head| to_database_time(head.recorded_at()))
        .transpose()?;
    let expected_digest = expected.map(|head| head.digest().as_bytes().to_vec());
    let expected_fence_attempt_id = request
        .expected_fence()
        .map(|fence| *fence.attempt_id().as_uuid());
    let expected_fence_epoch = request
        .expected_fence()
        .map(|fence| i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence))
        .transpose()?;
    query(
        r"
INSERT INTO stateknot.run_quarantines (
    tenant_id,
    run_id,
    quarantine_id,
    quarantined_at,
    cause_kind,
    component,
    evidence_digest,
    expected_journal_sequence,
    expected_journal_event_id,
    expected_journal_recorded_at,
    expected_journal_digest,
    expected_fence_attempt_id,
    expected_fence_epoch,
    record_digest,
    created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $4)
",
    )
    .bind(request.tenant_id().as_str())
    .bind(*request.run_id().as_uuid())
    .bind(*request.quarantine_id().as_uuid())
    .bind(to_database_time(quarantine.quarantined_at())?)
    .bind(request.cause().as_str())
    .bind(request.component().as_str())
    .bind(request.evidence_digest().as_bytes())
    .bind(expected_sequence)
    .bind(expected_event_id)
    .bind(expected_recorded_at)
    .bind(expected_digest)
    .bind(expected_fence_attempt_id)
    .bind(expected_fence_epoch)
    .bind(quarantine.digest().as_bytes())
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("run quarantine evidence insert", source))?;
    Ok(())
}

async fn commit_run_quarantine_projection(
    transaction: &mut Transaction<'_, Postgres>,
    quarantine: &RunQuarantine,
) -> Result<(), StoreError> {
    let request = quarantine.request();
    let expected_fence_attempt_id = request
        .expected_fence()
        .map(|fence| *fence.attempt_id().as_uuid());
    let expected_fence_epoch = request
        .expected_fence()
        .map(|fence| i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence))
        .transpose()?;
    let updated = query(
        r"
UPDATE stateknot.runs
SET quarantined_at = $3,
    quarantine_reason = $4,
    lease_attempt_id = NULL,
    lease_acquired_at = NULL,
    lease_renewed_at = NULL,
    lease_expires_at = NULL,
    updated_at = GREATEST(updated_at, $3)
WHERE tenant_id = $1
  AND run_id = $2
  AND quarantined_at IS NULL
  AND (
      $5::uuid IS NULL
      OR (
          lease_attempt_id = $5
          AND fencing_epoch = $6
          AND lease_expires_at > clock_timestamp()
      )
  )
",
    )
    .bind(request.tenant_id().as_str())
    .bind(*request.run_id().as_uuid())
    .bind(to_database_time(quarantine.quarantined_at())?)
    .bind(run_quarantine_reason(request))
    .bind(expected_fence_attempt_id)
    .bind(expected_fence_epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("run quarantine projection update", source))?
    .rows_affected();
    if updated == 1 {
        return Ok(());
    }
    Err(if request.expected_fence().is_some() {
        StoreError::LeaseExpired
    } else {
        StoreError::RunQuarantineCommitConflict
    })
}

fn decode_run_quarantine(row: &RunQuarantineRow) -> Result<RunQuarantine, StoreError> {
    let tenant_id = TenantId::try_from(row.tenant_id.clone())
        .map_err(|_| StoreError::corrupt("run quarantine tenant"))?;
    let run_id = RunId::from_uuid(row.run_id)
        .map_err(|_| StoreError::corrupt("run quarantine run identity"))?;
    let quarantine_id = QuarantineId::from_uuid(row.quarantine_id)
        .map_err(|_| StoreError::corrupt("run quarantine identity"))?;
    let expectation = match (
        row.expected_journal_sequence,
        row.expected_journal_event_id,
        row.expected_journal_recorded_at,
        row.expected_journal_digest.as_deref(),
    ) {
        (None, None, None, None) => stateknot_core::JournalExpectation::empty(),
        (Some(sequence), Some(event_id), Some(recorded_at), Some(digest)) => {
            stateknot_core::JournalExpectation::exact(JournalHead::new(
                tenant_id.clone(),
                run_id,
                positive_sequence(sequence)?,
                EventId::from_uuid(event_id)
                    .map_err(|_| StoreError::corrupt("run quarantine event identity"))?,
                from_database_time(recorded_at)?,
                decode_digest(digest, "run quarantine expected journal digest")?,
            ))
        }
        _ => return Err(StoreError::corrupt("run quarantine journal shape")),
    };
    let cause = run_quarantine_cause_from_text(&row.cause_kind)?;
    let component = RunQuarantineComponent::new(row.component.clone())
        .map_err(|_| StoreError::corrupt("run quarantine component"))?;
    let expected_fence = match (row.expected_fence_attempt_id, row.expected_fence_epoch) {
        (None, None) => None,
        (Some(attempt_id), Some(epoch)) => Some(RunFence::new(
            tenant_id.clone(),
            run_id,
            AttemptId::from_uuid(attempt_id)
                .map_err(|_| StoreError::corrupt("run quarantine fence attempt"))?,
            FencingEpoch::new(
                u64::try_from(epoch)
                    .map_err(|_| StoreError::corrupt("run quarantine fence epoch"))?,
            )
            .map_err(|_| StoreError::corrupt("run quarantine fence epoch"))?,
        )),
        _ => return Err(StoreError::corrupt("run quarantine fence shape")),
    };
    let mut request = RunQuarantineRequest::new(
        tenant_id,
        run_id,
        quarantine_id,
        expectation,
        cause,
        component,
        decode_digest(&row.evidence_digest, "run quarantine evidence digest")?,
    )
    .map_err(|_| StoreError::corrupt("run quarantine request"))?;
    if let Some(expected_fence) = expected_fence {
        request = request
            .with_expected_fence(expected_fence)
            .map_err(|_| StoreError::corrupt("run quarantine request fence"))?;
    }
    let quarantine = materialize_run_quarantine(request, from_database_time(row.quarantined_at)?)
        .map_err(|_| StoreError::corrupt("run quarantine record"))?;
    let reason = run_quarantine_reason(quarantine.request());
    let expected_fencing_epoch = quarantine
        .request()
        .expected_fence()
        .map(|fence| {
            i64::try_from(fence.epoch().get())
                .map_err(|_| StoreError::corrupt("run quarantine fence epoch projection"))
        })
        .transpose()?;
    if row.created_at != row.quarantined_at
        || decode_digest(&row.record_digest, "run quarantine record digest")? != quarantine.digest()
        || row.run_quarantined_at != Some(row.quarantined_at)
        || row.run_quarantine_reason.as_deref() != Some(reason.as_str())
        || row.run_lease_attempt_id.is_some()
        || row.run_lease_acquired_at.is_some()
        || row.run_lease_renewed_at.is_some()
        || row.run_lease_expires_at.is_some()
        || expected_fencing_epoch.is_some_and(|epoch| row.run_fencing_epoch != epoch)
        || row.run_updated_at < row.quarantined_at
        || row
            .run_scheduler_ready_at
            .is_some_and(|ready_at| ready_at > row.run_updated_at)
    {
        return Err(StoreError::corrupt("run quarantine projection"));
    }
    Ok(quarantine)
}

async fn load_and_verify_wait_registration(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
    wait_id: Uuid,
) -> Result<DurableWait, StoreError> {
    let row = query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID.as_str())
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(wait_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("wait registration load", source))?
        .ok_or(StoreError::WaitRegistrationNotFound)?;
    let sequence = row.registration_sequence;
    let wait = decode_wait_registration(&row)?;
    verify_wait_registration_event(transaction, &wait, sequence).await?;
    if wait.tenant_id() != tenant_id
        || wait.run_id() != run_id
        || durable_wait_identity(&wait) != wait_id
    {
        return Err(StoreError::corrupt("wait registration event anchor"));
    }
    Ok(wait)
}

async fn verify_wait_registration_event(
    transaction: &mut Transaction<'_, Postgres>,
    wait: &DurableWait,
    sequence: i64,
) -> Result<(), StoreError> {
    let event_row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(wait.tenant_id().as_str())
        .bind(*wait.run_id().as_uuid())
        .bind(sequence)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("wait registration event load", source))?
        .ok_or_else(|| StoreError::corrupt("wait registration event anchor"))?;
    let event = decode_event(event_row)?;
    if wait.journal() != &event.head() {
        return Err(StoreError::corrupt("wait registration event anchor"));
    }
    Ok(())
}

async fn verify_current_wait_set(
    transaction: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
) -> Result<(), StoreError> {
    if stored.is_quarantined()
        && stored.lifecycle().status() == RunStatus::Waiting
        && stored.unresolved_wait_count() == 0
    {
        return Ok(());
    }
    let provenance = stored.lifecycle().provenance();
    let rows = query_as::<_, WaitRegistrationRow>(SELECT_OUTSTANDING_WAIT_REGISTRATIONS.as_str())
        .bind(provenance.tenant_id().as_str())
        .bind(*provenance.run_id().as_uuid())
        .fetch_all(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("current wait-set load", source))?;
    let expected = stored.lifecycle().waits();
    if rows.len() != expected.map_or(0, RunWaits::len) {
        return Err(StoreError::corrupt("current durable wait set"));
    }
    let mut durable = BTreeMap::new();
    for row in rows {
        let sequence = row.registration_sequence;
        let wait = decode_wait_registration(&row)?;
        verify_wait_registration_event(transaction, &wait, sequence).await?;
        let marker = wait.marker();
        if durable.insert(run_wait_identity(&marker), marker).is_some() {
            return Err(StoreError::corrupt("current durable wait set"));
        }
    }
    if expected.is_some_and(|waits| {
        waits
            .iter()
            .any(|marker| durable.get(&run_wait_identity(marker)) != Some(marker))
    }) || !durable.is_empty() && expected.is_none()
    {
        return Err(StoreError::corrupt("current durable wait set"));
    }
    Ok(())
}

async fn load_discovery_wait_owner<'owners>(
    transaction: &mut Transaction<'_, Postgres>,
    owners: &'owners mut BTreeMap<RunId, StoredRun>,
    tenant_id: &TenantId,
    run_id: RunId,
) -> Result<&'owners StoredRun, StoreError> {
    match owners.entry(run_id) {
        std::collections::btree_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
        std::collections::btree_map::Entry::Vacant(entry) => {
            let row = query_as::<_, RunRow>(SELECT_RUN)
                .bind(tenant_id.as_str())
                .bind(*run_id.as_uuid())
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|source| StoreError::database("wait discovery owner load", source))?
                .ok_or_else(|| StoreError::corrupt("wait discovery owner"))?;
            let owner = decode_run(row)?;
            if owner.lifecycle().provenance().tenant_id() != tenant_id
                || owner.lifecycle().provenance().run_id() != run_id
                || owner.lifecycle().status() != RunStatus::Waiting
                || owner.is_quarantined()
            {
                return Err(StoreError::corrupt("wait discovery owner"));
            }
            verify_current_wait_set(transaction, &owner).await?;
            Ok(entry.insert(owner))
        }
    }
}

fn wait_discovery_key_after<Identity: Ord>(
    current: &(Timestamp, RunId, Identity),
    previous: &(Timestamp, RunId, Identity),
) -> bool {
    current > previous
}

async fn load_interrupt_record_from_row(
    transaction: &mut Transaction<'_, Postgres>,
    registration: &WaitRegistrationRow,
) -> Result<InterruptRecord, StoreError> {
    let wait = decode_wait_registration(registration)?;
    let DurableWait::Interrupt { request } = wait else {
        return Err(StoreError::WaitRegistrationKindMismatch);
    };
    verify_wait_registration_event(
        transaction,
        &DurableWait::Interrupt {
            request: request.clone(),
        },
        registration.registration_sequence,
    )
    .await?;
    let row = query_as::<_, InterruptResolutionRow>(SELECT_INTERRUPT_RESOLUTION)
        .bind(request.intent().tenant_id().as_str())
        .bind(*request.intent().run_id().as_uuid())
        .bind(*request.marker().interrupt_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("interrupt resolution load", source))?;
    let resolution = match (registration.status.as_str(), row) {
        ("outstanding", None) => None,
        ("abandoned", None) => return Err(StoreError::WaitWasAbandoned),
        ("resolved", Some(row)) => {
            let resolution = decode_interrupt_resolution(&row, &request)?;
            if registration
                .resolution_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "interrupt projected resolution digest"))
                .transpose()?
                != Some(resolution.digest())
                || registration.terminal_sequence != Some(row.resolution_sequence)
                || registration.terminal_event_id != Some(row.resolution_event_id)
                || registration.terminal_recorded_at != Some(row.resolved_at)
                || registration.terminal_event_digest.as_deref()
                    != Some(row.resolution_event_digest.as_slice())
            {
                return Err(StoreError::corrupt("interrupt terminal projection"));
            }
            verify_terminal_event(transaction, resolution.journal()).await?;
            Some(resolution)
        }
        _ => return Err(StoreError::InterruptResolutionCommitConflict),
    };
    InterruptRecord::restore(*request, resolution)
        .map_err(|_| StoreError::corrupt("interrupt history"))
}

async fn load_timer_record_from_row(
    transaction: &mut Transaction<'_, Postgres>,
    registration: &WaitRegistrationRow,
) -> Result<DurableTimerRecord, StoreError> {
    let wait = decode_wait_registration(registration)?;
    let DurableWait::Timer { timer } = wait else {
        return Err(StoreError::WaitRegistrationKindMismatch);
    };
    verify_wait_registration_event(
        transaction,
        &DurableWait::Timer {
            timer: timer.clone(),
        },
        registration.registration_sequence,
    )
    .await?;
    let row = query_as::<_, TimerFiringRow>(SELECT_TIMER_FIRING)
        .bind(timer.intent().tenant_id().as_str())
        .bind(*timer.intent().run_id().as_uuid())
        .bind(*timer.marker().timer_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("timer firing load", source))?;
    let firing = match (registration.status.as_str(), row) {
        ("outstanding", None) => None,
        ("abandoned", None) => return Err(StoreError::WaitWasAbandoned),
        ("fired", Some(row)) => {
            let firing = decode_timer_firing(&row, &timer)?;
            if registration
                .firing_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "timer projected firing digest"))
                .transpose()?
                != Some(firing.digest())
                || registration.terminal_sequence != Some(row.firing_sequence)
                || registration.terminal_event_id != Some(row.firing_event_id)
                || registration.terminal_recorded_at != Some(row.fired_at)
                || registration.terminal_event_digest.as_deref()
                    != Some(row.firing_event_digest.as_slice())
            {
                return Err(StoreError::corrupt("timer terminal projection"));
            }
            verify_terminal_event(transaction, firing.journal()).await?;
            Some(firing)
        }
        _ => return Err(StoreError::TimerFiringCommitConflict),
    };
    DurableTimerRecord::restore(*timer, firing)
        .map_err(|_| StoreError::corrupt("durable timer history"))
}

async fn verify_terminal_event(
    transaction: &mut Transaction<'_, Postgres>,
    journal: &JournalHead,
) -> Result<(), StoreError> {
    let row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(journal.tenant_id().as_str())
        .bind(*journal.run_id().as_uuid())
        .bind(
            i64::try_from(journal.sequence().get())
                .map_err(|_| StoreError::corrupt("wait terminal sequence"))?,
        )
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("wait terminal event load", source))?
        .ok_or_else(|| StoreError::corrupt("wait terminal event anchor"))?;
    let event = decode_event(row)?;
    if event.head() != *journal {
        return Err(StoreError::corrupt("wait terminal event anchor"));
    }
    Ok(())
}

async fn load_locked_current_checkpoint(
    transaction: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
    tenant_id: &TenantId,
    run_id: RunId,
) -> Result<Option<Checkpoint>, StoreError> {
    let Some(pointer) = stored.checkpoint() else {
        return Ok(None);
    };
    let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(*pointer.checkpoint_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("locked checkpoint load", source))?
        .ok_or_else(|| StoreError::corrupt("locked checkpoint pointer"))?;
    let checkpoint = decode_checkpoint(row)?;
    if checkpoint.tenant_id() != tenant_id
        || checkpoint.run_id() != run_id
        || checkpoint.checkpoint_id() != pointer.checkpoint_id()
        || checkpoint.superstep() != pointer.superstep()
        || checkpoint.digest() != pointer.digest()
    {
        return Err(StoreError::corrupt("locked checkpoint projection"));
    }
    verify_checkpoint_anchor(transaction, &checkpoint).await?;
    Ok(Some(checkpoint))
}

async fn verify_checkpoint_anchor(
    transaction: &mut Transaction<'_, Postgres>,
    checkpoint: &Checkpoint,
) -> Result<(), StoreError> {
    let sequence = i64::try_from(checkpoint.journal_head().sequence().get())
        .map_err(|_| StoreError::corrupt("checkpoint journal sequence"))?;
    let row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(checkpoint.tenant_id().as_str())
        .bind(*checkpoint.run_id().as_uuid())
        .bind(sequence)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("checkpoint anchor verification", source))?
        .ok_or_else(|| StoreError::corrupt("checkpoint journal anchor"))?;
    let event = decode_event(row)?;
    if event.head() != *checkpoint.journal_head() {
        return Err(StoreError::corrupt("checkpoint journal anchor"));
    }
    Ok(())
}

fn decode_checkpoint_lineage(
    rows: Vec<CheckpointRow>,
    tenant_id: &TenantId,
    run_id: RunId,
    page_size: CheckpointLineagePageSize,
) -> Result<(Vec<Checkpoint>, Option<Checkpoint>), StoreError> {
    let mut checkpoints = rows
        .into_iter()
        .map(decode_checkpoint)
        .collect::<Result<Vec<_>, _>>()?;
    if checkpoints
        .iter()
        .any(|checkpoint| checkpoint.tenant_id() != tenant_id || checkpoint.run_id() != run_id)
    {
        return Err(StoreError::corrupt("checkpoint lineage scope"));
    }
    let lookahead = if checkpoints.len() > usize::from(page_size.get()) {
        checkpoints.pop()
    } else {
        None
    };
    Ok((checkpoints, lookahead))
}

async fn verify_checkpoint_anchors(
    transaction: &mut Transaction<'_, Postgres>,
    checkpoints: &[Checkpoint],
) -> Result<(), StoreError> {
    let first = checkpoints
        .first()
        .ok_or_else(|| StoreError::corrupt("checkpoint lineage page"))?;
    let mut sequences = Vec::with_capacity(checkpoints.len());
    for checkpoint in checkpoints {
        sequences.push(
            i64::try_from(checkpoint.journal_head().sequence().get())
                .map_err(|_| StoreError::corrupt("checkpoint journal sequence"))?,
        );
    }
    let rows = query_as::<_, EventRow>(SELECT_EVENTS_BY_SEQUENCES)
        .bind(first.tenant_id().as_str())
        .bind(*first.run_id().as_uuid())
        .bind(&sequences)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("checkpoint lineage anchors", source))?;
    if rows.len() != checkpoints.len() {
        return Err(StoreError::corrupt("checkpoint lineage anchors"));
    }

    let mut anchors = BTreeMap::new();
    for row in rows {
        let event = decode_event(row)?;
        if anchors.insert(event.sequence(), event).is_some() {
            return Err(StoreError::corrupt("checkpoint lineage anchors"));
        }
    }
    for checkpoint in checkpoints {
        let event = anchors
            .get(&checkpoint.journal_head().sequence())
            .ok_or_else(|| StoreError::corrupt("checkpoint lineage anchor"))?;
        if event.head() != *checkpoint.journal_head() {
            return Err(StoreError::corrupt("checkpoint lineage anchor"));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn decode_run(row: RunRow) -> Result<StoredRun, StoreError> {
    let tenant_id =
        TenantId::try_from(row.tenant_id).map_err(|_| StoreError::corrupt("run tenant"))?;
    let run_id = RunId::from_uuid(row.run_id).map_err(|_| StoreError::corrupt("run identity"))?;
    let thread_id = stateknot_core::ThreadId::from_uuid(row.thread_id)
        .map_err(|_| StoreError::corrupt("run thread identity"))?;
    let invocation_id = InvocationId::from_uuid(row.invocation_id)
        .map_err(|_| StoreError::corrupt("run invocation identity"))?;
    let lifecycle = decode_lifecycle(&row.lifecycle_bytes)?;
    let provenance = lifecycle.provenance();
    if provenance.tenant_id() != &tenant_id
        || provenance.run_id() != run_id
        || provenance.thread_id() != thread_id
        || provenance.invocation_id() != invocation_id
        || lifecycle.revision().to_string() != row.lifecycle_revision
        || run_status_text(lifecycle.status()) != row.lifecycle_status
        || lifecycle.admitted_at() != from_database_time(row.admitted_at)?
        || lifecycle.changed_at() != from_database_time(row.changed_at)?
    {
        return Err(StoreError::corrupt("run projection"));
    }

    let journal_head = match (
        row.journal_sequence,
        row.journal_event_id,
        row.journal_recorded_at,
        row.journal_digest,
    ) {
        (None, None, None, None) => None,
        (Some(sequence), Some(event_id), Some(recorded_at), Some(digest)) => {
            Some(JournalHead::new(
                tenant_id.clone(),
                run_id,
                positive_sequence(sequence)?,
                EventId::from_uuid(event_id)
                    .map_err(|_| StoreError::corrupt("journal head event identity"))?,
                from_database_time(recorded_at)?,
                decode_digest(&digest, "journal head digest")?,
            ))
        }
        _ => return Err(StoreError::corrupt("journal head shape")),
    };

    let checkpoint = match (
        row.checkpoint_id,
        row.checkpoint_superstep,
        row.checkpoint_digest,
    ) {
        (None, None, None) => None,
        (Some(checkpoint_id), Some(superstep), Some(digest)) => Some(CheckpointPointer {
            checkpoint_id: CheckpointId::from_uuid(checkpoint_id)
                .map_err(|_| StoreError::corrupt("checkpoint pointer identity"))?,
            superstep: nonnegative_superstep(superstep)?,
            digest: decode_digest(&digest, "checkpoint pointer digest")?,
        }),
        _ => return Err(StoreError::corrupt("checkpoint pointer shape")),
    };

    let last_fencing_epoch = if row.fencing_epoch == 0 {
        None
    } else {
        let value =
            u64::try_from(row.fencing_epoch).map_err(|_| StoreError::corrupt("fencing epoch"))?;
        Some(FencingEpoch::new(value).map_err(|_| StoreError::corrupt("fencing epoch"))?)
    };
    let lease = match (
        row.lease_attempt_id,
        row.lease_acquired_at,
        row.lease_renewed_at,
        row.lease_expires_at,
    ) {
        (None, None, None, None) => None,
        (Some(attempt_id), Some(acquired_at), Some(renewed_at), Some(expires_at)) => {
            let epoch = last_fencing_epoch.ok_or_else(|| StoreError::corrupt("lease epoch"))?;
            Some(
                RunLease::restore(
                    RunFence::new(
                        tenant_id,
                        run_id,
                        AttemptId::from_uuid(attempt_id)
                            .map_err(|_| StoreError::corrupt("lease attempt identity"))?,
                        epoch,
                    ),
                    from_database_time(acquired_at)?,
                    from_database_time(renewed_at)?,
                    from_database_time(expires_at)?,
                )
                .map_err(|_| StoreError::corrupt("run lease"))?,
            )
        }
        _ => return Err(StoreError::corrupt("lease shape")),
    };
    let (scheduler_ready_at, scheduler_not_before) =
        decode_scheduler_readiness(row.scheduler_ready_at, row.scheduler_not_before, &lifecycle)?;
    if lease.is_some() && scheduler_not_before.is_some() {
        return Err(StoreError::corrupt(
            "run scheduler delayed retry lease shape",
        ));
    }
    let quarantined = row.quarantined_at.is_some();
    let expected_waits = wait_set_projection(&lifecycle)
        .map_err(|()| StoreError::corrupt("run wait-set projection"))?;
    let stored_wait_digest = row
        .wait_set_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "run wait-set digest"))
        .transpose()?;
    let stored_next_timer_due_at = row.next_timer_due_at.map(from_database_time).transpose()?;
    let stored_next_interrupt_expiry_at = row
        .next_interrupt_expiry_at
        .map(from_database_time)
        .transpose()?;
    let legacy_quarantined_wait = quarantined
        && lifecycle.status() == RunStatus::Waiting
        && stored_wait_digest.is_none()
        && row.unresolved_wait_count == 0
        && stored_next_timer_due_at.is_none()
        && stored_next_interrupt_expiry_at.is_none();
    if !legacy_quarantined_wait
        && (stored_wait_digest != expected_waits.digest
            || row.unresolved_wait_count != expected_waits.count
            || stored_next_timer_due_at != expected_waits.next_timer_due_at
            || stored_next_interrupt_expiry_at != expected_waits.next_interrupt_expiry_at)
    {
        return Err(StoreError::corrupt("run wait-set projection"));
    }
    let unresolved_wait_count = u8::try_from(row.unresolved_wait_count)
        .map_err(|_| StoreError::corrupt("run wait-set count"))?;

    Ok(StoredRun {
        lifecycle,
        journal_head,
        lease,
        last_fencing_epoch,
        checkpoint,
        scheduler_ready_at,
        scheduler_not_before,
        wait_set_digest: stored_wait_digest,
        unresolved_wait_count,
        next_timer_due_at: stored_next_timer_due_at,
        next_interrupt_expiry_at: stored_next_interrupt_expiry_at,
        quarantined,
    })
}

fn wait_set_projection(lifecycle: &RunLifecycle) -> Result<WaitSetProjection, ()> {
    let Some(waits) = lifecycle.waits() else {
        return Ok(WaitSetProjection {
            digest: None,
            count: 0,
            next_timer_due_at: None,
            next_interrupt_expiry_at: None,
        });
    };
    let canonical = serde_json_canonicalizer::to_vec(waits).map_err(|_| ())?;
    let mut preimage = Vec::with_capacity(WAIT_SET_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(WAIT_SET_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    let next_timer_due_at = waits
        .iter()
        .filter_map(|wait| wait.as_timer().map(stateknot_core::RunTimer::due_at))
        .min();
    let next_interrupt_expiry_at = waits
        .iter()
        .filter_map(|wait| {
            wait.as_interrupt()
                .and_then(stateknot_core::RunInterrupt::expires_at)
        })
        .min();
    Ok(WaitSetProjection {
        digest: Some(Digest::sha256(preimage)),
        count: i16::try_from(waits.len()).map_err(|_| ())?,
        next_timer_due_at,
        next_interrupt_expiry_at,
    })
}

fn decode_scheduler_readiness(
    scheduler_ready_at: Option<DateTime<Utc>>,
    scheduler_not_before: Option<DateTime<Utc>>,
    lifecycle: &RunLifecycle,
) -> Result<(Option<Timestamp>, Option<Timestamp>), StoreError> {
    let scheduler_ready_at = scheduler_ready_at.map(from_database_time).transpose()?;
    let scheduler_not_before = scheduler_not_before.map(from_database_time).transpose()?;
    if lifecycle_is_scheduler_runnable(lifecycle.status()) {
        let ready_at = scheduler_ready_at
            .ok_or_else(|| StoreError::corrupt("run scheduler readiness shape"))?;
        if ready_at < lifecycle.admitted_at() {
            return Err(StoreError::corrupt("run scheduler readiness time"));
        }
        if scheduler_not_before.is_some_and(|not_before| not_before < ready_at) {
            return Err(StoreError::corrupt("run scheduler delayed retry shape"));
        }
    } else if scheduler_ready_at.is_some() || scheduler_not_before.is_some() {
        return Err(StoreError::corrupt("run scheduler readiness shape"));
    }
    Ok((scheduler_ready_at, scheduler_not_before))
}

fn encode_lifecycle(lifecycle: &RunLifecycle) -> Result<Vec<u8>, StoreError> {
    let value =
        serde_json::to_value(lifecycle).map_err(|_| StoreError::encoding("run lifecycle"))?;
    let bounded = BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM)
        .map_err(|_| StoreError::encoding("run lifecycle"))?;
    let canonical =
        CanonicalJson::new(&bounded).map_err(|_| StoreError::encoding("run lifecycle"))?;
    Ok(canonical.as_bytes().to_vec())
}

fn encode_durable_wait(wait: &DurableWait) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(wait)
        .map_err(|_| StoreError::encoding("durable wait registration"))?;
    if bytes.is_empty() || bytes.len() > MAX_WAIT_REGISTRATION_BYTES {
        return Err(StoreError::InvalidWaitRegistrationBatch);
    }
    Ok(bytes)
}

fn durable_wait_identity(wait: &DurableWait) -> Uuid {
    match wait {
        DurableWait::Interrupt { request } => *request.marker().interrupt_id().as_uuid(),
        DurableWait::Timer { timer } => *timer.marker().timer_id().as_uuid(),
    }
}

fn run_wait_identity(wait: &stateknot_core::RunWait) -> Uuid {
    match wait {
        stateknot_core::RunWait::Interrupt { interrupt } => *interrupt.interrupt_id().as_uuid(),
        stateknot_core::RunWait::Timer { timer } => *timer.timer_id().as_uuid(),
    }
}

fn durable_wait_digest(wait: &DurableWait) -> Digest {
    match wait {
        DurableWait::Interrupt { request } => request.digest(),
        DurableWait::Timer { timer } => timer.digest(),
    }
}

fn durable_wait_kind_text(wait: &DurableWait) -> &'static str {
    match wait {
        DurableWait::Interrupt { .. } => "interrupt",
        DurableWait::Timer { .. } => "timer",
    }
}

const fn wait_abandonment_reason_text(reason: WaitAbandonmentReason) -> &'static str {
    match reason {
        WaitAbandonmentReason::RunCancellation => "run_cancellation",
        WaitAbandonmentReason::RunFailure => "run_failure",
    }
}

fn wait_abandonment_reason_from_text(value: &str) -> Result<WaitAbandonmentReason, StoreError> {
    match value {
        "run_cancellation" => Ok(WaitAbandonmentReason::RunCancellation),
        "run_failure" => Ok(WaitAbandonmentReason::RunFailure),
        _ => Err(StoreError::corrupt("wait abandonment reason")),
    }
}

fn materialize_wait_abandonment(
    wait: DurableWait,
    reason: WaitAbandonmentReason,
    journal: JournalHead,
) -> Result<WaitAbandonment, StoreError> {
    if wait.tenant_id() != journal.tenant_id()
        || wait.run_id() != journal.run_id()
        || journal.sequence() <= wait.journal().sequence()
        || journal.recorded_at() < wait.journal().recorded_at()
    {
        return Err(StoreError::InvalidWaitAbandonment);
    }
    let canonical = serde_json_canonicalizer::to_vec(&WaitAbandonmentDigestWire {
        registration_digest: durable_wait_digest(&wait),
        reason,
        journal: &journal,
    })
    .map_err(|_| StoreError::encoding("wait abandonment"))?;
    let mut preimage = Vec::with_capacity(WAIT_ABANDONMENT_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(WAIT_ABANDONMENT_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(WaitAbandonment {
        wait,
        reason,
        journal,
        digest: Digest::sha256(preimage),
    })
}

fn decode_wait_abandonment(
    row: &WaitAbandonmentRow,
    wait: DurableWait,
) -> Result<WaitAbandonment, StoreError> {
    let reason = wait_abandonment_reason_from_text(&row.reason_kind)?;
    let journal = JournalHead::new(
        wait.tenant_id().clone(),
        wait.run_id(),
        positive_sequence(row.abandonment_sequence)?,
        EventId::from_uuid(row.abandonment_event_id)
            .map_err(|_| StoreError::corrupt("wait abandonment event identity"))?,
        from_database_time(row.abandoned_at)?,
        decode_digest(
            &row.abandonment_event_digest,
            "wait abandonment event digest",
        )?,
    );
    let abandonment = materialize_wait_abandonment(wait, reason, journal)?;
    if row.tenant_id != abandonment.wait().tenant_id().as_str()
        || row.run_id != *abandonment.wait().run_id().as_uuid()
        || row.wait_id != durable_wait_identity(abandonment.wait())
        || row.wait_kind != durable_wait_kind_text(abandonment.wait())
        || decode_digest(
            &row.registration_digest,
            "wait abandonment registration digest",
        )? != durable_wait_digest(abandonment.wait())
        || row.created_at != row.abandoned_at
        || decode_digest(&row.abandonment_digest, "wait abandonment digest")?
            != abandonment.digest()
    {
        return Err(StoreError::corrupt("wait abandonment projection"));
    }
    Ok(abandonment)
}

fn decode_wait_registration(row: &WaitRegistrationRow) -> Result<DurableWait, StoreError> {
    let wait = serde_json::from_slice::<DurableWait>(&row.record_bytes)
        .map_err(|_| StoreError::corrupt("durable wait registration bytes"))?;
    let canonical = serde_json_canonicalizer::to_vec(&wait)
        .map_err(|_| StoreError::corrupt("durable wait registration bytes"))?;
    if canonical != row.record_bytes
        || row.tenant_id != wait.tenant_id().as_str()
        || row.run_id != *wait.run_id().as_uuid()
        || row.wait_id != durable_wait_identity(&wait)
        || from_database_time(row.registered_at)? != wait.journal().recorded_at()
        || row.registration_sequence
            != i64::try_from(wait.journal().sequence().get())
                .map_err(|_| StoreError::corrupt("wait registration sequence"))?
        || row.registration_event_id != *wait.journal().event_id().as_uuid()
        || decode_digest(
            &row.registration_event_digest,
            "wait registration event digest",
        )? != wait.journal().digest()
        || row.created_at != row.registered_at
        || row.updated_at < row.created_at
    {
        return Err(StoreError::corrupt("durable wait registration projection"));
    }

    match &wait {
        DurableWait::Interrupt { request } => {
            let marker = request.marker();
            if row.wait_kind != "interrupt"
                || row.interrupt_kind.as_deref() != Some(interrupt_kind_text(marker.kind()))
                || row.timer_kind.is_some()
                || row.due_at.is_some()
                || row.expires_at.map(from_database_time).transpose()? != marker.expires_at()
                || row
                    .action_digest
                    .as_deref()
                    .map(|bytes| decode_digest(bytes, "interrupt action digest"))
                    .transpose()?
                    != Some(request.intent().action_digest())
                || decode_digest(&row.intent_digest, "interrupt registration intent digest")?
                    != request.intent().intent_digest()
                || decode_digest(&row.record_digest, "interrupt request digest")?
                    != request.digest()
            {
                return Err(StoreError::corrupt("interrupt registration projection"));
            }
        }
        DurableWait::Timer { timer } => {
            let marker = timer.marker();
            if row.wait_kind != "timer"
                || row.interrupt_kind.is_some()
                || row.timer_kind.as_deref() != Some(timer_kind_text(marker.kind()))
                || row.due_at.map(from_database_time).transpose()? != Some(marker.due_at())
                || row.expires_at.is_some()
                || row.action_digest.is_some()
                || decode_digest(&row.intent_digest, "timer registration intent digest")?
                    != timer.intent().intent_digest()
                || decode_digest(&row.record_digest, "durable timer digest")? != timer.digest()
            {
                return Err(StoreError::corrupt("timer registration projection"));
            }
        }
    }
    validate_wait_terminal_projection(row, &wait)?;
    Ok(wait)
}

fn validate_wait_terminal_projection(
    row: &WaitRegistrationRow,
    wait: &DurableWait,
) -> Result<(), StoreError> {
    let terminal_shape = (
        row.terminal_sequence,
        row.terminal_event_id,
        row.terminal_recorded_at,
        row.terminal_event_digest.as_deref(),
    );
    match row.status.as_str() {
        "outstanding" => {
            if terminal_shape != (None, None, None, None)
                || row.resolution_digest.is_some()
                || row.firing_digest.is_some()
                || row.abandonment_digest.is_some()
                || row.updated_at != row.registered_at
            {
                return Err(StoreError::corrupt("outstanding wait projection"));
            }
        }
        "resolved" | "fired" | "abandoned" => {
            let (Some(sequence), Some(event_id), Some(recorded_at), Some(event_digest)) =
                terminal_shape
            else {
                return Err(StoreError::corrupt("terminal wait projection"));
            };
            if sequence <= row.registration_sequence
                || stateknot_core::EventId::from_uuid(event_id).is_err()
                || recorded_at < row.registered_at
                || row.updated_at != recorded_at
                || decode_digest(event_digest, "terminal wait event digest").is_err()
            {
                return Err(StoreError::corrupt("terminal wait projection"));
            }
            let exact_kind = match (row.status.as_str(), wait) {
                ("resolved", DurableWait::Interrupt { request }) => {
                    row.resolution_digest.is_some()
                        && row.firing_digest.is_none()
                        && row.abandonment_digest.is_none()
                        && request.marker().expires_at().is_none_or(|expires_at| {
                            from_database_time(recorded_at)
                                .is_ok_and(|resolved_at| resolved_at < expires_at)
                        })
                }
                ("fired", DurableWait::Timer { timer }) => {
                    row.resolution_digest.is_none()
                        && row.firing_digest.is_some()
                        && row.abandonment_digest.is_none()
                        && from_database_time(recorded_at)
                            .is_ok_and(|fired_at| fired_at >= timer.marker().due_at())
                }
                ("abandoned", _) => {
                    row.resolution_digest.is_none()
                        && row.firing_digest.is_none()
                        && row.abandonment_digest.is_some()
                }
                _ => false,
            };
            if !exact_kind {
                return Err(StoreError::corrupt("terminal wait projection"));
            }
        }
        _ => return Err(StoreError::corrupt("wait registration status")),
    }
    Ok(())
}

fn encode_interrupt_resolution(resolution: &InterruptResolution) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(resolution)
        .map_err(|_| StoreError::encoding("interrupt resolution"))?;
    if bytes.is_empty() || bytes.len() > MAX_INTERRUPT_RESOLUTION_BYTES {
        return Err(StoreError::InvalidInterruptResolution);
    }
    Ok(bytes)
}

fn decode_interrupt_resolution(
    row: &InterruptResolutionRow,
    request: &InterruptRequest,
) -> Result<InterruptResolution, StoreError> {
    let resolution = serde_json::from_slice::<InterruptResolution>(&row.resolution_bytes)
        .map_err(|_| StoreError::corrupt("interrupt resolution bytes"))?;
    let canonical = serde_json_canonicalizer::to_vec(&resolution)
        .map_err(|_| StoreError::corrupt("interrupt resolution bytes"))?;
    let journal = resolution.journal();
    if canonical != row.resolution_bytes
        || row.tenant_id != request.intent().tenant_id().as_str()
        || row.run_id != *request.intent().run_id().as_uuid()
        || row.interrupt_id != *request.marker().interrupt_id().as_uuid()
        || decode_digest(&row.request_digest, "interrupt resolution request digest")?
            != request.digest()
        || row.resolution_sequence
            != i64::try_from(journal.sequence().get())
                .map_err(|_| StoreError::corrupt("interrupt resolution sequence"))?
        || row.resolution_event_id != *journal.event_id().as_uuid()
        || from_database_time(row.resolved_at)? != journal.recorded_at()
        || decode_digest(
            &row.resolution_event_digest,
            "interrupt resolution event digest",
        )? != journal.digest()
        || decode_digest(&row.intent_digest, "interrupt resolution intent digest")?
            != resolution.intent().intent_digest()
        || decode_digest(&row.resolution_digest, "interrupt resolution digest")?
            != resolution.digest()
        || row.created_at != row.resolved_at
        || resolution.intent().request() != &request.head()
    {
        return Err(StoreError::corrupt("interrupt resolution projection"));
    }
    Ok(resolution)
}

fn encode_timer_firing(firing: &TimerFiring) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(firing)
        .map_err(|_| StoreError::encoding("timer firing"))?;
    if bytes.is_empty() || bytes.len() > MAX_TIMER_FIRING_BYTES {
        return Err(StoreError::InvalidTimerFiring);
    }
    Ok(bytes)
}

fn decode_timer_firing(
    row: &TimerFiringRow,
    timer: &DurableTimer,
) -> Result<TimerFiring, StoreError> {
    let firing = serde_json::from_slice::<TimerFiring>(&row.firing_bytes)
        .map_err(|_| StoreError::corrupt("timer firing bytes"))?;
    let canonical = serde_json_canonicalizer::to_vec(&firing)
        .map_err(|_| StoreError::corrupt("timer firing bytes"))?;
    let journal = firing.journal();
    if canonical != row.firing_bytes
        || row.tenant_id != timer.intent().tenant_id().as_str()
        || row.run_id != *timer.intent().run_id().as_uuid()
        || row.timer_id != *timer.marker().timer_id().as_uuid()
        || decode_digest(&row.timer_digest, "timer firing registration digest")? != timer.digest()
        || row.firing_sequence
            != i64::try_from(journal.sequence().get())
                .map_err(|_| StoreError::corrupt("timer firing sequence"))?
        || row.firing_event_id != *journal.event_id().as_uuid()
        || from_database_time(row.fired_at)? != journal.recorded_at()
        || decode_digest(&row.firing_event_digest, "timer firing event digest")? != journal.digest()
        || decode_digest(&row.intent_digest, "timer firing intent digest")?
            != firing.intent().intent_digest()
        || decode_digest(&row.firing_digest, "timer firing digest")? != firing.digest()
        || row.created_at != row.fired_at
        || firing.intent().timer() != &timer.head()
    {
        return Err(StoreError::corrupt("timer firing projection"));
    }
    Ok(firing)
}

fn decode_lifecycle(bytes: &[u8]) -> Result<RunLifecycle, StoreError> {
    let bounded = BoundedJson::from_slice_with_limits(bytes, JsonLimits::MAXIMUM)
        .map_err(|_| StoreError::corrupt("run lifecycle bytes"))?;
    let canonical =
        CanonicalJson::new(&bounded).map_err(|_| StoreError::corrupt("run lifecycle bytes"))?;
    if canonical.as_bytes() != bytes {
        return Err(StoreError::corrupt("run lifecycle canonical bytes"));
    }
    serde_json::from_value(bounded.into_value())
        .map_err(|_| StoreError::corrupt("run lifecycle value"))
}

fn encode_checkpoint(checkpoint: &Checkpoint) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(checkpoint)
        .map_err(|_| StoreError::encoding("checkpoint"))?;
    if bytes.is_empty() || bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(StoreError::encoding("checkpoint size"));
    }
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn decode_checkpoint(row: CheckpointRow) -> Result<Checkpoint, StoreError> {
    if row.checkpoint_bytes.is_empty() || row.checkpoint_bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(StoreError::corrupt("checkpoint byte length"));
    }
    let checkpoint = serde_json::from_slice::<Checkpoint>(&row.checkpoint_bytes)
        .map_err(|_| StoreError::corrupt("checkpoint value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&checkpoint)
        .map_err(|_| StoreError::corrupt("checkpoint canonicalization"))?;
    if canonical != row.checkpoint_bytes {
        return Err(StoreError::corrupt("checkpoint canonical bytes"));
    }

    let tenant_id =
        TenantId::try_from(row.tenant_id).map_err(|_| StoreError::corrupt("checkpoint tenant"))?;
    let run_id =
        RunId::from_uuid(row.run_id).map_err(|_| StoreError::corrupt("checkpoint run identity"))?;
    let checkpoint_id = CheckpointId::from_uuid(row.checkpoint_id)
        .map_err(|_| StoreError::corrupt("checkpoint identity"))?;
    let superstep = nonnegative_superstep(row.superstep)?;
    let journal_sequence = positive_sequence(row.journal_sequence)?;
    let journal_event_id = EventId::from_uuid(row.journal_event_id)
        .map_err(|_| StoreError::corrupt("checkpoint journal event identity"))?;
    let journal_recorded_at = from_database_time(row.journal_recorded_at)?;
    let journal_digest = decode_digest(&row.journal_digest, "checkpoint journal digest")?;
    let graph_definition_digest = decode_digest(
        &row.graph_definition_digest,
        "checkpoint graph definition digest",
    )?;
    let state_schema_digest =
        decode_digest(&row.state_schema_digest, "checkpoint state schema digest")?;
    let state_digest = decode_digest(&row.state_digest, "checkpoint state digest")?;
    let intent_digest = decode_digest(&row.intent_digest, "checkpoint intent digest")?;
    let checkpoint_digest = decode_digest(&row.checkpoint_digest, "checkpoint complete digest")?;

    let parent_matches = match (
        checkpoint.parent(),
        row.parent_checkpoint_id,
        row.parent_superstep,
        row.parent_digest,
    ) {
        (None, None, None, None) => true,
        (Some(parent), Some(parent_id), Some(parent_superstep), Some(parent_digest)) => {
            CheckpointId::from_uuid(parent_id).ok() == Some(parent.checkpoint_id())
                && nonnegative_superstep(parent_superstep).ok() == Some(parent.superstep())
                && decode_digest(&parent_digest, "checkpoint parent digest").ok()
                    == Some(parent.digest())
        }
        _ => false,
    };

    let schema = checkpoint.graph().state_schema();
    if checkpoint.tenant_id() != &tenant_id
        || checkpoint.run_id() != run_id
        || checkpoint.checkpoint_id() != checkpoint_id
        || checkpoint.superstep() != superstep
        || !parent_matches
        || checkpoint.journal_head().sequence() != journal_sequence
        || checkpoint.journal_head().event_id() != journal_event_id
        || checkpoint.journal_head().recorded_at() != journal_recorded_at
        || checkpoint.journal_head().digest() != journal_digest
        || checkpoint.graph().definition_digest() != graph_definition_digest
        || schema.id().as_str() != row.state_schema_id
        || schema.version().to_string() != row.state_schema_version
        || schema.digest() != state_schema_digest
        || checkpoint.state().digest() != state_digest
        || checkpoint.intent_digest() != intent_digest
        || checkpoint.digest() != checkpoint_digest
    {
        return Err(StoreError::corrupt("checkpoint projection"));
    }
    Ok(checkpoint)
}

fn encode_tool_invocation_intent(intent: &ToolInvocationIntent) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(intent)
        .map_err(|_| StoreError::encoding("tool invocation intent"))?;
    if bytes.is_empty() || bytes.len() > MAX_TOOL_INVOCATION_INTENT_BYTES {
        return Err(StoreError::encoding("tool invocation intent size"));
    }
    Ok(bytes)
}

fn encode_tool_invocation_record(invocation: &ToolInvocation) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(invocation)
        .map_err(|_| StoreError::encoding("tool invocation record"))?;
    if bytes.is_empty() || bytes.len() > MAX_TOOL_INVOCATION_RECORD_BYTES {
        return Err(StoreError::encoding("tool invocation record size"));
    }
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn decode_tool_invocation_intent(
    row: &ToolInvocationRow,
) -> Result<ToolInvocationIntent, StoreError> {
    if row.intent_bytes.is_empty() || row.intent_bytes.len() > MAX_TOOL_INVOCATION_INTENT_BYTES {
        return Err(StoreError::corrupt("tool invocation intent byte length"));
    }
    let intent = serde_json::from_slice::<ToolInvocationIntent>(&row.intent_bytes)
        .map_err(|_| StoreError::corrupt("tool invocation intent value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&intent)
        .map_err(|_| StoreError::corrupt("tool invocation intent canonicalization"))?;
    if canonical != row.intent_bytes {
        return Err(StoreError::corrupt(
            "tool invocation intent canonical bytes",
        ));
    }

    let tenant_id = TenantId::try_from(row.tenant_id.as_str())
        .map_err(|_| StoreError::corrupt("tool invocation tenant"))?;
    let run_id = RunId::from_uuid(row.run_id)
        .map_err(|_| StoreError::corrupt("tool invocation run identity"))?;
    let invocation_id = InvocationId::from_uuid(row.invocation_id)
        .map_err(|_| StoreError::corrupt("tool invocation identity"))?;
    let base_checkpoint_id = CheckpointId::from_uuid(row.base_checkpoint_id)
        .map_err(|_| StoreError::corrupt("tool invocation base checkpoint identity"))?;
    let base_superstep = nonnegative_superstep(row.base_superstep)?;
    let base_digest = decode_digest(
        &row.base_checkpoint_digest,
        "tool invocation base checkpoint digest",
    )?;
    let activation = intent.activation();
    if intent.tenant_id() != &tenant_id
        || intent.run_id() != run_id
        || intent.invocation_id() != invocation_id
        || activation.base_checkpoint().checkpoint_id() != base_checkpoint_id
        || activation.base_checkpoint().superstep() != base_superstep
        || activation.base_checkpoint().digest() != base_digest
        || activation.graph_namespace().as_str() != row.graph_namespace
        || activation.node_id().as_str() != row.node_id
        || activation.input_digest()
            != decode_digest(
                &row.activation_input_digest,
                "tool invocation activation input digest",
            )?
        || intent.intent_digest()
            != decode_digest(&row.intent_digest, "tool invocation intent digest")?
    {
        return Err(StoreError::corrupt("tool invocation intent projection"));
    }
    from_database_time(row.created_at)?;
    from_database_time(row.updated_at)?;
    Ok(intent)
}

#[allow(clippy::too_many_lines)]
fn decode_tool_invocation_revision(
    row: ToolInvocationRevisionRow,
    intent: &ToolInvocationIntent,
) -> Result<ToolInvocation, StoreError> {
    if row.record_bytes.is_empty() || row.record_bytes.len() > MAX_TOOL_INVOCATION_RECORD_BYTES {
        return Err(StoreError::corrupt("tool invocation record byte length"));
    }
    let invocation = serde_json::from_slice::<ToolInvocation>(&row.record_bytes)
        .map_err(|_| StoreError::corrupt("tool invocation record value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&invocation)
        .map_err(|_| StoreError::corrupt("tool invocation record canonicalization"))?;
    if canonical != row.record_bytes {
        return Err(StoreError::corrupt(
            "tool invocation record canonical bytes",
        ));
    }

    let tenant_id = TenantId::try_from(row.tenant_id)
        .map_err(|_| StoreError::corrupt("tool invocation revision tenant"))?;
    let run_id = RunId::from_uuid(row.run_id)
        .map_err(|_| StoreError::corrupt("tool invocation revision run identity"))?;
    let invocation_id = InvocationId::from_uuid(row.invocation_id)
        .map_err(|_| StoreError::corrupt("tool invocation revision identity"))?;
    let revision = nonnegative_tool_invocation_revision(row.revision)?;
    let previous_matches = match (
        invocation.previous(),
        row.previous_revision,
        row.previous_digest,
    ) {
        (None, None, None) => true,
        (Some(previous), Some(previous_revision), Some(previous_digest)) => {
            nonnegative_tool_invocation_revision(previous_revision).ok()
                == Some(previous.revision())
                && decode_digest(&previous_digest, "tool invocation predecessor digest").ok()
                    == Some(previous.digest())
        }
        _ => false,
    };
    let journal_sequence = positive_sequence(row.journal_sequence)?;
    let journal_event_id = EventId::from_uuid(row.journal_event_id)
        .map_err(|_| StoreError::corrupt("tool invocation journal event identity"))?;
    let journal_recorded_at = from_database_time(row.journal_recorded_at)?;
    let journal_digest = decode_digest(&row.journal_digest, "tool invocation journal digest")?;
    let attempt_id = row
        .attempt_id
        .map(AttemptId::from_uuid)
        .transpose()
        .map_err(|_| StoreError::corrupt("tool invocation attempt identity"))?;
    let started_attempt_id = row
        .started_attempt_id
        .map(AttemptId::from_uuid)
        .transpose()
        .map_err(|_| StoreError::corrupt("tool invocation started attempt identity"))?;
    let transition_kind = invocation.transition().map(ToolInvocationTransition::kind);
    let expected_started = match invocation.transition() {
        Some(ToolInvocationTransition::StartAttempt { attempt_id }) => Some(*attempt_id),
        _ => None,
    };
    let transition_digest = row
        .transition_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "tool invocation transition digest"))
        .transpose()?;

    if invocation.intent() != intent
        || invocation.intent().tenant_id() != &tenant_id
        || invocation.intent().run_id() != run_id
        || invocation.intent().invocation_id() != invocation_id
        || invocation.revision() != revision
        || !previous_matches
        || invocation.journal_head().sequence() != journal_sequence
        || invocation.journal_head().event_id() != journal_event_id
        || invocation.journal_head().recorded_at() != journal_recorded_at
        || invocation.journal_head().digest() != journal_digest
        || tool_invocation_status_text(invocation.status()) != row.status
        || invocation.attempt_id() != attempt_id
        || transition_kind.map(tool_invocation_transition_kind_text)
            != row.transition_kind.as_deref()
        || expected_started != started_attempt_id
        || invocation.transition_digest() != transition_digest
        || invocation.digest()
            != decode_digest(&row.record_digest, "tool invocation record digest")?
        || from_database_time(row.created_at)? != invocation.journal_head().recorded_at()
    {
        return Err(StoreError::corrupt("tool invocation revision projection"));
    }
    Ok(invocation)
}

fn nonnegative_tool_invocation_revision(value: i64) -> Result<ToolInvocationRevision, StoreError> {
    let value =
        u64::try_from(value).map_err(|_| StoreError::corrupt("tool invocation revision"))?;
    ToolInvocationRevision::new(value).map_err(|_| StoreError::corrupt("tool invocation revision"))
}

fn validate_tool_invocation_current_projection(
    row: &ToolInvocationRow,
    current: &ToolInvocation,
) -> Result<(), StoreError> {
    let current_revision = nonnegative_tool_invocation_revision(row.current_revision)?;
    let current_attempt = row
        .current_attempt_id
        .map(AttemptId::from_uuid)
        .transpose()
        .map_err(|_| StoreError::corrupt("tool invocation current attempt"))?;
    let current_digest = decode_digest(
        &row.current_record_digest,
        "tool invocation current record digest",
    )?;
    if current.revision() != current_revision
        || tool_invocation_status_text(current.status()) != row.current_status
        || current.attempt_id() != current_attempt
        || current.digest() != current_digest
        || from_database_time(row.updated_at)? != current.journal_head().recorded_at()
    {
        return Err(StoreError::corrupt("tool invocation current projection"));
    }
    Ok(())
}

async fn load_tool_invocation_revision_row(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    revision: ToolInvocationRevision,
) -> Result<ToolInvocationRevisionRow, StoreError> {
    let revision = i64::try_from(revision.get())
        .map_err(|_| StoreError::corrupt("tool invocation revision"))?;
    query_as::<_, ToolInvocationRevisionRow>(SELECT_TOOL_INVOCATION_REVISION)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(*invocation_id.as_uuid())
        .bind(revision)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("tool invocation revision load", source))?
        .ok_or(StoreError::ToolInvocationNotFound)
}

async fn verify_tool_invocation_base_checkpoint(
    transaction: &mut Transaction<'_, Postgres>,
    intent: &ToolInvocationIntent,
) -> Result<(), StoreError> {
    let head = intent.activation().base_checkpoint();
    let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
        .bind(intent.tenant_id().as_str())
        .bind(*intent.run_id().as_uuid())
        .bind(*head.checkpoint_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("tool invocation base checkpoint", source))?
        .ok_or_else(|| StoreError::corrupt("tool invocation base checkpoint"))?;
    let checkpoint = decode_checkpoint(row)?;
    if checkpoint.head() != *head || !tool_invocation_activation_is_ready(&checkpoint, intent) {
        return Err(StoreError::corrupt("tool invocation base checkpoint"));
    }
    verify_checkpoint_anchor(transaction, &checkpoint).await
}

fn tool_invocation_activation_is_ready(
    checkpoint: &Checkpoint,
    intent: &ToolInvocationIntent,
) -> bool {
    activation_is_canonical_ready_root(checkpoint, intent.activation())
}

async fn ensure_no_unsettled_tool_invocations(
    transaction: &mut Transaction<'_, Postgres>,
    checkpoint: &Checkpoint,
) -> Result<(), StoreError> {
    let superstep = i64::try_from(checkpoint.superstep().get())
        .map_err(|_| StoreError::corrupt("checkpoint superstep"))?;
    let exists = query_scalar::<_, bool>(SELECT_UNSETTLED_TOOL_INVOCATION_EXISTS)
        .bind(checkpoint.tenant_id().as_str())
        .bind(*checkpoint.run_id().as_uuid())
        .bind(*checkpoint.checkpoint_id().as_uuid())
        .bind(superstep)
        .bind(checkpoint.digest().as_bytes())
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("tool invocation barrier check", source))?;
    if exists {
        return Err(StoreError::CheckpointBlockedByToolInvocation);
    }
    Ok(())
}

async fn verify_tool_invocation_anchor(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ToolInvocation,
) -> Result<(), StoreError> {
    let sequence = i64::try_from(invocation.journal_head().sequence().get())
        .map_err(|_| StoreError::corrupt("tool invocation journal sequence"))?;
    let row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(invocation.intent().tenant_id().as_str())
        .bind(*invocation.intent().run_id().as_uuid())
        .bind(sequence)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("tool invocation anchor", source))?
        .ok_or_else(|| StoreError::corrupt("tool invocation journal anchor"))?;
    let projection_digest = row
        .projection_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "tool invocation projection digest"))
        .transpose()?;
    let event = decode_event(row)?;
    if event.head() != *invocation.journal_head() || projection_digest != Some(invocation.digest())
    {
        return Err(StoreError::corrupt("tool invocation journal anchor"));
    }
    Ok(())
}

fn encode_model_invocation_intent(intent: &ModelInvocationIntent) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(intent)
        .map_err(|_| StoreError::encoding("model invocation intent"))?;
    if bytes.is_empty() || bytes.len() > MAX_MODEL_INVOCATION_INTENT_BYTES {
        return Err(StoreError::encoding("model invocation intent size"));
    }
    Ok(bytes)
}

fn model_invocation_record_wire(invocation: &ModelInvocation) -> ModelInvocationRecordWire {
    ModelInvocationRecordWire {
        schema: MODEL_INVOCATION_RECORD_SCHEMA.to_owned(),
        intent_digest: invocation.intent().intent_digest(),
        revision: invocation.revision(),
        state: invocation.state().clone(),
        previous: invocation.previous().cloned(),
        journal_head: invocation.journal_head().clone(),
        transition: invocation.transition().cloned(),
        transition_digest: invocation.transition_digest(),
        digest: invocation.digest(),
    }
}

fn encode_model_invocation_record(invocation: &ModelInvocation) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(&model_invocation_record_wire(invocation))
        .map_err(|_| StoreError::encoding("model invocation record"))?;
    if bytes.is_empty() || bytes.len() > MAX_MODEL_INVOCATION_RECORD_BYTES {
        return Err(StoreError::encoding("model invocation record size"));
    }
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn decode_model_invocation_intent(
    row: &ModelInvocationRow,
) -> Result<ModelInvocationIntent, StoreError> {
    if row.intent_bytes.is_empty() || row.intent_bytes.len() > MAX_MODEL_INVOCATION_INTENT_BYTES {
        return Err(StoreError::corrupt("model invocation intent byte length"));
    }
    let intent = serde_json::from_slice::<ModelInvocationIntent>(&row.intent_bytes)
        .map_err(|_| StoreError::corrupt("model invocation intent value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&intent)
        .map_err(|_| StoreError::corrupt("model invocation intent canonicalization"))?;
    if canonical != row.intent_bytes {
        return Err(StoreError::corrupt(
            "model invocation intent canonical bytes",
        ));
    }

    let tenant_id = TenantId::try_from(row.tenant_id.as_str())
        .map_err(|_| StoreError::corrupt("model invocation tenant"))?;
    let run_id = RunId::from_uuid(row.run_id)
        .map_err(|_| StoreError::corrupt("model invocation run identity"))?;
    let invocation_id = InvocationId::from_uuid(row.invocation_id)
        .map_err(|_| StoreError::corrupt("model invocation identity"))?;
    let base_checkpoint_id = CheckpointId::from_uuid(row.base_checkpoint_id)
        .map_err(|_| StoreError::corrupt("model invocation base checkpoint identity"))?;
    let base_superstep = nonnegative_superstep(row.base_superstep)?;
    let base_digest = decode_digest(
        &row.base_checkpoint_digest,
        "model invocation base checkpoint digest",
    )?;
    let activation = intent.activation();
    if intent.tenant_id() != &tenant_id
        || intent.run_id() != run_id
        || intent.invocation_id() != invocation_id
        || activation.base_checkpoint().checkpoint_id() != base_checkpoint_id
        || activation.base_checkpoint().superstep() != base_superstep
        || activation.base_checkpoint().digest() != base_digest
        || activation.graph_namespace().as_str() != row.graph_namespace
        || activation.node_id().as_str() != row.node_id
        || activation.input_digest()
            != decode_digest(
                &row.activation_input_digest,
                "model invocation activation input digest",
            )?
        || intent.intent_digest()
            != decode_digest(&row.intent_digest, "model invocation intent digest")?
    {
        return Err(StoreError::corrupt("model invocation intent projection"));
    }
    from_database_time(row.created_at)?;
    from_database_time(row.updated_at)?;
    Ok(intent)
}

#[allow(clippy::too_many_lines)]
fn decode_model_invocation_revision(
    row: ModelInvocationRevisionRow,
    intent: &ModelInvocationIntent,
) -> Result<ModelInvocation, StoreError> {
    if row.record_bytes.is_empty() || row.record_bytes.len() > MAX_MODEL_INVOCATION_RECORD_BYTES {
        return Err(StoreError::corrupt("model invocation record byte length"));
    }
    let wire = serde_json::from_slice::<ModelInvocationRecordWire>(&row.record_bytes)
        .map_err(|_| StoreError::corrupt("model invocation record value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&wire)
        .map_err(|_| StoreError::corrupt("model invocation record canonicalization"))?;
    if canonical != row.record_bytes {
        return Err(StoreError::corrupt(
            "model invocation record canonical bytes",
        ));
    }
    if wire.schema != MODEL_INVOCATION_RECORD_SCHEMA || wire.intent_digest != intent.intent_digest()
    {
        return Err(StoreError::corrupt("model invocation record format"));
    }
    let invocation = ModelInvocation::restore(
        intent.clone(),
        wire.revision,
        wire.state,
        wire.previous,
        wire.journal_head,
        wire.transition,
        wire.transition_digest,
        wire.digest,
    )
    .map_err(|_| StoreError::corrupt("model invocation record integrity"))?;

    let tenant_id = TenantId::try_from(row.tenant_id)
        .map_err(|_| StoreError::corrupt("model invocation revision tenant"))?;
    let run_id = RunId::from_uuid(row.run_id)
        .map_err(|_| StoreError::corrupt("model invocation revision run identity"))?;
    let invocation_id = InvocationId::from_uuid(row.invocation_id)
        .map_err(|_| StoreError::corrupt("model invocation revision identity"))?;
    let revision = nonnegative_model_invocation_revision(row.revision)?;
    let previous_matches = match (
        invocation.previous(),
        row.previous_revision,
        row.previous_digest,
    ) {
        (None, None, None) => true,
        (Some(previous), Some(previous_revision), Some(previous_digest)) => {
            nonnegative_model_invocation_revision(previous_revision).ok()
                == Some(previous.revision())
                && decode_digest(&previous_digest, "model invocation predecessor digest").ok()
                    == Some(previous.digest())
        }
        _ => false,
    };
    let journal_sequence = positive_sequence(row.journal_sequence)?;
    let journal_event_id = EventId::from_uuid(row.journal_event_id)
        .map_err(|_| StoreError::corrupt("model invocation journal event identity"))?;
    let journal_recorded_at = from_database_time(row.journal_recorded_at)?;
    let journal_digest = decode_digest(&row.journal_digest, "model invocation journal digest")?;
    let attempt_id = row
        .attempt_id
        .map(AttemptId::from_uuid)
        .transpose()
        .map_err(|_| StoreError::corrupt("model invocation attempt identity"))?;
    let started_attempt_id = row
        .started_attempt_id
        .map(AttemptId::from_uuid)
        .transpose()
        .map_err(|_| StoreError::corrupt("model invocation started attempt identity"))?;
    let transition_kind = invocation.transition().map(ModelInvocationTransition::kind);
    let expected_started = match invocation.transition() {
        Some(ModelInvocationTransition::StartAttempt { attempt_id }) => Some(*attempt_id),
        _ => None,
    };
    let transition_digest = row
        .transition_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "model invocation transition digest"))
        .transpose()?;

    if invocation.intent() != intent
        || invocation.intent().tenant_id() != &tenant_id
        || invocation.intent().run_id() != run_id
        || invocation.intent().invocation_id() != invocation_id
        || invocation.revision() != revision
        || !previous_matches
        || invocation.journal_head().sequence() != journal_sequence
        || invocation.journal_head().event_id() != journal_event_id
        || invocation.journal_head().recorded_at() != journal_recorded_at
        || invocation.journal_head().digest() != journal_digest
        || model_invocation_status_text(invocation.status()) != row.status
        || invocation.attempt_id() != attempt_id
        || transition_kind.map(model_invocation_transition_kind_text)
            != row.transition_kind.as_deref()
        || expected_started != started_attempt_id
        || invocation.transition_digest() != transition_digest
        || invocation.digest()
            != decode_digest(&row.record_digest, "model invocation record digest")?
        || from_database_time(row.created_at)? != invocation.journal_head().recorded_at()
    {
        return Err(StoreError::corrupt("model invocation revision projection"));
    }
    Ok(invocation)
}

fn nonnegative_model_invocation_revision(
    value: i64,
) -> Result<ModelInvocationRevision, StoreError> {
    let value =
        u64::try_from(value).map_err(|_| StoreError::corrupt("model invocation revision"))?;
    ModelInvocationRevision::new(value)
        .map_err(|_| StoreError::corrupt("model invocation revision"))
}

fn validate_model_invocation_current_projection(
    row: &ModelInvocationRow,
    current: &ModelInvocation,
) -> Result<(), StoreError> {
    let current_revision = nonnegative_model_invocation_revision(row.current_revision)?;
    let current_attempt = row
        .current_attempt_id
        .map(AttemptId::from_uuid)
        .transpose()
        .map_err(|_| StoreError::corrupt("model invocation current attempt"))?;
    let current_digest = decode_digest(
        &row.current_record_digest,
        "model invocation current record digest",
    )?;
    if current.revision() != current_revision
        || model_invocation_status_text(current.status()) != row.current_status
        || current.attempt_id() != current_attempt
        || current.digest() != current_digest
        || from_database_time(row.updated_at)? != current.journal_head().recorded_at()
    {
        return Err(StoreError::corrupt("model invocation current projection"));
    }
    Ok(())
}

async fn load_model_invocation_revision_row(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    revision: ModelInvocationRevision,
) -> Result<ModelInvocationRevisionRow, StoreError> {
    let revision = i64::try_from(revision.get())
        .map_err(|_| StoreError::corrupt("model invocation revision"))?;
    query_as::<_, ModelInvocationRevisionRow>(SELECT_MODEL_INVOCATION_REVISION)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(*invocation_id.as_uuid())
        .bind(revision)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("model invocation revision load", source))?
        .ok_or(StoreError::ModelInvocationNotFound)
}

async fn verify_model_invocation_base_checkpoint(
    transaction: &mut Transaction<'_, Postgres>,
    intent: &ModelInvocationIntent,
) -> Result<(), StoreError> {
    let head = intent.activation().base_checkpoint();
    let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
        .bind(intent.tenant_id().as_str())
        .bind(*intent.run_id().as_uuid())
        .bind(*head.checkpoint_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("model invocation base checkpoint", source))?
        .ok_or_else(|| StoreError::corrupt("model invocation base checkpoint"))?;
    let checkpoint = decode_checkpoint(row)?;
    if checkpoint.head() != *head || !model_invocation_activation_is_ready(&checkpoint, intent) {
        return Err(StoreError::corrupt("model invocation base checkpoint"));
    }
    verify_checkpoint_anchor(transaction, &checkpoint).await
}

fn model_invocation_activation_is_ready(
    checkpoint: &Checkpoint,
    intent: &ModelInvocationIntent,
) -> bool {
    activation_is_canonical_ready_root(checkpoint, intent.activation())
}

async fn ensure_no_unsettled_model_invocations(
    transaction: &mut Transaction<'_, Postgres>,
    checkpoint: &Checkpoint,
) -> Result<(), StoreError> {
    let superstep = i64::try_from(checkpoint.superstep().get())
        .map_err(|_| StoreError::corrupt("checkpoint superstep"))?;
    let exists = query_scalar::<_, bool>(SELECT_UNSETTLED_MODEL_INVOCATION_EXISTS)
        .bind(checkpoint.tenant_id().as_str())
        .bind(*checkpoint.run_id().as_uuid())
        .bind(*checkpoint.checkpoint_id().as_uuid())
        .bind(superstep)
        .bind(checkpoint.digest().as_bytes())
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("model invocation barrier check", source))?;
    if exists {
        return Err(StoreError::CheckpointBlockedByModelInvocation);
    }
    Ok(())
}

async fn verify_model_invocation_anchor(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ModelInvocation,
) -> Result<(), StoreError> {
    let sequence = i64::try_from(invocation.journal_head().sequence().get())
        .map_err(|_| StoreError::corrupt("model invocation journal sequence"))?;
    let row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(invocation.intent().tenant_id().as_str())
        .bind(*invocation.intent().run_id().as_uuid())
        .bind(sequence)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("model invocation anchor", source))?
        .ok_or_else(|| StoreError::corrupt("model invocation journal anchor"))?;
    let projection_digest = row
        .projection_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "model invocation projection digest"))
        .transpose()?;
    let event = decode_event(row)?;
    if event.head() != *invocation.journal_head() || projection_digest != Some(invocation.digest())
    {
        return Err(StoreError::corrupt("model invocation journal anchor"));
    }
    Ok(())
}

fn encode_graph_definition(graph: &CompiledGraph) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(graph)
        .map_err(|_| StoreError::encoding("compiled graph definition"))?;
    if bytes.is_empty() || bytes.len() > MAX_COMPILED_GRAPH_BYTES {
        return Err(StoreError::encoding("compiled graph definition size"));
    }
    Ok(bytes)
}

fn decode_graph_definition(row: GraphDefinitionRow) -> Result<StoredGraphDefinition, StoreError> {
    if row.definition_bytes.is_empty() || row.definition_bytes.len() > MAX_COMPILED_GRAPH_BYTES {
        return Err(StoreError::corrupt("compiled graph byte length"));
    }
    let graph = serde_json::from_slice::<CompiledGraph>(&row.definition_bytes)
        .map_err(|_| StoreError::corrupt("compiled graph definition"))?;
    let canonical = serde_json_canonicalizer::to_vec(&graph)
        .map_err(|_| StoreError::corrupt("compiled graph canonicalization"))?;
    if canonical != row.definition_bytes {
        return Err(StoreError::corrupt("compiled graph canonical bytes"));
    }
    let tenant_id = TenantId::try_from(row.tenant_id)
        .map_err(|_| StoreError::corrupt("compiled graph tenant"))?;
    let identity = graph.identity();
    let owner = identity.owner();
    if owner.issuer().as_str() != row.owner_issuer
        || owner.subject().as_str() != row.owner_subject
        || identity.name().as_str() != row.graph_name
        || identity.version().to_string() != row.graph_version
        || graph.definition_digest()
            != decode_digest(&row.definition_digest, "compiled graph definition digest")?
    {
        return Err(StoreError::corrupt("compiled graph projection"));
    }
    Ok(StoredGraphDefinition {
        tenant_id,
        graph,
        registered_at: from_database_time(row.registered_at)?,
    })
}

async fn load_graph_definition_row(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    reference: &GraphReference,
) -> Result<Option<GraphDefinitionRow>, StoreError> {
    let identity = reference.identity();
    let owner = identity.owner();
    query_as::<_, GraphDefinitionRow>(SELECT_GRAPH_DEFINITION)
        .bind(tenant_id.as_str())
        .bind(owner.issuer().as_str())
        .bind(owner.subject().as_str())
        .bind(identity.name().as_str())
        .bind(identity.version().to_string())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("graph definition load", source))
}

fn encode_agent_admission(admission: &AgentAdmission) -> Result<Vec<u8>, StoreError> {
    let bytes = admission
        .canonical_bytes()
        .map_err(|_| StoreError::encoding("agent admission"))?;
    if bytes.is_empty() || bytes.len() > MAX_AGENT_ADMISSION_BYTES {
        return Err(StoreError::encoding("agent admission size"));
    }
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn decode_agent_admission(row: &AgentAdmissionRow) -> Result<AgentAdmission, StoreError> {
    if row.admission_bytes.is_empty() || row.admission_bytes.len() > MAX_AGENT_ADMISSION_BYTES {
        return Err(StoreError::corrupt("agent admission byte length"));
    }
    let admission = serde_json::from_slice::<AgentAdmission>(&row.admission_bytes)
        .map_err(|_| StoreError::corrupt("agent admission bytes"))?;
    let canonical = admission
        .canonical_bytes()
        .map_err(|_| StoreError::corrupt("agent admission canonicalization"))?;
    if canonical != row.admission_bytes {
        return Err(StoreError::corrupt("agent admission canonical bytes"));
    }

    let intent = admission.intent();
    let provenance = intent.provenance();
    let agent = intent.descriptor().metadata().identity();
    let agent_owner = agent.owner();
    let graph = intent.graph();
    let graph_identity = graph.identity();
    let graph_owner = graph_identity.owner();
    let policy = intent.authority().policy();
    let policy_owner = policy.owner();
    let admitted_at = from_database_time(row.admitted_at)?;

    if row.tenant_id != provenance.tenant_id().as_str()
        || row.run_id != *provenance.run_id().as_uuid()
        || row.agent_owner_issuer != agent_owner.issuer().as_str()
        || row.agent_owner_subject != agent_owner.subject().as_str()
        || row.agent_name != agent.name().as_str()
        || row.agent_version != agent.version().to_string()
        || row.graph_owner_issuer != graph_owner.issuer().as_str()
        || row.graph_owner_subject != graph_owner.subject().as_str()
        || row.graph_name != graph_identity.name().as_str()
        || row.graph_version != graph_identity.version().to_string()
        || decode_digest(
            &row.graph_definition_digest,
            "agent admission graph definition digest",
        )? != graph.definition_digest()
        || row.policy_owner_issuer != policy_owner.issuer().as_str()
        || row.policy_owner_subject != policy_owner.subject().as_str()
        || row.policy_name != policy.name().as_str()
        || row.policy_version != policy.version().to_string()
        || decode_digest(&row.policy_digest, "agent admission policy digest")?
            != intent.authority().policy_digest()
        || decode_digest(&row.intent_digest, "agent admission intent digest")?
            != intent.intent_digest()
        || decode_digest(&row.admission_digest, "agent admission digest")? != admission.digest()
        || admitted_at != admission.admitted_at()
        || row.journal_sequence != 1
        || row.checkpoint_superstep != 0
        || from_database_time(row.journal_recorded_at)? != admission.admitted_at()
        || from_database_time(row.created_at)? != admission.admitted_at()
    {
        return Err(StoreError::corrupt("agent admission projection"));
    }
    decode_digest(&row.journal_digest, "agent admission journal digest")?;
    decode_digest(&row.checkpoint_digest, "agent admission checkpoint digest")?;
    Ok(admission)
}

async fn load_agent_admission_row(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
) -> Result<Option<AgentAdmissionRow>, StoreError> {
    query_as::<_, AgentAdmissionRow>(SELECT_AGENT_ADMISSION)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("agent admission load", source))
}

fn initial_active_agent_lifecycle(admission: &AgentAdmission) -> Result<RunLifecycle, StoreError> {
    RunLifecycle::admitted(
        admission.intent().provenance().clone(),
        admission.admitted_at(),
    )
    .apply(RunTransition::Start {
        started_at: admission.admitted_at(),
    })
    .map_err(|_| StoreError::corrupt("agent admission initial lifecycle"))
}

#[allow(clippy::too_many_lines)]
async fn verify_stored_agent_admission(
    transaction: &mut Transaction<'_, Postgres>,
    run: StoredRun,
    row: AgentAdmissionRow,
) -> Result<StoredAgentAdmission, StoreError> {
    let admission = decode_agent_admission(&row)?;
    let intent = admission.intent();
    let provenance = intent.provenance();
    if run.lifecycle().provenance() != provenance
        || run.lifecycle().admitted_at() != admission.admitted_at()
        || run.lifecycle().revision() <= RunRevision::ZERO
    {
        return Err(StoreError::corrupt("agent admission run projection"));
    }

    let graph_row = load_graph_definition_row(transaction, provenance.tenant_id(), intent.graph())
        .await?
        .ok_or_else(|| StoreError::corrupt("agent admission graph reference"))?;
    let graph = decode_graph_definition(graph_row)?;
    if graph.tenant_id() != provenance.tenant_id() || graph.graph().reference() != *intent.graph() {
        return Err(StoreError::corrupt("agent admission graph reference"));
    }

    let event_row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(provenance.tenant_id().as_str())
        .bind(*provenance.run_id().as_uuid())
        .bind(row.journal_sequence)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("agent admission event load", source))?
        .ok_or_else(|| StoreError::corrupt("agent admission event anchor"))?;
    let committed_projection = event_row
        .projection_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "agent admission event projection digest"))
        .transpose()?;
    let event = decode_event(event_row)?;

    let checkpoint_row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
        .bind(provenance.tenant_id().as_str())
        .bind(*provenance.run_id().as_uuid())
        .bind(row.checkpoint_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("agent admission checkpoint load", source))?
        .ok_or_else(|| StoreError::corrupt("agent admission checkpoint anchor"))?;
    let checkpoint = decode_checkpoint(checkpoint_row)?;
    let initial_lifecycle = initial_active_agent_lifecycle(&admission)?;
    let expected_projection = agent_admission_projection_digest(
        &admission,
        event.intent_digest(),
        &checkpoint.write_intent(),
        &initial_lifecycle,
    )?;

    if event.tenant_id() != provenance.tenant_id()
        || event.run_id() != provenance.run_id()
        || event.sequence() != JournalSequence::FIRST
        || event.event_id().as_uuid() != &row.journal_event_id
        || event.recorded_at() != admission.admitted_at()
        || event.digest() != decode_digest(&row.journal_digest, "agent admission journal digest")?
        || event.previous_digest().is_some()
        || event.source().worker_fence().is_some()
        || event.payload().kind().as_str() != AgentAdmission::JOURNAL_EVENT_KIND
        || committed_projection != Some(expected_projection)
        || checkpoint.tenant_id() != provenance.tenant_id()
        || checkpoint.run_id() != provenance.run_id()
        || checkpoint.checkpoint_id().as_uuid() != &row.checkpoint_id
        || checkpoint.superstep() != Superstep::INITIAL
        || checkpoint.parent().is_some()
        || checkpoint.graph() != intent.graph()
        || checkpoint.journal_head() != &event.head()
        || checkpoint.digest()
            != decode_digest(&row.checkpoint_digest, "agent admission checkpoint digest")?
    {
        return Err(StoreError::corrupt("agent admission atomic anchors"));
    }

    let current_journal = run
        .journal_head()
        .ok_or_else(|| StoreError::corrupt("agent admission current journal"))?;
    let current_checkpoint = run
        .checkpoint()
        .ok_or_else(|| StoreError::corrupt("agent admission current checkpoint"))?;
    if current_journal.sequence() < event.sequence()
        || current_journal.recorded_at() < event.recorded_at()
        || current_checkpoint.superstep() < checkpoint.superstep()
    {
        return Err(StoreError::corrupt("agent admission current heads"));
    }

    Ok(StoredAgentAdmission {
        admission,
        run,
        event,
        checkpoint,
    })
}

fn validate_agent_admission_commit_input(
    intent: &AgentAdmissionIntent,
    append: &JournalAppend,
    checkpoint: &CheckpointWrite,
) -> Result<(), StoreError> {
    let provenance = intent.provenance();
    if append.worker_fence().is_some()
        || append.expectation().head().is_some()
        || append.intent().tenant_id() != provenance.tenant_id()
        || append.intent().run_id() != provenance.run_id()
        || append.intent().payload().kind().as_str() != AgentAdmission::JOURNAL_EVENT_KIND
        || checkpoint.tenant_id() != provenance.tenant_id()
        || checkpoint.run_id() != provenance.run_id()
        || checkpoint.superstep() != Superstep::INITIAL
        || checkpoint.parent().is_some()
        || checkpoint.graph() != intent.graph()
    {
        return Err(StoreError::InvalidAgentAdmissionCommit);
    }
    Ok(())
}

fn validate_agent_initial_checkpoint<V>(
    graph: &CompiledGraph,
    checkpoint: &CheckpointWrite,
    schemas: &V,
) -> Result<(), StoreError>
where
    V: GraphSchemaValidator + ?Sized,
{
    if checkpoint.graph() != &graph.reference()
        || checkpoint.superstep() != Superstep::INITIAL
        || checkpoint.parent().is_some()
        || checkpoint.ready_nodes() != graph.entry_nodes()
    {
        return Err(StoreError::InvalidAgentAdmissionCommit);
    }
    match catch_unwind(AssertUnwindSafe(|| {
        schemas.validate(graph.state_schema(), checkpoint.state().data())
    })) {
        Err(_) | Ok(Err(GraphSchemaValidationError::Unavailable)) => {
            Err(StoreError::AgentAdmissionSchemaUnavailable)
        }
        Ok(Err(GraphSchemaValidationError::Rejected)) => {
            Err(StoreError::AgentAdmissionStateRejected)
        }
        Ok(Err(_)) => Err(StoreError::AgentAdmissionStateRejected),
        Ok(Ok(())) => Ok(()),
    }
}

async fn lock_agent_admission_key(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
) -> Result<(), StoreError> {
    query(
        r"
SELECT pg_advisory_xact_lock(
    hashtextextended('stateknot:agent-admission:' || $1 || ':' || $2::text, 0)
)
",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("agent admission serialization lock", source))?;
    Ok(())
}

async fn load_locked_agent_admission(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
) -> Result<Option<StoredAgentAdmission>, StoreError> {
    lock_agent_admission_key(transaction, tenant_id, run_id).await?;
    let run_row = query_as::<_, RunRow>(SELECT_RUN_FOR_UPDATE)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("agent admission run lookup", source))?;
    let Some(run_row) = run_row else {
        return Ok(None);
    };
    let run = decode_run(run_row)?;
    verify_current_wait_set(transaction, &run).await?;
    let admission_row = load_agent_admission_row(transaction, tenant_id, run_id)
        .await?
        .ok_or(StoreError::AgentAdmissionConflict)?;
    verify_stored_agent_admission(transaction, run, admission_row)
        .await
        .map(Some)
}

fn agent_submission_digest(
    intent: &AgentAdmissionIntent,
    initial_state: &CheckpointState,
    initial_ready_nodes: &ReadyNodes,
) -> Result<Digest, StoreError> {
    #[derive(Serialize)]
    struct Wire<'a> {
        tenant_id: &'a TenantId,
        descriptor: &'a AgentDescriptor,
        request: &'a AgentRequest,
        budget_layers: &'a [AgentAdmissionBudgetLayer],
        budget: &'a ResolvedBudget,
        graph: &'a GraphReference,
        authority: &'a AgentAdmissionAuthority,
        initial_state: &'a CheckpointState,
        initial_ready_nodes: &'a ReadyNodes,
    }

    let canonical = serde_json_canonicalizer::to_vec(&Wire {
        tenant_id: intent.provenance().tenant_id(),
        descriptor: intent.descriptor(),
        request: intent.request(),
        budget_layers: intent.budget_layers(),
        budget: intent.budget(),
        graph: intent.graph(),
        authority: intent.authority(),
        initial_state,
        initial_ready_nodes,
    })
    .map_err(|_| StoreError::encoding("agent submission"))?;
    let mut preimage = Vec::with_capacity(AGENT_SUBMISSION_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(AGENT_SUBMISSION_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

async fn lock_agent_submission_key(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    key_digest: Digest,
) -> Result<(), StoreError> {
    query(
        r"
SELECT pg_advisory_xact_lock(
    hashtextextended(
        'stateknot:agent-submission:' || $1 || ':' || encode($2::bytea, 'hex'),
        0
    )
)
",
    )
    .bind(tenant_id.as_str())
    .bind(key_digest.as_bytes())
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("agent submission serialization lock", source))?;
    Ok(())
}

async fn load_agent_submission_row(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    key_digest: Digest,
) -> Result<Option<AgentSubmissionRow>, StoreError> {
    query_as::<_, AgentSubmissionRow>(SELECT_AGENT_SUBMISSION)
        .bind(tenant_id.as_str())
        .bind(key_digest.as_bytes())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("agent submission load", source))
}

fn verify_agent_submission(
    row: &AgentSubmissionRow,
    key_digest: Digest,
    admission: StoredAgentAdmission,
) -> Result<StoredAgentSubmission, StoreError> {
    let durable_key_digest = decode_digest(&row.key_digest, "agent submission key digest")?;
    let durable_submission_digest =
        decode_digest(&row.submission_digest, "agent submission digest")?;
    let durable_admission_digest =
        decode_digest(&row.admission_digest, "agent submission admission digest")?;
    let created_at = from_database_time(row.created_at)?;
    let intent = admission.admission().intent();
    let expected_submission_digest = agent_submission_digest(
        intent,
        admission.checkpoint().state(),
        admission.checkpoint().ready_nodes(),
    )?;

    if row.tenant_id != intent.provenance().tenant_id().as_str()
        || row.run_id != *intent.provenance().run_id().as_uuid()
        || durable_key_digest != key_digest
        || durable_submission_digest != expected_submission_digest
        || durable_admission_digest != admission.admission().digest()
        || created_at != admission.admission().admitted_at()
    {
        return Err(StoreError::corrupt("agent submission projection"));
    }

    Ok(StoredAgentSubmission {
        key_digest,
        submission_digest: durable_submission_digest,
        admission,
        created_at,
    })
}

async fn load_locked_agent_submission(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    key_digest: Digest,
) -> Result<Option<StoredAgentSubmission>, StoreError> {
    lock_agent_submission_key(transaction, tenant_id, key_digest).await?;
    let row = query_as::<_, AgentSubmissionRow>(SELECT_AGENT_SUBMISSION_FOR_UPDATE)
        .bind(tenant_id.as_str())
        .bind(key_digest.as_bytes())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("agent submission locked load", source))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let run_id = RunId::from_uuid(row.run_id)
        .map_err(|_| StoreError::corrupt("agent submission run identity"))?;
    let admission = load_locked_agent_admission(transaction, tenant_id, run_id)
        .await?
        .ok_or_else(|| StoreError::corrupt("agent submission admission reference"))?;
    verify_agent_submission(&row, key_digest, admission).map(Some)
}

async fn insert_agent_submission(
    transaction: &mut Transaction<'_, Postgres>,
    key_digest: Digest,
    submission_digest: Digest,
    admission: &StoredAgentAdmission,
) -> Result<(), StoreError> {
    let snapshot = admission.admission();
    let provenance = snapshot.intent().provenance();
    let inserted = query(
        r"
INSERT INTO stateknot.agent_submission_keys (
    tenant_id,
    key_digest,
    submission_digest,
    run_id,
    admission_digest,
    created_at
)
VALUES ($1, $2, $3, $4, $5, $6)
",
    )
    .bind(provenance.tenant_id().as_str())
    .bind(key_digest.as_bytes())
    .bind(submission_digest.as_bytes())
    .bind(*provenance.run_id().as_uuid())
    .bind(snapshot.digest().as_bytes())
    .bind(to_database_time(snapshot.admitted_at())?)
    .execute(&mut **transaction)
    .await;
    match inserted {
        Ok(result) if result.rows_affected() == 1 => Ok(()),
        Ok(_) => Err(StoreError::corrupt("agent submission insert row count")),
        Err(source)
            if has_database_constraint(&source, "agent_submission_keys_pkey")
                || has_database_constraint(&source, "agent_submission_keys_run_unique") =>
        {
            Err(StoreError::AgentSubmissionConflict)
        }
        Err(source) => Err(StoreError::database("agent submission insert", source)),
    }
}

fn validate_agent_admission_retry(
    stored: &StoredAgentAdmission,
    intent: &AgentAdmissionIntent,
    append: &JournalAppend,
    checkpoint: &CheckpointWrite,
) -> Result<(), StoreError> {
    if stored.admission().intent() != intent
        || !stored.event().matches_intent(append.intent())
        || !stored.checkpoint().matches_write(checkpoint)
    {
        return Err(StoreError::AgentAdmissionConflict);
    }
    Ok(())
}

async fn insert_agent_pending_run(
    transaction: &mut Transaction<'_, Postgres>,
    lifecycle: &RunLifecycle,
) -> Result<bool, StoreError> {
    let provenance = lifecycle.provenance();
    let lifecycle_bytes = encode_lifecycle(lifecycle)?;
    let observed_at = to_database_time(lifecycle.admitted_at())?;
    let inserted = query(
        r"
INSERT INTO stateknot.runs (
    tenant_id,
    run_id,
    thread_id,
    invocation_id,
    lifecycle_bytes,
    lifecycle_revision,
    lifecycle_status,
    admitted_at,
    changed_at,
    scheduler_ready_at
)
VALUES ($1, $2, $3, $4, $5, $6::numeric, $7, $8, $8, $8)
ON CONFLICT (tenant_id, run_id) DO NOTHING
",
    )
    .bind(provenance.tenant_id().as_str())
    .bind(*provenance.run_id().as_uuid())
    .bind(*provenance.thread_id().as_uuid())
    .bind(*provenance.invocation_id().as_uuid())
    .bind(lifecycle_bytes)
    .bind(lifecycle.revision().to_string())
    .bind(run_status_text(lifecycle.status()))
    .bind(observed_at)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("agent admission run insert", source))?
    .rows_affected();
    Ok(inserted == 1)
}

#[allow(clippy::too_many_lines)]
async fn insert_agent_admission(
    transaction: &mut Transaction<'_, Postgres>,
    admission: &AgentAdmission,
    event: &JournalEvent,
    checkpoint: &Checkpoint,
) -> Result<(), StoreError> {
    let intent = admission.intent();
    let provenance = intent.provenance();
    let agent = intent.descriptor().metadata().identity();
    let agent_owner = agent.owner();
    let graph = intent.graph();
    let graph_identity = graph.identity();
    let graph_owner = graph_identity.owner();
    let policy = intent.authority().policy();
    let policy_owner = policy.owner();
    let admission_bytes = encode_agent_admission(admission)?;
    let journal_sequence =
        i64::try_from(event.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?;
    let checkpoint_superstep = i64::try_from(checkpoint.superstep().get())
        .map_err(|_| StoreError::encoding("agent admission checkpoint superstep"))?;
    let admitted_at = to_database_time(admission.admitted_at())?;

    let inserted = query(
        r"
INSERT INTO stateknot.agent_admissions (
    tenant_id,
    run_id,
    agent_owner_issuer,
    agent_owner_subject,
    agent_name,
    agent_version,
    graph_owner_issuer,
    graph_owner_subject,
    graph_name,
    graph_version,
    graph_definition_digest,
    policy_owner_issuer,
    policy_owner_subject,
    policy_name,
    policy_version,
    policy_digest,
    intent_digest,
    admission_digest,
    admitted_at,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    checkpoint_id,
    checkpoint_superstep,
    checkpoint_digest,
    admission_bytes,
    created_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
    $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28
)
",
    )
    .bind(provenance.tenant_id().as_str())
    .bind(*provenance.run_id().as_uuid())
    .bind(agent_owner.issuer().as_str())
    .bind(agent_owner.subject().as_str())
    .bind(agent.name().as_str())
    .bind(agent.version().to_string())
    .bind(graph_owner.issuer().as_str())
    .bind(graph_owner.subject().as_str())
    .bind(graph_identity.name().as_str())
    .bind(graph_identity.version().to_string())
    .bind(graph.definition_digest().as_bytes())
    .bind(policy_owner.issuer().as_str())
    .bind(policy_owner.subject().as_str())
    .bind(policy.name().as_str())
    .bind(policy.version().to_string())
    .bind(intent.authority().policy_digest().as_bytes())
    .bind(intent.intent_digest().as_bytes())
    .bind(admission.digest().as_bytes())
    .bind(admitted_at)
    .bind(journal_sequence)
    .bind(*event.event_id().as_uuid())
    .bind(to_database_time(event.recorded_at())?)
    .bind(event.digest().as_bytes())
    .bind(*checkpoint.checkpoint_id().as_uuid())
    .bind(checkpoint_superstep)
    .bind(checkpoint.digest().as_bytes())
    .bind(admission_bytes)
    .bind(admitted_at)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("agent admission insert", source))?
    .rows_affected();
    if inserted != 1 {
        return Err(StoreError::corrupt("agent admission insert row count"));
    }
    Ok(())
}

enum NewAgentAdmissionOutcome {
    Committed(StoredAgentAdmission),
    Idempotent(StoredAgentAdmission),
}

async fn commit_new_agent_admission(
    transaction: &mut Transaction<'_, Postgres>,
    intent: AgentAdmissionIntent,
    append: JournalAppend,
    checkpoint_write: CheckpointWrite,
) -> Result<NewAgentAdmissionOutcome, StoreError> {
    let tenant_id = intent.provenance().tenant_id().clone();
    let run_id = intent.provenance().run_id();
    let admitted_at = database_now(transaction, "agent admission clock").await?;
    let admission = AgentAdmission::commit(intent, admitted_at)
        .map_err(|_| StoreError::AgentAdmissionRejected)?;
    let pending = RunLifecycle::admitted(admission.intent().provenance().clone(), admitted_at);
    let active = pending
        .clone()
        .apply(RunTransition::Start {
            started_at: admitted_at,
        })
        .map_err(|_| StoreError::InvalidAgentAdmissionCommit)?;

    if !insert_agent_pending_run(transaction, &pending).await? {
        let stored = load_locked_agent_admission(transaction, &tenant_id, run_id)
            .await?
            .ok_or(StoreError::AgentAdmissionConflict)?;
        validate_agent_admission_retry(&stored, admission.intent(), &append, &checkpoint_write)?;
        return Ok(NewAgentAdmissionOutcome::Idempotent(stored));
    }

    let event = JournalEvent::commit(append, admitted_at)
        .map_err(|error| map_event_commit_error(&error))?;
    let checkpoint = Checkpoint::commit(checkpoint_write, event.head())
        .map_err(|_| StoreError::InvalidAgentAdmissionCommit)?;
    let projection_digest = agent_admission_projection_digest(
        &admission,
        event.intent_digest(),
        &checkpoint.write_intent(),
        &active,
    )?;
    let prepared = prepared_projection(&active)?;

    insert_event(transaction, &event, projection_digest).await?;
    insert_checkpoint(transaction, &checkpoint, event.source()).await?;
    update_checkpoint_pointer(transaction, &checkpoint, event.source()).await?;
    update_run_head(transaction, &event, Some(&prepared)).await?;
    insert_agent_admission(transaction, &admission, &event, &checkpoint).await?;

    let run_row = fetch_locked_run_row(transaction, &tenant_id, run_id).await?;
    let run = decode_run(run_row)?;
    verify_current_wait_set(transaction, &run).await?;
    let admission_row = load_agent_admission_row(transaction, &tenant_id, run_id)
        .await?
        .ok_or_else(|| StoreError::corrupt("agent admission committed row"))?;
    let stored = verify_stored_agent_admission(transaction, run, admission_row).await?;
    Ok(NewAgentAdmissionOutcome::Committed(stored))
}

fn decode_scheduler_fairness_policy(
    row: SchedulerFairnessShardRow,
) -> Result<StoredSchedulerFairnessPolicy, StoreError> {
    let shard_id = SchedulerShardId::try_from(row.shard_id)
        .map_err(|_| StoreError::corrupt("scheduler fairness shard identity"))?;
    let cycle_length = u16::try_from(row.cycle_length)
        .map_err(|_| StoreError::corrupt("scheduler fairness cycle length"))?;
    let registration =
        SchedulerFairnessPolicyRegistration::new(shard_id, row.policy_bytes, cycle_length)
            .map_err(|_| StoreError::corrupt("scheduler fairness policy shape"))?;
    let digest = decode_digest(&row.policy_digest, "scheduler fairness policy digest")?;
    if registration.policy_digest() != digest {
        return Err(StoreError::corrupt("scheduler fairness policy checksum"));
    }
    let next_slot = u16::try_from(row.next_slot)
        .map_err(|_| StoreError::corrupt("scheduler fairness cursor"))?;
    if next_slot >= cycle_length || row.next_sequence < 0 {
        return Err(StoreError::corrupt("scheduler fairness cursor projection"));
    }
    let registered_at = from_database_time(row.registered_at)?;
    let updated_at = from_database_time(row.updated_at)?;
    if updated_at < registered_at {
        return Err(StoreError::corrupt("scheduler fairness clock projection"));
    }
    Ok(StoredSchedulerFairnessPolicy {
        registration,
        registered_at,
    })
}

async fn load_scheduler_fairness_reservation_row(
    transaction: &mut Transaction<'_, Postgres>,
    reservation_id: SchedulerReservationId,
) -> Result<Option<SchedulerFairnessReservationRow>, StoreError> {
    query_as::<_, SchedulerFairnessReservationRow>(SELECT_SCHEDULER_FAIRNESS_RESERVATION)
        .bind(*reservation_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("scheduler fairness reservation lookup", source))
}

async fn load_valid_scheduler_fairness_reservation(
    transaction: &mut Transaction<'_, Postgres>,
    shard_id: &SchedulerShardId,
    policy_digest: Digest,
    reservation_id: SchedulerReservationId,
) -> Result<Option<SchedulerFairnessReservation>, StoreError> {
    let Some(row) = load_scheduler_fairness_reservation_row(transaction, reservation_id).await?
    else {
        return Ok(None);
    };
    let reservation = decode_scheduler_fairness_reservation(row)?;
    validate_scheduler_fairness_reservation(&reservation, shard_id, policy_digest, reservation_id)?;
    Ok(Some(reservation))
}

async fn insert_scheduler_fairness_reservation(
    transaction: &mut Transaction<'_, Postgres>,
    shard_id: &SchedulerShardId,
    policy_digest: Digest,
    reservation_id: SchedulerReservationId,
    shard_row: SchedulerFairnessShardRow,
) -> Result<SchedulerFairnessReservation, StoreError> {
    let stored = decode_scheduler_fairness_policy(shard_row.clone())?;
    if stored.registration().policy_digest() != policy_digest {
        return Err(StoreError::SchedulerFairnessPolicyConflict);
    }
    let sequence = u64::try_from(shard_row.next_sequence)
        .map_err(|_| StoreError::corrupt("scheduler fairness sequence"))?;
    if shard_row.next_sequence == i64::MAX {
        return Err(StoreError::SchedulerFairnessSequenceExhausted);
    }
    let slot = u16::try_from(shard_row.next_slot)
        .map_err(|_| StoreError::corrupt("scheduler fairness cursor"))?;
    let cycle_length = stored.registration().cycle_length();
    if slot >= cycle_length {
        return Err(StoreError::corrupt("scheduler fairness cursor"));
    }
    let reserved_at = database_now(transaction, "scheduler fairness reservation clock").await?;
    let reservation = SchedulerFairnessReservation {
        shard_id: shard_id.clone(),
        reservation_id,
        policy_digest,
        sequence,
        slot,
        reserved_at,
    };

    let insert = query(
        r"
INSERT INTO stateknot.scheduler_fairness_reservations (
    shard_id,
    reservation_id,
    policy_digest,
    sequence,
    slot,
    reserved_at
)
VALUES ($1, $2, $3, $4, $5, $6)
",
    )
    .bind(shard_id.as_str())
    .bind(*reservation_id.as_uuid())
    .bind(policy_digest.as_bytes())
    .bind(shard_row.next_sequence)
    .bind(shard_row.next_slot)
    .bind(to_database_time(reserved_at)?)
    .execute(&mut **transaction)
    .await;
    if let Err(source) = insert {
        if has_database_error_code(&source, "23505") {
            return Err(StoreError::SchedulerFairnessReservationConflict);
        }
        return Err(StoreError::database(
            "scheduler fairness reservation insert",
            source,
        ));
    }

    let next_slot = if slot + 1 == cycle_length {
        0
    } else {
        slot + 1
    };
    let updated = query(
        r"
UPDATE stateknot.scheduler_fairness_shards
SET next_slot = $1,
    next_sequence = next_sequence + 1,
    updated_at = $2
WHERE shard_id = $3
  AND policy_digest = $4
  AND next_slot = $5
  AND next_sequence = $6
",
    )
    .bind(i32::from(next_slot))
    .bind(to_database_time(reserved_at)?)
    .bind(shard_id.as_str())
    .bind(policy_digest.as_bytes())
    .bind(shard_row.next_slot)
    .bind(shard_row.next_sequence)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("scheduler fairness cursor advance", source))?
    .rows_affected();
    if updated != 1 {
        return Err(StoreError::SchedulerFairnessPolicyConflict);
    }
    Ok(reservation)
}

fn decode_scheduler_fairness_reservation(
    row: SchedulerFairnessReservationRow,
) -> Result<SchedulerFairnessReservation, StoreError> {
    let shard_id = SchedulerShardId::try_from(row.shard_id)
        .map_err(|_| StoreError::corrupt("scheduler fairness reservation shard"))?;
    let reservation_id = SchedulerReservationId::from_uuid(row.reservation_id)
        .map_err(|_| StoreError::corrupt("scheduler fairness reservation identity"))?;
    let policy_digest = decode_digest(
        &row.policy_digest,
        "scheduler fairness reservation policy digest",
    )?;
    let shard_policy_digest = decode_digest(
        &row.shard_policy_digest,
        "scheduler fairness shard policy digest",
    )?;
    let sequence = u64::try_from(row.sequence)
        .map_err(|_| StoreError::corrupt("scheduler fairness reservation sequence"))?;
    let slot = u16::try_from(row.slot)
        .map_err(|_| StoreError::corrupt("scheduler fairness reservation slot"))?;
    let cycle_length = u16::try_from(row.cycle_length)
        .map_err(|_| StoreError::corrupt("scheduler fairness reservation cycle"))?;
    if cycle_length == 0
        || cycle_length > SchedulerFairnessPolicyRegistration::MAX_CYCLE_LENGTH
        || slot >= cycle_length
        || policy_digest != shard_policy_digest
    {
        return Err(StoreError::corrupt(
            "scheduler fairness reservation policy projection",
        ));
    }
    Ok(SchedulerFairnessReservation {
        shard_id,
        reservation_id,
        policy_digest,
        sequence,
        slot,
        reserved_at: from_database_time(row.reserved_at)?,
    })
}

fn validate_scheduler_fairness_reservation(
    reservation: &SchedulerFairnessReservation,
    shard_id: &SchedulerShardId,
    policy_digest: Digest,
    reservation_id: SchedulerReservationId,
) -> Result<(), StoreError> {
    if reservation.shard_id() != shard_id
        || reservation.policy_digest() != policy_digest
        || reservation.reservation_id() != reservation_id
    {
        return Err(StoreError::SchedulerFairnessReservationConflict);
    }
    Ok(())
}

fn encode_outbox_destination_config(config: &JournalPayload) -> Result<Vec<u8>, StoreError> {
    let canonical = config
        .canonical_json()
        .map_err(|_| StoreError::encoding("outbox destination config"))?;
    let bytes = canonical.as_bytes().to_vec();
    if bytes.is_empty() || bytes.len() > MAX_OUTBOX_DESTINATION_BYTES {
        return Err(StoreError::encoding("outbox destination config size"));
    }
    Ok(bytes)
}

fn decode_outbox_destination(
    row: OutboxDestinationRow,
) -> Result<StoredOutboxDestination, StoreError> {
    if row.config_bytes.is_empty() || row.config_bytes.len() > MAX_OUTBOX_DESTINATION_BYTES {
        return Err(StoreError::corrupt("outbox destination byte length"));
    }
    let config = serde_json::from_slice::<JournalPayload>(&row.config_bytes)
        .map_err(|_| StoreError::corrupt("outbox destination config"))?;
    let canonical = config
        .canonical_json()
        .map_err(|_| StoreError::corrupt("outbox destination canonicalization"))?;
    if canonical.as_bytes() != row.config_bytes {
        return Err(StoreError::corrupt("outbox destination canonical bytes"));
    }
    let tenant_id = TenantId::try_from(row.tenant_id)
        .map_err(|_| StoreError::corrupt("outbox destination tenant"))?;
    let destination_id = DestinationId::from_uuid(row.destination_id)
        .map_err(|_| StoreError::corrupt("outbox destination identity"))?;
    let snapshot_digest = decode_digest(&row.snapshot_digest, "outbox destination digest")?;
    if config.digest() != snapshot_digest
        || config.kind().as_str() != row.config_kind
        || config.schema().id().as_str() != row.schema_id
        || config.schema().version().to_string() != row.schema_version
        || config.schema().digest()
            != decode_digest(&row.schema_digest, "outbox destination schema digest")?
    {
        return Err(StoreError::corrupt("outbox destination projection"));
    }
    Ok(StoredOutboxDestination {
        destination: OutboxDestinationRef::new(tenant_id, destination_id, snapshot_digest),
        config,
        created_at: from_database_time(row.created_at)?,
    })
}

async fn load_outbox_destination_row(
    transaction: &mut Transaction<'_, Postgres>,
    destination: &OutboxDestinationRef,
) -> Result<Option<OutboxDestinationRow>, StoreError> {
    query_as::<_, OutboxDestinationRow>(SELECT_OUTBOX_DESTINATION)
        .bind(destination.tenant_id().as_str())
        .bind(*destination.destination_id().as_uuid())
        .bind(destination.snapshot_digest().as_bytes())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("outbox destination load", source))
}

async fn load_and_decode_outbox_destination(
    transaction: &mut Transaction<'_, Postgres>,
    destination: &OutboxDestinationRef,
) -> Result<StoredOutboxDestination, StoreError> {
    let row = load_outbox_destination_row(transaction, destination)
        .await?
        .ok_or(StoreError::OutboxDestinationNotFound)?;
    let stored = decode_outbox_destination(row)?;
    if stored.destination() != destination {
        return Err(StoreError::corrupt("outbox destination binding"));
    }
    Ok(stored)
}

fn encode_outbox_delivery(delivery: &OutboxDelivery) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(delivery)
        .map_err(|_| StoreError::encoding("outbox delivery"))?;
    if bytes.is_empty() || bytes.len() > MAX_OUTBOX_DELIVERY_BYTES {
        return Err(StoreError::encoding("outbox delivery size"));
    }
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn decode_outbox_delivery(row: &OutboxDeliveryRow) -> Result<OutboxDelivery, StoreError> {
    if row.delivery_bytes.is_empty() || row.delivery_bytes.len() > MAX_OUTBOX_DELIVERY_BYTES {
        return Err(StoreError::corrupt("outbox delivery byte length"));
    }
    let delivery = serde_json::from_slice::<OutboxDelivery>(&row.delivery_bytes)
        .map_err(|_| StoreError::corrupt("outbox delivery value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&delivery)
        .map_err(|_| StoreError::corrupt("outbox delivery canonicalization"))?;
    if canonical != row.delivery_bytes {
        return Err(StoreError::corrupt("outbox delivery canonical bytes"));
    }
    let intent = delivery.intent();
    let origin = delivery.origin();
    let destination = intent.destination();
    let origin_sequence = i64::try_from(origin.sequence().get())
        .map_err(|_| StoreError::corrupt("outbox origin sequence"))?;
    if intent.tenant_id().as_str() != row.tenant_id
        || *intent.run_id().as_uuid() != row.run_id
        || *intent.delivery_id().as_uuid() != row.delivery_id
        || origin_sequence != row.origin_sequence
        || *origin.event_id().as_uuid() != row.origin_event_id
        || origin.recorded_at() != from_database_time(row.origin_recorded_at)?
        || origin.digest() != decode_digest(&row.origin_digest, "outbox origin digest")?
        || *destination.destination_id().as_uuid() != row.destination_id
        || destination.snapshot_digest()
            != decode_digest(
                &row.destination_snapshot_digest,
                "outbox destination snapshot digest",
            )?
        || intent.intent_digest() != decode_digest(&row.intent_digest, "outbox intent digest")?
        || intent.expires_at() != from_database_time(row.expires_at)?
        || delivery.digest() != decode_digest(&row.delivery_digest, "outbox delivery digest")?
        || origin.recorded_at() != from_database_time(row.created_at)?
        || from_database_time(row.updated_at)? < origin.recorded_at()
    {
        return Err(StoreError::corrupt("outbox delivery projection"));
    }
    Ok(delivery)
}

async fn load_outbox_delivery_row(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
    delivery_id: DeliveryId,
    for_update: bool,
) -> Result<Option<OutboxDeliveryRow>, StoreError> {
    let statement = if for_update {
        SELECT_OUTBOX_DELIVERY_FOR_UPDATE.as_str()
    } else {
        SELECT_OUTBOX_DELIVERY.as_str()
    };
    query_as::<_, OutboxDeliveryRow>(statement)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(*delivery_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("outbox delivery load", source))
}

fn encode_outbox_attempt_start(start: &OutboxAttemptStart) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(start)
        .map_err(|_| StoreError::encoding("outbox attempt start"))?;
    if bytes.is_empty() || bytes.len() > MAX_OUTBOX_ATTEMPT_START_BYTES {
        return Err(StoreError::encoding("outbox attempt start size"));
    }
    Ok(bytes)
}

fn decode_outbox_attempt_start(
    row: &OutboxAttemptStartRow,
) -> Result<OutboxAttemptStart, StoreError> {
    if row.start_bytes.is_empty() || row.start_bytes.len() > MAX_OUTBOX_ATTEMPT_START_BYTES {
        return Err(StoreError::corrupt("outbox attempt start byte length"));
    }
    let start = serde_json::from_slice::<OutboxAttemptStart>(&row.start_bytes)
        .map_err(|_| StoreError::corrupt("outbox attempt start value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&start)
        .map_err(|_| StoreError::corrupt("outbox attempt start canonicalization"))?;
    if canonical != row.start_bytes {
        return Err(StoreError::corrupt("outbox attempt start canonical bytes"));
    }
    let delivery = start.delivery();
    let fence = start.fence();
    let epoch = i64::try_from(fence.epoch().get())
        .map_err(|_| StoreError::corrupt("outbox attempt epoch"))?;
    if delivery.tenant_id().as_str() != row.tenant_id
        || *delivery.run_id().as_uuid() != row.run_id
        || *delivery.delivery_id().as_uuid() != row.delivery_id
        || delivery.expires_at() != from_database_time(row.delivery_expires_at)?
        || delivery.digest() != decode_digest(&row.delivery_digest, "outbox delivery digest")?
        || epoch != row.epoch
        || *fence.attempt_id().as_uuid() != row.attempt_id
        || start.started_at() != from_database_time(row.started_at)?
        || start.expires_at() != from_database_time(row.expires_at)?
        || start.digest() != decode_digest(&row.start_digest, "outbox attempt start digest")?
        || start.started_at() != from_database_time(row.created_at)?
    {
        return Err(StoreError::corrupt("outbox attempt start projection"));
    }
    Ok(start)
}

fn encode_outbox_attempt_completion(
    completion: &OutboxAttemptCompletion,
) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(completion)
        .map_err(|_| StoreError::encoding("outbox attempt completion"))?;
    if bytes.is_empty() || bytes.len() > MAX_OUTBOX_ATTEMPT_COMPLETION_BYTES {
        return Err(StoreError::encoding("outbox attempt completion size"));
    }
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn decode_outbox_attempt_completion(
    row: &OutboxAttemptCompletionRow,
) -> Result<OutboxAttemptCompletion, StoreError> {
    if row.completion_bytes.is_empty()
        || row.completion_bytes.len() > MAX_OUTBOX_ATTEMPT_COMPLETION_BYTES
    {
        return Err(StoreError::corrupt("outbox attempt completion byte length"));
    }
    let completion = serde_json::from_slice::<OutboxAttemptCompletion>(&row.completion_bytes)
        .map_err(|_| StoreError::corrupt("outbox attempt completion value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&completion)
        .map_err(|_| StoreError::corrupt("outbox attempt completion canonicalization"))?;
    if canonical != row.completion_bytes {
        return Err(StoreError::corrupt(
            "outbox attempt completion canonical bytes",
        ));
    }
    let start = completion.start();
    let fence = start.fence();
    let delivery = start.delivery();
    let epoch = i64::try_from(fence.epoch().get())
        .map_err(|_| StoreError::corrupt("outbox completion epoch"))?;
    let (outcome_kind, retry_kind, retry_delay) = outbox_completion_projection(&completion)?;
    if delivery.tenant_id().as_str() != row.tenant_id
        || *delivery.run_id().as_uuid() != row.run_id
        || *delivery.delivery_id().as_uuid() != row.delivery_id
        || epoch != row.epoch
        || *fence.attempt_id().as_uuid() != row.attempt_id
        || start.started_at() != from_database_time(row.started_at)?
        || start.expires_at() != from_database_time(row.attempt_expires_at)?
        || start.digest() != decode_digest(&row.start_digest, "outbox completion start digest")?
        || outcome_kind != row.outcome_kind
        || retry_kind != row.retry_advice_kind.as_deref()
        || retry_delay != row.retry_delay_millis
        || completion.completed_at() != from_database_time(row.completed_at)?
        || completion.digest() != decode_digest(&row.completion_digest, "outbox completion digest")?
        || completion.completed_at() != from_database_time(row.created_at)?
    {
        return Err(StoreError::corrupt("outbox attempt completion projection"));
    }
    Ok(completion)
}

fn outbox_completion_projection(
    completion: &OutboxAttemptCompletion,
) -> Result<(&'static str, Option<&'static str>, Option<i64>), StoreError> {
    match completion.outcome() {
        OutboxAttemptOutcome::Acknowledged { .. } => Ok(("acknowledged", None, None)),
        OutboxAttemptOutcome::Failed { failure } => match failure.retry_advice() {
            RetryAdvice::Never => Ok(("failed", Some("never"), None)),
            RetryAdvice::SafeAfter { delay } => {
                Ok(("failed", Some("safe_after"), Some(delay.as_i64())))
            }
            RetryAdvice::ReconcileFirst => Err(StoreError::InvalidOutboxTransition),
        },
        _ => Err(StoreError::InvalidOutboxTransition),
    }
}

async fn load_outbox_attempt_completion_row(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
    delivery_id: DeliveryId,
    epoch: i64,
) -> Result<Option<OutboxAttemptCompletionRow>, StoreError> {
    query_as::<_, OutboxAttemptCompletionRow>(SELECT_OUTBOX_ATTEMPT_COMPLETION)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(*delivery_id.as_uuid())
        .bind(epoch)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("outbox attempt completion load", source))
}

async fn load_and_verify_outbox_attempts(
    transaction: &mut Transaction<'_, Postgres>,
    delivery: &OutboxDelivery,
) -> Result<Vec<OutboxAttempt>, StoreError> {
    let rows = query_as::<_, OutboxAttemptStartRow>(SELECT_OUTBOX_ATTEMPT_HISTORY.as_str())
        .bind(delivery.intent().tenant_id().as_str())
        .bind(*delivery.intent().run_id().as_uuid())
        .bind(*delivery.intent().delivery_id().as_uuid())
        .bind(0_i64)
        .bind(i64::try_from(MAX_OUTBOX_ATTEMPTS + 1).unwrap_or(i64::MAX))
        .fetch_all(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("outbox attempt history load", source))?;
    if rows.len() > MAX_OUTBOX_ATTEMPTS {
        return Err(StoreError::corrupt("outbox attempt history bound"));
    }
    let completion_rows =
        query_as::<_, OutboxAttemptCompletionRow>(SELECT_OUTBOX_ATTEMPT_COMPLETION_HISTORY)
            .bind(delivery.intent().tenant_id().as_str())
            .bind(*delivery.intent().run_id().as_uuid())
            .bind(*delivery.intent().delivery_id().as_uuid())
            .bind(i64::try_from(MAX_OUTBOX_ATTEMPTS + 1).unwrap_or(i64::MAX))
            .fetch_all(&mut **transaction)
            .await
            .map_err(|source| StoreError::database("outbox completion history load", source))?;
    if completion_rows.len() > MAX_OUTBOX_ATTEMPTS {
        return Err(StoreError::corrupt("outbox completion history bound"));
    }
    let mut completions = BTreeMap::new();
    for row in completion_rows {
        let epoch = row.epoch;
        if completions.insert(epoch, row).is_some() {
            return Err(StoreError::corrupt("outbox completion history identity"));
        }
    }
    let mut verifier = OutboxAttemptHistoryVerifier::new(delivery);
    let mut attempts = Vec::with_capacity(rows.len());
    for row in rows {
        let start = decode_outbox_attempt_start(&row)?;
        if start.delivery() != &delivery.head() {
            return Err(StoreError::corrupt("outbox attempt delivery binding"));
        }
        let epoch = i64::try_from(start.fence().epoch().get())
            .map_err(|_| StoreError::corrupt("outbox attempt epoch"))?;
        let completion = completions
            .remove(&epoch)
            .map(|row| decode_outbox_attempt_completion(&row))
            .transpose()?;
        let attempt = OutboxAttempt::restore(start, completion)
            .map_err(|_| StoreError::corrupt("outbox attempt join"))?;
        verifier
            .verify_next(&attempt)
            .map_err(|_| StoreError::corrupt("outbox attempt history"))?;
        attempts.push(attempt);
    }
    if !completions.is_empty() {
        return Err(StoreError::corrupt("outbox completion without start"));
    }
    Ok(attempts)
}

async fn verify_outbox_projection(
    transaction: &mut Transaction<'_, Postgres>,
    row: &OutboxDeliveryRow,
    delivery: &OutboxDelivery,
) -> Result<(), StoreError> {
    verify_outbox_delivery_anchor(transaction, delivery).await?;
    let destination =
        load_and_decode_outbox_destination(transaction, delivery.intent().destination()).await?;
    if destination.destination() != delivery.intent().destination() {
        return Err(StoreError::corrupt("outbox delivery destination binding"));
    }
    let attempts = load_and_verify_outbox_attempts(transaction, delivery).await?;
    verify_outbox_projection_records(row, delivery, &attempts)
}

async fn verify_outbox_delivery_anchor(
    transaction: &mut Transaction<'_, Postgres>,
    delivery: &OutboxDelivery,
) -> Result<(), StoreError> {
    let sequence = i64::try_from(delivery.origin().sequence().get())
        .map_err(|_| StoreError::corrupt("outbox origin sequence"))?;
    let row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(delivery.intent().tenant_id().as_str())
        .bind(*delivery.intent().run_id().as_uuid())
        .bind(sequence)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("outbox journal anchor load", source))?
        .ok_or_else(|| StoreError::corrupt("outbox journal anchor"))?;
    let event = decode_event(row)?;
    if event.head() != *delivery.origin() {
        return Err(StoreError::corrupt("outbox journal anchor"));
    }
    Ok(())
}

fn verify_outbox_projection_records(
    row: &OutboxDeliveryRow,
    delivery: &OutboxDelivery,
    attempts: &[OutboxAttempt],
) -> Result<(), StoreError> {
    if usize::try_from(row.attempt_count).ok() != Some(attempts.len())
        || row.attempt_count < 0
        || usize::try_from(row.attempt_count)
            .ok()
            .is_none_or(|count| count > MAX_OUTBOX_ATTEMPTS)
    {
        return Err(StoreError::corrupt("outbox attempt count projection"));
    }
    if row.status == "expired" {
        return verify_expired_outbox_projection(row, delivery, attempts);
    }
    let Some(last) = attempts.last() else {
        return verify_initial_outbox_projection(row, delivery);
    };
    let updated_at = from_database_time(row.updated_at)?;
    let next_attempt_at = row.next_attempt_at.map(from_database_time).transpose()?;
    let terminal_at = row.terminal_at.map(from_database_time).transpose()?;
    let last_completion_digest = row
        .last_completion_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "outbox last completion digest"))
        .transpose()?;

    let start = last.start();
    verify_outbox_current_attempt_projection(row, start)?;

    let Some(completion) = last.completion() else {
        if attempts.len() == MAX_OUTBOX_ATTEMPTS
            && start.expires_at() < delivery.intent().expires_at()
            && row.status == "dead_letter"
            && next_attempt_at.is_none()
            && last_completion_digest.is_none()
            && terminal_at == Some(start.expires_at())
            && updated_at == start.expires_at()
        {
            return Ok(());
        }
        if row.status != "delivering"
            || next_attempt_at != Some(start.expires_at())
            || last_completion_digest.is_some()
            || terminal_at.is_some()
            || updated_at != start.started_at()
        {
            return Err(StoreError::corrupt("outbox delivering projection"));
        }
        return Ok(());
    };

    if last_completion_digest != Some(completion.digest())
        || updated_at != completion.completed_at()
    {
        return Err(StoreError::corrupt("outbox completion projection"));
    }
    match completion.outcome() {
        OutboxAttemptOutcome::Acknowledged { .. } => {
            if row.status != "acknowledged"
                || next_attempt_at.is_some()
                || terminal_at != Some(completion.completed_at())
            {
                return Err(StoreError::corrupt("outbox acknowledgement projection"));
            }
        }
        OutboxAttemptOutcome::Failed { failure } => match failure.retry_advice() {
            RetryAdvice::Never => {
                if row.status != "dead_letter"
                    || next_attempt_at.is_some()
                    || terminal_at != Some(completion.completed_at())
                {
                    return Err(StoreError::corrupt("outbox dead-letter projection"));
                }
            }
            RetryAdvice::SafeAfter { .. } if attempts.len() == MAX_OUTBOX_ATTEMPTS => {
                if row.status != "dead_letter"
                    || next_attempt_at.is_some()
                    || terminal_at != Some(completion.completed_at())
                {
                    return Err(StoreError::corrupt("outbox attempt-limit projection"));
                }
            }
            RetryAdvice::SafeAfter { delay } => {
                let retry_at = add_duration(
                    completion.completed_at(),
                    Duration::from_millis(
                        u64::try_from(delay.as_i64())
                            .map_err(|_| StoreError::corrupt("outbox retry delay"))?,
                    ),
                )?;
                if row.status != "retry_scheduled"
                    || next_attempt_at != Some(retry_at)
                    || terminal_at.is_some()
                {
                    return Err(StoreError::corrupt("outbox retry projection"));
                }
            }
            RetryAdvice::ReconcileFirst => {
                return Err(StoreError::corrupt("outbox retry advice"));
            }
        },
        _ => return Err(StoreError::corrupt("outbox attempt outcome")),
    }
    Ok(())
}

fn verify_initial_outbox_projection(
    row: &OutboxDeliveryRow,
    delivery: &OutboxDelivery,
) -> Result<(), StoreError> {
    let next_attempt_at = row.next_attempt_at.map(from_database_time).transpose()?;
    if row.status != "pending"
        || row.current_attempt_id.is_some()
        || row.current_epoch.is_some()
        || row.current_attempt_started_at.is_some()
        || row.current_attempt_expires_at.is_some()
        || next_attempt_at != Some(delivery.origin().recorded_at())
        || row.last_completion_digest.is_some()
        || row.terminal_at.is_some()
        || from_database_time(row.updated_at)? != delivery.origin().recorded_at()
    {
        return Err(StoreError::corrupt("outbox initial projection"));
    }
    Ok(())
}

fn verify_expired_outbox_projection(
    row: &OutboxDeliveryRow,
    delivery: &OutboxDelivery,
    attempts: &[OutboxAttempt],
) -> Result<(), StoreError> {
    let mut verifier = OutboxAttemptHistoryVerifier::new(delivery);
    for attempt in attempts {
        verifier
            .verify_next(attempt)
            .map_err(|_| StoreError::corrupt("outbox expired history"))?;
    }
    let next_attempt_at = row.next_attempt_at.map(from_database_time).transpose()?;
    let terminal_at = row.terminal_at.map(from_database_time).transpose()?;
    let updated_at = from_database_time(row.updated_at)?;
    let last_completion_digest = row
        .last_completion_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "outbox last completion digest"))
        .transpose()?;
    if verifier
        .status_at(delivery.intent().expires_at())
        .map_err(|_| StoreError::corrupt("outbox expiry projection"))?
        != OutboxDeliveryStatus::Expired
        || next_attempt_at.is_some()
        || terminal_at != Some(delivery.intent().expires_at())
        || updated_at != delivery.intent().expires_at()
        || last_completion_digest
            != attempts
                .last()
                .and_then(OutboxAttempt::completion)
                .map(OutboxAttemptCompletion::digest)
    {
        return Err(StoreError::corrupt("outbox expiry projection"));
    }
    if let Some(last) = attempts.last() {
        verify_outbox_current_attempt_projection(row, last.start())?;
    } else if row.current_attempt_id.is_some()
        || row.current_epoch.is_some()
        || row.current_attempt_started_at.is_some()
        || row.current_attempt_expires_at.is_some()
    {
        return Err(StoreError::corrupt("outbox expired current attempt"));
    }
    Ok(())
}

fn verify_outbox_current_attempt_projection(
    row: &OutboxDeliveryRow,
    start: &OutboxAttemptStart,
) -> Result<(), StoreError> {
    let epoch = i64::try_from(start.fence().epoch().get())
        .map_err(|_| StoreError::corrupt("outbox current epoch"))?;
    if row.current_attempt_id != Some(*start.fence().attempt_id().as_uuid())
        || row.current_epoch != Some(epoch)
        || row
            .current_attempt_started_at
            .map(from_database_time)
            .transpose()?
            != Some(start.started_at())
        || row
            .current_attempt_expires_at
            .map(from_database_time)
            .transpose()?
            != Some(start.expires_at())
    {
        return Err(StoreError::corrupt("outbox current attempt projection"));
    }
    Ok(())
}

fn outbox_outcomes_equal(left: &OutboxAttemptOutcome, right: &OutboxAttemptOutcome) -> bool {
    match (
        serde_json_canonicalizer::to_vec(left),
        serde_json_canonicalizer::to_vec(right),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn outbox_attempts_equal(left: &OutboxAttempt, right: &OutboxAttempt) -> bool {
    match (
        serde_json_canonicalizer::to_vec(left),
        serde_json_canonicalizer::to_vec(right),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn encode_node_attempt(attempt: &NodeAttempt) -> Result<Vec<u8>, StoreError> {
    serde_json_canonicalizer::to_vec(attempt).map_err(|_| StoreError::encoding("node attempt"))
}

fn encode_node_attempt_start(start: &NodeAttemptStart) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(start)
        .map_err(|_| StoreError::encoding("node attempt start"))?;
    if bytes.is_empty() || bytes.len() > MAX_NODE_ATTEMPT_START_BYTES {
        return Err(StoreError::encoding("node attempt start size"));
    }
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn decode_node_attempt_start(row: &NodeAttemptStartRow) -> Result<NodeAttemptStart, StoreError> {
    if row.start_bytes.is_empty() || row.start_bytes.len() > MAX_NODE_ATTEMPT_START_BYTES {
        return Err(StoreError::corrupt("node attempt start byte length"));
    }
    let start = serde_json::from_slice::<NodeAttemptStart>(&row.start_bytes)
        .map_err(|_| StoreError::corrupt("node attempt start value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&start)
        .map_err(|_| StoreError::corrupt("node attempt start canonicalization"))?;
    if canonical != row.start_bytes {
        return Err(StoreError::corrupt("node attempt start canonical bytes"));
    }

    let activation = start.activation();
    let base = activation.base_checkpoint();
    let base_journal = base.journal_head();
    let journal = start.journal_head();
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::corrupt("node attempt base superstep"))?;
    let base_journal_sequence = i64::try_from(base_journal.sequence().get())
        .map_err(|_| StoreError::corrupt("node attempt base journal sequence"))?;
    let fence_epoch = i64::try_from(start.fence().epoch().get())
        .map_err(|_| StoreError::corrupt("node attempt fence epoch"))?;
    let journal_sequence = i64::try_from(journal.sequence().get())
        .map_err(|_| StoreError::corrupt("node attempt journal sequence"))?;

    if activation.tenant_id().as_str() != row.tenant_id
        || *activation.run_id().as_uuid() != row.run_id
        || *base.checkpoint_id().as_uuid() != row.base_checkpoint_id
        || base_superstep != row.base_superstep
        || base.digest()
            != decode_digest(
                &row.base_checkpoint_digest,
                "node attempt base checkpoint digest",
            )?
        || base_journal_sequence != row.base_journal_sequence
        || *base_journal.event_id().as_uuid() != row.base_journal_event_id
        || base_journal.recorded_at() != from_database_time(row.base_journal_recorded_at)?
        || base_journal.digest()
            != decode_digest(&row.base_journal_digest, "node attempt base journal digest")?
        || activation.graph_namespace().as_str() != row.graph_namespace
        || activation.node_id().as_str() != row.node_id
        || activation.input_digest()
            != decode_digest(&row.activation_input_digest, "node attempt input digest")?
        || start.activation_digest()
            != decode_digest(&row.activation_digest, "node attempt activation digest")?
        || *start.attempt_id().as_uuid() != row.attempt_id
        || *start.fence().attempt_id().as_uuid() != row.fence_attempt_id
        || fence_epoch != row.fence_epoch
        || journal_sequence != row.journal_sequence
        || *journal.event_id().as_uuid() != row.journal_event_id
        || journal.recorded_at() != from_database_time(row.journal_recorded_at)?
        || journal.digest() != decode_digest(&row.journal_digest, "node attempt journal digest")?
        || start.digest() != decode_digest(&row.start_digest, "node attempt start digest")?
        || journal.recorded_at() != from_database_time(row.created_at)?
    {
        return Err(StoreError::corrupt("node attempt start projection"));
    }
    Ok(start)
}

fn encode_node_attempt_completion(
    completion: &NodeAttemptCompletion,
) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(completion)
        .map_err(|_| StoreError::encoding("node attempt completion"))?;
    if bytes.is_empty() || bytes.len() > MAX_NODE_ATTEMPT_COMPLETION_BYTES {
        return Err(StoreError::encoding("node attempt completion size"));
    }
    Ok(bytes)
}

fn node_attempt_status_text(status: NodeAttemptStatus) -> Result<&'static str, StoreError> {
    match status {
        NodeAttemptStatus::Executing => Ok("executing"),
        NodeAttemptStatus::Succeeded => Ok("succeeded"),
        NodeAttemptStatus::Failed => Ok("failed"),
        _ => Err(StoreError::encoding("node attempt status")),
    }
}

fn node_attempt_retry_projection(
    completion: &NodeAttemptCompletion,
) -> Result<(Option<&'static str>, Option<DateTime<Utc>>), StoreError> {
    let Some(failure) = completion.outcome().failure() else {
        return Ok((None, None));
    };
    match failure.retry_advice() {
        RetryAdvice::Never => Ok((Some("never"), None)),
        RetryAdvice::SafeAfter { delay } => {
            let delay_micros = delay
                .as_i64()
                .checked_mul(1_000)
                .ok_or_else(|| StoreError::encoding("node attempt retry time"))?;
            let eligible_micros = completion
                .journal_head()
                .recorded_at()
                .unix_micros()
                .checked_add(delay_micros)
                .ok_or_else(|| StoreError::encoding("node attempt retry time"))?;
            let eligible = Timestamp::from_unix_micros(eligible_micros)
                .map_err(|_| StoreError::encoding("node attempt retry time"))?;
            Ok((Some("safe_after"), Some(to_database_time(eligible)?)))
        }
        RetryAdvice::ReconcileFirst => Err(StoreError::InvalidNodeAttemptTransition),
    }
}

#[allow(clippy::too_many_lines)]
fn decode_node_attempt_completion(
    row: &NodeAttemptCompletionRow,
    expected_start: &NodeAttemptStart,
) -> Result<NodeAttemptCompletion, StoreError> {
    if row.completion_bytes.is_empty()
        || row.completion_bytes.len() > MAX_NODE_ATTEMPT_COMPLETION_BYTES
    {
        return Err(StoreError::corrupt("node attempt completion byte length"));
    }
    let completion = serde_json::from_slice::<NodeAttemptCompletion>(&row.completion_bytes)
        .map_err(|_| StoreError::corrupt("node attempt completion value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&completion)
        .map_err(|_| StoreError::corrupt("node attempt completion canonicalization"))?;
    if canonical != row.completion_bytes || completion.start() != &expected_start.head() {
        return Err(StoreError::corrupt(
            "node attempt completion canonical bytes",
        ));
    }

    let start = completion.start();
    let activation = start.activation();
    let base = activation.base_checkpoint();
    let start_journal = start.journal_head();
    let journal = completion.journal_head();
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::corrupt("node completion base superstep"))?;
    let fence_epoch = i64::try_from(start.fence().epoch().get())
        .map_err(|_| StoreError::corrupt("node completion fence epoch"))?;
    let start_sequence = i64::try_from(start_journal.sequence().get())
        .map_err(|_| StoreError::corrupt("node completion start sequence"))?;
    let journal_sequence = i64::try_from(journal.sequence().get())
        .map_err(|_| StoreError::corrupt("node completion journal sequence"))?;
    let (result_intent_digest, result_record_digest, failure_id) = match completion.outcome() {
        NodeAttemptOutcome::Succeeded { result } => {
            (Some(result.intent_digest()), Some(result.digest()), None)
        }
        NodeAttemptOutcome::Failed { failure } => (None, None, Some(*failure.id().as_uuid())),
        _ => return Err(StoreError::corrupt("node attempt completion outcome")),
    };
    let (retry_kind, retry_not_before) = node_attempt_retry_projection(&completion)
        .map_err(|_| StoreError::corrupt("node completion retry projection"))?;

    if activation.tenant_id().as_str() != row.tenant_id
        || *activation.run_id().as_uuid() != row.run_id
        || *start.attempt_id().as_uuid() != row.attempt_id
        || *base.checkpoint_id().as_uuid() != row.base_checkpoint_id
        || base_superstep != row.base_superstep
        || base.digest()
            != decode_digest(&row.base_checkpoint_digest, "node completion base digest")?
        || activation.graph_namespace().as_str() != row.graph_namespace
        || activation.node_id().as_str() != row.node_id
        || activation.input_digest()
            != decode_digest(&row.activation_input_digest, "node completion input digest")?
        || expected_start.activation_digest()
            != decode_digest(&row.activation_digest, "node completion activation digest")?
        || *start.fence().attempt_id().as_uuid() != row.fence_attempt_id
        || fence_epoch != row.fence_epoch
        || start_sequence != row.start_journal_sequence
        || *start_journal.event_id().as_uuid() != row.start_journal_event_id
        || start_journal.recorded_at() != from_database_time(row.start_journal_recorded_at)?
        || start_journal.digest()
            != decode_digest(
                &row.start_journal_digest,
                "node completion start journal digest",
            )?
        || start.digest() != decode_digest(&row.start_digest, "node completion start digest")?
        || node_attempt_status_text(completion.status())
            .map_err(|_| StoreError::corrupt("node completion status"))?
            != row.status
        || journal_sequence != row.journal_sequence
        || *journal.event_id().as_uuid() != row.journal_event_id
        || journal.recorded_at() != from_database_time(row.journal_recorded_at)?
        || journal.digest() != decode_digest(&row.journal_digest, "node completion journal digest")?
        || decode_optional_digest(
            row.result_intent_digest.as_deref(),
            "node result intent digest",
        )? != result_intent_digest
        || decode_optional_digest(
            row.result_record_digest.as_deref(),
            "node result record digest",
        )? != result_record_digest
        || row.failure_id != failure_id
        || row.retry_kind.as_deref() != retry_kind
        || row.retry_not_before != retry_not_before
        || completion.digest() != decode_digest(&row.completion_digest, "node completion digest")?
        || journal.recorded_at() != from_database_time(row.created_at)?
    {
        return Err(StoreError::corrupt("node attempt completion projection"));
    }
    Ok(completion)
}

fn decode_optional_digest(
    bytes: Option<&[u8]>,
    record: &'static str,
) -> Result<Option<Digest>, StoreError> {
    bytes.map(|bytes| decode_digest(bytes, record)).transpose()
}

async fn load_node_attempt_record(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: &RunId,
    attempt_id: AttemptId,
) -> Result<Option<NodeAttempt>, StoreError> {
    let Some(start_row) = query_as::<_, NodeAttemptStartRow>(SELECT_NODE_ATTEMPT_BY_ID)
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(*attempt_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("node attempt lookup", source))?
    else {
        return Ok(None);
    };
    load_node_attempt_from_start_row(transaction, start_row)
        .await
        .map(Some)
}

async fn load_node_attempt_from_start_row(
    transaction: &mut Transaction<'_, Postgres>,
    start_row: NodeAttemptStartRow,
) -> Result<NodeAttempt, StoreError> {
    let start = decode_node_attempt_start(&start_row)?;
    let completion_row = query_as::<_, NodeAttemptCompletionRow>(SELECT_NODE_ATTEMPT_COMPLETION)
        .bind(start.activation().tenant_id().as_str())
        .bind(*start.activation().run_id().as_uuid())
        .bind(*start.attempt_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("node attempt completion lookup", source))?;
    let completion = completion_row
        .map(|row| decode_node_attempt_completion(&row, &start))
        .transpose()?;
    NodeAttempt::restore(start, completion).map_err(|_| StoreError::corrupt("node attempt join"))
}

async fn load_locked_node_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    expected: &NodeAttemptStartHead,
) -> Result<NodeAttempt, StoreError> {
    let activation = expected.activation();
    let row = query_as::<_, NodeAttemptStartRow>(SELECT_NODE_ATTEMPT_BY_ID_FOR_UPDATE)
        .bind(activation.tenant_id().as_str())
        .bind(*activation.run_id().as_uuid())
        .bind(*expected.attempt_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("node attempt row lock", source))?
        .ok_or(StoreError::NodeAttemptNotFound)?;
    let attempt = load_node_attempt_from_start_row(transaction, row).await?;
    if attempt.start().head() != *expected {
        return Err(StoreError::StaleNodeAttemptStart);
    }
    Ok(attempt)
}

async fn load_latest_locked_node_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    activation: &NodeActivation,
) -> Result<Option<NodeAttempt>, StoreError> {
    let base = activation.base_checkpoint();
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
    let row = query_as::<_, NodeAttemptStartRow>(SELECT_LATEST_NODE_ATTEMPT_FOR_UPDATE)
        .bind(activation.tenant_id().as_str())
        .bind(*activation.run_id().as_uuid())
        .bind(*base.checkpoint_id().as_uuid())
        .bind(base_superstep)
        .bind(base.digest().as_bytes())
        .bind(activation.graph_namespace().as_str())
        .bind(activation.node_id().as_str())
        .bind(activation.input_digest().as_bytes())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("latest node attempt row lock", source))?;
    match row {
        Some(row) => load_node_attempt_from_start_row(transaction, row)
            .await
            .map(Some),
        None => Ok(None),
    }
}

async fn count_node_attempts(
    transaction: &mut Transaction<'_, Postgres>,
    activation: &NodeActivation,
) -> Result<usize, StoreError> {
    let base = activation.base_checkpoint();
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
    let count = query_scalar::<_, i64>(SELECT_NODE_ATTEMPT_COUNT)
        .bind(activation.tenant_id().as_str())
        .bind(*activation.run_id().as_uuid())
        .bind(*base.checkpoint_id().as_uuid())
        .bind(base_superstep)
        .bind(base.digest().as_bytes())
        .bind(activation.graph_namespace().as_str())
        .bind(activation.node_id().as_str())
        .bind(activation.input_digest().as_bytes())
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("node attempt history count", source))?;
    usize::try_from(count).map_err(|_| StoreError::corrupt("node attempt history count"))
}

async fn verify_node_attempt_anchor(
    transaction: &mut Transaction<'_, Postgres>,
    journal: &JournalHead,
    fence: &RunFence,
    projection_digest: Digest,
    operation: &'static str,
) -> Result<JournalEvent, StoreError> {
    let sequence = i64::try_from(journal.sequence().get())
        .map_err(|_| StoreError::corrupt("node attempt journal sequence"))?;
    let row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(journal.tenant_id().as_str())
        .bind(*journal.run_id().as_uuid())
        .bind(sequence)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database(operation, source))?
        .ok_or_else(|| StoreError::corrupt("node attempt journal anchor"))?;
    let durable_projection = row
        .projection_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "node attempt projection digest"))
        .transpose()?;
    let event = decode_event(row)?;
    if event.head() != *journal
        || event.source().worker_fence() != Some(fence)
        || durable_projection != Some(projection_digest)
    {
        return Err(StoreError::corrupt("node attempt journal anchor"));
    }
    Ok(event)
}

async fn verify_node_attempt_base_checkpoint(
    transaction: &mut Transaction<'_, Postgres>,
    start: &NodeAttemptStart,
) -> Result<(), StoreError> {
    let activation = start.activation();
    let base = activation.base_checkpoint();
    let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
        .bind(activation.tenant_id().as_str())
        .bind(*activation.run_id().as_uuid())
        .bind(*base.checkpoint_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("node attempt base checkpoint", source))?
        .ok_or_else(|| StoreError::corrupt("node attempt base checkpoint"))?;
    let checkpoint = decode_checkpoint(row)?;
    if checkpoint.head() != *base || !node_attempt_activation_is_ready(&checkpoint, activation) {
        return Err(StoreError::corrupt("node attempt base checkpoint"));
    }
    verify_checkpoint_anchor(transaction, &checkpoint).await
}

async fn verify_node_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    attempt: &NodeAttempt,
) -> Result<JournalEvent, StoreError> {
    let start = attempt.start();
    verify_node_attempt_base_checkpoint(transaction, start).await?;
    let start_event = verify_node_attempt_anchor(
        transaction,
        start.journal_head(),
        start.fence(),
        start.digest(),
        "node attempt start anchor",
    )
    .await?;

    let Some(completion) = attempt.completion() else {
        return Ok(start_event);
    };
    let event = verify_node_attempt_anchor(
        transaction,
        completion.journal_head(),
        start.fence(),
        completion.digest(),
        "node attempt completion anchor",
    )
    .await?;

    if let NodeAttemptOutcome::Succeeded { result } = completion.outcome() {
        let row = load_pending_node_result_row(transaction, result.activation())
            .await?
            .ok_or_else(|| StoreError::corrupt("node attempt successful result"))?;
        if row.node_attempt_id != Some(*start.attempt_id().as_uuid()) {
            return Err(StoreError::corrupt("node attempt successful result owner"));
        }
        let durable_result = decode_pending_node_result(&row)?;
        if durable_result.head() != **result {
            return Err(StoreError::corrupt("node attempt successful result"));
        }
        verify_pending_node_result_base_checkpoint(transaction, &durable_result).await?;
        verify_pending_node_result_bindings(transaction, &durable_result).await?;
    }
    Ok(event)
}

fn encode_pending_node_result(result: &PendingNodeResult) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(result)
        .map_err(|_| StoreError::encoding("pending node result"))?;
    if bytes.is_empty() || bytes.len() > MAX_PENDING_NODE_RESULT_BYTES {
        return Err(StoreError::encoding("pending node result size"));
    }
    Ok(bytes)
}

#[allow(clippy::too_many_lines)]
fn decode_pending_node_result(row: &PendingNodeResultRow) -> Result<PendingNodeResult, StoreError> {
    if row.result_bytes.is_empty() || row.result_bytes.len() > MAX_PENDING_NODE_RESULT_BYTES {
        return Err(StoreError::corrupt("pending node result byte length"));
    }
    let result = serde_json::from_slice::<PendingNodeResult>(&row.result_bytes)
        .map_err(|_| StoreError::corrupt("pending node result value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&result)
        .map_err(|_| StoreError::corrupt("pending node result canonicalization"))?;
    if canonical != row.result_bytes {
        return Err(StoreError::corrupt("pending node result canonical bytes"));
    }

    let activation = result.intent().activation();
    let base = activation.base_checkpoint();
    let base_journal = base.journal_head();
    let journal = result.journal_head();
    let fence_epoch = i64::try_from(result.fence().epoch().get())
        .map_err(|_| StoreError::corrupt("pending node result fence epoch"))?;
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::corrupt("pending node result base superstep"))?;
    let base_journal_sequence = i64::try_from(base_journal.sequence().get())
        .map_err(|_| StoreError::corrupt("pending node result base journal sequence"))?;
    let journal_sequence = i64::try_from(journal.sequence().get())
        .map_err(|_| StoreError::corrupt("pending node result journal sequence"))?;

    if activation.tenant_id().as_str() != row.tenant_id
        || *activation.run_id().as_uuid() != row.run_id
        || *base.checkpoint_id().as_uuid() != row.base_checkpoint_id
        || base_superstep != row.base_superstep
        || base.digest()
            != decode_digest(
                &row.base_checkpoint_digest,
                "pending node result base checkpoint digest",
            )?
        || base_journal_sequence != row.base_journal_sequence
        || *base_journal.event_id().as_uuid() != row.base_journal_event_id
        || base_journal.recorded_at() != from_database_time(row.base_journal_recorded_at)?
        || base_journal.digest()
            != decode_digest(
                &row.base_journal_digest,
                "pending node result base journal digest",
            )?
        || activation.graph_namespace().as_str() != row.graph_namespace
        || activation.node_id().as_str() != row.node_id
        || activation.input_digest()
            != decode_digest(
                &row.activation_input_digest,
                "pending node result activation input digest",
            )?
        || result.intent().intent_digest()
            != decode_digest(&row.intent_digest, "pending node result intent digest")?
        || pending_node_result_control_kind_text(result.intent().control().kind())
            != row.control_kind
        || *result.fence().attempt_id().as_uuid() != row.fence_attempt_id
        || fence_epoch != row.fence_epoch
        || journal_sequence != row.journal_sequence
        || *journal.event_id().as_uuid() != row.journal_event_id
        || journal.recorded_at() != from_database_time(row.journal_recorded_at)?
        || journal.digest()
            != decode_digest(&row.journal_digest, "pending node result journal digest")?
        || result.digest()
            != decode_digest(&row.record_digest, "pending node result record digest")?
        || journal.recorded_at() != from_database_time(row.created_at)?
    {
        return Err(StoreError::corrupt("pending node result projection"));
    }
    Ok(result)
}

async fn load_pending_node_result_row(
    transaction: &mut Transaction<'_, Postgres>,
    activation: &NodeActivation,
) -> Result<Option<PendingNodeResultRow>, StoreError> {
    query_as::<_, PendingNodeResultRow>(SELECT_PENDING_NODE_RESULT)
        .bind(activation.tenant_id().as_str())
        .bind(*activation.run_id().as_uuid())
        .bind(*activation.base_checkpoint().checkpoint_id().as_uuid())
        .bind(activation.graph_namespace().as_str())
        .bind(activation.node_id().as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("pending node result lookup", source))
}

async fn load_pending_node_result_head_row(
    transaction: &mut Transaction<'_, Postgres>,
    head: &PendingNodeResultHead,
) -> Result<PendingNodeResultHeadRow, StoreError> {
    let activation = head.activation();
    query_as::<_, PendingNodeResultHeadRow>(SELECT_PENDING_NODE_RESULT_HEAD)
        .bind(activation.tenant_id().as_str())
        .bind(*activation.run_id().as_uuid())
        .bind(*activation.base_checkpoint().checkpoint_id().as_uuid())
        .bind(activation.graph_namespace().as_str())
        .bind(activation.node_id().as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("pending node result cursor lookup", source))?
        .ok_or(StoreError::InvalidPendingNodeResultCursor)
}

fn decode_pending_node_result_head(
    row: PendingNodeResultHeadRow,
    base: &CheckpointHead,
) -> Result<PendingNodeResultHead, StoreError> {
    let base_superstep = nonnegative_superstep(row.base_superstep)?;
    if row.tenant_id != base.tenant_id().as_str()
        || row.run_id != *base.run_id().as_uuid()
        || row.base_checkpoint_id != *base.checkpoint_id().as_uuid()
        || base_superstep != base.superstep()
        || decode_digest(
            &row.base_checkpoint_digest,
            "pending result compact base digest",
        )? != base.digest()
    {
        return Err(StoreError::corrupt("pending result compact base"));
    }
    let graph_namespace = GraphNamespace::new(row.graph_namespace)
        .map_err(|_| StoreError::corrupt("pending result compact namespace"))?;
    let node_id =
        NodeId::new(row.node_id).map_err(|_| StoreError::corrupt("pending result compact node"))?;
    let activation = NodeActivation::new(
        base.clone(),
        graph_namespace,
        node_id,
        decode_digest(
            &row.activation_input_digest,
            "pending result compact input digest",
        )?,
    );
    let fence_epoch = u64::try_from(row.fence_epoch)
        .ok()
        .and_then(|value| FencingEpoch::new(value).ok())
        .ok_or_else(|| StoreError::corrupt("pending result compact fence epoch"))?;
    let fence = RunFence::new(
        base.tenant_id().clone(),
        base.run_id(),
        AttemptId::from_uuid(row.fence_attempt_id)
            .map_err(|_| StoreError::corrupt("pending result compact attempt"))?,
        fence_epoch,
    );
    let journal = JournalHead::new(
        base.tenant_id().clone(),
        base.run_id(),
        positive_sequence(row.journal_sequence)?,
        EventId::from_uuid(row.journal_event_id)
            .map_err(|_| StoreError::corrupt("pending result compact event"))?,
        from_database_time(row.journal_recorded_at)?,
        decode_digest(&row.journal_digest, "pending result compact journal digest")?,
    );
    PendingNodeResultHead::new(
        activation,
        decode_digest(&row.intent_digest, "pending result compact intent digest")?,
        fence,
        journal,
        decode_digest(&row.record_digest, "pending result compact record digest")?,
    )
    .map_err(|_| StoreError::corrupt("pending result compact head"))
}

async fn load_locked_barrier_result_heads(
    transaction: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
) -> Result<Vec<PendingNodeResultHead>, StoreError> {
    let query_limit = i64::try_from(BarrierResultHeads::MAX_LEN + 1)
        .map_err(|_| StoreError::InvalidCheckpointBarrier)?;
    let rows =
        query_as::<_, PendingNodeResultHeadRow>(SELECT_PENDING_NODE_RESULT_HEADS_FOR_BARRIER)
            .bind(base.tenant_id().as_str())
            .bind(*base.run_id().as_uuid())
            .bind(*base.checkpoint_id().as_uuid())
            .bind(query_limit)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|source| StoreError::database("checkpoint barrier result heads", source))?;
    rows.into_iter()
        .map(|row| decode_pending_node_result_head(row, base))
        .collect()
}

fn validate_complete_barrier_result_heads(
    durable: &[PendingNodeResultHead],
    expected: &BarrierResultHeads,
) -> Result<(), StoreError> {
    if durable == expected.as_slice() {
        return Ok(());
    }
    if durable.len() < expected.len()
        && durable
            .iter()
            .all(|head| expected.iter().any(|expected| expected == head))
    {
        return Err(StoreError::CheckpointBarrierIncomplete);
    }
    Err(StoreError::CheckpointBarrierResultConflict)
}

async fn load_barrier_consumption_rows(
    transaction: &mut Transaction<'_, Postgres>,
    base: &CheckpointHead,
) -> Result<Vec<PendingNodeResultConsumptionRow>, StoreError> {
    query_as::<_, PendingNodeResultConsumptionRow>(SELECT_PENDING_NODE_RESULT_CONSUMPTIONS_BY_BASE)
        .bind(base.tenant_id().as_str())
        .bind(*base.run_id().as_uuid())
        .bind(*base.checkpoint_id().as_uuid())
        .fetch_all(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("checkpoint barrier consumptions", source))
}

async fn verify_barrier_consumptions(
    transaction: &mut Transaction<'_, Postgres>,
    barrier: &CheckpointBarrier,
    successor: &Checkpoint,
) -> Result<(), StoreError> {
    let base = barrier.base_checkpoint();
    let rows = load_barrier_consumption_rows(transaction, base).await?;
    if rows.len() != barrier.result_heads().len() {
        return Err(StoreError::CheckpointBarrierCommitConflict);
    }
    for (row, result) in rows.iter().zip(barrier.result_heads().iter()) {
        let activation = result.activation();
        if row.tenant_id != base.tenant_id().as_str()
            || row.run_id != *base.run_id().as_uuid()
            || row.base_checkpoint_id != *base.checkpoint_id().as_uuid()
            || nonnegative_superstep(row.base_superstep)? != base.superstep()
            || decode_digest(
                &row.base_checkpoint_digest,
                "checkpoint barrier consumption base digest",
            )? != base.digest()
            || row.graph_namespace != activation.graph_namespace().as_str()
            || row.node_id != activation.node_id().as_str()
            || decode_digest(
                &row.result_record_digest,
                "checkpoint barrier consumption result digest",
            )? != result.digest()
            || row.successor_checkpoint_id != *successor.checkpoint_id().as_uuid()
            || nonnegative_superstep(row.successor_superstep)? != successor.superstep()
            || decode_digest(
                &row.successor_checkpoint_digest,
                "checkpoint barrier consumption successor digest",
            )? != successor.digest()
            || positive_sequence(row.successor_journal_sequence)?
                != successor.journal_head().sequence()
            || row.successor_journal_event_id != *successor.journal_head().event_id().as_uuid()
            || from_database_time(row.successor_journal_recorded_at)?
                != successor.journal_head().recorded_at()
            || decode_digest(
                &row.successor_journal_digest,
                "checkpoint barrier consumption journal digest",
            )? != successor.journal_head().digest()
            || from_database_time(row.created_at)? != successor.journal_head().recorded_at()
        {
            return Err(StoreError::CheckpointBarrierCommitConflict);
        }
    }
    Ok(())
}

fn pending_result_cursor_matches_base(
    cursor: &PendingNodeResultPageCursor,
    base: &CheckpointHead,
) -> bool {
    let snapshot = cursor.snapshot_journal_head();
    let after = cursor.after();
    cursor.base_checkpoint() == base
        && snapshot.tenant_id() == base.tenant_id()
        && snapshot.run_id() == base.run_id()
        && snapshot.sequence() >= after.journal_head().sequence()
        && snapshot.recorded_at() >= after.journal_head().recorded_at()
        && after.activation().base_checkpoint() == base
        && after.activation().graph_namespace().is_root()
}

fn pending_node_result_activation_is_ready(
    checkpoint: &Checkpoint,
    intent: &PendingNodeResultIntent,
) -> bool {
    activation_is_canonical_ready_root(checkpoint, intent.activation())
}

fn node_attempt_activation_is_ready(checkpoint: &Checkpoint, activation: &NodeActivation) -> bool {
    activation_is_canonical_ready_root(checkpoint, activation)
}

fn activation_is_canonical_ready_root(
    checkpoint: &Checkpoint,
    activation: &NodeActivation,
) -> bool {
    NodeActivation::for_ready_root(checkpoint, activation.node_id().clone())
        .is_ok_and(|expected| expected == *activation)
}

async fn reject_reused_node_worker_attempt(
    transaction: &mut Transaction<'_, Postgres>,
    activation: &NodeActivation,
    fence: &RunFence,
) -> Result<(), StoreError> {
    let base = activation.base_checkpoint();
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
    let fence_epoch =
        i64::try_from(fence.epoch().get()).map_err(|_| StoreError::InvalidNodeAttemptTransition)?;
    let reused: bool = query_scalar(
        r"
SELECT EXISTS (
    SELECT 1
    FROM stateknot.node_attempts
    WHERE tenant_id = $1
      AND run_id = $2
      AND base_checkpoint_id = $3
      AND base_superstep = $4
      AND base_checkpoint_digest = $5
      AND graph_namespace = $6
      AND node_id = $7
      AND activation_input_digest = $8
      AND fence_attempt_id = $9
      AND fence_epoch <> $10
)
",
    )
    .bind(activation.tenant_id().as_str())
    .bind(*activation.run_id().as_uuid())
    .bind(*base.checkpoint_id().as_uuid())
    .bind(base_superstep)
    .bind(base.digest().as_bytes())
    .bind(activation.graph_namespace().as_str())
    .bind(activation.node_id().as_str())
    .bind(activation.input_digest().as_bytes())
    .bind(*fence.attempt_id().as_uuid())
    .bind(fence_epoch)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("node attempt worker history", source))?;
    if reused {
        return Err(StoreError::InvalidNodeAttemptTransition);
    }
    Ok(())
}

async fn verify_pending_node_result_base_checkpoint(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
) -> Result<(), StoreError> {
    let activation = result.intent().activation();
    let base = activation.base_checkpoint();
    let row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
        .bind(activation.tenant_id().as_str())
        .bind(*activation.run_id().as_uuid())
        .bind(*base.checkpoint_id().as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("pending node result base checkpoint", source))?
        .ok_or_else(|| StoreError::corrupt("pending node result base checkpoint"))?;
    let checkpoint = decode_checkpoint(row)?;
    if checkpoint.head() != *base
        || !pending_node_result_activation_is_ready(&checkpoint, result.intent())
    {
        return Err(StoreError::corrupt("pending node result base checkpoint"));
    }
    verify_checkpoint_anchor(transaction, &checkpoint).await
}

async fn verify_pending_node_result_anchor(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
) -> Result<JournalEvent, StoreError> {
    let sequence = i64::try_from(result.journal_head().sequence().get())
        .map_err(|_| StoreError::corrupt("pending node result journal sequence"))?;
    let row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(result.intent().tenant_id().as_str())
        .bind(*result.intent().run_id().as_uuid())
        .bind(sequence)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("pending node result anchor", source))?
        .ok_or_else(|| StoreError::corrupt("pending node result journal anchor"))?;
    let projection_digest = row
        .projection_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "pending node result projection digest"))
        .transpose()?;
    let event = decode_event(row)?;
    if event.head() != *result.journal_head()
        || event.source().worker_fence() != Some(result.fence())
        || projection_digest != Some(result.digest())
    {
        return Err(StoreError::corrupt("pending node result journal anchor"));
    }
    Ok(event)
}

async fn verify_pending_node_result(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
) -> Result<JournalEvent, StoreError> {
    verify_pending_node_result_base_checkpoint(transaction, result).await?;
    let row = load_pending_node_result_row(transaction, result.intent().activation())
        .await?
        .ok_or_else(|| StoreError::corrupt("pending node result owner row"))?;
    let durable = decode_pending_node_result(&row)?;
    if encode_pending_node_result(&durable)? != encode_pending_node_result(result)? {
        return Err(StoreError::corrupt("pending node result owner row"));
    }
    let event = if let Some(owner) = row.node_attempt_id {
        let attempt_id = AttemptId::from_uuid(owner)
            .map_err(|_| StoreError::corrupt("pending node result attempt owner"))?;
        let attempt = load_node_attempt_record(
            transaction,
            result.intent().tenant_id(),
            &result.intent().run_id(),
            attempt_id,
        )
        .await?
        .ok_or_else(|| StoreError::corrupt("pending node result attempt owner"))?;
        let completion = attempt
            .completion()
            .ok_or_else(|| StoreError::corrupt("pending node result attempt completion"))?;
        if attempt.start().attempt_id() != attempt_id
            || completion.outcome().result() != Some(&result.head())
        {
            return Err(StoreError::corrupt(
                "pending node result attempt completion",
            ));
        }
        verify_node_attempt_base_checkpoint(transaction, attempt.start()).await?;
        verify_node_attempt_anchor(
            transaction,
            attempt.start().journal_head(),
            attempt.start().fence(),
            attempt.start().digest(),
            "pending result node start anchor",
        )
        .await?;
        verify_node_attempt_anchor(
            transaction,
            completion.journal_head(),
            attempt.start().fence(),
            completion.digest(),
            "pending result node completion anchor",
        )
        .await?
    } else {
        verify_pending_node_result_anchor(transaction, result).await?
    };
    verify_pending_node_result_bindings(transaction, result).await?;
    Ok(event)
}

async fn verify_pending_node_result_bindings(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
) -> Result<(), StoreError> {
    let activation = result.intent().activation();
    let tool_rows =
        query_as::<_, PendingNodeResultBindingRow>(SELECT_PENDING_NODE_RESULT_TOOL_BINDINGS)
            .bind(activation.tenant_id().as_str())
            .bind(*activation.run_id().as_uuid())
            .bind(*activation.base_checkpoint().checkpoint_id().as_uuid())
            .bind(activation.graph_namespace().as_str())
            .bind(activation.node_id().as_str())
            .fetch_all(&mut **transaction)
            .await
            .map_err(|source| StoreError::database("pending node result tool bindings", source))?;
    let model_rows =
        query_as::<_, PendingNodeResultBindingRow>(SELECT_PENDING_NODE_RESULT_MODEL_BINDINGS)
            .bind(activation.tenant_id().as_str())
            .bind(*activation.run_id().as_uuid())
            .bind(*activation.base_checkpoint().checkpoint_id().as_uuid())
            .bind(activation.graph_namespace().as_str())
            .bind(activation.node_id().as_str())
            .fetch_all(&mut **transaction)
            .await
            .map_err(|source| StoreError::database("pending node result model bindings", source))?;

    let tool_bindings = result
        .intent()
        .bindings()
        .iter()
        .filter(|binding| binding.kind() == NodeInvocationBindingKind::Tool)
        .collect::<Vec<_>>();
    let model_bindings = result
        .intent()
        .bindings()
        .iter()
        .filter(|binding| binding.kind() == NodeInvocationBindingKind::Model)
        .collect::<Vec<_>>();
    verify_pending_node_result_binding_rows(result, &tool_bindings, tool_rows)?;
    verify_pending_node_result_binding_rows(result, &model_bindings, model_rows)?;

    let mut anchors = BTreeMap::new();
    verify_pending_tool_invocations(transaction, result, &tool_bindings, &mut anchors).await?;
    verify_pending_model_invocations(transaction, result, &model_bindings, &mut anchors).await?;
    verify_pending_invocation_anchors(transaction, result, anchors).await
}

fn verify_pending_node_result_binding_rows(
    result: &PendingNodeResult,
    expected: &[&NodeInvocationBinding],
    rows: Vec<PendingNodeResultBindingRow>,
) -> Result<(), StoreError> {
    if rows.len() != expected.len() {
        return Err(StoreError::corrupt("pending node result binding count"));
    }
    let mut rows_by_id = BTreeMap::new();
    for row in rows {
        let id = row.invocation_id;
        if rows_by_id.insert(id, row).is_some() {
            return Err(StoreError::corrupt("pending node result binding identity"));
        }
    }
    for binding in expected {
        let row = rows_by_id
            .remove(binding.invocation_id().as_uuid())
            .ok_or_else(|| StoreError::corrupt("pending node result binding identity"))?;
        if !pending_node_result_binding_row_matches(&row, result, binding)? {
            return Err(StoreError::corrupt(
                "pending node result binding projection",
            ));
        }
    }
    if !rows_by_id.is_empty() {
        return Err(StoreError::corrupt("pending node result binding identity"));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn pending_node_result_binding_row_matches(
    row: &PendingNodeResultBindingRow,
    result: &PendingNodeResult,
    binding: &NodeInvocationBinding,
) -> Result<bool, StoreError> {
    let activation = result.intent().activation();
    let base = activation.base_checkpoint();
    let result_journal = result.journal_head();
    let invocation_journal = binding.journal_head();
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::corrupt("pending node result binding base superstep"))?;
    let result_journal_sequence = i64::try_from(result_journal.sequence().get())
        .map_err(|_| StoreError::corrupt("pending node result binding result sequence"))?;
    let invocation_journal_sequence = i64::try_from(invocation_journal.sequence().get())
        .map_err(|_| StoreError::corrupt("pending node result binding invocation sequence"))?;
    let (revision, record_digest) = match binding {
        NodeInvocationBinding::Tool { head, .. } => (
            i64::try_from(head.revision().get())
                .map_err(|_| StoreError::corrupt("pending tool binding revision"))?,
            head.digest(),
        ),
        NodeInvocationBinding::Model { head, .. } => (
            i64::try_from(head.revision().get())
                .map_err(|_| StoreError::corrupt("pending model binding revision"))?,
            head.digest(),
        ),
    };
    Ok(row.tenant_id == activation.tenant_id().as_str()
        && row.run_id == *activation.run_id().as_uuid()
        && row.base_checkpoint_id == *base.checkpoint_id().as_uuid()
        && row.base_superstep == base_superstep
        && decode_digest(
            &row.base_checkpoint_digest,
            "pending node result binding base digest",
        )? == base.digest()
        && row.graph_namespace == activation.graph_namespace().as_str()
        && row.node_id == activation.node_id().as_str()
        && decode_digest(
            &row.activation_input_digest,
            "pending node result binding input digest",
        )? == activation.input_digest()
        && decode_digest(
            &row.result_record_digest,
            "pending node result binding result digest",
        )? == result.digest()
        && row.result_journal_sequence == result_journal_sequence
        && from_database_time(row.result_journal_recorded_at)? == result_journal.recorded_at()
        && decode_digest(
            &row.result_journal_digest,
            "pending node result binding result journal digest",
        )? == result_journal.digest()
        && row.invocation_id == *binding.invocation_id().as_uuid()
        && row.invocation_revision == revision
        && decode_digest(
            &row.invocation_record_digest,
            "pending node result binding invocation digest",
        )? == record_digest
        && row.invocation_journal_sequence == invocation_journal_sequence
        && from_database_time(row.invocation_journal_recorded_at)?
            == invocation_journal.recorded_at()
        && decode_digest(
            &row.invocation_journal_digest,
            "pending node result binding invocation journal digest",
        )? == invocation_journal.digest())
}

async fn verify_pending_tool_invocations(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
    bindings: &[&NodeInvocationBinding],
    anchors: &mut BTreeMap<JournalSequence, (JournalHead, Digest)>,
) -> Result<(), StoreError> {
    for chunk in bindings.chunks(PENDING_TOOL_BINDING_BATCH_SIZE) {
        verify_pending_tool_invocation_chunk(transaction, result, chunk, anchors).await?;
    }
    Ok(())
}

async fn verify_pending_tool_invocation_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
    bindings: &[&NodeInvocationBinding],
    anchors: &mut BTreeMap<JournalSequence, (JournalHead, Digest)>,
) -> Result<(), StoreError> {
    let ids = bindings
        .iter()
        .map(|binding| *binding.invocation_id().as_uuid())
        .collect::<Vec<_>>();
    let revisions = bindings
        .iter()
        .map(|binding| {
            let head = binding
                .tool_head()
                .ok_or_else(|| StoreError::corrupt("pending tool binding kind"))?;
            i64::try_from(head.revision().get())
                .map_err(|_| StoreError::corrupt("pending tool binding revision"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let intent_rows = query_as::<_, ToolInvocationRow>(SELECT_TOOL_INVOCATIONS_BY_IDS)
        .bind(result.intent().tenant_id().as_str())
        .bind(*result.intent().run_id().as_uuid())
        .bind(&ids)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("pending tool binding intents", source))?;
    let revision_rows =
        query_as::<_, ToolInvocationRevisionRow>(SELECT_TOOL_INVOCATION_REVISIONS_BY_HEADS)
            .bind(result.intent().tenant_id().as_str())
            .bind(*result.intent().run_id().as_uuid())
            .bind(&ids)
            .bind(&revisions)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|source| StoreError::database("pending tool binding revisions", source))?;
    let mut intents = unique_tool_invocation_rows(intent_rows)?;
    let mut records = unique_tool_invocation_revision_rows(revision_rows)?;
    if intents.len() != bindings.len() || records.len() != bindings.len() {
        return Err(StoreError::corrupt("pending tool binding records"));
    }
    for binding in bindings {
        let head = binding
            .tool_head()
            .ok_or_else(|| StoreError::corrupt("pending tool binding kind"))?;
        let intent_row = intents
            .remove(head.invocation_id().as_uuid())
            .ok_or_else(|| StoreError::corrupt("pending tool binding intent"))?;
        let intent = decode_tool_invocation_intent(&intent_row)?;
        let revision = i64::try_from(head.revision().get())
            .map_err(|_| StoreError::corrupt("pending tool binding revision"))?;
        let revision_row = records
            .remove(&(*head.invocation_id().as_uuid(), revision))
            .ok_or_else(|| StoreError::corrupt("pending tool binding revision"))?;
        let invocation = decode_tool_invocation_revision(revision_row, &intent)?;
        validate_tool_invocation_current_projection(&intent_row, &invocation)?;
        if intent.activation() != result.intent().activation() || invocation.head() != *head {
            return Err(StoreError::corrupt("pending tool binding record"));
        }
        insert_pending_invocation_anchor(anchors, invocation.journal_head(), invocation.digest())?;
    }
    if !intents.is_empty() || !records.is_empty() {
        return Err(StoreError::corrupt("pending tool binding records"));
    }
    Ok(())
}

async fn verify_pending_model_invocations(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
    bindings: &[&NodeInvocationBinding],
    anchors: &mut BTreeMap<JournalSequence, (JournalHead, Digest)>,
) -> Result<(), StoreError> {
    for chunk in bindings.chunks(PENDING_MODEL_BINDING_BATCH_SIZE) {
        verify_pending_model_invocation_chunk(transaction, result, chunk, anchors).await?;
    }
    Ok(())
}

async fn verify_pending_model_invocation_chunk(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
    bindings: &[&NodeInvocationBinding],
    anchors: &mut BTreeMap<JournalSequence, (JournalHead, Digest)>,
) -> Result<(), StoreError> {
    let ids = bindings
        .iter()
        .map(|binding| *binding.invocation_id().as_uuid())
        .collect::<Vec<_>>();
    let revisions = bindings
        .iter()
        .map(|binding| {
            let head = binding
                .model_head()
                .ok_or_else(|| StoreError::corrupt("pending model binding kind"))?;
            i64::try_from(head.revision().get())
                .map_err(|_| StoreError::corrupt("pending model binding revision"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let intent_rows = query_as::<_, ModelInvocationRow>(SELECT_MODEL_INVOCATIONS_BY_IDS)
        .bind(result.intent().tenant_id().as_str())
        .bind(*result.intent().run_id().as_uuid())
        .bind(&ids)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("pending model binding intents", source))?;
    let revision_rows =
        query_as::<_, ModelInvocationRevisionRow>(SELECT_MODEL_INVOCATION_REVISIONS_BY_HEADS)
            .bind(result.intent().tenant_id().as_str())
            .bind(*result.intent().run_id().as_uuid())
            .bind(&ids)
            .bind(&revisions)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|source| StoreError::database("pending model binding revisions", source))?;
    let mut intents = unique_model_invocation_rows(intent_rows)?;
    let mut records = unique_model_invocation_revision_rows(revision_rows)?;
    if intents.len() != bindings.len() || records.len() != bindings.len() {
        return Err(StoreError::corrupt("pending model binding records"));
    }
    for binding in bindings {
        let head = binding
            .model_head()
            .ok_or_else(|| StoreError::corrupt("pending model binding kind"))?;
        let intent_row = intents
            .remove(head.invocation_id().as_uuid())
            .ok_or_else(|| StoreError::corrupt("pending model binding intent"))?;
        let intent = decode_model_invocation_intent(&intent_row)?;
        let revision = i64::try_from(head.revision().get())
            .map_err(|_| StoreError::corrupt("pending model binding revision"))?;
        let revision_row = records
            .remove(&(*head.invocation_id().as_uuid(), revision))
            .ok_or_else(|| StoreError::corrupt("pending model binding revision"))?;
        let invocation = decode_model_invocation_revision(revision_row, &intent)?;
        validate_model_invocation_current_projection(&intent_row, &invocation)?;
        if intent.activation() != result.intent().activation() || invocation.head() != *head {
            return Err(StoreError::corrupt("pending model binding record"));
        }
        insert_pending_invocation_anchor(anchors, invocation.journal_head(), invocation.digest())?;
    }
    if !intents.is_empty() || !records.is_empty() {
        return Err(StoreError::corrupt("pending model binding records"));
    }
    Ok(())
}

fn unique_tool_invocation_rows(
    rows: Vec<ToolInvocationRow>,
) -> Result<BTreeMap<Uuid, ToolInvocationRow>, StoreError> {
    let mut values = BTreeMap::new();
    for row in rows {
        let id = row.invocation_id;
        if values.insert(id, row).is_some() {
            return Err(StoreError::corrupt("pending tool binding intents"));
        }
    }
    Ok(values)
}

fn unique_tool_invocation_revision_rows(
    rows: Vec<ToolInvocationRevisionRow>,
) -> Result<BTreeMap<(Uuid, i64), ToolInvocationRevisionRow>, StoreError> {
    let mut values = BTreeMap::new();
    for row in rows {
        let key = (row.invocation_id, row.revision);
        if values.insert(key, row).is_some() {
            return Err(StoreError::corrupt("pending tool binding revisions"));
        }
    }
    Ok(values)
}

fn unique_model_invocation_rows(
    rows: Vec<ModelInvocationRow>,
) -> Result<BTreeMap<Uuid, ModelInvocationRow>, StoreError> {
    let mut values = BTreeMap::new();
    for row in rows {
        let id = row.invocation_id;
        if values.insert(id, row).is_some() {
            return Err(StoreError::corrupt("pending model binding intents"));
        }
    }
    Ok(values)
}

fn unique_model_invocation_revision_rows(
    rows: Vec<ModelInvocationRevisionRow>,
) -> Result<BTreeMap<(Uuid, i64), ModelInvocationRevisionRow>, StoreError> {
    let mut values = BTreeMap::new();
    for row in rows {
        let key = (row.invocation_id, row.revision);
        if values.insert(key, row).is_some() {
            return Err(StoreError::corrupt("pending model binding revisions"));
        }
    }
    Ok(values)
}

fn insert_pending_invocation_anchor(
    anchors: &mut BTreeMap<JournalSequence, (JournalHead, Digest)>,
    head: &JournalHead,
    digest: Digest,
) -> Result<(), StoreError> {
    if anchors
        .insert(head.sequence(), (head.clone(), digest))
        .is_some()
    {
        return Err(StoreError::corrupt(
            "pending node result invocation journal identity",
        ));
    }
    Ok(())
}

async fn verify_pending_invocation_anchors(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
    expected: BTreeMap<JournalSequence, (JournalHead, Digest)>,
) -> Result<(), StoreError> {
    if expected.is_empty() {
        return Ok(());
    }
    let expected = expected.into_iter().collect::<Vec<_>>();
    for expected in expected.chunks(PENDING_INVOCATION_ANCHOR_BATCH_SIZE) {
        let sequences = expected
            .iter()
            .map(|(sequence, _)| {
                i64::try_from(sequence.get())
                    .map_err(|_| StoreError::corrupt("pending invocation journal sequence"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rows = query_as::<_, EventRow>(SELECT_EVENTS_BY_SEQUENCES)
            .bind(result.intent().tenant_id().as_str())
            .bind(*result.intent().run_id().as_uuid())
            .bind(&sequences)
            .fetch_all(&mut **transaction)
            .await
            .map_err(|source| StoreError::database("pending invocation journal anchors", source))?;
        if rows.len() != expected.len() {
            return Err(StoreError::corrupt("pending invocation journal anchors"));
        }
        let mut actual = BTreeMap::new();
        for row in rows {
            let projection = row
                .projection_digest
                .as_deref()
                .map(|bytes| decode_digest(bytes, "pending invocation projection digest"))
                .transpose()?;
            let event = decode_event(row)?;
            if actual
                .insert(event.sequence(), (event, projection))
                .is_some()
            {
                return Err(StoreError::corrupt("pending invocation journal anchors"));
            }
        }
        for (sequence, (head, digest)) in expected {
            let (event, projection) = actual
                .remove(sequence)
                .ok_or_else(|| StoreError::corrupt("pending invocation journal anchor"))?;
            if event.head() != *head || projection != Some(*digest) {
                return Err(StoreError::corrupt("pending invocation journal anchor"));
            }
        }
        if !actual.is_empty() {
            return Err(StoreError::corrupt("pending invocation journal anchors"));
        }
    }
    Ok(())
}

fn projection_digest(projection: &RunProjection) -> Result<Digest, StoreError> {
    let wire = match projection {
        RunProjection::Unchanged => ProjectionDigestWire::Unchanged,
        RunProjection::Transition {
            expected_revision,
            transition,
        } => ProjectionDigestWire::Transition {
            expected_revision,
            transition,
        },
    };
    let canonical = serde_json_canonicalizer::to_vec(&wire)
        .map_err(|_| StoreError::encoding("run projection intent"))?;
    let mut preimage = Vec::with_capacity(PROJECTION_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(PROJECTION_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn agent_admission_projection_digest(
    admission: &AgentAdmission,
    event_intent_digest: Digest,
    checkpoint: &CheckpointWrite,
    lifecycle: &RunLifecycle,
) -> Result<Digest, StoreError> {
    let lifecycle_digest = Digest::sha256(encode_lifecycle(lifecycle)?);
    let canonical = serde_json_canonicalizer::to_vec(&AgentAdmissionProjectionDigestWire {
        admission: admission.digest(),
        event_intent: event_intent_digest,
        checkpoint_intent: checkpoint.intent_digest(),
        lifecycle: lifecycle_digest,
    })
    .map_err(|_| StoreError::encoding("agent admission projection intent"))?;
    let mut preimage =
        Vec::with_capacity(AGENT_ADMISSION_PROJECTION_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(AGENT_ADMISSION_PROJECTION_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn barrier_projection_digest(
    projection: &RunProjection,
    barrier_intent_digest: Digest,
) -> Result<Digest, StoreError> {
    let wire = BarrierProjectionDigestWire {
        run_projection_digest: projection_digest(projection)?,
        barrier_intent_digest,
    };
    let canonical = serde_json_canonicalizer::to_vec(&wire)
        .map_err(|_| StoreError::encoding("checkpoint barrier projection intent"))?;
    let mut preimage = Vec::with_capacity(BARRIER_PROJECTION_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(BARRIER_PROJECTION_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn wait_registration_projection_digest(
    expected_revision: RunRevision,
    checkpoint_write: &CheckpointWrite,
    intents: &[WaitRegistrationIntent],
) -> Result<Digest, StoreError> {
    let registrations = intents
        .iter()
        .map(wait_registration_projection_item)
        .collect::<Vec<_>>();
    let wire = WaitRegistrationProjectionDigestWire {
        expected_revision: &expected_revision,
        checkpoint_intent_digest: checkpoint_write.intent_digest(),
        registrations: &registrations,
    };
    let canonical = serde_json_canonicalizer::to_vec(&wire)
        .map_err(|_| StoreError::encoding("wait registration projection intent"))?;
    let mut preimage =
        Vec::with_capacity(WAIT_REGISTRATION_PROJECTION_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(WAIT_REGISTRATION_PROJECTION_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn wait_barrier_projection_digest(
    expected_revision: RunRevision,
    barrier_intent_digest: Digest,
    intents: &[WaitRegistrationIntent],
) -> Result<Digest, StoreError> {
    let registrations = intents
        .iter()
        .map(wait_registration_projection_item)
        .collect::<Vec<_>>();
    let wire = WaitBarrierProjectionDigestWire {
        expected_revision: &expected_revision,
        barrier_intent_digest,
        registrations: &registrations,
    };
    let canonical = serde_json_canonicalizer::to_vec(&wire)
        .map_err(|_| StoreError::encoding("wait barrier projection intent"))?;
    let mut preimage =
        Vec::with_capacity(WAIT_BARRIER_PROJECTION_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(WAIT_BARRIER_PROJECTION_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn wait_terminal_projection_digest(
    domain: &[u8],
    expected_revision: RunRevision,
    intent_digest: Digest,
) -> Result<Digest, StoreError> {
    let canonical = serde_json_canonicalizer::to_vec(&WaitTerminalProjectionDigestWire {
        expected_revision: &expected_revision,
        intent_digest,
    })
    .map_err(|_| StoreError::encoding("wait terminal projection intent"))?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn wait_abandonment_projection_digest(
    projection: &RunProjection,
    waits: &[DurableWait],
) -> Result<Digest, StoreError> {
    let mut registrations = waits
        .iter()
        .map(|wait| WaitAbandonmentProjectionItem {
            wait_kind: durable_wait_kind_text(wait),
            wait_id: durable_wait_identity(wait).to_string(),
            registration_digest: durable_wait_digest(wait),
        })
        .collect::<Vec<_>>();
    registrations.sort_unstable_by(|left, right| left.wait_id.cmp(&right.wait_id));
    let wire = WaitAbandonmentProjectionDigestWire {
        run_projection_digest: projection_digest(projection)?,
        registrations: &registrations,
    };
    let canonical = serde_json_canonicalizer::to_vec(&wire)
        .map_err(|_| StoreError::encoding("wait abandonment projection intent"))?;
    let mut preimage =
        Vec::with_capacity(WAIT_ABANDONMENT_PROJECTION_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(WAIT_ABANDONMENT_PROJECTION_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn wait_abandonment_transition_reason(
    transition: &RunTransition,
) -> Result<WaitAbandonmentReason, StoreError> {
    match transition {
        RunTransition::RequestCancellation { .. } => Ok(WaitAbandonmentReason::RunCancellation),
        RunTransition::Fail { .. } => Ok(WaitAbandonmentReason::RunFailure),
        _ => Err(StoreError::InvalidWaitAbandonment),
    }
}

fn wait_registration_projection_item(
    intent: &WaitRegistrationIntent,
) -> WaitRegistrationProjectionItem {
    match intent {
        WaitRegistrationIntent::Interrupt { request } => WaitRegistrationProjectionItem {
            wait_kind: "interrupt",
            wait_id: request.interrupt_id().to_string(),
            intent_digest: request.intent_digest(),
        },
        WaitRegistrationIntent::Timer { timer } => WaitRegistrationProjectionItem {
            wait_kind: "timer",
            wait_id: timer.timer_id().to_string(),
            intent_digest: timer.intent_digest(),
        },
    }
}

fn validate_wait_registration_batch(
    append: &JournalAppend,
    intents: &[WaitRegistrationIntent],
) -> Result<(), StoreError> {
    if intents.is_empty() || intents.len() > RunWaits::MAX_LEN {
        return Err(StoreError::InvalidWaitRegistrationBatch);
    }
    let tenant_id = append.intent().tenant_id();
    let run_id = append.intent().run_id();
    let event_id = append.intent().event_id();
    let mut identities = BTreeMap::new();
    for intent in intents {
        let item = wait_registration_projection_item(intent);
        if intent.tenant_id() != tenant_id
            || intent.run_id() != run_id
            || intent.registration_event_id() != event_id
            || identities
                .insert(item.wait_id, (item.wait_kind, item.intent_digest))
                .is_some()
        {
            return Err(StoreError::InvalidWaitRegistrationBatch);
        }
    }
    Ok(())
}

fn materialize_wait_registrations(
    intents: Vec<WaitRegistrationIntent>,
    event: &JournalEvent,
) -> Result<Vec<DurableWait>, StoreError> {
    intents
        .into_iter()
        .map(|intent| {
            intent
                .commit(event.head())
                .map_err(|_| StoreError::InvalidWaitRegistrationBatch)
        })
        .collect()
}

fn prepare_durable_wait_projection(
    stored: &StoredRun,
    tenant_id: &TenantId,
    run_id: RunId,
    expected_revision: RunRevision,
    transition: RunTransition,
    recorded_at: Timestamp,
) -> Result<PreparedProjection, StoreError> {
    let current = stored.lifecycle();
    if current.revision() != expected_revision {
        return Err(StoreError::StaleLifecycleRevision);
    }
    if current.provenance().tenant_id() != tenant_id || current.provenance().run_id() != run_id {
        return Err(StoreError::InvalidLifecycleTransition);
    }
    let lifecycle = current
        .clone()
        .apply(transition)
        .map_err(|_| StoreError::InvalidLifecycleTransition)?;
    if lifecycle.changed_at() > recorded_at {
        return Err(StoreError::LifecycleObservationAfterCommit);
    }
    prepared_projection(&lifecycle)
}

fn prepared_projection(lifecycle: &RunLifecycle) -> Result<PreparedProjection, StoreError> {
    let wait_projection = wait_set_projection(lifecycle)
        .map_err(|()| StoreError::encoding("run wait-set projection"))?;
    Ok(PreparedProjection {
        lifecycle_bytes: encode_lifecycle(lifecycle)?,
        revision: lifecycle.revision().to_string(),
        status: run_status_text(lifecycle.status()),
        changed_at: to_database_time(lifecycle.changed_at())?,
        wait_set_digest: wait_projection.digest,
        unresolved_wait_count: wait_projection.count,
        next_timer_due_at: wait_projection
            .next_timer_due_at
            .map(to_database_time)
            .transpose()?,
        next_interrupt_expiry_at: wait_projection
            .next_interrupt_expiry_at
            .map(to_database_time)
            .transpose()?,
    })
}

fn decode_event(row: EventRow) -> Result<JournalEvent, StoreError> {
    if let Some(bytes) = row.projection_digest.as_deref() {
        decode_digest(bytes, "journal projection digest")?;
    }
    let tenant_id = TenantId::try_from(row.tenant_id)
        .map_err(|_| StoreError::corrupt("journal event tenant"))?;
    let run_id = RunId::from_uuid(row.run_id)
        .map_err(|_| StoreError::corrupt("journal event run identity"))?;
    let sequence = positive_sequence(row.sequence)?;
    let event_id = EventId::from_uuid(row.event_id)
        .map_err(|_| StoreError::corrupt("journal event identity"))?;
    let recorded_at = from_database_time(row.recorded_at)?;
    let payload: stateknot_core::JournalPayload = serde_json::from_slice(&row.payload_bytes)
        .map_err(|_| StoreError::corrupt("journal payload value"))?;
    let canonical = payload
        .canonical_json()
        .map_err(|_| StoreError::corrupt("journal payload canonicalization"))?;
    if canonical.as_bytes() != row.payload_bytes
        || payload.kind().as_str() != row.event_kind
        || payload.schema().id().as_str() != row.schema_id
        || payload.schema().version().to_string() != row.schema_version
        || payload.schema().digest() != decode_digest(&row.schema_digest, "schema digest")?
        || payload.digest() != decode_digest(&row.payload_digest, "payload digest")?
    {
        return Err(StoreError::corrupt("journal payload projection"));
    }

    let source = match (
        row.source_kind.as_str(),
        row.worker_attempt_id,
        row.worker_epoch,
    ) {
        ("control_plane", None, None) => JournalEventSource::control_plane(),
        ("worker", Some(attempt_id), Some(epoch)) => {
            let attempt_id = AttemptId::from_uuid(attempt_id)
                .map_err(|_| StoreError::corrupt("journal worker attempt"))?;
            let epoch = u64::try_from(epoch)
                .ok()
                .and_then(|value| FencingEpoch::new(value).ok())
                .ok_or_else(|| StoreError::corrupt("journal worker epoch"))?;
            JournalEventSource::worker(RunFence::new(tenant_id.clone(), run_id, attempt_id, epoch))
        }
        _ => return Err(StoreError::corrupt("journal source shape")),
    };
    let intent = match source {
        JournalEventSource::ControlPlane => {
            JournalEventIntent::control_plane(tenant_id, run_id, event_id, payload)
        }
        JournalEventSource::Worker { fence } => {
            JournalEventIntent::worker(tenant_id, run_id, event_id, fence, payload)
        }
    }
    .map_err(|_| StoreError::corrupt("journal intent"))?;
    if intent.intent_digest() != decode_digest(&row.intent_digest, "intent digest")? {
        return Err(StoreError::corrupt("journal intent digest"));
    }

    let previous_digest = row
        .previous_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "previous event digest"))
        .transpose()?;
    let digest = decode_digest(&row.event_digest, "event digest")?;
    JournalEvent::restore(intent, sequence, recorded_at, previous_digest, digest)
        .map_err(|_| StoreError::corrupt("journal event"))
}

fn prepare_projection(
    stored: &StoredRun,
    append: &JournalAppend,
    projection: RunProjection,
    recorded_at: Timestamp,
) -> Result<Option<PreparedProjection>, StoreError> {
    let RunProjection::Transition {
        expected_revision,
        transition,
    } = projection
    else {
        return Ok(None);
    };
    let current = stored.lifecycle();
    if current.revision() != expected_revision {
        return Err(StoreError::StaleLifecycleRevision);
    }
    if current.provenance().tenant_id() != append.intent().tenant_id()
        || current.provenance().run_id() != append.intent().run_id()
    {
        return Err(StoreError::InvalidLifecycleTransition);
    }
    if matches!(
        transition.kind(),
        RunTransitionKind::Wait
            | RunTransitionKind::ResolveInterrupt
            | RunTransitionKind::FireTimer
    ) || (current.status() == RunStatus::Waiting
        && matches!(
            transition.kind(),
            RunTransitionKind::RequestCancellation | RunTransitionKind::Fail
        ))
    {
        return Err(StoreError::DurableWaitMutationRequired);
    }
    let lifecycle = current
        .clone()
        .apply(transition)
        .map_err(|_| StoreError::InvalidLifecycleTransition)?;
    if lifecycle.changed_at() > recorded_at {
        return Err(StoreError::LifecycleObservationAfterCommit);
    }

    prepared_projection(&lifecycle).map(Some)
}

fn validate_outbox_batch(
    append: &JournalAppend,
    intents: &[OutboxDeliveryIntent],
) -> Result<(), StoreError> {
    if intents.is_empty() || intents.len() > MAX_OUTBOX_DELIVERIES_PER_EVENT {
        return Err(StoreError::InvalidOutboxBatch);
    }
    let mut identities = BTreeMap::new();
    for intent in intents {
        if intent.tenant_id() != append.intent().tenant_id()
            || intent.run_id() != append.intent().run_id()
            || intent.origin_event_id() != append.intent().event_id()
            || identities
                .insert(intent.delivery_id(), intent.intent_digest())
                .is_some()
        {
            return Err(StoreError::InvalidOutboxBatch);
        }
    }
    Ok(())
}

fn materialize_outbox_deliveries(
    intents: Vec<OutboxDeliveryIntent>,
    event: &JournalEvent,
) -> Result<Vec<OutboxDelivery>, StoreError> {
    intents
        .into_iter()
        .map(|intent| {
            OutboxDelivery::commit(intent, event.head())
                .map_err(|_| StoreError::OutboxEnqueueConflict)
        })
        .collect()
}

async fn reap_outbox_terminals(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    observed_at: Timestamp,
) -> Result<(), StoreError> {
    let observed_at = to_database_time(observed_at)?;
    query(
        r"
WITH candidates AS (
    SELECT tenant_id, run_id, delivery_id
    FROM stateknot.outbox_deliveries
    WHERE tenant_id = $1
      AND status = 'delivering'
      AND attempt_count = 64
      AND last_completion_digest IS NULL
      AND current_attempt_expires_at <= $2
      AND current_attempt_expires_at < expires_at
    ORDER BY current_attempt_expires_at ASC, delivery_id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $3
)
UPDATE stateknot.outbox_deliveries AS delivery
SET status = 'dead_letter',
    next_attempt_at = NULL,
    terminal_at = delivery.current_attempt_expires_at,
    updated_at = delivery.current_attempt_expires_at
FROM candidates
WHERE delivery.tenant_id = candidates.tenant_id
  AND delivery.run_id = candidates.run_id
  AND delivery.delivery_id = candidates.delivery_id
",
    )
    .bind(tenant_id.as_str())
    .bind(observed_at)
    .bind(OUTBOX_TERMINAL_REAP_BATCH_SIZE)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("outbox attempt-limit reap", source))?;

    query(
        r"
WITH candidates AS (
    SELECT tenant_id, run_id, delivery_id
    FROM stateknot.outbox_deliveries
    WHERE tenant_id = $1
      AND status IN ('pending', 'delivering', 'retry_scheduled')
      AND expires_at <= $2
    ORDER BY expires_at ASC, delivery_id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $3
)
UPDATE stateknot.outbox_deliveries AS delivery
SET status = 'expired',
    next_attempt_at = NULL,
    terminal_at = delivery.expires_at,
    updated_at = delivery.expires_at
FROM candidates
WHERE delivery.tenant_id = candidates.tenant_id
  AND delivery.run_id = candidates.run_id
  AND delivery.delivery_id = candidates.delivery_id
",
    )
    .bind(tenant_id.as_str())
    .bind(observed_at)
    .bind(OUTBOX_TERMINAL_REAP_BATCH_SIZE)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("outbox delivery-expiry reap", source))?;
    Ok(())
}

async fn load_idempotent_outbox_claim(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    attempt_id: AttemptId,
) -> Result<Option<OutboxClaim>, StoreError> {
    let Some(start_row) =
        query_as::<_, OutboxAttemptStartRow>(SELECT_OUTBOX_ATTEMPT_BY_ID.as_str())
            .bind(tenant_id.as_str())
            .bind(*attempt_id.as_uuid())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|source| StoreError::database("outbox claim idempotency lookup", source))?
    else {
        return Ok(None);
    };
    let start = decode_outbox_attempt_start(&start_row)?;
    let delivery_row = load_outbox_delivery_row(
        transaction,
        tenant_id,
        start.delivery().run_id(),
        start.delivery().delivery_id(),
        true,
    )
    .await?
    .ok_or_else(|| StoreError::corrupt("outbox claim delivery"))?;
    let delivery = decode_outbox_delivery(&delivery_row)?;
    if delivery.head() != *start.delivery() {
        return Err(StoreError::corrupt("outbox claim start binding"));
    }
    verify_outbox_projection(transaction, &delivery_row, &delivery).await?;
    let current_observation = database_now(transaction, "idempotent outbox claim clock").await?;
    let epoch = i64::try_from(start.fence().epoch().get())
        .map_err(|_| StoreError::InvalidOutboxTransition)?;
    if delivery_row.status != "delivering"
        || delivery_row.current_attempt_id != Some(*start.fence().attempt_id().as_uuid())
        || delivery_row.current_epoch != Some(epoch)
    {
        return Err(StoreError::StaleOutboxFence);
    }
    if current_observation >= start.expires_at() {
        return Err(StoreError::OutboxAttemptExpired);
    }
    let destination =
        load_and_decode_outbox_destination(transaction, delivery.intent().destination()).await?;
    Ok(Some(OutboxClaim {
        destination,
        delivery,
        start,
    }))
}

async fn insert_outbox_delivery(
    transaction: &mut Transaction<'_, Postgres>,
    delivery: &OutboxDelivery,
    source: &JournalEventSource,
) -> Result<(), StoreError> {
    let intent = delivery.intent();
    let origin = delivery.origin();
    let destination = intent.destination();
    let bytes = encode_outbox_delivery(delivery)?;
    let sequence =
        i64::try_from(origin.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?;
    let (source_kind, worker_attempt_id, worker_epoch) = match source {
        JournalEventSource::ControlPlane => ("control_plane", None, None),
        JournalEventSource::Worker { fence } => (
            "worker",
            Some(*fence.attempt_id().as_uuid()),
            Some(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?),
        ),
    };
    let inserted = query(
        r"
INSERT INTO stateknot.outbox_deliveries (
    tenant_id,
    run_id,
    delivery_id,
    origin_sequence,
    origin_event_id,
    origin_recorded_at,
    origin_digest,
    destination_id,
    destination_snapshot_digest,
    intent_digest,
    expires_at,
    delivery_digest,
    delivery_bytes,
    status,
    attempt_count,
    current_attempt_id,
    current_epoch,
    current_attempt_started_at,
    current_attempt_expires_at,
    next_attempt_at,
    last_completion_digest,
    terminal_at,
    created_at,
    updated_at
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
    'pending', 0, NULL, NULL, NULL, NULL, $6, NULL, NULL, $6, $6
FROM stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND (
      $14 = 'control_plane'
      OR (
          $14 = 'worker'
          AND current_run.lease_attempt_id = $15
          AND current_run.fencing_epoch = $16
          AND current_run.lease_expires_at > clock_timestamp()
      )
  )
",
    )
    .bind(intent.tenant_id().as_str())
    .bind(*intent.run_id().as_uuid())
    .bind(*intent.delivery_id().as_uuid())
    .bind(sequence)
    .bind(*origin.event_id().as_uuid())
    .bind(to_database_time(origin.recorded_at())?)
    .bind(origin.digest().as_bytes())
    .bind(*destination.destination_id().as_uuid())
    .bind(destination.snapshot_digest().as_bytes())
    .bind(intent.intent_digest().as_bytes())
    .bind(to_database_time(intent.expires_at())?)
    .bind(delivery.digest().as_bytes())
    .bind(&bytes)
    .bind(source_kind)
    .bind(worker_attempt_id)
    .bind(worker_epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| {
        if has_database_constraint(&source, "outbox_deliveries_pkey") {
            StoreError::OutboxDeliveryIdConflict
        } else if has_database_constraint(&source, "outbox_deliveries_destination_fk") {
            StoreError::OutboxDestinationNotFound
        } else {
            StoreError::database("outbox delivery insert", source)
        }
    })?
    .rows_affected();
    if inserted != 1 {
        return Err(if source_kind == "worker" {
            StoreError::LeaseExpired
        } else {
            StoreError::corrupt("outbox delivery insert row count")
        });
    }
    Ok(())
}

async fn insert_outbox_attempt_claim(
    transaction: &mut Transaction<'_, Postgres>,
    delivery: &OutboxDelivery,
    start: &OutboxAttemptStart,
) -> Result<(), StoreError> {
    let origin = delivery.origin();
    let sequence =
        i64::try_from(origin.sequence().get()).map_err(|_| StoreError::InvalidOutboxTransition)?;
    let epoch = i64::try_from(start.fence().epoch().get())
        .map_err(|_| StoreError::InvalidOutboxTransition)?;
    query(
        r"
INSERT INTO stateknot.run_attempt_claims (
    tenant_id,
    run_id,
    attempt_id,
    claim_kind,
    invocation_id,
    invocation_revision,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    claimed_at,
    activation_digest,
    delivery_id,
    delivery_epoch
)
VALUES ($1, $2, $3, 'outbox_attempt', NULL, NULL, $4, $5, $6, $7, $8, NULL, $9, $10)
",
    )
    .bind(start.delivery().tenant_id().as_str())
    .bind(*start.delivery().run_id().as_uuid())
    .bind(*start.fence().attempt_id().as_uuid())
    .bind(sequence)
    .bind(*origin.event_id().as_uuid())
    .bind(to_database_time(origin.recorded_at())?)
    .bind(origin.digest().as_bytes())
    .bind(to_database_time(start.started_at())?)
    .bind(*start.delivery().delivery_id().as_uuid())
    .bind(epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| {
        if has_database_error_code(&source, "23505") {
            StoreError::OutboxAttemptIdConflict
        } else {
            StoreError::database("outbox attempt claim insert", source)
        }
    })?;
    Ok(())
}

async fn insert_outbox_attempt_start(
    transaction: &mut Transaction<'_, Postgres>,
    delivery: &OutboxDelivery,
    start: &OutboxAttemptStart,
) -> Result<(), StoreError> {
    let bytes = encode_outbox_attempt_start(start)?;
    let epoch = i64::try_from(start.fence().epoch().get())
        .map_err(|_| StoreError::InvalidOutboxTransition)?;
    query(
        r"
INSERT INTO stateknot.outbox_attempts (
    tenant_id,
    run_id,
    delivery_id,
    delivery_expires_at,
    delivery_digest,
    epoch,
    attempt_id,
    started_at,
    expires_at,
    start_digest,
    start_bytes,
    created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $8)
",
    )
    .bind(start.delivery().tenant_id().as_str())
    .bind(*start.delivery().run_id().as_uuid())
    .bind(*start.delivery().delivery_id().as_uuid())
    .bind(to_database_time(delivery.intent().expires_at())?)
    .bind(delivery.digest().as_bytes())
    .bind(epoch)
    .bind(*start.fence().attempt_id().as_uuid())
    .bind(to_database_time(start.started_at())?)
    .bind(to_database_time(start.expires_at())?)
    .bind(start.digest().as_bytes())
    .bind(&bytes)
    .execute(&mut **transaction)
    .await
    .map_err(|source| {
        if has_database_error_code(&source, "23505") {
            StoreError::OutboxAttemptIdConflict
        } else {
            StoreError::database("outbox attempt start insert", source)
        }
    })?;
    Ok(())
}

async fn update_outbox_delivery_claim(
    transaction: &mut Transaction<'_, Postgres>,
    delivery: &OutboxDelivery,
    start: &OutboxAttemptStart,
    previous_count: i64,
) -> Result<(), StoreError> {
    let epoch = i64::try_from(start.fence().epoch().get())
        .map_err(|_| StoreError::InvalidOutboxTransition)?;
    let updated = query(
        r"
UPDATE stateknot.outbox_deliveries
SET status = 'delivering',
    attempt_count = $4,
    current_attempt_id = $5,
    current_epoch = $4,
    current_attempt_started_at = $6,
    current_attempt_expires_at = $7,
    next_attempt_at = $7,
    last_completion_digest = NULL,
    terminal_at = NULL,
    updated_at = $6
WHERE tenant_id = $1
  AND run_id = $2
  AND delivery_id = $3
  AND delivery_digest = $8
  AND attempt_count = $9
  AND status IN ('pending', 'delivering', 'retry_scheduled')
  AND next_attempt_at <= $6
  AND expires_at > $6
",
    )
    .bind(start.delivery().tenant_id().as_str())
    .bind(*start.delivery().run_id().as_uuid())
    .bind(*start.delivery().delivery_id().as_uuid())
    .bind(epoch)
    .bind(*start.fence().attempt_id().as_uuid())
    .bind(to_database_time(start.started_at())?)
    .bind(to_database_time(start.expires_at())?)
    .bind(delivery.digest().as_bytes())
    .bind(previous_count)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("outbox delivery claim update", source))?
    .rows_affected();
    if updated != 1 {
        return Err(StoreError::InvalidOutboxTransition);
    }
    Ok(())
}

async fn insert_outbox_attempt_completion(
    transaction: &mut Transaction<'_, Postgres>,
    completion: &OutboxAttemptCompletion,
) -> Result<(), StoreError> {
    let start = completion.start();
    let fence = start.fence();
    let delivery = start.delivery();
    let epoch =
        i64::try_from(fence.epoch().get()).map_err(|_| StoreError::InvalidOutboxTransition)?;
    let bytes = encode_outbox_attempt_completion(completion)?;
    let (outcome_kind, retry_kind, retry_delay) = outbox_completion_projection(completion)?;
    query(
        r"
INSERT INTO stateknot.outbox_attempt_completions (
    tenant_id,
    run_id,
    delivery_id,
    epoch,
    attempt_id,
    started_at,
    attempt_expires_at,
    start_digest,
    outcome_kind,
    retry_advice_kind,
    retry_delay_millis,
    completed_at,
    completion_digest,
    completion_bytes,
    created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $12)
",
    )
    .bind(delivery.tenant_id().as_str())
    .bind(*delivery.run_id().as_uuid())
    .bind(*delivery.delivery_id().as_uuid())
    .bind(epoch)
    .bind(*fence.attempt_id().as_uuid())
    .bind(to_database_time(start.started_at())?)
    .bind(to_database_time(start.expires_at())?)
    .bind(start.digest().as_bytes())
    .bind(outcome_kind)
    .bind(retry_kind)
    .bind(retry_delay)
    .bind(to_database_time(completion.completed_at())?)
    .bind(completion.digest().as_bytes())
    .bind(&bytes)
    .execute(&mut **transaction)
    .await
    .map_err(|source| {
        if has_database_error_code(&source, "23505") {
            StoreError::OutboxCompletionConflict
        } else {
            StoreError::database("outbox attempt completion insert", source)
        }
    })?;
    Ok(())
}

async fn update_outbox_delivery_completion(
    transaction: &mut Transaction<'_, Postgres>,
    delivery: &OutboxDelivery,
    start: &OutboxAttemptStart,
    completion: &OutboxAttemptCompletion,
    attempt_count: i64,
) -> Result<(), StoreError> {
    let (status, next_attempt_at, terminal_at) = match completion.outcome() {
        OutboxAttemptOutcome::Acknowledged { .. } => (
            "acknowledged",
            None,
            Some(to_database_time(completion.completed_at())?),
        ),
        OutboxAttemptOutcome::Failed { failure } => match failure.retry_advice() {
            RetryAdvice::Never => (
                "dead_letter",
                None,
                Some(to_database_time(completion.completed_at())?),
            ),
            RetryAdvice::SafeAfter { .. }
                if usize::try_from(attempt_count).ok() == Some(MAX_OUTBOX_ATTEMPTS) =>
            {
                (
                    "dead_letter",
                    None,
                    Some(to_database_time(completion.completed_at())?),
                )
            }
            RetryAdvice::SafeAfter { delay } => {
                let duration = Duration::from_millis(
                    u64::try_from(delay.as_i64())
                        .map_err(|_| StoreError::InvalidOutboxTransition)?,
                );
                let retry_at = add_duration(completion.completed_at(), duration)?;
                ("retry_scheduled", Some(to_database_time(retry_at)?), None)
            }
            RetryAdvice::ReconcileFirst => return Err(StoreError::InvalidOutboxTransition),
        },
        _ => return Err(StoreError::InvalidOutboxTransition),
    };
    let epoch = i64::try_from(start.fence().epoch().get())
        .map_err(|_| StoreError::InvalidOutboxTransition)?;
    let updated = query(
        r"
UPDATE stateknot.outbox_deliveries
SET status = $6,
    next_attempt_at = $7,
    last_completion_digest = $8,
    terminal_at = $9,
    updated_at = $10
WHERE tenant_id = $1
  AND run_id = $2
  AND delivery_id = $3
  AND delivery_digest = $4
  AND attempt_count = $5
  AND status = 'delivering'
  AND current_attempt_id = $11
  AND current_epoch = $12
  AND current_attempt_expires_at > $10
  AND last_completion_digest IS NULL
",
    )
    .bind(start.delivery().tenant_id().as_str())
    .bind(*start.delivery().run_id().as_uuid())
    .bind(*start.delivery().delivery_id().as_uuid())
    .bind(delivery.digest().as_bytes())
    .bind(attempt_count)
    .bind(status)
    .bind(next_attempt_at)
    .bind(completion.digest().as_bytes())
    .bind(terminal_at)
    .bind(to_database_time(completion.completed_at())?)
    .bind(*start.fence().attempt_id().as_uuid())
    .bind(epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("outbox delivery completion update", source))?
    .rows_affected();
    if updated != 1 {
        return Err(StoreError::StaleOutboxFence);
    }
    Ok(())
}

async fn insert_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: &JournalEvent,
    projection_digest: Digest,
) -> Result<(), StoreError> {
    let (source_kind, worker_attempt_id, worker_epoch, worker_write) = match event.source() {
        JournalEventSource::ControlPlane => ("control_plane", None, None, false),
        JournalEventSource::Worker { fence } => (
            "worker",
            Some(*fence.attempt_id().as_uuid()),
            Some(
                i64::try_from(fence.epoch().get())
                    .map_err(|_| StoreError::encoding("journal worker epoch"))?,
            ),
            true,
        ),
    };
    let payload = event
        .payload()
        .canonical_json()
        .map_err(|_| StoreError::encoding("journal payload"))?;
    let schema = event.payload().schema();
    let sequence =
        i64::try_from(event.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?;

    let inserted = query(
        r"
INSERT INTO stateknot.run_events (
    tenant_id,
    run_id,
    sequence,
    event_id,
    recorded_at,
    source_kind,
    worker_attempt_id,
    worker_epoch,
    event_kind,
    schema_id,
    schema_version,
    schema_digest,
    payload_bytes,
    payload_digest,
    intent_digest,
    projection_digest,
    previous_digest,
    event_digest
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
FROM stateknot.runs AS lease_run
WHERE lease_run.tenant_id = $1
  AND lease_run.run_id = $2
  AND (
      $6 = 'control_plane'
      OR (
          $6 = 'worker'
          AND lease_run.lease_attempt_id = $7
          AND lease_run.fencing_epoch = $8
          AND lease_run.lease_expires_at > clock_timestamp()
      )
  )
",
    )
    .bind(event.tenant_id().as_str())
    .bind(*event.run_id().as_uuid())
    .bind(sequence)
    .bind(*event.event_id().as_uuid())
    .bind(to_database_time(event.recorded_at())?)
    .bind(source_kind)
    .bind(worker_attempt_id)
    .bind(worker_epoch)
    .bind(event.payload().kind().as_str())
    .bind(schema.id().as_str())
    .bind(schema.version().to_string())
    .bind(schema.digest().as_bytes())
    .bind(payload.as_bytes())
    .bind(event.payload_digest().as_bytes())
    .bind(event.intent_digest().as_bytes())
    .bind(projection_digest.as_bytes())
    .bind(
        event
            .previous_digest()
            .map(|digest| digest.as_bytes().to_vec()),
    )
    .bind(event.digest().as_bytes())
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("journal event insert", source))?
    .rows_affected();
    if inserted != 1 {
        if worker_write {
            return Err(StoreError::LeaseExpired);
        }
        return Err(StoreError::corrupt("journal insert row count"));
    }
    Ok(())
}

async fn insert_wait_registration(
    transaction: &mut Transaction<'_, Postgres>,
    wait: &DurableWait,
) -> Result<(), StoreError> {
    let record_bytes = encode_durable_wait(wait)?;
    let sequence = i64::try_from(wait.journal().sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let registered_at = to_database_time(wait.journal().recorded_at())?;
    let (
        wait_id,
        wait_kind,
        interrupt_kind,
        timer_kind,
        due_at,
        expires_at,
        action_digest,
        intent_digest,
        record_digest,
    ) = match wait {
        DurableWait::Interrupt { request } => (
            *request.marker().interrupt_id().as_uuid(),
            "interrupt",
            Some(interrupt_kind_text(request.marker().kind())),
            None,
            None,
            request
                .marker()
                .expires_at()
                .map(to_database_time)
                .transpose()?,
            Some(request.intent().action_digest()),
            request.intent().intent_digest(),
            request.digest(),
        ),
        DurableWait::Timer { timer } => (
            *timer.marker().timer_id().as_uuid(),
            "timer",
            None,
            Some(timer_kind_text(timer.marker().kind())),
            Some(to_database_time(timer.marker().due_at())?),
            None,
            None,
            timer.intent().intent_digest(),
            timer.digest(),
        ),
    };
    let result = query(
        r"
INSERT INTO stateknot.run_wait_registrations (
    tenant_id,
    run_id,
    wait_id,
    wait_kind,
    interrupt_kind,
    timer_kind,
    registered_at,
    due_at,
    expires_at,
    action_digest,
    registration_sequence,
    registration_event_id,
    registration_event_digest,
    intent_digest,
    record_digest,
    record_bytes,
    status,
    created_at,
    updated_at
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
    'outstanding', $7, $7
)
",
    )
    .bind(wait.tenant_id().as_str())
    .bind(*wait.run_id().as_uuid())
    .bind(wait_id)
    .bind(wait_kind)
    .bind(interrupt_kind)
    .bind(timer_kind)
    .bind(registered_at)
    .bind(due_at)
    .bind(expires_at)
    .bind(action_digest.as_ref().map(Digest::as_bytes))
    .bind(sequence)
    .bind(*wait.journal().event_id().as_uuid())
    .bind(wait.journal().digest().as_bytes())
    .bind(intent_digest.as_bytes())
    .bind(record_digest.as_bytes())
    .bind(&record_bytes)
    .execute(&mut **transaction)
    .await;
    match result {
        Ok(result) if result.rows_affected() == 1 => Ok(()),
        Ok(_) => Err(StoreError::corrupt("wait registration insert row count")),
        Err(source) if has_database_error_code(&source, "23505") => {
            Err(StoreError::WaitRegistrationCommitConflict)
        }
        Err(source) => Err(StoreError::database("wait registration insert", source)),
    }
}

async fn verify_wait_registration_set(
    transaction: &mut Transaction<'_, Postgres>,
    event: &JournalEvent,
    expected: &[DurableWait],
) -> Result<(), StoreError> {
    let sequence =
        i64::try_from(event.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?;
    let rows = query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATIONS_BY_ORIGIN.as_str())
        .bind(event.tenant_id().as_str())
        .bind(*event.run_id().as_uuid())
        .bind(sequence)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("wait registration set load", source))?;
    let mut durable = BTreeMap::new();
    for row in rows {
        let wait = decode_wait_registration(&row)?;
        if wait.journal() != &event.head()
            || durable.insert(durable_wait_identity(&wait), wait).is_some()
        {
            return Err(StoreError::corrupt("wait registration set"));
        }
    }
    if durable.len() != expected.len()
        || expected
            .iter()
            .any(|wait| durable.get(&durable_wait_identity(wait)) != Some(wait))
    {
        return Err(StoreError::WaitRegistrationCommitConflict);
    }
    Ok(())
}

async fn insert_interrupt_resolution(
    transaction: &mut Transaction<'_, Postgres>,
    request: &InterruptRequest,
    resolution: &InterruptResolution,
) -> Result<(), StoreError> {
    let bytes = encode_interrupt_resolution(resolution)?;
    let journal = resolution.journal();
    let result = query(
        r"
INSERT INTO stateknot.interrupt_resolutions (
    tenant_id,
    run_id,
    interrupt_id,
    request_digest,
    resolution_sequence,
    resolution_event_id,
    resolved_at,
    resolution_event_digest,
    intent_digest,
    resolution_digest,
    resolution_bytes,
    created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $7)
",
    )
    .bind(request.intent().tenant_id().as_str())
    .bind(*request.intent().run_id().as_uuid())
    .bind(*request.marker().interrupt_id().as_uuid())
    .bind(request.digest().as_bytes())
    .bind(
        i64::try_from(journal.sequence().get())
            .map_err(|_| StoreError::JournalSequenceExhausted)?,
    )
    .bind(*journal.event_id().as_uuid())
    .bind(to_database_time(journal.recorded_at())?)
    .bind(journal.digest().as_bytes())
    .bind(resolution.intent().intent_digest().as_bytes())
    .bind(resolution.digest().as_bytes())
    .bind(&bytes)
    .execute(&mut **transaction)
    .await;
    match result {
        Ok(result) if result.rows_affected() == 1 => Ok(()),
        Ok(_) => Err(StoreError::corrupt("interrupt resolution insert row count")),
        Err(source) if has_database_error_code(&source, "23505") => {
            Err(StoreError::InterruptResolutionCommitConflict)
        }
        Err(source) => Err(StoreError::database("interrupt resolution insert", source)),
    }
}

async fn project_interrupt_resolution(
    transaction: &mut Transaction<'_, Postgres>,
    request: &InterruptRequest,
    resolution: &InterruptResolution,
) -> Result<(), StoreError> {
    let journal = resolution.journal();
    let updated = query(
        r"
UPDATE stateknot.run_wait_registrations
SET status = 'resolved',
    terminal_sequence = $5,
    terminal_event_id = $6,
    terminal_recorded_at = $7,
    terminal_event_digest = $8,
    resolution_digest = $9,
    updated_at = $7
WHERE tenant_id = $1
  AND run_id = $2
  AND wait_id = $3
  AND wait_kind = 'interrupt'
  AND record_digest = $4
  AND status = 'outstanding'
",
    )
    .bind(request.intent().tenant_id().as_str())
    .bind(*request.intent().run_id().as_uuid())
    .bind(*request.marker().interrupt_id().as_uuid())
    .bind(request.digest().as_bytes())
    .bind(
        i64::try_from(journal.sequence().get())
            .map_err(|_| StoreError::JournalSequenceExhausted)?,
    )
    .bind(*journal.event_id().as_uuid())
    .bind(to_database_time(journal.recorded_at())?)
    .bind(journal.digest().as_bytes())
    .bind(resolution.digest().as_bytes())
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("interrupt resolution projection", source))?
    .rows_affected();
    if updated != 1 {
        return Err(StoreError::InterruptResolutionCommitConflict);
    }
    Ok(())
}

async fn insert_timer_firing(
    transaction: &mut Transaction<'_, Postgres>,
    timer: &DurableTimer,
    firing: &TimerFiring,
) -> Result<(), StoreError> {
    let bytes = encode_timer_firing(firing)?;
    let journal = firing.journal();
    let result = query(
        r"
INSERT INTO stateknot.timer_firings (
    tenant_id,
    run_id,
    timer_id,
    timer_digest,
    firing_sequence,
    firing_event_id,
    fired_at,
    firing_event_digest,
    intent_digest,
    firing_digest,
    firing_bytes,
    created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $7)
",
    )
    .bind(timer.intent().tenant_id().as_str())
    .bind(*timer.intent().run_id().as_uuid())
    .bind(*timer.marker().timer_id().as_uuid())
    .bind(timer.digest().as_bytes())
    .bind(
        i64::try_from(journal.sequence().get())
            .map_err(|_| StoreError::JournalSequenceExhausted)?,
    )
    .bind(*journal.event_id().as_uuid())
    .bind(to_database_time(journal.recorded_at())?)
    .bind(journal.digest().as_bytes())
    .bind(firing.intent().intent_digest().as_bytes())
    .bind(firing.digest().as_bytes())
    .bind(&bytes)
    .execute(&mut **transaction)
    .await;
    match result {
        Ok(result) if result.rows_affected() == 1 => Ok(()),
        Ok(_) => Err(StoreError::corrupt("timer firing insert row count")),
        Err(source) if has_database_error_code(&source, "23505") => {
            Err(StoreError::TimerFiringCommitConflict)
        }
        Err(source) => Err(StoreError::database("timer firing insert", source)),
    }
}

async fn project_timer_firing(
    transaction: &mut Transaction<'_, Postgres>,
    timer: &DurableTimer,
    firing: &TimerFiring,
) -> Result<(), StoreError> {
    let journal = firing.journal();
    let updated = query(
        r"
UPDATE stateknot.run_wait_registrations
SET status = 'fired',
    terminal_sequence = $5,
    terminal_event_id = $6,
    terminal_recorded_at = $7,
    terminal_event_digest = $8,
    firing_digest = $9,
    updated_at = $7
WHERE tenant_id = $1
  AND run_id = $2
  AND wait_id = $3
  AND wait_kind = 'timer'
  AND record_digest = $4
  AND status = 'outstanding'
",
    )
    .bind(timer.intent().tenant_id().as_str())
    .bind(*timer.intent().run_id().as_uuid())
    .bind(*timer.marker().timer_id().as_uuid())
    .bind(timer.digest().as_bytes())
    .bind(
        i64::try_from(journal.sequence().get())
            .map_err(|_| StoreError::JournalSequenceExhausted)?,
    )
    .bind(*journal.event_id().as_uuid())
    .bind(to_database_time(journal.recorded_at())?)
    .bind(journal.digest().as_bytes())
    .bind(firing.digest().as_bytes())
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("timer firing projection", source))?
    .rows_affected();
    if updated != 1 {
        return Err(StoreError::TimerFiringCommitConflict);
    }
    Ok(())
}

async fn insert_wait_abandonment(
    transaction: &mut Transaction<'_, Postgres>,
    abandonment: &WaitAbandonment,
) -> Result<(), StoreError> {
    let result = query(
        r"
INSERT INTO stateknot.wait_abandonments (
    tenant_id,
    run_id,
    wait_id,
    wait_kind,
    registration_digest,
    reason_kind,
    abandonment_sequence,
    abandonment_event_id,
    abandoned_at,
    abandonment_event_digest,
    abandonment_digest,
    created_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $9)
",
    )
    .bind(abandonment.wait().tenant_id().as_str())
    .bind(*abandonment.wait().run_id().as_uuid())
    .bind(durable_wait_identity(abandonment.wait()))
    .bind(durable_wait_kind_text(abandonment.wait()))
    .bind(durable_wait_digest(abandonment.wait()).as_bytes())
    .bind(wait_abandonment_reason_text(abandonment.reason()))
    .bind(
        i64::try_from(abandonment.journal().sequence().get())
            .map_err(|_| StoreError::JournalSequenceExhausted)?,
    )
    .bind(*abandonment.journal().event_id().as_uuid())
    .bind(to_database_time(abandonment.journal().recorded_at())?)
    .bind(abandonment.journal().digest().as_bytes())
    .bind(abandonment.digest().as_bytes())
    .execute(&mut **transaction)
    .await;
    match result {
        Ok(result) if result.rows_affected() == 1 => Ok(()),
        Ok(_) => Err(StoreError::corrupt("wait abandonment insert row count")),
        Err(source) if has_database_error_code(&source, "23505") => {
            Err(StoreError::WaitAbandonmentCommitConflict)
        }
        Err(source) => Err(StoreError::database("wait abandonment insert", source)),
    }
}

async fn project_wait_abandonment(
    transaction: &mut Transaction<'_, Postgres>,
    abandonment: &WaitAbandonment,
) -> Result<(), StoreError> {
    let updated = query(
        r"
UPDATE stateknot.run_wait_registrations
SET status = 'abandoned',
    terminal_sequence = $5,
    terminal_event_id = $6,
    terminal_recorded_at = $7,
    terminal_event_digest = $8,
    abandonment_digest = $9,
    updated_at = $7
WHERE tenant_id = $1
  AND run_id = $2
  AND wait_id = $3
  AND record_digest = $4
  AND status = 'outstanding'
",
    )
    .bind(abandonment.wait().tenant_id().as_str())
    .bind(*abandonment.wait().run_id().as_uuid())
    .bind(durable_wait_identity(abandonment.wait()))
    .bind(durable_wait_digest(abandonment.wait()).as_bytes())
    .bind(
        i64::try_from(abandonment.journal().sequence().get())
            .map_err(|_| StoreError::JournalSequenceExhausted)?,
    )
    .bind(*abandonment.journal().event_id().as_uuid())
    .bind(to_database_time(abandonment.journal().recorded_at())?)
    .bind(abandonment.journal().digest().as_bytes())
    .bind(abandonment.digest().as_bytes())
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("wait abandonment projection", source))?
    .rows_affected();
    if updated != 1 {
        return Err(StoreError::WaitAbandonmentCommitConflict);
    }
    Ok(())
}

async fn load_wait_abandonment_set(
    transaction: &mut Transaction<'_, Postgres>,
    event: &JournalEvent,
) -> Result<Vec<WaitAbandonment>, StoreError> {
    let sequence =
        i64::try_from(event.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?;
    let rows = query_as::<_, WaitAbandonmentRow>(SELECT_WAIT_ABANDONMENTS_BY_EVENT.as_str())
        .bind(event.tenant_id().as_str())
        .bind(*event.run_id().as_uuid())
        .bind(sequence)
        .fetch_all(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("wait abandonment set load", source))?;
    if rows.is_empty() || rows.len() > RunWaits::MAX_LEN {
        return Err(StoreError::WaitAbandonmentCommitConflict);
    }
    let mut abandonments = Vec::with_capacity(rows.len());
    for row in rows {
        let registration =
            query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID.as_str())
                .bind(event.tenant_id().as_str())
                .bind(*event.run_id().as_uuid())
                .bind(row.wait_id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|source| StoreError::database("abandoned registration load", source))?
                .ok_or(StoreError::WaitAbandonmentCommitConflict)?;
        let wait = decode_wait_registration(&registration)?;
        let abandonment = decode_wait_abandonment(&row, wait)?;
        if registration.status != "abandoned"
            || registration.abandonment_digest.as_deref() != Some(row.abandonment_digest.as_slice())
            || registration.terminal_sequence != Some(row.abandonment_sequence)
            || registration.terminal_event_id != Some(row.abandonment_event_id)
            || registration.terminal_recorded_at != Some(row.abandoned_at)
            || registration.terminal_event_digest.as_deref()
                != Some(row.abandonment_event_digest.as_slice())
            || abandonment.journal() != &event.head()
        {
            return Err(StoreError::corrupt("wait abandonment terminal projection"));
        }
        verify_wait_registration_event(
            transaction,
            abandonment.wait(),
            registration.registration_sequence,
        )
        .await?;
        abandonments.push(abandonment);
    }
    Ok(abandonments)
}

async fn load_wait_abandonment_by_id(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &TenantId,
    run_id: RunId,
    wait_id: Uuid,
) -> Result<WaitAbandonment, StoreError> {
    let registration = query_as::<_, WaitRegistrationRow>(SELECT_WAIT_REGISTRATION_BY_ID.as_str())
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(wait_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("abandoned registration load", source))?
        .ok_or(StoreError::WaitRegistrationNotFound)?;
    let wait = decode_wait_registration(&registration)?;
    verify_wait_registration_event(transaction, &wait, registration.registration_sequence).await?;

    let row = query_as::<_, WaitAbandonmentRow>(SELECT_WAIT_ABANDONMENT_BY_ID.as_str())
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(wait_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("wait abandonment load", source))?
        .ok_or(StoreError::WaitAbandonmentNotFound)?;
    let abandonment = decode_wait_abandonment(&row, wait)?;
    if registration.status != "abandoned"
        || registration.abandonment_digest.as_deref() != Some(row.abandonment_digest.as_slice())
        || registration.terminal_sequence != Some(row.abandonment_sequence)
        || registration.terminal_event_id != Some(row.abandonment_event_id)
        || registration.terminal_recorded_at != Some(row.abandoned_at)
        || registration.terminal_event_digest.as_deref()
            != Some(row.abandonment_event_digest.as_slice())
    {
        return Err(StoreError::corrupt("wait abandonment terminal projection"));
    }
    verify_terminal_event(transaction, abandonment.journal()).await?;
    Ok(abandonment)
}

async fn insert_tool_invocation_intent(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ToolInvocation,
    fence: &RunFence,
) -> Result<(), StoreError> {
    let intent = invocation.intent();
    let activation = intent.activation();
    let base = activation.base_checkpoint();
    let intent_bytes = encode_tool_invocation_intent(intent)?;
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::encoding("tool invocation base superstep"))?;
    let current_revision = i64::try_from(invocation.revision().get())
        .map_err(|_| StoreError::encoding("tool invocation revision"))?;
    let fence_epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let created_at = to_database_time(invocation.journal_head().recorded_at())?;
    let inserted = query(
        r"
INSERT INTO stateknot.tool_invocations (
    tenant_id,
    run_id,
    invocation_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    intent_digest,
    intent_bytes,
    current_revision,
    current_status,
    current_attempt_id,
    current_record_digest,
    created_at,
    updated_at
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $16
FROM stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND current_run.checkpoint_id = $4
  AND current_run.checkpoint_superstep = $5
  AND current_run.checkpoint_digest = $6
  AND current_run.lease_attempt_id = $17
  AND current_run.fencing_epoch = $18
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(intent.tenant_id().as_str())
    .bind(*intent.run_id().as_uuid())
    .bind(*intent.invocation_id().as_uuid())
    .bind(*base.checkpoint_id().as_uuid())
    .bind(base_superstep)
    .bind(base.digest().as_bytes())
    .bind(activation.graph_namespace().as_str())
    .bind(activation.node_id().as_str())
    .bind(activation.input_digest().as_bytes())
    .bind(intent.intent_digest().as_bytes())
    .bind(intent_bytes)
    .bind(current_revision)
    .bind(tool_invocation_status_text(invocation.status()))
    .bind(invocation.attempt_id().map(|attempt| *attempt.as_uuid()))
    .bind(invocation.digest().as_bytes())
    .bind(created_at)
    .bind(*fence.attempt_id().as_uuid())
    .bind(fence_epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("tool invocation intent insert", source))?
    .rows_affected();
    if inserted != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_invocation_attempt_claim(
    transaction: &mut Transaction<'_, Postgres>,
    claim_kind: InvocationAttemptKind,
    tenant_id: &TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    revision: i64,
    attempt_id: AttemptId,
    journal_head: &JournalHead,
    fence: &RunFence,
) -> Result<(), StoreError> {
    let journal_sequence = i64::try_from(journal_head.sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let fence_epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let claimed_at = to_database_time(journal_head.recorded_at())?;
    let result = query(
        r"
INSERT INTO stateknot.run_attempt_claims (
    tenant_id,
    run_id,
    attempt_id,
    claim_kind,
    invocation_id,
    invocation_revision,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    claimed_at
)
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $9
FROM stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND current_run.lease_attempt_id = $11
  AND current_run.fencing_epoch = $12
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*attempt_id.as_uuid())
    .bind(claim_kind.as_str())
    .bind(*invocation_id.as_uuid())
    .bind(revision)
    .bind(journal_sequence)
    .bind(*journal_head.event_id().as_uuid())
    .bind(claimed_at)
    .bind(journal_head.digest().as_bytes())
    .bind(*fence.attempt_id().as_uuid())
    .bind(fence_epoch)
    .execute(&mut **transaction)
    .await;
    let inserted = match result {
        Ok(result) => result.rows_affected(),
        Err(source)
            if has_database_constraint(&source, "run_attempt_claims_pkey")
                || has_database_constraint(
                    &source,
                    "run_attempt_claims_logical_revision_unique",
                )
                || has_database_constraint(&source, "run_attempt_claims_anchor_unique") =>
        {
            return Err(claim_kind.conflict());
        }
        Err(source) => {
            return Err(StoreError::database(
                "invocation attempt claim insert",
                source,
            ));
        }
    };
    if inserted != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

async fn insert_node_attempt_claim(
    transaction: &mut Transaction<'_, Postgres>,
    start: &NodeAttemptStart,
) -> Result<(), StoreError> {
    let journal = start.journal_head();
    let journal_sequence = i64::try_from(journal.sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let fence_epoch =
        i64::try_from(start.fence().epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let claimed_at = to_database_time(journal.recorded_at())?;
    let result = query(
        r"
INSERT INTO stateknot.run_attempt_claims (
    tenant_id,
    run_id,
    attempt_id,
    claim_kind,
    invocation_id,
    invocation_revision,
    activation_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    claimed_at
)
SELECT $1, $2, $3, 'node_attempt', NULL, NULL, $4, $5, $6, $7, $8, $7
FROM stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND current_run.lease_attempt_id = $9
  AND current_run.fencing_epoch = $10
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(start.activation().tenant_id().as_str())
    .bind(*start.activation().run_id().as_uuid())
    .bind(*start.attempt_id().as_uuid())
    .bind(start.activation_digest().as_bytes())
    .bind(journal_sequence)
    .bind(*journal.event_id().as_uuid())
    .bind(claimed_at)
    .bind(journal.digest().as_bytes())
    .bind(*start.fence().attempt_id().as_uuid())
    .bind(fence_epoch)
    .execute(&mut **transaction)
    .await;
    let inserted = match result {
        Ok(result) => result.rows_affected(),
        Err(source) if has_database_constraint(&source, "run_attempt_claims_pkey") => {
            return Err(StoreError::NodeAttemptIdConflict);
        }
        Err(source)
            if has_database_constraint(&source, "run_attempt_claims_anchor_unique")
                || has_database_constraint(&source, "run_attempt_claims_node_exact_unique") =>
        {
            return Err(StoreError::NodeAttemptCommitConflict);
        }
        Err(source) => {
            return Err(StoreError::database("node attempt claim insert", source));
        }
    };
    if inserted != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

async fn insert_node_attempt_start(
    transaction: &mut Transaction<'_, Postgres>,
    start: &NodeAttemptStart,
) -> Result<(), StoreError> {
    let activation = start.activation();
    let base = activation.base_checkpoint();
    let base_journal = base.journal_head();
    let journal = start.journal_head();
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::encoding("node attempt base superstep"))?;
    let base_sequence = i64::try_from(base_journal.sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let fence_epoch =
        i64::try_from(start.fence().epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let journal_sequence = i64::try_from(journal.sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let created_at = to_database_time(journal.recorded_at())?;
    let start_bytes = encode_node_attempt_start(start)?;
    let result = query(
        r"
INSERT INTO stateknot.node_attempts (
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    base_journal_sequence,
    base_journal_event_id,
    base_journal_recorded_at,
    base_journal_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    activation_digest,
    attempt_id,
    fence_attempt_id,
    fence_epoch,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    start_digest,
    start_bytes,
    created_at
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
    $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
FROM stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND current_run.checkpoint_id = $3
  AND current_run.checkpoint_superstep = $4
  AND current_run.checkpoint_digest = $5
  AND current_run.lease_attempt_id = $15
  AND current_run.fencing_epoch = $16
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(activation.tenant_id().as_str())
    .bind(*activation.run_id().as_uuid())
    .bind(*base.checkpoint_id().as_uuid())
    .bind(base_superstep)
    .bind(base.digest().as_bytes())
    .bind(base_sequence)
    .bind(*base_journal.event_id().as_uuid())
    .bind(to_database_time(base_journal.recorded_at())?)
    .bind(base_journal.digest().as_bytes())
    .bind(activation.graph_namespace().as_str())
    .bind(activation.node_id().as_str())
    .bind(activation.input_digest().as_bytes())
    .bind(start.activation_digest().as_bytes())
    .bind(*start.attempt_id().as_uuid())
    .bind(*start.fence().attempt_id().as_uuid())
    .bind(fence_epoch)
    .bind(journal_sequence)
    .bind(*journal.event_id().as_uuid())
    .bind(created_at)
    .bind(journal.digest().as_bytes())
    .bind(start.digest().as_bytes())
    .bind(start_bytes)
    .bind(created_at)
    .execute(&mut **transaction)
    .await;
    let inserted = match result {
        Ok(result) => result.rows_affected(),
        Err(source) if has_database_constraint(&source, "node_attempts_pkey") => {
            return Err(StoreError::NodeAttemptIdConflict);
        }
        Err(source) if has_database_constraint(&source, "node_attempts_start_anchor_unique") => {
            return Err(StoreError::NodeAttemptCommitConflict);
        }
        Err(source) => return Err(StoreError::database("node attempt start insert", source)),
    };
    if inserted != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn insert_node_attempt_completion(
    transaction: &mut Transaction<'_, Postgres>,
    durable_start: &NodeAttemptStart,
    completion: &NodeAttemptCompletion,
) -> Result<(), StoreError> {
    let start = completion.start();
    if start != &durable_start.head() {
        return Err(StoreError::InvalidNodeAttemptTransition);
    }
    let activation = start.activation();
    let base = activation.base_checkpoint();
    let start_journal = start.journal_head();
    let journal = completion.journal_head();
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::encoding("node completion base superstep"))?;
    let fence_epoch =
        i64::try_from(start.fence().epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let start_sequence = i64::try_from(start_journal.sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let journal_sequence = i64::try_from(journal.sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let created_at = to_database_time(journal.recorded_at())?;
    let completion_bytes = encode_node_attempt_completion(completion)?;
    let (result_intent_digest, result_record_digest, failure_id) = match completion.outcome() {
        NodeAttemptOutcome::Succeeded { result } => (
            Some(result.intent_digest().as_bytes().to_vec()),
            Some(result.digest().as_bytes().to_vec()),
            None,
        ),
        NodeAttemptOutcome::Failed { failure } => (None, None, Some(*failure.id().as_uuid())),
        _ => return Err(StoreError::InvalidNodeAttemptTransition),
    };
    let (retry_kind, retry_not_before) = node_attempt_retry_projection(completion)?;
    let result = query(
        r"
INSERT INTO stateknot.node_attempt_completions (
    tenant_id,
    run_id,
    attempt_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    activation_digest,
    fence_attempt_id,
    fence_epoch,
    start_journal_sequence,
    start_journal_event_id,
    start_journal_recorded_at,
    start_journal_digest,
    start_digest,
    status,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    result_intent_digest,
    result_record_digest,
    failure_id,
    retry_kind,
    retry_not_before,
    completion_digest,
    completion_bytes,
    created_at
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
    $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
    $25, $26, $27, $28, $29, $30
FROM stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND current_run.lease_attempt_id = $11
  AND current_run.fencing_epoch = $12
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(activation.tenant_id().as_str())
    .bind(*activation.run_id().as_uuid())
    .bind(*start.attempt_id().as_uuid())
    .bind(*base.checkpoint_id().as_uuid())
    .bind(base_superstep)
    .bind(base.digest().as_bytes())
    .bind(activation.graph_namespace().as_str())
    .bind(activation.node_id().as_str())
    .bind(activation.input_digest().as_bytes())
    .bind(durable_start.activation_digest().as_bytes())
    .bind(*start.fence().attempt_id().as_uuid())
    .bind(fence_epoch)
    .bind(start_sequence)
    .bind(*start_journal.event_id().as_uuid())
    .bind(to_database_time(start_journal.recorded_at())?)
    .bind(start_journal.digest().as_bytes())
    .bind(start.digest().as_bytes())
    .bind(node_attempt_status_text(completion.status())?)
    .bind(journal_sequence)
    .bind(*journal.event_id().as_uuid())
    .bind(created_at)
    .bind(journal.digest().as_bytes())
    .bind(result_intent_digest)
    .bind(result_record_digest)
    .bind(failure_id)
    .bind(retry_kind)
    .bind(retry_not_before)
    .bind(completion.digest().as_bytes())
    .bind(completion_bytes)
    .bind(created_at)
    .execute(&mut **transaction)
    .await;
    let inserted = match result {
        Ok(result) => result.rows_affected(),
        Err(source) if has_database_constraint(&source, "node_attempt_completions_pkey") => {
            return Err(StoreError::InvalidNodeAttemptTransition);
        }
        Err(source)
            if has_database_constraint(&source, "node_attempt_completions_anchor_unique") =>
        {
            return Err(StoreError::NodeAttemptCommitConflict);
        }
        Err(source) => {
            return Err(StoreError::database(
                "node attempt completion insert",
                source,
            ));
        }
    };
    if inserted != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

async fn insert_initial_tool_invocation_revision(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ToolInvocation,
    fence: &RunFence,
) -> Result<(), StoreError> {
    insert_tool_invocation_revision(transaction, invocation, None, fence).await
}

async fn insert_successor_tool_invocation_revision(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ToolInvocation,
    expected: &ToolInvocationHead,
    fence: &RunFence,
) -> Result<(), StoreError> {
    insert_tool_invocation_revision(transaction, invocation, Some(expected), fence).await
}

#[allow(clippy::too_many_lines)]
async fn insert_tool_invocation_revision(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ToolInvocation,
    expected: Option<&ToolInvocationHead>,
    fence: &RunFence,
) -> Result<(), StoreError> {
    let intent = invocation.intent();
    let record_bytes = encode_tool_invocation_record(invocation)?;
    let revision = i64::try_from(invocation.revision().get())
        .map_err(|_| StoreError::encoding("tool invocation revision"))?;
    let (previous_revision, previous_digest) =
        invocation.previous().map_or((None, None), |previous| {
            (
                i64::try_from(previous.revision().get()).ok(),
                Some(previous.digest().as_bytes().to_vec()),
            )
        });
    if invocation.previous().is_some() && previous_revision.is_none() {
        return Err(StoreError::encoding("tool invocation predecessor revision"));
    }
    let journal_sequence = i64::try_from(invocation.journal_head().sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let transition_kind = invocation
        .transition()
        .map(ToolInvocationTransition::kind)
        .map(tool_invocation_transition_kind_text);
    let started_attempt = match invocation.transition() {
        Some(ToolInvocationTransition::StartAttempt { attempt_id }) => Some(*attempt_id),
        _ => None,
    };
    let (expected_revision, expected_digest) = expected.map_or((None, None), |head| {
        (
            i64::try_from(head.revision().get()).ok(),
            Some(head.digest().as_bytes().to_vec()),
        )
    });
    if expected.is_some() && expected_revision.is_none() {
        return Err(StoreError::encoding("tool invocation expected revision"));
    }
    let fence_epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let created_at = to_database_time(invocation.journal_head().recorded_at())?;

    if let Some(attempt_id) = started_attempt {
        insert_invocation_attempt_claim(
            transaction,
            InvocationAttemptKind::Tool,
            intent.tenant_id(),
            intent.run_id(),
            intent.invocation_id(),
            revision,
            attempt_id,
            invocation.journal_head(),
            fence,
        )
        .await?;
    }

    let result = query(
        r"
INSERT INTO stateknot.tool_invocation_revisions (
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
FROM stateknot.runs AS current_run
JOIN stateknot.tool_invocations AS current_invocation
  ON current_invocation.tenant_id = current_run.tenant_id
 AND current_invocation.run_id = current_run.run_id
 AND current_invocation.invocation_id = $3
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND (
      (
          $19::bigint IS NULL
          AND current_invocation.current_revision = $4
          AND current_invocation.current_record_digest = $16
      )
      OR
      (
          $19::bigint IS NOT NULL
          AND current_invocation.current_revision = $19
          AND current_invocation.current_record_digest = $20
      )
  )
  AND current_run.lease_attempt_id = $21
  AND current_run.fencing_epoch = $22
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(intent.tenant_id().as_str())
    .bind(*intent.run_id().as_uuid())
    .bind(*intent.invocation_id().as_uuid())
    .bind(revision)
    .bind(previous_revision)
    .bind(previous_digest)
    .bind(journal_sequence)
    .bind(*invocation.journal_head().event_id().as_uuid())
    .bind(to_database_time(invocation.journal_head().recorded_at())?)
    .bind(invocation.journal_head().digest().as_bytes())
    .bind(tool_invocation_status_text(invocation.status()))
    .bind(invocation.attempt_id().map(|attempt| *attempt.as_uuid()))
    .bind(transition_kind)
    .bind(started_attempt.map(|attempt| *attempt.as_uuid()))
    .bind(
        invocation
            .transition_digest()
            .map(|digest| digest.as_bytes().to_vec()),
    )
    .bind(invocation.digest().as_bytes())
    .bind(record_bytes)
    .bind(created_at)
    .bind(expected_revision)
    .bind(expected_digest)
    .bind(*fence.attempt_id().as_uuid())
    .bind(fence_epoch)
    .execute(&mut **transaction)
    .await;
    let inserted = match result {
        Ok(result) => result.rows_affected(),
        Err(source)
            if has_database_constraint(
                &source,
                "tool_invocation_revisions_started_attempt_unique",
            ) =>
        {
            return Err(StoreError::InvalidToolInvocationTransition);
        }
        Err(source) => {
            return Err(StoreError::database(
                "tool invocation revision insert",
                source,
            ));
        }
    };
    if inserted != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

async fn update_tool_invocation_current(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ToolInvocation,
    expected: &ToolInvocationHead,
    fence: &RunFence,
) -> Result<(), StoreError> {
    let revision = i64::try_from(invocation.revision().get())
        .map_err(|_| StoreError::encoding("tool invocation revision"))?;
    let expected_revision = i64::try_from(expected.revision().get())
        .map_err(|_| StoreError::StaleToolInvocationHead)?;
    let fence_epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let updated = query(
        r"
UPDATE stateknot.tool_invocations AS current_invocation
SET current_revision = $4,
    current_status = $5,
    current_attempt_id = $6,
    current_record_digest = $7,
    updated_at = $8
FROM stateknot.runs AS current_run
WHERE current_invocation.tenant_id = $1
  AND current_invocation.run_id = $2
  AND current_invocation.invocation_id = $3
  AND current_invocation.current_revision = $9
  AND current_invocation.current_record_digest = $10
  AND current_run.tenant_id = current_invocation.tenant_id
  AND current_run.run_id = current_invocation.run_id
  AND current_run.lease_attempt_id = $11
  AND current_run.fencing_epoch = $12
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(invocation.intent().tenant_id().as_str())
    .bind(*invocation.intent().run_id().as_uuid())
    .bind(*invocation.intent().invocation_id().as_uuid())
    .bind(revision)
    .bind(tool_invocation_status_text(invocation.status()))
    .bind(invocation.attempt_id().map(|attempt| *attempt.as_uuid()))
    .bind(invocation.digest().as_bytes())
    .bind(to_database_time(invocation.journal_head().recorded_at())?)
    .bind(expected_revision)
    .bind(expected.digest().as_bytes())
    .bind(*fence.attempt_id().as_uuid())
    .bind(fence_epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("tool invocation current update", source))?
    .rows_affected();
    if updated != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

async fn insert_model_invocation_intent(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ModelInvocation,
    fence: &RunFence,
) -> Result<(), StoreError> {
    let intent = invocation.intent();
    let activation = intent.activation();
    let base = activation.base_checkpoint();
    let intent_bytes = encode_model_invocation_intent(intent)?;
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::encoding("model invocation base superstep"))?;
    let current_revision = i64::try_from(invocation.revision().get())
        .map_err(|_| StoreError::encoding("model invocation revision"))?;
    let fence_epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let created_at = to_database_time(invocation.journal_head().recorded_at())?;
    let inserted = query(
        r"
INSERT INTO stateknot.model_invocations (
    tenant_id,
    run_id,
    invocation_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    intent_digest,
    intent_bytes,
    current_revision,
    current_status,
    current_attempt_id,
    current_record_digest,
    created_at,
    updated_at
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $16
FROM stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND current_run.checkpoint_id = $4
  AND current_run.checkpoint_superstep = $5
  AND current_run.checkpoint_digest = $6
  AND current_run.lease_attempt_id = $17
  AND current_run.fencing_epoch = $18
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(intent.tenant_id().as_str())
    .bind(*intent.run_id().as_uuid())
    .bind(*intent.invocation_id().as_uuid())
    .bind(*base.checkpoint_id().as_uuid())
    .bind(base_superstep)
    .bind(base.digest().as_bytes())
    .bind(activation.graph_namespace().as_str())
    .bind(activation.node_id().as_str())
    .bind(activation.input_digest().as_bytes())
    .bind(intent.intent_digest().as_bytes())
    .bind(intent_bytes)
    .bind(current_revision)
    .bind(model_invocation_status_text(invocation.status()))
    .bind(invocation.attempt_id().map(|attempt| *attempt.as_uuid()))
    .bind(invocation.digest().as_bytes())
    .bind(created_at)
    .bind(*fence.attempt_id().as_uuid())
    .bind(fence_epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("model invocation intent insert", source))?
    .rows_affected();
    if inserted != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

async fn insert_initial_model_invocation_revision(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ModelInvocation,
    fence: &RunFence,
) -> Result<(), StoreError> {
    insert_model_invocation_revision(transaction, invocation, None, fence).await
}

async fn insert_successor_model_invocation_revision(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ModelInvocation,
    expected: &ModelInvocationHead,
    fence: &RunFence,
) -> Result<(), StoreError> {
    insert_model_invocation_revision(transaction, invocation, Some(expected), fence).await
}

#[allow(clippy::too_many_lines)]
async fn insert_model_invocation_revision(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ModelInvocation,
    expected: Option<&ModelInvocationHead>,
    fence: &RunFence,
) -> Result<(), StoreError> {
    let intent = invocation.intent();
    let record_bytes = encode_model_invocation_record(invocation)?;
    let revision = i64::try_from(invocation.revision().get())
        .map_err(|_| StoreError::encoding("model invocation revision"))?;
    let (previous_revision, previous_digest) =
        invocation.previous().map_or((None, None), |previous| {
            (
                i64::try_from(previous.revision().get()).ok(),
                Some(previous.digest().as_bytes().to_vec()),
            )
        });
    if invocation.previous().is_some() && previous_revision.is_none() {
        return Err(StoreError::encoding(
            "model invocation predecessor revision",
        ));
    }
    let journal_sequence = i64::try_from(invocation.journal_head().sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let transition_kind = invocation
        .transition()
        .map(ModelInvocationTransition::kind)
        .map(model_invocation_transition_kind_text);
    let started_attempt = match invocation.transition() {
        Some(ModelInvocationTransition::StartAttempt { attempt_id }) => Some(*attempt_id),
        _ => None,
    };
    let (expected_revision, expected_digest) = expected.map_or((None, None), |head| {
        (
            i64::try_from(head.revision().get()).ok(),
            Some(head.digest().as_bytes().to_vec()),
        )
    });
    if expected.is_some() && expected_revision.is_none() {
        return Err(StoreError::encoding("model invocation expected revision"));
    }
    let fence_epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let created_at = to_database_time(invocation.journal_head().recorded_at())?;

    if let Some(attempt_id) = started_attempt {
        insert_invocation_attempt_claim(
            transaction,
            InvocationAttemptKind::Model,
            intent.tenant_id(),
            intent.run_id(),
            intent.invocation_id(),
            revision,
            attempt_id,
            invocation.journal_head(),
            fence,
        )
        .await?;
    }

    let result = query(
        r"
INSERT INTO stateknot.model_invocation_revisions (
    tenant_id,
    run_id,
    invocation_id,
    revision,
    previous_revision,
    previous_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    status,
    attempt_id,
    transition_kind,
    started_attempt_id,
    transition_digest,
    record_digest,
    record_bytes,
    created_at
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
FROM stateknot.runs AS current_run
JOIN stateknot.model_invocations AS current_invocation
  ON current_invocation.tenant_id = current_run.tenant_id
 AND current_invocation.run_id = current_run.run_id
 AND current_invocation.invocation_id = $3
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND (
      (
          $19::bigint IS NULL
          AND current_invocation.current_revision = $4
          AND current_invocation.current_record_digest = $16
      )
      OR
      (
          $19::bigint IS NOT NULL
          AND current_invocation.current_revision = $19
          AND current_invocation.current_record_digest = $20
      )
  )
  AND current_run.lease_attempt_id = $21
  AND current_run.fencing_epoch = $22
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(intent.tenant_id().as_str())
    .bind(*intent.run_id().as_uuid())
    .bind(*intent.invocation_id().as_uuid())
    .bind(revision)
    .bind(previous_revision)
    .bind(previous_digest)
    .bind(journal_sequence)
    .bind(*invocation.journal_head().event_id().as_uuid())
    .bind(to_database_time(invocation.journal_head().recorded_at())?)
    .bind(invocation.journal_head().digest().as_bytes())
    .bind(model_invocation_status_text(invocation.status()))
    .bind(invocation.attempt_id().map(|attempt| *attempt.as_uuid()))
    .bind(transition_kind)
    .bind(started_attempt.map(|attempt| *attempt.as_uuid()))
    .bind(
        invocation
            .transition_digest()
            .map(|digest| digest.as_bytes().to_vec()),
    )
    .bind(invocation.digest().as_bytes())
    .bind(record_bytes)
    .bind(created_at)
    .bind(expected_revision)
    .bind(expected_digest)
    .bind(*fence.attempt_id().as_uuid())
    .bind(fence_epoch)
    .execute(&mut **transaction)
    .await;
    let inserted = match result {
        Ok(result) => result.rows_affected(),
        Err(source)
            if has_database_constraint(
                &source,
                "model_invocation_revisions_started_attempt_unique",
            ) =>
        {
            return Err(StoreError::InvalidModelInvocationTransition);
        }
        Err(source) => {
            return Err(StoreError::database(
                "model invocation revision insert",
                source,
            ));
        }
    };
    if inserted != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

async fn update_model_invocation_current(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &ModelInvocation,
    expected: &ModelInvocationHead,
    fence: &RunFence,
) -> Result<(), StoreError> {
    let revision = i64::try_from(invocation.revision().get())
        .map_err(|_| StoreError::encoding("model invocation revision"))?;
    let expected_revision = i64::try_from(expected.revision().get())
        .map_err(|_| StoreError::StaleModelInvocationHead)?;
    let fence_epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let updated = query(
        r"
UPDATE stateknot.model_invocations AS current_invocation
SET current_revision = $4,
    current_status = $5,
    current_attempt_id = $6,
    current_record_digest = $7,
    updated_at = $8
FROM stateknot.runs AS current_run
WHERE current_invocation.tenant_id = $1
  AND current_invocation.run_id = $2
  AND current_invocation.invocation_id = $3
  AND current_invocation.current_revision = $9
  AND current_invocation.current_record_digest = $10
  AND current_run.tenant_id = current_invocation.tenant_id
  AND current_run.run_id = current_invocation.run_id
  AND current_run.lease_attempt_id = $11
  AND current_run.fencing_epoch = $12
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(invocation.intent().tenant_id().as_str())
    .bind(*invocation.intent().run_id().as_uuid())
    .bind(*invocation.intent().invocation_id().as_uuid())
    .bind(revision)
    .bind(model_invocation_status_text(invocation.status()))
    .bind(invocation.attempt_id().map(|attempt| *attempt.as_uuid()))
    .bind(invocation.digest().as_bytes())
    .bind(to_database_time(invocation.journal_head().recorded_at())?)
    .bind(expected_revision)
    .bind(expected.digest().as_bytes())
    .bind(*fence.attempt_id().as_uuid())
    .bind(fence_epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("model invocation current update", source))?
    .rows_affected();
    if updated != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

async fn insert_pending_node_result(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
    node_attempt_id: AttemptId,
    fence: &RunFence,
) -> Result<(), StoreError> {
    let intent = result.intent();
    let activation = intent.activation();
    let base = activation.base_checkpoint();
    let base_journal = base.journal_head();
    let journal = result.journal_head();
    let result_bytes = encode_pending_node_result(result)?;
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::encoding("pending node result base superstep"))?;
    let base_journal_sequence = i64::try_from(base_journal.sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let fence_epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let journal_sequence = i64::try_from(journal.sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let created_at = to_database_time(journal.recorded_at())?;
    let inserted = query(
        r"
INSERT INTO stateknot.pending_node_results (
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    base_journal_sequence,
    base_journal_event_id,
    base_journal_recorded_at,
    base_journal_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    node_attempt_id,
    intent_digest,
    control_kind,
    fence_attempt_id,
    fence_epoch,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    record_digest,
    result_bytes,
    created_at
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
    $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
FROM stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND current_run.checkpoint_id = $3
  AND current_run.checkpoint_superstep = $4
  AND current_run.checkpoint_digest = $5
  AND current_run.lease_attempt_id = $16
  AND current_run.fencing_epoch = $17
  AND current_run.lease_expires_at > clock_timestamp()
",
    )
    .bind(intent.tenant_id().as_str())
    .bind(*intent.run_id().as_uuid())
    .bind(*base.checkpoint_id().as_uuid())
    .bind(base_superstep)
    .bind(base.digest().as_bytes())
    .bind(base_journal_sequence)
    .bind(*base_journal.event_id().as_uuid())
    .bind(to_database_time(base_journal.recorded_at())?)
    .bind(base_journal.digest().as_bytes())
    .bind(activation.graph_namespace().as_str())
    .bind(activation.node_id().as_str())
    .bind(activation.input_digest().as_bytes())
    .bind(*node_attempt_id.as_uuid())
    .bind(intent.intent_digest().as_bytes())
    .bind(pending_node_result_control_kind_text(
        intent.control().kind(),
    ))
    .bind(*fence.attempt_id().as_uuid())
    .bind(fence_epoch)
    .bind(journal_sequence)
    .bind(*journal.event_id().as_uuid())
    .bind(created_at)
    .bind(journal.digest().as_bytes())
    .bind(result.digest().as_bytes())
    .bind(result_bytes)
    .bind(created_at)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("pending node result insert", source))?
    .rows_affected();
    if inserted != 1 {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

async fn insert_pending_node_result_bindings(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
    fence: &RunFence,
) -> Result<(), StoreError> {
    insert_pending_node_result_binding_kind(
        transaction,
        result,
        fence,
        NodeInvocationBindingKind::Model,
    )
    .await?;
    insert_pending_node_result_binding_kind(
        transaction,
        result,
        fence,
        NodeInvocationBindingKind::Tool,
    )
    .await
}

struct PendingBindingValues {
    invocation_ids: Vec<Uuid>,
    revisions: Vec<i64>,
    record_digests: Vec<Vec<u8>>,
    journal_sequences: Vec<i64>,
    journal_recorded_at: Vec<DateTime<Utc>>,
    journal_digests: Vec<Vec<u8>>,
}

fn pending_binding_values(
    result: &PendingNodeResult,
    kind: NodeInvocationBindingKind,
) -> Result<PendingBindingValues, StoreError> {
    let mut values = PendingBindingValues {
        invocation_ids: Vec::new(),
        revisions: Vec::new(),
        record_digests: Vec::new(),
        journal_sequences: Vec::new(),
        journal_recorded_at: Vec::new(),
        journal_digests: Vec::new(),
    };
    for binding in result
        .intent()
        .bindings()
        .iter()
        .filter(|binding| binding.kind() == kind)
    {
        let (revision, digest) = match binding {
            NodeInvocationBinding::Model { head, .. } => (head.revision().get(), head.digest()),
            NodeInvocationBinding::Tool { head, .. } => (head.revision().get(), head.digest()),
        };
        values
            .invocation_ids
            .push(*binding.invocation_id().as_uuid());
        values.revisions.push(
            i64::try_from(revision)
                .map_err(|_| StoreError::encoding("pending node result binding revision"))?,
        );
        values.record_digests.push(digest.as_bytes().to_vec());
        values.journal_sequences.push(
            i64::try_from(binding.journal_head().sequence().get())
                .map_err(|_| StoreError::JournalSequenceExhausted)?,
        );
        values
            .journal_recorded_at
            .push(to_database_time(binding.journal_head().recorded_at())?);
        values
            .journal_digests
            .push(binding.journal_head().digest().as_bytes().to_vec());
    }
    Ok(values)
}

#[allow(clippy::too_many_lines)]
async fn insert_pending_node_result_binding_kind(
    transaction: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
    fence: &RunFence,
    kind: NodeInvocationBindingKind,
) -> Result<(), StoreError> {
    let values = pending_binding_values(result, kind)?;
    if values.invocation_ids.is_empty() {
        return Ok(());
    }
    let (table, operation) = match kind {
        NodeInvocationBindingKind::Model => (
            "stateknot.pending_node_result_model_bindings",
            "pending node result model binding insert",
        ),
        NodeInvocationBindingKind::Tool => (
            "stateknot.pending_node_result_tool_bindings",
            "pending node result tool binding insert",
        ),
    };
    let statement = format!(
        r"
INSERT INTO {table} (
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    activation_input_digest,
    result_record_digest,
    result_journal_sequence,
    result_journal_recorded_at,
    result_journal_digest,
    invocation_id,
    invocation_revision,
    invocation_record_digest,
    invocation_journal_sequence,
    invocation_journal_recorded_at,
    invocation_journal_digest
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
    binding.invocation_id,
    binding.invocation_revision,
    binding.invocation_record_digest,
    binding.invocation_journal_sequence,
    binding.invocation_journal_recorded_at,
    binding.invocation_journal_digest
FROM UNNEST(
    $13::uuid[],
    $14::bigint[],
    $15::bytea[],
    $16::bigint[],
    $17::timestamptz[],
    $18::bytea[]
) AS binding (
    invocation_id,
    invocation_revision,
    invocation_record_digest,
    invocation_journal_sequence,
    invocation_journal_recorded_at,
    invocation_journal_digest
)
CROSS JOIN stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND current_run.checkpoint_id = $3
  AND current_run.checkpoint_superstep = $4
  AND current_run.checkpoint_digest = $5
  AND current_run.lease_attempt_id = $19
  AND current_run.fencing_epoch = $20
  AND current_run.lease_expires_at > clock_timestamp()
"
    );
    let activation = result.intent().activation();
    let base = activation.base_checkpoint();
    let base_superstep = i64::try_from(base.superstep().get())
        .map_err(|_| StoreError::encoding("pending node result binding base superstep"))?;
    let result_sequence = i64::try_from(result.journal_head().sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let fence_epoch = i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?;
    let inserted = query(&statement)
        .bind(activation.tenant_id().as_str())
        .bind(*activation.run_id().as_uuid())
        .bind(*base.checkpoint_id().as_uuid())
        .bind(base_superstep)
        .bind(base.digest().as_bytes())
        .bind(activation.graph_namespace().as_str())
        .bind(activation.node_id().as_str())
        .bind(activation.input_digest().as_bytes())
        .bind(result.digest().as_bytes())
        .bind(result_sequence)
        .bind(to_database_time(result.journal_head().recorded_at())?)
        .bind(result.journal_head().digest().as_bytes())
        .bind(&values.invocation_ids)
        .bind(&values.revisions)
        .bind(&values.record_digests)
        .bind(&values.journal_sequences)
        .bind(&values.journal_recorded_at)
        .bind(&values.journal_digests)
        .bind(*fence.attempt_id().as_uuid())
        .bind(fence_epoch)
        .execute(&mut **transaction)
        .await;
    let inserted = match inserted {
        Ok(result) => result.rows_affected(),
        Err(source) if is_invalid_pending_binding_constraint(&source) => {
            return Err(StoreError::InvalidPendingNodeResultBinding);
        }
        Err(source) => return Err(StoreError::database(operation, source)),
    };
    if inserted
        != u64::try_from(values.invocation_ids.len())
            .map_err(|_| StoreError::encoding("pending node result binding count"))?
    {
        return Err(StoreError::LeaseExpired);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn insert_barrier_consumptions(
    transaction: &mut Transaction<'_, Postgres>,
    barrier: &CheckpointBarrier,
    successor: &Checkpoint,
    source: &JournalEventSource,
) -> Result<(), StoreError> {
    let base = barrier.base_checkpoint();
    let base_superstep =
        i64::try_from(base.superstep().get()).map_err(|_| StoreError::InvalidCheckpointBarrier)?;
    let successor_superstep = i64::try_from(successor.superstep().get())
        .map_err(|_| StoreError::encoding("checkpoint barrier successor superstep"))?;
    let successor_sequence = i64::try_from(successor.journal_head().sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let graph_namespaces = barrier
        .result_heads()
        .iter()
        .map(|head| head.activation().graph_namespace().as_str().to_owned())
        .collect::<Vec<_>>();
    let node_ids = barrier
        .result_heads()
        .iter()
        .map(|head| head.activation().node_id().as_str().to_owned())
        .collect::<Vec<_>>();
    let result_digests = barrier
        .result_heads()
        .iter()
        .map(|head| head.digest().as_bytes().to_vec())
        .collect::<Vec<_>>();
    let (worker_attempt_id, worker_epoch, worker_write) = match source {
        JournalEventSource::ControlPlane => (None, None, false),
        JournalEventSource::Worker { fence } => (
            Some(*fence.attempt_id().as_uuid()),
            Some(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?),
            true,
        ),
    };

    let inserted = query(
        r"
INSERT INTO stateknot.pending_node_result_consumptions (
    tenant_id,
    run_id,
    base_checkpoint_id,
    base_superstep,
    base_checkpoint_digest,
    graph_namespace,
    node_id,
    result_record_digest,
    successor_checkpoint_id,
    successor_superstep,
    successor_checkpoint_digest,
    successor_journal_sequence,
    successor_journal_event_id,
    successor_journal_recorded_at,
    successor_journal_digest,
    created_at
)
SELECT
    $1, $2, $3, $4, $5,
    expected.graph_namespace,
    expected.node_id,
    expected.result_record_digest,
    $6, $7, $8, $9, $10, $11, $12, $11
FROM UNNEST(
    $13::text[],
    $14::text[],
    $15::bytea[]
) AS expected (graph_namespace, node_id, result_record_digest)
JOIN stateknot.pending_node_results AS pending
  ON pending.tenant_id = $1
 AND pending.run_id = $2
 AND pending.base_checkpoint_id = $3
 AND pending.base_superstep = $4
 AND pending.base_checkpoint_digest = $5
 AND pending.graph_namespace = expected.graph_namespace
 AND pending.node_id = expected.node_id
 AND pending.record_digest = expected.result_record_digest
CROSS JOIN stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND current_run.checkpoint_id = $3
  AND current_run.checkpoint_superstep = $4
  AND current_run.checkpoint_digest = $5
  AND (
      $16::uuid IS NULL
      OR (
          current_run.lease_attempt_id = $16
          AND current_run.fencing_epoch = $17
          AND current_run.lease_expires_at > clock_timestamp()
      )
  )
",
    )
    .bind(base.tenant_id().as_str())
    .bind(*base.run_id().as_uuid())
    .bind(*base.checkpoint_id().as_uuid())
    .bind(base_superstep)
    .bind(base.digest().as_bytes())
    .bind(*successor.checkpoint_id().as_uuid())
    .bind(successor_superstep)
    .bind(successor.digest().as_bytes())
    .bind(successor_sequence)
    .bind(*successor.journal_head().event_id().as_uuid())
    .bind(to_database_time(successor.journal_head().recorded_at())?)
    .bind(successor.journal_head().digest().as_bytes())
    .bind(&graph_namespaces)
    .bind(&node_ids)
    .bind(&result_digests)
    .bind(worker_attempt_id)
    .bind(worker_epoch)
    .execute(&mut **transaction)
    .await;
    let inserted = match inserted {
        Ok(result) => result.rows_affected(),
        Err(source)
            if has_database_constraint(&source, "pending_node_result_consumptions_result_fk")
                || has_database_constraint(&source, "pending_node_result_consumptions_pkey") =>
        {
            return Err(StoreError::CheckpointBarrierResultConflict);
        }
        Err(source)
            if has_database_constraint(
                &source,
                "pending_node_result_consumptions_successor_fk",
            ) =>
        {
            return Err(StoreError::CheckpointBarrierCommitConflict);
        }
        Err(source) => {
            return Err(StoreError::database(
                "checkpoint barrier consumption insert",
                source,
            ));
        }
    };
    let expected_count = u64::try_from(barrier.result_heads().len())
        .map_err(|_| StoreError::InvalidCheckpointBarrier)?;
    if inserted != expected_count {
        if worker_write {
            return Err(StoreError::LeaseExpired);
        }
        return Err(StoreError::CheckpointBarrierResultConflict);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn insert_checkpoint(
    transaction: &mut Transaction<'_, Postgres>,
    checkpoint: &Checkpoint,
    source: &JournalEventSource,
) -> Result<(), StoreError> {
    let checkpoint_bytes = encode_checkpoint(checkpoint)?;
    let superstep = i64::try_from(checkpoint.superstep().get())
        .map_err(|_| StoreError::encoding("checkpoint superstep"))?;
    let journal_sequence = i64::try_from(checkpoint.journal_head().sequence().get())
        .map_err(|_| StoreError::JournalSequenceExhausted)?;
    let (parent_id, parent_superstep, parent_digest) =
        checkpoint.parent().map_or((None, None, None), |parent| {
            (
                Some(*parent.checkpoint_id().as_uuid()),
                i64::try_from(parent.superstep().get()).ok(),
                Some(parent.digest().as_bytes().to_vec()),
            )
        });
    if checkpoint.parent().is_some() && parent_superstep.is_none() {
        return Err(StoreError::encoding("checkpoint parent superstep"));
    }
    let (worker_attempt_id, worker_epoch, worker_write) = match source {
        JournalEventSource::ControlPlane => (None, None, false),
        JournalEventSource::Worker { fence } => (
            Some(*fence.attempt_id().as_uuid()),
            Some(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?),
            true,
        ),
    };
    let schema = checkpoint.graph().state_schema();

    let inserted = query(
        r"
INSERT INTO stateknot.run_checkpoints (
    tenant_id,
    run_id,
    checkpoint_id,
    superstep,
    parent_checkpoint_id,
    parent_superstep,
    parent_digest,
    journal_sequence,
    journal_event_id,
    journal_recorded_at,
    journal_digest,
    graph_definition_digest,
    state_schema_id,
    state_schema_version,
    state_schema_digest,
    state_digest,
    intent_digest,
    checkpoint_digest,
    checkpoint_bytes
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
FROM stateknot.runs AS current_run
WHERE current_run.tenant_id = $1
  AND current_run.run_id = $2
  AND (
      ($5::uuid IS NULL AND current_run.checkpoint_id IS NULL)
      OR (
          current_run.checkpoint_id = $5
          AND current_run.checkpoint_superstep = $6
          AND current_run.checkpoint_digest = $7
      )
  )
  AND (
      $20::uuid IS NULL
      OR (
          current_run.lease_attempt_id = $20
          AND current_run.fencing_epoch = $21
          AND current_run.lease_expires_at > clock_timestamp()
      )
  )
",
    )
    .bind(checkpoint.tenant_id().as_str())
    .bind(*checkpoint.run_id().as_uuid())
    .bind(*checkpoint.checkpoint_id().as_uuid())
    .bind(superstep)
    .bind(parent_id)
    .bind(parent_superstep)
    .bind(parent_digest)
    .bind(journal_sequence)
    .bind(*checkpoint.journal_head().event_id().as_uuid())
    .bind(to_database_time(checkpoint.journal_head().recorded_at())?)
    .bind(checkpoint.journal_head().digest().as_bytes())
    .bind(checkpoint.graph().definition_digest().as_bytes())
    .bind(schema.id().as_str())
    .bind(schema.version().to_string())
    .bind(schema.digest().as_bytes())
    .bind(checkpoint.state().digest().as_bytes())
    .bind(checkpoint.intent_digest().as_bytes())
    .bind(checkpoint.digest().as_bytes())
    .bind(checkpoint_bytes)
    .bind(worker_attempt_id)
    .bind(worker_epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("checkpoint insert", source))?
    .rows_affected();
    if inserted != 1 {
        if worker_write {
            return Err(StoreError::LeaseExpired);
        }
        return Err(StoreError::StaleCheckpointHead);
    }
    Ok(())
}

async fn update_checkpoint_pointer(
    transaction: &mut Transaction<'_, Postgres>,
    checkpoint: &Checkpoint,
    source: &JournalEventSource,
) -> Result<(), StoreError> {
    let superstep = i64::try_from(checkpoint.superstep().get())
        .map_err(|_| StoreError::encoding("checkpoint superstep"))?;
    let (parent_id, parent_superstep, parent_digest) =
        checkpoint.parent().map_or((None, None, None), |parent| {
            (
                Some(*parent.checkpoint_id().as_uuid()),
                i64::try_from(parent.superstep().get()).ok(),
                Some(parent.digest().as_bytes().to_vec()),
            )
        });
    if checkpoint.parent().is_some() && parent_superstep.is_none() {
        return Err(StoreError::encoding("checkpoint parent superstep"));
    }
    let (worker_attempt_id, worker_epoch, worker_write) = match source {
        JournalEventSource::ControlPlane => (None, None, false),
        JournalEventSource::Worker { fence } => (
            Some(*fence.attempt_id().as_uuid()),
            Some(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?),
            true,
        ),
    };

    let updated = query(
        r"
UPDATE stateknot.runs
SET checkpoint_id = $3,
    checkpoint_superstep = $4,
    checkpoint_digest = $5
WHERE tenant_id = $1
  AND run_id = $2
  AND (
      ($6::uuid IS NULL AND checkpoint_id IS NULL)
      OR (
          checkpoint_id = $6
          AND checkpoint_superstep = $7
          AND checkpoint_digest = $8
      )
  )
  AND (
      $9::uuid IS NULL
      OR (
          lease_attempt_id = $9
          AND fencing_epoch = $10
          AND lease_expires_at > clock_timestamp()
      )
  )
",
    )
    .bind(checkpoint.tenant_id().as_str())
    .bind(*checkpoint.run_id().as_uuid())
    .bind(*checkpoint.checkpoint_id().as_uuid())
    .bind(superstep)
    .bind(checkpoint.digest().as_bytes())
    .bind(parent_id)
    .bind(parent_superstep)
    .bind(parent_digest)
    .bind(worker_attempt_id)
    .bind(worker_epoch)
    .execute(&mut **transaction)
    .await
    .map_err(|source| StoreError::database("checkpoint pointer update", source))?
    .rows_affected();
    if updated != 1 {
        if worker_write {
            return Err(StoreError::LeaseExpired);
        }
        return Err(StoreError::StaleCheckpointHead);
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn update_run_head(
    transaction: &mut Transaction<'_, Postgres>,
    event: &JournalEvent,
    projection: Option<&PreparedProjection>,
) -> Result<(), StoreError> {
    let sequence =
        i64::try_from(event.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?;
    let recorded_at = to_database_time(event.recorded_at())?;
    let (worker_attempt_id, worker_epoch, worker_write) = match event.source() {
        JournalEventSource::ControlPlane => (None, None, false),
        JournalEventSource::Worker { fence } => (
            Some(*fence.attempt_id().as_uuid()),
            Some(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?),
            true,
        ),
    };
    let updated = if let Some(projection) = projection {
        query(
            r"
UPDATE stateknot.runs
SET journal_sequence = $3,
    journal_event_id = $4,
    journal_recorded_at = $5,
    journal_digest = $6,
    lifecycle_bytes = $7,
    lifecycle_revision = $8::numeric,
    lifecycle_status = $9,
    changed_at = $10,
    scheduler_ready_at = CASE
        WHEN $9 IN ('pending', 'active', 'cancellation_requested') THEN $5
        ELSE NULL
    END,
    scheduler_not_before = NULL,
    lease_attempt_id = CASE
        WHEN $9 IN ('pending', 'active', 'cancellation_requested')
            THEN lease_attempt_id
        ELSE NULL
    END,
    lease_acquired_at = CASE
        WHEN $9 IN ('pending', 'active', 'cancellation_requested')
            THEN lease_acquired_at
        ELSE NULL
    END,
    lease_renewed_at = CASE
        WHEN $9 IN ('pending', 'active', 'cancellation_requested')
            THEN lease_renewed_at
        ELSE NULL
    END,
    lease_expires_at = CASE
        WHEN $9 IN ('pending', 'active', 'cancellation_requested')
            THEN lease_expires_at
        ELSE NULL
    END,
    wait_set_digest = $11,
    unresolved_wait_count = $12,
    next_timer_due_at = $13,
    next_interrupt_expiry_at = $14,
    updated_at = $5
WHERE tenant_id = $1
  AND run_id = $2
  AND (
      $15::uuid IS NULL
      OR (
          lease_attempt_id = $15
          AND fencing_epoch = $16
          AND lease_expires_at > clock_timestamp()
      )
  )
",
        )
        .bind(event.tenant_id().as_str())
        .bind(*event.run_id().as_uuid())
        .bind(sequence)
        .bind(*event.event_id().as_uuid())
        .bind(recorded_at)
        .bind(event.digest().as_bytes())
        .bind(&projection.lifecycle_bytes)
        .bind(&projection.revision)
        .bind(projection.status)
        .bind(projection.changed_at)
        .bind(
            projection
                .wait_set_digest
                .as_ref()
                .map(stateknot_core::Digest::as_bytes),
        )
        .bind(projection.unresolved_wait_count)
        .bind(projection.next_timer_due_at)
        .bind(projection.next_interrupt_expiry_at)
        .bind(worker_attempt_id)
        .bind(worker_epoch)
        .execute(&mut **transaction)
        .await
    } else {
        query(
            r"
UPDATE stateknot.runs
SET journal_sequence = $3,
    journal_event_id = $4,
    journal_recorded_at = $5,
    journal_digest = $6,
    updated_at = $5
WHERE tenant_id = $1
  AND run_id = $2
  AND (
      $7::uuid IS NULL
      OR (
          lease_attempt_id = $7
          AND fencing_epoch = $8
          AND lease_expires_at > clock_timestamp()
      )
  )
",
        )
        .bind(event.tenant_id().as_str())
        .bind(*event.run_id().as_uuid())
        .bind(sequence)
        .bind(*event.event_id().as_uuid())
        .bind(recorded_at)
        .bind(event.digest().as_bytes())
        .bind(worker_attempt_id)
        .bind(worker_epoch)
        .execute(&mut **transaction)
        .await
    }
    .map_err(|source| StoreError::database("run head update", source))?
    .rows_affected();
    if updated != 1 {
        if worker_write {
            return Err(StoreError::LeaseExpired);
        }
        return Err(StoreError::corrupt("run head row count"));
    }
    Ok(())
}

fn authorize_worker<'a>(
    stored: &'a StoredRun,
    fence: &RunFence,
    observed_at: Timestamp,
) -> Result<&'a RunLease, StoreError> {
    let lease = stored.lease().ok_or(StoreError::NoActiveLease)?;
    match lease.validate_write(fence, observed_at) {
        Ok(()) => Ok(lease),
        Err(RunLeaseValidationError::Expired { .. }) => Err(StoreError::LeaseExpired),
        Err(
            RunLeaseValidationError::ObservationBeforeAcquisition { .. }
            | RunLeaseValidationError::ObservationBeforeRenewal { .. },
        ) => Err(StoreError::DatabaseClockRegression),
        Err(_) => Err(StoreError::StaleFence),
    }
}

fn validate_runnable(stored: &StoredRun) -> Result<(), StoreError> {
    if stored.is_quarantined() {
        return Err(StoreError::RunQuarantined);
    }
    if !lifecycle_is_scheduler_runnable(stored.lifecycle().status()) {
        return Err(StoreError::RunNotRunnable);
    }
    Ok(())
}

fn scheduler_available_at(stored: &StoredRun) -> Result<Timestamp, StoreError> {
    let ready_at = stored
        .scheduler_ready_at()
        .ok_or_else(|| StoreError::corrupt("run scheduler readiness shape"))?;
    let after_retry = stored
        .scheduler_not_before()
        .map_or(ready_at, |not_before| ready_at.max(not_before));
    Ok(stored
        .lease()
        .map_or(after_retry, |lease| after_retry.max(lease.expires_at())))
}

const fn lifecycle_is_scheduler_runnable(status: RunStatus) -> bool {
    matches!(
        status,
        RunStatus::Pending | RunStatus::Active | RunStatus::CancellationRequested
    )
}

fn validate_tool_invocation_transition_lifecycle(
    stored: &StoredRun,
    transition: ToolInvocationTransitionKind,
) -> Result<(), StoreError> {
    let status = stored.lifecycle().status();
    let allowed = match transition {
        ToolInvocationTransitionKind::StartAttempt => status == RunStatus::Active,
        ToolInvocationTransitionKind::RecordResult
        | ToolInvocationTransitionKind::RecordError
        | ToolInvocationTransitionKind::ReconcileResult
        | ToolInvocationTransitionKind::ReconcileError => matches!(
            status,
            RunStatus::Active | RunStatus::Waiting | RunStatus::CancellationRequested
        ),
    };
    if !allowed {
        return Err(StoreError::RunNotRunnable);
    }
    Ok(())
}

fn validate_model_invocation_transition_lifecycle(
    stored: &StoredRun,
    transition: ModelInvocationTransitionKind,
) -> Result<(), StoreError> {
    let status = stored.lifecycle().status();
    let allowed = match transition {
        ModelInvocationTransitionKind::StartAttempt => status == RunStatus::Active,
        ModelInvocationTransitionKind::RecordResponse
        | ModelInvocationTransitionKind::RecordError => matches!(
            status,
            RunStatus::Active | RunStatus::Waiting | RunStatus::CancellationRequested
        ),
    };
    if !allowed {
        return Err(StoreError::RunNotRunnable);
    }
    Ok(())
}

fn validate_node_attempt_completion_lifecycle(stored: &StoredRun) -> Result<(), StoreError> {
    if matches!(
        stored.lifecycle().status(),
        RunStatus::Active | RunStatus::Waiting | RunStatus::CancellationRequested
    ) {
        Ok(())
    } else {
        Err(StoreError::RunNotRunnable)
    }
}

fn positive_sequence(value: i64) -> Result<JournalSequence, StoreError> {
    let value = u64::try_from(value).map_err(|_| StoreError::corrupt("journal sequence"))?;
    JournalSequence::new(value).map_err(|_| StoreError::corrupt("journal sequence"))
}

fn nonnegative_superstep(value: i64) -> Result<Superstep, StoreError> {
    let value = u64::try_from(value).map_err(|_| StoreError::corrupt("checkpoint superstep"))?;
    Superstep::new(value).map_err(|_| StoreError::corrupt("checkpoint superstep"))
}

fn decode_digest(bytes: &[u8], record: &'static str) -> Result<Digest, StoreError> {
    let bytes: [u8; Digest::SHA256_LEN] =
        bytes.try_into().map_err(|_| StoreError::corrupt(record))?;
    Ok(Digest::from_sha256(bytes))
}

fn from_database_time(value: DateTime<Utc>) -> Result<Timestamp, StoreError> {
    Timestamp::from_unix_micros(value.timestamp_micros())
        .map_err(|_| StoreError::corrupt("timestamp"))
}

fn to_database_time(value: Timestamp) -> Result<DateTime<Utc>, StoreError> {
    DateTime::from_timestamp_micros(value.unix_micros())
        .ok_or_else(|| StoreError::encoding("timestamp"))
}

fn add_duration(value: Timestamp, duration: Duration) -> Result<Timestamp, StoreError> {
    let microseconds =
        i64::try_from(duration.as_micros()).map_err(|_| StoreError::encoding("lease duration"))?;
    let value = value
        .unix_micros()
        .checked_add(microseconds)
        .ok_or_else(|| StoreError::encoding("lease expiry"))?;
    Timestamp::from_unix_micros(value).map_err(|_| StoreError::encoding("lease expiry"))
}

const fn run_status_text(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Pending => "pending",
        RunStatus::Active => "active",
        RunStatus::Waiting => "waiting",
        RunStatus::CancellationRequested => "cancellation_requested",
        RunStatus::Succeeded => "succeeded",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

const fn interrupt_kind_text(kind: RunInterruptKind) -> &'static str {
    match kind {
        RunInterruptKind::Approval => "approval",
        RunInterruptKind::Input => "input",
        RunInterruptKind::Authentication => "authentication",
        RunInterruptKind::ExternalSignal => "external_signal",
        RunInterruptKind::Reconciliation => "reconciliation",
    }
}

const fn timer_kind_text(kind: RunTimerKind) -> &'static str {
    match kind {
        RunTimerKind::Sleep => "sleep",
        RunTimerKind::RetryBackoff => "retry_backoff",
    }
}

const fn tool_invocation_status_text(status: ToolInvocationStatus) -> &'static str {
    match status {
        ToolInvocationStatus::Prepared => "prepared",
        ToolInvocationStatus::Executing => "executing",
        ToolInvocationStatus::Committed => "committed",
        ToolInvocationStatus::Failed => "failed",
        ToolInvocationStatus::Unknown => "unknown",
    }
}

const fn tool_invocation_transition_kind_text(kind: ToolInvocationTransitionKind) -> &'static str {
    match kind {
        ToolInvocationTransitionKind::StartAttempt => "start_attempt",
        ToolInvocationTransitionKind::RecordResult => "record_result",
        ToolInvocationTransitionKind::RecordError => "record_error",
        ToolInvocationTransitionKind::ReconcileResult => "reconcile_result",
        ToolInvocationTransitionKind::ReconcileError => "reconcile_error",
    }
}

const fn model_invocation_status_text(status: ModelInvocationStatus) -> &'static str {
    match status {
        ModelInvocationStatus::Prepared => "prepared",
        ModelInvocationStatus::Executing => "executing",
        ModelInvocationStatus::Committed => "committed",
        ModelInvocationStatus::Failed => "failed",
    }
}

const fn model_invocation_transition_kind_text(
    kind: ModelInvocationTransitionKind,
) -> &'static str {
    match kind {
        ModelInvocationTransitionKind::StartAttempt => "start_attempt",
        ModelInvocationTransitionKind::RecordResponse => "record_response",
        ModelInvocationTransitionKind::RecordError => "record_error",
    }
}

const fn pending_node_result_control_kind_text(kind: NodeControlKind) -> &'static str {
    match kind {
        NodeControlKind::Continue => "continue",
        NodeControlKind::Route => "route",
        NodeControlKind::Wait => "wait",
        NodeControlKind::Terminal => "terminal",
    }
}

fn map_pending_node_result_commit_error(error: &PendingNodeResultError) -> StoreError {
    match error {
        PendingNodeResultError::JournalNotAfterBinding
        | PendingNodeResultError::BindingClockRegression => {
            StoreError::InvalidPendingNodeResultBinding
        }
        _ => StoreError::InvalidPendingNodeResult,
    }
}

fn map_event_commit_error(error: &JournalEventError) -> StoreError {
    match error {
        JournalEventError::SequenceOverflow => StoreError::JournalSequenceExhausted,
        _ => StoreError::encoding("journal event"),
    }
}

fn has_database_error_code(error: &sqlx_core::Error, expected: &str) -> bool {
    matches!(
        error,
        sqlx_core::Error::Database(database)
            if database.code().is_some_and(|code| code.as_ref() == expected)
    )
}

fn has_database_constraint(error: &sqlx_core::Error, expected: &str) -> bool {
    matches!(
        error,
        sqlx_core::Error::Database(database)
            if database.constraint().is_some_and(|constraint| constraint == expected)
    )
}

fn is_invalid_pending_binding_constraint(error: &sqlx_core::Error) -> bool {
    matches!(
        error,
        sqlx_core::Error::Database(database)
            if database.constraint().is_some_and(|constraint| matches!(
                constraint,
                "pending_node_result_tool_bindings_activation_fk"
                    | "pending_node_result_tool_bindings_revision_fk"
                    | "pending_node_result_tool_bindings_causal"
                    | "pending_node_result_tool_bindings_once"
                    | "pending_node_result_tool_bindings_pkey"
                    | "pending_node_result_model_bindings_activation_fk"
                    | "pending_node_result_model_bindings_revision_fk"
                    | "pending_node_result_model_bindings_causal"
                    | "pending_node_result_model_bindings_once"
                    | "pending_node_result_model_bindings_pkey"
            ))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConfigurationError, PostgresTransportSecurity};

    #[test]
    fn store_debug_and_configuration_do_not_expose_urls() {
        let options = PostgresStoreOptions::default();
        assert_eq!(
            options.transport_security(),
            PostgresTransportSecurity::VerifyFull
        );
        assert_eq!(
            options.clone().with_pool_size(2, 1).validate(),
            Err(ConfigurationError::PoolMinimumExceedsMaximum)
        );
        assert_eq!(
            options
                .clone()
                .with_transaction_timeouts(Duration::from_secs(2), Duration::from_secs(1))
                .validate(),
            Err(ConfigurationError::LockTimeoutNotBelowStatementTimeout)
        );
        assert_eq!(
            options
                .with_lease_timing(Duration::from_secs(60), Duration::from_secs(30))
                .validate(),
            Err(ConfigurationError::LeaseDurationExceedsMaximumHorizon)
        );
    }

    #[test]
    fn database_timing_configuration_is_exactly_representable() {
        let rounded = PostgresStoreOptions::default()
            .with_transaction_timeouts(Duration::from_nanos(1), Duration::from_nanos(1_000_001));
        assert_eq!(rounded.lock_timeout_setting(), "1ms");
        assert_eq!(rounded.statement_timeout_setting(), "2ms");
        assert!(rounded.validate().is_ok());

        assert_eq!(
            PostgresStoreOptions::default()
                .with_transaction_timeouts(Duration::from_nanos(1), Duration::from_nanos(2))
                .validate(),
            Err(ConfigurationError::LockTimeoutNotBelowStatementTimeout)
        );
        assert_eq!(
            PostgresStoreOptions::default()
                .with_lease_timing(Duration::from_nanos(1_500), Duration::from_secs(1))
                .validate(),
            Err(ConfigurationError::LeaseTimingNotMicrosecondAligned {
                name: "lease duration"
            })
        );
        assert_eq!(
            PostgresStoreOptions::default()
                .with_transaction_timeouts(
                    Duration::from_millis(i32::MAX as u64 + 1),
                    Duration::from_millis(i32::MAX as u64 + 2),
                )
                .validate(),
            Err(ConfigurationError::PostgresTimeoutTooLarge {
                name: "lock timeout"
            })
        );
        assert_eq!(
            PostgresStoreOptions::default()
                .with_outbox_attempt_lease(Duration::ZERO)
                .validate(),
            Err(ConfigurationError::ZeroDuration {
                name: "outbox attempt lease duration"
            })
        );
        assert_eq!(
            PostgresStoreOptions::default()
                .with_outbox_attempt_lease(Duration::from_nanos(1_500))
                .validate(),
            Err(ConfigurationError::LeaseTimingNotMicrosecondAligned {
                name: "outbox attempt lease duration"
            })
        );
        assert!(
            PostgresStoreOptions::default()
                .with_outbox_attempt_lease(Duration::from_secs(5 * 60))
                .validate()
                .is_ok()
        );
        assert_eq!(
            PostgresStoreOptions::default()
                .with_outbox_attempt_lease(Duration::from_micros(300_000_001))
                .validate(),
            Err(ConfigurationError::OutboxAttemptLeaseTooLong)
        );
    }

    #[test]
    fn page_size_is_strictly_bounded() {
        assert!(JournalPageSize::new(1).is_ok());
        assert!(JournalPageSize::new(JournalPageSize::MAX).is_ok());
        assert!(JournalPageSize::new(0).is_err());
        assert!(JournalPageSize::new(JournalPageSize::MAX + 1).is_err());

        assert!(CheckpointLineagePageSize::new(1).is_ok());
        assert!(CheckpointLineagePageSize::new(CheckpointLineagePageSize::MAX).is_ok());
        assert!(CheckpointLineagePageSize::new(0).is_err());
        assert!(CheckpointLineagePageSize::new(CheckpointLineagePageSize::MAX + 1).is_err());

        assert!(ToolInvocationHistoryPageSize::new(1).is_ok());
        assert!(ToolInvocationHistoryPageSize::new(ToolInvocationHistoryPageSize::MAX).is_ok());
        assert!(ToolInvocationHistoryPageSize::new(0).is_err());
        assert!(
            ToolInvocationHistoryPageSize::new(ToolInvocationHistoryPageSize::MAX + 1).is_err()
        );

        assert!(ModelInvocationHistoryPageSize::new(1).is_ok());
        assert!(ModelInvocationHistoryPageSize::new(0).is_err());
        assert!(ModelInvocationHistoryPageSize::new(2).is_err());

        assert!(PendingNodeResultPageSize::new(1).is_ok());
        assert!(PendingNodeResultPageSize::new(PendingNodeResultPageSize::MAX).is_ok());
        assert!(PendingNodeResultPageSize::new(0).is_err());
        assert!(PendingNodeResultPageSize::new(PendingNodeResultPageSize::MAX + 1).is_err());

        assert!(RunnableRunPageSize::new(1).is_ok());
        assert!(RunnableRunPageSize::new(RunnableRunPageSize::MAX).is_ok());
        assert!(RunnableRunPageSize::new(0).is_err());
        assert!(RunnableRunPageSize::new(RunnableRunPageSize::MAX + 1).is_err());

        assert!(OutboxAttemptHistoryPageSize::new(1).is_ok());
        assert!(OutboxAttemptHistoryPageSize::new(OutboxAttemptHistoryPageSize::MAX).is_ok());
        assert!(OutboxAttemptHistoryPageSize::new(0).is_err());
        assert!(OutboxAttemptHistoryPageSize::new(OutboxAttemptHistoryPageSize::MAX + 1).is_err());
    }

    #[test]
    fn retry_classification_is_conservative() {
        assert!(StoreError::database("test", sqlx_core::Error::PoolTimedOut).is_retryable());
        assert!(!StoreError::RunNotFound.is_retryable());
    }

    #[test]
    fn projection_intent_digests_are_versioned_and_frozen() {
        let unchanged = projection_digest(&RunProjection::unchanged()).unwrap();
        assert_eq!(
            unchanged.to_string(),
            "sha256:c1bee6a7d79cfc7b48f7dc9bbf625ccd3f1bf87bfdb2d624fe40517ad19534d8"
        );

        let transition = RunProjection::transition(
            RunRevision::ZERO,
            RunTransition::Start {
                started_at: "2030-01-01T00:00:01.000000Z".parse().unwrap(),
            },
        );
        assert_eq!(
            projection_digest(&transition).unwrap().to_string(),
            "sha256:9f2dfecd7af4cee03f6cc1b9f95ac3e3f191303def4b037ea1c6ac29df0e225b"
        );

        let barrier_intent =
            "sha256:3b454071dfa5f18b7dd7ac7084236afdf88e5f73fc3aa4382d32f82ca974112b"
                .parse()
                .unwrap();
        assert_eq!(
            barrier_projection_digest(&RunProjection::unchanged(), barrier_intent)
                .unwrap()
                .to_string(),
            "sha256:dafe1bdef91699c3db6ce24b1272af5bb6da7fbd7055d25bc0cd2223bb29b195"
        );
    }
}
