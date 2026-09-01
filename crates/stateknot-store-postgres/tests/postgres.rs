// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real `PostgreSQL` migration, transaction, idempotency, and fencing tests.

use std::{
    borrow::Cow,
    collections::BTreeSet,
    sync::{Arc, LazyLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use sqlx_core::{
    migrate::{Migration, MigrationType, Migrator},
    query::query,
    query_as::query_as,
    query_scalar::query_scalar,
};
use sqlx_postgres::{PgPool, PgPoolOptions};
use stateknot_core::{
    AgentAdmission, AgentAdmissionAuthority, AgentAdmissionBudgetLayer, AgentAdmissionIntent,
    AgentDescriptor, AgentRequest, AgentResultProvenance, AgentSubmissionKey, AttemptId,
    BoundedJson, BudgetLimits, BudgetUsage, CapabilityIdentity, CapabilityName,
    CapabilityReference, Checkpoint, CheckpointBarrier, CheckpointHead, CheckpointId,
    CheckpointState, CheckpointWrite, CompiledGraph, DeliveryId, DestinationId, Digest,
    DurationMillis, EventId, Failure, FailureCategory, FailureCode, FailureId, FailureMessage,
    FailureOrigin, GraphExecutionLimits, GraphNamespace, GraphNode, GraphReducer,
    GraphReducerError, GraphReducerInput, GraphReducerReference, GraphReference, GraphRoutes,
    GraphSchemaValidationError, GraphSchemaValidator, InterruptId, InterruptRequestIntent,
    InterruptResolutionIntent, InterruptResolver, InvocationId, IssuerId, JournalAppend,
    JournalEventIntent, JournalEventKind, JournalExpectation, JournalHead, JournalPayload,
    JournalSequence, ModelDescriptor, ModelError, ModelErrorPhase, ModelErrorProvenance,
    ModelInvocationIntent, ModelInvocationStatus, ModelInvocationTransition, ModelRequest,
    ModelResponse, NodeActivation, NodeAttemptStatus, NodeControl, NodeDispatchReason, NodeId,
    NodeInvocationBinding, NodeInvocationBindings, NodeStateChange, NodeStateUpdate,
    OutboxDeliveryIntent, OutboxDestinationRef, PendingNodeResultHead, PendingNodeResultIntent,
    PrincipalIdentity, QuarantineId, ReadyNodeRecoveryPlanner, ReadyNodes, RecoveryNodeKind,
    RetryAdvice, RunCancellationRequest, RunFailure, RunId, RunInterruptKind, RunStatus,
    RunTimerKind, RunTransition, SchedulerReservationId, SchedulerShardId, SchemaId,
    SchemaReference, Scope, ScopeSet, SubjectId, Superstep, TenantId, ThreadId, TimerFiringIntent,
    TimerId, TimerRegistrationIntent, Timestamp, ToolArtifacts, ToolDescriptor, ToolInput,
    ToolInvocation, ToolInvocationIntent, ToolInvocationStatus, ToolInvocationTransition,
    ToolResult, ToolResultProvenance, Version, WaitRegistrationIntent,
};
use stateknot_store_postgres::{
    AdmissionOutcome, AgentAdmissionCommitOutcome, AgentSubmissionCommitOutcome, AppendOutcome,
    BarrierCommitOutcome, CheckpointCommitOutcome, CheckpointLineagePageSize,
    CorruptionQuarantineContext, DelayedRetryScheduleOutcome, GraphDefinitionRegistrationOutcome,
    GraphReplayLimits, InterruptResolutionCommitOutcome, JournalPageSize, LeaseClaimOutcome,
    LeaseReleaseOutcome, LeaseRenewalOutcome, ModelInvocationCommitOutcome,
    ModelInvocationHistoryPageSize, NodeAttemptCommitOutcome, NodeAttemptHistoryPageSize,
    OutboxAttemptHistoryPageSize, OutboxClaimOutcome, OutboxCompletionOutcome,
    OutboxDestinationRegistrationOutcome, OutboxEnqueueOutcome, PendingNodeResultCommitOutcome,
    PendingNodeResultPageSize, PostgresStore, PostgresStoreOptions, PostgresTransportSecurity,
    RunProjection, RunQuarantineCause, RunQuarantineCommitOutcome, RunQuarantineComponent,
    RunQuarantineRequest, RunnableRunPageSize, SchedulerFairnessPolicyRegistration,
    SchedulerFairnessPolicyRegistrationOutcome, SchedulerFairnessRetentionPolicy, StoreError,
    StoredAgentAdmission, TimerFiringCommitOutcome, ToolInvocationCommitOutcome,
    ToolInvocationHistoryPageSize, WaitAbandonmentCommitOutcome, WaitAbandonmentReason,
    WaitCheckpointCommitOutcome, WaitDiscoveryPageSize,
};
use uuid::Uuid;

const DATABASE_URL_ENV: &str = "STATEKNOT_TEST_DATABASE_URL";
const REQUIRE_DATABASE_ENV: &str = "STATEKNOT_REQUIRE_POSTGRES_TESTS";
static DATABASE_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn test_store() -> Option<PostgresStore> {
    test_store_with_lease_duration(Duration::from_secs(30)).await
}

async fn test_store_with_lease_duration(lease_duration: Duration) -> Option<PostgresStore> {
    test_store_with_options(test_options(lease_duration)).await
}

async fn test_store_with_outbox_attempt_lease(lease_duration: Duration) -> Option<PostgresStore> {
    test_store_with_options(
        test_options(Duration::from_secs(30)).with_outbox_attempt_lease(lease_duration),
    )
    .await
}

async fn test_store_with_options(options: PostgresStoreOptions) -> Option<PostgresStore> {
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return None,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    PostgresStore::migrate_database(&database_url, options.clone())
        .await
        .expect("migrations must succeed");
    let store = PostgresStore::connect(&database_url, options)
        .await
        .expect("test PostgreSQL must connect with an exact schema");
    Some(store)
}

fn test_options(lease_duration: Duration) -> PostgresStoreOptions {
    PostgresStoreOptions::default()
        .with_transport_security(PostgresTransportSecurity::Disabled)
        .with_pool_size(1, 48)
        .with_transaction_timeouts(Duration::from_secs(5), Duration::from_secs(20))
        .with_lease_timing(lease_duration, Duration::from_secs(5 * 60))
}

async fn remove_transactional_outbox(pool: &PgPool) {
    remove_durable_waits(pool).await;
    query(
        "ALTER TABLE stateknot.outbox_deliveries \
         DROP CONSTRAINT outbox_deliveries_current_attempt_fk, \
         DROP CONSTRAINT outbox_deliveries_last_completion_fk",
    )
    .execute(pool)
    .await
    .expect("v8 delivery back-references must be removed from the fixture");
    for table in [
        "stateknot.outbox_attempt_completions",
        "stateknot.outbox_attempts",
        "stateknot.outbox_deliveries",
        "stateknot.outbox_destinations",
    ] {
        query(&format!("DROP TABLE {table}"))
            .execute(pool)
            .await
            .expect("v8 outbox table must be removed from the fixture");
    }
    query(
        "ALTER TABLE stateknot.run_attempt_claims \
         DROP CONSTRAINT run_attempt_claims_outbox_exact_unique, \
         DROP CONSTRAINT run_attempt_claims_ids_are_uuid_v7, \
         DROP CONSTRAINT run_attempt_claims_kind_valid, \
         DROP CONSTRAINT run_attempt_claims_owner_shape, \
         DROP CONSTRAINT run_attempt_claims_clock_valid",
    )
    .execute(pool)
    .await
    .expect("v8 attempt-registry constraints must be removed from the fixture");
    query("DROP INDEX stateknot.run_attempt_claims_non_outbox_anchor_unique")
        .execute(pool)
        .await
        .expect("v8 partial attempt-anchor index must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.run_attempt_claims \
         DROP COLUMN delivery_id, \
         DROP COLUMN delivery_epoch, \
         ADD CONSTRAINT run_attempt_claims_anchor_unique UNIQUE ( \
             tenant_id, run_id, journal_sequence \
         ), \
         ADD CONSTRAINT run_attempt_claims_ids_are_uuid_v7 CHECK ( \
             stateknot.is_uuid_v7(run_id) \
             AND stateknot.is_uuid_v7(attempt_id) \
             AND (invocation_id IS NULL OR stateknot.is_uuid_v7(invocation_id)) \
             AND stateknot.is_uuid_v7(journal_event_id) \
         ), \
         ADD CONSTRAINT run_attempt_claims_kind_valid CHECK ( \
             claim_kind IN ('tool_invocation', 'model_invocation', 'node_attempt') \
         ), \
         ADD CONSTRAINT run_attempt_claims_owner_shape CHECK ( \
             ( \
                 claim_kind IN ('tool_invocation', 'model_invocation') \
                 AND invocation_id IS NOT NULL \
                 AND invocation_revision > 0 \
                 AND activation_digest IS NULL \
             ) \
             OR ( \
                 claim_kind = 'node_attempt' \
                 AND invocation_id IS NULL \
                 AND invocation_revision IS NULL \
                 AND octet_length(activation_digest) = 32 \
             ) \
         ), \
         ADD CONSTRAINT run_attempt_claims_clock_valid CHECK ( \
             claimed_at = journal_recorded_at \
         )",
    )
    .execute(pool)
    .await
    .expect("attempt registry must be restored to its exact v7 shape");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 8")
        .execute(pool)
        .await
        .expect("v8 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

async fn remove_durable_waits(pool: &PgPool) {
    remove_run_quarantines(pool).await;
    query(
        "ALTER TABLE stateknot.run_wait_registrations \
         DROP CONSTRAINT run_wait_registrations_resolution_fk, \
         DROP CONSTRAINT run_wait_registrations_firing_fk, \
         DROP CONSTRAINT run_wait_registrations_abandonment_fk",
    )
    .execute(pool)
    .await
    .expect("v9 wait terminal back-references must be removed from the fixture");
    for table in [
        "stateknot.interrupt_resolutions",
        "stateknot.timer_firings",
        "stateknot.wait_abandonments",
        "stateknot.run_wait_registrations",
    ] {
        query(&format!("DROP TABLE {table}"))
            .execute(pool)
            .await
            .expect("v9 durable wait table must be removed from the fixture");
    }
    query(
        "ALTER TABLE stateknot.runs \
         DROP CONSTRAINT runs_wait_projection_shape, \
         DROP COLUMN wait_set_digest, \
         DROP COLUMN unresolved_wait_count, \
         DROP COLUMN next_timer_due_at, \
         DROP COLUMN next_interrupt_expiry_at",
    )
    .execute(pool)
    .await
    .expect("v9 run wait projection must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 9")
        .execute(pool)
        .await
        .expect("v9 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

async fn remove_run_quarantines(pool: &PgPool) {
    remove_fenced_recovery_quarantines(pool).await;
    query("DROP TABLE stateknot.run_quarantines")
        .execute(pool)
        .await
        .expect("v10 run-quarantine table must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 10")
        .execute(pool)
        .await
        .expect("v10 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

async fn remove_fenced_recovery_quarantines(pool: &PgPool) {
    remove_delayed_retry_wakeup(pool).await;
    query(
        "ALTER TABLE stateknot.run_quarantines \
         DROP CONSTRAINT run_quarantines_fence_shape, \
         DROP COLUMN expected_fence_attempt_id, \
         DROP COLUMN expected_fence_epoch",
    )
    .execute(pool)
    .await
    .expect("v11 fenced quarantine columns must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 11")
        .execute(pool)
        .await
        .expect("v11 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

async fn remove_delayed_retry_wakeup(pool: &PgPool) {
    remove_graph_registry(pool).await;
    query("DROP INDEX stateknot.runs_scheduler_ready")
        .execute(pool)
        .await
        .expect("v12 scheduler index must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.runs \
         DROP CONSTRAINT runs_scheduler_not_before_shape, \
         DROP COLUMN scheduler_not_before",
    )
    .execute(pool)
    .await
    .expect("v12 delayed retry projection must be removed from the fixture");
    query(
        "CREATE INDEX runs_scheduler_ready \
         ON stateknot.runs ( \
             tenant_id, \
             (GREATEST( \
                 scheduler_ready_at, \
                 COALESCE(lease_expires_at, scheduler_ready_at) \
             )), \
             run_id \
         ) \
         WHERE quarantined_at IS NULL \
           AND scheduler_ready_at IS NOT NULL \
           AND lifecycle_status IN ('pending', 'active', 'cancellation_requested')",
    )
    .execute(pool)
    .await
    .expect("the exact v7 scheduler index must be restored");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 12")
        .execute(pool)
        .await
        .expect("v12 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

async fn remove_graph_registry(pool: &PgPool) {
    remove_scheduler_fairness(pool).await;
    query("DROP TABLE stateknot.graph_definitions")
        .execute(pool)
        .await
        .expect("v13 graph registry table must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 13")
        .execute(pool)
        .await
        .expect("v13 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

async fn remove_scheduler_fairness(pool: &PgPool) {
    remove_agent_admissions(pool).await;
    query("DROP TABLE stateknot.scheduler_fairness_reservations")
        .execute(pool)
        .await
        .expect("v14 scheduler reservation table must be removed from the fixture");
    query("DROP TABLE stateknot.scheduler_fairness_shards")
        .execute(pool)
        .await
        .expect("v14 scheduler shard table must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 14")
        .execute(pool)
        .await
        .expect("v14 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

async fn remove_agent_submission_keys(pool: &PgPool) {
    query("DROP TABLE stateknot.agent_submission_keys")
        .execute(pool)
        .await
        .expect("v16 Agent submission-key table must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.agent_admissions \
         DROP CONSTRAINT agent_admissions_run_digest_unique",
    )
    .execute(pool)
    .await
    .expect("v16 Agent admission reference key must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 16")
        .execute(pool)
        .await
        .expect("v16 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

async fn remove_agent_admissions(pool: &PgPool) {
    remove_agent_submission_keys(pool).await;
    query("DROP TABLE stateknot.agent_admissions")
        .execute(pool)
        .await
        .expect("v15 Agent admission table must be removed from the fixture");
    query("DROP INDEX stateknot.runs_scheduler_ready")
        .execute(pool)
        .await
        .expect("v15 scheduler index must be removed from the fixture");
    query(
        "CREATE INDEX runs_scheduler_ready \
         ON stateknot.runs ( \
             tenant_id, \
             (GREATEST( \
                 scheduler_ready_at, \
                 COALESCE(scheduler_not_before, scheduler_ready_at), \
                 COALESCE(lease_expires_at, scheduler_ready_at) \
             )), \
             run_id \
         ) \
         WHERE quarantined_at IS NULL \
           AND scheduler_ready_at IS NOT NULL \
           AND lifecycle_status IN ('pending', 'active', 'cancellation_requested')",
    )
    .execute(pool)
    .await
    .expect("the exact v12 scheduler index must be restored");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 15")
        .execute(pool)
        .await
        .expect("v15 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

async fn remove_scheduler_readiness(pool: &PgPool) {
    remove_transactional_outbox(pool).await;
    query("DROP INDEX stateknot.runs_scheduler_ready")
        .execute(pool)
        .await
        .expect("v7 scheduler index must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.runs \
         DROP CONSTRAINT runs_scheduler_ready_shape, \
         DROP COLUMN scheduler_ready_at",
    )
    .execute(pool)
    .await
    .expect("v7 scheduler projection must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 7")
        .execute(pool)
        .await
        .expect("v7 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
}

trait PendingNodeResultTestExt {
    async fn commit_test_pending_node_result(
        &self,
        append: JournalAppend,
        intent: PendingNodeResultIntent,
    ) -> Result<PendingNodeResultCommitOutcome, StoreError>;
}

impl PendingNodeResultTestExt for PostgresStore {
    async fn commit_test_pending_node_result(
        &self,
        append: JournalAppend,
        intent: PendingNodeResultIntent,
    ) -> Result<PendingNodeResultCommitOutcome, StoreError> {
        let tenant_id = append.intent().tenant_id().clone();
        let run_id = append.intent().run_id();
        let start_event_id = append.intent().event_id();
        let payload = append.intent().payload().clone();
        let fence = append
            .worker_fence()
            .cloned()
            .ok_or(StoreError::WrongAppendAuthority)?;
        let attempt_id = AttemptId::from_uuid(*start_event_id.as_uuid())
            .map_err(|_| StoreError::NodeAttemptCommitConflict)?;
        let activation = intent.activation().clone();
        let started = self
            .start_node_attempt(append, activation.clone(), attempt_id)
            .await?;
        let success_intent =
            JournalEventIntent::worker(tenant_id, run_id, EventId::generate(), fence, payload)
                .map_err(|_| StoreError::NodeAttemptCommitConflict)?;
        let success_append = JournalAppend::new(
            JournalExpectation::exact(started.event().head()),
            success_intent,
        )
        .map_err(|_| StoreError::NodeAttemptCommitConflict)?;
        let outcome = self
            .succeed_node_attempt(
                success_append,
                &started.attempt().start().head(),
                intent,
                BudgetUsage::zero(),
            )
            .await?;
        let result = self.load_pending_node_result(&activation).await?;
        match outcome {
            NodeAttemptCommitOutcome::Committed { event, .. } => {
                Ok(PendingNodeResultCommitOutcome::Committed { event, result })
            }
            NodeAttemptCommitOutcome::Idempotent { event, .. } => {
                Ok(PendingNodeResultCommitOutcome::Idempotent { event, result })
            }
            _ => Err(StoreError::NodeAttemptCommitConflict),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_claim_rejects_clock_before_latest_renewal() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("test administration connection must open");
    let tenant_id = tenant("claim-clock-regression");
    let run_id = RunId::generate();
    let attempt_id = AttemptId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    store
        .claim_lease(&tenant_id, run_id, attempt_id)
        .await
        .unwrap();

    let updated = query(
        "UPDATE stateknot.runs \
         SET lease_renewed_at = clock_timestamp() + interval '1 minute', \
             lease_expires_at = clock_timestamp() + interval '2 minutes' \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .execute(&administration)
    .await
    .expect("future durable renewal observation must be injected")
    .rows_affected();
    assert_eq!(updated, 1);

    assert!(matches!(
        store.claim_lease(&tenant_id, run_id, attempt_id).await,
        Err(StoreError::DatabaseClockRegression)
    ));
    administration.close().await;
    store.close().await;
}

#[test]
fn run_quarantine_contract_rejects_unbounded_or_crossed_codes() {
    for invalid in ["", "Uppercase", "contains space", "contains/slash"] {
        assert!(matches!(
            RunQuarantineComponent::new(invalid),
            Err(StoreError::InvalidRunQuarantineComponent)
        ));
    }
    assert!(matches!(
        RunQuarantineComponent::new("a".repeat(RunQuarantineComponent::MAX_LEN + 1)),
        Err(StoreError::InvalidRunQuarantineComponent)
    ));
    assert_eq!(
        RunQuarantineComponent::from_corrupt_store_error(&StoreError::CorruptData {
            record: "run lifecycle bytes",
        })
        .unwrap()
        .as_str(),
        "store.run_lifecycle_bytes"
    );
    assert!(matches!(
        RunQuarantineComponent::from_corrupt_store_error(&StoreError::RunNotFound),
        Err(StoreError::InvalidRunQuarantineRequest)
    ));

    let tenant_id = tenant("quarantine-contract");
    let run_id = RunId::generate();
    let crossed_head = JournalHead::new(
        tenant_id.clone(),
        RunId::generate(),
        JournalSequence::new(1).unwrap(),
        EventId::generate(),
        Timestamp::from_unix_micros(1).unwrap(),
        Digest::sha256(b"crossed quarantine head"),
    );
    assert!(matches!(
        RunQuarantineRequest::new(
            tenant_id,
            run_id,
            QuarantineId::generate(),
            JournalExpectation::exact(crossed_head.clone()),
            RunQuarantineCause::IntegrityFailure,
            RunQuarantineComponent::new("checkpoint.digest").unwrap(),
            Digest::sha256(b"evidence"),
        ),
        Err(StoreError::InvalidRunQuarantineRequest)
    ));
    assert!(matches!(
        CorruptionQuarantineContext::new(
            crossed_head.tenant_id().clone(),
            run_id,
            QuarantineId::generate(),
            JournalExpectation::exact(crossed_head),
            Digest::sha256(b"recovery evidence"),
        ),
        Err(StoreError::InvalidRunQuarantineRequest)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_quarantine_is_atomic_idempotent_and_removes_execution_ownership() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("run-quarantine");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .expect("the fixture must hold execution ownership before quarantine");
    let quarantine_id = QuarantineId::generate();
    let request = quarantine_request(
        tenant_id.clone(),
        run_id,
        quarantine_id,
        JournalExpectation::empty(),
        RunQuarantineCause::ProjectionMismatch,
        "recovery.lifecycle_projection",
        b"redacted projection evidence",
    );

    let committed = store
        .quarantine_run(request.clone())
        .await
        .expect("quarantine evidence and projection must commit atomically");
    let RunQuarantineCommitOutcome::Committed(quarantine) = committed else {
        panic!("first quarantine request must commit")
    };
    assert_eq!(quarantine.request(), &request);
    assert_eq!(
        store.load_run_quarantine(&tenant_id, run_id).await.unwrap(),
        quarantine
    );
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert!(run.is_quarantined());
    assert!(run.lease().is_none());
    assert!(
        store
            .load_runnable_run_page(&tenant_id, None, RunnableRunPageSize::new(16).unwrap())
            .await
            .unwrap()
            .records()
            .iter()
            .all(|candidate| candidate.run().lifecycle().provenance().run_id() != run_id)
    );
    assert!(matches!(
        store
            .claim_lease(&tenant_id, run_id, AttemptId::generate())
            .await,
        Err(StoreError::RunQuarantined)
    ));

    let retry = store
        .quarantine_run(request.clone())
        .await
        .expect("same-ID lost-ack retry must converge");
    assert!(matches!(
        retry,
        RunQuarantineCommitOutcome::Idempotent(ref value) if value == &quarantine
    ));
    let changed = quarantine_request(
        tenant_id.clone(),
        run_id,
        quarantine_id,
        JournalExpectation::empty(),
        RunQuarantineCause::IntegrityFailure,
        "recovery.changed_intent",
        b"different evidence",
    );
    assert!(matches!(
        store.quarantine_run(changed).await,
        Err(StoreError::RunQuarantineIdConflict)
    ));
    assert!(matches!(
        store
            .quarantine_run(quarantine_request(
                tenant_id.clone(),
                run_id,
                QuarantineId::generate(),
                JournalExpectation::empty(),
                RunQuarantineCause::ProjectionMismatch,
                "recovery.lifecycle_projection",
                b"redacted projection evidence",
            ))
            .await,
        Err(StoreError::RunQuarantineConflict)
    ));
    let other_tenant = tenant("run-quarantine-crossed");
    assert!(matches!(
        store.load_run_quarantine(&other_tenant, run_id).await,
        Err(StoreError::RunNotFound)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_renewal_retry_confirms_only_the_already_committed_expiry() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(2)).await else {
        return;
    };
    let tenant_id = tenant("renewal-expiry");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let claim = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap();
    let lease = claim.lease();
    let desired_expiry = Timestamp::from_unix_micros(
        lease
            .expires_at()
            .unix_micros()
            .checked_add(2_000_000)
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .renew_lease(lease.fence(), desired_expiry)
            .await
            .unwrap(),
        LeaseRenewalOutcome::Renewed(_)
    ));
    let live = store.observe_live_lease(lease.fence()).await.unwrap();
    assert_eq!(live.lease().fence(), lease.fence());
    assert!(live.observed_at() < live.lease().expires_at());

    tokio::time::sleep(Duration::from_millis(4_200)).await;
    assert!(matches!(
        store
            .renew_lease(lease.fence(), desired_expiry)
            .await
            .unwrap(),
        LeaseRenewalOutcome::Idempotent(_)
    ));
    assert!(matches!(
        store.observe_live_lease(lease.fence()).await,
        Err(StoreError::LeaseExpired)
    ));
    let later_expiry =
        Timestamp::from_unix_micros(desired_expiry.unix_micros().checked_add(2_000_000).unwrap())
            .unwrap();
    assert!(matches!(
        store.renew_lease(lease.fence(), later_expiry).await,
        Err(StoreError::LeaseExpired)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn runnable_run_pages_are_tenant_scoped_snapshot_stable_and_lease_aware() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_millis(250)).await else {
        return;
    };
    let tenant_id = tenant("scheduler-page");
    let foreign_tenant = tenant("scheduler-page-foreign");
    let first_initial = RunId::generate();
    let second_initial = RunId::generate();
    let foreign_run = RunId::generate();
    for run_id in [first_initial, second_initial] {
        Box::pin(admit_atomic_agent_fixture(&store, &tenant_id, run_id)).await;
    }
    Box::pin(admit_atomic_agent_fixture(
        &store,
        &foreign_tenant,
        foreign_run,
    ))
    .await;

    let first_page = store
        .load_runnable_run_page(&tenant_id, None, RunnableRunPageSize::new(1).unwrap())
        .await
        .expect("first scheduler page must load");
    assert_eq!(first_page.records().len(), 1);
    assert!(first_page.has_more());
    let first_candidate = &first_page.records()[0];
    assert_eq!(first_candidate.ready_at(), first_candidate.available_at());
    let first_run = first_candidate.run().lifecycle().provenance().run_id();
    let remaining_initial = if first_run == first_initial {
        second_initial
    } else {
        first_initial
    };
    let cursor = first_page.next_cursor().unwrap();
    assert!(matches!(
        store
            .load_runnable_run_page(
                &foreign_tenant,
                Some(&cursor),
                RunnableRunPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::InvalidRunnableRunCursor)
    ));

    let first_lease = store
        .claim_lease(&tenant_id, first_run, AttemptId::generate())
        .await
        .expect("the exact page candidate must be claimable")
        .lease()
        .clone();
    assert!(matches!(
        store.release_lease(first_lease.fence()).await.unwrap(),
        LeaseReleaseOutcome::Released
    ));
    let released = store.load_run(&tenant_id, first_run).await.unwrap();
    assert!(released.scheduler_ready_at().unwrap() > first_page.snapshot_at());

    let late_run = RunId::generate();
    let late_admission = Box::pin(admit_atomic_agent_fixture(&store, &tenant_id, late_run)).await;
    let continuation = store
        .load_runnable_run_page(
            &tenant_id,
            Some(&cursor),
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .expect("snapshot continuation must remain valid after queue mutations");
    let continuation_ids = continuation
        .records()
        .iter()
        .map(|candidate| candidate.run().lifecycle().provenance().run_id())
        .collect::<Vec<_>>();
    assert_eq!(continuation.snapshot_at(), first_page.snapshot_at());
    assert_eq!(continuation_ids, vec![remaining_initial]);
    assert!(!continuation.has_more());

    let fresh = store
        .load_runnable_run_page(
            &tenant_id,
            None,
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .expect("a fresh snapshot must observe requeued and newly admitted work");
    let fresh_ids = fresh
        .records()
        .iter()
        .map(|candidate| candidate.run().lifecycle().provenance().run_id())
        .collect::<Vec<_>>();
    assert_eq!(fresh_ids.len(), 3);
    assert!(fresh_ids.contains(&first_initial));
    assert!(fresh_ids.contains(&second_initial));
    assert!(fresh_ids.contains(&late_run));
    assert!(!fresh_ids.contains(&foreign_run));

    let delayed_lease = store
        .claim_lease(&tenant_id, remaining_initial, AttemptId::generate())
        .await
        .expect("another exact candidate must be claimable")
        .lease()
        .clone();
    let before_expiry = store
        .load_runnable_run_page(
            &tenant_id,
            None,
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .expect("a live lease must be hidden from a new scheduler snapshot");
    assert!(before_expiry.records().iter().all(|candidate| {
        candidate.run().lifecycle().provenance().run_id() != remaining_initial
    }));

    tokio::time::sleep(Duration::from_millis(350)).await;
    let after_expiry = store
        .load_runnable_run_page(
            &tenant_id,
            None,
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .expect("an expired lease must become discoverable without a polling update");
    let expired_candidate = after_expiry
        .records()
        .iter()
        .find(|candidate| candidate.run().lifecycle().provenance().run_id() == remaining_initial)
        .expect("the expired candidate must reappear");
    assert_eq!(expired_candidate.available_at(), delayed_lease.expires_at());
    assert!(expired_candidate.available_at() > expired_candidate.ready_at());

    let before_cancellation = store.load_run(&tenant_id, late_run).await.unwrap();
    let cancellation = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                late_run,
                EventId::generate(),
                JournalExpectation::exact(late_admission.event().head()),
                732,
            ),
            RunProjection::transition(
                before_cancellation.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: cancellation_request(before_cancellation.lifecycle().admitted_at()),
                },
            ),
        )
        .await
        .expect("a cancellation request must remain scheduler-runnable");
    let cancellation_requested = store.load_run(&tenant_id, late_run).await.unwrap();
    assert_eq!(
        cancellation_requested.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert!(
        cancellation_requested.scheduler_ready_at().unwrap()
            > cancellation_requested.lifecycle().changed_at(),
        "a lifecycle transition must requeue at its database commit observation"
    );
    let cancellation_page = store
        .load_runnable_run_page(
            &tenant_id,
            None,
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        cancellation_page
            .records()
            .iter()
            .any(|candidate| { candidate.run().lifecycle().provenance().run_id() == late_run })
    );

    store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                late_run,
                EventId::generate(),
                JournalExpectation::exact(cancellation.event().head()),
                733,
            ),
            RunProjection::transition(
                cancellation_requested.lifecycle().revision(),
                RunTransition::ConfirmCancellation {
                    completed_at: cancellation.event().recorded_at(),
                    usage: BudgetUsage::zero(),
                },
            ),
        )
        .await
        .expect("terminal cancellation must commit");
    let terminal = store.load_run(&tenant_id, late_run).await.unwrap();
    assert_eq!(terminal.lifecycle().status(), RunStatus::Cancelled);
    assert_eq!(terminal.scheduler_ready_at(), None);
    let terminal_page = store
        .load_runnable_run_page(
            &tenant_id,
            None,
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        terminal_page
            .records()
            .iter()
            .all(|candidate| { candidate.run().lifecycle().provenance().run_id() != late_run })
    );

    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_schedulers_claim_exactly_one_discovered_run() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("scheduler-claim-race");
    let run_id = RunId::generate();
    Box::pin(admit_atomic_agent_fixture(&store, &tenant_id, run_id)).await;
    let page = store
        .load_runnable_run_page(&tenant_id, None, RunnableRunPageSize::new(1).unwrap())
        .await
        .expect("the shared candidate must be discoverable");
    assert_eq!(
        page.records()[0].run().lifecycle().provenance().run_id(),
        run_id
    );

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        tasks.spawn(async move {
            store
                .claim_lease(&tenant_id, run_id, AttemptId::generate())
                .await
        });
    }
    let mut claimed = 0_u64;
    let mut held = 0_u64;
    while let Some(joined) = tasks.join_next().await {
        match joined.expect("scheduler contender must not panic") {
            Ok(LeaseClaimOutcome::Claimed(_)) => claimed += 1,
            Err(StoreError::LeaseHeld) => held += 1,
            outcome => panic!("unexpected scheduler claim outcome: {outcome:?}"),
        }
    }
    assert_eq!(claimed, 1);
    assert_eq!(held, 23);
    assert!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .lease()
            .is_some()
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_after_event_insert_rolls_back_event_and_head_together() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("test administration connection must open");
    let tenant_id = tenant("atomicity");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    query("ALTER TABLE stateknot.runs DROP CONSTRAINT IF EXISTS test_atomic_append_rollback")
        .execute(&administration)
        .await
        .unwrap();
    let reject_target = format!(
        "ALTER TABLE stateknot.runs ADD CONSTRAINT test_atomic_append_rollback CHECK (tenant_id <> '{}') NOT VALID",
        tenant_id.as_str()
    );
    query(&reject_target)
        .execute(&administration)
        .await
        .unwrap();

    let append_result = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                1,
            ),
            RunProjection::unchanged(),
        )
        .await;

    query("ALTER TABLE stateknot.runs DROP CONSTRAINT test_atomic_append_rollback")
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
    assert!(matches!(append_result, Err(StoreError::Database { .. })));

    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert!(run.journal_head().is_none());
    let page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .expect("rolled-back append must leave a valid empty journal");
    assert!(page.events().is_empty());
    assert!(!page.has_more());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_connection_refuses_an_unmigrated_database() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_schema_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .expect("test administration connection must open");
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated test database must be created");

    assert!(matches!(
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30))).await,
        Err(StoreError::SchemaNotMigrated)
    ));
    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("isolated database migration must succeed");
    let store = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("runtime connection must accept the exact migrated schema");
    store.close().await;

    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .expect("isolated test database must be dropped");
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn current_migrations_upgrade_existing_v1_history_without_guessing_projection_intent() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .expect("test administration connection must open");
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated upgrade database must be created");

    let legacy_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("legacy database connection must open");
    let v1_migrator = Migrator {
        migrations: Cow::Owned(vec![Migration::new(
            1,
            Cow::Borrowed("initial"),
            MigrationType::Simple,
            Cow::Borrowed(include_str!("../migrations/0001_initial.sql")),
            false,
        )]),
        ignore_missing: false,
        locking: true,
        no_tx: false,
    };
    v1_migrator
        .run(&legacy_pool)
        .await
        .expect("the exact v1 migration must apply");

    let tenant_id = tenant("v1-upgrade");
    let run_id = RunId::generate();
    let provenance = provenance(tenant_id.clone(), run_id);
    let thread_id = provenance.thread_id();
    let invocation_id = provenance.invocation_id();
    let recorded_at = "2030-01-01T00:00:00.000001Z".parse::<Timestamp>().unwrap();
    let recorded_at_db = chrono::DateTime::from_timestamp_micros(recorded_at.unix_micros())
        .expect("fixture timestamp must fit PostgreSQL");
    let lifecycle = stateknot_core::RunLifecycle::admitted(provenance, recorded_at);
    let lifecycle_bytes =
        serde_json_canonicalizer::to_vec(&lifecycle).expect("legacy lifecycle must canonicalize");
    query(
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
    changed_at
)
VALUES ($1, $2, $3, $4, $5, $6::numeric, 'pending', $7, $7)
",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*thread_id.as_uuid())
    .bind(*invocation_id.as_uuid())
    .bind(lifecycle_bytes)
    .bind(lifecycle.revision().to_string())
    .bind(recorded_at_db)
    .execute(&legacy_pool)
    .await
    .expect("legacy run must be inserted through the v1 schema");

    let legacy_event_id = EventId::generate();
    let legacy_append = || {
        control_append(
            tenant_id.clone(),
            run_id,
            legacy_event_id,
            JournalExpectation::empty(),
            700,
        )
    };
    let legacy_event = stateknot_core::JournalEvent::commit(legacy_append(), recorded_at)
        .expect("legacy event fixture must commit");
    let payload_bytes = legacy_event
        .payload()
        .canonical_json()
        .expect("legacy payload must canonicalize")
        .as_bytes()
        .to_vec();
    let schema = legacy_event.payload().schema();
    query(
        r"
INSERT INTO stateknot.run_events (
    tenant_id,
    run_id,
    sequence,
    event_id,
    recorded_at,
    source_kind,
    event_kind,
    schema_id,
    schema_version,
    schema_digest,
    payload_bytes,
    payload_digest,
    intent_digest,
    event_digest
)
VALUES ($1, $2, $3, $4, $5, 'control_plane', $6, $7, $8, $9, $10, $11, $12, $13)
",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(i64::try_from(legacy_event.sequence().get()).unwrap())
    .bind(*legacy_event.event_id().as_uuid())
    .bind(recorded_at_db)
    .bind(legacy_event.payload().kind().as_str())
    .bind(schema.id().as_str())
    .bind(schema.version().to_string())
    .bind(schema.digest().as_bytes())
    .bind(payload_bytes)
    .bind(legacy_event.payload_digest().as_bytes())
    .bind(legacy_event.intent_digest().as_bytes())
    .bind(legacy_event.digest().as_bytes())
    .execute(&legacy_pool)
    .await
    .expect("legacy event must be inserted through the v1 schema");
    query(
        r"
UPDATE stateknot.runs
SET journal_sequence = $3,
    journal_event_id = $4,
    journal_recorded_at = $5,
    journal_digest = $6,
    updated_at = $5
WHERE tenant_id = $1 AND run_id = $2
",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(i64::try_from(legacy_event.sequence().get()).unwrap())
    .bind(*legacy_event.event_id().as_uuid())
    .bind(recorded_at_db)
    .bind(legacy_event.digest().as_bytes())
    .execute(&legacy_pool)
    .await
    .expect("legacy journal head must be projected through the v1 schema");
    legacy_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("current migrations must upgrade an existing v1 history");
    let store = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("the upgraded runtime schema must be accepted");
    let page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .expect("legacy history must remain readable and verifiable");
    assert_eq!(page.events().len(), 1);
    assert_eq!(page.events()[0], legacy_event);
    assert!(!page.has_more());
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .expect("an upgraded v1 run must expose an empty checkpoint pointer"),
        None
    );
    assert!(matches!(
        store
            .append_control_plane(legacy_append(), RunProjection::unchanged())
            .await,
        Err(StoreError::ProjectionIntentConflict)
    ));

    let successor = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(legacy_event.head()),
                701,
            ),
            RunProjection::unchanged(),
        )
        .await
        .expect("new projection-bound events must append after the upgrade");
    assert_eq!(successor.event().sequence().get(), 2);
    store.close().await;

    query(&format!("DROP DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated upgrade database must be dropped");
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_four_backfills_existing_tool_attempts_into_the_run_registry() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v4_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .expect("test administration connection must open");
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated v4 upgrade database must be created");

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("fixture database must initially reach the current schema");
    let legacy_store = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("fixture store must connect");
    let tenant_id = tenant("v4-tool-attempt-upgrade");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(
        &legacy_store,
        &tenant_id,
        run_id,
        710,
    ))
    .await;
    let lease = legacy_store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let tool_invocation_id = InvocationId::generate();
    let tool_prepared = legacy_store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                711,
            ),
            tool_invocation_intent(checkpoint.checkpoint(), tool_invocation_id),
        )
        .await
        .unwrap();
    let existing_attempt_id = AttemptId::generate();
    let tool_executing = legacy_store
        .advance_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(tool_prepared.event().head()),
                lease.fence().clone(),
                712,
            ),
            &tool_prepared.invocation().head(),
            ToolInvocationTransition::StartAttempt {
                attempt_id: existing_attempt_id,
            },
        )
        .await
        .expect("the authentic pre-v4 tool attempt fixture must commit");
    legacy_store.close().await;

    let legacy_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("isolated fixture administration connection must open");
    remove_scheduler_readiness(&legacy_pool).await;
    query("DROP TABLE stateknot.node_attempt_completions")
        .execute(&legacy_pool)
        .await
        .expect("v6 node-attempt completions must be removed from the fixture");
    query("DROP TABLE stateknot.pending_node_result_consumptions")
        .execute(&legacy_pool)
        .await
        .expect("v5 pending-result consumptions must be removed from the fixture");
    query("DROP TABLE stateknot.pending_node_result_tool_bindings")
        .execute(&legacy_pool)
        .await
        .expect("v5 pending-result tool bindings must be removed from the fixture");
    query("DROP TABLE stateknot.pending_node_result_model_bindings")
        .execute(&legacy_pool)
        .await
        .expect("v5 pending-result model bindings must be removed from the fixture");
    query("DROP TABLE stateknot.pending_node_results")
        .execute(&legacy_pool)
        .await
        .expect("v5 pending results must be removed from the fixture");
    query("DROP TABLE stateknot.node_attempts")
        .execute(&legacy_pool)
        .await
        .expect("v6 node-attempt starts must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 6")
        .execute(&legacy_pool)
        .await
        .expect("v6 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
    query(
        "ALTER TABLE stateknot.model_invocation_revisions \
         DROP CONSTRAINT model_invocation_revisions_committed_binding_unique",
    )
    .execute(&legacy_pool)
    .await
    .expect("v5 model revision binding key must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.tool_invocation_revisions \
         DROP CONSTRAINT tool_invocation_revisions_committed_binding_unique",
    )
    .execute(&legacy_pool)
    .await
    .expect("v5 tool revision binding key must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.model_invocations \
         DROP CONSTRAINT model_invocations_exact_activation_unique",
    )
    .execute(&legacy_pool)
    .await
    .expect("v5 model activation key must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.tool_invocations \
         DROP CONSTRAINT tool_invocations_exact_activation_unique",
    )
    .execute(&legacy_pool)
    .await
    .expect("v5 tool activation key must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.run_checkpoints \
         DROP CONSTRAINT run_checkpoints_exact_anchor_unique",
    )
    .execute(&legacy_pool)
    .await
    .expect("v5 checkpoint anchor key must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.run_events \
         DROP CONSTRAINT run_events_worker_anchor_unique",
    )
    .execute(&legacy_pool)
    .await
    .expect("v5 worker anchor key must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 5")
        .execute(&legacy_pool)
        .await
        .expect("v5 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
    query(
        "ALTER TABLE stateknot.tool_invocation_revisions \
         DROP CONSTRAINT tool_invocation_revisions_global_attempt_claim_fk",
    )
    .execute(&legacy_pool)
    .await
    .expect("v4 tool claim foreign key must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.tool_invocation_revisions \
         DROP COLUMN attempt_claim_kind",
    )
    .execute(&legacy_pool)
    .await
    .expect("v4 tool claim discriminator must be removed from the fixture");
    query(
        "ALTER TABLE stateknot.model_invocations \
         DROP CONSTRAINT model_invocations_current_record_fk",
    )
    .execute(&legacy_pool)
    .await
    .expect("v4 model current-record foreign key must be removed from the fixture");
    query("DROP TABLE stateknot.model_invocation_revisions")
        .execute(&legacy_pool)
        .await
        .expect("v4 model revisions must be removed from the fixture");
    query("DROP TABLE stateknot.model_invocations")
        .execute(&legacy_pool)
        .await
        .expect("v4 model intents must be removed from the fixture");
    query("DROP TABLE stateknot.run_attempt_claims")
        .execute(&legacy_pool)
        .await
        .expect("v4 attempt registry must be removed from the fixture");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 4")
        .execute(&legacy_pool)
        .await
        .expect("v4 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
    legacy_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 4 must upgrade an existing v3 attempt history");
    let upgraded_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("the upgraded v4 runtime schema must be accepted");
    upgraded_store
        .verify_schema()
        .await
        .expect("the upgraded schema must pass the exact runtime probe");
    let restored_tool = upgraded_store
        .load_tool_invocation(&tenant_id, run_id, tool_invocation_id)
        .await
        .expect("the pre-v4 tool invocation must remain fully verifiable");
    assert_eq!(restored_tool.head(), tool_executing.invocation().head());

    let verification_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("upgraded fixture verification connection must open");
    let crossed_claim = query(
        "UPDATE stateknot.run_attempt_claims \
         SET claim_kind = 'model_invocation' \
         WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*existing_attempt_id.as_uuid())
    .execute(&verification_pool)
    .await;
    assert!(
        crossed_claim.is_err(),
        "the exact tool revision foreign key must reject a crossed claim kind"
    );
    verification_pool.close().await;

    let current_lease = upgraded_store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let model_invocation_id = InvocationId::generate();
    let model_prepared = upgraded_store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(tool_executing.event().head()),
                current_lease.fence().clone(),
                713,
            ),
            model_invocation_intent(checkpoint.checkpoint(), model_invocation_id),
        )
        .await
        .expect("new model work must be available after the upgrade");
    assert!(matches!(
        upgraded_store
            .advance_model_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(model_prepared.event().head()),
                    current_lease.fence().clone(),
                    714,
                ),
                &model_prepared.invocation().head(),
                ModelInvocationTransition::StartAttempt {
                    attempt_id: existing_attempt_id,
                },
            )
            .await,
        Err(StoreError::InvalidModelInvocationTransition)
    ));
    assert_eq!(
        upgraded_store
            .load_model_invocation(&tenant_id, run_id, model_invocation_id)
            .await
            .unwrap()
            .status(),
        ModelInvocationStatus::Prepared
    );
    let journal = upgraded_store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 4);
    assert_eq!(journal.events().last().unwrap(), model_prepared.event());
    upgraded_store.close().await;

    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .expect("isolated v4 upgrade database must be dropped");
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_six_preserves_legacy_results_without_fabricating_starts() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v6_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .expect("test administration connection must open");
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated v6 upgrade database must be created");

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("fixture database must initially reach the current schema");
    let fixture_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("fixture store must connect");
    let tenant_id = tenant("v6-legacy-result-upgrade");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(
        &fixture_store,
        &tenant_id,
        run_id,
        720,
    ))
    .await;
    let lease = fixture_store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"truthful legacy result");
    let committed = fixture_store
        .commit_test_pending_node_result(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                721,
            ),
            pending_result_intent(activation.clone(), NodeInvocationBindings::empty()),
        )
        .await
        .expect("fixture result must commit before reversing only v6 additions");
    let expected_result = committed.result().clone();
    let result_event_id = committed.event().event_id();
    fixture_store.close().await;

    let fixture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("isolated fixture administration connection must open");
    remove_scheduler_readiness(&fixture_pool).await;
    query(
        "UPDATE stateknot.run_events SET projection_digest = $3 \
         WHERE tenant_id = $1 AND run_id = $2 AND event_id = $4",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(expected_result.digest().as_bytes())
    .bind(*result_event_id.as_uuid())
    .execute(&fixture_pool)
    .await
    .expect("legacy result event must directly bind its result digest");
    query("DELETE FROM stateknot.node_attempt_completions")
        .execute(&fixture_pool)
        .await
        .expect("v6 completion fixture rows must be removed");
    query(
        "UPDATE stateknot.pending_node_results SET node_attempt_id = NULL \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .execute(&fixture_pool)
    .await
    .expect("legacy result must truthfully have no physical attempt owner");
    query("DELETE FROM stateknot.node_attempts")
        .execute(&fixture_pool)
        .await
        .expect("v6 start fixture rows must be removed");
    query("DELETE FROM stateknot.run_attempt_claims WHERE claim_kind = 'node_attempt'")
        .execute(&fixture_pool)
        .await
        .expect("v6 node claim fixture rows must be removed");
    query("DROP TABLE stateknot.node_attempt_completions")
        .execute(&fixture_pool)
        .await
        .expect("v6 completion table must be removed");
    query(
        "ALTER TABLE stateknot.pending_node_results \
         DROP CONSTRAINT pending_node_results_node_attempt_fk, \
         DROP CONSTRAINT pending_node_results_node_attempt_exact_unique, \
         DROP CONSTRAINT pending_node_results_node_attempt_unique, \
         DROP CONSTRAINT pending_node_results_node_attempt_id_valid, \
         DROP COLUMN node_attempt_id",
    )
    .execute(&fixture_pool)
    .await
    .expect("v6 pending-result owner additions must be removed");
    query("DROP TABLE stateknot.node_attempts")
        .execute(&fixture_pool)
        .await
        .expect("v6 start table must be removed");
    query(
        "ALTER TABLE stateknot.run_attempt_claims \
         DROP CONSTRAINT run_attempt_claims_node_exact_unique, \
         DROP CONSTRAINT run_attempt_claims_owner_shape, \
         DROP CONSTRAINT run_attempt_claims_kind_valid, \
         DROP CONSTRAINT run_attempt_claims_ids_are_uuid_v7, \
         DROP COLUMN activation_digest, \
         ALTER COLUMN invocation_id SET NOT NULL, \
         ALTER COLUMN invocation_revision SET NOT NULL, \
         ADD CONSTRAINT run_attempt_claims_ids_are_uuid_v7 CHECK ( \
             stateknot.is_uuid_v7(run_id) \
             AND stateknot.is_uuid_v7(attempt_id) \
             AND stateknot.is_uuid_v7(invocation_id) \
             AND stateknot.is_uuid_v7(journal_event_id) \
         ), \
         ADD CONSTRAINT run_attempt_claims_kind_valid CHECK ( \
             claim_kind IN ('tool_invocation', 'model_invocation') \
         ), \
         ADD CONSTRAINT run_attempt_claims_position_valid CHECK ( \
             invocation_revision > 0 AND journal_sequence > 0 \
         )",
    )
    .execute(&fixture_pool)
    .await
    .expect("attempt registry must be restored to its exact v5 shape");
    let deleted = query("DELETE FROM _sqlx_migrations WHERE version = 6")
        .execute(&fixture_pool)
        .await
        .expect("v6 migration metadata must be removed from the fixture")
        .rows_affected();
    assert_eq!(deleted, 1);
    fixture_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 6 must upgrade the exact v5 legacy-result fixture");
    let upgraded_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("the upgraded v6 runtime schema must be accepted");
    assert_eq!(
        upgraded_store
            .load_pending_node_result(&activation)
            .await
            .expect("legacy result must remain fully verifiable after v6"),
        expected_result
    );
    let verification_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("upgraded fixture verification connection must open");
    let owner_is_null: bool = query_scalar(
        "SELECT node_attempt_id IS NULL FROM stateknot.pending_node_results \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .fetch_one(&verification_pool)
    .await
    .expect("upgraded legacy owner projection must be queryable");
    assert!(owner_is_null);
    verification_pool.close().await;
    upgraded_store.close().await;

    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .expect("isolated v6 upgrade database must be dropped");
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_seven_backfills_an_indexed_fail_closed_scheduler_projection() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v7_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .expect("test administration connection must open");
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated v7 upgrade database must be created");

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("fixture database must initially reach the current schema");
    let fixture_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("fixture store must connect");
    let tenant_id = tenant("v7-scheduler-upgrade");
    let pending_run = RunId::generate();
    let leased_run = RunId::generate();
    fixture_store
        .admit_run(provenance(tenant_id.clone(), pending_run))
        .await
        .expect("the v6-style pending fixture must be admitted");
    Box::pin(start_run_with_checkpoint(
        &fixture_store,
        &tenant_id,
        leased_run,
        730,
    ))
    .await;
    fixture_store
        .claim_lease(&tenant_id, leased_run, AttemptId::generate())
        .await
        .expect("the v6-style active fixture must retain a lease");
    fixture_store.close().await;

    let fixture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("isolated fixture administration connection must open");
    remove_scheduler_readiness(&fixture_pool).await;
    fixture_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 7 must upgrade the exact v6 run projection");
    let upgraded_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("the upgraded v7 runtime schema must be accepted");
    upgraded_store
        .verify_schema()
        .await
        .expect("the scheduler index and validated shape constraint must be present");

    let verification_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("upgraded fixture verification connection must open");
    let backfill_is_exact = query_scalar::<_, bool>(
        "SELECT \
             (SELECT scheduler_ready_at = updated_at \
              FROM stateknot.runs WHERE tenant_id = $1 AND run_id = $2) \
             AND \
             (SELECT scheduler_ready_at = changed_at \
                     AND GREATEST( \
                         scheduler_ready_at, \
                         COALESCE(lease_expires_at, scheduler_ready_at) \
                     ) = lease_expires_at \
              FROM stateknot.runs WHERE tenant_id = $1 AND run_id = $3)",
    )
    .bind(tenant_id.as_str())
    .bind(*pending_run.as_uuid())
    .bind(*leased_run.as_uuid())
    .fetch_one(&verification_pool)
    .await
    .expect("v7 readiness backfill must be queryable");
    assert!(backfill_is_exact);

    let index_definition = query_scalar::<_, String>(
        "SELECT indexdef FROM pg_catalog.pg_indexes \
         WHERE schemaname = 'stateknot' AND indexname = 'runs_scheduler_ready'",
    )
    .fetch_one(&verification_pool)
    .await
    .expect("the scheduler index must exist");
    let index_definition = index_definition.to_ascii_lowercase();
    assert!(index_definition.contains("greatest(scheduler_ready_at"));
    assert!(index_definition.contains("quarantined_at is null"));

    let page = upgraded_store
        .load_runnable_run_page(
            &tenant_id,
            None,
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .expect("legacy uninitialized rows must be evaluated safely");
    let page_ids = page
        .records()
        .iter()
        .map(|candidate| candidate.run().lifecycle().provenance().run_id())
        .collect::<Vec<_>>();
    assert!(
        page_ids.is_empty(),
        "legacy rows without an initial checkpoint must fail closed from discovery"
    );

    assert!(
        query(
            "UPDATE stateknot.runs SET scheduler_ready_at = NULL \
             WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(tenant_id.as_str())
        .bind(*pending_run.as_uuid())
        .execute(&verification_pool)
        .await
        .is_err(),
        "the validated v7 shape must reject a missing runnable projection"
    );
    query("ALTER TABLE stateknot.runs DROP CONSTRAINT runs_scheduler_ready_shape")
        .execute(&verification_pool)
        .await
        .expect("the isolated corruption fixture must remove the shape guard");
    assert!(matches!(
        upgraded_store.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(
        "UPDATE stateknot.runs SET scheduler_ready_at = NULL \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*pending_run.as_uuid())
    .execute(&verification_pool)
    .await
    .expect("the isolated corruption fixture must bypass the removed guard");
    assert!(matches!(
        upgraded_store.load_run(&tenant_id, pending_run).await,
        Err(StoreError::CorruptData { .. })
    ));

    verification_pool.close().await;
    upgraded_store.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .expect("isolated v7 upgrade database must be dropped");
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_eight_preserves_v7_attempts_and_installs_exact_outbox_guards() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v8_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .expect("test administration connection must open");
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated v8 upgrade database must be created");

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("fixture database must initially reach the current schema");
    let fixture_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("fixture store must connect");
    let tenant_id = tenant("v8-attempt-upgrade");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(
        &fixture_store,
        &tenant_id,
        run_id,
        740,
    ))
    .await;
    let lease = fixture_store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"v8 preserved node attempt");
    let node_attempt_id = AttemptId::generate();
    let started = fixture_store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                741,
            ),
            activation,
            node_attempt_id,
        )
        .await
        .expect("authentic v7 node attempt fixture must commit");
    fixture_store.close().await;

    let fixture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("isolated v8 fixture administration connection must open");
    remove_transactional_outbox(&fixture_pool).await;
    let legacy_version = query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
        .fetch_one(&fixture_pool)
        .await
        .unwrap();
    assert_eq!(legacy_version, 7);
    let preserved_claims = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.run_attempt_claims \
         WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3 \
           AND claim_kind = 'node_attempt'",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*node_attempt_id.as_uuid())
    .fetch_one(&fixture_pool)
    .await
    .unwrap();
    assert_eq!(preserved_claims, 1);
    fixture_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 8 must upgrade the exact v7 attempt registry");
    let upgraded_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("the upgraded v8 runtime schema must be accepted");
    upgraded_store.verify_schema().await.unwrap();
    let restored = upgraded_store
        .load_node_attempt(&tenant_id, &run_id, node_attempt_id)
        .await
        .expect("pre-v8 node attempt must remain fully verifiable");
    assert_eq!(restored.start().head(), started.attempt().start().head());

    let verification_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    for index in [
        "outbox_deliveries_ready",
        "outbox_deliveries_expiry",
        "outbox_deliveries_abandoned_limit",
        "run_attempt_claims_non_outbox_anchor_unique",
    ] {
        let definition = query_scalar::<_, String>(
            "SELECT indexdef FROM pg_catalog.pg_indexes \
             WHERE schemaname = 'stateknot' AND indexname = $1",
        )
        .bind(index)
        .fetch_one(&verification_pool)
        .await
        .expect("v8 operational index must exist");
        assert!(definition.to_ascii_lowercase().contains("where"));
    }
    query("SET enable_seqscan = off")
        .execute(&verification_pool)
        .await
        .unwrap();
    let ready_plan = query_scalar::<_, String>(
        "EXPLAIN (COSTS OFF) \
         SELECT tenant_id, run_id, delivery_id \
         FROM stateknot.outbox_deliveries \
         WHERE tenant_id = $1 \
           AND status IN ('pending', 'delivering', 'retry_scheduled') \
           AND attempt_count < 64 \
           AND next_attempt_at <= clock_timestamp() \
           AND expires_at > clock_timestamp() \
         ORDER BY next_attempt_at ASC, delivery_id ASC \
         FOR UPDATE SKIP LOCKED LIMIT 1",
    )
    .bind(tenant_id.as_str())
    .fetch_all(&verification_pool)
    .await
    .unwrap()
    .join("\n")
    .to_ascii_lowercase();
    query("RESET enable_seqscan")
        .execute(&verification_pool)
        .await
        .unwrap();
    assert!(ready_plan.contains("outbox_deliveries_ready"));

    let (destination, config) = outbox_destination(&tenant_id, 8);
    upgraded_store
        .register_outbox_destination(destination.clone(), config)
        .await
        .unwrap();
    let event_id = EventId::generate();
    upgraded_store
        .append_control_plane_with_outbox(
            control_append(
                tenant_id.clone(),
                run_id,
                event_id,
                JournalExpectation::exact(started.event().head()),
                742,
            ),
            RunProjection::unchanged(),
            vec![outbox_intent(
                &tenant_id,
                run_id,
                event_id,
                DeliveryId::generate(),
                &destination,
                8,
                Duration::from_secs(60),
            )],
        )
        .await
        .unwrap();
    assert!(matches!(
        upgraded_store
            .claim_outbox_delivery(&tenant_id, node_attempt_id)
            .await,
        Err(StoreError::OutboxAttemptIdConflict)
    ));
    assert!(matches!(
        upgraded_store
            .claim_outbox_delivery(&tenant_id, AttemptId::generate())
            .await
            .unwrap(),
        OutboxClaimOutcome::Claimed(_)
    ));

    verification_pool.close().await;
    upgraded_store.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .expect("isolated v8 upgrade database must be dropped");
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_nine_quarantines_legacy_waits_without_fabricating_evidence() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v9_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .expect("test administration connection must open");
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated v9 upgrade database must be created");

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("v9 fixture database must initially reach the current schema");
    let fixture_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("v9 fixture store must connect");
    let preserved_tenant = tenant("v9-preserved");
    let preserved_run = RunId::generate();
    fixture_store
        .admit_run(provenance(preserved_tenant.clone(), preserved_run))
        .await
        .unwrap();

    let legacy_tenant = tenant("v9-legacy-wait");
    let legacy_run = RunId::generate();
    let admitted = fixture_store
        .admit_run(provenance(legacy_tenant.clone(), legacy_run))
        .await
        .unwrap();
    let started = fixture_store
        .append_control_plane(
            control_append(
                legacy_tenant.clone(),
                legacy_run,
                EventId::generate(),
                JournalExpectation::empty(),
                750,
            ),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
        )
        .await
        .unwrap();
    let active = fixture_store
        .load_run(&legacy_tenant, legacy_run)
        .await
        .unwrap();
    let wait_event_id = EventId::generate();
    fixture_store
        .append_control_plane_initial_wait_checkpoint(
            control_append(
                legacy_tenant.clone(),
                legacy_run,
                wait_event_id,
                JournalExpectation::exact(started.event().head()),
                751,
            ),
            active.lifecycle().revision(),
            initial_checkpoint_write(legacy_tenant.clone(), legacy_run, CheckpointId::generate()),
            vec![WaitRegistrationIntent::timer(
                TimerRegistrationIntent::new(
                    legacy_tenant.clone(),
                    legacy_run,
                    TimerId::generate(),
                    wait_event_id,
                    RunTimerKind::Sleep,
                    timestamp_after(Duration::from_secs(60)),
                )
                .unwrap(),
            )],
        )
        .await
        .unwrap();
    fixture_store.close().await;

    let fixture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("isolated v9 fixture administration connection must open");
    remove_durable_waits(&fixture_pool).await;
    let legacy_version = query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
        .fetch_one(&fixture_pool)
        .await
        .unwrap();
    assert_eq!(legacy_version, 8);
    let legacy_quarantine = query_scalar::<_, Option<String>>(
        "SELECT quarantine_reason FROM stateknot.runs \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(legacy_tenant.as_str())
    .bind(*legacy_run.as_uuid())
    .fetch_one(&fixture_pool)
    .await
    .unwrap();
    assert!(legacy_quarantine.is_none());
    fixture_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 9 must upgrade the exact v8 fixture");
    let upgraded_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("the upgraded v9 runtime schema must be accepted");
    let preserved = upgraded_store
        .load_run(&preserved_tenant, preserved_run)
        .await
        .unwrap();
    assert!(!preserved.is_quarantined());
    assert_eq!(preserved.unresolved_wait_count(), 0);
    let legacy = upgraded_store
        .load_run(&legacy_tenant, legacy_run)
        .await
        .unwrap();
    assert!(legacy.is_quarantined());
    assert_eq!(legacy.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(legacy.unresolved_wait_count(), 0);
    assert!(legacy.wait_set_digest().is_none());

    let verification_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    let quarantine_reason = query_scalar::<_, String>(
        "SELECT quarantine_reason FROM stateknot.runs \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(legacy_tenant.as_str())
    .bind(*legacy_run.as_uuid())
    .fetch_one(&verification_pool)
    .await
    .unwrap();
    assert_eq!(
        quarantine_reason,
        "migration-9: legacy waiting lifecycle has no durable wait records"
    );
    let durable_evidence = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(legacy_tenant.as_str())
    .bind(*legacy_run.as_uuid())
    .fetch_one(&verification_pool)
    .await
    .unwrap();
    assert_eq!(durable_evidence, 0);
    for index in [
        "run_wait_registrations_due",
        "run_wait_registrations_expiry",
    ] {
        let definition = query_scalar::<_, String>(
            "SELECT indexdef FROM pg_catalog.pg_indexes \
             WHERE schemaname = 'stateknot' AND indexname = $1",
        )
        .bind(index)
        .fetch_one(&verification_pool)
        .await
        .expect("v9 operational wait index must exist");
        assert!(definition.to_ascii_lowercase().contains("where"));
    }
    query("SET enable_seqscan = off")
        .execute(&verification_pool)
        .await
        .unwrap();
    let due_plan = query_scalar::<_, String>(
        "EXPLAIN (COSTS OFF) \
         SELECT tenant_id, due_at, run_id, wait_id \
         FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 AND wait_kind = 'timer' \
           AND status = 'outstanding' AND due_at <= clock_timestamp() \
         ORDER BY due_at, run_id, wait_id LIMIT 1",
    )
    .bind(legacy_tenant.as_str())
    .fetch_all(&verification_pool)
    .await
    .unwrap()
    .join("\n")
    .to_ascii_lowercase();
    query("RESET enable_seqscan")
        .execute(&verification_pool)
        .await
        .unwrap();
    assert!(due_plan.contains("run_wait_registrations_due"));

    verification_pool.close().await;
    upgraded_store.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .expect("isolated v9 upgrade database must be dropped");
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_ten_preserves_legacy_quarantine_without_fabricating_evidence() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v10_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .expect("test administration connection must open");
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .expect("isolated v10 upgrade database must be created");

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("v10 fixture database must initially reach the current schema");
    let fixture_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("v10 fixture store must connect");
    let legacy_tenant = tenant("v10-legacy-quarantine");
    let legacy_run = RunId::generate();
    fixture_store
        .admit_run(provenance(legacy_tenant.clone(), legacy_run))
        .await
        .unwrap();
    let preserved_tenant = tenant("v10-preserved");
    let preserved_run = RunId::generate();
    fixture_store
        .admit_run(provenance(preserved_tenant.clone(), preserved_run))
        .await
        .unwrap();
    fixture_store.close().await;

    let fixture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .expect("isolated v10 fixture administration connection must open");
    query(
        "UPDATE stateknot.runs \
         SET quarantined_at = clock_timestamp(), \
             quarantine_reason = 'legacy operator quarantine without structured evidence' \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(legacy_tenant.as_str())
    .bind(*legacy_run.as_uuid())
    .execute(&fixture_pool)
    .await
    .unwrap();
    remove_run_quarantines(&fixture_pool).await;
    let legacy_version = query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
        .fetch_one(&fixture_pool)
        .await
        .unwrap();
    assert_eq!(legacy_version, 9);
    fixture_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 10 must upgrade the exact v9 fixture");
    let upgraded_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .expect("the upgraded v10 runtime schema must be accepted");
    upgraded_store.verify_schema().await.unwrap();
    assert!(
        upgraded_store
            .load_run(&legacy_tenant, legacy_run)
            .await
            .unwrap()
            .is_quarantined()
    );
    assert!(matches!(
        upgraded_store
            .load_run_quarantine(&legacy_tenant, legacy_run)
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    assert!(
        !upgraded_store
            .load_run(&preserved_tenant, preserved_run)
            .await
            .unwrap()
            .is_quarantined()
    );

    let committed = upgraded_store
        .quarantine_run(quarantine_request(
            preserved_tenant.clone(),
            preserved_run,
            QuarantineId::generate(),
            JournalExpectation::empty(),
            RunQuarantineCause::IntegrityFailure,
            "migration10.post_upgrade",
            b"post-upgrade quarantine evidence",
        ))
        .await
        .expect("post-upgrade structured quarantine must commit");
    assert!(matches!(
        committed,
        RunQuarantineCommitOutcome::Committed(_)
    ));

    let verification_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    let evidence_count = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.run_quarantines WHERE tenant_id = $1",
    )
    .bind(preserved_tenant.as_str())
    .fetch_one(&verification_pool)
    .await
    .unwrap();
    assert_eq!(evidence_count, 1);
    let index_definition = query_scalar::<_, String>(
        "SELECT indexdef FROM pg_catalog.pg_indexes \
         WHERE schemaname = 'stateknot' AND indexname = 'run_quarantines_observed'",
    )
    .fetch_one(&verification_pool)
    .await
    .expect("v10 operational quarantine index must exist")
    .to_ascii_lowercase();
    assert!(index_definition.contains("tenant_id"));
    assert!(index_definition.contains("quarantined_at"));
    assert!(index_definition.contains("run_id"));

    verification_pool.close().await;
    upgraded_store.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .expect("isolated v10 upgrade database must be dropped");
    administration.close().await;
}

fn database_url_with_name(database_url: &str, database_name: &str) -> String {
    let (prefix, current_database) = database_url
        .rsplit_once('/')
        .expect("test PostgreSQL URL must contain a database path");
    let query = current_database
        .find('?')
        .map_or("", |index| &current_database[index..]);
    format!("{prefix}/{database_name}{query}")
}

fn provenance(tenant_id: TenantId, run_id: RunId) -> AgentResultProvenance {
    let owner = PrincipalIdentity::new(
        "https://issuer.example.com/stateknot"
            .parse::<IssuerId>()
            .unwrap(),
        "integration-registry".parse::<SubjectId>().unwrap(),
    );
    let agent = CapabilityIdentity::new(
        owner,
        CapabilityReference::new(
            CapabilityName::new("integration-agent").unwrap(),
            Version::new(1, 0, 0),
        ),
    );
    AgentResultProvenance::new(
        tenant_id,
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        agent,
    )
}

fn tenant(prefix: &str) -> TenantId {
    TenantId::new(format!("{prefix}-{}", RunId::generate())).unwrap()
}

fn scheduler_shard(prefix: &str) -> SchedulerShardId {
    SchedulerShardId::new(format!("{prefix}-{}", RunId::generate())).unwrap()
}

fn quarantine_request(
    tenant_id: TenantId,
    run_id: RunId,
    quarantine_id: QuarantineId,
    expectation: JournalExpectation,
    cause: RunQuarantineCause,
    component: &str,
    evidence: &[u8],
) -> RunQuarantineRequest {
    RunQuarantineRequest::new(
        tenant_id,
        run_id,
        quarantine_id,
        expectation,
        cause,
        RunQuarantineComponent::new(component).unwrap(),
        Digest::sha256(evidence),
    )
    .unwrap()
}

fn payload(index: u64) -> JournalPayload {
    let schema = SchemaReference::new(
        "https://stateknot.github.io/schema/integration-event/1.0.0"
            .parse::<SchemaId>()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(b"stateknot integration event schema v1"),
    );
    JournalPayload::new(
        schema,
        JournalEventKind::new("integration-event").unwrap(),
        BoundedJson::try_from_value(json!({"index": index.to_string()})).unwrap(),
    )
    .unwrap()
}

fn outbox_destination_config(index: u64) -> JournalPayload {
    let schema = SchemaReference::new(
        "https://stateknot.github.io/schema/outbox-destination/1.0.0"
            .parse::<SchemaId>()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(b"stateknot outbox destination schema v1"),
    );
    JournalPayload::new(
        schema,
        JournalEventKind::new("outbox-destination").unwrap(),
        BoundedJson::try_from_value(json!({
            "adapter": "a2a-push-v1",
            "credential_handle": format!("vault://integration/outbox/{index}"),
            "endpoint": format!("https://receiver{index}.example.invalid/a2a/push")
        }))
        .unwrap(),
    )
    .unwrap()
}

fn outbox_destination(tenant_id: &TenantId, index: u64) -> (OutboxDestinationRef, JournalPayload) {
    let config = outbox_destination_config(index);
    let destination = OutboxDestinationRef::new(
        tenant_id.clone(),
        DestinationId::generate(),
        config.digest(),
    );
    (destination, config)
}

fn outbox_payload(index: u64) -> JournalPayload {
    let schema = SchemaReference::new(
        "https://stateknot.github.io/schema/a2a-push/1.0.0"
            .parse::<SchemaId>()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(b"stateknot a2a push schema v1"),
    );
    JournalPayload::new(
        schema,
        JournalEventKind::new("a2a-task-update").unwrap(),
        BoundedJson::try_from_value(json!({
            "state": "completed",
            "task_id": format!("task-{index}")
        }))
        .unwrap(),
    )
    .unwrap()
}

fn timestamp_after(duration: Duration) -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("integration-test clock must be after the Unix epoch");
    let now_micros = i64::try_from(now.as_micros()).expect("current time must fit Timestamp");
    let added_micros = i64::try_from(duration.as_micros()).expect("test duration must fit i64");
    Timestamp::from_unix_micros(now_micros.checked_add(added_micros).unwrap()).unwrap()
}

fn outbox_intent(
    tenant_id: &TenantId,
    run_id: RunId,
    event_id: EventId,
    delivery_id: DeliveryId,
    destination: &OutboxDestinationRef,
    index: u64,
    expires_after: Duration,
) -> OutboxDeliveryIntent {
    OutboxDeliveryIntent::new(
        tenant_id.clone(),
        run_id,
        delivery_id,
        event_id,
        destination.clone(),
        outbox_payload(index),
        timestamp_after(expires_after),
    )
    .unwrap()
}

async fn enqueue_outbox_fixture(
    store: &PostgresStore,
    tenant_id: &TenantId,
    run_id: RunId,
    count: u64,
    expires_after: Duration,
) -> (OutboxDestinationRef, OutboxEnqueueOutcome) {
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let (destination, config) = outbox_destination(tenant_id, count);
    store
        .register_outbox_destination(destination.clone(), config)
        .await
        .unwrap();
    let event_id = EventId::generate();
    let intents = (0..count)
        .map(|index| {
            outbox_intent(
                tenant_id,
                run_id,
                event_id,
                DeliveryId::generate(),
                &destination,
                index,
                expires_after,
            )
        })
        .collect();
    let outcome = store
        .append_control_plane_with_outbox(
            control_append(
                tenant_id.clone(),
                run_id,
                event_id,
                JournalExpectation::empty(),
                count,
            ),
            RunProjection::unchanged(),
            intents,
        )
        .await
        .unwrap();
    (destination, outcome)
}

fn cancellation_request(requested_at: Timestamp) -> RunCancellationRequest {
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Cancelled,
        FailureCode::new("run.cancelled").unwrap(),
        FailureOrigin::new("test.scheduler").unwrap(),
        FailureMessage::new("The integration run was cancelled.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap();
    RunCancellationRequest::new(failure, requested_at).unwrap()
}

fn terminal_run_failure(event_id: EventId, completed_at: Timestamp) -> RunFailure {
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new("run.integration_failed").unwrap(),
        FailureOrigin::new("test.scheduler").unwrap(),
        FailureMessage::new("The integration run failed safely.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(event_id);
    RunFailure::new(failure, completed_at, BudgetUsage::zero()).unwrap()
}

struct InitialWaitPair {
    run_id: RunId,
    interrupt_id: InterruptId,
    timer_id: TimerId,
    commit: WaitCheckpointCommitOutcome,
}

async fn start_initial_wait_pair(
    store: &PostgresStore,
    tenant_id: &TenantId,
    event_seed: u64,
) -> InitialWaitPair {
    let run_id = RunId::generate();
    let admitted = store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let started = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                event_seed,
            ),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
        )
        .await
        .unwrap();
    let active = store.load_run(tenant_id, run_id).await.unwrap();
    let wait_event_id = EventId::generate();
    let interrupt_id = InterruptId::generate();
    let timer_id = TimerId::generate();
    let registrations = vec![
        WaitRegistrationIntent::interrupt(
            InterruptRequestIntent::new(
                tenant_id.clone(),
                run_id,
                interrupt_id,
                wait_event_id,
                RunInterruptKind::Approval,
                payload(event_seed + 1),
                Digest::sha256(format!("wait-action-{event_seed}").as_bytes()),
                None,
                ScopeSet::empty(),
                Some(timestamp_after(Duration::from_secs(3_600))),
            )
            .unwrap(),
        ),
        WaitRegistrationIntent::timer(
            TimerRegistrationIntent::new(
                tenant_id.clone(),
                run_id,
                timer_id,
                wait_event_id,
                RunTimerKind::Sleep,
                timestamp_after(Duration::from_secs(1_800)),
            )
            .unwrap(),
        ),
    ];
    let commit = store
        .append_control_plane_initial_wait_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                wait_event_id,
                JournalExpectation::exact(started.event().head()),
                event_seed + 2,
            ),
            active.lifecycle().revision(),
            initial_checkpoint_write(tenant_id.clone(), run_id, CheckpointId::generate()),
            registrations,
        )
        .await
        .unwrap();
    InitialWaitPair {
        run_id,
        interrupt_id,
        timer_id,
        commit,
    }
}

fn outbox_failure(index: u64, retry_advice: RetryAdvice) -> Failure {
    let category = if matches!(retry_advice, RetryAdvice::ReconcileFirst) {
        FailureCategory::AmbiguousExternalOutcome
    } else {
        FailureCategory::DependencyUnavailable
    };
    Failure::new(
        FailureId::generate(),
        category,
        FailureCode::new(format!("outbox.delivery-{index}")).unwrap(),
        FailureOrigin::new("test.outbox-adapter").unwrap(),
        FailureMessage::new("The test destination did not acknowledge delivery.").unwrap(),
        retry_advice,
    )
    .unwrap()
}

fn checkpoint_capability(name: &str) -> CapabilityIdentity {
    let owner = PrincipalIdentity::new(
        "https://issuer.example.com/stateknot"
            .parse::<IssuerId>()
            .unwrap(),
        "checkpoint-registry".parse::<SubjectId>().unwrap(),
    );
    CapabilityIdentity::new(
        owner,
        CapabilityReference::new(CapabilityName::new(name).unwrap(), Version::new(1, 0, 0)),
    )
}

fn checkpoint_schema(name: &str) -> SchemaReference {
    SchemaReference::new(
        format!("https://stateknot.github.io/schema/{name}/1.0.0")
            .parse::<SchemaId>()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(format!("stateknot integration {name} schema v1")),
    )
}

fn checkpoint_state_schema() -> SchemaReference {
    SchemaReference::new(
        "https://stateknot.github.io/schema/integration-state/1.0.0"
            .parse::<SchemaId>()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(b"stateknot integration checkpoint state schema v1"),
    )
}

fn checkpoint_compiled_graph() -> CompiledGraph {
    static GRAPH: LazyLock<CompiledGraph> = LazyLock::new(|| build_checkpoint_compiled_graph(64));
    GRAPH.clone()
}

fn build_checkpoint_compiled_graph(maximum_supersteps: u64) -> CompiledGraph {
    let nodes = (1..=64_u64).map(|index| {
        let node_id = NodeId::new(format!("node-{index:04}")).unwrap();
        let continue_to = (index < 64).then(|| ready_node(index + 1));
        GraphNode::new(
            node_id,
            continue_to,
            GraphRoutes::empty(),
            None,
            index == 64,
        )
        .unwrap()
    });
    CompiledGraph::compile(
        checkpoint_capability("integration-workflow"),
        checkpoint_schema("integration-input"),
        checkpoint_state_schema(),
        checkpoint_schema("integration-update"),
        checkpoint_schema("integration-output"),
        GraphReducerReference::new(
            checkpoint_capability("integration-reducer"),
            Digest::sha256(b"stateknot integration reducer v1"),
        ),
        ready_node(1),
        nodes,
        GraphExecutionLimits::new(Superstep::new(maximum_supersteps).unwrap(), 1).unwrap(),
    )
    .unwrap()
}

fn checkpoint_graph() -> GraphReference {
    checkpoint_compiled_graph().reference()
}

struct AcceptGraphSchemas;

impl GraphSchemaValidator for AcceptGraphSchemas {
    fn validate(
        &self,
        _: &SchemaReference,
        _: &BoundedJson,
    ) -> Result<(), GraphSchemaValidationError> {
        Ok(())
    }
}

fn agent_fixture_definition() -> AgentDescriptor {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["descriptors"]["valid"][0].clone()).unwrap()
}

fn agent_fixture_request_and_budget() -> (AgentRequest, BudgetLimits) {
    let mut fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-runtime-v1.json"
    ))
    .unwrap();
    let deadline = timestamp_after(Duration::from_secs(3_600)).to_string();
    fixture["requests"]["valid"][0]["budget_limits"]["deadline"] = json!(deadline);
    fixture["base_budget_layers"][0]["deadline"] = json!(deadline);
    (
        serde_json::from_value(fixture["requests"]["valid"][0].clone()).unwrap(),
        serde_json::from_value(fixture["base_budget_layers"][0].clone()).unwrap(),
    )
}

fn agent_admission_fixture(
    tenant_id: TenantId,
    run_id: RunId,
) -> (AgentAdmissionIntent, JournalAppend, CheckpointWrite) {
    let descriptor = agent_fixture_definition();
    let provenance = AgentResultProvenance::for_agent(
        tenant_id.clone(),
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        &descriptor,
    );
    let (request, limits) = agent_fixture_request_and_budget();
    let graph = checkpoint_graph();
    let policy = checkpoint_capability("agent-admission-policy");
    let evidence = JournalPayload::new(
        checkpoint_schema("agent-admission-evidence"),
        JournalEventKind::new(AgentAdmissionAuthority::EVIDENCE_KIND).unwrap(),
        BoundedJson::try_from_value(json!({"decision": "allow"})).unwrap(),
    )
    .unwrap();
    let authority = AgentAdmissionAuthority::new(
        policy.owner().clone(),
        ScopeSet::empty(),
        policy,
        Digest::sha256(b"integration Agent admission policy v1"),
        evidence,
    )
    .unwrap();
    let budget_layer = AgentAdmissionBudgetLayer::new(
        checkpoint_capability("agent-admission-budget"),
        authority.evidence().digest(),
        limits,
    )
    .unwrap();
    let intent = AgentAdmissionIntent::new(
        provenance,
        descriptor,
        request,
        [budget_layer],
        graph.clone(),
        authority,
    )
    .unwrap();
    let payload = JournalPayload::new(
        checkpoint_schema("agent-admission-event"),
        JournalEventKind::new(AgentAdmission::JOURNAL_EVENT_KIND).unwrap(),
        BoundedJson::try_from_value(json!({
            "intent_digest": intent.intent_digest().to_string()
        }))
        .unwrap(),
    )
    .unwrap();
    let event =
        JournalEventIntent::control_plane(tenant_id.clone(), run_id, EventId::generate(), payload)
            .unwrap();
    let append = JournalAppend::new(JournalExpectation::empty(), event).unwrap();
    let checkpoint = CheckpointWrite::initial(
        tenant_id,
        run_id,
        CheckpointId::generate(),
        graph.clone(),
        checkpoint_state(&graph, 0),
        checkpoint_compiled_graph().entry_nodes().clone(),
    )
    .unwrap();
    (intent, append, checkpoint)
}

fn agent_submission_retry_fixture(
    tenant_id: TenantId,
    run_id: RunId,
    template_intent: &AgentAdmissionIntent,
    template_checkpoint: &CheckpointWrite,
    request: AgentRequest,
) -> (AgentAdmissionIntent, JournalAppend, CheckpointWrite) {
    let descriptor = template_intent.descriptor().clone();
    let provenance = AgentResultProvenance::for_agent(
        tenant_id.clone(),
        run_id,
        ThreadId::generate(),
        InvocationId::generate(),
        &descriptor,
    );
    let intent = AgentAdmissionIntent::new(
        provenance,
        descriptor,
        request,
        template_intent.budget_layers().iter().cloned(),
        template_intent.graph().clone(),
        template_intent.authority().clone(),
    )
    .unwrap();
    let payload = JournalPayload::new(
        checkpoint_schema("agent-admission-event"),
        JournalEventKind::new(AgentAdmission::JOURNAL_EVENT_KIND).unwrap(),
        BoundedJson::try_from_value(json!({
            "intent_digest": intent.intent_digest().to_string()
        }))
        .unwrap(),
    )
    .unwrap();
    let event =
        JournalEventIntent::control_plane(tenant_id.clone(), run_id, EventId::generate(), payload)
            .unwrap();
    let append = JournalAppend::new(JournalExpectation::empty(), event).unwrap();
    let checkpoint = CheckpointWrite::initial(
        tenant_id,
        run_id,
        CheckpointId::generate(),
        template_intent.graph().clone(),
        template_checkpoint.state().clone(),
        template_checkpoint.ready_nodes().clone(),
    )
    .unwrap();
    (intent, append, checkpoint)
}

async fn admit_atomic_agent_fixture(
    store: &PostgresStore,
    tenant_id: &TenantId,
    run_id: RunId,
) -> StoredAgentAdmission {
    store
        .register_graph_definition(tenant_id.clone(), checkpoint_compiled_graph())
        .await
        .unwrap();
    let (intent, append, checkpoint) = agent_admission_fixture(tenant_id.clone(), run_id);
    match Box::pin(store.admit_agent_run(intent, append, checkpoint, &AcceptGraphSchemas))
        .await
        .unwrap()
    {
        AgentAdmissionCommitOutcome::Committed(stored)
        | AgentAdmissionCommitOutcome::Idempotent(stored) => stored,
        _ => panic!("unsupported Agent admission outcome"),
    }
}

struct IntegrationGraphReducer {
    reference: GraphReducerReference,
}

impl IntegrationGraphReducer {
    fn new() -> Self {
        Self {
            reference: checkpoint_compiled_graph().reducer().clone(),
        }
    }
}

impl GraphReducer for IntegrationGraphReducer {
    fn reference(&self) -> &GraphReducerReference {
        &self.reference
    }

    fn reduce(
        &self,
        state: &BoundedJson,
        _: &[GraphReducerInput<'_>],
    ) -> Result<BoundedJson, GraphReducerError> {
        Ok(state.clone())
    }
}

fn checkpoint_state(graph: &GraphReference, index: u64) -> CheckpointState {
    CheckpointState::new(
        graph.state_schema().clone(),
        BoundedJson::try_from_value(json!({
            "completed_supersteps": index.to_string(),
            "status": "durable"
        }))
        .unwrap(),
    )
    .unwrap()
}

fn ready_node(index: u64) -> ReadyNodes {
    ReadyNodes::try_new([NodeId::new(format!("node-{index:04}")).unwrap()]).unwrap()
}

fn initial_checkpoint_write(
    tenant_id: TenantId,
    run_id: RunId,
    checkpoint_id: CheckpointId,
) -> CheckpointWrite {
    let graph = checkpoint_graph();
    CheckpointWrite::initial(
        tenant_id,
        run_id,
        checkpoint_id,
        graph.clone(),
        checkpoint_state(&graph, 0),
        ready_node(1),
    )
    .unwrap()
}

fn successor_checkpoint_write(
    checkpoint_id: CheckpointId,
    parent: &Checkpoint,
    index: u64,
) -> CheckpointWrite {
    CheckpointWrite::successor(
        checkpoint_id,
        parent,
        checkpoint_state(parent.graph(), index),
        ready_node(index + 1),
    )
    .unwrap()
}

async fn start_run_with_checkpoint(
    store: &PostgresStore,
    tenant_id: &TenantId,
    run_id: RunId,
    event_index: u64,
) -> CheckpointCommitOutcome {
    let registration = store
        .register_graph_definition(tenant_id.clone(), checkpoint_compiled_graph())
        .await
        .unwrap();
    assert!(matches!(
        registration,
        GraphDefinitionRegistrationOutcome::Registered(_)
            | GraphDefinitionRegistrationOutcome::Idempotent(_)
    ));
    let admitted = store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let lifecycle = admitted.lifecycle();
    store
        .append_control_plane_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                event_index,
            ),
            RunProjection::transition(
                lifecycle.revision(),
                RunTransition::Start {
                    started_at: lifecycle.admitted_at(),
                },
            ),
            initial_checkpoint_write(tenant_id.clone(), run_id, CheckpointId::generate()),
        )
        .await
        .unwrap()
}

async fn start_run_with_ready_checkpoint(
    store: &PostgresStore,
    tenant_id: &TenantId,
    run_id: RunId,
    event_index: u64,
    ready_nodes: ReadyNodes,
) -> CheckpointCommitOutcome {
    let admitted = store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let graph = checkpoint_graph();
    let write = CheckpointWrite::initial(
        tenant_id.clone(),
        run_id,
        CheckpointId::generate(),
        graph.clone(),
        checkpoint_state(&graph, 0),
        ready_nodes,
    )
    .unwrap();
    store
        .append_control_plane_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                event_index,
            ),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
            write,
        )
        .await
        .unwrap()
}

fn tool_descriptor() -> ToolDescriptor {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-tool-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["descriptors"]["valid"][0].clone()).unwrap()
}

fn tool_input(descriptor: &ToolDescriptor) -> ToolInput {
    ToolInput::new(
        descriptor.input_schema().clone(),
        BoundedJson::try_from_value(json!({
            "amount": 42,
            "currency": "CNY"
        }))
        .unwrap(),
    )
    .unwrap()
}

fn tool_invocation_intent(
    checkpoint: &Checkpoint,
    invocation_id: InvocationId,
) -> ToolInvocationIntent {
    tool_invocation_intent_for_activation(pending_activation(checkpoint, &[]), invocation_id)
}

fn tool_invocation_intent_for_activation(
    activation: NodeActivation,
    invocation_id: InvocationId,
) -> ToolInvocationIntent {
    let descriptor = tool_descriptor();
    ToolInvocationIntent::new(
        activation,
        invocation_id,
        descriptor.clone(),
        tool_input(&descriptor),
        descriptor.limits().clone(),
    )
    .unwrap()
}

fn tool_result(intent: &ToolInvocationIntent, attempt_id: AttemptId) -> ToolResult {
    ToolResult::new(
        ToolResultProvenance::new(
            intent.invocation_id(),
            attempt_id,
            intent.descriptor().metadata().identity().clone(),
        ),
        intent.descriptor().output_schema().clone(),
        BoundedJson::try_from_value(json!({
            "accepted": true,
            "transaction_id": "txn-integration"
        }))
        .unwrap(),
        ToolArtifacts::empty(),
    )
}

fn model_descriptor() -> ModelDescriptor {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-agent-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["descriptors"]["valid"][0]["model"].clone()).unwrap()
}

fn model_request() -> ModelRequest {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-model-request-v1.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["requests"]["valid"][0].clone()).unwrap()
}

fn model_invocation_intent(
    checkpoint: &Checkpoint,
    invocation_id: InvocationId,
) -> ModelInvocationIntent {
    model_invocation_intent_for_activation(pending_activation(checkpoint, &[]), invocation_id)
}

fn model_invocation_intent_for_activation(
    activation: NodeActivation,
    invocation_id: InvocationId,
) -> ModelInvocationIntent {
    ModelInvocationIntent::new(
        activation,
        invocation_id,
        model_descriptor(),
        model_request(),
    )
    .unwrap()
}

fn model_response(intent: &ModelInvocationIntent, attempt_id: AttemptId) -> ModelResponse {
    let mut fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../stateknot-core/tests/fixtures/core-model-response-v1.json"
    ))
    .unwrap();
    let mut value = fixture["responses"]["valid"][0].take();
    value["provenance"]["attempt_id"] = serde_json::to_value(attempt_id).unwrap();
    value["provenance"]["model"] =
        serde_json::to_value(intent.descriptor().metadata().identity()).unwrap();
    let response = serde_json::from_value::<ModelResponse>(value).unwrap();
    response
        .validate_for(intent.descriptor(), intent.request())
        .unwrap();
    response
}

fn model_error(
    intent: &ModelInvocationIntent,
    attempt_id: AttemptId,
    retry_advice: RetryAdvice,
) -> ModelError {
    ModelError::new(
        Failure::new(
            FailureId::generate(),
            FailureCategory::DependencyUnavailable,
            FailureCode::new("model.dependency_unavailable").unwrap(),
            FailureOrigin::new("model.integration").unwrap(),
            FailureMessage::new("The integration model is temporarily unavailable.").unwrap(),
            retry_advice,
        )
        .unwrap(),
        ModelErrorPhase::Dispatch,
        ModelErrorProvenance::new(
            attempt_id,
            intent.descriptor().metadata().identity().clone(),
            None,
            None,
            None,
        ),
        None,
    )
}

fn pending_activation(checkpoint: &Checkpoint, _input: &[u8]) -> NodeActivation {
    NodeActivation::for_ready_root(
        checkpoint,
        checkpoint
            .ready_nodes()
            .iter()
            .next()
            .expect("integration checkpoint must have a ready node")
            .clone(),
    )
    .unwrap()
}

fn drifted_pending_activation(checkpoint: &Checkpoint, input: &[u8]) -> NodeActivation {
    NodeActivation::new(
        checkpoint.head(),
        GraphNamespace::root(),
        checkpoint
            .ready_nodes()
            .iter()
            .next()
            .expect("integration checkpoint must have a ready node")
            .clone(),
        Digest::sha256(input),
    )
}

fn pending_result_intent(
    activation: NodeActivation,
    bindings: NodeInvocationBindings,
) -> PendingNodeResultIntent {
    PendingNodeResultIntent::new(
        activation,
        NodeStateChange::Unchanged,
        NodeControl::Continue,
        bindings,
    )
    .unwrap()
}

async fn commit_ready_results(
    store: &PostgresStore,
    checkpoint: &Checkpoint,
    fence: &stateknot_core::RunFence,
    first_event_index: u64,
) -> (Vec<PendingNodeResultHead>, JournalHead) {
    let mut journal_head = checkpoint.journal_head().clone();
    let mut result_heads = Vec::with_capacity(checkpoint.ready_nodes().len());
    for (offset, node_id) in checkpoint.ready_nodes().iter().cloned().enumerate() {
        let activation = NodeActivation::for_ready_root(checkpoint, node_id).unwrap();
        let committed = store
            .commit_test_pending_node_result(
                worker_append(
                    checkpoint.tenant_id().clone(),
                    checkpoint.run_id(),
                    EventId::generate(),
                    JournalExpectation::exact(journal_head),
                    fence.clone(),
                    first_event_index + u64::try_from(offset).unwrap(),
                ),
                pending_result_intent(activation, NodeInvocationBindings::empty()),
            )
            .await
            .unwrap();
        journal_head = committed.event().head();
        result_heads.push(committed.result().head());
    }
    (result_heads, journal_head)
}

async fn prepare_tool_invocation_fixture(
    store: &PostgresStore,
    tenant_prefix: &str,
    event_index: u64,
) -> (TenantId, RunId, InvocationId, ToolInvocationCommitOutcome) {
    let tenant_id = tenant(tenant_prefix);
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(
        store,
        &tenant_id,
        run_id,
        event_index,
    ))
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let invocation_id = InvocationId::generate();
    let prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                event_index + 1,
            ),
            tool_invocation_intent(checkpoint.checkpoint(), invocation_id),
        )
        .await
        .unwrap();
    (tenant_id, run_id, invocation_id, prepared)
}

async fn prepare_model_invocation_fixture(
    store: &PostgresStore,
    tenant_prefix: &str,
    event_index: u64,
) -> (TenantId, RunId, InvocationId, ModelInvocationCommitOutcome) {
    let tenant_id = tenant(tenant_prefix);
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(
        store,
        &tenant_id,
        run_id,
        event_index,
    ))
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let invocation_id = InvocationId::generate();
    let prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                event_index + 1,
            ),
            model_invocation_intent(checkpoint.checkpoint(), invocation_id),
        )
        .await
        .unwrap();
    (tenant_id, run_id, invocation_id, prepared)
}

fn worker_append(
    tenant_id: TenantId,
    run_id: RunId,
    event_id: EventId,
    expectation: JournalExpectation,
    fence: stateknot_core::RunFence,
    index: u64,
) -> JournalAppend {
    JournalAppend::new(
        expectation,
        JournalEventIntent::worker(tenant_id, run_id, event_id, fence, payload(index)).unwrap(),
    )
    .unwrap()
}

fn control_append(
    tenant_id: TenantId,
    run_id: RunId,
    event_id: EventId,
    expectation: JournalExpectation,
    index: u64,
) -> JournalAppend {
    let intent =
        JournalEventIntent::control_plane(tenant_id, run_id, event_id, payload(index)).unwrap();
    JournalAppend::new(expectation, intent).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn outbox_destination_and_atomic_enqueue_are_fail_closed_and_idempotent() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("outbox-enqueue");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    let (destination, config) = outbox_destination(&tenant_id, 1);
    let mismatched = OutboxDestinationRef::new(
        tenant_id.clone(),
        destination.destination_id(),
        Digest::sha256(b"not the destination configuration"),
    );
    assert!(matches!(
        store
            .register_outbox_destination(mismatched, config.clone())
            .await,
        Err(StoreError::OutboxDestinationSnapshotMismatch)
    ));
    let registered = store
        .register_outbox_destination(destination.clone(), config.clone())
        .await
        .unwrap();
    assert!(matches!(
        registered,
        OutboxDestinationRegistrationOutcome::Registered(_)
    ));
    assert_eq!(registered.destination().destination(), &destination);
    assert_eq!(registered.destination().config(), &config);
    let idempotent_destination = store
        .register_outbox_destination(destination.clone(), config.clone())
        .await
        .unwrap();
    assert!(matches!(
        idempotent_destination,
        OutboxDestinationRegistrationOutcome::Idempotent(_)
    ));
    assert_eq!(
        store.load_outbox_destination(&destination).await.unwrap(),
        registered.destination().clone()
    );
    let crossed_destination = OutboxDestinationRef::new(
        tenant("outbox-crossed-destination"),
        destination.destination_id(),
        destination.snapshot_digest(),
    );
    assert!(matches!(
        store.load_outbox_destination(&crossed_destination).await,
        Err(StoreError::OutboxDestinationNotFound)
    ));

    let invalid_batch_event = EventId::generate();
    let invalid_batch_append = || {
        control_append(
            tenant_id.clone(),
            run_id,
            invalid_batch_event,
            JournalExpectation::empty(),
            9,
        )
    };
    assert!(matches!(
        store
            .append_control_plane_with_outbox(
                invalid_batch_append(),
                RunProjection::unchanged(),
                Vec::new(),
            )
            .await,
        Err(StoreError::InvalidOutboxBatch)
    ));
    let duplicated = outbox_intent(
        &tenant_id,
        run_id,
        invalid_batch_event,
        DeliveryId::generate(),
        &destination,
        9,
        Duration::from_secs(60),
    );
    assert!(matches!(
        store
            .append_control_plane_with_outbox(
                invalid_batch_append(),
                RunProjection::unchanged(),
                vec![duplicated.clone(), duplicated],
            )
            .await,
        Err(StoreError::InvalidOutboxBatch)
    ));
    let wrong_origin = outbox_intent(
        &tenant_id,
        run_id,
        EventId::generate(),
        DeliveryId::generate(),
        &destination,
        9,
        Duration::from_secs(60),
    );
    assert!(matches!(
        store
            .append_control_plane_with_outbox(
                invalid_batch_append(),
                RunProjection::unchanged(),
                vec![wrong_origin],
            )
            .await,
        Err(StoreError::InvalidOutboxBatch)
    ));
    let oversized = (0..65)
        .map(|index| {
            outbox_intent(
                &tenant_id,
                run_id,
                invalid_batch_event,
                DeliveryId::generate(),
                &destination,
                index,
                Duration::from_secs(60),
            )
        })
        .collect();
    assert!(matches!(
        store
            .append_control_plane_with_outbox(
                invalid_batch_append(),
                RunProjection::unchanged(),
                oversized,
            )
            .await,
        Err(StoreError::InvalidOutboxBatch)
    ));

    let missing_event_id = EventId::generate();
    let missing_destination = OutboxDestinationRef::new(
        tenant_id.clone(),
        DestinationId::generate(),
        Digest::sha256(b"missing destination snapshot"),
    );
    let missing_intent = outbox_intent(
        &tenant_id,
        run_id,
        missing_event_id,
        DeliveryId::generate(),
        &missing_destination,
        10,
        Duration::from_secs(60),
    );
    assert!(matches!(
        store
            .append_control_plane_with_outbox(
                control_append(
                    tenant_id.clone(),
                    run_id,
                    missing_event_id,
                    JournalExpectation::empty(),
                    10,
                ),
                RunProjection::unchanged(),
                vec![missing_intent],
            )
            .await,
        Err(StoreError::OutboxDestinationNotFound)
    ));
    assert!(
        store
            .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(4).unwrap())
            .await
            .unwrap()
            .events()
            .is_empty()
    );

    let event_id = EventId::generate();
    let first_delivery_id = DeliveryId::generate();
    let second_delivery_id = DeliveryId::generate();
    let first_intent = outbox_intent(
        &tenant_id,
        run_id,
        event_id,
        first_delivery_id,
        &destination,
        11,
        Duration::from_secs(60),
    );
    let second_intent = outbox_intent(
        &tenant_id,
        run_id,
        event_id,
        second_delivery_id,
        &destination,
        12,
        Duration::from_secs(60),
    );
    let enqueue = || {
        control_append(
            tenant_id.clone(),
            run_id,
            event_id,
            JournalExpectation::empty(),
            11,
        )
    };
    let committed = store
        .append_control_plane_with_outbox(
            enqueue(),
            RunProjection::unchanged(),
            vec![first_intent.clone(), second_intent.clone()],
        )
        .await
        .unwrap();
    assert!(matches!(committed, OutboxEnqueueOutcome::Committed { .. }));
    assert_eq!(committed.deliveries().len(), 2);
    for delivery in committed.deliveries() {
        assert_eq!(
            store
                .load_outbox_delivery(&tenant_id, run_id, delivery.intent().delivery_id())
                .await
                .unwrap(),
            *delivery
        );
        assert_eq!(delivery.origin(), &committed.event().head());
    }

    let lost_ack = store
        .append_control_plane_with_outbox(
            enqueue(),
            RunProjection::unchanged(),
            vec![first_intent.clone(), second_intent.clone()],
        )
        .await
        .unwrap();
    assert!(matches!(lost_ack, OutboxEnqueueOutcome::Idempotent { .. }));
    assert_eq!(lost_ack.event(), committed.event());
    assert_eq!(lost_ack.deliveries(), committed.deliveries());
    assert!(matches!(
        store
            .append_control_plane_with_outbox(
                enqueue(),
                RunProjection::unchanged(),
                vec![first_intent],
            )
            .await,
        Err(StoreError::OutboxEnqueueConflict)
    ));

    let next_event_id = EventId::generate();
    let reused_delivery = outbox_intent(
        &tenant_id,
        run_id,
        next_event_id,
        first_delivery_id,
        &destination,
        13,
        Duration::from_secs(60),
    );
    assert!(matches!(
        store
            .append_control_plane_with_outbox(
                control_append(
                    tenant_id.clone(),
                    run_id,
                    next_event_id,
                    JournalExpectation::exact(committed.event().head()),
                    13,
                ),
                RunProjection::unchanged(),
                vec![reused_delivery],
            )
            .await,
        Err(StoreError::OutboxDeliveryIdConflict)
    ));
    assert_eq!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .journal_head(),
        Some(&committed.event().head())
    );

    query("ALTER TABLE stateknot.runs DROP CONSTRAINT IF EXISTS test_outbox_atomic_rollback")
        .execute(&administration)
        .await
        .unwrap();
    query(&format!(
        "ALTER TABLE stateknot.runs ADD CONSTRAINT test_outbox_atomic_rollback \
         CHECK (tenant_id <> '{}' OR journal_sequence <= 1) NOT VALID",
        tenant_id.as_str()
    ))
    .execute(&administration)
    .await
    .unwrap();
    let rollback_event_id = EventId::generate();
    let rollback_delivery_id = DeliveryId::generate();
    let rollback_result = store
        .append_control_plane_with_outbox(
            control_append(
                tenant_id.clone(),
                run_id,
                rollback_event_id,
                JournalExpectation::exact(committed.event().head()),
                14,
            ),
            RunProjection::unchanged(),
            vec![outbox_intent(
                &tenant_id,
                run_id,
                rollback_event_id,
                rollback_delivery_id,
                &destination,
                14,
                Duration::from_secs(60),
            )],
        )
        .await;
    query("ALTER TABLE stateknot.runs DROP CONSTRAINT test_outbox_atomic_rollback")
        .execute(&administration)
        .await
        .unwrap();
    assert!(matches!(rollback_result, Err(StoreError::Database { .. })));
    assert!(matches!(
        store
            .load_outbox_delivery(&tenant_id, run_id, rollback_delivery_id)
            .await,
        Err(StoreError::OutboxDeliveryNotFound)
    ));
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(4).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events(), &[committed.event().clone()]);

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_outbox_enqueue_is_fenced_but_preserves_committed_lost_ack_retries() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(30)).await else {
        return;
    };
    let tenant_id = tenant("outbox-worker-fence");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (destination, config) = outbox_destination(&tenant_id, 20);
    store
        .register_outbox_destination(destination.clone(), config)
        .await
        .unwrap();
    let event_id = EventId::generate();
    let delivery_id = DeliveryId::generate();
    let delivery = outbox_intent(
        &tenant_id,
        run_id,
        event_id,
        delivery_id,
        &destination,
        20,
        Duration::from_secs(60),
    );
    let first_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            event_id,
            JournalExpectation::empty(),
            first_lease.fence().clone(),
            20,
        )
    };
    let committed = store
        .append_worker_with_outbox(
            first_append(),
            RunProjection::unchanged(),
            vec![delivery.clone()],
        )
        .await
        .unwrap();
    assert!(matches!(committed, OutboxEnqueueOutcome::Committed { .. }));

    let current_lease = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let lost_ack = store
        .append_worker_with_outbox(first_append(), RunProjection::unchanged(), vec![delivery])
        .await
        .expect("a committed event must remain recoverable after fence takeover");
    assert!(matches!(lost_ack, OutboxEnqueueOutcome::Idempotent { .. }));
    assert_eq!(lost_ack.event(), committed.event());

    let stale_event_id = EventId::generate();
    let stale_delivery_id = DeliveryId::generate();
    assert!(matches!(
        store
            .append_worker_with_outbox(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    stale_event_id,
                    JournalExpectation::exact(committed.event().head()),
                    first_lease.fence().clone(),
                    21,
                ),
                RunProjection::unchanged(),
                vec![outbox_intent(
                    &tenant_id,
                    run_id,
                    stale_event_id,
                    stale_delivery_id,
                    &destination,
                    21,
                    Duration::from_secs(60),
                )],
            )
            .await,
        Err(StoreError::StaleFence)
    ));
    assert!(matches!(
        store
            .load_outbox_delivery(&tenant_id, run_id, stale_delivery_id)
            .await,
        Err(StoreError::OutboxDeliveryNotFound)
    ));

    let current_event_id = EventId::generate();
    let current_delivery_id = DeliveryId::generate();
    let current = store
        .append_worker_with_outbox(
            worker_append(
                tenant_id.clone(),
                run_id,
                current_event_id,
                JournalExpectation::exact(committed.event().head()),
                current_lease.fence().clone(),
                22,
            ),
            RunProjection::unchanged(),
            vec![outbox_intent(
                &tenant_id,
                run_id,
                current_event_id,
                current_delivery_id,
                &destination,
                22,
                Duration::from_secs(60),
            )],
        )
        .await
        .unwrap();
    assert_eq!(current.event().sequence().get(), 2);
    assert_eq!(
        store
            .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(4).unwrap())
            .await
            .unwrap()
            .events()
            .len(),
        2
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn concurrent_outbox_claims_are_unique_durable_and_lost_ack_safe() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("outbox-concurrent-claim");
    let run_id = RunId::generate();
    let (_, enqueued) =
        enqueue_outbox_fixture(&store, &tenant_id, run_id, 24, Duration::from_secs(120)).await;
    let expected_ids = enqueued
        .deliveries()
        .iter()
        .map(|delivery| delivery.intent().delivery_id())
        .collect::<BTreeSet<_>>();

    let mut tasks = Vec::new();
    for _ in 0..24 {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        let attempt_id = AttemptId::generate();
        tasks.push(tokio::spawn(async move {
            (
                attempt_id,
                store.claim_outbox_delivery(&tenant_id, attempt_id).await,
            )
        }));
    }
    let mut claims = Vec::new();
    for task in tasks {
        let (attempt_id, outcome) = task.await.unwrap();
        match outcome.unwrap() {
            OutboxClaimOutcome::Claimed(claim) => {
                assert_eq!(claim.fence().attempt_id(), attempt_id);
                assert_eq!(claim.fence().tenant_id(), &tenant_id);
                assert_eq!(claim.destination().destination().tenant_id(), &tenant_id);
                claims.push(claim);
            }
            other => panic!("fresh concurrent attempt must claim one row: {other:?}"),
        }
    }
    let claimed_ids = claims
        .iter()
        .map(|claim| claim.delivery().intent().delivery_id())
        .collect::<BTreeSet<_>>();
    assert_eq!(claimed_ids, expected_ids);
    assert_eq!(claims.len(), 24);

    let first = claims.first().unwrap().clone();
    let durable_before_dispatch = query_scalar::<_, bool>(
        "SELECT EXISTS ( \
             SELECT 1 \
             FROM stateknot.outbox_attempts AS attempt \
             JOIN stateknot.run_attempt_claims AS claim \
               ON claim.tenant_id = attempt.tenant_id \
              AND claim.run_id = attempt.run_id \
              AND claim.attempt_id = attempt.attempt_id \
             JOIN stateknot.outbox_deliveries AS delivery \
               ON delivery.tenant_id = attempt.tenant_id \
              AND delivery.run_id = attempt.run_id \
              AND delivery.delivery_id = attempt.delivery_id \
             WHERE attempt.tenant_id = $1 \
               AND attempt.run_id = $2 \
               AND attempt.attempt_id = $3 \
               AND claim.claim_kind = 'outbox_attempt' \
               AND delivery.status = 'delivering' \
         )",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*first.fence().attempt_id().as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert!(durable_before_dispatch);

    let recovered = store
        .claim_outbox_delivery(&tenant_id, first.fence().attempt_id())
        .await
        .unwrap();
    let recovered = match recovered {
        OutboxClaimOutcome::Idempotent(claim) => claim,
        other => panic!("live claim retry must recover the exact start: {other:?}"),
    };
    assert_eq!(recovered.start(), first.start());
    assert_eq!(recovered.delivery(), first.delivery());

    let evidence = Digest::sha256(b"bounded protocol acknowledgement");
    let acknowledged = store
        .acknowledge_outbox_attempt(first.fence(), Some(evidence))
        .await
        .unwrap();
    assert!(matches!(
        acknowledged,
        OutboxCompletionOutcome::Committed { .. }
    ));
    let completion_digest = acknowledged.completion().unwrap().digest();
    let lost_ack = store
        .acknowledge_outbox_attempt(first.fence(), Some(evidence))
        .await
        .unwrap();
    assert!(matches!(
        lost_ack,
        OutboxCompletionOutcome::Idempotent { .. }
    ));
    assert_eq!(lost_ack.completion().unwrap().digest(), completion_digest);
    assert!(matches!(
        store
            .acknowledge_outbox_attempt(
                first.fence(),
                Some(Digest::sha256(b"conflicting acknowledgement evidence")),
            )
            .await,
        Err(StoreError::OutboxCompletionConflict)
    ));
    assert!(matches!(
        store
            .claim_outbox_delivery(&tenant_id, first.fence().attempt_id())
            .await,
        Err(StoreError::StaleOutboxFence)
    ));

    assert!(matches!(
        store
            .claim_outbox_delivery(&tenant_id, AttemptId::generate())
            .await
            .unwrap(),
        OutboxClaimOutcome::NoWork
    ));
    assert!(matches!(
        store
            .claim_outbox_delivery(&tenant("outbox-isolated-claim"), AttemptId::generate(),)
            .await
            .unwrap(),
        OutboxClaimOutcome::NoWork
    ));

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn outbox_retry_takeover_dead_letter_expiry_and_history_follow_database_time() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_outbox_attempt_lease(Duration::from_secs(2)).await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("outbox-retry");
    let run_id = RunId::generate();
    let (_, enqueued) =
        enqueue_outbox_fixture(&store, &tenant_id, run_id, 1, Duration::from_secs(10)).await;
    let delivery_id = enqueued.deliveries()[0].intent().delivery_id();

    let first_attempt_id = AttemptId::generate();
    let first = match store
        .claim_outbox_delivery(&tenant_id, first_attempt_id)
        .await
        .unwrap()
    {
        OutboxClaimOutcome::Claimed(claim) => claim,
        other => panic!("first delivery attempt must be claimed: {other:?}"),
    };
    assert_eq!(first.fence().epoch().get(), 1);
    assert!(matches!(
        store
            .claim_outbox_delivery(&tenant_id, AttemptId::generate())
            .await
            .unwrap(),
        OutboxClaimOutcome::NoWork
    ));

    tokio::time::sleep(Duration::from_millis(2_200)).await;
    assert!(matches!(
        store
            .claim_outbox_delivery(&tenant_id, first_attempt_id)
            .await,
        Err(StoreError::OutboxAttemptExpired)
    ));
    assert!(matches!(
        store.acknowledge_outbox_attempt(first.fence(), None).await,
        Err(StoreError::OutboxAttemptExpired)
    ));
    let second = match store
        .claim_outbox_delivery(&tenant_id, AttemptId::generate())
        .await
        .unwrap()
    {
        OutboxClaimOutcome::Claimed(claim) => claim,
        other => panic!("expired attempt must be taken over: {other:?}"),
    };
    assert_eq!(second.delivery(), first.delivery());
    assert_eq!(second.fence().epoch().get(), 2);
    assert!(matches!(
        store.acknowledge_outbox_attempt(first.fence(), None).await,
        Err(StoreError::StaleOutboxFence)
    ));

    assert!(matches!(
        store
            .fail_outbox_attempt(
                second.fence(),
                outbox_failure(0, RetryAdvice::ReconcileFirst)
            )
            .await,
        Err(StoreError::InvalidOutboxTransition)
    ));

    let retry_failure = outbox_failure(
        1,
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(1_000).unwrap(),
        },
    );
    assert!(matches!(
        store
            .fail_outbox_attempt(second.fence(), retry_failure)
            .await
            .unwrap(),
        OutboxCompletionOutcome::Committed { .. }
    ));
    assert!(matches!(
        store
            .claim_outbox_delivery(&tenant_id, AttemptId::generate())
            .await
            .unwrap(),
        OutboxClaimOutcome::NoWork
    ));

    let first_page = store
        .load_outbox_attempt_history_page(
            &tenant_id,
            run_id,
            delivery_id,
            None,
            OutboxAttemptHistoryPageSize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_page.records().len(), 1);
    assert!(first_page.has_more());
    assert_eq!(first_page.records()[0].start(), first.start());
    let cursor = first_page.next_cursor().unwrap();
    let second_page = store
        .load_outbox_attempt_history_page(
            &tenant_id,
            run_id,
            delivery_id,
            Some(&cursor),
            OutboxAttemptHistoryPageSize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_page.records().len(), 1);
    assert!(!second_page.has_more());
    assert_eq!(second_page.records()[0].start(), second.start());

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let third = match store
        .claim_outbox_delivery(&tenant_id, AttemptId::generate())
        .await
        .unwrap()
    {
        OutboxClaimOutcome::Claimed(claim) => claim,
        other => panic!("safe-after boundary must release work: {other:?}"),
    };
    assert_eq!(third.fence().epoch().get(), 3);
    let terminal_failure = outbox_failure(2, RetryAdvice::Never);
    let terminal = store
        .fail_outbox_attempt(third.fence(), terminal_failure.clone())
        .await
        .unwrap();
    assert!(matches!(
        terminal,
        OutboxCompletionOutcome::Committed { .. }
    ));
    let terminal_digest = terminal.completion().unwrap().digest();
    let terminal_lost_ack = store
        .fail_outbox_attempt(third.fence(), terminal_failure)
        .await
        .unwrap();
    assert!(matches!(
        terminal_lost_ack,
        OutboxCompletionOutcome::Idempotent { .. }
    ));
    assert_eq!(
        terminal_lost_ack.completion().unwrap().digest(),
        terminal_digest
    );
    assert!(matches!(
        store
            .fail_outbox_attempt(third.fence(), outbox_failure(3, RetryAdvice::Never))
            .await,
        Err(StoreError::OutboxCompletionConflict)
    ));
    assert!(matches!(
        store
            .claim_outbox_delivery(&tenant_id, AttemptId::generate())
            .await
            .unwrap(),
        OutboxClaimOutcome::NoWork
    ));
    let status = query_scalar::<_, String>(
        "SELECT status FROM stateknot.outbox_deliveries \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*delivery_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!(status, "dead_letter");

    let expiry_tenant = tenant("outbox-expiry");
    let expiry_run = RunId::generate();
    let (_, expiry_enqueue) = enqueue_outbox_fixture(
        &store,
        &expiry_tenant,
        expiry_run,
        1,
        Duration::from_secs(1),
    )
    .await;
    let expiry_delivery = expiry_enqueue.deliveries()[0].intent().delivery_id();
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    assert!(matches!(
        store
            .claim_outbox_delivery(&expiry_tenant, AttemptId::generate())
            .await
            .unwrap(),
        OutboxClaimOutcome::NoWork
    ));
    let expired = query_scalar::<_, bool>(
        "SELECT status = 'expired' \
                AND next_attempt_at IS NULL \
                AND terminal_at = expires_at \
                AND updated_at = expires_at \
         FROM stateknot.outbox_deliveries \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3",
    )
    .bind(expiry_tenant.as_str())
    .bind(*expiry_run.as_uuid())
    .bind(*expiry_delivery.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert!(expired);
    store
        .load_outbox_delivery(&expiry_tenant, expiry_run, expiry_delivery)
        .await
        .expect("a terminal expired delivery remains audit-readable");

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn outbox_attempt_limit_is_hard_bounded_for_completed_and_abandoned_attempts() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_outbox_attempt_lease(Duration::from_secs(3)).await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("outbox-attempt-limit");
    let run_id = RunId::generate();
    let (destination, first_enqueue) =
        enqueue_outbox_fixture(&store, &tenant_id, run_id, 1, Duration::from_secs(5 * 60)).await;
    let first_delivery_id = first_enqueue.deliveries()[0].intent().delivery_id();

    for expected_epoch in 1..=64_u64 {
        let claim = match store
            .claim_outbox_delivery(&tenant_id, AttemptId::generate())
            .await
            .unwrap()
        {
            OutboxClaimOutcome::Claimed(claim) => claim,
            other => panic!("attempt {expected_epoch} must be claimable: {other:?}"),
        };
        assert_eq!(claim.delivery().intent().delivery_id(), first_delivery_id);
        assert_eq!(claim.fence().epoch().get(), expected_epoch);
        assert!(matches!(
            store
                .fail_outbox_attempt(
                    claim.fence(),
                    outbox_failure(
                        expected_epoch,
                        RetryAdvice::SafeAfter {
                            delay: DurationMillis::ZERO,
                        },
                    ),
                )
                .await
                .unwrap(),
            OutboxCompletionOutcome::Committed { .. }
        ));
    }
    assert!(matches!(
        store
            .claim_outbox_delivery(&tenant_id, AttemptId::generate())
            .await
            .unwrap(),
        OutboxClaimOutcome::NoWork
    ));
    let completed_limit = query_scalar::<_, bool>(
        "SELECT status = 'dead_letter' \
                AND attempt_count = 64 \
                AND next_attempt_at IS NULL \
                AND last_completion_digest IS NOT NULL \
                AND terminal_at = updated_at \
         FROM stateknot.outbox_deliveries \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*first_delivery_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert!(completed_limit);

    let mut cursor = None;
    let mut epochs = Vec::new();
    loop {
        let page = store
            .load_outbox_attempt_history_page(
                &tenant_id,
                run_id,
                first_delivery_id,
                cursor.as_ref(),
                OutboxAttemptHistoryPageSize::new(16).unwrap(),
            )
            .await
            .unwrap();
        epochs.extend(
            page.records()
                .iter()
                .map(|attempt| attempt.start().fence().epoch().get()),
        );
        if !page.has_more() {
            break;
        }
        cursor = page.next_cursor();
    }
    assert_eq!(epochs, (1..=64).collect::<Vec<_>>());

    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    let second_event_id = EventId::generate();
    let second_delivery_id = DeliveryId::generate();
    store
        .append_control_plane_with_outbox(
            control_append(
                tenant_id.clone(),
                run_id,
                second_event_id,
                JournalExpectation::exact(run.journal_head().unwrap().clone()),
                65,
            ),
            RunProjection::unchanged(),
            vec![outbox_intent(
                &tenant_id,
                run_id,
                second_event_id,
                second_delivery_id,
                &destination,
                65,
                Duration::from_secs(5 * 60),
            )],
        )
        .await
        .unwrap();
    for expected_epoch in 1..64_u64 {
        let claim = match store
            .claim_outbox_delivery(&tenant_id, AttemptId::generate())
            .await
            .unwrap()
        {
            OutboxClaimOutcome::Claimed(claim) => claim,
            other => panic!("abandoned fixture attempt {expected_epoch} must claim: {other:?}"),
        };
        assert_eq!(claim.delivery().intent().delivery_id(), second_delivery_id);
        assert_eq!(claim.fence().epoch().get(), expected_epoch);
        store
            .fail_outbox_attempt(
                claim.fence(),
                outbox_failure(
                    100 + expected_epoch,
                    RetryAdvice::SafeAfter {
                        delay: DurationMillis::ZERO,
                    },
                ),
            )
            .await
            .unwrap();
    }
    let abandoned = match store
        .claim_outbox_delivery(&tenant_id, AttemptId::generate())
        .await
        .unwrap()
    {
        OutboxClaimOutcome::Claimed(claim) => claim,
        other => panic!("64th abandoned fixture attempt must claim: {other:?}"),
    };
    assert_eq!(abandoned.fence().epoch().get(), 64);
    tokio::time::sleep(Duration::from_millis(3_300)).await;
    assert!(matches!(
        store
            .claim_outbox_delivery(&tenant_id, AttemptId::generate())
            .await
            .unwrap(),
        OutboxClaimOutcome::NoWork
    ));
    let abandoned_limit = query_scalar::<_, bool>(
        "SELECT status = 'dead_letter' \
                AND attempt_count = 64 \
                AND next_attempt_at IS NULL \
                AND last_completion_digest IS NULL \
                AND terminal_at = current_attempt_expires_at \
                AND updated_at = current_attempt_expires_at \
         FROM stateknot.outbox_deliveries \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*second_delivery_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert!(abandoned_limit);
    store
        .load_outbox_delivery(&tenant_id, run_id, second_delivery_id)
        .await
        .expect("attempt-limit dead letters remain fully audit-readable");
    let last_page = store
        .load_outbox_attempt_history_page(
            &tenant_id,
            run_id,
            second_delivery_id,
            None,
            OutboxAttemptHistoryPageSize::new(16).unwrap(),
        )
        .await
        .unwrap();
    assert!(last_page.has_more());

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn outbox_load_claim_and_completion_fail_closed_on_every_durable_anchor() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();

    let destination_tenant = tenant("outbox-corrupt-destination");
    let destination_run = RunId::generate();
    let (destination, destination_enqueue) = enqueue_outbox_fixture(
        &store,
        &destination_tenant,
        destination_run,
        1,
        Duration::from_secs(60),
    )
    .await;
    let destination_delivery = destination_enqueue.deliveries()[0].intent().delivery_id();
    query(
        "UPDATE stateknot.outbox_destinations \
         SET config_bytes = config_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND destination_id = $2 AND snapshot_digest = $3",
    )
    .bind(destination_tenant.as_str())
    .bind(*destination.destination_id().as_uuid())
    .bind(destination.snapshot_digest().as_bytes())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store.load_outbox_destination(&destination).await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_outbox_delivery(&destination_tenant, destination_run, destination_delivery,)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .claim_outbox_delivery(&destination_tenant, AttemptId::generate())
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let delivery_tenant = tenant("outbox-corrupt-delivery");
    let delivery_run = RunId::generate();
    let (_, delivery_enqueue) = enqueue_outbox_fixture(
        &store,
        &delivery_tenant,
        delivery_run,
        1,
        Duration::from_secs(60),
    )
    .await;
    let delivery_id = delivery_enqueue.deliveries()[0].intent().delivery_id();
    query(
        "UPDATE stateknot.outbox_deliveries \
         SET delivery_bytes = delivery_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3",
    )
    .bind(delivery_tenant.as_str())
    .bind(*delivery_run.as_uuid())
    .bind(*delivery_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_outbox_delivery(&delivery_tenant, delivery_run, delivery_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .claim_outbox_delivery(&delivery_tenant, AttemptId::generate())
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let origin_tenant = tenant("outbox-corrupt-origin");
    let origin_run = RunId::generate();
    let (_, origin_enqueue) = enqueue_outbox_fixture(
        &store,
        &origin_tenant,
        origin_run,
        1,
        Duration::from_secs(60),
    )
    .await;
    let origin_delivery = origin_enqueue.deliveries()[0].intent().delivery_id();
    query(
        "UPDATE stateknot.run_events \
         SET payload_bytes = payload_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND sequence = 1",
    )
    .bind(origin_tenant.as_str())
    .bind(*origin_run.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_outbox_delivery(&origin_tenant, origin_run, origin_delivery)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .claim_outbox_delivery(&origin_tenant, AttemptId::generate())
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let start_tenant = tenant("outbox-corrupt-start");
    let start_run = RunId::generate();
    let (_, start_enqueue) =
        enqueue_outbox_fixture(&store, &start_tenant, start_run, 1, Duration::from_secs(60)).await;
    let start_delivery = start_enqueue.deliveries()[0].intent().delivery_id();
    let start_claim = match store
        .claim_outbox_delivery(&start_tenant, AttemptId::generate())
        .await
        .unwrap()
    {
        OutboxClaimOutcome::Claimed(claim) => claim,
        other => panic!("start corruption fixture must claim: {other:?}"),
    };
    query(
        "UPDATE stateknot.outbox_attempts \
         SET start_bytes = start_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3 AND epoch = 1",
    )
    .bind(start_tenant.as_str())
    .bind(*start_run.as_uuid())
    .bind(*start_delivery.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_outbox_attempt_history_page(
                &start_tenant,
                start_run,
                start_delivery,
                None,
                OutboxAttemptHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .acknowledge_outbox_attempt(start_claim.fence(), None)
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let completion_tenant = tenant("outbox-corrupt-completion");
    let completion_run = RunId::generate();
    let (_, completion_enqueue) = enqueue_outbox_fixture(
        &store,
        &completion_tenant,
        completion_run,
        1,
        Duration::from_secs(60),
    )
    .await;
    let completion_delivery = completion_enqueue.deliveries()[0].intent().delivery_id();
    let completion_claim = match store
        .claim_outbox_delivery(&completion_tenant, AttemptId::generate())
        .await
        .unwrap()
    {
        OutboxClaimOutcome::Claimed(claim) => claim,
        other => panic!("completion corruption fixture must claim: {other:?}"),
    };
    store
        .acknowledge_outbox_attempt(completion_claim.fence(), None)
        .await
        .unwrap();
    query(
        "UPDATE stateknot.outbox_attempt_completions \
         SET completion_bytes = completion_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND delivery_id = $3 AND epoch = 1",
    )
    .bind(completion_tenant.as_str())
    .bind(*completion_run.as_uuid())
    .bind(*completion_delivery.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_outbox_delivery(&completion_tenant, completion_run, completion_delivery,)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .acknowledge_outbox_attempt(completion_claim.fence(), None)
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let failed_claim_count = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.outbox_attempts \
         WHERE (tenant_id = $1 AND run_id = $2) \
            OR (tenant_id = $3 AND run_id = $4) \
            OR (tenant_id = $5 AND run_id = $6)",
    )
    .bind(destination_tenant.as_str())
    .bind(*destination_run.as_uuid())
    .bind(delivery_tenant.as_str())
    .bind(*delivery_run.as_uuid())
    .bind(origin_tenant.as_str())
    .bind(*origin_run.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!(
        failed_claim_count, 0,
        "failed validation must roll back claim starts"
    );

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_attempt_start_is_atomic_idempotent_and_load_verifiable() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("node-attempt-start");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_290)).await;
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"durable node attempt");
    let node_attempt_id = AttemptId::generate();
    let start_event_id = EventId::generate();
    let start_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            start_event_id,
            JournalExpectation::exact(checkpoint.event().head()),
            first_lease.fence().clone(),
            1_291,
        )
    };

    let started = store
        .start_node_attempt(start_append(), activation.clone(), node_attempt_id)
        .await
        .expect("node attempt start must commit before dispatch");
    assert!(matches!(
        started,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    assert_eq!(started.attempt().status(), NodeAttemptStatus::Executing);
    assert_eq!(started.event().sequence().get(), 2);

    let retry = store
        .start_node_attempt(start_append(), activation.clone(), node_attempt_id)
        .await
        .expect("lost start acknowledgement must converge");
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(
        retry.attempt().start().head(),
        started.attempt().start().head()
    );

    let restored = store
        .load_node_attempt(&tenant_id, &run_id, node_attempt_id)
        .await
        .expect("durable attempt must fully verify");
    assert_eq!(restored.start().head(), started.attempt().start().head());
    let page = store
        .load_node_attempt_history_page(
            &activation,
            None,
            NodeAttemptHistoryPageSize::new(1).unwrap(),
        )
        .await
        .expect("node attempt history must verify");
    assert_eq!(page.records().len(), 1);
    assert!(!page.has_more());
    assert_eq!(page.records()[0].start().head(), restored.start().head());

    assert!(matches!(
        store
            .start_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(started.event().head()),
                    first_lease.fence().clone(),
                    1_292,
                ),
                activation,
                node_attempt_id,
            )
            .await,
        Err(StoreError::NodeAttemptIdConflict)
    ));
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 2);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn node_attempt_failure_is_atomic_idempotent_and_blocks_unsafe_retry() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("node-attempt-failure");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_300)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"failed durable node attempt");
    let started = store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                1_301,
            ),
            activation.clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let failure_event_id = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new("node.integration_failed").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("The integration node failed safely.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(failure_event_id);
    let failure_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            failure_event_id,
            JournalExpectation::exact(started.event().head()),
            lease.fence().clone(),
            1_302,
        )
    };

    let failed = store
        .fail_node_attempt(
            failure_append(),
            &started.attempt().start().head(),
            failure.clone(),
            BudgetUsage::zero(),
        )
        .await
        .expect("node failure and its event must commit atomically");
    assert!(matches!(failed, NodeAttemptCommitOutcome::Committed { .. }));
    assert_eq!(failed.attempt().status(), NodeAttemptStatus::Failed);

    let retry = store
        .fail_node_attempt(
            failure_append(),
            &started.attempt().start().head(),
            failure,
            BudgetUsage::zero(),
        )
        .await
        .expect("lost failure acknowledgement must converge");
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(retry.attempt().status(), NodeAttemptStatus::Failed);
    assert_eq!(
        store
            .load_node_attempt(&tenant_id, &run_id, started.attempt().start().attempt_id(),)
            .await
            .unwrap()
            .status(),
        NodeAttemptStatus::Failed
    );

    assert!(matches!(
        store
            .start_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(failed.event().head()),
                    lease.fence().clone(),
                    1_303,
                ),
                activation,
                AttemptId::generate(),
            )
            .await,
        Err(StoreError::InvalidNodeAttemptTransition)
    ));
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 3);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn node_attempt_hard_limit_is_recoverable_and_rejects_a_sixty_fifth_start() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(5 * 60)).await else {
        return;
    };
    let tenant_id = tenant("node-attempt-hard-limit");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_320)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let fence = lease.fence().clone();
    let activation =
        NodeActivation::for_ready_root(checkpoint.checkpoint(), NodeId::new("node-0001").unwrap())
            .unwrap();
    let mut journal_head = checkpoint.event().head().clone();
    let mut last_start = None;

    for index in 0..ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
        let ordinal = u64::try_from(index).unwrap();
        let start_event_id = EventId::generate();
        let attempt_id = AttemptId::generate();
        let start_parent = journal_head.clone();
        let start_payload = 1_321 + ordinal * 2;
        let started = store
            .start_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    start_event_id,
                    JournalExpectation::exact(start_parent.clone()),
                    fence.clone(),
                    start_payload,
                ),
                activation.clone(),
                attempt_id,
            )
            .await
            .expect("every start through the hard ceiling must commit");
        assert!(matches!(
            started,
            NodeAttemptCommitOutcome::Committed { .. }
        ));
        if index + 1 == ReadyNodeRecoveryPlanner::MAX_ATTEMPTS_PER_NODE {
            last_start = Some((start_event_id, start_parent, start_payload, attempt_id));
        }

        let failure_event_id = EventId::generate();
        let failure = Failure::new(
            FailureId::generate(),
            FailureCategory::Internal,
            FailureCode::new("node.retryable_limit_test").unwrap(),
            FailureOrigin::new("graph.integration").unwrap(),
            FailureMessage::new("The bounded integration node failed safely.").unwrap(),
            RetryAdvice::SafeAfter {
                delay: DurationMillis::ZERO,
            },
        )
        .unwrap()
        .with_caused_by_event(failure_event_id);
        let failed = store
            .fail_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    failure_event_id,
                    JournalExpectation::exact(started.event().head()),
                    fence.clone(),
                    start_payload + 1,
                ),
                &started.attempt().start().head(),
                failure,
                BudgetUsage::zero(),
            )
            .await
            .expect("retryable failure through the hard ceiling must commit");
        assert!(matches!(failed, NodeAttemptCommitOutcome::Committed { .. }));
        journal_head = failed.event().head().clone();
    }

    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(journal_head.clone()),
        Digest::sha256(b"bounded node-attempt recovery evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    let plan = recovery.plan_ready_nodes().await.unwrap();
    assert_eq!(plan.nodes()[0].kind(), RecoveryNodeKind::Exhausted);
    assert!(plan.nodes()[0].exhausted_attempt().is_some());
    assert!(matches!(
        store
            .start_recovered_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(journal_head.clone()),
                    fence.clone(),
                    1_450,
                ),
                &plan,
                activation.node_id(),
                AttemptId::generate(),
            )
            .await,
        Err(StoreError::ReadyNodeNotDispatchable)
    ));
    assert!(matches!(
        store
            .start_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(journal_head.clone()),
                    fence.clone(),
                    1_451,
                ),
                activation.clone(),
                AttemptId::generate(),
            )
            .await,
        Err(StoreError::NodeAttemptLimitExceeded)
    ));

    let (last_event_id, last_parent, last_payload, last_attempt_id) =
        last_start.expect("the final allowed start must be retained");
    let retry = store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                last_event_id,
                JournalExpectation::exact(last_parent),
                fence,
                last_payload,
            ),
            activation,
            last_attempt_id,
        )
        .await
        .expect("an exact lost-ACK retry at the limit must remain idempotent");
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .journal_head(),
        Some(&journal_head)
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn node_attempt_recovery_requires_takeover_and_database_safe_after_time() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("node-attempt-recovery");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_305)).await;
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"recoverable node attempt");
    let abandoned = store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                first_lease.fence().clone(),
                1_306,
            ),
            activation.clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .start_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(abandoned.event().head()),
                    first_lease.fence().clone(),
                    1_307,
                ),
                activation.clone(),
                AttemptId::generate(),
            )
            .await,
        Err(StoreError::InvalidNodeAttemptTransition)
    ));

    let successor_lease = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let recovered = store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(abandoned.event().head()),
                successor_lease.fence().clone(),
                1_308,
            ),
            activation.clone(),
            AttemptId::generate(),
        )
        .await
        .expect("a higher fencing epoch may recover an unfinished attempt");
    let stale_failure_event_id = EventId::generate();
    let stale_failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new("node.abandoned").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("The abandoned node attempt cannot complete late.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(stale_failure_event_id);
    assert!(matches!(
        store
            .fail_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    stale_failure_event_id,
                    JournalExpectation::exact(recovered.event().head()),
                    first_lease.fence().clone(),
                    1_309,
                ),
                &abandoned.attempt().start().head(),
                stale_failure,
                BudgetUsage::zero(),
            )
            .await,
        Err(StoreError::StaleFence)
    ));

    let failure_event_id = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::DependencyUnavailable,
        FailureCode::new("node.retry_later").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("The node may retry after the durable delay.").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(1_000).unwrap(),
        },
    )
    .unwrap()
    .with_caused_by_event(failure_event_id);
    let failed = store
        .fail_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                failure_event_id,
                JournalExpectation::exact(recovered.event().head()),
                successor_lease.fence().clone(),
                1_310,
            ),
            &recovered.attempt().start().head(),
            failure,
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    let final_attempt_id = AttemptId::generate();
    assert!(matches!(
        store
            .start_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(failed.event().head()),
                    successor_lease.fence().clone(),
                    1_311,
                ),
                activation.clone(),
                final_attempt_id,
            )
            .await,
        Err(StoreError::InvalidNodeAttemptTransition)
    ));
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let eligible = store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(failed.event().head()),
                successor_lease.fence().clone(),
                1_312,
            ),
            activation.clone(),
            final_attempt_id,
        )
        .await
        .expect("database-observed safe-after delay must eventually admit retry");
    assert_eq!(eligible.attempt().status(), NodeAttemptStatus::Executing);

    let first_page = store
        .load_node_attempt_history_page(
            &activation,
            None,
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_page.records().len(), 2);
    assert!(first_page.has_more());
    let cursor = first_page.next_cursor().unwrap();
    let second_page = store
        .load_node_attempt_history_page(
            &activation,
            Some(&cursor),
            NodeAttemptHistoryPageSize::new(2).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_page.records().len(), 1);
    assert_eq!(
        second_page.records()[0].status(),
        NodeAttemptStatus::Executing
    );
    assert!(!second_page.has_more());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn node_attempt_success_atomically_binds_result_and_checkpoint_barrier() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("node-attempt-success");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_310)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation =
        pending_activation(checkpoint.checkpoint(), b"successful durable node attempt");
    let started = store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                1_311,
            ),
            activation.clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let result_intent = pending_result_intent(activation.clone(), NodeInvocationBindings::empty());
    let success_event_id = EventId::generate();
    let success_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            success_event_id,
            JournalExpectation::exact(started.event().head()),
            lease.fence().clone(),
            1_312,
        )
    };

    let succeeded = store
        .succeed_node_attempt(
            success_append(),
            &started.attempt().start().head(),
            result_intent.clone(),
            BudgetUsage::zero(),
        )
        .await
        .expect("success completion and pending result must commit together");
    assert!(matches!(
        succeeded,
        NodeAttemptCommitOutcome::Committed { .. }
    ));
    assert_eq!(succeeded.attempt().status(), NodeAttemptStatus::Succeeded);

    let exact_retry = store
        .succeed_node_attempt(
            success_append(),
            &started.attempt().start().head(),
            result_intent.clone(),
            BudgetUsage::zero(),
        )
        .await
        .expect("lost success acknowledgement must converge by event identity");
    assert!(matches!(
        exact_retry,
        NodeAttemptCommitOutcome::Idempotent { .. }
    ));
    let semantic_retry = store
        .succeed_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(succeeded.event().head()),
                lease.fence().clone(),
                1_313,
            ),
            &started.attempt().start().head(),
            result_intent,
            BudgetUsage::zero(),
        )
        .await
        .expect("physical-attempt semantic retry must return the original winner");
    assert!(matches!(
        semantic_retry,
        NodeAttemptCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(semantic_retry.event().head(), succeeded.event().head());

    let pending = store
        .load_pending_node_result(&activation)
        .await
        .expect("attempt-owned pending result must fully verify");
    let restored = store
        .load_node_attempt(&tenant_id, &run_id, started.attempt().start().attempt_id())
        .await
        .expect("successful physical attempt must fully verify");
    assert_eq!(restored.status(), NodeAttemptStatus::Succeeded);
    assert_eq!(
        restored.completion().unwrap().outcome().result().unwrap(),
        &pending.head()
    );

    let barrier = CheckpointBarrier::new(
        checkpoint.checkpoint(),
        successor_checkpoint_write(CheckpointId::generate(), checkpoint.checkpoint(), 1),
        [pending.head()],
    )
    .unwrap();
    let advanced = store
        .append_worker_barrier(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(succeeded.event().head()),
                lease.fence().clone(),
                1_314,
            ),
            RunProjection::unchanged(),
            barrier,
        )
        .await
        .expect("attempt-owned result must satisfy the complete checkpoint barrier");
    assert_eq!(advanced.checkpoint().superstep().get(), 1);
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 4);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn node_attempt_reads_fail_closed_and_attempt_ids_are_run_wide() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("test administration connection must open");
    let tenant_id = tenant("node-attempt-corruption");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_320)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"verified node attempt");
    let node_attempt_id = AttemptId::generate();
    let started = store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                1_321,
            ),
            activation.clone(),
            node_attempt_id,
        )
        .await
        .unwrap();

    let tool_invocation_id = InvocationId::generate();
    let prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(started.event().head()),
                lease.fence().clone(),
                1_322,
            ),
            tool_invocation_intent(checkpoint.checkpoint(), tool_invocation_id),
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .advance_tool_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(prepared.event().head()),
                    lease.fence().clone(),
                    1_323,
                ),
                &prepared.invocation().head(),
                ToolInvocationTransition::StartAttempt {
                    attempt_id: node_attempt_id,
                },
            )
            .await,
        Err(StoreError::InvalidToolInvocationTransition)
    ));
    assert_eq!(
        store
            .load_tool_invocation(&tenant_id, run_id, tool_invocation_id)
            .await
            .unwrap()
            .status(),
        ToolInvocationStatus::Prepared
    );

    let failure_event_id = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::Internal,
        FailureCode::new("node.integrity_fixture").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("The integrity fixture completed with a durable failure.").unwrap(),
        RetryAdvice::Never,
    )
    .unwrap()
    .with_caused_by_event(failure_event_id);
    store
        .fail_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                failure_event_id,
                JournalExpectation::exact(prepared.event().head()),
                lease.fence().clone(),
                1_324,
            ),
            &started.attempt().start().head(),
            failure,
            BudgetUsage::zero(),
        )
        .await
        .unwrap();

    let original_completion: Vec<u8> = query_scalar(
        "SELECT completion_bytes FROM stateknot.node_attempt_completions \
         WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*node_attempt_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    query(
        "UPDATE stateknot.node_attempt_completions \
         SET completion_bytes = completion_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*node_attempt_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_node_attempt(&tenant_id, &run_id, node_attempt_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_node_attempt_history_page(
                &activation,
                None,
                NodeAttemptHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    query(
        "UPDATE stateknot.node_attempt_completions \
         SET completion_bytes = $4 \
         WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*node_attempt_id.as_uuid())
    .bind(original_completion)
    .execute(&administration)
    .await
    .unwrap();
    assert_eq!(
        store
            .load_node_attempt(&tenant_id, &run_id, node_attempt_id)
            .await
            .unwrap()
            .status(),
        NodeAttemptStatus::Failed
    );

    query(
        "UPDATE stateknot.node_attempts \
         SET start_bytes = start_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND attempt_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*node_attempt_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_node_attempt(&tenant_id, &run_id, node_attempt_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_node_attempt_history_page(
                &activation,
                None,
                NodeAttemptHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn pending_node_results_are_attempt_owned_fenced_and_load_verifiable() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("pending-node-result");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_100)).await;
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"pending result activation");
    let intent = pending_result_intent(activation.clone(), NodeInvocationBindings::empty());
    let event_id = EventId::generate();
    let result_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            event_id,
            JournalExpectation::exact(checkpoint.event().head()),
            first_lease.fence().clone(),
            1_101,
        )
    };

    let committed = store
        .commit_test_pending_node_result(result_append(), intent.clone())
        .await
        .expect("pending result must commit atomically");
    assert!(matches!(
        committed,
        PendingNodeResultCommitOutcome::Committed { .. }
    ));
    assert_eq!(committed.event().sequence().get(), 3);
    assert_eq!(
        store
            .load_pending_node_result(&activation)
            .await
            .expect("pending result must fully restore"),
        *committed.result()
    );

    let same_event_retry = store
        .commit_test_pending_node_result(result_append(), intent.clone())
        .await
        .expect("lost result acknowledgement must converge");
    assert!(matches!(
        same_event_retry,
        PendingNodeResultCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(same_event_retry.result(), committed.result());

    let successor_lease = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert!(matches!(
        store
            .commit_test_pending_node_result(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(committed.event().head()),
                    successor_lease.fence().clone(),
                    1_102,
                ),
                intent.clone(),
            )
            .await,
        Err(StoreError::InvalidNodeAttemptTransition)
    ));
    assert!(matches!(
        store
            .commit_pending_node_result(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(committed.event().head()),
                    successor_lease.fence().clone(),
                    1_102,
                ),
                intent.clone(),
            )
            .await,
        Err(StoreError::NodeAttemptRequired)
    ));

    let changed_update = NodeStateUpdate::new(
        checkpoint.checkpoint().graph().state_schema().clone(),
        BoundedJson::try_from_value(json!({"value": "different"})).unwrap(),
    )
    .unwrap();
    let conflicting = PendingNodeResultIntent::new(
        activation.clone(),
        NodeStateChange::Update {
            update: changed_update,
        },
        NodeControl::Continue,
        NodeInvocationBindings::empty(),
    )
    .unwrap();
    assert!(matches!(
        store
            .commit_test_pending_node_result(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(committed.event().head()),
                    successor_lease.fence().clone(),
                    1_103,
                ),
                conflicting,
            )
            .await,
        Err(StoreError::InvalidNodeAttemptTransition)
    ));
    let crossed_input =
        drifted_pending_activation(checkpoint.checkpoint(), b"another activation input");
    assert!(matches!(
        store.load_pending_node_result(&crossed_input).await,
        Err(StoreError::PendingNodeResultNotFound)
    ));

    let stale_tenant = tenant("pending-node-result-stale-fence");
    let stale_run = RunId::generate();
    let stale_checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &stale_tenant,
        stale_run,
        1_110,
    ))
    .await;
    let stale_lease = store
        .claim_lease(&stale_tenant, stale_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    store
        .supersede_lease(&stale_tenant, stale_run, AttemptId::generate())
        .await
        .unwrap();
    let stale_activation =
        pending_activation(stale_checkpoint.checkpoint(), b"stale result activation");
    assert!(matches!(
        store
            .commit_test_pending_node_result(
                worker_append(
                    stale_tenant.clone(),
                    stale_run,
                    EventId::generate(),
                    JournalExpectation::exact(stale_checkpoint.event().head()),
                    stale_lease.fence().clone(),
                    1_111,
                ),
                pending_result_intent(stale_activation.clone(), NodeInvocationBindings::empty(),),
            )
            .await,
        Err(StoreError::StaleFence)
    ));
    assert!(matches!(
        store.load_pending_node_result(&stale_activation).await,
        Err(StoreError::PendingNodeResultNotFound)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn unconsumed_pending_result_pages_are_stable_bounded_and_fully_verified() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("pending-result-page");
    let run_id = RunId::generate();
    let ready_nodes = ReadyNodes::try_new(
        ["node-delta", "node-alpha", "node-charlie", "node-bravo"]
            .into_iter()
            .map(|node| NodeId::new(node).unwrap()),
    )
    .unwrap();
    let checkpoint = Box::pin(start_run_with_ready_checkpoint(
        &store,
        &tenant_id,
        run_id,
        1_120,
        ready_nodes,
    ))
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let mut journal_head = checkpoint.event().head();

    for (index, node) in ["node-charlie", "node-alpha", "node-bravo"]
        .into_iter()
        .enumerate()
    {
        let activation =
            NodeActivation::for_ready_root(checkpoint.checkpoint(), NodeId::new(node).unwrap())
                .unwrap();
        let committed = store
            .commit_test_pending_node_result(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(journal_head),
                    lease.fence().clone(),
                    1_121 + u64::try_from(index).unwrap(),
                ),
                pending_result_intent(activation, NodeInvocationBindings::empty()),
            )
            .await
            .unwrap();
        journal_head = committed.event().head();
    }

    let first = store
        .load_unconsumed_pending_node_result_page(
            &checkpoint.checkpoint().head(),
            None,
            PendingNodeResultPageSize::new(2).unwrap(),
        )
        .await
        .expect("first pending-result page must be fully verified");
    assert_eq!(first.records().len(), 2);
    assert_eq!(
        first
            .records()
            .iter()
            .map(|result| result.intent().activation().node_id().as_str())
            .collect::<Vec<_>>(),
        vec!["node-alpha", "node-bravo"]
    );
    assert!(first.has_more());
    assert_eq!(first.snapshot_journal_head(), &journal_head);
    let first_cursor = first.next_cursor().unwrap();

    let second = store
        .load_unconsumed_pending_node_result_page(
            &checkpoint.checkpoint().head(),
            Some(&first_cursor),
            PendingNodeResultPageSize::new(2).unwrap(),
        )
        .await
        .expect("unchanged journal snapshot must continue exactly");
    assert_eq!(second.records().len(), 1);
    assert_eq!(
        second.records()[0].intent().activation().node_id().as_str(),
        "node-charlie"
    );
    assert!(!second.has_more());
    assert_eq!(
        second.snapshot_journal_head(),
        first.snapshot_journal_head()
    );

    let delta_activation =
        NodeActivation::for_ready_root(checkpoint.checkpoint(), NodeId::new("node-delta").unwrap())
            .unwrap();
    let delta = store
        .commit_test_pending_node_result(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(journal_head),
                lease.fence().clone(),
                1_124,
            ),
            pending_result_intent(delta_activation, NodeInvocationBindings::empty()),
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .load_unconsumed_pending_node_result_page(
                &checkpoint.checkpoint().head(),
                Some(&first_cursor),
                PendingNodeResultPageSize::new(2).unwrap(),
            )
            .await,
        Err(StoreError::StalePendingNodeResultSnapshot)
    ));

    let restarted = store
        .load_unconsumed_pending_node_result_page(
            &checkpoint.checkpoint().head(),
            None,
            PendingNodeResultPageSize::new(2).unwrap(),
        )
        .await
        .expect("stale scanning must restart from a new exact snapshot");
    assert_eq!(restarted.snapshot_journal_head(), &delta.event().head());
    assert_eq!(
        restarted
            .records()
            .iter()
            .map(|result| result.intent().activation().node_id().as_str())
            .collect::<Vec<_>>(),
        vec!["node-alpha", "node-bravo"]
    );
    assert!(restarted.has_more());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_barrier_atomically_consumes_complete_results_and_is_idempotent() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("worker-barrier-commit");
    let run_id = RunId::generate();
    let ready_nodes = ReadyNodes::try_new(
        ["node-bravo", "node-alpha"]
            .into_iter()
            .map(|node| NodeId::new(node).unwrap()),
    )
    .unwrap();
    let initial = Box::pin(start_run_with_ready_checkpoint(
        &store,
        &tenant_id,
        run_id,
        1_130,
        ready_nodes,
    ))
    .await;
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let mut journal_head = initial.event().head();
    let mut result_heads = Vec::new();
    for (index, node) in ["node-bravo", "node-alpha"].into_iter().enumerate() {
        let activation =
            NodeActivation::for_ready_root(initial.checkpoint(), NodeId::new(node).unwrap())
                .unwrap();
        let committed = store
            .commit_test_pending_node_result(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(journal_head),
                    first_lease.fence().clone(),
                    1_131 + u64::try_from(index).unwrap(),
                ),
                pending_result_intent(activation, NodeInvocationBindings::empty()),
            )
            .await
            .unwrap();
        journal_head = committed.event().head();
        result_heads.push(committed.result().head());
    }

    let successor_write = CheckpointWrite::successor(
        CheckpointId::generate(),
        initial.checkpoint(),
        checkpoint_state(initial.checkpoint().graph(), 1),
        ready_node(3),
    )
    .unwrap();
    let barrier =
        CheckpointBarrier::new(initial.checkpoint(), successor_write, result_heads).unwrap();
    let barrier_event_id = EventId::generate();
    let append = worker_append(
        tenant_id.clone(),
        run_id,
        barrier_event_id,
        JournalExpectation::exact(journal_head),
        first_lease.fence().clone(),
        1_133,
    );
    let committed = store
        .append_worker_barrier(append.clone(), RunProjection::unchanged(), barrier.clone())
        .await
        .expect("complete worker barrier must commit atomically");
    assert!(matches!(committed, BarrierCommitOutcome::Committed { .. }));
    assert_eq!(
        committed.checkpoint().parent(),
        Some(&initial.checkpoint().head())
    );
    assert_eq!(
        committed.checkpoint().journal_head(),
        &committed.event().head()
    );
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap(),
        Some(committed.checkpoint().clone())
    );

    store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap();
    let retry = store
        .append_worker_barrier(append, RunProjection::unchanged(), barrier)
        .await
        .expect("lost barrier acknowledgement must survive lease takeover");
    assert!(matches!(retry, BarrierCommitOutcome::Idempotent { .. }));
    assert_eq!(retry.event(), committed.event());
    assert_eq!(retry.checkpoint(), committed.checkpoint());

    assert!(matches!(
        store
            .load_unconsumed_pending_node_result_page(
                &initial.checkpoint().head(),
                None,
                PendingNodeResultPageSize::new(2).unwrap(),
            )
            .await,
        Err(StoreError::StaleCheckpointHead)
    ));
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let consumption_count = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.pending_node_result_consumptions \
         WHERE tenant_id = $1 AND run_id = $2 AND base_checkpoint_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*initial.checkpoint().checkpoint_id().as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!(consumption_count, 2);
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn worker_wait_barrier_atomically_consumes_results_checkpoints_and_waits() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("worker-wait-barrier");
    let run_id = RunId::generate();
    let base = Box::pin(start_run_with_ready_checkpoint(
        &store,
        &tenant_id,
        run_id,
        1_140,
        ready_node(1),
    ))
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (result_heads, result_journal) =
        commit_ready_results(&store, base.checkpoint(), lease.fence(), 1_141).await;
    let successor = CheckpointWrite::successor(
        CheckpointId::generate(),
        base.checkpoint(),
        checkpoint_state(base.checkpoint().graph(), 1),
        ready_node(2),
    )
    .unwrap();
    let barrier = CheckpointBarrier::new(base.checkpoint(), successor, result_heads).unwrap();
    let waiting_before = store.load_run(&tenant_id, run_id).await.unwrap();
    let wait_event_id = EventId::generate();
    let interrupt_id = InterruptId::generate();
    let timer_id = TimerId::generate();
    let registrations = vec![
        WaitRegistrationIntent::interrupt(
            InterruptRequestIntent::new(
                tenant_id.clone(),
                run_id,
                interrupt_id,
                wait_event_id,
                RunInterruptKind::Approval,
                payload(1_142),
                Digest::sha256(b"worker wait barrier action"),
                None,
                ScopeSet::empty(),
                Some(timestamp_after(Duration::from_secs(3_600))),
            )
            .unwrap(),
        ),
        WaitRegistrationIntent::timer(
            TimerRegistrationIntent::new(
                tenant_id.clone(),
                run_id,
                timer_id,
                wait_event_id,
                RunTimerKind::Sleep,
                timestamp_after(Duration::from_secs(1_800)),
            )
            .unwrap(),
        ),
    ];
    let append = worker_append(
        tenant_id.clone(),
        run_id,
        wait_event_id,
        JournalExpectation::exact(result_journal),
        lease.fence().clone(),
        1_143,
    );
    let committed = store
        .append_worker_wait_barrier(
            append.clone(),
            waiting_before.lifecycle().revision(),
            barrier.clone(),
            registrations.clone(),
        )
        .await
        .expect("the complete worker wait barrier must commit atomically");
    assert!(matches!(
        committed,
        WaitCheckpointCommitOutcome::Committed { .. }
    ));
    assert_eq!(committed.waits().len(), 2);
    assert_eq!(
        committed.checkpoint().parent(),
        Some(&base.checkpoint().head())
    );
    assert_eq!(
        committed.checkpoint().journal_head(),
        &committed.event().head()
    );
    let waiting = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(waiting.lifecycle().status(), RunStatus::Waiting);
    assert!(waiting.lease().is_none());
    assert_eq!(waiting.unresolved_wait_count(), 2);
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap(),
        Some(committed.checkpoint().clone())
    );
    assert_eq!(
        store
            .load_interrupt_request(&tenant_id, run_id, interrupt_id)
            .await
            .unwrap()
            .journal(),
        &committed.event().head()
    );
    assert_eq!(
        store
            .load_durable_timer(&tenant_id, run_id, timer_id)
            .await
            .unwrap()
            .journal(),
        &committed.event().head()
    );

    store.release_lease(lease.fence()).await.unwrap();
    let retry = store
        .append_worker_wait_barrier(
            append.clone(),
            waiting_before.lifecycle().revision(),
            barrier.clone(),
            registrations,
        )
        .await
        .expect("lost wait-barrier acknowledgement must ignore a released old fence");
    assert!(matches!(
        retry,
        WaitCheckpointCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(retry.event(), committed.event());
    assert_eq!(retry.checkpoint(), committed.checkpoint());
    assert_eq!(retry.waits(), committed.waits());
    assert!(matches!(
        store
            .append_control_plane_wait_barrier(
                append,
                waiting_before.lifecycle().revision(),
                barrier,
                Vec::new(),
            )
            .await,
        Err(StoreError::WrongAppendAuthority)
    ));

    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let (consumption_count, wait_count) = query_as::<_, (i64, i64)>(
        "SELECT \
             (SELECT count(*) FROM stateknot.pending_node_result_consumptions \
              WHERE tenant_id = $1 AND run_id = $2 AND base_checkpoint_id = $3), \
             (SELECT count(*) FROM stateknot.run_wait_registrations \
              WHERE tenant_id = $1 AND run_id = $2 AND registration_event_id = $4)",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*base.checkpoint().checkpoint_id().as_uuid())
    .bind(*wait_event_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!((consumption_count, wait_count), (1, 2));
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn wait_barrier_failure_rolls_back_every_joined_projection() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("wait-barrier-rollback");
    let run_id = RunId::generate();
    let base = Box::pin(start_run_with_ready_checkpoint(
        &store,
        &tenant_id,
        run_id,
        1_145,
        ready_node(1),
    ))
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (result_heads, result_journal) =
        commit_ready_results(&store, base.checkpoint(), lease.fence(), 1_146).await;
    let successor_id = CheckpointId::generate();
    let barrier = CheckpointBarrier::new(
        base.checkpoint(),
        CheckpointWrite::successor(
            successor_id,
            base.checkpoint(),
            checkpoint_state(base.checkpoint().graph(), 1),
            ready_node(2),
        )
        .unwrap(),
        result_heads,
    )
    .unwrap();
    let active = store.load_run(&tenant_id, run_id).await.unwrap();
    let wait_event_id = EventId::generate();
    let interrupt_id = InterruptId::generate();
    let timer_id = TimerId::generate();
    let registrations = vec![
        WaitRegistrationIntent::interrupt(
            InterruptRequestIntent::new(
                tenant_id.clone(),
                run_id,
                interrupt_id,
                wait_event_id,
                RunInterruptKind::Approval,
                payload(1_147),
                Digest::sha256(b"wait barrier rollback action"),
                None,
                ScopeSet::empty(),
                Some(timestamp_after(Duration::from_secs(3_600))),
            )
            .unwrap(),
        ),
        WaitRegistrationIntent::timer(
            TimerRegistrationIntent::new(
                tenant_id.clone(),
                run_id,
                timer_id,
                wait_event_id,
                RunTimerKind::RetryBackoff,
                timestamp_after(Duration::from_secs(1_800)),
            )
            .unwrap(),
        ),
    ];
    let append = worker_append(
        tenant_id.clone(),
        run_id,
        wait_event_id,
        JournalExpectation::exact(result_journal.clone()),
        lease.fence().clone(),
        1_148,
    );

    query("ALTER TABLE stateknot.runs DROP CONSTRAINT IF EXISTS test_wait_barrier_rollback")
        .execute(&administration)
        .await
        .unwrap();
    let reject_target = format!(
        "ALTER TABLE stateknot.runs ADD CONSTRAINT test_wait_barrier_rollback \
         CHECK (tenant_id <> '{}') NOT VALID",
        tenant_id.as_str()
    );
    query(&reject_target)
        .execute(&administration)
        .await
        .unwrap();
    let result = store
        .append_worker_wait_barrier(
            append.clone(),
            active.lifecycle().revision(),
            barrier.clone(),
            registrations.clone(),
        )
        .await;
    query("ALTER TABLE stateknot.runs DROP CONSTRAINT test_wait_barrier_rollback")
        .execute(&administration)
        .await
        .unwrap();
    assert!(matches!(result, Err(StoreError::Database { .. })));

    let unchanged = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(unchanged.lifecycle().status(), RunStatus::Active);
    assert_eq!(unchanged.unresolved_wait_count(), 0);
    assert_eq!(unchanged.journal_head(), Some(&result_journal));
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap(),
        Some(base.checkpoint().clone())
    );
    let (event_count, checkpoint_count, wait_count, consumption_count) =
        query_as::<_, (i64, i64, i64, i64)>(
            "SELECT \
                 (SELECT count(*) FROM stateknot.run_events \
                  WHERE tenant_id = $1 AND run_id = $2 AND event_id = $3), \
                 (SELECT count(*) FROM stateknot.run_checkpoints \
                  WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $4), \
                 (SELECT count(*) FROM stateknot.run_wait_registrations \
                  WHERE tenant_id = $1 AND run_id = $2 AND registration_event_id = $3), \
                 (SELECT count(*) FROM stateknot.pending_node_result_consumptions \
                  WHERE tenant_id = $1 AND run_id = $2 AND successor_checkpoint_id = $4)",
        )
        .bind(tenant_id.as_str())
        .bind(*run_id.as_uuid())
        .bind(*wait_event_id.as_uuid())
        .bind(*successor_id.as_uuid())
        .fetch_one(&administration)
        .await
        .unwrap();
    assert_eq!(
        (event_count, checkpoint_count, wait_count, consumption_count),
        (0, 0, 0, 0)
    );

    let recovered = store
        .append_worker_wait_barrier(
            append,
            active.lifecycle().revision(),
            barrier,
            registrations,
        )
        .await
        .unwrap();
    assert!(matches!(
        recovered,
        WaitCheckpointCommitOutcome::Committed { .. }
    ));
    assert_eq!(recovered.waits().len(), 2);

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn barrier_rejects_incomplete_conflicting_and_stale_fenced_inputs_without_mutation() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };

    let incomplete_tenant = tenant("barrier-incomplete");
    let incomplete_run = RunId::generate();
    let incomplete_ready = ReadyNodes::try_new(
        ["node-alpha", "node-bravo"]
            .into_iter()
            .map(|node| NodeId::new(node).unwrap()),
    )
    .unwrap();
    let incomplete_base = Box::pin(start_run_with_ready_checkpoint(
        &store,
        &incomplete_tenant,
        incomplete_run,
        1_150,
        incomplete_ready,
    ))
    .await;
    let incomplete_lease = store
        .claim_lease(&incomplete_tenant, incomplete_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let alpha_activation = NodeActivation::for_ready_root(
        incomplete_base.checkpoint(),
        NodeId::new("node-alpha").unwrap(),
    )
    .unwrap();
    let alpha = store
        .commit_test_pending_node_result(
            worker_append(
                incomplete_tenant.clone(),
                incomplete_run,
                EventId::generate(),
                JournalExpectation::exact(incomplete_base.event().head()),
                incomplete_lease.fence().clone(),
                1_151,
            ),
            pending_result_intent(alpha_activation, NodeInvocationBindings::empty()),
        )
        .await
        .unwrap();
    let fabricated_bravo = PendingNodeResultHead::new(
        NodeActivation::for_ready_root(
            incomplete_base.checkpoint(),
            NodeId::new("node-bravo").unwrap(),
        )
        .unwrap(),
        Digest::sha256(b"missing bravo intent"),
        incomplete_lease.fence().clone(),
        alpha.event().head(),
        Digest::sha256(b"missing bravo result"),
    )
    .unwrap();
    let incomplete_successor_id = CheckpointId::generate();
    let incomplete_barrier = CheckpointBarrier::new(
        incomplete_base.checkpoint(),
        CheckpointWrite::successor(
            incomplete_successor_id,
            incomplete_base.checkpoint(),
            checkpoint_state(incomplete_base.checkpoint().graph(), 1),
            ready_node(2),
        )
        .unwrap(),
        [alpha.result().head(), fabricated_bravo],
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker_barrier(
                worker_append(
                    incomplete_tenant.clone(),
                    incomplete_run,
                    EventId::generate(),
                    JournalExpectation::exact(alpha.event().head()),
                    incomplete_lease.fence().clone(),
                    1_152,
                ),
                RunProjection::unchanged(),
                incomplete_barrier,
            )
            .await,
        Err(StoreError::CheckpointBarrierIncomplete)
    ));
    assert_eq!(
        store
            .load_current_checkpoint(&incomplete_tenant, incomplete_run)
            .await
            .unwrap(),
        Some(incomplete_base.checkpoint().clone())
    );
    assert!(matches!(
        store
            .load_checkpoint(&incomplete_tenant, incomplete_run, incomplete_successor_id,)
            .await,
        Err(StoreError::CheckpointNotFound)
    ));

    let conflict_tenant = tenant("barrier-result-conflict");
    let conflict_run = RunId::generate();
    let conflict_base = Box::pin(start_run_with_ready_checkpoint(
        &store,
        &conflict_tenant,
        conflict_run,
        1_160,
        ready_node(1),
    ))
    .await;
    let conflict_lease = store
        .claim_lease(&conflict_tenant, conflict_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (mut conflict_heads, conflict_journal) = commit_ready_results(
        &store,
        conflict_base.checkpoint(),
        conflict_lease.fence(),
        1_161,
    )
    .await;
    let authentic = conflict_heads[0].clone();
    conflict_heads[0] = PendingNodeResultHead::new(
        authentic.activation().clone(),
        authentic.intent_digest(),
        authentic.fence().clone(),
        authentic.journal_head().clone(),
        Digest::sha256(b"substituted barrier result digest"),
    )
    .unwrap();
    let conflict_successor_id = CheckpointId::generate();
    let conflict_barrier = CheckpointBarrier::new(
        conflict_base.checkpoint(),
        CheckpointWrite::successor(
            conflict_successor_id,
            conflict_base.checkpoint(),
            checkpoint_state(conflict_base.checkpoint().graph(), 1),
            ready_node(2),
        )
        .unwrap(),
        conflict_heads,
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker_barrier(
                worker_append(
                    conflict_tenant.clone(),
                    conflict_run,
                    EventId::generate(),
                    JournalExpectation::exact(conflict_journal),
                    conflict_lease.fence().clone(),
                    1_162,
                ),
                RunProjection::unchanged(),
                conflict_barrier,
            )
            .await,
        Err(StoreError::CheckpointBarrierResultConflict)
    ));
    assert!(matches!(
        store
            .load_checkpoint(&conflict_tenant, conflict_run, conflict_successor_id)
            .await,
        Err(StoreError::CheckpointNotFound)
    ));

    let stale_tenant = tenant("barrier-stale-fence");
    let stale_run = RunId::generate();
    let stale_base = Box::pin(start_run_with_ready_checkpoint(
        &store,
        &stale_tenant,
        stale_run,
        1_170,
        ready_node(1),
    ))
    .await;
    let stale_lease = store
        .claim_lease(&stale_tenant, stale_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (stale_heads, stale_journal) =
        commit_ready_results(&store, stale_base.checkpoint(), stale_lease.fence(), 1_171).await;
    store
        .supersede_lease(&stale_tenant, stale_run, AttemptId::generate())
        .await
        .unwrap();
    let stale_barrier = CheckpointBarrier::new(
        stale_base.checkpoint(),
        CheckpointWrite::successor(
            CheckpointId::generate(),
            stale_base.checkpoint(),
            checkpoint_state(stale_base.checkpoint().graph(), 1),
            ready_node(2),
        )
        .unwrap(),
        stale_heads,
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker_barrier(
                worker_append(
                    stale_tenant.clone(),
                    stale_run,
                    EventId::generate(),
                    JournalExpectation::exact(stale_journal.clone()),
                    stale_lease.fence().clone(),
                    1_172,
                ),
                RunProjection::unchanged(),
                stale_barrier.clone(),
            )
            .await,
        Err(StoreError::StaleFence)
    ));
    let control_plane = store
        .append_control_plane_barrier(
            control_append(
                stale_tenant,
                stale_run,
                EventId::generate(),
                JournalExpectation::exact(stale_journal),
                1_173,
            ),
            RunProjection::unchanged(),
            stale_barrier,
        )
        .await
        .expect("control plane must be able to commit the still-current complete barrier");
    assert!(matches!(
        control_plane,
        BarrierCommitOutcome::Committed { .. }
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn barrier_rejects_complete_results_while_external_invocations_are_unsettled() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };

    let tool_tenant = tenant("barrier-unsettled-tool");
    let tool_run = RunId::generate();
    let tool_base = Box::pin(start_run_with_checkpoint(
        &store,
        &tool_tenant,
        tool_run,
        1_174,
    ))
    .await;
    let tool_lease = store
        .claim_lease(&tool_tenant, tool_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let tool_intent = tool_invocation_intent(tool_base.checkpoint(), InvocationId::generate());
    let tool_prepared = store
        .prepare_tool_invocation(
            worker_append(
                tool_tenant.clone(),
                tool_run,
                EventId::generate(),
                JournalExpectation::exact(tool_base.event().head()),
                tool_lease.fence().clone(),
                1_175,
            ),
            tool_intent.clone(),
        )
        .await
        .unwrap();
    let tool_result = store
        .commit_test_pending_node_result(
            worker_append(
                tool_tenant.clone(),
                tool_run,
                EventId::generate(),
                JournalExpectation::exact(tool_prepared.event().head()),
                tool_lease.fence().clone(),
                1_176,
            ),
            pending_result_intent(
                tool_intent.activation().clone(),
                NodeInvocationBindings::empty(),
            ),
        )
        .await
        .unwrap();
    let tool_barrier = CheckpointBarrier::new(
        tool_base.checkpoint(),
        successor_checkpoint_write(CheckpointId::generate(), tool_base.checkpoint(), 1),
        [tool_result.result().head()],
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker_barrier(
                worker_append(
                    tool_tenant.clone(),
                    tool_run,
                    EventId::generate(),
                    JournalExpectation::exact(tool_result.event().head()),
                    tool_lease.fence().clone(),
                    1_177,
                ),
                RunProjection::unchanged(),
                tool_barrier,
            )
            .await,
        Err(StoreError::CheckpointBlockedByToolInvocation)
    ));
    assert_eq!(
        store
            .load_current_checkpoint(&tool_tenant, tool_run)
            .await
            .unwrap(),
        Some(tool_base.checkpoint().clone())
    );

    let model_tenant = tenant("barrier-unsettled-model");
    let model_run = RunId::generate();
    let model_base = Box::pin(start_run_with_checkpoint(
        &store,
        &model_tenant,
        model_run,
        1_178,
    ))
    .await;
    let model_lease = store
        .claim_lease(&model_tenant, model_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let model_intent = model_invocation_intent(model_base.checkpoint(), InvocationId::generate());
    let model_prepared = store
        .prepare_model_invocation(
            worker_append(
                model_tenant.clone(),
                model_run,
                EventId::generate(),
                JournalExpectation::exact(model_base.event().head()),
                model_lease.fence().clone(),
                1_179,
            ),
            model_intent.clone(),
        )
        .await
        .unwrap();
    let model_result = store
        .commit_test_pending_node_result(
            worker_append(
                model_tenant.clone(),
                model_run,
                EventId::generate(),
                JournalExpectation::exact(model_prepared.event().head()),
                model_lease.fence().clone(),
                1_180,
            ),
            pending_result_intent(
                model_intent.activation().clone(),
                NodeInvocationBindings::empty(),
            ),
        )
        .await
        .unwrap();
    let model_barrier = CheckpointBarrier::new(
        model_base.checkpoint(),
        successor_checkpoint_write(CheckpointId::generate(), model_base.checkpoint(), 1),
        [model_result.result().head()],
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker_barrier(
                worker_append(
                    model_tenant.clone(),
                    model_run,
                    EventId::generate(),
                    JournalExpectation::exact(model_result.event().head()),
                    model_lease.fence().clone(),
                    1_181,
                ),
                RunProjection::unchanged(),
                model_barrier,
            )
            .await,
        Err(StoreError::CheckpointBlockedByModelInvocation)
    ));
    assert_eq!(
        store
            .load_current_checkpoint(&model_tenant, model_run)
            .await
            .unwrap(),
        Some(model_base.checkpoint().clone())
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn barrier_consumption_failure_rolls_back_event_checkpoint_and_run_heads() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("barrier-rollback");
    let run_id = RunId::generate();
    let base = Box::pin(start_run_with_ready_checkpoint(
        &store,
        &tenant_id,
        run_id,
        1_180,
        ready_node(1),
    ))
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (result_heads, result_journal) =
        commit_ready_results(&store, base.checkpoint(), lease.fence(), 1_181).await;
    let successor_id = CheckpointId::generate();
    let barrier = CheckpointBarrier::new(
        base.checkpoint(),
        CheckpointWrite::successor(
            successor_id,
            base.checkpoint(),
            checkpoint_state(base.checkpoint().graph(), 1),
            ready_node(2),
        )
        .unwrap(),
        result_heads,
    )
    .unwrap();

    query(
        "ALTER TABLE stateknot.pending_node_result_consumptions \
         DROP CONSTRAINT IF EXISTS test_barrier_consumption_rollback",
    )
    .execute(&administration)
    .await
    .unwrap();
    let reject_target = format!(
        "ALTER TABLE stateknot.pending_node_result_consumptions \
         ADD CONSTRAINT test_barrier_consumption_rollback \
         CHECK (tenant_id <> '{}') NOT VALID",
        tenant_id.as_str()
    );
    query(&reject_target)
        .execute(&administration)
        .await
        .unwrap();
    let barrier_event_id = EventId::generate();
    let result = store
        .append_control_plane_barrier(
            control_append(
                tenant_id.clone(),
                run_id,
                barrier_event_id,
                JournalExpectation::exact(result_journal.clone()),
                1_182,
            ),
            RunProjection::unchanged(),
            barrier,
        )
        .await;
    query(
        "ALTER TABLE stateknot.pending_node_result_consumptions \
         DROP CONSTRAINT test_barrier_consumption_rollback",
    )
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(result, Err(StoreError::Database { .. })));

    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(run.journal_head(), Some(&result_journal));
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap(),
        Some(base.checkpoint().clone())
    );
    assert!(matches!(
        store
            .load_checkpoint(&tenant_id, run_id, successor_id)
            .await,
        Err(StoreError::CheckpointNotFound)
    ));
    let consumption_count = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.pending_node_result_consumptions \
         WHERE tenant_id = $1 AND run_id = $2 AND base_checkpoint_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*base.checkpoint().checkpoint_id().as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!(consumption_count, 0);
    let events = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(events.events().len(), 3);
    assert!(
        events
            .events()
            .iter()
            .all(|event| event.event_id() != barrier_event_id)
    );
    let pending = store
        .load_unconsumed_pending_node_result_page(
            &base.checkpoint().head(),
            None,
            PendingNodeResultPageSize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pending.records().len(), 1);
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_identical_barriers_converge_on_one_physical_commit() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("barrier-concurrency");
    let run_id = RunId::generate();
    let base = Box::pin(start_run_with_ready_checkpoint(
        &store,
        &tenant_id,
        run_id,
        1_190,
        ready_node(1),
    ))
    .await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (result_heads, result_journal) =
        commit_ready_results(&store, base.checkpoint(), lease.fence(), 1_191).await;
    let barrier = CheckpointBarrier::new(
        base.checkpoint(),
        CheckpointWrite::successor(
            CheckpointId::generate(),
            base.checkpoint(),
            checkpoint_state(base.checkpoint().graph(), 1),
            ready_node(2),
        )
        .unwrap(),
        result_heads,
    )
    .unwrap();
    let append = worker_append(
        tenant_id.clone(),
        run_id,
        EventId::generate(),
        JournalExpectation::exact(result_journal),
        lease.fence().clone(),
        1_192,
    );
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let append = append.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            store
                .append_worker_barrier(append, RunProjection::unchanged(), barrier)
                .await
        });
    }

    let mut committed = 0_u64;
    let mut idempotent = 0_u64;
    let mut winner = None;
    while let Some(joined) = tasks.join_next().await {
        let outcome = joined
            .expect("barrier task must not panic")
            .expect("identical barrier contenders must converge");
        let identity = (outcome.event().head(), outcome.checkpoint().head());
        if let Some(winner) = &winner {
            assert_eq!(&identity, winner);
        } else {
            winner = Some(identity);
        }
        match outcome {
            BarrierCommitOutcome::Committed { .. } => committed += 1,
            BarrierCommitOutcome::Idempotent { .. } => idempotent += 1,
            _ => panic!("unexpected barrier outcome"),
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(idempotent, 23);
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap()
            .unwrap()
            .head(),
        winner.unwrap().1
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn pending_node_result_bindings_prove_exact_committed_tool_and_model_revisions() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("pending-node-result-bindings");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_120)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"shared bound activation");

    let tool_intent =
        tool_invocation_intent_for_activation(activation.clone(), InvocationId::generate());
    let tool_prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                1_121,
            ),
            tool_intent.clone(),
        )
        .await
        .unwrap();
    let tool_attempt = AttemptId::generate();
    let tool_executing = store
        .advance_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(tool_prepared.event().head()),
                lease.fence().clone(),
                1_122,
            ),
            &tool_prepared.invocation().head(),
            ToolInvocationTransition::StartAttempt {
                attempt_id: tool_attempt,
            },
        )
        .await
        .unwrap();
    let tool_committed = store
        .advance_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(tool_executing.event().head()),
                lease.fence().clone(),
                1_123,
            ),
            &tool_executing.invocation().head(),
            ToolInvocationTransition::RecordResult {
                result: tool_result(&tool_intent, tool_attempt),
            },
        )
        .await
        .unwrap();

    let model_intent =
        model_invocation_intent_for_activation(activation.clone(), InvocationId::generate());
    let model_prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(tool_committed.event().head()),
                lease.fence().clone(),
                1_124,
            ),
            model_intent.clone(),
        )
        .await
        .unwrap();
    let model_attempt = AttemptId::generate();
    let model_executing = store
        .advance_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(model_prepared.event().head()),
                lease.fence().clone(),
                1_125,
            ),
            &model_prepared.invocation().head(),
            ModelInvocationTransition::StartAttempt {
                attempt_id: model_attempt,
            },
        )
        .await
        .unwrap();
    let model_committed = store
        .advance_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(model_executing.event().head()),
                lease.fence().clone(),
                1_126,
            ),
            &model_executing.invocation().head(),
            ModelInvocationTransition::RecordResponse {
                response: model_response(&model_intent, model_attempt),
            },
        )
        .await
        .unwrap();

    let bindings = NodeInvocationBindings::try_new(
        &activation,
        [
            NodeInvocationBinding::from_tool(tool_committed.invocation()).unwrap(),
            NodeInvocationBinding::from_model(model_committed.invocation()).unwrap(),
        ],
    )
    .unwrap();
    let intent = pending_result_intent(activation.clone(), bindings);
    let committed = store
        .commit_test_pending_node_result(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(model_committed.event().head()),
                lease.fence().clone(),
                1_127,
            ),
            intent,
        )
        .await
        .expect("exact committed tool and model bindings must commit");
    assert_eq!(committed.result().intent().bindings().len(), 2);
    let restored = store
        .load_pending_node_result(&activation)
        .await
        .expect("bound pending result must verify every full invocation record");
    assert_eq!(restored, *committed.result());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn pending_node_result_recovery_batches_large_binding_sets() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("pending-node-result-binding-batches");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_130)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"batched bound activation");
    let mut journal_head = checkpoint.event().head();
    let mut event_index = 1_131_u64;
    let mut bindings = Vec::new();

    for _ in 0..5 {
        let intent =
            tool_invocation_intent_for_activation(activation.clone(), InvocationId::generate());
        let prepared = store
            .prepare_tool_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(journal_head),
                    lease.fence().clone(),
                    event_index,
                ),
                intent.clone(),
            )
            .await
            .unwrap();
        event_index += 1;
        let attempt = AttemptId::generate();
        let executing = store
            .advance_tool_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(prepared.event().head()),
                    lease.fence().clone(),
                    event_index,
                ),
                &prepared.invocation().head(),
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt,
                },
            )
            .await
            .unwrap();
        event_index += 1;
        let committed = store
            .advance_tool_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(executing.event().head()),
                    lease.fence().clone(),
                    event_index,
                ),
                &executing.invocation().head(),
                ToolInvocationTransition::RecordResult {
                    result: tool_result(&intent, attempt),
                },
            )
            .await
            .unwrap();
        event_index += 1;
        journal_head = committed.event().head();
        bindings.push(NodeInvocationBinding::from_tool(committed.invocation()).unwrap());
    }

    for _ in 0..4 {
        let intent =
            model_invocation_intent_for_activation(activation.clone(), InvocationId::generate());
        let prepared = store
            .prepare_model_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(journal_head),
                    lease.fence().clone(),
                    event_index,
                ),
                intent.clone(),
            )
            .await
            .unwrap();
        event_index += 1;
        let attempt = AttemptId::generate();
        let executing = store
            .advance_model_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(prepared.event().head()),
                    lease.fence().clone(),
                    event_index,
                ),
                &prepared.invocation().head(),
                ModelInvocationTransition::StartAttempt {
                    attempt_id: attempt,
                },
            )
            .await
            .unwrap();
        event_index += 1;
        let committed = store
            .advance_model_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(executing.event().head()),
                    lease.fence().clone(),
                    event_index,
                ),
                &executing.invocation().head(),
                ModelInvocationTransition::RecordResponse {
                    response: model_response(&intent, attempt),
                },
            )
            .await
            .unwrap();
        event_index += 1;
        journal_head = committed.event().head();
        bindings.push(NodeInvocationBinding::from_model(committed.invocation()).unwrap());
    }

    let bindings = NodeInvocationBindings::try_new(&activation, bindings).unwrap();
    let committed = store
        .commit_test_pending_node_result(
            worker_append(
                tenant_id,
                run_id,
                EventId::generate(),
                JournalExpectation::exact(journal_head),
                lease.fence().clone(),
                event_index,
            ),
            pending_result_intent(activation.clone(), bindings),
        )
        .await
        .unwrap();
    assert_eq!(committed.result().intent().bindings().len(), 9);
    assert_eq!(
        store.load_pending_node_result(&activation).await.unwrap(),
        *committed.result()
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_identical_node_attempt_retries_converge_on_one_physical_winner() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("pending-node-result-concurrency");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_140)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"concurrent activation");
    let intent = pending_result_intent(activation.clone(), NodeInvocationBindings::empty());
    let parent = checkpoint.event().head();
    let start_event_id = EventId::generate();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24_u64 {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        let intent = intent.clone();
        let fence = lease.fence().clone();
        let parent = parent.clone();
        tasks.spawn(async move {
            for _ in 0..64 {
                match store
                    .commit_test_pending_node_result(
                        worker_append(
                            tenant_id.clone(),
                            run_id,
                            start_event_id,
                            JournalExpectation::exact(parent.clone()),
                            fence.clone(),
                            1_141,
                        ),
                        intent.clone(),
                    )
                    .await
                {
                    result @ Ok(_) => return result,
                    Err(error) if error.is_retryable() => tokio::task::yield_now().await,
                    error @ Err(_) => return error,
                }
            }
            panic!("identical node-attempt retries did not converge within the test bound")
        });
    }

    let mut committed = 0_u64;
    let mut idempotent = 0_u64;
    let mut winner = None;
    while let Some(joined) = tasks.join_next().await {
        let outcome = joined
            .expect("pending result task must not panic")
            .expect("all semantic contenders must converge");
        match outcome {
            PendingNodeResultCommitOutcome::Committed { event, result } => {
                committed += 1;
                winner = Some((event.head(), result.head()));
            }
            PendingNodeResultCommitOutcome::Idempotent { event, result } => {
                idempotent += 1;
                let observed = (event.head(), result.head());
                if let Some(winner) = &winner {
                    assert_eq!(&observed, winner);
                } else {
                    winner = Some(observed);
                }
            }
            _ => panic!("unexpected pending node result outcome"),
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(idempotent, 23);
    let restored = store.load_pending_node_result(&activation).await.unwrap();
    assert_eq!(restored.head(), winner.unwrap().1);
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 3);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn invalid_pending_binding_rolls_back_event_result_bindings_and_run_head() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("pending-node-result-invalid-binding");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_170)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"missing bound invocation");

    let base_time = checkpoint.event().recorded_at();
    let fake_head = |sequence: u64, label: &'static [u8]| {
        JournalHead::new(
            tenant_id.clone(),
            run_id,
            JournalSequence::new(sequence).unwrap(),
            EventId::generate(),
            base_time,
            Digest::sha256(label),
        )
    };
    let fake_intent =
        tool_invocation_intent_for_activation(activation.clone(), InvocationId::generate());
    let fake_prepared =
        ToolInvocation::prepare(fake_intent.clone(), fake_head(2, b"missing prepare event"))
            .unwrap();
    let fake_attempt = AttemptId::generate();
    let fake_executing = fake_prepared
        .advance(
            ToolInvocationTransition::StartAttempt {
                attempt_id: fake_attempt,
            },
            fake_head(3, b"missing start event"),
        )
        .unwrap();
    let fake_committed = fake_executing
        .advance(
            ToolInvocationTransition::RecordResult {
                result: tool_result(&fake_intent, fake_attempt),
            },
            fake_head(4, b"missing result event"),
        )
        .unwrap();
    let bindings = NodeInvocationBindings::try_new(
        &activation,
        [NodeInvocationBinding::from_tool(&fake_committed).unwrap()],
    )
    .unwrap();

    let mut durable_head = checkpoint.event().head();
    for offset in 0..4_u64 {
        let outcome = store
            .append_worker(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(durable_head),
                    lease.fence().clone(),
                    1_171 + offset,
                ),
                RunProjection::unchanged(),
            )
            .await
            .unwrap();
        durable_head = match outcome {
            AppendOutcome::Committed(event) | AppendOutcome::Idempotent(event) => event.head(),
            _ => panic!("unexpected journal append outcome"),
        };
    }
    assert_eq!(durable_head.sequence().get(), 5);
    let result_event_id = EventId::generate();
    let commit = store
        .commit_test_pending_node_result(
            worker_append(
                tenant_id.clone(),
                run_id,
                result_event_id,
                JournalExpectation::exact(durable_head.clone()),
                lease.fence().clone(),
                1_175,
            ),
            pending_result_intent(activation.clone(), bindings),
        )
        .await;
    assert!(matches!(
        commit,
        Err(StoreError::InvalidPendingNodeResultBinding)
    ));
    assert!(matches!(
        store.load_pending_node_result(&activation).await,
        Err(StoreError::PendingNodeResultNotFound)
    ));
    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(run.journal_head().unwrap().sequence().get(), 6);
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 6);
    assert_eq!(journal.events().last().unwrap().event_id(), result_event_id);
    assert_eq!(
        store
            .load_node_attempt(
                &tenant_id,
                &run_id,
                AttemptId::from_uuid(*result_event_id.as_uuid()).unwrap(),
            )
            .await
            .unwrap()
            .status(),
        NodeAttemptStatus::Executing
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn cancellation_blocks_new_pending_results_but_preserves_committed_idempotency() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };

    let blocked_tenant = tenant("pending-result-cancellation-block");
    let blocked_run = RunId::generate();
    let blocked_checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &blocked_tenant,
        blocked_run,
        1_180,
    ))
    .await;
    let blocked_lease = store
        .claim_lease(&blocked_tenant, blocked_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let active = store.load_run(&blocked_tenant, blocked_run).await.unwrap();
    let cancellation = store
        .append_control_plane(
            control_append(
                blocked_tenant.clone(),
                blocked_run,
                EventId::generate(),
                JournalExpectation::exact(blocked_checkpoint.event().head()),
                1_181,
            ),
            RunProjection::transition(
                active.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: cancellation_request(blocked_checkpoint.event().recorded_at()),
                },
            ),
        )
        .await
        .unwrap();
    let blocked_activation = pending_activation(
        blocked_checkpoint.checkpoint(),
        b"cancelled pending activation",
    );
    assert!(matches!(
        store
            .commit_test_pending_node_result(
                worker_append(
                    blocked_tenant.clone(),
                    blocked_run,
                    EventId::generate(),
                    JournalExpectation::exact(cancellation.event().head()),
                    blocked_lease.fence().clone(),
                    1_182,
                ),
                pending_result_intent(blocked_activation.clone(), NodeInvocationBindings::empty(),),
            )
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    assert!(matches!(
        store.load_pending_node_result(&blocked_activation).await,
        Err(StoreError::PendingNodeResultNotFound)
    ));

    let durable_tenant = tenant("pending-result-cancellation-idempotency");
    let durable_run = RunId::generate();
    let durable_checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &durable_tenant,
        durable_run,
        1_190,
    ))
    .await;
    let durable_lease = store
        .claim_lease(&durable_tenant, durable_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let durable_activation = pending_activation(
        durable_checkpoint.checkpoint(),
        b"durable pending activation",
    );
    let durable_intent =
        pending_result_intent(durable_activation.clone(), NodeInvocationBindings::empty());
    let durable_start_event_id = EventId::generate();
    let committed = store
        .commit_test_pending_node_result(
            worker_append(
                durable_tenant.clone(),
                durable_run,
                durable_start_event_id,
                JournalExpectation::exact(durable_checkpoint.event().head()),
                durable_lease.fence().clone(),
                1_191,
            ),
            durable_intent.clone(),
        )
        .await
        .unwrap();
    let active = store.load_run(&durable_tenant, durable_run).await.unwrap();
    let cancelled = store
        .append_control_plane(
            control_append(
                durable_tenant.clone(),
                durable_run,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                1_192,
            ),
            RunProjection::transition(
                active.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: cancellation_request(committed.event().recorded_at()),
                },
            ),
        )
        .await
        .unwrap();
    assert_eq!(cancelled.event().sequence().get(), 4);
    let retry = store
        .commit_test_pending_node_result(
            worker_append(
                durable_tenant,
                durable_run,
                durable_start_event_id,
                JournalExpectation::exact(durable_checkpoint.event().head()),
                durable_lease.fence().clone(),
                1_191,
            ),
            durable_intent,
        )
        .await
        .expect("committed result must remain idempotent after cancellation");
    assert!(matches!(
        retry,
        PendingNodeResultCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(retry.result(), committed.result());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_node_result_recovery_rejects_noncanonical_or_corrupted_bytes() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("pending-result-corruption");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_200)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"corruption activation");
    store
        .commit_test_pending_node_result(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                1_201,
            ),
            pending_result_intent(activation.clone(), NodeInvocationBindings::empty()),
        )
        .await
        .unwrap();
    let updated = query(
        "UPDATE stateknot.pending_node_results \
         SET result_bytes = $3 \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(b"{}".as_slice())
    .execute(&administration)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(updated, 1);
    assert!(matches!(
        store.load_pending_node_result(&activation).await,
        Err(StoreError::CorruptData { .. })
    ));
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migrations_admission_projection_idempotency_and_pages() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    PostgresStore::migrate_database(&database_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration retry must be safe");
    store
        .verify_schema()
        .await
        .expect("schema verification retry must be safe");

    let tenant_id = tenant("admission");
    let run_id = RunId::generate();
    let provenance = provenance(tenant_id.clone(), run_id);
    let admitted = store
        .admit_run(provenance.clone())
        .await
        .expect("admission must commit");
    assert!(matches!(admitted, AdmissionOutcome::Committed(_)));
    let admitted_lifecycle = admitted.lifecycle().clone();
    let retry = store
        .admit_run(provenance)
        .await
        .expect("admission retry must converge");
    assert!(matches!(retry, AdmissionOutcome::Idempotent(_)));

    let started_at = Timestamp::from_unix_micros(
        admitted_lifecycle
            .admitted_at()
            .unix_micros()
            .checked_add(1)
            .unwrap(),
    )
    .unwrap();
    let active = admitted_lifecycle
        .clone()
        .apply(RunTransition::Start { started_at })
        .unwrap();
    let start_transition = RunTransition::Start { started_at };
    let future_transition = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                0,
            ),
            RunProjection::transition(
                admitted_lifecycle.revision(),
                RunTransition::Start {
                    started_at: Timestamp::MAX,
                },
            ),
        )
        .await;
    assert!(matches!(
        future_transition,
        Err(StoreError::LifecycleObservationAfterCommit)
    ));
    let unchanged = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(unchanged.lifecycle().revision().get(), 0);
    assert!(unchanged.journal_head().is_none());
    let first_event_id = EventId::generate();
    let first = control_append(
        tenant_id.clone(),
        run_id,
        first_event_id,
        JournalExpectation::empty(),
        1,
    );
    let committed = store
        .append_control_plane(
            first,
            RunProjection::transition(admitted_lifecycle.revision(), start_transition.clone()),
        )
        .await
        .expect("first event and lifecycle must commit atomically");
    assert!(matches!(committed, AppendOutcome::Committed(_)));
    assert_eq!(committed.event().sequence().get(), 1);

    let retry = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                first_event_id,
                JournalExpectation::empty(),
                1,
            ),
            RunProjection::transition(admitted_lifecycle.revision(), start_transition),
        )
        .await
        .expect("lost acknowledgement retry must converge before projection checks");
    assert!(matches!(retry, AppendOutcome::Idempotent(_)));

    let projection_conflict = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                first_event_id,
                JournalExpectation::empty(),
                1,
            ),
            RunProjection::unchanged(),
        )
        .await;
    assert!(matches!(
        projection_conflict,
        Err(StoreError::ProjectionIntentConflict)
    ));

    let conflict = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                first_event_id,
                JournalExpectation::empty(),
                999,
            ),
            RunProjection::unchanged(),
        )
        .await;
    assert!(matches!(conflict, Err(StoreError::EventIdConflict)));

    let stale = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                2,
            ),
            RunProjection::unchanged(),
        )
        .await;
    assert!(matches!(stale, Err(StoreError::StaleJournalHead)));

    let invalid_transition = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                3,
            ),
            RunProjection::transition(active.revision(), RunTransition::Start { started_at }),
        )
        .await;
    assert!(matches!(
        invalid_transition,
        Err(StoreError::InvalidLifecycleTransition)
    ));

    let second = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                2,
            ),
            RunProjection::unchanged(),
        )
        .await
        .expect("exact successor must commit");
    assert_eq!(second.event().sequence().get(), 2);

    let first_page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(1).unwrap())
        .await
        .expect("first page must validate");
    assert_eq!(first_page.events().len(), 1);
    assert!(first_page.has_more());
    let first_cursor = first_page.events()[0].head();
    let final_page = store
        .load_journal_page(
            &tenant_id,
            run_id,
            Some(&first_cursor),
            JournalPageSize::new(10).unwrap(),
        )
        .await
        .expect("suffix page must validate to the run head");
    assert_eq!(final_page.events().len(), 1);
    assert!(!final_page.has_more());
    assert_eq!(final_page.events()[0].digest(), second.event().digest());

    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(stored.lifecycle().revision().get(), 1);
    assert_eq!(stored.journal_head(), Some(&second.event().head()));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn leases_fence_late_workers_and_preserve_lost_ack_retries() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("lease");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    let first_attempt = AttemptId::generate();
    let first_claim = store
        .claim_lease(&tenant_id, run_id, first_attempt)
        .await
        .unwrap();
    assert!(matches!(first_claim, LeaseClaimOutcome::Claimed(_)));
    let first_lease = first_claim.lease().clone();
    assert_eq!(first_lease.fence().epoch().get(), 1);
    let claim_retry = store
        .claim_lease(&tenant_id, run_id, first_attempt)
        .await
        .unwrap();
    assert!(matches!(claim_retry, LeaseClaimOutcome::Idempotent(_)));
    assert!(matches!(
        store
            .claim_lease(&tenant_id, run_id, AttemptId::generate())
            .await,
        Err(StoreError::LeaseHeld)
    ));

    let first_event_id = EventId::generate();
    let first_worker_append = || {
        JournalAppend::new(
            JournalExpectation::empty(),
            JournalEventIntent::worker(
                tenant_id.clone(),
                run_id,
                first_event_id,
                first_lease.fence().clone(),
                payload(10),
            )
            .unwrap(),
        )
        .unwrap()
    };
    let first_event = store
        .append_worker(first_worker_append(), RunProjection::unchanged())
        .await
        .unwrap();

    let desired_expiry = Timestamp::from_unix_micros(
        first_lease
            .expires_at()
            .unix_micros()
            .checked_add(1_000_000)
            .unwrap(),
    )
    .unwrap();
    let renewed = store
        .renew_lease(first_lease.fence(), desired_expiry)
        .await
        .unwrap();
    assert!(matches!(renewed, LeaseRenewalOutcome::Renewed(_)));
    let renewal_retry = store
        .renew_lease(first_lease.fence(), desired_expiry)
        .await
        .unwrap();
    assert!(matches!(renewal_retry, LeaseRenewalOutcome::Idempotent(_)));

    assert_eq!(
        store.release_lease(first_lease.fence()).await.unwrap(),
        LeaseReleaseOutcome::Released
    );
    assert_eq!(
        store.release_lease(first_lease.fence()).await.unwrap(),
        LeaseReleaseOutcome::Idempotent
    );
    let lost_ack = store
        .append_worker(first_worker_append(), RunProjection::unchanged())
        .await
        .expect("committed event must remain observable after lease release");
    assert!(matches!(lost_ack, AppendOutcome::Idempotent(_)));

    let stale_new_event = JournalAppend::new(
        JournalExpectation::exact(first_event.event().head()),
        JournalEventIntent::worker(
            tenant_id.clone(),
            run_id,
            EventId::generate(),
            first_lease.fence().clone(),
            payload(11),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker(stale_new_event, RunProjection::unchanged())
            .await,
        Err(StoreError::NoActiveLease)
    ));

    let second_claim = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap();
    let second_lease = second_claim.lease().clone();
    assert_eq!(second_lease.fence().epoch().get(), 2);
    assert_ne!(
        second_lease.fence().attempt_id(),
        first_lease.fence().attempt_id()
    );
    let stale_after_takeover = JournalAppend::new(
        JournalExpectation::exact(first_event.event().head()),
        JournalEventIntent::worker(
            tenant_id.clone(),
            run_id,
            EventId::generate(),
            first_lease.fence().clone(),
            payload(12),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker(stale_after_takeover, RunProjection::unchanged())
            .await,
        Err(StoreError::StaleFence)
    ));

    let forced = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .expect("trusted takeover must supersede an unexpired lease");
    let forced_lease = forced.lease();
    assert_eq!(forced_lease.fence().epoch().get(), 3);
    let forced_retry = store
        .supersede_lease(&tenant_id, run_id, forced_lease.fence().attempt_id())
        .await
        .unwrap();
    assert!(matches!(forced_retry, LeaseClaimOutcome::Idempotent(_)));

    let second_stale = JournalAppend::new(
        JournalExpectation::exact(first_event.event().head()),
        JournalEventIntent::worker(
            tenant_id.clone(),
            run_id,
            EventId::generate(),
            second_lease.fence().clone(),
            payload(13),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        store
            .append_worker(second_stale, RunProjection::unchanged())
            .await,
        Err(StoreError::StaleFence)
    ));

    let current = JournalAppend::new(
        JournalExpectation::exact(first_event.event().head()),
        JournalEventIntent::worker(
            tenant_id.clone(),
            run_id,
            EventId::generate(),
            forced_lease.fence().clone(),
            payload(14),
        )
        .unwrap(),
    )
    .unwrap();
    let second_event = store
        .append_worker(current, RunProjection::unchanged())
        .await
        .expect("current fence must commit");
    assert_eq!(second_event.event().sequence().get(), 2);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_appenders_converge_to_one_contiguous_history() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("concurrency");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for index in 0..100_u64 {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        tasks.push(tokio::spawn(async move {
            let event_id = EventId::generate();
            loop {
                let run = store.load_run(&tenant_id, run_id).await.unwrap();
                let expectation = run
                    .journal_head()
                    .map_or_else(JournalExpectation::empty, |head| {
                        JournalExpectation::exact(head.clone())
                    });
                let append =
                    control_append(tenant_id.clone(), run_id, event_id, expectation, index);
                match store
                    .append_control_plane(append, RunProjection::unchanged())
                    .await
                {
                    Ok(outcome) => return outcome.event().sequence(),
                    Err(StoreError::StaleJournalHead) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected concurrent append failure: {error}"),
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("appender task must not panic");
    }

    let page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(128).unwrap())
        .await
        .expect("complete concurrent history must validate");
    assert_eq!(page.events().len(), 100);
    assert!(!page.has_more());
    for (index, event) in page.events().iter().enumerate() {
        assert_eq!(event.sequence().get(), u64::try_from(index).unwrap() + 1);
    }
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn checkpoint_commit_recovery_idempotency_and_projection_binding() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("checkpoint-recovery");
    let run_id = RunId::generate();
    let admitted = store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    let first_event_id = EventId::generate();
    let first_checkpoint_id = CheckpointId::generate();
    let first_append = control_append(
        tenant_id.clone(),
        run_id,
        first_event_id,
        JournalExpectation::empty(),
        100,
    );
    let first_write = initial_checkpoint_write(tenant_id.clone(), run_id, first_checkpoint_id);
    let first = store
        .append_control_plane_checkpoint(
            first_append.clone(),
            RunProjection::unchanged(),
            first_write.clone(),
        )
        .await
        .expect("initial checkpoint must commit atomically");
    assert!(matches!(first, CheckpointCommitOutcome::Committed { .. }));
    assert_eq!(first.event().sequence().get(), 1);
    assert_eq!(first.checkpoint().superstep().get(), 0);
    assert_eq!(first.checkpoint().journal_head(), &first.event().head());

    let stored = store.load_run(&tenant_id, run_id).await.unwrap();
    let pointer = stored
        .checkpoint()
        .expect("checkpoint pointer must advance");
    assert_eq!(pointer.checkpoint_id(), first_checkpoint_id);
    assert_eq!(pointer.digest(), first.checkpoint().digest());
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap(),
        Some(first.checkpoint().clone())
    );
    assert_eq!(
        store
            .load_checkpoint(&tenant_id, run_id, first_checkpoint_id)
            .await
            .unwrap(),
        first.checkpoint().clone()
    );

    let retry = store
        .append_control_plane_checkpoint(
            first_append.clone(),
            RunProjection::unchanged(),
            first_write.clone(),
        )
        .await
        .expect("lost checkpoint acknowledgement must converge");
    assert!(matches!(retry, CheckpointCommitOutcome::Idempotent { .. }));
    assert_eq!(retry.checkpoint(), first.checkpoint());

    let started_at = Timestamp::from_unix_micros(
        admitted
            .lifecycle()
            .admitted_at()
            .unix_micros()
            .checked_add(1)
            .unwrap(),
    )
    .unwrap();
    let projection_conflict = store
        .append_control_plane_checkpoint(
            first_append.clone(),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start { started_at },
            ),
            first_write.clone(),
        )
        .await;
    assert!(matches!(
        projection_conflict,
        Err(StoreError::ProjectionIntentConflict)
    ));

    let different_write =
        initial_checkpoint_write(tenant_id.clone(), run_id, CheckpointId::generate());
    assert!(matches!(
        store
            .append_control_plane_checkpoint(
                first_append.clone(),
                RunProjection::unchanged(),
                different_write,
            )
            .await,
        Err(StoreError::CheckpointCommitConflict)
    ));
    assert!(matches!(
        store
            .append_control_plane(first_append, RunProjection::unchanged())
            .await,
        Err(StoreError::CheckpointCommitConflict)
    ));

    let successor_id = CheckpointId::generate();
    assert!(matches!(
        store
            .append_control_plane_checkpoint(
                control_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(first.event().head()),
                    101,
                ),
                RunProjection::unchanged(),
                successor_checkpoint_write(successor_id, first.checkpoint(), 1),
            )
            .await,
        Err(StoreError::CheckpointBarrierRequired)
    ));
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap(),
        Some(first.checkpoint().clone())
    );
    assert!(matches!(
        store
            .load_checkpoint(&tenant_id, run_id, CheckpointId::generate())
            .await,
        Err(StoreError::CheckpointNotFound)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_commits_fence_stale_workers_but_preserve_lost_ack_retries() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("checkpoint-fence");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();

    let first_event_id = EventId::generate();
    let first_write = initial_checkpoint_write(tenant_id.clone(), run_id, CheckpointId::generate());
    let first_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            first_event_id,
            JournalExpectation::empty(),
            first_lease.fence().clone(),
            200,
        )
    };
    let first = store
        .append_worker_checkpoint(
            first_append(),
            RunProjection::unchanged(),
            first_write.clone(),
        )
        .await
        .unwrap();

    let _current_lease = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let lost_ack = store
        .append_worker_checkpoint(first_append(), RunProjection::unchanged(), first_write)
        .await
        .expect("already committed checkpoint must survive lease takeover");
    assert!(matches!(
        lost_ack,
        CheckpointCommitOutcome::Idempotent { .. }
    ));

    let stale_write = successor_checkpoint_write(CheckpointId::generate(), first.checkpoint(), 1);
    assert!(matches!(
        store
            .append_worker_checkpoint(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(first.event().head()),
                    first_lease.fence().clone(),
                    201,
                ),
                RunProjection::unchanged(),
                stale_write,
            )
            .await,
        Err(StoreError::CheckpointBarrierRequired)
    ));

    assert!(matches!(
        store
            .append_worker_checkpoint(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(first.event().head()),
                    first_lease.fence().clone(),
                    202,
                ),
                RunProjection::unchanged(),
                successor_checkpoint_write(CheckpointId::generate(), first.checkpoint(), 1),
            )
            .await,
        Err(StoreError::CheckpointBarrierRequired)
    ));
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap(),
        Some(first.checkpoint().clone())
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn tool_invocation_preparation_requires_a_ready_root_activation() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("tool-invocation-activation");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 880)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let ready_node = checkpoint
        .checkpoint()
        .ready_nodes()
        .iter()
        .next()
        .unwrap()
        .clone();
    let invalid_activations = [
        NodeActivation::new(
            checkpoint.checkpoint().head(),
            GraphNamespace::root(),
            NodeId::new("not-a-ready-node").unwrap(),
            Digest::sha256(b"invalid non-ready integration activation input"),
        ),
        NodeActivation::new(
            checkpoint.checkpoint().head(),
            GraphNamespace::new("nested").unwrap(),
            ready_node,
            Digest::sha256(b"invalid nested integration activation input"),
        ),
        drifted_pending_activation(
            checkpoint.checkpoint(),
            b"invalid canonical integration activation input",
        ),
    ];

    for (index, activation) in invalid_activations.into_iter().enumerate() {
        let descriptor = tool_descriptor();
        let invocation_id = InvocationId::generate();
        let intent = ToolInvocationIntent::new(
            activation,
            invocation_id,
            descriptor.clone(),
            tool_input(&descriptor),
            descriptor.limits().clone(),
        )
        .unwrap();
        assert!(matches!(
            store
                .prepare_tool_invocation(
                    worker_append(
                        tenant_id.clone(),
                        run_id,
                        EventId::generate(),
                        JournalExpectation::exact(checkpoint.event().head()),
                        lease.fence().clone(),
                        881 + u64::try_from(index).unwrap(),
                    ),
                    intent,
                )
                .await,
            Err(StoreError::InvalidToolInvocationActivation)
        ));
        assert!(matches!(
            store
                .load_tool_invocation(&tenant_id, run_id, invocation_id)
                .await,
            Err(StoreError::ToolInvocationNotFound)
        ));
    }

    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 1);
    assert_eq!(journal.events().last().unwrap(), checkpoint.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn model_invocation_preparation_requires_a_ready_root_activation() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("model-invocation-activation");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_020)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let ready_node = checkpoint
        .checkpoint()
        .ready_nodes()
        .iter()
        .next()
        .unwrap()
        .clone();
    let invalid_activations = [
        NodeActivation::new(
            checkpoint.checkpoint().head(),
            GraphNamespace::root(),
            NodeId::new("not-a-ready-model-node").unwrap(),
            Digest::sha256(b"invalid non-ready integration model activation input"),
        ),
        NodeActivation::new(
            checkpoint.checkpoint().head(),
            GraphNamespace::new("nested").unwrap(),
            ready_node,
            Digest::sha256(b"invalid nested integration model activation input"),
        ),
        drifted_pending_activation(
            checkpoint.checkpoint(),
            b"invalid canonical integration model activation input",
        ),
    ];

    for (index, activation) in invalid_activations.into_iter().enumerate() {
        let invocation_id = InvocationId::generate();
        let intent = ModelInvocationIntent::new(
            activation,
            invocation_id,
            model_descriptor(),
            model_request(),
        )
        .unwrap();
        assert!(matches!(
            store
                .prepare_model_invocation(
                    worker_append(
                        tenant_id.clone(),
                        run_id,
                        EventId::generate(),
                        JournalExpectation::exact(checkpoint.event().head()),
                        lease.fence().clone(),
                        1_021 + u64::try_from(index).unwrap(),
                    ),
                    intent,
                )
                .await,
            Err(StoreError::InvalidModelInvocationActivation)
        ));
        assert!(matches!(
            store
                .load_model_invocation(&tenant_id, run_id, invocation_id)
                .await,
            Err(StoreError::ModelInvocationNotFound)
        ));
    }

    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 1);
    assert_eq!(journal.events().last().unwrap(), checkpoint.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn cancellation_blocks_new_tool_work_but_accepts_an_inflight_result() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("tool-invocation-cancellation");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 890)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();

    let first_invocation_id = InvocationId::generate();
    let first_intent = tool_invocation_intent(checkpoint.checkpoint(), first_invocation_id);
    let first_prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                891,
            ),
            first_intent.clone(),
        )
        .await
        .unwrap();
    let first_attempt_id = AttemptId::generate();
    let first_executing = store
        .advance_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(first_prepared.event().head()),
                lease.fence().clone(),
                892,
            ),
            &first_prepared.invocation().head(),
            ToolInvocationTransition::StartAttempt {
                attempt_id: first_attempt_id,
            },
        )
        .await
        .unwrap();

    let second_invocation_id = InvocationId::generate();
    let second_prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(first_executing.event().head()),
                lease.fence().clone(),
                893,
            ),
            tool_invocation_intent(checkpoint.checkpoint(), second_invocation_id),
        )
        .await
        .unwrap();

    let active = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(active.lifecycle().status(), RunStatus::Active);
    let cancellation = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(second_prepared.event().head()),
                894,
            ),
            RunProjection::transition(
                active.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: cancellation_request(second_prepared.event().recorded_at()),
                },
            ),
        )
        .await
        .unwrap();

    assert!(matches!(
        store
            .advance_tool_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(cancellation.event().head()),
                    lease.fence().clone(),
                    895,
                ),
                &second_prepared.invocation().head(),
                ToolInvocationTransition::StartAttempt {
                    attempt_id: AttemptId::generate(),
                },
            )
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    let blocked_invocation_id = InvocationId::generate();
    assert!(matches!(
        store
            .prepare_tool_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(cancellation.event().head()),
                    lease.fence().clone(),
                    896,
                ),
                tool_invocation_intent(checkpoint.checkpoint(), blocked_invocation_id),
            )
            .await,
        Err(StoreError::RunNotRunnable)
    ));

    let completed = store
        .advance_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(cancellation.event().head()),
                lease.fence().clone(),
                897,
            ),
            &first_executing.invocation().head(),
            ToolInvocationTransition::RecordResult {
                result: tool_result(&first_intent, first_attempt_id),
            },
        )
        .await
        .expect("an in-flight external result remains durable after cancellation intent");
    assert_eq!(
        completed.invocation().status(),
        ToolInvocationStatus::Committed
    );
    assert_eq!(
        store
            .load_tool_invocation(&tenant_id, run_id, second_invocation_id)
            .await
            .unwrap()
            .status(),
        ToolInvocationStatus::Prepared
    );
    assert!(matches!(
        store
            .load_tool_invocation(&tenant_id, run_id, blocked_invocation_id)
            .await,
        Err(StoreError::ToolInvocationNotFound)
    ));
    assert_eq!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::CancellationRequested
    );
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 6);
    assert_eq!(journal.events().last().unwrap(), completed.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn cancellation_blocks_new_model_work_but_accepts_an_inflight_response() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("model-invocation-cancellation");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_030)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();

    let first_invocation_id = InvocationId::generate();
    let first_intent = model_invocation_intent(checkpoint.checkpoint(), first_invocation_id);
    let first_prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                1_031,
            ),
            first_intent.clone(),
        )
        .await
        .unwrap();
    let first_attempt_id = AttemptId::generate();
    let first_executing = store
        .advance_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(first_prepared.event().head()),
                lease.fence().clone(),
                1_032,
            ),
            &first_prepared.invocation().head(),
            ModelInvocationTransition::StartAttempt {
                attempt_id: first_attempt_id,
            },
        )
        .await
        .unwrap();

    let second_invocation_id = InvocationId::generate();
    let second_prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(first_executing.event().head()),
                lease.fence().clone(),
                1_033,
            ),
            model_invocation_intent(checkpoint.checkpoint(), second_invocation_id),
        )
        .await
        .unwrap();

    let active = store.load_run(&tenant_id, run_id).await.unwrap();
    let cancellation = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(second_prepared.event().head()),
                1_034,
            ),
            RunProjection::transition(
                active.lifecycle().revision(),
                RunTransition::RequestCancellation {
                    request: cancellation_request(second_prepared.event().recorded_at()),
                },
            ),
        )
        .await
        .unwrap();

    assert!(matches!(
        store
            .advance_model_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(cancellation.event().head()),
                    lease.fence().clone(),
                    1_035,
                ),
                &second_prepared.invocation().head(),
                ModelInvocationTransition::StartAttempt {
                    attempt_id: AttemptId::generate(),
                },
            )
            .await,
        Err(StoreError::RunNotRunnable)
    ));
    let blocked_invocation_id = InvocationId::generate();
    assert!(matches!(
        store
            .prepare_model_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(cancellation.event().head()),
                    lease.fence().clone(),
                    1_036,
                ),
                model_invocation_intent(checkpoint.checkpoint(), blocked_invocation_id),
            )
            .await,
        Err(StoreError::RunNotRunnable)
    ));

    let completed = store
        .advance_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(cancellation.event().head()),
                lease.fence().clone(),
                1_037,
            ),
            &first_executing.invocation().head(),
            ModelInvocationTransition::RecordResponse {
                response: model_response(&first_intent, first_attempt_id),
            },
        )
        .await
        .expect("an in-flight model response remains durable after cancellation intent");
    assert_eq!(
        completed.invocation().status(),
        ModelInvocationStatus::Committed
    );
    assert_eq!(
        store
            .load_model_invocation(&tenant_id, run_id, second_invocation_id)
            .await
            .unwrap()
            .status(),
        ModelInvocationStatus::Prepared
    );
    assert!(matches!(
        store
            .load_model_invocation(&tenant_id, run_id, blocked_invocation_id)
            .await,
        Err(StoreError::ModelInvocationNotFound)
    ));
    assert_eq!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .lifecycle()
            .status(),
        RunStatus::CancellationRequested
    );
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 6);
    assert_eq!(journal.events().last().unwrap(), completed.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn tool_invocations_are_atomic_fenced_idempotent_and_page_verifiable() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("tool-invocation");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 800)).await;
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();

    let invocation_id = InvocationId::generate();
    let intent = tool_invocation_intent(checkpoint.checkpoint(), invocation_id);
    let prepare_event_id = EventId::generate();
    let prepare_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            prepare_event_id,
            JournalExpectation::exact(checkpoint.event().head()),
            first_lease.fence().clone(),
            801,
        )
    };
    let prepared = store
        .prepare_tool_invocation(prepare_append(), intent.clone())
        .await
        .expect("tool invocation preparation must commit");
    assert!(matches!(
        prepared,
        ToolInvocationCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        prepared.invocation().status(),
        ToolInvocationStatus::Prepared
    );
    assert_eq!(prepared.event().sequence().get(), 2);

    let prepare_retry = store
        .prepare_tool_invocation(prepare_append(), intent.clone())
        .await
        .expect("lost preparation acknowledgement must converge");
    assert!(matches!(
        prepare_retry,
        ToolInvocationCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(
        prepare_retry.invocation().head(),
        prepared.invocation().head()
    );
    let crossed_invocation_id = InvocationId::generate();
    assert!(matches!(
        store
            .prepare_tool_invocation(
                prepare_append(),
                tool_invocation_intent(checkpoint.checkpoint(), crossed_invocation_id),
            )
            .await,
        Err(StoreError::ToolInvocationCommitConflict)
    ));
    assert!(matches!(
        store
            .load_tool_invocation(&tenant_id, run_id, crossed_invocation_id)
            .await,
        Err(StoreError::ToolInvocationNotFound)
    ));
    assert_eq!(
        store
            .load_tool_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .head(),
        prepared.invocation().head()
    );

    assert!(matches!(
        store
            .append_worker_checkpoint(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(prepared.event().head()),
                    first_lease.fence().clone(),
                    806,
                ),
                RunProjection::unchanged(),
                successor_checkpoint_write(CheckpointId::generate(), checkpoint.checkpoint(), 1,),
            )
            .await,
        Err(StoreError::CheckpointBarrierRequired)
    ));

    let physical_attempt = AttemptId::generate();
    let start_event_id = EventId::generate();
    let start_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            start_event_id,
            JournalExpectation::exact(prepared.event().head()),
            first_lease.fence().clone(),
            802,
        )
    };
    let executing = store
        .advance_tool_invocation(
            start_append(),
            &prepared.invocation().head(),
            ToolInvocationTransition::StartAttempt {
                attempt_id: physical_attempt,
            },
        )
        .await
        .expect("physical attempt claim must commit");
    assert_eq!(
        executing.invocation().status(),
        ToolInvocationStatus::Executing
    );

    let current_lease = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let start_retry = store
        .advance_tool_invocation(
            start_append(),
            &prepared.invocation().head(),
            ToolInvocationTransition::StartAttempt {
                attempt_id: physical_attempt,
            },
        )
        .await
        .expect("committed attempt claim must survive lease takeover");
    assert!(matches!(
        start_retry,
        ToolInvocationCommitOutcome::Idempotent { .. }
    ));

    let result_event_id = EventId::generate();
    let result_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            result_event_id,
            JournalExpectation::exact(executing.event().head()),
            current_lease.fence().clone(),
            803,
        )
    };
    let committed = store
        .advance_tool_invocation(
            result_append(),
            &executing.invocation().head(),
            ToolInvocationTransition::RecordResult {
                result: tool_result(&intent, physical_attempt),
            },
        )
        .await
        .expect("validated tool result must commit");
    assert_eq!(
        committed.invocation().status(),
        ToolInvocationStatus::Committed
    );
    assert_eq!(committed.invocation().revision().get(), 2);

    let result_retry = store
        .advance_tool_invocation(
            result_append(),
            &executing.invocation().head(),
            ToolInvocationTransition::RecordResult {
                result: tool_result(&intent, physical_attempt),
            },
        )
        .await
        .expect("lost result acknowledgement must converge");
    assert!(matches!(
        result_retry,
        ToolInvocationCommitOutcome::Idempotent { .. }
    ));

    let mut cursor = None;
    let mut statuses = Vec::new();
    loop {
        let page = store
            .load_tool_invocation_history_page(
                &tenant_id,
                run_id,
                invocation_id,
                cursor.as_ref(),
                ToolInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await
            .expect("each invocation history page must verify");
        statuses.extend(
            page.records()
                .iter()
                .map(stateknot_core::ToolInvocation::status),
        );
        cursor = page.next_cursor();
        if !page.has_more() {
            break;
        }
    }
    assert_eq!(
        statuses,
        vec![
            ToolInvocationStatus::Prepared,
            ToolInvocationStatus::Executing,
            ToolInvocationStatus::Committed,
        ]
    );
    assert_eq!(
        cursor.expect("final cursor").head(),
        committed.invocation().head()
    );

    assert!(matches!(
        store
            .advance_tool_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(committed.event().head()),
                    current_lease.fence().clone(),
                    804,
                ),
                &prepared.invocation().head(),
                ToolInvocationTransition::StartAttempt {
                    attempt_id: AttemptId::generate(),
                },
            )
            .await,
        Err(StoreError::StaleToolInvocationHead)
    ));

    let second_intent = tool_invocation_intent(checkpoint.checkpoint(), InvocationId::generate());
    assert!(matches!(
        store
            .prepare_tool_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(committed.event().head()),
                    first_lease.fence().clone(),
                    805,
                ),
                second_intent,
            )
            .await,
        Err(StoreError::StaleFence)
    ));

    let activation = intent.activation().clone();
    let bindings = NodeInvocationBindings::try_new(
        &activation,
        [NodeInvocationBinding::from_tool(committed.invocation()).unwrap()],
    )
    .unwrap();
    let pending = store
        .commit_test_pending_node_result(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                current_lease.fence().clone(),
                807,
            ),
            pending_result_intent(activation, bindings),
        )
        .await
        .expect("a settled tool binding must commit its node result");
    let barrier_intent = CheckpointBarrier::new(
        checkpoint.checkpoint(),
        successor_checkpoint_write(CheckpointId::generate(), checkpoint.checkpoint(), 1),
        [pending.result().head()],
    )
    .unwrap();
    let barrier = store
        .append_worker_barrier(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(pending.event().head()),
                current_lease.fence().clone(),
                808,
            ),
            RunProjection::unchanged(),
            barrier_intent,
        )
        .await
        .expect("a committed tool invocation must release the checkpoint barrier");
    assert_eq!(barrier.checkpoint().superstep().get(), 1);
    assert_eq!(barrier.event().sequence().get(), 7);
    assert_eq!(
        store
            .load_tool_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .status(),
        ToolInvocationStatus::Committed
    );

    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 7);
    assert_eq!(journal.events().last().unwrap(), barrier.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn failed_invocation_projection_update_rolls_back_event_and_revision() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("test administration connection must open");
    let tenant_id = tenant("tool-invocation-rollback");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 810)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let invocation_id = InvocationId::generate();
    let prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                811,
            ),
            tool_invocation_intent(checkpoint.checkpoint(), invocation_id),
        )
        .await
        .unwrap();

    query(
        "ALTER TABLE stateknot.tool_invocations \
         DROP CONSTRAINT IF EXISTS test_tool_invocation_rollback",
    )
    .execute(&administration)
    .await
    .unwrap();
    let reject_projection = format!(
        "ALTER TABLE stateknot.tool_invocations \
         ADD CONSTRAINT test_tool_invocation_rollback \
         CHECK (tenant_id <> '{}' OR current_status = 'prepared') NOT VALID",
        tenant_id.as_str()
    );
    query(&reject_projection)
        .execute(&administration)
        .await
        .unwrap();

    let advance = store
        .advance_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(prepared.event().head()),
                lease.fence().clone(),
                812,
            ),
            &prepared.invocation().head(),
            ToolInvocationTransition::StartAttempt {
                attempt_id: AttemptId::generate(),
            },
        )
        .await;

    query(
        "ALTER TABLE stateknot.tool_invocations \
         DROP CONSTRAINT test_tool_invocation_rollback",
    )
    .execute(&administration)
    .await
    .unwrap();
    administration.close().await;
    assert!(matches!(advance, Err(StoreError::Database { .. })));

    let current = store
        .load_tool_invocation(&tenant_id, run_id, invocation_id)
        .await
        .expect("a failed advance must retain the prepared projection");
    assert_eq!(current.head(), prepared.invocation().head());
    let history = store
        .load_tool_invocation_history_page(
            &tenant_id,
            run_id,
            invocation_id,
            None,
            ToolInvocationHistoryPageSize::new(2).unwrap(),
        )
        .await
        .expect("a rolled-back revision must not appear in history");
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].head(), prepared.invocation().head());
    assert!(!history.has_more());
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .expect("a rolled-back invocation event must not appear in the journal");
    assert_eq!(journal.events().len(), 2);
    assert_eq!(journal.events().last().unwrap(), prepared.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn concurrent_invocation_advances_admit_exactly_one_physical_attempt() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("tool-invocation-concurrency");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 820)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let invocation_id = InvocationId::generate();
    let prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                821,
            ),
            tool_invocation_intent(checkpoint.checkpoint(), invocation_id),
        )
        .await
        .unwrap();
    let prepared_head = prepared.invocation().head();
    let prepared_journal_head = prepared.event().head();

    let writers = 24_u64;
    let mut tasks = Vec::new();
    for index in 0..writers {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        let fence = lease.fence().clone();
        let expected = prepared_head.clone();
        let journal_head = prepared_journal_head.clone();
        tasks.push(tokio::spawn(async move {
            store
                .advance_tool_invocation(
                    worker_append(
                        tenant_id,
                        run_id,
                        EventId::generate(),
                        JournalExpectation::exact(journal_head),
                        fence,
                        822 + index,
                    ),
                    &expected,
                    ToolInvocationTransition::StartAttempt {
                        attempt_id: AttemptId::generate(),
                    },
                )
                .await
        }));
    }

    let mut winners = Vec::new();
    for task in tasks {
        match task.await.expect("invocation writer must not panic") {
            Ok(outcome) => winners.push(outcome),
            Err(StoreError::StaleToolInvocationHead) => {}
            Err(error) => panic!("unexpected invocation writer failure: {error}"),
        }
    }
    assert_eq!(winners.len(), 1);
    let winner = winners.pop().unwrap();
    assert!(matches!(
        winner,
        ToolInvocationCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        winner.invocation().status(),
        ToolInvocationStatus::Executing
    );

    let current = store
        .load_tool_invocation(&tenant_id, run_id, invocation_id)
        .await
        .unwrap();
    assert_eq!(current.head(), winner.invocation().head());
    let history = store
        .load_tool_invocation_history_page(
            &tenant_id,
            run_id,
            invocation_id,
            None,
            ToolInvocationHistoryPageSize::new(2).unwrap(),
        )
        .await
        .expect("the winning transition must form one complete history");
    assert_eq!(
        history
            .records()
            .iter()
            .map(stateknot_core::ToolInvocation::status)
            .collect::<Vec<_>>(),
        vec![
            ToolInvocationStatus::Prepared,
            ToolInvocationStatus::Executing,
        ]
    );
    assert!(!history.has_more());
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 3);
    assert_eq!(journal.events().last().unwrap(), winner.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn model_invocations_are_atomic_fenced_idempotent_and_page_verifiable() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("model-invocation");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 900)).await;
    let first_lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();

    let invocation_id = InvocationId::generate();
    let intent = model_invocation_intent(checkpoint.checkpoint(), invocation_id);
    let prepare_event_id = EventId::generate();
    let prepare_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            prepare_event_id,
            JournalExpectation::exact(checkpoint.event().head()),
            first_lease.fence().clone(),
            901,
        )
    };
    let prepared = store
        .prepare_model_invocation(prepare_append(), intent.clone())
        .await
        .expect("model invocation preparation must commit");
    assert!(matches!(
        prepared,
        ModelInvocationCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        prepared.invocation().status(),
        ModelInvocationStatus::Prepared
    );
    assert_eq!(prepared.event().sequence().get(), 2);

    let prepare_retry = store
        .prepare_model_invocation(prepare_append(), intent.clone())
        .await
        .expect("lost model preparation acknowledgement must converge");
    assert!(matches!(
        prepare_retry,
        ModelInvocationCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(
        prepare_retry.invocation().head(),
        prepared.invocation().head()
    );
    let crossed_invocation_id = InvocationId::generate();
    assert!(matches!(
        store
            .prepare_model_invocation(
                prepare_append(),
                model_invocation_intent(checkpoint.checkpoint(), crossed_invocation_id),
            )
            .await,
        Err(StoreError::ModelInvocationCommitConflict)
    ));
    assert!(matches!(
        store
            .load_model_invocation(&tenant_id, run_id, crossed_invocation_id)
            .await,
        Err(StoreError::ModelInvocationNotFound)
    ));
    assert_eq!(
        store
            .load_model_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .head(),
        prepared.invocation().head()
    );

    assert!(matches!(
        store
            .append_worker_checkpoint(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(prepared.event().head()),
                    first_lease.fence().clone(),
                    906,
                ),
                RunProjection::unchanged(),
                successor_checkpoint_write(CheckpointId::generate(), checkpoint.checkpoint(), 1,),
            )
            .await,
        Err(StoreError::CheckpointBarrierRequired)
    ));

    let physical_attempt = AttemptId::generate();
    let start_event_id = EventId::generate();
    let start_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            start_event_id,
            JournalExpectation::exact(prepared.event().head()),
            first_lease.fence().clone(),
            902,
        )
    };
    let executing = store
        .advance_model_invocation(
            start_append(),
            &prepared.invocation().head(),
            ModelInvocationTransition::StartAttempt {
                attempt_id: physical_attempt,
            },
        )
        .await
        .expect("physical model attempt claim must commit");
    assert_eq!(
        executing.invocation().status(),
        ModelInvocationStatus::Executing
    );

    let current_lease = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let start_retry = store
        .advance_model_invocation(
            start_append(),
            &prepared.invocation().head(),
            ModelInvocationTransition::StartAttempt {
                attempt_id: physical_attempt,
            },
        )
        .await
        .expect("committed model attempt claim must survive lease takeover");
    assert!(matches!(
        start_retry,
        ModelInvocationCommitOutcome::Idempotent { .. }
    ));

    let response_event_id = EventId::generate();
    let response_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            response_event_id,
            JournalExpectation::exact(executing.event().head()),
            current_lease.fence().clone(),
            903,
        )
    };
    let committed = store
        .advance_model_invocation(
            response_append(),
            &executing.invocation().head(),
            ModelInvocationTransition::RecordResponse {
                response: model_response(&intent, physical_attempt),
            },
        )
        .await
        .expect("validated model response must commit");
    assert_eq!(
        committed.invocation().status(),
        ModelInvocationStatus::Committed
    );
    assert_eq!(committed.invocation().revision().get(), 2);

    let response_retry = store
        .advance_model_invocation(
            response_append(),
            &executing.invocation().head(),
            ModelInvocationTransition::RecordResponse {
                response: model_response(&intent, physical_attempt),
            },
        )
        .await
        .expect("lost model response acknowledgement must converge");
    assert!(matches!(
        response_retry,
        ModelInvocationCommitOutcome::Idempotent { .. }
    ));

    let mut cursor = None;
    let mut statuses = Vec::new();
    loop {
        let page = store
            .load_model_invocation_history_page(
                &tenant_id,
                run_id,
                invocation_id,
                cursor.as_ref(),
                ModelInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await
            .expect("each model invocation history page must verify");
        statuses.extend(
            page.records()
                .iter()
                .map(stateknot_core::ModelInvocation::status),
        );
        cursor = page.next_cursor();
        if !page.has_more() {
            break;
        }
    }
    assert_eq!(
        statuses,
        vec![
            ModelInvocationStatus::Prepared,
            ModelInvocationStatus::Executing,
            ModelInvocationStatus::Committed,
        ]
    );
    assert_eq!(
        cursor.expect("final model cursor").head(),
        committed.invocation().head()
    );

    assert!(matches!(
        store
            .advance_model_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(committed.event().head()),
                    current_lease.fence().clone(),
                    904,
                ),
                &prepared.invocation().head(),
                ModelInvocationTransition::StartAttempt {
                    attempt_id: AttemptId::generate(),
                },
            )
            .await,
        Err(StoreError::StaleModelInvocationHead)
    ));

    let second_intent = model_invocation_intent(checkpoint.checkpoint(), InvocationId::generate());
    assert!(matches!(
        store
            .prepare_model_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(committed.event().head()),
                    first_lease.fence().clone(),
                    905,
                ),
                second_intent,
            )
            .await,
        Err(StoreError::StaleFence)
    ));

    let activation = intent.activation().clone();
    let bindings = NodeInvocationBindings::try_new(
        &activation,
        [NodeInvocationBinding::from_model(committed.invocation()).unwrap()],
    )
    .unwrap();
    let pending = store
        .commit_test_pending_node_result(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                current_lease.fence().clone(),
                907,
            ),
            pending_result_intent(activation, bindings),
        )
        .await
        .expect("a settled model binding must commit its node result");
    let barrier_intent = CheckpointBarrier::new(
        checkpoint.checkpoint(),
        successor_checkpoint_write(CheckpointId::generate(), checkpoint.checkpoint(), 1),
        [pending.result().head()],
    )
    .unwrap();
    let barrier = store
        .append_worker_barrier(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(pending.event().head()),
                current_lease.fence().clone(),
                908,
            ),
            RunProjection::unchanged(),
            barrier_intent,
        )
        .await
        .expect("a committed model invocation must release the checkpoint barrier");
    assert_eq!(barrier.checkpoint().superstep().get(), 1);
    assert_eq!(barrier.event().sequence().get(), 7);
    assert_eq!(
        store
            .load_model_invocation(&tenant_id, run_id, invocation_id)
            .await
            .unwrap()
            .status(),
        ModelInvocationStatus::Committed
    );

    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 7);
    assert_eq!(journal.events().last().unwrap(), barrier.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn model_retry_delay_and_run_wide_attempt_claims_are_enforced() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("model-invocation-retry");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 920)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let invocation_id = InvocationId::generate();
    let intent = model_invocation_intent(checkpoint.checkpoint(), invocation_id);
    let prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                921,
            ),
            intent.clone(),
        )
        .await
        .unwrap();
    let first_attempt = AttemptId::generate();
    let executing = store
        .advance_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(prepared.event().head()),
                lease.fence().clone(),
                922,
            ),
            &prepared.invocation().head(),
            ModelInvocationTransition::StartAttempt {
                attempt_id: first_attempt,
            },
        )
        .await
        .unwrap();
    let failed = store
        .advance_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(executing.event().head()),
                lease.fence().clone(),
                923,
            ),
            &executing.invocation().head(),
            ModelInvocationTransition::RecordError {
                error: model_error(
                    &intent,
                    first_attempt,
                    RetryAdvice::SafeAfter {
                        delay: DurationMillis::new(2_000).unwrap(),
                    },
                ),
            },
        )
        .await
        .unwrap();
    assert_eq!(failed.invocation().status(), ModelInvocationStatus::Failed);

    let retry_attempt = AttemptId::generate();
    assert!(matches!(
        store
            .advance_model_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(failed.event().head()),
                    lease.fence().clone(),
                    924,
                ),
                &failed.invocation().head(),
                ModelInvocationTransition::StartAttempt {
                    attempt_id: retry_attempt,
                },
            )
            .await,
        Err(StoreError::InvalidModelInvocationTransition)
    ));

    let tool_invocation_id = InvocationId::generate();
    let tool_prepared = store
        .prepare_tool_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(failed.event().head()),
                lease.fence().clone(),
                925,
            ),
            tool_invocation_intent(checkpoint.checkpoint(), tool_invocation_id),
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .advance_tool_invocation(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(tool_prepared.event().head()),
                    lease.fence().clone(),
                    926,
                ),
                &tool_prepared.invocation().head(),
                ToolInvocationTransition::StartAttempt {
                    attempt_id: first_attempt,
                },
            )
            .await,
        Err(StoreError::InvalidToolInvocationTransition)
    ));
    assert_eq!(
        store
            .load_tool_invocation(&tenant_id, run_id, tool_invocation_id)
            .await
            .unwrap()
            .status(),
        ToolInvocationStatus::Prepared
    );
    assert!(matches!(
        store
            .start_node_attempt(
                worker_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(tool_prepared.event().head()),
                    lease.fence().clone(),
                    926,
                ),
                pending_activation(checkpoint.checkpoint(), b"cross-kind attempt identity"),
                first_attempt,
            )
            .await,
        Err(StoreError::NodeAttemptIdConflict)
    ));

    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let retrying = store
        .advance_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(tool_prepared.event().head()),
                lease.fence().clone(),
                927,
            ),
            &failed.invocation().head(),
            ModelInvocationTransition::StartAttempt {
                attempt_id: retry_attempt,
            },
        )
        .await
        .expect("retry must start after the durable minimum delay");
    let committed = store
        .advance_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(retrying.event().head()),
                lease.fence().clone(),
                928,
            ),
            &retrying.invocation().head(),
            ModelInvocationTransition::RecordResponse {
                response: model_response(&intent, retry_attempt),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        committed.invocation().status(),
        ModelInvocationStatus::Committed
    );

    let mut cursor = None;
    let mut statuses = Vec::new();
    loop {
        let page = store
            .load_model_invocation_history_page(
                &tenant_id,
                run_id,
                invocation_id,
                cursor.as_ref(),
                ModelInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await
            .expect("retry history must verify its complete delay proof");
        statuses.push(page.records()[0].status());
        cursor = page.next_cursor();
        if !page.has_more() {
            break;
        }
    }
    assert_eq!(
        statuses,
        vec![
            ModelInvocationStatus::Prepared,
            ModelInvocationStatus::Executing,
            ModelInvocationStatus::Failed,
            ModelInvocationStatus::Executing,
            ModelInvocationStatus::Committed,
        ]
    );
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 7);
    assert_eq!(journal.events().last().unwrap(), committed.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn failed_model_projection_update_rolls_back_event_claim_and_revision() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("test administration connection must open");
    let tenant_id = tenant("model-invocation-rollback");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 940)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let invocation_id = InvocationId::generate();
    let prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                941,
            ),
            model_invocation_intent(checkpoint.checkpoint(), invocation_id),
        )
        .await
        .unwrap();

    query(
        "ALTER TABLE stateknot.model_invocations \
         DROP CONSTRAINT IF EXISTS test_model_invocation_rollback",
    )
    .execute(&administration)
    .await
    .unwrap();
    let reject_projection = format!(
        "ALTER TABLE stateknot.model_invocations \
         ADD CONSTRAINT test_model_invocation_rollback \
         CHECK (tenant_id <> '{}' OR current_status = 'prepared') NOT VALID",
        tenant_id.as_str()
    );
    query(&reject_projection)
        .execute(&administration)
        .await
        .unwrap();

    let attempt_id = AttemptId::generate();
    let event_id = EventId::generate();
    let advance_append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            event_id,
            JournalExpectation::exact(prepared.event().head()),
            lease.fence().clone(),
            942,
        )
    };
    let advance = store
        .advance_model_invocation(
            advance_append(),
            &prepared.invocation().head(),
            ModelInvocationTransition::StartAttempt { attempt_id },
        )
        .await;

    query(
        "ALTER TABLE stateknot.model_invocations \
         DROP CONSTRAINT test_model_invocation_rollback",
    )
    .execute(&administration)
    .await
    .unwrap();
    administration.close().await;
    assert!(matches!(advance, Err(StoreError::Database { .. })));

    let current = store
        .load_model_invocation(&tenant_id, run_id, invocation_id)
        .await
        .expect("a failed model advance must retain the prepared projection");
    assert_eq!(current.head(), prepared.invocation().head());
    let history = store
        .load_model_invocation_history_page(
            &tenant_id,
            run_id,
            invocation_id,
            None,
            ModelInvocationHistoryPageSize::new(1).unwrap(),
        )
        .await
        .expect("a rolled-back model revision must not appear in history");
    assert_eq!(history.records().len(), 1);
    assert_eq!(history.records()[0].head(), prepared.invocation().head());
    assert!(!history.has_more());
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .expect("a rolled-back model event must not appear in the journal");
    assert_eq!(journal.events().len(), 2);
    assert_eq!(journal.events().last().unwrap(), prepared.event());

    let executing = store
        .advance_model_invocation(
            advance_append(),
            &prepared.invocation().head(),
            ModelInvocationTransition::StartAttempt { attempt_id },
        )
        .await
        .expect("rolled-back event and attempt identities must remain reusable");
    assert_eq!(
        executing.invocation().status(),
        ModelInvocationStatus::Executing
    );
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 3);
    assert_eq!(journal.events().last().unwrap(), executing.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn concurrent_model_advances_admit_exactly_one_physical_attempt() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("model-invocation-concurrency");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 960)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let invocation_id = InvocationId::generate();
    let prepared = store
        .prepare_model_invocation(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                lease.fence().clone(),
                961,
            ),
            model_invocation_intent(checkpoint.checkpoint(), invocation_id),
        )
        .await
        .unwrap();
    let prepared_head = prepared.invocation().head();
    let prepared_journal_head = prepared.event().head();

    let writers = 24_u64;
    let mut tasks = Vec::new();
    for index in 0..writers {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        let fence = lease.fence().clone();
        let expected = prepared_head.clone();
        let journal_head = prepared_journal_head.clone();
        tasks.push(tokio::spawn(async move {
            store
                .advance_model_invocation(
                    worker_append(
                        tenant_id,
                        run_id,
                        EventId::generate(),
                        JournalExpectation::exact(journal_head),
                        fence,
                        962 + index,
                    ),
                    &expected,
                    ModelInvocationTransition::StartAttempt {
                        attempt_id: AttemptId::generate(),
                    },
                )
                .await
        }));
    }

    let mut winners = Vec::new();
    for task in tasks {
        match task.await.expect("model invocation writer must not panic") {
            Ok(outcome) => winners.push(outcome),
            Err(StoreError::StaleModelInvocationHead) => {}
            Err(error) => panic!("unexpected model invocation writer failure: {error}"),
        }
    }
    assert_eq!(winners.len(), 1);
    let winner = winners.pop().unwrap();
    assert!(matches!(
        winner,
        ModelInvocationCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        winner.invocation().status(),
        ModelInvocationStatus::Executing
    );

    let current = store
        .load_model_invocation(&tenant_id, run_id, invocation_id)
        .await
        .unwrap();
    assert_eq!(current.head(), winner.invocation().head());
    let first_page = store
        .load_model_invocation_history_page(
            &tenant_id,
            run_id,
            invocation_id,
            None,
            ModelInvocationHistoryPageSize::new(1).unwrap(),
        )
        .await
        .expect("the preparation page must verify");
    assert_eq!(
        first_page.records()[0].status(),
        ModelInvocationStatus::Prepared
    );
    assert!(first_page.has_more());
    let second_page = store
        .load_model_invocation_history_page(
            &tenant_id,
            run_id,
            invocation_id,
            first_page.next_cursor().as_ref(),
            ModelInvocationHistoryPageSize::new(1).unwrap(),
        )
        .await
        .expect("the winning transition must verify");
    assert_eq!(
        second_page.records()[0].status(),
        ModelInvocationStatus::Executing
    );
    assert!(!second_page.has_more());
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 3);
    assert_eq!(journal.events().last().unwrap(), winner.event());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn invocation_reads_fail_closed_on_corrupt_canonical_bytes_and_anchors() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("test administration connection must open");

    let (intent_tenant, intent_run, intent_id, _) = Box::pin(prepare_tool_invocation_fixture(
        &store,
        "tool-intent-corruption",
        840,
    ))
    .await;
    query(
        "UPDATE stateknot.tool_invocations \
         SET intent_bytes = intent_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3",
    )
    .bind(intent_tenant.as_str())
    .bind(*intent_run.as_uuid())
    .bind(*intent_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_tool_invocation(&intent_tenant, intent_run, intent_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_tool_invocation_history_page(
                &intent_tenant,
                intent_run,
                intent_id,
                None,
                ToolInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let (record_tenant, record_run, record_id, _) = Box::pin(prepare_tool_invocation_fixture(
        &store,
        "tool-record-corruption",
        850,
    ))
    .await;
    query(
        "UPDATE stateknot.tool_invocation_revisions \
         SET record_bytes = record_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3 AND revision = 0",
    )
    .bind(record_tenant.as_str())
    .bind(*record_run.as_uuid())
    .bind(*record_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_tool_invocation(&record_tenant, record_run, record_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_tool_invocation_history_page(
                &record_tenant,
                record_run,
                record_id,
                None,
                ToolInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let (anchor_tenant, anchor_run, anchor_id, anchor_prepared) = Box::pin(
        prepare_tool_invocation_fixture(&store, "tool-anchor-corruption", 860),
    )
    .await;
    query(
        "UPDATE stateknot.run_events \
         SET payload_bytes = payload_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND sequence = $3",
    )
    .bind(anchor_tenant.as_str())
    .bind(*anchor_run.as_uuid())
    .bind(i64::try_from(anchor_prepared.event().sequence().get()).unwrap())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_tool_invocation(&anchor_tenant, anchor_run, anchor_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_tool_invocation_history_page(
                &anchor_tenant,
                anchor_run,
                anchor_id,
                None,
                ToolInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let (projection_tenant, projection_run, projection_id, projection_prepared) = Box::pin(
        prepare_tool_invocation_fixture(&store, "tool-projection-corruption", 870),
    )
    .await;
    let wrong_projection = Digest::sha256(b"wrong tool invocation projection");
    query(
        "UPDATE stateknot.run_events \
         SET projection_digest = $4 \
         WHERE tenant_id = $1 AND run_id = $2 AND sequence = $3",
    )
    .bind(projection_tenant.as_str())
    .bind(*projection_run.as_uuid())
    .bind(i64::try_from(projection_prepared.event().sequence().get()).unwrap())
    .bind(wrong_projection.as_bytes())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_tool_invocation(&projection_tenant, projection_run, projection_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_tool_invocation_history_page(
                &projection_tenant,
                projection_run,
                projection_id,
                None,
                ToolInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let (model_intent_tenant, model_intent_run, model_intent_id, _) = Box::pin(
        prepare_model_invocation_fixture(&store, "model-intent-corruption", 980),
    )
    .await;
    query(
        "UPDATE stateknot.model_invocations \
         SET intent_bytes = intent_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3",
    )
    .bind(model_intent_tenant.as_str())
    .bind(*model_intent_run.as_uuid())
    .bind(*model_intent_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_model_invocation(&model_intent_tenant, model_intent_run, model_intent_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_model_invocation_history_page(
                &model_intent_tenant,
                model_intent_run,
                model_intent_id,
                None,
                ModelInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let (model_record_tenant, model_record_run, model_record_id, _) = Box::pin(
        prepare_model_invocation_fixture(&store, "model-record-corruption", 990),
    )
    .await;
    query(
        "UPDATE stateknot.model_invocation_revisions \
         SET record_bytes = record_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND invocation_id = $3 AND revision = 0",
    )
    .bind(model_record_tenant.as_str())
    .bind(*model_record_run.as_uuid())
    .bind(*model_record_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_model_invocation(&model_record_tenant, model_record_run, model_record_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_model_invocation_history_page(
                &model_record_tenant,
                model_record_run,
                model_record_id,
                None,
                ModelInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let (model_anchor_tenant, model_anchor_run, model_anchor_id, model_anchor_prepared) = Box::pin(
        prepare_model_invocation_fixture(&store, "model-anchor-corruption", 1_000),
    )
    .await;
    query(
        "UPDATE stateknot.run_events \
         SET payload_bytes = payload_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND sequence = $3",
    )
    .bind(model_anchor_tenant.as_str())
    .bind(*model_anchor_run.as_uuid())
    .bind(i64::try_from(model_anchor_prepared.event().sequence().get()).unwrap())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_model_invocation(&model_anchor_tenant, model_anchor_run, model_anchor_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_model_invocation_history_page(
                &model_anchor_tenant,
                model_anchor_run,
                model_anchor_id,
                None,
                ModelInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let (
        model_projection_tenant,
        model_projection_run,
        model_projection_id,
        model_projection_prepared,
    ) = Box::pin(prepare_model_invocation_fixture(
        &store,
        "model-projection-corruption",
        1_010,
    ))
    .await;
    let wrong_model_projection = Digest::sha256(b"wrong model invocation projection");
    query(
        "UPDATE stateknot.run_events \
         SET projection_digest = $4 \
         WHERE tenant_id = $1 AND run_id = $2 AND sequence = $3",
    )
    .bind(model_projection_tenant.as_str())
    .bind(*model_projection_run.as_uuid())
    .bind(i64::try_from(model_projection_prepared.event().sequence().get()).unwrap())
    .bind(wrong_model_projection.as_bytes())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_model_invocation(
                &model_projection_tenant,
                model_projection_run,
                model_projection_id,
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_model_invocation_history_page(
                &model_projection_tenant,
                model_projection_run,
                model_projection_id,
                None,
                ModelInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failure_after_checkpoint_insert_rolls_back_every_projection() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("checkpoint-rollback");
    let run_id = RunId::generate();
    let checkpoint_id = CheckpointId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    query("ALTER TABLE stateknot.runs DROP CONSTRAINT IF EXISTS test_checkpoint_rollback")
        .execute(&administration)
        .await
        .unwrap();
    let reject_target = format!(
        "ALTER TABLE stateknot.runs ADD CONSTRAINT test_checkpoint_rollback CHECK (tenant_id <> '{}') NOT VALID",
        tenant_id.as_str()
    );
    query(&reject_target)
        .execute(&administration)
        .await
        .unwrap();

    let result = store
        .append_control_plane_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                300,
            ),
            RunProjection::unchanged(),
            initial_checkpoint_write(tenant_id.clone(), run_id, checkpoint_id),
        )
        .await;

    query("ALTER TABLE stateknot.runs DROP CONSTRAINT test_checkpoint_rollback")
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
    assert!(matches!(result, Err(StoreError::Database { .. })));

    let run = store.load_run(&tenant_id, run_id).await.unwrap();
    assert!(run.journal_head().is_none());
    assert!(run.checkpoint().is_none());
    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap(),
        None
    );
    assert!(matches!(
        store
            .load_checkpoint(&tenant_id, run_id, checkpoint_id)
            .await,
        Err(StoreError::CheckpointNotFound)
    ));
    let page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert!(page.events().is_empty());
    let lineage = store
        .load_checkpoint_lineage_page(
            &tenant_id,
            run_id,
            None,
            CheckpointLineagePageSize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert!(lineage.checkpoints().is_empty());
    assert!(!lineage.has_more());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn checkpoint_lineage_pages_are_exact_bounded_and_advance_safe() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("checkpoint-lineage");
    let run_id = RunId::generate();
    let first = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 600)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let mut checkpoints = vec![first.checkpoint().clone()];
    for index in 1..=5_u64 {
        let parent = checkpoints.last().unwrap();
        let (result_heads, result_journal) =
            commit_ready_results(&store, parent, lease.fence(), 600 + index * 2).await;
        let barrier = CheckpointBarrier::new(
            parent,
            successor_checkpoint_write(CheckpointId::generate(), parent, index),
            result_heads,
        )
        .unwrap();
        let outcome = store
            .append_control_plane_barrier(
                control_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(result_journal),
                    601 + index * 2,
                ),
                RunProjection::unchanged(),
                barrier,
            )
            .await
            .unwrap();
        checkpoints.push(outcome.checkpoint().clone());
    }

    let first_page = store
        .load_checkpoint_lineage_page(
            &tenant_id,
            run_id,
            None,
            CheckpointLineagePageSize::new(2).unwrap(),
        )
        .await
        .expect("the current reverse-lineage page must validate");
    assert_eq!(
        first_page
            .checkpoints()
            .iter()
            .map(|checkpoint| checkpoint.superstep().get())
            .collect::<Vec<_>>(),
        vec![5, 4]
    );
    assert!(first_page.has_more());
    let continuation = first_page
        .next_cursor()
        .expect("a bounded first page must expose its exact parent");
    assert_eq!(continuation, checkpoints[3].head());

    let (advanced_results, advanced_journal) =
        commit_ready_results(&store, checkpoints.last().unwrap(), lease.fence(), 620).await;
    let advanced_barrier = CheckpointBarrier::new(
        checkpoints.last().unwrap(),
        successor_checkpoint_write(CheckpointId::generate(), checkpoints.last().unwrap(), 6),
        advanced_results,
    )
    .unwrap();
    let advanced = store
        .append_control_plane_barrier(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(advanced_journal),
                621,
            ),
            RunProjection::unchanged(),
            advanced_barrier,
        )
        .await
        .expect("a later barrier must be allowed to advance the current pointer");

    let second_page = store
        .load_checkpoint_lineage_page(
            &tenant_id,
            run_id,
            Some(&continuation),
            CheckpointLineagePageSize::new(2).unwrap(),
        )
        .await
        .expect("an immutable continuation must survive a later barrier");
    assert_eq!(
        second_page
            .checkpoints()
            .iter()
            .map(|checkpoint| checkpoint.superstep().get())
            .collect::<Vec<_>>(),
        vec![3, 2]
    );
    let final_page = store
        .load_checkpoint_lineage_page(
            &tenant_id,
            run_id,
            second_page.next_cursor().as_ref(),
            CheckpointLineagePageSize::new(2).unwrap(),
        )
        .await
        .expect("the final page must terminate exactly at the root");
    assert_eq!(
        final_page
            .checkpoints()
            .iter()
            .map(|checkpoint| checkpoint.superstep().get())
            .collect::<Vec<_>>(),
        vec![1, 0]
    );
    assert!(!final_page.has_more());
    assert_eq!(final_page.next_cursor(), None);

    let current_page = store
        .load_checkpoint_lineage_page(
            &tenant_id,
            run_id,
            None,
            CheckpointLineagePageSize::new(1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(current_page.checkpoints(), &[advanced.checkpoint().clone()]);
    assert_eq!(current_page.next_cursor(), Some(checkpoints[5].head()));

    let mut tampered_value = serde_json::to_value(checkpoints[3].head()).unwrap();
    tampered_value["digest"] = json!(Digest::sha256(b"tampered checkpoint cursor"));
    let tampered_cursor: CheckpointHead = serde_json::from_value(tampered_value).unwrap();
    assert!(matches!(
        store
            .load_checkpoint_lineage_page(
                &tenant_id,
                run_id,
                Some(&tampered_cursor),
                CheckpointLineagePageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::InvalidCheckpointCursor)
    ));
    assert!(matches!(
        store
            .load_checkpoint_lineage_page(
                &tenant("crossed-checkpoint-lineage"),
                run_id,
                Some(&continuation),
                CheckpointLineagePageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::InvalidCheckpointCursor)
    ));

    let broken_tenant = tenant("broken-checkpoint-lineage");
    let broken_run = RunId::generate();
    let broken_root = Box::pin(start_run_with_checkpoint(
        &store,
        &broken_tenant,
        broken_run,
        630,
    ))
    .await;
    let broken_lease = store
        .claim_lease(&broken_tenant, broken_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let (broken_results, broken_journal) =
        commit_ready_results(&store, broken_root.checkpoint(), broken_lease.fence(), 631).await;
    let broken_barrier = CheckpointBarrier::new(
        broken_root.checkpoint(),
        successor_checkpoint_write(CheckpointId::generate(), broken_root.checkpoint(), 1),
        broken_results,
    )
    .unwrap();
    store
        .append_control_plane_barrier(
            control_append(
                broken_tenant.clone(),
                broken_run,
                EventId::generate(),
                JournalExpectation::exact(broken_journal),
                632,
            ),
            RunProjection::unchanged(),
            broken_barrier,
        )
        .await
        .unwrap();

    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    query("SET session_replication_role = replica")
        .execute(&administration)
        .await
        .unwrap();
    query(
        "DELETE FROM stateknot.run_checkpoints \
         WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $3",
    )
    .bind(broken_tenant.as_str())
    .bind(*broken_run.as_uuid())
    .bind(*broken_root.checkpoint().checkpoint_id().as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    query("SET session_replication_role = origin")
        .execute(&administration)
        .await
        .unwrap();
    assert!(matches!(
        store
            .load_checkpoint_lineage_page(
                &broken_tenant,
                broken_run,
                None,
                CheckpointLineagePageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    query(
        "UPDATE stateknot.run_events \
         SET payload_bytes = payload_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND sequence = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(i64::try_from(checkpoints[2].journal_head().sequence().get()).unwrap())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_checkpoint_lineage_page(
                &tenant_id,
                run_id,
                Some(&checkpoints[2].head()),
                CheckpointLineagePageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    query(
        "UPDATE stateknot.run_checkpoints \
         SET checkpoint_bytes = checkpoint_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*checkpoints[0].checkpoint_id().as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_checkpoint_lineage_page(
                &tenant_id,
                run_id,
                Some(&checkpoints[0].head()),
                CheckpointLineagePageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn checkpoint_load_fails_closed_on_corrupt_bytes_and_journal_anchor() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("checkpoint-corruption");
    let run_id = RunId::generate();
    let checkpoint_id = CheckpointId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    store
        .append_control_plane_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                400,
            ),
            RunProjection::unchanged(),
            initial_checkpoint_write(tenant_id.clone(), run_id, checkpoint_id),
        )
        .await
        .unwrap();

    query(
        "UPDATE stateknot.run_checkpoints \
         SET checkpoint_bytes = checkpoint_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*checkpoint_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();

    assert!(matches!(
        store
            .load_checkpoint(&tenant_id, run_id, checkpoint_id)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store.load_current_checkpoint(&tenant_id, run_id).await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_checkpoint_lineage_page(
                &tenant_id,
                run_id,
                None,
                CheckpointLineagePageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let anchor_tenant = tenant("checkpoint-anchor-corruption");
    let anchor_run = RunId::generate();
    let anchor_checkpoint = CheckpointId::generate();
    store
        .admit_run(provenance(anchor_tenant.clone(), anchor_run))
        .await
        .unwrap();
    store
        .append_control_plane_checkpoint(
            control_append(
                anchor_tenant.clone(),
                anchor_run,
                EventId::generate(),
                JournalExpectation::empty(),
                401,
            ),
            RunProjection::unchanged(),
            initial_checkpoint_write(anchor_tenant.clone(), anchor_run, anchor_checkpoint),
        )
        .await
        .unwrap();
    query(
        "UPDATE stateknot.run_events \
         SET payload_bytes = payload_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND sequence = 1",
    )
    .bind(anchor_tenant.as_str())
    .bind(*anchor_run.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_checkpoint(&anchor_tenant, anchor_run, anchor_checkpoint)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_current_checkpoint(&anchor_tenant, anchor_run)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert!(matches!(
        store
            .load_checkpoint_lineage_page(
                &anchor_tenant,
                anchor_run,
                None,
                CheckpointLineagePageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn concurrent_checkpoint_writers_form_one_linear_barrier_chain() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store_with_lease_duration(Duration::from_secs(120)).await else {
        return;
    };
    let tenant_id = tenant("checkpoint-concurrency");
    let run_id = RunId::generate();
    let initial = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 500)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    assert_eq!(initial.checkpoint().superstep().get(), 0);

    let writers = 24_u64;
    let mut tasks = Vec::new();
    for index in 1..=writers {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        let fence = lease.fence().clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let parent = store
                    .load_current_checkpoint(&tenant_id, run_id)
                    .await
                    .unwrap()
                    .expect("initial checkpoint must remain present");
                let run = store.load_run(&tenant_id, run_id).await.unwrap();
                let activation = NodeActivation::for_ready_root(
                    &parent,
                    parent.ready_nodes().iter().next().unwrap().clone(),
                )
                .unwrap();
                let result = match store
                    .commit_test_pending_node_result(
                        worker_append(
                            tenant_id.clone(),
                            run_id,
                            EventId::generate(),
                            JournalExpectation::exact(run.journal_head().unwrap().clone()),
                            fence.clone(),
                            500 + index,
                        ),
                        pending_result_intent(activation, NodeInvocationBindings::empty()),
                    )
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(
                        StoreError::StaleJournalHead
                        | StoreError::StaleCheckpointHead
                        | StoreError::InvalidNodeAttemptTransition,
                    ) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) if error.is_retryable() => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => panic!("unexpected concurrent result failure: {error}"),
                };
                let run = store.load_run(&tenant_id, run_id).await.unwrap();
                let barrier = CheckpointBarrier::new(
                    &parent,
                    successor_checkpoint_write(CheckpointId::generate(), &parent, index),
                    [result.result().head()],
                )
                .unwrap();
                match store
                    .append_control_plane_barrier(
                        control_append(
                            tenant_id.clone(),
                            run_id,
                            EventId::generate(),
                            JournalExpectation::exact(run.journal_head().unwrap().clone()),
                            600 + index,
                        ),
                        RunProjection::unchanged(),
                        barrier,
                    )
                    .await
                {
                    Ok(outcome) => return outcome.checkpoint().superstep(),
                    Err(StoreError::StaleJournalHead | StoreError::StaleCheckpointHead) => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) if error.is_retryable() => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected checkpoint writer failure: {error}"),
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("checkpoint writer must not panic");
    }

    let current = store
        .load_current_checkpoint(&tenant_id, run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.superstep().get(), writers);
    assert_eq!(current.journal_head().sequence().get(), writers * 3 + 1);
    let page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(128).unwrap())
        .await
        .unwrap();
    assert_eq!(
        page.events().len(),
        usize::try_from(writers * 3 + 1).unwrap()
    );
    assert!(!page.has_more());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn initial_wait_checkpoint_is_atomic_exact_and_fail_closed() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("initial-wait");
    let run_id = RunId::generate();
    let admitted = store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let started = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                900,
            ),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
        )
        .await
        .unwrap();
    let active = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(active.lifecycle().status(), RunStatus::Active);

    let wait_event_id = EventId::generate();
    let interrupt_id = InterruptId::generate();
    let timer_id = TimerId::generate();
    let interrupt = InterruptRequestIntent::new(
        tenant_id.clone(),
        run_id,
        interrupt_id,
        wait_event_id,
        RunInterruptKind::Approval,
        payload(901),
        Digest::sha256(b"production wait action"),
        Some(PrincipalIdentity::new(
            "https://issuer.example.com/waits"
                .parse::<IssuerId>()
                .unwrap(),
            "integration-approver".parse::<SubjectId>().unwrap(),
        )),
        ScopeSet::try_new([
            "run.resolve".parse::<Scope>().unwrap(),
            "action.approve".parse::<Scope>().unwrap(),
        ])
        .unwrap(),
        Some(timestamp_after(Duration::from_secs(600))),
    )
    .unwrap();
    let timer = TimerRegistrationIntent::new(
        tenant_id.clone(),
        run_id,
        timer_id,
        wait_event_id,
        RunTimerKind::RetryBackoff,
        timestamp_after(Duration::from_secs(300)),
    )
    .unwrap();
    let registrations = vec![
        WaitRegistrationIntent::interrupt(interrupt),
        WaitRegistrationIntent::timer(timer),
    ];
    let checkpoint_write =
        initial_checkpoint_write(tenant_id.clone(), run_id, CheckpointId::generate());
    let wait_append = control_append(
        tenant_id.clone(),
        run_id,
        wait_event_id,
        JournalExpectation::exact(started.event().head()),
        902,
    );
    let committed = store
        .append_control_plane_initial_wait_checkpoint(
            wait_append.clone(),
            active.lifecycle().revision(),
            checkpoint_write.clone(),
            registrations.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        committed,
        WaitCheckpointCommitOutcome::Committed { .. }
    ));
    assert_eq!(committed.waits().len(), 2);

    let waiting = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(waiting.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(waiting.unresolved_wait_count(), 2);
    assert!(waiting.wait_set_digest().is_some());
    assert!(waiting.next_timer_due_at().is_some());
    assert!(waiting.next_interrupt_expiry_at().is_some());
    assert_eq!(
        store
            .load_interrupt_request(&tenant_id, run_id, interrupt_id)
            .await
            .unwrap()
            .journal(),
        &committed.event().head()
    );
    assert_eq!(
        store
            .load_durable_timer(&tenant_id, run_id, timer_id)
            .await
            .unwrap()
            .journal(),
        &committed.event().head()
    );
    assert!(matches!(
        store
            .load_durable_timer(
                &tenant_id,
                run_id,
                TimerId::from_uuid(*interrupt_id.as_uuid()).unwrap()
            )
            .await,
        Err(StoreError::WaitRegistrationKindMismatch)
    ));

    let retry = store
        .append_control_plane_initial_wait_checkpoint(
            wait_append,
            active.lifecycle().revision(),
            checkpoint_write,
            registrations,
        )
        .await
        .unwrap();
    assert!(matches!(
        retry,
        WaitCheckpointCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(retry.event(), committed.event());
    assert_eq!(retry.checkpoint(), committed.checkpoint());
    assert_eq!(retry.waits(), committed.waits());

    let bypass = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                903,
            ),
            RunProjection::transition(
                waiting.lifecycle().revision(),
                RunTransition::ResolveInterrupt {
                    interrupt_id,
                    resolved_at: committed.event().recorded_at(),
                },
            ),
        )
        .await;
    assert!(matches!(
        bypass,
        Err(StoreError::DurableWaitMutationRequired)
    ));

    let row_count = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.run_wait_registrations \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!(row_count, 2);

    let worker_tenant = tenant("initial-worker-wait");
    let worker_run = RunId::generate();
    let worker_admitted = store
        .admit_run(provenance(worker_tenant.clone(), worker_run))
        .await
        .unwrap();
    let worker_started = store
        .append_control_plane(
            control_append(
                worker_tenant.clone(),
                worker_run,
                EventId::generate(),
                JournalExpectation::empty(),
                904,
            ),
            RunProjection::transition(
                worker_admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: worker_admitted.lifecycle().admitted_at(),
                },
            ),
        )
        .await
        .unwrap();
    let worker_active = store.load_run(&worker_tenant, worker_run).await.unwrap();
    let worker_lease = store
        .claim_lease(&worker_tenant, worker_run, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let worker_wait_event_id = EventId::generate();
    let worker_timer_id = TimerId::generate();
    let worker_registrations = vec![WaitRegistrationIntent::timer(
        TimerRegistrationIntent::new(
            worker_tenant.clone(),
            worker_run,
            worker_timer_id,
            worker_wait_event_id,
            RunTimerKind::Sleep,
            timestamp_after(Duration::from_secs(1_800)),
        )
        .unwrap(),
    )];
    let worker_checkpoint_write =
        initial_checkpoint_write(worker_tenant.clone(), worker_run, CheckpointId::generate());
    let worker_wait_append = worker_append(
        worker_tenant.clone(),
        worker_run,
        worker_wait_event_id,
        JournalExpectation::exact(worker_started.event().head()),
        worker_lease.fence().clone(),
        905,
    );
    let worker_committed = store
        .append_worker_initial_wait_checkpoint(
            worker_wait_append.clone(),
            worker_active.lifecycle().revision(),
            worker_checkpoint_write.clone(),
            worker_registrations.clone(),
        )
        .await
        .expect("a worker initial wait must update its checkpoint before releasing ownership");
    assert!(matches!(
        worker_committed,
        WaitCheckpointCommitOutcome::Committed { .. }
    ));
    let worker_waiting = store.load_run(&worker_tenant, worker_run).await.unwrap();
    assert_eq!(worker_waiting.lifecycle().status(), RunStatus::Waiting);
    assert!(worker_waiting.lease().is_none());
    assert_eq!(worker_waiting.unresolved_wait_count(), 1);
    assert_eq!(
        store
            .load_current_checkpoint(&worker_tenant, worker_run)
            .await
            .unwrap(),
        Some(worker_committed.checkpoint().clone())
    );
    let worker_retry = store
        .append_worker_initial_wait_checkpoint(
            worker_wait_append,
            worker_active.lifecycle().revision(),
            worker_checkpoint_write,
            worker_registrations,
        )
        .await
        .expect("a lost acknowledgement must survive the committed lease release");
    assert!(matches!(
        worker_retry,
        WaitCheckpointCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(worker_retry.event(), worker_committed.event());
    assert_eq!(worker_retry.checkpoint(), worker_committed.checkpoint());

    let updated = query(
        "UPDATE stateknot.runs SET wait_set_digest = $3 \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(vec![0_u8; Digest::SHA256_LEN])
    .execute(&administration)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(updated, 1);
    assert!(matches!(
        store.load_run(&tenant_id, run_id).await,
        Err(StoreError::CorruptData { .. })
    ));
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn interrupt_resolution_and_timer_firing_are_atomic_and_retry_exact() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("wait-terminal");
    let run_id = RunId::generate();
    let admitted = store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let started = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                910,
            ),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
        )
        .await
        .unwrap();
    let active = store.load_run(&tenant_id, run_id).await.unwrap();
    let database_micros =
        query_scalar::<_, i64>("SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint")
            .fetch_one(&administration)
            .await
            .unwrap();
    let due_at = Timestamp::from_unix_micros(database_micros + 5_000_000).unwrap();
    let expires_at = Timestamp::from_unix_micros(database_micros + 60_000_000).unwrap();
    let wait_event_id = EventId::generate();
    let interrupt_id = InterruptId::generate();
    let timer_id = TimerId::generate();
    let required_principal = PrincipalIdentity::new(
        "https://issuer.example.com/waits"
            .parse::<IssuerId>()
            .unwrap(),
        "terminal-approver".parse::<SubjectId>().unwrap(),
    );
    let required_scopes = ScopeSet::try_new([
        "run.resolve".parse::<Scope>().unwrap(),
        "action.approve".parse::<Scope>().unwrap(),
    ])
    .unwrap();
    let registrations = vec![
        WaitRegistrationIntent::interrupt(
            InterruptRequestIntent::new(
                tenant_id.clone(),
                run_id,
                interrupt_id,
                wait_event_id,
                RunInterruptKind::Approval,
                payload(911),
                Digest::sha256(b"terminal wait action"),
                Some(required_principal.clone()),
                required_scopes.clone(),
                Some(expires_at),
            )
            .unwrap(),
        ),
        WaitRegistrationIntent::timer(
            TimerRegistrationIntent::new(
                tenant_id.clone(),
                run_id,
                timer_id,
                wait_event_id,
                RunTimerKind::Sleep,
                due_at,
            )
            .unwrap(),
        ),
    ];
    let waiting_commit = store
        .append_control_plane_initial_wait_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                wait_event_id,
                JournalExpectation::exact(started.event().head()),
                912,
            ),
            active.lifecycle().revision(),
            initial_checkpoint_write(tenant_id.clone(), run_id, CheckpointId::generate()),
            registrations,
        )
        .await
        .unwrap();
    let waiting = store.load_run(&tenant_id, run_id).await.unwrap();

    let request = store
        .load_interrupt_request(&tenant_id, run_id, interrupt_id)
        .await
        .unwrap();
    let resolution_event_id = EventId::generate();
    let resolution_intent = InterruptResolutionIntent::new(
        &request,
        resolution_event_id,
        payload(913),
        InterruptResolver::new(
            required_principal,
            ScopeSet::try_new([
                "run.resolve".parse::<Scope>().unwrap(),
                "action.approve".parse::<Scope>().unwrap(),
                "audit.read".parse::<Scope>().unwrap(),
            ])
            .unwrap(),
        ),
    )
    .unwrap();
    let resolution_append = control_append(
        tenant_id.clone(),
        run_id,
        resolution_event_id,
        JournalExpectation::exact(waiting_commit.event().head()),
        914,
    );
    let resolved = store
        .resolve_interrupt(
            resolution_append.clone(),
            waiting.lifecycle().revision(),
            resolution_intent.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        resolved,
        InterruptResolutionCommitOutcome::Committed { .. }
    ));
    assert!(!resolved.record().is_outstanding());
    let resolution_retry = store
        .resolve_interrupt(
            resolution_append,
            waiting.lifecycle().revision(),
            resolution_intent,
        )
        .await
        .unwrap();
    assert!(matches!(
        resolution_retry,
        InterruptResolutionCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(resolution_retry.record(), resolved.record());
    assert_eq!(
        store
            .load_interrupt_record(&tenant_id, run_id, interrupt_id)
            .await
            .unwrap(),
        *resolved.record()
    );

    let remaining = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(remaining.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(remaining.unresolved_wait_count(), 1);
    assert!(remaining.next_interrupt_expiry_at().is_none());
    assert_eq!(remaining.next_timer_due_at(), Some(due_at));
    let timer = store
        .load_durable_timer(&tenant_id, run_id, timer_id)
        .await
        .unwrap();
    let firing_event_id = EventId::generate();
    let firing_intent = TimerFiringIntent::new(&timer, firing_event_id).unwrap();
    let firing_append = control_append(
        tenant_id.clone(),
        run_id,
        firing_event_id,
        JournalExpectation::exact(resolved.event().head()),
        915,
    );
    assert!(matches!(
        store
            .fire_timer(
                firing_append.clone(),
                remaining.lifecycle().revision(),
                firing_intent.clone(),
            )
            .await,
        Err(StoreError::InvalidTimerFiring)
    ));
    let observed_micros =
        query_scalar::<_, i64>("SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint")
            .fetch_one(&administration)
            .await
            .unwrap();
    let remaining_micros = due_at
        .unix_micros()
        .saturating_sub(observed_micros)
        .saturating_add(100_000);
    tokio::time::sleep(Duration::from_micros(
        u64::try_from(remaining_micros.max(0)).unwrap(),
    ))
    .await;
    let fired = store
        .fire_timer(
            firing_append.clone(),
            remaining.lifecycle().revision(),
            firing_intent.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(fired, TimerFiringCommitOutcome::Committed { .. }));
    assert!(!fired.record().is_pending());
    let firing_retry = store
        .fire_timer(
            firing_append,
            remaining.lifecycle().revision(),
            firing_intent,
        )
        .await
        .unwrap();
    assert!(matches!(
        firing_retry,
        TimerFiringCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(firing_retry.record(), fired.record());
    assert_eq!(
        store
            .load_durable_timer_record(&tenant_id, run_id, timer_id)
            .await
            .unwrap(),
        *fired.record()
    );

    let active_again = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(active_again.lifecycle().status(), RunStatus::Active);
    assert_eq!(active_again.unresolved_wait_count(), 0);
    assert!(active_again.wait_set_digest().is_none());
    assert!(active_again.scheduler_ready_at().is_some());
    let resolution_count = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.interrupt_resolutions \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    let firing_count = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.timer_firings \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!((resolution_count, firing_count), (1, 1));
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_identical_interrupt_resolutions_converge_on_one_commit() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let options = test_options(Duration::from_secs(30))
        .with_transaction_timeouts(Duration::from_secs(15), Duration::from_secs(45));
    let Some(store) = test_store_with_options(options).await else {
        return;
    };
    let tenant_id = tenant("interrupt-resolution-race");
    let fixture = start_initial_wait_pair(&store, &tenant_id, 1_490).await;
    let waiting = store.load_run(&tenant_id, fixture.run_id).await.unwrap();
    let request = store
        .load_interrupt_request(&tenant_id, fixture.run_id, fixture.interrupt_id)
        .await
        .unwrap();
    let resolution_event_id = EventId::generate();
    let resolution = InterruptResolutionIntent::new(
        &request,
        resolution_event_id,
        payload(1_493),
        InterruptResolver::new(
            PrincipalIdentity::new(
                "https://issuer.example.com/waits"
                    .parse::<IssuerId>()
                    .unwrap(),
                "race-resolver".parse::<SubjectId>().unwrap(),
            ),
            ScopeSet::empty(),
        ),
    )
    .unwrap();
    let append = control_append(
        tenant_id.clone(),
        fixture.run_id,
        resolution_event_id,
        JournalExpectation::exact(fixture.commit.event().head()),
        1_494,
    );
    let expected_revision = waiting.lifecycle().revision();

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let append = append.clone();
        let resolution = resolution.clone();
        tasks.spawn(async move {
            store
                .resolve_interrupt(append, expected_revision, resolution)
                .await
        });
    }
    let mut committed = 0_u64;
    let mut idempotent = 0_u64;
    while let Some(joined) = tasks.join_next().await {
        match joined
            .expect("resolution contender must not panic")
            .unwrap()
        {
            InterruptResolutionCommitOutcome::Committed { .. } => committed += 1,
            InterruptResolutionCommitOutcome::Idempotent { .. } => idempotent += 1,
            outcome => panic!("unexpected resolution outcome: {outcome:?}"),
        }
    }
    assert_eq!((committed, idempotent), (1, 23));
    let remaining = store.load_run(&tenant_id, fixture.run_id).await.unwrap();
    assert_eq!(remaining.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(remaining.unresolved_wait_count(), 1);
    assert_eq!(
        store
            .load_interrupt_record(&tenant_id, fixture.run_id, fixture.interrupt_id)
            .await
            .unwrap()
            .resolution()
            .unwrap()
            .intent(),
        &resolution
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn waiting_run_cancellation_and_failure_abandon_every_wait_exactly_once() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();

    let cancellation_tenant = tenant("wait-abandon-cancellation");
    let cancellation_fixture = start_initial_wait_pair(&store, &cancellation_tenant, 1_500).await;
    let waiting = store
        .load_run(&cancellation_tenant, cancellation_fixture.run_id)
        .await
        .unwrap();
    let cancellation_event_id = EventId::generate();
    let cancellation_append = control_append(
        cancellation_tenant.clone(),
        cancellation_fixture.run_id,
        cancellation_event_id,
        JournalExpectation::exact(cancellation_fixture.commit.event().head()),
        1_503,
    );
    let cancellation_transition = RunTransition::RequestCancellation {
        request: cancellation_request(waiting.lifecycle().changed_at()),
    };
    assert!(matches!(
        store
            .append_control_plane(
                cancellation_append.clone(),
                RunProjection::transition(
                    waiting.lifecycle().revision(),
                    cancellation_transition.clone(),
                ),
            )
            .await,
        Err(StoreError::DurableWaitMutationRequired)
    ));
    let cancellation = store
        .append_control_plane_abandon_waits(
            cancellation_append.clone(),
            waiting.lifecycle().revision(),
            cancellation_transition.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        cancellation,
        WaitAbandonmentCommitOutcome::Committed { .. }
    ));
    assert_eq!(cancellation.abandonments().len(), 2);
    assert!(cancellation.abandonments().iter().all(|abandonment| {
        abandonment.reason() == WaitAbandonmentReason::RunCancellation
            && abandonment.journal() == &cancellation.event().head()
    }));
    let cancelled = store
        .load_run(&cancellation_tenant, cancellation_fixture.run_id)
        .await
        .unwrap();
    assert_eq!(
        cancelled.lifecycle().status(),
        RunStatus::CancellationRequested
    );
    assert_eq!(cancelled.unresolved_wait_count(), 0);
    assert!(cancelled.wait_set_digest().is_none());
    assert!(cancelled.next_timer_due_at().is_none());
    assert!(cancelled.next_interrupt_expiry_at().is_none());

    let interrupt_abandonment = store
        .load_interrupt_abandonment(
            &cancellation_tenant,
            cancellation_fixture.run_id,
            cancellation_fixture.interrupt_id,
        )
        .await
        .unwrap();
    let timer_abandonment = store
        .load_timer_abandonment(
            &cancellation_tenant,
            cancellation_fixture.run_id,
            cancellation_fixture.timer_id,
        )
        .await
        .unwrap();
    assert!(cancellation.abandonments().contains(&interrupt_abandonment));
    assert!(cancellation.abandonments().contains(&timer_abandonment));
    assert!(matches!(
        store
            .load_interrupt_record(
                &cancellation_tenant,
                cancellation_fixture.run_id,
                cancellation_fixture.interrupt_id,
            )
            .await,
        Err(StoreError::WaitWasAbandoned)
    ));
    assert!(matches!(
        store
            .load_durable_timer_record(
                &cancellation_tenant,
                cancellation_fixture.run_id,
                cancellation_fixture.timer_id,
            )
            .await,
        Err(StoreError::WaitWasAbandoned)
    ));

    let cancellation_retry = store
        .append_control_plane_abandon_waits(
            cancellation_append,
            waiting.lifecycle().revision(),
            cancellation_transition,
        )
        .await
        .unwrap();
    assert!(matches!(
        cancellation_retry,
        WaitAbandonmentCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(cancellation_retry.event(), cancellation.event());
    assert_eq!(
        cancellation_retry.abandonments(),
        cancellation.abandonments()
    );

    let failure_tenant = tenant("wait-abandon-failure");
    let failure_fixture = start_initial_wait_pair(&store, &failure_tenant, 1_510).await;
    let failure_waiting = store
        .load_run(&failure_tenant, failure_fixture.run_id)
        .await
        .unwrap();
    let failure_event_id = EventId::generate();
    let failure_append = control_append(
        failure_tenant.clone(),
        failure_fixture.run_id,
        failure_event_id,
        JournalExpectation::exact(failure_fixture.commit.event().head()),
        1_513,
    );
    let failure_transition = RunTransition::Fail {
        failure: terminal_run_failure(failure_event_id, failure_waiting.lifecycle().changed_at()),
    };
    let failed = store
        .append_control_plane_abandon_waits(
            failure_append.clone(),
            failure_waiting.lifecycle().revision(),
            failure_transition.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        failed,
        WaitAbandonmentCommitOutcome::Committed { .. }
    ));
    assert_eq!(failed.abandonments().len(), 2);
    assert!(
        failed
            .abandonments()
            .iter()
            .all(|abandonment| abandonment.reason() == WaitAbandonmentReason::RunFailure)
    );
    let failed_run = store
        .load_run(&failure_tenant, failure_fixture.run_id)
        .await
        .unwrap();
    assert_eq!(failed_run.lifecycle().status(), RunStatus::Failed);
    assert_eq!(failed_run.unresolved_wait_count(), 0);
    let failure_retry = store
        .append_control_plane_abandon_waits(
            failure_append,
            failure_waiting.lifecycle().revision(),
            failure_transition,
        )
        .await
        .unwrap();
    assert!(matches!(
        failure_retry,
        WaitAbandonmentCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(failure_retry.event(), failed.event());
    assert_eq!(failure_retry.abandonments(), failed.abandonments());

    let (abandonment_count, abandoned_projection_count) = query_as::<_, (i64, i64)>(
        "SELECT \
                 (SELECT count(*) FROM stateknot.wait_abandonments \
                  WHERE tenant_id IN ($1, $2)), \
                 (SELECT count(*) FROM stateknot.run_wait_registrations \
                  WHERE tenant_id IN ($1, $2) AND status = 'abandoned')",
    )
    .bind(cancellation_tenant.as_str())
    .bind(failure_tenant.as_str())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!((abandonment_count, abandoned_projection_count), (4, 4));

    let corrupted = query(
        "UPDATE stateknot.wait_abandonments SET reason_kind = 'run_failure' \
         WHERE tenant_id = $1 AND run_id = $2 AND wait_id = $3",
    )
    .bind(cancellation_tenant.as_str())
    .bind(*cancellation_fixture.run_id.as_uuid())
    .bind(*cancellation_fixture.interrupt_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap()
    .rows_affected();
    assert_eq!(corrupted, 1);
    assert!(matches!(
        store
            .load_interrupt_abandonment(
                &cancellation_tenant,
                cancellation_fixture.run_id,
                cancellation_fixture.interrupt_id,
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn wait_abandonment_failure_rolls_back_event_details_projections_and_lifecycle() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("wait-abandon-rollback");
    let fixture = start_initial_wait_pair(&store, &tenant_id, 1_520).await;
    let waiting = store.load_run(&tenant_id, fixture.run_id).await.unwrap();
    let abandonment_event_id = EventId::generate();
    let abandonment_append = control_append(
        tenant_id.clone(),
        fixture.run_id,
        abandonment_event_id,
        JournalExpectation::exact(fixture.commit.event().head()),
        1_523,
    );
    let transition = RunTransition::RequestCancellation {
        request: cancellation_request(waiting.lifecycle().changed_at()),
    };

    query("ALTER TABLE stateknot.runs DROP CONSTRAINT IF EXISTS test_wait_abandon_rollback")
        .execute(&administration)
        .await
        .unwrap();
    let reject_target = format!(
        "ALTER TABLE stateknot.runs ADD CONSTRAINT test_wait_abandon_rollback \
         CHECK (tenant_id <> '{}') NOT VALID",
        tenant_id.as_str()
    );
    query(&reject_target)
        .execute(&administration)
        .await
        .unwrap();
    let result = store
        .append_control_plane_abandon_waits(
            abandonment_append.clone(),
            waiting.lifecycle().revision(),
            transition.clone(),
        )
        .await;
    query("ALTER TABLE stateknot.runs DROP CONSTRAINT test_wait_abandon_rollback")
        .execute(&administration)
        .await
        .unwrap();
    assert!(matches!(result, Err(StoreError::Database { .. })));

    let unchanged = store.load_run(&tenant_id, fixture.run_id).await.unwrap();
    assert_eq!(unchanged.lifecycle().status(), RunStatus::Waiting);
    assert_eq!(unchanged.unresolved_wait_count(), 2);
    assert_eq!(
        unchanged.journal_head(),
        Some(&fixture.commit.event().head())
    );
    let (event_count, abandonment_count, outstanding_count) = query_as::<_, (i64, i64, i64)>(
        "SELECT \
                 (SELECT count(*) FROM stateknot.run_events \
                  WHERE tenant_id = $1 AND run_id = $2 AND event_id = $3), \
                 (SELECT count(*) FROM stateknot.wait_abandonments \
                  WHERE tenant_id = $1 AND run_id = $2), \
                 (SELECT count(*) FROM stateknot.run_wait_registrations \
                  WHERE tenant_id = $1 AND run_id = $2 AND status = 'outstanding')",
    )
    .bind(tenant_id.as_str())
    .bind(*fixture.run_id.as_uuid())
    .bind(*abandonment_event_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!(
        (event_count, abandonment_count, outstanding_count),
        (0, 0, 2)
    );

    let recovered = store
        .append_control_plane_abandon_waits(
            abandonment_append,
            waiting.lifecycle().revision(),
            transition,
        )
        .await
        .unwrap();
    assert!(matches!(
        recovered,
        WaitAbandonmentCommitOutcome::Committed { .. }
    ));
    assert_eq!(recovered.abandonments().len(), 2);

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn due_and_expired_wait_discovery_pages_are_bounded_stable_and_tenant_scoped() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("wait-discovery");
    let run_id = RunId::generate();
    let admitted = store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let started = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                920,
            ),
            RunProjection::transition(
                admitted.lifecycle().revision(),
                RunTransition::Start {
                    started_at: admitted.lifecycle().admitted_at(),
                },
            ),
        )
        .await
        .unwrap();
    let active = store.load_run(&tenant_id, run_id).await.unwrap();
    let database_micros =
        query_scalar::<_, i64>("SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint")
            .fetch_one(&administration)
            .await
            .unwrap();
    let timer_one_due = Timestamp::from_unix_micros(database_micros + 3_000_000).unwrap();
    let timer_two_due = Timestamp::from_unix_micros(database_micros + 3_200_000).unwrap();
    let interrupt_one_expiry = Timestamp::from_unix_micros(database_micros + 3_100_000).unwrap();
    let interrupt_two_expiry = Timestamp::from_unix_micros(database_micros + 3_300_000).unwrap();
    let timer_one_id = TimerId::generate();
    let timer_two_id = TimerId::generate();
    let interrupt_one_id = InterruptId::generate();
    let interrupt_two_id = InterruptId::generate();
    let wait_event_id = EventId::generate();
    let required_scopes = ScopeSet::try_new(["run.resolve".parse::<Scope>().unwrap()]).unwrap();
    let registrations = vec![
        WaitRegistrationIntent::timer(
            TimerRegistrationIntent::new(
                tenant_id.clone(),
                run_id,
                timer_one_id,
                wait_event_id,
                RunTimerKind::Sleep,
                timer_one_due,
            )
            .unwrap(),
        ),
        WaitRegistrationIntent::timer(
            TimerRegistrationIntent::new(
                tenant_id.clone(),
                run_id,
                timer_two_id,
                wait_event_id,
                RunTimerKind::RetryBackoff,
                timer_two_due,
            )
            .unwrap(),
        ),
        WaitRegistrationIntent::interrupt(
            InterruptRequestIntent::new(
                tenant_id.clone(),
                run_id,
                interrupt_one_id,
                wait_event_id,
                RunInterruptKind::ExternalSignal,
                payload(921),
                Digest::sha256(b"expired interrupt one"),
                None,
                required_scopes.clone(),
                Some(interrupt_one_expiry),
            )
            .unwrap(),
        ),
        WaitRegistrationIntent::interrupt(
            InterruptRequestIntent::new(
                tenant_id.clone(),
                run_id,
                interrupt_two_id,
                wait_event_id,
                RunInterruptKind::Reconciliation,
                payload(922),
                Digest::sha256(b"expired interrupt two"),
                None,
                required_scopes,
                Some(interrupt_two_expiry),
            )
            .unwrap(),
        ),
    ];
    let waiting_commit = store
        .append_control_plane_initial_wait_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                wait_event_id,
                JournalExpectation::exact(started.event().head()),
                923,
            ),
            active.lifecycle().revision(),
            initial_checkpoint_write(tenant_id.clone(), run_id, CheckpointId::generate()),
            registrations,
        )
        .await
        .unwrap();
    let observed_micros =
        query_scalar::<_, i64>("SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint")
            .fetch_one(&administration)
            .await
            .unwrap();
    let remaining_micros = interrupt_two_expiry
        .unix_micros()
        .saturating_sub(observed_micros)
        .saturating_add(100_000);
    tokio::time::sleep(Duration::from_micros(
        u64::try_from(remaining_micros.max(0)).unwrap(),
    ))
    .await;

    let page_size = WaitDiscoveryPageSize::new(1).unwrap();
    let first_due = store
        .load_due_timer_page(&tenant_id, None, page_size)
        .await
        .unwrap();
    assert_eq!(first_due.records().len(), 1);
    assert!(first_due.has_more());
    assert_eq!(first_due.records()[0].marker().timer_id(), timer_one_id);
    let due_cursor = first_due.next_cursor().unwrap();
    let wrong_tenant = tenant("wait-discovery-crossed");
    assert!(matches!(
        store
            .load_due_timer_page(&wrong_tenant, Some(&due_cursor), page_size)
            .await,
        Err(StoreError::InvalidDueTimerCursor)
    ));

    let waiting = store.load_run(&tenant_id, run_id).await.unwrap();
    let firing_event_id = EventId::generate();
    store
        .fire_timer(
            control_append(
                tenant_id.clone(),
                run_id,
                firing_event_id,
                JournalExpectation::exact(waiting_commit.event().head()),
                924,
            ),
            waiting.lifecycle().revision(),
            TimerFiringIntent::new(&first_due.records()[0], firing_event_id).unwrap(),
        )
        .await
        .unwrap();
    let second_due = store
        .load_due_timer_page(&tenant_id, Some(&due_cursor), page_size)
        .await
        .unwrap();
    assert_eq!(second_due.snapshot_at(), first_due.snapshot_at());
    assert_eq!(second_due.records().len(), 1);
    assert!(!second_due.has_more());
    assert_eq!(second_due.records()[0].marker().timer_id(), timer_two_id);

    let first_expired = store
        .load_expired_interrupt_page(&tenant_id, None, page_size)
        .await
        .unwrap();
    assert_eq!(first_expired.records().len(), 1);
    assert!(first_expired.has_more());
    assert_eq!(
        first_expired.records()[0].marker().interrupt_id(),
        interrupt_one_id
    );
    let expired_cursor = first_expired.next_cursor().unwrap();
    assert!(matches!(
        store
            .load_expired_interrupt_page(&wrong_tenant, Some(&expired_cursor), page_size)
            .await,
        Err(StoreError::InvalidExpiredInterruptCursor)
    ));
    let second_expired = store
        .load_expired_interrupt_page(&tenant_id, Some(&expired_cursor), page_size)
        .await
        .unwrap();
    assert_eq!(second_expired.snapshot_at(), first_expired.snapshot_at());
    assert_eq!(second_expired.records().len(), 1);
    assert!(!second_expired.has_more());
    assert_eq!(
        second_expired.records()[0].marker().interrupt_id(),
        interrupt_two_id
    );
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_quarantine_observation_cannot_stop_a_newer_run_head() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("stale-quarantine");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let appended = store
        .append_control_plane(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                930,
            ),
            RunProjection::unchanged(),
        )
        .await
        .unwrap();
    let stale = quarantine_request(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::empty(),
        RunQuarantineCause::IntegrityFailure,
        "journal.stale_observation",
        b"stale evidence",
    );
    assert!(matches!(
        store.quarantine_run(stale).await,
        Err(StoreError::StaleRunQuarantineObservation)
    ));
    assert!(
        !store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .is_quarantined()
    );
    assert!(matches!(
        store.load_run_quarantine(&tenant_id, run_id).await,
        Err(StoreError::RunQuarantineNotFound)
    ));

    let exact = quarantine_request(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(appended.event().head()),
        RunQuarantineCause::IntegrityFailure,
        "journal.chain",
        b"exact journal evidence",
    );
    store
        .quarantine_run(exact)
        .await
        .expect("exact current observation must quarantine");
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_identical_quarantine_requests_converge_on_one_record() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let options = test_options(Duration::from_secs(30))
        .with_transaction_timeouts(Duration::from_secs(15), Duration::from_secs(45));
    let Some(store) = test_store_with_options(options).await else {
        return;
    };
    let tenant_id = tenant("quarantine-race");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let request = quarantine_request(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::empty(),
        RunQuarantineCause::IntegrityFailure,
        "checkpoint.concurrent_digest",
        b"shared concurrent evidence",
    );
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let request = request.clone();
        tasks.spawn(async move { store.quarantine_run(request).await });
    }
    let mut committed = 0;
    let mut idempotent = 0;
    while let Some(joined) = tasks.join_next().await {
        match joined
            .expect("quarantine contender must not panic")
            .unwrap()
        {
            RunQuarantineCommitOutcome::Committed(_) => committed += 1,
            RunQuarantineCommitOutcome::Idempotent(_) => idempotent += 1,
            _ => unreachable!("quarantine outcomes are closed for this provider version"),
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(idempotent, 23);
    assert_eq!(
        store
            .load_run_quarantine(&tenant_id, run_id)
            .await
            .unwrap()
            .request(),
        &request
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quarantine_update_failure_rolls_back_evidence_and_corruption_fails_closed() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let tenant_id = tenant("quarantine-rollback");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let request = quarantine_request(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::empty(),
        RunQuarantineCause::IntegrityFailure,
        "checkpoint.rollback",
        b"rollback evidence",
    );
    query("ALTER TABLE stateknot.runs DROP CONSTRAINT IF EXISTS test_quarantine_rollback")
        .execute(&administration)
        .await
        .unwrap();
    let reject_target = format!(
        "ALTER TABLE stateknot.runs ADD CONSTRAINT test_quarantine_rollback CHECK (tenant_id <> '{}') NOT VALID",
        tenant_id.as_str()
    );
    query(&reject_target)
        .execute(&administration)
        .await
        .unwrap();
    assert!(matches!(
        store.quarantine_run(request.clone()).await,
        Err(StoreError::Database { .. })
    ));
    let evidence_count = query_scalar::<_, i64>(
        "SELECT count(*) FROM stateknot.run_quarantines WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!(evidence_count, 0);
    assert!(
        !store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .is_quarantined()
    );
    query("ALTER TABLE stateknot.runs DROP CONSTRAINT test_quarantine_rollback")
        .execute(&administration)
        .await
        .unwrap();

    store
        .quarantine_run(request)
        .await
        .expect("same request must recover after rollback");
    query(
        "UPDATE stateknot.run_quarantines \
         SET record_digest = decode(repeat('00', 32), 'hex') \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store.load_run_quarantine(&tenant_id, run_id).await,
        Err(StoreError::CorruptData { .. })
    ));
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn recovery_wrapper_quarantines_only_corruption_and_rejects_stale_observations() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();

    let healthy_tenant = tenant("recovery-wrapper-healthy");
    let healthy_run = RunId::generate();
    store
        .admit_run(provenance(healthy_tenant.clone(), healthy_run))
        .await
        .unwrap();
    let healthy_context = CorruptionQuarantineContext::new(
        healthy_tenant.clone(),
        healthy_run,
        QuarantineId::generate(),
        JournalExpectation::empty(),
        Digest::sha256(b"healthy recovery evidence"),
    )
    .unwrap();
    let healthy = store
        .with_corruption_quarantine(
            healthy_context,
            store.load_run(&healthy_tenant, healthy_run),
        )
        .await
        .expect("successful recovery reads must pass through");
    assert!(!healthy.is_quarantined());

    let ordinary_error_context = CorruptionQuarantineContext::new(
        healthy_tenant.clone(),
        healthy_run,
        QuarantineId::generate(),
        JournalExpectation::empty(),
        Digest::sha256(b"ordinary error evidence"),
    )
    .unwrap();
    assert!(matches!(
        store
            .with_corruption_quarantine(
                ordinary_error_context,
                store.load_checkpoint(&healthy_tenant, healthy_run, CheckpointId::generate()),
            )
            .await,
        Err(StoreError::CheckpointNotFound)
    ));
    assert!(matches!(
        store
            .load_run_quarantine(&healthy_tenant, healthy_run)
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));

    let corrupt_tenant = tenant("recovery-wrapper-corrupt");
    let corrupt_run = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &corrupt_tenant,
        corrupt_run,
        940,
    ))
    .await;
    query(
        "UPDATE stateknot.run_checkpoints \
         SET checkpoint_bytes = checkpoint_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $3",
    )
    .bind(corrupt_tenant.as_str())
    .bind(*corrupt_run.as_uuid())
    .bind(*checkpoint.checkpoint().checkpoint_id().as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    let corrupt_context = CorruptionQuarantineContext::new(
        corrupt_tenant.clone(),
        corrupt_run,
        QuarantineId::generate(),
        JournalExpectation::exact(checkpoint.event().head()),
        Digest::sha256(b"retained corrupt checkpoint evidence"),
    )
    .unwrap();
    for context in [corrupt_context.clone(), corrupt_context] {
        assert!(matches!(
            store
                .with_corruption_quarantine(
                    context,
                    store.load_checkpoint(
                        &corrupt_tenant,
                        corrupt_run,
                        checkpoint.checkpoint().checkpoint_id(),
                    ),
                )
                .await,
            Err(StoreError::RunQuarantined)
        ));
    }
    let quarantine = store
        .load_run_quarantine(&corrupt_tenant, corrupt_run)
        .await
        .unwrap();
    assert_eq!(
        quarantine.request().cause(),
        RunQuarantineCause::IntegrityFailure
    );
    assert!(
        quarantine
            .request()
            .component()
            .as_str()
            .starts_with("store.checkpoint")
    );

    let stale_tenant = tenant("recovery-wrapper-stale");
    let stale_run = RunId::generate();
    let stale_checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &stale_tenant,
        stale_run,
        941,
    ))
    .await;
    query(
        "UPDATE stateknot.run_checkpoints \
         SET checkpoint_bytes = checkpoint_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $3",
    )
    .bind(stale_tenant.as_str())
    .bind(*stale_run.as_uuid())
    .bind(*stale_checkpoint.checkpoint().checkpoint_id().as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    let stale_context = CorruptionQuarantineContext::new(
        stale_tenant.clone(),
        stale_run,
        QuarantineId::generate(),
        JournalExpectation::empty(),
        Digest::sha256(b"stale corruption evidence"),
    )
    .unwrap();
    assert!(matches!(
        store
            .with_corruption_quarantine(
                stale_context,
                store.load_checkpoint(
                    &stale_tenant,
                    stale_run,
                    stale_checkpoint.checkpoint().checkpoint_id(),
                ),
            )
            .await,
        Err(StoreError::StaleRunQuarantineObservation)
    ));
    assert!(
        !store
            .load_run(&stale_tenant, stale_run)
            .await
            .unwrap()
            .is_quarantined()
    );

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn claimed_run_recovery_requires_exact_live_fence_and_journal_observation() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("claimed-recovery");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 950)).await;
    let claim = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .expect("runnable run must be claimable");
    let fence = claim.lease().fence().clone();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(checkpoint.event().head()),
        Digest::sha256(b"claimed recovery evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .expect("exact live ownership must start recovery");
    assert_eq!(recovery.fence(), &fence);
    assert_eq!(recovery.quarantine_context().expected_fence(), Some(&fence));
    assert_eq!(
        recovery
            .initial_run()
            .lease()
            .expect("recovery starts under a live lease")
            .fence(),
        &fence
    );

    let lineage = recovery
        .load_checkpoint_lineage_page(None, CheckpointLineagePageSize::new(8).unwrap())
        .await
        .expect("checkpoint lineage must recover through the guarded surface");
    assert_eq!(lineage.checkpoints(), &[checkpoint.checkpoint().clone()]);
    assert!(lineage.next_cursor().is_none());
    let journal = recovery
        .load_journal_page(None, JournalPageSize::new(16).unwrap())
        .await
        .expect("journal must recover through the guarded surface");
    assert_eq!(journal.events(), &[checkpoint.event().clone()]);
    assert!(!journal.has_more());

    assert!(matches!(
        recovery
            .load_tool_invocation_history_page(
                InvocationId::generate(),
                None,
                ToolInvocationHistoryPageSize::new(1).unwrap(),
            )
            .await,
        Err(StoreError::ToolInvocationNotFound)
    ));
    assert!(matches!(
        store.load_run_quarantine(&tenant_id, run_id).await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    recovery
        .revalidate()
        .await
        .expect("unchanged live recovery must revalidate");

    let stale_observation = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::empty(),
        Digest::sha256(b"stale claimed recovery evidence"),
    )
    .unwrap();
    assert!(matches!(
        store
            .begin_claimed_run_recovery(fence.clone(), stale_observation)
            .await,
        Err(StoreError::StaleClaimedRunRecoveryObservation)
    ));

    let crossed = CorruptionQuarantineContext::new(
        tenant("claimed-recovery-crossed"),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::empty(),
        Digest::sha256(b"crossed claimed recovery evidence"),
    )
    .unwrap();
    assert!(matches!(
        store
            .begin_claimed_run_recovery(fence.clone(), crossed)
            .await,
        Err(StoreError::InvalidClaimedRunRecoveryContext)
    ));

    let successor = store
        .supersede_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .expect("trusted takeover must issue a successor fence");
    assert!(matches!(
        recovery.revalidate().await,
        Err(StoreError::StaleFence)
    ));
    let successor_context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(checkpoint.event().head()),
        Digest::sha256(b"successor claimed recovery evidence"),
    )
    .unwrap();
    store
        .begin_claimed_run_recovery(successor.lease().fence().clone(), successor_context)
        .await
        .expect("the exact successor may recover the unchanged journal");
    assert!(matches!(
        store.load_run_quarantine(&tenant_id, run_id).await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn claimed_recovery_quarantine_cannot_cross_a_successor_fence() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();

    let current_tenant = tenant("claimed-recovery-current");
    let current_run = RunId::generate();
    let current_checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &current_tenant,
        current_run,
        951,
    ))
    .await;
    let current_claim = store
        .claim_lease(&current_tenant, current_run, AttemptId::generate())
        .await
        .unwrap();
    let current_fence = current_claim.lease().fence().clone();
    let current_context = CorruptionQuarantineContext::new(
        current_tenant.clone(),
        current_run,
        QuarantineId::generate(),
        JournalExpectation::exact(current_checkpoint.event().head()),
        Digest::sha256(b"current fenced corruption evidence"),
    )
    .unwrap();
    let current_recovery = store
        .begin_claimed_run_recovery(current_fence.clone(), current_context)
        .await
        .unwrap();
    corrupt_checkpoint_bytes(
        &administration,
        &current_tenant,
        current_run,
        current_checkpoint.checkpoint().checkpoint_id(),
    )
    .await;
    for _ in 0..2 {
        assert!(matches!(
            current_recovery
                .load_checkpoint_lineage_page(None, CheckpointLineagePageSize::new(8).unwrap())
                .await,
            Err(StoreError::RunQuarantined)
        ));
    }
    let current_quarantine = store
        .load_run_quarantine(&current_tenant, current_run)
        .await
        .expect("fenced evidence must be auditable");
    assert_eq!(
        current_quarantine.request().expected_fence(),
        Some(&current_fence)
    );
    let stopped = store.load_run(&current_tenant, current_run).await.unwrap();
    assert!(stopped.is_quarantined());
    assert!(stopped.lease().is_none());
    query(
        "UPDATE stateknot.runs SET fencing_epoch = fencing_epoch + 1 \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(current_tenant.as_str())
    .bind(*current_run.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_run_quarantine(&current_tenant, current_run)
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let stale_tenant = tenant("claimed-recovery-stale-owner");
    let stale_run = RunId::generate();
    let stale_checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &stale_tenant,
        stale_run,
        952,
    ))
    .await;
    let stale_claim = store
        .claim_lease(&stale_tenant, stale_run, AttemptId::generate())
        .await
        .unwrap();
    let stale_fence = stale_claim.lease().fence().clone();
    let stale_context = CorruptionQuarantineContext::new(
        stale_tenant.clone(),
        stale_run,
        QuarantineId::generate(),
        JournalExpectation::exact(stale_checkpoint.event().head()),
        Digest::sha256(b"stale owner corruption evidence"),
    )
    .unwrap();
    let stale_recovery = store
        .begin_claimed_run_recovery(stale_fence, stale_context)
        .await
        .unwrap();
    let successor = store
        .supersede_lease(&stale_tenant, stale_run, AttemptId::generate())
        .await
        .expect("successor must replace the recovery owner");
    corrupt_checkpoint_bytes(
        &administration,
        &stale_tenant,
        stale_run,
        stale_checkpoint.checkpoint().checkpoint_id(),
    )
    .await;
    assert!(matches!(
        stale_recovery
            .load_checkpoint_lineage_page(None, CheckpointLineagePageSize::new(8).unwrap())
            .await,
        Err(StoreError::StaleFence)
    ));
    let still_owned = store.load_run(&stale_tenant, stale_run).await.unwrap();
    assert!(!still_owned.is_quarantined());
    assert_eq!(
        still_owned
            .lease()
            .expect("successor lease must remain")
            .fence(),
        successor.lease().fence()
    );
    assert!(matches!(
        store.load_run_quarantine(&stale_tenant, stale_run).await,
        Err(StoreError::RunQuarantineNotFound)
    ));

    let successor_context = CorruptionQuarantineContext::new(
        stale_tenant.clone(),
        stale_run,
        QuarantineId::generate(),
        JournalExpectation::exact(stale_checkpoint.event().head()),
        Digest::sha256(b"successor corruption evidence"),
    )
    .unwrap();
    let successor_recovery = store
        .begin_claimed_run_recovery(successor.lease().fence().clone(), successor_context)
        .await
        .expect("successor may inspect the same corrupt durable state");
    assert!(matches!(
        successor_recovery
            .load_checkpoint_lineage_page(None, CheckpointLineagePageSize::new(8).unwrap())
            .await,
        Err(StoreError::RunQuarantined)
    ));

    let expired_tenant = tenant("claimed-recovery-expired-owner");
    let expired_run = RunId::generate();
    let expired_checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &expired_tenant,
        expired_run,
        953,
    ))
    .await;
    let expired_claim = store
        .claim_lease(&expired_tenant, expired_run, AttemptId::generate())
        .await
        .unwrap();
    let expired_context = CorruptionQuarantineContext::new(
        expired_tenant.clone(),
        expired_run,
        QuarantineId::generate(),
        JournalExpectation::exact(expired_checkpoint.event().head()),
        Digest::sha256(b"expired owner corruption evidence"),
    )
    .unwrap();
    let expired_recovery = store
        .begin_claimed_run_recovery(expired_claim.lease().fence().clone(), expired_context)
        .await
        .unwrap();
    query(
        "UPDATE stateknot.runs \
         SET lease_acquired_at = clock_timestamp() - interval '2 seconds', \
             lease_renewed_at = clock_timestamp() - interval '1 second', \
             lease_expires_at = clock_timestamp() - interval '1 microsecond' \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(expired_tenant.as_str())
    .bind(*expired_run.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    corrupt_checkpoint_bytes(
        &administration,
        &expired_tenant,
        expired_run,
        expired_checkpoint.checkpoint().checkpoint_id(),
    )
    .await;
    assert!(matches!(
        expired_recovery
            .load_checkpoint_lineage_page(None, CheckpointLineagePageSize::new(8).unwrap())
            .await,
        Err(StoreError::LeaseExpired)
    ));
    assert!(
        !store
            .load_run(&expired_tenant, expired_run)
            .await
            .unwrap()
            .is_quarantined()
    );
    assert!(matches!(
        store
            .load_run_quarantine(&expired_tenant, expired_run)
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_recovery_plans_fresh_ready_nodes_deterministically() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };

    let fresh_tenant = tenant("ready-plan-fresh");
    let fresh_run = RunId::generate();
    let fresh_checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &fresh_tenant,
        fresh_run,
        960,
    ))
    .await;
    let fresh_claim = store
        .claim_lease(&fresh_tenant, fresh_run, AttemptId::generate())
        .await
        .unwrap();
    let fresh_fence = fresh_claim.lease().fence().clone();
    let fresh_context = CorruptionQuarantineContext::new(
        fresh_tenant.clone(),
        fresh_run,
        QuarantineId::generate(),
        JournalExpectation::exact(fresh_checkpoint.event().head()),
        Digest::sha256(b"fresh ready-plan evidence"),
    )
    .unwrap();
    let fresh_recovery = store
        .begin_claimed_run_recovery(fresh_fence.clone(), fresh_context)
        .await
        .unwrap();
    let fresh_plan = fresh_recovery.plan_ready_nodes().await.unwrap();
    assert!(fresh_plan.observed_at() >= fresh_recovery.initial_observed_at());
    assert_eq!(fresh_plan.fence(), &fresh_fence);
    assert_eq!(fresh_plan.checkpoint(), fresh_checkpoint.checkpoint());
    assert_eq!(fresh_plan.nodes().len(), 1);
    assert_eq!(fresh_plan.nodes()[0].kind(), RecoveryNodeKind::Dispatchable);
    assert_eq!(
        fresh_plan.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::FirstAttempt)
    );
    assert_eq!(
        fresh_plan.nodes()[0].activation(),
        &NodeActivation::for_ready_root(
            fresh_checkpoint.checkpoint(),
            NodeId::new("node-0001").unwrap(),
        )
        .unwrap()
    );
    fresh_recovery.revalidate().await.unwrap();
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_recovery_plans_superseded_attempt_for_crash_takeover() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let takeover_tenant = tenant("ready-plan-takeover");
    let takeover_run = RunId::generate();
    let takeover_checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &takeover_tenant,
        takeover_run,
        970,
    ))
    .await;
    let old_claim = store
        .claim_lease(&takeover_tenant, takeover_run, AttemptId::generate())
        .await
        .unwrap();
    let activation = NodeActivation::for_ready_root(
        takeover_checkpoint.checkpoint(),
        NodeId::new("node-0001").unwrap(),
    )
    .unwrap();
    let started = store
        .start_node_attempt(
            worker_append(
                takeover_tenant.clone(),
                takeover_run,
                EventId::generate(),
                JournalExpectation::exact(takeover_checkpoint.event().head()),
                old_claim.lease().fence().clone(),
                971,
            ),
            activation.clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let successor = store
        .supersede_lease(&takeover_tenant, takeover_run, AttemptId::generate())
        .await
        .unwrap();
    let successor_fence = successor.lease().fence().clone();
    let takeover_context = CorruptionQuarantineContext::new(
        takeover_tenant.clone(),
        takeover_run,
        QuarantineId::generate(),
        JournalExpectation::exact(started.event().head()),
        Digest::sha256(b"takeover ready-plan evidence"),
    )
    .unwrap();
    let takeover_recovery = store
        .begin_claimed_run_recovery(successor_fence.clone(), takeover_context)
        .await
        .unwrap();
    let takeover_plan = takeover_recovery.plan_ready_nodes().await.unwrap();
    assert_eq!(takeover_plan.nodes().len(), 1);
    assert_eq!(takeover_plan.nodes()[0].activation(), &activation);
    assert_eq!(
        takeover_plan.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::SupersededAttempt)
    );
    takeover_recovery.revalidate().await.unwrap();
    assert!(matches!(
        store
            .load_run_quarantine(&takeover_tenant, takeover_run)
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn delayed_retry_wakeup_is_plan_bound_idempotent_and_scheduler_visible_when_due() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("delayed-retry-wakeup");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 972)).await;
    let claim = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap();
    let fence = claim.lease().fence().clone();
    let initial_context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(checkpoint.event().head()),
        Digest::sha256(b"initial delayed-retry plan evidence"),
    )
    .unwrap();
    let initial_recovery = store
        .begin_claimed_run_recovery(fence.clone(), initial_context)
        .await
        .unwrap();
    let initial_plan = initial_recovery.plan_ready_nodes().await.unwrap();
    assert_eq!(
        initial_plan.nodes()[0].kind(),
        RecoveryNodeKind::Dispatchable
    );
    assert!(matches!(
        store.schedule_delayed_retry_wakeup(&initial_plan).await,
        Err(StoreError::InvalidDelayedRetryPlan)
    ));

    let node_id = NodeId::new("node-0001").unwrap();
    let started = store
        .start_recovered_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(initial_plan.journal_head().clone()),
                fence.clone(),
                973,
            ),
            &initial_plan,
            &node_id,
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let failure_event_id = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::DependencyUnavailable,
        FailureCode::new("node.delayed_retry_wakeup").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("The node must remain hidden until its durable retry time.").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(3_000).unwrap(),
        },
    )
    .unwrap()
    .with_caused_by_event(failure_event_id);
    let failed = store
        .fail_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                failure_event_id,
                JournalExpectation::exact(started.event().head()),
                fence.clone(),
                974,
            ),
            &started.attempt().start().head(),
            failure,
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    let deferred_context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(failed.event().head()),
        Digest::sha256(b"deferred delayed-retry plan evidence"),
    )
    .unwrap();
    let deferred_recovery = store
        .begin_claimed_run_recovery(fence.clone(), deferred_context)
        .await
        .unwrap();
    let deferred_plan = deferred_recovery.plan_ready_nodes().await.unwrap();
    assert_eq!(deferred_plan.nodes()[0].kind(), RecoveryNodeKind::Deferred);
    let not_before = deferred_plan.earliest_deferred_at().unwrap();
    assert!(not_before > deferred_plan.observed_at());
    let queue_age = store
        .load_run(&tenant_id, run_id)
        .await
        .unwrap()
        .scheduler_ready_at()
        .unwrap();

    let scheduled = store
        .schedule_delayed_retry_wakeup(&deferred_plan)
        .await
        .expect("the exact deferred plan must atomically release and schedule");
    assert_eq!(
        scheduled,
        DelayedRetryScheduleOutcome::Scheduled { not_before }
    );
    let sleeping = store.load_run(&tenant_id, run_id).await.unwrap();
    assert!(sleeping.lease().is_none());
    assert_eq!(sleeping.scheduler_ready_at(), Some(queue_age));
    assert_eq!(sleeping.scheduler_not_before(), Some(not_before));

    assert!(matches!(
        store
            .claim_lease(&tenant_id, run_id, AttemptId::generate())
            .await,
        Err(StoreError::RunNotYetAvailable)
    ));
    let hidden = store
        .load_runnable_run_page(
            &tenant_id,
            None,
            RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
        )
        .await
        .unwrap();
    assert!(
        hidden
            .records()
            .iter()
            .all(|candidate| candidate.run().lifecycle().provenance().run_id() != run_id)
    );
    assert_eq!(
        store
            .schedule_delayed_retry_wakeup(&deferred_plan)
            .await
            .expect("a lost scheduling acknowledgement must converge"),
        DelayedRetryScheduleOutcome::Idempotent { not_before }
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let due_candidate = loop {
        let page = store
            .load_runnable_run_page(
                &tenant_id,
                None,
                RunnableRunPageSize::new(RunnableRunPageSize::MAX).unwrap(),
            )
            .await
            .unwrap();
        if let Some(candidate) = page
            .records()
            .iter()
            .find(|candidate| candidate.run().lifecycle().provenance().run_id() == run_id)
        {
            break candidate.clone();
        }
        assert!(
            Instant::now() < deadline,
            "the indexed delayed retry never became scheduler-visible"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(due_candidate.ready_at(), queue_age);
    assert_eq!(due_candidate.available_at(), not_before);
    assert_eq!(due_candidate.run().scheduler_not_before(), Some(not_before));

    let successor = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .expect("an inclusive due retry must be directly claimable");
    assert!(matches!(successor, LeaseClaimOutcome::Claimed(_)));
    assert_eq!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .scheduler_not_before(),
        None
    );
    assert!(matches!(
        store.schedule_delayed_retry_wakeup(&deferred_plan).await,
        Err(StoreError::StaleFence)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn delayed_retry_that_becomes_due_keeps_ownership_for_replanning() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("delayed-retry-due-race");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 978)).await;
    let claim = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap();
    let fence = claim.lease().fence().clone();
    let activation = pending_activation(checkpoint.checkpoint(), b"due retry race");
    let started = store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                fence.clone(),
                979,
            ),
            activation.clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let failure_event_id = EventId::generate();
    let failure = Failure::new(
        FailureId::generate(),
        FailureCategory::DependencyUnavailable,
        FailureCode::new("node.retry_due_during_schedule").unwrap(),
        FailureOrigin::new("graph.integration").unwrap(),
        FailureMessage::new("The retry becomes due before scheduler projection commits.").unwrap(),
        RetryAdvice::SafeAfter {
            delay: DurationMillis::new(250).unwrap(),
        },
    )
    .unwrap()
    .with_caused_by_event(failure_event_id);
    let failed = store
        .fail_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                failure_event_id,
                JournalExpectation::exact(started.event().head()),
                fence.clone(),
                980,
            ),
            &started.attempt().start().head(),
            failure,
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    let recovery_context = || {
        CorruptionQuarantineContext::new(
            tenant_id.clone(),
            run_id,
            QuarantineId::generate(),
            JournalExpectation::exact(failed.event().head()),
            Digest::sha256(b"due-race delayed retry evidence"),
        )
        .unwrap()
    };
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), recovery_context())
        .await
        .unwrap();
    let deferred_plan = recovery.plan_ready_nodes().await.unwrap();
    assert_eq!(deferred_plan.nodes()[0].kind(), RecoveryNodeKind::Deferred);
    let not_before = deferred_plan.earliest_deferred_at().unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        store
            .schedule_delayed_retry_wakeup(&deferred_plan)
            .await
            .unwrap(),
        DelayedRetryScheduleOutcome::Due { not_before }
    );
    let still_owned = store.load_run(&tenant_id, run_id).await.unwrap();
    assert_eq!(still_owned.lease().unwrap().fence(), &fence);
    assert_eq!(still_owned.scheduler_not_before(), None);

    let due_recovery = store
        .begin_claimed_run_recovery(fence.clone(), recovery_context())
        .await
        .unwrap();
    let due_plan = due_recovery.plan_ready_nodes().await.unwrap();
    assert_eq!(
        due_plan.nodes()[0].dispatch_reason(),
        Some(NodeDispatchReason::SafeRetry)
    );
    let restarted = store
        .start_recovered_node_attempt(
            worker_append(
                tenant_id,
                run_id,
                EventId::generate(),
                JournalExpectation::exact(due_plan.journal_head().clone()),
                fence,
                981,
            ),
            &due_plan,
            activation.node_id(),
            AttemptId::generate(),
        )
        .await
        .expect("replanning under the retained lease must admit the due retry");
    assert_eq!(restarted.attempt().status(), NodeAttemptStatus::Executing);
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn recovered_node_start_is_durable_idempotent_and_plan_scoped() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("ready-plan-dispatch");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 975)).await;
    let claim = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap();
    let fence = claim.lease().fence().clone();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(checkpoint.event().head()),
        Digest::sha256(b"ready-plan dispatch evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence.clone(), context)
        .await
        .unwrap();
    let plan = Arc::new(recovery.plan_ready_nodes().await.unwrap());
    let node_id = NodeId::new("node-0001").unwrap();
    let event_id = EventId::generate();
    let attempt_id = AttemptId::generate();
    let append = || {
        worker_append(
            tenant_id.clone(),
            run_id,
            event_id,
            JournalExpectation::exact(plan.journal_head().clone()),
            fence.clone(),
            976,
        )
    };

    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        let fence = fence.clone();
        let plan = Arc::clone(&plan);
        let node_id = node_id.clone();
        tasks.spawn(async move {
            for _ in 0..64 {
                let append = worker_append(
                    tenant_id.clone(),
                    run_id,
                    event_id,
                    JournalExpectation::exact(plan.journal_head().clone()),
                    fence.clone(),
                    976,
                );
                match store
                    .start_recovered_node_attempt(append, &plan, &node_id, attempt_id)
                    .await
                {
                    result @ Ok(_) => return result,
                    Err(error) if error.is_retryable() => tokio::task::yield_now().await,
                    error @ Err(_) => return error,
                }
            }
            panic!("identical recovered starts did not converge within the test bound")
        });
    }

    let mut committed = 0_u64;
    let mut idempotent = 0_u64;
    let mut winner = None;
    while let Some(joined) = tasks.join_next().await {
        let outcome = joined
            .expect("recovered start task must not panic")
            .expect("all identical recovered starts must converge");
        let observed = (
            outcome.event().head().clone(),
            outcome.attempt().start().head().clone(),
        );
        if let Some(winner) = &winner {
            assert_eq!(&observed, winner);
        } else {
            winner = Some(observed);
        }
        match outcome {
            NodeAttemptCommitOutcome::Committed { .. } => committed += 1,
            NodeAttemptCommitOutcome::Idempotent { .. } => idempotent += 1,
            _ => panic!("unexpected recovered node-start outcome"),
        }
    }
    assert_eq!(committed, 1);
    assert_eq!(idempotent, 23);
    let (winner_event, winner_start) = winner.expect("one physical start must win");
    assert_eq!(winner_start.activation(), plan.nodes()[0].activation());

    let retry = store
        .start_recovered_node_attempt(append(), &plan, &node_id, attempt_id)
        .await
        .expect("lost durable-start acknowledgement must converge");
    assert!(matches!(retry, NodeAttemptCommitOutcome::Idempotent { .. }));
    assert_eq!(retry.attempt().start().head(), winner_start);
    assert_eq!(
        store
            .load_node_attempt(&tenant_id, &run_id, attempt_id)
            .await
            .unwrap()
            .start()
            .head(),
        winner_start
    );

    assert!(matches!(
        store
            .start_recovered_node_attempt(
                append(),
                &plan,
                &NodeId::new("absent-node").unwrap(),
                AttemptId::generate(),
            )
            .await,
        Err(StoreError::ReadyNodeNotDispatchable)
    ));
    let control_append = control_append(
        tenant_id,
        run_id,
        EventId::generate(),
        JournalExpectation::exact(winner_event),
        977,
    );
    assert!(matches!(
        store
            .start_recovered_node_attempt(control_append, &plan, &node_id, AttemptId::generate(),)
            .await,
        Err(StoreError::InvalidReadyNodeDispatchPlan)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_recovery_reuses_attempt_owned_results_as_barrier_input() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("ready-plan-completed");
    let run_id = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 980)).await;
    let claim = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap();
    let fence = claim.lease().fence().clone();
    let activation =
        NodeActivation::for_ready_root(checkpoint.checkpoint(), NodeId::new("node-0001").unwrap())
            .unwrap();
    let started = store
        .start_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(checkpoint.event().head()),
                fence.clone(),
                981,
            ),
            activation.clone(),
            AttemptId::generate(),
        )
        .await
        .unwrap();
    let completed = store
        .succeed_node_attempt(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(started.event().head()),
                fence.clone(),
                982,
            ),
            &started.attempt().start().head(),
            pending_result_intent(activation.clone(), NodeInvocationBindings::empty()),
            BudgetUsage::zero(),
        )
        .await
        .unwrap();
    let durable_result = store.load_pending_node_result(&activation).await.unwrap();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(completed.event().head()),
        Digest::sha256(b"completed ready-plan evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence, context)
        .await
        .unwrap();
    let plan = recovery.plan_ready_nodes().await.unwrap();
    assert!(plan.is_barrier_ready());
    assert_eq!(plan.nodes()[0].kind(), RecoveryNodeKind::Completed);
    assert_eq!(plan.nodes()[0].result(), Some(&durable_result.head()));
    assert_eq!(
        plan.barrier_result_heads().unwrap().unwrap().as_slice(),
        &[durable_result.head()]
    );
    recovery.revalidate().await.unwrap();
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_recovery_ready_plan_treats_missing_checkpoint_as_ordinary_state() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };

    let pending_tenant = tenant("ready-plan-no-checkpoint");
    let pending_run = RunId::generate();
    store
        .admit_run(provenance(pending_tenant.clone(), pending_run))
        .await
        .unwrap();
    let pending_claim = store
        .claim_lease(&pending_tenant, pending_run, AttemptId::generate())
        .await
        .unwrap();
    let pending_context = CorruptionQuarantineContext::new(
        pending_tenant.clone(),
        pending_run,
        QuarantineId::generate(),
        JournalExpectation::empty(),
        Digest::sha256(b"missing checkpoint ready-plan evidence"),
    )
    .unwrap();
    let pending_recovery = store
        .begin_claimed_run_recovery(pending_claim.lease().fence().clone(), pending_context)
        .await
        .unwrap();
    assert!(matches!(
        pending_recovery.plan_ready_nodes().await,
        Err(StoreError::ReadyNodeRecoveryCheckpointMissing)
    ));
    assert!(matches!(
        store
            .load_run_quarantine(&pending_tenant, pending_run)
            .await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noncanonical_ready_activation_is_rejected_before_recovery() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let drift_tenant = tenant("ready-plan-drift");
    let drift_run = RunId::generate();
    let checkpoint = Box::pin(start_run_with_checkpoint(
        &store,
        &drift_tenant,
        drift_run,
        990,
    ))
    .await;
    let claim = store
        .claim_lease(&drift_tenant, drift_run, AttemptId::generate())
        .await
        .unwrap();
    let fence = claim.lease().fence().clone();
    let drifted_activation =
        drifted_pending_activation(checkpoint.checkpoint(), b"drifted input digest");
    assert!(matches!(
        store
            .start_node_attempt(
                worker_append(
                    drift_tenant.clone(),
                    drift_run,
                    EventId::generate(),
                    JournalExpectation::exact(checkpoint.event().head()),
                    fence.clone(),
                    991,
                ),
                drifted_activation.clone(),
                AttemptId::generate(),
            )
            .await,
        Err(StoreError::InvalidNodeAttemptActivation)
    ));
    let drift_context = CorruptionQuarantineContext::new(
        drift_tenant.clone(),
        drift_run,
        QuarantineId::generate(),
        JournalExpectation::exact(checkpoint.event().head()),
        Digest::sha256(b"drifted ready-plan evidence"),
    )
    .unwrap();
    let drift_recovery = store
        .begin_claimed_run_recovery(fence.clone(), drift_context)
        .await
        .unwrap();
    let plan = drift_recovery.plan_ready_nodes().await.unwrap();
    assert_eq!(
        plan.nodes()[0].activation(),
        &NodeActivation::for_ready_root(
            checkpoint.checkpoint(),
            NodeId::new("node-0001").unwrap(),
        )
        .unwrap()
    );
    assert_eq!(plan.nodes()[0].kind(), RecoveryNodeKind::Dispatchable);
    assert!(matches!(
        store.load_run_quarantine(&drift_tenant, drift_run).await,
        Err(StoreError::RunQuarantineNotFound)
    ));
    assert_eq!(
        store
            .load_run(&drift_tenant, drift_run)
            .await
            .unwrap()
            .journal_head()
            .cloned(),
        Some(checkpoint.event().head())
    );
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn claimed_recovery_migration_eleven_preserves_unfenced_v1_quarantine_evidence() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v11_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .unwrap();

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .unwrap();
    let fixture_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .unwrap();
    let tenant_id = tenant("v11-unfenced-evidence");
    let run_id = RunId::generate();
    fixture_store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let original = fixture_store
        .quarantine_run(quarantine_request(
            tenant_id.clone(),
            run_id,
            QuarantineId::generate(),
            JournalExpectation::empty(),
            RunQuarantineCause::OperatorPolicy,
            "migration11.v1_preserved",
            b"migration eleven v1 evidence",
        ))
        .await
        .unwrap()
        .quarantine()
        .clone();
    assert!(original.request().expected_fence().is_none());
    fixture_store.close().await;

    let fixture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    remove_fenced_recovery_quarantines(&fixture_pool).await;
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&fixture_pool)
            .await
            .unwrap(),
        10
    );
    fixture_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 11 must upgrade the exact v10 fixture");
    let upgraded = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .unwrap();
    assert_eq!(
        upgraded
            .load_run_quarantine(&tenant_id, run_id)
            .await
            .expect("v1 evidence must retain its exact digest"),
        original
    );
    let verification = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    let fence_columns = query_as::<_, (Option<Uuid>, Option<i64>)>(
        "SELECT expected_fence_attempt_id, expected_fence_epoch \
         FROM stateknot.run_quarantines WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .fetch_one(&verification)
    .await
    .unwrap();
    assert_eq!(fence_columns, (None, None));
    let fence_constraint = query_scalar::<_, bool>(
        "SELECT EXISTS ( \
             SELECT 1 FROM pg_catalog.pg_constraint \
             WHERE conrelid = to_regclass('stateknot.run_quarantines') \
               AND conname = 'run_quarantines_fence_shape' \
               AND convalidated \
         )",
    )
    .fetch_one(&verification)
    .await
    .unwrap();
    assert!(fence_constraint);

    verification.close().await;
    upgraded.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_twelve_preserves_queue_age_and_installs_delayed_retry_guards() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v12_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .unwrap();

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .unwrap();
    let fixture_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .unwrap();
    let tenant_id = tenant("v12-delayed-retry");
    let pending_run = RunId::generate();
    fixture_store
        .admit_run(provenance(tenant_id.clone(), pending_run))
        .await
        .unwrap();
    let leased_run = RunId::generate();
    Box::pin(start_run_with_checkpoint(
        &fixture_store,
        &tenant_id,
        leased_run,
        1_460,
    ))
    .await;
    fixture_store
        .claim_lease(&tenant_id, leased_run, AttemptId::generate())
        .await
        .unwrap();
    let original_pending_ready_at = fixture_store
        .load_run(&tenant_id, pending_run)
        .await
        .unwrap()
        .scheduler_ready_at()
        .unwrap();
    let original_leased_ready_at = fixture_store
        .load_run(&tenant_id, leased_run)
        .await
        .unwrap()
        .scheduler_ready_at()
        .unwrap();
    fixture_store.close().await;

    let fixture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    remove_delayed_retry_wakeup(&fixture_pool).await;
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&fixture_pool)
            .await
            .unwrap(),
        11
    );
    fixture_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 12 must upgrade the exact v11 fixture");
    let upgraded = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("the upgraded v12 schema must pass exact verification");
    upgraded.verify_schema().await.unwrap();
    let pending = upgraded.load_run(&tenant_id, pending_run).await.unwrap();
    let leased = upgraded.load_run(&tenant_id, leased_run).await.unwrap();
    assert_eq!(
        pending.scheduler_ready_at(),
        Some(original_pending_ready_at)
    );
    assert_eq!(leased.scheduler_ready_at(), Some(original_leased_ready_at));
    assert_eq!(pending.scheduler_not_before(), None);
    assert_eq!(leased.scheduler_not_before(), None);

    let verification = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT count(*) FROM stateknot.runs WHERE scheduler_not_before IS NOT NULL",
        )
        .fetch_one(&verification)
        .await
        .unwrap(),
        0
    );
    let index_definition = query_scalar::<_, String>(
        "SELECT indexdef FROM pg_catalog.pg_indexes \
         WHERE schemaname = 'stateknot' AND indexname = 'runs_scheduler_ready'",
    )
    .fetch_one(&verification)
    .await
    .unwrap()
    .to_ascii_lowercase();
    assert!(index_definition.contains("scheduler_not_before"));
    assert!(index_definition.contains("lease_expires_at"));
    let delayed_retry_constraint = query_scalar::<_, bool>(
        "SELECT EXISTS ( \
             SELECT 1 FROM pg_catalog.pg_constraint \
             WHERE conrelid = to_regclass('stateknot.runs') \
               AND conname = 'runs_scheduler_not_before_shape' \
               AND convalidated \
         )",
    )
    .fetch_one(&verification)
    .await
    .unwrap();
    assert!(delayed_retry_constraint);
    assert!(
        query(
            "UPDATE stateknot.runs \
             SET scheduler_not_before = scheduler_ready_at - interval '1 microsecond' \
             WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(tenant_id.as_str())
        .bind(*pending_run.as_uuid())
        .execute(&verification)
        .await
        .is_err(),
        "the validated v12 shape must reject a gate before queue admission"
    );
    assert!(
        query(
            "UPDATE stateknot.runs \
             SET scheduler_not_before = scheduler_ready_at \
             WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(tenant_id.as_str())
        .bind(*leased_run.as_uuid())
        .execute(&verification)
        .await
        .is_err(),
        "the validated v12 shape must reject a delayed gate beside a lease"
    );
    query("ALTER TABLE stateknot.runs DROP CONSTRAINT runs_scheduler_not_before_shape")
        .execute(&verification)
        .await
        .unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(
        "UPDATE stateknot.runs \
         SET scheduler_not_before = scheduler_ready_at - interval '1 microsecond' \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*pending_run.as_uuid())
    .execute(&verification)
    .await
    .unwrap();
    assert!(matches!(
        upgraded.load_run(&tenant_id, pending_run).await,
        Err(StoreError::CorruptData { .. })
    ));

    verification.close().await;
    upgraded.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_registry_is_tenant_scoped_immutable_and_fully_verified() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("graph-registry");
    let other_tenant_id = tenant("graph-registry-other");
    let graph = checkpoint_compiled_graph();

    let registered = store
        .register_graph_definition(tenant_id.clone(), graph.clone())
        .await
        .unwrap();
    assert!(matches!(
        registered,
        GraphDefinitionRegistrationOutcome::Registered(_)
    ));
    assert_eq!(registered.definition().tenant_id(), &tenant_id);
    assert_eq!(registered.definition().graph(), &graph);

    let idempotent = store
        .register_graph_definition(tenant_id.clone(), graph.clone())
        .await
        .unwrap();
    assert!(matches!(
        idempotent,
        GraphDefinitionRegistrationOutcome::Idempotent(_)
    ));
    assert_eq!(idempotent.definition(), registered.definition());
    assert_eq!(
        store
            .load_graph_definition(&tenant_id, &graph.reference())
            .await
            .unwrap(),
        registered.definition().clone()
    );
    assert!(matches!(
        store
            .load_graph_definition(&other_tenant_id, &graph.reference())
            .await,
        Err(StoreError::GraphDefinitionNotFound)
    ));

    let conflicting = build_checkpoint_compiled_graph(63);
    assert_eq!(conflicting.identity(), graph.identity());
    assert_ne!(conflicting.definition_digest(), graph.definition_digest());
    assert!(matches!(
        store
            .register_graph_definition(tenant_id.clone(), conflicting)
            .await,
        Err(StoreError::GraphDefinitionConflict)
    ));

    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    query(
        "UPDATE stateknot.graph_definitions \
         SET definition_bytes = definition_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1",
    )
    .bind(tenant_id.as_str())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store
            .load_graph_definition(&tenant_id, &graph.reference())
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_graph_registrations_choose_one_immutable_version() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("graph-registry-race");
    let first = checkpoint_compiled_graph();
    let second = build_checkpoint_compiled_graph(63);
    let mut tasks = Vec::new();
    for index in 0..24 {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        let graph = if index % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        tasks.push(tokio::spawn(async move {
            store.register_graph_definition(tenant_id, graph).await
        }));
    }

    let mut registered = 0_usize;
    let mut idempotent = 0_usize;
    let mut conflicts = 0_usize;
    for task in tasks {
        match task.await.unwrap() {
            Ok(GraphDefinitionRegistrationOutcome::Registered(_)) => registered += 1,
            Ok(GraphDefinitionRegistrationOutcome::Idempotent(_)) => idempotent += 1,
            Err(StoreError::GraphDefinitionConflict) => conflicts += 1,
            result => panic!("unexpected graph registration race result: {result:?}"),
        }
    }
    assert_eq!(registered, 1);
    assert_eq!(idempotent, 11);
    assert_eq!(conflicts, 12);

    let first_loaded = store
        .load_graph_definition(&tenant_id, &first.reference())
        .await;
    let second_loaded = store
        .load_graph_definition(&tenant_id, &second.reference())
        .await;
    assert!(matches!(
        (&first_loaded, &second_loaded),
        (Ok(_), Err(StoreError::GraphDefinitionNotFound))
            | (Err(StoreError::GraphDefinitionNotFound), Ok(_))
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claimed_recovery_revalidates_and_quarantines_a_missing_pinned_graph() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("pinned-graph-recovery");
    let run_id = RunId::generate();
    let initial = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_470)).await;
    let lease = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .clone();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(initial.event().head()),
        Digest::sha256(b"pinned graph recovery evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(lease.fence().clone(), context)
        .await
        .unwrap();

    let definition = recovery.load_pinned_graph().await.unwrap();
    assert_eq!(definition.tenant_id(), &tenant_id);
    assert_eq!(definition.graph(), &checkpoint_compiled_graph());
    recovery.plan_ready_nodes().await.unwrap();

    let database_url = std::env::var(DATABASE_URL_ENV).unwrap();
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    query("DELETE FROM stateknot.graph_definitions WHERE tenant_id = $1")
        .bind(tenant_id.as_str())
        .execute(&administration)
        .await
        .unwrap();
    assert!(matches!(
        recovery.load_pinned_graph().await,
        Err(StoreError::RunQuarantined)
    ));
    assert!(
        store
            .load_run(&tenant_id, run_id)
            .await
            .unwrap()
            .is_quarantined()
    );

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noninitial_replay_recomputes_committed_state_with_bounded_memory() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("noninitial-replay-valid");
    let run_id = RunId::generate();
    let initial = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_500)).await;
    let fence = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let (result_heads, result_journal_head) =
        commit_ready_results(&store, initial.checkpoint(), &fence, 1_501).await;
    let successor = CheckpointWrite::successor(
        CheckpointId::generate(),
        initial.checkpoint(),
        initial.checkpoint().state().clone(),
        ready_node(2),
    )
    .unwrap();
    let barrier = CheckpointBarrier::new(initial.checkpoint(), successor, result_heads).unwrap();
    let committed = store
        .append_worker_barrier(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(result_journal_head),
                fence.clone(),
                1_502,
            ),
            RunProjection::unchanged(),
            barrier,
        )
        .await
        .unwrap();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(committed.event().head()),
        Digest::sha256(b"noninitial replay valid evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence, context)
        .await
        .unwrap();

    assert!(matches!(
        recovery
            .validate_noninitial_replay(
                &AcceptGraphSchemas,
                &IntegrationGraphReducer::new(),
                GraphReplayLimits::new(1).unwrap(),
            )
            .await,
        Err(StoreError::GraphReplayResourceLimit)
    ));
    assert!(matches!(
        store.load_run_quarantine(&tenant_id, run_id).await,
        Err(StoreError::RunQuarantineNotFound)
    ));

    let report = recovery
        .validate_noninitial_replay(
            &AcceptGraphSchemas,
            &IntegrationGraphReducer::new(),
            GraphReplayLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(report.checkpoints_validated(), 2);
    assert_eq!(report.barriers_replayed(), 1);
    assert_eq!(report.results_replayed(), 1);
    assert!(report.maximum_barrier_result_bytes() > 1);
    recovery.revalidate().await.unwrap();
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noninitial_replay_quarantines_a_semantically_divergent_successor() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("noninitial-replay-divergence");
    let run_id = RunId::generate();
    let initial = Box::pin(start_run_with_checkpoint(&store, &tenant_id, run_id, 1_510)).await;
    let fence = store
        .claim_lease(&tenant_id, run_id, AttemptId::generate())
        .await
        .unwrap()
        .lease()
        .fence()
        .clone();
    let (result_heads, result_journal_head) =
        commit_ready_results(&store, initial.checkpoint(), &fence, 1_511).await;
    let divergent_successor = CheckpointWrite::successor(
        CheckpointId::generate(),
        initial.checkpoint(),
        checkpoint_state(initial.checkpoint().graph(), 1),
        ready_node(2),
    )
    .unwrap();
    let barrier =
        CheckpointBarrier::new(initial.checkpoint(), divergent_successor, result_heads).unwrap();
    let committed = store
        .append_worker_barrier(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(result_journal_head),
                fence.clone(),
                1_512,
            ),
            RunProjection::unchanged(),
            barrier,
        )
        .await
        .unwrap();
    let context = CorruptionQuarantineContext::new(
        tenant_id.clone(),
        run_id,
        QuarantineId::generate(),
        JournalExpectation::exact(committed.event().head()),
        Digest::sha256(b"noninitial replay divergence evidence"),
    )
    .unwrap();
    let recovery = store
        .begin_claimed_run_recovery(fence, context)
        .await
        .unwrap();

    assert!(matches!(
        recovery
            .validate_noninitial_replay(
                &AcceptGraphSchemas,
                &IntegrationGraphReducer::new(),
                GraphReplayLimits::default(),
            )
            .await,
        Err(StoreError::RunQuarantined)
    ));
    let quarantined = store.load_run(&tenant_id, run_id).await.unwrap();
    assert!(quarantined.is_quarantined());
    assert!(quarantined.lease().is_none());
    assert!(store.load_run_quarantine(&tenant_id, run_id).await.is_ok());
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_thirteen_installs_an_exact_immutable_graph_registry() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v13_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .unwrap();

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .unwrap();
    let fixture_store =
        PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
            .await
            .unwrap();
    let tenant_id = tenant("v13-graph-registry");
    let existing_run = RunId::generate();
    fixture_store
        .admit_run(provenance(tenant_id.clone(), existing_run))
        .await
        .unwrap();
    fixture_store.close().await;

    let fixture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    remove_graph_registry(&fixture_pool).await;
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&fixture_pool)
            .await
            .unwrap(),
        12
    );
    fixture_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 13 must upgrade the exact v12 fixture");
    let upgraded = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("the upgraded v13 schema must pass exact verification");
    upgraded.verify_schema().await.unwrap();
    upgraded.load_run(&tenant_id, existing_run).await.unwrap();
    assert!(matches!(
        upgraded
            .register_graph_definition(tenant_id.clone(), checkpoint_compiled_graph())
            .await
            .unwrap(),
        GraphDefinitionRegistrationOutcome::Registered(_)
    ));

    let verification = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    let digest_index = query_scalar::<_, String>(
        "SELECT indexdef FROM pg_catalog.pg_indexes \
         WHERE schemaname = 'stateknot' \
           AND indexname = 'graph_definitions_digest_lookup'",
    )
    .fetch_one(&verification)
    .await
    .unwrap()
    .to_ascii_lowercase();
    assert!(digest_index.contains("tenant_id"));
    assert!(digest_index.contains("definition_digest"));
    assert!(
        query(
            "INSERT INTO stateknot.graph_definitions ( \
                 tenant_id, owner_issuer, owner_subject, graph_name, graph_version, \
                 definition_digest, definition_bytes \
             ) VALUES ( \
                 'invalid tenant', 'https://issuer.example.com', 'subject', \
                 'graph', '1.0.0', decode(repeat('00', 32), 'hex'), '{}'::text::bytea \
             )",
        )
        .execute(&verification)
        .await
        .is_err(),
        "the validated tenant constraint must reject crossed key grammar"
    );
    query(
        "ALTER TABLE stateknot.graph_definitions \
         DROP CONSTRAINT graph_definitions_bytes_bounded",
    )
    .execute(&verification)
    .await
    .unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(
        "UPDATE stateknot.graph_definitions \
         SET definition_bytes = definition_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1",
    )
    .bind(tenant_id.as_str())
    .execute(&verification)
    .await
    .unwrap();
    assert!(matches!(
        upgraded
            .load_graph_definition(&tenant_id, &checkpoint_graph())
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    verification.close().await;
    upgraded.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduler_fairness_policy_is_immutable_and_reservations_are_lost_ack_safe() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let shard_id = scheduler_shard("fairness-idempotency");
    let registration = SchedulerFairnessPolicyRegistration::new(
        shard_id.clone(),
        br#"{"algorithm":"test_v1","weights":[2,1]}"#,
        3,
    )
    .unwrap();
    let registered = store
        .register_scheduler_fairness_policy(registration.clone())
        .await
        .unwrap();
    assert!(matches!(
        registered,
        SchedulerFairnessPolicyRegistrationOutcome::Registered(_)
    ));
    assert_eq!(
        store
            .register_scheduler_fairness_policy(registration.clone())
            .await
            .unwrap()
            .policy(),
        registered.policy()
    );
    assert!(matches!(
        store
            .register_scheduler_fairness_policy(
                SchedulerFairnessPolicyRegistration::new(
                    shard_id.clone(),
                    br#"{"algorithm":"test_v2","weights":[1,2]}"#,
                    3,
                )
                .unwrap(),
            )
            .await,
        Err(StoreError::SchedulerFairnessPolicyConflict)
    ));

    let reservation_id = SchedulerReservationId::generate();
    let first = store
        .reserve_scheduler_fairness_slot(&shard_id, registration.policy_digest(), reservation_id)
        .await
        .unwrap();
    let recovered = store
        .reserve_scheduler_fairness_slot(&shard_id, registration.policy_digest(), reservation_id)
        .await
        .unwrap();
    assert_eq!(first, recovered);
    assert_eq!(first.sequence(), 0);
    assert_eq!(first.slot(), 0);
    assert!(matches!(
        store
            .reserve_scheduler_fairness_slot(
                &shard_id,
                Digest::sha256(b"wrong fairness policy"),
                reservation_id,
            )
            .await,
        Err(StoreError::SchedulerFairnessReservationConflict)
    ));
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn concurrent_scheduler_replicas_share_one_linear_fairness_cursor() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let shard_id = scheduler_shard("fairness-concurrency");
    let registration = SchedulerFairnessPolicyRegistration::new(
        shard_id.clone(),
        br#"{"algorithm":"test_v1","weights":[3,1,1]}"#,
        5,
    )
    .unwrap();
    store
        .register_scheduler_fairness_policy(registration.clone())
        .await
        .unwrap();
    let policy_digest = registration.policy_digest();

    let stable_reservation_id = SchedulerReservationId::generate();
    let mut duplicate_tasks = Vec::new();
    for _ in 0..24 {
        let store = store.clone();
        let shard_id = shard_id.clone();
        duplicate_tasks.push(tokio::spawn(async move {
            store
                .reserve_scheduler_fairness_slot(&shard_id, policy_digest, stable_reservation_id)
                .await
        }));
    }
    let mut duplicate_results = Vec::new();
    for task in duplicate_tasks {
        duplicate_results.push(task.await.unwrap().unwrap());
    }
    assert!(duplicate_results.windows(2).all(|pair| pair[0] == pair[1]));
    assert_eq!(duplicate_results[0].sequence(), 0);

    let mut unique_tasks = Vec::new();
    for _ in 0..40 {
        let store = store.clone();
        let shard_id = shard_id.clone();
        unique_tasks.push(tokio::spawn(async move {
            store
                .reserve_scheduler_fairness_slot(
                    &shard_id,
                    policy_digest,
                    SchedulerReservationId::generate(),
                )
                .await
        }));
    }
    let mut reservations = Vec::new();
    for task in unique_tasks {
        reservations.push(task.await.unwrap().unwrap());
    }
    reservations.sort_by_key(stateknot_store_postgres::SchedulerFairnessReservation::sequence);
    for (offset, reservation) in reservations.iter().enumerate() {
        let sequence = u64::try_from(offset + 1).unwrap();
        assert_eq!(reservation.sequence(), sequence);
        assert_eq!(u64::from(reservation.slot()), sequence % 5);
    }
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduler_reservation_retention_is_database_timed_bounded_and_cursor_neutral() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    assert!(matches!(
        SchedulerFairnessRetentionPolicy::new(Duration::from_secs(60), 1),
        Err(StoreError::InvalidSchedulerFairnessRetention)
    ));
    assert!(matches!(
        SchedulerFairnessRetentionPolicy::new(Duration::from_secs(60 * 60), 0),
        Err(StoreError::InvalidSchedulerFairnessRetention)
    ));
    let shard_id = scheduler_shard("fairness-retention");
    let registration = SchedulerFairnessPolicyRegistration::new(
        shard_id.clone(),
        br#"{"algorithm":"test_v1","weights":[1,1]}"#,
        2,
    )
    .unwrap();
    store
        .register_scheduler_fairness_policy(registration.clone())
        .await
        .unwrap();
    let mut reservations = Vec::new();
    for _ in 0..3 {
        reservations.push(
            store
                .reserve_scheduler_fairness_slot(
                    &shard_id,
                    registration.policy_digest(),
                    SchedulerReservationId::generate(),
                )
                .await
                .unwrap(),
        );
    }

    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    query(
        "UPDATE stateknot.scheduler_fairness_reservations \
         SET reserved_at = clock_timestamp() - interval '2 hours' \
         WHERE reservation_id = ANY($1)",
    )
    .bind(
        reservations[..2]
            .iter()
            .map(|reservation| *reservation.reservation_id().as_uuid())
            .collect::<Vec<_>>(),
    )
    .execute(&administration)
    .await
    .unwrap();
    let policy = SchedulerFairnessRetentionPolicy::new(Duration::from_secs(60 * 60), 1).unwrap();
    let first = store
        .prune_scheduler_fairness_reservations(policy)
        .await
        .unwrap();
    let second = store
        .prune_scheduler_fairness_reservations(policy)
        .await
        .unwrap();
    let empty = store
        .prune_scheduler_fairness_reservations(policy)
        .await
        .unwrap();
    assert_eq!(
        (first.deleted(), second.deleted(), empty.deleted()),
        (1, 1, 0)
    );
    assert!(first.cutoff() < first.observed_at());
    assert_eq!(
        query_scalar::<_, i64>(
            "SELECT count(*) FROM stateknot.scheduler_fairness_reservations WHERE shard_id = $1",
        )
        .bind(shard_id.as_str())
        .fetch_one(&administration)
        .await
        .unwrap(),
        1
    );
    let next = store
        .reserve_scheduler_fairness_slot(
            &shard_id,
            registration.policy_digest(),
            SchedulerReservationId::generate(),
        )
        .await
        .unwrap();
    assert_eq!(next.sequence(), 3);
    assert_eq!(next.slot(), 1);
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_fourteen_installs_verified_distributed_fairness_state() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v14_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .unwrap();

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .unwrap();
    let fixture_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    remove_scheduler_fairness(&fixture_pool).await;
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&fixture_pool)
            .await
            .unwrap(),
        13
    );
    fixture_pool.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 14 must upgrade the exact v13 fixture");
    let upgraded = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("the upgraded v14 schema must pass exact verification");
    upgraded.verify_schema().await.unwrap();
    let shard_id = scheduler_shard("v14-fairness");
    let registration = SchedulerFairnessPolicyRegistration::new(
        shard_id.clone(),
        br#"{"algorithm":"test_v1","weights":[1]}"#,
        1,
    )
    .unwrap();
    upgraded
        .register_scheduler_fairness_policy(registration)
        .await
        .unwrap();

    let verification = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    query(
        "ALTER TABLE stateknot.scheduler_fairness_shards \
         DROP CONSTRAINT scheduler_fairness_shards_policy_bounded",
    )
    .execute(&verification)
    .await
    .unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    query(
        "UPDATE stateknot.scheduler_fairness_shards \
         SET policy_bytes = policy_bytes || convert_to(' ', 'UTF8') \
         WHERE shard_id = $1",
    )
    .bind(shard_id.as_str())
    .execute(&verification)
    .await
    .unwrap();
    assert!(matches!(
        upgraded.load_scheduler_fairness_policy(&shard_id).await,
        Err(StoreError::CorruptData { .. })
    ));

    verification.close().await;
    upgraded.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn atomic_agent_admission_is_scheduler_ready_retry_exact_and_tamper_evident() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("atomic-agent-admission");
    let run_id = RunId::generate();
    store
        .register_graph_definition(tenant_id.clone(), checkpoint_compiled_graph())
        .await
        .unwrap();
    let (intent, append, checkpoint) = agent_admission_fixture(tenant_id.clone(), run_id);

    let committed = Box::pin(store.admit_agent_run(
        intent.clone(),
        append.clone(),
        checkpoint.clone(),
        &AcceptGraphSchemas,
    ))
    .await
    .expect("the complete Agent admission must commit atomically");
    let AgentAdmissionCommitOutcome::Committed(stored) = committed else {
        panic!("first Agent admission must be a physical commit")
    };
    assert_eq!(stored.admission().intent(), &intent);
    assert_eq!(stored.run().lifecycle().status(), RunStatus::Active);
    assert_eq!(stored.event().sequence(), JournalSequence::FIRST);
    assert_eq!(stored.checkpoint().superstep(), Superstep::INITIAL);
    assert_eq!(stored.checkpoint().journal_head(), &stored.event().head());
    assert_eq!(
        store
            .load_agent_admission(&tenant_id, run_id)
            .await
            .unwrap()
            .admission(),
        stored.admission()
    );
    let page = store
        .load_runnable_run_page(&tenant_id, None, RunnableRunPageSize::new(8).unwrap())
        .await
        .unwrap();
    assert!(page.records().iter().any(|candidate| {
        candidate.run().lifecycle().provenance().run_id() == run_id
            && candidate.run().checkpoint().is_some()
    }));

    let retry = Box::pin(store.admit_agent_run(
        intent.clone(),
        append.clone(),
        checkpoint.clone(),
        &AcceptGraphSchemas,
    ))
    .await
    .expect("an exact lost-ack retry must recover durable evidence");
    assert!(matches!(retry, AgentAdmissionCommitOutcome::Idempotent(_)));

    let conflicting_event = JournalEventIntent::control_plane(
        tenant_id.clone(),
        run_id,
        EventId::generate(),
        append.intent().payload().clone(),
    )
    .unwrap();
    let conflicting_append =
        JournalAppend::new(JournalExpectation::empty(), conflicting_event).unwrap();
    assert!(matches!(
        Box::pin(store.admit_agent_run(
            intent,
            conflicting_append,
            checkpoint,
            &AcceptGraphSchemas,
        ))
        .await,
        Err(StoreError::AgentAdmissionConflict)
    ));

    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    query(
        "UPDATE stateknot.agent_admissions \
         SET admission_bytes = admission_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store.load_agent_admission(&tenant_id, run_id).await,
        Err(StoreError::CorruptData { .. })
    ));
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_agent_admissions_converge_and_late_failure_rolls_back_all_rows() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("concurrent-agent-admission");
    store
        .register_graph_definition(tenant_id.clone(), checkpoint_compiled_graph())
        .await
        .unwrap();
    let run_id = RunId::generate();
    let (intent, append, checkpoint) = agent_admission_fixture(tenant_id.clone(), run_id);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..2 {
        let store = store.clone();
        let intent = intent.clone();
        let append = append.clone();
        let checkpoint = checkpoint.clone();
        tasks.spawn(async move {
            Box::pin(store.admit_agent_run(intent, append, checkpoint, &AcceptGraphSchemas)).await
        });
    }
    let mut committed = 0;
    let mut idempotent = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap().unwrap() {
            AgentAdmissionCommitOutcome::Committed(_) => committed += 1,
            AgentAdmissionCommitOutcome::Idempotent(_) => idempotent += 1,
            _ => panic!("unsupported Agent admission outcome"),
        }
    }
    assert_eq!((committed, idempotent), (1, 1));

    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    query(
        "CREATE FUNCTION stateknot.reject_agent_admission_test() RETURNS trigger \
         LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''injected Agent admission failure''; END'",
    )
    .execute(&administration)
    .await
    .unwrap();
    query(
        "CREATE TRIGGER reject_agent_admission_test \
         BEFORE INSERT ON stateknot.agent_admissions \
         FOR EACH ROW EXECUTE FUNCTION stateknot.reject_agent_admission_test()",
    )
    .execute(&administration)
    .await
    .unwrap();

    let rollback_run = RunId::generate();
    let (rollback_intent, rollback_append, rollback_checkpoint) =
        agent_admission_fixture(tenant_id.clone(), rollback_run);
    assert!(matches!(
        Box::pin(store.admit_agent_run(
            rollback_intent,
            rollback_append,
            rollback_checkpoint,
            &AcceptGraphSchemas,
        ))
        .await,
        Err(StoreError::Database { .. })
    ));
    for table in ["runs", "run_events", "run_checkpoints", "agent_admissions"] {
        let count = query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM stateknot.{table} WHERE tenant_id = $1 AND run_id = $2"
        ))
        .bind(tenant_id.as_str())
        .bind(*rollback_run.as_uuid())
        .fetch_one(&administration)
        .await
        .unwrap();
        assert_eq!(count, 0, "{table} must roll back with the failed admission");
    }
    query("DROP TRIGGER reject_agent_admission_test ON stateknot.agent_admissions")
        .execute(&administration)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.reject_agent_admission_test()")
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn durable_agent_submission_keys_converge_conflict_and_roll_back_atomically() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("durable-agent-submission-key");
    store
        .register_graph_definition(tenant_id.clone(), checkpoint_compiled_graph())
        .await
        .unwrap();

    let key = AgentSubmissionKey::new("request_provider_agent_submission_key_01").unwrap();
    let original_run = RunId::generate();
    let (intent, append, checkpoint) = agent_admission_fixture(tenant_id.clone(), original_run);
    let committed = Box::pin(store.submit_agent_run(
        &key,
        intent.clone(),
        append.clone(),
        checkpoint.clone(),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap();
    assert!(matches!(
        committed,
        AgentSubmissionCommitOutcome::Committed(_)
    ));
    assert_eq!(
        committed
            .stored()
            .admission()
            .admission()
            .intent()
            .provenance()
            .run_id(),
        original_run
    );
    assert_eq!(committed.stored().key_digest(), key.digest_for(&tenant_id));
    assert_eq!(
        committed.stored().created_at(),
        committed.stored().admission().admission().admitted_at()
    );

    let (retry_intent, retry_append, retry_checkpoint) = agent_submission_retry_fixture(
        tenant_id.clone(),
        RunId::generate(),
        &intent,
        &checkpoint,
        intent.request().clone(),
    );
    let retry = Box::pin(store.submit_agent_run(
        &key,
        retry_intent,
        retry_append,
        retry_checkpoint,
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap();
    assert!(matches!(retry, AgentSubmissionCommitOutcome::Idempotent(_)));
    assert_eq!(
        retry
            .stored()
            .admission()
            .admission()
            .intent()
            .provenance()
            .run_id(),
        original_run
    );
    assert_eq!(
        store
            .load_agent_submission(&tenant_id, &key)
            .await
            .unwrap()
            .admission()
            .admission()
            .intent()
            .provenance()
            .run_id(),
        original_run
    );

    let changed_request = AgentRequest::new(
        intent.request().input_schema().clone(),
        BoundedJson::try_from_value(json!({"changed": true})).unwrap(),
        intent.request().budget_limits().clone(),
    );
    let (changed_intent, changed_append, changed_checkpoint) = agent_submission_retry_fixture(
        tenant_id.clone(),
        RunId::generate(),
        &intent,
        &checkpoint,
        changed_request,
    );
    assert!(matches!(
        Box::pin(store.submit_agent_run(
            &key,
            changed_intent,
            changed_append,
            changed_checkpoint,
            &AcceptGraphSchemas,
        ))
        .await,
        Err(StoreError::AgentSubmissionConflict)
    ));

    let second_key = AgentSubmissionKey::new("request_provider_second_key_same_run_01").unwrap();
    assert!(matches!(
        Box::pin(store.submit_agent_run(
            &second_key,
            intent.clone(),
            append.clone(),
            checkpoint.clone(),
            &AcceptGraphSchemas,
        ))
        .await,
        Err(StoreError::AgentSubmissionConflict)
    ));
    assert!(matches!(
        store.load_agent_submission(&tenant_id, &second_key).await,
        Err(StoreError::AgentSubmissionNotFound)
    ));

    let concurrent_key =
        AgentSubmissionKey::new("request_provider_concurrent_submission_key_01").unwrap();
    let (template_intent, _, template_checkpoint) =
        agent_admission_fixture(tenant_id.clone(), RunId::generate());
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let store = store.clone();
        let key = concurrent_key.clone();
        let candidate = agent_submission_retry_fixture(
            tenant_id.clone(),
            RunId::generate(),
            &template_intent,
            &template_checkpoint,
            template_intent.request().clone(),
        );
        tasks.spawn(async move {
            Box::pin(store.submit_agent_run(
                &key,
                candidate.0,
                candidate.1,
                candidate.2,
                &AcceptGraphSchemas,
            ))
            .await
        });
    }
    let mut physical_commits = 0;
    let mut idempotent_retries = 0;
    let mut selected_run = None;
    while let Some(result) = tasks.join_next().await {
        let outcome = result.unwrap().unwrap();
        match &outcome {
            AgentSubmissionCommitOutcome::Committed(_) => physical_commits += 1,
            AgentSubmissionCommitOutcome::Idempotent(_) => idempotent_retries += 1,
            _ => panic!("unsupported Agent submission outcome"),
        }
        let run_id = outcome
            .stored()
            .admission()
            .admission()
            .intent()
            .provenance()
            .run_id();
        assert!(selected_run.is_none_or(|selected| selected == run_id));
        selected_run = Some(run_id);
    }
    assert_eq!((physical_commits, idempotent_retries), (1, 23));

    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var(DATABASE_URL_ENV).unwrap())
        .await
        .unwrap();
    let stored_key_digest = query_scalar::<_, Vec<u8>>(
        "SELECT key_digest FROM stateknot.agent_submission_keys \
         WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant_id.as_str())
    .bind(*original_run.as_uuid())
    .fetch_one(&administration)
    .await
    .unwrap();
    assert_eq!(stored_key_digest, key.digest_for(&tenant_id).as_bytes());
    assert_ne!(stored_key_digest, key.as_str().as_bytes());

    query(
        "DROP TRIGGER IF EXISTS reject_agent_submission_key_test \
         ON stateknot.agent_submission_keys",
    )
    .execute(&administration)
    .await
    .unwrap();
    query(
        "CREATE OR REPLACE FUNCTION stateknot.reject_agent_submission_key_test() RETURNS trigger \
         LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''injected Agent submission-key failure''; END'",
    )
    .execute(&administration)
    .await
    .unwrap();
    query(
        "CREATE TRIGGER reject_agent_submission_key_test \
         BEFORE INSERT ON stateknot.agent_submission_keys \
         FOR EACH ROW EXECUTE FUNCTION stateknot.reject_agent_submission_key_test()",
    )
    .execute(&administration)
    .await
    .unwrap();

    let rollback_key =
        AgentSubmissionKey::new("request_provider_rollback_submission_key_01").unwrap();
    let rollback_run = RunId::generate();
    let (rollback_intent, rollback_append, rollback_checkpoint) =
        agent_admission_fixture(tenant_id.clone(), rollback_run);
    assert!(matches!(
        Box::pin(store.submit_agent_run(
            &rollback_key,
            rollback_intent,
            rollback_append,
            rollback_checkpoint,
            &AcceptGraphSchemas,
        ))
        .await,
        Err(StoreError::Database { .. })
    ));
    for table in [
        "runs",
        "run_events",
        "run_checkpoints",
        "agent_admissions",
        "agent_submission_keys",
    ] {
        let count = query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM stateknot.{table} WHERE tenant_id = $1 AND run_id = $2"
        ))
        .bind(tenant_id.as_str())
        .bind(*rollback_run.as_uuid())
        .fetch_one(&administration)
        .await
        .unwrap();
        assert_eq!(count, 0, "{table} must roll back with the failed mapping");
    }
    query("DROP TRIGGER reject_agent_submission_key_test ON stateknot.agent_submission_keys")
        .execute(&administration)
        .await
        .unwrap();
    query("DROP FUNCTION stateknot.reject_agent_submission_key_test()")
        .execute(&administration)
        .await
        .unwrap();

    query(
        "UPDATE stateknot.agent_submission_keys \
         SET submission_digest = decode(repeat('00', 32), 'hex') \
         WHERE tenant_id = $1 AND key_digest = $2",
    )
    .bind(tenant_id.as_str())
    .bind(key.digest_for(&tenant_id).as_bytes())
    .execute(&administration)
    .await
    .unwrap();
    assert!(matches!(
        store.load_agent_submission(&tenant_id, &key).await,
        Err(StoreError::CorruptData { .. })
    ));

    administration.close().await;
    store.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn migration_fifteen_installs_atomic_agent_admissions_during_full_upgrade() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v15_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .unwrap();
    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .unwrap();
    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    remove_agent_admissions(&fixture).await;
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        14
    );
    fixture.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("the current migrator must upgrade the exact v14 fixture through migration 16");
    let upgraded = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("the fully upgraded schema must pass exact verification");
    upgraded.verify_schema().await.unwrap();
    let tenant_id = tenant("v15-agent-admission");
    let run_id = RunId::generate();
    upgraded
        .register_graph_definition(tenant_id.clone(), checkpoint_compiled_graph())
        .await
        .unwrap();
    let (intent, append, checkpoint) = agent_admission_fixture(tenant_id.clone(), run_id);
    Box::pin(upgraded.admit_agent_run(intent, append, checkpoint, &AcceptGraphSchemas))
        .await
        .unwrap();
    upgraded
        .load_agent_admission(&tenant_id, run_id)
        .await
        .unwrap();

    let verification = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    query(
        "ALTER TABLE stateknot.agent_admissions \
         DROP CONSTRAINT agent_admissions_bytes_bounded",
    )
    .execute(&verification)
    .await
    .unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    verification.close().await;
    upgraded.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn migration_sixteen_upgrades_existing_admissions_and_verifies_submission_keys() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let database_url = match std::env::var(DATABASE_URL_ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) if std::env::var_os(REQUIRE_DATABASE_ENV).is_some() => {
            panic!("mandatory PostgreSQL test URL is missing")
        }
        Err(std::env::VarError::NotPresent) => return,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("PostgreSQL test URL must be valid Unicode")
        }
    };
    let database_name = format!(
        "stateknot_v16_upgrade_{}",
        RunId::generate().to_string().replace('-', "")
    );
    let administration_url = database_url_with_name(&database_url, "postgres");
    let isolated_url = database_url_with_name(&database_url, &database_name);
    let administration = PgPoolOptions::new()
        .max_connections(1)
        .connect(&administration_url)
        .await
        .unwrap();
    query(&format!("CREATE DATABASE {database_name}"))
        .execute(&administration)
        .await
        .unwrap();
    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .unwrap();

    let legacy_store = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .unwrap();
    let tenant_id = tenant("v16-existing-agent-admission");
    let run_id = RunId::generate();
    legacy_store
        .register_graph_definition(tenant_id.clone(), checkpoint_compiled_graph())
        .await
        .unwrap();
    let (intent, append, checkpoint) = agent_admission_fixture(tenant_id.clone(), run_id);
    Box::pin(legacy_store.admit_agent_run(
        intent.clone(),
        append.clone(),
        checkpoint.clone(),
        &AcceptGraphSchemas,
    ))
    .await
    .unwrap();
    legacy_store.close().await;

    let fixture = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    remove_agent_submission_keys(&fixture).await;
    assert_eq!(
        query_scalar::<_, i64>("SELECT max(version) FROM _sqlx_migrations")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        15
    );
    assert_eq!(
        query_scalar::<_, i64>("SELECT count(*) FROM stateknot.agent_admissions")
            .fetch_one(&fixture)
            .await
            .unwrap(),
        1
    );
    fixture.close().await;

    PostgresStore::migrate_database(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("migration 16 must upgrade a populated exact v15 fixture");
    let upgraded = PostgresStore::connect(&isolated_url, test_options(Duration::from_secs(30)))
        .await
        .expect("the upgraded v16 schema must pass exact verification");
    upgraded.verify_schema().await.unwrap();
    let key = AgentSubmissionKey::new("request_v16_existing_agent_admission_01").unwrap();
    let mapped =
        Box::pin(upgraded.submit_agent_run(&key, intent, append, checkpoint, &AcceptGraphSchemas))
            .await
            .expect("migration 16 must map an exact retained v15 admission");
    assert!(matches!(mapped, AgentSubmissionCommitOutcome::Committed(_)));
    assert_eq!(
        upgraded
            .load_agent_submission(&tenant_id, &key)
            .await
            .unwrap()
            .admission()
            .admission()
            .intent()
            .provenance()
            .run_id(),
        run_id
    );

    let verification = PgPoolOptions::new()
        .max_connections(1)
        .connect(&isolated_url)
        .await
        .unwrap();
    query(
        "ALTER TABLE stateknot.agent_submission_keys \
         DROP CONSTRAINT agent_submission_keys_digest_lengths",
    )
    .execute(&verification)
    .await
    .unwrap();
    assert!(matches!(
        upgraded.verify_schema().await,
        Err(StoreError::IncompleteSchema)
    ));
    verification.close().await;
    upgraded.close().await;
    query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
        .execute(&administration)
        .await
        .unwrap();
    administration.close().await;
}

async fn corrupt_checkpoint_bytes(
    administration: &PgPool,
    tenant_id: &TenantId,
    run_id: RunId,
    checkpoint_id: CheckpointId,
) {
    query(
        "UPDATE stateknot.run_checkpoints \
         SET checkpoint_bytes = checkpoint_bytes || convert_to(' ', 'UTF8') \
         WHERE tenant_id = $1 AND run_id = $2 AND checkpoint_id = $3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
    .bind(*checkpoint_id.as_uuid())
    .execute(administration)
    .await
    .unwrap();
}
