// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Real `PostgreSQL` migration, transaction, idempotency, and fencing tests.

use std::{borrow::Cow, time::Duration};

use serde_json::json;
use sqlx_core::{
    migrate::{Migration, MigrationType, Migrator},
    query::query,
};
use sqlx_postgres::PgPoolOptions;
use stateknot_core::{
    AgentResultProvenance, AttemptId, BoundedJson, CapabilityIdentity, CapabilityName,
    CapabilityReference, Checkpoint, CheckpointHead, CheckpointId, CheckpointState,
    CheckpointWrite, Digest, DurationMillis, EventId, Failure, FailureCategory, FailureCode,
    FailureId, FailureMessage, FailureOrigin, GraphNamespace, GraphReference, InvocationId,
    IssuerId, JournalAppend, JournalEventIntent, JournalEventKind, JournalExpectation, JournalHead,
    JournalPayload, JournalSequence, ModelDescriptor, ModelError, ModelErrorPhase,
    ModelErrorProvenance, ModelInvocationIntent, ModelInvocationStatus, ModelInvocationTransition,
    ModelRequest, ModelResponse, NodeActivation, NodeControl, NodeId, NodeInvocationBinding,
    NodeInvocationBindings, NodeStateChange, NodeStateUpdate, PendingNodeResultIntent,
    PrincipalIdentity, ReadyNodes, RetryAdvice, RunCancellationRequest, RunId, RunStatus,
    RunTransition, SchemaId, SchemaReference, SubjectId, TenantId, ThreadId, Timestamp,
    ToolArtifacts, ToolDescriptor, ToolInput, ToolInvocation, ToolInvocationIntent,
    ToolInvocationStatus, ToolInvocationTransition, ToolResult, ToolResultProvenance, Version,
};
use stateknot_store_postgres::{
    AdmissionOutcome, AppendOutcome, CheckpointCommitOutcome, CheckpointLineagePageSize,
    JournalPageSize, LeaseClaimOutcome, LeaseReleaseOutcome, LeaseRenewalOutcome,
    ModelInvocationCommitOutcome, ModelInvocationHistoryPageSize, PendingNodeResultCommitOutcome,
    PostgresStore, PostgresStoreOptions, PostgresTransportSecurity, RunProjection, StoreError,
    ToolInvocationCommitOutcome, ToolInvocationHistoryPageSize,
};

const DATABASE_URL_ENV: &str = "STATEKNOT_TEST_DATABASE_URL";
const REQUIRE_DATABASE_ENV: &str = "STATEKNOT_REQUIRE_POSTGRES_TESTS";
static DATABASE_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn test_store() -> Option<PostgresStore> {
    test_store_with_lease_duration(Duration::from_secs(30)).await
}

async fn test_store_with_lease_duration(lease_duration: Duration) -> Option<PostgresStore> {
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
    let options = test_options(lease_duration);
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

    tokio::time::sleep(Duration::from_millis(4_200)).await;
    assert!(matches!(
        store
            .renew_lease(lease.fence(), desired_expiry)
            .await
            .unwrap(),
        LeaseRenewalOutcome::Idempotent(_)
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

    query(&format!("DROP DATABASE {database_name}"))
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

fn checkpoint_graph() -> GraphReference {
    let owner = PrincipalIdentity::new(
        "https://issuer.example.com/stateknot"
            .parse::<IssuerId>()
            .unwrap(),
        "checkpoint-registry".parse::<SubjectId>().unwrap(),
    );
    let identity = CapabilityIdentity::new(
        owner,
        CapabilityReference::new(
            CapabilityName::new("integration-workflow").unwrap(),
            Version::new(1, 0, 0),
        ),
    );
    let schema = SchemaReference::new(
        "https://stateknot.github.io/schema/integration-state/1.0.0"
            .parse::<SchemaId>()
            .unwrap(),
        Version::new(1, 0, 0),
        Digest::sha256(b"stateknot integration checkpoint state schema v1"),
    );
    GraphReference::new(
        identity,
        Digest::sha256(b"stateknot integration compiled workflow v1"),
        schema,
    )
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
    tool_invocation_intent_for_activation(
        NodeActivation::new(
            checkpoint.head(),
            GraphNamespace::root(),
            checkpoint
                .ready_nodes()
                .iter()
                .next()
                .expect("integration checkpoint must have a ready node")
                .clone(),
            Digest::sha256(b"integration node activation input"),
        ),
        invocation_id,
    )
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
    model_invocation_intent_for_activation(
        NodeActivation::new(
            checkpoint.head(),
            GraphNamespace::root(),
            checkpoint
                .ready_nodes()
                .iter()
                .next()
                .expect("integration checkpoint must have a ready node")
                .clone(),
            Digest::sha256(b"integration model activation input"),
        ),
        invocation_id,
    )
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

fn pending_activation(checkpoint: &Checkpoint, input: &[u8]) -> NodeActivation {
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
async fn pending_node_results_are_fenced_semantically_idempotent_and_load_verifiable() {
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
        .commit_pending_node_result(result_append(), intent.clone())
        .await
        .expect("pending result must commit atomically");
    assert!(matches!(
        committed,
        PendingNodeResultCommitOutcome::Committed { .. }
    ));
    assert_eq!(committed.event().sequence().get(), 2);
    assert_eq!(
        store
            .load_pending_node_result(&activation)
            .await
            .expect("pending result must fully restore"),
        *committed.result()
    );

    let same_event_retry = store
        .commit_pending_node_result(result_append(), intent.clone())
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
    let replacement_retry = store
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
        .await
        .expect("semantic retry after lease takeover must return the original winner");
    assert!(matches!(
        replacement_retry,
        PendingNodeResultCommitOutcome::Idempotent { .. }
    ));
    assert_eq!(replacement_retry.event(), committed.event());
    assert_eq!(replacement_retry.result(), committed.result());

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
            .commit_pending_node_result(
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
        Err(StoreError::PendingNodeResultConflict)
    ));
    let crossed_input = pending_activation(checkpoint.checkpoint(), b"another activation input");
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
            .commit_pending_node_result(
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
        .commit_pending_node_result(
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
        .commit_pending_node_result(
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
async fn concurrent_pending_node_result_commits_converge_on_one_physical_winner() {
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
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..24_u64 {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        let intent = intent.clone();
        let fence = lease.fence().clone();
        let parent = parent.clone();
        tasks.spawn(async move {
            store
                .commit_pending_node_result(
                    worker_append(
                        tenant_id,
                        run_id,
                        EventId::generate(),
                        JournalExpectation::exact(parent),
                        fence,
                        1_141 + index,
                    ),
                    intent,
                )
                .await
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
    assert_eq!(journal.events().len(), 2);
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
        .commit_pending_node_result(
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
    assert_eq!(run.journal_head(), Some(&durable_head));
    let journal = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(journal.events().len(), 5);
    assert!(
        journal
            .events()
            .iter()
            .all(|event| event.event_id() != result_event_id)
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
            .commit_pending_node_result(
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
    let committed = store
        .commit_pending_node_result(
            worker_append(
                durable_tenant.clone(),
                durable_run,
                EventId::generate(),
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
    let retry = store
        .commit_pending_node_result(
            worker_append(
                durable_tenant,
                durable_run,
                EventId::generate(),
                JournalExpectation::exact(cancelled.event().head()),
                durable_lease.fence().clone(),
                1_193,
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
        .commit_pending_node_result(
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

    let first_checkpoint = first.checkpoint().clone();
    let second_event_id = EventId::generate();
    let second_checkpoint_id = CheckpointId::generate();
    let second = store
        .append_control_plane_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                second_event_id,
                JournalExpectation::exact(first.event().head()),
                101,
            ),
            RunProjection::unchanged(),
            successor_checkpoint_write(second_checkpoint_id, &first_checkpoint, 1),
        )
        .await
        .expect("successor checkpoint must commit");
    assert_eq!(second.checkpoint().superstep().get(), 1);
    assert_eq!(second.checkpoint().parent(), Some(&first_checkpoint.head()));

    let stale_branch = successor_checkpoint_write(CheckpointId::generate(), &first_checkpoint, 2);
    assert!(matches!(
        store
            .append_control_plane_checkpoint(
                control_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(second.event().head()),
                    102,
                ),
                RunProjection::unchanged(),
                stale_branch,
            )
            .await,
        Err(StoreError::StaleCheckpointHead)
    ));

    let reused_old_id = successor_checkpoint_write(first_checkpoint_id, second.checkpoint(), 2);
    assert!(matches!(
        store
            .append_control_plane_checkpoint(
                control_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(second.event().head()),
                    103,
                ),
                RunProjection::unchanged(),
                reused_old_id,
            )
            .await,
        Err(StoreError::CheckpointIdConflict)
    ));

    assert_eq!(
        store
            .load_current_checkpoint(&tenant_id, run_id)
            .await
            .unwrap(),
        Some(second.checkpoint().clone())
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

    let current_lease = store
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
        Err(StoreError::StaleFence)
    ));

    let current = store
        .append_worker_checkpoint(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(first.event().head()),
                current_lease.fence().clone(),
                202,
            ),
            RunProjection::unchanged(),
            successor_checkpoint_write(CheckpointId::generate(), first.checkpoint(), 1),
        )
        .await
        .expect("current fence must commit checkpoint successor");
    assert_eq!(current.checkpoint().superstep().get(), 1);
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
        (
            GraphNamespace::root(),
            NodeId::new("not-a-ready-node").unwrap(),
        ),
        (GraphNamespace::new("nested").unwrap(), ready_node),
    ];

    for (index, (namespace, node_id)) in invalid_activations.into_iter().enumerate() {
        let descriptor = tool_descriptor();
        let invocation_id = InvocationId::generate();
        let intent = ToolInvocationIntent::new(
            NodeActivation::new(
                checkpoint.checkpoint().head(),
                namespace,
                node_id,
                Digest::sha256(b"invalid integration activation input"),
            ),
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
        (
            GraphNamespace::root(),
            NodeId::new("not-a-ready-model-node").unwrap(),
        ),
        (GraphNamespace::new("nested").unwrap(), ready_node),
    ];

    for (index, (namespace, node_id)) in invalid_activations.into_iter().enumerate() {
        let invocation_id = InvocationId::generate();
        let intent = ModelInvocationIntent::new(
            NodeActivation::new(
                checkpoint.checkpoint().head(),
                namespace,
                node_id,
                Digest::sha256(b"invalid integration model activation input"),
            ),
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
        Err(StoreError::CheckpointBlockedByToolInvocation)
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

    let barrier = store
        .append_worker_checkpoint(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                current_lease.fence().clone(),
                807,
            ),
            RunProjection::unchanged(),
            successor_checkpoint_write(CheckpointId::generate(), checkpoint.checkpoint(), 1),
        )
        .await
        .expect("a committed tool invocation must release the checkpoint barrier");
    assert_eq!(barrier.checkpoint().superstep().get(), 1);
    assert_eq!(barrier.event().sequence().get(), 5);
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
    assert_eq!(journal.events().len(), 5);
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
        Err(StoreError::CheckpointBlockedByModelInvocation)
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

    let barrier = store
        .append_worker_checkpoint(
            worker_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(committed.event().head()),
                current_lease.fence().clone(),
                907,
            ),
            RunProjection::unchanged(),
            successor_checkpoint_write(CheckpointId::generate(), checkpoint.checkpoint(), 1),
        )
        .await
        .expect("a committed model invocation must release the checkpoint barrier");
    assert_eq!(barrier.checkpoint().superstep().get(), 1);
    assert_eq!(barrier.event().sequence().get(), 5);
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
    assert_eq!(journal.events().len(), 5);
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
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();

    let first = store
        .append_control_plane_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                600,
            ),
            RunProjection::unchanged(),
            initial_checkpoint_write(tenant_id.clone(), run_id, CheckpointId::generate()),
        )
        .await
        .unwrap();
    let mut checkpoints = vec![first.checkpoint().clone()];
    let mut journal_head = first.event().head();
    for index in 1..=5_u64 {
        let outcome = store
            .append_control_plane_checkpoint(
                control_append(
                    tenant_id.clone(),
                    run_id,
                    EventId::generate(),
                    JournalExpectation::exact(journal_head),
                    600 + index,
                ),
                RunProjection::unchanged(),
                successor_checkpoint_write(
                    CheckpointId::generate(),
                    checkpoints.last().unwrap(),
                    index,
                ),
            )
            .await
            .unwrap();
        journal_head = outcome.event().head();
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

    let advanced = store
        .append_control_plane_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::exact(journal_head),
                606,
            ),
            RunProjection::unchanged(),
            successor_checkpoint_write(CheckpointId::generate(), checkpoints.last().unwrap(), 6),
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
    store
        .admit_run(provenance(broken_tenant.clone(), broken_run))
        .await
        .unwrap();
    let broken_root = store
        .append_control_plane_checkpoint(
            control_append(
                broken_tenant.clone(),
                broken_run,
                EventId::generate(),
                JournalExpectation::empty(),
                607,
            ),
            RunProjection::unchanged(),
            initial_checkpoint_write(broken_tenant.clone(), broken_run, CheckpointId::generate()),
        )
        .await
        .unwrap();
    store
        .append_control_plane_checkpoint(
            control_append(
                broken_tenant.clone(),
                broken_run,
                EventId::generate(),
                JournalExpectation::exact(broken_root.event().head()),
                608,
            ),
            RunProjection::unchanged(),
            successor_checkpoint_write(CheckpointId::generate(), broken_root.checkpoint(), 1),
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
         WHERE tenant_id = $1 AND run_id = $2 AND sequence = 3",
    )
    .bind(tenant_id.as_str())
    .bind(*run_id.as_uuid())
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
async fn concurrent_checkpoint_writers_form_one_linear_barrier_chain() {
    let _database_test_guard = DATABASE_TEST_MUTEX.lock().await;
    let Some(store) = test_store().await else {
        return;
    };
    let tenant_id = tenant("checkpoint-concurrency");
    let run_id = RunId::generate();
    store
        .admit_run(provenance(tenant_id.clone(), run_id))
        .await
        .unwrap();
    let initial = store
        .append_control_plane_checkpoint(
            control_append(
                tenant_id.clone(),
                run_id,
                EventId::generate(),
                JournalExpectation::empty(),
                500,
            ),
            RunProjection::unchanged(),
            initial_checkpoint_write(tenant_id.clone(), run_id, CheckpointId::generate()),
        )
        .await
        .unwrap();
    assert_eq!(initial.checkpoint().superstep().get(), 0);

    let writers = 24_u64;
    let mut tasks = Vec::new();
    for index in 1..=writers {
        let store = store.clone();
        let tenant_id = tenant_id.clone();
        tasks.push(tokio::spawn(async move {
            let event_id = EventId::generate();
            let checkpoint_id = CheckpointId::generate();
            loop {
                let parent = store
                    .load_current_checkpoint(&tenant_id, run_id)
                    .await
                    .unwrap()
                    .expect("initial checkpoint must remain present");
                let run = store.load_run(&tenant_id, run_id).await.unwrap();
                let append = control_append(
                    tenant_id.clone(),
                    run_id,
                    event_id,
                    JournalExpectation::exact(run.journal_head().unwrap().clone()),
                    500 + index,
                );
                let write = successor_checkpoint_write(checkpoint_id, &parent, index);
                match store
                    .append_control_plane_checkpoint(append, RunProjection::unchanged(), write)
                    .await
                {
                    Ok(outcome) => return outcome.checkpoint().superstep(),
                    Err(StoreError::StaleJournalHead | StoreError::StaleCheckpointHead) => {
                        tokio::task::yield_now().await;
                    }
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
    assert_eq!(current.journal_head().sequence().get(), writers + 1);
    let page = store
        .load_journal_page(&tenant_id, run_id, None, JournalPageSize::new(128).unwrap())
        .await
        .unwrap();
    assert_eq!(page.events().len(), usize::try_from(writers + 1).unwrap());
    assert!(!page.has_more());
    store.close().await;
}
