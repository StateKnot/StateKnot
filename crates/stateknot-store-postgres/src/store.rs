// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, collections::BTreeMap, fmt, sync::LazyLock, time::Duration};

use chrono::{DateTime, Utc};
use serde::Serialize;
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
    AgentResultProvenance, AttemptId, BoundedJson, CanonicalJson, Checkpoint, CheckpointHead,
    CheckpointId, CheckpointLineageVerifier, CheckpointWrite, Digest, EventId, FencingEpoch,
    InvocationId, JournalAppend, JournalChainVerifier, JournalEvent, JournalEventError,
    JournalEventIntent, JournalEventSource, JournalHead, JournalSequence, JsonLimits, RunFence,
    RunId, RunLease, RunLeaseValidationError, RunLifecycle, RunRevision, RunStatus, RunTransition,
    Superstep, TenantId, Timestamp, ToolInvocation, ToolInvocationHead,
    ToolInvocationHistoryVerifier, ToolInvocationIntent, ToolInvocationRevision,
    ToolInvocationStatus, ToolInvocationTransition, ToolInvocationTransitionKind,
};
use uuid::Uuid;

use crate::{
    AdmissionOutcome, AppendOutcome, CheckpointCommitOutcome, CheckpointLineagePage,
    CheckpointLineagePageSize, CheckpointPointer, JournalPage, JournalPageSize, LeaseClaimOutcome,
    LeaseReleaseOutcome, LeaseRenewalOutcome, PostgresStoreOptions, RunProjection, StoreError,
    StoredRun, ToolInvocationCommitOutcome, ToolInvocationHistoryPage,
    ToolInvocationHistoryPageSize,
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
    ]),
    ignore_missing: false,
    locking: true,
    no_tx: false,
});

const MIN_POSTGRES_VERSION_NUMBER: i32 = 160_000;
const MAX_POSTGRES_VERSION_NUMBER: i32 = 179_999;
const MAX_CHECKPOINT_BYTES: usize = 2_621_440;
const MAX_TOOL_INVOCATION_INTENT_BYTES: usize = 4_194_304;
const MAX_TOOL_INVOCATION_RECORD_BYTES: usize = 16_777_216;
const PROJECTION_DIGEST_DOMAIN: &[u8] = b"stateknot-postgres-run-projection-v1\0";

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
    quarantined_at
FROM stateknot.runs
WHERE tenant_id = $1 AND run_id = $2
FOR UPDATE
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

/// Connected `PostgreSQL` durability provider.
///
/// Clones share one bounded connection pool. `Debug` intentionally omits pool
/// internals and connection credentials.
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    options: PostgresStoreOptions,
}

impl PostgresStore {
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

        let complete = query_scalar::<_, bool>(
            "SELECT to_regclass('stateknot.runs') IS NOT NULL \
                 AND to_regclass('stateknot.run_events') IS NOT NULL \
                 AND to_regclass('stateknot.run_checkpoints') IS NOT NULL \
                 AND to_regclass('stateknot.tool_invocations') IS NOT NULL \
                 AND to_regclass('stateknot.tool_invocation_revisions') IS NOT NULL \
                 AND to_regprocedure('stateknot.is_uuid_v7(uuid)') IS NOT NULL",
        )
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
            query_scalar::<_, i32>("SELECT current_setting('server_version_num')::integer")
                .fetch_one(&pool)
                .await
                .map_err(|source| StoreError::database("server version check", source))?;
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

    /// Idempotently admits a pending run using a database commit timestamp.
    ///
    /// A retry with the same tenant/run identity succeeds only when the durable
    /// admission provenance is identical. If the run has progressed, its current
    /// validated lifecycle is returned.
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
    changed_at
)
VALUES ($1, $2, $3, $4, $5, $6::numeric, $7, $8, $8)
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
        let row = query_as::<_, RunRow>(SELECT_RUN)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| StoreError::database("run load", source))?
            .ok_or(StoreError::RunNotFound)?;
        let stored = decode_run(row)?;
        if stored.lifecycle().provenance().tenant_id() != tenant_id
            || stored.lifecycle().provenance().run_id() != run_id
        {
            return Err(StoreError::corrupt("run scope"));
        }
        Ok(stored)
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
    /// Retrying the same successor attempt is idempotent.
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

    /// Atomically appends a control-plane event and commits one graph barrier.
    ///
    /// The checkpoint write must belong to the same tenant/run, its exact
    /// parent must match the locked run pointer, and its anchoring event and
    /// optional lifecycle projection commit in the same transaction.
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
        self.append_checkpoint(
            append,
            projection,
            checkpoint,
            AppendAuthority::ControlPlane,
        )
        .await
    }

    /// Atomically appends a fenced worker event and commits one graph barrier.
    ///
    /// The database rechecks the exact unexpired lease while inserting the
    /// event, checkpoint, and updated run heads. Expiry at any statement rolls
    /// back the complete transaction.
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
        self.append_checkpoint(append, projection, checkpoint, AppendAuthority::Worker)
            .await
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
        update_run_head(&mut transaction, &event, prepared_projection.as_ref()).await?;
        update_checkpoint_pointer(&mut transaction, &checkpoint, event.source()).await?;

        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("checkpoint append commit", source))?;
        Ok(CheckpointCommitOutcome::Committed { event, checkpoint })
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

struct PreparedProjection {
    lifecycle_bytes: Vec<u8>,
    revision: String,
    status: &'static str,
    changed_at: DateTime<Utc>,
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

    Ok(StoredRun {
        lifecycle,
        journal_head,
        lease,
        last_fencing_epoch,
        checkpoint,
        quarantined: row.quarantined_at.is_some(),
    })
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
    let activation = intent.activation();
    activation.graph_namespace().is_root()
        && checkpoint.ready_nodes().contains(activation.node_id())
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
    let lifecycle = current
        .clone()
        .apply(transition)
        .map_err(|_| StoreError::InvalidLifecycleTransition)?;
    if lifecycle.changed_at() > recorded_at {
        return Err(StoreError::LifecycleObservationAfterCommit);
    }

    Ok(Some(PreparedProjection {
        lifecycle_bytes: encode_lifecycle(&lifecycle)?,
        revision: lifecycle.revision().to_string(),
        status: run_status_text(lifecycle.status()),
        changed_at: to_database_time(lifecycle.changed_at())?,
    }))
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
        Some(ToolInvocationTransition::StartAttempt { attempt_id }) => Some(*attempt_id.as_uuid()),
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
    .bind(started_attempt)
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
    updated_at = $5
WHERE tenant_id = $1
  AND run_id = $2
  AND (
      $11::uuid IS NULL
      OR (
          lease_attempt_id = $11
          AND fencing_epoch = $12
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
    if !matches!(
        stored.lifecycle().status(),
        RunStatus::Pending | RunStatus::Active | RunStatus::CancellationRequested
    ) {
        return Err(StoreError::RunNotRunnable);
    }
    Ok(())
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
    }
}
