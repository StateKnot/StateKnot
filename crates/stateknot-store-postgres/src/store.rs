// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, fmt, sync::LazyLock, time::Duration};

use chrono::{DateTime, Utc};
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
    AgentResultProvenance, AttemptId, BoundedJson, CanonicalJson, Digest, EventId, FencingEpoch,
    JournalAppend, JournalChainVerifier, JournalEvent, JournalEventError, JournalEventIntent,
    JournalEventSource, JournalHead, JournalSequence, JsonLimits, RunFence, RunId, RunLease,
    RunLeaseValidationError, RunLifecycle, RunStatus, TenantId, Timestamp,
};
use uuid::Uuid;

use crate::{
    AdmissionOutcome, AppendOutcome, JournalPage, JournalPageSize, LeaseClaimOutcome,
    LeaseReleaseOutcome, LeaseRenewalOutcome, PostgresStoreOptions, RunProjection, StoreError,
    StoredRun,
};

static MIGRATOR: LazyLock<Migrator> = LazyLock::new(|| Migrator {
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
});

const MIN_POSTGRES_VERSION_NUMBER: i32 = 160_000;
const MAX_POSTGRES_VERSION_NUMBER: i32 = 179_999;

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
    previous_digest,
    event_digest
FROM stateknot.run_events
WHERE tenant_id = $1 AND run_id = $2 AND sequence > $3
ORDER BY sequence ASC
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
            let event = decode_event(row)?;
            if !event.matches_intent(append.intent()) {
                return Err(StoreError::EventIdConflict);
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
        insert_event(&mut transaction, &event).await?;
        update_run_head(&mut transaction, &event, prepared_projection.as_ref()).await?;

        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("journal append commit", source))?;
        Ok(AppendOutcome::Committed(event))
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
    previous_digest: Option<Vec<u8>>,
    event_digest: Vec<u8>,
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
            previous_digest: row.try_get("previous_digest")?,
            event_digest: row.try_get("event_digest")?,
        })
    }
}

struct PreparedProjection {
    lifecycle_bytes: Vec<u8>,
    revision: String,
    status: &'static str,
    changed_at: DateTime<Utc>,
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

fn decode_run(row: RunRow) -> Result<StoredRun, StoreError> {
    let tenant_id =
        TenantId::try_from(row.tenant_id).map_err(|_| StoreError::corrupt("run tenant"))?;
    let run_id = RunId::from_uuid(row.run_id).map_err(|_| StoreError::corrupt("run identity"))?;
    let thread_id = stateknot_core::ThreadId::from_uuid(row.thread_id)
        .map_err(|_| StoreError::corrupt("run thread identity"))?;
    let invocation_id = stateknot_core::InvocationId::from_uuid(row.invocation_id)
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

fn decode_event(row: EventRow) -> Result<JournalEvent, StoreError> {
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
    previous_digest,
    event_digest
)
SELECT
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
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

fn positive_sequence(value: i64) -> Result<JournalSequence, StoreError> {
    let value = u64::try_from(value).map_err(|_| StoreError::corrupt("journal sequence"))?;
    JournalSequence::new(value).map_err(|_| StoreError::corrupt("journal sequence"))
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
    }

    #[test]
    fn retry_classification_is_conservative() {
        assert!(StoreError::database("test", sqlx_core::Error::PoolTimedOut).is_retryable());
        assert!(!StoreError::RunNotFound.is_retryable());
    }
}
