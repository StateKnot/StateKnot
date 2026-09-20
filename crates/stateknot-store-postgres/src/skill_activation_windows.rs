// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Utc};
use sqlx_core::{from_row::FromRow, query::query, query_as::query_as, query_scalar::query_scalar};
use sqlx_postgres::{PgRow, Postgres};
use stateknot_core::{
    BoxFuture, Digest, SkillActingWindow, SkillActingWindowId, SkillActingWindowOpenRequest,
    SkillActingWindowRevocation, SkillActingWindowRevocationReason, SkillActivationApproval,
    SkillActivationSource, SkillActivationStore, SkillActivationStoreError,
    SkillActivationStoreFailure, TenantId, Timestamp, ToolAuthorizationReceipt,
};

use super::{
    PostgresStore, StoreError, decode_digest, from_database_time, has_database_constraint,
    to_database_time,
};

const MAX_APPROVAL_BYTES: usize = 65_536;
const MAX_WINDOW_BYTES: usize = 131_072;
const MAX_REVOCATION_BYTES: usize = 65_536;

const SELECT_WINDOW: &str = r"
SELECT tenant_id, window_id, approval_id, run_id, thread_id, subject_digest,
       opened_at, expires_at, window_digest, window_bytes, window_bytes_digest
FROM stateknot.skill_acting_windows
WHERE tenant_id=$1 AND window_id=$2
";

#[derive(Debug)]
struct WindowRow {
    tenant_id: String,
    window_id: uuid::Uuid,
    approval_id: uuid::Uuid,
    run_id: uuid::Uuid,
    thread_id: uuid::Uuid,
    subject_digest: Vec<u8>,
    opened_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    window_digest: Vec<u8>,
    window_bytes: Vec<u8>,
    window_bytes_digest: Vec<u8>,
}

impl<'row> FromRow<'row, PgRow> for WindowRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::error::Error> {
        use sqlx_core::row::Row;
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            window_id: row.try_get("window_id")?,
            approval_id: row.try_get("approval_id")?,
            run_id: row.try_get("run_id")?,
            thread_id: row.try_get("thread_id")?,
            subject_digest: row.try_get("subject_digest")?,
            opened_at: row.try_get("opened_at")?,
            expires_at: row.try_get("expires_at")?,
            window_digest: row.try_get("window_digest")?,
            window_bytes: row.try_get("window_bytes")?,
            window_bytes_digest: row.try_get("window_bytes_digest")?,
        })
    }
}

impl PostgresStore {
    /// Atomically persists one exact approval and database-clock acting window.
    #[allow(clippy::too_many_lines)]
    pub async fn open_skill_acting_window(
        &self,
        request: SkillActingWindowOpenRequest,
    ) -> Result<SkillActingWindow, StoreError> {
        let tenant = request.approval().scope().tenant_id();
        let mut transaction = self.begin_mutation("Skill acting window open").await?;
        lock_window(&mut transaction, tenant, request.window_id()).await?;

        if let Some(row) = query_as::<_, WindowRow>(SELECT_WINDOW)
            .bind(tenant.as_str())
            .bind(*request.window_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("Skill acting window retry", source))?
        {
            let stored = decode_window(&row)?;
            if stored.approval() != request.approval() {
                return Err(StoreError::SkillActivationConflict);
            }
            if locked_window_inactive(
                &mut transaction,
                tenant,
                request.window_id(),
                row.expires_at,
                "Skill acting window retry",
            )
            .await?
            {
                return Err(StoreError::SkillActingWindowInactive);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("Skill acting window retry commit", source)
            })?;
            return Ok(stored);
        }

        if let SkillActivationSource::Nested { parent_window_id } = request.approval().source() {
            let parent = load_locked_active(
                &mut transaction,
                tenant,
                *parent_window_id,
                "nested Skill parent",
            )
            .await?;
            if parent.approval().scope() != request.approval().scope() {
                return Err(StoreError::InvalidSkillActivation);
            }
        }

        let now: DateTime<Utc> = query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("Skill acting window database clock", source))?;
        let opened_at = from_database_time(now)?;
        let duration_micros = request
            .approval()
            .requested_duration()
            .duration()
            .as_i64()
            .checked_mul(1_000)
            .ok_or(StoreError::InvalidSkillActivation)?;
        let expires_at = Timestamp::from_unix_micros(
            opened_at
                .unix_micros()
                .checked_add(duration_micros)
                .ok_or(StoreError::InvalidSkillActivation)?,
        )
        .map_err(|_| StoreError::InvalidSkillActivation)?;
        let window = SkillActingWindow::new(
            request.window_id(),
            request.approval().clone(),
            opened_at,
            expires_at,
        )
        .map_err(|_| StoreError::InvalidSkillActivation)?;
        let approval_bytes = encode(request.approval(), MAX_APPROVAL_BYTES, "Skill approval")?;
        let window_bytes = encode(&window, MAX_WINDOW_BYTES, "Skill acting window")?;
        let (source_kind, parent_window_id) = match request.approval().source() {
            SkillActivationSource::Direct => ("direct", None),
            SkillActivationSource::Nested { parent_window_id } => {
                ("nested", Some(*parent_window_id.as_uuid()))
            }
            _ => return Err(StoreError::InvalidSkillActivation),
        };
        let approval = request.approval();
        let scope = approval.scope();
        let subject = approval.subject();
        let inserted_approval = query(
            r"INSERT INTO stateknot.skill_activation_approvals (
tenant_id,approval_id,run_id,thread_id,protocol,origin,skill_uri,manifest_digest,
subject_digest,source_kind,parent_window_id,policy_digest,decision_digest,
requested_duration_ms,approval_digest,recorded_at,approval_bytes
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
ON CONFLICT (tenant_id,approval_id) DO NOTHING",
        )
        .bind(tenant.as_str())
        .bind(*approval.approval_id().as_uuid())
        .bind(*scope.run_id().as_uuid())
        .bind(*scope.thread_id().as_uuid())
        .bind(subject.protocol())
        .bind(subject.origin())
        .bind(subject.uri())
        .bind(subject.manifest_digest().as_bytes())
        .bind(subject.subject_digest().as_bytes())
        .bind(source_kind)
        .bind(parent_window_id)
        .bind(approval.policy_digest().as_bytes())
        .bind(approval.decision_digest().as_bytes())
        .bind(approval.requested_duration().duration().as_i64())
        .bind(approval.approval_digest().as_bytes())
        .bind(now)
        .bind(&approval_bytes)
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("Skill activation approval insert", source))?;
        if inserted_approval.rows_affected() == 0 {
            let existing: Vec<u8> = query_scalar(
                "SELECT approval_bytes FROM stateknot.skill_activation_approvals WHERE tenant_id=$1 AND approval_id=$2",
            )
            .bind(tenant.as_str())
            .bind(*approval.approval_id().as_uuid())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("Skill activation approval conflict", source))?;
            if existing != approval_bytes
                || decode::<SkillActivationApproval>(
                    &existing,
                    MAX_APPROVAL_BYTES,
                    "Skill approval",
                )? != *approval
            {
                return Err(StoreError::SkillActivationConflict);
            }
        }

        let inserted_window = query(
            r"INSERT INTO stateknot.skill_acting_windows (
tenant_id,window_id,approval_id,run_id,thread_id,subject_digest,opened_at,expires_at,
window_digest,window_bytes
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(tenant.as_str())
        .bind(*window.window_id().as_uuid())
        .bind(*approval.approval_id().as_uuid())
        .bind(*scope.run_id().as_uuid())
        .bind(*scope.thread_id().as_uuid())
        .bind(subject.subject_digest().as_bytes())
        .bind(to_database_time(opened_at)?)
        .bind(to_database_time(expires_at)?)
        .bind(window.window_digest().as_bytes())
        .bind(&window_bytes)
        .execute(&mut *transaction)
        .await;
        if let Err(source) = inserted_window {
            if has_database_constraint(&source, "skill_acting_windows_approval_unique") {
                return Err(StoreError::SkillActivationConflict);
            }
            return Err(StoreError::database("Skill acting window insert", source));
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("Skill acting window commit", source))?;
        Ok(window)
    }

    /// Loads an exact currently active window at database time.
    pub async fn load_active_skill_acting_window(
        &self,
        tenant: &TenantId,
        window_id: SkillActingWindowId,
    ) -> Result<SkillActingWindow, StoreError> {
        let mut transaction = self.begin_mutation("Skill acting window load").await?;
        let window =
            load_locked_active(&mut transaction, tenant, window_id, "Skill acting window").await?;
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("Skill acting window load commit", source))?;
        Ok(window)
    }

    /// Revalidates exact durable evidence and active state at database time.
    pub async fn assert_skill_acting_window_active(
        &self,
        expected: &SkillActingWindow,
    ) -> Result<(), StoreError> {
        let actual = self
            .load_active_skill_acting_window(
                expected.approval().scope().tenant_id(),
                expected.window_id(),
            )
            .await?;
        if &actual != expected {
            return Err(StoreError::SkillActivationConflict);
        }
        Ok(())
    }

    /// Appends the first immutable revocation for an exact window.
    pub async fn revoke_skill_acting_window(
        &self,
        expected: &SkillActingWindow,
        reason: SkillActingWindowRevocationReason,
    ) -> Result<SkillActingWindowRevocation, StoreError> {
        let tenant = expected.approval().scope().tenant_id();
        let mut transaction = self.begin_mutation("Skill acting window revoke").await?;
        lock_window(&mut transaction, tenant, expected.window_id()).await?;
        let row = query_as::<_, WindowRow>(SELECT_WINDOW)
            .bind(tenant.as_str())
            .bind(*expected.window_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("Skill acting window revoke load", source))?
            .ok_or(StoreError::SkillActingWindowNotFound)?;
        if decode_window(&row)? != *expected {
            return Err(StoreError::SkillActivationConflict);
        }
        if let Some(bytes) = query_scalar::<_, Vec<u8>>(
            "SELECT revocation_bytes FROM stateknot.skill_acting_window_revocations WHERE tenant_id=$1 AND window_id=$2",
        )
        .bind(tenant.as_str())
        .bind(*expected.window_id().as_uuid())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("Skill acting window revocation retry", source))?
        {
            let existing = decode::<SkillActingWindowRevocation>(&bytes, MAX_REVOCATION_BYTES, "Skill revocation")?;
            if existing.reason() != reason {
                return Err(StoreError::SkillActivationConflict);
            }
            transaction.commit().await.map_err(|source| StoreError::database("Skill acting window revocation retry commit", source))?;
            return Ok(existing);
        }
        let now: DateTime<Utc> = query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("Skill revocation database clock", source))?;
        let revocation = SkillActingWindowRevocation::new(
            tenant.clone(),
            expected.window_id(),
            reason,
            from_database_time(now)?,
        )
        .map_err(|_| StoreError::InvalidSkillActivation)?;
        let bytes = encode(&revocation, MAX_REVOCATION_BYTES, "Skill revocation")?;
        query(
            "INSERT INTO stateknot.skill_acting_window_revocations (tenant_id,window_id,reason,revoked_at,revocation_digest,revocation_bytes) VALUES ($1,$2,$3,$4,$5,$6)",
        )
        .bind(tenant.as_str())
        .bind(*expected.window_id().as_uuid())
        .bind(revocation_reason_text(reason))
        .bind(now)
        .bind(revocation.revocation_digest().as_bytes())
        .bind(&bytes)
        .execute(&mut *transaction)
        .await
        .map_err(|source| StoreError::database("Skill acting window revocation insert", source))?;
        transaction.commit().await.map_err(|source| {
            StoreError::database("Skill acting window revocation commit", source)
        })?;
        Ok(revocation)
    }
}

pub(super) async fn validate_receipt_window(
    transaction: &mut sqlx_core::transaction::Transaction<'_, Postgres>,
    receipt: &ToolAuthorizationReceipt,
) -> Result<(), StoreError> {
    let Some(window_id) = receipt.authorization_window_id() else {
        return Ok(());
    };
    let provenance = receipt.provenance();
    let window = load_locked_active(
        transaction,
        provenance.tenant_id(),
        window_id,
        "Tool authorization Skill window",
    )
    .await?;
    let scope = window.approval().scope();
    if scope.run_id() != provenance.run_id()
        || scope.thread_id() != provenance.thread_id()
        || window.approval().subject().subject_digest() != receipt.subject_digest()
    {
        return Err(StoreError::InvalidToolAuthorizationReceipt);
    }
    Ok(())
}

async fn load_locked_active(
    transaction: &mut sqlx_core::transaction::Transaction<'_, Postgres>,
    tenant: &TenantId,
    window_id: SkillActingWindowId,
    operation: &'static str,
) -> Result<SkillActingWindow, StoreError> {
    lock_window(transaction, tenant, window_id).await?;
    let row = query_as::<_, WindowRow>(SELECT_WINDOW)
        .bind(tenant.as_str())
        .bind(*window_id.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|source| StoreError::database(operation, source))?
        .ok_or(StoreError::SkillActingWindowNotFound)?;
    let window = decode_window(&row)?;
    if locked_window_inactive(transaction, tenant, window_id, row.expires_at, operation).await? {
        return Err(StoreError::SkillActingWindowInactive);
    }
    Ok(window)
}

// Serialize window admission, receipt commit, active-state reads, and
// revocation without granting UPDATE on immutable evidence tables. The
// tenant grammar excludes '/', so the advisory-lock identity is unambiguous;
// a 64-bit hash collision only causes conservative extra serialization.
async fn lock_window(
    transaction: &mut sqlx_core::transaction::Transaction<'_, Postgres>,
    tenant: &TenantId,
    window_id: SkillActingWindowId,
) -> Result<(), StoreError> {
    query("SELECT pg_advisory_xact_lock(hashtextextended($1 || '/' || $2::text, 0))")
        .bind(tenant.as_str())
        .bind(*window_id.as_uuid())
        .execute(&mut **transaction)
        .await
        .map_err(|source| StoreError::database("Skill acting window serialization", source))?;
    Ok(())
}

async fn locked_window_inactive(
    transaction: &mut sqlx_core::transaction::Transaction<'_, Postgres>,
    tenant: &TenantId,
    window_id: SkillActingWindowId,
    expires_at: DateTime<Utc>,
    operation: &'static str,
) -> Result<bool, StoreError> {
    query_scalar(
        "SELECT clock_timestamp() >= $3 OR EXISTS (SELECT 1 FROM stateknot.skill_acting_window_revocations WHERE tenant_id=$1 AND window_id=$2)",
    )
    .bind(tenant.as_str())
    .bind(*window_id.as_uuid())
    .bind(expires_at)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| StoreError::database(operation, source))
}

fn decode_window(row: &WindowRow) -> Result<SkillActingWindow, StoreError> {
    let window =
        decode::<SkillActingWindow>(&row.window_bytes, MAX_WINDOW_BYTES, "Skill acting window")?;
    let scope = window.approval().scope();
    if row.tenant_id != scope.tenant_id().as_str()
        || row.window_id != *window.window_id().as_uuid()
        || row.approval_id != *window.approval().approval_id().as_uuid()
        || row.run_id != *scope.run_id().as_uuid()
        || row.thread_id != *scope.thread_id().as_uuid()
        || decode_digest(&row.subject_digest, "Skill subject")?
            != window.approval().subject().subject_digest()
        || from_database_time(row.opened_at)? != window.opened_at()
        || from_database_time(row.expires_at)? != window.expires_at()
        || decode_digest(&row.window_digest, "Skill window")? != window.window_digest()
        || Digest::sha256(&row.window_bytes)
            != decode_digest(&row.window_bytes_digest, "Skill window bytes")?
    {
        return Err(StoreError::corrupt("Skill acting window projection"));
    }
    Ok(window)
}

fn encode<T: serde::Serialize>(
    value: &T,
    maximum: usize,
    component: &'static str,
) -> Result<Vec<u8>, StoreError> {
    let bytes =
        serde_json_canonicalizer::to_vec(value).map_err(|_| StoreError::encoding(component))?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(StoreError::encoding(component));
    }
    Ok(bytes)
}

fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    maximum: usize,
    component: &'static str,
) -> Result<T, StoreError> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(StoreError::corrupt(component));
    }
    serde_json::from_slice(bytes).map_err(|_| StoreError::corrupt(component))
}

const fn revocation_reason_text(reason: SkillActingWindowRevocationReason) -> &'static str {
    match reason {
        SkillActingWindowRevocationReason::User => "user",
        SkillActingWindowRevocationReason::Policy => "policy",
        SkillActingWindowRevocationReason::Compromised => "compromised",
        SkillActingWindowRevocationReason::Superseded => "superseded",
        SkillActingWindowRevocationReason::Administrative => "administrative",
        _ => "invalid",
    }
}

impl SkillActivationStore for PostgresStore {
    fn open(
        &self,
        request: SkillActingWindowOpenRequest,
    ) -> BoxFuture<'_, Result<SkillActingWindow, SkillActivationStoreError>> {
        Box::pin(async move {
            self.open_skill_acting_window(request)
                .await
                .map_err(store_error)
        })
    }

    fn load_active(
        &self,
        tenant_id: TenantId,
        window_id: SkillActingWindowId,
    ) -> BoxFuture<'_, Result<SkillActingWindow, SkillActivationStoreError>> {
        Box::pin(async move {
            self.load_active_skill_acting_window(&tenant_id, window_id)
                .await
                .map_err(store_error)
        })
    }

    fn assert_active(
        &self,
        window: SkillActingWindow,
    ) -> BoxFuture<'_, Result<(), SkillActivationStoreError>> {
        Box::pin(async move {
            self.assert_skill_acting_window_active(&window)
                .await
                .map_err(store_error)
        })
    }

    fn revoke(
        &self,
        window: SkillActingWindow,
        reason: SkillActingWindowRevocationReason,
    ) -> BoxFuture<'_, Result<SkillActingWindowRevocation, SkillActivationStoreError>> {
        Box::pin(async move {
            self.revoke_skill_acting_window(&window, reason)
                .await
                .map_err(store_error)
        })
    }
}

fn store_error(source: StoreError) -> SkillActivationStoreError {
    let failure = match source {
        StoreError::SkillActingWindowNotFound => SkillActivationStoreFailure::NotFound,
        StoreError::SkillActingWindowInactive => SkillActivationStoreFailure::Inactive,
        StoreError::Database { .. }
        | StoreError::Migration { .. }
        | StoreError::UnsupportedServerVersion
        | StoreError::SchemaNotMigrated
        | StoreError::IncompatibleSchema
        | StoreError::IncompleteSchema => SkillActivationStoreFailure::Unavailable,
        _ => SkillActivationStoreFailure::Rejected,
    };
    SkillActivationStoreError::new(failure, source)
}
