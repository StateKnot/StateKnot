// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use chrono::{DateTime, Utc};
use sqlx_core::{from_row::FromRow, query::query, query_as::query_as, row::Row};
use sqlx_postgres::PgRow;
use stateknot_core::{
    AuthorizationReceiptId, BoxFuture, InvocationId, RunId, TenantId, Timestamp,
    ToolAuthorizationOperation, ToolAuthorizationReceipt, ToolAuthorizationReceiptSink,
    ToolAuthorizationReceiptSinkError, ToolAuthorizationReceiptSinkFailure, ToolInvocationIntent,
};
use uuid::Uuid;

use super::{
    MAX_TOOL_INVOCATION_INTENT_BYTES, PostgresStore, StoreError, decode_digest, from_database_time,
    has_database_constraint, to_database_time,
};

const MAX_RECEIPT_BYTES: usize = 65_536;

const SELECT_RECEIPT: &str = r"
SELECT
    tenant_id,
    receipt_id,
    run_id,
    thread_id,
    invocation_id,
    attempt_id,
    origin_event_id,
    operation,
    descriptor_digest,
    input_digest,
    subject_digest,
    policy_digest,
    decision_digest,
    authorization_window_id,
    receipt_digest,
    has_recovery_handle,
    authorized_at,
    recorded_at,
    receipt_bytes,
    receipt_bytes_digest
FROM stateknot.tool_authorization_receipts
WHERE tenant_id = $1 AND receipt_id = $2
";

const SELECT_INVOCATION_AUTHORIZATION_BOUNDARY: &str = r"
SELECT
    invocation.intent_bytes,
    invocation.current_status,
    invocation.current_attempt_id,
    run.thread_id,
    revision.journal_event_id
FROM stateknot.tool_invocations AS invocation
JOIN stateknot.runs AS run
  ON run.tenant_id = invocation.tenant_id
 AND run.run_id = invocation.run_id
JOIN stateknot.tool_invocation_revisions AS revision
  ON revision.tenant_id = invocation.tenant_id
 AND revision.run_id = invocation.run_id
 AND revision.invocation_id = invocation.invocation_id
 AND revision.revision = invocation.current_revision
 AND revision.record_digest = invocation.current_record_digest
WHERE invocation.tenant_id = $1
  AND invocation.run_id = $2
  AND invocation.invocation_id = $3
FOR UPDATE OF invocation
";

const SELECT_RECEIPT_PAGE: &str = r"
SELECT
    tenant_id,
    receipt_id,
    run_id,
    thread_id,
    invocation_id,
    attempt_id,
    origin_event_id,
    operation,
    descriptor_digest,
    input_digest,
    subject_digest,
    policy_digest,
    decision_digest,
    authorization_window_id,
    receipt_digest,
    has_recovery_handle,
    authorized_at,
    recorded_at,
    receipt_bytes,
    receipt_bytes_digest
FROM stateknot.tool_authorization_receipts
WHERE tenant_id = $1
  AND run_id = $2
  AND invocation_id = $3
  AND (recorded_at, receipt_id) > ($4, $5)
ORDER BY recorded_at ASC, receipt_id ASC
LIMIT $6
";

/// Result of an exact immutable authorization-receipt write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolAuthorizationReceiptOutcome {
    /// New evidence was committed.
    Recorded,
    /// The exact canonical receipt was already durable.
    Idempotent,
}

/// One verified immutable receipt and its authoritative database commit time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredToolAuthorizationReceipt {
    receipt: ToolAuthorizationReceipt,
    recorded_at: Timestamp,
}

impl StoredToolAuthorizationReceipt {
    /// Returns the integrity-verified authorization evidence.
    #[must_use]
    pub const fn receipt(&self) -> &ToolAuthorizationReceipt {
        &self.receipt
    }

    /// Returns when the database made the receipt durable.
    #[must_use]
    pub const fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }
}

/// Bounded number of authorization receipts returned in one audit page.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ToolAuthorizationReceiptPageSize(u8);

impl ToolAuthorizationReceiptPageSize {
    /// Maximum decoded receipt count owned by one query.
    pub const MAX: u8 = 100;

    /// Constructs a positive bounded page size.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidToolAuthorizationReceiptPageSize`] outside
    /// `1..=100`.
    pub const fn new(value: u8) -> Result<Self, StoreError> {
        if value == 0 || value > Self::MAX {
            return Err(StoreError::InvalidToolAuthorizationReceiptPageSize);
        }
        Ok(Self(value))
    }

    /// Returns the validated query bound.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// One ascending, verified authorization-receipt audit page.
#[derive(Clone, Debug)]
pub struct ToolAuthorizationReceiptPage {
    records: Vec<StoredToolAuthorizationReceipt>,
    has_more: bool,
}

impl ToolAuthorizationReceiptPage {
    /// Returns receipts in database commit order.
    #[must_use]
    pub fn records(&self) -> &[StoredToolAuthorizationReceipt] {
        &self.records
    }

    /// Returns whether a later receipt remained in the observed snapshot.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    /// Returns the exact full-record continuation cursor.
    #[must_use]
    pub fn next_cursor(&self) -> Option<StoredToolAuthorizationReceipt> {
        self.records.last().cloned()
    }
}

impl PostgresStore {
    /// Durably records an exact Tool authorization before provider I/O.
    ///
    /// A fresh write is accepted only while the referenced Tool invocation is
    /// at the exact current execution/reconciliation boundary. An exact retry
    /// remains idempotent after the invocation advances.
    ///
    /// # Errors
    ///
    /// Returns an invalid/conflict/not-found error for crossed provenance or
    /// reused receipt identity, an integrity failure for corrupted durable
    /// bytes, or an availability error when `PostgreSQL` cannot commit.
    #[allow(clippy::too_many_lines)]
    pub async fn record_tool_authorization_receipt(
        &self,
        receipt: ToolAuthorizationReceipt,
    ) -> Result<ToolAuthorizationReceiptOutcome, StoreError> {
        let receipt_bytes = encode_receipt(&receipt)?;
        let provenance = receipt.provenance();
        let operation = operation_text(receipt.operation())
            .ok_or(StoreError::InvalidToolAuthorizationReceipt)?;
        let mut transaction = self.begin_mutation("Tool authorization receipt").await?;

        if let Some(row) = query_as::<_, ToolAuthorizationReceiptRow>(SELECT_RECEIPT)
            .bind(provenance.tenant_id().as_str())
            .bind(*receipt.receipt_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("Tool authorization receipt retry", source))?
        {
            let stored = decode_receipt(&row)?;
            if stored.receipt != receipt || encode_receipt(&stored.receipt)? != receipt_bytes {
                return Err(StoreError::ToolAuthorizationReceiptConflict);
            }
            transaction.commit().await.map_err(|source| {
                StoreError::database("Tool authorization receipt retry commit", source)
            })?;
            return Ok(ToolAuthorizationReceiptOutcome::Idempotent);
        }

        super::skill_activation_windows::validate_receipt_window(&mut transaction, &receipt)
            .await?;
        let boundary =
            query_as::<_, ToolAuthorizationBoundaryRow>(SELECT_INVOCATION_AUTHORIZATION_BOUNDARY)
                .bind(provenance.tenant_id().as_str())
                .bind(*provenance.run_id().as_uuid())
                .bind(*provenance.invocation_id().as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| StoreError::database("Tool authorization boundary", source))?
                .ok_or(StoreError::ToolAuthorizationReceiptNotFound)?;
        validate_boundary(&receipt, &boundary)?;

        let inserted = query(
            r"
INSERT INTO stateknot.tool_authorization_receipts (
    tenant_id,
    receipt_id,
    run_id,
    thread_id,
    invocation_id,
    attempt_id,
    origin_event_id,
    operation,
    descriptor_digest,
    input_digest,
    subject_digest,
    policy_digest,
    decision_digest,
    authorization_window_id,
    receipt_digest,
    has_recovery_handle,
    authorized_at,
    receipt_bytes
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
)
ON CONFLICT (tenant_id, receipt_id) DO NOTHING
",
        )
        .bind(provenance.tenant_id().as_str())
        .bind(*receipt.receipt_id().as_uuid())
        .bind(*provenance.run_id().as_uuid())
        .bind(*provenance.thread_id().as_uuid())
        .bind(*provenance.invocation_id().as_uuid())
        .bind(*provenance.attempt_id().as_uuid())
        .bind(*provenance.origin_event_id().as_uuid())
        .bind(operation)
        .bind(receipt.descriptor_digest().as_bytes())
        .bind(receipt.input_digest().as_bytes())
        .bind(receipt.subject_digest().as_bytes())
        .bind(receipt.policy_digest().as_bytes())
        .bind(receipt.decision_digest().as_bytes())
        .bind(receipt.authorization_window_id().map(|id| *id.as_uuid()))
        .bind(receipt.receipt_digest().as_bytes())
        .bind(receipt.has_recovery_handle())
        .bind(to_database_time(receipt.authorized_at())?)
        .bind(receipt_bytes.clone())
        .execute(&mut *transaction)
        .await
        .map_err(map_receipt_insert_error)?
        .rows_affected();

        let row = query_as::<_, ToolAuthorizationReceiptRow>(SELECT_RECEIPT)
            .bind(provenance.tenant_id().as_str())
            .bind(*receipt.receipt_id().as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("Tool authorization receipt verify", source))?
            .ok_or(StoreError::ToolAuthorizationReceiptConflict)?;
        let stored = decode_receipt(&row)?;
        if stored.receipt != receipt || encode_receipt(&stored.receipt)? != receipt_bytes {
            return Err(StoreError::ToolAuthorizationReceiptConflict);
        }
        transaction
            .commit()
            .await
            .map_err(|source| StoreError::database("Tool authorization receipt commit", source))?;
        Ok(if inserted == 1 {
            ToolAuthorizationReceiptOutcome::Recorded
        } else {
            ToolAuthorizationReceiptOutcome::Idempotent
        })
    }

    /// Loads one exact immutable authorization receipt with full validation.
    ///
    /// # Errors
    ///
    /// Returns not-found, corruption, or database availability failures.
    pub async fn load_tool_authorization_receipt(
        &self,
        tenant_id: &TenantId,
        receipt_id: AuthorizationReceiptId,
    ) -> Result<StoredToolAuthorizationReceipt, StoreError> {
        let mut transaction = self
            .begin_repeatable_read("Tool authorization receipt load")
            .await?;
        let row = query_as::<_, ToolAuthorizationReceiptRow>(SELECT_RECEIPT)
            .bind(tenant_id.as_str())
            .bind(*receipt_id.as_uuid())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("Tool authorization receipt load", source))?
            .ok_or(StoreError::ToolAuthorizationReceiptNotFound)?;
        let stored = decode_receipt(&row)?;
        if stored.receipt.provenance().tenant_id() != tenant_id
            || stored.receipt.receipt_id() != receipt_id
        {
            return Err(StoreError::corrupt("Tool authorization receipt scope"));
        }
        transaction.commit().await.map_err(|source| {
            StoreError::database("Tool authorization receipt load commit", source)
        })?;
        Ok(stored)
    }

    /// Lists one bounded verified receipt page for an exact Tool invocation.
    ///
    /// # Errors
    ///
    /// Returns an invalid cursor when the supplied full record crosses scope or
    /// no longer matches immutable storage; otherwise returns integrity or
    /// database errors.
    pub async fn load_tool_authorization_receipt_page(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        invocation_id: InvocationId,
        after: Option<&StoredToolAuthorizationReceipt>,
        page_size: ToolAuthorizationReceiptPageSize,
    ) -> Result<ToolAuthorizationReceiptPage, StoreError> {
        if after.is_some_and(|cursor| {
            let provenance = cursor.receipt.provenance();
            provenance.tenant_id() != tenant_id
                || provenance.run_id() != run_id
                || provenance.invocation_id() != invocation_id
        }) {
            return Err(StoreError::InvalidToolAuthorizationReceiptCursor);
        }
        let mut transaction = self
            .begin_repeatable_read("Tool authorization receipt page")
            .await?;
        if let Some(cursor) = after {
            let row = query_as::<_, ToolAuthorizationReceiptRow>(SELECT_RECEIPT)
                .bind(tenant_id.as_str())
                .bind(*cursor.receipt.receipt_id().as_uuid())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| {
                    StoreError::database("Tool authorization receipt cursor", source)
                })?
                .ok_or(StoreError::InvalidToolAuthorizationReceiptCursor)?;
            if decode_receipt(&row)? != *cursor {
                return Err(StoreError::InvalidToolAuthorizationReceiptCursor);
            }
        }
        let (after_time, after_id) = after.map_or(
            (
                to_database_time(Timestamp::MIN)
                    .expect("minimum core timestamp fits qualified PostgreSQL"),
                Uuid::nil(),
            ),
            |cursor| {
                (
                    to_database_time(cursor.recorded_at)
                        .expect("validated stored receipt time remains representable"),
                    *cursor.receipt.receipt_id().as_uuid(),
                )
            },
        );
        let limit = i64::from(page_size.get()) + 1;
        let rows = query_as::<_, ToolAuthorizationReceiptRow>(SELECT_RECEIPT_PAGE)
            .bind(tenant_id.as_str())
            .bind(*run_id.as_uuid())
            .bind(*invocation_id.as_uuid())
            .bind(after_time)
            .bind(after_id)
            .bind(limit)
            .fetch_all(&mut *transaction)
            .await
            .map_err(|source| StoreError::database("Tool authorization receipt page", source))?;
        let mut records = rows
            .into_iter()
            .map(|row| decode_receipt(&row))
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = records.len() > usize::from(page_size.get());
        if has_more {
            records.pop();
        }
        transaction.commit().await.map_err(|source| {
            StoreError::database("Tool authorization receipt page commit", source)
        })?;
        Ok(ToolAuthorizationReceiptPage { records, has_more })
    }
}

impl ToolAuthorizationReceiptSink for PostgresStore {
    fn record(
        &self,
        receipt: ToolAuthorizationReceipt,
    ) -> BoxFuture<'_, Result<(), ToolAuthorizationReceiptSinkError>> {
        Box::pin(async move {
            self.record_tool_authorization_receipt(receipt)
                .await
                .map(|_| ())
                .map_err(|source| {
                    let failure = receipt_sink_failure(&source);
                    ToolAuthorizationReceiptSinkError::new(failure, source)
                })
        })
    }
}

fn receipt_sink_failure(error: &StoreError) -> ToolAuthorizationReceiptSinkFailure {
    match error {
        StoreError::Database { .. }
        | StoreError::Migration { .. }
        | StoreError::UnsupportedServerVersion
        | StoreError::SchemaNotMigrated
        | StoreError::IncompatibleSchema
        | StoreError::IncompleteSchema => ToolAuthorizationReceiptSinkFailure::Unavailable,
        _ => ToolAuthorizationReceiptSinkFailure::Rejected,
    }
}

fn validate_boundary(
    receipt: &ToolAuthorizationReceipt,
    boundary: &ToolAuthorizationBoundaryRow,
) -> Result<(), StoreError> {
    if boundary.intent_bytes.is_empty()
        || boundary.intent_bytes.len() > MAX_TOOL_INVOCATION_INTENT_BYTES
    {
        return Err(StoreError::corrupt(
            "Tool authorization invocation intent size",
        ));
    }
    let intent = serde_json::from_slice::<ToolInvocationIntent>(&boundary.intent_bytes)
        .map_err(|_| StoreError::corrupt("Tool authorization invocation intent"))?;
    let canonical = serde_json_canonicalizer::to_vec(&intent)
        .map_err(|_| StoreError::corrupt("Tool authorization invocation canonicalization"))?;
    if canonical != boundary.intent_bytes {
        return Err(StoreError::corrupt(
            "Tool authorization invocation canonical bytes",
        ));
    }
    let provenance = receipt.provenance();
    let thread_id = stateknot_core::ThreadId::from_uuid(boundary.thread_id)
        .map_err(|_| StoreError::corrupt("Tool authorization thread identity"))?;
    let attempt_id = boundary
        .current_attempt_id
        .map(stateknot_core::AttemptId::from_uuid)
        .transpose()
        .map_err(|_| StoreError::corrupt("Tool authorization attempt identity"))?;
    let origin_event_id = stateknot_core::EventId::from_uuid(boundary.journal_event_id)
        .map_err(|_| StoreError::corrupt("Tool authorization event identity"))?;
    let expected_status = match receipt.operation() {
        ToolAuthorizationOperation::Execute => "executing",
        ToolAuthorizationOperation::Reconcile => "unknown",
        _ => return Err(StoreError::InvalidToolAuthorizationReceipt),
    };
    let descriptor = intent.descriptor();
    if intent.tenant_id() != provenance.tenant_id()
        || intent.run_id() != provenance.run_id()
        || intent.invocation_id() != provenance.invocation_id()
        || thread_id != provenance.thread_id()
        || attempt_id != Some(provenance.attempt_id())
        || origin_event_id != provenance.origin_event_id()
        || boundary.current_status != expected_status
        || descriptor.metadata().identity() != receipt.tool()
        || ToolAuthorizationReceipt::digest_descriptor(descriptor)
            .map_err(|_| StoreError::InvalidToolAuthorizationReceipt)?
            != receipt.descriptor_digest()
        || ToolAuthorizationReceipt::digest_input(intent.input())
            .map_err(|_| StoreError::InvalidToolAuthorizationReceipt)?
            != receipt.input_digest()
    {
        return Err(StoreError::InvalidToolAuthorizationReceipt);
    }
    Ok(())
}

fn encode_receipt(receipt: &ToolAuthorizationReceipt) -> Result<Vec<u8>, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(receipt)
        .map_err(|_| StoreError::encoding("Tool authorization receipt"))?;
    if bytes.is_empty() || bytes.len() > MAX_RECEIPT_BYTES {
        return Err(StoreError::InvalidToolAuthorizationReceipt);
    }
    Ok(bytes)
}

fn decode_receipt(
    row: &ToolAuthorizationReceiptRow,
) -> Result<StoredToolAuthorizationReceipt, StoreError> {
    if row.receipt_bytes.is_empty() || row.receipt_bytes.len() > MAX_RECEIPT_BYTES {
        return Err(StoreError::corrupt("Tool authorization receipt size"));
    }
    let receipt = serde_json::from_slice::<ToolAuthorizationReceipt>(&row.receipt_bytes)
        .map_err(|_| StoreError::corrupt("Tool authorization receipt value"))?;
    let canonical = serde_json_canonicalizer::to_vec(&receipt)
        .map_err(|_| StoreError::corrupt("Tool authorization receipt canonicalization"))?;
    if canonical != row.receipt_bytes {
        return Err(StoreError::corrupt(
            "Tool authorization receipt canonical bytes",
        ));
    }
    let provenance = receipt.provenance();
    let tenant_id = TenantId::try_from(row.tenant_id.as_str())
        .map_err(|_| StoreError::corrupt("Tool authorization receipt tenant"))?;
    let receipt_id = AuthorizationReceiptId::from_uuid(row.receipt_id)
        .map_err(|_| StoreError::corrupt("Tool authorization receipt identity"))?;
    let run_id = RunId::from_uuid(row.run_id)
        .map_err(|_| StoreError::corrupt("Tool authorization receipt run"))?;
    let thread_id = stateknot_core::ThreadId::from_uuid(row.thread_id)
        .map_err(|_| StoreError::corrupt("Tool authorization receipt thread"))?;
    let invocation_id = InvocationId::from_uuid(row.invocation_id)
        .map_err(|_| StoreError::corrupt("Tool authorization receipt invocation"))?;
    let attempt_id = stateknot_core::AttemptId::from_uuid(row.attempt_id)
        .map_err(|_| StoreError::corrupt("Tool authorization receipt attempt"))?;
    let origin_event_id = stateknot_core::EventId::from_uuid(row.origin_event_id)
        .map_err(|_| StoreError::corrupt("Tool authorization receipt event"))?;
    let recorded_at = from_database_time(row.recorded_at)?;
    if tenant_id != *provenance.tenant_id()
        || receipt_id != receipt.receipt_id()
        || run_id != provenance.run_id()
        || thread_id != provenance.thread_id()
        || invocation_id != provenance.invocation_id()
        || attempt_id != provenance.attempt_id()
        || origin_event_id != provenance.origin_event_id()
        || Some(row.operation.as_str()) != operation_text(receipt.operation())
        || decode_digest(
            &row.descriptor_digest,
            "Tool authorization descriptor digest",
        )? != receipt.descriptor_digest()
        || decode_digest(&row.input_digest, "Tool authorization input digest")?
            != receipt.input_digest()
        || decode_digest(&row.subject_digest, "Tool authorization subject digest")?
            != receipt.subject_digest()
        || decode_digest(&row.policy_digest, "Tool authorization policy digest")?
            != receipt.policy_digest()
        || decode_digest(&row.decision_digest, "Tool authorization decision digest")?
            != receipt.decision_digest()
        || row.authorization_window_id != receipt.authorization_window_id().map(|id| *id.as_uuid())
        || decode_digest(&row.receipt_digest, "Tool authorization receipt digest")?
            != receipt.receipt_digest()
        || decode_digest(
            &row.receipt_bytes_digest,
            "Tool authorization receipt bytes digest",
        )? != stateknot_core::Digest::sha256(&row.receipt_bytes)
        || row.has_recovery_handle != receipt.has_recovery_handle()
        || from_database_time(row.authorized_at)? != receipt.authorized_at()
        || recorded_at < receipt.authorized_at()
    {
        return Err(StoreError::corrupt("Tool authorization receipt projection"));
    }
    Ok(StoredToolAuthorizationReceipt {
        receipt,
        recorded_at,
    })
}

const fn operation_text(operation: ToolAuthorizationOperation) -> Option<&'static str> {
    match operation {
        ToolAuthorizationOperation::Execute => Some("execute"),
        ToolAuthorizationOperation::Reconcile => Some("reconcile"),
        _ => None,
    }
}

fn map_receipt_insert_error(source: sqlx_core::Error) -> StoreError {
    if has_database_constraint(&source, "tool_authorization_receipts_run_thread_fk")
        || has_database_constraint(&source, "tool_authorization_receipts_attempt_event_fk")
        || has_database_constraint(&source, "tool_authorization_receipts_window_fk")
    {
        StoreError::ToolAuthorizationReceiptNotFound
    } else if has_database_constraint(&source, "tool_authorization_receipts_ids_are_uuid_v7")
        || has_database_constraint(&source, "tool_authorization_receipts_operation_valid")
        || has_database_constraint(&source, "tool_authorization_receipts_digest_lengths")
        || has_database_constraint(&source, "tool_authorization_receipts_bytes_bounded")
        || has_database_constraint(&source, "tool_authorization_receipts_clock_valid")
        || has_database_constraint(&source, "tool_authorization_receipts_window_uuid_v7")
    {
        StoreError::InvalidToolAuthorizationReceipt
    } else {
        StoreError::database("Tool authorization receipt insert", source)
    }
}

struct ToolAuthorizationBoundaryRow {
    intent_bytes: Vec<u8>,
    current_status: String,
    current_attempt_id: Option<Uuid>,
    thread_id: Uuid,
    journal_event_id: Uuid,
}

impl<'row> FromRow<'row, PgRow> for ToolAuthorizationBoundaryRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            intent_bytes: row.try_get("intent_bytes")?,
            current_status: row.try_get("current_status")?,
            current_attempt_id: row.try_get("current_attempt_id")?,
            thread_id: row.try_get("thread_id")?,
            journal_event_id: row.try_get("journal_event_id")?,
        })
    }
}

struct ToolAuthorizationReceiptRow {
    tenant_id: String,
    receipt_id: Uuid,
    run_id: Uuid,
    thread_id: Uuid,
    invocation_id: Uuid,
    attempt_id: Uuid,
    origin_event_id: Uuid,
    operation: String,
    descriptor_digest: Vec<u8>,
    input_digest: Vec<u8>,
    subject_digest: Vec<u8>,
    policy_digest: Vec<u8>,
    decision_digest: Vec<u8>,
    authorization_window_id: Option<Uuid>,
    receipt_digest: Vec<u8>,
    has_recovery_handle: bool,
    authorized_at: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
    receipt_bytes: Vec<u8>,
    receipt_bytes_digest: Vec<u8>,
}

impl<'row> FromRow<'row, PgRow> for ToolAuthorizationReceiptRow {
    fn from_row(row: &'row PgRow) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            receipt_id: row.try_get("receipt_id")?,
            run_id: row.try_get("run_id")?,
            thread_id: row.try_get("thread_id")?,
            invocation_id: row.try_get("invocation_id")?,
            attempt_id: row.try_get("attempt_id")?,
            origin_event_id: row.try_get("origin_event_id")?,
            operation: row.try_get("operation")?,
            descriptor_digest: row.try_get("descriptor_digest")?,
            input_digest: row.try_get("input_digest")?,
            subject_digest: row.try_get("subject_digest")?,
            policy_digest: row.try_get("policy_digest")?,
            decision_digest: row.try_get("decision_digest")?,
            authorization_window_id: row.try_get("authorization_window_id")?,
            receipt_digest: row.try_get("receipt_digest")?,
            has_recovery_handle: row.try_get("has_recovery_handle")?,
            authorized_at: row.try_get("authorized_at")?,
            recorded_at: row.try_get("recorded_at")?,
            receipt_bytes: row.try_get("receipt_bytes")?,
            receipt_bytes_digest: row.try_get("receipt_bytes_digest")?,
        })
    }
}
