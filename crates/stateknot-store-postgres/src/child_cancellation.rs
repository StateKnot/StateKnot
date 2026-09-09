// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded durable cancellation delivery. Delivery never confirms a terminal outcome.

#[allow(clippy::wildcard_imports)]
use super::*;
use child_runs::{ancestry, anchored_event, load_record_inner, lock_tree, row_head};
use stateknot_core::{ChildRunKey, RunCancellationRequest};

/// What the child had durably observed when its cancellation work was consumed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildCancellationOutcome {
    /// This transaction requested cancellation; execution cleanup is still required.
    Requested,
    /// Another cancellation already owned the child; its reason was not overwritten.
    AlreadyRequested,
    /// The child was already terminal; its outcome and accounting were not changed.
    Terminal,
}

impl ChildCancellationOutcome {
    const fn text(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::AlreadyRequested => "already_requested",
            Self::Terminal => "terminal",
        }
    }
}

/// Immutable delivery evidence; not acknowledgement that external work stopped.
#[derive(Clone, Debug)]
pub struct ChildCancellationReceipt {
    outcome: ChildCancellationOutcome,
    lifecycle: RunLifecycle,
    head: JournalHead,
    delivered_at: Timestamp,
}

impl ChildCancellationReceipt {
    /// Returns whether cancellation was newly requested, already requested, or unnecessary.
    #[must_use]
    pub const fn outcome(&self) -> ChildCancellationOutcome {
        self.outcome
    }
    /// Returns the captured child lifecycle, not a later mutable projection.
    #[must_use]
    pub const fn lifecycle(&self) -> &RunLifecycle {
        &self.lifecycle
    }
    /// Returns the exact child journal observation retained after later appends.
    #[must_use]
    pub const fn head(&self) -> &JournalHead {
        &self.head
    }
    /// Returns the database observation that consumed queue work.
    #[must_use]
    pub const fn delivered_at(&self) -> Timestamp {
        self.delivered_at
    }
}

/// Verified immutable parent cancellation witness and optional child delivery receipt.
#[derive(Clone, Debug)]
pub struct ChildCancellationRecord {
    key: ChildRunKey,
    child_run_id: RunId,
    parent_lifecycle: RunLifecycle,
    parent_head: JournalHead,
    queued_at: Timestamp,
    receipt: Option<ChildCancellationReceipt>,
}

impl ChildCancellationRecord {
    /// Returns the exact tenant/parent activation/slot ownership key.
    #[must_use]
    pub const fn key(&self) -> &ChildRunKey {
        &self.key
    }
    /// Returns the immutable owned child identity.
    #[must_use]
    pub const fn child_run_id(&self) -> RunId {
        self.child_run_id
    }
    /// Returns the captured parent cancellation lifecycle.
    #[must_use]
    pub const fn parent_lifecycle(&self) -> &RunLifecycle {
        &self.parent_lifecycle
    }
    /// Returns the source observation, possibly a later audit head for upgraded work.
    #[must_use]
    pub const fn parent_head(&self) -> &JournalHead {
        &self.parent_head
    }
    /// Returns queue insertion time; scan order is not a permanent delivery watermark.
    #[must_use]
    pub const fn queued_at(&self) -> Timestamp {
        self.queued_at
    }
    /// Returns the original once-only delivery evidence.
    #[must_use]
    pub const fn receipt(&self) -> Option<&ChildCancellationReceipt> {
        self.receipt.as_ref()
    }
}

/// Atomic delivery result, with original evidence recovered on logical retries.
#[derive(Clone, Debug)]
pub enum ChildCancellationDelivery {
    /// Child request/abandonment, descendant queue work and receipt committed together.
    Committed(ChildCancellationRecord),
    /// This ownership key was already delivered; no child mutation was repeated.
    Idempotent(ChildCancellationRecord),
}

impl ChildCancellationDelivery {
    /// Returns verified original delivery evidence for either outcome.
    #[must_use]
    pub const fn record(&self) -> &ChildCancellationRecord {
        match self {
            Self::Committed(value) | Self::Idempotent(value) => value,
        }
    }
}

impl PostgresStore {
    /// Loads a cancellation witness and verifies its ownership, lifecycle and exact anchors.
    /// Authentication and tenant delegation authorization must precede this trusted-store API.
    pub async fn load_child_cancellation(
        &self,
        key: &ChildRunKey,
    ) -> Result<ChildCancellationRecord, StoreError> {
        let mut tx = self
            .begin_repeatable_read("child cancellation read")
            .await?;
        let parent = query_as::<_, RunRow>(SELECT_RUN)
            .bind(key.tenant_id().as_str())
            .bind(*key.parent_run_id().as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|source| StoreError::database("child cancellation parent", source))?
            .ok_or(StoreError::ChildCancellationNotFound)?;
        let parent = decode_run(parent)?;
        let child = Box::pin(load_record_inner(&mut tx, key, false))
            .await?
            .ok_or(StoreError::ChildRunNotFound)?;
        let record = load_cancellation(&mut tx, key, &parent, &child).await?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child cancellation read commit", source))?;
        Ok(record)
    }

    /// Discovers at most 16 pending cancellation keys using an index range.
    /// Continue with the last key, even after delivery; restart at `None` after a full sweep.
    /// A full sweep must pass blocked/quarantined items; never retain an event-time watermark.
    pub async fn pending_child_cancellations_after(
        &self,
        tenant: &TenantId,
        after: Option<&ChildRunKey>,
    ) -> Result<Vec<ChildRunKey>, StoreError> {
        let cursor = if let Some(key) = after {
            if key.tenant_id() != tenant {
                return Err(StoreError::ChildRunRejected);
            }
            Some(query_as::<_, (DateTime<Utc>, Uuid)>("SELECT queued_at, child_run_id FROM stateknot.child_run_cancellations WHERE tenant_id=$1 AND parent_run_id=$2 AND key_digest=$3")
                .bind(tenant.as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes())
                .fetch_optional(&self.pool).await.map_err(|source| StoreError::database("child cancellation cursor", source))?
                .ok_or(StoreError::ChildCancellationNotFound)?)
        } else {
            None
        };
        let sql = if cursor.is_some() {
            "SELECT owner.key_bytes, work.key_digest, work.parent_run_id FROM stateknot.child_run_cancellations AS work JOIN stateknot.child_run_ownership AS owner USING (tenant_id,parent_run_id,key_digest,child_run_id) WHERE work.tenant_id=$1 AND work.delivered_at IS NULL AND (work.queued_at,work.child_run_id)>($2,$3) ORDER BY work.queued_at,work.child_run_id LIMIT 16"
        } else {
            "SELECT owner.key_bytes, work.key_digest, work.parent_run_id FROM stateknot.child_run_cancellations AS work JOIN stateknot.child_run_ownership AS owner USING (tenant_id,parent_run_id,key_digest,child_run_id) WHERE work.tenant_id=$1 AND work.delivered_at IS NULL ORDER BY work.queued_at,work.child_run_id LIMIT 16"
        };
        let mut listing = query_as::<_, (Vec<u8>, Vec<u8>, Uuid)>(sql).bind(tenant.as_str());
        if let Some((at, child)) = cursor {
            listing = listing.bind(at).bind(child);
        }
        let rows = listing
            .fetch_all(&self.pool)
            .await
            .map_err(|source| StoreError::database("child cancellation discovery", source))?;
        rows.into_iter()
            .map(|(bytes, digest, parent)| {
                if bytes.len() > 65_536 {
                    return Err(StoreError::corrupt("child cancellation key size"));
                }
                let key: ChildRunKey = serde_json::from_slice(&bytes)
                    .map_err(|_| StoreError::corrupt("child cancellation key"))?;
                if key.tenant_id() != tenant
                    || *key.parent_run_id().as_uuid() != parent
                    || key.digest() != decode_digest(&digest, "child cancellation key checksum")?
                {
                    return Err(StoreError::corrupt("child cancellation key scope"));
                }
                Ok(key)
            })
            .collect()
    }

    /// Delivers a parent's durable cancellation to one owned child in one transaction.
    /// Locks tree → parent → child. Child request, all existing wait abandonments,
    /// descendant work and immutable receipt commit together. No external code runs here.
    /// Existing child cancellation/terminal outcomes win without replacement. Logical
    /// retries recover the first receipt before checking candidate IDs or stale heads.
    /// This trusted control-plane operation never confirms cancellation or invents usage.
    #[allow(clippy::too_many_lines)]
    pub async fn deliver_child_cancellation(
        &self,
        key: &ChildRunKey,
        append: JournalAppend,
        request: RunCancellationRequest,
    ) -> Result<ChildCancellationDelivery, StoreError> {
        if append.worker_fence().is_some()
            || append.intent().tenant_id() != key.tenant_id()
            || append.intent().payload().kind().as_str() != "child-run-cancellation-requested"
            || request.failure().caused_by_event_id() != Some(append.intent().event_id())
        {
            return Err(StoreError::ChildRunRejected);
        }
        let mut tx = self.begin_mutation("child cancellation delivery").await?;
        let ancestors = ancestry(&mut tx, key.tenant_id(), key.parent_run_id()).await?;
        lock_tree(&mut tx, key.tenant_id(), ancestors[0]).await?;
        let parent =
            decode_run(fetch_locked_run_row(&mut tx, key.tenant_id(), key.parent_run_id()).await?)?;
        let child = Box::pin(load_record_inner(&mut tx, key, true))
            .await?
            .ok_or(StoreError::ChildRunNotFound)?;
        let record = load_cancellation(&mut tx, key, &parent, &child).await?;
        if append.intent().run_id() != record.child_run_id {
            return Err(StoreError::ChildRunRejected);
        }
        if record.receipt.is_some() {
            tx.commit()
                .await
                .map_err(|source| StoreError::database("child cancellation retry", source))?;
            return Ok(ChildCancellationDelivery::Idempotent(record));
        }
        if parent.is_quarantined() || child.child().run().is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        let current = child.child().run();
        let mut at = database_now(&mut tx, "child cancellation clock")
            .await?
            .max(record.queued_at);
        let (outcome, lifecycle, head) = if current.lifecycle().status().is_terminal() {
            (
                ChildCancellationOutcome::Terminal,
                current.lifecycle().clone(),
                current
                    .journal_head()
                    .cloned()
                    .ok_or(StoreError::ChildRunRejected)?,
            )
        } else if current.lifecycle().cancellation_request().is_some() {
            (
                ChildCancellationOutcome::AlreadyRequested,
                current.lifecycle().clone(),
                current
                    .journal_head()
                    .cloned()
                    .ok_or(StoreError::ChildRunRejected)?,
            )
        } else {
            if append.expectation().head() != current.journal_head() {
                return Err(StoreError::StaleJournalHead);
            }
            at = at.max(
                current
                    .journal_head()
                    .ok_or(StoreError::ChildRunRejected)?
                    .recorded_at(),
            );
            if request.requested_at() < record.parent_lifecycle.changed_at() {
                return Err(StoreError::ChildRunRejected);
            }
            let transition = RunTransition::RequestCancellation { request };
            let projection =
                RunProjection::transition(current.lifecycle().revision(), transition.clone());
            let prepared = prepare_durable_wait_projection(
                current,
                key.tenant_id(),
                record.child_run_id,
                current.lifecycle().revision(),
                transition.clone(),
                at,
            )?;
            let lifecycle = current
                .lifecycle()
                .clone()
                .apply(transition)
                .map_err(|_| StoreError::InvalidLifecycleTransition)?;
            verify_current_wait_set(&mut tx, current).await?;
            let waits = cancellation_waits(&mut tx, current).await?;
            let digest = if waits.is_empty() {
                projection_digest(&projection)?
            } else {
                wait_abandonment_projection_digest(&projection, &waits)?
            };
            let event =
                JournalEvent::commit(append, at).map_err(|error| map_event_commit_error(&error))?;
            insert_event(&mut tx, &event, digest).await?;
            for wait in waits {
                let abandonment = materialize_wait_abandonment(
                    wait,
                    WaitAbandonmentReason::RunCancellation,
                    event.head(),
                )?;
                insert_wait_abandonment(&mut tx, &abandonment).await?;
                project_wait_abandonment(&mut tx, &abandonment).await?;
            }
            update_run_head(&mut tx, &event, Some(&prepared)).await?;
            (ChildCancellationOutcome::Requested, lifecycle, event.head())
        };
        at = at.max(head.recorded_at());
        let bytes = encode_lifecycle(&lifecycle)?;
        query("INSERT INTO stateknot.child_run_cancellation_receipts (tenant_id,parent_run_id,key_digest,child_run_id,outcome,lifecycle_bytes,lifecycle_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest,delivered_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)")
            .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes()).bind(*record.child_run_id.as_uuid())
            .bind(outcome.text()).bind(&bytes).bind(Digest::sha256(&bytes).as_bytes())
            .bind(i64::try_from(head.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?)
            .bind(*head.event_id().as_uuid()).bind(to_database_time(head.recorded_at())?).bind(head.digest().as_bytes()).bind(to_database_time(at)?)
            .execute(&mut *tx).await.map_err(|source| StoreError::database("child cancellation receipt insert", source))?;
        let updated = query("UPDATE stateknot.child_run_cancellations SET delivered_at=$4 WHERE tenant_id=$1 AND parent_run_id=$2 AND key_digest=$3 AND delivered_at IS NULL")
            .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes()).bind(to_database_time(at)?)
            .execute(&mut *tx).await.map_err(|source| StoreError::database("child cancellation delivery projection", source))?.rows_affected();
        if updated != 1 {
            return Err(StoreError::corrupt("child cancellation delivery row count"));
        }
        let mut record = record;
        record.receipt = Some(ChildCancellationReceipt {
            outcome,
            lifecycle,
            head,
            delivered_at: at,
        });
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child cancellation delivery commit", source))?;
        Ok(ChildCancellationDelivery::Committed(record))
    }
}

pub(super) async fn cancellation_waits(
    tx: &mut Transaction<'_, Postgres>,
    stored: &StoredRun,
) -> Result<Vec<DurableWait>, StoreError> {
    if stored.lifecycle().status() != RunStatus::Waiting {
        return Ok(Vec::new());
    }
    let scope = stored.lifecycle().provenance();
    let rows = query_as::<_, WaitRegistrationRow>(
        SELECT_OUTSTANDING_WAIT_REGISTRATIONS_FOR_UPDATE.as_str(),
    )
    .bind(scope.tenant_id().as_str())
    .bind(*scope.run_id().as_uuid())
    .fetch_all(&mut **tx)
    .await
    .map_err(|source| StoreError::database("child cancellation wait lock", source))?;
    if rows.is_empty()
        || rows.len() > RunWaits::MAX_LEN
        || rows.len() != usize::from(stored.unresolved_wait_count())
    {
        return Err(StoreError::WaitAbandonmentCommitConflict);
    }
    let mut waits = Vec::with_capacity(rows.len());
    for row in rows {
        let wait = decode_wait_registration(&row)?;
        if row.status != "outstanding" {
            return Err(StoreError::WaitAbandonmentCommitConflict);
        }
        verify_wait_registration_event(tx, &wait, row.registration_sequence).await?;
        waits.push(wait);
    }
    Ok(waits)
}

fn get_error(source: sqlx_core::Error) -> StoreError {
    StoreError::database("child cancellation decode", source)
}

fn lifecycle_from_row(row: &PgRow) -> Result<RunLifecycle, StoreError> {
    let bytes: Vec<u8> = row.try_get("lifecycle_bytes").map_err(get_error)?;
    if bytes.is_empty() || bytes.len() > 4_194_304 {
        return Err(StoreError::corrupt("child cancellation lifecycle size"));
    }
    let lifecycle: RunLifecycle = serde_json::from_slice(&bytes)
        .map_err(|_| StoreError::corrupt("child cancellation lifecycle"))?;
    if encode_lifecycle(&lifecycle)? != bytes
        || Digest::sha256(&bytes)
            != decode_digest(
                &row.try_get::<Vec<u8>, _>("lifecycle_digest")
                    .map_err(get_error)?,
                "child cancellation lifecycle checksum",
            )?
    {
        return Err(StoreError::corrupt("child cancellation lifecycle checksum"));
    }
    Ok(lifecycle)
}

async fn verify_head(
    tx: &mut Transaction<'_, Postgres>,
    head: &JournalHead,
) -> Result<(JournalEvent, Option<Digest>), StoreError> {
    let event = anchored_event(
        tx,
        head.tenant_id(),
        head.run_id(),
        i64::try_from(head.sequence().get())
            .map_err(|_| StoreError::corrupt("child cancellation sequence"))?,
    )
    .await?;
    if event.0.head() != *head {
        return Err(StoreError::corrupt("child cancellation journal anchor"));
    }
    Ok(event)
}

fn cancellation_matches(left: &RunLifecycle, right: &RunLifecycle) -> Result<bool, StoreError> {
    let encode = |value: &RunLifecycle| {
        serde_json_canonicalizer::to_vec(&value.cancellation_request())
            .map_err(|_| StoreError::corrupt("child cancellation request encoding"))
    };
    Ok(encode(left)? == encode(right)?)
}

#[allow(clippy::too_many_lines)]
async fn load_cancellation(
    tx: &mut Transaction<'_, Postgres>,
    key: &ChildRunKey,
    parent: &StoredRun,
    child: &ChildRunRecord,
) -> Result<ChildCancellationRecord, StoreError> {
    let row = query("SELECT * FROM stateknot.child_run_cancellations WHERE tenant_id=$1 AND parent_run_id=$2 AND key_digest=$3")
        .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes()).fetch_optional(&mut **tx).await
        .map_err(|source| StoreError::database("child cancellation witness", source))?.ok_or(StoreError::ChildCancellationNotFound)?;
    let lifecycle = lifecycle_from_row(&row)?;
    let head = row_head(&row, key.tenant_id(), key.parent_run_id())?;
    verify_head(tx, &head).await?;
    let child_id = child.child().admission().intent().provenance().run_id();
    let queued_at = from_database_time(row.try_get("queued_at").map_err(get_error)?)?;
    let delivered: Option<DateTime<Utc>> = row.try_get("delivered_at").map_err(get_error)?;
    if row.try_get::<Uuid, _>("child_run_id").map_err(get_error)? != *child_id.as_uuid()
        || lifecycle.status() != RunStatus::CancellationRequested
        || lifecycle.provenance() != parent.lifecycle().provenance()
        || !cancellation_matches(&lifecycle, parent.lifecycle())?
        || parent.lifecycle().revision() < lifecycle.revision()
        || parent
            .journal_head()
            .is_none_or(|current| current.sequence() < head.sequence())
        || lifecycle.changed_at() > head.recorded_at()
        || head.sequence() <= child.spawn().sequence()
    {
        return Err(StoreError::corrupt("child cancellation parent binding"));
    }
    let receipt_row = query("SELECT * FROM stateknot.child_run_cancellation_receipts WHERE tenant_id=$1 AND parent_run_id=$2 AND key_digest=$3")
        .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes()).fetch_optional(&mut **tx).await
        .map_err(|source| StoreError::database("child cancellation receipt", source))?;
    if delivered.is_some() != receipt_row.is_some() {
        return Err(StoreError::corrupt(
            "child cancellation delivery projection",
        ));
    }
    let receipt = if let Some(row) = receipt_row {
        let captured = lifecycle_from_row(&row)?;
        let receipt_head = row_head(&row, key.tenant_id(), child_id)?;
        let (event, projection) = verify_head(tx, &receipt_head).await?;
        let delivered_at = from_database_time(row.try_get("delivered_at").map_err(get_error)?)?;
        let outcome = match row
            .try_get::<String, _>("outcome")
            .map_err(get_error)?
            .as_str()
        {
            "requested" => ChildCancellationOutcome::Requested,
            "already_requested" => ChildCancellationOutcome::AlreadyRequested,
            "terminal" => ChildCancellationOutcome::Terminal,
            _ => return Err(StoreError::corrupt("child cancellation receipt outcome")),
        };
        let current = child.child().run();
        if row.try_get::<Uuid, _>("child_run_id").map_err(get_error)? != *child_id.as_uuid()
            || captured.provenance() != current.lifecycle().provenance()
            || captured.revision() > current.lifecycle().revision()
            || captured.changed_at() > receipt_head.recorded_at()
            || current
                .journal_head()
                .is_none_or(|value| value.sequence() < receipt_head.sequence())
            || delivered != Some(to_database_time(delivered_at)?)
            || delivered_at < queued_at
            || delivered_at < receipt_head.recorded_at()
        {
            return Err(StoreError::corrupt("child cancellation receipt binding"));
        }
        match outcome {
            ChildCancellationOutcome::Terminal => {
                if !captured.status().is_terminal()
                    || encode_lifecycle(&captured)? != encode_lifecycle(current.lifecycle())?
                {
                    return Err(StoreError::corrupt("child cancellation terminal receipt"));
                }
            }
            ChildCancellationOutcome::Requested | ChildCancellationOutcome::AlreadyRequested => {
                if captured.status() != RunStatus::CancellationRequested
                    || !cancellation_matches(&captured, current.lifecycle())?
                {
                    return Err(StoreError::corrupt("child cancellation request receipt"));
                }
                if outcome == ChildCancellationOutcome::Requested {
                    let request = captured
                        .cancellation_request()
                        .ok_or(StoreError::ChildRunRejected)?;
                    if request.failure().caused_by_event_id() != Some(receipt_head.event_id())
                        || event.payload().kind().as_str() != "child-run-cancellation-requested"
                        || !matches!(event.source(), JournalEventSource::ControlPlane)
                    {
                        return Err(StoreError::corrupt("child cancellation delivered event"));
                    }
                    let previous = captured
                        .revision()
                        .get()
                        .checked_sub(1)
                        .ok_or(StoreError::ChildRunRejected)?;
                    let transition = RunProjection::transition(
                        RunRevision::new(previous),
                        RunTransition::RequestCancellation {
                            request: request.clone(),
                        },
                    );
                    let abandonments = load_wait_abandonment_set_or_empty(tx, &event).await?;
                    if abandonments
                        .iter()
                        .any(|value| value.reason() != WaitAbandonmentReason::RunCancellation)
                    {
                        return Err(StoreError::corrupt("child cancellation wait reason"));
                    }
                    let waits = abandonments
                        .into_iter()
                        .map(|value| value.wait().clone())
                        .collect::<Vec<_>>();
                    let expected = if waits.is_empty() {
                        projection_digest(&transition)?
                    } else {
                        wait_abandonment_projection_digest(&transition, &waits)?
                    };
                    if projection != Some(expected) {
                        return Err(StoreError::corrupt("child cancellation request projection"));
                    }
                }
            }
        }
        Some(ChildCancellationReceipt {
            outcome,
            lifecycle: captured,
            head: receipt_head,
            delivered_at,
        })
    } else {
        None
    };
    Ok(ChildCancellationRecord {
        key: key.clone(),
        child_run_id: child_id,
        parent_lifecycle: lifecycle,
        parent_head: head,
        queued_at,
        receipt,
    })
}

pub(super) async fn verify_schema(pool: &PgPool) -> Result<(), StoreError> {
    let complete = query_scalar::<_, bool>(r"SELECT
      NOT EXISTS (SELECT 1 FROM (VALUES
        ('runs_child_cancellation_capture','stateknot.runs','stateknot.capture_child_run_cancellation()',17),
        ('runs_child_cancellation_claim_guard','stateknot.runs','stateknot.guard_child_cancellation_claim()',19),
        ('child_cancellations_immutable','stateknot.child_run_cancellations','stateknot.guard_child_cancellation_evidence()',27),
        ('child_cancellation_receipts_immutable','stateknot.child_run_cancellation_receipts','stateknot.guard_child_cancellation_evidence()',27)
      ) AS expected(name,relation,function,mask) WHERE NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname=expected.name AND tgrelid=to_regclass(expected.relation) AND tgfoid=to_regprocedure(expected.function) AND tgtype=expected.mask AND tgenabled='O' AND NOT tgisinternal))
      AND NOT EXISTS (SELECT 1 FROM (VALUES
        ('child_run_cancellations_owner_fk','stateknot.child_run_cancellations','FOREIGN KEY (tenant_id, parent_run_id, key_digest, child_run_id) REFERENCES stateknot.child_run_ownership(tenant_id, parent_run_id, key_digest, child_run_id) ON DELETE RESTRICT'),
        ('child_run_cancellations_event_fk','stateknot.child_run_cancellations','FOREIGN KEY (tenant_id, parent_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest) REFERENCES stateknot.run_events(tenant_id, run_id, sequence, event_id, recorded_at, event_digest) ON DELETE RESTRICT'),
        ('child_run_cancellation_receipts_source_fk','stateknot.child_run_cancellation_receipts','FOREIGN KEY (tenant_id, parent_run_id, key_digest, child_run_id) REFERENCES stateknot.child_run_cancellations(tenant_id, parent_run_id, key_digest, child_run_id) ON DELETE RESTRICT'),
        ('child_run_cancellation_receipts_event_fk','stateknot.child_run_cancellation_receipts','FOREIGN KEY (tenant_id, child_run_id, journal_sequence, journal_event_id, journal_recorded_at, journal_digest) REFERENCES stateknot.run_events(tenant_id, run_id, sequence, event_id, recorded_at, event_digest) ON DELETE RESTRICT'),
        ('child_run_cancellations_pkey','stateknot.child_run_cancellations','PRIMARY KEY (tenant_id, parent_run_id, key_digest)'),
        ('child_run_cancellations_child_unique','stateknot.child_run_cancellations','UNIQUE (tenant_id, child_run_id)'),
        ('child_run_cancellations_exact_unique','stateknot.child_run_cancellations','UNIQUE (tenant_id, parent_run_id, key_digest, child_run_id)'),
        ('child_run_cancellation_receipts_pkey','stateknot.child_run_cancellation_receipts','PRIMARY KEY (tenant_id, parent_run_id, key_digest)'),
        ('child_run_cancellation_receipts_child_unique','stateknot.child_run_cancellation_receipts','UNIQUE (tenant_id, child_run_id)'),
        ('child_run_cancellations_check','stateknot.child_run_cancellations','CHECK ((lifecycle_digest = sha256(lifecycle_bytes)))'),
        ('child_run_cancellation_receipts_check','stateknot.child_run_cancellation_receipts','CHECK ((lifecycle_digest = sha256(lifecycle_bytes)))'),
        ('child_run_cancellations_key_digest_check','stateknot.child_run_cancellations','CHECK ((octet_length(key_digest) = 32))'),
        ('child_run_cancellations_lifecycle_bytes_check','stateknot.child_run_cancellations','CHECK (((octet_length(lifecycle_bytes) >= 1) AND (octet_length(lifecycle_bytes) <= 4194304)))'),
        ('child_run_cancellation_receipts_lifecycle_bytes_check','stateknot.child_run_cancellation_receipts','CHECK (((octet_length(lifecycle_bytes) >= 1) AND (octet_length(lifecycle_bytes) <= 4194304)))'),
        ('child_run_cancellation_receipts_outcome_check','stateknot.child_run_cancellation_receipts','CHECK ((outcome = ANY (ARRAY[''requested''::text, ''already_requested''::text, ''terminal''::text])))'),
        ('child_run_cancellations_clock_valid','stateknot.child_run_cancellations','CHECK (((delivered_at IS NULL) OR (delivered_at >= queued_at)))')
      ) AS expected(name,relation,definition) WHERE NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname=expected.name AND conrelid=to_regclass(expected.relation) AND convalidated AND NOT condeferrable AND pg_get_constraintdef(oid)=expected.definition))
      AND EXISTS (SELECT 1 FROM pg_index WHERE indexrelid=to_regclass('stateknot.child_run_cancellations_pending') AND indrelid=to_regclass('stateknot.child_run_cancellations') AND indisvalid AND indisready AND indislive AND pg_get_indexdef(indexrelid)='CREATE INDEX child_run_cancellations_pending ON stateknot.child_run_cancellations USING btree (tenant_id, queued_at, child_run_id) WHERE (delivered_at IS NULL)')")
        .fetch_one(pool).await.map_err(|source| StoreError::database("child cancellation schema", source))?;
    if !complete {
        return Err(StoreError::IncompleteSchema);
    }
    for function in include_str!("../migrations/0021_child_run_cancellation.sql")
        .split("CREATE FUNCTION stateknot.")
        .skip(1)
    {
        let (name, _) = function
            .split_once("() RETURNS trigger")
            .ok_or(StoreError::IncompleteSchema)?;
        let body = function
            .split("$$")
            .nth(1)
            .ok_or(StoreError::IncompleteSchema)?;
        let installed = query_scalar::<_, String>("SELECT prosrc FROM pg_proc JOIN pg_language ON pg_language.oid=prolang WHERE pg_proc.oid=to_regprocedure($1) AND NOT prosecdef AND lanname='plpgsql'")
            .bind(format!("stateknot.{name}()")).fetch_optional(pool).await.map_err(|source| StoreError::database("child cancellation guard", source))?;
        if installed.as_deref() != Some(body) {
            return Err(StoreError::IncompleteSchema);
        }
    }
    Ok(())
}
