// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Dedicated, sealed child Join storage. No user code runs in a transaction.

#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{ChildRunJoinBinding, ChildRunJoinHead, ChildRunJoinRequest};

/// Verified registration and optional publication/consumption, without copying
/// child output payloads. Publication is not parent result consumption.
#[derive(Clone, Debug)]
pub struct ChildJoinRecord {
    request: ChildRunJoinRequest,
    registration: JournalEvent,
    binding: Option<ChildRunJoinBinding>,
    head: Option<ChildRunJoinHead>,
    consumed: Option<JournalHead>,
}
impl ChildJoinRecord {
    /// Returns the complete sealed set in canonical slot order.
    #[must_use]
    pub const fn request(&self) -> &ChildRunJoinRequest {
        &self.request
    }
    /// Returns the event committed atomically with parent lease release.
    #[must_use]
    pub const fn registration(&self) -> &JournalEvent {
        &self.registration
    }
    /// Returns immutable complete terminal evidence, if published.
    #[must_use]
    pub const fn binding(&self) -> Option<&ChildRunJoinBinding> {
        self.binding.as_ref()
    }
    /// Bind this exact head to the pending parent result; reading is not consumption.
    #[must_use]
    pub const fn head(&self) -> Option<&ChildRunJoinHead> {
        self.head.as_ref()
    }
    /// Returns the exact pending-result event that consumed publication once.
    #[must_use]
    pub const fn consumed(&self) -> Option<&JournalHead> {
        self.consumed.as_ref()
    }
}

/// Both outcomes return original identities; retry candidates cannot replace evidence.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ChildJoinCommitOutcome {
    /// Fresh atomic registration or publication.
    Committed(ChildJoinRecord),
    /// Original fact recovered before fresh fence/head/lifecycle checks.
    Idempotent(ChildJoinRecord),
}
impl ChildJoinCommitOutcome {
    /// Returns verified original evidence for either outcome.
    #[must_use]
    pub const fn record(&self) -> &ChildJoinRecord {
        match self {
            Self::Committed(record) | Self::Idempotent(record) => record,
        }
    }
}

impl PostgresStore {
    /// Seals ALL children owned by one exact current activation, journals the
    /// registration, and releases the parent lease in the SAME transaction.
    ///
    /// Requires an unfinished live physical start and exclusive ownership of the
    /// Run execution boundary: no other unfinished physical node in the current
    /// fence may exist. The original attempt remains executing, not a fabricated
    /// failure/completion. Higher-fence recovery may re-enter the logical node.
    /// No new child slot can be added after registration. The caller must stop
    /// dispatching on the released fence and arrange bounded publication scans.
    /// Cancellation bypasses this successful-wait gate, retaining all evidence.
    #[allow(clippy::too_many_lines)]
    pub async fn register_child_join(
        &self,
        request: ChildRunJoinRequest,
        node: &NodeAttemptStartHead,
        append: JournalAppend,
    ) -> Result<ChildJoinCommitOutcome, StoreError> {
        validate_append(&request, &append, "child-join-registered")?;
        let activation = request.activation();
        let mut tx = self.begin_mutation("child Join registration").await?;
        lock_parent(&mut tx, activation).await?;
        if let Some(record) = load(&mut tx, activation, true).await? {
            if record.request() != &request {
                return Err(StoreError::ChildJoinRejected);
            }
            tx.commit()
                .await
                .map_err(|source| StoreError::database("child Join retry", source))?;
            return Ok(ChildJoinCommitOutcome::Idempotent(record));
        }
        let parent = decode_run(
            fetch_locked_run_row(&mut tx, activation.tenant_id(), activation.run_id()).await?,
        )?;
        let at = child_runs::authorize_parent_append(&mut tx, &parent, &append, true).await?;
        if node.activation() != activation || append.worker_fence() != Some(node.fence()) {
            return Err(StoreError::ChildJoinRejected);
        }
        let checkpoint = load_locked_current_checkpoint(
            &mut tx,
            &parent,
            activation.tenant_id(),
            activation.run_id(),
        )
        .await?
        .ok_or(StoreError::StaleCheckpointHead)?;
        if checkpoint.head() != *activation.base_checkpoint()
            || !node_attempt_activation_is_ready(&checkpoint, activation)
        {
            return Err(StoreError::StaleCheckpointHead);
        }
        let attempt = load_locked_node_attempt(&mut tx, node).await?;
        verify_node_attempt_start(&mut tx, attempt.start()).await?;
        if attempt.start().head() != *node
            || attempt.completion().is_some()
            || load_pending_node_result_row(&mut tx, activation)
                .await?
                .is_some()
        {
            return Err(StoreError::ChildJoinRejected);
        }
        let overlapping = query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM stateknot.node_attempts n WHERE n.tenant_id=$1 AND n.run_id=$2 AND n.fence_attempt_id=$3 AND n.attempt_id<>$4 AND NOT EXISTS (SELECT 1 FROM stateknot.node_attempt_completions c WHERE c.tenant_id=n.tenant_id AND c.run_id=n.run_id AND c.attempt_id=n.attempt_id))")
            .bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid())
            .bind(*node.fence().attempt_id().as_uuid()).bind(*node.attempt_id().as_uuid())
            .fetch_one(&mut *tx).await.map_err(|source| StoreError::database("child Join exclusive boundary", source))?;
        if overlapping {
            return Err(StoreError::ChildJoinRejected);
        }
        ensure_no_unsettled_tool_invocations(&mut tx, &checkpoint).await?;
        ensure_no_unsettled_model_invocations(&mut tx, &checkpoint).await?;
        verify_members(&mut tx, &request, true).await?;
        let event =
            JournalEvent::commit(append, at).map_err(|error| map_event_commit_error(&error))?;
        insert_event(
            &mut tx,
            &event,
            projection("registration", request.digest(), node.digest(), &event)?,
        )
        .await?;
        // The worker append checks the live fence before the same transaction
        // installs the wait gate and clears that lease.
        update_run_head(&mut tx, &event, None).await?;
        let bytes = request
            .canonical_bytes()
            .map_err(|_| StoreError::ChildJoinRejected)?;
        query("INSERT INTO stateknot.child_run_joins (tenant_id,parent_run_id,activation_digest,request_digest,request_bytes,request_checksum,base_checkpoint_id,graph_namespace,node_id,node_attempt_id,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)")
            .bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid())
            .bind(request.activation_digest().as_bytes()).bind(request.digest().as_bytes())
            .bind(&bytes).bind(Digest::sha256(&bytes).as_bytes())
            .bind(*activation.base_checkpoint().checkpoint_id().as_uuid()).bind(activation.graph_namespace().as_str()).bind(activation.node_id().as_str())
            .bind(*node.attempt_id().as_uuid()).bind(sequence(&event.head())?).bind(*event.event_id().as_uuid())
            .bind(to_database_time(at)?).bind(event.digest().as_bytes())
            .execute(&mut *tx).await.map_err(|source| StoreError::database("child Join insert", source))?;
        // Check lease expiry again at the write, not only before evidence reads.
        let released = query("UPDATE stateknot.runs SET lease_attempt_id=NULL,lease_acquired_at=NULL,lease_renewed_at=NULL,lease_expires_at=NULL,scheduler_not_before=NULL,updated_at=$5 WHERE tenant_id=$1 AND run_id=$2 AND lease_attempt_id=$3 AND fencing_epoch=$4 AND lease_expires_at>clock_timestamp()")
            .bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid())
            .bind(*node.fence().attempt_id().as_uuid()).bind(i64::try_from(node.fence().epoch().get()).map_err(|_| StoreError::StaleFence)?)
            .bind(to_database_time(at)?).execute(&mut *tx).await.map_err(|source| StoreError::database("child Join lease release", source))?.rows_affected();
        if released != 1 {
            return Err(StoreError::LeaseExpired);
        }
        let record = load(&mut tx, activation, true)
            .await?
            .ok_or(StoreError::ChildJoinRejected)?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child Join register commit", source))?;
        Ok(ChildJoinCommitOutcome::Committed(record))
    }

    /// Publishes one immutable complete terminal binding and makes an Active
    /// parent claimable. Requires every sealed child's verified priced settlement.
    /// No child outputs are merged, no budgets are charged, and no result is
    /// consumed here. Registration before/after completion uses the same durable
    /// discovery predicate: there is no destructive notification dequeue.
    pub async fn publish_child_join(
        &self,
        request: &ChildRunJoinRequest,
        append: JournalAppend,
    ) -> Result<ChildJoinCommitOutcome, StoreError> {
        validate_append(request, &append, "child-join-published")?;
        if append.worker_fence().is_some() {
            return Err(StoreError::WrongAppendAuthority);
        }
        let activation = request.activation();
        let mut tx = self.begin_mutation("child Join publication").await?;
        lock_parent(&mut tx, activation).await?;
        let record = load(&mut tx, activation, true)
            .await?
            .ok_or(StoreError::ChildJoinRejected)?;
        if record.request() != request {
            return Err(StoreError::ChildJoinRejected);
        }
        if record.binding().is_some() {
            tx.commit()
                .await
                .map_err(|source| StoreError::database("child Join publish retry", source))?;
            return Ok(ChildJoinCommitOutcome::Idempotent(record));
        }
        let parent = decode_run(
            fetch_locked_run_row(&mut tx, activation.tenant_id(), activation.run_id()).await?,
        )?;
        if parent.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::RunNotRunnable);
        }
        if failure_closes::exists(&mut tx, activation.tenant_id(), activation.run_id()).await? {
            return Err(StoreError::RunFailureClosing);
        }
        let at = child_runs::authorize_parent_append(&mut tx, &parent, &append, false).await?;
        let terminals = verify_members(&mut tx, request, true)
            .await?
            .ok_or(StoreError::ChildRunSettlementUnavailable)?;
        let binding = ChildRunJoinBinding::new(request.clone(), terminals)
            .map_err(|_| StoreError::ChildJoinRejected)?;
        let at = binding
            .terminals()
            .iter()
            .fold(at, |clock, value| clock.max(value.terminal().recorded_at()));
        let event =
            JournalEvent::commit(append, at).map_err(|error| map_event_commit_error(&error))?;
        let head = binding
            .head(event.head())
            .map_err(|_| StoreError::ChildJoinRejected)?;
        insert_event(
            &mut tx,
            &event,
            projection("publication", request.digest(), binding.digest(), &event)?,
        )
        .await?;
        let bytes = binding
            .canonical_bytes()
            .map_err(|_| StoreError::ChildJoinRejected)?;
        query("INSERT INTO stateknot.child_run_join_bindings (tenant_id,parent_run_id,activation_digest,binding_bytes,binding_checksum,head_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest,ready_at) SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,GREATEST(clock_timestamp(),queued_at) FROM stateknot.child_run_joins WHERE tenant_id=$1 AND parent_run_id=$2 AND activation_digest=$3")
            .bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid()).bind(request.activation_digest().as_bytes())
            .bind(&bytes).bind(Digest::sha256(&bytes).as_bytes()).bind(canonical(&head)?)
            .bind(sequence(&event.head())?).bind(*event.event_id().as_uuid()).bind(to_database_time(at)?).bind(event.digest().as_bytes())
            .execute(&mut *tx).await.map_err(|source| StoreError::database("child Join binding insert", source))?;
        query("UPDATE stateknot.child_run_joins j SET ready_at=b.ready_at FROM stateknot.child_run_join_bindings b WHERE j.tenant_id=$1 AND j.parent_run_id=$2 AND j.activation_digest=$3 AND b.tenant_id=j.tenant_id AND b.parent_run_id=j.parent_run_id AND b.activation_digest=j.activation_digest")
            .bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid()).bind(request.activation_digest().as_bytes())
            .execute(&mut *tx).await.map_err(|source| StoreError::database("child Join ready projection", source))?;
        update_run_head(&mut tx, &event, None).await?;
        query("UPDATE stateknot.runs SET scheduler_ready_at=GREATEST(clock_timestamp(),$3),scheduler_not_before=NULL WHERE tenant_id=$1 AND run_id=$2")
            .bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid()).bind(to_database_time(at)?)
            .execute(&mut *tx).await.map_err(|source| StoreError::database("child Join scheduler wakeup", source))?;
        let record = load(&mut tx, activation, true)
            .await?
            .ok_or(StoreError::ChildJoinRejected)?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child Join publication commit", source))?;
        Ok(ChildJoinCommitOutcome::Committed(record))
    }

    /// Authenticated caller scope is required. Reads a consistent snapshot and
    /// verifies canonical identity, physical start, membership, terminal evidence,
    /// publication and optional result consumption. Returns no child outputs.
    pub async fn load_child_join(
        &self,
        activation: &NodeActivation,
    ) -> Result<Option<ChildJoinRecord>, StoreError> {
        let mut tx = self.begin_repeatable_read("child Join read").await?;
        let record = load(&mut tx, activation, false).await?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child Join read commit", source))?;
        Ok(record)
    }

    /// At most 16 registered, unquarantined Active parent joins whose children
    /// are all settled. Retain the last request as an in-process continuation;
    /// after a full sweep or restart begin with `None`. Continue past item errors.
    /// Completed cursor rows are retained; a cursor is not a permanent watermark.
    pub async fn pending_child_joins_after(
        &self,
        tenant: &TenantId,
        after: Option<&ChildRunJoinRequest>,
    ) -> Result<Vec<ChildRunJoinRequest>, StoreError> {
        let mut tx = self.begin_repeatable_read("child Join discovery").await?;
        let mut cursor = None;
        if let Some(after) = after {
            if after.activation().tenant_id() != tenant {
                return Err(StoreError::ChildJoinRejected);
            }
            cursor = query_as::<_, (DateTime<Utc>, Uuid, Vec<u8>)>("SELECT queued_at,parent_run_id,activation_digest FROM stateknot.child_run_joins WHERE tenant_id=$1 AND parent_run_id=$2 AND request_digest=$3")
                .bind(tenant.as_str()).bind(*after.activation().run_id().as_uuid()).bind(after.digest().as_bytes())
                .fetch_optional(&mut *tx).await.map_err(|source| StoreError::database("child Join cursor", source))?;
            if cursor.is_none() {
                return Err(StoreError::ChildJoinRejected);
            }
        }
        let rows = query_scalar::<_, Vec<u8>>("SELECT j.request_bytes FROM stateknot.child_run_joins j JOIN stateknot.runs r ON r.tenant_id=j.tenant_id AND r.run_id=j.parent_run_id WHERE j.tenant_id=$1 AND j.ready_at IS NULL AND r.lifecycle_status='active' AND r.quarantined_at IS NULL AND ($2::timestamptz IS NULL OR (j.queued_at,j.parent_run_id,j.activation_digest)>($2,$3,$4)) AND NOT EXISTS (SELECT 1 FROM stateknot.child_run_ownership o WHERE o.tenant_id=j.tenant_id AND o.parent_run_id=j.parent_run_id AND o.activation_digest=j.activation_digest AND NOT o.settled) ORDER BY j.queued_at,j.parent_run_id,j.activation_digest LIMIT 16")
            .bind(tenant.as_str()).bind(cursor.as_ref().map(|value| value.0)).bind(cursor.as_ref().map(|value| value.1)).bind(cursor.as_ref().map(|value| &value.2))
            .fetch_all(&mut *tx).await.map_err(|source| StoreError::database("child Join discovery", source))?;
        let requests = rows
            .iter()
            .map(|bytes| decode::<ChildRunJoinRequest>(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child Join discovery commit", source))?;
        Ok(requests)
    }
}

fn validate_append(
    request: &ChildRunJoinRequest,
    append: &JournalAppend,
    kind: &str,
) -> Result<(), StoreError> {
    if append.intent().tenant_id() != request.activation().tenant_id()
        || append.intent().run_id() != request.activation().run_id()
        || append.intent().payload().kind().as_str() != kind
    {
        return Err(StoreError::ChildJoinRejected);
    }
    Ok(())
}
async fn lock_parent(
    tx: &mut Transaction<'_, Postgres>,
    activation: &NodeActivation,
) -> Result<(), StoreError> {
    let ancestors = child_runs::ancestry(tx, activation.tenant_id(), activation.run_id()).await?;
    child_runs::lock_tree(tx, activation.tenant_id(), ancestors[0]).await?;
    fetch_locked_run_row(tx, activation.tenant_id(), activation.run_id()).await?;
    Ok(())
}
fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    serde_json_canonicalizer::to_vec(value).map_err(|_| StoreError::encoding("child Join bytes"))
}
fn decode<T: serde::de::DeserializeOwned + Serialize>(bytes: &[u8]) -> Result<T, StoreError> {
    if bytes.is_empty() || bytes.len() > ChildRunJoinRequest::MAX_BYTES {
        return Err(StoreError::corrupt("child Join size"));
    }
    let value: T =
        serde_json::from_slice(bytes).map_err(|_| StoreError::corrupt("child Join value"))?;
    if canonical(&value)? != bytes {
        return Err(StoreError::corrupt("child Join canonical bytes"));
    }
    Ok(value)
}
fn sequence(head: &JournalHead) -> Result<i64, StoreError> {
    i64::try_from(head.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)
}
fn projection(
    domain: &str,
    request: Digest,
    evidence: Digest,
    event: &JournalEvent,
) -> Result<Digest, StoreError> {
    Ok(Digest::sha256(canonical(&(
        "stateknot.child-join.v1",
        domain,
        request,
        evidence,
        event.intent_digest(),
    ))?))
}

/// Verifies the COMPLETE admitted membership, not just the supplied subset.
/// Child rows are read one at a time; outputs are not retained in the result.
async fn verify_members(
    tx: &mut Transaction<'_, Postgres>,
    request: &ChildRunJoinRequest,
    lock: bool,
) -> Result<Option<Vec<stateknot_core::ChildRunBudgetSettlement>>, StoreError> {
    Box::pin(verify_members_inner(tx, request, lock)).await
}

async fn verify_members_inner(
    tx: &mut Transaction<'_, Postgres>,
    request: &ChildRunJoinRequest,
    lock: bool,
) -> Result<Option<Vec<stateknot_core::ChildRunBudgetSettlement>>, StoreError> {
    let a = request.activation();
    let keys = query_scalar::<_, Vec<u8>>("SELECT key_bytes FROM stateknot.child_run_ownership WHERE tenant_id=$1 AND parent_run_id=$2 AND activation_digest=$3 ORDER BY child_slot COLLATE \"C\" LIMIT 65")
        .bind(a.tenant_id().as_str()).bind(*a.run_id().as_uuid()).bind(request.activation_digest().as_bytes())
        .fetch_all(&mut **tx).await.map_err(|source| StoreError::database("child Join membership", source))?;
    if keys.len() != request.keys().len() {
        return Err(StoreError::ChildJoinRejected);
    }
    let mut terminals = Vec::new();
    for (bytes, key) in keys.iter().zip(request.keys()) {
        if *bytes != canonical(key)? {
            return Err(StoreError::ChildJoinRejected);
        }
        let child = Box::pin(child_runs::load_record_inner(tx, key, lock))
            .await?
            .ok_or(StoreError::ChildRunNotFound)?;
        if lock && child.child().run().is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if let Some(terminal) = child.settlement() {
            terminals.push(terminal.clone());
        }
    }
    Ok((terminals.len() == keys.len()).then_some(terminals))
}

async fn load(
    tx: &mut Transaction<'_, Postgres>,
    activation: &NodeActivation,
    lock_children: bool,
) -> Result<Option<ChildJoinRecord>, StoreError> {
    Box::pin(load_inner(tx, activation, lock_children)).await
}

#[allow(clippy::too_many_lines)]
async fn load_inner(
    tx: &mut Transaction<'_, Postgres>,
    activation: &NodeActivation,
    lock_children: bool,
) -> Result<Option<ChildJoinRecord>, StoreError> {
    let row = query("SELECT * FROM stateknot.child_run_joins WHERE tenant_id=$1 AND parent_run_id=$2 AND base_checkpoint_id=$3 AND graph_namespace=$4 AND node_id=$5")
        .bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid()).bind(*activation.base_checkpoint().checkpoint_id().as_uuid())
        .bind(activation.graph_namespace().as_str()).bind(activation.node_id().as_str())
        .fetch_optional(&mut **tx).await.map_err(|source| StoreError::database("child Join load", source))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let err = |source| StoreError::database("child Join projection", source);
    let bytes: Vec<u8> = row.try_get("request_bytes").map_err(err)?;
    let request: ChildRunJoinRequest = decode(&bytes)?;
    if request.activation() != activation {
        return Err(StoreError::ChildJoinRejected);
    }
    if request.activation_digest()
        != decode_digest(
            &row.try_get::<Vec<u8>, _>("activation_digest")
                .map_err(err)?,
            "child Join activation",
        )?
        || request.digest()
            != decode_digest(
                &row.try_get::<Vec<u8>, _>("request_digest").map_err(err)?,
                "child Join request",
            )?
        || Digest::sha256(&bytes)
            != decode_digest(
                &row.try_get::<Vec<u8>, _>("request_checksum").map_err(err)?,
                "child Join checksum",
            )?
    {
        return Err(StoreError::corrupt("child Join identity"));
    }
    let id = AttemptId::from_uuid(row.try_get("node_attempt_id").map_err(err)?)
        .map_err(|_| StoreError::corrupt("child Join node"))?;
    let node = load_node_attempt_record(tx, activation.tenant_id(), &activation.run_id(), id)
        .await?
        .ok_or_else(|| StoreError::corrupt("child Join node"))?;
    verify_node_attempt_start(tx, node.start()).await?;
    let registration_head =
        child_runs::row_head(&row, activation.tenant_id(), activation.run_id())?;
    let (registration, registration_projection) = anchored_event(tx, &registration_head).await?;
    if node.start().activation() != activation
        || registration.source().worker_fence() != Some(node.start().fence())
        || registration.sequence() <= node.start().journal_head().sequence()
        || registration.recorded_at() < node.start().journal_head().recorded_at()
        || registration.payload().kind().as_str() != "child-join-registered"
        || registration_projection
            != Some(projection(
                "registration",
                request.digest(),
                node.start().digest(),
                &registration,
            )?)
    {
        return Err(StoreError::corrupt("child Join registration"));
    }
    let terminals =
        verify_members(tx, &request, lock_children)
            .await
            .map_err(|error| match error {
                StoreError::ChildJoinRejected | StoreError::ChildRunNotFound => {
                    StoreError::corrupt("child Join membership")
                }
                other => other,
            })?;
    let publication = query("SELECT * FROM stateknot.child_run_join_bindings WHERE tenant_id=$1 AND parent_run_id=$2 AND activation_digest=$3")
        .bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid()).bind(request.activation_digest().as_bytes())
        .fetch_optional(&mut **tx).await.map_err(|source| StoreError::database("child Join binding load", source))?;
    let mut record = ChildJoinRecord {
        request,
        registration,
        binding: None,
        head: None,
        consumed: None,
    };
    let ready_at: Option<DateTime<Utc>> = row.try_get("ready_at").map_err(err)?;
    if let Some(publication) = publication {
        let bytes: Vec<u8> = publication.try_get("binding_bytes").map_err(err)?;
        let binding: ChildRunJoinBinding = decode(&bytes)?;
        let expected = ChildRunJoinBinding::new(
            record.request.clone(),
            terminals.ok_or_else(|| StoreError::corrupt("child Join terminals missing"))?,
        )
        .map_err(|_| StoreError::corrupt("child Join terminals"))?;
        let head = child_runs::row_head(&publication, activation.tenant_id(), activation.run_id())?;
        let (event, digest) = anchored_event(tx, &head).await?;
        let join_head = binding
            .head(head.clone())
            .map_err(|_| StoreError::corrupt("child Join head"))?;
        if binding != expected
            || Digest::sha256(&bytes)
                != decode_digest(
                    &publication
                        .try_get::<Vec<u8>, _>("binding_checksum")
                        .map_err(err)?,
                    "child Join binding checksum",
                )?
            || publication
                .try_get::<Vec<u8>, _>("head_bytes")
                .map_err(err)?
                != canonical(&join_head)?
            || ready_at != Some(publication.try_get("ready_at").map_err(err)?)
            || event.source().worker_fence().is_some()
            || event.payload().kind().as_str() != "child-join-published"
            || event.sequence() <= record.registration.sequence()
            || event.recorded_at() < record.registration.recorded_at()
            || digest
                != Some(projection(
                    "publication",
                    record.request.digest(),
                    binding.digest(),
                    &event,
                )?)
        {
            return Err(StoreError::corrupt("child Join publication"));
        }
        record.binding = Some(binding);
        record.head = Some(join_head);
    } else if ready_at.is_some() {
        return Err(StoreError::corrupt("child Join ready projection"));
    }
    let consumption = query_as::<_, (i64,DateTime<Utc>)>("SELECT journal_sequence,consumed_at FROM stateknot.child_run_join_consumptions WHERE tenant_id=$1 AND parent_run_id=$2 AND activation_digest=$3")
        .bind(activation.tenant_id().as_str()).bind(*activation.run_id().as_uuid()).bind(record.request.activation_digest().as_bytes())
        .fetch_optional(&mut **tx).await.map_err(|source| StoreError::database("child Join consumption load", source))?;
    if let Some((seq, at)) = consumption {
        let result_row = load_pending_node_result_row(tx, activation)
            .await?
            .ok_or_else(|| StoreError::corrupt("child Join consumed result"))?;
        let result = decode_pending_node_result(&result_row)?;
        if record.head.is_none()
            || result.intent().child_join() != record.head.as_ref()
            || sequence(result.journal_head())? != seq
            || row
                .try_get::<Option<DateTime<Utc>>, _>("consumed_at")
                .map_err(err)?
                != Some(at)
        {
            return Err(StoreError::corrupt("child Join consumed result"));
        }
        // Authenticate start and completion without recursing result -> Join.
        let attempt_id = AttemptId::from_uuid(
            result_row
                .node_attempt_id
                .ok_or_else(|| StoreError::corrupt("child Join result physical start"))?,
        )
        .map_err(|_| StoreError::corrupt("child Join result physical start"))?;
        let attempt =
            load_node_attempt_record(tx, activation.tenant_id(), &activation.run_id(), attempt_id)
                .await?
                .ok_or_else(|| StoreError::corrupt("child Join result physical start"))?;
        verify_node_attempt_start(tx, attempt.start()).await?;
        let completion = attempt
            .completion()
            .ok_or_else(|| StoreError::corrupt("child Join result completion"))?;
        if !matches!(completion.outcome(), NodeAttemptOutcome::Succeeded { result: completed } if **completed == result.head())
        {
            return Err(StoreError::corrupt("child Join result completion"));
        }
        verify_node_attempt_anchor(
            tx,
            result.journal_head(),
            result.fence(),
            completion.digest(),
            "child Join result completion anchor",
        )
        .await?;
        record.consumed = Some(result.journal_head().clone());
    } else if row
        .try_get::<Option<DateTime<Utc>>, _>("consumed_at")
        .map_err(err)?
        .is_some()
    {
        return Err(StoreError::corrupt("child Join consumption projection"));
    }
    Ok(Some(record))
}

async fn anchored_event(
    tx: &mut Transaction<'_, Postgres>,
    head: &JournalHead,
) -> Result<(JournalEvent, Option<Digest>), StoreError> {
    let (event, projection) =
        child_runs::anchored_event(tx, head.tenant_id(), head.run_id(), sequence(head)?).await?;
    if event.head() != *head {
        return Err(StoreError::corrupt("child Join journal anchor"));
    }
    Ok((event, projection))
}

pub(super) async fn verify_result(
    tx: &mut Transaction<'_, Postgres>,
    result: &PendingNodeResult,
    committed: bool,
) -> Result<(), StoreError> {
    let a = result.intent().activation();
    // Fresh writes hold the parent row, not the tree. Refuse unpublished joins
    // without reading mutable child terminals under READ COMMITTED.
    let ready = query_scalar::<_, bool>("SELECT ready_at IS NOT NULL FROM stateknot.child_run_joins WHERE tenant_id=$1 AND parent_run_id=$2 AND base_checkpoint_id=$3 AND graph_namespace=$4 AND node_id=$5")
        .bind(a.tenant_id().as_str()).bind(*a.run_id().as_uuid()).bind(*a.base_checkpoint().checkpoint_id().as_uuid())
        .bind(a.graph_namespace().as_str()).bind(a.node_id().as_str()).fetch_optional(&mut **tx).await.map_err(|source| StoreError::database("child Join result readiness", source))?;
    if ready.is_none() && result.intent().child_join().is_none() {
        return Ok(());
    }
    if ready != Some(true) {
        return Err(if committed {
            StoreError::corrupt("missing or unpublished result child Join")
        } else {
            StoreError::ChildJoinRejected
        });
    }
    // Publication sealed membership and proved every child terminal. Fresh
    // consumption already holds the parent row; lock these terminal children
    // in canonical slot order so operator quarantine cannot race consumption.
    // Never acquire the tree lock after the parent. Terminal children cannot
    // spawn or settle outstanding descendants; their audit/quarantine writers
    // do not lock ancestors. Historical replay remains a nonlocking proof read.
    let record = load(tx, a, !committed).await?;
    match record {
        None if result.intent().child_join().is_none() => Ok(()),
        Some(record)
            if record.head().is_some()
                && result.intent().child_join() == record.head()
                && if committed {
                    record.consumed() == Some(result.journal_head())
                } else {
                    record.consumed().is_none()
                } =>
        {
            Ok(())
        }
        _ if committed => Err(StoreError::corrupt("pending result child Join")),
        _ => Err(StoreError::ChildJoinRejected),
    }
}

pub(super) async fn verify_schema(pool: &PgPool) -> Result<(), StoreError> {
    let installed = query_scalar::<_, String>(CATALOG_QUERY)
        .fetch_one(pool)
        .await
        .map_err(|source| StoreError::database("child Join catalog", source))?;
    let expected: serde_json::Value = serde_json::from_str(include_str!("child_join_catalog.json"))
        .map_err(|_| StoreError::IncompleteSchema)?;
    if serde_json::from_str::<serde_json::Value>(&installed)
        .map_err(|_| StoreError::IncompleteSchema)?
        != expected
    {
        return Err(StoreError::IncompleteSchema);
    }
    for function in include_str!("../migrations/0022_child_run_joins.sql")
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
        let installed = query_scalar::<_,String>("SELECT prosrc FROM pg_proc JOIN pg_language ON pg_language.oid=prolang WHERE pg_proc.oid=to_regprocedure($1) AND NOT prosecdef AND lanname='plpgsql'")
            .bind(format!("stateknot.{name}()")).fetch_optional(pool).await.map_err(|source| StoreError::database("child Join guard",source))?;
        if installed.as_deref() != Some(body) {
            return Err(StoreError::IncompleteSchema);
        }
    }
    Ok(())
}

// Full additive catalog, including nullability, exact checks/FKs/keys, live
// indexes and enabled trigger definitions. Function bodies are checked above.
const CATALOG_QUERY: &str = r"
WITH relations AS (SELECT oid,relname FROM pg_class WHERE relnamespace='stateknot'::regnamespace AND relname IN ('child_run_joins','child_run_join_bindings','child_run_join_consumptions'))
SELECT jsonb_build_object(
 'columns',(SELECT jsonb_agg(jsonb_build_array(r.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull) ORDER BY r.relname,a.attnum) FROM relations r JOIN pg_attribute a ON a.attrelid=r.oid WHERE a.attnum>0 AND NOT a.attisdropped),
 'constraints',(SELECT jsonb_agg(jsonb_build_array(r.relname,c.conname,pg_get_constraintdef(c.oid),c.convalidated,c.condeferrable) ORDER BY r.relname,c.conname) FROM relations r JOIN pg_constraint c ON c.conrelid=r.oid),
 'indexes',(SELECT jsonb_agg(jsonb_build_array(r.relname,pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indislive) ORDER BY r.relname,pg_get_indexdef(i.indexrelid)) FROM relations r JOIN pg_index i ON i.indrelid=r.oid),
 'triggers',(SELECT jsonb_agg(jsonb_build_array(t.tgname,pg_get_triggerdef(t.oid),t.tgenabled) ORDER BY t.tgname) FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgrelid IN (SELECT oid FROM relations) OR (t.tgrelid='stateknot.runs'::regclass AND t.tgname='runs_child_join_guard') OR (t.tgrelid='stateknot.child_run_ownership'::regclass AND t.tgname='child_join_spawn_guard') OR (t.tgrelid='stateknot.pending_node_results'::regclass AND t.tgname='pending_results_child_join_consume')))
)::text
";
