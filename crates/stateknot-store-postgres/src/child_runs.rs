// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Transactional local ownership. High-level child joins are a separate runtime boundary.

// This private module shares the store's transaction and integrity helpers.
#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{
    ChildRunAdmissionIntent, ChildRunBudgetAccount, ChildRunBudgetSettlement, ChildRunKey,
};

const OWNER_COLUMNS: &str = "tenant_id, parent_run_id, key_digest, key_bytes, child_slot, activation_digest, parent_node_attempt_id, spawn_digest, child_run_id, root_run_id, ancestors, intent_bytes, child_admission_digest, parent_checkpoint_id, parent_checkpoint_superstep, parent_checkpoint_digest, journal_sequence, journal_event_id, journal_recorded_at, journal_digest, settled, terminal_pending_at";

/// Fully verified ownership, the original child identities and committed spawn event.
#[derive(Clone, Debug)]
pub struct ChildRunRecord {
    intent: ChildRunAdmissionIntent,
    child: StoredAgentAdmission,
    spawn: JournalEvent,
    ancestors: Vec<RunId>,
    settlement: Option<ChildRunBudgetSettlement>,
}

impl ChildRunRecord {
    /// Returns the original immutable spawn intent, not replacement retry IDs.
    #[must_use]
    pub const fn intent(&self) -> &ChildRunAdmissionIntent {
        &self.intent
    }
    /// Returns the isolated child admission and current child run projection.
    #[must_use]
    pub const fn child(&self) -> &StoredAgentAdmission {
        &self.child
    }
    /// Returns the exact parent journal event committed with ownership/admission.
    #[must_use]
    pub const fn spawn(&self) -> &JournalEvent {
        &self.spawn
    }
    /// Returns root-to-parent ancestry, with at most 32 distinct identities.
    #[must_use]
    pub fn ancestors(&self) -> &[RunId] {
        &self.ancestors
    }
    /// Returns verified once-only accounting settlement, not a runtime Join.
    #[must_use]
    pub const fn settlement(&self) -> Option<&ChildRunBudgetSettlement> {
        self.settlement.as_ref()
    }
}

/// Result of atomic child ownership/admission/account reservation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ChildRunCommitOutcome {
    /// All ownership, admission, checkpoint, budget and audit facts committed together.
    Committed(ChildRunRecord),
    /// The original logical key/intent already committed, including after lease expiry.
    Idempotent(ChildRunRecord),
}
impl ChildRunCommitOutcome {
    /// Returns the original durable child record for either outcome.
    #[must_use]
    pub const fn record(&self) -> &ChildRunRecord {
        match self {
            Self::Committed(record) | Self::Idempotent(record) => record,
        }
    }
}

/// Result of replacing one reservation with immutable child terminal usage.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ChildRunSettlementOutcome {
    /// Exact terminal evidence and the new parent account committed atomically.
    Committed {
        /// Parent accounting audit event.
        event: JournalEvent,
        /// Original owned-child record with terminal settlement.
        record: ChildRunRecord,
    },
    /// The ownership already had this exact terminal settlement.
    Idempotent {
        /// Original accounting audit event.
        event: JournalEvent,
        /// Original owned-child record with terminal settlement.
        record: ChildRunRecord,
    },
}

impl PostgresStore {
    /// Atomically creates one fresh isolated child and reserves its complete budget.
    ///
    /// `direct_usage` is a trusted, complete DIRECT-only parent observation at the
    /// exact journal head in `parent_append`; it must exclude child subtree usage.
    /// Schema callbacks run before the mutation transaction. The transaction locks
    /// the tree coordinator, then ancestors root-to-parent, and rechecks readiness,
    /// a live node start/fence, immutable admission, all ancestor limits and budget.
    /// Parent model/tool invocations may not overlap outstanding child work.
    ///
    /// Same logical key and spawn digest recover the original child BEFORE fresh
    /// lease/deadline/readiness checks. Candidate audit/checkpoint/Run IDs cannot
    /// replace it. Existing arbitrary root runs cannot be attached as children.
    /// Trusted caller authentication/delegation policy remains a server boundary.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn admit_child_run<V: GraphSchemaValidator + ?Sized>(
        &self,
        intent: ChildRunAdmissionIntent,
        node: &NodeAttemptStartHead,
        parent_append: JournalAppend,
        child_append: JournalAppend,
        child_checkpoint: CheckpointWrite,
        direct_usage: BudgetUsage,
        schemas: &V,
    ) -> Result<ChildRunCommitOutcome, StoreError> {
        let key = intent.key();
        validate_agent_admission_commit_input(intent.child(), &child_append, &child_checkpoint)?;
        if parent_append.intent().tenant_id() != key.tenant_id()
            || parent_append.intent().run_id() != key.parent_run_id()
            || parent_append.intent().payload().kind().as_str() != "child-run-admitted"
            || child_checkpoint.state() != intent.initial_state()
            || child_checkpoint.ready_nodes() != intent.child_graph().entry_nodes()
        {
            return Err(StoreError::ChildRunRejected);
        }
        if let Some(record) = self.load_child_run_optional(key).await? {
            if record.intent.spawn_digest() != intent.spawn_digest() {
                return Err(StoreError::ChildRunConflict);
            }
            return Ok(ChildRunCommitOutcome::Idempotent(record));
        }
        let parent = self
            .load_agent_admission(key.tenant_id(), key.parent_run_id())
            .await?;
        let checkpoint = self
            .load_current_checkpoint(key.tenant_id(), key.parent_run_id())
            .await?
            .ok_or(StoreError::StaleCheckpointHead)?;
        let parent_graph = self
            .load_graph_definition(key.tenant_id(), parent.admission().intent().graph())
            .await?;
        let child_graph = self
            .load_graph_definition(key.tenant_id(), intent.child().graph())
            .await?;
        if child_graph.graph() != intent.child_graph() {
            return Err(StoreError::ChildRunRejected);
        }
        intent
            .validate_declaration(parent_graph.graph())
            .map_err(|_| StoreError::ChildRunRejected)?;
        let schema_clock = self.observe_database_clock().await?;
        catch_unwind(AssertUnwindSafe(|| {
            intent.validate_for(parent.admission(), &checkpoint, schemas, schema_clock)
        }))
        .map_err(|_| StoreError::AgentAdmissionSchemaUnavailable)?
        .map_err(|_| StoreError::ChildRunRejected)?;
        validate_agent_initial_checkpoint(child_graph.graph(), &child_checkpoint, schemas)?;

        let mut tx = self.begin_mutation("atomic child admission").await?;
        let ancestors = ancestry(&mut tx, key.tenant_id(), key.parent_run_id()).await?;
        lock_tree(&mut tx, key.tenant_id(), ancestors[0]).await?;
        if ancestry(&mut tx, key.tenant_id(), key.parent_run_id()).await? != ancestors {
            return Err(StoreError::corrupt("child ancestry changed"));
        }
        // All ancestors are locked before checking cancellation and live counts.
        for ancestor in &ancestors {
            fetch_locked_run_row(&mut tx, key.tenant_id(), *ancestor).await?;
        }
        if let Some(record) = load_record(&mut tx, key).await? {
            load_account(&mut tx, key.tenant_id(), key.parent_run_id()).await?;
            if record.intent.spawn_digest() != intent.spawn_digest() {
                return Err(StoreError::ChildRunConflict);
            }
            tx.commit()
                .await
                .map_err(|source| StoreError::database("child admission retry", source))?;
            return Ok(ChildRunCommitOutcome::Idempotent(record));
        }
        let stored =
            decode_run(fetch_locked_run_row(&mut tx, key.tenant_id(), key.parent_run_id()).await?)?;
        let parent_row = load_agent_admission_row(&mut tx, key.tenant_id(), key.parent_run_id())
            .await?
            .ok_or(StoreError::AgentAdmissionNotFound)?;
        let parent = verify_stored_agent_admission(&mut tx, stored, parent_row).await?;
        if parent.admission().digest() != intent.parent_admission_digest() {
            return Err(StoreError::ChildRunConflict);
        }
        let current = load_locked_current_checkpoint(
            &mut tx,
            parent.run(),
            key.tenant_id(),
            key.parent_run_id(),
        )
        .await?
        .ok_or(StoreError::StaleCheckpointHead)?;
        if current.head() != *key.parent().base_checkpoint()
            || !node_attempt_activation_is_ready(&current, key.parent())
        {
            return Err(StoreError::StaleCheckpointHead);
        }
        let observed_at =
            authorize_parent_append(&mut tx, parent.run(), &parent_append, true).await?;
        let fence = parent_append
            .worker_fence()
            .ok_or(StoreError::WrongAppendAuthority)?;
        if node.activation() != key.parent() || node.fence() != fence {
            return Err(StoreError::ChildRunRejected);
        }
        let attempt = load_node_attempt_record(
            &mut tx,
            key.tenant_id(),
            &key.parent_run_id(),
            node.attempt_id(),
        )
        .await?
        .ok_or(StoreError::ChildRunRejected)?;
        verify_node_attempt(&mut tx, &attempt).await?;
        if attempt.start().head() != *node || attempt.completion().is_some() {
            return Err(StoreError::ChildRunRejected);
        }
        ensure_no_unsettled_tool_invocations(&mut tx, &current).await?;
        ensure_no_unsettled_model_invocations(&mut tx, &current).await?;
        check_topology(&mut tx, key.tenant_id(), &ancestors).await?;
        let mut account = match load_account(&mut tx, key.tenant_id(), key.parent_run_id()).await? {
            Some(account) => account
                .observe_direct(
                    parent_append
                        .expectation()
                        .head()
                        .ok_or(StoreError::StaleJournalHead)?
                        .clone(),
                    direct_usage,
                )
                .map_err(|_| StoreError::ChildRunRejected)?,
            None => ChildRunBudgetAccount::new(
                parent.admission(),
                parent_graph.graph(),
                parent_append
                    .expectation()
                    .head()
                    .ok_or(StoreError::StaleJournalHead)?
                    .clone(),
                direct_usage,
            )
            .map_err(|_| StoreError::ChildRunRejected)?,
        };
        account = account
            .reserve(&intent, parent_graph.graph(), observed_at)
            .map_err(|_| StoreError::ChildRunRejected)?;
        let child_id = intent.child().provenance().run_id();
        // Reject existing identities without inverting admission-key/run lock order.
        let exists = query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2)",
        )
        .bind(key.tenant_id().as_str())
        .bind(*child_id.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(|source| StoreError::database("child fresh identity check", source))?;
        if exists {
            return Err(StoreError::ChildRunConflict);
        }
        if load_locked_agent_admission(&mut tx, key.tenant_id(), child_id)
            .await?
            .is_some()
        {
            return Err(StoreError::ChildRunConflict);
        }
        let child = match Box::pin(commit_new_agent_admission(
            &mut tx,
            intent.child().clone(),
            child_append,
            child_checkpoint,
        ))
        .await?
        {
            NewAgentAdmissionOutcome::Committed(child) => child,
            NewAgentAdmissionOutcome::Idempotent(_) => return Err(StoreError::ChildRunConflict),
        };
        let at = database_now(&mut tx, "child spawn clock")
            .await?
            .max(observed_at)
            .max(child.admission().admitted_at());
        account
            .remaining(at)
            .map_err(|_| StoreError::ChildRunRejected)?;
        let event = JournalEvent::commit(parent_append, at)
            .map_err(|error| map_event_commit_error(&error))?;
        insert_event(
            &mut tx,
            &event,
            spawn_projection(&intent, child.admission(), child.checkpoint(), &event)?,
        )
        .await?;
        insert_owner(
            &mut tx,
            &intent,
            attempt.start(),
            &ancestors,
            &child,
            &event,
        )
        .await?;
        save_account(&mut tx, &account, &event.head()).await?;
        mark_child_capability(&mut tx, key.tenant_id(), key.parent_run_id()).await?;
        mark_child_capability(&mut tx, key.tenant_id(), child_id).await?;
        update_run_head(&mut tx, &event, None).await?; // final live database-time fence
        let record = load_record(&mut tx, key)
            .await?
            .ok_or_else(|| StoreError::corrupt("new child ownership"))?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("atomic child admission commit", source))?;
        Ok(ChildRunCommitOutcome::Committed(record))
    }

    /// Loads fully verified ownership and original child identities within one
    /// repeatable-read snapshot. Callers authenticate read access before lookup.
    pub async fn load_child_run(&self, key: &ChildRunKey) -> Result<ChildRunRecord, StoreError> {
        self.load_child_run_optional(key)
            .await?
            .ok_or(StoreError::ChildRunNotFound)
    }

    async fn load_child_run_optional(
        &self,
        key: &ChildRunKey,
    ) -> Result<Option<ChildRunRecord>, StoreError> {
        let mut tx = self.begin_repeatable_read("child ownership read").await?;
        let result = Box::pin(load_record_inner(&mut tx, key, false)).await?;
        if result.is_some() {
            Box::pin(load_account_inner(
                &mut tx,
                key.tenant_id(),
                key.parent_run_id(),
                false,
            ))
            .await?;
        }
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child ownership read commit", source))?;
        Ok(result)
    }

    /// Returns a verified accounting projection, not a fresh direct-usage report.
    /// Invocation providers must add/subtract delegated charges exactly once.
    pub async fn load_child_budget_account(
        &self,
        tenant: &TenantId,
        parent: RunId,
    ) -> Result<Option<ChildRunBudgetAccount>, StoreError> {
        let mut tx = self.begin_repeatable_read("child budget read").await?;
        let account = Box::pin(load_account_inner(&mut tx, tenant, parent, false)).await?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child budget read commit", source))?;
        Ok(account)
    }

    /// Adds settled immediate-child subtree charges to complete DIRECT-only usage.
    /// Outstanding children fail closed. The eventual journal CAS and terminal
    /// mutation revalidate the account; this read alone never authorizes closure.
    pub async fn include_child_usage(
        &self,
        tenant: &TenantId,
        parent: RunId,
        direct: BudgetUsage,
    ) -> Result<BudgetUsage, StoreError> {
        let Some(account) = self.load_child_budget_account(tenant, parent).await? else {
            return Ok(direct);
        };
        ensure_settled(&account)?;
        direct
            .validate_monotonic_after(account.direct_usage())
            .map_err(|_| StoreError::IncompleteChildAccounting)?;
        direct
            .checked_accumulate(
                &account
                    .delegated_usage()
                    .map_err(|_| StoreError::IncompleteChildAccounting)?,
            )
            .map_err(|_| StoreError::IncompleteChildAccounting)
    }

    /// Lists at most 256 retained key/child identity pairs without materializing
    /// private input snapshots. The next exact record load verifies all evidence.
    pub async fn list_child_run_identities(
        &self,
        tenant: &TenantId,
        parent: RunId,
    ) -> Result<Vec<(Digest, RunId)>, StoreError> {
        let rows = query_as::<_, (Vec<u8>, Uuid)>("SELECT key_digest, child_run_id FROM stateknot.child_run_ownership WHERE tenant_id = $1 AND parent_run_id = $2 ORDER BY key_digest LIMIT 257")
            .bind(tenant.as_str()).bind(*parent.as_uuid()).fetch_all(&self.pool).await
            .map_err(|source| StoreError::database("child identity list", source))?;
        if rows.len() > 256 {
            return Err(StoreError::corrupt("child lifetime count"));
        }
        rows.into_iter()
            .map(|(digest, run)| {
                Ok((
                    decode_digest(&digest, "child key")?,
                    RunId::from_uuid(run).map_err(|_| StoreError::corrupt("child identity"))?,
                ))
            })
            .collect()
    }

    /// Discovers at most sixteen durable terminal notifications for reconciliation.
    /// Records remain discoverable until a successful settlement transaction;
    /// there is no cursor/notification race or destructive dequeue before commit.
    pub async fn pending_child_settlements(
        &self,
        tenant: &TenantId,
    ) -> Result<Vec<ChildRunKey>, StoreError> {
        self.pending_child_settlements_after(tenant, None).await
    }

    /// Continues a bounded notification sweep after its last returned key.
    /// The immutable terminal anchor retains the cursor after settlement. Skip
    /// unpriced/quarantined items and continue so they cannot block later work.
    /// Restart from None after each complete sweep: commit order need not equal
    /// timestamp order. This is discovery, never a permanent delivery watermark.
    pub async fn pending_child_settlements_after(
        &self,
        tenant: &TenantId,
        after: Option<&ChildRunKey>,
    ) -> Result<Vec<ChildRunKey>, StoreError> {
        let cursor = if let Some(key) = after {
            if key.tenant_id() != tenant {
                return Err(StoreError::ChildRunRejected);
            }
            Some(query_as::<_, (DateTime<Utc>, Uuid)>("SELECT terminal.journal_recorded_at, terminal.child_run_id FROM stateknot.child_run_ownership AS owner JOIN stateknot.child_run_terminals AS terminal ON terminal.tenant_id=owner.tenant_id AND terminal.child_run_id=owner.child_run_id WHERE owner.tenant_id=$1 AND owner.parent_run_id=$2 AND owner.key_digest=$3")
                .bind(tenant.as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes())
                .fetch_optional(&self.pool).await.map_err(|source| StoreError::database("child notification cursor", source))?
                .ok_or(StoreError::ChildRunNotFound)?)
        } else {
            None
        };
        // Separate query shapes keep the cursor in the btree range condition,
        // including PostgreSQL generic plans; an optional-parameter OR can scan
        // an arbitrarily long earlier prefix of unresolved notifications.
        let sql = if cursor.is_some() {
            "SELECT key_bytes, key_digest, parent_run_id FROM stateknot.child_run_ownership WHERE tenant_id = $1 AND terminal_pending_at IS NOT NULL AND NOT settled AND (terminal_pending_at, child_run_id) > ($2,$3) ORDER BY terminal_pending_at, child_run_id LIMIT 16"
        } else {
            "SELECT key_bytes, key_digest, parent_run_id FROM stateknot.child_run_ownership WHERE tenant_id = $1 AND terminal_pending_at IS NOT NULL AND NOT settled ORDER BY terminal_pending_at, child_run_id LIMIT 16"
        };
        let mut listing = query_as::<_, (Vec<u8>, Vec<u8>, Uuid)>(sql).bind(tenant.as_str());
        if let Some((at, child)) = cursor {
            listing = listing.bind(at).bind(child);
        }
        let rows = listing
            .fetch_all(&self.pool)
            .await
            .map_err(|source| StoreError::database("child settlement discovery", source))?;
        rows.into_iter()
            .map(|(bytes, digest, parent)| {
                let key: ChildRunKey = serde_json::from_slice(&bytes)
                    .map_err(|_| StoreError::corrupt("pending child key"))?;
                if key.tenant_id() != tenant
                    || bytes.len() > 65_536
                    || key.digest() != decode_digest(&digest, "pending child key digest")?
                    || *key.parent_run_id().as_uuid() != parent
                {
                    return Err(StoreError::corrupt("pending child key scope"));
                }
                Ok(key)
            })
            .collect()
    }

    /// Settles one child's complete immutable terminal evidence exactly once.
    ///
    /// No external code runs under the transaction. It locks the tree, parent,
    /// then child; child terminal commits never lock parents. Control-plane or
    /// currently fenced worker audit writes are accepted. Unpriced outcomes stay
    /// outstanding and discoverable. This accounting operation is not a Join.
    #[allow(clippy::too_many_lines)]
    pub async fn settle_child_run(
        &self,
        key: &ChildRunKey,
        append: JournalAppend,
    ) -> Result<ChildRunSettlementOutcome, StoreError> {
        if append.intent().tenant_id() != key.tenant_id()
            || append.intent().run_id() != key.parent_run_id()
            || append.intent().payload().kind().as_str() != "child-run-settled"
        {
            return Err(StoreError::ChildRunRejected);
        }
        let mut tx = self.begin_mutation("child settlement").await?;
        let ancestors = ancestry(&mut tx, key.tenant_id(), key.parent_run_id()).await?;
        lock_tree(&mut tx, key.tenant_id(), ancestors[0]).await?;
        let parent =
            decode_run(fetch_locked_run_row(&mut tx, key.tenant_id(), key.parent_run_id()).await?)?;
        let record = load_record(&mut tx, key)
            .await?
            .ok_or(StoreError::ChildRunNotFound)?;
        if record.settlement.is_some() {
            load_account(&mut tx, key.tenant_id(), key.parent_run_id()).await?;
            let event = load_settlement_event(&mut tx, key).await?;
            tx.commit()
                .await
                .map_err(|source| StoreError::database("child settlement retry", source))?;
            return Ok(ChildRunSettlementOutcome::Idempotent { event, record });
        }
        let at = authorize_parent_append(&mut tx, &parent, &append, false).await?;
        if record.child.run().is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        let child_id = record.child.admission().intent().provenance().run_id();
        // load_record already locks this existing child's Run. Taking the root
        // admission-key lock here would invert that API's key -> Run lock order.
        let evidence = terminal_evidence(&mut tx, &record.child).await?;
        let account = load_account(&mut tx, key.tenant_id(), key.parent_run_id())
            .await?
            .ok_or_else(|| StoreError::corrupt("owned child account"))?;
        let account = account
            .settle(key, evidence.clone())
            .map_err(|_| StoreError::ChildRunSettlementUnavailable)?;
        let at = at.max(evidence.terminal().recorded_at());
        let event =
            JournalEvent::commit(append, at).map_err(|error| map_event_commit_error(&error))?;
        let bytes = serde_json_canonicalizer::to_vec(&evidence)
            .map_err(|_| StoreError::encoding("child settlement"))?;
        let digest = Digest::sha256(&bytes);
        insert_event(
            &mut tx,
            &event,
            settlement_projection(key.digest(), digest, &event)?,
        )
        .await?;
        query("INSERT INTO stateknot.child_run_settlements (tenant_id, parent_run_id, key_digest, child_run_id, settlement_bytes, settlement_digest, journal_sequence, journal_event_id, journal_recorded_at, journal_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
            .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes())
            .bind(*child_id.as_uuid()).bind(bytes).bind(digest.as_bytes())
            .bind(i64::try_from(event.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?)
            .bind(*event.event_id().as_uuid()).bind(to_database_time(event.recorded_at())?).bind(event.digest().as_bytes())
            .execute(&mut *tx).await.map_err(|source| StoreError::database("child settlement insert", source))?;
        query("UPDATE stateknot.child_run_ownership SET settled = true, terminal_pending_at = NULL WHERE tenant_id = $1 AND parent_run_id = $2 AND key_digest = $3 AND NOT settled")
            .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes())
            .execute(&mut *tx).await.map_err(|source| StoreError::database("child settlement projection", source))?;
        save_account(&mut tx, &account, &event.head()).await?;
        update_run_head(&mut tx, &event, None).await?;
        let record = load_record(&mut tx, key)
            .await?
            .ok_or_else(|| StoreError::corrupt("settled child ownership"))?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("child settlement commit", source))?;
        Ok(ChildRunSettlementOutcome::Committed { event, record })
    }
}

fn decode_intent(bytes: &[u8]) -> Result<ChildRunAdmissionIntent, StoreError> {
    if bytes.is_empty() || bytes.len() > ChildRunAdmissionIntent::MAX_SNAPSHOT_BYTES {
        return Err(StoreError::corrupt("child intent size"));
    }
    let intent: ChildRunAdmissionIntent =
        serde_json::from_slice(bytes).map_err(|_| StoreError::corrupt("child intent"))?;
    if intent
        .canonical_bytes()
        .map_err(|_| StoreError::corrupt("child canonical intent"))?
        != bytes
    {
        return Err(StoreError::corrupt("child canonical intent"));
    }
    Ok(intent)
}

fn ensure_settled(account: &ChildRunBudgetAccount) -> Result<(), StoreError> {
    if account
        .children()
        .iter()
        .any(|entry| entry.settlement().is_none())
    {
        return Err(StoreError::UnsettledChildRuns);
    }
    Ok(())
}

/// Called with the parent row locked, after idempotency recovery and before dispatch.
pub(super) async fn authorize_invocation_budget(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    observed: Option<Digest>,
) -> Result<(), StoreError> {
    let account = load_account(tx, tenant, run).await?;
    if let Some(account) = &account {
        ensure_settled(account)?;
    }
    if account.as_ref().map(ChildRunBudgetAccount::digest) != observed {
        return Err(StoreError::ChildBudgetObservationRequired);
    }
    if let Some(digest) = observed {
        query("SELECT set_config('stateknot.child_budget_digest', encode($1::bytea, 'hex'), true)")
            .bind(digest.as_bytes())
            .execute(&mut **tx)
            .await
            .map_err(|source| StoreError::database("child invocation budget fence", source))?;
    }
    Ok(())
}

/// All terminal writers share this check, including generic control-plane appends.
pub(super) async fn validate_terminal_accounting(
    tx: &mut Transaction<'_, Postgres>,
    event: &JournalEvent,
    projection: &PreparedProjection,
) -> Result<(), StoreError> {
    if !matches!(projection.status, "succeeded" | "failed" | "cancelled") {
        return Ok(());
    }
    let Some(account) = load_account(tx, event.tenant_id(), event.run_id()).await? else {
        return Ok(());
    };
    ensure_settled(&account)?;
    let lifecycle: RunLifecycle = serde_json::from_slice(&projection.lifecycle_bytes)
        .map_err(|_| StoreError::corrupt("child terminal accounting lifecycle"))?;
    let total = lifecycle
        .terminal_usage()
        .ok_or(StoreError::IncompleteChildAccounting)?;
    let direct = total
        .checked_subtract_cumulative(
            &account
                .delegated_usage()
                .map_err(|_| StoreError::IncompleteChildAccounting)?,
        )
        .map_err(|_| StoreError::IncompleteChildAccounting)?;
    let account = account
        .observe_direct(event.head(), direct)
        .map_err(|_| StoreError::IncompleteChildAccounting)?;
    save_account(tx, &account, &event.head()).await?;
    query("SELECT set_config('stateknot.child_terminal_digest', encode($1::bytea, 'hex'), true)")
        .bind(Digest::sha256(&projection.lifecycle_bytes).as_bytes())
        .execute(&mut **tx)
        .await
        .map_err(|source| StoreError::database("child terminal accounting fence", source))?;
    Ok(())
}

pub(super) async fn lock_tree(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    root: RunId,
) -> Result<(), StoreError> {
    query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("stateknot:child-tree:{}:{root}", tenant.as_str()))
        .execute(&mut **tx)
        .await
        .map_err(|source| StoreError::database("child tree lock", source))?;
    Ok(())
}

pub(super) async fn ancestry(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    parent: RunId,
) -> Result<Vec<RunId>, StoreError> {
    let row = query_as::<_, (Vec<Uuid>, Uuid)>("SELECT ancestors, root_run_id FROM stateknot.child_run_ownership WHERE tenant_id = $1 AND child_run_id = $2")
        .bind(tenant.as_str()).bind(*parent.as_uuid()).fetch_optional(&mut **tx).await
        .map_err(|source| StoreError::database("child ancestry", source))?;
    let mut values = Vec::new();
    if let Some((path, root)) = row {
        if path.is_empty() || path.len() >= 32 || path[0] != root || path.contains(parent.as_uuid())
        {
            return Err(StoreError::ChildRunTopologyExceeded);
        }
        for id in path {
            values
                .push(RunId::from_uuid(id).map_err(|_| StoreError::corrupt("child ancestor id"))?);
        }
    }
    values.push(parent);
    if values
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != values.len()
    {
        return Err(StoreError::corrupt("child ancestry cycle"));
    }
    // Every prefix must be the immutable ownership path of that ancestor;
    // checking only the immediate parent's cached array could hide a broken tree.
    for (position, ancestor) in values.iter().enumerate().take(values.len() - 1) {
        let stored = query_scalar::<_, Vec<Uuid>>("SELECT ancestors FROM stateknot.child_run_ownership WHERE tenant_id=$1 AND child_run_id=$2")
            .bind(tenant.as_str()).bind(*ancestor.as_uuid()).fetch_optional(&mut **tx).await
            .map_err(|source| StoreError::database("child ancestry prefix", source))?;
        let expected = values[..position]
            .iter()
            .map(|id| *id.as_uuid())
            .collect::<Vec<_>>();
        if (position == 0 && stored.is_some())
            || (position > 0 && stored.as_ref() != Some(&expected))
        {
            return Err(StoreError::corrupt("child ancestry prefix"));
        }
    }
    Ok(values)
}

async fn load_graph(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    reference: &GraphReference,
) -> Result<CompiledGraph, StoreError> {
    let row = load_graph_definition_row(tx, tenant, reference)
        .await?
        .ok_or(StoreError::GraphDefinitionNotFound)?;
    let graph = decode_graph_definition(row)?.graph().clone();
    if graph.reference() != *reference {
        return Err(StoreError::corrupt("child graph reference"));
    }
    Ok(graph)
}

async fn check_topology(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    ancestors: &[RunId],
) -> Result<(), StoreError> {
    let active = query_scalar::<_, Vec<Uuid>>("SELECT ancestors FROM stateknot.child_run_ownership WHERE tenant_id = $1 AND root_run_id = $2 AND NOT settled LIMIT 257")
        .bind(tenant.as_str()).bind(*ancestors[0].as_uuid()).fetch_all(&mut **tx).await
        .map_err(|source| StoreError::database("child active tree", source))?;
    if active.len() > 256 {
        return Err(StoreError::corrupt("child active tree bound"));
    }
    for (position, ancestor) in ancestors.iter().enumerate() {
        let run = decode_run(fetch_locked_run_row(tx, tenant, *ancestor).await?)?;
        if run.is_quarantined() || run.lifecycle().status() != RunStatus::Active {
            return Err(StoreError::ChildRunRejected);
        }
        let row = load_agent_admission_row(tx, tenant, *ancestor)
            .await?
            .ok_or(StoreError::AgentAdmissionNotFound)?;
        let admission = verify_stored_agent_admission(tx, run, row).await?;
        let graph = load_graph(tx, tenant, admission.admission().intent().graph()).await?;
        let limits = graph
            .child_runs()
            .ok_or(StoreError::ChildRunRejected)?
            .limits();
        let count = active
            .iter()
            .filter(|path| path.contains(ancestor.as_uuid()))
            .count();
        if ancestors.len() - position > usize::from(limits.maximum_descendant_depth())
            || count >= usize::from(limits.maximum_active_descendants())
        {
            return Err(StoreError::ChildRunTopologyExceeded);
        }
    }
    Ok(())
}

pub(super) async fn authorize_parent_append(
    tx: &mut Transaction<'_, Postgres>,
    parent: &StoredRun,
    append: &JournalAppend,
    fresh: bool,
) -> Result<Timestamp, StoreError> {
    if parent.is_quarantined() {
        return Err(StoreError::RunQuarantined);
    }
    if parent.lifecycle().status().is_terminal()
        || (fresh && parent.lifecycle().status() != RunStatus::Active)
    {
        return Err(StoreError::RunNotRunnable);
    }
    if append.expectation().head() != parent.journal_head() {
        return Err(StoreError::StaleJournalHead);
    }
    let now = database_now(tx, "child parent authority clock").await?;
    if let Some(fence) = append.worker_fence() {
        authorize_worker(parent, fence, now)?;
    } else if fresh {
        return Err(StoreError::WrongAppendAuthority);
    }
    Ok(now.max(
        parent
            .journal_head()
            .ok_or(StoreError::StaleJournalHead)?
            .recorded_at(),
    ))
}

pub(super) async fn mark_child_capability(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
) -> Result<(), StoreError> {
    query(
        "UPDATE stateknot.runs SET child_runtime_version = 1 WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(tenant.as_str())
    .bind(*run.as_uuid())
    .execute(&mut **tx)
    .await
    .map_err(|source| StoreError::database("child capability fence", source))?;
    Ok(())
}

fn spawn_projection(
    intent: &ChildRunAdmissionIntent,
    admission: &AgentAdmission,
    checkpoint: &Checkpoint,
    event: &JournalEvent,
) -> Result<Digest, StoreError> {
    let bytes = serde_json_canonicalizer::to_vec(&(
        "stateknot.child-spawn.v1",
        intent.spawn_digest(),
        admission.digest(),
        checkpoint.digest(),
        event.intent_digest(),
    ))
    .map_err(|_| StoreError::encoding("child spawn projection"))?;
    Ok(Digest::sha256(bytes))
}

fn settlement_projection(
    key: Digest,
    digest: Digest,
    event: &JournalEvent,
) -> Result<Digest, StoreError> {
    Ok(Digest::sha256(
        serde_json_canonicalizer::to_vec(&(
            "stateknot.child-settlement.v1",
            key,
            digest,
            event.intent_digest(),
        ))
        .map_err(|_| StoreError::encoding("child settlement projection"))?,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn insert_owner(
    tx: &mut Transaction<'_, Postgres>,
    intent: &ChildRunAdmissionIntent,
    node: &NodeAttemptStart,
    ancestors: &[RunId],
    child: &StoredAgentAdmission,
    event: &JournalEvent,
) -> Result<(), StoreError> {
    let key = intent.key();
    let base = key.parent().base_checkpoint();
    let bytes = intent
        .canonical_bytes()
        .map_err(|_| StoreError::encoding("child intent"))?;
    query("INSERT INTO stateknot.child_run_ownership (tenant_id,parent_run_id,key_digest,child_slot,activation_digest,spawn_digest,child_run_id,root_run_id,ancestors,intent_bytes,child_admission_digest,parent_checkpoint_id,parent_checkpoint_superstep,parent_checkpoint_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest,key_bytes,parent_node_attempt_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)")
        .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes())
        .bind(key.slot().as_str()).bind(node.activation_digest().as_bytes()).bind(intent.spawn_digest().as_bytes())
        .bind(*intent.child().provenance().run_id().as_uuid()).bind(*ancestors[0].as_uuid())
        .bind(ancestors.iter().map(|run| *run.as_uuid()).collect::<Vec<_>>()).bind(bytes).bind(child.admission().digest().as_bytes())
        .bind(*base.checkpoint_id().as_uuid()).bind(i64::try_from(base.superstep().get()).map_err(|_| StoreError::ChildRunRejected)?)
        .bind(base.digest().as_bytes()).bind(i64::try_from(event.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?)
        .bind(*event.event_id().as_uuid()).bind(to_database_time(event.recorded_at())?).bind(event.digest().as_bytes())
        .bind(serde_json_canonicalizer::to_vec(key).map_err(|_| StoreError::encoding("child key"))?)
        .bind(*node.attempt_id().as_uuid())
        .execute(&mut **tx).await.map_err(|source| StoreError::database("child ownership insert", source))?;
    Ok(())
}

async fn save_account(
    tx: &mut Transaction<'_, Postgres>,
    account: &ChildRunBudgetAccount,
    head: &JournalHead,
) -> Result<(), StoreError> {
    let bytes = account
        .canonical_bytes()
        .map_err(|_| StoreError::encoding("child budget account"))?;
    query("INSERT INTO stateknot.child_run_budget_accounts (tenant_id,parent_run_id,account_digest,account_bytes,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (tenant_id,parent_run_id) DO UPDATE SET account_digest=EXCLUDED.account_digest,account_bytes=EXCLUDED.account_bytes,journal_sequence=EXCLUDED.journal_sequence,journal_event_id=EXCLUDED.journal_event_id,journal_recorded_at=EXCLUDED.journal_recorded_at,journal_digest=EXCLUDED.journal_digest")
        .bind(head.tenant_id().as_str()).bind(*head.run_id().as_uuid()).bind(account.digest().as_bytes()).bind(bytes)
        .bind(i64::try_from(head.sequence().get()).map_err(|_| StoreError::JournalSequenceExhausted)?)
        .bind(*head.event_id().as_uuid()).bind(to_database_time(head.recorded_at())?).bind(head.digest().as_bytes())
        .execute(&mut **tx).await.map_err(|source| StoreError::database("child budget save", source))?;
    Ok(())
}

pub(super) async fn anchored_event(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
    sequence: i64,
) -> Result<(JournalEvent, Option<Digest>), StoreError> {
    let row = query_as::<_, EventRow>(SELECT_EVENT_BY_SEQUENCE)
        .bind(tenant.as_str())
        .bind(*run.as_uuid())
        .bind(sequence)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|source| StoreError::database("child audit anchor", source))?
        .ok_or_else(|| StoreError::corrupt("child audit event"))?;
    let projection = row
        .projection_digest
        .as_deref()
        .map(|bytes| decode_digest(bytes, "child audit projection"))
        .transpose()?;
    Ok((decode_event(row)?, projection))
}

pub(super) fn row_head(
    row: &PgRow,
    tenant: &TenantId,
    run: RunId,
) -> Result<JournalHead, StoreError> {
    let get_error = |source| StoreError::database("child head decode", source);
    Ok(JournalHead::new(
        tenant.clone(),
        run,
        positive_sequence(row.try_get("journal_sequence").map_err(get_error)?)?,
        EventId::from_uuid(row.try_get("journal_event_id").map_err(get_error)?)
            .map_err(|_| StoreError::corrupt("child event id"))?,
        from_database_time(row.try_get("journal_recorded_at").map_err(get_error)?)?,
        decode_digest(
            &row.try_get::<Vec<u8>, _>("journal_digest")
                .map_err(get_error)?,
            "child event digest",
        )?,
    ))
}

async fn terminal_evidence(
    tx: &mut Transaction<'_, Postgres>,
    child: &StoredAgentAdmission,
) -> Result<ChildRunBudgetSettlement, StoreError> {
    let provenance = child.admission().intent().provenance();
    let row =
        query("SELECT * FROM stateknot.child_run_terminals WHERE tenant_id=$1 AND child_run_id=$2")
            .bind(provenance.tenant_id().as_str())
            .bind(*provenance.run_id().as_uuid())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|source| StoreError::database("child terminal load", source))?
            .ok_or(StoreError::ChildRunSettlementUnavailable)?;
    let head = row_head(&row, provenance.tenant_id(), provenance.run_id())?;
    let (event, _) = anchored_event(
        tx,
        provenance.tenant_id(),
        provenance.run_id(),
        i64::try_from(head.sequence().get())
            .map_err(|_| StoreError::corrupt("child terminal sequence"))?,
    )
    .await?;
    let bytes: Vec<u8> = row
        .try_get("lifecycle_bytes")
        .map_err(|source| StoreError::database("child terminal bytes", source))?;
    let digest: Vec<u8> = row
        .try_get("lifecycle_digest")
        .map_err(|source| StoreError::database("child terminal digest", source))?;
    let lifecycle: RunLifecycle = serde_json::from_slice(&bytes)
        .map_err(|_| StoreError::corrupt("child terminal lifecycle"))?;
    if bytes != encode_lifecycle(&lifecycle)?
        || Digest::sha256(&bytes) != decode_digest(&digest, "child terminal checksum")?
        || event.head() != head
        || encode_lifecycle(child.run().lifecycle())? != bytes
    {
        return Err(StoreError::corrupt("child terminal binding"));
    }
    ChildRunBudgetSettlement::new(child.admission(), &lifecycle, head)
        .map_err(|_| StoreError::ChildRunSettlementUnavailable)
}

async fn load_settlement_event(
    tx: &mut Transaction<'_, Postgres>,
    key: &ChildRunKey,
) -> Result<JournalEvent, StoreError> {
    let row = query("SELECT * FROM stateknot.child_run_settlements WHERE tenant_id=$1 AND parent_run_id=$2 AND key_digest=$3")
        .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes()).fetch_one(&mut **tx).await
        .map_err(|source| StoreError::database("child settlement event", source))?;
    let head = row_head(&row, key.tenant_id(), key.parent_run_id())?;
    let (event, _) = anchored_event(
        tx,
        key.tenant_id(),
        key.parent_run_id(),
        i64::try_from(head.sequence().get())
            .map_err(|_| StoreError::corrupt("child settlement sequence"))?,
    )
    .await?;
    if event.head() != head {
        return Err(StoreError::corrupt("child settlement head"));
    }
    Ok(event)
}

#[allow(clippy::too_many_lines)]
async fn load_record(
    tx: &mut Transaction<'_, Postgres>,
    key: &ChildRunKey,
) -> Result<Option<ChildRunRecord>, StoreError> {
    Box::pin(load_record_inner(tx, key, true)).await
}

#[allow(clippy::too_many_lines)]
pub(super) async fn load_record_inner(
    tx: &mut Transaction<'_, Postgres>,
    key: &ChildRunKey,
    lock_child: bool,
) -> Result<Option<ChildRunRecord>, StoreError> {
    let sql = format!(
        "SELECT {OWNER_COLUMNS} FROM stateknot.child_run_ownership WHERE tenant_id=$1 AND parent_run_id=$2 AND key_digest=$3"
    );
    let Some(row) = query(&sql)
        .bind(key.tenant_id().as_str())
        .bind(*key.parent_run_id().as_uuid())
        .bind(key.digest().as_bytes())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|source| StoreError::database("child owner load", source))?
    else {
        return Ok(None);
    };
    let get_error = |source| StoreError::database("child owner decode", source);
    let intent = decode_intent(
        &row.try_get::<Vec<u8>, _>("intent_bytes")
            .map_err(get_error)?,
    )?;
    let child_id = RunId::from_uuid(row.try_get("child_run_id").map_err(get_error)?)
        .map_err(|_| StoreError::corrupt("child run id"))?;
    let locked_child = if lock_child {
        Some(fetch_locked_run_row(tx, key.tenant_id(), child_id).await?)
    } else {
        None
    };
    // Terminal capture can update the ownership notification while acquiring
    // that child lock. Refresh its mutable projection AFTER the lock, too.
    let row = if lock_child {
        query(&sql)
            .bind(key.tenant_id().as_str())
            .bind(*key.parent_run_id().as_uuid())
            .bind(key.digest().as_bytes())
            .fetch_one(&mut **tx)
            .await
            .map_err(|source| StoreError::database("locked child owner projection", source))?
    } else {
        row
    };
    let parent_row = load_agent_admission_row(tx, key.tenant_id(), key.parent_run_id())
        .await?
        .ok_or_else(|| StoreError::corrupt("child parent admission"))?;
    let parent = decode_agent_admission(&parent_row)?;
    let node_id = AttemptId::from_uuid(row.try_get("parent_node_attempt_id").map_err(get_error)?)
        .map_err(|_| StoreError::corrupt("child node attempt id"))?;
    let node = load_node_attempt_record(tx, key.tenant_id(), &key.parent_run_id(), node_id)
        .await?
        .ok_or_else(|| StoreError::corrupt("child node attempt"))?;
    verify_node_attempt_start(tx, node.start()).await?;
    if node.start().activation() != key.parent()
        || node.start().activation_digest()
            != decode_digest(
                &row.try_get::<Vec<u8>, _>("activation_digest")
                    .map_err(get_error)?,
                "child activation",
            )?
        || row.try_get::<Vec<u8>, _>("key_bytes").map_err(get_error)?
            != serde_json_canonicalizer::to_vec(key)
                .map_err(|_| StoreError::corrupt("child key bytes"))?
    {
        return Err(StoreError::corrupt("child node activation"));
    }
    let checkpoint_row = query_as::<_, CheckpointRow>(SELECT_CHECKPOINT_BY_ID)
        .bind(key.tenant_id().as_str())
        .bind(*key.parent_run_id().as_uuid())
        .bind(*key.parent().base_checkpoint().checkpoint_id().as_uuid())
        .fetch_one(&mut **tx)
        .await
        .map_err(|source| StoreError::database("child base checkpoint", source))?;
    let checkpoint = decode_checkpoint(checkpoint_row)?;
    verify_checkpoint_anchor(tx, &checkpoint).await?;
    if checkpoint.head() != *key.parent().base_checkpoint()
        || !node_attempt_activation_is_ready(&checkpoint, key.parent())
    {
        return Err(StoreError::corrupt("child checkpoint activation"));
    }
    let graph = load_graph(tx, key.tenant_id(), parent.intent().graph()).await?;
    if load_graph(tx, key.tenant_id(), intent.child().graph()).await? != *intent.child_graph() {
        return Err(StoreError::corrupt("owned child graph"));
    }
    intent
        .validate_declaration(&graph)
        .map_err(|_| StoreError::corrupt("child declaration"))?;
    if intent.key() != key
        || intent.parent_admission_digest() != parent.digest()
        || intent.child().provenance().run_id() != child_id
        || row.try_get::<String, _>("child_slot").map_err(get_error)? != key.slot().as_str()
        || decode_digest(
            &row.try_get::<Vec<u8>, _>("spawn_digest")
                .map_err(get_error)?,
            "child spawn digest",
        )? != intent.spawn_digest()
        || row
            .try_get::<Uuid, _>("parent_checkpoint_id")
            .map_err(get_error)?
            != *key.parent().base_checkpoint().checkpoint_id().as_uuid()
        || row
            .try_get::<i64, _>("parent_checkpoint_superstep")
            .map_err(get_error)?
            != i64::try_from(key.parent().base_checkpoint().superstep().get())
                .map_err(|_| StoreError::corrupt("child base step"))?
        || decode_digest(
            &row.try_get::<Vec<u8>, _>("parent_checkpoint_digest")
                .map_err(get_error)?,
            "child base digest",
        )? != key.parent().base_checkpoint().digest()
    {
        return Err(StoreError::corrupt("child ownership projection"));
    }
    let head = row_head(&row, key.tenant_id(), key.parent_run_id())?;
    let (spawn, projection) = anchored_event(
        tx,
        key.tenant_id(),
        key.parent_run_id(),
        i64::try_from(head.sequence().get())
            .map_err(|_| StoreError::corrupt("child spawn sequence"))?,
    )
    .await?;
    // Read-only callers use one repeatable-read snapshot. Mutation callers
    // already hold the parent lock and lock the child before reading its
    // lifecycle/terminal notification, avoiding READ COMMITTED torn evidence.
    let child_run = if let Some(row) = locked_child {
        decode_run(row)?
    } else {
        decode_run(
            query_as::<_, RunRow>(SELECT_RUN)
                .bind(key.tenant_id().as_str())
                .bind(*child_id.as_uuid())
                .fetch_one(&mut **tx)
                .await
                .map_err(|source| StoreError::database("owned child run", source))?,
        )?
    };
    let child_row = load_agent_admission_row(tx, key.tenant_id(), child_id)
        .await?
        .ok_or_else(|| StoreError::corrupt("owned child admission"))?;
    let child = verify_stored_agent_admission(tx, child_run, child_row).await?;
    if child.admission().intent() != intent.child()
        || child.checkpoint().state() != intent.initial_state()
        || child.admission().digest()
            != decode_digest(
                &row.try_get::<Vec<u8>, _>("child_admission_digest")
                    .map_err(get_error)?,
                "owned admission digest",
            )?
        || spawn.head() != head
        || spawn.source().worker_fence() != Some(node.start().fence())
        || spawn.sequence() <= node.start().journal_head().sequence()
        || spawn.payload().kind().as_str() != "child-run-admitted"
        || projection
            != Some(spawn_projection(
                &intent,
                child.admission(),
                child.checkpoint(),
                &spawn,
            )?)
    {
        return Err(StoreError::corrupt("child atomic anchors"));
    }
    let expected_path = ancestry(tx, key.tenant_id(), key.parent_run_id()).await?;
    let path: Vec<Uuid> = row.try_get("ancestors").map_err(get_error)?;
    if path
        != expected_path
            .iter()
            .map(|id| *id.as_uuid())
            .collect::<Vec<_>>()
        || row.try_get::<Uuid, _>("root_run_id").map_err(get_error)? != *expected_path[0].as_uuid()
    {
        return Err(StoreError::corrupt("child ancestry projection"));
    }
    let settled: bool = row.try_get("settled").map_err(get_error)?;
    let pending: Option<DateTime<Utc>> = row.try_get("terminal_pending_at").map_err(get_error)?;
    let terminal_at = query_scalar::<_, DateTime<Utc>>("SELECT journal_recorded_at FROM stateknot.child_run_terminals WHERE tenant_id=$1 AND child_run_id=$2")
        .bind(key.tenant_id().as_str()).bind(*child_id.as_uuid()).fetch_optional(&mut **tx).await
        .map_err(|source| StoreError::database("child notification evidence", source))?;
    if terminal_at.is_some() != child.run().lifecycle().status().is_terminal()
        || pending != if settled { None } else { terminal_at }
    {
        return Err(StoreError::corrupt("child terminal notification"));
    }
    let settlement_row = query("SELECT * FROM stateknot.child_run_settlements WHERE tenant_id=$1 AND parent_run_id=$2 AND key_digest=$3")
        .bind(key.tenant_id().as_str()).bind(*key.parent_run_id().as_uuid()).bind(key.digest().as_bytes()).fetch_optional(&mut **tx).await
        .map_err(|source| StoreError::database("child settlement load", source))?;
    if settled != settlement_row.is_some() {
        return Err(StoreError::corrupt("child settlement projection"));
    }
    let settlement = if let Some(row) = settlement_row {
        let bytes: Vec<u8> = row.try_get("settlement_bytes").map_err(get_error)?;
        let evidence: ChildRunBudgetSettlement = serde_json::from_slice(&bytes)
            .map_err(|_| StoreError::corrupt("child settlement bytes"))?;
        let digest = Digest::sha256(&bytes);
        let expected = terminal_evidence(tx, &child).await?;
        let head = row_head(&row, key.tenant_id(), key.parent_run_id())?;
        let (event, projection) = anchored_event(
            tx,
            key.tenant_id(),
            key.parent_run_id(),
            i64::try_from(head.sequence().get())
                .map_err(|_| StoreError::corrupt("settlement sequence"))?,
        )
        .await?;
        if evidence != expected
            || serde_json_canonicalizer::to_vec(&evidence)
                .map_err(|_| StoreError::corrupt("settlement encoding"))?
                != bytes
            || decode_digest(
                &row.try_get::<Vec<u8>, _>("settlement_digest")
                    .map_err(get_error)?,
                "child settlement checksum",
            )? != digest
            || row.try_get::<Uuid, _>("child_run_id").map_err(get_error)? != *child_id.as_uuid()
            || head != event.head()
            || projection != Some(settlement_projection(key.digest(), digest, &event)?)
            || event.payload().kind().as_str() != "child-run-settled"
        {
            return Err(StoreError::corrupt("child settlement anchors"));
        }
        Some(evidence)
    } else {
        None
    };
    Ok(Some(ChildRunRecord {
        intent,
        child,
        spawn,
        ancestors: expected_path,
        settlement,
    }))
}

async fn load_account(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    parent_id: RunId,
) -> Result<Option<ChildRunBudgetAccount>, StoreError> {
    Box::pin(load_account_inner(tx, tenant, parent_id, true)).await
}

async fn load_account_inner(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    parent_id: RunId,
    lock_children: bool,
) -> Result<Option<ChildRunBudgetAccount>, StoreError> {
    let row = query(
        "SELECT * FROM stateknot.child_run_budget_accounts WHERE tenant_id=$1 AND parent_run_id=$2",
    )
    .bind(tenant.as_str())
    .bind(*parent_id.as_uuid())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|source| StoreError::database("child budget load", source))?;
    let Some(row) = row else {
        let exists = query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM stateknot.child_run_ownership WHERE tenant_id=$1 AND parent_run_id=$2)")
            .bind(tenant.as_str()).bind(*parent_id.as_uuid()).fetch_one(&mut **tx).await.map_err(|source| StoreError::database("child budget completeness", source))?;
        if exists {
            return Err(StoreError::corrupt("child budget missing"));
        }
        return Ok(None);
    };
    let get_error = |source| StoreError::database("child budget decode", source);
    let bytes: Vec<u8> = row.try_get("account_bytes").map_err(get_error)?;
    if bytes.len() > ChildRunBudgetAccount::MAX_SNAPSHOT_BYTES {
        return Err(StoreError::corrupt("child budget size"));
    }
    let account: ChildRunBudgetAccount =
        serde_json::from_slice(&bytes).map_err(|_| StoreError::corrupt("child budget account"))?;
    let head = row_head(&row, tenant, parent_id)?;
    let (event, _) = anchored_event(
        tx,
        tenant,
        parent_id,
        i64::try_from(head.sequence().get())
            .map_err(|_| StoreError::corrupt("child budget sequence"))?,
    )
    .await?;
    let (direct_event, _) = anchored_event(
        tx,
        tenant,
        parent_id,
        i64::try_from(account.direct_head().sequence().get())
            .map_err(|_| StoreError::corrupt("child direct head"))?,
    )
    .await?;
    if direct_event.head() != *account.direct_head() {
        return Err(StoreError::corrupt("child direct anchor"));
    }
    let admission_row = load_agent_admission_row(tx, tenant, parent_id)
        .await?
        .ok_or_else(|| StoreError::corrupt("child budget parent"))?;
    let admission = decode_agent_admission(&admission_row)?;
    let graph = load_graph(tx, tenant, admission.intent().graph()).await?;
    account
        .validate_for(&admission, &graph)
        .map_err(|_| StoreError::corrupt("child budget parent binding"))?;
    if account
        .canonical_bytes()
        .map_err(|_| StoreError::corrupt("child budget encoding"))?
        != bytes
        || account.digest()
            != decode_digest(
                &row.try_get::<Vec<u8>, _>("account_digest")
                    .map_err(get_error)?,
                "child budget checksum",
            )?
        || head != event.head()
        || account.direct_head().sequence() > head.sequence()
    {
        return Err(StoreError::corrupt("child budget anchor"));
    }
    let count = query_scalar::<_, i64>("SELECT count(*) FROM stateknot.child_run_ownership WHERE tenant_id=$1 AND parent_run_id=$2")
        .bind(tenant.as_str()).bind(*parent_id.as_uuid()).fetch_one(&mut **tx).await.map_err(|source| StoreError::database("child budget entry count", source))?;
    if usize::try_from(count).ok() != Some(account.children().len()) {
        return Err(StoreError::corrupt("child budget completeness"));
    }
    for entry in account.children() {
        let record = Box::pin(load_record_inner(tx, entry.key(), lock_children))
            .await?
            .ok_or_else(|| StoreError::corrupt("child budget ownership"))?;
        if entry.spawn_digest() != record.intent.spawn_digest()
            || entry.child() != record.child.admission().intent().provenance()
            || entry.intent_digest() != record.child.admission().intent().intent_digest()
            || entry.settlement() != record.settlement.as_ref()
            || entry.reservation()
                != &stateknot_core::CumulativeBudgetReservation::from_budget(
                    record.intent.child().budget(),
                )
                .map_err(|_| StoreError::corrupt("child reservation"))?
        {
            return Err(StoreError::corrupt("child budget entry binding"));
        }
    }
    Ok(Some(account))
}

pub(super) async fn verify_schema(pool: &PgPool) -> Result<(), StoreError> {
    let complete = query_scalar::<_, bool>(r"SELECT
        NOT EXISTS (
          SELECT 1 FROM (VALUES
            ('runs_child_mutation_guard', 'stateknot.runs', 'stateknot.guard_child_run_mutation()', 19),
            ('runs_child_terminal_capture', 'stateknot.runs', 'stateknot.capture_child_run_terminal()', 17),
            ('admissions_child_capability', 'stateknot.agent_admissions', 'stateknot.mark_child_graph_admission()', 5),
            ('tool_revisions_child_budget_guard', 'stateknot.tool_invocation_revisions', 'stateknot.guard_child_direct_invocation()', 7),
            ('model_revisions_child_budget_guard', 'stateknot.model_invocation_revisions', 'stateknot.guard_child_direct_invocation()', 7),
            ('ownership_child_immutable', 'stateknot.child_run_ownership', 'stateknot.guard_child_evidence()', 27),
            ('terminals_child_immutable', 'stateknot.child_run_terminals', 'stateknot.guard_child_evidence()', 27),
            ('settlements_child_immutable', 'stateknot.child_run_settlements', 'stateknot.guard_child_evidence()', 27)
          ) AS expected(name, relation, function, mask)
          WHERE NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = expected.name
            AND tgrelid = to_regclass(expected.relation) AND tgfoid = to_regprocedure(expected.function)
            AND tgenabled = 'O' AND NOT tgisinternal AND tgtype = expected.mask)
        )
        AND NOT EXISTS (
          SELECT 1 FROM (VALUES
            ('child_run_budget_accounts_admission_fk', 'stateknot.child_run_budget_accounts', 'stateknot.agent_admissions'),
            ('child_run_budget_accounts_event_fk', 'stateknot.child_run_budget_accounts', 'stateknot.run_events'),
            ('child_run_ownership_parent_fk', 'stateknot.child_run_ownership', 'stateknot.agent_admissions'),
            ('child_run_ownership_child_fk', 'stateknot.child_run_ownership', 'stateknot.agent_admissions'),
            ('child_run_ownership_root_fk', 'stateknot.child_run_ownership', 'stateknot.agent_admissions'),
            ('child_run_ownership_node_fk', 'stateknot.child_run_ownership', 'stateknot.node_attempts'),
            ('child_run_ownership_budget_fk', 'stateknot.child_run_ownership', 'stateknot.child_run_budget_accounts'),
            ('child_run_ownership_checkpoint_fk', 'stateknot.child_run_ownership', 'stateknot.run_checkpoints'),
            ('child_run_ownership_event_fk', 'stateknot.child_run_ownership', 'stateknot.run_events'),
            ('child_run_terminals_owner_fk', 'stateknot.child_run_terminals', 'stateknot.child_run_ownership'),
            ('child_run_terminals_event_fk', 'stateknot.child_run_terminals', 'stateknot.run_events'),
            ('child_run_settlements_owner_fk', 'stateknot.child_run_settlements', 'stateknot.child_run_ownership'),
            ('child_run_settlements_terminal_fk', 'stateknot.child_run_settlements', 'stateknot.child_run_terminals'),
            ('child_run_settlements_event_fk', 'stateknot.child_run_settlements', 'stateknot.run_events')
          ) AS expected(name, relation, target)
          WHERE NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = expected.name
            AND conrelid = to_regclass(expected.relation) AND confrelid = to_regclass(expected.target)
            AND contype = 'f' AND convalidated AND confdeltype = 'r')
        )
        AND (SELECT count(*) = 4 FROM pg_constraint WHERE convalidated AND conname IN
            ('child_run_ownership_child_unique','child_run_ownership_exact_unique','child_run_ownership_ancestry_valid','runs_child_runtime_version_valid')
            AND connamespace = to_regnamespace('stateknot'))
        AND (SELECT count(*) = 2 FROM pg_index WHERE indisvalid AND indisready AND indislive
            AND indrelid = to_regclass('stateknot.child_run_ownership') AND indpred IS NOT NULL
            AND indexrelid IN (to_regclass('stateknot.child_run_ownership_active_tree'),to_regclass('stateknot.child_run_ownership_terminal_pending')))
        AND EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('stateknot.runs')
            AND attname = 'child_runtime_version' AND atttypid = 'int2'::regtype AND attnotnull AND NOT attisdropped)")
        .fetch_one(pool).await.map_err(|source| StoreError::database("child schema verification", source))?;
    if !complete {
        return Err(StoreError::IncompleteSchema);
    }
    // Verify actual installed guard code, not only names/checksummed migration history.
    for function in include_str!("../migrations/0020_child_run_ownership.sql")
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
            .bind(format!("stateknot.{name}()")).fetch_optional(pool).await
            .map_err(|source| StoreError::database("child guard definition verification", source))?;
        if installed.as_deref() != Some(body) {
            return Err(StoreError::IncompleteSchema);
        }
    }
    Ok(())
}
