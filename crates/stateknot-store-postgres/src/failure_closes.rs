// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Sealed failure decisions: settled direct work, then durable child drain.

#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{FailureCategory, RunFailure};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    failure: Failure,
    direct_usage: BudgetUsage,
    checkpoint: CheckpointHead,
    lifecycle: RunLifecycle,
}

/// Verified immutable failure decision. Active business state is not runnable
/// while this record is pending; completion retains the same original failure.
#[derive(Clone, Debug)]
pub struct RunFailureCloseRecord {
    intent: Intent,
    event: JournalEvent,
    completed_at: Option<Timestamp>,
}
impl RunFailureCloseRecord {
    /// Original failure occurrence; later candidates never replace it.
    #[must_use]
    pub const fn failure(&self) -> &Failure {
        &self.intent.failure
    }
    /// Complete, priced DIRECT usage frozen before releasing execution ownership.
    #[must_use]
    pub const fn direct_usage(&self) -> &BudgetUsage {
        &self.intent.direct_usage
    }
    /// Exact sealed checkpoint; no successor is allowed.
    #[must_use]
    pub const fn checkpoint(&self) -> &CheckpointHead {
        &self.intent.checkpoint
    }
    /// Active lifecycle at registration; not a cancellation request.
    #[must_use]
    pub const fn lifecycle(&self) -> &RunLifecycle {
        &self.intent.lifecycle
    }
    /// Worker-fenced registration atomically captured with lease release and child work.
    #[must_use]
    pub const fn registration(&self) -> &JournalEvent {
        &self.event
    }
    /// Database observation at final Failed commit, if complete.
    #[must_use]
    pub const fn completed_at(&self) -> Option<Timestamp> {
        self.completed_at
    }
}

/// First decision wins; Existing does not assert equivalence of retry candidates.
#[derive(Clone, Debug)]
pub enum RunFailureCloseOutcome {
    /// The requested decision was committed atomically.
    Committed(RunFailureCloseRecord),
    /// The original decision was recovered, including after a lost acknowledgement.
    Existing(RunFailureCloseRecord),
}
impl RunFailureCloseOutcome {
    /// Returns verified original evidence for either outcome.
    #[must_use]
    pub const fn record(&self) -> &RunFailureCloseRecord {
        match self {
            Self::Committed(record) | Self::Existing(record) => record,
        }
    }
}

/// Tenant-bound stable range cursor; not an authorization grant or permanent watermark.
#[derive(Clone, Debug)]
pub struct RunFailureCloseCursor {
    tenant: TenantId,
    run_id: RunId,
    requested_at: Timestamp,
}
impl RunFailureCloseCursor {
    /// Source event time for measuring oldest pending work and sweep lag.
    #[must_use]
    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }
    /// Tenant selected by the authenticated maintenance host.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant
    }
    /// Run identity, still valid after completion removes the candidate.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }
}

impl PostgresStore {
    /// Persist a non-cancellation failure for an Active run whose direct work is
    /// fully settled. Caller supplies trusted COMPLETE direct usage, never child
    /// usage. Unknown direct cost or unfinished physical work rejects the whole
    /// transaction without releasing its recovery lease. All immediate unsettled
    /// children receive durable cancellation work; no recursive run locks.
    #[allow(clippy::too_many_lines)]
    pub async fn request_run_failure_close(
        &self,
        checkpoint: &CheckpointHead,
        failure: Failure,
        direct_usage: BudgetUsage,
        append: JournalAppend,
    ) -> Result<RunFailureCloseOutcome, StoreError> {
        let tenant = checkpoint.tenant_id();
        let run_id = checkpoint.run_id();
        if append.intent().tenant_id() != tenant || append.intent().run_id() != run_id {
            return Err(StoreError::InvalidRunFailureClose);
        }
        let mut tx = self.begin_mutation("failure close request").await?;
        let run = decode_run(fetch_locked_run_row(&mut tx, tenant, run_id).await?)?;
        if let Some(record) = load(&mut tx, &run).await? {
            return Ok(RunFailureCloseOutcome::Existing(record));
        }
        if append.intent().payload().kind().as_str() != "run-failure-close-requested"
            || failure.category() == FailureCategory::Cancelled
            || direct_usage.unpriced_cost_events().get() != 0
        {
            return Err(StoreError::InvalidRunFailureClose);
        }
        let at = child_runs::authorize_parent_append(&mut tx, &run, &append, true).await?;
        let current = load_locked_current_checkpoint(&mut tx, &run, tenant, run_id)
            .await?
            .ok_or(StoreError::StaleCheckpointHead)?;
        if current.head() != *checkpoint {
            return Err(StoreError::StaleCheckpointHead);
        }
        let fence = append
            .worker_fence()
            .ok_or(StoreError::WrongAppendAuthority)?
            .clone();
        ensure_no_unsettled_tool_invocations(&mut tx, &current).await?;
        ensure_no_unsettled_model_invocations(&mut tx, &current).await?;
        let unfinished = query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM stateknot.node_attempts n WHERE n.tenant_id=$1 AND n.run_id=$2 AND n.fence_attempt_id=$3 AND NOT EXISTS (SELECT 1 FROM stateknot.node_attempt_completions c WHERE c.tenant_id=n.tenant_id AND c.run_id=n.run_id AND c.attempt_id=n.attempt_id))")
            .bind(tenant.as_str()).bind(*run_id.as_uuid()).bind(*fence.attempt_id().as_uuid())
            .fetch_one(&mut *tx).await.map_err(|source| StoreError::database("failure close direct boundary",source))?;
        if unfinished {
            return Err(StoreError::InvalidRunFailureClose);
        }
        if let Some(account) =
            child_runs::load_account_inner(&mut tx, tenant, run_id, false).await?
        {
            direct_usage
                .validate_monotonic_after(account.direct_usage())
                .map_err(|_| StoreError::IncompleteChildAccounting)?;
        }
        let intent = Intent {
            failure,
            direct_usage,
            checkpoint: checkpoint.clone(),
            lifecycle: run.lifecycle().clone(),
        };
        let bytes = encode(&intent)?;
        if bytes.len() > 4_194_304 {
            return Err(StoreError::InvalidRunFailureClose);
        }
        let event =
            JournalEvent::commit(append, at).map_err(|error| map_event_commit_error(&error))?;
        insert_event(&mut tx, &event, projection(&bytes, &event)?).await?;
        update_run_head(&mut tx, &event, None).await?;
        query("INSERT INTO stateknot.run_failure_closes (tenant_id,run_id,intent_bytes,intent_digest,journal_sequence,journal_event_id,journal_recorded_at,journal_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(tenant.as_str()).bind(*run_id.as_uuid()).bind(&bytes).bind(Digest::sha256(&bytes).as_bytes())
            .bind(i64::try_from(event.sequence().get()).map_err(|_| StoreError::InvalidRunFailureClose)?)
            .bind(*event.event_id().as_uuid()).bind(to_database_time(at)?).bind(event.digest().as_bytes())
            .execute(&mut *tx).await.map_err(|source| StoreError::database("failure close insert",source))?;
        let released = query("UPDATE stateknot.runs SET lease_attempt_id=NULL,lease_acquired_at=NULL,lease_renewed_at=NULL,lease_expires_at=NULL,scheduler_not_before=NULL,updated_at=$5 WHERE tenant_id=$1 AND run_id=$2 AND lease_attempt_id=$3 AND fencing_epoch=$4 AND lease_expires_at>clock_timestamp()")
            .bind(tenant.as_str()).bind(*run_id.as_uuid()).bind(*fence.attempt_id().as_uuid())
            .bind(i64::try_from(fence.epoch().get()).map_err(|_| StoreError::StaleFence)?).bind(to_database_time(at)?)
            .execute(&mut *tx).await.map_err(|source| StoreError::database("failure close lease release",source))?.rows_affected();
        if released != 1 {
            return Err(StoreError::LeaseExpired);
        }
        tx.commit()
            .await
            .map_err(|source| StoreError::database("failure close commit", source))?;
        Ok(RunFailureCloseOutcome::Committed(RunFailureCloseRecord {
            intent,
            event,
            completed_at: None,
        }))
    }

    /// Verified read under a consistent snapshot, including original evidence after completion.
    pub async fn load_run_failure_close(
        &self,
        tenant: &TenantId,
        run_id: RunId,
    ) -> Result<Option<RunFailureCloseRecord>, StoreError> {
        let mut tx = self.begin_mutation("failure close read").await?;
        let run = decode_run(fetch_locked_run_row(&mut tx, tenant, run_id).await?)?;
        load(&mut tx, &run).await
    }

    /// At most 16 pending decisions. Preserve cursor through item failures, reset
    /// to None at sweep end and after process loss. Quarantine is not hidden.
    pub async fn pending_run_failure_closes_after(
        &self,
        tenant: &TenantId,
        after: Option<&RunFailureCloseCursor>,
    ) -> Result<Vec<RunFailureCloseCursor>, StoreError> {
        if after.is_some_and(|cursor| cursor.tenant_id() != tenant) {
            return Err(StoreError::InvalidRunFailureClose);
        }
        let sql = if after.is_some() {
            "SELECT run_id,journal_recorded_at FROM stateknot.run_failure_closes WHERE tenant_id=$1 AND completed_at IS NULL AND (journal_recorded_at,run_id)>($2,$3) ORDER BY journal_recorded_at,run_id LIMIT 16"
        } else {
            "SELECT run_id,journal_recorded_at FROM stateknot.run_failure_closes WHERE tenant_id=$1 AND completed_at IS NULL ORDER BY journal_recorded_at,run_id LIMIT 16"
        };
        let mut listing = query_as::<_, (Uuid, DateTime<Utc>)>(sql).bind(tenant.as_str());
        if let Some(cursor) = after {
            listing = listing
                .bind(to_database_time(cursor.requested_at)?)
                .bind(*cursor.run_id.as_uuid());
        }
        listing
            .fetch_all(&self.pool)
            .await
            .map_err(|source| StoreError::database("failure close discovery", source))?
            .into_iter()
            .map(|(id, at)| {
                Ok(RunFailureCloseCursor {
                    tenant: tenant.clone(),
                    run_id: RunId::from_uuid(id).map_err(|_| StoreError::InvalidRunFailureClose)?,
                    requested_at: from_database_time(at)?,
                })
            })
            .collect()
    }

    /// Finalizes only with exact frozen direct plus fully verified settled child
    /// usage. Call with a control-plane audit append; no execution lease is acquired.
    pub async fn complete_run_failure_close(
        &self,
        append: JournalAppend,
    ) -> Result<RunFailureCloseOutcome, StoreError> {
        let mut tx = self.begin_mutation("failure close completion").await?;
        let run = decode_run(
            fetch_locked_run_row(
                &mut tx,
                append.intent().tenant_id(),
                append.intent().run_id(),
            )
            .await?,
        )?;
        let record = load(&mut tx, &run)
            .await?
            .ok_or(StoreError::InvalidRunFailureClose)?;
        if record.completed_at().is_some() {
            return Ok(RunFailureCloseOutcome::Existing(record));
        }
        if append.worker_fence().is_some()
            || append.intent().payload().kind().as_str() != "run-failure-close-completed"
        {
            return Err(StoreError::InvalidRunFailureClose);
        }
        let at = child_runs::authorize_parent_append(&mut tx, &run, &append, false).await?;
        let mut usage = record.direct_usage().clone();
        if let Some(account) = child_runs::load_account_inner(
            &mut tx,
            append.intent().tenant_id(),
            append.intent().run_id(),
            false,
        )
        .await?
        {
            child_runs::ensure_settled(&account)?;
            usage = usage
                .checked_accumulate(
                    &account
                        .delegated_usage()
                        .map_err(|_| StoreError::IncompleteChildAccounting)?,
                )
                .map_err(|_| StoreError::IncompleteChildAccounting)?;
        }
        let failure = RunFailure::new(record.failure().clone(), at, usage)
            .map_err(|_| StoreError::InvalidRunFailureClose)?;
        let transition = RunTransition::Fail { failure };
        let projection = RunProjection::transition(run.lifecycle().revision(), transition.clone());
        let prepared = prepare_durable_wait_projection(
            &run,
            append.intent().tenant_id(),
            append.intent().run_id(),
            run.lifecycle().revision(),
            transition,
            at,
        )?;
        let event =
            JournalEvent::commit(append, at).map_err(|error| map_event_commit_error(&error))?;
        insert_event(&mut tx, &event, projection_digest(&projection)?).await?;
        update_run_head(&mut tx, &event, Some(&prepared)).await?;
        tx.commit()
            .await
            .map_err(|source| StoreError::database("failure close final commit", source))?;
        Ok(RunFailureCloseOutcome::Committed(RunFailureCloseRecord {
            completed_at: Some(at),
            ..record
        }))
    }
}

pub(super) async fn exists(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &TenantId,
    run: RunId,
) -> Result<bool, StoreError> {
    query_scalar("SELECT EXISTS (SELECT 1 FROM stateknot.run_failure_closes WHERE tenant_id=$1 AND run_id=$2)")
        .bind(tenant.as_str()).bind(*run.as_uuid()).fetch_one(&mut **tx).await.map_err(|source| StoreError::database("failure close gate",source))
}

fn encode(intent: &Intent) -> Result<Vec<u8>, StoreError> {
    serde_json_canonicalizer::to_vec(intent).map_err(|_| StoreError::InvalidRunFailureClose)
}
fn projection(bytes: &[u8], event: &JournalEvent) -> Result<Digest, StoreError> {
    Ok(Digest::sha256(
        serde_json_canonicalizer::to_vec(&(
            "stateknot.run-failure-close.v1",
            Digest::sha256(bytes),
            event.intent_digest(),
        ))
        .map_err(|_| StoreError::InvalidRunFailureClose)?,
    ))
}

pub(super) async fn load(
    tx: &mut Transaction<'_, Postgres>,
    run: &StoredRun,
) -> Result<Option<RunFailureCloseRecord>, StoreError> {
    let scope = run.lifecycle().provenance();
    let row = query("SELECT * FROM stateknot.run_failure_closes WHERE tenant_id=$1 AND run_id=$2")
        .bind(scope.tenant_id().as_str())
        .bind(*scope.run_id().as_uuid())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|source| StoreError::database("failure close load", source))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let err = |source| StoreError::database("failure close decode", source);
    let bytes: Vec<u8> = row.try_get("intent_bytes").map_err(err)?;
    if bytes.is_empty() || bytes.len() > 4_194_304 {
        return Err(StoreError::corrupt("failure close size"));
    }
    let intent: Intent =
        serde_json::from_slice(&bytes).map_err(|_| StoreError::corrupt("failure close intent"))?;
    let head = child_runs::row_head(&row, scope.tenant_id(), scope.run_id())?;
    let (event, digest) = child_runs::anchored_event(
        tx,
        scope.tenant_id(),
        scope.run_id(),
        i64::try_from(head.sequence().get()).map_err(|_| StoreError::InvalidRunFailureClose)?,
    )
    .await?;
    let completed_at = row
        .try_get::<Option<DateTime<Utc>>, _>("completed_at")
        .map_err(err)?
        .map(from_database_time)
        .transpose()?;
    let checkpoint = load_locked_current_checkpoint(tx, run, scope.tenant_id(), scope.run_id())
        .await?
        .ok_or(StoreError::StaleCheckpointHead)?;
    if encode(&intent)? != bytes
        || Digest::sha256(&bytes).as_bytes()
            != row.try_get::<Vec<u8>, _>("intent_digest").map_err(err)?
        || digest != Some(projection(&bytes, &event)?)
        || event.head() != head
        || event.source().worker_fence().is_none()
        || event.payload().kind().as_str() != "run-failure-close-requested"
        || intent.lifecycle.provenance() != scope
        || intent.lifecycle.status() != RunStatus::Active
        || intent.lifecycle.changed_at() > head.recorded_at()
        || intent.checkpoint != checkpoint.head()
        || intent.failure.category() == FailureCategory::Cancelled
        || intent.direct_usage.unpriced_cost_events().get() != 0
        || run
            .journal_head()
            .is_none_or(|current| current.sequence() < head.sequence())
    {
        return Err(StoreError::corrupt("failure close binding"));
    }
    let valid = if let Some(at) = completed_at {
        run.lifecycle().status() == RunStatus::Failed
            && at == run.lifecycle().changed_at()
            && at >= head.recorded_at()
            && serde_json_canonicalizer::to_vec(&run.lifecycle().terminal_failure()).ok()
                == serde_json_canonicalizer::to_vec(&Some(&intent.failure)).ok()
            && run.lifecycle().revision().get()
                == intent
                    .lifecycle
                    .revision()
                    .get()
                    .checked_add(1)
                    .ok_or(StoreError::InvalidRunFailureClose)?
    } else {
        encode_lifecycle(run.lifecycle())? == encode_lifecycle(&intent.lifecycle)?
    };
    if !valid || run.lease().is_some() {
        return Err(StoreError::corrupt("failure close lifecycle"));
    }
    Ok(Some(RunFailureCloseRecord {
        intent,
        event,
        completed_at,
    }))
}

pub(super) async fn validate_direct_usage(
    tx: &mut Transaction<'_, Postgres>,
    event: &JournalEvent,
    direct: &BudgetUsage,
) -> Result<(), StoreError> {
    let run = decode_run(fetch_locked_run_row(tx, event.tenant_id(), event.run_id()).await?)?;
    if let Some(record) = load(tx, &run).await? {
        if direct != record.direct_usage() {
            return Err(StoreError::IncompleteChildAccounting);
        }
    }
    Ok(())
}

pub(super) async fn verify_schema(pool: &PgPool) -> Result<(), StoreError> {
    let installed = query_scalar::<_, String>(CATALOG_QUERY)
        .fetch_one(pool)
        .await
        .map_err(|source| StoreError::database("failure close catalog", source))?;
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("failure_close_catalog.json"))
            .map_err(|_| StoreError::IncompleteSchema)?;
    if serde_json::from_str::<serde_json::Value>(&installed)
        .map_err(|_| StoreError::IncompleteSchema)?
        != expected
    {
        return Err(StoreError::IncompleteSchema);
    }
    for function in include_str!("../migrations/0024_run_failure_closes.sql")
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
            .bind(format!("stateknot.{name}()")).fetch_optional(pool).await.map_err(|source| StoreError::database("failure close guard",source))?;
        if installed.as_deref() != Some(body) {
            return Err(StoreError::IncompleteSchema);
        }
    }
    Ok(())
}

const CATALOG_QUERY: &str = r"
WITH relations AS (SELECT oid,relname FROM pg_class WHERE relnamespace='stateknot'::regnamespace AND relname IN ('run_failure_closes'))
SELECT jsonb_build_object(
 'columns',(SELECT jsonb_agg(jsonb_build_array(r.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull) ORDER BY r.relname,a.attnum) FROM relations r JOIN pg_attribute a ON a.attrelid=r.oid WHERE a.attnum>0 AND NOT a.attisdropped),
 'constraints',(SELECT jsonb_agg(jsonb_build_array(r.relname,c.conname,pg_get_constraintdef(c.oid),c.convalidated,c.condeferrable) ORDER BY r.relname,c.conname) FROM relations r JOIN pg_constraint c ON c.conrelid=r.oid),
 'indexes',(SELECT jsonb_agg(jsonb_build_array(r.relname,pg_get_indexdef(i.indexrelid),i.indisvalid,i.indisready,i.indislive) ORDER BY r.relname,pg_get_indexdef(i.indexrelid)) FROM relations r JOIN pg_index i ON i.indrelid=r.oid),
 'triggers',(SELECT jsonb_agg(jsonb_build_array(t.tgname,pg_get_triggerdef(t.oid),t.tgenabled) ORDER BY t.tgname) FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgrelid IN (SELECT oid FROM relations) OR (t.tgrelid='stateknot.runs'::regclass AND t.tgname IN ('runs_failure_close_guard','runs_failure_close_complete')) OR (t.tgrelid='stateknot.child_run_ownership'::regclass AND t.tgname='child_ownership_failure_close_guard')))
)::text
";
