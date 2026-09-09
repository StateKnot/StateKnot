// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Database-clock deadline cancellation; never terminal acknowledgement.

#[allow(clippy::wildcard_imports)]
use super::*;
use stateknot_core::{
    FailureCategory, FailureCode, FailureId, FailureMessage, FailureOrigin, JournalEventKind,
    JournalExpectation, RunCancellationRequest, SchemaReference,
};

/// Tenant-bound range cursor. It remains usable after the candidate closes.
#[derive(Clone, Debug)]
pub struct AgentDeadlineCursor {
    tenant: TenantId,
    deadline: Timestamp,
    run_id: RunId,
}
impl AgentDeadlineCursor {
    /// Returns the admitted run identity; not an authorization grant.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }
    /// Returns the exact finite deadline used by the index.
    #[must_use]
    pub const fn deadline(&self) -> Timestamp {
        self.deadline
    }
    /// Returns the tenant scope.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant
    }
}

/// Observation after serializing with all lifecycle, spawn and wait writers.
#[derive(Clone, Debug)]
pub enum AgentDeadlineCancellationOutcome {
    /// An earlier sealed failure owns closure; its original reason is retained.
    FailureClosing,
    /// Request, wait abandonment and immediate-child cancellation queue committed together.
    Requested(JournalHead),
    /// An earlier cancellation won. Its original reason is never replaced.
    AlreadyRequested(Box<RunCancellationRequest>),
    /// A terminal writer won. Its outcome and usage remain untouched.
    Terminal(RunStatus),
    /// The authoritative database clock has not reached the admitted deadline.
    NotDue {
        /// Exact admitted deadline.
        deadline: Timestamp,
        /// Database clock after acquiring the run lock.
        observed_at: Timestamp,
    },
}

impl PostgresStore {
    /// Discovers at most 16 due admitted runs, including Waiting and sealed Join runs.
    /// Continue past errors using the last candidate, reset to `None` at sweep end,
    /// and restart at `None` after process loss. This does not acquire a lease.
    /// The caller must authenticate and authorize tenant-wide maintenance first.
    pub async fn due_agent_deadlines_after(
        &self,
        tenant: &TenantId,
        after: Option<&AgentDeadlineCursor>,
    ) -> Result<Vec<AgentDeadlineCursor>, StoreError> {
        if after.is_some_and(|cursor| cursor.tenant_id() != tenant) {
            return Err(StoreError::InvalidAgentDeadline);
        }
        let sql = if after.is_some() {
            "SELECT agent_deadline_at,run_id FROM stateknot.runs WHERE tenant_id=$1 AND agent_deadline_at IS NOT NULL AND lifecycle_status IN ('pending','active','waiting') AND NOT EXISTS (SELECT 1 FROM stateknot.run_failure_closes c WHERE c.tenant_id=stateknot.runs.tenant_id AND c.run_id=stateknot.runs.run_id) AND agent_deadline_at <= statement_timestamp() AND (agent_deadline_at,run_id)>($2,$3) ORDER BY agent_deadline_at,run_id LIMIT 16"
        } else {
            "SELECT agent_deadline_at,run_id FROM stateknot.runs WHERE tenant_id=$1 AND agent_deadline_at IS NOT NULL AND lifecycle_status IN ('pending','active','waiting') AND NOT EXISTS (SELECT 1 FROM stateknot.run_failure_closes c WHERE c.tenant_id=stateknot.runs.tenant_id AND c.run_id=stateknot.runs.run_id) AND agent_deadline_at <= statement_timestamp() ORDER BY agent_deadline_at,run_id LIMIT 16"
        };
        let mut listing = query_as::<_, (DateTime<Utc>, Uuid)>(sql).bind(tenant.as_str());
        if let Some(cursor) = after {
            listing = listing
                .bind(to_database_time(cursor.deadline)?)
                .bind(*cursor.run_id.as_uuid());
        }
        listing
            .fetch_all(&self.pool)
            .await
            .map_err(|source| StoreError::database("Agent deadline discovery", source))?
            .into_iter()
            .map(|(deadline, run)| {
                Ok(AgentDeadlineCursor {
                    tenant: tenant.clone(),
                    deadline: from_database_time(deadline)?,
                    run_id: RunId::from_uuid(run)
                        .map_err(|_| StoreError::corrupt("Agent deadline run identity"))?,
                })
            })
            .collect()
    }

    /// Requests cancellation only after verifying the immutable admission and
    /// observing expiry under the run row lock. No wall-clock input is accepted.
    /// The deployment-owned schema must accept the documented deadline audit payload.
    /// New candidate IDs on retry converge on the original cancellation reason.
    /// A due deadline is not evidence that external work stopped or cost is known.
    /// This is a trusted control-plane API, not an untrusted tenant endpoint.
    pub async fn request_agent_deadline_cancellation<V: GraphSchemaValidator + ?Sized>(
        &self,
        tenant: &TenantId,
        run_id: RunId,
        event_id: EventId,
        failure_id: FailureId,
        schema: &SchemaReference,
        schemas: &V,
    ) -> Result<AgentDeadlineCancellationOutcome, StoreError> {
        Box::pin(self.request_agent_deadline_cancellation_inner(
            tenant, run_id, event_id, failure_id, schema, schemas,
        ))
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn request_agent_deadline_cancellation_inner<V: GraphSchemaValidator + ?Sized>(
        &self,
        tenant: &TenantId,
        run_id: RunId,
        event_id: EventId,
        failure_id: FailureId,
        schema: &SchemaReference,
        schemas: &V,
    ) -> Result<AgentDeadlineCancellationOutcome, StoreError> {
        // No tree/ancestor locks: like every ordinary cancellation writer, the
        // parent row serializes spawn and triggers bounded direct-child capture.
        let mut tx = self.begin_mutation("Agent deadline cancellation").await?;
        let run = decode_run(fetch_locked_run_row(&mut tx, tenant, run_id).await?)?;
        verify_current_wait_set(&mut tx, &run).await?;
        let row = load_agent_admission_row(&mut tx, tenant, run_id)
            .await?
            .ok_or(StoreError::AgentAdmissionNotFound)?;
        let stored = verify_stored_agent_admission(&mut tx, run, row).await?;
        let run = stored.run();
        let deadline = stored.admission().intent().budget().deadline();
        let indexed = query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT agent_deadline_at FROM stateknot.runs WHERE tenant_id=$1 AND run_id=$2",
        )
        .bind(tenant.as_str())
        .bind(*run_id.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(|source| StoreError::database("Agent deadline projection", source))?;
        if indexed.map(from_database_time).transpose()? != Some(deadline) {
            return Err(StoreError::corrupt("Agent deadline projection"));
        }
        if let Some(request) = run.lifecycle().cancellation_request() {
            return Ok(AgentDeadlineCancellationOutcome::AlreadyRequested(
                Box::new(request.clone()),
            ));
        }
        if run.lifecycle().status().is_terminal() {
            return Ok(AgentDeadlineCancellationOutcome::Terminal(
                run.lifecycle().status(),
            ));
        }
        if run.is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if failure_closes::exists(&mut tx, tenant, run_id).await? {
            return Ok(AgentDeadlineCancellationOutcome::FailureClosing);
        }
        let observed_at = database_now(&mut tx, "Agent deadline clock").await?;
        if observed_at < deadline {
            return Ok(AgentDeadlineCancellationOutcome::NotDue {
                deadline,
                observed_at,
            });
        }
        let head = run.journal_head().ok_or(StoreError::InvalidAgentDeadline)?;
        let at = observed_at.max(head.recorded_at());
        let failure = Failure::new(
            failure_id,
            FailureCategory::Cancelled,
            FailureCode::new("agent.deadline.expired").expect("static failure code"),
            FailureOrigin::new("stateknot.runtime.agent_deadlines").expect("static failure origin"),
            FailureMessage::new(
                "The admitted Agent deadline expired; cooperative cancellation was requested.",
            )
            .expect("static public-safe failure message"),
            RetryAdvice::Never,
        )
        .expect("static cancellation semantics")
        .with_caused_by_event(event_id);
        let request = RunCancellationRequest::new(failure, at)
            .map_err(|_| StoreError::InvalidAgentDeadline)?;
        let transition = RunTransition::RequestCancellation { request };
        let projection = RunProjection::transition(run.lifecycle().revision(), transition.clone());
        let prepared = prepare_durable_wait_projection(
            run,
            tenant,
            run_id,
            run.lifecycle().revision(),
            transition,
            at,
        )?;
        let data = BoundedJson::try_from_value(serde_json::json!({
            "operation":"agent_deadline_cancellation_requested",
            "admission_digest": hex_digest(stored.admission().digest()),
            "deadline":deadline.to_string(), "failure_id":failure_id.to_string(),
        }))
        .map_err(|_| StoreError::InvalidAgentDeadline)?;
        schemas
            .validate(schema, &data)
            .map_err(|_| StoreError::InvalidAgentDeadline)?;
        let payload = JournalPayload::new(
            schema.clone(),
            JournalEventKind::new("agent-deadline-cancellation-requested")
                .expect("static event kind"),
            data,
        )
        .map_err(|_| StoreError::InvalidAgentDeadline)?;
        let append = JournalAppend::new(
            JournalExpectation::exact(head.clone()),
            JournalEventIntent::control_plane(tenant.clone(), run_id, event_id, payload)
                .map_err(|_| StoreError::InvalidAgentDeadline)?,
        )
        .map_err(|_| StoreError::InvalidAgentDeadline)?;
        let waits = child_cancellation::cancellation_waits(&mut tx, run).await?;
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
        tx.commit()
            .await
            .map_err(|source| StoreError::database("Agent deadline commit", source))?;
        Ok(AgentDeadlineCancellationOutcome::Requested(event.head()))
    }
}

fn hex_digest(value: Digest) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(64);
    for byte in value.as_bytes() {
        write!(&mut text, "{byte:02x}").expect("String write");
    }
    text
}

pub(super) async fn verify_schema(pool: &PgPool) -> Result<(), StoreError> {
    let installed = query_scalar::<_, String>(CATALOG_QUERY)
        .fetch_one(pool)
        .await
        .map_err(|source| StoreError::database("Agent deadline catalog", source))?;
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("agent_deadline_catalog.json"))
            .map_err(|_| StoreError::IncompleteSchema)?;
    if serde_json::from_str::<serde_json::Value>(&installed)
        .map_err(|_| StoreError::IncompleteSchema)?
        != expected
    {
        return Err(StoreError::IncompleteSchema);
    }
    for function in include_str!("../migrations/0023_agent_deadlines.sql")
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
            .bind(format!("stateknot.{name}()")).fetch_optional(pool).await
            .map_err(|source| StoreError::database("Agent deadline guard",source))?;
        if installed.as_deref() != Some(body) {
            return Err(StoreError::IncompleteSchema);
        }
    }
    Ok(())
}

const CATALOG_QUERY: &str = r"
SELECT jsonb_build_object(
 'column',(SELECT jsonb_build_array(format_type(atttypid,atttypmod),attnotnull,attgenerated,atthasdef) FROM pg_attribute WHERE attrelid='stateknot.runs'::regclass AND attname='agent_deadline_at' AND NOT attisdropped),
 'constraint',(SELECT jsonb_build_array(pg_get_constraintdef(oid),convalidated,condeferrable) FROM pg_constraint WHERE conrelid='stateknot.runs'::regclass AND conname='runs_agent_deadline_finite'),
 'index',(SELECT jsonb_build_array(pg_get_indexdef(indexrelid),indisvalid,indisready,indislive) FROM pg_index WHERE indexrelid=to_regclass('stateknot.runs_due_agent_deadlines')),
 'triggers',(SELECT jsonb_agg(jsonb_build_array(tgname,pg_get_triggerdef(oid),tgenabled) ORDER BY tgname) FROM pg_trigger WHERE NOT tgisinternal AND ((tgrelid='stateknot.runs'::regclass AND tgname='runs_agent_deadline_guard') OR (tgrelid='stateknot.agent_admissions'::regclass AND tgname='agent_admissions_deadline_capture')))
)::text
";
