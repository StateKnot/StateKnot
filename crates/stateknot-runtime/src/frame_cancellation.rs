// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Finite cold cancellation after an authenticated whole-frame closure.

use crate::{
    DurableGraphLifecycleOptions, JsonSchemaRegistry, StandardAgentCancellationSchemaError,
    standard_agent_cancellation_event_schema,
};
use serde::Serialize;
use stateknot_core::{
    BoxFuture, CancellationSignal, CheckpointHead, EventId, JournalAppend, JournalEventIntent,
    JournalEventSource, JournalExpectation, RunId, RunStatus, RunTransition, SchemaReference,
    TenantId,
};
use stateknot_store_postgres::{
    AppendOutcome, GraphFrameClosureCursor, JournalPageSize, PostgresStore, RunProjection,
    StoreError, StoredRun,
};
use std::time::Duration;
use thiserror::Error;

/// Startup failures; cancellation never falls back to an unvalidated schema.
#[derive(Debug, Error)]
pub enum FrameCancellationBuildError {
    /// The embedded cancellation schema is malformed.
    #[error(transparent)]
    SchemaDefinition(#[from] StandardAgentCancellationSchemaError),
    /// The deployment omitted the exact release cancellation schema.
    #[error("frame cancellation schema is unavailable")]
    SchemaUnavailable,
}

/// A result backed by a complete owning closure and the actual journal append.
#[derive(Clone, Debug)]
pub enum FrameCancellationOutcome {
    /// The real append committed or its exact historical result was recovered.
    /// The box only owns the actual append result; it is not a permission token.
    Cancelled(Box<AppendOutcome>),
    /// No whole closure exists, or its owning proof selects sealed failure.
    NotApplicable,
}

/// One attempted raw candidate, retaining its exact result for supervision.
#[derive(Debug)]
pub struct FrameCancellationItem {
    candidate: GraphFrameClosureCursor,
    result: Result<FrameCancellationOutcome, StoreError>,
}
impl FrameCancellationItem {
    /// Actual tenant/run cursor, not a metric label or execution permission.
    #[must_use]
    pub const fn candidate(&self) -> &GraphFrameClosureCursor {
        &self.candidate
    }
    /// Item errors remain visible even when a later candidate succeeds.
    pub fn result(&self) -> Result<&FrameCancellationOutcome, &StoreError> {
        self.result.as_ref()
    }
}

/// One raw page of at most 16 keys, including progress past terminal metadata.
#[derive(Debug)]
pub struct FrameCancellationTick {
    cursor: Option<GraphFrameClosureCursor>,
    items: Vec<FrameCancellationItem>,
    cancelled: bool,
}
impl FrameCancellationTick {
    /// Last processed raw cursor; exhaustion resets the sweep to `None`.
    #[must_use]
    pub const fn cursor(&self) -> Option<&GraphFrameClosureCursor> {
        self.cursor.as_ref()
    }
    /// Actual attempts/errors; terminal metadata skips confer no ACK result.
    #[must_use]
    pub fn items(&self) -> &[FrameCancellationItem] {
        &self.items
    }
    /// Shutdown stopped discovery or admission before an unprocessed key.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        self.cancelled
    }
}

/// Confirms an original cancellation without execution leases or providers.
///
/// The host authorizes each tenant, runs fair recurring ticks, retains cursors,
/// and inspects item errors. A complete Store owning read precedes every new
/// candidate or cold ACK. Existing atomic terminal accounting revalidates that
/// proof and the settled children before committing. No task is spawned here.
#[derive(Clone, Debug)]
pub struct DurableFrameCancellationFinalizer {
    store: PostgresStore,
    schemas: JsonSchemaRegistry,
    schema: SchemaReference,
    options: DurableGraphLifecycleOptions,
}

struct PreparedCancellation {
    append: JournalAppend,
    projection: RunProjection,
}

impl DurableFrameCancellationFinalizer {
    /// Requires the exact offline cancellation schema and existing retry policy.
    pub fn new(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        options: DurableGraphLifecycleOptions,
    ) -> Result<Self, FrameCancellationBuildError> {
        let (schema, _) = standard_agent_cancellation_event_schema()?;
        if !schemas.contains(&schema) {
            return Err(FrameCancellationBuildError::SchemaUnavailable);
        }
        Ok(Self {
            store,
            schemas,
            schema,
            options,
        })
    }

    /// Finishes one original cancellation or authenticates its actual terminal ACK.
    /// No old handoff, candidate `EventId`, live fence or executable registry is needed.
    pub fn resume<'a>(
        &'a self,
        tenant: &'a TenantId,
        run: RunId,
    ) -> BoxFuture<'a, Result<FrameCancellationOutcome, StoreError>> {
        Box::pin(async move {
            let mut pending = None;
            for attempt in 1..=self.options.maximum_mutation_attempts() {
                let result = self.resume_attempt(tenant, run, &mut pending).await;
                match result {
                    Err(error)
                        if attempt < self.options.maximum_mutation_attempts()
                            && (error.is_retryable() || stale_observation(&error)) =>
                    {
                        // Unknown COMMIT outcomes retry the identical ID/time/usage.
                        // An explicit CAS loss instead reloads the actual winner.
                        if stale_observation(&error) {
                            pending = None;
                        }
                        self.backoff(attempt).await;
                    }
                    other => return other,
                }
            }
            unreachable!("validated positive mutation attempt count")
        })
    }

    /// Processes one finite raw PK page and retains progress past every item error.
    /// Only read-only discovery may be abandoned on shutdown. Once an item is
    /// admitted, await its complete finite mutation/ACK recovery before stopping.
    pub fn tick(
        &self,
        tenant: TenantId,
        after: Option<GraphFrameClosureCursor>,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<FrameCancellationTick, StoreError>> {
        Box::pin(async move {
            // Scope validation precedes even an already-triggered shutdown.
            if after
                .as_ref()
                .is_some_and(|cursor| cursor.tenant_id() != &tenant)
            {
                return Err(StoreError::InvalidGraphFrameClosureCursor);
            }
            let mut tick = FrameCancellationTick {
                cursor: after,
                items: Vec::with_capacity(16),
                cancelled: false,
            };
            let page = tokio::select! {
                biased;
                () = shutdown.cancelled() => { tick.cancelled = true; return Ok(tick); }
                result = self.store.graph_frame_closure_page(&tenant, tick.cursor.as_ref()) => result?,
            };
            let mut processed = 0;
            for candidate in page.candidates() {
                let metadata = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => { tick.cancelled = true; break; }
                    result = self.store.load_run(candidate.tenant_id(), candidate.run_id()) => result,
                };
                let result = match metadata {
                    // Discovery only: this skip does not authenticate an ACK.
                    Ok(run) if run.lifecycle().status().is_terminal() => None,
                    Ok(_) => Some(self.resume(candidate.tenant_id(), candidate.run_id()).await),
                    Err(error) => Some(Err(error)),
                };
                tick.cursor = Some(candidate.clone());
                processed += 1;
                if let Some(result) = result {
                    tick.items.push(FrameCancellationItem {
                        candidate: candidate.clone(),
                        result,
                    });
                }
            }
            if processed == page.candidates().len() && !page.has_more() {
                tick.cursor = None;
            }
            Ok(tick)
        })
    }

    async fn resume_attempt(
        &self,
        tenant: &TenantId,
        run: RunId,
        pending: &mut Option<Box<PreparedCancellation>>,
    ) -> Result<FrameCancellationOutcome, StoreError> {
        if pending.is_none() {
            *pending = self.prepare(tenant, run).await?;
        }
        let Some(prepared) = pending.as_ref() else {
            return Ok(FrameCancellationOutcome::NotApplicable);
        };
        let outcome = Box::pin(
            self.store
                .append_control_plane(prepared.append.clone(), prepared.projection.clone()),
        )
        .await?;
        Ok(FrameCancellationOutcome::Cancelled(Box::new(outcome)))
    }

    #[allow(clippy::too_many_lines)]
    fn prepare<'a>(
        &'a self,
        tenant: &'a TenantId,
        run: RunId,
    ) -> BoxFuture<'a, Result<Option<Box<PreparedCancellation>>, StoreError>> {
        Box::pin(async move {
            let Some(record) = self.store.load_graph_frame_closure(tenant, run).await? else {
                return Ok(None);
            };
            let closure = Box::new(record);
            let Some(original) = closure.original_cancellation_lifecycle() else {
                return Ok(None);
            };
            let root = closure.root_checkpoint();
            if root.tenant_id() != tenant
                || root.run_id() != run
                || original.provenance().tenant_id() != tenant
                || original.provenance().run_id() != run
                || original.status() != RunStatus::CancellationRequested
                || original
                    .cancellation_request()
                    .is_none_or(|request| request.failure().id() != closure.failure().id())
            {
                return Err(StoreError::GraphFrameConflict);
            }
            let current = Box::pin(self.store.load_run(tenant, run)).await?;
            validate_root_pointer(&current, root)?;
            let usage = Box::pin(self.store.include_child_usage(
                tenant,
                run,
                closure.direct_usage().clone(),
            ))
            .await?;
            let payload = crate::lifecycle::cancelled_payload(
                &self.schemas,
                &self.schema,
                root,
                closure.failure().id(),
            )
            .map_err(|_| StoreError::GraphFrameConflict)?;
            let (event_id, completed_at) = match current.lifecycle().status() {
                RunStatus::CancellationRequested => {
                    if current.is_quarantined() {
                        return Err(StoreError::RunQuarantined);
                    }
                    if !canonical_equal(current.lifecycle(), original)? {
                        return Err(StoreError::GraphFrameConflict);
                    }
                    if current.journal_head() != Some(&closure.event().head()) {
                        return Err(StoreError::StaleJournalHead);
                    }
                    let now = self.store.observe_database_clock().await?;
                    if now < closure.event().recorded_at() || now < original.changed_at() {
                        return Err(StoreError::DatabaseClockRegression);
                    }
                    (EventId::generate(), now)
                }
                RunStatus::Cancelled => {
                    let completed_at = current.lifecycle().changed_at();
                    if current.lifecycle().terminal_usage() != Some(&usage) {
                        return Err(StoreError::IncompleteChildAccounting);
                    }
                    let expected = original
                        .clone()
                        .apply(RunTransition::ConfirmCancellation {
                            completed_at,
                            usage: usage.clone(),
                        })
                        .map_err(|_| StoreError::InvalidLifecycleTransition)?;
                    if !canonical_equal(current.lifecycle(), &expected)? {
                        return Err(StoreError::GraphFrameConflict);
                    }
                    let page = Box::pin(self.store.load_journal_page(
                        tenant,
                        run,
                        Some(&closure.event().head()),
                        JournalPageSize::new(1)?,
                    ))
                    .await?;
                    let event = page
                        .events()
                        .first()
                        .ok_or(StoreError::GraphFrameConflict)?;
                    if page.events().len() != 1
                        || page.has_more()
                        || current.journal_head() != Some(&event.head())
                        || event.tenant_id() != tenant
                        || event.run_id() != run
                        || closure.event().sequence().get().checked_add(1)
                            != Some(event.sequence().get())
                        || event.previous_digest() != Some(closure.event().digest())
                        || event.source() != &JournalEventSource::ControlPlane
                        || event.payload() != &payload
                        || completed_at < closure.event().recorded_at()
                        || completed_at > event.recorded_at()
                    {
                        return Err(StoreError::GraphFrameConflict);
                    }
                    // Persistence time can be later than the actual transition.
                    // Rebuild from the committed lifecycle, never recorded_at.
                    (event.event_id(), completed_at)
                }
                _ => return Err(StoreError::GraphFrameConflict),
            };
            let append = JournalAppend::new(
                JournalExpectation::exact(closure.event().head()),
                JournalEventIntent::control_plane(tenant.clone(), run, event_id, payload)
                    .map_err(|_| StoreError::GraphFrameConflict)?,
            )
            .map_err(|_| StoreError::GraphFrameConflict)?;
            Ok(Some(Box::new(PreparedCancellation {
                append,
                projection: RunProjection::transition(
                    original.revision(),
                    RunTransition::ConfirmCancellation {
                        completed_at,
                        usage,
                    },
                ),
            })))
        })
    }

    async fn backoff(&self, attempt: u8) {
        let delay = self
            .options
            .mutation_retry_initial_delay()
            .saturating_mul(1_u32 << (attempt - 1))
            .min(Duration::from_secs(1));
        tokio::time::sleep(delay).await;
    }
}

fn stale_observation(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::StaleJournalHead | StoreError::StaleLifecycleRevision
    )
}

fn canonical_equal<T: Serialize>(left: &T, right: &T) -> Result<bool, StoreError> {
    let encode =
        |value| serde_json_canonicalizer::to_vec(value).map_err(|_| StoreError::GraphFrameConflict);
    Ok(encode(left)? == encode(right)?)
}

fn validate_root_pointer(current: &StoredRun, root: &CheckpointHead) -> Result<(), StoreError> {
    let matches = current.checkpoint().is_some_and(|pointer| {
        pointer.checkpoint_id() == root.checkpoint_id()
            && pointer.superstep() == root.superstep()
            && pointer.digest() == root.digest()
    });
    if current.lease().is_some() || !matches {
        return Err(StoreError::GraphFrameConflict);
    }
    Ok(())
}
