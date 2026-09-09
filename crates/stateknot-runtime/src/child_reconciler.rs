// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Trusted tenant-scoped child cancellation and accounting maintenance.

use crate::{JsonSchemaRegistry, JsonSchemaRegistryBuilder, JsonSchemaRegistryError};
use serde_json::{Value, json};
use stateknot_core::{
    BoundedJson, BoxFuture, CancellationSignal, ChildRunKey, Digest, EventId, Failure,
    FailureCategory, FailureCode, FailureId, FailureMessage, FailureOrigin, JournalAppend,
    JournalEventIntent, JournalEventKind, JournalExpectation, RetryAdvice, RunCancellationRequest,
    SchemaReference, TenantId, Version,
};
use stateknot_store_postgres::{
    ChildCancellationDelivery, ChildCancellationOutcome, ChildRunSettlementOutcome, PostgresStore,
    StoreError,
};
use std::{fmt, fmt::Write, time::Duration};
use thiserror::Error;

/// Offline identity of the embedded reconciliation audit schema.
pub const STANDARD_CHILD_RECONCILIATION_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/child-reconciliation-event/1.0.0";

/// Returns the closed schema shipped in the binary; no HTTP lookup is performed.
pub fn standard_child_reconciliation_event_schema()
-> Result<(SchemaReference, Value), ChildReconcilerBuildError> {
    let document: Value = serde_json::from_str(include_str!(
        "../schemas/child-reconciliation-event-1.0.0.json"
    ))
    .map_err(|_| ChildReconcilerBuildError::SchemaDefinition)?;
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| ChildReconcilerBuildError::SchemaDefinition)?;
    let id = STANDARD_CHILD_RECONCILIATION_EVENT_SCHEMA_ID
        .parse()
        .map_err(|_| ChildReconcilerBuildError::SchemaDefinition)?;
    Ok((
        SchemaReference::new(id, Version::new(1, 0, 0), Digest::sha256(canonical)),
        document,
    ))
}

/// Registers the exact audit schema before freezing the deployment registry.
pub fn register_standard_child_reconciliation_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, ChildReconcilerBuildError> {
    let (reference, document) = standard_child_reconciliation_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}

/// Finite retry policy. Each tick scans at most 16 cancellation and 16 settlement keys.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableChildReconcilerOptions {
    maximum_mutation_attempts: u8,
    retry_initial_delay: Duration,
}

impl DurableChildReconcilerOptions {
    /// Rejects attempts outside 1–10 and initial delay outside (0, 1 second].
    pub fn new(
        maximum_mutation_attempts: u8,
        retry_initial_delay: Duration,
    ) -> Result<Self, ChildReconcilerBuildError> {
        if !(1..=10).contains(&maximum_mutation_attempts)
            || retry_initial_delay.is_zero()
            || retry_initial_delay > Duration::from_secs(1)
        {
            return Err(ChildReconcilerBuildError::InvalidOptions);
        }
        Ok(Self {
            maximum_mutation_attempts,
            retry_initial_delay,
        })
    }
    /// Returns the hard bound on fresh/read-recovery attempts for one key.
    #[must_use]
    pub const fn maximum_mutation_attempts(self) -> u8 {
        self.maximum_mutation_attempts
    }

    pub(crate) fn retry_delay(self, attempt: u8) -> Duration {
        self.retry_initial_delay
            .saturating_mul(1_u32 << (attempt - 1))
            .min(Duration::from_secs(1))
    }
}
impl Default for DurableChildReconcilerOptions {
    fn default() -> Self {
        Self {
            maximum_mutation_attempts: 3,
            retry_initial_delay: Duration::from_millis(25),
        }
    }
}

/// In-process scan continuation. Retain it between ticks; restart at `None` after a crash.
/// Each lane automatically starts a new sweep once it reaches the end.
#[derive(Clone, Debug)]
pub struct ChildReconciliationCursor {
    tenant: TenantId,
    cancellation_after: Option<ChildRunKey>,
    settlement_after: Option<ChildRunKey>,
}

/// Maintenance lane, for bounded-cardinality counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildReconciliationKind {
    /// Deliver durable parent cancellation without confirming child termination.
    Cancellation,
    /// Replace one reservation with verified terminal usage once.
    Settlement,
}

/// Successful per-key maintenance observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildReconciliationCommit {
    /// Newly consumed cancellation work; includes whether the child was already terminal.
    Cancellation(ChildCancellationOutcome),
    /// Original cancellation receipt was recovered without another mutation.
    CancellationRecovered,
    /// Newly recorded terminal settlement.
    Settlement,
    /// Original settlement was recovered without another charge.
    SettlementRecovered,
}

/// One bounded, inspectable result. Errors do not prevent other keys from progressing.
pub struct ChildReconciliationItem {
    key: ChildRunKey,
    kind: ChildReconciliationKind,
    result: Result<ChildReconciliationCommit, StoreError>,
}
impl ChildReconciliationItem {
    /// Returns ownership for secure operator correlation, not a metric label.
    #[must_use]
    pub const fn key(&self) -> &ChildRunKey {
        &self.key
    }
    /// Returns the lane that processed this key.
    #[must_use]
    pub const fn kind(&self) -> ChildReconciliationKind {
        self.kind
    }
    /// Returns complete typed success or failure. Unknown cost remains an explicit error.
    pub fn result(&self) -> Result<ChildReconciliationCommit, &StoreError> {
        self.result.as_ref().copied()
    }
}
impl fmt::Debug for ChildReconciliationItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildReconciliationItem")
            .field("key", &self.key)
            .field("kind", &self.kind)
            .field("result", &self.result.as_ref().map_err(ToString::to_string))
            .finish()
    }
}

/// Result of one finite tick; this is neither a child Join nor an Agent execution result.
#[derive(Debug)]
pub struct ChildReconciliationTick {
    cursor: ChildReconciliationCursor,
    items: Vec<ChildReconciliationItem>,
    cancelled: bool,
}
impl ChildReconciliationTick {
    /// Pass this continuation to the next tick, including after per-item errors.
    #[must_use]
    pub const fn cursor(&self) -> &ChildReconciliationCursor {
        &self.cursor
    }
    /// Returns up to 32 results. Inspect errors and alert on unresolved/corrupt evidence.
    #[must_use]
    pub fn items(&self) -> &[ChildReconciliationItem] {
        &self.items
    }
    /// Indicates cooperative shutdown; unfinished work remains in durable queues.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        self.cancelled
    }
}

/// Startup validation error; no runtime network schema loading is allowed.
#[derive(Debug, Error)]
pub enum ChildReconcilerBuildError {
    /// Retry policy would be unbounded or outside supported limits.
    #[error("invalid child reconciler retry policy")]
    InvalidOptions,
    /// Embedded release schema is invalid.
    #[error("invalid embedded child reconciliation schema")]
    SchemaDefinition,
    /// Deployment omitted the exact embedded schema.
    #[error("child reconciliation schema is absent from the deployment registry")]
    SchemaUnavailable,
    /// Registry rejected schema registration.
    #[error(transparent)]
    Registry(#[from] JsonSchemaRegistryError),
}

/// Trusted control-plane worker for owned children; never dispatches user/provider code.
/// The caller authenticates the tenant and controls scheduling, fairness and tick cadence.
/// Schedule alongside execution workers. No lease or transaction is held between ticks.
///
/// Register the schema in the same builder as the deployment's other schemas.
/// The host retains the continuation and inspects each result; this helper is
/// one finite maintenance step, not an execution loop or a child Join.
///
/// ```no_run
/// use stateknot_core::{CancellationSignal, TenantId};
/// use stateknot_runtime::{
///     ChildReconciliationCursor, ChildReconciliationTick, DurableChildReconciler,
///     DurableChildReconcilerOptions, JsonSchemaRegistryBuilder,
///     register_standard_child_reconciliation_event_schema,
/// };
/// use stateknot_store_postgres::{PostgresStore, StoreError};
///
/// fn configure(
///     store: PostgresStore,
///     mut deployment_schemas: JsonSchemaRegistryBuilder,
/// ) -> Result<DurableChildReconciler, Box<dyn std::error::Error>> {
///     register_standard_child_reconciliation_event_schema(&mut deployment_schemas)?;
///     Ok(DurableChildReconciler::new(
///         store, deployment_schemas.build()?, DurableChildReconcilerOptions::default(),
///     )?)
/// }
///
/// async fn maintenance_step(
///     worker: &DurableChildReconciler,
///     authorized_tenant: TenantId,
///     continuation: &mut Option<ChildReconciliationCursor>,
///     shutdown: CancellationSignal,
/// ) -> Result<ChildReconciliationTick, StoreError> {
///     let report = worker.tick(authorized_tenant, continuation.clone(), shutdown).await?;
///     *continuation = Some(report.cursor().clone());
///     // Return every per-item error to the host's alerting/reconciliation policy.
///     // After process loss start a new sweep with None; no durable work is lost.
///     Ok(report)
/// }
/// ```
#[derive(Clone, Debug)]
pub struct DurableChildReconciler {
    store: PostgresStore,
    schemas: JsonSchemaRegistry,
    schema: SchemaReference,
    options: DurableChildReconcilerOptions,
}
impl DurableChildReconciler {
    /// Freezes the pool, retry policy and exact offline audit schema at startup.
    pub fn new(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        options: DurableChildReconcilerOptions,
    ) -> Result<Self, ChildReconcilerBuildError> {
        let (schema, _) = standard_child_reconciliation_event_schema()?;
        if !schemas.contains(&schema) {
            return Err(ChildReconcilerBuildError::SchemaUnavailable);
        }
        Ok(Self {
            store,
            schemas,
            schema,
            options,
        })
    }

    /// Reconciles one page per lane. Inspect per-key errors and retain the cursor so
    /// an unknown-price/quarantined item cannot permanently block later work.
    /// Shutdown may win during an ambiguous commit; next ticks recover durable facts.
    pub fn tick(
        &self,
        tenant: TenantId,
        after: Option<ChildReconciliationCursor>,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<ChildReconciliationTick, StoreError>> {
        Box::pin(self.tick_inner(tenant, after, shutdown))
    }

    async fn tick_inner(
        &self,
        tenant: TenantId,
        after: Option<ChildReconciliationCursor>,
        shutdown: CancellationSignal,
    ) -> Result<ChildReconciliationTick, StoreError> {
        if after.as_ref().is_some_and(|cursor| cursor.tenant != tenant) {
            return Err(StoreError::ChildRunRejected);
        }
        let mut tick = ChildReconciliationTick {
            cursor: after.unwrap_or(ChildReconciliationCursor {
                tenant: tenant.clone(),
                cancellation_after: None,
                settlement_after: None,
            }),
            items: Vec::with_capacity(32),
            cancelled: false,
        };
        for kind in [
            ChildReconciliationKind::Cancellation,
            ChildReconciliationKind::Settlement,
        ] {
            if shutdown.is_cancelled() {
                tick.cancelled = true;
                break;
            }
            let keys = tokio::select! {
                biased;
                () = shutdown.cancelled() => { tick.cancelled = true; break; }
                keys = async {
                    match kind {
                        ChildReconciliationKind::Cancellation => self.store.pending_child_cancellations_after(&tenant, tick.cursor.cancellation_after.as_ref()).await,
                        ChildReconciliationKind::Settlement => self.store.pending_child_settlements_after(&tenant, tick.cursor.settlement_after.as_ref()).await,
                    }
                } => keys?,
            };
            let exhausted = keys.len() < 16;
            for key in keys {
                let result = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => { tick.cancelled = true; break; }
                    result = self.reconcile_with_retry(&key, kind) => result,
                };
                match kind {
                    ChildReconciliationKind::Cancellation => {
                        tick.cursor.cancellation_after = Some(key.clone());
                    }
                    ChildReconciliationKind::Settlement => {
                        tick.cursor.settlement_after = Some(key.clone());
                    }
                }
                tick.items
                    .push(ChildReconciliationItem { key, kind, result });
            }
            if tick.cancelled {
                break;
            }
            if exhausted {
                match kind {
                    ChildReconciliationKind::Cancellation => tick.cursor.cancellation_after = None,
                    ChildReconciliationKind::Settlement => tick.cursor.settlement_after = None,
                }
            }
        }
        Ok(tick)
    }

    async fn reconcile_with_retry(
        &self,
        key: &ChildRunKey,
        kind: ChildReconciliationKind,
    ) -> Result<ChildReconciliationCommit, StoreError> {
        let event_id = EventId::generate();
        let failure_id = FailureId::generate();
        for attempt in 1..=self.options.maximum_mutation_attempts {
            let result = match kind {
                ChildReconciliationKind::Cancellation => {
                    Box::pin(self.cancel(key, event_id, failure_id)).await
                }
                ChildReconciliationKind::Settlement => Box::pin(self.settle(key, event_id)).await,
            };
            match result {
                Err(error)
                    if attempt < self.options.maximum_mutation_attempts
                        && (error.is_retryable()
                            || matches!(
                                error,
                                StoreError::StaleJournalHead | StoreError::StaleLifecycleRevision
                            )) =>
                {
                    let delay = self
                        .options
                        .retry_initial_delay
                        .saturating_mul(1_u32 << (attempt - 1))
                        .min(Duration::from_secs(1));
                    tokio::time::sleep(delay).await;
                }
                other => return other,
            }
        }
        unreachable!("validated positive attempt count always returns")
    }

    async fn cancel(
        &self,
        key: &ChildRunKey,
        event_id: EventId,
        failure_id: FailureId,
    ) -> Result<ChildReconciliationCommit, StoreError> {
        let record = self.store.load_child_cancellation(key).await?;
        if record.receipt().is_some() {
            return Ok(ChildReconciliationCommit::CancellationRecovered);
        }
        let child = self
            .store
            .load_run(key.tenant_id(), record.child_run_id())
            .await?;
        let at = self.store.observe_database_clock().await?;
        let failure = Failure::new(
            failure_id,
            FailureCategory::Cancelled,
            FailureCode::new(if record.is_parent_failure_close() {
                "child.parent_failed"
            } else {
                "child.parent_cancelled"
            })
            .expect("static code"),
            FailureOrigin::new("stateknot.runtime.child_reconciler").expect("static origin"),
            FailureMessage::new(if record.is_parent_failure_close() {
                "The owning parent is closing after failure."
            } else {
                "The owning parent requested cancellation."
            })
            .expect("static public message"),
            RetryAdvice::Never,
        )
        .expect("static cancellation semantics")
        .with_caused_by_event(event_id);
        let request =
            RunCancellationRequest::new(failure, at).map_err(|_| StoreError::ChildRunRejected)?;
        let append = self.append(
            key,
            record.child_run_id(),
            child
                .journal_head()
                .cloned()
                .ok_or(StoreError::ChildRunRejected)?,
            event_id,
            "child-run-cancellation-requested",
            "child_run_cancellation_requested",
        )?;
        match Box::pin(self.store.deliver_child_cancellation(key, append, request)).await? {
            ChildCancellationDelivery::Committed(record) => {
                Ok(ChildReconciliationCommit::Cancellation(
                    record
                        .receipt()
                        .ok_or(StoreError::ChildRunRejected)?
                        .outcome(),
                ))
            }
            ChildCancellationDelivery::Idempotent(_) => {
                Ok(ChildReconciliationCommit::CancellationRecovered)
            }
        }
    }

    async fn settle(
        &self,
        key: &ChildRunKey,
        event_id: EventId,
    ) -> Result<ChildReconciliationCommit, StoreError> {
        let parent = self
            .store
            .load_run(key.tenant_id(), key.parent_run_id())
            .await?;
        let append = self.append(
            key,
            key.parent_run_id(),
            parent
                .journal_head()
                .cloned()
                .ok_or(StoreError::ChildRunRejected)?,
            event_id,
            "child-run-settled",
            "child_run_settled",
        )?;
        match Box::pin(self.store.settle_child_run(key, append)).await? {
            ChildRunSettlementOutcome::Committed { .. } => {
                Ok(ChildReconciliationCommit::Settlement)
            }
            ChildRunSettlementOutcome::Idempotent { .. } => {
                Ok(ChildReconciliationCommit::SettlementRecovered)
            }
            _ => Err(StoreError::ChildRunRejected),
        }
    }

    fn append(
        &self,
        key: &ChildRunKey,
        run: stateknot_core::RunId,
        head: stateknot_core::JournalHead,
        id: EventId,
        kind: &str,
        operation: &str,
    ) -> Result<JournalAppend, StoreError> {
        let mut digest = String::with_capacity(64);
        for value in key.digest().as_bytes() {
            write!(digest, "{value:02x}").expect("String writes are infallible");
        }
        let data = BoundedJson::try_from_value(
            json!({ "operation": operation, "ownership_digest": digest }),
        )
        .map_err(|_| StoreError::ChildRunRejected)?;
        self.schemas
            .validate_bounded(&self.schema, &data)
            .map_err(|_| StoreError::ChildRunRejected)?;
        let payload = stateknot_core::JournalPayload::new(
            self.schema.clone(),
            JournalEventKind::new(kind).map_err(|_| StoreError::ChildRunRejected)?,
            data,
        )
        .map_err(|_| StoreError::ChildRunRejected)?;
        let intent = JournalEventIntent::control_plane(key.tenant_id().clone(), run, id, payload)
            .map_err(|_| StoreError::ChildRunRejected)?;
        JournalAppend::new(JournalExpectation::exact(head), intent)
            .map_err(|_| StoreError::ChildRunRejected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JsonSchemaRegistryLimits;
    #[test]
    fn schema_is_closed_pinned_and_retry_bounds_are_finite() {
        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        let schema = register_standard_child_reconciliation_event_schema(&mut builder).unwrap();
        assert_eq!(
            schema.digest().to_string(),
            "sha256:54346f985269669cbdd22b6c70c77aebbadb5953fc96c45518af3086f4cdfd08"
        );
        let registry = builder.build().unwrap();
        for operation in ["child_run_settled", "child_run_cancellation_requested"] {
            let mut value = json!({ "operation": operation, "ownership_digest": "a".repeat(64) });
            registry
                .validate_bounded(
                    &schema,
                    &BoundedJson::try_from_value(value.clone()).unwrap(),
                )
                .unwrap();
            value["private_parent_reason"] = json!("must not leak");
            assert!(
                registry
                    .validate_bounded(&schema, &BoundedJson::try_from_value(value).unwrap())
                    .is_err()
            );
        }
        assert!(DurableChildReconcilerOptions::new(0, Duration::from_millis(1)).is_err());
        assert!(DurableChildReconcilerOptions::new(11, Duration::from_millis(1)).is_err());
        assert!(DurableChildReconcilerOptions::new(3, Duration::ZERO).is_err());
        assert!(DurableChildReconcilerOptions::new(3, Duration::from_secs(2)).is_err());
    }
}
