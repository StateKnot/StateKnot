// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Finite failure-close maintenance, independent of execution ownership.

use crate::{
    DurableGraphLifecycleOptions, JsonSchemaRegistry, JsonSchemaRegistryBuilder,
    JsonSchemaRegistryError,
};
use serde_json::Value;
use stateknot_core::{
    BoundedJson, BoxFuture, CancellationSignal, Digest, EventId, FailureId, JournalAppend,
    JournalEventIntent, JournalEventKind, JournalExpectation, JournalPayload, SchemaReference,
    TenantId, Version,
};
use stateknot_store_postgres::{
    PostgresStore, RunFailureCloseCursor, RunFailureCloseOutcome, StoreError,
};
use std::time::Duration;
use thiserror::Error;

/// Offline immutable release identity; never fetched at runtime.
pub const STANDARD_RUN_FAILURE_CLOSE_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/run-failure-close-event/1.0.0";

/// Returns the exact closed embedded audit schema.
pub fn standard_run_failure_close_event_schema()
-> Result<(SchemaReference, Value), RunFailureCloseBuildError> {
    let document: Value = serde_json::from_str(include_str!(
        "../schemas/run-failure-close-event-1.0.0.json"
    ))
    .map_err(|_| RunFailureCloseBuildError::SchemaDefinition)?;
    let bytes = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| RunFailureCloseBuildError::SchemaDefinition)?;
    Ok((
        SchemaReference::new(
            STANDARD_RUN_FAILURE_CLOSE_EVENT_SCHEMA_ID
                .parse()
                .map_err(|_| RunFailureCloseBuildError::SchemaDefinition)?,
            Version::new(1, 0, 0),
            Digest::sha256(bytes),
        ),
        document,
    ))
}
/// Register before freezing a deployment with child-enabled graph lifecycles.
pub fn register_standard_run_failure_close_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, RunFailureCloseBuildError> {
    let (reference, document) = standard_run_failure_close_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}
/// Startup fails closed if the exact offline schema is unavailable.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RunFailureCloseBuildError {
    /// Malformed embedded release artifact.
    #[error("invalid embedded failure close schema")]
    SchemaDefinition,
    /// Deployment omitted the exact release schema.
    #[error("failure close schema is unavailable")]
    SchemaUnavailable,
    /// Schema registry refused registration.
    #[error(transparent)]
    Registry(#[from] JsonSchemaRegistryError),
}

/// Tenant-bound continuation. Reset after process loss; not a durable watermark.
#[derive(Clone, Debug)]
pub struct RunFailureCloseSweepCursor {
    tenant: TenantId,
    after: Option<RunFailureCloseCursor>,
}
/// One bounded candidate and its outcome; inspect errors even when later items succeed.
#[derive(Debug)]
pub struct RunFailureCloseItem {
    candidate: RunFailureCloseCursor,
    result: Result<RunFailureCloseOutcome, StoreError>,
}
impl RunFailureCloseItem {
    /// Tenant/run identity, not a metric label.
    #[must_use]
    pub const fn candidate(&self) -> &RunFailureCloseCursor {
        &self.candidate
    }
    /// Pending/unknown child evidence remains an error, never fabricated completion.
    pub fn result(&self) -> Result<&RunFailureCloseOutcome, &StoreError> {
        self.result.as_ref()
    }
}
/// At most 16 attempts at closure per tick, each with finite mutation retries.
#[derive(Debug)]
pub struct RunFailureCloseTick {
    cursor: RunFailureCloseSweepCursor,
    items: Vec<RunFailureCloseItem>,
    cancelled: bool,
}
impl RunFailureCloseTick {
    /// Preserve across ticks including item failures; exhausted sweeps reset automatically.
    #[must_use]
    pub const fn cursor(&self) -> &RunFailureCloseSweepCursor {
        &self.cursor
    }
    /// Every item is available for host supervision and alerting.
    #[must_use]
    pub fn items(&self) -> &[RunFailureCloseItem] {
        &self.items
    }
    /// Shutdown interrupted the tick; ambiguous commits remain recoverable.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        self.cancelled
    }
}

/// Completes original failures after child cancellation/settlement reconciliation.
/// The host must authorize tenants, supply fair recurring finite ticks, and inspect
/// every error. No task is implicitly spawned. Direct evidence was frozen by the
/// worker before lease release; this coordinator must not call providers again.
///
/// ```no_run
/// use stateknot_core::{CancellationSignal, TenantId};
/// use stateknot_runtime::{DurableRunFailureCloser, DurableGraphLifecycleOptions,
///     JsonSchemaRegistryBuilder, RunFailureCloseSweepCursor, RunFailureCloseTick,
///     register_standard_run_failure_close_event_schema};
/// use stateknot_store_postgres::{PostgresStore, StoreError};
/// fn configure(store: PostgresStore, mut schemas: JsonSchemaRegistryBuilder)
///     -> Result<DurableRunFailureCloser, Box<dyn std::error::Error>> {
///     register_standard_run_failure_close_event_schema(&mut schemas)?;
///     Ok(DurableRunFailureCloser::new(store, schemas.build()?,
///         DurableGraphLifecycleOptions::default())?)
/// }
/// async fn step(worker: &DurableRunFailureCloser, tenant: TenantId,
///     cursor: &mut Option<RunFailureCloseSweepCursor>, shutdown: CancellationSignal)
///     -> Result<RunFailureCloseTick, StoreError> {
///     let report = worker.tick(tenant, cursor.clone(), shutdown).await?;
///     *cursor = Some(report.cursor().clone());
///     // Inspect every item result; schedule another fair tick even after errors.
///     Ok(report)
/// }
/// ```
#[derive(Clone, Debug)]
pub struct DurableRunFailureCloser {
    store: PostgresStore,
    schemas: JsonSchemaRegistry,
    schema: SchemaReference,
    options: DurableGraphLifecycleOptions,
}
impl DurableRunFailureCloser {
    /// Requires the exact release schema and finite retry policy.
    pub fn new(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        options: DurableGraphLifecycleOptions,
    ) -> Result<Self, RunFailureCloseBuildError> {
        let (schema, _) = standard_run_failure_close_event_schema()?;
        if !schemas.contains(&schema) {
            return Err(RunFailureCloseBuildError::SchemaUnavailable);
        }
        Ok(Self {
            store,
            schemas,
            schema,
            options,
        })
    }
    /// Independent of runnable discovery. A bad child/run does not starve later candidates.
    pub fn tick(
        &self,
        tenant: TenantId,
        after: Option<RunFailureCloseSweepCursor>,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<RunFailureCloseTick, StoreError>> {
        Box::pin(async move {
            if after.as_ref().is_some_and(|cursor| cursor.tenant != tenant) {
                return Err(StoreError::InvalidRunFailureClose);
            }
            let mut tick = RunFailureCloseTick {
                cursor: after.unwrap_or(RunFailureCloseSweepCursor {
                    tenant: tenant.clone(),
                    after: None,
                }),
                items: Vec::with_capacity(16),
                cancelled: false,
            };
            let candidates = tokio::select! {
                biased;
                ()=shutdown.cancelled()=> {tick.cancelled=true;return Ok(tick);}
                result=self.store.pending_run_failure_closes_after(&tenant,tick.cursor.after.as_ref())=>result?,
            };
            let exhausted = candidates.len() < 16;
            for candidate in candidates {
                let result = tokio::select! {
                    biased;
                    ()=shutdown.cancelled()=> {tick.cancelled=true;break;}
                    result=self.complete_with_retry(&candidate)=>result,
                };
                tick.cursor.after = Some(candidate.clone());
                tick.items.push(RunFailureCloseItem { candidate, result });
            }
            if exhausted && !tick.cancelled {
                tick.cursor.after = None;
            }
            Ok(tick)
        })
    }
    async fn complete_with_retry(
        &self,
        candidate: &RunFailureCloseCursor,
    ) -> Result<RunFailureCloseOutcome, StoreError> {
        let event = EventId::generate();
        for attempt in 1..=self.options.maximum_mutation_attempts() {
            let result = self.complete(candidate, event).await;
            match result {
                Err(error)
                    if attempt < self.options.maximum_mutation_attempts()
                        && (error.is_retryable()
                            || matches!(
                                error,
                                StoreError::StaleJournalHead | StoreError::StaleLifecycleRevision
                            )) =>
                {
                    tokio::time::sleep(
                        self.options
                            .mutation_retry_initial_delay()
                            .saturating_mul(1_u32 << (attempt - 1))
                            .min(Duration::from_secs(1)),
                    )
                    .await;
                }
                other => return other,
            }
        }
        unreachable!("validated positive attempt count")
    }
    async fn complete(
        &self,
        candidate: &RunFailureCloseCursor,
        event: EventId,
    ) -> Result<RunFailureCloseOutcome, StoreError> {
        let record = self
            .store
            .load_run_failure_close(candidate.tenant_id(), candidate.run_id())
            .await?
            .ok_or(StoreError::InvalidRunFailureClose)?;
        if record.completed_at().is_some() {
            return Ok(RunFailureCloseOutcome::Existing(record));
        }
        let run = self
            .store
            .load_run(candidate.tenant_id(), candidate.run_id())
            .await?;
        let payload = payload(&self.schemas, &self.schema, record.failure().id(), true)?;
        let append = JournalAppend::new(
            JournalExpectation::exact(
                run.journal_head()
                    .cloned()
                    .ok_or(StoreError::InvalidRunFailureClose)?,
            ),
            JournalEventIntent::control_plane(
                candidate.tenant_id().clone(),
                candidate.run_id(),
                event,
                payload,
            )
            .map_err(|_| StoreError::InvalidRunFailureClose)?,
        )
        .map_err(|_| StoreError::InvalidRunFailureClose)?;
        self.store.complete_run_failure_close(append).await
    }
}

pub(crate) fn payload(
    schemas: &JsonSchemaRegistry,
    schema: &SchemaReference,
    failure: FailureId,
    complete: bool,
) -> Result<JournalPayload, StoreError> {
    let (operation, kind) = if complete {
        ("run_failure_close_completed", "run-failure-close-completed")
    } else {
        ("run_failure_close_requested", "run-failure-close-requested")
    };
    let data = BoundedJson::try_from_value(
        serde_json::json!({"operation":operation,"failure_id":failure.to_string()}),
    )
    .map_err(|_| StoreError::InvalidRunFailureClose)?;
    schemas
        .validate_bounded(schema, &data)
        .map_err(|_| StoreError::InvalidRunFailureClose)?;
    JournalPayload::new(
        schema.clone(),
        JournalEventKind::new(kind).expect("static event kind"),
        data,
    )
    .map_err(|_| StoreError::InvalidRunFailureClose)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn schema_is_offline_closed_and_exact() {
        let mut builder =
            JsonSchemaRegistryBuilder::new(crate::JsonSchemaRegistryLimits::default());
        let reference = register_standard_run_failure_close_event_schema(&mut builder).unwrap();
        assert_eq!(
            reference,
            standard_run_failure_close_event_schema().unwrap().0
        );
        let schemas = builder.build().unwrap();
        for complete in [false, true] {
            payload(&schemas, &reference, FailureId::generate(), complete).unwrap();
        }
        let data=BoundedJson::try_from_value(serde_json::json!({"operation":"run_failure_close_requested","failure_id":FailureId::generate().to_string(),"private_failure":"forbidden"})).unwrap();
        assert!(schemas.validate_bounded(&reference, &data).is_err());
    }
}
