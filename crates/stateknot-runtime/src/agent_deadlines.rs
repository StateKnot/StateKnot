// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Finite tenant-scoped deadline supervision, independent of execution leases.

use crate::{
    DurableGraphLifecycleOptions, JsonSchemaRegistry, JsonSchemaRegistryBuilder,
    JsonSchemaRegistryError,
};
use serde_json::Value;
use stateknot_core::{
    BoxFuture, CancellationSignal, Digest, EventId, FailureId, SchemaReference, TenantId, Version,
};
use stateknot_store_postgres::{
    AgentDeadlineCancellationOutcome, AgentDeadlineCursor, PostgresStore, StoreError,
};
use std::{fmt, time::Duration};
use thiserror::Error;

/// Offline identity of the exact embedded audit schema.
pub const STANDARD_AGENT_DEADLINE_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/agent-deadline-event/1.0.0";

/// Returns the closed release schema without fetching it over HTTP.
pub fn standard_agent_deadline_event_schema()
-> Result<(SchemaReference, Value), AgentDeadlineBuildError> {
    let document: Value =
        serde_json::from_str(include_str!("../schemas/agent-deadline-event-1.0.0.json"))
            .map_err(|_| AgentDeadlineBuildError::SchemaDefinition)?;
    let bytes = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| AgentDeadlineBuildError::SchemaDefinition)?;
    Ok((
        SchemaReference::new(
            STANDARD_AGENT_DEADLINE_EVENT_SCHEMA_ID
                .parse()
                .map_err(|_| AgentDeadlineBuildError::SchemaDefinition)?,
            Version::new(1, 0, 0),
            Digest::sha256(bytes),
        ),
        document,
    ))
}

/// Registers the exact deadline schema before the deployment registry freezes.
pub fn register_standard_agent_deadline_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, AgentDeadlineBuildError> {
    let (reference, document) = standard_agent_deadline_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}

/// Startup failures; none falls back to unvalidated audit payloads.
#[derive(Debug, Error)]
pub enum AgentDeadlineBuildError {
    /// Embedded release schema is malformed.
    #[error("invalid embedded Agent deadline schema")]
    SchemaDefinition,
    /// The deployment did not register the exact schema.
    #[error("Agent deadline schema is absent from the deployment registry")]
    SchemaUnavailable,
    /// Schema registry refused registration.
    #[error(transparent)]
    Registry(#[from] JsonSchemaRegistryError),
}

/// Per-tenant scan continuation. Restart with `None` after process loss.
#[derive(Clone, Debug)]
pub struct AgentDeadlineSweepCursor {
    tenant: TenantId,
    after: Option<AgentDeadlineCursor>,
}

/// One candidate's typed result; payload bodies are not accumulated in a tick.
pub struct AgentDeadlineItem {
    candidate: AgentDeadlineCursor,
    result: Result<AgentDeadlineCancellationOutcome, StoreError>,
}
impl AgentDeadlineItem {
    /// Exact admitted deadline and tenant/run identity, not a metric label.
    #[must_use]
    pub const fn candidate(&self) -> &AgentDeadlineCursor {
        &self.candidate
    }
    /// Inspect every error; a failed candidate is revisited on the next sweep.
    pub fn result(&self) -> Result<&AgentDeadlineCancellationOutcome, &StoreError> {
        self.result.as_ref()
    }
}
impl fmt::Debug for AgentDeadlineItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentDeadlineItem")
            .field("candidate", &self.candidate)
            .field("result", &self.result.as_ref().map_err(ToString::to_string))
            .finish()
    }
}

/// A finite maintenance quantum, not a terminal Run outcome.
#[derive(Debug)]
pub struct AgentDeadlineTick {
    cursor: AgentDeadlineSweepCursor,
    items: Vec<AgentDeadlineItem>,
    cancelled: bool,
}
impl AgentDeadlineTick {
    /// Retain between ticks, including after per-item errors. Exhausted sweeps reset themselves.
    #[must_use]
    pub const fn cursor(&self) -> &AgentDeadlineSweepCursor {
        &self.cursor
    }
    /// At most 16 observations, with failures preserved for host supervision.
    #[must_use]
    pub fn items(&self) -> &[AgentDeadlineItem] {
        &self.items
    }
    /// Shutdown interrupted work; any ambiguous commit remains logically recoverable.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        self.cancelled
    }
}

/// Trusted deadline maintenance for admitted roots and children. The host must
/// authorize each tenant, enforce fairness/cadence, preserve cursors, and alert
/// on failures and sweep lag. Run this alongside child reconciliation and
/// independently leased execution/cleanup workers, even when no run is runnable.
/// No implicit task is spawned and no execution lease is held between ticks.
///
/// ```no_run
/// use stateknot_core::{CancellationSignal, TenantId};
/// use stateknot_runtime::{AgentDeadlineSweepCursor, AgentDeadlineTick,
///     DurableAgentDeadlineReconciler, DurableGraphLifecycleOptions,
///     JsonSchemaRegistryBuilder, register_standard_agent_deadline_event_schema};
/// use stateknot_store_postgres::{PostgresStore, StoreError};
/// fn configure(store: PostgresStore, mut schemas: JsonSchemaRegistryBuilder)
///     -> Result<DurableAgentDeadlineReconciler, Box<dyn std::error::Error>> {
///     register_standard_agent_deadline_event_schema(&mut schemas)?;
///     Ok(DurableAgentDeadlineReconciler::new(store, schemas.build()?,
///         DurableGraphLifecycleOptions::default())?)
/// }
/// async fn step(worker: &DurableAgentDeadlineReconciler, tenant: TenantId,
///     cursor: &mut Option<AgentDeadlineSweepCursor>, shutdown: CancellationSignal)
///     -> Result<AgentDeadlineTick, StoreError> {
///     let report = worker.tick(tenant, cursor.clone(), shutdown).await?;
///     *cursor = Some(report.cursor().clone());
///     // Inspect every result and schedule the next finite tick with fairness.
///     Ok(report)
/// }
/// ```
#[derive(Clone, Debug)]
pub struct DurableAgentDeadlineReconciler {
    store: PostgresStore,
    schemas: JsonSchemaRegistry,
    schema: SchemaReference,
    options: DurableGraphLifecycleOptions,
}
impl DurableAgentDeadlineReconciler {
    /// Requires an exact offline schema and the existing finite lifecycle retry policy.
    pub fn new(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        options: DurableGraphLifecycleOptions,
    ) -> Result<Self, AgentDeadlineBuildError> {
        let (schema, _) = standard_agent_deadline_event_schema()?;
        if !schemas.contains(&schema) {
            return Err(AgentDeadlineBuildError::SchemaUnavailable);
        }
        Ok(Self {
            store,
            schemas,
            schema,
            options,
        })
    }
    /// At most 16 candidates, each with 1–10 attempts. A per-item error does not
    /// block later candidates; discovery errors fail the tick without advancing.
    pub fn tick(
        &self,
        tenant: TenantId,
        after: Option<AgentDeadlineSweepCursor>,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<AgentDeadlineTick, StoreError>> {
        Box::pin(async move {
            if after.as_ref().is_some_and(|cursor| cursor.tenant != tenant) {
                return Err(StoreError::InvalidAgentDeadline);
            }
            let mut tick = AgentDeadlineTick {
                cursor: after.unwrap_or(AgentDeadlineSweepCursor {
                    tenant: tenant.clone(),
                    after: None,
                }),
                items: Vec::with_capacity(16),
                cancelled: false,
            };
            let candidates = tokio::select! {
                biased;
                () = shutdown.cancelled() => { tick.cancelled=true; return Ok(tick); }
                candidates = self.store.due_agent_deadlines_after(&tenant,tick.cursor.after.as_ref()) => candidates?,
            };
            let exhausted = candidates.len() < 16;
            for candidate in candidates {
                let result = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => { tick.cancelled=true; break; }
                    result = self.request_with_retry(&candidate) => result,
                };
                tick.cursor.after = Some(candidate.clone());
                tick.items.push(AgentDeadlineItem { candidate, result });
            }
            if exhausted && !tick.cancelled {
                tick.cursor.after = None;
            }
            Ok(tick)
        })
    }

    async fn request_with_retry(
        &self,
        candidate: &AgentDeadlineCursor,
    ) -> Result<AgentDeadlineCancellationOutcome, StoreError> {
        let event = EventId::generate();
        let failure = FailureId::generate();
        for attempt in 1..=self.options.maximum_mutation_attempts() {
            let result = self
                .store
                .request_agent_deadline_cancellation(
                    candidate.tenant_id(),
                    candidate.run_id(),
                    event,
                    failure,
                    &self.schema,
                    &self.schemas,
                )
                .await;
            match result {
                Err(error)
                    if error.is_retryable()
                        && attempt < self.options.maximum_mutation_attempts() =>
                {
                    let delay = self
                        .options
                        .mutation_retry_initial_delay()
                        .saturating_mul(1_u32 << (attempt - 1))
                        .min(Duration::from_secs(1));
                    tokio::time::sleep(delay).await;
                }
                other => return other,
            }
        }
        unreachable!("validated positive retry bound")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JsonSchemaRegistryLimits;
    use stateknot_core::BoundedJson;

    #[test]
    fn deadline_schema_is_offline_closed_and_exactly_pinned() {
        let mut builder = JsonSchemaRegistryBuilder::new(JsonSchemaRegistryLimits::default());
        let reference = register_standard_agent_deadline_event_schema(&mut builder).unwrap();
        assert_eq!(reference, standard_agent_deadline_event_schema().unwrap().0);
        let registry = builder.build().unwrap();
        let mut value = serde_json::json!({"operation":"agent_deadline_cancellation_requested",
            "admission_digest":"a".repeat(64),"deadline":"2026-09-09T12:00:00.000000Z","failure_id":FailureId::generate().to_string()});
        registry
            .validate_bounded(
                &reference,
                &BoundedJson::try_from_value(value.clone()).unwrap(),
            )
            .unwrap();
        value["output"] = serde_json::json!({"must_not_leak":true});
        assert!(
            registry
                .validate_bounded(&reference, &BoundedJson::try_from_value(value).unwrap())
                .is_err()
        );
    }
}
