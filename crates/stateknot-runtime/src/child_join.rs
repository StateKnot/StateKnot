// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded Join publication and read-only, slot-scoped terminal access.

use std::sync::Arc;

use serde_json::{Value, json};
use stateknot_core::{
    BoundedJson, BoxFuture, CancellationSignal, ChildRunJoinBinding, ChildRunJoinHead,
    ChildRunJoinRequest, ChildRunSlot, Digest, EventId, JournalAppend, JournalEventIntent,
    JournalEventKind, JournalExpectation, JournalPayload, RunLifecycle, SchemaReference, TenantId,
    Version,
};
use stateknot_store_postgres::{
    ChildJoinCommitOutcome, ChildJoinRecord, PostgresStore, StoreError,
};

use crate::{
    ChildReconcilerBuildError, DurableChildReconcilerOptions, ExecutableGraphRegistry,
    JsonSchemaRegistry, JsonSchemaRegistryBuilder,
};

/// Offline identity of the embedded, closed Join audit schema.
pub const STANDARD_CHILD_JOIN_EVENT_SCHEMA_ID: &str =
    "https://stknot.com/schemas/runtime/child-join-event/1.0.0";

/// Returns the exact release schema, without fetching its identity URL.
pub fn standard_child_join_event_schema()
-> Result<(SchemaReference, Value), ChildReconcilerBuildError> {
    let document: Value =
        serde_json::from_str(include_str!("../schemas/child-join-event-1.0.0.json"))
            .map_err(|_| ChildReconcilerBuildError::SchemaDefinition)?;
    let canonical = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| ChildReconcilerBuildError::SchemaDefinition)?;
    Ok((
        SchemaReference::new(
            STANDARD_CHILD_JOIN_EVENT_SCHEMA_ID
                .parse()
                .map_err(|_| ChildReconcilerBuildError::SchemaDefinition)?,
            Version::new(1, 0, 0),
            Digest::sha256(canonical),
        ),
        document,
    ))
}

/// Registers the Join schema before freezing a Join-enabled deployment.
pub fn register_standard_child_join_event_schema(
    builder: &mut JsonSchemaRegistryBuilder,
) -> Result<SchemaReference, ChildReconcilerBuildError> {
    let (reference, document) = standard_child_join_event_schema()?;
    builder.register(reference.clone(), document)?;
    Ok(reference)
}

pub(crate) fn schema_available(schemas: &JsonSchemaRegistry) -> bool {
    standard_child_join_event_schema().is_ok_and(|(schema, _)| schemas.contains(&schema))
}

pub(crate) fn payload(
    schemas: &JsonSchemaRegistry,
    request: &ChildRunJoinRequest,
    publication: bool,
) -> Result<JournalPayload, StoreError> {
    let (schema, _) =
        standard_child_join_event_schema().map_err(|_| StoreError::ChildJoinRejected)?;
    let (operation, kind) = if publication {
        ("child_join_published", "child-join-published")
    } else {
        ("child_join_registered", "child-join-registered")
    };
    let digest = request.digest().to_string();
    let digest = digest
        .strip_prefix("sha256:")
        .ok_or(StoreError::ChildJoinRejected)?;
    let data =
        BoundedJson::try_from_value(json!({"operation": operation, "request_digest": digest}))
            .map_err(|_| StoreError::ChildJoinRejected)?;
    schemas
        .validate_bounded(&schema, &data)
        .map_err(|_| StoreError::ChildJoinRejected)?;
    JournalPayload::new(
        schema,
        JournalEventKind::new(kind).map_err(|_| StoreError::ChildJoinRejected)?,
        data,
    )
    .map_err(|_| StoreError::ChildJoinRejected)
}

/// Verified publication and lazy, exact-slot terminal access for resumed node code.
/// The context retains compact terminal proofs, never all child output bodies.
/// Each read revalidates ownership, settlement, frozen graph and output schema.
/// Holding or reading this handle does not consume the Join or authorize writes.
#[derive(Clone, Debug)]
pub struct GraphChildJoin {
    store: PostgresStore,
    registry: ExecutableGraphRegistry,
    binding: ChildRunJoinBinding,
    head: ChildRunJoinHead,
}

impl GraphChildJoin {
    pub(crate) async fn prepare(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
        record: &ChildJoinRecord,
    ) -> Result<Arc<Self>, StoreError> {
        if record.consumed().is_some() {
            return Err(StoreError::ChildJoinRejected);
        }
        let value = Arc::new(Self {
            store,
            registry,
            binding: record
                .binding()
                .cloned()
                .ok_or(StoreError::ChildJoinRejected)?,
            head: record
                .head()
                .cloned()
                .ok_or(StoreError::ChildJoinRejected)?,
        });
        // Qualify every output before dispatch, one at a time; no 64-output buffer.
        for key in value.binding.request().keys() {
            value.load_child(key.slot()).await?;
        }
        Ok(value)
    }

    /// Returns canonical slot order and complete, priced terminal proofs.
    #[must_use]
    pub const fn binding(&self) -> &ChildRunJoinBinding {
        &self.binding
    }

    /// Returns the exact publication automatically attached to node completion.
    #[must_use]
    pub const fn head(&self) -> &ChildRunJoinHead {
        &self.head
    }

    /// Loads only a sealed slot. Success retains its validated Agent output;
    /// failure/cancellation remain typed terminal lifecycle evidence. Bodies
    /// stay subject to existing per-Run bounds; callers control retention.
    pub async fn load_child(&self, slot: &ChildRunSlot) -> Result<RunLifecycle, StoreError> {
        let index = self
            .binding
            .request()
            .keys()
            .iter()
            .position(|key| key.slot() == slot)
            .ok_or(StoreError::ChildJoinRejected)?;
        let record = self
            .store
            .load_child_run(&self.binding.request().keys()[index])
            .await?;
        if record.child().run().is_quarantined() {
            return Err(StoreError::RunQuarantined);
        }
        if record.settlement() != Some(&self.binding.terminals()[index]) {
            return Err(StoreError::ChildJoinRejected);
        }
        let graph = self
            .registry
            .resolve(record.intent().child().graph())
            .ok_or(StoreError::ChildJoinRejected)?;
        if graph.graph() != record.intent().child_graph() {
            return Err(StoreError::ChildJoinRejected);
        }
        let lifecycle = record.child().run().lifecycle();
        if let Some(result) = lifecycle.result() {
            if result.output_schema() != graph.graph().output_schema()
                || result.output_schema() != record.intent().child().descriptor().output_schema()
            {
                return Err(StoreError::ChildJoinRejected);
            }
            self.registry
                .schemas()
                .validate_bounded(result.output_schema(), result.output())
                .map_err(|_| StoreError::ChildJoinRejected)?;
        }
        Ok(lifecycle.clone())
    }
}

/// Tenant-bound in-process continuation. After a restart begin a new sweep at None.
#[derive(Clone, Debug)]
pub struct ChildJoinPublicationCursor {
    tenant: TenantId,
    after: Option<ChildRunJoinRequest>,
}

/// Per-request publication result; errors never silently disappear or block later keys.
#[derive(Debug)]
pub struct ChildJoinPublicationItem {
    request: ChildRunJoinRequest,
    result: Result<ChildJoinCommitOutcome, StoreError>,
}
impl ChildJoinPublicationItem {
    /// Exact request for authorized operator correlation, not metric labels.
    #[must_use]
    pub const fn request(&self) -> &ChildRunJoinRequest {
        &self.request
    }
    /// Exact commit/recovery or public-safe error. Inspect every item.
    pub fn result(&self) -> Result<&ChildJoinCommitOutcome, &StoreError> {
        self.result.as_ref()
    }
}

/// One finite publication pass, separate from cancellation and accounting lanes.
#[derive(Debug)]
pub struct ChildJoinPublicationTick {
    cursor: ChildJoinPublicationCursor,
    items: Vec<ChildJoinPublicationItem>,
    cancelled: bool,
}
impl ChildJoinPublicationTick {
    /// Retain after successes and per-item errors. Each completed sweep resets itself.
    #[must_use]
    pub const fn cursor(&self) -> &ChildJoinPublicationCursor {
        &self.cursor
    }
    /// At most 16 results. No output bodies are aggregated.
    #[must_use]
    pub fn items(&self) -> &[ChildJoinPublicationItem] {
        &self.items
    }
    /// Unfinished or ambiguously committed work stays recoverable in the store.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        self.cancelled
    }
}

/// Trusted, tenant-scoped Join publisher; no node or provider code is dispatched.
/// Schedule alongside `DurableChildReconciler` and execution workers. The host
/// authenticates tenants, retains cursors, applies fairness, and alerts on errors.
/// No parent lease or transaction is retained between calls.
///
/// ```no_run
/// use stateknot_core::{CancellationSignal, TenantId};
/// use stateknot_runtime::{ChildJoinPublicationCursor, ChildJoinPublicationTick,
///     DurableChildJoinPublisher, DurableChildReconcilerOptions, JsonSchemaRegistryBuilder,
///     register_standard_child_join_event_schema};
/// use stateknot_store_postgres::{PostgresStore, StoreError};
///
/// fn configure(store: PostgresStore, mut schemas: JsonSchemaRegistryBuilder)
///     -> Result<DurableChildJoinPublisher, Box<dyn std::error::Error>> {
///     register_standard_child_join_event_schema(&mut schemas)?;
///     Ok(DurableChildJoinPublisher::new(store, schemas.build()?,
///         DurableChildReconcilerOptions::default())?)
/// }
///
/// async fn publish_step(worker: &DurableChildJoinPublisher, authorized_tenant: TenantId,
///     cursor: &mut Option<ChildJoinPublicationCursor>, shutdown: CancellationSignal)
///     -> Result<ChildJoinPublicationTick, StoreError> {
///     let report = worker.tick(authorized_tenant, cursor.clone(), shutdown).await?;
///     *cursor = Some(report.cursor().clone());
///     // Return the entire report to the host: inspect all item errors, apply
///     // fairness and cadence, and restart with None after process loss.
///     Ok(report)
/// }
/// ```
#[derive(Clone, Debug)]
pub struct DurableChildJoinPublisher {
    store: PostgresStore,
    schemas: JsonSchemaRegistry,
    options: DurableChildReconcilerOptions,
}
impl DurableChildJoinPublisher {
    /// Requires the exact embedded schema and a finite validated retry policy.
    pub fn new(
        store: PostgresStore,
        schemas: JsonSchemaRegistry,
        options: DurableChildReconcilerOptions,
    ) -> Result<Self, ChildReconcilerBuildError> {
        if !schema_available(&schemas) {
            return Err(ChildReconcilerBuildError::SchemaUnavailable);
        }
        Ok(Self {
            store,
            schemas,
            options,
        })
    }

    /// Processes at most 16 eligible parents, continuing past each per-item error.
    /// Discovery failure fails the tick; retry from the last retained cursor.
    /// Shutdown can win during an ambiguous commit; subsequent reads converge.
    pub fn tick(
        &self,
        tenant: TenantId,
        after: Option<ChildJoinPublicationCursor>,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<ChildJoinPublicationTick, StoreError>> {
        Box::pin(async move {
            if after.as_ref().is_some_and(|cursor| cursor.tenant != tenant) {
                return Err(StoreError::ChildJoinRejected);
            }
            let mut tick = ChildJoinPublicationTick {
                cursor: after.unwrap_or(ChildJoinPublicationCursor {
                    tenant: tenant.clone(),
                    after: None,
                }),
                items: Vec::with_capacity(16),
                cancelled: false,
            };
            let requests = tokio::select! {
                biased;
                () = shutdown.cancelled() => { tick.cancelled = true; return Ok(tick); }
                requests = self.store.pending_child_joins_after(&tenant, tick.cursor.after.as_ref()) => requests?,
            };
            let exhausted = requests.len() < 16;
            for request in requests {
                let result = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => { tick.cancelled = true; break; }
                    result = self.publish_with_retry(&request) => result,
                };
                tick.cursor.after = Some(request.clone());
                tick.items
                    .push(ChildJoinPublicationItem { request, result });
            }
            if exhausted && !tick.cancelled {
                tick.cursor.after = None;
            }
            Ok(tick)
        })
    }

    async fn publish_with_retry(
        &self,
        request: &ChildRunJoinRequest,
    ) -> Result<ChildJoinCommitOutcome, StoreError> {
        let id = EventId::generate();
        for attempt in 1..=self.options.maximum_mutation_attempts() {
            let result = self.publish(request, id).await;
            match result {
                Err(error)
                    if attempt < self.options.maximum_mutation_attempts()
                        && (error.is_retryable()
                            || matches!(error, StoreError::StaleJournalHead)) =>
                {
                    tokio::time::sleep(self.options.retry_delay(attempt)).await;
                }
                other => return other,
            }
        }
        unreachable!("validated positive attempt count")
    }

    async fn publish(
        &self,
        request: &ChildRunJoinRequest,
        id: EventId,
    ) -> Result<ChildJoinCommitOutcome, StoreError> {
        let activation = request.activation();
        let run = self
            .store
            .load_run(activation.tenant_id(), activation.run_id())
            .await?;
        let append = JournalAppend::new(
            JournalExpectation::exact(
                run.journal_head()
                    .cloned()
                    .ok_or(StoreError::ChildJoinRejected)?,
            ),
            JournalEventIntent::control_plane(
                activation.tenant_id().clone(),
                activation.run_id(),
                id,
                payload(&self.schemas, request, true)?,
            )
            .map_err(|_| StoreError::ChildJoinRejected)?,
        )
        .map_err(|_| StoreError::ChildJoinRejected)?;
        self.store.publish_child_join(request, append).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_schema_is_offline_closed_and_independent_of_reconciliation_v1() {
        let mut builder = JsonSchemaRegistryBuilder::default();
        let schema = register_standard_child_join_event_schema(&mut builder).unwrap();
        assert_eq!(
            schema.digest().to_string(),
            "sha256:d63a62eead850debf4a4ed79aae622bed6a89e20fe60ce5e146bb93f3c86a21e"
        );
        crate::register_standard_child_reconciliation_event_schema(&mut builder).unwrap();
        let registry = builder.build().unwrap();
        assert!(schema_available(&registry));
        for operation in ["child_join_registered", "child_join_published"] {
            let mut data = json!({"operation": operation, "request_digest": "a".repeat(64)});
            registry
                .validate_bounded(&schema, &BoundedJson::try_from_value(data.clone()).unwrap())
                .unwrap();
            data["child_output"] = json!({"private": true});
            assert!(
                registry
                    .validate_bounded(&schema, &BoundedJson::try_from_value(data).unwrap())
                    .is_err()
            );
        }
        for data in [
            json!({"operation":"child_run_settled", "request_digest":"a".repeat(64)}),
            json!({"operation":"child_join_published", "request_digest":"not-a-digest"}),
        ] {
            assert!(
                registry
                    .validate_bounded(&schema, &BoundedJson::try_from_value(data).unwrap())
                    .is_err()
            );
        }
    }
}
