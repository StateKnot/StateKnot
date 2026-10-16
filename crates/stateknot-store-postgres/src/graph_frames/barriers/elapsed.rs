// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! V3 compact originals and the complete distinct event-clock inventory.
//! Local row decoding never replaces the caller's full owning result proof.

#[allow(clippy::wildcard_imports)]
use super::*;
use crate::store::graph_waits::{
    self, GraphWaitDatabaseError, GraphWaitInventory, GraphWaitPreparationError,
    OriginalGraphWaits, PreparedGraphWaitState,
};
use stateknot_core::{NodeControl, NodeWait};

const INTENT_DOMAIN: &[u8] = b"stateknot-postgres-frame-barrier-scope-v3\0";
const COMPOUND_DOMAIN: &[u8] = b"stateknot-postgres-frame-barrier-compound-v3\0";
const ORIGINAL_DOMAIN: &[u8] = b"stateknot-postgres-graph-wait-original-v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CompactWait {
    kind: WaitKind,
    condition_count: u8,
    original_digest: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WaitKind {
    Wait,
}

impl CompactWait {
    fn new(waits: &NodeWaits) -> Result<Self, StoreError> {
        Ok(Self {
            kind: WaitKind::Wait,
            condition_count: u8::try_from(waits.len())
                .map_err(|_| StoreError::GraphFrameLimitExceeded)?,
            original_digest: domain_digest(ORIGINAL_DOMAIN, waits)?,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ElapsedWire {
    version: u8,
    barrier: Box<GraphFrameBarrier>,
    checkpoint: Box<GraphFrameCheckpointHead>,
    disposition: CompactWait,
    scope: Box<EntryScope>,
    budget: Box<EntryBudget>,
    scope_intent_digest: Digest,
    compound_digest: Digest,
    wait_revision: RunRevision,
    wait_inventory: Box<GraphWaitInventory>,
}

#[derive(Serialize)]
struct ScopePreimage<'a> {
    version: u8,
    barrier_intent_digest: Digest,
    disposition: CompactWait,
    scope: &'a EntryScope,
    budget: &'a EntryBudget,
    wait_revision: RunRevision,
}

#[derive(Serialize)]
struct CompoundPreimage<'a> {
    version: u8,
    scope_intent_digest: Digest,
    event: &'a JournalEvent,
    checkpoint: &'a GraphFrameCheckpointHead,
    wait_inventory: &'a GraphWaitInventory,
}

pub(super) fn scope_intent(
    barrier: &GraphFrameBarrier,
    disposition: &Disposition,
    scope: &EntryScope,
    budget: &EntryBudget,
    revision: RunRevision,
) -> Result<Digest, StoreError> {
    let Disposition::Wait { waits } = disposition else {
        return Err(StoreError::GraphFrameConflict);
    };
    domain_digest(
        INTENT_DOMAIN,
        &ScopePreimage {
            version: 3,
            barrier_intent_digest: barrier.intent_digest(),
            disposition: CompactWait::new(waits)?,
            scope,
            budget,
            wait_revision: revision,
        },
    )
}

pub(super) fn compound(
    scope: Digest,
    event: &JournalEvent,
    checkpoint: &GraphFrameCheckpointHead,
    inventory: &GraphWaitInventory,
) -> Result<Digest, StoreError> {
    domain_digest(
        COMPOUND_DOMAIN,
        &CompoundPreimage {
            version: 3,
            scope_intent_digest: scope,
            event,
            checkpoint,
            wait_inventory: inventory,
        },
    )
}

pub(super) fn encode(record: &StoredGraphFrameBarrier) -> Result<Vec<u8>, StoreError> {
    let Disposition::Wait { waits } = &record.disposition else {
        return Err(StoreError::GraphFrameConflict);
    };
    let bytes = serde_json_canonicalizer::to_vec(&ElapsedWire {
        version: 3,
        barrier: Box::new(record.barrier.clone()),
        checkpoint: Box::new(record.checkpoint.head()),
        disposition: CompactWait::new(waits)?,
        scope: Box::new(record.scope.clone()),
        budget: Box::new(record.budget.clone()),
        scope_intent_digest: record.scope_intent_digest,
        compound_digest: record.digest,
        wait_revision: record.wait_revision.ok_or(StoreError::GraphFrameConflict)?,
        wait_inventory: Box::new(
            record
                .wait_inventory
                .clone()
                .ok_or(StoreError::GraphFrameConflict)?,
        ),
    })
    .map_err(|_| StoreError::encoding("elapsed scoped Wait wire"))?;
    if bytes.is_empty() || bytes.len() > MAX_BARRIER_BYTES {
        return Err(StoreError::GraphFrameLimitExceeded);
    }
    Ok(bytes)
}

async fn original_results(
    tx: &mut Transaction<'_, Postgres>,
    base: &GraphFrameCheckpoint,
    heads: &[PendingNodeResultHead],
    expected_conditions: usize,
) -> Result<(Vec<PendingNodeResult>, NodeWaits), StoreError> {
    let durable = load_locked_barrier_result_heads(tx, &base.checkpoint().head()).await?;
    validate_complete_barrier_result_heads(&durable, heads)
        .map_err(|_| StoreError::corrupt("elapsed scoped Wait complete result set"))?;
    let mut results = Vec::with_capacity(heads.len());
    let mut size = 0usize;
    let mut specifications = Vec::with_capacity(expected_conditions);
    for head in heads {
        let row = load_pending_node_result_row(tx, head.activation())
            .await?
            .ok_or_else(|| StoreError::corrupt("elapsed scoped Wait original result missing"))?;
        size = size
            .checked_add(row.result_bytes.len())
            .ok_or(StoreError::GraphReplayResourceLimit)?;
        if size > GraphReplayLimits::default().maximum_barrier_result_bytes() {
            return Err(StoreError::GraphReplayResourceLimit);
        }
        let result = decode_pending_node_result(&row)?;
        if result.head() != *head {
            return Err(StoreError::corrupt(
                "elapsed scoped Wait original result projection",
            ));
        }
        if let NodeControl::Wait { waits } = result.intent().control() {
            if specifications
                .len()
                .checked_add(waits.len())
                .is_none_or(|n| n > NodeWaits::MAX_LEN)
            {
                return Err(StoreError::corrupt(
                    "elapsed scoped Wait original condition bound",
                ));
            }
            specifications.extend(waits.iter().cloned());
        }
        results.push(result);
    }
    let originals = NodeWaits::try_new(specifications)
        .map_err(|_| StoreError::corrupt("elapsed scoped Wait original condition identities"))?;
    Ok((results, originals))
}

// The enclosing whole edge is already present. Bound every local full row and
// complete result set before reconstructing the original Wait. Subsequent
// owning replay still authenticates every result's Model/Tool/attempt/Return.
pub(super) fn decode<'a>(
    tx: &'a mut Transaction<'_, Postgres>,
    bytes: &'a [u8],
) -> stateknot_core::BoxFuture<'a, Result<Box<Wire>, StoreError>> {
    Box::pin(async move {
        let wire: ElapsedWire = serde_json::from_slice(bytes)
            .map_err(|_| StoreError::corrupt("elapsed scoped Wait wire"))?;
        if wire.version != 3
            || wire.disposition.condition_count == 0
            || usize::from(wire.disposition.condition_count) > NodeWaits::MAX_LEN
            || wire.wait_inventory.condition_count()
                != usize::from(wire.disposition.condition_count)
            || serde_json_canonicalizer::to_vec(&wire)
                .map_err(|_| StoreError::corrupt("elapsed scoped Wait canonical wire"))?
                != bytes
        {
            return Err(StoreError::corrupt(
                "elapsed scoped Wait version or canonical bytes",
            ));
        }
        let base = wire.barrier.base_checkpoint();
        let base_checkpoint = decode_frame_checkpoint(
            checkpoint_row(
                tx,
                base.checkpoint().tenant_id(),
                base.checkpoint().run_id(),
                base.frame().namespace(),
                base.checkpoint().checkpoint_id(),
            )
            .await?,
        )?;
        if base_checkpoint.head() != *base {
            return Err(StoreError::corrupt("elapsed scoped Wait original base"));
        }
        let successor = decode_frame_checkpoint(
            checkpoint_row(
                tx,
                base.checkpoint().tenant_id(),
                base.checkpoint().run_id(),
                base.frame().namespace(),
                wire.checkpoint.checkpoint().checkpoint_id(),
            )
            .await?,
        )?;
        if successor.head() != *wire.checkpoint {
            return Err(StoreError::corrupt(
                "elapsed scoped Wait original successor",
            ));
        }
        let heads = wire.barrier.result_heads();
        let (results, originals) = original_results(
            tx,
            &base_checkpoint,
            heads,
            usize::from(wire.disposition.condition_count),
        )
        .await?;
        if CompactWait::new(&originals)? != wire.disposition {
            return Err(StoreError::corrupt(
                "elapsed scoped Wait complete original disposition",
            ));
        }
        let (event, _) = child_runs::anchored_event(
            tx,
            base.checkpoint().tenant_id(),
            base.checkpoint().run_id(),
            i64::try_from(wire.checkpoint.checkpoint().journal_head().sequence().get())
                .map_err(|_| StoreError::JournalSequenceExhausted)?,
        )
        .await?;
        let prepared = OriginalGraphWaits::from_complete_results(
            base_checkpoint.checkpoint(),
            base.frame().namespace(),
            heads,
            &results,
            &originals,
        )
        .and_then(|original| original.prepare(&wire.budget.observed_head, &event))
        .map_err(read_error)?;
        let inventory = prepared
            .inventory(successor.checkpoint())
            .map_err(read_error)?;
        inventory
            .validate_summary(&wire.wait_inventory)
            .map_err(read_error)?;
        graph_waits::verify(tx, &prepared, &inventory)
            .await
            .map_err(database_read_error)?;
        Ok(Box::new(Wire {
            barrier: *wire.barrier,
            checkpoint: *wire.checkpoint,
            disposition: Disposition::Wait { waits: originals },
            scope: *wire.scope,
            budget: *wire.budget,
            scope_intent_digest: wire.scope_intent_digest,
            compound_digest: wire.compound_digest,
            wait_revision: Some(wire.wait_revision),
            wait_inventory: Some(*wire.wait_inventory),
        }))
    })
}

pub(super) fn registrations(
    waits: &NodeWaits,
    event: &JournalEvent,
) -> Result<Vec<DurableWait>, StoreError> {
    let mut registered = Vec::with_capacity(waits.len());
    for specification in waits.iter() {
        if matches!(specification, NodeWait::Timer { due_at, .. } if *due_at <= event.recorded_at())
        {
            continue;
        }
        let intent = specification
            .registration_intent(event.tenant_id().clone(), event.run_id(), event.event_id())
            .map_err(|_| StoreError::corrupt("elapsed scoped Wait original registration intent"))?;
        registered.push(
            intent
                .commit(event.head())
                .map_err(|_| StoreError::corrupt("elapsed scoped Wait registration clock"))?,
        );
    }
    Ok(registered)
}

pub(super) fn read_error(_: GraphWaitPreparationError) -> StoreError {
    StoreError::corrupt("elapsed scoped Wait original-condition inventory")
}
pub(super) fn write_error(_: GraphWaitPreparationError) -> StoreError {
    StoreError::InvalidWaitRegistrationBatch
}
pub(super) fn database_read_error(error: GraphWaitDatabaseError) -> StoreError {
    match error {
        GraphWaitDatabaseError::Database { source } => {
            StoreError::database("elapsed scoped Wait conditions", source)
        }
        _ => StoreError::corrupt("elapsed scoped Wait condition physical projection"),
    }
}
pub(super) fn database_write_error(error: GraphWaitDatabaseError) -> StoreError {
    match error {
        GraphWaitDatabaseError::Database { source } => {
            StoreError::database("elapsed scoped Wait conditions", source)
        }
        GraphWaitDatabaseError::IdentityConflict => StoreError::InvalidWaitRegistrationBatch,
        _ => StoreError::GraphFrameConflict,
    }
}

pub(super) fn effect(inventory: &GraphWaitInventory) -> GraphWaitEffect {
    match inventory.state() {
        PreparedGraphWaitState::Waiting => GraphWaitEffect::Waiting,
        PreparedGraphWaitState::Ready => GraphWaitEffect::Ready,
    }
}
