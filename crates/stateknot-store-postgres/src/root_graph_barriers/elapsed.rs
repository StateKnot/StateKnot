// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! V3 original-condition Waits. This module is called only after the owning
//! Root transaction has authenticated its complete base, results and source.

use super::super::graph_waits::{
    GraphWaitDatabaseError, GraphWaitInventory, OriginalGraphWaits, PreparedGraphWaitInventory,
    PreparedGraphWaitState, PreparedGraphWaits,
};
#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) const INTENT_DOMAIN: &[u8] = b"stateknot-postgres-root-graph-barrier-intent-v3\0";
pub(super) const COMPOUND_DOMAIN: &[u8] = b"stateknot-postgres-root-graph-barrier-compound-v3\0";

/// Positive reference to the complete immutable Agent admission. Every read
/// authenticates that full admission again; a null deadline is never a source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AgentSource {
    admission_digest: Digest,
    first_journal: JournalHead,
    initial_checkpoint: CheckpointHead,
}

impl AgentSource {
    pub(super) fn new(admission: &StoredAgentAdmission) -> Self {
        Self {
            admission_digest: admission.admission().digest(),
            first_journal: admission.event().head(),
            initial_checkpoint: admission.checkpoint().head(),
        }
    }

    pub(super) fn verify(&self, admission: &StoredAgentAdmission) -> Result<(), StoreError> {
        if *self != Self::new(admission) {
            return Err(StoreError::corrupt("elapsed Root original Agent admission"));
        }
        Ok(())
    }
}

pub(super) fn prepare<'a>(
    base: &Checkpoint,
    barrier: &CheckpointBarrier,
    results: &'a [PendingNodeResult],
    disposition: &'a GraphBarrierDisposition,
    source: &JournalHead,
    event: &JournalEvent,
    successor: &Checkpoint,
) -> Result<(PreparedGraphWaits<'a>, PreparedGraphWaitInventory), StoreError> {
    let GraphBarrierDisposition::Wait { waits } = disposition else {
        return Err(StoreError::InvalidWaitRegistrationBatch);
    };
    let prepared = OriginalGraphWaits::from_complete_results(
        base,
        &GraphNamespace::root(),
        barrier.result_heads().as_slice(),
        results,
        waits,
    )
    .and_then(|original| original.prepare(source, event))
    .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?;
    let inventory = prepared
        .inventory(successor)
        .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?;
    Ok((prepared, inventory))
}

pub(super) fn materialize(
    before: &RunLifecycle,
    barrier: &CheckpointBarrier,
    disposition: &GraphBarrierDisposition,
    event: &JournalEvent,
    inventory: &GraphWaitInventory,
) -> Result<(RunLifecycle, Vec<DurableWait>, Digest), StoreError> {
    let GraphBarrierDisposition::Wait { waits } = disposition else {
        return Err(StoreError::InvalidWaitRegistrationBatch);
    };
    if before.status() != RunStatus::Active
        || before.provenance().tenant_id() != event.tenant_id()
        || before.provenance().run_id() != event.run_id()
    {
        return Err(StoreError::InvalidLifecycleTransition);
    }
    // The original projection commits every original intent, including timers
    // which became due. Actual registrations contain only future conditions.
    let intents = waits
        .registration_intents(event.tenant_id(), event.run_id(), event.event_id())
        .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?;
    let digest =
        wait_barrier_projection_digest(before.revision(), barrier.intent_digest(), &intents)?;
    let registrations = intents
        .into_iter()
        .filter(|intent| !matches!(intent, WaitRegistrationIntent::Timer { timer } if timer.due_at() <= event.recorded_at()))
        .map(|intent| intent.commit(event.head()).map_err(|_| StoreError::InvalidWaitRegistrationBatch))
        .collect::<Result<Vec<_>, _>>()?;
    if registrations.len() != usize::from(inventory.registered_count()) {
        return Err(StoreError::InvalidWaitRegistrationBatch);
    }
    let after = match inventory.state() {
        PreparedGraphWaitState::Ready if registrations.is_empty() => before.clone(),
        PreparedGraphWaitState::Waiting if !registrations.is_empty() => before
            .clone()
            .apply(RunTransition::Wait {
                waits: RunWaits::try_new(registrations.iter().map(DurableWait::marker))
                    .map_err(|_| StoreError::InvalidWaitRegistrationBatch)?,
            })
            .map_err(|_| StoreError::InvalidLifecycleTransition)?,
        _ => return Err(StoreError::InvalidWaitRegistrationBatch),
    };
    if after.changed_at() > event.recorded_at() {
        return Err(StoreError::LifecycleObservationAfterCommit);
    }
    Ok((after, registrations, digest))
}

pub(super) fn database_error(error: GraphWaitDatabaseError, reading: bool) -> StoreError {
    match error {
        GraphWaitDatabaseError::Database { source } => {
            StoreError::database("elapsed Root Wait conditions", source)
        }
        _ if reading => StoreError::corrupt("elapsed Root Wait physical inventory"),
        _ => StoreError::WaitRegistrationCommitConflict,
    }
}

pub(super) fn effect(
    inventory: &GraphWaitInventory,
) -> super::super::graph_frames::GraphWaitEffect {
    match inventory.state() {
        PreparedGraphWaitState::Waiting => super::super::graph_frames::GraphWaitEffect::Waiting,
        PreparedGraphWaitState::Ready => super::super::graph_frames::GraphWaitEffect::Ready,
    }
}

pub(super) fn schema() -> Result<stateknot_core::SchemaReference, StoreError> {
    let mut document = serde_json::json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "$id":"https://stknot.com/schemas/store/root-graph-barrier-event/3.0.0",
        "type":"object", "additionalProperties":false,
        "properties":{}, "required":[]
    });
    let fields = [
        ("version", serde_json::json!({"const":3})),
        (
            "source",
            serde_json::json!({"enum":["agent_admission","trusted_graph"]}),
        ),
        ("graph_namespace", serde_json::json!({"const":""})),
        (
            "base_checkpoint_id",
            serde_json::json!({"type":"string","format":"uuid"}),
        ),
        (
            "successor_checkpoint_id",
            serde_json::json!({"type":"string","format":"uuid"}),
        ),
        (
            "base_checkpoint_digest",
            serde_json::json!({"type":"string","pattern":"^sha256:[0-9a-f]{64}$"}),
        ),
        (
            "successor_intent_digest",
            serde_json::json!({"type":"string","pattern":"^sha256:[0-9a-f]{64}$"}),
        ),
        (
            "barrier_intent_digest",
            serde_json::json!({"type":"string","pattern":"^sha256:[0-9a-f]{64}$"}),
        ),
        (
            "intent_digest",
            serde_json::json!({"type":"string","pattern":"^sha256:[0-9a-f]{64}$"}),
        ),
    ];
    for (name, schema) in fields {
        document["properties"][name] = schema;
        document["required"]
            .as_array_mut()
            .ok_or_else(|| StoreError::encoding("elapsed Root schema"))?
            .push(serde_json::json!(name));
    }
    let bytes = serde_json_canonicalizer::to_vec(&document)
        .map_err(|_| StoreError::encoding("elapsed Root schema"))?;
    Ok(stateknot_core::SchemaReference::new(
        "https://stknot.com/schemas/store/root-graph-barrier-event/3.0.0"
            .parse()
            .map_err(|_| StoreError::encoding("elapsed Root schema identity"))?,
        stateknot_core::Version::new(3, 0, 0),
        Digest::sha256(bytes),
    ))
}
