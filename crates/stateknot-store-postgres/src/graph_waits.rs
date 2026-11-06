// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Complete original graph Wait inputs and one exact event-clock classification.
//!
//! This module does not authenticate database rows or grant scheduling authority.
//! Writers call it after the existing full owning read/locked result comparison;
//! readers call it after authenticating the whole original event and results.
//! Its preparation is not a durable receipt. In particular an elapsed condition
//! cannot be exposed as a registered Timer or used before the whole transaction
//! containing its distinct evidence commits.

use serde::{Deserialize, Serialize};
use stateknot_core::{
    Checkpoint, CheckpointHead, Digest, DurableWait, GraphNamespace, JournalEvent, JournalHead,
    NodeControl, NodeWait, NodeWaits, PendingNodeResult, PendingNodeResultHead, ReadyNodes,
    TimerRegistrationIntent, WaitRegistrationIntent,
};
use thiserror::Error;

#[path = "graph_waits/database.rs"]
mod database;
pub(super) use database::{GraphWaitDatabaseError, insert, verify};

/// The complete original graph Wait and its exact immutable result owners.
/// Borrowing full specifications avoids copying large Interrupt payloads.
pub(super) struct OriginalGraphWaits<'a> {
    base: CheckpointHead,
    namespace: GraphNamespace,
    conditions: Vec<OriginalCondition<'a>>,
}

struct OriginalCondition<'a> {
    specification: &'a NodeWait,
    owner: PendingNodeResultHead,
    result_wait_index: u8,
}

impl<'a> OriginalGraphWaits<'a> {
    /// Checks structural agreement after the caller authenticates all full rows.
    /// Every result, including non-Wait controls, remains in the exact head set.
    pub(super) fn from_complete_results(
        base: &Checkpoint,
        namespace: &GraphNamespace,
        heads: &[PendingNodeResultHead],
        results: &'a [PendingNodeResult],
        original: &NodeWaits,
    ) -> Result<Self, GraphWaitPreparationError> {
        if heads.is_empty()
            || heads.len() > ReadyNodes::MAX_LEN
            || heads.len() != results.len()
            || heads.len() != base.ready_nodes().len()
            || heads
                .iter()
                .zip(base.ready_nodes().iter())
                .any(|(head, node)| head.activation().node_id() != node)
        {
            return Err(GraphWaitPreparationError::ResultOwnership);
        }
        let base_head = base.head();
        let mut conditions = Vec::with_capacity(original.len());
        for (head, result) in heads.iter().zip(results) {
            let activation = result.intent().activation();
            if result.head() != *head
                || activation.base_checkpoint() != &base_head
                || activation.graph_namespace() != namespace
                || result.fence().tenant_id() != base.tenant_id()
                || result.fence().run_id() != base.run_id()
            {
                return Err(GraphWaitPreparationError::ResultOwnership);
            }
            match result.intent().control() {
                NodeControl::Wait { waits } => {
                    for (index, specification) in waits.iter().enumerate() {
                        if conditions.len() == NodeWaits::MAX_LEN {
                            return Err(GraphWaitPreparationError::CompleteConditions);
                        }
                        conditions.push(OriginalCondition {
                            specification,
                            owner: head.clone(),
                            result_wait_index: u8::try_from(index)
                                .map_err(|_| GraphWaitPreparationError::CompleteConditions)?,
                        });
                    }
                }
                NodeControl::Continue | NodeControl::Route { .. } => (),
                NodeControl::Terminal { .. } => {
                    return Err(GraphWaitPreparationError::CompleteConditions);
                }
            }
        }
        if conditions.len() != original.len()
            || conditions
                .iter()
                .zip(original.iter())
                .any(|(condition, expected)| condition.specification != expected)
        {
            return Err(GraphWaitPreparationError::CompleteConditions);
        }
        // The already-validated NodeWaits preserves semantic order and rejects
        // identity reuse across Timer and Interrupt kinds; exact comparison
        // above transfers that complete bound to the original result inventory.
        Ok(Self {
            base: base_head,
            namespace: namespace.clone(),
            conditions,
        })
    }

    /// Classifies against the actual candidate or authenticated original event.
    /// The source head and full owners are bound before any registration occurs.
    pub(super) fn prepare(
        self,
        source: &JournalHead,
        event: &JournalEvent,
    ) -> Result<PreparedGraphWaits<'a>, GraphWaitPreparationError> {
        let fence = event
            .source()
            .worker_fence()
            .ok_or(GraphWaitPreparationError::JournalBinding)?;
        if source.tenant_id() != event.tenant_id()
            || source.run_id() != event.run_id()
            || fence.tenant_id() != event.tenant_id()
            || fence.run_id() != event.run_id()
            || source.sequence().get().checked_add(1) != Some(event.sequence().get())
            || event.previous_digest() != Some(source.digest())
            || source.recorded_at() > event.recorded_at()
            || self.conditions.iter().any(|condition| {
                let activation = condition.owner.activation();
                activation.tenant_id() != event.tenant_id()
                    || activation.run_id() != event.run_id()
                    || condition.owner.journal_head().sequence() > source.sequence()
                    || condition.owner.journal_head().recorded_at() > source.recorded_at()
            })
        {
            return Err(GraphWaitPreparationError::JournalBinding);
        }
        let mut conditions = Vec::with_capacity(self.conditions.len());
        for condition in self.conditions {
            let intent = condition
                .specification
                .registration_intent(event.tenant_id().clone(), event.run_id(), event.event_id())
                .map_err(|_| GraphWaitPreparationError::Encoding)?;
            let observation = match intent {
                WaitRegistrationIntent::Timer { timer }
                    if timer.due_at() <= event.recorded_at() =>
                {
                    // Keep the unmodified original intent and actual event head.
                    // No future-only marker and no synthetic firing is created.
                    ConditionObservation::ElapsedTimer {
                        timer,
                        event: event.head(),
                    }
                }
                WaitRegistrationIntent::Interrupt { ref request }
                    if request
                        .expires_at()
                        .is_some_and(|expiry| expiry <= event.recorded_at()) =>
                {
                    return Err(GraphWaitPreparationError::ExpiredInterrupt);
                }
                intent => ConditionObservation::Registered(Box::new(
                    intent
                        .commit(event.head())
                        .map_err(|_| GraphWaitPreparationError::Encoding)?,
                )),
            };
            conditions.push(PreparedGraphWaitCondition {
                specification: condition.specification,
                owner: condition.owner,
                result_wait_index: condition.result_wait_index,
                observation,
            });
        }
        Ok(PreparedGraphWaits {
            base: self.base,
            namespace: self.namespace,
            source: source.clone(),
            event: event.head(),
            conditions,
        })
    }
}

/// Prepared complete inventory; this is not a committed or authenticated receipt.
pub(super) struct PreparedGraphWaits<'a> {
    base: CheckpointHead,
    namespace: GraphNamespace,
    source: JournalHead,
    event: JournalHead,
    conditions: Vec<PreparedGraphWaitCondition<'a>>,
}

impl PreparedGraphWaits<'_> {
    /// Ready means no original condition blocks at this event, never dispatch.
    pub(super) fn state(&self) -> PreparedGraphWaitState {
        if self
            .conditions
            .iter()
            .any(|condition| matches!(condition.observation, ConditionObservation::Registered(_)))
        {
            PreparedGraphWaitState::Waiting
        } else {
            PreparedGraphWaitState::Ready
        }
    }
}

pub(super) struct PreparedGraphWaitCondition<'a> {
    specification: &'a NodeWait,
    owner: PendingNodeResultHead,
    result_wait_index: u8,
    observation: ConditionObservation,
}

impl PreparedGraphWaitCondition<'_> {
    fn reference(
        &self,
        expected: &JournalHead,
    ) -> Result<(Digest, ObservationReference), GraphWaitPreparationError> {
        match &self.observation {
            ConditionObservation::Registered(wait) => {
                if wait.journal() != expected {
                    return Err(GraphWaitPreparationError::JournalBinding);
                }
                Ok(match wait.as_ref() {
                    DurableWait::Interrupt { request } => (
                        request.intent().intent_digest(),
                        ObservationReference::RegisteredInterrupt {
                            wait_id: request.marker().interrupt_id(),
                            record_digest: request.digest(),
                        },
                    ),
                    DurableWait::Timer { timer } => (
                        timer.intent().intent_digest(),
                        ObservationReference::RegisteredTimer {
                            wait_id: timer.marker().timer_id(),
                            record_digest: timer.digest(),
                        },
                    ),
                })
            }
            ConditionObservation::ElapsedTimer { timer, event } => {
                if event != expected
                    || timer.tenant_id() != event.tenant_id()
                    || timer.run_id() != event.run_id()
                    || timer.registration_event_id() != event.event_id()
                    || timer.due_at() > event.recorded_at()
                {
                    return Err(GraphWaitPreparationError::JournalBinding);
                }
                Ok((
                    timer.intent_digest(),
                    ObservationReference::ElapsedTimer {
                        timer: timer.clone(),
                    },
                ))
            }
        }
    }
}

pub(super) enum ConditionObservation {
    Registered(Box<DurableWait>),
    /// Not a `DurableTimer` or `TimerFiring`. Persistence must bind the complete
    /// original owner/spec/source and this exact real whole-barrier head.
    ElapsedTimer {
        timer: TimerRegistrationIntent,
        event: JournalHead,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum PreparedGraphWaitState {
    Waiting,
    Ready,
}

#[derive(Debug, Error)]
pub(super) enum GraphWaitPreparationError {
    #[error("graph Wait inputs do not contain the exact complete original results")]
    ResultOwnership,
    #[error("graph Wait conditions differ from complete original node controls")]
    CompleteConditions,
    #[error("graph Wait event does not extend its exact original source")]
    JournalBinding,
    #[error("graph Wait contains an expired Interrupt requiring explicit policy handling")]
    ExpiredInterrupt,
    #[error("graph Wait intent or registration could not be encoded")]
    Encoding,
    #[error("graph Wait successor differs from the original base, scope or event")]
    Successor,
    #[error("graph Wait condition inventory exceeds its existing component bounds")]
    InventoryBound,
    #[error("graph Wait condition inventory differs from its complete original inputs")]
    InventoryMismatch,
}

// A condition keeps compact owner metadata, never the Interrupt payload. Its
// owner contains one bounded NodeActivation/CheckpointHead and JournalHead;
// the successor adds one CheckpointHead and the source adds one JournalHead.
// Existing Core string, UUID, counter and digest bounds fit 32 KiB per record.
// At most 64 references plus the two checkpoint heads fit 64 KiB. These are
// independent bounds, not an increase to either existing whole-barrier bound.
const MAX_CONDITION_BYTES: usize = 32 * 1024;
const MAX_INVENTORY_BYTES: usize = 64 * 1024;
const SPEC_DOMAIN: &[u8] = b"stateknot-postgres-original-graph-wait-spec-v1\0";
const INVENTORY_DOMAIN: &[u8] = b"stateknot-postgres-original-graph-wait-inventory-v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ObservationReference {
    RegisteredInterrupt {
        wait_id: stateknot_core::InterruptId,
        record_digest: Digest,
    },
    RegisteredTimer {
        wait_id: stateknot_core::TimerId,
        record_digest: Digest,
    },
    ElapsedTimer {
        timer: TimerRegistrationIntent,
    },
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionWire {
    version: u8,
    ordinal: u8,
    owner: PendingNodeResultHead,
    result_wait_index: u8,
    specification_digest: Digest,
    registration_intent_digest: Digest,
    source: JournalHead,
    successor: CheckpointHead,
    observation: ObservationReference,
}

/// Canonical immutable metadata for one original condition. Database owning
/// authentication is still required; deserialization alone grants no authority.
pub(super) struct PreparedConditionRecord {
    wire: ConditionWire,
    bytes: Vec<u8>,
    checksum: Digest,
}

impl PreparedConditionRecord {
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(super) const fn checksum(&self) -> Digest {
        self.checksum
    }
    /// Compares a physical row to inputs rederived from full authenticated
    /// results and the actual original event, including strict canonical bytes.
    pub(super) fn validate_bytes(&self, bytes: &[u8]) -> Result<(), GraphWaitPreparationError> {
        let restored: ConditionWire = decode_bounded(bytes, MAX_CONDITION_BYTES)?;
        if restored != self.wire || bytes != self.bytes {
            return Err(GraphWaitPreparationError::InventoryMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConditionReference {
    ordinal: u8,
    checksum: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GraphWaitInventory {
    version: u8,
    base: CheckpointHead,
    namespace: GraphNamespace,
    source: JournalHead,
    event: JournalHead,
    successor: CheckpointHead,
    state: PreparedGraphWaitState,
    registered_count: u8,
    #[serde(deserialize_with = "bounded_condition_references")]
    conditions: Vec<ConditionReference>,
}

impl GraphWaitInventory {
    pub(super) const fn state(&self) -> PreparedGraphWaitState {
        self.state
    }
    pub(super) const fn registered_count(&self) -> u8 {
        self.registered_count
    }
    pub(super) fn condition_count(&self) -> usize {
        self.conditions.len()
    }
    pub(super) fn digest(&self) -> Result<Digest, GraphWaitPreparationError> {
        domain_digest(INVENTORY_DOMAIN, self)
    }
}

/// Exact compact closure over all original conditions and their actual effect.
/// It is preparation for a whole writer/reader, not an authenticated receipt.
pub(super) struct PreparedGraphWaitInventory {
    wire: GraphWaitInventory,
    records: Vec<PreparedConditionRecord>,
}

impl PreparedGraphWaitInventory {
    pub(super) const fn summary(&self) -> &GraphWaitInventory {
        &self.wire
    }
    pub(super) fn validate_summary(
        &self,
        stored: &GraphWaitInventory,
    ) -> Result<(), GraphWaitPreparationError> {
        if stored != &self.wire {
            return Err(GraphWaitPreparationError::InventoryMismatch);
        }
        Ok(())
    }
}

impl PreparedGraphWaits<'_> {
    /// Binds the complete original inventory to its actual committed or
    /// candidate successor. The caller separately authenticates its whole edge.
    pub(super) fn inventory(
        &self,
        successor: &Checkpoint,
    ) -> Result<PreparedGraphWaitInventory, GraphWaitPreparationError> {
        if successor.parent() != Some(&self.base)
            || successor.tenant_id() != self.base.tenant_id()
            || successor.run_id() != self.base.run_id()
            || successor.graph() != self.base.graph()
            || successor.journal_head() != &self.event
            || self.base.superstep().get().checked_add(1) != Some(successor.superstep().get())
        {
            return Err(GraphWaitPreparationError::Successor);
        }
        let successor = successor.head();
        let mut records = Vec::with_capacity(self.conditions.len());
        let mut references = Vec::with_capacity(self.conditions.len());
        let mut registered_count = 0u8;
        for (ordinal, condition) in self.conditions.iter().enumerate() {
            let (registration_intent_digest, observation) = condition.reference(&self.event)?;
            if matches!(condition.observation, ConditionObservation::Registered(_)) {
                registered_count = registered_count
                    .checked_add(1)
                    .ok_or(GraphWaitPreparationError::InventoryBound)?;
            }
            let wire = ConditionWire {
                version: 1,
                ordinal: u8::try_from(ordinal)
                    .map_err(|_| GraphWaitPreparationError::InventoryBound)?,
                owner: condition.owner.clone(),
                result_wait_index: condition.result_wait_index,
                specification_digest: domain_digest(SPEC_DOMAIN, condition.specification)?,
                registration_intent_digest,
                source: self.source.clone(),
                successor: successor.clone(),
                observation,
            };
            let bytes = encode_bounded(&wire, MAX_CONDITION_BYTES)?;
            let checksum = Digest::sha256(&bytes);
            references.push(ConditionReference {
                ordinal: wire.ordinal,
                checksum,
            });
            records.push(PreparedConditionRecord {
                wire,
                bytes,
                checksum,
            });
        }
        let wire = GraphWaitInventory {
            version: 1,
            base: self.base.clone(),
            namespace: self.namespace.clone(),
            source: self.source.clone(),
            event: self.event.clone(),
            successor,
            state: self.state(),
            registered_count,
            conditions: references,
        };
        encode_bounded(&wire, MAX_INVENTORY_BYTES)?;
        domain_digest(INVENTORY_DOMAIN, &wire)?;
        Ok(PreparedGraphWaitInventory { wire, records })
    }
}

fn encode_bounded<T: Serialize>(
    value: &T,
    maximum: usize,
) -> Result<Vec<u8>, GraphWaitPreparationError> {
    let bytes =
        serde_json_canonicalizer::to_vec(value).map_err(|_| GraphWaitPreparationError::Encoding)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(GraphWaitPreparationError::InventoryBound);
    }
    Ok(bytes)
}

fn decode_bounded<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    maximum: usize,
) -> Result<T, GraphWaitPreparationError> {
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(GraphWaitPreparationError::InventoryBound);
    }
    serde_json::from_slice(bytes).map_err(|_| GraphWaitPreparationError::InventoryMismatch)
}

fn domain_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, GraphWaitPreparationError> {
    let canonical =
        serde_json_canonicalizer::to_vec(value).map_err(|_| GraphWaitPreparationError::Encoding)?;
    let mut bytes = Vec::with_capacity(domain.len() + canonical.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&canonical);
    Ok(Digest::sha256(bytes))
}

fn bounded_condition_references<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ConditionReference>, D::Error> {
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value = Vec<ConditionReference>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("one to 64 original condition references")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut values =
                Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(NodeWaits::MAX_LEN));
            while values.len() < NodeWaits::MAX_LEN {
                let Some(value) = sequence.next_element()? else {
                    if values.is_empty() {
                        return Err(serde::de::Error::custom(
                            "empty original condition inventory",
                        ));
                    }
                    return Ok(values);
                };
                values.push(value);
            }
            if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(
                    "original condition inventory bound",
                ));
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Visitor)
}

// This exact catalog check is mandatory before the new local rows can be read
// as deployed schema. Migrations and role deployment remain separate operations.
pub(super) async fn verify_schema(pool: &sqlx_postgres::PgPool) -> Result<(), super::StoreError> {
    let actual =
        sqlx_core::query_scalar::query_scalar::<_, String>(include_str!("graph_wait_catalog.sql"))
            .fetch_one(pool)
            .await
            .map_err(|e| super::StoreError::database("graph Wait condition catalog", e))?;
    let expected: serde_json::Value = serde_json::from_str(include_str!("graph_wait_catalog.json"))
        .map_err(|_| super::StoreError::IncompleteSchema)?;
    if serde_json::from_str::<serde_json::Value>(&actual)
        .map_err(|_| super::StoreError::IncompleteSchema)?
        != expected
    {
        return Err(super::StoreError::IncompleteSchema);
    }
    Ok(())
}
