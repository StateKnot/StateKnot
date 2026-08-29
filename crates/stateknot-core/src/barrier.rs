// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Integrity-bound inputs for one deterministic graph checkpoint barrier.
//!
//! The graph runtime computes state reduction and routing before opening a
//! storage transaction. This module binds that successor write to the exact
//! immutable result set it consumed. Durable adapters must reload the full
//! base checkpoint and every result, then compare these compact identities
//! before committing the successor.

use std::{collections::BTreeSet, fmt};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, SeqAccess},
};
use thiserror::Error;

use crate::{
    Checkpoint, CheckpointHead, CheckpointWrite, Digest, GraphNamespace, NodeId,
    PendingNodeResultHead, ReadyNodes,
};

const BARRIER_INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-checkpoint-barrier-intent-v1\0";

/// Canonically ordered, duplicate-free result heads consumed by one barrier.
///
/// The collection is deliberately compact. Full pending results can be large
/// and must be loaded and reduced in bounded pages before constructing the
/// barrier. Storage rechecks these heads against immutable rows under the run
/// lock.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BarrierResultHeads(Box<[PendingNodeResultHead]>);

impl BarrierResultHeads {
    /// Maximum results in one barrier, equal to the checkpoint ready-set bound.
    pub const MAX_LEN: usize = ReadyNodes::MAX_LEN;

    /// Validates, canonicalizes, and constructs a non-empty result-head set.
    ///
    /// Values serialize in ascending `(graph_namespace, node_id)` order.
    /// V1 checkpoint ready sets describe the root graph, so nested namespaces
    /// are rejected until nested checkpoint scheduling has its own exact set.
    ///
    /// # Errors
    ///
    /// Returns [`BarrierResultHeadsError`] for an empty or oversized set,
    /// duplicate activation keys, mixed base checkpoints, or nested results.
    pub fn try_new<I>(values: I) -> Result<Self, BarrierResultHeadsError>
    where
        I: IntoIterator<Item = PendingNodeResultHead>,
    {
        let mut collected = Vec::new();
        let mut identities = BTreeSet::new();
        let mut base: Option<CheckpointHead> = None;
        for value in values {
            if collected.len() == Self::MAX_LEN {
                return Err(BarrierResultHeadsError::TooMany {
                    maximum: Self::MAX_LEN,
                    actual: Self::MAX_LEN + 1,
                });
            }
            let activation = value.activation();
            if !activation.graph_namespace().is_root() {
                return Err(BarrierResultHeadsError::NestedGraphNamespace {
                    namespace: activation.graph_namespace().clone(),
                });
            }
            if let Some(expected) = &base {
                if activation.base_checkpoint() != expected {
                    return Err(BarrierResultHeadsError::MixedBaseCheckpoints);
                }
            } else {
                base = Some(activation.base_checkpoint().clone());
            }
            let identity = (
                activation.graph_namespace().clone(),
                activation.node_id().clone(),
            );
            if !identities.insert(identity.clone()) {
                return Err(BarrierResultHeadsError::DuplicateActivation {
                    namespace: identity.0,
                    node_id: identity.1,
                });
            }
            collected.push(value);
        }
        if collected.is_empty() {
            return Err(BarrierResultHeadsError::Empty);
        }
        collected.sort_unstable_by(|left, right| {
            let left = left.activation();
            let right = right.activation();
            (left.graph_namespace(), left.node_id())
                .cmp(&(right.graph_namespace(), right.node_id()))
        });
        Ok(Self(collected.into_boxed_slice()))
    }

    /// Returns the number of logical activation results.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the collection is empty.
    ///
    /// Valid instances are never empty; this conventional collection method
    /// is provided so generic callers can use the type without assumptions.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates result heads in canonical activation order.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &PendingNodeResultHead> {
        self.0.iter()
    }

    /// Returns the exact canonical result-head slice.
    #[must_use]
    pub const fn as_slice(&self) -> &[PendingNodeResultHead] {
        &self.0
    }
}

impl Serialize for BarrierResultHeads {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BarrierResultHeads {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BarrierResultHeadsVisitor)
    }
}

struct BarrierResultHeadsVisitor;

impl<'de> de::Visitor<'de> for BarrierResultHeadsVisitor {
    type Value = BarrierResultHeads;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "one to {} unique root-graph pending result heads",
            BarrierResultHeads::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(
            sequence
                .size_hint()
                .unwrap_or_default()
                .min(BarrierResultHeads::MAX_LEN),
        );
        while let Some(value) = sequence.next_element::<PendingNodeResultHead>()? {
            if values.len() == BarrierResultHeads::MAX_LEN {
                return Err(de::Error::custom(BarrierResultHeadsError::TooMany {
                    maximum: BarrierResultHeads::MAX_LEN,
                    actual: BarrierResultHeads::MAX_LEN + 1,
                }));
            }
            values.push(value);
        }
        BarrierResultHeads::try_new(values).map_err(de::Error::custom)
    }
}

impl JsonSchema for BarrierResultHeads {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "BarrierResultHeads".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::BarrierResultHeads").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<PendingNodeResultHead>(),
            "minItems": 1,
            "maxItems": 1024,
            "uniqueItems": true,
            "description": "Serialized in canonical (graph_namespace, node_id) order; runtime additionally requires one exact base checkpoint."
        })
    }
}

/// Invalid compact result set for a checkpoint barrier.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum BarrierResultHeadsError {
    /// A barrier with no active nodes cannot advance a checkpoint.
    #[error("checkpoint barrier result set must not be empty")]
    Empty,
    /// The ready-set hard ceiling was exceeded.
    #[error("checkpoint barrier contains {actual} results; maximum is {maximum}")]
    TooMany {
        /// Absolute maximum.
        maximum: usize,
        /// First observed count beyond the maximum.
        actual: usize,
    },
    /// Two results named the same logical activation.
    #[error("checkpoint barrier contains a duplicate logical activation")]
    DuplicateActivation {
        /// Duplicated graph namespace.
        namespace: GraphNamespace,
        /// Duplicated node identity.
        node_id: NodeId,
    },
    /// Result heads did not share one exact immutable base checkpoint.
    #[error("checkpoint barrier result heads use different base checkpoints")]
    MixedBaseCheckpoints,
    /// V1 root-ready checkpoints cannot consume nested graph results.
    #[error("checkpoint barrier result belongs to an unsupported nested graph namespace")]
    NestedGraphNamespace {
        /// Rejected nested namespace.
        namespace: GraphNamespace,
    },
}

/// Integrity-bound semantic input to one successor checkpoint transaction.
///
/// This value binds the authentic base checkpoint head and ready set, every
/// pending result consumed, and the exact successor write intent. It does not
/// execute reducers or graph callbacks; those must finish before construction.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointBarrier {
    base_checkpoint: CheckpointHead,
    base_ready_nodes: ReadyNodes,
    result_heads: BarrierResultHeads,
    successor: CheckpointWrite,
    intent_digest: Digest,
}

impl CheckpointBarrier {
    /// Constructs a barrier from a fully validated base checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointBarrierError`] unless results exactly cover the
    /// base ready set, the successor names that exact base, and integrity bytes
    /// can be canonicalized.
    pub fn new<I>(
        base: &Checkpoint,
        successor: CheckpointWrite,
        result_heads: I,
    ) -> Result<Self, CheckpointBarrierError>
    where
        I: IntoIterator<Item = PendingNodeResultHead>,
    {
        let result_heads =
            BarrierResultHeads::try_new(result_heads).map_err(CheckpointBarrierError::results)?;
        Self::build(
            base.head(),
            base.ready_nodes().clone(),
            result_heads,
            successor,
            None,
        )
    }

    /// Restores a serialized barrier and verifies its complete intent digest.
    ///
    /// Durable adapters must additionally compare `base_ready_nodes` with the
    /// fully restored base checkpoint before accepting this compact proof.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointBarrierError`] for any structural or integrity
    /// mismatch.
    pub fn restore(
        base_checkpoint: CheckpointHead,
        base_ready_nodes: ReadyNodes,
        result_heads: BarrierResultHeads,
        successor: CheckpointWrite,
        intent_digest: Digest,
    ) -> Result<Self, CheckpointBarrierError> {
        Self::build(
            base_checkpoint,
            base_ready_nodes,
            result_heads,
            successor,
            Some(intent_digest),
        )
    }

    fn build(
        base_checkpoint: CheckpointHead,
        base_ready_nodes: ReadyNodes,
        result_heads: BarrierResultHeads,
        successor: CheckpointWrite,
        supplied_digest: Option<Digest>,
    ) -> Result<Self, CheckpointBarrierError> {
        validate_barrier_shape(
            &base_checkpoint,
            &base_ready_nodes,
            &result_heads,
            &successor,
        )?;
        let intent_digest = compute_barrier_digest(&CheckpointBarrierDigestWire {
            base_checkpoint: &base_checkpoint,
            base_ready_nodes: &base_ready_nodes,
            result_heads: &result_heads,
            successor_intent_digest: successor.intent_digest(),
        })?;
        if supplied_digest.is_some_and(|supplied| supplied != intent_digest) {
            return Err(CheckpointBarrierError::DigestMismatch);
        }
        Ok(Self {
            base_checkpoint,
            base_ready_nodes,
            result_heads,
            successor,
            intent_digest,
        })
    }

    /// Returns the exact base checkpoint compact identity.
    #[must_use]
    pub const fn base_checkpoint(&self) -> &CheckpointHead {
        &self.base_checkpoint
    }

    /// Returns the ready set copied from the validated base checkpoint.
    #[must_use]
    pub const fn base_ready_nodes(&self) -> &ReadyNodes {
        &self.base_ready_nodes
    }

    /// Returns exact consumed result identities in stable reduction order.
    #[must_use]
    pub const fn result_heads(&self) -> &BarrierResultHeads {
        &self.result_heads
    }

    /// Returns the exact successor checkpoint write intent.
    #[must_use]
    pub const fn successor(&self) -> &CheckpointWrite {
        &self.successor
    }

    /// Consumes the barrier and returns its successor write intent.
    #[must_use]
    pub fn into_successor(self) -> CheckpointWrite {
        self.successor
    }

    /// Returns the domain-separated idempotency fingerprint.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }
}

impl fmt::Debug for CheckpointBarrier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointBarrier")
            .field("base_checkpoint", &self.base_checkpoint)
            .field("base_ready_count", &self.base_ready_nodes.len())
            .field("result_count", &self.result_heads.len())
            .field("successor", &self.successor)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for CheckpointBarrier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            base_checkpoint: CheckpointHead,
            base_ready_nodes: ReadyNodes,
            result_heads: BarrierResultHeads,
            successor: CheckpointWrite,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.base_checkpoint,
            wire.base_ready_nodes,
            wire.result_heads,
            wire.successor,
            wire.intent_digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid or corrupted checkpoint barrier intent.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointBarrierError {
    /// The compact result collection was intrinsically invalid.
    #[error("checkpoint barrier result set is invalid: {source}")]
    Results {
        /// Exact collection error.
        #[source]
        source: BarrierResultHeadsError,
    },
    /// The copied base ready set was empty.
    #[error("checkpoint barrier base ready set must not be empty")]
    EmptyReadySet,
    /// The successor did not name the exact base checkpoint as its parent.
    #[error("checkpoint barrier successor does not name the exact base checkpoint")]
    SuccessorParentMismatch,
    /// Result heads belonged to another immutable base checkpoint.
    #[error("checkpoint barrier result does not belong to the base checkpoint")]
    ResultBaseMismatch,
    /// A result named a node outside the base ready set.
    #[error("checkpoint barrier result node was not ready in the base checkpoint")]
    UnexpectedResultNode {
        /// Rejected node identity.
        node_id: NodeId,
    },
    /// At least one ready node had no result.
    #[error("checkpoint barrier does not contain every base ready node")]
    MissingResultNode {
        /// Missing node identity.
        node_id: NodeId,
    },
    /// Canonical barrier integrity material could not be produced.
    #[error("checkpoint barrier intent integrity calculation failed: {source}")]
    Integrity {
        /// Exact canonicalization failure.
        #[source]
        source: CheckpointBarrierIntegrityError,
    },
    /// Serialized caller-controlled fields did not match the barrier digest.
    #[error("checkpoint barrier intent digest does not match its fields")]
    DigestMismatch,
}

impl CheckpointBarrierError {
    const fn results(source: BarrierResultHeadsError) -> Self {
        Self::Results { source }
    }
}

impl From<CheckpointBarrierIntegrityError> for CheckpointBarrierError {
    fn from(source: CheckpointBarrierIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

fn validate_barrier_shape(
    base_checkpoint: &CheckpointHead,
    base_ready_nodes: &ReadyNodes,
    result_heads: &BarrierResultHeads,
    successor: &CheckpointWrite,
) -> Result<(), CheckpointBarrierError> {
    if base_ready_nodes.is_empty() {
        return Err(CheckpointBarrierError::EmptyReadySet);
    }
    if successor.parent() != Some(base_checkpoint) {
        return Err(CheckpointBarrierError::SuccessorParentMismatch);
    }
    let mut observed = BTreeSet::new();
    for head in result_heads.iter() {
        let activation = head.activation();
        if activation.base_checkpoint() != base_checkpoint {
            return Err(CheckpointBarrierError::ResultBaseMismatch);
        }
        if !base_ready_nodes.contains(activation.node_id()) {
            return Err(CheckpointBarrierError::UnexpectedResultNode {
                node_id: activation.node_id().clone(),
            });
        }
        observed.insert(activation.node_id().clone());
    }
    for node_id in base_ready_nodes {
        if !observed.contains(node_id) {
            return Err(CheckpointBarrierError::MissingResultNode {
                node_id: node_id.clone(),
            });
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct CheckpointBarrierDigestWire<'a> {
    base_checkpoint: &'a CheckpointHead,
    base_ready_nodes: &'a ReadyNodes,
    result_heads: &'a BarrierResultHeads,
    successor_intent_digest: Digest,
}

fn compute_barrier_digest(
    value: &CheckpointBarrierDigestWire<'_>,
) -> Result<Digest, CheckpointBarrierIntegrityError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| CheckpointBarrierIntegrityError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(BARRIER_INTENT_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(BARRIER_INTENT_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

/// Failure to canonicalize checkpoint barrier integrity material.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointBarrierIntegrityError {
    /// A closed typed checksum preimage could not be canonicalized.
    #[error("checkpoint barrier checksum preimage serialization failed")]
    CanonicalSerialization,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AttemptId, CheckpointId, FencingEpoch, GraphNamespace, JournalHead, JournalSequence,
        NodeActivation, PendingNodeResult, PendingNodeResultIntent, RunFence, Timestamp,
    };
    use serde_json::{Value, from_value, json, to_value};

    fn checkpoint() -> Checkpoint {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-checkpoint-v1.json"))
                .unwrap();
        from_value(fixture["checkpoints"][0].clone()).unwrap()
    }

    fn result(base: &Checkpoint, node_id: NodeId, sequence: u64) -> PendingNodeResult {
        let activation = NodeActivation::new(
            base.head(),
            GraphNamespace::root(),
            node_id,
            Digest::sha256(sequence.to_be_bytes()),
        );
        let intent = PendingNodeResultIntent::new(
            activation.clone(),
            crate::NodeStateChange::Unchanged,
            crate::NodeControl::Continue,
            crate::NodeInvocationBindings::empty(),
        )
        .unwrap();
        let recorded_at = Timestamp::from_unix_micros(
            base.journal_head().recorded_at().unix_micros()
                + i64::try_from(sequence).unwrap() * 1_000_000,
        )
        .unwrap();
        let event_id = format!("01912345-6789-7abc-8def-0123456789{sequence:02x}")
            .parse()
            .unwrap();
        let journal = JournalHead::new(
            base.tenant_id().clone(),
            base.run_id(),
            JournalSequence::new(base.journal_head().sequence().get() + sequence).unwrap(),
            event_id,
            recorded_at,
            Digest::sha256(format!("result-{sequence}")),
        );
        let fence = RunFence::new(
            base.tenant_id().clone(),
            base.run_id(),
            format!("01912345-6789-7abc-8def-0123456788{sequence:02x}")
                .parse::<AttemptId>()
                .unwrap(),
            FencingEpoch::new(sequence).unwrap(),
        );
        PendingNodeResult::commit(intent, fence, journal).unwrap()
    }

    fn successor(base: &Checkpoint) -> CheckpointWrite {
        CheckpointWrite::successor(
            CheckpointId::generate(),
            base,
            base.state().clone(),
            ReadyNodes::empty(),
        )
        .unwrap()
    }

    #[test]
    fn exact_ready_coverage_is_canonical_and_integrity_bound() {
        let base = checkpoint();
        let heads = base
            .ready_nodes()
            .iter()
            .rev()
            .enumerate()
            .map(|(index, node_id)| {
                result(&base, node_id.clone(), u64::try_from(index + 1).unwrap()).head()
            })
            .collect::<Vec<_>>();
        let barrier = CheckpointBarrier::new(&base, successor(&base), heads).unwrap();
        assert_eq!(barrier.result_heads().len(), base.ready_nodes().len());
        assert!(
            barrier
                .result_heads()
                .iter()
                .map(|head| head.activation().node_id())
                .is_sorted()
        );
        assert_eq!(
            from_value::<CheckpointBarrier>(to_value(&barrier).unwrap()).unwrap(),
            barrier
        );

        let mut changed = to_value(&barrier).unwrap();
        changed["successor"]["ready_nodes"] = json!(["substituted"]);
        assert!(from_value::<CheckpointBarrier>(changed).is_err());
    }

    #[test]
    fn barrier_rejects_missing_unexpected_mixed_and_nested_results() {
        let base = checkpoint();
        let first = base.ready_nodes().iter().next().unwrap().clone();
        let one = result(&base, first.clone(), 1).head();
        if base.ready_nodes().len() > 1 {
            assert!(matches!(
                CheckpointBarrier::new(&base, successor(&base), [one.clone()]),
                Err(CheckpointBarrierError::MissingResultNode { .. })
            ));
        }

        let unexpected = result(&base, NodeId::new("unexpected").unwrap(), 2).head();
        assert!(matches!(
            CheckpointBarrier::new(&base, successor(&base), [unexpected]),
            Err(CheckpointBarrierError::UnexpectedResultNode { .. })
        ));

        let other_base = Checkpoint::commit(successor(&base), one.journal_head().clone()).unwrap();
        let mixed = result(&other_base, NodeId::new("nested-result").unwrap(), 3).head();
        assert!(matches!(
            BarrierResultHeads::try_new([one.clone(), mixed]),
            Err(BarrierResultHeadsError::MixedBaseCheckpoints)
        ));

        let nested_activation = NodeActivation::new(
            base.head(),
            GraphNamespace::new("nested").unwrap(),
            first,
            Digest::sha256(b"nested"),
        );
        let nested_intent = PendingNodeResultIntent::new(
            nested_activation.clone(),
            crate::NodeStateChange::Unchanged,
            crate::NodeControl::Continue,
            crate::NodeInvocationBindings::empty(),
        )
        .unwrap();
        let nested = PendingNodeResult::commit(
            nested_intent,
            RunFence::new(
                base.tenant_id().clone(),
                base.run_id(),
                "01912345-6789-7abc-8def-0123456788ff".parse().unwrap(),
                FencingEpoch::FIRST,
            ),
            one.journal_head().clone(),
        )
        .unwrap();
        assert!(matches!(
            BarrierResultHeads::try_new([nested.head()]),
            Err(BarrierResultHeadsError::NestedGraphNamespace { .. })
        ));
    }
}
