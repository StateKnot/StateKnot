// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Experimental RFC-0022 scoped barrier integrity. No storage authority.

use std::{collections::BTreeMap, fmt};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    CheckpointWrite, Digest, GraphFrameCheckpoint, GraphFrameCheckpointHead, NodeId,
    PendingNodeResultHead, ReadyNodes,
};

const DIGEST_DOMAIN: &[u8] = b"stateknot-graph-frame-barrier-intent-v1\0";

/// Exact bounded result set and successor intent for one isolated frame.
///
/// Construction verifies the full base checkpoint. Restoring this compact
/// value verifies its internal scope, ready-set coverage and checksum; storage
/// must additionally reload the full base, exact result rows and journal anchors
/// and revalidate the active leaf and live fence inside the commit transaction.
/// The legacy root [`crate::CheckpointBarrier`] remains root-only.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFrameBarrier {
    base_checkpoint: GraphFrameCheckpointHead,
    base_ready_nodes: ReadyNodes,
    #[schemars(schema_with = "result_heads_schema")]
    result_heads: Box<[PendingNodeResultHead]>,
    successor: CheckpointWrite,
    intent_digest: Digest,
}

impl GraphFrameBarrier {
    /// Maximum results, equal to the existing ready-node bound.
    pub const MAX_RESULTS: usize = ReadyNodes::MAX_LEN;

    /// Constructs an intent from a full verified frame checkpoint.
    ///
    /// Results are ordered by node identity before hashing. Every result must
    /// carry the exact scoped ready activation, including its input digest.
    ///
    /// # Errors
    /// Rejects incomplete, repeated, oversized or substituted results, a
    /// successor outside the exact local lineage, or canonicalization failure.
    pub fn new(
        base: &GraphFrameCheckpoint,
        successor: CheckpointWrite,
        results: impl IntoIterator<Item = PendingNodeResultHead>,
    ) -> Result<Self, GraphFrameBarrierError> {
        let results = collect_results(results)?;
        Self::build(
            base.head(),
            base.checkpoint().ready_nodes().clone(),
            results,
            successor,
            None,
        )
    }

    fn build(
        base_checkpoint: GraphFrameCheckpointHead,
        base_ready_nodes: ReadyNodes,
        results: Vec<PendingNodeResultHead>,
        successor: CheckpointWrite,
        supplied_digest: Option<Digest>,
    ) -> Result<Self, GraphFrameBarrierError> {
        if base_ready_nodes.is_empty() {
            return Err(GraphFrameBarrierError::EmptyReadySet);
        }
        if successor.parent() != Some(base_checkpoint.checkpoint()) {
            return Err(GraphFrameBarrierError::SuccessorParentMismatch);
        }
        let mut ordered = BTreeMap::new();
        for result in results {
            let activation = result.activation();
            let node = activation.node_id();
            let expected_input = crate::tool_invocation::compute_ready_node_input_digest(
                base_checkpoint.checkpoint().digest(),
                base_checkpoint.frame().namespace(),
                node,
            )
            .map_err(|_| GraphFrameBarrierError::CanonicalSerialization)?;
            if activation.base_checkpoint() != base_checkpoint.checkpoint()
                || activation.graph_namespace() != base_checkpoint.frame().namespace()
                || activation.input_digest() != expected_input
                || !base_ready_nodes.contains(node)
            {
                return Err(GraphFrameBarrierError::ActivationMismatch {
                    node_id: node.clone(),
                });
            }
            let node = node.clone();
            if ordered.insert(node.clone(), result).is_some() {
                return Err(GraphFrameBarrierError::DuplicateNode { node_id: node });
            }
        }
        for node in &base_ready_nodes {
            if !ordered.contains_key(node) {
                return Err(GraphFrameBarrierError::MissingNode {
                    node_id: node.clone(),
                });
            }
        }
        let result_heads: Box<[_]> = ordered.into_values().collect();
        let canonical = serde_json_canonicalizer::to_vec(&DigestWire {
            base_checkpoint: &base_checkpoint,
            base_ready_nodes: &base_ready_nodes,
            result_heads: &result_heads,
            successor_intent_digest: successor.intent_digest(),
        })
        .map_err(|_| GraphFrameBarrierError::CanonicalSerialization)?;
        let mut preimage = Vec::with_capacity(DIGEST_DOMAIN.len() + canonical.len());
        preimage.extend_from_slice(DIGEST_DOMAIN);
        preimage.extend_from_slice(&canonical);
        let intent_digest = Digest::sha256(preimage);
        if supplied_digest.is_some_and(|digest| digest != intent_digest) {
            return Err(GraphFrameBarrierError::DigestMismatch);
        }
        Ok(Self {
            base_checkpoint,
            base_ready_nodes,
            result_heads,
            successor,
            intent_digest,
        })
    }

    /// Returns the exact frame and local base position.
    #[must_use]
    pub const fn base_checkpoint(&self) -> &GraphFrameCheckpointHead {
        &self.base_checkpoint
    }

    /// Returns the ready set copied from the full verified base.
    #[must_use]
    pub const fn base_ready_nodes(&self) -> &ReadyNodes {
        &self.base_ready_nodes
    }

    /// Returns the complete canonical node result set.
    #[must_use]
    pub const fn result_heads(&self) -> &[PendingNodeResultHead] {
        &self.result_heads
    }

    /// Returns the exact successor write.
    #[must_use]
    pub const fn successor(&self) -> &CheckpointWrite {
        &self.successor
    }

    /// Returns the domain-separated idempotency fingerprint.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Consumes the intent while retaining the frame through the base head.
    #[must_use]
    pub fn into_parts(self) -> (GraphFrameCheckpointHead, CheckpointWrite) {
        (self.base_checkpoint, self.successor)
    }
}

impl fmt::Debug for GraphFrameBarrier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GraphFrameBarrier")
            .field("base_checkpoint", &self.base_checkpoint)
            .field("ready_count", &self.base_ready_nodes.len())
            .field("result_count", &self.result_heads.len())
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for GraphFrameBarrier {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            base_checkpoint: GraphFrameCheckpointHead,
            base_ready_nodes: ReadyNodes,
            #[serde(deserialize_with = "bounded_results")]
            result_heads: Vec<PendingNodeResultHead>,
            successor: CheckpointWrite,
            intent_digest: Digest,
        }
        let wire = crate::json::deserialize_object::<Wire, _>(deserializer)?;
        // Unlike construction, persisted readers require the canonical order.
        if wire
            .result_heads
            .windows(2)
            .any(|pair| pair[0].activation().node_id() >= pair[1].activation().node_id())
        {
            return Err(de::Error::custom(GraphFrameBarrierError::NoncanonicalOrder));
        }
        Self::build(
            wire.base_checkpoint,
            wire.base_ready_nodes,
            wire.result_heads,
            wire.successor,
            Some(wire.intent_digest),
        )
        .map_err(de::Error::custom)
    }
}

fn collect_results(
    results: impl IntoIterator<Item = PendingNodeResultHead>,
) -> Result<Vec<PendingNodeResultHead>, GraphFrameBarrierError> {
    let mut bounded = Vec::new();
    for result in results {
        if bounded.len() == GraphFrameBarrier::MAX_RESULTS {
            return Err(GraphFrameBarrierError::TooManyResults);
        }
        bounded.push(result);
    }
    Ok(bounded)
}

fn bounded_results<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<PendingNodeResultHead>, D::Error> {
    struct Visitor;
    impl<'de> de::Visitor<'de> for Visitor {
        type Value = Vec<PendingNodeResultHead>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("at most 1024 scoped pending result heads")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut values = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or(0)
                    .min(GraphFrameBarrier::MAX_RESULTS),
            );
            while let Some(value) = sequence.next_element()? {
                if values.len() == GraphFrameBarrier::MAX_RESULTS {
                    return Err(de::Error::custom(GraphFrameBarrierError::TooManyResults));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Visitor)
}

fn result_heads_schema(generator: &mut SchemaGenerator) -> Schema {
    json_schema!({ "type": "array", "items": generator.subschema_for::<PendingNodeResultHead>(),
        "minItems": 1, "maxItems": 1024, "uniqueItems": true,
        "description": "Canonical node order; every exact scoped ready activation must occur once." })
}

#[derive(Serialize)]
struct DigestWire<'a> {
    base_checkpoint: &'a GraphFrameCheckpointHead,
    base_ready_nodes: &'a ReadyNodes,
    result_heads: &'a [PendingNodeResultHead],
    successor_intent_digest: Digest,
}

/// Public-safe failure to bind a frame barrier. Contains no state or output.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphFrameBarrierError {
    /// A quiescent checkpoint cannot advance a result barrier.
    #[error("frame barrier base ready set is empty")]
    EmptyReadySet,
    /// The local successor does not name the exact base checkpoint.
    #[error("frame barrier successor does not name its exact base")]
    SuccessorParentMismatch,
    /// A result crossed scope, ready-set membership or input identity.
    #[error("frame barrier result activation for node {node_id} does not match its scoped base")]
    ActivationMismatch {
        /// Rejected node.
        node_id: NodeId,
    },
    /// A ready node appeared more than once.
    #[error("frame barrier repeats node {node_id}")]
    DuplicateNode {
        /// Repeated node.
        node_id: NodeId,
    },
    /// A ready node had no result.
    #[error("frame barrier is missing node {node_id}")]
    MissingNode {
        /// Missing node.
        node_id: NodeId,
    },
    /// The finite result ceiling was exceeded before collecting another value.
    #[error("frame barrier has more than 1024 result heads")]
    TooManyResults,
    /// A persisted result set was not in strictly ascending node order.
    #[error("frame barrier result heads are not in canonical node order")]
    NoncanonicalOrder,
    /// Closed typed integrity material could not be canonicalized.
    #[error("frame barrier integrity material could not be serialized")]
    CanonicalSerialization,
    /// Persisted fields did not match the fingerprint.
    #[error("frame barrier intent digest does not match its fields")]
    DigestMismatch,
}
