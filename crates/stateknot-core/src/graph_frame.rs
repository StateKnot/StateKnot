// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Integrity-bound data for RFC-0022's experimental same-Run graph frames.
//!
//! These values validate scope and checksums; they are not admission receipts.
//! Nested execution is not supported until the compiler, registry, transactional
//! store and recovery driver implement the complete RFC acceptance gates.

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    Checkpoint, CheckpointHead, Digest, GraphNamespace, GraphReference, NodeActivation, NodeId,
};

const NAMESPACE_DOMAIN: &[u8] = b"stateknot-graph-frame-namespace-v1\0";
const IDENTITY_DOMAIN: &[u8] = b"stateknot-graph-frame-identity-v1\0";
const CHECKPOINT_DOMAIN: &[u8] = b"stateknot-graph-frame-checkpoint-v1\0";

/// Immutable logical child-frame identity, independent of physical attempts.
///
/// The namespace binds the caller and slot, while the checksum also binds the
/// exact target pin. Substituting the target at one namespace is a conflict.
/// Construction proves neither caller readiness nor a declared, authorized
/// call. The store must verify those facts under the current Run fence.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFrameIdentity {
    origin: NodeActivation,
    slot: NodeId,
    target: GraphReference,
    namespace: GraphNamespace,
    digest: Digest,
}

impl GraphFrameIdentity {
    /// Seven full SHA-256 segments fit the existing 512-byte namespace bound.
    pub const MAX_DEPTH: usize = 7;

    /// Derives a same-owner, isolated-state frame from bounded domain data.
    ///
    /// # Errors
    ///
    /// Rejects an invalid ancestor namespace, excessive depth, another owner,
    /// a different state schema, or canonical encoding failure.
    pub fn new(
        origin: NodeActivation,
        slot: NodeId,
        target: GraphReference,
    ) -> Result<Self, GraphFrameError> {
        if origin.base_checkpoint().graph().identity().owner() != target.identity().owner() {
            return Err(GraphFrameError::OwnerMismatch);
        }
        if origin.base_checkpoint().graph().state_schema() != target.state_schema() {
            return Err(GraphFrameError::StateSchemaMismatch);
        }
        let parent = origin.graph_namespace().as_str();
        let depth = frame_depth(origin.graph_namespace())?;
        if depth >= Self::MAX_DEPTH {
            return Err(GraphFrameError::DepthExceeded);
        }
        let segment = checksum(
            NAMESPACE_DOMAIN,
            &NamespacePreimage {
                origin: &origin,
                slot: &slot,
            },
        )?;
        // Digest's display is fixed `sha256:` followed by 64 lowercase hex bytes.
        let encoded = segment.to_string();
        let segment = &encoded["sha256:".len()..];
        let namespace = GraphNamespace::new(if parent.is_empty() {
            segment.to_owned()
        } else {
            format!("{parent}/{segment}")
        })
        .map_err(|_| GraphFrameError::InvalidNamespace)?;
        let digest = checksum(
            IDENTITY_DOMAIN,
            &IdentityPreimage {
                origin: &origin,
                slot: &slot,
                target: &target,
                namespace: &namespace,
            },
        )?;
        Ok(Self {
            origin,
            slot,
            target,
            namespace,
            digest,
        })
    }

    /// Returns the exact suspended caller activation.
    #[must_use]
    pub const fn origin(&self) -> &NodeActivation {
        &self.origin
    }
    /// Returns the declared stable child slot.
    #[must_use]
    pub const fn slot(&self) -> &NodeId {
        &self.slot
    }
    /// Returns the pinned child implementation and state schema.
    #[must_use]
    pub const fn target(&self) -> &GraphReference {
        &self.target
    }
    /// Returns the derived non-root namespace.
    #[must_use]
    pub const fn namespace(&self) -> &GraphNamespace {
        &self.namespace
    }
    /// Returns the checksum of all immutable identity fields.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for GraphFrameIdentity {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            origin: NodeActivation,
            slot: NodeId,
            target: GraphReference,
            namespace: GraphNamespace,
            digest: Digest,
        }
        let wire = crate::json::deserialize_object::<Wire, _>(deserializer)?;
        let result = Self::new(wire.origin, wire.slot, wire.target).map_err(de::Error::custom)?;
        if result.namespace != wire.namespace || result.digest != wire.digest {
            return Err(de::Error::custom(GraphFrameError::DigestMismatch));
        }
        Ok(result)
    }
}

/// A fully validated checkpoint bound to one exact logical child frame.
///
/// Ordinary checkpoint bytes remain unchanged. This additional binding prevents
/// a scoped projection from treating the same graph/head as another frame.
/// Admission, journal anchoring and same-frame predecessor storage still require
/// the transactional store; a checksum is not proof of execution or authority.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFrameCheckpoint {
    frame: GraphFrameIdentity,
    checkpoint: Checkpoint,
    digest: Digest,
}

impl GraphFrameCheckpoint {
    /// Binds a validated checkpoint that follows its caller's journal prefix.
    ///
    /// # Errors
    ///
    /// Rejects tenant, Run, graph, journal-order or checksum encoding mismatches.
    pub fn new(frame: GraphFrameIdentity, checkpoint: Checkpoint) -> Result<Self, GraphFrameError> {
        validate_checkpoint_scope(&frame, &checkpoint.head())?;
        let digest = checkpoint_digest(&frame, &checkpoint.head())?;
        Ok(Self {
            frame,
            checkpoint,
            digest,
        })
    }

    /// Returns the immutable frame binding.
    #[must_use]
    pub const fn frame(&self) -> &GraphFrameIdentity {
        &self.frame
    }
    /// Returns the fully restored checkpoint, including validated state.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
    /// Returns the checksum of frame identity and complete checkpoint head.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Returns compact scope data after validating the complete checkpoint.
    #[must_use]
    pub fn head(&self) -> GraphFrameCheckpointHead {
        GraphFrameCheckpointHead {
            frame: self.frame.clone(),
            checkpoint: self.checkpoint.head(),
            digest: self.digest,
        }
    }

    /// Consumes the binding into its validated immutable frame and checkpoint.
    ///
    /// This transfers owned state without cloning its bounded JSON payload.
    #[must_use]
    pub fn into_parts(self) -> (GraphFrameIdentity, Checkpoint) {
        (self.frame, self.checkpoint)
    }

    /// Checks a contiguous successor in this exact frame.
    ///
    /// # Errors
    ///
    /// Rejects another frame or any predecessor other than this exact head.
    pub fn verify_successor(&self, next: &Self) -> Result<(), GraphFrameError> {
        if self.frame != next.frame {
            return Err(GraphFrameError::FrameMismatch);
        }
        if next.checkpoint.parent() != Some(&self.checkpoint.head()) {
            return Err(GraphFrameError::PredecessorMismatch);
        }
        Ok(())
    }

    /// Derives an activation for an actually ready node in this bound frame.
    ///
    /// The existing activation domain binds checkpoint digest, exact namespace
    /// and node identity. Root derivation and historical root bytes are unchanged.
    ///
    /// # Errors
    ///
    /// Rejects an absent ready node or canonical encoding failure.
    pub fn activation(&self, node_id: NodeId) -> Result<NodeActivation, GraphFrameError> {
        if !self.checkpoint.ready_nodes().contains(&node_id) {
            return Err(GraphFrameError::NodeNotReady { node_id });
        }
        let input_digest = crate::tool_invocation::compute_ready_node_input_digest(
            self.checkpoint.digest(),
            self.frame.namespace(),
            &node_id,
        )
        .map_err(|_| GraphFrameError::CanonicalSerialization)?;
        Ok(NodeActivation::new(
            self.checkpoint.head(),
            self.frame.namespace().clone(),
            node_id,
            input_digest,
        ))
    }
}

impl<'de> Deserialize<'de> for GraphFrameCheckpoint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            frame: GraphFrameIdentity,
            checkpoint: Checkpoint,
            digest: Digest,
        }
        let wire = crate::json::deserialize_object::<Wire, _>(deserializer)?;
        let result = Self::new(wire.frame, wire.checkpoint).map_err(de::Error::custom)?;
        if result.digest != wire.digest {
            return Err(de::Error::custom(GraphFrameError::DigestMismatch));
        }
        Ok(result)
    }
}

/// Compact frame checkpoint identity for optimistic comparison.
///
/// Like [`CheckpointHead`], this omits state and cannot independently prove the
/// checkpoint checksum. Obtain it from [`GraphFrameCheckpoint::head`] or storage
/// that has restored and verified the full checkpoint and its journal anchor.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFrameCheckpointHead {
    frame: GraphFrameIdentity,
    checkpoint: CheckpointHead,
    digest: Digest,
}
impl GraphFrameCheckpointHead {
    /// Returns the exact logical frame.
    #[must_use]
    pub const fn frame(&self) -> &GraphFrameIdentity {
        &self.frame
    }
    /// Returns the exact local checkpoint position and committed checksum.
    #[must_use]
    pub const fn checkpoint(&self) -> &CheckpointHead {
        &self.checkpoint
    }
    /// Returns the frame/checkpoint binding checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}
impl<'de> Deserialize<'de> for GraphFrameCheckpointHead {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            frame: GraphFrameIdentity,
            checkpoint: CheckpointHead,
            digest: Digest,
        }
        let wire = crate::json::deserialize_object::<Wire, _>(deserializer)?;
        validate_checkpoint_scope(&wire.frame, &wire.checkpoint).map_err(de::Error::custom)?;
        let digest = checkpoint_digest(&wire.frame, &wire.checkpoint).map_err(de::Error::custom)?;
        if digest != wire.digest {
            return Err(de::Error::custom(GraphFrameError::DigestMismatch));
        }
        Ok(Self {
            frame: wire.frame,
            checkpoint: wire.checkpoint,
            digest,
        })
    }
}

/// Invalid or corrupted experimental frame data. These errors carry no state.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphFrameError {
    /// Frame callers and targets must have the same exact principal owner.
    #[error("graph frame target has another owner")]
    OwnerMismatch,
    /// The isolated-state profile requires exactly equal state schema pins.
    #[error("graph frame target state schema differs from its caller")]
    StateSchemaMismatch,
    /// Frame ancestry consists only of full lowercase SHA-256 segments.
    #[error("graph frame ancestor namespace is not a canonical frame path")]
    InvalidNamespace,
    /// An eighth nested frame cannot fit the existing namespace ceiling.
    #[error("graph frame exceeds the seven-level nesting bound")]
    DepthExceeded,
    /// Checkpoints cannot cross the caller's tenant or Run.
    #[error("graph frame checkpoint crosses its caller tenant or Run")]
    CheckpointScopeMismatch,
    /// A checkpoint must retain the exact declared target graph and schemas.
    #[error("graph frame checkpoint has another graph pin")]
    CheckpointGraphMismatch,
    /// A child's journal head must follow the suspended caller's head.
    #[error("graph frame checkpoint does not follow its caller journal prefix")]
    JournalOrder,
    /// A serialized binding did not match its reconstructed identity.
    #[error("graph frame checksum or derived namespace does not match")]
    DigestMismatch,
    /// A successor was bound to another logical frame.
    #[error("graph frame successor has another frame identity")]
    FrameMismatch,
    /// A successor did not name the current exact predecessor.
    #[error("graph frame successor does not name the current checkpoint")]
    PredecessorMismatch,
    /// A node was not ready at the scoped checkpoint.
    #[error("graph frame node {node_id} is not ready")]
    NodeNotReady {
        /// Rejected bounded node identity.
        node_id: NodeId,
    },
    /// A closed checksum preimage could not be encoded.
    #[error("graph frame canonical serialization failed")]
    CanonicalSerialization,
}

#[derive(Serialize)]
struct NamespacePreimage<'a> {
    origin: &'a NodeActivation,
    slot: &'a NodeId,
}
#[derive(Serialize)]
struct IdentityPreimage<'a> {
    origin: &'a NodeActivation,
    slot: &'a NodeId,
    target: &'a GraphReference,
    namespace: &'a GraphNamespace,
}
#[derive(Serialize)]
struct CheckpointPreimage<'a> {
    frame_identity: Digest,
    checkpoint_head: &'a CheckpointHead,
}

fn frame_depth(namespace: &GraphNamespace) -> Result<usize, GraphFrameError> {
    if namespace.is_root() {
        return Ok(0);
    }
    let mut depth = 0;
    for segment in namespace.as_str().split('/') {
        if segment.len() != Digest::SHA256_LEN * 2
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(GraphFrameError::InvalidNamespace);
        }
        depth += 1;
    }
    Ok(depth)
}
fn validate_checkpoint_scope(
    frame: &GraphFrameIdentity,
    checkpoint: &CheckpointHead,
) -> Result<(), GraphFrameError> {
    if frame.origin.tenant_id() != checkpoint.tenant_id()
        || frame.origin.run_id() != checkpoint.run_id()
    {
        return Err(GraphFrameError::CheckpointScopeMismatch);
    }
    if frame.target != *checkpoint.graph() {
        return Err(GraphFrameError::CheckpointGraphMismatch);
    }
    let parent = frame.origin.base_checkpoint().journal_head();
    if checkpoint.journal_head().sequence() <= parent.sequence()
        || checkpoint.journal_head().recorded_at() < parent.recorded_at()
    {
        return Err(GraphFrameError::JournalOrder);
    }
    Ok(())
}
fn checkpoint_digest(
    frame: &GraphFrameIdentity,
    checkpoint: &CheckpointHead,
) -> Result<Digest, GraphFrameError> {
    checksum(
        CHECKPOINT_DOMAIN,
        &CheckpointPreimage {
            frame_identity: frame.digest(),
            checkpoint_head: checkpoint,
        },
    )
}
fn checksum<T: Serialize>(domain: &[u8], value: &T) -> Result<Digest, GraphFrameError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| GraphFrameError::CanonicalSerialization)?;
    let mut bytes = Vec::with_capacity(domain.len() + canonical.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&canonical);
    Ok(Digest::sha256(bytes))
}
