// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Pure child ownership identities, not authority or committed admission proof.

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{Digest, NodeActivation, NodeId, NodeIdError, RunId, TenantId};

const KEY_DOMAIN: &[u8] = b"stateknot-child-run-key-v1\0";

/// A stable, case-sensitive child slot declared by an executable definition.
///
/// Reuses the bounded [`NodeId`] grammar, but is a distinct Rust type. A slot
/// is not a child Run ID, physical attempt, arbitrary model string, or path.
/// Construction validates syntax only; declaration membership is a separate
/// admission check.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct ChildRunSlot(NodeId);

impl ChildRunSlot {
    /// Maximum encoded slot length in bytes.
    pub const MAX_LEN: usize = NodeId::MAX_LEN;

    /// Constructs a bounded slot using the same validation on every wire path.
    pub fn new(value: impl Into<String>) -> Result<Self, NodeIdError> {
        NodeId::new(value).map(Self)
    }

    /// Returns the exact declared slot text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Integrity-bound logical ownership key for one isolated child Run.
///
/// The key binds the complete parent activation and slot. It intentionally
/// excludes physical attempts, fences, clocks, and candidate child Run IDs.
/// Its digest can index an ownership record but cannot authorize admission:
/// the store must prove the activation's committed readiness, current worker
/// authority, declaration membership, and atomic budget reservation.
///
/// Deserialization recomputes the digest. A matching checksum detects drift;
/// it is not a signature and does not authenticate caller-supplied data.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunKey {
    parent: NodeActivation,
    slot: ChildRunSlot,
    digest: Digest,
}

impl ChildRunKey {
    /// Computes a deterministic logical key from validated domain values.
    pub fn new(parent: NodeActivation, slot: ChildRunSlot) -> Result<Self, ChildRunKeyError> {
        #[derive(Serialize)]
        struct Preimage<'a> {
            tenant_id: &'a TenantId,
            parent_run_id: RunId,
            parent_activation_digest: Digest,
            child_slot: &'a ChildRunSlot,
        }

        let activation_digest = crate::node_attempt::compute_activation_digest(&parent)
            .map_err(|_| ChildRunKeyError::CanonicalSerialization)?;
        let canonical = serde_json_canonicalizer::to_vec(&Preimage {
            tenant_id: parent.tenant_id(),
            parent_run_id: parent.run_id(),
            parent_activation_digest: activation_digest,
            child_slot: &slot,
        })
        .map_err(|_| ChildRunKeyError::CanonicalSerialization)?;
        let mut preimage = Vec::with_capacity(KEY_DOMAIN.len() + canonical.len());
        preimage.extend_from_slice(KEY_DOMAIN);
        preimage.extend_from_slice(&canonical);
        Ok(Self {
            parent,
            slot,
            digest: Digest::sha256(preimage),
        })
    }

    /// Returns the exact parent logical activation, without physical authority.
    #[must_use]
    pub const fn parent(&self) -> &NodeActivation {
        &self.parent
    }

    /// Returns the declared child slot.
    #[must_use]
    pub const fn slot(&self) -> &ChildRunSlot {
        &self.slot
    }

    /// Returns the tenant inherited from the parent activation.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        self.parent.tenant_id()
    }

    /// Returns the parent Run ID, never a generated child ID.
    #[must_use]
    pub const fn parent_run_id(&self) -> RunId {
        self.parent.run_id()
    }

    /// Returns the domain-separated SHA-256 ownership fingerprint.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for ChildRunKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            parent: NodeActivation,
            slot: ChildRunSlot,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        let key = Self::new(wire.parent, wire.slot).map_err(de::Error::custom)?;
        if key.digest != wire.digest {
            return Err(de::Error::custom(ChildRunKeyError::DigestMismatch));
        }
        Ok(key)
    }
}

/// Failure constructing or restoring a logical child ownership key.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ChildRunKeyError {
    /// The closed integrity preimage could not be encoded canonically.
    #[error("child run key canonical serialization failed")]
    CanonicalSerialization,
    /// Stored identity fields no longer match their integrity fingerprint.
    #[error("child run key digest mismatch")]
    DigestMismatch,
}

#[cfg(test)]
#[path = "child_run_tests.rs"]
mod tests;
