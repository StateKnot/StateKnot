// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Dedicated child Join identity and terminal evidence, independent of timers.

use crate::{ChildRunBudgetSettlement, ChildRunKey, Digest, JournalHead, NodeActivation};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, de};
use std::{collections::BTreeSet, fmt};
use thiserror::Error;

/// One sealed, nonempty set of owned children for one logical parent activation.
/// The store must prove ownership and require the complete admitted set before
/// registering it. Construction alone grants no authority and performs no I/O.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunJoinRequest {
    #[schemars(schema_with = "version_schema")]
    version: u8,
    #[schemars(schema_with = "keys_schema")]
    keys: Vec<ChildRunKey>,
    activation_digest: Digest,
    digest: Digest,
}

impl ChildRunJoinRequest {
    /// Same bound as the maximum number of declared slots on one node.
    pub const MAX_CHILDREN: usize = 64;
    /// Hard canonical wire ceiling; child inputs and outputs are not included.
    pub const MAX_BYTES: usize = 4 * 1024 * 1024;

    /// Seals keys in case-sensitive slot order, independent of completion order.
    pub fn new(keys: impl IntoIterator<Item = ChildRunKey>) -> Result<Self, ChildRunJoinError> {
        let mut values = Vec::new();
        for key in keys {
            if values.len() == Self::MAX_CHILDREN {
                return Err(ChildRunJoinError::Bounds);
            }
            values.push(key);
        }
        let first = values.first().ok_or(ChildRunJoinError::Bounds)?;
        if values.iter().any(|key| key.parent() != first.parent()) {
            return Err(ChildRunJoinError::Scope);
        }
        let activation_digest = crate::node_attempt::compute_activation_digest(first.parent())
            .map_err(|_| ChildRunJoinError::Encoding)?;
        values.sort_by(|left, right| left.slot().cmp(right.slot()));
        if values
            .windows(2)
            .any(|pair| pair[0].slot() == pair[1].slot())
        {
            return Err(ChildRunJoinError::Duplicate);
        }
        let digest = checksum(
            "stateknot.child-join-request.v1",
            &(1_u8, &values, activation_digest),
        )?;
        let result = Self {
            version: 1,
            keys: values,
            activation_digest,
            digest,
        };
        result.canonical_bytes()?;
        Ok(result)
    }
    /// Returns canonical slot order. Identity is physical-attempt independent.
    #[must_use]
    pub fn keys(&self) -> &[ChildRunKey] {
        &self.keys
    }
    /// Returns the common exact parent activation.
    #[must_use]
    pub fn activation(&self) -> &NodeActivation {
        self.keys[0].parent()
    }
    /// Returns the existing canonical node activation digest.
    #[must_use]
    pub const fn activation_digest(&self) -> Digest {
        self.activation_digest
    }
    /// Returns the sealed request identity.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Encodes the bounded versioned request without storage coordinates.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ChildRunJoinError> {
        canonical(self)
    }
}

impl<'de> Deserialize<'de> for ChildRunJoinRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            version: u8,
            #[serde(deserialize_with = "bounded_values")]
            keys: Vec<ChildRunKey>,
            activation_digest: Digest,
            digest: Digest,
        }
        let wire = Wire::deserialize(deserializer)?;
        if wire.version != 1
            || wire
                .keys
                .windows(2)
                .any(|pair| pair[0].slot() >= pair[1].slot())
        {
            return Err(de::Error::custom(ChildRunJoinError::Noncanonical));
        }
        let result = Self::new(wire.keys).map_err(de::Error::custom)?;
        if result.digest != wire.digest || result.activation_digest != wire.activation_digest {
            return Err(de::Error::custom(ChildRunJoinError::DigestMismatch));
        }
        Ok(result)
    }
}

/// Immutable complete, priced terminal evidence in the request's declared order.
/// Each position corresponds to the same-position ownership key. A store must
/// revalidate the actual owned admission and exact terminal journal for every
/// position; a matching checksum is not authentication or proof of execution.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunJoinBinding {
    request: ChildRunJoinRequest,
    #[schemars(schema_with = "terminals_schema")]
    terminals: Vec<ChildRunBudgetSettlement>,
    digest: Digest,
}
impl ChildRunJoinBinding {
    /// Constructs a complete binding, rejecting duplicates, crossed tenants or missing children.
    pub fn new(
        request: ChildRunJoinRequest,
        terminals: impl IntoIterator<Item = ChildRunBudgetSettlement>,
    ) -> Result<Self, ChildRunJoinError> {
        let mut values = Vec::new();
        let mut children = BTreeSet::new();
        for value in terminals {
            if values.len() == ChildRunJoinRequest::MAX_CHILDREN {
                return Err(ChildRunJoinError::Bounds);
            }
            if value.terminal().tenant_id() != request.activation().tenant_id()
                || value.terminal().run_id() == request.activation().run_id()
            {
                return Err(ChildRunJoinError::Scope);
            }
            if !children.insert(value.terminal().run_id()) {
                return Err(ChildRunJoinError::Duplicate);
            }
            values.push(value);
        }
        if values.len() != request.keys.len() {
            return Err(ChildRunJoinError::Bounds);
        }
        let digest = checksum(
            "stateknot.child-join-binding.v1",
            &(request.digest(), &values),
        )?;
        let result = Self {
            request,
            terminals: values,
            digest,
        };
        result.canonical_bytes()?;
        Ok(result)
    }
    /// Returns the original sealed request.
    #[must_use]
    pub const fn request(&self) -> &ChildRunJoinRequest {
        &self.request
    }
    /// Returns one complete terminal observation per request key, in slot order.
    #[must_use]
    pub fn terminals(&self) -> &[ChildRunBudgetSettlement] {
        &self.terminals
    }
    /// Returns the version-domain-separated evidence digest.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
    /// Returns bounded canonical evidence without copying child output payloads.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ChildRunJoinError> {
        canonical(self)
    }
    /// Anchors the immutable binding to a parent publication event.
    pub fn head(&self, journal: JournalHead) -> Result<ChildRunJoinHead, ChildRunJoinError> {
        if self
            .terminals
            .iter()
            .any(|value| value.terminal().recorded_at() > journal.recorded_at())
        {
            return Err(ChildRunJoinError::Clock);
        }
        ChildRunJoinHead::restore(
            self.request.activation().clone(),
            self.request.digest(),
            self.digest,
            journal,
        )
    }
}
impl<'de> Deserialize<'de> for ChildRunJoinBinding {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            request: ChildRunJoinRequest,
            #[serde(deserialize_with = "bounded_values")]
            terminals: Vec<ChildRunBudgetSettlement>,
            digest: Digest,
        }
        let wire = Wire::deserialize(deserializer)?;
        let result = Self::new(wire.request, wire.terminals).map_err(de::Error::custom)?;
        if result.digest != wire.digest {
            return Err(de::Error::custom(ChildRunJoinError::DigestMismatch));
        }
        Ok(result)
    }
}

/// Compact Join evidence consumed by a parent pending result. The parent event
/// is not a child terminal event: cross-Run journal sequences are never compared.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunJoinHead {
    activation: NodeActivation,
    request_digest: Digest,
    binding_digest: Digest,
    journal_head: JournalHead,
}
impl ChildRunJoinHead {
    fn restore(
        activation: NodeActivation,
        request_digest: Digest,
        binding_digest: Digest,
        journal_head: JournalHead,
    ) -> Result<Self, ChildRunJoinError> {
        if journal_head.tenant_id() != activation.tenant_id()
            || journal_head.run_id() != activation.run_id()
        {
            return Err(ChildRunJoinError::Scope);
        }
        let base = activation.base_checkpoint().journal_head();
        if journal_head.sequence() <= base.sequence()
            || journal_head.recorded_at() < base.recorded_at()
        {
            return Err(ChildRunJoinError::Clock);
        }
        Ok(Self {
            activation,
            request_digest,
            binding_digest,
            journal_head,
        })
    }
    /// Returns the consuming parent activation.
    #[must_use]
    pub const fn activation(&self) -> &NodeActivation {
        &self.activation
    }
    /// Returns the original sealed request identity.
    #[must_use]
    pub const fn request_digest(&self) -> Digest {
        self.request_digest
    }
    /// Returns the complete terminal binding digest.
    #[must_use]
    pub const fn binding_digest(&self) -> Digest {
        self.binding_digest
    }
    /// Returns the exact parent publication event.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }
}
impl<'de> Deserialize<'de> for ChildRunJoinHead {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            activation: NodeActivation,
            request_digest: Digest,
            binding_digest: Digest,
            journal_head: JournalHead,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.activation,
            wire.request_digest,
            wire.binding_digest,
            wire.journal_head,
        )
        .map_err(de::Error::custom)
    }
}

fn canonical<T: Serialize>(value: &T) -> Result<Vec<u8>, ChildRunJoinError> {
    let bytes = serde_json_canonicalizer::to_vec(value).map_err(|_| ChildRunJoinError::Encoding)?;
    if bytes.len() > ChildRunJoinRequest::MAX_BYTES {
        return Err(ChildRunJoinError::Bounds);
    }
    Ok(bytes)
}

fn version_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({"type":"integer","const":1})
}
fn keys_schema(generator: &mut SchemaGenerator) -> Schema {
    json_schema!({"type":"array","minItems":1,"maxItems":64,
        "items":generator.subschema_for::<ChildRunKey>()})
}
fn terminals_schema(generator: &mut SchemaGenerator) -> Schema {
    json_schema!({"type":"array","minItems":1,"maxItems":64,
        "items":generator.subschema_for::<ChildRunBudgetSettlement>()})
}
fn checksum<T: Serialize>(domain: &str, value: &T) -> Result<Digest, ChildRunJoinError> {
    let mut bytes = domain.as_bytes().to_vec();
    bytes.push(0);
    bytes.extend(canonical(value)?);
    Ok(Digest::sha256(bytes))
}
fn bounded_values<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Vec<T>, D::Error> {
    struct Visitor<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> de::Visitor<'de> for Visitor<T> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("at most 64 child Join values")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element()? {
                if values.len() == ChildRunJoinRequest::MAX_CHILDREN {
                    return Err(de::Error::custom(ChildRunJoinError::Bounds));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Visitor(std::marker::PhantomData))
}

/// Closed validation failures, without child inputs, outputs or diagnostics.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ChildRunJoinError {
    /// Empty, oversized or incomplete set.
    #[error("child Join bounds exceeded or incomplete")]
    Bounds,
    /// Crossed activation, Run or tenant.
    #[error("child Join scope mismatch")]
    Scope,
    /// A child or declared slot occurred more than once.
    #[error("duplicate child Join member")]
    Duplicate,
    /// Unsupported version or noncanonical ordering.
    #[error("noncanonical child Join encoding")]
    Noncanonical,
    /// Restored integrity evidence changed.
    #[error("child Join digest mismatch")]
    DigestMismatch,
    /// Publication precedes its required evidence.
    #[error("child Join publication ordering mismatch")]
    Clock,
    /// Canonical serialization failed.
    #[error("child Join encoding failed")]
    Encoding,
}
