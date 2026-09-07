// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Immutable delegation declarations. Live ownership still needs a transaction.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    AgentDescriptor, CapabilityIdentity, ChildRunAdmissionIntent, ChildRunSlot, CompiledGraph,
    Digest, GraphReference, NodeId, SchemaReference,
};

/// Exact Agent definition pin, including instructions, tools, model and limits.
///
/// Identity alone cannot detect different definitions published under the same
/// owner/name/version. This domain-separated digest includes the complete
/// canonical descriptor, without copying its private content into the parent.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildAgentReference {
    identity: CapabilityIdentity,
    definition_digest: Digest,
}

impl ChildAgentReference {
    /// Pins a complete trusted descriptor using strict interoperable JSON.
    pub fn from_descriptor(descriptor: &AgentDescriptor) -> Result<Self, ChildRunPolicyError> {
        let canonical = crate::agent_admission::canonical_bytes(descriptor)
            .map_err(|_| ChildRunPolicyError::AgentEncoding)?;
        let mut bytes = b"stateknot.child-agent-definition.v1\0".to_vec();
        bytes.extend_from_slice(&canonical);
        Ok(Self {
            identity: descriptor.metadata().identity().clone(),
            definition_digest: Digest::sha256(bytes),
        })
    }

    /// Returns the exact owner-qualified Agent version.
    #[must_use]
    pub const fn identity(&self) -> &CapabilityIdentity {
        &self.identity
    }

    /// Returns the complete descriptor fingerprint, not an authorization grant.
    #[must_use]
    pub const fn definition_digest(&self) -> Digest {
        self.definition_digest
    }
}

/// One node-owned, named delegation slot with pinned Agent and graph contracts.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChildRunDeclaration {
    node_id: NodeId,
    slot: ChildRunSlot,
    agent: ChildAgentReference,
    graph: GraphReference,
    input_schema: SchemaReference,
    output_schema: SchemaReference,
}

impl ChildRunDeclaration {
    /// Declares exactly one target. Schema pins must match its compiled graph.
    pub fn new(
        node_id: NodeId,
        slot: ChildRunSlot,
        agent: &AgentDescriptor,
        graph: &CompiledGraph,
    ) -> Result<Self, ChildRunPolicyError> {
        if agent.input_schema() != graph.input_schema()
            || agent.output_schema() != graph.output_schema()
        {
            return Err(ChildRunPolicyError::TargetSchemaMismatch);
        }
        Ok(Self {
            node_id,
            slot,
            agent: ChildAgentReference::from_descriptor(agent)?,
            graph: graph.reference(),
            input_schema: agent.input_schema().clone(),
            output_schema: agent.output_schema().clone(),
        })
    }

    /// Returns the owning node in the parent graph.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the stable, case-sensitive slot identity.
    #[must_use]
    pub const fn slot(&self) -> &ChildRunSlot {
        &self.slot
    }

    /// Returns the complete Agent definition pin.
    #[must_use]
    pub const fn agent(&self) -> &ChildAgentReference {
        &self.agent
    }

    /// Returns the exact independently executable child graph.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }

    /// Returns the pinned child input schema.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaReference {
        &self.input_schema
    }

    /// Returns the pinned child terminal output schema.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaReference {
        &self.output_schema
    }

    /// Validates a locally resolved graph and strictly decreasing depth.
    ///
    /// Depth counts edges below this Run (a leaf has depth zero). Every target
    /// must fit the parent's declared remaining depth. This excludes recursive
    /// deployment cycles; it does not count live descendants or reserve budget.
    pub fn validate_target(
        &self,
        target: &CompiledGraph,
        limits: ChildRunTopologyLimits,
    ) -> Result<(), ChildRunPolicyError> {
        if &target.reference() != self.graph() {
            return Err(ChildRunPolicyError::TargetGraphMismatch);
        }
        if target.input_schema() != self.input_schema()
            || target.output_schema() != self.output_schema()
        {
            return Err(ChildRunPolicyError::TargetSchemaMismatch);
        }
        let child_depth = target
            .child_runs()
            .map_or(0, |policy| policy.limits().maximum_descendant_depth());
        if child_depth >= limits.maximum_descendant_depth() {
            return Err(ChildRunPolicyError::DepthNotNarrowed);
        }
        Ok(())
    }
}

/// Finite topology limits pinned by a parent graph, never unlimited defaults.
///
/// Children-per-activation is bounded separately by the declared slot set.
/// `maximum_children_per_run` counts lifetime immediate children, including
/// terminated children; `maximum_active_descendants` counts nonterminal Runs
/// anywhere below this Run. A store must enforce all ancestor limits atomically.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
// Wire names distinguish ceilings from future observed topology counts.
#[allow(clippy::struct_field_names)]
pub struct ChildRunTopologyLimits {
    #[schemars(range(min = 1, max = 32))]
    maximum_descendant_depth: u16,
    #[schemars(range(min = 1, max = 256))]
    maximum_children_per_run: u16,
    #[schemars(range(min = 1, max = 256))]
    maximum_active_descendants: u16,
}

impl ChildRunTopologyLimits {
    /// Hard limit for bounded ancestor traversal.
    pub const MAX_DEPTH: u16 = 32;
    /// Hard lifetime direct-child and live-descendant count bound.
    pub const MAX_CHILDREN: u16 = 256;

    /// Constructs positive ceilings within the framework limits.
    pub fn new(depth: u16, children: u16, active: u16) -> Result<Self, ChildRunPolicyError> {
        if !(1..=Self::MAX_DEPTH).contains(&depth)
            || !(1..=Self::MAX_CHILDREN).contains(&children)
            || !(1..=Self::MAX_CHILDREN).contains(&active)
        {
            return Err(ChildRunPolicyError::InvalidLimits);
        }
        Ok(Self {
            maximum_descendant_depth: depth,
            maximum_children_per_run: children,
            maximum_active_descendants: active,
        })
    }

    /// Returns maximum remaining descendant edges from this Run.
    #[must_use]
    pub const fn maximum_descendant_depth(self) -> u16 {
        self.maximum_descendant_depth
    }

    /// Returns the lifetime immediate-child ceiling, not active child count.
    #[must_use]
    pub const fn maximum_children_per_run(self) -> u16 {
        self.maximum_children_per_run
    }

    /// Returns the simultaneous nonterminal-descendant ceiling.
    #[must_use]
    pub const fn maximum_active_descendants(self) -> u16 {
        self.maximum_active_descendants
    }
}

impl<'de> Deserialize<'de> for ChildRunTopologyLimits {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(clippy::struct_field_names)]
        struct Wire {
            maximum_descendant_depth: u16,
            maximum_children_per_run: u16,
            maximum_active_descendants: u16,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.maximum_descendant_depth,
            wire.maximum_children_per_run,
            wire.maximum_active_descendants,
        )
        .map_err(de::Error::custom)
    }
}

/// Closed version-one delegation profile embedded in the parent graph digest.
///
/// Declarations are sorted by `(node_id, slot)`, which also defines future join
/// reduction order. Only cancel-and-join is supported; no detached option exists.
/// A graph without this policy delegates nothing. This profile authorizes only
/// declared target selection, not live admission or external side effects.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphChildRunPolicy {
    #[schemars(range(min = 1, max = 1))]
    version: u8,
    limits: ChildRunTopologyLimits,
    #[schemars(length(min = 1, max = 256))]
    declarations: Box<[ChildRunDeclaration]>,
}

impl GraphChildRunPolicy {
    /// Maximum declarations in one graph definition.
    pub const MAX_DECLARATIONS: usize = 256;
    /// Maximum distinct child slots for one node activation.
    pub const MAX_SLOTS_PER_NODE: usize = 64;

    /// Validates and orders bounded, duplicate-free declarations.
    pub fn new<I>(
        limits: ChildRunTopologyLimits,
        declarations: I,
    ) -> Result<Self, ChildRunPolicyError>
    where
        I: IntoIterator<Item = ChildRunDeclaration>,
    {
        let mut ordered = BTreeMap::new();
        let mut per_node = BTreeMap::new();
        for declaration in declarations {
            if ordered.len() == Self::MAX_DECLARATIONS {
                return Err(ChildRunPolicyError::TooManyDeclarations);
            }
            let count = per_node.entry(declaration.node_id.clone()).or_insert(0);
            *count += 1;
            if *count > Self::MAX_SLOTS_PER_NODE {
                return Err(ChildRunPolicyError::TooManyNodeSlots);
            }
            let key = (declaration.node_id.clone(), declaration.slot.clone());
            if ordered.insert(key, declaration).is_some() {
                return Err(ChildRunPolicyError::DuplicateSlot);
            }
        }
        if ordered.is_empty() {
            return Err(ChildRunPolicyError::EmptyDeclarations);
        }
        Ok(Self {
            version: 1,
            limits,
            declarations: ordered.into_values().collect(),
        })
    }

    /// Returns the exact finite topology contract.
    #[must_use]
    pub const fn limits(&self) -> ChildRunTopologyLimits {
        self.limits
    }

    /// Returns declarations in canonical node/slot order.
    #[must_use]
    pub const fn declarations(&self) -> &[ChildRunDeclaration] {
        &self.declarations
    }

    /// Resolves one node-owned slot. Another node's equal slot name is distinct.
    #[must_use]
    pub fn declaration(&self, node: &NodeId, slot: &ChildRunSlot) -> Option<&ChildRunDeclaration> {
        self.declarations
            .binary_search_by(|value| (&value.node_id, &value.slot).cmp(&(node, slot)))
            .ok()
            .map(|index| &self.declarations[index])
    }

    pub(crate) fn validate_nodes(&self, graph: &CompiledGraph) -> Result<(), ChildRunPolicyError> {
        if self
            .declarations
            .iter()
            .any(|item| graph.node(item.node_id()).is_none())
        {
            return Err(ChildRunPolicyError::UndeclaredParentNode);
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for GraphChildRunPolicy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            version: u8,
            limits: ChildRunTopologyLimits,
            #[serde(deserialize_with = "bounded_declarations")]
            declarations: Vec<ChildRunDeclaration>,
        }
        let wire = Wire::deserialize(deserializer)?;
        if wire.version != 1 {
            return Err(de::Error::custom(ChildRunPolicyError::UnsupportedVersion));
        }
        Self::new(wire.limits, wire.declarations).map_err(de::Error::custom)
    }
}

fn bounded_declarations<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ChildRunDeclaration>, D::Error> {
    struct Visitor;
    impl<'de> de::Visitor<'de> for Visitor {
        type Value = Vec<ChildRunDeclaration>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("at most 256 child declarations")
        }

        fn visit_seq<A: de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let maximum = GraphChildRunPolicy::MAX_DECLARATIONS;
            if sequence.size_hint().is_some_and(|size| size > maximum) {
                return Err(de::Error::custom(ChildRunPolicyError::TooManyDeclarations));
            }
            let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(maximum));
            while let Some(value) = sequence.next_element()? {
                if values.len() == maximum {
                    return Err(de::Error::custom(ChildRunPolicyError::TooManyDeclarations));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Visitor)
}

impl ChildRunAdmissionIntent {
    /// Checks target selection against the exact trusted parent graph policy.
    ///
    /// This is independent of `validate_for`: callers must perform both checks
    /// using authoritative graph/admission/checkpoint snapshots. Neither is an
    /// atomic budget reservation, a fence check, or an external authority grant.
    pub fn validate_declaration(&self, parent: &CompiledGraph) -> Result<(), ChildRunPolicyError> {
        if self.key().parent().base_checkpoint().graph() != &parent.reference() {
            return Err(ChildRunPolicyError::ParentGraphMismatch);
        }
        let policy = parent
            .child_runs()
            .ok_or(ChildRunPolicyError::DelegationNotDeclared)?;
        let declaration = policy
            .declaration(self.key().parent().node_id(), self.key().slot())
            .ok_or(ChildRunPolicyError::DelegationNotDeclared)?;
        declaration.validate_target(self.child_graph(), policy.limits())?;
        if declaration.agent() != &ChildAgentReference::from_descriptor(self.child().descriptor())?
        {
            return Err(ChildRunPolicyError::TargetAgentMismatch);
        }
        Ok(())
    }
}

/// Public-safe declaration/profile validation failure; no request data is exposed.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ChildRunPolicyError {
    /// Complete descriptor could not be encoded as interoperable canonical JSON.
    #[error("child Agent definition encoding failed")]
    AgentEncoding,
    /// A limit is zero or exceeds its fixed resource ceiling.
    #[error("invalid child topology limits")]
    InvalidLimits,
    /// An empty policy is not a delegation declaration.
    #[error("child policy must declare at least one slot")]
    EmptyDeclarations,
    /// Aggregate graph declarations exceeded their fixed bound.
    #[error("too many graph child declarations")]
    TooManyDeclarations,
    /// One activation could select too many distinct slots.
    #[error("too many child slots on one node")]
    TooManyNodeSlots,
    /// A node/slot identity was repeated.
    #[error("duplicate child slot on one node")]
    DuplicateSlot,
    /// Version is not supported and must not be treated as the current profile.
    #[error("unsupported child policy version")]
    UnsupportedVersion,
    /// A declaration refers to a node missing from its parent graph.
    #[error("child declaration references an unknown parent node")]
    UndeclaredParentNode,
    /// The trusted graph is not the graph pinned by the parent activation.
    #[error("child declaration parent graph mismatch")]
    ParentGraphMismatch,
    /// There is no delegation policy or no slot owned by this parent node.
    #[error("child delegation is not declared by the parent node")]
    DelegationNotDeclared,
    /// Target identity, version, graph bytes, or state schema changed.
    #[error("declared child graph mismatch")]
    TargetGraphMismatch,
    /// Target input/output schema pins differ.
    #[error("declared child input or output schema mismatch")]
    TargetSchemaMismatch,
    /// The full target Agent descriptor differs, even under an equal identity.
    #[error("declared child Agent definition mismatch")]
    TargetAgentMismatch,
    /// A nested target could exceed the parent's remaining descendant depth.
    #[error("declared child depth does not narrow the parent depth")]
    DepthNotNarrowed,
}

#[cfg(test)]
#[path = "child_run_policy_tests.rs"]
mod tests;
