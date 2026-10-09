// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Experimental RFC-0022 declarations bound into compiled graph definitions.
use std::{collections::BTreeMap, fmt};

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{CompiledGraph, GraphFrameIdentity, GraphReference, NodeId, RouteId, SchemaReference};

/// One framework-owned, isolated-state call site with a fixed return route.
///
/// This pins data, not executable authority. The immutable runtime registry must
/// resolve and validate the target before admission, and no application executor
/// may be installed for this node. RFC-0022 remains experimental.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFrameCall {
    node_id: NodeId,
    slot: NodeId,
    target: GraphReference,
    input_schema: SchemaReference,
    output_schema: SchemaReference,
    return_route: RouteId,
}
impl GraphFrameCall {
    /// Pins a compiled target without an implicit state/input adapter.
    ///
    /// # Errors
    ///
    /// Rejects a target whose input and state schemas are not exactly equal.
    pub fn new(
        node_id: NodeId,
        slot: NodeId,
        target: &CompiledGraph,
        return_route: RouteId,
    ) -> Result<Self, GraphFrameCompileError> {
        Self::from_pins(
            node_id,
            slot,
            target.reference(),
            target.input_schema().clone(),
            target.output_schema().clone(),
            return_route,
        )
    }
    fn from_pins(
        node_id: NodeId,
        slot: NodeId,
        target: GraphReference,
        input_schema: SchemaReference,
        output_schema: SchemaReference,
        return_route: RouteId,
    ) -> Result<Self, GraphFrameCompileError> {
        if target.state_schema() != &input_schema {
            return Err(GraphFrameCompileError::TargetInputStateMismatch);
        }
        Ok(Self {
            node_id,
            slot,
            target,
            input_schema,
            output_schema,
            return_route,
        })
    }
    /// Returns the framework-owned caller node.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    /// Returns the stable logical child slot.
    #[must_use]
    pub const fn slot(&self) -> &NodeId {
        &self.slot
    }
    /// Returns the complete pinned target graph reference.
    #[must_use]
    pub const fn target(&self) -> &GraphReference {
        &self.target
    }
    /// Returns the child input/state schema required at frame entry.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaReference {
        &self.input_schema
    }
    /// Returns the output schema required at the parent update boundary.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaReference {
        &self.output_schema
    }
    /// Returns the single declared parent return route.
    #[must_use]
    pub const fn return_route(&self) -> &RouteId {
        &self.return_route
    }
    /// Validates the actual compiled target resolved by the immutable registry.
    ///
    /// # Errors
    ///
    /// Rejects any definition, identity, input/state or output schema substitution.
    pub fn validate_target(&self, target: &CompiledGraph) -> Result<(), GraphFrameCompileError> {
        if self.target != target.reference()
            || self.input_schema != *target.input_schema()
            || self.output_schema != *target.output_schema()
        {
            return Err(GraphFrameCompileError::TargetPinMismatch);
        }
        Ok(())
    }
}
impl<'de> Deserialize<'de> for GraphFrameCall {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            node_id: NodeId,
            slot: NodeId,
            target: GraphReference,
            input_schema: SchemaReference,
            output_schema: SchemaReference,
            return_route: RouteId,
        }
        let wire = crate::json::deserialize_object::<Wire, _>(deserializer)?;
        Self::from_pins(
            wire.node_id,
            wire.slot,
            wire.target,
            wire.input_schema,
            wire.output_schema,
            wire.return_route,
        )
        .map_err(de::Error::custom)
    }
}

/// Finite, canonically ordered same-Run frame declarations for one graph.
///
/// Graphs with this policy require parallelism one. Limits are relative to this
/// caller and must be intersected with inherited Run limits; neither a new frame
/// nor a physical retry grants another budget. Declaration data alone does not
/// enable nested execution while RFC-0022 is Draft.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphFrameCallPolicy {
    #[schemars(range(min = 1, max = 1))]
    version: u8,
    #[schemars(range(min = 1, max = 7))]
    maximum_depth: u8,
    #[schemars(range(min = 1, max = 4096))]
    maximum_frame_starts: u16,
    #[schemars(length(min = 1, max = 1024))]
    calls: Box<[GraphFrameCall]>,
}
impl GraphFrameCallPolicy {
    /// Lifetime frame-start ceiling; retries reuse the original logical frame.
    pub const MAX_FRAME_STARTS: u16 = 4096;
    /// At most one framework call declaration per bounded graph node.
    pub const MAX_CALLS: usize = CompiledGraph::MAX_NODES;
    /// Validates finite limits and orders distinct call sites by node identity.
    ///
    /// # Errors
    ///
    /// Rejects empty, repeated or excessive call sites and invalid finite limits.
    pub fn new(
        maximum_depth: u8,
        maximum_frame_starts: u16,
        calls: impl IntoIterator<Item = GraphFrameCall>,
    ) -> Result<Self, GraphFrameCompileError> {
        if maximum_depth == 0
            || usize::from(maximum_depth) > GraphFrameIdentity::MAX_DEPTH
            || maximum_frame_starts == 0
            || maximum_frame_starts > Self::MAX_FRAME_STARTS
        {
            return Err(GraphFrameCompileError::InvalidLimits);
        }
        let mut ordered = BTreeMap::new();
        for call in calls {
            if ordered.len() == Self::MAX_CALLS {
                return Err(GraphFrameCompileError::TooManyCalls);
            }
            if ordered.insert(call.node_id.clone(), call).is_some() {
                return Err(GraphFrameCompileError::DuplicateNode);
            }
        }
        if ordered.is_empty() {
            return Err(GraphFrameCompileError::EmptyCalls);
        }
        Ok(Self {
            version: 1,
            maximum_depth,
            maximum_frame_starts,
            calls: ordered.into_values().collect(),
        })
    }
    /// Returns the maximum remaining nested edges allowed from this graph.
    #[must_use]
    pub const fn maximum_depth(&self) -> u8 {
        self.maximum_depth
    }
    /// Returns the graph's finite inherited Run frame-start ceiling.
    #[must_use]
    pub const fn maximum_frame_starts(&self) -> u16 {
        self.maximum_frame_starts
    }
    /// Returns canonically ordered framework call sites.
    #[must_use]
    pub const fn calls(&self) -> &[GraphFrameCall] {
        &self.calls
    }
    /// Resolves one exact framework-owned caller node.
    #[must_use]
    pub fn call(&self, node: &NodeId) -> Option<&GraphFrameCall> {
        self.calls
            .binary_search_by(|call| call.node_id.cmp(node))
            .ok()
            .map(|index| &self.calls[index])
    }
    pub(crate) fn validate_parent(
        &self,
        graph: &CompiledGraph,
    ) -> Result<(), GraphFrameCompileError> {
        if graph.limits().maximum_parallelism() != 1 {
            return Err(GraphFrameCompileError::ParallelCaller);
        }
        for call in &self.calls {
            if graph.identity().owner() != call.target.identity().owner() {
                return Err(GraphFrameCompileError::OwnerMismatch);
            }
            if graph.state_schema() != &call.input_schema
                || graph.update_schema() != &call.output_schema
            {
                return Err(GraphFrameCompileError::ParentSchemaMismatch);
            }
            let node = graph
                .node(&call.node_id)
                .ok_or(GraphFrameCompileError::MissingCaller)?;
            if node.continue_to().is_some()
                || node.wait_to().is_some()
                || node.allows_terminal()
                || node.routes().len() != 1
                || !node
                    .routes()
                    .iter()
                    .any(|route| route.route_id() == call.return_route())
            {
                return Err(GraphFrameCompileError::ReturnRouteMismatch);
            }
            if graph.child_runs().is_some_and(|policy| {
                policy
                    .declarations()
                    .iter()
                    .any(|declaration| declaration.node_id() == call.node_id())
            }) {
                return Err(GraphFrameCompileError::ChildRunCallerConflict);
            }
        }
        Ok(())
    }
}
impl<'de> Deserialize<'de> for GraphFrameCallPolicy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            version: u8,
            maximum_depth: u8,
            maximum_frame_starts: u16,
            #[serde(deserialize_with = "bounded_calls")]
            calls: Vec<GraphFrameCall>,
        }
        let wire = crate::json::deserialize_object::<Wire, _>(deserializer)?;
        if wire.version != 1 {
            return Err(de::Error::custom(
                GraphFrameCompileError::UnsupportedVersion,
            ));
        }
        if wire
            .calls
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
        {
            return Err(de::Error::custom(GraphFrameCompileError::NoncanonicalOrder));
        }
        Self::new(wire.maximum_depth, wire.maximum_frame_starts, wire.calls)
            .map_err(de::Error::custom)
    }
}
fn bounded_calls<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<GraphFrameCall>, D::Error> {
    struct Visitor;
    impl<'de> de::Visitor<'de> for Visitor {
        type Value = Vec<GraphFrameCall>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("at most 1024 graph frame call declarations")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut values = Vec::with_capacity(
                sequence
                    .size_hint()
                    .unwrap_or(0)
                    .min(GraphFrameCallPolicy::MAX_CALLS),
            );
            while let Some(call) = sequence.next_element()? {
                if values.len() == GraphFrameCallPolicy::MAX_CALLS {
                    return Err(de::Error::custom(GraphFrameCompileError::TooManyCalls));
                }
                values.push(call);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Visitor)
}

/// Public-safe validation failure for experimental graph frame declarations.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphFrameCompileError {
    /// Depth/start limits were zero or exceeded immutable ceilings.
    #[error("graph frame limits must be finite and within framework ceilings")]
    InvalidLimits,
    /// The policy contained no declared call.
    #[error("graph frame call policy must contain a call")]
    EmptyCalls,
    /// Declaration count exceeded the graph's bounded node count.
    #[error("graph frame call policy contains too many calls")]
    TooManyCalls,
    /// Two declarations tried to own the same caller node.
    #[error("graph frame call policy repeats a caller node")]
    DuplicateNode,
    /// A serialized policy did not use canonical caller order.
    #[error("graph frame calls must use canonical node order")]
    NoncanonicalOrder,
    /// A serialized policy named another wire version.
    #[error("unsupported graph frame call policy version")]
    UnsupportedVersion,
    /// The isolated-state profile has no implicit input/state adapter.
    #[error("graph frame target input and state schema pins differ")]
    TargetInputStateMismatch,
    /// An installed target differed from the declaration's exact immutable pins.
    #[error("graph frame target definition or schema pins differ")]
    TargetPinMismatch,
    /// A caller attempted to select another principal's implementation.
    #[error("graph frame target has another owner")]
    OwnerMismatch,
    /// Parent state/update did not match child input/output respectively.
    #[error("graph frame parent and target schema pins are incompatible")]
    ParentSchemaMismatch,
    /// A declared framework caller was absent from the graph.
    #[error("graph frame caller node is absent")]
    MissingCaller,
    /// A caller had controls other than its single fixed return route.
    #[error("graph frame caller must declare only its exact return route")]
    ReturnRouteMismatch,
    /// The initial profile cannot suspend a parallel caller.
    #[error("graph frame caller graph must have parallelism one")]
    ParallelCaller,
    /// Framework calls cannot also admit independent child Runs at that node.
    #[error("graph frame caller cannot declare child-Run delegation")]
    ChildRunCallerConflict,
}
