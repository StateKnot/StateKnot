// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable, integrity-bound state machine for logical tool invocations.
//!
//! A logical invocation is anchored to one graph-node activation and keeps the
//! same [`InvocationId`] across physical attempts. Preparation and every state
//! transition are committed with an exact run-journal head. The executor must
//! never perform an external call while holding the store transaction: it first
//! commits `prepared`, claims an `executing` attempt, performs the call, then
//! commits its result or public-safe failure under the current run fence.

use std::{fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::decimal::{UnsignedDecimalError, parse_bounded_u64};
use crate::{
    AttemptId, ByteCount, Checkpoint, CheckpointHead, Digest, ExecutionCount, InvocationId,
    JournalHead, JournalSequence, NodeId, RetryAdvice, RunId, TenantId, Timestamp, ToolDescriptor,
    ToolError, ToolExecutionLimits, ToolExternalEffect, ToolInput, ToolResult, ToolRisk,
};

const MAX_DATABASE_ORDINAL: u64 = i64::MAX as u64;
const INVOCATION_REVISION_PATTERN: &str = "^(0|[1-9][0-9]{0,18})$";
const GRAPH_NAMESPACE_PATTERN: &str =
    "^(?:[A-Za-z0-9][A-Za-z0-9_.-]{0,127}(?:/[A-Za-z0-9][A-Za-z0-9_.-]{0,127})*)?$";
const READY_NODE_INPUT_DIGEST_DOMAIN: &[u8] = b"stateknot-ready-node-input-v1\0";
const INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-tool-invocation-intent-v1\0";
const TRANSITION_DIGEST_DOMAIN: &[u8] = b"stateknot-tool-invocation-transition-v1\0";
const RECORD_DIGEST_DOMAIN: &[u8] = b"stateknot-tool-invocation-record-v1\0";

/// Slash-separated namespace of one node inside a root or nested graph.
///
/// The empty string identifies the root graph. Non-root segments use the same
/// bounded grammar as [`NodeId`], which makes the canonical representation safe
/// for compound database keys without permitting path traversal components.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GraphNamespace(Box<str>);

impl GraphNamespace {
    /// Maximum encoded namespace length in bytes.
    pub const MAX_LEN: usize = 512;

    /// Constructs the root graph namespace.
    #[must_use]
    pub fn root() -> Self {
        Self(Box::from(""))
    }

    /// Validates and constructs a graph namespace.
    ///
    /// # Errors
    ///
    /// Returns [`GraphNamespaceError`] for an oversized namespace or an empty,
    /// path-like, or otherwise invalid non-root segment.
    pub fn new(value: impl Into<String>) -> Result<Self, GraphNamespaceError> {
        let value = value.into();
        if value.len() > Self::MAX_LEN {
            return Err(GraphNamespaceError::TooLong {
                maximum: Self::MAX_LEN,
                actual: value.len(),
            });
        }
        if value.is_empty() {
            return Ok(Self::root());
        }
        for (index, segment) in value.split('/').enumerate() {
            NodeId::new(segment)
                .map_err(|source| GraphNamespaceError::InvalidSegment { index, source })?;
        }
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the canonical namespace text, empty for the root graph.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this is the root graph namespace.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }
}

impl Default for GraphNamespace {
    fn default() -> Self {
        Self::root()
    }
}

impl AsRef<str> for GraphNamespace {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for GraphNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("GraphNamespace")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for GraphNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for GraphNamespace {
    type Err = GraphNamespaceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for GraphNamespace {
    type Error = GraphNamespaceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for GraphNamespace {
    type Error = GraphNamespaceError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for GraphNamespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for GraphNamespace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(GraphNamespaceVisitor)
    }
}

struct GraphNamespaceVisitor;

impl de::Visitor<'_> for GraphNamespaceVisitor {
    type Value = GraphNamespace;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a root or slash-separated bounded graph namespace")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        GraphNamespace::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        GraphNamespace::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for GraphNamespace {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "GraphNamespace".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::GraphNamespace").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "maxLength": 512,
            "pattern": GRAPH_NAMESPACE_PATTERN,
            "description": "The empty string is the root graph. Runtime additionally rejects '.' and '..' segments."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid graph namespace.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GraphNamespaceError {
    /// The encoded namespace exceeded its hard wire and index ceiling.
    #[error("graph namespace is {actual} bytes; maximum is {maximum}")]
    TooLong {
        /// Maximum supported bytes.
        maximum: usize,
        /// Observed bytes.
        actual: usize,
    },
    /// One non-root path segment was not a valid node identity.
    #[error("graph namespace segment {index} is invalid: {source}")]
    InvalidSegment {
        /// Zero-based segment position.
        index: usize,
        /// Exact node-identity validation failure.
        #[source]
        source: crate::NodeIdError,
    },
}

/// Monotonic zero-based revision of one logical invocation record.
///
/// The decimal-string wire form preserves exact values across languages and
/// the maximum matches a signed `PostgreSQL BIGINT`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ToolInvocationRevision(u64);

impl ToolInvocationRevision {
    /// Preparation record revision.
    pub const INITIAL: Self = Self(0);
    /// Largest revision supported by the v1 storage contract.
    pub const MAX: Self = Self(MAX_DATABASE_ORDINAL);

    /// Constructs a storage-compatible revision.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationRevisionError::AboveMaximum`] above signed
    /// `BIGINT`.
    pub const fn new(value: u64) -> Result<Self, ToolInvocationRevisionError> {
        if value > MAX_DATABASE_ORDINAL {
            return Err(ToolInvocationRevisionError::AboveMaximum);
        }
        Ok(Self(value))
    }

    /// Returns the integer revision.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the exact next revision or `None` at the storage ceiling.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        if self.0 == MAX_DATABASE_ORDINAL {
            None
        } else {
            Some(Self(self.0 + 1))
        }
    }
}

impl fmt::Display for ToolInvocationRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for ToolInvocationRevision {
    type Err = ToolInvocationRevisionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = parse_bounded_u64(value, MAX_DATABASE_ORDINAL)
            .map_err(ToolInvocationRevisionError::from_decimal_error)?;
        Self::new(value)
    }
}

impl Serialize for ToolInvocationRevision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ToolInvocationRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ToolInvocationRevisionVisitor)
    }
}

struct ToolInvocationRevisionVisitor;

impl de::Visitor<'_> for ToolInvocationRevisionVisitor {
    type Value = ToolInvocationRevision;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical non-negative decimal PostgreSQL BIGINT revision")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

impl JsonSchema for ToolInvocationRevision {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ToolInvocationRevision".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ToolInvocationRevision").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 19,
            "pattern": INVOCATION_REVISION_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid canonical invocation revision.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolInvocationRevisionError {
    /// The wire value was empty or contained a non-decimal byte.
    #[error("tool invocation revision must contain only unsigned ASCII decimal digits")]
    InvalidFormat,
    /// The decimal text contained a leading zero.
    #[error("tool invocation revision must use canonical decimal text")]
    NonCanonical,
    /// The value exceeded signed `PostgreSQL BIGINT`.
    #[error("tool invocation revision exceeds the PostgreSQL BIGINT maximum")]
    AboveMaximum,
}

impl ToolInvocationRevisionError {
    const fn from_decimal_error(error: UnsignedDecimalError) -> Self {
        match error {
            UnsignedDecimalError::Empty | UnsignedDecimalError::InvalidCharacter { .. } => {
                Self::InvalidFormat
            }
            UnsignedDecimalError::LeadingZero => Self::NonCanonical,
            UnsignedDecimalError::TooLong { .. } | UnsignedDecimalError::Overflow => {
                Self::AboveMaximum
            }
        }
    }
}

/// Exact graph-node activation that owns a logical tool invocation.
///
/// `input_digest` is the scheduler-computed digest of the node's deterministic
/// activation input, not merely the tool arguments. The store must additionally
/// prove from the full base checkpoint that this node belongs to its ready set.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeActivation {
    base_checkpoint: CheckpointHead,
    graph_namespace: GraphNamespace,
    node_id: NodeId,
    input_digest: Digest,
}

impl NodeActivation {
    /// Constructs a node activation from already validated identities.
    #[must_use]
    pub const fn new(
        base_checkpoint: CheckpointHead,
        graph_namespace: GraphNamespace,
        node_id: NodeId,
        input_digest: Digest,
    ) -> Self {
        Self {
            base_checkpoint,
            graph_namespace,
            node_id,
            input_digest,
        }
    }

    /// Derives the canonical root-graph activation for one ready node.
    ///
    /// The logical input digest is domain-separated from every other checksum
    /// and binds the complete base-checkpoint digest, root namespace, and node
    /// identity. Replaying the same checkpoint therefore produces byte-for-byte
    /// identical activations without consulting a process clock, worker ID, or
    /// completion order.
    ///
    /// # Errors
    ///
    /// Returns [`NodeActivationError::NodeNotReady`] when `node_id` is absent
    /// from the checkpoint's exact ready set, or
    /// [`NodeActivationError::CanonicalSerialization`] if the closed digest
    /// preimage cannot be encoded canonically.
    pub fn for_ready_root(
        checkpoint: &Checkpoint,
        node_id: NodeId,
    ) -> Result<Self, NodeActivationError> {
        if !checkpoint.ready_nodes().contains(&node_id) {
            return Err(NodeActivationError::NodeNotReady { node_id });
        }
        let graph_namespace = GraphNamespace::root();
        let input_digest =
            compute_ready_node_input_digest(checkpoint.digest(), &graph_namespace, &node_id)?;
        Ok(Self::new(
            checkpoint.head(),
            graph_namespace,
            node_id,
            input_digest,
        ))
    }

    /// Returns the exact base checkpoint head.
    #[must_use]
    pub const fn base_checkpoint(&self) -> &CheckpointHead {
        &self.base_checkpoint
    }

    /// Returns the graph namespace containing the node.
    #[must_use]
    pub const fn graph_namespace(&self) -> &GraphNamespace {
        &self.graph_namespace
    }

    /// Returns the durable node identity.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the deterministic activation-input digest.
    #[must_use]
    pub const fn input_digest(&self) -> Digest {
        self.input_digest
    }

    /// Returns the tenant boundary inherited from the checkpoint.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        self.base_checkpoint.tenant_id()
    }

    /// Returns the run identity inherited from the checkpoint.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.base_checkpoint.run_id()
    }
}

/// Invalid deterministic construction of a ready-node activation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeActivationError {
    /// The requested node is absent from the checkpoint's exact ready set.
    #[error("node {node_id:?} is not ready in the base checkpoint")]
    NodeNotReady {
        /// Rejected node identity.
        node_id: NodeId,
    },
    /// The closed activation-input checksum preimage could not be canonicalized.
    #[error("ready-node activation input canonical serialization failed")]
    CanonicalSerialization,
}

/// One resolved limit field used in narrowed-limit diagnostics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ToolInvocationLimit {
    /// Attempt timeout.
    Timeout,
    /// Capability concurrency.
    MaxConcurrency,
    /// Encoded argument bytes.
    MaxInputBytes,
    /// Inline result bytes.
    MaxInlineResultBytes,
    /// Result artifact count.
    MaxArtifacts,
    /// Aggregate result artifact bytes.
    MaxTotalArtifactBytes,
}

/// Immutable preparation intent for one logical tool invocation.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvocationIntent {
    activation: NodeActivation,
    invocation_id: InvocationId,
    descriptor: ToolDescriptor,
    input: ToolInput,
    effective_limits: ToolExecutionLimits,
    intent_digest: Digest,
}

impl ToolInvocationIntent {
    /// Constructs and checksums a durable invocation intent.
    ///
    /// `effective_limits` is the already resolved intersection of system,
    /// tenant, policy, run-budget, and descriptor ceilings. It may only narrow
    /// the immutable descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationIntentError`] for schema substitution, widened
    /// limits, oversized arguments, or an integrity serialization failure.
    pub fn new(
        activation: NodeActivation,
        invocation_id: InvocationId,
        descriptor: ToolDescriptor,
        input: ToolInput,
        effective_limits: ToolExecutionLimits,
    ) -> Result<Self, ToolInvocationIntentError> {
        validate_intent_shape(&descriptor, &input, &effective_limits)?;
        let intent_digest = compute_intent_digest(&ToolInvocationIntentDigestWire {
            activation: &activation,
            invocation_id,
            descriptor: &descriptor,
            input: &input,
            effective_limits: &effective_limits,
        })?;
        Ok(Self {
            activation,
            invocation_id,
            descriptor,
            input,
            effective_limits,
            intent_digest,
        })
    }

    /// Restores an intent and verifies its invariant and checksum layers.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationIntentError`] when validation or integrity fails.
    pub fn restore(
        activation: NodeActivation,
        invocation_id: InvocationId,
        descriptor: ToolDescriptor,
        input: ToolInput,
        effective_limits: ToolExecutionLimits,
        intent_digest: Digest,
    ) -> Result<Self, ToolInvocationIntentError> {
        let restored = Self::new(
            activation,
            invocation_id,
            descriptor,
            input,
            effective_limits,
        )?;
        if restored.intent_digest != intent_digest {
            return Err(ToolInvocationIntentError::DigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the owning graph-node activation.
    #[must_use]
    pub const fn activation(&self) -> &NodeActivation {
        &self.activation
    }

    /// Returns the stable logical invocation identifier.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the exact registered tool-version snapshot.
    #[must_use]
    pub const fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    /// Returns bounded schema-pinned arguments.
    #[must_use]
    pub const fn input(&self) -> &ToolInput {
        &self.input
    }

    /// Returns the invocation's resolved effective ceilings.
    #[must_use]
    pub const fn effective_limits(&self) -> &ToolExecutionLimits {
        &self.effective_limits
    }

    /// Returns the domain-separated preparation fingerprint.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the tenant boundary inherited from the activation.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        self.activation.tenant_id()
    }

    /// Returns the run identity inherited from the activation.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.activation.run_id()
    }
}

impl fmt::Debug for ToolInvocationIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolInvocationIntent")
            .field("activation", &self.activation)
            .field("invocation_id", &self.invocation_id)
            .field("descriptor", &self.descriptor)
            .field("input", &self.input)
            .field("effective_limits", &self.effective_limits)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ToolInvocationIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            activation: NodeActivation,
            invocation_id: InvocationId,
            descriptor: ToolDescriptor,
            input: ToolInput,
            effective_limits: ToolExecutionLimits,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.activation,
            wire.invocation_id,
            wire.descriptor,
            wire.input,
            wire.effective_limits,
            wire.intent_digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid durable tool invocation intent.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolInvocationIntentError {
    /// Arguments named a schema other than the pinned descriptor schema.
    #[error("tool invocation input schema does not match the descriptor")]
    InputSchemaMismatch,
    /// A resolved limit widened rather than narrowed the descriptor ceiling.
    #[error("effective tool invocation limit {limit:?} exceeds the descriptor ceiling")]
    EffectiveLimitWidened {
        /// Widened field.
        limit: ToolInvocationLimit,
    },
    /// Compact arguments exceeded the resolved invocation limit.
    #[error("tool invocation input is {actual} bytes; effective maximum is {maximum}")]
    InputLimitExceeded {
        /// Resolved input-byte ceiling.
        maximum: ByteCount,
        /// Exact compact input size.
        actual: ByteCount,
    },
    /// Canonical integrity material could not be serialized.
    #[error("tool invocation intent integrity calculation failed: {source}")]
    Integrity {
        /// Exact integrity failure.
        #[source]
        source: ToolInvocationIntegrityError,
    },
    /// Persisted intent checksum did not match caller-controlled fields.
    #[error("tool invocation intent digest does not match its fields")]
    DigestMismatch,
}

impl From<ToolInvocationIntegrityError> for ToolInvocationIntentError {
    fn from(source: ToolInvocationIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Durable lifecycle state of one logical tool invocation.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ToolInvocationStatus {
    /// Intent is committed but no physical attempt has started.
    Prepared,
    /// Exactly one physical attempt has been durably claimed.
    Executing,
    /// A validated successful result is durable.
    Committed,
    /// A known failed outcome is durable and retry remains policy-gated.
    Failed,
    /// An external write outcome is ambiguous and must be reconciled.
    Unknown,
}

/// Integrity-bound state payload of one invocation revision.
#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolInvocationState {
    /// Prepared intent awaiting an attempt claim.
    Prepared,
    /// In-flight physical attempt.
    Executing {
        /// Unique physical attempt identifier.
        attempt_id: AttemptId,
    },
    /// Validated successful result.
    Committed {
        /// Exact erased result and provenance.
        result: ToolResult,
    },
    /// Known failed outcome.
    Failed {
        /// Public-safe failure and effect evidence.
        error: ToolError,
    },
    /// Ambiguous external effect awaiting reconciliation.
    Unknown {
        /// Public-safe ambiguous-outcome evidence.
        error: ToolError,
    },
}

impl ToolInvocationState {
    /// Returns the lifecycle discriminator.
    #[must_use]
    pub const fn status(&self) -> ToolInvocationStatus {
        match self {
            Self::Prepared => ToolInvocationStatus::Prepared,
            Self::Executing { .. } => ToolInvocationStatus::Executing,
            Self::Committed { .. } => ToolInvocationStatus::Committed,
            Self::Failed { .. } => ToolInvocationStatus::Failed,
            Self::Unknown { .. } => ToolInvocationStatus::Unknown,
        }
    }

    /// Returns the physical attempt represented by this state, if any.
    #[must_use]
    pub const fn attempt_id(&self) -> Option<AttemptId> {
        match self {
            Self::Prepared => None,
            Self::Executing { attempt_id } => Some(*attempt_id),
            Self::Committed { result } => Some(result.provenance().attempt_id()),
            Self::Failed { error } | Self::Unknown { error } => {
                Some(error.provenance().attempt_id())
            }
        }
    }
}

impl<'de> Deserialize<'de> for ToolInvocationState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Prepared,
            Executing { attempt_id: AttemptId },
            Committed { result: ToolResult },
            Failed { error: ToolError },
            Unknown { error: ToolError },
        }

        Ok(match Wire::deserialize(deserializer)? {
            Wire::Prepared => Self::Prepared,
            Wire::Executing { attempt_id } => Self::Executing { attempt_id },
            Wire::Committed { result } => Self::Committed { result },
            Wire::Failed { error } => Self::Failed { error },
            Wire::Unknown { error } => Self::Unknown { error },
        })
    }
}

/// Kind of one explicit invocation state transition.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ToolInvocationTransitionKind {
    /// Claim a new physical attempt.
    StartAttempt,
    /// Commit the executing attempt's successful result.
    RecordResult,
    /// Commit the executing attempt's failure evidence.
    RecordError,
    /// Resolve an ambiguous outcome to a successful result.
    ReconcileResult,
    /// Record reconciliation evidence without inventing a success.
    ReconcileError,
}

/// Explicit transition appended to the run journal and invocation history.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolInvocationTransition {
    /// Claim a unique physical attempt after durable preparation or safe retry.
    StartAttempt {
        /// New physical attempt identity.
        attempt_id: AttemptId,
    },
    /// Commit a successful result from the current executing attempt.
    RecordResult {
        /// Validated erased result.
        result: ToolResult,
    },
    /// Commit public-safe failure and external-effect evidence.
    RecordError {
        /// Validated attempt failure.
        error: ToolError,
    },
    /// Resolve the current unknown attempt to a successful result.
    ReconcileResult {
        /// Authoritatively recovered result of the original attempt.
        result: ToolResult,
    },
    /// Record authoritative or still-ambiguous reconciliation evidence.
    ReconcileError {
        /// Failure evidence for the original attempt.
        error: ToolError,
    },
}

impl ToolInvocationTransition {
    /// Returns the closed transition discriminator.
    #[must_use]
    pub const fn kind(&self) -> ToolInvocationTransitionKind {
        match self {
            Self::StartAttempt { .. } => ToolInvocationTransitionKind::StartAttempt,
            Self::RecordResult { .. } => ToolInvocationTransitionKind::RecordResult,
            Self::RecordError { .. } => ToolInvocationTransitionKind::RecordError,
            Self::ReconcileResult { .. } => ToolInvocationTransitionKind::ReconcileResult,
            Self::ReconcileError { .. } => ToolInvocationTransitionKind::ReconcileError,
        }
    }
}

/// Compact exact identity of a validated invocation revision.
///
/// A head is an optimistic comparison token. It intentionally omits arguments,
/// results, and errors; obtain it from [`ToolInvocation::head`] or from storage
/// that has restored and verified the corresponding full record.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvocationHead {
    tenant_id: TenantId,
    run_id: RunId,
    invocation_id: InvocationId,
    revision: ToolInvocationRevision,
    status: ToolInvocationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<AttemptId>,
    journal_head: JournalHead,
    digest: Digest,
}

impl ToolInvocationHead {
    /// Constructs a trusted compact head while enforcing scope and state shape.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationHeadError`] for crossed journal scope or an
    /// impossible status/attempt combination.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        invocation_id: InvocationId,
        revision: ToolInvocationRevision,
        status: ToolInvocationStatus,
        attempt_id: Option<AttemptId>,
        journal_head: JournalHead,
        digest: Digest,
    ) -> Result<Self, ToolInvocationHeadError> {
        validate_journal_scope(&tenant_id, run_id, &journal_head)
            .map_err(ToolInvocationHeadError::from_scope)?;
        validate_status_attempt(status, attempt_id)?;
        validate_revision_status(revision, status)?;
        Ok(Self {
            tenant_id,
            run_id,
            invocation_id,
            revision,
            status,
            attempt_id,
            journal_head,
            digest,
        })
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the durable run identity.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the stable logical invocation identity.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the exact record revision.
    #[must_use]
    pub const fn revision(&self) -> ToolInvocationRevision {
        self.revision
    }

    /// Returns the lifecycle state at this revision.
    #[must_use]
    pub const fn status(&self) -> ToolInvocationStatus {
        self.status
    }

    /// Returns the physical attempt represented by this revision, if any.
    #[must_use]
    pub const fn attempt_id(&self) -> Option<AttemptId> {
        self.attempt_id
    }

    /// Returns the exact journal prefix anchoring this revision.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the complete invocation-record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for ToolInvocationHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            invocation_id: InvocationId,
            revision: ToolInvocationRevision,
            status: ToolInvocationStatus,
            attempt_id: Option<AttemptId>,
            journal_head: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.tenant_id,
            wire.run_id,
            wire.invocation_id,
            wire.revision,
            wire.status,
            wire.attempt_id,
            wire.journal_head,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid compact invocation head.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolInvocationHeadError {
    /// Journal head crossed the invocation tenant boundary.
    #[error("tool invocation journal head crosses the tenant boundary")]
    JournalTenantMismatch,
    /// Journal head named another run.
    #[error("tool invocation journal head does not belong to the run")]
    JournalRunMismatch,
    /// Prepared status unexpectedly named a physical attempt.
    #[error("prepared tool invocation head must not contain an attempt")]
    PreparedHasAttempt,
    /// A non-prepared status omitted its physical attempt.
    #[error("non-prepared tool invocation head must contain an attempt")]
    AttemptMissing,
    /// Revision zero named a state other than preparation.
    #[error("tool invocation head revision zero must be prepared")]
    InitialStatusMismatch,
    /// A later revision tried to return to preparation.
    #[error("prepared tool invocation head must use revision zero")]
    PreparedRevisionMismatch,
}

impl ToolInvocationHeadError {
    const fn from_scope(error: InvocationScopeError) -> Self {
        match error {
            InvocationScopeError::Tenant => Self::JournalTenantMismatch,
            InvocationScopeError::Run => Self::JournalRunMismatch,
        }
    }
}

/// One immutable, journal-anchored revision of a logical tool invocation.
///
/// Deserialization verifies the record's local checksums, scope, provenance,
/// limits, and predecessor-head shape. To prove retry policy and the complete
/// state history, stream every revision in ascending order through
/// [`ToolInvocationHistoryVerifier`].
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvocation {
    intent: ToolInvocationIntent,
    revision: ToolInvocationRevision,
    state: ToolInvocationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous: Option<ToolInvocationHead>,
    journal_head: JournalHead,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition: Option<ToolInvocationTransition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition_digest: Option<Digest>,
    digest: Digest,
}

impl ToolInvocation {
    /// Materializes the initial prepared revision after its journal event commits.
    ///
    /// The supplied journal head must belong to the activation's tenant/run and
    /// strictly advance the base checkpoint journal sequence without regressing
    /// its durable clock.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationError`] for scope, ordering, or integrity failure.
    pub fn prepare(
        intent: ToolInvocationIntent,
        journal_head: JournalHead,
    ) -> Result<Self, ToolInvocationError> {
        validate_preparation_journal(&intent, &journal_head)?;
        let revision = ToolInvocationRevision::INITIAL;
        let state = ToolInvocationState::Prepared;
        let digest = compute_record_digest(&ToolInvocationRecordDigestWire {
            intent_digest: intent.intent_digest,
            revision,
            state: &state,
            previous: None,
            journal_head: &journal_head,
            transition_digest: None,
        })?;
        Ok(Self {
            intent,
            revision,
            state,
            previous: None,
            journal_head,
            transition: None,
            transition_digest: None,
            digest,
        })
    }

    /// Applies one legal transition and constructs its next immutable revision.
    ///
    /// This method enforces state legality, exact attempt provenance, descriptor
    /// and effective limits, external-effect retry safety, minimum retry delay,
    /// journal scope/order, and revision overflow. Stores must still compare
    /// [`Self::head`] under the run lock and current fencing token.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationError`] when the transition cannot safely commit.
    pub fn advance(
        &self,
        transition: ToolInvocationTransition,
        journal_head: JournalHead,
    ) -> Result<Self, ToolInvocationError> {
        validate_successor_journal(self, &journal_head)?;
        let revision = self
            .revision
            .checked_next()
            .ok_or(ToolInvocationError::RevisionOverflow)?;
        let state = apply_transition(self, &transition, journal_head.recorded_at())?;
        let transition_digest = compute_transition_digest(&transition)?;
        let previous = self.head();
        let digest = compute_record_digest(&ToolInvocationRecordDigestWire {
            intent_digest: self.intent.intent_digest,
            revision,
            state: &state,
            previous: Some(&previous),
            journal_head: &journal_head,
            transition_digest: Some(transition_digest),
        })?;
        Ok(Self {
            intent: self.intent.clone(),
            revision,
            state,
            previous: Some(previous),
            journal_head,
            transition: Some(transition),
            transition_digest: Some(transition_digest),
            digest,
        })
    }

    /// Restores a record and verifies all invariants available in that revision.
    ///
    /// Failed-to-executing retry authorization depends on the predecessor's full
    /// failure payload and is therefore verified by
    /// [`ToolInvocationHistoryVerifier`], not by a compact predecessor head.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationError`] for malformed state, provenance, scope,
    /// ordering, transition checksum, or record checksum.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        intent: ToolInvocationIntent,
        revision: ToolInvocationRevision,
        state: ToolInvocationState,
        previous: Option<ToolInvocationHead>,
        journal_head: JournalHead,
        transition: Option<ToolInvocationTransition>,
        transition_digest: Option<Digest>,
        digest: Digest,
    ) -> Result<Self, ToolInvocationError> {
        validate_record_shape(
            &intent,
            revision,
            &state,
            previous.as_ref(),
            &journal_head,
            transition.as_ref(),
            transition_digest,
        )?;
        let expected = compute_record_digest(&ToolInvocationRecordDigestWire {
            intent_digest: intent.intent_digest,
            revision,
            state: &state,
            previous: previous.as_ref(),
            journal_head: &journal_head,
            transition_digest,
        })?;
        if digest != expected {
            return Err(ToolInvocationError::DigestMismatch);
        }
        Ok(Self {
            intent,
            revision,
            state,
            previous,
            journal_head,
            transition,
            transition_digest,
            digest,
        })
    }

    /// Returns the immutable preparation intent.
    #[must_use]
    pub const fn intent(&self) -> &ToolInvocationIntent {
        &self.intent
    }

    /// Returns this record's monotonic revision.
    #[must_use]
    pub const fn revision(&self) -> ToolInvocationRevision {
        self.revision
    }

    /// Returns the integrity-bound lifecycle state.
    #[must_use]
    pub const fn state(&self) -> &ToolInvocationState {
        &self.state
    }

    /// Returns the lifecycle discriminator.
    #[must_use]
    pub const fn status(&self) -> ToolInvocationStatus {
        self.state.status()
    }

    /// Returns the physical attempt represented by this revision, if any.
    #[must_use]
    pub const fn attempt_id(&self) -> Option<AttemptId> {
        self.state.attempt_id()
    }

    /// Validates authoritative success evidence for this invocation's unknown attempt.
    ///
    /// This performs the same invocation, attempt, tool, schema, inline-size,
    /// artifact-limit, and artifact-ownership checks used by [`Self::advance`]
    /// before a reconciliation transaction is attempted.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationError`] unless this revision is unknown and the
    /// result is bound to its exact durable intent and physical attempt.
    pub fn validate_reconciliation_result(
        &self,
        result: &ToolResult,
    ) -> Result<(), ToolInvocationError> {
        let ToolInvocationState::Unknown { error } = &self.state else {
            return Err(ToolInvocationError::InvalidTransition {
                status: self.status(),
                transition: ToolInvocationTransitionKind::ReconcileResult,
            });
        };
        validate_result_binding(&self.intent, error.provenance().attempt_id(), result)
    }

    /// Validates authoritative failure/effect evidence for an unknown attempt.
    ///
    /// The evidence may resolve the invocation to `Failed`, or deliberately
    /// retain `Unknown` when the external effect remains uncertain. The same
    /// identity, attempt, risk/effect, and retry-safety checks used by
    /// [`Self::advance`] are applied here.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationError`] unless this revision is unknown and the
    /// error is bound to its exact durable intent and physical attempt.
    pub fn validate_reconciliation_error(
        &self,
        error: &ToolError,
    ) -> Result<(), ToolInvocationError> {
        let ToolInvocationState::Unknown { error: previous } = &self.state else {
            return Err(ToolInvocationError::InvalidTransition {
                status: self.status(),
                transition: ToolInvocationTransitionKind::ReconcileError,
            });
        };
        validate_error_binding(&self.intent, previous.provenance().attempt_id(), error)
    }

    /// Returns the exact predecessor head, absent only for preparation.
    #[must_use]
    pub const fn previous(&self) -> Option<&ToolInvocationHead> {
        self.previous.as_ref()
    }

    /// Returns the exact journal prefix anchoring this revision.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the explicit transition, absent only for preparation.
    #[must_use]
    pub const fn transition(&self) -> Option<&ToolInvocationTransition> {
        self.transition.as_ref()
    }

    /// Returns the transition fingerprint, absent only for preparation.
    #[must_use]
    pub const fn transition_digest(&self) -> Option<Digest> {
        self.transition_digest
    }

    /// Returns the complete domain-separated record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns a compact exact optimistic-comparison token.
    #[must_use]
    pub fn head(&self) -> ToolInvocationHead {
        ToolInvocationHead {
            tenant_id: self.intent.tenant_id().clone(),
            run_id: self.intent.run_id(),
            invocation_id: self.intent.invocation_id,
            revision: self.revision,
            status: self.status(),
            attempt_id: self.attempt_id(),
            journal_head: self.journal_head.clone(),
            digest: self.digest,
        }
    }
}

impl fmt::Debug for ToolInvocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolInvocation")
            .field("intent", &self.intent)
            .field("revision", &self.revision)
            .field("state", &self.state)
            .field("previous", &self.previous)
            .field("journal_head", &self.journal_head)
            .field("transition", &self.transition)
            .field("transition_digest", &self.transition_digest)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ToolInvocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: ToolInvocationIntent,
            revision: ToolInvocationRevision,
            state: ToolInvocationState,
            previous: Option<ToolInvocationHead>,
            journal_head: JournalHead,
            transition: Option<ToolInvocationTransition>,
            transition_digest: Option<Digest>,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.intent,
            wire.revision,
            wire.state,
            wire.previous,
            wire.journal_head,
            wire.transition,
            wire.transition_digest,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Streaming validator for one complete ascending invocation history.
///
/// Rejections are transactional: [`Self::verify_next`] never advances the last
/// accepted record on failure. A store may therefore page immutable revisions
/// without buffering the whole history while still proving every transition.
#[derive(Clone, Debug, Default)]
pub struct ToolInvocationHistoryVerifier {
    last: Option<ToolInvocation>,
}

impl ToolInvocationHistoryVerifier {
    /// Constructs an empty verifier expecting revision zero.
    #[must_use]
    pub const fn new() -> Self {
        Self { last: None }
    }

    /// Continues after one already trusted, fully restored record.
    ///
    /// A paged store must first reload the cursor by exact identity and compare
    /// its canonical record before using this constructor. This preserves the
    /// predecessor failure payload needed to authorize a retry transition.
    #[must_use]
    pub fn after(record: ToolInvocation) -> Self {
        Self { last: Some(record) }
    }

    /// Returns the last verified head, if any.
    #[must_use]
    pub fn head(&self) -> Option<ToolInvocationHead> {
        self.last.as_ref().map(ToolInvocation::head)
    }

    /// Returns whether at least one revision has been verified.
    #[must_use]
    pub const fn has_records(&self) -> bool {
        self.last.is_some()
    }

    /// Verifies and then advances to the next ascending revision.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInvocationHistoryError`] for a non-initial first record,
    /// intent substitution, head mismatch, unsafe retry, or any other state
    /// transition failure.
    pub fn verify_next(
        &mut self,
        record: &ToolInvocation,
    ) -> Result<(), ToolInvocationHistoryError> {
        let Some(previous) = self.last.as_ref() else {
            if record.revision != ToolInvocationRevision::INITIAL {
                return Err(ToolInvocationHistoryError::FirstRecordNotInitial {
                    actual: record.revision,
                });
            }
            self.last = Some(record.clone());
            return Ok(());
        };

        if record.intent != previous.intent {
            return Err(ToolInvocationHistoryError::IntentMismatch);
        }
        if record.previous.as_ref() != Some(&previous.head()) {
            return Err(ToolInvocationHistoryError::PreviousHeadMismatch);
        }
        let transition = record
            .transition
            .as_ref()
            .ok_or(ToolInvocationHistoryError::TransitionMissing)?;
        let expected = previous
            .advance(transition.clone(), record.journal_head.clone())
            .map_err(|source| ToolInvocationHistoryError::Transition { source })?;
        if !canonical_equal(&expected, record)
            .map_err(|source| ToolInvocationHistoryError::Integrity { source })?
        {
            return Err(ToolInvocationHistoryError::RecordMismatch);
        }

        self.last = Some(record.clone());
        Ok(())
    }
}

/// Invalid ascending invocation history.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolInvocationHistoryError {
    /// The first supplied record was not preparation revision zero.
    #[error("first tool invocation record is revision {actual}; expected 0")]
    FirstRecordNotInitial {
        /// Rejected first revision.
        actual: ToolInvocationRevision,
    },
    /// A successor substituted immutable preparation fields.
    #[error("tool invocation history changed its immutable intent")]
    IntentMismatch,
    /// A successor did not name the exact previously verified head.
    #[error("tool invocation history predecessor head mismatch")]
    PreviousHeadMismatch,
    /// A non-initial record omitted its transition.
    #[error("tool invocation history successor is missing its transition")]
    TransitionMissing,
    /// Applying the transition to the full predecessor failed.
    #[error("tool invocation history transition is invalid: {source}")]
    Transition {
        /// Exact state-machine failure.
        #[source]
        source: ToolInvocationError,
    },
    /// Expected and persisted successor bytes did not match exactly.
    #[error("tool invocation history successor does not match the applied transition")]
    RecordMismatch,
    /// Exact record comparison could not be canonicalized.
    #[error("tool invocation history integrity comparison failed: {source}")]
    Integrity {
        /// Exact canonicalization failure.
        #[source]
        source: ToolInvocationIntegrityError,
    },
}

/// Artifact binding field rejected while restoring a durable result.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, PartialEq)]
#[non_exhaustive]
pub enum ToolArtifactBinding {
    /// Artifact belongs to another tenant.
    Tenant,
    /// Artifact provenance names another run.
    Run,
    /// Artifact provenance names another registry principal.
    Principal,
    /// Artifact provenance omits or substitutes the producing capability.
    Capability,
}

/// Invalid invocation record, transition, provenance, or integrity layer.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolInvocationError {
    /// Canonical typed integrity material could not be serialized.
    #[error("tool invocation integrity calculation failed: {source}")]
    Integrity {
        /// Exact canonical integrity failure.
        #[source]
        source: ToolInvocationIntegrityError,
    },
    /// Journal head crossed the invocation tenant boundary.
    #[error("tool invocation journal head crosses the tenant boundary")]
    JournalTenantMismatch,
    /// Journal head named another run.
    #[error("tool invocation journal head does not belong to the run")]
    JournalRunMismatch,
    /// A revision did not strictly advance its required journal predecessor.
    #[error("tool invocation journal sequence {actual} does not advance {previous}")]
    JournalDidNotAdvance {
        /// Required prior sequence.
        previous: JournalSequence,
        /// Rejected current sequence.
        actual: JournalSequence,
    },
    /// A revision's durable clock preceded its required predecessor.
    #[error("tool invocation journal clock regressed from {previous} to {actual}")]
    ClockRegression {
        /// Required prior durable timestamp.
        previous: Timestamp,
        /// Rejected current timestamp.
        actual: Timestamp,
    },
    /// No storage-compatible successor revision exists.
    #[error("tool invocation revision exceeds the PostgreSQL BIGINT maximum")]
    RevisionOverflow,
    /// Revision zero did not use the exact preparation shape.
    #[error("initial tool invocation revision must be prepared with no predecessor or transition")]
    InvalidInitialShape,
    /// A successor omitted or unexpectedly added predecessor/transition fields.
    #[error(
        "tool invocation successor must contain predecessor, transition, and transition digest"
    )]
    InvalidSuccessorShape,
    /// Compact predecessor crossed the immutable tenant boundary.
    #[error("tool invocation predecessor crosses the tenant boundary")]
    PreviousTenantMismatch,
    /// Compact predecessor named another run.
    #[error("tool invocation predecessor names another run")]
    PreviousRunMismatch,
    /// Compact predecessor named another logical invocation.
    #[error("tool invocation predecessor names another invocation")]
    PreviousInvocationMismatch,
    /// Current revision was not the exact successor of its compact predecessor.
    #[error("tool invocation revision {actual} does not follow predecessor {previous}")]
    PreviousRevisionMismatch {
        /// Predecessor revision.
        previous: ToolInvocationRevision,
        /// Rejected current revision.
        actual: ToolInvocationRevision,
    },
    /// Persisted transition checksum did not match its transition payload.
    #[error("tool invocation transition digest does not match its payload")]
    TransitionDigestMismatch,
    /// Transition is not legal from the predecessor lifecycle state.
    #[error("tool invocation transition {transition:?} is invalid from {status:?}")]
    InvalidTransition {
        /// Predecessor status.
        status: ToolInvocationStatus,
        /// Rejected transition kind.
        transition: ToolInvocationTransitionKind,
    },
    /// Transition payload and resulting state differed.
    #[error("tool invocation transition payload does not match its resulting state")]
    TransitionStateMismatch,
    /// A retry reused the immediately preceding physical attempt identity.
    #[error("tool invocation retry must use a new physical attempt identifier")]
    ReusedAttemptId,
    /// Failed outcome did not explicitly authorize a safe retry.
    #[error("tool invocation failure does not authorize retry")]
    RetryNotAuthorized,
    /// Retry time could not be represented in the canonical timestamp range.
    #[error("tool invocation retry delay exceeds the supported timestamp range")]
    RetryDelayOutOfRange,
    /// Retry occurred before the failure's explicit minimum delay elapsed.
    #[error("tool invocation retry at {actual} precedes not-before time {not_before}")]
    RetryDelayNotElapsed {
        /// Earliest permitted durable retry timestamp.
        not_before: Timestamp,
        /// Rejected retry timestamp.
        actual: Timestamp,
    },
    /// Failure evidence and tool semantics do not make repetition safe.
    #[error("tool invocation failure evidence does not permit a repeated external call")]
    RetryUnsafe,
    /// Successful result named another logical invocation.
    #[error("tool result names another logical invocation")]
    ResultInvocationMismatch,
    /// Successful result named another physical attempt.
    #[error("tool result names another physical attempt")]
    ResultAttemptMismatch,
    /// Successful result named another tool version.
    #[error("tool result names another tool identity")]
    ResultToolMismatch,
    /// Successful result named another output schema.
    #[error("tool result names another output schema")]
    ResultSchemaMismatch,
    /// Inline result exceeded the resolved invocation ceiling.
    #[error("tool result is {actual} inline bytes; effective maximum is {maximum}")]
    ResultInlineLimitExceeded {
        /// Resolved inline-result ceiling.
        maximum: ByteCount,
        /// Exact compact result size.
        actual: ByteCount,
    },
    /// Result artifact count exceeded the resolved invocation ceiling.
    #[error("tool result has {actual} artifacts; effective maximum is {maximum}")]
    ResultArtifactCountExceeded {
        /// Resolved artifact-count ceiling.
        maximum: ExecutionCount,
        /// Actual artifact count.
        actual: ExecutionCount,
    },
    /// Result artifact bytes exceeded the resolved invocation ceiling.
    #[error("tool result artifacts total {actual} bytes; effective maximum is {maximum}")]
    ResultArtifactBytesExceeded {
        /// Resolved aggregate artifact-byte ceiling.
        maximum: ByteCount,
        /// Actual aggregate artifact bytes.
        actual: ByteCount,
    },
    /// One artifact crossed an invocation ownership boundary.
    #[error("tool result artifact at index {index} has invalid {binding:?} binding")]
    ResultArtifactBinding {
        /// Zero-based artifact position.
        index: usize,
        /// Rejected ownership field.
        binding: ToolArtifactBinding,
    },
    /// Failure evidence named another logical invocation.
    #[error("tool error names another logical invocation")]
    ErrorInvocationMismatch,
    /// Failure evidence named another physical attempt.
    #[error("tool error names another physical attempt")]
    ErrorAttemptMismatch,
    /// Failure evidence named another tool version.
    #[error("tool error names another tool identity")]
    ErrorToolMismatch,
    /// Failure effect evidence contradicted the descriptor's reviewed risk.
    #[error("tool error effect {effect:?} contradicts descriptor risk {risk:?}")]
    ErrorEffectRiskMismatch {
        /// Reviewed tool risk.
        risk: ToolRisk,
        /// Rejected effect evidence.
        effect: ToolExternalEffect,
    },
    /// A non-idempotent applied write claimed automatic retry was safe.
    #[error("applied non-idempotent tool write cannot authorize automatic retry")]
    UnsafeRetryAfterAppliedNonIdempotentWrite,
    /// Failed state incorrectly contained ambiguous outcome evidence.
    #[error("failed tool invocation state cannot contain unknown effect evidence")]
    FailedStateHasUnknownEffect,
    /// Unknown state omitted exact ambiguous outcome evidence.
    #[error("unknown tool invocation state requires unknown effect evidence")]
    UnknownStateRequiresUnknownEffect,
    /// Persisted complete record checksum did not match its fields.
    #[error("tool invocation record digest does not match its fields")]
    DigestMismatch,
}

impl From<ToolInvocationIntegrityError> for ToolInvocationError {
    fn from(source: ToolInvocationIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Failure to canonicalize a closed invocation checksum preimage.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolInvocationIntegrityError {
    /// Typed values could not be represented as canonical JSON.
    #[error("tool invocation checksum preimage canonical serialization failed")]
    CanonicalSerialization,
}

#[derive(Serialize)]
struct ReadyNodeInputDigestWire<'a> {
    base_checkpoint_digest: Digest,
    graph_namespace: &'a GraphNamespace,
    node_id: &'a NodeId,
}

#[derive(Serialize)]
struct ToolInvocationIntentDigestWire<'a> {
    activation: &'a NodeActivation,
    invocation_id: InvocationId,
    descriptor: &'a ToolDescriptor,
    input: &'a ToolInput,
    effective_limits: &'a ToolExecutionLimits,
}

#[derive(Serialize)]
struct ToolInvocationRecordDigestWire<'a> {
    intent_digest: Digest,
    revision: ToolInvocationRevision,
    state: &'a ToolInvocationState,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous: Option<&'a ToolInvocationHead>,
    journal_head: &'a JournalHead,
    #[serde(skip_serializing_if = "Option::is_none")]
    transition_digest: Option<Digest>,
}

fn compute_ready_node_input_digest(
    base_checkpoint_digest: Digest,
    graph_namespace: &GraphNamespace,
    node_id: &NodeId,
) -> Result<Digest, NodeActivationError> {
    let canonical = serde_json_canonicalizer::to_vec(&ReadyNodeInputDigestWire {
        base_checkpoint_digest,
        graph_namespace,
        node_id,
    })
    .map_err(|_| NodeActivationError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(READY_NODE_INPUT_DIGEST_DOMAIN.len() + canonical.len());
    preimage.extend_from_slice(READY_NODE_INPUT_DIGEST_DOMAIN);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn compute_intent_digest(
    value: &ToolInvocationIntentDigestWire<'_>,
) -> Result<Digest, ToolInvocationIntegrityError> {
    domain_separated_digest(INTENT_DIGEST_DOMAIN, value)
}

fn compute_transition_digest(
    value: &ToolInvocationTransition,
) -> Result<Digest, ToolInvocationIntegrityError> {
    domain_separated_digest(TRANSITION_DIGEST_DOMAIN, value)
}

fn compute_record_digest(
    value: &ToolInvocationRecordDigestWire<'_>,
) -> Result<Digest, ToolInvocationIntegrityError> {
    domain_separated_digest(RECORD_DIGEST_DOMAIN, value)
}

fn domain_separated_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, ToolInvocationIntegrityError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| ToolInvocationIntegrityError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn canonical_equal<T: Serialize>(
    left: &T,
    right: &T,
) -> Result<bool, ToolInvocationIntegrityError> {
    let left = serde_json_canonicalizer::to_vec(left)
        .map_err(|_| ToolInvocationIntegrityError::CanonicalSerialization)?;
    let right = serde_json_canonicalizer::to_vec(right)
        .map_err(|_| ToolInvocationIntegrityError::CanonicalSerialization)?;
    Ok(left == right)
}

fn validate_intent_shape(
    descriptor: &ToolDescriptor,
    input: &ToolInput,
    effective: &ToolExecutionLimits,
) -> Result<(), ToolInvocationIntentError> {
    if input.schema() != descriptor.input_schema() {
        return Err(ToolInvocationIntentError::InputSchemaMismatch);
    }
    let descriptor_limits = descriptor.limits();
    for (widened, limit) in [
        (
            effective.timeout() > descriptor_limits.timeout(),
            ToolInvocationLimit::Timeout,
        ),
        (
            effective.max_concurrency() > descriptor_limits.max_concurrency(),
            ToolInvocationLimit::MaxConcurrency,
        ),
        (
            effective.max_input_bytes() > descriptor_limits.max_input_bytes(),
            ToolInvocationLimit::MaxInputBytes,
        ),
        (
            effective.max_inline_result_bytes() > descriptor_limits.max_inline_result_bytes(),
            ToolInvocationLimit::MaxInlineResultBytes,
        ),
        (
            effective.max_artifacts() > descriptor_limits.max_artifacts(),
            ToolInvocationLimit::MaxArtifacts,
        ),
        (
            effective.max_total_artifact_bytes() > descriptor_limits.max_total_artifact_bytes(),
            ToolInvocationLimit::MaxTotalArtifactBytes,
        ),
    ] {
        if widened {
            return Err(ToolInvocationIntentError::EffectiveLimitWidened { limit });
        }
    }

    let actual = byte_count_from_usize(input.value().stats().compact_bytes());
    let maximum = effective.max_input_bytes();
    if actual > maximum {
        return Err(ToolInvocationIntentError::InputLimitExceeded { maximum, actual });
    }
    Ok(())
}

fn byte_count_from_usize(value: usize) -> ByteCount {
    ByteCount::new(u64::try_from(value).unwrap_or(u64::MAX))
}

fn execution_count_from_usize(value: usize) -> ExecutionCount {
    ExecutionCount::new(u64::try_from(value).unwrap_or(u64::MAX))
}

#[derive(Clone, Copy)]
enum InvocationScopeError {
    Tenant,
    Run,
}

fn validate_journal_scope(
    tenant_id: &TenantId,
    run_id: RunId,
    head: &JournalHead,
) -> Result<(), InvocationScopeError> {
    if head.tenant_id() != tenant_id {
        return Err(InvocationScopeError::Tenant);
    }
    if head.run_id() != run_id {
        return Err(InvocationScopeError::Run);
    }
    Ok(())
}

fn validate_status_attempt(
    status: ToolInvocationStatus,
    attempt_id: Option<AttemptId>,
) -> Result<(), ToolInvocationHeadError> {
    match (status, attempt_id) {
        (ToolInvocationStatus::Prepared, None)
        | (
            ToolInvocationStatus::Executing
            | ToolInvocationStatus::Committed
            | ToolInvocationStatus::Failed
            | ToolInvocationStatus::Unknown,
            Some(_),
        ) => Ok(()),
        (ToolInvocationStatus::Prepared, Some(_)) => {
            Err(ToolInvocationHeadError::PreparedHasAttempt)
        }
        (
            ToolInvocationStatus::Executing
            | ToolInvocationStatus::Committed
            | ToolInvocationStatus::Failed
            | ToolInvocationStatus::Unknown,
            None,
        ) => Err(ToolInvocationHeadError::AttemptMissing),
    }
}

fn validate_revision_status(
    revision: ToolInvocationRevision,
    status: ToolInvocationStatus,
) -> Result<(), ToolInvocationHeadError> {
    match (revision == ToolInvocationRevision::INITIAL, status) {
        (true, ToolInvocationStatus::Prepared)
        | (
            false,
            ToolInvocationStatus::Executing
            | ToolInvocationStatus::Committed
            | ToolInvocationStatus::Failed
            | ToolInvocationStatus::Unknown,
        ) => Ok(()),
        (true, _) => Err(ToolInvocationHeadError::InitialStatusMismatch),
        (false, ToolInvocationStatus::Prepared) => {
            Err(ToolInvocationHeadError::PreparedRevisionMismatch)
        }
    }
}

fn map_scope_error(error: InvocationScopeError) -> ToolInvocationError {
    match error {
        InvocationScopeError::Tenant => ToolInvocationError::JournalTenantMismatch,
        InvocationScopeError::Run => ToolInvocationError::JournalRunMismatch,
    }
}

fn validate_journal_advances(
    previous: &JournalHead,
    actual: &JournalHead,
) -> Result<(), ToolInvocationError> {
    if actual.sequence() <= previous.sequence() {
        return Err(ToolInvocationError::JournalDidNotAdvance {
            previous: previous.sequence(),
            actual: actual.sequence(),
        });
    }
    if actual.recorded_at() < previous.recorded_at() {
        return Err(ToolInvocationError::ClockRegression {
            previous: previous.recorded_at(),
            actual: actual.recorded_at(),
        });
    }
    Ok(())
}

fn validate_preparation_journal(
    intent: &ToolInvocationIntent,
    journal_head: &JournalHead,
) -> Result<(), ToolInvocationError> {
    validate_journal_scope(intent.tenant_id(), intent.run_id(), journal_head)
        .map_err(map_scope_error)?;
    validate_journal_advances(
        intent.activation.base_checkpoint().journal_head(),
        journal_head,
    )
}

fn validate_successor_journal(
    invocation: &ToolInvocation,
    journal_head: &JournalHead,
) -> Result<(), ToolInvocationError> {
    validate_journal_scope(
        invocation.intent.tenant_id(),
        invocation.intent.run_id(),
        journal_head,
    )
    .map_err(map_scope_error)?;
    validate_journal_advances(&invocation.journal_head, journal_head)
}

#[allow(clippy::too_many_arguments)]
fn validate_record_shape(
    intent: &ToolInvocationIntent,
    revision: ToolInvocationRevision,
    state: &ToolInvocationState,
    previous: Option<&ToolInvocationHead>,
    journal_head: &JournalHead,
    transition: Option<&ToolInvocationTransition>,
    transition_digest: Option<Digest>,
) -> Result<(), ToolInvocationError> {
    validate_journal_scope(intent.tenant_id(), intent.run_id(), journal_head)
        .map_err(map_scope_error)?;
    validate_state_binding(intent, state)?;

    if revision == ToolInvocationRevision::INITIAL {
        if !matches!(state, ToolInvocationState::Prepared)
            || previous.is_some()
            || transition.is_some()
            || transition_digest.is_some()
        {
            return Err(ToolInvocationError::InvalidInitialShape);
        }
        return validate_preparation_journal(intent, journal_head);
    }

    let (Some(previous), Some(transition), Some(transition_digest)) =
        (previous, transition, transition_digest)
    else {
        return Err(ToolInvocationError::InvalidSuccessorShape);
    };
    if previous.tenant_id() != intent.tenant_id() {
        return Err(ToolInvocationError::PreviousTenantMismatch);
    }
    if previous.run_id() != intent.run_id() {
        return Err(ToolInvocationError::PreviousRunMismatch);
    }
    if previous.invocation_id() != intent.invocation_id() {
        return Err(ToolInvocationError::PreviousInvocationMismatch);
    }
    if previous.revision().checked_next() != Some(revision) {
        return Err(ToolInvocationError::PreviousRevisionMismatch {
            previous: previous.revision(),
            actual: revision,
        });
    }
    validate_journal_advances(previous.journal_head(), journal_head)?;

    if compute_transition_digest(transition)? != transition_digest {
        return Err(ToolInvocationError::TransitionDigestMismatch);
    }
    validate_transition_shape(intent, previous, transition, state)
}

fn validate_state_binding(
    intent: &ToolInvocationIntent,
    state: &ToolInvocationState,
) -> Result<(), ToolInvocationError> {
    match state {
        ToolInvocationState::Prepared | ToolInvocationState::Executing { .. } => Ok(()),
        ToolInvocationState::Committed { result } => {
            validate_result_binding(intent, result.provenance().attempt_id(), result)
        }
        ToolInvocationState::Failed { error } => {
            validate_error_binding(intent, error.provenance().attempt_id(), error)?;
            if error.external_effect() == ToolExternalEffect::Unknown {
                return Err(ToolInvocationError::FailedStateHasUnknownEffect);
            }
            Ok(())
        }
        ToolInvocationState::Unknown { error } => {
            validate_error_binding(intent, error.provenance().attempt_id(), error)?;
            if error.external_effect() != ToolExternalEffect::Unknown {
                return Err(ToolInvocationError::UnknownStateRequiresUnknownEffect);
            }
            Ok(())
        }
    }
}

fn validate_result_binding(
    intent: &ToolInvocationIntent,
    expected_attempt: AttemptId,
    result: &ToolResult,
) -> Result<(), ToolInvocationError> {
    let provenance = result.provenance();
    if provenance.invocation_id() != intent.invocation_id() {
        return Err(ToolInvocationError::ResultInvocationMismatch);
    }
    if provenance.attempt_id() != expected_attempt {
        return Err(ToolInvocationError::ResultAttemptMismatch);
    }
    if provenance.tool() != intent.descriptor.metadata().identity() {
        return Err(ToolInvocationError::ResultToolMismatch);
    }
    if result.output_schema() != intent.descriptor.output_schema() {
        return Err(ToolInvocationError::ResultSchemaMismatch);
    }

    let actual = byte_count_from_usize(result.output().stats().compact_bytes());
    let maximum = intent.effective_limits.max_inline_result_bytes();
    if actual > maximum {
        return Err(ToolInvocationError::ResultInlineLimitExceeded { maximum, actual });
    }
    let actual = execution_count_from_usize(result.artifacts().len());
    let maximum = intent.effective_limits.max_artifacts();
    if actual > maximum {
        return Err(ToolInvocationError::ResultArtifactCountExceeded { maximum, actual });
    }
    let actual = result.artifacts().total_bytes();
    let maximum = intent.effective_limits.max_total_artifact_bytes();
    if actual > maximum {
        return Err(ToolInvocationError::ResultArtifactBytesExceeded { maximum, actual });
    }

    let expected_principal = intent.descriptor.metadata().identity().owner();
    let expected_capability = intent.descriptor.metadata().identity().capability();
    for (index, artifact) in result.artifacts().iter().enumerate() {
        let binding = if artifact.identity().tenant_id() != intent.tenant_id() {
            Some(ToolArtifactBinding::Tenant)
        } else if artifact.provenance().run_id() != intent.run_id() {
            Some(ToolArtifactBinding::Run)
        } else if artifact.provenance().principal() != expected_principal {
            Some(ToolArtifactBinding::Principal)
        } else if artifact.provenance().capability() != Some(expected_capability) {
            Some(ToolArtifactBinding::Capability)
        } else {
            None
        };
        if let Some(binding) = binding {
            return Err(ToolInvocationError::ResultArtifactBinding { index, binding });
        }
    }
    Ok(())
}

fn validate_error_binding(
    intent: &ToolInvocationIntent,
    expected_attempt: AttemptId,
    error: &ToolError,
) -> Result<(), ToolInvocationError> {
    let provenance = error.provenance();
    if provenance.invocation_id() != intent.invocation_id() {
        return Err(ToolInvocationError::ErrorInvocationMismatch);
    }
    if provenance.attempt_id() != expected_attempt {
        return Err(ToolInvocationError::ErrorAttemptMismatch);
    }
    if provenance.tool() != intent.descriptor.metadata().identity() {
        return Err(ToolInvocationError::ErrorToolMismatch);
    }

    let risk = intent.descriptor.semantics().risk();
    let effect = error.external_effect();
    let valid = match risk {
        ToolRisk::ReadOnly => effect == ToolExternalEffect::NotApplicable,
        ToolRisk::IdempotentWrite | ToolRisk::NonIdempotentWrite => matches!(
            effect,
            ToolExternalEffect::NotStarted
                | ToolExternalEffect::NotApplied
                | ToolExternalEffect::Applied
                | ToolExternalEffect::Unknown
        ),
    };
    if !valid {
        return Err(ToolInvocationError::ErrorEffectRiskMismatch { risk, effect });
    }
    if risk == ToolRisk::NonIdempotentWrite
        && effect == ToolExternalEffect::Applied
        && matches!(
            error.failure().retry_advice(),
            RetryAdvice::SafeAfter { .. }
        )
    {
        return Err(ToolInvocationError::UnsafeRetryAfterAppliedNonIdempotentWrite);
    }
    Ok(())
}

fn validate_transition_shape(
    intent: &ToolInvocationIntent,
    previous: &ToolInvocationHead,
    transition: &ToolInvocationTransition,
    state: &ToolInvocationState,
) -> Result<(), ToolInvocationError> {
    let valid = match (previous.status(), transition, state) {
        (
            ToolInvocationStatus::Prepared | ToolInvocationStatus::Failed,
            ToolInvocationTransition::StartAttempt { attempt_id },
            ToolInvocationState::Executing {
                attempt_id: state_attempt,
            },
        ) => {
            if previous.attempt_id() == Some(*attempt_id) {
                return Err(ToolInvocationError::ReusedAttemptId);
            }
            attempt_id == state_attempt
        }
        (
            ToolInvocationStatus::Executing,
            ToolInvocationTransition::RecordResult { result },
            ToolInvocationState::Committed {
                result: state_result,
            },
        )
        | (
            ToolInvocationStatus::Unknown,
            ToolInvocationTransition::ReconcileResult { result },
            ToolInvocationState::Committed {
                result: state_result,
            },
        ) => result == state_result,
        (
            ToolInvocationStatus::Executing,
            ToolInvocationTransition::RecordError { error },
            ToolInvocationState::Failed { error: state_error },
        ) if error.external_effect() != ToolExternalEffect::Unknown => {
            canonical_equal(error, state_error)?
        }
        (
            ToolInvocationStatus::Executing,
            ToolInvocationTransition::RecordError { error },
            ToolInvocationState::Unknown { error: state_error },
        ) if error.external_effect() == ToolExternalEffect::Unknown => {
            canonical_equal(error, state_error)?
        }
        (
            ToolInvocationStatus::Unknown,
            ToolInvocationTransition::ReconcileError { error },
            ToolInvocationState::Unknown { error: state_error },
        ) if error.external_effect() == ToolExternalEffect::Unknown => {
            canonical_equal(error, state_error)?
        }
        (
            ToolInvocationStatus::Unknown,
            ToolInvocationTransition::ReconcileError { error },
            ToolInvocationState::Failed { error: state_error },
        ) if error.external_effect() != ToolExternalEffect::Unknown => {
            canonical_equal(error, state_error)?
        }
        _ => {
            return Err(ToolInvocationError::InvalidTransition {
                status: previous.status(),
                transition: transition.kind(),
            });
        }
    };
    if !valid {
        return Err(ToolInvocationError::TransitionStateMismatch);
    }

    let expected_attempt = previous.attempt_id();
    match transition {
        ToolInvocationTransition::StartAttempt { .. } => {}
        ToolInvocationTransition::RecordResult { result }
        | ToolInvocationTransition::ReconcileResult { result } => {
            validate_result_binding(
                intent,
                expected_attempt.ok_or(ToolInvocationError::TransitionStateMismatch)?,
                result,
            )?;
        }
        ToolInvocationTransition::RecordError { error }
        | ToolInvocationTransition::ReconcileError { error } => {
            validate_error_binding(
                intent,
                expected_attempt.ok_or(ToolInvocationError::TransitionStateMismatch)?,
                error,
            )?;
        }
    }
    Ok(())
}

fn apply_transition(
    invocation: &ToolInvocation,
    transition: &ToolInvocationTransition,
    recorded_at: Timestamp,
) -> Result<ToolInvocationState, ToolInvocationError> {
    match (&invocation.state, transition) {
        (ToolInvocationState::Prepared, ToolInvocationTransition::StartAttempt { attempt_id }) => {
            Ok(ToolInvocationState::Executing {
                attempt_id: *attempt_id,
            })
        }
        (
            ToolInvocationState::Failed { error },
            ToolInvocationTransition::StartAttempt { attempt_id },
        ) => {
            let previous_attempt = error.provenance().attempt_id();
            if previous_attempt == *attempt_id {
                return Err(ToolInvocationError::ReusedAttemptId);
            }
            validate_retry(invocation, error, recorded_at)?;
            Ok(ToolInvocationState::Executing {
                attempt_id: *attempt_id,
            })
        }
        (
            ToolInvocationState::Executing { attempt_id },
            ToolInvocationTransition::RecordResult { result },
        ) => {
            validate_result_binding(&invocation.intent, *attempt_id, result)?;
            Ok(ToolInvocationState::Committed {
                result: result.clone(),
            })
        }
        (
            ToolInvocationState::Executing { attempt_id },
            ToolInvocationTransition::RecordError { error },
        ) => {
            validate_error_binding(&invocation.intent, *attempt_id, error)?;
            Ok(state_from_error(error.clone()))
        }
        (
            ToolInvocationState::Unknown { error: previous },
            ToolInvocationTransition::ReconcileResult { result },
        ) => {
            validate_result_binding(
                &invocation.intent,
                previous.provenance().attempt_id(),
                result,
            )?;
            Ok(ToolInvocationState::Committed {
                result: result.clone(),
            })
        }
        (
            ToolInvocationState::Unknown { error: previous },
            ToolInvocationTransition::ReconcileError { error },
        ) => {
            validate_error_binding(
                &invocation.intent,
                previous.provenance().attempt_id(),
                error,
            )?;
            Ok(state_from_error(error.clone()))
        }
        _ => Err(ToolInvocationError::InvalidTransition {
            status: invocation.status(),
            transition: transition.kind(),
        }),
    }
}

fn state_from_error(error: ToolError) -> ToolInvocationState {
    if error.external_effect() == ToolExternalEffect::Unknown {
        ToolInvocationState::Unknown { error }
    } else {
        ToolInvocationState::Failed { error }
    }
}

fn validate_retry(
    invocation: &ToolInvocation,
    error: &ToolError,
    recorded_at: Timestamp,
) -> Result<(), ToolInvocationError> {
    let Some(delay) = error.failure().retry_advice().safe_after_delay() else {
        return Err(ToolInvocationError::RetryNotAuthorized);
    };
    let risk = invocation.intent.descriptor.semantics().risk();
    let safe = matches!(
        (risk, error.external_effect()),
        (ToolRisk::ReadOnly, ToolExternalEffect::NotApplicable)
            | (
                ToolRisk::IdempotentWrite,
                ToolExternalEffect::NotStarted
                    | ToolExternalEffect::NotApplied
                    | ToolExternalEffect::Applied
            )
            | (
                ToolRisk::NonIdempotentWrite,
                ToolExternalEffect::NotStarted | ToolExternalEffect::NotApplied
            )
    );
    if !safe {
        return Err(ToolInvocationError::RetryUnsafe);
    }

    let not_before_micros = i128::from(invocation.journal_head.recorded_at().unix_micros())
        + i128::from(delay.as_i64()) * 1_000;
    if not_before_micros > i128::from(Timestamp::MAX.unix_micros()) {
        return Err(ToolInvocationError::RetryDelayOutOfRange);
    }
    let not_before = Timestamp::from_unix_micros(
        i64::try_from(not_before_micros).map_err(|_| ToolInvocationError::RetryDelayOutOfRange)?,
    )
    .map_err(|_| ToolInvocationError::RetryDelayOutOfRange)?;
    if recorded_at < not_before {
        return Err(ToolInvocationError::RetryDelayNotElapsed {
            not_before,
            actual: recorded_at,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use schemars::schema_for;
    use serde_json::{Value, from_value, json, to_value};

    use super::*;
    use crate::{
        Checkpoint, DurationMillis, EventId, Failure, FailureCategory, FailureCode, FailureId,
        FailureMessage, FailureOrigin, ToolErrorPhase, ToolErrorProvenance,
    };

    fn fixture(path: &[&str], source: &str) -> Value {
        let mut value: Value = serde_json::from_str(source).unwrap();
        for component in path {
            value = match component.parse::<usize>() {
                Ok(index) => value[index].clone(),
                Err(_) => value[*component].clone(),
            };
        }
        value
    }

    fn checkpoint() -> Checkpoint {
        from_value(fixture(
            &["checkpoints", "0"],
            include_str!("../tests/fixtures/core-checkpoint-v1.json"),
        ))
        .unwrap()
    }

    fn descriptor() -> ToolDescriptor {
        from_value(fixture(
            &["descriptors", "valid", "0"],
            include_str!("../tests/fixtures/core-tool-v1.json"),
        ))
        .unwrap()
    }

    fn input() -> ToolInput {
        from_value(fixture(
            &["inputs", "valid", "0"],
            include_str!("../tests/fixtures/core-tool-runtime-v1.json"),
        ))
        .unwrap()
    }

    fn result() -> ToolResult {
        from_value(fixture(
            &["results", "valid", "0"],
            include_str!("../tests/fixtures/core-tool-runtime-v1.json"),
        ))
        .unwrap()
    }

    fn fixture_error(index: &str) -> ToolError {
        from_value(fixture(
            &["errors", "valid", index],
            include_str!("../tests/fixtures/core-tool-runtime-v1.json"),
        ))
        .unwrap()
    }

    fn invocation_id() -> InvocationId {
        "01912345-6789-7abc-8def-0123456789ad".parse().unwrap()
    }

    fn attempt(suffix: &str) -> AttemptId {
        format!("01912345-6789-7abc-8def-0123456789{suffix}")
            .parse()
            .unwrap()
    }

    fn intent() -> ToolInvocationIntent {
        let checkpoint = checkpoint();
        let descriptor = descriptor();
        ToolInvocationIntent::new(
            NodeActivation::new(
                checkpoint.head(),
                GraphNamespace::root(),
                NodeId::new("authorize").unwrap(),
                Digest::sha256(b"node-input"),
            ),
            invocation_id(),
            descriptor.clone(),
            input(),
            descriptor.limits().clone(),
        )
        .unwrap()
    }

    fn journal(intent: &ToolInvocationIntent, sequence: u64) -> JournalHead {
        let base = intent
            .activation()
            .base_checkpoint()
            .journal_head()
            .recorded_at();
        let offset = i64::try_from(sequence - 1).unwrap() * 1_000_000;
        let event_id: EventId =
            format!("01912345-6789-7abc-8def-0123456789{:02x}", 0xc0 + sequence)
                .parse()
                .unwrap();
        JournalHead::new(
            intent.tenant_id().clone(),
            intent.run_id(),
            JournalSequence::new(sequence).unwrap(),
            event_id,
            Timestamp::from_unix_micros(base.unix_micros() + offset).unwrap(),
            Digest::sha256(sequence.to_be_bytes()),
        )
    }

    fn prepared() -> ToolInvocation {
        let intent = intent();
        let head = journal(&intent, 2);
        ToolInvocation::prepare(intent, head).unwrap()
    }

    fn safe_error(delay_millis: i64) -> ToolError {
        let failure = Failure::new(
            "01912345-6789-7abc-8def-0123456789b8"
                .parse::<FailureId>()
                .unwrap(),
            FailureCategory::DependencyUnavailable,
            FailureCode::new("tool.dependency_unavailable").unwrap(),
            FailureOrigin::new("tool.payments").unwrap(),
            FailureMessage::new("The payment dependency is temporarily unavailable.").unwrap(),
            RetryAdvice::SafeAfter {
                delay: DurationMillis::new(delay_millis).unwrap(),
            },
        )
        .unwrap();
        ToolError::new(
            failure,
            ToolErrorPhase::Execution,
            ToolExternalEffect::NotApplied,
            ToolErrorProvenance::new(
                invocation_id(),
                attempt("ab"),
                descriptor().metadata().identity().clone(),
            ),
        )
        .unwrap()
    }

    #[test]
    fn namespace_and_revision_wires_are_canonical_and_bounded() {
        for valid in ["", "parent", "parent/child-v2", "a.b/c_d"] {
            let namespace = GraphNamespace::new(valid).unwrap();
            assert_eq!(to_value(&namespace).unwrap(), json!(valid));
            assert_eq!(
                from_value::<GraphNamespace>(json!(valid)).unwrap(),
                namespace
            );
        }
        for invalid in ["/node", "node/", "node//child", ".", "parent/.."] {
            assert!(GraphNamespace::new(invalid).is_err());
        }
        assert!(GraphNamespace::new("a".repeat(GraphNamespace::MAX_LEN + 1)).is_err());

        for value in [0, 1, MAX_DATABASE_ORDINAL] {
            let revision = ToolInvocationRevision::new(value).unwrap();
            assert_eq!(
                revision
                    .to_string()
                    .parse::<ToolInvocationRevision>()
                    .unwrap(),
                revision
            );
            assert_eq!(
                from_value::<ToolInvocationRevision>(json!(value.to_string())).unwrap(),
                revision
            );
        }
        for invalid in [
            json!(0),
            json!("00"),
            json!("-1"),
            json!("9223372036854775808"),
        ] {
            assert!(from_value::<ToolInvocationRevision>(invalid).is_err());
        }
    }

    #[test]
    fn ready_root_activation_is_deterministic_and_checkpoint_bound() {
        let checkpoint = checkpoint();
        let node_id = NodeId::new("authorize").unwrap();
        let first = NodeActivation::for_ready_root(&checkpoint, node_id.clone()).unwrap();
        let replayed = NodeActivation::for_ready_root(&checkpoint, node_id).unwrap();

        assert_eq!(first, replayed);
        assert_eq!(first.base_checkpoint(), &checkpoint.head());
        assert!(first.graph_namespace().is_root());
        assert_eq!(first.node_id().as_str(), "authorize");
        assert_eq!(
            first.input_digest(),
            "sha256:fe68cf4a42614bfc0bf4b41ab0fd3e552840e2a389bb61354af2dbdff086bb2c"
                .parse()
                .unwrap()
        );

        let sibling =
            NodeActivation::for_ready_root(&checkpoint, NodeId::new("reserve-stock").unwrap())
                .unwrap();
        assert_ne!(first.input_digest(), sibling.input_digest());

        assert_eq!(
            NodeActivation::for_ready_root(&checkpoint, NodeId::new("not-ready").unwrap(),),
            Err(NodeActivationError::NodeNotReady {
                node_id: NodeId::new("not-ready").unwrap(),
            })
        );
    }

    #[test]
    fn intent_is_narrowed_integrity_bound_and_secret_safe_in_debug() {
        let intent = intent();
        let restored = from_value::<ToolInvocationIntent>(to_value(&intent).unwrap()).unwrap();
        assert_eq!(restored, intent);

        let mut tampered = to_value(&intent).unwrap();
        tampered["input"]["value"]["amount"] = json!(43);
        assert!(from_value::<ToolInvocationIntent>(tampered).is_err());

        let descriptor = descriptor();
        let widened = ToolExecutionLimits::new(
            DurationMillis::new(descriptor.limits().timeout().as_i64() + 1).unwrap(),
            descriptor.limits().max_concurrency(),
            descriptor.limits().max_input_bytes(),
            descriptor.limits().max_inline_result_bytes(),
            descriptor.limits().max_artifacts(),
            descriptor.limits().max_total_artifact_bytes(),
        )
        .unwrap();
        assert_eq!(
            ToolInvocationIntent::new(
                intent.activation().clone(),
                invocation_id(),
                descriptor,
                input(),
                widened,
            ),
            Err(ToolInvocationIntentError::EffectiveLimitWidened {
                limit: ToolInvocationLimit::Timeout,
            })
        );

        let debug = format!("{intent:?}");
        assert!(!debug.contains("CNY"));
        assert!(!debug.contains("amount"));
        assert!(debug.contains("ToolInput"));
    }

    #[test]
    fn success_path_round_trips_and_history_verifies_transactionally() {
        let prepared = prepared();
        let executing = prepared
            .advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ab"),
                },
                journal(prepared.intent(), 3),
            )
            .unwrap();
        let committed = executing
            .advance(
                ToolInvocationTransition::RecordResult { result: result() },
                journal(executing.intent(), 4),
            )
            .unwrap();

        assert_eq!(prepared.status(), ToolInvocationStatus::Prepared);
        assert_eq!(executing.status(), ToolInvocationStatus::Executing);
        assert_eq!(committed.status(), ToolInvocationStatus::Committed);
        assert_eq!(committed.revision().get(), 2);
        let decoded = from_value::<ToolInvocation>(to_value(&committed).unwrap()).unwrap();
        assert_eq!(decoded.head(), committed.head());

        let mut verifier = ToolInvocationHistoryVerifier::new();
        for record in [&prepared, &executing, &committed] {
            verifier.verify_next(record).unwrap();
        }
        assert_eq!(verifier.head(), Some(committed.head()));

        assert!(matches!(
            committed.advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ac"),
                },
                journal(committed.intent(), 5),
            ),
            Err(ToolInvocationError::InvalidTransition {
                status: ToolInvocationStatus::Committed,
                transition: ToolInvocationTransitionKind::StartAttempt,
            })
        ));
    }

    #[test]
    fn unknown_outcome_cannot_retry_and_only_reconciliation_resolves_it() {
        let prepared = prepared();
        let executing = prepared
            .advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ab"),
                },
                journal(prepared.intent(), 3),
            )
            .unwrap();
        let unknown = executing
            .advance(
                ToolInvocationTransition::RecordError {
                    error: fixture_error("1"),
                },
                journal(executing.intent(), 4),
            )
            .unwrap();
        assert_eq!(unknown.status(), ToolInvocationStatus::Unknown);
        unknown.validate_reconciliation_result(&result()).unwrap();
        unknown
            .validate_reconciliation_error(&fixture_error("1"))
            .unwrap();
        assert!(matches!(
            prepared.validate_reconciliation_result(&result()),
            Err(ToolInvocationError::InvalidTransition {
                status: ToolInvocationStatus::Prepared,
                transition: ToolInvocationTransitionKind::ReconcileResult,
            })
        ));
        assert!(matches!(
            unknown.advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ac"),
                },
                journal(unknown.intent(), 5),
            ),
            Err(ToolInvocationError::InvalidTransition {
                status: ToolInvocationStatus::Unknown,
                transition: ToolInvocationTransitionKind::StartAttempt,
            })
        ));

        let reconciled = unknown
            .advance(
                ToolInvocationTransition::ReconcileResult { result: result() },
                journal(unknown.intent(), 5),
            )
            .unwrap();
        assert_eq!(reconciled.status(), ToolInvocationStatus::Committed);
        assert!(matches!(
            reconciled.validate_reconciliation_error(&fixture_error("1")),
            Err(ToolInvocationError::InvalidTransition {
                status: ToolInvocationStatus::Committed,
                transition: ToolInvocationTransitionKind::ReconcileError,
            })
        ));
    }

    #[test]
    fn retry_requires_explicit_safety_and_elapsed_delay() {
        let prepared = prepared();
        let executing = prepared
            .advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ab"),
                },
                journal(prepared.intent(), 3),
            )
            .unwrap();
        let failed = executing
            .advance(
                ToolInvocationTransition::RecordError {
                    error: safe_error(2_000),
                },
                journal(executing.intent(), 4),
            )
            .unwrap();

        let early_head = JournalHead::new(
            failed.intent().tenant_id().clone(),
            failed.intent().run_id(),
            JournalSequence::new(5).unwrap(),
            "01912345-6789-7abc-8def-0123456789c5".parse().unwrap(),
            Timestamp::from_unix_micros(
                failed.journal_head().recorded_at().unix_micros() + 1_000_000,
            )
            .unwrap(),
            Digest::sha256(b"early"),
        );
        assert!(matches!(
            failed.advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ac"),
                },
                early_head,
            ),
            Err(ToolInvocationError::RetryDelayNotElapsed { .. })
        ));

        let retried = failed
            .advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ac"),
                },
                journal(failed.intent(), 6),
            )
            .unwrap();
        assert_eq!(retried.attempt_id(), Some(attempt("ac")));

        let never_failed = executing
            .advance(
                ToolInvocationTransition::RecordError {
                    error: fixture_error("0"),
                },
                journal(executing.intent(), 4),
            )
            .unwrap();
        assert!(matches!(
            never_failed.advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ac"),
                },
                journal(never_failed.intent(), 5),
            ),
            Err(ToolInvocationError::RetryNotAuthorized)
        ));
    }

    #[test]
    fn tampering_and_branch_substitution_fail_closed() {
        let prepared = prepared();
        let executing = prepared
            .advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ab"),
                },
                journal(prepared.intent(), 3),
            )
            .unwrap();
        let mut wire = to_value(&executing).unwrap();
        wire["state"]["attempt_id"] = json!(attempt("ac"));
        assert!(from_value::<ToolInvocation>(wire).is_err());

        let mut wire = to_value(&executing).unwrap();
        wire["transition_digest"] = json!(Digest::sha256(b"wrong-transition"));
        assert!(from_value::<ToolInvocation>(wire).is_err());

        let branch = prepared
            .advance(
                ToolInvocationTransition::StartAttempt {
                    attempt_id: attempt("ac"),
                },
                journal(prepared.intent(), 4),
            )
            .unwrap();
        let mut verifier = ToolInvocationHistoryVerifier::new();
        verifier.verify_next(&prepared).unwrap();
        verifier.verify_next(&executing).unwrap();
        let accepted = verifier.head();
        assert_eq!(
            verifier.verify_next(&branch),
            Err(ToolInvocationHistoryError::PreviousHeadMismatch)
        );
        assert_eq!(verifier.head(), accepted);
    }

    #[test]
    fn public_object_schemas_are_closed() {
        for schema in [
            to_value(schema_for!(NodeActivation)).unwrap(),
            to_value(schema_for!(ToolInvocationIntent)).unwrap(),
            to_value(schema_for!(ToolInvocationHead)).unwrap(),
            to_value(schema_for!(ToolInvocation)).unwrap(),
        ] {
            assert_eq!(schema["additionalProperties"], false);
        }
    }
}
