// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Immutable, integrity-bound graph checkpoints for deterministic recovery.
//!
//! A checkpoint is a committed superstep barrier, not a mutable scheduler
//! snapshot. Its state, next ready-node set, graph definition, predecessor,
//! and exact journal head are all checksummed. Store implementations must
//! commit a checkpoint and its anchoring journal event in one transaction and
//! compare the predecessor with the locked current checkpoint head.

use std::{
    collections::{BTreeSet, btree_set},
    fmt,
    str::FromStr,
};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess},
};
use thiserror::Error;

use crate::decimal::{UnsignedDecimalError, parse_bounded_u64};
use crate::{
    BoundedJson, CanonicalJson, CanonicalJsonError, CapabilityIdentity, CheckpointId, Digest,
    JournalHead, JournalSequence, JsonLimits, RunId, SchemaReference, TenantId, Timestamp,
};

const MAX_DATABASE_ORDINAL: u64 = i64::MAX as u64;
const SUPERSTEP_PATTERN: &str = "^(0|[1-9][0-9]{0,18})$";
const NODE_ID_PATTERN: &str = "^(?!\\.{1,2}$)[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$";
const STATE_DIGEST_DOMAIN: &[u8] = b"stateknot-checkpoint-state-v1\0";
const INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-checkpoint-intent-v1\0";
const CHECKPOINT_DIGEST_DOMAIN: &[u8] = b"stateknot-checkpoint-v1\0";

/// Zero-based graph superstep committed at a deterministic barrier.
///
/// The maximum matches a signed `PostgreSQL BIGINT`. Its JSON representation
/// is a canonical decimal string so every supported language transports the
/// full range without numeric precision loss.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Superstep(u64);

impl Superstep {
    /// Initial graph checkpoint position.
    pub const INITIAL: Self = Self(0);

    /// Largest position supported by the v1 storage contract.
    pub const MAX: Self = Self(MAX_DATABASE_ORDINAL);

    /// Constructs a `PostgreSQL`-compatible superstep.
    ///
    /// # Errors
    ///
    /// Returns [`SuperstepError::AboveMaximum`] when `value` exceeds signed
    /// `BIGINT`.
    pub const fn new(value: u64) -> Result<Self, SuperstepError> {
        if value > MAX_DATABASE_ORDINAL {
            return Err(SuperstepError::AboveMaximum);
        }
        Ok(Self(value))
    }

    /// Returns the zero-based integer position.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the exact successor or `None` at the storage ceiling.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        if self.0 == MAX_DATABASE_ORDINAL {
            None
        } else {
            Some(Self(self.0 + 1))
        }
    }
}

impl fmt::Display for Superstep {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for Superstep {
    type Err = SuperstepError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = parse_bounded_u64(value, MAX_DATABASE_ORDINAL)
            .map_err(SuperstepError::from_decimal_error)?;
        Self::new(value)
    }
}

impl Serialize for Superstep {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Superstep {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SuperstepVisitor)
    }
}

struct SuperstepVisitor;

impl de::Visitor<'_> for SuperstepVisitor {
    type Value = Superstep;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical non-negative decimal PostgreSQL BIGINT superstep")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

impl JsonSchema for Superstep {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Superstep".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Superstep").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 19,
            "pattern": SUPERSTEP_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid canonical superstep value.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SuperstepError {
    /// The wire value was empty or contained a non-decimal byte.
    #[error("superstep must contain only unsigned ASCII decimal digits")]
    InvalidFormat,

    /// The wire value contained a leading zero.
    #[error("superstep must use canonical decimal text")]
    NonCanonical,

    /// The value exceeded signed `PostgreSQL BIGINT`.
    #[error("superstep exceeds the PostgreSQL BIGINT maximum")]
    AboveMaximum,
}

impl SuperstepError {
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

/// Stable, case-sensitive identity of one compiled graph node.
///
/// Node identities are durable schema, not display labels. Renaming a node is
/// therefore a graph migration. The bounded ASCII grammar is safe for logs,
/// maps, and database indexes while deliberately excluding path separators.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeId(Box<str>);

impl NodeId {
    /// Maximum encoded length in bytes.
    pub const MAX_LEN: usize = 128;

    /// Validates and constructs a node identity.
    ///
    /// # Errors
    ///
    /// Returns [`NodeIdError`] for empty, oversized, path-like, or unsupported
    /// identifier text.
    pub fn new(value: impl Into<String>) -> Result<Self, NodeIdError> {
        let value = value.into();
        validate_node_id(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact case-sensitive identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("NodeId")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for NodeId {
    type Err = NodeIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for NodeId {
    type Error = NodeIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for NodeId {
    type Error = NodeIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for NodeId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(NodeIdVisitor)
    }
}

struct NodeIdVisitor;

impl de::Visitor<'_> for NodeIdVisitor {
    type Value = NodeId;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded canonical StateKnot graph node identifier")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        NodeId::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        NodeId::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for NodeId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NodeId".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::NodeId").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 128,
            "pattern": NODE_ID_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

fn validate_node_id(value: &str) -> Result<(), NodeIdError> {
    if value.is_empty() {
        return Err(NodeIdError::Empty);
    }
    if value.len() > NodeId::MAX_LEN {
        return Err(NodeIdError::TooLong {
            max: NodeId::MAX_LEN,
            actual: value.len(),
        });
    }
    if matches!(value, "." | "..") {
        return Err(NodeIdError::PathLike);
    }
    if !value.as_bytes()[0].is_ascii_alphanumeric() {
        return Err(NodeIdError::InvalidByte { index: 0 });
    }
    if let Some((index, _)) = value
        .bytes()
        .enumerate()
        .find(|(_, byte)| !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(NodeIdError::InvalidByte { index });
    }
    Ok(())
}

/// Invalid graph node identity.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeIdError {
    /// The identity contained no bytes.
    #[error("node identifier must not be empty")]
    Empty,

    /// The identity exceeded [`NodeId::MAX_LEN`].
    #[error("node identifier is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The identity was `.` or `..`.
    #[error("node identifier must not be path-like")]
    PathLike,

    /// A byte did not belong to the stable ASCII grammar.
    #[error("node identifier contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first invalid byte.
        index: usize,
    },
}

/// Deterministically ordered set of nodes eligible for the next superstep.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadyNodes(BTreeSet<NodeId>);

impl ReadyNodes {
    /// Hard maximum number of ready nodes in one checkpoint.
    pub const MAX_LEN: usize = 1024;

    /// Constructs a bounded set, rejecting duplicate identities.
    ///
    /// # Errors
    ///
    /// Returns [`ReadyNodesError`] for a duplicate or more than
    /// [`ReadyNodes::MAX_LEN`] identities.
    pub fn try_new(nodes: impl IntoIterator<Item = NodeId>) -> Result<Self, ReadyNodesError> {
        let mut values = BTreeSet::new();
        for node in nodes {
            if values.contains(&node) {
                return Err(ReadyNodesError::Duplicate { node });
            }
            if values.len() == Self::MAX_LEN {
                return Err(ReadyNodesError::TooMany {
                    maximum: Self::MAX_LEN,
                    actual: Self::MAX_LEN + 1,
                });
            }
            values.insert(node);
        }
        Ok(Self(values))
    }

    /// Returns an empty ready set for a terminal or waiting barrier.
    #[must_use]
    pub const fn empty() -> Self {
        Self(BTreeSet::new())
    }

    /// Returns the number of ready nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` when no node is immediately runnable.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns node identities in deterministic ASCII byte order.
    pub fn iter(&self) -> btree_set::Iter<'_, NodeId> {
        self.0.iter()
    }

    /// Returns `true` when the exact node identity is present.
    #[must_use]
    pub fn contains(&self, node: &NodeId) -> bool {
        self.0.contains(node)
    }
}

impl Default for ReadyNodes {
    fn default() -> Self {
        Self::empty()
    }
}

impl<'a> IntoIterator for &'a ReadyNodes {
    type Item = &'a NodeId;
    type IntoIter = btree_set::Iter<'a, NodeId>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<Vec<NodeId>> for ReadyNodes {
    type Error = ReadyNodesError;

    fn try_from(nodes: Vec<NodeId>) -> Result<Self, Self::Error> {
        Self::try_new(nodes)
    }
}

impl Serialize for ReadyNodes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for ReadyNodes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ReadyNodesVisitor)
    }
}

struct ReadyNodesVisitor;

impl<'de> de::Visitor<'de> for ReadyNodesVisitor {
    type Value = ReadyNodes;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {} unique graph node identifiers",
            ReadyNodes::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = BTreeSet::new();
        while let Some(node) = sequence.next_element::<NodeId>()? {
            if values.contains(&node) {
                return Err(de::Error::custom(ReadyNodesError::Duplicate { node }));
            }
            if values.len() == ReadyNodes::MAX_LEN {
                return Err(de::Error::custom(ReadyNodesError::TooMany {
                    maximum: ReadyNodes::MAX_LEN,
                    actual: ReadyNodes::MAX_LEN + 1,
                }));
            }
            values.insert(node);
        }
        Ok(ReadyNodes(values))
    }
}

impl JsonSchema for ReadyNodes {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ReadyNodes".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ReadyNodes").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<NodeId>(),
            "maxItems": 1024,
            "uniqueItems": true,
            "description": "The wire array is serialized in deterministic ascending NodeId order. Runtime rejects duplicate identities."
        })
    }
}

/// Invalid next-superstep ready set.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ReadyNodesError {
    /// The input repeated a node identity.
    #[error("ready-node set contains duplicate node {node:?}")]
    Duplicate {
        /// Repeated node identity.
        node: NodeId,
    },

    /// The hard ready-node ceiling was exceeded.
    #[error("ready-node set contains {actual} nodes; maximum is {maximum}")]
    TooMany {
        /// Absolute maximum.
        maximum: usize,
        /// First observed count beyond the maximum.
        actual: usize,
    },
}

/// Exact graph definition and state schema required to resume a checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphReference {
    identity: CapabilityIdentity,
    definition_digest: Digest,
    state_schema: SchemaReference,
}

impl GraphReference {
    /// Constructs an immutable graph reference.
    #[must_use]
    pub const fn new(
        identity: CapabilityIdentity,
        definition_digest: Digest,
        state_schema: SchemaReference,
    ) -> Self {
        Self {
            identity,
            definition_digest,
            state_schema,
        }
    }

    /// Returns the owner-qualified graph capability identity.
    #[must_use]
    pub const fn identity(&self) -> &CapabilityIdentity {
        &self.identity
    }

    /// Returns the checksum of the canonical compiled graph definition.
    #[must_use]
    pub const fn definition_digest(&self) -> Digest {
        self.definition_digest
    }

    /// Returns the exact schema required for checkpoint state.
    #[must_use]
    pub const fn state_schema(&self) -> &SchemaReference {
        &self.state_schema
    }
}

/// Immutable, schema-pinned JSON graph state with a verified checksum.
///
/// State JSON is materialized under [`JsonLimits::MAXIMUM`] (currently 2 MiB)
/// and must still be validated against [`Self::schema`] by the graph schema
/// registry before execution. The checksum binds both schema identity and RFC
/// 8785 state bytes.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointState {
    schema: SchemaReference,
    data: BoundedJson,
    digest: Digest,
}

impl CheckpointState {
    /// Constructs state and computes its domain-separated checksum.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStateError::Integrity`] when the state cannot be
    /// represented as RFC 8785 interoperable JSON.
    pub fn new(schema: SchemaReference, data: BoundedJson) -> Result<Self, CheckpointStateError> {
        let digest =
            compute_state_digest(&schema, &data).map_err(CheckpointStateError::integrity)?;
        Ok(Self {
            schema,
            data,
            digest,
        })
    }

    /// Restores state from durable columns and verifies its checksum.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStateError`] when canonicalization fails or the
    /// supplied checksum does not match the schema and state bytes.
    pub fn restore(
        schema: SchemaReference,
        data: BoundedJson,
        digest: Digest,
    ) -> Result<Self, CheckpointStateError> {
        let expected =
            compute_state_digest(&schema, &data).map_err(CheckpointStateError::integrity)?;
        if digest != expected {
            return Err(CheckpointStateError::DigestMismatch);
        }
        Ok(Self {
            schema,
            data,
            digest,
        })
    }

    /// Returns the exact state schema reference.
    #[must_use]
    pub const fn schema(&self) -> &SchemaReference {
        &self.schema
    }

    /// Borrows validated resource-bounded state JSON.
    #[must_use]
    pub const fn data(&self) -> &BoundedJson {
        &self.data
    }

    /// Returns the checksum binding schema and canonical state bytes.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl fmt::Debug for CheckpointState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointState")
            .field("schema", &self.schema)
            .field("stats", &self.data.stats())
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for CheckpointState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema: SchemaReference,
            #[serde(deserialize_with = "deserialize_checkpoint_data")]
            data: BoundedJson,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.schema, wire.data, wire.digest).map_err(de::Error::custom)
    }
}

fn deserialize_checkpoint_data<'de, D>(deserializer: D) -> Result<BoundedJson, D::Error>
where
    D: Deserializer<'de>,
{
    BoundedJson::deserialize_with_limits(deserializer, JsonLimits::MAXIMUM)
}

/// Invalid or corrupted checkpoint state.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointStateError {
    /// Integrity material could not be canonicalized.
    #[error("checkpoint state integrity calculation failed: {source}")]
    Integrity {
        /// Underlying integrity failure.
        #[source]
        source: CheckpointIntegrityError,
    },

    /// The persisted checksum did not match state fields.
    #[error("checkpoint state digest does not match its schema and data")]
    DigestMismatch,
}

impl CheckpointStateError {
    const fn integrity(source: CheckpointIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Compact exact identity of a previously validated committed checkpoint.
///
/// A head is suitable for optimistic comparison but is not independently
/// self-validating because it omits state. Obtain it from [`Checkpoint::head`]
/// or trusted storage that has first restored the full checkpoint.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointHead {
    tenant_id: TenantId,
    run_id: RunId,
    checkpoint_id: CheckpointId,
    superstep: Superstep,
    graph: GraphReference,
    journal_head: JournalHead,
    digest: Digest,
}

impl CheckpointHead {
    /// Constructs a trusted head while enforcing its tenant/run scope.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointHeadError`] when the journal head crosses the
    /// checkpoint tenant or run boundary.
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        checkpoint_id: CheckpointId,
        superstep: Superstep,
        graph: GraphReference,
        journal_head: JournalHead,
        digest: Digest,
    ) -> Result<Self, CheckpointHeadError> {
        validate_journal_scope(&tenant_id, run_id, &journal_head)
            .map_err(CheckpointHeadError::from_scope)?;
        Ok(Self {
            tenant_id,
            run_id,
            checkpoint_id,
            superstep,
            graph,
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

    /// Returns the immutable checkpoint identity.
    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    /// Returns the committed barrier position.
    #[must_use]
    pub const fn superstep(&self) -> Superstep {
        self.superstep
    }

    /// Returns the exact graph definition reference.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }

    /// Returns the exact journal prefix included in this checkpoint.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the committed checkpoint checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for CheckpointHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            checkpoint_id: CheckpointId,
            superstep: Superstep,
            graph: GraphReference,
            journal_head: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.tenant_id,
            wire.run_id,
            wire.checkpoint_id,
            wire.superstep,
            wire.graph,
            wire.journal_head,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid scope in a compact checkpoint head.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointHeadError {
    /// The journal head crossed a tenant boundary.
    #[error("checkpoint journal head crosses the checkpoint tenant boundary")]
    JournalTenantMismatch,

    /// The journal head named another run.
    #[error("checkpoint journal head does not belong to the checkpoint run")]
    JournalRunMismatch,
}

impl CheckpointHeadError {
    const fn from_scope(error: JournalScopeError) -> Self {
        match error {
            JournalScopeError::Tenant => Self::JournalTenantMismatch,
            JournalScopeError::Run => Self::JournalRunMismatch,
        }
    }
}

/// Stable checkpoint write intent before the anchoring journal event commits.
///
/// The intent checksum is the idempotency fingerprint for checkpoint storage.
/// It deliberately excludes the future journal head while binding every
/// caller-controlled field and the exact predecessor.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointWrite {
    tenant_id: TenantId,
    run_id: RunId,
    checkpoint_id: CheckpointId,
    superstep: Superstep,
    graph: GraphReference,
    state: CheckpointState,
    ready_nodes: ReadyNodes,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<CheckpointHead>,
    intent_digest: Digest,
}

impl CheckpointWrite {
    /// Constructs the initial superstep-zero checkpoint intent.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointWriteError`] when the state schema differs from the
    /// graph reference or integrity serialization fails.
    pub fn initial(
        tenant_id: TenantId,
        run_id: RunId,
        checkpoint_id: CheckpointId,
        graph: GraphReference,
        state: CheckpointState,
        ready_nodes: ReadyNodes,
    ) -> Result<Self, CheckpointWriteError> {
        Self::build(
            tenant_id,
            run_id,
            checkpoint_id,
            Superstep::INITIAL,
            graph,
            state,
            ready_nodes,
            None,
            None,
        )
    }

    /// Constructs the exact successor of a validated committed checkpoint.
    ///
    /// The graph reference and scope are inherited rather than accepted from
    /// the caller, preventing a recovery chain from silently switching graph
    /// code or tenant identity.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointWriteError`] for position exhaustion, a reused
    /// checkpoint ID, a mismatched state schema, or integrity failure.
    pub fn successor(
        checkpoint_id: CheckpointId,
        parent: &Checkpoint,
        state: CheckpointState,
        ready_nodes: ReadyNodes,
    ) -> Result<Self, CheckpointWriteError> {
        let superstep = parent
            .superstep
            .checked_next()
            .ok_or(CheckpointWriteError::SuperstepOverflow)?;
        Self::build(
            parent.tenant_id.clone(),
            parent.run_id,
            checkpoint_id,
            superstep,
            parent.graph.clone(),
            state,
            ready_nodes,
            Some(parent.head()),
            None,
        )
    }

    /// Restores a write intent from durable columns and verifies its checksum.
    ///
    /// This constructor exists for storage providers that map fields to typed
    /// columns instead of round-tripping the public JSON envelope.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointWriteError`] for any invalid initial/successor
    /// shape, crossed scope, graph/schema mismatch, or checksum mismatch.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        tenant_id: TenantId,
        run_id: RunId,
        checkpoint_id: CheckpointId,
        superstep: Superstep,
        graph: GraphReference,
        state: CheckpointState,
        ready_nodes: ReadyNodes,
        parent: Option<CheckpointHead>,
        intent_digest: Digest,
    ) -> Result<Self, CheckpointWriteError> {
        Self::build(
            tenant_id,
            run_id,
            checkpoint_id,
            superstep,
            graph,
            state,
            ready_nodes,
            parent,
            Some(intent_digest),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        tenant_id: TenantId,
        run_id: RunId,
        checkpoint_id: CheckpointId,
        superstep: Superstep,
        graph: GraphReference,
        state: CheckpointState,
        ready_nodes: ReadyNodes,
        parent: Option<CheckpointHead>,
        supplied_intent_digest: Option<Digest>,
    ) -> Result<Self, CheckpointWriteError> {
        validate_write_shape(
            &tenant_id,
            run_id,
            checkpoint_id,
            superstep,
            &graph,
            &state,
            parent.as_ref(),
        )?;
        let intent_digest = compute_intent_digest(&CheckpointIntentDigestWire {
            tenant_id: &tenant_id,
            run_id,
            checkpoint_id,
            superstep,
            graph: &graph,
            state_digest: state.digest,
            ready_nodes: &ready_nodes,
            parent: parent.as_ref(),
        })
        .map_err(CheckpointWriteError::integrity)?;
        if supplied_intent_digest.is_some_and(|supplied| supplied != intent_digest) {
            return Err(CheckpointWriteError::IntentDigestMismatch);
        }
        Ok(Self {
            tenant_id,
            run_id,
            checkpoint_id,
            superstep,
            graph,
            state,
            ready_nodes,
            parent,
            intent_digest,
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

    /// Returns the new immutable checkpoint identity.
    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    /// Returns the intended barrier position.
    #[must_use]
    pub const fn superstep(&self) -> Superstep {
        self.superstep
    }

    /// Returns the exact graph definition reference.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }

    /// Returns the immutable graph state.
    #[must_use]
    pub const fn state(&self) -> &CheckpointState {
        &self.state
    }

    /// Returns the sorted next-superstep ready set.
    #[must_use]
    pub const fn ready_nodes(&self) -> &ReadyNodes {
        &self.ready_nodes
    }

    /// Returns the exact predecessor, absent only at superstep zero.
    #[must_use]
    pub const fn parent(&self) -> Option<&CheckpointHead> {
        self.parent.as_ref()
    }

    /// Returns the stable idempotency checksum for this write.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }
}

impl fmt::Debug for CheckpointWrite {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointWrite")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("checkpoint_id", &self.checkpoint_id)
            .field("superstep", &self.superstep)
            .field("graph", &self.graph)
            .field("state", &self.state)
            .field("ready_nodes", &self.ready_nodes)
            .field("parent", &self.parent)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for CheckpointWrite {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            checkpoint_id: CheckpointId,
            superstep: Superstep,
            graph: GraphReference,
            state: CheckpointState,
            ready_nodes: ReadyNodes,
            parent: Option<CheckpointHead>,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::build(
            wire.tenant_id,
            wire.run_id,
            wire.checkpoint_id,
            wire.superstep,
            wire.graph,
            wire.state,
            wire.ready_nodes,
            wire.parent,
            Some(wire.intent_digest),
        )
        .map_err(de::Error::custom)
    }
}

/// Structurally invalid or corrupted checkpoint write intent.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointWriteError {
    /// Checkpoint state named a schema other than the graph's pinned schema.
    #[error("checkpoint state schema does not match the graph state schema")]
    StateSchemaMismatch,

    /// Superstep zero unexpectedly named a predecessor.
    #[error("initial checkpoint must not contain a parent")]
    InitialHasParent,

    /// A positive superstep omitted its predecessor.
    #[error("non-initial checkpoint must contain a parent")]
    SuccessorMissingParent,

    /// The predecessor crossed a tenant boundary.
    #[error("checkpoint parent crosses the checkpoint tenant boundary")]
    ParentTenantMismatch,

    /// The predecessor named another run.
    #[error("checkpoint parent does not belong to the checkpoint run")]
    ParentRunMismatch,

    /// The successor reused its predecessor's immutable identity.
    #[error("checkpoint successor must use a new checkpoint identifier")]
    ReusedCheckpointId,

    /// The predecessor had no representable successor.
    #[error("checkpoint superstep overflowed")]
    SuperstepOverflow,

    /// The supplied position was not the predecessor's exact successor.
    #[error("checkpoint superstep is {actual}; expected {expected}")]
    NonContiguousSuperstep {
        /// Exact required successor.
        expected: Superstep,
        /// Rejected position.
        actual: Superstep,
    },

    /// The graph definition differed from its predecessor.
    #[error("checkpoint graph reference differs from its parent")]
    ParentGraphMismatch,

    /// Integrity material could not be canonicalized.
    #[error("checkpoint intent integrity calculation failed: {source}")]
    Integrity {
        /// Underlying integrity failure.
        #[source]
        source: CheckpointIntegrityError,
    },

    /// The persisted intent checksum did not match caller-controlled fields.
    #[error("checkpoint intent digest does not match its fields")]
    IntentDigestMismatch,
}

impl CheckpointWriteError {
    const fn integrity(source: CheckpointIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// One committed, self-validating graph checkpoint.
///
/// The full checksum binds the validated write intent to the exact committed
/// journal head. Restoring this value verifies state, intent, predecessor, and
/// checkpoint integrity before any graph code may observe it.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    tenant_id: TenantId,
    run_id: RunId,
    checkpoint_id: CheckpointId,
    superstep: Superstep,
    graph: GraphReference,
    state: CheckpointState,
    ready_nodes: ReadyNodes,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent: Option<CheckpointHead>,
    intent_digest: Digest,
    journal_head: JournalHead,
    digest: Digest,
}

impl Checkpoint {
    /// Materializes a checkpoint after its anchoring event has committed.
    ///
    /// Stores must call this using the journal head produced inside the same
    /// transaction that inserts the checkpoint and advances the run head.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] for a crossed journal scope, a journal head
    /// that did not advance beyond the parent, clock regression, or integrity
    /// serialization failure.
    pub fn commit(
        write: CheckpointWrite,
        journal_head: JournalHead,
    ) -> Result<Self, CheckpointError> {
        Self::build(write, journal_head, None)
    }

    /// Restores a full checkpoint and verifies its persisted checksum.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] for any structural, chronology, scope, or
    /// checksum mismatch.
    pub fn restore(
        write: CheckpointWrite,
        journal_head: JournalHead,
        digest: Digest,
    ) -> Result<Self, CheckpointError> {
        Self::build(write, journal_head, Some(digest))
    }

    fn build(
        write: CheckpointWrite,
        journal_head: JournalHead,
        supplied_digest: Option<Digest>,
    ) -> Result<Self, CheckpointError> {
        validate_journal_scope(&write.tenant_id, write.run_id, &journal_head)
            .map_err(CheckpointError::from_scope)?;
        if let Some(parent) = &write.parent {
            if journal_head.sequence() <= parent.journal_head.sequence() {
                return Err(CheckpointError::JournalDidNotAdvance {
                    previous: parent.journal_head.sequence(),
                    actual: journal_head.sequence(),
                });
            }
            if journal_head.recorded_at() < parent.journal_head.recorded_at() {
                return Err(CheckpointError::ClockRegression {
                    previous: parent.journal_head.recorded_at(),
                    actual: journal_head.recorded_at(),
                });
            }
        }

        let digest = compute_checkpoint_digest(&CheckpointDigestWire {
            intent_digest: write.intent_digest,
            journal_head: &journal_head,
        })
        .map_err(CheckpointError::integrity)?;
        if supplied_digest.is_some_and(|supplied| supplied != digest) {
            return Err(CheckpointError::DigestMismatch);
        }

        Ok(Self {
            tenant_id: write.tenant_id,
            run_id: write.run_id,
            checkpoint_id: write.checkpoint_id,
            superstep: write.superstep,
            graph: write.graph,
            state: write.state,
            ready_nodes: write.ready_nodes,
            parent: write.parent,
            intent_digest: write.intent_digest,
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

    /// Returns the immutable checkpoint identity.
    #[must_use]
    pub const fn checkpoint_id(&self) -> CheckpointId {
        self.checkpoint_id
    }

    /// Returns the committed barrier position.
    #[must_use]
    pub const fn superstep(&self) -> Superstep {
        self.superstep
    }

    /// Returns the exact graph definition reference.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }

    /// Returns the immutable graph state.
    #[must_use]
    pub const fn state(&self) -> &CheckpointState {
        &self.state
    }

    /// Returns the sorted next-superstep ready set.
    #[must_use]
    pub const fn ready_nodes(&self) -> &ReadyNodes {
        &self.ready_nodes
    }

    /// Returns the exact predecessor, absent only at superstep zero.
    #[must_use]
    pub const fn parent(&self) -> Option<&CheckpointHead> {
        self.parent.as_ref()
    }

    /// Returns the stable write-intent checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the exact journal prefix included in this checkpoint.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the complete committed checkpoint checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns a compact predecessor/head value for optimistic comparison.
    #[must_use]
    pub fn head(&self) -> CheckpointHead {
        CheckpointHead {
            tenant_id: self.tenant_id.clone(),
            run_id: self.run_id,
            checkpoint_id: self.checkpoint_id,
            superstep: self.superstep,
            graph: self.graph.clone(),
            journal_head: self.journal_head.clone(),
            digest: self.digest,
        }
    }

    /// Returns `true` when this checkpoint materialized the exact write intent.
    #[must_use]
    pub fn matches_write(&self, write: &CheckpointWrite) -> bool {
        self.tenant_id == write.tenant_id
            && self.run_id == write.run_id
            && self.checkpoint_id == write.checkpoint_id
            && self.superstep == write.superstep
            && self.graph == write.graph
            && self.state == write.state
            && self.ready_nodes == write.ready_nodes
            && self.parent == write.parent
            && self.intent_digest == write.intent_digest
    }

    /// Reconstructs the verified write intent that produced this checkpoint.
    #[must_use]
    pub fn write_intent(&self) -> CheckpointWrite {
        CheckpointWrite {
            tenant_id: self.tenant_id.clone(),
            run_id: self.run_id,
            checkpoint_id: self.checkpoint_id,
            superstep: self.superstep,
            graph: self.graph.clone(),
            state: self.state.clone(),
            ready_nodes: self.ready_nodes.clone(),
            parent: self.parent.clone(),
            intent_digest: self.intent_digest,
        }
    }
}

impl fmt::Debug for Checkpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Checkpoint")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("checkpoint_id", &self.checkpoint_id)
            .field("superstep", &self.superstep)
            .field("graph", &self.graph)
            .field("state", &self.state)
            .field("ready_nodes", &self.ready_nodes)
            .field("parent", &self.parent)
            .field("intent_digest", &self.intent_digest)
            .field("journal_head", &self.journal_head)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for Checkpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            checkpoint_id: CheckpointId,
            superstep: Superstep,
            graph: GraphReference,
            state: CheckpointState,
            ready_nodes: ReadyNodes,
            parent: Option<CheckpointHead>,
            intent_digest: Digest,
            journal_head: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        let write = CheckpointWrite::build(
            wire.tenant_id,
            wire.run_id,
            wire.checkpoint_id,
            wire.superstep,
            wire.graph,
            wire.state,
            wire.ready_nodes,
            wire.parent,
            Some(wire.intent_digest),
        )
        .map_err(de::Error::custom)?;
        Self::restore(write, wire.journal_head, wire.digest).map_err(de::Error::custom)
    }
}

/// Invalid or corrupted committed checkpoint.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointError {
    /// The journal head crossed a tenant boundary.
    #[error("checkpoint journal head crosses the checkpoint tenant boundary")]
    JournalTenantMismatch,

    /// The journal head named another run.
    #[error("checkpoint journal head does not belong to the checkpoint run")]
    JournalRunMismatch,

    /// The successor checkpoint did not include any newer journal fact.
    #[error("checkpoint journal sequence is {actual}; it must be greater than {previous}")]
    JournalDidNotAdvance {
        /// Parent checkpoint's journal sequence.
        previous: JournalSequence,
        /// Rejected successor journal sequence.
        actual: JournalSequence,
    },

    /// Durable journal time regressed across checkpoints.
    #[error("checkpoint journal time {actual} precedes parent time {previous}")]
    ClockRegression {
        /// Parent journal observation.
        previous: Timestamp,
        /// Rejected successor observation.
        actual: Timestamp,
    },

    /// Integrity material could not be canonicalized.
    #[error("checkpoint integrity calculation failed: {source}")]
    Integrity {
        /// Underlying integrity failure.
        #[source]
        source: CheckpointIntegrityError,
    },

    /// The persisted complete checksum did not match checkpoint fields.
    #[error("checkpoint digest does not match its intent and journal head")]
    DigestMismatch,
}

impl CheckpointError {
    const fn from_scope(error: JournalScopeError) -> Self {
        match error {
            JournalScopeError::Tenant => Self::JournalTenantMismatch,
            JournalScopeError::Run => Self::JournalRunMismatch,
        }
    }

    const fn integrity(source: CheckpointIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Streaming verifier for a checkpoint lineage read from newest to oldest.
///
/// The verifier starts from an exact trusted tip and accepts only the full
/// checkpoint named by that head, followed by its exact parent, until the
/// superstep-zero root is reached. It buffers one compact head rather than the
/// checkpoint state history, so stores can validate arbitrarily long lineages
/// through bounded reverse pages. Rejection leaves the verifier unchanged.
#[derive(Debug)]
pub struct CheckpointLineageVerifier {
    expected: Option<CheckpointHead>,
}

impl CheckpointLineageVerifier {
    /// Constructs a reverse-lineage verifier from an exact checkpoint tip.
    #[must_use]
    pub const fn from_tip(tip: CheckpointHead) -> Self {
        Self {
            expected: Some(tip),
        }
    }

    /// Returns the exact checkpoint head required next, or `None` at the root.
    #[must_use]
    pub const fn expected(&self) -> Option<&CheckpointHead> {
        self.expected.as_ref()
    }

    /// Returns whether the complete lineage through superstep zero was accepted.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.expected.is_none()
    }

    /// Verifies and accepts the next checkpoint in newest-to-oldest order.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointLineageError::HeadMismatch`] when `checkpoint` is
    /// not the exact full value named by the expected head, or
    /// [`CheckpointLineageError::AlreadyComplete`] after the root was accepted.
    pub fn verify_next(&mut self, checkpoint: &Checkpoint) -> Result<(), CheckpointLineageError> {
        let Some(expected) = &self.expected else {
            return Err(CheckpointLineageError::AlreadyComplete);
        };
        if checkpoint.head() != *expected {
            return Err(CheckpointLineageError::HeadMismatch);
        }
        self.expected.clone_from(&checkpoint.parent);
        Ok(())
    }
}

/// Rejected checkpoint in reverse lineage verification.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointLineageError {
    /// A checkpoint did not materialize the exact expected compact head.
    #[error("checkpoint does not match the expected lineage head")]
    HeadMismatch,

    /// Another checkpoint appeared after the superstep-zero root.
    #[error("checkpoint lineage is already complete")]
    AlreadyComplete,
}

/// Failure to produce a domain-separated checkpoint checksum.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CheckpointIntegrityError {
    /// State JSON was outside the RFC 8785 interoperable domain.
    #[error("checkpoint state canonicalization failed: {source}")]
    StateCanonicalization {
        /// Exact canonical JSON failure.
        #[source]
        source: CanonicalJsonError,
    },

    /// Canonical serialization of a closed typed checksum preimage failed.
    #[error("checkpoint checksum preimage canonical serialization failed")]
    CanonicalSerialization,
}

#[derive(Serialize)]
struct StateDigestWire<'a> {
    schema: &'a SchemaReference,
    data_digest: Digest,
}

#[derive(Serialize)]
struct CheckpointIntentDigestWire<'a> {
    tenant_id: &'a TenantId,
    run_id: RunId,
    checkpoint_id: CheckpointId,
    superstep: Superstep,
    graph: &'a GraphReference,
    state_digest: Digest,
    ready_nodes: &'a ReadyNodes,
    parent: Option<&'a CheckpointHead>,
}

#[derive(Serialize)]
struct CheckpointDigestWire<'a> {
    intent_digest: Digest,
    journal_head: &'a JournalHead,
}

fn compute_state_digest(
    schema: &SchemaReference,
    data: &BoundedJson,
) -> Result<Digest, CheckpointIntegrityError> {
    let canonical = CanonicalJson::new(data)
        .map_err(|source| CheckpointIntegrityError::StateCanonicalization { source })?;
    domain_separated_digest(
        STATE_DIGEST_DOMAIN,
        &StateDigestWire {
            schema,
            data_digest: canonical.digest(),
        },
    )
}

fn compute_intent_digest(
    value: &CheckpointIntentDigestWire<'_>,
) -> Result<Digest, CheckpointIntegrityError> {
    domain_separated_digest(INTENT_DIGEST_DOMAIN, value)
}

fn compute_checkpoint_digest(
    value: &CheckpointDigestWire<'_>,
) -> Result<Digest, CheckpointIntegrityError> {
    domain_separated_digest(CHECKPOINT_DIGEST_DOMAIN, value)
}

fn domain_separated_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, CheckpointIntegrityError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| CheckpointIntegrityError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

#[allow(clippy::too_many_arguments)]
fn validate_write_shape(
    tenant_id: &TenantId,
    run_id: RunId,
    checkpoint_id: CheckpointId,
    superstep: Superstep,
    graph: &GraphReference,
    state: &CheckpointState,
    parent: Option<&CheckpointHead>,
) -> Result<(), CheckpointWriteError> {
    if state.schema != graph.state_schema {
        return Err(CheckpointWriteError::StateSchemaMismatch);
    }

    match (superstep, parent) {
        (Superstep::INITIAL, None) => Ok(()),
        (Superstep::INITIAL, Some(_)) => Err(CheckpointWriteError::InitialHasParent),
        (_, None) => Err(CheckpointWriteError::SuccessorMissingParent),
        (actual, Some(parent)) => {
            if parent.tenant_id != *tenant_id {
                return Err(CheckpointWriteError::ParentTenantMismatch);
            }
            if parent.run_id != run_id {
                return Err(CheckpointWriteError::ParentRunMismatch);
            }
            if parent.checkpoint_id == checkpoint_id {
                return Err(CheckpointWriteError::ReusedCheckpointId);
            }
            let expected = parent
                .superstep
                .checked_next()
                .ok_or(CheckpointWriteError::SuperstepOverflow)?;
            if actual != expected {
                return Err(CheckpointWriteError::NonContiguousSuperstep { expected, actual });
            }
            if parent.graph != *graph {
                return Err(CheckpointWriteError::ParentGraphMismatch);
            }
            Ok(())
        }
    }
}

#[derive(Clone, Copy)]
enum JournalScopeError {
    Tenant,
    Run,
}

fn validate_journal_scope(
    tenant_id: &TenantId,
    run_id: RunId,
    journal_head: &JournalHead,
) -> Result<(), JournalScopeError> {
    if journal_head.tenant_id() != tenant_id {
        return Err(JournalScopeError::Tenant);
    }
    if journal_head.run_id() != run_id {
        return Err(JournalScopeError::Run);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CapabilityName, CapabilityReference, EventId, IssuerId, PrincipalIdentity, SchemaId,
        SubjectId, Version,
    };
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    fn tenant() -> TenantId {
        TenantId::try_from("tenant-production").unwrap()
    }

    fn other_tenant() -> TenantId {
        TenantId::try_from("tenant-other").unwrap()
    }

    fn run() -> RunId {
        "01912345-6789-7abc-8def-0123456789ae".parse().unwrap()
    }

    fn other_run() -> RunId {
        "01912345-6789-7abc-8def-0123456789af".parse().unwrap()
    }

    fn checkpoint_id(suffix: &str) -> CheckpointId {
        format!("01912345-6789-7abc-8def-0123456789{suffix}")
            .parse()
            .unwrap()
    }

    fn state_schema(label: &[u8]) -> SchemaReference {
        SchemaReference::new(
            "https://stateknot.github.io/schema/test/workflow-state/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(label),
        )
    }

    fn graph_with_schema(schema: SchemaReference) -> GraphReference {
        let owner = PrincipalIdentity::new(
            "https://issuer.example.com/stateknot"
                .parse::<IssuerId>()
                .unwrap(),
            "workflow-registry".parse::<SubjectId>().unwrap(),
        );
        let capability = CapabilityReference::new(
            "orders.fulfillment".parse::<CapabilityName>().unwrap(),
            Version::new(1, 4, 0),
        );
        GraphReference::new(
            CapabilityIdentity::new(owner, capability),
            Digest::sha256(b"compiled-order-graph-v1.4.0"),
            schema,
        )
    }

    fn graph() -> GraphReference {
        graph_with_schema(state_schema(b"order-state-schema-v1"))
    }

    fn state(value: Value) -> CheckpointState {
        CheckpointState::new(
            graph().state_schema().clone(),
            BoundedJson::try_from_value(value).unwrap(),
        )
        .unwrap()
    }

    fn ready(values: &[&str]) -> ReadyNodes {
        ReadyNodes::try_new(values.iter().map(|value| value.parse::<NodeId>().unwrap())).unwrap()
    }

    fn at(offset_micros: i64) -> Timestamp {
        let base = "2030-01-01T00:00:00.000000Z".parse::<Timestamp>().unwrap();
        Timestamp::from_unix_micros(base.unix_micros() + offset_micros).unwrap()
    }

    fn journal(sequence: u64, observed_at: Timestamp) -> JournalHead {
        let event_id = format!("01912345-6789-7abc-8def-0123456789{sequence:02x}")
            .parse::<EventId>()
            .unwrap();
        JournalHead::new(
            tenant(),
            run(),
            JournalSequence::new(sequence).unwrap(),
            event_id,
            observed_at,
            Digest::sha256(format!("journal-event-{sequence}")),
        )
    }

    fn initial_checkpoint() -> Checkpoint {
        let write = CheckpointWrite::initial(
            tenant(),
            run(),
            checkpoint_id("d1"),
            graph(),
            state(json!({"order_id": "order-42", "status": "pending"})),
            ready(&["authorize", "reserve-stock"]),
        )
        .unwrap();
        Checkpoint::commit(write, journal(1, at(1))).unwrap()
    }

    fn successor_checkpoint() -> Checkpoint {
        let parent = initial_checkpoint();
        let write = CheckpointWrite::successor(
            checkpoint_id("d2"),
            &parent,
            state(json!({"order_id": "order-42", "status": "reserved"})),
            ready(&["capture-payment"]),
        )
        .unwrap();
        Checkpoint::commit(write, journal(3, at(3))).unwrap()
    }

    #[test]
    fn supersteps_use_canonical_full_width_decimal_text() {
        for (text, expected) in [
            ("0", Superstep::INITIAL),
            ("1", Superstep::new(1).unwrap()),
            ("9223372036854775807", Superstep::MAX),
        ] {
            let decoded = text.parse::<Superstep>().unwrap();
            assert_eq!(decoded, expected);
            assert_eq!(decoded.to_string(), text);
            assert_eq!(to_value(decoded).unwrap(), Value::from(text));
        }

        for invalid in ["", "00", "01", "-1", "1.0", "9223372036854775808"] {
            assert!(invalid.parse::<Superstep>().is_err(), "accepted {invalid}");
        }
        assert!(from_value::<Superstep>(json!(0)).is_err());
        assert_eq!(Superstep::MAX.checked_next(), None);
    }

    #[test]
    fn node_ids_and_ready_sets_are_bounded_unique_and_sorted() {
        for value in ["a", "Authorize", "order.capture-v2", "node_42"] {
            let node = NodeId::try_from(value).unwrap();
            assert_eq!(node.as_str(), value);
            assert_eq!(node.to_string(), value);
        }
        for invalid in ["", ".", "..", "_private", "node/name", "node name", "节点"] {
            assert!(NodeId::try_from(invalid).is_err(), "accepted {invalid:?}");
        }
        assert!(NodeId::new("a".repeat(NodeId::MAX_LEN)).is_ok());
        assert!(NodeId::new("a".repeat(NodeId::MAX_LEN + 1)).is_err());

        let set = ready(&["z-last", "a-first", "middle"]);
        assert_eq!(set.len(), 3);
        assert!(set.contains(&NodeId::try_from("middle").unwrap()));
        assert_eq!(
            to_value(&set).unwrap(),
            json!(["a-first", "middle", "z-last"])
        );
        assert!(from_value::<ReadyNodes>(json!(["same", "same"])).is_err());

        let too_many =
            (0..=ReadyNodes::MAX_LEN).map(|index| NodeId::new(format!("node-{index:04}")).unwrap());
        assert_eq!(
            ReadyNodes::try_new(too_many),
            Err(ReadyNodesError::TooMany {
                maximum: ReadyNodes::MAX_LEN,
                actual: ReadyNodes::MAX_LEN + 1,
            })
        );
    }

    #[test]
    fn state_digest_binds_schema_and_canonical_json() {
        let schema = state_schema(b"schema-a");
        let first = CheckpointState::new(
            schema.clone(),
            BoundedJson::from_str(r#"{"z":0,"a":[true,{"name":"é"}]}"#).unwrap(),
        )
        .unwrap();
        let equivalent = CheckpointState::new(
            schema.clone(),
            BoundedJson::from_str(" { \"a\" : [ true, { \"name\" : \"é\" } ], \"z\" : -0.0 }")
                .unwrap(),
        )
        .unwrap();
        assert_eq!(first.digest(), equivalent.digest());

        let different_schema =
            CheckpointState::new(state_schema(b"schema-b"), equivalent.data().clone()).unwrap();
        assert_ne!(first.digest(), different_schema.digest());

        let mut tampered = to_value(&first).unwrap();
        tampered["data"]["z"] = json!(1);
        assert!(from_value::<CheckpointState>(tampered).is_err());

        let unsafe_integer = BoundedJson::from_str("9007199254740992").unwrap();
        assert!(matches!(
            CheckpointState::new(schema, unsafe_integer),
            Err(CheckpointStateError::Integrity { .. })
        ));
    }

    #[test]
    fn checkpoint_state_deserialization_uses_the_explicit_two_mib_ceiling() {
        let large_data = BoundedJson::from_str_with_limits(
            &format!("\"{}\"", "x".repeat(JsonLimits::DEFAULT.max_bytes() + 1)),
            JsonLimits::MAXIMUM,
        )
        .unwrap();
        let state = CheckpointState::new(state_schema(b"large-state"), large_data).unwrap();
        let encoded = serde_json::to_vec(&state).unwrap();
        let decoded = serde_json::from_slice::<CheckpointState>(&encoded).unwrap();
        assert_eq!(decoded, state);

        let oversized = json!({
            "schema": state_schema(b"oversized"),
            "data": "x".repeat(JsonLimits::MAXIMUM.max_string_bytes() + 1),
            "digest": Digest::sha256(b"irrelevant"),
        });
        assert!(from_value::<CheckpointState>(oversized).is_err());
    }

    #[test]
    fn initial_checkpoint_round_trips_and_redacts_state() {
        let checkpoint = initial_checkpoint();
        assert_eq!(checkpoint.superstep(), Superstep::INITIAL);
        assert!(checkpoint.parent().is_none());
        assert_eq!(checkpoint.journal_head().sequence(), JournalSequence::FIRST);
        assert_eq!(checkpoint.graph(), &graph());
        assert_eq!(
            checkpoint.ready_nodes(),
            &ready(&["authorize", "reserve-stock"])
        );
        assert!(checkpoint.matches_write(&checkpoint.write_intent()));

        let encoded = to_value(&checkpoint).unwrap();
        assert_eq!(from_value::<Checkpoint>(encoded).unwrap(), checkpoint);
        assert_eq!(
            Checkpoint::restore(
                checkpoint.write_intent(),
                checkpoint.journal_head().clone(),
                checkpoint.digest(),
            )
            .unwrap(),
            checkpoint
        );

        let debug = format!("{checkpoint:?}");
        assert!(debug.contains("CheckpointState"));
        assert!(!debug.contains("order-42"));
        assert!(!debug.contains("pending"));
    }

    #[test]
    fn successor_is_contiguous_and_inherits_scope_and_graph() {
        let parent = initial_checkpoint();
        let write = CheckpointWrite::successor(
            checkpoint_id("d2"),
            &parent,
            state(json!({"status": "authorized"})),
            ready(&["capture-payment"]),
        )
        .unwrap();
        assert_eq!(write.superstep(), Superstep::new(1).unwrap());
        assert_eq!(write.tenant_id(), parent.tenant_id());
        assert_eq!(write.run_id(), parent.run_id());
        assert_eq!(write.graph(), parent.graph());
        assert_eq!(write.parent(), Some(&parent.head()));

        let restored = CheckpointWrite::restore(
            write.tenant_id().clone(),
            write.run_id(),
            write.checkpoint_id(),
            write.superstep(),
            write.graph().clone(),
            write.state().clone(),
            write.ready_nodes().clone(),
            write.parent().cloned(),
            write.intent_digest(),
        )
        .unwrap();
        assert_eq!(restored, write);

        let checkpoint = Checkpoint::commit(write, journal(3, at(3))).unwrap();
        assert_eq!(checkpoint.superstep(), Superstep::new(1).unwrap());
        assert_eq!(checkpoint.parent(), Some(&parent.head()));
        assert_eq!(checkpoint.journal_head().sequence().get(), 3);
    }

    #[test]
    fn write_shape_rejects_schema_scope_position_graph_and_identity_changes() {
        let parent = initial_checkpoint();
        let wrong_schema_state = CheckpointState::new(
            state_schema(b"wrong-schema"),
            BoundedJson::try_from_value(json!({})).unwrap(),
        )
        .unwrap();
        assert_eq!(
            CheckpointWrite::successor(
                checkpoint_id("d2"),
                &parent,
                wrong_schema_state,
                ReadyNodes::empty(),
            ),
            Err(CheckpointWriteError::StateSchemaMismatch)
        );
        assert_eq!(
            CheckpointWrite::successor(
                parent.checkpoint_id(),
                &parent,
                state(json!({})),
                ReadyNodes::empty(),
            ),
            Err(CheckpointWriteError::ReusedCheckpointId)
        );

        let valid = CheckpointWrite::successor(
            checkpoint_id("d2"),
            &parent,
            state(json!({})),
            ReadyNodes::empty(),
        )
        .unwrap();
        let mut wire = to_value(&valid).unwrap();

        wire["superstep"] = json!("3");
        assert!(from_value::<CheckpointWrite>(wire.clone()).is_err());
        wire["superstep"] = json!("1");

        wire["tenant_id"] = json!(other_tenant());
        assert!(from_value::<CheckpointWrite>(wire.clone()).is_err());
        wire["tenant_id"] = json!(tenant());

        wire["run_id"] = json!(other_run());
        assert!(from_value::<CheckpointWrite>(wire.clone()).is_err());
        wire["run_id"] = json!(run());

        wire["parent"]["graph"]["definition_digest"] = json!(Digest::sha256(b"other-graph"));
        assert!(from_value::<CheckpointWrite>(wire).is_err());
    }

    #[test]
    fn committed_checkpoint_rejects_nonadvancing_or_regressing_journal_heads() {
        let parent = initial_checkpoint();
        let make_write = || {
            CheckpointWrite::successor(
                checkpoint_id("d2"),
                &parent,
                state(json!({"status": "next"})),
                ReadyNodes::empty(),
            )
            .unwrap()
        };

        assert_eq!(
            Checkpoint::commit(make_write(), journal(1, at(2))),
            Err(CheckpointError::JournalDidNotAdvance {
                previous: JournalSequence::FIRST,
                actual: JournalSequence::FIRST,
            })
        );
        assert_eq!(
            Checkpoint::commit(make_write(), journal(2, at(0))),
            Err(CheckpointError::ClockRegression {
                previous: at(1),
                actual: at(0),
            })
        );

        let crossed_tenant = JournalHead::new(
            other_tenant(),
            run(),
            JournalSequence::new(2).unwrap(),
            "01912345-6789-7abc-8def-012345678902".parse().unwrap(),
            at(2),
            Digest::sha256(b"other-tenant"),
        );
        assert_eq!(
            Checkpoint::commit(make_write(), crossed_tenant),
            Err(CheckpointError::JournalTenantMismatch)
        );
    }

    #[test]
    fn every_integrity_layer_rejects_tampering() {
        let checkpoint = successor_checkpoint();
        let original = to_value(&checkpoint).unwrap();

        let mut state_data = original.clone();
        state_data["state"]["data"]["status"] = json!("captured");
        assert!(from_value::<Checkpoint>(state_data).is_err());

        let mut state_digest = original.clone();
        state_digest["state"]["digest"] = json!(Digest::sha256(b"wrong-state"));
        assert!(from_value::<Checkpoint>(state_digest).is_err());

        let mut ready_nodes = original.clone();
        ready_nodes["ready_nodes"] = json!(["different-node"]);
        assert!(from_value::<Checkpoint>(ready_nodes).is_err());

        let mut intent_digest = original.clone();
        intent_digest["intent_digest"] = json!(Digest::sha256(b"wrong-intent"));
        assert!(from_value::<Checkpoint>(intent_digest).is_err());

        let mut journal_head = original.clone();
        journal_head["journal_head"]["digest"] = json!(Digest::sha256(b"wrong-journal"));
        assert!(from_value::<Checkpoint>(journal_head).is_err());

        let mut digest = original;
        digest["digest"] = json!(Digest::sha256(b"wrong-checkpoint"));
        assert!(from_value::<Checkpoint>(digest).is_err());
    }

    #[test]
    fn compact_head_enforces_scope_and_public_schemas_are_closed() {
        let checkpoint = initial_checkpoint();
        let head = checkpoint.head();
        assert_eq!(
            from_value::<CheckpointHead>(to_value(&head).unwrap()).unwrap(),
            head
        );

        assert_eq!(
            CheckpointHead::new(
                tenant(),
                other_run(),
                checkpoint.checkpoint_id(),
                checkpoint.superstep(),
                checkpoint.graph().clone(),
                checkpoint.journal_head().clone(),
                checkpoint.digest(),
            ),
            Err(CheckpointHeadError::JournalRunMismatch)
        );

        for schema in [
            to_value(schemars::schema_for!(GraphReference)).unwrap(),
            to_value(schemars::schema_for!(CheckpointState)).unwrap(),
            to_value(schemars::schema_for!(CheckpointHead)).unwrap(),
            to_value(schemars::schema_for!(CheckpointWrite)).unwrap(),
            to_value(schemars::schema_for!(Checkpoint)).unwrap(),
        ] {
            assert_eq!(schema["additionalProperties"], false);
        }
        let ready_schema = to_value(schemars::schema_for!(ReadyNodes)).unwrap();
        assert_eq!(ready_schema["maxItems"], ReadyNodes::MAX_LEN);
        assert_eq!(ready_schema["uniqueItems"], true);
    }

    #[test]
    fn reverse_lineage_verifier_is_streaming_exact_and_transactional() {
        let root = initial_checkpoint();
        let tip = successor_checkpoint();
        let mut verifier = CheckpointLineageVerifier::from_tip(tip.head());

        assert_eq!(verifier.expected(), Some(&tip.head()));
        assert_eq!(
            verifier.verify_next(&root),
            Err(CheckpointLineageError::HeadMismatch)
        );
        assert_eq!(
            verifier.expected(),
            Some(&tip.head()),
            "rejection must not advance the verifier"
        );

        verifier.verify_next(&tip).unwrap();
        assert_eq!(verifier.expected(), Some(&root.head()));
        verifier.verify_next(&root).unwrap();
        assert!(verifier.is_complete());
        assert_eq!(verifier.expected(), None);
        assert_eq!(
            verifier.verify_next(&root),
            Err(CheckpointLineageError::AlreadyComplete)
        );
    }

    proptest! {
        #[test]
        fn every_supported_superstep_round_trips(value in 0_u64..=MAX_DATABASE_ORDINAL) {
            let step = Superstep::new(value).unwrap();
            prop_assert_eq!(step.to_string().parse::<Superstep>().unwrap(), step);
            prop_assert_eq!(serde_json::from_value::<Superstep>(to_value(step).unwrap()).unwrap(), step);
        }
    }
}
