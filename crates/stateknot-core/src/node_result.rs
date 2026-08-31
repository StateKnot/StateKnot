// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Immutable pending node results for deterministic graph barriers.
//!
//! A successful logical node activation commits exactly one pending result
//! before a superstep barrier may reduce state. The semantic intent is
//! independent of the physical worker that won the commit, while the final
//! record retains that worker's exact run fence and journal anchor. Tool and
//! model observations are referenced only through committed, activation-bound
//! ledger revisions; their potentially large payloads are never copied here.

use std::{collections::BTreeSet, fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess},
};
use thiserror::Error;

use crate::{
    BoundedJson, CanonicalJson, CanonicalJsonError, Digest, EventId, InterruptId,
    InterruptRequestIntent, InvocationId, JournalHead, JournalPayload, JsonLimits, ModelInvocation,
    ModelInvocationHead, ModelInvocationStatus, NodeActivation, PrincipalIdentity, RunFence, RunId,
    RunInterruptKind, RunTimerKind, SchemaReference, ScopeSet, TenantId, TimerId,
    TimerRegistrationIntent, ToolInvocation, ToolInvocationHead, ToolInvocationStatus,
    WaitRegistrationIntent,
};

const ROUTE_ID_PATTERN: &str = "^(?!\\.{1,2}$)[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$";
const UPDATE_DIGEST_DOMAIN: &[u8] = b"stateknot-node-state-update-v1\0";
const TERMINAL_DIGEST_DOMAIN: &[u8] = b"stateknot-node-terminal-output-v1\0";
const INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-pending-node-result-intent-v1\0";
const RECORD_DIGEST_DOMAIN: &[u8] = b"stateknot-pending-node-result-v1\0";

/// Stable conditional route identity declared by a compiled graph.
///
/// A route identifies a graph-definition branch, not a destination node. The
/// barrier resolves it through the exact graph version pinned by the base
/// checkpoint, which permits one route to fan out without runtime graph
/// mutation.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RouteId(Box<str>);

impl RouteId {
    /// Maximum encoded route identity length in bytes.
    pub const MAX_LEN: usize = 128;

    /// Validates and constructs a route identity.
    ///
    /// # Errors
    ///
    /// Returns [`RouteIdError`] for an empty, path-like, oversized, or
    /// non-canonical ASCII identity.
    pub fn new(value: impl Into<String>) -> Result<Self, RouteIdError> {
        let value = value.into();
        validate_route_id(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact route identity text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RouteId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for RouteId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RouteId")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for RouteId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RouteId {
    type Err = RouteIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for RouteId {
    type Error = RouteIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for RouteId {
    type Error = RouteIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for RouteId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RouteId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(RouteIdVisitor)
    }
}

struct RouteIdVisitor;

impl de::Visitor<'_> for RouteIdVisitor {
    type Value = RouteId;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded canonical graph route identifier")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        RouteId::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for RouteId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "RouteId".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::RouteId").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 128,
            "pattern": ROUTE_ID_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid conditional route identity.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RouteIdError {
    /// Route identity was empty.
    #[error("route identifier must not be empty")]
    Empty,
    /// Route identity exceeded [`RouteId::MAX_LEN`].
    #[error("route identifier is {actual} bytes; maximum is {maximum}")]
    TooLong {
        /// Maximum accepted bytes.
        maximum: usize,
        /// Observed bytes.
        actual: usize,
    },
    /// Route identity was `.` or `..`.
    #[error("route identifier must not be path-like")]
    PathLike,
    /// A byte was outside the stable ASCII grammar.
    #[error("route identifier contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset.
        index: usize,
    },
}

fn validate_route_id(value: &str) -> Result<(), RouteIdError> {
    if value.is_empty() {
        return Err(RouteIdError::Empty);
    }
    if value.len() > RouteId::MAX_LEN {
        return Err(RouteIdError::TooLong {
            maximum: RouteId::MAX_LEN,
            actual: value.len(),
        });
    }
    if matches!(value, "." | "..") {
        return Err(RouteIdError::PathLike);
    }
    for (index, byte) in value.bytes().enumerate() {
        let valid = if index == 0 {
            byte.is_ascii_alphanumeric()
        } else {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-')
        };
        if !valid {
            return Err(RouteIdError::InvalidByte { index });
        }
    }
    Ok(())
}

/// Schema-pinned bounded update emitted by one successful node activation.
///
/// The graph registry must validate [`Self::data`] against [`Self::schema`]
/// before construction. This type independently binds the schema identity and
/// RFC 8785 data bytes so storage corruption cannot substitute either field.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeStateUpdate {
    schema: SchemaReference,
    data: BoundedJson,
    digest: Digest,
}

impl NodeStateUpdate {
    /// Constructs and checksums a validated update.
    ///
    /// # Errors
    ///
    /// Returns [`NodeStateUpdateError::Integrity`] when canonicalization fails.
    pub fn new(schema: SchemaReference, data: BoundedJson) -> Result<Self, NodeStateUpdateError> {
        let digest = compute_payload_digest(UPDATE_DIGEST_DOMAIN, &schema, &data)
            .map_err(NodeStateUpdateError::integrity)?;
        Ok(Self {
            schema,
            data,
            digest,
        })
    }

    /// Restores a durable update and verifies its checksum.
    ///
    /// # Errors
    ///
    /// Returns [`NodeStateUpdateError`] for canonicalization failure or a
    /// mismatched persisted digest.
    pub fn restore(
        schema: SchemaReference,
        data: BoundedJson,
        digest: Digest,
    ) -> Result<Self, NodeStateUpdateError> {
        let restored = Self::new(schema, data)?;
        if restored.digest != digest {
            return Err(NodeStateUpdateError::DigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the pinned update schema.
    #[must_use]
    pub const fn schema(&self) -> &SchemaReference {
        &self.schema
    }

    /// Returns the bounded update JSON.
    #[must_use]
    pub const fn data(&self) -> &BoundedJson {
        &self.data
    }

    /// Returns the domain-separated update checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl fmt::Debug for NodeStateUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeStateUpdate")
            .field("schema", &self.schema)
            .field("data_stats", &self.data.stats())
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for NodeStateUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema: SchemaReference,
            #[serde(deserialize_with = "deserialize_maximum_json")]
            data: BoundedJson,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.schema, wire.data, wire.digest).map_err(de::Error::custom)
    }
}

/// Invalid or corrupted schema-pinned node update.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeStateUpdateError {
    /// Canonical integrity material could not be produced.
    #[error("node state update integrity calculation failed: {source}")]
    Integrity {
        /// Exact integrity failure.
        #[source]
        source: PendingNodeResultIntegrityError,
    },
    /// The supplied checksum did not match the schema and data.
    #[error("node state update digest does not match its fields")]
    DigestMismatch,
}

impl NodeStateUpdateError {
    const fn integrity(source: PendingNodeResultIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Schema-pinned bounded output emitted by a terminal graph node.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeTerminalOutput {
    schema: SchemaReference,
    data: BoundedJson,
    digest: Digest,
}

impl NodeTerminalOutput {
    /// Constructs and checksums validated terminal output.
    ///
    /// # Errors
    ///
    /// Returns [`NodeTerminalOutputError::Integrity`] when canonicalization
    /// fails.
    pub fn new(
        schema: SchemaReference,
        data: BoundedJson,
    ) -> Result<Self, NodeTerminalOutputError> {
        let digest = compute_payload_digest(TERMINAL_DIGEST_DOMAIN, &schema, &data)
            .map_err(NodeTerminalOutputError::integrity)?;
        Ok(Self {
            schema,
            data,
            digest,
        })
    }

    /// Restores durable output and verifies its checksum.
    ///
    /// # Errors
    ///
    /// Returns [`NodeTerminalOutputError`] for canonicalization failure or a
    /// mismatched persisted digest.
    pub fn restore(
        schema: SchemaReference,
        data: BoundedJson,
        digest: Digest,
    ) -> Result<Self, NodeTerminalOutputError> {
        let restored = Self::new(schema, data)?;
        if restored.digest != digest {
            return Err(NodeTerminalOutputError::DigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the graph output schema.
    #[must_use]
    pub const fn schema(&self) -> &SchemaReference {
        &self.schema
    }

    /// Returns bounded terminal output JSON.
    #[must_use]
    pub const fn data(&self) -> &BoundedJson {
        &self.data
    }

    /// Returns the domain-separated output checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl fmt::Debug for NodeTerminalOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeTerminalOutput")
            .field("schema", &self.schema)
            .field("data_stats", &self.data.stats())
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for NodeTerminalOutput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema: SchemaReference,
            #[serde(deserialize_with = "deserialize_maximum_json")]
            data: BoundedJson,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.schema, wire.data, wire.digest).map_err(de::Error::custom)
    }
}

/// Invalid or corrupted schema-pinned terminal output.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeTerminalOutputError {
    /// Canonical integrity material could not be produced.
    #[error("node terminal output integrity calculation failed: {source}")]
    Integrity {
        /// Exact integrity failure.
        #[source]
        source: PendingNodeResultIntegrityError,
    },
    /// The supplied checksum did not match the schema and data.
    #[error("node terminal output digest does not match its fields")]
    DigestMismatch,
}

impl NodeTerminalOutputError {
    const fn integrity(source: PendingNodeResultIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

fn deserialize_maximum_json<'de, D>(deserializer: D) -> Result<BoundedJson, D::Error>
where
    D: Deserializer<'de>,
{
    BoundedJson::deserialize_with_limits(deserializer, JsonLimits::MAXIMUM)
}

/// State contribution from one successful logical node activation.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NodeStateChange {
    /// The activation intentionally emitted no state update.
    Unchanged,
    /// The activation emitted one schema-validated typed update.
    Update {
        /// Exact update consumed by the compiled reducer plan.
        update: NodeStateUpdate,
    },
}

impl NodeStateChange {
    /// Returns the update when this activation changed state.
    #[must_use]
    pub const fn update(&self) -> Option<&NodeStateUpdate> {
        match self {
            Self::Unchanged => None,
            Self::Update { update } => Some(update),
        }
    }
}

/// Complete, uncommitted lifecycle condition emitted by a graph node.
///
/// Unlike [`crate::RunWait`], this value deliberately has no registration
/// timestamp or journal identity. Those are authoritative database facts that
/// exist only when the lifecycle coordinator atomically commits the graph
/// barrier and wait registrations. Every policy-bearing interrupt field is
/// retained here so recovery can reproduce the exact registration without
/// consulting mutable application configuration.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NodeWait {
    /// Require one authenticated, authorized external resolution.
    Interrupt {
        /// Stable interrupt identity selected before node-result commit.
        interrupt_id: InterruptId,
        /// Protocol-neutral reason the run needs external input.
        interrupt_kind: RunInterruptKind,
        /// Complete schema-pinned public request payload.
        request_payload: JournalPayload,
        /// Immutable action or question protected by the resolution.
        action_digest: Digest,
        /// Exact required resolver, when policy selected one.
        #[serde(skip_serializing_if = "Option::is_none")]
        required_principal: Option<PrincipalIdentity>,
        /// Every scope the resolver must possess.
        required_scopes: ScopeSet,
        /// Exclusive resolution expiry, when finite.
        #[serde(skip_serializing_if = "Option::is_none")]
        expires_at: Option<crate::Timestamp>,
    },
    /// Require one database-clock timer observation.
    Timer {
        /// Stable timer identity selected before node-result commit.
        timer_id: TimerId,
        /// Semantic purpose of the timer.
        timer_kind: RunTimerKind,
        /// Inclusive earliest database instant at which it may fire.
        due_at: crate::Timestamp,
    },
}

impl NodeWait {
    /// Constructs a complete interrupt registration specification.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn interrupt(
        interrupt_id: InterruptId,
        interrupt_kind: RunInterruptKind,
        request_payload: JournalPayload,
        action_digest: Digest,
        required_principal: Option<PrincipalIdentity>,
        required_scopes: ScopeSet,
        expires_at: Option<crate::Timestamp>,
    ) -> Self {
        Self::Interrupt {
            interrupt_id,
            interrupt_kind,
            request_payload,
            action_digest,
            required_principal,
            required_scopes,
            expires_at,
        }
    }

    /// Constructs a complete timer registration specification.
    #[must_use]
    pub const fn timer(
        timer_id: TimerId,
        timer_kind: RunTimerKind,
        due_at: crate::Timestamp,
    ) -> Self {
        Self::Timer {
            timer_id,
            timer_kind,
            due_at,
        }
    }

    /// Converts this durable node result into an exact event-bound provider
    /// registration intent.
    ///
    /// The provider still supplies and verifies the authoritative database
    /// registration time while committing the wait barrier.
    ///
    /// # Errors
    ///
    /// Returns [`crate::DurableWaitError`] only if canonical intent integrity
    /// material cannot be encoded.
    pub fn registration_intent(
        &self,
        tenant_id: TenantId,
        run_id: RunId,
        event_id: EventId,
    ) -> Result<WaitRegistrationIntent, crate::DurableWaitError> {
        match self {
            Self::Interrupt {
                interrupt_id,
                interrupt_kind,
                request_payload,
                action_digest,
                required_principal,
                required_scopes,
                expires_at,
            } => InterruptRequestIntent::new(
                tenant_id,
                run_id,
                *interrupt_id,
                event_id,
                *interrupt_kind,
                request_payload.clone(),
                *action_digest,
                required_principal.clone(),
                required_scopes.clone(),
                *expires_at,
            )
            .map(WaitRegistrationIntent::interrupt),
            Self::Timer {
                timer_id,
                timer_kind,
                due_at,
            } => TimerRegistrationIntent::new(
                tenant_id,
                run_id,
                *timer_id,
                event_id,
                *timer_kind,
                *due_at,
            )
            .map(WaitRegistrationIntent::timer),
        }
    }

    fn identity_uuid(&self) -> uuid::Uuid {
        match self {
            Self::Interrupt { interrupt_id, .. } => interrupt_id.into_uuid(),
            Self::Timer { timer_id, .. } => timer_id.into_uuid(),
        }
    }
}

impl fmt::Debug for NodeWait {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Interrupt {
                interrupt_id,
                interrupt_kind,
                request_payload,
                action_digest,
                required_principal,
                required_scopes,
                expires_at,
            } => formatter
                .debug_struct("NodeWait::Interrupt")
                .field("interrupt_id", interrupt_id)
                .field("interrupt_kind", interrupt_kind)
                .field("request_schema", request_payload.schema())
                .field("request_digest", &request_payload.digest())
                .field("action_digest", action_digest)
                .field("required_principal", required_principal)
                .field("required_scope_count", &required_scopes.len())
                .field("expires_at", expires_at)
                .finish_non_exhaustive(),
            Self::Timer {
                timer_id,
                timer_kind,
                due_at,
            } => formatter
                .debug_struct("NodeWait::Timer")
                .field("timer_id", timer_id)
                .field("timer_kind", timer_kind)
                .field("due_at", due_at)
                .finish_non_exhaustive(),
        }
    }
}

impl<'de> Deserialize<'de> for NodeWait {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Interrupt {
                interrupt_id: InterruptId,
                interrupt_kind: RunInterruptKind,
                request_payload: JournalPayload,
                action_digest: Digest,
                required_principal: Option<PrincipalIdentity>,
                required_scopes: ScopeSet,
                expires_at: Option<crate::Timestamp>,
            },
            Timer {
                timer_id: TimerId,
                timer_kind: RunTimerKind,
                due_at: crate::Timestamp,
            },
        }

        Ok(match Wire::deserialize(deserializer)? {
            Wire::Interrupt {
                interrupt_id,
                interrupt_kind,
                request_payload,
                action_digest,
                required_principal,
                required_scopes,
                expires_at,
            } => Self::interrupt(
                interrupt_id,
                interrupt_kind,
                request_payload,
                action_digest,
                required_principal,
                required_scopes,
                expires_at,
            ),
            Wire::Timer {
                timer_id,
                timer_kind,
                due_at,
            } => Self::timer(timer_id, timer_kind, due_at),
        })
    }
}

/// Non-empty, bounded, identity-unique uncommitted wait batch.
#[derive(Clone, Eq, PartialEq)]
pub struct NodeWaits {
    values: Box<[NodeWait]>,
}

impl NodeWaits {
    /// Hard maximum number of conditions one graph barrier may register.
    pub const MAX_LEN: usize = 64;

    /// Validates one complete wait specification batch.
    ///
    /// # Errors
    ///
    /// Returns [`NodeWaitsError`] for an empty, oversized, or identity-crossed
    /// batch.
    pub fn try_new<I>(values: I) -> Result<Self, NodeWaitsError>
    where
        I: IntoIterator<Item = NodeWait>,
    {
        let mut collected = Vec::new();
        for value in values {
            if collected.len() == Self::MAX_LEN {
                return Err(NodeWaitsError::TooMany {
                    maximum: Self::MAX_LEN,
                    actual: Self::MAX_LEN + 1,
                });
            }
            if collected
                .iter()
                .any(|existing: &NodeWait| existing.identity_uuid() == value.identity_uuid())
            {
                return Err(NodeWaitsError::DuplicateIdentity);
            }
            collected.push(value);
        }
        if collected.is_empty() {
            return Err(NodeWaitsError::Empty);
        }
        Ok(Self {
            values: collected.into_boxed_slice(),
        })
    }

    /// Returns the number of conditions in semantic order.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether no conditions exist.
    ///
    /// This is always `false` for a valid value.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterates uncommitted conditions in deterministic semantic order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &NodeWait> {
        self.values.iter()
    }

    /// Converts every condition to an exact registration intent for one
    /// lifecycle journal event.
    ///
    /// # Errors
    ///
    /// Returns [`crate::DurableWaitError`] if an intent checksum cannot be
    /// encoded.
    pub fn registration_intents(
        &self,
        tenant_id: &TenantId,
        run_id: RunId,
        event_id: EventId,
    ) -> Result<Vec<WaitRegistrationIntent>, crate::DurableWaitError> {
        self.values
            .iter()
            .map(|wait| wait.registration_intent(tenant_id.clone(), run_id, event_id))
            .collect()
    }
}

impl fmt::Debug for NodeWaits {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeWaits")
            .field("count", &self.len())
            .finish_non_exhaustive()
    }
}

impl Serialize for NodeWaits {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeWaits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(NodeWaitsVisitor)
    }
}

struct NodeWaitsVisitor;

impl<'de> de::Visitor<'de> for NodeWaitsVisitor {
    type Value = NodeWaits;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "one to {} unique graph wait specifications",
            NodeWaits::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::<NodeWait>::with_capacity(
            sequence
                .size_hint()
                .unwrap_or_default()
                .min(NodeWaits::MAX_LEN),
        );
        while let Some(value) = sequence.next_element::<NodeWait>()? {
            if values.len() == NodeWaits::MAX_LEN {
                return Err(de::Error::custom(NodeWaitsError::TooMany {
                    maximum: NodeWaits::MAX_LEN,
                    actual: NodeWaits::MAX_LEN + 1,
                }));
            }
            if values
                .iter()
                .any(|existing| existing.identity_uuid() == value.identity_uuid())
            {
                return Err(de::Error::custom(NodeWaitsError::DuplicateIdentity));
            }
            values.push(value);
        }
        NodeWaits::try_new(values).map_err(de::Error::custom)
    }
}

impl JsonSchema for NodeWaits {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NodeWaits".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::NodeWaits").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<NodeWait>(),
            "minItems": 1,
            "maxItems": 64,
            "uniqueItems": true,
            "description": "Complete uncommitted graph wait specifications. UUID identity uniqueness is enforced at runtime."
        })
    }
}

/// Invalid graph wait specification batch.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeWaitsError {
    /// A suspending node supplied no condition.
    #[error("a graph wait batch must contain at least one condition")]
    Empty,
    /// The hard simultaneous-condition ceiling was exceeded.
    #[error("graph wait batch has {actual} conditions; hard maximum is {maximum}")]
    TooMany {
        /// Absolute simultaneous-condition ceiling.
        maximum: usize,
        /// First observed count beyond the ceiling.
        actual: usize,
    },
    /// Two variants reused one UUID identity.
    #[error("graph wait condition identities must be globally unique")]
    DuplicateIdentity,
}

/// Closed control outcome emitted alongside a node's state contribution.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NodeControl {
    /// Follow the node's unconditional compiled edges.
    Continue,
    /// Select one declared conditional route.
    Route {
        /// Route resolved by the pinned graph definition at the barrier.
        route_id: RouteId,
    },
    /// Suspend the run on a non-empty atomic condition batch.
    Wait {
        /// Interrupts and timers to register at the barrier.
        waits: NodeWaits,
    },
    /// Complete the graph with schema-validated output.
    Terminal {
        /// Exact successful graph output.
        output: NodeTerminalOutput,
    },
}

impl NodeControl {
    /// Returns the stable control discriminator.
    #[must_use]
    pub const fn kind(&self) -> NodeControlKind {
        match self {
            Self::Continue => NodeControlKind::Continue,
            Self::Route { .. } => NodeControlKind::Route,
            Self::Wait { .. } => NodeControlKind::Wait,
            Self::Terminal { .. } => NodeControlKind::Terminal,
        }
    }
}

/// Stable discriminator for one node control result.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum NodeControlKind {
    /// Unconditional compiled edges.
    Continue,
    /// Conditional route selection.
    Route,
    /// Durable suspension.
    Wait,
    /// Successful graph completion.
    Terminal,
}

/// Kind of external invocation referenced by a pending node result.
///
/// Variant order is the canonical binding sort order.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum NodeInvocationBindingKind {
    /// Model-provider invocation.
    Model,
    /// Tool invocation.
    Tool,
}

/// Exact committed external invocation consumed by one node result.
///
/// The embedded activation makes cross-node substitution locally detectable.
/// A durable adapter must additionally reload the exact full invocation
/// revision and prove that its intent activation equals this value before it
/// accepts the reference.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NodeInvocationBinding {
    /// Exact committed model invocation.
    Model {
        /// Activation recorded by the full invocation intent.
        activation: NodeActivation,
        /// Compact committed model revision identity.
        head: ModelInvocationHead,
    },
    /// Exact committed tool invocation.
    Tool {
        /// Activation recorded by the full invocation intent.
        activation: NodeActivation,
        /// Compact committed tool revision identity.
        head: ToolInvocationHead,
    },
}

impl NodeInvocationBinding {
    /// Constructs a binding from a fully validated committed model invocation.
    ///
    /// # Errors
    ///
    /// Returns [`NodeInvocationBindingError`] unless the invocation is
    /// committed and its journal anchor strictly follows its base checkpoint.
    pub fn from_model(invocation: &ModelInvocation) -> Result<Self, NodeInvocationBindingError> {
        Self::restore_model(invocation.intent().activation().clone(), invocation.head())
    }

    /// Constructs a binding from a fully validated committed tool invocation.
    ///
    /// # Errors
    ///
    /// Returns [`NodeInvocationBindingError`] unless the invocation is
    /// committed and its journal anchor strictly follows its base checkpoint.
    pub fn from_tool(invocation: &ToolInvocation) -> Result<Self, NodeInvocationBindingError> {
        Self::restore_tool(invocation.intent().activation().clone(), invocation.head())
    }

    /// Returns the external invocation kind.
    #[must_use]
    pub const fn kind(&self) -> NodeInvocationBindingKind {
        match self {
            Self::Model { .. } => NodeInvocationBindingKind::Model,
            Self::Tool { .. } => NodeInvocationBindingKind::Tool,
        }
    }

    /// Returns the exact owning node activation.
    #[must_use]
    pub const fn activation(&self) -> &NodeActivation {
        match self {
            Self::Model { activation, .. } | Self::Tool { activation, .. } => activation,
        }
    }

    /// Returns the stable logical invocation identity.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        match self {
            Self::Model { head, .. } => head.invocation_id(),
            Self::Tool { head, .. } => head.invocation_id(),
        }
    }

    /// Returns the exact journal head that committed the external result.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        match self {
            Self::Model { head, .. } => head.journal_head(),
            Self::Tool { head, .. } => head.journal_head(),
        }
    }

    /// Returns the model head for a model binding.
    #[must_use]
    pub const fn model_head(&self) -> Option<&ModelInvocationHead> {
        match self {
            Self::Model { head, .. } => Some(head),
            Self::Tool { .. } => None,
        }
    }

    /// Returns the tool head for a tool binding.
    #[must_use]
    pub const fn tool_head(&self) -> Option<&ToolInvocationHead> {
        match self {
            Self::Tool { head, .. } => Some(head),
            Self::Model { .. } => None,
        }
    }

    fn restore_model(
        activation: NodeActivation,
        head: ModelInvocationHead,
    ) -> Result<Self, NodeInvocationBindingError> {
        if head.status() != ModelInvocationStatus::Committed {
            return Err(NodeInvocationBindingError::ModelNotCommitted {
                actual: head.status(),
            });
        }
        validate_binding_scope(
            &activation,
            head.tenant_id(),
            head.run_id(),
            head.journal_head(),
        )?;
        Ok(Self::Model { activation, head })
    }

    fn restore_tool(
        activation: NodeActivation,
        head: ToolInvocationHead,
    ) -> Result<Self, NodeInvocationBindingError> {
        if head.status() != ToolInvocationStatus::Committed {
            return Err(NodeInvocationBindingError::ToolNotCommitted {
                actual: head.status(),
            });
        }
        validate_binding_scope(
            &activation,
            head.tenant_id(),
            head.run_id(),
            head.journal_head(),
        )?;
        Ok(Self::Tool { activation, head })
    }
}

impl<'de> Deserialize<'de> for NodeInvocationBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Model {
                activation: NodeActivation,
                head: ModelInvocationHead,
            },
            Tool {
                activation: NodeActivation,
                head: ToolInvocationHead,
            },
        }

        match Wire::deserialize(deserializer)? {
            Wire::Model { activation, head } => Self::restore_model(activation, head),
            Wire::Tool { activation, head } => Self::restore_tool(activation, head),
        }
        .map_err(de::Error::custom)
    }
}

/// Invalid external invocation binding.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeInvocationBindingError {
    /// Model revision was not committed.
    #[error("model invocation binding must reference committed state, got {actual:?}")]
    ModelNotCommitted {
        /// Rejected lifecycle status.
        actual: ModelInvocationStatus,
    },
    /// Tool revision was not committed.
    #[error("tool invocation binding must reference committed state, got {actual:?}")]
    ToolNotCommitted {
        /// Rejected lifecycle status.
        actual: ToolInvocationStatus,
    },
    /// Invocation head crossed the activation tenant boundary.
    #[error("invocation binding crosses its activation tenant boundary")]
    TenantMismatch,
    /// Invocation head named another run.
    #[error("invocation binding does not belong to its activation run")]
    RunMismatch,
    /// Invocation result did not advance the activation's base journal.
    #[error("invocation binding journal does not follow its activation base checkpoint")]
    JournalNotAfterBase,
    /// Invocation result time preceded its activation base checkpoint.
    #[error("invocation binding clock precedes its activation base checkpoint")]
    ClockRegression,
}

fn validate_binding_scope(
    activation: &NodeActivation,
    tenant_id: &TenantId,
    run_id: RunId,
    journal_head: &JournalHead,
) -> Result<(), NodeInvocationBindingError> {
    if tenant_id != activation.tenant_id() || journal_head.tenant_id() != activation.tenant_id() {
        return Err(NodeInvocationBindingError::TenantMismatch);
    }
    if run_id != activation.run_id() || journal_head.run_id() != activation.run_id() {
        return Err(NodeInvocationBindingError::RunMismatch);
    }
    let base = activation.base_checkpoint().journal_head();
    if journal_head.sequence() <= base.sequence() {
        return Err(NodeInvocationBindingError::JournalNotAfterBase);
    }
    if journal_head.recorded_at() < base.recorded_at() {
        return Err(NodeInvocationBindingError::ClockRegression);
    }
    Ok(())
}

/// Canonically ordered, duplicate-free, bounded external invocation bindings.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeInvocationBindings(Box<[NodeInvocationBinding]>);

impl NodeInvocationBindings {
    /// Maximum external invocation references in one pending result.
    pub const MAX_LEN: usize = 256;

    /// Constructs an empty binding collection.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Validates activation ownership, uniqueness, and the hard count ceiling.
    ///
    /// Values are serialized in canonical `(kind, invocation_id)` order.
    ///
    /// # Errors
    ///
    /// Returns [`NodeInvocationBindingsError`] for a crossed activation,
    /// duplicate logical invocation, or more than [`Self::MAX_LEN`] entries.
    pub fn try_new<I>(
        activation: &NodeActivation,
        values: I,
    ) -> Result<Self, NodeInvocationBindingsError>
    where
        I: IntoIterator<Item = NodeInvocationBinding>,
    {
        let values = collect_bindings(values)?;
        validate_bindings_activation(activation, &values)?;
        Ok(Self(values.into_boxed_slice()))
    }

    /// Returns the number of external invocation references.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no external invocation was consumed.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates bindings in canonical identity order.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &NodeInvocationBinding> {
        self.0.iter()
    }
}

impl Serialize for NodeInvocationBindings {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeInvocationBindings {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(NodeInvocationBindingsVisitor)
    }
}

struct NodeInvocationBindingsVisitor;

impl<'de> de::Visitor<'de> for NodeInvocationBindingsVisitor {
    type Value = NodeInvocationBindings;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "at most {} unique committed node invocation bindings",
            NodeInvocationBindings::MAX_LEN
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
                .min(NodeInvocationBindings::MAX_LEN),
        );
        while let Some(value) = sequence.next_element::<NodeInvocationBinding>()? {
            if values.len() == NodeInvocationBindings::MAX_LEN {
                return Err(de::Error::custom(NodeInvocationBindingsError::TooMany {
                    maximum: NodeInvocationBindings::MAX_LEN,
                    actual: NodeInvocationBindings::MAX_LEN + 1,
                }));
            }
            values.push(value);
        }
        let values = collect_bindings(values).map_err(de::Error::custom)?;
        if let Some(first) = values.first() {
            validate_bindings_activation(first.activation(), &values).map_err(de::Error::custom)?;
        }
        Ok(NodeInvocationBindings(values.into_boxed_slice()))
    }
}

impl JsonSchema for NodeInvocationBindings {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "NodeInvocationBindings".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::NodeInvocationBindings").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<NodeInvocationBinding>(),
            "maxItems": 256,
            "uniqueItems": true,
            "description": "Serialized in canonical (kind, invocation_id) order; runtime additionally requires one exact node activation."
        })
    }
}

/// Invalid pending-result invocation collection.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum NodeInvocationBindingsError {
    /// The hard reference ceiling was exceeded.
    #[error("node invocation bindings contain {actual} entries; maximum is {maximum}")]
    TooMany {
        /// Absolute maximum.
        maximum: usize,
        /// First observed count beyond the maximum.
        actual: usize,
    },
    /// The same invocation kind and logical ID appeared more than once.
    #[error("node invocation bindings contain a duplicate {kind:?} invocation {invocation_id}")]
    Duplicate {
        /// Duplicated invocation kind.
        kind: NodeInvocationBindingKind,
        /// Duplicated invocation identity.
        invocation_id: InvocationId,
    },
    /// A binding belonged to another logical node activation.
    #[error("node invocation binding does not belong to the pending result activation")]
    ActivationMismatch,
}

fn collect_bindings<I>(values: I) -> Result<Vec<NodeInvocationBinding>, NodeInvocationBindingsError>
where
    I: IntoIterator<Item = NodeInvocationBinding>,
{
    let mut collected = Vec::new();
    let mut identities = BTreeSet::new();
    for value in values {
        if collected.len() == NodeInvocationBindings::MAX_LEN {
            return Err(NodeInvocationBindingsError::TooMany {
                maximum: NodeInvocationBindings::MAX_LEN,
                actual: NodeInvocationBindings::MAX_LEN + 1,
            });
        }
        let identity = (value.kind(), value.invocation_id());
        if !identities.insert(identity) {
            return Err(NodeInvocationBindingsError::Duplicate {
                kind: identity.0,
                invocation_id: identity.1,
            });
        }
        collected.push(value);
    }
    collected.sort_unstable_by_key(|value| (value.kind(), value.invocation_id()));
    Ok(collected)
}

fn validate_bindings_activation(
    activation: &NodeActivation,
    values: &[NodeInvocationBinding],
) -> Result<(), NodeInvocationBindingsError> {
    if values
        .iter()
        .any(|binding| binding.activation() != activation)
    {
        return Err(NodeInvocationBindingsError::ActivationMismatch);
    }
    Ok(())
}

/// Immutable semantic result for one logical node activation before commit.
///
/// Its checksum deliberately excludes worker fence and journal position. This
/// is the idempotency fingerprint: the same logical result may be returned
/// after a lost acknowledgement, while any changed update, control outcome, or
/// external observation is a conflict.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingNodeResultIntent {
    activation: NodeActivation,
    state_change: NodeStateChange,
    control: NodeControl,
    bindings: NodeInvocationBindings,
    intent_digest: Digest,
}

impl PendingNodeResultIntent {
    /// Constructs and checksums a semantic pending result.
    ///
    /// # Errors
    ///
    /// Returns [`PendingNodeResultIntentError`] for a crossed invocation
    /// activation or canonical integrity failure.
    pub fn new(
        activation: NodeActivation,
        state_change: NodeStateChange,
        control: NodeControl,
        bindings: NodeInvocationBindings,
    ) -> Result<Self, PendingNodeResultIntentError> {
        validate_bindings_activation(&activation, &bindings.0)
            .map_err(PendingNodeResultIntentError::bindings)?;
        let intent_digest = compute_intent_digest(&PendingNodeResultIntentDigestWire {
            activation: &activation,
            state_change: &state_change,
            control: &control,
            bindings: &bindings,
        })?;
        Ok(Self {
            activation,
            state_change,
            control,
            bindings,
            intent_digest,
        })
    }

    /// Restores a durable semantic result and verifies its checksum.
    ///
    /// # Errors
    ///
    /// Returns [`PendingNodeResultIntentError`] for invariant or checksum
    /// failure.
    pub fn restore(
        activation: NodeActivation,
        state_change: NodeStateChange,
        control: NodeControl,
        bindings: NodeInvocationBindings,
        intent_digest: Digest,
    ) -> Result<Self, PendingNodeResultIntentError> {
        let restored = Self::new(activation, state_change, control, bindings)?;
        if restored.intent_digest != intent_digest {
            return Err(PendingNodeResultIntentError::DigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the exact logical node activation.
    #[must_use]
    pub const fn activation(&self) -> &NodeActivation {
        &self.activation
    }

    /// Returns the typed state contribution.
    #[must_use]
    pub const fn state_change(&self) -> &NodeStateChange {
        &self.state_change
    }

    /// Returns the explicit control outcome.
    #[must_use]
    pub const fn control(&self) -> &NodeControl {
        &self.control
    }

    /// Returns committed external invocation bindings.
    #[must_use]
    pub const fn bindings(&self) -> &NodeInvocationBindings {
        &self.bindings
    }

    /// Returns the semantic idempotency fingerprint.
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

impl fmt::Debug for PendingNodeResultIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingNodeResultIntent")
            .field("activation", &self.activation)
            .field("state_change", &self.state_change)
            .field("control", &self.control)
            .field("binding_count", &self.bindings.len())
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for PendingNodeResultIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            activation: NodeActivation,
            state_change: NodeStateChange,
            control: NodeControl,
            bindings: NodeInvocationBindings,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.activation,
            wire.state_change,
            wire.control,
            wire.bindings,
            wire.intent_digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid or corrupted pending node-result intent.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum PendingNodeResultIntentError {
    /// An external invocation reference crossed the node activation boundary.
    #[error("pending node result has invalid invocation bindings: {source}")]
    Bindings {
        /// Exact collection failure.
        #[source]
        source: NodeInvocationBindingsError,
    },
    /// Canonical integrity material could not be produced.
    #[error("pending node result intent integrity calculation failed: {source}")]
    Integrity {
        /// Exact integrity failure.
        #[source]
        source: PendingNodeResultIntegrityError,
    },
    /// Persisted semantic checksum did not match caller-controlled fields.
    #[error("pending node result intent digest does not match its fields")]
    DigestMismatch,
}

impl PendingNodeResultIntentError {
    const fn bindings(source: NodeInvocationBindingsError) -> Self {
        Self::Bindings { source }
    }
}

impl From<PendingNodeResultIntegrityError> for PendingNodeResultIntentError {
    fn from(source: PendingNodeResultIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// One immutable, worker-fenced and journal-anchored pending node result.
///
/// Storage must compare the exact live fence and base checkpoint under the run
/// lock, append the result journal event, and insert this record in one
/// transaction. A barrier later consumes the record into one successor
/// checkpoint; consumption never mutates these integrity-bearing fields.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingNodeResult {
    intent: PendingNodeResultIntent,
    fence: RunFence,
    journal_head: JournalHead,
    digest: Digest,
}

impl PendingNodeResult {
    /// Materializes a semantic result after its worker journal event commits.
    ///
    /// # Errors
    ///
    /// Returns [`PendingNodeResultError`] for crossed scope, stale ordering, or
    /// integrity failure.
    pub fn commit(
        intent: PendingNodeResultIntent,
        fence: RunFence,
        journal_head: JournalHead,
    ) -> Result<Self, PendingNodeResultError> {
        validate_result_shape(&intent, &fence, &journal_head)?;
        let digest = compute_record_digest(&PendingNodeResultDigestWire {
            intent_digest: intent.intent_digest,
            fence: &fence,
            journal_head: &journal_head,
        })?;
        Ok(Self {
            intent,
            fence,
            journal_head,
            digest,
        })
    }

    /// Restores a durable pending result and verifies every local invariant.
    ///
    /// Durable adapters must additionally verify the exact base checkpoint,
    /// full external invocation revisions, journal event source, and live or
    /// historical lease facts from their authoritative tables.
    ///
    /// # Errors
    ///
    /// Returns [`PendingNodeResultError`] for invariant or checksum failure.
    pub fn restore(
        intent: PendingNodeResultIntent,
        fence: RunFence,
        journal_head: JournalHead,
        digest: Digest,
    ) -> Result<Self, PendingNodeResultError> {
        let restored = Self::commit(intent, fence, journal_head)?;
        if restored.digest != digest {
            return Err(PendingNodeResultError::DigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the immutable semantic intent.
    #[must_use]
    pub const fn intent(&self) -> &PendingNodeResultIntent {
        &self.intent
    }

    /// Returns the physical worker and fencing epoch that won the commit.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Returns the exact journal prefix anchoring this result.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the complete domain-separated record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns a compact exact comparison and consumption token.
    #[must_use]
    pub fn head(&self) -> PendingNodeResultHead {
        PendingNodeResultHead {
            activation: self.intent.activation.clone(),
            intent_digest: self.intent.intent_digest,
            fence: self.fence.clone(),
            journal_head: self.journal_head.clone(),
            digest: self.digest,
        }
    }
}

impl fmt::Debug for PendingNodeResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingNodeResult")
            .field("intent", &self.intent)
            .field("fence", &self.fence)
            .field("journal_head", &self.journal_head)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for PendingNodeResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: PendingNodeResultIntent,
            fence: RunFence,
            journal_head: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.intent, wire.fence, wire.journal_head, wire.digest)
            .map_err(de::Error::custom)
    }
}

/// Compact exact identity of one pending node result.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PendingNodeResultHead {
    activation: NodeActivation,
    intent_digest: Digest,
    fence: RunFence,
    journal_head: JournalHead,
    digest: Digest,
}

impl PendingNodeResultHead {
    /// Constructs a trusted compact head while enforcing scope and base order.
    ///
    /// Obtain heads from [`PendingNodeResult::head`] unless restoring verified
    /// durable metadata.
    ///
    /// # Errors
    ///
    /// Returns [`PendingNodeResultError`] for crossed scope or a journal anchor
    /// that does not follow the activation checkpoint.
    pub fn new(
        activation: NodeActivation,
        intent_digest: Digest,
        fence: RunFence,
        journal_head: JournalHead,
        digest: Digest,
    ) -> Result<Self, PendingNodeResultError> {
        validate_basic_result_scope(&activation, &fence, &journal_head)?;
        Ok(Self {
            activation,
            intent_digest,
            fence,
            journal_head,
            digest,
        })
    }

    /// Returns the exact logical activation.
    #[must_use]
    pub const fn activation(&self) -> &NodeActivation {
        &self.activation
    }

    /// Returns the semantic idempotency fingerprint.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the winning physical worker fence.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Returns the result's exact journal anchor.
    #[must_use]
    pub const fn journal_head(&self) -> &JournalHead {
        &self.journal_head
    }

    /// Returns the exact record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for PendingNodeResultHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            activation: NodeActivation,
            intent_digest: Digest,
            fence: RunFence,
            journal_head: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.activation,
            wire.intent_digest,
            wire.fence,
            wire.journal_head,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid or corrupted pending node-result record.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum PendingNodeResultError {
    /// Worker fence crossed the activation tenant boundary.
    #[error("pending node result fence crosses the activation tenant boundary")]
    FenceTenantMismatch,
    /// Worker fence named another run.
    #[error("pending node result fence does not belong to the activation run")]
    FenceRunMismatch,
    /// Journal head crossed the activation tenant boundary.
    #[error("pending node result journal crosses the activation tenant boundary")]
    JournalTenantMismatch,
    /// Journal head named another run.
    #[error("pending node result journal does not belong to the activation run")]
    JournalRunMismatch,
    /// Result journal did not strictly follow the base checkpoint.
    #[error("pending node result journal does not follow its base checkpoint")]
    JournalNotAfterBase,
    /// Result durable time preceded the base checkpoint.
    #[error("pending node result clock precedes its base checkpoint")]
    ClockRegression,
    /// Result journal did not strictly follow every bound external result.
    #[error("pending node result journal does not follow every invocation binding")]
    JournalNotAfterBinding,
    /// Result durable time preceded a bound external result.
    #[error("pending node result clock precedes an invocation binding")]
    BindingClockRegression,
    /// Canonical integrity material could not be produced.
    #[error("pending node result integrity calculation failed: {source}")]
    Integrity {
        /// Exact integrity failure.
        #[source]
        source: PendingNodeResultIntegrityError,
    },
    /// Persisted record checksum did not match its fields.
    #[error("pending node result digest does not match its fields")]
    DigestMismatch,
}

impl From<PendingNodeResultIntegrityError> for PendingNodeResultError {
    fn from(source: PendingNodeResultIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

fn validate_result_shape(
    intent: &PendingNodeResultIntent,
    fence: &RunFence,
    journal_head: &JournalHead,
) -> Result<(), PendingNodeResultError> {
    validate_basic_result_scope(intent.activation(), fence, journal_head)?;
    for binding in intent.bindings().iter() {
        if journal_head.sequence() <= binding.journal_head().sequence() {
            return Err(PendingNodeResultError::JournalNotAfterBinding);
        }
        if journal_head.recorded_at() < binding.journal_head().recorded_at() {
            return Err(PendingNodeResultError::BindingClockRegression);
        }
    }
    Ok(())
}

fn validate_basic_result_scope(
    activation: &NodeActivation,
    fence: &RunFence,
    journal_head: &JournalHead,
) -> Result<(), PendingNodeResultError> {
    if fence.tenant_id() != activation.tenant_id() {
        return Err(PendingNodeResultError::FenceTenantMismatch);
    }
    if fence.run_id() != activation.run_id() {
        return Err(PendingNodeResultError::FenceRunMismatch);
    }
    if journal_head.tenant_id() != activation.tenant_id() {
        return Err(PendingNodeResultError::JournalTenantMismatch);
    }
    if journal_head.run_id() != activation.run_id() {
        return Err(PendingNodeResultError::JournalRunMismatch);
    }
    let base = activation.base_checkpoint().journal_head();
    if journal_head.sequence() <= base.sequence() {
        return Err(PendingNodeResultError::JournalNotAfterBase);
    }
    if journal_head.recorded_at() < base.recorded_at() {
        return Err(PendingNodeResultError::ClockRegression);
    }
    Ok(())
}

#[derive(Serialize)]
struct SchemaPayloadDigestWire<'a> {
    schema: &'a SchemaReference,
    data_digest: Digest,
}

#[derive(Serialize)]
struct PendingNodeResultIntentDigestWire<'a> {
    activation: &'a NodeActivation,
    state_change: &'a NodeStateChange,
    control: &'a NodeControl,
    bindings: &'a NodeInvocationBindings,
}

#[derive(Serialize)]
struct PendingNodeResultDigestWire<'a> {
    intent_digest: Digest,
    fence: &'a RunFence,
    journal_head: &'a JournalHead,
}

fn compute_payload_digest(
    domain: &[u8],
    schema: &SchemaReference,
    data: &BoundedJson,
) -> Result<Digest, PendingNodeResultIntegrityError> {
    let canonical = CanonicalJson::new(data)
        .map_err(|source| PendingNodeResultIntegrityError::PayloadCanonicalization { source })?;
    domain_separated_digest(
        domain,
        &SchemaPayloadDigestWire {
            schema,
            data_digest: canonical.digest(),
        },
    )
}

fn compute_intent_digest(
    value: &PendingNodeResultIntentDigestWire<'_>,
) -> Result<Digest, PendingNodeResultIntegrityError> {
    domain_separated_digest(INTENT_DIGEST_DOMAIN, value)
}

fn compute_record_digest(
    value: &PendingNodeResultDigestWire<'_>,
) -> Result<Digest, PendingNodeResultIntegrityError> {
    domain_separated_digest(RECORD_DIGEST_DOMAIN, value)
}

fn domain_separated_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, PendingNodeResultIntegrityError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| PendingNodeResultIntegrityError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

/// Failure to canonicalize pending node-result integrity material.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum PendingNodeResultIntegrityError {
    /// A schema-pinned payload was not RFC 8785 interoperable.
    #[error("node result payload canonicalization failed: {source}")]
    PayloadCanonicalization {
        /// Exact canonical JSON failure.
        #[source]
        source: CanonicalJsonError,
    },
    /// A closed typed checksum preimage could not be canonicalized.
    #[error("pending node result checksum preimage serialization failed")]
    CanonicalSerialization,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AttemptId, Checkpoint, EventId, FencingEpoch, JournalSequence, ModelInvocationRevision,
        NodeId, RunTimerKind, TimerId, Timestamp, ToolInvocationRevision,
    };
    use serde_json::{Value, from_value, json, to_value};

    fn checkpoint() -> Checkpoint {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-checkpoint-v1.json"))
                .unwrap();
        from_value(fixture["checkpoints"][0].clone()).unwrap()
    }

    fn node_activation(node: &str) -> NodeActivation {
        NodeActivation::new(
            checkpoint().head(),
            crate::GraphNamespace::root(),
            NodeId::new(node).unwrap(),
            Digest::sha256(format!("{node}-input")),
        )
    }

    fn invocation_id(suffix: &str) -> InvocationId {
        format!("01912345-6789-7abc-8def-0123456789{suffix}")
            .parse()
            .unwrap()
    }

    fn attempt_id(suffix: &str) -> AttemptId {
        format!("01912345-6789-7abc-8def-0123456789{suffix}")
            .parse()
            .unwrap()
    }

    fn journal(activation: &NodeActivation, sequence: u64) -> JournalHead {
        let base = activation.base_checkpoint().journal_head();
        let recorded_at = Timestamp::from_unix_micros(
            base.recorded_at().unix_micros() + i64::try_from(sequence - 1).unwrap() * 1_000_000,
        )
        .unwrap();
        let event_id: EventId =
            format!("01912345-6789-7abc-8def-0123456789{:02x}", 0xd0 + sequence)
                .parse()
                .unwrap();
        JournalHead::new(
            activation.tenant_id().clone(),
            activation.run_id(),
            JournalSequence::new(sequence).unwrap(),
            event_id,
            recorded_at,
            Digest::sha256(sequence.to_be_bytes()),
        )
    }

    fn fence(activation: &NodeActivation, suffix: &str, epoch: u64) -> RunFence {
        RunFence::new(
            activation.tenant_id().clone(),
            activation.run_id(),
            attempt_id(suffix),
            FencingEpoch::new(epoch).unwrap(),
        )
    }

    fn model_binding(activation: &NodeActivation) -> NodeInvocationBinding {
        let head = ModelInvocationHead::new(
            activation.tenant_id().clone(),
            activation.run_id(),
            invocation_id("d1"),
            ModelInvocationRevision::new(2).unwrap(),
            ModelInvocationStatus::Committed,
            Some(attempt_id("a1")),
            journal(activation, 2),
            Digest::sha256(b"model-record"),
        )
        .unwrap();
        NodeInvocationBinding::restore_model(activation.clone(), head).unwrap()
    }

    fn tool_binding(activation: &NodeActivation) -> NodeInvocationBinding {
        let head = ToolInvocationHead::new(
            activation.tenant_id().clone(),
            activation.run_id(),
            invocation_id("d2"),
            ToolInvocationRevision::new(2).unwrap(),
            ToolInvocationStatus::Committed,
            Some(attempt_id("a2")),
            journal(activation, 3),
            Digest::sha256(b"tool-record"),
        )
        .unwrap();
        NodeInvocationBinding::restore_tool(activation.clone(), head).unwrap()
    }

    fn bounded(value: Value) -> BoundedJson {
        BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM).unwrap()
    }

    fn update(activation: &NodeActivation) -> NodeStateUpdate {
        NodeStateUpdate::new(
            activation.base_checkpoint().graph().state_schema().clone(),
            bounded(json!({"approved": true, "amount": 42})),
        )
        .unwrap()
    }

    fn route_intent(activation: &NodeActivation) -> PendingNodeResultIntent {
        let bindings = NodeInvocationBindings::try_new(
            activation,
            [tool_binding(activation), model_binding(activation)],
        )
        .unwrap();
        PendingNodeResultIntent::new(
            activation.clone(),
            NodeStateChange::Update {
                update: update(activation),
            },
            NodeControl::Route {
                route_id: RouteId::new("approved").unwrap(),
            },
            bindings,
        )
        .unwrap()
    }

    #[test]
    fn route_ids_enforce_the_closed_wire_grammar() {
        for valid in ["approved", "route.v2", "retry_path", "A-1"] {
            let route = RouteId::new(valid).unwrap();
            assert_eq!(route.as_str(), valid);
            assert_eq!(from_value::<RouteId>(json!(valid)).unwrap(), route);
        }
        for invalid in ["", ".", "..", "-route", "route/path", "route space"] {
            assert!(RouteId::new(invalid).is_err(), "accepted {invalid:?}");
        }
        assert!(RouteId::new("r".repeat(RouteId::MAX_LEN + 1)).is_err());
        assert!(from_value::<RouteId>(json!(42)).is_err());
    }

    #[test]
    fn schema_pinned_payloads_are_integrity_bound_and_debug_redacted() {
        let activation = node_activation("authorize");
        let update = update(&activation);
        let wire = to_value(&update).unwrap();
        assert_eq!(from_value::<NodeStateUpdate>(wire.clone()).unwrap(), update);

        let mut tampered = wire;
        tampered["data"]["approved"] = json!(false);
        assert!(from_value::<NodeStateUpdate>(tampered).is_err());

        let secret = "do-not-print-node-payload";
        let terminal = NodeTerminalOutput::new(
            activation.base_checkpoint().graph().state_schema().clone(),
            bounded(json!({"secret": secret})),
        )
        .unwrap();
        assert!(!format!("{terminal:?}").contains(secret));
        let mut terminal_wire = to_value(&terminal).unwrap();
        terminal_wire["digest"] = json!(Digest::sha256(b"wrong"));
        assert!(from_value::<NodeTerminalOutput>(terminal_wire).is_err());
    }

    #[test]
    fn invocation_bindings_are_committed_activation_bound_unique_and_canonical() {
        let activation = node_activation("authorize");
        let tool = tool_binding(&activation);
        let model = model_binding(&activation);
        let bindings =
            NodeInvocationBindings::try_new(&activation, [tool.clone(), model.clone()]).unwrap();
        assert_eq!(bindings.len(), 2);
        assert_eq!(
            bindings
                .iter()
                .map(NodeInvocationBinding::kind)
                .collect::<Vec<_>>(),
            vec![
                NodeInvocationBindingKind::Model,
                NodeInvocationBindingKind::Tool
            ]
        );
        assert_eq!(
            from_value::<NodeInvocationBindings>(to_value(&bindings).unwrap()).unwrap(),
            bindings
        );

        assert_eq!(
            NodeInvocationBindings::try_new(&activation, [tool.clone(), tool]),
            Err(NodeInvocationBindingsError::Duplicate {
                kind: NodeInvocationBindingKind::Tool,
                invocation_id: invocation_id("d2"),
            })
        );

        let other = node_activation("another-node");
        assert_eq!(
            NodeInvocationBindings::try_new(&other, [model]),
            Err(NodeInvocationBindingsError::ActivationMismatch)
        );

        let executing = ModelInvocationHead::new(
            activation.tenant_id().clone(),
            activation.run_id(),
            invocation_id("d3"),
            ModelInvocationRevision::new(1).unwrap(),
            ModelInvocationStatus::Executing,
            Some(attempt_id("a3")),
            journal(&activation, 2),
            Digest::sha256(b"executing"),
        )
        .unwrap();
        assert_eq!(
            NodeInvocationBinding::restore_model(activation, executing),
            Err(NodeInvocationBindingError::ModelNotCommitted {
                actual: ModelInvocationStatus::Executing,
            })
        );
    }

    #[test]
    fn node_wait_batches_are_bounded_identity_unique_and_closed() {
        let due_at = "2099-01-01T00:00:00.000000Z".parse().unwrap();
        assert_eq!(NodeWaits::try_new([]), Err(NodeWaitsError::Empty));

        let timer_id = TimerId::generate();
        let timer = NodeWait::timer(timer_id, RunTimerKind::Sleep, due_at);
        assert_eq!(
            NodeWaits::try_new([timer.clone(), timer]),
            Err(NodeWaitsError::DuplicateIdentity)
        );

        let oversized = (0..=NodeWaits::MAX_LEN)
            .map(|_| NodeWait::timer(TimerId::generate(), RunTimerKind::Sleep, due_at));
        assert_eq!(
            NodeWaits::try_new(oversized),
            Err(NodeWaitsError::TooMany {
                maximum: NodeWaits::MAX_LEN,
                actual: NodeWaits::MAX_LEN + 1,
            })
        );

        let waits = NodeWaits::try_new([NodeWait::timer(
            timer_id,
            RunTimerKind::RetryBackoff,
            due_at,
        )])
        .unwrap();
        let wire = to_value(&waits).unwrap();
        assert_eq!(from_value::<NodeWaits>(wire.clone()).unwrap(), waits);
        let mut crossed = wire;
        crossed[0]["registered_at"] = json!("2098-01-01T00:00:00.000000Z");
        assert!(from_value::<NodeWaits>(crossed).is_err());

        let schema = to_value(schemars::schema_for!(NodeWaits)).unwrap();
        assert_eq!(schema["minItems"], 1);
        assert_eq!(schema["maxItems"], NodeWaits::MAX_LEN);
        assert_eq!(schema["uniqueItems"], true);
    }

    #[test]
    fn interrupt_node_wait_preserves_policy_and_uses_the_lifecycle_event_clock() {
        let activation = node_activation("approval");
        let secret = "approve-production-deployment";
        let request_payload = JournalPayload::new(
            SchemaReference::new(
                "https://stknot.com/schemas/tests/node-wait-request/1.0.0"
                    .parse()
                    .unwrap(),
                crate::Version::new(1, 0, 0),
                Digest::sha256(b"node wait request schema"),
            ),
            crate::JournalEventKind::new("node-wait-request").unwrap(),
            bounded(json!({"request": secret})),
        )
        .unwrap();
        let event_id = EventId::generate();
        let interrupt_id = crate::InterruptId::generate();
        let action_digest = Digest::sha256(b"approve deployment action");
        let registered_at = Timestamp::from_unix_micros(
            activation
                .base_checkpoint()
                .journal_head()
                .recorded_at()
                .unix_micros()
                + 5_000_000,
        )
        .unwrap();
        let expires_at =
            Timestamp::from_unix_micros(registered_at.unix_micros() + 60_000_000).unwrap();
        let wait = NodeWait::interrupt(
            interrupt_id,
            crate::RunInterruptKind::Approval,
            request_payload,
            action_digest,
            None,
            ScopeSet::empty(),
            Some(expires_at),
        );
        assert!(!format!("{wait:?}").contains(secret));

        let mut registrations = NodeWaits::try_new([wait])
            .unwrap()
            .registration_intents(activation.tenant_id(), activation.run_id(), event_id)
            .unwrap();
        let registration = registrations.pop().unwrap();
        let WaitRegistrationIntent::Interrupt { request } = &registration else {
            panic!("expected interrupt registration")
        };
        assert_eq!(request.interrupt_id(), interrupt_id);
        assert_eq!(request.request_event_id(), event_id);
        assert_eq!(request.action_digest(), action_digest);
        assert_eq!(request.expires_at(), Some(expires_at));

        let journal = JournalHead::new(
            activation.tenant_id().clone(),
            activation.run_id(),
            JournalSequence::new(10).unwrap(),
            event_id,
            registered_at,
            Digest::sha256(b"node wait lifecycle event"),
        );
        let crate::DurableWait::Interrupt { request } = registration.commit(journal).unwrap()
        else {
            panic!("expected materialized interrupt")
        };
        assert_eq!(request.marker().requested_at(), registered_at);
        assert_eq!(request.marker().expires_at(), Some(expires_at));
    }

    #[test]
    fn pending_result_separates_semantic_idempotency_from_physical_provenance() {
        let activation = node_activation("authorize");
        let intent = route_intent(&activation);
        let first = PendingNodeResult::commit(
            intent.clone(),
            fence(&activation, "b1", 1),
            journal(&activation, 4),
        )
        .unwrap();
        let replacement = PendingNodeResult::commit(
            intent.clone(),
            fence(&activation, "b2", 2),
            journal(&activation, 5),
        )
        .unwrap();

        assert_eq!(
            first.intent().intent_digest(),
            replacement.intent().intent_digest()
        );
        assert_ne!(first.digest(), replacement.digest());
        assert_eq!(
            from_value::<PendingNodeResult>(to_value(&first).unwrap()).unwrap(),
            first
        );
        assert_eq!(
            from_value::<PendingNodeResultHead>(to_value(first.head()).unwrap()).unwrap(),
            first.head()
        );

        let mut changed_route = to_value(&intent).unwrap();
        changed_route["control"]["route_id"] = json!("rejected");
        assert!(from_value::<PendingNodeResultIntent>(changed_route).is_err());

        let mut changed_fence = to_value(&first).unwrap();
        changed_fence["fence"]["epoch"] = json!("2");
        assert!(from_value::<PendingNodeResult>(changed_fence).is_err());

        let mut extra = to_value(&first).unwrap();
        extra["unsafe_extension"] = json!(true);
        assert!(from_value::<PendingNodeResult>(extra).is_err());
    }

    #[test]
    fn result_anchor_must_follow_bindings_and_waits_materialize_at_lifecycle_event() {
        let activation = node_activation("authorize");
        let intent = route_intent(&activation);
        assert_eq!(
            PendingNodeResult::commit(
                intent.clone(),
                fence(&activation, "b1", 1),
                journal(&activation, 3),
            ),
            Err(PendingNodeResultError::JournalNotAfterBinding)
        );

        let timer_id = "01912345-6789-7abc-8def-0123456789e1"
            .parse::<TimerId>()
            .unwrap();
        let due_at = Timestamp::from_unix_micros(
            activation
                .base_checkpoint()
                .journal_head()
                .recorded_at()
                .unix_micros()
                + 5_000_000,
        )
        .unwrap();
        let waits =
            NodeWaits::try_new([NodeWait::timer(timer_id, RunTimerKind::Sleep, due_at)]).unwrap();
        let lifecycle_event_id: EventId = "01912345-6789-7abc-8def-0123456789f1".parse().unwrap();
        let registrations = waits
            .registration_intents(
                activation.tenant_id(),
                activation.run_id(),
                lifecycle_event_id,
            )
            .unwrap();
        assert_eq!(registrations.len(), 1);
        let WaitRegistrationIntent::Timer { timer } = &registrations[0] else {
            panic!("expected timer registration");
        };
        assert_eq!(timer.timer_id(), timer_id);
        assert_eq!(timer.registration_event_id(), lifecycle_event_id);
        assert_eq!(timer.due_at(), due_at);

        let intent = PendingNodeResultIntent::new(
            activation.clone(),
            NodeStateChange::Unchanged,
            NodeControl::Wait { waits },
            NodeInvocationBindings::empty(),
        )
        .unwrap();
        PendingNodeResult::commit(intent, fence(&activation, "b1", 1), journal(&activation, 2))
            .unwrap();
    }

    #[test]
    fn pending_result_public_schema_is_closed_and_bounded() {
        let schema = to_value(schemars::schema_for!(PendingNodeResult)).unwrap();
        assert_eq!(schema["additionalProperties"], false);

        let bindings_schema = to_value(schemars::schema_for!(NodeInvocationBindings)).unwrap();
        assert_eq!(bindings_schema["maxItems"], NodeInvocationBindings::MAX_LEN);
        assert_eq!(bindings_schema["uniqueItems"], true);
    }
}
