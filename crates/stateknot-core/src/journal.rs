// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Integrity-bound, append-only journal envelopes for durable run facts.
//!
//! The journal is the ordered source of truth for recovery and event streaming.
//! Core validates canonical payloads, idempotency identity, sequence continuity,
//! checksums, and worker provenance. A production store remains responsible for
//! serializing appends under a locked run head and, for worker writes, checking
//! the exact current unexpired lease with the database clock in that same
//! transaction.

use std::{fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::decimal::{UnsignedDecimalError, parse_bounded_u64};
use crate::{
    BoundedJson, BoundedJsonError, CanonicalJson, CanonicalJsonError, Digest, EventId, JsonLimits,
    RunFence, RunId, RunLease, RunLeaseValidationError, SchemaReference, TenantId, Timestamp,
};

const MAX_DATABASE_ORDINAL: u64 = i64::MAX as u64;
const POSITIVE_I64_PATTERN: &str = "^[1-9][0-9]{0,18}$";
const EVENT_KIND_PATTERN: &str = "^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$";
const INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-journal-intent-v1\0";
const EVENT_DIGEST_DOMAIN: &[u8] = b"stateknot-journal-event-v1\0";

/// Positive, contiguous event position within one tenant-scoped run journal.
///
/// The first record is sequence one. The maximum matches `PostgreSQL` signed
/// `BIGINT`, and the JSON wire form is a canonical decimal string.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JournalSequence(u64);

impl JournalSequence {
    /// Sequence of the first event in a run journal.
    pub const FIRST: Self = Self(1);

    /// Largest sequence representable by the `PostgreSQL` v1 schema.
    pub const MAX: Self = Self(MAX_DATABASE_ORDINAL);

    /// Constructs a positive `PostgreSQL`-compatible sequence.
    ///
    /// # Errors
    ///
    /// Returns [`JournalSequenceError::Zero`] for zero and
    /// [`JournalSequenceError::AboveMaximum`] above signed `BIGINT`.
    pub const fn new(value: u64) -> Result<Self, JournalSequenceError> {
        if value == 0 {
            return Err(JournalSequenceError::Zero);
        }
        if value > MAX_DATABASE_ORDINAL {
            return Err(JournalSequenceError::AboveMaximum);
        }
        Ok(Self(value))
    }

    /// Returns the integer sequence.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the exact contiguous successor or `None` at the storage limit.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        if self.0 == MAX_DATABASE_ORDINAL {
            None
        } else {
            Some(Self(self.0 + 1))
        }
    }
}

impl fmt::Display for JournalSequence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for JournalSequence {
    type Err = JournalSequenceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = parse_bounded_u64(value, MAX_DATABASE_ORDINAL)
            .map_err(JournalSequenceError::from_decimal_error)?;
        Self::new(value)
    }
}

impl Serialize for JournalSequence {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for JournalSequence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(JournalSequenceVisitor)
    }
}

impl JsonSchema for JournalSequence {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "JournalSequence".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::JournalSequence").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 19,
            "pattern": POSITIVE_I64_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

struct JournalSequenceVisitor;

impl de::Visitor<'_> for JournalSequenceVisitor {
    type Value = JournalSequence;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical positive decimal PostgreSQL BIGINT journal sequence")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

/// Invalid canonical journal sequence.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JournalSequenceError {
    /// Sequence zero is outside the one-based journal.
    #[error("journal sequence must be positive")]
    Zero,

    /// The sequence exceeded `PostgreSQL` signed `BIGINT`.
    #[error("journal sequence exceeds the PostgreSQL BIGINT maximum")]
    AboveMaximum,

    /// The wire value was empty or contained a non-decimal byte.
    #[error("journal sequence must contain only unsigned ASCII decimal digits")]
    InvalidFormat,

    /// The wire value contained a leading zero.
    #[error("journal sequence must use canonical decimal text")]
    NonCanonical,
}

impl JournalSequenceError {
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

/// Stable lower-kebab-case semantic name of a durable event payload.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JournalEventKind(Box<str>);

impl JournalEventKind {
    /// Maximum UTF-8 byte length of an event kind.
    pub const MAX_LEN: usize = 96;

    /// Validates and constructs an event kind.
    ///
    /// # Errors
    ///
    /// Returns [`JournalEventKindError`] unless the value is a non-empty,
    /// bounded lower-kebab-case ASCII identifier beginning with a letter.
    pub fn new(value: impl Into<String>) -> Result<Self, JournalEventKindError> {
        let value = value.into();
        validate_event_kind(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the stable semantic name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for JournalEventKind {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for JournalEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("JournalEventKind")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for JournalEventKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for JournalEventKind {
    type Err = JournalEventKindError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for JournalEventKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for JournalEventKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(JournalEventKindVisitor)
    }
}

impl JsonSchema for JournalEventKind {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "JournalEventKind".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::JournalEventKind").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 96,
            "pattern": EVENT_KIND_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

struct JournalEventKindVisitor;

impl de::Visitor<'_> for JournalEventKindVisitor {
    type Value = JournalEventKind;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded lower-kebab-case journal event kind")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

fn validate_event_kind(value: &str) -> Result<(), JournalEventKindError> {
    if value.is_empty() {
        return Err(JournalEventKindError::Empty);
    }
    if value.len() > JournalEventKind::MAX_LEN {
        return Err(JournalEventKindError::TooLong {
            maximum: JournalEventKind::MAX_LEN,
            actual: value.len(),
        });
    }
    let bytes = value.as_bytes();
    if !bytes[0].is_ascii_lowercase() {
        return Err(JournalEventKindError::InvalidStart);
    }
    for (index, byte) in bytes.iter().copied().enumerate() {
        if !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && byte != b'-' {
            return Err(JournalEventKindError::InvalidByte { index });
        }
        if byte == b'-' && (index + 1 == bytes.len() || (index > 0 && bytes[index - 1] == b'-')) {
            return Err(JournalEventKindError::NonCanonicalSeparator { index });
        }
    }
    Ok(())
}

/// Invalid durable event kind.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JournalEventKindError {
    /// The event kind was empty.
    #[error("journal event kind must not be empty")]
    Empty,

    /// The event kind exceeded its hard byte bound.
    #[error("journal event kind is {actual} bytes; maximum is {maximum}")]
    TooLong {
        /// Maximum accepted byte length.
        maximum: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The first byte was not a lowercase ASCII letter.
    #[error("journal event kind must begin with a lowercase ASCII letter")]
    InvalidStart,

    /// A byte was outside lowercase ASCII letters, digits, and hyphen.
    #[error("journal event kind contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset.
        index: usize,
    },

    /// A hyphen was repeated or trailing.
    #[error("journal event kind contains a non-canonical hyphen at offset {index}")]
    NonCanonicalSeparator {
        /// Zero-based byte offset.
        index: usize,
    },
}

/// Schema-bound durable event payload whose checksum uses RFC 8785 bytes.
///
/// The stable payload envelope is exactly `{schema, kind, data}`. Construction
/// validates the complete envelope under [`JsonLimits::MAXIMUM`] and caches its
/// RFC 8785 SHA-256 checksum. Schema evaluation itself is performed by the
/// trusted local registry before this value is admitted.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalPayload {
    schema: SchemaReference,
    kind: JournalEventKind,
    data: BoundedJson,
    #[serde(skip)]
    #[schemars(skip)]
    digest: Digest,
}

impl JournalPayload {
    /// Constructs an integrity-ready payload envelope.
    ///
    /// # Errors
    ///
    /// Returns [`JournalPayloadError`] if the complete envelope exceeds core
    /// bounds, cannot be serialized, or contains a non-interoperable JSON
    /// integer for RFC 8785 canonicalization.
    pub fn new(
        schema: SchemaReference,
        kind: JournalEventKind,
        data: BoundedJson,
    ) -> Result<Self, JournalPayloadError> {
        let canonical = canonical_payload(&schema, &kind, &data)?;
        Ok(Self {
            schema,
            kind,
            data,
            digest: canonical.digest(),
        })
    }

    /// Returns the pinned payload schema.
    #[must_use]
    pub const fn schema(&self) -> &SchemaReference {
        &self.schema
    }

    /// Returns the semantic event kind.
    #[must_use]
    pub const fn kind(&self) -> &JournalEventKind {
        &self.kind
    }

    /// Returns the bounded event data without permitting mutation.
    #[must_use]
    pub const fn data(&self) -> &BoundedJson {
        &self.data
    }

    /// Returns SHA-256 over the exact RFC 8785 payload envelope bytes.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Reproduces the exact RFC 8785 payload envelope bytes for durable storage.
    ///
    /// # Errors
    ///
    /// Fails closed if canonicalization unexpectedly no longer reproduces the
    /// already validated envelope.
    pub fn canonical_json(&self) -> Result<CanonicalJson, JournalPayloadError> {
        let canonical = canonical_payload(&self.schema, &self.kind, &self.data)?;
        if canonical.digest() != self.digest {
            return Err(JournalPayloadError::DigestChanged);
        }
        Ok(canonical)
    }
}

impl fmt::Debug for JournalPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JournalPayload")
            .field("schema", &self.schema)
            .field("kind", &self.kind)
            .field("data_stats", &self.data.stats())
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for JournalPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema: SchemaReference,
            kind: JournalEventKind,
            data: BoundedJson,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.schema, wire.kind, wire.data).map_err(de::Error::custom)
    }
}

#[derive(Serialize)]
struct PayloadEnvelope<'a> {
    schema: &'a SchemaReference,
    kind: &'a JournalEventKind,
    data: &'a BoundedJson,
}

fn canonical_payload(
    schema: &SchemaReference,
    kind: &JournalEventKind,
    data: &BoundedJson,
) -> Result<CanonicalJson, JournalPayloadError> {
    let value = serde_json::to_value(PayloadEnvelope { schema, kind, data })
        .map_err(|_| JournalPayloadError::EnvelopeSerialization)?;
    let bounded = BoundedJson::try_from_value_with_limits(value, JsonLimits::MAXIMUM)
        .map_err(JournalPayloadError::envelope_bounds)?;
    CanonicalJson::new(&bounded).map_err(JournalPayloadError::canonical)
}

/// Invalid canonical durable event payload.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JournalPayloadError {
    /// Typed envelope serialization failed before canonicalization.
    #[error("journal payload envelope serialization failed")]
    EnvelopeSerialization,

    /// The complete schema/kind/data envelope exceeded core JSON bounds.
    #[error("journal payload envelope violates JSON bounds: {source}")]
    EnvelopeBounds {
        /// Underlying bounded JSON failure.
        #[source]
        source: BoundedJsonError,
    },

    /// RFC 8785 canonicalization failed closed.
    #[error("journal payload canonicalization failed: {source}")]
    Canonical {
        /// Underlying canonical JSON failure.
        #[source]
        source: CanonicalJsonError,
    },

    /// Recomputed canonical bytes no longer matched the admitted checksum.
    #[error("journal payload canonical digest changed after validation")]
    DigestChanged,
}

impl JournalPayloadError {
    const fn envelope_bounds(source: BoundedJsonError) -> Self {
        Self::EnvelopeBounds { source }
    }

    const fn canonical(source: CanonicalJsonError) -> Self {
        Self::Canonical { source }
    }
}

/// Trusted runtime origin assigned to one journal append.
///
/// `ControlPlane` is available only to the trusted API/scheduler transaction
/// path; it is not a privilege a remote or worker request can self-declare.
/// Worker writes carry the exact token that the store must fence atomically.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JournalEventSource {
    /// Trusted API, scheduler, recovery, or migration control path.
    ControlPlane,
    /// Physical worker attempt holding a run lease.
    Worker {
        /// Exact run-scoped fencing token.
        fence: RunFence,
    },
}

impl JournalEventSource {
    /// Constructs a trusted control-plane source marker.
    #[must_use]
    pub const fn control_plane() -> Self {
        Self::ControlPlane
    }

    /// Constructs a worker source marker.
    #[must_use]
    pub const fn worker(fence: RunFence) -> Self {
        Self::Worker { fence }
    }

    /// Returns the worker fence, if this event originates from an attempt.
    #[must_use]
    pub const fn worker_fence(&self) -> Option<&RunFence> {
        match self {
            Self::Worker { fence } => Some(fence),
            Self::ControlPlane => None,
        }
    }
}

/// Stable idempotent request to append one semantic event.
///
/// Retrying the same `EventId` is successful only when every intent field and
/// its digest match the existing event. Reusing an ID with different content is
/// a conflict, never last-write-wins behavior.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalEventIntent {
    tenant_id: TenantId,
    run_id: RunId,
    event_id: EventId,
    source: JournalEventSource,
    payload: JournalPayload,
    intent_digest: Digest,
}

impl JournalEventIntent {
    /// Constructs a control-plane event intent.
    ///
    /// # Errors
    ///
    /// Returns [`JournalIntentError::Integrity`] if the small typed digest
    /// preimage unexpectedly cannot be canonicalized.
    pub fn control_plane(
        tenant_id: TenantId,
        run_id: RunId,
        event_id: EventId,
        payload: JournalPayload,
    ) -> Result<Self, JournalIntentError> {
        Self::from_parts(
            tenant_id,
            run_id,
            event_id,
            JournalEventSource::ControlPlane,
            payload,
        )
    }

    /// Constructs a worker event intent bound to its run-scoped fence.
    ///
    /// # Errors
    ///
    /// Returns [`JournalIntentError`] if the fence crosses the supplied tenant
    /// or run, or the digest preimage cannot be canonicalized.
    pub fn worker(
        tenant_id: TenantId,
        run_id: RunId,
        event_id: EventId,
        fence: RunFence,
        payload: JournalPayload,
    ) -> Result<Self, JournalIntentError> {
        Self::from_parts(
            tenant_id,
            run_id,
            event_id,
            JournalEventSource::worker(fence),
            payload,
        )
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the run journal identity.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the stable idempotency identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Returns the trusted source marker.
    #[must_use]
    pub const fn source(&self) -> &JournalEventSource {
        &self.source
    }

    /// Returns the schema-bound payload.
    #[must_use]
    pub const fn payload(&self) -> &JournalPayload {
        &self.payload
    }

    /// Returns the domain-separated digest of the complete append intent.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    fn from_parts(
        tenant_id: TenantId,
        run_id: RunId,
        event_id: EventId,
        source: JournalEventSource,
        payload: JournalPayload,
    ) -> Result<Self, JournalIntentError> {
        validate_source_scope(&tenant_id, run_id, &source)?;
        let intent_digest =
            compute_intent_digest(&tenant_id, run_id, event_id, &source, payload.digest())
                .map_err(JournalIntentError::integrity)?;
        Ok(Self {
            tenant_id,
            run_id,
            event_id,
            source,
            payload,
            intent_digest,
        })
    }
}

impl fmt::Debug for JournalEventIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JournalEventIntent")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("event_id", &self.event_id)
            .field("source", &self.source)
            .field("payload", &self.payload)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for JournalEventIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            event_id: EventId,
            source: JournalEventSource,
            payload: JournalPayload,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        let intent = Self::from_parts(
            wire.tenant_id,
            wire.run_id,
            wire.event_id,
            wire.source,
            wire.payload,
        )
        .map_err(de::Error::custom)?;
        if intent.intent_digest != wire.intent_digest {
            return Err(de::Error::custom(JournalIntentError::IntentDigestMismatch));
        }
        Ok(intent)
    }
}

fn validate_source_scope(
    tenant_id: &TenantId,
    run_id: RunId,
    source: &JournalEventSource,
) -> Result<(), JournalIntentError> {
    if let JournalEventSource::Worker { fence } = source {
        if fence.tenant_id() != tenant_id {
            return Err(JournalIntentError::FenceTenantMismatch);
        }
        if fence.run_id() != run_id {
            return Err(JournalIntentError::FenceRunMismatch);
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct IntentDigestWire<'a> {
    tenant_id: &'a TenantId,
    run_id: RunId,
    event_id: EventId,
    source: &'a JournalEventSource,
    payload_digest: Digest,
}

fn compute_intent_digest(
    tenant_id: &TenantId,
    run_id: RunId,
    event_id: EventId,
    source: &JournalEventSource,
    payload_digest: Digest,
) -> Result<Digest, JournalIntegrityError> {
    domain_separated_digest(
        INTENT_DIGEST_DOMAIN,
        &IntentDigestWire {
            tenant_id,
            run_id,
            event_id,
            source,
            payload_digest,
        },
    )
}

/// Invalid relationship or checksum in an event append intent.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JournalIntentError {
    /// A worker fence named another tenant.
    #[error("journal worker fence crosses the intent tenant boundary")]
    FenceTenantMismatch,

    /// A worker fence named another run.
    #[error("journal worker fence does not belong to the intent run")]
    FenceRunMismatch,

    /// The typed intent digest preimage could not be canonicalized.
    #[error("journal intent integrity calculation failed: {source}")]
    Integrity {
        /// Underlying integrity failure.
        #[source]
        source: JournalIntegrityError,
    },

    /// A serialized intent checksum did not match its fields.
    #[error("journal intent digest does not match its fields")]
    IntentDigestMismatch,
}

impl JournalIntentError {
    const fn integrity(source: JournalIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Exact committed journal head used for optimistic append comparison.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalHead {
    tenant_id: TenantId,
    run_id: RunId,
    sequence: JournalSequence,
    event_id: EventId,
    recorded_at: Timestamp,
    digest: Digest,
}

impl JournalHead {
    /// Constructs a head recovered from trusted durable metadata.
    #[must_use]
    pub const fn new(
        tenant_id: TenantId,
        run_id: RunId,
        sequence: JournalSequence,
        event_id: EventId,
        recorded_at: Timestamp,
        digest: Digest,
    ) -> Self {
        Self {
            tenant_id,
            run_id,
            sequence,
            event_id,
            recorded_at,
            digest,
        }
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the run journal identity.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the committed head sequence.
    #[must_use]
    pub const fn sequence(&self) -> JournalSequence {
        self.sequence
    }

    /// Returns the committed head event identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Returns the head's durable clock observation.
    #[must_use]
    pub const fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// Returns the head event checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// Optimistic journal precondition supplied with an append request.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum JournalExpectation {
    /// The run journal must not contain any event.
    Empty,
    /// The current durable head must exactly match every supplied field.
    Exact {
        /// Previously observed committed head.
        head: JournalHead,
    },
}

impl JournalExpectation {
    /// Constructs an empty-journal expectation.
    #[must_use]
    pub const fn empty() -> Self {
        Self::Empty
    }

    /// Constructs an exact-head expectation.
    #[must_use]
    pub const fn exact(head: JournalHead) -> Self {
        Self::Exact { head }
    }

    /// Returns the exact expected head, if any.
    #[must_use]
    pub const fn head(&self) -> Option<&JournalHead> {
        match self {
            Self::Empty => None,
            Self::Exact { head } => Some(head),
        }
    }
}

/// Atomic append request combining an idempotent intent and exact head check.
///
/// Store implementations must first look up `(tenant, run, event_id)`. An
/// identical existing intent is returned idempotently even if the caller's head
/// is now stale; a conflicting reuse is rejected. Only a new event proceeds to
/// exact-head and optional lease validation under the locked run row.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalAppend {
    expectation: JournalExpectation,
    intent: JournalEventIntent,
}

impl JournalAppend {
    /// Constructs an append request whose exact head belongs to the same scope.
    ///
    /// # Errors
    ///
    /// Returns [`JournalAppendError`] if an exact head crosses tenant or run
    /// boundaries.
    pub fn new(
        expectation: JournalExpectation,
        intent: JournalEventIntent,
    ) -> Result<Self, JournalAppendError> {
        if let Some(head) = expectation.head() {
            if head.tenant_id != intent.tenant_id {
                return Err(JournalAppendError::HeadTenantMismatch);
            }
            if head.run_id != intent.run_id {
                return Err(JournalAppendError::HeadRunMismatch);
            }
        }
        Ok(Self {
            expectation,
            intent,
        })
    }

    /// Returns the optimistic head precondition.
    #[must_use]
    pub const fn expectation(&self) -> &JournalExpectation {
        &self.expectation
    }

    /// Returns the idempotent event intent.
    #[must_use]
    pub const fn intent(&self) -> &JournalEventIntent {
        &self.intent
    }

    /// Returns the worker fence that must be checked by the store, if any.
    #[must_use]
    pub const fn worker_fence(&self) -> Option<&RunFence> {
        self.intent.source.worker_fence()
    }

    /// Performs a non-authoritative worker lease preflight.
    ///
    /// A production store must repeat this comparison using its current locked
    /// run row and database clock in the commit transaction.
    ///
    /// # Errors
    ///
    /// Returns [`JournalAuthorityError::ControlPlaneSource`] for a control-plane
    /// append or wraps an exact fence/expiry mismatch.
    pub fn validate_worker_lease(
        &self,
        lease: &RunLease,
        observed_at: Timestamp,
    ) -> Result<(), JournalAuthorityError> {
        let fence = self
            .worker_fence()
            .ok_or(JournalAuthorityError::ControlPlaneSource)?;
        lease
            .validate_write(fence, observed_at)
            .map_err(JournalAuthorityError::lease)
    }
}

impl fmt::Debug for JournalAppend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JournalAppend")
            .field("expectation", &self.expectation)
            .field("intent", &self.intent)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for JournalAppend {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            expectation: JournalExpectation,
            intent: JournalEventIntent,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.expectation, wire.intent).map_err(de::Error::custom)
    }
}

/// Invalid exact-head scope in an append request.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JournalAppendError {
    /// The expected head crossed a tenant boundary.
    #[error("journal expected head crosses the append tenant boundary")]
    HeadTenantMismatch,

    /// The expected head named a different run.
    #[error("journal expected head does not belong to the append run")]
    HeadRunMismatch,
}

/// Rejected worker-authority preflight.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JournalAuthorityError {
    /// The append belongs to a trusted non-worker path.
    #[error("control-plane journal append has no worker lease")]
    ControlPlaneSource,

    /// The current lease rejected the worker token or observation.
    #[error("journal worker lease validation failed: {source}")]
    Lease {
        /// Exact lease validation failure.
        #[source]
        source: RunLeaseValidationError,
    },
}

impl JournalAuthorityError {
    const fn lease(source: RunLeaseValidationError) -> Self {
        Self::Lease { source }
    }
}

/// One committed, self-validating record in a run's append-only journal.
///
/// `digest` is a domain-separated SHA-256 checksum over stable event metadata,
/// the full intent checksum, payload checksum, and previous event checksum. It
/// detects accidental corruption, omission, and reordering; an unkeyed hash
/// chain is not proof against a privileged actor able to rewrite every record.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalEvent {
    tenant_id: TenantId,
    run_id: RunId,
    sequence: JournalSequence,
    event_id: EventId,
    recorded_at: Timestamp,
    source: JournalEventSource,
    payload: JournalPayload,
    payload_digest: Digest,
    intent_digest: Digest,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous_digest: Option<Digest>,
    digest: Digest,
}

impl JournalEvent {
    /// Restores and verifies an event from durable storage columns.
    ///
    /// The intent reconstructs and validates tenant, run, event identity,
    /// source, payload, payload checksum, and intent checksum. This method then
    /// validates sequence/predecessor shape and recomputes the complete event
    /// checksum before returning a usable event.
    ///
    /// A store that persists redundant payload or intent digest columns must
    /// compare those columns with `intent` before calling this method.
    ///
    /// # Errors
    ///
    /// Returns [`JournalEventError`] when the sequence/predecessor shape or
    /// complete event checksum does not match the reconstructed event.
    pub fn restore(
        intent: JournalEventIntent,
        sequence: JournalSequence,
        recorded_at: Timestamp,
        previous_digest: Option<Digest>,
        digest: Digest,
    ) -> Result<Self, JournalEventError> {
        let payload_digest = intent.payload.digest();
        let intent_digest = intent.intent_digest;
        let event = Self {
            tenant_id: intent.tenant_id,
            run_id: intent.run_id,
            sequence,
            event_id: intent.event_id,
            recorded_at,
            source: intent.source,
            payload: intent.payload,
            payload_digest,
            intent_digest,
            previous_digest,
            digest,
        };
        event.validate()?;
        Ok(event)
    }

    /// Materializes a validated append after the store has won its transaction.
    ///
    /// `recorded_at` must be the database observation selected while holding the
    /// current run row. For a non-empty journal it cannot precede the prior head;
    /// a store may clamp a regressing wall clock to the prior durable value while
    /// sequence remains the authoritative order.
    ///
    /// # Errors
    ///
    /// Returns [`JournalEventError`] for sequence exhaustion, clock regression,
    /// or an unexpected integrity serialization failure.
    pub fn commit(
        append: JournalAppend,
        recorded_at: Timestamp,
    ) -> Result<Self, JournalEventError> {
        let (sequence, previous_digest) = match append.expectation {
            JournalExpectation::Empty => (JournalSequence::FIRST, None),
            JournalExpectation::Exact { head } => {
                if recorded_at < head.recorded_at {
                    return Err(JournalEventError::ClockRegression {
                        previous: head.recorded_at,
                        actual: recorded_at,
                    });
                }
                let sequence = head
                    .sequence
                    .checked_next()
                    .ok_or(JournalEventError::SequenceOverflow)?;
                (sequence, Some(head.digest))
            }
        };

        let intent = append.intent;
        let payload_digest = intent.payload.digest();
        let digest = compute_event_digest(&EventDigestWire {
            tenant_id: &intent.tenant_id,
            run_id: intent.run_id,
            sequence,
            event_id: intent.event_id,
            recorded_at,
            payload_digest,
            intent_digest: intent.intent_digest,
            previous_digest,
        })
        .map_err(JournalEventError::integrity)?;
        Ok(Self {
            tenant_id: intent.tenant_id,
            run_id: intent.run_id,
            sequence,
            event_id: intent.event_id,
            recorded_at,
            source: intent.source,
            payload: intent.payload,
            payload_digest,
            intent_digest: intent.intent_digest,
            previous_digest,
            digest,
        })
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the run journal identity.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the one-based contiguous event sequence.
    #[must_use]
    pub const fn sequence(&self) -> JournalSequence {
        self.sequence
    }

    /// Returns the stable idempotency identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Returns the durable database clock observation.
    #[must_use]
    pub const fn recorded_at(&self) -> Timestamp {
        self.recorded_at
    }

    /// Returns the trusted source marker.
    #[must_use]
    pub const fn source(&self) -> &JournalEventSource {
        &self.source
    }

    /// Returns the schema-bound payload.
    #[must_use]
    pub const fn payload(&self) -> &JournalPayload {
        &self.payload
    }

    /// Returns the RFC 8785 payload checksum.
    #[must_use]
    pub const fn payload_digest(&self) -> Digest {
        self.payload_digest
    }

    /// Returns the domain-separated append-intent checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the immediately preceding event checksum, if any.
    #[must_use]
    pub const fn previous_digest(&self) -> Option<Digest> {
        self.previous_digest
    }

    /// Returns this complete event's domain-separated checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns the exact durable head represented by this event.
    #[must_use]
    pub fn head(&self) -> JournalHead {
        JournalHead::new(
            self.tenant_id.clone(),
            self.run_id,
            self.sequence,
            self.event_id,
            self.recorded_at,
            self.digest,
        )
    }

    /// Returns whether an idempotent retry is exactly the committed intent.
    #[must_use]
    pub fn matches_intent(&self, intent: &JournalEventIntent) -> bool {
        self.tenant_id == intent.tenant_id
            && self.run_id == intent.run_id
            && self.event_id == intent.event_id
            && self.source == intent.source
            && self.payload == intent.payload
            && self.intent_digest == intent.intent_digest
    }

    fn validate(&self) -> Result<(), JournalEventError> {
        if self.sequence == JournalSequence::FIRST {
            if self.previous_digest.is_some() {
                return Err(JournalEventError::UnexpectedPreviousDigest);
            }
        } else if self.previous_digest.is_none() {
            return Err(JournalEventError::MissingPreviousDigest);
        }

        if self.payload.digest() != self.payload_digest {
            return Err(JournalEventError::PayloadDigestMismatch);
        }
        validate_source_scope(&self.tenant_id, self.run_id, &self.source)
            .map_err(JournalEventError::intent)?;
        let expected_intent = compute_intent_digest(
            &self.tenant_id,
            self.run_id,
            self.event_id,
            &self.source,
            self.payload_digest,
        )
        .map_err(JournalEventError::integrity)?;
        if expected_intent != self.intent_digest {
            return Err(JournalEventError::IntentDigestMismatch);
        }
        let expected_event = compute_event_digest(&EventDigestWire {
            tenant_id: &self.tenant_id,
            run_id: self.run_id,
            sequence: self.sequence,
            event_id: self.event_id,
            recorded_at: self.recorded_at,
            payload_digest: self.payload_digest,
            intent_digest: self.intent_digest,
            previous_digest: self.previous_digest,
        })
        .map_err(JournalEventError::integrity)?;
        if expected_event != self.digest {
            return Err(JournalEventError::EventDigestMismatch);
        }
        Ok(())
    }
}

impl fmt::Debug for JournalEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JournalEvent")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("sequence", &self.sequence)
            .field("event_id", &self.event_id)
            .field("recorded_at", &self.recorded_at)
            .field("source", &self.source)
            .field("payload", &self.payload)
            .field("payload_digest", &self.payload_digest)
            .field("intent_digest", &self.intent_digest)
            .field("previous_digest", &self.previous_digest)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for JournalEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            sequence: JournalSequence,
            event_id: EventId,
            recorded_at: Timestamp,
            source: JournalEventSource,
            payload: JournalPayload,
            payload_digest: Digest,
            intent_digest: Digest,
            previous_digest: Option<Digest>,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        let event = Self {
            tenant_id: wire.tenant_id,
            run_id: wire.run_id,
            sequence: wire.sequence,
            event_id: wire.event_id,
            recorded_at: wire.recorded_at,
            source: wire.source,
            payload: wire.payload,
            payload_digest: wire.payload_digest,
            intent_digest: wire.intent_digest,
            previous_digest: wire.previous_digest,
            digest: wire.digest,
        };
        event.validate().map_err(de::Error::custom)?;
        Ok(event)
    }
}

#[derive(Serialize)]
struct EventDigestWire<'a> {
    tenant_id: &'a TenantId,
    run_id: RunId,
    sequence: JournalSequence,
    event_id: EventId,
    recorded_at: Timestamp,
    payload_digest: Digest,
    intent_digest: Digest,
    previous_digest: Option<Digest>,
}

fn compute_event_digest(wire: &EventDigestWire<'_>) -> Result<Digest, JournalIntegrityError> {
    domain_separated_digest(EVENT_DIGEST_DOMAIN, wire)
}

/// Intrinsically invalid journal event or append materialization.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JournalEventError {
    /// No contiguous `PostgreSQL` sequence remains.
    #[error("journal sequence overflowed")]
    SequenceOverflow,

    /// The database clock observation preceded the committed head.
    #[error("journal event time {actual} precedes head time {previous}")]
    ClockRegression {
        /// Previous durable observation.
        previous: Timestamp,
        /// Rejected event observation.
        actual: Timestamp,
    },

    /// The first sequence improperly named a predecessor.
    #[error("first journal event must not contain a previous digest")]
    UnexpectedPreviousDigest,

    /// A non-first sequence omitted its predecessor checksum.
    #[error("non-first journal event must contain a previous digest")]
    MissingPreviousDigest,

    /// The serialized payload checksum did not match canonical bytes.
    #[error("journal payload digest does not match the canonical payload")]
    PayloadDigestMismatch,

    /// The serialized intent checksum did not match event identity and content.
    #[error("journal intent digest does not match the event fields")]
    IntentDigestMismatch,

    /// The serialized event checksum did not match the record.
    #[error("journal event digest does not match the event fields")]
    EventDigestMismatch,

    /// Worker source scope was invalid.
    #[error("journal event source is invalid: {source}")]
    Intent {
        /// Underlying source-scope failure.
        #[source]
        source: JournalIntentError,
    },

    /// A small typed checksum preimage could not be canonicalized.
    #[error("journal event integrity calculation failed: {source}")]
    Integrity {
        /// Underlying integrity failure.
        #[source]
        source: JournalIntegrityError,
    },
}

impl JournalEventError {
    const fn intent(source: JournalIntentError) -> Self {
        Self::Intent { source }
    }

    const fn integrity(source: JournalIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Streaming verifier for a complete journal or a suffix after a trusted head.
///
/// Each accepted event must be individually self-validating, in the same scope,
/// exactly one sequence after the current head, chained to its digest, and no
/// earlier in durable time. Rejection leaves the verifier unchanged.
#[derive(Debug, Default)]
pub struct JournalChainVerifier {
    head: Option<JournalHead>,
}

impl JournalChainVerifier {
    /// Constructs a verifier expecting the first event.
    #[must_use]
    pub const fn new() -> Self {
        Self { head: None }
    }

    /// Constructs a suffix verifier after a trusted checkpoint or archive head.
    #[must_use]
    pub const fn after(head: JournalHead) -> Self {
        Self { head: Some(head) }
    }

    /// Returns the most recently accepted head.
    #[must_use]
    pub const fn head(&self) -> Option<&JournalHead> {
        self.head.as_ref()
    }

    /// Verifies and accepts the next event without buffering payload history.
    ///
    /// # Errors
    ///
    /// Returns [`JournalChainError`] for intrinsic corruption, wrong scope,
    /// sequence gaps, predecessor mismatch, or durable-clock regression.
    pub fn verify_next(&mut self, event: &JournalEvent) -> Result<(), JournalChainError> {
        event.validate().map_err(JournalChainError::event)?;
        match &self.head {
            None => {
                if event.sequence != JournalSequence::FIRST {
                    return Err(JournalChainError::FirstSequence {
                        actual: event.sequence,
                    });
                }
            }
            Some(head) => {
                if event.tenant_id != head.tenant_id {
                    return Err(JournalChainError::TenantMismatch);
                }
                if event.run_id != head.run_id {
                    return Err(JournalChainError::RunMismatch);
                }
                let expected = head
                    .sequence
                    .checked_next()
                    .ok_or(JournalChainError::SequenceOverflow)?;
                if event.sequence != expected {
                    return Err(JournalChainError::SequenceGap {
                        expected,
                        actual: event.sequence,
                    });
                }
                if event.previous_digest != Some(head.digest) {
                    return Err(JournalChainError::PreviousDigestMismatch);
                }
                if event.recorded_at < head.recorded_at {
                    return Err(JournalChainError::ClockRegression {
                        previous: head.recorded_at,
                        actual: event.recorded_at,
                    });
                }
            }
        }
        self.head = Some(event.head());
        Ok(())
    }
}

/// Rejected event in streaming journal-chain verification.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JournalChainError {
    /// An event failed its own checksum or structural validation.
    #[error("journal event failed intrinsic validation: {source}")]
    Event {
        /// Underlying event failure.
        #[source]
        source: JournalEventError,
    },

    /// A complete journal did not start at sequence one.
    #[error("journal begins at sequence {actual}; expected one")]
    FirstSequence {
        /// Rejected first sequence.
        actual: JournalSequence,
    },

    /// The event crossed a tenant boundary.
    #[error("journal event crosses the verifier tenant boundary")]
    TenantMismatch,

    /// The event named a different run.
    #[error("journal event does not belong to the verifier run")]
    RunMismatch,

    /// The current verified head has no representable successor.
    #[error("journal verifier sequence overflowed")]
    SequenceOverflow,

    /// The event was not the exact contiguous successor.
    #[error("journal sequence is {actual}; expected {expected}")]
    SequenceGap {
        /// Exact required successor.
        expected: JournalSequence,
        /// Rejected sequence.
        actual: JournalSequence,
    },

    /// The event did not chain to the accepted head checksum.
    #[error("journal previous digest does not match the verifier head")]
    PreviousDigestMismatch,

    /// Durable time regressed across the chain.
    #[error("journal event time {actual} precedes verified head time {previous}")]
    ClockRegression {
        /// Previous durable observation.
        previous: Timestamp,
        /// Rejected event observation.
        actual: Timestamp,
    },
}

impl JournalChainError {
    const fn event(source: JournalEventError) -> Self {
        Self::Event { source }
    }
}

/// Failure to produce a small domain-separated journal checksum.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JournalIntegrityError {
    /// Canonical serialization of a closed typed checksum preimage failed.
    #[error("journal checksum preimage canonical serialization failed")]
    CanonicalSerialization,
}

fn domain_separated_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, JournalIntegrityError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| JournalIntegrityError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AttemptId, FencingEpoch, SchemaId, Version};
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    fn at(offset_micros: i64) -> Timestamp {
        let base = "2030-01-01T00:00:00.000000Z".parse::<Timestamp>().unwrap();
        Timestamp::from_unix_micros(base.unix_micros() + offset_micros).unwrap()
    }

    fn tenant() -> TenantId {
        TenantId::try_from("tenant-a").unwrap()
    }

    fn other_tenant() -> TenantId {
        TenantId::try_from("tenant-b").unwrap()
    }

    fn run() -> RunId {
        "01912345-6789-7abc-8def-0123456789ab".parse().unwrap()
    }

    fn other_run() -> RunId {
        "01912345-6789-7abc-8def-0123456789bb".parse().unwrap()
    }

    fn event(suffix: &str) -> EventId {
        format!("01912345-6789-7abc-8def-0123456789{suffix}")
            .parse()
            .unwrap()
    }

    fn attempt(suffix: &str) -> AttemptId {
        format!("01912345-6789-7abc-8def-0123456789{suffix}")
            .parse()
            .unwrap()
    }

    fn schema() -> SchemaReference {
        SchemaReference::new(
            "https://stateknot.github.io/schema/run-event/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"run event schema v1"),
        )
    }

    fn payload(value: Value) -> JournalPayload {
        JournalPayload::new(
            schema(),
            JournalEventKind::new("run-transition-applied").unwrap(),
            BoundedJson::try_from_value(value).unwrap(),
        )
        .unwrap()
    }

    fn control_intent(event_id: EventId, value: Value) -> JournalEventIntent {
        JournalEventIntent::control_plane(tenant(), run(), event_id, payload(value)).unwrap()
    }

    fn first_event() -> JournalEvent {
        JournalEvent::commit(
            JournalAppend::new(
                JournalExpectation::Empty,
                control_intent(event("a1"), json!({"revision": "0"})),
            )
            .unwrap(),
            at(10),
        )
        .unwrap()
    }

    fn worker_fence() -> RunFence {
        RunFence::new(tenant(), run(), attempt("ac"), FencingEpoch::FIRST)
    }

    #[test]
    fn sequences_are_positive_canonical_database_ordinals() {
        assert_eq!(JournalSequence::new(1), Ok(JournalSequence::FIRST));
        assert_eq!(JournalSequence::new(0), Err(JournalSequenceError::Zero));
        assert_eq!(
            JournalSequence::new(MAX_DATABASE_ORDINAL + 1),
            Err(JournalSequenceError::AboveMaximum)
        );
        assert_eq!(JournalSequence::MAX.checked_next(), None);
        assert_eq!(JournalSequence::FIRST.checked_next().unwrap().get(), 2);
        assert_eq!(to_value(JournalSequence::FIRST).unwrap(), json!("1"));
        for invalid in [json!("0"), json!("01"), json!("x"), json!(1), Value::Null] {
            assert!(from_value::<JournalSequence>(invalid).is_err());
        }
    }

    #[test]
    fn event_kinds_enforce_lower_kebab_case_and_bounds() {
        for valid in ["run-admitted", "tool2-result", "a"] {
            assert_eq!(JournalEventKind::new(valid).unwrap().as_str(), valid);
        }
        assert_eq!(JournalEventKind::new(""), Err(JournalEventKindError::Empty));
        assert_eq!(
            JournalEventKind::new("9event"),
            Err(JournalEventKindError::InvalidStart)
        );
        assert!(matches!(
            JournalEventKind::new("Run-Admitted"),
            Err(JournalEventKindError::InvalidStart)
        ));
        assert!(matches!(
            JournalEventKind::new("run--admitted"),
            Err(JournalEventKindError::NonCanonicalSeparator { .. })
        ));
        assert!(matches!(
            JournalEventKind::new("run-admitted-"),
            Err(JournalEventKindError::NonCanonicalSeparator { .. })
        ));
        assert!(matches!(
            JournalEventKind::new("run_admitted"),
            Err(JournalEventKindError::InvalidByte { .. })
        ));
    }

    #[test]
    fn payload_digest_is_rfc_8785_stable_and_redacted() {
        let first = payload(json!({"z": -0.0, "a": [1, true]}));
        let second = JournalPayload::new(
            schema(),
            JournalEventKind::new("run-transition-applied").unwrap(),
            BoundedJson::from_str(r#"{"a":[1,true],"z":0}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(first.digest(), second.digest());
        assert_eq!(
            first.canonical_json().unwrap(),
            second.canonical_json().unwrap()
        );
        assert_eq!(first.canonical_json().unwrap().digest(), first.digest());
        let debug = format!("{:?}", payload(json!({"secret": "never-print-me"})));
        assert!(!debug.contains("never-print-me"));
    }

    #[test]
    fn payload_rejects_non_interoperable_integers() {
        let result = JournalPayload::new(
            schema(),
            JournalEventKind::new("run-transition-applied").unwrap(),
            BoundedJson::from_str("9007199254740992").unwrap(),
        );
        assert!(matches!(
            result,
            Err(JournalPayloadError::Canonical {
                source: CanonicalJsonError::IntegerOutsideIJsonSafeRange
            })
        ));
    }

    #[test]
    fn first_event_has_stable_idempotency_and_chain_identity() {
        let intent = control_intent(event("a1"), json!({"revision": "0"}));
        let append = JournalAppend::new(JournalExpectation::Empty, intent.clone()).unwrap();
        let committed = JournalEvent::commit(append, at(10)).unwrap();
        assert_eq!(committed.sequence(), JournalSequence::FIRST);
        assert_eq!(committed.previous_digest(), None);
        assert_eq!(committed.payload_digest(), committed.payload().digest());
        assert!(committed.matches_intent(&intent));
        assert_eq!(committed.head().digest(), committed.digest());

        let round_trip = from_value::<JournalEvent>(to_value(&committed).unwrap()).unwrap();
        assert_eq!(round_trip, committed);
    }

    #[test]
    fn storage_restore_recomputes_the_complete_event_integrity_layer() {
        let committed = first_event();
        let intent = control_intent(committed.event_id(), json!({"revision": "0"}));
        let restored = JournalEvent::restore(
            intent.clone(),
            committed.sequence(),
            committed.recorded_at(),
            committed.previous_digest(),
            committed.digest(),
        )
        .unwrap();
        assert_eq!(restored, committed);
        assert_eq!(
            JournalEvent::restore(
                intent,
                committed.sequence(),
                committed.recorded_at(),
                committed.previous_digest(),
                Digest::sha256(b"corrupt durable event"),
            ),
            Err(JournalEventError::EventDigestMismatch)
        );
    }

    #[test]
    fn same_event_id_with_different_content_is_not_an_idempotent_match() {
        let committed = first_event();
        let same = control_intent(event("a1"), json!({"revision": "0"}));
        let conflict = control_intent(event("a1"), json!({"revision": "1"}));
        assert!(committed.matches_intent(&same));
        assert!(!committed.matches_intent(&conflict));
        assert_ne!(same.intent_digest(), conflict.intent_digest());
    }

    #[test]
    fn worker_intent_binds_scope_and_current_lease() {
        let fence = worker_fence();
        let intent = JournalEventIntent::worker(
            tenant(),
            run(),
            event("a2"),
            fence.clone(),
            payload(json!({"revision": "1"})),
        )
        .unwrap();
        let append =
            JournalAppend::new(JournalExpectation::exact(first_event().head()), intent).unwrap();
        let lease = RunLease::new(fence, at(10), at(20)).unwrap();
        assert_eq!(append.validate_worker_lease(&lease, at(19)), Ok(()));
        assert!(matches!(
            append.validate_worker_lease(&lease, at(20)),
            Err(JournalAuthorityError::Lease {
                source: RunLeaseValidationError::Expired { .. }
            })
        ));

        let wrong_tenant = JournalEventIntent::worker(
            other_tenant(),
            run(),
            event("a2"),
            worker_fence(),
            payload(json!({})),
        );
        assert_eq!(wrong_tenant, Err(JournalIntentError::FenceTenantMismatch));
    }

    #[test]
    fn exact_head_must_share_the_intent_scope() {
        let first = first_event();
        let intent = JournalEventIntent::control_plane(
            other_tenant(),
            run(),
            event("a2"),
            payload(json!({})),
        )
        .unwrap();
        assert_eq!(
            JournalAppend::new(JournalExpectation::exact(first.head()), intent),
            Err(JournalAppendError::HeadTenantMismatch)
        );

        let intent = JournalEventIntent::control_plane(
            tenant(),
            other_run(),
            event("a2"),
            payload(json!({})),
        )
        .unwrap();
        assert_eq!(
            JournalAppend::new(JournalExpectation::exact(first.head()), intent),
            Err(JournalAppendError::HeadRunMismatch)
        );
    }

    #[test]
    fn contiguous_chain_verifies_without_buffering_payloads() {
        let first = first_event();
        let second = JournalEvent::commit(
            JournalAppend::new(
                JournalExpectation::exact(first.head()),
                control_intent(event("a2"), json!({"revision": "1"})),
            )
            .unwrap(),
            at(11),
        )
        .unwrap();
        assert_eq!(second.sequence().get(), 2);
        assert_eq!(second.previous_digest(), Some(first.digest()));

        let mut verifier = JournalChainVerifier::new();
        verifier.verify_next(&first).unwrap();
        verifier.verify_next(&second).unwrap();
        assert_eq!(verifier.head(), Some(&second.head()));
    }

    #[test]
    fn chain_verifier_rejects_gaps_wrong_scope_and_wrong_predecessor() {
        let first = first_event();
        let second = JournalEvent::commit(
            JournalAppend::new(
                JournalExpectation::exact(first.head()),
                control_intent(event("a2"), json!({})),
            )
            .unwrap(),
            at(11),
        )
        .unwrap();
        let third = JournalEvent::commit(
            JournalAppend::new(
                JournalExpectation::exact(second.head()),
                control_intent(event("a3"), json!({})),
            )
            .unwrap(),
            at(12),
        )
        .unwrap();

        let mut gap = JournalChainVerifier::after(first.head());
        assert!(matches!(
            gap.verify_next(&third),
            Err(JournalChainError::SequenceGap { .. })
        ));
        assert_eq!(gap.head(), Some(&first.head()));

        let foreign = JournalEvent::commit(
            JournalAppend::new(
                JournalExpectation::Empty,
                JournalEventIntent::control_plane(
                    other_tenant(),
                    run(),
                    event("b1"),
                    payload(json!({})),
                )
                .unwrap(),
            )
            .unwrap(),
            at(11),
        )
        .unwrap();
        let mut scoped = JournalChainVerifier::after(first.head());
        assert_eq!(
            scoped.verify_next(&foreign),
            Err(JournalChainError::TenantMismatch)
        );

        let alternate_head = JournalHead::new(
            tenant(),
            run(),
            JournalSequence::FIRST,
            first.event_id(),
            first.recorded_at(),
            Digest::sha256(b"alternate-head"),
        );
        let wrong_predecessor = JournalEvent::commit(
            JournalAppend::new(
                JournalExpectation::exact(alternate_head),
                control_intent(event("b2"), json!({})),
            )
            .unwrap(),
            at(11),
        )
        .unwrap();
        let mut predecessor = JournalChainVerifier::after(first.head());
        assert_eq!(
            predecessor.verify_next(&wrong_predecessor),
            Err(JournalChainError::PreviousDigestMismatch)
        );

        let earlier_head = JournalHead::new(
            tenant(),
            run(),
            JournalSequence::FIRST,
            first.event_id(),
            at(1),
            first.digest(),
        );
        let earlier = JournalEvent::commit(
            JournalAppend::new(
                JournalExpectation::exact(earlier_head),
                control_intent(event("b3"), json!({})),
            )
            .unwrap(),
            at(2),
        )
        .unwrap();
        let mut clock = JournalChainVerifier::after(first.head());
        assert!(matches!(
            clock.verify_next(&earlier),
            Err(JournalChainError::ClockRegression { .. })
        ));
    }

    #[test]
    fn event_deserialization_detects_every_integrity_layer() {
        let encoded = to_value(first_event()).unwrap();

        let mut payload_digest = encoded.clone();
        payload_digest["payload_digest"] = json!(Digest::sha256(b"wrong").to_string());
        assert!(from_value::<JournalEvent>(payload_digest).is_err());

        let mut intent_digest = encoded.clone();
        intent_digest["intent_digest"] = json!(Digest::sha256(b"wrong").to_string());
        assert!(from_value::<JournalEvent>(intent_digest).is_err());

        let mut event_digest = encoded.clone();
        event_digest["digest"] = json!(Digest::sha256(b"wrong").to_string());
        assert!(from_value::<JournalEvent>(event_digest).is_err());

        let mut predecessor = encoded.clone();
        predecessor["previous_digest"] = json!(Digest::sha256(b"unexpected").to_string());
        assert!(from_value::<JournalEvent>(predecessor).is_err());

        let mut extra = encoded;
        extra["payload"]["extra"] = json!(true);
        assert!(from_value::<JournalEvent>(extra).is_err());
    }

    #[test]
    fn commit_rejects_clock_regression_and_sequence_exhaustion() {
        let first = first_event();
        let append = JournalAppend::new(
            JournalExpectation::exact(first.head()),
            control_intent(event("a2"), json!({})),
        )
        .unwrap();
        assert!(matches!(
            JournalEvent::commit(append, at(9)),
            Err(JournalEventError::ClockRegression { .. })
        ));

        let exhausted = JournalHead::new(
            tenant(),
            run(),
            JournalSequence::MAX,
            event("af"),
            at(10),
            Digest::sha256(b"head"),
        );
        let append = JournalAppend::new(
            JournalExpectation::exact(exhausted),
            control_intent(event("b0"), json!({})),
        )
        .unwrap();
        assert_eq!(
            JournalEvent::commit(append, at(11)),
            Err(JournalEventError::SequenceOverflow)
        );
    }

    #[test]
    fn schemas_close_all_integrity_bearing_objects() {
        for schema in [
            to_value(schemars::schema_for!(JournalPayload)).unwrap(),
            to_value(schemars::schema_for!(JournalEventIntent)).unwrap(),
            to_value(schemars::schema_for!(JournalAppend)).unwrap(),
            to_value(schemars::schema_for!(JournalEvent)).unwrap(),
        ] {
            assert_eq!(schema["type"], "object");
            assert_eq!(schema["additionalProperties"], false);
        }
    }

    proptest! {
        #[test]
        fn generated_contiguous_chains_replay_to_their_last_head(length in 1_usize..128) {
            let mut verifier = JournalChainVerifier::new();
            let mut head = None;
            for index in 0..length {
                let event_id = EventId::generate();
                let intent = control_intent(event_id, json!({"index": index.to_string()}));
                let expectation = head
                    .clone()
                    .map_or(JournalExpectation::Empty, JournalExpectation::exact);
                let event = JournalEvent::commit(
                    JournalAppend::new(expectation, intent).unwrap(),
                    at(i64::try_from(index).unwrap()),
                ).unwrap();
                verifier.verify_next(&event).unwrap();
                head = Some(event.head());
            }
            prop_assert_eq!(verifier.head(), head.as_ref());
        }
    }
}
