// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Validated scheduler/tenant identifiers and canonical generated `UUIDv7` IDs.

use std::{borrow::Borrow, fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;
use uuid::{Uuid, Variant, Version};

use crate::Digest;

const UUID_V7_PATTERN: &str =
    "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

/// A validated tenant boundary identifier.
///
/// Tenant IDs contain 1 to 128 ASCII letters, digits, `.`, `_`, `:`, or `-`.
/// The path-like special values `.` and `..` are rejected. This validation is
/// applied equally to constructors, string parsing, and deserialization.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TenantId(Box<str>);

impl TenantId {
    /// Maximum encoded length of a tenant identifier in bytes.
    pub const MAX_LEN: usize = 128;

    /// Validates and constructs a tenant identifier without copying a `String`.
    ///
    /// # Errors
    ///
    /// Returns [`TenantIdError`] when `value` is empty, too long, path-like, or
    /// contains a byte outside the allowed ASCII grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, TenantIdError> {
        let value = value.into();
        validate_tenant_id(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the canonical tenant identifier text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for TenantId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for TenantId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for TenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("TenantId")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TenantId {
    type Err = TenantIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for TenantId {
    type Error = TenantIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for TenantId {
    type Error = TenantIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<TenantId> for String {
    fn from(value: TenantId) -> Self {
        value.0.into()
    }
}

impl Serialize for TenantId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TenantId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TenantIdVisitor)
    }
}

impl JsonSchema for TenantId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "TenantId".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::TenantId").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 128,
            "pattern": "^(?!\\.{1,2}$)[A-Za-z0-9._:-]+$"
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for a [`TenantId`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TenantIdError {
    /// The identifier contained no bytes.
    #[error("tenant identifier must not be empty")]
    Empty,

    /// The identifier exceeded [`TenantId::MAX_LEN`].
    #[error("tenant identifier is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The identifier was `.` or `..`.
    #[error("tenant identifier must not be path-like")]
    PathLike,

    /// A byte did not belong to the allowed ASCII grammar.
    #[error("tenant identifier contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first invalid byte.
        index: usize,
    },
}

struct TenantIdVisitor;

impl de::Visitor<'_> for TenantIdVisitor {
    type Value = TenantId;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical StateKnot tenant identifier")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        TenantId::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        TenantId::try_from(value).map_err(E::custom)
    }
}

fn validate_tenant_id(value: &str) -> Result<(), TenantIdError> {
    if value.is_empty() {
        return Err(TenantIdError::Empty);
    }
    if value.len() > TenantId::MAX_LEN {
        return Err(TenantIdError::TooLong {
            max: TenantId::MAX_LEN,
            actual: value.len(),
        });
    }
    if matches!(value, "." | "..") {
        return Err(TenantIdError::PathLike);
    }

    if let Some((index, _)) = value.bytes().enumerate().find(|(_, byte)| {
        !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b':' | b'-')
    }) {
        return Err(TenantIdError::InvalidByte { index });
    }

    Ok(())
}

/// Opaque caller key for durable Agent-submission idempotency.
///
/// Keys contain 16 to 128 header-safe ASCII bytes. Clients should generate at
/// least 128 bits of unpredictability and reuse the same key only for the same
/// logical submission. The raw value is never included in `Debug`; durability
/// providers store only its tenant-scoped digest.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AgentSubmissionKey(Box<str>);

impl AgentSubmissionKey {
    /// Smallest accepted encoded key length.
    pub const MIN_LEN: usize = 16;
    /// Largest accepted encoded key length.
    pub const MAX_LEN: usize = 128;

    /// Validates one externally supplied idempotency key.
    ///
    /// # Errors
    ///
    /// Rejects short, oversized, non-ASCII, whitespace, disallowed punctuation,
    /// or control input instead of normalizing two caller keys into one durable
    /// identity.
    pub fn new(value: impl Into<String>) -> Result<Self, AgentSubmissionKeyError> {
        let value = value.into();
        validate_agent_submission_key(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Generates two canonical `UUIDv7` values carrying 148 random bits in total.
    ///
    /// A single `UUIDv7` carries 74 random bits; joining two independently
    /// generated values keeps timestamp sortability while meeting the public
    /// recommendation of at least 128 bits of unpredictability.
    #[must_use]
    pub fn generate() -> Self {
        Self(format!("{}.{}", Uuid::now_v7(), Uuid::now_v7()).into_boxed_str())
    }

    /// Returns the exact caller key for an outbound idempotency header.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Derives the one-way tenant-scoped storage key.
    ///
    /// Scoping prevents equal raw keys in different tenants from sharing a
    /// database lookup value. The domain and delimiter make the encoding
    /// unambiguous and versioned.
    #[must_use]
    pub fn digest_for(&self, tenant_id: &TenantId) -> Digest {
        const DOMAIN: &[u8] = b"stateknot.agent-submission-key.v1\0";

        let mut preimage =
            Vec::with_capacity(DOMAIN.len() + tenant_id.as_str().len() + 1 + self.as_str().len());
        preimage.extend_from_slice(DOMAIN);
        preimage.extend_from_slice(tenant_id.as_str().as_bytes());
        preimage.push(0);
        preimage.extend_from_slice(self.as_str().as_bytes());
        Digest::sha256(preimage)
    }
}

impl fmt::Debug for AgentSubmissionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentSubmissionKey")
            .field("byte_length", &self.as_str().len())
            .finish_non_exhaustive()
    }
}

impl FromStr for AgentSubmissionKey {
    type Err = AgentSubmissionKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for AgentSubmissionKey {
    type Error = AgentSubmissionKeyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for AgentSubmissionKey {
    type Error = AgentSubmissionKeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for AgentSubmissionKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AgentSubmissionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(AgentSubmissionKeyVisitor)
    }
}

impl JsonSchema for AgentSubmissionKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "AgentSubmissionKey".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::AgentSubmissionKey").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 16,
            "maxLength": 128,
            "pattern": "^[A-Za-z0-9._~-]{16,128}$"
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid external Agent-submission idempotency key.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentSubmissionKeyError {
    /// The key did not meet the minimum length required for safe caller IDs.
    #[error("Agent submission key is {actual} bytes; minimum is {minimum}")]
    TooShort {
        /// Required minimum byte length.
        minimum: usize,
        /// Observed byte length.
        actual: usize,
    },
    /// The key exceeded the hard request/header bound.
    #[error("Agent submission key is {actual} bytes; maximum is {maximum}")]
    TooLong {
        /// Accepted maximum byte length.
        maximum: usize,
        /// Observed byte length.
        actual: usize,
    },
    /// A byte did not belong to the exact opaque-key grammar.
    #[error("Agent submission key contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first rejected byte.
        index: usize,
    },
}

struct AgentSubmissionKeyVisitor;

impl de::Visitor<'_> for AgentSubmissionKeyVisitor {
    type Value = AgentSubmissionKey;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded opaque StateKnot Agent submission key")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        AgentSubmissionKey::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        AgentSubmissionKey::try_from(value).map_err(E::custom)
    }
}

fn validate_agent_submission_key(value: &str) -> Result<(), AgentSubmissionKeyError> {
    if value.len() < AgentSubmissionKey::MIN_LEN {
        return Err(AgentSubmissionKeyError::TooShort {
            minimum: AgentSubmissionKey::MIN_LEN,
            actual: value.len(),
        });
    }
    if value.len() > AgentSubmissionKey::MAX_LEN {
        return Err(AgentSubmissionKeyError::TooLong {
            maximum: AgentSubmissionKey::MAX_LEN,
            actual: value.len(),
        });
    }
    if let Some((index, _)) = value.bytes().enumerate().find(|(_, byte)| {
        !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b'~' | b'-')
    }) {
        return Err(AgentSubmissionKeyError::InvalidByte { index });
    }
    Ok(())
}

/// A stable distributed-scheduler shard identifier.
///
/// Shard IDs use the same bounded ASCII grammar as tenant IDs but represent a
/// control-plane ownership boundary, never a tenant authorization boundary.
/// A policy change should normally publish a new shard identifier so rolling
/// deployments cannot silently run different weighted schedules.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchedulerShardId(Box<str>);

impl SchedulerShardId {
    /// Maximum encoded shard identifier length in bytes.
    pub const MAX_LEN: usize = 128;

    /// Validates and constructs a scheduler shard identifier.
    ///
    /// # Errors
    ///
    /// Returns [`SchedulerShardIdError`] for an empty, oversized, path-like,
    /// or non-canonical ASCII value.
    pub fn new(value: impl Into<String>) -> Result<Self, SchedulerShardIdError> {
        let value = value.into();
        validate_scheduler_shard_id(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact durable shard identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SchedulerShardId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for SchedulerShardId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SchedulerShardId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SchedulerShardId")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for SchedulerShardId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SchedulerShardId {
    type Err = SchedulerShardIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for SchedulerShardId {
    type Error = SchedulerShardIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for SchedulerShardId {
    type Error = SchedulerShardIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SchedulerShardId> for String {
    fn from(value: SchedulerShardId) -> Self {
        value.0.into()
    }
}

impl Serialize for SchedulerShardId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SchedulerShardId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SchedulerShardIdVisitor)
    }
}

impl JsonSchema for SchedulerShardId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SchedulerShardId".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::SchedulerShardId").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 128,
            "pattern": "^(?!\\.{1,2}$)[A-Za-z0-9._:-]+$"
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for a [`SchedulerShardId`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SchedulerShardIdError {
    /// The identifier contained no bytes.
    #[error("scheduler shard identifier must not be empty")]
    Empty,
    /// The identifier exceeded [`SchedulerShardId::MAX_LEN`].
    #[error("scheduler shard identifier is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },
    /// The identifier was `.` or `..`.
    #[error("scheduler shard identifier must not be path-like")]
    PathLike,
    /// A byte did not belong to the allowed ASCII grammar.
    #[error("scheduler shard identifier contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first invalid byte.
        index: usize,
    },
}

struct SchedulerShardIdVisitor;

impl de::Visitor<'_> for SchedulerShardIdVisitor {
    type Value = SchedulerShardId;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical StateKnot scheduler shard identifier")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        SchedulerShardId::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        SchedulerShardId::try_from(value).map_err(E::custom)
    }
}

fn validate_scheduler_shard_id(value: &str) -> Result<(), SchedulerShardIdError> {
    if value.is_empty() {
        return Err(SchedulerShardIdError::Empty);
    }
    if value.len() > SchedulerShardId::MAX_LEN {
        return Err(SchedulerShardIdError::TooLong {
            max: SchedulerShardId::MAX_LEN,
            actual: value.len(),
        });
    }
    if matches!(value, "." | "..") {
        return Err(SchedulerShardIdError::PathLike);
    }
    if let Some((index, _)) = value.bytes().enumerate().find(|(_, byte)| {
        !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b':' | b'-')
    }) {
        return Err(SchedulerShardIdError::InvalidByte { index });
    }
    Ok(())
}

/// Parse or construction failure for a StateKnot-generated identifier.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GeneratedIdError {
    /// The input was not a UUID.
    #[error("identifier is not a valid UUID")]
    InvalidUuid,

    /// The input used a valid but non-canonical UUID representation.
    #[error("identifier must use lowercase hyphenated UUID text")]
    NonCanonical,

    /// The UUID was not version 7.
    #[error("identifier must be UUID version 7")]
    WrongVersion,

    /// The UUID did not use the RFC 4122/RFC 9562 variant.
    #[error("identifier must use the RFC 4122 variant")]
    WrongVariant,
}

fn validate_generated_uuid(value: Uuid) -> Result<Uuid, GeneratedIdError> {
    if value.get_variant() != Variant::RFC4122 {
        return Err(GeneratedIdError::WrongVariant);
    }
    if value.get_version() != Some(Version::SortRand) {
        return Err(GeneratedIdError::WrongVersion);
    }
    Ok(value)
}

fn parse_generated_uuid(value: &str) -> Result<Uuid, GeneratedIdError> {
    let parsed = Uuid::parse_str(value).map_err(|_| GeneratedIdError::InvalidUuid)?;
    if parsed.hyphenated().to_string() != value {
        return Err(GeneratedIdError::NonCanonical);
    }
    validate_generated_uuid(parsed)
}

fn serialize_generated_uuid<S>(value: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.collect_str(&value.hyphenated())
}

struct GeneratedUuidVisitor;

impl de::Visitor<'_> for GeneratedUuidVisitor {
    type Value = Uuid;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a lowercase hyphenated UUID version 7 identifier")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        parse_generated_uuid(value).map_err(E::custom)
    }
}

fn deserialize_generated_uuid<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_str(GeneratedUuidVisitor)
}

macro_rules! define_generated_id {
    ($name:ident, $documentation:literal) => {
        #[doc = $documentation]
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a `UUIDv7` identifier using the process UUID generator.
            #[must_use]
            pub fn generate() -> Self {
                Self(Uuid::now_v7())
            }

            /// Constructs the typed identifier from a `UUIDv7` value.
            ///
            /// # Errors
            ///
            /// Returns [`GeneratedIdError`] when the UUID has the wrong version
            /// or variant.
            pub fn from_uuid(value: Uuid) -> Result<Self, GeneratedIdError> {
                validate_generated_uuid(value).map(Self)
            }

            /// Returns the underlying UUID value.
            #[must_use]
            pub const fn as_uuid(&self) -> &Uuid {
                &self.0
            }

            /// Consumes this identifier and returns its UUID value.
            #[must_use]
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl TryFrom<Uuid> for $name {
            type Error = GeneratedIdError;

            fn try_from(value: Uuid) -> Result<Self, Self::Error> {
                Self::from_uuid(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.into_uuid()
            }
        }

        impl FromStr for $name {
            type Err = GeneratedIdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                parse_generated_uuid(value).map(Self)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0.hyphenated())
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0.hyphenated(), formatter)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serialize_generated_uuid(&self.0, serializer)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserialize_generated_uuid(deserializer).map(Self)
            }
        }

        impl JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                stringify!($name).into()
            }

            fn schema_id() -> std::borrow::Cow<'static, str> {
                concat!(module_path!(), "::", stringify!($name)).into()
            }

            fn json_schema(_: &mut SchemaGenerator) -> Schema {
                json_schema!({
                    "type": "string",
                    "format": "uuid",
                    "minLength": 36,
                    "maxLength": 36,
                    "pattern": UUID_V7_PATTERN
                })
            }

            fn inline_schema() -> bool {
                true
            }
        }
    };
}

define_generated_id!(RunId, "A tenant-scoped durable run identifier.");
define_generated_id!(ThreadId, "A tenant-scoped conversation thread identifier.");
define_generated_id!(EventId, "A tenant-scoped durable event identifier.");
define_generated_id!(FailureId, "A tenant-scoped failure occurrence identifier.");
define_generated_id!(MessageId, "A tenant-scoped durable message identifier.");
define_generated_id!(ArtifactId, "A tenant-scoped artifact identifier.");
define_generated_id!(
    InvocationId,
    "A tenant-scoped external invocation identifier."
);
define_generated_id!(InterruptId, "A tenant-scoped interrupt identifier.");
define_generated_id!(TimerId, "A tenant-scoped durable timer identifier.");
define_generated_id!(
    DeliveryId,
    "A tenant-scoped durable outbox delivery identifier."
);
define_generated_id!(
    DestinationId,
    "A tenant-scoped durable outbox destination identifier."
);
define_generated_id!(
    CheckpointId,
    "A tenant-scoped immutable checkpoint identifier."
);
define_generated_id!(
    QuarantineId,
    "A tenant-scoped durable run-quarantine observation identifier."
);
define_generated_id!(
    AttemptId,
    "A tenant-scoped node or dependency attempt identifier."
);
define_generated_id!(
    SchedulerReservationId,
    "A durable distributed-scheduler slot-reservation identifier."
);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, from_str, from_value, to_string};

    #[test]
    fn tenant_id_accepts_the_documented_grammar() {
        for value in ["a", "tenant-42", "org.example:production", "A_B"] {
            let tenant = TenantId::try_from(value).expect("valid tenant ID");
            assert_eq!(tenant.as_str(), value);
            assert_eq!(tenant.to_string(), value);
        }

        let longest = "a".repeat(TenantId::MAX_LEN);
        assert_eq!(TenantId::new(longest.clone()).unwrap().as_str(), longest);
    }

    #[test]
    fn tenant_id_rejects_ambiguous_or_unbounded_values() {
        assert_eq!(TenantId::try_from(""), Err(TenantIdError::Empty));
        assert_eq!(TenantId::try_from("."), Err(TenantIdError::PathLike));
        assert_eq!(TenantId::try_from(".."), Err(TenantIdError::PathLike));
        assert_eq!(
            TenantId::try_from("a".repeat(TenantId::MAX_LEN + 1)),
            Err(TenantIdError::TooLong {
                max: TenantId::MAX_LEN,
                actual: TenantId::MAX_LEN + 1,
            })
        );

        for value in ["tenant name", "tenant/name", "tenant\\name", "租户"] {
            assert!(matches!(
                TenantId::try_from(value),
                Err(TenantIdError::InvalidByte { .. })
            ));
        }
    }

    #[test]
    fn scheduler_shard_id_has_an_independent_bounded_control_plane_type() {
        let shard = SchedulerShardId::try_from("prod:fairness-01").unwrap();
        assert_eq!(shard.as_str(), "prod:fairness-01");
        assert_eq!(shard.to_string(), "prod:fairness-01");
        assert_eq!(
            SchedulerShardId::try_from("."),
            Err(SchedulerShardIdError::PathLike)
        );
        assert!(matches!(
            SchedulerShardId::try_from("bad/shard"),
            Err(SchedulerShardIdError::InvalidByte { index: 3 })
        ));
    }

    #[test]
    fn tenant_id_serde_revalidates_input() {
        let tenant = TenantId::try_from("org.example:prod").unwrap();
        let encoded = to_string(&tenant).unwrap();
        assert_eq!(encoded, "\"org.example:prod\"");
        assert_eq!(from_str::<TenantId>(&encoded).unwrap(), tenant);
        assert!(from_str::<TenantId>("\"../other\"").is_err());
        assert!(from_str::<TenantId>("42").is_err());
    }

    #[test]
    fn tenant_id_schema_matches_runtime_validation_bounds() {
        let schema = serde_json::to_value(schemars::schema_for!(TenantId)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 1);
        assert_eq!(schema["maxLength"], TenantId::MAX_LEN);
        assert_eq!(schema["pattern"], "^(?!\\.{1,2}$)[A-Za-z0-9._:-]+$");
    }

    #[test]
    fn agent_submission_keys_are_bounded_redacted_and_tenant_scoped() {
        let key = AgentSubmissionKey::new("request_01K4Z8Q6QH7W5X3M2N1P").unwrap();
        assert_eq!(
            from_str::<AgentSubmissionKey>(&to_string(&key).unwrap()).unwrap(),
            key
        );
        let debug = format!("{key:?}");
        assert!(!debug.contains(key.as_str()));
        assert!(debug.contains("byte_length"));

        let first = TenantId::new("tenant-a").unwrap();
        let second = TenantId::new("tenant-b").unwrap();
        assert_eq!(key.digest_for(&first), key.digest_for(&first));
        assert_ne!(key.digest_for(&first), key.digest_for(&second));

        let generated = AgentSubmissionKey::generate();
        assert_eq!(generated.as_str().len(), 73);
        let generated_parts = generated.as_str().split('.').collect::<Vec<_>>();
        assert_eq!(generated_parts.len(), 2);
        for part in generated_parts {
            assert_eq!(
                part.parse::<Uuid>().unwrap().get_version(),
                Some(Version::SortRand)
            );
        }
        assert!(AgentSubmissionKey::new("too-short").is_err());
        assert!(AgentSubmissionKey::new("x".repeat(AgentSubmissionKey::MAX_LEN + 1)).is_err());
        assert!(AgentSubmissionKey::new("request key with spaces").is_err());
    }

    #[test]
    fn agent_submission_key_schema_matches_runtime_grammar() {
        let schema = serde_json::to_value(schemars::schema_for!(AgentSubmissionKey)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], AgentSubmissionKey::MIN_LEN);
        assert_eq!(schema["maxLength"], AgentSubmissionKey::MAX_LEN);
        assert_eq!(schema["pattern"], "^[A-Za-z0-9._~-]{16,128}$");
    }

    #[test]
    fn generated_ids_are_canonical_uuid_v7_values() {
        macro_rules! assert_id {
            ($id_type:ty) => {{
                let id = <$id_type>::generate();
                assert_eq!(id.as_uuid().get_version(), Some(Version::SortRand));
                assert_eq!(id.as_uuid().get_variant(), Variant::RFC4122);

                let text = id.to_string();
                assert_eq!(text.len(), 36);
                assert_eq!(text, text.to_ascii_lowercase());
                assert_eq!(text.parse::<$id_type>().unwrap(), id);
            }};
        }

        assert_id!(RunId);
        assert_id!(ThreadId);
        assert_id!(EventId);
        assert_id!(FailureId);
        assert_id!(MessageId);
        assert_id!(ArtifactId);
        assert_id!(InvocationId);
        assert_id!(InterruptId);
        assert_id!(TimerId);
        assert_id!(DeliveryId);
        assert_id!(DestinationId);
        assert_id!(CheckpointId);
        assert_id!(QuarantineId);
        assert_id!(AttemptId);
    }

    #[test]
    fn generated_ids_reject_noncanonical_and_non_v7_values() {
        let id = RunId::generate();
        let canonical = id.to_string();

        assert_eq!(
            canonical.to_ascii_uppercase().parse::<RunId>(),
            Err(GeneratedIdError::NonCanonical)
        );
        assert_eq!(
            canonical.replace('-', "").parse::<RunId>(),
            Err(GeneratedIdError::NonCanonical)
        );
        assert_eq!(
            "not-a-uuid".parse::<RunId>(),
            Err(GeneratedIdError::InvalidUuid)
        );
        assert_eq!(
            "550e8400-e29b-41d4-a716-446655440000".parse::<RunId>(),
            Err(GeneratedIdError::WrongVersion)
        );

        let mut wrong_variant_bytes = *id.as_uuid().as_bytes();
        wrong_variant_bytes[8] &= 0b0011_1111;
        let wrong_variant = Uuid::from_bytes(wrong_variant_bytes);
        assert_eq!(
            RunId::from_uuid(wrong_variant),
            Err(GeneratedIdError::WrongVariant)
        );
    }

    #[test]
    fn generated_id_serde_uses_and_enforces_canonical_text() {
        let id = InvocationId::generate();
        let encoded = to_string(&id).unwrap();
        assert_eq!(encoded, format!("\"{id}\""));
        assert_eq!(from_str::<InvocationId>(&encoded).unwrap(), id);

        let uppercase = Value::String(id.to_string().to_ascii_uppercase());
        assert!(from_value::<InvocationId>(uppercase).is_err());
        assert!(from_value::<InvocationId>(Value::Null).is_err());
    }

    #[test]
    fn generated_id_schema_requires_canonical_uuid_v7_text() {
        let schema = serde_json::to_value(schemars::schema_for!(RunId)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["format"], "uuid");
        assert_eq!(schema["minLength"], 36);
        assert_eq!(schema["maxLength"], 36);
        assert_eq!(schema["pattern"], UUID_V7_PATTERN);
    }
}
