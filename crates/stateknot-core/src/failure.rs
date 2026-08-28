// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Protocol-neutral failures with explicit retry and reconciliation semantics.

use std::{borrow::Borrow, error::Error as StdError, fmt, str::FromStr, sync::Arc};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use serde_json::Value;
use thiserror::Error;

use crate::{
    BoundedJson, BoundedJsonError, DurationMillis, EventId, FailureId, JsonLimits, JsonLimitsError,
    SchemaReference,
};

const FAILURE_IDENTIFIER_PATTERN: &str = "^[a-z][a-z0-9_-]*(\\.[a-z][a-z0-9_-]*)*$";

/// Validation failure shared by [`FailureCode`] and [`FailureOrigin`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum FailureIdentifierError {
    /// The identifier contained no bytes.
    #[error("stable failure identifier must not be empty")]
    Empty,

    /// The identifier exceeded the 128-byte wire ceiling.
    #[error("stable failure identifier is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The first byte, or the first byte after `.`, was not lowercase ASCII.
    #[error("failure identifier segment must start with lowercase ASCII at offset {index}")]
    InvalidSegmentStart {
        /// Zero-based byte offset of the invalid segment start.
        index: usize,
    },

    /// A non-separator byte did not belong to the stable ASCII grammar.
    #[error("stable failure identifier contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the invalid byte.
        index: usize,
    },
}

fn validate_failure_identifier(value: &str) -> Result<(), FailureIdentifierError> {
    const MAX_LEN: usize = 128;

    if value.is_empty() {
        return Err(FailureIdentifierError::Empty);
    }
    if value.len() > MAX_LEN {
        return Err(FailureIdentifierError::TooLong {
            max: MAX_LEN,
            actual: value.len(),
        });
    }

    let mut at_segment_start = true;
    for (index, byte) in value.bytes().enumerate() {
        if at_segment_start {
            if !byte.is_ascii_lowercase() {
                return Err(FailureIdentifierError::InvalidSegmentStart { index });
            }
            at_segment_start = false;
        } else if byte == b'.' {
            at_segment_start = true;
        } else if !byte.is_ascii_lowercase()
            && !byte.is_ascii_digit()
            && !matches!(byte, b'_' | b'-')
        {
            return Err(FailureIdentifierError::InvalidByte { index });
        }
    }

    if at_segment_start {
        return Err(FailureIdentifierError::InvalidSegmentStart { index: value.len() });
    }

    Ok(())
}

macro_rules! define_failure_identifier {
    ($name:ident, $visitor:ident, $documentation:literal) => {
        #[doc = $documentation]
        #[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Box<str>);

        impl $name {
            /// Maximum encoded length in bytes.
            pub const MAX_LEN: usize = 128;

            /// Validates and constructs the stable identifier.
            ///
            /// # Errors
            ///
            /// Returns [`FailureIdentifierError`] when the value is empty,
            /// oversized, or outside the lowercase segmented ASCII grammar.
            pub fn new(value: impl Into<String>) -> Result<Self, FailureIdentifierError> {
                let value = value.into();
                validate_failure_identifier(&value)?;
                Ok(Self(value.into_boxed_str()))
            }

            /// Returns the canonical identifier text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.as_str())
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = FailureIdentifierError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = FailureIdentifierError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = FailureIdentifierError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0.into()
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_string($visitor)
            }
        }

        struct $visitor;

        impl de::Visitor<'_> for $visitor {
            type Value = $name;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a lowercase segmented stable failure identifier")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                $name::try_from(value).map_err(E::custom)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                $name::try_from(value).map_err(E::custom)
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
                    "minLength": 1,
                    "maxLength": 128,
                    "pattern": FAILURE_IDENTIFIER_PATTERN
                })
            }

            fn inline_schema() -> bool {
                true
            }
        }
    };
}

define_failure_identifier!(
    FailureCode,
    FailureCodeVisitor,
    "A stable, machine-readable failure code owned by an application or adapter."
);
define_failure_identifier!(
    FailureOrigin,
    FailureOriginVisitor,
    "A stable component or dependency namespace that originated a failure."
);

/// A bounded message explicitly approved for public error surfaces.
///
/// Construction validates shape, not confidentiality. Callers must supply a
/// message that contains no secrets, prompts, private resource existence,
/// stack traces, provider payloads, or internal implementation details.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FailureMessage(Box<str>);

impl FailureMessage {
    /// Maximum encoded UTF-8 length in bytes.
    pub const MAX_BYTES: usize = 1024;

    /// Validates and constructs a public-safe, single-line message.
    ///
    /// # Errors
    ///
    /// Returns [`FailureMessageError`] when the value is empty, oversized,
    /// whitespace-padded, or contains a control, bidirectional formatting
    /// control, line separator, or Unicode noncharacter.
    pub fn new(value: impl Into<String>) -> Result<Self, FailureMessageError> {
        let value = value.into();
        validate_failure_message(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the public-safe message text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the encoded UTF-8 length without revealing message contents.
    #[must_use]
    pub fn len_bytes(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for FailureMessage {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for FailureMessage {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for FailureMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FailureMessage")
            .field("utf8_bytes", &self.len_bytes())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for FailureMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for FailureMessage {
    type Err = FailureMessageError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for FailureMessage {
    type Error = FailureMessageError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for FailureMessage {
    type Error = FailureMessageError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<FailureMessage> for String {
    fn from(value: FailureMessage) -> Self {
        value.0.into()
    }
}

impl Serialize for FailureMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for FailureMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(FailureMessageVisitor)
    }
}

struct FailureMessageVisitor;

impl de::Visitor<'_> for FailureMessageVisitor {
    type Value = FailureMessage;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded public-safe single-line failure message")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        FailureMessage::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        FailureMessage::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for FailureMessage {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FailureMessage".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::FailureMessage").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 1024,
            "description": "A public-safe single-line message bounded to 1024 UTF-8 bytes. StateKnot additionally rejects leading/trailing whitespace, controls, bidirectional formatting controls, Unicode line separators, and Unicode noncharacters at runtime. Shape validation cannot prove that the caller omitted secrets."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid public failure message text.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum FailureMessageError {
    /// The message contained no bytes.
    #[error("failure message must not be empty")]
    Empty,

    /// The message exceeded [`FailureMessage::MAX_BYTES`].
    #[error("failure message is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted UTF-8 byte length.
        max: usize,
        /// Observed UTF-8 byte length.
        actual: usize,
    },

    /// The message began or ended with Unicode whitespace.
    #[error("failure message must not contain leading or trailing whitespace")]
    SurroundingWhitespace,

    /// A control, bidi formatting control, line separator, or noncharacter was present.
    #[error("failure message contains a disallowed scalar at byte offset {index}")]
    DisallowedScalar {
        /// Zero-based UTF-8 byte offset of the rejected scalar.
        index: usize,
    },
}

fn validate_failure_message(value: &str) -> Result<(), FailureMessageError> {
    if value.is_empty() {
        return Err(FailureMessageError::Empty);
    }
    if value.len() > FailureMessage::MAX_BYTES {
        return Err(FailureMessageError::TooLong {
            max: FailureMessage::MAX_BYTES,
            actual: value.len(),
        });
    }
    if value.chars().next().is_some_and(char::is_whitespace)
        || value.chars().next_back().is_some_and(char::is_whitespace)
    {
        return Err(FailureMessageError::SurroundingWhitespace);
    }
    if let Some((index, _)) = value.char_indices().find(|(_, scalar)| {
        scalar.is_control()
            || is_bidi_formatting_control(*scalar)
            || matches!(scalar, '\u{2028}' | '\u{2029}')
            || is_unicode_noncharacter(*scalar)
    }) {
        return Err(FailureMessageError::DisallowedScalar { index });
    }
    Ok(())
}

const fn is_unicode_noncharacter(value: char) -> bool {
    let code_point = value as u32;
    (code_point >= 0xfdd0 && code_point <= 0xfdef) || code_point & 0xfffe == 0xfffe
}

const fn is_bidi_formatting_control(value: char) -> bool {
    matches!(
        value,
        '\u{061c}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2066}'..='\u{2069}'
    )
}

/// Stable semantic category shared by component-specific errors.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCategory {
    /// A caller-supplied value failed validation.
    InvalidInput,
    /// Authentication was absent or invalid.
    Unauthenticated,
    /// The authenticated principal lacked a required permission.
    PermissionDenied,
    /// A policy explicitly denied the operation.
    PolicyDenied,
    /// A visible resource was not found.
    NotFound,
    /// Current state conflicted with the requested transition.
    Conflict,
    /// The requested operation or capability is unsupported.
    Unsupported,
    /// A rate or quota limit prevented execution.
    RateLimited,
    /// The operation exceeded its deadline.
    DeadlineExceeded,
    /// Cancellation was requested or observed.
    Cancelled,
    /// A required dependency was temporarily unavailable.
    DependencyUnavailable,
    /// Durable or transported data failed integrity validation.
    DataCorruption,
    /// An external side effect may have occurred but cannot yet be proven.
    AmbiguousExternalOutcome,
    /// An internal invariant or implementation failed.
    Internal,
}

/// Explicit recovery advice supplied by the originating component.
///
/// Retryability is never inferred from [`FailureCategory`]. A scheduler must
/// still intersect this advice with idempotency, deadline, budget, attempt,
/// circuit-breaker, and policy constraints.
#[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetryAdvice {
    /// Do not automatically retry this operation.
    Never,
    /// A new attempt is semantically safe no earlier than the stated delay.
    SafeAfter {
        /// Minimum delay before a scheduler may begin a new attempt.
        delay: DurationMillis,
    },
    /// Determine the external outcome before choosing retry or compensation.
    ReconcileFirst,
}

impl RetryAdvice {
    /// Returns the explicit minimum delay for a safe retry, if present.
    #[must_use]
    pub const fn safe_after_delay(self) -> Option<DurationMillis> {
        match self {
            Self::SafeAfter { delay } => Some(delay),
            Self::Never | Self::ReconcileFirst => None,
        }
    }

    /// Returns whether recovery must reconcile an external outcome first.
    #[must_use]
    pub const fn requires_reconciliation(self) -> bool {
        matches!(self, Self::ReconcileFirst)
    }
}

impl<'de> Deserialize<'de> for RetryAdvice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RetryAdviceVisitor)
    }
}

struct RetryAdviceVisitor;

impl<'de> de::Visitor<'de> for RetryAdviceVisitor {
    type Value = RetryAdvice;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a closed retry-advice object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut kind = None;
        let mut delay = None;

        while let Some(field) = map.next_key::<RetryAdviceField>()? {
            match field {
                RetryAdviceField::Kind => {
                    if kind.is_some() {
                        return Err(de::Error::duplicate_field("kind"));
                    }
                    kind = Some(map.next_value::<RetryAdviceKind>()?);
                }
                RetryAdviceField::Delay => {
                    if delay.is_some() {
                        return Err(de::Error::duplicate_field("delay"));
                    }
                    delay = Some(map.next_value::<DurationMillis>()?);
                }
            }
        }

        match (kind.ok_or_else(|| de::Error::missing_field("kind"))?, delay) {
            (RetryAdviceKind::Never, None) => Ok(RetryAdvice::Never),
            (RetryAdviceKind::SafeAfter, Some(delay)) => Ok(RetryAdvice::SafeAfter { delay }),
            (RetryAdviceKind::ReconcileFirst, None) => Ok(RetryAdvice::ReconcileFirst),
            (RetryAdviceKind::SafeAfter, None) => Err(de::Error::missing_field("delay")),
            (RetryAdviceKind::Never | RetryAdviceKind::ReconcileFirst, Some(_)) => {
                Err(de::Error::unknown_field("delay", &["kind"]))
            }
        }
    }
}

enum RetryAdviceField {
    Kind,
    Delay,
}

impl<'de> Deserialize<'de> for RetryAdviceField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_identifier(RetryAdviceFieldVisitor)
    }
}

struct RetryAdviceFieldVisitor;

impl de::Visitor<'_> for RetryAdviceFieldVisitor {
    type Value = RetryAdviceField;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("`kind` or `delay`")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match value {
            "kind" => Ok(RetryAdviceField::Kind),
            "delay" => Ok(RetryAdviceField::Delay),
            _ => Err(E::unknown_field(value, &["kind", "delay"])),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RetryAdviceKind {
    Never,
    SafeAfter,
    ReconcileFirst,
}

/// Schema-bound, resource-limited structured information safe to expose.
///
/// The schema reference gives adapters an explicit mapping contract. The value
/// is revalidated under limits tighter than general [`BoundedJson`] values.
/// Enclosing transports must also cap the raw error body because generic Serde
/// cannot account for insignificant whitespace outside this value.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FailureDetails {
    schema: SchemaReference,
    value: BoundedJson,
}

impl FailureDetails {
    /// Maximum compact JSON size for public details.
    pub const MAX_JSON_BYTES: usize = 16 * 1024;
    /// Maximum object/array nesting depth for public details.
    pub const MAX_JSON_DEPTH: usize = 8;
    /// Maximum entries in any one public-details container.
    pub const MAX_JSON_CONTAINER_ENTRIES: usize = 64;
    /// Maximum total value nodes in public details.
    pub const MAX_JSON_NODES: usize = 512;
    /// Maximum decoded UTF-8 bytes in one public-details string.
    pub const MAX_JSON_STRING_BYTES: usize = 4 * 1024;
    /// Maximum decoded UTF-8 bytes in one public-details object key.
    pub const MAX_JSON_OBJECT_KEY_BYTES: usize = 128;

    /// Constructs schema-bound details after enforcing the tighter limits.
    ///
    /// # Errors
    ///
    /// Returns [`FailureDetailsError`] if the value exceeds any details limit
    /// or the library's static limit configuration is internally invalid.
    pub fn try_new(
        schema: SchemaReference,
        value: BoundedJson,
    ) -> Result<Self, FailureDetailsError> {
        let limits = failure_details_json_limits()?;
        let value = BoundedJson::try_from_value_with_limits(value.into_value(), limits)
            .map_err(FailureDetailsError::json)?;
        Ok(Self { schema, value })
    }

    /// Constructs schema-bound details from a trusted in-process JSON value.
    ///
    /// Wire input must first use an enclosing raw-body limit and ordinary
    /// deserialization so duplicate object member names remain detectable.
    ///
    /// # Errors
    ///
    /// Returns [`FailureDetailsError`] if the value exceeds any details limit
    /// or the library's static limit configuration is internally invalid.
    pub fn try_from_value(
        schema: SchemaReference,
        value: Value,
    ) -> Result<Self, FailureDetailsError> {
        let limits = failure_details_json_limits()?;
        let value = BoundedJson::try_from_value_with_limits(value, limits)
            .map_err(FailureDetailsError::json)?;
        Ok(Self { schema, value })
    }

    /// Returns the immutable schema identity for the details value.
    #[must_use]
    pub const fn schema(&self) -> &SchemaReference {
        &self.schema
    }

    /// Returns the validated structured details without permitting mutation.
    #[must_use]
    pub const fn value(&self) -> &BoundedJson {
        &self.value
    }

    /// Consumes the details and returns its schema and validated value.
    #[must_use]
    pub fn into_parts(self) -> (SchemaReference, BoundedJson) {
        (self.schema, self.value)
    }
}

impl fmt::Debug for FailureDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FailureDetails")
            .field("schema", &self.schema)
            .field("value", &self.value)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for FailureDetails {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema: SchemaReference,
            value: BoundedJson,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::try_new(wire.schema, wire.value).map_err(de::Error::custom)
    }
}

fn failure_details_json_limits() -> Result<JsonLimits, FailureDetailsError> {
    JsonLimits::try_new(
        FailureDetails::MAX_JSON_BYTES,
        FailureDetails::MAX_JSON_DEPTH,
        FailureDetails::MAX_JSON_CONTAINER_ENTRIES,
        FailureDetails::MAX_JSON_NODES,
        FailureDetails::MAX_JSON_STRING_BYTES,
        FailureDetails::MAX_JSON_OBJECT_KEY_BYTES,
    )
    .map_err(FailureDetailsError::limits_configuration)
}

/// Invalid schema-bound public failure details.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum FailureDetailsError {
    /// The static details configuration violated the global JSON hard limits.
    #[error("failure details JSON limits are internally invalid: {source}")]
    LimitsConfiguration {
        /// Underlying invalid limit configuration.
        #[source]
        source: JsonLimitsError,
    },

    /// The details value exceeded a resource-safety or JSON interoperability limit.
    #[error("failure details violate JSON safety limits: {source}")]
    Json {
        /// Underlying bounded JSON violation.
        #[source]
        source: BoundedJsonError,
    },
}

impl FailureDetailsError {
    const fn limits_configuration(source: JsonLimitsError) -> Self {
        Self::LimitsConfiguration { source }
    }

    const fn json(source: BoundedJsonError) -> Self {
        Self::Json { source }
    }
}

/// Invalid relationship between failure category and recovery advice.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum FailureBuildError {
    /// An ambiguous external outcome did not require reconciliation.
    #[error("ambiguous external outcomes must use reconcile-first advice")]
    AmbiguousOutcomeRequiresReconciliation,

    /// Reconciliation advice was attached to a non-ambiguous category.
    #[error("reconcile-first advice is reserved for ambiguous external outcomes")]
    ReconciliationRequiresAmbiguousOutcome,
}

type PrivateSource = dyn StdError + Send + Sync + 'static;

/// One protocol-neutral failure occurrence.
///
/// Serialized and schema-visible fields are safe for an authorized public
/// error surface. The optional in-process source chain is deliberately absent
/// from Serde, JSON Schema, and `Debug`; it may contain private provider,
/// database, transport, or implementation diagnostics.
#[derive(Clone)]
pub struct Failure {
    id: FailureId,
    category: FailureCategory,
    code: FailureCode,
    origin: FailureOrigin,
    message: FailureMessage,
    retry_advice: RetryAdvice,
    details: Option<FailureDetails>,
    caused_by_event_id: Option<EventId>,
    private_source: Option<Arc<PrivateSource>>,
}

impl Failure {
    /// Constructs a failure from validated public components.
    ///
    /// Optional details, durable causation, and a private source chain can be
    /// attached with the consuming `with_*` methods before publication.
    ///
    /// # Errors
    ///
    /// Returns [`FailureBuildError`] if ambiguous external outcome semantics
    /// and reconciliation advice do not correspond exactly.
    pub fn new(
        id: FailureId,
        category: FailureCategory,
        code: FailureCode,
        origin: FailureOrigin,
        message: FailureMessage,
        retry_advice: RetryAdvice,
    ) -> Result<Self, FailureBuildError> {
        validate_failure_recovery(category, retry_advice)?;
        Ok(Self {
            id,
            category,
            code,
            origin,
            message,
            retry_advice,
            details: None,
            caused_by_event_id: None,
            private_source: None,
        })
    }

    /// Attaches already validated public structured details.
    #[must_use]
    pub fn with_details(mut self, details: FailureDetails) -> Self {
        self.details = Some(details);
        self
    }

    /// Attaches the durable event that directly caused this failure.
    #[must_use]
    pub fn with_caused_by_event(mut self, event_id: EventId) -> Self {
        self.caused_by_event_id = Some(event_id);
        self
    }

    /// Attaches private in-process diagnostics that never cross a wire boundary.
    #[must_use]
    pub fn with_private_source<E>(mut self, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        self.private_source = Some(Arc::new(source));
        self
    }

    /// Attaches an already shared private in-process diagnostic chain.
    #[must_use]
    pub fn with_shared_private_source(mut self, source: Arc<PrivateSource>) -> Self {
        self.private_source = Some(source);
        self
    }

    /// Returns the stable identifier of this occurrence.
    #[must_use]
    pub const fn id(&self) -> FailureId {
        self.id
    }

    /// Returns the protocol-neutral semantic category.
    #[must_use]
    pub const fn category(&self) -> FailureCategory {
        self.category
    }

    /// Returns the stable application- or adapter-owned code.
    #[must_use]
    pub const fn code(&self) -> &FailureCode {
        &self.code
    }

    /// Returns the component or dependency namespace that originated the failure.
    #[must_use]
    pub const fn origin(&self) -> &FailureOrigin {
        &self.origin
    }

    /// Returns the message approved for authorized public error surfaces.
    #[must_use]
    pub const fn message(&self) -> &FailureMessage {
        &self.message
    }

    /// Returns explicit recovery advice without inferring it from category.
    #[must_use]
    pub const fn retry_advice(&self) -> RetryAdvice {
        self.retry_advice
    }

    /// Returns optional schema-bound public details.
    #[must_use]
    pub const fn details(&self) -> Option<&FailureDetails> {
        self.details.as_ref()
    }

    /// Returns the durable causal event identifier, when known.
    #[must_use]
    pub const fn caused_by_event_id(&self) -> Option<EventId> {
        self.caused_by_event_id
    }

    /// Returns whether private in-process diagnostics are attached.
    #[must_use]
    pub const fn has_private_source(&self) -> bool {
        self.private_source.is_some()
    }

    /// Returns private diagnostics to trusted in-process callers only.
    #[must_use]
    pub fn private_source(&self) -> Option<&PrivateSource> {
        self.private_source.as_deref()
    }
}

fn validate_failure_recovery(
    category: FailureCategory,
    retry_advice: RetryAdvice,
) -> Result<(), FailureBuildError> {
    match (category, retry_advice) {
        (FailureCategory::AmbiguousExternalOutcome, RetryAdvice::ReconcileFirst) => Ok(()),
        (FailureCategory::AmbiguousExternalOutcome, _) => {
            Err(FailureBuildError::AmbiguousOutcomeRequiresReconciliation)
        }
        (_, RetryAdvice::ReconcileFirst) => {
            Err(FailureBuildError::ReconciliationRequiresAmbiguousOutcome)
        }
        _ => Ok(()),
    }
}

impl fmt::Debug for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Failure")
            .field("id", &self.id)
            .field("category", &self.category)
            .field("code", &self.code)
            .field("origin", &self.origin)
            .field("message_utf8_bytes", &self.message.len_bytes())
            .field("retry_advice", &self.retry_advice)
            .field("has_details", &self.details.is_some())
            .field("caused_by_event_id", &self.caused_by_event_id)
            .field("has_private_source", &self.private_source.is_some())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.message, formatter)
    }
}

impl StdError for Failure {}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FailureWire {
    id: FailureId,
    category: FailureCategory,
    code: FailureCode,
    origin: FailureOrigin,
    message: FailureMessage,
    retry_advice: RetryAdvice,
    #[serde(default)]
    details: Option<FailureDetails>,
    #[serde(default)]
    caused_by_event_id: Option<EventId>,
}

#[derive(Serialize)]
struct FailureWireRef<'a> {
    id: FailureId,
    category: FailureCategory,
    code: &'a FailureCode,
    origin: &'a FailureOrigin,
    message: &'a FailureMessage,
    retry_advice: RetryAdvice,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<&'a FailureDetails>,
    #[serde(skip_serializing_if = "Option::is_none")]
    caused_by_event_id: Option<EventId>,
}

impl Serialize for Failure {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        FailureWireRef {
            id: self.id,
            category: self.category,
            code: &self.code,
            origin: &self.origin,
            message: &self.message,
            retry_advice: self.retry_advice,
            details: self.details.as_ref(),
            caused_by_event_id: self.caused_by_event_id,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Failure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = FailureWire::deserialize(deserializer)?;
        let mut failure = Self::new(
            wire.id,
            wire.category,
            wire.code,
            wire.origin,
            wire.message,
            wire.retry_advice,
        )
        .map_err(de::Error::custom)?;
        failure.details = wire.details;
        failure.caused_by_event_id = wire.caused_by_event_id;
        Ok(failure)
    }
}

impl JsonSchema for Failure {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Failure".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Failure").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        FailureWire::json_schema(generator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Digest, SchemaId, Version};
    use serde_json::{from_value, json, to_value};

    fn schema_reference() -> SchemaReference {
        SchemaReference::new(
            "https://stateknot.github.io/schema/failure-details/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"failure details schema"),
        )
    }

    fn failure(category: FailureCategory, retry_advice: RetryAdvice) -> Failure {
        Failure::new(
            FailureId::generate(),
            category,
            "dependency.rate_limited".parse().unwrap(),
            "provider.openai".parse().unwrap(),
            "The dependency is temporarily unavailable."
                .parse()
                .unwrap(),
            retry_advice,
        )
        .unwrap()
    }

    #[test]
    fn stable_failure_identifiers_enforce_the_segmented_grammar() {
        for value in [
            "a",
            "rate_limited",
            "provider.openai",
            "stateknot.tool-runtime_v2",
        ] {
            assert_eq!(value.parse::<FailureCode>().unwrap().as_str(), value);
            assert_eq!(value.parse::<FailureOrigin>().unwrap().as_str(), value);
        }

        for value in [
            "",
            "A",
            "2provider",
            ".provider",
            "provider.",
            "provider..openai",
            "provider/OpenAI",
            "provider:openai",
            "provider.é",
        ] {
            assert!(value.parse::<FailureCode>().is_err(), "accepted {value:?}");
            assert!(
                value.parse::<FailureOrigin>().is_err(),
                "accepted {value:?}"
            );
        }

        let too_long = "a".repeat(FailureCode::MAX_LEN + 1);
        assert_eq!(
            too_long.parse::<FailureCode>(),
            Err(FailureIdentifierError::TooLong {
                max: FailureCode::MAX_LEN,
                actual: FailureCode::MAX_LEN + 1,
            })
        );
    }

    #[test]
    fn public_messages_are_bounded_single_line_values_with_redacted_debug() {
        let message = FailureMessage::new("请求暂时无法完成。").unwrap();
        assert_eq!(message.as_str(), "请求暂时无法完成。");
        assert_eq!(message.to_string(), "请求暂时无法完成。");
        assert!(!format!("{message:?}").contains("请求"));

        for value in [
            "",
            " padded",
            "padded ",
            "two\nlines",
            "tab\tinside",
            "line\u{2028}separator",
            "bidi\u{202e}override",
            "noncharacter\u{fdd0}",
        ] {
            assert!(FailureMessage::new(value).is_err(), "accepted {value:?}");
        }

        assert_eq!(
            FailureMessage::new("a".repeat(FailureMessage::MAX_BYTES + 1)),
            Err(FailureMessageError::TooLong {
                max: FailureMessage::MAX_BYTES,
                actual: FailureMessage::MAX_BYTES + 1,
            })
        );
    }

    #[test]
    fn categories_and_retry_advice_have_closed_canonical_wire_forms() {
        let categories = [
            (FailureCategory::InvalidInput, "invalid_input"),
            (FailureCategory::Unauthenticated, "unauthenticated"),
            (FailureCategory::PermissionDenied, "permission_denied"),
            (FailureCategory::PolicyDenied, "policy_denied"),
            (FailureCategory::NotFound, "not_found"),
            (FailureCategory::Conflict, "conflict"),
            (FailureCategory::Unsupported, "unsupported"),
            (FailureCategory::RateLimited, "rate_limited"),
            (FailureCategory::DeadlineExceeded, "deadline_exceeded"),
            (FailureCategory::Cancelled, "cancelled"),
            (
                FailureCategory::DependencyUnavailable,
                "dependency_unavailable",
            ),
            (FailureCategory::DataCorruption, "data_corruption"),
            (
                FailureCategory::AmbiguousExternalOutcome,
                "ambiguous_external_outcome",
            ),
            (FailureCategory::Internal, "internal"),
        ];
        for (category, text) in categories {
            assert_eq!(to_value(category).unwrap(), json!(text));
            assert_eq!(
                from_value::<FailureCategory>(json!(text)).unwrap(),
                category
            );
        }
        assert!(from_value::<FailureCategory>(json!("unknown")).is_err());

        let retry_forms = [
            (RetryAdvice::Never, json!({ "kind": "never" })),
            (
                RetryAdvice::SafeAfter {
                    delay: DurationMillis::new(250).unwrap(),
                },
                json!({ "kind": "safe_after", "delay": "250" }),
            ),
            (
                RetryAdvice::ReconcileFirst,
                json!({ "kind": "reconcile_first" }),
            ),
        ];
        for (advice, value) in retry_forms {
            assert_eq!(to_value(advice).unwrap(), value);
            assert_eq!(from_value::<RetryAdvice>(value).unwrap(), advice);
        }
        assert!(from_value::<RetryAdvice>(json!({ "kind": "never", "delay": "1" })).is_err());
        assert!(from_value::<RetryAdvice>(json!({ "kind": "retry" })).is_err());
    }

    #[test]
    fn failure_details_revalidate_every_tighter_json_dimension() {
        let valid = FailureDetails::try_from_value(
            schema_reference(),
            json!({ "field": "region", "reason": "unsupported" }),
        )
        .unwrap();
        assert_eq!(valid.value().as_value()["field"], "region");
        assert!(!format!("{valid:?}").contains("unsupported"));

        let long_string = Value::String("a".repeat(FailureDetails::MAX_JSON_STRING_BYTES + 1));
        assert!(matches!(
            FailureDetails::try_from_value(schema_reference(), long_string),
            Err(FailureDetailsError::Json {
                source: BoundedJsonError::StringTooLong { .. }
            })
        ));

        let many_entries = Value::Array(
            (0..=FailureDetails::MAX_JSON_CONTAINER_ENTRIES)
                .map(|value| json!(value))
                .collect(),
        );
        assert!(matches!(
            FailureDetails::try_from_value(schema_reference(), many_entries),
            Err(FailureDetailsError::Json {
                source: BoundedJsonError::TooManyContainerEntries { .. }
            })
        ));

        let mut nested = Value::Null;
        for _ in 0..=FailureDetails::MAX_JSON_DEPTH {
            nested = Value::Array(vec![nested]);
        }
        assert!(matches!(
            FailureDetails::try_from_value(schema_reference(), nested),
            Err(FailureDetailsError::Json {
                source: BoundedJsonError::NestingTooDeep { .. }
            })
        ));

        let oversized_compact = Value::Array(
            (0..5)
                .map(|_| Value::String("a".repeat(FailureDetails::MAX_JSON_STRING_BYTES)))
                .collect(),
        );
        assert!(matches!(
            FailureDetails::try_from_value(schema_reference(), oversized_compact),
            Err(FailureDetailsError::Json {
                source: BoundedJsonError::CompactRepresentationTooLarge { .. }
            })
        ));

        let too_many_nodes = Value::Array(
            (0..FailureDetails::MAX_JSON_CONTAINER_ENTRIES)
                .map(|_| Value::Array((0..8).map(|_| Value::Null).collect()))
                .collect(),
        );
        assert!(matches!(
            FailureDetails::try_from_value(schema_reference(), too_many_nodes),
            Err(FailureDetailsError::Json {
                source: BoundedJsonError::TooManyNodes { .. }
            })
        ));

        let mut oversized_key = serde_json::Map::new();
        oversized_key.insert(
            "k".repeat(FailureDetails::MAX_JSON_OBJECT_KEY_BYTES + 1),
            Value::Null,
        );
        assert!(matches!(
            FailureDetails::try_from_value(schema_reference(), Value::Object(oversized_key)),
            Err(FailureDetailsError::Json {
                source: BoundedJsonError::ObjectKeyTooLong { .. }
            })
        ));

        let limits = failure_details_json_limits().unwrap();
        assert_eq!(limits.max_bytes(), FailureDetails::MAX_JSON_BYTES);
        assert_eq!(limits.max_depth(), FailureDetails::MAX_JSON_DEPTH);
        assert_eq!(
            limits.max_container_entries(),
            FailureDetails::MAX_JSON_CONTAINER_ENTRIES
        );
        assert_eq!(limits.max_nodes(), FailureDetails::MAX_JSON_NODES);
        assert_eq!(
            limits.max_string_bytes(),
            FailureDetails::MAX_JSON_STRING_BYTES
        );
        assert_eq!(
            limits.max_object_key_bytes(),
            FailureDetails::MAX_JSON_OBJECT_KEY_BYTES
        );
    }

    #[test]
    fn ambiguous_outcomes_and_reconciliation_are_an_exact_pair() {
        assert!(
            Failure::new(
                FailureId::generate(),
                FailureCategory::AmbiguousExternalOutcome,
                "tool.outcome_unknown".parse().unwrap(),
                "tool.payments".parse().unwrap(),
                "The payment outcome must be reconciled.".parse().unwrap(),
                RetryAdvice::ReconcileFirst,
            )
            .is_ok()
        );

        assert_eq!(
            Failure::new(
                FailureId::generate(),
                FailureCategory::AmbiguousExternalOutcome,
                "tool.outcome_unknown".parse().unwrap(),
                "tool.payments".parse().unwrap(),
                "The payment outcome must be reconciled.".parse().unwrap(),
                RetryAdvice::Never,
            )
            .unwrap_err(),
            FailureBuildError::AmbiguousOutcomeRequiresReconciliation
        );
        assert_eq!(
            Failure::new(
                FailureId::generate(),
                FailureCategory::Internal,
                "runtime.invariant".parse().unwrap(),
                "stateknot.runtime".parse().unwrap(),
                "The operation could not be completed.".parse().unwrap(),
                RetryAdvice::ReconcileFirst,
            )
            .unwrap_err(),
            FailureBuildError::ReconciliationRequiresAmbiguousOutcome
        );
    }

    #[derive(Debug, Error)]
    #[error("private database diagnostics: password=secret")]
    struct PrivateDiagnostic;

    #[test]
    fn wire_debug_and_public_display_never_expose_private_diagnostics() {
        let details =
            FailureDetails::try_from_value(schema_reference(), json!({ "safe": "public detail" }))
                .unwrap();
        let event_id = EventId::generate();
        let failure = failure(FailureCategory::DependencyUnavailable, RetryAdvice::Never)
            .with_details(details)
            .with_caused_by_event(event_id)
            .with_private_source(PrivateDiagnostic);

        let encoded = to_value(&failure).unwrap();
        assert_eq!(encoded["caused_by_event_id"], event_id.to_string());
        assert!(encoded.get("private_source").is_none());
        assert!(!encoded.to_string().contains("password"));

        let debug = format!("{failure:?}");
        assert!(!debug.contains("password"));
        assert!(!debug.contains("public detail"));
        assert_eq!(
            failure.to_string(),
            "The dependency is temporarily unavailable."
        );
        assert!(StdError::source(&failure).is_none());
        assert!(failure.private_source().is_some());

        let decoded = from_value::<Failure>(encoded).unwrap();
        assert!(!decoded.has_private_source());
        assert_eq!(decoded.caused_by_event_id(), Some(event_id));
        assert_eq!(decoded.category(), failure.category());
    }

    #[test]
    fn deserialization_rejects_private_or_unknown_fields_and_bad_invariants() {
        let base = to_value(failure(FailureCategory::Internal, RetryAdvice::Never)).unwrap();
        let mut with_source = base.clone();
        with_source["private_source"] = json!("secret");
        assert!(from_value::<Failure>(with_source).is_err());

        let mut unknown = base.clone();
        unknown["extension"] = json!(true);
        assert!(from_value::<Failure>(unknown).is_err());

        let mut invalid = base;
        invalid["category"] = json!("ambiguous_external_outcome");
        assert!(from_value::<Failure>(invalid).is_err());
    }

    #[test]
    fn schemas_close_failure_objects_and_exclude_private_sources() {
        let schema = to_value(schemars::schema_for!(Failure)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert!(schema["properties"].get("private_source").is_none());
        assert!(schema["properties"].get("details").is_some());

        let details_schema = to_value(schemars::schema_for!(FailureDetails)).unwrap();
        assert_eq!(details_schema["type"], "object");
        assert_eq!(details_schema["additionalProperties"], false);

        let retry_schema = to_value(schemars::schema_for!(RetryAdvice)).unwrap();
        let variants = retry_schema["oneOf"].as_array().unwrap();
        assert_eq!(variants.len(), 3);
        assert!(
            variants
                .iter()
                .all(|variant| variant["additionalProperties"] == false),
            "retry schema was not closed: {retry_schema}"
        );

        let code_schema = to_value(schemars::schema_for!(FailureCode)).unwrap();
        assert_eq!(code_schema["pattern"], FAILURE_IDENTIFIER_PATTERN);
        assert_eq!(code_schema["maxLength"], FailureCode::MAX_LEN);

        let message_schema = to_value(schemars::schema_for!(FailureMessage)).unwrap();
        assert_eq!(message_schema["maxLength"], FailureMessage::MAX_BYTES);
    }
}
