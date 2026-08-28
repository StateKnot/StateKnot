// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Resource-bounded JSON values for untrusted runtime boundaries.

use std::{collections::BTreeMap, fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};
use thiserror::Error;

const KIBIBYTE: usize = 1024;
const MEBIBYTE: usize = KIBIBYTE * KIBIBYTE;

/// A configurable dimension of the bounded JSON contract.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum JsonLimit {
    /// Raw and compact encoded JSON bytes.
    Bytes,
    /// Nested array/object depth.
    Depth,
    /// Members in one object or elements in one array.
    ContainerEntries,
    /// Total JSON value nodes, excluding object keys.
    Nodes,
    /// Decoded UTF-8 bytes in one string value.
    StringBytes,
    /// Decoded UTF-8 bytes in one object member name.
    ObjectKeyBytes,
}

impl fmt::Display for JsonLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Bytes => "JSON bytes",
            Self::Depth => "JSON nesting depth",
            Self::ContainerEntries => "JSON container entries",
            Self::Nodes => "JSON value nodes",
            Self::StringBytes => "JSON string bytes",
            Self::ObjectKeyBytes => "JSON object-key bytes",
        })
    }
}

/// Validated resource limits applied while JSON is materialized.
///
/// The same byte ceiling is applied to the raw input before parsing and to the
/// compact representation of the resulting value. Limits cannot exceed
/// [`JsonLimits::MAXIMUM`], so callers cannot accidentally disable the process
/// safety boundary through configuration.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JsonLimits {
    max_bytes: usize,
    max_depth: usize,
    max_container_entries: usize,
    max_nodes: usize,
    max_string_bytes: usize,
    max_object_key_bytes: usize,
}

impl JsonLimits {
    /// Default limits for messages, tool values, and extensions.
    pub const DEFAULT: Self = Self {
        max_bytes: 256 * KIBIBYTE,
        max_depth: 32,
        max_container_entries: 1024,
        max_nodes: 16_384,
        max_string_bytes: 64 * KIBIBYTE,
        max_object_key_bytes: 256,
    };

    /// Absolute v1 ceilings accepted by [`JsonLimits::try_new`].
    pub const MAXIMUM: Self = Self {
        max_bytes: 2 * MEBIBYTE,
        max_depth: 64,
        max_container_entries: 8192,
        max_nodes: 131_072,
        max_string_bytes: MEBIBYTE,
        max_object_key_bytes: 1024,
    };

    /// Constructs a validated set of non-zero JSON limits.
    ///
    /// # Errors
    ///
    /// Returns [`JsonLimitsError`] if a dimension is zero or exceeds the
    /// corresponding hard ceiling in [`JsonLimits::MAXIMUM`].
    pub fn try_new(
        max_bytes: usize,
        max_depth: usize,
        max_container_entries: usize,
        max_nodes: usize,
        max_string_bytes: usize,
        max_object_key_bytes: usize,
    ) -> Result<Self, JsonLimitsError> {
        validate_configured_limit(JsonLimit::Bytes, max_bytes, Self::MAXIMUM.max_bytes)?;
        validate_configured_limit(JsonLimit::Depth, max_depth, Self::MAXIMUM.max_depth)?;
        validate_configured_limit(
            JsonLimit::ContainerEntries,
            max_container_entries,
            Self::MAXIMUM.max_container_entries,
        )?;
        validate_configured_limit(JsonLimit::Nodes, max_nodes, Self::MAXIMUM.max_nodes)?;
        validate_configured_limit(
            JsonLimit::StringBytes,
            max_string_bytes,
            Self::MAXIMUM.max_string_bytes,
        )?;
        validate_configured_limit(
            JsonLimit::ObjectKeyBytes,
            max_object_key_bytes,
            Self::MAXIMUM.max_object_key_bytes,
        )?;

        Ok(Self {
            max_bytes,
            max_depth,
            max_container_entries,
            max_nodes,
            max_string_bytes,
            max_object_key_bytes,
        })
    }

    /// Returns the raw and compact encoded byte ceiling.
    #[must_use]
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    /// Returns the array/object nesting-depth ceiling.
    #[must_use]
    pub const fn max_depth(self) -> usize {
        self.max_depth
    }

    /// Returns the per-array and per-object entry ceiling.
    #[must_use]
    pub const fn max_container_entries(self) -> usize {
        self.max_container_entries
    }

    /// Returns the total value-node ceiling.
    #[must_use]
    pub const fn max_nodes(self) -> usize {
        self.max_nodes
    }

    /// Returns the decoded byte ceiling for one string value.
    #[must_use]
    pub const fn max_string_bytes(self) -> usize {
        self.max_string_bytes
    }

    /// Returns the decoded byte ceiling for one object member name.
    #[must_use]
    pub const fn max_object_key_bytes(self) -> usize {
        self.max_object_key_bytes
    }
}

impl Default for JsonLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

fn validate_configured_limit(
    limit: JsonLimit,
    actual: usize,
    maximum: usize,
) -> Result<(), JsonLimitsError> {
    if actual == 0 {
        return Err(JsonLimitsError::Zero { limit });
    }
    if actual > maximum {
        return Err(JsonLimitsError::AboveHardMaximum {
            limit,
            maximum,
            actual,
        });
    }
    Ok(())
}

/// Invalid configuration for [`JsonLimits`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum JsonLimitsError {
    /// A limit was configured as zero.
    #[error("{limit} limit must be greater than zero")]
    Zero {
        /// The invalid dimension.
        limit: JsonLimit,
    },

    /// A configured limit exceeded `StateKnot`'s absolute v1 ceiling.
    #[error("configured {limit} limit is {actual}; hard maximum is {maximum}")]
    AboveHardMaximum {
        /// The invalid dimension.
        limit: JsonLimit,
        /// `StateKnot`'s hard ceiling.
        maximum: usize,
        /// The configured value.
        actual: usize,
    },
}

/// Safe aggregate statistics for a materialized [`BoundedJson`] value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JsonStats {
    compact_bytes: usize,
    max_depth: usize,
    nodes: usize,
}

impl JsonStats {
    /// Returns the exact byte length produced by compact `serde_json`
    /// serialization of this value.
    #[must_use]
    pub const fn compact_bytes(self) -> usize {
        self.compact_bytes
    }

    /// Returns the deepest observed array/object nesting level.
    ///
    /// Scalar roots have depth zero and a root container has depth one.
    #[must_use]
    pub const fn max_depth(self) -> usize {
        self.max_depth
    }

    /// Returns the number of JSON values, excluding object member names.
    #[must_use]
    pub const fn nodes(self) -> usize {
        self.nodes
    }
}

/// JSON rejected by syntax, interoperability, or resource-safety validation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum BoundedJsonError {
    /// The raw input exceeded the configured byte ceiling before parsing.
    #[error("JSON input is {actual} bytes; maximum is {maximum}")]
    InputTooLarge {
        /// Configured maximum.
        maximum: usize,
        /// Observed input length.
        actual: usize,
    },

    /// The compact materialized representation exceeded the byte ceiling.
    #[error("compact JSON reached {actual} bytes; maximum is {maximum}")]
    CompactRepresentationTooLarge {
        /// Configured maximum.
        maximum: usize,
        /// First observed size beyond the maximum.
        actual: usize,
    },

    /// Array/object nesting exceeded the configured depth.
    #[error("JSON nesting depth reached {actual}; maximum is {maximum}")]
    NestingTooDeep {
        /// Configured maximum.
        maximum: usize,
        /// First observed depth beyond the maximum.
        actual: usize,
    },

    /// One array or object contained too many entries.
    #[error("JSON container reached {actual} entries; maximum is {maximum}")]
    TooManyContainerEntries {
        /// Configured maximum.
        maximum: usize,
        /// First observed count beyond the maximum.
        actual: usize,
    },

    /// The document contained too many value nodes.
    #[error("JSON document reached {actual} value nodes; maximum is {maximum}")]
    TooManyNodes {
        /// Configured maximum.
        maximum: usize,
        /// First observed count beyond the maximum.
        actual: usize,
    },

    /// One decoded string value exceeded its byte ceiling.
    #[error("JSON string is {actual} decoded bytes; maximum is {maximum}")]
    StringTooLong {
        /// Configured maximum.
        maximum: usize,
        /// Observed decoded UTF-8 byte length.
        actual: usize,
    },

    /// One decoded object member name exceeded its byte ceiling.
    #[error("JSON object key is {actual} decoded bytes; maximum is {maximum}")]
    ObjectKeyTooLong {
        /// Configured maximum.
        maximum: usize,
        /// Observed decoded UTF-8 byte length.
        actual: usize,
    },

    /// An object repeated a member name after JSON escape processing.
    #[error("JSON object contains a duplicate member name")]
    DuplicateObjectKey,

    /// A generic Serde source supplied NaN or infinity, which JSON forbids.
    #[error("JSON numbers must be finite")]
    NonFiniteNumber,

    /// A generic Serde source supplied an integer unsupported by `serde_json`.
    #[error("JSON integer is outside serde_json's supported range")]
    NumberOutOfRange,

    /// The input was not exactly one syntactically valid JSON value.
    #[error("invalid JSON at line {line}, column {column}")]
    InvalidJson {
        /// One-based line reported by `serde_json` when available.
        line: usize,
        /// One-based column reported by `serde_json` when available.
        column: usize,
    },
}

/// An immutable JSON value materialized under validated resource limits.
///
/// Untrusted wire data must enter through [`BoundedJson::from_slice`] or
/// [`BoundedJson::from_str`]. These constructors reject oversized raw input
/// before parsing, reject duplicate object names after escape processing, and
/// enforce all semantic limits while values are visited. Constructing from an
/// existing [`Value`] is intended only for trusted in-process data because a
/// `Value` has already discarded duplicate member names.
///
/// This type is a resource-safety substrate. It does not perform JSON Schema
/// validation and its sorted object storage is not RFC 8785 canonicalization.
/// Its generic [`Deserialize`] implementation enforces semantic and compact
/// limits, but cannot observe whitespace outside the value; enclosing
/// transports must independently cap their raw body or record size.
#[derive(Clone, Eq, PartialEq)]
pub struct BoundedJson {
    value: Value,
    stats: JsonStats,
}

impl BoundedJson {
    /// Parses one JSON value with [`JsonLimits::DEFAULT`].
    ///
    /// # Errors
    ///
    /// Returns [`BoundedJsonError`] for invalid JSON, duplicate object names,
    /// trailing data, or a resource-limit violation.
    pub fn from_slice(input: &[u8]) -> Result<Self, BoundedJsonError> {
        Self::from_slice_with_limits(input, JsonLimits::DEFAULT)
    }

    /// Parses one JSON value with explicitly validated limits.
    ///
    /// The raw input length, including insignificant whitespace, is checked
    /// before the parser runs.
    ///
    /// # Errors
    ///
    /// Returns [`BoundedJsonError`] for invalid JSON, duplicate object names,
    /// trailing data, or a resource-limit violation.
    pub fn from_slice_with_limits(
        input: &[u8],
        limits: JsonLimits,
    ) -> Result<Self, BoundedJsonError> {
        if input.len() > limits.max_bytes {
            return Err(BoundedJsonError::InputTooLarge {
                maximum: limits.max_bytes,
                actual: input.len(),
            });
        }

        let mut tracker = Tracker::new(limits);
        let mut deserializer = serde_json::Deserializer::from_slice(input);
        let parsed = ValueSeed {
            tracker: &mut tracker,
            parent_depth: 0,
        }
        .deserialize(&mut deserializer);

        let value = match parsed {
            Ok(value) => value,
            Err(error) => return Err(map_deserialize_error(&mut tracker, &error)),
        };

        if let Err(error) = deserializer.end() {
            return Err(map_deserialize_error(&mut tracker, &error));
        }

        Ok(Self {
            value,
            stats: tracker.stats(),
        })
    }

    /// Parses one UTF-8 JSON value with [`JsonLimits::DEFAULT`].
    ///
    /// # Errors
    ///
    /// Returns [`BoundedJsonError`] for invalid JSON, duplicate object names,
    /// trailing data, or a resource-limit violation.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(input: &str) -> Result<Self, BoundedJsonError> {
        Self::from_slice(input.as_bytes())
    }

    /// Parses one UTF-8 JSON value with explicitly validated limits.
    ///
    /// # Errors
    ///
    /// Returns [`BoundedJsonError`] for invalid JSON, duplicate object names,
    /// trailing data, or a resource-limit violation.
    pub fn from_str_with_limits(input: &str, limits: JsonLimits) -> Result<Self, BoundedJsonError> {
        Self::from_slice_with_limits(input.as_bytes(), limits)
    }

    /// Validates and deterministically rebuilds a trusted in-process value
    /// with [`JsonLimits::DEFAULT`].
    ///
    /// This cannot detect duplicate object names because [`Value`] has already
    /// collapsed them. Wire data must use [`BoundedJson::from_slice`].
    ///
    /// # Errors
    ///
    /// Returns [`BoundedJsonError`] if the materialized value violates a
    /// resource or numeric limit.
    pub fn try_from_value(value: Value) -> Result<Self, BoundedJsonError> {
        Self::try_from_value_with_limits(value, JsonLimits::DEFAULT)
    }

    /// Validates and deterministically rebuilds a trusted in-process value
    /// with explicit limits.
    ///
    /// This cannot detect duplicate object names because [`Value`] has already
    /// collapsed them. Wire data must use [`BoundedJson::from_slice`].
    ///
    /// # Errors
    ///
    /// Returns [`BoundedJsonError`] if the materialized value violates a
    /// resource or numeric limit.
    pub fn try_from_value_with_limits(
        value: Value,
        limits: JsonLimits,
    ) -> Result<Self, BoundedJsonError> {
        let mut tracker = Tracker::new(limits);
        let parsed = ValueSeed {
            tracker: &mut tracker,
            parent_depth: 0,
        }
        .deserialize(value);

        match parsed {
            Ok(value) => Ok(Self {
                value,
                stats: tracker.stats(),
            }),
            Err(error) => Err(map_deserialize_error(&mut tracker, &error)),
        }
    }

    /// Borrows the validated JSON value without permitting mutation.
    #[must_use]
    pub const fn as_value(&self) -> &Value {
        &self.value
    }

    /// Consumes this wrapper and returns the materialized JSON value.
    #[must_use]
    pub fn into_value(self) -> Value {
        self.value
    }

    /// Returns non-sensitive aggregate statistics captured during validation.
    #[must_use]
    pub const fn stats(&self) -> JsonStats {
        self.stats
    }
}

impl AsRef<Value> for BoundedJson {
    fn as_ref(&self) -> &Value {
        self.as_value()
    }
}

impl fmt::Debug for BoundedJson {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedJson")
            .field("compact_bytes", &self.stats.compact_bytes)
            .field("max_depth", &self.stats.max_depth)
            .field("nodes", &self.stats.nodes)
            .finish_non_exhaustive()
    }
}

impl FromStr for BoundedJson {
    type Err = BoundedJsonError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::from_str(input)
    }
}

impl TryFrom<Value> for BoundedJson {
    type Error = BoundedJsonError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Self::try_from_value(value)
    }
}

impl Serialize for BoundedJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BoundedJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut tracker = Tracker::new(JsonLimits::DEFAULT);
        let value = ValueSeed {
            tracker: &mut tracker,
            parent_depth: 0,
        }
        .deserialize(deserializer)?;

        Ok(Self {
            value,
            stats: tracker.stats(),
        })
    }
}

impl JsonSchema for BoundedJson {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "BoundedJson".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::BoundedJson").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "Any JSON value. StateKnot enforces byte, depth, container-entry, node, string, object-key, duplicate-key, and finite-number constraints at runtime; JSON Schema alone cannot express those aggregate limits."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

fn map_deserialize_error(tracker: &mut Tracker, error: &serde_json::Error) -> BoundedJsonError {
    tracker
        .violation
        .take()
        .unwrap_or(BoundedJsonError::InvalidJson {
            line: error.line(),
            column: error.column(),
        })
}

struct Tracker {
    limits: JsonLimits,
    compact_bytes: usize,
    max_depth: usize,
    nodes: usize,
    violation: Option<BoundedJsonError>,
}

impl Tracker {
    const fn new(limits: JsonLimits) -> Self {
        Self {
            limits,
            compact_bytes: 0,
            max_depth: 0,
            nodes: 0,
            violation: None,
        }
    }

    const fn stats(&self) -> JsonStats {
        JsonStats {
            compact_bytes: self.compact_bytes,
            max_depth: self.max_depth,
            nodes: self.nodes,
        }
    }

    fn reject<T, E>(&mut self, error: BoundedJsonError) -> Result<T, E>
    where
        E: de::Error,
    {
        let message = error.to_string();
        if self.violation.is_none() {
            self.violation = Some(error);
        }
        Err(E::custom(message))
    }

    fn add_node<E>(&mut self) -> Result<(), E>
    where
        E: de::Error,
    {
        let actual = self.nodes.saturating_add(1);
        if actual > self.limits.max_nodes {
            return self.reject(BoundedJsonError::TooManyNodes {
                maximum: self.limits.max_nodes,
                actual,
            });
        }
        self.nodes = actual;
        Ok(())
    }

    fn enter_container<E>(&mut self, parent_depth: usize) -> Result<usize, E>
    where
        E: de::Error,
    {
        let actual = parent_depth.saturating_add(1);
        if actual > self.limits.max_depth {
            return self.reject(BoundedJsonError::NestingTooDeep {
                maximum: self.limits.max_depth,
                actual,
            });
        }
        self.max_depth = self.max_depth.max(actual);
        Ok(actual)
    }

    fn add_compact_bytes<E>(&mut self, additional: usize) -> Result<(), E>
    where
        E: de::Error,
    {
        let actual = self.compact_bytes.saturating_add(additional);
        if actual > self.limits.max_bytes {
            return self.reject(BoundedJsonError::CompactRepresentationTooLarge {
                maximum: self.limits.max_bytes,
                actual,
            });
        }
        self.compact_bytes = actual;
        Ok(())
    }

    fn check_string<E>(&mut self, actual: usize) -> Result<(), E>
    where
        E: de::Error,
    {
        if actual > self.limits.max_string_bytes {
            return self.reject(BoundedJsonError::StringTooLong {
                maximum: self.limits.max_string_bytes,
                actual,
            });
        }
        Ok(())
    }

    fn check_object_key<E>(&mut self, actual: usize) -> Result<(), E>
    where
        E: de::Error,
    {
        if actual > self.limits.max_object_key_bytes {
            return self.reject(BoundedJsonError::ObjectKeyTooLong {
                maximum: self.limits.max_object_key_bytes,
                actual,
            });
        }
        Ok(())
    }
}

struct ValueSeed<'tracker> {
    tracker: &'tracker mut Tracker,
    parent_depth: usize,
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.tracker.add_node::<D::Error>()?;
        deserializer.deserialize_any(ValueVisitor {
            tracker: self.tracker,
            parent_depth: self.parent_depth,
        })
    }
}

struct ValueVisitor<'tracker> {
    tracker: &'tracker mut Tracker,
    parent_depth: usize,
}

impl ValueVisitor<'_> {
    fn visit_number<E>(self, number: Number) -> Result<Value, E>
    where
        E: de::Error,
    {
        self.tracker
            .add_compact_bytes::<E>(number.to_string().len())?;
        Ok(Value::Number(number))
    }

    fn visit_string_value<E>(self, value: String) -> Result<Value, E>
    where
        E: de::Error,
    {
        self.tracker.check_string::<E>(value.len())?;
        self.tracker
            .add_compact_bytes::<E>(encoded_json_string_len(&value))?;
        Ok(Value::String(value))
    }
}

impl<'de> Visitor<'de> for ValueVisitor<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a resource-bounded JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.tracker
            .add_compact_bytes::<E>(if value { 4 } else { 5 })?;
        Ok(Value::Bool(value))
    }

    fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(i64::from(value))
    }

    fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(i64::from(value))
    }

    fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_i64(i64::from(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_number(value.into())
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let Some(number) = Number::from_i128(value) else {
            return self.tracker.reject(BoundedJsonError::NumberOutOfRange);
        };
        self.visit_number(number)
    }

    fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_u64(u64::from(value))
    }

    fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_u64(u64::from(value))
    }

    fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_u64(u64::from(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_number(value.into())
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let Some(number) = Number::from_u128(value) else {
            return self.tracker.reject(BoundedJsonError::NumberOutOfRange);
        };
        self.visit_number(number)
    }

    fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_f64(f64::from(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let Some(number) = Number::from_f64(value) else {
            return self.tracker.reject(BoundedJsonError::NonFiniteNumber);
        };
        self.visit_number(number)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.tracker.check_string::<E>(value.len())?;
        self.tracker
            .add_compact_bytes::<E>(encoded_json_string_len(value))?;
        Ok(Value::String(value.to_owned()))
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(value)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string_value(value)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_unit()
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.tracker.add_compact_bytes::<E>(4)?;
        Ok(Value::Null)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let depth = self
            .tracker
            .enter_container::<A::Error>(self.parent_depth)?;
        self.tracker.add_compact_bytes::<A::Error>(2)?;

        let maximum = self.tracker.limits.max_container_entries;
        let remaining_nodes = self
            .tracker
            .limits
            .max_nodes
            .saturating_sub(self.tracker.nodes);
        let capacity = sequence
            .size_hint()
            .unwrap_or(0)
            .min(maximum)
            .min(remaining_nodes);
        let mut values = Vec::with_capacity(capacity);

        while values.len() < maximum {
            let next = sequence.next_element_seed(ArrayElementSeed {
                tracker: self.tracker,
                parent_depth: depth,
                separator: !values.is_empty(),
            })?;
            match next {
                Some(value) => values.push(value),
                None => return Ok(Value::Array(values)),
            }
        }

        let _: Option<()> = sequence.next_element_seed(RejectExtraEntrySeed {
            tracker: self.tracker,
            maximum,
        })?;
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let depth = self
            .tracker
            .enter_container::<A::Error>(self.parent_depth)?;
        self.tracker.add_compact_bytes::<A::Error>(2)?;

        let maximum = self.tracker.limits.max_container_entries;
        let mut sorted = BTreeMap::new();

        while sorted.len() < maximum {
            let next_key = object.next_key_seed(ObjectKeySeed {
                tracker: self.tracker,
                separator: !sorted.is_empty(),
            })?;
            let Some(key) = next_key else {
                return Ok(Value::Object(sorted_map(sorted)));
            };

            if sorted.contains_key(&key) {
                return self.tracker.reject(BoundedJsonError::DuplicateObjectKey);
            }

            let value = object.next_value_seed(ValueSeed {
                tracker: self.tracker,
                parent_depth: depth,
            })?;
            sorted.insert(key, value);
        }

        let _: Option<()> = object.next_key_seed(RejectExtraEntrySeed {
            tracker: self.tracker,
            maximum,
        })?;
        Ok(Value::Object(sorted_map(sorted)))
    }
}

struct ArrayElementSeed<'tracker> {
    tracker: &'tracker mut Tracker,
    parent_depth: usize,
    separator: bool,
}

impl<'de> DeserializeSeed<'de> for ArrayElementSeed<'_> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        if self.separator {
            self.tracker.add_compact_bytes::<D::Error>(1)?;
        }
        ValueSeed {
            tracker: self.tracker,
            parent_depth: self.parent_depth,
        }
        .deserialize(deserializer)
    }
}

struct ObjectKeySeed<'tracker> {
    tracker: &'tracker mut Tracker,
    separator: bool,
}

impl<'de> DeserializeSeed<'de> for ObjectKeySeed<'_> {
    type Value = String;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(ObjectKeyVisitor {
            tracker: self.tracker,
            separator: self.separator,
        })
    }
}

struct ObjectKeyVisitor<'tracker> {
    tracker: &'tracker mut Tracker,
    separator: bool,
}

impl ObjectKeyVisitor<'_> {
    fn visit_key<E>(self, value: &str) -> Result<String, E>
    where
        E: de::Error,
    {
        self.tracker.check_object_key::<E>(value.len())?;
        if self.separator {
            self.tracker.add_compact_bytes::<E>(1)?;
        }
        self.tracker
            .add_compact_bytes::<E>(encoded_json_string_len(value).saturating_add(1))?;
        Ok(value.to_owned())
    }
}

impl<'de> Visitor<'de> for ObjectKeyVisitor<'_> {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded JSON object member name")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_key(value)
    }

    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_key(value)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.tracker.check_object_key::<E>(value.len())?;
        if self.separator {
            self.tracker.add_compact_bytes::<E>(1)?;
        }
        self.tracker
            .add_compact_bytes::<E>(encoded_json_string_len(&value).saturating_add(1))?;
        Ok(value)
    }
}

struct RejectExtraEntrySeed<'tracker> {
    tracker: &'tracker mut Tracker,
    maximum: usize,
}

impl<'de> DeserializeSeed<'de> for RejectExtraEntrySeed<'_> {
    type Value = ();

    fn deserialize<D>(self, _deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        self.tracker
            .reject(BoundedJsonError::TooManyContainerEntries {
                maximum: self.maximum,
                actual: self.maximum.saturating_add(1),
            })
    }
}

fn sorted_map(values: BTreeMap<String, Value>) -> Map<String, Value> {
    values.into_iter().collect()
}

fn encoded_json_string_len(value: &str) -> usize {
    value.bytes().fold(2_usize, |length, byte| {
        length.saturating_add(match byte {
            b'\x08' | b'\t' | b'\n' | b'\x0c' | b'\r' | b'"' | b'\\' => 2,
            0x00..=0x1f => 6,
            _ => 1,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{collection, prelude::*};
    use serde::de::value::{F64Deserializer, U128Deserializer};
    use serde_json::json;

    fn limits(
        max_bytes: usize,
        max_depth: usize,
        max_container_entries: usize,
        max_nodes: usize,
        max_string_bytes: usize,
        max_object_key_bytes: usize,
    ) -> JsonLimits {
        JsonLimits::try_new(
            max_bytes,
            max_depth,
            max_container_entries,
            max_nodes,
            max_string_bytes,
            max_object_key_bytes,
        )
        .unwrap()
    }

    #[test]
    fn default_and_hard_limits_are_stable_and_validated() {
        assert_eq!(JsonLimits::default(), JsonLimits::DEFAULT);
        assert_eq!(JsonLimits::DEFAULT.max_bytes(), 256 * KIBIBYTE);
        assert_eq!(JsonLimits::DEFAULT.max_depth(), 32);
        assert_eq!(JsonLimits::DEFAULT.max_container_entries(), 1024);
        assert_eq!(JsonLimits::DEFAULT.max_nodes(), 16_384);
        assert_eq!(JsonLimits::DEFAULT.max_string_bytes(), 64 * KIBIBYTE);
        assert_eq!(JsonLimits::DEFAULT.max_object_key_bytes(), 256);

        for limit in [
            JsonLimit::Bytes,
            JsonLimit::Depth,
            JsonLimit::ContainerEntries,
            JsonLimit::Nodes,
            JsonLimit::StringBytes,
            JsonLimit::ObjectKeyBytes,
        ] {
            let result = match limit {
                JsonLimit::Bytes => JsonLimits::try_new(0, 1, 1, 1, 1, 1),
                JsonLimit::Depth => JsonLimits::try_new(1, 0, 1, 1, 1, 1),
                JsonLimit::ContainerEntries => JsonLimits::try_new(1, 1, 0, 1, 1, 1),
                JsonLimit::Nodes => JsonLimits::try_new(1, 1, 1, 0, 1, 1),
                JsonLimit::StringBytes => JsonLimits::try_new(1, 1, 1, 1, 0, 1),
                JsonLimit::ObjectKeyBytes => JsonLimits::try_new(1, 1, 1, 1, 1, 0),
            };
            assert_eq!(result, Err(JsonLimitsError::Zero { limit }));
        }

        assert_eq!(
            JsonLimits::try_new(JsonLimits::MAXIMUM.max_bytes() + 1, 1, 1, 1, 1, 1),
            Err(JsonLimitsError::AboveHardMaximum {
                limit: JsonLimit::Bytes,
                maximum: JsonLimits::MAXIMUM.max_bytes(),
                actual: JsonLimits::MAXIMUM.max_bytes() + 1,
            })
        );

        for (limit, result, maximum) in [
            (
                JsonLimit::Depth,
                JsonLimits::try_new(1, JsonLimits::MAXIMUM.max_depth() + 1, 1, 1, 1, 1),
                JsonLimits::MAXIMUM.max_depth(),
            ),
            (
                JsonLimit::ContainerEntries,
                JsonLimits::try_new(
                    1,
                    1,
                    JsonLimits::MAXIMUM.max_container_entries() + 1,
                    1,
                    1,
                    1,
                ),
                JsonLimits::MAXIMUM.max_container_entries(),
            ),
            (
                JsonLimit::Nodes,
                JsonLimits::try_new(1, 1, 1, JsonLimits::MAXIMUM.max_nodes() + 1, 1, 1),
                JsonLimits::MAXIMUM.max_nodes(),
            ),
            (
                JsonLimit::StringBytes,
                JsonLimits::try_new(1, 1, 1, 1, JsonLimits::MAXIMUM.max_string_bytes() + 1, 1),
                JsonLimits::MAXIMUM.max_string_bytes(),
            ),
            (
                JsonLimit::ObjectKeyBytes,
                JsonLimits::try_new(
                    1,
                    1,
                    1,
                    1,
                    1,
                    JsonLimits::MAXIMUM.max_object_key_bytes() + 1,
                ),
                JsonLimits::MAXIMUM.max_object_key_bytes(),
            ),
        ] {
            assert_eq!(
                result,
                Err(JsonLimitsError::AboveHardMaximum {
                    limit,
                    maximum,
                    actual: maximum + 1,
                })
            );
        }
    }

    #[test]
    fn parses_every_json_kind_and_tracks_exact_compact_statistics() {
        let input = r#" { "z": [null, true, false, -2, 3, 1.5, "a\né"], "a": {} } "#;
        let bounded = BoundedJson::from_slice(input.as_bytes()).unwrap();
        let compact = serde_json::to_vec(bounded.as_value()).unwrap();

        assert_eq!(bounded.stats().compact_bytes(), compact.len());
        assert_eq!(bounded.stats().max_depth(), 2);
        assert_eq!(bounded.stats().nodes(), 10);
        assert_eq!(bounded.as_value()["z"][6], "a\né");

        let keys = bounded
            .as_value()
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(keys, ["a", "z"]);
    }

    #[test]
    fn rejects_duplicate_keys_after_escape_processing_before_reading_value() {
        for input in [
            r#"{"a":1,"a":2}"#,
            r#"{"a":1,"\u0061":2}"#,
            r#"{"nested":{"x":1,"\u0078":2}}"#,
        ] {
            assert_eq!(
                BoundedJson::from_str(input),
                Err(BoundedJsonError::DuplicateObjectKey),
                "accepted {input}"
            );
        }

        let deep_value = format!("{}0{}", "[".repeat(200), "]".repeat(200));
        let input = format!(r#"{{"a":1,"\u0061":{deep_value}}}"#);
        assert_eq!(
            BoundedJson::from_str(&input),
            Err(BoundedJsonError::DuplicateObjectKey)
        );
    }

    #[test]
    fn rejects_invalid_syntax_and_trailing_values_without_echoing_input() {
        for input in ["", "[1,]", r#"{"a":}"#, "true false", "\u{feff}null"] {
            let error = BoundedJson::from_str(input).unwrap_err();
            assert!(matches!(error, BoundedJsonError::InvalidJson { .. }));
            if !input.is_empty() {
                assert!(!error.to_string().contains(input));
            }
        }

        assert!(matches!(
            BoundedJson::from_slice(&[0xff]),
            Err(BoundedJsonError::InvalidJson { .. })
        ));
    }

    #[test]
    fn raw_input_is_bounded_before_whitespace_or_syntax_processing() {
        let configured = limits(4, 4, 4, 4, 4, 4);
        assert!(BoundedJson::from_str_with_limits("null", configured).is_ok());
        assert_eq!(
            BoundedJson::from_str_with_limits(" null", configured),
            Err(BoundedJsonError::InputTooLarge {
                maximum: 4,
                actual: 5,
            })
        );
    }

    #[test]
    fn compact_encoding_is_bounded_for_trusted_values() {
        let configured = limits(3, 4, 4, 4, 4, 4);
        assert!(
            BoundedJson::try_from_value_with_limits(Value::String("a".into()), configured).is_ok()
        );
        assert_eq!(
            BoundedJson::try_from_value_with_limits(Value::String("aa".into()), configured),
            Err(BoundedJsonError::CompactRepresentationTooLarge {
                maximum: 3,
                actual: 4,
            })
        );
    }

    #[test]
    fn nesting_depth_uses_zero_for_scalars_and_one_for_root_containers() {
        let configured = limits(128, 2, 8, 16, 32, 32);
        assert_eq!(
            BoundedJson::from_str_with_limits("0", configured)
                .unwrap()
                .stats()
                .max_depth(),
            0
        );
        assert_eq!(
            BoundedJson::from_str_with_limits("[[0]]", configured)
                .unwrap()
                .stats()
                .max_depth(),
            2
        );
        assert_eq!(
            BoundedJson::from_str_with_limits("[[[0]]]", configured),
            Err(BoundedJsonError::NestingTooDeep {
                maximum: 2,
                actual: 3,
            })
        );
    }

    #[test]
    fn container_limit_stops_before_traversing_an_extra_value() {
        let configured = limits(2048, 4, 2, 16, 32, 32);
        assert!(BoundedJson::from_str_with_limits("[0,1]", configured).is_ok());
        assert!(BoundedJson::from_str_with_limits(r#"{"a":0,"b":1}"#, configured).is_ok());

        let deep_value = format!("{}0{}", "[".repeat(200), "]".repeat(200));
        for input in [
            format!("[0,1,{deep_value}]"),
            format!(r#"{{"a":0,"b":1,"c":{deep_value}}}"#),
        ] {
            assert_eq!(
                BoundedJson::from_str_with_limits(&input, configured),
                Err(BoundedJsonError::TooManyContainerEntries {
                    maximum: 2,
                    actual: 3,
                })
            );
        }
    }

    #[test]
    fn node_limit_stops_before_traversing_an_extra_value() {
        let configured = limits(1024, 4, 8, 2, 32, 32);
        assert!(BoundedJson::from_str_with_limits("[0]", configured).is_ok());

        let deep_value = format!("{}0{}", "[".repeat(200), "]".repeat(200));
        let input = format!("[0,{deep_value}]");
        assert_eq!(
            BoundedJson::from_str_with_limits(&input, configured),
            Err(BoundedJsonError::TooManyNodes {
                maximum: 2,
                actual: 3,
            })
        );
    }

    #[test]
    fn string_and_key_limits_count_decoded_utf8_bytes() {
        let configured = limits(128, 4, 8, 8, 2, 2);
        assert!(BoundedJson::from_str_with_limits(r#""\u00e9""#, configured).is_ok());
        assert_eq!(
            BoundedJson::from_str_with_limits(r#""\u00e9a""#, configured),
            Err(BoundedJsonError::StringTooLong {
                maximum: 2,
                actual: 3,
            })
        );
        assert!(BoundedJson::from_str_with_limits(r#"{"\u00e9":0}"#, configured).is_ok());
        assert_eq!(
            BoundedJson::from_str_with_limits(r#"{"\u00e9a":0}"#, configured),
            Err(BoundedJsonError::ObjectKeyTooLong {
                maximum: 2,
                actual: 3,
            })
        );
    }

    #[test]
    fn generic_serde_rejects_non_json_numeric_values() {
        let nan_error =
            BoundedJson::deserialize(F64Deserializer::<de::value::Error>::new(f64::NAN))
                .unwrap_err();
        assert!(nan_error.to_string().contains("finite"));

        let integer_error =
            BoundedJson::deserialize(U128Deserializer::<de::value::Error>::new(u128::MAX))
                .unwrap_err();
        assert!(integer_error.to_string().contains("supported range"));
    }

    #[test]
    fn serde_round_trip_revalidates_and_debug_is_redacted() {
        let secret = "credential-like-secret";
        let bounded = BoundedJson::from_str(&format!(r#"{{"secret":"{secret}"}}"#)).unwrap();
        let debug = format!("{bounded:?}");
        assert!(!debug.contains(secret));
        assert!(debug.contains("compact_bytes"));

        let encoded = serde_json::to_string(&bounded).unwrap();
        let decoded: BoundedJson = serde_json::from_str(&encoded).unwrap();
        let parsed: BoundedJson = encoded.parse().unwrap();
        assert_eq!(decoded, bounded);
        assert_eq!(parsed, bounded);
        assert_eq!(decoded.stats(), bounded.stats());
    }

    #[test]
    fn trusted_value_construction_sorts_objects_without_claiming_canonicalization() {
        let mut object = Map::new();
        object.insert("z".into(), json!(1));
        object.insert("a".into(), json!(2));
        let bounded = BoundedJson::try_from_value(Value::Object(object)).unwrap();
        assert_eq!(serde_json::to_string(&bounded).unwrap(), r#"{"a":2,"z":1}"#);
        assert_eq!(bounded.stats().compact_bytes(), 13);

        let schema = serde_json::to_value(schemars::schema_for!(BoundedJson)).unwrap();
        assert!(schema.get("type").is_none());
        assert!(schema["description"].as_str().unwrap().contains("runtime"));
    }

    fn arbitrary_json() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(|value| Value::Number(value.into())),
            "[a-zA-Z0-9\\n\\t]{0,24}".prop_map(Value::String),
        ];

        leaf.prop_recursive(4, 64, 8, |inner| {
            prop_oneof![
                collection::vec(inner.clone(), 0..5).prop_map(Value::Array),
                collection::btree_map("[a-z]{1,8}", inner, 0..5)
                    .prop_map(|values| { Value::Object(values.into_iter().collect()) }),
            ]
        })
    }

    proptest! {
        #[test]
        fn bounded_values_round_trip_with_exact_compact_size(value in arbitrary_json()) {
            let encoded = serde_json::to_vec(&value).unwrap();
            let bounded = BoundedJson::from_slice(&encoded).unwrap();
            prop_assert_eq!(bounded.as_value(), &value);
            prop_assert_eq!(bounded.stats().compact_bytes(), encoded.len());

            let reparsed = BoundedJson::try_from_value(value).unwrap();
            prop_assert_eq!(reparsed.stats(), bounded.stats());
        }
    }
}
