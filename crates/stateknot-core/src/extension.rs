// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded, namespaced extension data without implicit authority.

use std::{
    borrow::Borrow,
    collections::{BTreeMap, btree_map},
    fmt,
    str::FromStr,
};

use fluent_uri::Uri;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer, de, de::DeserializeSeed, ser::SerializeMap,
};
use thiserror::Error;

use crate::{BoundedJson, BoundedJsonError, JsonLimit, JsonLimits, SchemaReference};

const REVERSE_DNS_PATTERN: &str =
    "^[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\\.[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?){2,}$";
const URI_PATTERN: &str = "^(https://|urn:)";

/// Namespace form used by an [`ExtensionKey`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ExtensionKeyKind {
    /// A normalized HTTPS or URN identifier.
    Uri,
    /// A lowercase reverse-DNS name with at least three labels.
    ReverseDns,
}

/// A stable namespace identity for one extension value.
///
/// URI keys are restricted to normalized HTTPS identifiers without userinfo,
/// query, or fragment components, or normalized `urn:` identifiers with a
/// lowercase RFC 8141 namespace identifier. URI keys are identifiers only and
/// are never dereferenced. Reverse-DNS keys use at least three lowercase DNS
/// labels, reserving the leading labels for the controlling organization and
/// the remaining labels for a versioned feature name.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExtensionKey {
    value: Box<str>,
    kind: ExtensionKeyKind,
}

impl ExtensionKey {
    /// Maximum encoded key length in bytes.
    pub const MAX_LEN: usize = 512;

    /// Parses and validates a namespaced extension key.
    ///
    /// # Errors
    ///
    /// Returns [`ExtensionKeyError`] when the key is empty, oversized,
    /// non-canonical, uses an unsupported URI form, or violates the strict
    /// reverse-DNS grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, ExtensionKeyError> {
        let value = value.into();
        validate_extension_key(&value).map(|kind| Self {
            value: value.into_boxed_str(),
            kind,
        })
    }

    /// Returns the exact canonical namespace text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Returns the validated namespace form.
    #[must_use]
    pub const fn kind(&self) -> ExtensionKeyKind {
        self.kind
    }
}

impl AsRef<str> for ExtensionKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for ExtensionKey {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for ExtensionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExtensionKey")
            .field("value", &self.as_str())
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for ExtensionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ExtensionKey {
    type Err = ExtensionKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for ExtensionKey {
    type Error = ExtensionKeyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ExtensionKey {
    type Error = ExtensionKeyError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ExtensionKey> for String {
    fn from(value: ExtensionKey) -> Self {
        value.value.into()
    }
}

impl Serialize for ExtensionKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ExtensionKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(ExtensionKeyVisitor)
    }
}

struct ExtensionKeyVisitor;

impl de::Visitor<'_> for ExtensionKeyVisitor {
    type Value = ExtensionKey;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a normalized HTTPS/URN or lowercase reverse-DNS extension key")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        ExtensionKey::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        ExtensionKey::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for ExtensionKey {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ExtensionKey".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ExtensionKey").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "oneOf": [
                {
                    "type": "string",
                    "format": "uri",
                    "minLength": 8,
                    "maxLength": 512,
                    "pattern": URI_PATTERN
                },
                {
                    "type": "string",
                    "minLength": 5,
                    "maxLength": 512,
                    "pattern": REVERSE_DNS_PATTERN
                }
            ],
            "description": "A normalized HTTPS/URN identifier or lowercase reverse-DNS extension name. Runtime validation enforces stricter URI and per-label invariants."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid extension namespace identity.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ExtensionKeyError {
    /// The key contained no bytes.
    #[error("extension key must not be empty")]
    Empty,

    /// The key exceeded [`ExtensionKey::MAX_LEN`].
    #[error("extension key is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// Text containing a URI separator was not a valid URI.
    #[error("extension URI key is invalid")]
    InvalidUri,

    /// The URI scheme was not one of the supported identifier-only schemes.
    #[error("extension URI key must use HTTPS or URN")]
    UnsupportedUriScheme,

    /// An HTTPS key did not contain a non-empty authority host.
    #[error("HTTPS extension key must contain an authority host")]
    MissingAuthority,

    /// URI user information was present.
    #[error("extension URI key must not contain user information")]
    UserInfoNotAllowed,

    /// A query component was present, including an empty query.
    #[error("extension URI key must not contain a query")]
    QueryNotAllowed,

    /// A fragment component was present, including an empty fragment.
    #[error("extension URI key must not contain a fragment")]
    FragmentNotAllowed,

    /// A URN did not contain a lowercase valid namespace and non-empty value.
    #[error("URN extension key must contain a lowercase namespace identifier and value")]
    InvalidUrn,

    /// The URI input did not already use its normalized spelling.
    #[error("extension URI key must use normalized RFC 3986 text")]
    NonCanonicalUri,

    /// A reverse-DNS key contained fewer than three labels.
    #[error("reverse-DNS extension key must contain at least three labels")]
    TooFewReverseDnsLabels,

    /// A reverse-DNS label was empty or exceeded 63 bytes.
    #[error("reverse-DNS label at index {label_index} has invalid length {actual}")]
    InvalidReverseDnsLabelLength {
        /// Zero-based label index.
        label_index: usize,
        /// Observed label byte length.
        actual: usize,
    },

    /// A label did not begin with lowercase ASCII.
    #[error("reverse-DNS label at index {label_index} must start with lowercase ASCII")]
    InvalidReverseDnsLabelStart {
        /// Zero-based label index.
        label_index: usize,
    },

    /// A label did not end in a lowercase letter or digit.
    #[error("reverse-DNS label at index {label_index} must end with a letter or digit")]
    InvalidReverseDnsLabelEnd {
        /// Zero-based label index.
        label_index: usize,
    },

    /// A reverse-DNS byte was outside the lowercase DNS-label grammar.
    #[error("reverse-DNS extension key contains an invalid byte at offset {index}")]
    InvalidReverseDnsByte {
        /// Zero-based byte offset in the full key.
        index: usize,
    },
}

fn validate_extension_key(value: &str) -> Result<ExtensionKeyKind, ExtensionKeyError> {
    if value.is_empty() {
        return Err(ExtensionKeyError::Empty);
    }
    if value.len() > ExtensionKey::MAX_LEN {
        return Err(ExtensionKeyError::TooLong {
            max: ExtensionKey::MAX_LEN,
            actual: value.len(),
        });
    }

    if value.contains(':') {
        validate_uri_extension_key(value)
    } else {
        validate_reverse_dns_extension_key(value)
    }
}

fn validate_uri_extension_key(value: &str) -> Result<ExtensionKeyKind, ExtensionKeyError> {
    let uri = Uri::parse(value).map_err(|_| ExtensionKeyError::InvalidUri)?;
    if uri.query().is_some() {
        return Err(ExtensionKeyError::QueryNotAllowed);
    }
    if uri.fragment().is_some() {
        return Err(ExtensionKeyError::FragmentNotAllowed);
    }

    let scheme = uri.scheme().as_str();
    if scheme.eq_ignore_ascii_case("https") {
        let authority = uri.authority().ok_or(ExtensionKeyError::MissingAuthority)?;
        if authority.host().is_empty() {
            return Err(ExtensionKeyError::MissingAuthority);
        }
        if authority.userinfo().is_some() {
            return Err(ExtensionKeyError::UserInfoNotAllowed);
        }
    } else if scheme.eq_ignore_ascii_case("urn") {
        if uri.authority().is_some() || !valid_urn_namespace_and_value(value) {
            return Err(ExtensionKeyError::InvalidUrn);
        }
    } else {
        return Err(ExtensionKeyError::UnsupportedUriScheme);
    }

    if uri.normalize().as_str() != value {
        return Err(ExtensionKeyError::NonCanonicalUri);
    }
    Ok(ExtensionKeyKind::Uri)
}

fn valid_urn_namespace_and_value(value: &str) -> bool {
    let Some(body) = value.get(4..) else {
        return false;
    };
    let Some((namespace, identifier)) = body.split_once(':') else {
        return false;
    };
    if !(2..=32).contains(&namespace.len()) || identifier.is_empty() {
        return false;
    }

    let bytes = namespace.as_bytes();
    bytes[0].is_ascii_lowercase()
        && bytes[bytes.len() - 1].is_ascii_alphanumeric()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

fn validate_reverse_dns_extension_key(value: &str) -> Result<ExtensionKeyKind, ExtensionKeyError> {
    let label_count = value.bytes().filter(|byte| *byte == b'.').count() + 1;
    if label_count < 3 {
        return Err(ExtensionKeyError::TooFewReverseDnsLabels);
    }

    let mut offset = 0;
    for (label_index, label) in value.split('.').enumerate() {
        if label.is_empty() || label.len() > 63 {
            return Err(ExtensionKeyError::InvalidReverseDnsLabelLength {
                label_index,
                actual: label.len(),
            });
        }
        let bytes = label.as_bytes();
        if !bytes[0].is_ascii_lowercase() {
            return Err(ExtensionKeyError::InvalidReverseDnsLabelStart { label_index });
        }
        if !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
            return Err(ExtensionKeyError::InvalidReverseDnsLabelEnd { label_index });
        }
        if let Some((inner_index, _)) = bytes.iter().enumerate().find(|(_, byte)| {
            !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && **byte != b'-'
        }) {
            return Err(ExtensionKeyError::InvalidReverseDnsByte {
                index: offset + inner_index,
            });
        }
        offset += label.len() + 1;
    }
    Ok(ExtensionKeyKind::ReverseDns)
}

/// Explicit trust mode for one extension value.
///
/// Opaque values may be retained or forwarded only according to an adapter's
/// negotiated profile. They cannot influence authorization, policy,
/// deterministic execution, hashing, or capability selection. Schema-bound
/// values carry an immutable schema identity but still require validation
/// against a trusted local registry before any semantic use.
#[derive(Clone, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExtensionValue {
    /// Bounded data with no registered semantic contract.
    Opaque {
        /// The preserved bounded value.
        value: BoundedJson,
    },
    /// Data whose intended semantics are pinned to an immutable schema.
    SchemaBound {
        /// Immutable identity of the value's schema.
        schema: SchemaReference,
        /// The bounded value to validate against the local schema registry.
        value: BoundedJson,
    },
}

impl ExtensionValue {
    /// Wraps bounded data without granting it registered semantics.
    #[must_use]
    pub const fn opaque(value: BoundedJson) -> Self {
        Self::Opaque { value }
    }

    /// Binds bounded data to an immutable schema identity.
    #[must_use]
    pub const fn schema_bound(schema: SchemaReference, value: BoundedJson) -> Self {
        Self::SchemaBound { schema, value }
    }

    /// Returns the bounded JSON value in either trust mode.
    #[must_use]
    pub const fn value(&self) -> &BoundedJson {
        match self {
            Self::Opaque { value } | Self::SchemaBound { value, .. } => value,
        }
    }

    /// Returns the immutable schema identity when semantic use was declared.
    #[must_use]
    pub const fn schema(&self) -> Option<&SchemaReference> {
        match self {
            Self::Opaque { .. } => None,
            Self::SchemaBound { schema, .. } => Some(schema),
        }
    }

    /// Returns whether this value must remain semantically opaque.
    #[must_use]
    pub const fn is_opaque(&self) -> bool {
        matches!(self, Self::Opaque { .. })
    }

    /// Consumes the wrapper into its optional schema and bounded value.
    #[must_use]
    pub fn into_parts(self) -> (Option<SchemaReference>, BoundedJson) {
        match self {
            Self::Opaque { value } => (None, value),
            Self::SchemaBound { schema, value } => (Some(schema), value),
        }
    }

    fn try_restrict(self, limits: JsonLimits) -> Result<Self, BoundedJsonError> {
        match self {
            Self::Opaque { value } => {
                BoundedJson::try_from_value_with_limits(value.into_value(), limits)
                    .map(Self::opaque)
            }
            Self::SchemaBound { schema, value } => {
                BoundedJson::try_from_value_with_limits(value.into_value(), limits)
                    .map(|value| Self::schema_bound(schema, value))
            }
        }
    }
}

impl fmt::Debug for ExtensionValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Opaque { value } => formatter
                .debug_struct("ExtensionValue::Opaque")
                .field("value", value)
                .finish_non_exhaustive(),
            Self::SchemaBound { schema, value } => formatter
                .debug_struct("ExtensionValue::SchemaBound")
                .field("schema", schema)
                .field("value", value)
                .finish_non_exhaustive(),
        }
    }
}

/// Configurable dimension of the bounded extension-map contract.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ExtensionLimit {
    /// Number of top-level extension entries.
    Entries,
    /// Compact encoded bytes for the complete map.
    TotalBytes,
    /// Encoded bytes in one namespace key.
    KeyBytes,
    /// One of the nested bounded JSON dimensions.
    ValueJson(JsonLimit),
}

impl fmt::Display for ExtensionLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entries => formatter.write_str("extension entries"),
            Self::TotalBytes => formatter.write_str("extension total bytes"),
            Self::KeyBytes => formatter.write_str("extension key bytes"),
            Self::ValueJson(limit) => write!(formatter, "extension value {limit}"),
        }
    }
}

/// Validated limits that may only narrow `StateKnot`'s hard extension ceiling.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ExtensionLimits {
    max_entries: usize,
    max_total_bytes: usize,
    max_key_bytes: usize,
    value_json_limits: JsonLimits,
}

impl ExtensionLimits {
    /// Absolute v1 ceiling used by generic deserialization.
    pub const HARD_MAXIMUM: Self = Self {
        max_entries: 64,
        max_total_bytes: 256 * 1024,
        max_key_bytes: ExtensionKey::MAX_LEN,
        value_json_limits: JsonLimits::DEFAULT,
    };

    /// Constructs a valid profile no wider than [`Self::HARD_MAXIMUM`].
    ///
    /// # Errors
    ///
    /// Returns [`ExtensionLimitsError`] when a scalar is zero or below its
    /// meaningful minimum, a dimension exceeds the hard maximum, or nested
    /// JSON limits are wider than the hard extension profile.
    pub fn try_new(
        max_entries: usize,
        max_total_bytes: usize,
        max_key_bytes: usize,
        value_json_limits: JsonLimits,
    ) -> Result<Self, ExtensionLimitsError> {
        validate_extension_limit(
            ExtensionLimit::Entries,
            max_entries,
            1,
            Self::HARD_MAXIMUM.max_entries,
        )?;
        validate_extension_limit(
            ExtensionLimit::TotalBytes,
            max_total_bytes,
            2,
            Self::HARD_MAXIMUM.max_total_bytes,
        )?;
        validate_extension_limit(
            ExtensionLimit::KeyBytes,
            max_key_bytes,
            1,
            Self::HARD_MAXIMUM.max_key_bytes,
        )?;
        validate_value_json_limits(value_json_limits)?;

        Ok(Self {
            max_entries,
            max_total_bytes,
            max_key_bytes,
            value_json_limits,
        })
    }

    /// Returns the top-level entry ceiling.
    #[must_use]
    pub const fn max_entries(self) -> usize {
        self.max_entries
    }

    /// Returns the compact complete-map byte ceiling.
    #[must_use]
    pub const fn max_total_bytes(self) -> usize {
        self.max_total_bytes
    }

    /// Returns the per-key encoded byte ceiling.
    #[must_use]
    pub const fn max_key_bytes(self) -> usize {
        self.max_key_bytes
    }

    /// Returns limits applied to every nested extension value.
    #[must_use]
    pub const fn value_json_limits(self) -> JsonLimits {
        self.value_json_limits
    }
}

impl Default for ExtensionLimits {
    fn default() -> Self {
        Self::HARD_MAXIMUM
    }
}

fn validate_extension_limit(
    limit: ExtensionLimit,
    actual: usize,
    minimum: usize,
    maximum: usize,
) -> Result<(), ExtensionLimitsError> {
    if actual == 0 {
        return Err(ExtensionLimitsError::Zero { limit });
    }
    if actual < minimum {
        return Err(ExtensionLimitsError::BelowMinimum {
            limit,
            minimum,
            actual,
        });
    }
    if actual > maximum {
        return Err(ExtensionLimitsError::AboveHardMaximum {
            limit,
            maximum,
            actual,
        });
    }
    Ok(())
}

fn validate_value_json_limits(limits: JsonLimits) -> Result<(), ExtensionLimitsError> {
    let maximum = JsonLimits::DEFAULT;
    for (limit, actual, maximum) in [
        (JsonLimit::Bytes, limits.max_bytes(), maximum.max_bytes()),
        (JsonLimit::Depth, limits.max_depth(), maximum.max_depth()),
        (
            JsonLimit::ContainerEntries,
            limits.max_container_entries(),
            maximum.max_container_entries(),
        ),
        (JsonLimit::Nodes, limits.max_nodes(), maximum.max_nodes()),
        (
            JsonLimit::StringBytes,
            limits.max_string_bytes(),
            maximum.max_string_bytes(),
        ),
        (
            JsonLimit::ObjectKeyBytes,
            limits.max_object_key_bytes(),
            maximum.max_object_key_bytes(),
        ),
    ] {
        if actual > maximum {
            return Err(ExtensionLimitsError::AboveHardMaximum {
                limit: ExtensionLimit::ValueJson(limit),
                maximum,
                actual,
            });
        }
    }
    Ok(())
}

/// Invalid configurable extension-map limits.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ExtensionLimitsError {
    /// A scalar limit was configured as zero.
    #[error("{limit} limit must be greater than zero")]
    Zero {
        /// The invalid dimension.
        limit: ExtensionLimit,
    },

    /// A non-zero scalar could not represent the smallest valid value.
    #[error("configured {limit} limit is {actual}; minimum is {minimum}")]
    BelowMinimum {
        /// The invalid dimension.
        limit: ExtensionLimit,
        /// Smallest meaningful value for that dimension.
        minimum: usize,
        /// Configured value.
        actual: usize,
    },

    /// A configured dimension exceeded the immutable v1 hard ceiling.
    #[error("configured {limit} limit is {actual}; hard maximum is {maximum}")]
    AboveHardMaximum {
        /// The invalid dimension.
        limit: ExtensionLimit,
        /// `StateKnot`'s hard ceiling.
        maximum: usize,
        /// Configured value.
        actual: usize,
    },
}

/// A sorted, unique, resource-bounded extension map.
///
/// Deserializing an extension never grants authority or activates behavior.
/// Adapters negotiate and register supported keys separately, schema-validate
/// every semantically used value, and either reject or retain unknown opaque
/// values according to their protocol profile. Serialization order is the
/// exact byte order of canonical key text.
#[derive(Clone, Eq, PartialEq)]
pub struct Extensions {
    entries: BTreeMap<ExtensionKey, ExtensionValue>,
    compact_bytes: usize,
}

impl Extensions {
    /// Constructs a map under the immutable v1 hard ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`ExtensionsError`] for duplicate keys or a count, key, nested
    /// JSON, compact-size, or internal encoding violation.
    pub fn try_new<I>(entries: I) -> Result<Self, ExtensionsError>
    where
        I: IntoIterator<Item = (ExtensionKey, ExtensionValue)>,
    {
        Self::try_new_with_limits(entries, ExtensionLimits::HARD_MAXIMUM)
    }

    /// Constructs a map under an explicitly narrowed limit profile.
    ///
    /// # Errors
    ///
    /// Returns [`ExtensionsError`] for duplicate keys or a count, key, nested
    /// JSON, compact-size, or internal encoding violation.
    pub fn try_new_with_limits<I>(
        entries: I,
        limits: ExtensionLimits,
    ) -> Result<Self, ExtensionsError>
    where
        I: IntoIterator<Item = (ExtensionKey, ExtensionValue)>,
    {
        let mut map = BTreeMap::new();
        let mut compact_bytes = 2;
        for (key, value) in entries {
            insert_extension(&mut map, &mut compact_bytes, key, value, limits)?;
        }
        Ok(Self {
            entries: map,
            compact_bytes,
        })
    }

    /// Revalidates an existing map under a profile that may be narrower.
    ///
    /// # Errors
    ///
    /// Returns [`ExtensionsError`] when existing entries exceed the profile.
    pub fn try_restrict(self, limits: ExtensionLimits) -> Result<Self, ExtensionsError> {
        Self::try_new_with_limits(self.entries, limits)
    }

    /// Returns the number of extension entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the map contains no extensions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the exact compact JSON byte length of the complete map.
    #[must_use]
    pub const fn compact_bytes(&self) -> usize {
        self.compact_bytes
    }

    /// Looks up one canonical namespace key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&ExtensionValue> {
        self.entries.get(key)
    }

    /// Returns entries in canonical key-byte order.
    pub fn iter(&self) -> btree_map::Iter<'_, ExtensionKey, ExtensionValue> {
        self.entries.iter()
    }

    /// Consumes the map into entries in canonical key-byte order.
    #[must_use]
    pub fn into_entries(self) -> Vec<(ExtensionKey, ExtensionValue)> {
        self.entries.into_iter().collect()
    }
}

impl Default for Extensions {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            compact_bytes: 2,
        }
    }
}

impl fmt::Debug for Extensions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Extensions")
            .field("entries", &self.len())
            .field("compact_bytes", &self.compact_bytes)
            .finish_non_exhaustive()
    }
}

impl<'a> IntoIterator for &'a Extensions {
    type Item = (&'a ExtensionKey, &'a ExtensionValue);
    type IntoIter = btree_map::Iter<'a, ExtensionKey, ExtensionValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<Vec<(ExtensionKey, ExtensionValue)>> for Extensions {
    type Error = ExtensionsError;

    fn try_from(entries: Vec<(ExtensionKey, ExtensionValue)>) -> Result<Self, Self::Error> {
        Self::try_new(entries)
    }
}

fn insert_extension(
    entries: &mut BTreeMap<ExtensionKey, ExtensionValue>,
    compact_bytes: &mut usize,
    key: ExtensionKey,
    value: ExtensionValue,
    limits: ExtensionLimits,
) -> Result<(), ExtensionsError> {
    if key.as_str().len() > limits.max_key_bytes {
        return Err(ExtensionsError::KeyTooLong {
            max: limits.max_key_bytes,
            actual: key.as_str().len(),
        });
    }
    if entries.contains_key(&key) {
        return Err(ExtensionsError::DuplicateKey);
    }
    if entries.len() == limits.max_entries {
        return Err(ExtensionsError::TooManyEntries {
            max: limits.max_entries,
            observed: limits.max_entries + 1,
        });
    }

    let value = value
        .try_restrict(limits.value_json_limits)
        .map_err(ExtensionsError::value)?;
    let value_bytes = serde_json::to_vec(&value)
        .map_err(|_| ExtensionsError::InternalEncoding)?
        .len();
    let separator_bytes = usize::from(!entries.is_empty());
    let next_bytes = compact_bytes
        .saturating_add(separator_bytes)
        .saturating_add(key.as_str().len())
        .saturating_add(3)
        .saturating_add(value_bytes);
    if next_bytes > limits.max_total_bytes {
        return Err(ExtensionsError::TotalBytesExceeded {
            max: limits.max_total_bytes,
            actual: next_bytes,
        });
    }

    entries.insert(key, value);
    *compact_bytes = next_bytes;
    Ok(())
}

impl Serialize for Extensions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.entries.len()))?;
        for (key, value) in &self.entries {
            map.serialize_entry(key.as_str(), value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Extensions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(ExtensionsVisitor)
    }
}

struct ExtensionsVisitor;

impl<'de> de::Visitor<'de> for ExtensionsVisitor {
    type Value = Extensions;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a namespaced extension object containing at most {} entries",
            ExtensionLimits::HARD_MAXIMUM.max_entries
        )
    }

    fn visit_map<A>(self, mut source: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let limits = ExtensionLimits::HARD_MAXIMUM;
        let mut entries = BTreeMap::new();
        let mut compact_bytes = 2;
        while entries.len() < limits.max_entries {
            let Some(key) = source.next_key::<ExtensionKey>()? else {
                return Ok(Extensions {
                    entries,
                    compact_bytes,
                });
            };
            if entries.contains_key(&key) {
                return Err(de::Error::custom(ExtensionsError::DuplicateKey));
            }
            let value = source.next_value::<ExtensionValue>()?;
            insert_extension(&mut entries, &mut compact_bytes, key, value, limits)
                .map_err(de::Error::custom)?;
        }

        let _: Option<()> = source.next_key_seed(RejectExtraExtensionSeed {
            maximum: limits.max_entries,
        })?;
        Ok(Extensions {
            entries,
            compact_bytes,
        })
    }
}

struct RejectExtraExtensionSeed {
    maximum: usize,
}

impl<'de> DeserializeSeed<'de> for RejectExtraExtensionSeed {
    type Value = ();

    fn deserialize<D>(self, _deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Err(de::Error::custom(ExtensionsError::TooManyEntries {
            max: self.maximum,
            observed: self.maximum.saturating_add(1),
        }))
    }
}

impl JsonSchema for Extensions {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Extensions".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Extensions").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "object",
            "minProperties": 0,
            "maxProperties": 64,
            "propertyNames": generator.subschema_for::<ExtensionKey>(),
            "additionalProperties": generator.subschema_for::<ExtensionValue>(),
            "description": "A deterministically ordered extension map. StateKnot additionally enforces exact 262144-byte compact-map and nested bounded-JSON limits at runtime; deserialization alone never activates extension semantics."
        })
    }
}

/// Invalid bounded extension-map data.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ExtensionsError {
    /// More entries were supplied than the active profile permits.
    #[error("extension map contains at least {observed} entries; maximum is {max}")]
    TooManyEntries {
        /// Configured maximum.
        max: usize,
        /// First observed count beyond the maximum.
        observed: usize,
    },

    /// The same canonical namespace key appeared more than once.
    #[error("extension map contains a duplicate namespace key")]
    DuplicateKey,

    /// A valid hard-bound key exceeded a narrowed profile.
    #[error("extension key is {actual} bytes; active profile maximum is {max}")]
    KeyTooLong {
        /// Configured maximum.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// One nested extension value violated the active bounded JSON profile.
    #[error("extension value violates JSON safety limits: {source}")]
    Value {
        /// Underlying bounded JSON violation.
        #[source]
        source: BoundedJsonError,
    },

    /// The exact compact map representation exceeded the active profile.
    #[error("extension map reached {actual} compact bytes; maximum is {max}")]
    TotalBytesExceeded {
        /// Configured maximum.
        max: usize,
        /// First observed compact length beyond the maximum.
        actual: usize,
    },

    /// Serialization of already validated internal components unexpectedly failed.
    #[error("validated extension value could not be compactly encoded")]
    InternalEncoding,
}

impl ExtensionsError {
    const fn value(source: BoundedJsonError) -> Self {
        Self::Value { source }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;
    use proptest::{collection, prelude::*};
    use serde_json::{Value, from_value, json, to_value};

    use crate::{Digest, SchemaId, Version};

    fn bounded(value: Value) -> BoundedJson {
        BoundedJson::try_from_value(value).unwrap()
    }

    fn schema_reference() -> SchemaReference {
        SchemaReference::new(
            "https://stateknot.github.io/schema/extension/citations/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"canonical extension schema"),
        )
    }

    fn opaque(value: Value) -> ExtensionValue {
        ExtensionValue::opaque(bounded(value))
    }

    fn limits(
        max_entries: usize,
        max_total_bytes: usize,
        max_key_bytes: usize,
        value_json_limits: JsonLimits,
    ) -> ExtensionLimits {
        ExtensionLimits::try_new(
            max_entries,
            max_total_bytes,
            max_key_bytes,
            value_json_limits,
        )
        .unwrap()
    }

    #[test]
    fn keys_accept_canonical_uri_and_reverse_dns_namespaces() {
        for (value, kind) in [
            (
                "https://example.com/extensions/citations/v1",
                ExtensionKeyKind::Uri,
            ),
            (
                "https://example.com:8443/extensions/a%20b/v1",
                ExtensionKeyKind::Uri,
            ),
            ("urn:example:extension:citations:v1", ExtensionKeyKind::Uri),
            ("urn:ietf:rfc:8141", ExtensionKeyKind::Uri),
            ("io.stateknot.feature", ExtensionKeyKind::ReverseDns),
            ("com.example.citations-v1", ExtensionKeyKind::ReverseDns),
        ] {
            let key = ExtensionKey::new(value).unwrap();
            assert_eq!(key.as_str(), value);
            assert_eq!(key.kind(), kind);
            assert_eq!(key.to_string(), value);
            assert_eq!(to_value(&key).unwrap(), Value::from(value));
            assert_eq!(from_value::<ExtensionKey>(json!(value)).unwrap(), key);
            assert_eq!(String::from(key), value);
        }
    }

    #[test]
    fn keys_reject_ambiguous_active_or_non_canonical_namespaces() {
        for value in [
            "example.feature",
            "com..feature",
            "com.example.",
            "1com.example.feature",
            "com.Example.feature",
            "com.example.-feature",
            "com.example.feature-",
            "com.example.feat_ure",
            "com.example.feature/name",
            "com.example.例",
            "http://example.com/extension/v1",
            "ftp://example.com/extension/v1",
            "did:example:extension",
            "HTTPS://example.com/extension/v1",
            "https://EXAMPLE.com/extension/v1",
            "https://example.com:443/extension/v1",
            "https://example.com/a/../extension/v1",
            "https://example.com/extension/%7eowner",
            "https://user@example.com/extension/v1",
            "https://example.com/extension/v1?profile=full",
            "https://example.com/extension/v1#metadata",
            "https:/example.com/extension/v1",
            "urn:a:value",
            "urn:Example:value",
            "urn:example:",
            "urn:example",
            "URN:example:value",
            "urn:example:value?profile=full",
            "urn:example:value#metadata",
        ] {
            assert!(
                ExtensionKey::new(value).is_err(),
                "accepted extension key {value:?}"
            );
        }

        assert_eq!(ExtensionKey::new(""), Err(ExtensionKeyError::Empty));
        let oversized = "a".repeat(ExtensionKey::MAX_LEN + 1);
        assert_eq!(
            ExtensionKey::new(&oversized),
            Err(ExtensionKeyError::TooLong {
                max: ExtensionKey::MAX_LEN,
                actual: oversized.len(),
            })
        );
        assert!(from_value::<ExtensionKey>(json!(42)).is_err());
        assert!(from_value::<ExtensionKey>(Value::Null).is_err());
    }

    #[test]
    fn key_schema_documents_both_namespace_forms() {
        let schema = to_value(schemars::schema_for!(ExtensionKey)).unwrap();
        let alternatives = schema["oneOf"].as_array().unwrap();
        assert_eq!(alternatives.len(), 2);
        assert_eq!(alternatives[0]["format"], "uri");
        assert_eq!(alternatives[0]["maxLength"], ExtensionKey::MAX_LEN);
        assert_eq!(alternatives[0]["pattern"], URI_PATTERN);
        assert_eq!(alternatives[1]["pattern"], REVERSE_DNS_PATTERN);
    }

    #[test]
    fn extension_values_have_closed_explicit_trust_modes() {
        let secret = "credential-like-secret";
        let opaque = opaque(json!({ "secret": secret }));
        assert!(opaque.is_opaque());
        assert!(opaque.schema().is_none());
        assert_eq!(opaque.value().as_value()["secret"], secret);
        assert!(!format!("{opaque:?}").contains(secret));
        assert_eq!(
            to_value(&opaque).unwrap(),
            json!({
                "kind": "opaque",
                "value": { "secret": secret }
            })
        );

        let reference = schema_reference();
        let bound = ExtensionValue::schema_bound(reference.clone(), bounded(json!([1, 2, 3])));
        assert!(!bound.is_opaque());
        assert_eq!(bound.schema(), Some(&reference));
        let encoded = to_value(&bound).unwrap();
        assert_eq!(
            from_value::<ExtensionValue>(encoded.clone()).unwrap(),
            bound
        );
        assert_eq!(encoded["kind"], "schema_bound");
        assert_eq!(encoded["schema"], to_value(reference.clone()).unwrap());

        let (schema, value) = bound.into_parts();
        assert_eq!(schema, Some(reference));
        assert_eq!(value.as_value(), &json!([1, 2, 3]));

        for invalid in [
            json!({ "kind": "unknown", "value": null }),
            json!({ "kind": "opaque" }),
            json!({ "kind": "opaque", "value": null, "schema": null }),
            json!({ "kind": "opaque", "value": null, "extra": true }),
            json!({ "kind": "schema_bound", "value": null }),
            json!({ "kind": "schema_bound", "schema": null, "value": null }),
            Value::Null,
            json!([]),
        ] {
            assert!(
                from_value::<ExtensionValue>(invalid.clone()).is_err(),
                "accepted extension value {invalid}"
            );
        }
    }

    #[test]
    fn extension_limit_profiles_have_stable_defaults_and_minimums() {
        let hard = ExtensionLimits::HARD_MAXIMUM;
        assert_eq!(ExtensionLimits::default(), hard);
        assert_eq!(hard.max_entries(), 64);
        assert_eq!(hard.max_total_bytes(), 256 * 1024);
        assert_eq!(hard.max_key_bytes(), ExtensionKey::MAX_LEN);
        assert_eq!(hard.value_json_limits(), JsonLimits::DEFAULT);

        for (limit, result) in [
            (
                ExtensionLimit::Entries,
                ExtensionLimits::try_new(
                    0,
                    hard.max_total_bytes(),
                    hard.max_key_bytes(),
                    JsonLimits::DEFAULT,
                ),
            ),
            (
                ExtensionLimit::TotalBytes,
                ExtensionLimits::try_new(
                    hard.max_entries(),
                    0,
                    hard.max_key_bytes(),
                    JsonLimits::DEFAULT,
                ),
            ),
            (
                ExtensionLimit::KeyBytes,
                ExtensionLimits::try_new(
                    hard.max_entries(),
                    hard.max_total_bytes(),
                    0,
                    JsonLimits::DEFAULT,
                ),
            ),
        ] {
            assert_eq!(result, Err(ExtensionLimitsError::Zero { limit }));
        }

        assert_eq!(
            ExtensionLimits::try_new(1, 1, 1, JsonLimits::DEFAULT),
            Err(ExtensionLimitsError::BelowMinimum {
                limit: ExtensionLimit::TotalBytes,
                minimum: 2,
                actual: 1,
            })
        );
    }

    #[test]
    fn extension_limit_profiles_cannot_exceed_hard_ceilings() {
        let hard = ExtensionLimits::HARD_MAXIMUM;
        for (limit, result, maximum) in [
            (
                ExtensionLimit::Entries,
                ExtensionLimits::try_new(
                    hard.max_entries() + 1,
                    hard.max_total_bytes(),
                    hard.max_key_bytes(),
                    JsonLimits::DEFAULT,
                ),
                hard.max_entries(),
            ),
            (
                ExtensionLimit::TotalBytes,
                ExtensionLimits::try_new(
                    hard.max_entries(),
                    hard.max_total_bytes() + 1,
                    hard.max_key_bytes(),
                    JsonLimits::DEFAULT,
                ),
                hard.max_total_bytes(),
            ),
            (
                ExtensionLimit::KeyBytes,
                ExtensionLimits::try_new(
                    hard.max_entries(),
                    hard.max_total_bytes(),
                    hard.max_key_bytes() + 1,
                    JsonLimits::DEFAULT,
                ),
                hard.max_key_bytes(),
            ),
        ] {
            assert_eq!(
                result,
                Err(ExtensionLimitsError::AboveHardMaximum {
                    limit,
                    maximum,
                    actual: maximum + 1,
                })
            );
        }

        let wider_json = JsonLimits::try_new(
            JsonLimits::DEFAULT.max_bytes() + 1,
            JsonLimits::DEFAULT.max_depth(),
            JsonLimits::DEFAULT.max_container_entries(),
            JsonLimits::DEFAULT.max_nodes(),
            JsonLimits::DEFAULT.max_string_bytes(),
            JsonLimits::DEFAULT.max_object_key_bytes(),
        )
        .unwrap();
        assert_eq!(
            ExtensionLimits::try_new(1, 2, 1, wider_json),
            Err(ExtensionLimitsError::AboveHardMaximum {
                limit: ExtensionLimit::ValueJson(JsonLimit::Bytes),
                maximum: JsonLimits::DEFAULT.max_bytes(),
                actual: JsonLimits::DEFAULT.max_bytes() + 1,
            })
        );
    }

    #[test]
    fn maps_are_sorted_unique_and_track_exact_compact_bytes() {
        let empty = Extensions::try_new(Vec::new()).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty, Extensions::default());
        assert_eq!(empty.compact_bytes(), 2);
        assert_eq!(serde_json::to_string(&empty).unwrap(), "{}");

        let secret = "opaque-secret";
        let entries = vec![
            (
                ExtensionKey::new("urn:example:zeta:v1").unwrap(),
                opaque(json!({ "secret": secret })),
            ),
            (
                ExtensionKey::new("com.example.alpha").unwrap(),
                ExtensionValue::schema_bound(
                    schema_reference(),
                    bounded(json!({ "enabled": true })),
                ),
            ),
        ];
        let extensions = Extensions::try_new(entries).unwrap();
        let encoded = serde_json::to_vec(&extensions).unwrap();

        assert_eq!(extensions.len(), 2);
        assert_eq!(extensions.compact_bytes(), encoded.len());
        assert!(extensions.get("com.example.alpha").is_some());
        assert!(extensions.get("com.example.missing").is_none());
        assert_eq!(
            extensions
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            ["com.example.alpha", "urn:example:zeta:v1"]
        );
        assert_eq!(
            serde_json::from_slice::<Extensions>(&encoded).unwrap(),
            extensions
        );

        let debug = format!("{extensions:?}");
        assert!(debug.contains("entries: 2"));
        assert!(!debug.contains(secret));
        assert!(!debug.contains("com.example.alpha"));

        let entries = extensions.clone().into_entries();
        assert_eq!(entries[0].0.as_str(), "com.example.alpha");
        assert_eq!(Extensions::try_from(entries).unwrap(), extensions);
    }

    #[test]
    fn map_construction_rejects_duplicates_before_capacity_errors() {
        let key = ExtensionKey::new("com.example.duplicate").unwrap();
        assert_eq!(
            Extensions::try_new(vec![
                (key.clone(), opaque(json!(1))),
                (key, opaque(json!(2))),
            ]),
            Err(ExtensionsError::DuplicateKey)
        );

        let duplicate_at_capacity = limits(1, 1024, 128, JsonLimits::DEFAULT);
        let key = ExtensionKey::new("com.example.duplicate").unwrap();
        assert_eq!(
            Extensions::try_new_with_limits(
                vec![(key.clone(), opaque(json!(1))), (key, opaque(json!(2)))],
                duplicate_at_capacity,
            ),
            Err(ExtensionsError::DuplicateKey)
        );
    }

    #[test]
    fn narrowed_profiles_enforce_every_map_dimension() {
        let first = (
            ExtensionKey::new("com.example.alpha").unwrap(),
            opaque(json!("one")),
        );
        let second = (
            ExtensionKey::new("com.example.beta").unwrap(),
            opaque(json!("two")),
        );

        assert_eq!(
            Extensions::try_new_with_limits(
                vec![first.clone(), second.clone()],
                limits(1, 1024, 128, JsonLimits::DEFAULT),
            ),
            Err(ExtensionsError::TooManyEntries {
                max: 1,
                observed: 2,
            })
        );
        assert_eq!(
            Extensions::try_new_with_limits(
                vec![first.clone()],
                limits(1, 1024, 8, JsonLimits::DEFAULT),
            ),
            Err(ExtensionsError::KeyTooLong {
                max: 8,
                actual: first.0.as_str().len(),
            })
        );

        let narrow_json = JsonLimits::try_new(128, 4, 8, 8, 3, 16).unwrap();
        assert_eq!(
            Extensions::try_new_with_limits(
                vec![(
                    ExtensionKey::new("com.example.alpha").unwrap(),
                    opaque(json!("four")),
                )],
                limits(1, 1024, 128, narrow_json),
            ),
            Err(ExtensionsError::Value {
                source: BoundedJsonError::StringTooLong {
                    maximum: 3,
                    actual: 4,
                },
            })
        );

        let complete = Extensions::try_new(vec![first]).unwrap();
        let exact = complete.compact_bytes();
        assert_eq!(
            complete
                .clone()
                .try_restrict(limits(1, exact - 1, 128, JsonLimits::DEFAULT,)),
            Err(ExtensionsError::TotalBytesExceeded {
                max: exact - 1,
                actual: exact,
            })
        );
        let restricted = complete
            .clone()
            .try_restrict(limits(1, exact, 128, JsonLimits::DEFAULT))
            .unwrap();
        assert_eq!(restricted, complete);
        assert_eq!(restricted.compact_bytes(), exact);
    }

    #[test]
    fn wire_duplicates_and_entry_overflow_stop_before_values_are_traversed() {
        let deep_value = format!("{}0{}", "[".repeat(200), "]".repeat(200));
        let duplicate = format!(
            r#"{{"com.example.key":{{"kind":"opaque","value":null}},"com.example.key":{deep_value}}}"#
        );
        let error = serde_json::from_str::<Extensions>(&duplicate).unwrap_err();
        assert!(error.to_string().contains("duplicate namespace key"));

        let mut overflow = String::from("{");
        for index in 0..ExtensionLimits::HARD_MAXIMUM.max_entries() {
            if index != 0 {
                overflow.push(',');
            }
            write!(
                overflow,
                r#""com.example.key{index}":{{"kind":"opaque","value":null}}"#
            )
            .unwrap();
        }
        let oversized_extra_key = "x".repeat(10_000);
        write!(overflow, r#","{oversized_extra_key}":{deep_value}}}"#).unwrap();
        let error = serde_json::from_str::<Extensions>(&overflow).unwrap_err();
        assert!(error.to_string().contains("at least 65 entries"));

        let nested_duplicate = r#"{
            "com.example.key": {
                "kind": "opaque",
                "value": {"decoded": 1, "\u0064ecoded": 2}
            }
        }"#;
        assert!(serde_json::from_str::<Extensions>(nested_duplicate).is_err());
    }

    #[test]
    fn map_schema_exposes_closed_values_and_hard_entry_ceiling() {
        let schema = to_value(schemars::schema_for!(Extensions)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["minProperties"], 0);
        assert_eq!(
            schema["maxProperties"],
            ExtensionLimits::HARD_MAXIMUM.max_entries()
        );
        assert!(schema.get("propertyNames").is_some());
        assert!(schema.get("additionalProperties").is_some());

        let value_schema = to_value(schemars::schema_for!(ExtensionValue)).unwrap();
        let variants = value_schema["oneOf"].as_array().unwrap();
        assert_eq!(variants.len(), 2);
        assert!(
            variants
                .iter()
                .all(|variant| variant["additionalProperties"] == false)
        );
    }

    proptest! {
        #[test]
        fn insertion_order_cannot_change_wire_bytes_or_accounting(
            values in collection::btree_map("[a-z][a-z0-9]{0,10}", any::<i64>(), 0..20)
        ) {
            let forward = values
                .iter()
                .map(|(name, value)| {
                    (
                        ExtensionKey::new(format!("com.example.{name}")).unwrap(),
                        opaque(json!(value)),
                    )
                })
                .collect::<Vec<_>>();
            let reverse = forward.iter().cloned().rev().collect::<Vec<_>>();
            let forward = Extensions::try_new(forward).unwrap();
            let reverse = Extensions::try_new(reverse).unwrap();
            let encoded = serde_json::to_vec(&forward).unwrap();

            prop_assert_eq!(&forward, &reverse);
            prop_assert_eq!(serde_json::to_vec(&reverse).unwrap(), encoded.clone());
            prop_assert_eq!(forward.compact_bytes(), encoded.len());
        }
    }
}
