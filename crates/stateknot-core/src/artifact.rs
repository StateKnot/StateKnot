// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Tenant-scoped, integrity-bound artifact references and content parts.

use std::{borrow::Borrow, collections::BTreeMap, fmt, str::FromStr};

use mime::Mime;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{
    ArtifactId, ByteCount, CapabilityReference, ContentMetadata, ContentSource, Digest, EventId,
    JsonContent, PrincipalIdentity, RunId, SchemaReference, TenantId, TextContent,
};

const MEDIA_TYPE_PATTERN: &str =
    "^[a-z0-9][a-z0-9!#$&.^_+-]{0,126}/[a-z0-9][a-z0-9!#$&.^_+-]{0,126}(?:;.*)?$";
const RETENTION_CLASS_PATTERN: &str = "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,63}$";

/// A concrete, canonical media type for stored artifact bytes.
///
/// Type, subtype, and parameter names are lowercase; parameters are unique and
/// sorted by name; `charset` values are lowercase; other parameter values
/// retain their case. Wildcard media ranges are rejected. `StateKnot` validates
/// syntax offline and does not claim that a type is currently registered with
/// IANA. MIME sniffing and authorization based only on this declared value are
/// forbidden at the artifact boundary.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MediaType(Box<str>);

impl MediaType {
    /// Maximum canonical encoded length in bytes.
    pub const MAX_LEN: usize = 512;

    /// Maximum number of media-type parameters.
    pub const MAX_PARAMETERS: usize = 16;

    /// Maximum decoded length of one parameter value in bytes.
    pub const MAX_PARAMETER_VALUE_LEN: usize = 128;

    /// Parses and deterministically normalizes a concrete media type.
    ///
    /// # Errors
    ///
    /// Returns [`MediaTypeError`] for invalid, wildcard, ambiguous, or
    /// resource-unbounded media type text.
    pub fn new(value: impl AsRef<str>) -> Result<Self, MediaTypeError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(MediaTypeError::Empty);
        }
        if value.len() > Self::MAX_LEN {
            return Err(MediaTypeError::TooLong {
                max: Self::MAX_LEN,
                actual: value.len(),
            });
        }

        let parsed = value
            .parse::<Mime>()
            .map_err(|_| MediaTypeError::InvalidSyntax)?;
        let essence = parsed.essence_str();
        let (top_level, subtype) = essence
            .split_once('/')
            .ok_or(MediaTypeError::InvalidSyntax)?;

        if top_level == "*" || subtype == "*" {
            return Err(MediaTypeError::WildcardNotAllowed);
        }
        if !is_restricted_name(top_level) {
            return Err(MediaTypeError::InvalidTypeName);
        }
        if !is_restricted_name(subtype) {
            return Err(MediaTypeError::InvalidSubtypeName);
        }

        let mut parameters = BTreeMap::new();
        for (index, (name, parameter_value)) in parsed.params().enumerate() {
            let actual = index + 1;
            if actual > Self::MAX_PARAMETERS {
                return Err(MediaTypeError::TooManyParameters {
                    max: Self::MAX_PARAMETERS,
                    actual,
                });
            }

            let name = name.as_str();
            if !is_restricted_name(name) {
                return Err(MediaTypeError::InvalidParameterName);
            }

            let parameter_value = parameter_value.as_str();
            if parameter_value.len() > Self::MAX_PARAMETER_VALUE_LEN {
                return Err(MediaTypeError::ParameterValueTooLong {
                    max: Self::MAX_PARAMETER_VALUE_LEN,
                    actual: parameter_value.len(),
                });
            }
            if !is_supported_parameter_value(parameter_value) {
                return Err(MediaTypeError::InvalidParameterValue);
            }

            if parameters
                .insert(name.to_owned(), parameter_value.to_owned())
                .is_some()
            {
                return Err(MediaTypeError::DuplicateParameter);
            }
        }

        let mut canonical = String::with_capacity(value.len());
        canonical.push_str(essence);
        for (name, parameter_value) in parameters {
            canonical.push(';');
            canonical.push_str(&name);
            canonical.push('=');
            push_canonical_parameter_value(&mut canonical, &parameter_value);
        }

        if canonical.len() > Self::MAX_LEN {
            return Err(MediaTypeError::TooLong {
                max: Self::MAX_LEN,
                actual: canonical.len(),
            });
        }

        Ok(Self(canonical.into_boxed_str()))
    }

    /// Returns the canonical media type including parameters.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the lowercase `type/subtype` portion without parameters.
    #[must_use]
    pub fn essence(&self) -> &str {
        self.as_str()
            .split_once(';')
            .map_or(self.as_str(), |(essence, _)| essence)
    }

    /// Returns the lowercase top-level type.
    #[must_use]
    pub fn top_level(&self) -> &str {
        self.essence()
            .split_once('/')
            .map_or("", |(top_level, _)| top_level)
    }

    /// Returns the lowercase subtype, including any structured suffix.
    #[must_use]
    pub fn subtype(&self) -> &str {
        self.essence()
            .split_once('/')
            .map_or("", |(_, subtype)| subtype)
    }
}

impl AsRef<str> for MediaType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for MediaType {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for MediaType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("MediaType")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for MediaType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for MediaType {
    type Err = MediaTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for MediaType {
    type Error = MediaTypeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for MediaType {
    type Error = MediaTypeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<MediaType> for String {
    fn from(value: MediaType) -> Self {
        value.0.into()
    }
}

impl Serialize for MediaType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for MediaType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(MediaTypeVisitor)
    }
}

struct MediaTypeVisitor;

impl de::Visitor<'_> for MediaTypeVisitor {
    type Value = MediaType;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded concrete RFC media type")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        MediaType::new(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        MediaType::new(value).map_err(E::custom)
    }
}

impl JsonSchema for MediaType {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "MediaType".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::MediaType").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 3,
            "maxLength": 512,
            "pattern": MEDIA_TYPE_PATTERN,
            "description": "A canonical concrete media type. StateKnot additionally enforces RFC name syntax, unique sorted parameters, parameter count/value bounds, and canonical quoting at runtime."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for [`MediaType`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum MediaTypeError {
    /// The value contained no bytes.
    #[error("media type must not be empty")]
    Empty,

    /// Raw or canonical text exceeded [`MediaType::MAX_LEN`].
    #[error("media type is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The MIME parser rejected the syntax.
    #[error("media type syntax is invalid")]
    InvalidSyntax,

    /// A media range was supplied instead of a concrete representation type.
    #[error("artifact media type must not contain a wildcard")]
    WildcardNotAllowed,

    /// The top-level type violated RFC 6838 restricted-name syntax.
    #[error("media top-level type violates RFC 6838 restricted-name syntax")]
    InvalidTypeName,

    /// The subtype violated RFC 6838 restricted-name syntax.
    #[error("media subtype violates RFC 6838 restricted-name syntax")]
    InvalidSubtypeName,

    /// A parameter name violated RFC 6838 restricted-name syntax.
    #[error("media parameter name violates RFC 6838 restricted-name syntax")]
    InvalidParameterName,

    /// A parameter name appeared more than once after case normalization.
    #[error("media type contains a duplicate parameter name")]
    DuplicateParameter,

    /// The media type contained too many parameters.
    #[error("media type has {actual} parameters; maximum is {max}")]
    TooManyParameters {
        /// Maximum accepted parameter count.
        max: usize,
        /// First observed count beyond the maximum.
        actual: usize,
    },

    /// One decoded parameter value exceeded its byte ceiling.
    #[error("media parameter value is {actual} bytes; maximum is {max}")]
    ParameterValueTooLong {
        /// Maximum accepted decoded value length.
        max: usize,
        /// Observed decoded byte length.
        actual: usize,
    },

    /// A parameter value could not be represented by the stable ASCII subset.
    #[error("media parameter value uses unsupported or ambiguous characters")]
    InvalidParameterValue,
}

fn is_restricted_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 127 || !bytes[0].is_ascii_alphanumeric() {
        return false;
    }

    bytes.iter().copied().skip(1).all(|byte| {
        byte.is_ascii_alphanumeric()
            || matches!(
                byte,
                b'!' | b'#' | b'$' | b'&' | b'-' | b'^' | b'_' | b'.' | b'+'
            )
    })
}

fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn is_supported_parameter_value(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| (0x20..=0x7e).contains(&byte) && !matches!(byte, b'"' | b'\\'))
}

fn push_canonical_parameter_value(output: &mut String, value: &str) {
    if value.bytes().all(is_http_token_byte) {
        output.push_str(value);
    } else {
        output.push('"');
        output.push_str(value);
        output.push('"');
    }
}

/// A bounded logical artifact name that is never a filesystem path.
///
/// Names preserve exact UTF-8 without normalization. Path separators,
/// dot-segments, leading/trailing whitespace, controls, and Unicode
/// noncharacters are rejected. Adapters must still apply output-context
/// escaping and must never use the name directly as a local path.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArtifactName(Box<str>);

impl ArtifactName {
    /// Maximum UTF-8 encoded length in bytes.
    pub const MAX_BYTES: usize = 255;

    /// Validates and constructs a logical artifact name.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactNameError`] for empty, oversized, path-like,
    /// whitespace-ambiguous, control-bearing, or noncharacter-bearing names.
    pub fn new(value: impl Into<String>) -> Result<Self, ArtifactNameError> {
        let value = value.into();
        validate_artifact_name(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact logical name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ArtifactName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for ArtifactName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for ArtifactName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactName")
            .field("utf8_bytes", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl FromStr for ArtifactName {
    type Err = ArtifactNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for ArtifactName {
    type Error = ArtifactNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ArtifactName {
    type Error = ArtifactNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ArtifactName> for String {
    fn from(value: ArtifactName) -> Self {
        value.0.into()
    }
}

impl Serialize for ArtifactName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ArtifactName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(ArtifactNameVisitor)
    }
}

struct ArtifactNameVisitor;

impl de::Visitor<'_> for ArtifactNameVisitor {
    type Value = ArtifactName;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded logical artifact name")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        ArtifactName::new(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        ArtifactName::new(value).map_err(E::custom)
    }
}

impl JsonSchema for ArtifactName {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ArtifactName".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ArtifactName").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 255,
            "description": "A logical artifact name bounded to 255 UTF-8 bytes. StateKnot rejects path separators, dot-segments, leading/trailing whitespace, controls, and Unicode noncharacters at runtime."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for [`ArtifactName`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ArtifactNameError {
    /// The name contained no bytes.
    #[error("artifact name must not be empty")]
    Empty,

    /// The name exceeded [`ArtifactName::MAX_BYTES`].
    #[error("artifact name is {actual} UTF-8 bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted UTF-8 byte length.
        max: usize,
        /// Observed UTF-8 byte length.
        actual: usize,
    },

    /// The name was `.` or `..`.
    #[error("artifact name must not be a path dot-segment")]
    PathLike,

    /// The name began or ended with Unicode whitespace.
    #[error("artifact name must not begin or end with whitespace")]
    BoundaryWhitespace,

    /// The name contained a path separator.
    #[error("artifact name contains a path separator at UTF-8 byte offset {byte_index}")]
    PathSeparator {
        /// Zero-based UTF-8 byte offset.
        byte_index: usize,
    },

    /// The name contained a control or Unicode noncharacter.
    #[error("artifact name contains a disallowed Unicode scalar at UTF-8 byte offset {byte_index}")]
    DisallowedCodePoint {
        /// Zero-based UTF-8 byte offset without disclosure of content.
        byte_index: usize,
    },
}

fn validate_artifact_name(value: &str) -> Result<(), ArtifactNameError> {
    if value.is_empty() {
        return Err(ArtifactNameError::Empty);
    }
    if value.len() > ArtifactName::MAX_BYTES {
        return Err(ArtifactNameError::TooLong {
            max: ArtifactName::MAX_BYTES,
            actual: value.len(),
        });
    }
    if matches!(value, "." | "..") {
        return Err(ArtifactNameError::PathLike);
    }
    if value.chars().next().is_some_and(char::is_whitespace)
        || value.chars().next_back().is_some_and(char::is_whitespace)
    {
        return Err(ArtifactNameError::BoundaryWhitespace);
    }
    if let Some((byte_index, _)) = value
        .char_indices()
        .find(|(_, scalar)| matches!(scalar, '/' | '\\'))
    {
        return Err(ArtifactNameError::PathSeparator { byte_index });
    }
    if let Some((byte_index, _)) = value
        .char_indices()
        .find(|(_, scalar)| scalar.is_control() || is_unicode_noncharacter(*scalar))
    {
        return Err(ArtifactNameError::DisallowedCodePoint { byte_index });
    }

    Ok(())
}

/// A bounded human-readable artifact description.
///
/// Descriptions preserve exact UTF-8, are never emitted by `Debug`, and use
/// the same control/noncharacter policy as [`TextContent`].
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArtifactDescription(Box<str>);

impl ArtifactDescription {
    /// Maximum UTF-8 encoded length in bytes.
    pub const MAX_BYTES: usize = 4096;

    /// Validates and constructs a human-readable description.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactDescriptionError`] for empty, oversized,
    /// control-bearing, or noncharacter-bearing descriptions.
    pub fn new(value: impl Into<String>) -> Result<Self, ArtifactDescriptionError> {
        let value = value.into();
        validate_artifact_description(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact description.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ArtifactDescription {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for ArtifactDescription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactDescription")
            .field("utf8_bytes", &self.0.len())
            .finish_non_exhaustive()
    }
}

impl FromStr for ArtifactDescription {
    type Err = ArtifactDescriptionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for ArtifactDescription {
    type Error = ArtifactDescriptionError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ArtifactDescription {
    type Error = ArtifactDescriptionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ArtifactDescription> for String {
    fn from(value: ArtifactDescription) -> Self {
        value.0.into()
    }
}

impl Serialize for ArtifactDescription {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ArtifactDescription {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(ArtifactDescriptionVisitor)
    }
}

struct ArtifactDescriptionVisitor;

impl de::Visitor<'_> for ArtifactDescriptionVisitor {
    type Value = ArtifactDescription;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded human-readable artifact description")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        ArtifactDescription::new(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        ArtifactDescription::new(value).map_err(E::custom)
    }
}

impl JsonSchema for ArtifactDescription {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ArtifactDescription".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ArtifactDescription").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 4096,
            "description": "A description bounded to 4096 UTF-8 bytes. StateKnot rejects disallowed controls and Unicode noncharacters at runtime."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for [`ArtifactDescription`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ArtifactDescriptionError {
    /// The description contained no bytes.
    #[error("artifact description must not be empty")]
    Empty,

    /// The description exceeded [`ArtifactDescription::MAX_BYTES`].
    #[error("artifact description is {actual} UTF-8 bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted UTF-8 byte length.
        max: usize,
        /// Observed UTF-8 byte length.
        actual: usize,
    },

    /// The description contained a control or Unicode noncharacter.
    #[error(
        "artifact description contains a disallowed Unicode scalar at UTF-8 byte offset {byte_index}"
    )]
    DisallowedCodePoint {
        /// Zero-based UTF-8 byte offset without disclosure of content.
        byte_index: usize,
    },
}

fn validate_artifact_description(value: &str) -> Result<(), ArtifactDescriptionError> {
    if value.is_empty() {
        return Err(ArtifactDescriptionError::Empty);
    }
    if value.len() > ArtifactDescription::MAX_BYTES {
        return Err(ArtifactDescriptionError::TooLong {
            max: ArtifactDescription::MAX_BYTES,
            actual: value.len(),
        });
    }
    if let Some((byte_index, _)) = value.char_indices().find(|(_, scalar)| {
        (scalar.is_control() && !matches!(scalar, '\t' | '\n' | '\r'))
            || is_unicode_noncharacter(*scalar)
    }) {
        return Err(ArtifactDescriptionError::DisallowedCodePoint { byte_index });
    }
    Ok(())
}

const fn is_unicode_noncharacter(value: char) -> bool {
    let value = value as u32;
    (value >= 0xfdd0 && value <= 0xfdef) || (value & 0xfffe) == 0xfffe
}

/// An opaque, case-sensitive artifact retention-policy class.
///
/// Core assigns no default lifetime, ordering, legal-hold behavior, or deletion
/// permission. A versioned runtime policy interprets the class and records
/// concrete lifecycle decisions separately.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RetentionClass(Box<str>);

impl RetentionClass {
    /// Maximum encoded length in bytes.
    pub const MAX_LEN: usize = 64;

    /// Validates and constructs an opaque retention class.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionClassError`] for empty, oversized, or invalid ASCII
    /// policy keys.
    pub fn new(value: impl Into<String>) -> Result<Self, RetentionClassError> {
        let value = value.into();
        validate_retention_class(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact case-sensitive policy key.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for RetentionClass {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for RetentionClass {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for RetentionClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RetentionClass")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for RetentionClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RetentionClass {
    type Err = RetentionClassError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for RetentionClass {
    type Error = RetentionClassError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for RetentionClass {
    type Error = RetentionClassError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<RetentionClass> for String {
    fn from(value: RetentionClass) -> Self {
        value.0.into()
    }
}

impl Serialize for RetentionClass {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RetentionClass {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(RetentionClassVisitor)
    }
}

struct RetentionClassVisitor;

impl de::Visitor<'_> for RetentionClassVisitor {
    type Value = RetentionClass;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded artifact retention-policy class")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        RetentionClass::new(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        RetentionClass::new(value).map_err(E::custom)
    }
}

impl JsonSchema for RetentionClass {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "RetentionClass".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::RetentionClass").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 64,
            "pattern": RETENTION_CLASS_PATTERN,
            "description": "An opaque, case-sensitive retention-policy class; it grants no deletion or declassification authority by itself."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for [`RetentionClass`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RetentionClassError {
    /// The class contained no bytes.
    #[error("retention class must not be empty")]
    Empty,

    /// The class exceeded [`RetentionClass::MAX_LEN`].
    #[error("retention class is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The first byte was not an ASCII letter or digit.
    #[error("retention class must start with an ASCII letter or digit")]
    InvalidStart,

    /// A later byte did not belong to the stable ASCII grammar.
    #[error("retention class contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first invalid byte.
        index: usize,
    },
}

fn validate_retention_class(value: &str) -> Result<(), RetentionClassError> {
    if value.is_empty() {
        return Err(RetentionClassError::Empty);
    }
    if value.len() > RetentionClass::MAX_LEN {
        return Err(RetentionClassError::TooLong {
            max: RetentionClass::MAX_LEN,
            actual: value.len(),
        });
    }
    if !value.as_bytes()[0].is_ascii_alphanumeric() {
        return Err(RetentionClassError::InvalidStart);
    }
    if let Some((index, _)) = value.bytes().enumerate().skip(1).find(|(_, byte)| {
        !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
    }) {
        return Err(RetentionClassError::InvalidByte { index });
    }
    Ok(())
}

/// Semantic modality used for provider capability negotiation and rendering.
///
/// The declared modality is not inferred from, and cannot override, the media
/// type or byte-level inspection policy. Adapters must validate all three when
/// mapping an artifact to an external provider or protocol.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ArtifactModality {
    /// Large or externally stored textual content.
    Text,
    /// Image content.
    Image,
    /// Audio content.
    Audio,
    /// Video content.
    Video,
    /// Human-oriented document content.
    Document,
    /// Machine-oriented structured data.
    StructuredData,
    /// An archive or compound package.
    Archive,
    /// Other opaque binary content.
    Binary,
}

/// The tenant-qualified identity of an artifact.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct ArtifactIdentity {
    tenant_id: TenantId,
    artifact_id: ArtifactId,
}

impl ArtifactIdentity {
    /// Constructs a tenant-qualified artifact identity.
    #[must_use]
    pub const fn new(tenant_id: TenantId, artifact_id: ArtifactId) -> Self {
        Self {
            tenant_id,
            artifact_id,
        }
    }

    /// Returns the owning tenant.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the tenant-local artifact identifier.
    #[must_use]
    pub const fn artifact_id(&self) -> ArtifactId {
        self.artifact_id
    }
}

/// Human-facing artifact metadata, separate from storage coordinates.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPresentation {
    name: ArtifactName,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<ArtifactDescription>,
}

impl ArtifactPresentation {
    /// Constructs presentation metadata from validated values.
    #[must_use]
    pub const fn new(name: ArtifactName, description: Option<ArtifactDescription>) -> Self {
        Self { name, description }
    }

    /// Returns the logical display name.
    #[must_use]
    pub const fn name(&self) -> &ArtifactName {
        &self.name
    }

    /// Returns the optional human-readable description.
    #[must_use]
    pub const fn description(&self) -> Option<&ArtifactDescription> {
        self.description.as_ref()
    }
}

/// Integrity and interpretation metadata for immutable artifact bytes.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRepresentation {
    media_type: MediaType,
    modality: ArtifactModality,
    byte_length: ByteCount,
    digest: Digest,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<SchemaReference>,
}

impl ArtifactRepresentation {
    /// Constructs immutable representation metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactRepresentationError`] if a zero-length representation
    /// is not bound to the SHA-256 digest of empty bytes.
    pub fn new(
        media_type: MediaType,
        modality: ArtifactModality,
        byte_length: ByteCount,
        digest: Digest,
        schema: Option<SchemaReference>,
    ) -> Result<Self, ArtifactRepresentationError> {
        if byte_length.get() == 0 && digest != Digest::sha256(b"") {
            return Err(ArtifactRepresentationError::EmptyDigestMismatch);
        }
        if schema.is_some() && modality != ArtifactModality::StructuredData {
            return Err(ArtifactRepresentationError::SchemaRequiresStructuredData);
        }
        Ok(Self {
            media_type,
            modality,
            byte_length,
            digest,
            schema,
        })
    }

    /// Returns the declared concrete media type.
    #[must_use]
    pub const fn media_type(&self) -> &MediaType {
        &self.media_type
    }

    /// Returns the declared semantic modality.
    #[must_use]
    pub const fn modality(&self) -> ArtifactModality {
        self.modality
    }

    /// Returns the exact byte length expected from the resolver.
    #[must_use]
    pub const fn byte_length(&self) -> ByteCount {
        self.byte_length
    }

    /// Returns the digest expected from the resolver.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns the optional schema binding for structured bytes.
    #[must_use]
    pub const fn schema(&self) -> Option<&SchemaReference> {
        self.schema.as_ref()
    }
}

impl<'de> Deserialize<'de> for ArtifactRepresentation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            media_type: MediaType,
            modality: ArtifactModality,
            byte_length: ByteCount,
            digest: Digest,
            schema: Option<SchemaReference>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.media_type,
            wire.modality,
            wire.byte_length,
            wire.digest,
            wire.schema,
        )
        .map_err(de::Error::custom)
    }
}

/// Validation failure for [`ArtifactRepresentation`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ArtifactRepresentationError {
    /// Zero bytes were paired with a digest other than SHA-256 of empty bytes.
    #[error("zero-length artifact must use the SHA-256 digest of empty bytes")]
    EmptyDigestMismatch,

    /// A schema was attached to a representation not declared as structured data.
    #[error("artifact schemas require the structured_data modality")]
    SchemaRequiresStructuredData,
}

/// Append-only creator and causation attribution for an artifact.
///
/// The principal owns the registry context for an optional capability
/// reference. `run_id` and `event_id` identify the durable causation record;
/// neither field grants access to the artifact.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactProvenance {
    principal: PrincipalIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    capability: Option<CapabilityReference>,
    run_id: RunId,
    event_id: EventId,
}

impl ArtifactProvenance {
    /// Constructs creator and causation attribution.
    #[must_use]
    pub const fn new(
        principal: PrincipalIdentity,
        capability: Option<CapabilityReference>,
        run_id: RunId,
        event_id: EventId,
    ) -> Self {
        Self {
            principal,
            capability,
            run_id,
            event_id,
        }
    }

    /// Returns the principal responsible for registering the artifact.
    #[must_use]
    pub const fn principal(&self) -> &PrincipalIdentity {
        &self.principal
    }

    /// Returns the optional producing capability and pinned version.
    #[must_use]
    pub const fn capability(&self) -> Option<&CapabilityReference> {
        self.capability.as_ref()
    }

    /// Returns the causing run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the causing durable event.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }
}

/// A canonical bounded set of direct artifact parents.
///
/// Parent identifiers are sorted and unique. The set records direct lineage;
/// transitive traversal and cycle detection belong to the tenant-scoped
/// artifact registry.
#[derive(Clone, Default, Eq, Hash, PartialEq)]
pub struct ArtifactParents(Box<[ArtifactId]>);

impl ArtifactParents {
    /// Maximum number of direct parents on one artifact.
    pub const MAX_LEN: usize = 32;

    /// Constructs an empty lineage set.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Validates, sorts, and constructs direct parent identifiers.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactParentsError`] when the input is too large or
    /// contains a duplicate identifier.
    pub fn new<I>(values: I) -> Result<Self, ArtifactParentsError>
    where
        I: IntoIterator<Item = ArtifactId>,
    {
        let mut parents = Vec::new();
        for parent in values {
            if parents.len() == Self::MAX_LEN {
                return Err(ArtifactParentsError::TooMany {
                    max: Self::MAX_LEN,
                    actual: Self::MAX_LEN + 1,
                });
            }
            parents.push(parent);
        }
        parents.sort_unstable();
        if parents.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ArtifactParentsError::Duplicate);
        }
        Ok(Self(parents.into_boxed_slice()))
    }

    /// Returns the sorted direct parent identifiers.
    #[must_use]
    pub const fn as_slice(&self) -> &[ArtifactId] {
        &self.0
    }

    /// Returns the number of direct parents.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether this lineage set is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn contains(&self, artifact_id: ArtifactId) -> bool {
        self.0.binary_search(&artifact_id).is_ok()
    }
}

impl fmt::Debug for ArtifactParents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactParents")
            .field("count", &self.len())
            .finish_non_exhaustive()
    }
}

impl Serialize for ArtifactParents {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ArtifactParents {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArtifactParentsVisitor)
    }
}

struct ArtifactParentsVisitor;

impl<'de> de::Visitor<'de> for ArtifactParentsVisitor {
    type Value = ArtifactParents;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {} unique artifact identifiers",
            ArtifactParents::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut parents = Vec::with_capacity(
            sequence
                .size_hint()
                .unwrap_or_default()
                .min(ArtifactParents::MAX_LEN),
        );
        while let Some(parent) = sequence.next_element::<ArtifactId>()? {
            if parents.contains(&parent) {
                return Err(de::Error::custom(ArtifactParentsError::Duplicate));
            }
            if parents.len() == ArtifactParents::MAX_LEN {
                return Err(de::Error::custom(ArtifactParentsError::TooMany {
                    max: ArtifactParents::MAX_LEN,
                    actual: ArtifactParents::MAX_LEN + 1,
                }));
            }
            parents.push(parent);
        }
        parents.sort_unstable();
        Ok(ArtifactParents(parents.into_boxed_slice()))
    }
}

impl JsonSchema for ArtifactParents {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ArtifactParents".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ArtifactParents").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ArtifactId>(),
            "maxItems": 32,
            "uniqueItems": true,
            "description": "A canonical lexicographically sorted set of direct artifact parents. Sorting is enforced at runtime."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for [`ArtifactParents`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ArtifactParentsError {
    /// Too many direct parents were supplied.
    #[error("artifact has {actual} direct parents; maximum is {max}")]
    TooMany {
        /// Maximum accepted direct parent count.
        max: usize,
        /// First observed count beyond the maximum.
        actual: usize,
    },

    /// A direct parent identifier appeared more than once.
    #[error("artifact parent identifiers must be unique")]
    Duplicate,
}

/// A complete durable reference to immutable artifact bytes.
///
/// The reference contains no URL, storage credential, bucket, object key, or
/// filesystem path. A tenant-aware resolver must authorize the caller, stream
/// no more than the declared length, and verify the digest before use.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactRef {
    identity: ArtifactIdentity,
    presentation: ArtifactPresentation,
    representation: ArtifactRepresentation,
    metadata: ContentMetadata,
    retention_class: RetentionClass,
    provenance: ArtifactProvenance,
    parents: ArtifactParents,
}

impl ArtifactRef {
    /// Constructs a complete artifact reference from validated components.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactRefError`] when content metadata does not identify an
    /// artifact source or the artifact names itself as a direct parent.
    pub fn new(
        identity: ArtifactIdentity,
        presentation: ArtifactPresentation,
        representation: ArtifactRepresentation,
        metadata: ContentMetadata,
        retention_class: RetentionClass,
        provenance: ArtifactProvenance,
        parents: ArtifactParents,
    ) -> Result<Self, ArtifactRefError> {
        if metadata.source() != ContentSource::Artifact {
            return Err(ArtifactRefError::InvalidContentSource {
                actual: metadata.source(),
            });
        }
        if parents.contains(identity.artifact_id()) {
            return Err(ArtifactRefError::SelfParent);
        }
        Ok(Self {
            identity,
            presentation,
            representation,
            metadata,
            retention_class,
            provenance,
            parents,
        })
    }

    /// Returns the tenant-qualified identity.
    #[must_use]
    pub const fn identity(&self) -> &ArtifactIdentity {
        &self.identity
    }

    /// Returns the human-facing presentation metadata.
    #[must_use]
    pub const fn presentation(&self) -> &ArtifactPresentation {
        &self.presentation
    }

    /// Returns the immutable representation metadata.
    #[must_use]
    pub const fn representation(&self) -> &ArtifactRepresentation {
        &self.representation
    }

    /// Returns the content security metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ContentMetadata {
        &self.metadata
    }

    /// Returns the opaque retention class.
    #[must_use]
    pub const fn retention_class(&self) -> &RetentionClass {
        &self.retention_class
    }

    /// Returns creator and causation attribution.
    #[must_use]
    pub const fn provenance(&self) -> &ArtifactProvenance {
        &self.provenance
    }

    /// Returns the canonical direct lineage.
    #[must_use]
    pub const fn parents(&self) -> &ArtifactParents {
        &self.parents
    }
}

impl fmt::Debug for ArtifactRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactRef")
            .field("identity", &self.identity)
            .field("presentation", &self.presentation)
            .field("representation", &self.representation)
            .field("metadata", &self.metadata)
            .field("retention_class", &self.retention_class)
            .field("provenance", &self.provenance)
            .field("parent_count", &self.parents.len())
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ArtifactRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            identity: ArtifactIdentity,
            presentation: ArtifactPresentation,
            representation: ArtifactRepresentation,
            metadata: ContentMetadata,
            retention_class: RetentionClass,
            provenance: ArtifactProvenance,
            parents: ArtifactParents,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.identity,
            wire.presentation,
            wire.representation,
            wire.metadata,
            wire.retention_class,
            wire.provenance,
            wire.parents,
        )
        .map_err(de::Error::custom)
    }
}

/// Validation failure for [`ArtifactRef`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ArtifactRefError {
    /// Metadata described another immediate source.
    #[error("artifact reference metadata source must be artifact, got {actual:?}")]
    InvalidContentSource {
        /// The rejected source classification.
        actual: ContentSource,
    },

    /// The artifact listed its own identifier as a direct parent.
    #[error("artifact must not list itself as a direct parent")]
    SelfParent,
}

/// A closed v1 content value used across runtime boundaries.
///
/// Protocol adapters ingest inline bytes and URLs into the artifact boundary
/// before constructing this enum. Consequently no variant can carry raw bytes,
/// base64 text, storage coordinates, or a permanent remote URL.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(
    tag = "type",
    content = "content",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ContentPart {
    /// Validated inline UTF-8 text.
    Text(TextContent),
    /// Resource-bounded structured JSON.
    Json(JsonContent),
    /// A tenant-scoped immutable artifact reference.
    Artifact(Box<ArtifactRef>),
}

impl ContentPart {
    /// Returns security metadata common to every content variant.
    #[must_use]
    pub const fn metadata(&self) -> &ContentMetadata {
        match self {
            Self::Text(content) => content.metadata(),
            Self::Json(content) => content.metadata(),
            Self::Artifact(content) => content.metadata(),
        }
    }
}

impl From<TextContent> for ContentPart {
    fn from(value: TextContent) -> Self {
        Self::Text(value)
    }
}

impl From<JsonContent> for ContentPart {
    fn from(value: JsonContent) -> Self {
        Self::Json(value)
    }
}

impl From<ArtifactRef> for ContentPart {
    fn from(value: ArtifactRef) -> Self {
        Self::Artifact(Box::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BoundedJson, ContentTrust, IssuerId, RedactionState, SecurityLabel, SubjectId, Version,
    };
    use serde_json::{Value, from_value, json, to_value};

    const ARTIFACT_ID: &str = "01912345-6789-7abc-8def-0123456789ab";
    const PARENT_A: &str = "01912345-6789-7abc-8def-0123456789ac";
    const PARENT_B: &str = "01912345-6789-7abc-8def-0123456789ad";
    const RUN_ID: &str = "01912345-6789-7abc-8def-0123456789ae";
    const EVENT_ID: &str = "01912345-6789-7abc-8def-0123456789af";

    fn artifact_metadata() -> ContentMetadata {
        ContentMetadata::new(
            ContentSource::Artifact,
            ContentTrust::Untrusted,
            "internal/pii".parse::<SecurityLabel>().unwrap(),
            RedactionState::NotApplied,
        )
    }

    fn provenance() -> ArtifactProvenance {
        ArtifactProvenance::new(
            PrincipalIdentity::new(
                "https://issuer.example.com".parse::<IssuerId>().unwrap(),
                "service-account".parse::<SubjectId>().unwrap(),
            ),
            Some(CapabilityReference::new(
                "files.convert".parse().unwrap(),
                Version::new(1, 2, 3),
            )),
            RUN_ID.parse().unwrap(),
            EVENT_ID.parse().unwrap(),
        )
    }

    fn representation() -> ArtifactRepresentation {
        ArtifactRepresentation::new(
            "application/pdf".parse().unwrap(),
            ArtifactModality::Document,
            ByteCount::new(12),
            Digest::sha256(b"artifact-pdf"),
            None,
        )
        .unwrap()
    }

    fn artifact_ref() -> ArtifactRef {
        ArtifactRef::new(
            ArtifactIdentity::new("tenant-a".parse().unwrap(), ARTIFACT_ID.parse().unwrap()),
            ArtifactPresentation::new(
                "incident-report.pdf".parse().unwrap(),
                Some("Sanitized incident report".parse().unwrap()),
            ),
            representation(),
            artifact_metadata(),
            "audit/7y".parse().unwrap(),
            provenance(),
            ArtifactParents::new([PARENT_B.parse().unwrap(), PARENT_A.parse().unwrap()]).unwrap(),
        )
        .unwrap()
    }

    fn assert_no_storage_coordinate_keys(value: &Value) {
        match value {
            Value::Object(object) => {
                for key in object.keys() {
                    assert!(
                        !matches!(
                            key.as_str(),
                            "url" | "uri" | "bucket" | "object_key" | "path" | "credential"
                        ),
                        "wire contains forbidden storage-coordinate key {key}"
                    );
                }
                for value in object.values() {
                    assert_no_storage_coordinate_keys(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    assert_no_storage_coordinate_keys(value);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn media_types_normalize_case_parameters_order_and_quotes() {
        for (input, expected) in [
            ("IMAGE/PNG", "image/png"),
            (
                "Text/Plain; Format=flowed; Charset=UTF-8",
                "text/plain;charset=utf-8;format=flowed",
            ),
            (
                "video/mp4; codecs=\"avc1.42E01E, mp4a.40.2\"",
                "video/mp4;codecs=\"avc1.42E01E, mp4a.40.2\"",
            ),
        ] {
            let media_type = input.parse::<MediaType>().unwrap();
            assert_eq!(media_type.as_str(), expected);
            assert_eq!(to_value(media_type).unwrap(), Value::from(expected));
        }

        let media_type = "application/ld+json;profile=\"https://example.com/v1\""
            .parse::<MediaType>()
            .unwrap();
        assert_eq!(media_type.essence(), "application/ld+json");
        assert_eq!(media_type.top_level(), "application");
        assert_eq!(media_type.subtype(), "ld+json");
    }

    #[test]
    fn media_types_reject_ranges_ambiguity_and_resource_excess() {
        assert_eq!("".parse::<MediaType>(), Err(MediaTypeError::Empty));
        for wildcard in ["*/*", "image/*"] {
            assert!(matches!(
                wildcard.parse::<MediaType>(),
                Err(MediaTypeError::WildcardNotAllowed | MediaTypeError::InvalidSubtypeName)
            ));
        }
        assert_eq!(
            "application/json;a=1;A=2".parse::<MediaType>(),
            Err(MediaTypeError::DuplicateParameter)
        );
        assert_eq!(
            "application/json;bad%=value".parse::<MediaType>(),
            Err(MediaTypeError::InvalidParameterName)
        );
        assert_eq!(
            "application/json;profile=\"bad\\value\"".parse::<MediaType>(),
            Err(MediaTypeError::InvalidParameterValue)
        );
        assert_eq!(
            "application/json;profile=\"é\"".parse::<MediaType>(),
            Err(MediaTypeError::InvalidParameterValue)
        );

        let too_many = format!(
            "application/json;{}",
            (0..=MediaType::MAX_PARAMETERS)
                .map(|index| format!("p{index}=v"))
                .collect::<Vec<_>>()
                .join(";")
        );
        assert_eq!(
            too_many.parse::<MediaType>(),
            Err(MediaTypeError::TooManyParameters {
                max: MediaType::MAX_PARAMETERS,
                actual: MediaType::MAX_PARAMETERS + 1,
            })
        );

        let too_long = "a".repeat(MediaType::MAX_LEN + 1);
        assert_eq!(
            too_long.parse::<MediaType>(),
            Err(MediaTypeError::TooLong {
                max: MediaType::MAX_LEN,
                actual: MediaType::MAX_LEN + 1,
            })
        );
    }

    #[test]
    fn media_type_serde_and_schema_revalidate_input() {
        let media_type = from_value::<MediaType>(json!("Application/JSON")).unwrap();
        assert_eq!(media_type.as_str(), "application/json");
        assert!(from_value::<MediaType>(json!(42)).is_err());
        assert!(from_value::<MediaType>(json!("image/*")).is_err());

        let schema = to_value(schemars::schema_for!(MediaType)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["maxLength"], MediaType::MAX_LEN);
        assert_eq!(schema["pattern"], MEDIA_TYPE_PATTERN);
    }

    #[test]
    fn artifact_names_are_bounded_path_safe_and_debug_redacted() {
        for value in ["report.pdf", "事故报告 42.pdf", "résumé.txt"] {
            let name = value.parse::<ArtifactName>().unwrap();
            assert_eq!(name.as_str(), value);
            assert_eq!(to_value(&name).unwrap(), Value::from(value));
            let debug = format!("{name:?}");
            assert!(!debug.contains(value));
        }

        assert_eq!("".parse::<ArtifactName>(), Err(ArtifactNameError::Empty));
        assert_eq!(
            ".".parse::<ArtifactName>(),
            Err(ArtifactNameError::PathLike)
        );
        assert_eq!(
            "..".parse::<ArtifactName>(),
            Err(ArtifactNameError::PathLike)
        );
        for value in [" report.pdf", "report.pdf "] {
            assert_eq!(
                value.parse::<ArtifactName>(),
                Err(ArtifactNameError::BoundaryWhitespace)
            );
        }
        for (value, byte_index) in [("../secret", 2), ("dir\\secret", 3)] {
            assert_eq!(
                value.parse::<ArtifactName>(),
                Err(ArtifactNameError::PathSeparator { byte_index })
            );
        }
        assert_eq!(
            "a\nb".parse::<ArtifactName>(),
            Err(ArtifactNameError::DisallowedCodePoint { byte_index: 1 })
        );
    }

    #[test]
    fn descriptions_and_retention_classes_are_strict_and_redacted() {
        let description = "Secret line one\nline two"
            .parse::<ArtifactDescription>()
            .unwrap();
        assert_eq!(description.as_str(), "Secret line one\nline two");
        assert!(!format!("{description:?}").contains("Secret"));
        assert_eq!(
            "a\u{0}b".parse::<ArtifactDescription>(),
            Err(ArtifactDescriptionError::DisallowedCodePoint { byte_index: 1 })
        );

        let retention = "Audit/7Y".parse::<RetentionClass>().unwrap();
        assert_eq!(retention.as_str(), "Audit/7Y");
        assert_ne!(retention, "audit/7y".parse().unwrap());
        assert_eq!(
            "bad class".parse::<RetentionClass>(),
            Err(RetentionClassError::InvalidByte { index: 3 })
        );
    }

    #[test]
    fn zero_length_representation_requires_the_known_empty_digest() {
        let valid = ArtifactRepresentation::new(
            "application/octet-stream".parse().unwrap(),
            ArtifactModality::Binary,
            ByteCount::new(0),
            Digest::sha256(b""),
            None,
        );
        assert!(valid.is_ok());

        assert_eq!(
            ArtifactRepresentation::new(
                "application/octet-stream".parse().unwrap(),
                ArtifactModality::Binary,
                ByteCount::new(0),
                Digest::sha256(b"not empty"),
                None,
            ),
            Err(ArtifactRepresentationError::EmptyDigestMismatch)
        );

        let invalid_wire = json!({
            "media_type": "application/octet-stream",
            "modality": "binary",
            "byte_length": "0",
            "digest": Digest::sha256(b"not empty").to_string()
        });
        assert!(from_value::<ArtifactRepresentation>(invalid_wire).is_err());

        let schema = SchemaReference::new(
            "https://schemas.example.com/artifact/1.0.0"
                .parse()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"artifact schema"),
        );
        assert_eq!(
            ArtifactRepresentation::new(
                "application/pdf".parse().unwrap(),
                ArtifactModality::Document,
                ByteCount::new(12),
                Digest::sha256(b"artifact-pdf"),
                Some(schema),
            ),
            Err(ArtifactRepresentationError::SchemaRequiresStructuredData)
        );
    }

    #[test]
    fn artifact_parents_are_sorted_unique_bounded_and_redacted() {
        let parent_a = PARENT_A.parse::<ArtifactId>().unwrap();
        let parent_b = PARENT_B.parse::<ArtifactId>().unwrap();
        let parents = ArtifactParents::new([parent_b, parent_a]).unwrap();
        assert_eq!(parents.as_slice(), &[parent_a, parent_b]);
        assert_eq!(to_value(&parents).unwrap(), json!([PARENT_A, PARENT_B]));
        assert_eq!(format!("{parents:?}"), "ArtifactParents { count: 2, .. }");
        assert_eq!(
            ArtifactParents::new([parent_a, parent_a]),
            Err(ArtifactParentsError::Duplicate)
        );

        let too_many = (0..=ArtifactParents::MAX_LEN)
            .map(|_| ArtifactId::generate())
            .collect::<Vec<_>>();
        assert_eq!(
            ArtifactParents::new(too_many),
            Err(ArtifactParentsError::TooMany {
                max: ArtifactParents::MAX_LEN,
                actual: ArtifactParents::MAX_LEN + 1,
            })
        );

        assert!(from_value::<ArtifactParents>(json!([PARENT_B, PARENT_A])).is_ok());
        assert!(from_value::<ArtifactParents>(json!([PARENT_A, PARENT_A])).is_err());
        assert_eq!(ArtifactParents::default(), ArtifactParents::empty());

        let too_many_wire = (0..=ArtifactParents::MAX_LEN)
            .map(|_| ArtifactId::generate())
            .collect::<Vec<_>>();
        assert!(from_value::<ArtifactParents>(json!(too_many_wire)).is_err());
    }

    #[test]
    fn complete_artifact_reference_round_trips_without_storage_coordinates() {
        let artifact = artifact_ref();
        let encoded = to_value(&artifact).unwrap();
        assert_eq!(encoded["identity"]["tenant_id"], "tenant-a");
        assert_eq!(encoded["identity"]["artifact_id"], ARTIFACT_ID);
        assert_eq!(encoded["representation"]["media_type"], "application/pdf");
        assert_eq!(encoded["parents"], json!([PARENT_A, PARENT_B]));
        assert_eq!(
            from_value::<ArtifactRef>(encoded.clone()).unwrap(),
            artifact
        );

        assert_no_storage_coordinate_keys(&encoded);

        let debug = format!("{artifact:?}");
        assert!(!debug.contains("incident-report.pdf"));
        assert!(!debug.contains("Sanitized incident report"));
        assert!(!debug.contains("service-account"));
    }

    #[test]
    fn artifact_reference_rejects_source_confusion_self_parent_and_unknown_fields() {
        let identity =
            ArtifactIdentity::new("tenant-a".parse().unwrap(), ARTIFACT_ID.parse().unwrap());
        let presentation = ArtifactPresentation::new("report.pdf".parse().unwrap(), None);
        let wrong_metadata = ContentMetadata::new(
            ContentSource::RemoteAgent,
            ContentTrust::Untrusted,
            "internal".parse().unwrap(),
            RedactionState::NotApplied,
        );
        assert_eq!(
            ArtifactRef::new(
                identity.clone(),
                presentation.clone(),
                representation(),
                wrong_metadata,
                "audit/7y".parse().unwrap(),
                provenance(),
                ArtifactParents::empty(),
            ),
            Err(ArtifactRefError::InvalidContentSource {
                actual: ContentSource::RemoteAgent,
            })
        );
        assert_eq!(
            ArtifactRef::new(
                identity,
                presentation,
                representation(),
                artifact_metadata(),
                "audit/7y".parse().unwrap(),
                provenance(),
                ArtifactParents::new([ARTIFACT_ID.parse().unwrap()]).unwrap(),
            ),
            Err(ArtifactRefError::SelfParent)
        );

        let mut encoded = to_value(artifact_ref()).unwrap();
        encoded["extra"] = Value::Bool(true);
        assert!(from_value::<ArtifactRef>(encoded).is_err());
    }

    #[test]
    fn content_part_is_closed_tagged_and_exposes_security_metadata() {
        let artifact = artifact_ref();
        let part = ContentPart::from(artifact.clone());
        let expected = json!({
            "type": "artifact",
            "content": to_value(artifact).unwrap()
        });
        assert_eq!(to_value(&part).unwrap(), expected);
        assert_eq!(from_value::<ContentPart>(expected).unwrap(), part);
        assert_eq!(part.metadata(), artifact_metadata().borrow());

        for invalid in [
            json!({"type": "raw", "content": "AA=="}),
            json!({"type": "url", "content": "https://example.com/file"}),
            json!({
                "type": "artifact",
                "content": to_value(artifact_ref()).unwrap(),
                "extra": true
            }),
        ] {
            assert!(from_value::<ContentPart>(invalid).is_err());
        }

        let schema = to_value(schemars::schema_for!(ContentPart)).unwrap();
        assert!(schema.get("oneOf").is_some());
    }

    #[test]
    fn artifact_schemas_close_objects_and_express_collection_bounds() {
        let identity = to_value(schemars::schema_for!(ArtifactIdentity)).unwrap();
        assert_eq!(identity["additionalProperties"], false);
        assert_eq!(identity["required"], json!(["tenant_id", "artifact_id"]));

        let parents = to_value(schemars::schema_for!(ArtifactParents)).unwrap();
        assert_eq!(parents["type"], "array");
        assert_eq!(parents["maxItems"], ArtifactParents::MAX_LEN);
        assert_eq!(parents["uniqueItems"], true);

        let artifact = to_value(schemars::schema_for!(ArtifactRef)).unwrap();
        assert_eq!(artifact["type"], "object");
        assert_eq!(artifact["additionalProperties"], false);
        assert_eq!(
            artifact["required"],
            json!([
                "identity",
                "presentation",
                "representation",
                "metadata",
                "retention_class",
                "provenance",
                "parents"
            ])
        );
    }

    #[test]
    fn bounded_json_content_still_maps_to_the_json_content_part() {
        let json_content = JsonContent::new(
            BoundedJson::from_str(r#"{"answer":42}"#).unwrap(),
            None,
            ContentMetadata::new(
                ContentSource::Tool,
                ContentTrust::Untrusted,
                "internal".parse().unwrap(),
                RedactionState::NotApplied,
            ),
        );
        let part = ContentPart::from(json_content);
        assert!(matches!(part, ContentPart::Json(_)));
    }
}
