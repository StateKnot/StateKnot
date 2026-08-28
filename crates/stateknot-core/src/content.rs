// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Validated content values and security metadata for runtime boundaries.

use std::{borrow::Borrow, collections::HashSet, fmt, str::FromStr};

use language_tags::LanguageTag as ParsedLanguageTag;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{BoundedJson, SchemaReference};

const KIBIBYTE: usize = 1024;
const LANGUAGE_TAG_PATTERN: &str = "^[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*$";
const SECURITY_LABEL_PATTERN: &str = "^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$";

/// A stable, case-normalized RFC 5646 language tag.
///
/// Parsing accepts well-formed language tags, grandfathered tags, and private
/// use tags. `StateKnot` stores lowercase ASCII because RFC 5646 comparison is
/// case-insensitive. It intentionally does not consult the mutable IANA
/// Language Subtag Registry or apply registry-version-dependent preferred-value
/// replacements, so durable data remains readable offline and over time.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LanguageTag(Box<str>);

impl LanguageTag {
    /// Maximum encoded length in bytes.
    pub const MAX_LEN: usize = 255;

    /// Validates and constructs a language tag.
    ///
    /// # Errors
    ///
    /// Returns [`LanguageTagError`] when the input is empty, too long, not a
    /// well-formed RFC 5646 language tag, or repeats a variant or extension
    /// singleton.
    pub fn new(value: impl AsRef<str>) -> Result<Self, LanguageTagError> {
        let value = value.as_ref();
        if value.is_empty() {
            return Err(LanguageTagError::Empty);
        }
        if value.len() > Self::MAX_LEN {
            return Err(LanguageTagError::TooLong {
                max: Self::MAX_LEN,
                actual: value.len(),
            });
        }

        let parsed =
            ParsedLanguageTag::parse(value).map_err(|_| LanguageTagError::InvalidSyntax)?;

        let mut variants = HashSet::new();
        if parsed
            .variant_subtags()
            .any(|variant| !variants.insert(variant))
        {
            return Err(LanguageTagError::DuplicateVariant);
        }

        let mut extensions = HashSet::new();
        if parsed
            .extension_subtags()
            .any(|(singleton, _)| !extensions.insert(singleton))
        {
            return Err(LanguageTagError::DuplicateExtension);
        }

        Ok(Self(
            parsed.into_string().to_ascii_lowercase().into_boxed_str(),
        ))
    }

    /// Returns the stable lowercase wire representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for LanguageTag {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for LanguageTag {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for LanguageTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("LanguageTag")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for LanguageTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for LanguageTag {
    type Err = LanguageTagError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for LanguageTag {
    type Error = LanguageTagError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for LanguageTag {
    type Error = LanguageTagError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<LanguageTag> for String {
    fn from(value: LanguageTag) -> Self {
        value.0.into()
    }
}

impl Serialize for LanguageTag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LanguageTag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(LanguageTagVisitor)
    }
}

struct LanguageTagVisitor;

impl de::Visitor<'_> for LanguageTagVisitor {
    type Value = LanguageTag;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded, well-formed RFC 5646 language tag")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        LanguageTag::new(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        LanguageTag::new(value).map_err(E::custom)
    }
}

impl JsonSchema for LanguageTag {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "LanguageTag".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::LanguageTag").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 255,
            "pattern": LANGUAGE_TAG_PATTERN,
            "description": "A well-formed RFC 5646 language tag serialized in lowercase. Decoding accepts ASCII case variants; StateKnot also rejects duplicate variants and extension singletons at runtime without consulting the mutable IANA registry."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for a [`LanguageTag`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum LanguageTagError {
    /// The tag contained no bytes.
    #[error("language tag must not be empty")]
    Empty,

    /// The tag exceeded [`LanguageTag::MAX_LEN`].
    #[error("language tag is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The tag did not match the RFC 5646 well-formed grammar.
    #[error("language tag is not well-formed RFC 5646 text")]
    InvalidSyntax,

    /// A variant subtag appeared more than once.
    #[error("language tag contains a duplicate variant subtag")]
    DuplicateVariant,

    /// An extension singleton appeared more than once.
    #[error("language tag contains a duplicate extension singleton")]
    DuplicateExtension,
}

/// An opaque, bounded label interpreted only by an explicit policy engine.
///
/// Labels are case-sensitive and are never trimmed, normalized, ranked, or
/// treated as an authorization grant by `stateknot-core`.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SecurityLabel(Box<str>);

impl SecurityLabel {
    /// Maximum encoded length in bytes.
    pub const MAX_LEN: usize = 128;

    /// Validates and constructs an opaque security label.
    ///
    /// # Errors
    ///
    /// Returns [`SecurityLabelError`] when the value is empty, too long, does
    /// not start with an ASCII alphanumeric byte, or contains a byte outside
    /// the stable ASCII grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, SecurityLabelError> {
        let value = value.into();
        validate_security_label(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact, case-sensitive policy label.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SecurityLabel {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for SecurityLabel {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SecurityLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SecurityLabel")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for SecurityLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SecurityLabel {
    type Err = SecurityLabelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for SecurityLabel {
    type Error = SecurityLabelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for SecurityLabel {
    type Error = SecurityLabelError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SecurityLabel> for String {
    fn from(value: SecurityLabel) -> Self {
        value.0.into()
    }
}

impl Serialize for SecurityLabel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SecurityLabel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(SecurityLabelVisitor)
    }
}

struct SecurityLabelVisitor;

impl de::Visitor<'_> for SecurityLabelVisitor {
    type Value = SecurityLabel;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded StateKnot security policy label")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        SecurityLabel::new(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        SecurityLabel::new(value).map_err(E::custom)
    }
}

impl JsonSchema for SecurityLabel {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SecurityLabel".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::SecurityLabel").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 128,
            "pattern": SECURITY_LABEL_PATTERN,
            "description": "An opaque, case-sensitive policy label; it conveys no authorization by itself."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for a [`SecurityLabel`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SecurityLabelError {
    /// The label contained no bytes.
    #[error("security label must not be empty")]
    Empty,

    /// The label exceeded [`SecurityLabel::MAX_LEN`].
    #[error("security label is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The first byte was not an ASCII letter or digit.
    #[error("security label must start with an ASCII letter or digit")]
    InvalidStart,

    /// A later byte did not belong to the stable ASCII grammar.
    #[error("security label contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first invalid byte.
        index: usize,
    },
}

fn validate_security_label(value: &str) -> Result<(), SecurityLabelError> {
    if value.is_empty() {
        return Err(SecurityLabelError::Empty);
    }
    if value.len() > SecurityLabel::MAX_LEN {
        return Err(SecurityLabelError::TooLong {
            max: SecurityLabel::MAX_LEN,
            actual: value.len(),
        });
    }

    if !value.as_bytes()[0].is_ascii_alphanumeric() {
        return Err(SecurityLabelError::InvalidStart);
    }

    if let Some((index, _)) = value.bytes().enumerate().skip(1).find(|(_, byte)| {
        !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
    }) {
        return Err(SecurityLabelError::InvalidByte { index });
    }

    Ok(())
}

/// The immediate domain source from which content entered `StateKnot`.
///
/// This is attribution metadata, not an identity or authorization decision.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ContentSource {
    /// Application-owned configuration or code.
    Application,
    /// Direct user input.
    User,
    /// Model-generated output.
    Model,
    /// Tool output or MCP-provided content.
    Tool,
    /// Content received from a remote agent, including A2A peers.
    RemoteAgent,
    /// Content resolved from a registered artifact.
    Artifact,
}

/// An asserted content trust classification.
///
/// This field supports policy evaluation and audit. Deserializing
/// [`ContentTrust::ApplicationControlled`] never grants authority and cannot by
/// itself construct a trusted instruction; that requires an application-owned
/// provenance and policy path outside this metadata type.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ContentTrust {
    /// The application asserts control over the content's authorship.
    ApplicationControlled,
    /// The content must be handled as untrusted data.
    Untrusted,
}

/// The redaction transformation recorded for this representation.
///
/// `NotApplied` means no redaction transformation was recorded; it does not
/// mean the content is non-sensitive or safe to log.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum RedactionState {
    /// No redaction transformation was applied to this representation.
    NotApplied,
    /// Some policy-selected content was replaced or removed.
    Partial,
    /// The policy-selected sensitive content was fully replaced or removed.
    Full,
}

/// Security and source metadata carried by each content value.
///
/// The fields are explicit so adapters cannot silently discard trust-boundary
/// information. This object is an auditable claim only: it does not authorize
/// execution, declassification, logging, or conversion into an instruction.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContentMetadata {
    source: ContentSource,
    trust: ContentTrust,
    security_label: SecurityLabel,
    redaction: RedactionState,
}

impl ContentMetadata {
    /// Constructs metadata from explicit validated components.
    #[must_use]
    pub const fn new(
        source: ContentSource,
        trust: ContentTrust,
        security_label: SecurityLabel,
        redaction: RedactionState,
    ) -> Self {
        Self {
            source,
            trust,
            security_label,
            redaction,
        }
    }

    /// Constructs the common untrusted, not-yet-redacted classification.
    #[must_use]
    pub const fn untrusted(source: ContentSource, security_label: SecurityLabel) -> Self {
        Self::new(
            source,
            ContentTrust::Untrusted,
            security_label,
            RedactionState::NotApplied,
        )
    }

    /// Returns the immediate content source.
    #[must_use]
    pub const fn source(&self) -> ContentSource {
        self.source
    }

    /// Returns the asserted trust classification.
    #[must_use]
    pub const fn trust(&self) -> ContentTrust {
        self.trust
    }

    /// Returns the opaque policy label.
    #[must_use]
    pub const fn security_label(&self) -> &SecurityLabel {
        &self.security_label
    }

    /// Returns the redaction transformation state.
    #[must_use]
    pub const fn redaction(&self) -> RedactionState {
        self.redaction
    }
}

/// Validated, bounded UTF-8 text with mandatory security metadata.
///
/// Text is preserved byte-for-byte without trimming or Unicode normalization.
/// `StateKnot` rejects C0/C1 controls other than tab, line feed, and carriage
/// return, plus Unicode noncharacters. Multilingual text and bidi formatting
/// characters remain valid data; renderers and logs must still escape untrusted
/// content for their output context.
#[derive(Clone, Eq, PartialEq, Serialize)]
pub struct TextContent {
    text: Box<str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<LanguageTag>,
    metadata: ContentMetadata,
}

impl TextContent {
    /// Maximum UTF-8 encoded text length in bytes.
    pub const MAX_BYTES: usize = 256 * KIBIBYTE;

    /// Validates borrowed text and copies it into an immutable value.
    ///
    /// # Errors
    ///
    /// Returns [`TextContentError`] when text is empty, exceeds the byte
    /// ceiling, or contains a disallowed control or Unicode noncharacter.
    pub fn new(
        text: &str,
        language: Option<LanguageTag>,
        metadata: ContentMetadata,
    ) -> Result<Self, TextContentError> {
        validate_text(text)?;
        Ok(Self {
            text: text.into(),
            language,
            metadata,
        })
    }

    /// Validates owned text without copying its bytes.
    ///
    /// # Errors
    ///
    /// Returns [`TextContentError`] when text is empty, exceeds the byte
    /// ceiling, or contains a disallowed control or Unicode noncharacter.
    pub fn from_string(
        text: String,
        language: Option<LanguageTag>,
        metadata: ContentMetadata,
    ) -> Result<Self, TextContentError> {
        validate_text(&text)?;
        Ok(Self {
            text: text.into_boxed_str(),
            language,
            metadata,
        })
    }

    /// Returns the exact validated UTF-8 text.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Returns the optional stable language tag.
    #[must_use]
    pub const fn language(&self) -> Option<&LanguageTag> {
        self.language.as_ref()
    }

    /// Returns the mandatory security metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ContentMetadata {
        &self.metadata
    }

    /// Consumes the value and returns its text allocation.
    #[must_use]
    pub fn into_text(self) -> String {
        self.text.into()
    }
}

impl fmt::Debug for TextContent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TextContent")
            .field("text_bytes", &self.text.len())
            .field("language", &self.language)
            .field("metadata", &self.metadata)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for TextContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            text: String,
            language: Option<LanguageTag>,
            metadata: ContentMetadata,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::from_string(wire.text, wire.language, wire.metadata).map_err(de::Error::custom)
    }
}

impl JsonSchema for TextContent {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "TextContent".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::TextContent").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 262_144,
                    "description": "Exact UTF-8 text. maxLength is a necessary code-point ceiling; StateKnot separately enforces the 262144-byte ceiling and rejects disallowed controls and Unicode noncharacters at runtime."
                },
                "language": generator.subschema_for::<LanguageTag>(),
                "metadata": generator.subschema_for::<ContentMetadata>()
            },
            "required": ["text", "metadata"],
            "additionalProperties": false
        })
    }
}

/// Validation failure for [`TextContent`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TextContentError {
    /// The text contained no bytes.
    #[error("text content must not be empty")]
    Empty,

    /// The text exceeded [`TextContent::MAX_BYTES`].
    #[error("text content is {actual} UTF-8 bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted UTF-8 byte length.
        max: usize,
        /// Observed UTF-8 byte length.
        actual: usize,
    },

    /// A control or Unicode noncharacter was present.
    #[error("text content contains a disallowed Unicode scalar at UTF-8 byte offset {byte_index}")]
    DisallowedCodePoint {
        /// Zero-based UTF-8 byte offset without disclosure of content.
        byte_index: usize,
    },
}

fn validate_text(text: &str) -> Result<(), TextContentError> {
    if text.is_empty() {
        return Err(TextContentError::Empty);
    }
    if text.len() > TextContent::MAX_BYTES {
        return Err(TextContentError::TooLong {
            max: TextContent::MAX_BYTES,
            actual: text.len(),
        });
    }

    if let Some((byte_index, _)) = text.char_indices().find(|(_, scalar)| {
        (scalar.is_control() && !matches!(scalar, '\t' | '\n' | '\r'))
            || is_unicode_noncharacter(*scalar)
    }) {
        return Err(TextContentError::DisallowedCodePoint { byte_index });
    }

    Ok(())
}

const fn is_unicode_noncharacter(value: char) -> bool {
    let value = value as u32;
    (value >= 0xfdd0 && value <= 0xfdef) || (value & 0xfffe) == 0xfffe
}

/// Structured content materialized under bounded JSON limits.
///
/// An optional [`SchemaReference`] binds the declared schema identity, version,
/// and digest. Construction does not resolve a schema registry or execute JSON
/// Schema validation; adapters must perform that explicit operation before a
/// schema-dependent use.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonContent {
    value: BoundedJson,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<SchemaReference>,
    metadata: ContentMetadata,
}

impl JsonContent {
    /// Constructs structured content from validated components.
    #[must_use]
    pub const fn new(
        value: BoundedJson,
        schema: Option<SchemaReference>,
        metadata: ContentMetadata,
    ) -> Self {
        Self {
            value,
            schema,
            metadata,
        }
    }

    /// Returns the bounded JSON value.
    #[must_use]
    pub const fn value(&self) -> &BoundedJson {
        &self.value
    }

    /// Returns the optional immutable schema binding.
    #[must_use]
    pub const fn schema(&self) -> Option<&SchemaReference> {
        self.schema.as_ref()
    }

    /// Returns the mandatory security metadata.
    #[must_use]
    pub const fn metadata(&self) -> &ContentMetadata {
        &self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Digest, SchemaId, Version};
    use proptest::{collection, prelude::*};
    use serde_json::{Value, from_value, json, to_value};

    fn label() -> SecurityLabel {
        "internal/pii".parse().unwrap()
    }

    fn untrusted_user_metadata() -> ContentMetadata {
        ContentMetadata::untrusted(ContentSource::User, label())
    }

    fn schema_reference() -> SchemaReference {
        SchemaReference::new(
            "https://schemas.example.com/content/1.0.0"
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(b"content schema"),
        )
    }

    #[test]
    fn language_tags_use_stable_case_insensitive_wire_text() {
        for (input, expected) in [
            ("en-US", "en-us"),
            ("ZH-Hant-TW", "zh-hant-tw"),
            ("i-KLINGON", "i-klingon"),
            ("X-PRIVATE-42", "x-private-42"),
        ] {
            let tag = input.parse::<LanguageTag>().unwrap();
            assert_eq!(tag.as_str(), expected);
            assert_eq!(tag.to_string(), expected);
            assert_eq!(to_value(&tag).unwrap(), Value::from(expected));
        }

        assert_eq!(
            "en-us".parse::<LanguageTag>().unwrap(),
            "EN-US".parse::<LanguageTag>().unwrap()
        );

        let mut private_subtags = vec!["aaaaaaaa"; 28];
        private_subtags.push("a");
        let maximum = format!("x-{}", private_subtags.join("-"));
        assert_eq!(maximum.len(), LanguageTag::MAX_LEN);
        assert_eq!(maximum.parse::<LanguageTag>().unwrap().as_str(), maximum);
    }

    #[test]
    fn language_tags_reject_invalid_or_ambiguous_text() {
        assert_eq!("".parse::<LanguageTag>(), Err(LanguageTagError::Empty));
        assert_eq!(
            "a".repeat(LanguageTag::MAX_LEN + 1).parse::<LanguageTag>(),
            Err(LanguageTagError::TooLong {
                max: LanguageTag::MAX_LEN,
                actual: LanguageTag::MAX_LEN + 1,
            })
        );

        for invalid in ["en_US", "en--US", "é", "x-", "*"] {
            assert_eq!(
                invalid.parse::<LanguageTag>(),
                Err(LanguageTagError::InvalidSyntax),
                "accepted {invalid:?}"
            );
        }

        assert_eq!(
            "sl-rozaj-ROZAJ".parse::<LanguageTag>(),
            Err(LanguageTagError::DuplicateVariant)
        );
        assert_eq!(
            "en-a-foo-A-bar".parse::<LanguageTag>(),
            Err(LanguageTagError::DuplicateExtension)
        );
    }

    #[test]
    fn language_tag_serde_and_schema_revalidate_input() {
        let tag = from_value::<LanguageTag>(json!("FR-ca")).unwrap();
        assert_eq!(tag.as_str(), "fr-ca");
        assert!(from_value::<LanguageTag>(json!(42)).is_err());
        assert!(from_value::<LanguageTag>(json!("en_a")).is_err());

        let schema = to_value(schemars::schema_for!(LanguageTag)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 1);
        assert_eq!(schema["maxLength"], LanguageTag::MAX_LEN);
        assert_eq!(schema["pattern"], LANGUAGE_TAG_PATTERN);
    }

    #[test]
    fn security_labels_preserve_case_and_exact_spelling() {
        for value in ["public", "Internal/PII", "tenant:42/restricted-v1", "A_b.c"] {
            let label = value.parse::<SecurityLabel>().unwrap();
            assert_eq!(label.as_str(), value);
            assert_eq!(to_value(&label).unwrap(), Value::from(value));
        }

        assert_ne!(
            "internal".parse::<SecurityLabel>().unwrap(),
            "Internal".parse::<SecurityLabel>().unwrap()
        );

        let maximum = "a".repeat(SecurityLabel::MAX_LEN);
        assert_eq!(maximum.parse::<SecurityLabel>().unwrap().as_str(), maximum);
    }

    #[test]
    fn security_labels_reject_values_outside_the_opaque_grammar() {
        assert_eq!("".parse::<SecurityLabel>(), Err(SecurityLabelError::Empty));
        assert_eq!(
            "a".repeat(SecurityLabel::MAX_LEN + 1)
                .parse::<SecurityLabel>(),
            Err(SecurityLabelError::TooLong {
                max: SecurityLabel::MAX_LEN,
                actual: SecurityLabel::MAX_LEN + 1,
            })
        );

        for value in ["_internal", "-public", "/root", " PII"] {
            assert_eq!(
                value.parse::<SecurityLabel>(),
                Err(SecurityLabelError::InvalidStart),
                "accepted {value:?}"
            );
        }

        for (value, index) in [("a b", 1), ("a\\b", 1), ("a?b", 1), ("a中", 1)] {
            assert_eq!(
                value.parse::<SecurityLabel>(),
                Err(SecurityLabelError::InvalidByte { index }),
                "accepted {value:?}"
            );
        }
    }

    #[test]
    fn security_label_serde_and_schema_enforce_the_wire_contract() {
        let label = from_value::<SecurityLabel>(json!("tenant:42/PII")).unwrap();
        assert_eq!(label.as_str(), "tenant:42/PII");
        assert!(from_value::<SecurityLabel>(json!(null)).is_err());
        assert!(from_value::<SecurityLabel>(json!("bad label")).is_err());

        let schema = to_value(schemars::schema_for!(SecurityLabel)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 1);
        assert_eq!(schema["maxLength"], SecurityLabel::MAX_LEN);
        assert_eq!(schema["pattern"], SECURITY_LABEL_PATTERN);
    }

    #[test]
    fn content_security_metadata_has_exact_closed_wire_values() {
        let metadata = ContentMetadata::new(
            ContentSource::RemoteAgent,
            ContentTrust::Untrusted,
            label(),
            RedactionState::Partial,
        );
        let expected = json!({
            "source": "remote_agent",
            "trust": "untrusted",
            "security_label": "internal/pii",
            "redaction": "partial"
        });
        assert_eq!(to_value(&metadata).unwrap(), expected);
        assert_eq!(from_value::<ContentMetadata>(expected).unwrap(), metadata);

        for invalid in [
            json!({
                "source": "remote_agent",
                "trust": "trusted",
                "security_label": "internal/pii",
                "redaction": "partial"
            }),
            json!({
                "source": "remote_agent",
                "trust": "untrusted",
                "security_label": "internal/pii",
                "redaction": "partial",
                "extra": true
            }),
        ] {
            assert!(from_value::<ContentMetadata>(invalid).is_err());
        }

        let asserted = from_value::<ContentMetadata>(json!({
            "source": "remote_agent",
            "trust": "application_controlled",
            "security_label": "internal/pii",
            "redaction": "not_applied"
        }))
        .unwrap();
        assert_eq!(asserted.trust(), ContentTrust::ApplicationControlled);

        let schema = to_value(schemars::schema_for!(ContentMetadata)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["required"],
            json!(["source", "trust", "security_label", "redaction"])
        );
    }

    #[test]
    fn text_content_preserves_exact_unicode_without_normalization() {
        let composed = TextContent::new(
            "Café\n中文\tمرحبا",
            Some("FR-ca".parse().unwrap()),
            untrusted_user_metadata(),
        )
        .unwrap();
        assert_eq!(composed.text(), "Café\n中文\tمرحبا");
        assert_eq!(composed.language().unwrap().as_str(), "fr-ca");

        let decomposed = TextContent::new("Cafe\u{301}", None, untrusted_user_metadata()).unwrap();
        let normalized = TextContent::new("Café", None, untrusted_user_metadata()).unwrap();
        assert_ne!(decomposed.text(), normalized.text());
    }

    #[test]
    fn text_content_enforces_byte_and_scalar_limits() {
        assert_eq!(
            TextContent::new("", None, untrusted_user_metadata()),
            Err(TextContentError::Empty)
        );

        let maximum = "a".repeat(TextContent::MAX_BYTES);
        assert!(TextContent::new(&maximum, None, untrusted_user_metadata()).is_ok());
        let too_long = format!("{maximum}a");
        assert_eq!(
            TextContent::new(&too_long, None, untrusted_user_metadata()),
            Err(TextContentError::TooLong {
                max: TextContent::MAX_BYTES,
                actual: TextContent::MAX_BYTES + 1,
            })
        );

        let multi_byte_too_long = "界".repeat(TextContent::MAX_BYTES / 3 + 1);
        assert!(matches!(
            TextContent::new(&multi_byte_too_long, None, untrusted_user_metadata()),
            Err(TextContentError::TooLong { .. })
        ));

        for (value, byte_index) in [
            ("a\0b", 1),
            ("a\u{1b}b", 1),
            ("a\u{7f}b", 1),
            ("a\u{80}b", 1),
            ("界\u{fdd0}", 3),
            ("a\u{fffe}", 1),
            ("a\u{10ffff}", 1),
        ] {
            assert_eq!(
                TextContent::new(value, None, untrusted_user_metadata()),
                Err(TextContentError::DisallowedCodePoint { byte_index }),
                "accepted {value:?}"
            );
        }

        assert!(TextContent::new("a\tb\nc\rd", None, untrusted_user_metadata()).is_ok());
    }

    #[test]
    fn text_content_rejects_every_c0_c1_control_and_unicode_noncharacter() {
        for code_point in (0_u32..=0x1f).chain(0x7f..=0x9f) {
            let scalar = char::from_u32(code_point).unwrap();
            let value = format!("a{scalar}b");
            let result = TextContent::new(&value, None, untrusted_user_metadata());
            if matches!(scalar, '\t' | '\n' | '\r') {
                assert!(result.is_ok(), "rejected allowed U+{code_point:04X}");
            } else {
                assert_eq!(
                    result,
                    Err(TextContentError::DisallowedCodePoint { byte_index: 1 }),
                    "accepted U+{code_point:04X}"
                );
            }
        }

        let mut noncharacters: Vec<u32> = (0xfdd0..=0xfdef).collect();
        for plane in 0_u32..=0x10 {
            noncharacters.push(plane * 0x1_0000 + 0xfffe);
            noncharacters.push(plane * 0x1_0000 + 0xffff);
        }

        for code_point in noncharacters {
            let scalar = char::from_u32(code_point).unwrap();
            let value = format!("a{scalar}");
            assert_eq!(
                TextContent::new(&value, None, untrusted_user_metadata()),
                Err(TextContentError::DisallowedCodePoint { byte_index: 1 }),
                "accepted U+{code_point:04X}"
            );
        }
    }

    #[test]
    fn text_content_serde_revalidates_and_debug_redacts_text() {
        let expected = json!({
            "text": "secret-token-42",
            "language": "en-us",
            "metadata": {
                "source": "user",
                "trust": "untrusted",
                "security_label": "internal/pii",
                "redaction": "not_applied"
            }
        });
        let content = from_value::<TextContent>(expected.clone()).unwrap();
        assert_eq!(to_value(&content).unwrap(), expected);

        let debug = format!("{content:?}");
        assert!(debug.contains("text_bytes: 15"));
        assert!(!debug.contains("secret-token-42"));

        for invalid in [
            json!({
                "text": "bad\u{0}",
                "metadata": {
                    "source": "user",
                    "trust": "untrusted",
                    "security_label": "internal/pii",
                    "redaction": "not_applied"
                }
            }),
            json!({
                "text": "valid",
                "metadata": {
                    "source": "user",
                    "trust": "untrusted",
                    "security_label": "internal/pii",
                    "redaction": "not_applied"
                },
                "extra": true
            }),
        ] {
            assert!(from_value::<TextContent>(invalid).is_err());
        }
    }

    #[test]
    fn text_content_schema_is_closed_and_documents_runtime_bounds() {
        let schema = to_value(schemars::schema_for!(TextContent)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"], json!(["text", "metadata"]));
        assert_eq!(schema["properties"]["text"]["minLength"], 1);
        assert_eq!(
            schema["properties"]["text"]["maxLength"],
            TextContent::MAX_BYTES
        );
        assert!(
            schema["properties"]["text"]["description"]
                .as_str()
                .unwrap()
                .contains("runtime")
        );
    }

    #[test]
    fn json_content_round_trips_with_optional_schema_and_safe_debug() {
        let value =
            BoundedJson::from_str(r#"{"api_token_7391":"sensitive-payload-7391","count":2}"#)
                .unwrap();
        let content = JsonContent::new(
            value,
            Some(schema_reference()),
            ContentMetadata::untrusted(ContentSource::Tool, label()),
        );
        let encoded = to_value(&content).unwrap();
        assert_eq!(
            encoded["value"],
            json!({"api_token_7391": "sensitive-payload-7391", "count": 2})
        );
        assert!(encoded.get("schema").is_some());
        assert_eq!(from_value::<JsonContent>(encoded).unwrap(), content);

        let debug = format!("{content:?}");
        assert!(debug.contains("compact_bytes"));
        assert!(!debug.contains("api_token_7391"));
        assert!(!debug.contains("sensitive-payload-7391"));

        let without_schema = JsonContent::new(
            BoundedJson::from_str("true").unwrap(),
            None,
            untrusted_user_metadata(),
        );
        assert!(to_value(without_schema).unwrap().get("schema").is_none());
    }

    #[test]
    fn json_content_wire_object_is_closed_and_bounded() {
        let metadata = json!({
            "source": "tool",
            "trust": "untrusted",
            "security_label": "internal/pii",
            "redaction": "not_applied"
        });
        assert!(
            from_value::<JsonContent>(json!({
                "value": true,
                "metadata": metadata,
                "extra": 1
            }))
            .is_err()
        );

        let oversized = "x".repeat(crate::JsonLimits::DEFAULT.max_string_bytes() + 1);
        assert!(
            from_value::<JsonContent>(json!({
                "value": oversized,
                "metadata": {
                    "source": "tool",
                    "trust": "untrusted",
                    "security_label": "internal/pii",
                    "redaction": "not_applied"
                }
            }))
            .is_err()
        );

        let schema = to_value(schemars::schema_for!(JsonContent)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"], json!(["value", "metadata"]));
    }

    fn arbitrary_safe_text() -> impl Strategy<Value = String> {
        collection::vec(
            any::<char>().prop_filter("allowed text scalar", |scalar| {
                (!scalar.is_control() || matches!(scalar, '\t' | '\n' | '\r'))
                    && !is_unicode_noncharacter(*scalar)
            }),
            1..128,
        )
        .prop_map(|scalars| scalars.into_iter().collect())
    }

    proptest! {
        #[test]
        fn valid_text_round_trips_without_unicode_or_byte_changes(text in arbitrary_safe_text()) {
            let content = TextContent::new(&text, None, untrusted_user_metadata()).unwrap();
            let encoded = to_value(&content).unwrap();
            let decoded = from_value::<TextContent>(encoded).unwrap();

            prop_assert_eq!(decoded.text().as_bytes(), text.as_bytes());
            prop_assert_eq!(decoded, content);
        }
    }
}
