// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Stable names for executable capabilities.

use std::{borrow::Borrow, fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{Extensions, PrincipalIdentity, ScopeSet, Timestamp, Version};

const CAPABILITY_NAME_PATTERN: &str = "^[A-Za-z0-9_.-]{1,128}$";
const KIBIBYTE: usize = 1024;

/// A stable, case-sensitive capability name.
///
/// Names contain 1 to 128 ASCII letters, digits, `_`, `-`, or `.`. This is
/// the tool-name grammar from the MCP 2026-07-28 specification, made
/// mandatory at the `StateKnot` domain boundary. A name is unique only within
/// its owning registry; owner/provenance remains a separate identity field.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CapabilityName(Box<str>);

impl CapabilityName {
    /// Maximum encoded length in bytes.
    pub const MAX_LEN: usize = 128;

    /// Validates and constructs a capability name without copying a `String`.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityNameError`] when `value` is empty, too long, or
    /// contains a byte outside the stable ASCII grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, CapabilityNameError> {
        let value = value.into();
        validate_capability_name(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact, case-sensitive name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for CapabilityName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for CapabilityName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for CapabilityName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CapabilityName")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for CapabilityName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CapabilityName {
    type Err = CapabilityNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for CapabilityName {
    type Error = CapabilityNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for CapabilityName {
    type Error = CapabilityNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CapabilityName> for String {
    fn from(value: CapabilityName) -> Self {
        value.0.into()
    }
}

impl Serialize for CapabilityName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CapabilityName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(CapabilityNameVisitor)
    }
}

struct CapabilityNameVisitor;

impl de::Visitor<'_> for CapabilityNameVisitor {
    type Value = CapabilityName;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical StateKnot capability name")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CapabilityName::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CapabilityName::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for CapabilityName {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CapabilityName".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::CapabilityName").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 128,
            "pattern": CAPABILITY_NAME_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for a [`CapabilityName`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CapabilityNameError {
    /// The name contained no bytes.
    #[error("capability name must not be empty")]
    Empty,

    /// The name exceeded [`CapabilityName::MAX_LEN`].
    #[error("capability name is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// A byte did not belong to the allowed ASCII grammar.
    #[error("capability name contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first invalid byte.
        index: usize,
    },
}

/// A version-pinned reference to a registered capability.
///
/// Names are unique only within their owning registry. Durable provenance must
/// therefore pair this value with the owning principal or registry identity.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct CapabilityReference {
    name: CapabilityName,
    version: Version,
}

impl CapabilityReference {
    /// Constructs a reference from validated components.
    #[must_use]
    pub const fn new(name: CapabilityName, version: Version) -> Self {
        Self { name, version }
    }

    /// Returns the registry-local capability name.
    #[must_use]
    pub const fn name(&self) -> &CapabilityName {
        &self.name
    }

    /// Returns the pinned capability version.
    #[must_use]
    pub const fn version(&self) -> Version {
        self.version
    }
}

/// An owner-qualified, version-pinned capability identity.
///
/// [`CapabilityName`] is registry-local, so durable references use this pair
/// whenever a surrounding record does not already pin the owning registry.
/// The serialized owner is an auditable claim, not authentication or proof
/// that the capability is registered.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct CapabilityIdentity {
    owner: PrincipalIdentity,
    capability: CapabilityReference,
}

impl CapabilityIdentity {
    /// Constructs an owner-qualified identity from validated components.
    #[must_use]
    pub const fn new(owner: PrincipalIdentity, capability: CapabilityReference) -> Self {
        Self { owner, capability }
    }

    /// Returns the principal owning the registry namespace.
    #[must_use]
    pub const fn owner(&self) -> &PrincipalIdentity {
        &self.owner
    }

    /// Returns the registry-local, version-pinned reference.
    #[must_use]
    pub const fn capability(&self) -> &CapabilityReference {
        &self.capability
    }

    /// Returns the registry-local capability name.
    #[must_use]
    pub const fn name(&self) -> &CapabilityName {
        self.capability.name()
    }

    /// Returns the pinned capability version.
    #[must_use]
    pub const fn version(&self) -> Version {
        self.capability.version()
    }
}

/// A bounded single-line display title for a capability.
///
/// Titles preserve exact UTF-8, reject boundary whitespace and formatting
/// controls that make audit or UI rendering ambiguous, and redact their text
/// from `Debug`. Renderers must still perform output-context escaping.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CapabilityTitle(Box<str>);

impl CapabilityTitle {
    /// Maximum UTF-8 encoded length in bytes.
    pub const MAX_BYTES: usize = 256;

    /// Validates and constructs a capability title.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityTitleError`] for empty, oversized,
    /// whitespace-ambiguous, control-bearing, or noncharacter-bearing text.
    pub fn new(value: impl Into<String>) -> Result<Self, CapabilityTitleError> {
        let value = value.into();
        validate_capability_title(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact title text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the UTF-8 encoded length without disclosing the title.
    #[must_use]
    pub fn len_bytes(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for CapabilityTitle {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for CapabilityTitle {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for CapabilityTitle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityTitle")
            .field("utf8_bytes", &self.len_bytes())
            .finish_non_exhaustive()
    }
}

impl FromStr for CapabilityTitle {
    type Err = CapabilityTitleError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for CapabilityTitle {
    type Error = CapabilityTitleError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for CapabilityTitle {
    type Error = CapabilityTitleError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CapabilityTitle> for String {
    fn from(value: CapabilityTitle) -> Self {
        value.0.into()
    }
}

impl Serialize for CapabilityTitle {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CapabilityTitle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(CapabilityTitleVisitor)
    }
}

struct CapabilityTitleVisitor;

impl de::Visitor<'_> for CapabilityTitleVisitor {
    type Value = CapabilityTitle;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded single-line capability title")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CapabilityTitle::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CapabilityTitle::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for CapabilityTitle {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CapabilityTitle".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::CapabilityTitle").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 256,
            "description": "A single-line display title bounded to 256 UTF-8 bytes. StateKnot additionally rejects boundary whitespace, controls, bidi formatting controls, Unicode line separators, and noncharacters at runtime."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid capability title text.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CapabilityTitleError {
    /// The title contained no bytes.
    #[error("capability title must not be empty")]
    Empty,

    /// The title exceeded [`CapabilityTitle::MAX_BYTES`].
    #[error("capability title is {actual} UTF-8 bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted UTF-8 byte length.
        max: usize,
        /// Observed UTF-8 byte length.
        actual: usize,
    },

    /// The title began or ended with Unicode whitespace.
    #[error("capability title must not have leading or trailing whitespace")]
    BoundaryWhitespace,

    /// A control, bidi formatting control, line separator, or noncharacter was present.
    #[error("capability title contains a disallowed scalar at UTF-8 byte offset {byte_index}")]
    DisallowedCodePoint {
        /// Zero-based UTF-8 byte offset without disclosure of title text.
        byte_index: usize,
    },
}

fn validate_capability_title(value: &str) -> Result<(), CapabilityTitleError> {
    if value.is_empty() {
        return Err(CapabilityTitleError::Empty);
    }
    if value.len() > CapabilityTitle::MAX_BYTES {
        return Err(CapabilityTitleError::TooLong {
            max: CapabilityTitle::MAX_BYTES,
            actual: value.len(),
        });
    }
    if value.chars().next().is_some_and(char::is_whitespace)
        || value.chars().next_back().is_some_and(char::is_whitespace)
    {
        return Err(CapabilityTitleError::BoundaryWhitespace);
    }
    if let Some((byte_index, _)) = value.char_indices().find(|(_, scalar)| {
        scalar.is_control()
            || is_bidi_formatting_control(*scalar)
            || matches!(scalar, '\u{2028}' | '\u{2029}')
            || is_unicode_noncharacter(*scalar)
    }) {
        return Err(CapabilityTitleError::DisallowedCodePoint { byte_index });
    }
    Ok(())
}

/// Bounded trusted-registry description text for a capability.
///
/// Descriptions preserve exact UTF-8 and may contain internal tabs and line
/// breaks. Text is redacted from `Debug` because descriptions are commonly
/// copied into model context. Shape validation does not make remote descriptor
/// text trusted; only an authenticated, policy-approved registry may select it
/// for discovery or prompt construction.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CapabilityDescription(Box<str>);

impl CapabilityDescription {
    /// Maximum UTF-8 encoded length in bytes.
    pub const MAX_BYTES: usize = 16 * KIBIBYTE;

    /// Validates and constructs a capability description.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityDescriptionError`] for empty, oversized,
    /// whitespace-ambiguous, control-bearing, or noncharacter-bearing text.
    pub fn new(value: impl Into<String>) -> Result<Self, CapabilityDescriptionError> {
        let value = value.into();
        validate_capability_description(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact description text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the UTF-8 encoded length without disclosing the description.
    #[must_use]
    pub fn len_bytes(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for CapabilityDescription {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for CapabilityDescription {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for CapabilityDescription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityDescription")
            .field("utf8_bytes", &self.len_bytes())
            .finish_non_exhaustive()
    }
}

impl FromStr for CapabilityDescription {
    type Err = CapabilityDescriptionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for CapabilityDescription {
    type Error = CapabilityDescriptionError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for CapabilityDescription {
    type Error = CapabilityDescriptionError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CapabilityDescription> for String {
    fn from(value: CapabilityDescription) -> Self {
        value.0.into()
    }
}

impl Serialize for CapabilityDescription {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CapabilityDescription {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(CapabilityDescriptionVisitor)
    }
}

struct CapabilityDescriptionVisitor;

impl de::Visitor<'_> for CapabilityDescriptionVisitor {
    type Value = CapabilityDescription;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded capability description")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CapabilityDescription::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CapabilityDescription::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for CapabilityDescription {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CapabilityDescription".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::CapabilityDescription").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 16384,
            "description": "Exact description text bounded to 16384 UTF-8 bytes. StateKnot additionally rejects boundary whitespace, disallowed controls, bidi formatting controls, and Unicode noncharacters at runtime. Shape validation does not establish descriptor trust."
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Invalid capability description text.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CapabilityDescriptionError {
    /// The description contained no bytes.
    #[error("capability description must not be empty")]
    Empty,

    /// The description exceeded [`CapabilityDescription::MAX_BYTES`].
    #[error("capability description is {actual} UTF-8 bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted UTF-8 byte length.
        max: usize,
        /// Observed UTF-8 byte length.
        actual: usize,
    },

    /// The description began or ended with Unicode whitespace.
    #[error("capability description must not have leading or trailing whitespace")]
    BoundaryWhitespace,

    /// A disallowed control, bidi formatting control, or noncharacter was present.
    #[error(
        "capability description contains a disallowed scalar at UTF-8 byte offset {byte_index}"
    )]
    DisallowedCodePoint {
        /// Zero-based UTF-8 byte offset without disclosure of description text.
        byte_index: usize,
    },
}

fn validate_capability_description(value: &str) -> Result<(), CapabilityDescriptionError> {
    if value.is_empty() {
        return Err(CapabilityDescriptionError::Empty);
    }
    if value.len() > CapabilityDescription::MAX_BYTES {
        return Err(CapabilityDescriptionError::TooLong {
            max: CapabilityDescription::MAX_BYTES,
            actual: value.len(),
        });
    }
    if value.chars().next().is_some_and(char::is_whitespace)
        || value.chars().next_back().is_some_and(char::is_whitespace)
    {
        return Err(CapabilityDescriptionError::BoundaryWhitespace);
    }
    if let Some((byte_index, _)) = value.char_indices().find(|(_, scalar)| {
        (scalar.is_control() && !matches!(scalar, '\t' | '\n' | '\r'))
            || is_bidi_formatting_control(*scalar)
            || is_unicode_noncharacter(*scalar)
    }) {
        return Err(CapabilityDescriptionError::DisallowedCodePoint { byte_index });
    }
    Ok(())
}

const fn is_unicode_noncharacter(value: char) -> bool {
    let value = value as u32;
    (value >= 0xfdd0 && value <= 0xfdef) || (value & 0xfffe) == 0xfffe
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

/// Stable classification of a discoverable capability.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    /// A model invocation capability.
    Model,
    /// A callable tool capability.
    Tool,
    /// A collaborating local or remote agent capability.
    Agent,
    /// A versioned workflow or graph capability.
    Workflow,
    /// An application-defined non-tool producer capability.
    Application,
}

/// Stable lifecycle classification for a capability version.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CapabilityLifecycleState {
    /// Available without a published retirement notice.
    Active,
    /// Still represented but carrying a migration notice.
    Deprecated,
    /// Retained for history but unavailable for new execution.
    Retired,
}

#[derive(Clone, Eq, Hash, PartialEq)]
enum CapabilityLifecycleValue {
    Active,
    Deprecated {
        announced_at: Timestamp,
        sunset_at: Option<Timestamp>,
        notice: CapabilityDescription,
        replacement: Option<CapabilityIdentity>,
    },
    Retired {
        retired_at: Timestamp,
        notice: CapabilityDescription,
        replacement: Option<CapabilityIdentity>,
    },
}

/// Validated lifecycle metadata for one pinned capability version.
///
/// Deprecation does not itself disable execution, and a future sunset does not
/// act as a clock-driven policy. A registry snapshots and enforces availability
/// separately. `Retired` records remain decodable for audit and recovery but
/// must not be selected for new execution.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct CapabilityLifecycle(CapabilityLifecycleValue);

impl CapabilityLifecycle {
    /// Constructs an active lifecycle.
    #[must_use]
    pub const fn active() -> Self {
        Self(CapabilityLifecycleValue::Active)
    }

    /// Constructs a deprecated lifecycle with migration guidance.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityLifecycleError`] when a sunset is not strictly
    /// later than the deprecation announcement.
    pub fn deprecated(
        announced_at: Timestamp,
        sunset_at: Option<Timestamp>,
        notice: CapabilityDescription,
        replacement: Option<CapabilityIdentity>,
    ) -> Result<Self, CapabilityLifecycleError> {
        if let Some(sunset_at) = sunset_at {
            if sunset_at <= announced_at {
                return Err(CapabilityLifecycleError::InvalidSunsetOrder {
                    announced_at,
                    sunset_at,
                });
            }
        }
        Ok(Self(CapabilityLifecycleValue::Deprecated {
            announced_at,
            sunset_at,
            notice,
            replacement,
        }))
    }

    /// Constructs a retired lifecycle retained for durable history.
    #[must_use]
    pub fn retired(
        retired_at: Timestamp,
        notice: CapabilityDescription,
        replacement: Option<CapabilityIdentity>,
    ) -> Self {
        Self(CapabilityLifecycleValue::Retired {
            retired_at,
            notice,
            replacement,
        })
    }

    /// Returns the stable lifecycle state.
    #[must_use]
    pub const fn state(&self) -> CapabilityLifecycleState {
        match self.0 {
            CapabilityLifecycleValue::Active => CapabilityLifecycleState::Active,
            CapabilityLifecycleValue::Deprecated { .. } => CapabilityLifecycleState::Deprecated,
            CapabilityLifecycleValue::Retired { .. } => CapabilityLifecycleState::Retired,
        }
    }

    /// Returns the deprecation announcement time when applicable.
    #[must_use]
    pub const fn announced_at(&self) -> Option<Timestamp> {
        match self.0 {
            CapabilityLifecycleValue::Deprecated { announced_at, .. } => Some(announced_at),
            CapabilityLifecycleValue::Active | CapabilityLifecycleValue::Retired { .. } => None,
        }
    }

    /// Returns the optional published sunset time.
    #[must_use]
    pub const fn sunset_at(&self) -> Option<Timestamp> {
        match self.0 {
            CapabilityLifecycleValue::Deprecated { sunset_at, .. } => sunset_at,
            CapabilityLifecycleValue::Active | CapabilityLifecycleValue::Retired { .. } => None,
        }
    }

    /// Returns the retirement time when applicable.
    #[must_use]
    pub const fn retired_at(&self) -> Option<Timestamp> {
        match self.0 {
            CapabilityLifecycleValue::Retired { retired_at, .. } => Some(retired_at),
            CapabilityLifecycleValue::Active | CapabilityLifecycleValue::Deprecated { .. } => None,
        }
    }

    /// Returns migration guidance when deprecated or retired.
    #[must_use]
    pub const fn notice(&self) -> Option<&CapabilityDescription> {
        match &self.0 {
            CapabilityLifecycleValue::Deprecated { notice, .. }
            | CapabilityLifecycleValue::Retired { notice, .. } => Some(notice),
            CapabilityLifecycleValue::Active => None,
        }
    }

    /// Returns the optional owner-qualified replacement capability.
    #[must_use]
    pub const fn replacement(&self) -> Option<&CapabilityIdentity> {
        match &self.0 {
            CapabilityLifecycleValue::Deprecated { replacement, .. }
            | CapabilityLifecycleValue::Retired { replacement, .. } => replacement.as_ref(),
            CapabilityLifecycleValue::Active => None,
        }
    }
}

impl Default for CapabilityLifecycle {
    fn default() -> Self {
        Self::active()
    }
}

impl fmt::Debug for CapabilityLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("CapabilityLifecycle");
        debug.field("state", &self.state());
        if let Some(announced_at) = self.announced_at() {
            debug.field("announced_at", &announced_at);
        }
        if let Some(sunset_at) = self.sunset_at() {
            debug.field("sunset_at", &sunset_at);
        }
        if let Some(retired_at) = self.retired_at() {
            debug.field("retired_at", &retired_at);
        }
        debug
            .field("has_notice", &self.notice().is_some())
            .field("has_replacement", &self.replacement().is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum CapabilityLifecycleWire {
    Active {},
    Deprecated {
        announced_at: Timestamp,
        #[serde(default)]
        sunset_at: Option<Timestamp>,
        notice: CapabilityDescription,
        #[serde(default)]
        replacement: Option<CapabilityIdentity>,
    },
    Retired {
        retired_at: Timestamp,
        notice: CapabilityDescription,
        #[serde(default)]
        replacement: Option<CapabilityIdentity>,
    },
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum CapabilityLifecycleWireRef<'a> {
    Active {},
    Deprecated {
        announced_at: Timestamp,
        #[serde(skip_serializing_if = "Option::is_none")]
        sunset_at: Option<Timestamp>,
        notice: &'a CapabilityDescription,
        #[serde(skip_serializing_if = "Option::is_none")]
        replacement: Option<&'a CapabilityIdentity>,
    },
    Retired {
        retired_at: Timestamp,
        notice: &'a CapabilityDescription,
        #[serde(skip_serializing_if = "Option::is_none")]
        replacement: Option<&'a CapabilityIdentity>,
    },
}

impl Serialize for CapabilityLifecycle {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.0 {
            CapabilityLifecycleValue::Active => CapabilityLifecycleWireRef::Active {},
            CapabilityLifecycleValue::Deprecated {
                announced_at,
                sunset_at,
                notice,
                replacement,
            } => CapabilityLifecycleWireRef::Deprecated {
                announced_at: *announced_at,
                sunset_at: *sunset_at,
                notice,
                replacement: replacement.as_ref(),
            },
            CapabilityLifecycleValue::Retired {
                retired_at,
                notice,
                replacement,
            } => CapabilityLifecycleWireRef::Retired {
                retired_at: *retired_at,
                notice,
                replacement: replacement.as_ref(),
            },
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CapabilityLifecycle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match CapabilityLifecycleWire::deserialize(deserializer)? {
            CapabilityLifecycleWire::Active {} => Ok(Self::active()),
            CapabilityLifecycleWire::Deprecated {
                announced_at,
                sunset_at,
                notice,
                replacement,
            } => Self::deprecated(announced_at, sunset_at, notice, replacement)
                .map_err(de::Error::custom),
            CapabilityLifecycleWire::Retired {
                retired_at,
                notice,
                replacement,
            } => Ok(Self::retired(retired_at, notice, replacement)),
        }
    }
}

impl JsonSchema for CapabilityLifecycle {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CapabilityLifecycle".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::CapabilityLifecycle").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        CapabilityLifecycleWire::json_schema(generator)
    }
}

/// Invalid capability lifecycle metadata.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CapabilityLifecycleError {
    /// A sunset did not follow its deprecation announcement.
    #[error("capability sunset {sunset_at} must be later than announcement {announced_at}")]
    InvalidSunsetOrder {
        /// Time at which deprecation was announced.
        announced_at: Timestamp,
        /// Invalid proposed sunset time.
        sunset_at: Timestamp,
    },
}

/// Common discovery metadata shared by specialized capability descriptors.
///
/// Model modalities, tool schemas, risk, and execution limits intentionally do
/// not live here; specialized descriptors make those required and type-safe.
/// The owner, lifecycle, scopes, and extensions are claims until a trusted
/// tenant registry authenticates the owner, pins this exact version, applies
/// policy, and snapshots the resulting descriptor for an execution attempt.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityMetadata {
    identity: CapabilityIdentity,
    kind: CapabilityKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<CapabilityTitle>,
    description: CapabilityDescription,
    lifecycle: CapabilityLifecycle,
    required_scopes: ScopeSet,
    extensions: Extensions,
}

impl CapabilityMetadata {
    /// Constructs common metadata and checks cross-field lifecycle invariants.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityMetadataError`] when the lifecycle names the
    /// capability itself as its replacement.
    pub fn new(
        identity: CapabilityIdentity,
        kind: CapabilityKind,
        title: Option<CapabilityTitle>,
        description: CapabilityDescription,
        lifecycle: CapabilityLifecycle,
        required_scopes: ScopeSet,
        extensions: Extensions,
    ) -> Result<Self, CapabilityMetadataError> {
        if lifecycle.replacement() == Some(&identity) {
            return Err(CapabilityMetadataError::ReplacementIsSelf);
        }
        Ok(Self {
            identity,
            kind,
            title,
            description,
            lifecycle,
            required_scopes,
            extensions,
        })
    }

    /// Returns the owner-qualified, version-pinned identity.
    #[must_use]
    pub const fn identity(&self) -> &CapabilityIdentity {
        &self.identity
    }

    /// Returns the specialized descriptor classification.
    #[must_use]
    pub const fn kind(&self) -> CapabilityKind {
        self.kind
    }

    /// Returns the optional human-readable display title.
    #[must_use]
    pub const fn title(&self) -> Option<&CapabilityTitle> {
        self.title.as_ref()
    }

    /// Returns the exact registry-approved description text.
    #[must_use]
    pub const fn description(&self) -> &CapabilityDescription {
        &self.description
    }

    /// Returns the validated lifecycle metadata.
    #[must_use]
    pub const fn lifecycle(&self) -> &CapabilityLifecycle {
        &self.lifecycle
    }

    /// Returns scopes required before this capability can be selected.
    #[must_use]
    pub const fn required_scopes(&self) -> &ScopeSet {
        &self.required_scopes
    }

    /// Returns bounded protocol/provider extension data.
    #[must_use]
    pub const fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl fmt::Debug for CapabilityMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityMetadata")
            .field("identity", &self.identity)
            .field("kind", &self.kind)
            .field("has_title", &self.title.is_some())
            .field("description_utf8_bytes", &self.description.len_bytes())
            .field("lifecycle", &self.lifecycle)
            .field("required_scopes", &self.required_scopes.len())
            .field("extensions", &self.extensions)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityMetadataWire {
    identity: CapabilityIdentity,
    kind: CapabilityKind,
    #[serde(default)]
    title: Option<CapabilityTitle>,
    description: CapabilityDescription,
    lifecycle: CapabilityLifecycle,
    required_scopes: ScopeSet,
    extensions: Extensions,
}

impl<'de> Deserialize<'de> for CapabilityMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CapabilityMetadataWire::deserialize(deserializer)?;
        Self::new(
            wire.identity,
            wire.kind,
            wire.title,
            wire.description,
            wire.lifecycle,
            wire.required_scopes,
            wire.extensions,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid cross-field capability metadata.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CapabilityMetadataError {
    /// A deprecated or retired capability pointed to itself as replacement.
    #[error("capability lifecycle replacement must not equal its own identity")]
    ReplacementIsSelf,
}

fn validate_capability_name(value: &str) -> Result<(), CapabilityNameError> {
    if value.is_empty() {
        return Err(CapabilityNameError::Empty);
    }
    if value.len() > CapabilityName::MAX_LEN {
        return Err(CapabilityNameError::TooLong {
            max: CapabilityName::MAX_LEN,
            actual: value.len(),
        });
    }

    if let Some((index, _)) = value
        .bytes()
        .enumerate()
        .find(|(_, byte)| !byte.is_ascii_alphanumeric() && !matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(CapabilityNameError::InvalidByte { index });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    use crate::{BoundedJson, ExtensionKey, ExtensionValue, IssuerId, Scope, SubjectId};

    fn principal(subject: &str) -> PrincipalIdentity {
        PrincipalIdentity::new(
            "https://issuer.example.com/tenant"
                .parse::<IssuerId>()
                .unwrap(),
            subject.parse::<SubjectId>().unwrap(),
        )
    }

    fn identity(name: &str, version: Version) -> CapabilityIdentity {
        CapabilityIdentity::new(
            principal("registry-owner"),
            CapabilityReference::new(name.parse().unwrap(), version),
        )
    }

    fn timestamp(value: &str) -> Timestamp {
        value.parse().unwrap()
    }

    fn description(value: &str) -> CapabilityDescription {
        CapabilityDescription::new(value).unwrap()
    }

    fn scopes(values: &[&str]) -> ScopeSet {
        ScopeSet::try_new(values.iter().map(|value| value.parse::<Scope>().unwrap())).unwrap()
    }

    fn extensions(secret: &str) -> Extensions {
        Extensions::try_new([(
            ExtensionKey::new("com.example.discovery").unwrap(),
            ExtensionValue::opaque(
                BoundedJson::try_from_value(json!({ "secret": secret })).unwrap(),
            ),
        )])
        .unwrap()
    }

    #[test]
    fn capability_names_preserve_all_valid_spellings() {
        let all_allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-";
        assert_eq!(
            all_allowed.parse::<CapabilityName>().unwrap().as_str(),
            all_allowed
        );

        for value in [
            "getUser",
            "DATA_EXPORT_v2",
            "admin.tools.list",
            "ops.restart-service",
            ".",
            "-",
            "0",
        ] {
            let name = value.parse::<CapabilityName>().unwrap();
            assert_eq!(name.as_str(), value);
            assert_eq!(name.to_string(), value);
            assert_eq!(to_value(&name).unwrap(), Value::from(value));
        }

        let maximum = "a".repeat(CapabilityName::MAX_LEN);
        assert_eq!(maximum.parse::<CapabilityName>().unwrap().as_str(), maximum);
    }

    #[test]
    fn capability_names_are_case_sensitive() {
        let lower = "getuser".parse::<CapabilityName>().unwrap();
        let mixed = "getUser".parse::<CapabilityName>().unwrap();
        assert_ne!(lower, mixed);
    }

    #[test]
    fn capability_names_reject_out_of_contract_input() {
        assert_eq!(
            "".parse::<CapabilityName>(),
            Err(CapabilityNameError::Empty)
        );
        assert_eq!(
            "a".repeat(CapabilityName::MAX_LEN + 1)
                .parse::<CapabilityName>(),
            Err(CapabilityNameError::TooLong {
                max: CapabilityName::MAX_LEN,
                actual: CapabilityName::MAX_LEN + 1,
            })
        );

        for (value, index) in [
            ("restart service", 7),
            ("ops/restart", 3),
            ("ops:restart", 3),
            ("tool,name", 4),
            ("tool\\name", 4),
            ("tool\nname", 4),
            ("工具", 0),
        ] {
            assert_eq!(
                value.parse::<CapabilityName>(),
                Err(CapabilityNameError::InvalidByte { index }),
                "accepted {value:?}"
            );
        }

        for byte in 0_u8..=0x7f {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.') {
                continue;
            }
            let value = String::from_utf8(vec![byte]).unwrap();
            assert_eq!(
                value.parse::<CapabilityName>(),
                Err(CapabilityNameError::InvalidByte { index: 0 }),
                "accepted ASCII byte 0x{byte:02x}"
            );
        }
    }

    #[test]
    fn capability_name_serde_and_schema_enforce_the_wire_contract() {
        let name = "ops.restart-service".parse::<CapabilityName>().unwrap();
        assert_eq!(
            from_value::<CapabilityName>(json!(name.as_str())).unwrap(),
            name
        );
        assert!(from_value::<CapabilityName>(json!(42)).is_err());
        assert!(from_value::<CapabilityName>(Value::Null).is_err());

        let schema = to_value(schemars::schema_for!(CapabilityName)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 1);
        assert_eq!(schema["maxLength"], CapabilityName::MAX_LEN);
        assert_eq!(schema["pattern"], CAPABILITY_NAME_PATTERN);
    }

    #[test]
    fn capability_references_pin_name_and_version_in_a_closed_object() {
        let reference = CapabilityReference::new(
            "ops.restart-service".parse().unwrap(),
            Version::new(2, 3, 4),
        );
        let expected = json!({
            "name": "ops.restart-service",
            "version": "2.3.4"
        });
        assert_eq!(to_value(&reference).unwrap(), expected);
        assert_eq!(
            from_value::<CapabilityReference>(expected).unwrap(),
            reference
        );

        assert!(
            from_value::<CapabilityReference>(json!({
                "name": "ops.restart-service",
                "version": "2.3.4",
                "extra": true
            }))
            .is_err()
        );

        let schema = to_value(schemars::schema_for!(CapabilityReference)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"], json!(["name", "version"]));
    }

    #[test]
    fn capability_identities_are_owner_qualified_and_closed() {
        let identity = identity("payments.capture", Version::new(2, 1, 0));
        assert_eq!(identity.owner(), &principal("registry-owner"));
        assert_eq!(identity.capability().name().as_str(), "payments.capture");
        assert_eq!(identity.name().as_str(), "payments.capture");
        assert_eq!(identity.version(), Version::new(2, 1, 0));

        let encoded = to_value(&identity).unwrap();
        assert_eq!(
            encoded,
            json!({
                "owner": {
                    "issuer": "https://issuer.example.com/tenant",
                    "subject": "registry-owner"
                },
                "capability": {
                    "name": "payments.capture",
                    "version": "2.1.0"
                }
            })
        );
        assert_eq!(from_value::<CapabilityIdentity>(encoded).unwrap(), identity);

        let other_owner =
            CapabilityIdentity::new(principal("other-owner"), identity.capability().clone());
        assert_ne!(other_owner, identity);

        assert!(
            from_value::<CapabilityIdentity>(json!({
                "owner": {
                    "issuer": "https://issuer.example.com/tenant",
                    "subject": "registry-owner"
                },
                "capability": {
                    "name": "payments.capture",
                    "version": "2.1.0"
                },
                "trusted": true
            }))
            .is_err()
        );

        let schema = to_value(schemars::schema_for!(CapabilityIdentity)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"], json!(["owner", "capability"]));
    }

    #[test]
    fn capability_text_preserves_unicode_and_redacts_debug() {
        let title_secret = "财务付款工具 secret-title";
        let title = CapabilityTitle::new(title_secret).unwrap();
        assert_eq!(title.as_str(), title_secret);
        assert_eq!(title.len_bytes(), title_secret.len());
        assert!(!format!("{title:?}").contains(title_secret));
        assert_eq!(
            from_value::<CapabilityTitle>(to_value(&title).unwrap()).unwrap(),
            title
        );

        let description_secret =
            "Create a payment only after approval.\n\tNever infer the currency. secret-description";
        let description = CapabilityDescription::new(description_secret).unwrap();
        assert_eq!(description.as_str(), description_secret);
        assert_eq!(description.len_bytes(), description_secret.len());
        assert!(!format!("{description:?}").contains(description_secret));
        assert_eq!(
            from_value::<CapabilityDescription>(to_value(&description).unwrap()).unwrap(),
            description
        );

        assert_eq!(
            String::from(CapabilityTitle::new("Display title").unwrap()),
            "Display title"
        );
        assert_eq!(
            String::from(CapabilityDescription::new("Detailed description").unwrap()),
            "Detailed description"
        );
    }

    #[test]
    fn capability_titles_enforce_single_line_unambiguous_bounds() {
        assert_eq!(CapabilityTitle::new(""), Err(CapabilityTitleError::Empty));
        assert_eq!(
            CapabilityTitle::new("a".repeat(CapabilityTitle::MAX_BYTES + 1)),
            Err(CapabilityTitleError::TooLong {
                max: CapabilityTitle::MAX_BYTES,
                actual: CapabilityTitle::MAX_BYTES + 1,
            })
        );
        let exact_multibyte = "é".repeat(CapabilityTitle::MAX_BYTES / 2);
        assert!(CapabilityTitle::new(exact_multibyte).is_ok());

        for value in [" title", "title ", "\ttitle", "title\n"] {
            assert_eq!(
                CapabilityTitle::new(value),
                Err(CapabilityTitleError::BoundaryWhitespace),
                "accepted {value:?}"
            );
        }

        for scalar in [
            '\u{0000}',
            '\u{001f}',
            '\u{007f}',
            '\u{0085}',
            '\u{061c}',
            '\u{200e}',
            '\u{200f}',
            '\u{2028}',
            '\u{2029}',
            '\u{202a}',
            '\u{202e}',
            '\u{2066}',
            '\u{2069}',
            '\u{fdd0}',
            '\u{10ffff}',
        ] {
            let value = format!("a{scalar}b");
            assert!(
                matches!(
                    CapabilityTitle::new(&value),
                    Err(CapabilityTitleError::DisallowedCodePoint { .. })
                ),
                "accepted U+{:04X}",
                u32::from(scalar)
            );
        }

        assert!(from_value::<CapabilityTitle>(json!(42)).is_err());
        assert!(from_value::<CapabilityTitle>(Value::Null).is_err());
        let schema = to_value(schemars::schema_for!(CapabilityTitle)).unwrap();
        assert_eq!(schema["maxLength"], CapabilityTitle::MAX_BYTES);
    }

    #[test]
    fn capability_descriptions_enforce_prompt_and_resource_safety() {
        assert_eq!(
            CapabilityDescription::new(""),
            Err(CapabilityDescriptionError::Empty)
        );
        assert_eq!(
            CapabilityDescription::new("a".repeat(CapabilityDescription::MAX_BYTES + 1)),
            Err(CapabilityDescriptionError::TooLong {
                max: CapabilityDescription::MAX_BYTES,
                actual: CapabilityDescription::MAX_BYTES + 1,
            })
        );
        assert!(CapabilityDescription::new("a".repeat(CapabilityDescription::MAX_BYTES)).is_ok());
        assert!(CapabilityDescription::new("line one\n\tline two\rline three").is_ok());

        for value in [
            " description",
            "description ",
            "\ndescription",
            "description\r",
        ] {
            assert_eq!(
                CapabilityDescription::new(value),
                Err(CapabilityDescriptionError::BoundaryWhitespace),
                "accepted {value:?}"
            );
        }

        for code_point in [0_u32..=0x1f, 0x7f..=0x9f].into_iter().flatten() {
            let scalar = char::from_u32(code_point).unwrap();
            let value = format!("a{scalar}b");
            if matches!(scalar, '\t' | '\n' | '\r') {
                assert!(CapabilityDescription::new(value).is_ok());
            } else {
                assert!(matches!(
                    CapabilityDescription::new(value),
                    Err(CapabilityDescriptionError::DisallowedCodePoint { .. })
                ));
            }
        }

        for scalar in [
            '\u{061c}',
            '\u{200e}',
            '\u{200f}',
            '\u{202a}',
            '\u{202e}',
            '\u{2066}',
            '\u{2069}',
            '\u{fdd0}',
            '\u{10ffff}',
        ] {
            let value = format!("a{scalar}b");
            assert!(matches!(
                CapabilityDescription::new(value),
                Err(CapabilityDescriptionError::DisallowedCodePoint { .. })
            ));
        }

        assert!(from_value::<CapabilityDescription>(json!(42)).is_err());
        let schema = to_value(schemars::schema_for!(CapabilityDescription)).unwrap();
        assert_eq!(schema["maxLength"], CapabilityDescription::MAX_BYTES);
    }

    #[test]
    fn capability_kinds_have_closed_canonical_wire_values() {
        for (kind, expected) in [
            (CapabilityKind::Model, "model"),
            (CapabilityKind::Tool, "tool"),
            (CapabilityKind::Agent, "agent"),
            (CapabilityKind::Workflow, "workflow"),
            (CapabilityKind::Application, "application"),
        ] {
            assert_eq!(to_value(kind).unwrap(), Value::from(expected));
            assert_eq!(from_value::<CapabilityKind>(json!(expected)).unwrap(), kind);
        }
        assert!(from_value::<CapabilityKind>(json!("unknown")).is_err());
        assert!(from_value::<CapabilityKind>(Value::Null).is_err());
    }

    #[test]
    fn capability_lifecycles_are_closed_and_time_ordered() {
        let active = CapabilityLifecycle::active();
        assert_eq!(active, CapabilityLifecycle::default());
        assert_eq!(active.state(), CapabilityLifecycleState::Active);
        assert_eq!(active.announced_at(), None);
        assert_eq!(active.sunset_at(), None);
        assert_eq!(active.retired_at(), None);
        assert!(active.notice().is_none());
        assert!(active.replacement().is_none());
        assert_eq!(to_value(&active).unwrap(), json!({ "status": "active" }));

        let announced_at = timestamp("2026-08-29T00:00:00.000000Z");
        let sunset_at = timestamp("2027-02-28T00:00:00.000000Z");
        let replacement = identity("payments.capture", Version::new(3, 0, 0));
        let notice_secret = "Migrate to payments.capture 3.0.0 before retirement.";
        let deprecated = CapabilityLifecycle::deprecated(
            announced_at,
            Some(sunset_at),
            description(notice_secret),
            Some(replacement.clone()),
        )
        .unwrap();
        assert_eq!(deprecated.state(), CapabilityLifecycleState::Deprecated);
        assert_eq!(deprecated.announced_at(), Some(announced_at));
        assert_eq!(deprecated.sunset_at(), Some(sunset_at));
        assert_eq!(deprecated.retired_at(), None);
        assert_eq!(deprecated.notice().unwrap().as_str(), notice_secret);
        assert_eq!(deprecated.replacement(), Some(&replacement));
        assert!(!format!("{deprecated:?}").contains(notice_secret));

        let encoded = to_value(&deprecated).unwrap();
        assert_eq!(encoded["status"], "deprecated");
        assert!(encoded.get("announced_at").is_some());
        assert!(encoded.get("sunset_at").is_some());
        assert_eq!(
            from_value::<CapabilityLifecycle>(encoded).unwrap(),
            deprecated
        );

        for invalid_sunset in [announced_at, timestamp("2026-08-28T23:59:59.999999Z")] {
            assert_eq!(
                CapabilityLifecycle::deprecated(
                    announced_at,
                    Some(invalid_sunset),
                    description("Migrate before retirement."),
                    None,
                ),
                Err(CapabilityLifecycleError::InvalidSunsetOrder {
                    announced_at,
                    sunset_at: invalid_sunset,
                })
            );
        }

        let retired_at = timestamp("2027-02-28T00:00:00.000001Z");
        let retired = CapabilityLifecycle::retired(
            retired_at,
            description("This capability is retained only for history."),
            Some(replacement),
        );
        assert_eq!(retired.state(), CapabilityLifecycleState::Retired);
        assert_eq!(retired.retired_at(), Some(retired_at));
        assert_eq!(
            from_value::<CapabilityLifecycle>(to_value(&retired).unwrap()).unwrap(),
            retired
        );

        for invalid in [
            json!({ "status": "unknown" }),
            json!({ "status": "active", "notice": "unexpected" }),
            json!({ "status": "deprecated", "announced_at": announced_at }),
            json!({
                "status": "deprecated",
                "announced_at": sunset_at,
                "sunset_at": announced_at,
                "notice": "Invalid order."
            }),
            json!({ "status": "retired", "notice": "Missing timestamp." }),
            Value::Null,
        ] {
            assert!(
                from_value::<CapabilityLifecycle>(invalid.clone()).is_err(),
                "accepted lifecycle {invalid}"
            );
        }
        assert!(
            serde_json::from_str::<CapabilityLifecycle>(r#"{"status":"active","status":"active"}"#)
                .is_err()
        );
    }

    #[test]
    fn capability_metadata_revalidates_cross_field_invariants_and_redacts_text() {
        let subject = identity("payments.capture", Version::new(2, 1, 0));
        let replacement = identity("payments.capture", Version::new(3, 0, 0));
        let title_secret = "Capture payment secret-title";
        let description_secret = "Capture one approved payment. secret-description";
        let extension_secret = "extension-secret";
        let lifecycle = CapabilityLifecycle::deprecated(
            timestamp("2026-08-29T00:00:00.000000Z"),
            Some(timestamp("2027-02-28T00:00:00.000000Z")),
            description("Migrate to the replacement before sunset."),
            Some(replacement),
        )
        .unwrap();
        let metadata = CapabilityMetadata::new(
            subject.clone(),
            CapabilityKind::Tool,
            Some(CapabilityTitle::new(title_secret).unwrap()),
            description(description_secret),
            lifecycle,
            scopes(&["payments:read", "payments:write"]),
            extensions(extension_secret),
        )
        .unwrap();

        assert_eq!(metadata.identity(), &subject);
        assert_eq!(metadata.kind(), CapabilityKind::Tool);
        assert_eq!(metadata.title().unwrap().as_str(), title_secret);
        assert_eq!(metadata.description().as_str(), description_secret);
        assert_eq!(metadata.required_scopes().len(), 2);
        assert_eq!(metadata.extensions().len(), 1);
        assert_eq!(
            metadata.lifecycle().state(),
            CapabilityLifecycleState::Deprecated
        );

        let debug = format!("{metadata:?}");
        assert!(!debug.contains(title_secret));
        assert!(!debug.contains(description_secret));
        assert!(!debug.contains(extension_secret));

        let encoded = to_value(&metadata).unwrap();
        assert_eq!(
            from_value::<CapabilityMetadata>(encoded.clone()).unwrap(),
            metadata
        );
        let mut unknown = encoded.clone();
        unknown["authenticated"] = json!(true);
        assert!(from_value::<CapabilityMetadata>(unknown).is_err());

        let self_replacing = CapabilityLifecycle::deprecated(
            timestamp("2026-08-29T00:00:00.000000Z"),
            None,
            description("Use a replacement."),
            Some(subject.clone()),
        )
        .unwrap();
        assert_eq!(
            CapabilityMetadata::new(
                subject.clone(),
                CapabilityKind::Tool,
                None,
                description("Capture one payment."),
                self_replacing,
                ScopeSet::empty(),
                Extensions::default(),
            ),
            Err(CapabilityMetadataError::ReplacementIsSelf)
        );

        let mut invalid_wire = encoded;
        invalid_wire["lifecycle"]["replacement"] = to_value(&subject).unwrap();
        assert!(from_value::<CapabilityMetadata>(invalid_wire).is_err());
    }

    #[test]
    fn capability_metadata_and_lifecycle_schemas_are_closed() {
        let schema = to_value(schemars::schema_for!(CapabilityMetadata)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        let required = schema["required"].as_array().unwrap();
        for field in [
            "identity",
            "kind",
            "description",
            "lifecycle",
            "required_scopes",
            "extensions",
        ] {
            assert!(required.contains(&Value::from(field)));
        }
        assert!(!required.contains(&Value::from("title")));

        let lifecycle = to_value(schemars::schema_for!(CapabilityLifecycle)).unwrap();
        let variants = lifecycle["oneOf"].as_array().unwrap();
        assert_eq!(variants.len(), 3);
        assert!(
            variants
                .iter()
                .all(|variant| variant["additionalProperties"] == false)
        );
    }

    proptest! {
        #[test]
        fn valid_description_text_round_trips_exactly(
            first in "[A-Za-z0-9]",
            middle in "[A-Za-z0-9 \\t\\n\\r]{0,512}",
            last in "[A-Za-z0-9]",
        ) {
            let value = format!("{first}{middle}{last}");
            let description = CapabilityDescription::new(&value).unwrap();
            prop_assert_eq!(description.as_str(), value.as_str());
            let encoded = serde_json::to_vec(&description).unwrap();
            let decoded = serde_json::from_slice::<CapabilityDescription>(&encoded).unwrap();
            prop_assert_eq!(decoded, description);
        }

        #[test]
        fn sunset_order_is_strict_for_all_representable_instants(
            announced_micros in -1_000_000_000_000_i64..1_000_000_000_000_i64,
            delta in 1_i64..1_000_000_000_i64,
        ) {
            let announced_at = Timestamp::from_unix_micros(announced_micros).unwrap();
            let sunset_at = Timestamp::from_unix_micros(announced_micros + delta).unwrap();
            prop_assert!(CapabilityLifecycle::deprecated(
                announced_at,
                Some(sunset_at),
                description("Migrate before sunset."),
                None,
            ).is_ok());
            prop_assert!(CapabilityLifecycle::deprecated(
                sunset_at,
                Some(announced_at),
                description("Invalid reverse ordering."),
                None,
            ).is_err());
        }
    }
}
