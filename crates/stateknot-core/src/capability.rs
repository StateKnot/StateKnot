// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Stable names for executable capabilities.

use std::{borrow::Borrow, fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::Version;

const CAPABILITY_NAME_PATTERN: &str = "^[A-Za-z0-9_.-]{1,128}$";

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
    use serde_json::{Value, from_value, json, to_value};

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
}
