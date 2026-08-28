// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Canonical semantic versions used to pin durable `StateKnot` contracts.

use std::{fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

const VERSION_PATTERN: &str = "^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$";

/// A canonical three-component semantic version.
///
/// `StateKnot` contract versions intentionally exclude pre-release and build
/// metadata. Their wire form is exactly `major.minor.patch`, with three `u64`
/// components and no leading zeroes except for the value zero itself.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Version {
    major: u64,
    minor: u64,
    patch: u64,
}

impl Version {
    /// Maximum length of the canonical text representation in bytes.
    pub const MAX_LEN: usize = 62;

    /// Constructs a version from its numeric components.
    #[must_use]
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Returns the major component.
    #[must_use]
    pub const fn major(self) -> u64 {
        self.major
    }

    /// Returns the minor component.
    #[must_use]
    pub const fn minor(self) -> u64 {
        self.minor
    }

    /// Returns the patch component.
    #[must_use]
    pub const fn patch(self) -> u64 {
        self.patch
    }
}

impl fmt::Display for Version {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl FromStr for Version {
    type Err = VersionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > Self::MAX_LEN {
            return Err(VersionError::TooLong {
                max: Self::MAX_LEN,
                actual: value.len(),
            });
        }

        let mut parts = value.split('.');
        let major = parse_component(parts.next(), VersionComponent::Major)?;
        let minor = parse_component(parts.next(), VersionComponent::Minor)?;
        let patch = parse_component(parts.next(), VersionComponent::Patch)?;
        if parts.next().is_some() {
            return Err(VersionError::InvalidFormat);
        }

        Ok(Self::new(major, minor, patch))
    }
}

impl From<(u64, u64, u64)> for Version {
    fn from((major, minor, patch): (u64, u64, u64)) -> Self {
        Self::new(major, minor, patch)
    }
}

impl From<Version> for (u64, u64, u64) {
    fn from(value: Version) -> Self {
        (value.major, value.minor, value.patch)
    }
}

impl Serialize for Version {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Version {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(VersionVisitor)
    }
}

impl JsonSchema for Version {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Version".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Version").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 5,
            "maxLength": 62,
            "pattern": VERSION_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// The component of a [`Version`] that failed validation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum VersionComponent {
    /// The major component.
    Major,
    /// The minor component.
    Minor,
    /// The patch component.
    Patch,
}

impl fmt::Display for VersionComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Major => "major",
            Self::Minor => "minor",
            Self::Patch => "patch",
        })
    }
}

/// Parse failure for a canonical [`Version`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum VersionError {
    /// The encoded version exceeded [`Version::MAX_LEN`].
    #[error("version is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The value was not exactly three decimal components.
    #[error("version must contain exactly three decimal components")]
    InvalidFormat,

    /// A component used a leading zero.
    #[error("version {component} component is not canonical")]
    NonCanonical {
        /// Component containing the leading zero.
        component: VersionComponent,
    },

    /// A component exceeded the `u64` range.
    #[error("version {component} component exceeds the supported range")]
    ComponentOverflow {
        /// Component that overflowed.
        component: VersionComponent,
    },
}

struct VersionVisitor;

impl de::Visitor<'_> for VersionVisitor {
    type Value = Version;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical three-component StateKnot version")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

fn parse_component(value: Option<&str>, component: VersionComponent) -> Result<u64, VersionError> {
    let value = value.ok_or(VersionError::InvalidFormat)?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(VersionError::InvalidFormat);
    }
    if value.len() > 1 && value.starts_with('0') {
        return Err(VersionError::NonCanonical { component });
    }
    value
        .parse()
        .map_err(|_| VersionError::ComponentOverflow { component })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, from_str, from_value, to_string};

    #[test]
    fn versions_round_trip_at_numeric_boundaries() {
        for value in [0, 1, 9, 10, u64::MAX] {
            let version = Version::new(value, value, value);
            assert_eq!(version.to_string().parse::<Version>().unwrap(), version);
            assert_eq!(<(u64, u64, u64)>::from(version), (value, value, value));
        }

        assert!(Version::new(1, 10, 0) < Version::new(2, 0, 0));
        assert_eq!(Version::new(1, 2, 3).to_string(), "1.2.3");
    }

    #[test]
    fn versions_reject_noncanonical_or_unsupported_forms() {
        for value in [
            "",
            "1",
            "1.2",
            "1.2.3.4",
            "1..3",
            ".1.2",
            "1.2.",
            "v1.2.3",
            "1.2.3-alpha",
            "1.2.3+build",
            "1. 2.3",
        ] {
            assert_eq!(value.parse::<Version>(), Err(VersionError::InvalidFormat));
        }

        assert_eq!(
            "01.2.3".parse::<Version>(),
            Err(VersionError::NonCanonical {
                component: VersionComponent::Major,
            })
        );
        assert_eq!(
            "1.02.3".parse::<Version>(),
            Err(VersionError::NonCanonical {
                component: VersionComponent::Minor,
            })
        );
        assert_eq!(
            "1.2.03".parse::<Version>(),
            Err(VersionError::NonCanonical {
                component: VersionComponent::Patch,
            })
        );
        assert_eq!(
            "18446744073709551616.0.0".parse::<Version>(),
            Err(VersionError::ComponentOverflow {
                component: VersionComponent::Major,
            })
        );

        let too_long = format!("{}.0.0", "1".repeat(Version::MAX_LEN));
        assert_eq!(
            too_long.parse::<Version>(),
            Err(VersionError::TooLong {
                max: Version::MAX_LEN,
                actual: too_long.len(),
            })
        );
    }

    #[test]
    fn version_serde_uses_and_enforces_canonical_text() {
        let version = Version::new(12, 34, 56);
        let encoded = to_string(&version).unwrap();
        assert_eq!(encoded, "\"12.34.56\"");
        assert_eq!(from_str::<Version>(&encoded).unwrap(), version);
        assert!(from_value::<Version>(Value::Array(Vec::new())).is_err());
        assert!(from_str::<Version>("\"12.034.56\"").is_err());
    }

    #[test]
    fn version_schema_matches_runtime_bounds() {
        let schema = serde_json::to_value(schemars::schema_for!(Version)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 5);
        assert_eq!(schema["maxLength"], Version::MAX_LEN);
        assert_eq!(schema["pattern"], VERSION_PATTERN);
    }
}
