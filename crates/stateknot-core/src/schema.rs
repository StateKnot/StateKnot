// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Stable identities for local, digest-pinned JSON schemas.

use std::{fmt, str::FromStr};

use fluent_uri::Uri;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{Digest, Version};

const SCHEMA_ID_PATTERN: &str = "^https://[^?#]+$";

/// A normalized, absolute HTTPS identifier for a schema resource.
///
/// A schema ID is an identity, never an instruction to fetch over the network.
/// `StateKnot` resolves it only from an explicitly populated local registry and
/// verifies schema bytes against a pinned [`Digest`].
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SchemaId(Box<str>);

impl SchemaId {
    /// Maximum encoded length in bytes.
    pub const MAX_LEN: usize = 512;

    /// Returns the canonical URI text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SchemaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("SchemaId")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for SchemaId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for SchemaId {
    type Err = SchemaIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(SchemaIdError::Empty);
        }
        if value.len() > Self::MAX_LEN {
            return Err(SchemaIdError::TooLong {
                max: Self::MAX_LEN,
                actual: value.len(),
            });
        }

        let uri = Uri::parse(value).map_err(|_| SchemaIdError::InvalidUri)?;
        let scheme = uri.scheme().as_str();
        if !scheme.eq_ignore_ascii_case("https") {
            return Err(SchemaIdError::UnsupportedScheme);
        }

        let authority = uri.authority().ok_or(SchemaIdError::MissingAuthority)?;
        if authority.host().is_empty() {
            return Err(SchemaIdError::MissingAuthority);
        }
        if authority.userinfo().is_some() {
            return Err(SchemaIdError::UserInfoNotAllowed);
        }
        if uri.query().is_some() {
            return Err(SchemaIdError::QueryNotAllowed);
        }
        if uri.fragment().is_some() {
            return Err(SchemaIdError::FragmentNotAllowed);
        }

        if uri.normalize().as_str() != value {
            return Err(SchemaIdError::NonCanonical);
        }

        Ok(Self(value.into()))
    }
}

impl Serialize for SchemaId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SchemaId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SchemaIdVisitor)
    }
}

struct SchemaIdVisitor;

impl de::Visitor<'_> for SchemaIdVisitor {
    type Value = SchemaId;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a normalized absolute HTTPS schema identifier")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

impl JsonSchema for SchemaId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SchemaId".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::SchemaId").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "format": "uri",
            "minLength": 9,
            "maxLength": 512,
            "pattern": SCHEMA_ID_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for a [`SchemaId`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SchemaIdError {
    /// The identifier was empty.
    #[error("schema identifier must not be empty")]
    Empty,

    /// The identifier exceeded [`SchemaId::MAX_LEN`].
    #[error("schema identifier is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The text was not an absolute RFC 3986 URI.
    #[error("schema identifier must be an absolute RFC 3986 URI")]
    InvalidUri,

    /// The URI did not use the supported HTTPS scheme.
    #[error("schema identifier must use HTTPS")]
    UnsupportedScheme,

    /// The HTTPS URI did not contain a non-empty authority host.
    #[error("schema identifier must contain an HTTPS authority host")]
    MissingAuthority,

    /// URI user information was present.
    #[error("schema identifier must not contain user information")]
    UserInfoNotAllowed,

    /// A query component was present, including an empty query.
    #[error("schema identifier must not contain a query")]
    QueryNotAllowed,

    /// A fragment component was present, including an empty fragment.
    #[error("schema identifier must not contain a fragment")]
    FragmentNotAllowed,

    /// The input did not already use its normalized RFC 3986 spelling.
    #[error("schema identifier must use normalized RFC 3986 text")]
    NonCanonical,
}

/// An immutable reference to canonical JSON schema bytes.
///
/// The explicit version supports compatibility policy, while the digest
/// prevents a reused URI or version from silently changing validation
/// behavior.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaReference {
    id: SchemaId,
    version: Version,
    digest: Digest,
}

impl SchemaReference {
    /// Constructs an immutable schema reference from validated components.
    #[must_use]
    pub const fn new(id: SchemaId, version: Version, digest: Digest) -> Self {
        Self {
            id,
            version,
            digest,
        }
    }

    /// Returns the stable schema identifier.
    #[must_use]
    pub const fn id(&self) -> &SchemaId {
        &self.id
    }

    /// Returns the explicitly pinned schema version.
    #[must_use]
    pub const fn version(&self) -> Version {
        self.version
    }

    /// Returns the digest of the canonical schema bytes.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, from_value, json, to_value};

    const RUN_EVENT_ID: &str = "https://stateknot.github.io/schema/run-event/1.0.0";

    #[test]
    fn schema_ids_accept_only_normalized_https_uris() {
        for value in [
            RUN_EVENT_ID,
            "https://schemas.example.com/tool/input/2.3.4",
            "https://schemas.example.com:8443/a%20b/1.0.0",
        ] {
            let id = value.parse::<SchemaId>().unwrap();
            assert_eq!(id.as_str(), value);
            assert_eq!(id.to_string(), value);
            assert_eq!(to_value(&id).unwrap(), Value::from(value));
        }
    }

    #[test]
    fn schema_ids_reject_ambiguous_or_active_uris() {
        for value in [
            "schema/run-event/1.0.0",
            "http://example.com/schema/1.0.0",
            "HTTPS://example.com/schema/1.0.0",
            "https://EXAMPLE.com/schema/1.0.0",
            "https:/schema/1.0.0",
            "https://user@example.com/schema/1.0.0",
            "https://example.com/schema/1.0.0?",
            "https://example.com/schema/1.0.0?profile=full",
            "https://example.com/schema/1.0.0#",
            "https://example.com/schema/1.0.0#part",
            "https://example.com/a/../schema/1.0.0",
            "https://example.com:443/schema/1.0.0",
            "https://例子.example/schema/1.0.0",
        ] {
            assert!(value.parse::<SchemaId>().is_err(), "accepted {value:?}");
        }

        let oversized = format!("https://example.com/{}", "a".repeat(SchemaId::MAX_LEN));
        assert_eq!(
            oversized.parse::<SchemaId>(),
            Err(SchemaIdError::TooLong {
                max: SchemaId::MAX_LEN,
                actual: oversized.len(),
            })
        );
    }

    #[test]
    fn schema_id_serde_and_schema_enforce_the_wire_contract() {
        let id = RUN_EVENT_ID.parse::<SchemaId>().unwrap();
        assert_eq!(from_value::<SchemaId>(json!(RUN_EVENT_ID)).unwrap(), id);
        assert!(from_value::<SchemaId>(json!(42)).is_err());
        assert!(from_value::<SchemaId>(Value::Null).is_err());

        let schema = to_value(schemars::schema_for!(SchemaId)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["format"], "uri");
        assert_eq!(schema["minLength"], 9);
        assert_eq!(schema["maxLength"], SchemaId::MAX_LEN);
        assert_eq!(schema["pattern"], SCHEMA_ID_PATTERN);
    }

    #[test]
    fn schema_references_bind_id_version_and_digest() {
        let id = RUN_EVENT_ID.parse::<SchemaId>().unwrap();
        let version = Version::new(1, 0, 0);
        let digest = Digest::sha256(b"canonical schema");
        let reference = SchemaReference::new(id.clone(), version, digest);

        assert_eq!(reference.id(), &id);
        assert_eq!(reference.version(), version);
        assert_eq!(reference.digest(), digest);

        let encoded = to_value(&reference).unwrap();
        assert_eq!(
            encoded,
            json!({
                "id": RUN_EVENT_ID,
                "version": "1.0.0",
                "digest": digest.to_string(),
            })
        );
        assert_eq!(from_value::<SchemaReference>(encoded).unwrap(), reference);
    }

    #[test]
    fn schema_references_reject_missing_extra_or_invalid_fields() {
        let digest = Digest::sha256(b"canonical schema").to_string();
        for invalid in [
            json!({"id": RUN_EVENT_ID, "version": "1.0.0"}),
            json!({"id": RUN_EVENT_ID, "version": "1.0.0", "digest": digest, "extra": true}),
            json!({"id": "http://example.com/schema", "version": "1.0.0", "digest": digest}),
            json!({"id": RUN_EVENT_ID, "version": "v1", "digest": digest}),
            json!({"id": RUN_EVENT_ID, "version": "1.0.0", "digest": "sha256:00"}),
        ] {
            assert!(
                from_value::<SchemaReference>(invalid.clone()).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn schema_reference_schema_is_closed() {
        let schema = to_value(schemars::schema_for!(SchemaReference)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"], json!(["id", "version", "digest"]));
        assert_eq!(schema["properties"]["id"]["format"], "uri");
    }
}
