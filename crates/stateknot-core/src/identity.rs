// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Exact external issuer/subject identities for authenticated principals.

use std::{borrow::Borrow, fmt, str::FromStr};

use fluent_uri::Uri;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

const ISSUER_ID_PATTERN: &str = "^[Hh][Tt][Tt][Pp][Ss]://[^/?#@]+(?:/[^?#]*)?$";
const SUBJECT_ID_PATTERN: &str = r"^[\u0020-\u007E]{1,255}$";

/// An exact, case-sensitive OIDC/OAuth issuer identifier.
///
/// The value is an absolute HTTPS URI with a host and optional port/path. It
/// contains no user information, query, or fragment. Equality deliberately
/// performs no URI normalization: OIDC requires the configured issuer,
/// discovery metadata, and token `iss` claim to match exactly.
///
/// This type is identity data only. Constructing or deserializing it does not
/// authenticate a token and never performs discovery or network access.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IssuerId(Box<str>);

impl IssuerId {
    /// Maximum encoded length in bytes.
    pub const MAX_LEN: usize = 512;

    /// Validates and constructs an exact issuer identifier.
    ///
    /// # Errors
    ///
    /// Returns [`IssuerIdError`] when `value` is not a bounded HTTPS issuer
    /// URI with the required OIDC/OAuth components.
    pub fn new(value: impl Into<String>) -> Result<Self, IssuerIdError> {
        let value = value.into();
        validate_issuer_id(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact issuer text used for security comparison.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for IssuerId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for IssuerId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for IssuerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("IssuerId")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for IssuerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for IssuerId {
    type Err = IssuerIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for IssuerId {
    type Error = IssuerIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for IssuerId {
    type Error = IssuerIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<IssuerId> for String {
    fn from(value: IssuerId) -> Self {
        value.0.into()
    }
}

impl Serialize for IssuerId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for IssuerId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(IssuerIdVisitor)
    }
}

struct IssuerIdVisitor;

impl de::Visitor<'_> for IssuerIdVisitor {
    type Value = IssuerId;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an exact HTTPS OIDC/OAuth issuer identifier")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        IssuerId::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        IssuerId::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for IssuerId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "IssuerId".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::IssuerId").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "format": "uri",
            "minLength": 9,
            "maxLength": 512,
            "pattern": ISSUER_ID_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for an [`IssuerId`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum IssuerIdError {
    /// The identifier contained no bytes.
    #[error("issuer identifier must not be empty")]
    Empty,

    /// The identifier exceeded [`IssuerId::MAX_LEN`].
    #[error("issuer identifier is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The text was not an RFC 3986 URI.
    #[error("issuer identifier must be an RFC 3986 URI")]
    InvalidUri,

    /// The URI did not use the HTTPS scheme.
    #[error("issuer identifier must use HTTPS")]
    UnsupportedScheme,

    /// The URI did not contain a non-empty authority host.
    #[error("issuer identifier must contain an HTTPS authority host")]
    MissingAuthority,

    /// A port was empty or outside the `u16` range used by HTTPS transports.
    #[error("issuer identifier port must be an integer from 0 through 65535")]
    InvalidPort,

    /// URI user information was present.
    #[error("issuer identifier must not contain user information")]
    UserInfoNotAllowed,

    /// A query component was present, including an empty query.
    #[error("issuer identifier must not contain a query")]
    QueryNotAllowed,

    /// A fragment component was present, including an empty fragment.
    #[error("issuer identifier must not contain a fragment")]
    FragmentNotAllowed,
}

fn validate_issuer_id(value: &str) -> Result<(), IssuerIdError> {
    if value.is_empty() {
        return Err(IssuerIdError::Empty);
    }
    if value.len() > IssuerId::MAX_LEN {
        return Err(IssuerIdError::TooLong {
            max: IssuerId::MAX_LEN,
            actual: value.len(),
        });
    }

    let uri = Uri::parse(value).map_err(|_| IssuerIdError::InvalidUri)?;
    if !uri.scheme().as_str().eq_ignore_ascii_case("https") {
        return Err(IssuerIdError::UnsupportedScheme);
    }

    let authority = uri.authority().ok_or(IssuerIdError::MissingAuthority)?;
    if authority.host().is_empty() {
        return Err(IssuerIdError::MissingAuthority);
    }
    if authority.userinfo().is_some() {
        return Err(IssuerIdError::UserInfoNotAllowed);
    }
    if authority
        .port()
        .is_some_and(|port| port.is_empty() || authority.port_to_u16().is_err())
    {
        return Err(IssuerIdError::InvalidPort);
    }
    if uri.query().is_some() {
        return Err(IssuerIdError::QueryNotAllowed);
    }
    if uri.fragment().is_some() {
        return Err(IssuerIdError::FragmentNotAllowed);
    }

    Ok(())
}

/// A bounded, case-sensitive OIDC subject identifier.
///
/// Subject IDs contain 1 to 255 printable ASCII bytes. OIDC allows at most 255
/// ASCII characters; `StateKnot` additionally rejects control bytes so the
/// value is safe to persist across supported database/text boundaries. The
/// value remains opaque and is never normalized or treated as a username.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SubjectId(Box<str>);

impl SubjectId {
    /// Maximum encoded length in bytes.
    pub const MAX_LEN: usize = 255;

    /// Validates and constructs an opaque subject identifier.
    ///
    /// # Errors
    ///
    /// Returns [`SubjectIdError`] when `value` is empty, too long, non-ASCII,
    /// or contains an ASCII control byte.
    pub fn new(value: impl Into<String>) -> Result<Self, SubjectIdError> {
        let value = value.into();
        validate_subject_id(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact, case-sensitive subject value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SubjectId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for SubjectId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SubjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SubjectId")
            .field("byte_len", &self.as_str().len())
            .field("value", &"<redacted>")
            .finish()
    }
}

impl FromStr for SubjectId {
    type Err = SubjectIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for SubjectId {
    type Error = SubjectIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for SubjectId {
    type Error = SubjectIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SubjectId> for String {
    fn from(value: SubjectId) -> Self {
        value.0.into()
    }
}

impl Serialize for SubjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SubjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(SubjectIdVisitor)
    }
}

struct SubjectIdVisitor;

impl de::Visitor<'_> for SubjectIdVisitor {
    type Value = SubjectId;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded printable-ASCII OIDC subject identifier")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        SubjectId::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        SubjectId::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for SubjectId {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SubjectId".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::SubjectId").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 255,
            "pattern": SUBJECT_ID_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for a [`SubjectId`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SubjectIdError {
    /// The identifier contained no bytes.
    #[error("subject identifier must not be empty")]
    Empty,

    /// The identifier exceeded [`SubjectId::MAX_LEN`].
    #[error("subject identifier is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// A byte was not printable ASCII.
    #[error("subject identifier contains a non-printable-ASCII byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first invalid byte.
        index: usize,
    },
}

fn validate_subject_id(value: &str) -> Result<(), SubjectIdError> {
    if value.is_empty() {
        return Err(SubjectIdError::Empty);
    }
    if value.len() > SubjectId::MAX_LEN {
        return Err(SubjectIdError::TooLong {
            max: SubjectId::MAX_LEN,
            actual: value.len(),
        });
    }

    if let Some((index, _)) = value
        .bytes()
        .enumerate()
        .find(|(_, byte)| !(0x20..=0x7e).contains(byte))
    {
        return Err(SubjectIdError::InvalidByte { index });
    }

    Ok(())
}

/// The stable external identity of an authenticated principal.
///
/// OIDC guarantees uniqueness and stability only for the issuer/subject pair.
/// Tenant boundaries remain separate and must still be included in storage
/// keys and authorization decisions.
#[derive(
    Clone, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct PrincipalIdentity {
    issuer: IssuerId,
    subject: SubjectId,
}

impl PrincipalIdentity {
    /// Constructs an identity from already validated components.
    #[must_use]
    pub const fn new(issuer: IssuerId, subject: SubjectId) -> Self {
        Self { issuer, subject }
    }

    /// Returns the exact authenticated issuer.
    #[must_use]
    pub const fn issuer(&self) -> &IssuerId {
        &self.issuer
    }

    /// Returns the issuer-local subject.
    #[must_use]
    pub const fn subject(&self) -> &SubjectId {
        &self.subject
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, from_value, json, to_value};

    const ISSUER: &str = "https://issuer.example.com/tenant";

    #[test]
    fn issuer_ids_preserve_exact_oidc_identity_text() {
        for value in [
            "https://issuer.example.com",
            "https://issuer.example.com/",
            ISSUER,
            "https://issuer.example.com:8443/tenant",
            "HTTPS://ISSUER.example.com:443/a/../tenant",
            "https://issuer.example.com/%7Etenant",
        ] {
            let issuer = value.parse::<IssuerId>().unwrap();
            assert_eq!(issuer.as_str(), value);
            assert_eq!(issuer.to_string(), value);
            assert_eq!(to_value(&issuer).unwrap(), Value::from(value));
        }

        let lowercase = "https://issuer.example.com".parse::<IssuerId>().unwrap();
        let uppercase = "HTTPS://ISSUER.example.com".parse::<IssuerId>().unwrap();
        let slash = "https://issuer.example.com/".parse::<IssuerId>().unwrap();
        let default_port = "https://issuer.example.com:443"
            .parse::<IssuerId>()
            .unwrap();
        assert_ne!(lowercase, uppercase);
        assert_ne!(lowercase, slash);
        assert_ne!(lowercase, default_port);

        let prefix = "https://issuer.example.com/";
        let maximum = format!("{prefix}{}", "a".repeat(IssuerId::MAX_LEN - prefix.len()));
        assert_eq!(maximum.len(), IssuerId::MAX_LEN);
        assert_eq!(maximum.parse::<IssuerId>().unwrap().as_str(), maximum);
    }

    #[test]
    fn issuer_ids_reject_non_oidc_urls() {
        for value in [
            "issuer.example.com",
            "http://issuer.example.com",
            "https:/issuer.example.com",
            "https://",
            "https://issuer.example.com:",
            "https://issuer.example.com:65536",
            "https://user@issuer.example.com",
            "https://issuer.example.com?",
            "https://issuer.example.com?tenant=one",
            "https://issuer.example.com#",
            "https://issuer.example.com#fragment",
            "https://例子.example.com",
        ] {
            assert!(value.parse::<IssuerId>().is_err(), "accepted {value:?}");
        }

        let oversized = format!(
            "https://issuer.example.com/{}",
            "a".repeat(IssuerId::MAX_LEN)
        );
        assert_eq!(
            oversized.parse::<IssuerId>(),
            Err(IssuerIdError::TooLong {
                max: IssuerId::MAX_LEN,
                actual: oversized.len(),
            })
        );
    }

    #[test]
    fn issuer_id_serde_and_schema_enforce_the_wire_contract() {
        let issuer = ISSUER.parse::<IssuerId>().unwrap();
        assert_eq!(from_value::<IssuerId>(json!(ISSUER)).unwrap(), issuer);
        assert!(from_value::<IssuerId>(json!(42)).is_err());
        assert!(from_value::<IssuerId>(Value::Null).is_err());

        let schema = to_value(schemars::schema_for!(IssuerId)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["format"], "uri");
        assert_eq!(schema["minLength"], 9);
        assert_eq!(schema["maxLength"], IssuerId::MAX_LEN);
        assert_eq!(schema["pattern"], ISSUER_ID_PATTERN);
    }

    #[test]
    fn subject_ids_are_opaque_case_sensitive_and_redacted_in_debug() {
        let all_printable_ascii = (0x20_u8..=0x7e).map(char::from).collect::<String>();
        assert_eq!(
            all_printable_ascii.parse::<SubjectId>().unwrap().as_str(),
            all_printable_ascii
        );

        for value in [
            "24400320",
            "AItOawmwtWwcT0k51BayewNvutrJUqsvl6qs7A4",
            "User Name",
            "subject:\"quoted\"\\opaque",
        ] {
            let subject = value.parse::<SubjectId>().unwrap();
            assert_eq!(subject.as_str(), value);
            assert_eq!(to_value(&subject).unwrap(), Value::from(value));
            let debug = format!("{subject:?}");
            assert!(!debug.contains(value));
            assert!(debug.contains("<redacted>"));
        }

        assert_ne!(
            "subject".parse::<SubjectId>().unwrap(),
            "Subject".parse::<SubjectId>().unwrap()
        );

        let maximum = "a".repeat(SubjectId::MAX_LEN);
        assert_eq!(maximum.parse::<SubjectId>().unwrap().as_str(), maximum);
    }

    #[test]
    fn subject_ids_reject_empty_control_unicode_and_oversized_values() {
        assert_eq!("".parse::<SubjectId>(), Err(SubjectIdError::Empty));
        assert_eq!(
            "a".repeat(SubjectId::MAX_LEN + 1).parse::<SubjectId>(),
            Err(SubjectIdError::TooLong {
                max: SubjectId::MAX_LEN,
                actual: SubjectId::MAX_LEN + 1,
            })
        );

        for byte in 0_u8..=0x7f {
            if (0x20..=0x7e).contains(&byte) {
                continue;
            }
            let value = String::from_utf8(vec![byte]).unwrap();
            assert_eq!(
                value.parse::<SubjectId>(),
                Err(SubjectIdError::InvalidByte { index: 0 }),
                "accepted ASCII byte 0x{byte:02x}"
            );
        }
        assert_eq!(
            "用户".parse::<SubjectId>(),
            Err(SubjectIdError::InvalidByte { index: 0 })
        );
    }

    #[test]
    fn subject_id_serde_and_schema_enforce_the_wire_contract() {
        let subject = "subject-42".parse::<SubjectId>().unwrap();
        assert_eq!(
            from_value::<SubjectId>(json!(subject.as_str())).unwrap(),
            subject
        );
        assert!(from_value::<SubjectId>(json!(42)).is_err());
        assert!(from_value::<SubjectId>(Value::Null).is_err());

        let schema = to_value(schemars::schema_for!(SubjectId)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 1);
        assert_eq!(schema["maxLength"], SubjectId::MAX_LEN);
        assert_eq!(schema["pattern"], SUBJECT_ID_PATTERN);
    }

    #[test]
    fn principal_identity_requires_the_exact_issuer_subject_pair() {
        let issuer = ISSUER.parse::<IssuerId>().unwrap();
        let subject = "subject-42".parse::<SubjectId>().unwrap();
        let identity = PrincipalIdentity::new(issuer.clone(), subject.clone());

        assert_eq!(identity.issuer(), &issuer);
        assert_eq!(identity.subject(), &subject);
        assert_eq!(
            to_value(&identity).unwrap(),
            json!({ "issuer": ISSUER, "subject": "subject-42" })
        );
        let debug = format!("{identity:?}");
        assert!(debug.contains(ISSUER));
        assert!(!debug.contains(subject.as_str()));
        assert!(debug.contains("<redacted>"));
        assert_ne!(
            identity,
            PrincipalIdentity::new(
                "https://other.example.com".parse().unwrap(),
                subject.clone()
            )
        );

        for invalid in [
            json!({ "issuer": ISSUER }),
            json!({ "subject": "subject-42" }),
            json!({ "issuer": ISSUER, "subject": "subject-42", "extra": true }),
            json!({ "issuer": "http://issuer.example.com", "subject": "subject-42" }),
            json!({ "issuer": ISSUER, "subject": "subject\n42" }),
            Value::Null,
        ] {
            assert!(
                from_value::<PrincipalIdentity>(invalid.clone()).is_err(),
                "PrincipalIdentity accepted {invalid}"
            );
        }

        let schema = to_value(schemars::schema_for!(PrincipalIdentity)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"], json!(["issuer", "subject"]));
        assert_eq!(schema["properties"]["issuer"]["pattern"], ISSUER_ID_PATTERN);
        assert_eq!(
            schema["properties"]["subject"]["pattern"],
            SUBJECT_ID_PATTERN
        );
    }
}
