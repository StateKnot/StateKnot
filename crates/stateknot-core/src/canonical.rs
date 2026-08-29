// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Explicit RFC 8785 canonical JSON for integrity-bearing boundaries.

use std::{fmt, hash::Hash};

use serde_json::Value;
use thiserror::Error;

use crate::{BoundedJson, Digest};

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Immutable RFC 8785 JSON Canonicalization Scheme bytes.
///
/// Construction accepts only an already resource-bounded JSON value and
/// rejects integer representations outside the interoperable I-JSON range.
/// This type is intentionally not serializable: callers must explicitly use
/// [`Self::as_bytes`] at a hashing, signing, approval, or durable-envelope
/// boundary instead of accidentally treating canonical bytes as ordinary JSON.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct CanonicalJson {
    text: Box<str>,
    digest: Digest,
}

impl CanonicalJson {
    /// Canonicalizes one bounded value according to RFC 8785.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalJsonError::IntegerOutsideIJsonSafeRange`] before
    /// serialization if an integer cannot be transported exactly through the
    /// interoperable IEEE-754 integer range. It fails closed if the canonical
    /// serializer cannot encode the otherwise validated value.
    pub fn new(value: &BoundedJson) -> Result<Self, CanonicalJsonError> {
        validate_ijson_numbers(value.as_value())?;
        let bytes = serde_json_canonicalizer::to_vec(value.as_value())
            .map_err(|_| CanonicalJsonError::Serialization)?;
        let text = String::from_utf8(bytes).map_err(|_| CanonicalJsonError::InvalidUtf8)?;
        let digest = Digest::sha256(text.as_bytes());
        Ok(Self {
            text: text.into_boxed_str(),
            digest,
        })
    }

    /// Returns the canonical UTF-8 bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.text.as_bytes()
    }

    /// Returns the canonical JSON text.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        &self.text
    }

    /// Returns SHA-256 over exactly [`Self::as_bytes`].
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl TryFrom<&BoundedJson> for CanonicalJson {
    type Error = CanonicalJsonError;

    fn try_from(value: &BoundedJson) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Debug for CanonicalJson {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CanonicalJson")
            .field("bytes", &self.text.len())
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Failure to produce integrity-bearing RFC 8785 bytes.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CanonicalJsonError {
    /// An integer exceeded the exact interoperable range `-(2^53-1)..=2^53-1`.
    #[error("JSON integer is outside the interoperable I-JSON safe range")]
    IntegerOutsideIJsonSafeRange,

    /// The RFC 8785 serializer rejected the validated value.
    #[error("RFC 8785 canonical serialization failed")]
    Serialization,

    /// The serializer unexpectedly emitted bytes that were not UTF-8.
    #[error("RFC 8785 canonical serialization emitted invalid UTF-8")]
    InvalidUtf8,
}

fn validate_ijson_numbers(value: &Value) -> Result<(), CanonicalJsonError> {
    match value {
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                if value.unsigned_abs() > MAX_SAFE_INTEGER {
                    return Err(CanonicalJsonError::IntegerOutsideIJsonSafeRange);
                }
            } else if let Some(value) = number.as_u64() {
                if value > MAX_SAFE_INTEGER {
                    return Err(CanonicalJsonError::IntegerOutsideIJsonSafeRange);
                }
            }
            Ok(())
        }
        Value::Array(values) => values.iter().try_for_each(validate_ijson_numbers),
        Value::Object(values) => values.values().try_for_each(validate_ijson_numbers),
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_rfc_8785_section_3_2_2_vector() {
        let input = BoundedJson::from_str(
            r#"{
                "numbers": [333333333.33333329, 1E30, 4.50,
                            2e-3, 0.000000000000000000000000001],
                "string": "\u20ac$\u000F\u000aA'\u0042\u0022\u005c\\\"\/",
                "literals": [null, true, false]
            }"#,
        )
        .unwrap();

        let canonical = CanonicalJson::new(&input).unwrap();
        assert_eq!(
            canonical.as_str(),
            r#"{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\u000f\nA'B\"\\\\\"/"}"#
        );
        assert_eq!(canonical.digest(), Digest::sha256(canonical.as_bytes()));
        assert_eq!(canonical.as_str().len(), canonical.as_bytes().len());
    }

    #[test]
    fn equivalent_json_text_has_identical_canonical_identity() {
        let first = BoundedJson::from_str(r#"{"z":0,"a":[true,{"x":"é"}]}"#).unwrap();
        let second =
            BoundedJson::from_str(" { \"a\" : [ true, { \"x\" : \"é\" } ], \"z\": -0.0 }").unwrap();

        let first = CanonicalJson::new(&first).unwrap();
        let second = CanonicalJson::new(&second).unwrap();
        assert_eq!(first.as_str(), r#"{"a":[true,{"x":"é"}],"z":0}"#);
        assert_eq!(first, second);
    }

    #[test]
    fn rejects_integer_values_that_jcs_would_round() {
        for input in ["9007199254740992", "-9007199254740992"] {
            let bounded = BoundedJson::from_str(input).unwrap();
            assert_eq!(
                CanonicalJson::new(&bounded),
                Err(CanonicalJsonError::IntegerOutsideIJsonSafeRange)
            );
        }

        for input in ["9007199254740991", "-9007199254740991"] {
            assert!(CanonicalJson::new(&BoundedJson::from_str(input).unwrap()).is_ok());
        }
    }

    #[test]
    fn floating_point_exponents_remain_in_the_rfc_8785_domain() {
        let bounded = BoundedJson::from_str(r"[1e30,9007199254740992.0,-0.0]").unwrap();
        let canonical = CanonicalJson::try_from(&bounded).unwrap();
        assert_eq!(canonical.as_str(), "[1e+30,9007199254740992,0]");
    }

    #[test]
    fn diagnostics_disclose_only_size_and_digest() {
        let secret = "never-print-this-value";
        let bounded = BoundedJson::from_str(&format!(r#"{{"secret":"{secret}"}}"#)).unwrap();
        let canonical = CanonicalJson::new(&bounded).unwrap();
        let debug = format!("{canonical:?}");
        assert!(debug.contains("bytes"));
        assert!(debug.contains("sha256:"));
        assert!(!debug.contains(secret));
    }
}
