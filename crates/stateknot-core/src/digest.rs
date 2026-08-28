// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Canonical integrity digests for durable and security-bearing values.

use std::{fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const SHA256_PREFIX: &str = "sha256:";
const SHA256_TEXT_LEN: usize = SHA256_PREFIX.len() + (Digest::SHA256_LEN * 2);
const SHA256_PATTERN: &str = "^sha256:[0-9a-f]{64}$";

/// An integrity hash algorithm supported by `StateKnot`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum DigestAlgorithm {
    /// SHA-256.
    Sha256,
}

impl DigestAlgorithm {
    /// Returns the canonical algorithm name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
        }
    }
}

impl fmt::Display for DigestAlgorithm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum DigestValue {
    Sha256([u8; Digest::SHA256_LEN]),
}

/// A canonical integrity digest.
///
/// SHA-256 is mandatory in v1. The stable wire form is exactly
/// `sha256:<64 lowercase hexadecimal digits>`.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Digest(DigestValue);

impl Digest {
    /// Length of a SHA-256 digest in bytes.
    pub const SHA256_LEN: usize = 32;

    /// Computes the SHA-256 digest of `input`.
    #[must_use]
    pub fn sha256(input: impl AsRef<[u8]>) -> Self {
        let bytes: [u8; Self::SHA256_LEN] = Sha256::digest(input.as_ref()).into();
        Self::from_sha256(bytes)
    }

    /// Constructs a digest from a previously computed SHA-256 value.
    #[must_use]
    pub const fn from_sha256(bytes: [u8; Self::SHA256_LEN]) -> Self {
        Self(DigestValue::Sha256(bytes))
    }

    /// Returns the digest algorithm.
    #[must_use]
    pub const fn algorithm(self) -> DigestAlgorithm {
        match self.0 {
            DigestValue::Sha256(_) => DigestAlgorithm::Sha256,
        }
    }

    /// Returns the raw digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            DigestValue::Sha256(bytes) => bytes,
        }
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Digest")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:", self.algorithm())?;
        for byte in self.as_bytes() {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for Digest {
    type Err = DigestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > SHA256_TEXT_LEN {
            return Err(DigestError::TooLong {
                max: SHA256_TEXT_LEN,
                actual: value.len(),
            });
        }

        let (algorithm, encoded) = value.split_once(':').ok_or(DigestError::InvalidFormat)?;
        if algorithm != DigestAlgorithm::Sha256.as_str() {
            return if algorithm.eq_ignore_ascii_case(DigestAlgorithm::Sha256.as_str()) {
                Err(DigestError::NonCanonical)
            } else {
                Err(DigestError::UnsupportedAlgorithm)
            };
        }
        if encoded.len() != Self::SHA256_LEN * 2 {
            return Err(DigestError::InvalidLength {
                expected: Self::SHA256_LEN * 2,
                actual: encoded.len(),
            });
        }

        let mut bytes = [0_u8; Self::SHA256_LEN];
        for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
            let high = decode_nibble(pair[0], SHA256_PREFIX.len() + index * 2)?;
            let low = decode_nibble(pair[1], SHA256_PREFIX.len() + index * 2 + 1)?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self::from_sha256(bytes))
    }
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(DigestVisitor)
    }
}

impl JsonSchema for Digest {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Digest".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Digest").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 71,
            "maxLength": 71,
            "pattern": SHA256_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Parse failure for a canonical [`Digest`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DigestError {
    /// The encoded digest exceeded the v1 canonical bound.
    #[error("digest is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The value did not contain an algorithm separator.
    #[error("digest must contain an algorithm and encoded value")]
    InvalidFormat,

    /// The algorithm is not supported by this `StateKnot` version.
    #[error("digest algorithm is not supported")]
    UnsupportedAlgorithm,

    /// The value used a recognized but non-canonical representation.
    #[error("digest must use lowercase canonical text")]
    NonCanonical,

    /// The hexadecimal portion had the wrong byte length.
    #[error("digest value is {actual} bytes; expected {expected}")]
    InvalidLength {
        /// Required encoded length in bytes.
        expected: usize,
        /// Observed encoded length in bytes.
        actual: usize,
    },

    /// The hexadecimal portion contained an invalid ASCII byte.
    #[error("digest contains invalid hexadecimal text at offset {index}")]
    InvalidHex {
        /// Zero-based byte offset in the complete encoded digest.
        index: usize,
    },
}

struct DigestVisitor;

impl de::Visitor<'_> for DigestVisitor {
    type Value = Digest;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical StateKnot integrity digest")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

fn decode_nibble(value: u8, index: usize) -> Result<u8, DigestError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Err(DigestError::NonCanonical),
        _ => Err(DigestError::InvalidHex { index }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, from_str, from_value, to_string};

    const EMPTY_SHA256: &str =
        "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const ABC_SHA256: &str =
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    #[test]
    fn sha256_matches_published_vectors_and_round_trips() {
        let empty = Digest::sha256([]);
        assert_eq!(empty.to_string(), EMPTY_SHA256);
        assert_eq!(empty.algorithm(), DigestAlgorithm::Sha256);
        assert_eq!(empty.as_bytes().len(), Digest::SHA256_LEN);
        assert_eq!(EMPTY_SHA256.parse::<Digest>().unwrap(), empty);

        let abc = Digest::sha256(b"abc");
        assert_eq!(abc.to_string(), ABC_SHA256);
        assert_eq!(ABC_SHA256.parse::<Digest>().unwrap(), abc);
    }

    #[test]
    fn digest_rejects_noncanonical_or_malformed_text() {
        assert_eq!("sha256".parse::<Digest>(), Err(DigestError::InvalidFormat));
        assert_eq!(
            ABC_SHA256.replace("sha256", "sha512").parse::<Digest>(),
            Err(DigestError::UnsupportedAlgorithm)
        );
        assert_eq!(
            ABC_SHA256.replace("sha256", "SHA256").parse::<Digest>(),
            Err(DigestError::NonCanonical)
        );
        assert_eq!(
            ABC_SHA256.to_ascii_uppercase().parse::<Digest>(),
            Err(DigestError::NonCanonical)
        );

        let short = "sha256:00";
        assert_eq!(
            short.parse::<Digest>(),
            Err(DigestError::InvalidLength {
                expected: Digest::SHA256_LEN * 2,
                actual: 2,
            })
        );

        let invalid = ABC_SHA256.replacen('b', "g", 1);
        assert_eq!(
            invalid.parse::<Digest>(),
            Err(DigestError::InvalidHex {
                index: SHA256_PREFIX.len(),
            })
        );

        let too_long = format!("{ABC_SHA256}0");
        assert_eq!(
            too_long.parse::<Digest>(),
            Err(DigestError::TooLong {
                max: SHA256_TEXT_LEN,
                actual: too_long.len(),
            })
        );
    }

    #[test]
    fn digest_serde_uses_and_enforces_canonical_text() {
        let digest = Digest::sha256(b"serde");
        let encoded = to_string(&digest).unwrap();
        assert_eq!(from_str::<Digest>(&encoded).unwrap(), digest);
        assert!(from_value::<Digest>(Value::Null).is_err());
        assert!(from_str::<Digest>(&format!("\"{}\"", digest.to_string().to_uppercase())).is_err());
    }

    #[test]
    fn digest_schema_matches_runtime_bounds() {
        let schema = serde_json::to_value(schemars::schema_for!(Digest)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], SHA256_TEXT_LEN);
        assert_eq!(schema["maxLength"], SHA256_TEXT_LEN);
        assert_eq!(schema["pattern"], SHA256_PATTERN);
    }
}
