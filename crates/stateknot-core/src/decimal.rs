// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Internal canonical decimal-string support for full-width integers.

use std::fmt;

use serde::{Deserializer, Serializer, de};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UnsignedDecimalError {
    Empty,
    TooLong { max: usize, actual: usize },
    InvalidCharacter { index: usize },
    LeadingZero,
    Overflow,
}

impl fmt::Display for UnsignedDecimalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("decimal string must not be empty"),
            Self::TooLong { max, actual } => {
                write!(
                    formatter,
                    "decimal string is {actual} bytes; maximum is {max}"
                )
            }
            Self::InvalidCharacter { index } => {
                write!(
                    formatter,
                    "decimal string contains an invalid byte at offset {index}"
                )
            }
            Self::LeadingZero => formatter.write_str("decimal string contains a leading zero"),
            Self::Overflow => formatter.write_str("decimal string exceeds the supported range"),
        }
    }
}

pub(crate) fn parse_bounded_u64(value: &str, maximum: u64) -> Result<u64, UnsignedDecimalError> {
    if value.is_empty() {
        return Err(UnsignedDecimalError::Empty);
    }

    let max_len = decimal_digits(maximum);
    if value.len() > max_len {
        return Err(UnsignedDecimalError::TooLong {
            max: max_len,
            actual: value.len(),
        });
    }

    if let Some(index) = value.bytes().position(|byte| !byte.is_ascii_digit()) {
        return Err(UnsignedDecimalError::InvalidCharacter { index });
    }
    if value.len() > 1 && value.starts_with('0') {
        return Err(UnsignedDecimalError::LeadingZero);
    }

    let parsed = value
        .parse::<u64>()
        .map_err(|_| UnsignedDecimalError::Overflow)?;
    if parsed > maximum {
        return Err(UnsignedDecimalError::Overflow);
    }
    Ok(parsed)
}

const fn decimal_digits(mut value: u64) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

pub(crate) mod serde_u64 {
    use super::{Deserializer, Serializer, de, fmt, parse_bounded_u64};

    // Serde's `with` module contract passes fields by reference.
    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub(crate) fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(value)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(UnsignedDecimalVisitor)
    }

    struct UnsignedDecimalVisitor;

    impl de::Visitor<'_> for UnsignedDecimalVisitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a canonical decimal string containing a u64")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            parse_bounded_u64(value, u64::MAX).map_err(E::custom)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_parser_enforces_canonical_text_and_limit() {
        assert_eq!(parse_bounded_u64("0", u64::MAX), Ok(0));
        assert_eq!(
            parse_bounded_u64("18446744073709551615", u64::MAX),
            Ok(u64::MAX)
        );
        assert_eq!(
            parse_bounded_u64("", u64::MAX),
            Err(UnsignedDecimalError::Empty)
        );
        assert_eq!(
            parse_bounded_u64("00", u64::MAX),
            Err(UnsignedDecimalError::LeadingZero)
        );
        assert_eq!(
            parse_bounded_u64("1x", u64::MAX),
            Err(UnsignedDecimalError::InvalidCharacter { index: 1 })
        );
        assert_eq!(
            parse_bounded_u64("18446744073709551616", u64::MAX),
            Err(UnsignedDecimalError::Overflow)
        );
        assert_eq!(
            parse_bounded_u64("10", 9),
            Err(UnsignedDecimalError::TooLong { max: 1, actual: 2 })
        );
        assert_eq!(
            parse_bounded_u64("9", 8),
            Err(UnsignedDecimalError::Overflow)
        );
    }
}
