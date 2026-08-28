// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Exact, checked accounting values used by budgets and provider usage.

use std::{fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::decimal::{UnsignedDecimalError, parse_bounded_u64};

const U64_DECIMAL_PATTERN: &str = "^(0|[1-9][0-9]{0,19})$";
const CURRENCY_CODE_PATTERN: &str = "^[A-Z]{3}$";

macro_rules! define_count {
    ($name:ident, $visitor:ident, $schema_name:literal, $documentation:literal) => {
        #[doc = $documentation]
        ///
        /// The JSON wire form is a canonical unsigned decimal string. Arithmetic
        /// is available only through checked methods, so release builds cannot
        /// silently wrap accounting values.
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u64);

        impl $name {
            /// Zero usage.
            pub const ZERO: Self = Self(0);

            /// Largest representable usage.
            pub const MAX: Self = Self(u64::MAX);

            /// Constructs a count from its integer representation.
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Returns the underlying integer count.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }

            /// Adds two counts, returning `None` on overflow.
            #[must_use]
            pub const fn checked_add(self, other: Self) -> Option<Self> {
                match self.0.checked_add(other.0) {
                    Some(value) => Some(Self(value)),
                    None => None,
                }
            }

            /// Subtracts two counts, returning `None` on underflow.
            #[must_use]
            pub const fn checked_sub(self, other: Self) -> Option<Self> {
                match self.0.checked_sub(other.0) {
                    Some(value) => Some(Self(value)),
                    None => None,
                }
            }

            /// Multiplies a count by an integer, returning `None` on overflow.
            #[must_use]
            pub const fn checked_mul(self, multiplier: u64) -> Option<Self> {
                match self.0.checked_mul(multiplier) {
                    Some(value) => Some(Self(value)),
                    None => None,
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = CountParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                parse_bounded_u64(value, u64::MAX)
                    .map(Self)
                    .map_err(CountParseError::from_decimal_error)
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self::new(value)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_str($visitor)
            }
        }

        struct $visitor;

        impl de::Visitor<'_> for $visitor {
            type Value = $name;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a canonical decimal string containing a {}", $schema_name)
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                value.parse().map_err(E::custom)
            }
        }

        impl JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                $schema_name.into()
            }

            fn schema_id() -> std::borrow::Cow<'static, str> {
                concat!(module_path!(), "::", $schema_name).into()
            }

            fn json_schema(_: &mut SchemaGenerator) -> Schema {
                json_schema!({
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 20,
                    "pattern": U64_DECIMAL_PATTERN
                })
            }

            fn inline_schema() -> bool {
                true
            }
        }
    };
}

define_count!(
    TokenCount,
    TokenCountVisitor,
    "TokenCount",
    "A count of model input, cached-input, reasoning, or output tokens."
);
define_count!(
    ByteCount,
    ByteCountVisitor,
    "ByteCount",
    "A count of input, output, event, checkpoint, or artifact bytes."
);

/// Parse failure for a canonical token or byte count.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CountParseError {
    /// The encoded count was empty or contained a non-decimal byte.
    #[error("count must contain only unsigned ASCII decimal digits")]
    InvalidFormat,

    /// The encoded count exceeded the maximum canonical byte length.
    #[error("count is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// The encoded count contained a leading zero.
    #[error("count contains a leading zero")]
    NonCanonical,

    /// The encoded count exceeded the `u64` range.
    #[error("count exceeds the supported range")]
    Overflow,
}

impl CountParseError {
    fn from_decimal_error(error: UnsignedDecimalError) -> Self {
        match error {
            UnsignedDecimalError::Empty | UnsignedDecimalError::InvalidCharacter { .. } => {
                Self::InvalidFormat
            }
            UnsignedDecimalError::TooLong { max, actual } => Self::TooLong { max, actual },
            UnsignedDecimalError::LeadingZero => Self::NonCanonical,
            UnsignedDecimalError::Overflow => Self::Overflow,
        }
    }
}

/// A structurally valid ISO 4217 alphabetic currency code.
///
/// This type intentionally validates the stable three-uppercase-ASCII-letter
/// structure, not mutable current-currency membership. Provider configuration
/// validates membership against a versioned ISO 4217 catalog; durable events
/// must remain readable after a code becomes historical.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CurrencyCode([u8; Self::LEN]);

impl CurrencyCode {
    /// Encoded length in bytes.
    pub const LEN: usize = 3;

    /// Validates an alphabetic currency code.
    ///
    /// # Errors
    ///
    /// Returns [`CurrencyCodeError::InvalidCharacter`] when any byte is not an
    /// uppercase ASCII letter.
    pub fn new(bytes: [u8; Self::LEN]) -> Result<Self, CurrencyCodeError> {
        if let Some(index) = bytes.iter().position(|byte| !byte.is_ascii_uppercase()) {
            return Err(CurrencyCodeError::InvalidCharacter { index });
        }
        Ok(Self(bytes))
    }

    /// Returns the three canonical ASCII bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LEN] {
        &self.0
    }
}

impl fmt::Debug for CurrencyCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CurrencyCode")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl fmt::Display for CurrencyCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = std::str::from_utf8(&self.0).map_err(|_| fmt::Error)?;
        formatter.write_str(value)
    }
}

impl FromStr for CurrencyCode {
    type Err = CurrencyCodeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != Self::LEN {
            return Err(CurrencyCodeError::InvalidLength {
                expected: Self::LEN,
                actual: value.len(),
            });
        }
        let bytes: [u8; Self::LEN] =
            value
                .as_bytes()
                .try_into()
                .map_err(|_| CurrencyCodeError::InvalidLength {
                    expected: Self::LEN,
                    actual: value.len(),
                })?;
        Self::new(bytes)
    }
}

impl Serialize for CurrencyCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for CurrencyCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(CurrencyCodeVisitor)
    }
}

struct CurrencyCodeVisitor;

impl de::Visitor<'_> for CurrencyCodeVisitor {
    type Value = CurrencyCode;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a three-letter uppercase ISO 4217 currency code")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

impl JsonSchema for CurrencyCode {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CurrencyCode".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::CurrencyCode").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 3,
            "maxLength": 3,
            "pattern": CURRENCY_CODE_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for a [`CurrencyCode`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CurrencyCodeError {
    /// The code was not exactly three bytes.
    #[error("currency code is {actual} bytes; expected {expected}")]
    InvalidLength {
        /// Required encoded length in bytes.
        expected: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// A byte was not an uppercase ASCII letter.
    #[error("currency code contains an invalid byte at offset {index}")]
    InvalidCharacter {
        /// Zero-based byte offset of the invalid value.
        index: usize,
    },
}

/// A non-negative known monetary cost in integer micro-units.
///
/// The stable JSON form is an object with exactly `currency` and
/// `micro_units`; the latter is a canonical decimal string. Unknown cost is
/// represented by absence in an enclosing usage value, not by zero money.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Money {
    currency: CurrencyCode,
    #[serde(with = "crate::decimal::serde_u64")]
    micro_units: u64,
}

impl Money {
    /// Constructs a known monetary amount.
    #[must_use]
    pub const fn new(currency: CurrencyCode, micro_units: u64) -> Self {
        Self {
            currency,
            micro_units,
        }
    }

    /// Constructs zero cost in a specific currency.
    #[must_use]
    pub const fn zero(currency: CurrencyCode) -> Self {
        Self::new(currency, 0)
    }

    /// Returns the currency code.
    #[must_use]
    pub const fn currency(self) -> CurrencyCode {
        self.currency
    }

    /// Returns the integer count of micro-units.
    #[must_use]
    pub const fn micro_units(self) -> u64 {
        self.micro_units
    }

    /// Adds amounts of the same currency without overflow.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyArithmeticError::CurrencyMismatch`] for unlike
    /// currencies or [`MoneyArithmeticError::Overflow`] when the result
    /// exceeds `u64`.
    pub fn checked_add(self, other: Self) -> Result<Self, MoneyArithmeticError> {
        self.ensure_same_currency(other)?;
        let micro_units = self
            .micro_units
            .checked_add(other.micro_units)
            .ok_or(MoneyArithmeticError::Overflow)?;
        Ok(Self::new(self.currency, micro_units))
    }

    /// Subtracts amounts of the same currency without underflow.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyArithmeticError::CurrencyMismatch`] for unlike
    /// currencies or [`MoneyArithmeticError::Underflow`] when `other` is
    /// greater than `self`.
    pub fn checked_sub(self, other: Self) -> Result<Self, MoneyArithmeticError> {
        self.ensure_same_currency(other)?;
        let micro_units = self
            .micro_units
            .checked_sub(other.micro_units)
            .ok_or(MoneyArithmeticError::Underflow)?;
        Ok(Self::new(self.currency, micro_units))
    }

    /// Multiplies this amount by an integer without overflow.
    ///
    /// # Errors
    ///
    /// Returns [`MoneyArithmeticError::Overflow`] when the result exceeds
    /// `u64`.
    pub fn checked_mul(self, multiplier: u64) -> Result<Self, MoneyArithmeticError> {
        let micro_units = self
            .micro_units
            .checked_mul(multiplier)
            .ok_or(MoneyArithmeticError::Overflow)?;
        Ok(Self::new(self.currency, micro_units))
    }

    fn ensure_same_currency(self, other: Self) -> Result<(), MoneyArithmeticError> {
        if self.currency == other.currency {
            Ok(())
        } else {
            Err(MoneyArithmeticError::CurrencyMismatch {
                left: self.currency,
                right: other.currency,
            })
        }
    }
}

impl JsonSchema for Money {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Money".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Money").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "object",
            "additionalProperties": false,
            "required": ["currency", "micro_units"],
            "properties": {
                "currency": {
                    "type": "string",
                    "minLength": 3,
                    "maxLength": 3,
                    "pattern": CURRENCY_CODE_PATTERN
                },
                "micro_units": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": 20,
                    "pattern": U64_DECIMAL_PATTERN
                }
            }
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Failure from checked monetary arithmetic.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum MoneyArithmeticError {
    /// Arithmetic was requested across different currencies.
    #[error("cannot combine {left} and {right} without an explicit conversion")]
    CurrencyMismatch {
        /// Currency of the left operand.
        left: CurrencyCode,
        /// Currency of the right operand.
        right: CurrencyCode,
    },

    /// The result exceeded the `u64` micro-unit range.
    #[error("monetary arithmetic overflowed")]
    Overflow,

    /// Subtraction would have produced a negative amount.
    #[error("monetary subtraction would produce a negative amount")]
    Underflow,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{Value, from_str, from_value, json, to_string, to_value};

    fn usd() -> CurrencyCode {
        "USD".parse().unwrap()
    }

    #[test]
    fn counts_use_exact_decimal_strings_and_checked_arithmetic() {
        for value in [0, 1, (1_u64 << 53) - 1, 1_u64 << 53, u64::MAX] {
            let tokens = TokenCount::new(value);
            let bytes = ByteCount::new(value);
            assert_eq!(tokens.to_string().parse::<TokenCount>().unwrap(), tokens);
            assert_eq!(bytes.to_string().parse::<ByteCount>().unwrap(), bytes);
            assert_eq!(to_string(&tokens).unwrap(), format!("\"{value}\""));
            assert_eq!(to_string(&bytes).unwrap(), format!("\"{value}\""));
        }

        assert_eq!(TokenCount::MAX.checked_add(TokenCount::new(1)), None);
        assert_eq!(ByteCount::ZERO.checked_sub(ByteCount::new(1)), None);
        assert_eq!(TokenCount::new(4).checked_mul(3), Some(TokenCount::new(12)));
    }

    #[test]
    fn counts_reject_noncanonical_or_inexact_json() {
        for value in ["", "00", "01", "+1", "-1", "1.0", "1e3", " 1"] {
            assert!(value.parse::<TokenCount>().is_err(), "accepted {value:?}");
            assert!(value.parse::<ByteCount>().is_err(), "accepted {value:?}");
        }
        assert_eq!(
            "18446744073709551616".parse::<TokenCount>(),
            Err(CountParseError::Overflow)
        );
        assert!(from_value::<TokenCount>(json!(1)).is_err());
        assert!(from_value::<ByteCount>(Value::Null).is_err());
    }

    #[test]
    fn count_schemas_match_runtime_wire_bounds() {
        for schema in [
            to_value(schemars::schema_for!(TokenCount)).unwrap(),
            to_value(schemars::schema_for!(ByteCount)).unwrap(),
        ] {
            assert_eq!(schema["type"], "string");
            assert_eq!(schema["minLength"], 1);
            assert_eq!(schema["maxLength"], 20);
            assert_eq!(schema["pattern"], U64_DECIMAL_PATTERN);
        }
    }

    #[test]
    fn currency_codes_enforce_stable_iso_structure() {
        for value in ["USD", "EUR", "XAU", "BGN"] {
            let code = value.parse::<CurrencyCode>().unwrap();
            assert_eq!(code.to_string(), value);
            assert_eq!(to_string(&code).unwrap(), format!("\"{value}\""));
        }

        for value in ["", "US", "USDD", "usd", "UsD", "U1D", "¥EN", " USD"] {
            assert!(value.parse::<CurrencyCode>().is_err(), "accepted {value:?}");
        }
        assert!(from_value::<CurrencyCode>(json!("usd")).is_err());
        assert!(from_value::<CurrencyCode>(json!(123)).is_err());
    }

    #[test]
    fn money_round_trips_and_rejects_ambiguous_objects() {
        let money = Money::new(usd(), 1_234_567);
        let encoded = to_value(money).unwrap();
        assert_eq!(
            encoded,
            json!({"currency": "USD", "micro_units": "1234567"})
        );
        assert_eq!(from_value::<Money>(encoded).unwrap(), money);

        for invalid in [
            json!({"currency": "USD", "micro_units": 123}),
            json!({"currency": "usd", "micro_units": "123"}),
            json!({"currency": "USD", "micro_units": "01"}),
            json!({"currency": "USD", "micro_units": "18446744073709551616"}),
            json!({"currency": "USD", "micro_units": "1", "extra": true}),
            json!({"currency": "USD"}),
        ] {
            assert!(
                from_value::<Money>(invalid.clone()).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn money_arithmetic_requires_one_currency_and_never_wraps() {
        let eur: CurrencyCode = "EUR".parse().unwrap();
        let one = Money::new(usd(), 1);
        let two = Money::new(usd(), 2);

        assert_eq!(one.checked_add(two), Ok(Money::new(usd(), 3)));
        assert_eq!(two.checked_sub(one), Ok(one));
        assert_eq!(two.checked_mul(3), Ok(Money::new(usd(), 6)));
        assert_eq!(
            Money::new(usd(), u64::MAX).checked_add(one),
            Err(MoneyArithmeticError::Overflow)
        );
        assert_eq!(one.checked_sub(two), Err(MoneyArithmeticError::Underflow));
        assert_eq!(
            one.checked_add(Money::new(eur, 1)),
            Err(MoneyArithmeticError::CurrencyMismatch {
                left: usd(),
                right: eur,
            })
        );
    }

    #[test]
    fn money_schema_requires_exact_fields_and_string_amount() {
        let schema = to_value(schemars::schema_for!(Money)).unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["required"], json!(["currency", "micro_units"]));
        assert_eq!(
            schema["properties"]["currency"]["pattern"],
            CURRENCY_CODE_PATTERN
        );
        assert_eq!(
            schema["properties"]["micro_units"]["pattern"],
            U64_DECIMAL_PATTERN
        );
    }

    proptest! {
        #[test]
        fn checked_count_arithmetic_matches_u64(left in any::<u64>(), right in any::<u64>()) {
            let left_tokens = TokenCount::new(left);
            let right_tokens = TokenCount::new(right);
            prop_assert_eq!(
                left_tokens.checked_add(right_tokens).map(TokenCount::get),
                left.checked_add(right)
            );
            prop_assert_eq!(
                left_tokens.checked_sub(right_tokens).map(TokenCount::get),
                left.checked_sub(right)
            );

            let left_bytes = ByteCount::new(left);
            let right_bytes = ByteCount::new(right);
            prop_assert_eq!(
                left_bytes.checked_add(right_bytes).map(ByteCount::get),
                left.checked_add(right)
            );
            prop_assert_eq!(
                left_bytes.checked_sub(right_bytes).map(ByteCount::get),
                left.checked_sub(right)
            );
        }

        #[test]
        fn count_wire_round_trip_preserves_all_u64_values(value in any::<u64>()) {
            let count = TokenCount::new(value);
            let encoded = to_string(&count).unwrap();
            prop_assert_eq!(from_str::<TokenCount>(&encoded).unwrap(), count);
        }

        #[test]
        fn checked_money_arithmetic_matches_u64(left in any::<u64>(), right in any::<u64>()) {
            let left_money = Money::new(usd(), left);
            let right_money = Money::new(usd(), right);
            prop_assert_eq!(
                left_money.checked_add(right_money).ok().map(Money::micro_units),
                left.checked_add(right)
            );
            prop_assert_eq!(
                left_money.checked_sub(right_money).ok().map(Money::micro_units),
                left.checked_sub(right)
            );
        }
    }
}
