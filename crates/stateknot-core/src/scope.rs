// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded OAuth-compatible authorization scopes and deterministic scope sets.

use std::{
    borrow::Borrow,
    collections::{BTreeSet, btree_set},
    fmt,
    str::FromStr,
};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess},
};
use thiserror::Error;

const SCOPE_PATTERN: &str = r"^[\u0021\u0023-\u005B\u005D-\u007E]{1,256}$";

/// A bounded, case-sensitive OAuth 2.0 scope token.
///
/// The grammar is RFC 6749 `scope-token`: visible ASCII other than space,
/// double quote, and backslash. `StateKnot` additionally limits one token to
/// 256 bytes so remote authorization data cannot create unbounded domain
/// values.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Scope(Box<str>);

impl Scope {
    /// Maximum encoded length in bytes.
    pub const MAX_LEN: usize = 256;

    /// Validates and constructs a scope without copying a `String`.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeError`] when `value` is empty, too long, or does not
    /// conform to the RFC 6749 `scope-token` grammar.
    pub fn new(value: impl Into<String>) -> Result<Self, ScopeError> {
        let value = value.into();
        validate_scope(&value)?;
        Ok(Self(value.into_boxed_str()))
    }

    /// Returns the exact, case-sensitive scope token.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Scope {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for Scope {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for Scope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Scope")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Scope {
    type Err = ScopeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for Scope {
    type Error = ScopeError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for Scope {
    type Error = ScopeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Scope> for String {
    fn from(value: Scope) -> Self {
        value.0.into()
    }
}

impl Serialize for Scope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Scope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(ScopeVisitor)
    }
}

struct ScopeVisitor;

impl de::Visitor<'_> for ScopeVisitor {
    type Value = Scope;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded RFC 6749 scope token")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Scope::try_from(value).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Scope::try_from(value).map_err(E::custom)
    }
}

impl JsonSchema for Scope {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Scope".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Scope").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 256,
            "pattern": SCOPE_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation failure for a [`Scope`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ScopeError {
    /// The scope contained no bytes.
    #[error("scope must not be empty")]
    Empty,

    /// The scope exceeded [`Scope::MAX_LEN`].
    #[error("scope is {actual} bytes; maximum is {max}")]
    TooLong {
        /// Maximum accepted byte length.
        max: usize,
        /// Observed byte length.
        actual: usize,
    },

    /// A byte did not belong to the RFC 6749 `scope-token` grammar.
    #[error("scope contains an invalid byte at offset {index}")]
    InvalidByte {
        /// Zero-based byte offset of the first invalid byte.
        index: usize,
    },
}

fn validate_scope(value: &str) -> Result<(), ScopeError> {
    if value.is_empty() {
        return Err(ScopeError::Empty);
    }
    if value.len() > Scope::MAX_LEN {
        return Err(ScopeError::TooLong {
            max: Scope::MAX_LEN,
            actual: value.len(),
        });
    }

    if let Some((index, _)) = value
        .bytes()
        .enumerate()
        .find(|(_, byte)| !matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
    {
        return Err(ScopeError::InvalidByte { index });
    }

    Ok(())
}

/// A bounded set of unique scopes with deterministic canonical ordering.
///
/// The JSON wire form is an array sorted by exact ASCII byte order. Duplicate
/// input is rejected rather than silently deduplicated. These properties make
/// authorization decisions, audit records, and canonical hashes reproducible.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScopeSet(BTreeSet<Scope>);

impl ScopeSet {
    /// Maximum number of distinct scopes in one set.
    pub const MAX_LEN: usize = 128;

    /// Returns an empty scope set.
    #[must_use]
    pub const fn empty() -> Self {
        Self(BTreeSet::new())
    }

    /// Constructs a set from already validated scopes.
    ///
    /// # Errors
    ///
    /// Returns [`ScopeSetError`] on the first duplicate or when more than
    /// [`ScopeSet::MAX_LEN`] distinct entries are observed. Iteration stops at
    /// that point, so an unbounded iterator cannot cause unbounded work.
    pub fn try_new<I>(scopes: I) -> Result<Self, ScopeSetError>
    where
        I: IntoIterator<Item = Scope>,
    {
        let mut set = BTreeSet::new();
        for scope in scopes {
            if set.contains(&scope) {
                return Err(ScopeSetError::Duplicate { scope });
            }
            if set.len() == Self::MAX_LEN {
                return Err(ScopeSetError::TooMany {
                    max: Self::MAX_LEN,
                    observed: Self::MAX_LEN + 1,
                });
            }
            set.insert(scope);
        }
        Ok(Self(set))
    }

    /// Returns the number of scopes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` when no authority is represented.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns `true` when the exact, case-sensitive scope is present.
    #[must_use]
    pub fn contains(&self, scope: &Scope) -> bool {
        self.0.contains(scope)
    }

    /// Returns scopes in deterministic ASCII byte order.
    pub fn iter(&self) -> btree_set::Iter<'_, Scope> {
        self.0.iter()
    }

    /// Returns `true` when every scope in this set is present in `other`.
    #[must_use]
    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }

    /// Returns the scopes granted by both sets.
    ///
    /// Intersection can only narrow authority and cannot exceed either input's
    /// validated size bound.
    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        Self(self.0.intersection(&other.0).cloned().collect())
    }
}

impl<'a> IntoIterator for &'a ScopeSet {
    type Item = &'a Scope;
    type IntoIter = btree_set::Iter<'a, Scope>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<Vec<Scope>> for ScopeSet {
    type Error = ScopeSetError;

    fn try_from(scopes: Vec<Scope>) -> Result<Self, Self::Error> {
        Self::try_new(scopes)
    }
}

impl Serialize for ScopeSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for ScopeSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ScopeSetVisitor)
    }
}

struct ScopeSetVisitor;

impl<'de> de::Visitor<'de> for ScopeSetVisitor {
    type Value = ScopeSet;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {} unique scopes",
            ScopeSet::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut scopes = BTreeSet::new();
        while let Some(scope) = sequence.next_element::<Scope>()? {
            if scopes.contains(&scope) {
                return Err(de::Error::custom(ScopeSetError::Duplicate { scope }));
            }
            if scopes.len() == ScopeSet::MAX_LEN {
                return Err(de::Error::custom(ScopeSetError::TooMany {
                    max: ScopeSet::MAX_LEN,
                    observed: ScopeSet::MAX_LEN + 1,
                }));
            }
            scopes.insert(scope);
        }
        Ok(ScopeSet(scopes))
    }
}

impl JsonSchema for ScopeSet {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ScopeSet".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ScopeSet").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<Scope>(),
            "maxItems": 128,
            "uniqueItems": true
        })
    }
}

/// Construction or deserialization failure for a [`ScopeSet`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ScopeSetError {
    /// The input repeated a scope.
    #[error("scope set contains duplicate scope {scope:?}")]
    Duplicate {
        /// Repeated scope.
        scope: Scope,
    },

    /// The input exceeded [`ScopeSet::MAX_LEN`].
    #[error("scope set contains at least {observed} distinct scopes; maximum is {max}")]
    TooMany {
        /// Maximum accepted number of distinct scopes.
        max: usize,
        /// Minimum number of distinct scopes observed before validation stopped.
        observed: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    fn scope_set_from_bits(bits: u64) -> ScopeSet {
        ScopeSet::try_new(
            (0..u64::BITS)
                .filter(|index| bits & (1_u64 << index) != 0)
                .map(|index| format!("scope:{index:02}").parse::<Scope>().unwrap()),
        )
        .unwrap()
    }

    #[test]
    fn scopes_accept_exactly_the_bounded_rfc_6749_grammar() {
        let all_allowed = (0x21_u8..=0x7e)
            .filter(|byte| !matches!(byte, b'"' | b'\\'))
            .map(char::from)
            .collect::<String>();
        let scope = all_allowed.parse::<Scope>().unwrap();
        assert_eq!(scope.as_str(), all_allowed);

        for value in [
            "read",
            "ops:restart",
            "https://api.example.com/auth/data.read",
            "Read",
        ] {
            let scope = value.parse::<Scope>().unwrap();
            assert_eq!(scope.as_str(), value);
            assert_eq!(to_value(&scope).unwrap(), Value::from(value));
        }

        let maximum = "a".repeat(Scope::MAX_LEN);
        assert_eq!(maximum.parse::<Scope>().unwrap().as_str(), maximum);
    }

    #[test]
    fn scopes_reject_out_of_contract_input() {
        assert_eq!("".parse::<Scope>(), Err(ScopeError::Empty));
        assert_eq!(
            "a".repeat(Scope::MAX_LEN + 1).parse::<Scope>(),
            Err(ScopeError::TooLong {
                max: Scope::MAX_LEN,
                actual: Scope::MAX_LEN + 1,
            })
        );

        for (value, index) in [
            ("data read", 4),
            ("data\"read", 4),
            ("data\\read", 4),
            ("data\nread", 4),
            ("读取", 0),
        ] {
            assert_eq!(
                value.parse::<Scope>(),
                Err(ScopeError::InvalidByte { index }),
                "accepted {value:?}"
            );
        }

        for byte in 0_u8..=0x7f {
            if matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e) {
                continue;
            }
            let value = String::from_utf8(vec![byte]).unwrap();
            assert_eq!(
                value.parse::<Scope>(),
                Err(ScopeError::InvalidByte { index: 0 }),
                "accepted ASCII byte 0x{byte:02x}"
            );
        }
    }

    #[test]
    fn scope_serde_and_schema_enforce_the_wire_contract() {
        let scope = "ops:restart".parse::<Scope>().unwrap();
        assert_eq!(from_value::<Scope>(json!(scope.as_str())).unwrap(), scope);
        assert!(from_value::<Scope>(json!(42)).is_err());
        assert!(from_value::<Scope>(Value::Null).is_err());

        let schema = to_value(schemars::schema_for!(Scope)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 1);
        assert_eq!(schema["maxLength"], Scope::MAX_LEN);
        assert_eq!(schema["pattern"], SCOPE_PATTERN);
    }

    #[test]
    fn scope_sets_reject_duplicates_and_stop_at_the_limit() {
        let read = "read".parse::<Scope>().unwrap();
        assert_eq!(
            ScopeSet::try_new([read.clone(), read.clone()]),
            Err(ScopeSetError::Duplicate { scope: read })
        );

        let maximum = ScopeSet::try_new(
            (0..ScopeSet::MAX_LEN).map(|index| format!("scope:{index}").parse().unwrap()),
        )
        .unwrap();
        assert_eq!(maximum.len(), ScopeSet::MAX_LEN);

        let oversized =
            (0..=ScopeSet::MAX_LEN).map(|index| format!("scope:{index}").parse().unwrap());
        assert_eq!(
            ScopeSet::try_new(oversized),
            Err(ScopeSetError::TooMany {
                max: ScopeSet::MAX_LEN,
                observed: ScopeSet::MAX_LEN + 1,
            })
        );

        let oversized_wire = Value::Array(
            (0..=ScopeSet::MAX_LEN)
                .map(|index| Value::from(format!("scope:{index}")))
                .collect(),
        );
        assert!(from_value::<ScopeSet>(oversized_wire).is_err());
    }

    #[test]
    fn scope_sets_have_a_sorted_strict_wire_form() {
        let set = ScopeSet::try_new(
            ["write", "admin", "read"].map(|value| value.parse::<Scope>().unwrap()),
        )
        .unwrap();

        assert_eq!(to_value(&set).unwrap(), json!(["admin", "read", "write"]));
        assert_eq!(
            from_value::<ScopeSet>(json!(["write", "admin", "read"])).unwrap(),
            set
        );
        assert!(from_value::<ScopeSet>(json!(["read", "read"])).is_err());
        assert!(from_value::<ScopeSet>(json!("read write")).is_err());
        assert!(from_value::<ScopeSet>(Value::Null).is_err());

        let schema = to_value(schemars::schema_for!(ScopeSet)).unwrap();
        assert_eq!(schema["type"], "array");
        assert_eq!(schema["maxItems"], ScopeSet::MAX_LEN);
        assert_eq!(schema["uniqueItems"], true);
        assert_eq!(schema["items"]["type"], "string");
        assert_eq!(schema["items"]["pattern"], SCOPE_PATTERN);
    }

    proptest! {
        #[test]
        fn intersection_is_commutative_and_never_widens(left in any::<u64>(), right in any::<u64>()) {
            let left = scope_set_from_bits(left);
            let right = scope_set_from_bits(right);
            let intersection = left.intersection(&right);

            prop_assert!(intersection.is_subset(&left));
            prop_assert!(intersection.is_subset(&right));
            prop_assert_eq!(&intersection, &right.intersection(&left));
            prop_assert_eq!(&left.intersection(&left), &left);
        }
    }
}
