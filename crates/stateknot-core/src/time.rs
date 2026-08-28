// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Bounded, canonical time values for durable `StateKnot` contracts.

use std::{
    fmt,
    ops::Range,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

const TIMESTAMP_LEN: usize = 27;
const TIMESTAMP_PATTERN: &str =
    "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z$";
const MICROS_PER_SECOND: i128 = 1_000_000;

/// A UTC instant with canonical microsecond precision.
///
/// The stable wire form is exactly `YYYY-MM-DDTHH:MM:SS.ffffffZ`. Years are
/// bounded to `0000..=9999`, offsets other than `Z` are rejected, leap seconds
/// are not represented, and precision is never silently truncated.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Earliest supported instant: `0000-01-01T00:00:00.000000Z`.
    pub const MIN: Self = Self(-62_167_219_200_000_000);

    /// Latest supported instant: `9999-12-31T23:59:59.999999Z`.
    pub const MAX: Self = Self(253_402_300_799_999_999);

    /// Constructs a timestamp from microseconds relative to the Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns [`TimestampError::OutOfRange`] when the value lies outside the
    /// canonical four-digit year range.
    pub const fn from_unix_micros(value: i64) -> Result<Self, TimestampError> {
        if value < Self::MIN.0 || value > Self::MAX.0 {
            return Err(TimestampError::OutOfRange);
        }
        Ok(Self(value))
    }

    /// Returns microseconds relative to the Unix epoch.
    #[must_use]
    pub const fn unix_micros(self) -> i64 {
        self.0
    }

    /// Converts this timestamp to [`SystemTime`].
    ///
    /// # Errors
    ///
    /// Returns [`TimestampError::SystemTimeOutOfRange`] if the platform cannot
    /// represent this otherwise valid timestamp.
    pub fn to_system_time(self) -> Result<SystemTime, TimestampError> {
        if self.0 >= 0 {
            UNIX_EPOCH
                .checked_add(Duration::from_micros(self.0.unsigned_abs()))
                .ok_or(TimestampError::SystemTimeOutOfRange)
        } else {
            UNIX_EPOCH
                .checked_sub(Duration::from_micros(self.0.unsigned_abs()))
                .ok_or(TimestampError::SystemTimeOutOfRange)
        }
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Timestamp")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let date_time = DateTime::<Utc>::from_timestamp_micros(self.0).ok_or(fmt::Error)?;
        write!(
            formatter,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
            date_time.year(),
            date_time.month(),
            date_time.day(),
            date_time.hour(),
            date_time.minute(),
            date_time.second(),
            date_time.nanosecond() / 1_000,
        )
    }
}

impl FromStr for Timestamp {
    type Err = TimestampError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != TIMESTAMP_LEN {
            return Err(TimestampError::InvalidLength {
                expected: TIMESTAMP_LEN,
                actual: value.len(),
            });
        }
        let bytes = value.as_bytes();
        if bytes[4] != b'-'
            || bytes[7] != b'-'
            || bytes[10] != b'T'
            || bytes[13] != b':'
            || bytes[16] != b':'
            || bytes[19] != b'.'
            || bytes[26] != b'Z'
        {
            return Err(TimestampError::InvalidFormat);
        }

        let year =
            i32::try_from(parse_decimal(bytes, 0..4)?).map_err(|_| TimestampError::InvalidValue)?;
        let month = parse_decimal(bytes, 5..7)?;
        let day = parse_decimal(bytes, 8..10)?;
        let hour = parse_decimal(bytes, 11..13)?;
        let minute = parse_decimal(bytes, 14..16)?;
        let second = parse_decimal(bytes, 17..19)?;
        let microsecond = parse_decimal(bytes, 20..26)?;

        let date = NaiveDate::from_ymd_opt(year, month, day).ok_or(TimestampError::InvalidValue)?;
        let date_time = date
            .and_hms_micro_opt(hour, minute, second, microsecond)
            .ok_or(TimestampError::InvalidValue)?;
        let unix_micros = date_time.and_utc().timestamp_micros();
        Self::from_unix_micros(unix_micros)
    }
}

impl TryFrom<SystemTime> for Timestamp {
    type Error = TimestampError;

    fn try_from(value: SystemTime) -> Result<Self, Self::Error> {
        match value.duration_since(UNIX_EPOCH) {
            Ok(duration) => timestamp_from_duration(duration, false),
            Err(error) => timestamp_from_duration(error.duration(), true),
        }
    }
}

impl TryFrom<Timestamp> for SystemTime {
    type Error = TimestampError;

    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        value.to_system_time()
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(TimestampVisitor)
    }
}

impl JsonSchema for Timestamp {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Timestamp".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Timestamp").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "format": "date-time",
            "minLength": 27,
            "maxLength": 27,
            "pattern": TIMESTAMP_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation or conversion failure for a [`Timestamp`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TimestampError {
    /// The encoded timestamp did not have the canonical byte length.
    #[error("timestamp is {actual} bytes; expected {expected}")]
    InvalidLength {
        /// Required encoded length in bytes.
        expected: usize,
        /// Observed encoded length in bytes.
        actual: usize,
    },

    /// The timestamp did not use the canonical separators and UTC suffix.
    #[error("timestamp must use canonical UTC text")]
    InvalidFormat,

    /// A calendar or clock component was invalid.
    #[error("timestamp contains an invalid calendar or clock value")]
    InvalidValue,

    /// The instant was outside the supported four-digit year range.
    #[error("timestamp is outside the supported range")]
    OutOfRange,

    /// Conversion would silently discard sub-microsecond precision.
    #[error("timestamp contains sub-microsecond precision")]
    SubmicrosecondPrecision,

    /// The platform's [`SystemTime`] range could not represent the instant.
    #[error("timestamp cannot be represented by SystemTime on this platform")]
    SystemTimeOutOfRange,
}

struct TimestampVisitor;

impl de::Visitor<'_> for TimestampVisitor {
    type Value = Timestamp;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical UTC timestamp with six fractional digits")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

fn parse_decimal(bytes: &[u8], range: Range<usize>) -> Result<u32, TimestampError> {
    bytes[range]
        .iter()
        .try_fold(0_u32, |value, byte| {
            byte.is_ascii_digit()
                .then_some(value * 10 + u32::from(*byte - b'0'))
        })
        .ok_or(TimestampError::InvalidFormat)
}

fn timestamp_from_duration(
    duration: Duration,
    before_epoch: bool,
) -> Result<Timestamp, TimestampError> {
    if duration.subsec_nanos() % 1_000 != 0 {
        return Err(TimestampError::SubmicrosecondPrecision);
    }
    let magnitude =
        i128::from(duration.as_secs()) * MICROS_PER_SECOND + i128::from(duration.subsec_micros());
    let value = if before_epoch { -magnitude } else { magnitude };
    let value = i64::try_from(value).map_err(|_| TimestampError::OutOfRange)?;
    Timestamp::from_unix_micros(value)
}

/// A non-negative duration represented as signed 64-bit milliseconds.
///
/// Its stable serialized form is a JSON integer. Construction from
/// [`Duration`] rejects sub-millisecond precision instead of truncating it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DurationMillis(i64);

impl DurationMillis {
    /// Zero milliseconds.
    pub const ZERO: Self = Self(0);

    /// Largest supported duration.
    pub const MAX: Self = Self(i64::MAX);

    /// Validates a signed millisecond count.
    ///
    /// # Errors
    ///
    /// Returns [`DurationMillisError::Negative`] for a negative value.
    pub const fn new(milliseconds: i64) -> Result<Self, DurationMillisError> {
        if milliseconds < 0 {
            return Err(DurationMillisError::Negative { milliseconds });
        }
        Ok(Self(milliseconds))
    }

    /// Returns the signed millisecond count used by durable storage.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }

    /// Adds two durations without overflow.
    #[must_use]
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        match self.0.checked_add(other.0) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Subtracts two durations without producing a negative value.
    #[must_use]
    pub const fn checked_sub(self, other: Self) -> Option<Self> {
        match self.0.checked_sub(other.0) {
            Some(value) if value >= 0 => Some(Self(value)),
            _ => None,
        }
    }
}

impl TryFrom<i64> for DurationMillis {
    type Error = DurationMillisError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<u64> for DurationMillis {
    type Error = DurationMillisError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let value = i64::try_from(value).map_err(|_| DurationMillisError::TooLarge)?;
        Ok(Self(value))
    }
}

impl TryFrom<Duration> for DurationMillis {
    type Error = DurationMillisError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        if value.subsec_nanos() % 1_000_000 != 0 {
            return Err(DurationMillisError::SubmillisecondPrecision);
        }
        let milliseconds =
            i64::try_from(value.as_millis()).map_err(|_| DurationMillisError::TooLarge)?;
        Ok(Self(milliseconds))
    }
}

impl From<DurationMillis> for i64 {
    fn from(value: DurationMillis) -> Self {
        value.0
    }
}

impl From<DurationMillis> for Duration {
    fn from(value: DurationMillis) -> Self {
        Self::from_millis(value.0.unsigned_abs())
    }
}

impl Serialize for DurationMillis {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for DurationMillis {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

impl JsonSchema for DurationMillis {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "DurationMillis".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::DurationMillis").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "integer",
            "minimum": 0,
            "maximum": 9_223_372_036_854_775_807_i64
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Validation or conversion failure for [`DurationMillis`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurationMillisError {
    /// The duration was negative.
    #[error("duration must not be negative")]
    Negative {
        /// Rejected signed millisecond value.
        milliseconds: i64,
    },

    /// The duration exceeded the signed 64-bit millisecond range.
    #[error("duration exceeds the supported millisecond range")]
    TooLarge,

    /// Conversion would silently discard sub-millisecond precision.
    #[error("duration contains sub-millisecond precision")]
    SubmillisecondPrecision,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, from_str, from_value, to_string, to_value};

    #[test]
    fn timestamps_round_trip_canonical_boundaries() {
        for (text, micros) in [
            ("0000-01-01T00:00:00.000000Z", Timestamp::MIN.0),
            ("1969-12-31T23:59:59.999999Z", -1),
            ("1970-01-01T00:00:00.000000Z", 0),
            ("2000-02-29T12:34:56.123456Z", 951_827_696_123_456),
            ("9999-12-31T23:59:59.999999Z", Timestamp::MAX.0),
        ] {
            let timestamp = text.parse::<Timestamp>().unwrap();
            assert_eq!(timestamp.unix_micros(), micros);
            assert_eq!(timestamp.to_string(), text);
            assert_eq!(Timestamp::from_unix_micros(micros).unwrap(), timestamp);
        }
    }

    #[test]
    fn timestamps_reject_noncanonical_or_invalid_values() {
        for value in [
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.000Z",
            "1970-01-01t00:00:00.000000Z",
            "1970-01-01T00:00:00.000000z",
            "1970-01-01T00:00:00.000000+00:00",
            "2025-02-29T00:00:00.000000Z",
            "2024-13-01T00:00:00.000000Z",
            "2024-01-01T24:00:00.000000Z",
            "2024-01-01T23:60:00.000000Z",
            "2024-01-01T23:59:60.000000Z",
            "٢٠٢٤-01-01T00:00:00.000000Z",
        ] {
            assert!(value.parse::<Timestamp>().is_err(), "accepted {value:?}");
        }

        assert_eq!(
            Timestamp::from_unix_micros(Timestamp::MIN.0 - 1),
            Err(TimestampError::OutOfRange)
        );
        assert_eq!(
            Timestamp::from_unix_micros(Timestamp::MAX.0 + 1),
            Err(TimestampError::OutOfRange)
        );
    }

    #[test]
    fn timestamp_system_time_conversion_is_exact() {
        for micros in [-1_i64, 0, 1, 1_234_567] {
            let timestamp = Timestamp::from_unix_micros(micros).unwrap();
            let system_time = timestamp.to_system_time().unwrap();
            assert_eq!(Timestamp::try_from(system_time).unwrap(), timestamp);
        }

        // Windows `SystemTime` has 100 ns resolution, so use the smallest
        // sub-microsecond value that remains observable on every CI target.
        let submicrosecond = UNIX_EPOCH.checked_add(Duration::from_nanos(100)).unwrap();
        assert_eq!(
            Timestamp::try_from(submicrosecond),
            Err(TimestampError::SubmicrosecondPrecision)
        );
    }

    #[test]
    fn timestamp_serde_and_schema_enforce_the_wire_contract() {
        let timestamp = Timestamp::from_unix_micros(0).unwrap();
        let encoded = to_string(&timestamp).unwrap();
        assert_eq!(encoded, "\"1970-01-01T00:00:00.000000Z\"");
        assert_eq!(from_str::<Timestamp>(&encoded).unwrap(), timestamp);
        assert!(from_value::<Timestamp>(Value::Null).is_err());

        let schema = to_value(schemars::schema_for!(Timestamp)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["format"], "date-time");
        assert_eq!(schema["minLength"], TIMESTAMP_LEN);
        assert_eq!(schema["maxLength"], TIMESTAMP_LEN);
        assert_eq!(schema["pattern"], TIMESTAMP_PATTERN);
    }

    #[test]
    fn duration_millis_enforces_bounds_and_checked_arithmetic() {
        assert_eq!(DurationMillis::new(0).unwrap(), DurationMillis::ZERO);
        assert_eq!(
            DurationMillis::new(-1),
            Err(DurationMillisError::Negative { milliseconds: -1 })
        );
        assert_eq!(
            DurationMillis::try_from(i64::MAX as u64 + 1),
            Err(DurationMillisError::TooLarge)
        );

        let one = DurationMillis::new(1).unwrap();
        assert_eq!(one.checked_add(one).unwrap().as_i64(), 2);
        assert_eq!(one.checked_sub(one), Some(DurationMillis::ZERO));
        assert_eq!(one.checked_sub(DurationMillis::new(2).unwrap()), None);
        assert_eq!(DurationMillis::MAX.checked_add(one), None);
    }

    #[test]
    fn duration_millis_std_conversion_never_truncates() {
        let exact = Duration::from_millis(1_234);
        let encoded = DurationMillis::try_from(exact).unwrap();
        assert_eq!(encoded.as_i64(), 1_234);
        assert_eq!(Duration::from(encoded), exact);

        assert_eq!(
            DurationMillis::try_from(Duration::from_nanos(1)),
            Err(DurationMillisError::SubmillisecondPrecision)
        );
    }

    #[test]
    fn duration_millis_serde_and_schema_enforce_the_wire_contract() {
        let duration = DurationMillis::new(42).unwrap();
        assert_eq!(to_string(&duration).unwrap(), "42");
        assert_eq!(from_str::<DurationMillis>("42").unwrap(), duration);
        assert!(from_str::<DurationMillis>("-1").is_err());
        assert!(from_str::<DurationMillis>("1.5").is_err());
        assert!(from_str::<DurationMillis>("\"42\"").is_err());

        let schema = to_value(schemars::schema_for!(DurationMillis)).unwrap();
        assert_eq!(schema["type"], "integer");
        assert_eq!(schema["minimum"], 0);
        assert_eq!(schema["maximum"], i64::MAX);
    }
}
