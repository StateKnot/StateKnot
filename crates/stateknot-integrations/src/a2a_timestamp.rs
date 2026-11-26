// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, str::FromStr};

use a2a as wire;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::A2aContractError;

/// An A2A UTC instant, ordered at nanosecond precision, including leap seconds.
///
/// Parsing accepts the A2A SDK timestamp syntax and normalizes numeric offsets
/// to UTC. Display and Serde use a UTC string with zero, three, six or nine
/// fractional digits and a trailing `Z`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct A2aTimestamp {
    seconds: i64,
    nanoseconds: u32,
}

impl A2aTimestamp {
    /// Constructs an instant in the SDK's UTC year range −262143 through 262142.
    /// An ordinary second accepts 0 through 999,999,999 nanoseconds. A leap
    /// second uses the preceding minute's last Unix second with 1,000,000,000
    /// through 1,999,999,999 nanoseconds.
    #[must_use]
    pub fn from_unix_parts(seconds: i64, nanoseconds: u32) -> Option<Self> {
        let utc = time::OffsetDateTime::from_unix_timestamp(seconds).ok()?;
        if !(-262_143..=262_142).contains(&utc.year())
            || nanoseconds >= 2_000_000_000
            || (nanoseconds >= 1_000_000_000 && utc.second() != 59)
        {
            return None;
        }
        Some(Self {
            seconds,
            nanoseconds,
        })
    }

    /// Returns the integral Unix second, excluding leap seconds.
    #[must_use]
    pub const fn unix_seconds(self) -> i64 {
        self.seconds
    }

    /// Returns nanoseconds within the Unix second, including its leap-second extension.
    #[must_use]
    pub const fn subsec_nanos(self) -> u32 {
        self.nanoseconds
    }

    pub(crate) const fn from_sdk_parts(seconds: i64, nanoseconds: u32) -> Self {
        Self {
            seconds,
            nanoseconds,
        }
    }

    pub(crate) const fn from_task_status(status: &wire::TaskStatus) -> Option<Self> {
        match status.timestamp {
            Some(value) => Some(Self::from_sdk_parts(
                value.timestamp(),
                value.timestamp_subsec_nanos(),
            )),
            None => None,
        }
    }

    pub(crate) fn to_wire<T: FromStr>(self) -> T {
        self.to_string().parse().unwrap_or_else(|_| {
            unreachable!("canonical A2A timestamp must parse in its wire field")
        })
    }
}

impl FromStr for A2aTimestamp {
    type Err = A2aContractError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        // The SDK field selects the same parser that reads task and list wires.
        let status = wire::TaskStatus {
            state: wire::TaskState::Submitted,
            message: None,
            timestamp: Some(
                input
                    .parse()
                    .map_err(|_| A2aContractError::InvalidText { field: "timestamp" })?,
            ),
        };
        Ok(Self::from_task_status(&status).expect("the parsed timestamp is present"))
    }
}

impl fmt::Display for A2aTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let utc = time::OffsetDateTime::from_unix_timestamp(self.seconds)
            .expect("validated UTC timestamp");
        let year = utc.year();
        if (0..=9999).contains(&year) {
            write!(f, "{year:04}")?;
        } else {
            write!(f, "{year:+05}")?;
        }
        let second = u32::from(utc.second()) + self.nanoseconds / 1_000_000_000;
        write!(
            f,
            "-{:02}-{:02}T{:02}:{:02}:{second:02}",
            u8::from(utc.month()),
            utc.day(),
            utc.hour(),
            utc.minute()
        )?;
        let fraction = self.nanoseconds % 1_000_000_000;
        if fraction != 0 {
            if fraction % 1_000_000 == 0 {
                write!(f, ".{:03}", fraction / 1_000_000)?;
            } else if fraction % 1_000 == 0 {
                write!(f, ".{:06}", fraction / 1_000)?;
            } else {
                write!(f, ".{fraction:09}")?;
            }
        }
        f.write_str("Z")
    }
}

impl Serialize for A2aTimestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for A2aTimestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TimestampVisitor;
        impl de::Visitor<'_> for TimestampVisitor {
            type Value = A2aTimestamp;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an A2A timestamp string")
            }

            fn visit_str<E: de::Error>(self, input: &str) -> Result<Self::Value, E> {
                input.parse().map_err(E::custom)
            }
        }
        deserializer.deserialize_str(TimestampVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{A2aListTasksRequest, A2aTaskState, A2aTaskStatus};
    use serde_json::json;

    #[test]
    fn status_and_list_timestamps_match_official_sdk_wires() {
        for (input, canonical) in [
            ("1970-01-01T00:00:00Z", "1970-01-01T00:00:00Z"),
            ("1969-12-31T23:59:59.500Z", "1969-12-31T23:59:59.500Z"),
            (
                "2026-10-08T00:15:00.123456+02:00",
                "2026-10-07T22:15:00.123456Z",
            ),
            (
                "2026-10-08T00:00:00.12345678901Z",
                "2026-10-08T00:00:00.123456789Z",
            ),
            (
                "2017-01-01T00:59:60.123456789+01:00",
                "2016-12-31T23:59:60.123456789Z",
            ),
            ("0000-01-01T00:00:00Z", "0000-01-01T00:00:00Z"),
            ("-0001-12-31T23:59:59Z", "-0001-12-31T23:59:59Z"),
            ("-262143-01-01T00:00:00Z", "-262143-01-01T00:00:00Z"),
            (
                "+262142-12-31T23:59:60.999999999Z",
                "+262142-12-31T23:59:60.999999999Z",
            ),
        ] {
            let official = serde_json::from_value::<wire::TaskStatus>(json!({
                "state": "TASK_STATE_WORKING", "timestamp": input
            }))
            .unwrap();
            let expected = official.timestamp.unwrap();
            let stamp: A2aTimestamp = input.parse().unwrap();
            assert_eq!(stamp.unix_seconds(), expected.timestamp());
            assert_eq!(stamp.subsec_nanos(), expected.timestamp_subsec_nanos());
            assert_eq!(
                A2aTimestamp::from_unix_parts(
                    expected.timestamp(),
                    expected.timestamp_subsec_nanos()
                ),
                Some(stamp)
            );
            assert_eq!(stamp.to_string(), canonical);
            assert_eq!(
                serde_json::to_value(stamp).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
            assert_eq!(
                serde_json::from_value::<A2aTimestamp>(json!(input)).unwrap(),
                stamp
            );
            let status = A2aTaskStatus::new(A2aTaskState::Working, None, Some(stamp)).unwrap();
            assert_eq!(status.timestamp(), Some(stamp));
            let wire = A2aListTasksRequest::new()
                .with_status_timestamp_after(stamp)
                .into_wire(None);
            assert_eq!(wire.status_timestamp_after, Some(expected));
            assert_eq!(
                A2aListTasksRequest::try_from_wire(wire)
                    .unwrap()
                    .status_timestamp_after(),
                Some(stamp)
            );
        }
    }

    #[test]
    fn rejected_and_ordered_timestamps_match_the_protocol() {
        for input in [
            "",
            "2026-02-30T00:00:00Z",
            "2026-01-01T12:00:00",
            "2026-01-01T00:00:61Z",
            "+262143-01-01T00:00:00Z",
            "-262144-12-31T23:59:59Z",
        ] {
            assert!(
                serde_json::from_value::<wire::TaskStatus>(json!({
                    "state": "TASK_STATE_WORKING", "timestamp": input
                }))
                .is_err(),
                "{input}"
            );
            assert!(input.parse::<A2aTimestamp>().is_err(), "{input}");
        }
        let last = "2016-12-31T23:59:59.999999999Z"
            .parse::<A2aTimestamp>()
            .unwrap();
        let leap = "2016-12-31T23:59:60Z".parse::<A2aTimestamp>().unwrap();
        let next = "2017-01-01T00:00:00Z".parse::<A2aTimestamp>().unwrap();
        assert!(last < leap && leap < next);
        assert_eq!(leap.unix_seconds(), last.unix_seconds());
        assert!(A2aTimestamp::from_unix_parts(last.unix_seconds(), 2_000_000_000).is_none());
        assert!(A2aTimestamp::from_unix_parts(next.unix_seconds(), 1_000_000_000).is_none());
        assert!(A2aTimestamp::from_unix_parts(i64::MIN, 0).is_none());
        assert!(A2aTimestamp::from_unix_parts(i64::MAX, 0).is_none());
    }
}
