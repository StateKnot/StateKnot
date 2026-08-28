// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for canonical time wire forms.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{DurationMillis, Timestamp};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-time/2.0.0";
const LEGACY_FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-time/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    timestamps: TimestampFixtures,
    durations_millis: DurationFixtures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimestampFixtures {
    valid: Vec<ValidTimestamp>,
    invalid: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidTimestamp {
    text: String,
    unix_micros: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurationFixtures {
    valid: Vec<ValidDuration>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidDuration {
    text: String,
    milliseconds: i64,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-time-v2.json"))
        .expect("canonical time fixture must be valid JSON")
}

#[test]
fn canonical_timestamp_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.timestamps.valid {
        let parsed = expected.text.parse::<Timestamp>().unwrap();
        assert_eq!(parsed.unix_micros(), expected.unix_micros);
        assert_eq!(parsed.to_string(), expected.text);
        assert_eq!(
            serde_json::to_string(&parsed).unwrap(),
            format!("\"{}\"", expected.text)
        );
    }

    for invalid in fixture.timestamps.invalid {
        assert!(
            invalid.parse::<Timestamp>().is_err(),
            "accepted {invalid:?}"
        );
    }
}

#[test]
fn canonical_duration_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.durations_millis.valid {
        let duration = expected.text.parse::<DurationMillis>().unwrap();
        assert_eq!(duration.as_i64(), expected.milliseconds);
        let encoded = serde_json::to_value(duration).unwrap();
        assert_eq!(encoded, Value::from(expected.text));
        assert_eq!(
            serde_json::from_value::<DurationMillis>(encoded).unwrap(),
            duration
        );
    }

    for invalid in fixture.durations_millis.invalid {
        assert!(
            serde_json::from_value::<DurationMillis>(invalid.clone()).is_err(),
            "accepted {invalid:?}"
        );
    }
}

#[test]
fn legacy_numeric_duration_wire_is_not_silently_accepted() {
    let fixture: Value = serde_json::from_str(include_str!("fixtures/core-time-v1.json"))
        .expect("legacy time fixture must remain valid JSON");
    assert_eq!(fixture["schema"], LEGACY_FIXTURE_SCHEMA);

    for legacy in fixture["durations_millis"]["valid"]
        .as_array()
        .expect("legacy valid durations must be an array")
    {
        assert!(
            serde_json::from_value::<DurationMillis>(legacy.clone()).is_err(),
            "accepted legacy numeric duration {legacy}"
        );
    }
}
