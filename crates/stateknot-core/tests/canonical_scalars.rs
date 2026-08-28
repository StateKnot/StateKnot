// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for canonical scalar wire forms.

use serde::Deserialize;
use stateknot_core::{Digest, Version};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-scalars/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    versions: VersionFixtures,
    digests: DigestFixtures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct VersionFixtures {
    valid: Vec<ValidVersion>,
    invalid: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidVersion {
    text: String,
    major: u64,
    minor: u64,
    patch: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DigestFixtures {
    valid: Vec<ValidDigest>,
    invalid: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidDigest {
    input_utf8: String,
    text: String,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-scalars-v1.json"))
        .expect("canonical scalar fixture must be valid JSON")
}

#[test]
fn canonical_version_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.versions.valid {
        let version = expected.text.parse::<Version>().unwrap();
        assert_eq!(version.major(), expected.major);
        assert_eq!(version.minor(), expected.minor);
        assert_eq!(version.patch(), expected.patch);
        assert_eq!(version.to_string(), expected.text);
        assert_eq!(
            serde_json::to_string(&version).unwrap(),
            format!("\"{}\"", expected.text)
        );
    }

    for invalid in fixture.versions.invalid {
        assert!(invalid.parse::<Version>().is_err(), "accepted {invalid:?}");
    }
}

#[test]
fn canonical_digest_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.digests.valid {
        let computed = Digest::sha256(expected.input_utf8.as_bytes());
        let parsed = expected.text.parse::<Digest>().unwrap();
        assert_eq!(computed, parsed);
        assert_eq!(parsed.to_string(), expected.text);
        assert_eq!(
            serde_json::to_string(&parsed).unwrap(),
            format!("\"{}\"", expected.text)
        );
    }

    for invalid in fixture.digests.invalid {
        assert!(invalid.parse::<Digest>().is_err(), "accepted {invalid:?}");
    }
}
