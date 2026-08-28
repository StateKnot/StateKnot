// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for accounting wire forms.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{ByteCount, CurrencyCode, Money, TokenCount};

const FIXTURE_SCHEMA: &str =
    "https://stateknot.github.io/schema/test-fixture/core-accounting/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    counts: TextFixtures,
    currencies: TextFixtures,
    money: MoneyFixtures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TextFixtures {
    valid: Vec<String>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MoneyFixtures {
    valid: Vec<ValidMoney>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidMoney {
    currency: String,
    micro_units: String,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-accounting-v1.json"))
        .expect("canonical accounting fixture must be valid JSON")
}

#[test]
fn canonical_count_fixture_matches_both_count_contracts() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.counts.valid {
        let tokens = expected.parse::<TokenCount>().unwrap();
        let bytes = expected.parse::<ByteCount>().unwrap();
        assert_eq!(tokens.to_string(), expected);
        assert_eq!(bytes.to_string(), expected);
        assert_eq!(
            serde_json::to_value(tokens).unwrap(),
            Value::from(expected.clone())
        );
        assert_eq!(serde_json::to_value(bytes).unwrap(), Value::from(expected));
    }

    for invalid in fixture.counts.invalid {
        assert!(
            serde_json::from_value::<TokenCount>(invalid.clone()).is_err(),
            "TokenCount accepted {invalid}"
        );
        assert!(
            serde_json::from_value::<ByteCount>(invalid.clone()).is_err(),
            "ByteCount accepted {invalid}"
        );
    }
}

#[test]
fn canonical_currency_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.currencies.valid {
        let code = expected.parse::<CurrencyCode>().unwrap();
        assert_eq!(code.to_string(), expected);
        assert_eq!(serde_json::to_value(code).unwrap(), Value::from(expected));
    }

    for invalid in fixture.currencies.invalid {
        assert!(
            serde_json::from_value::<CurrencyCode>(invalid.clone()).is_err(),
            "CurrencyCode accepted {invalid}"
        );
    }
}

#[test]
fn canonical_money_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.money.valid {
        let encoded = serde_json::json!({
            "currency": expected.currency,
            "micro_units": expected.micro_units,
        });
        let money = serde_json::from_value::<Money>(encoded.clone()).unwrap();
        assert_eq!(serde_json::to_value(money).unwrap(), encoded);
    }

    for invalid in fixture.money.invalid {
        assert!(
            serde_json::from_value::<Money>(invalid.clone()).is_err(),
            "Money accepted {invalid}"
        );
    }
}
