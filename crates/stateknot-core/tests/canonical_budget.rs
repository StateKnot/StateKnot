// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Cross-version compatibility fixtures for finite budget wire forms.

use serde::Deserialize;
use serde_json::Value;
use stateknot_core::{
    BudgetDimension, BudgetLimits, BudgetUsage, CostLimits, KnownCosts, ResolvedBudget,
};

const FIXTURE_SCHEMA: &str = "https://stateknot.github.io/schema/test-fixture/core-budget/1.0.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    schema: String,
    dimensions: TextFixtures,
    cost_limits: ObjectFixtures,
    known_costs: ObjectFixtures,
    limits: ObjectFixtures,
    resolved: ObjectFixtures,
    usage: ObjectFixtures,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TextFixtures {
    valid: Vec<String>,
    invalid: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectFixtures {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/core-budget-v1.json"))
        .expect("canonical budget fixture must be valid JSON")
}

#[test]
fn canonical_budget_dimension_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);

    for expected in fixture.dimensions.valid {
        let decoded =
            serde_json::from_value::<BudgetDimension>(Value::from(expected.clone())).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            Value::from(expected)
        );
    }

    for invalid in fixture.dimensions.invalid {
        assert!(
            serde_json::from_value::<BudgetDimension>(invalid.clone()).is_err(),
            "BudgetDimension accepted {invalid}"
        );
    }
}

#[test]
fn canonical_cost_limit_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_object_fixture::<CostLimits>(fixture.cost_limits, "CostLimits");
}

#[test]
fn canonical_known_cost_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_object_fixture::<KnownCosts>(fixture.known_costs, "KnownCosts");
}

#[test]
fn canonical_partial_budget_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_object_fixture::<BudgetLimits>(fixture.limits, "BudgetLimits");
}

#[test]
fn canonical_resolved_budget_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_object_fixture::<ResolvedBudget>(fixture.resolved, "ResolvedBudget");
}

#[test]
fn canonical_budget_usage_fixture_matches_runtime_contract() {
    let fixture = load_fixture();
    assert_eq!(fixture.schema, FIXTURE_SCHEMA);
    assert_object_fixture::<BudgetUsage>(fixture.usage, "BudgetUsage");
}

fn assert_object_fixture<T>(fixture: ObjectFixtures, type_name: &str)
where
    T: for<'de> Deserialize<'de> + serde::Serialize,
{
    for expected in fixture.valid {
        let decoded = serde_json::from_value::<T>(expected.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), expected);
    }

    for invalid in fixture.invalid {
        assert!(
            serde_json::from_value::<T>(invalid.clone()).is_err(),
            "{type_name} accepted {invalid}"
        );
    }
}
