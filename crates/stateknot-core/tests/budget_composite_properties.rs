// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Independent finite-budget models: wide arithmetic, currency maps and topology.

use std::collections::BTreeMap;

use proptest::{collection, prelude::*};
use serde_json::{Value, json};
use stateknot_core::{
    BudgetLimits, BudgetRemaining, BudgetUsage, CumulativeBudgetReservation, ResolvedBudget,
    Timestamp,
};

const FIELDS: [&str; 20] = [
    "graph_depth",
    "graph_steps",
    "model_attempts",
    "model_turns",
    "input_tokens",
    "cached_input_tokens",
    "reasoning_tokens",
    "output_tokens",
    "tool_calls",
    "write_calls",
    "remote_agent_delegations",
    "retries",
    "concurrent_branches",
    "fan_out",
    "input_bytes",
    "output_bytes",
    "event_bytes",
    "checkpoint_bytes",
    "artifact_bytes",
    "unpriced_cost_events",
];
const PEAKS: [usize; 3] = [0, 12, 13];
const SUBSETS: [(usize, usize); 3] = [(5, 4), (6, 7), (9, 8)];
const CURRENCIES: [&str; 16] = [
    "AUD", "CAD", "CHF", "CNY", "EUR", "GBP", "HKD", "INR", "JPY", "KRW", "MXN", "NZD", "RUB",
    "SGD", "USD", "ZAR",
];

#[derive(Clone, Debug)]
struct Model {
    scalars: [u64; 20],
    costs: BTreeMap<&'static str, u64>,
}

fn normalize(scalars: &mut [u64; 20]) {
    for (subset, inclusive) in SUBSETS {
        scalars[subset] = scalars[subset].min(scalars[inclusive]);
    }
}

fn model(maximum: u64) -> impl Strategy<Value = Model> {
    (
        collection::vec(0..=maximum, 20),
        collection::vec((any::<bool>(), 0..=maximum), 16),
    )
        .prop_map(|(scalars, costs)| {
            let mut scalars: [u64; 20] = scalars.try_into().unwrap();
            normalize(&mut scalars);
            Model {
                scalars,
                costs: CURRENCIES
                    .into_iter()
                    .zip(costs)
                    .filter_map(|(currency, (present, units))| present.then_some((currency, units)))
                    .collect(),
            }
        })
}

fn mixed_model() -> impl Strategy<Value = Model> {
    prop_oneof![3 => model(1024), 1 => model(u64::MAX)]
}

fn cost_wire(costs: &BTreeMap<&str, u64>) -> Value {
    costs
        .iter()
        .map(|(currency, units)| {
            json!({
                "currency": currency, "micro_units": units.to_string(),
            })
        })
        .collect()
}

impl Model {
    fn wire(&self) -> Value {
        let mut fields: serde_json::Map<String, Value> = FIELDS
            .into_iter()
            .zip(self.scalars)
            .map(|(name, units)| (name.into(), json!(units.to_string())))
            .collect();
        fields.insert("known_costs".into(), cost_wire(&self.costs));
        Value::Object(fields)
    }

    fn usage(&self) -> BudgetUsage {
        serde_json::from_value(self.wire()).unwrap()
    }

    fn budget_wire(&self, deadline: Timestamp) -> Value {
        let mut wire = self.wire();
        let fields = wire.as_object_mut().unwrap();
        fields.remove("unpriced_cost_events");
        let costs = fields.remove("known_costs").unwrap();
        fields.insert("costs".into(), costs);
        fields.insert("deadline".into(), json!(deadline));
        wire
    }

    fn budget(&self, deadline: Timestamp) -> ResolvedBudget {
        serde_json::from_value(self.budget_wire(deadline)).unwrap()
    }

    fn bounded(&self) -> Self {
        let mut value = self.clone();
        for amount in &mut value.scalars {
            *amount %= 1025;
        }
        normalize(&mut value.scalars);
        for amount in value.costs.values_mut() {
            *amount %= 1025;
        }
        value
    }

    fn cumulative(&self) -> Self {
        let mut value = self.clone();
        for index in PEAKS {
            value.scalars[index] = 0;
        }
        value
    }
}

// u128 arithmetic deliberately does not call Core's checked count/cost operations.
fn aggregate(values: &[Model]) -> Option<Model> {
    let mut scalars = [0_u128; 20];
    let mut costs = BTreeMap::<&str, u128>::new();
    for value in values {
        for (index, amount) in value.scalars.into_iter().enumerate() {
            scalars[index] = if PEAKS.contains(&index) {
                scalars[index].max(u128::from(amount))
            } else {
                scalars[index] + u128::from(amount)
            };
        }
        for (currency, amount) in &value.costs {
            *costs.entry(currency).or_default() += u128::from(*amount);
        }
    }
    let scalars: Vec<u64> = scalars
        .into_iter()
        .map(u64::try_from)
        .collect::<Result<_, _>>()
        .ok()?;
    let costs = costs
        .into_iter()
        .map(|(currency, amount)| u64::try_from(amount).map(|amount| (currency, amount)))
        .collect::<Result<_, _>>()
        .ok()?;
    Some(Model {
        scalars: scalars.try_into().unwrap(),
        costs,
    })
}

fn subtract(total: &Model, contribution: &Model) -> Option<Model> {
    let mut result = total.clone();
    for (index, amount) in contribution.scalars.into_iter().enumerate() {
        if !PEAKS.contains(&index) {
            result.scalars[index] = total.scalars[index].checked_sub(amount)?;
        }
    }
    for (currency, amount) in &contribution.costs {
        let previous = total.costs.get(currency).copied().unwrap_or(0);
        let residual = previous.checked_sub(*amount)?;
        if total.costs.contains_key(currency) {
            result.costs.insert(currency, residual);
        }
    }
    SUBSETS
        .iter()
        .all(|(subset, inclusive)| result.scalars[*subset] <= result.scalars[*inclusive])
        .then_some(result)
}

fn remaining(budget: &Model, usage: &Model, deadline: Timestamp, now: Timestamp) -> Option<Value> {
    if now >= deadline || usage.scalars[19] != 0 {
        return None;
    }
    let mut remaining = budget.clone();
    for index in 0..19 {
        remaining.scalars[index] = budget.scalars[index].checked_sub(usage.scalars[index])?;
    }
    for (currency, amount) in &usage.costs {
        let limit = *budget.costs.get(currency)?;
        remaining
            .costs
            .insert(currency, limit.checked_sub(*amount)?);
    }
    Some(remaining.budget_wire(deadline))
}

fn deadline() -> Timestamp {
    Timestamp::from_unix_micros(2_000_000).unwrap()
}

fn before() -> Timestamp {
    Timestamp::from_unix_micros(1_999_999).unwrap()
}

fn check_wire<T: serde::Serialize>(
    actual: Result<T, impl std::fmt::Debug>,
    expected: Option<Value>,
) {
    assert_eq!(actual.is_ok(), expected.is_some());
    if let (Ok(actual), Some(expected)) = (actual, expected) {
        let wire = serde_json::to_value(actual).unwrap();
        assert_eq!(wire, expected);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn every_usage_dimension_matches_wide_sum_and_high_water(left in mixed_model(), right in mixed_model()) {
        let expected = aggregate(&[left.clone(), right.clone()]);
        let actual = left.usage().checked_accumulate(&right.usage());
        check_wire(actual, expected.as_ref().map(Model::wire));
        check_wire(right.usage().checked_accumulate(&left.usage()), expected.as_ref().map(Model::wire));
        prop_assert_eq!(serde_json::to_value(left.usage().cumulative_only()).unwrap(), left.cumulative().wire());
    }

    #[test]
    fn cumulative_subtraction_matches_independent_residual_and_subset_rules(total in mixed_model(), contribution in mixed_model()) {
        check_wire(total.usage().checked_subtract_cumulative(&contribution.usage()), subtract(&total, &contribution).as_ref().map(Model::wire));
        let bounded_total = total.bounded();
        let bounded_contribution = contribution.bounded();
        let combined = aggregate(&[bounded_total, bounded_contribution.clone()]).unwrap();
        check_wire(combined.usage().checked_subtract_cumulative(&bounded_contribution.usage()), subtract(&combined, &bounded_contribution).as_ref().map(Model::wire));
    }

    #[test]
    fn finite_remaining_matches_every_ceiling_currency_and_deadline(budget in model(1024), usage in model(1024)) {
        let actual_budget = budget.budget(deadline());
        check_wire(actual_budget.remaining(&usage.usage(), before()), remaining(&budget, &usage, deadline(), before()));
        for now in [deadline(), Timestamp::from_unix_micros(2_000_001).unwrap()] {
            prop_assert!(actual_budget.remaining(&usage.usage(), now).is_err());
        }
        let mut in_budget = usage.clone();
        for index in 0..19 { in_budget.scalars[index] = in_budget.scalars[index].min(budget.scalars[index]); }
        in_budget.scalars[19] = 0;
        normalize(&mut in_budget.scalars);
        in_budget.costs.retain(|currency, _| budget.costs.contains_key(currency));
        for (currency, amount) in &mut in_budget.costs { *amount = (*amount).min(budget.costs[currency]); }
        check_wire(actual_budget.remaining(&in_budget.usage(), before()), remaining(&budget, &in_budget, deadline(), before()));
    }

    #[test]
    fn layered_resolution_intersects_all_partial_limits_without_currency_widening(
        baseline in mixed_model(), layers in collection::vec((mixed_model(), any::<[bool; 21]>(), 1_i64..4_000_000), 0..16),
    ) {
        let mut expected = baseline.clone();
        let mut at = deadline();
        let mut actual: Vec<BudgetLimits> = vec![serde_json::from_value(baseline.budget_wire(at)).unwrap()];
        for (layer, included, micros) in layers {
            let time = Timestamp::from_unix_micros(micros).unwrap();
            let mut wire = layer.budget_wire(time);
            for (index, name) in FIELDS[..19].iter().enumerate() {
                if included[index] { expected.scalars[index] = expected.scalars[index].min(layer.scalars[index]); }
                else { wire.as_object_mut().unwrap().remove(*name); }
            }
            if included[19] { at = at.min(time); }
            else { wire.as_object_mut().unwrap().remove("deadline"); }
            if included[20] {
                expected.costs.retain(|currency, _| layer.costs.contains_key(currency));
                for (currency, amount) in &mut expected.costs { *amount = (*amount).min(layer.costs[currency]); }
            } else { wire.as_object_mut().unwrap().remove("costs"); }
            actual.push(serde_json::from_value(wire).unwrap());
        }
        normalize(&mut expected.scalars);
        let resolved = ResolvedBudget::resolve(&actual).unwrap();
        prop_assert_eq!(serde_json::to_value(&resolved).unwrap(), expected.budget_wire(at));
        actual.reverse();
        prop_assert_eq!(ResolvedBudget::resolve(&actual).unwrap(), resolved);
    }

    #[test]
    fn reservation_capacity_matches_direct_plus_all_cumulative_children(
        parent in mixed_model(), direct in mixed_model(), children in collection::vec(mixed_model(), 0..17),
    ) {
        let mut projections = Vec::new();
        let mut reservations = Vec::new();
        for child in children {
            let mut projection = child.cumulative();
            projection.scalars[19] = 0;
            reservations.push(CumulativeBudgetReservation::new(projection.usage()).unwrap());
            projections.push(projection);
        }
        let mut total = vec![direct.clone()];
        total.extend(projections);
        let expected = aggregate(&total).and_then(|sum| remaining(&parent, &sum, deadline(), before()));
        check_wire(CumulativeBudgetReservation::check_capacity(&parent.budget(deadline()), &direct.usage(), &reservations, before()), expected.clone());
        reservations.reverse();
        check_wire(CumulativeBudgetReservation::check_capacity(&parent.budget(deadline()), &direct.usage(), &reservations, before()), expected);
        // Every generated case also exercises successful multi-child capacity;
        // random unknown cost and overflow cannot dominate the positive path.
        let mut bounded: Vec<Model> = total.iter().map(Model::bounded).collect();
        for value in &mut bounded { value.scalars[19] = 0; }
        let fitted = aggregate(&bounded).unwrap();
        let reservations: Vec<_> = bounded[1..].iter().map(|child| CumulativeBudgetReservation::new(child.usage()).unwrap()).collect();
        check_wire(CumulativeBudgetReservation::check_capacity(&fitted.budget(deadline()), &bounded[0].usage(), &reservations, before()), remaining(&fitted, &fitted, deadline(), before()));
    }

    #[test]
    fn narrowing_matches_all_independent_limits_and_currency_membership(parent in mixed_model(), child in mixed_model()) {
        let accepted = (0..19).all(|index| child.scalars[index] <= parent.scalars[index])
            && child.costs.iter().all(|(currency, units)| parent.costs.get(currency).is_some_and(|limit| units <= limit));
        prop_assert_eq!(child.budget(deadline()).validate_narrowing(&parent.budget(deadline())).is_ok(), accepted);
        let mut narrowed = child.clone();
        for index in 0..19 { narrowed.scalars[index] = narrowed.scalars[index].min(parent.scalars[index]); }
        normalize(&mut narrowed.scalars);
        narrowed.costs.retain(|currency, _| parent.costs.contains_key(currency));
        for (currency, units) in &mut narrowed.costs { *units = (*units).min(parent.costs[currency]); }
        prop_assert!(narrowed.budget(before()).validate_narrowing(&parent.budget(deadline())).is_ok());
        prop_assert!(narrowed.budget(deadline()).validate_narrowing(&parent.budget(before())).is_err());
        if narrowed.costs.len() == CURRENCIES.len() { narrowed.costs.remove("AUD"); }
        narrowed.costs.insert("AED", 0);
        // An unlisted zero ceiling is still an unauthorized currency.
        prop_assert!(narrowed.budget(deadline()).validate_narrowing(&parent.budget(deadline())).is_err());
    }

    #[test]
    fn remaining_deduction_preserves_topology_and_charges_each_cumulative_dimension(
        parent in model(1024), charge in model(1024),
    ) {
        let zero = BudgetUsage::zero();
        let capacity: BudgetRemaining = parent.budget(deadline()).remaining(&zero, before()).unwrap();
        check_wire(capacity.deduct_cumulative(&charge.usage()), remaining(&parent, &charge.cumulative(), deadline(), before()));
        let mut exact_charge = charge.cumulative();
        exact_charge.scalars[19] = 0;
        let mut fitted = exact_charge.clone();
        for index in PEAKS { fitted.scalars[index] = parent.scalars[index]; }
        let capacity = fitted.budget(deadline()).remaining(&zero, before()).unwrap();
        check_wire(capacity.deduct_cumulative(&exact_charge.usage()), remaining(&fitted, &exact_charge, deadline(), before()));
    }
}

#[test]
fn every_cumulative_overflow_and_peak_maximum_is_exercised() {
    let zero = Model {
        scalars: [0; 20],
        costs: BTreeMap::new(),
    };
    for (index, field) in FIELDS.iter().enumerate() {
        let mut left = zero.clone();
        let mut right = zero.clone();
        left.scalars[index] = u64::MAX;
        right.scalars[index] = if PEAKS.contains(&index) { u64::MAX } else { 1 };
        for (subset, inclusive) in SUBSETS {
            if index == subset {
                left.scalars[inclusive] = u64::MAX;
                right.scalars[inclusive] = 1;
            }
        }
        let expected = aggregate(&[left.clone(), right.clone()]);
        assert_eq!(expected.is_some(), PEAKS.contains(&index), "{field}");
        check_wire(
            left.usage().checked_accumulate(&right.usage()),
            expected.as_ref().map(Model::wire),
        );
    }
    for currency in CURRENCIES {
        let mut left = zero.clone();
        let mut right = zero.clone();
        left.costs.insert(currency, u64::MAX);
        right.costs.insert(currency, 1);
        assert!(aggregate(&[left.clone(), right.clone()]).is_none());
        assert!(left.usage().checked_accumulate(&right.usage()).is_err());
    }
    let mut at_limit = zero.clone();
    at_limit
        .costs
        .extend(CURRENCIES.into_iter().map(|currency| (currency, 0)));
    assert_eq!(at_limit.usage().known_costs().len(), 16);
    let mut extra = zero;
    extra.costs.insert("AED", 0);
    assert!(at_limit.usage().checked_accumulate(&extra.usage()).is_err());
}
