// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{BudgetLimits, ByteCount, CostLimits, Money, TokenCount};
use proptest::prelude::*;
use serde_json::{Value, from_value, json, to_value};

fn now() -> Timestamp {
    "2029-01-01T00:00:00.000000Z".parse().unwrap()
}

fn budget(value: u64) -> ResolvedBudget {
    let count = ExecutionCount::new(value);
    let tokens = TokenCount::new(value);
    let bytes = ByteCount::new(value);
    ResolvedBudget::resolve(&[BudgetLimits::empty()
        .with_deadline("2030-01-01T00:00:00.000000Z".parse().unwrap())
        .with_graph_depth(count)
        .with_graph_steps(count)
        .with_model_attempts(count)
        .with_model_turns(count)
        .with_input_tokens(tokens)
        .with_cached_input_tokens(tokens)
        .with_output_tokens(tokens)
        .with_reasoning_tokens(tokens)
        .with_tool_calls(count)
        .with_write_calls(count)
        .with_remote_agent_delegations(count)
        .with_retries(count)
        .with_concurrent_branches(count)
        .with_fan_out(count)
        .with_input_bytes(bytes)
        .with_output_bytes(bytes)
        .with_event_bytes(bytes)
        .with_checkpoint_bytes(bytes)
        .with_artifact_bytes(bytes)
        .with_costs(CostLimits::try_new([Money::new("USD".parse().unwrap(), value)]).unwrap())])
    .unwrap()
}

fn reservation(value: u64) -> CumulativeBudgetReservation {
    CumulativeBudgetReservation::from_budget(&budget(value)).unwrap()
}

fn check(
    accounted: &BudgetUsage,
    outstanding: &[CumulativeBudgetReservation],
) -> Result<BudgetRemaining, CumulativeBudgetReservationError> {
    CumulativeBudgetReservation::check_capacity(&budget(100), accounted, outstanding, now())
}

#[test]
fn projection_covers_every_cumulative_field_without_charging_peaks() {
    let limits = to_value(budget(37)).unwrap();
    let reservation = reservation(37);
    let amount = to_value(reservation.amount()).unwrap();
    for (field, value) in limits.as_object().unwrap() {
        match field.as_str() {
            "deadline" => assert!(amount.get(field).is_none()),
            "costs" => assert_eq!(amount["known_costs"], *value),
            "graph_depth" | "concurrent_branches" | "fan_out" => assert_eq!(amount[field], "0"),
            _ => assert_eq!(amount[field], *value, "{field}"),
        }
    }
    assert_eq!(amount["unpriced_cost_events"], "0");
    assert_eq!(
        from_value::<CumulativeBudgetReservation>(to_value(&reservation).unwrap()).unwrap(),
        reservation
    );
}

#[test]
fn cumulative_capacity_includes_accounted_and_all_reservations() {
    let accounted = reservation(20).amount().clone();
    let remaining = check(&accounted, &[reservation(30), reservation(40)]).unwrap();
    assert_eq!(remaining.input_tokens(), TokenCount::new(10));
    assert_eq!(remaining.graph_steps(), ExecutionCount::new(10));
    assert_eq!(
        remaining
            .costs()
            .get("USD".parse().unwrap())
            .unwrap()
            .micro_units(),
        10
    );
    assert!(check(&accounted, &[reservation(50), reservation(31)]).is_err());
    // Equal amounts are separate reservations, not duplicate identifiers.
    assert!(check(&accounted, &[reservation(41), reservation(41)]).is_err());
    assert_eq!(
        check(&accounted, &[reservation(40), reservation(40)])
            .unwrap()
            .input_tokens(),
        TokenCount::ZERO
    );
}

#[test]
fn every_cumulative_scalar_is_checked_at_the_boundary() {
    let base = to_value(reservation(0).amount()).unwrap();
    for field in [
        "graph_steps",
        "model_attempts",
        "model_turns",
        "input_tokens",
        "output_tokens",
        "tool_calls",
        "remote_agent_delegations",
        "retries",
        "input_bytes",
        "output_bytes",
        "event_bytes",
        "checkpoint_bytes",
        "artifact_bytes",
    ] {
        let mut wire = base.clone();
        wire[field] = json!("101");
        let candidate = CumulativeBudgetReservation::new(from_value(wire).unwrap()).unwrap();
        assert!(
            check(&BudgetUsage::zero(), &[candidate]).is_err(),
            "{field}"
        );
    }
    for (subset, inclusive) in [
        ("cached_input_tokens", "input_tokens"),
        ("reasoning_tokens", "output_tokens"),
        ("write_calls", "tool_calls"),
    ] {
        let mut parent = to_value(budget(100)).unwrap();
        parent[subset] = json!("10");
        let parent = from_value(parent).unwrap();
        let mut amount = base.clone();
        amount[subset] = json!("11");
        amount[inclusive] = json!("11");
        let candidate = CumulativeBudgetReservation::new(from_value(amount).unwrap()).unwrap();
        assert!(
            CumulativeBudgetReservation::check_capacity(
                &parent,
                &BudgetUsage::zero(),
                &[candidate],
                now()
            )
            .is_err(),
            "{subset}"
        );
    }
}

#[test]
fn noncumulative_and_unpriced_reservations_fail_on_constructor_and_wire() {
    for field in [
        "graph_depth",
        "concurrent_branches",
        "fan_out",
        "unpriced_cost_events",
    ] {
        let mut wire = to_value(reservation(0)).unwrap();
        wire["amount"][field] = json!("1");
        let amount = from_value(wire["amount"].clone()).unwrap();
        assert!(CumulativeBudgetReservation::new(amount).is_err(), "{field}");
        assert!(from_value::<CumulativeBudgetReservation>(wire).is_err());
    }
    let mut wire = to_value(reservation(1)).unwrap();
    wire["committed"] = json!(true);
    assert!(from_value::<CumulativeBudgetReservation>(wire).is_err());
    let mut wire = to_value(reservation(1)).unwrap();
    wire["amount"]["cached_input_tokens"] = json!("2");
    assert!(from_value::<CumulativeBudgetReservation>(wire).is_err());
}

#[test]
fn existing_peaks_are_checked_but_not_added_for_each_child() {
    let usage = BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(70))
        .concurrent_branches(ExecutionCount::new(80))
        .fan_out(ExecutionCount::new(90))
        .build()
        .unwrap();
    let remaining = check(&usage, &[reservation(40), reservation(40)]).unwrap();
    assert_eq!(remaining.graph_depth(), ExecutionCount::new(30));
    assert_eq!(remaining.concurrent_branches(), ExecutionCount::new(20));
    assert_eq!(remaining.fan_out(), ExecutionCount::new(10));
    let excess = BudgetUsage::builder()
        .graph_depth(ExecutionCount::new(101))
        .build()
        .unwrap();
    assert!(check(&excess, &[]).is_err());
}

#[test]
fn unknown_usage_and_unbudgeted_currency_are_never_zero_cost() {
    let unknown = BudgetUsage::builder()
        .unpriced_cost_events(ExecutionCount::new(1))
        .build()
        .unwrap();
    assert!(check(&unknown, &[]).is_err());
    for value in [0, 1] {
        let costs = KnownCosts::try_new([Money::new("EUR".parse().unwrap(), value)]).unwrap();
        let candidate = CumulativeBudgetReservation::new(
            BudgetUsage::builder().known_costs(costs).build().unwrap(),
        )
        .unwrap();
        assert!(check(&BudgetUsage::zero(), &[candidate]).is_err());
    }
    let costs = KnownCosts::try_new([Money::new("USD".parse().unwrap(), 101)]).unwrap();
    let candidate = CumulativeBudgetReservation::new(
        BudgetUsage::builder().known_costs(costs).build().unwrap(),
    )
    .unwrap();
    assert!(check(&BudgetUsage::zero(), &[candidate]).is_err());
}

#[test]
fn expired_and_overflowing_capacity_fail_closed() {
    let parent = budget(100);
    assert!(
        CumulativeBudgetReservation::check_capacity(
            &parent,
            &BudgetUsage::zero(),
            &[],
            parent.deadline()
        )
        .is_err()
    );
    let maximum = budget(u64::MAX);
    let huge = CumulativeBudgetReservation::from_budget(&maximum).unwrap();
    assert!(matches!(
        CumulativeBudgetReservation::check_capacity(
            &maximum,
            &BudgetUsage::zero(),
            &[huge, reservation(1)],
            now()
        ),
        Err(CumulativeBudgetReservationError::Usage(_))
    ));
    let cost_only = |value| {
        CumulativeBudgetReservation::new(
            BudgetUsage::builder()
                .known_costs(
                    KnownCosts::try_new([Money::new("USD".parse().unwrap(), value)]).unwrap(),
                )
                .build()
                .unwrap(),
        )
        .unwrap()
    };
    assert!(matches!(
        CumulativeBudgetReservation::check_capacity(
            &maximum,
            &BudgetUsage::zero(),
            &[cost_only(u64::MAX), cost_only(1)],
            now()
        ),
        Err(CumulativeBudgetReservationError::Usage(_))
    ));
}

#[test]
fn capacity_work_is_bounded_and_wire_schema_requires_zero_peaks() {
    let at_limit = vec![reservation(0); CumulativeBudgetReservation::MAX_RESERVATIONS];
    assert!(check(&BudgetUsage::zero(), &at_limit).is_ok());
    let too_many = vec![reservation(0); CumulativeBudgetReservation::MAX_RESERVATIONS + 1];
    assert!(matches!(
        check(&BudgetUsage::zero(), &too_many),
        Err(CumulativeBudgetReservationError::TooManyReservations)
    ));
    let schema: Value = to_value(schemars::schema_for!(CumulativeBudgetReservation)).unwrap();
    assert_eq!(schema["additionalProperties"], false);
    for field in [
        "graph_depth",
        "concurrent_branches",
        "fan_out",
        "unpriced_cost_events",
    ] {
        assert_eq!(
            schema["properties"]["amount"]["allOf"][1]["properties"][field]["const"],
            "0"
        );
    }
}

proptest! {
    #[test]
    fn reservation_order_does_not_change_capacity(a in 0_u64..100, b in 0_u64..100, spent in 0_u64..100) {
        let accounted = reservation(spent).amount().clone();
        let left = check(&accounted, &[reservation(a), reservation(b)]);
        let right = check(&accounted, &[reservation(b), reservation(a)]);
        prop_assert_eq!(left.is_ok(), a + b + spent <= 100);
        prop_assert_eq!(left.is_ok(), right.is_ok());
        if let (Ok(left), Ok(right)) = (left, right) {
            prop_assert_eq!(to_value(left).unwrap(), to_value(right).unwrap());
        }
    }
}
