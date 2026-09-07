// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Offline ownership-key and cumulative-capacity arithmetic example.
//! This does not spawn children, acquire leases, or commit reservations.

use std::error::Error;

use stateknot_core::{
    BudgetLimits, BudgetUsage, ByteCount, Checkpoint, ChildRunKey, ChildRunSlot, CostLimits,
    CumulativeBudgetReservation, ExecutionCount, Money, NodeActivation, NodeId, ResolvedBudget,
    Timestamp, TokenCount,
};

fn limits(amount: u64) -> Result<ResolvedBudget, Box<dyn Error>> {
    let count = ExecutionCount::new(amount);
    let tokens = TokenCount::new(amount);
    let bytes = ByteCount::new(amount);
    Ok(ResolvedBudget::resolve(&[BudgetLimits::empty()
        .with_deadline("2031-01-01T00:00:00.000000Z".parse()?)
        .with_graph_depth(ExecutionCount::new(8))
        .with_concurrent_branches(ExecutionCount::new(4))
        .with_fan_out(ExecutionCount::new(8))
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
        .with_input_bytes(bytes)
        .with_output_bytes(bytes)
        .with_event_bytes(bytes)
        .with_checkpoint_bytes(bytes)
        .with_artifact_bytes(bytes)
        .with_costs(CostLimits::try_new([Money::new(
            "USD".parse()?,
            amount,
        )])?)])?)
}

fn main() -> Result<(), Box<dyn Error>> {
    // This public, synthetic fixture supplies a committed-shaped checkpoint.
    // A production adapter must load and verify the actual durable checkpoint.
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../tests/fixtures/core-checkpoint-v1.json"))?;
    let checkpoint: Checkpoint = serde_json::from_value(fixture["checkpoints"][0].clone())?;
    let activation = NodeActivation::for_ready_root(&checkpoint, NodeId::new("authorize")?)?;
    let key = ChildRunKey::new(activation, ChildRunSlot::new("risk-check")?)?;
    let wire = serde_json::to_string(&key)?;
    assert_eq!(serde_json::from_str::<ChildRunKey>(&wire)?, key);

    // Pure arithmetic: no resource is reserved by creating these values.
    let parent = limits(100)?;
    let left = CumulativeBudgetReservation::from_budget(&limits(40)?)?;
    let right = CumulativeBudgetReservation::from_budget(&limits(50)?)?;
    let observed_at: Timestamp = "2030-01-01T00:00:02.000000Z".parse()?;
    let accounted = BudgetUsage::builder()
        .input_tokens(TokenCount::new(5))
        .build()?;
    let remaining = CumulativeBudgetReservation::check_capacity(
        &parent,
        &accounted,
        &[left.clone(), right.clone()],
        observed_at,
    )?;
    assert_eq!(remaining.input_tokens(), TokenCount::new(5));

    // A third allocation is refused; it cannot reuse siblings' reserved tokens.
    let extra = CumulativeBudgetReservation::from_budget(&limits(6)?)?;
    assert!(
        CumulativeBudgetReservation::check_capacity(
            &parent,
            &accounted,
            &[left, right, extra],
            observed_at,
        )
        .is_err()
    );

    println!("logical ownership: {}", key.digest());
    println!("remaining input tokens: {}", remaining.input_tokens());
    println!("excess allocation refused; no child Run or durable reservation created");
    Ok(())
}
