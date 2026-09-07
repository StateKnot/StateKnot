// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Pure cumulative-capacity arithmetic, separate from durable reservation.

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    BudgetDimension, BudgetEvaluationError, BudgetRemaining, BudgetUsage, BudgetUsageError,
    CostCollectionError, ExecutionCount, KnownCosts, ResolvedBudget, Timestamp,
};

/// An immutable ceiling reserved for cumulative work, not observed usage.
///
/// All cumulative dimensions, inclusive token/call subsets, and configured
/// currencies are reserved. High-water dimensions (`graph_depth`,
/// `concurrent_branches`, `fan_out`) are deliberately absent (zero in the
/// underlying amount); topology and live concurrency require their own
/// admission checks. Unknown price is never a zero-cost reservation.
///
/// This value does not reserve database capacity. A durable caller must bind
/// it to one ownership key and atomically check the complete set of current
/// reservations and accounted usage while serializing competing admissions
/// and direct-work budget consumption. Never persist the projected capacity
/// from [`Self::check_capacity`] as actual usage.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CumulativeBudgetReservation {
    #[schemars(schema_with = "amount_schema")]
    amount: BudgetUsage,
}

fn amount_schema(generator: &mut SchemaGenerator) -> Schema {
    json_schema!({
        "allOf": [
            generator.subschema_for::<BudgetUsage>(),
            {"properties": {
                "graph_depth": {"const": "0"},
                "concurrent_branches": {"const": "0"},
                "fan_out": {"const": "0"},
                "unpriced_cost_events": {"const": "0"}
            }}
        ]
    })
}

impl CumulativeBudgetReservation {
    /// Maximum reservation count accepted by one bounded capacity check.
    ///
    /// This is an arithmetic work bound, not an implemented child fan-out limit.
    pub const MAX_RESERVATIONS: usize = 256;

    /// Constructs a reservation from explicit cumulative ceilings.
    ///
    /// Rejects high-water dimensions and unpriced activity. Subset and integer
    /// validation is inherited from [`BudgetUsage`]. Zero denies further
    /// cumulative work; it does not mean unlimited capacity.
    pub fn new(amount: BudgetUsage) -> Result<Self, CumulativeBudgetReservationError> {
        for (dimension, count) in [
            (BudgetDimension::GraphDepth, amount.graph_depth()),
            (
                BudgetDimension::ConcurrentBranches,
                amount.concurrent_branches(),
            ),
            (BudgetDimension::FanOut, amount.fan_out()),
        ] {
            if count != ExecutionCount::ZERO {
                return Err(CumulativeBudgetReservationError::HighWaterDimension { dimension });
            }
        }
        if amount.unpriced_cost_events() != ExecutionCount::ZERO {
            return Err(CumulativeBudgetReservationError::UnpricedCost);
        }
        Ok(Self { amount })
    }

    /// Reserves every cumulative ceiling from an already finite budget.
    ///
    /// Does not copy the deadline or high-water limits. Child deadline,
    /// authority, ancestry, and concurrency narrowing must be checked apart
    /// from this arithmetic projection.
    pub fn from_budget(budget: &ResolvedBudget) -> Result<Self, CumulativeBudgetReservationError> {
        Self::new(
            BudgetUsage::builder()
                .graph_steps(budget.graph_steps())
                .model_attempts(budget.model_attempts())
                .model_turns(budget.model_turns())
                .input_tokens(budget.input_tokens())
                .cached_input_tokens(budget.cached_input_tokens())
                .reasoning_tokens(budget.reasoning_tokens())
                .output_tokens(budget.output_tokens())
                .tool_calls(budget.tool_calls())
                .write_calls(budget.write_calls())
                .remote_agent_delegations(budget.remote_agent_delegations())
                .retries(budget.retries())
                .input_bytes(budget.input_bytes())
                .output_bytes(budget.output_bytes())
                .event_bytes(budget.event_bytes())
                .checkpoint_bytes(budget.checkpoint_bytes())
                .artifact_bytes(budget.artifact_bytes())
                .known_costs(KnownCosts::try_new(budget.costs().iter().copied())?)
                .build()?,
        )
    }

    /// Returns reserved ceilings encoded using the validated usage dimensions.
    ///
    /// These amounts are not evidence of spent tokens, calls, bytes, or money.
    #[must_use]
    pub const fn amount(&self) -> &BudgetUsage {
        &self.amount
    }

    /// Checks accounted usage plus all outstanding reservations, including any
    /// proposed new reservation, against the parent budget at a supplied clock.
    ///
    /// `accounted` must include direct usage and settled child usage exactly
    /// once, but exclude these outstanding reservations. Uses checked addition
    /// and rejects unknown price, unsupported currencies, excess limits, and
    /// expired deadlines. Existing high-water usage is checked, not decremented
    /// per child. Duplicate entries are charged twice, never silently deduped.
    ///
    /// This pure check neither owns a lock nor proves a durable snapshot is
    /// complete. Commit-time atomicity is the storage adapter's responsibility.
    pub fn check_capacity(
        parent: &ResolvedBudget,
        accounted: &BudgetUsage,
        outstanding: &[Self],
        observed_at: Timestamp,
    ) -> Result<BudgetRemaining, CumulativeBudgetReservationError> {
        if outstanding.len() > Self::MAX_RESERVATIONS {
            return Err(CumulativeBudgetReservationError::TooManyReservations);
        }
        let mut projected = accounted.clone();
        for reservation in outstanding {
            projected = projected.checked_accumulate(&reservation.amount)?;
        }
        Ok(parent.remaining(&projected, observed_at)?)
    }
}

impl<'de> Deserialize<'de> for CumulativeBudgetReservation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            amount: BudgetUsage,
        }
        Self::new(Wire::deserialize(deserializer)?.amount).map_err(de::Error::custom)
    }
}

/// Invalid cumulative reservation or insufficient projected budget capacity.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CumulativeBudgetReservationError {
    /// A peak topology count cannot be reserved as cumulative expenditure.
    #[error("cannot cumulatively reserve high-water dimension {dimension:?}")]
    HighWaterDimension {
        /// The rejected non-cumulative dimension.
        dimension: BudgetDimension,
    },
    /// Unknown external price cannot be treated as a finite known reservation.
    #[error("cannot reserve unpriced activity as known cumulative cost")]
    UnpricedCost,
    /// One check exceeded its bounded amount of arithmetic work.
    #[error("too many outstanding cumulative budget reservations")]
    TooManyReservations,
    /// Checked scalar addition, currency union, or subset validation failed.
    #[error(transparent)]
    Usage(#[from] BudgetUsageError),
    /// A currency-specific reservation was invalid.
    #[error(transparent)]
    Costs(#[from] CostCollectionError),
    /// The projected capacity exceeds a ceiling or cannot be evaluated safely.
    #[error(transparent)]
    Capacity(#[from] BudgetEvaluationError),
}

#[cfg(test)]
#[path = "budget_reservation_tests.rs"]
mod tests;
