// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Finite, layered execution budgets and monotonic usage accounting.

use std::{collections::BTreeMap, fmt, slice};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{ByteCount, CurrencyCode, ExecutionCount, Money, Timestamp, TokenCount};

/// Maximum number of independently configured layers resolved into one budget.
pub const MAX_BUDGET_LAYERS: usize = 16;

/// Maximum number of currencies tracked by one run budget.
pub const MAX_COST_CURRENCIES: usize = 16;

/// A bounded set of monetary ceilings keyed by currency.
///
/// Values serialize in ascending currency-code order. A missing currency is not
/// unlimited: a charge in an unlisted currency is rejected by budget
/// evaluation. An empty set permits no priced charge. `StateKnot` never
/// performs implicit currency conversion.
#[derive(Clone, Default, Eq, Hash, PartialEq)]
pub struct CostLimits(Box<[Money]>);

impl CostLimits {
    /// Validates and constructs currency-specific cost ceilings.
    ///
    /// # Errors
    ///
    /// Returns [`CostCollectionError`] when a currency is repeated or the
    /// currency-count ceiling is exceeded. Empty input is valid and denies all
    /// priced charges.
    pub fn try_new<I>(values: I) -> Result<Self, CostCollectionError>
    where
        I: IntoIterator<Item = Money>,
    {
        collect_costs(values).map(Self)
    }

    /// Returns the number of independently budgeted currencies.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no priced currency is permitted.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns limits in ascending currency-code order.
    pub fn iter(&self) -> slice::Iter<'_, Money> {
        self.0.iter()
    }

    /// Returns the ceiling for one currency when configured.
    #[must_use]
    pub fn get(&self, currency: CurrencyCode) -> Option<Money> {
        self.0
            .binary_search_by_key(&currency, |money| money.currency())
            .ok()
            .map(|index| self.0[index])
    }

    fn most_restrictive(&self, other: &Self) -> Self {
        let values = self
            .iter()
            .filter_map(|left| {
                other.get(left.currency()).map(|right| {
                    Money::new(left.currency(), left.micro_units().min(right.micro_units()))
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self(values)
    }
}

impl fmt::Debug for CostLimits {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("CostLimits").field(&self.0).finish()
    }
}

impl<'a> IntoIterator for &'a CostLimits {
    type Item = &'a Money;
    type IntoIter = slice::Iter<'a, Money>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<Vec<Money>> for CostLimits {
    type Error = CostCollectionError;

    fn try_from(values: Vec<Money>) -> Result<Self, Self::Error> {
        Self::try_new(values)
    }
}

impl Serialize for CostLimits {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for CostLimits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CostLimitsVisitor)
    }
}

struct CostLimitsVisitor;

impl<'de> de::Visitor<'de> for CostLimitsVisitor {
    type Value = CostLimits;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {MAX_COST_CURRENCIES} unique currency limits"
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(
            sequence
                .size_hint()
                .unwrap_or_default()
                .min(MAX_COST_CURRENCIES),
        );
        while let Some(value) = sequence.next_element::<Money>()? {
            if values.len() == MAX_COST_CURRENCIES {
                return Err(de::Error::custom(CostCollectionError::TooMany {
                    max: MAX_COST_CURRENCIES,
                    observed: MAX_COST_CURRENCIES + 1,
                }));
            }
            values.push(value);
        }
        Self::Value::try_new(values).map_err(de::Error::custom)
    }
}

impl JsonSchema for CostLimits {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "CostLimits".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::CostLimits").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<Money>(),
            "minItems": 0,
            "maxItems": 16,
            "uniqueItems": true,
            "description": "Currency codes must be unique; StateKnot serializes entries in ascending currency order."
        })
    }
}

/// Known accumulated monetary charges keyed by currency.
///
/// An empty value means no *known* charge. It does not represent unpriced
/// provider activity; [`BudgetUsage::unpriced_cost_events`] tracks that state
/// explicitly.
#[derive(Clone, Default, Eq, Hash, PartialEq)]
pub struct KnownCosts(Box<[Money]>);

impl KnownCosts {
    /// Validates and constructs known charges.
    ///
    /// # Errors
    ///
    /// Returns [`CostCollectionError`] for duplicate currencies or excessive
    /// currency cardinality. Empty input is valid.
    pub fn try_new<I>(values: I) -> Result<Self, CostCollectionError>
    where
        I: IntoIterator<Item = Money>,
    {
        collect_costs(values).map(Self)
    }

    /// Returns an empty known-cost ledger.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns the number of currencies with known accumulated cost.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no known monetary cost has been recorded.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns known charges in ascending currency-code order.
    pub fn iter(&self) -> slice::Iter<'_, Money> {
        self.0.iter()
    }

    /// Returns the accumulated charge for one currency when present.
    #[must_use]
    pub fn get(&self, currency: CurrencyCode) -> Option<Money> {
        self.0
            .binary_search_by_key(&currency, |money| money.currency())
            .ok()
            .map(|index| self.0[index])
    }

    fn checked_accumulate(&self, other: &Self) -> Result<Self, CostCollectionError> {
        let mut values: BTreeMap<CurrencyCode, Money> = BTreeMap::new();
        for money in self.iter().chain(other.iter()).copied() {
            if let Some(existing) = values.get_mut(&money.currency()) {
                let micro_units = existing
                    .micro_units()
                    .checked_add(money.micro_units())
                    .ok_or(CostCollectionError::Overflow {
                        currency: money.currency(),
                    })?;
                *existing = Money::new(money.currency(), micro_units);
            } else {
                if values.len() == MAX_COST_CURRENCIES {
                    return Err(CostCollectionError::TooMany {
                        max: MAX_COST_CURRENCIES,
                        observed: MAX_COST_CURRENCIES + 1,
                    });
                }
                values.insert(money.currency(), money);
            }
        }
        Ok(Self(
            values.into_values().collect::<Vec<_>>().into_boxed_slice(),
        ))
    }
}

impl fmt::Debug for KnownCosts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("KnownCosts").field(&self.0).finish()
    }
}

impl<'a> IntoIterator for &'a KnownCosts {
    type Item = &'a Money;
    type IntoIter = slice::Iter<'a, Money>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<Vec<Money>> for KnownCosts {
    type Error = CostCollectionError;

    fn try_from(values: Vec<Money>) -> Result<Self, Self::Error> {
        Self::try_new(values)
    }
}

impl Serialize for KnownCosts {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

impl<'de> Deserialize<'de> for KnownCosts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(KnownCostsVisitor)
    }
}

struct KnownCostsVisitor;

impl<'de> de::Visitor<'de> for KnownCostsVisitor {
    type Value = KnownCosts;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "an array containing at most {MAX_COST_CURRENCIES} unique currency charges"
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(
            sequence
                .size_hint()
                .unwrap_or_default()
                .min(MAX_COST_CURRENCIES),
        );
        while let Some(value) = sequence.next_element::<Money>()? {
            if values.len() == MAX_COST_CURRENCIES {
                return Err(de::Error::custom(CostCollectionError::TooMany {
                    max: MAX_COST_CURRENCIES,
                    observed: MAX_COST_CURRENCIES + 1,
                }));
            }
            values.push(value);
        }
        Self::Value::try_new(values).map_err(de::Error::custom)
    }
}

impl JsonSchema for KnownCosts {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "KnownCosts".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::KnownCosts").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<Money>(),
            "maxItems": 16,
            "uniqueItems": true,
            "description": "Known charges keyed by unique currency; unpriced events are represented separately."
        })
    }
}

fn collect_costs<I>(values: I) -> Result<Box<[Money]>, CostCollectionError>
where
    I: IntoIterator<Item = Money>,
{
    let mut collected = BTreeMap::new();
    for money in values {
        if collected.contains_key(&money.currency()) {
            return Err(CostCollectionError::DuplicateCurrency {
                currency: money.currency(),
            });
        }
        if collected.len() == MAX_COST_CURRENCIES {
            return Err(CostCollectionError::TooMany {
                max: MAX_COST_CURRENCIES,
                observed: MAX_COST_CURRENCIES + 1,
            });
        }
        collected.insert(money.currency(), money);
    }
    Ok(collected
        .into_values()
        .collect::<Vec<_>>()
        .into_boxed_slice())
}

/// Validation or arithmetic failure for a currency-keyed cost collection.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum CostCollectionError {
    /// The same currency appeared more than once.
    #[error("cost collection repeats currency {currency}")]
    DuplicateCurrency {
        /// Repeated currency.
        currency: CurrencyCode,
    },

    /// Currency cardinality exceeded the hard v1 ceiling.
    #[error("cost collection contains at least {observed} currencies; maximum is {max}")]
    TooMany {
        /// Maximum accepted distinct currencies.
        max: usize,
        /// Minimum distinct count observed before validation stopped.
        observed: usize,
    },

    /// Accumulated known cost exceeded the integer representation.
    #[error("known cost for {currency} overflowed")]
    Overflow {
        /// Currency whose accumulator overflowed.
        currency: CurrencyCode,
    },
}

/// Stable budget dimension used by resolution and enforcement errors.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum BudgetDimension {
    /// Absolute run deadline.
    Deadline,
    /// Maximum graph nesting depth observed.
    GraphDepth,
    /// Cumulative graph steps committed.
    GraphSteps,
    /// Cumulative provider invocation attempts.
    ModelAttempts,
    /// Cumulative logical agent/model turns.
    ModelTurns,
    /// Inclusive cumulative model input tokens.
    InputTokens,
    /// Cached-input subset of cumulative input tokens.
    CachedInputTokens,
    /// Reasoning subset of cumulative output tokens.
    ReasoningTokens,
    /// Inclusive cumulative model output tokens.
    OutputTokens,
    /// Cumulative tool calls.
    ToolCalls,
    /// Write-capable subset of cumulative tool calls.
    WriteCalls,
    /// Cumulative remote-agent delegations.
    RemoteAgentDelegations,
    /// Cumulative retries across bounded retry policies.
    Retries,
    /// Maximum concurrently active branches observed.
    ConcurrentBranches,
    /// Maximum fan-out produced by one scheduling decision.
    FanOut,
    /// Cumulative materialized input bytes.
    InputBytes,
    /// Cumulative materialized output bytes.
    OutputBytes,
    /// Cumulative durable event bytes.
    EventBytes,
    /// Cumulative durable checkpoint bytes.
    CheckpointBytes,
    /// Cumulative artifact bytes registered by the run.
    ArtifactBytes,
    /// Known monetary cost ceilings keyed by currency.
    Costs,
}

/// Optional limits supplied by one configuration or request layer.
///
/// Absence means that this layer has no opinion; it never means the resolved
/// run is unlimited. [`ResolvedBudget::resolve`] requires another layer to
/// supply every dimension and always chooses the most restrictive value.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    deadline: Option<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    graph_depth: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    graph_steps: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_attempts: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model_turns: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_tokens: Option<TokenCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cached_input_tokens: Option<TokenCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_tokens: Option<TokenCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_tokens: Option<TokenCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_calls: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_agent_delegations: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retries: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    concurrent_branches: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fan_out: Option<ExecutionCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_bytes: Option<ByteCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_bytes: Option<ByteCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_bytes: Option<ByteCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_bytes: Option<ByteCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_bytes: Option<ByteCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    costs: Option<CostLimits>,
}

macro_rules! define_optional_limit_methods {
    ($(($field:ident, $setter:ident, $type:ty)),+ $(,)?) => {
        $(
            #[doc = concat!("Returns this layer's optional `", stringify!($field), "` limit.")]
            #[must_use]
            pub const fn $field(&self) -> Option<$type> {
                self.$field
            }

            #[doc = concat!("Sets this layer's `", stringify!($field), "` limit.")]
            #[must_use]
            pub const fn $setter(mut self, value: $type) -> Self {
                self.$field = Some(value);
                self
            }
        )+
    };
}

impl BudgetLimits {
    /// Constructs a layer with no limits of its own.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            deadline: None,
            graph_depth: None,
            graph_steps: None,
            model_attempts: None,
            model_turns: None,
            input_tokens: None,
            cached_input_tokens: None,
            reasoning_tokens: None,
            output_tokens: None,
            tool_calls: None,
            write_calls: None,
            remote_agent_delegations: None,
            retries: None,
            concurrent_branches: None,
            fan_out: None,
            input_bytes: None,
            output_bytes: None,
            event_bytes: None,
            checkpoint_bytes: None,
            artifact_bytes: None,
            costs: None,
        }
    }

    /// Returns whether this layer contributes no constraint.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.deadline.is_none()
            && self.graph_depth.is_none()
            && self.graph_steps.is_none()
            && self.model_attempts.is_none()
            && self.model_turns.is_none()
            && self.input_tokens.is_none()
            && self.cached_input_tokens.is_none()
            && self.reasoning_tokens.is_none()
            && self.output_tokens.is_none()
            && self.tool_calls.is_none()
            && self.write_calls.is_none()
            && self.remote_agent_delegations.is_none()
            && self.retries.is_none()
            && self.concurrent_branches.is_none()
            && self.fan_out.is_none()
            && self.input_bytes.is_none()
            && self.output_bytes.is_none()
            && self.event_bytes.is_none()
            && self.checkpoint_bytes.is_none()
            && self.artifact_bytes.is_none()
            && self.costs.is_none()
    }

    define_optional_limit_methods!(
        (deadline, with_deadline, Timestamp),
        (graph_depth, with_graph_depth, ExecutionCount),
        (graph_steps, with_graph_steps, ExecutionCount),
        (model_attempts, with_model_attempts, ExecutionCount),
        (model_turns, with_model_turns, ExecutionCount),
        (input_tokens, with_input_tokens, TokenCount),
        (cached_input_tokens, with_cached_input_tokens, TokenCount),
        (reasoning_tokens, with_reasoning_tokens, TokenCount),
        (output_tokens, with_output_tokens, TokenCount),
        (tool_calls, with_tool_calls, ExecutionCount),
        (write_calls, with_write_calls, ExecutionCount),
        (
            remote_agent_delegations,
            with_remote_agent_delegations,
            ExecutionCount
        ),
        (retries, with_retries, ExecutionCount),
        (
            concurrent_branches,
            with_concurrent_branches,
            ExecutionCount
        ),
        (fan_out, with_fan_out, ExecutionCount),
        (input_bytes, with_input_bytes, ByteCount),
        (output_bytes, with_output_bytes, ByteCount),
        (event_bytes, with_event_bytes, ByteCount),
        (checkpoint_bytes, with_checkpoint_bytes, ByteCount),
        (artifact_bytes, with_artifact_bytes, ByteCount),
    );

    /// Returns this layer's optional currency-specific cost ceilings.
    #[must_use]
    pub const fn costs(&self) -> Option<&CostLimits> {
        self.costs.as_ref()
    }

    /// Sets this layer's currency-specific cost ceilings.
    #[must_use]
    pub fn with_costs(mut self, value: CostLimits) -> Self {
        self.costs = Some(value);
        self
    }

    /// Intersects two partial layers without inventing missing values.
    ///
    /// When both layers specify costs, their currency allowlists are
    /// intersected and same-currency ceilings take the minimum. A layer cannot
    /// introduce a currency absent from another applicable cost layer.
    #[must_use]
    pub fn most_restrictive(&self, other: &Self) -> Self {
        Self {
            deadline: min_option(self.deadline, other.deadline),
            graph_depth: min_option(self.graph_depth, other.graph_depth),
            graph_steps: min_option(self.graph_steps, other.graph_steps),
            model_attempts: min_option(self.model_attempts, other.model_attempts),
            model_turns: min_option(self.model_turns, other.model_turns),
            input_tokens: min_option(self.input_tokens, other.input_tokens),
            cached_input_tokens: min_option(self.cached_input_tokens, other.cached_input_tokens),
            reasoning_tokens: min_option(self.reasoning_tokens, other.reasoning_tokens),
            output_tokens: min_option(self.output_tokens, other.output_tokens),
            tool_calls: min_option(self.tool_calls, other.tool_calls),
            write_calls: min_option(self.write_calls, other.write_calls),
            remote_agent_delegations: min_option(
                self.remote_agent_delegations,
                other.remote_agent_delegations,
            ),
            retries: min_option(self.retries, other.retries),
            concurrent_branches: min_option(self.concurrent_branches, other.concurrent_branches),
            fan_out: min_option(self.fan_out, other.fan_out),
            input_bytes: min_option(self.input_bytes, other.input_bytes),
            output_bytes: min_option(self.output_bytes, other.output_bytes),
            event_bytes: min_option(self.event_bytes, other.event_bytes),
            checkpoint_bytes: min_option(self.checkpoint_bytes, other.checkpoint_bytes),
            artifact_bytes: min_option(self.artifact_bytes, other.artifact_bytes),
            costs: match (&self.costs, &other.costs) {
                (Some(left), Some(right)) => Some(left.most_restrictive(right)),
                (Some(value), None) | (None, Some(value)) => Some(value.clone()),
                (None, None) => None,
            },
        }
    }
}

fn min_option<T>(left: Option<T>, right: Option<T>) -> Option<T>
where
    T: Copy + Ord,
{
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

/// A complete, finite run budget after all applicable layers are intersected.
///
/// Every dimension is present. Cached input is bounded by inclusive input, and
/// reasoning is bounded by inclusive output even when a looser redundant
/// sub-limit was configured.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedBudget {
    deadline: Timestamp,
    graph_depth: ExecutionCount,
    graph_steps: ExecutionCount,
    model_attempts: ExecutionCount,
    model_turns: ExecutionCount,
    input_tokens: TokenCount,
    cached_input_tokens: TokenCount,
    reasoning_tokens: TokenCount,
    output_tokens: TokenCount,
    tool_calls: ExecutionCount,
    write_calls: ExecutionCount,
    remote_agent_delegations: ExecutionCount,
    retries: ExecutionCount,
    concurrent_branches: ExecutionCount,
    fan_out: ExecutionCount,
    input_bytes: ByteCount,
    output_bytes: ByteCount,
    event_bytes: ByteCount,
    checkpoint_bytes: ByteCount,
    artifact_bytes: ByteCount,
    costs: CostLimits,
}

macro_rules! define_resolved_getters {
    ($(($field:ident, $type:ty)),+ $(,)?) => {
        $(
            #[doc = concat!("Returns the resolved `", stringify!($field), "` ceiling.")]
            #[must_use]
            pub const fn $field(&self) -> $type {
                self.$field
            }
        )+
    };
}

impl ResolvedBudget {
    /// Resolves ordered configuration layers by taking each finite minimum.
    ///
    /// Layer order does not affect the result. The slice is bounded so an
    /// untrusted request cannot trigger unbounded configuration work.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetResolutionError`] for an empty or oversized layer list,
    /// any missing dimension, or an invalid cost collection.
    pub fn resolve(layers: &[BudgetLimits]) -> Result<Self, BudgetResolutionError> {
        if layers.is_empty() {
            return Err(BudgetResolutionError::EmptyLayers);
        }
        if layers.len() > MAX_BUDGET_LAYERS {
            return Err(BudgetResolutionError::TooManyLayers {
                max: MAX_BUDGET_LAYERS,
                actual: layers.len(),
            });
        }

        let mut effective = BudgetLimits::empty();
        for layer in layers {
            effective = effective.most_restrictive(layer);
        }

        let input_tokens = required(effective.input_tokens, BudgetDimension::InputTokens)?;
        let output_tokens = required(effective.output_tokens, BudgetDimension::OutputTokens)?;
        let tool_calls = required(effective.tool_calls, BudgetDimension::ToolCalls)?;
        let cached_input_tokens = required(
            effective.cached_input_tokens,
            BudgetDimension::CachedInputTokens,
        )?
        .min(input_tokens);
        let reasoning_tokens =
            required(effective.reasoning_tokens, BudgetDimension::ReasoningTokens)?
                .min(output_tokens);
        let write_calls =
            required(effective.write_calls, BudgetDimension::WriteCalls)?.min(tool_calls);

        Ok(Self {
            deadline: required(effective.deadline, BudgetDimension::Deadline)?,
            graph_depth: required(effective.graph_depth, BudgetDimension::GraphDepth)?,
            graph_steps: required(effective.graph_steps, BudgetDimension::GraphSteps)?,
            model_attempts: required(effective.model_attempts, BudgetDimension::ModelAttempts)?,
            model_turns: required(effective.model_turns, BudgetDimension::ModelTurns)?,
            input_tokens,
            cached_input_tokens,
            reasoning_tokens,
            output_tokens,
            tool_calls,
            write_calls,
            remote_agent_delegations: required(
                effective.remote_agent_delegations,
                BudgetDimension::RemoteAgentDelegations,
            )?,
            retries: required(effective.retries, BudgetDimension::Retries)?,
            concurrent_branches: required(
                effective.concurrent_branches,
                BudgetDimension::ConcurrentBranches,
            )?,
            fan_out: required(effective.fan_out, BudgetDimension::FanOut)?,
            input_bytes: required(effective.input_bytes, BudgetDimension::InputBytes)?,
            output_bytes: required(effective.output_bytes, BudgetDimension::OutputBytes)?,
            event_bytes: required(effective.event_bytes, BudgetDimension::EventBytes)?,
            checkpoint_bytes: required(
                effective.checkpoint_bytes,
                BudgetDimension::CheckpointBytes,
            )?,
            artifact_bytes: required(effective.artifact_bytes, BudgetDimension::ArtifactBytes)?,
            costs: effective.costs.ok_or(BudgetResolutionError::Missing {
                dimension: BudgetDimension::Costs,
            })?,
        })
    }

    define_resolved_getters!(
        (deadline, Timestamp),
        (graph_depth, ExecutionCount),
        (graph_steps, ExecutionCount),
        (model_attempts, ExecutionCount),
        (model_turns, ExecutionCount),
        (input_tokens, TokenCount),
        (cached_input_tokens, TokenCount),
        (reasoning_tokens, TokenCount),
        (output_tokens, TokenCount),
        (tool_calls, ExecutionCount),
        (write_calls, ExecutionCount),
        (remote_agent_delegations, ExecutionCount),
        (retries, ExecutionCount),
        (concurrent_branches, ExecutionCount),
        (fan_out, ExecutionCount),
        (input_bytes, ByteCount),
        (output_bytes, ByteCount),
        (event_bytes, ByteCount),
        (checkpoint_bytes, ByteCount),
        (artifact_bytes, ByteCount),
    );

    /// Returns resolved currency-specific monetary ceilings.
    #[must_use]
    pub const fn costs(&self) -> &CostLimits {
        &self.costs
    }

    fn validate(&self) -> Result<(), BudgetResolutionError> {
        if self.cached_input_tokens > self.input_tokens {
            return Err(BudgetResolutionError::InvalidSubsetLimit {
                subset: BudgetDimension::CachedInputTokens,
                inclusive: BudgetDimension::InputTokens,
            });
        }
        if self.reasoning_tokens > self.output_tokens {
            return Err(BudgetResolutionError::InvalidSubsetLimit {
                subset: BudgetDimension::ReasoningTokens,
                inclusive: BudgetDimension::OutputTokens,
            });
        }
        if self.write_calls > self.tool_calls {
            return Err(BudgetResolutionError::InvalidSubsetLimit {
                subset: BudgetDimension::WriteCalls,
                inclusive: BudgetDimension::ToolCalls,
            });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ResolvedBudget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            deadline: Timestamp,
            graph_depth: ExecutionCount,
            graph_steps: ExecutionCount,
            model_attempts: ExecutionCount,
            model_turns: ExecutionCount,
            input_tokens: TokenCount,
            cached_input_tokens: TokenCount,
            reasoning_tokens: TokenCount,
            output_tokens: TokenCount,
            tool_calls: ExecutionCount,
            write_calls: ExecutionCount,
            remote_agent_delegations: ExecutionCount,
            retries: ExecutionCount,
            concurrent_branches: ExecutionCount,
            fan_out: ExecutionCount,
            input_bytes: ByteCount,
            output_bytes: ByteCount,
            event_bytes: ByteCount,
            checkpoint_bytes: ByteCount,
            artifact_bytes: ByteCount,
            costs: CostLimits,
        }

        let wire = Wire::deserialize(deserializer)?;
        let budget = Self {
            deadline: wire.deadline,
            graph_depth: wire.graph_depth,
            graph_steps: wire.graph_steps,
            model_attempts: wire.model_attempts,
            model_turns: wire.model_turns,
            input_tokens: wire.input_tokens,
            cached_input_tokens: wire.cached_input_tokens,
            reasoning_tokens: wire.reasoning_tokens,
            output_tokens: wire.output_tokens,
            tool_calls: wire.tool_calls,
            write_calls: wire.write_calls,
            remote_agent_delegations: wire.remote_agent_delegations,
            retries: wire.retries,
            concurrent_branches: wire.concurrent_branches,
            fan_out: wire.fan_out,
            input_bytes: wire.input_bytes,
            output_bytes: wire.output_bytes,
            event_bytes: wire.event_bytes,
            checkpoint_bytes: wire.checkpoint_bytes,
            artifact_bytes: wire.artifact_bytes,
            costs: wire.costs,
        };
        budget.validate().map_err(de::Error::custom)?;
        Ok(budget)
    }
}

fn required<T>(value: Option<T>, dimension: BudgetDimension) -> Result<T, BudgetResolutionError> {
    value.ok_or(BudgetResolutionError::Missing { dimension })
}

/// Failure while intersecting partial budget layers.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum BudgetResolutionError {
    /// No configuration layer was supplied.
    #[error("at least one budget layer is required")]
    EmptyLayers,

    /// Too many independently supplied layers were requested.
    #[error("budget resolution received {actual} layers; maximum is {max}")]
    TooManyLayers {
        /// Maximum accepted layers.
        max: usize,
        /// Observed layer count.
        actual: usize,
    },

    /// No layer supplied a finite value for one mandatory dimension.
    #[error("resolved budget is missing dimension {dimension:?}")]
    Missing {
        /// Missing dimension.
        dimension: BudgetDimension,
    },

    /// A serialized subset limit exceeded its inclusive parent limit.
    #[error("budget subset {subset:?} exceeds inclusive limit {inclusive:?}")]
    InvalidSubsetLimit {
        /// Subset dimension.
        subset: BudgetDimension,
        /// Inclusive parent dimension.
        inclusive: BudgetDimension,
    },
}

/// Builder for a validated [`BudgetUsage`] snapshot or delta.
#[derive(Clone, Debug, Default)]
pub struct BudgetUsageBuilder {
    graph_depth: ExecutionCount,
    graph_steps: ExecutionCount,
    model_attempts: ExecutionCount,
    model_turns: ExecutionCount,
    input_tokens: TokenCount,
    cached_input_tokens: TokenCount,
    reasoning_tokens: TokenCount,
    output_tokens: TokenCount,
    tool_calls: ExecutionCount,
    write_calls: ExecutionCount,
    remote_agent_delegations: ExecutionCount,
    retries: ExecutionCount,
    concurrent_branches: ExecutionCount,
    fan_out: ExecutionCount,
    input_bytes: ByteCount,
    output_bytes: ByteCount,
    event_bytes: ByteCount,
    checkpoint_bytes: ByteCount,
    artifact_bytes: ByteCount,
    known_costs: KnownCosts,
    unpriced_cost_events: ExecutionCount,
}

macro_rules! define_usage_builder_methods {
    ($(($field:ident, $type:ty)),+ $(,)?) => {
        $(
            #[doc = concat!("Sets the usage builder's `", stringify!($field), "` value.")]
            #[must_use]
            pub const fn $field(mut self, value: $type) -> Self {
                self.$field = value;
                self
            }
        )+
    };
}

impl BudgetUsageBuilder {
    define_usage_builder_methods!(
        (graph_depth, ExecutionCount),
        (graph_steps, ExecutionCount),
        (model_attempts, ExecutionCount),
        (model_turns, ExecutionCount),
        (input_tokens, TokenCount),
        (cached_input_tokens, TokenCount),
        (reasoning_tokens, TokenCount),
        (output_tokens, TokenCount),
        (tool_calls, ExecutionCount),
        (write_calls, ExecutionCount),
        (remote_agent_delegations, ExecutionCount),
        (retries, ExecutionCount),
        (concurrent_branches, ExecutionCount),
        (fan_out, ExecutionCount),
        (input_bytes, ByteCount),
        (output_bytes, ByteCount),
        (event_bytes, ByteCount),
        (checkpoint_bytes, ByteCount),
        (artifact_bytes, ByteCount),
        (unpriced_cost_events, ExecutionCount),
    );

    /// Sets known currency-specific accumulated charges.
    #[must_use]
    pub fn known_costs(mut self, value: KnownCosts) -> Self {
        self.known_costs = value;
        self
    }

    /// Validates and builds the immutable usage value.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetUsageError`] if cached input exceeds inclusive input or
    /// reasoning exceeds inclusive output.
    pub fn build(self) -> Result<BudgetUsage, BudgetUsageError> {
        BudgetUsage::from_builder(self)
    }
}

/// Monotonic run usage with explicit provider-observability gaps.
///
/// `graph_depth`, `concurrent_branches`, and `fan_out` are high-water marks.
/// Every other numeric field is cumulative. Input tokens include cached input;
/// output tokens include reasoning. Unpriced provider activity is counted
/// separately and never treated as zero cost.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetUsage {
    graph_depth: ExecutionCount,
    graph_steps: ExecutionCount,
    model_attempts: ExecutionCount,
    model_turns: ExecutionCount,
    input_tokens: TokenCount,
    cached_input_tokens: TokenCount,
    reasoning_tokens: TokenCount,
    output_tokens: TokenCount,
    tool_calls: ExecutionCount,
    write_calls: ExecutionCount,
    remote_agent_delegations: ExecutionCount,
    retries: ExecutionCount,
    concurrent_branches: ExecutionCount,
    fan_out: ExecutionCount,
    input_bytes: ByteCount,
    output_bytes: ByteCount,
    event_bytes: ByteCount,
    checkpoint_bytes: ByteCount,
    artifact_bytes: ByteCount,
    known_costs: KnownCosts,
    unpriced_cost_events: ExecutionCount,
}

macro_rules! define_usage_getters {
    ($(($field:ident, $type:ty)),+ $(,)?) => {
        $(
            #[doc = concat!("Returns recorded `", stringify!($field), "` usage.")]
            #[must_use]
            pub const fn $field(&self) -> $type {
                self.$field
            }
        )+
    };
}

impl BudgetUsage {
    /// Returns a builder initialized to zero usage.
    #[must_use]
    pub fn builder() -> BudgetUsageBuilder {
        BudgetUsageBuilder::default()
    }

    /// Returns a valid all-zero usage snapshot.
    #[must_use]
    pub fn zero() -> Self {
        Self {
            graph_depth: ExecutionCount::ZERO,
            graph_steps: ExecutionCount::ZERO,
            model_attempts: ExecutionCount::ZERO,
            model_turns: ExecutionCount::ZERO,
            input_tokens: TokenCount::ZERO,
            cached_input_tokens: TokenCount::ZERO,
            reasoning_tokens: TokenCount::ZERO,
            output_tokens: TokenCount::ZERO,
            tool_calls: ExecutionCount::ZERO,
            write_calls: ExecutionCount::ZERO,
            remote_agent_delegations: ExecutionCount::ZERO,
            retries: ExecutionCount::ZERO,
            concurrent_branches: ExecutionCount::ZERO,
            fan_out: ExecutionCount::ZERO,
            input_bytes: ByteCount::ZERO,
            output_bytes: ByteCount::ZERO,
            event_bytes: ByteCount::ZERO,
            checkpoint_bytes: ByteCount::ZERO,
            artifact_bytes: ByteCount::ZERO,
            known_costs: KnownCosts::empty(),
            unpriced_cost_events: ExecutionCount::ZERO,
        }
    }

    fn from_builder(builder: BudgetUsageBuilder) -> Result<Self, BudgetUsageError> {
        if builder.cached_input_tokens > builder.input_tokens {
            return Err(BudgetUsageError::SubsetExceedsInclusive {
                subset: BudgetDimension::CachedInputTokens,
                inclusive: BudgetDimension::InputTokens,
            });
        }
        if builder.reasoning_tokens > builder.output_tokens {
            return Err(BudgetUsageError::SubsetExceedsInclusive {
                subset: BudgetDimension::ReasoningTokens,
                inclusive: BudgetDimension::OutputTokens,
            });
        }
        if builder.write_calls > builder.tool_calls {
            return Err(BudgetUsageError::SubsetExceedsInclusive {
                subset: BudgetDimension::WriteCalls,
                inclusive: BudgetDimension::ToolCalls,
            });
        }
        Ok(Self {
            graph_depth: builder.graph_depth,
            graph_steps: builder.graph_steps,
            model_attempts: builder.model_attempts,
            model_turns: builder.model_turns,
            input_tokens: builder.input_tokens,
            cached_input_tokens: builder.cached_input_tokens,
            reasoning_tokens: builder.reasoning_tokens,
            output_tokens: builder.output_tokens,
            tool_calls: builder.tool_calls,
            write_calls: builder.write_calls,
            remote_agent_delegations: builder.remote_agent_delegations,
            retries: builder.retries,
            concurrent_branches: builder.concurrent_branches,
            fan_out: builder.fan_out,
            input_bytes: builder.input_bytes,
            output_bytes: builder.output_bytes,
            event_bytes: builder.event_bytes,
            checkpoint_bytes: builder.checkpoint_bytes,
            artifact_bytes: builder.artifact_bytes,
            known_costs: builder.known_costs,
            unpriced_cost_events: builder.unpriced_cost_events,
        })
    }

    /// Monotonically accumulates a validated usage observation or delta.
    ///
    /// High-water dimensions take their maximum; cumulative dimensions use
    /// checked addition. This pure operation does not replace the durable
    /// reservation and commit protocol required for parallel execution.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetUsageError`] on integer or monetary overflow, or when
    /// the union of cost currencies exceeds the hard ceiling.
    pub fn checked_accumulate(&self, other: &Self) -> Result<Self, BudgetUsageError> {
        Ok(Self {
            graph_depth: self.graph_depth.max(other.graph_depth),
            graph_steps: add_execution(
                self.graph_steps,
                other.graph_steps,
                BudgetDimension::GraphSteps,
            )?,
            model_attempts: add_execution(
                self.model_attempts,
                other.model_attempts,
                BudgetDimension::ModelAttempts,
            )?,
            model_turns: add_execution(
                self.model_turns,
                other.model_turns,
                BudgetDimension::ModelTurns,
            )?,
            input_tokens: add_tokens(
                self.input_tokens,
                other.input_tokens,
                BudgetDimension::InputTokens,
            )?,
            cached_input_tokens: add_tokens(
                self.cached_input_tokens,
                other.cached_input_tokens,
                BudgetDimension::CachedInputTokens,
            )?,
            reasoning_tokens: add_tokens(
                self.reasoning_tokens,
                other.reasoning_tokens,
                BudgetDimension::ReasoningTokens,
            )?,
            output_tokens: add_tokens(
                self.output_tokens,
                other.output_tokens,
                BudgetDimension::OutputTokens,
            )?,
            tool_calls: add_execution(
                self.tool_calls,
                other.tool_calls,
                BudgetDimension::ToolCalls,
            )?,
            write_calls: add_execution(
                self.write_calls,
                other.write_calls,
                BudgetDimension::WriteCalls,
            )?,
            remote_agent_delegations: add_execution(
                self.remote_agent_delegations,
                other.remote_agent_delegations,
                BudgetDimension::RemoteAgentDelegations,
            )?,
            retries: add_execution(self.retries, other.retries, BudgetDimension::Retries)?,
            concurrent_branches: self.concurrent_branches.max(other.concurrent_branches),
            fan_out: self.fan_out.max(other.fan_out),
            input_bytes: add_bytes(
                self.input_bytes,
                other.input_bytes,
                BudgetDimension::InputBytes,
            )?,
            output_bytes: add_bytes(
                self.output_bytes,
                other.output_bytes,
                BudgetDimension::OutputBytes,
            )?,
            event_bytes: add_bytes(
                self.event_bytes,
                other.event_bytes,
                BudgetDimension::EventBytes,
            )?,
            checkpoint_bytes: add_bytes(
                self.checkpoint_bytes,
                other.checkpoint_bytes,
                BudgetDimension::CheckpointBytes,
            )?,
            artifact_bytes: add_bytes(
                self.artifact_bytes,
                other.artifact_bytes,
                BudgetDimension::ArtifactBytes,
            )?,
            known_costs: self
                .known_costs
                .checked_accumulate(&other.known_costs)
                .map_err(BudgetUsageError::Costs)?,
            unpriced_cost_events: add_execution(
                self.unpriced_cost_events,
                other.unpriced_cost_events,
                BudgetDimension::Costs,
            )?,
        })
    }

    define_usage_getters!(
        (graph_depth, ExecutionCount),
        (graph_steps, ExecutionCount),
        (model_attempts, ExecutionCount),
        (model_turns, ExecutionCount),
        (input_tokens, TokenCount),
        (cached_input_tokens, TokenCount),
        (reasoning_tokens, TokenCount),
        (output_tokens, TokenCount),
        (tool_calls, ExecutionCount),
        (write_calls, ExecutionCount),
        (remote_agent_delegations, ExecutionCount),
        (retries, ExecutionCount),
        (concurrent_branches, ExecutionCount),
        (fan_out, ExecutionCount),
        (input_bytes, ByteCount),
        (output_bytes, ByteCount),
        (event_bytes, ByteCount),
        (checkpoint_bytes, ByteCount),
        (artifact_bytes, ByteCount),
        (unpriced_cost_events, ExecutionCount),
    );

    /// Returns known accumulated charges by currency.
    #[must_use]
    pub const fn known_costs(&self) -> &KnownCosts {
        &self.known_costs
    }
}

impl<'de> Deserialize<'de> for BudgetUsage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            graph_depth: ExecutionCount,
            graph_steps: ExecutionCount,
            model_attempts: ExecutionCount,
            model_turns: ExecutionCount,
            input_tokens: TokenCount,
            cached_input_tokens: TokenCount,
            reasoning_tokens: TokenCount,
            output_tokens: TokenCount,
            tool_calls: ExecutionCount,
            write_calls: ExecutionCount,
            remote_agent_delegations: ExecutionCount,
            retries: ExecutionCount,
            concurrent_branches: ExecutionCount,
            fan_out: ExecutionCount,
            input_bytes: ByteCount,
            output_bytes: ByteCount,
            event_bytes: ByteCount,
            checkpoint_bytes: ByteCount,
            artifact_bytes: ByteCount,
            known_costs: KnownCosts,
            unpriced_cost_events: ExecutionCount,
        }

        let wire = Wire::deserialize(deserializer)?;
        BudgetUsageBuilder {
            graph_depth: wire.graph_depth,
            graph_steps: wire.graph_steps,
            model_attempts: wire.model_attempts,
            model_turns: wire.model_turns,
            input_tokens: wire.input_tokens,
            cached_input_tokens: wire.cached_input_tokens,
            reasoning_tokens: wire.reasoning_tokens,
            output_tokens: wire.output_tokens,
            tool_calls: wire.tool_calls,
            write_calls: wire.write_calls,
            remote_agent_delegations: wire.remote_agent_delegations,
            retries: wire.retries,
            concurrent_branches: wire.concurrent_branches,
            fan_out: wire.fan_out,
            input_bytes: wire.input_bytes,
            output_bytes: wire.output_bytes,
            event_bytes: wire.event_bytes,
            checkpoint_bytes: wire.checkpoint_bytes,
            artifact_bytes: wire.artifact_bytes,
            known_costs: wire.known_costs,
            unpriced_cost_events: wire.unpriced_cost_events,
        }
        .build()
        .map_err(de::Error::custom)
    }
}

fn add_execution(
    left: ExecutionCount,
    right: ExecutionCount,
    dimension: BudgetDimension,
) -> Result<ExecutionCount, BudgetUsageError> {
    left.checked_add(right)
        .ok_or(BudgetUsageError::Overflow { dimension })
}

fn add_tokens(
    left: TokenCount,
    right: TokenCount,
    dimension: BudgetDimension,
) -> Result<TokenCount, BudgetUsageError> {
    left.checked_add(right)
        .ok_or(BudgetUsageError::Overflow { dimension })
}

fn add_bytes(
    left: ByteCount,
    right: ByteCount,
    dimension: BudgetDimension,
) -> Result<ByteCount, BudgetUsageError> {
    left.checked_add(right)
        .ok_or(BudgetUsageError::Overflow { dimension })
}

/// Failure while constructing or monotonically accumulating usage.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum BudgetUsageError {
    /// Provider-normalized subset usage exceeded its inclusive total.
    #[error("usage subset {subset:?} exceeds inclusive usage {inclusive:?}")]
    SubsetExceedsInclusive {
        /// Subset dimension.
        subset: BudgetDimension,
        /// Inclusive parent dimension.
        inclusive: BudgetDimension,
    },

    /// Checked arithmetic overflowed in one dimension.
    #[error("budget usage overflowed dimension {dimension:?}")]
    Overflow {
        /// Overflowed dimension.
        dimension: BudgetDimension,
    },

    /// Known costs could not be accumulated safely.
    #[error("invalid known cost usage: {0}")]
    Costs(CostCollectionError),
}

/// Remaining finite capacity after evaluating one usage snapshot.
///
/// This value can only be constructed by [`ResolvedBudget::remaining`]. A
/// successful value therefore proves that every observed dimension is within
/// its ceiling and every cost event is priced in a configured currency.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetRemaining {
    deadline: Timestamp,
    graph_depth: ExecutionCount,
    graph_steps: ExecutionCount,
    model_attempts: ExecutionCount,
    model_turns: ExecutionCount,
    input_tokens: TokenCount,
    cached_input_tokens: TokenCount,
    reasoning_tokens: TokenCount,
    output_tokens: TokenCount,
    tool_calls: ExecutionCount,
    write_calls: ExecutionCount,
    remote_agent_delegations: ExecutionCount,
    retries: ExecutionCount,
    concurrent_branches: ExecutionCount,
    fan_out: ExecutionCount,
    input_bytes: ByteCount,
    output_bytes: ByteCount,
    event_bytes: ByteCount,
    checkpoint_bytes: ByteCount,
    artifact_bytes: ByteCount,
    costs: CostLimits,
}

impl ResolvedBudget {
    /// Evaluates usage at an explicitly supplied clock observation.
    ///
    /// The caller supplies `observed_at` so this pure operation is deterministic
    /// and a runtime can durably record the clock value that influenced a
    /// decision. Equality with the deadline is expired.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetEvaluationError`] for an expired deadline, any exceeded
    /// scalar ceiling, an unbudgeted currency, or unpriced provider activity.
    pub fn remaining(
        &self,
        usage: &BudgetUsage,
        observed_at: Timestamp,
    ) -> Result<BudgetRemaining, BudgetEvaluationError> {
        if observed_at >= self.deadline {
            return Err(BudgetEvaluationError::DeadlineExceeded {
                deadline: self.deadline,
                observed_at,
            });
        }

        Ok(BudgetRemaining {
            deadline: self.deadline,
            graph_depth: remaining_execution(
                BudgetDimension::GraphDepth,
                self.graph_depth,
                usage.graph_depth,
            )?,
            graph_steps: remaining_execution(
                BudgetDimension::GraphSteps,
                self.graph_steps,
                usage.graph_steps,
            )?,
            model_attempts: remaining_execution(
                BudgetDimension::ModelAttempts,
                self.model_attempts,
                usage.model_attempts,
            )?,
            model_turns: remaining_execution(
                BudgetDimension::ModelTurns,
                self.model_turns,
                usage.model_turns,
            )?,
            input_tokens: remaining_tokens(
                BudgetDimension::InputTokens,
                self.input_tokens,
                usage.input_tokens,
            )?,
            cached_input_tokens: remaining_tokens(
                BudgetDimension::CachedInputTokens,
                self.cached_input_tokens,
                usage.cached_input_tokens,
            )?,
            reasoning_tokens: remaining_tokens(
                BudgetDimension::ReasoningTokens,
                self.reasoning_tokens,
                usage.reasoning_tokens,
            )?,
            output_tokens: remaining_tokens(
                BudgetDimension::OutputTokens,
                self.output_tokens,
                usage.output_tokens,
            )?,
            tool_calls: remaining_execution(
                BudgetDimension::ToolCalls,
                self.tool_calls,
                usage.tool_calls,
            )?,
            write_calls: remaining_execution(
                BudgetDimension::WriteCalls,
                self.write_calls,
                usage.write_calls,
            )?,
            remote_agent_delegations: remaining_execution(
                BudgetDimension::RemoteAgentDelegations,
                self.remote_agent_delegations,
                usage.remote_agent_delegations,
            )?,
            retries: remaining_execution(BudgetDimension::Retries, self.retries, usage.retries)?,
            concurrent_branches: remaining_execution(
                BudgetDimension::ConcurrentBranches,
                self.concurrent_branches,
                usage.concurrent_branches,
            )?,
            fan_out: remaining_execution(BudgetDimension::FanOut, self.fan_out, usage.fan_out)?,
            input_bytes: remaining_bytes(
                BudgetDimension::InputBytes,
                self.input_bytes,
                usage.input_bytes,
            )?,
            output_bytes: remaining_bytes(
                BudgetDimension::OutputBytes,
                self.output_bytes,
                usage.output_bytes,
            )?,
            event_bytes: remaining_bytes(
                BudgetDimension::EventBytes,
                self.event_bytes,
                usage.event_bytes,
            )?,
            checkpoint_bytes: remaining_bytes(
                BudgetDimension::CheckpointBytes,
                self.checkpoint_bytes,
                usage.checkpoint_bytes,
            )?,
            artifact_bytes: remaining_bytes(
                BudgetDimension::ArtifactBytes,
                self.artifact_bytes,
                usage.artifact_bytes,
            )?,
            costs: remaining_costs(&self.costs, usage)?,
        })
    }
}

macro_rules! define_remaining_getters {
    ($(($field:ident, $type:ty)),+ $(,)?) => {
        $(
            #[doc = concat!("Returns remaining `", stringify!($field), "` capacity.")]
            #[must_use]
            pub const fn $field(&self) -> $type {
                self.$field
            }
        )+
    };
}

impl BudgetRemaining {
    /// Returns the absolute deadline used by this remaining-capacity view.
    #[must_use]
    pub const fn deadline(&self) -> Timestamp {
        self.deadline
    }

    define_remaining_getters!(
        (graph_depth, ExecutionCount),
        (graph_steps, ExecutionCount),
        (model_attempts, ExecutionCount),
        (model_turns, ExecutionCount),
        (input_tokens, TokenCount),
        (cached_input_tokens, TokenCount),
        (reasoning_tokens, TokenCount),
        (output_tokens, TokenCount),
        (tool_calls, ExecutionCount),
        (write_calls, ExecutionCount),
        (remote_agent_delegations, ExecutionCount),
        (retries, ExecutionCount),
        (concurrent_branches, ExecutionCount),
        (fan_out, ExecutionCount),
        (input_bytes, ByteCount),
        (output_bytes, ByteCount),
        (event_bytes, ByteCount),
        (checkpoint_bytes, ByteCount),
        (artifact_bytes, ByteCount),
    );

    /// Returns known remaining monetary capacity by configured currency.
    #[must_use]
    pub const fn costs(&self) -> &CostLimits {
        &self.costs
    }
}

fn remaining_execution(
    dimension: BudgetDimension,
    limit: ExecutionCount,
    actual: ExecutionCount,
) -> Result<ExecutionCount, BudgetEvaluationError> {
    limit
        .checked_sub(actual)
        .ok_or(BudgetEvaluationError::ExecutionLimitExceeded {
            dimension,
            limit,
            actual,
        })
}

fn remaining_tokens(
    dimension: BudgetDimension,
    limit: TokenCount,
    actual: TokenCount,
) -> Result<TokenCount, BudgetEvaluationError> {
    limit
        .checked_sub(actual)
        .ok_or(BudgetEvaluationError::TokenLimitExceeded {
            dimension,
            limit,
            actual,
        })
}

fn remaining_bytes(
    dimension: BudgetDimension,
    limit: ByteCount,
    actual: ByteCount,
) -> Result<ByteCount, BudgetEvaluationError> {
    limit
        .checked_sub(actual)
        .ok_or(BudgetEvaluationError::ByteLimitExceeded {
            dimension,
            limit,
            actual,
        })
}

fn remaining_costs(
    limits: &CostLimits,
    usage: &BudgetUsage,
) -> Result<CostLimits, BudgetEvaluationError> {
    for actual in usage.known_costs.iter().copied() {
        let limit =
            limits
                .get(actual.currency())
                .ok_or(BudgetEvaluationError::UnbudgetedCurrency {
                    currency: actual.currency(),
                })?;
        if actual.micro_units() > limit.micro_units() {
            return Err(BudgetEvaluationError::CostLimitExceeded { limit, actual });
        }
    }
    if usage.unpriced_cost_events != ExecutionCount::ZERO {
        return Err(BudgetEvaluationError::UnpricedCost {
            events: usage.unpriced_cost_events,
        });
    }

    let remaining = limits
        .iter()
        .map(|limit| {
            let actual = usage
                .known_costs
                .get(limit.currency())
                .map_or(0, Money::micro_units);
            Money::new(limit.currency(), limit.micro_units() - actual)
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(CostLimits(remaining))
}

/// Failure while evaluating a complete budget against usage.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum BudgetEvaluationError {
    /// The clock reached or passed the absolute deadline.
    #[error("budget deadline {deadline} was reached at {observed_at}")]
    DeadlineExceeded {
        /// Resolved absolute deadline.
        deadline: Timestamp,
        /// Clock value used for the deterministic decision.
        observed_at: Timestamp,
    },

    /// An execution-count ceiling was exceeded.
    #[error("budget dimension {dimension:?} used {actual}; limit is {limit}")]
    ExecutionLimitExceeded {
        /// Exceeded dimension.
        dimension: BudgetDimension,
        /// Resolved finite ceiling.
        limit: ExecutionCount,
        /// Observed usage.
        actual: ExecutionCount,
    },

    /// A token ceiling was exceeded.
    #[error("budget dimension {dimension:?} used {actual}; limit is {limit}")]
    TokenLimitExceeded {
        /// Exceeded dimension.
        dimension: BudgetDimension,
        /// Resolved finite ceiling.
        limit: TokenCount,
        /// Observed usage.
        actual: TokenCount,
    },

    /// A byte ceiling was exceeded.
    #[error("budget dimension {dimension:?} used {actual}; limit is {limit}")]
    ByteLimitExceeded {
        /// Exceeded dimension.
        dimension: BudgetDimension,
        /// Resolved finite ceiling.
        limit: ByteCount,
        /// Observed usage.
        actual: ByteCount,
    },

    /// Known charge exceeded its same-currency ceiling.
    #[error("known cost {actual:?} exceeds budget limit {limit:?}")]
    CostLimitExceeded {
        /// Resolved same-currency ceiling.
        limit: Money,
        /// Observed known charge.
        actual: Money,
    },

    /// A known charge used a currency absent from the resolved budget.
    #[error("known cost uses unbudgeted currency {currency}")]
    UnbudgetedCurrency {
        /// Currency not present in the resolved budget.
        currency: CurrencyCode,
    },

    /// At least one cost-bearing event could not be priced.
    #[error("remaining monetary budget is indeterminate after {events} unpriced events")]
    UnpricedCost {
        /// Number of provider or capability events without known cost.
        events: ExecutionCount,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    fn timestamp(value: &str) -> Timestamp {
        value.parse().unwrap()
    }

    fn currency(index: usize) -> CurrencyCode {
        let first = u8::try_from(index / (26 * 26)).unwrap();
        let second = u8::try_from((index / 26) % 26).unwrap();
        let third = u8::try_from(index % 26).unwrap();
        CurrencyCode::new([b'A' + first, b'A' + second, b'A' + third]).unwrap()
    }

    fn usd() -> CurrencyCode {
        "USD".parse().unwrap()
    }

    fn eur() -> CurrencyCode {
        "EUR".parse().unwrap()
    }

    fn full_limits(value: u64) -> BudgetLimits {
        let executions = ExecutionCount::new(value);
        let tokens = TokenCount::new(value);
        let bytes = ByteCount::new(value);
        BudgetLimits::empty()
            .with_deadline(timestamp("2030-01-01T00:00:00.000000Z"))
            .with_graph_depth(executions)
            .with_graph_steps(executions)
            .with_model_attempts(executions)
            .with_model_turns(executions)
            .with_input_tokens(tokens)
            .with_cached_input_tokens(tokens)
            .with_reasoning_tokens(tokens)
            .with_output_tokens(tokens)
            .with_tool_calls(executions)
            .with_write_calls(executions)
            .with_remote_agent_delegations(executions)
            .with_retries(executions)
            .with_concurrent_branches(executions)
            .with_fan_out(executions)
            .with_input_bytes(bytes)
            .with_output_bytes(bytes)
            .with_event_bytes(bytes)
            .with_checkpoint_bytes(bytes)
            .with_artifact_bytes(bytes)
            .with_costs(CostLimits::try_new([Money::new(usd(), value)]).unwrap())
    }

    fn resolved(value: u64) -> ResolvedBudget {
        ResolvedBudget::resolve(&[full_limits(value)]).unwrap()
    }

    #[test]
    fn cost_collections_are_sorted_unique_bounded_and_exact() {
        let limits = CostLimits::try_new([Money::new(usd(), 20), Money::new(eur(), 10)]).unwrap();
        assert_eq!(limits.len(), 2);
        assert!(!limits.is_empty());
        assert_eq!(limits.get(usd()), Some(Money::new(usd(), 20)));
        assert_eq!(limits.get("JPY".parse().unwrap()), None);
        assert_eq!(
            to_value(&limits).unwrap(),
            json!([
                {"currency": "EUR", "micro_units": "10"},
                {"currency": "USD", "micro_units": "20"}
            ])
        );
        assert_eq!(
            from_value::<CostLimits>(to_value(&limits).unwrap()).unwrap(),
            limits
        );

        assert!(CostLimits::try_new([]).unwrap().is_empty());
        assert_eq!(KnownCosts::try_new([]).unwrap(), KnownCosts::empty());
        assert_eq!(
            CostLimits::try_new([Money::new(usd(), 1), Money::new(usd(), 2)]),
            Err(CostCollectionError::DuplicateCurrency { currency: usd() })
        );
        assert!(
            from_value::<KnownCosts>(json!([
                {"currency": "USD", "micro_units": "1"},
                {"currency": "USD", "micro_units": "2"}
            ]))
            .is_err()
        );

        let too_many = (0..=MAX_COST_CURRENCIES)
            .map(|index| Money::new(currency(index), 1))
            .collect::<Vec<_>>();
        assert_eq!(
            CostLimits::try_new(too_many.clone()),
            Err(CostCollectionError::TooMany {
                max: MAX_COST_CURRENCIES,
                observed: MAX_COST_CURRENCIES + 1,
            })
        );
        assert!(from_value::<KnownCosts>(to_value(too_many).unwrap()).is_err());
    }

    #[test]
    fn cost_intersection_takes_per_currency_minimum_without_conversion() {
        let left = CostLimits::try_new([Money::new(usd(), 100), Money::new(eur(), 200)]).unwrap();
        let right = CostLimits::try_new([Money::new(usd(), 80)]).unwrap();
        let effective = left.most_restrictive(&right);

        assert_eq!(effective.get(usd()), Some(Money::new(usd(), 80)));
        assert_eq!(effective.get(eur()), None);
        assert_eq!(left.most_restrictive(&right), right.most_restrictive(&left));
    }

    #[test]
    fn resolution_requires_every_finite_dimension_and_is_order_independent() {
        assert_eq!(
            ResolvedBudget::resolve(&[]),
            Err(BudgetResolutionError::EmptyLayers)
        );
        assert_eq!(
            ResolvedBudget::resolve(&vec![BudgetLimits::empty(); MAX_BUDGET_LAYERS + 1]),
            Err(BudgetResolutionError::TooManyLayers {
                max: MAX_BUDGET_LAYERS,
                actual: MAX_BUDGET_LAYERS + 1,
            })
        );

        let mut missing = full_limits(100);
        missing.artifact_bytes = None;
        assert_eq!(
            ResolvedBudget::resolve(&[missing]),
            Err(BudgetResolutionError::Missing {
                dimension: BudgetDimension::ArtifactBytes,
            })
        );

        let system = full_limits(100);
        let tenant = BudgetLimits::empty()
            .with_deadline(timestamp("2029-01-01T00:00:00.000000Z"))
            .with_graph_steps(ExecutionCount::new(60))
            .with_input_tokens(TokenCount::new(50))
            .with_cached_input_tokens(TokenCount::new(90))
            .with_output_tokens(TokenCount::new(40))
            .with_reasoning_tokens(TokenCount::new(80))
            .with_tool_calls(ExecutionCount::new(20))
            .with_write_calls(ExecutionCount::new(30))
            .with_costs(CostLimits::try_new([Money::new(usd(), 70)]).unwrap());
        let forward = ResolvedBudget::resolve(&[system.clone(), tenant.clone()]).unwrap();
        let reverse = ResolvedBudget::resolve(&[tenant, system]).unwrap();

        assert_eq!(forward, reverse);
        assert_eq!(forward.deadline(), timestamp("2029-01-01T00:00:00.000000Z"));
        assert_eq!(forward.graph_steps(), ExecutionCount::new(60));
        assert_eq!(forward.input_tokens(), TokenCount::new(50));
        assert_eq!(forward.cached_input_tokens(), TokenCount::new(50));
        assert_eq!(forward.output_tokens(), TokenCount::new(40));
        assert_eq!(forward.reasoning_tokens(), TokenCount::new(40));
        assert_eq!(forward.tool_calls(), ExecutionCount::new(20));
        assert_eq!(forward.write_calls(), ExecutionCount::new(20));
        assert_eq!(forward.costs().get(usd()), Some(Money::new(usd(), 70)));
    }

    #[test]
    fn resolved_budget_wire_rejects_subset_confusion_and_unknown_fields() {
        let budget = resolved(100);
        let encoded = to_value(&budget).unwrap();
        assert_eq!(
            from_value::<ResolvedBudget>(encoded.clone()).unwrap(),
            budget
        );

        let mut cached_too_large = encoded.clone();
        cached_too_large["input_tokens"] = json!("10");
        cached_too_large["cached_input_tokens"] = json!("11");
        assert!(from_value::<ResolvedBudget>(cached_too_large).is_err());

        let mut reasoning_too_large = encoded.clone();
        reasoning_too_large["output_tokens"] = json!("10");
        reasoning_too_large["reasoning_tokens"] = json!("11");
        assert!(from_value::<ResolvedBudget>(reasoning_too_large).is_err());

        let mut writes_too_large = encoded.clone();
        writes_too_large["tool_calls"] = json!("10");
        writes_too_large["write_calls"] = json!("11");
        assert!(from_value::<ResolvedBudget>(writes_too_large).is_err());

        let mut unknown = encoded;
        unknown["unlimited"] = Value::Bool(true);
        assert!(from_value::<ResolvedBudget>(unknown).is_err());
    }

    #[test]
    fn usage_builder_enforces_normalized_subset_relationships() {
        assert_eq!(BudgetUsage::zero(), BudgetUsage::builder().build().unwrap());
        assert_eq!(
            BudgetUsage::builder()
                .input_tokens(TokenCount::new(1))
                .cached_input_tokens(TokenCount::new(2))
                .build(),
            Err(BudgetUsageError::SubsetExceedsInclusive {
                subset: BudgetDimension::CachedInputTokens,
                inclusive: BudgetDimension::InputTokens,
            })
        );
        assert_eq!(
            BudgetUsage::builder()
                .output_tokens(TokenCount::new(1))
                .reasoning_tokens(TokenCount::new(2))
                .build(),
            Err(BudgetUsageError::SubsetExceedsInclusive {
                subset: BudgetDimension::ReasoningTokens,
                inclusive: BudgetDimension::OutputTokens,
            })
        );
        assert_eq!(
            BudgetUsage::builder()
                .tool_calls(ExecutionCount::new(1))
                .write_calls(ExecutionCount::new(2))
                .build(),
            Err(BudgetUsageError::SubsetExceedsInclusive {
                subset: BudgetDimension::WriteCalls,
                inclusive: BudgetDimension::ToolCalls,
            })
        );
    }

    #[test]
    fn usage_accumulation_adds_totals_and_merges_high_water_marks() {
        let left = BudgetUsage::builder()
            .graph_depth(ExecutionCount::new(3))
            .graph_steps(ExecutionCount::new(5))
            .model_attempts(ExecutionCount::new(1))
            .model_turns(ExecutionCount::new(1))
            .input_tokens(TokenCount::new(20))
            .cached_input_tokens(TokenCount::new(5))
            .reasoning_tokens(TokenCount::new(2))
            .output_tokens(TokenCount::new(8))
            .tool_calls(ExecutionCount::new(2))
            .write_calls(ExecutionCount::new(1))
            .concurrent_branches(ExecutionCount::new(4))
            .fan_out(ExecutionCount::new(3))
            .known_costs(KnownCosts::try_new([Money::new(usd(), 10)]).unwrap())
            .build()
            .unwrap();
        let right = BudgetUsage::builder()
            .graph_depth(ExecutionCount::new(2))
            .graph_steps(ExecutionCount::new(7))
            .model_attempts(ExecutionCount::new(2))
            .model_turns(ExecutionCount::new(1))
            .input_tokens(TokenCount::new(30))
            .cached_input_tokens(TokenCount::new(10))
            .reasoning_tokens(TokenCount::new(3))
            .output_tokens(TokenCount::new(9))
            .tool_calls(ExecutionCount::new(3))
            .write_calls(ExecutionCount::new(1))
            .concurrent_branches(ExecutionCount::new(6))
            .fan_out(ExecutionCount::new(2))
            .known_costs(
                KnownCosts::try_new([Money::new(usd(), 15), Money::new(eur(), 4)]).unwrap(),
            )
            .unpriced_cost_events(ExecutionCount::new(1))
            .build()
            .unwrap();
        let total = left.checked_accumulate(&right).unwrap();

        assert_eq!(total.graph_depth(), ExecutionCount::new(3));
        assert_eq!(total.graph_steps(), ExecutionCount::new(12));
        assert_eq!(total.model_attempts(), ExecutionCount::new(3));
        assert_eq!(total.input_tokens(), TokenCount::new(50));
        assert_eq!(total.cached_input_tokens(), TokenCount::new(15));
        assert_eq!(total.reasoning_tokens(), TokenCount::new(5));
        assert_eq!(total.output_tokens(), TokenCount::new(17));
        assert_eq!(total.tool_calls(), ExecutionCount::new(5));
        assert_eq!(total.write_calls(), ExecutionCount::new(2));
        assert_eq!(total.concurrent_branches(), ExecutionCount::new(6));
        assert_eq!(total.fan_out(), ExecutionCount::new(3));
        assert_eq!(total.known_costs().get(usd()), Some(Money::new(usd(), 25)));
        assert_eq!(total.known_costs().get(eur()), Some(Money::new(eur(), 4)));
        assert_eq!(total.unpriced_cost_events(), ExecutionCount::new(1));

        let cost_overflow = BudgetUsage::builder()
            .known_costs(KnownCosts::try_new([Money::new(usd(), u64::MAX)]).unwrap())
            .build()
            .unwrap()
            .checked_accumulate(
                &BudgetUsage::builder()
                    .known_costs(KnownCosts::try_new([Money::new(usd(), 1)]).unwrap())
                    .build()
                    .unwrap(),
            );
        assert_eq!(
            cost_overflow,
            Err(BudgetUsageError::Costs(CostCollectionError::Overflow {
                currency: usd(),
            }))
        );

        let overflow = BudgetUsage::builder()
            .graph_steps(ExecutionCount::MAX)
            .build()
            .unwrap()
            .checked_accumulate(
                &BudgetUsage::builder()
                    .graph_steps(ExecutionCount::new(1))
                    .build()
                    .unwrap(),
            );
        assert_eq!(
            overflow,
            Err(BudgetUsageError::Overflow {
                dimension: BudgetDimension::GraphSteps,
            })
        );
    }

    #[test]
    fn usage_wire_is_closed_and_revalidates_normalized_totals() {
        let usage = BudgetUsage::builder()
            .input_tokens(TokenCount::new(10))
            .cached_input_tokens(TokenCount::new(4))
            .output_tokens(TokenCount::new(8))
            .reasoning_tokens(TokenCount::new(3))
            .tool_calls(ExecutionCount::new(2))
            .write_calls(ExecutionCount::new(1))
            .known_costs(KnownCosts::try_new([Money::new(usd(), 5)]).unwrap())
            .build()
            .unwrap();
        let encoded = to_value(&usage).unwrap();
        assert_eq!(from_value::<BudgetUsage>(encoded.clone()).unwrap(), usage);

        let mut invalid = encoded.clone();
        invalid["input_tokens"] = json!("1");
        assert!(from_value::<BudgetUsage>(invalid).is_err());

        let mut unknown = encoded;
        unknown["extra"] = Value::Null;
        assert!(from_value::<BudgetUsage>(unknown).is_err());
    }

    #[test]
    fn remaining_is_exact_and_unknown_cost_fails_closed() {
        let budget = resolved(100);
        let usage = BudgetUsage::builder()
            .graph_depth(ExecutionCount::new(4))
            .graph_steps(ExecutionCount::new(10))
            .model_attempts(ExecutionCount::new(2))
            .model_turns(ExecutionCount::new(1))
            .input_tokens(TokenCount::new(30))
            .cached_input_tokens(TokenCount::new(8))
            .reasoning_tokens(TokenCount::new(7))
            .output_tokens(TokenCount::new(20))
            .tool_calls(ExecutionCount::new(5))
            .write_calls(ExecutionCount::new(2))
            .input_bytes(ByteCount::new(40))
            .known_costs(KnownCosts::try_new([Money::new(usd(), 25)]).unwrap())
            .build()
            .unwrap();
        let remaining = budget
            .remaining(&usage, timestamp("2029-01-01T00:00:00.000000Z"))
            .unwrap();

        assert_eq!(remaining.graph_depth(), ExecutionCount::new(96));
        assert_eq!(remaining.graph_steps(), ExecutionCount::new(90));
        assert_eq!(remaining.input_tokens(), TokenCount::new(70));
        assert_eq!(remaining.cached_input_tokens(), TokenCount::new(92));
        assert_eq!(remaining.reasoning_tokens(), TokenCount::new(93));
        assert_eq!(remaining.output_tokens(), TokenCount::new(80));
        assert_eq!(remaining.tool_calls(), ExecutionCount::new(95));
        assert_eq!(remaining.write_calls(), ExecutionCount::new(98));
        assert_eq!(remaining.input_bytes(), ByteCount::new(60));
        assert_eq!(remaining.costs().get(usd()), Some(Money::new(usd(), 75)));

        let unpriced = BudgetUsage::builder()
            .unpriced_cost_events(ExecutionCount::new(1))
            .build()
            .unwrap();
        assert_eq!(
            budget.remaining(&unpriced, timestamp("2029-01-01T00:00:00.000000Z")),
            Err(BudgetEvaluationError::UnpricedCost {
                events: ExecutionCount::new(1),
            })
        );
    }

    #[test]
    fn evaluation_reports_deadline_scalar_and_currency_failures() {
        let budget = resolved(10);
        assert_eq!(
            budget.remaining(
                &BudgetUsage::zero(),
                timestamp("2030-01-01T00:00:00.000000Z")
            ),
            Err(BudgetEvaluationError::DeadlineExceeded {
                deadline: timestamp("2030-01-01T00:00:00.000000Z"),
                observed_at: timestamp("2030-01-01T00:00:00.000000Z"),
            })
        );

        let steps = BudgetUsage::builder()
            .graph_steps(ExecutionCount::new(11))
            .build()
            .unwrap();
        assert_eq!(
            budget.remaining(&steps, timestamp("2029-01-01T00:00:00.000000Z")),
            Err(BudgetEvaluationError::ExecutionLimitExceeded {
                dimension: BudgetDimension::GraphSteps,
                limit: ExecutionCount::new(10),
                actual: ExecutionCount::new(11),
            })
        );

        let tokens = BudgetUsage::builder()
            .input_tokens(TokenCount::new(11))
            .build()
            .unwrap();
        assert!(matches!(
            budget.remaining(&tokens, timestamp("2029-01-01T00:00:00.000000Z")),
            Err(BudgetEvaluationError::TokenLimitExceeded {
                dimension: BudgetDimension::InputTokens,
                ..
            })
        ));

        let bytes = BudgetUsage::builder()
            .input_bytes(ByteCount::new(11))
            .build()
            .unwrap();
        assert!(matches!(
            budget.remaining(&bytes, timestamp("2029-01-01T00:00:00.000000Z")),
            Err(BudgetEvaluationError::ByteLimitExceeded {
                dimension: BudgetDimension::InputBytes,
                ..
            })
        ));

        let unbudgeted = BudgetUsage::builder()
            .known_costs(KnownCosts::try_new([Money::new(eur(), 1)]).unwrap())
            .build()
            .unwrap();
        assert_eq!(
            budget.remaining(&unbudgeted, timestamp("2029-01-01T00:00:00.000000Z")),
            Err(BudgetEvaluationError::UnbudgetedCurrency { currency: eur() })
        );

        let deny_all_costs =
            ResolvedBudget::resolve(
                &[full_limits(10).with_costs(CostLimits::try_new([]).unwrap())],
            )
            .unwrap();
        assert!(
            deny_all_costs
                .remaining(
                    &BudgetUsage::zero(),
                    timestamp("2029-01-01T00:00:00.000000Z")
                )
                .is_ok()
        );
        let priced = BudgetUsage::builder()
            .known_costs(KnownCosts::try_new([Money::new(usd(), 1)]).unwrap())
            .build()
            .unwrap();
        assert_eq!(
            deny_all_costs.remaining(&priced, timestamp("2029-01-01T00:00:00.000000Z")),
            Err(BudgetEvaluationError::UnbudgetedCurrency { currency: usd() })
        );

        let overspent = BudgetUsage::builder()
            .known_costs(KnownCosts::try_new([Money::new(usd(), 11)]).unwrap())
            .build()
            .unwrap();
        assert_eq!(
            budget.remaining(&overspent, timestamp("2029-01-01T00:00:00.000000Z")),
            Err(BudgetEvaluationError::CostLimitExceeded {
                limit: Money::new(usd(), 10),
                actual: Money::new(usd(), 11),
            })
        );
    }

    #[test]
    fn budget_schemas_close_objects_and_publish_collection_bounds() {
        let limits = to_value(schemars::schema_for!(BudgetLimits)).unwrap();
        let resolved = to_value(schemars::schema_for!(ResolvedBudget)).unwrap();
        let usage = to_value(schemars::schema_for!(BudgetUsage)).unwrap();
        let remaining = to_value(schemars::schema_for!(BudgetRemaining)).unwrap();
        let costs = to_value(schemars::schema_for!(CostLimits)).unwrap();
        let known = to_value(schemars::schema_for!(KnownCosts)).unwrap();

        assert_eq!(limits["additionalProperties"], false);
        assert_eq!(resolved["additionalProperties"], false);
        assert_eq!(usage["additionalProperties"], false);
        assert_eq!(remaining["additionalProperties"], false);
        assert!(limits["required"].as_array().is_none_or(Vec::is_empty));
        assert_eq!(resolved["required"].as_array().unwrap().len(), 21);
        assert_eq!(usage["required"].as_array().unwrap().len(), 21);
        assert_eq!(costs["minItems"], 0);
        assert_eq!(costs["maxItems"], MAX_COST_CURRENCIES);
        assert_eq!(costs["uniqueItems"], true);
        assert_eq!(known["maxItems"], MAX_COST_CURRENCIES);
        assert_eq!(known["uniqueItems"], true);
    }

    proptest! {
        #[test]
        fn scalar_resolution_is_commutative_idempotent_and_never_widens(
            left in any::<u64>(),
            right in any::<u64>(),
        ) {
            let left = BudgetLimits::empty().with_graph_steps(ExecutionCount::new(left));
            let right = BudgetLimits::empty().with_graph_steps(ExecutionCount::new(right));
            let forward = left.most_restrictive(&right);
            let reverse = right.most_restrictive(&left);
            prop_assert_eq!(&forward, &reverse);
            prop_assert_eq!(
                forward.graph_steps(),
                Some(ExecutionCount::new(left.graph_steps().unwrap().get().min(right.graph_steps().unwrap().get())))
            );
            prop_assert_eq!(left.most_restrictive(&left), left);
        }

        #[test]
        fn usage_accumulation_matches_checked_counts_and_high_water(
            left in any::<u32>(),
            right in any::<u32>(),
        ) {
            let left_usage = BudgetUsage::builder()
                .graph_depth(ExecutionCount::new(u64::from(left)))
                .graph_steps(ExecutionCount::new(u64::from(left)))
                .build()
                .unwrap();
            let right_usage = BudgetUsage::builder()
                .graph_depth(ExecutionCount::new(u64::from(right)))
                .graph_steps(ExecutionCount::new(u64::from(right)))
                .build()
                .unwrap();
            let total = left_usage.checked_accumulate(&right_usage).unwrap();
            prop_assert_eq!(total.graph_depth().get(), u64::from(left.max(right)));
            prop_assert_eq!(total.graph_steps().get(), u64::from(left) + u64::from(right));
        }

        #[test]
        fn remaining_count_is_exact_for_every_in_budget_value(
            limit in any::<u32>(),
            usage in any::<u32>(),
        ) {
            let limit = u64::from(limit);
            let usage = u64::from(usage).min(limit);
            let budget = resolved(limit);
            let usage_value = BudgetUsage::builder()
                .graph_steps(ExecutionCount::new(usage))
                .build()
                .unwrap();
            let remaining = budget
                .remaining(&usage_value, timestamp("2029-01-01T00:00:00.000000Z"))
                .unwrap();
            prop_assert_eq!(remaining.graph_steps().get(), limit - usage);
        }
    }
}
