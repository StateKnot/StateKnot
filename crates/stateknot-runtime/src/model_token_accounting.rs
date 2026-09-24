// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Digest-pinned, offline token pricing for one model binding.

use serde::{Deserialize, Serialize};
use stateknot_core::{
    CapabilityIdentity, CurrencyCode, Digest, KnownCosts, ModelInvocation, ModelInvocationState,
    ModelProviderModelId, ModelUsage, Money, ToolInvocation,
};

use crate::{AgentInvocationAccounting, AgentInvocationAccountingReference, AgentInvocationCharge};

const TOKENS_PER_MILLION: u128 = 1_000_000;

/// Operator-supplied price snapshot, in currency micro-units per million tokens.
///
/// Input includes cached tokens and output includes reasoning tokens. A provider
/// with separately priced output categories needs a different accounting
/// implementation. Rates must match the exact provider contract and remain
/// installed while runs using this graph version can be recovered.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelTokenRateCard {
    /// Currency of every rate.
    pub currency: CurrencyCode,
    /// Rate for input tokens not reported as cached.
    pub input_per_million: u64,
    /// Rate for the cached-input subset.
    pub cached_input_per_million: u64,
    /// Rate for all generated output tokens, including reasoning.
    pub output_per_million: u64,
}

/// Deterministic pricing of one exact model binding; tools fail closed as unpriced.
pub struct ModelTokenAccounting {
    reference: AgentInvocationAccountingReference,
    model: CapabilityIdentity,
    provider_model_id: ModelProviderModelId,
    rates: ModelTokenRateCard,
}

impl ModelTokenAccounting {
    /// Pins the exact model binding and rate snapshot in the graph contract.
    ///
    /// The caller must validate that this rate card is the provider's actual
    /// billing contract. No live price lookup occurs during replay.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the immutable contract cannot be encoded.
    pub fn new(
        identity: CapabilityIdentity,
        model: CapabilityIdentity,
        provider_model_id: ModelProviderModelId,
        rates: ModelTokenRateCard,
    ) -> Result<Self, serde_json::Error> {
        let contract = serde_json::to_vec(&(
            "stateknot.model-token-accounting.v1",
            &model,
            &provider_model_id,
            rates,
        ))?;
        Ok(Self {
            reference: AgentInvocationAccountingReference::new(identity, Digest::sha256(contract)),
            model,
            provider_model_id,
            rates,
        })
    }

    /// Returns the frozen rate card for deployment verification.
    #[must_use]
    pub const fn rates(&self) -> ModelTokenRateCard {
        self.rates
    }

    fn price(&self, usage: &ModelUsage) -> AgentInvocationCharge {
        let input = u128::from(usage.input_tokens().get());
        let cached = match usage.cached_input_tokens() {
            Some(tokens) => u128::from(tokens.get()),
            None if input == 0
                || self.rates.input_per_million == self.rates.cached_input_per_million =>
            {
                0
            }
            None => return AgentInvocationCharge::Unpriced,
        };
        let output = u128::from(usage.output_tokens().get());
        let charge = (input - cached)
            .checked_mul(u128::from(self.rates.input_per_million))
            .and_then(|sum| {
                cached
                    .checked_mul(u128::from(self.rates.cached_input_per_million))
                    .and_then(|part| sum.checked_add(part))
            })
            .and_then(|sum| {
                output
                    .checked_mul(u128::from(self.rates.output_per_million))
                    .and_then(|part| sum.checked_add(part))
            })
            .and_then(|sum| sum.checked_add(TOKENS_PER_MILLION - 1))
            .map(|sum| sum / TOKENS_PER_MILLION)
            .and_then(|sum| u64::try_from(sum).ok());
        let Some(micro_units) = charge else {
            return AgentInvocationCharge::Unpriced;
        };
        match KnownCosts::try_new([Money::new(self.rates.currency, micro_units)]) {
            Ok(costs) => AgentInvocationCharge::Known(costs),
            Err(_) => AgentInvocationCharge::Unpriced,
        }
    }
}

impl AgentInvocationAccounting for ModelTokenAccounting {
    fn reference(&self) -> &AgentInvocationAccountingReference {
        &self.reference
    }

    fn model_charge(&self, invocation: &ModelInvocation) -> AgentInvocationCharge {
        if invocation.intent().descriptor().metadata().identity() != &self.model {
            return AgentInvocationCharge::Unpriced;
        }
        match invocation.state() {
            ModelInvocationState::Committed { response }
                if response.provenance().provider_model_id() == Some(&self.provider_model_id) =>
            {
                self.price(response.usage())
            }
            ModelInvocationState::Failed { error }
                if error.provenance().provider_model_id() == Some(&self.provider_model_id) =>
            {
                error
                    .usage()
                    .map_or(AgentInvocationCharge::Unpriced, |usage| self.price(usage))
            }
            ModelInvocationState::Committed { .. }
            | ModelInvocationState::Failed { .. }
            | ModelInvocationState::Prepared
            | ModelInvocationState::Executing { .. } => AgentInvocationCharge::Unpriced,
        }
    }

    fn tool_charge(&self, _: &ToolInvocation) -> AgentInvocationCharge {
        AgentInvocationCharge::Unpriced
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateknot_core::TokenCount;

    fn accounting() -> ModelTokenAccounting {
        let owner = "https://issuer.example.com".parse().unwrap();
        let principal =
            stateknot_core::PrincipalIdentity::new(owner, "accounting".parse().unwrap());
        let capability = |name| {
            CapabilityIdentity::new(
                principal.clone(),
                stateknot_core::CapabilityReference::new(
                    stateknot_core::CapabilityName::new(name).unwrap(),
                    stateknot_core::Version::new(1, 0, 0),
                ),
            )
        };
        ModelTokenAccounting::new(
            capability("accounting.token"),
            capability("models.primary"),
            ModelProviderModelId::new("provider-model-v1").unwrap(),
            ModelTokenRateCard {
                currency: "USD".parse().unwrap(),
                input_per_million: 2_000_000,
                cached_input_per_million: 500_000,
                output_per_million: 8_000_000,
            },
        )
        .unwrap()
    }

    #[test]
    fn prices_reported_categories_with_one_final_rounding() {
        let usage = ModelUsage::new(
            TokenCount::new(10),
            Some(TokenCount::new(2)),
            TokenCount::new(3),
            Some(TokenCount::new(1)),
        )
        .unwrap();
        let AgentInvocationCharge::Known(costs) = accounting().price(&usage) else {
            panic!("complete provider usage should be priced")
        };
        assert_eq!(costs.get("USD".parse().unwrap()).unwrap().micro_units(), 41);
    }

    #[test]
    fn missing_discount_breakdown_fails_closed() {
        let usage = ModelUsage::new(TokenCount::new(10), None, TokenCount::new(3), None).unwrap();
        assert!(matches!(
            accounting().price(&usage),
            AgentInvocationCharge::Unpriced
        ));
    }

    #[test]
    fn equal_input_rates_do_not_require_a_cache_breakdown() {
        let first = accounting();
        let mut rates = first.rates();
        rates.cached_input_per_million = rates.input_per_million;
        let flat = ModelTokenAccounting::new(
            first.reference.identity().clone(),
            first.model.clone(),
            first.provider_model_id.clone(),
            rates,
        )
        .unwrap();
        let usage = ModelUsage::new(TokenCount::new(10), None, TokenCount::ZERO, None).unwrap();
        let AgentInvocationCharge::Known(costs) = flat.price(&usage) else {
            panic!("equal input rates allow an absent cache breakdown")
        };
        assert_eq!(costs.get("USD".parse().unwrap()).unwrap().micro_units(), 20);
    }

    #[test]
    fn overflowing_tariff_arithmetic_fails_closed() {
        let first = accounting();
        let rates = ModelTokenRateCard {
            input_per_million: u64::MAX,
            cached_input_per_million: u64::MAX,
            output_per_million: u64::MAX,
            ..first.rates()
        };
        let extreme = ModelTokenAccounting::new(
            first.reference.identity().clone(),
            first.model.clone(),
            first.provider_model_id.clone(),
            rates,
        )
        .unwrap();
        let usage = ModelUsage::new(
            TokenCount::new(u64::MAX - 1),
            None,
            TokenCount::new(1),
            None,
        )
        .unwrap();
        assert!(matches!(
            extreme.price(&usage),
            AgentInvocationCharge::Unpriced
        ));
    }

    #[test]
    fn pricing_revision_changes_contract_digest() {
        let first = accounting();
        let mut rates = first.rates();
        rates.output_per_million += 1;
        let second = ModelTokenAccounting::new(
            first.reference.identity().clone(),
            first.model.clone(),
            first.provider_model_id.clone(),
            rates,
        )
        .unwrap();
        assert_ne!(
            first.reference.definition_digest(),
            second.reference.definition_digest()
        );
        let other_model = ModelTokenAccounting::new(
            first.reference.identity().clone(),
            first.model.clone(),
            ModelProviderModelId::new("provider-model-v2").unwrap(),
            first.rates(),
        )
        .unwrap();
        assert_ne!(
            first.reference.definition_digest(),
            other_model.reference.definition_digest()
        );
    }
}
