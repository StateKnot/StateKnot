// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Replica-safe deterministic weighted fairness above tenant-isolated workers.

use std::{sync::Arc, time::Duration};

use serde::Serialize;
use stateknot_core::{
    BoxFuture, CancellationSignal, Digest, SchedulerReservationId, SchedulerShardId, TenantId,
};
use stateknot_store_postgres::{
    PostgresStore, SchedulerFairnessPolicyRegistration, SchedulerFairnessReservation, StoreError,
};
use thiserror::Error;

use crate::{
    DurableGraphDriverOptions, DurableGraphLifecycleOptions, DurableTenantScheduler,
    DurableTenantSchedulerBuildError, DurableTenantSchedulerOptions, ExecutableGraphRegistry,
    GraphLifecycleEvidenceProvider, TenantSchedulerError, TenantSchedulerTick,
};

const ALGORITHM: &str = "smooth_weighted_round_robin_v1";
const MAX_RESERVATION_RETRY_DELAY: Duration = Duration::from_secs(1);

/// One tenant's positive service weight inside a fairness shard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantFairnessWeight {
    tenant_id: TenantId,
    weight: u16,
}

impl TenantFairnessWeight {
    /// Largest individual weight. The aggregate cycle has a separate bound.
    pub const MAX_WEIGHT: u16 = 1024;

    /// Constructs one positive bounded tenant weight.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above [`Self::MAX_WEIGHT`].
    pub fn new(tenant_id: TenantId, weight: u16) -> Result<Self, TenantFairnessWeightError> {
        if weight == 0 || weight > Self::MAX_WEIGHT {
            return Err(TenantFairnessWeightError::InvalidWeight);
        }
        Ok(Self { tenant_id, weight })
    }

    /// Returns the exact tenant queue boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns this tenant's positive cycle weight.
    #[must_use]
    pub const fn weight(&self) -> u16 {
        self.weight
    }
}

/// Invalid individual tenant fairness weight.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum TenantFairnessWeightError {
    /// Weight was zero or exceeded the hard per-tenant bound.
    #[error("tenant fairness weight is invalid")]
    InvalidWeight,
}

/// Exact reservation-count starvation bound for one continuously eligible tenant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TenantStarvationBound {
    maximum_reservations_until_selection: u16,
}

impl TenantStarvationBound {
    /// Returns the maximum global slot reservations between selections.
    ///
    /// This is a scheduling-order bound, not a wall-clock latency promise. A
    /// deployment must still keep worker capacity available and bound each
    /// tenant scan/claim operation to translate it into elapsed time.
    #[must_use]
    pub const fn maximum_reservations_until_selection(self) -> u16 {
        self.maximum_reservations_until_selection
    }
}

/// Immutable deterministic weighted schedule shared by every shard replica.
///
/// Construction sorts tenants by exact identifier, executes one complete
/// smooth weighted-round-robin cycle, verifies exact weight counts, and derives
/// each tenant's largest circular slot gap from the resulting cycle. The
/// canonical policy bytes include the algorithm version and ordered weights;
/// `PostgreSQL` permanently binds those bytes to the shard identity.
#[derive(Clone, Debug)]
pub struct WeightedFairnessPolicy {
    shard_id: SchedulerShardId,
    tenants: Box<[TenantFairnessWeight]>,
    cycle: Box<[usize]>,
    starvation_bounds: Box<[TenantStarvationBound]>,
    canonical_bytes: Box<[u8]>,
    registration: SchedulerFairnessPolicyRegistration,
}

impl WeightedFairnessPolicy {
    /// Maximum tenant queues in one shard.
    pub const MAX_TENANTS: usize = 1024;
    /// Maximum weighted slots in one deterministic cycle.
    pub const MAX_CYCLE_LENGTH: u16 = SchedulerFairnessPolicyRegistration::MAX_CYCLE_LENGTH;

    /// Compiles an immutable replica-independent weighted schedule.
    ///
    /// # Errors
    ///
    /// Rejects empty/oversized tenant sets, duplicate tenant IDs, aggregate
    /// weight overflow, canonical encoding failure, or invalid store binding.
    pub fn new<I>(
        shard_id: SchedulerShardId,
        tenants: I,
    ) -> Result<Self, WeightedFairnessPolicyError>
    where
        I: IntoIterator<Item = TenantFairnessWeight>,
    {
        let mut tenants = tenants.into_iter().collect::<Vec<_>>();
        if tenants.is_empty() {
            return Err(WeightedFairnessPolicyError::Empty);
        }
        if tenants.len() > Self::MAX_TENANTS {
            return Err(WeightedFairnessPolicyError::TooManyTenants);
        }
        tenants.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
        if tenants
            .windows(2)
            .any(|pair| pair[0].tenant_id == pair[1].tenant_id)
        {
            return Err(WeightedFairnessPolicyError::DuplicateTenant);
        }

        let cycle_length = tenants.iter().try_fold(0_u16, |total, tenant| {
            total
                .checked_add(tenant.weight)
                .filter(|value| *value <= Self::MAX_CYCLE_LENGTH)
                .ok_or(WeightedFairnessPolicyError::CycleTooLarge)
        })?;
        let cycle = compile_smooth_cycle(&tenants, cycle_length);
        verify_cycle_counts(&tenants, &cycle)?;
        let starvation_bounds = derive_starvation_bounds(tenants.len(), &cycle)?;
        let wire = FairnessPolicyWire {
            algorithm: ALGORITHM,
            tenants: tenants
                .iter()
                .map(|tenant| FairnessTenantWire {
                    tenant_id: tenant.tenant_id.as_str(),
                    weight: tenant.weight,
                })
                .collect(),
        };
        let canonical_bytes = serde_json_canonicalizer::to_vec(&wire)
            .map_err(|_| WeightedFairnessPolicyError::CanonicalEncoding)?;
        let registration = SchedulerFairnessPolicyRegistration::new(
            shard_id.clone(),
            canonical_bytes.clone(),
            cycle_length,
        )
        .map_err(|_| WeightedFairnessPolicyError::StoreBinding)?;
        Ok(Self {
            shard_id,
            tenants: tenants.into_boxed_slice(),
            cycle: cycle.into_boxed_slice(),
            starvation_bounds: starvation_bounds.into_boxed_slice(),
            canonical_bytes: canonical_bytes.into_boxed_slice(),
            registration,
        })
    }

    /// Returns the immutable distributed shard identity.
    #[must_use]
    pub const fn shard_id(&self) -> &SchedulerShardId {
        &self.shard_id
    }

    /// Returns the domain-separated durable policy checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.registration.policy_digest()
    }

    /// Returns the exact number of weighted slots per complete cycle.
    #[must_use]
    pub fn cycle_length(&self) -> u16 {
        u16::try_from(self.cycle.len()).expect("policy construction bounds cycle length")
    }

    /// Returns the canonical exact tenant/weight snapshot.
    #[must_use]
    pub const fn tenants(&self) -> &[TenantFairnessWeight] {
        &self.tenants
    }

    /// Returns the canonical policy bytes registered durably.
    #[must_use]
    pub const fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Returns the exact continuously-eligible selection bound for a tenant.
    #[must_use]
    pub fn starvation_bound(&self, tenant_id: &TenantId) -> Option<TenantStarvationBound> {
        self.tenants
            .binary_search_by(|entry| entry.tenant_id.cmp(tenant_id))
            .ok()
            .map(|index| self.starvation_bounds[index])
    }

    /// Resolves one reserved cycle slot to its tenant queue.
    #[must_use]
    pub fn tenant_for_slot(&self, slot: u16) -> Option<&TenantId> {
        self.cycle
            .get(usize::from(slot))
            .map(|index| &self.tenants[*index].tenant_id)
    }

    fn registration(&self) -> SchedulerFairnessPolicyRegistration {
        self.registration.clone()
    }
}

#[derive(Serialize)]
struct FairnessPolicyWire<'a> {
    algorithm: &'static str,
    tenants: Vec<FairnessTenantWire<'a>>,
}

#[derive(Serialize)]
struct FairnessTenantWire<'a> {
    tenant_id: &'a str,
    weight: u16,
}

/// Invalid immutable weighted fairness policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum WeightedFairnessPolicyError {
    /// No tenant queues were configured.
    #[error("weighted fairness policy must contain at least one tenant")]
    Empty,
    /// The hard tenant count was exceeded.
    #[error("weighted fairness policy contains too many tenants")]
    TooManyTenants,
    /// The same tenant boundary appeared more than once.
    #[error("weighted fairness policy contains a duplicate tenant")]
    DuplicateTenant,
    /// Aggregate weights exceeded the bounded cycle.
    #[error("weighted fairness policy cycle is too large")]
    CycleTooLarge,
    /// The compiled cycle did not preserve exact configured shares.
    #[error("weighted fairness cycle failed internal count verification")]
    InvalidCompiledCycle,
    /// The deterministic policy could not be encoded canonically.
    #[error("weighted fairness policy canonical encoding failed")]
    CanonicalEncoding,
    /// The compiled policy could not satisfy the durability-provider contract.
    #[error("weighted fairness policy store binding failed")]
    StoreBinding,
}

fn compile_smooth_cycle(tenants: &[TenantFairnessWeight], cycle_length: u16) -> Vec<usize> {
    let total = i64::from(cycle_length);
    let mut current = vec![0_i64; tenants.len()];
    let mut cycle = Vec::with_capacity(usize::from(cycle_length));
    for _ in 0..cycle_length {
        for (score, tenant) in current.iter_mut().zip(tenants) {
            *score += i64::from(tenant.weight);
        }
        let selected = current
            .iter()
            .enumerate()
            .max_by(|(left_index, left), (right_index, right)| {
                left.cmp(right).then_with(|| right_index.cmp(left_index))
            })
            .map(|(index, _)| index)
            .expect("nonempty policy has one smooth weighted candidate");
        current[selected] -= total;
        cycle.push(selected);
    }
    cycle
}

fn verify_cycle_counts(
    tenants: &[TenantFairnessWeight],
    cycle: &[usize],
) -> Result<(), WeightedFairnessPolicyError> {
    let mut counts = vec![0_u16; tenants.len()];
    for index in cycle {
        let Some(count) = counts.get_mut(*index) else {
            return Err(WeightedFairnessPolicyError::InvalidCompiledCycle);
        };
        *count = count
            .checked_add(1)
            .ok_or(WeightedFairnessPolicyError::InvalidCompiledCycle)?;
    }
    if counts
        .iter()
        .zip(tenants)
        .any(|(actual, tenant)| *actual != tenant.weight)
    {
        return Err(WeightedFairnessPolicyError::InvalidCompiledCycle);
    }
    Ok(())
}

fn derive_starvation_bounds(
    tenant_count: usize,
    cycle: &[usize],
) -> Result<Vec<TenantStarvationBound>, WeightedFairnessPolicyError> {
    let cycle_length = cycle.len();
    let mut positions = vec![Vec::new(); tenant_count];
    for (position, index) in cycle.iter().copied().enumerate() {
        positions
            .get_mut(index)
            .ok_or(WeightedFairnessPolicyError::InvalidCompiledCycle)?
            .push(position);
    }
    positions
        .into_iter()
        .map(|tenant_positions| {
            if tenant_positions.is_empty() {
                return Err(WeightedFairnessPolicyError::InvalidCompiledCycle);
            }
            let maximum = tenant_positions
                .iter()
                .copied()
                .zip(
                    tenant_positions
                        .iter()
                        .copied()
                        .skip(1)
                        .chain(tenant_positions.first().map(|first| first + cycle_length)),
                )
                .map(|(current, next)| next - current)
                .max()
                .ok_or(WeightedFairnessPolicyError::InvalidCompiledCycle)?;
            Ok(TenantStarvationBound {
                maximum_reservations_until_selection: u16::try_from(maximum)
                    .map_err(|_| WeightedFairnessPolicyError::InvalidCompiledCycle)?,
            })
        })
        .collect()
}

/// Retry policy for one durable global slot reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableFairSchedulerOptions {
    maximum_reservation_attempts: u8,
    reservation_retry_initial_delay: Duration,
}

impl DurableFairSchedulerOptions {
    /// Absolute number of identical reservation attempts.
    pub const HARD_MAXIMUM_RESERVATION_ATTEMPTS: u8 = 10;

    /// Constructs a bounded lost-acknowledgement retry policy.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive attempts or a zero/greater-than-one-second delay.
    pub fn new(
        maximum_reservation_attempts: u8,
        reservation_retry_initial_delay: Duration,
    ) -> Result<Self, DurableFairSchedulerOptionsError> {
        if maximum_reservation_attempts == 0
            || maximum_reservation_attempts > Self::HARD_MAXIMUM_RESERVATION_ATTEMPTS
        {
            return Err(DurableFairSchedulerOptionsError::InvalidReservationAttempts);
        }
        if reservation_retry_initial_delay.is_zero()
            || reservation_retry_initial_delay > MAX_RESERVATION_RETRY_DELAY
        {
            return Err(DurableFairSchedulerOptionsError::InvalidReservationRetryDelay);
        }
        Ok(Self {
            maximum_reservation_attempts,
            reservation_retry_initial_delay,
        })
    }

    /// Returns the maximum identical transaction attempts.
    #[must_use]
    pub const fn maximum_reservation_attempts(self) -> u8 {
        self.maximum_reservation_attempts
    }

    /// Returns the first exponential retry delay.
    #[must_use]
    pub const fn reservation_retry_initial_delay(self) -> Duration {
        self.reservation_retry_initial_delay
    }
}

impl Default for DurableFairSchedulerOptions {
    fn default() -> Self {
        Self {
            maximum_reservation_attempts: 3,
            reservation_retry_initial_delay: Duration::from_millis(25),
        }
    }
}

/// Invalid distributed fairness retry policy.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableFairSchedulerOptionsError {
    /// Reservation attempts were zero or above the hard ceiling.
    #[error("fair scheduler reservation attempt count is invalid")]
    InvalidReservationAttempts,
    /// Initial retry delay was zero or above one second.
    #[error("fair scheduler reservation retry delay is invalid")]
    InvalidReservationRetryDelay,
}

/// Complete result of one globally fair scheduler tick.
#[derive(Debug)]
pub struct FairSchedulerTick {
    reservation: SchedulerFairnessReservation,
    tenant_id: TenantId,
    starvation_bound: TenantStarvationBound,
    reservation_retries: u8,
    tenant_tick: TenantSchedulerTick,
}

impl FairSchedulerTick {
    /// Returns the globally ordered durable slot reservation.
    #[must_use]
    pub const fn reservation(&self) -> &SchedulerFairnessReservation {
        &self.reservation
    }

    /// Returns the selected tenant queue.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the configured continuously-eligible scheduling bound.
    #[must_use]
    pub const fn starvation_bound(&self) -> TenantStarvationBound {
        self.starvation_bound
    }

    /// Returns transient database retries that reused the reservation ID.
    #[must_use]
    pub const fn reservation_retries(&self) -> u8 {
        self.reservation_retries
    }

    /// Returns the selected tenant worker's bounded outcome.
    #[must_use]
    pub const fn tenant_tick(&self) -> &TenantSchedulerTick {
        &self.tenant_tick
    }

    /// Consumes this value into durable selection and tenant execution parts.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        SchedulerFairnessReservation,
        TenantId,
        TenantStarvationBound,
        u8,
        TenantSchedulerTick,
    ) {
        (
            self.reservation,
            self.tenant_id,
            self.starvation_bound,
            self.reservation_retries,
            self.tenant_tick,
        )
    }
}

/// Distributed weighted-fair scheduler layered over tenant-isolated workers.
///
/// Every replica reserves from the same `PostgreSQL` cursor before inspecting a
/// tenant queue. Therefore aggregate selection order, exact per-cycle shares,
/// and reservation-count starvation bounds survive process restarts and
/// horizontal scaling. Tenant queue scans and claims remain strictly scoped to
/// the selected tenant.
#[derive(Clone)]
pub struct DurableFairScheduler {
    store: PostgresStore,
    tenant_scheduler: DurableTenantScheduler,
    policy: Arc<WeightedFairnessPolicy>,
    options: DurableFairSchedulerOptions,
}

impl DurableFairScheduler {
    /// Builds the tenant worker and registers the immutable global policy.
    ///
    /// # Errors
    ///
    /// Returns a local tenant-scheduler binding failure, immutable policy
    /// conflict, corruption, or database error. No run work is claimed before
    /// this constructor completes.
    #[allow(clippy::too_many_arguments)]
    pub async fn register(
        store: PostgresStore,
        registry: ExecutableGraphRegistry,
        evidence: Arc<dyn GraphLifecycleEvidenceProvider>,
        driver_options: DurableGraphDriverOptions,
        lifecycle_options: DurableGraphLifecycleOptions,
        tenant_options: DurableTenantSchedulerOptions,
        policy: WeightedFairnessPolicy,
        options: DurableFairSchedulerOptions,
    ) -> Result<Self, DurableFairSchedulerBuildError> {
        let tenant_scheduler = DurableTenantScheduler::new(
            store.clone(),
            registry,
            evidence,
            driver_options,
            lifecycle_options,
            tenant_options,
        )?;
        let registration = policy.registration();
        let outcome = store
            .register_scheduler_fairness_policy(registration.clone())
            .await?;
        if outcome.policy().registration() != &registration {
            return Err(DurableFairSchedulerBuildError::PolicyProjectionMismatch);
        }
        Ok(Self {
            store,
            tenant_scheduler,
            policy: Arc::new(policy),
            options,
        })
    }

    /// Returns the immutable weighted policy snapshot.
    #[must_use]
    pub fn policy(&self) -> &WeightedFairnessPolicy {
        &self.policy
    }

    /// Returns the bounded durable reservation retry policy.
    #[must_use]
    pub const fn options(&self) -> DurableFairSchedulerOptions {
        self.options
    }

    /// Reserves one global slot, selects its tenant, and executes one tenant tick.
    ///
    /// A stable reservation ID is allocated once and retained across transient
    /// database errors. Cancellation observed before a durable reservation
    /// returns an error-free cancelled tenant tick only after selection; callers
    /// that need to stop before any new selection should cease calling `tick`.
    pub fn tick(
        &self,
        shutdown: CancellationSignal,
    ) -> BoxFuture<'_, Result<FairSchedulerTick, FairSchedulerError>> {
        Box::pin(self.tick_inner(shutdown))
    }

    async fn tick_inner(
        &self,
        shutdown: CancellationSignal,
    ) -> Result<FairSchedulerTick, FairSchedulerError> {
        let reservation_id = SchedulerReservationId::generate();
        let mut attempt = 1_u8;
        let (reservation, reservation_retries) = loop {
            match self
                .store
                .reserve_scheduler_fairness_slot(
                    self.policy.shard_id(),
                    self.policy.digest(),
                    reservation_id,
                )
                .await
            {
                Ok(reservation) => break (reservation, attempt - 1),
                Err(error)
                    if attempt < self.options.maximum_reservation_attempts()
                        && error.is_retryable() =>
                {
                    let delay = exponential_backoff(
                        self.options.reservation_retry_initial_delay(),
                        attempt,
                    );
                    tokio::time::sleep(delay).await;
                    attempt = attempt.saturating_add(1);
                }
                Err(source) => return Err(FairSchedulerError::Store { source }),
            }
        };
        let tenant_id = self
            .policy
            .tenant_for_slot(reservation.slot())
            .cloned()
            .ok_or(FairSchedulerError::PolicyProjectionMismatch)?;
        let starvation_bound = self
            .policy
            .starvation_bound(&tenant_id)
            .ok_or(FairSchedulerError::PolicyProjectionMismatch)?;
        let tenant_tick = self
            .tenant_scheduler
            .tick(tenant_id.clone(), shutdown)
            .await
            .map_err(|source| FairSchedulerError::Tenant { source })?;
        Ok(FairSchedulerTick {
            reservation,
            tenant_id,
            starvation_bound,
            reservation_retries,
            tenant_tick,
        })
    }
}

/// Startup failure for a distributed fair scheduler.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DurableFairSchedulerBuildError {
    /// Tenant worker/agent loop construction failed.
    #[error("fair scheduler tenant worker binding failed: {source}")]
    Tenant {
        /// Exact local binding failure.
        #[from]
        source: DurableTenantSchedulerBuildError,
    },
    /// Durable policy registration failed.
    #[error("fair scheduler policy registration failed: {source}")]
    Store {
        /// Payload-redacted provider failure.
        #[from]
        source: StoreError,
    },
    /// Stored policy projection differed from the local immutable snapshot.
    #[error("fair scheduler durable policy projection does not match local policy")]
    PolicyProjectionMismatch,
}

/// Failure after a fair scheduler is fully registered.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FairSchedulerError {
    /// Durable slot reservation failed.
    #[error("fair scheduler slot reservation failed: {source}")]
    Store {
        /// Payload-redacted provider failure.
        #[source]
        source: StoreError,
    },
    /// The tenant-scoped scheduler failed before a closed tick outcome.
    #[error("selected tenant scheduler failed: {source}")]
    Tenant {
        /// Exact tenant scheduler failure.
        #[source]
        source: TenantSchedulerError,
    },
    /// Durable slot coordinates escaped the registered immutable policy.
    #[error("fair scheduler durable slot does not match the registered policy")]
    PolicyProjectionMismatch,
}

fn exponential_backoff(initial: Duration, attempt: u8) -> Duration {
    let multiplier = 1_u32
        .checked_shl(u32::from(attempt.saturating_sub(1)))
        .unwrap_or(u32::MAX);
    initial
        .checked_mul(multiplier)
        .unwrap_or(MAX_RESERVATION_RETRY_DELAY)
        .min(MAX_RESERVATION_RETRY_DELAY)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    fn tenant(value: &str) -> TenantId {
        TenantId::try_from(value).unwrap()
    }

    fn shard(value: &str) -> SchedulerShardId {
        SchedulerShardId::try_from(value).unwrap()
    }

    fn weight(value: &str, weight: u16) -> TenantFairnessWeight {
        TenantFairnessWeight::new(tenant(value), weight).unwrap()
    }

    #[test]
    fn policy_is_order_independent_and_preserves_exact_weight_counts() {
        let left = WeightedFairnessPolicy::new(
            shard("prod:v1"),
            [
                weight("tenant-c", 1),
                weight("tenant-a", 5),
                weight("tenant-b", 2),
            ],
        )
        .unwrap();
        let right = WeightedFairnessPolicy::new(
            shard("prod:v1"),
            [
                weight("tenant-b", 2),
                weight("tenant-c", 1),
                weight("tenant-a", 5),
            ],
        )
        .unwrap();
        assert_eq!(left.digest(), right.digest());
        assert_eq!(left.canonical_bytes(), right.canonical_bytes());
        assert_eq!(left.cycle, right.cycle);

        let counts = left
            .cycle
            .iter()
            .fold(BTreeMap::new(), |mut counts, index| {
                *counts
                    .entry(left.tenants[*index].tenant_id.as_str())
                    .or_insert(0_u16) += 1;
                counts
            });
        assert_eq!(counts["tenant-a"], 5);
        assert_eq!(counts["tenant-b"], 2);
        assert_eq!(counts["tenant-c"], 1);
    }

    #[test]
    fn every_reported_starvation_bound_matches_the_circular_schedule() {
        let policy = WeightedFairnessPolicy::new(
            shard("bounds:v1"),
            [
                weight("a", 20),
                weight("b", 8),
                weight("c", 8),
                weight("d", 8),
            ],
        )
        .unwrap();
        for tenant in policy.tenants() {
            let bound = policy.starvation_bound(tenant.tenant_id()).unwrap();
            let mut last = None;
            let mut observed = 0_usize;
            for offset in 0..(policy.cycle.len() * 2) {
                let slot = offset % policy.cycle.len();
                if policy.tenant_for_slot(u16::try_from(slot).unwrap()) == Some(tenant.tenant_id())
                {
                    if let Some(previous) = last {
                        observed = observed.max(offset - previous);
                    }
                    last = Some(offset);
                }
            }
            assert_eq!(
                usize::from(bound.maximum_reservations_until_selection()),
                observed
            );
        }
    }

    #[test]
    fn reference_noisy_tenant_receives_exactly_twenty_percent_per_cycle() {
        let mut tenants = vec![weight("noisy", 20)];
        for index in 0..10 {
            tenants.push(weight(&format!("small-{index:02}"), 8));
        }
        let policy = WeightedFairnessPolicy::new(shard("reference-load:v1"), tenants).unwrap();
        assert_eq!(policy.cycle_length(), 100);
        let noisy_index = policy
            .tenants
            .iter()
            .position(|entry| entry.tenant_id.as_str() == "noisy")
            .unwrap();
        assert_eq!(
            policy
                .cycle
                .iter()
                .filter(|index| **index == noisy_index)
                .count(),
            20
        );
        assert!(
            policy
                .starvation_bound(&tenant("small-00"))
                .unwrap()
                .maximum_reservations_until_selection()
                <= 13
        );
    }

    #[test]
    fn invalid_policy_shapes_fail_closed() {
        assert!(matches!(
            WeightedFairnessPolicy::new(shard("empty"), []),
            Err(WeightedFairnessPolicyError::Empty)
        ));
        assert!(matches!(
            WeightedFairnessPolicy::new(shard("duplicate"), [weight("a", 1), weight("a", 2)]),
            Err(WeightedFairnessPolicyError::DuplicateTenant)
        ));
        assert_eq!(
            TenantFairnessWeight::new(tenant("a"), 0),
            Err(TenantFairnessWeightError::InvalidWeight)
        );
    }

    #[test]
    fn fair_scheduler_options_enforce_bounded_lost_ack_retries() {
        assert_eq!(
            DurableFairSchedulerOptions::new(0, Duration::from_millis(1)),
            Err(DurableFairSchedulerOptionsError::InvalidReservationAttempts)
        );
        assert_eq!(
            DurableFairSchedulerOptions::new(1, Duration::ZERO),
            Err(DurableFairSchedulerOptionsError::InvalidReservationRetryDelay)
        );
        assert!(DurableFairSchedulerOptions::default().maximum_reservation_attempts() > 0);
    }

    proptest! {
        #[test]
        fn compiled_policy_preserves_arbitrary_bounded_weights_and_circular_bounds(
            weights in prop::collection::vec(1_u16..=32, 1..=24),
        ) {
            let entries = weights
                .iter()
                .copied()
                .enumerate()
                .map(|(index, weight)| {
                    TenantFairnessWeight::new(
                        tenant(&format!("property-{index:03}")),
                        weight,
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            let mut reversed = entries.clone();
            reversed.reverse();
            let policy = WeightedFairnessPolicy::new(shard("property:v1"), entries).unwrap();
            let reversed_policy =
                WeightedFairnessPolicy::new(shard("property:v1"), reversed).unwrap();
            prop_assert_eq!(policy.digest(), reversed_policy.digest());
            prop_assert_eq!(policy.cycle.as_ref(), reversed_policy.cycle.as_ref());

            for (index, entry) in policy.tenants.iter().enumerate() {
                let actual = policy.cycle.iter().filter(|slot| **slot == index).count();
                prop_assert_eq!(actual, usize::from(entry.weight()));
                let positions = policy
                    .cycle
                    .iter()
                    .enumerate()
                    .filter_map(|(slot, selected)| (*selected == index).then_some(slot))
                    .collect::<Vec<_>>();
                let maximum_gap = positions
                    .iter()
                    .copied()
                    .zip(
                        positions
                            .iter()
                            .copied()
                            .skip(1)
                            .chain(positions.first().map(|first| first + policy.cycle.len())),
                    )
                    .map(|(current, next)| next - current)
                    .max()
                    .unwrap();
                prop_assert_eq!(
                    usize::from(
                        policy
                            .starvation_bound(entry.tenant_id())
                            .unwrap()
                            .maximum_reservations_until_selection()
                    ),
                    maximum_gap,
                );
            }
        }
    }
}
