// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Protocol-neutral execution leases and stale-writer fencing tokens.
//!
//! These values make the store contract explicit, but an in-memory validation
//! result is never authoritative. A production store must compare the exact
//! token and lease expiry against the current run row using the database clock
//! in the same transaction that commits a worker write.

use std::{fmt, str::FromStr};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::decimal::{UnsignedDecimalError, parse_bounded_u64};
use crate::{AttemptId, RunId, TenantId, Timestamp};

const MAX_DATABASE_ORDINAL: u64 = i64::MAX as u64;
const POSITIVE_I64_PATTERN: &str = "^[1-9][0-9]{0,18}$";

/// Positive, monotonically increasing database fencing epoch for one run.
///
/// The maximum deliberately matches `PostgreSQL` `BIGINT`, and the wire form is
/// a canonical decimal string. The first successful claim uses epoch one;
/// every later claim uses exactly the checked successor, including claims made
/// after an orderly release.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FencingEpoch(u64);

impl FencingEpoch {
    /// Epoch assigned to the first successful lease claim.
    pub const FIRST: Self = Self(1);

    /// Largest epoch representable by the `PostgreSQL` v1 schema.
    pub const MAX: Self = Self(MAX_DATABASE_ORDINAL);

    /// Constructs a positive PostgreSQL-compatible epoch.
    ///
    /// # Errors
    ///
    /// Returns [`FencingEpochError::Zero`] for zero and
    /// [`FencingEpochError::AboveMaximum`] above `PostgreSQL` `BIGINT`.
    pub const fn new(value: u64) -> Result<Self, FencingEpochError> {
        if value == 0 {
            return Err(FencingEpochError::Zero);
        }
        if value > MAX_DATABASE_ORDINAL {
            return Err(FencingEpochError::AboveMaximum);
        }
        Ok(Self(value))
    }

    /// Returns the integer epoch.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the exact successor or `None` at the `PostgreSQL` limit.
    #[must_use]
    pub const fn checked_next(self) -> Option<Self> {
        if self.0 == MAX_DATABASE_ORDINAL {
            None
        } else {
            Some(Self(self.0 + 1))
        }
    }
}

impl fmt::Display for FencingEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for FencingEpoch {
    type Err = FencingEpochError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = parse_bounded_u64(value, MAX_DATABASE_ORDINAL)
            .map_err(FencingEpochError::from_decimal_error)?;
        Self::new(value)
    }
}

impl Serialize for FencingEpoch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for FencingEpoch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(FencingEpochVisitor)
    }
}

impl JsonSchema for FencingEpoch {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FencingEpoch".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::FencingEpoch").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": 19,
            "pattern": POSITIVE_I64_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

struct FencingEpochVisitor;

impl de::Visitor<'_> for FencingEpochVisitor {
    type Value = FencingEpoch;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a canonical positive decimal PostgreSQL BIGINT fencing epoch")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }
}

/// Invalid canonical fencing epoch.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum FencingEpochError {
    /// Epoch zero is reserved for “no lease has ever been issued”.
    #[error("fencing epoch must be positive")]
    Zero,

    /// The value exceeded `PostgreSQL` signed `BIGINT`.
    #[error("fencing epoch exceeds the PostgreSQL BIGINT maximum")]
    AboveMaximum,

    /// The wire value was empty or contained a non-decimal byte.
    #[error("fencing epoch must contain only unsigned ASCII decimal digits")]
    InvalidFormat,

    /// The wire value contained a leading zero.
    #[error("fencing epoch must use canonical decimal text")]
    NonCanonical,
}

impl FencingEpochError {
    const fn from_decimal_error(error: UnsignedDecimalError) -> Self {
        match error {
            UnsignedDecimalError::Empty | UnsignedDecimalError::InvalidCharacter { .. } => {
                Self::InvalidFormat
            }
            UnsignedDecimalError::LeadingZero => Self::NonCanonical,
            UnsignedDecimalError::TooLong { .. } | UnsignedDecimalError::Overflow => {
                Self::AboveMaximum
            }
        }
    }
}

/// Run-scoped proof of one physical execution attempt's ownership epoch.
///
/// The token is copied into every worker-originated journal append, checkpoint,
/// invocation-ledger mutation, and outbox write. It grants nothing by itself;
/// only an exact match with the current unexpired database lease authorizes a
/// commit.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunFence {
    tenant_id: TenantId,
    run_id: RunId,
    attempt_id: AttemptId,
    epoch: FencingEpoch,
}

impl RunFence {
    /// Constructs a run-scoped fencing token from trusted allocation results.
    #[must_use]
    pub const fn new(
        tenant_id: TenantId,
        run_id: RunId,
        attempt_id: AttemptId,
        epoch: FencingEpoch,
    ) -> Self {
        Self {
            tenant_id,
            run_id,
            attempt_id,
            epoch,
        }
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the leased run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the physical execution attempt.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the monotonic ownership epoch.
    #[must_use]
    pub const fn epoch(&self) -> FencingEpoch {
        self.epoch
    }
}

/// Snapshot of the current exclusive execution lease for one run.
///
/// `expires_at` is exclusive. Renewal retains the same fence and must happen
/// before the old expiry while strictly extending it. Supersession changes the
/// attempt and advances the epoch exactly once; it may occur before expiry for
/// an explicit, serialized revocation. Neither operation is authoritative
/// until the corresponding conditional database transaction commits.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunLease {
    fence: RunFence,
    acquired_at: Timestamp,
    renewed_at: Timestamp,
    expires_at: Timestamp,
}

impl RunLease {
    /// Constructs a newly acquired lease.
    ///
    /// # Errors
    ///
    /// Returns [`RunLeaseError::ExpiryNotAfterObservation`] unless expiry is
    /// strictly later than acquisition.
    pub fn new(
        fence: RunFence,
        acquired_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, RunLeaseError> {
        Self::from_observations(fence, acquired_at, acquired_at, expires_at)
    }

    /// Constructs the first lease for a run at epoch one.
    ///
    /// # Errors
    ///
    /// Returns [`RunLeaseError::ExpiryNotAfterObservation`] unless expiry is
    /// strictly later than acquisition.
    pub fn first(
        tenant_id: TenantId,
        run_id: RunId,
        attempt_id: AttemptId,
        acquired_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, RunLeaseError> {
        Self::new(
            RunFence::new(tenant_id, run_id, attempt_id, FencingEpoch::FIRST),
            acquired_at,
            expires_at,
        )
    }

    /// Returns the exact ownership token.
    #[must_use]
    pub const fn fence(&self) -> &RunFence {
        &self.fence
    }

    /// Returns the original database acquisition observation.
    #[must_use]
    pub const fn acquired_at(&self) -> Timestamp {
        self.acquired_at
    }

    /// Returns the latest successful acquisition or renewal observation.
    #[must_use]
    pub const fn renewed_at(&self) -> Timestamp {
        self.renewed_at
    }

    /// Returns the exclusive database expiry observation.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Validates a proposed worker write against this snapshot.
    ///
    /// This method is useful for pure state-machine tests and early rejection.
    /// Production authorization must repeat the same exact fence and expiry
    /// comparison against the locked current row using the database clock in
    /// the write transaction.
    ///
    /// # Errors
    ///
    /// Returns [`RunLeaseValidationError`] for a different tenant, run,
    /// attempt, epoch, a clock observation before acquisition, or an observation
    /// at or after the exclusive expiry.
    pub fn validate_write(
        &self,
        fence: &RunFence,
        observed_at: Timestamp,
    ) -> Result<(), RunLeaseValidationError> {
        self.validate_fence(fence)?;
        if observed_at < self.acquired_at {
            return Err(RunLeaseValidationError::ObservationBeforeAcquisition {
                acquired_at: self.acquired_at,
                observed_at,
            });
        }
        if observed_at >= self.expires_at {
            return Err(RunLeaseValidationError::Expired {
                expires_at: self.expires_at,
                observed_at,
            });
        }
        Ok(())
    }

    /// Returns a renewed snapshot with the same fence and a later expiry.
    ///
    /// Renewal observed exactly at the old expiry is too late. The new expiry
    /// must strictly extend the previous expiry, preventing a retry from
    /// accidentally shortening a live lease.
    ///
    /// # Errors
    ///
    /// Returns [`RunLeaseError`] for a stale token, regressing or late renewal,
    /// or a non-extending expiry.
    pub fn renewed(
        &self,
        fence: &RunFence,
        renewed_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, RunLeaseError> {
        self.validate_fence(fence).map_err(RunLeaseError::fence)?;
        if renewed_at < self.renewed_at {
            return Err(RunLeaseError::RenewalClockRegression {
                previous: self.renewed_at,
                actual: renewed_at,
            });
        }
        if renewed_at >= self.expires_at {
            return Err(RunLeaseError::RenewalTooLate {
                expires_at: self.expires_at,
                renewed_at,
            });
        }
        if expires_at <= self.expires_at {
            return Err(RunLeaseError::ExpiryNotExtended {
                previous: self.expires_at,
                actual: expires_at,
            });
        }
        Self::from_observations(self.fence.clone(), self.acquired_at, renewed_at, expires_at)
    }

    /// Returns a successor lease with a distinct attempt and the next epoch.
    ///
    /// This models both expiry takeover and an explicit serialized revocation.
    /// An orderly release does not reset the persisted epoch; a subsequent
    /// claim must still use its exact successor.
    ///
    /// # Errors
    ///
    /// Returns [`RunLeaseError`] for attempt reuse, a database-clock regression,
    /// epoch exhaustion, or invalid successor timing.
    pub fn superseded(
        &self,
        attempt_id: AttemptId,
        acquired_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, RunLeaseError> {
        if attempt_id == self.fence.attempt_id {
            return Err(RunLeaseError::AttemptReused { attempt_id });
        }
        if acquired_at < self.renewed_at {
            return Err(RunLeaseError::SupersessionClockRegression {
                previous: self.renewed_at,
                actual: acquired_at,
            });
        }
        let epoch = self
            .fence
            .epoch
            .checked_next()
            .ok_or(RunLeaseError::EpochOverflow)?;
        let fence = RunFence::new(
            self.fence.tenant_id.clone(),
            self.fence.run_id,
            attempt_id,
            epoch,
        );
        Self::new(fence, acquired_at, expires_at)
    }

    fn from_observations(
        fence: RunFence,
        acquired_at: Timestamp,
        renewed_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, RunLeaseError> {
        if renewed_at < acquired_at {
            return Err(RunLeaseError::RenewalBeforeAcquisition {
                acquired_at,
                renewed_at,
            });
        }
        if expires_at <= renewed_at {
            return Err(RunLeaseError::ExpiryNotAfterObservation {
                observed_at: renewed_at,
                expires_at,
            });
        }
        Ok(Self {
            fence,
            acquired_at,
            renewed_at,
            expires_at,
        })
    }

    fn validate_fence(&self, fence: &RunFence) -> Result<(), RunLeaseValidationError> {
        if fence.tenant_id != self.fence.tenant_id {
            return Err(RunLeaseValidationError::TenantMismatch);
        }
        if fence.run_id != self.fence.run_id {
            return Err(RunLeaseValidationError::RunMismatch);
        }
        if fence.attempt_id != self.fence.attempt_id {
            return Err(RunLeaseValidationError::AttemptMismatch {
                expected: self.fence.attempt_id,
                actual: fence.attempt_id,
            });
        }
        if fence.epoch != self.fence.epoch {
            return Err(RunLeaseValidationError::EpochMismatch {
                expected: self.fence.epoch,
                actual: fence.epoch,
            });
        }
        Ok(())
    }
}

impl fmt::Debug for RunLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunLease")
            .field("fence", &self.fence)
            .field("acquired_at", &self.acquired_at)
            .field("renewed_at", &self.renewed_at)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for RunLease {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            fence: RunFence,
            acquired_at: Timestamp,
            renewed_at: Timestamp,
            expires_at: Timestamp,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::from_observations(
            wire.fence,
            wire.acquired_at,
            wire.renewed_at,
            wire.expires_at,
        )
        .map_err(de::Error::custom)
    }
}

/// Intrinsically invalid lease acquisition, renewal, or supersession.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RunLeaseError {
    /// The snapshot claimed a renewal before its acquisition.
    #[error("lease renewal {renewed_at} precedes acquisition {acquired_at}")]
    RenewalBeforeAcquisition {
        /// Original acquisition observation.
        acquired_at: Timestamp,
        /// Rejected latest renewal observation.
        renewed_at: Timestamp,
    },

    /// Expiry was not later than its acquisition or renewal observation.
    #[error("lease expiry {expires_at} must be later than observation {observed_at}")]
    ExpiryNotAfterObservation {
        /// Acquisition or renewal observation.
        observed_at: Timestamp,
        /// Rejected exclusive expiry.
        expires_at: Timestamp,
    },

    /// A renewal was attempted with a different token.
    #[error("lease renewal fencing token is stale or belongs to another run")]
    Fence {
        /// Exact mismatch detected before timing checks.
        #[source]
        source: RunLeaseValidationError,
    },

    /// The renewal database observation regressed.
    #[error("lease renewal {actual} precedes previous observation {previous}")]
    RenewalClockRegression {
        /// Latest committed acquisition or renewal observation.
        previous: Timestamp,
        /// Rejected renewal observation.
        actual: Timestamp,
    },

    /// Renewal arrived at or after the old exclusive expiry.
    #[error("lease renewal {renewed_at} is not before expiry {expires_at}")]
    RenewalTooLate {
        /// Old exclusive expiry.
        expires_at: Timestamp,
        /// Rejected renewal observation.
        renewed_at: Timestamp,
    },

    /// A renewal did not strictly extend expiry.
    #[error("renewed expiry {actual} must be later than previous expiry {previous}")]
    ExpiryNotExtended {
        /// Previous exclusive expiry.
        previous: Timestamp,
        /// Rejected new expiry.
        actual: Timestamp,
    },

    /// A successor lease reused the superseded physical attempt.
    #[error("successor lease cannot reuse attempt {attempt_id}")]
    AttemptReused {
        /// Rejected attempt identity.
        attempt_id: AttemptId,
    },

    /// A successor database observation preceded the previous lease record.
    #[error("lease supersession {actual} precedes previous observation {previous}")]
    SupersessionClockRegression {
        /// Latest acquisition or renewal observation of the old lease.
        previous: Timestamp,
        /// Rejected successor acquisition observation.
        actual: Timestamp,
    },

    /// No PostgreSQL-compatible successor epoch exists.
    #[error("fencing epoch overflowed")]
    EpochOverflow,
}

impl RunLeaseError {
    const fn fence(source: RunLeaseValidationError) -> Self {
        Self::Fence { source }
    }
}

/// Rejected worker write against the current lease snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RunLeaseValidationError {
    /// The token crossed a tenant boundary.
    #[error("fencing token tenant does not match the current lease")]
    TenantMismatch,

    /// The token named a different run.
    #[error("fencing token run does not match the current lease")]
    RunMismatch,

    /// The physical attempt is not the current lease holder.
    #[error("fencing token attempt {actual} does not match current attempt {expected}")]
    AttemptMismatch {
        /// Current attempt.
        expected: AttemptId,
        /// Rejected attempt.
        actual: AttemptId,
    },

    /// The token carried a superseded or otherwise non-current epoch.
    #[error("fencing epoch {actual} does not match current epoch {expected}")]
    EpochMismatch {
        /// Current epoch.
        expected: FencingEpoch,
        /// Rejected epoch.
        actual: FencingEpoch,
    },

    /// The proposed database observation preceded acquisition.
    #[error("write observation {observed_at} precedes lease acquisition {acquired_at}")]
    ObservationBeforeAcquisition {
        /// Current lease acquisition.
        acquired_at: Timestamp,
        /// Rejected observation.
        observed_at: Timestamp,
    },

    /// The proposed database observation was at or after exclusive expiry.
    #[error("lease expired at {expires_at}; write observed at {observed_at}")]
    Expired {
        /// Exclusive lease expiry.
        expires_at: Timestamp,
        /// Rejected observation.
        observed_at: Timestamp,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    fn at(offset_micros: i64) -> Timestamp {
        let base = "2030-01-01T00:00:00.000000Z".parse::<Timestamp>().unwrap();
        Timestamp::from_unix_micros(base.unix_micros() + offset_micros).unwrap()
    }

    fn tenant() -> TenantId {
        TenantId::try_from("tenant-a").unwrap()
    }

    fn run() -> RunId {
        "01912345-6789-7abc-8def-0123456789ab".parse().unwrap()
    }

    fn attempt(suffix: &str) -> AttemptId {
        format!("01912345-6789-7abc-8def-0123456789{suffix}")
            .parse()
            .unwrap()
    }

    fn lease() -> RunLease {
        RunLease::first(tenant(), run(), attempt("ac"), at(10), at(20)).unwrap()
    }

    #[test]
    fn fencing_epochs_are_positive_canonical_database_ordinals() {
        assert_eq!(FencingEpoch::new(1), Ok(FencingEpoch::FIRST));
        assert_eq!(FencingEpoch::new(0), Err(FencingEpochError::Zero));
        assert_eq!(
            FencingEpoch::new(MAX_DATABASE_ORDINAL + 1),
            Err(FencingEpochError::AboveMaximum)
        );
        assert_eq!(FencingEpoch::MAX.checked_next(), None);
        assert_eq!(FencingEpoch::FIRST.checked_next().unwrap().get(), 2);

        assert_eq!(
            to_value(FencingEpoch::MAX).unwrap(),
            json!(i64::MAX.to_string())
        );
        assert_eq!(
            from_value::<FencingEpoch>(json!("1")).unwrap(),
            FencingEpoch::FIRST
        );
        for invalid in [json!("0"), json!("01"), json!("x"), json!(1), Value::Null] {
            assert!(from_value::<FencingEpoch>(invalid).is_err());
        }
    }

    #[test]
    fn fencing_epoch_schema_matches_runtime_shape() {
        let schema = to_value(schemars::schema_for!(FencingEpoch)).unwrap();
        assert_eq!(schema["type"], "string");
        assert_eq!(schema["minLength"], 1);
        assert_eq!(schema["maxLength"], 19);
        assert_eq!(schema["pattern"], POSITIVE_I64_PATTERN);
    }

    #[test]
    fn lease_expiry_is_exclusive() {
        let lease = lease();
        assert_eq!(lease.fence().epoch(), FencingEpoch::FIRST);
        assert_eq!(lease.acquired_at(), at(10));
        assert_eq!(lease.renewed_at(), at(10));
        assert_eq!(lease.expires_at(), at(20));
        assert_eq!(lease.validate_write(lease.fence(), at(10)), Ok(()));
        assert_eq!(lease.validate_write(lease.fence(), at(19)), Ok(()));
        assert_eq!(
            lease.validate_write(lease.fence(), at(20)),
            Err(RunLeaseValidationError::Expired {
                expires_at: at(20),
                observed_at: at(20),
            })
        );
        assert!(matches!(
            lease.validate_write(lease.fence(), at(9)),
            Err(RunLeaseValidationError::ObservationBeforeAcquisition { .. })
        ));
    }

    #[test]
    fn renewal_keeps_the_fence_and_strictly_extends_expiry() {
        let lease = lease();
        let renewed = lease.renewed(lease.fence(), at(19), at(30)).unwrap();
        assert_eq!(renewed.fence(), lease.fence());
        assert_eq!(renewed.acquired_at(), at(10));
        assert_eq!(renewed.renewed_at(), at(19));
        assert_eq!(renewed.expires_at(), at(30));

        assert!(matches!(
            lease.renewed(lease.fence(), at(20), at(30)),
            Err(RunLeaseError::RenewalTooLate { .. })
        ));
        assert!(matches!(
            lease.renewed(lease.fence(), at(19), at(20)),
            Err(RunLeaseError::ExpiryNotExtended { .. })
        ));
        let twice = renewed.renewed(renewed.fence(), at(18), at(40));
        assert!(matches!(
            twice,
            Err(RunLeaseError::RenewalClockRegression { .. })
        ));
    }

    #[test]
    fn supersession_fences_the_old_attempt_even_before_expiry() {
        let old = lease();
        let new = old.superseded(attempt("ad"), at(15), at(25)).unwrap();
        assert_eq!(new.fence().epoch().get(), 2);
        assert_eq!(new.fence().attempt_id(), attempt("ad"));
        assert_eq!(
            new.validate_write(old.fence(), at(16)),
            Err(RunLeaseValidationError::AttemptMismatch {
                expected: attempt("ad"),
                actual: attempt("ac"),
            })
        );
        assert_eq!(new.validate_write(new.fence(), at(16)), Ok(()));
        assert!(matches!(
            old.superseded(attempt("ac"), at(15), at(25)),
            Err(RunLeaseError::AttemptReused { .. })
        ));
        assert!(matches!(
            old.superseded(attempt("ad"), at(9), at(25)),
            Err(RunLeaseError::SupersessionClockRegression { .. })
        ));
    }

    #[test]
    fn every_token_component_is_bound() {
        let lease = lease();
        let other_tenant = RunFence::new(
            TenantId::try_from("tenant-b").unwrap(),
            run(),
            attempt("ac"),
            FencingEpoch::FIRST,
        );
        assert_eq!(
            lease.validate_write(&other_tenant, at(15)),
            Err(RunLeaseValidationError::TenantMismatch)
        );

        let other_run = RunFence::new(
            tenant(),
            "01912345-6789-7abc-8def-0123456789bb".parse().unwrap(),
            attempt("ac"),
            FencingEpoch::FIRST,
        );
        assert_eq!(
            lease.validate_write(&other_run, at(15)),
            Err(RunLeaseValidationError::RunMismatch)
        );

        let other_epoch = RunFence::new(
            tenant(),
            run(),
            attempt("ac"),
            FencingEpoch::new(2).unwrap(),
        );
        assert!(matches!(
            lease.validate_write(&other_epoch, at(15)),
            Err(RunLeaseValidationError::EpochMismatch { .. })
        ));
    }

    #[test]
    fn snapshot_deserialization_revalidates_timing_and_shape() {
        let encoded = to_value(lease()).unwrap();
        assert_eq!(from_value::<RunLease>(encoded.clone()).unwrap(), lease());

        let mut expiry = encoded.clone();
        expiry["expires_at"] = expiry["renewed_at"].clone();
        assert!(from_value::<RunLease>(expiry).is_err());

        let mut renewal = encoded.clone();
        renewal["renewed_at"] = json!(at(9).to_string());
        assert!(from_value::<RunLease>(renewal).is_err());

        let mut extra = encoded;
        extra["owner"] = json!("untrusted-worker");
        assert!(from_value::<RunLease>(extra).is_err());
    }

    #[test]
    fn successor_rejects_epoch_exhaustion() {
        let fence = RunFence::new(tenant(), run(), attempt("ac"), FencingEpoch::MAX);
        let lease = RunLease::new(fence, at(10), at(20)).unwrap();
        assert_eq!(
            lease.superseded(attempt("ad"), at(20), at(30)),
            Err(RunLeaseError::EpochOverflow)
        );
    }

    proptest! {
        #[test]
        fn accepted_renewals_preserve_token_and_monotonically_extend(
            renewal_offsets in proptest::collection::vec(1_i64..10_000, 1..64)
        ) {
            let mut lease = RunLease::first(
                tenant(), run(), attempt("ac"), at(0), at(10_000)
            ).unwrap();
            let fence = lease.fence().clone();
            let mut observed = 0_i64;
            for offset in renewal_offsets {
                observed = (observed + offset).min(9_999);
                let new_expiry = lease.expires_at().unix_micros().checked_add(10_000).unwrap();
                let renewed = lease.renewed(
                    &fence,
                    Timestamp::from_unix_micros(at(0).unix_micros() + observed).unwrap(),
                    Timestamp::from_unix_micros(new_expiry).unwrap(),
                ).unwrap();
                prop_assert_eq!(renewed.fence(), &fence);
                prop_assert!(renewed.expires_at() > lease.expires_at());
                lease = renewed;
            }
        }
    }
}
