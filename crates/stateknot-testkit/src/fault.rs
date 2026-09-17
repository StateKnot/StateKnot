// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

const MAX_INVARIANT_CHECKS: u16 = 64;

/// Stable fault case understood by qualification report schema version one.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultCaseId {
    /// The primary database becomes unavailable and later recovers.
    DatabaseUnavailable,
    /// One host dependency readiness gate becomes unavailable and later recovers.
    HostDependencyUnavailable,
    /// A Worker exits while accepted work remains recoverable.
    WorkerLoss,
    /// A replacement host becomes ready before the old host drains.
    HostRollingReplacement,
    /// Online identity verification becomes unavailable and later recovers.
    VerifierUnavailable,
    /// The protected operations policy expires and is replaced.
    OperationsPolicyExpiry,
}

impl FaultCaseId {
    /// Every fault required by the version-one release profile.
    pub const ALL: [Self; 6] = [
        Self::DatabaseUnavailable,
        Self::HostDependencyUnavailable,
        Self::WorkerLoss,
        Self::HostRollingReplacement,
        Self::VerifierUnavailable,
        Self::OperationsPolicyExpiry,
    ];
}

/// Invalid fault-plan or transition data.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FaultError {
    /// The plan is empty or exceeds the closed matrix.
    #[error("fault plan must contain between one and six cases")]
    InvalidPlanSize,
    /// A case occurs more than once in a plan or report.
    #[error("fault case {0:?} is duplicated")]
    DuplicateCase(FaultCaseId),
    /// The invariant-check requirement is outside its bound.
    #[error("fault case {0:?} has an invalid invariant-check requirement")]
    InvalidInvariantRequirement(FaultCaseId),
    /// A transition references a case outside the plan.
    #[error("fault case {0:?} was not planned")]
    UnplannedCase(FaultCaseId),
    /// A transition count overflowed.
    #[error("fault case {0:?} transition count overflowed")]
    CountOverflow(FaultCaseId),
    /// Injection, recovery or invariant checks occurred in an impossible order.
    #[error("fault case {0:?} has an invalid transition order")]
    InvalidTransition(FaultCaseId),
}

/// One planned fault and the number of invariants that must be checked.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultRequirement {
    case: FaultCaseId,
    required_invariant_checks: u16,
}

impl FaultRequirement {
    /// Creates a requirement with a bounded non-zero invariant count.
    pub fn new(case: FaultCaseId, required_invariant_checks: u16) -> Result<Self, FaultError> {
        if required_invariant_checks == 0 || required_invariant_checks > MAX_INVARIANT_CHECKS {
            return Err(FaultError::InvalidInvariantRequirement(case));
        }
        Ok(Self {
            case,
            required_invariant_checks,
        })
    }

    /// Returns the stable fault identifier.
    pub const fn case(&self) -> FaultCaseId {
        self.case
    }

    /// Returns the required number of invariant checks.
    pub const fn required_invariant_checks(&self) -> u16 {
        self.required_invariant_checks
    }

    pub(crate) fn validate(&self) -> Result<(), FaultError> {
        if self.required_invariant_checks == 0
            || self.required_invariant_checks > MAX_INVARIANT_CHECKS
        {
            return Err(FaultError::InvalidInvariantRequirement(self.case));
        }
        Ok(())
    }
}

/// Closed, deterministically ordered fault plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultPlan {
    requirements: Vec<FaultRequirement>,
}

impl FaultPlan {
    /// Creates a non-empty plan with unique stable fault identifiers.
    pub fn new(mut requirements: Vec<FaultRequirement>) -> Result<Self, FaultError> {
        requirements.sort_by_key(FaultRequirement::case);
        let plan = Self { requirements };
        plan.validate()?;
        Ok(plan)
    }

    /// Returns the reduced CI plan. It is never sufficient for release qualification.
    pub fn ci_default() -> Self {
        Self::new(vec![
            FaultRequirement::new(FaultCaseId::HostDependencyUnavailable, 2)
                .expect("constant requirement is valid"),
            FaultRequirement::new(FaultCaseId::HostRollingReplacement, 3)
                .expect("constant requirement is valid"),
        ])
        .expect("constant plan is valid")
    }

    /// Returns the complete version-one release fault matrix.
    pub fn release_default() -> Self {
        Self::new(
            FaultCaseId::ALL
                .into_iter()
                .map(|case| FaultRequirement::new(case, 1).expect("constant requirement is valid"))
                .collect(),
        )
        .expect("constant plan is valid")
    }

    /// Returns the sorted requirements.
    pub fn requirements(&self) -> &[FaultRequirement] {
        &self.requirements
    }

    pub(crate) fn validate(&self) -> Result<(), FaultError> {
        if self.requirements.is_empty() || self.requirements.len() > FaultCaseId::ALL.len() {
            return Err(FaultError::InvalidPlanSize);
        }
        let mut seen = BTreeSet::new();
        for requirement in &self.requirements {
            requirement.validate()?;
            if !seen.insert(requirement.case) {
                return Err(FaultError::DuplicateCase(requirement.case));
            }
        }
        Ok(())
    }

    pub(crate) fn is_release_complete(&self) -> bool {
        self.requirements.len() == FaultCaseId::ALL.len()
            && FaultCaseId::ALL
                .iter()
                .all(|case| self.requirements.iter().any(|entry| entry.case == *case))
    }
}

/// Executed counts for one planned fault.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FaultResult {
    case: FaultCaseId,
    required_invariant_checks: u16,
    injections: u32,
    recoveries: u32,
    invariant_checks: u32,
}

impl FaultResult {
    pub(crate) fn planned(requirement: &FaultRequirement) -> Self {
        Self {
            case: requirement.case,
            required_invariant_checks: requirement.required_invariant_checks,
            injections: 0,
            recoveries: 0,
            invariant_checks: 0,
        }
    }

    /// Returns the stable case identifier.
    pub const fn case(&self) -> FaultCaseId {
        self.case
    }

    /// Returns whether injection, recovery and every required invariant were observed.
    pub const fn is_complete(&self) -> bool {
        self.injections > 0
            && self.recoveries > 0
            && self.invariant_checks >= self.required_invariant_checks as u32
    }

    pub(crate) fn injected(&mut self) -> Result<(), FaultError> {
        if self.injections != self.recoveries {
            return Err(FaultError::InvalidTransition(self.case));
        }
        self.injections = self
            .injections
            .checked_add(1)
            .ok_or(FaultError::CountOverflow(self.case))?;
        Ok(())
    }

    pub(crate) fn recovered(&mut self) -> Result<(), FaultError> {
        if self.recoveries >= self.injections {
            return Err(FaultError::InvalidTransition(self.case));
        }
        self.recoveries = self
            .recoveries
            .checked_add(1)
            .ok_or(FaultError::CountOverflow(self.case))?;
        Ok(())
    }

    pub(crate) fn invariant_checked(&mut self) -> Result<(), FaultError> {
        if self.injections == 0 {
            return Err(FaultError::InvalidTransition(self.case));
        }
        self.invariant_checks = self
            .invariant_checks
            .checked_add(1)
            .ok_or(FaultError::CountOverflow(self.case))?;
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), FaultError> {
        FaultRequirement::new(self.case, self.required_invariant_checks)?;
        if self.recoveries > self.injections || (self.injections == 0 && self.invariant_checks != 0)
        {
            return Err(FaultError::InvalidTransition(self.case));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_are_closed_unique_and_sorted() {
        let plan = FaultPlan::new(vec![
            FaultRequirement::new(FaultCaseId::WorkerLoss, 1).unwrap(),
            FaultRequirement::new(FaultCaseId::DatabaseUnavailable, 2).unwrap(),
        ])
        .unwrap();
        assert_eq!(
            plan.requirements()[0].case(),
            FaultCaseId::DatabaseUnavailable
        );
        assert!(FaultPlan::new(vec![]).is_err());
        assert!(
            FaultPlan::new(vec![
                FaultRequirement::new(FaultCaseId::WorkerLoss, 1).unwrap(),
                FaultRequirement::new(FaultCaseId::WorkerLoss, 2).unwrap(),
            ])
            .is_err()
        );
        assert!(FaultPlan::release_default().is_release_complete());
        assert!(!FaultPlan::ci_default().is_release_complete());
    }

    #[test]
    fn fault_transitions_are_ordered() {
        let requirement = FaultRequirement::new(FaultCaseId::WorkerLoss, 1).unwrap();
        let mut result = FaultResult::planned(&requirement);
        assert_eq!(
            result.recovered(),
            Err(FaultError::InvalidTransition(FaultCaseId::WorkerLoss))
        );
        assert_eq!(
            result.invariant_checked(),
            Err(FaultError::InvalidTransition(FaultCaseId::WorkerLoss))
        );
        result.injected().unwrap();
        assert_eq!(
            result.injected(),
            Err(FaultError::InvalidTransition(FaultCaseId::WorkerLoss))
        );
        result.invariant_checked().unwrap();
        result.recovered().unwrap();
        assert!(result.is_complete());
    }
}
