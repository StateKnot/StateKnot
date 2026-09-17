// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::{AgentHostHealth, AgentHostOperationsError};
use crate::agent_http::{AgentHttpOperation, AgentHttpPrincipal, HttpError};
use stateknot_runtime::AgentServiceCaller;
use std::{sync::RwLock, time::Duration};
use tokio::time::Instant;

#[cfg(test)]
#[path = "policy_tests.rs"]
mod tests;

struct Snapshot {
    generation: u64,
    expires: Instant,
    operators: Vec<AgentServiceCaller>,
}

/// Trusted process-local inspection ACL bound to one actual host health view.
/// No automatic refresh, wildcard, token-tenant mapping or business resource grant.
pub struct AgentHostOperationsPolicy {
    pub(super) health: AgentHostHealth,
    snapshot: RwLock<Snapshot>,
}
impl AgentHostOperationsPolicy {
    /// Maximum distinct exact callers; empty denies everyone.
    pub const MAX_OPERATORS: usize = 128;
    /// Maximum monotonic freshness lease.
    pub const MAX_LEASE: Duration = Duration::from_secs(3600);

    /// Validates caller uniqueness and a nonzero lease, at generation one.
    pub fn new(
        health: AgentHostHealth,
        operators: Vec<AgentServiceCaller>,
        lease: Duration,
    ) -> Result<Self, AgentHostOperationsError> {
        Ok(Self {
            health,
            snapshot: RwLock::new(Self::validate(operators, lease, 1)?),
        })
    }

    /// Atomically replaces from a freshly validated trusted source. Stale CAS,
    /// overflow, poison or invalid input leaves the old policy unchanged.
    /// Already authorized responses are not retroactively revoked.
    pub fn replace(
        &self,
        expected_generation: u64,
        operators: Vec<AgentServiceCaller>,
        lease: Duration,
    ) -> Result<u64, AgentHostOperationsError> {
        let next = expected_generation
            .checked_add(1)
            .ok_or(AgentHostOperationsError::InvalidPolicy)?;
        let replacement = Self::validate(operators, lease, next)?;
        let mut current = self
            .snapshot
            .write()
            .map_err(|_| AgentHostOperationsError::InvalidPolicy)?;
        if current.generation != expected_generation {
            return Err(AgentHostOperationsError::InvalidPolicy);
        }
        *current = replacement;
        Ok(next)
    }

    /// CAS generation only, not proof of freshness.
    pub fn generation(&self) -> Result<u64, AgentHostOperationsError> {
        self.snapshot
            .read()
            .map(|s| s.generation)
            .map_err(|_| AgentHostOperationsError::InvalidPolicy)
    }

    pub(super) fn authorize(&self, principal: &AgentHttpPrincipal) -> Result<(), HttpError> {
        if !principal.allows(AgentHttpOperation::InspectHost) {
            return Err(HttpError::Denied);
        }
        let current = self.snapshot.read().map_err(|_| HttpError::Unavailable)?;
        if Instant::now() >= current.expires {
            return Err(HttpError::Unavailable);
        }
        if !current.operators.contains(principal.caller()) {
            return Err(HttpError::Denied);
        }
        Ok(())
    }

    fn validate(
        operators: Vec<AgentServiceCaller>,
        lease: Duration,
        generation: u64,
    ) -> Result<Snapshot, AgentHostOperationsError> {
        if operators.len() > Self::MAX_OPERATORS
            || lease.is_zero()
            || lease > Self::MAX_LEASE
            || operators
                .iter()
                .enumerate()
                .any(|(index, caller)| operators[..index].contains(caller))
        {
            return Err(AgentHostOperationsError::InvalidPolicy);
        }
        Ok(Snapshot {
            generation,
            expires: Instant::now() + lease,
            operators,
        })
    }
}
