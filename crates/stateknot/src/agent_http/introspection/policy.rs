// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::super::{AgentHttpAuthenticationError, AgentHttpOperation, AgentHttpPrincipal};
use super::IntrospectionConfigurationError;
use stateknot_core::{PrincipalIdentity, TenantId};
use stateknot_runtime::AgentServiceCaller;
use std::{collections::BTreeMap, sync::RwLock, time::Duration};
use tokio::time::Instant;

/// Explicit trusted identity binding, not an `AgentService` resource grant.
#[derive(Clone)]
pub struct TenantBinding {
    caller: AgentServiceCaller,
    operations: Vec<AgentHttpOperation>,
}

impl TenantBinding {
    /// Binds one exact issuer/subject to one tenant and at most four operations.
    /// No wildcard or token-supplied tenant is interpreted.
    #[must_use]
    pub fn new(
        tenant: TenantId,
        principal: PrincipalIdentity,
        operations: &[AgentHttpOperation],
    ) -> Self {
        let operations = [
            AgentHttpOperation::Submit,
            AgentHttpOperation::Read,
            AgentHttpOperation::Cancel,
            AgentHttpOperation::InspectHost,
        ]
        .into_iter()
        .filter(|op| operations.contains(op))
        .collect();
        Self {
            caller: AgentServiceCaller::new(tenant, principal),
            operations,
        }
    }
}

struct Snapshot {
    generation: u64,
    expires: Instant,
    bindings: BTreeMap<PrincipalIdentity, TenantBinding>,
}

/// Shared default-deny, bounded, process-local tenant mapping policy.
/// The trusted control plane must refresh it; this is not a durable ACL ledger.
pub struct TenantPolicy(RwLock<Snapshot>);

impl TenantPolicy {
    /// Maximum bindings per snapshot.
    pub const MAX_BINDINGS: usize = 1024;
    /// Maximum freshness lease; no automatic renewal.
    pub const MAX_LEASE: Duration = Duration::from_secs(3600);

    /// Validates a snapshot at generation 1. An empty snapshot denies everyone.
    ///
    /// # Errors
    /// Rejects duplicate principals, excessive bindings or zero/excessive lease.
    pub fn new(
        bindings: Vec<TenantBinding>,
        lease: Duration,
    ) -> Result<Self, IntrospectionConfigurationError> {
        Ok(Self(RwLock::new(Self::snapshot(bindings, lease, 1)?)))
    }

    /// Atomically replaces validated bindings, including explicit revocations.
    /// Callers must have revalidated the trusted source, not merely the old cache.
    /// Already authorized work is not rolled back. Returns the next generation.
    ///
    /// # Errors
    /// Rejects invalid snapshots, stale generations, overflow or poisoned locks.
    /// The previous snapshot remains intact on failure.
    pub fn replace(
        &self,
        expected_generation: u64,
        bindings: Vec<TenantBinding>,
        lease: Duration,
    ) -> Result<u64, IntrospectionConfigurationError> {
        let next = expected_generation
            .checked_add(1)
            .ok_or(IntrospectionConfigurationError)?;
        let replacement = Self::snapshot(bindings, lease, next)?;
        let mut current = self
            .0
            .write()
            .map_err(|_| IntrospectionConfigurationError)?;
        if current.generation != expected_generation {
            return Err(IntrospectionConfigurationError);
        }
        *current = replacement;
        Ok(next)
    }

    /// Returns the CAS generation, not evidence of policy freshness or a digest.
    ///
    /// # Errors
    /// Fails closed when the policy lock was poisoned.
    pub fn generation(&self) -> Result<u64, IntrospectionConfigurationError> {
        self.0
            .read()
            .map(|s| s.generation)
            .map_err(|_| IntrospectionConfigurationError)
    }

    pub(super) fn check(&self) -> Result<(), AgentHttpAuthenticationError> {
        let current = self
            .0
            .read()
            .map_err(|_| AgentHttpAuthenticationError::Unavailable)?;
        if Instant::now() >= current.expires {
            return Err(AgentHttpAuthenticationError::Unavailable);
        }
        Ok(())
    }

    pub(super) fn resolve(
        &self,
        principal: &PrincipalIdentity,
        scopes: &[String],
        required: &[String; 3],
        inspection_scope: Option<&str>,
    ) -> Result<AgentHttpPrincipal, AgentHttpAuthenticationError> {
        let current = self
            .0
            .read()
            .map_err(|_| AgentHttpAuthenticationError::Unavailable)?;
        if Instant::now() >= current.expires {
            return Err(AgentHttpAuthenticationError::Unavailable);
        }
        let binding = current
            .bindings
            .get(principal)
            .ok_or(AgentHttpAuthenticationError::Unauthenticated)?;
        let operations = [
            AgentHttpOperation::Submit,
            AgentHttpOperation::Read,
            AgentHttpOperation::Cancel,
        ]
        .into_iter()
        .zip(required)
        .filter_map(|(op, scope)| {
            (binding.operations.contains(&op) && scopes.contains(scope)).then_some(op)
        });
        let inspection = inspection_scope
            .filter(|scope| {
                binding
                    .operations
                    .contains(&AgentHttpOperation::InspectHost)
                    && scopes.iter().any(|granted| granted == scope)
            })
            .map(|_| AgentHttpOperation::InspectHost);
        Ok(AgentHttpPrincipal::new(
            binding.caller.clone(),
            operations.chain(inspection),
        ))
    }

    fn snapshot(
        bindings: Vec<TenantBinding>,
        lease: Duration,
        generation: u64,
    ) -> Result<Snapshot, IntrospectionConfigurationError> {
        if bindings.len() > Self::MAX_BINDINGS || lease.is_zero() || lease > Self::MAX_LEASE {
            return Err(IntrospectionConfigurationError);
        }
        let mut result = BTreeMap::new();
        for binding in bindings {
            if result
                .insert(binding.caller.principal().clone(), binding)
                .is_some()
            {
                return Err(IntrospectionConfigurationError);
            }
        }
        Ok(Snapshot {
            generation,
            expires: Instant::now() + lease,
            bindings: result,
        })
    }
}
