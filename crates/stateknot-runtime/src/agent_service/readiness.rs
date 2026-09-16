// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::AgentServiceV1;
use thiserror::Error;

/// Closed dependency-readiness failure; contains no database or deployment details.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentServiceReadinessError {
    /// No public Agent is installed, or an exact executable binding is unavailable.
    #[error("Agent service deployment is unavailable")]
    DeploymentUnavailable,
    /// The actual service pool cannot verify its required `PostgreSQL` schema.
    #[error("Agent service storage is unavailable")]
    StorageUnavailable,
}

impl AgentServiceV1 {
    /// Checks frozen Agent/executable bindings and the actual store's schema.
    ///
    /// Read-only and unauthenticated: this is a trusted host operation, not a
    /// business endpoint. Does not call initial-state factories, policy decisions,
    /// or executors. The host must separately check identity/policy dependencies,
    /// bound this future, and qualify database role privileges before deployment.
    /// Readiness is an observation, not a guarantee of future request success.
    ///
    /// # Errors
    ///
    /// Rejects empty/drifted/missing executable deployments or unavailable schema.
    pub async fn check_readiness(&self) -> Result<(), AgentServiceReadinessError> {
        if self.deployments.is_empty() {
            return Err(AgentServiceReadinessError::DeploymentUnavailable);
        }
        for identity in self.deployments.bindings.keys() {
            let binding = self
                .deployments
                .resolve(identity)
                .map_err(|_| AgentServiceReadinessError::DeploymentUnavailable)?;
            let executable = self
                .runs
                .executable_registry()
                .resolve(&binding.graph)
                .ok_or(AgentServiceReadinessError::DeploymentUnavailable)?;
            if executable.graph().input_schema() != binding.descriptor.input_schema()
                || executable.graph().output_schema() != binding.descriptor.output_schema()
            {
                return Err(AgentServiceReadinessError::DeploymentUnavailable);
            }
        }
        self.store
            .verify_schema()
            .await
            .map_err(|_| AgentServiceReadinessError::StorageUnavailable)
    }
}
