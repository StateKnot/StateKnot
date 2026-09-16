// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Default-deny, offline resource policy for trusted static/service-account ACLs.
//! Retain artifacts privately before activation. No ownership inference, network
//! PDP, persistent read/deny audit ledger or distributed policy cache is provided.
//!
//! ```
//! use std::{sync::Arc, time::Duration};
//! use stateknot_core::Digest;
//! use stateknot_runtime::{JsonSchemaRegistryBuilder, agent_policy::*};
//!
//! fn load_policy(
//!     retained_bytes: &[u8], trusted_digest: Digest,
//!     schemas: &mut JsonSchemaRegistryBuilder,
//! ) -> Result<Arc<AgentResourcePolicy>, PolicyError> {
//!     register_agent_policy_evidence_schema(schemas)?;
//!     let artifact = PolicyArtifact::from_json(retained_bytes, trusted_digest)?;
//!     Ok(Arc::new(AgentResourcePolicy::new(artifact, Duration::from_secs(300))?))
//! }
//! ```

use super::{
    AgentServiceAuthorizationError, AgentServiceAuthorizer, AgentServiceRunAuthorization,
    AgentServiceRunGrant, AgentServiceRunOperation, AgentServiceRunTarget,
    AgentServiceSubmissionAuthorization, AgentServiceSubmissionGrant,
};
use stateknot_core::{
    AgentAdmissionAuthority, AgentAdmissionBudgetLayer, BoundedJson, BoxFuture, JournalEventKind,
    JournalPayload,
};
use std::{
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::time::Instant;

mod artifact;
mod schema;
use artifact::checksum;
pub use artifact::{
    PolicyArtifact, PolicyDocument, RunAccessTarget, RunPermission, RunRule, SubmissionRule,
};
pub use schema::{agent_policy_evidence_schema, register_agent_policy_evidence_schema};

/// Sanitized invalid artifact, lease, CAS or unavailable policy state.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Agent resource policy is invalid or unavailable")]
pub struct PolicyError;

struct Snapshot {
    artifact: Arc<PolicyArtifact>,
    expires: Instant,
    generation: u64,
}

/// Shared bounded resource policy with explicit expiring CAS refresh.
/// Replacements affect later authorization checks, not already granted work.
pub struct AgentResourcePolicy(RwLock<Snapshot>);

impl AgentResourcePolicy {
    /// Maximum local freshness lease. Renew only from a revalidated trusted source.
    pub const MAX_LEASE: Duration = Duration::from_secs(3600);

    /// Installs a retained artifact at generation 1 with finite freshness.
    ///
    /// # Errors
    /// Rejects zero/excessive lease, expired artifact or unavailable host clock.
    pub fn new(artifact: PolicyArtifact, lease: Duration) -> Result<Self, PolicyError> {
        Ok(Self(RwLock::new(Self::snapshot(artifact, lease, 1)?)))
    }

    /// Atomically replaces the snapshot after validating a trusted refresh.
    /// Unrelated rule changes do not alter a selected rule's admission evidence.
    ///
    /// # Errors
    /// Invalid/expired input, stale generation, overflow or poisoned lock leave
    /// the previous snapshot intact. This is not cross-replica coordination.
    pub fn replace(
        &self,
        expected_generation: u64,
        artifact: PolicyArtifact,
        lease: Duration,
    ) -> Result<u64, PolicyError> {
        let next = expected_generation.checked_add(1).ok_or(PolicyError)?;
        let replacement = Self::snapshot(artifact, lease, next)?;
        let mut current = self.0.write().map_err(|_| PolicyError)?;
        if current.generation != expected_generation {
            return Err(PolicyError);
        }
        *current = replacement;
        Ok(next)
    }

    /// Returns process-local CAS generation; neither a digest nor freshness proof.
    ///
    /// # Errors
    /// Rejects poisoned policy state.
    pub fn generation(&self) -> Result<u64, PolicyError> {
        self.0.read().map(|s| s.generation).map_err(|_| PolicyError)
    }

    /// Checks the current snapshot without granting an operation or doing I/O.
    /// An empty, fresh deny-all artifact is healthy.
    ///
    /// # Errors
    /// Rejects absolute/monotonic expiration, clock failure or poisoned state.
    pub fn check_readiness(&self) -> Result<(), PolicyError> {
        self.current().map(|_| ())
    }

    fn snapshot(
        artifact: PolicyArtifact,
        lease: Duration,
        generation: u64,
    ) -> Result<Snapshot, PolicyError> {
        if lease.is_zero() || lease > Self::MAX_LEASE {
            return Err(PolicyError);
        }
        let remaining = artifact
            .document
            .valid_until
            .unix_micros()
            .checked_sub(now_micros()?)
            .filter(|value| *value > 0)
            .ok_or(PolicyError)?;
        let absolute = Duration::from_micros(u64::try_from(remaining).map_err(|_| PolicyError)?);
        Ok(Snapshot {
            artifact: Arc::new(artifact),
            expires: Instant::now() + lease.min(absolute),
            generation,
        })
    }

    fn current(&self) -> Result<Arc<PolicyArtifact>, PolicyError> {
        let current = self.0.read().map_err(|_| PolicyError)?;
        if Instant::now() >= current.expires
            || now_micros()? >= current.artifact.document.valid_until.unix_micros()
        {
            return Err(PolicyError);
        }
        Ok(current.artifact.clone())
    }

    fn submission(
        &self,
        context: &AgentServiceSubmissionAuthorization,
    ) -> Result<AgentServiceSubmissionGrant, AgentServiceAuthorizationError> {
        let artifact = self
            .current()
            .map_err(|_| AgentServiceAuthorizationError::Unavailable)?;
        let (index, rule) = artifact
            .document
            .submissions
            .iter()
            .enumerate()
            .find(|(_, rule)| {
                &rule.tenant == context.caller().tenant_id()
                    && &rule.principal == context.caller().principal()
                    && &rule.agent == context.agent()
                    && &rule.input_schema == context.request().input_schema()
            })
            .ok_or(AgentServiceAuthorizationError::Denied)?;
        let policy_digest = artifact.submissions[index];
        let invalid = |_| AgentServiceAuthorizationError::InvalidEvidence;
        let request_digest =
            checksum(b"stateknot.agent-policy-request.v1\0", context.request()).map_err(invalid)?;
        let (schema, _) = agent_policy_evidence_schema().map_err(invalid)?;
        let data = BoundedJson::try_from_value(serde_json::json!({
            "operation": "submission_granted",
            "policy_digest": policy_digest,
            "request_digest": request_digest
        }))
        .map_err(|_| AgentServiceAuthorizationError::InvalidEvidence)?;
        let kind = JournalEventKind::new(AgentAdmissionAuthority::EVIDENCE_KIND)
            .map_err(|_| AgentServiceAuthorizationError::InvalidEvidence)?;
        let evidence = JournalPayload::new(schema, kind, data)
            .map_err(|_| AgentServiceAuthorizationError::InvalidEvidence)?;
        let budget = AgentAdmissionBudgetLayer::new(
            artifact.document.policy.clone(),
            evidence.digest(),
            rule.budget_limits.clone(),
        )
        .map_err(|_| AgentServiceAuthorizationError::InvalidEvidence)?;
        let authority = AgentAdmissionAuthority::new(
            rule.principal.clone(),
            rule.granted_scopes.clone(),
            artifact.document.policy.clone(),
            policy_digest,
            evidence,
        )
        .map_err(|_| AgentServiceAuthorizationError::InvalidEvidence)?;
        Ok(AgentServiceSubmissionGrant::new(authority, vec![budget]))
    }

    fn run(
        &self,
        context: &AgentServiceRunAuthorization,
    ) -> Result<AgentServiceRunGrant, AgentServiceAuthorizationError> {
        let artifact = self
            .current()
            .map_err(|_| AgentServiceAuthorizationError::Unavailable)?;
        let operation = match context.operation() {
            AgentServiceRunOperation::Read => RunPermission::Read,
            AgentServiceRunOperation::Cancel => RunPermission::Cancel,
        };
        let target = match context.target() {
            AgentServiceRunTarget::Run(id) => RunAccessTarget::Run(*id),
            AgentServiceRunTarget::Submission(digest) => RunAccessTarget::Submission(*digest),
        };
        let index = artifact
            .document
            .runs
            .iter()
            .position(|rule| {
                &rule.tenant == context.caller().tenant_id()
                    && &rule.principal == context.caller().principal()
                    && rule.operation == operation
                    && (rule.target == target || rule.target == RunAccessTarget::TenantRuns)
            })
            .ok_or(AgentServiceAuthorizationError::Denied)?;
        let policy_digest = artifact.runs[index];
        let decision = checksum(
            b"stateknot.agent-policy-run-decision.v1\0",
            &(
                policy_digest,
                context.caller().tenant_id(),
                context.caller().principal(),
                operation,
                target,
            ),
        )
        .map_err(|_| AgentServiceAuthorizationError::InvalidEvidence)?;
        Ok(AgentServiceRunGrant::new(
            context.caller().principal().clone(),
            artifact.document.policy.clone(),
            policy_digest,
            decision,
        ))
    }
}

impl AgentServiceAuthorizer for AgentResourcePolicy {
    fn authorize_submission(
        &self,
        context: AgentServiceSubmissionAuthorization,
    ) -> BoxFuture<'_, Result<AgentServiceSubmissionGrant, AgentServiceAuthorizationError>> {
        Box::pin(async move { self.submission(&context) })
    }

    fn authorize_run(
        &self,
        context: AgentServiceRunAuthorization,
    ) -> BoxFuture<'_, Result<AgentServiceRunGrant, AgentServiceAuthorizationError>> {
        Box::pin(async move { self.run(&context) })
    }
}

fn now_micros() -> Result<i64, PolicyError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PolicyError)?
            .as_micros(),
    )
    .map_err(|_| PolicyError)
}

#[cfg(test)]
mod tests;
