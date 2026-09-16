// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use super::PolicyError;
use serde::{Deserialize, Serialize};
use stateknot_core::{
    BoundedJson, BudgetLimits, CapabilityIdentity, Digest, PrincipalIdentity, RunId,
    SchemaReference, ScopeSet, TenantId, Timestamp,
};

/// One exact admission selector and the restrictions it grants.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubmissionRule {
    /// Trusted tenant, never taken from request JSON.
    pub tenant: TenantId,
    /// Exact authenticated issuer/subject.
    pub principal: PrincipalIdentity,
    /// Exact owner-qualified Agent revision.
    pub agent: CapabilityIdentity,
    /// Exact input schema including its content digest.
    pub input_schema: SchemaReference,
    /// Explicit execution scopes; normal admission checks still apply.
    pub granted_scopes: ScopeSet,
    /// Nonempty restrictive layer, intersected with descriptor/request limits.
    pub budget_limits: BudgetLimits,
}

/// Explicit operation granted by a run rule.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunPermission {
    /// Read snapshots, key lookups and activity.
    Read,
    /// Request cooperative durable cancellation.
    Cancel,
}

/// Exact target or explicitly privileged tenant-operator scope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RunAccessTarget {
    /// One exact run; does not also grant key lookup.
    Run(RunId),
    /// One tenant-bound submission digest; does not also grant run-ID lookup.
    Submission(Digest),
    /// All run IDs and submission digests in this rule's tenant. Use sparingly.
    TenantRuns,
}

/// One explicit run permission. Overlapping selectors are rejected at startup.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunRule {
    /// Trusted tenant boundary.
    pub tenant: TenantId,
    /// Exact authenticated issuer/subject.
    pub principal: PrincipalIdentity,
    /// Read or cancel, never inferred from submission permission.
    pub operation: RunPermission,
    /// Exact resource or explicit tenant-wide operator scope.
    pub target: RunAccessTarget,
}

/// Host-controlled versioned policy configuration, not an authentication token.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyDocument {
    /// Must be 1; unsupported versions fail closed.
    pub format_version: u16,
    /// Identity of this versioned policy implementation/configuration family.
    pub policy: CapabilityIdentity,
    /// Absolute expiration retained across process restarts.
    pub valid_until: Timestamp,
    /// Explicit admission rules; empty denies all submissions.
    pub submissions: Vec<SubmissionRule>,
    /// Explicit run rules; empty denies all reads/cancellations.
    pub runs: Vec<RunRule>,
}

/// Validated canonical policy artifact. Retain bytes privately before installation.
/// Its digest is not proof of origin; authenticate the configuration source.
#[derive(Clone)]
pub struct PolicyArtifact {
    pub(super) document: PolicyDocument,
    canonical: Vec<u8>,
    digest: Digest,
    pub(super) submissions: Vec<Digest>,
    pub(super) runs: Vec<Digest>,
}

impl PolicyArtifact {
    /// Maximum total rules; bounded JSON may impose a lower effective ceiling.
    pub const MAX_RULES: usize = 1024;

    /// Validates trusted configuration and computes normalized canonical bytes.
    /// No clock check is performed until installation into a live policy.
    ///
    /// # Errors
    /// Rejects unknown versions, oversized configuration, empty budgets and
    /// duplicate/overlapping selectors. Rule ordering never resolves ambiguity.
    pub fn new(document: PolicyDocument) -> Result<Self, PolicyError> {
        if document.format_version != 1
            || document
                .submissions
                .len()
                .saturating_add(document.runs.len())
                > Self::MAX_RULES
        {
            return Err(PolicyError);
        }
        // Validate structure before canonicalizing or retaining any rule copies.
        let value = serde_json::to_value(&document).map_err(|_| PolicyError)?;
        BoundedJson::try_from_value(value).map_err(|_| PolicyError)?;
        for (i, rule) in document.submissions.iter().enumerate() {
            if rule.budget_limits.is_empty()
                || document.submissions[..i].iter().any(|old| {
                    old.tenant == rule.tenant
                        && old.principal == rule.principal
                        && old.agent == rule.agent
                        && old.input_schema == rule.input_schema
                })
            {
                return Err(PolicyError);
            }
        }
        for (i, rule) in document.runs.iter().enumerate() {
            if document.runs[..i].iter().any(|old| {
                old.tenant == rule.tenant
                    && old.principal == rule.principal
                    && old.operation == rule.operation
                    && (old.target == rule.target
                        || old.target == RunAccessTarget::TenantRuns
                        || rule.target == RunAccessTarget::TenantRuns)
            }) {
                return Err(PolicyError);
            }
            if rule.operation == RunPermission::Cancel
                && matches!(rule.target, RunAccessTarget::Submission(_))
            {
                // The service has no cancellation-by-key endpoint.
                return Err(PolicyError);
            }
        }
        let canonical = serde_json_canonicalizer::to_vec(&document).map_err(|_| PolicyError)?;
        let digest = Digest::sha256(&canonical);
        let submissions = document
            .submissions
            .iter()
            .map(|rule| {
                checksum(
                    b"stateknot.agent-policy-submission-rule.v1\0",
                    &(&document.policy, rule),
                )
            })
            .collect::<Result<_, _>>()?;
        let runs = document
            .runs
            .iter()
            .map(|rule| {
                checksum(
                    b"stateknot.agent-policy-run-rule.v1\0",
                    &(&document.policy, rule),
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            document,
            canonical,
            digest,
            submissions,
            runs,
        })
    }

    /// Loads bounded duplicate-free JSON and verifies its normalized digest.
    /// The expected digest must come from an independently trusted manifest.
    ///
    /// # Errors
    /// Rejects malformed/unknown/oversized configuration or checksum mismatch.
    pub fn from_json(bytes: &[u8], expected: Digest) -> Result<Self, PolicyError> {
        let bounded = BoundedJson::from_slice(bytes).map_err(|_| PolicyError)?;
        let document = serde_json::from_value(bounded.into_value()).map_err(|_| PolicyError)?;
        let artifact = Self::new(document)?;
        if artifact.digest != expected {
            return Err(PolicyError);
        }
        Ok(artifact)
    }

    /// Returns the complete normalized artifact checksum, not a selected-rule digest.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns private canonical configuration bytes for immutable retention.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }
}

pub(super) fn checksum(domain: &[u8], value: &impl Serialize) -> Result<Digest, PolicyError> {
    let mut bytes = domain.to_vec();
    bytes.extend(serde_json_canonicalizer::to_vec(value).map_err(|_| PolicyError)?);
    Ok(Digest::sha256(bytes))
}
