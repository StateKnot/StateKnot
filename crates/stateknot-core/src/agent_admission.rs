// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Integrity-bound, database-clock-ready Agent admission snapshots.

use std::{collections::BTreeMap, fmt};

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use thiserror::Error;

use crate::{
    AgentDescriptor, AgentRequest, AgentRequestValidationError, AgentResultProvenance,
    AgentResultProvenanceError, BudgetEvaluationError, BudgetLimits, BudgetResolutionError,
    BudgetUsage, ByteCount, CapabilityIdentity, CapabilityLifecycleState, Digest, GraphReference,
    JournalPayload, MAX_BUDGET_LAYERS, PrincipalIdentity, ResolvedBudget, ScopeSet, Timestamp,
};

const INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot.agent-admission-intent.v1\0";
const ADMISSION_DIGEST_DOMAIN: &[u8] = b"stateknot.agent-admission.v1\0";
const MAX_SNAPSHOT_BYTES_USIZE: usize = 16 * 1024 * 1024;

/// One immutable, attributable budget-policy layer used at admission.
///
/// `source` pins the policy artifact identity while `decision_digest` binds the
/// exact evaluated decision outside this protocol-neutral core. The limits are
/// still re-resolved locally; a decision digest can never widen them.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentAdmissionBudgetLayer {
    source: CapabilityIdentity,
    decision_digest: Digest,
    limits: BudgetLimits,
}

impl AgentAdmissionBudgetLayer {
    /// Constructs one non-empty, version-pinned budget policy layer.
    ///
    /// # Errors
    ///
    /// Returns [`AgentAdmissionBudgetLayerError::EmptyLimits`] when the policy
    /// contributes no restriction and would therefore create ambiguous audit
    /// evidence.
    pub fn new(
        source: CapabilityIdentity,
        decision_digest: Digest,
        limits: BudgetLimits,
    ) -> Result<Self, AgentAdmissionBudgetLayerError> {
        if limits.is_empty() {
            return Err(AgentAdmissionBudgetLayerError::EmptyLimits);
        }
        Ok(Self {
            source,
            decision_digest,
            limits,
        })
    }

    /// Returns the owner-qualified, version-pinned policy source.
    #[must_use]
    pub const fn source(&self) -> &CapabilityIdentity {
        &self.source
    }

    /// Returns the digest of the exact external policy decision.
    #[must_use]
    pub const fn decision_digest(&self) -> Digest {
        self.decision_digest
    }

    /// Returns the restrictions contributed by this policy source.
    #[must_use]
    pub const fn limits(&self) -> &BudgetLimits {
        &self.limits
    }
}

impl fmt::Debug for AgentAdmissionBudgetLayer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentAdmissionBudgetLayer")
            .field("source", &self.source)
            .field("decision_digest", &self.decision_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for AgentAdmissionBudgetLayer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            source: CapabilityIdentity,
            decision_digest: Digest,
            limits: BudgetLimits,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.source, wire.decision_digest, wire.limits).map_err(de::Error::custom)
    }
}

/// Invalid immutable budget policy layer.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentAdmissionBudgetLayerError {
    /// A durable policy layer must contribute at least one restriction.
    #[error("agent admission budget layer must not be empty")]
    EmptyLimits,
}

/// Authenticated principal and policy decision that authorized one new run.
///
/// This value is an audit snapshot, not a signature verifier. A trusted
/// control plane authenticates the principal, evaluates the pinned policy and
/// validates `evidence` against its offline schema before constructing it.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentAdmissionAuthority {
    principal: PrincipalIdentity,
    granted_scopes: ScopeSet,
    policy: CapabilityIdentity,
    policy_digest: Digest,
    evidence: JournalPayload,
}

impl AgentAdmissionAuthority {
    /// Stable event kind required for a granted admission decision.
    pub const EVIDENCE_KIND: &'static str = "agent-admission-granted";

    /// Constructs one granted, schema-pinned policy decision snapshot.
    ///
    /// # Errors
    ///
    /// Rejects evidence that does not explicitly represent a granted Agent
    /// admission. Schema evaluation remains the trusted registry's duty.
    pub fn new(
        principal: PrincipalIdentity,
        granted_scopes: ScopeSet,
        policy: CapabilityIdentity,
        policy_digest: Digest,
        evidence: JournalPayload,
    ) -> Result<Self, AgentAdmissionAuthorityError> {
        if evidence.kind().as_str() != Self::EVIDENCE_KIND {
            return Err(AgentAdmissionAuthorityError::WrongEvidenceKind);
        }
        Ok(Self {
            principal,
            granted_scopes,
            policy,
            policy_digest,
            evidence,
        })
    }

    /// Returns the exact authenticated external principal.
    #[must_use]
    pub const fn principal(&self) -> &PrincipalIdentity {
        &self.principal
    }

    /// Returns the scopes granted to this run after policy narrowing.
    #[must_use]
    pub const fn granted_scopes(&self) -> &ScopeSet {
        &self.granted_scopes
    }

    /// Returns the owner-qualified admission policy version.
    #[must_use]
    pub const fn policy(&self) -> &CapabilityIdentity {
        &self.policy
    }

    /// Returns the immutable policy artifact digest.
    #[must_use]
    pub const fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// Returns bounded, schema-pinned decision evidence.
    #[must_use]
    pub const fn evidence(&self) -> &JournalPayload {
        &self.evidence
    }
}

impl fmt::Debug for AgentAdmissionAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentAdmissionAuthority")
            .field("principal", &self.principal)
            .field("granted_scope_count", &self.granted_scopes.len())
            .field("policy", &self.policy)
            .field("policy_digest", &self.policy_digest)
            .field("evidence_schema", self.evidence.schema())
            .field("evidence_digest", &self.evidence.digest())
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for AgentAdmissionAuthority {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            principal: PrincipalIdentity,
            granted_scopes: ScopeSet,
            policy: CapabilityIdentity,
            policy_digest: Digest,
            evidence: JournalPayload,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.principal,
            wire.granted_scopes,
            wire.policy,
            wire.policy_digest,
            wire.evidence,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid trusted authorization snapshot.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentAdmissionAuthorityError {
    /// Evidence described another policy outcome.
    #[error("agent admission authority evidence must have kind agent-admission-granted")]
    WrongEvidenceKind,
}

/// Caller-controlled, integrity-bound input to database-time admission.
///
/// Construction freezes every executable definition, request, policy layer,
/// authorization decision, resolved finite budget and graph version. It does
/// not claim that request/evidence JSON passed application schemas; the trusted
/// offline registries must prove those checks before calling the store.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentAdmissionIntent {
    provenance: AgentResultProvenance,
    descriptor: AgentDescriptor,
    request: AgentRequest,
    #[schemars(length(max = 16))]
    budget_layers: Box<[AgentAdmissionBudgetLayer]>,
    budget: ResolvedBudget,
    graph: GraphReference,
    authority: AgentAdmissionAuthority,
    intent_digest: Digest,
}

impl AgentAdmissionIntent {
    /// Maximum canonical intent or committed admission snapshot size.
    pub const MAX_SNAPSHOT_BYTES: ByteCount = ByteCount::new(MAX_SNAPSHOT_BYTES_USIZE as u64);

    /// Constructs a complete pre-commit admission intent.
    ///
    /// Budget layers are sorted by source identity and must be unique. The
    /// resolved budget is derived internally from those layers plus immutable
    /// Agent and request restrictions, so callers cannot inject a wider
    /// recovery budget.
    ///
    /// # Errors
    ///
    /// Rejects retired/substituted Agents, incomplete or excessive budget
    /// layers, request/schema drift, insufficient granted scopes, non-I-JSON
    /// integrity material, or an oversized snapshot.
    pub fn new<I>(
        provenance: AgentResultProvenance,
        descriptor: AgentDescriptor,
        request: AgentRequest,
        budget_layers: I,
        graph: GraphReference,
        authority: AgentAdmissionAuthority,
    ) -> Result<Self, AgentAdmissionIntentError>
    where
        I: IntoIterator<Item = AgentAdmissionBudgetLayer>,
    {
        Self::build(
            provenance,
            descriptor,
            request,
            budget_layers,
            None,
            graph,
            authority,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build<I>(
        provenance: AgentResultProvenance,
        descriptor: AgentDescriptor,
        request: AgentRequest,
        budget_layers: I,
        supplied_budget: Option<ResolvedBudget>,
        graph: GraphReference,
        authority: AgentAdmissionAuthority,
        supplied_intent_digest: Option<Digest>,
    ) -> Result<Self, AgentAdmissionIntentError>
    where
        I: IntoIterator<Item = AgentAdmissionBudgetLayer>,
    {
        if descriptor.metadata().lifecycle().state() == CapabilityLifecycleState::Retired {
            return Err(AgentAdmissionIntentError::RetiredAgent);
        }
        provenance
            .validate_for(&descriptor)
            .map_err(AgentAdmissionIntentError::provenance)?;
        if !descriptor
            .metadata()
            .required_scopes()
            .is_subset(authority.granted_scopes())
        {
            return Err(AgentAdmissionIntentError::InsufficientGrantedScopes);
        }

        let additional_layers = usize::from(!descriptor.budget_limits().is_empty())
            + usize::from(!request.budget_limits().is_empty());
        let maximum_base_layers = MAX_BUDGET_LAYERS
            .checked_sub(additional_layers)
            .expect("at most two built-in admission budget layers exist");
        let mut ordered_layers = BTreeMap::new();
        for layer in budget_layers {
            if ordered_layers.len() == maximum_base_layers {
                return Err(AgentAdmissionIntentError::TooManyBudgetLayers {
                    maximum: MAX_BUDGET_LAYERS,
                    actual: MAX_BUDGET_LAYERS + 1,
                });
            }
            let source = layer.source().clone();
            if ordered_layers.insert(source.clone(), layer).is_some() {
                return Err(AgentAdmissionIntentError::DuplicateBudgetLayerSource {
                    policy_source: Box::new(source),
                });
            }
        }
        let budget_layers = ordered_layers.into_values().collect::<Vec<_>>();
        let mut limits = budget_layers
            .iter()
            .map(|layer| layer.limits().clone())
            .collect::<Vec<_>>();
        if !descriptor.budget_limits().is_empty() {
            limits.push(descriptor.budget_limits().clone());
        }
        if !request.budget_limits().is_empty() {
            limits.push(request.budget_limits().clone());
        }
        let budget = ResolvedBudget::resolve(&limits)
            .map_err(AgentAdmissionIntentError::budget_resolution)?;
        if supplied_budget.is_some_and(|supplied| supplied != budget) {
            return Err(AgentAdmissionIntentError::ResolvedBudgetMismatch);
        }
        request
            .validate_with_resolved_budget(&descriptor, &budget)
            .map_err(AgentAdmissionIntentError::request)?;

        let mut intent = Self {
            provenance,
            descriptor,
            request,
            budget_layers: budget_layers.into_boxed_slice(),
            budget,
            graph,
            authority,
            intent_digest: Digest::sha256([]),
        };
        intent.intent_digest = intent.compute_digest()?;
        if supplied_intent_digest.is_some_and(|supplied| supplied != intent.intent_digest) {
            return Err(AgentAdmissionIntentError::IntentDigestMismatch);
        }
        intent.ensure_size()?;
        Ok(intent)
    }

    fn compute_digest(&self) -> Result<Digest, AgentAdmissionIntentError> {
        #[derive(Serialize)]
        struct Wire<'a> {
            provenance: &'a AgentResultProvenance,
            descriptor: &'a AgentDescriptor,
            request: &'a AgentRequest,
            budget_layers: &'a [AgentAdmissionBudgetLayer],
            budget: &'a ResolvedBudget,
            graph: &'a GraphReference,
            authority: &'a AgentAdmissionAuthority,
        }

        let canonical = canonical_bytes(&Wire {
            provenance: &self.provenance,
            descriptor: &self.descriptor,
            request: &self.request,
            budget_layers: &self.budget_layers,
            budget: &self.budget,
            graph: &self.graph,
            authority: &self.authority,
        })?;
        Ok(domain_digest(INTENT_DIGEST_DOMAIN, &canonical))
    }

    fn ensure_size(&self) -> Result<(), AgentAdmissionIntentError> {
        let bytes = canonical_bytes(self)?;
        if bytes.len() > MAX_SNAPSHOT_BYTES_USIZE {
            return Err(AgentAdmissionIntentError::SnapshotTooLarge {
                maximum: Self::MAX_SNAPSHOT_BYTES,
                actual: byte_count(bytes.len())?,
            });
        }
        Ok(())
    }

    /// Returns trusted tenant/run/thread/invocation and Agent identity.
    #[must_use]
    pub const fn provenance(&self) -> &AgentResultProvenance {
        &self.provenance
    }

    /// Returns the immutable executable Agent definition snapshot.
    #[must_use]
    pub const fn descriptor(&self) -> &AgentDescriptor {
        &self.descriptor
    }

    /// Returns schema-pinned bounded run input and request restrictions.
    #[must_use]
    pub const fn request(&self) -> &AgentRequest {
        &self.request
    }

    /// Returns canonically ordered, attributable base budget layers.
    #[must_use]
    pub const fn budget_layers(&self) -> &[AgentAdmissionBudgetLayer] {
        &self.budget_layers
    }

    /// Returns the exact finite budget derived from all snapshotted layers.
    #[must_use]
    pub const fn budget(&self) -> &ResolvedBudget {
        &self.budget
    }

    /// Returns the immutable compiled graph and state schema reference.
    #[must_use]
    pub const fn graph(&self) -> &GraphReference {
        &self.graph
    }

    /// Returns authenticated principal, scopes and policy evidence.
    #[must_use]
    pub const fn authority(&self) -> &AgentAdmissionAuthority {
        &self.authority
    }

    /// Returns the domain-separated checksum of every caller-controlled field.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }
}

impl fmt::Debug for AgentAdmissionIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentAdmissionIntent")
            .field("provenance", &self.provenance)
            .field("agent", self.descriptor.metadata().identity())
            .field("input_schema", self.request.input_schema())
            .field("input_stats", &self.request.input().stats())
            .field("budget_layer_count", &self.budget_layers.len())
            .field("graph", &self.graph)
            .field("authority", &self.authority)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for AgentAdmissionIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            provenance: AgentResultProvenance,
            descriptor: AgentDescriptor,
            request: AgentRequest,
            budget_layers: Vec<AgentAdmissionBudgetLayer>,
            budget: ResolvedBudget,
            graph: GraphReference,
            authority: AgentAdmissionAuthority,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::build(
            wire.provenance,
            wire.descriptor,
            wire.request,
            wire.budget_layers,
            Some(wire.budget),
            wire.graph,
            wire.authority,
            Some(wire.intent_digest),
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid or corrupted pre-commit Agent admission input.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentAdmissionIntentError {
    /// Retired definition snapshots cannot accept new work.
    #[error("retired agent definitions cannot be admitted")]
    RetiredAgent,
    /// Trusted run provenance named another Agent.
    #[error("agent admission provenance is invalid: {source}")]
    Provenance {
        /// Underlying exact identity mismatch.
        #[source]
        source: AgentResultProvenanceError,
    },
    /// Authorization did not cover every immutable Agent requirement.
    #[error("agent admission granted scopes do not cover the descriptor requirements")]
    InsufficientGrantedScopes,
    /// Two budget decisions claimed the same version-pinned policy source.
    #[error("agent admission repeats budget policy source {policy_source:?}")]
    DuplicateBudgetLayerSource {
        /// Repeated source identity.
        policy_source: Box<CapabilityIdentity>,
    },
    /// Base, Agent and request layers exceeded the hard core ceiling.
    #[error("agent admission has at least {actual} budget layers; maximum is {maximum}")]
    TooManyBudgetLayers {
        /// Immutable total layer ceiling.
        maximum: usize,
        /// First observed total above the ceiling.
        actual: usize,
    },
    /// The complete policy stack did not resolve every finite dimension.
    #[error("agent admission budget resolution failed: {source}")]
    BudgetResolution {
        /// Underlying finite-budget failure.
        #[source]
        source: BudgetResolutionError,
    },
    /// Request identity or input bytes contradicted the resolved snapshot.
    #[error("agent admission request is invalid: {source}")]
    Request {
        /// Underlying request relationship failure.
        #[source]
        source: AgentRequestValidationError,
    },
    /// Persisted resolved fields did not equal deterministic re-resolution.
    #[error("agent admission resolved budget does not match its policy layers")]
    ResolvedBudgetMismatch,
    /// Persisted intent digest did not match canonical fields.
    #[error("agent admission intent digest does not match its fields")]
    IntentDigestMismatch,
    /// Integrity-bearing typed data could not be represented as JSON.
    #[error("agent admission integrity serialization failed")]
    IntegritySerialization,
    /// Integrity material contained a non-interoperable integer.
    #[error("agent admission contains an integer outside the I-JSON safe range")]
    NonInteroperableNumber,
    /// Canonical snapshot exceeded its immutable storage boundary.
    #[error("agent admission snapshot is {actual}; maximum is {maximum}")]
    SnapshotTooLarge {
        /// Immutable canonical byte ceiling.
        maximum: ByteCount,
        /// Rejected canonical size.
        actual: ByteCount,
    },
    /// Host byte width could not be represented by portable accounting.
    #[error("agent admission snapshot byte accounting overflowed")]
    SnapshotBytesOverflow,
}

impl AgentAdmissionIntentError {
    const fn provenance(source: AgentResultProvenanceError) -> Self {
        Self::Provenance { source }
    }

    const fn budget_resolution(source: BudgetResolutionError) -> Self {
        Self::BudgetResolution { source }
    }

    const fn request(source: AgentRequestValidationError) -> Self {
        Self::Request { source }
    }
}

/// Fully committed Agent admission using the authoritative database clock.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentAdmission {
    intent: AgentAdmissionIntent,
    admitted_at: Timestamp,
    digest: Digest,
}

impl AgentAdmission {
    /// Stable journal event kind anchoring an atomic Agent admission.
    ///
    /// The event payload schema remains deployment-owned and must be validated
    /// by the trusted control-plane registry. The `PostgreSQL` provider binds the
    /// exact payload, admission snapshot, lifecycle start, and initial
    /// checkpoint through one composite projection digest.
    pub const JOURNAL_EVENT_KIND: &'static str = "agent-admitted";

    /// Materializes one intent at its durable database observation.
    ///
    /// # Errors
    ///
    /// Rejects a deadline reached at commit, non-canonical integrity material,
    /// or an oversized complete snapshot.
    pub fn commit(
        intent: AgentAdmissionIntent,
        admitted_at: Timestamp,
    ) -> Result<Self, AgentAdmissionError> {
        intent
            .budget()
            .remaining(&BudgetUsage::zero(), admitted_at)
            .map_err(AgentAdmissionError::budget_evaluation)?;
        let canonical = canonical_bytes(&AdmissionDigestWire {
            intent_digest: intent.intent_digest(),
            admitted_at,
        })
        .map_err(|error| AgentAdmissionError::from_intent_integrity(&error))?;
        let digest = domain_digest(ADMISSION_DIGEST_DOMAIN, &canonical);
        let admission = Self {
            intent,
            admitted_at,
            digest,
        };
        let bytes = admission.canonical_bytes()?;
        if bytes.len() > MAX_SNAPSHOT_BYTES_USIZE {
            return Err(AgentAdmissionError::SnapshotTooLarge {
                maximum: AgentAdmissionIntent::MAX_SNAPSHOT_BYTES,
                actual: byte_count(bytes.len())
                    .map_err(|error| AgentAdmissionError::from_intent_integrity(&error))?,
            });
        }
        Ok(admission)
    }

    /// Returns every caller-controlled, integrity-bound admission field.
    #[must_use]
    pub const fn intent(&self) -> &AgentAdmissionIntent {
        &self.intent
    }

    /// Returns the authoritative database commit observation.
    #[must_use]
    pub const fn admitted_at(&self) -> Timestamp {
        self.admitted_at
    }

    /// Returns the complete domain-separated admission checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Encodes the complete snapshot as deterministic canonical JSON.
    ///
    /// # Errors
    ///
    /// Returns an integrity error rather than emitting non-I-JSON data.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, AgentAdmissionError> {
        canonical_bytes(self).map_err(|error| AgentAdmissionError::from_intent_integrity(&error))
    }
}

impl fmt::Debug for AgentAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentAdmission")
            .field("intent", &self.intent)
            .field("admitted_at", &self.admitted_at)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for AgentAdmission {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: AgentAdmissionIntent,
            admitted_at: Timestamp,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        let admission = Self::commit(wire.intent, wire.admitted_at).map_err(de::Error::custom)?;
        if admission.digest != wire.digest {
            return Err(de::Error::custom(AgentAdmissionError::DigestMismatch));
        }
        Ok(admission)
    }
}

#[derive(Serialize)]
struct AdmissionDigestWire {
    intent_digest: Digest,
    admitted_at: Timestamp,
}

/// Invalid or corrupted committed Agent admission.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentAdmissionError {
    /// The finite deadline or another zero-usage dimension was exhausted.
    #[error("agent admission is not admissible at database commit: {source}")]
    BudgetEvaluation {
        /// Exact budget evaluation failure.
        #[source]
        source: BudgetEvaluationError,
    },
    /// Integrity-bearing typed data could not be represented as JSON.
    #[error("agent admission integrity serialization failed")]
    IntegritySerialization,
    /// Integrity material contained a non-interoperable integer.
    #[error("agent admission contains an integer outside the I-JSON safe range")]
    NonInteroperableNumber,
    /// Complete canonical admission bytes exceeded the storage boundary.
    #[error("agent admission snapshot is {actual}; maximum is {maximum}")]
    SnapshotTooLarge {
        /// Immutable canonical byte ceiling.
        maximum: ByteCount,
        /// Rejected canonical size.
        actual: ByteCount,
    },
    /// Host byte width could not be represented by portable accounting.
    #[error("agent admission snapshot byte accounting overflowed")]
    SnapshotBytesOverflow,
    /// Persisted complete digest did not match intent and database time.
    #[error("agent admission digest does not match intent and admitted time")]
    DigestMismatch,
}

impl AgentAdmissionError {
    const fn budget_evaluation(source: BudgetEvaluationError) -> Self {
        Self::BudgetEvaluation { source }
    }

    const fn from_intent_integrity(error: &AgentAdmissionIntentError) -> Self {
        match error {
            AgentAdmissionIntentError::NonInteroperableNumber => Self::NonInteroperableNumber,
            AgentAdmissionIntentError::SnapshotBytesOverflow => Self::SnapshotBytesOverflow,
            _ => Self::IntegritySerialization,
        }
    }
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, AgentAdmissionIntentError> {
    let value = serde_json::to_value(value)
        .map_err(|_| AgentAdmissionIntentError::IntegritySerialization)?;
    validate_i_json_numbers(&value)?;
    serde_json_canonicalizer::to_vec(&value)
        .map_err(|_| AgentAdmissionIntentError::IntegritySerialization)
}

fn validate_i_json_numbers(value: &Value) -> Result<(), AgentAdmissionIntentError> {
    const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    match value {
        Value::Number(number) => {
            if number
                .as_i64()
                .is_some_and(|value| value.unsigned_abs() > MAX_SAFE_INTEGER)
                || number
                    .as_u64()
                    .is_some_and(|value| value > MAX_SAFE_INTEGER)
            {
                return Err(AgentAdmissionIntentError::NonInteroperableNumber);
            }
            Ok(())
        }
        Value::Array(values) => values.iter().try_for_each(validate_i_json_numbers),
        Value::Object(values) => values.values().try_for_each(validate_i_json_numbers),
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
    }
}

fn domain_digest(domain: &[u8], canonical: &[u8]) -> Digest {
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(canonical);
    Digest::sha256(preimage)
}

fn byte_count(value: usize) -> Result<ByteCount, AgentAdmissionIntentError> {
    u64::try_from(value)
        .map(ByteCount::new)
        .map_err(|_| AgentAdmissionIntentError::SnapshotBytesOverflow)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        AgentResultProvenance, BoundedJson, GraphReference, JournalEventKind, SchemaReference,
    };

    const BEFORE_DEADLINE: &str = "2029-12-31T23:59:59.000000Z";
    const AT_DEADLINE: &str = "2030-01-01T00:00:00.000000Z";

    fn fixture_value(path: &str) -> Value {
        let source = match path {
            "agent" => include_str!("../tests/fixtures/core-agent-v1.json"),
            "runtime" => include_str!("../tests/fixtures/core-agent-runtime-v1.json"),
            "checkpoint" => include_str!("../tests/fixtures/core-checkpoint-v1.json"),
            "journal" => include_str!("../tests/fixtures/core-journal-v1.json"),
            _ => unreachable!(),
        };
        serde_json::from_str(source).unwrap()
    }

    fn parts() -> (
        AgentResultProvenance,
        AgentDescriptor,
        AgentRequest,
        AgentAdmissionBudgetLayer,
        GraphReference,
        AgentAdmissionAuthority,
    ) {
        let agent = fixture_value("agent");
        let runtime = fixture_value("runtime");
        let checkpoint = fixture_value("checkpoint");
        let journal = fixture_value("journal");
        let descriptor = serde_json::from_value(agent["descriptors"]["valid"][0].clone()).unwrap();
        let provenance =
            serde_json::from_value(runtime["result_provenances"]["valid"][0].clone()).unwrap();
        let request = serde_json::from_value(runtime["requests"]["valid"][0].clone()).unwrap();
        let graph =
            serde_json::from_value::<GraphReference>(checkpoint["checkpoints"][0]["graph"].clone())
                .unwrap();
        let schema =
            serde_json::from_value::<SchemaReference>(journal["schema_reference"].clone()).unwrap();
        let evidence = JournalPayload::new(
            schema,
            AgentAdmissionAuthority::EVIDENCE_KIND
                .parse::<JournalEventKind>()
                .unwrap(),
            BoundedJson::try_from(json!({"decision": "allow"})).unwrap(),
        )
        .unwrap();
        let policy = graph.identity().clone();
        let authority = AgentAdmissionAuthority::new(
            policy.owner().clone(),
            ScopeSet::empty(),
            policy.clone(),
            Digest::sha256(b"policy artifact"),
            evidence,
        )
        .unwrap();
        let limits = serde_json::from_value(runtime["base_budget_layers"][0].clone()).unwrap();
        let layer =
            AgentAdmissionBudgetLayer::new(policy, authority.evidence().digest(), limits).unwrap();
        (provenance, descriptor, request, layer, graph, authority)
    }

    fn intent() -> AgentAdmissionIntent {
        let (provenance, descriptor, request, layer, graph, authority) = parts();
        AgentAdmissionIntent::new(provenance, descriptor, request, [layer], graph, authority)
            .unwrap()
    }

    #[test]
    fn admission_round_trips_and_revalidates_every_integrity_layer() {
        let admission =
            AgentAdmission::commit(intent(), BEFORE_DEADLINE.parse::<Timestamp>().unwrap())
                .unwrap();
        let wire = serde_json::to_value(&admission).unwrap();
        assert_eq!(
            admission.intent().intent_digest().to_string(),
            "sha256:7e5dff5d4aa9c88b1b3fb2de26fe743c29cb9918c3294187760e9b3cba3d19a1"
        );
        assert_eq!(
            admission.digest().to_string(),
            "sha256:912e66ac145070b6363af01641db91acf6ad536494f4f322f08ce6c6f35138af"
        );
        assert_eq!(
            Digest::sha256(admission.canonical_bytes().unwrap()).to_string(),
            "sha256:f81f9aebe817786e6b6b313d8d2b120662407d31f9a7b849a38a7163ca018bca"
        );
        let decoded = serde_json::from_value::<AgentAdmission>(wire.clone()).unwrap();
        assert_eq!(decoded, admission);
        assert_eq!(
            decoded.canonical_bytes().unwrap(),
            admission.canonical_bytes().unwrap()
        );

        let mut budget_tamper = wire.clone();
        budget_tamper["intent"]["budget"]["output_bytes"] = json!("999999999");
        assert!(serde_json::from_value::<AgentAdmission>(budget_tamper).is_err());

        let mut digest_tamper = wire;
        digest_tamper["digest"] = serde_json::to_value(Digest::sha256(b"substitute")).unwrap();
        assert!(serde_json::from_value::<AgentAdmission>(digest_tamper).is_err());

        let mut unknown_field = serde_json::to_value(&admission).unwrap();
        unknown_field["unexpected"] = json!(true);
        assert!(serde_json::from_value::<AgentAdmission>(unknown_field).is_err());
    }

    #[test]
    fn database_clock_deadline_and_scope_authority_fail_closed() {
        assert!(matches!(
            AgentAdmission::commit(intent(), AT_DEADLINE.parse::<Timestamp>().unwrap()),
            Err(AgentAdmissionError::BudgetEvaluation { .. })
        ));

        let (provenance, mut descriptor, request, layer, graph, authority) = parts();
        let mut descriptor_wire = serde_json::to_value(&descriptor).unwrap();
        descriptor_wire["metadata"]["required_scopes"] = json!(["agent:invoke"]);
        descriptor = serde_json::from_value(descriptor_wire).unwrap();
        assert!(matches!(
            AgentAdmissionIntent::new(provenance, descriptor, request, [layer], graph, authority,),
            Err(AgentAdmissionIntentError::InsufficientGrantedScopes)
        ));
    }

    #[test]
    fn admission_debug_redacts_request_and_policy_payloads() {
        let admission =
            AgentAdmission::commit(intent(), BEFORE_DEADLINE.parse::<Timestamp>().unwrap())
                .unwrap();
        let debug = format!("{admission:?}");
        assert!(!debug.contains("INC-42"));
        assert!(!debug.contains("Summarize the evidence"));
        assert!(!debug.contains("allow"));
        assert!(debug.contains("intent_digest"));
    }
}
