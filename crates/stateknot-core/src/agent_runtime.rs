// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Runtime-neutral agent admission requests and successful terminal results.

use std::fmt;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{
    AgentDescriptor, ArtifactRef, BoundedJson, BudgetEvaluationError, BudgetLimits,
    BudgetResolutionError, BudgetUsage, ByteCount, CapabilityIdentity, CapabilityLifecycleState,
    InvocationId, MAX_BUDGET_LAYERS, ResolvedBudget, RunId, SchemaReference, TenantId, ThreadId,
    Timestamp,
};

/// Schema-bound input and request-local budget constraints for one agent run.
///
/// A request deliberately contains no tenant, run, thread, or invocation ID;
/// those trusted identifiers are assigned by run admission. `budget_limits` is
/// another restrictive layer and can never widen system, tenant, policy, or
/// immutable agent limits. Input JSON is structurally bounded here, while a
/// trusted local schema registry performs digest-pinned schema evaluation.
#[derive(Clone, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRequest {
    input_schema: SchemaReference,
    input: BoundedJson,
    budget_limits: BudgetLimits,
}

impl AgentRequest {
    /// Constructs an intrinsically bounded agent request.
    #[must_use]
    pub const fn new(
        input_schema: SchemaReference,
        input: BoundedJson,
        budget_limits: BudgetLimits,
    ) -> Self {
        Self {
            input_schema,
            input,
            budget_limits,
        }
    }

    /// Returns the exact input schema identity.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaReference {
        &self.input_schema
    }

    /// Returns the bounded input without permitting mutation.
    #[must_use]
    pub const fn input(&self) -> &BoundedJson {
        &self.input
    }

    /// Returns the optional request-local budget layer.
    #[must_use]
    pub const fn budget_limits(&self) -> &BudgetLimits {
        &self.budget_limits
    }

    /// Consumes this request into schema, input, and budget components.
    #[must_use]
    pub fn into_parts(self) -> (SchemaReference, BoundedJson, BudgetLimits) {
        (self.input_schema, self.input, self.budget_limits)
    }

    /// Validates a new invocation and resolves its complete finite budget.
    ///
    /// `base_budget_layers` contains only the already selected system, tenant,
    /// and policy layers. This method appends the immutable agent layer and this
    /// request layer, evaluates the supplied durable clock observation, and
    /// checks the materialized input against the resulting byte ceiling.
    /// Deprecated agents remain invocable; retired agents remain readable for
    /// recovery but cannot be admitted as new work.
    ///
    /// Digest-pinned JSON Schema evaluation remains a separate trusted-registry
    /// operation and must complete before the returned budget is committed.
    ///
    /// # Errors
    ///
    /// Returns [`AgentRequestValidationError`] for retirement, schema
    /// substitution, excessive layer count, incomplete/invalid budget
    /// resolution, an already expired deadline, or oversized input.
    pub fn resolve_for(
        &self,
        descriptor: &AgentDescriptor,
        base_budget_layers: &[BudgetLimits],
        observed_at: Timestamp,
    ) -> Result<ResolvedBudget, AgentRequestValidationError> {
        if descriptor.metadata().lifecycle().state() == CapabilityLifecycleState::Retired {
            return Err(AgentRequestValidationError::RetiredAgent {
                identity: Box::new(descriptor.metadata().identity().clone()),
            });
        }

        self.validate_schema_for(descriptor)?;

        let additional_layers = usize::from(!descriptor.budget_limits().is_empty())
            + usize::from(!self.budget_limits.is_empty());
        let actual_layers = base_budget_layers
            .len()
            .checked_add(additional_layers)
            .unwrap_or(usize::MAX);
        if actual_layers > MAX_BUDGET_LAYERS {
            return Err(AgentRequestValidationError::TooManyBudgetLayers {
                maximum: MAX_BUDGET_LAYERS,
                actual: actual_layers,
            });
        }

        let mut layers = Vec::with_capacity(actual_layers);
        layers.extend_from_slice(base_budget_layers);
        if !descriptor.budget_limits().is_empty() {
            layers.push(descriptor.budget_limits().clone());
        }
        if !self.budget_limits.is_empty() {
            layers.push(self.budget_limits.clone());
        }

        let budget = ResolvedBudget::resolve(&layers)
            .map_err(AgentRequestValidationError::budget_resolution)?;
        budget
            .remaining(&BudgetUsage::zero(), observed_at)
            .map_err(AgentRequestValidationError::budget_evaluation)?;
        self.validate_with_resolved_budget(descriptor, &budget)?;
        Ok(budget)
    }

    /// Revalidates schema identity and input bytes against a trusted budget.
    ///
    /// This is intended for recovery after the exact resolved budget was
    /// durably snapshotted by [`Self::resolve_for`]. It intentionally permits a
    /// now-retired descriptor because retirement cannot invalidate old runs.
    ///
    /// # Errors
    ///
    /// Returns [`AgentRequestValidationError`] for schema substitution or input
    /// beyond the snapshotted budget ceiling.
    pub fn validate_with_resolved_budget(
        &self,
        descriptor: &AgentDescriptor,
        budget: &ResolvedBudget,
    ) -> Result<(), AgentRequestValidationError> {
        self.validate_schema_for(descriptor)?;
        let actual = byte_count_from_usize(self.input.stats().compact_bytes());
        let maximum = budget.input_bytes();
        if actual > maximum {
            return Err(AgentRequestValidationError::InputLimitExceeded { maximum, actual });
        }
        Ok(())
    }

    fn validate_schema_for(
        &self,
        descriptor: &AgentDescriptor,
    ) -> Result<(), AgentRequestValidationError> {
        if &self.input_schema != descriptor.input_schema() {
            return Err(AgentRequestValidationError::SchemaMismatch {
                expected: Box::new(descriptor.input_schema().clone()),
                actual: Box::new(self.input_schema.clone()),
            });
        }
        Ok(())
    }
}

impl fmt::Debug for AgentRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentRequest")
            .field("input_schema", &self.input_schema)
            .field("input_stats", &self.input.stats())
            .field("has_budget_limits", &!self.budget_limits.is_empty())
            .finish_non_exhaustive()
    }
}

/// Invalid relationship between an agent request and an admission snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentRequestValidationError {
    /// Historical descriptors cannot be selected for new work.
    #[error("agent {identity:?} is retired and cannot accept a new request")]
    RetiredAgent {
        /// Retired owner-qualified agent identity.
        identity: Box<CapabilityIdentity>,
    },
    /// Input named a schema other than the descriptor snapshot.
    #[error("agent input schema {actual:?} does not match descriptor {expected:?}")]
    SchemaMismatch {
        /// Exact descriptor input schema.
        expected: Box<SchemaReference>,
        /// Rejected request input schema.
        actual: Box<SchemaReference>,
    },
    /// Base, agent, and request layers exceeded the resolution hard bound.
    #[error("agent admission has {actual} budget layers; maximum is {maximum}")]
    TooManyBudgetLayers {
        /// Core layer-count ceiling.
        maximum: usize,
        /// Total non-empty layers that would be resolved.
        actual: usize,
    },
    /// Layered budget resolution failed closed.
    #[error("agent request budget resolution failed: {source}")]
    BudgetResolution {
        /// Underlying finite-budget failure.
        #[source]
        source: BudgetResolutionError,
    },
    /// The resolved budget was already exhausted at admission.
    #[error("agent request budget is not admissible: {source}")]
    BudgetEvaluation {
        /// Underlying deadline or zero-usage evaluation failure.
        #[source]
        source: BudgetEvaluationError,
    },
    /// Compact request input exceeded the resolved run ceiling.
    #[error("agent input is {actual} bytes; resolved budget maximum is {maximum}")]
    InputLimitExceeded {
        /// Resolved cumulative input-byte ceiling.
        maximum: ByteCount,
        /// Exact compact request size.
        actual: ByteCount,
    },
}

impl AgentRequestValidationError {
    const fn budget_resolution(source: BudgetResolutionError) -> Self {
        Self::BudgetResolution { source }
    }

    const fn budget_evaluation(source: BudgetEvaluationError) -> Self {
        Self::BudgetEvaluation { source }
    }
}

/// Trusted execution identity attached to one successful agent result.
#[derive(Clone, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResultProvenance {
    tenant_id: TenantId,
    run_id: RunId,
    thread_id: ThreadId,
    invocation_id: InvocationId,
    agent: CapabilityIdentity,
}

impl AgentResultProvenance {
    /// Constructs exact agent-invocation provenance from trusted admission IDs.
    #[must_use]
    pub const fn new(
        tenant_id: TenantId,
        run_id: RunId,
        thread_id: ThreadId,
        invocation_id: InvocationId,
        agent: CapabilityIdentity,
    ) -> Self {
        Self {
            tenant_id,
            run_id,
            thread_id,
            invocation_id,
            agent,
        }
    }

    /// Constructs provenance bound to an immutable agent descriptor.
    #[must_use]
    pub fn for_agent(
        tenant_id: TenantId,
        run_id: RunId,
        thread_id: ThreadId,
        invocation_id: InvocationId,
        descriptor: &AgentDescriptor,
    ) -> Self {
        Self::new(
            tenant_id,
            run_id,
            thread_id,
            invocation_id,
            descriptor.metadata().identity().clone(),
        )
    }

    /// Returns the tenant boundary for storage and authorization.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the enclosing durable run identifier.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the enclosing conversation thread identifier.
    #[must_use]
    pub const fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    /// Returns the logical root or nested-agent invocation identifier.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the exact owner-qualified agent version.
    #[must_use]
    pub const fn agent(&self) -> &CapabilityIdentity {
        &self.agent
    }

    /// Rebinds this provenance to an immutable descriptor snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`AgentResultProvenanceError`] when the agent identity differs.
    pub fn validate_for(
        &self,
        descriptor: &AgentDescriptor,
    ) -> Result<(), AgentResultProvenanceError> {
        let expected = descriptor.metadata().identity();
        if &self.agent != expected {
            return Err(AgentResultProvenanceError::AgentIdentityMismatch {
                expected: Box::new(expected.clone()),
                actual: Box::new(self.agent.clone()),
            });
        }
        Ok(())
    }
}

impl fmt::Debug for AgentResultProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentResultProvenance")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("thread_id", &self.thread_id)
            .field("invocation_id", &self.invocation_id)
            .field("agent", &self.agent)
            .finish_non_exhaustive()
    }
}

/// Descriptor-binding failure for trusted agent result provenance.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentResultProvenanceError {
    /// Provenance named another owner-qualified agent version.
    #[error("agent result identity {actual:?} does not match descriptor {expected:?}")]
    AgentIdentityMismatch {
        /// Exact descriptor identity.
        expected: Box<CapabilityIdentity>,
        /// Rejected provenance identity.
        actual: Box<CapabilityIdentity>,
    },
}

/// Canonical bounded final artifact references returned by one agent invocation.
///
/// Artifact bytes remain external. Values retain semantic output order while
/// tenant-qualified identities must be unique. The hard count protects generic
/// deserialization before a run budget is available; the budget can only
/// narrow aggregate artifact bytes.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct AgentArtifacts {
    values: Box<[ArtifactRef]>,
    total_bytes: ByteCount,
}

impl AgentArtifacts {
    /// Absolute v1 artifact-reference count for one successful agent result.
    pub const MAX_LEN: usize = 64;

    /// Constructs an empty final artifact set.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Validates count, identity uniqueness, and byte arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`AgentArtifactsError`] for too many references, duplicate
    /// tenant-qualified identities, or aggregate byte overflow.
    pub fn try_new<I>(values: I) -> Result<Self, AgentArtifactsError>
    where
        I: IntoIterator<Item = ArtifactRef>,
    {
        let mut collected = Vec::new();
        let mut total_bytes = ByteCount::ZERO;
        for value in values {
            if collected.len() == Self::MAX_LEN {
                return Err(AgentArtifactsError::TooMany {
                    maximum: Self::MAX_LEN,
                    actual: Self::MAX_LEN + 1,
                });
            }
            if collected
                .iter()
                .any(|existing: &ArtifactRef| existing.identity() == value.identity())
            {
                return Err(AgentArtifactsError::DuplicateIdentity);
            }
            total_bytes = total_bytes
                .checked_add(value.representation().byte_length())
                .ok_or(AgentArtifactsError::TotalBytesOverflow)?;
            collected.push(value);
        }
        Ok(Self {
            values: collected.into_boxed_slice(),
            total_bytes,
        })
    }

    /// Returns the number of final artifact references.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether the result has no final artifact references.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns checked aggregate immutable representation bytes.
    #[must_use]
    pub const fn total_bytes(&self) -> ByteCount {
        self.total_bytes
    }

    /// Iterates in semantic result order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &ArtifactRef> {
        self.values.iter()
    }

    /// Consumes this set into ordered artifact references.
    #[must_use]
    pub fn into_vec(self) -> Vec<ArtifactRef> {
        self.values.into_vec()
    }
}

impl fmt::Debug for AgentArtifacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentArtifacts")
            .field("count", &self.len())
            .field("total_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

impl Serialize for AgentArtifacts {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AgentArtifacts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(AgentArtifactsVisitor)
    }
}

struct AgentArtifactsVisitor;

impl<'de> de::Visitor<'de> for AgentArtifactsVisitor {
    type Value = AgentArtifacts;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "at most {} unique final agent artifact references",
            AgentArtifacts::MAX_LEN
        )
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(
            sequence
                .size_hint()
                .unwrap_or_default()
                .min(AgentArtifacts::MAX_LEN),
        );
        while let Some(value) = sequence.next_element::<ArtifactRef>()? {
            if values.len() == AgentArtifacts::MAX_LEN {
                return Err(de::Error::custom(AgentArtifactsError::TooMany {
                    maximum: AgentArtifacts::MAX_LEN,
                    actual: AgentArtifacts::MAX_LEN + 1,
                }));
            }
            if values
                .iter()
                .any(|existing: &ArtifactRef| existing.identity() == value.identity())
            {
                return Err(de::Error::custom(AgentArtifactsError::DuplicateIdentity));
            }
            values.push(value);
        }
        AgentArtifacts::try_new(values).map_err(de::Error::custom)
    }
}

impl JsonSchema for AgentArtifacts {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "AgentArtifacts".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::AgentArtifacts").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ArtifactRef>(),
            "maxItems": 64,
            "uniqueItems": true,
            "description": "Ordered final agent artifact references with unique tenant-qualified identities. Identity uniqueness is enforced at runtime."
        })
    }
}

/// Invalid final artifact collection for one successful agent invocation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentArtifactsError {
    /// The global pre-budget artifact count ceiling was exceeded.
    #[error("agent result has {actual} artifacts; hard maximum is {maximum}")]
    TooMany {
        /// Absolute v1 count ceiling.
        maximum: usize,
        /// First observed count beyond the ceiling.
        actual: usize,
    },
    /// The same tenant-qualified artifact identity appeared more than once.
    #[error("agent result artifact identities must be unique")]
    DuplicateIdentity,
    /// Aggregate representation byte arithmetic overflowed.
    #[error("agent result aggregate artifact bytes overflowed")]
    TotalBytesOverflow,
}

/// Successful, schema-bound terminal result of one agent invocation.
///
/// This compact value does not duplicate model responses, messages, tool
/// ledgers, approval records, or handoff history; those remain ordered durable
/// events. Construction validates intrinsic accounting and artifact binding.
/// Before commit, a runtime must additionally call [`Self::validate_for`] and
/// validate `output` against the digest-pinned local output schema.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResult {
    provenance: AgentResultProvenance,
    completed_at: Timestamp,
    output_schema: SchemaReference,
    output: BoundedJson,
    artifacts: AgentArtifacts,
    usage: BudgetUsage,
}

impl AgentResult {
    /// Constructs an intrinsically valid successful terminal result.
    ///
    /// # Errors
    ///
    /// Returns [`AgentResultError`] when the run has no accountable model turn,
    /// attempts are lower than turns, materialized output/artifact bytes are
    /// underreported, or an artifact belongs to another tenant or run.
    pub fn new(
        provenance: AgentResultProvenance,
        completed_at: Timestamp,
        output_schema: SchemaReference,
        output: BoundedJson,
        artifacts: AgentArtifacts,
        usage: BudgetUsage,
    ) -> Result<Self, AgentResultError> {
        if usage.model_turns() == crate::ExecutionCount::ZERO {
            return Err(AgentResultError::MissingModelTurn);
        }
        if usage.model_attempts() < usage.model_turns() {
            return Err(AgentResultError::ModelAttemptsBelowTurns {
                attempts: usage.model_attempts(),
                turns: usage.model_turns(),
            });
        }

        let output_bytes = byte_count_from_usize(output.stats().compact_bytes());
        if output_bytes > usage.output_bytes() {
            return Err(AgentResultError::OutputBytesUnderreported {
                minimum: output_bytes,
                actual: usage.output_bytes(),
            });
        }
        if artifacts.total_bytes() > usage.artifact_bytes() {
            return Err(AgentResultError::ArtifactBytesUnderreported {
                minimum: artifacts.total_bytes(),
                actual: usage.artifact_bytes(),
            });
        }

        for (index, artifact) in artifacts.iter().enumerate() {
            if artifact.identity().tenant_id() != provenance.tenant_id() {
                return Err(AgentResultError::ArtifactTenantMismatch {
                    index,
                    expected: Box::new(provenance.tenant_id().clone()),
                    actual: Box::new(artifact.identity().tenant_id().clone()),
                });
            }
            if artifact.provenance().run_id() != provenance.run_id() {
                return Err(AgentResultError::ArtifactRunMismatch {
                    index,
                    expected: provenance.run_id(),
                    actual: artifact.provenance().run_id(),
                });
            }
        }

        Ok(Self {
            provenance,
            completed_at,
            output_schema,
            output,
            artifacts,
            usage,
        })
    }

    /// Constructs a result whose output schema comes from the agent snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`AgentResultError`] under the same conditions as [`Self::new`].
    pub fn for_invocation(
        provenance: AgentResultProvenance,
        completed_at: Timestamp,
        descriptor: &AgentDescriptor,
        output: BoundedJson,
        artifacts: AgentArtifacts,
        usage: BudgetUsage,
    ) -> Result<Self, AgentResultError> {
        Self::new(
            provenance,
            completed_at,
            descriptor.output_schema().clone(),
            output,
            artifacts,
            usage,
        )
    }

    /// Returns exact invocation provenance.
    #[must_use]
    pub const fn provenance(&self) -> &AgentResultProvenance {
        &self.provenance
    }

    /// Returns the durable clock observation at successful completion.
    #[must_use]
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }

    /// Returns the exact final-output schema identity.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaReference {
        &self.output_schema
    }

    /// Returns bounded final output without permitting mutation.
    #[must_use]
    pub const fn output(&self) -> &BoundedJson {
        &self.output
    }

    /// Returns bounded final artifact references in semantic order.
    #[must_use]
    pub const fn artifacts(&self) -> &AgentArtifacts {
        &self.artifacts
    }

    /// Returns complete cumulative run usage at this terminal event.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    /// Consumes this result into its durable components.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        AgentResultProvenance,
        Timestamp,
        SchemaReference,
        BoundedJson,
        AgentArtifacts,
        BudgetUsage,
    ) {
        (
            self.provenance,
            self.completed_at,
            self.output_schema,
            self.output,
            self.artifacts,
            self.usage,
        )
    }

    /// Revalidates a successful result before its terminal commit.
    ///
    /// `expected_provenance` must come from trusted run admission, never from
    /// this result. `budget` must be the exact snapshot returned by
    /// [`AgentRequest::resolve_for`]. The method verifies request and result
    /// schema binding, all trusted execution IDs, input accounting, and every
    /// finite budget dimension at `completed_at`.
    ///
    /// # Errors
    ///
    /// Returns [`AgentResultValidationError`] for any substituted identity or
    /// schema, unaccounted input, invalid recovered request, or exhausted budget.
    pub fn validate_for(
        &self,
        expected_provenance: &AgentResultProvenance,
        request: &AgentRequest,
        descriptor: &AgentDescriptor,
        budget: &ResolvedBudget,
    ) -> Result<(), AgentResultValidationError> {
        expected_provenance
            .validate_for(descriptor)
            .map_err(AgentResultValidationError::expected_provenance)?;

        if self.provenance.tenant_id != expected_provenance.tenant_id {
            return Err(AgentResultValidationError::TenantMismatch {
                expected: Box::new(expected_provenance.tenant_id.clone()),
                actual: Box::new(self.provenance.tenant_id.clone()),
            });
        }
        if self.provenance.run_id != expected_provenance.run_id {
            return Err(AgentResultValidationError::RunMismatch {
                expected: expected_provenance.run_id,
                actual: self.provenance.run_id,
            });
        }
        if self.provenance.thread_id != expected_provenance.thread_id {
            return Err(AgentResultValidationError::ThreadMismatch {
                expected: expected_provenance.thread_id,
                actual: self.provenance.thread_id,
            });
        }
        if self.provenance.invocation_id != expected_provenance.invocation_id {
            return Err(AgentResultValidationError::InvocationMismatch {
                expected: expected_provenance.invocation_id,
                actual: self.provenance.invocation_id,
            });
        }
        if self.provenance.agent != expected_provenance.agent {
            return Err(AgentResultValidationError::AgentIdentityMismatch {
                expected: Box::new(expected_provenance.agent.clone()),
                actual: Box::new(self.provenance.agent.clone()),
            });
        }

        request
            .validate_with_resolved_budget(descriptor, budget)
            .map_err(AgentResultValidationError::request)?;
        let input_bytes = byte_count_from_usize(request.input.stats().compact_bytes());
        if input_bytes > self.usage.input_bytes() {
            return Err(AgentResultValidationError::InputBytesUnderreported {
                minimum: input_bytes,
                actual: self.usage.input_bytes(),
            });
        }

        if &self.output_schema != descriptor.output_schema() {
            return Err(AgentResultValidationError::OutputSchemaMismatch {
                expected: Box::new(descriptor.output_schema().clone()),
                actual: Box::new(self.output_schema.clone()),
            });
        }

        budget
            .remaining(&self.usage, self.completed_at)
            .map_err(AgentResultValidationError::budget)?;
        Ok(())
    }
}

impl fmt::Debug for AgentResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentResult")
            .field("provenance", &self.provenance)
            .field("completed_at", &self.completed_at)
            .field("output_schema", &self.output_schema)
            .field("output_stats", &self.output.stats())
            .field("artifacts", &self.artifacts)
            .field("usage_recorded", &true)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for AgentResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            provenance: AgentResultProvenance,
            completed_at: Timestamp,
            output_schema: SchemaReference,
            output: BoundedJson,
            artifacts: AgentArtifacts,
            usage: BudgetUsage,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.provenance,
            wire.completed_at,
            wire.output_schema,
            wire.output,
            wire.artifacts,
            wire.usage,
        )
        .map_err(de::Error::custom)
    }
}

/// Intrinsic invalidity in a successful terminal agent result.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentResultError {
    /// A successful model-backed agent reported no logical turn.
    #[error("successful agent result must account for at least one model turn")]
    MissingModelTurn,
    /// Physical model attempts cannot be fewer than logical turns.
    #[error("agent result reports {attempts} model attempts for {turns} model turns")]
    ModelAttemptsBelowTurns {
        /// Reported physical attempts.
        attempts: crate::ExecutionCount,
        /// Reported logical turns.
        turns: crate::ExecutionCount,
    },
    /// Final inline output was not fully included in cumulative byte usage.
    #[error("agent result records {actual} output bytes; at least {minimum} are required")]
    OutputBytesUnderreported {
        /// Compact final-output bytes.
        minimum: ByteCount,
        /// Reported cumulative output bytes.
        actual: ByteCount,
    },
    /// Returned run-created artifacts were not fully included in byte usage.
    #[error("agent result records {actual} artifact bytes; at least {minimum} are required")]
    ArtifactBytesUnderreported {
        /// Aggregate returned artifact representation bytes.
        minimum: ByteCount,
        /// Reported cumulative artifact bytes.
        actual: ByteCount,
    },
    /// A final artifact belongs to another tenant.
    #[error("agent artifact at index {index} tenant {actual} does not match {expected}")]
    ArtifactTenantMismatch {
        /// Zero-based final artifact position.
        index: usize,
        /// Exact invocation tenant.
        expected: Box<TenantId>,
        /// Rejected artifact tenant.
        actual: Box<TenantId>,
    },
    /// A final artifact was not created by this durable run.
    #[error("agent artifact at index {index} run {actual} does not match {expected}")]
    ArtifactRunMismatch {
        /// Zero-based final artifact position.
        index: usize,
        /// Exact invocation run.
        expected: RunId,
        /// Rejected artifact run.
        actual: RunId,
    },
}

/// Invalid relationship between a terminal result and trusted run snapshots.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AgentResultValidationError {
    /// Trusted expected provenance itself named another agent descriptor.
    #[error("expected agent result provenance is invalid: {source}")]
    ExpectedProvenance {
        /// Underlying descriptor-binding failure.
        #[source]
        source: AgentResultProvenanceError,
    },
    /// Result provenance named another tenant.
    #[error("agent result tenant {actual} does not match expected {expected}")]
    TenantMismatch {
        /// Trusted invocation tenant.
        expected: Box<TenantId>,
        /// Rejected result tenant.
        actual: Box<TenantId>,
    },
    /// Result provenance named another run.
    #[error("agent result run {actual} does not match expected {expected}")]
    RunMismatch {
        /// Trusted run identifier.
        expected: RunId,
        /// Rejected run identifier.
        actual: RunId,
    },
    /// Result provenance named another thread.
    #[error("agent result thread {actual} does not match expected {expected}")]
    ThreadMismatch {
        /// Trusted thread identifier.
        expected: ThreadId,
        /// Rejected thread identifier.
        actual: ThreadId,
    },
    /// Result provenance named another logical invocation.
    #[error("agent result invocation {actual} does not match expected {expected}")]
    InvocationMismatch {
        /// Trusted invocation identifier.
        expected: InvocationId,
        /// Rejected invocation identifier.
        actual: InvocationId,
    },
    /// Result provenance named another agent snapshot.
    #[error("agent result identity {actual:?} does not match expected {expected:?}")]
    AgentIdentityMismatch {
        /// Trusted owner-qualified agent identity.
        expected: Box<CapabilityIdentity>,
        /// Rejected result identity.
        actual: Box<CapabilityIdentity>,
    },
    /// Recovered request did not match descriptor or budget snapshots.
    #[error("agent result request binding is invalid: {source}")]
    Request {
        /// Underlying request validation failure.
        #[source]
        source: AgentRequestValidationError,
    },
    /// Initial materialized input was absent from cumulative run accounting.
    #[error("agent result records {actual} input bytes; at least {minimum} are required")]
    InputBytesUnderreported {
        /// Compact initial request bytes.
        minimum: ByteCount,
        /// Reported cumulative input bytes.
        actual: ByteCount,
    },
    /// Result named another final-output schema.
    #[error("agent output schema {actual:?} does not match descriptor {expected:?}")]
    OutputSchemaMismatch {
        /// Exact descriptor output schema.
        expected: Box<SchemaReference>,
        /// Rejected result output schema.
        actual: Box<SchemaReference>,
    },
    /// Terminal usage or completion time exceeded the resolved budget.
    #[error("agent terminal result exceeds its resolved budget: {source}")]
    Budget {
        /// Exact failed budget dimension.
        #[source]
        source: BudgetEvaluationError,
    },
}

impl AgentResultValidationError {
    const fn expected_provenance(source: AgentResultProvenanceError) -> Self {
        Self::ExpectedProvenance { source }
    }

    const fn request(source: AgentRequestValidationError) -> Self {
        Self::Request { source }
    }

    const fn budget(source: BudgetEvaluationError) -> Self {
        Self::Budget { source }
    }
}

fn byte_count_from_usize(value: usize) -> ByteCount {
    ByteCount::new(u64::try_from(value).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, from_value, json, to_value};

    use super::*;
    use crate::{ArtifactId, BudgetUsageBuilder, CostLimits, ExecutionCount, TokenCount};

    const RUN_ID: &str = "01912345-6789-7abc-8def-0123456789ae";
    const OTHER_RUN_ID: &str = "01912345-6789-7abc-8def-0123456789ad";
    const THREAD_ID: &str = "01912345-6789-7abc-8def-0123456789af";
    const INVOCATION_ID: &str = "01912345-6789-7abc-8def-0123456789b0";
    const COMPLETED_AT: &str = "2029-12-31T23:59:59.000000Z";

    fn descriptor() -> AgentDescriptor {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-agent-v1.json")).unwrap();
        from_value(fixture["descriptors"]["valid"][0].clone()).unwrap()
    }

    fn request() -> AgentRequest {
        let descriptor = descriptor();
        AgentRequest::new(
            descriptor.input_schema().clone(),
            BoundedJson::try_from_value(json!({
                "incident_id": "INC-42",
                "question": "Summarize the evidence"
            }))
            .unwrap(),
            BudgetLimits::empty().with_output_bytes(ByteCount::new(4096)),
        )
    }

    fn base_budget() -> BudgetLimits {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-budget-v1.json")).unwrap();
        let value = from_value::<ResolvedBudget>(fixture["resolved"]["valid"][0].clone()).unwrap();
        BudgetLimits::empty()
            .with_deadline(value.deadline())
            .with_graph_depth(value.graph_depth())
            .with_graph_steps(value.graph_steps())
            .with_model_attempts(value.model_attempts())
            .with_model_turns(value.model_turns())
            .with_input_tokens(value.input_tokens())
            .with_cached_input_tokens(value.cached_input_tokens())
            .with_reasoning_tokens(value.reasoning_tokens())
            .with_output_tokens(value.output_tokens())
            .with_tool_calls(value.tool_calls())
            .with_write_calls(value.write_calls())
            .with_remote_agent_delegations(value.remote_agent_delegations())
            .with_retries(value.retries())
            .with_concurrent_branches(value.concurrent_branches())
            .with_fan_out(value.fan_out())
            .with_input_bytes(value.input_bytes())
            .with_output_bytes(value.output_bytes())
            .with_event_bytes(value.event_bytes())
            .with_checkpoint_bytes(value.checkpoint_bytes())
            .with_artifact_bytes(value.artifact_bytes())
            .with_costs(value.costs().clone())
    }

    fn budget() -> ResolvedBudget {
        request()
            .resolve_for(
                &descriptor(),
                &[base_budget()],
                COMPLETED_AT.parse().unwrap(),
            )
            .unwrap()
    }

    fn provenance() -> AgentResultProvenance {
        AgentResultProvenance::for_agent(
            "tenant-production".parse().unwrap(),
            RUN_ID.parse().unwrap(),
            THREAD_ID.parse().unwrap(),
            INVOCATION_ID.parse().unwrap(),
            &descriptor(),
        )
    }

    fn output() -> BoundedJson {
        BoundedJson::try_from_value(json!({
            "summary": "Database latency caused the incident.",
            "severity": "high"
        }))
        .unwrap()
    }

    fn usage_for(output: &BoundedJson, artifacts: &AgentArtifacts) -> BudgetUsage {
        BudgetUsage::builder()
            .model_attempts(ExecutionCount::new(3))
            .model_turns(ExecutionCount::new(2))
            .input_tokens(TokenCount::new(800))
            .output_tokens(TokenCount::new(200))
            .input_bytes(byte_count_from_usize(
                request().input().stats().compact_bytes(),
            ))
            .output_bytes(byte_count_from_usize(output.stats().compact_bytes()))
            .artifact_bytes(artifacts.total_bytes())
            .known_costs(crate::KnownCosts::empty())
            .build()
            .unwrap()
    }

    fn result() -> AgentResult {
        let output = output();
        let artifacts = AgentArtifacts::empty();
        AgentResult::for_invocation(
            provenance(),
            COMPLETED_AT.parse().unwrap(),
            &descriptor(),
            output.clone(),
            artifacts.clone(),
            usage_for(&output, &artifacts),
        )
        .unwrap()
    }

    fn bound_artifact(tenant: &str, run_id: &str, artifact_id: ArtifactId) -> ArtifactRef {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-artifact-v1.json")).unwrap();
        let mut value = fixture["artifact_refs"]["valid"][0].clone();
        value["identity"]["tenant_id"] = Value::from(tenant);
        value["identity"]["artifact_id"] = Value::from(artifact_id.to_string());
        value["provenance"]["run_id"] = Value::from(run_id);
        from_value(value).unwrap()
    }

    #[test]
    fn request_resolves_all_layers_and_redacts_content() {
        let request = request();
        let budget = request
            .resolve_for(
                &descriptor(),
                &[base_budget()],
                COMPLETED_AT.parse().unwrap(),
            )
            .unwrap();
        assert_eq!(budget.model_turns(), ExecutionCount::new(12));
        assert_eq!(budget.tool_calls(), ExecutionCount::new(24));
        assert_eq!(budget.write_calls(), ExecutionCount::new(1));
        assert_eq!(budget.output_bytes(), ByteCount::new(4096));
        assert!(!format!("{request:?}").contains("INC-42"));

        let scalar = AgentRequest::new(
            descriptor().input_schema().clone(),
            BoundedJson::try_from_value(json!("INC-42")).unwrap(),
            BudgetLimits::empty(),
        );
        scalar
            .validate_with_resolved_budget(&descriptor(), &budget)
            .expect("agent input root shape belongs to the registered schema");
    }

    #[test]
    fn request_rejects_schema_substitution_expiry_and_layer_exhaustion() {
        let descriptor = descriptor();
        let mut value = to_value(request()).unwrap();
        value["input_schema"] = to_value(descriptor.output_schema()).unwrap();
        let wrong_schema = from_value::<AgentRequest>(value).unwrap();
        assert!(matches!(
            wrong_schema.validate_with_resolved_budget(&descriptor, &budget()),
            Err(AgentRequestValidationError::SchemaMismatch { .. })
        ));

        assert!(matches!(
            request().resolve_for(
                &descriptor,
                &[base_budget()],
                "2030-01-01T00:00:00.000000Z".parse().unwrap(),
            ),
            Err(AgentRequestValidationError::BudgetEvaluation { .. })
        ));

        let too_many = vec![BudgetLimits::empty(); MAX_BUDGET_LAYERS];
        assert!(matches!(
            request().resolve_for(&descriptor, &too_many, COMPLETED_AT.parse().unwrap(),),
            Err(AgentRequestValidationError::TooManyBudgetLayers { .. })
        ));
    }

    #[test]
    fn request_rejects_retired_agent_but_recovery_validation_remains_possible() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-agent-v1.json")).unwrap();
        let mut value = fixture["descriptors"]["valid"][0].clone();
        value["metadata"]["lifecycle"] = json!({
            "status": "retired",
            "retired_at": "2026-08-29T00:00:00.000000Z",
            "notice": "Historical agent version"
        });
        let retired = from_value::<AgentDescriptor>(value).unwrap();
        assert!(matches!(
            request().resolve_for(&retired, &[base_budget()], COMPLETED_AT.parse().unwrap(),),
            Err(AgentRequestValidationError::RetiredAgent { .. })
        ));
        request()
            .validate_with_resolved_budget(&retired, &budget())
            .unwrap();
    }

    #[test]
    fn final_artifacts_are_bounded_unique_and_run_scoped() {
        let artifact = bound_artifact("tenant-production", RUN_ID, ArtifactId::generate());
        assert_eq!(
            AgentArtifacts::try_new([artifact.clone(), artifact]),
            Err(AgentArtifactsError::DuplicateIdentity)
        );

        let too_many = (0..=AgentArtifacts::MAX_LEN)
            .map(|_| bound_artifact("tenant-production", RUN_ID, ArtifactId::generate()));
        assert!(matches!(
            AgentArtifacts::try_new(too_many),
            Err(AgentArtifactsError::TooMany { .. })
        ));

        let foreign = AgentArtifacts::try_new([bound_artifact(
            "tenant-other",
            RUN_ID,
            ArtifactId::generate(),
        )])
        .unwrap();
        let output = output();
        assert!(matches!(
            AgentResult::for_invocation(
                provenance(),
                COMPLETED_AT.parse().unwrap(),
                &descriptor(),
                output.clone(),
                foreign.clone(),
                usage_for(&output, &foreign),
            ),
            Err(AgentResultError::ArtifactTenantMismatch { .. })
        ));
    }

    #[test]
    fn result_requires_complete_accounting_and_exact_run_binding() {
        let descriptor = descriptor();
        let request = request();
        let budget = budget();
        let result = result();
        result
            .validate_for(&provenance(), &request, &descriptor, &budget)
            .unwrap();

        let foreign = AgentResultProvenance::for_agent(
            "tenant-production".parse().unwrap(),
            OTHER_RUN_ID.parse().unwrap(),
            THREAD_ID.parse().unwrap(),
            INVOCATION_ID.parse().unwrap(),
            &descriptor,
        );
        assert!(matches!(
            result.validate_for(&foreign, &request, &descriptor, &budget),
            Err(AgentResultValidationError::RunMismatch { .. })
        ));

        let output = output();
        let artifacts = AgentArtifacts::empty();
        let underreported = BudgetUsageBuilder::default()
            .model_attempts(ExecutionCount::new(1))
            .model_turns(ExecutionCount::new(1))
            .input_bytes(ByteCount::ZERO)
            .output_bytes(byte_count_from_usize(output.stats().compact_bytes()))
            .artifact_bytes(ByteCount::ZERO)
            .build()
            .unwrap();
        let result = AgentResult::for_invocation(
            provenance(),
            COMPLETED_AT.parse().unwrap(),
            &descriptor,
            output,
            artifacts,
            underreported,
        )
        .unwrap();
        assert!(matches!(
            result.validate_for(&provenance(), &request, &descriptor, &budget),
            Err(AgentResultValidationError::InputBytesUnderreported { .. })
        ));
    }

    #[test]
    fn result_rejects_impossible_turn_and_output_accounting() {
        let output = output();
        assert_eq!(
            AgentResult::for_invocation(
                provenance(),
                COMPLETED_AT.parse().unwrap(),
                &descriptor(),
                output.clone(),
                AgentArtifacts::empty(),
                BudgetUsage::zero(),
            ),
            Err(AgentResultError::MissingModelTurn)
        );

        let attempts_below_turns = BudgetUsage::builder()
            .model_attempts(ExecutionCount::new(1))
            .model_turns(ExecutionCount::new(2))
            .output_bytes(byte_count_from_usize(output.stats().compact_bytes()))
            .build()
            .unwrap();
        assert!(matches!(
            AgentResult::for_invocation(
                provenance(),
                COMPLETED_AT.parse().unwrap(),
                &descriptor(),
                output.clone(),
                AgentArtifacts::empty(),
                attempts_below_turns,
            ),
            Err(AgentResultError::ModelAttemptsBelowTurns { .. })
        ));

        let output_underreported = BudgetUsage::builder()
            .model_attempts(ExecutionCount::new(1))
            .model_turns(ExecutionCount::new(1))
            .output_bytes(ByteCount::ZERO)
            .build()
            .unwrap();
        assert!(matches!(
            AgentResult::for_invocation(
                provenance(),
                COMPLETED_AT.parse().unwrap(),
                &descriptor(),
                output,
                AgentArtifacts::empty(),
                output_underreported,
            ),
            Err(AgentResultError::OutputBytesUnderreported { .. })
        ));
    }

    #[test]
    fn result_wire_is_closed_and_redacts_output_and_usage_values() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<AgentRequest>();
        assert_send_sync::<AgentResultProvenance>();
        assert_send_sync::<AgentArtifacts>();
        assert_send_sync::<AgentResult>();

        let result = result();
        let value = to_value(&result).unwrap();
        let decoded = from_value::<AgentResult>(value.clone()).unwrap();
        assert_eq!(to_value(decoded).unwrap(), value);
        let debug = format!("{result:?}");
        assert!(!debug.contains("Database latency"));
        assert!(!debug.contains("800"));

        let mut unknown = value;
        unknown["raw_responses"] = json!([]);
        assert!(from_value::<AgentResult>(unknown).is_err());

        for schema in [
            to_value(schemars::schema_for!(AgentRequest)).unwrap(),
            to_value(schemars::schema_for!(AgentResultProvenance)).unwrap(),
            to_value(schemars::schema_for!(AgentResult)).unwrap(),
        ] {
            assert_eq!(schema["type"], "object");
            assert_eq!(schema["additionalProperties"], false);
        }
        let artifacts = to_value(schemars::schema_for!(AgentArtifacts)).unwrap();
        assert_eq!(artifacts["type"], "array");
        assert_eq!(artifacts["maxItems"], AgentArtifacts::MAX_LEN);
    }

    #[test]
    fn request_budget_requires_a_complete_base_policy() {
        assert!(matches!(
            request().resolve_for(
                &descriptor(),
                &[BudgetLimits::empty().with_costs(CostLimits::default())],
                COMPLETED_AT.parse().unwrap(),
            ),
            Err(AgentRequestValidationError::BudgetResolution { .. })
        ));
    }
}
