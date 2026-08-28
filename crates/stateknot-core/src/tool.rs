// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Provider- and protocol-neutral tool descriptor contracts.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    ByteCount, CapabilityKind, CapabilityMetadata, DurationMillis, ExecutionCount, SchemaReference,
};

/// External side-effect class declared by a trusted tool registry.
///
/// This value is a reviewed semantic claim, not a fact inferred from an HTTP
/// method, provider annotation, or model output. Retry remains subject to the
/// full execution contract, failure advice, deadline, budget, and policy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRisk {
    /// The tool does not intentionally change externally observable state.
    ReadOnly,
    /// Repeating the same logical write is safe under the declared mechanism.
    IdempotentWrite,
    /// Repeating the write may create an additional external effect.
    NonIdempotentWrite,
}

/// Mechanism governing repeated invocation of a tool operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolIdempotency {
    /// Idempotency is irrelevant because the tool is read-only.
    NotApplicable,
    /// Identical canonical arguments are intrinsically safe to repeat.
    Intrinsic,
    /// The runtime must provide the same durable idempotency key on every attempt.
    RequiredKey,
    /// The tool cannot guarantee that a repeated write has one business effect.
    Unsupported,
}

/// Validated side-effect and recovery semantics for one tool version.
///
/// Status-query support means an implementation can reconcile an ambiguous
/// invocation outcome. Compensation support means a separately authorized
/// recovery operation exists; it does not make the original write atomic or
/// erase its audit history.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionSemantics {
    risk: ToolRisk,
    idempotency: ToolIdempotency,
    status_query: bool,
    compensation: bool,
}

impl ToolExecutionSemantics {
    /// Constructs a cross-field validated execution contract.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionSemanticsError`] when risk and idempotency
    /// conflict, or when a read-only tool claims write-recovery operations.
    pub const fn new(
        risk: ToolRisk,
        idempotency: ToolIdempotency,
        status_query: bool,
        compensation: bool,
    ) -> Result<Self, ToolExecutionSemanticsError> {
        let idempotency_is_valid = matches!(
            (risk, idempotency),
            (ToolRisk::ReadOnly, ToolIdempotency::NotApplicable)
                | (
                    ToolRisk::IdempotentWrite,
                    ToolIdempotency::Intrinsic | ToolIdempotency::RequiredKey
                )
                | (ToolRisk::NonIdempotentWrite, ToolIdempotency::Unsupported)
        );
        if !idempotency_is_valid {
            return Err(ToolExecutionSemanticsError::InvalidIdempotency { risk, idempotency });
        }
        if matches!(risk, ToolRisk::ReadOnly) && (status_query || compensation) {
            return Err(ToolExecutionSemanticsError::ReadOnlyRecoverySupport {
                status_query,
                compensation,
            });
        }
        Ok(Self {
            risk,
            idempotency,
            status_query,
            compensation,
        })
    }

    /// Returns the reviewed external side-effect class.
    #[must_use]
    pub const fn risk(&self) -> ToolRisk {
        self.risk
    }

    /// Returns the declared repeated-invocation mechanism.
    #[must_use]
    pub const fn idempotency(&self) -> ToolIdempotency {
        self.idempotency
    }

    /// Returns whether ambiguous outcomes can be queried authoritatively.
    #[must_use]
    pub const fn supports_status_query(&self) -> bool {
        self.status_query
    }

    /// Returns whether a separately authorized compensation operation exists.
    #[must_use]
    pub const fn supports_compensation(&self) -> bool {
        self.compensation
    }

    /// Returns whether the runtime must supply a durable idempotency key.
    #[must_use]
    pub const fn requires_idempotency_key(&self) -> bool {
        matches!(self.idempotency, ToolIdempotency::RequiredKey)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolExecutionSemanticsWire {
    risk: ToolRisk,
    idempotency: ToolIdempotency,
    status_query: bool,
    compensation: bool,
}

impl<'de> Deserialize<'de> for ToolExecutionSemantics {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ToolExecutionSemanticsWire::deserialize(deserializer)?;
        Self::new(
            wire.risk,
            wire.idempotency,
            wire.status_query,
            wire.compensation,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid tool side-effect and recovery semantics.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolExecutionSemanticsError {
    /// The idempotency mechanism does not match the declared risk class.
    #[error("tool risk {risk:?} is incompatible with idempotency {idempotency:?}")]
    InvalidIdempotency {
        /// Declared side-effect class.
        risk: ToolRisk,
        /// Conflicting repeated-invocation mechanism.
        idempotency: ToolIdempotency,
    },

    /// A read-only tool declared write-recovery behavior.
    #[error(
        "read-only tool cannot declare status-query={status_query} or compensation={compensation}"
    )]
    ReadOnlyRecoverySupport {
        /// Invalid status-query declaration.
        status_query: bool,
        /// Invalid compensation declaration.
        compensation: bool,
    },
}

/// Broad resource access required by a tool implementation.
///
/// Exact destinations, paths, operations, and credential handles live in a
/// tenant-controlled executor profile. This declaration can only cause policy
/// to require or deny access; it never grants access by itself.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResourceAccess {
    /// No access to the resource category is required.
    None,
    /// The tool only reads from the resource category.
    ReadOnly,
    /// The tool may read and write the resource category.
    ReadWrite,
}

/// Resource categories that must be satisfied by an executor profile.
///
/// `dynamic_code` means the tool executes code supplied at invocation time. It
/// does not describe ordinary compiled tool implementation code. `StateKnot` v1
/// does not provide a built-in arbitrary-code sandbox; policy may route such a
/// tool to an independently operated sandbox service or deny it.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResourceRequirements {
    network: ToolResourceAccess,
    filesystem: ToolResourceAccess,
    credentials: bool,
    dynamic_code: bool,
}

impl ToolResourceRequirements {
    /// Constructs explicit resource-category requirements.
    #[must_use]
    pub const fn new(
        network: ToolResourceAccess,
        filesystem: ToolResourceAccess,
        credentials: bool,
        dynamic_code: bool,
    ) -> Self {
        Self {
            network,
            filesystem,
            credentials,
            dynamic_code,
        }
    }

    /// Constructs a tool requiring no external resource category.
    #[must_use]
    pub const fn none() -> Self {
        Self::new(
            ToolResourceAccess::None,
            ToolResourceAccess::None,
            false,
            false,
        )
    }

    /// Returns the broad network access declaration.
    #[must_use]
    pub const fn network(&self) -> ToolResourceAccess {
        self.network
    }

    /// Returns the broad filesystem access declaration.
    #[must_use]
    pub const fn filesystem(&self) -> ToolResourceAccess {
        self.filesystem
    }

    /// Returns whether opaque credentials must be resolved for an invocation.
    #[must_use]
    pub const fn requires_credentials(&self) -> bool {
        self.credentials
    }

    /// Returns whether invocation-supplied code is executed.
    #[must_use]
    pub const fn executes_dynamic_code(&self) -> bool {
        self.dynamic_code
    }

    /// Returns whether a declared resource category may be written.
    #[must_use]
    pub const fn has_write_access(&self) -> bool {
        matches!(self.network, ToolResourceAccess::ReadWrite)
            || matches!(self.filesystem, ToolResourceAccess::ReadWrite)
    }
}

/// Cancellation behavior exposed by a tool implementation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCancellationSupport {
    /// The implementation has no cancellation entry point.
    Unsupported,
    /// The implementation observes cancellation on a best-effort basis.
    Cooperative,
}

/// Runtime-facing optional behavior supported by a tool implementation.
///
/// Cooperative cancellation never proves that an external effect did not
/// occur. Progress events are observational and cannot commit tool success.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInvocationCapabilities {
    cancellation: ToolCancellationSupport,
    max_progress_events: ExecutionCount,
}

impl ToolInvocationCapabilities {
    /// Constructs invocation behavior flags.
    #[must_use]
    pub const fn new(
        cancellation: ToolCancellationSupport,
        max_progress_events: ExecutionCount,
    ) -> Self {
        Self {
            cancellation,
            max_progress_events,
        }
    }

    /// Returns the supported cancellation mode.
    #[must_use]
    pub const fn cancellation(&self) -> ToolCancellationSupport {
        self.cancellation
    }

    /// Returns whether bounded progress events may be emitted.
    #[must_use]
    pub const fn supports_progress_events(&self) -> bool {
        self.max_progress_events.get() != 0
    }

    /// Returns the maximum progress events emitted by one invocation.
    #[must_use]
    pub const fn max_progress_events(&self) -> ExecutionCount {
        self.max_progress_events
    }
}

/// Finite per-invocation execution ceilings declared by a tool version.
///
/// System, tenant, policy, run, and descriptor limits are intersected; these
/// values can never widen another layer. `max_total_artifact_bytes` covers all
/// artifacts produced by one invocation, not one artifact or a complete run.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolExecutionLimits {
    timeout: DurationMillis,
    max_concurrency: ExecutionCount,
    max_input_bytes: ByteCount,
    max_inline_result_bytes: ByteCount,
    max_artifacts: ExecutionCount,
    max_total_artifact_bytes: ByteCount,
}

impl ToolExecutionLimits {
    /// Constructs finite limits and validates unusable zero combinations.
    ///
    /// # Errors
    ///
    /// Returns [`ToolExecutionLimitsError`] when timeout, concurrency, or
    /// input/inline result capacity is zero, or when artifact count and byte
    /// capacity disagree about whether artifacts are permitted.
    pub const fn new(
        timeout: DurationMillis,
        max_concurrency: ExecutionCount,
        max_input_bytes: ByteCount,
        max_inline_result_bytes: ByteCount,
        max_artifacts: ExecutionCount,
        max_total_artifact_bytes: ByteCount,
    ) -> Result<Self, ToolExecutionLimitsError> {
        if timeout.as_i64() == 0 {
            return Err(ToolExecutionLimitsError::ZeroTimeout);
        }
        if max_concurrency.get() == 0 {
            return Err(ToolExecutionLimitsError::ZeroConcurrency);
        }
        if max_input_bytes.get() == 0 {
            return Err(ToolExecutionLimitsError::ZeroInputBytes);
        }
        if max_inline_result_bytes.get() == 0 {
            return Err(ToolExecutionLimitsError::ZeroInlineResultBytes);
        }
        if (max_artifacts.get() == 0) != (max_total_artifact_bytes.get() == 0) {
            return Err(ToolExecutionLimitsError::ArtifactCapacityMismatch {
                max_artifacts,
                max_total_artifact_bytes,
            });
        }
        Ok(Self {
            timeout,
            max_concurrency,
            max_input_bytes,
            max_inline_result_bytes,
            max_artifacts,
            max_total_artifact_bytes,
        })
    }

    /// Returns the maximum elapsed time for one invocation attempt.
    #[must_use]
    pub const fn timeout(&self) -> DurationMillis {
        self.timeout
    }

    /// Returns the maximum concurrent calls for this capability version.
    #[must_use]
    pub const fn max_concurrency(&self) -> ExecutionCount {
        self.max_concurrency
    }

    /// Returns the maximum compact encoded argument bytes for one invocation.
    #[must_use]
    pub const fn max_input_bytes(&self) -> ByteCount {
        self.max_input_bytes
    }

    /// Returns the maximum inline result bytes from one invocation.
    #[must_use]
    pub const fn max_inline_result_bytes(&self) -> ByteCount {
        self.max_inline_result_bytes
    }

    /// Returns the maximum number of artifacts from one invocation.
    #[must_use]
    pub const fn max_artifacts(&self) -> ExecutionCount {
        self.max_artifacts
    }

    /// Returns the maximum total artifact bytes from one invocation.
    #[must_use]
    pub const fn max_total_artifact_bytes(&self) -> ByteCount {
        self.max_total_artifact_bytes
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolExecutionLimitsWire {
    timeout: DurationMillis,
    max_concurrency: ExecutionCount,
    max_input_bytes: ByteCount,
    max_inline_result_bytes: ByteCount,
    max_artifacts: ExecutionCount,
    max_total_artifact_bytes: ByteCount,
}

impl<'de> Deserialize<'de> for ToolExecutionLimits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ToolExecutionLimitsWire::deserialize(deserializer)?;
        Self::new(
            wire.timeout,
            wire.max_concurrency,
            wire.max_input_bytes,
            wire.max_inline_result_bytes,
            wire.max_artifacts,
            wire.max_total_artifact_bytes,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid per-invocation tool limits.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolExecutionLimitsError {
    /// A zero timeout would make every invocation ineligible.
    #[error("tool timeout must be greater than zero milliseconds")]
    ZeroTimeout,

    /// A zero concurrency ceiling would make every invocation ineligible.
    #[error("tool max concurrency must be greater than zero")]
    ZeroConcurrency,

    /// Schema validation and invocation require a positive argument capacity.
    #[error("tool max input bytes must be greater than zero")]
    ZeroInputBytes,

    /// A successful inline result needs a positive encoded byte capacity.
    #[error("tool max inline result bytes must be greater than zero")]
    ZeroInlineResultBytes,

    /// Artifact count and total bytes disagreed about artifact support.
    #[error(
        "tool artifact capacity requires both count and bytes to be zero or both to be positive"
    )]
    ArtifactCapacityMismatch {
        /// Declared maximum artifact count.
        max_artifacts: ExecutionCount,
        /// Declared maximum total artifact bytes.
        max_total_artifact_bytes: ByteCount,
    },
}

/// Immutable, protocol-neutral description of one executable tool version.
///
/// Schema references are identities only. Before registration, a trusted local
/// schema registry verifies their digests, JSON Schema 2020-12 documents, and
/// provider compatibility profile. Before selection, the tenant registry
/// authenticates metadata ownership, applies policy, and snapshots this exact
/// descriptor for the invocation attempt.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolDescriptor {
    metadata: CapabilityMetadata,
    input_schema: SchemaReference,
    output_schema: SchemaReference,
    semantics: ToolExecutionSemantics,
    resources: ToolResourceRequirements,
    invocation: ToolInvocationCapabilities,
    limits: ToolExecutionLimits,
}

impl ToolDescriptor {
    /// Constructs a descriptor and validates cross-component invariants.
    ///
    /// # Errors
    ///
    /// Returns [`ToolDescriptorError`] when common metadata is not a tool or a
    /// read-only tool declares network/filesystem write access.
    pub fn new(
        metadata: CapabilityMetadata,
        input_schema: SchemaReference,
        output_schema: SchemaReference,
        semantics: ToolExecutionSemantics,
        resources: ToolResourceRequirements,
        invocation: ToolInvocationCapabilities,
        limits: ToolExecutionLimits,
    ) -> Result<Self, ToolDescriptorError> {
        if metadata.kind() != CapabilityKind::Tool {
            return Err(ToolDescriptorError::WrongCapabilityKind {
                actual: metadata.kind(),
            });
        }
        if semantics.risk() == ToolRisk::ReadOnly && resources.has_write_access() {
            return Err(ToolDescriptorError::ReadOnlyResourceWrite {
                network: resources.network(),
                filesystem: resources.filesystem(),
            });
        }
        Ok(Self {
            metadata,
            input_schema,
            output_schema,
            semantics,
            resources,
            invocation,
            limits,
        })
    }

    /// Returns common identity, discovery, lifecycle, scope, and extension data.
    #[must_use]
    pub const fn metadata(&self) -> &CapabilityMetadata {
        &self.metadata
    }

    /// Returns the immutable input schema identity.
    #[must_use]
    pub const fn input_schema(&self) -> &SchemaReference {
        &self.input_schema
    }

    /// Returns the immutable output schema identity.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaReference {
        &self.output_schema
    }

    /// Returns side-effect, idempotency, and recovery semantics.
    #[must_use]
    pub const fn semantics(&self) -> &ToolExecutionSemantics {
        &self.semantics
    }

    /// Returns resource categories that an executor profile must satisfy.
    #[must_use]
    pub const fn resources(&self) -> &ToolResourceRequirements {
        &self.resources
    }

    /// Returns optional invocation behavior implemented by this tool.
    #[must_use]
    pub const fn invocation(&self) -> &ToolInvocationCapabilities {
        &self.invocation
    }

    /// Returns finite per-invocation ceilings.
    #[must_use]
    pub const fn limits(&self) -> &ToolExecutionLimits {
        &self.limits
    }
}

impl fmt::Debug for ToolDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolDescriptor")
            .field("metadata", &self.metadata)
            .field("input_schema", &self.input_schema)
            .field("output_schema", &self.output_schema)
            .field("semantics", &self.semantics)
            .field("resources", &self.resources)
            .field("invocation", &self.invocation)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolDescriptorWire {
    metadata: CapabilityMetadata,
    input_schema: SchemaReference,
    output_schema: SchemaReference,
    semantics: ToolExecutionSemantics,
    resources: ToolResourceRequirements,
    invocation: ToolInvocationCapabilities,
    limits: ToolExecutionLimits,
}

impl<'de> Deserialize<'de> for ToolDescriptor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ToolDescriptorWire::deserialize(deserializer)?;
        Self::new(
            wire.metadata,
            wire.input_schema,
            wire.output_schema,
            wire.semantics,
            wire.resources,
            wire.invocation,
            wire.limits,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid cross-component tool descriptor.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolDescriptorError {
    /// Common metadata classified the capability as something other than a tool.
    #[error("tool descriptor requires kind=tool, received {actual:?}")]
    WrongCapabilityKind {
        /// Conflicting capability kind.
        actual: CapabilityKind,
    },

    /// A read-only tool requested write access to a broad resource category.
    #[error(
        "read-only tool cannot declare network={network:?} or filesystem={filesystem:?} write access"
    )]
    ReadOnlyResourceWrite {
        /// Conflicting network access.
        network: ToolResourceAccess,
        /// Conflicting filesystem access.
        filesystem: ToolResourceAccess,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    use crate::{
        BoundedJson, CapabilityDescription, CapabilityIdentity, CapabilityLifecycle,
        CapabilityName, CapabilityReference, CapabilityTitle, Digest, ExtensionKey, ExtensionValue,
        Extensions, IssuerId, PrincipalIdentity, SchemaId, ScopeSet, SubjectId, Version,
    };

    fn principal() -> PrincipalIdentity {
        PrincipalIdentity::new(
            "https://issuer.example.com/tenant"
                .parse::<IssuerId>()
                .unwrap(),
            "registry-owner".parse::<SubjectId>().unwrap(),
        )
    }

    fn metadata(kind: CapabilityKind, secret: &str) -> CapabilityMetadata {
        let identity = CapabilityIdentity::new(
            principal(),
            CapabilityReference::new(
                "payments.capture".parse::<CapabilityName>().unwrap(),
                Version::new(2, 1, 0),
            ),
        );
        let extensions = Extensions::try_new([(
            ExtensionKey::new("com.example.tool").unwrap(),
            ExtensionValue::opaque(
                BoundedJson::try_from_value(json!({ "secret": secret })).unwrap(),
            ),
        )])
        .unwrap();
        CapabilityMetadata::new(
            identity,
            kind,
            Some(CapabilityTitle::new(format!("Capture payment {secret}")).unwrap()),
            CapabilityDescription::new(format!("Capture one approved payment. {secret}")).unwrap(),
            CapabilityLifecycle::active(),
            ScopeSet::empty(),
            extensions,
        )
        .unwrap()
    }

    fn schema(name: &str) -> SchemaReference {
        SchemaReference::new(
            format!("https://schemas.example.com/tools/{name}/1.0.0")
                .parse::<SchemaId>()
                .unwrap(),
            Version::new(1, 0, 0),
            Digest::sha256(name),
        )
    }

    fn read_only_semantics() -> ToolExecutionSemantics {
        ToolExecutionSemantics::new(
            ToolRisk::ReadOnly,
            ToolIdempotency::NotApplicable,
            false,
            false,
        )
        .unwrap()
    }

    fn default_limits() -> ToolExecutionLimits {
        ToolExecutionLimits::new(
            DurationMillis::new(30_000).unwrap(),
            ExecutionCount::new(16),
            ByteCount::new(64 * 1024),
            ByteCount::new(256 * 1024),
            ExecutionCount::new(4),
            ByteCount::new(25 * 1024 * 1024),
        )
        .unwrap()
    }

    fn descriptor(secret: &str) -> ToolDescriptor {
        ToolDescriptor::new(
            metadata(CapabilityKind::Tool, secret),
            schema("capture-input"),
            schema("capture-output"),
            ToolExecutionSemantics::new(
                ToolRisk::IdempotentWrite,
                ToolIdempotency::RequiredKey,
                true,
                true,
            )
            .unwrap(),
            ToolResourceRequirements::new(
                ToolResourceAccess::ReadWrite,
                ToolResourceAccess::None,
                true,
                false,
            ),
            ToolInvocationCapabilities::new(
                ToolCancellationSupport::Cooperative,
                ExecutionCount::new(128),
            ),
            default_limits(),
        )
        .unwrap()
    }

    #[test]
    fn tool_enums_have_closed_canonical_wire_values() {
        for (value, expected) in [
            (ToolRisk::ReadOnly, "read_only"),
            (ToolRisk::IdempotentWrite, "idempotent_write"),
            (ToolRisk::NonIdempotentWrite, "non_idempotent_write"),
        ] {
            assert_eq!(to_value(value).unwrap(), Value::from(expected));
            assert_eq!(from_value::<ToolRisk>(json!(expected)).unwrap(), value);
        }
        for (value, expected) in [
            (ToolIdempotency::NotApplicable, "not_applicable"),
            (ToolIdempotency::Intrinsic, "intrinsic"),
            (ToolIdempotency::RequiredKey, "required_key"),
            (ToolIdempotency::Unsupported, "unsupported"),
        ] {
            assert_eq!(to_value(value).unwrap(), Value::from(expected));
            assert_eq!(
                from_value::<ToolIdempotency>(json!(expected)).unwrap(),
                value
            );
        }
        for (value, expected) in [
            (ToolResourceAccess::None, "none"),
            (ToolResourceAccess::ReadOnly, "read_only"),
            (ToolResourceAccess::ReadWrite, "read_write"),
        ] {
            assert_eq!(to_value(value).unwrap(), Value::from(expected));
            assert_eq!(
                from_value::<ToolResourceAccess>(json!(expected)).unwrap(),
                value
            );
        }
        for (value, expected) in [
            (ToolCancellationSupport::Unsupported, "unsupported"),
            (ToolCancellationSupport::Cooperative, "cooperative"),
        ] {
            assert_eq!(to_value(value).unwrap(), Value::from(expected));
            assert_eq!(
                from_value::<ToolCancellationSupport>(json!(expected)).unwrap(),
                value
            );
        }
        assert!(from_value::<ToolRisk>(json!("unknown")).is_err());
        assert!(from_value::<ToolIdempotency>(Value::Null).is_err());
        assert!(from_value::<ToolResourceAccess>(json!(42)).is_err());
    }

    #[test]
    fn execution_semantics_reject_every_incoherent_pair() {
        let risks = [
            ToolRisk::ReadOnly,
            ToolRisk::IdempotentWrite,
            ToolRisk::NonIdempotentWrite,
        ];
        let idempotency_modes = [
            ToolIdempotency::NotApplicable,
            ToolIdempotency::Intrinsic,
            ToolIdempotency::RequiredKey,
            ToolIdempotency::Unsupported,
        ];

        for risk in risks {
            for idempotency in idempotency_modes {
                let expected_valid = matches!(
                    (risk, idempotency),
                    (ToolRisk::ReadOnly, ToolIdempotency::NotApplicable)
                        | (
                            ToolRisk::IdempotentWrite,
                            ToolIdempotency::Intrinsic | ToolIdempotency::RequiredKey
                        )
                        | (ToolRisk::NonIdempotentWrite, ToolIdempotency::Unsupported)
                );
                assert_eq!(
                    ToolExecutionSemantics::new(risk, idempotency, false, false).is_ok(),
                    expected_valid,
                    "unexpected result for {risk:?}/{idempotency:?}"
                );
            }
        }

        for (status_query, compensation) in [(true, false), (false, true), (true, true)] {
            assert_eq!(
                ToolExecutionSemantics::new(
                    ToolRisk::ReadOnly,
                    ToolIdempotency::NotApplicable,
                    status_query,
                    compensation,
                ),
                Err(ToolExecutionSemanticsError::ReadOnlyRecoverySupport {
                    status_query,
                    compensation,
                })
            );
        }
    }

    #[test]
    fn execution_semantics_round_trip_and_revalidate_wire_data() {
        let semantics = ToolExecutionSemantics::new(
            ToolRisk::IdempotentWrite,
            ToolIdempotency::RequiredKey,
            true,
            true,
        )
        .unwrap();
        assert_eq!(semantics.risk(), ToolRisk::IdempotentWrite);
        assert_eq!(semantics.idempotency(), ToolIdempotency::RequiredKey);
        assert!(semantics.requires_idempotency_key());
        assert!(semantics.supports_status_query());
        assert!(semantics.supports_compensation());

        let expected = json!({
            "risk": "idempotent_write",
            "idempotency": "required_key",
            "status_query": true,
            "compensation": true
        });
        assert_eq!(to_value(&semantics).unwrap(), expected);
        assert_eq!(
            from_value::<ToolExecutionSemantics>(expected).unwrap(),
            semantics
        );

        for invalid in [
            json!({
                "risk": "idempotent_write",
                "idempotency": "unsupported",
                "status_query": false,
                "compensation": false
            }),
            json!({
                "risk": "read_only",
                "idempotency": "not_applicable",
                "status_query": true,
                "compensation": false
            }),
            json!({
                "risk": "read_only",
                "idempotency": "not_applicable",
                "status_query": false,
                "compensation": false,
                "safe": true
            }),
            Value::Null,
        ] {
            assert!(
                from_value::<ToolExecutionSemantics>(invalid.clone()).is_err(),
                "accepted semantics {invalid}"
            );
        }
    }

    #[test]
    fn resource_and_invocation_capabilities_are_explicit_closed_claims() {
        let none = ToolResourceRequirements::none();
        assert_eq!(none.network(), ToolResourceAccess::None);
        assert_eq!(none.filesystem(), ToolResourceAccess::None);
        assert!(!none.requires_credentials());
        assert!(!none.executes_dynamic_code());
        assert!(!none.has_write_access());

        let resources = ToolResourceRequirements::new(
            ToolResourceAccess::ReadOnly,
            ToolResourceAccess::ReadWrite,
            true,
            true,
        );
        assert!(resources.has_write_access());
        assert!(resources.requires_credentials());
        assert!(resources.executes_dynamic_code());
        let encoded = to_value(&resources).unwrap();
        assert_eq!(
            from_value::<ToolResourceRequirements>(encoded.clone()).unwrap(),
            resources
        );
        let mut unknown = encoded;
        unknown["shell"] = json!(true);
        assert!(from_value::<ToolResourceRequirements>(unknown).is_err());

        let invocation = ToolInvocationCapabilities::new(
            ToolCancellationSupport::Cooperative,
            ExecutionCount::new(128),
        );
        assert_eq!(
            invocation.cancellation(),
            ToolCancellationSupport::Cooperative
        );
        assert!(invocation.supports_progress_events());
        assert_eq!(invocation.max_progress_events(), ExecutionCount::new(128));
        assert_eq!(
            from_value::<ToolInvocationCapabilities>(to_value(&invocation).unwrap()).unwrap(),
            invocation
        );
        assert!(
            from_value::<ToolInvocationCapabilities>(json!({
                "cancellation": "authoritative",
                "max_progress_events": "128"
            }))
            .is_err()
        );
    }

    #[test]
    fn execution_limits_are_finite_consistent_and_closed() {
        let limits = default_limits();
        assert_eq!(limits.timeout(), DurationMillis::new(30_000).unwrap());
        assert_eq!(limits.max_concurrency(), ExecutionCount::new(16));
        assert_eq!(limits.max_input_bytes(), ByteCount::new(64 * 1024));
        assert_eq!(limits.max_inline_result_bytes(), ByteCount::new(256 * 1024));
        assert_eq!(limits.max_artifacts(), ExecutionCount::new(4));
        assert_eq!(
            limits.max_total_artifact_bytes(),
            ByteCount::new(25 * 1024 * 1024)
        );

        let encoded = to_value(&limits).unwrap();
        assert_eq!(encoded["timeout"], "30000");
        assert_eq!(encoded["max_concurrency"], "16");
        assert_eq!(encoded["max_input_bytes"], "65536");
        assert_eq!(encoded["max_inline_result_bytes"], "262144");
        assert_eq!(encoded["max_artifacts"], "4");
        assert_eq!(encoded["max_total_artifact_bytes"], "26214400");
        assert_eq!(
            from_value::<ToolExecutionLimits>(encoded.clone()).unwrap(),
            limits
        );

        assert_eq!(
            ToolExecutionLimits::new(
                DurationMillis::ZERO,
                ExecutionCount::new(1),
                ByteCount::new(1),
                ByteCount::new(1),
                ExecutionCount::ZERO,
                ByteCount::ZERO,
            ),
            Err(ToolExecutionLimitsError::ZeroTimeout)
        );
        assert_eq!(
            ToolExecutionLimits::new(
                DurationMillis::new(1).unwrap(),
                ExecutionCount::ZERO,
                ByteCount::new(1),
                ByteCount::new(1),
                ExecutionCount::ZERO,
                ByteCount::ZERO,
            ),
            Err(ToolExecutionLimitsError::ZeroConcurrency)
        );
        assert_eq!(
            ToolExecutionLimits::new(
                DurationMillis::new(1).unwrap(),
                ExecutionCount::new(1),
                ByteCount::ZERO,
                ByteCount::new(1),
                ExecutionCount::ZERO,
                ByteCount::ZERO,
            ),
            Err(ToolExecutionLimitsError::ZeroInputBytes)
        );
        assert_eq!(
            ToolExecutionLimits::new(
                DurationMillis::new(1).unwrap(),
                ExecutionCount::new(1),
                ByteCount::new(1),
                ByteCount::ZERO,
                ExecutionCount::ZERO,
                ByteCount::ZERO,
            ),
            Err(ToolExecutionLimitsError::ZeroInlineResultBytes)
        );
        for (max_artifacts, max_total_artifact_bytes) in [
            (ExecutionCount::new(1), ByteCount::ZERO),
            (ExecutionCount::ZERO, ByteCount::new(1)),
        ] {
            assert_eq!(
                ToolExecutionLimits::new(
                    DurationMillis::new(1).unwrap(),
                    ExecutionCount::new(1),
                    ByteCount::new(1),
                    ByteCount::new(1),
                    max_artifacts,
                    max_total_artifact_bytes,
                ),
                Err(ToolExecutionLimitsError::ArtifactCapacityMismatch {
                    max_artifacts,
                    max_total_artifact_bytes,
                })
            );
        }

        let mut unknown = encoded;
        unknown["unlimited"] = json!(true);
        assert!(from_value::<ToolExecutionLimits>(unknown).is_err());
    }

    #[test]
    fn descriptors_revalidate_kind_resources_and_redact_discovery_text() {
        let secret = "descriptor-secret";
        let descriptor = descriptor(secret);
        assert_eq!(descriptor.metadata().kind(), CapabilityKind::Tool);
        assert_eq!(
            descriptor.input_schema().id().as_str(),
            "https://schemas.example.com/tools/capture-input/1.0.0"
        );
        assert_eq!(
            descriptor.output_schema().id().as_str(),
            "https://schemas.example.com/tools/capture-output/1.0.0"
        );
        assert!(descriptor.semantics().requires_idempotency_key());
        assert!(descriptor.resources().requires_credentials());
        assert!(descriptor.invocation().supports_progress_events());
        assert_eq!(descriptor.limits(), &default_limits());
        assert!(!format!("{descriptor:?}").contains(secret));

        let encoded = to_value(&descriptor).unwrap();
        assert_eq!(
            from_value::<ToolDescriptor>(encoded.clone()).unwrap(),
            descriptor
        );

        assert_eq!(
            ToolDescriptor::new(
                metadata(CapabilityKind::Agent, secret),
                schema("input"),
                schema("output"),
                read_only_semantics(),
                ToolResourceRequirements::none(),
                ToolInvocationCapabilities::new(
                    ToolCancellationSupport::Unsupported,
                    ExecutionCount::ZERO,
                ),
                default_limits(),
            ),
            Err(ToolDescriptorError::WrongCapabilityKind {
                actual: CapabilityKind::Agent,
            })
        );

        let write_resources = ToolResourceRequirements::new(
            ToolResourceAccess::ReadWrite,
            ToolResourceAccess::None,
            false,
            false,
        );
        assert_eq!(
            ToolDescriptor::new(
                metadata(CapabilityKind::Tool, secret),
                schema("input"),
                schema("output"),
                read_only_semantics(),
                write_resources,
                ToolInvocationCapabilities::new(
                    ToolCancellationSupport::Unsupported,
                    ExecutionCount::ZERO,
                ),
                default_limits(),
            ),
            Err(ToolDescriptorError::ReadOnlyResourceWrite {
                network: ToolResourceAccess::ReadWrite,
                filesystem: ToolResourceAccess::None,
            })
        );

        let mut wrong_kind = encoded.clone();
        wrong_kind["metadata"]["kind"] = json!("agent");
        assert!(from_value::<ToolDescriptor>(wrong_kind).is_err());
        let mut unsafe_resources = encoded.clone();
        unsafe_resources["semantics"] = to_value(read_only_semantics()).unwrap();
        assert!(from_value::<ToolDescriptor>(unsafe_resources).is_err());
        let mut unknown = encoded;
        unknown["provider"] = json!("remote");
        assert!(from_value::<ToolDescriptor>(unknown).is_err());
    }

    #[test]
    fn tool_contract_schemas_are_closed_and_require_every_semantic_field() {
        for schema in [
            to_value(schemars::schema_for!(ToolExecutionSemantics)).unwrap(),
            to_value(schemars::schema_for!(ToolResourceRequirements)).unwrap(),
            to_value(schemars::schema_for!(ToolInvocationCapabilities)).unwrap(),
            to_value(schemars::schema_for!(ToolExecutionLimits)).unwrap(),
            to_value(schemars::schema_for!(ToolDescriptor)).unwrap(),
        ] {
            assert_eq!(schema["type"], "object");
            assert_eq!(schema["additionalProperties"], false);
        }

        let descriptor_schema = to_value(schemars::schema_for!(ToolDescriptor)).unwrap();
        assert_eq!(
            descriptor_schema["required"],
            json!([
                "metadata",
                "input_schema",
                "output_schema",
                "semantics",
                "resources",
                "invocation",
                "limits"
            ])
        );
    }

    proptest! {
        #[test]
        fn every_positive_limit_tuple_round_trips_without_widening(
            timeout in 1_i64..=1_000_000_000_i64,
            concurrency in 1_u64..=1_000_000_u64,
            input_bytes in 1_u64..=1_000_000_000_u64,
            inline_bytes in 1_u64..=1_000_000_000_u64,
            artifact_count in 0_u64..=1_000_u64,
            artifact_bytes in 1_u64..=1_000_000_000_u64,
        ) {
            let artifact_bytes = if artifact_count == 0 { 0 } else { artifact_bytes };
            let limits = ToolExecutionLimits::new(
                DurationMillis::new(timeout).unwrap(),
                ExecutionCount::new(concurrency),
                ByteCount::new(input_bytes),
                ByteCount::new(inline_bytes),
                ExecutionCount::new(artifact_count),
                ByteCount::new(artifact_bytes),
            ).unwrap();
            let encoded = serde_json::to_vec(&limits).unwrap();
            let decoded = serde_json::from_slice::<ToolExecutionLimits>(&encoded).unwrap();
            prop_assert_eq!(decoded, limits);
        }
    }
}
