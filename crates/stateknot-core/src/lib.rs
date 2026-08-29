// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Provider-independent domain types shared across `StateKnot` runtime boundaries.
//!
//! This crate is intentionally independent of model providers, wire protocols,
//! databases, HTTP servers, and async executors. Implemented contracts are
//! introduced only with strict validation, schemas, and versioned wire
//! fixtures from RFC-0001.

#![forbid(unsafe_code)]

mod accounting;
mod artifact;
mod budget;
mod capability;
mod content;
mod decimal;
mod digest;
mod extension;
mod failure;
mod identity;
mod ids;
mod json;
mod message;
mod model;
mod schema;
mod scope;
mod time;
mod tool;
mod version;

pub use accounting::{
    ByteCount, CountParseError, CurrencyCode, CurrencyCodeError, ExecutionCount, Money,
    MoneyArithmeticError, TokenCount,
};
pub use artifact::{
    ArtifactDescription, ArtifactDescriptionError, ArtifactIdentity, ArtifactModality,
    ArtifactName, ArtifactNameError, ArtifactParents, ArtifactParentsError, ArtifactPresentation,
    ArtifactProvenance, ArtifactRef, ArtifactRefError, ArtifactRepresentation,
    ArtifactRepresentationError, ContentPart, MediaType, MediaTypeError, RetentionClass,
    RetentionClassError,
};
pub use budget::{
    BudgetDimension, BudgetEvaluationError, BudgetLimits, BudgetRemaining, BudgetResolutionError,
    BudgetUsage, BudgetUsageBuilder, BudgetUsageError, CostCollectionError, CostLimits, KnownCosts,
    MAX_BUDGET_LAYERS, MAX_COST_CURRENCIES, ResolvedBudget,
};
pub use capability::{
    CapabilityDescription, CapabilityDescriptionError, CapabilityIdentity, CapabilityKind,
    CapabilityLifecycle, CapabilityLifecycleError, CapabilityLifecycleState, CapabilityMetadata,
    CapabilityMetadataError, CapabilityName, CapabilityNameError, CapabilityReference,
    CapabilityTitle, CapabilityTitleError,
};
pub use content::{
    ContentMetadata, ContentSource, ContentTrust, JsonContent, LanguageTag, LanguageTagError,
    RedactionState, SecurityLabel, SecurityLabelError, TextContent, TextContentError,
};
pub use digest::{Digest, DigestAlgorithm, DigestError};
pub use extension::{
    ExtensionKey, ExtensionKeyError, ExtensionKeyKind, ExtensionLimit, ExtensionLimits,
    ExtensionLimitsError, ExtensionValue, Extensions, ExtensionsError,
};
pub use failure::{
    Failure, FailureBuildError, FailureCategory, FailureCode, FailureDetails, FailureDetailsError,
    FailureIdentifierError, FailureMessage, FailureMessageError, FailureOrigin, RetryAdvice,
};
pub use identity::{IssuerId, IssuerIdError, PrincipalIdentity, SubjectId, SubjectIdError};
pub use ids::{
    ArtifactId, AttemptId, EventId, FailureId, GeneratedIdError, InterruptId, InvocationId,
    MessageId, RunId, TenantId, TenantIdError, ThreadId,
};
pub use json::{BoundedJson, BoundedJsonError, JsonLimit, JsonLimits, JsonLimitsError, JsonStats};
pub use message::{
    Instruction, InstructionContent, InstructionError, InstructionIdentity, InstructionName,
    InstructionNameError, InstructionProvenance, Message, MessageError, MessageParts,
    MessagePartsError, MessageProducer, MessageProducerKind, MessageProvenance, MessageRole,
};
pub use model::{
    ModelCapabilities, ModelCapabilitiesError, ModelCapabilityIssue, ModelCapabilityMismatch,
    ModelCapabilityMismatchError, ModelModalities, ModelModalitiesError, ModelModality,
    ModelRequirements, ModelRequirementsError, ModelStructuredOutputCapabilities,
    ModelStructuredOutputCapabilitiesError, ModelStructuredOutputLevel, ModelTokenLimits,
    ModelTokenLimitsError, ModelToolCapabilities, ModelToolCapabilitiesError, ModelToolChoice,
    ModelToolChoices, ModelToolChoicesError, ModelToolRequirements, ModelToolRequirementsError,
};
pub use schema::{SchemaId, SchemaIdError, SchemaReference};
pub use scope::{Scope, ScopeError, ScopeSet, ScopeSetError};
pub use time::{DurationMillis, DurationMillisError, Timestamp, TimestampError};
pub use tool::{
    ToolCancellationSupport, ToolDescriptor, ToolDescriptorError, ToolExecutionLimits,
    ToolExecutionLimitsError, ToolExecutionSemantics, ToolExecutionSemanticsError, ToolIdempotency,
    ToolInvocationCapabilities, ToolResourceAccess, ToolResourceRequirements, ToolRisk,
};
pub use version::{Version, VersionComponent, VersionError};
