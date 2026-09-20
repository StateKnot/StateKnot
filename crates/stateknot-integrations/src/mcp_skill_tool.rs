// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Exact-version Tool bindings guarded by one active MCP Skill window.
//!
//! The adapter is intentionally an [`ErasedTool`] rather than an alternate
//! dispatcher. Registering it in the ordinary immutable Tool registry keeps
//! `StateKnot`'s durable attempt-start, terminal evidence, retry, and
//! reconciliation boundaries in force. The active Skill policy is evaluated
//! after the durable attempt start and immediately before every provider call.

use std::{fmt, sync::Arc};

use serde::Serialize;
use stateknot_core::{
    AttemptId, AuthorizationReceiptId, CapabilityIdentity, Digest, DurationMillis, ErasedTool,
    EventId, Failure, FailureCategory, FailureCode, FailureId, FailureMessage, FailureOrigin,
    InvocationId, RetryAdvice, RunId, TenantId, ThreadId, Timestamp, ToolAuthorizationOperation,
    ToolAuthorizationProvenance, ToolAuthorizationReceipt, ToolAuthorizationReceiptSink,
    ToolAuthorizationReceiptSinkError, ToolAuthorizationReceiptSinkFailure, ToolContext,
    ToolDescriptor, ToolError, ToolErrorPhase, ToolErrorProvenance, ToolExternalEffect, ToolInput,
    ToolReconciliationContext, ToolReconciliationObservation, ToolReconciliationProbeError,
    ToolResult, ToolRisk,
};
use thiserror::Error;

use crate::{McpActivatedSkill, McpSkillExecutionPermit, McpSkillHostError};

const SKILL_SUBJECT_DOMAIN: &[u8] = b"stateknot.mcp-skill-tool-subject.v1\0";

/// Provider operation covered by one Skill policy decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpSkillToolOperation {
    /// Execute the admitted business Tool attempt.
    Execute,
    /// Query authoritative state for an already-started ambiguous attempt.
    Reconcile,
}

/// Exact bounded invocation facts available to a Skill Tool policy.
///
/// [`Debug`] for the retained [`ToolInput`] discloses only schema and resource
/// statistics. Policy code may explicitly inspect [`Self::input`] when its
/// decision depends on arguments, but audit sinks must not log those values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpSkillToolInvocation {
    tenant_id: TenantId,
    run_id: RunId,
    thread_id: ThreadId,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    origin_event_id: Option<EventId>,
    has_recovery_handle: bool,
    input: ToolInput,
}

impl McpSkillToolInvocation {
    fn for_execution(context: &ToolContext, input: &ToolInput) -> Self {
        Self {
            tenant_id: context.tenant_id().clone(),
            run_id: context.run_id(),
            thread_id: context.thread_id(),
            invocation_id: context.invocation_id(),
            attempt_id: context.attempt_id(),
            origin_event_id: context.origin_event_id(),
            has_recovery_handle: false,
            input: input.clone(),
        }
    }

    fn for_reconciliation(context: &ToolReconciliationContext, input: &ToolInput) -> Self {
        Self {
            tenant_id: context.tenant_id().clone(),
            run_id: context.run_id(),
            thread_id: context.thread_id(),
            invocation_id: context.invocation_id(),
            attempt_id: context.attempt_id(),
            origin_event_id: context.origin_event_id(),
            has_recovery_handle: context.recovery_handle().is_some(),
            input: input.clone(),
        }
    }

    /// Returns the trusted tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the enclosing durable run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the enclosing conversation thread.
    #[must_use]
    pub const fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    /// Returns the logical Tool invocation.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the original physical Tool attempt.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the committed event authorizing provider I/O when available.
    #[must_use]
    pub const fn origin_event_id(&self) -> Option<EventId> {
        self.origin_event_id
    }

    /// Returns whether reconciliation has an opaque provider recovery handle.
    ///
    /// The handle itself is deliberately not disclosed to policy.
    #[must_use]
    pub const fn has_recovery_handle(&self) -> bool {
        self.has_recovery_handle
    }

    /// Returns the exact schema-bound, resource-limited Tool arguments.
    #[must_use]
    pub const fn input(&self) -> &ToolInput {
        &self.input
    }
}

/// Explicit Host-code exposure declaration for a bound Tool.
///
/// This is a trusted Host assertion, not a value inferred from remote Skill
/// content. Choose [`Self::Possible`] whenever Skill-controlled input can cause
/// code to execute in the Host process or its local operating environment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpSkillHostCodeExecution {
    /// The Tool cannot execute code in the Host environment.
    NotPossible,
    /// The Tool can execute code in the Host environment.
    Possible,
}

impl McpSkillHostCodeExecution {
    const fn is_possible(self) -> bool {
        matches!(self, Self::Possible)
    }
}

/// Immutable, exact-version facts sent to the Skill Host policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpSkillToolBinding {
    tool_name: Arc<str>,
    identity: CapabilityIdentity,
    descriptor_digest: Digest,
    host_code_execution: bool,
}

impl McpSkillToolBinding {
    fn new(
        descriptor: &ToolDescriptor,
        host_code_execution: McpSkillHostCodeExecution,
    ) -> Result<Self, McpSkillBoundToolBuildError> {
        Ok(Self {
            tool_name: Arc::from(descriptor.metadata().identity().name().as_str()),
            identity: descriptor.metadata().identity().clone(),
            descriptor_digest: ToolAuthorizationReceipt::digest_descriptor(descriptor)
                .map_err(|_| McpSkillBoundToolBuildError::DescriptorEncoding)?,
            host_code_execution: host_code_execution.is_possible(),
        })
    }

    /// Returns the exact model-visible Tool name.
    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }

    /// Returns the owner-qualified, version-pinned Tool identity.
    #[must_use]
    pub const fn identity(&self) -> &CapabilityIdentity {
        &self.identity
    }

    /// Returns the domain-separated digest of the complete Tool descriptor.
    #[must_use]
    pub const fn descriptor_digest(&self) -> Digest {
        self.descriptor_digest
    }

    /// Returns whether Host-code execution was disclosed to policy.
    #[must_use]
    pub const fn host_code_execution(&self) -> bool {
        self.host_code_execution
    }
}

/// One exact Tool provider guarded by an active, content-bound Skill policy.
///
/// Construct this adapter after Skill activation, then register the adapter—not
/// the unguarded provider—in `stateknot_runtime::ToolProviderRegistryBuilder`.
/// The type itself depends only on `stateknot-core`, so integrations retains a
/// one-way dependency graph.
pub struct McpSkillBoundTool {
    skill: Arc<McpActivatedSkill>,
    provider: Arc<dyn ErasedTool>,
    descriptor: ToolDescriptor,
    binding: McpSkillToolBinding,
    receipt_sink: Arc<dyn ToolAuthorizationReceiptSink>,
}

impl McpSkillBoundTool {
    /// Freezes one exact provider descriptor under the active Skill window.
    ///
    /// # Errors
    ///
    /// Fails if the complete descriptor cannot be canonically encoded.
    pub fn new(
        skill: Arc<McpActivatedSkill>,
        provider: Arc<dyn ErasedTool>,
        receipt_sink: Arc<dyn ToolAuthorizationReceiptSink>,
        host_code_execution: McpSkillHostCodeExecution,
    ) -> Result<Self, McpSkillBoundToolBuildError> {
        let descriptor = provider.descriptor().clone();
        let binding = McpSkillToolBinding::new(&descriptor, host_code_execution)?;
        Ok(Self {
            skill,
            provider,
            descriptor,
            binding,
            receipt_sink,
        })
    }

    /// Returns the exact policy-visible binding snapshot.
    #[must_use]
    pub const fn binding(&self) -> &McpSkillToolBinding {
        &self.binding
    }

    /// Returns the origin-scoped active Skill guarding this provider.
    #[must_use]
    pub fn skill(&self) -> &McpActivatedSkill {
        &self.skill
    }

    fn descriptor_is_current(&self) -> bool {
        self.provider.descriptor() == &self.descriptor
    }
}

impl fmt::Debug for McpSkillBoundTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpSkillBoundTool")
            .field("skill", self.skill.identity())
            .field("manifest_digest", &self.skill.entry().manifest_digest())
            .field("binding", &self.binding)
            .field("supports_reconciliation", &self.supports_reconciliation())
            .finish_non_exhaustive()
    }
}

impl ErasedTool for McpSkillBoundTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn supports_reconciliation(&self) -> bool {
        self.provider.supports_reconciliation()
    }

    fn call(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> stateknot_core::BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            if !self.descriptor_is_current() {
                return Err(tool_gate_error(
                    &context,
                    &self.descriptor,
                    GateFailure::DescriptorDrift,
                ));
            }
            let invocation = McpSkillToolInvocation::for_execution(&context, &input);
            let permit = self
                .skill
                .authorize_bound_tool_operation(
                    &self.binding,
                    McpSkillToolOperation::Execute,
                    &invocation,
                )
                .await
                .map_err(|source| {
                    tool_gate_error(
                        &context,
                        &self.descriptor,
                        GateFailure::Authorization(source),
                    )
                })?;
            if !permit_matches(
                &permit,
                &self.binding,
                McpSkillToolOperation::Execute,
                &invocation,
            ) {
                return Err(tool_gate_error(
                    &context,
                    &self.descriptor,
                    GateFailure::PermitMismatch,
                ));
            }
            persist_authorization_receipt(
                self.receipt_sink.as_ref(),
                &permit,
                &self.binding,
                &invocation,
                context.observed_at(),
            )
            .await
            .map_err(|failure| tool_gate_error(&context, &self.descriptor, failure))?;
            dispatch_call(permit, Arc::clone(&self.provider), context, input).await
        })
    }

    fn reconcile(
        &self,
        context: ToolReconciliationContext,
        input: ToolInput,
    ) -> stateknot_core::BoxFuture<
        '_,
        Result<ToolReconciliationObservation, ToolReconciliationProbeError>,
    > {
        Box::pin(async move {
            if !self.descriptor_is_current() {
                return Err(reconciliation_gate_error(GateFailure::DescriptorDrift));
            }
            let invocation = McpSkillToolInvocation::for_reconciliation(&context, &input);
            let permit = self
                .skill
                .authorize_bound_tool_operation(
                    &self.binding,
                    McpSkillToolOperation::Reconcile,
                    &invocation,
                )
                .await
                .map_err(|source| reconciliation_gate_error(GateFailure::Authorization(source)))?;
            if !permit_matches(
                &permit,
                &self.binding,
                McpSkillToolOperation::Reconcile,
                &invocation,
            ) {
                return Err(reconciliation_gate_error(GateFailure::PermitMismatch));
            }
            persist_authorization_receipt(
                self.receipt_sink.as_ref(),
                &permit,
                &self.binding,
                &invocation,
                context.observed_at(),
            )
            .await
            .map_err(reconciliation_gate_error)?;
            dispatch_reconciliation(permit, Arc::clone(&self.provider), context, input).await
        })
    }
}

async fn dispatch_call(
    _permit: McpSkillExecutionPermit<'_>,
    provider: Arc<dyn ErasedTool>,
    context: ToolContext,
    input: ToolInput,
) -> Result<ToolResult, ToolError> {
    provider.call(context, input).await
}

async fn dispatch_reconciliation(
    _permit: McpSkillExecutionPermit<'_>,
    provider: Arc<dyn ErasedTool>,
    context: ToolReconciliationContext,
    input: ToolInput,
) -> Result<ToolReconciliationObservation, ToolReconciliationProbeError> {
    provider.reconcile(context, input).await
}

fn permit_matches(
    permit: &McpSkillExecutionPermit<'_>,
    binding: &McpSkillToolBinding,
    operation: McpSkillToolOperation,
    invocation: &McpSkillToolInvocation,
) -> bool {
    permit.tool_name() == binding.tool_name()
        && permit.tool_identity() == Some(binding.identity())
        && permit.tool_descriptor_digest() == Some(binding.descriptor_digest())
        && permit.operation() == operation
        && permit.invocation() == Some(invocation)
        && permit.host_code_execution() == binding.host_code_execution()
}

async fn persist_authorization_receipt(
    sink: &dyn ToolAuthorizationReceiptSink,
    permit: &McpSkillExecutionPermit<'_>,
    binding: &McpSkillToolBinding,
    invocation: &McpSkillToolInvocation,
    authorized_at: Timestamp,
) -> Result<(), GateFailure> {
    let origin_event_id = invocation
        .origin_event_id()
        .ok_or(GateFailure::MissingDurableOrigin)?;
    let subject_digest = skill_subject_digest(permit, binding)?;
    let input_digest = ToolAuthorizationReceipt::digest_input(invocation.input())
        .map_err(|_| GateFailure::ReceiptEncoding)?;
    let grant = permit.grant();
    let operation = match permit.operation() {
        McpSkillToolOperation::Execute => ToolAuthorizationOperation::Execute,
        McpSkillToolOperation::Reconcile => ToolAuthorizationOperation::Reconcile,
    };
    let receipt = ToolAuthorizationReceipt::new(
        AuthorizationReceiptId::generate(),
        ToolAuthorizationProvenance::new(
            invocation.tenant_id().clone(),
            invocation.run_id(),
            invocation.thread_id(),
            invocation.invocation_id(),
            invocation.attempt_id(),
            origin_event_id,
        ),
        operation,
        binding.identity().clone(),
        binding.descriptor_digest(),
        input_digest,
        subject_digest,
        grant.policy().clone(),
        grant.policy_digest(),
        grant.decision_digest(),
        authorized_at,
        invocation.has_recovery_handle(),
    )
    .map_err(|_| GateFailure::ReceiptEncoding)?;
    sink.record(receipt).await.map_err(GateFailure::ReceiptSink)
}

fn skill_subject_digest(
    permit: &McpSkillExecutionPermit<'_>,
    binding: &McpSkillToolBinding,
) -> Result<Digest, GateFailure> {
    #[derive(Serialize)]
    struct Subject<'a> {
        origin: &'a str,
        skill_uri: &'a str,
        manifest_digest: &'a str,
        activation_id: u64,
        tool_name: &'a str,
        tool_identity: &'a CapabilityIdentity,
        descriptor_digest: Digest,
        host_code_execution: bool,
    }

    let canonical = serde_json_canonicalizer::to_vec(&Subject {
        origin: permit.identity().origin().as_str(),
        skill_uri: permit.identity().uri(),
        manifest_digest: permit.manifest_digest(),
        activation_id: permit.activation_id(),
        tool_name: binding.tool_name(),
        tool_identity: binding.identity(),
        descriptor_digest: binding.descriptor_digest(),
        host_code_execution: binding.host_code_execution(),
    })
    .map_err(|_| GateFailure::ReceiptEncoding)?;
    let mut preimage = Vec::with_capacity(SKILL_SUBJECT_DOMAIN.len() + 8 + canonical.len());
    preimage.extend_from_slice(SKILL_SUBJECT_DOMAIN);
    preimage.extend_from_slice(
        &u64::try_from(canonical.len())
            .expect("Skill authorization subject length fits u64")
            .to_be_bytes(),
    );
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

#[derive(Debug, Error)]
enum GateFailure {
    #[error("active MCP Skill policy rejected the exact Tool operation")]
    Authorization(#[source] McpSkillHostError),
    #[error("installed Tool descriptor changed after Skill binding")]
    DescriptorDrift,
    #[error("MCP Skill execution permit does not match the bound Tool operation")]
    PermitMismatch,
    #[error("durable Tool origin event is missing")]
    MissingDurableOrigin,
    #[error("MCP Skill Tool authorization receipt cannot be encoded")]
    ReceiptEncoding,
    #[error("MCP Skill Tool authorization receipt was not durably accepted")]
    ReceiptSink(#[source] ToolAuthorizationReceiptSinkError),
}

fn gate_failure_shape(failure: &GateFailure) -> (FailureCategory, &'static str, &'static str) {
    match failure {
        GateFailure::Authorization(McpSkillHostError::ToolCallDenied) => (
            FailureCategory::PolicyDenied,
            "stateknot.mcp_skill_tool.policy_denied",
            "The active Skill policy denied this exact Tool operation.",
        ),
        GateFailure::Authorization(McpSkillHostError::PolicyUnavailable) => (
            FailureCategory::DependencyUnavailable,
            "stateknot.mcp_skill_tool.policy_unavailable",
            "The active Skill policy could not authorize this Tool operation.",
        ),
        GateFailure::Authorization(_) | GateFailure::PermitMismatch => (
            FailureCategory::Internal,
            "stateknot.mcp_skill_tool.authorization_invalid",
            "The Skill Tool authorization boundary returned invalid evidence.",
        ),
        GateFailure::DescriptorDrift => (
            FailureCategory::DataCorruption,
            "stateknot.mcp_skill_tool.descriptor_drift",
            "The installed Tool descriptor differs from its Skill binding.",
        ),
        GateFailure::MissingDurableOrigin => (
            FailureCategory::DataCorruption,
            "stateknot.mcp_skill_tool.origin_missing",
            "The Tool operation has no durable origin event for authorization evidence.",
        ),
        GateFailure::ReceiptEncoding => (
            FailureCategory::Internal,
            "stateknot.mcp_skill_tool.receipt_encoding",
            "The Tool authorization evidence could not be encoded.",
        ),
        GateFailure::ReceiptSink(error)
            if error.failure() == ToolAuthorizationReceiptSinkFailure::Unavailable =>
        {
            (
                FailureCategory::DependencyUnavailable,
                "stateknot.mcp_skill_tool.receipt_unavailable",
                "The Tool authorization evidence could not be made durable.",
            )
        }
        GateFailure::ReceiptSink(_) => (
            FailureCategory::DataCorruption,
            "stateknot.mcp_skill_tool.receipt_rejected",
            "The durable store rejected the Tool authorization evidence.",
        ),
    }
}

fn common_failure(failure: GateFailure) -> Failure {
    let (category, code, message) = gate_failure_shape(&failure);
    let retry = match &failure {
        GateFailure::ReceiptSink(error)
            if error.failure() == ToolAuthorizationReceiptSinkFailure::Unavailable =>
        {
            RetryAdvice::SafeAfter {
                delay: DurationMillis::new(250).expect("positive constant"),
            }
        }
        _ => RetryAdvice::Never,
    };
    Failure::new(
        FailureId::generate(),
        category,
        FailureCode::new(code).expect("static Skill Tool failure code is valid"),
        FailureOrigin::new("stateknot.mcp_skill_tool")
            .expect("static Skill Tool failure origin is valid"),
        FailureMessage::new(message).expect("static Skill Tool failure message is valid"),
        retry,
    )
    .expect("static Skill Tool failure semantics are coherent")
    .with_private_source(failure)
}

fn tool_gate_error(
    context: &ToolContext,
    descriptor: &ToolDescriptor,
    failure: GateFailure,
) -> ToolError {
    let effect = if descriptor.semantics().risk() == ToolRisk::ReadOnly {
        ToolExternalEffect::NotApplicable
    } else {
        ToolExternalEffect::NotStarted
    };
    ToolError::new(
        common_failure(failure),
        ToolErrorPhase::Preparation,
        effect,
        ToolErrorProvenance::for_invocation(context, descriptor),
    )
    .expect("Skill authorization occurs before provider execution")
}

fn reconciliation_gate_error(failure: GateFailure) -> ToolReconciliationProbeError {
    ToolReconciliationProbeError::new(common_failure(failure))
        .expect("Skill reconciliation authorization never requests recursive reconciliation")
}

/// Failure to freeze an exact Skill-guarded Tool binding.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum McpSkillBoundToolBuildError {
    /// The immutable Tool descriptor could not be canonically encoded.
    #[error("MCP Skill Tool descriptor cannot be canonically encoded")]
    DescriptorEncoding,
}
