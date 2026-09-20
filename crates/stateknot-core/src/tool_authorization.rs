// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Provider-neutral, payload-redacted Tool authorization receipts.

use std::{error::Error as StdError, fmt, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    AttemptId, AuthorizationReceiptId, BoxFuture, CapabilityIdentity, Digest, EventId,
    InvocationId, RunId, TenantId, ThreadId, Timestamp, ToolDescriptor, ToolInput,
};

const RECEIPT_DIGEST_DOMAIN: &[u8] = b"stateknot.tool-authorization-receipt.v1\0";
const DESCRIPTOR_DIGEST_DOMAIN: &[u8] = b"stateknot.tool-authorization-descriptor.v1\0";
const INPUT_DIGEST_DOMAIN: &[u8] = b"stateknot.tool-authorization-input.v1\0";

/// Provider operation authorized by one immutable receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolAuthorizationOperation {
    /// Execute one already-started physical Tool attempt.
    Execute,
    /// Query authoritative state for an ambiguous physical attempt.
    Reconcile,
}

/// Exact durable invocation identity covered by an authorization decision.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_field_names,
    reason = "explicit ID suffixes keep every durable provenance boundary unambiguous"
)]
pub struct ToolAuthorizationProvenance {
    tenant_id: TenantId,
    run_id: RunId,
    thread_id: ThreadId,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    origin_event_id: EventId,
}

impl ToolAuthorizationProvenance {
    /// Constructs exact provenance for one durable-before-dispatch Tool attempt.
    #[must_use]
    pub const fn new(
        tenant_id: TenantId,
        run_id: RunId,
        thread_id: ThreadId,
        invocation_id: InvocationId,
        attempt_id: AttemptId,
        origin_event_id: EventId,
    ) -> Self {
        Self {
            tenant_id,
            run_id,
            thread_id,
            invocation_id,
            attempt_id,
            origin_event_id,
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

    /// Returns the already-started physical attempt.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the committed event that authorized provider I/O.
    #[must_use]
    pub const fn origin_event_id(&self) -> EventId {
        self.origin_event_id
    }
}

/// Immutable approval evidence that must be durable before provider I/O.
///
/// Tool arguments and policy payloads are never retained. Domain-separated
/// digests bind those values to the exact receipt without turning the receipt
/// table into a secret store. A receipt proves authorization, not dispatch or
/// external application of the Tool operation. Each provider call receives a
/// fresh authorization receipt, even when it belongs to the same durable Tool
/// attempt: a receipt whose commit acknowledgement was lost never proves that
/// provider I/O happened, so a later call must be authorized independently.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolAuthorizationReceipt {
    receipt_id: AuthorizationReceiptId,
    provenance: ToolAuthorizationProvenance,
    operation: ToolAuthorizationOperation,
    tool: CapabilityIdentity,
    descriptor_digest: Digest,
    input_digest: Digest,
    subject_digest: Digest,
    policy: CapabilityIdentity,
    policy_digest: Digest,
    decision_digest: Digest,
    authorized_at: Timestamp,
    has_recovery_handle: bool,
    receipt_digest: Digest,
}

impl ToolAuthorizationReceipt {
    /// Computes the domain-separated binding for one complete Tool descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`ToolAuthorizationReceiptError::Encoding`] if canonical RFC
    /// 8785 encoding unexpectedly fails.
    pub fn digest_descriptor(
        descriptor: &ToolDescriptor,
    ) -> Result<Digest, ToolAuthorizationReceiptError> {
        digest_canonical(DESCRIPTOR_DIGEST_DOMAIN, descriptor)
    }

    /// Computes the domain-separated binding for exact schema-pinned arguments.
    ///
    /// The input value is never included in the returned receipt.
    ///
    /// # Errors
    ///
    /// Returns [`ToolAuthorizationReceiptError::Encoding`] if canonical RFC
    /// 8785 encoding unexpectedly fails.
    pub fn digest_input(input: &ToolInput) -> Result<Digest, ToolAuthorizationReceiptError> {
        digest_canonical(INPUT_DIGEST_DOMAIN, input)
    }

    /// Constructs a complete, integrity-bound authorization receipt.
    ///
    /// # Errors
    ///
    /// Returns [`ToolAuthorizationReceiptError::Encoding`] if canonical RFC
    /// 8785 encoding unexpectedly fails.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        receipt_id: AuthorizationReceiptId,
        provenance: ToolAuthorizationProvenance,
        operation: ToolAuthorizationOperation,
        tool: CapabilityIdentity,
        descriptor_digest: Digest,
        input_digest: Digest,
        subject_digest: Digest,
        policy: CapabilityIdentity,
        policy_digest: Digest,
        decision_digest: Digest,
        authorized_at: Timestamp,
        has_recovery_handle: bool,
    ) -> Result<Self, ToolAuthorizationReceiptError> {
        let mut receipt = Self {
            receipt_id,
            provenance,
            operation,
            tool,
            descriptor_digest,
            input_digest,
            subject_digest,
            policy,
            policy_digest,
            decision_digest,
            authorized_at,
            has_recovery_handle,
            receipt_digest: Digest::sha256(b""),
        };
        receipt.receipt_digest = receipt.compute_digest()?;
        Ok(receipt)
    }

    /// Returns the stable idempotency identity for this authorization fact.
    #[must_use]
    pub const fn receipt_id(&self) -> AuthorizationReceiptId {
        self.receipt_id
    }

    /// Returns exact durable attempt provenance.
    #[must_use]
    pub const fn provenance(&self) -> &ToolAuthorizationProvenance {
        &self.provenance
    }

    /// Returns the authorized provider operation.
    #[must_use]
    pub const fn operation(&self) -> ToolAuthorizationOperation {
        self.operation
    }

    /// Returns the exact owner-qualified Tool version.
    #[must_use]
    pub const fn tool(&self) -> &CapabilityIdentity {
        &self.tool
    }

    /// Returns the complete Tool descriptor binding digest.
    #[must_use]
    pub const fn descriptor_digest(&self) -> Digest {
        self.descriptor_digest
    }

    /// Returns the domain-separated exact Tool input digest.
    #[must_use]
    pub const fn input_digest(&self) -> Digest {
        self.input_digest
    }

    /// Returns the protocol/application authorization-subject digest.
    #[must_use]
    pub const fn subject_digest(&self) -> Digest {
        self.subject_digest
    }

    /// Returns the exact policy implementation identity.
    #[must_use]
    pub const fn policy(&self) -> &CapabilityIdentity {
        &self.policy
    }

    /// Returns the immutable policy artifact digest.
    #[must_use]
    pub const fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// Returns the policy-supplied exact decision evidence digest.
    #[must_use]
    pub const fn decision_digest(&self) -> Digest {
        self.decision_digest
    }

    /// Returns the trusted runtime observation at authorization.
    #[must_use]
    pub const fn authorized_at(&self) -> Timestamp {
        self.authorized_at
    }

    /// Returns whether reconciliation had an opaque provider recovery handle.
    #[must_use]
    pub const fn has_recovery_handle(&self) -> bool {
        self.has_recovery_handle
    }

    /// Returns the domain-separated digest covering every receipt field.
    #[must_use]
    pub const fn receipt_digest(&self) -> Digest {
        self.receipt_digest
    }

    fn compute_digest(&self) -> Result<Digest, ToolAuthorizationReceiptError> {
        #[derive(Serialize)]
        struct Preimage<'a> {
            receipt_id: AuthorizationReceiptId,
            provenance: &'a ToolAuthorizationProvenance,
            operation: ToolAuthorizationOperation,
            tool: &'a CapabilityIdentity,
            descriptor_digest: Digest,
            input_digest: Digest,
            subject_digest: Digest,
            policy: &'a CapabilityIdentity,
            policy_digest: Digest,
            decision_digest: Digest,
            authorized_at: Timestamp,
            has_recovery_handle: bool,
        }

        let canonical = serde_json_canonicalizer::to_vec(&Preimage {
            receipt_id: self.receipt_id,
            provenance: &self.provenance,
            operation: self.operation,
            tool: &self.tool,
            descriptor_digest: self.descriptor_digest,
            input_digest: self.input_digest,
            subject_digest: self.subject_digest,
            policy: &self.policy,
            policy_digest: self.policy_digest,
            decision_digest: self.decision_digest,
            authorized_at: self.authorized_at,
            has_recovery_handle: self.has_recovery_handle,
        })
        .map_err(|_| ToolAuthorizationReceiptError::Encoding)?;
        let mut bytes = Vec::with_capacity(RECEIPT_DIGEST_DOMAIN.len() + 8 + canonical.len());
        bytes.extend_from_slice(RECEIPT_DIGEST_DOMAIN);
        bytes.extend_from_slice(
            &u64::try_from(canonical.len())
                .expect("authorization receipt length fits u64")
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&canonical);
        Ok(Digest::sha256(bytes))
    }
}

fn digest_canonical<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, ToolAuthorizationReceiptError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| ToolAuthorizationReceiptError::Encoding)?;
    let mut bytes = Vec::with_capacity(domain.len() + 8 + canonical.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(
        &u64::try_from(canonical.len())
            .expect("canonical authorization binding length fits u64")
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&canonical);
    Ok(Digest::sha256(bytes))
}

impl fmt::Debug for ToolAuthorizationReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolAuthorizationReceipt")
            .field("receipt_id", &self.receipt_id)
            .field("provenance", &self.provenance)
            .field("operation", &self.operation)
            .field("tool", &self.tool)
            .field("descriptor_digest", &self.descriptor_digest)
            .field("input_digest", &self.input_digest)
            .field("subject_digest", &self.subject_digest)
            .field("policy", &self.policy)
            .field("policy_digest", &self.policy_digest)
            .field("decision_digest", &self.decision_digest)
            .field("authorized_at", &self.authorized_at)
            .field("has_recovery_handle", &self.has_recovery_handle)
            .field("receipt_digest", &self.receipt_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ToolAuthorizationReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            receipt_id: AuthorizationReceiptId,
            provenance: ToolAuthorizationProvenance,
            operation: ToolAuthorizationOperation,
            tool: CapabilityIdentity,
            descriptor_digest: Digest,
            input_digest: Digest,
            subject_digest: Digest,
            policy: CapabilityIdentity,
            policy_digest: Digest,
            decision_digest: Digest,
            authorized_at: Timestamp,
            has_recovery_handle: bool,
            receipt_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        let receipt = Self::new(
            wire.receipt_id,
            wire.provenance,
            wire.operation,
            wire.tool,
            wire.descriptor_digest,
            wire.input_digest,
            wire.subject_digest,
            wire.policy,
            wire.policy_digest,
            wire.decision_digest,
            wire.authorized_at,
            wire.has_recovery_handle,
        )
        .map_err(de::Error::custom)?;
        if receipt.receipt_digest != wire.receipt_digest {
            return Err(de::Error::custom(
                ToolAuthorizationReceiptError::DigestMismatch,
            ));
        }
        Ok(receipt)
    }
}

/// Invalid or corrupted Tool authorization receipt.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolAuthorizationReceiptError {
    /// Canonical receipt encoding failed.
    #[error("tool authorization receipt canonical encoding failed")]
    Encoding,
    /// Decoded fields did not reproduce the retained integrity digest.
    #[error("tool authorization receipt digest does not match its fields")]
    DigestMismatch,
}

/// Stable failure class returned by a durable authorization-receipt sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolAuthorizationReceiptSinkFailure {
    /// Durable storage could not accept the receipt now.
    Unavailable,
    /// The receipt conflicted with or failed durable validation.
    Rejected,
}

#[derive(Debug)]
struct PrivateReceiptSinkSource(Arc<dyn StdError + Send + Sync + 'static>);

impl fmt::Display for PrivateReceiptSinkSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("private Tool authorization receipt sink failure")
    }
}

impl StdError for PrivateReceiptSinkSource {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Payload-redacted durable receipt sink failure.
pub struct ToolAuthorizationReceiptSinkError {
    failure: ToolAuthorizationReceiptSinkFailure,
    private_source: PrivateReceiptSinkSource,
}

impl ToolAuthorizationReceiptSinkError {
    /// Wraps a private provider diagnostic with a stable public classification.
    #[must_use]
    pub fn new<E>(failure: ToolAuthorizationReceiptSinkFailure, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            failure,
            private_source: PrivateReceiptSinkSource(Arc::new(source)),
        }
    }

    /// Returns whether storage was unavailable or rejected the receipt.
    #[must_use]
    pub const fn failure(&self) -> ToolAuthorizationReceiptSinkFailure {
        self.failure
    }

    /// Returns the private diagnostic to trusted in-process callers only.
    #[must_use]
    pub fn private_source(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self.private_source.0.as_ref()
    }
}

impl fmt::Debug for ToolAuthorizationReceiptSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolAuthorizationReceiptSinkError")
            .field("failure", &self.failure)
            .field("has_private_source", &true)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ToolAuthorizationReceiptSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Tool authorization receipt sink failed")
    }
}

impl StdError for ToolAuthorizationReceiptSinkError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.private_source)
    }
}

/// Mandatory durable-before-provider-I/O receipt boundary.
///
/// Successful completion means the exact immutable receipt is durable or an
/// exact idempotent record was recovered. Implementations must reject identity
/// reuse with different bytes and must never log Tool argument values. They
/// must not collapse distinct receipt identities merely because their durable
/// Tool provenance matches; those identities represent independently
/// authorized provider calls.
pub trait ToolAuthorizationReceiptSink: Send + Sync + 'static {
    /// Durably records one exact authorization decision before provider I/O.
    fn record(
        &self,
        receipt: ToolAuthorizationReceipt,
    ) -> BoxFuture<'_, Result<(), ToolAuthorizationReceiptSinkError>>;
}

#[cfg(test)]
mod tests {
    use std::{error::Error as StdError, fmt};

    use serde_json::{Value, from_value, to_value};

    use super::*;
    use crate::{
        CapabilityName, CapabilityReference, IssuerId, PrincipalIdentity, SubjectId, Version,
    };

    fn principal(subject: &str) -> PrincipalIdentity {
        PrincipalIdentity::new(
            "https://issuer.example.com/tenant"
                .parse::<IssuerId>()
                .unwrap(),
            subject.parse::<SubjectId>().unwrap(),
        )
    }

    fn capability(owner: &str, name: &str, version: Version) -> CapabilityIdentity {
        CapabilityIdentity::new(
            principal(owner),
            CapabilityReference::new(name.parse::<CapabilityName>().unwrap(), version),
        )
    }

    fn receipt() -> ToolAuthorizationReceipt {
        ToolAuthorizationReceipt::new(
            "018f1f65-45d3-7a2e-8a19-4de38b78345f".parse().unwrap(),
            ToolAuthorizationProvenance::new(
                "tenant-a".parse().unwrap(),
                "018f1f65-45d3-7a2e-8a19-4de38b783460".parse().unwrap(),
                "018f1f65-45d3-7a2e-8a19-4de38b783461".parse().unwrap(),
                "018f1f65-45d3-7a2e-8a19-4de38b783462".parse().unwrap(),
                "018f1f65-45d3-7a2e-8a19-4de38b783463".parse().unwrap(),
                "018f1f65-45d3-7a2e-8a19-4de38b783464".parse().unwrap(),
            ),
            ToolAuthorizationOperation::Execute,
            capability("tool-registry", "filesystem.read", Version::new(2, 1, 0)),
            Digest::sha256(b"complete descriptor"),
            Digest::sha256(b"private tool input"),
            Digest::sha256(b"mcp skill subject"),
            capability(
                "policy-registry",
                "skill-tool-policy",
                Version::new(3, 0, 4),
            ),
            Digest::sha256(b"policy artifact"),
            Digest::sha256(b"decision evidence"),
            "2026-09-20T12:34:56.123456Z".parse().unwrap(),
            true,
        )
        .unwrap()
    }

    #[test]
    fn receipt_round_trips_and_rejects_field_or_digest_tampering() {
        let receipt = receipt();
        let value = to_value(&receipt).unwrap();
        assert_eq!(
            from_value::<ToolAuthorizationReceipt>(value.clone()).unwrap(),
            receipt
        );

        let mut changed_input = value.clone();
        changed_input["input_digest"] = to_value(Digest::sha256(b"different input")).unwrap();
        assert!(
            from_value::<ToolAuthorizationReceipt>(changed_input)
                .unwrap_err()
                .to_string()
                .contains("digest does not match")
        );

        let mut changed_digest = value;
        changed_digest["receipt_digest"] = to_value(Digest::sha256(b"forged")).unwrap();
        assert!(
            from_value::<ToolAuthorizationReceipt>(changed_digest)
                .unwrap_err()
                .to_string()
                .contains("digest does not match")
        );
    }

    #[test]
    fn receipt_digest_changes_for_every_security_relevant_operation_bit() {
        let execute = receipt();
        let reconcile = ToolAuthorizationReceipt::new(
            execute.receipt_id(),
            execute.provenance().clone(),
            ToolAuthorizationOperation::Reconcile,
            execute.tool().clone(),
            execute.descriptor_digest(),
            execute.input_digest(),
            execute.subject_digest(),
            execute.policy().clone(),
            execute.policy_digest(),
            execute.decision_digest(),
            execute.authorized_at(),
            execute.has_recovery_handle(),
        )
        .unwrap();

        assert_ne!(execute.receipt_digest(), reconcile.receipt_digest());
    }

    #[test]
    fn wire_form_contains_only_identity_and_digest_evidence() {
        let serialized = serde_json::to_string(&receipt()).unwrap();
        assert!(!serialized.contains("private tool input"));
        assert!(!serialized.contains("decision evidence"));
        assert!(!serialized.contains("policy artifact"));

        let value = serde_json::from_str::<Value>(&serialized).unwrap();
        assert!(value.get("input").is_none());
        assert!(value.get("policy_payload").is_none());
        assert!(value.get("decision_payload").is_none());
    }

    #[derive(Debug)]
    struct SecretSource;

    impl fmt::Display for SecretSource {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("private-database-diagnostic")
        }
    }

    impl StdError for SecretSource {}

    #[test]
    fn sink_error_public_views_redact_private_source() {
        let error = ToolAuthorizationReceiptSinkError::new(
            ToolAuthorizationReceiptSinkFailure::Unavailable,
            SecretSource,
        );

        assert_eq!(
            error.failure(),
            ToolAuthorizationReceiptSinkFailure::Unavailable
        );
        assert!(!error.to_string().contains("private-database-diagnostic"));
        assert!(!format!("{error:?}").contains("private-database-diagnostic"));
        assert_eq!(
            error.private_source().to_string(),
            "private-database-diagnostic"
        );
    }
}
