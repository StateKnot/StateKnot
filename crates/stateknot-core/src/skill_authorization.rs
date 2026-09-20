// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Provider-neutral durable Skill activation approvals and acting windows.

use std::{error::Error as StdError, fmt, sync::Arc};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    BoxFuture, CapabilityIdentity, Digest, DurationMillis, RunId, SkillActingWindowId,
    SkillActivationApprovalId, TenantId, ThreadId, Timestamp,
};

const APPROVAL_DIGEST_DOMAIN: &[u8] = b"stateknot.skill-activation-approval.v1\0";
const WINDOW_DIGEST_DOMAIN: &[u8] = b"stateknot.skill-acting-window.v1\0";
const REVOCATION_DIGEST_DOMAIN: &[u8] = b"stateknot.skill-acting-window-revocation.v1\0";
const SUBJECT_DIGEST_DOMAIN: &[u8] = b"stateknot.skill-authorization-subject.v1\0";
const ACTING_WINDOW_DURATION_PATTERN: &str =
    "^(?:[1-9][0-9]{3,6}|[1-7][0-9]{7}|8[0-5][0-9]{6}|86[0-3][0-9]{5}|86400000)$";
const MAX_PROTOCOL_BYTES: usize = 32;
const MAX_ORIGIN_BYTES: usize = 128;
const MAX_URI_BYTES: usize = 4096;

/// Exact durable run scope in which one Skill may act.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
pub struct SkillActivationScope {
    tenant_id: TenantId,
    run_id: RunId,
    thread_id: ThreadId,
}

impl SkillActivationScope {
    /// Constructs a run-scoped Skill authority boundary.
    #[must_use]
    pub const fn new(tenant_id: TenantId, run_id: RunId, thread_id: ThreadId) -> Self {
        Self {
            tenant_id,
            run_id,
            thread_id,
        }
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the durable run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the conversation thread.
    #[must_use]
    pub const fn thread_id(&self) -> ThreadId {
        self.thread_id
    }
}

/// Exact external Skill content approved for activation.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SkillAuthorizationSubject {
    protocol: Box<str>,
    origin: Box<str>,
    uri: Box<str>,
    manifest_digest: Digest,
}

impl SkillAuthorizationSubject {
    /// Constructs a bounded, content-bound Skill subject.
    pub fn new(
        protocol: impl Into<String>,
        origin: impl Into<String>,
        uri: impl Into<String>,
        manifest_digest: Digest,
    ) -> Result<Self, SkillAuthorizationSubjectError> {
        let protocol = protocol.into();
        let origin = origin.into();
        let uri = uri.into();
        validate_label(&protocol, MAX_PROTOCOL_BYTES, true)?;
        validate_label(&origin, MAX_ORIGIN_BYTES, false)?;
        validate_label(&uri, MAX_URI_BYTES, false)?;
        Ok(Self {
            protocol: protocol.into_boxed_str(),
            origin: origin.into_boxed_str(),
            uri: uri.into_boxed_str(),
            manifest_digest,
        })
    }

    /// Returns the lower-case protocol profile.
    #[must_use]
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    /// Returns the host-assigned origin.
    #[must_use]
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Returns the exact provider-scoped Skill URI.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the complete immutable Manifest binding.
    #[must_use]
    pub const fn manifest_digest(&self) -> Digest {
        self.manifest_digest
    }

    /// Returns a domain-separated digest used by operation receipts.
    #[must_use]
    pub fn subject_digest(&self) -> Digest {
        digest_canonical(SUBJECT_DIGEST_DOMAIN, self)
            .expect("validated Skill subject canonicalizes")
    }
}

impl<'de> Deserialize<'de> for SkillAuthorizationSubject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            protocol: String,
            origin: String,
            uri: String,
            manifest_digest: Digest,
        }
        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.protocol, wire.origin, wire.uri, wire.manifest_digest)
            .map_err(de::Error::custom)
    }
}

/// Invalid external Skill subject.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SkillAuthorizationSubjectError {
    /// A required label was empty.
    #[error("Skill authorization subject contains an empty label")]
    Empty,
    /// A bounded label was too long.
    #[error("Skill authorization subject label is too long")]
    TooLong,
    /// A label contained boundary whitespace or a control character.
    #[error("Skill authorization subject label is not canonical")]
    NonCanonical,
    /// The protocol was not lower-case ASCII with the allowed punctuation.
    #[error("Skill authorization protocol is invalid")]
    InvalidProtocol,
}

fn validate_label(
    value: &str,
    maximum: usize,
    protocol: bool,
) -> Result<(), SkillAuthorizationSubjectError> {
    if value.is_empty() {
        return Err(SkillAuthorizationSubjectError::Empty);
    }
    if value.len() > maximum {
        return Err(SkillAuthorizationSubjectError::TooLong);
    }
    if value.trim() != value || value.chars().any(char::is_control) {
        return Err(SkillAuthorizationSubjectError::NonCanonical);
    }
    if protocol
        && !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
        })
    {
        return Err(SkillAuthorizationSubjectError::InvalidProtocol);
    }
    Ok(())
}

/// Why the exact Skill received fresh activation approval.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
#[non_exhaustive]
pub enum SkillActivationSource {
    /// A user or trusted application selected the Skill directly.
    Direct,
    /// A live parent acting window requested a manifest-listed nested Skill.
    Nested {
        /// Exact parent authority window.
        parent_window_id: SkillActingWindowId,
    },
}

impl<'de> Deserialize<'de> for SkillActivationSource {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Kind {
            Direct,
            Nested,
        }

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            kind: Kind,
            #[serde(default)]
            parent_window_id: Option<SkillActingWindowId>,
        }

        let wire = Wire::deserialize(deserializer)?;
        match (wire.kind, wire.parent_window_id) {
            (Kind::Direct, None) => Ok(Self::Direct),
            (Kind::Nested, Some(parent_window_id)) => Ok(Self::Nested { parent_window_id }),
            (Kind::Direct, Some(_)) => Err(de::Error::custom(
                "direct Skill activation source cannot contain a parent window",
            )),
            (Kind::Nested, None) => Err(de::Error::missing_field("parent_window_id")),
        }
    }
}

/// Strict requested lifetime for an acting window.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SkillActingWindowDuration(DurationMillis);

impl SkillActingWindowDuration {
    /// Minimum acting window: one second.
    pub const MIN_MILLIS: i64 = 1_000;
    /// Maximum acting window: twenty-four hours.
    pub const MAX_MILLIS: i64 = 86_400_000;

    /// Validates a finite production acting-window lifetime.
    pub const fn new(value: DurationMillis) -> Result<Self, SkillActingWindowError> {
        if value.as_i64() < Self::MIN_MILLIS || value.as_i64() > Self::MAX_MILLIS {
            return Err(SkillActingWindowError::InvalidDuration);
        }
        Ok(Self(value))
    }

    /// Returns the validated duration.
    #[must_use]
    pub const fn duration(self) -> DurationMillis {
        self.0
    }
}

impl<'de> Deserialize<'de> for SkillActingWindowDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let duration = DurationMillis::deserialize(deserializer)?;
        Self::new(duration).map_err(de::Error::custom)
    }
}

impl JsonSchema for SkillActingWindowDuration {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SkillActingWindowDuration".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::SkillActingWindowDuration").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "minLength": 4,
            "maxLength": 8,
            "pattern": ACTING_WINDOW_DURATION_PATTERN
        })
    }

    fn inline_schema() -> bool {
        true
    }
}

/// Immutable policy approval for one exact Skill activation.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SkillActivationApproval {
    approval_id: SkillActivationApprovalId,
    scope: SkillActivationScope,
    subject: SkillAuthorizationSubject,
    source: SkillActivationSource,
    policy: CapabilityIdentity,
    policy_digest: Digest,
    decision_digest: Digest,
    requested_duration: SkillActingWindowDuration,
    approval_digest: Digest,
}

impl SkillActivationApproval {
    /// Constructs integrity-bound, payload-redacted activation evidence.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        approval_id: SkillActivationApprovalId,
        scope: SkillActivationScope,
        subject: SkillAuthorizationSubject,
        source: SkillActivationSource,
        policy: CapabilityIdentity,
        policy_digest: Digest,
        decision_digest: Digest,
        requested_duration: SkillActingWindowDuration,
    ) -> Result<Self, SkillActingWindowError> {
        let mut value = Self {
            approval_id,
            scope,
            subject,
            source,
            policy,
            policy_digest,
            decision_digest,
            requested_duration,
            approval_digest: Digest::sha256(b""),
        };
        value.approval_digest = value.compute_digest()?;
        Ok(value)
    }

    /// Returns the idempotent approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> SkillActivationApprovalId {
        self.approval_id
    }

    /// Returns the exact durable scope.
    #[must_use]
    pub const fn scope(&self) -> &SkillActivationScope {
        &self.scope
    }

    /// Returns the content-bound Skill subject.
    #[must_use]
    pub const fn subject(&self) -> &SkillAuthorizationSubject {
        &self.subject
    }

    /// Returns the direct or nested source.
    #[must_use]
    pub const fn source(&self) -> &SkillActivationSource {
        &self.source
    }

    /// Returns the version-pinned approval policy.
    #[must_use]
    pub const fn policy(&self) -> &CapabilityIdentity {
        &self.policy
    }

    /// Returns the immutable policy artifact digest.
    #[must_use]
    pub const fn policy_digest(&self) -> Digest {
        self.policy_digest
    }

    /// Returns the private decision-evidence binding.
    #[must_use]
    pub const fn decision_digest(&self) -> Digest {
        self.decision_digest
    }

    /// Returns the requested bounded lifetime.
    #[must_use]
    pub const fn requested_duration(&self) -> SkillActingWindowDuration {
        self.requested_duration
    }

    /// Returns the digest covering every approval field.
    #[must_use]
    pub const fn approval_digest(&self) -> Digest {
        self.approval_digest
    }

    fn compute_digest(&self) -> Result<Digest, SkillActingWindowError> {
        #[derive(Serialize)]
        struct Preimage<'a> {
            approval_id: SkillActivationApprovalId,
            scope: &'a SkillActivationScope,
            subject: &'a SkillAuthorizationSubject,
            source: &'a SkillActivationSource,
            policy: &'a CapabilityIdentity,
            policy_digest: Digest,
            decision_digest: Digest,
            requested_duration: SkillActingWindowDuration,
        }
        digest_canonical(
            APPROVAL_DIGEST_DOMAIN,
            &Preimage {
                approval_id: self.approval_id,
                scope: &self.scope,
                subject: &self.subject,
                source: &self.source,
                policy: &self.policy,
                policy_digest: self.policy_digest,
                decision_digest: self.decision_digest,
                requested_duration: self.requested_duration,
            },
        )
    }
}

impl fmt::Debug for SkillActivationApproval {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SkillActivationApproval")
            .field("approval_id", &self.approval_id)
            .field("scope", &self.scope)
            .field("subject", &self.subject)
            .field("source", &self.source)
            .field("policy", &self.policy)
            .field("policy_digest", &self.policy_digest)
            .field("decision_digest", &self.decision_digest)
            .field("requested_duration", &self.requested_duration)
            .field("approval_digest", &self.approval_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for SkillActivationApproval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            approval_id: SkillActivationApprovalId,
            scope: SkillActivationScope,
            subject: SkillAuthorizationSubject,
            source: SkillActivationSource,
            policy: CapabilityIdentity,
            policy_digest: Digest,
            decision_digest: Digest,
            requested_duration: SkillActingWindowDuration,
            approval_digest: Digest,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self::new(
            wire.approval_id,
            wire.scope,
            wire.subject,
            wire.source,
            wire.policy,
            wire.policy_digest,
            wire.decision_digest,
            wire.requested_duration,
        )
        .map_err(de::Error::custom)?;
        if value.approval_digest != wire.approval_digest {
            return Err(de::Error::custom(SkillActingWindowError::DigestMismatch));
        }
        Ok(value)
    }
}

/// Idempotent request to open an approved acting window.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SkillActingWindowOpenRequest {
    window_id: SkillActingWindowId,
    approval: SkillActivationApproval,
}

impl SkillActingWindowOpenRequest {
    /// Constructs an exact open request whose IDs may be safely retried.
    #[must_use]
    pub const fn new(window_id: SkillActingWindowId, approval: SkillActivationApproval) -> Self {
        Self {
            window_id,
            approval,
        }
    }

    /// Returns the acting-window identity.
    #[must_use]
    pub const fn window_id(&self) -> SkillActingWindowId {
        self.window_id
    }

    /// Returns the immutable approval evidence.
    #[must_use]
    pub const fn approval(&self) -> &SkillActivationApproval {
        &self.approval
    }
}

/// Durable database-clock Skill authority window.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SkillActingWindow {
    window_id: SkillActingWindowId,
    approval: SkillActivationApproval,
    opened_at: Timestamp,
    expires_at: Timestamp,
    window_digest: Digest,
}

impl SkillActingWindow {
    /// Constructs a store-authoritative acting window.
    pub fn new(
        window_id: SkillActingWindowId,
        approval: SkillActivationApproval,
        opened_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, SkillActingWindowError> {
        let requested_micros = approval
            .requested_duration()
            .duration()
            .as_i64()
            .checked_mul(1_000)
            .ok_or(SkillActingWindowError::InvalidClock)?;
        let expected = opened_at
            .unix_micros()
            .checked_add(requested_micros)
            .ok_or(SkillActingWindowError::InvalidClock)?;
        if expires_at.unix_micros() != expected {
            return Err(SkillActingWindowError::InvalidClock);
        }
        let mut value = Self {
            window_id,
            approval,
            opened_at,
            expires_at,
            window_digest: Digest::sha256(b""),
        };
        value.window_digest = value.compute_digest()?;
        Ok(value)
    }

    /// Returns the durable acting-window identity.
    #[must_use]
    pub const fn window_id(&self) -> SkillActingWindowId {
        self.window_id
    }

    /// Returns the immutable activation approval.
    #[must_use]
    pub const fn approval(&self) -> &SkillActivationApproval {
        &self.approval
    }

    /// Returns the database-clock commit instant.
    #[must_use]
    pub const fn opened_at(&self) -> Timestamp {
        self.opened_at
    }

    /// Returns the exclusive database-clock expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the integrity digest covering the window.
    #[must_use]
    pub const fn window_digest(&self) -> Digest {
        self.window_digest
    }

    fn compute_digest(&self) -> Result<Digest, SkillActingWindowError> {
        #[derive(Serialize)]
        struct Preimage<'a> {
            window_id: SkillActingWindowId,
            approval: &'a SkillActivationApproval,
            opened_at: Timestamp,
            expires_at: Timestamp,
        }
        digest_canonical(
            WINDOW_DIGEST_DOMAIN,
            &Preimage {
                window_id: self.window_id,
                approval: &self.approval,
                opened_at: self.opened_at,
                expires_at: self.expires_at,
            },
        )
    }
}

impl fmt::Debug for SkillActingWindow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SkillActingWindow")
            .field("window_id", &self.window_id)
            .field("approval", &self.approval)
            .field("opened_at", &self.opened_at)
            .field("expires_at", &self.expires_at)
            .field("window_digest", &self.window_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for SkillActingWindow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            window_id: SkillActingWindowId,
            approval: SkillActivationApproval,
            opened_at: Timestamp,
            expires_at: Timestamp,
            window_digest: Digest,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self::new(
            wire.window_id,
            wire.approval,
            wire.opened_at,
            wire.expires_at,
        )
        .map_err(de::Error::custom)?;
        if value.window_digest != wire.window_digest {
            return Err(de::Error::custom(SkillActingWindowError::DigestMismatch));
        }
        Ok(value)
    }
}

/// Closed reason for permanently revoking an acting window.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SkillActingWindowRevocationReason {
    /// The approving user withdrew authority.
    User,
    /// The governing policy withdrew authority.
    Policy,
    /// The Skill or authority boundary may be compromised.
    Compromised,
    /// A successor activation replaced this window.
    Superseded,
    /// A trusted administrator withdrew authority.
    Administrative,
}

/// Immutable first revocation event for an acting window.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SkillActingWindowRevocation {
    tenant_id: TenantId,
    window_id: SkillActingWindowId,
    reason: SkillActingWindowRevocationReason,
    revoked_at: Timestamp,
    revocation_digest: Digest,
}

impl SkillActingWindowRevocation {
    /// Constructs database-clock revocation evidence.
    pub fn new(
        tenant_id: TenantId,
        window_id: SkillActingWindowId,
        reason: SkillActingWindowRevocationReason,
        revoked_at: Timestamp,
    ) -> Result<Self, SkillActingWindowError> {
        let mut value = Self {
            tenant_id,
            window_id,
            reason,
            revoked_at,
            revocation_digest: Digest::sha256(b""),
        };
        value.revocation_digest = digest_canonical(
            REVOCATION_DIGEST_DOMAIN,
            &(
                &value.tenant_id,
                value.window_id,
                value.reason,
                value.revoked_at,
            ),
        )?;
        Ok(value)
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the revoked window.
    #[must_use]
    pub const fn window_id(&self) -> SkillActingWindowId {
        self.window_id
    }

    /// Returns the closed revocation reason.
    #[must_use]
    pub const fn reason(&self) -> SkillActingWindowRevocationReason {
        self.reason
    }

    /// Returns the database-clock revocation instant.
    #[must_use]
    pub const fn revoked_at(&self) -> Timestamp {
        self.revoked_at
    }

    /// Returns the event digest.
    #[must_use]
    pub const fn revocation_digest(&self) -> Digest {
        self.revocation_digest
    }
}

impl<'de> Deserialize<'de> for SkillActingWindowRevocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            window_id: SkillActingWindowId,
            reason: SkillActingWindowRevocationReason,
            revoked_at: Timestamp,
            revocation_digest: Digest,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self::new(wire.tenant_id, wire.window_id, wire.reason, wire.revoked_at)
            .map_err(de::Error::custom)?;
        if value.revocation_digest != wire.revocation_digest {
            return Err(de::Error::custom(SkillActingWindowError::DigestMismatch));
        }
        Ok(value)
    }
}

/// Invalid or corrupted Skill approval/window evidence.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum SkillActingWindowError {
    /// Requested duration fell outside one second through twenty-four hours.
    #[error("Skill acting-window duration is outside the supported range")]
    InvalidDuration,
    /// Store timestamps did not exactly represent the approved lifetime.
    #[error("Skill acting-window clock evidence is invalid")]
    InvalidClock,
    /// Canonical evidence encoding failed.
    #[error("Skill acting-window canonical encoding failed")]
    Encoding,
    /// Decoded evidence did not reproduce its digest.
    #[error("Skill acting-window digest does not match its fields")]
    DigestMismatch,
}

fn digest_canonical<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, SkillActingWindowError> {
    let canonical =
        serde_json_canonicalizer::to_vec(value).map_err(|_| SkillActingWindowError::Encoding)?;
    let mut bytes = Vec::with_capacity(domain.len() + 8 + canonical.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(
        &u64::try_from(canonical.len())
            .expect("Skill authorization evidence length fits u64")
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&canonical);
    Ok(Digest::sha256(bytes))
}

/// Stable public class for durable Skill activation storage failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SkillActivationStoreFailure {
    /// Durable storage cannot serve the request now.
    Unavailable,
    /// The requested durable identity does not exist.
    NotFound,
    /// The window expired or has been revoked.
    Inactive,
    /// Durable evidence conflicted with or failed validation.
    Rejected,
}

/// Payload-redacted durable Skill activation store failure.
pub struct SkillActivationStoreError {
    failure: SkillActivationStoreFailure,
    source: Arc<dyn StdError + Send + Sync + 'static>,
}

impl SkillActivationStoreError {
    /// Wraps a private provider error with a stable public class.
    #[must_use]
    pub fn new<E>(failure: SkillActivationStoreFailure, source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            failure,
            source: Arc::new(source),
        }
    }

    /// Returns the stable failure class.
    #[must_use]
    pub const fn failure(&self) -> SkillActivationStoreFailure {
        self.failure
    }

    /// Returns the private trusted diagnostic.
    #[must_use]
    pub fn private_source(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self.source.as_ref()
    }
}

impl fmt::Debug for SkillActivationStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SkillActivationStoreError")
            .field("failure", &self.failure)
            .field("has_private_source", &true)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for SkillActivationStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Skill activation store failed")
    }
}

impl StdError for SkillActivationStoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Mandatory durable approval and acting-window authority.
pub trait SkillActivationStore: Send + Sync + 'static {
    /// Atomically commits one approval and database-clock acting window.
    fn open(
        &self,
        request: SkillActingWindowOpenRequest,
    ) -> BoxFuture<'_, Result<SkillActingWindow, SkillActivationStoreError>>;

    /// Loads one exact currently active window for process restart recovery.
    fn load_active(
        &self,
        tenant_id: TenantId,
        window_id: SkillActingWindowId,
    ) -> BoxFuture<'_, Result<SkillActingWindow, SkillActivationStoreError>>;

    /// Revalidates exact durable evidence immediately before sensitive work.
    fn assert_active(
        &self,
        window: SkillActingWindow,
    ) -> BoxFuture<'_, Result<(), SkillActivationStoreError>>;

    /// Commits the first immutable revocation event and returns it idempotently.
    fn revoke(
        &self,
        window: SkillActingWindow,
        reason: SkillActingWindowRevocationReason,
    ) -> BoxFuture<'_, Result<SkillActingWindowRevocation, SkillActivationStoreError>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CapabilityName, CapabilityReference, IssuerId, PrincipalIdentity, SubjectId, Version,
    };

    fn policy() -> CapabilityIdentity {
        CapabilityIdentity::new(
            PrincipalIdentity::new(
                "https://issuer.example.com".parse::<IssuerId>().unwrap(),
                "policy".parse::<SubjectId>().unwrap(),
            ),
            CapabilityReference::new(
                "skill-activation".parse::<CapabilityName>().unwrap(),
                Version::new(1, 0, 0),
            ),
        )
    }

    fn approval() -> SkillActivationApproval {
        SkillActivationApproval::new(
            "018f1f65-45d3-7a2e-8a19-4de38b783450".parse().unwrap(),
            SkillActivationScope::new(
                "tenant-a".parse().unwrap(),
                "018f1f65-45d3-7a2e-8a19-4de38b783451".parse().unwrap(),
                "018f1f65-45d3-7a2e-8a19-4de38b783452".parse().unwrap(),
            ),
            SkillAuthorizationSubject::new(
                "mcp",
                "production",
                "skill://review/SKILL.md",
                Digest::sha256(b"manifest"),
            )
            .unwrap(),
            SkillActivationSource::Direct,
            policy(),
            Digest::sha256(b"policy"),
            Digest::sha256(b"decision"),
            SkillActingWindowDuration::new(DurationMillis::new(60_000).unwrap()).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn approval_window_and_revocation_reject_tampering() {
        let approval = approval();
        let opened: Timestamp = "2026-09-20T12:00:00.000000Z".parse().unwrap();
        let expires: Timestamp = "2026-09-20T12:01:00.000000Z".parse().unwrap();
        let window = SkillActingWindow::new(
            "018f1f65-45d3-7a2e-8a19-4de38b783453".parse().unwrap(),
            approval,
            opened,
            expires,
        )
        .unwrap();
        let encoded = serde_json::to_value(&window).unwrap();
        assert_eq!(
            serde_json::from_value::<SkillActingWindow>(encoded.clone()).unwrap(),
            window
        );
        let mut changed = encoded;
        changed["expires_at"] = serde_json::json!("2026-09-20T12:02:00.000000Z");
        assert!(serde_json::from_value::<SkillActingWindow>(changed).is_err());

        let revocation = SkillActingWindowRevocation::new(
            window.approval().scope().tenant_id().clone(),
            window.window_id(),
            SkillActingWindowRevocationReason::Policy,
            expires,
        )
        .unwrap();
        assert_eq!(
            serde_json::from_value::<SkillActingWindowRevocation>(
                serde_json::to_value(&revocation).unwrap()
            )
            .unwrap(),
            revocation
        );
    }

    #[test]
    fn subject_and_duration_are_strictly_bounded() {
        assert!(
            SkillAuthorizationSubject::new(
                "MCP",
                "origin",
                "skill://a/SKILL.md",
                Digest::sha256(b"x")
            )
            .is_err()
        );
        assert!(SkillActingWindowDuration::new(DurationMillis::new(999).unwrap()).is_err());
        assert!(SkillActingWindowDuration::new(DurationMillis::new(86_400_001).unwrap()).is_err());
    }
}
