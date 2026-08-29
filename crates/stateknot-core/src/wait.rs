// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Integrity-bound durable interrupt and timer records.
//!
//! [`RunInterrupt`](crate::RunInterrupt) and [`RunTimer`](crate::RunTimer) are
//! deliberately compact lifecycle markers. This module carries the separately
//! persisted request payload, authorization evidence, exact journal anchors,
//! and terminal resolution/firing facts needed to recover those markers after
//! a process crash. Stores commit a registration beside the journal event and
//! checkpoint that enter `waiting`, and commit a resolution or firing beside
//! the exact lifecycle transition that removes one marker.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    Digest, EventId, InterruptId, JournalHead, JournalPayload, PrincipalIdentity, RunId,
    RunInterrupt, RunInterruptKind, RunTimer, RunTimerKind, ScopeSet, TenantId, TimerId, Timestamp,
};

const INTERRUPT_INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-interrupt-intent-v1\0";
const INTERRUPT_REQUEST_DIGEST_DOMAIN: &[u8] = b"stateknot-interrupt-request-v1\0";
const INTERRUPT_RESOLUTION_INTENT_DIGEST_DOMAIN: &[u8] =
    b"stateknot-interrupt-resolution-intent-v1\0";
const INTERRUPT_RESOLUTION_DIGEST_DOMAIN: &[u8] = b"stateknot-interrupt-resolution-v1\0";
const TIMER_INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-timer-intent-v1\0";
const TIMER_DIGEST_DOMAIN: &[u8] = b"stateknot-timer-v1\0";
const TIMER_FIRING_INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-timer-firing-intent-v1\0";
const TIMER_FIRING_DIGEST_DOMAIN: &[u8] = b"stateknot-timer-firing-v1\0";

/// Pre-commit request to register one externally resolvable interrupt.
///
/// The request and action digests are distinct: `request_payload` describes
/// what the resolver sees or supplies, while `action_digest` binds the exact
/// immutable operation whose approval/input/authentication is being requested.
/// Reusing `interrupt_id` or `request_event_id` is idempotent only when the
/// complete intent digest matches.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptRequestIntent {
    tenant_id: TenantId,
    run_id: RunId,
    interrupt_id: InterruptId,
    request_event_id: EventId,
    kind: RunInterruptKind,
    request_payload: JournalPayload,
    action_digest: Digest,
    #[serde(skip_serializing_if = "Option::is_none")]
    required_principal: Option<PrincipalIdentity>,
    required_scopes: ScopeSet,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<Timestamp>,
    intent_digest: Digest,
}

impl InterruptRequestIntent {
    /// Constructs a complete immutable interrupt-registration intent.
    ///
    /// The exclusive expiry is checked against the authoritative database
    /// observation when [`InterruptRequest::commit`] materializes the request.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::CanonicalSerialization`] if the integrity
    /// preimage cannot be encoded canonically.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        interrupt_id: InterruptId,
        request_event_id: EventId,
        kind: RunInterruptKind,
        request_payload: JournalPayload,
        action_digest: Digest,
        required_principal: Option<PrincipalIdentity>,
        required_scopes: ScopeSet,
        expires_at: Option<Timestamp>,
    ) -> Result<Self, DurableWaitError> {
        let intent_digest = compute_digest(
            INTERRUPT_INTENT_DIGEST_DOMAIN,
            &InterruptIntentDigestWire {
                tenant_id: &tenant_id,
                run_id,
                interrupt_id,
                request_event_id,
                kind,
                request_payload_digest: request_payload.digest(),
                action_digest,
                required_principal: required_principal.as_ref(),
                required_scopes: &required_scopes,
                expires_at,
            },
        )?;
        Ok(Self {
            tenant_id,
            run_id,
            interrupt_id,
            request_event_id,
            kind,
            request_payload,
            action_digest,
            required_principal,
            required_scopes,
            expires_at,
            intent_digest,
        })
    }

    /// Restores and verifies a persisted registration intent.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::IntentDigestMismatch`] after any mutation.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        tenant_id: TenantId,
        run_id: RunId,
        interrupt_id: InterruptId,
        request_event_id: EventId,
        kind: RunInterruptKind,
        request_payload: JournalPayload,
        action_digest: Digest,
        required_principal: Option<PrincipalIdentity>,
        required_scopes: ScopeSet,
        expires_at: Option<Timestamp>,
        intent_digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        let restored = Self::new(
            tenant_id,
            run_id,
            interrupt_id,
            request_event_id,
            kind,
            request_payload,
            action_digest,
            required_principal,
            required_scopes,
            expires_at,
        )?;
        if restored.intent_digest != intent_digest {
            return Err(DurableWaitError::IntentDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the owning run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the stable interrupt identity.
    #[must_use]
    pub const fn interrupt_id(&self) -> InterruptId {
        self.interrupt_id
    }

    /// Returns the exact event that must register this request.
    #[must_use]
    pub const fn request_event_id(&self) -> EventId {
        self.request_event_id
    }

    /// Returns the protocol-neutral interrupt kind.
    #[must_use]
    pub const fn kind(&self) -> RunInterruptKind {
        self.kind
    }

    /// Returns the schema-pinned request payload.
    #[must_use]
    pub const fn request_payload(&self) -> &JournalPayload {
        &self.request_payload
    }

    /// Returns the immutable action checksum protected by the interrupt.
    #[must_use]
    pub const fn action_digest(&self) -> Digest {
        self.action_digest
    }

    /// Returns the exact required resolver, when one was selected.
    #[must_use]
    pub const fn required_principal(&self) -> Option<&PrincipalIdentity> {
        self.required_principal.as_ref()
    }

    /// Returns every scope a resolver must possess.
    #[must_use]
    pub const fn required_scopes(&self) -> &ScopeSet {
        &self.required_scopes
    }

    /// Returns the exclusive resolution expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }

    /// Returns the complete semantic idempotency checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }
}

impl fmt::Debug for InterruptRequestIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InterruptRequestIntent")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("interrupt_id", &self.interrupt_id)
            .field("request_event_id", &self.request_event_id)
            .field("kind", &self.kind)
            .field("request_payload", &self.request_payload)
            .field("action_digest", &self.action_digest)
            .field("required_principal", &self.required_principal)
            .field("required_scopes", &self.required_scopes)
            .field("expires_at", &self.expires_at)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for InterruptRequestIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            interrupt_id: InterruptId,
            request_event_id: EventId,
            kind: RunInterruptKind,
            request_payload: JournalPayload,
            action_digest: Digest,
            required_principal: Option<PrincipalIdentity>,
            required_scopes: ScopeSet,
            expires_at: Option<Timestamp>,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.tenant_id,
            wire.run_id,
            wire.interrupt_id,
            wire.request_event_id,
            wire.kind,
            wire.request_payload,
            wire.action_digest,
            wire.required_principal,
            wire.required_scopes,
            wire.expires_at,
            wire.intent_digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Immutable interrupt request committed with its registration journal fact.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptRequest {
    intent: InterruptRequestIntent,
    marker: RunInterrupt,
    journal: JournalHead,
    digest: Digest,
}

impl InterruptRequest {
    /// Materializes a request at the authoritative journal observation.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError`] if scope, event identity, expiry, or
    /// canonical integrity is invalid.
    pub fn commit(
        intent: InterruptRequestIntent,
        journal: JournalHead,
    ) -> Result<Self, DurableWaitError> {
        validate_journal_scope(
            intent.tenant_id(),
            intent.run_id(),
            intent.request_event_id(),
            &journal,
        )?;
        let marker = RunInterrupt::new(
            intent.interrupt_id(),
            intent.kind(),
            journal.recorded_at(),
            intent.expires_at(),
        )
        .map_err(|_| DurableWaitError::InterruptExpiryNotAfterRegistration)?;
        let digest = compute_interrupt_request_digest(&intent, &marker, &journal)?;
        Ok(Self {
            intent,
            marker,
            journal,
            digest,
        })
    }

    /// Restores and verifies a persisted request.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::RecordDigestMismatch`] after mutation.
    pub fn restore(
        intent: InterruptRequestIntent,
        marker: &RunInterrupt,
        journal: JournalHead,
        digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        let restored = Self::commit(intent, journal)?;
        if &restored.marker != marker {
            return Err(DurableWaitError::LifecycleMarkerMismatch);
        }
        if restored.digest != digest {
            return Err(DurableWaitError::RecordDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the immutable pre-commit intent.
    #[must_use]
    pub const fn intent(&self) -> &InterruptRequestIntent {
        &self.intent
    }

    /// Returns the compact lifecycle marker.
    #[must_use]
    pub const fn marker(&self) -> &RunInterrupt {
        &self.marker
    }

    /// Returns the exact registration journal head.
    #[must_use]
    pub const fn journal(&self) -> &JournalHead {
        &self.journal
    }

    /// Returns the complete request checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns a compact integrity-verifiable resolution anchor.
    #[must_use]
    pub fn head(&self) -> InterruptRequestHead {
        InterruptRequestHead {
            tenant_id: self.intent.tenant_id.clone(),
            run_id: self.intent.run_id,
            marker: self.marker.clone(),
            request_payload_digest: self.intent.request_payload.digest(),
            action_digest: self.intent.action_digest,
            required_principal: self.intent.required_principal.clone(),
            required_scopes: self.intent.required_scopes.clone(),
            intent_digest: self.intent.intent_digest,
            journal: self.journal.clone(),
            digest: self.digest,
        }
    }
}

impl fmt::Debug for InterruptRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InterruptRequest")
            .field("intent", &self.intent)
            .field("marker", &self.marker)
            .field("journal", &self.journal)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for InterruptRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: InterruptRequestIntent,
            marker: RunInterrupt,
            journal: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.intent, &wire.marker, wire.journal, wire.digest)
            .map_err(de::Error::custom)
    }
}

/// Compact immutable interrupt anchor carried by a resolution.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptRequestHead {
    tenant_id: TenantId,
    run_id: RunId,
    marker: RunInterrupt,
    request_payload_digest: Digest,
    action_digest: Digest,
    #[serde(skip_serializing_if = "Option::is_none")]
    required_principal: Option<PrincipalIdentity>,
    required_scopes: ScopeSet,
    intent_digest: Digest,
    journal: JournalHead,
    digest: Digest,
}

impl InterruptRequestHead {
    /// Restores and verifies a compact request anchor.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError`] for scope, clock, or digest disagreement.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        tenant_id: TenantId,
        run_id: RunId,
        marker: RunInterrupt,
        request_payload_digest: Digest,
        action_digest: Digest,
        required_principal: Option<PrincipalIdentity>,
        required_scopes: ScopeSet,
        intent_digest: Digest,
        journal: JournalHead,
        digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        if journal.tenant_id() != &tenant_id {
            return Err(DurableWaitError::JournalTenantMismatch);
        }
        if journal.run_id() != run_id {
            return Err(DurableWaitError::JournalRunMismatch);
        }
        if journal.recorded_at() != marker.requested_at() {
            return Err(DurableWaitError::LifecycleMarkerMismatch);
        }
        if let Some(expires_at) = marker.expires_at() {
            if expires_at <= marker.requested_at() {
                return Err(DurableWaitError::InterruptExpiryNotAfterRegistration);
            }
        }
        let expected_intent = compute_digest(
            INTERRUPT_INTENT_DIGEST_DOMAIN,
            &InterruptIntentDigestWire {
                tenant_id: &tenant_id,
                run_id,
                interrupt_id: marker.interrupt_id(),
                request_event_id: journal.event_id(),
                kind: marker.kind(),
                request_payload_digest,
                action_digest,
                required_principal: required_principal.as_ref(),
                required_scopes: &required_scopes,
                expires_at: marker.expires_at(),
            },
        )?;
        if expected_intent != intent_digest {
            return Err(DurableWaitError::IntentDigestMismatch);
        }
        let expected = compute_digest(
            INTERRUPT_REQUEST_DIGEST_DOMAIN,
            &InterruptRequestDigestWire {
                intent_digest,
                marker: &marker,
                journal: &journal,
            },
        )?;
        if expected != digest {
            return Err(DurableWaitError::RecordDigestMismatch);
        }
        Ok(Self {
            tenant_id,
            run_id,
            marker,
            request_payload_digest,
            action_digest,
            required_principal,
            required_scopes,
            intent_digest,
            journal,
            digest,
        })
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the owning run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the compact lifecycle marker.
    #[must_use]
    pub const fn marker(&self) -> &RunInterrupt {
        &self.marker
    }

    /// Returns the stable interrupt identity.
    #[must_use]
    pub const fn interrupt_id(&self) -> InterruptId {
        self.marker.interrupt_id()
    }

    /// Returns the request payload checksum.
    #[must_use]
    pub const fn request_payload_digest(&self) -> Digest {
        self.request_payload_digest
    }

    /// Returns the protected action checksum.
    #[must_use]
    pub const fn action_digest(&self) -> Digest {
        self.action_digest
    }

    /// Returns the exact required resolver, if configured.
    #[must_use]
    pub const fn required_principal(&self) -> Option<&PrincipalIdentity> {
        self.required_principal.as_ref()
    }

    /// Returns every required scope.
    #[must_use]
    pub const fn required_scopes(&self) -> &ScopeSet {
        &self.required_scopes
    }

    /// Returns the complete pre-commit intent checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the exact registration journal head.
    #[must_use]
    pub const fn journal(&self) -> &JournalHead {
        &self.journal
    }

    /// Returns the complete request checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl fmt::Debug for InterruptRequestHead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InterruptRequestHead")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("marker", &self.marker)
            .field("request_payload_digest", &self.request_payload_digest)
            .field("action_digest", &self.action_digest)
            .field("required_principal", &self.required_principal)
            .field("required_scopes", &self.required_scopes)
            .field("intent_digest", &self.intent_digest)
            .field("journal", &self.journal)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for InterruptRequestHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            marker: RunInterrupt,
            request_payload_digest: Digest,
            action_digest: Digest,
            required_principal: Option<PrincipalIdentity>,
            required_scopes: ScopeSet,
            intent_digest: Digest,
            journal: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.tenant_id,
            wire.run_id,
            wire.marker,
            wire.request_payload_digest,
            wire.action_digest,
            wire.required_principal,
            wire.required_scopes,
            wire.intent_digest,
            wire.journal,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Authenticated identity and granted authorization scopes of one resolver.
///
/// A durable resolver always has an authenticated principal. The scope set is
/// the bounded authority snapshot evaluated for this resolution, not a bearer
/// token and never a credential.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptResolver {
    principal: PrincipalIdentity,
    granted_scopes: ScopeSet,
}

impl InterruptResolver {
    /// Constructs resolver provenance from validated identity and scopes.
    #[must_use]
    pub const fn new(principal: PrincipalIdentity, granted_scopes: ScopeSet) -> Self {
        Self {
            principal,
            granted_scopes,
        }
    }

    /// Returns the authenticated resolver identity.
    #[must_use]
    pub const fn principal(&self) -> &PrincipalIdentity {
        &self.principal
    }

    /// Returns the authority snapshot presented for resolution.
    #[must_use]
    pub const fn granted_scopes(&self) -> &ScopeSet {
        &self.granted_scopes
    }
}

/// Pre-commit authenticated resolution of one exact interrupt request.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptResolutionIntent {
    request: InterruptRequestHead,
    resolution_event_id: EventId,
    resolution_payload: JournalPayload,
    resolver: InterruptResolver,
    intent_digest: Digest,
}

impl InterruptResolutionIntent {
    /// Constructs and authorizes a resolution intent.
    ///
    /// Authorization is checked again when the journal observation commits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::ResolverPrincipalMismatch`] or
    /// [`DurableWaitError::ResolverScopesInsufficient`] for insufficient
    /// authority, and fails closed on canonicalization errors.
    pub fn new(
        request: &InterruptRequest,
        resolution_event_id: EventId,
        resolution_payload: JournalPayload,
        resolver: InterruptResolver,
    ) -> Result<Self, DurableWaitError> {
        let request = request.head();
        authorize_resolver(&request, &resolver)?;
        Self::from_head(request, resolution_event_id, resolution_payload, resolver)
    }

    fn from_head(
        request: InterruptRequestHead,
        resolution_event_id: EventId,
        resolution_payload: JournalPayload,
        resolver: InterruptResolver,
    ) -> Result<Self, DurableWaitError> {
        authorize_resolver(&request, &resolver)?;
        let intent_digest = compute_digest(
            INTERRUPT_RESOLUTION_INTENT_DIGEST_DOMAIN,
            &InterruptResolutionIntentDigestWire {
                request: &request,
                resolution_event_id,
                resolution_payload_digest: resolution_payload.digest(),
                resolver: &resolver,
            },
        )?;
        Ok(Self {
            request,
            resolution_event_id,
            resolution_payload,
            resolver,
            intent_digest,
        })
    }

    /// Restores and verifies a persisted resolution intent.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError`] for invalid authorization or integrity.
    pub fn restore(
        request: InterruptRequestHead,
        resolution_event_id: EventId,
        resolution_payload: JournalPayload,
        resolver: InterruptResolver,
        intent_digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        let restored = Self::from_head(request, resolution_event_id, resolution_payload, resolver)?;
        if restored.intent_digest != intent_digest {
            return Err(DurableWaitError::IntentDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the exact interrupt request anchor.
    #[must_use]
    pub const fn request(&self) -> &InterruptRequestHead {
        &self.request
    }

    /// Returns the event that must commit this resolution.
    #[must_use]
    pub const fn resolution_event_id(&self) -> EventId {
        self.resolution_event_id
    }

    /// Returns the schema-pinned resolution payload.
    #[must_use]
    pub const fn resolution_payload(&self) -> &JournalPayload {
        &self.resolution_payload
    }

    /// Returns authenticated resolver provenance.
    #[must_use]
    pub const fn resolver(&self) -> &InterruptResolver {
        &self.resolver
    }

    /// Returns the semantic idempotency checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }
}

impl fmt::Debug for InterruptResolutionIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InterruptResolutionIntent")
            .field("request", &self.request)
            .field("resolution_event_id", &self.resolution_event_id)
            .field("resolution_payload", &self.resolution_payload)
            .field("resolver", &self.resolver)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for InterruptResolutionIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            request: InterruptRequestHead,
            resolution_event_id: EventId,
            resolution_payload: JournalPayload,
            resolver: InterruptResolver,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.request,
            wire.resolution_event_id,
            wire.resolution_payload,
            wire.resolver,
            wire.intent_digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Immutable authorized resolution committed with its journal fact.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptResolution {
    intent: InterruptResolutionIntent,
    journal: JournalHead,
    digest: Digest,
}

impl InterruptResolution {
    /// Commits a resolution at the journal's database-clock observation.
    ///
    /// Resolution at the exact exclusive expiry is rejected.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError`] for substituted request/event/scope,
    /// insufficient authority, clock regression, expiry, or integrity failure.
    pub fn commit(
        intent: InterruptResolutionIntent,
        journal: JournalHead,
    ) -> Result<Self, DurableWaitError> {
        validate_journal_scope(
            intent.request.tenant_id(),
            intent.request.run_id(),
            intent.resolution_event_id(),
            &journal,
        )?;
        validate_terminal_causality(intent.request.journal(), &journal)?;
        authorize_resolver(&intent.request, &intent.resolver)?;
        if journal.recorded_at() < intent.request.marker.requested_at() {
            return Err(DurableWaitError::ResolutionBeforeRegistration);
        }
        if let Some(expires_at) = intent.request.marker.expires_at() {
            if journal.recorded_at() >= expires_at {
                return Err(DurableWaitError::InterruptExpired);
            }
        }
        let digest = compute_digest(
            INTERRUPT_RESOLUTION_DIGEST_DOMAIN,
            &TerminalDigestWire {
                intent_digest: intent.intent_digest,
                journal: &journal,
            },
        )?;
        Ok(Self {
            intent,
            journal,
            digest,
        })
    }

    /// Restores and verifies a persisted resolution.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::RecordDigestMismatch`] after mutation.
    pub fn restore(
        intent: InterruptResolutionIntent,
        journal: JournalHead,
        digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        let restored = Self::commit(intent, journal)?;
        if restored.digest != digest {
            return Err(DurableWaitError::RecordDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the immutable resolution intent.
    #[must_use]
    pub const fn intent(&self) -> &InterruptResolutionIntent {
        &self.intent
    }

    /// Returns the exact resolution journal head.
    #[must_use]
    pub const fn journal(&self) -> &JournalHead {
        &self.journal
    }

    /// Returns the authoritative resolution observation.
    #[must_use]
    pub const fn resolved_at(&self) -> Timestamp {
        self.journal.recorded_at()
    }

    /// Returns the complete resolution checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl fmt::Debug for InterruptResolution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InterruptResolution")
            .field("intent", &self.intent)
            .field("journal", &self.journal)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for InterruptResolution {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: InterruptResolutionIntent,
            journal: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.intent, wire.journal, wire.digest).map_err(de::Error::custom)
    }
}

/// Complete durable interrupt history: one request and at most one resolution.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptRecord {
    request: InterruptRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolution: Option<InterruptResolution>,
}

impl InterruptRecord {
    /// Constructs an unresolved durable interrupt.
    #[must_use]
    pub const fn unresolved(request: InterruptRequest) -> Self {
        Self {
            request,
            resolution: None,
        }
    }

    /// Restores and validates the complete immutable history.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::InterruptRequestMismatch`] if a resolution
    /// belongs to another request.
    pub fn restore(
        request: InterruptRequest,
        resolution: Option<InterruptResolution>,
    ) -> Result<Self, DurableWaitError> {
        if resolution
            .as_ref()
            .is_some_and(|value| value.intent.request != request.head())
        {
            return Err(DurableWaitError::InterruptRequestMismatch);
        }
        Ok(Self {
            request,
            resolution,
        })
    }

    /// Returns the immutable request.
    #[must_use]
    pub const fn request(&self) -> &InterruptRequest {
        &self.request
    }

    /// Returns the committed resolution, if present.
    #[must_use]
    pub const fn resolution(&self) -> Option<&InterruptResolution> {
        self.resolution.as_ref()
    }

    /// Returns whether the interrupt remains outstanding.
    #[must_use]
    pub const fn is_outstanding(&self) -> bool {
        self.resolution.is_none()
    }
}

impl<'de> Deserialize<'de> for InterruptRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            request: InterruptRequest,
            resolution: Option<InterruptResolution>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.request, wire.resolution).map_err(de::Error::custom)
    }
}

/// Pre-commit request to register one durable timer.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimerRegistrationIntent {
    tenant_id: TenantId,
    run_id: RunId,
    timer_id: TimerId,
    registration_event_id: EventId,
    kind: RunTimerKind,
    due_at: Timestamp,
    intent_digest: Digest,
}

impl TimerRegistrationIntent {
    /// Constructs an immutable timer-registration intent.
    ///
    /// The inclusive due time is checked against the authoritative database
    /// observation when [`DurableTimer::commit`] materializes the timer.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::CanonicalSerialization`] if the integrity
    /// preimage cannot be encoded canonically.
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        timer_id: TimerId,
        registration_event_id: EventId,
        kind: RunTimerKind,
        due_at: Timestamp,
    ) -> Result<Self, DurableWaitError> {
        let intent_digest = compute_digest(
            TIMER_INTENT_DIGEST_DOMAIN,
            &TimerIntentDigestWire {
                tenant_id: &tenant_id,
                run_id,
                timer_id,
                registration_event_id,
                kind,
                due_at,
            },
        )?;
        Ok(Self {
            tenant_id,
            run_id,
            timer_id,
            registration_event_id,
            kind,
            due_at,
            intent_digest,
        })
    }

    /// Restores and verifies a persisted timer intent.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::IntentDigestMismatch`] after mutation.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        tenant_id: TenantId,
        run_id: RunId,
        timer_id: TimerId,
        registration_event_id: EventId,
        kind: RunTimerKind,
        due_at: Timestamp,
        intent_digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        let restored = Self::new(
            tenant_id,
            run_id,
            timer_id,
            registration_event_id,
            kind,
            due_at,
        )?;
        if restored.intent_digest != intent_digest {
            return Err(DurableWaitError::IntentDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the owning run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the stable timer identity.
    #[must_use]
    pub const fn timer_id(&self) -> TimerId {
        self.timer_id
    }

    /// Returns the exact event that must register this timer.
    #[must_use]
    pub const fn registration_event_id(&self) -> EventId {
        self.registration_event_id
    }

    /// Returns the protocol-neutral timer purpose.
    #[must_use]
    pub const fn kind(&self) -> RunTimerKind {
        self.kind
    }

    /// Returns the inclusive earliest firing instant.
    #[must_use]
    pub const fn due_at(&self) -> Timestamp {
        self.due_at
    }

    /// Returns the semantic idempotency checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }
}

impl fmt::Debug for TimerRegistrationIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TimerRegistrationIntent")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("timer_id", &self.timer_id)
            .field("registration_event_id", &self.registration_event_id)
            .field("kind", &self.kind)
            .field("due_at", &self.due_at)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for TimerRegistrationIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            timer_id: TimerId,
            registration_event_id: EventId,
            kind: RunTimerKind,
            due_at: Timestamp,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.tenant_id,
            wire.run_id,
            wire.timer_id,
            wire.registration_event_id,
            wire.kind,
            wire.due_at,
            wire.intent_digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Immutable timer committed with its registration journal fact.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableTimer {
    intent: TimerRegistrationIntent,
    marker: RunTimer,
    journal: JournalHead,
    digest: Digest,
}

impl DurableTimer {
    /// Materializes a timer at the authoritative journal observation.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError`] for scope/event substitution, a due time
    /// not strictly after registration, or integrity failure.
    pub fn commit(
        intent: TimerRegistrationIntent,
        journal: JournalHead,
    ) -> Result<Self, DurableWaitError> {
        validate_journal_scope(
            intent.tenant_id(),
            intent.run_id(),
            intent.registration_event_id(),
            &journal,
        )?;
        let marker = RunTimer::new(
            intent.timer_id(),
            intent.kind(),
            journal.recorded_at(),
            intent.due_at(),
        )
        .map_err(|_| DurableWaitError::TimerDueNotAfterRegistration)?;
        let digest = compute_digest(
            TIMER_DIGEST_DOMAIN,
            &TimerDigestWire {
                intent_digest: intent.intent_digest,
                marker: &marker,
                journal: &journal,
            },
        )?;
        Ok(Self {
            intent,
            marker,
            journal,
            digest,
        })
    }

    /// Restores and verifies a persisted timer.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::RecordDigestMismatch`] after mutation.
    pub fn restore(
        intent: TimerRegistrationIntent,
        marker: &RunTimer,
        journal: JournalHead,
        digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        let restored = Self::commit(intent, journal)?;
        if &restored.marker != marker {
            return Err(DurableWaitError::LifecycleMarkerMismatch);
        }
        if restored.digest != digest {
            return Err(DurableWaitError::RecordDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the immutable pre-commit intent.
    #[must_use]
    pub const fn intent(&self) -> &TimerRegistrationIntent {
        &self.intent
    }

    /// Returns the compact lifecycle marker.
    #[must_use]
    pub const fn marker(&self) -> &RunTimer {
        &self.marker
    }

    /// Returns the exact registration journal head.
    #[must_use]
    pub const fn journal(&self) -> &JournalHead {
        &self.journal
    }

    /// Returns the complete timer checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns a compact integrity-verifiable firing anchor.
    #[must_use]
    pub fn head(&self) -> DurableTimerHead {
        DurableTimerHead {
            tenant_id: self.intent.tenant_id.clone(),
            run_id: self.intent.run_id,
            marker: self.marker.clone(),
            intent_digest: self.intent.intent_digest,
            journal: self.journal.clone(),
            digest: self.digest,
        }
    }
}

impl fmt::Debug for DurableTimer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableTimer")
            .field("intent", &self.intent)
            .field("marker", &self.marker)
            .field("journal", &self.journal)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for DurableTimer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: TimerRegistrationIntent,
            marker: RunTimer,
            journal: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.intent, &wire.marker, wire.journal, wire.digest)
            .map_err(de::Error::custom)
    }
}

/// Compact immutable timer anchor carried by a firing intent.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableTimerHead {
    tenant_id: TenantId,
    run_id: RunId,
    marker: RunTimer,
    intent_digest: Digest,
    journal: JournalHead,
    digest: Digest,
}

impl DurableTimerHead {
    /// Restores and verifies a compact timer anchor.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError`] for scope, clock, or digest disagreement.
    pub fn restore(
        tenant_id: TenantId,
        run_id: RunId,
        marker: RunTimer,
        intent_digest: Digest,
        journal: JournalHead,
        digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        if journal.tenant_id() != &tenant_id {
            return Err(DurableWaitError::JournalTenantMismatch);
        }
        if journal.run_id() != run_id {
            return Err(DurableWaitError::JournalRunMismatch);
        }
        if journal.recorded_at() != marker.scheduled_at() {
            return Err(DurableWaitError::LifecycleMarkerMismatch);
        }
        if marker.due_at() <= marker.scheduled_at() {
            return Err(DurableWaitError::TimerDueNotAfterRegistration);
        }
        let expected_intent = compute_digest(
            TIMER_INTENT_DIGEST_DOMAIN,
            &TimerIntentDigestWire {
                tenant_id: &tenant_id,
                run_id,
                timer_id: marker.timer_id(),
                registration_event_id: journal.event_id(),
                kind: marker.kind(),
                due_at: marker.due_at(),
            },
        )?;
        if expected_intent != intent_digest {
            return Err(DurableWaitError::IntentDigestMismatch);
        }
        let expected = compute_digest(
            TIMER_DIGEST_DOMAIN,
            &TimerDigestWire {
                intent_digest,
                marker: &marker,
                journal: &journal,
            },
        )?;
        if expected != digest {
            return Err(DurableWaitError::RecordDigestMismatch);
        }
        Ok(Self {
            tenant_id,
            run_id,
            marker,
            intent_digest,
            journal,
            digest,
        })
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the owning run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the compact lifecycle marker.
    #[must_use]
    pub const fn marker(&self) -> &RunTimer {
        &self.marker
    }

    /// Returns the stable timer identity.
    #[must_use]
    pub const fn timer_id(&self) -> TimerId {
        self.marker.timer_id()
    }

    /// Returns the complete registration intent checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the exact registration journal head.
    #[must_use]
    pub const fn journal(&self) -> &JournalHead {
        &self.journal
    }

    /// Returns the complete timer checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for DurableTimerHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            marker: RunTimer,
            intent_digest: Digest,
            journal: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.tenant_id,
            wire.run_id,
            wire.marker,
            wire.intent_digest,
            wire.journal,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Pre-commit request to fire one exact durable timer.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimerFiringIntent {
    timer: DurableTimerHead,
    firing_event_id: EventId,
    intent_digest: Digest,
}

impl TimerFiringIntent {
    /// Constructs an immutable firing intent.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::CanonicalSerialization`] if its compact
    /// integrity preimage cannot be encoded.
    pub fn new(timer: &DurableTimer, firing_event_id: EventId) -> Result<Self, DurableWaitError> {
        Self::from_head(timer.head(), firing_event_id)
    }

    fn from_head(
        timer: DurableTimerHead,
        firing_event_id: EventId,
    ) -> Result<Self, DurableWaitError> {
        let intent_digest = compute_digest(
            TIMER_FIRING_INTENT_DIGEST_DOMAIN,
            &TimerFiringIntentDigestWire {
                timer: &timer,
                firing_event_id,
            },
        )?;
        Ok(Self {
            timer,
            firing_event_id,
            intent_digest,
        })
    }

    /// Restores and verifies a persisted firing intent.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::IntentDigestMismatch`] after mutation.
    pub fn restore(
        timer: DurableTimerHead,
        firing_event_id: EventId,
        intent_digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        let restored = Self::from_head(timer, firing_event_id)?;
        if restored.intent_digest != intent_digest {
            return Err(DurableWaitError::IntentDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the exact timer anchor.
    #[must_use]
    pub const fn timer(&self) -> &DurableTimerHead {
        &self.timer
    }

    /// Returns the event that must commit this firing.
    #[must_use]
    pub const fn firing_event_id(&self) -> EventId {
        self.firing_event_id
    }

    /// Returns the semantic idempotency checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }
}

impl<'de> Deserialize<'de> for TimerFiringIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            timer: DurableTimerHead,
            firing_event_id: EventId,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.timer, wire.firing_event_id, wire.intent_digest)
            .map_err(de::Error::custom)
    }
}

/// Immutable timer firing committed with its journal fact.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimerFiring {
    intent: TimerFiringIntent,
    journal: JournalHead,
    digest: Digest,
}

impl TimerFiring {
    /// Commits a firing at or after its inclusive database-clock due instant.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError`] for scope/event substitution, an early
    /// observation, or integrity failure.
    pub fn commit(
        intent: TimerFiringIntent,
        journal: JournalHead,
    ) -> Result<Self, DurableWaitError> {
        validate_journal_scope(
            intent.timer.tenant_id(),
            intent.timer.run_id(),
            intent.firing_event_id(),
            &journal,
        )?;
        validate_terminal_causality(intent.timer.journal(), &journal)?;
        if journal.recorded_at() < intent.timer.marker.due_at() {
            return Err(DurableWaitError::TimerNotDue);
        }
        let digest = compute_digest(
            TIMER_FIRING_DIGEST_DOMAIN,
            &TerminalDigestWire {
                intent_digest: intent.intent_digest,
                journal: &journal,
            },
        )?;
        Ok(Self {
            intent,
            journal,
            digest,
        })
    }

    /// Restores and verifies a persisted firing.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::RecordDigestMismatch`] after mutation.
    pub fn restore(
        intent: TimerFiringIntent,
        journal: JournalHead,
        digest: Digest,
    ) -> Result<Self, DurableWaitError> {
        let restored = Self::commit(intent, journal)?;
        if restored.digest != digest {
            return Err(DurableWaitError::RecordDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the immutable firing intent.
    #[must_use]
    pub const fn intent(&self) -> &TimerFiringIntent {
        &self.intent
    }

    /// Returns the exact firing journal head.
    #[must_use]
    pub const fn journal(&self) -> &JournalHead {
        &self.journal
    }

    /// Returns the authoritative firing observation.
    #[must_use]
    pub const fn fired_at(&self) -> Timestamp {
        self.journal.recorded_at()
    }

    /// Returns the complete firing checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for TimerFiring {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: TimerFiringIntent,
            journal: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.intent, wire.journal, wire.digest).map_err(de::Error::custom)
    }
}

/// Complete durable timer history: one registration and at most one firing.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DurableTimerRecord {
    timer: DurableTimer,
    #[serde(skip_serializing_if = "Option::is_none")]
    firing: Option<TimerFiring>,
}

impl DurableTimerRecord {
    /// Constructs an unfired durable timer.
    #[must_use]
    pub const fn pending(timer: DurableTimer) -> Self {
        Self {
            timer,
            firing: None,
        }
    }

    /// Restores and validates the complete immutable history.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError::TimerRegistrationMismatch`] if a firing
    /// belongs to another timer.
    pub fn restore(
        timer: DurableTimer,
        firing: Option<TimerFiring>,
    ) -> Result<Self, DurableWaitError> {
        if firing
            .as_ref()
            .is_some_and(|value| value.intent.timer != timer.head())
        {
            return Err(DurableWaitError::TimerRegistrationMismatch);
        }
        Ok(Self { timer, firing })
    }

    /// Returns the immutable timer registration.
    #[must_use]
    pub const fn timer(&self) -> &DurableTimer {
        &self.timer
    }

    /// Returns the committed firing, if present.
    #[must_use]
    pub const fn firing(&self) -> Option<&TimerFiring> {
        self.firing.as_ref()
    }

    /// Returns whether the timer remains outstanding.
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        self.firing.is_none()
    }
}

impl<'de> Deserialize<'de> for DurableTimerRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            timer: DurableTimer,
            firing: Option<TimerFiring>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.timer, wire.firing).map_err(de::Error::custom)
    }
}

/// One protocol-neutral condition to register in an atomic waiting batch.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WaitRegistrationIntent {
    /// An authenticated external resolution is required.
    Interrupt {
        /// Complete interrupt request intent.
        request: InterruptRequestIntent,
    },
    /// A database-clock due instant must be observed.
    Timer {
        /// Complete timer registration intent.
        timer: TimerRegistrationIntent,
    },
}

impl WaitRegistrationIntent {
    /// Constructs an interrupt wait intent.
    #[must_use]
    pub const fn interrupt(request: InterruptRequestIntent) -> Self {
        Self::Interrupt { request }
    }

    /// Constructs a timer wait intent.
    #[must_use]
    pub const fn timer(timer: TimerRegistrationIntent) -> Self {
        Self::Timer { timer }
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        match self {
            Self::Interrupt { request } => request.tenant_id(),
            Self::Timer { timer } => timer.tenant_id(),
        }
    }

    /// Returns the owning run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        match self {
            Self::Interrupt { request } => request.run_id(),
            Self::Timer { timer } => timer.run_id(),
        }
    }

    /// Returns the journal event that must register this condition.
    #[must_use]
    pub const fn registration_event_id(&self) -> EventId {
        match self {
            Self::Interrupt { request } => request.request_event_id(),
            Self::Timer { timer } => timer.registration_event_id(),
        }
    }

    /// Materializes the condition against its exact committed journal head.
    ///
    /// # Errors
    ///
    /// Returns [`DurableWaitError`] for any scope, timing, or integrity
    /// disagreement.
    pub fn commit(self, journal: JournalHead) -> Result<DurableWait, DurableWaitError> {
        match self {
            Self::Interrupt { request } => {
                InterruptRequest::commit(request, journal).map(|request| DurableWait::Interrupt {
                    request: Box::new(request),
                })
            }
            Self::Timer { timer } => {
                DurableTimer::commit(timer, journal).map(|timer| DurableWait::Timer {
                    timer: Box::new(timer),
                })
            }
        }
    }
}

/// One fully materialized durable wait registration.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DurableWait {
    /// An immutable interrupt request.
    Interrupt {
        /// Complete durable interrupt request.
        request: Box<InterruptRequest>,
    },
    /// An immutable durable timer.
    Timer {
        /// Complete durable timer registration.
        timer: Box<DurableTimer>,
    },
}

impl DurableWait {
    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        match self {
            Self::Interrupt { request } => request.intent().tenant_id(),
            Self::Timer { timer } => timer.intent().tenant_id(),
        }
    }

    /// Returns the owning run.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        match self {
            Self::Interrupt { request } => request.intent().run_id(),
            Self::Timer { timer } => timer.intent().run_id(),
        }
    }

    /// Returns the exact registration journal head.
    #[must_use]
    pub const fn journal(&self) -> &JournalHead {
        match self {
            Self::Interrupt { request } => request.journal(),
            Self::Timer { timer } => timer.journal(),
        }
    }

    /// Returns the compact lifecycle marker.
    #[must_use]
    pub fn marker(&self) -> crate::RunWait {
        match self {
            Self::Interrupt { request } => crate::RunWait::interrupt(request.marker().clone()),
            Self::Timer { timer } => crate::RunWait::timer(timer.marker().clone()),
        }
    }
}

/// Invalid durable wait registration, authorization, timing, or integrity.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum DurableWaitError {
    /// A domain-separated integrity preimage could not be serialized.
    #[error("durable wait canonical serialization failed")]
    CanonicalSerialization,
    /// A persisted semantic intent checksum changed.
    #[error("durable wait intent digest does not match its canonical fields")]
    IntentDigestMismatch,
    /// A persisted immutable record checksum changed.
    #[error("durable wait record digest does not match its canonical fields")]
    RecordDigestMismatch,
    /// A registration or completion journal anchor crossed tenant scope.
    #[error("durable wait journal anchor crossed the tenant boundary")]
    JournalTenantMismatch,
    /// A registration or completion journal anchor belonged to another run.
    #[error("durable wait journal anchor belonged to another run")]
    JournalRunMismatch,
    /// A journal anchor did not use the event named by the intent.
    #[error("durable wait journal event does not match the intent")]
    JournalEventMismatch,
    /// A compact lifecycle marker disagreed with authoritative record fields.
    #[error("durable wait lifecycle marker does not match the durable record")]
    LifecycleMarkerMismatch,
    /// An interrupt expiry was not strictly after registration.
    #[error("interrupt expiry must be later than its registration observation")]
    InterruptExpiryNotAfterRegistration,
    /// A timer due instant was not strictly after registration.
    #[error("timer due instant must be later than its registration observation")]
    TimerDueNotAfterRegistration,
    /// A resolution named a different immutable request.
    #[error("interrupt resolution does not belong to the durable request")]
    InterruptRequestMismatch,
    /// A timer firing named a different immutable registration.
    #[error("timer firing does not belong to the durable timer")]
    TimerRegistrationMismatch,
    /// The authenticated resolver was not the exact required principal.
    #[error("interrupt resolver principal does not satisfy the request")]
    ResolverPrincipalMismatch,
    /// The resolver authority snapshot omitted one or more required scopes.
    #[error("interrupt resolver scopes do not satisfy the request")]
    ResolverScopesInsufficient,
    /// A resolution observation preceded registration.
    #[error("interrupt resolution preceded its registration observation")]
    ResolutionBeforeRegistration,
    /// A resolution arrived at or after the exclusive interrupt expiry.
    #[error("interrupt resolution arrived outside its exclusive validity window")]
    InterruptExpired,
    /// A timer firing preceded its inclusive due instant.
    #[error("timer firing preceded its inclusive due instant")]
    TimerNotDue,
    /// A resolution/firing did not causally follow its registration event.
    #[error("durable wait completion must follow its registration journal event")]
    TerminalJournalNotAfterRegistration,
}

#[derive(Serialize)]
struct InterruptIntentDigestWire<'a> {
    tenant_id: &'a TenantId,
    run_id: RunId,
    interrupt_id: InterruptId,
    request_event_id: EventId,
    kind: RunInterruptKind,
    request_payload_digest: Digest,
    action_digest: Digest,
    required_principal: Option<&'a PrincipalIdentity>,
    required_scopes: &'a ScopeSet,
    expires_at: Option<Timestamp>,
}

#[derive(Serialize)]
struct InterruptRequestDigestWire<'a> {
    intent_digest: Digest,
    marker: &'a RunInterrupt,
    journal: &'a JournalHead,
}

#[derive(Serialize)]
struct InterruptResolutionIntentDigestWire<'a> {
    request: &'a InterruptRequestHead,
    resolution_event_id: EventId,
    resolution_payload_digest: Digest,
    resolver: &'a InterruptResolver,
}

#[derive(Serialize)]
struct TimerIntentDigestWire<'a> {
    tenant_id: &'a TenantId,
    run_id: RunId,
    timer_id: TimerId,
    registration_event_id: EventId,
    kind: RunTimerKind,
    due_at: Timestamp,
}

#[derive(Serialize)]
struct TimerDigestWire<'a> {
    intent_digest: Digest,
    marker: &'a RunTimer,
    journal: &'a JournalHead,
}

#[derive(Serialize)]
struct TimerFiringIntentDigestWire<'a> {
    timer: &'a DurableTimerHead,
    firing_event_id: EventId,
}

#[derive(Serialize)]
struct TerminalDigestWire<'a> {
    intent_digest: Digest,
    journal: &'a JournalHead,
}

fn compute_interrupt_request_digest(
    intent: &InterruptRequestIntent,
    marker: &RunInterrupt,
    journal: &JournalHead,
) -> Result<Digest, DurableWaitError> {
    compute_digest(
        INTERRUPT_REQUEST_DIGEST_DOMAIN,
        &InterruptRequestDigestWire {
            intent_digest: intent.intent_digest,
            marker,
            journal,
        },
    )
}

fn compute_digest<T: Serialize>(domain: &[u8], value: &T) -> Result<Digest, DurableWaitError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| DurableWaitError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn validate_journal_scope(
    tenant_id: &TenantId,
    run_id: RunId,
    event_id: EventId,
    journal: &JournalHead,
) -> Result<(), DurableWaitError> {
    if journal.tenant_id() != tenant_id {
        return Err(DurableWaitError::JournalTenantMismatch);
    }
    if journal.run_id() != run_id {
        return Err(DurableWaitError::JournalRunMismatch);
    }
    if journal.event_id() != event_id {
        return Err(DurableWaitError::JournalEventMismatch);
    }
    Ok(())
}

fn authorize_resolver(
    request: &InterruptRequestHead,
    resolver: &InterruptResolver,
) -> Result<(), DurableWaitError> {
    if request
        .required_principal()
        .is_some_and(|required| required != resolver.principal())
    {
        return Err(DurableWaitError::ResolverPrincipalMismatch);
    }
    if !request
        .required_scopes()
        .is_subset(resolver.granted_scopes())
    {
        return Err(DurableWaitError::ResolverScopesInsufficient);
    }
    Ok(())
}

fn validate_terminal_causality(
    registration: &JournalHead,
    terminal: &JournalHead,
) -> Result<(), DurableWaitError> {
    if terminal.sequence().get() <= registration.sequence().get()
        || terminal.event_id() == registration.event_id()
    {
        return Err(DurableWaitError::TerminalJournalNotAfterRegistration);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::{from_value, json, to_value};

    use super::*;
    use crate::{
        BoundedJson, IssuerId, JournalEventKind, JournalSequence, SchemaId, SchemaReference, Scope,
        SubjectId, Version,
    };

    fn id<T: std::str::FromStr>(suffix: u8) -> T
    where
        T::Err: fmt::Debug,
    {
        format!("01912345-6789-7abc-8def-0123456789{suffix:02x}")
            .parse()
            .unwrap()
    }

    fn at(seconds: i64) -> Timestamp {
        Timestamp::from_unix_micros(1_893_456_000_000_000 + seconds * 1_000_000).unwrap()
    }

    fn payload(kind: &str, secret: &str) -> JournalPayload {
        JournalPayload::new(
            SchemaReference::new(
                format!("https://stateknot.github.io/schema/wait/{kind}/1.0.0")
                    .parse::<SchemaId>()
                    .unwrap(),
                Version::new(1, 0, 0),
                Digest::sha256(format!("{kind}-schema")),
            ),
            JournalEventKind::new(kind).unwrap(),
            BoundedJson::try_from_value(json!({"secret": secret})).unwrap(),
        )
        .unwrap()
    }

    fn principal(subject: &str) -> PrincipalIdentity {
        PrincipalIdentity::new(
            "https://issuer.example.com/tenant"
                .parse::<IssuerId>()
                .unwrap(),
            subject.parse::<SubjectId>().unwrap(),
        )
    }

    fn scopes(values: &[&str]) -> ScopeSet {
        ScopeSet::try_new(values.iter().map(|value| value.parse::<Scope>().unwrap())).unwrap()
    }

    fn head(
        tenant_id: TenantId,
        run_id: RunId,
        event_id: EventId,
        sequence: u64,
        recorded_at: Timestamp,
    ) -> JournalHead {
        JournalHead::new(
            tenant_id,
            run_id,
            JournalSequence::new(sequence).unwrap(),
            event_id,
            recorded_at,
            Digest::sha256(format!("event-{sequence}")),
        )
    }

    fn request(expires_at: Option<Timestamp>) -> InterruptRequest {
        let tenant_id = TenantId::new("tenant-wait").unwrap();
        let run_id = id::<RunId>(0x10);
        let event_id = id::<EventId>(0x12);
        let intent = InterruptRequestIntent::new(
            tenant_id.clone(),
            run_id,
            id::<InterruptId>(0x11),
            event_id,
            RunInterruptKind::Approval,
            payload("approval-request", "request-secret"),
            Digest::sha256(b"bound-action"),
            Some(principal("approver")),
            scopes(&["agent.approve", "run.resolve"]),
            expires_at,
        )
        .unwrap();
        InterruptRequest::commit(intent, head(tenant_id, run_id, event_id, 7, at(0))).unwrap()
    }

    fn resolution(
        request: &InterruptRequest,
        resolved_at: Timestamp,
    ) -> Result<InterruptResolution, DurableWaitError> {
        let event_id = id::<EventId>(0x13);
        let intent = InterruptResolutionIntent::new(
            request,
            event_id,
            payload("approval-resolution", "resolution-secret"),
            InterruptResolver::new(
                principal("approver"),
                scopes(&["agent.approve", "run.resolve", "audit.read"]),
            ),
        )?;
        InterruptResolution::commit(
            intent,
            head(
                request.intent().tenant_id().clone(),
                request.intent().run_id(),
                event_id,
                8,
                resolved_at,
            ),
        )
    }

    fn timer() -> DurableTimer {
        let tenant_id = TenantId::new("tenant-wait").unwrap();
        let run_id = id::<RunId>(0x10);
        let event_id = id::<EventId>(0x20);
        let intent = TimerRegistrationIntent::new(
            tenant_id.clone(),
            run_id,
            id::<TimerId>(0x21),
            event_id,
            RunTimerKind::RetryBackoff,
            at(5),
        )
        .unwrap();
        DurableTimer::commit(intent, head(tenant_id, run_id, event_id, 9, at(0))).unwrap()
    }

    #[test]
    fn interrupt_authorization_timing_history_and_redaction_are_exact() {
        let request = request(Some(at(10)));
        let committed_resolution = resolution(&request, at(5)).unwrap();
        let record =
            InterruptRecord::restore(request.clone(), Some(committed_resolution.clone())).unwrap();
        assert!(!record.is_outstanding());
        assert_eq!(record.resolution().unwrap().resolved_at(), at(5));
        assert_eq!(record.request().marker().requested_at(), at(0));

        let wire = to_value(&record).unwrap();
        assert_eq!(from_value::<InterruptRecord>(wire).unwrap(), record);
        let debug = format!("{record:?}");
        assert!(!debug.contains("request-secret"));
        assert!(!debug.contains("resolution-secret"));
        assert!(!debug.contains("approver"));

        assert_eq!(
            resolution(&request, at(10)),
            Err(DurableWaitError::InterruptExpired)
        );
        assert_eq!(
            resolution(&request, at(-1)),
            Err(DurableWaitError::ResolutionBeforeRegistration)
        );
    }

    #[test]
    fn interrupt_resolution_requires_exact_principal_and_scope_subset() {
        let request = request(None);
        let wrong_principal = InterruptResolutionIntent::new(
            &request,
            id::<EventId>(0x30),
            payload("approval-resolution", "safe"),
            InterruptResolver::new(
                principal("other"),
                scopes(&["agent.approve", "run.resolve"]),
            ),
        );
        assert_eq!(
            wrong_principal,
            Err(DurableWaitError::ResolverPrincipalMismatch)
        );

        let missing_scope = InterruptResolutionIntent::new(
            &request,
            id::<EventId>(0x31),
            payload("approval-resolution", "safe"),
            InterruptResolver::new(principal("approver"), scopes(&["agent.approve"])),
        );
        assert_eq!(
            missing_scope,
            Err(DurableWaitError::ResolverScopesInsufficient)
        );
    }

    #[test]
    fn timer_firing_uses_an_inclusive_due_boundary_and_exact_history() {
        let timer = timer();
        let firing_event_id = id::<EventId>(0x22);
        let early_intent = TimerFiringIntent::new(&timer, firing_event_id).unwrap();
        assert_eq!(
            TimerFiring::commit(
                early_intent,
                head(
                    timer.intent().tenant_id().clone(),
                    timer.intent().run_id(),
                    firing_event_id,
                    10,
                    at(4),
                ),
            ),
            Err(DurableWaitError::TimerNotDue)
        );

        let intent = TimerFiringIntent::new(&timer, firing_event_id).unwrap();
        let firing = TimerFiring::commit(
            intent,
            head(
                timer.intent().tenant_id().clone(),
                timer.intent().run_id(),
                firing_event_id,
                10,
                at(5),
            ),
        )
        .unwrap();
        let record = DurableTimerRecord::restore(timer, Some(firing)).unwrap();
        assert!(!record.is_pending());
        assert_eq!(record.firing().unwrap().fired_at(), at(5));
        let wire = to_value(&record).unwrap();
        assert_eq!(from_value::<DurableTimerRecord>(wire).unwrap(), record);
    }

    #[test]
    fn every_integrity_and_scope_layer_fails_closed_after_tampering() {
        let request = request(Some(at(10)));
        let resolution = resolution(&request, at(5)).unwrap();
        let timer = timer();

        let mut changed_action = to_value(&request).unwrap();
        changed_action["intent"]["action_digest"] = json!(Digest::sha256(b"changed"));
        assert!(from_value::<InterruptRequest>(changed_action).is_err());

        let mut changed_authority = to_value(request.head()).unwrap();
        changed_authority["required_scopes"] = json!(["run.resolve"]);
        assert!(from_value::<InterruptRequestHead>(changed_authority).is_err());

        let mut changed_resolution = to_value(&resolution).unwrap();
        changed_resolution["intent"]["resolution_payload"]["data"] = json!({"approved": false});
        assert!(from_value::<InterruptResolution>(changed_resolution).is_err());

        let mut changed_due = to_value(&timer).unwrap();
        changed_due["intent"]["due_at"] = json!(at(6));
        assert!(from_value::<DurableTimer>(changed_due).is_err());

        let other_tenant = TenantId::new("tenant-other").unwrap();
        assert_eq!(
            InterruptRequest::commit(
                request.intent().clone(),
                head(
                    other_tenant,
                    request.intent().run_id(),
                    request.intent().request_event_id(),
                    7,
                    at(0),
                ),
            ),
            Err(DurableWaitError::JournalTenantMismatch)
        );
    }

    #[test]
    fn registration_enum_materializes_the_exact_lifecycle_marker() {
        let request = request(Some(at(10)));
        let intent = WaitRegistrationIntent::interrupt(request.intent().clone());
        let committed = intent.commit(request.journal().clone()).unwrap();
        assert_eq!(committed.marker().as_interrupt(), Some(request.marker()));

        let timer = timer();
        let intent = WaitRegistrationIntent::timer(timer.intent().clone());
        let committed = intent.commit(timer.journal().clone()).unwrap();
        assert_eq!(committed.marker().as_timer(), Some(timer.marker()));
    }

    #[test]
    fn terminal_records_must_causally_follow_registration() {
        let request = request(None);
        let resolution_event_id = id::<EventId>(0x70);
        let intent = InterruptResolutionIntent::new(
            &request,
            resolution_event_id,
            payload("approval-resolution", "safe"),
            InterruptResolver::new(
                principal("approver"),
                scopes(&["agent.approve", "run.resolve"]),
            ),
        )
        .unwrap();
        assert_eq!(
            InterruptResolution::commit(
                intent,
                head(
                    request.intent().tenant_id().clone(),
                    request.intent().run_id(),
                    resolution_event_id,
                    request.journal().sequence().get(),
                    at(1),
                ),
            ),
            Err(DurableWaitError::TerminalJournalNotAfterRegistration)
        );
    }

    proptest! {
        #[test]
        fn timer_accepts_exactly_strict_registration_and_inclusive_firing_offsets(
            due_offset in 1_i64..=86_400,
            firing_offset in -10_i64..=86_410,
        ) {
            let tenant_id = TenantId::new("tenant-property").unwrap();
            let run_id = id::<RunId>(0x40);
            let registration_event_id = id::<EventId>(0x41);
            let intent = TimerRegistrationIntent::new(
                tenant_id.clone(),
                run_id,
                id::<TimerId>(0x42),
                registration_event_id,
                RunTimerKind::Sleep,
                at(due_offset),
            ).unwrap();
            let timer = DurableTimer::commit(
                intent,
                head(tenant_id.clone(), run_id, registration_event_id, 1, at(0)),
            ).unwrap();
            let firing_event_id = id::<EventId>(0x43);
            let firing_intent = TimerFiringIntent::new(&timer, firing_event_id).unwrap();
            let result = TimerFiring::commit(
                firing_intent,
                head(tenant_id, run_id, firing_event_id, 2, at(firing_offset)),
            );
            prop_assert_eq!(result.is_ok(), firing_offset >= due_offset);
        }

        #[test]
        fn interrupt_exclusive_expiry_matches_all_observation_offsets(
            expiry_offset in 1_i64..=86_400,
            resolution_offset in -10_i64..=86_410,
        ) {
            let request = request(Some(at(expiry_offset)));
            let result = resolution(&request, at(resolution_offset));
            prop_assert_eq!(
                result.is_ok(),
                (0..expiry_offset).contains(&resolution_offset)
            );
        }
    }
}
