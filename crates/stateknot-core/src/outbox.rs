// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Integrity-bound transactional outbox deliveries and delivery attempts.
//!
//! An [`OutboxDeliveryIntent`] is supplied beside one journal append and is
//! materialized as [`OutboxDelivery`] only after that exact origin event has a
//! durable [`JournalHead`]. A store must insert both records in one database
//! transaction. This closes the crash window between committing a run fact and
//! scheduling its notification.
//!
//! Delivery is deliberately at-least-once. Every network attempt first commits
//! a fixed, non-renewable [`DeliveryFence`]. A response may complete the attempt
//! only before its exclusive expiry. If the process loses an acknowledgement,
//! a later attempt may repeat the same stable [`DeliveryId`]; destination
//! adapters therefore accept only duplicate-tolerant notification protocols.
//! Non-idempotent model and tool side effects belong in their invocation
//! ledgers, not this outbox.

use std::{collections::BTreeSet, fmt};

use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{
    AttemptId, DeliveryId, DestinationId, Digest, EventId, Failure, FencingEpoch, JournalHead,
    JournalPayload, RetryAdvice, RunId, TenantId, Timestamp,
};

const INTENT_DIGEST_DOMAIN: &[u8] = b"stateknot-outbox-intent-v1\0";
const DELIVERY_DIGEST_DOMAIN: &[u8] = b"stateknot-outbox-delivery-v1\0";
const ATTEMPT_START_DIGEST_DOMAIN: &[u8] = b"stateknot-outbox-attempt-start-v1\0";
const ATTEMPT_COMPLETION_DIGEST_DOMAIN: &[u8] = b"stateknot-outbox-attempt-completion-v1\0";

/// Hard bound on physical attempts retained for one delivery.
pub const MAX_OUTBOX_ATTEMPTS: usize = 64;

/// Longest permitted fixed attempt lease, in milliseconds.
///
/// Protocol adapters must configure their complete request timeout below this
/// bound and below the lease they request. Attempts are intentionally not
/// renewable, keeping takeover and lost-ack behavior deterministic.
pub const MAX_OUTBOX_ATTEMPT_LEASE_MILLIS: i64 = 300_000;

/// Immutable reference to one tenant-owned destination configuration snapshot.
///
/// The referenced snapshot contains protocol routing and credential *handles*;
/// raw credentials must never be embedded in an outbox record. Keeping the
/// digest here prevents a destination update from changing an in-flight
/// delivery's authority or routing semantics.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxDestinationRef {
    tenant_id: TenantId,
    destination_id: DestinationId,
    snapshot_digest: Digest,
}

impl OutboxDestinationRef {
    /// Constructs an immutable tenant-scoped destination reference.
    #[must_use]
    pub const fn new(
        tenant_id: TenantId,
        destination_id: DestinationId,
        snapshot_digest: Digest,
    ) -> Self {
        Self {
            tenant_id,
            destination_id,
            snapshot_digest,
        }
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the stable destination identity.
    #[must_use]
    pub const fn destination_id(&self) -> DestinationId {
        self.destination_id
    }

    /// Returns the immutable destination snapshot checksum.
    #[must_use]
    pub const fn snapshot_digest(&self) -> Digest {
        self.snapshot_digest
    }
}

/// Idempotent pre-commit request to enqueue one notification.
///
/// `origin_event_id` lets a retry prove which journal append must be paired
/// with this intent. Reusing `delivery_id` is valid only when every field and
/// `intent_digest` match the existing record.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxDeliveryIntent {
    tenant_id: TenantId,
    run_id: RunId,
    delivery_id: DeliveryId,
    origin_event_id: EventId,
    destination: OutboxDestinationRef,
    payload: JournalPayload,
    expires_at: Timestamp,
    intent_digest: Digest,
}

impl OutboxDeliveryIntent {
    /// Constructs an integrity-bound enqueue request.
    ///
    /// The payload is a schema-pinned canonical envelope consumed by the
    /// protocol adapter. It is not an arbitrary external side-effect command.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxDeliveryError`] for a crossed tenant boundary or an
    /// integrity calculation failure.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        delivery_id: DeliveryId,
        origin_event_id: EventId,
        destination: OutboxDestinationRef,
        payload: JournalPayload,
        expires_at: Timestamp,
    ) -> Result<Self, OutboxDeliveryError> {
        if destination.tenant_id() != &tenant_id {
            return Err(OutboxDeliveryError::DestinationTenantMismatch);
        }
        let intent_digest = compute_intent_digest(&OutboxIntentDigestWire {
            tenant_id: &tenant_id,
            run_id,
            delivery_id,
            origin_event_id,
            destination: &destination,
            payload_digest: payload.digest(),
            expires_at,
        })?;
        Ok(Self {
            tenant_id,
            run_id,
            delivery_id,
            origin_event_id,
            destination,
            payload,
            expires_at,
            intent_digest,
        })
    }

    /// Restores and verifies a persisted enqueue request.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxDeliveryError`] when scope or the checksum differs.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        tenant_id: TenantId,
        run_id: RunId,
        delivery_id: DeliveryId,
        origin_event_id: EventId,
        destination: OutboxDestinationRef,
        payload: JournalPayload,
        expires_at: Timestamp,
        intent_digest: Digest,
    ) -> Result<Self, OutboxDeliveryError> {
        let restored = Self::new(
            tenant_id,
            run_id,
            delivery_id,
            origin_event_id,
            destination,
            payload,
            expires_at,
        )?;
        if restored.intent_digest != intent_digest {
            return Err(OutboxDeliveryError::IntentDigestMismatch);
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

    /// Returns the stable delivery identity used across retries.
    #[must_use]
    pub const fn delivery_id(&self) -> DeliveryId {
        self.delivery_id
    }

    /// Returns the exact origin event requested by the caller.
    #[must_use]
    pub const fn origin_event_id(&self) -> EventId {
        self.origin_event_id
    }

    /// Returns the immutable destination snapshot reference.
    #[must_use]
    pub const fn destination(&self) -> &OutboxDestinationRef {
        &self.destination
    }

    /// Returns the immutable schema-pinned delivery payload.
    #[must_use]
    pub const fn payload(&self) -> &JournalPayload {
        &self.payload
    }

    /// Returns the exclusive delivery deadline.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the complete idempotency checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }
}

impl fmt::Debug for OutboxDeliveryIntent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxDeliveryIntent")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("delivery_id", &self.delivery_id)
            .field("origin_event_id", &self.origin_event_id)
            .field("destination", &self.destination)
            .field("payload", &self.payload)
            .field("expires_at", &self.expires_at)
            .field("intent_digest", &self.intent_digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for OutboxDeliveryIntent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            delivery_id: DeliveryId,
            origin_event_id: EventId,
            destination: OutboxDestinationRef,
            payload: JournalPayload,
            expires_at: Timestamp,
            intent_digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.tenant_id,
            wire.run_id,
            wire.delivery_id,
            wire.origin_event_id,
            wire.destination,
            wire.payload,
            wire.expires_at,
            wire.intent_digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Immutable outbox record atomically committed with its origin journal fact.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxDelivery {
    intent: OutboxDeliveryIntent,
    origin: JournalHead,
    digest: Digest,
}

impl OutboxDelivery {
    /// Materializes an enqueue intent against the exact committed origin.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxDeliveryError`] for substituted scope/event identity, a
    /// deadline that is not after the origin, or integrity failure.
    pub fn commit(
        intent: OutboxDeliveryIntent,
        origin: JournalHead,
    ) -> Result<Self, OutboxDeliveryError> {
        validate_delivery_shape(&intent, &origin)?;
        let digest = compute_delivery_digest(&OutboxDeliveryDigestWire {
            tenant_id: &intent.tenant_id,
            run_id: intent.run_id,
            delivery_id: intent.delivery_id,
            intent_digest: intent.intent_digest,
            origin: &origin,
            expires_at: intent.expires_at,
        })?;
        Ok(Self {
            intent,
            origin,
            digest,
        })
    }

    /// Restores and verifies a durable delivery record.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxDeliveryError`] when any invariant or checksum differs.
    pub fn restore(
        intent: OutboxDeliveryIntent,
        origin: JournalHead,
        digest: Digest,
    ) -> Result<Self, OutboxDeliveryError> {
        let restored = Self::commit(intent, origin)?;
        if restored.digest != digest {
            return Err(OutboxDeliveryError::DeliveryDigestMismatch);
        }
        Ok(restored)
    }

    /// Returns the immutable enqueue intent.
    #[must_use]
    pub const fn intent(&self) -> &OutboxDeliveryIntent {
        &self.intent
    }

    /// Returns the exact atomically paired journal origin.
    #[must_use]
    pub const fn origin(&self) -> &JournalHead {
        &self.origin
    }

    /// Returns the complete delivery checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns a compact exact reference used by attempt records.
    #[must_use]
    pub fn head(&self) -> OutboxDeliveryHead {
        OutboxDeliveryHead {
            tenant_id: self.intent.tenant_id.clone(),
            run_id: self.intent.run_id,
            delivery_id: self.intent.delivery_id,
            origin: self.origin.clone(),
            intent_digest: self.intent.intent_digest,
            expires_at: self.intent.expires_at,
            digest: self.digest,
        }
    }
}

impl fmt::Debug for OutboxDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxDelivery")
            .field("intent", &self.intent)
            .field("origin", &self.origin)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for OutboxDelivery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            intent: OutboxDeliveryIntent,
            origin: JournalHead,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.intent, wire.origin, wire.digest).map_err(de::Error::custom)
    }
}

/// Compact, integrity-verifiable identity of one committed delivery.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxDeliveryHead {
    tenant_id: TenantId,
    run_id: RunId,
    delivery_id: DeliveryId,
    origin: JournalHead,
    intent_digest: Digest,
    expires_at: Timestamp,
    digest: Digest,
}

impl OutboxDeliveryHead {
    /// Restores and verifies a compact delivery identity.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxDeliveryError`] for crossed scope, an invalid deadline,
    /// or a checksum mismatch.
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        tenant_id: TenantId,
        run_id: RunId,
        delivery_id: DeliveryId,
        origin: JournalHead,
        intent_digest: Digest,
        expires_at: Timestamp,
        digest: Digest,
    ) -> Result<Self, OutboxDeliveryError> {
        validate_delivery_head_scope(&tenant_id, run_id, &origin, expires_at)?;
        let expected = compute_delivery_digest(&OutboxDeliveryDigestWire {
            tenant_id: &tenant_id,
            run_id,
            delivery_id,
            intent_digest,
            origin: &origin,
            expires_at,
        })?;
        if expected != digest {
            return Err(OutboxDeliveryError::DeliveryDigestMismatch);
        }
        Ok(Self {
            tenant_id,
            run_id,
            delivery_id,
            origin,
            intent_digest,
            expires_at,
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

    /// Returns the stable delivery identity.
    #[must_use]
    pub const fn delivery_id(&self) -> DeliveryId {
        self.delivery_id
    }

    /// Returns the exact origin journal head.
    #[must_use]
    pub const fn origin(&self) -> &JournalHead {
        &self.origin
    }

    /// Returns the enqueue intent checksum.
    #[must_use]
    pub const fn intent_digest(&self) -> Digest {
        self.intent_digest
    }

    /// Returns the exclusive delivery deadline.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the full delivery checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for OutboxDeliveryHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            tenant_id: TenantId,
            run_id: RunId,
            delivery_id: DeliveryId,
            origin: JournalHead,
            intent_digest: Digest,
            expires_at: Timestamp,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.tenant_id,
            wire.run_id,
            wire.delivery_id,
            wire.origin,
            wire.intent_digest,
            wire.expires_at,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Fencing token authorizing one fixed physical delivery attempt.
///
/// The token is not authority by itself. Completion stores must compare every
/// field and the database clock against the locked current delivery row in the
/// same transaction.
#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryFence {
    tenant_id: TenantId,
    run_id: RunId,
    delivery_id: DeliveryId,
    attempt_id: AttemptId,
    epoch: FencingEpoch,
}

impl DeliveryFence {
    /// Constructs a delivery-scoped fence from trusted allocation results.
    #[must_use]
    pub const fn new(
        tenant_id: TenantId,
        run_id: RunId,
        delivery_id: DeliveryId,
        attempt_id: AttemptId,
        epoch: FencingEpoch,
    ) -> Self {
        Self {
            tenant_id,
            run_id,
            delivery_id,
            attempt_id,
            epoch,
        }
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

    /// Returns the fenced delivery.
    #[must_use]
    pub const fn delivery_id(&self) -> DeliveryId {
        self.delivery_id
    }

    /// Returns the unique physical attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the monotonic delivery ownership epoch.
    #[must_use]
    pub const fn epoch(&self) -> FencingEpoch {
        self.epoch
    }
}

/// Lifecycle state of one physical outbox attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OutboxAttemptStatus {
    /// The attempt is durably started and has no completion.
    Delivering,
    /// The destination acknowledged the notification.
    Acknowledged,
    /// Public-safe failure evidence was committed.
    Failed,
}

/// Terminal outcome of one physical delivery attempt.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, JsonSchema, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum OutboxAttemptOutcome {
    /// The destination returned a protocol-defined acknowledgement.
    Acknowledged {
        /// Optional digest of bounded, non-secret acknowledgement evidence.
        #[serde(skip_serializing_if = "Option::is_none")]
        evidence_digest: Option<Digest>,
    },
    /// The attempt failed with explicit retry advice.
    Failed {
        /// Public-safe failure evidence; secrets and raw response bodies are forbidden.
        failure: Failure,
    },
}

impl OutboxAttemptOutcome {
    /// Returns the terminal physical-attempt status.
    #[must_use]
    pub const fn status(&self) -> OutboxAttemptStatus {
        match self {
            Self::Acknowledged { .. } => OutboxAttemptStatus::Acknowledged,
            Self::Failed { .. } => OutboxAttemptStatus::Failed,
        }
    }

    /// Returns acknowledgement evidence, if the attempt succeeded.
    #[must_use]
    pub const fn evidence_digest(&self) -> Option<Digest> {
        match self {
            Self::Acknowledged { evidence_digest } => *evidence_digest,
            Self::Failed { .. } => None,
        }
    }

    /// Returns public-safe failure evidence, if the attempt failed.
    #[must_use]
    pub const fn failure(&self) -> Option<&Failure> {
        match self {
            Self::Acknowledged { .. } => None,
            Self::Failed { failure } => Some(failure),
        }
    }
}

impl fmt::Debug for OutboxAttemptOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Acknowledged { evidence_digest } => formatter
                .debug_struct("Acknowledged")
                .field("evidence_digest", evidence_digest)
                .finish(),
            Self::Failed { failure } => formatter
                .debug_struct("Failed")
                .field("failure_id", &failure.id())
                .field("category", &failure.category())
                .field("code", failure.code())
                .field("origin", failure.origin())
                .field("retry_advice", &failure.retry_advice())
                .finish_non_exhaustive(),
        }
    }
}

impl<'de> Deserialize<'de> for OutboxAttemptOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[allow(clippy::large_enum_variant)]
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
        enum Wire {
            Acknowledged {
                #[serde(default)]
                evidence_digest: Option<Digest>,
            },
            Failed {
                failure: Failure,
            },
        }

        Ok(match Wire::deserialize(deserializer)? {
            Wire::Acknowledged { evidence_digest } => Self::Acknowledged { evidence_digest },
            Wire::Failed { failure } => Self::Failed { failure },
        })
    }
}

/// Durable admission record committed before one network request begins.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxAttemptStart {
    delivery: OutboxDeliveryHead,
    fence: DeliveryFence,
    started_at: Timestamp,
    expires_at: Timestamp,
    digest: Digest,
}

impl OutboxAttemptStart {
    /// Constructs a fixed, non-renewable attempt lease.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxAttemptError`] for crossed scope, timing outside the
    /// delivery window, a lease over five minutes, or integrity failure.
    pub fn new(
        delivery: &OutboxDelivery,
        fence: DeliveryFence,
        started_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, OutboxAttemptError> {
        Self::materialize(delivery.head(), fence, started_at, expires_at)
    }

    /// Restores and verifies a durable attempt start.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxAttemptError`] when any invariant or checksum differs.
    pub fn restore(
        delivery: OutboxDeliveryHead,
        fence: DeliveryFence,
        started_at: Timestamp,
        expires_at: Timestamp,
        digest: Digest,
    ) -> Result<Self, OutboxAttemptError> {
        let restored = Self::materialize(delivery, fence, started_at, expires_at)?;
        if restored.digest != digest {
            return Err(OutboxAttemptError::StartDigestMismatch);
        }
        Ok(restored)
    }

    fn materialize(
        delivery: OutboxDeliveryHead,
        fence: DeliveryFence,
        started_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, OutboxAttemptError> {
        validate_attempt_start(&delivery, &fence, started_at, expires_at)?;
        let digest = compute_attempt_start_digest(&OutboxAttemptStartDigestWire {
            delivery: &delivery,
            fence: &fence,
            started_at,
            expires_at,
        })?;
        Ok(Self {
            delivery,
            fence,
            started_at,
            expires_at,
            digest,
        })
    }

    /// Returns the exact delivery being attempted.
    #[must_use]
    pub const fn delivery(&self) -> &OutboxDeliveryHead {
        &self.delivery
    }

    /// Returns the attempt fence.
    #[must_use]
    pub const fn fence(&self) -> &DeliveryFence {
        &self.fence
    }

    /// Returns the database start observation.
    #[must_use]
    pub const fn started_at(&self) -> Timestamp {
        self.started_at
    }

    /// Returns the exclusive fixed attempt expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the full start checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// Returns a compact exact reference used by completion records.
    #[must_use]
    pub fn head(&self) -> OutboxAttemptStartHead {
        OutboxAttemptStartHead {
            delivery: self.delivery.clone(),
            fence: self.fence.clone(),
            started_at: self.started_at,
            expires_at: self.expires_at,
            digest: self.digest,
        }
    }
}

impl fmt::Debug for OutboxAttemptStart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxAttemptStart")
            .field("delivery", &self.delivery)
            .field("fence", &self.fence)
            .field("started_at", &self.started_at)
            .field("expires_at", &self.expires_at)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for OutboxAttemptStart {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            delivery: OutboxDeliveryHead,
            fence: DeliveryFence,
            started_at: Timestamp,
            expires_at: Timestamp,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(
            wire.delivery,
            wire.fence,
            wire.started_at,
            wire.expires_at,
            wire.digest,
        )
        .map_err(de::Error::custom)
    }
}

/// Compact exact identity of one durable delivery-attempt start.
#[derive(Clone, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxAttemptStartHead {
    delivery: OutboxDeliveryHead,
    fence: DeliveryFence,
    started_at: Timestamp,
    expires_at: Timestamp,
    digest: Digest,
}

impl OutboxAttemptStartHead {
    /// Restores and verifies a compact start identity.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxAttemptError`] for invalid scope, timing, or checksum.
    pub fn restore(
        delivery: OutboxDeliveryHead,
        fence: DeliveryFence,
        started_at: Timestamp,
        expires_at: Timestamp,
        digest: Digest,
    ) -> Result<Self, OutboxAttemptError> {
        Ok(OutboxAttemptStart::restore(delivery, fence, started_at, expires_at, digest)?.head())
    }

    /// Returns the exact delivery identity.
    #[must_use]
    pub const fn delivery(&self) -> &OutboxDeliveryHead {
        &self.delivery
    }

    /// Returns the exact attempt fence.
    #[must_use]
    pub const fn fence(&self) -> &DeliveryFence {
        &self.fence
    }

    /// Returns the database start observation.
    #[must_use]
    pub const fn started_at(&self) -> Timestamp {
        self.started_at
    }

    /// Returns the exclusive fixed attempt expiry.
    #[must_use]
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns the complete start checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl<'de> Deserialize<'de> for OutboxAttemptStartHead {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            delivery: OutboxDeliveryHead,
            fence: DeliveryFence,
            started_at: Timestamp,
            expires_at: Timestamp,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        OutboxAttemptStart::restore(
            wire.delivery,
            wire.fence,
            wire.started_at,
            wire.expires_at,
            wire.digest,
        )
        .map(|start| start.head())
        .map_err(de::Error::custom)
    }
}

/// Immutable terminal record for one already-started delivery attempt.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxAttemptCompletion {
    start: OutboxAttemptStartHead,
    outcome: OutboxAttemptOutcome,
    completed_at: Timestamp,
    digest: Digest,
}

impl OutboxAttemptCompletion {
    /// Commits a protocol acknowledgement before the attempt fence expires.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxAttemptError`] for invalid timing or integrity material.
    pub fn acknowledge(
        start: &OutboxAttemptStart,
        evidence_digest: Option<Digest>,
        completed_at: Timestamp,
    ) -> Result<Self, OutboxAttemptError> {
        Self::materialize(
            start.head(),
            OutboxAttemptOutcome::Acknowledged { evidence_digest },
            completed_at,
        )
    }

    /// Commits public-safe failure evidence before the attempt fence expires.
    ///
    /// Reconcile-first failures are rejected: outbox destinations must tolerate
    /// duplicate delivery and may therefore use only safe-after or never.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxAttemptError`] for unsafe retry advice, invalid timing,
    /// or integrity material.
    pub fn fail(
        start: &OutboxAttemptStart,
        failure: Failure,
        completed_at: Timestamp,
    ) -> Result<Self, OutboxAttemptError> {
        Self::materialize(
            start.head(),
            OutboxAttemptOutcome::Failed { failure },
            completed_at,
        )
    }

    /// Restores and verifies a persisted terminal record.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxAttemptError`] when any invariant or checksum differs.
    pub fn restore(
        start: OutboxAttemptStartHead,
        outcome: OutboxAttemptOutcome,
        completed_at: Timestamp,
        digest: Digest,
    ) -> Result<Self, OutboxAttemptError> {
        let restored = Self::materialize(start, outcome, completed_at)?;
        if restored.digest != digest {
            return Err(OutboxAttemptError::CompletionDigestMismatch);
        }
        Ok(restored)
    }

    fn materialize(
        start: OutboxAttemptStartHead,
        outcome: OutboxAttemptOutcome,
        completed_at: Timestamp,
    ) -> Result<Self, OutboxAttemptError> {
        validate_attempt_completion(&start, &outcome, completed_at)?;
        let digest = compute_attempt_completion_digest(&OutboxAttemptCompletionDigestWire {
            start: &start,
            outcome: &outcome,
            completed_at,
        })?;
        Ok(Self {
            start,
            outcome,
            completed_at,
            digest,
        })
    }

    /// Returns the exact attempt start completed by this record.
    #[must_use]
    pub const fn start(&self) -> &OutboxAttemptStartHead {
        &self.start
    }

    /// Returns the terminal outcome.
    #[must_use]
    pub const fn outcome(&self) -> &OutboxAttemptOutcome {
        &self.outcome
    }

    /// Returns the terminal physical-attempt status.
    #[must_use]
    pub const fn status(&self) -> OutboxAttemptStatus {
        self.outcome.status()
    }

    /// Returns the database completion observation.
    #[must_use]
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }

    /// Returns the complete terminal-record checksum.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

impl fmt::Debug for OutboxAttemptCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxAttemptCompletion")
            .field("start", &self.start)
            .field("outcome", &self.outcome)
            .field("completed_at", &self.completed_at)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for OutboxAttemptCompletion {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            start: OutboxAttemptStartHead,
            outcome: OutboxAttemptOutcome,
            completed_at: Timestamp,
            digest: Digest,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.start, wire.outcome, wire.completed_at, wire.digest)
            .map_err(de::Error::custom)
    }
}

/// Fully restored physical delivery attempt, active or terminal.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxAttempt {
    start: OutboxAttemptStart,
    #[serde(skip_serializing_if = "Option::is_none")]
    completion: Option<OutboxAttemptCompletion>,
}

impl OutboxAttempt {
    /// Constructs an active attempt from its durable start.
    #[must_use]
    pub const fn delivering(start: OutboxAttemptStart) -> Self {
        Self {
            start,
            completion: None,
        }
    }

    /// Restores an attempt and verifies the exact start/completion join.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxAttemptError::CompletionStartMismatch`] for a crossed
    /// physical attempt.
    pub fn restore(
        start: OutboxAttemptStart,
        completion: Option<OutboxAttemptCompletion>,
    ) -> Result<Self, OutboxAttemptError> {
        if completion
            .as_ref()
            .is_some_and(|completion| completion.start() != &start.head())
        {
            return Err(OutboxAttemptError::CompletionStartMismatch);
        }
        Ok(Self { start, completion })
    }

    /// Returns the immutable durable start.
    #[must_use]
    pub const fn start(&self) -> &OutboxAttemptStart {
        &self.start
    }

    /// Returns the optional terminal completion.
    #[must_use]
    pub const fn completion(&self) -> Option<&OutboxAttemptCompletion> {
        self.completion.as_ref()
    }

    /// Returns the current physical-attempt status.
    #[must_use]
    pub const fn status(&self) -> OutboxAttemptStatus {
        match &self.completion {
            Some(completion) => completion.status(),
            None => OutboxAttemptStatus::Delivering,
        }
    }
}

impl fmt::Debug for OutboxAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxAttempt")
            .field("start", &self.start)
            .field("completion", &self.completion)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for OutboxAttempt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            start: OutboxAttemptStart,
            #[serde(default)]
            completion: Option<OutboxAttemptCompletion>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::restore(wire.start, wire.completion).map_err(de::Error::custom)
    }
}

/// Projected lifecycle of a complete delivery at one database observation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OutboxDeliveryStatus {
    /// Eligible for a new physical attempt.
    Pending,
    /// A fixed attempt lease is still live.
    Delivering,
    /// A safe retry exists but its delay has not elapsed.
    RetryScheduled,
    /// A destination acknowledgement terminated delivery.
    Acknowledged,
    /// Retry advice or the hard attempt bound terminated delivery.
    DeadLetter,
    /// The delivery deadline elapsed before acknowledgement.
    Expired,
}

/// Streaming validator and state projector for one delivery's attempt history.
///
/// Attempts use epoch one followed by exact successors. An uncompleted attempt
/// can be replaced only after its fixed exclusive expiry. This verifier is
/// bounded and rejects rather than mutates its state on every invalid record.
#[derive(Clone, Debug)]
pub struct OutboxAttemptHistoryVerifier {
    delivery: OutboxDeliveryHead,
    last: Option<OutboxAttempt>,
    attempt_ids: BTreeSet<AttemptId>,
    count: usize,
}

impl OutboxAttemptHistoryVerifier {
    /// Constructs an empty verifier for one committed delivery.
    #[must_use]
    pub fn new(delivery: &OutboxDelivery) -> Self {
        Self {
            delivery: delivery.head(),
            last: None,
            attempt_ids: BTreeSet::new(),
            count: 0,
        }
    }

    /// Returns the delivery identity being verified.
    #[must_use]
    pub const fn delivery(&self) -> &OutboxDeliveryHead {
        &self.delivery
    }

    /// Returns the number of verified physical attempts.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.count
    }

    /// Returns the last verified full attempt, if present.
    #[must_use]
    pub const fn last(&self) -> Option<&OutboxAttempt> {
        self.last.as_ref()
    }

    /// Verifies and advances to the next physical attempt.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxAttemptHistoryError`] for substitution, non-contiguous
    /// fencing, premature takeover/retry, terminal successors, or overflow.
    pub fn verify_next(
        &mut self,
        attempt: &OutboxAttempt,
    ) -> Result<(), OutboxAttemptHistoryError> {
        if self.count == MAX_OUTBOX_ATTEMPTS {
            return Err(OutboxAttemptHistoryError::AttemptLimitExceeded);
        }
        if attempt.start.delivery != self.delivery {
            return Err(OutboxAttemptHistoryError::DeliveryMismatch);
        }
        let attempt_id = attempt.start.fence.attempt_id;
        if self.attempt_ids.contains(&attempt_id) {
            return Err(OutboxAttemptHistoryError::AttemptIdReused { attempt_id });
        }

        if let Some(previous) = self.last.as_ref() {
            validate_attempt_successor(previous, attempt)?;
        } else if attempt.start.fence.epoch != FencingEpoch::FIRST {
            return Err(OutboxAttemptHistoryError::FirstEpochInvalid {
                actual: attempt.start.fence.epoch,
            });
        }

        self.attempt_ids.insert(attempt_id);
        self.last = Some(attempt.clone());
        self.count += 1;
        Ok(())
    }

    /// Projects durable delivery state at one database-clock observation.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxAttemptHistoryError::ObservationClockRegression`] when
    /// the observation precedes already-verified durable evidence.
    pub fn status_at(
        &self,
        observed_at: Timestamp,
    ) -> Result<OutboxDeliveryStatus, OutboxAttemptHistoryError> {
        let latest_at = self.last.as_ref().map_or_else(
            || self.delivery.origin.recorded_at(),
            |attempt| {
                attempt.completion.as_ref().map_or_else(
                    || attempt.start.started_at,
                    |completion| completion.completed_at,
                )
            },
        );
        if observed_at < latest_at {
            return Err(OutboxAttemptHistoryError::ObservationClockRegression {
                latest_at,
                observed_at,
            });
        }

        if let Some(completion) = self
            .last
            .as_ref()
            .and_then(|value| value.completion.as_ref())
        {
            match completion.outcome() {
                OutboxAttemptOutcome::Acknowledged { .. } => {
                    return Ok(OutboxDeliveryStatus::Acknowledged);
                }
                OutboxAttemptOutcome::Failed { failure }
                    if matches!(failure.retry_advice(), RetryAdvice::Never) =>
                {
                    return Ok(OutboxDeliveryStatus::DeadLetter);
                }
                OutboxAttemptOutcome::Failed { .. } if self.count == MAX_OUTBOX_ATTEMPTS => {
                    return Ok(OutboxDeliveryStatus::DeadLetter);
                }
                OutboxAttemptOutcome::Failed { .. } => {}
            }
        }

        if let Some(last) = self.last.as_ref() {
            if last.completion.is_none()
                && self.count == MAX_OUTBOX_ATTEMPTS
                && observed_at >= last.start.expires_at
            {
                return Ok(if last.start.expires_at < self.delivery.expires_at {
                    OutboxDeliveryStatus::DeadLetter
                } else {
                    OutboxDeliveryStatus::Expired
                });
            }
        }

        if observed_at >= self.delivery.expires_at {
            return Ok(OutboxDeliveryStatus::Expired);
        }

        let Some(last) = self.last.as_ref() else {
            return Ok(OutboxDeliveryStatus::Pending);
        };
        let Some(completion) = last.completion.as_ref() else {
            if observed_at < last.start.expires_at {
                return Ok(OutboxDeliveryStatus::Delivering);
            }
            return Ok(OutboxDeliveryStatus::Pending);
        };

        let OutboxAttemptOutcome::Failed { failure } = completion.outcome() else {
            return Ok(OutboxDeliveryStatus::Acknowledged);
        };
        let RetryAdvice::SafeAfter { delay } = failure.retry_advice() else {
            return Ok(OutboxDeliveryStatus::DeadLetter);
        };
        let eligible_at = retry_eligible_at(completion.completed_at, delay.as_i64())
            .map_err(|_| OutboxAttemptHistoryError::RetryDelayOutOfRange)?;
        Ok(if observed_at < eligible_at {
            OutboxDeliveryStatus::RetryScheduled
        } else {
            OutboxDeliveryStatus::Pending
        })
    }
}

/// Invalid delivery scope, atomic origin binding, deadline, or checksum.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum OutboxDeliveryError {
    /// Destination reference crossed the enqueue tenant boundary.
    #[error("outbox destination crosses the delivery tenant boundary")]
    DestinationTenantMismatch,
    /// Origin journal crossed the enqueue tenant boundary.
    #[error("outbox origin crosses the delivery tenant boundary")]
    OriginTenantMismatch,
    /// Origin journal named another run.
    #[error("outbox origin does not belong to the delivery run")]
    OriginRunMismatch,
    /// The committed journal event differed from the requested origin event.
    #[error("outbox origin event does not match the enqueue intent")]
    OriginEventMismatch,
    /// The delivery deadline was not strictly after the origin commit time.
    #[error("outbox delivery deadline must be after its origin")]
    DeadlineNotAfterOrigin,
    /// Persisted enqueue checksum did not match its fields.
    #[error("outbox intent digest does not match its fields")]
    IntentDigestMismatch,
    /// Persisted committed-delivery checksum did not match its fields.
    #[error("outbox delivery digest does not match its fields")]
    DeliveryDigestMismatch,
    /// Canonical integrity material could not be serialized.
    #[error("outbox delivery integrity calculation failed: {source}")]
    Integrity {
        /// Exact integrity failure.
        #[source]
        source: OutboxDeliveryIntegrityError,
    },
}

impl From<OutboxDeliveryIntegrityError> for OutboxDeliveryError {
    fn from(source: OutboxDeliveryIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Failure to canonicalize delivery integrity material.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum OutboxDeliveryIntegrityError {
    /// A closed typed checksum preimage could not be canonicalized.
    #[error("outbox delivery checksum preimage serialization failed")]
    CanonicalSerialization,
}

/// Invalid attempt fence, timing, completion, or checksum.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum OutboxAttemptError {
    /// Attempt fence crossed the delivery tenant boundary.
    #[error("outbox attempt fence crosses the delivery tenant boundary")]
    FenceTenantMismatch,
    /// Attempt fence named another run.
    #[error("outbox attempt fence does not belong to the delivery run")]
    FenceRunMismatch,
    /// Attempt fence named another delivery.
    #[error("outbox attempt fence does not belong to the delivery")]
    FenceDeliveryMismatch,
    /// Attempt start preceded the atomic origin commit.
    #[error("outbox attempt starts before its delivery origin")]
    StartBeforeOrigin,
    /// Attempt start was at or after the delivery deadline.
    #[error("outbox attempt starts at or after the delivery deadline")]
    StartOutsideDeliveryWindow,
    /// Attempt expiry was not strictly after start.
    #[error("outbox attempt expiry must be after its start")]
    ExpiryNotAfterStart,
    /// Attempt expiry exceeded the delivery deadline.
    #[error("outbox attempt expiry exceeds the delivery deadline")]
    ExpiryAfterDeliveryDeadline,
    /// Attempt lease exceeded the hard duration bound.
    #[error("outbox attempt lease is {actual_millis}ms; maximum is {maximum_millis}ms")]
    LeaseTooLong {
        /// Maximum accepted duration.
        maximum_millis: i64,
        /// Observed ceiling duration in milliseconds.
        actual_millis: i64,
    },
    /// Completion preceded its start.
    #[error("outbox attempt completion precedes its start")]
    CompletionBeforeStart,
    /// Completion occurred at or after the exclusive attempt expiry.
    #[error("outbox attempt completion is outside its live fence")]
    CompletionOutsideFence,
    /// Duplicate-tolerant delivery cannot require ambiguity reconciliation.
    #[error("outbox failure cannot require external reconciliation")]
    ReconciliationUnsupported,
    /// Safe retry delay could not be represented as a canonical timestamp.
    #[error("outbox retry delay exceeds the supported timestamp range")]
    RetryDelayOutOfRange,
    /// Completion joined to another physical start.
    #[error("outbox attempt completion does not belong to its start")]
    CompletionStartMismatch,
    /// Persisted attempt-start checksum did not match its fields.
    #[error("outbox attempt start digest does not match its fields")]
    StartDigestMismatch,
    /// Persisted attempt-completion checksum did not match its fields.
    #[error("outbox attempt completion digest does not match its fields")]
    CompletionDigestMismatch,
    /// Canonical integrity material could not be serialized.
    #[error("outbox attempt integrity calculation failed: {source}")]
    Integrity {
        /// Exact integrity failure.
        #[source]
        source: OutboxAttemptIntegrityError,
    },
}

impl From<OutboxAttemptIntegrityError> for OutboxAttemptError {
    fn from(source: OutboxAttemptIntegrityError) -> Self {
        Self::Integrity { source }
    }
}

/// Failure to canonicalize attempt integrity material.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum OutboxAttemptIntegrityError {
    /// A closed typed checksum preimage could not be canonicalized.
    #[error("outbox attempt checksum preimage serialization failed")]
    CanonicalSerialization,
}

/// Invalid physical-attempt history for one logical delivery.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum OutboxAttemptHistoryError {
    /// More than the bounded number of attempts was supplied.
    #[error("outbox delivery exceeded its physical attempt limit")]
    AttemptLimitExceeded,
    /// An attempt belonged to another logical delivery.
    #[error("outbox attempt history changed its logical delivery")]
    DeliveryMismatch,
    /// A physical attempt identity was used twice.
    #[error("outbox attempt history reused physical attempt {attempt_id}")]
    AttemptIdReused {
        /// Reused physical identity.
        attempt_id: AttemptId,
    },
    /// First attempt did not use fencing epoch one.
    #[error("first outbox attempt used epoch {actual}; expected one")]
    FirstEpochInvalid {
        /// Rejected first epoch.
        actual: FencingEpoch,
    },
    /// A successor did not use the exact next fencing epoch.
    #[error("outbox attempt fencing epoch is not the exact successor")]
    EpochNotSuccessor,
    /// A successor start clock regressed behind prior durable evidence.
    #[error("outbox attempt history durable clock regressed")]
    ClockRegression,
    /// An unfinished attempt was replaced before its fixed lease expired.
    #[error("unfinished outbox attempt cannot be replaced before lease expiry")]
    LiveAttemptSuperseded,
    /// An acknowledged delivery was attempted again.
    #[error("acknowledged outbox delivery cannot start another attempt")]
    PreviousAttemptAcknowledged,
    /// Failure evidence did not authorize another automatic attempt.
    #[error("outbox failure recovery advice does not authorize automatic retry")]
    RetryNotAuthorized,
    /// A successor preceded the explicit safe-after delay.
    #[error("outbox retry started before its explicit safe-after delay elapsed")]
    RetryDelayNotElapsed {
        /// Earliest allowed successor start.
        eligible_at: Timestamp,
        /// Rejected successor start.
        started_at: Timestamp,
    },
    /// Safe-after timestamp could not be represented.
    #[error("outbox retry delay exceeds the supported timestamp range")]
    RetryDelayOutOfRange,
    /// A status observation preceded already verified durable evidence.
    #[error("outbox status observation clock regressed")]
    ObservationClockRegression {
        /// Latest durable timestamp in the verified history.
        latest_at: Timestamp,
        /// Rejected observation.
        observed_at: Timestamp,
    },
}

#[derive(Serialize)]
struct OutboxIntentDigestWire<'a> {
    tenant_id: &'a TenantId,
    run_id: RunId,
    delivery_id: DeliveryId,
    origin_event_id: EventId,
    destination: &'a OutboxDestinationRef,
    payload_digest: Digest,
    expires_at: Timestamp,
}

#[derive(Serialize)]
struct OutboxDeliveryDigestWire<'a> {
    tenant_id: &'a TenantId,
    run_id: RunId,
    delivery_id: DeliveryId,
    intent_digest: Digest,
    origin: &'a JournalHead,
    expires_at: Timestamp,
}

#[derive(Serialize)]
struct OutboxAttemptStartDigestWire<'a> {
    delivery: &'a OutboxDeliveryHead,
    fence: &'a DeliveryFence,
    started_at: Timestamp,
    expires_at: Timestamp,
}

#[derive(Serialize)]
struct OutboxAttemptCompletionDigestWire<'a> {
    start: &'a OutboxAttemptStartHead,
    outcome: &'a OutboxAttemptOutcome,
    completed_at: Timestamp,
}

fn compute_intent_digest(
    value: &OutboxIntentDigestWire<'_>,
) -> Result<Digest, OutboxDeliveryIntegrityError> {
    delivery_digest(INTENT_DIGEST_DOMAIN, value)
}

fn compute_delivery_digest(
    value: &OutboxDeliveryDigestWire<'_>,
) -> Result<Digest, OutboxDeliveryIntegrityError> {
    delivery_digest(DELIVERY_DIGEST_DOMAIN, value)
}

fn delivery_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, OutboxDeliveryIntegrityError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| OutboxDeliveryIntegrityError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn compute_attempt_start_digest(
    value: &OutboxAttemptStartDigestWire<'_>,
) -> Result<Digest, OutboxAttemptIntegrityError> {
    attempt_digest(ATTEMPT_START_DIGEST_DOMAIN, value)
}

fn compute_attempt_completion_digest(
    value: &OutboxAttemptCompletionDigestWire<'_>,
) -> Result<Digest, OutboxAttemptIntegrityError> {
    attempt_digest(ATTEMPT_COMPLETION_DIGEST_DOMAIN, value)
}

fn attempt_digest<T: Serialize>(
    domain: &[u8],
    value: &T,
) -> Result<Digest, OutboxAttemptIntegrityError> {
    let canonical = serde_json_canonicalizer::to_vec(value)
        .map_err(|_| OutboxAttemptIntegrityError::CanonicalSerialization)?;
    let mut preimage = Vec::with_capacity(domain.len() + canonical.len());
    preimage.extend_from_slice(domain);
    preimage.extend_from_slice(&canonical);
    Ok(Digest::sha256(preimage))
}

fn validate_delivery_shape(
    intent: &OutboxDeliveryIntent,
    origin: &JournalHead,
) -> Result<(), OutboxDeliveryError> {
    validate_delivery_head_scope(&intent.tenant_id, intent.run_id, origin, intent.expires_at)?;
    if origin.event_id() != intent.origin_event_id {
        return Err(OutboxDeliveryError::OriginEventMismatch);
    }
    Ok(())
}

fn validate_delivery_head_scope(
    tenant_id: &TenantId,
    run_id: RunId,
    origin: &JournalHead,
    expires_at: Timestamp,
) -> Result<(), OutboxDeliveryError> {
    if origin.tenant_id() != tenant_id {
        return Err(OutboxDeliveryError::OriginTenantMismatch);
    }
    if origin.run_id() != run_id {
        return Err(OutboxDeliveryError::OriginRunMismatch);
    }
    if expires_at <= origin.recorded_at() {
        return Err(OutboxDeliveryError::DeadlineNotAfterOrigin);
    }
    Ok(())
}

fn validate_attempt_start(
    delivery: &OutboxDeliveryHead,
    fence: &DeliveryFence,
    started_at: Timestamp,
    expires_at: Timestamp,
) -> Result<(), OutboxAttemptError> {
    if fence.tenant_id != delivery.tenant_id {
        return Err(OutboxAttemptError::FenceTenantMismatch);
    }
    if fence.run_id != delivery.run_id {
        return Err(OutboxAttemptError::FenceRunMismatch);
    }
    if fence.delivery_id != delivery.delivery_id {
        return Err(OutboxAttemptError::FenceDeliveryMismatch);
    }
    if started_at < delivery.origin.recorded_at() {
        return Err(OutboxAttemptError::StartBeforeOrigin);
    }
    if started_at >= delivery.expires_at {
        return Err(OutboxAttemptError::StartOutsideDeliveryWindow);
    }
    if expires_at <= started_at {
        return Err(OutboxAttemptError::ExpiryNotAfterStart);
    }
    if expires_at > delivery.expires_at {
        return Err(OutboxAttemptError::ExpiryAfterDeliveryDeadline);
    }
    let duration_micros =
        i128::from(expires_at.unix_micros()) - i128::from(started_at.unix_micros());
    let maximum_micros = i128::from(MAX_OUTBOX_ATTEMPT_LEASE_MILLIS) * 1_000;
    if duration_micros > maximum_micros {
        let actual_millis = i64::try_from((duration_micros + 999) / 1_000).unwrap_or(i64::MAX);
        return Err(OutboxAttemptError::LeaseTooLong {
            maximum_millis: MAX_OUTBOX_ATTEMPT_LEASE_MILLIS,
            actual_millis,
        });
    }
    Ok(())
}

fn validate_attempt_completion(
    start: &OutboxAttemptStartHead,
    outcome: &OutboxAttemptOutcome,
    completed_at: Timestamp,
) -> Result<(), OutboxAttemptError> {
    if completed_at < start.started_at {
        return Err(OutboxAttemptError::CompletionBeforeStart);
    }
    if completed_at >= start.expires_at {
        return Err(OutboxAttemptError::CompletionOutsideFence);
    }
    if let OutboxAttemptOutcome::Failed { failure } = outcome {
        match failure.retry_advice() {
            RetryAdvice::ReconcileFirst => {
                return Err(OutboxAttemptError::ReconciliationUnsupported);
            }
            RetryAdvice::SafeAfter { delay } => {
                retry_eligible_at(completed_at, delay.as_i64())?;
            }
            RetryAdvice::Never => {}
        }
    }
    Ok(())
}

fn validate_attempt_successor(
    previous: &OutboxAttempt,
    next: &OutboxAttempt,
) -> Result<(), OutboxAttemptHistoryError> {
    if next.start.fence.epoch
        != previous
            .start
            .fence
            .epoch
            .checked_next()
            .ok_or(OutboxAttemptHistoryError::AttemptLimitExceeded)?
    {
        return Err(OutboxAttemptHistoryError::EpochNotSuccessor);
    }

    let anchor = previous
        .completion
        .as_ref()
        .map_or(previous.start.expires_at, |completion| {
            completion.completed_at
        });
    if next.start.started_at < anchor {
        return Err(if previous.completion.is_none() {
            OutboxAttemptHistoryError::LiveAttemptSuperseded
        } else {
            OutboxAttemptHistoryError::ClockRegression
        });
    }

    let Some(completion) = previous.completion.as_ref() else {
        return Ok(());
    };
    match completion.outcome() {
        OutboxAttemptOutcome::Acknowledged { .. } => {
            Err(OutboxAttemptHistoryError::PreviousAttemptAcknowledged)
        }
        OutboxAttemptOutcome::Failed { failure } => match failure.retry_advice() {
            RetryAdvice::SafeAfter { delay } => {
                let eligible_at = retry_eligible_at(completion.completed_at, delay.as_i64())
                    .map_err(|_| OutboxAttemptHistoryError::RetryDelayOutOfRange)?;
                if next.start.started_at < eligible_at {
                    Err(OutboxAttemptHistoryError::RetryDelayNotElapsed {
                        eligible_at,
                        started_at: next.start.started_at,
                    })
                } else {
                    Ok(())
                }
            }
            RetryAdvice::Never | RetryAdvice::ReconcileFirst => {
                Err(OutboxAttemptHistoryError::RetryNotAuthorized)
            }
        },
    }
}

fn retry_eligible_at(
    completed_at: Timestamp,
    delay_millis: i64,
) -> Result<Timestamp, OutboxAttemptError> {
    let eligible_micros = i128::from(completed_at.unix_micros()) + i128::from(delay_millis) * 1_000;
    if eligible_micros > i128::from(Timestamp::MAX.unix_micros()) {
        return Err(OutboxAttemptError::RetryDelayOutOfRange);
    }
    Timestamp::from_unix_micros(
        i64::try_from(eligible_micros).map_err(|_| OutboxAttemptError::RetryDelayOutOfRange)?,
    )
    .map_err(|_| OutboxAttemptError::RetryDelayOutOfRange)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BoundedJson, DurationMillis, FailureCategory, FailureCode, FailureId, FailureMessage,
        FailureOrigin, JournalEventKind, JournalSequence, SchemaId, SchemaReference, Version,
    };
    use proptest::prelude::*;
    use serde_json::{Value, from_value, json, to_value};

    const BASE_MICROS: i64 = 1_893_456_000_000_000;

    fn id<T: std::str::FromStr>(suffix: u8) -> T
    where
        T::Err: fmt::Debug,
    {
        format!("01912345-6789-7abc-8def-0123456789{suffix:02x}")
            .parse()
            .unwrap()
    }

    fn timestamp(offset_micros: i64) -> Timestamp {
        Timestamp::from_unix_micros(BASE_MICROS + offset_micros).unwrap()
    }

    fn payload() -> JournalPayload {
        JournalPayload::new(
            SchemaReference::new(
                "https://stateknot.github.io/schema/outbox-test/1.0.0"
                    .parse::<SchemaId>()
                    .unwrap(),
                Version::new(1, 0, 0),
                Digest::sha256(b"outbox-test-schema"),
            ),
            JournalEventKind::new("a2a-task-update").unwrap(),
            BoundedJson::try_from_value(json!({
                "task_id": "task-42",
                "state": "completed"
            }))
            .unwrap(),
        )
        .unwrap()
    }

    fn delivery_with_deadline(deadline_offset_micros: i64) -> OutboxDelivery {
        let tenant_id = TenantId::new("tenant-a").unwrap();
        let run_id = id::<RunId>(0x10);
        let origin_event_id = id::<EventId>(0x11);
        let destination = OutboxDestinationRef::new(
            tenant_id.clone(),
            id::<DestinationId>(0x12),
            Digest::sha256(b"destination-snapshot"),
        );
        let intent = OutboxDeliveryIntent::new(
            tenant_id.clone(),
            run_id,
            id::<DeliveryId>(0x13),
            origin_event_id,
            destination,
            payload(),
            timestamp(deadline_offset_micros),
        )
        .unwrap();
        let origin = JournalHead::new(
            tenant_id,
            run_id,
            JournalSequence::FIRST,
            origin_event_id,
            timestamp(0),
            Digest::sha256(b"origin-event"),
        );
        OutboxDelivery::commit(intent, origin).unwrap()
    }

    fn delivery() -> OutboxDelivery {
        delivery_with_deadline(86_400_000_000)
    }

    fn start(
        delivery: &OutboxDelivery,
        suffix: u8,
        epoch: u64,
        started_offset_micros: i64,
        lease_micros: i64,
    ) -> OutboxAttemptStart {
        OutboxAttemptStart::new(
            delivery,
            DeliveryFence::new(
                delivery.intent().tenant_id().clone(),
                delivery.intent().run_id(),
                delivery.intent().delivery_id(),
                id::<AttemptId>(suffix),
                FencingEpoch::new(epoch).unwrap(),
            ),
            timestamp(started_offset_micros),
            timestamp(started_offset_micros + lease_micros),
        )
        .unwrap()
    }

    fn failure(suffix: u8, retry_advice: RetryAdvice) -> Failure {
        let category = if matches!(retry_advice, RetryAdvice::ReconcileFirst) {
            FailureCategory::AmbiguousExternalOutcome
        } else {
            FailureCategory::Internal
        };
        Failure::new(
            id::<FailureId>(suffix),
            category,
            FailureCode::new("outbox.delivery_failed").unwrap(),
            FailureOrigin::new("stateknot.outbox.test").unwrap(),
            FailureMessage::new("Delivery failed safely").unwrap(),
            retry_advice,
        )
        .unwrap()
    }

    #[test]
    fn delivery_binds_origin_destination_payload_deadline_and_every_digest() {
        let delivery = delivery();
        let encoded = to_value(&delivery).unwrap();
        assert_eq!(
            from_value::<OutboxDelivery>(encoded.clone()).unwrap(),
            delivery
        );
        assert_eq!(
            from_value::<OutboxDeliveryHead>(to_value(delivery.head()).unwrap()).unwrap(),
            delivery.head()
        );

        for path in ["delivery_id", "expires_at"] {
            let mut changed = to_value(delivery.head()).unwrap();
            changed[path] = if path == "delivery_id" {
                json!(id::<DeliveryId>(0x14))
            } else {
                json!(timestamp(86_399_000_000))
            };
            assert!(from_value::<OutboxDeliveryHead>(changed).is_err());
        }

        let mut changed_payload = encoded;
        changed_payload["intent"]["payload"]["data"]["state"] = json!("failed");
        assert!(from_value::<OutboxDelivery>(changed_payload).is_err());
    }

    #[test]
    fn delivery_rejects_crossed_scope_substituted_origin_and_closed_deadline() {
        let delivery = delivery();
        let intent = delivery.intent().clone();
        let origin = delivery.origin().clone();

        let crossed = JournalHead::new(
            TenantId::new("tenant-b").unwrap(),
            origin.run_id(),
            origin.sequence(),
            origin.event_id(),
            origin.recorded_at(),
            origin.digest(),
        );
        assert_eq!(
            OutboxDelivery::commit(intent.clone(), crossed),
            Err(OutboxDeliveryError::OriginTenantMismatch)
        );

        let substituted = JournalHead::new(
            origin.tenant_id().clone(),
            origin.run_id(),
            origin.sequence(),
            id::<EventId>(0x15),
            origin.recorded_at(),
            origin.digest(),
        );
        assert_eq!(
            OutboxDelivery::commit(intent, substituted),
            Err(OutboxDeliveryError::OriginEventMismatch)
        );

        let deadline = delivery_with_deadline(1);
        let mut changed = to_value(deadline).unwrap();
        changed["intent"]["expires_at"] = json!(timestamp(0));
        assert!(from_value::<OutboxDelivery>(changed).is_err());
    }

    #[test]
    fn attempt_lease_and_completion_enforce_exclusive_time_boundaries() {
        let delivery = delivery_with_deadline(600_000_000);
        let live = start(&delivery, 0x20, 1, 1_000_000, 10_000_000);
        assert!(
            OutboxAttemptCompletion::acknowledge(
                &live,
                Some(Digest::sha256(b"ack")),
                timestamp(10_999_999),
            )
            .is_ok()
        );
        assert!(matches!(
            OutboxAttemptCompletion::acknowledge(&live, None, timestamp(11_000_000)),
            Err(OutboxAttemptError::CompletionOutsideFence)
        ));

        let too_long = OutboxAttemptStart::new(
            &delivery,
            DeliveryFence::new(
                delivery.intent().tenant_id().clone(),
                delivery.intent().run_id(),
                delivery.intent().delivery_id(),
                id::<AttemptId>(0x21),
                FencingEpoch::new(2).unwrap(),
            ),
            timestamp(1_000_000),
            timestamp(1_000_000 + (MAX_OUTBOX_ATTEMPT_LEASE_MILLIS + 1) * 1_000),
        );
        assert!(matches!(
            too_long,
            Err(OutboxAttemptError::LeaseTooLong { .. })
        ));
    }

    #[test]
    fn outbox_rejects_reconcile_first_because_duplicates_are_the_contract() {
        let delivery = delivery();
        let start = start(&delivery, 0x22, 1, 1_000_000, 10_000_000);
        assert!(matches!(
            OutboxAttemptCompletion::fail(
                &start,
                failure(0x23, RetryAdvice::ReconcileFirst),
                timestamp(2_000_000),
            ),
            Err(OutboxAttemptError::ReconciliationUnsupported)
        ));
    }

    #[test]
    fn lost_ack_recovery_waits_for_fixed_expiry_and_advances_exact_epoch() {
        let delivery = delivery();
        let first = OutboxAttempt::delivering(start(&delivery, 0x30, 1, 1_000_000, 5_000_000));
        let early = OutboxAttempt::delivering(start(&delivery, 0x31, 2, 5_999_999, 5_000_000));
        let recovered = OutboxAttempt::delivering(start(&delivery, 0x31, 2, 6_000_000, 5_000_000));

        let mut verifier = OutboxAttemptHistoryVerifier::new(&delivery);
        verifier.verify_next(&first).unwrap();
        assert_eq!(
            verifier.verify_next(&early),
            Err(OutboxAttemptHistoryError::LiveAttemptSuperseded)
        );
        assert_eq!(verifier.count(), 1);
        verifier.verify_next(&recovered).unwrap();
        assert_eq!(verifier.count(), 2);
    }

    #[test]
    fn safe_after_retry_and_status_projection_use_durable_database_time() {
        let delivery = delivery();
        let first_start = start(&delivery, 0x40, 1, 1_000_000, 10_000_000);
        let completion = OutboxAttemptCompletion::fail(
            &first_start,
            failure(
                0x41,
                RetryAdvice::SafeAfter {
                    delay: DurationMillis::new(2_500).unwrap(),
                },
            ),
            timestamp(2_000_000),
        )
        .unwrap();
        let first = OutboxAttempt::restore(first_start, Some(completion)).unwrap();

        let mut verifier = OutboxAttemptHistoryVerifier::new(&delivery);
        verifier.verify_next(&first).unwrap();
        assert_eq!(
            verifier.status_at(timestamp(4_499_999)).unwrap(),
            OutboxDeliveryStatus::RetryScheduled
        );
        assert_eq!(
            verifier.status_at(timestamp(4_500_000)).unwrap(),
            OutboxDeliveryStatus::Pending
        );

        let early = OutboxAttempt::delivering(start(&delivery, 0x42, 2, 4_499_999, 1_000_000));
        assert!(matches!(
            verifier.verify_next(&early),
            Err(OutboxAttemptHistoryError::RetryDelayNotElapsed { .. })
        ));
        let retry = OutboxAttempt::delivering(start(&delivery, 0x42, 2, 4_500_000, 1_000_000));
        verifier.verify_next(&retry).unwrap();
    }

    #[test]
    fn acknowledgement_and_never_failures_are_absorbing_terminal_states() {
        let delivery = delivery();
        let acknowledged_start = start(&delivery, 0x50, 1, 1_000_000, 10_000_000);
        let acknowledged = OutboxAttempt::restore(
            acknowledged_start.clone(),
            Some(
                OutboxAttemptCompletion::acknowledge(
                    &acknowledged_start,
                    None,
                    timestamp(2_000_000),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let successor = OutboxAttempt::delivering(start(&delivery, 0x51, 2, 3_000_000, 1_000_000));
        let mut verifier = OutboxAttemptHistoryVerifier::new(&delivery);
        verifier.verify_next(&acknowledged).unwrap();
        assert_eq!(
            verifier.status_at(timestamp(3_000_000)).unwrap(),
            OutboxDeliveryStatus::Acknowledged
        );
        assert_eq!(
            verifier.verify_next(&successor),
            Err(OutboxAttemptHistoryError::PreviousAttemptAcknowledged)
        );

        let failed_start = start(&delivery, 0x52, 1, 1_000_000, 10_000_000);
        let failed = OutboxAttempt::restore(
            failed_start.clone(),
            Some(
                OutboxAttemptCompletion::fail(
                    &failed_start,
                    failure(0x53, RetryAdvice::Never),
                    timestamp(2_000_000),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let mut verifier = OutboxAttemptHistoryVerifier::new(&delivery);
        verifier.verify_next(&failed).unwrap();
        assert_eq!(
            verifier.status_at(timestamp(3_000_000)).unwrap(),
            OutboxDeliveryStatus::DeadLetter
        );
        assert_eq!(
            verifier.verify_next(&successor),
            Err(OutboxAttemptHistoryError::RetryNotAuthorized)
        );
    }

    #[test]
    fn attempt_history_is_hard_bounded_and_rejections_do_not_advance_state() {
        let delivery = delivery();
        let mut verifier = OutboxAttemptHistoryVerifier::new(&delivery);
        for index in 0..MAX_OUTBOX_ATTEMPTS {
            let started_offset = 1_000_000 + i64::try_from(index).unwrap() * 2_000;
            let attempt_start = start(
                &delivery,
                u8::try_from(0x80 + index).unwrap(),
                u64::try_from(index + 1).unwrap(),
                started_offset,
                1_000,
            );
            let completion = OutboxAttemptCompletion::fail(
                &attempt_start,
                failure(
                    u8::try_from(0x40 + index).unwrap(),
                    RetryAdvice::SafeAfter {
                        delay: DurationMillis::ZERO,
                    },
                ),
                timestamp(started_offset + 500),
            )
            .unwrap();
            verifier
                .verify_next(&OutboxAttempt::restore(attempt_start, Some(completion)).unwrap())
                .unwrap();
        }
        assert_eq!(verifier.count(), MAX_OUTBOX_ATTEMPTS);
        assert_eq!(
            verifier.status_at(timestamp(2_000_000)).unwrap(),
            OutboxDeliveryStatus::DeadLetter
        );
        assert_eq!(
            verifier.status_at(timestamp(90_000_000_000)).unwrap(),
            OutboxDeliveryStatus::DeadLetter,
            "attempt exhaustion is an absorbing terminal state"
        );
        let extra = OutboxAttempt::delivering(start(
            &delivery,
            0x7f,
            u64::try_from(MAX_OUTBOX_ATTEMPTS + 1).unwrap(),
            2_000_000,
            1_000,
        ));
        assert_eq!(
            verifier.verify_next(&extra),
            Err(OutboxAttemptHistoryError::AttemptLimitExceeded)
        );
        assert_eq!(verifier.count(), MAX_OUTBOX_ATTEMPTS);
    }

    #[test]
    fn all_integrity_bearing_wires_are_closed_and_revalidated() {
        let delivery = delivery();
        let start = start(&delivery, 0x60, 1, 1_000_000, 10_000_000);
        let completion = OutboxAttemptCompletion::acknowledge(
            &start,
            Some(Digest::sha256(b"ack")),
            timestamp(2_000_000),
        )
        .unwrap();
        let attempt = OutboxAttempt::restore(start.clone(), Some(completion.clone())).unwrap();

        macro_rules! rejects_extra {
            ($value:expr, $type:ty) => {{
                let mut extra = to_value($value).unwrap();
                extra["unsafe_extension"] = Value::Bool(true);
                assert!(from_value::<$type>(extra).is_err());
            }};
        }
        rejects_extra!(delivery.intent(), OutboxDeliveryIntent);
        rejects_extra!(&delivery, OutboxDelivery);
        rejects_extra!(delivery.head(), OutboxDeliveryHead);
        rejects_extra!(&start, OutboxAttemptStart);
        rejects_extra!(start.head(), OutboxAttemptStartHead);
        rejects_extra!(&completion, OutboxAttemptCompletion);
        rejects_extra!(&attempt, OutboxAttempt);

        let mut changed = to_value(completion).unwrap();
        changed["outcome"]["evidence_digest"] = json!(Digest::sha256(b"changed"));
        assert!(from_value::<OutboxAttemptCompletion>(changed).is_err());
    }

    proptest! {
        #[test]
        fn safe_after_projection_changes_only_at_the_exact_boundary(
            delay_millis in 1_i64..=86_000_000_i64,
        ) {
            let delivery = delivery_with_deadline(86_400_000_000);
            let attempt_start = start(&delivery, 0x70, 1, 1_000_000, 10_000_000);
            let completed_at = timestamp(2_000_000);
            let completion = OutboxAttemptCompletion::fail(
                &attempt_start,
                failure(
                    0x71,
                    RetryAdvice::SafeAfter {
                        delay: DurationMillis::new(delay_millis).unwrap(),
                    },
                ),
                completed_at,
            )
            .unwrap();
            let mut verifier = OutboxAttemptHistoryVerifier::new(&delivery);
            verifier
                .verify_next(&OutboxAttempt::restore(attempt_start, Some(completion)).unwrap())
                .unwrap();
            let eligible_at = timestamp(2_000_000 + delay_millis * 1_000);
            let before = Timestamp::from_unix_micros(eligible_at.unix_micros() - 1).unwrap();

            prop_assert_eq!(
                verifier.status_at(before).unwrap(),
                OutboxDeliveryStatus::RetryScheduled
            );
            prop_assert_eq!(
                verifier.status_at(eligible_at).unwrap(),
                OutboxDeliveryStatus::Pending
            );
        }

        #[test]
        fn fixed_attempt_lease_acceptance_matches_the_published_bound(
            lease_millis in 1_i64..=MAX_OUTBOX_ATTEMPT_LEASE_MILLIS + 1,
        ) {
            let delivery = delivery_with_deadline(600_000_000);
            let result = OutboxAttemptStart::new(
                &delivery,
                DeliveryFence::new(
                    delivery.intent().tenant_id().clone(),
                    delivery.intent().run_id(),
                    delivery.intent().delivery_id(),
                    id::<AttemptId>(0x72),
                    FencingEpoch::FIRST,
                ),
                timestamp(1_000_000),
                timestamp(1_000_000 + lease_millis * 1_000),
            );
            prop_assert_eq!(result.is_ok(), lease_millis <= MAX_OUTBOX_ATTEMPT_LEASE_MILLIS);
        }
    }
}
