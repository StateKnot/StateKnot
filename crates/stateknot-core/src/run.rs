// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable, protocol-neutral lifecycle contracts for one admitted agent run.
//!
//! This module models business progress only. Worker attempts, leases, fencing
//! epochs, journal sequence numbers, and transport-specific task states remain
//! separate runtime concerns. That separation lets a process crash or a lease
//! change without inventing a business-state transition.

use std::fmt;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;

use crate::{
    AgentResult, AgentResultProvenance, BudgetUsage, ExecutionCount, Failure, FailureCategory,
    InterruptId, RetryAdvice, TimerId, Timestamp,
};

/// Monotonic optimistic revision of a run lifecycle snapshot.
///
/// The wire form is the same canonical unsigned decimal string used by other
/// exact counters, but the distinct Rust type prevents an accounting value
/// from being passed as a lifecycle revision accidentally.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Eq,
    Hash,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(transparent)]
pub struct RunRevision(ExecutionCount);

impl RunRevision {
    /// Initial revision assigned at successful run admission.
    pub const ZERO: Self = Self(ExecutionCount::ZERO);

    /// Largest representable revision.
    pub const MAX: Self = Self(ExecutionCount::MAX);

    /// Constructs a revision from its integer representation.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(ExecutionCount::new(value))
    }

    /// Returns the underlying integer revision.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(ExecutionCount::new(1)) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for RunRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable projection of a run's current business lifecycle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// Admission committed, but semantic execution has not started.
    Pending,
    /// The run is allowed to make progress; a worker need not currently hold it.
    Active,
    /// Progress is durably suspended on one or more explicit conditions.
    Waiting,
    /// Cancellation intent committed and blocks every non-cancellation outcome.
    CancellationRequested,
    /// A validated successful result committed.
    Succeeded,
    /// A non-cancellation terminal failure committed.
    Failed,
    /// The committed cancellation request reached its terminal acknowledgement.
    Cancelled,
}

impl RunStatus {
    /// Returns whether this state can never transition again.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

/// Semantic reason why a run needs an externally supplied resolution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunInterruptKind {
    /// A principal must authorize an already bound action.
    Approval,
    /// Additional caller data is required.
    Input,
    /// Authentication or delegated authorization is required.
    Authentication,
    /// A named external signal or callback must arrive.
    ExternalSignal,
    /// An ambiguous external effect must be reconciled before progress.
    Reconciliation,
}

/// Immutable lifecycle marker for one unresolved interrupt.
///
/// Request and resolution payloads, authorization requirements, action
/// digests, and resolver provenance belong to the durable interrupt record.
/// The runtime commits that record atomically with the lifecycle transition;
/// this compact marker prevents large or sensitive payloads from being copied
/// into every run snapshot.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunInterrupt {
    interrupt_id: InterruptId,
    kind: RunInterruptKind,
    requested_at: Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<Timestamp>,
}

impl RunInterrupt {
    /// Constructs an interrupt marker with an optional exclusive expiry.
    ///
    /// A resolution observed exactly at `expires_at` is expired.
    ///
    /// # Errors
    ///
    /// Returns [`RunInterruptError::ExpiryNotAfterRequest`] unless an expiry is
    /// strictly later than the request observation.
    pub fn new(
        interrupt_id: InterruptId,
        kind: RunInterruptKind,
        requested_at: Timestamp,
        expires_at: Option<Timestamp>,
    ) -> Result<Self, RunInterruptError> {
        if let Some(expires_at) = expires_at {
            if expires_at <= requested_at {
                return Err(RunInterruptError::ExpiryNotAfterRequest {
                    requested_at,
                    expires_at,
                });
            }
        }
        Ok(Self {
            interrupt_id,
            kind,
            requested_at,
            expires_at,
        })
    }

    /// Returns the tenant-scoped interrupt identifier.
    #[must_use]
    pub const fn interrupt_id(&self) -> InterruptId {
        self.interrupt_id
    }

    /// Returns the semantic interrupt kind.
    #[must_use]
    pub const fn kind(&self) -> RunInterruptKind {
        self.kind
    }

    /// Returns the durable clock observation at registration.
    #[must_use]
    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }

    /// Returns the exclusive resolution expiry, if configured.
    #[must_use]
    pub const fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }
}

impl fmt::Debug for RunInterrupt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunInterrupt")
            .field("interrupt_id", &self.interrupt_id)
            .field("kind", &self.kind)
            .field("requested_at", &self.requested_at)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for RunInterrupt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            interrupt_id: InterruptId,
            kind: RunInterruptKind,
            requested_at: Timestamp,
            expires_at: Option<Timestamp>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(
            wire.interrupt_id,
            wire.kind,
            wire.requested_at,
            wire.expires_at,
        )
        .map_err(de::Error::custom)
    }
}

/// Invalid intrinsic timing for a run interrupt.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RunInterruptError {
    /// An exclusive expiry was not later than registration.
    #[error("interrupt expiry {expires_at} must be later than request {requested_at}")]
    ExpiryNotAfterRequest {
        /// Durable request observation.
        requested_at: Timestamp,
        /// Rejected exclusive expiry.
        expires_at: Timestamp,
    },
}

/// Semantic purpose of one durable run timer.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTimerKind {
    /// A graph or agent deliberately sleeps until a scheduled instant.
    Sleep,
    /// Retry policy defers another attempt until a backoff instant.
    RetryBackoff,
}

/// Immutable marker for one unresolved durable timer.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunTimer {
    timer_id: TimerId,
    kind: RunTimerKind,
    scheduled_at: Timestamp,
    due_at: Timestamp,
}

impl RunTimer {
    /// Constructs a timer whose due instant is strictly after registration.
    ///
    /// # Errors
    ///
    /// Returns [`RunTimerError::DueNotAfterSchedule`] when the timer could fire
    /// at or before its registration observation.
    pub fn new(
        timer_id: TimerId,
        kind: RunTimerKind,
        scheduled_at: Timestamp,
        due_at: Timestamp,
    ) -> Result<Self, RunTimerError> {
        if due_at <= scheduled_at {
            return Err(RunTimerError::DueNotAfterSchedule {
                scheduled_at,
                due_at,
            });
        }
        Ok(Self {
            timer_id,
            kind,
            scheduled_at,
            due_at,
        })
    }

    /// Returns the tenant-scoped timer identifier.
    #[must_use]
    pub const fn timer_id(&self) -> TimerId {
        self.timer_id
    }

    /// Returns the semantic timer purpose.
    #[must_use]
    pub const fn kind(&self) -> RunTimerKind {
        self.kind
    }

    /// Returns the durable clock observation at registration.
    #[must_use]
    pub const fn scheduled_at(&self) -> Timestamp {
        self.scheduled_at
    }

    /// Returns the inclusive earliest firing instant.
    #[must_use]
    pub const fn due_at(&self) -> Timestamp {
        self.due_at
    }
}

impl fmt::Debug for RunTimer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunTimer")
            .field("timer_id", &self.timer_id)
            .field("kind", &self.kind)
            .field("scheduled_at", &self.scheduled_at)
            .field("due_at", &self.due_at)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for RunTimer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            timer_id: TimerId,
            kind: RunTimerKind,
            scheduled_at: Timestamp,
            due_at: Timestamp,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.timer_id, wire.kind, wire.scheduled_at, wire.due_at)
            .map_err(de::Error::custom)
    }
}

/// Invalid intrinsic timing for a durable run timer.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RunTimerError {
    /// The due instant was not later than registration.
    #[error("timer due instant {due_at} must be later than schedule {scheduled_at}")]
    DueNotAfterSchedule {
        /// Durable registration observation.
        scheduled_at: Timestamp,
        /// Rejected inclusive due instant.
        due_at: Timestamp,
    },
}

/// One explicit condition suspending semantic run progress.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunWait {
    /// Wait for an authenticated and authorized interrupt resolution record.
    Interrupt {
        /// Immutable unresolved interrupt marker.
        interrupt: RunInterrupt,
    },
    /// Wait for a durable scheduler observation at or after a due instant.
    Timer {
        /// Immutable unresolved timer marker.
        timer: RunTimer,
    },
}

impl RunWait {
    /// Constructs an interrupt wait.
    #[must_use]
    pub const fn interrupt(interrupt: RunInterrupt) -> Self {
        Self::Interrupt { interrupt }
    }

    /// Constructs a timer wait.
    #[must_use]
    pub const fn timer(timer: RunTimer) -> Self {
        Self::Timer { timer }
    }

    /// Returns the interrupt marker when this is an interrupt wait.
    #[must_use]
    pub const fn as_interrupt(&self) -> Option<&RunInterrupt> {
        match self {
            Self::Interrupt { interrupt } => Some(interrupt),
            Self::Timer { .. } => None,
        }
    }

    /// Returns the timer marker when this is a timer wait.
    #[must_use]
    pub const fn as_timer(&self) -> Option<&RunTimer> {
        match self {
            Self::Timer { timer } => Some(timer),
            Self::Interrupt { .. } => None,
        }
    }

    const fn registered_at(&self) -> Timestamp {
        match self {
            Self::Interrupt { interrupt } => interrupt.requested_at(),
            Self::Timer { timer } => timer.scheduled_at(),
        }
    }

    fn identity_uuid(&self) -> uuid::Uuid {
        match self {
            Self::Interrupt { interrupt } => interrupt.interrupt_id().into_uuid(),
            Self::Timer { timer } => timer.timer_id().into_uuid(),
        }
    }
}

/// Non-empty, bounded batch of conditions registered by one state transition.
///
/// Parallel graph branches may suspend on several conditions at the same
/// barrier. Their markers retain stable semantic order, share one registration
/// observation, and have globally unique UUID identities across interrupt and
/// timer variants.
#[derive(Clone, Eq, PartialEq)]
pub struct RunWaits {
    values: Box<[RunWait]>,
    registered_at: Timestamp,
}

impl RunWaits {
    /// Maximum simultaneous outstanding conditions for one run.
    pub const MAX_LEN: usize = 64;

    /// Validates a non-empty batch of wait conditions.
    ///
    /// # Errors
    ///
    /// Returns [`RunWaitsError`] for an empty or oversized batch, duplicate
    /// identities, or conditions observed at different registration instants.
    pub fn try_new<I>(values: I) -> Result<Self, RunWaitsError>
    where
        I: IntoIterator<Item = RunWait>,
    {
        let mut collected = Vec::new();
        let mut registered_at = None;
        for value in values {
            if collected.len() == Self::MAX_LEN {
                return Err(RunWaitsError::TooMany {
                    maximum: Self::MAX_LEN,
                    actual: Self::MAX_LEN + 1,
                });
            }
            let value_registered_at = value.registered_at();
            if let Some(expected) = registered_at {
                if value_registered_at != expected {
                    return Err(RunWaitsError::MixedRegistrationTimes {
                        expected,
                        actual: value_registered_at,
                    });
                }
            } else {
                registered_at = Some(value_registered_at);
            }
            if collected
                .iter()
                .any(|existing: &RunWait| existing.identity_uuid() == value.identity_uuid())
            {
                return Err(RunWaitsError::DuplicateIdentity);
            }
            collected.push(value);
        }

        let registered_at = registered_at.ok_or(RunWaitsError::Empty)?;
        Ok(Self {
            values: collected.into_boxed_slice(),
            registered_at,
        })
    }

    /// Returns the number of outstanding conditions.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether no conditions are present.
    ///
    /// This is always `false` for a valid value; the method is provided for
    /// ordinary collection ergonomics and future-proof generic callers.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the common registration observation.
    #[must_use]
    pub const fn registered_at(&self) -> Timestamp {
        self.registered_at
    }

    /// Iterates conditions in deterministic semantic order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &RunWait> {
        self.values.iter()
    }

    /// Looks up an outstanding interrupt by exact identifier.
    #[must_use]
    pub fn interrupt(&self, interrupt_id: InterruptId) -> Option<&RunInterrupt> {
        self.values
            .iter()
            .filter_map(RunWait::as_interrupt)
            .find(|interrupt| interrupt.interrupt_id() == interrupt_id)
    }

    /// Looks up an outstanding timer by exact identifier.
    #[must_use]
    pub fn timer(&self, timer_id: TimerId) -> Option<&RunTimer> {
        self.values
            .iter()
            .filter_map(RunWait::as_timer)
            .find(|timer| timer.timer_id() == timer_id)
    }

    /// Consumes the batch into stable ordered conditions.
    #[must_use]
    pub fn into_vec(self) -> Vec<RunWait> {
        self.values.into_vec()
    }

    fn without_interrupt(self, interrupt_id: InterruptId) -> Option<Self> {
        let values = self
            .values
            .into_vec()
            .into_iter()
            .filter(|wait| {
                wait.as_interrupt()
                    .is_none_or(|interrupt| interrupt.interrupt_id() != interrupt_id)
            })
            .collect::<Vec<_>>();
        Self::from_remaining(values, self.registered_at)
    }

    fn without_timer(self, timer_id: TimerId) -> Option<Self> {
        let values = self
            .values
            .into_vec()
            .into_iter()
            .filter(|wait| {
                wait.as_timer()
                    .is_none_or(|timer| timer.timer_id() != timer_id)
            })
            .collect::<Vec<_>>();
        Self::from_remaining(values, self.registered_at)
    }

    fn from_remaining(values: Vec<RunWait>, registered_at: Timestamp) -> Option<Self> {
        if values.is_empty() {
            None
        } else {
            Some(Self {
                values: values.into_boxed_slice(),
                registered_at,
            })
        }
    }
}

impl fmt::Debug for RunWaits {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunWaits")
            .field("count", &self.len())
            .field("registered_at", &self.registered_at)
            .finish_non_exhaustive()
    }
}

impl Serialize for RunWaits {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RunWaits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(RunWaitsVisitor)
    }
}

struct RunWaitsVisitor;

impl<'de> de::Visitor<'de> for RunWaitsVisitor {
    type Value = RunWaits;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "one to {} unique run wait conditions registered together",
            RunWaits::MAX_LEN
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
                .min(RunWaits::MAX_LEN),
        );
        while let Some(value) = sequence.next_element::<RunWait>()? {
            if values.len() == RunWaits::MAX_LEN {
                return Err(de::Error::custom(RunWaitsError::TooMany {
                    maximum: RunWaits::MAX_LEN,
                    actual: RunWaits::MAX_LEN + 1,
                }));
            }
            if values
                .iter()
                .any(|existing: &RunWait| existing.identity_uuid() == value.identity_uuid())
            {
                return Err(de::Error::custom(RunWaitsError::DuplicateIdentity));
            }
            values.push(value);
        }
        RunWaits::try_new(values).map_err(de::Error::custom)
    }
}

impl JsonSchema for RunWaits {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "RunWaits".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::RunWaits").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<RunWait>(),
            "minItems": 1,
            "maxItems": 64,
            "uniqueItems": true,
            "description": "Stable ordered outstanding conditions. UUID identity uniqueness and the common registration timestamp are enforced at runtime."
        })
    }
}

/// Invalid batch of run wait conditions.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RunWaitsError {
    /// A waiting transition supplied no condition.
    #[error("a waiting run must have at least one unresolved condition")]
    Empty,
    /// The hard simultaneous-condition ceiling was exceeded.
    #[error("run has {actual} wait conditions; hard maximum is {maximum}")]
    TooMany {
        /// Absolute simultaneous-condition ceiling.
        maximum: usize,
        /// First observed count beyond the ceiling.
        actual: usize,
    },
    /// Two wait variants reused the same UUID identity.
    #[error("run wait condition identities must be globally unique")]
    DuplicateIdentity,
    /// One atomic wait registration contained multiple clock observations.
    #[error("wait registration time {actual} does not match batch time {expected}")]
    MixedRegistrationTimes {
        /// Registration observation established by the first condition.
        expected: Timestamp,
        /// Rejected later condition observation.
        actual: Timestamp,
    },
}

/// Non-cancellation terminal failure with complete cumulative usage.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunFailure {
    failure: Failure,
    completed_at: Timestamp,
    usage: BudgetUsage,
}

impl RunFailure {
    /// Constructs a non-cancellation terminal failure.
    ///
    /// # Errors
    ///
    /// Returns [`RunFailureError::CancellationRequiresCancellationPath`] when
    /// a cancellation-category failure tries to bypass the two-phase path.
    pub fn new(
        failure: Failure,
        completed_at: Timestamp,
        usage: BudgetUsage,
    ) -> Result<Self, RunFailureError> {
        if failure.category() == FailureCategory::Cancelled {
            return Err(RunFailureError::CancellationRequiresCancellationPath);
        }
        Ok(Self {
            failure,
            completed_at,
            usage,
        })
    }

    /// Returns the public terminal failure.
    #[must_use]
    pub const fn failure(&self) -> &Failure {
        &self.failure
    }

    /// Returns the durable clock observation at terminal commit.
    #[must_use]
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }

    /// Returns complete cumulative usage at terminal commit.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    /// Consumes the terminal record into its durable components.
    #[must_use]
    pub fn into_parts(self) -> (Failure, Timestamp, BudgetUsage) {
        (self.failure, self.completed_at, self.usage)
    }
}

impl fmt::Debug for RunFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunFailure")
            .field("failure", &self.failure)
            .field("completed_at", &self.completed_at)
            .field("usage_recorded", &true)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for RunFailure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            failure: Failure,
            completed_at: Timestamp,
            usage: BudgetUsage,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.failure, wire.completed_at, wire.usage).map_err(de::Error::custom)
    }
}

/// Invalid failure supplied to the ordinary failed terminal state.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RunFailureError {
    /// A cancellation failure attempted to skip cancellation intent.
    #[error(
        "cancelled failures must use request-cancellation and confirm-cancellation transitions"
    )]
    CancellationRequiresCancellationPath,
}

/// Immutable cancellation intent committed before cooperative cleanup.
///
/// Once this value enters the lifecycle, only terminal cancellation may
/// follow. The original failure occurrence is retained unchanged so a later
/// acknowledgement cannot substitute a different reason.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunCancellationRequest {
    failure: Failure,
    requested_at: Timestamp,
}

impl RunCancellationRequest {
    /// Constructs terminal-intent metadata from a non-retryable cancellation.
    ///
    /// # Errors
    ///
    /// Returns [`RunCancellationError`] unless the failure category is
    /// `Cancelled` and its explicit retry advice is `Never`.
    pub fn new(failure: Failure, requested_at: Timestamp) -> Result<Self, RunCancellationError> {
        if failure.category() != FailureCategory::Cancelled {
            return Err(RunCancellationError::CategoryNotCancelled {
                actual: failure.category(),
            });
        }
        if failure.retry_advice() != RetryAdvice::Never {
            return Err(RunCancellationError::RetryAdviceNotNever {
                actual: failure.retry_advice(),
            });
        }
        Ok(Self {
            failure,
            requested_at,
        })
    }

    /// Returns the immutable cancellation failure occurrence.
    #[must_use]
    pub const fn failure(&self) -> &Failure {
        &self.failure
    }

    /// Returns the durable clock observation when cancellation won admission.
    #[must_use]
    pub const fn requested_at(&self) -> Timestamp {
        self.requested_at
    }
}

impl fmt::Debug for RunCancellationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunCancellationRequest")
            .field("failure", &self.failure)
            .field("requested_at", &self.requested_at)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for RunCancellationRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            failure: Failure,
            requested_at: Timestamp,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.failure, wire.requested_at).map_err(de::Error::custom)
    }
}

/// Terminal acknowledgement of an earlier immutable cancellation request.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunCancellation {
    request: RunCancellationRequest,
    completed_at: Timestamp,
    usage: BudgetUsage,
}

impl RunCancellation {
    /// Constructs a terminal cancellation acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns [`RunCancellationError::CompletionBeforeRequest`] when the
    /// completion observation precedes committed cancellation intent.
    pub fn new(
        request: RunCancellationRequest,
        completed_at: Timestamp,
        usage: BudgetUsage,
    ) -> Result<Self, RunCancellationError> {
        if completed_at < request.requested_at() {
            return Err(RunCancellationError::CompletionBeforeRequest {
                requested_at: request.requested_at(),
                completed_at,
            });
        }
        Ok(Self {
            request,
            completed_at,
            usage,
        })
    }

    /// Returns the exact cancellation request that won the race.
    #[must_use]
    pub const fn request(&self) -> &RunCancellationRequest {
        &self.request
    }

    /// Returns the durable clock observation at terminal acknowledgement.
    #[must_use]
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }

    /// Returns complete cumulative usage at terminal acknowledgement.
    #[must_use]
    pub const fn usage(&self) -> &BudgetUsage {
        &self.usage
    }

    /// Consumes the terminal record into its durable components.
    #[must_use]
    pub fn into_parts(self) -> (RunCancellationRequest, Timestamp, BudgetUsage) {
        (self.request, self.completed_at, self.usage)
    }
}

impl fmt::Debug for RunCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunCancellation")
            .field("request", &self.request)
            .field("completed_at", &self.completed_at)
            .field("usage_recorded", &true)
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for RunCancellation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            request: RunCancellationRequest,
            completed_at: Timestamp,
            usage: BudgetUsage,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.request, wire.completed_at, wire.usage).map_err(de::Error::custom)
    }
}

/// Invalid cancellation intent or acknowledgement.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RunCancellationError {
    /// Cancellation intent used a non-cancellation failure category.
    #[error("cancellation request failure category is {actual:?}; expected cancelled")]
    CategoryNotCancelled {
        /// Rejected semantic category.
        actual: FailureCategory,
    },
    /// Cancellation intent suggested another attempt.
    #[error("cancellation request retry advice is {actual:?}; expected never")]
    RetryAdviceNotNever {
        /// Rejected recovery advice.
        actual: RetryAdvice,
    },
    /// Terminal acknowledgement preceded the request observation.
    #[error("cancellation completed at {completed_at} before request at {requested_at}")]
    CompletionBeforeRequest {
        /// Durable cancellation-request observation.
        requested_at: Timestamp,
        /// Rejected terminal observation.
        completed_at: Timestamp,
    },
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum RunState {
    Pending,
    Active {
        activated_at: Timestamp,
    },
    Waiting {
        waits: RunWaits,
        changed_at: Timestamp,
    },
    CancellationRequested {
        request: RunCancellationRequest,
    },
    Succeeded {
        result: AgentResult,
    },
    Failed {
        failure: RunFailure,
    },
    Cancelled {
        cancellation: RunCancellation,
    },
}

impl RunState {
    const fn status(&self) -> RunStatus {
        match self {
            Self::Pending => RunStatus::Pending,
            Self::Active { .. } => RunStatus::Active,
            Self::Waiting { .. } => RunStatus::Waiting,
            Self::CancellationRequested { .. } => RunStatus::CancellationRequested,
            Self::Succeeded { .. } => RunStatus::Succeeded,
            Self::Failed { .. } => RunStatus::Failed,
            Self::Cancelled { .. } => RunStatus::Cancelled,
        }
    }

    const fn changed_at(&self, admitted_at: Timestamp) -> Timestamp {
        match self {
            Self::Pending => admitted_at,
            Self::Active { activated_at } => *activated_at,
            Self::Waiting { changed_at, .. } => *changed_at,
            Self::CancellationRequested { request } => request.requested_at(),
            Self::Succeeded { result } => result.completed_at(),
            Self::Failed { failure } => failure.completed_at(),
            Self::Cancelled { cancellation } => cancellation.completed_at(),
        }
    }
}

/// Immutable snapshot of one admitted run's protocol-neutral business state.
///
/// Use [`Self::apply`] to obtain the next snapshot. The operation consumes the
/// old value, checks the closed transition graph and timestamps, and increments
/// the optimistic revision exactly once. Durable runtimes still perform the
/// transition and journal append atomically under an expected revision and a
/// valid fencing epoch.
#[derive(Clone, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunLifecycle {
    provenance: AgentResultProvenance,
    admitted_at: Timestamp,
    revision: RunRevision,
    state: RunState,
}

impl RunLifecycle {
    /// Creates the initial pending snapshot after admission has committed.
    #[must_use]
    pub const fn admitted(provenance: AgentResultProvenance, admitted_at: Timestamp) -> Self {
        Self {
            provenance,
            admitted_at,
            revision: RunRevision::ZERO,
            state: RunState::Pending,
        }
    }

    /// Returns trusted tenant, run, thread, invocation, and agent identity.
    #[must_use]
    pub const fn provenance(&self) -> &AgentResultProvenance {
        &self.provenance
    }

    /// Returns the durable clock observation at admission commit.
    #[must_use]
    pub const fn admitted_at(&self) -> Timestamp {
        self.admitted_at
    }

    /// Returns the current optimistic lifecycle revision.
    #[must_use]
    pub const fn revision(&self) -> RunRevision {
        self.revision
    }

    /// Returns the stable status projection.
    #[must_use]
    pub const fn status(&self) -> RunStatus {
        self.state.status()
    }

    /// Returns the most recent committed lifecycle observation.
    #[must_use]
    pub const fn changed_at(&self) -> Timestamp {
        self.state.changed_at(self.admitted_at)
    }

    /// Returns outstanding conditions only while waiting.
    #[must_use]
    pub const fn waits(&self) -> Option<&RunWaits> {
        match &self.state {
            RunState::Waiting { waits, .. } => Some(waits),
            _ => None,
        }
    }

    /// Returns cancellation intent while requested or terminally cancelled.
    #[must_use]
    pub const fn cancellation_request(&self) -> Option<&RunCancellationRequest> {
        match &self.state {
            RunState::CancellationRequested { request } => Some(request),
            RunState::Cancelled { cancellation } => Some(cancellation.request()),
            _ => None,
        }
    }

    /// Returns the successful terminal result, if committed.
    #[must_use]
    pub const fn result(&self) -> Option<&AgentResult> {
        match &self.state {
            RunState::Succeeded { result } => Some(result),
            _ => None,
        }
    }

    /// Returns the terminal failure occurrence for failed or cancelled runs.
    #[must_use]
    pub const fn terminal_failure(&self) -> Option<&Failure> {
        match &self.state {
            RunState::Failed { failure } => Some(failure.failure()),
            RunState::Cancelled { cancellation } => Some(cancellation.request().failure()),
            _ => None,
        }
    }

    /// Returns terminal cumulative usage for any completed outcome.
    #[must_use]
    pub const fn terminal_usage(&self) -> Option<&BudgetUsage> {
        match &self.state {
            RunState::Succeeded { result } => Some(result.usage()),
            RunState::Failed { failure } => Some(failure.usage()),
            RunState::Cancelled { cancellation } => Some(cancellation.usage()),
            _ => None,
        }
    }

    /// Applies one intrinsically valid lifecycle transition.
    ///
    /// A successful result is rebound to the trusted run provenance here.
    /// Before a durable terminal commit, the runtime must also call
    /// [`AgentResult::validate_for`] with the exact admitted request,
    /// descriptor snapshot, resolved budget, and digest-pinned schema
    /// validation result. Those values intentionally are not duplicated in
    /// every lifecycle snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`RunTransitionError`] for a transition outside the closed
    /// graph, a regressing clock, a stale/expired resolution, an early timer,
    /// substituted result provenance, or revision overflow.
    pub fn apply(self, transition: RunTransition) -> Result<Self, RunTransitionError> {
        let status = self.status();
        let transition_kind = transition.kind();
        if !transition_allowed(status, transition_kind) {
            return Err(RunTransitionError::InvalidTransition {
                status,
                transition: transition_kind,
            });
        }

        let changed_at = transition.changed_at();
        let previous = self.changed_at();
        if changed_at < previous {
            return Err(RunTransitionError::ClockRegression {
                previous,
                actual: changed_at,
            });
        }

        let RunLifecycle {
            provenance,
            admitted_at,
            revision,
            state,
        } = self;
        let next_state = apply_transition(&provenance, state, transition)?;

        let revision = revision
            .checked_next()
            .ok_or(RunTransitionError::RevisionOverflow)?;
        Ok(Self {
            provenance,
            admitted_at,
            revision,
            state: next_state,
        })
    }

    fn validate_snapshot(&self) -> Result<(), RunLifecycleError> {
        let status = self.status();
        if status == RunStatus::Pending && self.revision != RunRevision::ZERO {
            return Err(RunLifecycleError::PendingRevisionNotZero {
                actual: self.revision,
            });
        }
        if status != RunStatus::Pending && self.revision == RunRevision::ZERO {
            return Err(RunLifecycleError::AdvancedStateAtZeroRevision { status });
        }

        let changed_at = self.changed_at();
        if changed_at < self.admitted_at {
            return Err(RunLifecycleError::StateBeforeAdmission {
                admitted_at: self.admitted_at,
                changed_at,
            });
        }
        if let RunState::Waiting { waits, changed_at } = &self.state {
            if *changed_at < waits.registered_at() {
                return Err(RunLifecycleError::WaitingChangeBeforeRegistration {
                    registered_at: waits.registered_at(),
                    changed_at: *changed_at,
                });
            }
        }
        if let RunState::Succeeded { result } = &self.state {
            if result.provenance() != &self.provenance {
                return Err(RunLifecycleError::ResultProvenanceMismatch {
                    expected: Box::new(self.provenance.clone()),
                    actual: Box::new(result.provenance().clone()),
                });
            }
        }
        Ok(())
    }
}

fn apply_transition(
    provenance: &AgentResultProvenance,
    state: RunState,
    transition: RunTransition,
) -> Result<RunState, RunTransitionError> {
    let status = state.status();
    let transition_kind = transition.kind();
    Ok(match (state, transition) {
        (RunState::Pending, RunTransition::Start { started_at }) => RunState::Active {
            activated_at: started_at,
        },
        (RunState::Active { .. }, RunTransition::Wait { waits }) => RunState::Waiting {
            changed_at: waits.registered_at(),
            waits,
        },
        (
            RunState::Waiting { waits, .. },
            RunTransition::ResolveInterrupt {
                interrupt_id,
                resolved_at,
            },
        ) => resolve_interrupt(waits, interrupt_id, resolved_at)?,
        (RunState::Waiting { waits, .. }, RunTransition::FireTimer { timer_id, fired_at }) => {
            fire_timer(waits, timer_id, fired_at)?
        }
        (
            RunState::Pending | RunState::Active { .. } | RunState::Waiting { .. },
            RunTransition::RequestCancellation { request },
        ) => RunState::CancellationRequested { request },
        (
            RunState::CancellationRequested { request },
            RunTransition::ConfirmCancellation {
                completed_at,
                usage,
            },
        ) => RunState::Cancelled {
            cancellation: RunCancellation::new(request, completed_at, usage)
                .map_err(RunTransitionError::cancellation)?,
        },
        (RunState::Active { .. }, RunTransition::Succeed { result }) => {
            if result.provenance() != provenance {
                return Err(RunTransitionError::ResultProvenanceMismatch {
                    expected: Box::new(provenance.clone()),
                    actual: Box::new(result.provenance().clone()),
                });
            }
            RunState::Succeeded { result }
        }
        (
            RunState::Pending | RunState::Active { .. } | RunState::Waiting { .. },
            RunTransition::Fail { failure },
        ) => RunState::Failed { failure },
        _ => {
            return Err(RunTransitionError::InvalidTransition {
                status,
                transition: transition_kind,
            });
        }
    })
}

fn resolve_interrupt(
    waits: RunWaits,
    interrupt_id: InterruptId,
    resolved_at: Timestamp,
) -> Result<RunState, RunTransitionError> {
    let interrupt = waits
        .interrupt(interrupt_id)
        .ok_or(RunTransitionError::InterruptNotOutstanding { interrupt_id })?;
    if let Some(expires_at) = interrupt.expires_at() {
        if resolved_at >= expires_at {
            return Err(RunTransitionError::InterruptExpired {
                interrupt_id,
                expires_at,
                resolved_at,
            });
        }
    }
    Ok(match waits.without_interrupt(interrupt_id) {
        Some(waits) => RunState::Waiting {
            waits,
            changed_at: resolved_at,
        },
        None => RunState::Active {
            activated_at: resolved_at,
        },
    })
}

fn fire_timer(
    waits: RunWaits,
    timer_id: TimerId,
    fired_at: Timestamp,
) -> Result<RunState, RunTransitionError> {
    let timer = waits
        .timer(timer_id)
        .ok_or(RunTransitionError::TimerNotOutstanding { timer_id })?;
    if fired_at < timer.due_at() {
        return Err(RunTransitionError::TimerNotDue {
            timer_id,
            due_at: timer.due_at(),
            fired_at,
        });
    }
    Ok(match waits.without_timer(timer_id) {
        Some(waits) => RunState::Waiting {
            waits,
            changed_at: fired_at,
        },
        None => RunState::Active {
            activated_at: fired_at,
        },
    })
}

impl fmt::Debug for RunLifecycle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunLifecycle")
            .field("provenance", &self.provenance)
            .field("admitted_at", &self.admitted_at)
            .field("revision", &self.revision)
            .field("status", &self.status())
            .field("changed_at", &self.changed_at())
            .field("wait_count", &self.waits().map_or(0, RunWaits::len))
            .field(
                "has_cancellation_request",
                &self.cancellation_request().is_some(),
            )
            .field("has_result", &self.result().is_some())
            .field("has_failure", &self.terminal_failure().is_some())
            .field("usage_recorded", &self.terminal_usage().is_some())
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for RunLifecycle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            provenance: AgentResultProvenance,
            admitted_at: Timestamp,
            revision: RunRevision,
            state: RunState,
        }

        let wire = Wire::deserialize(deserializer)?;
        let lifecycle = Self {
            provenance: wire.provenance,
            admitted_at: wire.admitted_at,
            revision: wire.revision,
            state: wire.state,
        };
        lifecycle.validate_snapshot().map_err(de::Error::custom)?;
        Ok(lifecycle)
    }
}

/// Intrinsically invalid serialized lifecycle snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RunLifecycleError {
    /// Only the initial pending snapshot may use revision zero.
    #[error("pending run revision is {actual}; expected zero")]
    PendingRevisionNotZero {
        /// Rejected pending revision.
        actual: RunRevision,
    },
    /// An advanced state claimed to have applied no transition.
    #[error("run state {status:?} cannot exist at revision zero")]
    AdvancedStateAtZeroRevision {
        /// Rejected advanced status.
        status: RunStatus,
    },
    /// State time preceded durable admission.
    #[error("run state changed at {changed_at} before admission at {admitted_at}")]
    StateBeforeAdmission {
        /// Durable admission observation.
        admitted_at: Timestamp,
        /// Rejected state observation.
        changed_at: Timestamp,
    },
    /// A partially resolved wait claimed a change before registration.
    #[error("waiting state changed at {changed_at} before registration at {registered_at}")]
    WaitingChangeBeforeRegistration {
        /// Common wait registration observation.
        registered_at: Timestamp,
        /// Rejected latest change observation.
        changed_at: Timestamp,
    },
    /// A success snapshot substituted trusted run identity.
    #[error("successful result provenance does not match admitted run provenance")]
    ResultProvenanceMismatch {
        /// Trusted admission provenance.
        expected: Box<AgentResultProvenance>,
        /// Rejected result provenance.
        actual: Box<AgentResultProvenance>,
    },
}

/// Stable name of an attempted lifecycle transition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTransitionKind {
    /// Start semantic execution after admission.
    Start,
    /// Suspend on a non-empty atomic condition batch.
    Wait,
    /// Commit one authorized interrupt resolution.
    ResolveInterrupt,
    /// Commit one timer firing at or after its due instant.
    FireTimer,
    /// Commit cancellation intent and block ordinary terminal outcomes.
    RequestCancellation,
    /// Acknowledge the already committed cancellation request.
    ConfirmCancellation,
    /// Commit a validated successful result.
    Succeed,
    /// Commit a non-cancellation terminal failure.
    Fail,
}

/// One immutable input to the pure run lifecycle state machine.
#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RunTransition {
    /// Start an admitted pending run.
    Start {
        /// Durable execution-start observation.
        started_at: Timestamp,
    },
    /// Suspend an active run on one atomic condition batch.
    Wait {
        /// Non-empty, bounded outstanding conditions.
        waits: RunWaits,
    },
    /// Resolve exactly one outstanding unexpired interrupt.
    ResolveInterrupt {
        /// Exact interrupt selected by the durable resolution record.
        interrupt_id: InterruptId,
        /// Durable resolution observation.
        resolved_at: Timestamp,
    },
    /// Fire exactly one outstanding timer no earlier than its due instant.
    FireTimer {
        /// Exact durable timer selected by the scheduler.
        timer_id: TimerId,
        /// Durable firing observation.
        fired_at: Timestamp,
    },
    /// Commit immutable cancellation intent.
    RequestCancellation {
        /// Validated, non-retryable cancellation request.
        request: RunCancellationRequest,
    },
    /// Acknowledge cancellation after cooperative cleanup or enforced stop.
    ConfirmCancellation {
        /// Durable terminal observation.
        completed_at: Timestamp,
        /// Complete cumulative usage at terminal acknowledgement.
        usage: BudgetUsage,
    },
    /// Commit a successful agent result.
    Succeed {
        /// Intrinsically valid result; runtime context validation remains required.
        result: AgentResult,
    },
    /// Commit a non-cancellation terminal failure.
    Fail {
        /// Validated failure and complete cumulative usage.
        failure: RunFailure,
    },
}

impl RunTransition {
    /// Returns the stable transition name.
    #[must_use]
    pub const fn kind(&self) -> RunTransitionKind {
        match self {
            Self::Start { .. } => RunTransitionKind::Start,
            Self::Wait { .. } => RunTransitionKind::Wait,
            Self::ResolveInterrupt { .. } => RunTransitionKind::ResolveInterrupt,
            Self::FireTimer { .. } => RunTransitionKind::FireTimer,
            Self::RequestCancellation { .. } => RunTransitionKind::RequestCancellation,
            Self::ConfirmCancellation { .. } => RunTransitionKind::ConfirmCancellation,
            Self::Succeed { .. } => RunTransitionKind::Succeed,
            Self::Fail { .. } => RunTransitionKind::Fail,
        }
    }

    const fn changed_at(&self) -> Timestamp {
        match self {
            Self::Start { started_at } => *started_at,
            Self::Wait { waits } => waits.registered_at(),
            Self::ResolveInterrupt { resolved_at, .. } => *resolved_at,
            Self::FireTimer { fired_at, .. } => *fired_at,
            Self::RequestCancellation { request } => request.requested_at(),
            Self::ConfirmCancellation { completed_at, .. } => *completed_at,
            Self::Succeed { result } => result.completed_at(),
            Self::Fail { failure } => failure.completed_at(),
        }
    }
}

impl fmt::Debug for RunTransition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("RunTransition");
        debug.field("kind", &self.kind());
        match self {
            Self::Start { started_at } => {
                debug.field("changed_at", started_at);
            }
            Self::Wait { waits } => {
                debug
                    .field("changed_at", &waits.registered_at())
                    .field("wait_count", &waits.len());
            }
            Self::ResolveInterrupt {
                interrupt_id,
                resolved_at,
            } => {
                debug
                    .field("interrupt_id", interrupt_id)
                    .field("changed_at", resolved_at);
            }
            Self::FireTimer { timer_id, fired_at } => {
                debug
                    .field("timer_id", timer_id)
                    .field("changed_at", fired_at);
            }
            Self::RequestCancellation { request } => {
                debug
                    .field("failure", request.failure())
                    .field("changed_at", &request.requested_at());
            }
            Self::ConfirmCancellation { completed_at, .. } => {
                debug
                    .field("changed_at", completed_at)
                    .field("usage_recorded", &true);
            }
            Self::Succeed { result } => {
                debug
                    .field("result", result)
                    .field("changed_at", &result.completed_at());
            }
            Self::Fail { failure } => {
                debug
                    .field("failure", failure.failure())
                    .field("changed_at", &failure.completed_at())
                    .field("usage_recorded", &true);
            }
        }
        debug.finish_non_exhaustive()
    }
}

const fn transition_allowed(status: RunStatus, transition: RunTransitionKind) -> bool {
    matches!(
        (status, transition),
        (RunStatus::Pending, RunTransitionKind::Start)
            | (
                RunStatus::Pending | RunStatus::Active | RunStatus::Waiting,
                RunTransitionKind::RequestCancellation | RunTransitionKind::Fail
            )
            | (
                RunStatus::Active,
                RunTransitionKind::Wait | RunTransitionKind::Succeed
            )
            | (
                RunStatus::Waiting,
                RunTransitionKind::ResolveInterrupt | RunTransitionKind::FireTimer
            )
            | (
                RunStatus::CancellationRequested,
                RunTransitionKind::ConfirmCancellation
            )
    )
}

/// Rejected pure lifecycle transition.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RunTransitionError {
    /// The current state has no edge for this transition.
    #[error("transition {transition:?} is invalid from run state {status:?}")]
    InvalidTransition {
        /// Current lifecycle status.
        status: RunStatus,
        /// Rejected transition kind.
        transition: RunTransitionKind,
    },
    /// A transition clock observation moved backward.
    #[error("transition time {actual} precedes current state time {previous}")]
    ClockRegression {
        /// Latest committed lifecycle observation.
        previous: Timestamp,
        /// Rejected transition observation.
        actual: Timestamp,
    },
    /// An interrupt resolution named no currently outstanding marker.
    #[error("interrupt {interrupt_id} is not outstanding for this run")]
    InterruptNotOutstanding {
        /// Stale, duplicate, or unrelated interrupt identifier.
        interrupt_id: InterruptId,
    },
    /// An interrupt resolution arrived at or after exclusive expiry.
    #[error(
        "interrupt {interrupt_id} expired at {expires_at}; resolution arrived at {resolved_at}"
    )]
    InterruptExpired {
        /// Exact expired interrupt identifier.
        interrupt_id: InterruptId,
        /// Exclusive expiry observation.
        expires_at: Timestamp,
        /// Rejected resolution observation.
        resolved_at: Timestamp,
    },
    /// A firing named no currently outstanding timer.
    #[error("timer {timer_id} is not outstanding for this run")]
    TimerNotOutstanding {
        /// Stale, duplicate, or unrelated timer identifier.
        timer_id: TimerId,
    },
    /// A timer firing preceded its inclusive due instant.
    #[error("timer {timer_id} is due at {due_at}; firing arrived at {fired_at}")]
    TimerNotDue {
        /// Exact early timer identifier.
        timer_id: TimerId,
        /// Inclusive earliest firing instant.
        due_at: Timestamp,
        /// Rejected firing observation.
        fired_at: Timestamp,
    },
    /// A successful result substituted trusted run identity.
    #[error("successful result provenance does not match admitted run provenance")]
    ResultProvenanceMismatch {
        /// Trusted admission provenance.
        expected: Box<AgentResultProvenance>,
        /// Rejected result provenance.
        actual: Box<AgentResultProvenance>,
    },
    /// Cancellation acknowledgement was intrinsically invalid.
    #[error("terminal cancellation is invalid: {source}")]
    Cancellation {
        /// Underlying cancellation timing failure.
        #[source]
        source: RunCancellationError,
    },
    /// No next optimistic revision can be represented.
    #[error("run lifecycle revision overflowed")]
    RevisionOverflow,
}

impl RunTransitionError {
    const fn cancellation(source: RunCancellationError) -> Self {
        Self::Cancellation { source }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FailureCode, FailureId, FailureMessage, FailureOrigin, RunId, TenantId, ThreadId};
    use proptest::{collection, prelude::*};
    use serde_json::{Value, from_value, json, to_value};

    fn at(offset_micros: i64) -> Timestamp {
        let base = "2030-01-01T00:00:00.000000Z".parse::<Timestamp>().unwrap();
        Timestamp::from_unix_micros(base.unix_micros() + offset_micros).unwrap()
    }

    fn canonical_runtime_fixture() -> Value {
        serde_json::from_str(include_str!("../tests/fixtures/core-agent-runtime-v1.json")).unwrap()
    }

    fn provenance() -> AgentResultProvenance {
        let fixture = canonical_runtime_fixture();
        from_value(fixture["result_provenances"]["valid"][0].clone()).unwrap()
    }

    fn other_provenance() -> AgentResultProvenance {
        let mut value = to_value(provenance()).unwrap();
        value["invocation_id"] = Value::from("01912345-6789-7abc-8def-0123456789bf");
        from_value(value).unwrap()
    }

    fn result_at(completed_at: Timestamp, provenance: &AgentResultProvenance) -> AgentResult {
        let fixture = canonical_runtime_fixture();
        let mut value = fixture["results"]["valid"][0].clone();
        value["completed_at"] = Value::from(completed_at.to_string());
        value["provenance"] = to_value(provenance).unwrap();
        from_value(value).unwrap()
    }

    fn failure(category: FailureCategory, advice: RetryAdvice) -> Failure {
        Failure::new(
            FailureId::generate(),
            category,
            "runtime.test".parse::<FailureCode>().unwrap(),
            "stateknot.runtime".parse::<FailureOrigin>().unwrap(),
            "The run reached a tested terminal condition."
                .parse::<FailureMessage>()
                .unwrap(),
            advice,
        )
        .unwrap()
    }

    fn cancellation_request(requested_at: Timestamp) -> RunCancellationRequest {
        RunCancellationRequest::new(
            failure(FailureCategory::Cancelled, RetryAdvice::Never),
            requested_at,
        )
        .unwrap()
    }

    fn ordinary_failure(completed_at: Timestamp) -> RunFailure {
        RunFailure::new(
            failure(FailureCategory::Internal, RetryAdvice::Never),
            completed_at,
            BudgetUsage::zero(),
        )
        .unwrap()
    }

    fn interrupt(
        interrupt_id: InterruptId,
        requested_at: Timestamp,
        expires_at: Timestamp,
    ) -> RunInterrupt {
        RunInterrupt::new(
            interrupt_id,
            RunInterruptKind::Approval,
            requested_at,
            Some(expires_at),
        )
        .unwrap()
    }

    fn timer(timer_id: TimerId, scheduled_at: Timestamp, due_at: Timestamp) -> RunTimer {
        RunTimer::new(timer_id, RunTimerKind::Sleep, scheduled_at, due_at).unwrap()
    }

    #[test]
    fn admission_is_the_only_pending_zero_revision_snapshot() {
        let lifecycle = RunLifecycle::admitted(provenance(), at(0));
        assert_eq!(lifecycle.status(), RunStatus::Pending);
        assert_eq!(lifecycle.revision(), RunRevision::ZERO);
        assert_eq!(lifecycle.admitted_at(), at(0));
        assert_eq!(lifecycle.changed_at(), at(0));
        assert!(!lifecycle.status().is_terminal());
        assert!(lifecycle.waits().is_none());
        assert!(lifecycle.cancellation_request().is_none());
        assert!(lifecycle.result().is_none());
        assert!(lifecycle.terminal_failure().is_none());
        assert!(lifecycle.terminal_usage().is_none());

        let wire = to_value(&lifecycle).unwrap();
        assert_eq!(wire["revision"], "0");
        assert_eq!(wire["state"], json!({"kind": "pending"}));
        assert_eq!(
            from_value::<RunLifecycle>(wire).unwrap().status(),
            RunStatus::Pending
        );
    }

    #[test]
    fn multiple_waits_resolve_independently_and_resume_only_when_empty() {
        let interrupt_id = InterruptId::generate();
        let timer_id = TimerId::generate();
        let lifecycle = RunLifecycle::admitted(provenance(), at(0))
            .apply(RunTransition::Start { started_at: at(1) })
            .unwrap()
            .apply(RunTransition::Wait {
                waits: RunWaits::try_new([
                    RunWait::interrupt(interrupt(interrupt_id, at(2), at(8))),
                    RunWait::timer(timer(timer_id, at(2), at(6))),
                ])
                .unwrap(),
            })
            .unwrap();

        assert_eq!(lifecycle.status(), RunStatus::Waiting);
        assert_eq!(lifecycle.revision(), RunRevision::new(2));
        assert_eq!(lifecycle.waits().unwrap().len(), 2);

        let lifecycle = lifecycle
            .apply(RunTransition::ResolveInterrupt {
                interrupt_id,
                resolved_at: at(4),
            })
            .unwrap();
        assert_eq!(lifecycle.status(), RunStatus::Waiting);
        assert_eq!(lifecycle.changed_at(), at(4));
        assert_eq!(lifecycle.waits().unwrap().len(), 1);
        assert!(lifecycle.waits().unwrap().interrupt(interrupt_id).is_none());
        assert!(lifecycle.waits().unwrap().timer(timer_id).is_some());

        let lifecycle = lifecycle
            .apply(RunTransition::FireTimer {
                timer_id,
                fired_at: at(6),
            })
            .unwrap();
        assert_eq!(lifecycle.status(), RunStatus::Active);
        assert_eq!(lifecycle.changed_at(), at(6));
        assert_eq!(lifecycle.revision(), RunRevision::new(4));
        assert!(lifecycle.waits().is_none());
    }

    #[test]
    fn interrupt_expiry_is_exclusive_and_stale_resolution_is_rejected() {
        let interrupt_id = InterruptId::generate();
        let lifecycle = RunLifecycle::admitted(provenance(), at(0))
            .apply(RunTransition::Start { started_at: at(1) })
            .unwrap()
            .apply(RunTransition::Wait {
                waits: RunWaits::try_new([RunWait::interrupt(interrupt(
                    interrupt_id,
                    at(2),
                    at(5),
                ))])
                .unwrap(),
            })
            .unwrap();

        assert_eq!(
            lifecycle
                .clone()
                .apply(RunTransition::ResolveInterrupt {
                    interrupt_id,
                    resolved_at: at(5),
                })
                .unwrap_err(),
            RunTransitionError::InterruptExpired {
                interrupt_id,
                expires_at: at(5),
                resolved_at: at(5),
            }
        );
        assert!(
            lifecycle
                .apply(RunTransition::ResolveInterrupt {
                    interrupt_id: InterruptId::generate(),
                    resolved_at: at(4),
                })
                .is_err()
        );
    }

    #[test]
    fn timers_cannot_fire_early_or_twice() {
        let timer_id = TimerId::generate();
        let lifecycle = RunLifecycle::admitted(provenance(), at(0))
            .apply(RunTransition::Start { started_at: at(1) })
            .unwrap()
            .apply(RunTransition::Wait {
                waits: RunWaits::try_new([RunWait::timer(timer(timer_id, at(2), at(7)))]).unwrap(),
            })
            .unwrap();

        assert_eq!(
            lifecycle
                .clone()
                .apply(RunTransition::FireTimer {
                    timer_id,
                    fired_at: at(6),
                })
                .unwrap_err(),
            RunTransitionError::TimerNotDue {
                timer_id,
                due_at: at(7),
                fired_at: at(6),
            }
        );

        let active = lifecycle
            .apply(RunTransition::FireTimer {
                timer_id,
                fired_at: at(7),
            })
            .unwrap();
        assert_eq!(active.status(), RunStatus::Active);
        assert_eq!(
            active
                .apply(RunTransition::FireTimer {
                    timer_id,
                    fired_at: at(8),
                })
                .unwrap_err(),
            RunTransitionError::InvalidTransition {
                status: RunStatus::Active,
                transition: RunTransitionKind::FireTimer,
            }
        );
    }

    #[test]
    fn wait_batches_are_non_empty_bounded_unique_and_atomic() {
        assert_eq!(RunWaits::try_new([]), Err(RunWaitsError::Empty));

        let duplicate = uuid::Uuid::now_v7();
        let interrupt_id = InterruptId::from_uuid(duplicate).unwrap();
        let timer_id = TimerId::from_uuid(duplicate).unwrap();
        assert_eq!(
            RunWaits::try_new([
                RunWait::interrupt(interrupt(interrupt_id, at(1), at(4))),
                RunWait::timer(timer(timer_id, at(1), at(4))),
            ]),
            Err(RunWaitsError::DuplicateIdentity)
        );

        assert_eq!(
            RunWaits::try_new([
                RunWait::interrupt(interrupt(InterruptId::generate(), at(1), at(4))),
                RunWait::timer(timer(TimerId::generate(), at(2), at(4))),
            ]),
            Err(RunWaitsError::MixedRegistrationTimes {
                expected: at(1),
                actual: at(2),
            })
        );

        let too_many = (0..=RunWaits::MAX_LEN)
            .map(|_| RunWait::timer(timer(TimerId::generate(), at(1), at(4))));
        assert_eq!(
            RunWaits::try_new(too_many),
            Err(RunWaitsError::TooMany {
                maximum: RunWaits::MAX_LEN,
                actual: RunWaits::MAX_LEN + 1,
            })
        );
    }

    #[test]
    fn wait_marker_constructors_reject_non_future_boundaries() {
        assert_eq!(
            RunInterrupt::new(
                InterruptId::generate(),
                RunInterruptKind::Input,
                at(3),
                Some(at(3)),
            ),
            Err(RunInterruptError::ExpiryNotAfterRequest {
                requested_at: at(3),
                expires_at: at(3),
            })
        );
        assert_eq!(
            RunTimer::new(
                TimerId::generate(),
                RunTimerKind::RetryBackoff,
                at(3),
                at(2),
            ),
            Err(RunTimerError::DueNotAfterSchedule {
                scheduled_at: at(3),
                due_at: at(2),
            })
        );
    }

    #[test]
    fn cancellation_request_wins_over_every_uncommitted_outcome() {
        let provenance = provenance();
        let lifecycle = RunLifecycle::admitted(provenance.clone(), at(0))
            .apply(RunTransition::Start { started_at: at(1) })
            .unwrap()
            .apply(RunTransition::RequestCancellation {
                request: cancellation_request(at(2)),
            })
            .unwrap();

        assert_eq!(lifecycle.status(), RunStatus::CancellationRequested);
        assert!(lifecycle.cancellation_request().is_some());
        assert_eq!(
            lifecycle
                .clone()
                .apply(RunTransition::Succeed {
                    result: result_at(at(3), &provenance),
                })
                .unwrap_err(),
            RunTransitionError::InvalidTransition {
                status: RunStatus::CancellationRequested,
                transition: RunTransitionKind::Succeed,
            }
        );
        assert_eq!(
            lifecycle
                .clone()
                .apply(RunTransition::Fail {
                    failure: ordinary_failure(at(3)),
                })
                .unwrap_err(),
            RunTransitionError::InvalidTransition {
                status: RunStatus::CancellationRequested,
                transition: RunTransitionKind::Fail,
            }
        );

        let cancelled = lifecycle
            .apply(RunTransition::ConfirmCancellation {
                completed_at: at(4),
                usage: BudgetUsage::zero(),
            })
            .unwrap();
        assert_eq!(cancelled.status(), RunStatus::Cancelled);
        assert!(cancelled.status().is_terminal());
        assert_eq!(
            cancelled.terminal_failure().unwrap().category(),
            FailureCategory::Cancelled
        );
        assert!(cancelled.terminal_usage().is_some());
        assert!(cancelled.result().is_none());
        assert!(
            cancelled
                .apply(RunTransition::RequestCancellation {
                    request: cancellation_request(at(5)),
                })
                .is_err()
        );
    }

    #[test]
    fn a_committed_success_wins_over_late_cancellation() {
        let provenance = provenance();
        let succeeded = RunLifecycle::admitted(provenance.clone(), at(0))
            .apply(RunTransition::Start { started_at: at(1) })
            .unwrap()
            .apply(RunTransition::Succeed {
                result: result_at(at(2), &provenance),
            })
            .unwrap();
        assert_eq!(succeeded.status(), RunStatus::Succeeded);
        assert!(succeeded.result().is_some());
        assert!(succeeded.terminal_failure().is_none());
        assert!(succeeded.terminal_usage().is_some());
        assert_eq!(
            succeeded
                .apply(RunTransition::RequestCancellation {
                    request: cancellation_request(at(3)),
                })
                .unwrap_err(),
            RunTransitionError::InvalidTransition {
                status: RunStatus::Succeeded,
                transition: RunTransitionKind::RequestCancellation,
            }
        );
    }

    #[test]
    fn success_must_match_trusted_admission_provenance() {
        let expected = provenance();
        let actual = other_provenance();
        let error = RunLifecycle::admitted(expected.clone(), at(0))
            .apply(RunTransition::Start { started_at: at(1) })
            .unwrap()
            .apply(RunTransition::Succeed {
                result: result_at(at(2), &actual),
            })
            .unwrap_err();
        assert_eq!(
            error,
            RunTransitionError::ResultProvenanceMismatch {
                expected: Box::new(expected),
                actual: Box::new(actual),
            }
        );
    }

    #[test]
    fn ordinary_failure_cannot_smuggle_cancellation() {
        assert_eq!(
            RunFailure::new(
                failure(FailureCategory::Cancelled, RetryAdvice::Never),
                at(1),
                BudgetUsage::zero(),
            )
            .unwrap_err(),
            RunFailureError::CancellationRequiresCancellationPath
        );
        assert_eq!(
            RunCancellationRequest::new(
                failure(FailureCategory::Internal, RetryAdvice::Never),
                at(1),
            )
            .unwrap_err(),
            RunCancellationError::CategoryNotCancelled {
                actual: FailureCategory::Internal,
            }
        );
    }

    #[test]
    fn lifecycle_clock_never_moves_backward() {
        let lifecycle = RunLifecycle::admitted(provenance(), at(5));
        assert_eq!(
            lifecycle
                .apply(RunTransition::Start { started_at: at(4) })
                .unwrap_err(),
            RunTransitionError::ClockRegression {
                previous: at(5),
                actual: at(4),
            }
        );
    }

    #[test]
    fn snapshot_deserialization_rejects_unreachable_cross_field_states() {
        let pending = RunLifecycle::admitted(provenance(), at(2));
        let mut value = to_value(&pending).unwrap();
        value["revision"] = Value::from("1");
        assert!(from_value::<RunLifecycle>(value).is_err());

        let mut value = to_value(&pending).unwrap();
        value["state"] = json!({"kind": "active", "activated_at": at(2).to_string()});
        assert!(from_value::<RunLifecycle>(value).is_err());

        let active = pending
            .clone()
            .apply(RunTransition::Start { started_at: at(3) })
            .unwrap();
        let mut value = to_value(active).unwrap();
        value["state"]["activated_at"] = Value::from(at(1).to_string());
        assert!(from_value::<RunLifecycle>(value).is_err());

        let mut succeeded = to_value(
            RunLifecycle::admitted(provenance(), at(0))
                .apply(RunTransition::Start { started_at: at(1) })
                .unwrap()
                .apply(RunTransition::Succeed {
                    result: result_at(at(2), &provenance()),
                })
                .unwrap(),
        )
        .unwrap();
        succeeded["state"]["result"]["provenance"] = to_value(other_provenance()).unwrap();
        assert!(from_value::<RunLifecycle>(succeeded).is_err());
    }

    #[test]
    fn wire_contracts_are_closed_and_revalidate_nested_values() {
        assert!(
            from_value::<RunInterrupt>(json!({
                "interrupt_id": InterruptId::generate().to_string(),
                "kind": "input",
                "requested_at": at(1).to_string(),
                "expires_at": at(2).to_string(),
                "payload": {}
            }))
            .is_err()
        );
        assert!(
            from_value::<RunTransition>(json!({
                "kind": "start",
                "started_at": at(1).to_string(),
                "extra": true
            }))
            .is_err()
        );
        assert!(from_value::<RunTransition>(json!({"kind": "unknown"})).is_err());
        assert!(from_value::<RunWaits>(json!([])).is_err());
    }

    #[test]
    fn revision_overflow_fails_closed() {
        let active = RunLifecycle::admitted(provenance(), at(0))
            .apply(RunTransition::Start { started_at: at(1) })
            .unwrap();
        let mut value = to_value(active).unwrap();
        value["revision"] = Value::from(u64::MAX.to_string());
        let active = from_value::<RunLifecycle>(value).unwrap();
        assert_eq!(
            active
                .apply(RunTransition::Fail {
                    failure: ordinary_failure(at(2)),
                })
                .unwrap_err(),
            RunTransitionError::RevisionOverflow
        );
    }

    fn transition_for(
        action: u8,
        lifecycle: &RunLifecycle,
        observed_at: Timestamp,
    ) -> RunTransition {
        match action {
            0 => RunTransition::Start {
                started_at: observed_at,
            },
            1 => RunTransition::Wait {
                waits: RunWaits::try_new([
                    RunWait::interrupt(interrupt(
                        InterruptId::generate(),
                        observed_at,
                        Timestamp::from_unix_micros(observed_at.unix_micros() + 10).unwrap(),
                    )),
                    RunWait::timer(timer(
                        TimerId::generate(),
                        observed_at,
                        Timestamp::from_unix_micros(observed_at.unix_micros() + 2).unwrap(),
                    )),
                ])
                .unwrap(),
            },
            2 => RunTransition::ResolveInterrupt {
                interrupt_id: lifecycle
                    .waits()
                    .and_then(|waits| waits.iter().find_map(RunWait::as_interrupt))
                    .map_or_else(InterruptId::generate, RunInterrupt::interrupt_id),
                resolved_at: observed_at,
            },
            3 => {
                let timer = lifecycle
                    .waits()
                    .and_then(|waits| waits.iter().find_map(RunWait::as_timer));
                RunTransition::FireTimer {
                    timer_id: timer.map_or_else(TimerId::generate, RunTimer::timer_id),
                    fired_at: timer.map_or(observed_at, RunTimer::due_at).max(observed_at),
                }
            }
            4 => RunTransition::RequestCancellation {
                request: cancellation_request(observed_at),
            },
            5 => RunTransition::ConfirmCancellation {
                completed_at: observed_at,
                usage: BudgetUsage::zero(),
            },
            6 => RunTransition::Succeed {
                result: result_at(observed_at, lifecycle.provenance()),
            },
            _ => RunTransition::Fail {
                failure: ordinary_failure(observed_at),
            },
        }
    }

    proptest! {
        #[test]
        fn randomized_state_machine_never_skips_revision_or_revives_terminal_state(
            actions in collection::vec(0_u8..8, 0..128),
        ) {
            let mut lifecycle = RunLifecycle::admitted(provenance(), at(0));
            for (index, action) in actions.into_iter().enumerate() {
                let observed_at = at(i64::try_from(index + 1).unwrap());
                let previous_status = lifecycle.status();
                let previous_revision = lifecycle.revision();
                let previous_wire = to_value(&lifecycle).unwrap();
                let transition = transition_for(action, &lifecycle, observed_at);
                match lifecycle.clone().apply(transition) {
                    Ok(next) => {
                        prop_assert_eq!(next.revision().get(), previous_revision.get() + 1);
                        prop_assert!(next.changed_at() >= lifecycle.changed_at());
                        prop_assert!(!previous_status.is_terminal());
                        lifecycle = next;
                    }
                    Err(_) => {
                        prop_assert_eq!(to_value(&lifecycle).unwrap(), previous_wire);
                    }
                }
                if previous_status.is_terminal() {
                    prop_assert_eq!(lifecycle.status(), previous_status);
                }
            }
        }
    }

    #[test]
    fn timer_id_is_a_strict_uuid_v7_wire_type() {
        let id = TimerId::generate();
        let wire = serde_json::to_string(&id).unwrap();
        assert_eq!(serde_json::from_str::<TimerId>(&wire).unwrap(), id);
        assert!(
            serde_json::from_str::<TimerId>(r#""550e8400-e29b-41d4-a716-446655440000""#).is_err()
        );

        let run_id: RunId = "01912345-6789-7abc-8def-0123456789ae".parse().unwrap();
        let thread_id: ThreadId = "01912345-6789-7abc-8def-0123456789af".parse().unwrap();
        let tenant: TenantId = "tenant-production".parse().unwrap();
        assert_eq!(run_id.to_string().len(), 36);
        assert_eq!(thread_id.to_string().len(), 36);
        assert_eq!(tenant.as_str(), "tenant-production");
    }
}
