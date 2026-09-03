// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Runtime-neutral typed and erased tool execution contracts.

use std::{
    error::Error as StdError,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::DeserializeOwned};
use thiserror::Error;

use crate::{
    ArtifactRef, AttemptId, BoundedJson, BudgetRemaining, ByteCount, CancellationSignal,
    CapabilityIdentity, CapabilityReference, DurationMillis, ExecutionCount, Failure,
    FailureCategory, FailureCode, FailureId, FailureMessage, FailureOrigin, InvocationId,
    JsonLimits, PrincipalIdentity, RetryAdvice, RunId, SchemaReference, TenantId, ThreadId,
    Timestamp, ToolDescriptor, ToolIdempotency, ToolRisk,
};

/// Stable provider-facing idempotency key for one logical tool invocation.
///
/// The key is derived only from [`InvocationId`], so every physical
/// [`AttemptId`] for the same logical invocation receives identical text. It
/// contains no tenant name, tool arguments, credential, or user-provided
/// secret. The value is deliberately not serializable as a general payload;
/// durable invocation records persist the underlying invocation identifier.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ToolIdempotencyKey(InvocationId);

impl ToolIdempotencyKey {
    /// Derives the stable key for one logical invocation.
    #[must_use]
    pub const fn from_invocation_id(invocation_id: InvocationId) -> Self {
        Self(invocation_id)
    }

    /// Returns the logical invocation from which this key was derived.
    #[must_use]
    pub const fn invocation_id(self) -> InvocationId {
        self.0
    }
}

impl fmt::Display for ToolIdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl fmt::Debug for ToolIdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ToolIdempotencyKey([REDACTED])")
    }
}

/// Ephemeral, capability-limited context for reconciling one physical tool attempt.
///
/// Reconciliation is not a new business attempt: it receives the original
/// invocation and attempt identities, never a fresh attempt identifier or a
/// mutable property bag. The finite deadline is independently bounded by the
/// run deadline and the frozen tool timeout. A descriptor must explicitly
/// declare status-query support before this context can be constructed.
#[derive(Clone)]
pub struct ToolReconciliationContext {
    tenant_id: TenantId,
    run_id: RunId,
    thread_id: ThreadId,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    tool: CapabilityIdentity,
    idempotency: ToolIdempotency,
    idempotency_key: Option<ToolIdempotencyKey>,
    observed_at: Timestamp,
    deadline: Timestamp,
    deadline_instant: Instant,
    effective_timeout: DurationMillis,
    cancellation: CancellationSignal,
}

impl ToolReconciliationContext {
    /// Constructs a finite context for the original ambiguous attempt.
    ///
    /// # Errors
    ///
    /// Rejects descriptors without status-query support, zero or widened
    /// timeouts, expired run deadlines, and unrepresentable monotonic deadlines.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        thread_id: ThreadId,
        invocation_id: InvocationId,
        attempt_id: AttemptId,
        descriptor: &ToolDescriptor,
        effective_timeout: DurationMillis,
        observed_at: Timestamp,
        observed_instant: Instant,
        run_deadline: Timestamp,
        cancellation: CancellationSignal,
    ) -> Result<Self, ToolReconciliationContextError> {
        if !descriptor.semantics().supports_status_query() {
            return Err(ToolReconciliationContextError::StatusQueryUnsupported);
        }
        if effective_timeout == DurationMillis::ZERO {
            return Err(ToolReconciliationContextError::ZeroEffectiveTimeout);
        }
        let descriptor_timeout = descriptor.limits().timeout();
        if effective_timeout > descriptor_timeout {
            return Err(ToolReconciliationContextError::TimeoutExceedsDescriptor {
                descriptor: descriptor_timeout,
                actual: effective_timeout,
            });
        }

        let run_remaining_micros = i128::from(run_deadline.unix_micros())
            .checked_sub(i128::from(observed_at.unix_micros()))
            .expect("subtracting two i64 timestamps cannot overflow i128");
        if run_remaining_micros <= 0 {
            return Err(ToolReconciliationContextError::DeadlineReached {
                deadline: run_deadline,
                observed_at,
            });
        }
        let timeout_micros = i128::from(effective_timeout.as_i64()) * 1_000;
        let remaining_micros = run_remaining_micros.min(timeout_micros);
        let deadline_micros = i128::from(observed_at.unix_micros()) + remaining_micros;
        let deadline_micros = i64::try_from(deadline_micros)
            .expect("a reconciliation deadline bounded by a valid run deadline fits i64");
        let deadline = Timestamp::from_unix_micros(deadline_micros)
            .expect("a reconciliation deadline bounded by a valid run deadline is valid");
        let remaining_micros = u64::try_from(remaining_micros)
            .expect("a positive supported timestamp distance fits u64 microseconds");
        let remaining = Duration::from_micros(remaining_micros);
        let deadline_instant = observed_instant
            .checked_add(remaining)
            .ok_or(ToolReconciliationContextError::MonotonicDeadlineOutOfRange { remaining })?;

        let idempotency = descriptor.semantics().idempotency();
        let idempotency_key = descriptor
            .semantics()
            .requires_idempotency_key()
            .then_some(ToolIdempotencyKey::from_invocation_id(invocation_id));
        Ok(Self {
            tenant_id,
            run_id,
            thread_id,
            invocation_id,
            attempt_id,
            tool: descriptor.metadata().identity().clone(),
            idempotency,
            idempotency_key,
            observed_at,
            deadline,
            deadline_instant,
            effective_timeout,
            cancellation,
        })
    }

    /// Returns the tenant boundary.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the enclosing run identity.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the enclosing conversation thread identity.
    #[must_use]
    pub const fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    /// Returns the original logical invocation identity.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the original physical attempt identity.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the frozen owner-qualified tool identity.
    #[must_use]
    pub const fn tool(&self) -> &CapabilityIdentity {
        &self.tool
    }

    /// Returns the descriptor-declared idempotency mechanism.
    #[must_use]
    pub const fn idempotency(&self) -> ToolIdempotency {
        self.idempotency
    }

    /// Returns the stable original invocation key when required.
    #[must_use]
    pub const fn idempotency_key(&self) -> Option<ToolIdempotencyKey> {
        self.idempotency_key
    }

    /// Returns the trusted wall-clock observation.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the finite wall-clock probe deadline.
    #[must_use]
    pub const fn deadline(&self) -> Timestamp {
        self.deadline
    }

    /// Returns the equivalent process-local monotonic deadline.
    #[must_use]
    pub const fn deadline_instant(&self) -> Instant {
        self.deadline_instant
    }

    /// Returns the effective probe timeout.
    #[must_use]
    pub const fn effective_timeout(&self) -> DurationMillis {
        self.effective_timeout
    }

    /// Returns the cooperative cancellation signal.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }

    /// Returns a stop reason at one monotonic observation.
    #[must_use]
    pub fn stop_reason_at(&self, observed_instant: Instant) -> Option<ToolStopReason> {
        if self.cancellation.is_cancelled() {
            Some(ToolStopReason::Cancelled)
        } else if self
            .deadline_instant
            .checked_duration_since(observed_instant)
            .filter(|remaining| !remaining.is_zero())
            .is_none()
        {
            Some(ToolStopReason::DeadlineExceeded)
        } else {
            None
        }
    }

    /// Revalidates this context against the immutable descriptor snapshot.
    pub fn validate_for(
        &self,
        descriptor: &ToolDescriptor,
    ) -> Result<(), ToolReconciliationContextBindingError> {
        if !descriptor.semantics().supports_status_query() {
            return Err(ToolReconciliationContextBindingError::StatusQueryUnsupported);
        }
        let expected_tool = descriptor.metadata().identity();
        if &self.tool != expected_tool {
            return Err(
                ToolReconciliationContextBindingError::ToolIdentityMismatch {
                    expected: Box::new(expected_tool.clone()),
                    actual: Box::new(self.tool.clone()),
                },
            );
        }
        let expected_idempotency = descriptor.semantics().idempotency();
        if self.idempotency != expected_idempotency {
            return Err(ToolReconciliationContextBindingError::IdempotencyMismatch {
                expected: expected_idempotency,
                actual: self.idempotency,
            });
        }
        if self.effective_timeout > descriptor.limits().timeout() {
            return Err(
                ToolReconciliationContextBindingError::TimeoutExceedsDescriptor {
                    descriptor: descriptor.limits().timeout(),
                    actual: self.effective_timeout,
                },
            );
        }
        let key_is_valid = matches!(
            (self.idempotency, self.idempotency_key),
            (ToolIdempotency::RequiredKey, Some(key)) if key.invocation_id() == self.invocation_id
        ) || (self.idempotency != ToolIdempotency::RequiredKey
            && self.idempotency_key.is_none());
        if !key_is_valid {
            return Err(ToolReconciliationContextBindingError::InvalidIdempotencyKeyBinding);
        }
        Ok(())
    }
}

impl fmt::Debug for ToolReconciliationContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolReconciliationContext")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("thread_id", &self.thread_id)
            .field("invocation_id", &self.invocation_id)
            .field("attempt_id", &self.attempt_id)
            .field("tool", &self.tool)
            .field("idempotency", &self.idempotency)
            .field("has_idempotency_key", &self.idempotency_key.is_some())
            .field("observed_at", &self.observed_at)
            .field("deadline", &self.deadline)
            .field("effective_timeout", &self.effective_timeout)
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// Failure to construct a finite reconciliation context.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolReconciliationContextError {
    /// The descriptor did not declare an executable status query.
    #[error("tool descriptor does not support reconciliation status queries")]
    StatusQueryUnsupported,
    /// A zero timeout would create an immediately ineligible probe.
    #[error("tool reconciliation effective timeout must be greater than zero")]
    ZeroEffectiveTimeout,
    /// A runtime layer attempted to widen the immutable tool timeout.
    #[error("tool reconciliation timeout {actual}ms exceeds descriptor timeout {descriptor}ms")]
    TimeoutExceedsDescriptor {
        /// Immutable descriptor ceiling.
        descriptor: DurationMillis,
        /// Rejected effective timeout.
        actual: DurationMillis,
    },
    /// The run deadline was already reached.
    #[error("tool reconciliation deadline {deadline} was reached at {observed_at}")]
    DeadlineReached {
        /// Durable run deadline.
        deadline: Timestamp,
        /// Wall-clock observation.
        observed_at: Timestamp,
    },
    /// The finite deadline could not be represented by the monotonic clock.
    #[error("tool reconciliation monotonic deadline is out of range after {remaining:?}")]
    MonotonicDeadlineOutOfRange {
        /// Positive duration to the deadline.
        remaining: Duration,
    },
}

/// Invalid relationship between a reconciliation context and descriptor.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolReconciliationContextBindingError {
    /// The descriptor no longer declares a status query.
    #[error("tool descriptor does not support reconciliation status queries")]
    StatusQueryUnsupported,
    /// The context names a different immutable tool version.
    #[error("tool reconciliation identity {actual:?} does not match descriptor {expected:?}")]
    ToolIdentityMismatch {
        /// Expected identity.
        expected: Box<CapabilityIdentity>,
        /// Actual identity.
        actual: Box<CapabilityIdentity>,
    },
    /// The descriptor idempotency mechanism changed.
    #[error("tool reconciliation idempotency {actual:?} does not match descriptor {expected:?}")]
    IdempotencyMismatch {
        /// Expected mechanism.
        expected: ToolIdempotency,
        /// Actual mechanism.
        actual: ToolIdempotency,
    },
    /// The context widened the immutable timeout ceiling.
    #[error("tool reconciliation timeout {actual}ms exceeds descriptor timeout {descriptor}ms")]
    TimeoutExceedsDescriptor {
        /// Immutable descriptor ceiling.
        descriptor: DurationMillis,
        /// Actual timeout.
        actual: DurationMillis,
    },
    /// The stable idempotency key does not belong to the original invocation.
    #[error("tool reconciliation idempotency key is not bound to its invocation")]
    InvalidIdempotencyKeyBinding,
}

/// Reason a tool boundary must stop before accepting another result.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ToolStopReason {
    /// The enclosing run requested cooperative cancellation.
    Cancelled,
    /// The effective monotonic invocation deadline was reached.
    DeadlineExceeded,
}

/// Ephemeral, capability-limited context for exactly one tool attempt.
///
/// The context binds stable logical invocation identity to one physical
/// attempt and one immutable tool version. Its effective deadline is the
/// intersection of the remaining run deadline and an already narrowed tool
/// timeout. It exposes a durable idempotency key only when the descriptor
/// requires one. It contains no arguments, raw credentials, database handle,
/// provider client, artifact storage coordinates, or mutable property bag.
///
/// Cancellation is cooperative and may race with completion. Neither a
/// cancellation observation nor a timeout proves that a write did not occur.
#[derive(Clone)]
pub struct ToolContext {
    tenant_id: TenantId,
    run_id: RunId,
    thread_id: ThreadId,
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    tool: CapabilityIdentity,
    idempotency: ToolIdempotency,
    idempotency_key: Option<ToolIdempotencyKey>,
    budget: BudgetRemaining,
    observed_at: Timestamp,
    deadline: Timestamp,
    deadline_instant: Instant,
    effective_timeout: DurationMillis,
    cancellation: CancellationSignal,
    progress: Option<ToolProgressReporter>,
}

impl ToolContext {
    /// Constructs one attempt context from paired wall and monotonic clocks.
    ///
    /// `effective_timeout` must already be narrowed by system, tenant, policy,
    /// and run limits and cannot exceed the descriptor ceiling. `observed_at`
    /// and `observed_instant` must describe the same runtime observation.
    ///
    /// # Errors
    ///
    /// Returns [`ToolContextError`] for a zero or widened timeout, an expired
    /// run deadline, or a monotonic deadline that the platform cannot represent.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        run_id: RunId,
        thread_id: ThreadId,
        invocation_id: InvocationId,
        attempt_id: AttemptId,
        descriptor: &ToolDescriptor,
        budget: BudgetRemaining,
        effective_timeout: DurationMillis,
        observed_at: Timestamp,
        observed_instant: Instant,
        cancellation: CancellationSignal,
    ) -> Result<Self, ToolContextError> {
        if effective_timeout == DurationMillis::ZERO {
            return Err(ToolContextError::ZeroEffectiveTimeout);
        }
        let descriptor_timeout = descriptor.limits().timeout();
        if effective_timeout > descriptor_timeout {
            return Err(ToolContextError::TimeoutExceedsDescriptor {
                descriptor: descriptor_timeout,
                actual: effective_timeout,
            });
        }

        let run_deadline = budget.deadline();
        let run_remaining_micros = i128::from(run_deadline.unix_micros())
            .checked_sub(i128::from(observed_at.unix_micros()))
            .expect("subtracting two i64 timestamps cannot overflow i128");
        if run_remaining_micros <= 0 {
            return Err(ToolContextError::DeadlineReached {
                deadline: run_deadline,
                observed_at,
            });
        }

        let timeout_micros = i128::from(effective_timeout.as_i64()) * 1_000;
        let remaining_micros = run_remaining_micros.min(timeout_micros);
        let deadline_micros = i128::from(observed_at.unix_micros()) + remaining_micros;
        let deadline_micros = i64::try_from(deadline_micros)
            .expect("an effective deadline bounded by a valid run deadline fits i64");
        let deadline = Timestamp::from_unix_micros(deadline_micros)
            .expect("an effective deadline bounded by a valid run deadline is a timestamp");
        let remaining_micros = u64::try_from(remaining_micros)
            .expect("a positive supported timestamp distance fits u64 microseconds");
        let remaining = Duration::from_micros(remaining_micros);
        let deadline_instant = observed_instant
            .checked_add(remaining)
            .ok_or(ToolContextError::MonotonicDeadlineOutOfRange { remaining })?;

        let idempotency = descriptor.semantics().idempotency();
        let idempotency_key = descriptor
            .semantics()
            .requires_idempotency_key()
            .then_some(ToolIdempotencyKey::from_invocation_id(invocation_id));

        Ok(Self {
            tenant_id,
            run_id,
            thread_id,
            invocation_id,
            attempt_id,
            tool: descriptor.metadata().identity().clone(),
            idempotency,
            idempotency_key,
            budget,
            observed_at,
            deadline,
            deadline_instant,
            effective_timeout,
            cancellation,
            progress: None,
        })
    }

    /// Constructs a context with a durable, ordered progress sink.
    ///
    /// This has the same clock and timeout invariants as [`Self::new`]. The
    /// reporter enforces the descriptor's finite progress-event ceiling and
    /// serial ordering before forwarding events to `progress_sink`.
    ///
    /// # Errors
    ///
    /// Returns [`ToolContextError`] under the same conditions as [`Self::new`],
    /// or when the descriptor declares zero progress events.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_progress(
        tenant_id: TenantId,
        run_id: RunId,
        thread_id: ThreadId,
        invocation_id: InvocationId,
        attempt_id: AttemptId,
        descriptor: &ToolDescriptor,
        budget: BudgetRemaining,
        effective_timeout: DurationMillis,
        observed_at: Timestamp,
        observed_instant: Instant,
        cancellation: CancellationSignal,
        progress_sink: Arc<dyn ToolProgressSink>,
    ) -> Result<Self, ToolContextError> {
        let mut context = Self::new(
            tenant_id,
            run_id,
            thread_id,
            invocation_id,
            attempt_id,
            descriptor,
            budget,
            effective_timeout,
            observed_at,
            observed_instant,
            cancellation,
        )?;
        let maximum = descriptor.invocation().max_progress_events();
        if maximum == ExecutionCount::ZERO {
            return Err(ToolContextError::ProgressUnsupported);
        }
        context.progress = Some(ToolProgressReporter::new(
            ToolProgressProvenance::new(
                invocation_id,
                attempt_id,
                descriptor.metadata().identity().clone(),
            ),
            maximum,
            progress_sink,
        ));
        Ok(context)
    }

    /// Returns the tenant boundary for policy, storage, and audit correlation.
    #[must_use]
    pub const fn tenant_id(&self) -> &TenantId {
        &self.tenant_id
    }

    /// Returns the enclosing durable run identifier.
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the enclosing durable conversation thread identifier.
    #[must_use]
    pub const fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    /// Returns the stable logical tool invocation identifier.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the exact physical execution-attempt identifier.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the immutable owner-qualified tool version.
    #[must_use]
    pub const fn tool(&self) -> &CapabilityIdentity {
        &self.tool
    }

    /// Returns the descriptor-declared idempotency mechanism.
    #[must_use]
    pub const fn idempotency(&self) -> ToolIdempotency {
        self.idempotency
    }

    /// Returns the stable key when this exact tool version requires one.
    #[must_use]
    pub const fn idempotency_key(&self) -> Option<ToolIdempotencyKey> {
        self.idempotency_key
    }

    /// Returns the required stable idempotency key.
    ///
    /// # Errors
    ///
    /// Returns [`ToolContextBindingError::IdempotencyKeyNotRequired`] when the
    /// descriptor uses another mechanism.
    pub const fn required_idempotency_key(
        &self,
    ) -> Result<ToolIdempotencyKey, ToolContextBindingError> {
        match self.idempotency_key {
            Some(key) => Ok(key),
            None => Err(ToolContextBindingError::IdempotencyKeyNotRequired {
                idempotency: self.idempotency,
            }),
        }
    }

    /// Returns the finite remaining run capacity captured for this attempt.
    #[must_use]
    pub const fn budget(&self) -> &BudgetRemaining {
        &self.budget
    }

    /// Returns the wall-clock observation paired with the monotonic clock.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Returns the intersected durable wall-clock deadline.
    #[must_use]
    pub const fn deadline(&self) -> Timestamp {
        self.deadline
    }

    /// Returns the equivalent process-local monotonic deadline.
    #[must_use]
    pub const fn deadline_instant(&self) -> Instant {
        self.deadline_instant
    }

    /// Returns the already narrowed positive invocation timeout.
    #[must_use]
    pub const fn effective_timeout(&self) -> DurationMillis {
        self.effective_timeout
    }

    /// Returns the cooperative cancellation signal.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }

    /// Returns the ordered progress reporter when the runtime supplied a sink.
    #[must_use]
    pub const fn progress(&self) -> Option<&ToolProgressReporter> {
        self.progress.as_ref()
    }

    /// Returns remaining monotonic time, or `None` at and after the deadline.
    #[must_use]
    pub fn remaining_time_at(&self, observed_instant: Instant) -> Option<Duration> {
        self.deadline_instant
            .checked_duration_since(observed_instant)
            .filter(|remaining| !remaining.is_zero())
    }

    /// Returns the deterministic stop reason at one monotonic observation.
    ///
    /// Cancellation wins when both cancellation and deadline are observable.
    #[must_use]
    pub fn stop_reason_at(&self, observed_instant: Instant) -> Option<ToolStopReason> {
        if self.cancellation.is_cancelled() {
            Some(ToolStopReason::Cancelled)
        } else if self.remaining_time_at(observed_instant).is_none() {
            Some(ToolStopReason::DeadlineExceeded)
        } else {
            None
        }
    }

    /// Rebinds this context to the immutable descriptor used by an executor.
    ///
    /// # Errors
    ///
    /// Returns [`ToolContextBindingError`] if identity, idempotency, or timeout
    /// differs from the descriptor snapshot.
    pub fn validate_for(&self, descriptor: &ToolDescriptor) -> Result<(), ToolContextBindingError> {
        let expected_tool = descriptor.metadata().identity();
        if &self.tool != expected_tool {
            return Err(ToolContextBindingError::ToolIdentityMismatch {
                expected: Box::new(expected_tool.clone()),
                actual: Box::new(self.tool.clone()),
            });
        }
        let expected_idempotency = descriptor.semantics().idempotency();
        if self.idempotency != expected_idempotency {
            return Err(ToolContextBindingError::IdempotencyMismatch {
                expected: expected_idempotency,
                actual: self.idempotency,
            });
        }
        if self.effective_timeout > descriptor.limits().timeout() {
            return Err(ToolContextBindingError::TimeoutExceedsDescriptor {
                descriptor: descriptor.limits().timeout(),
                actual: self.effective_timeout,
            });
        }
        let key_is_valid = matches!(
            (self.idempotency, self.idempotency_key),
            (ToolIdempotency::RequiredKey, Some(key)) if key.invocation_id() == self.invocation_id
        ) || (self.idempotency != ToolIdempotency::RequiredKey
            && self.idempotency_key.is_none());
        if !key_is_valid {
            return Err(ToolContextBindingError::InvalidIdempotencyKeyBinding);
        }
        if let Some(progress) = &self.progress {
            let maximum = descriptor.invocation().max_progress_events();
            if maximum == ExecutionCount::ZERO || progress.maximum_events() != maximum {
                return Err(ToolContextBindingError::ProgressLimitMismatch {
                    expected: maximum,
                    actual: progress.maximum_events(),
                });
            }
            if progress.provenance().invocation_id() != self.invocation_id
                || progress.provenance().attempt_id() != self.attempt_id
                || progress.provenance().tool() != &self.tool
            {
                return Err(ToolContextBindingError::ProgressProvenanceMismatch);
            }
        }
        Ok(())
    }
}

impl fmt::Debug for ToolContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolContext")
            .field("tenant_id", &self.tenant_id)
            .field("run_id", &self.run_id)
            .field("thread_id", &self.thread_id)
            .field("invocation_id", &self.invocation_id)
            .field("attempt_id", &self.attempt_id)
            .field("tool", &self.tool)
            .field("idempotency", &self.idempotency)
            .field("has_idempotency_key", &self.idempotency_key.is_some())
            .field("observed_at", &self.observed_at)
            .field("deadline", &self.deadline)
            .field("effective_timeout", &self.effective_timeout)
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("progress_enabled", &self.progress.is_some())
            .finish_non_exhaustive()
    }
}

/// Failure to construct a finite tool-attempt context.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolContextError {
    /// An effective zero timeout would make the attempt immediately ineligible.
    #[error("tool context effective timeout must be greater than zero")]
    ZeroEffectiveTimeout,
    /// A runtime layer attempted to widen the immutable tool timeout.
    #[error("tool context timeout {actual}ms exceeds descriptor timeout {descriptor}ms")]
    TimeoutExceedsDescriptor {
        /// Immutable descriptor ceiling.
        descriptor: DurationMillis,
        /// Rejected effective timeout.
        actual: DurationMillis,
    },
    /// The wall-clock observation reached or passed the run deadline.
    #[error("tool context deadline {deadline} was reached at {observed_at}")]
    DeadlineReached {
        /// Durable run deadline.
        deadline: Timestamp,
        /// Wall-clock observation used to construct the context.
        observed_at: Timestamp,
    },
    /// The platform monotonic clock could not represent the finite deadline.
    #[error("tool context monotonic deadline is out of range after {remaining:?}")]
    MonotonicDeadlineOutOfRange {
        /// Positive duration between the paired observation and deadline.
        remaining: Duration,
    },
    /// A progress sink was supplied to a descriptor that forbids progress.
    #[error("tool descriptor does not allow progress events")]
    ProgressUnsupported,
}

/// Invalid relationship between a tool context and descriptor snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolContextBindingError {
    /// The context names a different owner-qualified tool version.
    #[error("tool context identity {actual:?} does not match descriptor {expected:?}")]
    ToolIdentityMismatch {
        /// Exact descriptor identity.
        expected: Box<CapabilityIdentity>,
        /// Rejected context identity.
        actual: Box<CapabilityIdentity>,
    },
    /// The captured idempotency mechanism differs from the descriptor.
    #[error("tool context idempotency {actual:?} does not match descriptor {expected:?}")]
    IdempotencyMismatch {
        /// Exact descriptor mechanism.
        expected: ToolIdempotency,
        /// Rejected context mechanism.
        actual: ToolIdempotency,
    },
    /// A decoded or substituted context widened the timeout ceiling.
    #[error("tool context timeout {actual}ms exceeds descriptor timeout {descriptor}ms")]
    TimeoutExceedsDescriptor {
        /// Immutable descriptor ceiling.
        descriptor: DurationMillis,
        /// Rejected context timeout.
        actual: DurationMillis,
    },
    /// The stable key was absent, unexpected, or derived from another invocation.
    #[error("tool context idempotency key is not bound to its logical invocation")]
    InvalidIdempotencyKeyBinding,
    /// The caller requested a key for a mechanism that does not use one.
    #[error("tool idempotency mechanism {idempotency:?} does not require a key")]
    IdempotencyKeyNotRequired {
        /// Descriptor mechanism that does not use a runtime-supplied key.
        idempotency: ToolIdempotency,
    },
    /// The reporter ceiling differed from the immutable descriptor snapshot.
    #[error("tool progress maximum {actual} does not match descriptor {expected}")]
    ProgressLimitMismatch {
        /// Descriptor progress-event ceiling.
        expected: ExecutionCount,
        /// Rejected reporter ceiling.
        actual: ExecutionCount,
    },
    /// The reporter was bound to another invocation, attempt, or tool version.
    #[error("tool progress reporter provenance does not match its context")]
    ProgressProvenanceMismatch,
}

/// One normalized numeric progress observation requested by a tool.
///
/// Units are tool-defined by its registered contract. A known `total` is
/// positive and cannot be smaller than `completed`. The reporter enforces
/// strict monotonicity across observations and freezes a total once declared.
#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolProgressUpdate {
    completed: ExecutionCount,
    #[serde(skip_serializing_if = "Option::is_none")]
    total: Option<ExecutionCount>,
}

impl ToolProgressUpdate {
    /// Constructs a structurally valid progress observation.
    ///
    /// # Errors
    ///
    /// Returns [`ToolProgressUpdateError`] for a zero total or completed value
    /// greater than the known total.
    pub const fn new(
        completed: ExecutionCount,
        total: Option<ExecutionCount>,
    ) -> Result<Self, ToolProgressUpdateError> {
        if let Some(total) = total {
            if total.get() == 0 {
                return Err(ToolProgressUpdateError::ZeroTotal);
            }
            if completed.get() > total.get() {
                return Err(ToolProgressUpdateError::CompletedExceedsTotal { completed, total });
            }
        }
        Ok(Self { completed, total })
    }

    /// Returns completed tool-defined units.
    #[must_use]
    pub const fn completed(self) -> ExecutionCount {
        self.completed
    }

    /// Returns the known total tool-defined units, when declared.
    #[must_use]
    pub const fn total(self) -> Option<ExecutionCount> {
        self.total
    }
}

impl<'de> Deserialize<'de> for ToolProgressUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            completed: ExecutionCount,
            total: Option<ExecutionCount>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.completed, wire.total).map_err(de::Error::custom)
    }
}

/// Invalid structural progress observation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolProgressUpdateError {
    /// A known total cannot be zero.
    #[error("tool progress total must be greater than zero")]
    ZeroTotal,
    /// Completed units exceeded the known total.
    #[error("tool progress completed {completed} exceeds total {total}")]
    CompletedExceedsTotal {
        /// Rejected completed units.
        completed: ExecutionCount,
        /// Known total units.
        total: ExecutionCount,
    },
}

/// Stable identity attached to every progress event from one attempt.
#[derive(Clone, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolProgressProvenance {
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    tool: CapabilityIdentity,
}

impl ToolProgressProvenance {
    /// Constructs exact progress provenance.
    #[must_use]
    pub const fn new(
        invocation_id: InvocationId,
        attempt_id: AttemptId,
        tool: CapabilityIdentity,
    ) -> Self {
        Self {
            invocation_id,
            attempt_id,
            tool,
        }
    }

    /// Returns the stable logical invocation identifier.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the physical attempt emitting progress.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the exact owner-qualified tool version.
    #[must_use]
    pub const fn tool(&self) -> &CapabilityIdentity {
        &self.tool
    }
}

impl fmt::Debug for ToolProgressProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolProgressProvenance")
            .field("invocation_id", &self.invocation_id)
            .field("attempt_id", &self.attempt_id)
            .field("tool", &self.tool)
            .finish_non_exhaustive()
    }
}

/// Ordered, identity-bound progress event accepted by the runtime sink.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolProgressEvent {
    provenance: ToolProgressProvenance,
    sequence: ExecutionCount,
    update: ToolProgressUpdate,
}

impl ToolProgressEvent {
    /// Constructs an event from reporter-assigned identity and sequence.
    #[must_use]
    pub const fn new(
        provenance: ToolProgressProvenance,
        sequence: ExecutionCount,
        update: ToolProgressUpdate,
    ) -> Self {
        Self {
            provenance,
            sequence,
            update,
        }
    }

    /// Returns exact invocation provenance.
    #[must_use]
    pub const fn provenance(&self) -> &ToolProgressProvenance {
        &self.provenance
    }

    /// Returns the contiguous zero-based event sequence.
    #[must_use]
    pub const fn sequence(&self) -> ExecutionCount {
        self.sequence
    }

    /// Returns the normalized numeric update.
    #[must_use]
    pub const fn update(&self) -> ToolProgressUpdate {
        self.update
    }

    /// Revalidates identity and the descriptor event ceiling.
    ///
    /// Contiguous ordering and monotonic progress across multiple events remain
    /// the reporter or durable replay accumulator's responsibility.
    ///
    /// # Errors
    ///
    /// Returns [`ToolProgressEventValidationError`] for substituted context,
    /// invocation, attempt, tool, or an impossible sequence.
    pub fn validate_for(
        &self,
        context: &ToolContext,
        descriptor: &ToolDescriptor,
    ) -> Result<(), ToolProgressEventValidationError> {
        context
            .validate_for(descriptor)
            .map_err(ToolProgressEventValidationError::context)?;
        if self.provenance.invocation_id != context.invocation_id() {
            return Err(ToolProgressEventValidationError::InvocationMismatch {
                expected: context.invocation_id(),
                actual: self.provenance.invocation_id,
            });
        }
        if self.provenance.attempt_id != context.attempt_id() {
            return Err(ToolProgressEventValidationError::AttemptMismatch {
                expected: context.attempt_id(),
                actual: self.provenance.attempt_id,
            });
        }
        let expected_tool = descriptor.metadata().identity();
        if &self.provenance.tool != expected_tool {
            return Err(ToolProgressEventValidationError::ToolIdentityMismatch {
                expected: Box::new(expected_tool.clone()),
                actual: Box::new(self.provenance.tool.clone()),
            });
        }
        let maximum = descriptor.invocation().max_progress_events();
        if self.sequence >= maximum {
            return Err(ToolProgressEventValidationError::SequenceLimitExceeded {
                maximum,
                actual: self.sequence,
            });
        }
        Ok(())
    }
}

/// Invalid relationship between a progress event and invocation snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolProgressEventValidationError {
    /// Context identity or invocation semantics did not match the descriptor.
    #[error("tool progress context binding is invalid: {source}")]
    Context {
        /// Underlying context binding failure.
        #[source]
        source: ToolContextBindingError,
    },
    /// Event named another logical invocation.
    #[error("tool progress invocation {actual} does not match context {expected}")]
    InvocationMismatch {
        /// Exact context invocation.
        expected: InvocationId,
        /// Rejected event invocation.
        actual: InvocationId,
    },
    /// Event named another physical attempt.
    #[error("tool progress attempt {actual} does not match context {expected}")]
    AttemptMismatch {
        /// Exact context attempt.
        expected: AttemptId,
        /// Rejected event attempt.
        actual: AttemptId,
    },
    /// Event named another owner-qualified tool version.
    #[error("tool progress identity {actual:?} does not match descriptor {expected:?}")]
    ToolIdentityMismatch {
        /// Exact descriptor identity.
        expected: Box<CapabilityIdentity>,
        /// Rejected event identity.
        actual: Box<CapabilityIdentity>,
    },
    /// Sequence lies outside the descriptor's zero-based event capacity.
    #[error("tool progress sequence {actual} exceeds event capacity {maximum}")]
    SequenceLimitExceeded {
        /// Descriptor event-count ceiling.
        maximum: ExecutionCount,
        /// Rejected zero-based sequence.
        actual: ExecutionCount,
    },
}

impl ToolProgressEventValidationError {
    const fn context(source: ToolContextBindingError) -> Self {
        Self::Context { source }
    }
}

type PrivateProgressSinkSource = dyn StdError + Send + Sync + 'static;

/// Private diagnostic returned by a runtime progress sink.
#[derive(Clone)]
pub struct ToolProgressSinkError {
    private_source: Arc<PrivateProgressSinkSource>,
}

impl ToolProgressSinkError {
    /// Wraps one private sink diagnostic.
    #[must_use]
    pub fn new<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            private_source: Arc::new(source),
        }
    }

    /// Returns the private diagnostic to trusted in-process callers only.
    #[must_use]
    pub fn private_source(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self.private_source.as_ref()
    }
}

impl fmt::Debug for ToolProgressSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolProgressSinkError")
            .field("has_private_source", &true)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ToolProgressSinkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("tool progress sink failed")
    }
}

impl StdError for ToolProgressSinkError {}

/// Runtime-owned durable sink for already ordered tool progress events.
///
/// A successful future asserts that the sink accepted the event in sequence.
/// Implementations must not reorder events and must apply their own durable
/// fencing before exposing them externally.
pub trait ToolProgressSink: Send + Sync + 'static {
    /// Accepts one identity-bound progress event.
    fn emit(
        &self,
        event: ToolProgressEvent,
    ) -> crate::BoxFuture<'_, Result<(), ToolProgressSinkError>>;
}

#[derive(Debug)]
struct ToolProgressState {
    next_sequence: ExecutionCount,
    last_completed: Option<ExecutionCount>,
    total: Option<ExecutionCount>,
    in_flight: bool,
    poisoned: bool,
}

/// Cloneable ordered progress handle bound to one physical attempt.
///
/// Concurrent emissions are rejected because async completion order cannot be
/// inferred safely. Dropping an in-flight emission future or observing a sink
/// failure permanently poisons the reporter, preventing gaps from being hidden
/// by later events.
#[derive(Clone)]
pub struct ToolProgressReporter {
    provenance: ToolProgressProvenance,
    maximum_events: ExecutionCount,
    sink: Arc<dyn ToolProgressSink>,
    state: Arc<Mutex<ToolProgressState>>,
}

impl ToolProgressReporter {
    fn new(
        provenance: ToolProgressProvenance,
        maximum_events: ExecutionCount,
        sink: Arc<dyn ToolProgressSink>,
    ) -> Self {
        debug_assert!(maximum_events != ExecutionCount::ZERO);
        Self {
            provenance,
            maximum_events,
            sink,
            state: Arc::new(Mutex::new(ToolProgressState {
                next_sequence: ExecutionCount::ZERO,
                last_completed: None,
                total: None,
                in_flight: false,
                poisoned: false,
            })),
        }
    }

    /// Returns exact invocation provenance attached to emitted events.
    #[must_use]
    pub const fn provenance(&self) -> &ToolProgressProvenance {
        &self.provenance
    }

    /// Returns the finite descriptor event ceiling.
    #[must_use]
    pub const fn maximum_events(&self) -> ExecutionCount {
        self.maximum_events
    }

    /// Emits one strictly increasing progress observation.
    ///
    /// # Errors
    ///
    /// Returns [`ToolProgressError`] for concurrency, non-monotonic values,
    /// total changes, exhaustion, poisoned state, or a runtime sink failure.
    pub fn emit(
        &self,
        update: ToolProgressUpdate,
    ) -> crate::BoxFuture<'_, Result<ToolProgressEvent, ToolProgressError>> {
        Box::pin(async move {
            let (event, mut reservation) = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| ToolProgressError::StatePoisoned)?;
                if state.poisoned {
                    return Err(ToolProgressError::StatePoisoned);
                }
                if state.in_flight {
                    return Err(ToolProgressError::ConcurrentEmission);
                }
                if state.next_sequence >= self.maximum_events {
                    return Err(ToolProgressError::MaximumReached {
                        maximum: self.maximum_events,
                    });
                }
                if let Some(previous) = state.last_completed {
                    if update.completed() <= previous {
                        return Err(ToolProgressError::NotIncreasing {
                            previous,
                            actual: update.completed(),
                        });
                    }
                }
                if let (Some(expected), Some(actual)) = (state.total, update.total()) {
                    if expected != actual {
                        return Err(ToolProgressError::TotalChanged { expected, actual });
                    }
                }
                let total = update.total().or(state.total);
                if let Some(total) = total {
                    if update.completed() > total {
                        return Err(ToolProgressError::CompletedExceedsEstablishedTotal {
                            completed: update.completed(),
                            total,
                        });
                    }
                }
                let normalized = ToolProgressUpdate {
                    completed: update.completed(),
                    total,
                };
                let event = ToolProgressEvent::new(
                    self.provenance.clone(),
                    state.next_sequence,
                    normalized,
                );
                state.in_flight = true;
                (event, ToolProgressReservation::new(Arc::clone(&self.state)))
            };

            match self.sink.emit(event.clone()).await {
                Ok(()) => {
                    reservation.commit(event.update())?;
                    Ok(event)
                }
                Err(source) => {
                    reservation.fail();
                    Err(ToolProgressError::Sink { source })
                }
            }
        })
    }
}

impl fmt::Debug for ToolProgressReporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolProgressReporter")
            .field("provenance", &self.provenance)
            .field("maximum_events", &self.maximum_events)
            .finish_non_exhaustive()
    }
}

struct ToolProgressReservation {
    state: Arc<Mutex<ToolProgressState>>,
    active: bool,
}

impl ToolProgressReservation {
    fn new(state: Arc<Mutex<ToolProgressState>>) -> Self {
        Self {
            state,
            active: true,
        }
    }

    fn commit(&mut self, update: ToolProgressUpdate) -> Result<(), ToolProgressError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ToolProgressError::StatePoisoned)?;
        state.next_sequence = state
            .next_sequence
            .checked_add(ExecutionCount::new(1))
            .ok_or(ToolProgressError::StatePoisoned)?;
        state.last_completed = Some(update.completed());
        state.total = update.total();
        state.in_flight = false;
        self.active = false;
        Ok(())
    }

    fn fail(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.in_flight = false;
            state.poisoned = true;
        }
        self.active = false;
    }
}

impl Drop for ToolProgressReservation {
    fn drop(&mut self) {
        if self.active {
            self.fail();
        }
    }
}

/// Failure to emit an ordered tool progress event.
#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum ToolProgressError {
    /// A previous sink failure, dropped future, or mutex panic poisoned ordering.
    #[error("tool progress reporter is poisoned")]
    StatePoisoned,
    /// Another progress emission has not completed.
    #[error("concurrent tool progress emission is not allowed")]
    ConcurrentEmission,
    /// The finite descriptor event ceiling was reached.
    #[error("tool progress event maximum {maximum} was reached")]
    MaximumReached {
        /// Descriptor progress-event ceiling.
        maximum: ExecutionCount,
    },
    /// Completed units did not strictly increase.
    #[error("tool progress completed {actual} does not exceed previous {previous}")]
    NotIncreasing {
        /// Last accepted completed units.
        previous: ExecutionCount,
        /// Rejected completed units.
        actual: ExecutionCount,
    },
    /// A tool tried to change a previously established total.
    #[error("tool progress total {actual} does not match established {expected}")]
    TotalChanged {
        /// Previously established total.
        expected: ExecutionCount,
        /// Rejected new total.
        actual: ExecutionCount,
    },
    /// Completed units exceeded a total established by an earlier event.
    #[error("tool progress completed {completed} exceeds established total {total}")]
    CompletedExceedsEstablishedTotal {
        /// Rejected completed units.
        completed: ExecutionCount,
        /// Previously established total.
        total: ExecutionCount,
    },
    /// Runtime progress sink rejected an otherwise valid ordered event.
    #[error("tool progress sink failed")]
    Sink {
        /// Private sink diagnostic.
        source: ToolProgressSinkError,
    },
}

/// Schema-bound, resource-limited arguments for one erased tool invocation.
///
/// Tool arguments must have an object root. Construction provides structural
/// and resource safety but does not execute JSON Schema validation; the trusted
/// [`ToolSchemaRegistry`] performs validation against the digest-pinned schema
/// before typed deserialization and before any tool code runs.
#[derive(Clone, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolInput {
    schema: SchemaReference,
    value: BoundedJson,
}

impl ToolInput {
    /// Constructs object-root arguments from a bounded JSON value.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInputError::ObjectRootRequired`] for a scalar or array root.
    pub fn new(schema: SchemaReference, value: BoundedJson) -> Result<Self, ToolInputError> {
        if !value.as_value().is_object() {
            return Err(ToolInputError::ObjectRootRequired);
        }
        Ok(Self { schema, value })
    }

    /// Returns the immutable input schema identity.
    #[must_use]
    pub const fn schema(&self) -> &SchemaReference {
        &self.schema
    }

    /// Returns the bounded arguments without permitting mutation.
    #[must_use]
    pub const fn value(&self) -> &BoundedJson {
        &self.value
    }

    /// Consumes the input and returns schema identity and arguments.
    #[must_use]
    pub fn into_parts(self) -> (SchemaReference, BoundedJson) {
        (self.schema, self.value)
    }

    /// Revalidates schema identity plus descriptor and run byte ceilings.
    ///
    /// Actual JSON Schema evaluation is deliberately separate and must be
    /// performed through the registered local schema implementation.
    ///
    /// # Errors
    ///
    /// Returns [`ToolInputValidationError`] for a substituted context/schema or
    /// input that exceeds a descriptor or remaining-run ceiling.
    pub fn validate_for(
        &self,
        context: &ToolContext,
        descriptor: &ToolDescriptor,
    ) -> Result<(), ToolInputValidationError> {
        context
            .validate_for(descriptor)
            .map_err(ToolInputValidationError::context)?;
        if &self.schema != descriptor.input_schema() {
            return Err(ToolInputValidationError::SchemaMismatch {
                expected: Box::new(descriptor.input_schema().clone()),
                actual: Box::new(self.schema.clone()),
            });
        }
        let actual = byte_count_from_usize(self.value.stats().compact_bytes());
        let descriptor_maximum = descriptor.limits().max_input_bytes();
        if actual > descriptor_maximum {
            return Err(ToolInputValidationError::DescriptorLimitExceeded {
                maximum: descriptor_maximum,
                actual,
            });
        }
        let budget_maximum = context.budget().input_bytes();
        if actual > budget_maximum {
            return Err(ToolInputValidationError::BudgetLimitExceeded {
                maximum: budget_maximum,
                actual,
            });
        }
        Ok(())
    }
}

impl fmt::Debug for ToolInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolInput")
            .field("schema", &self.schema)
            .field("stats", &self.value.stats())
            .finish_non_exhaustive()
    }
}

impl<'de> Deserialize<'de> for ToolInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            schema: SchemaReference,
            value: BoundedJson,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::new(wire.schema, wire.value).map_err(de::Error::custom)
    }
}

/// Invalid structural tool input.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolInputError {
    /// Registered tool argument schemas and values require an object root.
    #[error("tool input JSON must have an object root")]
    ObjectRootRequired,
}

/// Invalid relationship between tool input and an invocation snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolInputValidationError {
    /// Context identity or invocation semantics did not match the descriptor.
    #[error("tool input context binding is invalid: {source}")]
    Context {
        /// Underlying context binding failure.
        #[source]
        source: ToolContextBindingError,
    },
    /// Input named a schema other than the descriptor's pinned schema.
    #[error("tool input schema {actual:?} does not match descriptor {expected:?}")]
    SchemaMismatch {
        /// Exact descriptor schema.
        expected: Box<SchemaReference>,
        /// Rejected input schema.
        actual: Box<SchemaReference>,
    },
    /// Compact arguments exceeded the immutable descriptor ceiling.
    #[error("tool input is {actual} bytes; descriptor maximum is {maximum}")]
    DescriptorLimitExceeded {
        /// Descriptor input-byte ceiling.
        maximum: ByteCount,
        /// Exact compact input size.
        actual: ByteCount,
    },
    /// Compact arguments exceeded the remaining run capacity.
    #[error("tool input is {actual} bytes; remaining run maximum is {maximum}")]
    BudgetLimitExceeded {
        /// Remaining run input-byte capacity.
        maximum: ByteCount,
        /// Exact compact input size.
        actual: ByteCount,
    },
}

impl ToolInputValidationError {
    const fn context(source: ToolContextBindingError) -> Self {
        Self::Context { source }
    }
}

/// Canonical bounded set of artifact references returned by one tool call.
///
/// The global limit protects generic deserialization before a descriptor can
/// be selected. Descriptor and remaining-budget ceilings can only narrow it.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct ToolArtifacts {
    values: Box<[ArtifactRef]>,
    total_bytes: ByteCount,
}

impl ToolArtifacts {
    /// Absolute v1 artifact-reference count for one tool result.
    pub const MAX_LEN: usize = 64;

    /// Constructs an empty artifact set.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Validates count, identity uniqueness, and aggregate byte arithmetic.
    ///
    /// # Errors
    ///
    /// Returns [`ToolArtifactsError`] for too many artifacts, duplicate
    /// tenant-qualified identities, or byte-count overflow.
    pub fn try_new<I>(values: I) -> Result<Self, ToolArtifactsError>
    where
        I: IntoIterator<Item = ArtifactRef>,
    {
        let mut collected = Vec::new();
        let mut total_bytes = ByteCount::ZERO;
        for value in values {
            if collected.len() == Self::MAX_LEN {
                return Err(ToolArtifactsError::TooMany {
                    maximum: Self::MAX_LEN,
                    actual: Self::MAX_LEN + 1,
                });
            }
            if collected
                .iter()
                .any(|existing: &ArtifactRef| existing.identity() == value.identity())
            {
                return Err(ToolArtifactsError::DuplicateIdentity);
            }
            total_bytes = total_bytes
                .checked_add(value.representation().byte_length())
                .ok_or(ToolArtifactsError::TotalBytesOverflow)?;
            collected.push(value);
        }
        Ok(Self {
            values: collected.into_boxed_slice(),
            total_bytes,
        })
    }

    /// Returns the number of artifact references.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether no artifact references were returned.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the checked aggregate immutable representation size.
    #[must_use]
    pub const fn total_bytes(&self) -> ByteCount {
        self.total_bytes
    }

    /// Iterates over artifact references in their semantic output order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &ArtifactRef> {
        self.values.iter()
    }

    /// Consumes this set and returns its ordered artifact references.
    #[must_use]
    pub fn into_vec(self) -> Vec<ArtifactRef> {
        self.values.into_vec()
    }
}

impl fmt::Debug for ToolArtifacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolArtifacts")
            .field("count", &self.len())
            .field("total_bytes", &self.total_bytes)
            .finish_non_exhaustive()
    }
}

impl Serialize for ToolArtifacts {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ToolArtifacts {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ToolArtifactsVisitor)
    }
}

struct ToolArtifactsVisitor;

impl<'de> de::Visitor<'de> for ToolArtifactsVisitor {
    type Value = ToolArtifacts;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "at most {} unique artifact references",
            ToolArtifacts::MAX_LEN
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
                .min(ToolArtifacts::MAX_LEN),
        );
        while let Some(value) = sequence.next_element::<ArtifactRef>()? {
            if values.len() == ToolArtifacts::MAX_LEN {
                return Err(de::Error::custom(ToolArtifactsError::TooMany {
                    maximum: ToolArtifacts::MAX_LEN,
                    actual: ToolArtifacts::MAX_LEN + 1,
                }));
            }
            if values
                .iter()
                .any(|existing: &ArtifactRef| existing.identity() == value.identity())
            {
                return Err(de::Error::custom(ToolArtifactsError::DuplicateIdentity));
            }
            values.push(value);
        }
        Self::Value::try_new(values).map_err(de::Error::custom)
    }
}

impl JsonSchema for ToolArtifacts {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ToolArtifacts".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ToolArtifacts").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "array",
            "items": generator.subschema_for::<ArtifactRef>(),
            "maxItems": 64,
            "uniqueItems": true,
            "description": "Ordered artifact references with unique tenant-qualified identities. Identity uniqueness is enforced at runtime."
        })
    }
}

/// Invalid artifact collection returned by one tool attempt.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolArtifactsError {
    /// The global pre-descriptor artifact count ceiling was exceeded.
    #[error("tool result has {actual} artifacts; hard maximum is {maximum}")]
    TooMany {
        /// Absolute v1 count ceiling.
        maximum: usize,
        /// First observed count beyond the ceiling.
        actual: usize,
    },
    /// The same tenant-qualified artifact identity appeared more than once.
    #[error("tool result artifact identities must be unique")]
    DuplicateIdentity,
    /// Aggregate representation byte arithmetic overflowed.
    #[error("tool result aggregate artifact bytes overflowed")]
    TotalBytesOverflow,
}

/// Typed output plus independently bounded artifact references.
///
/// The inline value is serialized and validated against the descriptor output
/// schema by [`ToolAdapter`]. Artifact bytes are never embedded here; tools
/// publish immutable bytes through their capability-scoped artifact binding and
/// return only authorized [`ArtifactRef`] values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolOutput<T> {
    value: T,
    artifacts: ToolArtifacts,
}

impl<T> ToolOutput<T> {
    /// Constructs an inline-only typed output.
    #[must_use]
    pub fn inline(value: T) -> Self {
        Self {
            value,
            artifacts: ToolArtifacts::empty(),
        }
    }

    /// Constructs typed output with already bounded artifact references.
    #[must_use]
    pub const fn with_artifacts(value: T, artifacts: ToolArtifacts) -> Self {
        Self { value, artifacts }
    }

    /// Returns the typed inline value.
    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    /// Returns independently bounded artifact references.
    #[must_use]
    pub const fn artifacts(&self) -> &ToolArtifacts {
        &self.artifacts
    }

    /// Consumes this output into inline value and artifacts.
    #[must_use]
    pub fn into_parts(self) -> (T, ToolArtifacts) {
        (self.value, self.artifacts)
    }
}

impl<T> From<T> for ToolOutput<T> {
    fn from(value: T) -> Self {
        Self::inline(value)
    }
}

/// Stable invocation and tool identity attached to a successful tool result.
#[derive(Clone, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResultProvenance {
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    tool: CapabilityIdentity,
}

impl ToolResultProvenance {
    /// Constructs result provenance from exact execution identity.
    #[must_use]
    pub const fn new(
        invocation_id: InvocationId,
        attempt_id: AttemptId,
        tool: CapabilityIdentity,
    ) -> Self {
        Self {
            invocation_id,
            attempt_id,
            tool,
        }
    }

    /// Returns the stable logical invocation identifier.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the physical attempt that produced this result.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the exact owner-qualified tool version.
    #[must_use]
    pub const fn tool(&self) -> &CapabilityIdentity {
        &self.tool
    }
}

impl fmt::Debug for ToolResultProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolResultProvenance")
            .field("invocation_id", &self.invocation_id)
            .field("attempt_id", &self.attempt_id)
            .field("tool", &self.tool)
            .finish_non_exhaustive()
    }
}

/// Successful erased tool result ready for durable validation and commit.
///
/// Construction proves only intrinsic resource safety. Before committing or
/// exposing a decoded value, runtimes must call [`Self::validate_for`] and the
/// trusted schema registry must validate `output` against `output_schema`.
#[derive(Clone, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolResult {
    provenance: ToolResultProvenance,
    output_schema: SchemaReference,
    output: BoundedJson,
    artifacts: ToolArtifacts,
}

impl ToolResult {
    /// Constructs an intrinsically bounded erased result.
    #[must_use]
    pub const fn new(
        provenance: ToolResultProvenance,
        output_schema: SchemaReference,
        output: BoundedJson,
        artifacts: ToolArtifacts,
    ) -> Self {
        Self {
            provenance,
            output_schema,
            output,
            artifacts,
        }
    }

    /// Constructs a result whose identity and schema come from trusted snapshots.
    #[must_use]
    pub fn for_invocation(
        context: &ToolContext,
        descriptor: &ToolDescriptor,
        output: BoundedJson,
        artifacts: ToolArtifacts,
    ) -> Self {
        Self::new(
            ToolResultProvenance::new(
                context.invocation_id(),
                context.attempt_id(),
                descriptor.metadata().identity().clone(),
            ),
            descriptor.output_schema().clone(),
            output,
            artifacts,
        )
    }

    /// Returns stable execution provenance.
    #[must_use]
    pub const fn provenance(&self) -> &ToolResultProvenance {
        &self.provenance
    }

    /// Returns the pinned output schema identity.
    #[must_use]
    pub const fn output_schema(&self) -> &SchemaReference {
        &self.output_schema
    }

    /// Returns the bounded inline output without permitting mutation.
    #[must_use]
    pub const fn output(&self) -> &BoundedJson {
        &self.output
    }

    /// Returns bounded artifact references in semantic output order.
    #[must_use]
    pub const fn artifacts(&self) -> &ToolArtifacts {
        &self.artifacts
    }

    /// Consumes the result into its validated components.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ToolResultProvenance,
        SchemaReference,
        BoundedJson,
        ToolArtifacts,
    ) {
        (
            self.provenance,
            self.output_schema,
            self.output,
            self.artifacts,
        )
    }

    /// Revalidates identity, output, artifacts, descriptor limits, and budget.
    ///
    /// # Errors
    ///
    /// Returns [`ToolResultValidationError`] for substituted provenance/schema,
    /// excessive output, or artifact ownership/provenance that does not belong
    /// to this exact tenant, run, and registered tool version.
    pub fn validate_for(
        &self,
        context: &ToolContext,
        descriptor: &ToolDescriptor,
    ) -> Result<(), ToolResultValidationError> {
        context
            .validate_for(descriptor)
            .map_err(ToolResultValidationError::context)?;
        if self.provenance.invocation_id != context.invocation_id() {
            return Err(ToolResultValidationError::InvocationMismatch {
                expected: context.invocation_id(),
                actual: self.provenance.invocation_id,
            });
        }
        if self.provenance.attempt_id != context.attempt_id() {
            return Err(ToolResultValidationError::AttemptMismatch {
                expected: context.attempt_id(),
                actual: self.provenance.attempt_id,
            });
        }
        let expected_tool = descriptor.metadata().identity();
        if &self.provenance.tool != expected_tool {
            return Err(ToolResultValidationError::ToolIdentityMismatch {
                expected: Box::new(expected_tool.clone()),
                actual: Box::new(self.provenance.tool.clone()),
            });
        }
        if &self.output_schema != descriptor.output_schema() {
            return Err(ToolResultValidationError::SchemaMismatch {
                expected: Box::new(descriptor.output_schema().clone()),
                actual: Box::new(self.output_schema.clone()),
            });
        }

        let output_bytes = byte_count_from_usize(self.output.stats().compact_bytes());
        let descriptor_output_maximum = descriptor.limits().max_inline_result_bytes();
        if output_bytes > descriptor_output_maximum {
            return Err(ToolResultValidationError::InlineDescriptorLimitExceeded {
                maximum: descriptor_output_maximum,
                actual: output_bytes,
            });
        }
        let budget_output_maximum = context.budget().output_bytes();
        if output_bytes > budget_output_maximum {
            return Err(ToolResultValidationError::InlineBudgetLimitExceeded {
                maximum: budget_output_maximum,
                actual: output_bytes,
            });
        }

        let artifact_count = execution_count_from_usize(self.artifacts.len());
        let descriptor_artifact_maximum = descriptor.limits().max_artifacts();
        if artifact_count > descriptor_artifact_maximum {
            return Err(ToolResultValidationError::ArtifactCountLimitExceeded {
                maximum: descriptor_artifact_maximum,
                actual: artifact_count,
            });
        }
        let artifact_bytes = self.artifacts.total_bytes();
        let descriptor_artifact_bytes = descriptor.limits().max_total_artifact_bytes();
        if artifact_bytes > descriptor_artifact_bytes {
            return Err(ToolResultValidationError::ArtifactDescriptorBytesExceeded {
                maximum: descriptor_artifact_bytes,
                actual: artifact_bytes,
            });
        }
        let budget_artifact_bytes = context.budget().artifact_bytes();
        if artifact_bytes > budget_artifact_bytes {
            return Err(ToolResultValidationError::ArtifactBudgetBytesExceeded {
                maximum: budget_artifact_bytes,
                actual: artifact_bytes,
            });
        }

        for (index, artifact) in self.artifacts.iter().enumerate() {
            validate_artifact_binding(index, artifact, context, descriptor)?;
        }
        Ok(())
    }
}

impl fmt::Debug for ToolResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolResult")
            .field("provenance", &self.provenance)
            .field("output_schema", &self.output_schema)
            .field("output_stats", &self.output.stats())
            .field("artifacts", &self.artifacts)
            .finish_non_exhaustive()
    }
}

fn validate_artifact_binding(
    index: usize,
    artifact: &ArtifactRef,
    context: &ToolContext,
    descriptor: &ToolDescriptor,
) -> Result<(), ToolResultValidationError> {
    if artifact.identity().tenant_id() != context.tenant_id() {
        return Err(ToolResultValidationError::ArtifactTenantMismatch {
            index,
            expected: Box::new(context.tenant_id().clone()),
            actual: Box::new(artifact.identity().tenant_id().clone()),
        });
    }
    if artifact.provenance().run_id() != context.run_id() {
        return Err(ToolResultValidationError::ArtifactRunMismatch {
            index,
            expected: context.run_id(),
            actual: artifact.provenance().run_id(),
        });
    }
    let expected_principal = descriptor.metadata().identity().owner();
    if artifact.provenance().principal() != expected_principal {
        return Err(ToolResultValidationError::ArtifactPrincipalMismatch {
            index,
            expected: Box::new(expected_principal.clone()),
            actual: Box::new(artifact.provenance().principal().clone()),
        });
    }
    let expected_capability = descriptor.metadata().identity().capability();
    if artifact.provenance().capability() != Some(expected_capability) {
        return Err(ToolResultValidationError::ArtifactCapabilityMismatch {
            index,
            expected: Box::new(expected_capability.clone()),
            actual: artifact
                .provenance()
                .capability()
                .map(|value| Box::new(value.clone())),
        });
    }
    Ok(())
}

/// Invalid relationship between a successful result and invocation snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolResultValidationError {
    /// Context identity or invocation semantics did not match the descriptor.
    #[error("tool result context binding is invalid: {source}")]
    Context {
        /// Underlying context binding failure.
        #[source]
        source: ToolContextBindingError,
    },
    /// Result named another logical invocation.
    #[error("tool result invocation {actual} does not match context {expected}")]
    InvocationMismatch {
        /// Exact context invocation.
        expected: InvocationId,
        /// Rejected result invocation.
        actual: InvocationId,
    },
    /// Result named another physical attempt.
    #[error("tool result attempt {actual} does not match context {expected}")]
    AttemptMismatch {
        /// Exact context attempt.
        expected: AttemptId,
        /// Rejected result attempt.
        actual: AttemptId,
    },
    /// Result named another owner-qualified tool version.
    #[error("tool result identity {actual:?} does not match descriptor {expected:?}")]
    ToolIdentityMismatch {
        /// Exact descriptor identity.
        expected: Box<CapabilityIdentity>,
        /// Rejected result identity.
        actual: Box<CapabilityIdentity>,
    },
    /// Result named another pinned output schema.
    #[error("tool result schema {actual:?} does not match descriptor {expected:?}")]
    SchemaMismatch {
        /// Exact descriptor schema.
        expected: Box<SchemaReference>,
        /// Rejected result schema.
        actual: Box<SchemaReference>,
    },
    /// Inline output exceeded the descriptor ceiling.
    #[error("tool inline output is {actual} bytes; descriptor maximum is {maximum}")]
    InlineDescriptorLimitExceeded {
        /// Descriptor inline-output ceiling.
        maximum: ByteCount,
        /// Exact compact output size.
        actual: ByteCount,
    },
    /// Inline output exceeded remaining run capacity.
    #[error("tool inline output is {actual} bytes; remaining run maximum is {maximum}")]
    InlineBudgetLimitExceeded {
        /// Remaining run output capacity.
        maximum: ByteCount,
        /// Exact compact output size.
        actual: ByteCount,
    },
    /// Artifact count exceeded the descriptor ceiling.
    #[error("tool result has {actual} artifacts; descriptor maximum is {maximum}")]
    ArtifactCountLimitExceeded {
        /// Descriptor artifact-count ceiling.
        maximum: ExecutionCount,
        /// Actual result count.
        actual: ExecutionCount,
    },
    /// Aggregate artifact bytes exceeded the descriptor ceiling.
    #[error("tool artifacts total {actual} bytes; descriptor maximum is {maximum}")]
    ArtifactDescriptorBytesExceeded {
        /// Descriptor aggregate artifact-byte ceiling.
        maximum: ByteCount,
        /// Actual aggregate artifact bytes.
        actual: ByteCount,
    },
    /// Aggregate artifact bytes exceeded remaining run capacity.
    #[error("tool artifacts total {actual} bytes; remaining run maximum is {maximum}")]
    ArtifactBudgetBytesExceeded {
        /// Remaining run artifact-byte capacity.
        maximum: ByteCount,
        /// Actual aggregate artifact bytes.
        actual: ByteCount,
    },
    /// An artifact belongs to another tenant.
    #[error("tool artifact at index {index} tenant {actual} does not match {expected}")]
    ArtifactTenantMismatch {
        /// Zero-based artifact position.
        index: usize,
        /// Exact invocation tenant.
        expected: Box<TenantId>,
        /// Rejected artifact tenant.
        actual: Box<TenantId>,
    },
    /// An artifact provenance record belongs to another run.
    #[error("tool artifact at index {index} run {actual} does not match {expected}")]
    ArtifactRunMismatch {
        /// Zero-based artifact position.
        index: usize,
        /// Exact invocation run.
        expected: RunId,
        /// Rejected artifact run.
        actual: RunId,
    },
    /// An artifact names another registry principal.
    #[error("tool artifact at index {index} principal does not match the descriptor owner")]
    ArtifactPrincipalMismatch {
        /// Zero-based artifact position.
        index: usize,
        /// Exact descriptor owner.
        expected: Box<PrincipalIdentity>,
        /// Rejected artifact principal.
        actual: Box<PrincipalIdentity>,
    },
    /// An artifact omits or substitutes the producing tool version.
    #[error("tool artifact at index {index} capability does not match the descriptor")]
    ArtifactCapabilityMismatch {
        /// Zero-based artifact position.
        index: usize,
        /// Exact descriptor capability reference.
        expected: Box<CapabilityReference>,
        /// Rejected optional artifact capability.
        actual: Option<Box<CapabilityReference>>,
    },
}

impl ToolResultValidationError {
    const fn context(source: ToolContextBindingError) -> Self {
        Self::Context { source }
    }
}

/// Stage at which one physical tool attempt failed.
///
/// Phase is observation evidence only. It never implies retry safety or an
/// external side-effect outcome; those are represented separately.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorPhase {
    /// Input binding, schema validation, or typed decoding failed before tool code ran.
    Preparation,
    /// The typed tool implementation ran and returned or raised a failure.
    Execution,
    /// A nominal success failed output serialization, schema, or result validation.
    Result,
}

/// Best available evidence about externally observable write effects.
///
/// This is not inferred from HTTP status, transport closure, timeout, or
/// cancellation. `Applied` and `NotApplied` require authoritative adapter/tool
/// evidence. `Unknown` means reconciliation is mandatory before retry or
/// compensation. Read-only tools always use `NotApplicable`.
#[derive(
    Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ToolExternalEffect {
    /// The tool is read-only, so write-effect evidence does not apply.
    NotApplicable,
    /// Tool implementation code was not invoked.
    NotStarted,
    /// Authoritative evidence proves that the intended write was not applied.
    NotApplied,
    /// Authoritative evidence proves that the intended write was applied.
    Applied,
    /// The intended write may or may not have been applied.
    Unknown,
}

/// Stable invocation and tool identity attached to a failed tool attempt.
#[derive(Clone, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ToolErrorProvenance {
    invocation_id: InvocationId,
    attempt_id: AttemptId,
    tool: CapabilityIdentity,
}

impl ToolErrorProvenance {
    /// Constructs failure provenance from exact invocation identity.
    #[must_use]
    pub const fn new(
        invocation_id: InvocationId,
        attempt_id: AttemptId,
        tool: CapabilityIdentity,
    ) -> Self {
        Self {
            invocation_id,
            attempt_id,
            tool,
        }
    }

    /// Constructs provenance from trusted context and descriptor snapshots.
    #[must_use]
    pub fn for_invocation(context: &ToolContext, descriptor: &ToolDescriptor) -> Self {
        Self::new(
            context.invocation_id(),
            context.attempt_id(),
            descriptor.metadata().identity().clone(),
        )
    }

    /// Returns the stable logical invocation identifier.
    #[must_use]
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the failed physical attempt identifier.
    #[must_use]
    pub const fn attempt_id(&self) -> AttemptId {
        self.attempt_id
    }

    /// Returns the exact owner-qualified tool version.
    #[must_use]
    pub const fn tool(&self) -> &CapabilityIdentity {
        &self.tool
    }
}

impl fmt::Debug for ToolErrorProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolErrorProvenance")
            .field("invocation_id", &self.invocation_id)
            .field("attempt_id", &self.attempt_id)
            .field("tool", &self.tool)
            .finish_non_exhaustive()
    }
}

/// Public-safe typed failure returned by a tool implementation or adapter.
///
/// `external_effect` is deliberately independent of failure category. The one
/// exception is an exact invariant: `Unknown` is paired with
/// [`FailureCategory::AmbiguousExternalOutcome`] and therefore with
/// [`RetryAdvice::ReconcileFirst`].
#[derive(Clone)]
pub struct ToolError {
    failure: Failure,
    phase: ToolErrorPhase,
    external_effect: ToolExternalEffect,
    provenance: ToolErrorProvenance,
}

impl ToolError {
    /// Constructs a failure while enforcing phase/effect/ambiguity invariants.
    ///
    /// # Errors
    ///
    /// Returns [`ToolErrorBuildError`] when effect evidence contradicts the
    /// failure category or claims an impossible preparation/result phase.
    pub fn new(
        failure: Failure,
        phase: ToolErrorPhase,
        external_effect: ToolExternalEffect,
        provenance: ToolErrorProvenance,
    ) -> Result<Self, ToolErrorBuildError> {
        validate_tool_error_shape(&failure, phase, external_effect)?;
        Ok(Self {
            failure,
            phase,
            external_effect,
            provenance,
        })
    }

    /// Revalidates invocation, tool identity, risk, and retry safety.
    ///
    /// # Errors
    ///
    /// Returns [`ToolErrorValidationError`] when public evidence does not belong
    /// to the exact context/descriptor or would permit an unsafe repeated write.
    pub fn validate_for(
        &self,
        context: &ToolContext,
        descriptor: &ToolDescriptor,
    ) -> Result<(), ToolErrorValidationError> {
        context
            .validate_for(descriptor)
            .map_err(ToolErrorValidationError::context)?;
        if self.provenance.invocation_id != context.invocation_id() {
            return Err(ToolErrorValidationError::InvocationMismatch {
                expected: context.invocation_id(),
                actual: self.provenance.invocation_id,
            });
        }
        if self.provenance.attempt_id != context.attempt_id() {
            return Err(ToolErrorValidationError::AttemptMismatch {
                expected: context.attempt_id(),
                actual: self.provenance.attempt_id,
            });
        }
        let expected_tool = descriptor.metadata().identity();
        if &self.provenance.tool != expected_tool {
            return Err(ToolErrorValidationError::ToolIdentityMismatch {
                expected: Box::new(expected_tool.clone()),
                actual: Box::new(self.provenance.tool.clone()),
            });
        }

        let risk = descriptor.semantics().risk();
        match (risk, self.external_effect) {
            (ToolRisk::ReadOnly, ToolExternalEffect::NotApplicable)
            | (
                ToolRisk::IdempotentWrite | ToolRisk::NonIdempotentWrite,
                ToolExternalEffect::NotStarted
                | ToolExternalEffect::NotApplied
                | ToolExternalEffect::Applied
                | ToolExternalEffect::Unknown,
            ) => {}
            _ => {
                return Err(ToolErrorValidationError::EffectRiskMismatch {
                    risk,
                    effect: self.external_effect,
                });
            }
        }

        if risk == ToolRisk::NonIdempotentWrite
            && self.external_effect == ToolExternalEffect::Applied
            && matches!(self.failure.retry_advice(), RetryAdvice::SafeAfter { .. })
        {
            return Err(ToolErrorValidationError::UnsafeRetryAfterAppliedNonIdempotentWrite);
        }
        Ok(())
    }

    /// Returns the common public-safe failure occurrence.
    #[must_use]
    pub const fn failure(&self) -> &Failure {
        &self.failure
    }

    /// Consumes this value and returns the common failure occurrence.
    #[must_use]
    pub fn into_failure(self) -> Failure {
        self.failure
    }

    /// Returns the failed boundary stage.
    #[must_use]
    pub const fn phase(&self) -> ToolErrorPhase {
        self.phase
    }

    /// Returns authoritative or explicitly unknown external-effect evidence.
    #[must_use]
    pub const fn external_effect(&self) -> ToolExternalEffect {
        self.external_effect
    }

    /// Returns stable invocation correlation evidence.
    #[must_use]
    pub const fn provenance(&self) -> &ToolErrorProvenance {
        &self.provenance
    }
}

fn validate_tool_error_shape(
    failure: &Failure,
    phase: ToolErrorPhase,
    external_effect: ToolExternalEffect,
) -> Result<(), ToolErrorBuildError> {
    let ambiguous = failure.category() == FailureCategory::AmbiguousExternalOutcome;
    if external_effect == ToolExternalEffect::Unknown && !ambiguous {
        return Err(ToolErrorBuildError::UnknownEffectRequiresAmbiguousFailure);
    }
    if ambiguous && external_effect != ToolExternalEffect::Unknown {
        return Err(ToolErrorBuildError::AmbiguousFailureRequiresUnknownEffect);
    }
    let phase_effect_valid = match phase {
        ToolErrorPhase::Preparation => matches!(
            external_effect,
            ToolExternalEffect::NotApplicable | ToolExternalEffect::NotStarted
        ),
        ToolErrorPhase::Execution => true,
        ToolErrorPhase::Result => matches!(
            external_effect,
            ToolExternalEffect::NotApplicable
                | ToolExternalEffect::Applied
                | ToolExternalEffect::Unknown
        ),
    };
    if !phase_effect_valid {
        return Err(ToolErrorBuildError::PhaseEffectMismatch {
            phase,
            effect: external_effect,
        });
    }
    Ok(())
}

impl fmt::Debug for ToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolError")
            .field("failure", &self.failure)
            .field("phase", &self.phase)
            .field("external_effect", &self.external_effect)
            .field("provenance", &self.provenance)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ToolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.failure, formatter)
    }
}

impl StdError for ToolError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.failure)
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ToolErrorWire {
    failure: Failure,
    phase: ToolErrorPhase,
    external_effect: ToolExternalEffect,
    provenance: ToolErrorProvenance,
}

#[derive(Serialize)]
struct ToolErrorWireRef<'a> {
    failure: &'a Failure,
    phase: ToolErrorPhase,
    external_effect: ToolExternalEffect,
    provenance: &'a ToolErrorProvenance,
}

impl Serialize for ToolError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        ToolErrorWireRef {
            failure: &self.failure,
            phase: self.phase,
            external_effect: self.external_effect,
            provenance: &self.provenance,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ToolError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ToolErrorWire::deserialize(deserializer)?;
        Self::new(
            wire.failure,
            wire.phase,
            wire.external_effect,
            wire.provenance,
        )
        .map_err(de::Error::custom)
    }
}

impl JsonSchema for ToolError {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "ToolError".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::ToolError").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        ToolErrorWire::json_schema(generator)
    }
}

/// Invalid intrinsic relationship within a [`ToolError`].
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolErrorBuildError {
    /// Unknown write outcome must use the common ambiguous failure category.
    #[error("unknown tool effect requires an ambiguous-external-outcome failure")]
    UnknownEffectRequiresAmbiguousFailure,
    /// The common ambiguous category must carry explicit unknown effect evidence.
    #[error("ambiguous-external-outcome failure requires unknown tool effect")]
    AmbiguousFailureRequiresUnknownEffect,
    /// The claimed effect cannot occur at the stated boundary phase.
    #[error("tool error phase {phase:?} is incompatible with effect {effect:?}")]
    PhaseEffectMismatch {
        /// Failed boundary phase.
        phase: ToolErrorPhase,
        /// Contradictory effect evidence.
        effect: ToolExternalEffect,
    },
}

/// Invalid relationship between a tool failure and invocation snapshot.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolErrorValidationError {
    /// Context identity or invocation semantics did not match the descriptor.
    #[error("tool error context binding is invalid: {source}")]
    Context {
        /// Underlying context binding failure.
        #[source]
        source: ToolContextBindingError,
    },
    /// Failure named another logical invocation.
    #[error("tool error invocation {actual} does not match context {expected}")]
    InvocationMismatch {
        /// Exact context invocation.
        expected: InvocationId,
        /// Rejected failure invocation.
        actual: InvocationId,
    },
    /// Failure named another physical attempt.
    #[error("tool error attempt {actual} does not match context {expected}")]
    AttemptMismatch {
        /// Exact context attempt.
        expected: AttemptId,
        /// Rejected failure attempt.
        actual: AttemptId,
    },
    /// Failure named another owner-qualified tool version.
    #[error("tool error identity {actual:?} does not match descriptor {expected:?}")]
    ToolIdentityMismatch {
        /// Exact descriptor identity.
        expected: Box<CapabilityIdentity>,
        /// Rejected failure identity.
        actual: Box<CapabilityIdentity>,
    },
    /// Effect evidence did not match the descriptor risk class.
    #[error("tool risk {risk:?} is incompatible with external effect {effect:?}")]
    EffectRiskMismatch {
        /// Immutable descriptor risk.
        risk: ToolRisk,
        /// Rejected effect evidence.
        effect: ToolExternalEffect,
    },
    /// A repeated non-idempotent write was advised after the effect was applied.
    #[error("an applied non-idempotent write cannot use safe-after retry advice")]
    UnsafeRetryAfterAppliedNonIdempotentWrite,
}

impl ToolErrorValidationError {
    const fn context(source: ToolContextBindingError) -> Self {
        Self::Context { source }
    }
}

type PrivateSchemaSource = dyn StdError + Send + Sync + 'static;

/// Private diagnostic returned by a trusted local tool schema registry.
///
/// Public formatting is intentionally generic because schema engines can
/// include tool arguments, outputs, filesystem paths, or implementation details
/// in their native diagnostics. Trusted diagnostics can retrieve the private
/// source explicitly.
#[derive(Clone)]
pub struct ToolSchemaValidationError {
    private_source: Arc<PrivateSchemaSource>,
}

impl ToolSchemaValidationError {
    /// Wraps one private schema-registry diagnostic.
    #[must_use]
    pub fn new<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self {
            private_source: Arc::new(source),
        }
    }

    /// Wraps a diagnostic already shared by the trusted registry.
    #[must_use]
    pub fn from_shared(source: Arc<PrivateSchemaSource>) -> Self {
        Self {
            private_source: source,
        }
    }

    /// Returns the private diagnostic to trusted in-process callers only.
    #[must_use]
    pub fn private_source(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self.private_source.as_ref()
    }
}

impl fmt::Debug for ToolSchemaValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolSchemaValidationError")
            .field("has_private_source", &true)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for ToolSchemaValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("tool schema validation failed")
    }
}

impl StdError for ToolSchemaValidationError {}

/// Whether a schema participates in typed input or output validation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ToolSchemaRole {
    /// Object-root input schema validated before tool code runs.
    Input,
    /// Output schema validated after typed serialization.
    Output,
}

/// Trusted offline registry for digest-pinned tool JSON schemas.
///
/// `validate_type_schema` runs during adapter construction. Implementations
/// resolve `reference` only from local configuration, verify its canonical
/// bytes and digest, require JSON Schema 2020-12, require an object-root input,
/// and prove that the generated Rust type schema is compatible with the pinned
/// contract. `validate_instance` evaluates a bounded value against that exact
/// schema without network access or schema-supplied code execution.
pub trait ToolSchemaRegistry: Send + Sync + 'static {
    /// Validates one generated Rust type schema against its pinned contract.
    fn validate_type_schema(
        &self,
        reference: &SchemaReference,
        role: ToolSchemaRole,
        generated: &Schema,
    ) -> Result<(), ToolSchemaValidationError>;

    /// Validates one bounded runtime instance against its pinned contract.
    fn validate_instance(
        &self,
        reference: &SchemaReference,
        role: ToolSchemaRole,
        value: &BoundedJson,
    ) -> Result<(), ToolSchemaValidationError>;
}

/// Strongly typed application-authoring boundary for one immutable tool version.
///
/// Returning `Ok` is an authoritative assertion that the intended tool
/// operation completed; for a write, post-call serialization/schema failures
/// therefore carry `Applied` effect evidence. Returning `Err` must truthfully
/// report effect evidence. Implementations must not hide retries beneath one
/// [`AttemptId`]; every actual external exchange requires a separately admitted
/// and budgeted attempt, with the same logical [`InvocationId`] and idempotency
/// key when applicable.
pub trait Tool: Send + Sync + 'static {
    /// Typed object-root arguments.
    type Input: DeserializeOwned + JsonSchema + Send + 'static;
    /// Typed inline result governed by the descriptor output schema.
    type Output: Serialize + JsonSchema + Send + 'static;

    /// Returns the immutable descriptor for this exact registered tool version.
    fn descriptor(&self) -> &ToolDescriptor;

    /// Executes exactly one physical attempt.
    fn call(
        &self,
        context: ToolContext,
        input: Self::Input,
    ) -> crate::BoxFuture<'_, Result<ToolOutput<Self::Output>, ToolError>>;
}

/// One bounded observation produced by a tool reconciliation probe.
///
/// Result and error evidence must describe the original physical attempt; the
/// durable runtime revalidates that binding before commit. `Pending` means the
/// remote system supplied no authoritative terminal fact and therefore causes
/// no invocation-ledger mutation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ToolReconciliationObservation {
    /// Authoritative successful evidence for the original attempt.
    Result(ToolResult),
    /// Authoritative failure/effect evidence for the original attempt.
    Error(ToolError),
    /// No authoritative outcome is available yet.
    Pending {
        /// Minimum delay before the next bounded probe.
        retry_after: DurationMillis,
    },
}

impl ToolReconciliationObservation {
    /// Maximum provider-selected polling interval accepted by the core API.
    pub const MAX_RETRY_AFTER: DurationMillis = match DurationMillis::new(3_600_000) {
        Ok(value) => value,
        Err(_) => panic!("one hour is a valid duration"),
    };

    /// Constructs a positive, bounded pending observation.
    ///
    /// # Errors
    ///
    /// Rejects zero and intervals greater than one hour. Longer waits are
    /// represented by repeated durable probes so cancellation and leases stay
    /// observable.
    pub fn pending(
        retry_after: DurationMillis,
    ) -> Result<Self, ToolReconciliationObservationError> {
        validate_reconciliation_retry_after(retry_after)?;
        Ok(Self::Pending { retry_after })
    }

    /// Revalidates provider-authored polling bounds.
    ///
    /// Runtimes call this even when a provider constructed the public enum
    /// variant directly.
    pub fn validate(&self) -> Result<(), ToolReconciliationObservationError> {
        match self {
            Self::Result(_) | Self::Error(_) => Ok(()),
            Self::Pending { retry_after } => validate_reconciliation_retry_after(*retry_after),
        }
    }
}

fn validate_reconciliation_retry_after(
    retry_after: DurationMillis,
) -> Result<(), ToolReconciliationObservationError> {
    if retry_after == DurationMillis::ZERO {
        return Err(ToolReconciliationObservationError::ZeroRetryDelay);
    }
    if retry_after > ToolReconciliationObservation::MAX_RETRY_AFTER {
        return Err(ToolReconciliationObservationError::RetryDelayTooLarge {
            maximum: ToolReconciliationObservation::MAX_RETRY_AFTER,
            actual: retry_after,
        });
    }
    Ok(())
}

/// Invalid polling advice returned by a reconciliation provider.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolReconciliationObservationError {
    /// An immediate durable retry would create a hot loop.
    #[error("tool reconciliation retry delay must be greater than zero")]
    ZeroRetryDelay,
    /// Provider advice exceeded the bounded polling ceiling.
    #[error("tool reconciliation retry delay {actual}ms exceeds maximum {maximum}ms")]
    RetryDelayTooLarge {
        /// Maximum accepted delay.
        maximum: DurationMillis,
        /// Rejected delay.
        actual: DurationMillis,
    },
}

/// Public-safe failure of the reconciliation probe itself.
///
/// This is not evidence about the original tool outcome. In particular it may
/// never request `ReconcileFirst`, which would recursively turn a read/replay
/// probe failure into another ambiguous business operation.
#[derive(Clone, Debug, Error)]
#[error("tool reconciliation probe failed: {failure}")]
pub struct ToolReconciliationProbeError {
    failure: Box<Failure>,
}

impl ToolReconciliationProbeError {
    /// Wraps a public-safe probe failure with finite recovery advice.
    ///
    /// # Errors
    ///
    /// Rejects reconcile-first advice and unbounded or zero safe-after delays.
    pub fn new(failure: Failure) -> Result<Self, ToolReconciliationProbeErrorBuildError> {
        if failure.retry_advice().requires_reconciliation() {
            return Err(ToolReconciliationProbeErrorBuildError::RecursiveReconciliation);
        }
        if let Some(delay) = failure.retry_advice().safe_after_delay() {
            validate_reconciliation_retry_after(delay)
                .map_err(ToolReconciliationProbeErrorBuildError::invalid_retry)?;
        }
        Ok(Self {
            failure: Box::new(failure),
        })
    }

    /// Returns the public-safe probe failure.
    #[must_use]
    pub fn failure(&self) -> &Failure {
        self.failure.as_ref()
    }

    fn unsupported() -> Self {
        let failure = Failure::new(
            FailureId::generate(),
            FailureCategory::Unsupported,
            FailureCode::new("stateknot.tool.reconciliation_unsupported")
                .expect("static reconciliation failure code is valid"),
            FailureOrigin::new("stateknot.tool")
                .expect("static reconciliation failure origin is valid"),
            FailureMessage::new("This tool does not implement reconciliation.")
                .expect("static reconciliation failure message is valid"),
            RetryAdvice::Never,
        )
        .expect("static reconciliation failure semantics are coherent");
        Self {
            failure: Box::new(failure),
        }
    }
}

/// Invalid recovery advice attached to a reconciliation-probe failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolReconciliationProbeErrorBuildError {
    /// A probe failure cannot itself claim an ambiguous business outcome.
    #[error("tool reconciliation probe failure cannot require reconciliation")]
    RecursiveReconciliation,
    /// Safe retry advice violated polling bounds.
    #[error("tool reconciliation probe retry advice is invalid: {source}")]
    InvalidRetry {
        /// Exact polling-bound violation.
        #[source]
        source: ToolReconciliationObservationError,
    },
}

impl ToolReconciliationProbeErrorBuildError {
    const fn invalid_retry(source: ToolReconciliationObservationError) -> Self {
        Self::InvalidRetry { source }
    }
}

/// Object-safe tool boundary used by heterogeneous registries and runtimes.
///
/// Only adapters that have passed typed schema registration should enter an
/// executable registry. A successful future resolves to a fully bounded result;
/// the runtime still persists and fences the result before it becomes committed.
pub trait ErasedTool: Send + Sync + 'static {
    /// Returns the immutable descriptor snapshot used by this adapter.
    fn descriptor(&self) -> &ToolDescriptor;

    /// Returns whether this executable binding implements reconciliation.
    ///
    /// Immutable registries reject an implementation claim that was not
    /// declared by the descriptor. A descriptor may still declare a status
    /// query while leaving execution to a separately authorized manual
    /// reconciler, in which case this returns `false`.
    #[must_use]
    fn supports_reconciliation(&self) -> bool {
        false
    }

    /// Validates, decodes, executes, encodes, and revalidates one attempt.
    fn call(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> crate::BoxFuture<'_, Result<ToolResult, ToolError>>;

    /// Reconciles the outcome of the original ambiguous physical attempt.
    ///
    /// Implementations must query authoritative provider state or perform an
    /// operator-attested deduplicated replay. They must never issue an
    /// unprotected duplicate write. The default fails closed so older and
    /// ordinary tools cannot accidentally claim recovery support.
    fn reconcile(
        &self,
        _context: ToolReconciliationContext,
        _input: ToolInput,
    ) -> crate::BoxFuture<'_, Result<ToolReconciliationObservation, ToolReconciliationProbeError>>
    {
        Box::pin(async { Err(ToolReconciliationProbeError::unsupported()) })
    }
}

/// Framework-owned typed-to-erased adapter with pinned schema enforcement.
pub struct ToolAdapter<T, R> {
    tool: T,
    descriptor: ToolDescriptor,
    registry: R,
}

impl<T, R> ToolAdapter<T, R>
where
    T: Tool,
    R: ToolSchemaRegistry,
{
    /// Validates generated type schemas and freezes the descriptor snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ToolAdapterBuildError`] unless both generated Rust type schemas
    /// match their digest-pinned local registry contracts.
    pub fn new(tool: T, registry: R) -> Result<Self, ToolAdapterBuildError> {
        let descriptor = tool.descriptor().clone();
        let input_schema = SchemaGenerator::default().into_root_schema_for::<T::Input>();
        registry
            .validate_type_schema(
                descriptor.input_schema(),
                ToolSchemaRole::Input,
                &input_schema,
            )
            .map_err(|source| ToolAdapterBuildError::SchemaContract {
                role: ToolSchemaRole::Input,
                source,
            })?;
        let output_schema = SchemaGenerator::default().into_root_schema_for::<T::Output>();
        registry
            .validate_type_schema(
                descriptor.output_schema(),
                ToolSchemaRole::Output,
                &output_schema,
            )
            .map_err(|source| ToolAdapterBuildError::SchemaContract {
                role: ToolSchemaRole::Output,
                source,
            })?;
        Ok(Self {
            tool,
            descriptor,
            registry,
        })
    }

    /// Returns the strongly typed implementation.
    #[must_use]
    pub const fn tool(&self) -> &T {
        &self.tool
    }

    /// Returns the trusted local schema registry binding.
    #[must_use]
    pub const fn registry(&self) -> &R {
        &self.registry
    }

    /// Consumes the adapter into the typed tool and schema registry.
    #[must_use]
    pub fn into_inner(self) -> (T, R) {
        (self.tool, self.registry)
    }
}

impl<T, R> ErasedTool for ToolAdapter<T, R>
where
    T: Tool,
    R: ToolSchemaRegistry,
{
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn call(
        &self,
        context: ToolContext,
        input: ToolInput,
    ) -> crate::BoxFuture<'_, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            input
                .validate_for(&context, &self.descriptor)
                .map_err(|source| {
                    preparation_adapter_error(
                        &context,
                        &self.descriptor,
                        "stateknot.tool.input_binding_invalid",
                        "Tool input does not match the admitted invocation.",
                        source,
                    )
                })?;

            self.registry
                .validate_instance(
                    self.descriptor.input_schema(),
                    ToolSchemaRole::Input,
                    input.value(),
                )
                .map_err(|source| {
                    preparation_adapter_error(
                        &context,
                        &self.descriptor,
                        "stateknot.tool.input_schema_invalid",
                        "Tool input does not satisfy its registered schema.",
                        source,
                    )
                })?;

            let (_, input) = input.into_parts();
            let typed_input =
                serde_json::from_value::<T::Input>(input.into_value()).map_err(|source| {
                    preparation_adapter_error(
                        &context,
                        &self.descriptor,
                        "stateknot.tool.input_decode_failed",
                        "Tool input could not be decoded into its registered type.",
                        source,
                    )
                })?;

            let output = match self.tool.call(context.clone(), typed_input).await {
                Ok(output) => output,
                Err(error) => {
                    return match error.validate_for(&context, &self.descriptor) {
                        Ok(()) => Err(error),
                        Err(source) => Err(invalid_tool_error(&context, &self.descriptor, source)),
                    };
                }
            };

            let (typed_output, artifacts) = output.into_parts();
            let output_value = serde_json::to_value(typed_output).map_err(|source| {
                result_adapter_error(
                    &context,
                    &self.descriptor,
                    "stateknot.tool.output_encode_failed",
                    "Tool output could not be encoded from its registered type.",
                    source,
                )
            })?;
            let bounded_output = BoundedJson::try_from_value_with_limits(
                output_value,
                tool_output_json_limits(&self.descriptor),
            )
            .map_err(|source| {
                result_adapter_error(
                    &context,
                    &self.descriptor,
                    "stateknot.tool.output_limit_exceeded",
                    "Tool output exceeded its registered resource limits.",
                    source,
                )
            })?;

            self.registry
                .validate_instance(
                    self.descriptor.output_schema(),
                    ToolSchemaRole::Output,
                    &bounded_output,
                )
                .map_err(|source| {
                    result_adapter_error(
                        &context,
                        &self.descriptor,
                        "stateknot.tool.output_schema_invalid",
                        "Tool output does not satisfy its registered schema.",
                        source,
                    )
                })?;

            let result =
                ToolResult::for_invocation(&context, &self.descriptor, bounded_output, artifacts);
            result
                .validate_for(&context, &self.descriptor)
                .map_err(|source| {
                    result_adapter_error(
                        &context,
                        &self.descriptor,
                        "stateknot.tool.result_binding_invalid",
                        "Tool result does not match the admitted invocation.",
                        source,
                    )
                })?;
            Ok(result)
        })
    }
}

/// Registration failure for a typed tool adapter.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ToolAdapterBuildError {
    /// A generated Rust type schema did not match the pinned local contract.
    #[error("registered {role:?} tool schema rejected the generated Rust type schema: {source}")]
    SchemaContract {
        /// Schema boundary that failed registration.
        role: ToolSchemaRole,
        /// Private registry diagnostic.
        #[source]
        source: ToolSchemaValidationError,
    },
}

fn tool_output_json_limits(descriptor: &ToolDescriptor) -> JsonLimits {
    let descriptor_bytes =
        usize::try_from(descriptor.limits().max_inline_result_bytes().get()).unwrap_or(usize::MAX);
    let max_bytes = descriptor_bytes.min(JsonLimits::MAXIMUM.max_bytes());
    JsonLimits::try_new(
        max_bytes,
        JsonLimits::MAXIMUM.max_depth(),
        JsonLimits::MAXIMUM.max_container_entries(),
        JsonLimits::MAXIMUM.max_nodes(),
        JsonLimits::MAXIMUM.max_string_bytes(),
        JsonLimits::MAXIMUM.max_object_key_bytes(),
    )
    .expect("positive descriptor bytes narrowed to static JSON hard limits are valid")
}

fn preparation_adapter_error<E>(
    context: &ToolContext,
    descriptor: &ToolDescriptor,
    code: &str,
    message: &str,
    source: E,
) -> ToolError
where
    E: StdError + Send + Sync + 'static,
{
    let effect = if descriptor.semantics().risk() == ToolRisk::ReadOnly {
        ToolExternalEffect::NotApplicable
    } else {
        ToolExternalEffect::NotStarted
    };
    adapter_error(
        context,
        descriptor,
        FailureCategory::InvalidInput,
        RetryAdvice::Never,
        code,
        message,
        ToolErrorPhase::Preparation,
        effect,
        source,
    )
}

fn result_adapter_error<E>(
    context: &ToolContext,
    descriptor: &ToolDescriptor,
    code: &str,
    message: &str,
    source: E,
) -> ToolError
where
    E: StdError + Send + Sync + 'static,
{
    let effect = if descriptor.semantics().risk() == ToolRisk::ReadOnly {
        ToolExternalEffect::NotApplicable
    } else {
        ToolExternalEffect::Applied
    };
    adapter_error(
        context,
        descriptor,
        FailureCategory::Internal,
        RetryAdvice::Never,
        code,
        message,
        ToolErrorPhase::Result,
        effect,
        source,
    )
}

fn invalid_tool_error<E>(context: &ToolContext, descriptor: &ToolDescriptor, source: E) -> ToolError
where
    E: StdError + Send + Sync + 'static,
{
    let (category, advice, effect) = if descriptor.semantics().risk() == ToolRisk::ReadOnly {
        (
            FailureCategory::Internal,
            RetryAdvice::Never,
            ToolExternalEffect::NotApplicable,
        )
    } else {
        (
            FailureCategory::AmbiguousExternalOutcome,
            RetryAdvice::ReconcileFirst,
            ToolExternalEffect::Unknown,
        )
    };
    adapter_error(
        context,
        descriptor,
        category,
        advice,
        "stateknot.tool.invalid_error_evidence",
        "Tool returned failure evidence that does not match its admitted invocation.",
        ToolErrorPhase::Execution,
        effect,
        source,
    )
}

#[allow(clippy::too_many_arguments)]
fn adapter_error<E>(
    context: &ToolContext,
    descriptor: &ToolDescriptor,
    category: FailureCategory,
    advice: RetryAdvice,
    code: &str,
    message: &str,
    phase: ToolErrorPhase,
    effect: ToolExternalEffect,
    source: E,
) -> ToolError
where
    E: StdError + Send + Sync + 'static,
{
    let failure = Failure::new(
        FailureId::generate(),
        category,
        FailureCode::new(code).expect("static adapter failure code must be valid"),
        FailureOrigin::new("stateknot.tool_adapter")
            .expect("static adapter failure origin must be valid"),
        FailureMessage::new(message).expect("static adapter failure message must be valid"),
        advice,
    )
    .expect("adapter category and retry advice must be compatible")
    .with_private_source(source);
    ToolError::new(
        failure,
        phase,
        effect,
        ToolErrorProvenance::for_invocation(context, descriptor),
    )
    .expect("adapter phase and effect evidence must be compatible")
}

fn byte_count_from_usize(value: usize) -> ByteCount {
    ByteCount::new(u64::try_from(value).unwrap_or(u64::MAX))
}

fn execution_count_from_usize(value: usize) -> ExecutionCount {
    ExecutionCount::new(u64::try_from(value).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
    };

    use serde_json::{Value, json, to_value};

    use super::*;
    use crate::{
        ArtifactId, BudgetUsage, FailureBuildError, ResolvedBudget, ToolCancellationSupport,
    };

    const ATTEMPT_ID: &str = "01912345-6789-7abc-8def-0123456789ab";
    const SECOND_ATTEMPT_ID: &str = "01912345-6789-7abc-8def-0123456789ac";
    const INVOCATION_ID: &str = "01912345-6789-7abc-8def-0123456789ad";
    const RUN_ID: &str = "01912345-6789-7abc-8def-0123456789ae";
    const THREAD_ID: &str = "01912345-6789-7abc-8def-0123456789af";
    const FAILURE_ID: &str = "01912345-6789-7abc-8def-0123456789b0";
    const OBSERVED_AT: &str = "2029-12-31T23:59:59.000000Z";

    fn descriptor() -> ToolDescriptor {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-tool-v1.json")).unwrap();
        serde_json::from_value(fixture["descriptors"]["valid"][0].clone()).unwrap()
    }

    fn descriptor_with_progress(maximum: u64) -> ToolDescriptor {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-tool-v1.json")).unwrap();
        let mut value = fixture["descriptors"]["valid"][0].clone();
        value["invocation"]["max_progress_events"] = Value::from(maximum.to_string());
        serde_json::from_value(value).unwrap()
    }

    fn remaining_budget(observed_at: Timestamp) -> BudgetRemaining {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-budget-v1.json")).unwrap();
        let resolved =
            serde_json::from_value::<ResolvedBudget>(fixture["resolved"]["valid"][0].clone())
                .unwrap();
        resolved
            .remaining(&BudgetUsage::zero(), observed_at)
            .unwrap()
    }

    fn context_with_attempt(attempt_id: AttemptId) -> ToolContext {
        let descriptor = descriptor();
        let observed_at = OBSERVED_AT.parse::<Timestamp>().unwrap();
        ToolContext::new(
            TenantId::new("tenant-production").unwrap(),
            RUN_ID.parse().unwrap(),
            THREAD_ID.parse().unwrap(),
            INVOCATION_ID.parse().unwrap(),
            attempt_id,
            &descriptor,
            remaining_budget(observed_at),
            DurationMillis::new(30_000).unwrap(),
            observed_at,
            Instant::now(),
            CancellationSignal::never(),
        )
        .unwrap()
    }

    fn context() -> ToolContext {
        context_with_attempt(ATTEMPT_ID.parse().unwrap())
    }

    fn failure(category: FailureCategory, advice: RetryAdvice) -> Failure {
        Failure::new(
            FAILURE_ID.parse().unwrap(),
            category,
            FailureCode::new("tool.execution_failed").unwrap(),
            FailureOrigin::new("tool.example").unwrap(),
            FailureMessage::new("The tool operation did not complete.").unwrap(),
            advice,
        )
        .unwrap()
    }

    fn provenance() -> ToolErrorProvenance {
        let descriptor = descriptor();
        ToolErrorProvenance::new(
            INVOCATION_ID.parse().unwrap(),
            ATTEMPT_ID.parse().unwrap(),
            descriptor.metadata().identity().clone(),
        )
    }

    #[test]
    fn context_intersects_deadlines_and_reuses_key_across_attempts() {
        let first = context_with_attempt(ATTEMPT_ID.parse().unwrap());
        let second = context_with_attempt(SECOND_ATTEMPT_ID.parse().unwrap());

        assert_eq!(first.invocation_id(), second.invocation_id());
        assert_ne!(first.attempt_id(), second.attempt_id());
        assert_eq!(
            first.required_idempotency_key().unwrap(),
            second.required_idempotency_key().unwrap()
        );
        assert_eq!(
            first.required_idempotency_key().unwrap().to_string(),
            INVOCATION_ID
        );
        assert_eq!(
            first.deadline(),
            "2030-01-01T00:00:00.000000Z".parse().unwrap()
        );
        assert_eq!(first.remaining_time_at(first.deadline_instant()), None);
    }

    #[test]
    fn context_rejects_zero_and_widened_timeouts() {
        let descriptor = descriptor();
        let observed_at = OBSERVED_AT.parse::<Timestamp>().unwrap();
        let make = |timeout| {
            ToolContext::new(
                TenantId::new("tenant-production").unwrap(),
                RUN_ID.parse().unwrap(),
                THREAD_ID.parse().unwrap(),
                INVOCATION_ID.parse().unwrap(),
                ATTEMPT_ID.parse().unwrap(),
                &descriptor,
                remaining_budget(observed_at),
                timeout,
                observed_at,
                Instant::now(),
                CancellationSignal::never(),
            )
        };
        assert_eq!(
            make(DurationMillis::ZERO).unwrap_err(),
            ToolContextError::ZeroEffectiveTimeout
        );
        assert!(matches!(
            make(DurationMillis::new(30_001).unwrap()),
            Err(ToolContextError::TimeoutExceedsDescriptor { .. })
        ));
    }

    #[test]
    fn reconciliation_context_preserves_original_identity_and_is_finite() {
        let descriptor = descriptor();
        let observed_at = OBSERVED_AT.parse::<Timestamp>().unwrap();
        let context = ToolReconciliationContext::new(
            TenantId::new("tenant-production").unwrap(),
            RUN_ID.parse().unwrap(),
            THREAD_ID.parse().unwrap(),
            INVOCATION_ID.parse().unwrap(),
            ATTEMPT_ID.parse().unwrap(),
            &descriptor,
            DurationMillis::new(30_000).unwrap(),
            observed_at,
            Instant::now(),
            "2030-01-01T00:00:00.000000Z".parse().unwrap(),
            CancellationSignal::never(),
        )
        .unwrap();

        context.validate_for(&descriptor).unwrap();
        assert_eq!(context.invocation_id().to_string(), INVOCATION_ID);
        assert_eq!(context.attempt_id().to_string(), ATTEMPT_ID);
        assert_eq!(
            context.idempotency_key().unwrap().to_string(),
            INVOCATION_ID
        );
        assert_eq!(
            context.deadline(),
            "2030-01-01T00:00:00.000000Z".parse().unwrap()
        );
        assert!(format!("{context:?}").contains("has_idempotency_key: true"));
    }

    #[test]
    fn reconciliation_polling_and_probe_failures_are_bounded() {
        assert_eq!(
            ToolReconciliationObservation::pending(DurationMillis::ZERO).unwrap_err(),
            ToolReconciliationObservationError::ZeroRetryDelay
        );
        let excessive = DurationMillis::new(3_600_001).unwrap();
        assert!(matches!(
            ToolReconciliationObservation::pending(excessive),
            Err(ToolReconciliationObservationError::RetryDelayTooLarge { .. })
        ));
        let ambiguous = failure(
            FailureCategory::AmbiguousExternalOutcome,
            RetryAdvice::ReconcileFirst,
        );
        assert_eq!(
            ToolReconciliationProbeError::new(ambiguous).unwrap_err(),
            ToolReconciliationProbeErrorBuildError::RecursiveReconciliation
        );
        let zero_retry = failure(
            FailureCategory::DependencyUnavailable,
            RetryAdvice::SafeAfter {
                delay: DurationMillis::ZERO,
            },
        );
        assert!(matches!(
            ToolReconciliationProbeError::new(zero_retry),
            Err(ToolReconciliationProbeErrorBuildError::InvalidRetry { .. })
        ));
    }

    #[test]
    fn input_requires_an_object_and_binds_schema_and_limits() {
        let descriptor = descriptor();
        assert_eq!(
            ToolInput::new(
                descriptor.input_schema().clone(),
                BoundedJson::try_from_value(json!([1, 2])).unwrap(),
            ),
            Err(ToolInputError::ObjectRootRequired)
        );
        let input = ToolInput::new(
            descriptor.input_schema().clone(),
            BoundedJson::try_from_value(json!({ "amount": 42 })).unwrap(),
        )
        .unwrap();
        input.validate_for(&context(), &descriptor).unwrap();
        assert!(!format!("{input:?}").contains("amount"));
    }

    fn artifact_value() -> Value {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/core-artifact-v1.json")).unwrap();
        fixture["artifact_refs"]["valid"][0].clone()
    }

    fn bound_artifact(tenant: &str, artifact_id: ArtifactId) -> ArtifactRef {
        let descriptor = descriptor();
        let mut value = artifact_value();
        value["identity"]["tenant_id"] = Value::from(tenant);
        value["identity"]["artifact_id"] = Value::from(artifact_id.to_string());
        value["provenance"]["principal"] =
            to_value(descriptor.metadata().identity().owner()).unwrap();
        value["provenance"]["capability"] =
            to_value(descriptor.metadata().identity().capability()).unwrap();
        value["provenance"]["run_id"] = Value::from(RUN_ID);
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn artifacts_are_unique_bounded_and_checked_against_tenant() {
        let first = bound_artifact("tenant-production", ArtifactId::generate());
        assert_eq!(
            ToolArtifacts::try_new([first.clone(), first]),
            Err(ToolArtifactsError::DuplicateIdentity)
        );

        let too_many = (0..=ToolArtifacts::MAX_LEN)
            .map(|_| bound_artifact("tenant-production", ArtifactId::generate()));
        assert!(matches!(
            ToolArtifacts::try_new(too_many),
            Err(ToolArtifactsError::TooMany { .. })
        ));

        let descriptor = descriptor();
        let context = context();
        let artifacts =
            ToolArtifacts::try_new([bound_artifact("tenant-other", ArtifactId::generate())])
                .unwrap();
        let result = ToolResult::for_invocation(
            &context,
            &descriptor,
            BoundedJson::try_from_value(json!({ "accepted": true })).unwrap(),
            artifacts,
        );
        assert!(matches!(
            result.validate_for(&context, &descriptor),
            Err(ToolResultValidationError::ArtifactTenantMismatch { .. })
        ));
    }

    #[test]
    fn tool_error_requires_exact_unknown_effect_and_ambiguity_pair() {
        assert_eq!(
            ToolError::new(
                failure(FailureCategory::Internal, RetryAdvice::Never),
                ToolErrorPhase::Execution,
                ToolExternalEffect::Unknown,
                provenance(),
            )
            .unwrap_err(),
            ToolErrorBuildError::UnknownEffectRequiresAmbiguousFailure
        );
        let ambiguous = ToolError::new(
            failure(
                FailureCategory::AmbiguousExternalOutcome,
                RetryAdvice::ReconcileFirst,
            ),
            ToolErrorPhase::Execution,
            ToolExternalEffect::Unknown,
            provenance(),
        )
        .unwrap();
        ambiguous.validate_for(&context(), &descriptor()).unwrap();
        assert_eq!(ambiguous.external_effect(), ToolExternalEffect::Unknown);

        assert!(matches!(
            Failure::new(
                FailureId::generate(),
                FailureCategory::AmbiguousExternalOutcome,
                FailureCode::new("tool.ambiguous").unwrap(),
                FailureOrigin::new("tool.example").unwrap(),
                FailureMessage::new("The external outcome is unknown.").unwrap(),
                RetryAdvice::Never,
            ),
            Err(FailureBuildError::AmbiguousOutcomeRequiresReconciliation)
        ));
    }

    #[derive(Debug, Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    struct CaptureInput {
        amount: u64,
    }

    #[derive(Debug, JsonSchema, Serialize)]
    #[serde(deny_unknown_fields)]
    struct CaptureOutput {
        accepted: bool,
        key: String,
    }

    struct CaptureTool {
        descriptor: ToolDescriptor,
    }

    impl Tool for CaptureTool {
        type Input = CaptureInput;
        type Output = CaptureOutput;

        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }

        fn call(
            &self,
            context: ToolContext,
            input: Self::Input,
        ) -> crate::BoxFuture<'_, Result<ToolOutput<Self::Output>, ToolError>> {
            let key = context.required_idempotency_key().unwrap().to_string();
            Box::pin(async move {
                Ok(ToolOutput::inline(CaptureOutput {
                    accepted: input.amount > 0,
                    key,
                }))
            })
        }
    }

    #[derive(Default)]
    struct AcceptingRegistry {
        type_validations: AtomicUsize,
        instance_validations: AtomicUsize,
    }

    impl ToolSchemaRegistry for AcceptingRegistry {
        fn validate_type_schema(
            &self,
            _: &SchemaReference,
            _: ToolSchemaRole,
            _: &Schema,
        ) -> Result<(), ToolSchemaValidationError> {
            self.type_validations.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn validate_instance(
            &self,
            _: &SchemaReference,
            _: ToolSchemaRole,
            _: &BoundedJson,
        ) -> Result<(), ToolSchemaValidationError> {
            self.instance_validations.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_ready<F>(future: F) -> F::Output
    where
        F: Future,
    {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test future unexpectedly remained pending"),
        }
    }

    #[derive(Default)]
    struct RecordingProgressSink {
        events: Mutex<Vec<ToolProgressEvent>>,
    }

    impl ToolProgressSink for RecordingProgressSink {
        fn emit(
            &self,
            event: ToolProgressEvent,
        ) -> crate::BoxFuture<'_, Result<(), ToolProgressSinkError>> {
            Box::pin(async move {
                self.events.lock().unwrap().push(event);
                Ok(())
            })
        }
    }

    struct PendingProgressSink;

    impl ToolProgressSink for PendingProgressSink {
        fn emit(
            &self,
            _: ToolProgressEvent,
        ) -> crate::BoxFuture<'_, Result<(), ToolProgressSinkError>> {
            Box::pin(std::future::pending())
        }
    }

    struct FailingProgressSink;

    impl ToolProgressSink for FailingProgressSink {
        fn emit(
            &self,
            _: ToolProgressEvent,
        ) -> crate::BoxFuture<'_, Result<(), ToolProgressSinkError>> {
            Box::pin(async {
                Err(ToolProgressSinkError::new(std::io::Error::other(
                    "private sink detail",
                )))
            })
        }
    }

    fn progress_context(
        descriptor: &ToolDescriptor,
        sink: Arc<dyn ToolProgressSink>,
    ) -> ToolContext {
        let observed_at = OBSERVED_AT.parse::<Timestamp>().unwrap();
        ToolContext::new_with_progress(
            TenantId::new("tenant-production").unwrap(),
            RUN_ID.parse().unwrap(),
            THREAD_ID.parse().unwrap(),
            INVOCATION_ID.parse().unwrap(),
            ATTEMPT_ID.parse().unwrap(),
            descriptor,
            remaining_budget(observed_at),
            DurationMillis::new(30_000).unwrap(),
            observed_at,
            Instant::now(),
            CancellationSignal::never(),
            sink,
        )
        .unwrap()
    }

    #[test]
    fn progress_reporter_orders_normalizes_and_bounds_events() {
        let descriptor = descriptor_with_progress(2);
        let sink = Arc::new(RecordingProgressSink::default());
        let context = progress_context(&descriptor, sink.clone());
        let reporter = context.progress().unwrap();

        let first = poll_ready(
            reporter.emit(ToolProgressUpdate::new(ExecutionCount::new(1), None).unwrap()),
        )
        .unwrap();
        let second = poll_ready(reporter.emit(
            ToolProgressUpdate::new(ExecutionCount::new(2), Some(ExecutionCount::new(10))).unwrap(),
        ))
        .unwrap();
        assert_eq!(first.sequence(), ExecutionCount::ZERO);
        assert_eq!(second.sequence(), ExecutionCount::new(1));
        assert_eq!(second.update().total(), Some(ExecutionCount::new(10)));
        first.validate_for(&context, &descriptor).unwrap();
        second.validate_for(&context, &descriptor).unwrap();
        let outside_capacity = ToolProgressEvent::new(
            first.provenance().clone(),
            ExecutionCount::new(2),
            first.update(),
        );
        assert!(matches!(
            outside_capacity.validate_for(&context, &descriptor),
            Err(ToolProgressEventValidationError::SequenceLimitExceeded { .. })
        ));
        assert_eq!(sink.events.lock().unwrap().as_slice(), &[first, second]);

        assert!(matches!(
            poll_ready(
                reporter.emit(
                    ToolProgressUpdate::new(ExecutionCount::new(3), Some(ExecutionCount::new(10)),)
                        .unwrap(),
                )
            ),
            Err(ToolProgressError::MaximumReached { .. })
        ));
    }

    #[test]
    fn progress_reporter_rejects_reordering_and_changed_totals() {
        let descriptor = descriptor_with_progress(4);
        let context = progress_context(&descriptor, Arc::new(RecordingProgressSink::default()));
        let reporter = context.progress().unwrap();
        poll_ready(reporter.emit(
            ToolProgressUpdate::new(ExecutionCount::new(2), Some(ExecutionCount::new(10))).unwrap(),
        ))
        .unwrap();

        assert!(matches!(
            poll_ready(
                reporter.emit(ToolProgressUpdate::new(ExecutionCount::new(2), None).unwrap(),)
            ),
            Err(ToolProgressError::NotIncreasing { .. })
        ));
        assert!(matches!(
            poll_ready(
                reporter.emit(
                    ToolProgressUpdate::new(ExecutionCount::new(3), Some(ExecutionCount::new(11)),)
                        .unwrap(),
                )
            ),
            Err(ToolProgressError::TotalChanged { .. })
        ));
    }

    #[test]
    fn progress_failure_or_dropped_future_poison_the_reporter() {
        let descriptor = descriptor_with_progress(4);
        let failed = progress_context(&descriptor, Arc::new(FailingProgressSink));
        let error = poll_ready(
            failed
                .progress()
                .unwrap()
                .emit(ToolProgressUpdate::new(ExecutionCount::new(1), None).unwrap()),
        )
        .unwrap_err();
        assert!(matches!(error, ToolProgressError::Sink { .. }));
        assert!(!format!("{error:?}").contains("private sink detail"));
        assert!(matches!(
            poll_ready(
                failed
                    .progress()
                    .unwrap()
                    .emit(ToolProgressUpdate::new(ExecutionCount::new(2), None).unwrap(),)
            ),
            Err(ToolProgressError::StatePoisoned)
        ));

        let pending = progress_context(&descriptor, Arc::new(PendingProgressSink));
        let reporter = pending.progress().unwrap();
        let mut first =
            reporter.emit(ToolProgressUpdate::new(ExecutionCount::new(1), None).unwrap());
        let waker = Waker::from(Arc::new(NoopWake));
        let mut task_context = Context::from_waker(&waker);
        assert!(matches!(
            first.as_mut().poll(&mut task_context),
            Poll::Pending
        ));
        assert!(matches!(
            poll_ready(
                reporter.emit(ToolProgressUpdate::new(ExecutionCount::new(2), None).unwrap(),)
            ),
            Err(ToolProgressError::ConcurrentEmission)
        ));
        drop(first);
        assert!(matches!(
            poll_ready(
                reporter.emit(ToolProgressUpdate::new(ExecutionCount::new(2), None).unwrap(),)
            ),
            Err(ToolProgressError::StatePoisoned)
        ));
    }

    #[test]
    fn progress_updates_reject_impossible_totals() {
        assert_eq!(
            ToolProgressUpdate::new(ExecutionCount::ZERO, Some(ExecutionCount::ZERO)),
            Err(ToolProgressUpdateError::ZeroTotal)
        );
        assert!(matches!(
            ToolProgressUpdate::new(ExecutionCount::new(2), Some(ExecutionCount::new(1)),),
            Err(ToolProgressUpdateError::CompletedExceedsTotal { .. })
        ));
    }

    #[test]
    fn typed_adapter_validates_both_schemas_and_is_object_safe() {
        fn accept_object(_: &dyn ErasedTool) {}
        fn assert_send_sync<T: Send + Sync>() {}

        let adapter = ToolAdapter::new(
            CaptureTool {
                descriptor: descriptor(),
            },
            AcceptingRegistry::default(),
        )
        .unwrap();
        assert_eq!(
            adapter.registry().type_validations.load(Ordering::Relaxed),
            2
        );
        accept_object(&adapter);
        assert_send_sync::<ToolContext>();
        assert_send_sync::<ToolResult>();
        assert_send_sync::<ToolError>();

        let descriptor = adapter.descriptor().clone();
        let context = context();
        let input = ToolInput::new(
            descriptor.input_schema().clone(),
            BoundedJson::try_from_value(json!({ "amount": 42 })).unwrap(),
        )
        .unwrap();
        let result = poll_ready(ErasedTool::call(&adapter, context.clone(), input)).unwrap();
        result.validate_for(&context, &descriptor).unwrap();
        assert_eq!(result.output().as_value()["accepted"], true);
        assert_eq!(result.output().as_value()["key"], INVOCATION_ID);
        assert_eq!(
            adapter
                .registry()
                .instance_validations
                .load(Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn wire_contracts_are_closed_and_redact_inline_values() {
        let descriptor = descriptor();
        let context = context();
        let result = ToolResult::for_invocation(
            &context,
            &descriptor,
            BoundedJson::try_from_value(json!({ "secret": "do-not-log" })).unwrap(),
            ToolArtifacts::empty(),
        );
        let value = to_value(&result).unwrap();
        let decoded = serde_json::from_value::<ToolResult>(value.clone()).unwrap();
        assert_eq!(to_value(decoded).unwrap(), value);
        assert!(!format!("{result:?}").contains("do-not-log"));

        for schema in [
            to_value(schemars::schema_for!(ToolProgressUpdate)).unwrap(),
            to_value(schemars::schema_for!(ToolProgressProvenance)).unwrap(),
            to_value(schemars::schema_for!(ToolProgressEvent)).unwrap(),
            to_value(schemars::schema_for!(ToolInput)).unwrap(),
            to_value(schemars::schema_for!(ToolResultProvenance)).unwrap(),
            to_value(schemars::schema_for!(ToolResult)).unwrap(),
            to_value(schemars::schema_for!(ToolErrorProvenance)).unwrap(),
            to_value(schemars::schema_for!(ToolError)).unwrap(),
        ] {
            assert_eq!(schema["type"], "object");
            assert_eq!(schema["additionalProperties"], false);
        }

        let mut invalid = value;
        invalid["provider_payload"] = json!({ "secret": true });
        assert!(serde_json::from_value::<ToolResult>(invalid).is_err());
        assert_eq!(
            descriptor.invocation().cancellation(),
            ToolCancellationSupport::Cooperative
        );
    }
}
